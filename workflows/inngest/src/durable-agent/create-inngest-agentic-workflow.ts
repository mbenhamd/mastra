import { createHash } from 'node:crypto';
import {
  createDurableBackgroundTaskCheckStep,
  createDurableLLMExecutionStep,
  createDurableLLMMappingStep,
  createDurableSignalDrainStep,
  createDurableToolCallStep,
  DurableAgentDefaults,
  DurableStepIds,
  assertTerminalToolResultRetained,
  createTerminalToolResultEnvelope,
  emitChunkEvent,
  emitFinishEvent,
  modelConfigSchema,
  durableAgenticOutputSchema,
  baseIterationStateSchema,
  createBaseIterationStateUpdate,
  globalRunRegistry,
  createDurableRuntimeRequestContext,
  mapDurableIterationToLLMInput,
  resolveRuntimeDependencies,
  resolveDurableToolCallConcurrency,
  TOOL_PERMISSION_POLICY_KEY,
  TOOL_PERMISSION_POLICY_REQUIRED_KEY,
  TOOL_PERMISSION_POLICY_STABLE_KEY,
  outputProcessorsOwnTerminalPersistence,
} from '@mastra/core/agent/durable';
import type {
  DurableToolPermissionResolver,
  DurableAgenticExecutionOutput,
  DurableAgenticWorkflowInput,
  DurableLLMStepOutput,
  DurableToolCallOutput,
  DurableToolCallInput,
  ToolPermissionPolicy,
} from '@mastra/core/agent/durable';
import { MessageList } from '@mastra/core/agent/message-list';
import type { PubSub } from '@mastra/core/events';
import { SpanType, EntityType, InternalSpans } from '@mastra/core/observability';
import type { ExportedSpan } from '@mastra/core/observability';
import { ProcessorRunner } from '@mastra/core/processors';
import type { RequestContext } from '@mastra/core/request-context';
import { PUBSUB_SYMBOL } from '@mastra/core/workflows/_constants';
import type { Inngest } from 'inngest';
import { z } from 'zod';

import { init } from '../index';

/**
 * Input schema for the durable agentic workflow.
 * Extends base with observability fields for Inngest.
 */
const durableAgenticInputSchema = z.object({
  runId: z.string(),
  agentId: z.string(),
  agentName: z.string().optional(),
  messageListState: z.any(),
  toolsMetadata: z.array(z.any()),
  modelConfig: modelConfigSchema,
  options: z.any(),
  state: z.any(),
  messageId: z.string(),
  // Observability fields (Inngest-specific)
  agentSpanData: z.any().optional(),
  modelSpanData: z.any().optional(),
  stepIndex: z.number().optional(),
});

// Output schema imported from shared (durableAgenticOutputSchema)

/**
 * Options for creating an Inngest durable agentic workflow
 */
export interface InngestDurableAgenticWorkflowOptions {
  /** Inngest client instance */
  inngest: Inngest;
  /** Maximum number of agentic loop iterations */
  maxSteps?: number;
  /**
   * Workflow IDs used by the parent agentic loop and its nested execution
   * workflow. Direct factory callers retain the historical shared IDs when
   * this option is omitted.
   */
  workflowIds?: InngestDurableAgenticWorkflowIds;
  /**
   * Trusted worker-local policy resolver. The function is captured by the
   * registered Inngest worker and is never serialized into workflow state.
   */
  resolveToolPermission?: DurableToolPermissionResolver;
}

/**
 * Iteration state schema - extends base with observability fields.
 */
const iterationStateSchema = baseIterationStateSchema.extend({
  // Observability - exported span data for agent run
  agentSpanData: z.any().optional(),
  // Observability - exported span data for model generation (ONE span for entire run)
  modelSpanData: z.any().optional(),
  // Step index for continuation across iterations (maintains step: 0, 1, 2, ...)
  stepIndex: z.number(),
});

type IterationState = z.infer<typeof iterationStateSchema> & {
  agentSpanData?: ExportedSpan<SpanType.AGENT_RUN>;
  modelSpanData?: ExportedSpan<SpanType.MODEL_GENERATION>;
};

/**
 * Create a durable agentic workflow using Inngest.
 *
 * This workflow implements the agentic loop pattern in a durable way using
 * Inngest's execution engine:
 *
 * 1. LLM Execution Step - Calls the LLM and gets response/tool calls
 * 2. Tool Call Steps (foreach) - Executes each tool call individually with suspend support
 * 3. LLM Mapping Step - Merges tool results back into state
 * 4. Loop - Continues if more tool calls are needed (dowhile)
 *
 * All state flows through workflow input/output, making it durable across
 * process restarts and execution engine replays.
 *
 * @param options - Configuration options
 * @returns An InngestWorkflow instance that implements the agentic loop
 */
/**
 * Durable-agent wire/function protocol. Function IDs include this version so
 * a pre-policy worker cannot claim an event whose authorization markers it
 * does not understand.
 */
export const INNGEST_DURABLE_AGENT_PROTOCOL_VERSION = 'v2' as const;

/** Prefix for Inngest engine workflow IDs to avoid collision with other engines and protocol versions. */
const INNGEST_ENGINE_PREFIX = `inngest:${INNGEST_DURABLE_AGENT_PROTOCOL_VERSION}`;

/** Inngest-prefixed workflow IDs */
export const InngestDurableStepIds = {
  AGENTIC_EXECUTION: `${INNGEST_ENGINE_PREFIX}:${DurableStepIds.AGENTIC_EXECUTION}`,
  AGENTIC_LOOP: `${INNGEST_ENGINE_PREFIX}:${DurableStepIds.AGENTIC_LOOP}`,
} as const;

export interface InngestDurableAgenticWorkflowIds {
  AGENTIC_EXECUTION: string;
  AGENTIC_LOOP: string;
}

/**
 * Derive collision-safe workflow IDs for one public durable-agent ID.
 *
 * Both IDs must share the same owner suffix: the loop is registered as an
 * Inngest function while the execution workflow is recursively exposed as a
 * nested function. Changing only the parent would leave nested functions from
 * different durable agents aliased to the same Inngest function ID.
 *
 * The protocol prefix, hash input, and width are a persisted routing contract.
 * A protocol change intentionally produces new Inngest function IDs. Keep the
 * prior worker deployment running long enough to drain pre-upgrade runs; a v2
 * wrapper doesn't resume a v1 snapshot under a different function identity.
 */
export function createInngestDurableAgenticWorkflowIds(agentId: string): InngestDurableAgenticWorkflowIds {
  if (!agentId) {
    throw new TypeError('Inngest durable-agent workflow IDs require a non-empty agent ID');
  }

  const ownerHash = createHash('sha256')
    .update(`mastra:inngest:durable-agent:${INNGEST_DURABLE_AGENT_PROTOCOL_VERSION}\0${agentId}`)
    .digest('hex')
    .slice(0, 32);

  return {
    AGENTIC_EXECUTION: `${InngestDurableStepIds.AGENTIC_EXECUTION}:${ownerHash}`,
    AGENTIC_LOOP: `${InngestDurableStepIds.AGENTIC_LOOP}:${ownerHash}`,
  };
}

export function createInngestDurableAgenticWorkflow(options: InngestDurableAgenticWorkflowOptions) {
  const {
    inngest,
    maxSteps = DurableAgentDefaults.MAX_STEPS,
    workflowIds = InngestDurableStepIds,
    resolveToolPermission,
  } = options;
  const { createWorkflow } = init(inngest);

  // Create the LLM execution step - tools and model are resolved from Mastra at runtime
  const llmExecutionStep = createDurableLLMExecutionStep();

  // Create the tool call step - each tool call runs as its own step with suspend support
  const toolCallStep = createDurableToolCallStep({ resolveToolPermission });

  // Create the LLM mapping step - reuse from core
  const llmMappingStep = createDurableLLMMappingStep();

  // Create the background task check step
  const backgroundTaskCheckStep = createDurableBackgroundTaskCheckStep();

  // Drain active-run signals after tools/background work settle. A signal
  // wins over a terminal candidate and forces one provider continuation.
  const signalDrainStep = createDurableSignalDrainStep();

  // Create the single iteration workflow (LLM -> Tool Calls -> Mapping)
  const singleIterationWorkflow = createWorkflow({
    id: workflowIds.AGENTIC_EXECUTION,
    inputSchema: iterationStateSchema,
    outputSchema: iterationStateSchema,
    options: {
      tracingPolicy: {
        // Mark all workflow spans as internal so they're hidden in traces
        // This makes the trace structure match regular agents (agent_run -> model_generation -> tool_call)
        internal: InternalSpans.WORKFLOW,
      },
      shouldPersistSnapshot: ({ workflowStatus }) => workflowStatus === 'suspended',
      validateInputs: false,
    },
    steps: [],
  })
    // Step 0: Convert iteration state to LLM input format
    .map(
      async ({ inputData }) => {
        const state = inputData as IterationState;
        return mapDurableIterationToLLMInput(state);
      },
      { id: 'map-to-llm-input' },
    )
    // Step 1: Execute LLM
    .then(llmExecutionStep)
    // Step 2: Extract tool calls as array for foreach
    .map(
      async ({ inputData, getInitData }) => {
        const llmOutput = inputData as DurableLLMStepOutput;
        const iterationCount = (getInitData() as IterationState).iterationCount;
        return (llmOutput.toolCalls ?? []).map(toolCall => ({
          ...toolCall,
          iterationCount,
          stepSpanData: llmOutput.stepSpanData,
        })) as DurableToolCallInput[];
      },
      { id: 'extract-tool-calls' },
    )
    // Step 3: Execute each tool call individually (with suspend support).
    // Tool result/error PubSub emission is handled by createDurableToolCallStep.
    // Concurrency is resolved per run at execution time from the serialized
    // iteration state (never a shared mutable object — the workflow instance is
    // reused across runs and Inngest replays memoized steps): approval/suspend
    // tool sets run sequentially, otherwise the run's `toolCallConcurrency`
    // applies (default 10). Mirrors @mastra/core's behavior after #9704.
    .foreach(toolCallStep, {
      concurrency: ({ inputData, getInitData, requestContext }) => {
        const state = getInitData() as IterationState | undefined;
        // The worker resolver may be async and may distinguish a fresh call
        // from a resume, while foreach concurrency resolution is synchronous
        // and has no resume phase. It therefore remains sequential. A live
        // immutable RequestContext snapshot can still unlock all-allow batches
        // through the same shared classifier as core durable execution.
        if (resolveToolPermission) return 1;
        const policyCandidate = requestContext?.get(TOOL_PERMISSION_POLICY_KEY);
        const permissionPolicy =
          typeof policyCandidate === 'function' ? (policyCandidate as ToolPermissionPolicy) : undefined;
        return resolveDurableToolCallConcurrency({
          options: state?.options,
          toolsMetadata: state?.toolsMetadata,
          toolCalls: inputData as DurableToolCallInput[],
          permissionPolicy: permissionPolicy ? toolCall => permissionPolicy(toolCall.toolName) : undefined,
          permissionPolicyStable: requestContext?.get(TOOL_PERMISSION_POLICY_STABLE_KEY) === true,
          permissionPolicyRequired:
            state?.options.permissionPolicyRequired === true ||
            requestContext?.get(TOOL_PERMISSION_POLICY_REQUIRED_KEY) === true,
        });
      },
    })
    // Step 4: Collect tool results, create observability spans, and bundle for mapping
    .map(
      async ({ inputData, getStepResult, getInitData, mastra }) => {
        const toolResults = inputData as DurableToolCallOutput[];
        const llmOutput = getStepResult(llmExecutionStep.id) as DurableLLMStepOutput;
        const initData = getInitData() as IterationState;

        // Create observability spans retroactively for each tool result
        // In the foreach pattern, individual tool calls don't have access to
        // the observability context, so we create spans here in the collection step
        const observability = mastra?.observability?.getSelectedInstance({});

        const modelSpanData = (llmOutput as any)?.modelSpanData as ExportedSpan<SpanType.MODEL_GENERATION> | undefined;
        const stepSpanData = (llmOutput as any)?.stepSpanData as ExportedSpan<SpanType.MODEL_STEP> | undefined;

        const modelSpan = modelSpanData ? observability?.rebuildSpan(modelSpanData) : undefined;
        const stepSpan = stepSpanData ? observability?.rebuildSpan(stepSpanData) : undefined;
        const agentSpan = initData.agentSpanData ? observability?.rebuildSpan(initData.agentSpanData) : undefined;
        const toolParentSpan = stepSpan ?? modelSpan ?? agentSpan;

        // Create tool call + tool result spans for each tool result
        for (const tr of toolResults) {
          const toolSpan = toolParentSpan?.createChildSpan({
            type: SpanType.TOOL_CALL,
            name: `tool: '${tr.toolName}'`,
            entityType: EntityType.TOOL,
            entityId: tr.toolName,
            entityName: tr.toolName,
            input: tr.args,
          });

          if (tr.error) {
            toolSpan?.error({ error: new Error(tr.error.message) });
          } else {
            toolSpan?.end({ output: tr.result });
          }

          // Create tool-result chunk span as child of model_step
          if (!tr.error) {
            stepSpan?.createEventSpan({
              type: SpanType.MODEL_CHUNK,
              name: `chunk: 'tool-result'`,
              output: {
                toolCallId: tr.toolCallId,
                toolName: tr.toolName,
                result: tr.result,
              },
            });
          }
        }

        // End step span (children before parent)
        // NOTE: We do NOT close the model span here - it stays open for the entire agent run
        // and is closed in map-final-output after the agentic loop completes
        const toolCalls = (llmOutput?.toolCalls ?? []) as DurableToolCallInput[];
        if (stepSpan) {
          const stepFinishPayload = (llmOutput as any).stepFinishPayload as any;
          stepSpan.end({
            output: {
              toolCalls: toolCalls.map(tc => ({
                toolCallId: tc.toolCallId,
                toolName: tc.toolName,
                args: tc.args,
              })),
              toolResults: toolResults.map((tr: DurableToolCallOutput) => ({
                toolCallId: tr.toolCallId,
                toolName: tr.toolName,
                result: tr.result,
                error: tr.error,
              })),
            },
            attributes: {
              usage: stepFinishPayload?.output?.usage,
              finishReason: stepFinishPayload?.stepResult?.reason,
              isContinued: stepFinishPayload?.stepResult?.isContinued,
            },
          });
        }

        return {
          llmOutput,
          toolResults,
          runId: initData.runId,
          agentId: initData.agentId,
          messageId: initData.messageId,
          state: llmOutput?.state ?? initData.state,
        };
      },
      { id: 'collect-tool-results' },
    )
    // Step 5: Map tool results back to state
    .then(llmMappingStep)
    // Step 6: Check for pending background tasks
    .then(backgroundTaskCheckStep)
    // Step 6.5: A queued signal supersedes direct terminal delivery.
    .then(signalDrainStep)
    // Step 7: Map back to iteration state format using shared function
    .map(
      async ({ inputData, getInitData }) => {
        const executionOutput = inputData as DurableAgenticExecutionOutput;
        const initData = getInitData() as IterationState;

        // Use shared function for base state update
        const baseUpdate = createBaseIterationStateUpdate({
          currentState: initData,
          executionOutput,
        });

        // Extend with Inngest-specific observability fields
        const newIterationState: IterationState = {
          ...baseUpdate,
          // Preserve agent span data for observability
          agentSpanData: initData.agentSpanData,
          // Preserve model span data (ONE span for entire agent run)
          modelSpanData: initData.modelSpanData,
          // Increment step index for next iteration (step: 0 → 1 → 2 → ...)
          stepIndex: initData.stepIndex + 1,
        };

        return newIterationState;
      },
      { id: 'update-iteration-state' },
    )
    .commit();

  // Create the main agentic loop workflow with dowhile
  return (
    createWorkflow({
      id: workflowIds.AGENTIC_LOOP,
      inputSchema: durableAgenticInputSchema,
      outputSchema: durableAgenticOutputSchema,
      options: {
        tracingPolicy: {
          // Mark all workflow spans as internal so they're hidden in traces
          // This makes the trace structure match regular agents (agent_run -> model_generation -> tool_call)
          internal: InternalSpans.WORKFLOW,
        },
        shouldPersistSnapshot: ({ workflowStatus }) => workflowStatus === 'suspended',
        validateInputs: false,
      },
      steps: [],
    })
      // Initialize iteration state from input
      // The AGENT_RUN span is created BEFORE the workflow starts (in InngestAgent.stream)
      // and passed via input.agentSpanData so the agent_run is the root of the trace
      .map(
        async ({ inputData }) => {
          const input = inputData as DurableAgenticWorkflowInput;

          // Use the agent span data passed from InngestAgent.stream()
          // This span was created before the workflow started, making it the trace root
          const agentSpanData = input.agentSpanData as ExportedSpan<SpanType.AGENT_RUN> | undefined;
          // Use the model span data passed from InngestAgent.stream()
          // This ensures ONE model_generation span contains all steps (like regular agents)
          const modelSpanData = input.modelSpanData as ExportedSpan<SpanType.MODEL_GENERATION> | undefined;

          const iterationState: IterationState = {
            ...input,
            iterationCount: 0,
            accumulatedSteps: [],
            accumulatedUsage: {
              inputTokens: 0,
              outputTokens: 0,
              totalTokens: 0,
            },
            lastStepResult: undefined,
            agentSpanData,
            modelSpanData,
            stepIndex: input.stepIndex ?? 0,
          };
          return iterationState;
        },
        { id: 'init-iteration-state' },
      )
      // Run the agentic loop with dowhile
      .dowhile(singleIterationWorkflow, async params => {
        const { inputData } = params;
        const state = inputData as IterationState;
        const pubsub = (params as any)[PUBSUB_SYMBOL] as PubSub | undefined;

        if (globalRunRegistry.get(state.runId)?.abortSignal?.aborted) {
          state.terminalToolResult = undefined;
          state.deferredStepFinishChunk = undefined;
          if (state.lastStepResult) {
            state.lastStepResult.reason = 'abort';
            state.lastStepResult.isContinued = false;
          }
          return false;
        }

        // Flush ordinary intermediate step-finish chunks here. Terminal data
        // stays in state until the memoized final map: Inngest may re-evaluate
        // a loop condition during replay, and caller-facing output must not be
        // published from that replayable predicate.
        if (pubsub && !state.terminalToolResult) {
          if (state.deferredStepFinishChunk) {
            await emitChunkEvent(pubsub, state.runId, state.deferredStepFinishChunk as any);
          }
          state.deferredStepFinishChunk = undefined;
        }

        if (state.terminalToolResult) {
          if (state.lastStepResult) state.lastStepResult.isContinued = false;
          return false;
        }

        // Check if we should continue
        const shouldContinue = state.lastStepResult?.isContinued === true;
        // Use maxSteps from options (per-request), falling back to workflow-level default
        const effectiveMaxSteps = state.options?.maxSteps ?? maxSteps;
        const underMaxSteps = state.iterationCount < effectiveMaxSteps;

        return shouldContinue && underMaxSteps;
      })
      // Map final state to output format, close agent span, and emit finish event
      .map(
        async params => {
          const { inputData, mastra, requestContext } = params;
          const state = inputData as IterationState;
          const initData = params.getInitData?.() as DurableAgenticWorkflowInput | undefined;

          // Abort may land after the loop predicate but before this final map.
          // It still wins over the not-yet-published terminal candidate.
          if (globalRunRegistry.get(state.runId)?.abortSignal?.aborted) {
            state.terminalToolResult = undefined;
            state.deferredStepFinishChunk = undefined;
            if (state.lastStepResult) {
              state.lastStepResult.reason = 'abort';
              state.lastStepResult.isContinued = false;
            }
          }

          // Access pubsub via symbol to emit finish event
          const pubsub = (params as any)[PUBSUB_SYMBOL] as PubSub | undefined;

          // Extract final text from last step
          const lastStep = state.accumulatedSteps[state.accumulatedSteps.length - 1];
          const finalText = lastStep?.text;

          // Persist only after the Inngest loop has committed to terminal
          // delivery. The mapping result can still lose to background work or
          // a signal in engines that support those precedence lanes.
          const terminalEnvelope = state.terminalToolResult
            ? createTerminalToolResultEnvelope(state.runId, state.accumulatedSteps.length, state.terminalToolResult)
            : undefined;
          if (terminalEnvelope) {
            state.terminalToolResult = terminalEnvelope.data;
          }

          // Inngest may finalize on a different worker than the one that ran
          // the last model/tool step. Rebuild memory and output processors from
          // the registered agent before final persistence instead of relying on
          // the process-local run registry. This mirrors the core durable
          // finalizer and makes the terminal marker recallable after restart.
          const resolvedRuntime =
            initData?.agentId && initData.runId
              ? await resolveRuntimeDependencies({
                  mastra: mastra as any,
                  runId: initData.runId,
                  agentId: initData.agentId,
                  input: { ...initData, messageListState: state.messageListState },
                  logger: mastra?.getLogger?.(),
                })
              : undefined;
          const finalMessageList =
            resolvedRuntime?.messageList ?? new MessageList().deserialize(state.messageListState);

          if (terminalEnvelope) {
            finalMessageList.add(
              {
                id: state.messageId,
                role: 'assistant',
                content: {
                  format: 2,
                  parts: [{ type: 'data-terminal-tool-result', ...terminalEnvelope }],
                },
                createdAt: new Date(),
              } as any,
              'response',
            );
          }
          state.messageListState = finalMessageList.serialize();

          const finalRequestContext = createDurableRuntimeRequestContext({
            entries: initData?.requestContextEntries,
            state: initData?.state ?? state.state,
            liveContext: requestContext as RequestContext | undefined,
          });
          const logger = mastra?.getLogger?.();
          const durableState = state.state;
          const terminalPersistenceOwned = outputProcessorsOwnTerminalPersistence(resolvedRuntime?.outputProcessors);
          const terminalMemoryPersistenceExpected = Boolean(
            terminalEnvelope &&
            durableState.memoryConfigured &&
            durableState.threadId &&
            durableState.resourceId &&
            !durableState.memoryConfig?.readOnly,
          );
          if (terminalMemoryPersistenceExpected && !terminalPersistenceOwned && !resolvedRuntime?.memory) {
            throw new Error(
              `[InngestDurableAgent] Cannot deliver terminal result for run "${state.runId}" because configured memory could not be rehydrated.`,
            );
          }
          if (terminalMemoryPersistenceExpected && durableState?.observationalMemory && !terminalPersistenceOwned) {
            throw new Error(
              `[InngestDurableAgent] Cannot deliver terminal result for run "${state.runId}" because observational-memory persistence could not be rehydrated.`,
            );
          }
          if (terminalMemoryPersistenceExpected && !terminalPersistenceOwned && !resolvedRuntime?.saveQueueManager) {
            throw new Error(
              `[InngestDurableAgent] Cannot deliver terminal result for run "${state.runId}" because the memory save queue could not be rehydrated.`,
            );
          }

          if (resolvedRuntime?.outputProcessors?.length) {
            try {
              const runner = new ProcessorRunner({
                inputProcessors: resolvedRuntime.inputProcessors ?? [],
                outputProcessors: resolvedRuntime.outputProcessors,
                errorProcessors: resolvedRuntime.errorProcessors ?? [],
                logger: logger as any,
                agentName: initData?.agentName ?? initData?.agentId ?? state.agentName ?? state.agentId,
                processorStates: resolvedRuntime.processorStates,
              });
              await runner.runOutputProcessors(finalMessageList, undefined, finalRequestContext, 0);
            } catch (error) {
              logger?.warn?.(`[InngestDurableAgent] Error running output processors: ${error}`);
              if (terminalEnvelope) throw error;
            }
          }
          if (terminalEnvelope) {
            assertTerminalToolResultRetained(finalMessageList, state.messageId, terminalEnvelope);
          }
          state.messageListState = finalMessageList.serialize();

          if (
            resolvedRuntime?.saveQueueManager &&
            resolvedRuntime.memory &&
            durableState?.threadId &&
            durableState?.resourceId &&
            !durableState.observationalMemory &&
            (!terminalEnvelope || !terminalPersistenceOwned) &&
            !durableState.memoryConfig?.readOnly
          ) {
            try {
              if (!durableState.threadExists) {
                await resolvedRuntime.memory.createThread?.({
                  threadId: durableState.threadId,
                  resourceId: durableState.resourceId,
                  memoryConfig: durableState.memoryConfig,
                });
                durableState.threadExists = true;
              }
              await (terminalEnvelope
                ? resolvedRuntime.saveQueueManager.flushMessagesStrict(
                    finalMessageList,
                    durableState.threadId,
                    durableState.memoryConfig,
                  )
                : resolvedRuntime.saveQueueManager.flushMessages(
                    finalMessageList,
                    durableState.threadId,
                    durableState.memoryConfig,
                  ));
            } catch (error) {
              logger?.warn?.(`[InngestDurableAgent] Error persisting messages: ${error}`);
              if (terminalEnvelope) throw error;
            }
          }

          const finalOutput = {
            messageListState: state.messageListState,
            messageId: state.messageId,
            stepResult: state.lastStepResult || {
              reason: 'stop',
              warnings: [],
              isContinued: false,
            },
            output: {
              text: finalText,
              usage: state.accumulatedUsage,
              steps: state.accumulatedSteps,
            },
            state: state.state,
            ...(state.terminalToolResult ? { terminalToolResult: state.terminalToolResult } : {}),
          };

          // End MODEL_GENERATION span with final output (children before parent)
          // This span was created BEFORE the workflow started and stayed open for all iterations
          const observability = mastra?.observability?.getSelectedInstance({});
          if (state.modelSpanData) {
            const modelSpan = observability?.rebuildSpan(state.modelSpanData);
            modelSpan?.end({
              output: {
                text: finalText,
                usage: state.accumulatedUsage,
              },
              attributes: {
                finishReason: state.lastStepResult?.reason || 'stop',
              },
            });
          }

          // End AGENT_RUN span with final output
          if (state.agentSpanData) {
            const agentSpan = observability?.rebuildSpan(state.agentSpanData);
            agentSpan?.end({
              output: finalOutput.output,
            });
          }

          // Emit finish event via pubsub
          if (pubsub) {
            const deferredTerminalStep = terminalEnvelope ? state.deferredStepFinishChunk : undefined;
            const terminalStepFinishChunk =
              deferredTerminalStep && typeof deferredTerminalStep === 'object'
                ? {
                    ...deferredTerminalStep,
                    payload: {
                      ...((deferredTerminalStep as { payload?: object }).payload ?? {}),
                      messageId: state.messageId,
                    },
                  }
                : deferredTerminalStep;
            if (terminalEnvelope) state.deferredStepFinishChunk = undefined;
            await emitFinishEvent(pubsub, state.runId, {
              output: finalOutput.output,
              stepResult: finalOutput.stepResult,
              ...(terminalEnvelope ? { terminalToolResult: terminalEnvelope } : {}),
              ...(terminalStepFinishChunk ? { terminalStepFinishChunk: terminalStepFinishChunk as any } : {}),
            });
          }

          return finalOutput;
        },
        { id: 'map-final-output' },
      )
      .commit()
  );
}
