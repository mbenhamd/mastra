import { z } from 'zod';
import { ErrorCategory, ErrorDomain, MastraError } from '../../../error';
import type { MastraScorer, MastraScorerEntry } from '../../../evals/base';
import { runScorer } from '../../../evals/hooks';
import type { PubSub } from '../../../events/pubsub';
import { validateMaxSteps, validateRecoveryMaxSteps } from '../../../llm/model/max-steps';
import type { IMastraLogger } from '../../../logger';
import { outputProcessorsOwnTerminalPersistence } from '../../../loop/shared/terminal-tool-result';
import { pruneAgentLoopSnapshot } from '../../../loop/workflows/prune-snapshot';
import type { Mastra } from '../../../mastra';
import { createObservabilityContext, InternalSpans } from '../../../observability';
import type { AIModelGenerationSpan, ExportedSpan, SpanType } from '../../../observability';
import { RequestContext } from '../../../request-context';
import { PUBSUB_SYMBOL } from '../../../workflows/constants';
import { createWorkflow } from '../../../workflows/create';
import type { WorkflowOptions } from '../../../workflows/types';
import { AGENT_RESPONSE_RECOVERY_CONTINUATION } from '../../merge-execution-options';
import { MessageList } from '../../message-list';
import {
  TOOL_PERMISSION_POLICY_KEY,
  TOOL_PERMISSION_POLICY_REQUIRED_KEY,
  TOOL_PERMISSION_POLICY_STABLE_KEY,
} from '../../tool-permission-prefilter';
import type { ToolPermissionPolicy } from '../../tool-permission-prefilter';
import { DurableStepIds, DurableAgentDefaults } from '../constants';
import { getBoundRunRegistryEntry, globalRunRegistry } from '../run-registry';
import {
  assertTerminalToolResultRetained,
  createTerminalToolResultEnvelope,
  emitChunkEvent,
  emitFinishEvent,
  emitIterationCompleteEvent,
} from '../stream-adapter';
import type {
  DurableToolCallInput,
  DurableAgenticWorkflowInput,
  DurableAgenticExecutionOutput,
  DurableLLMStepOutput,
  DurableToolCallOutput,
} from '../types';
import { createDurableRuntimeRequestContext } from '../utils/resolve-runtime';
import { mapDurableIterationToLLMInput } from './map-llm-input';
import {
  modelConfigSchema,
  modelListEntrySchema,
  durableAgenticOutputSchema,
  baseIterationStateSchema,
  createBaseIterationStateUpdate,
  resolveDurableToolCallConcurrency,
  executeDurableAgentScorers,
} from './shared';
import {
  createDurableBackgroundTaskCheckStep,
  createDurableGoalStep,
  createDurableIsTaskCompleteStep,
  createDurableLLMExecutionStep,
  createDurableToolCallStep,
  createDurableLLMMappingStep,
  createDurableSignalDrainStep,
} from './steps';

/**
 * Options for creating a durable agentic workflow
 */
export interface DurableAgenticWorkflowOptions {
  /** Maximum number of agentic loop iterations */
  maxSteps?: number;
  /** Internal response-recovery calls available beyond maxSteps. */
  recoveryMaxSteps?: number;
  /** Server-side lifecycle callback for the outer durable-agent workflow. */
  onFinish?: WorkflowOptions['onFinish'];
}

/**
 * Input schema for the durable agentic workflow.
 * Extends base schema with model list for fallback support.
 */
const durableAgenticInputSchema = z.object({
  __workflowKind: z.literal('durable-agent'),
  runId: z.string(),
  // Optional for workflows persisted before runtime registry bindings existed.
  runtimeBindingId: z.string().optional(),
  agentId: z.string(),
  agentName: z.string().optional(),
  versions: z.any().optional(),
  hasProcessors: z.boolean().optional(),
  runtimeBindings: z.any().optional(),
  runtimeResolution: z.literal('registry-required').optional(),
  messageListState: z.any(),
  toolsMetadata: z.array(z.any()),
  modelConfig: modelConfigSchema,
  // Model list for fallback support (when agent configured with array of models)
  modelList: z.array(modelListEntrySchema).optional(),
  options: z.any(),
  responseRecovery: z
    .object({
      phase: z.literal('reserved'),
      reservedAtIteration: z.number().int().nonnegative(),
      modelEntryId: z.string().optional(),
    })
    .optional(),
  state: z.any(),
  messageId: z.string(),
  // Exported AGENT_RUN / MODEL_GENERATION span data, threaded so the run shares one trace
  agentSpanData: z.any().optional(),
  modelSpanData: z.any().optional(),
  // JSON-safe snapshot of requestContext.entries() so durable steps can read
  // it (e.g. is-task-complete scorers pass it as customContext).
  requestContextEntries: z.record(z.string(), z.any()).optional(),
  requiredRequestContextCapabilities: z
    .object({
      authToken: z.literal(true).optional(),
    })
    .optional(),
});

// Re-export shared output schema (identical across implementations)
// Note: durableAgenticOutputSchema is imported from shared

/**
 * Schema for the iteration state that flows through the dowhile loop.
 * Extends base schema with model list for fallback support.
 */
const iterationStateSchema = baseIterationStateSchema.extend({
  // Model list for fallback support
  modelList: z.array(z.any()).optional(),
});

type IterationState = z.infer<typeof iterationStateSchema>;

/**
 * Create a durable agentic workflow.
 *
 * This workflow implements the agentic loop pattern in a durable way:
 *
 * 1. LLM Execution Step - Calls the LLM and gets response/tool calls
 * 2. Tool Call Steps (foreach) - Executes each tool call in parallel
 * 3. LLM Mapping Step - Merges tool results back into state
 * 4. Loop - Continues if more tool calls are needed (dowhile)
 *
 * All state flows through workflow input/output, making it durable across
 * process restarts and execution engine replays.
 */
export function createDurableAgenticWorkflow(options?: DurableAgenticWorkflowOptions, logger?: IMastraLogger) {
  validateMaxSteps(options?.maxSteps, logger);
  validateRecoveryMaxSteps(options?.recoveryMaxSteps, logger);

  const maxSteps = options?.maxSteps ?? DurableAgentDefaults.MAX_STEPS;
  const defaultRecoveryMaxSteps = options?.recoveryMaxSteps ?? 0;

  // Create the LLM execution step - tools and model are resolved from Mastra at runtime
  const llmExecutionStep = createDurableLLMExecutionStep();

  // Create the tool call step - each tool call runs as its own step with suspend support
  const toolCallStep = createDurableToolCallStep();

  // Create the LLM mapping step
  const llmMappingStep = createDurableLLMMappingStep();

  // Create the background task check step
  const backgroundTaskCheckStep = createDurableBackgroundTaskCheckStep();

  // Create the signal drain step — mirrors the non-durable `signalDrainStep`
  // which drains signals queued during tool execution.
  const signalDrainStep = createDurableSignalDrainStep();

  // Create the isTaskComplete evaluation step (mirrors the non-durable
  // createIsTaskCompleteStep). Lives as a real step (not predicate logic)
  // so it shows up in workflow traces and produces a proper state transition.
  const isTaskCompleteStep = createDurableIsTaskCompleteStep(maxSteps);

  // Create the goal evaluation step — mirrors the non-durable
  // `createGoalStep`. Runs after isTaskComplete so the goal judge
  // sees whether isTaskComplete already stopped the loop.
  const goalStep = createDurableGoalStep();

  // Create the single iteration workflow (LLM -> Tool Calls -> Mapping)
  // Note: tool-call foreach concurrency is resolved per run at execution time
  // (see resolveDurableToolCallConcurrency) — approval/suspend flows force
  // sequential execution; otherwise the run's `toolCallConcurrency` applies.
  // The workflow is created once at startup and reused for all runs.
  const singleIterationWorkflow = createWorkflow({
    id: DurableStepIds.AGENTIC_EXECUTION,
    inputSchema: iterationStateSchema,
    outputSchema: iterationStateSchema,
    options: {
      shouldPersistSnapshot: params => {
        // We need a persisted snapshot record to support both:
        //  - `resumeStream()` after a suspend (records with status
        //    `pending` / `paused` / `suspended`)
        //  - boot-time recovery of orphaned RUNNING runs after a process
        //    restart, via `DurableAgent.recoverActiveRuns()` — this requires
        //    the row to actually be stamped `running` while the loop is
        //    in-flight (issue #19056).
        //
        // The engine's persist path guards against overwriting a `suspended`
        // / `paused` snapshot with a later `running` update from the same
        // run (see `persistStepUpdate` in workflows/handlers/entry.ts), so
        // it is safe to return true for `running` here.
        return (
          params.workflowStatus === 'pending' ||
          params.workflowStatus === 'paused' ||
          params.workflowStatus === 'suspended' ||
          params.workflowStatus === 'running'
        );
      },
      // Agent-loop snapshots are pure resume artifacts — strip everything a
      // resume never reads before persisting.
      pruneSnapshot: pruneAgentLoopSnapshot,
      validateInputs: false,
      sharePubsub: true,
      // Internal durable-agent execution plumbing — hide workflow spans;
      // the agent/tool/model spans within still surface for users.
      tracingPolicy: {
        internal: InternalSpans.WORKFLOW,
      },
    },
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
    // Step 2: Extract tool calls as array for foreach (forward model_step span for nesting)
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
    // Concurrency is resolved per run from the serialized iteration state:
    // approval/suspend-capable tool sets run sequentially, everything else
    // honors the run's `toolCallConcurrency` (default 10). The workflow graph
    // is shared across runs, so this must be a resolver — never a mutated
    // shared options object.
    .foreach(toolCallStep, {
      concurrency: ({ inputData, getInitData, requestContext }) => {
        const state = getInitData() as IterationState | undefined;
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
    // Step 4: Collect tool results and bundle with LLM output for mapping step
    .map(
      async ({ inputData, getStepResult, getInitData }) => {
        const toolResults = inputData as DurableToolCallOutput[];
        const llmOutput = getStepResult(llmExecutionStep.id) as DurableLLMStepOutput;
        const initData = getInitData() as IterationState;

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
    // Step 6.5: Drain signals that were queued while tool execution was running
    // within this iteration. Mirrors the non-durable `signalDrainStep` which
    // sits between backgroundTaskCheckStep and isTaskCompleteStep.
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

        // Extend with core-specific fields
        const newIterationState: IterationState = {
          ...baseUpdate,
          modelList: initData.modelList,
        };

        return newIterationState;
      },
      { id: 'update-iteration-state' },
    )
    // Step 8: Evaluate user-supplied isTaskComplete scorers (if any). Runs as
    // a real step so it shows up in traces and may mutate lastStepResult /
    // messageListState before the dowhile predicate decides whether to loop
    // again. No-op when the run has no policy configured.
    .then(isTaskCompleteStep)
    // Step 9: Goal evaluation. Mirrors the non-durable createGoalStep — judges
    // whether the thread's active objective is satisfied or should continue.
    // No-op when no goal is configured or no active objective exists.
    .then(goalStep)
    .commit();

  // Create the main agentic loop workflow with dowhile
  return (
    createWorkflow({
      id: DurableStepIds.AGENTIC_LOOP,
      inputSchema: durableAgenticInputSchema,
      outputSchema: durableAgenticOutputSchema,
      options: {
        shouldPersistSnapshot: params => {
          // See the singleIterationWorkflow comment above — same policy for
          // the outer loop. The persist path guards against overwriting a
          // suspended snapshot with running.
          return (
            params.workflowStatus === 'pending' ||
            params.workflowStatus === 'paused' ||
            params.workflowStatus === 'suspended' ||
            params.workflowStatus === 'running'
          );
        },
        // Agent-loop snapshots are pure resume artifacts — strip everything a
        // resume never reads before persisting.
        pruneSnapshot: pruneAgentLoopSnapshot,
        validateInputs: false,
        onFinish: options?.onFinish,
        // Internal durable-agent execution plumbing — see singleIterationWorkflow.
        tracingPolicy: {
          internal: InternalSpans.WORKFLOW,
        },
      },
    })
      // Initialize iteration state from input
      .map(
        async ({ inputData }) => {
          const input = inputData as DurableAgenticWorkflowInput;
          validateMaxSteps(input.options?.maxSteps, logger);
          validateRecoveryMaxSteps(input.options?.recoveryMaxSteps, logger);
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
          };
          return iterationState;
        },
        { id: 'init-iteration-state' },
      )
      // Run the agentic loop with dowhile
      .dowhile(singleIterationWorkflow, async params => {
        const { inputData, mastra } = params;
        const state = inputData as IterationState;
        const initData = params.getInitData() as DurableAgenticWorkflowInput;
        const pubsub = (params as any)[PUBSUB_SYMBOL] as PubSub | undefined;
        const registryEntry = globalRunRegistry.get(state.runId);

        // ── Abort check ────────────────────────────────────────────────
        // If the abort signal has fired, stop the loop immediately.
        // The llm-execution step may have already emitted the ABORT event
        // and returned a clean output, but the signal may also have fired
        // between steps (e.g. inside a tool). Override the stepResult
        // reason so the FINISH event carries 'abort' and the client sees
        // the correct finishReason.
        if (registryEntry?.abortSignal?.aborted) {
          state.terminalToolResult = undefined;
          state.deferredStepFinishChunk = undefined;
          if (state.lastStepResult) {
            state.lastStepResult.reason = 'abort';
            state.lastStepResult.isContinued = false;
          }
          return false;
        }

        // Two-phase stop: if onIterationComplete returned { continue: false, feedback }
        // on the previous iteration, we allowed one more LLM turn with that feedback.
        // Now that the turn has completed, stop the loop unconditionally.
        let hasFinishedSteps = false;
        // Hard-stop tracks reasons that onIterationComplete must NOT override.
        // pendingFeedbackStop and delegationBailed are unconditional stops.
        let hardStop = false;
        if (state.pendingFeedbackStop) {
          hasFinishedSteps = true;
          hardStop = true;
          state.pendingFeedbackStop = false;
        }

        // Continuation check. isTaskComplete (when configured) runs as a
        // proper step inside singleIterationWorkflow and may have already
        // flipped lastStepResult.isContinued by the time we get here.
        // Declared as `let` because signal drain may force isContinued later.
        let shouldContinue = state.lastStepResult?.isContinued === true;
        const runMaxSteps = state.options?.maxSteps ?? maxSteps;
        const recoveryMaxSteps = state.options?.recoveryMaxSteps ?? defaultRecoveryMaxSteps;
        const underMaxSteps = state.iterationCount < runMaxSteps;

        // Evaluate user-supplied stopWhen predicate(s) parked on the registry
        // up-front so we can include them in the finality decision emitted on
        // the iteration-complete event. The predicate is a closure and can't
        // survive the wire, so we read it from in-process state. Cross-process
        // engines (Inngest after worker restart) won't have the registry entry
        // and fall back to maxSteps only.
        let stopWhenMatched = false;
        if (shouldContinue && !hasFinishedSteps && !state.terminalToolResult) {
          const stopWhen = registryEntry?.stopWhen;
          if (stopWhen && state.accumulatedSteps.length > 0) {
            const conditions = Array.isArray(stopWhen) ? stopWhen : [stopWhen];
            // Mirror agentic-loop: cast steps to any for v5/v6 StopCondition shape
            // compatibility — the StepRecord we accumulate is sufficient at runtime.
            const steps = state.accumulatedSteps as any;
            const results = await Promise.all(conditions.map(condition => condition({ steps })));
            stopWhenMatched = results.some(Boolean);
          }
        }

        if (stopWhenMatched) {
          hasFinishedSteps = true;
        }

        // Check if a delegation hook called ctx.bail() during this iteration.
        // The flag was set by the mapping step and propagated via iteration state.
        const delegationBailed = !!(state as any).delegationBailed;
        if (delegationBailed) {
          hasFinishedSteps = true;
          hardStop = true;
          // Reset the flag so it doesn't carry forward
          (state as any).delegationBailed = false;
        }

        // ── Inter-iteration signal drain ──────────────────────────────
        // Mirror the non-durable agentic-loop predicate: drain pending
        // signals that were queued while the previous iteration was
        // running. If signals are present, mark a response boundary,
        // rotate the messageId, add them to the transcript, emit them
        // to the stream, and force continuation so the LLM sees them.
        if (pubsub && registryEntry?.drainPendingSignals) {
          try {
            const pendingSignals = registryEntry.drainPendingSignals('pending');
            if (pendingSignals.length > 0) {
              const drainList = new MessageList();
              drainList.deserialize(state.messageListState);
              drainList.markResponseMessageBoundary();

              const nextMessageId =
                (mastra as Mastra | undefined)?.generateId?.() ??
                globalThis.crypto?.randomUUID?.() ??
                `msg_${Date.now()}`;
              state.messageId = nextMessageId;

              for (const pendingSignal of pendingSignals) {
                const signalForTranscript = drainList.addSignal(pendingSignal);
                await emitChunkEvent(pubsub, state.runId, signalForTranscript.toDataPart() as any);
              }

              state.messageListState = drainList.serialize();

              // A queued signal wins over a just-settled terminal tool result.
              // The model must process the new signal before this run can end.
              state.terminalToolResult = undefined;

              // Force continuation — the LLM must see the injected signals
              if (state.lastStepResult) {
                state.lastStepResult.isContinued = true;
              }
              shouldContinue = true;
            }
          } catch {
            // Signal drain is best-effort; if deserialization fails
            // the next iteration still runs with the un-drained state.
            // drainPendingSignals() is inside the try so signals remain
            // queued if the drain function itself throws.
          }
        }

        if (state.terminalToolResult) {
          hasFinishedSteps = true;
          hardStop = true;
        }

        let isFinal = !shouldContinue || !underMaxSteps || hasFinishedSteps;

        // Call onIterationComplete hook if provided (for every iteration, not
        // just continued ones). Mirrors the regular agentic-loop predicate:
        // the handler can return { continue: false } to stop, { continue: true }
        // to force-continue (if under maxSteps), and/or { feedback } to inject
        // a message before the next turn.
        const onIterationComplete = registryEntry?.onIterationComplete;
        if (onIterationComplete && !state.backgroundTaskPending) {
          const lastStep = state.accumulatedSteps[state.accumulatedSteps.length - 1];

          try {
            // Deserialize messageList for the callback's messages snapshot
            const callbackMessageList = new MessageList();
            try {
              callbackMessageList.deserialize(state.messageListState);
            } catch {
              // If deserialization fails, callback sees empty messages
            }

            const iterationContext = {
              iteration: state.accumulatedSteps.length,
              maxIterations: runMaxSteps,
              text: lastStep?.text ?? '',
              toolCalls: (lastStep?.toolCalls ?? []).map((tc: any) => ({
                id: tc.toolCallId || tc.id || '',
                name: tc.toolName || tc.name || '',
                args: (tc.args || {}) as Record<string, unknown>,
              })),
              toolResults: (lastStep?.toolResults ?? []).map((tr: any) => ({
                id: tr.toolCallId || tr.id || '',
                name: tr.toolName || tr.name || '',
                result: tr.result,
                error: tr.error,
              })),
              isFinal,
              finishReason: lastStep?.finishReason ?? 'unknown',
              runId: state.runId,
              threadId: initData.state?.threadId,
              resourceId: initData.state?.resourceId,
              agentId: state.agentId,
              agentName: state.agentName ?? state.agentId,
              messages: callbackMessageList.get.all.db(),
            };

            const iterationResult = await onIterationComplete(iterationContext);

            if (iterationResult) {
              // Determine whether we can run another turn. Hard stops
              // (pendingFeedbackStop, delegationBailed) are unconditional —
              // onIterationComplete cannot override them.
              const responseRecoveryRequested =
                iterationResult.continue === true && iterationResult[AGENT_RESPONSE_RECOVERY_CONTINUATION] === true;
              const canUseRecoveryTurn =
                responseRecoveryRequested &&
                !stopWhenMatched &&
                recoveryMaxSteps > 0 &&
                state.iterationCount >= runMaxSteps &&
                state.iterationCount < runMaxSteps + recoveryMaxSteps;
              const canRunAnotherTurn =
                !hardStop &&
                (underMaxSteps || canUseRecoveryTurn) &&
                (shouldContinue || iterationResult.continue === true);

              if (responseRecoveryRequested && canRunAnotherTurn) {
                state.responseRecovery = {
                  phase: 'reserved',
                  reservedAtIteration: state.iterationCount,
                  ...(state.lastModelEntryId ? { modelEntryId: state.lastModelEntryId } : {}),
                };
              }

              if (iterationResult.feedback && canRunAnotherTurn) {
                // Inject feedback as a synthetic assistant message so the LLM
                // sees it on the next turn. Mirror the regular agent: mark it
                // with completionResult.suppressFeedback so isTaskComplete
                // scorers skip it.
                const feedbackId =
                  (mastra as Mastra | undefined)?.generateId?.() ??
                  globalThis.crypto?.randomUUID?.() ??
                  `msg_${Date.now()}`;
                callbackMessageList.add(
                  {
                    id: feedbackId,
                    createdAt: new Date(),
                    type: 'text',
                    role: 'assistant',
                    content: {
                      parts: [{ type: 'text', text: iterationResult.feedback }],
                      metadata: {
                        mode: 'stream',
                        completionResult: { suppressFeedback: true },
                      },
                      format: 2,
                    },
                  } as any,
                  'response',
                );
                // Re-serialize the updated messageList
                state.messageListState = callbackMessageList.serialize();

                if (iterationResult.continue === false) {
                  // Two-phase stop: let one more LLM turn run with the feedback,
                  // then stop on the next predicate evaluation.
                  state.pendingFeedbackStop = true;
                  isFinal = false;
                } else if (!hasFinishedSteps && (underMaxSteps || canUseRecoveryTurn)) {
                  isFinal = false;
                  if (state.lastStepResult) {
                    state.lastStepResult.isContinued = true;
                  }
                }
              } else if (iterationResult.continue === false && !hasFinishedSteps) {
                hasFinishedSteps = true;
                isFinal = true;
              } else if (iterationResult.continue === true && !hardStop && (hasFinishedSteps || !shouldContinue)) {
                if (underMaxSteps || !runMaxSteps || canUseRecoveryTurn) {
                  hasFinishedSteps = false;
                  isFinal = false;
                  if (state.lastStepResult) {
                    state.lastStepResult.isContinued = true;
                  }
                }
              }
            }
          } catch (error) {
            // Log error but don't fail the iteration
            const logger = (mastra as Mastra | undefined)?.getLogger?.();
            logger?.error('Error in onIterationComplete hook:', error);
          }
        }

        // stopWhen/onIterationComplete can abort after the first arbitration
        // check. Re-check before any deferred terminal chunk can be committed.
        if (registryEntry?.abortSignal?.aborted) {
          state.terminalToolResult = undefined;
          state.deferredStepFinishChunk = undefined;
          state.responseRecovery = undefined;
          hasFinishedSteps = true;
          hardStop = true;
          isFinal = true;
          if (state.lastStepResult) {
            state.lastStepResult.reason = 'abort';
            state.lastStepResult.isContinued = false;
          }
        }

        if (isFinal && state.lastStepResult) {
          state.lastStepResult.isContinued = false;
        }

        // Rotate messageId for the next iteration. Each iteration's assistant
        // response is a distinct message, mirroring the non-durable agentic
        // loop which calls rotateResponseMessageId() between iterations. The
        // mutated state.messageId flows into the next singleIterationWorkflow
        // input via map-to-llm-input.
        //
        // We also mark the current MessageList's last assistant message as a
        // response boundary so MessageMerger won't collapse the next
        // iteration's assistant content into it. Without this, persisted
        // memory keeps a single assistant message and the rotated id is never
        // observable to consumers.
        if (!isFinal) {
          const nextMessageId =
            (mastra as Mastra | undefined)?.generateId?.() ?? globalThis.crypto?.randomUUID?.() ?? `msg_${Date.now()}`;
          state.messageId = nextMessageId;

          try {
            const boundaryList = new MessageList();
            boundaryList.deserialize(state.messageListState);
            boundaryList.markResponseMessageBoundary();
            state.messageListState = boundaryList.serialize();
          } catch {
            // Boundary marking is best-effort; if deserialization fails the
            // next iteration will still run with the un-marked state.
          }
        }

        // Emit an iteration-complete event for observability. This fires after
        // every iteration (including the last one) so client-side callbacks
        // (via stream-adapter) can track progress. The in-process callback
        // above has already been evaluated and its result applied to the
        // continuation decision.
        if (pubsub) {
          const lastStep = state.accumulatedSteps[state.accumulatedSteps.length - 1];
          // A terminal iteration keeps step-finish deferred until the durable
          // finalization step. Loop conditions can be re-evaluated during
          // replay, so publishing the terminal result from here could duplicate
          // a caller-facing answer. Ordinary iterations still flush before the
          // next model call.
          if (!state.terminalToolResult && state.deferredStepFinishChunk) {
            try {
              await emitChunkEvent(pubsub, state.runId, state.deferredStepFinishChunk as any);
            } catch (error) {
              (mastra as Mastra | undefined)
                ?.getLogger?.()
                ?.warn?.(`[DurableAgent] Failed to emit deferred step-finish: ${error}`);
            }
          }
          await emitIterationCompleteEvent(pubsub, state.runId, {
            iteration: state.iterationCount,
            maxIterations: runMaxSteps,
            text: lastStep?.text,
            toolCalls: lastStep?.toolCalls,
            toolResults: lastStep?.toolResults,
            isFinal,
            finishReason: lastStep?.finishReason,
            runId: state.runId,
            threadId: initData.state?.threadId,
            resourceId: initData.state?.resourceId,
            agentId: initData.agentId,
            agentName: initData.agentName,
          });
        }
        if (!state.terminalToolResult) {
          state.deferredStepFinishChunk = undefined;
        }

        return !isFinal;
      })
      // Map final state to output format, run output processors, persist memory, emit finish
      .map(
        async params => {
          const { inputData, mastra, requestContext, tracingContext } = params;
          const state = inputData as IterationState;
          const initData = params.getInitData() as DurableAgenticWorkflowInput;

          const pubsub = (params as any)[PUBSUB_SYMBOL] as PubSub | undefined;
          const logger = mastra?.getLogger?.();

          // Extract final text from last step
          const lastStep = state.accumulatedSteps[state.accumulatedSteps.length - 1];
          let finalText = lastStep?.text;

          // Run output processors (processOutputResult) if available
          const registryEntry = getBoundRunRegistryEntry(state.runId, state.runtimeBindingId);
          if (!registryEntry && state.runtimeResolution === 'registry-required') {
            throw new MastraError({
              id: 'DURABLE_AGENT_RUNTIME_REGISTRY_MISSING',
              domain: ErrorDomain.AGENT,
              category: ErrorCategory.SYSTEM,
              text: `DurableAgent runtime dependencies are unavailable for run "${state.runId}". Resume the run through DurableAgent so recovery checks can restore them.`,
              details: { agentId: state.agentId, runId: state.runId },
            });
          }

          if (registryEntry?.abortSignal?.aborted) {
            state.terminalToolResult = undefined;
            state.deferredStepFinishChunk = undefined;
            if (state.lastStepResult) {
              state.lastStepResult.reason = 'abort';
              state.lastStepResult.isContinued = false;
            }
          }

          // Only finalization may make a terminal envelope durable. Mapping is
          // still followed by background-task and signal precedence checks;
          // persisting there would leave a false terminal marker when either
          // one wins. MessageMerger de-duplicates this canonical data part if
          // the durable final map is replayed.
          const terminalEnvelope = state.terminalToolResult
            ? createTerminalToolResultEnvelope(state.runId, state.accumulatedSteps.length, state.terminalToolResult)
            : undefined;
          if (terminalEnvelope) {
            state.terminalToolResult = terminalEnvelope.data;
          }

          // Reuse the registry MessageList when present. Observational Memory's
          // live turn holds this exact object, so replacing it during finalization
          // would make the persistence owner save a stale pre-terminal list.
          const finalMessageList = registryEntry?.messageList ?? new MessageList();
          finalMessageList.deserialize(state.messageListState);
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

          const finalRequestContext = createDurableRuntimeRequestContext({
            entries: initData.requestContextEntries,
            state: initData.state,
            liveContext: requestContext,
          });

          if (registryEntry?.outputProcessors?.length) {
            try {
              const { ProcessorRunner } = await import('../../../processors/runner');
              const runner = new ProcessorRunner({
                inputProcessors: registryEntry.inputProcessors ?? [],
                outputProcessors: registryEntry.outputProcessors,
                errorProcessors: registryEntry.errorProcessors ?? [],
                logger: logger as any,
                agentName: initData.agentName ?? initData.agentId,
                processorStates: registryEntry.processorStates,
              });
              // Forward the step's tracingContext so processor_run spans parent
              // to the AGENT_RUN ancestor via ProcessorRunner's findParent walk.
              await runner.runOutputProcessors(
                finalMessageList,
                createObservabilityContext(tracingContext),
                finalRequestContext,
                0,
              );
            } catch (error) {
              logger?.warn?.(`[DurableAgent] Error running output processors: ${error}`);
              if (terminalEnvelope) throw error;
            }
          }
          if (terminalEnvelope) {
            assertTerminalToolResultRetained(finalMessageList, state.messageId, terminalEnvelope);
          }
          state.messageListState = finalMessageList.serialize();
          if (registryEntry) {
            registryEntry.messageList = finalMessageList;
          }

          // Memory persistence (executeOnFinish equivalent)
          const durableState = initData.state;
          const terminalPersistenceOwned = outputProcessorsOwnTerminalPersistence(registryEntry?.outputProcessors);
          const terminalMemoryPersistenceExpected = Boolean(
            terminalEnvelope &&
            durableState.memoryConfigured &&
            durableState.threadId &&
            durableState.resourceId &&
            !durableState.memoryConfig?.readOnly,
          );
          if (terminalMemoryPersistenceExpected && !terminalPersistenceOwned && !registryEntry?.memory) {
            throw new Error(
              `[DurableAgent] Cannot deliver terminal result for run "${state.runId}" because configured memory could not be resolved.`,
            );
          }
          if (terminalMemoryPersistenceExpected && durableState?.observationalMemory && !terminalPersistenceOwned) {
            throw new Error(
              `[DurableAgent] Cannot deliver terminal result for run "${state.runId}" because observational-memory persistence could not be resolved.`,
            );
          }
          if (terminalMemoryPersistenceExpected && !terminalPersistenceOwned && !registryEntry?.saveQueueManager) {
            throw new Error(
              `[DurableAgent] Cannot deliver terminal result for run "${state.runId}" because the memory save queue could not be resolved.`,
            );
          }
          if (
            registryEntry?.saveQueueManager &&
            registryEntry.memory &&
            durableState?.threadId &&
            durableState?.resourceId &&
            !durableState.observationalMemory &&
            (!terminalEnvelope || !terminalPersistenceOwned) &&
            // Respect readOnly memory config ("read memory but don't save new
            // messages"). Mirrors the non-durable executeOnFinish `!readOnlyMemory`
            // guard and the MessageHistory output processor's readOnly check.
            !durableState.memoryConfig?.readOnly
          ) {
            try {
              if (!durableState.threadExists) {
                await registryEntry.memory.createThread?.({
                  threadId: durableState.threadId,
                  resourceId: durableState.resourceId,
                  memoryConfig: durableState.memoryConfig,
                });
                durableState.threadExists = true;
              }

              await (terminalEnvelope
                ? registryEntry.saveQueueManager.flushMessagesStrict(
                    finalMessageList,
                    durableState.threadId,
                    durableState.memoryConfig,
                  )
                : registryEntry.saveQueueManager.flushMessages(
                    finalMessageList,
                    durableState.threadId,
                    durableState.memoryConfig,
                  ));
            } catch (error) {
              logger?.warn?.(`[DurableAgent] Error persisting messages: ${error}`);
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

          if (pubsub) {
            const deferredTerminalStep = terminalEnvelope ? state.deferredStepFinishChunk : undefined;
            const terminalStepFinishChunk =
              deferredTerminalStep && typeof deferredTerminalStep === 'object'
                ? {
                    ...deferredTerminalStep,
                    payload: {
                      ...((deferredTerminalStep as { payload?: object }).payload ?? {}),
                      // The finalizer, not an arbitrary streamed producer,
                      // authoritatively binds terminal persistence to the
                      // response message that survived retries/resume.
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

          // Title generation is an observer of an already committed response.
          // Publish FINISH first so this optional extra model call cannot add to
          // caller-visible terminal-result latency or retract a delivered answer.
          if (
            registryEntry?.generateThreadTitle &&
            durableState?.threadId &&
            durableState?.resourceId &&
            !durableState.memoryConfig?.readOnly
          ) {
            try {
              await registryEntry.generateThreadTitle({
                threadId: durableState.threadId,
                resourceId: durableState.resourceId,
                memoryConfig: durableState.memoryConfig,
                messageListState: state.messageListState,
                requestContext: finalRequestContext,
                tracingContext,
              });
            } catch (error) {
              logger?.warn?.(`[DurableAgent] Error generating thread title: ${error}`);
            }
          }

          // End MODEL_GENERATION then AGENT_RUN once at completion. After a resume the
          // originals were ended as `suspended`, so end the *resume* spans (registry override).
          try {
            const observability = (mastra as Mastra | undefined)?.observability?.getSelectedInstance({
              requestContext,
            });
            const reg = globalRunRegistry.get(initData.runId);
            const modelSpanData = reg?.resumeModelSpanData ?? state.modelSpanData ?? initData.modelSpanData;
            const agentSpanData = reg?.resumeAgentSpanData ?? initData.agentSpanData;
            if (observability) {
              if (modelSpanData) {
                const modelSpan = observability.rebuildSpan(
                  modelSpanData as ExportedSpan<SpanType.MODEL_GENERATION>,
                ) as AIModelGenerationSpan | undefined;
                modelSpan?.createTracker()?.endGeneration({
                  output: { text: finalText },
                  attributes: { finishReason: finalOutput.stepResult?.reason },
                  usage: state.accumulatedUsage,
                });
              }
              if (agentSpanData) {
                const agentSpan = observability.rebuildSpan(agentSpanData as ExportedSpan<SpanType.AGENT_RUN>);
                agentSpan?.end({ output: { text: finalText } });
              }
            }
          } catch (error) {
            logger?.warn?.(`[DurableAgent] Error ending observability spans: ${error}`);
          }

          return finalOutput;
        },
        { id: 'map-final-output' },
      )
      // Execute scorers (fire-and-forget, doesn't affect main result)
      .map(
        async ({ inputData, getInitData, mastra, requestContext, tracingContext }) => {
          executeDurableAgentScorers({
            initData: getInitData() as DurableAgenticWorkflowInput,
            finalOutput: inputData,
            mastra: mastra as Mastra | undefined,
            requestContext,
            tracingContext,
          });

          return inputData;
        },
        { id: 'execute-scorers' },
      )
      .commit()
  );
}
