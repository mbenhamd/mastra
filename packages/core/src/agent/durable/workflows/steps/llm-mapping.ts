import { z } from 'zod';
import type { PubSub } from '../../../../events/pubsub';
import {
  outputProcessorsAllowTerminalToolResult,
  resolveTerminalToolResult,
} from '../../../../loop/shared/terminal-tool-result';
import type { Mastra } from '../../../../mastra';
import { EntityType, SpanType } from '../../../../observability';
import type { ExportedSpan } from '../../../../observability';
import { ChunkFrom } from '../../../../stream/types';
import { toolPayloadTransformAllowsTerminalToolResult } from '../../../../tools/payload-transform';
import { PUBSUB_SYMBOL } from '../../../../workflows/constants';
import { createStep } from '../../../../workflows/workflow';
import { MessageList } from '../../../message-list';
import { ensureRemoteAbortListener } from '../../abort-transport';
import { DurableStepIds } from '../../constants';
import { getBoundRunRegistryEntry } from '../../run-registry';
import { emitChunkEvent } from '../../stream-adapter';
import type {
  DurableAgenticWorkflowInput,
  DurableLLMStepOutput,
  DurableToolCallOutput,
  DurableAgenticExecutionOutput,
  SerializableDurableState,
} from '../../types';
import { resolveRuntimeDependencies } from '../../utils/resolve-runtime';
import { normalizeModelOutput } from './normalize-model-output';

/**
 * Input schema for the durable LLM mapping step.
 * This combines the LLM execution output with tool call results.
 */
const durableLLMMappingInputSchema = z.object({
  llmOutput: z.any(), // DurableLLMStepOutput
  toolResults: z.array(z.any()), // DurableToolCallOutput[]
  runId: z.string(),
  agentId: z.string(),
  messageId: z.string(),
  state: z.any(), // SerializableDurableState
});

/**
 * Output schema for the durable LLM mapping step
 */
const durableLLMMappingOutputSchema = z.object({
  messageListState: z.any(),
  messageId: z.string(),
  stepResult: z.any(),
  toolResults: z.array(z.any()),
  output: z.object({
    text: z.string().optional(),
    toolCalls: z.array(z.any()).optional(),
    usage: z.any(),
    steps: z.array(z.any()),
  }),
  state: z.any(),
  delegationBailed: z.boolean().optional(),
  terminalToolResult: z.any().optional(),
  deferredStepFinishChunk: z.any().optional(),
  processorRetryCount: z.number().optional(),
  processorRetryFeedback: z.string().optional(),
  modelEntryId: z.string().optional(),
  responseRecoveryConsumed: z.boolean().optional(),
});

/**
 * Create a durable LLM mapping step.
 *
 * This step:
 * 1. Takes the LLM execution output and tool call results
 * 2. Updates the message list with tool results
 * 3. Combines everything into the final iteration output
 *
 * This is the "merge" step that combines parallel tool call results
 * back into a single coherent state.
 */
export function createDurableLLMMappingStep() {
  return createStep({
    id: DurableStepIds.LLM_MAPPING,
    inputSchema: durableLLMMappingInputSchema,
    outputSchema: durableLLMMappingOutputSchema,
    execute: async params => {
      const { inputData, mastra, requestContext, getInitData } = params;
      const {
        llmOutput,
        toolResults,
        runId: _runId,
        agentId: _agentId,
        messageId,
        state,
      } = inputData as {
        llmOutput: DurableLLMStepOutput;
        toolResults: DurableToolCallOutput[];
        runId: string;
        agentId: string;
        messageId: string;
        state: SerializableDurableState;
      };
      const durableInitData = getInitData() as DurableAgenticWorkflowInput;
      let registryEntry = getBoundRunRegistryEntry(_runId, durableInitData.runtimeBindingId);

      // 1. Deserialize message list
      const messageList = new MessageList({
        threadId: state.threadId,
        resourceId: state.resourceId,
      });
      messageList.deserialize(llmOutput.messageListState);
      const pubsub = (params as any)[PUBSUB_SYMBOL] as PubSub | undefined;
      if (pubsub) {
        try {
          await ensureRemoteAbortListener(pubsub, _runId, durableInitData.runtimeBindingId);
        } catch (error) {
          (mastra as Mastra | undefined)?.getLogger?.()?.warn?.('Failed to subscribe to cross-process abort requests', {
            runId: _runId,
            error: error instanceof Error ? error.message : String(error),
          });
        }
        registryEntry = getBoundRunRegistryEntry(_runId, durableInitData.runtimeBindingId);
      }

      // A declined approval has no `result` but is fully resolved: persist it as `output-denied`
      // with the approval decision (rather than as a successful `result`) so it round-trips on
      // recall. Mirrors the non-durable llm-mapping-step.
      const isDeniedApproval = (toolResult: { approval?: { approved?: boolean } }) =>
        toolResult?.approval?.approved === false;
      const persistInvocation = (part: Parameters<typeof messageList.updateToolInvocation>[0]) => {
        if (messageList.updateToolInvocation(part)) return;
        messageList.add(
          {
            id: crypto.randomUUID(),
            role: 'assistant',
            content: { format: 2, parts: [part] },
            createdAt: new Date(),
          },
          'response',
        );
      };

      // 2. Add tool results to message list
      // Look up tools from the in-process registry for toModelOutput support
      const registryModel = registryEntry?.model as { __metadataOnly?: boolean } | undefined;
      const needsRuntimeHydration =
        !registryEntry ||
        registryEntry.isPlaceholder === true ||
        !registryModel ||
        registryModel.__metadataOnly === true;
      if (needsRuntimeHydration && mastra) {
        await resolveRuntimeDependencies({
          mastra: mastra as Mastra,
          runId: _runId,
          agentId: _agentId,
          input: durableInitData,
          requestContext,
          logger: (mastra as Mastra).getLogger?.(),
        });
        registryEntry = getBoundRunRegistryEntry(_runId, durableInitData.runtimeBindingId);
      }
      const registryTools = registryEntry?.tools;

      // Rebuild the MODEL_STEP span early so MAPPING child spans can nest under it
      let stepSpan:
        | ReturnType<
            NonNullable<
              ReturnType<NonNullable<NonNullable<Mastra['observability']>['getSelectedInstance']>>
            >['rebuildSpan']
          >
        | undefined;
      if (llmOutput.stepSpanData) {
        try {
          const observability = (mastra as Mastra | undefined)?.observability?.getSelectedInstance({ requestContext });
          stepSpan = observability?.rebuildSpan(llmOutput.stepSpanData as ExportedSpan<SpanType.MODEL_STEP>);
        } catch {
          // Span bookkeeping must never break the merge step.
        }
      }

      if (toolResults.length > 0) {
        for (const toolResult of toolResults) {
          if (isDeniedApproval(toolResult)) {
            const deniedToolCallId = toolResult.resumeTargetToolCallId ?? toolResult.toolCallId;
            persistInvocation({
              type: 'tool-invocation' as const,
              toolInvocation: {
                state: 'output-denied' as const,
                toolCallId: deniedToolCallId,
                toolName: toolResult.toolName,
                args: toolResult.args,
                approval: {
                  id: toolResult.approval!.id,
                  approved: false,
                  reason: toolResult.approval!.reason,
                },
              },
            });
            if (deniedToolCallId !== toolResult.toolCallId) {
              persistInvocation({
                type: 'tool-invocation' as const,
                toolInvocation: {
                  state: 'result' as const,
                  toolCallId: toolResult.toolCallId,
                  toolName: toolResult.toolName,
                  args: toolResult.args,
                  result: toolResult.approval?.reason ?? 'Tool call was not approved by the user',
                },
              });
            }
            if (pubsub) {
              await emitChunkEvent(pubsub, _runId, {
                type: 'tool-result',
                runId: _runId,
                from: ChunkFrom.AGENT,
                payload: {
                  toolCallId: toolResult.toolCallId,
                  toolName: toolResult.toolName,
                  args: toolResult.args,
                  result: toolResult.approval?.reason ?? 'Tool call was not approved by the user',
                  providerMetadata: toolResult.providerMetadata as any,
                  providerExecuted: toolResult.providerExecuted,
                },
              });
            }
            continue;
          }

          const result = toolResult.error ? toolResult.error.message : toolResult.result;

          // Compute toModelOutput for successful tool results (Bug 9 parity).
          // Start from the existing providerMetadata so it's preserved even when
          // toModelOutput is absent or fails — otherwise provider-executed tools
          // or tools without a mapper lose their metadata.
          let providerMetadata: Record<string, unknown> | undefined = toolResult.providerMetadata as
            Record<string, unknown> | undefined;
          if (
            !toolResult.error &&
            toolResult.result != null &&
            !toolResult.providerExecuted &&
            !toolResult.modelOutputComputed
          ) {
            const tool = registryTools?.[toolResult.toolName] as
              { toModelOutput?: (output: unknown) => unknown } | undefined;

            if (tool?.toModelOutput) {
              const mappingSpan = stepSpan?.createChildSpan({
                type: SpanType.MAPPING,
                name: `tool output mapping: '${toolResult.toolName}'`,
                entityType: EntityType.TOOL,
                entityId: toolResult.toolName,
                entityName: toolResult.toolName,
                input: toolResult.result,
                attributes: {
                  mappingType: 'toModelOutput',
                  toolCallId: toolResult.toolCallId,
                },
              });
              try {
                let modelOutput = await tool.toModelOutput(toolResult.result);
                modelOutput = normalizeModelOutput(modelOutput);
                mappingSpan?.end({ output: modelOutput });

                // A nullish return means "no special mapping needed" — the raw result is
                // already what the model should see (see read-file.ts / sandboxToModelOutput).
                // Writing the key anyway would make the consumer in MessageList (which keys
                // off presence) override the real result with `undefined`, producing a tool
                // message with no `output`. Mirrors the non-durable llm-mapping-step.
                if (modelOutput != null) {
                  const existingMastra = (toolResult.providerMetadata as any)?.mastra;
                  providerMetadata = {
                    ...toolResult.providerMetadata,
                    mastra: { ...existingMastra, modelOutput },
                  };
                }
              } catch (err) {
                mappingSpan?.error({ error: err as Error, endSpan: true });
                // toModelOutput errors are non-fatal — the tool result is still usable
                (mastra as Mastra | undefined)
                  ?.getLogger?.()
                  ?.warn?.(`[DurableAgent] toModelOutput failed for tool "${toolResult.toolName}": ${err}`);
              }
            }
          }

          const resolvedToolCallId = toolResult.resumeTargetToolCallId ?? toolResult.toolCallId;
          persistInvocation({
            type: 'tool-invocation' as const,
            toolInvocation: {
              ...(toolResult.error
                ? { state: 'output-error' as const, errorText: toolResult.error.message }
                : { state: 'result' as const, result }),
              toolCallId: resolvedToolCallId,
              toolName: toolResult.toolName,
              args: toolResult.args,
              // Preserve the approval decision for an approved approval-gated tool so it
              // round-trips on recall as `approval: { approved: true }`.
              ...(toolResult.approval ? { approval: toolResult.approval } : {}),
            },
            ...(providerMetadata ? { providerMetadata: providerMetadata as any } : {}),
          });

          if (resolvedToolCallId !== toolResult.toolCallId) {
            persistInvocation({
              type: 'tool-invocation' as const,
              toolInvocation: {
                ...(toolResult.error
                  ? { state: 'output-error' as const, errorText: toolResult.error.message }
                  : { state: 'result' as const, result }),
                toolCallId: toolResult.toolCallId,
                toolName: toolResult.toolName,
                args: toolResult.args,
              },
              ...(providerMetadata ? { providerMetadata: providerMetadata as any } : {}),
            });
          }
        }
      }

      // 2b. Sync the updated messageList back to the in-process registry.
      // The durable workflow deserializes a fresh MessageList on every step,
      // so updates (output-denied, tool results) are invisible to other
      // steps that read from the registry — in particular tool-call.ts's
      // doFlush() which persists messages before suspension. Without this
      // sync, a declined tool's output-denied state would never reach memory
      // if the workflow re-suspends on a subsequent iteration.
      if (registryEntry) {
        registryEntry.messageList = messageList;
      }

      // 3. Determine if we should continue
      // When tool errors occur, always continue the agentic loop so the model
      // can see the error messages (already added to messageList above) and
      // self-correct. This matches the regular agent's behaviour where both
      // ToolNotFoundError and generic tool execution errors are recoverable.
      const hasToolErrors = toolResults.some(r => r.error !== undefined);
      const isContinued = hasToolErrors ? true : llmOutput.stepResult.isContinued;

      // Check if any delegation hook called ctx.bail(). The bail flag is
      // communicated via requestContext because Zod output validation strips
      // unknown fields from the tool result. We read it here and propagate
      // it on the serializable output so the dowhile predicate can stop.
      let delegationBailed = toolResults.some(toolResult => toolResult.delegationBailed === true);
      if (requestContext?.get('__mastra_delegationBailed')) {
        delegationBailed = true;
        requestContext.set('__mastra_delegationBailed', false);
      }

      // Terminal delivery bypasses the model, so fail closed whenever a
      // processor or payload transform could otherwise redact/block the tool
      // result. Those policies retain the ordinary model continuation path.
      const terminalDeliveryIsSafe =
        outputProcessorsAllowTerminalToolResult(registryEntry?.outputProcessors) &&
        toolPayloadTransformAllowsTerminalToolResult(
          registryEntry?.toolPayloadTransform,
          durableInitData.options?.transform !== undefined,
        ) &&
        llmOutput.stepResult.reason !== 'tripwire' &&
        // Terminal delivery replaces the ordinary assistant answer. A mixed
        // text+tool step has already streamed that text, so bypassing the next
        // model iteration would make live and persisted output disagree.
        !(typeof llmOutput.text === 'string' && llmOutput.text.length > 0);
      const terminalToolResult = terminalDeliveryIsSafe
        ? await resolveTerminalToolResult({
            calls: toolResults,
            tools: registryTools as any,
            runId: _runId,
            abortSignal: registryEntry?.abortSignal,
            onPolicyFailure: error =>
              (mastra as Mastra | undefined)
                ?.getLogger?.()
                ?.warn?.(`[DurableAgent][TerminalToolResult] ${error.message}`, { runId: _runId }),
          })
        : undefined;

      // The durable LLM step defers step-finish for tool-calling iterations.
      // Keep it in workflow state until signal/terminal resolution so every
      // engine emits tool-result -> terminal-result -> step-finish in order.
      const deferredChunk = llmOutput.deferredStepFinishChunk as any;
      let deferredStepFinishChunk: unknown;
      if (deferredChunk) {
        const stepContent: unknown[] = [];
        if (llmOutput.text) {
          stepContent.push({ type: 'text', text: llmOutput.text });
        }
        for (const tc of llmOutput.toolCalls ?? []) {
          stepContent.push({
            type: 'tool-call',
            toolCallId: tc.toolCallId,
            toolName: tc.toolName,
            args: tc.args,
          });
        }
        for (const tr of toolResults ?? []) {
          stepContent.push({
            type: 'tool-result',
            toolCallId: tr.toolCallId,
            toolName: tr.toolName,
            result: tr.error ? tr.error.message : tr.result,
            ...(tr.error ? { isError: true } : {}),
          });
        }
        deferredStepFinishChunk = {
          ...deferredChunk,
          payload: {
            ...deferredChunk.payload,
            _durableStepContent: stepContent,
            ...(terminalToolResult ? { terminalToolResult } : {}),
          },
        };
      }

      // 4. Build the output
      const output: DurableAgenticExecutionOutput = {
        messageListState: messageList.serialize(),
        messageId,
        stepResult: {
          ...llmOutput.stepResult,
          isContinued,
        },
        toolResults,
        output: {
          text: llmOutput.text,
          toolCalls: llmOutput.toolCalls,
          usage: llmOutput.stepResult.totalUsage ?? {
            inputTokens: 0,
            outputTokens: 0,
            totalTokens: 0,
          },
          steps: [], // Steps are accumulated at the loop level
        },
        state: {
          ...state,
          threadExists: state.threadExists,
        },
        processorRetryCount: llmOutput.processorRetryCount,
        processorRetryFeedback: llmOutput.processorRetryFeedback,
        modelEntryId: llmOutput.modelEntryId,
        responseRecoveryConsumed: llmOutput.responseRecoveryConsumed,
        delegationBailed,
        ...(terminalToolResult ? { terminalToolResult } : {}),
        ...(deferredStepFinishChunk ? { deferredStepFinishChunk } : {}),
      };

      // Close the MODEL_STEP span for tool-calling iterations: the LLM step defers it so
      // tool calls can nest under it, and the tools have now run. No-ops without tool calls.
      // The span was already rebuilt earlier so MAPPING child spans could nest under it.
      if (stepSpan) {
        try {
          const pendingPayload = llmOutput.stepFinishPayload as any;
          stepSpan.end({
            output: {
              text: llmOutput.text,
              toolCalls: llmOutput.toolCalls,
            },
            attributes: {
              usage: pendingPayload?.output?.usage,
              finishReason: pendingPayload?.stepResult?.reason,
              isContinued: pendingPayload?.stepResult?.isContinued,
            },
          });
        } catch (error) {
          // Span bookkeeping must never break the merge step.
          (mastra as Mastra | undefined)
            ?.getLogger?.()
            ?.warn?.(`[DurableAgent] Failed to close model_step span: ${error}`);
        }
      }

      return output;
    },
  });
}
