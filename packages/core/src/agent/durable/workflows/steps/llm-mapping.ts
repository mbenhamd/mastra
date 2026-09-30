import { z } from 'zod';
import type { PubSub } from '../../../../events/pubsub';
import {
  commitToolResult,
  computeModelOutputProviderMetadata,
} from '../../../../loop/shared/steps/tool-result-commit-core';
import {
  outputProcessorsAllowTerminalToolResult,
  resolveTerminalToolResult,
} from '../../../../loop/shared/terminal-tool-result';
import type { Mastra } from '../../../../mastra';
import { SpanType } from '../../../../observability';
import type { ExportedSpan } from '../../../../observability';
import { persistProcessorDataChunk } from '../../../../stream/base/output';
import { ChunkFrom } from '../../../../stream/types';
import {
  toolPayloadTransformAllowsTerminalToolResult,
  withToolPayloadTransformProviderMetadata,
} from '../../../../tools/payload-transform';
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
import { rebuildRunToolsFromMastra, resolveRuntimeDependencies } from '../../utils/resolve-runtime';

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
  modelSpanData: z.any().optional(),
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

      // 1. Deserialize message list.
      // Reuse the run's existing MessageList when the in-process registry has
      // one so external consumers holding a reference to it keep seeing state
      // updates.
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
      const messageList = (
        registryEntry?.messageList ??
        new MessageList({
          threadId: state.threadId,
          resourceId: state.resourceId,
        })
      ).deserialize(llmOutput.messageListState);

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

      // A pending client-side / HITL call: no result, no error, not provider-executed and
      // not a resolved denial. The client answers it on a follow-up request, so it must
      // stay in `call` state, must not appear as a tool result anywhere, and must end the
      // turn — the durable counterpart of the non-durable llm-mapping-step's
      // `hasPendingHITL` (issue #23295).
      //
      // Aborted calls also lack a result/error but were cancelled, not awaiting input,
      // so they must not count as a HITL suspension (mirrors the non-durable predicate's
      // `!tc.aborted`). Tripwire-blocked calls (`resultBlocked`) are resolved by policy,
      // not awaiting a client answer, so they keep the pre-existing continuation
      // semantics (isContinued follows tool errors / the model's stepResult) instead of
      // force-ending the turn.
      const isPendingClientCall = (toolResult: (typeof toolResults)[number]) =>
        toolResult.result === undefined &&
        !toolResult.error &&
        !toolResult.aborted &&
        !toolResult.resultBlocked &&
        !toolResult.providerExecuted &&
        !isDeniedApproval(toolResult) &&
        !toolResult.serverExecuted;

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
          const observability = mastra?.observability?.getSelectedInstance({ requestContext });
          stepSpan = observability?.rebuildSpan(llmOutput.stepSpanData as ExportedSpan<SpanType.MODEL_STEP>);
        } catch {
          // Span bookkeeping must never break the merge step.
        }
      }

      if (toolResults.length > 0) {
        for (const toolResult of toolResults) {
          // An aborted call was cancelled mid-flight, not completed: recording it
          // would fake-complete the call (`result: undefined` reads as success on
          // resume), so leave the invocation incomplete. Mirrors the non-durable
          // llm-mapping-step's aborted exclusion.
          if (toolResult.aborted) {
            continue;
          }

          // A processToolResult processor blocked this result via tripwire at
          // tool-call time: a tripwire chunk was emitted instead of the
          // tool-result, and no result crossed the boundary. Leave the
          // invocation incomplete, mirroring the main loop's tripwire handling
          // (commit and emission both skipped).
          if (toolResult.resultBlocked) {
            continue;
          }

          // Provider-executed results are already committed by llm-execution's
          // buildMessagesFromChunks when the result arrives in-stream; committing
          // here again would overwrite that entry with the serialized copy (and
          // clobber the providerMetadata captured from the live stream chunk).
          // A deferred provider result (no output yet) has nothing to commit.
          // Mirrors the non-durable llm-mapping-step's providerExecuted gate.
          if (toolResult.providerExecuted) {
            continue;
          }

          if (isDeniedApproval(toolResult)) {
            const deniedToolCallId = toolResult.resumeTargetToolCallId ?? toolResult.toolCallId;
            commitToolResult({
              messageList,
              outcome: {
                kind: 'denied',
                approval: {
                  id: toolResult.approval!.id,
                  approved: false,
                  reason: toolResult.approval!.reason,
                },
              },
              toolCallId: deniedToolCallId,
              toolName: toolResult.toolName,
              toolArgs: toolResult.args,
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

          // Recording a pending call as a `result` would overwrite the invocation with an
          // undefined value and feed the model a fabricated tool output on the next turn.
          if (isPendingClientCall(toolResult)) {
            continue;
          }

          // Compute toModelOutput for successful tool results (Bug 9 parity).
          // Start from the existing providerMetadata so it's preserved even when
          // toModelOutput is absent or fails — otherwise provider-executed tools
          // or tools without a mapper lose their metadata. Results that already
          // carry a mapped output from tool-call.ts (`modelOutputComputed`) are
          // not recomputed: the serialization boundary is why tool-call maps
          // eagerly, and this step only covers results that crossed the boundary
          // unmapped (background completion, provider fallback).
          let providerMetadata: Record<string, unknown> | undefined = toolResult.providerMetadata as
            | Record<string, unknown>
            | undefined;
          if (
            !toolResult.error &&
            toolResult.result != null &&
            !toolResult.providerExecuted &&
            !toolResult.modelOutputComputed
          ) {
            providerMetadata = await computeModelOutputProviderMetadata({
              tool: registryTools?.[toolResult.toolName] as
                | { toModelOutput?: (output: unknown) => unknown }
                | undefined,
              toolName: toolResult.toolName,
              toolCallId: toolResult.toolCallId,
              result: toolResult.result,
              existingProviderMetadata: toolResult.providerMetadata as Record<string, unknown> | undefined,
              parentSpan: stepSpan,
              onMappingError: (err: unknown) => {
                // toModelOutput errors are non-fatal — the tool result is still usable
                mastra
                  ?.getLogger?.()
                  ?.warn?.(`[DurableAgent] toModelOutput failed for tool "${toolResult.toolName}": ${err}`);
              },
            });
          }

          const resolvedToolCallId = toolResult.resumeTargetToolCallId ?? toolResult.toolCallId;
          // Layer the tool-payload-transform metadata captured at emission time
          // on top (L18b). The tool-call step's messageList is a local copy, so
          // the chunk-level metadata travels on the output record and is merged
          // into the persisted providerMetadata here — matching the main loop's
          // llm-mapping, which reads it off the live chunk. Without it,
          // transcript-target transforms would not apply to the persisted
          // args/result on recall.
          if (toolResult.transformMetadata) {
            providerMetadata = withToolPayloadTransformProviderMetadata(
              providerMetadata,
              toolResult.transformMetadata,
            ) as Record<string, unknown> | undefined;
          }

          // A tool error must be recorded as `output-error` with the message in
          // `errorText` so the transcript/adapters read it as a failure rather than
          // a normal result. Successful results keep `state: 'result'` + `result`.
          // The approval decision for an approved approval-gated tool is preserved
          // so it round-trips on recall as `approval: { approved: true }`.
          commitToolResult({
            messageList,
            outcome: toolResult.error
              ? { kind: 'error', errorText: toolResult.error.message }
              : { kind: 'result', result: toolResult.result },
            toolCallId: resolvedToolCallId,
            toolName: toolResult.toolName,
            toolArgs: toolResult.args,
            approval: toolResult.approval,
            providerMetadata: providerMetadata as any,
            fallbackAppend: true,
          });
          if (resolvedToolCallId !== toolResult.toolCallId) {
            persistInvocation({
              type: 'tool-invocation' as const,
              toolInvocation: {
                ...(toolResult.error
                  ? { state: 'output-error' as const, errorText: toolResult.error.message }
                  : { state: 'result' as const, result: toolResult.result }),
                toolCallId: toolResult.toolCallId,
                toolName: toolResult.toolName,
                args: toolResult.args,
              },
              ...(providerMetadata ? { providerMetadata: providerMetadata as any } : {}),
            });
          }
        }
      }

      // 2a. Persist processor-emitted data-* chunks (#19375 parity port).
      // The tool-call step's messageList is a local copy whose mutations don't
      // cross the step boundary, so non-transient data-* chunks emitted by
      // output processors during tool execution travel on the output record
      // and are committed here, into the messageList that gets serialized and
      // flushed to memory. Runs for every entry — including aborted/blocked/
      // provider-executed calls skipped by the commit loop above — because in
      // the main loop persistence happens at emission time, before any
      // tripwire or abort can intervene.
      for (const toolResult of toolResults) {
        for (const part of toolResult.processorDataParts ?? []) {
          persistProcessorDataChunk(messageList, part.messageId ?? messageId, part);
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
      //
      // Deliberate divergence from the main loop: this override
      // can trump a terminal finish reason — a step that finished with
      // `stop`/`length`/`content-filter` whose tool result errored still
      // continues. Main avoids the case structurally: its #17893
      // `hasPendingToolCalls` gate stops the loop before tools ever run on a
      // terminal reason. Durable runs tools first (the tool-call foreach is
      // unconditional), so this override IS its tool-error recovery path —
      // do not "fix" it to match main's gating without a pinning test for
      // tool-error recovery.
      const hasToolErrors = toolResults.some(r => r.error !== undefined);
      // A pending client call ends the turn so the client can answer it. Without this the
      // loop re-invoked the model on a result nobody produced.
      const hasPendingHITL = toolResults.some(isPendingClientCall);
      const isContinued = hasPendingHITL ? false : hasToolErrors ? true : llmOutput.stepResult.isContinued;

      // Check if any delegation hook called ctx.bail(). The primary channel
      // is the tool-call step's serializable output: the hook writes the flag
      // by-reference to the RequestContext the tool was built with, which the
      // tool-call step consumes in-process and carries as `delegationBailed`
      // on its output — the only channel that survives the evented engine's
      // per-step RequestContext rehydration (G3). The requestContext read is
      // kept as a fallback for same-process paths where the flag lands on
      // this step's own instance.
      let delegationBailed = toolResults.some(r => r?.delegationBailed === true);
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
          if (isPendingClientCall(tr)) continue;
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
            stepResult: {
              ...deferredChunk.payload?.stepResult,
              isContinued,
            },
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
        modelSpanData: llmOutput.modelSpanData,
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
          mastra?.getLogger?.()?.warn?.(`[DurableAgent] Failed to close model_step span: ${error}`);
        }
      }

      // savePerStep: persist this step before the loop continues, like the regular agent's
      // onStepFinish. The final step is saved by finalize-run. Observational memory saves on its
      // own, and a save here would corrupt its bookkeeping (same exclusion as finalize-run).
      if (
        isContinued &&
        state.savePerStep &&
        !state.observationalMemory &&
        !state.memoryConfig?.readOnly &&
        state.threadId &&
        state.resourceId
      ) {
        try {
          // Re-read the entry: tool-call may have rebuilt the save queue into it. A connect()
          // worker in another process has none until something rebuilds it.
          // Bound read: a reused caller runId must not borrow another execution's queue.
          const boundEntry = getBoundRunRegistryEntry(_runId, durableInitData.runtimeBindingId);
          let saveQueueManager = boundEntry?.saveQueueManager;
          let memory = boundEntry?.memory;
          if (!saveQueueManager && mastra) {
            const rebuilt = await rebuildRunToolsFromMastra({
              mastra: mastra as Mastra,
              runId: _runId,
              agentId: _agentId,
              state,
              toolsMetadata: durableInitData.toolsMetadata,
              messageListState: durableInitData.messageListState,
              requestContextEntries: durableInitData.requestContextEntries,
              requestContext,
            });
            saveQueueManager = rebuilt?.saveQueueManager;
            memory = rebuilt?.memory;
          }
          if (saveQueueManager) {
            if (!state.threadExists && memory) {
              const thread = await memory.getThreadById?.({ threadId: state.threadId });
              if (!thread) {
                await memory.createThread?.({
                  threadId: state.threadId,
                  resourceId: state.resourceId,
                  memoryConfig: state.memoryConfig,
                });
              }
              output.state = { ...output.state, threadExists: true };
            }
            await saveQueueManager.flushMessages(messageList, state.threadId, state.memoryConfig);
          }
        } catch (error) {
          mastra?.getLogger?.()?.warn?.(`[DurableAgent] Failed to save step: ${error}`);
        }
      }

      return output;
    },
  });
}
