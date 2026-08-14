import type { StepResult, ToolSet } from '@internal/ai-sdk-v5';
import { z } from 'zod/v4';
import type { MastraDBMessage, MessageList } from '../../../agent/message-list';
import { sanitizeToolName } from '../../../agent/message-list/utils/tool-name';
import { TripWire } from '../../../agent/trip-wire';
import { createObservabilityContext, EntityType, SpanType } from '../../../observability';
import type { ProcessorState } from '../../../processors';
import { ProcessorRunner, outputProcessorsSupportStream } from '../../../processors/runner';
import { DefaultStepResult } from '../../../stream/aisdk/v5/output-helpers';
import type { ChunkType, ProviderMetadata } from '../../../stream/types';
import { ChunkFrom } from '../../../stream/types';
import {
  toolPayloadTransformAllowsTerminalToolResult,
  transformToolPayloadForTargets,
  withToolPayloadTransformMetadata,
  withToolPayloadTransformProviderMetadata,
} from '../../../tools/payload-transform';
import { findProviderToolByName } from '../../../tools/provider-tool-utils';
import { createStep } from '../../../workflows/workflow';
import { readScoped, writeScoped } from '../../run-scope-access';
import type { RunScopeContext } from '../../run-scope-access';
import { DELEGATION_BAILED_KEY, STEP_TOOLS_KEY, TOOL_PAYLOAD_TRANSFORM_KEY } from '../../run-scope-keys';
import { outputProcessorsAllowTerminalToolResult, resolveTerminalToolResult } from '../../shared/terminal-tool-result';
import type { OuterLLMRun } from '../../types';
import { deserializeToolError } from '../errors';
import { llmIterationOutputSchema, toolCallOutputSchema } from '../schema';

/**
 * Walk messageList backwards looking for a tool-invocation part with the given
 * toolCallId in result state. Used to read the post-processToolResult value back
 * from the message list so we can sync any processor mutations into the
 * downstream tool-result stream chunk.
 */
function readToolResultFromMessageList(messageList: MessageList, toolCallId: string): unknown {
  const messages: MastraDBMessage[] = messageList.get.all.db();
  for (let i = messages.length - 1; i >= 0; i--) {
    const msg = messages[i];
    if (!msg || msg.role !== 'assistant' || !msg.content?.parts) continue;
    for (const part of msg.content.parts) {
      if (
        part?.type === 'tool-invocation' &&
        part.toolInvocation?.toolCallId === toolCallId &&
        part.toolInvocation?.state === 'result'
      ) {
        return part.toolInvocation.result;
      }
    }
  }
  return undefined;
}

export function createLLMMappingStep<Tools extends ToolSet = ToolSet, OUTPUT = undefined>(
  { models, _internal, ...rest }: OuterLLMRun<Tools, OUTPUT>,
  llmExecutionStep: any,
) {
  const scopeCtx: RunScopeContext = { mastra: rest.mastra, runId: rest.runId, _internal };
  /**
   * Output processor handling for tool-result and tool-error chunks.
   *
   * LLM-generated chunks (text-delta, tool-call, etc.) are processed through output processors
   * in the Inner MastraModelOutput (llm-execution-step.ts). However, tool-result and tool-error
   * chunks are created HERE after tool execution completes, so they would bypass the output
   * processor pipeline if we just enqueued them directly.
   *
   * To ensure output processors receive ALL chunk types (including tool-result), we create
   * a ProcessorRunner here that uses the SAME processorStates map as the Inner MastraModelOutput.
   * This ensures:
   * 1. Processors see tool-result chunks in processOutputStream
   * 2. Processor state (streamParts, customState) is shared across all chunks
   * 3. Blocking/tripwire works correctly for tool results
   */
  const processorRunner =
    outputProcessorsSupportStream(rest.outputProcessors) && rest.logger
      ? new ProcessorRunner({
          inputProcessors: [],
          outputProcessors: rest.outputProcessors,
          logger: rest.logger,
          agentName: 'LLMMappingStep',
          processorStates: rest.processorStates,
        })
      : undefined;

  // Build observability context from modelSpanTracker if tracing context is available
  const observabilityContext = createObservabilityContext(rest.modelSpanTracker?.getTracingContext());

  // Create a ProcessorStreamWriter from outputWriter so processOutputStream can emit custom chunks
  const streamWriter = rest.outputWriter
    ? { custom: async (data: { type: string }) => rest.outputWriter(data as ChunkType<OUTPUT>) }
    : undefined;
  // Helper function to process a chunk through output processors and enqueue it.
  // Returns the processed chunk, or null if the chunk was blocked by a processor.
  async function processAndEnqueueChunk(
    chunk: ChunkType<OUTPUT>,
    terminalResolutionState: { toolResultBlocked: boolean },
  ): Promise<ChunkType<OUTPUT> | null> {
    if (processorRunner && rest.processorStates) {
      const {
        part: processed,
        blocked,
        reason,
        tripwireOptions,
        processorId,
      } = await processorRunner.processPart(
        chunk,
        rest.processorStates as Map<string, ProcessorState<OUTPUT>>,
        observabilityContext,
        rest.requestContext,
        rest.messageList,
        0,
        streamWriter,
      );

      const enqueueTripwire = (r?: string, opts?: { retry?: boolean; metadata?: unknown }, pid?: string) => {
        rest.controller.enqueue({
          type: 'tripwire',
          payload: {
            reason: r || 'Output processor blocked content',
            retry: opts?.retry,
            metadata: opts?.metadata,
            processorId: pid,
          },
        } as ChunkType<OUTPUT>);
      };

      if (blocked) {
        // Emit a tripwire chunk so downstream knows about the abort
        terminalResolutionState.toolResultBlocked = true;
        enqueueTripwire(reason, tripwireOptions, processorId);
        return null;
      }

      if (processed) {
        rest.controller.enqueue(processed as ChunkType<OUTPUT>);
      }

      // Emit any parts a processor stashed for reprocessing (e.g. the non-text
      // part that triggered a BatchPartsProcessor flush), pushing each back
      // through the whole chain so it gets downstream processing.
      const reprocessed = await processorRunner.drainReprocessParts(
        rest.processorStates as Map<string, ProcessorState<OUTPUT>>,
        observabilityContext,
        rest.requestContext,
        rest.messageList,
        0,
        streamWriter,
      );
      for (const r of reprocessed) {
        if (r.blocked) {
          terminalResolutionState.toolResultBlocked = true;
          enqueueTripwire(r.reason, r.tripwireOptions, r.processorId);
          return processed ? (processed as ChunkType<OUTPUT>) : null;
        }
        if (r.part != null) {
          rest.controller.enqueue(r.part as ChunkType<OUTPUT>);
        }
      }

      return processed ? (processed as ChunkType<OUTPUT>) : null;
    } else {
      // No processor runner, just enqueue the chunk directly
      rest.controller.enqueue(chunk);
      return chunk;
    }
  }

  /**
   * Run processToolResult on all output processors that implement it.
   *
   * Fires after tool.execute() returns and before the tool-result chunk is enqueued
   * to streaming clients / fed to the next LLM call. Symmetric with processOutputStep
   * (which fires before tool execution).
   *
   * Returns true on success (caller proceeds with chunk emission). Returns false on
   * tripwire (caller should emit a tripwire chunk and stop).
   */
  async function runToolResultProcessors(args: {
    chunk: ChunkType<OUTPUT> & {
      payload: { toolCallId: string; toolName: string; args?: unknown; result?: unknown; providerExecuted?: boolean };
    };
    stepNumber: number;
    steps: Array<StepResult<ToolSet>>;
  }): Promise<{ ok: true } | { ok: false; tripwire: TripWire }> {
    if (!processorRunner || !rest.outputProcessors?.length) {
      return { ok: true };
    }
    const { chunk, stepNumber, steps } = args;
    try {
      await processorRunner.runProcessToolResult({
        steps,
        messages: rest.messageList.get.all.db(),
        messageList: rest.messageList,
        stepNumber,
        toolName: chunk.payload.toolName,
        toolCallId: chunk.payload.toolCallId,
        toolArgs: chunk.payload.args,
        result: chunk.payload.result,
        providerExecuted: chunk.payload.providerExecuted,
        ...observabilityContext,
        requestContext: rest.requestContext,
        retryCount: 0,
        writer: streamWriter,
        abortSignal: rest.options?.abortSignal,
      });

      // Sync any processor mutation back into the chunk so streaming clients see
      // the post-processor value, not the raw tool return.
      const postProcessorResult = readToolResultFromMessageList(rest.messageList, chunk.payload.toolCallId);
      if (postProcessorResult !== undefined && postProcessorResult !== chunk.payload.result) {
        (chunk.payload as { result: unknown }).result = postProcessorResult;
      }
      return { ok: true };
    } catch (error) {
      if (error instanceof TripWire) {
        return { ok: false, tripwire: error };
      }
      throw error;
    }
  }

  /**
   * Emit a tripwire chunk to the stream so MastraModelOutput captures it as the
   * step result. Mirrors the tripwire emission in processAndEnqueueChunk.
   */
  function emitTripwireChunk(tripwire: TripWire): void {
    rest.controller.enqueue({
      type: 'tripwire',
      payload: {
        reason: tripwire.message || 'Tool result blocked by processor',
        retry: tripwire.options?.retry,
        metadata: tripwire.options?.metadata,
        processorId: tripwire.processorId,
      },
    } as ChunkType<OUTPUT>);
  }

  return createStep({
    id: 'llmExecutionMappingStep',
    inputSchema: z.array(toolCallOutputSchema),
    outputSchema: llmIterationOutputSchema,
    execute: async ({ inputData, getStepResult, bail }) => {
      // Per-execution state: one Agent/workflow instance can serve concurrent
      // runs, so terminal eligibility must never live in a shared closure.
      const terminalResolutionState = { toolResultBlocked: false };
      const initialResult = getStepResult(llmExecutionStep);

      /**
       * llm-execution-step creates the latest StepResult before local tools run.
       * Once this mapping step persists their result, error, or denial on the
       * MessageList, rebuild that result so the next processInputStep call sees
       * the completed tool interaction through the documented StepResult
       * `toolCalls`/`toolResults` accessors.
       */
      const refreshLatestStepResult = () => {
        const steps = initialResult?.output?.steps;
        if (!Array.isArray(steps) || steps.length === 0) return;

        const latestStep = steps.at(-1);
        if (!latestStep) return;

        steps[steps.length - 1] = new DefaultStepResult({
          content: rest.messageList.get.response.aiV5.modelContent(steps.length),
          finishReason: latestStep.finishReason,
          providerMetadata: latestStep.providerMetadata,
          request: latestStep.request,
          response: {
            ...latestStep.response,
            messages: rest.messageList.get.response.aiV5.model(),
          },
          tripwire: latestStep.tripwire,
          usage: latestStep.usage,
          warnings: latestStep.warnings,
        });
      };

      /**
       * Compute toModelOutput for a successful tool call and return providerMetadata
       * with the result stored at mastra.modelOutput.
       *
       * Looks up the tool from dynamically loaded tools (`_internal.stepTools`, e.g. via
       * ToolSearchProcessor) first, then falls back to the agent's static tool definitions.
       *
       * When toModelOutput is defined, the transform runs under a MAPPING child span so
       * traces can distinguish "never invoked" from "ran no-op" from "ran transforming."
       */
      /**
       * Normalize modelOutput from toModelOutput() into the AI SDK's
       * LanguageModelV2ToolResultOutput shape.
       *
       * The AI SDK's content array only accepts type 'text' or 'media'.
       * Mastra's createTool docs show type 'image-url' as a convenience shorthand,
       * so we normalize that here into type 'media' with the correct structure.
       *
       * Previously this converted 'media' -> 'image-data'/'file-data' which was wrong
       * (those types are not valid in LanguageModelV2ToolResultOutput).
       * See: https://github.com/mastra-ai/mastra/issues/17876
       */
      function normalizeModelOutput(output: unknown): unknown {
        if (output == null || typeof output !== 'object') return output;

        const obj = output as Record<string, unknown>;
        if (obj.type !== 'content' || !Array.isArray(obj.value)) return output;

        return {
          ...obj,
          value: (obj.value as unknown[]).map(item => {
            if (item == null || typeof item !== 'object') return item;
            const part = item as Record<string, unknown>;
            // Normalize 'image-url' convenience type -> 'media' as AI SDK expects
            if (part.type === 'image-url' && typeof part.url === 'string') {
              // Prefer caller-supplied mediaType; fall back to parsing data: URI or defaulting to image/jpeg
              const mediaType =
                typeof part.mediaType === 'string' && part.mediaType
                  ? part.mediaType
                  : part.url.startsWith('data:')
                    ? part.url.slice(5, part.url.indexOf(';')) || 'image/jpeg'
                    : 'image/jpeg';
              return { type: 'media', data: part.url, mediaType };
            }
            // 'image-data'/'file-data' from old normalizeModelOutput — convert back to 'media'
            if (part.type === 'image-data' && typeof part.data === 'string') {
              return { type: 'media', data: part.data, mediaType: part.mediaType ?? 'image/jpeg' };
            }
            if (part.type === 'file-data' && typeof part.data === 'string') {
              return { type: 'media', data: part.data, mediaType: part.mediaType ?? 'application/octet-stream' };
            }
            return part;
          }),
        };
      }

      async function getProviderMetadataWithModelOutput(toolCall: {
        toolName: string;
        toolCallId?: string;
        result?: unknown;
        providerMetadata?: Record<string, unknown>;
      }) {
        const tool = ((
          readScoped(scopeCtx, STEP_TOOLS_KEY, 'stepTools') as
            Record<string, { toModelOutput?: (output: unknown) => unknown }> | undefined
        )?.[toolCall.toolName] ?? rest.tools?.[toolCall.toolName]) as
          { toModelOutput?: (output: unknown) => unknown } | undefined;
        let modelOutput: unknown;
        if (tool?.toModelOutput && toolCall.result != null) {
          const parentSpan = observabilityContext?.tracingContext?.currentSpan;
          const mappingSpan = parentSpan?.createChildSpan({
            type: SpanType.MAPPING,
            name: `tool output mapping: '${toolCall.toolName}'`,
            entityType: EntityType.TOOL,
            entityId: toolCall.toolName,
            entityName: toolCall.toolName,
            input: toolCall.result,
            attributes: {
              mappingType: 'toModelOutput',
              toolCallId: toolCall.toolCallId,
            },
          });
          try {
            modelOutput = await tool.toModelOutput(toolCall.result);
            // Normalize media parts to image-data/file-data as AI SDK expects
            modelOutput = normalizeModelOutput(modelOutput);
            mappingSpan?.end({ output: modelOutput });
          } catch (err) {
            mappingSpan?.error({ error: err as Error, endSpan: true });
            throw err;
          }
        }

        const existingMastra = (toolCall.providerMetadata as any)?.mastra;
        const providerMetadata = {
          ...toolCall.providerMetadata,
          ...(modelOutput != null ? { mastra: { ...existingMastra, modelOutput } } : {}),
        };
        const hasMetadata = Object.keys(providerMetadata).length > 0;
        return hasMetadata ? providerMetadata : undefined;
      }

      async function transformToolChunk(
        chunk: ChunkType<OUTPUT>,
        toolCall: {
          toolName: string;
          toolCallId: string;
          args?: unknown;
          result?: unknown;
          error?: unknown;
          providerMetadata?: Record<string, unknown>;
        },
        phase: 'output-available' | 'error' | 'approval',
      ): Promise<ChunkType<OUTPUT>> {
        const stepTools = readScoped(scopeCtx, STEP_TOOLS_KEY, 'stepTools') as ToolSet | undefined;
        const tool =
          stepTools?.[toolCall.toolName] ||
          findProviderToolByName(stepTools, toolCall.toolName) ||
          Object.values(stepTools || {}).find((t: any) => `id` in t && t.id === toolCall.toolName) ||
          rest.tools?.[toolCall.toolName] ||
          findProviderToolByName(rest.tools, toolCall.toolName) ||
          Object.values(rest.tools || {}).find((t: any) => `id` in t && t.id === toolCall.toolName);
        const source = {
          policy: readScoped(scopeCtx, TOOL_PAYLOAD_TRANSFORM_KEY, 'toolPayloadTransform'),
          toolTransform: (tool as { transform?: unknown } | undefined)?.transform as any,
        };
        const inputTransform = await transformToolPayloadForTargets(
          {
            phase: 'input-available',
            toolName: toolCall.toolName,
            toolCallId: toolCall.toolCallId,
            input: toolCall.args,
            providerMetadata: toolCall.providerMetadata,
          },
          source,
          rest.logger,
        );
        const transform = await transformToolPayloadForTargets(
          {
            phase,
            toolName: toolCall.toolName,
            toolCallId: toolCall.toolCallId,
            input: toolCall.args,
            output: toolCall.result,
            error: toolCall.error,
            providerMetadata: toolCall.providerMetadata,
          },
          source,
          rest.logger,
        );

        return withToolPayloadTransformMetadata(withToolPayloadTransformMetadata(chunk, inputTransform), transform);
      }

      // A declined approval has no `result` but is fully resolved — it must not be mistaken for a
      // pending HITL/deferred tool call (which would otherwise suspend or stall the loop).
      const isDeniedApproval = (toolCall: { approval?: { approved?: boolean } }) =>
        toolCall?.approval?.approved === false;
      const persistDeniedApproval = (toolCall: z.infer<typeof toolCallOutputSchema>) => {
        const deniedToolCallId = toolCall.resumeTargetToolCallId ?? toolCall.toolCallId;
        const deniedPart = {
          type: 'tool-invocation' as const,
          toolInvocation: {
            state: 'output-denied' as const,
            toolCallId: deniedToolCallId,
            toolName: sanitizeToolName(toolCall.toolName),
            args: toolCall.args,
            approval: {
              id: toolCall.approval!.id,
              approved: false,
              reason: toolCall.approval!.reason,
            },
          },
        };

        if (!rest.messageList.updateToolInvocation(deniedPart)) {
          // A recalled or provider-shaped history can be missing the original call part. Retain
          // the resolved denial as a response instead of silently dropping the user decision.
          rest.messageList.add(
            {
              id: _internal?.generateId?.() ?? crypto.randomUUID(),
              role: 'assistant',
              content: { format: 2, parts: [deniedPart] },
              createdAt: new Date(),
            },
            'response',
          );
        }

        if (deniedToolCallId !== toolCall.toolCallId) {
          // The model created a new provider call to communicate the decision. Resolve that
          // technical invocation too; otherwise the next provider request contains an orphan call.
          const syntheticResultPart = {
            type: 'tool-invocation',
            toolInvocation: {
              state: 'result' as const,
              toolCallId: toolCall.toolCallId,
              toolName: sanitizeToolName(toolCall.toolName),
              args: toolCall.args,
              result: toolCall.approval?.reason ?? 'Tool call was not approved by the user',
            },
          } as const;
          if (!rest.messageList.updateToolInvocation(syntheticResultPart)) {
            // Normalized/provider-shaped history may omit the current invocation too. Store a
            // complete resolved part so the next model prompt never contains an unmatched call.
            rest.messageList.add(
              {
                id: _internal?.generateId?.() ?? crypto.randomUUID(),
                role: 'assistant',
                content: { format: 2, parts: [syntheticResultPart] },
                createdAt: new Date(),
              },
              'response',
            );
          }
        }
      };
      const processDeniedApproval = async (toolCall: z.infer<typeof toolCallOutputSchema>) => {
        // Keep the recalled message in the approval-specific `output-denied` state, while
        // retaining the existing public stream contract that exposes a denial in toolResults.
        // Persist first so a later pending-HITL bail can never strand the approval decision.
        persistDeniedApproval(toolCall);

        const result = toolCall.approval?.reason ?? 'Tool call was not approved by the user';
        // This is a compatibility result, not tool output. Do not invoke toModelOutput or
        // tool-payload output transforms that legitimately expect the tool's output schema.
        const chunk: ChunkType<OUTPUT> = {
          type: 'tool-result',
          runId: rest.runId,
          from: ChunkFrom.AGENT,
          payload: {
            args: toolCall.args,
            toolCallId: toolCall.toolCallId,
            toolName: toolCall.toolName,
            result,
            providerMetadata: toolCall.providerMetadata as ProviderMetadata | undefined,
            providerExecuted: toolCall.providerExecuted,
          },
        };
        const processed = await processAndEnqueueChunk(chunk, terminalResolutionState);
        if (processed) await rest.options?.onChunk?.(processed);
      };
      const persistResolvedToolCall = (
        toolCall: z.infer<typeof toolCallOutputSchema>,
        resolvedPart: Parameters<typeof rest.messageList.updateToolInvocation>[0],
      ) => {
        const persistPart = (part: Parameters<typeof rest.messageList.updateToolInvocation>[0]) => {
          if (!rest.messageList.updateToolInvocation(part)) {
            rest.messageList.add(
              {
                id: _internal?.generateId?.() ?? crypto.randomUUID(),
                role: 'assistant',
                content: { format: 2, parts: [part] },
                createdAt: new Date(),
              },
              'response',
            );
          }
        };
        const resolvedToolCallId = toolCall.resumeTargetToolCallId ?? toolCall.toolCallId;
        persistPart({
          ...resolvedPart,
          toolInvocation: {
            ...resolvedPart.toolInvocation,
            toolCallId: resolvedToolCallId,
          },
        });

        if (resolvedToolCallId !== toolCall.toolCallId) {
          const { approval: _approval, ...syntheticInvocation } = resolvedPart.toolInvocation;
          persistPart({
            ...resolvedPart,
            toolInvocation: {
              ...syntheticInvocation,
              toolCallId: toolCall.toolCallId,
            },
          });
        }
      };

      if (
        inputData?.some(
          toolCall => toolCall?.result === undefined && !toolCall.providerExecuted && !isDeniedApproval(toolCall),
        )
      ) {
        const errorResults = inputData.filter(toolCall => toolCall?.error && !toolCall.providerExecuted);
        const abortedResults = inputData.filter(
          toolCall => toolCall?.aborted && toolCall.abortError && !toolCall.providerExecuted,
        );

        // Request-aborted tools remain incomplete in model history, but their
        // actual cancellation error is still authoritative for live stream and
        // Harness consumers. Emit that terminal without calling
        // persistResolvedToolCall or allowing another model iteration.
        for (const toolCall of abortedResults) {
          const reifiedError = deserializeToolError(toolCall.abortError);
          const chunk = await transformToolChunk(
            {
              type: 'tool-error',
              runId: rest.runId,
              from: ChunkFrom.AGENT,
              payload: {
                error: reifiedError,
                args: toolCall.args,
                toolCallId: toolCall.toolCallId,
                toolName: toolCall.toolName,
                providerMetadata: toolCall.providerMetadata as ProviderMetadata | undefined,
              },
            },
            { ...toolCall, error: reifiedError },
            'error',
          );
          const processed = await processAndEnqueueChunk(chunk, terminalResolutionState);
          if (processed) await rest.options?.onChunk?.(processed);
        }

        if (errorResults?.length) {
          for (const toolCall of errorResults) {
            // `toolCall.error` arrives as the plain {name,message,stack} the workflow step
            // serializes (Error instances become `{}` over the pubsub bus). Reify here so
            // chunk consumers see a real Error with name/message/stack intact.
            const reifiedError = deserializeToolError(toolCall.error);
            const chunk = await transformToolChunk(
              {
                type: 'tool-error',
                runId: rest.runId,
                from: ChunkFrom.AGENT,
                payload: {
                  error: reifiedError,
                  args: toolCall.args,
                  toolCallId: toolCall.toolCallId,
                  toolName: toolCall.toolName,
                  providerMetadata: toolCall.providerMetadata as ProviderMetadata | undefined,
                },
              },
              { ...toolCall, error: reifiedError },
              'error',
            );
            const processed = await processAndEnqueueChunk(chunk, terminalResolutionState);
            if (processed) await rest.options?.onChunk?.(processed);

            persistResolvedToolCall(toolCall, {
              type: 'tool-invocation' as const,
              toolInvocation: {
                state: 'output-error' as const,
                toolCallId: toolCall.toolCallId,
                toolName: sanitizeToolName(toolCall.toolName),
                args: toolCall.args,
                // Use the already-reified Error rather than `toolCall.error` (which is the
                // plain {name,message,stack} shape after the pubsub JSON round-trip).
                // Without reification the `instanceof Error` check below falls through to
                // `safeStringify`, dumping the whole stringified payload into the history.
                errorText: reifiedError.message || 'Tool execution failed',
                // Preserve the approval decision for an approved approval-gated tool that then
                // threw, so the decision round-trips on recall.
                ...(toolCall.approval ? { approval: toolCall.approval } : {}),
              },
              ...(withToolPayloadTransformProviderMetadata(
                toolCall.providerMetadata as ProviderMetadata,
                chunk.metadata,
              )
                ? {
                    providerMetadata: withToolPayloadTransformProviderMetadata(
                      toolCall.providerMetadata as ProviderMetadata,
                      chunk.metadata,
                    ) as ProviderMetadata,
                  }
                : {}),
            });
          }
        }

        // When tool errors occur, continue the agentic loop so the model can see the
        // error and self-correct (e.g., retry with different args, or respond to the user).
        // The error messages are already added to the messageList above, so the model
        // will see them on the next turn. This handles both tool-not-found errors
        // (hallucinated tool names) and tool execution errors (tool throws).
        //
        // A decline is resolved even though it has no result. Persist every denial
        // before either the error-recovery return or the pending-HITL bail path.
        for (const toolCall of inputData.filter(isDeniedApproval)) {
          await processDeniedApproval(toolCall);
        }

        // Check for pending HITL tool calls (tools with no result and no error).
        // In mixed turns with errors and pending HITL tools,
        // the HITL suspension path should take priority over continuing the loop.
        const hasPendingHITL = inputData.some(
          tc => tc.result === undefined && !tc.error && !tc.aborted && !tc.providerExecuted && !isDeniedApproval(tc),
        );

        if (errorResults.length > 0 || abortedResults.length > 0) {
          // Process successful siblings before either recovering from ordinary
          // errors or honoring the request abort. Settlements that completed in
          // this turn remain authoritative even though abort forbids another turn.
          const successfulResults = inputData.filter(tc => tc.result !== undefined);
          if (successfulResults.length) {
            const stepNumber = (initialResult?.output?.steps?.length ?? 0) as number;
            const steps = (initialResult?.output?.steps ?? []) as Array<StepResult<ToolSet>>;
            for (const toolCall of successfulResults) {
              // Compute modelOutput before emitting the chunk so consumers (e.g. harness)
              // can access it on the chunk's providerMetadata.mastra.modelOutput.
              // getProviderMetadataWithModelOutput already returns the fully-merged providerMetadata.
              const providerMetadata = !toolCall.providerExecuted
                ? await getProviderMetadataWithModelOutput(toolCall)
                : undefined;
              const chunkProviderMetadata = (providerMetadata ?? toolCall.providerMetadata) as
                ProviderMetadata | undefined;

              const chunk = await transformToolChunk(
                {
                  type: 'tool-result',
                  runId: rest.runId,
                  from: ChunkFrom.AGENT,
                  payload: {
                    args: toolCall.args,
                    toolCallId: toolCall.toolCallId,
                    toolName: toolCall.toolName,
                    result: toolCall.result,
                    providerMetadata: chunkProviderMetadata,
                    providerExecuted: toolCall.providerExecuted,
                  },
                },
                toolCall,
                'output-available',
              );
              const toolResultProcessing = await runToolResultProcessors({
                chunk: chunk as ChunkType<OUTPUT> & {
                  payload: {
                    toolCallId: string;
                    toolName: string;
                    args?: unknown;
                    result?: unknown;
                    providerExecuted?: boolean;
                  };
                },
                stepNumber,
                steps,
              });
              if (!toolResultProcessing.ok) {
                terminalResolutionState.toolResultBlocked = true;
                emitTripwireChunk(toolResultProcessing.tripwire);
                continue;
              }

              if (!toolCall.providerExecuted) {
                // Update tool invocations from state:'call' to state:'result' for successful client tools.
                // Provider-executed tools are handled by llm-execution-step.
                persistResolvedToolCall(toolCall, {
                  type: 'tool-invocation' as const,
                  toolInvocation: {
                    state: 'result' as const,
                    toolCallId: toolCall.toolCallId,
                    toolName: sanitizeToolName(toolCall.toolName),
                    args: toolCall.args,
                    result: (chunk as { payload: { result: unknown } }).payload.result,
                    // Preserve the approval decision for an approved approval-gated tool in a mixed
                    // turn (one tool errored, another approved) so it round-trips on recall too.
                    ...(toolCall.approval ? { approval: toolCall.approval } : {}),
                  },
                  ...(withToolPayloadTransformProviderMetadata(providerMetadata, chunk.metadata)
                    ? {
                        providerMetadata: withToolPayloadTransformProviderMetadata(
                          providerMetadata,
                          chunk.metadata,
                        ) as ProviderMetadata,
                      }
                    : {}),
                });
              }

              const processed = await processAndEnqueueChunk(chunk, terminalResolutionState);
              if (processed) await rest.options?.onChunk?.(processed);
            }
          }

          if (abortedResults.length === 0 && !hasPendingHITL) {
            // Ordinary tool errors remain model-visible and may self-recover
            // only when no pending interaction must suspend the turn.
            initialResult.stepResult.isContinued = true;
            initialResult.stepResult.reason = 'tool-calls';
            refreshLatestStepResult();
            return {
              ...initialResult,
              messages: {
                all: rest.messageList.get.all.aiV5.model(),
                user: rest.messageList.get.input.aiV5.model(),
                nonUser: rest.messageList.get.response.aiV5.model(),
              },
            };
          }
        }

        // A request abort is terminal even if a sibling error or processor had
        // already prepared a retry. Without an aborted tool, preserve the
        // existing processor-retry contract.
        if (abortedResults.length > 0 || initialResult.stepResult.reason !== 'retry') {
          initialResult.stepResult.isContinued = false;
        }

        // Update messages field to include any error messages we added to messageList
        refreshLatestStepResult();
        return bail({
          ...initialResult,
          messages: {
            all: rest.messageList.get.all.aiV5.model(),
            user: rest.messageList.get.input.aiV5.model(),
            nonUser: rest.messageList.get.response.aiV5.model(),
          },
        });
      }

      if (inputData?.length) {
        const stepNumberForToolResults = (initialResult?.output?.steps?.length ?? 0) as number;
        const stepsForToolResults = (initialResult?.output?.steps ?? []) as Array<StepResult<ToolSet>>;
        for (const toolCall of inputData) {
          // A declined approval has no step `result`: persist it as `output-denied` while still
          // emitting the denial result expected by the public stream/toolResults contract.
          if (isDeniedApproval(toolCall)) {
            await processDeniedApproval(toolCall);
            continue;
          }

          // No result yet — skip emitting a chunk. For deferred provider-executed tools
          // (e.g. Anthropic web_search), the result arrives in a later step and is handled
          // by processOutputStream's 'tool-result' case in llm-execution-step.
          if (toolCall.result === undefined) continue;

          // Compute modelOutput before emitting the chunk so consumers (e.g. harness)
          // can access it on the chunk's providerMetadata.mastra.modelOutput.
          // getProviderMetadataWithModelOutput already returns the fully-merged providerMetadata.
          const providerMetadata = !toolCall.providerExecuted
            ? await getProviderMetadataWithModelOutput(toolCall)
            : undefined;
          const chunkProviderMetadata = (providerMetadata ?? toolCall.providerMetadata) as ProviderMetadata | undefined;

          const chunk = await transformToolChunk(
            {
              type: 'tool-result',
              runId: rest.runId,
              from: ChunkFrom.AGENT,
              payload: {
                args: toolCall.args,
                toolCallId: toolCall.toolCallId,
                toolName: toolCall.toolName,
                result: toolCall.result,
                providerMetadata: chunkProviderMetadata,
                providerExecuted: toolCall.providerExecuted,
              },
            },
            toolCall,
            'output-available',
          );

          const toolResultProcessing = await runToolResultProcessors({
            chunk: chunk as ChunkType<OUTPUT> & {
              payload: {
                toolCallId: string;
                toolName: string;
                args?: unknown;
                result?: unknown;
                providerExecuted?: boolean;
              };
            },
            stepNumber: stepNumberForToolResults,
            steps: stepsForToolResults,
          });
          if (!toolResultProcessing.ok) {
            terminalResolutionState.toolResultBlocked = true;
            emitTripwireChunk(toolResultProcessing.tripwire);
            continue;
          }

          // Provider-executed tools are handled by llm-execution-step; for client-executed
          // tools we patch state:'call' -> state:'result' here after processors have run.
          if (!toolCall.providerExecuted) {
            persistResolvedToolCall(toolCall, {
              type: 'tool-invocation' as const,
              toolInvocation: {
                state: 'result' as const,
                toolCallId: toolCall.toolCallId,
                toolName: sanitizeToolName(toolCall.toolName),
                args: toolCall.args,
                result: (chunk as { payload: { result: unknown } }).payload.result,
                // Preserve the approval decision for an approved approval-gated tool so it
                // round-trips on recall as `approval: { approved: true }`.
                ...(toolCall.approval ? { approval: toolCall.approval } : {}),
              },
              ...(withToolPayloadTransformProviderMetadata(providerMetadata, chunk.metadata)
                ? {
                    providerMetadata: withToolPayloadTransformProviderMetadata(
                      providerMetadata,
                      chunk.metadata,
                    ) as ProviderMetadata,
                  }
                : {}),
            });
          }

          const processed = await processAndEnqueueChunk(chunk, terminalResolutionState);
          if (processed) await rest.options?.onChunk?.(processed);
        }

        // Check if any delegation hook called ctx.bail() — signal the loop to stop.
        // The bail flag is communicated via requestContext because Zod output validation
        // strips unknown fields (like _bailed) from the tool result object.
        if (rest.requestContext?.get('__mastra_delegationBailed')) {
          writeScoped(scopeCtx, DELEGATION_BAILED_KEY, '_delegationBailed', true);
          rest.requestContext.set('__mastra_delegationBailed', false);
        }

        const stepTools = readScoped(scopeCtx, STEP_TOOLS_KEY, 'stepTools') as ToolSet | undefined;
        const toolPayloadTransform = readScoped(scopeCtx, TOOL_PAYLOAD_TRANSFORM_KEY, 'toolPayloadTransform');
        const terminalDeliveryIsSafe =
          outputProcessorsAllowTerminalToolResult(rest.outputProcessors) &&
          toolPayloadTransformAllowsTerminalToolResult(toolPayloadTransform) &&
          // A terminal envelope replaces the ordinary assistant answer. If the
          // provider already streamed user-visible text in this run, stopping
          // here would make live delivery (preamble + terminal value) disagree
          // with persisted/materialized output (terminal value only).
          !(typeof initialResult.output?.text === 'string' && initialResult.output.text.length > 0);
        const terminalToolResult =
          rest.options?.abortSignal?.aborted || terminalResolutionState.toolResultBlocked || !terminalDeliveryIsSafe
            ? undefined
            : await resolveTerminalToolResult({
                calls: inputData,
                tools: stepTools,
                fallbackTools: rest.tools,
                runId: rest.runId,
                abortSignal: rest.options?.abortSignal,
                onPolicyFailure: error =>
                  rest.logger?.warn?.(`[TerminalToolResult] ${error.message}`, { runId: rest.runId }),
              });

        refreshLatestStepResult();
        return {
          ...initialResult,
          ...(terminalToolResult ? { terminalToolResult } : {}),
          messages: {
            all: rest.messageList.get.all.aiV5.model(),
            user: rest.messageList.get.input.aiV5.model(),
            nonUser: rest.messageList.get.response.aiV5.model(),
          },
        };
      }

      // Fallback: if inputData is empty or undefined, return initialResult as-is
      return initialResult;
    },
  });
}
