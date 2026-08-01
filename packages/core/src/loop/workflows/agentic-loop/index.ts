import { randomUUID } from 'node:crypto';
import type { StepResult, ToolSet } from '@internal/ai-sdk-v5';
import type { MastraDBMessage } from '../../../memory';
import { InternalSpans } from '../../../observability';
import { safeEnqueue } from '../../../stream/base';
import type { ChunkType } from '../../../stream/types';
import { ChunkFrom } from '../../../stream/types';
import { createWorkflow as createDirectWorkflow, createEventedWorkflow } from '../../../workflows/create';
import type { OutputWriter } from '../../../workflows/types';
import type { RunScopeContext } from '../../run-scope-access';
import { readScoped, writeScoped } from '../../run-scope-access';
import { DELEGATION_BAILED_KEY, DRAIN_PENDING_SIGNALS_KEY, RESOURCE_ID_KEY, THREAD_ID_KEY } from '../../run-scope-keys';
import type { LoopRun } from '../../types';
import { createAgenticExecutionWorkflow } from '../agentic-execution';
import { pruneAgentLoopSnapshot } from '../prune-snapshot';
import { llmIterationOutputSchema } from '../schema';
import type { LLMIterationData } from '../schema';

interface AgenticLoopParams<Tools extends ToolSet = ToolSet, OUTPUT = undefined> extends LoopRun<Tools, OUTPUT> {
  controller: ReadableStreamDefaultController<ChunkType<OUTPUT>>;
  outputWriter: OutputWriter;
}

export function createAgenticLoopWorkflow<Tools extends ToolSet = ToolSet, OUTPUT = undefined>(
  params: AgenticLoopParams<Tools, OUTPUT>,
) {
  const {
    models,
    _internal,
    messageId,
    runId,
    toolChoice,
    messageList,
    modelSettings,
    controller,
    outputWriter,
    ...rest
  } = params;

  const scopeCtx: RunScopeContext = { mastra: rest.mastra, runId, _internal };

  // Track accumulated steps across iterations to pass to stopWhen
  const accumulatedSteps: StepResult<Tools>[] = [];
  // Track previous content to determine what's new in each step
  let previousContentLength = 0;
  // When continue:false + feedback, allow one more LLM turn then stop
  let pendingFeedbackStop = false;
  // When this loop is a resume (e.g. after tool approval), the suspended run
  // already flushed its in-progress assistant message to storage. The first
  // continuation must start a fresh response message instead of merging into
  // that persisted row — see the guard in the dowhile below (issue #19445).
  let isResumeContinuationPending = !!rest.resumeContext;

  const agenticExecutionWorkflow = createAgenticExecutionWorkflow<Tools, OUTPUT>({
    messageId: messageId,
    models,
    _internal,
    modelSettings,
    toolChoice,
    controller,
    outputWriter,
    messageList,
    runId,
    ...rest,
  });

  const createWorkflow = process.env.MASTRA_EVENTED_EXECUTION === 'true' ? createEventedWorkflow : createDirectWorkflow;

  return createWorkflow({
    id: 'agentic-loop',
    inputSchema: llmIterationOutputSchema,
    outputSchema: llmIterationOutputSchema,
    options: {
      tracingPolicy: {
        // mark all workflow spans related to the
        // VNext execution as internal
        internal: InternalSpans.WORKFLOW,
      },
      shouldPersistSnapshot: params => {
        // We need a persisted snapshot record to support `resumeStream()`.
        // - Create the initial record early ("pending")
        // - Update it when execution is suspended ("paused"/"suspended")
        // Avoid persisting "running" snapshots so we don't overwrite an existing suspended snapshot.
        return (
          params.workflowStatus === 'pending' ||
          params.workflowStatus === 'paused' ||
          params.workflowStatus === 'suspended'
        );
      },
      // Agent-loop snapshots are pure resume artifacts — strip everything a
      // resume never reads (stale suspend payloads, duplicated message
      // arrays, AI SDK step history) before persisting.
      pruneSnapshot: pruneAgentLoopSnapshot,
      validateInputs: false,
    },
  })
    .dowhile(agenticExecutionWorkflow, async ({ inputData }) => {
      const typedInputData = inputData as LLMIterationData<Tools, OUTPUT>;
      const abortWon = rest.options?.abortSignal?.aborted === true;
      if (abortWon) {
        typedInputData.terminalToolResult = undefined;
        if (typedInputData.stepResult) {
          typedInputData.stepResult.reason = 'abort';
          typedInputData.stepResult.isContinued = false;
        }
      }
      let hasFinishedSteps = abortWon;

      // First loop-back after a resume: the suspended run flushed its
      // in-progress assistant message (reasoning + text + the pending
      // tool-call) to storage before parking. The approved tool result has
      // now attached to that same message — which is correct, it resolves the
      // message's own tool call — but everything the model produces from here
      // is a separate response and must NOT merge back into the persisted row.
      // Appending a second response's parts mutates the row in place, and
      // providers that sign reasoning blocks (Anthropic extended thinking)
      // then reject the next turn ("thinking blocks in the latest assistant
      // message cannot be modified"). Seal the flushed message and rotate to a
      // fresh response message for the continuation. See issue #19445.
      if (isResumeContinuationPending) {
        isResumeContinuationPending = false;
        messageList.markResponseMessageBoundary(typedInputData.stepResult?.messageId ?? typedInputData.messageId);

        const nextMessageId = rest.rotateResponseMessageId();
        typedInputData.messageId = nextMessageId;
        if (typedInputData.stepResult) {
          typedInputData.stepResult.messageId = nextMessageId;
          typedInputData.stepResult.isContinued = true;
        }
      }

      const pendingSignals = abortWon
        ? []
        : (readScoped(scopeCtx, DRAIN_PENDING_SIGNALS_KEY, 'drainPendingSignals')?.(runId) ?? []);
      if (pendingSignals.length > 0) {
        // Signals arriving after the execution workflow settled still win over
        // a terminal candidate. The next provider turn must see them.
        typedInputData.terminalToolResult = undefined;
        messageList.markResponseMessageBoundary(typedInputData.stepResult?.messageId ?? typedInputData.messageId);

        const nextMessageId = rest.rotateResponseMessageId();
        typedInputData.messageId = nextMessageId;
        for (const pendingSignal of pendingSignals) {
          const signalForTranscript = messageList.addSignal(pendingSignal);
          safeEnqueue(controller, signalForTranscript.toDataPart() as any);
        }
        if (typedInputData.stepResult) {
          typedInputData.stepResult.messageId = nextMessageId;
          typedInputData.stepResult.isContinued = true;
        }
        typedInputData.messages = {
          all: messageList.get.all.aiV5.model(),
          user: messageList.get.input.aiV5.model(),
          nonUser: messageList.get.response.aiV5.model(),
        };
      }

      if (pendingFeedbackStop) {
        hasFinishedSteps = true;
        pendingFeedbackStop = false;
      }

      const allContent: StepResult<Tools>['content'] = typedInputData.messages.nonUser.flatMap(
        message => message.content as unknown as StepResult<Tools>['content'],
      );

      // Only include new content in this step (content added since the previous iteration)
      const currentContent = allContent.slice(previousContentLength);
      previousContentLength = allContent.length;

      const toolResultParts = currentContent.filter(part => part.type === 'tool-result');

      const currentStep: StepResult<Tools> = {
        content: currentContent,
        usage: typedInputData.output.usage || { inputTokens: 0, outputTokens: 0, totalTokens: 0 },
        // we need to cast this because we add 'tripwire' and 'retry' for processor scenarios
        finishReason: (typedInputData.stepResult?.reason || 'unknown') as StepResult<Tools>['finishReason'],
        warnings: typedInputData.stepResult?.warnings || [],
        request: typedInputData.metadata?.request || {},
        response: {
          ...typedInputData.metadata,
          modelId: typedInputData.metadata?.modelId || typedInputData.metadata?.model || '',
          messages: [],
        } as StepResult<Tools>['response'],
        text: typedInputData.output.text || '',
        reasoning: typedInputData.output.reasoning || [],
        reasoningText: typedInputData.output.reasoningText || '',
        files: typedInputData.output.files || [],
        toolCalls: typedInputData.output.toolCalls || [],
        toolResults: toolResultParts as StepResult<Tools>['toolResults'],
        sources: typedInputData.output.sources || [],
        staticToolCalls: typedInputData.output.staticToolCalls || [],
        dynamicToolCalls: typedInputData.output.dynamicToolCalls || [],
        staticToolResults: toolResultParts.filter(
          (part: any) => part.dynamic === false,
        ) as StepResult<Tools>['staticToolResults'],
        dynamicToolResults: toolResultParts.filter(
          (part: any) => part.dynamic === true,
        ) as StepResult<Tools>['dynamicToolResults'],
        providerMetadata: typedInputData.metadata?.providerMetadata,
      };

      accumulatedSteps.push(currentStep);

      if (typedInputData.terminalToolResult) {
        hasFinishedSteps = true;
      }

      // Only call stopWhen if we're continuing (not on the final step)
      if (rest.stopWhen && !hasFinishedSteps && typedInputData.stepResult?.isContinued && accumulatedSteps.length > 0) {
        // Cast steps to any for v5/v6 StopCondition compatibility
        // v5 and v6 StepResult types have minor differences (e.g., rawFinishReason, finishReason format)
        // but are compatible at runtime for stop condition evaluation
        const steps = accumulatedSteps as any;
        const conditions = await Promise.all(
          (Array.isArray(rest.stopWhen) ? rest.stopWhen : [rest.stopWhen]).map(condition => {
            return condition({ steps });
          }),
        );

        const hasStopped = conditions.some(condition => condition);
        hasFinishedSteps = hasFinishedSteps || hasStopped;
      }

      // Call onIterationComplete hook if provided (call for every iteration, not just continued ones)
      if (rest.onIterationComplete && !typedInputData.backgroundTaskPending) {
        const isFinal = !typedInputData.stepResult?.isContinued || hasFinishedSteps;
        const iterationContext = {
          iteration: accumulatedSteps.length,
          maxIterations: rest.maxSteps,
          text: typedInputData.output.text || '',
          toolCalls: (typedInputData.output.toolCalls || []).map((tc: any) => ({
            id: tc.toolCallId || tc.id || '',
            name: tc.toolName || tc.name || '',
            args: (tc.args || {}) as Record<string, unknown>,
          })),
          toolResults: toolResultParts.map(tr => ({
            id: tr.toolCallId,
            name: tr.toolName,
            result: unwrapToolResultOutput(tr.output),
          })),
          isFinal,
          finishReason: typedInputData.stepResult?.reason || 'unknown',
          runId: runId,
          threadId: readScoped(scopeCtx, THREAD_ID_KEY, 'threadId'),
          resourceId: readScoped(scopeCtx, RESOURCE_ID_KEY, 'resourceId'),
          agentId: rest.agentId,
          agentName: rest.agentName || rest.agentId,
          messages: messageList.get.all.db(),
        };

        try {
          const iterationResult = await rest.onIterationComplete(iterationContext);

          // A terminal tool result is already the caller-visible answer. The
          // callback may observe that iteration, but its continuation feedback
          // must not become a hidden synthetic assistant message that appears
          // only in persisted history.
          if (iterationResult && !typedInputData.terminalToolResult) {
            if (iterationResult.feedback && typedInputData.stepResult?.isContinued) {
              messageList.add(
                {
                  id: rest.mastra?.generateId() || randomUUID(),
                  createdAt: new Date(),
                  type: 'text',
                  role: 'assistant',
                  content: {
                    parts: [
                      {
                        type: 'text',
                        text: iterationResult.feedback,
                      },
                    ],
                    metadata: {
                      mode: 'stream',
                      completionResult: {
                        suppressFeedback: true,
                      },
                    },
                    format: 2,
                  },
                } as MastraDBMessage,
                'response',
              );

              if (iterationResult.continue === false) {
                pendingFeedbackStop = true;
              } else if (!hasFinishedSteps && rest.maxSteps !== undefined && accumulatedSteps.length < rest.maxSteps) {
                hasFinishedSteps = false;
                typedInputData.stepResult.isContinued = true;
              }
            } else if (iterationResult.continue === false && !hasFinishedSteps) {
              hasFinishedSteps = true;
            } else if (
              iterationResult.continue === true &&
              (hasFinishedSteps || !typedInputData.stepResult?.isContinued)
            ) {
              // Forced continuation from a FINISHED state (the model returned
              // stop, or a stop condition fired). Setting the flags alone is
              // not enough: the finished iteration's response message is
              // closed, so the resumed provider call needs the same boundary
              // bookkeeping the pending-signals path performs above —
              // otherwise the loop re-enters against a sealed message and the
              // extra turn produces nothing caller-visible (live-diagnosed as
              // the silent-turn nudge failing to surface any text).
              if (rest.maxSteps === undefined || accumulatedSteps.length < rest.maxSteps) {
                if (iterationResult.feedback) {
                  messageList.add(
                    {
                      id: rest.mastra?.generateId() || randomUUID(),
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
                    } as MastraDBMessage,
                    'response',
                  );
                }
                // `turnContinues`: unlike the signal path above, nothing from the
                // user separates the sealed message from the one the resumed call
                // writes into. Both halves are the same caller-visible answer, so
                // the run's final text must be read across the pair.
                messageList.markResponseMessageBoundary(
                  typedInputData.stepResult?.messageId ?? typedInputData.messageId,
                  { turnContinues: true },
                );
                const forcedContinuationMessageId = rest.rotateResponseMessageId();
                typedInputData.messageId = forcedContinuationMessageId;
                hasFinishedSteps = false;
                if (typedInputData.stepResult) {
                  typedInputData.stepResult.messageId = forcedContinuationMessageId;
                  typedInputData.stepResult.isContinued = true;
                }
                typedInputData.messages = {
                  all: messageList.get.all.aiV5.model(),
                  user: messageList.get.input.aiV5.model(),
                  nonUser: messageList.get.response.aiV5.model(),
                };
              }
            }
          }
        } catch (error) {
          // Log error but don't fail the iteration
          rest.logger?.error('Error in onIterationComplete hook:', error);
        }
      }

      // Check if a delegation hook called ctx.bail() — stop the loop after this iteration
      if (!hasFinishedSteps && readScoped(scopeCtx, DELEGATION_BAILED_KEY, '_delegationBailed')) {
        hasFinishedSteps = true;
        writeScoped(scopeCtx, DELEGATION_BAILED_KEY, '_delegationBailed', false);
      }

      // A callback can abort after the predicate's entry check. Re-check at
      // the caller-visible commit point so terminal output never wins that
      // race or reaches persistence through the final stream state.
      if (rest.options?.abortSignal?.aborted) {
        typedInputData.terminalToolResult = undefined;
        hasFinishedSteps = true;
        if (typedInputData.stepResult) {
          typedInputData.stepResult.reason = 'abort';
          typedInputData.stepResult.isContinued = false;
        }
      }

      // Terminal tool results are a hard stop. User callbacks may observe the
      // terminal iteration but cannot spend another provider call by forcing it
      // to continue.
      if (typedInputData.terminalToolResult) {
        hasFinishedSteps = true;
      }

      if (typedInputData.stepResult) {
        typedInputData.stepResult.isContinued = hasFinishedSteps ? false : typedInputData.stepResult.isContinued;
      }

      // Emit step-finish for all cases except tripwire without any steps
      // When tripwire happens but we have steps (e.g., max retries exceeded), we still
      // need to emit step-finish so the stream properly finishes with all step data
      const hasSteps = (typedInputData.output?.steps?.length ?? 0) > 0;
      const shouldEmitStepFinish = typedInputData.stepResult?.reason !== 'tripwire' || hasSteps;

      // A terminal iteration is finalized by workflowLoopStream only after the
      // workflow transition has committed. Evented condition evaluation can be
      // replayed when publishing that transition fails; publishing either the
      // terminal answer or its step-finish here would duplicate caller-visible
      // output before the workflow has actually reached its terminal state.
      if (shouldEmitStepFinish && !typedInputData.terminalToolResult) {
        await outputWriter({
          type: 'step-finish',
          runId,
          from: ChunkFrom.AGENT,
          payload: typedInputData,
        });
      }

      const reason = typedInputData.stepResult?.reason;

      if (reason === undefined) {
        return false;
      }

      return typedInputData.stepResult?.isContinued ?? false;
    })
    .commit();
}

function unwrapToolResultOutput(output: unknown): unknown {
  if (!output || typeof output !== 'object' || Array.isArray(output)) {
    return output;
  }

  const record = output as Record<string, unknown>;
  if (!('value' in record)) {
    return output;
  }

  switch (record.type) {
    case 'text':
    case 'json':
    case 'error-text':
    case 'error-json':
    case 'content':
      return record.value;
    default:
      return output;
  }
}
