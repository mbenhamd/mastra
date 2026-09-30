import type { ToolSet } from '@internal/ai-sdk-v5';
import { safeEnqueue } from '../../../stream/base/input';
import type { ChunkType } from '../../../stream/types';
import { createStep } from '../../../workflows/workflow';
import { readScoped } from '../../run-scope-access';
import {
  AGENT_BACKGROUND_CONFIG_KEY,
  BACKGROUND_TASK_MANAGER_CONFIG_KEY,
  BACKGROUND_TASK_MANAGER_KEY,
  RESOURCE_ID_KEY,
  SKIP_BG_TASK_WAIT_KEY,
  THREAD_ID_KEY,
} from '../../run-scope-keys';
import { checkBackgroundTasks } from '../../shared/steps/background-task-check-core';
import type { OuterLLMRun } from '../../types';
import { llmIterationOutputSchema } from '../schema';
import type { LLMIterationData } from '../schema';

/**
 * Step that checks for pending background tasks after the LLM has responded.
 * Behavior lives in the shared `checkBackgroundTasks` core; this glue owns
 * RunScope access, the main loop's wait gate (first invocation or no
 * configured timeout → signal pending without blocking), and projection back
 * onto `LLMIterationData`.
 */
export function createBackgroundTaskCheckStep<Tools extends ToolSet = ToolSet, OUTPUT = undefined>({
  _internal,
  controller,
  runId,
  agentId,
  mastra,
}: OuterLLMRun<Tools, OUTPUT>) {
  const scopeCtx = { mastra, runId, _internal };
  return createStep({
    id: 'backgroundTaskCheckStep',
    inputSchema: llmIterationOutputSchema,
    outputSchema: llmIterationOutputSchema,
    execute: async ({ inputData, retryCount }) => {
      const typedInput = inputData as LLMIterationData<Tools, OUTPUT>;
      const outcome = await checkBackgroundTasks({
        bgManager: readScoped(scopeCtx, BACKGROUND_TASK_MANAGER_KEY, 'backgroundTaskManager'),
        runId,
        agentId,
        threadId: readScoped(scopeCtx, THREAD_ID_KEY, 'threadId'),
        resourceId: readScoped(scopeCtx, RESOURCE_ID_KEY, 'resourceId'),
        skipWait: readScoped(scopeCtx, SKIP_BG_TASK_WAIT_KEY, 'skipBgTaskWait'),
        retryCount,
        // Wait timeout: agent config → manager config. Never wait on the
        // first invocation or without an explicit timeout — background
        // results reach the live stream controller even after the run ends.
        resolveWaitMs: rc => {
          const agentBgConfig = readScoped(scopeCtx, AGENT_BACKGROUND_CONFIG_KEY, 'agentBackgroundConfig');
          const managerConfig = readScoped(scopeCtx, BACKGROUND_TASK_MANAGER_CONFIG_KEY, 'backgroundTaskManagerConfig');
          const waitTimeoutMs = agentBgConfig?.waitTimeoutMs ?? managerConfig?.waitTimeoutMs;
          return rc === 0 || !waitTimeoutMs ? undefined : waitTimeoutMs;
        },
        emitChunk: chunk => {
          safeEnqueue(controller, chunk as ChunkType<OUTPUT>);
        },
      });

      switch (outcome.status) {
        case 'pass-through':
          return typedInput;
        // A previously dispatched background task is still part of this run's
        // answer. Do not let an unrelated foreground success terminate while
        // that task is pending or while its completion needs another model turn.
        case 'timeout':
        case 'pending':
        case 'completed': {
          // The pending marker tells `isTaskCompleteStep` to skip scoring and
          // the loop to keep the run open; dropping the terminal result
          // prevents an unrelated foreground success from ending the answer
          // early.
          const inputWithoutTerminal = { ...typedInput, terminalToolResult: undefined };
          if (outcome.status === 'completed') {
            // Force the loop to continue so the LLM processes the injected result
            return {
              ...inputWithoutTerminal,
              backgroundTaskPending: true,
              ...(typedInput.stepResult ? { stepResult: { ...typedInput.stepResult, isContinued: true } } : {}),
            };
          }
          // Timeout: keep the pending marker but do not set isContinued, so the
          // foreground loop can end without losing the background lifecycle.
          return { ...inputWithoutTerminal, backgroundTaskPending: true };
        }
      }
    },
  });
}
