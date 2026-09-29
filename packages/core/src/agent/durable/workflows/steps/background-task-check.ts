import { z } from 'zod';
import { ErrorCategory, ErrorDomain, MastraError } from '../../../../error';
import type { PubSub } from '../../../../events/pubsub';
import { checkBackgroundTasks } from '../../../../loop/shared/steps/background-task-check-core';
import { PUBSUB_SYMBOL } from '../../../../workflows/constants';
import { createStep } from '../../../../workflows/workflow';
import { ensureRemoteAbortListener } from '../../abort-transport';
import { DurableStepIds } from '../../constants';
import { getBoundRunRegistryEntry } from '../../run-registry';
import { emitChunkEvent } from '../../stream-adapter';

const BG_CHECK_STEP_ID = `${DurableStepIds.AGENTIC_EXECUTION}-bg-task-check`;

/**
 * The background task check step accepts the output of llmMappingStep
 * and passes it through, adding backgroundTaskPending if tasks are running.
 */
const bgCheckInputSchema = z.any();
const bgCheckOutputSchema = z.any();

/**
 * Create a durable background task check step. Behavior lives in the shared
 * `checkBackgroundTasks` core; this glue owns registry/init-data access, the
 * durable wait gate, and pubsub emission.
 *
 * The durable wait gate differs from the main loop's on purpose: the regular
 * agent can skip waiting because background tool-result chunks are pushed
 * directly into the live ReadableStream controller — that works even after
 * this step returns. The durable agent emits tool-result chunks via pubsub,
 * and the subscription is torn down when the stream closes. If this step
 * returned without waiting, the workflow would finish, FINISH would fire,
 * cleanup would run, and the subscriber would be gone before the background
 * task could deliver its result. Therefore the durable agent must always
 * wait when tasks are running — using the configured waitTimeoutMs, or a 1 s
 * default to keep the workflow (and pubsub) alive. It only signals pending
 * without blocking on retryCount 0 when an explicit timeout is configured
 * (meaning the caller drives continuation externally).
 */
export function createDurableBackgroundTaskCheckStep() {
  return createStep({
    id: BG_CHECK_STEP_ID,
    inputSchema: bgCheckInputSchema,
    outputSchema: bgCheckOutputSchema,
    execute: async params => {
      const { inputData, getInitData, retryCount } = params;
      const pubsub = (params as any)[PUBSUB_SYMBOL] as PubSub | undefined;
      const typedInput = inputData as Record<string, any>;

      const initData = getInitData<{
        runId: string;
        runtimeBindingId?: string;
        agentId: string;
        runtimeResolution?: 'registry-required';
        options?: { skipBgTaskWait?: boolean };
        state?: { threadId?: string; resourceId?: string };
      }>();
      const { runId, runtimeBindingId, agentId } = initData;
      const registryEntryBeforeAbortListener = getBoundRunRegistryEntry(runId, runtimeBindingId);
      if (
        (!registryEntryBeforeAbortListener || registryEntryBeforeAbortListener.isPlaceholder === true) &&
        initData.runtimeResolution === 'registry-required'
      ) {
        throw new MastraError({
          id: 'DURABLE_AGENT_RUNTIME_REGISTRY_MISSING',
          domain: ErrorDomain.AGENT,
          category: ErrorCategory.SYSTEM,
          text: `DurableAgent runtime dependencies are unavailable for run "${runId}". Resume the run through DurableAgent so recovery checks can restore them.`,
          details: { agentId, runId },
        });
      }

      if (pubsub) {
        try {
          await ensureRemoteAbortListener(pubsub, runId, runtimeBindingId);
        } catch (error) {
          params.mastra?.getLogger?.()?.warn?.('Failed to subscribe to cross-process abort requests', {
            runId,
            error: error instanceof Error ? error.message : String(error),
          });
        }
      }

      const registryEntry = getBoundRunRegistryEntry(runId, runtimeBindingId);
      const bgManager = registryEntry?.backgroundTaskManager;

      const outcome = await checkBackgroundTasks({
        bgManager,
        runId,
        agentId,
        threadId: initData.state?.threadId,
        resourceId: initData.state?.resourceId,
        skipWait: initData.options?.skipBgTaskWait,
        retryCount,
        resolveWaitMs: rc => {
          const waitTimeoutMs = registryEntry?.backgroundTasksConfig?.waitTimeoutMs ?? bgManager?.config?.waitTimeoutMs;
          return rc === 0 && waitTimeoutMs ? undefined : (waitTimeoutMs ?? 1000);
        },
        emitChunk: chunk => {
          if (!pubsub) return;
          return emitChunkEvent(pubsub, runId, chunk as any);
        },
      });

      switch (outcome.status) {
        case 'pass-through':
          return typedInput;
        // A previously dispatched background task is still part of this run's
        // answer. Do not let an unrelated foreground success terminate while
        // that task is pending or while its completion needs another model
        // turn. The pending marker tells `isTaskCompleteStep` to skip scoring
        // and the loop to keep the run open; dropping the terminal result
        // prevents an unrelated foreground success from ending the answer
        // early.
        case 'timeout':
        case 'pending': {
          const inputWithoutTerminal = { ...typedInput, terminalToolResult: undefined };
          // Timeout: keep the pending marker but do not set isContinued, so the
          // foreground loop can end without misreporting the still-running
          // background work (durable contract: pubsub teardown races a late
          // task result, so the pending marker is what carries the lifecycle).
          return { ...inputWithoutTerminal, backgroundTaskPending: true };
        }
        case 'completed': {
          const inputWithoutTerminal = { ...typedInput, terminalToolResult: undefined };
          // Force the loop to continue so the LLM processes the injected result
          if (typedInput.stepResult) {
            return {
              ...inputWithoutTerminal,
              backgroundTaskPending: true,
              stepResult: { ...typedInput.stepResult, isContinued: true },
            };
          }
          return { ...inputWithoutTerminal, backgroundTaskPending: true };
        }
      }
    },
  });
}
