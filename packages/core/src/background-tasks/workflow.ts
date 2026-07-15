import { z } from 'zod';
import type { SuspendOptions } from '../workflows';
import { createStep, createWorkflow } from '../workflows/evented';
import type { BackgroundTaskManager } from './manager';
import { BACKGROUND_TASK_SHUTDOWN_ABORT_MESSAGE } from './shutdown';
import type { BackgroundTaskStatus } from './types';
import { BACKGROUND_TASK_WORKFLOW_ID } from './workflow-id';

export { BACKGROUND_TASK_WORKFLOW_ID } from './workflow-id';

const inputSchema = z.object({ taskId: z.string() });

const bodyIOSchema = z.object({
  taskId: z.string(),
  done: z.boolean().optional(),
  result: z.unknown().optional(),
});

const bodyOutputSchema = z.object({
  taskId: z.string(),
  done: z.boolean(),
  result: z.unknown().optional(),
});

const WORKFLOW_STATUS_TO_PERSIST = ['suspended', 'pending', 'paused', 'waiting'];

/**
 * Builds the per-task evented workflow that owns executor + retries.
 *
 * A single `run-attempt` step is the `dountil` body. It invokes the executor,
 * persists the outcome, advances retry bookkeeping, and returns whether the
 * loop is done. Keeping the durable loop body as a step gives each retry the
 * outer run's iteration identity; a nested durable workflow would need a
 * separate iteration-scoped ownership contract before it could be replayed
 * safely.
 *
 * Step bodies close over `manager` directly — the bg-tasks layer is the only
 * consumer of the `@internal` private fields.
 */
export function buildBackgroundTaskWorkflow(manager: BackgroundTaskManager) {
  const runAttemptStep = createStep({
    id: 'run-attempt',
    inputSchema: bodyIOSchema,
    outputSchema: bodyOutputSchema,
    execute: async ({ inputData, abortSignal: workflowAbortSignal, suspend, resumeData }) => {
      const { taskId } = inputData;
      const storage = await manager.getStorage();
      const task = await storage.getTask(taskId);
      if (!task || task.status === 'cancelled') {
        manager.deregisterTaskContext(taskId);
        return { taskId, done: true };
      }
      if (manager.isShuttingDown()) {
        manager.deregisterTaskContext(taskId);
        return { taskId, done: true };
      }

      // Resolve the executor. Two paths:
      //   1. Per-task `TaskContext` registered on the producer (in-process).
      //      Carries closure-captured state (e.g. agent memory hooks) and
      //      wins when present.
      //   2. Static executor registered by tool name. Used by remote workers
      //      that received the dispatch via PubSub and don't have access to
      //      the producer's per-task closure.
      const ctx = manager.taskContexts.get(taskId);
      const executor = ctx?.executor ?? manager.getStaticExecutor(task.toolName);
      if (!executor) {
        const errorInfo = {
          message:
            `No executor registered for tool "${task.toolName}". ` +
            `Register the tool on Mastra (so workers can resolve it cross-process) ` +
            `or run the task in the same process as the producer.`,
        };
        await storage.updateTask(taskId, { status: 'failed', error: errorInfo, completedAt: new Date() });
        const failedTask = await storage.getTask(taskId);
        if (failedTask) {
          await manager.runLocalCompletionHooks(failedTask, 'failed', { error: errorInfo });
          await manager.publishLifecycleEvent('task.failed', failedTask);
        }
        manager.deregisterTaskContext(taskId);
        return { taskId, done: true };
      }

      // Throttled progress publisher.
      const progressThrottleMs = manager.config.progressThrottleMs;
      const shouldThrottleProgress =
        typeof progressThrottleMs === 'number' && Number.isFinite(progressThrottleMs) && progressThrottleMs > 0;
      let lastProgressEmitMs: number | undefined;
      const onProgress = async (chunk: any) => {
        if (shouldThrottleProgress) {
          const now = Date.now();
          if (lastProgressEmitMs !== undefined && now - lastProgressEmitMs < progressThrottleMs!) return;
          lastProgressEmitMs = now;
        }
        await manager.publishLifecycleEvent('task.output', { ...task, chunk });
      };

      const abortController = new AbortController();
      manager.activeAbortControllers.set(taskId, abortController);
      if (manager.isShuttingDown()) {
        abortController.abort(new Error(BACKGROUND_TASK_SHUTDOWN_ABORT_MESSAGE));
        manager.activeAbortControllers.delete(taskId);
        manager.deregisterTaskContext(taskId);
        return { taskId, done: true };
      }
      // Wire the workflow's run-level abort signal into our local controller
      // so `workflow.getRun(taskId).cancel()` propagates to the executor.
      const onWorkflowAbort = () => abortController.abort(new Error('Task cancelled'));
      if (workflowAbortSignal.aborted) {
        abortController.abort(new Error('Task cancelled'));
      } else {
        workflowAbortSignal.addEventListener('abort', onWorkflowAbort, { once: true });
      }
      const timeoutHandle = setTimeout(() => {
        abortController.abort(new Error(`Task timed out after ${task.timeoutMs}ms`));
      }, task.timeoutMs);
      const wasShutdownAbort = () =>
        abortController.signal.aborted &&
        abortController.signal.reason instanceof Error &&
        abortController.signal.reason.message === BACKGROUND_TASK_SHUTDOWN_ABORT_MESSAGE;

      // Wrap the workflow runtime's `suspend` so we persist
      // `status: 'suspended'` + `suspendPayload`, fire the per-task
      // suspend hook (so the bg-task's `onResult` updates the agent's
      // message list), and publish the lifecycle event before
      // delegating. The runtime's `suspend` does not throw — it sets a
      // flag the step-executor reads after `execute` returns. We
      // capture the args here and call the runtime's suspend from the
      // step body after the executor returns, so `wrappedSuspend` can
      // safely run all its side effects synchronously inside the
      // tool's call.
      let pendingSuspend: { data?: unknown; suspendOptions?: SuspendOptions } | undefined;
      const wrappedSuspend = async (data?: unknown, suspendOptions?: SuspendOptions) => {
        if (wasShutdownAbort()) return;
        await storage.updateTask(taskId, {
          status: 'suspended',
          suspendPayload: data,
          suspendedAt: new Date(),
        });
        const suspendedTask = await storage.getTask(taskId);
        if (suspendedTask) {
          // Suspend is non-terminal — DO NOT use `runLocalCompletionHooks`
          // here. That helper deregisters the task context in its `finally`
          // block, which would strand the resume call (the workflow step
          // body re-enters and looks up `manager.taskContexts.get(taskId)`).
          await manager.runLocalSuspendHooks(suspendedTask);
          await manager.publishLifecycleEvent('task.suspended', suspendedTask);
        }
        pendingSuspend = { data, suspendOptions };
      };

      const persistAttemptOutcome = async ({
        outcome,
        result,
        error,
      }: {
        outcome: 'success' | 'retry' | 'cancelled' | 'timed_out';
        result?: unknown;
        error?: { message: string; stack?: string };
      }) => {
        const currentTask = await storage.getTask(taskId);
        if (!currentTask) return { taskId, done: true };

        if (outcome === 'cancelled') {
          manager.deregisterTaskContext(taskId);
          return { taskId, done: true };
        }

        if (outcome === 'timed_out') {
          const status = currentTask.status as string;
          if (status !== 'timed_out' && status !== 'cancelled') {
            await storage.updateTask(taskId, {
              status: 'timed_out',
              error: { message: `Task timed out after ${currentTask.timeoutMs}ms` },
              completedAt: new Date(),
            });
            const timedOutTask = await storage.getTask(taskId);
            if (timedOutTask) await manager.publishLifecycleEvent('task.failed', timedOutTask);
          }
          return { taskId, done: true };
        }

        if (outcome === 'success') {
          if ((currentTask.status as BackgroundTaskStatus) === 'cancelled') {
            manager.deregisterTaskContext(taskId);
            return { taskId, done: true };
          }
          await storage.updateTask(taskId, { status: 'completed', result, completedAt: new Date() });
          const completedTask = await storage.getTask(taskId);
          if (completedTask) {
            await manager.runLocalCompletionHooks(completedTask, 'completed', { result });
            await manager.publishLifecycleEvent('task.completed', completedTask);
          }
          return { taskId, done: true, result };
        }

        if (currentTask.retryCount < currentTask.maxRetries) {
          await storage.updateTask(taskId, {
            retryCount: currentTask.retryCount + 1,
            error: undefined,
            startedAt: new Date(),
          });
          return { taskId, done: false };
        }

        // A tool failure is a modeled background-task result, not an engine
        // failure. Persist it and end the loop cleanly; throwing from a loop
        // body would ask the workflow engine to retry the transition itself.
        const errorInfo = error ?? { message: 'Unknown error' };
        await storage.updateTask(taskId, { status: 'failed', error: errorInfo, completedAt: new Date() });
        const failedTask = await storage.getTask(taskId);
        if (failedTask) {
          await manager.runLocalCompletionHooks(failedTask, 'failed', { error: errorInfo });
          await manager.publishLifecycleEvent('task.failed', failedTask);
        }
        return { taskId, done: true };
      };

      let outcome: 'success' | 'retry' | 'cancelled' | 'timed_out';
      let attemptResult: unknown;
      let attemptError: { message: string; stack?: string } | undefined;
      try {
        attemptResult = await executor.execute(task.args, {
          abortSignal: abortController.signal,
          onProgress,
          suspend: wrappedSuspend,
          // On resume the runtime populates `resumeData`; undefined on
          // the initial run.
          resumeData,
        });

        if (pendingSuspend) {
          return suspend(pendingSuspend.data, pendingSuspend.suspendOptions as SuspendOptions);
        }

        if (wasShutdownAbort()) {
          manager.deregisterTaskContext(taskId);
          outcome = 'cancelled';
        } else {
          outcome = 'success';
        }
      } catch (error: any) {
        const currentTask = await storage.getTask(taskId);
        if (!currentTask || (currentTask.status as BackgroundTaskStatus) === 'cancelled') {
          manager.deregisterTaskContext(taskId);
          outcome = 'cancelled';
        } else if (wasShutdownAbort()) {
          // Shutdown aborts are local process teardown, not task timeouts.
          manager.deregisterTaskContext(taskId);
          outcome = 'cancelled';
        } else if (
          abortController.signal.aborted ||
          error?.name === 'AbortError' ||
          error?.message === 'Task cancelled' ||
          error?.message?.startsWith('Task timed out after ')
        ) {
          outcome = 'timed_out';
        } else {
          outcome = 'retry';
          attemptError = { message: error?.message ?? 'Unknown error', stack: error?.stack };
        }
      } finally {
        clearTimeout(timeoutHandle);
        workflowAbortSignal.removeEventListener('abort', onWorkflowAbort);
        manager.activeAbortControllers.delete(taskId);
      }

      return persistAttemptOutcome({ outcome, result: attemptResult, error: attemptError });
    },
  });

  return createWorkflow({
    id: BACKGROUND_TASK_WORKFLOW_ID,
    inputSchema,
    outputSchema: bodyOutputSchema,
    steps: [runAttemptStep],
    options: {
      shouldPersistSnapshot: ({ workflowStatus }) => WORKFLOW_STATUS_TO_PERSIST.includes(workflowStatus),
    },
  })
    .dountil(runAttemptStep, async ({ inputData }) => inputData?.done === true)
    .commit();
}
