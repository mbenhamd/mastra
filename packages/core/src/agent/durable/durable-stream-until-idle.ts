import { acquireStreamSlot, buildContinuationOpts, releaseStreamSlot, resolveStreamUntilIdleScope, TERMINAL_BG_CHUNKS } from "../shared/stream-until-idle";
/**
 * Implementation of `DurableAgent.streamUntilIdle`. Mirrors the regular
 * agent's `stream-until-idle.ts` but adapted for durable execution:
 * - `DurableAgent.stream()` returns `DurableAgentStreamResult` (not `MastraModelOutput`)
 * - Each continuation starts a new durable workflow (new runId)
 * - Cleanup functions from each inner stream are tracked and called on close
 *
 * High-level flow:
 * 1. Resolve memory scope (threadId, resourceId) -- falls through to plain
 *    `agent.stream` if no memory or bgManager.
 * 2. Register this call as the active wrapper for the scope, aborting any
 *    prior wrapper.
 * 3. Run initial turn via `agent.stream(messages, { _skipBgTaskWait: true })`
 *    and pipe its `fullStream` into a combined outer stream.
 * 4. Subscribe to `bgManager.stream(...)` for background task lifecycle
 *    events. On terminal events, queue a continuation.
 * 5. `maxIdleMs` fires only between turns when nothing is happening.
 */
import type { BackgroundTaskManager } from '../../background-tasks/manager';
import { deepMerge } from '../../utils';
import type { MessageListInput } from '../message-list';

import type { DurableAgent, DurableAgentStreamOptions, DurableAgentStreamResult } from './durable-agent';

export interface DurableStreamUntilIdleDeps {
  activeStreams: Map<string, () => void>;
  bgManager: BackgroundTaskManager | undefined;
}

export async function runDurableStreamUntilIdle<OUTPUT = undefined>(
  agent: DurableAgent<any, any, OUTPUT>,
  messages: MessageListInput,
  streamOptions: (DurableAgentStreamOptions<OUTPUT> & { maxIdleMs?: number }) | undefined,
  deps: DurableStreamUntilIdleDeps,
): Promise<DurableAgentStreamResult<OUTPUT>> {
  const { maxIdleMs: _maxIdleMs, ...restStreamOptions } = streamOptions ?? {};

  const defaultOptions = await agent.getDefaultOptions({
    requestContext: streamOptions?.requestContext,
  });
  const mergedOptions = deepMerge(
    defaultOptions as Record<string, unknown>,
    (restStreamOptions ?? {}) as Record<string, unknown>,
  ) as Record<string, any>;

  const scope = await resolveStreamUntilIdleScope(agent, mergedOptions);

  if (!deps.bgManager || !scope) {
    return (agent as any).stream(messages, restStreamOptions as any) as Promise<DurableAgentStreamResult<OUTPUT>>;
  }

  const { threadId, resourceId, scopeKey } = scope;
  const maxIdleMs = _maxIdleMs ?? 5 * 60_000;

  const baseContinuationOpts = {
    ...(restStreamOptions ?? {}),
    onFinish: undefined,
    _skipBgTaskWait: true,
  } as Record<string, any>;

  const initialStreamOpts = {
    ...(restStreamOptions ?? {}),
    _skipBgTaskWait: true,
  } as DurableAgentStreamOptions<OUTPUT>;

  // --- State ---
  const runningTaskIds = new Set<string>();
  const pendingCompletions: Array<Record<string, unknown>> = [];
  const processedCompletionIds = new Set<string>();
  const innerCleanups: Array<() => void> = [];
  let isProcessing = false;
  let closed = false;
  let idleTimer: ReturnType<typeof setTimeout> | undefined;
  let outerController!: ReadableStreamDefaultController<any>;
  const outerAbort = new AbortController();
  let firstRunId: string | undefined;

  // --- Close / idle timer ---
  const forceClose = () => {
    if (closed) return;
    closed = true;
    if (idleTimer) {
      clearTimeout(idleTimer);
      idleTimer = undefined;
    }
    outerAbort.abort();
    try {
      outerController.close();
    } catch {
      // already closed
    }
    for (const fn of innerCleanups) {
      try {
        fn();
      } catch {
        // ignore
      }
    }
    releaseStreamSlot(deps.activeStreams, scopeKey, forceClose);
  };

  const tryClose = () => {
    if (closed) return;
    if (isProcessing) return;
    if (runningTaskIds.size > 0) return;
    if (pendingCompletions.length > 0) return;
    forceClose();
  };

  const clearIdleTimer = () => {
    if (idleTimer) {
      clearTimeout(idleTimer);
      idleTimer = undefined;
    }
  };

  const updateIdleTimer = () => {
    if (closed) return;
    clearIdleTimer();
    if (isProcessing) return;
    if (runningTaskIds.size === 0) return;
    if (pendingCompletions.length > 0) return;
    idleTimer = setTimeout(forceClose, maxIdleMs);
  };

  // --- Stream plumbing ---
  const pipeInner = async (inner: ReadableStream<any>) => {
    const reader = inner.getReader();
    try {
      while (true) {
        if (outerAbort.signal.aborted) break;
        const { done, value } = await reader.read();
        if (done) break;
        clearIdleTimer();
        try {
          outerController.enqueue(value);
        } catch {
          break;
        }
        if (value && typeof value === 'object' && (value as any).type === 'background-task-started') {
          const taskId = (value as any).payload?.taskId;
          if (taskId) runningTaskIds.add(taskId);
        }
      }
    } finally {
      reader.releaseLock();
    }
  };

  const processIfIdle = async () => {
    if (isProcessing || closed || pendingCompletions.length === 0) return;
    isProcessing = true;
    try {
      const batch = pendingCompletions.splice(0, pendingCompletions.length);
      for (const chunk of batch) {
        const tid = (chunk as { payload?: { taskId?: string } }).payload?.taskId;
        if (tid) processedCompletionIds.add(tid);
      }
      const continuationOpts = buildContinuationOpts(baseContinuationOpts, restStreamOptions?.context as any[], batch);
      const inner = await (agent as any).stream([], continuationOpts);
      innerCleanups.push(inner.cleanup);
      await pipeInner(inner.fullStream);
    } catch (err) {
      try {
        outerController.error(err);
      } catch {
        // already closed
      }
      forceClose();
      return;
    } finally {
      isProcessing = false;
      if (pendingCompletions.length > 0) {
        void processIfIdle();
      } else {
        tryClose();
        updateIdleTimer();
      }
    }
  };

  // --- Setup ---
  acquireStreamSlot(deps.activeStreams, scopeKey, forceClose);

  (streamOptions as any)?.abortSignal?.addEventListener('abort', forceClose);

  const combinedStream = new ReadableStream<any>({
    start(controller) {
      outerController = controller;
    },
    cancel() {
      closed = true;
      outerAbort.abort();
      clearIdleTimer();
    },
  });

  // --- Subscribe to background task events ---
  const bgStream = deps.bgManager.stream({
    agentId: agent.id,
    threadId,
    resourceId,
    abortSignal: outerAbort.signal,
  });
  const bgReader = bgStream.getReader();
  void (async () => {
    try {
      while (true) {
        if (outerAbort.signal.aborted) break;
        const { done, value } = await bgReader.read();
        if (done) break;
        const chunk = value as { type?: string; payload?: Record<string, unknown> };
        if (!chunk || typeof chunk !== 'object' || typeof chunk.type !== 'string') continue;

        const taskId = (chunk.payload as { taskId?: string } | undefined)?.taskId;

        if (taskId && TERMINAL_BG_CHUNKS.has(chunk.type) && processedCompletionIds.has(taskId)) {
          continue;
        }

        updateIdleTimer();

        try {
          outerController.enqueue(chunk);
        } catch {
          break;
        }

        if (!taskId) continue;
        if (chunk.type === 'background-task-running') {
          runningTaskIds.add(taskId);
        } else if (TERMINAL_BG_CHUNKS.has(chunk.type)) {
          runningTaskIds.delete(taskId);
          pendingCompletions.push(chunk);
          void processIfIdle();
        }
      }
    } catch {
      // bg stream ended
    } finally {
      bgReader.releaseLock();
    }
  })();

  // --- Initial turn ---
  isProcessing = true;
  clearIdleTimer();
  let first: DurableAgentStreamResult<OUTPUT>;
  try {
    first = await (agent as any).stream(messages, initialStreamOpts);
  } catch (err) {
    forceClose();
    throw err;
  }
  firstRunId = first.runId;
  innerCleanups.push(first.cleanup);

  void (async () => {
    try {
      await pipeInner(first.fullStream);
    } catch (err) {
      try {
        outerController.error(err);
      } catch {
        // already closed
      }
    }
    isProcessing = false;
    if (pendingCompletions.length > 0) {
      void processIfIdle();
    } else {
      tryClose();
      updateIdleTimer();
    }
  })();

  return {
    output: new Proxy(first.output, {
      get(target, prop) {
        if (prop === 'fullStream') return combinedStream;
        const value = Reflect.get(target, prop, target);
        return typeof value === 'function' ? value.bind(target) : value;
      },
    }) as any,
    get fullStream() {
      return combinedStream;
    },
    runId: firstRunId!,
    threadId,
    resourceId,
    cleanup: forceClose,
  };
}
