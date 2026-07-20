/**
 * Implementation of `DurableAgent.streamUntilIdle` and
 * `DurableAgent.resume(..., { untilIdle })`. Mirrors the regular agent's
 * `stream-until-idle.ts` but adapted for durable execution:
 * - `DurableAgent.stream()` returns `DurableAgentStreamResult` (not `MastraModelOutput`)
 * - Each continuation starts a new durable workflow (new runId)
 * - Cleanup functions from each inner stream are tracked and called on close
 * - Inner `abort()` handles are fanned out so the outer `result.abort()`
 *   cancels every active durable run
 *
 * Uses the shared `runIdleLoop` helper from `loop/shared/stream-until-idle-helpers`
 * with durable-specific hooks for cleanup/abort tracking.
 */
import type { BackgroundTaskManager } from '../../background-tasks/manager';
import { runIdleLoop } from '../../loop/shared/stream-until-idle-helpers';
import type { AgentExecutionOptions } from '../agent.types';
import type { ResolvedAgentMemory } from '../execution-memory';
import type { MessageListInput } from '../message-list';

import type { DurableAgent, DurableAgentStreamOptions, DurableAgentStreamResult } from './durable-agent';

/**
 * Private handoff from the idle wrapper to DurableAgent.stream(). The wrapper
 * must resolve defaults to discover its memory scope; the inner durable
 * preparation reuses that exact resolution instead of invoking a dynamic
 * resolver a second time against potentially different runtime state.
 */
export const DURABLE_STREAM_UNTIL_IDLE_HANDOFF = Symbol('durableStreamUntilIdleHandoff');

export interface DurableStreamUntilIdleHandoff<OUTPUT = undefined> {
  defaultOptions: AgentExecutionOptions<OUTPUT>;
  resolvedMemory: ResolvedAgentMemory;
}

export interface DurableStreamUntilIdleDeps {
  activeStreams: Map<string, () => void>;
  bgManager: BackgroundTaskManager | undefined;
  prepareContinuation?: (resolvedOptions: Record<string, any>) => Promise<Record<string, any>>;
}

function agentForIdleLoop<OUTPUT>(
  agent: DurableAgent<any, any, OUTPUT>,
  resolvedDefaultOptions?: AgentExecutionOptions<OUTPUT>,
) {
  if (!resolvedDefaultOptions) return agent;
  let initialResolutionPending = true;
  return {
    id: agent.id,
    getDefaultOptions: async (options?: any) => {
      if (initialResolutionPending) {
        initialResolutionPending = false;
        return resolvedDefaultOptions;
      }
      return agent.getDefaultOptions(options);
    },
    getMemory: (options?: any) => agent.getMemory(options),
  };
}

/**
 * Run `DurableAgent.streamUntilIdle` (or `DurableAgent.stream({ untilIdle })`).
 * Initial turn invokes `agent.stream(messages, ...)`; continuations triggered
 * by background-task completions run as fresh `agent.stream([], ...)` calls
 * against the same memory thread.
 */
export async function runDurableStreamUntilIdle<OUTPUT = undefined>(
  agent: DurableAgent<any, any, OUTPUT>,
  messages: MessageListInput,
  streamOptions: (DurableAgentStreamOptions<OUTPUT> & { maxIdleMs?: number }) | undefined,
  deps: DurableStreamUntilIdleDeps,
  resolvedDefaultOptions?: AgentExecutionOptions<OUTPUT>,
): Promise<DurableAgentStreamResult<OUTPUT>> {
  // Durable-specific: track cleanup/abort handles from each inner stream
  const innerCleanups: Array<() => void> = [];
  const innerAborts: Array<(reason?: unknown) => void> = [];

  return runIdleLoop<
    ReturnType<typeof agentForIdleLoop<OUTPUT>>,
    DurableAgentStreamResult<OUTPUT>,
    DurableAgentStreamResult<OUTPUT>
  >(
    agentForIdleLoop(agent, resolvedDefaultOptions),
    streamOptions,
    deps,
    (opts, memory, defaultOptions, mergedOptions) =>
      (agent as any).stream(messages, {
        ...mergedOptions,
        _skipBgTaskWait: opts._skipBgTaskWait,
        [DURABLE_STREAM_UNTIL_IDLE_HANDOFF]: {
          defaultOptions: defaultOptions as AgentExecutionOptions<OUTPUT>,
          resolvedMemory: { value: memory },
        } satisfies DurableStreamUntilIdleHandoff<OUTPUT>,
      }) as Promise<DurableAgentStreamResult<OUTPUT>>,
    (opts, memory, defaultOptions) =>
      (agent as any).stream([], {
        ...opts,
        [DURABLE_STREAM_UNTIL_IDLE_HANDOFF]: {
          defaultOptions: defaultOptions as AgentExecutionOptions<OUTPUT>,
          resolvedMemory: { value: memory },
        } satisfies DurableStreamUntilIdleHandoff<OUTPUT>,
      }) as Promise<{ fullStream: ReadableStream<any> }>,
    (first, ctx) => {
      // No ctx means no bgManager / no memory — fall through without wrapping.
      if (!ctx) return first;

      return {
        output: new Proxy(first.output, {
          get(target, prop) {
            if (prop === 'fullStream') return ctx.combinedStream;
            const value = Reflect.get(target, prop, target);
            return typeof value === 'function' ? value.bind(target) : value;
          },
        }) as any,
        get fullStream() {
          return ctx.combinedStream;
        },
        runId: first.runId,
        threadId: ctx.threadId,
        resourceId: ctx.resourceId,
        cleanup: ctx.forceClose,
        abort: (reason?: unknown) => {
          // Fan the abort out to every inner DurableAgent.stream() that has been
          // spawned by the idle loop so far. `forceClose` then unwinds the outer
          // stream + idle timer.
          for (const innerAbort of innerAborts) {
            try {
              innerAbort(reason);
            } catch {
              // ignore — best-effort abort across siblings
            }
          }
          ctx.forceClose();
        },
      };
    },
    {
      onInnerResult: (inner: any) => {
        if (typeof inner.cleanup === 'function') innerCleanups.push(inner.cleanup);
        if (typeof inner.abort === 'function') innerAborts.push(inner.abort);
      },
      onForceClose: () => {
        for (const fn of innerCleanups) {
          try {
            fn();
          } catch {
            // ignore
          }
        }
      },
    },
  );
}

/**
 * Run `DurableAgent.resume(..., { untilIdle })`. Same idle-loop semantics as
 * `runDurableStreamUntilIdle` — initial turn calls `agent.resume(runId,
 * resumeData, ...)` against the existing run snapshot, and subsequent
 * continuations triggered by background-task completions use
 * `agent.stream([], continuationOpts)` (a normal multi-turn agent stream)
 * since the resume completes and we're back in regular conversation flow.
 */
export async function runResumeDurableStreamUntilIdle<OUTPUT = undefined>(
  agent: DurableAgent<any, any, OUTPUT>,
  runId: string,
  resumeData: unknown,
  streamOptions: (DurableAgentStreamOptions<OUTPUT> & { maxIdleMs?: number }) | undefined,
  deps: DurableStreamUntilIdleDeps,
  resolvedDefaultOptions?: AgentExecutionOptions<OUTPUT>,
): Promise<DurableAgentStreamResult<OUTPUT>> {
  const innerCleanups: Array<() => void> = [];
  const innerAborts: Array<(reason?: unknown) => void> = [];

  return runIdleLoop<
    ReturnType<typeof agentForIdleLoop<OUTPUT>>,
    DurableAgentStreamResult<OUTPUT>,
    DurableAgentStreamResult<OUTPUT>
  >(
    agentForIdleLoop(agent, resolvedDefaultOptions),
    streamOptions,
    deps,
    (_opts, _memory, _defaultOptions, mergedOptions) =>
      (agent as any).resume(runId, resumeData, mergedOptions) as Promise<DurableAgentStreamResult<OUTPUT>>,
    (opts, memory, defaultOptions) =>
      (agent as any).stream([], {
        ...opts,
        [DURABLE_STREAM_UNTIL_IDLE_HANDOFF]: {
          defaultOptions: defaultOptions as AgentExecutionOptions<OUTPUT>,
          resolvedMemory: { value: memory },
        } satisfies DurableStreamUntilIdleHandoff<OUTPUT>,
      }) as Promise<{ fullStream: ReadableStream<any> }>,
    (first, ctx) => {
      if (!ctx) return first;

      return {
        output: new Proxy(first.output, {
          get(target, prop) {
            if (prop === 'fullStream') return ctx.combinedStream;
            const value = Reflect.get(target, prop, target);
            return typeof value === 'function' ? value.bind(target) : value;
          },
        }) as any,
        get fullStream() {
          return ctx.combinedStream;
        },
        runId: first.runId,
        threadId: ctx.threadId,
        resourceId: ctx.resourceId,
        cleanup: ctx.forceClose,
        abort: (reason?: unknown) => {
          for (const innerAbort of innerAborts) {
            try {
              innerAbort(reason);
            } catch {
              // ignore
            }
          }
          ctx.forceClose();
        },
      };
    },
    {
      onInnerResult: (inner: any) => {
        if (typeof inner.cleanup === 'function') innerCleanups.push(inner.cleanup);
        if (typeof inner.abort === 'function') innerAborts.push(inner.abort);
      },
      onForceClose: () => {
        for (const fn of innerCleanups) {
          try {
            fn();
          } catch {
            // ignore
          }
        }
      },
    },
  );
}
