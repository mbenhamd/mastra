/**
 * Implementation of `Agent.streamUntilIdle`. Extracted from `agent.ts` to
 * keep that file focused on the public Agent surface. `Agent.streamUntilIdle`
 * is a thin delegate that forwards to `runStreamUntilIdle(this, ..., deps)`.
 *
 * High-level flow:
 * 1. Resolve memory / thread / resource scope (early-return to `agent.stream`
 *    if no memory backend exists — continuations require memory).
 * 2. Register this call as the active wrapper for `(threadId, resourceId)`,
 *    aborting any prior wrapper for the same scope (prevents duplicate
 *    bg-task event fan-out across concurrent calls).
 * 3. Run the initial turn via `agent.stream(...)` and pipe its `fullStream`
 *    into our own combined outer stream.
 * 4. Subscribe to `BackgroundTaskManager.stream(...)` for this scope; when a
 *    terminal bg event arrives, queue it and (when the outer is idle between
 *    turns) re-invoke the agent with a directive listing the just-completed
 *    tool-call IDs. Dedup set guards against at-least-once pubsub delivery.
 * 5. `maxIdleMs` only runs while the wrapper is between turns (not during an
 *    active inner stream) so slow first-tokens don't close the stream.
 */
import type { BackgroundTaskManager } from '../background-tasks/manager';
import { runIdleLoop } from '../loop/shared/stream-until-idle-helpers';
import type { MastraModelOutput } from '../stream/base/output';
import type { Agent } from './agent';
import type { ResolvedAgentMemory, ResolvedAgentMemoryHandoff } from './execution-memory';
import type { MessageListInput } from './message-list';

/**
 * [PF-523] Symbol key that carries preflight-resolved default options from
 * `agent.streamUntilIdle` / `agent.resumeStreamUntilIdle` into the idle-loop
 * wrapper and the inner `agent.stream` / `agent.resumeStream` calls. When
 * present, the wrapper must NOT re-resolve `getDefaultOptions` (the caller
 * already ran the execution preflight against these options).
 */
export const STREAM_UNTIL_IDLE_DEFAULT_OPTIONS = Symbol('streamUntilIdleDefaultOptions');

/**
 * [PF-523] Build the agent handle handed to `runIdleLoop`. When the caller
 * preflighted default options (streamUntilIdle / resumeStreamUntilIdle with
 * FGA or a request-context schema), return a structural shim whose
 * `getDefaultOptions` yields those options so the idle loop reuses them
 * instead of resolving defaults a second time.
 */
function agentForIdleLoop(
  agent: Agent<any, any, any, any>,
  streamOptions: Record<string, any> | undefined,
): { id: string; getDefaultOptions: (opts?: any) => any | Promise<any>; getMemory: (opts?: any) => Promise<any> } {
  const preflightedDefaultOptions = (streamOptions as Record<string | symbol, any> | undefined)?.[
    STREAM_UNTIL_IDLE_DEFAULT_OPTIONS
  ] as Record<string, any> | undefined;
  if (!preflightedDefaultOptions) return agent;
  return {
    id: agent.id,
    getDefaultOptions: async () => preflightedDefaultOptions,
    getMemory: (opts?: any) => agent.getMemory(opts),
  };
}

/**
 * Dependencies the extracted function needs access to that it can't reach
 * through the public `Agent` surface (e.g. private fields).
 */
export interface StreamUntilIdleDeps {
  /**
   * Map tracking the active `streamUntilIdle` wrapper per scope on the
   * calling Agent. The extracted function reads/writes this map directly so
   * a new call for the same scope can abort any prior still-open wrapper.
   * Lives as `#activeStreamUntilIdle` on the Agent instance.
   */
  activeStreams: Map<string, () => void>;
  /**
   * Optional background task manager resolved from Mastra. When absent,
   * `runStreamUntilIdle` falls through to a plain `agent.stream` call.
   */
  bgManager: BackgroundTaskManager | undefined;
  /** Keep live memory in runScope and pass only its random token downstream. */
  prepareResolvedMemoryHandoff: (
    runId: string,
    resolvedMemory: ResolvedAgentMemory,
    executionMemoryId?: string,
  ) => Promise<ResolvedAgentMemoryHandoff>;
  /** Generate the concrete run id required to key a fresh execution handoff. */
  resolveRunId: (options: Record<string, any>) => string;
}

function withDefaultOptions(
  options: Record<string, any>,
  defaultOptions: Record<string, any>,
): Record<string | symbol, any> {
  return {
    ...options,
    [STREAM_UNTIL_IDLE_DEFAULT_OPTIONS]: defaultOptions,
  };
}

/**
 * Run `agent.streamUntilIdle`. See the module doc above for the high-level
 * flow. Returns a `MastraModelOutput` whose `fullStream` spans the initial
 * turn PLUS any continuations triggered by background task completions.
 *
 * Aggregate properties (`text`, `toolCalls`, `toolResults`, `finishReason`,
 * `messageList`, `getFullOutput()`) still resolve against the first turn's
 * internal buffer. Consumers who need an aggregated view should read
 * `fullStream` and accumulate, or follow up with `agent.generate(...)`.
 */
export async function runStreamUntilIdle<OUTPUT>(
  agent: Agent<any, any, any, any>,
  messages: MessageListInput,
  streamOptions: (Record<string, any> & { maxIdleMs?: number }) | undefined,
  deps: StreamUntilIdleDeps,
  executionMemoryId?: string,
): Promise<MastraModelOutput<OUTPUT>> {
  return runIdleLoop<ReturnType<typeof agentForIdleLoop>, MastraModelOutput<OUTPUT>, MastraModelOutput<OUTPUT>>(
    agentForIdleLoop(agent, streamOptions),
    streamOptions,
    deps,
    async (opts, memory, defaultOptions, mergedOptions) => {
      const runId = mergedOptions.runId ?? deps.resolveRunId(mergedOptions);
      const handoff = await deps.prepareResolvedMemoryHandoff(runId, { value: memory }, executionMemoryId);
      try {
        return await (agent.stream as any)(
          messages,
          withDefaultOptions({ ...opts, runId }, defaultOptions),
          handoff.executionMemoryId,
        );
      } finally {
        handoff.release();
      }
    },
    opts => (agent.stream as any)([], opts) as Promise<{ fullStream: ReadableStream<any> }>,
    (first, ctx) => {
      // No ctx means no bgManager / no memory — fall through without wrapping.
      if (!ctx) return first;

      // Wrap the first turn's MastraModelOutput so `fullStream` returns our
      // combined stream (initial + continuations) while `text`, `finishReason`,
      // `toolCalls`, etc. still work — they resolve against the first turn's
      // internal event buffer, which gets populated as we consume its fullStream.
      return new Proxy(first, {
        get(target, prop) {
          if (prop === 'fullStream') return ctx.combinedStream;
          // Read target's own property with `this === target` so any internal
          // getters (e.g. `#getDelayedPromise`) don't recurse through the proxy
          // and hit our overridden fullStream.
          const value = Reflect.get(target, prop, target);
          return typeof value === 'function' ? value.bind(target) : value;
        },
      }) as MastraModelOutput<OUTPUT>;
    },
  );
}

/**
 * Run `agent.resumeStreamUntilIdle`. Same idle-loop semantics as
 * `runStreamUntilIdle` — initial turn calls `agent.resumeStream(resumeData,
 * ...)` against the existing run snapshot identified by `streamOptions.runId`,
 * and any subsequent continuations triggered by background-task completions
 * use `agent.stream([], continuationOpts)` (a normal multi-turn agent stream)
 * since the resume completes and we're back in regular conversation flow.
 *
 * `streamOptions` should include `runId` (required by `resumeStream` to load
 * the snapshot) and may include `toolCallId` if the resume is targeting a
 * specific suspended tool call. `maxIdleMs` works the same way as in
 * `streamUntilIdle`.
 */
export async function runResumeStreamUntilIdle<OUTPUT>(
  agent: Agent<any, any, any, any>,
  resumeData: any,
  streamOptions: (Record<string, any> & { maxIdleMs?: number; runId?: string; toolCallId?: string }) | undefined,
  deps: StreamUntilIdleDeps,
  executionMemoryId?: string,
): Promise<MastraModelOutput<OUTPUT>> {
  return runIdleLoop<ReturnType<typeof agentForIdleLoop>, MastraModelOutput<OUTPUT>, MastraModelOutput<OUTPUT>>(
    agentForIdleLoop(agent, streamOptions),
    streamOptions,
    deps,
    async (opts, memory, defaultOptions, mergedOptions) => {
      const runId = mergedOptions.runId as string | undefined;
      const resolvedOptions = withDefaultOptions(runId ? { ...opts, runId } : opts, defaultOptions);
      if (!runId) {
        // Preserve resumeStream's missing-run error path; it exits before
        // #execute, so there is no second memory resolution to suppress.
        return (agent.resumeStream as any)(resumeData, resolvedOptions, executionMemoryId);
      }

      const handoff = await deps.prepareResolvedMemoryHandoff(runId, { value: memory }, executionMemoryId);
      try {
        return await (agent.resumeStream as any)(resumeData, resolvedOptions, handoff.executionMemoryId);
      } finally {
        handoff.release();
      }
    },
    opts => (agent.stream as any)([], opts) as Promise<{ fullStream: ReadableStream<any> }>,
    (first, ctx) => {
      if (!ctx) return first;
      return new Proxy(first, {
        get(target, prop) {
          if (prop === 'fullStream') return ctx.combinedStream;
          const value = Reflect.get(target, prop, target);
          return typeof value === 'function' ? value.bind(target) : value;
        },
      }) as MastraModelOutput<OUTPUT>;
    },
  );
}
