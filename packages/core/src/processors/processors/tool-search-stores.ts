import type { ProcessInputStepArgs } from '../index';

const CURRENT_RUN_LOADED_TOOLS_KEY = 'toolSearchLoadedTools';
const CURRENT_RUN_CONTEXT_INITIALIZED_KEY = 'toolSearchContextInitialized';
export const TOOL_SEARCH_AUTO_LOAD_ACTIVATION_TYPE = 'tool-search-auto-load';
export const TOOL_SEARCH_AUTO_LOAD_ACTIVATION_VERSION = 1;

/**
 * Context handed to a {@link LoadedToolStore} on every operation.
 */
export interface LoadedToolStoreContext {
  /** Thread ID from the request context, or undefined when no thread is active. */
  threadId: string | undefined;
  /**
   * The current processInputStep arguments (exposes messages, requestContext, etc).
   * May be undefined on resume paths that resolve loaded state without a live step.
   */
  args?: ProcessInputStepArgs;
  /**
   * Persisted conversation messages supplied by a resume/recovery boundary
   * before a live `processInputStep` exists.
   */
  messages?: ProcessInputStepArgs['messages'];
}

/**
 * Backend abstraction for tracking which tools are "loaded" for a conversation.
 *
 * Two implementations ship with Mastra:
 * - {@link LegacyMapLoadedToolStore} (default) keeps loaded state in an in-memory
 *   map with TTL cleanup. This preserves the original processor behavior.
 * - {@link ContextLoadedToolStore} ('context' mode, opt-in) derives loaded state
 *   from the conversation messages. Restart-safe, requires no memory, and de-loads
 *   automatically when a result block is no longer present in the messages — parity
 *   with native provider tool-search.
 */
export interface LoadedToolStore {
  /** Resolve the set of currently-loaded tool names for this context. */
  getLoadedNames(ctx: LoadedToolStoreContext): Promise<Set<string>> | Set<string>;
  /** Record one or more tool names as loaded. */
  addLoaded(names: string[], ctx: LoadedToolStoreContext): Promise<void> | void;
  /** Clear processor-owned state for one thread, if the backend has any. */
  clearState(threadId: string): void;
  /** Clear all processor-owned state for this store. */
  clearAllState(): void;
  /** Release timers and processor-owned state. Safe to call more than once. */
  dispose(): void;
}

/**
 * Reads a canonical `load_tool` result. Discovery results are deliberately not
 * accepted here: only the `loaded` array emitted after a load-phase policy check
 * is activation evidence.
 */
function extractLoadToolNames(result: unknown): string[] {
  if (!result || typeof result !== 'object') return [];

  const candidate = result as { success?: unknown; loaded?: unknown };
  if (typeof candidate.success !== 'boolean') return [];

  const maybeLoaded = candidate.loaded;
  if (!Array.isArray(maybeLoaded)) return [];

  return maybeLoaded.filter((name): name is string => typeof name === 'string' && name.length > 0);
}

/**
 * Reads the explicit activation receipt emitted by `search_tools` in auto-load
 * mode. The marked names must exactly match the canonical search result names;
 * ordinary discovery-only results therefore cannot be replayed as loaded tools.
 */
function extractAutoLoadNames(result: unknown): string[] {
  if (!result || typeof result !== 'object') return [];

  const candidate = result as { results?: unknown; activation?: unknown };
  if (!Array.isArray(candidate.results) || !candidate.activation || typeof candidate.activation !== 'object') {
    return [];
  }

  const resultNames: string[] = [];
  for (const entry of candidate.results) {
    const searchResult = entry as { name?: unknown; description?: unknown; score?: unknown };
    if (
      typeof searchResult.name !== 'string' ||
      searchResult.name.length === 0 ||
      typeof searchResult.description !== 'string' ||
      typeof searchResult.score !== 'number' ||
      !Number.isFinite(searchResult.score)
    ) {
      return [];
    }
    resultNames.push(searchResult.name);
  }

  const activation = candidate.activation as { type?: unknown; version?: unknown; loaded?: unknown };
  if (
    activation.type !== TOOL_SEARCH_AUTO_LOAD_ACTIVATION_TYPE ||
    activation.version !== TOOL_SEARCH_AUTO_LOAD_ACTIVATION_VERSION ||
    !Array.isArray(activation.loaded)
  ) {
    return [];
  }

  const loadedNames: string[] = [];
  for (const name of activation.loaded) {
    if (typeof name !== 'string' || name.length === 0) return [];
    loadedNames.push(name);
  }

  const resultSet = new Set(resultNames);
  const loadedSet = new Set(loadedNames);
  if (
    loadedNames.length === 0 ||
    resultSet.size !== resultNames.length ||
    loadedSet.size !== loadedNames.length ||
    resultSet.size !== loadedSet.size ||
    [...resultSet].some(name => !loadedSet.has(name))
  ) {
    return [];
  }

  return loadedNames;
}

/**
 * Scans conversation messages for completed `search_tools` / `load_tool` invocations
 * and unions the tool names they activated.
 */
export function deriveLoadedNamesFromMessages(args: Pick<ProcessInputStepArgs, 'messages'>): Set<string> {
  const loaded = new Set<string>();

  if (!Array.isArray(args.messages)) return loaded;

  for (const message of args.messages) {
    // Tool results persisted by Mastra belong to assistant messages. Ignore
    // user-authored tool-shaped content instead of treating it as authorization.
    if (message.role !== 'assistant') continue;

    const parts = message.content?.parts;
    if (!parts) continue;

    for (const part of parts) {
      if (part.type !== 'tool-invocation') continue;
      const invocation = part.toolInvocation;
      if (!invocation) continue;
      if (invocation.toolName !== 'search_tools' && invocation.toolName !== 'load_tool') continue;
      if (invocation.state !== 'result') continue;

      const names =
        invocation.toolName === 'load_tool'
          ? extractLoadToolNames(invocation.result)
          : extractAutoLoadNames(invocation.result);
      for (const name of names) {
        loaded.add(name);
      }
    }
  }

  return loaded;
}

/**
 * 'context' mode store. Canonical assistant tool results in conversation messages
 * are the durable source of truth. A serializable snapshot in `args.state` bridges
 * execution to later steps in the same ProcessorRunner request, including requests
 * without a thread ID. The snapshot is never shared between requests, so an aborted
 * or concurrent run cannot contaminate another run for the same thread.
 */
export class ContextLoadedToolStore implements LoadedToolStore {
  private getCurrentRunLoadedNames(state: Record<string, unknown>): Set<string> {
    const value = state[CURRENT_RUN_LOADED_TOOLS_KEY];
    if (!Array.isArray(value)) return new Set();

    return new Set(value.filter((name): name is string => typeof name === 'string' && name.length > 0));
  }

  private getOrInitializeCurrentRun(ctx: LoadedToolStoreContext): Set<string> {
    const args = ctx.args!;
    const loaded = this.getCurrentRunLoadedNames(args.state);

    if (args.state[CURRENT_RUN_CONTEXT_INITIALIZED_KEY] !== true) {
      for (const name of deriveLoadedNamesFromMessages(args)) loaded.add(name);
      args.state[CURRENT_RUN_LOADED_TOOLS_KEY] = [...loaded];
      args.state[CURRENT_RUN_CONTEXT_INITIALIZED_KEY] = true;
    }

    return loaded;
  }

  getLoadedNames(ctx: LoadedToolStoreContext): Set<string> {
    if (ctx.args) return this.getOrInitializeCurrentRun(ctx);
    return ctx.messages ? deriveLoadedNamesFromMessages({ messages: ctx.messages }) : new Set();
  }

  addLoaded(names: string[], ctx: LoadedToolStoreContext): void {
    if (names.length === 0 || !ctx.args) return;

    const loaded = this.getOrInitializeCurrentRun(ctx);
    for (const name of names) {
      if (name.length > 0) loaded.add(name);
    }
    ctx.args.state[CURRENT_RUN_LOADED_TOOLS_KEY] = [...loaded];
  }

  // Context state belongs to messages and the live request, not this store.
  clearState(_threadId: string): void {}

  clearAllState(): void {}

  dispose(): void {}
}

/**
 * Thread state with timestamp for TTL management.
 */
interface LegacyThreadState {
  tools: Set<string>;
  lastAccessed: number;
}

interface LegacyMapLoadedToolStoreOptions {
  /**
   * Time-to-live for thread state in milliseconds. After this duration of
   * inactivity, thread state is eligible for cleanup. Set to 0 to disable.
   * @default 3600000 (1 hour)
   */
  ttl?: number;
}

/**
 * Legacy default store. Keeps loaded-tool state in an in-memory
 * `Map<threadId, { tools, lastAccessed }>` with TTL-based cleanup.
 *
 * This reproduces the original ToolSearchProcessor behavior exactly, including the
 * `'default'` thread-ID fallback used when no thread is active. It is the default
 * backend so existing behavior is unchanged; the context store is opt-in via the
 * processor's `storage` option.
 *
 * Known limitations (inherent to the in-memory map, fixed by the context store):
 * - State is lost on process restart.
 * - Anonymous requests (no thread ID) share the `'default'` entry.
 */
export class LegacyMapLoadedToolStore implements LoadedToolStore {
  private ttl: number;
  private threadLoadedTools = new Map<string, LegacyThreadState>();
  private intervalId?: ReturnType<typeof setInterval>;

  constructor(options: LegacyMapLoadedToolStoreOptions = {}) {
    this.ttl = options.ttl ?? 3_600_000;
    if (this.ttl > 0) {
      this.scheduleCleanup();
    }
  }

  private resolveThreadId(ctx: LoadedToolStoreContext): string {
    return ctx.threadId || 'default';
  }

  private getState(threadId: string): LegacyThreadState {
    let state = this.threadLoadedTools.get(threadId);
    if (!state) {
      state = { tools: new Set(), lastAccessed: Date.now() };
      this.threadLoadedTools.set(threadId, state);
    }
    state.lastAccessed = Date.now();
    return state;
  }

  getLoadedNames(ctx: LoadedToolStoreContext): Set<string> {
    return new Set(this.getState(this.resolveThreadId(ctx)).tools);
  }

  addLoaded(names: string[], ctx: LoadedToolStoreContext): void {
    if (names.length === 0) return;
    const state = this.getState(this.resolveThreadId(ctx));
    for (const name of names) state.tools.add(name);
  }

  clearState(threadId: string = 'default'): void {
    this.threadLoadedTools.delete(threadId);
  }

  clearAllState(): void {
    this.threadLoadedTools.clear();
  }

  dispose(): void {
    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = undefined;
    }
    this.clearAllState();
  }

  cleanupStaleState(): number {
    if (this.ttl <= 0) return 0;
    const now = Date.now();
    let cleaned = 0;
    for (const [threadId, state] of this.threadLoadedTools.entries()) {
      if (now - state.lastAccessed > this.ttl) {
        this.threadLoadedTools.delete(threadId);
        cleaned++;
      }
    }
    return cleaned;
  }

  getStateStats(): { threadCount: number; oldestAccessTime: number | null } {
    if (this.threadLoadedTools.size === 0) {
      return { threadCount: 0, oldestAccessTime: null };
    }
    let oldest = Date.now();
    for (const state of this.threadLoadedTools.values()) {
      if (state.lastAccessed < oldest) oldest = state.lastAccessed;
    }
    return { threadCount: this.threadLoadedTools.size, oldestAccessTime: oldest };
  }

  private scheduleCleanup(): void {
    const cleanupInterval = Math.max(this.ttl / 2, 60_000);
    this.intervalId = setInterval(() => {
      this.cleanupStaleState();
    }, cleanupInterval);
    this.intervalId.unref?.();
  }
}
