import { TTLCache } from '@isaacs/ttlcache';
import type { MastraLanguageModel } from '../../llm/model/shared.types';
import type { Mastra } from '../../mastra';
import type { CoreTool } from '../../tools/types';
import type { MessageList } from '../message-list';
import type { SaveQueueManager } from '../save-queue';
import type { RunRegistryEntry } from './types';

const pinnedRunRegistry = new Map<string, { entry: RunRegistryEntry; count: number }>();

/**
 * Global registry for accessing run entries from workflow steps.
 * This is necessary because workflow steps don't have direct access to
 * the DurableAgent instance's registry.
 *
 * Entries are keyed by runId (which are unique UUIDs).
 *
 * Uses TTLCache to prevent unbounded memory growth: entries auto-expire
 * after 10 minutes (refreshed on access) and the registry is hard-capped
 * at 1000 concurrent entries.
 */
export const globalRunRegistry = new TTLCache<string, RunRegistryEntry>({
  max: 1000,
  ttl: 10 * 60 * 1000,
  updateAgeOnGet: true,
  dispose: (entry, runId) => {
    // TTLCache can call dispose with an undefined value after a missing get()
    // refreshes expiration metadata. Terminal workflow cleanup and the public
    // cleanup handle may legitimately race, so disposal must remain idempotent.
    if (!pinnedRunRegistry.has(runId)) entry?.cleanup?.();
  },
  noDisposeOnSet: true,
});

/** Resolve a runtime entry, including one pinned for an active workflow segment. */
export function getGlobalRunRegistryEntry(runId: string): RunRegistryEntry | undefined {
  return pinnedRunRegistry.get(runId)?.entry ?? globalRunRegistry.get(runId);
}

/** Keep an active workflow segment immune to TTL/capacity eviction. */
export function pinGlobalRunRegistryEntry(runId: string): RunRegistryEntry | undefined {
  const pinned = pinnedRunRegistry.get(runId);
  if (pinned) {
    pinned.count += 1;
    return pinned.entry;
  }
  const entry = globalRunRegistry.get(runId);
  if (entry) pinnedRunRegistry.set(runId, { entry, count: 1 });
  return entry;
}

/** Release an exact active segment and restore its TTL entry when it remains resumable. */
export function unpinGlobalRunRegistryEntry(runId: string, runtimeBindingId: string | undefined): void {
  const pinned = pinnedRunRegistry.get(runId);
  if (!pinned || pinned.entry.runtimeBindingId !== runtimeBindingId) return;
  pinned.count -= 1;
  if (pinned.count > 0) return;
  pinnedRunRegistry.delete(runId);
  if (!globalRunRegistry.has(runId)) globalRunRegistry.set(runId, pinned.entry);
}

/** Remove a pin during explicit terminal/consumer cleanup. */
export function clearPinnedRunRegistryEntry(runId: string): void {
  pinnedRunRegistry.delete(runId);
}

/** Reset every global runtime binding, including entries pinned by an unfinished segment. */
export function clearGlobalRunRegistry(): void {
  const cachedEntries = new Set(globalRunRegistry.values());
  const pinnedOnlyEntries = new Set(
    [...pinnedRunRegistry.values()].map(({ entry }) => entry).filter(entry => !cachedEntries.has(entry)),
  );
  pinnedRunRegistry.clear();
  globalRunRegistry.clear();
  for (const entry of pinnedOnlyEntries) entry.cleanup?.();
}

/**
 * Collect the in-flight durable workflow executions owned by `mastra` so graceful
 * shutdown can await them. Pinned segments are included: an active segment may have
 * been evicted from the TTL cache while it is still running.
 */
export function getActiveDurableAgentWorkflowExecutions(mastra: Mastra): Promise<unknown>[] {
  // `globalRunRegistry` is a TTLCache, and — as the `dispose` handler above already
  // documents — it can surface an `undefined` value after a missing `get()` refreshes
  // expiration metadata. Type the set honestly and optional-chain the read; the
  // unguarded `entry.mastra` threw during `Mastra.shutdown()`.
  const entries = new Set<RunRegistryEntry | undefined>([
    ...globalRunRegistry.values(),
    ...[...pinnedRunRegistry.values()].map(({ entry }) => entry),
  ]);
  return Array.from(entries).flatMap(entry =>
    entry?.mastra === mastra && entry.workflowExecution ? [entry.workflowExecution] : [],
  );
}

/**
 * End a run's root spans (MODEL_GENERATION then AGENT_RUN) with an error so the trace
 * still exports — stores persist only span-end events. After a resume the fresh resume
 * spans are the active root, so prefer them. Ending an already-ended span is a no-op,
 * so the duplicate error paths (workflow failure + emitError) are safe. Never throws.
 */
export function endRunSpansWithError(runId: string, error: Error): void {
  try {
    const entry = globalRunRegistry.get(runId);
    (entry?.resumeModelSpan ?? entry?.modelSpan)?.error({ error, endSpan: true });
    (entry?.resumeAgentSpan ?? entry?.agentSpan)?.error({ error, endSpan: true });
  } catch {
    // Span bookkeeping must never break error reporting.
  }
}

/**
 * Read a run entry only when it is the exact entry captured by the durable
 * workflow input. A caller-reused runId must never rebind an older workflow to
 * a newer run's tools, processors, model, memory, or request context.
 */
export function getBoundRunRegistryEntry(runId: string, runtimeBindingId?: string): RunRegistryEntry | undefined {
  const entry = getGlobalRunRegistryEntry(runId);
  if (
    entry &&
    // A legacy placeholder may be created before its persisted binding is
    // loaded. Once a placeholder is stamped, it is fenced exactly like a full
    // runtime entry so a reused run ID cannot adopt another execution's
    // controller or cleanup.
    (entry.isPlaceholder !== true || entry.runtimeBindingId !== undefined) &&
    (entry.runtimeBindingId !== runtimeBindingId ||
      // Inputs persisted before runtime bindings existed may reconstruct after
      // a process restart, but must never attach to a newly registered run that
      // happens to reuse the same caller-provided ID.
      runtimeBindingId === undefined)
  ) {
    throw new Error(
      `Durable run ${runId} no longer matches its registered runtime dependencies. Refusing to execute a rebound run identifier.`,
    );
  }
  return entry;
}

/** Register without replacing another active run that happens to reuse the same ID. */
export function registerGlobalRunRegistryEntry(runId: string, entry: RunRegistryEntry): void {
  if (getGlobalRunRegistryEntry(runId)) {
    throw new Error(`Durable run ${runId} is already active. Refusing to replace its runtime dependencies.`);
  }
  globalRunRegistry.set(runId, entry);
}

/** Delete only the entry owned by the caller; stale cleanup must not delete a newer binding. */
export function deleteBoundRunRegistryEntry(runId: string, runtimeBindingId: string): boolean {
  const pinned = pinnedRunRegistry.get(runId);
  const cached = globalRunRegistry.get(runId);
  const pinnedMatches = pinned?.entry.runtimeBindingId === runtimeBindingId;
  const cachedMatches = cached?.runtimeBindingId === runtimeBindingId;
  if (!pinnedMatches && !cachedMatches) return false;

  // Terminal cleanup can race the active segment's `finally`. Remove the
  // matching pin first so a later unpin cannot restore terminal runtime state
  // into the TTL registry and block legitimate run-ID reuse.
  if (pinnedMatches) pinnedRunRegistry.delete(runId);

  if (cachedMatches) {
    const disposalSuppressedByAnotherPin = pinnedRunRegistry.has(runId);
    globalRunRegistry.delete(runId);
    if (disposalSuppressedByAnotherPin) cached.cleanup?.();
  }
  if (pinnedMatches && pinned.entry !== cached) pinned.entry.cleanup?.();
  return true;
}

/**
 * Registry for per-run non-serializable state.
 *
 * During durable execution, the DurableAgent needs to store non-serializable
 * objects (tools with execute functions, SaveQueueManager, etc.) that can't
 * flow through workflow state. This registry provides a way to store and
 * retrieve these objects keyed by runId.
 *
 * The registry is scoped to a single DurableAgent instance and entries are
 * cleaned up when a run completes.
 */
export class RunRegistry {
  #entries = new Map<string, RunRegistryEntry>();

  /**
   * Register non-serializable state for a run
   * @param runId - The unique run identifier
   * @param entry - The registry entry containing tools, saveQueueManager, etc.
   */
  register(runId: string, entry: RunRegistryEntry): void {
    // Clean up any existing entry first
    this.cleanup(runId);
    this.#entries.set(runId, entry);
  }

  /**
   * Get the registry entry for a run
   * @param runId - The unique run identifier
   * @returns The registry entry or undefined if not found
   */
  get(runId: string): RunRegistryEntry | undefined {
    return this.#entries.get(runId);
  }

  /**
   * Get tools for a specific run
   * @param runId - The unique run identifier
   * @returns The tools record or an empty object if not found
   */
  getTools(runId: string): Record<string, CoreTool> {
    return this.#entries.get(runId)?.tools ?? {};
  }

  /**
   * Get SaveQueueManager for a specific run
   * @param runId - The unique run identifier
   * @returns The SaveQueueManager or undefined if not found
   */
  getSaveQueueManager(runId: string): SaveQueueManager | undefined {
    return this.#entries.get(runId)?.saveQueueManager;
  }

  /**
   * Get the language model for a specific run
   * @param runId - The unique run identifier
   * @returns The MastraLanguageModel or undefined if not found
   */
  getModel(runId: string): MastraLanguageModel | undefined {
    return this.#entries.get(runId)?.model;
  }

  /**
   * Check if a run is registered
   * @param runId - The unique run identifier
   * @returns True if the run is registered
   */
  has(runId: string): boolean {
    return this.#entries.has(runId);
  }

  /**
   * Cleanup and remove a run's entry from the registry
   * @param runId - The unique run identifier
   */
  cleanup(runId: string): void {
    const entry = this.#entries.get(runId);
    if (entry) {
      // Call cleanup function if provided
      entry.cleanup?.();
      this.#entries.delete(runId);
    }
  }

  /** Ignore cleanup callbacks retained by an older execution that reused the same runId. */
  cleanupBound(runId: string, runtimeBindingId: string): boolean {
    if (this.#entries.get(runId)?.runtimeBindingId !== runtimeBindingId) return false;
    this.cleanup(runId);
    return true;
  }

  /**
   * Get the number of active runs in the registry
   */
  get size(): number {
    return this.#entries.size;
  }

  /**
   * Get all active run IDs
   */
  get runIds(): string[] {
    return Array.from(this.#entries.keys());
  }

  /**
   * Clear all entries from the registry
   * Calls cleanup on each entry before removing
   */
  clear(): void {
    for (const runId of this.#entries.keys()) {
      this.cleanup(runId);
    }
  }
}

/**
 * Extended registry entry that also stores the MessageList reference.
 * This is useful for accessing message state outside of workflow steps
 * (e.g., for callbacks that need to read messages).
 */
export interface ExtendedRunRegistryEntry extends RunRegistryEntry {
  /** MessageList reference for callback access */
  messageList?: MessageList;
  /** Thread ID for memory */
  threadId?: string;
  /** Resource ID for memory */
  resourceId?: string;
}

/**
 * Extended run registry that also stores MessageList references and memory info
 */
export class ExtendedRunRegistry extends RunRegistry {
  #messageLists = new Map<string, MessageList>();
  #memoryInfo = new Map<string, { threadId?: string; resourceId?: string }>();

  /**
   * Register non-serializable state for a run including MessageList
   */
  registerWithMessageList(
    runId: string,
    entry: RunRegistryEntry,
    messageList: MessageList,
    memoryInfo?: { threadId?: string; resourceId?: string },
  ): void {
    // Durable workflow steps deserialize a fresh MessageList and publish it
    // through the shared registry entry. Keep that entry as the live source
    // of truth so resume observers never read the stale list captured when
    // the run was first registered.
    entry.messageList = messageList;
    this.register(runId, entry);
    this.#messageLists.set(runId, messageList);
    if (memoryInfo) {
      this.#memoryInfo.set(runId, memoryInfo);
    }
  }

  /**
   * Get MessageList for a specific run
   */
  getMessageList(runId: string): MessageList | undefined {
    return this.get(runId)?.messageList ?? this.#messageLists.get(runId);
  }

  /**
   * Get memory info for a specific run
   */
  getMemoryInfo(runId: string): { threadId?: string; resourceId?: string } | undefined {
    return this.#memoryInfo.get(runId);
  }

  /**
   * Override cleanup to also remove MessageList and memory info
   */
  override cleanup(runId: string): void {
    super.cleanup(runId);
    this.#messageLists.delete(runId);
    this.#memoryInfo.delete(runId);
  }

  /**
   * Override clear to also clear MessageLists and memory info
   */
  override clear(): void {
    super.clear();
    this.#messageLists.clear();
    this.#memoryInfo.clear();
  }
}
