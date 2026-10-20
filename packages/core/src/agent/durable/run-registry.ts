import { TTLCache } from '@isaacs/ttlcache';
import type { MastraLanguageModel } from '../../llm/model/shared.types';
import type { Mastra } from '../../mastra';
import { unwrapMastraTracingProxy } from '../../observability/context';
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
 * (refreshed on access) and the registry is hard-capped at 1000 concurrent
 * entries.
 *
 * The TTL is a crash/leak safety net, NOT an activity timeout. A run that is
 * actively executing — waiting on a tool, a sub-agent, or a model call — can
 * go arbitrarily long without anything reading its entry, and disposal is
 * destructive (it force-closes the run's stream and drops the live model and
 * tools). Such runs hold their entry alive explicitly via `markRunActive`.
 */
export const RUN_REGISTRY_TTL_MS = 2 * 60 * 60 * 1000;

/**
 * How often active runs re-read their registry entry to refresh the sliding
 * TTL. Two orders of magnitude below the TTL, so a single missed tick is
 * harmless.
 */
export const RUN_REGISTRY_HEARTBEAT_MS = 60 * 1000;

export const globalRunRegistry = new TTLCache<string, RunRegistryEntry>({
  max: 1000,
  ttl: RUN_REGISTRY_TTL_MS,
  updateAgeOnGet: true,
  dispose: (entry, runId) => {
    // TTLCache can call dispose with an undefined value after a missing get()
    // refreshes expiration metadata. Terminal workflow cleanup and the public
    // cleanup handle may legitimately race, so disposal must remain idempotent.
    if (!pinnedRunRegistry.has(runId)) entry?.cleanup?.();
  },
  noDisposeOnSet: true,
});

/**
 * Resolve a runtime entry, including one pinned for an active workflow segment.
 * A missing key is checked with `has()` first: with `updateAgeOnGet`, `get()` on a
 * missing runId schedules an expiration for a key that has no value.
 */
export function getGlobalRunRegistryEntry(runId: string): RunRegistryEntry | undefined {
  return (
    pinnedRunRegistry.get(runId)?.entry ?? (globalRunRegistry.has(runId) ? globalRunRegistry.get(runId) : undefined)
  );
}

/** Keep an active workflow segment immune to TTL/capacity eviction. */
export function pinGlobalRunRegistryEntry(runId: string): RunRegistryEntry | undefined {
  const pinned = pinnedRunRegistry.get(runId);
  if (pinned) {
    pinned.count += 1;
    return pinned.entry;
  }
  const entry = globalRunRegistry.has(runId) ? globalRunRegistry.get(runId) : undefined;
  if (entry) pinnedRunRegistry.set(runId, { entry, count: 1 });
  return entry;
}

/** Release an exact active segment and restore its TTL entry when it remains resumable. */
export function unpinGlobalRunRegistryEntry(
  runId: string,
  runtimeBindingId: string | undefined,
  /**
   * The entry this segment pinned. A recovery reuses the run's persisted binding, so
   * a crashed execution's late `finally` must not unpin the recovered entry (PF-5928).
   */
  expectedEntry?: RunRegistryEntry,
): void {
  const pinned = pinnedRunRegistry.get(runId);
  if (!pinned || pinned.entry.runtimeBindingId !== runtimeBindingId) return;
  if (expectedEntry !== undefined && pinned.entry !== expectedEntry) return;
  pinned.count -= 1;
  if (pinned.count > 0) return;
  pinnedRunRegistry.delete(runId);
  if (!globalRunRegistry.has(runId)) globalRunRegistry.set(runId, pinned.entry);
}

/**
 * Evict the runtime binding of an execution that no longer owns the run, so a
 * recovery of the same run in this process does not collide with its own crashed
 * execution (PF-5928, #27357). The caller decides ownership (see DurableAgent.recover);
 * only this exact entry is removed, and its late finalizers are identity-gated and
 * cannot touch a successor.
 */
export function evictLostRunRegistryEntry(runId: string, entry: RunRegistryEntry): boolean {
  let evicted = false;
  if (pinnedRunRegistry.get(runId)?.entry === entry) {
    pinnedRunRegistry.delete(runId);
    evicted = true;
  }
  if (globalRunRegistry.has(runId) && globalRunRegistry.get(runId) === entry) {
    globalRunRegistry.delete(runId);
    evicted = true;
  }
  return evicted;
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
  __resetRunRegistryActivityForTests();
  pinnedRunRegistry.clear();
  globalRunRegistry.clear();
  for (const entry of pinnedOnlyEntries) entry.cleanup?.();
}

/**
 * Read a run's registry entry as seen from a workflow step running under `mastra`.
 *
 * Entries are keyed only by runId, so when another Mastra instance in this
 * process reuses the runId, a still-running workflow from the earlier run
 * would otherwise read (and write its transcript into) the new run's entry.
 * An entry owned by a different Mastra instance is treated as absent. An
 * ownerless entry (a placeholder or rehydrated entry) is claimed by the first
 * scoped reader, so a later instance reusing the runId cannot adopt it.
 *
 * Fork (PF): this is the cross-instance owner check only. Same-instance run-ID
 * reuse is fenced separately by the per-execution `runtimeBindingId`
 * ({@link getBoundRunRegistryEntry}); steps that carry a binding must use that.
 */
export function getRunRegistryEntry(runId: string, mastra: Mastra | undefined): RunRegistryEntry | undefined {
  const entry = getGlobalRunRegistryEntry(runId);
  if (!entry || !mastra) return entry;
  if (!entry.mastra) {
    entry.mastra = mastra;
    return entry;
  }
  return unwrapMastraTracingProxy(entry.mastra) === unwrapMastraTracingProxy(mastra) ? entry : undefined;
}

/**
 * Publish an entry for `runId`, owned by `mastra`, unless the slot holds
 * another Mastra instance's live run (cached or pinned by an active segment).
 * Returns whether the entry was stored.
 */
export function setRunRegistryEntry(runId: string, entry: RunRegistryEntry, mastra: Mastra | undefined): boolean {
  const current = getGlobalRunRegistryEntry(runId);
  if (current?.mastra && mastra && unwrapMastraTracingProxy(current.mastra) !== unwrapMastraTracingProxy(mastra))
    return false;
  // Fork (PF): never swap the entry an active segment pinned for a different
  // execution binding; the pin would keep serving the old entry anyway.
  const pinned = pinnedRunRegistry.get(runId);
  if (pinned && pinned.entry !== entry && pinned.entry.runtimeBindingId !== entry.runtimeBindingId) return false;
  if (mastra) entry.mastra ??= mastra;
  globalRunRegistry.set(runId, entry);
  return true;
}

/** Remove `runId`'s entry only if it belongs to `mastra` (or has no owner). */
export function deleteRunRegistryEntry(runId: string, mastra: Mastra | undefined): void {
  if (getRunRegistryEntry(runId, mastra) && globalRunRegistry.has(runId)) globalRunRegistry.delete(runId);
}

/** runId -> number of in-flight operations holding the entry alive. */
const activeRuns = new Map<string, number>();
let activityHeartbeat: ReturnType<typeof setInterval> | undefined;

function runActivityHeartbeat(): void {
  // The read is the point: `updateAgeOnGet` refreshes the sliding TTL.
  for (const runId of activeRuns.keys()) {
    if (globalRunRegistry.has(runId)) globalRunRegistry.get(runId);
  }
}

const inFlightResumes = new Map<string, Map<string, symbol>>();

/**
 * Marks a resume of `toolCallId` on `runId` as in flight in this process,
 * synchronously, so a concurrent resume of the same tool call can fail fast
 * instead of queueing on the execution claim (#27365).
 *
 * @returns A release function, or `undefined` when that tool call already has
 * a resume in flight. Release is idempotent and only clears its own marker.
 */
export function markResumeInFlight(runId: string, toolCallId: string): (() => void) | undefined {
  let byToolCall = inFlightResumes.get(runId);
  if (byToolCall?.has(toolCallId)) return undefined;
  if (!byToolCall) inFlightResumes.set(runId, (byToolCall = new Map()));
  const token = Symbol(toolCallId);
  byToolCall.set(toolCallId, token);
  return () => {
    const current = inFlightResumes.get(runId);
    if (current?.get(toolCallId) !== token) return;
    current.delete(toolCallId);
    if (current.size === 0) inFlightResumes.delete(runId);
  };
}

/**
 * Clears the in-flight marker for a tool call whose resumed segment is
 * suspending again, so a later resume of it is not treated as a duplicate.
 */
export function clearResumeInFlight(runId: string, toolCallId: string): void {
  const current = inFlightResumes.get(runId);
  if (!current) return;
  current.delete(toolCallId);
  if (current.size === 0) inFlightResumes.delete(runId);
}

/**
 * Hold a run's registry entry alive while an operation is in flight.
 *
 * Durable steps await work that can outlast the registry TTL (a tool or
 * sub-agent call, a stalled model call). Nothing reads the entry while that
 * await is pending, so the sliding TTL would otherwise expire it mid-run and
 * dispose the live runtime state out from under the still-running workflow.
 *
 * Refcounted, so concurrent operations on the same run each hold their own
 * claim. The returned release is idempotent — calling it twice releases one
 * claim, not two. Always release in a `finally`; if a release is ever missed
 * the entry still expires once the heartbeat stops, so this degrades safely.
 *
 * @param runId - The unique run identifier
 * @returns A function that releases this claim
 */
export function markRunActive(runId: string): () => void {
  activeRuns.set(runId, (activeRuns.get(runId) ?? 0) + 1);

  if (!activityHeartbeat) {
    activityHeartbeat = setInterval(runActivityHeartbeat, RUN_REGISTRY_HEARTBEAT_MS);
    // Never keep the process alive just to refresh a cache.
    activityHeartbeat.unref?.();
  }

  let released = false;
  return () => {
    if (released) return;
    released = true;

    const remaining = (activeRuns.get(runId) ?? 1) - 1;
    if (remaining > 0) {
      activeRuns.set(runId, remaining);
    } else {
      activeRuns.delete(runId);
    }

    if (activeRuns.size === 0 && activityHeartbeat) {
      clearInterval(activityHeartbeat);
      activityHeartbeat = undefined;
    }
  };
}

/**
 * Drop all activity claims and stop the heartbeat. Tests only — production
 * code releases claims through the function returned by `markRunActive`.
 */
export function __resetRunRegistryActivityForTests(): void {
  activeRuns.clear();
  if (activityHeartbeat) {
    clearInterval(activityHeartbeat);
    activityHeartbeat = undefined;
  }
}

// `values()` can yield undefined: with `updateAgeOnGet`, `get()` on a missing
// runId schedules an expiration for a key that has no value. Pinned active
// segments are included (deduplicated) because TTL/capacity eviction may have
// removed them from the cache while they are still running.
function registryEntries(): RunRegistryEntry[] {
  const entries = new Set<RunRegistryEntry | undefined>([
    ...globalRunRegistry.values(),
    ...[...pinnedRunRegistry.values()].map(({ entry }) => entry),
  ]);
  return Array.from(entries).filter((entry): entry is RunRegistryEntry => entry !== undefined);
}

/**
 * Collect the in-flight durable workflow executions owned by `mastra` so graceful
 * shutdown can await them.
 */
export function getActiveDurableAgentWorkflowExecutions(mastra: Mastra): Promise<unknown>[] {
  // `globalRunRegistry` is a TTLCache, and — as the `dispose` handler above already
  // documents — it can surface an `undefined` value after a missing `get()` refreshes
  // expiration metadata. Pinned segments are included: an active segment may have
  // been evicted from the TTL cache while it is still running.
  return registryEntries().flatMap(entry =>
    entry.mastra === mastra && entry.workflowExecution ? [entry.workflowExecution] : [],
  );
}

/**
 * Abandon the durable-agent executions `mastra` is still running: each stops
 * without writing and releases its run, so recovery can take it right away.
 * Executions that already settled are left alone.
 */
export async function abandonDurableAgentExecutions(mastra: Mastra): Promise<void> {
  const fences = registryEntries().flatMap(entry =>
    entry.mastra === mastra && entry.executionFence ? [entry.executionFence] : [],
  );
  await Promise.allSettled(fences.map(fence => fence.abandon()));
}

/**
 * End a run's root spans (MODEL_GENERATION then AGENT_RUN) with an error so the trace
 * still exports — stores persist only span-end events. After a resume the fresh resume
 * spans are the active root, so prefer them. Ending an already-ended span is a no-op,
 * so the duplicate error paths (workflow failure + emitError) are safe. Never throws.
 */
export function endRunSpansWithError(runId: string, error: Error): void {
  try {
    const entry = getGlobalRunRegistryEntry(runId);
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
  if (entry && isReboundRunRegistryEntry(entry, runtimeBindingId)) {
    throw new Error(
      `Durable run ${runId} no longer matches its registered runtime dependencies. Refusing to execute a rebound run identifier.`,
    );
  }
  return entry;
}

/**
 * Whether `runId` is currently registered to an execution other than
 * `runtimeBindingId`. A stale handle uses this to stand down quietly instead of
 * throwing (or acting) against a successor that legitimately reused the id.
 */
export function isRunIdReboundFrom(runId: string, runtimeBindingId: string | undefined): boolean {
  const entry = getGlobalRunRegistryEntry(runId);
  return entry !== undefined && isReboundRunRegistryEntry(entry, runtimeBindingId);
}

function isReboundRunRegistryEntry(entry: RunRegistryEntry, runtimeBindingId: string | undefined): boolean {
  return (
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
  );
}

/**
 * Runtime bindings whose registry entry this process released while that execution's workflow
 * was still running (PF-5928). A late step of the released execution (for example a tail that
 * kept running after a tripwire closed its stream and cleanup ran) must not re-seed a
 * remote-abort placeholder for the released run id, or a later run that reuses the id inherits
 * it. Each retirement lasts exactly until its execution settles and is then pruned: it is
 * bounded by the executions still running, never by a count, and a later segment of the same
 * execution (a resume awaits the prior segment) is never refused.
 */
const retiredRuntimeBindings = new Map<string, Promise<unknown>>();

function retireRuntimeBindingUntilSettled(runtimeBindingId: string, execution: Promise<unknown> | undefined): void {
  if (!execution) return;
  retiredRuntimeBindings.set(runtimeBindingId, execution);
  const prune = () => {
    if (retiredRuntimeBindings.get(runtimeBindingId) === execution) retiredRuntimeBindings.delete(runtimeBindingId);
  };
  void execution.then(prune, prune);
}

/** Whether this process released `runtimeBindingId` while its execution still runs. @internal */
export function isRuntimeBindingRetired(runtimeBindingId: string): boolean {
  return retiredRuntimeBindings.has(runtimeBindingId);
}

/** Register without replacing another active run that happens to reuse the same ID. */
export function registerGlobalRunRegistryEntry(runId: string, entry: RunRegistryEntry): void {
  if (getGlobalRunRegistryEntry(runId)) {
    throw new Error(`Durable run ${runId} is already active. Refusing to replace its runtime dependencies.`);
  }
  globalRunRegistry.set(runId, entry);
}

/**
 * Delete only the entry owned by the caller; stale cleanup must not delete a newer binding.
 * `expectedEntry` additionally gates on entry identity: a recovery reuses the run's persisted
 * binding, so a lost execution's late cleanup must not delete the recovered entry (PF-5928).
 */
export function deleteBoundRunRegistryEntry(
  runId: string,
  runtimeBindingId: string,
  expectedEntry?: RunRegistryEntry,
): boolean {
  const pinned = pinnedRunRegistry.get(runId);
  // Guarded read (upstream #26315 bug class): `get` on the TTL cache refreshes and can recreate the key.
  const cached = globalRunRegistry.has(runId) ? globalRunRegistry.get(runId) : undefined;
  const pinnedMatches =
    pinned?.entry.runtimeBindingId === runtimeBindingId &&
    (expectedEntry === undefined || pinned.entry === expectedEntry);
  const cachedMatches =
    cached?.runtimeBindingId === runtimeBindingId && (expectedEntry === undefined || cached === expectedEntry);
  if (!pinnedMatches && !cachedMatches) return false;
  retireRuntimeBindingUntilSettled(runtimeBindingId, (pinnedMatches ? pinned.entry : cached)?.workflowExecution);

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
