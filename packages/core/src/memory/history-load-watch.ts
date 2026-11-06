/**
 * PF-7679 (internal, not exported from any index): records that a run loaded a
 * specific stored message into its model context. Harness settlement notices
 * register a watch on their reminder row before saving it; the input processor
 * runner reports the remembered message ids of every run on a watched thread
 * once its input processors finish. A fired watch means that run's context
 * already carries the row, so the notice was delivered through history.
 *
 * Process-local: a run prepared in another process is never seen.
 */

interface HistoryLoadEntry {
  resourceId: string | undefined;
  rowId: string;
  loaded: boolean;
  onLoaded: (() => void) | undefined;
}

export interface HistoryLoadWatch {
  /** True once a run on the watched thread loaded the row into its context. */
  loaded(): boolean;
  /** Stop watching. Idempotent. */
  dispose(): void;
}

const watchesByThread = new Map<string, Set<HistoryLoadEntry>>();

/** Watch for a run on `threadId` (and `resourceId`, when both sides know it) that loads `rowId`. */
export function watchHistoryLoad(
  threadId: string,
  resourceId: string | undefined,
  rowId: string,
  onLoaded?: () => void,
): HistoryLoadWatch {
  const entry: HistoryLoadEntry = { resourceId, rowId, loaded: false, onLoaded };
  let entries = watchesByThread.get(threadId);
  if (entries === undefined) {
    entries = new Set();
    watchesByThread.set(threadId, entries);
  }
  entries.add(entry);
  return {
    loaded: () => entry.loaded,
    dispose: () => {
      const current = watchesByThread.get(threadId);
      if (current === undefined) return;
      current.delete(entry);
      if (current.size === 0) watchesByThread.delete(threadId);
    },
  };
}

/** Whether any watch exists, so runs pay nothing while no notice is pending. */
export function hasHistoryLoadWatches(): boolean {
  return watchesByThread.size > 0;
}

/** Report the stored message ids a run on `threadId` loaded into its context. */
export function noteHistoryLoaded(threadId: string, resourceId: string | undefined, ids: Iterable<string>): void {
  const entries = watchesByThread.get(threadId);
  if (entries === undefined) return;
  const loadedIds = new Set(ids);
  for (const entry of entries) {
    if (entry.loaded || !loadedIds.has(entry.rowId)) continue;
    if (entry.resourceId !== undefined && resourceId !== undefined && entry.resourceId !== resourceId) continue;
    entry.loaded = true;
    entry.onLoaded?.();
  }
}
