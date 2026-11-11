/**
 * The queue head of a `Session` (PF-8365): which queued item's turn is running
 * (live or suspended), that item's source, and the in-process resolvers of
 * `queue()` callers.
 *
 * `Session` creates one `QueueHead` and changes these fields only through its
 * operations. Each operation is exactly the statements the session ran inline
 * before; nothing here awaits, calls back into the session, or adds a check the
 * session did not make:
 *
 *   - `claim` writes the id and the source together. It does not refuse a held
 *     head: the callers guard that (`_maybeDrainQueue`, `_ensureQueuedItemContext`).
 *   - `release` clears both unconditionally; `releaseIf` clears them only when
 *     the given item is the current head. The two stay separate because the
 *     session's call sites differ in exactly that check.
 *   - `takeResolver` deletes a resolver before handing it out, so each resolver
 *     is settled at most once; the caller settles it.
 *   - `rejectAll` is the teardown loop of `_markEvicted` and `_markDeleted`.
 */

import type { AgentResult } from './types';

/** Where a queued item came from. */
export type QueuedItemSource = 'user' | 'goal';

/** A `queue()` caller's promise and the functions that settle it. */
export interface QueueResolver {
  promise: Promise<AgentResult>;
  resolve: (result: AgentResult) => void;
  reject: (err: unknown) => void;
}

export class QueueHead {
  /** `queuedItem.id` of the turn currently running (live or suspended). */
  private _currentQueuedItemId?: string;
  /** `queuedItem.source` of the turn currently running. Used by the goal
   *  judge loop to skip re-judging on goal-driven continuation turns. */
  private _currentQueuedItemSource?: QueuedItemSource;
  /**
   * Queue resolvers indexed by `queuedItem.id`. Registered in `queue()` so the
   * caller's promise settles when the head turn completes (or rejects on
   * permanent failure). Taken before settling. Items recovered from
   * `pendingQueue` on hydration have no resolver — `queue_item_replayed` is
   * emitted instead and the turn runs purely for its side-effects.
   */
  private readonly _queueResolvers = new Map<string, QueueResolver>();

  /** The current head's item id, if a queued turn is running or suspended. */
  get currentId(): string | undefined {
    return this._currentQueuedItemId;
  }

  /** The current head's source. */
  get currentSource(): QueuedItemSource | undefined {
    return this._currentQueuedItemSource;
  }

  /** Make `id` the current head. */
  claim(id: string, source: QueuedItemSource): void {
    this._currentQueuedItemId = id;
    this._currentQueuedItemSource = source;
  }

  /** Clear the current head, whichever item it is. */
  release(): void {
    this._currentQueuedItemId = undefined;
    this._currentQueuedItemSource = undefined;
  }

  /** Clear the current head only if it is `id`. */
  releaseIf(id: string): void {
    if (this._currentQueuedItemId === id) {
      this._currentQueuedItemId = undefined;
      this._currentQueuedItemSource = undefined;
    }
  }

  /** Register the resolver of a `queue()` caller. */
  register(id: string, resolver: QueueResolver): void {
    this._queueResolvers.set(id, resolver);
  }

  /** Remove and return the resolver for `id`, if one is registered. The caller settles it. */
  takeResolver(id: string): QueueResolver | undefined {
    const resolver = this._queueResolvers.get(id);
    if (resolver) {
      this._queueResolvers.delete(id);
    }
    return resolver;
  }

  /** Remove every resolver and reject each with `err`, in registration order. */
  rejectAll(err: unknown): void {
    for (const [queuedItemId, resolver] of this._queueResolvers) {
      this._queueResolvers.delete(queuedItemId);
      resolver.reject(err);
    }
  }

  /** Whether a resolver is registered for `id`. */
  hasResolver(id: string): boolean {
    return this._queueResolvers.has(id);
  }

  /** How many resolvers are registered. */
  get resolverCount(): number {
    return this._queueResolvers.size;
  }

  /**
   * The resolver registered for `id`, without taking it, typed so the caller can
   * only wait on its promise (the duplicate-admission path joins a live caller).
   */
  peekResolver(id: string): Pick<QueueResolver, 'promise'> | undefined {
    return this._queueResolvers.get(id);
  }
}
