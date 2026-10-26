import { MastraBase } from '../base';

export type IndexedLogScope = 'process' | 'durable';

export type IndexedLogRetention = {
  /** Maximum age of a retained entry. */
  maxAgeMs: number;
  /** Maximum number of entries retained for one log. */
  maxEntries: number;
};

export type IndexedLogEntry<T> = {
  cursor: number;
  storedAt: number;
  value: T;
};

export type IndexedLogAppendResult<T> = IndexedLogEntry<T> & {
  /** Generation atomically associated with the appended cursor. */
  logGeneration: string;
};

export type IndexedLogReadResult<T> = {
  /**
   * Contiguous entries after the requested cursor. Implementations may return
   * a bounded page; callers continue until they reach `nextCursor`.
   */
  entries: IndexedLogEntry<T>[];
  /**
   * Changes whenever a deleted or fully expired log is recreated. An empty
   * generation returned before the first append must remain stable until that
   * append (or explicit deletion), so a subscriber can safely wait for the
   * first event longer than the configured event-retention window.
   */
  logGeneration: string;
  /** Earliest cursor still available. Equals nextCursor when the log is empty. */
  firstCursor: number;
  /** Cursor that will be assigned by the next append. */
  nextCursor: number;
};

/**
 * Optional cache capability for an exact retained log.
 *
 * Implementations must allocate the cursor and append the entry atomically.
 * A cache that only exposes separate `increment()` and `listPush()` operations
 * does not satisfy this contract: concurrent publishers could append in a
 * different order from cursor allocation.
 */
export interface AtomicIndexedLogCache {
  readonly indexedLogScope: IndexedLogScope;

  appendIndexedLogEntry<T>(key: string, value: T, retention: IndexedLogRetention): Promise<IndexedLogAppendResult<T>>;

  readIndexedLogEntries<T>(
    key: string,
    afterCursor: number,
    retention: IndexedLogRetention,
  ): Promise<IndexedLogReadResult<T>>;

  deleteIndexedLog(key: string): Promise<void>;
}

export function isAtomicIndexedLogCache(cache: MastraServerCache): cache is MastraServerCache & AtomicIndexedLogCache {
  const candidate = cache as MastraServerCache & Partial<AtomicIndexedLogCache>;
  return (
    (candidate.indexedLogScope === 'process' || candidate.indexedLogScope === 'durable') &&
    typeof candidate.appendIndexedLogEntry === 'function' &&
    typeof candidate.readIndexedLogEntries === 'function' &&
    typeof candidate.deleteIndexedLog === 'function'
  );
}

export abstract class MastraServerCache extends MastraBase {
  constructor({ name }: { name: string }) {
    super({
      component: 'SERVER_CACHE',
      name,
    });
  }

  abstract get(key: string): Promise<unknown>;

  abstract listLength(key: string): Promise<number>;

  /**
   * Store a value in the cache.
   * @param key - Cache key
   * @param value - Value to store
   * @param ttlMs - Optional per-key TTL in milliseconds. If not provided, uses
   *   the implementation's default TTL.
   */
  abstract set(key: string, value: unknown, ttlMs?: number): Promise<void>;

  abstract listPush(key: string, value: unknown): Promise<void>;

  abstract listFromTo(key: string, from: number, to?: number): Promise<unknown[]>;

  abstract delete(key: string): Promise<void>;

  abstract clear(): Promise<void>;

  /**
   * Atomically increment a counter and return the new value.
   * Used for generating sequential indices for events.
   * Returns 1 on first call (counter starts at 0, increments to 1).
   *
   * For Redis: Uses INCR command which is atomic.
   * For in-memory: Uses a simple counter map.
   */
  abstract increment(key: string): Promise<number>;

  /**
   * Atomically allocate the next index from `counterKey` and append `value`
   * (with `index` set to the 0-based allocated index) to the list at `listKey`.
   * Returns the assigned index.
   *
   * The list and its counter must expire together: a counter that outlives its
   * list restarts at 0 while subscribers still expect the old numbering, and a
   * counter that expires first would start reusing indices already in the list.
   * Every append refreshes both keys to the same deadline.
   *
   * @param ttlMs - Minimum lifetime for both keys, in milliseconds. A cache
   *   whose own TTL is longer, or that never expires, keeps that instead. When
   *   omitted, the cache's default TTL applies.
   *
   * The default implementation composes `increment()` + `listPush()` (two
   * round-trips) and is therefore NOT atomic, and ignores `ttlMs`. Overrides
   * should perform both steps in one server-side operation.
   */
  async listPushIndexed(
    listKey: string,
    counterKey: string,
    value: Record<string, unknown>,
    _ttlMs?: number,
  ): Promise<number> {
    const index = (await this.increment(counterKey)) - 1;
    await this.listPush(listKey, { ...value, index });
    return index;
  }
}
