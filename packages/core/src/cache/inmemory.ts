import { TTLCache } from '@isaacs/ttlcache';
import { MastraServerCache } from './base';
import type {
  AtomicIndexedLogCache,
  IndexedLogAppendResult,
  IndexedLogEntry,
  IndexedLogReadResult,
  IndexedLogRetention,
} from './base';

type InMemoryIndexedLogState = {
  logGeneration: string;
  nextCursor: number;
  entries: IndexedLogEntry<unknown>[];
  expiresAt: number;
};

/**
 * Options for InMemoryServerCache
 */
export interface InMemoryServerCacheOptions {
  /**
   * Maximum number of items to store in cache.
   * Defaults to 1000.
   */
  maxSize?: number;

  /**
   * Default TTL in milliseconds for cached items.
   * Defaults to 300000 (5 minutes).
   * Set to 0 to disable TTL (items persist until explicitly deleted or evicted).
   */
  ttlMs?: number;
}

export class InMemoryServerCache extends MastraServerCache implements AtomicIndexedLogCache {
  readonly indexedLogScope = 'process' as const;

  private cache: TTLCache<string, unknown>;
  private ttlMs: number;

  constructor(options: InMemoryServerCacheOptions = {}) {
    super({ name: 'InMemoryServerCache' });

    this.ttlMs = options.ttlMs ?? 1000 * 60 * 5;
    // TTLCache requires positive integer or Infinity; use Infinity when TTL is disabled
    const ttl = this.ttlMs > 0 ? this.ttlMs : Infinity;

    this.cache = new TTLCache<string, unknown>({
      max: options.maxSize ?? 1000,
      ttl,
    });
  }

  async get(key: string): Promise<unknown> {
    return this.cache.get(key);
  }

  async set(key: string, value: unknown, ttlMs?: number): Promise<void> {
    if (ttlMs === undefined) {
      this.cache.set(key, value);
      return;
    }
    // TTLCache requires positive integer or Infinity; non-positive overrides
    // mean "no expiry" and must be normalized.
    this.cache.set(key, value, { ttl: ttlMs > 0 ? ttlMs : Infinity });
  }

  async listLength(key: string): Promise<number> {
    const value = this.cache.get(key);
    if (value === undefined) {
      return 0; // Key doesn't exist - return 0
    }
    if (!Array.isArray(value)) {
      throw new Error(`${key} exists but is not an array`);
    }
    return value.length;
  }

  async listPush(key: string, value: unknown): Promise<void> {
    const existing = this.cache.get(key);
    if (Array.isArray(existing)) {
      existing.push(value);
      // Refresh TTL on push by re-setting the key with the updated list
      if (this.ttlMs > 0) {
        this.cache.set(key, existing, { ttl: this.ttlMs });
      }
    } else if (existing !== undefined) {
      throw new Error(`${key} exists but is not an array`);
    } else {
      this.cache.set(key, [value]);
    }
  }

  async listFromTo(key: string, from: number, to: number = -1): Promise<unknown[]> {
    const list = this.cache.get(key) as unknown[];
    if (Array.isArray(list)) {
      // Make 'to' inclusive like Redis LRANGE - add 1 unless it's -1
      const endIndex = to === -1 ? undefined : to + 1;
      return list.slice(from, endIndex);
    }
    return [];
  }

  async delete(key: string): Promise<void> {
    this.cache.delete(key);
  }

  async clear(): Promise<void> {
    this.cache.clear();
  }

  async increment(key: string): Promise<number> {
    const value = this.cache.get(key);
    let counter: number;
    if (value === undefined) {
      counter = 1;
    } else if (typeof value === 'number') {
      counter = value + 1;
    } else {
      throw new Error(`${key} exists but is not a number`);
    }
    this.cache.set(key, counter);
    return counter;
  }

  async appendIndexedLogEntry<T>(
    key: string,
    value: T,
    retention: IndexedLogRetention,
  ): Promise<IndexedLogAppendResult<T>> {
    const now = Date.now();
    const state = this.getIndexedLogState(key, now);
    this.pruneIndexedLog(state, retention, now);

    const entry: IndexedLogEntry<T> = {
      cursor: state.nextCursor,
      storedAt: now,
      value,
    };
    state.nextCursor += 1;
    state.entries.push(entry as IndexedLogEntry<unknown>);
    state.expiresAt = now + retention.maxAgeMs;
    this.pruneIndexedLog(state, retention, now);

    // The log's explicit retention owns its lifetime rather than the cache's
    // general-purpose default TTL.
    this.cache.set(key, state, { ttl: retention.maxAgeMs });
    return { ...entry, logGeneration: state.logGeneration };
  }

  async readIndexedLogEntries<T>(
    key: string,
    afterCursor: number,
    retention: IndexedLogRetention,
  ): Promise<IndexedLogReadResult<T>> {
    const now = Date.now();
    const state = this.getIndexedLogState(key, now);
    this.pruneIndexedLog(state, retention, now);
    // Reads must not extend the replay promise. Keep the cache TTL aligned to
    // the last append so a quiet log receives a fresh generation after its
    // entire retention window expires.
    this.cache.set(key, state, { ttl: Math.max(1, state.expiresAt - now) });

    const firstCursor = state.entries[0]?.cursor ?? state.nextCursor;
    const entries = state.entries.filter(entry => entry.cursor > afterCursor) as IndexedLogEntry<T>[];
    return { entries, logGeneration: state.logGeneration, firstCursor, nextCursor: state.nextCursor };
  }

  async deleteIndexedLog(key: string): Promise<void> {
    this.cache.delete(key);
  }

  private getIndexedLogState(key: string, now: number): InMemoryIndexedLogState {
    const existing = this.cache.get(key);
    if (existing === undefined) {
      return this.newIndexedLogState();
    }
    const state = this.assertIndexedLogState(key, existing);
    if (state.expiresAt <= now) {
      this.cache.delete(key);
      return this.newIndexedLogState();
    }
    return state;
  }

  private assertIndexedLogState(key: string, value: unknown): InMemoryIndexedLogState {
    if (
      typeof value !== 'object' ||
      value === null ||
      !('logGeneration' in value) ||
      typeof value.logGeneration !== 'string' ||
      value.logGeneration.length === 0 ||
      !('nextCursor' in value) ||
      typeof value.nextCursor !== 'number' ||
      !Number.isSafeInteger(value.nextCursor) ||
      value.nextCursor < 0 ||
      !('entries' in value) ||
      !Array.isArray(value.entries) ||
      !('expiresAt' in value) ||
      typeof value.expiresAt !== 'number' ||
      Number.isNaN(value.expiresAt)
    ) {
      throw new Error(`${key} exists but is not an indexed log`);
    }
    return value as InMemoryIndexedLogState;
  }

  private newIndexedLogState(): InMemoryIndexedLogState {
    return {
      logGeneration: crypto.randomUUID(),
      nextCursor: 0,
      entries: [],
      // An empty generation is a subscription fence, not retained event data.
      // Keep it until the first append (which installs the finite retention
      // horizon) so a watcher may wait indefinitely for cursor zero.
      expiresAt: Infinity,
    };
  }

  private pruneIndexedLog(state: InMemoryIndexedLogState, retention: IndexedLogRetention, now: number): void {
    const cutoff = now - retention.maxAgeMs;
    let firstRetained = 0;
    while (firstRetained < state.entries.length && state.entries[firstRetained]!.storedAt <= cutoff) {
      firstRetained += 1;
    }
    if (firstRetained > 0) {
      state.entries.splice(0, firstRetained);
    }
    if (state.entries.length > retention.maxEntries) {
      state.entries.splice(0, state.entries.length - retention.maxEntries);
    }
  }
}
