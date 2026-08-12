import { randomUUID } from 'node:crypto';
import { mkdir, open, readFile, rename, rm, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { MastraServerCache } from '@mastra/core/cache';
import type {
  AtomicIndexedLogCache,
  IndexedLogAppendResult,
  IndexedLogEntry,
  IndexedLogReadResult,
  IndexedLogRetention,
} from '@mastra/core/cache';

type StoredValue = {
  value: unknown;
  expiresAt: number | null;
};

type StoredIndexedLog = {
  logGeneration: string;
  nextCursor: number;
  entries: IndexedLogEntry<unknown>[];
  expiresAt: number | null;
};

type DiskState = {
  values: Record<string, StoredValue>;
  logs: Record<string, StoredIndexedLog>;
};

const DEFAULT_TTL_MS = 5 * 60 * 1_000;
const LOCK_TIMEOUT_MS = 10_000;
const LOCK_RETRY_MS = 10;

const emptyState = (): DiskState => ({ values: {}, logs: {} });
const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

/**
 * Minimal cross-process cache used by the connect-worker tests.
 *
 * The production contract requires a durable shared cache whenever the caller
 * and Inngest worker live in different processes. A process-local fallback
 * cannot reconcile live indexed events with retained history. This file-backed
 * adapter gives both test processes one atomic retained log without introducing
 * a Redis service into the test runtime.
 */
export class SharedFileServerCache extends MastraServerCache implements AtomicIndexedLogCache {
  readonly indexedLogScope = 'durable' as const;

  private readonly statePath: string;
  private readonly lockPath: string;

  constructor(dbUrl: string) {
    super({ name: 'SharedFileServerCache' });
    const dbPath = fileURLToPath(dbUrl);
    this.statePath = `${dbPath}.pubsub-cache.json`;
    this.lockPath = `${this.statePath}.lock`;
  }

  private async readState(): Promise<DiskState> {
    try {
      return JSON.parse(await readFile(this.statePath, 'utf8')) as DiskState;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') return emptyState();
      throw error;
    }
  }

  private async removeAbandonedLock(): Promise<void> {
    try {
      const ownerPid = Number(await readFile(this.lockPath, 'utf8'));
      if (!Number.isSafeInteger(ownerPid) || ownerPid <= 0) return;
      try {
        process.kill(ownerPid, 0);
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== 'ESRCH') throw error;
        await rm(this.lockPath, { force: true });
      }
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
    }
  }

  private async withState<T>(mutate: (state: DiskState) => T | Promise<T>): Promise<T> {
    await mkdir(path.dirname(this.statePath), { recursive: true });
    const deadline = Date.now() + LOCK_TIMEOUT_MS;
    let lock: Awaited<ReturnType<typeof open>> | undefined;

    while (!lock) {
      try {
        lock = await open(this.lockPath, 'wx');
        await lock.writeFile(String(process.pid));
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== 'EEXIST' || Date.now() >= deadline) throw error;
        await this.removeAbandonedLock();
        await sleep(LOCK_RETRY_MS);
      }
    }

    try {
      const state = await this.readState();
      const result = await mutate(state);
      const temporaryPath = `${this.statePath}.${process.pid}.${randomUUID()}.tmp`;
      await writeFile(temporaryPath, JSON.stringify(state));
      await rename(temporaryPath, this.statePath);
      return result;
    } finally {
      await lock.close();
      await rm(this.lockPath, { force: true });
    }
  }

  private getLiveValue(state: DiskState, key: string): StoredValue | undefined {
    const stored = state.values[key];
    if (stored?.expiresAt !== null && stored?.expiresAt !== undefined && stored.expiresAt <= Date.now()) {
      delete state.values[key];
      return undefined;
    }
    return stored;
  }

  async get(key: string): Promise<unknown> {
    return this.withState(state => this.getLiveValue(state, key)?.value);
  }

  async set(key: string, value: unknown, ttlMs = DEFAULT_TTL_MS): Promise<void> {
    await this.withState(state => {
      state.values[key] = { value, expiresAt: ttlMs > 0 ? Date.now() + ttlMs : null };
    });
  }

  async listLength(key: string): Promise<number> {
    return this.withState(state => {
      const value = this.getLiveValue(state, key)?.value;
      if (value === undefined) return 0;
      if (!Array.isArray(value)) throw new Error(`${key} exists but is not an array`);
      return value.length;
    });
  }

  async listPush(key: string, value: unknown): Promise<void> {
    await this.withState(state => {
      const stored = this.getLiveValue(state, key);
      if (stored && !Array.isArray(stored.value)) throw new Error(`${key} exists but is not an array`);
      state.values[key] = {
        value: [...((stored?.value as unknown[] | undefined) ?? []), value],
        expiresAt: Date.now() + DEFAULT_TTL_MS,
      };
    });
  }

  async listFromTo(key: string, from: number, to = -1): Promise<unknown[]> {
    return this.withState(state => {
      const value = this.getLiveValue(state, key)?.value;
      if (value === undefined) return [];
      if (!Array.isArray(value)) throw new Error(`${key} exists but is not an array`);
      return value.slice(from, to === -1 ? undefined : to + 1);
    });
  }

  async delete(key: string): Promise<void> {
    await this.withState(state => {
      delete state.values[key];
    });
  }

  async clear(): Promise<void> {
    await this.withState(state => {
      state.values = {};
      state.logs = {};
    });
  }

  async increment(key: string): Promise<number> {
    return this.withState(state => {
      const stored = this.getLiveValue(state, key);
      if (stored && typeof stored.value !== 'number') throw new Error(`${key} exists but is not a number`);
      const value = ((stored?.value as number | undefined) ?? 0) + 1;
      state.values[key] = { value, expiresAt: Date.now() + DEFAULT_TTL_MS };
      return value;
    });
  }

  private logFor(state: DiskState, key: string, now: number): StoredIndexedLog {
    const current = state.logs[key];
    if (current && (current.expiresAt === null || current.expiresAt > now)) return current;

    const created: StoredIndexedLog = {
      logGeneration: randomUUID(),
      nextCursor: 0,
      entries: [],
      expiresAt: null,
    };
    state.logs[key] = created;
    return created;
  }

  private prune(log: StoredIndexedLog, retention: IndexedLogRetention, now: number): void {
    const cutoff = now - retention.maxAgeMs;
    log.entries = log.entries.filter(entry => entry.storedAt > cutoff);
    if (log.entries.length > retention.maxEntries) {
      log.entries.splice(0, log.entries.length - retention.maxEntries);
    }
  }

  async appendIndexedLogEntry<T>(
    key: string,
    value: T,
    retention: IndexedLogRetention,
  ): Promise<IndexedLogAppendResult<T>> {
    return this.withState(state => {
      const now = Date.now();
      const log = this.logFor(state, key, now);
      this.prune(log, retention, now);
      const entry: IndexedLogEntry<T> = { cursor: log.nextCursor, storedAt: now, value };
      log.nextCursor += 1;
      log.entries.push(entry as IndexedLogEntry<unknown>);
      log.expiresAt = now + retention.maxAgeMs;
      this.prune(log, retention, now);
      return { ...entry, logGeneration: log.logGeneration };
    });
  }

  async readIndexedLogEntries<T>(
    key: string,
    afterCursor: number,
    retention: IndexedLogRetention,
  ): Promise<IndexedLogReadResult<T>> {
    return this.withState(state => {
      const now = Date.now();
      const log = this.logFor(state, key, now);
      this.prune(log, retention, now);
      return {
        entries: log.entries.filter(entry => entry.cursor > afterCursor) as IndexedLogEntry<T>[],
        logGeneration: log.logGeneration,
        firstCursor: log.entries[0]?.cursor ?? log.nextCursor,
        nextCursor: log.nextCursor,
      };
    });
  }

  async deleteIndexedLog(key: string): Promise<void> {
    await this.withState(state => {
      delete state.logs[key];
    });
  }
}
