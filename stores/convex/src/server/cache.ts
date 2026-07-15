import type { GenericMutationCtx as MutationCtx } from 'convex/server';
import { mutationGeneric } from 'convex/server';
import type { GenericId } from 'convex/values';

import type { CacheRequest, CacheResponse } from '../cache/types';

const CACHE_TABLE = 'mastra_cache';
const CACHE_LIST_TABLE = 'mastra_cache_list_items';
const CACHE_MUTATION_BATCH_SIZE = 25;

type CacheKind = 'value' | 'list' | 'counter' | 'indexed-log' | 'deleted';
type CacheDoc = {
  _id: GenericId<string>;
  key: string;
  keyPrefix: string;
  kind: CacheKind;
  value?: string;
  counter?: number;
  retainedCount?: number;
  logGeneration?: string;
  expiresAt: number | null;
};
type CacheListItem = {
  _id: GenericId<string>;
  key: string;
  keyPrefix: string;
  index: number;
  storedAt?: number;
  value: string;
};
type DeleteBatchResult = {
  hasMore: boolean;
};

function encodeValue(value: unknown): string {
  // The cache wire format stores JSON strings; undefined is represented as null.
  return JSON.stringify(value === undefined ? null : value);
}

function decodeValue(value: string): unknown {
  return JSON.parse(value);
}

function isExpired(doc: { expiresAt: number | null }, now: number): boolean {
  return doc.expiresAt !== null && doc.expiresAt <= now;
}

function isIndexedLogRetention(value: unknown): value is { maxAgeMs: number; maxEntries: number } {
  if (typeof value !== 'object' || value === null) return false;
  const retention = value as { maxAgeMs?: unknown; maxEntries?: unknown };
  return (
    typeof retention.maxAgeMs === 'number' &&
    Number.isSafeInteger(retention.maxAgeMs) &&
    retention.maxAgeMs > 0 &&
    typeof retention.maxEntries === 'number' &&
    Number.isSafeInteger(retention.maxEntries) &&
    retention.maxEntries > 0
  );
}

function hasValidProposedLogGeneration(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0;
}

function normalizeListRange(from: number, to: number, length: number): { from: number; to: number } | null {
  const normalizedFrom = from < 0 ? Math.max(length + from, 0) : from;
  const normalizedTo = to < 0 ? length + to : to;
  if (normalizedTo < normalizedFrom || normalizedFrom >= length) return null;
  return { from: normalizedFrom, to: normalizedTo };
}

async function findCacheDoc(ctx: MutationCtx<any>, key: string): Promise<CacheDoc | null> {
  return (await ctx.db
    .query(CACHE_TABLE)
    .withIndex('by_key', (q: any) => q.eq('key', key))
    .first()) as CacheDoc | null;
}

async function deleteCacheKey(ctx: MutationCtx<any>, key: string): Promise<DeleteBatchResult> {
  const [doc, listItems] = await Promise.all([
    findCacheDoc(ctx, key),
    ctx.db
      .query(CACHE_LIST_TABLE)
      .withIndex('by_key_index', (q: any) => q.eq('key', key))
      .take(CACHE_MUTATION_BATCH_SIZE + 1),
  ]);

  if (doc && doc.kind !== 'deleted') {
    await ctx.db.patch(doc._id, { kind: 'deleted' });
  }

  for (const item of listItems.slice(0, CACHE_MUTATION_BATCH_SIZE) as CacheListItem[]) {
    await ctx.db.delete(item._id);
  }

  const hasMore = listItems.length > CACHE_MUTATION_BATCH_SIZE;
  if (doc && !hasMore) {
    await ctx.db.delete(doc._id);
  }

  return {
    hasMore,
  };
}

async function getLiveCacheDoc(
  ctx: MutationCtx<any>,
  key: string,
  now: number,
): Promise<{ doc: CacheDoc | null; hasMore: boolean }> {
  const doc = await findCacheDoc(ctx, key);
  if (!doc) return { doc: null, hasMore: false };
  if (doc.kind === 'deleted') {
    const cleanup = await deleteCacheKey(ctx, key);
    return { doc: null, hasMore: cleanup.hasMore };
  }
  if (!isExpired(doc, now)) return { doc, hasMore: false };

  const cleanup = await deleteCacheKey(ctx, key);
  return { doc: null, hasMore: cleanup.hasMore };
}

async function writeCacheDoc(
  ctx: MutationCtx<any>,
  key: string,
  existing: CacheDoc | null,
  patch: Omit<CacheDoc, '_id' | 'key'>,
): Promise<CacheDoc> {
  if (existing) {
    await ctx.db.patch(existing._id, patch);
    return { ...existing, ...patch };
  }

  const _id = await ctx.db.insert(CACHE_TABLE, { key, ...patch });
  return { _id, key, ...patch };
}

async function clearPrefix(ctx: MutationCtx<any>, keyPrefix: string): Promise<boolean> {
  const [docs, orphanListItems] = await Promise.all([
    ctx.db
      .query(CACHE_TABLE)
      .withIndex('by_key_prefix', (q: any) => q.eq('keyPrefix', keyPrefix))
      .take(CACHE_MUTATION_BATCH_SIZE + 1),
    ctx.db
      .query(CACHE_LIST_TABLE)
      .withIndex('by_key_prefix', (q: any) => q.eq('keyPrefix', keyPrefix))
      .take(1),
  ]);

  if (docs.length > 0) {
    for (const doc of docs.slice(0, CACHE_MUTATION_BATCH_SIZE) as CacheDoc[]) {
      if (doc.kind === 'list' || doc.kind === 'indexed-log' || doc.kind === 'deleted') {
        const cleanup = await deleteCacheKey(ctx, doc.key);
        return cleanup.hasMore || docs.length > 1 || orphanListItems.length > 0;
      }

      await ctx.db.delete(doc._id);
    }

    return docs.length > CACHE_MUTATION_BATCH_SIZE || orphanListItems.length > 0;
  }

  const listItems = (await ctx.db
    .query(CACHE_LIST_TABLE)
    .withIndex('by_key_prefix', (q: any) => q.eq('keyPrefix', keyPrefix))
    .take(CACHE_MUTATION_BATCH_SIZE + 1)) as CacheListItem[];

  for (const item of listItems.slice(0, CACHE_MUTATION_BATCH_SIZE)) {
    await ctx.db.delete(item._id);
  }

  return listItems.length > CACHE_MUTATION_BATCH_SIZE;
}

async function pruneIndexedLog(
  ctx: MutationCtx<any>,
  doc: CacheDoc,
  retention: { maxAgeMs: number; maxEntries: number },
  now: number,
  reserveEntries: number,
): Promise<{ doc: CacheDoc; hasMore: boolean }> {
  const items = (await ctx.db
    .query(CACHE_LIST_TABLE)
    .withIndex('by_key_index', (q: any) => q.eq('key', doc.key))
    .take(CACHE_MUTATION_BATCH_SIZE + 1)) as CacheListItem[];
  const retainedCount = doc.retainedCount ?? items.length;
  const targetCount = Math.max(retention.maxEntries - reserveEntries, 0);
  const cutoff = now - retention.maxAgeMs;
  let expiredCount = 0;
  while (expiredCount < items.length && (items[expiredCount]!.storedAt ?? 0) <= cutoff) {
    expiredCount += 1;
  }
  const overflowCount = Math.max(retainedCount - targetCount, 0);
  const deleteCount = Math.min(Math.max(expiredCount, overflowCount), CACHE_MUTATION_BATCH_SIZE, items.length);

  for (const item of items.slice(0, deleteCount)) {
    await ctx.db.delete(item._id);
  }

  const nextRetainedCount = Math.max(retainedCount - deleteCount, 0);
  if (nextRetainedCount !== retainedCount) {
    await ctx.db.patch(doc._id, { retainedCount: nextRetainedCount });
  }
  const nextDoc = { ...doc, retainedCount: nextRetainedCount };
  const nextItem = items[deleteCount];
  const hasMore =
    nextRetainedCount > targetCount ||
    (nextItem !== undefined && (nextItem.storedAt ?? 0) <= cutoff) ||
    (deleteCount === CACHE_MUTATION_BATCH_SIZE && nextRetainedCount > 0);
  return { doc: nextDoc, hasMore };
}

async function isIndexedLogFullyExpired(
  ctx: MutationCtx<any>,
  doc: CacheDoc,
  retention: { maxAgeMs: number },
  now: number,
): Promise<boolean> {
  if ((doc.retainedCount ?? 0) === 0) return false;
  const newest = (await ctx.db
    .query(CACHE_LIST_TABLE)
    .withIndex('by_key_index', (q: any) => q.eq('key', doc.key))
    .order('desc')
    .first()) as CacheListItem | null;
  return newest !== null && (newest.storedAt ?? 0) <= now - retention.maxAgeMs;
}

export async function handleCacheOperation(ctx: MutationCtx<any>, request: CacheRequest): Promise<CacheResponse> {
  const now = Date.now();

  switch (request.op) {
    case 'get': {
      const { doc, hasMore } = await getLiveCacheDoc(ctx, request.key, now);
      if (hasMore) return { ok: true, result: null, hasMore: true };
      if (!doc || doc.kind !== 'value') return { ok: true, result: null };
      return { ok: true, result: decodeValue(doc.value ?? 'null') };
    }

    case 'set': {
      let existing = await findCacheDoc(ctx, request.key);
      if (existing && (isExpired(existing, now) || existing.kind !== 'value')) {
        const cleanup = await deleteCacheKey(ctx, request.key);
        if (cleanup.hasMore) {
          return { ok: true, hasMore: true };
        }
        existing = null;
      }

      await writeCacheDoc(ctx, request.key, existing, {
        keyPrefix: request.keyPrefix,
        kind: 'value',
        value: encodeValue(request.value),
        expiresAt: request.expiresAt,
      });
      return { ok: true };
    }

    case 'listLength': {
      const { doc, hasMore } = await getLiveCacheDoc(ctx, request.key, now);
      if (hasMore) return { ok: true, result: 0, hasMore: true };
      if (!doc) return { ok: true, result: 0 };
      if (doc.kind !== 'list') return { ok: false, error: `${request.key} exists but is not an array` };

      return { ok: true, result: doc.counter ?? 0 };
    }

    case 'listPush': {
      let existing = await findCacheDoc(ctx, request.key);
      if (existing && (isExpired(existing, now) || existing.kind === 'deleted')) {
        const cleanup = await deleteCacheKey(ctx, request.key);
        if (cleanup.hasMore) {
          return { ok: true, hasMore: true };
        }
        existing = null;
      }
      if (existing && existing.kind !== 'list') {
        return { ok: false, error: `${request.key} exists but is not an array` };
      }

      const doc = existing
        ? await writeCacheDoc(ctx, request.key, existing, {
            kind: 'list',
            keyPrefix: request.keyPrefix,
            counter: (existing.counter ?? 0) + 1,
            expiresAt: request.expiresAt,
          })
        : await writeCacheDoc(ctx, request.key, null, {
            kind: 'list',
            keyPrefix: request.keyPrefix,
            counter: 1,
            expiresAt: request.expiresAt,
          });

      await ctx.db.insert(CACHE_LIST_TABLE, {
        key: request.key,
        keyPrefix: request.keyPrefix,
        index: (doc.counter ?? 1) - 1,
        value: encodeValue(request.value),
      });

      return { ok: true };
    }

    case 'listFromTo': {
      const { doc, hasMore } = await getLiveCacheDoc(ctx, request.key, now);
      if (hasMore) return { ok: true, result: [], hasMore: true };
      if (!doc || doc.kind !== 'list') return { ok: true, result: [] };

      const range = normalizeListRange(request.from, request.to, doc.counter ?? 0);
      if (!range) return { ok: true, result: [] };

      const query = ctx.db.query(CACHE_LIST_TABLE).withIndex('by_key_index', (q: any) => {
        return q.eq('key', request.key).gte('index', range.from).lte('index', range.to);
      });
      const items = (await query.collect()) as CacheListItem[];

      return { ok: true, result: items.map(item => decodeValue(item.value)) };
    }

    case 'delete': {
      const cleanup = await deleteCacheKey(ctx, request.key);
      return { ok: true, hasMore: cleanup.hasMore };
    }

    case 'clear': {
      const hasMore = await clearPrefix(ctx, request.keyPrefix);
      return { ok: true, hasMore };
    }

    case 'increment': {
      let existing = await findCacheDoc(ctx, request.key);
      if (existing && (isExpired(existing, now) || existing.kind === 'deleted')) {
        const cleanup = await deleteCacheKey(ctx, request.key);
        if (cleanup.hasMore) {
          return { ok: true, hasMore: true };
        }
        existing = null;
      }
      if (existing && existing.kind !== 'counter') {
        return { ok: false, error: `${request.key} exists but is not a number` };
      }

      const nextCounter = (existing?.counter ?? 0) + 1;
      await writeCacheDoc(ctx, request.key, existing, {
        kind: 'counter',
        keyPrefix: request.keyPrefix,
        counter: nextCounter,
        expiresAt: request.expiresAt,
      });

      return { ok: true, result: nextCounter };
    }

    case 'appendIndexedLog': {
      if (!isIndexedLogRetention(request.retention)) {
        return { ok: false, error: 'Indexed log retention requires positive safe-integer limits' };
      }
      if (!hasValidProposedLogGeneration(request.proposedLogGeneration)) {
        return { ok: false, error: 'Indexed log proposedLogGeneration must be a non-empty string' };
      }
      let existing = await findCacheDoc(ctx, request.key);
      if (existing && (isExpired(existing, now) || existing.kind === 'deleted')) {
        const cleanup = await deleteCacheKey(ctx, request.key);
        if (cleanup.hasMore) return { ok: true, hasMore: true };
        existing = null;
      }
      if (existing && existing.kind !== 'indexed-log') {
        return { ok: false, error: `${request.key} exists but is not an indexed log` };
      }

      if (existing && (await isIndexedLogFullyExpired(ctx, existing, request.retention, now))) {
        const cleanup = await deleteCacheKey(ctx, request.key);
        if (cleanup.hasMore) return { ok: true, hasMore: true };
        existing = null;
      }

      if (existing) {
        const pruned = await pruneIndexedLog(ctx, existing, request.retention, now, 1);
        if (pruned.hasMore) return { ok: true, hasMore: true };
        existing = pruned.doc;
      }

      const cursor = existing?.counter ?? 0;
      if (!Number.isSafeInteger(cursor) || cursor < 0 || cursor >= Number.MAX_SAFE_INTEGER) {
        return { ok: false, error: `${request.key} indexed log cursor exceeded the safe integer range` };
      }
      const expiresAt = now + request.retention.maxAgeMs;
      const logGeneration = existing?.logGeneration ?? request.proposedLogGeneration;
      await writeCacheDoc(ctx, request.key, existing, {
        kind: 'indexed-log',
        keyPrefix: request.keyPrefix,
        counter: cursor + 1,
        retainedCount: (existing?.retainedCount ?? 0) + 1,
        logGeneration,
        expiresAt,
      });
      await ctx.db.insert(CACHE_LIST_TABLE, {
        key: request.key,
        keyPrefix: request.keyPrefix,
        index: cursor,
        storedAt: now,
        value: encodeValue(request.value),
      });
      return { ok: true, result: { cursor, storedAt: now, value: request.value, logGeneration } };
    }

    case 'readIndexedLog': {
      if (!isIndexedLogRetention(request.retention)) {
        return { ok: false, error: 'Indexed log retention requires positive safe-integer limits' };
      }
      if (!hasValidProposedLogGeneration(request.proposedLogGeneration)) {
        return { ok: false, error: 'Indexed log proposedLogGeneration must be a non-empty string' };
      }
      if (!Number.isSafeInteger(request.afterCursor)) {
        return { ok: false, error: 'Indexed log afterCursor must be a safe integer' };
      }
      const { doc, hasMore: cleanupHasMore } = await getLiveCacheDoc(ctx, request.key, now);
      if (cleanupHasMore) return { ok: true, hasMore: true };
      if (!doc) {
        await writeCacheDoc(ctx, request.key, null, {
          kind: 'indexed-log',
          keyPrefix: request.keyPrefix,
          counter: 0,
          retainedCount: 0,
          logGeneration: request.proposedLogGeneration,
          expiresAt: null,
        });
        return {
          ok: true,
          result: {
            entries: [],
            logGeneration: request.proposedLogGeneration,
            firstCursor: 0,
            nextCursor: 0,
          },
        };
      }
      if (doc.kind !== 'indexed-log') {
        return { ok: false, error: `${request.key} exists but is not an indexed log` };
      }

      if (await isIndexedLogFullyExpired(ctx, doc, request.retention, now)) {
        const cleanup = await deleteCacheKey(ctx, request.key);
        if (cleanup.hasMore) return { ok: true, hasMore: true };
        await writeCacheDoc(ctx, request.key, null, {
          kind: 'indexed-log',
          keyPrefix: request.keyPrefix,
          counter: 0,
          retainedCount: 0,
          logGeneration: request.proposedLogGeneration,
          expiresAt: null,
        });
        return {
          ok: true,
          result: {
            entries: [],
            logGeneration: request.proposedLogGeneration,
            firstCursor: 0,
            nextCursor: 0,
          },
        };
      }

      const pruned = await pruneIndexedLog(ctx, doc, request.retention, now, 0);
      if (pruned.hasMore) return { ok: true, hasMore: true };
      const nextCursor = pruned.doc.counter ?? 0;
      const logGeneration = pruned.doc.logGeneration ?? request.proposedLogGeneration;
      if (!pruned.doc.logGeneration) {
        await ctx.db.patch(pruned.doc._id, { logGeneration });
      }
      const query = ctx.db.query(CACHE_LIST_TABLE).withIndex('by_key_index', (q: any) => {
        return q.eq('key', request.key).gte('index', request.afterCursor + 1);
      });
      const entries = (await query.collect()) as CacheListItem[];
      const first = (await ctx.db
        .query(CACHE_LIST_TABLE)
        .withIndex('by_key_index', (q: any) => q.eq('key', request.key))
        .first()) as CacheListItem | null;
      return {
        ok: true,
        result: {
          entries: entries.map(item => ({
            cursor: item.index,
            storedAt: item.storedAt ?? 0,
            value: decodeValue(item.value),
          })),
          logGeneration,
          firstCursor: first?.index ?? nextCursor,
          nextCursor,
        },
      };
    }
  }

  return { ok: false, error: `Unsupported operation ${(request as any).op}` };
}

export const mastraCache = mutationGeneric(
  async (ctx, request: CacheRequest): Promise<CacheResponse> => handleCacheOperation(ctx, request),
);
