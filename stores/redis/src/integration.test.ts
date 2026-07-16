/**
 * Integration tests for RedisServerCache with real Redis.
 *
 * These tests require Docker to be running with Redis:
 * ```bash
 * cd stores/redis && docker-compose up -d
 * ```
 *
 * Run tests:
 * ```bash
 * pnpm test
 * ```
 */

import { CachingPubSub, EventEmitterPubSub } from '@mastra/core/events';
import type { Event } from '@mastra/core/events';
import Redis from 'ioredis';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';

import { RedisServerCache } from './index';

// Redis connection for integration tests
// Must match docker-compose.yaml (port 6380, password redis_password)
const REDIS_PORT = 6380;
const REDIS_URL = `redis://:redis_password@localhost:${REDIS_PORT}`;

describe('RedisServerCache Integration', () => {
  let redis: Redis;
  let cache: RedisServerCache;

  beforeAll(async () => {
    redis = new Redis(REDIS_URL, {
      maxRetriesPerRequest: 3,
      retryStrategy: times => {
        if (times > 3) {
          throw new Error(`Redis connection failed after ${times} attempts. Is Docker running?`);
        }
        return Math.min(times * 100, 1000);
      },
    });

    // Wait for connection
    await new Promise<void>((resolve, reject) => {
      redis.once('ready', resolve);
      redis.once('error', reject);
      setTimeout(() => reject(new Error('Redis connection timeout')), 5000);
    });
  });

  afterAll(async () => {
    await redis.quit();
  });

  beforeEach(async () => {
    cache = new RedisServerCache(
      { client: redis },
      {
        keyPrefix: 'test:',
        ttlSeconds: 60, // 1 minute for tests
      },
    );
    // Clear test keys
    await cache.clear();
  });

  afterEach(async () => {
    await cache.clear();
  });

  describe('basic operations', () => {
    it('should set and get values', async () => {
      await cache.set('key1', { foo: 'bar' });
      const result = await cache.get('key1');
      // Cache handles JSON serialization/deserialization automatically
      expect(result).toEqual({ foo: 'bar' });
    });

    it('should return null for non-existent keys', async () => {
      const result = await cache.get('non-existent');
      expect(result).toBeNull();
    });

    it('should delete keys', async () => {
      await cache.set('key1', 'value');
      await cache.delete('key1');
      const result = await cache.get('key1');
      expect(result).toBeNull();
    });
  });

  describe('list operations', () => {
    it('should push and retrieve list items', async () => {
      // Cache handles JSON serialization automatically
      await cache.listPush('mylist', { id: '1' });
      await cache.listPush('mylist', { id: '2' });
      await cache.listPush('mylist', { id: '3' });

      const items = await cache.listFromTo('mylist', 0);
      expect(items).toHaveLength(3);
      expect(items[0]).toEqual({ id: '1' });
      expect(items[2]).toEqual({ id: '3' });
    });

    it('should return list length', async () => {
      await cache.listPush('mylist', 'item1');
      await cache.listPush('mylist', 'item2');

      const length = await cache.listLength('mylist');
      expect(length).toBe(2);
    });

    it('should return range of items', async () => {
      await cache.listPush('mylist', 'a');
      await cache.listPush('mylist', 'b');
      await cache.listPush('mylist', 'c');
      await cache.listPush('mylist', 'd');

      const range = await cache.listFromTo('mylist', 1, 2);
      expect(range).toEqual(['b', 'c']);
    });
  });

  describe('TTL behavior', () => {
    it('should expire items after TTL', async () => {
      const shortTtlCache = new RedisServerCache(
        { client: redis },
        {
          keyPrefix: 'ttl-test:',
          ttlSeconds: 1, // 1 second TTL
        },
      );

      await shortTtlCache.set('expiring-key', 'value');

      // Value should exist immediately
      const before = await shortTtlCache.get('expiring-key');
      expect(before).toBe('value');

      // Wait for expiry
      await new Promise(resolve => setTimeout(resolve, 1500));

      // Value should be gone
      const after = await shortTtlCache.get('expiring-key');
      expect(after).toBeNull();
    });
  });

  describe('atomic indexed log', () => {
    it('allocates concurrent cursors in the same order as retained entries', async () => {
      const retention = { maxAgeMs: 60_000, maxEntries: 100 };
      const appended = await Promise.all(
        Array.from({ length: 40 }, (_, index) =>
          cache.appendIndexedLogEntry('indexed-concurrent', { index }, retention),
        ),
      );
      const retained = await cache.readIndexedLogEntries<{ index: number }>('indexed-concurrent', -1, retention);

      expect(appended.map(entry => entry.cursor).sort((a, b) => a - b)).toEqual(
        Array.from({ length: 40 }, (_, index) => index),
      );
      expect(new Set(appended.map(entry => entry.logGeneration))).toEqual(new Set([retained.logGeneration]));
      expect(retained.entries.map(entry => entry.cursor)).toEqual(Array.from({ length: 40 }, (_, index) => index));
      expect(retained.logGeneration).toEqual(expect.any(String));
      expect(retained.firstCursor).toBe(0);
      expect(retained.nextCursor).toBe(40);
    });

    it('reports the retained floor after count and age pruning', async () => {
      const countRetention = { maxAgeMs: 60_000, maxEntries: 2 };
      await cache.appendIndexedLogEntry('indexed-count', 'zero', countRetention);
      await cache.appendIndexedLogEntry('indexed-count', 'one', countRetention);
      await cache.appendIndexedLogEntry('indexed-count', 'two', countRetention);

      await expect(cache.readIndexedLogEntries('indexed-count', -1, countRetention)).resolves.toMatchObject({
        firstCursor: 1,
        nextCursor: 3,
        entries: [
          { cursor: 1, value: 'one' },
          { cursor: 2, value: 'two' },
        ],
      });

      const ageRetention = { maxAgeMs: 300, maxEntries: 10 };
      await cache.appendIndexedLogEntry('indexed-age', 'old', ageRetention);
      await new Promise(resolve => setTimeout(resolve, 200));
      await cache.appendIndexedLogEntry('indexed-age', 'new', ageRetention);
      await new Promise(resolve => setTimeout(resolve, 150));

      await expect(cache.readIndexedLogEntries('indexed-age', -1, ageRetention)).resolves.toMatchObject({
        firstCursor: 1,
        nextCursor: 2,
        entries: [{ cursor: 1, value: 'new' }],
      });
    });

    it('deletes retained entries and resets an empty range', async () => {
      const retention = { maxAgeMs: 60_000, maxEntries: 10 };
      await cache.appendIndexedLogEntry('indexed-delete', { id: 'event' }, retention);
      const beforeDelete = await cache.readIndexedLogEntries('indexed-delete', -1, retention);

      await cache.deleteIndexedLog('indexed-delete');

      const afterDelete = await cache.readIndexedLogEntries('indexed-delete', -1, retention);
      expect(afterDelete).toEqual({
        logGeneration: expect.any(String),
        firstCursor: 0,
        nextCursor: 0,
        entries: [],
      });
      expect(afterDelete.logGeneration).not.toBe(beforeDelete.logGeneration);
    });

    it('keeps an empty log generation stable until the first append', async () => {
      const retention = { maxAgeMs: 50, maxEntries: 10 };
      const first = await cache.readIndexedLogEntries('indexed-empty', -1, retention);

      await new Promise(resolve => setTimeout(resolve, 100));

      const second = await cache.readIndexedLogEntries('indexed-empty', -1, retention);
      const appended = await cache.appendIndexedLogEntry('indexed-empty', 'first', retention);
      const afterAppend = await cache.readIndexedLogEntries('indexed-empty', -1, retention);

      expect(second.logGeneration).toBe(first.logGeneration);
      expect(appended.cursor).toBe(0);
      expect(afterAppend.logGeneration).toBe(first.logGeneration);
    });

    it('recreates a fully expired non-empty log when retention becomes shorter', async () => {
      const key = 'indexed-shortened-retention';
      const original = { maxAgeMs: 5_000, maxEntries: 10 };
      const shortened = { maxAgeMs: 50, maxEntries: 10 };
      const firstAppend = await cache.appendIndexedLogEntry(key, 'old', original);

      await new Promise(resolve => setTimeout(resolve, 75));

      const recreated = await cache.readIndexedLogEntries(key, -1, shortened);
      const nextAppend = await cache.appendIndexedLogEntry(key, 'new', shortened);

      expect(recreated).toEqual({
        entries: [],
        logGeneration: expect.any(String),
        firstCursor: 0,
        nextCursor: 0,
      });
      expect(recreated.logGeneration).not.toBe(firstAppend.logGeneration);
      expect(nextAppend).toMatchObject({ cursor: 0, logGeneration: recreated.logGeneration, value: 'new' });
    });
  });
});

describe('CachingPubSub with Redis Integration', () => {
  let redis: Redis;
  let cache: RedisServerCache;
  let innerPubsub: EventEmitterPubSub;
  let cachingPubsub: CachingPubSub;

  beforeAll(async () => {
    redis = new Redis(REDIS_URL, {
      maxRetriesPerRequest: 3,
      retryStrategy: times => {
        if (times > 3) {
          throw new Error(`Redis connection failed after ${times} attempts`);
        }
        return Math.min(times * 100, 1000);
      },
    });

    await new Promise<void>((resolve, reject) => {
      redis.once('ready', resolve);
      redis.once('error', reject);
      setTimeout(() => reject(new Error('Redis connection timeout')), 5000);
    });
  });

  afterAll(async () => {
    await redis.quit();
  });

  beforeEach(async () => {
    cache = new RedisServerCache(
      { client: redis },
      {
        keyPrefix: 'pubsub-test:',
        ttlSeconds: 60,
      },
    );
    innerPubsub = new EventEmitterPubSub();
    cachingPubsub = new CachingPubSub(innerPubsub, cache);
    await cache.clear();
  });

  afterEach(async () => {
    await cache.clear();
  });

  describe('resumable streams with Redis backend', () => {
    it('replays exact indexed history through a fresh cache and pubsub instance', async () => {
      const topic = 'exact-restart';
      const indexedReplay = { retentionMs: 60_000, maxEvents: 10 };
      const first = new CachingPubSub(new EventEmitterPubSub(), cache, { indexedReplay });

      await first.publish(topic, { type: 'zero', runId: 'run-exact', data: { value: 0 } });
      await first.publish(topic, { type: 'one', runId: 'run-exact', data: { value: 1 } });
      const firstRange = await first.getIndexedReplayRange(topic);

      const restartedCache = new RedisServerCache(
        { client: redis },
        {
          keyPrefix: 'pubsub-test:',
          ttlSeconds: 60,
        },
      );
      const restarted = new CachingPubSub(new EventEmitterPubSub(), restartedCache, { indexedReplay });
      const received: Event[] = [];

      await restarted.subscribeFromOffset(topic, 0, event => {
        received.push(event);
      });

      expect(received.map(event => [event.type, event.index])).toEqual([
        ['zero', 0],
        ['one', 1],
      ]);
      await expect(restarted.getIndexedReplayRange(topic)).resolves.toEqual(firstRange);
    });

    it('should replay events to late subscriber', async () => {
      const topic = 'test-stream-1';
      const receivedEvents: Event[] = [];

      // Publish events before any subscriber
      await cachingPubsub.publish(topic, {
        type: 'chunk',
        runId: 'run-1',
        data: { text: 'Hello ' },
      });
      await cachingPubsub.publish(topic, {
        type: 'chunk',
        runId: 'run-1',
        data: { text: 'World!' },
      });

      // Wait for Redis writes
      await new Promise(resolve => setTimeout(resolve, 50));

      // Late subscriber should receive all events
      await cachingPubsub.subscribeWithReplay(topic, event => {
        receivedEvents.push(event);
      });

      expect(receivedEvents).toHaveLength(2);
      expect(receivedEvents[0].data).toEqual({ text: 'Hello ' });
      expect(receivedEvents[1].data).toEqual({ text: 'World!' });
    });

    it('should receive both cached and live events', async () => {
      const topic = 'test-stream-2';
      const receivedEvents: Event[] = [];

      // Publish cached event
      await cachingPubsub.publish(topic, {
        type: 'chunk',
        runId: 'run-2',
        data: { text: 'Cached' },
      });
      await new Promise(resolve => setTimeout(resolve, 50));

      // Subscribe with replay
      await cachingPubsub.subscribeWithReplay(topic, event => {
        receivedEvents.push(event);
      });

      // Publish live event
      await cachingPubsub.publish(topic, {
        type: 'chunk',
        runId: 'run-2',
        data: { text: 'Live' },
      });

      expect(receivedEvents).toHaveLength(2);
      expect(receivedEvents[0].data).toEqual({ text: 'Cached' });
      expect(receivedEvents[1].data).toEqual({ text: 'Live' });
    });

    it('should handle disconnect/reconnect scenario', async () => {
      const topic = 'test-stream-3';
      const firstConnectionEvents: Event[] = [];
      const reconnectionEvents: Event[] = [];

      // First connection
      const firstCallback = (event: Event) => {
        firstConnectionEvents.push(event);
      };
      await cachingPubsub.subscribeWithReplay(topic, firstCallback);

      // Receive some events
      await cachingPubsub.publish(topic, {
        type: 'chunk',
        runId: 'run-3',
        data: { text: 'Before disconnect' },
      });

      expect(firstConnectionEvents).toHaveLength(1);

      // Disconnect
      await cachingPubsub.unsubscribe(topic, firstCallback);

      // Events while disconnected
      await cachingPubsub.publish(topic, {
        type: 'chunk',
        runId: 'run-3',
        data: { text: 'During disconnect' },
      });
      await new Promise(resolve => setTimeout(resolve, 50));

      // Reconnect - should get all events
      await cachingPubsub.subscribeWithReplay(topic, event => {
        reconnectionEvents.push(event);
      });

      expect(reconnectionEvents).toHaveLength(2);
      expect(reconnectionEvents[0].data).toEqual({ text: 'Before disconnect' });
      expect(reconnectionEvents[1].data).toEqual({ text: 'During disconnect' });
    });

    it('should maintain topic isolation', async () => {
      const topic1 = 'stream-a';
      const topic2 = 'stream-b';
      const topic1Events: Event[] = [];
      const topic2Events: Event[] = [];

      await cachingPubsub.publish(topic1, {
        type: 'chunk',
        runId: 'a',
        data: { source: 'topic1' },
      });
      await cachingPubsub.publish(topic2, {
        type: 'chunk',
        runId: 'b',
        data: { source: 'topic2' },
      });
      await new Promise(resolve => setTimeout(resolve, 50));

      await cachingPubsub.subscribeWithReplay(topic1, event => {
        topic1Events.push(event);
      });
      await cachingPubsub.subscribeWithReplay(topic2, event => {
        topic2Events.push(event);
      });

      expect(topic1Events).toHaveLength(1);
      expect(topic1Events[0].data).toEqual({ source: 'topic1' });

      expect(topic2Events).toHaveLength(1);
      expect(topic2Events[0].data).toEqual({ source: 'topic2' });
    });
  });
});
