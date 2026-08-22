import { describe, it, expect, beforeEach, vi } from 'vitest';
import { RedisServerCache, upstashPreset, nodeRedisPreset } from './index';
import type { RedisClient } from './index';

// Create a mock Redis client
function createMockClient(): RedisClient & { [key: string]: ReturnType<typeof vi.fn> } {
  return {
    get: vi.fn(),
    set: vi.fn(),
    llen: vi.fn(),
    rpush: vi.fn(),
    lrange: vi.fn(),
    del: vi.fn(),
    expire: vi.fn(),
    scan: vi.fn(),
    incr: vi.fn(),
    eval: vi.fn(),
  };
}

describe('RedisServerCache', () => {
  let mockClient: ReturnType<typeof createMockClient>;
  let cache: RedisServerCache;

  beforeEach(() => {
    mockClient = createMockClient();
    cache = new RedisServerCache({ client: mockClient });
  });

  describe('get', () => {
    it('should get a value with prefixed key and deserialize JSON', async () => {
      // Redis returns JSON string, cache deserializes it
      mockClient.get.mockResolvedValue('{"foo":"bar"}');

      const result = await cache.get('test-key');

      expect(mockClient.get).toHaveBeenCalledWith('mastra:cache:test-key');
      expect(result).toEqual({ foo: 'bar' });
    });

    it('should return null for non-existent key', async () => {
      mockClient.get.mockResolvedValue(null);

      const result = await cache.get('non-existent');

      expect(result).toBeNull();
    });

    it('should return plain string if not valid JSON', async () => {
      mockClient.get.mockResolvedValue('plain-string');

      const result = await cache.get('test-key');

      expect(result).toBe('plain-string');
    });
  });

  describe('set', () => {
    it('should set a value with TTL by default (ioredis style) and serialize to JSON', async () => {
      mockClient.set.mockResolvedValue('OK');

      await cache.set('test-key', { foo: 'bar' });

      // Default uses ioredis style: set(key, serialized-value, 'EX', seconds)
      expect(mockClient.set).toHaveBeenCalledWith('mastra:cache:test-key', '{"foo":"bar"}', 'EX', 300);
    });

    it('should set without TTL when ttlSeconds is 0', async () => {
      const noTtlCache = new RedisServerCache({ client: mockClient }, { ttlSeconds: 0 });
      mockClient.set.mockResolvedValue('OK');

      await noTtlCache.set('test-key', { foo: 'bar' });

      expect(mockClient.set).toHaveBeenCalledWith('mastra:cache:test-key', '{"foo":"bar"}');
    });

    it('should use custom TTL when specified', async () => {
      const customTtlCache = new RedisServerCache({ client: mockClient }, { ttlSeconds: 600 });
      mockClient.set.mockResolvedValue('OK');

      await customTtlCache.set('test-key', { foo: 'bar' });

      expect(mockClient.set).toHaveBeenCalledWith('mastra:cache:test-key', '{"foo":"bar"}', 'EX', 600);
    });
  });

  describe('listLength', () => {
    it('should return list length', async () => {
      mockClient.llen.mockResolvedValue(5);

      const result = await cache.listLength('my-list');

      expect(mockClient.llen).toHaveBeenCalledWith('mastra:cache:my-list');
      expect(result).toBe(5);
    });
  });

  describe('listPush', () => {
    it('should push serialized value to list and refresh TTL', async () => {
      mockClient.rpush.mockResolvedValue(1);
      mockClient.expire.mockResolvedValue(1);

      await cache.listPush('my-list', { event: 'test' });

      expect(mockClient.rpush).toHaveBeenCalledWith('mastra:cache:my-list', '{"event":"test"}');
      expect(mockClient.expire).toHaveBeenCalledWith('mastra:cache:my-list', 300);
    });

    it('should not refresh TTL when ttlSeconds is 0', async () => {
      const noTtlCache = new RedisServerCache({ client: mockClient }, { ttlSeconds: 0 });
      mockClient.rpush.mockResolvedValue(1);

      await noTtlCache.listPush('my-list', { event: 'test' });

      expect(mockClient.rpush).toHaveBeenCalledWith('mastra:cache:my-list', '{"event":"test"}');
      expect(mockClient.expire).not.toHaveBeenCalled();
    });
  });

  describe('listFromTo', () => {
    it('should get range from list and deserialize items', async () => {
      // Redis returns JSON strings, cache deserializes them
      const storedEvents = ['{"id":"1"}', '{"id":"2"}', '{"id":"3"}'];
      mockClient.lrange.mockResolvedValue(storedEvents);

      const result = await cache.listFromTo('my-list', 0, 2);

      expect(mockClient.lrange).toHaveBeenCalledWith('mastra:cache:my-list', 0, 2);
      expect(result).toEqual([{ id: '1' }, { id: '2' }, { id: '3' }]);
    });

    it('should use -1 as default end index', async () => {
      mockClient.lrange.mockResolvedValue([]);

      await cache.listFromTo('my-list', 0);

      expect(mockClient.lrange).toHaveBeenCalledWith('mastra:cache:my-list', 0, -1);
    });
  });

  describe('increment', () => {
    it('should increment with prefixed key and refresh TTL', async () => {
      mockClient.incr.mockResolvedValue(3);
      mockClient.expire.mockResolvedValue(1);

      const result = await cache.increment('counter');

      expect(mockClient.incr).toHaveBeenCalledWith('mastra:cache:counter');
      expect(mockClient.expire).toHaveBeenCalledWith('mastra:cache:counter', 300);
      expect(result).toBe(3);
    });

    it('should not refresh TTL when ttlSeconds is 0', async () => {
      const noTtlCache = new RedisServerCache({ client: mockClient }, { ttlSeconds: 0 });
      mockClient.incr.mockResolvedValue(1);

      const result = await noTtlCache.increment('counter');

      expect(mockClient.incr).toHaveBeenCalledWith('mastra:cache:counter');
      expect(mockClient.expire).not.toHaveBeenCalled();
      expect(result).toBe(1);
    });
  });

  describe('delete', () => {
    it('should delete a key', async () => {
      mockClient.del.mockResolvedValue(1);

      await cache.delete('test-key');

      expect(mockClient.del).toHaveBeenCalledWith('mastra:cache:test-key');
    });
  });

  describe('clear', () => {
    it('should scan and delete all keys with prefix', async () => {
      // First scan returns some keys, second returns empty
      mockClient.scan
        .mockResolvedValueOnce(['5', ['mastra:cache:key1', 'mastra:cache:key2']])
        .mockResolvedValueOnce(['0', []]);
      mockClient.del.mockResolvedValue(2);

      await cache.clear();

      expect(mockClient.scan).toHaveBeenCalledWith('0', 'MATCH', 'mastra:cache:*', 'COUNT', 100);
      expect(mockClient.del).toHaveBeenCalledWith('mastra:cache:key1', 'mastra:cache:key2');
    });

    it('should handle empty cache', async () => {
      mockClient.scan.mockResolvedValue(['0', []]);

      await cache.clear();

      expect(mockClient.scan).toHaveBeenCalled();
      expect(mockClient.del).not.toHaveBeenCalled();
    });

    it('should handle numeric cursor (for ioredis compatibility)', async () => {
      mockClient.scan.mockResolvedValueOnce([5, ['mastra:cache:key1']]).mockResolvedValueOnce([0, []]);
      mockClient.del.mockResolvedValue(1);

      await cache.clear();

      expect(mockClient.del).toHaveBeenCalledWith('mastra:cache:key1');
    });
  });

  describe('key prefix', () => {
    it('should use custom key prefix', async () => {
      const customCache = new RedisServerCache({ client: mockClient }, { keyPrefix: 'myapp:' });
      mockClient.get.mockResolvedValue('value');

      await customCache.get('test-key');

      expect(mockClient.get).toHaveBeenCalledWith('myapp:test-key');
    });
  });

  describe('atomic indexed log', () => {
    it('keeps ordinary cache operations available when a custom client has no eval capability', async () => {
      delete mockClient.eval;
      mockClient.set.mockResolvedValue('OK');

      await cache.set('ordinary-key', { value: true });

      expect(mockClient.set).toHaveBeenCalledWith('mastra:cache:ordinary-key', '{"value":true}', 'EX', 300);
      await expect(
        cache.appendIndexedLogEntry('events', { id: 'event-0' }, { maxAgeMs: 60_000, maxEntries: 10 }),
      ).rejects.toThrow(/require a client eval implementation or RedisServerCacheOptions\.evalScript adapter/);
    });

    it('advertises durable scope and decodes an atomic append response', async () => {
      mockClient.eval.mockResolvedValue(['4', '1767225600000', '{"id":"event-4"}', 'generation-a']);

      const result = await cache.appendIndexedLogEntry(
        'events',
        { id: 'event-4' },
        { maxAgeMs: 60_000, maxEntries: 10 },
      );

      expect(cache.indexedLogScope).toBe('durable');
      expect(result).toEqual({
        cursor: 4,
        storedAt: 1767225600000,
        value: { id: 'event-4' },
        logGeneration: 'generation-a',
      });
      expect(mockClient.eval).toHaveBeenCalledWith(
        expect.any(String),
        1,
        'mastra:cache:events',
        '{"id":"event-4"}',
        '60000',
        '10',
        expect.any(String),
      );
    });

    it('decodes retained range metadata and entries', async () => {
      mockClient.eval.mockResolvedValue([
        'generation-a',
        '3',
        '5',
        '3',
        '1767225600000',
        '{"id":"event-3"}',
        '4',
        '1767225600001',
        '{"id":"event-4"}',
      ]);

      await expect(
        cache.readIndexedLogEntries<{ id: string }>('events', 2, { maxAgeMs: 60_000, maxEntries: 10 }),
      ).resolves.toEqual({
        logGeneration: 'generation-a',
        firstCursor: 3,
        nextCursor: 5,
        entries: [
          { cursor: 3, storedAt: 1767225600000, value: { id: 'event-3' } },
          { cursor: 4, storedAt: 1767225600001, value: { id: 'event-4' } },
        ],
      });
    });

    it('deletes the complete indexed log key', async () => {
      mockClient.del.mockResolvedValue(1);

      await cache.deleteIndexedLog('events');

      expect(mockClient.del).toHaveBeenCalledWith('mastra:cache:events');
    });
  });

  describe('upstashPreset', () => {
    it('should use upstash-style set with expiry', async () => {
      const upstashCache = new RedisServerCache({ client: mockClient }, upstashPreset);
      mockClient.set.mockResolvedValue('OK');

      await upstashCache.set('test-key', 'value');

      // Upstash uses { ex: seconds } style, value is serialized to JSON
      expect(mockClient.set).toHaveBeenCalledWith('mastra:cache:test-key', '"value"', { ex: 300 });
    });

    it('should use upstash-style scan', async () => {
      const upstashCache = new RedisServerCache({ client: mockClient }, upstashPreset);
      mockClient.scan.mockResolvedValue(['0', []]);

      await upstashCache.clear();

      // Upstash uses { match, count } style
      expect(mockClient.scan).toHaveBeenCalledWith('0', { match: 'mastra:cache:*', count: 100 });
    });

    it('uses Upstash array arguments for indexed-log scripts', async () => {
      const upstashCache = new RedisServerCache({ client: mockClient }, upstashPreset);
      mockClient.eval.mockResolvedValue(['0', '1767225600000', '{}', 'generation-a']);

      await upstashCache.appendIndexedLogEntry('events', {}, { maxAgeMs: 1000, maxEntries: 2 });

      expect(mockClient.eval).toHaveBeenCalledWith(
        expect.any(String),
        ['mastra:cache:events'],
        ['{}', '1000', '2', expect.any(String)],
      );
    });
  });

  describe('nodeRedisPreset', () => {
    it('should use node-redis-style set with expiry', async () => {
      const nodeCache = new RedisServerCache({ client: mockClient }, nodeRedisPreset);
      mockClient.set.mockResolvedValue('OK');

      await nodeCache.set('test-key', 'value');

      // node-redis uses { EX: seconds } style, value is serialized to JSON
      expect(mockClient.set).toHaveBeenCalledWith('mastra:cache:test-key', '"value"', { EX: 300 });
    });

    it('should use node-redis-style scan', async () => {
      const nodeCache = new RedisServerCache({ client: mockClient }, nodeRedisPreset);
      mockClient.scan.mockResolvedValue(['0', []]);

      await nodeCache.clear();

      // node-redis uses { MATCH, COUNT } style
      expect(mockClient.scan).toHaveBeenCalledWith('0', { MATCH: 'mastra:cache:*', COUNT: 100 });
    });

    it('routes list length through lLen (camelCase) on node-redis clients', async () => {
      const nodeMock: any = { ...createMockClient(), lLen: vi.fn().mockResolvedValue(7) };
      const nodeCache = new RedisServerCache({ client: nodeMock }, nodeRedisPreset);

      const result = await nodeCache.listLength('my-list');

      expect(result).toBe(7);
      expect(nodeMock.lLen).toHaveBeenCalledWith('mastra:cache:my-list');
      expect(nodeMock.llen).not.toHaveBeenCalled();
    });

    it('routes list push through rPush (camelCase) on node-redis clients', async () => {
      const nodeMock: any = { ...createMockClient(), rPush: vi.fn().mockResolvedValue(1) };
      const nodeCache = new RedisServerCache({ client: nodeMock }, nodeRedisPreset);

      await nodeCache.listPush('my-list', { event: 'test' });

      expect(nodeMock.rPush).toHaveBeenCalledWith('mastra:cache:my-list', '{"event":"test"}');
      expect(nodeMock.rpush).not.toHaveBeenCalled();
    });

    it('routes list range through lRange (camelCase) on node-redis clients', async () => {
      const nodeMock: any = {
        ...createMockClient(),
        lRange: vi.fn().mockResolvedValue(['"a"', '"b"']),
      };
      const nodeCache = new RedisServerCache({ client: nodeMock }, nodeRedisPreset);

      const result = await nodeCache.listFromTo('my-list', 0, -1);

      expect(result).toEqual(['a', 'b']);
      expect(nodeMock.lRange).toHaveBeenCalledWith('mastra:cache:my-list', 0, -1);
      expect(nodeMock.lrange).not.toHaveBeenCalled();
    });

    it('uses node-redis key and argument objects for indexed-log scripts', async () => {
      const nodeMock: any = {
        ...createMockClient(),
        eval: vi.fn().mockResolvedValue(['0', '1767225600000', '{}', 'generation-a']),
      };
      const nodeCache = new RedisServerCache({ client: nodeMock }, nodeRedisPreset);

      await nodeCache.appendIndexedLogEntry('events', {}, { maxAgeMs: 1000, maxEntries: 2 });

      expect(nodeMock.eval).toHaveBeenCalledWith(expect.any(String), {
        keys: ['mastra:cache:events'],
        arguments: ['{}', '1000', '2', expect.any(String)],
      });
    });
  });
});
