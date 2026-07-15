import { MastraServerCache } from '@mastra/core/cache';
import type {
  AtomicIndexedLogCache,
  IndexedLogAppendResult,
  IndexedLogEntry,
  IndexedLogReadResult,
  IndexedLogRetention,
} from '@mastra/core/cache';

export interface RedisClient {
  get(key: string): Promise<unknown>;
  set(key: string, value: unknown, ...args: unknown[]): Promise<unknown>;
  llen(key: string): Promise<number>;
  rpush(key: string, ...values: unknown[]): Promise<number>;
  lrange(key: string, start: number, stop: number): Promise<unknown[]>;
  del(...keys: string[]): Promise<number>;
  expire(key: string, seconds: number): Promise<number | boolean>;
  scan(cursor: string | number, ...args: unknown[]): Promise<[string | number, string[]]>;
  incr(key: string): Promise<number>;
  eval(script: string, ...args: unknown[]): Promise<unknown>;
}

export interface RedisServerCacheOptions {
  keyPrefix?: string;
  ttlSeconds?: number;
  setWithExpiry?: (client: RedisClient, key: string, value: unknown, seconds: number) => Promise<unknown>;
  scanKeys?: (
    client: RedisClient,
    cursor: string | number,
    pattern: string,
    count: number,
  ) => Promise<[string | number, string[]]>;
  getListLength?: (client: RedisClient, key: string) => Promise<number>;
  pushToList?: (client: RedisClient, key: string, value: unknown) => Promise<number>;
  getListRange?: (client: RedisClient, key: string, start: number, stop: number) => Promise<unknown[]>;
  evalScript?: (client: RedisClient, script: string, keys: string[], arguments_: string[]) => Promise<unknown>;
}

const defaultSetWithExpiry = (client: RedisClient, key: string, value: unknown, seconds: number): Promise<unknown> => {
  return client.set(key, value, 'EX', seconds);
};

const defaultScanKeys = (
  client: RedisClient,
  cursor: string | number,
  pattern: string,
  count: number,
): Promise<[string | number, string[]]> => {
  return client.scan(cursor, 'MATCH', pattern, 'COUNT', count);
};

const defaultGetListLength = (client: RedisClient, key: string): Promise<number> => {
  return client.llen(key);
};

const defaultPushToList = (client: RedisClient, key: string, value: unknown): Promise<number> => {
  return client.rpush(key, value);
};

const defaultGetListRange = (client: RedisClient, key: string, start: number, stop: number): Promise<unknown[]> => {
  return client.lrange(key, start, stop);
};

const defaultEvalScript = (
  client: RedisClient,
  script: string,
  keys: string[],
  arguments_: string[],
): Promise<unknown> => {
  return client.eval(script, keys.length, ...keys, ...arguments_);
};

const APPEND_INDEXED_LOG_SCRIPT = `
  local key = KEYS[1]
  local maxAgeMs = tonumber(ARGV[2])
  local maxEntries = tonumber(ARGV[3])
  local logGeneration = redis.call('HGET', key, 'generation')
  if logGeneration == false then
    logGeneration = ARGV[4]
    redis.call('HSET', key, 'generation', logGeneration)
  end
  local serverTime = redis.call('TIME')
  local now = tonumber(serverTime[1]) * 1000 + math.floor(tonumber(serverTime[2]) / 1000)
  local cutoff = now - maxAgeMs
  local nextCursor = tonumber(redis.call('HGET', key, 'next') or '0')
  if nextCursor >= 9007199254740991 then
    return redis.error_reply('indexed log cursor exceeded JavaScript safe integer range')
  end

  local cursors = {}
  for _, field in ipairs(redis.call('HKEYS', key)) do
    if string.sub(field, 1, 2) == 'e:' then
      local cursor = tonumber(string.sub(field, 3))
      local storedAt = tonumber(redis.call('HGET', key, 't:' .. cursor))
      if storedAt == nil then
        return redis.error_reply('indexed log entry is missing storedAt metadata')
      end
      if storedAt <= cutoff then
        redis.call('HDEL', key, field, 't:' .. cursor)
      else
        table.insert(cursors, cursor)
      end
    end
  end
  table.sort(cursors)

  if nextCursor > 0 and #cursors == 0 then
    redis.call('DEL', key)
    logGeneration = ARGV[4]
    nextCursor = 0
    redis.call('HSET', key, 'generation', logGeneration)
  end

  while #cursors >= maxEntries do
    local cursor = table.remove(cursors, 1)
    redis.call('HDEL', key, 'e:' .. cursor, 't:' .. cursor)
  end

  redis.call('HSET', key, 'e:' .. nextCursor, ARGV[1], 't:' .. nextCursor, now, 'next', nextCursor + 1)
  redis.call('PEXPIRE', key, maxAgeMs)
  return { tostring(nextCursor), tostring(now), ARGV[1], logGeneration }
`;

const READ_INDEXED_LOG_SCRIPT = `
  local key = KEYS[1]
  local afterCursor = tonumber(ARGV[1])
  local maxAgeMs = tonumber(ARGV[2])
  local maxEntries = tonumber(ARGV[3])
  local nextRaw = redis.call('HGET', key, 'next')
  if nextRaw == false then
    redis.call('HSET', key, 'generation', ARGV[4], 'next', 0)
    return { ARGV[4], '0', '0' }
  end

  local nextCursor = tonumber(nextRaw)
  local logGeneration = redis.call('HGET', key, 'generation')
  if logGeneration == false then
    logGeneration = ARGV[4]
    redis.call('HSET', key, 'generation', logGeneration)
  end
  local serverTime = redis.call('TIME')
  local now = tonumber(serverTime[1]) * 1000 + math.floor(tonumber(serverTime[2]) / 1000)
  local cutoff = now - maxAgeMs
  local cursors = {}
  for _, field in ipairs(redis.call('HKEYS', key)) do
    if string.sub(field, 1, 2) == 'e:' then
      local cursor = tonumber(string.sub(field, 3))
      local storedAt = tonumber(redis.call('HGET', key, 't:' .. cursor))
      if storedAt == nil then
        return redis.error_reply('indexed log entry is missing storedAt metadata')
      end
      if storedAt <= cutoff then
        redis.call('HDEL', key, field, 't:' .. cursor)
      else
        table.insert(cursors, cursor)
      end
    end
  end
  table.sort(cursors)

  if nextCursor > 0 and #cursors == 0 then
    redis.call('DEL', key)
    redis.call('HSET', key, 'generation', ARGV[4], 'next', 0)
    return { ARGV[4], '0', '0' }
  end

  while #cursors > maxEntries do
    local cursor = table.remove(cursors, 1)
    redis.call('HDEL', key, 'e:' .. cursor, 't:' .. cursor)
  end

  local firstCursor = cursors[1] or nextCursor
  local result = { logGeneration, tostring(firstCursor), tostring(nextCursor) }
  for _, cursor in ipairs(cursors) do
    if cursor > afterCursor then
      table.insert(result, tostring(cursor))
      table.insert(result, redis.call('HGET', key, 't:' .. cursor))
      table.insert(result, redis.call('HGET', key, 'e:' .. cursor))
    end
  end
  return result
`;

export class RedisServerCache extends MastraServerCache implements AtomicIndexedLogCache {
  readonly indexedLogScope = 'durable' as const;

  private client: RedisClient;
  private keyPrefix: string;
  private ttlSeconds: number;
  private setWithExpiry: (client: RedisClient, key: string, value: unknown, seconds: number) => Promise<unknown>;
  private scanKeys: (
    client: RedisClient,
    cursor: string | number,
    pattern: string,
    count: number,
  ) => Promise<[string | number, string[]]>;
  private getListLength: (client: RedisClient, key: string) => Promise<number>;
  private pushToList: (client: RedisClient, key: string, value: unknown) => Promise<number>;
  private getListRange: (client: RedisClient, key: string, start: number, stop: number) => Promise<unknown[]>;
  private evalScript: (client: RedisClient, script: string, keys: string[], arguments_: string[]) => Promise<unknown>;

  constructor(config: { client: RedisClient }, options: RedisServerCacheOptions = {}) {
    super({ name: 'RedisServerCache' });

    this.client = config.client;
    this.keyPrefix = options.keyPrefix ?? 'mastra:cache:';
    this.ttlSeconds = options.ttlSeconds ?? 300;
    this.setWithExpiry = options.setWithExpiry ?? defaultSetWithExpiry;
    this.scanKeys = options.scanKeys ?? defaultScanKeys;
    this.getListLength = options.getListLength ?? defaultGetListLength;
    this.pushToList = options.pushToList ?? defaultPushToList;
    this.getListRange = options.getListRange ?? defaultGetListRange;
    this.evalScript = options.evalScript ?? defaultEvalScript;
  }

  private getKey(key: string): string {
    return `${this.keyPrefix}${key}`;
  }

  private serialize(value: unknown): string {
    return JSON.stringify(value);
  }

  private deserialize(value: unknown): unknown {
    if (typeof value === 'string') {
      try {
        return JSON.parse(value);
      } catch {
        return value;
      }
    }
    return value;
  }

  async get(key: string): Promise<unknown> {
    const fullKey = this.getKey(key);
    const value = await this.client.get(fullKey);
    if (value === null) {
      return null;
    }
    return this.deserialize(value);
  }

  async set(key: string, value: unknown, ttlMs?: number): Promise<void> {
    const fullKey = this.getKey(key);
    const serialized = this.serialize(value);
    const overrideSeconds = ttlMs !== undefined ? Math.max(1, Math.ceil(ttlMs / 1000)) : undefined;
    const effectiveSeconds = overrideSeconds ?? this.ttlSeconds;
    if (effectiveSeconds > 0) {
      await this.setWithExpiry(this.client, fullKey, serialized, effectiveSeconds);
    } else {
      await this.client.set(fullKey, serialized);
    }
  }

  async listLength(key: string): Promise<number> {
    const fullKey = this.getKey(key);
    return this.getListLength(this.client, fullKey);
  }

  async listPush(key: string, value: unknown): Promise<void> {
    const fullKey = this.getKey(key);
    const serialized = this.serialize(value);
    await this.pushToList(this.client, fullKey, serialized);

    if (this.ttlSeconds > 0) {
      await this.client.expire(fullKey, this.ttlSeconds);
    }
  }

  async listFromTo(key: string, from: number, to: number = -1): Promise<unknown[]> {
    const fullKey = this.getKey(key);
    const values = await this.getListRange(this.client, fullKey, from, to);
    return values.map(v => this.deserialize(v));
  }

  async delete(key: string): Promise<void> {
    const fullKey = this.getKey(key);
    await this.client.del(fullKey);
  }

  async clear(): Promise<void> {
    const pattern = `${this.keyPrefix}*`;
    let cursor: string | number = '0';

    do {
      const [nextCursor, keys] = await this.scanKeys(this.client, cursor, pattern, 100);

      if (keys.length > 0) {
        await this.client.del(...keys);
      }

      cursor = nextCursor;
    } while (cursor !== '0' && cursor !== 0);
  }

  async increment(key: string): Promise<number> {
    const fullKey = this.getKey(key);
    return this.client.incr(fullKey);
  }

  async appendIndexedLogEntry<T>(
    key: string,
    value: T,
    retention: IndexedLogRetention,
  ): Promise<IndexedLogAppendResult<T>> {
    this.assertIndexedLogRetention(retention);
    const serialized = this.serialize(value);
    const response = await this.evalScript(
      this.client,
      APPEND_INDEXED_LOG_SCRIPT,
      [this.getKey(key)],
      [serialized, String(retention.maxAgeMs), String(retention.maxEntries), crypto.randomUUID()],
    );
    const values = this.assertScriptArray(response, 'append');
    if (values.length !== 4) {
      throw new Error('Redis indexed log append returned an invalid response');
    }
    const logGeneration = values[3];
    if (typeof logGeneration !== 'string' || logGeneration.length === 0) {
      throw new Error('Redis indexed log returned an invalid logGeneration');
    }
    return {
      cursor: this.scriptNumber(values[0], 'cursor'),
      storedAt: this.scriptNumber(values[1], 'storedAt'),
      value: this.deserialize(values[2]) as T,
      logGeneration,
    };
  }

  async readIndexedLogEntries<T>(
    key: string,
    afterCursor: number,
    retention: IndexedLogRetention,
  ): Promise<IndexedLogReadResult<T>> {
    this.assertIndexedLogRetention(retention);
    if (!Number.isSafeInteger(afterCursor)) {
      throw new TypeError('Redis indexed log afterCursor must be a safe integer');
    }
    const response = await this.evalScript(
      this.client,
      READ_INDEXED_LOG_SCRIPT,
      [this.getKey(key)],
      [String(afterCursor), String(retention.maxAgeMs), String(retention.maxEntries), crypto.randomUUID()],
    );
    const values = this.assertScriptArray(response, 'read');
    if (values.length < 3 || (values.length - 3) % 3 !== 0) {
      throw new Error('Redis indexed log read returned an invalid response');
    }

    const entries: IndexedLogEntry<T>[] = [];
    for (let index = 3; index < values.length; index += 3) {
      entries.push({
        cursor: this.scriptNumber(values[index], 'cursor'),
        storedAt: this.scriptNumber(values[index + 1], 'storedAt'),
        value: this.deserialize(values[index + 2]) as T,
      });
    }
    const logGeneration = values[0];
    if (typeof logGeneration !== 'string' || logGeneration.length === 0) {
      throw new Error('Redis indexed log returned an invalid logGeneration');
    }
    return {
      entries,
      logGeneration,
      firstCursor: this.scriptNumber(values[1], 'firstCursor'),
      nextCursor: this.scriptNumber(values[2], 'nextCursor'),
    };
  }

  async deleteIndexedLog(key: string): Promise<void> {
    await this.client.del(this.getKey(key));
  }

  private assertIndexedLogRetention(retention: IndexedLogRetention): void {
    if (!Number.isSafeInteger(retention.maxAgeMs) || retention.maxAgeMs <= 0) {
      throw new TypeError('Redis indexed log maxAgeMs must be a positive safe integer');
    }
    if (!Number.isSafeInteger(retention.maxEntries) || retention.maxEntries <= 0) {
      throw new TypeError('Redis indexed log maxEntries must be a positive safe integer');
    }
  }

  private assertScriptArray(value: unknown, operation: 'append' | 'read'): unknown[] {
    if (!Array.isArray(value)) {
      throw new Error(`Redis indexed log ${operation} returned an invalid response`);
    }
    return value;
  }

  private scriptNumber(value: unknown, field: string): number {
    const parsed = Number(value);
    if (!Number.isSafeInteger(parsed) || parsed < 0) {
      throw new Error(`Redis indexed log returned an invalid ${field}`);
    }
    return parsed;
  }
}

export const upstashPreset: Pick<RedisServerCacheOptions, 'setWithExpiry' | 'scanKeys' | 'evalScript'> = {
  setWithExpiry: (client, key, value, seconds) => client.set(key, value, { ex: seconds } as any),
  scanKeys: (client, cursor, pattern, count) =>
    client.scan(cursor, { match: pattern, count } as any) as Promise<[string | number, string[]]>,
  evalScript: (client, script, keys, arguments_) => client.eval(script, keys, arguments_),
};

// node-redis v4+ exposes Redis multi-word commands as camelCase only
// (lLen / rPush / lRange), not as lowercase aliases. The defaults in this
// module use ioredis-style lowercase, so node-redis users need adapters that
// forward to the camelCase methods. Single-word commands (set, scan, del,
// expire, incr, get) work in lowercase under node-redis and don't need
// adapters; the existing setWithExpiry / scanKeys adapters only exist to
// reshape arguments, not to alias method names.
export const nodeRedisPreset: Pick<
  RedisServerCacheOptions,
  'setWithExpiry' | 'scanKeys' | 'getListLength' | 'pushToList' | 'getListRange' | 'evalScript'
> = {
  setWithExpiry: (client, key, value, seconds) => client.set(key, value, { EX: seconds } as any),
  scanKeys: (client, cursor, pattern, count) =>
    client.scan(cursor, { MATCH: pattern, COUNT: count } as any) as Promise<[string | number, string[]]>,
  getListLength: (client, key) => (client as any).lLen(key),
  pushToList: (client, key, value) => (client as any).rPush(key, value),
  getListRange: (client, key, start, stop) => (client as any).lRange(key, start, stop),
  evalScript: (client, script, keys, arguments_) => (client as any).eval(script, { keys, arguments: arguments_ }),
};
