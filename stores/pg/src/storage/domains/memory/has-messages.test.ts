import type { QueryResult } from 'pg';
import { describe, expect, it, vi } from 'vitest';

import type { DbClient, TxClient } from '../../client';
import { MemoryPG } from './index';

class RecordingDbClient implements DbClient {
  readonly $pool = {} as DbClient['$pool'];
  readonly one = vi.fn(async () => ({ exists: true }));

  connect(): Promise<never> {
    throw new Error('not implemented');
  }

  async none(): Promise<null> {
    throw new Error('not implemented');
  }

  async oneOrNone<T = any>(): Promise<T | null> {
    throw new Error('not implemented');
  }

  async any<T = any>(): Promise<T[]> {
    return [];
  }

  async manyOrNone<T = any>(): Promise<T[]> {
    return [];
  }

  async many<T = any>(): Promise<T[]> {
    return [];
  }

  async query(): Promise<QueryResult> {
    throw new Error('not implemented');
  }

  async tx<T>(_callback: (t: TxClient) => Promise<T>): Promise<T> {
    throw new Error('not implemented');
  }
}

describe('MemoryPG.hasMessages', () => {
  it('uses one SELECT EXISTS query without pagination COUNT amplification', async () => {
    const client = new RecordingDbClient();
    const memory = new MemoryPG({ client, schemaName: 'memory_test' });

    await expect(memory.hasMessages({ threadId: 'thread-1', resourceId: 'resource-1' })).resolves.toBe(true);

    expect(client.one).toHaveBeenCalledTimes(1);
    const [query, params] = client.one.mock.calls[0] ?? [];
    expect(query).toContain('SELECT EXISTS');
    expect(query).not.toContain('COUNT(');
    expect(query).toContain('"memory_test"."mastra_messages"');
    expect(query).toContain('thread_id IN ($1)');
    expect(query).toContain('"resourceId" = $2');
    expect(params).toEqual(['thread-1', 'resource-1']);
  });
});
