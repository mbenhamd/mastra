import { MastraError } from '@mastra/core/error';
import type { QueryResult } from 'pg';
import { describe, expect, it, vi } from 'vitest';
import type { DbClient, QueryValues, TxClient } from '../../client';
import { MemoryPG } from './index';

class DeleteDbClient implements DbClient {
  readonly $pool = {} as DbClient['$pool'];
  readonly none = vi.fn(async (_query: string, _values?: QueryValues): Promise<null> => null);

  connect(): Promise<never> {
    throw new Error('not implemented');
  }

  async one<T = any>(): Promise<T> {
    throw new Error('not implemented');
  }

  async oneOrNone<T = any>(): Promise<T | null> {
    throw new Error('not implemented');
  }

  async any<T = any>(): Promise<T[]> {
    throw new Error('not implemented');
  }

  async manyOrNone<T = any>(): Promise<T[]> {
    throw new Error('not implemented');
  }

  async many<T = any>(): Promise<T[]> {
    throw new Error('not implemented');
  }

  async query(): Promise<QueryResult> {
    throw new Error('not implemented');
  }

  async tx<T>(_callback: (t: TxClient) => Promise<T>): Promise<T> {
    throw new Error('not implemented');
  }
}

function createLogger() {
  return {
    debug: vi.fn(),
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    trackException: vi.fn(),
  };
}

describe('MemoryPG.deleteResource', () => {
  it('deletes only the requested resource row and treats absence as success', async () => {
    const client = new DeleteDbClient();
    const memory = new MemoryPG({ client });

    await memory.deleteResource({ resourceId: 'resource-1' });
    await memory.deleteResource({ resourceId: 'resource-1' });

    expect(client.none).toHaveBeenCalledTimes(2);
    expect(client.none).toHaveBeenNthCalledWith(
      1,
      'DELETE FROM "public"."mastra_resources" WHERE id = $1',
      ['resource-1'],
    );
    expect(client.none).toHaveBeenNthCalledWith(
      2,
      'DELETE FROM "public"."mastra_resources" WHERE id = $1',
      ['resource-1'],
    );
  });

  it('rejects storage failures without retaining driver details', async () => {
    const client = new DeleteDbClient();
    const driverError = new Error('connectionString=SECRET query=DELETE');
    client.none.mockRejectedValueOnce(driverError);
    const memory = new MemoryPG({ client });
    const logger = createLogger();
    memory.__setLogger(logger as any);
    const sensitiveResourceId = 'SECRET_RESOURCE_ID';

    const caught = await memory
      .deleteResource({ resourceId: sensitiveResourceId })
      .catch(error => error as MastraError);

    expect(caught).toBeInstanceOf(MastraError);
    expect(caught.id).toContain('DELETE_RESOURCE');
    expect(caught.cause).not.toBe(driverError);
    const serialized = JSON.stringify(caught.toJSON());
    expect(serialized).not.toContain('connectionString');
    expect(serialized).not.toContain('query=DELETE');
    expect(serialized).not.toContain(sensitiveResourceId);
    expect(logger.error).toHaveBeenCalledTimes(1);
    expect(logger.trackException).toHaveBeenCalledTimes(1);
    expect(JSON.stringify([logger.error.mock.calls, logger.trackException.mock.calls])).not.toContain(
      sensitiveResourceId,
    );
  });
});
