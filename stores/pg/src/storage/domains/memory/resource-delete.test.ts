import { MastraError } from '@mastra/core/error';
import { describe, expect, it, vi } from 'vitest';
import type { DbClient } from '../../client';
import { MemoryPG } from './index';

function createDeleteDbClient() {
  const none = vi.fn(async (): Promise<null> => null);
  const oneOrNone = vi.fn(async (): Promise<{ tablename: string } | null> => null);
  const manyOrNone = vi.fn(async (): Promise<Array<{ id: string }>> => []);
  const txClient = { none, oneOrNone, manyOrNone };
  const tx = vi.fn(async (operation: (client: typeof txClient) => Promise<unknown>) => operation(txClient));
  return { client: { tx } as unknown as DbClient, manyOrNone, none, oneOrNone, tx };
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
    const { client, none } = createDeleteDbClient();
    const memory = new MemoryPG({ client });

    await memory.deleteResource({ resourceId: 'resource-1' });
    await memory.deleteResource({ resourceId: 'resource-1' });

    expect(
      none.mock.calls.filter(([query]) => query === 'DELETE FROM "public"."mastra_resources" WHERE id = $1'),
    ).toEqual([
      ['DELETE FROM "public"."mastra_resources" WHERE id = $1', ['resource-1']],
      ['DELETE FROM "public"."mastra_resources" WHERE id = $1', ['resource-1']],
    ]);
  });

  it('reports erased resource-scoped observational-memory record ids only after commit', async () => {
    const { client, manyOrNone, oneOrNone } = createDeleteDbClient();
    oneOrNone.mockResolvedValue({ tablename: 'mastra_observational_memory' });
    manyOrNone.mockResolvedValue([{ id: 'om-generation-1' }, { id: 'om-generation-2' }]);
    const memory = new MemoryPG({ client });
    const observationalMemoryRecordIds: string[] = [];

    await memory.deleteResource({ resourceId: 'resource-1', observationalMemoryRecordIds });

    expect(manyOrNone).toHaveBeenCalledWith(
      'DELETE FROM "public"."mastra_observational_memory" WHERE "resourceId" = $1 AND "threadId" IS NULL RETURNING id',
      ['resource-1'],
    );
    expect(observationalMemoryRecordIds).toEqual(['om-generation-1', 'om-generation-2']);
  });

  it('rejects storage failures without retaining driver details', async () => {
    const { client, none } = createDeleteDbClient();
    const driverError = Object.assign(new Error('connectionString=SECRET query=DELETE'), { code: '57P01' });
    none.mockRejectedValueOnce(driverError);
    const memory = new MemoryPG({ client });
    const logger = createLogger();
    memory.__setLogger(logger as any);
    const sensitiveResourceId = 'SECRET_RESOURCE_ID';

    const caught = await memory
      .deleteResource({ resourceId: sensitiveResourceId })
      .catch(error => error as MastraError);

    expect(caught).toBeInstanceOf(MastraError);
    expect(caught.id).toContain('DELETE_RESOURCE');
    expect(caught.details?.failureCode).toBe('SQLSTATE_57');
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
