import { MastraError } from '@mastra/core/error';
import { WorkingMemoryValidationError } from '@mastra/core/storage';
import { describe, expect, it, vi } from 'vitest';
import type { DbClient } from '../../client';
import { MemoryPG } from './index';

function createRejectingDbClient(error: Error) {
  const tx = vi.fn(async (): Promise<never> => {
    throw error;
  });
  return { client: { tx } as unknown as DbClient, tx };
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

const timestamp = new Date('2026-01-01T00:00:00.000Z');
const sensitiveResourceId = 'SECRET_RESOURCE_ID';
const writeOperations = [
  {
    operation: 'SAVE_RESOURCE',
    invoke: (memory: MemoryPG) =>
      memory.saveResource({
        resource: {
          id: sensitiveResourceId,
          metadata: {},
          createdAt: timestamp,
          updatedAt: timestamp,
        },
      }),
  },
  {
    operation: 'UPDATE_RESOURCE',
    invoke: (memory: MemoryPG) => memory.updateResource({ resourceId: sensitiveResourceId, metadata: {} }),
  },
] as const;

describe('MemoryPG resource write failures', () => {
  it.each(writeOperations)(
    '$operation rejects driver failures without retaining sensitive details',
    async ({ operation, invoke }) => {
      const driverError = Object.assign(new Error('connectionString=SECRET query=RESOURCE_WRITE'), {
        code: '57P01',
      });
      const { client } = createRejectingDbClient(driverError);
      const memory = new MemoryPG({ client });
      const logger = createLogger();
      memory.__setLogger(logger as any);

      const caught = await invoke(memory).catch(error => error as MastraError);

      expect(caught).toBeInstanceOf(MastraError);
      expect(caught.id).toContain(operation);
      expect(caught.domain).toBe('STORAGE');
      expect(caught.category).toBe('THIRD_PARTY');
      expect(caught.details?.failureCode).toBe('SQLSTATE_57');
      expect(caught.cause).not.toBe(driverError);
      const serialized = JSON.stringify(caught.toJSON());
      expect(serialized).not.toContain('connectionString');
      expect(serialized).not.toContain('query=RESOURCE_WRITE');
      expect(serialized).not.toContain(sensitiveResourceId);
      expect(logger.error).toHaveBeenCalledTimes(1);
      expect(logger.trackException).toHaveBeenCalledTimes(1);
      expect(JSON.stringify([logger.error.mock.calls, logger.trackException.mock.calls])).not.toContain(
        sensitiveResourceId,
      );
    },
  );

  it.each(writeOperations)('$operation preserves WorkingMemoryValidationError identity', async ({ invoke }) => {
    const validationError = new WorkingMemoryValidationError('Revisioned working memory is immutable here.');
    const { client } = createRejectingDbClient(validationError);
    const memory = new MemoryPG({ client });

    await expect(invoke(memory)).rejects.toBe(validationError);
  });
});
