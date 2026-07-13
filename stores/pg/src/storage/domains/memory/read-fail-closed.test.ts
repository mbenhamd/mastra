import { getErrorFromUnknown, MastraError } from '@mastra/core/error';
import type { QueryResult } from 'pg';
import { describe, expect, it, vi } from 'vitest';
import { RoutingDbClient } from '../../client';
import type { DbClient, TxClient } from '../../client';
import { MemoryPG } from './index';

class ReadDbClient implements DbClient {
  readonly $pool = {} as DbClient['$pool'];
  private readonly failure?: Error;
  private readonly oneResults: unknown[];
  private readonly manyResults: unknown[][];

  constructor({
    failure,
    oneResults = [],
    manyResults = [],
  }: {
    failure?: Error;
    oneResults?: unknown[];
    manyResults?: unknown[][];
  } = {}) {
    this.failure = failure;
    this.oneResults = [...oneResults];
    this.manyResults = [...manyResults];
  }

  connect(): Promise<never> {
    throw new Error('not implemented');
  }

  async none(): Promise<null> {
    throw new Error('not implemented');
  }

  async one<T = any>(): Promise<T> {
    if (this.failure) throw this.failure;
    return this.oneResults.shift() as T;
  }

  async oneOrNone<T = any>(): Promise<T | null> {
    throw new Error('not implemented');
  }

  async any<T = any>(): Promise<T[]> {
    return this.manyOrNone<T>();
  }

  async manyOrNone<T = any>(): Promise<T[]> {
    if (this.failure) throw this.failure;
    return (this.manyResults.shift() ?? []) as T[];
  }

  async many<T = any>(): Promise<T[]> {
    return this.manyOrNone<T>();
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

const createdAt = new Date('2026-01-01T00:00:00.000Z');
const messageRow = {
  id: 'message-1',
  content: JSON.stringify({ format: 2, parts: [{ type: 'text', text: 'hello' }] }),
  role: 'assistant',
  type: 'v2',
  createdAt,
  createdAtZ: createdAt,
  threadId: 'thread-1',
  resourceId: 'resource-1',
};
const threadRow = {
  id: 'thread-1',
  resourceId: 'resource-1',
  title: 'Thread',
  metadata: {},
  createdAt,
  createdAtZ: createdAt,
  updatedAt: createdAt,
  updatedAtZ: createdAt,
};

describe('MemoryPG list read failures', () => {
  const sensitiveId = 'SECRET_SQL_PARAMETER';
  const sensitiveCauseText = 'database rejected connectionString=SECRET_CONNECTION_STRING query=$1';

  const failureOperations = [
    {
      operation: 'LIST_THREADS',
      invoke: (memory: MemoryPG) => memory.listThreads({ filter: { resourceId: sensitiveId } }),
    },
    {
      operation: 'LIST_MESSAGES_BY_ID',
      invoke: (memory: MemoryPG) => memory.listMessagesById({ messageIds: [sensitiveId] }),
    },
    {
      operation: 'LIST_MESSAGES',
      invoke: (memory: MemoryPG) => memory.listMessages({ threadId: sensitiveId, resourceId: sensitiveId }),
    },
    {
      operation: 'LIST_MESSAGES_BY_RESOURCE_ID',
      invoke: (memory: MemoryPG) => memory.listMessagesByResourceId({ resourceId: sensitiveId }),
    },
  ];
  const failureCases = failureOperations.flatMap(failureOperation =>
    (['direct', 'routed'] as const).map(clientMode => ({ ...failureOperation, clientMode })),
  );

  it.each(failureCases)(
    '$operation propagates a safe typed error through the $clientMode client without retaining the driver error',
    async ({ operation, invoke, clientMode }) => {
      const cause = new Error(sensitiveCauseText);
      const directClient = new ReadDbClient({ failure: cause });
      const client = clientMode === 'direct' ? directClient : new RoutingDbClient(directClient);
      const memory = new MemoryPG({ client });
      const logger = createLogger();
      memory.__setLogger(logger as any);

      let caught: unknown;
      try {
        await invoke(memory);
      } catch (error) {
        caught = error;
      }

      expect(caught).toBeInstanceOf(MastraError);
      const mastraError = caught as MastraError;
      expect(mastraError.id).toContain(operation);
      expect(mastraError.domain).toBe('STORAGE');
      expect(mastraError.category).toBe('THIRD_PARTY');
      expect(mastraError.cause).not.toBe(cause);
      expect(mastraError.cause?.message).not.toContain(sensitiveCauseText);

      const publicError = JSON.stringify(mastraError.toJSON());
      expect(publicError).not.toContain(sensitiveId);
      expect(publicError).not.toContain(sensitiveCauseText);
      expect(publicError).not.toContain('connectionString');
      expect(publicError).not.toContain('query=$1');

      expect(logger.error).toHaveBeenCalledTimes(1);
      expect(logger.trackException).toHaveBeenCalledTimes(1);
      const logged = JSON.stringify([logger.error.mock.calls, logger.trackException.mock.calls]);
      expect(logged).not.toContain(sensitiveId);
      expect(logged).not.toContain(sensitiveCauseText);
      expect(logged).not.toContain('connectionString');

      // Core normalization mutates a plain Error by adding `toJSON`. The storage error must remain safe even
      // if another consumer normalizes the original driver error after this method throws.
      getErrorFromUnknown(cause);
      const afterCauseNormalization = JSON.stringify(mastraError.toJSON());
      expect(afterCauseNormalization).not.toContain(sensitiveCauseText);
      expect(afterCauseNormalization).not.toContain('connectionString');
      expect(afterCauseNormalization).not.toContain('query=$1');
    },
  );

  it('does not serialize a driver error that already exposes toJSON', async () => {
    const cause = Object.assign(new Error(sensitiveCauseText), {
      toJSON: () => ({ message: sensitiveCauseText, connectionString: 'SECRET_CONNECTION_STRING' }),
    });
    const memory = new MemoryPG({ client: new ReadDbClient({ failure: cause }) });

    const caught = await memory.listMessages({ threadId: sensitiveId }).catch(error => error as MastraError);
    expect(caught).toBeInstanceOf(MastraError);
    const serialized = JSON.stringify(caught.toJSON());
    expect(serialized).not.toContain(sensitiveCauseText);
    expect(serialized).not.toContain('SECRET_CONNECTION_STRING');
    expect(caught.cause).not.toBe(cause);
  });

  it.each([
    {
      name: 'invalid thread identifier',
      invoke: (memory: MemoryPG) => memory.listMessages({ threadId: [sensitiveId, ''], page: 0 }),
    },
    {
      name: 'invalid thread page',
      invoke: (memory: MemoryPG) => memory.listMessages({ threadId: sensitiveId, page: -1 }),
    },
    {
      name: 'invalid resource page',
      invoke: (memory: MemoryPG) => memory.listMessagesByResourceId({ resourceId: sensitiveId, page: -1 }),
    },
    {
      name: 'non-string resource identifier',
      invoke: (memory: MemoryPG) =>
        memory.listMessagesByResourceId({ resourceId: { value: sensitiveId } as unknown as string }),
    },
  ])('does not serialize identifiers for $name through direct or routed clients', async ({ invoke }) => {
    for (const client of [new ReadDbClient(), new RoutingDbClient(new ReadDbClient())]) {
      const memory = new MemoryPG({ client });
      const logger = createLogger();
      memory.__setLogger(logger as any);

      const caught = await invoke(memory).catch(error => error as MastraError);

      expect(caught).toBeInstanceOf(MastraError);
      const serialized = JSON.stringify({
        message: caught.message,
        cause: caught.cause,
        details: caught.details,
        publicError: caught.toJSON(),
        logs: [logger.error.mock.calls, logger.trackException.mock.calls],
      });
      expect(serialized).not.toContain(sensitiveId);
    }
  });

  it('returns empty thread data only after a successful empty query', async () => {
    const memory = new MemoryPG({ client: new ReadDbClient({ oneResults: [{ count: '0' }] }) });

    await expect(memory.listThreads({})).resolves.toEqual({
      threads: [],
      total: 0,
      page: 0,
      perPage: 100,
      hasMore: false,
    });
  });

  it('returns empty message data only after successful empty queries', async () => {
    await expect(
      new MemoryPG({ client: new ReadDbClient({ manyResults: [[]] }) }).listMessagesById({
        messageIds: ['missing'],
      }),
    ).resolves.toEqual({ messages: [] });

    await expect(
      new MemoryPG({ client: new ReadDbClient({ oneResults: [{ count: '0' }] }) }).listMessages({
        threadId: 'missing',
      }),
    ).resolves.toEqual({ messages: [], total: 0, page: 0, perPage: 40, hasMore: false });

    await expect(
      new MemoryPG({ client: new ReadDbClient({ oneResults: [{ count: '0' }] }) }).listMessagesByResourceId({
        resourceId: 'missing',
      }),
    ).resolves.toEqual({ messages: [], total: 0, page: 0, perPage: 40, hasMore: false });
  });

  it('preserves successful non-empty thread and message results', async () => {
    const threads = await new MemoryPG({
      client: new ReadDbClient({ oneResults: [{ count: '1' }], manyResults: [[threadRow]] }),
    }).listThreads({});
    expect(threads.threads).toHaveLength(1);
    expect(threads.total).toBe(1);

    const byId = await new MemoryPG({
      client: new ReadDbClient({ manyResults: [[messageRow]] }),
    }).listMessagesById({ messageIds: ['message-1'] });
    expect(byId.messages).toHaveLength(1);

    const byThread = await new MemoryPG({
      client: new ReadDbClient({ oneResults: [{ count: '1' }], manyResults: [[messageRow]] }),
    }).listMessages({ threadId: 'thread-1' });
    expect(byThread.messages).toHaveLength(1);
    expect(byThread.total).toBe(1);

    const byResource = await new MemoryPG({
      client: new ReadDbClient({ oneResults: [{ count: '1' }], manyResults: [[messageRow]] }),
    }).listMessagesByResourceId({ resourceId: 'resource-1' });
    expect(byResource.messages).toHaveLength(1);
    expect(byResource.total).toBe(1);
  });
});
