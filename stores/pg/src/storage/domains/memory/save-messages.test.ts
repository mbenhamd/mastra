import type { MastraDBMessage } from '@mastra/core/memory';
import type { QueryResult } from 'pg';
import { describe, expect, it } from 'vitest';
import type { QueryValues, TxClient } from '../../client';
import type { RecordedQuery } from './test-utils';
import { RecordingDbClientBase } from './test-utils';
import { MemoryPG } from './index';

class RecordingTxClient implements TxClient {
  queries: RecordedQuery[] = [];
  reads: RecordedQuery[] = [];
  lifecycleLocks: string[] = [];
  sourceMessages: Record<string, any>[] = [];

  constructor(private readonly threads: Map<string, Record<string, unknown>>) {}

  async none(query: string, values?: QueryValues): Promise<null> {
    if (query.includes('pg_advisory_xact_lock')) {
      this.lifecycleLocks.push(String(values?.[0]));
      return null;
    }
    this.queries.push({ query, values });
    return null;
  }

  async one<T = any>(query: string, values?: QueryValues): Promise<T> {
    if (query.includes('xmin::text')) return { storageGeneration: 'clone-generation' } as T;

    this.queries.push({ query, values });
    if (query.includes('INSERT INTO') && query.includes('mastra_threads') && query.includes('RETURNING *')) {
      return {
        id: values?.[0],
        resourceId: values?.[1],
        title: values?.[2],
        metadata: values?.[3],
        createdAt: values?.[4],
        createdAtZ: values?.[5],
        updatedAt: values?.[6],
        updatedAtZ: values?.[7],
      } as T;
    }
    if (query.includes('INSERT INTO') && query.includes('mastra_resources') && query.includes('RETURNING *')) {
      return {
        id: values?.[0],
        workingMemory: values?.[1],
        metadata: values?.[2],
        createdAt: values?.[3],
        createdAtZ: values?.[4],
        updatedAt: values?.[5],
        updatedAtZ: values?.[6],
      } as T;
    }
    throw new Error('not implemented');
  }

  async oneOrNone<T = any>(query: string, values?: QueryValues): Promise<T | null> {
    this.reads.push({ query, values });
    if (query.includes('SELECT * FROM') && query.includes('mastra_threads') && query.includes('FOR UPDATE')) {
      const thread = this.threads.get(String(values?.[0]));
      return (thread as T | undefined) ?? null;
    }
    if (
      query.includes('SELECT "resourceId", metadata FROM') &&
      query.includes('mastra_threads') &&
      query.includes('FOR UPDATE')
    ) {
      const thread = this.threads.get(String(values?.[0]));
      return thread ? ({ resourceId: thread.resourceId, metadata: thread.metadata ?? {} } as T) : null;
    }
    if (
      query.includes('SELECT "workingMemory", metadata FROM') &&
      query.includes('mastra_resources') &&
      query.includes('FOR UPDATE')
    ) {
      return null;
    }
    if (query.includes('SELECT id FROM') && query.includes('FOR UPDATE')) {
      return { id: String(values?.[0]) } as T;
    }
    throw new Error('not implemented');
  }

  async any<T = any>(): Promise<T[]> {
    throw new Error('not implemented');
  }

  async manyOrNone<T = any>(query: string, values?: QueryValues): Promise<T[]> {
    if (query.includes('SELECT id, "resourceId" FROM') && query.includes('mastra_threads')) {
      return (values ?? [])
        .map(value => this.threads.get(String(value)))
        .filter((thread): thread is Record<string, unknown> => thread !== undefined) as T[];
    }
    return this.sourceMessages as T[];
  }

  async many<T = any>(): Promise<T[]> {
    throw new Error('not implemented');
  }

  async query(): Promise<QueryResult> {
    throw new Error('not implemented');
  }

  async batch<T>(promises: Promise<T>[]): Promise<T[]> {
    return Promise.all(promises);
  }
}

class RecordingDbClient extends RecordingDbClientBase {
  readonly txClient: RecordingTxClient;
  readonly threads = new Map<string, Record<string, unknown>>();

  constructor({
    thread,
    threads,
  }: { thread?: Record<string, unknown> | null; threads?: Record<string, unknown>[] } = {}) {
    super();
    const defaultThread = {
      id: 'thread-1',
      resourceId: 'resource-1',
      title: 'Test thread',
      metadata: {},
      createdAt: new Date('2025-01-01T00:00:00.000Z'),
      updatedAt: new Date('2025-01-01T00:00:00.000Z'),
    };
    const threadsToAdd = threads ?? (thread === undefined ? [defaultThread] : thread ? [thread] : []);
    for (const threadToAdd of threadsToAdd) {
      this.threads.set(String(threadToAdd.id), threadToAdd);
    }
    this.txClient = new RecordingTxClient(this.threads);
  }

  override async none(query: string, values?: QueryValues): Promise<null> {
    this.queries.push({ query, values });
    return null;
  }

  override async oneOrNone<T = any>(_query: string, values?: QueryValues): Promise<T | null> {
    const id = Array.isArray(values) ? values[0] : undefined;
    return id ? ((this.threads.get(String(id)) as T | undefined) ?? null) : null;
  }

  override async manyOrNone<T = any>(query: string): Promise<T[]> {
    if (query?.includes('information_schema.columns')) return [];
    throw new Error('not implemented');
  }

  override async tx<T>(callback: (t: TxClient) => Promise<T>): Promise<T> {
    return callback(this.txClient);
  }
}

let nextMessageId = 1;

function createMessage(overrides: Partial<MastraDBMessage> = {}): MastraDBMessage {
  return {
    id: overrides.id ?? `message-${nextMessageId++}`,
    threadId: overrides.threadId ?? 'thread-1',
    resourceId: overrides.resourceId ?? 'resource-1',
    role: overrides.role ?? 'user',
    type: overrides.type ?? 'v2',
    createdAt: overrides.createdAt ?? new Date('2025-01-01T00:00:00.000Z'),
    content: overrides.content ?? { format: 2, parts: [{ type: 'text', text: 'hello' }] },
  } as MastraDBMessage;
}

describe('MemoryPG.saveMessages', () => {
  it('binds UTC strings for both timestamp column variants', async () => {
    const client = new RecordingDbClient();
    const memory = new MemoryPG({ client });
    const createdAt = new Date('2025-07-01T12:34:56.789Z');

    await memory.saveMessages({ messages: [createMessage({ id: 'message-1', createdAt })] });

    const [insertQuery, threadUpdateQuery] = client.txClient.queries;
    expect(insertQuery!.values![3]).toBe(createdAt.toISOString());
    expect(insertQuery!.values![4]).toBe(createdAt.toISOString());
    expect(threadUpdateQuery!.values![0]).toMatch(/Z$/);
    expect(threadUpdateQuery!.values![1]).toBe(threadUpdateQuery!.values![0]);
  });

  it('inserts multiple messages with one multi-row upsert statement', async () => {
    const client = new RecordingDbClient();
    const memory = new MemoryPG({ client });

    await memory.saveMessages({
      messages: [
        createMessage({ id: 'message-1' }),
        createMessage({ id: 'message-2' }),
        createMessage({ id: 'message-3' }),
      ],
    });

    expect(client.txClient.queries).toHaveLength(2);
    const [insertQuery, threadUpdateQuery] = client.txClient.queries;
    expect(insertQuery!.query).toContain(
      'VALUES ($1, $2, $3, $4, $5, $6, $7, $8), ($9, $10, $11, $12, $13, $14, $15, $16), ($17, $18, $19, $20, $21, $22, $23, $24)',
    );
    expect(insertQuery!.values).toHaveLength(24);
    expect(threadUpdateQuery!.query).toContain('UPDATE "public"."mastra_threads"');
  });

  it('keeps last-write-wins behavior for duplicate message ids in the same batch', async () => {
    const client = new RecordingDbClient();
    const memory = new MemoryPG({ client });
    const firstCreatedAt = new Date('2025-01-01T00:00:00.000Z');
    const secondCreatedAt = new Date('2025-01-01T00:00:01.000Z');

    await memory.saveMessages({
      messages: [
        createMessage({ id: 'message-1', content: { content: 'first' }, createdAt: firstCreatedAt }),
        createMessage({ id: 'message-1', content: { content: 'second' }, createdAt: secondCreatedAt }),
      ],
    });

    const [insertQuery] = client.txClient.queries;
    expect(insertQuery!.query).toContain('VALUES ($1, $2, $3, $4, $5, $6, $7, $8)');
    expect(insertQuery!.query).not.toContain('$9');
    expect(insertQuery!.values).toHaveLength(8);
    expect(insertQuery!.values![2]).toBe(JSON.stringify({ content: 'second' }));
    expect(insertQuery!.values![3]).toBe(firstCreatedAt.toISOString());
    expect(insertQuery!.values![4]).toBe(firstCreatedAt.toISOString());
  });

  it('chunks message inserts under the Postgres bind parameter limit and updates the thread once', async () => {
    const client = new RecordingDbClient();
    const memory = new MemoryPG({ client });
    const messages = Array.from({ length: 8192 }, (_, index) => createMessage({ id: `message-${index}` }));

    await memory.saveMessages({ messages });

    expect(client.txClient.queries).toHaveLength(3);
    const [firstInsertQuery, secondInsertQuery, threadUpdateQuery] = client.txClient.queries;
    expect(firstInsertQuery!.query).toContain('INSERT INTO "public"."mastra_messages"');
    expect(firstInsertQuery!.values).toHaveLength(65528);
    expect(secondInsertQuery!.query).toContain('INSERT INTO "public"."mastra_messages"');
    expect(secondInsertQuery!.values).toHaveLength(8);
    expect(threadUpdateQuery!.query).toContain('UPDATE "public"."mastra_threads"');
    expect(threadUpdateQuery!.values).toHaveLength(3);
  }, 20_000);

  it('returns messages with string content parsed through MessageList', async () => {
    const client = new RecordingDbClient();
    const memory = new MemoryPG({ client });

    const result = await memory.saveMessages({
      messages: [
        createMessage({
          id: 'message-1',
          content: JSON.stringify({ format: 2, parts: [{ type: 'text', text: 'hello' }] }),
        }),
      ],
    });

    expect(result.messages).toHaveLength(1);
    expect(result.messages[0]!.content).toEqual({ format: 2, parts: [{ type: 'text', text: 'hello' }] });
  });

  it('does not start a transaction when a later message is missing required storage fields', async () => {
    const client = new RecordingDbClient();
    const memory = new MemoryPG({ client });

    await expect(
      memory.saveMessages({
        messages: [createMessage({ id: 'message-1' }), createMessage({ id: 'message-2', resourceId: '' })],
      }),
    ).rejects.toThrow("Expected to find a resourceId for message, but couldn't find one");

    expect(client.txClient.queries).toHaveLength(0);
  });

  it('does not start a transaction when a later message is missing a thread id', async () => {
    const client = new RecordingDbClient();
    const memory = new MemoryPG({ client });

    await expect(
      memory.saveMessages({
        messages: [createMessage({ id: 'message-1' }), createMessage({ id: 'message-2', threadId: '' })],
      }),
    ).rejects.toThrow("Expected to find a threadId for message, but couldn't find one");

    expect(client.txClient.queries).toHaveLength(0);
  });

  it('saves mixed-thread batches and updates every touched thread', async () => {
    const client = new RecordingDbClient({
      threads: [
        {
          id: 'thread-1',
          resourceId: 'resource-1',
          title: 'Test thread 1',
          metadata: {},
          createdAt: new Date('2025-01-01T00:00:00.000Z'),
          updatedAt: new Date('2025-01-01T00:00:00.000Z'),
        },
        {
          id: 'thread-2',
          resourceId: 'resource-2',
          title: 'Test thread 2',
          metadata: {},
          createdAt: new Date('2025-01-01T00:00:00.000Z'),
          updatedAt: new Date('2025-01-01T00:00:00.000Z'),
        },
      ],
    });
    const memory = new MemoryPG({ client });

    await memory.saveMessages({
      messages: [
        createMessage({ id: 'message-1', threadId: 'thread-1' }),
        createMessage({ id: 'message-2', threadId: 'thread-2', resourceId: 'resource-2' }),
      ],
    });

    expect(client.txClient.queries).toHaveLength(3);
    const [insertQuery, firstThreadUpdate, secondThreadUpdate] = client.txClient.queries;
    expect(insertQuery!.query).toContain('INSERT INTO "public"."mastra_messages"');
    expect(firstThreadUpdate!.values![2]).toBe('thread-1');
    expect(secondThreadUpdate!.values![2]).toBe('thread-2');
  });

  it('rejects messages for any missing thread before opening a transaction', async () => {
    const client = new RecordingDbClient();
    const memory = new MemoryPG({ client });

    await expect(
      memory.saveMessages({
        messages: [createMessage({ id: 'message-1' }), createMessage({ id: 'message-2', threadId: 'thread-2' })],
      }),
    ).rejects.toThrow('Thread thread-2 not found');
    expect(client.txClient.queries).toHaveLength(0);
  });

  it('rejects messages for a missing thread before opening a transaction', async () => {
    const client = new RecordingDbClient({ thread: null });
    const memory = new MemoryPG({ client });

    await expect(memory.saveMessages({ messages: [createMessage({ id: 'message-1' })] })).rejects.toThrow(
      'Thread thread-1 not found',
    );
    expect(client.txClient.queries).toHaveLength(0);
  });
});

describe('MemoryPG.saveThread', () => {
  it('binds UTC strings for both timestamp column variants', async () => {
    const client = new RecordingDbClient();
    const memory = new MemoryPG({ client });
    const createdAt = new Date('2025-07-01T12:34:56.789Z');
    const updatedAt = new Date('2025-07-02T01:02:03.456Z');

    await memory.saveThread({
      thread: {
        id: 'thread-1',
        resourceId: 'resource-1',
        title: 'Test thread',
        metadata: {},
        createdAt,
        updatedAt,
      },
    });

    expect(client.txClient.reads).toHaveLength(1);
    expect(client.txClient.reads[0]!.query).toContain('SELECT "resourceId", metadata FROM');
    expect(client.txClient.reads[0]!.query).toContain('FOR UPDATE');
    expect(client.txClient.queries).toHaveLength(1);
    expect(client.txClient.queries[0]!.values).toEqual([
      'thread-1',
      'resource-1',
      'Test thread',
      '{}',
      createdAt.toISOString(),
      createdAt.toISOString(),
      updatedAt.toISOString(),
      updatedAt.toISOString(),
    ]);
  });
});

describe('MemoryPG.saveResource', () => {
  it('binds UTC strings for both timestamp column variants', async () => {
    const client = new RecordingDbClient();
    const memory = new MemoryPG({ client });
    const createdAt = new Date('2025-07-01T12:34:56.789Z');
    const updatedAt = new Date('2025-07-02T01:02:03.456Z');

    await memory.saveResource({
      resource: {
        id: 'resource-1',
        workingMemory: 'Test memory',
        metadata: {},
        createdAt,
        updatedAt,
      },
    });

    expect(client.txClient.reads).toHaveLength(1);
    expect(client.txClient.reads[0]!.query).toContain('SELECT "workingMemory", metadata FROM');
    expect(client.txClient.reads[0]!.query).toContain('FOR UPDATE');
    expect(client.txClient.queries).toHaveLength(1);
    expect(client.txClient.queries[0]!.values![3]).toBe(createdAt.toISOString());
    expect(client.txClient.queries[0]!.values![4]).toBe(createdAt.toISOString());
    expect(client.txClient.queries[0]!.values![5]).toBe(updatedAt.toISOString());
    expect(client.txClient.queries[0]!.values![6]).toBe(updatedAt.toISOString());
  });
});

describe('MemoryPG.cloneThread', () => {
  it('prefers createdAtZ and binds the UTC string to both timestamp columns', async () => {
    const client = new RecordingDbClient();
    const memory = new MemoryPG({ client });
    const legacyCreatedAt = new Date('2025-07-01T07:34:56.789Z');
    const createdAtZ = new Date('2025-07-01T12:34:56.789Z');
    client.txClient.sourceMessages.push({
      id: 'message-1',
      threadId: 'thread-1',
      resourceId: 'resource-1',
      role: 'user',
      type: 'v2',
      content: JSON.stringify({ format: 2, parts: [{ type: 'text', text: 'hello' }] }),
      createdAt: legacyCreatedAt,
      createdAtZ,
    });

    const result = await memory.cloneThread({ sourceThreadId: 'thread-1', newThreadId: 'thread-2' });

    const messageInsert = client.txClient.queries[1]!;
    expect(messageInsert.query).toContain('mastra_messages');
    expect(messageInsert.values![3]).toBe(createdAtZ.toISOString());
    expect(messageInsert.values![4]).toBe(createdAtZ.toISOString());
    expect(messageInsert.values![3]).not.toBe(legacyCreatedAt.toISOString());
    expect(result.clonedMessages[0]!.createdAt).toEqual(createdAtZ);
  });

  it('locks source and destination before returning the in-transaction source owner', async () => {
    const client = new RecordingDbClient();
    const memory = new MemoryPG({ client });

    const result = await memory.cloneThread({ sourceThreadId: 'thread-1', newThreadId: 'aaa-clone' });

    expect(client.txClient.lifecycleLocks).toEqual(['mastra:thread-clone:aaa-clone', 'mastra:thread-clone:thread-1']);
    expect(client.txClient.reads.map(read => read.values)).toEqual([['aaa-clone'], ['thread-1']]);
    expect(client.txClient.reads.every(read => read.query.includes('SELECT * FROM'))).toBe(true);
    expect(client.txClient.reads.every(read => read.query.includes('FOR UPDATE'))).toBe(true);
    expect(result.sourceResourceId).toBe('resource-1');
    expect(result.thread.resourceId).toBe('resource-1');
  });

  it('preserves source-not-found precedence when the requested clone id already exists', async () => {
    const client = new RecordingDbClient({
      threads: [
        {
          id: 'existing-clone',
          resourceId: 'target-resource',
          title: 'Existing clone',
          metadata: {},
          createdAt: new Date('2025-01-01T00:00:00.000Z'),
          updatedAt: new Date('2025-01-01T00:00:00.000Z'),
        },
      ],
    });
    const memory = new MemoryPG({ client });

    await expect(
      memory.cloneThread({ sourceThreadId: 'missing-source', newThreadId: 'existing-clone' }),
    ).rejects.toThrow('Source thread with id missing-source not found');
  });
});
