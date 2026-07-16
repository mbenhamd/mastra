import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { Client } from '@libsql/client';
import { createClient } from '@libsql/client';
import { TABLE_THREAD_STATE } from '@mastra/core/storage';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { ThreadStateLibSQL } from './index';

const TEST_DB_URL = 'file::memory:?cache=shared';
const RESOURCE_ID = 'resource-1';

const createTestClient = () => createClient({ url: TEST_DB_URL });

interface Task {
  id: string;
  content: string;
  status: 'pending' | 'in_progress' | 'completed';
  activeForm: string;
}

const tasks = (): Task[] => [
  { id: 't1', content: 'First', status: 'pending', activeForm: 'Doing first' },
  { id: 't2', content: 'Second', status: 'in_progress', activeForm: 'Doing second' },
];

describe('ThreadStateLibSQL', () => {
  let client: Client;
  let store: ThreadStateLibSQL;

  beforeEach(async () => {
    client = createTestClient();
    store = new ThreadStateLibSQL({ client, maxRetries: 1, initialBackoffMs: 10 });
    await store.init();
    await store.dangerouslyClearAll();
  });

  afterEach(() => {
    client.close();
  });

  it('returns undefined for an unset (threadId, type)', async () => {
    expect(await store.getState({ resourceId: RESOURCE_ID, threadId: 'thread-1', type: 'task' })).toBeUndefined();
  });

  it('round-trips a JSON value', async () => {
    await store.setState({ resourceId: RESOURCE_ID, threadId: 'thread-1', type: 'task', value: tasks() });
    expect(await store.getState({ resourceId: RESOURCE_ID, threadId: 'thread-1', type: 'task' })).toEqual(tasks());
  });

  it('replaces the value on a subsequent set (upsert)', async () => {
    await store.setState({ resourceId: RESOURCE_ID, threadId: 'thread-1', type: 'task', value: tasks() });
    const next: Task[] = [{ id: 't3', content: 'Third', status: 'completed', activeForm: 'Done third' }];
    await store.setState({ resourceId: RESOURCE_ID, threadId: 'thread-1', type: 'task', value: next });
    expect(await store.getState({ resourceId: RESOURCE_ID, threadId: 'thread-1', type: 'task' })).toEqual(next);
  });

  it('scopes state per thread and per type', async () => {
    await store.setState({ resourceId: RESOURCE_ID, threadId: 'thread-1', type: 'task', value: tasks() });
    await store.setState({ resourceId: RESOURCE_ID, threadId: 'thread-1', type: 'goal', value: { objective: 'ship' } });
    expect(await store.getState({ resourceId: RESOURCE_ID, threadId: 'thread-2', type: 'task' })).toBeUndefined();
    expect(await store.getState({ resourceId: RESOURCE_ID, threadId: 'thread-1', type: 'goal' })).toEqual({
      objective: 'ship',
    });
  });

  it('scopes state per resource', async () => {
    await store.setState({ resourceId: RESOURCE_ID, threadId: 'thread-1', type: 'task', value: tasks() });
    expect(await store.getState({ resourceId: 'resource-2', threadId: 'thread-1', type: 'task' })).toBeUndefined();
  });

  // Rows written by @mastra/libsql <= 1.16.0 key on the bare threadId, while
  // every read here binds encodeThreadStateScope. There is deliberately no
  // fallback read: serving a legacy row would have to guess which resource
  // owned it, and guessing wrong hands one resource's state to another. The
  // changeset declares this a breaking change and requires draining or
  // migrating thread state before upgrading, so a miss is the specified
  // outcome rather than an oversight. This test exists because the break is
  // otherwise invisible -- the row is still there, the query just cannot
  // address it.
  it('does not serve a legacy row keyed on the bare threadId', async () => {
    await client.execute({
      sql: `INSERT INTO "${TABLE_THREAD_STATE}" ("threadId", "type", "value", "createdAt", "updatedAt")
            VALUES (?, ?, ?, ?, ?)`,
      args: ['thread-1', 'task', JSON.stringify(tasks()), new Date().toISOString(), new Date().toISOString()],
    });

    expect(await store.getState({ resourceId: RESOURCE_ID, threadId: 'thread-1', type: 'task' })).toBeUndefined();

    // The legacy row is orphaned, not overwritten: a scoped write lands beside
    // it under a different primary key, so both rows coexist until retention
    // prunes the old one.
    await store.setState({ resourceId: RESOURCE_ID, threadId: 'thread-1', type: 'task', value: tasks() });
    const rows = await client.execute({
      sql: `SELECT "threadId" FROM "${TABLE_THREAD_STATE}" WHERE "type" = ? ORDER BY "threadId"`,
      args: ['task'],
    });
    expect(rows.rows).toHaveLength(2);
    expect(rows.rows.map(row => row.threadId)).toContain('thread-1');
  });

  it('deletes a single (threadId, type)', async () => {
    await store.setState({ resourceId: RESOURCE_ID, threadId: 'thread-1', type: 'task', value: tasks() });
    await store.deleteState({ resourceId: RESOURCE_ID, threadId: 'thread-1', type: 'task' });
    expect(await store.getState({ resourceId: RESOURCE_ID, threadId: 'thread-1', type: 'task' })).toBeUndefined();
  });

  it('persists across store instances over the same database (durability)', async () => {
    await store.setState({ resourceId: RESOURCE_ID, threadId: 'thread-1', type: 'task', value: tasks() });

    // A fresh store instance over the same DB (simulating a process restart)
    // sees the persisted value.
    const reopened = new ThreadStateLibSQL({ client, maxRetries: 1, initialBackoffMs: 10 });
    await reopened.init();
    expect(await reopened.getState({ resourceId: RESOURCE_ID, threadId: 'thread-1', type: 'task' })).toEqual(tasks());
  });

  it('serializes mutations across independent clients targeting one database', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'mastra-thread-state-'));
    const url = `file:${join(directory, 'state.db')}`;
    const firstClient = createClient({ url, timeout: 0 });
    const secondClient = createClient({ url, timeout: 0 });
    try {
      const firstStore = new ThreadStateLibSQL({ client: firstClient, maxRetries: 20, initialBackoffMs: 1 });
      const secondStore = new ThreadStateLibSQL({ client: secondClient, maxRetries: 20, initialBackoffMs: 1 });
      await firstStore.init();
      await secondStore.init();

      const key = { resourceId: RESOURCE_ID, threadId: 'thread-1', type: 'counter' };
      await firstStore.setState({ ...key, value: 0 });
      await Promise.all(
        Array.from({ length: 40 }, (_, index) =>
          (index % 2 === 0 ? firstStore : secondStore).mutateState<number, void>({
            ...key,
            mutate: current => ({ operation: 'set', value: (current ?? 0) + 1, result: undefined }),
          }),
        ),
      );

      await expect(firstStore.getState(key)).resolves.toBe(40);
    } finally {
      firstClient.close();
      secondClient.close();
      await rm(directory, { recursive: true, force: true });
    }
  }, 15_000);

  it('rolls back a throwing mutation and leaves the prior value intact', async () => {
    const key = { resourceId: RESOURCE_ID, threadId: 'thread-1', type: 'counter' };
    await store.setState({ ...key, value: 1 });

    await expect(
      store.mutateState({
        ...key,
        mutate: () => {
          throw new Error('mutation rejected');
        },
      }),
    ).rejects.toThrow('mutation rejected');
    await expect(store.getState(key)).resolves.toBe(1);
  });
});
