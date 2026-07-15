import { randomUUID } from 'node:crypto';

import { MastraError } from '@mastra/core/error';
import type { ThreadStateKey, ThreadStateStorage } from '@mastra/core/storage';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';

import { exportSchemas, PostgresStore } from '../..';
import { TEST_CONFIG } from '../../test-utils';

const SCHEMA = `ts_${randomUUID().slice(0, 8)}`;
const KEY: ThreadStateKey = { resourceId: 'resource-1', threadId: 'thread-1', type: 'task' };

function newStore(id: string, schemaName = SCHEMA) {
  return new PostgresStore({ ...TEST_CONFIG, id, schemaName });
}

describe('ThreadStatePG', () => {
  let first: PostgresStore;
  let second: PostgresStore;
  let stateA: ThreadStateStorage;
  let stateB: ThreadStateStorage;

  beforeAll(async () => {
    first = newStore('pg-thread-state-a');
    second = newStore('pg-thread-state-b');
    // Full-store DDL is not catalog-race safe across independent initializers.
    // Initialize sequentially; the test below still exercises concurrent state
    // mutation through the two independent pools.
    await first.init();
    await second.init();
    stateA = first.stores.threadState!;
    stateB = second.stores.threadState!;
  });

  beforeEach(async () => {
    await stateA.dangerouslyClearAll();
  });

  afterAll(async () => {
    await first.db.none(`DROP SCHEMA IF EXISTS "${SCHEMA}" CASCADE`).catch(() => {});
    await Promise.all([first.close().catch(() => {}), second.close().catch(() => {})]);
  });

  it('is wired into PostgresStore and exported schema DDL', () => {
    expect(stateA).toBeDefined();
    const ddl = exportSchemas(SCHEMA);
    expect(ddl).toContain('mastra_thread_state');
    expect(ddl).toContain('PRIMARY KEY ("threadId", "type")');
  });

  it('round-trips JSON and isolates resource, thread, and type', async () => {
    await stateA.setState({ ...KEY, value: [{ id: 'one' }] });

    await expect(stateA.getState(KEY)).resolves.toEqual([{ id: 'one' }]);
    await expect(stateA.getState({ ...KEY, resourceId: 'resource-2' })).resolves.toBeUndefined();
    await expect(stateA.getState({ ...KEY, threadId: 'thread-2' })).resolves.toBeUndefined();
    await expect(stateA.getState({ ...KEY, type: 'goal' })).resolves.toBeUndefined();
  });

  it('survives a fresh store instance over the same database', async () => {
    await stateA.setState({ ...KEY, value: { durable: true } });

    const reopened = newStore('pg-thread-state-reopened');
    try {
      await reopened.init();
      await expect(reopened.stores.threadState!.getState(KEY)).resolves.toEqual({ durable: true });
    } finally {
      await reopened.close();
    }
  });

  it('serializes mutations across independent store instances', async () => {
    const counterKey = { ...KEY, type: 'counter' };
    await stateA.setState({ ...counterKey, value: 0 });

    await Promise.all(
      Array.from({ length: 80 }, (_, index) =>
        (index % 2 === 0 ? stateA : stateB).mutateState<number, void>({
          ...counterKey,
          mutate: current => ({ operation: 'set', value: (current ?? 0) + 1, result: undefined }),
        }),
      ),
    );

    await expect(stateA.getState(counterKey)).resolves.toBe(80);
  });

  it('supports keep/delete mutations, targeted deletion, and clear-all', async () => {
    await stateA.setState({ ...KEY, value: { version: 1 } });
    const kept = await stateA.mutateState<{ version: number }, string>({
      ...KEY,
      mutate: current => ({ operation: 'keep', result: `v${current?.version}` }),
    });
    expect(kept).toBe('v1');

    await stateA.mutateState({ ...KEY, mutate: () => ({ operation: 'delete', result: undefined }) });
    await expect(stateA.getState(KEY)).resolves.toBeUndefined();

    await stateA.setState({ ...KEY, value: 'a' });
    await stateA.setState({ ...KEY, threadId: 'thread-2', value: 'b' });
    await stateA.deleteState(KEY);
    await expect(stateA.getState(KEY)).resolves.toBeUndefined();
    await expect(stateA.getState({ ...KEY, threadId: 'thread-2' })).resolves.toBe('b');

    await stateA.dangerouslyClearAll();
    await expect(stateA.getState({ ...KEY, threadId: 'thread-2' })).resolves.toBeUndefined();
  });

  it('prunes only state older than the configured retention horizon', async () => {
    await stateA.setState({ ...KEY, threadId: 'old-thread', value: 'old' });
    await first.db.none(
      `UPDATE "${SCHEMA}"."mastra_thread_state"
       SET "updatedAtZ" = NOW() - INTERVAL '40 days'
       WHERE "type" = 'task'`,
    );
    await stateA.setState({ ...KEY, threadId: 'new-thread', value: 'new' });

    const result = await stateA.prune({ threadState: { maxAge: '30d' } });

    expect(result).toEqual([
      expect.objectContaining({ domain: 'threadState', table: 'mastra_thread_state', deleted: 1, done: true }),
    ]);
    await expect(stateA.getState({ ...KEY, threadId: 'old-thread' })).resolves.toBeUndefined();
    await expect(stateA.getState({ ...KEY, threadId: 'new-thread' })).resolves.toBe('new');
  });

  it('fails closed without leaking stored state in error details', async () => {
    const failureSchema = `ts_fail_${randomUUID().slice(0, 8)}`;
    const failing = newStore('pg-thread-state-failure', failureSchema);
    const secret = 'do-not-log-this-state-payload';
    try {
      await failing.init();
      await failing.stores.threadState!.setState({ ...KEY, value: { secret } });
      await failing.db.none(`DROP TABLE "${failureSchema}"."mastra_thread_state"`);

      let readError: unknown;
      try {
        await failing.stores.threadState!.getState(KEY);
      } catch (error) {
        readError = error;
      }
      expect(readError).toBeInstanceOf(MastraError);
      expect(readError).toMatchObject({
        details: { resourceId: KEY.resourceId, threadId: KEY.threadId, type: KEY.type },
      });
      expect(JSON.stringify(readError)).not.toContain(secret);

      let writeError: unknown;
      try {
        await failing.stores.threadState!.setState({ ...KEY, value: { secret } });
      } catch (error) {
        writeError = error;
      }
      expect(writeError).toBeInstanceOf(MastraError);
      expect(writeError).toMatchObject({
        details: { resourceId: KEY.resourceId, threadId: KEY.threadId, type: KEY.type },
      });
      expect(JSON.stringify(writeError)).not.toContain(secret);
    } finally {
      await failing.db.none(`DROP SCHEMA IF EXISTS "${failureSchema}" CASCADE`).catch(() => {});
      await failing.close().catch(() => {});
    }
  }, 15_000);

  it('rolls back a failed mutation', async () => {
    await stateA.setState({ ...KEY, value: { version: 1 } });

    let caught: unknown;
    try {
      await stateA.mutateState({
        ...KEY,
        mutate: () => {
          throw new Error('mutation rejected');
        },
      });
    } catch (error) {
      caught = error;
    }

    expect(caught).toBeInstanceOf(MastraError);
    await expect(stateA.getState(KEY)).resolves.toEqual({ version: 1 });
  });
});
