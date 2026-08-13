import type { MastraDBMessage } from '@mastra/core/memory';
import type { ObservationalMemoryRecord, StorageCloneThreadOutput } from '@mastra/core/storage';
import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { PostgresStore } from '../..';
import { connectionString } from '../../test-utils';
import type { MemoryPG } from '.';

describe('MemoryPG conditional clone rollback', () => {
  const schemaName = `clone_rollback_${Date.now()}`;
  let firstStore: PostgresStore;
  let secondStore: PostgresStore;
  let firstMemory: MemoryPG;
  let secondMemory: MemoryPG;

  beforeAll(async () => {
    firstStore = new PostgresStore({ id: 'clone-rollback-first', connectionString, schemaName });
    await firstStore.init();
    secondStore = new PostgresStore({
      id: 'clone-rollback-second',
      connectionString,
      disableInit: true,
      schemaName,
    });
    await secondStore.init();
    firstMemory = (await firstStore.getStore('memory')) as MemoryPG;
    secondMemory = (await secondStore.getStore('memory')) as MemoryPG;
  });

  afterAll(async () => {
    await Promise.allSettled([firstStore?.close(), secondStore?.close()]);
    const cleanup = new Pool({ connectionString });
    try {
      await cleanup.query(`DROP SCHEMA IF EXISTS "${schemaName}" CASCADE`);
    } finally {
      await cleanup.end();
    }
  });

  async function seedClone(suffix: string, targetResourceId = `target-resource-${suffix}`) {
    const sourceThreadId = `source-thread-${suffix}`;
    const cloneThreadId = `clone-thread-${suffix}`;
    const createdAt = new Date('2026-01-01T00:00:00.000Z');
    await firstMemory.saveThread({
      thread: {
        id: sourceThreadId,
        resourceId: `source-resource-${suffix}`,
        title: 'Source',
        metadata: {},
        createdAt,
        updatedAt: createdAt,
      },
    });
    await firstMemory.saveMessages({
      messages: [
        {
          id: `source-message-${suffix}`,
          threadId: sourceThreadId,
          resourceId: `source-resource-${suffix}`,
          role: 'user',
          content: { format: 2, parts: [{ type: 'text', text: 'Source message' }] },
          createdAt,
        } satisfies MastraDBMessage,
      ],
    });
    const clone = await firstMemory.cloneThread({
      sourceThreadId,
      newThreadId: cloneThreadId,
      resourceId: targetResourceId,
    });
    if (!clone.rollbackReceipt) throw new Error('Expected clone rollback receipt.');
    return clone as StorageCloneThreadOutput & {
      rollbackReceipt: NonNullable<StorageCloneThreadOutput['rollbackReceipt']>;
    };
  }

  it('deletes only the inserted OM generation and preserves prior target-resource OM', async () => {
    const targetResourceId = 'target-resource-prior-om';
    const priorRecord = await firstMemory.initializeObservationalMemory({
      threadId: null,
      resourceId: targetResourceId,
      scope: 'resource',
      config: {},
    });
    const clone = await seedClone('prior-om', targetResourceId);
    const clonedRecord: ObservationalMemoryRecord = {
      ...priorRecord,
      id: 'cloned-om-prior-om',
      activeObservations: 'Cloned observations',
      generationCount: priorRecord.generationCount + 1,
      createdAt: new Date('2026-01-02T00:00:00.000Z'),
      updatedAt: new Date('2026-01-02T00:00:00.000Z'),
    };
    const omReceipt = await firstMemory.insertObservationalMemoryRecord(clonedRecord);

    await expect(
      firstMemory.rollbackThreadClone({
        thread: clone.rollbackReceipt,
        observationalMemory: omReceipt,
        unverifiedObservationalMemoryRecordId: clonedRecord.id,
      }),
    ).resolves.toEqual({ status: 'rolled_back' });

    await expect(firstMemory.getThreadById({ threadId: clone.thread.id })).resolves.toBeNull();
    const history = await firstMemory.getObservationalMemoryHistory(null, targetResourceId);
    expect(history.map(record => record.id)).toEqual([priorRecord.id]);
  });

  it('preserves a clone modified through another store instance', async () => {
    const clone = await seedClone('modified');
    await secondMemory.updateThread({ id: clone.thread.id, title: 'Concurrent title' });

    await expect(firstMemory.rollbackThreadClone({ thread: clone.rollbackReceipt })).resolves.toEqual({
      status: 'conflict',
      reason: 'thread',
    });
    await expect(firstMemory.getThreadById({ threadId: clone.thread.id })).resolves.toMatchObject({
      title: 'Concurrent title',
    });
  });

  it('does not delete a replacement thread with the same id', async () => {
    const clone = await seedClone('recreated');
    await secondMemory.deleteThread({ threadId: clone.thread.id });
    await secondMemory.saveThread({
      thread: {
        ...clone.thread,
        title: 'Replacement thread',
        metadata: { replacement: true },
        updatedAt: new Date('2026-01-03T00:00:00.000Z'),
      },
    });

    await expect(firstMemory.rollbackThreadClone({ thread: clone.rollbackReceipt })).resolves.toEqual({
      status: 'conflict',
      reason: 'thread',
    });
    await expect(firstMemory.getThreadById({ threadId: clone.thread.id })).resolves.toMatchObject({
      title: 'Replacement thread',
      metadata: { replacement: true },
    });
  });
});
