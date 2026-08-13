import { describe, expect, it } from 'vitest';

import type { MastraDBMessage } from '../../../memory';
import type { ObservationalMemoryRecord, StorageCloneThreadOutput } from '../../types';

import { InMemoryDB } from '../inmemory-db';
import { InMemoryMemory } from './inmemory';

async function createMemory() {
  return new InMemoryMemory({ db: new InMemoryDB() });
}

async function seedClone(memory: Awaited<ReturnType<typeof createMemory>>, newThreadId: string) {
  const createdAt = new Date('2026-01-01T00:00:00.000Z');
  await memory.saveThread({
    thread: {
      id: 'source-thread',
      resourceId: 'source-resource',
      title: 'Source',
      metadata: {},
      createdAt,
      updatedAt: createdAt,
    },
  });
  await memory.saveMessages({
    messages: [
      {
        id: 'source-message',
        threadId: 'source-thread',
        resourceId: 'source-resource',
        role: 'user',
        content: { format: 2, parts: [{ type: 'text', text: 'Source message' }] },
        createdAt,
      } satisfies MastraDBMessage,
    ],
  });
  const clone = await memory.cloneThread({
    sourceThreadId: 'source-thread',
    newThreadId,
    resourceId: 'target-resource',
  });
  if (!clone.rollbackReceipt) throw new Error('Expected clone rollback receipt.');
  return clone as StorageCloneThreadOutput & {
    rollbackReceipt: NonNullable<StorageCloneThreadOutput['rollbackReceipt']>;
  };
}

describe('InMemoryMemory conditional clone rollback', () => {
  it('removes only the inserted OM generation and preserves prior target-resource OM', async () => {
    const memory = await createMemory();
    const priorRecord = await memory.initializeObservationalMemory({
      threadId: null,
      resourceId: 'target-resource',
      scope: 'resource',
      config: {},
    });
    const clone = await seedClone(memory, 'failed-clone');
    const clonedRecord: ObservationalMemoryRecord = {
      ...priorRecord,
      id: 'cloned-om-record',
      activeObservations: 'Cloned observations',
      generationCount: priorRecord.generationCount + 1,
      createdAt: new Date('2026-01-02T00:00:00.000Z'),
      updatedAt: new Date('2026-01-02T00:00:00.000Z'),
    };
    const omReceipt = await memory.insertObservationalMemoryRecord(clonedRecord);
    if (!omReceipt) throw new Error('Expected observational-memory insertion receipt.');

    await expect(
      memory.rollbackThreadClone({
        thread: clone.rollbackReceipt,
        observationalMemory: omReceipt,
        unverifiedObservationalMemoryRecordId: clonedRecord.id,
      }),
    ).resolves.toEqual({ status: 'rolled_back' });

    await expect(memory.getThreadById({ threadId: clone.thread.id })).resolves.toBeNull();
    await expect(memory.listMessages({ threadId: clone.thread.id })).resolves.toMatchObject({ messages: [] });
    await expect(memory.getObservationalMemoryHistory(null, 'target-resource')).resolves.toEqual([priorRecord]);
  });

  it('preserves a clone that was modified after its rollback receipt was issued', async () => {
    const memory = await createMemory();
    const clone = await seedClone(memory, 'modified-clone');
    await memory.updateThread({ id: clone.thread.id, title: 'Concurrent title' });

    await expect(memory.rollbackThreadClone({ thread: clone.rollbackReceipt })).resolves.toEqual({
      status: 'conflict',
      reason: 'thread',
    });
    await expect(memory.getThreadById({ threadId: clone.thread.id })).resolves.toMatchObject({
      title: 'Concurrent title',
    });
  });

  it('does not delete a replacement thread with the same id', async () => {
    const memory = await createMemory();
    const clone = await seedClone(memory, 'recreated-clone');
    await memory.deleteThread({ threadId: clone.thread.id });
    await memory.saveThread({
      thread: {
        ...clone.thread,
        title: 'Replacement thread',
        metadata: { replacement: true },
        updatedAt: new Date('2026-01-03T00:00:00.000Z'),
      },
    });

    await expect(memory.rollbackThreadClone({ thread: clone.rollbackReceipt })).resolves.toEqual({
      status: 'conflict',
      reason: 'thread',
    });
    await expect(memory.getThreadById({ threadId: clone.thread.id })).resolves.toMatchObject({
      title: 'Replacement thread',
      metadata: { replacement: true },
    });
  });

  it('preserves every clone artifact when the inserted OM generation changed', async () => {
    const memory = await createMemory();
    const clone = await seedClone(memory, 'om-modified-clone');
    const clonedRecord = await memory.initializeObservationalMemory({
      threadId: clone.thread.id,
      resourceId: clone.thread.resourceId,
      scope: 'thread',
      config: {},
    });
    await memory.clearObservationalMemory(clone.thread.id, clone.thread.resourceId);
    const omReceipt = await memory.insertObservationalMemoryRecord(clonedRecord);
    if (!omReceipt) throw new Error('Expected observational-memory insertion receipt.');
    await memory.updateActiveObservations({
      id: clonedRecord.id,
      observations: 'Concurrent observations',
      tokenCount: 2,
      lastObservedAt: new Date('2026-01-04T00:00:00.000Z'),
    });

    await expect(
      memory.rollbackThreadClone({
        thread: clone.rollbackReceipt,
        observationalMemory: omReceipt,
        unverifiedObservationalMemoryRecordId: clonedRecord.id,
      }),
    ).resolves.toEqual({ status: 'conflict', reason: 'observational_memory' });
    await expect(memory.getThreadById({ threadId: clone.thread.id })).resolves.not.toBeNull();
    await expect(memory.getObservationalMemory(clone.thread.id, clone.thread.resourceId)).resolves.toMatchObject({
      id: clonedRecord.id,
      activeObservations: 'Concurrent observations',
    });
  });
});
