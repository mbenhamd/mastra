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
  it('isolates observational-memory records at every public ingress and egress boundary', async () => {
    const db = new InMemoryDB();
    const memory = new InMemoryMemory({ db });
    const config = {
      nested: { enabled: true },
      serializedAt: '2026-01-01T00:00:00.000Z',
      resolveModel: () => 'configured-model',
    };
    const initialized = await memory.initializeObservationalMemory({
      threadId: 'thread-boundary',
      resourceId: 'resource-boundary',
      scope: 'thread',
      config,
    });
    const initializedCreatedAt = initialized.createdAt.getTime();
    config.nested.enabled = false;

    expect(Object.isFrozen(initialized)).toBe(true);
    expect(Object.isFrozen(initialized.config)).toBe(true);
    expect(Object.isFrozen(config)).toBe(false);
    expect(() => {
      initialized.activeObservations = 'caller mutation';
    }).toThrow(TypeError);
    initialized.createdAt.setTime(0);

    const fetched = await memory.getObservationalMemory('thread-boundary', 'resource-boundary');
    if (!fetched) throw new Error('Expected initialized observational memory.');
    expect(fetched.activeObservations).toBe('');
    expect(fetched.createdAt.getTime()).toBe(initializedCreatedAt);
    expect(fetched.config.nested).toEqual({ enabled: true });
    expect(fetched.config.serializedAt).toBe('2026-01-01T00:00:00.000Z');
    expect((fetched.config.resolveModel as () => string)()).toBe('configured-model');
    expect(() => {
      (fetched.config.nested as { enabled: boolean }).enabled = false;
    }).toThrow(TypeError);
    const fetchedUpdatedAt = fetched.updatedAt.getTime();
    fetched.updatedAt.setTime(0);
    await expect(memory.getObservationalMemory('thread-boundary', 'resource-boundary')).resolves.toMatchObject({
      updatedAt: new Date(fetchedUpdatedAt),
    });

    const history = await memory.getObservationalMemoryHistory('thread-boundary', 'resource-boundary');
    expect(history).toHaveLength(1);
    expect(Object.isFrozen(history[0])).toBe(true);
    expect(() => {
      history[0]!.activeObservations = 'history mutation';
    }).toThrow(TypeError);
    history[0]!.updatedAt.setTime(1);
    await expect(memory.getObservationalMemory('thread-boundary', 'resource-boundary')).resolves.toMatchObject({
      updatedAt: new Date(fetchedUpdatedAt),
    });

    const inserted: ObservationalMemoryRecord = {
      ...fetched,
      id: 'caller-owned-record',
      generationCount: 1,
      activeObservations: 'stored observations',
      config: { nested: { enabled: true } },
      createdAt: new Date('2026-01-02T00:00:00.000Z'),
      updatedAt: new Date('2026-01-02T00:00:00.000Z'),
    };
    await memory.insertObservationalMemoryRecord(inserted);
    inserted.activeObservations = 'mutated after insert';
    (inserted.config.nested as { enabled: boolean }).enabled = false;
    inserted.createdAt.setTime(0);

    await expect(memory.getObservationalMemory('thread-boundary', 'resource-boundary')).resolves.toMatchObject({
      id: 'caller-owned-record',
      activeObservations: 'stored observations',
      config: { nested: { enabled: true } },
      createdAt: new Date('2026-01-02T00:00:00.000Z'),
    });
  });

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

  it('does not let a returned observational-memory alias bypass a clone rollback receipt', async () => {
    const memory = await createMemory();
    const clone = await seedClone(memory, 'alias-clone');
    const clonedRecord = await memory.initializeObservationalMemory({
      threadId: clone.thread.id,
      resourceId: clone.thread.resourceId,
      scope: 'thread',
      config: {},
    });
    await memory.clearObservationalMemory(clone.thread.id, clone.thread.resourceId);
    const omReceipt = await memory.insertObservationalMemoryRecord(clonedRecord);
    const fetched = await memory.getObservationalMemory(clone.thread.id, clone.thread.resourceId);
    if (!fetched) throw new Error('Expected cloned observational memory.');

    expect(() => {
      fetched.activeObservations = 'mutation outside the storage API';
    }).toThrow(TypeError);
    await expect(memory.getObservationalMemory(clone.thread.id, clone.thread.resourceId)).resolves.toMatchObject({
      activeObservations: '',
    });

    await expect(
      memory.rollbackThreadClone({
        thread: clone.rollbackReceipt,
        observationalMemory: omReceipt,
        unverifiedObservationalMemoryRecordId: clonedRecord.id,
      }),
    ).resolves.toEqual({ status: 'rolled_back' });
    await expect(memory.getThreadById({ threadId: clone.thread.id })).resolves.toBeNull();
    await expect(memory.getObservationalMemory(clone.thread.id, clone.thread.resourceId)).resolves.toBeNull();
  });

  it('clears private clone generations with the rest of InMemoryDB', async () => {
    const db = new InMemoryDB();
    const memory = new InMemoryMemory({ db });
    const clone = await seedClone(memory, 'clear-clone');
    const record = await memory.initializeObservationalMemory({
      threadId: clone.thread.id,
      resourceId: clone.thread.resourceId,
      scope: 'thread',
      config: {},
    });

    expect(db.memoryThreadGenerations.has(clone.thread.id)).toBe(true);
    expect(db.memoryObservationalGenerations.has(record.id)).toBe(true);

    db.clear();

    expect(db.memoryThreadGenerations.size).toBe(0);
    expect(db.memoryObservationalGenerations.size).toBe(0);
  });
});
