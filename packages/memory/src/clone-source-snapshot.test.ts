import { InMemoryStore } from '@mastra/core/storage';
import type { MemoryStorage } from '@mastra/core/storage';
import { describe, expect, it, vi } from 'vitest';

import { Memory } from './index';

function deferred(): { promise: Promise<void>; resolve: () => void } {
  let resolve!: () => void;
  const promise = new Promise<void>(complete => {
    resolve = complete;
  });
  return { promise, resolve };
}

async function getMemoryStore(memory: Memory): Promise<MemoryStorage> {
  const store = await memory.storage.getStore('memory');
  if (!store) throw new Error('Expected memory storage.');
  return store;
}

async function seedResourceState(
  memory: Memory,
  memoryStore: MemoryStorage,
  resourceId: string,
  label: string,
): Promise<void> {
  await memory.updateWorkingMemoryByOwner({
    resourceId,
    workingMemory: JSON.stringify({ source: label }),
    expectedRevision: 0,
  });
  const record = await memoryStore.initializeObservationalMemory({
    threadId: null,
    resourceId,
    scope: 'resource',
    config: {},
  });
  await memoryStore.updateActiveObservations({
    id: record.id,
    observations: `${label} observations`,
    tokenCount: 2,
    lastObservedAt: new Date('2026-01-01T00:00:00.000Z'),
  });
}

describe('Memory clone source ownership snapshots', () => {
  it('copies OM and Working Memory from the ownership generation cloned by storage', async () => {
    const memory = new Memory({
      storage: new InMemoryStore(),
      options: { workingMemory: { enabled: true, scope: 'resource' } },
    });
    const memoryStore = await getMemoryStore(memory);
    const sourceThread = await memory.createThread({
      threadId: 'source-thread',
      resourceId: 'resource-a',
    });
    await seedResourceState(memory, memoryStore, 'resource-a', 'resource-a');
    await seedResourceState(memory, memoryStore, 'resource-b', 'resource-b');

    const cloneEntered = deferred();
    const continueClone = deferred();
    const cloneThread = memoryStore.cloneThread.bind(memoryStore);
    vi.spyOn(memoryStore, 'cloneThread').mockImplementation(async args => {
      cloneEntered.resolve();
      await continueClone.promise;
      return cloneThread(args);
    });

    const clonePromise = memory.cloneThread({
      sourceThreadId: sourceThread.id,
      newThreadId: 'cloned-thread',
      resourceId: 'target-resource',
    });
    await cloneEntered.promise;
    try {
      await memoryStore.saveThread({
        thread: {
          ...sourceThread,
          resourceId: 'resource-b',
          updatedAt: new Date('2026-01-02T00:00:00.000Z'),
        },
      });
    } finally {
      continueClone.resolve();
    }

    const clone = await clonePromise;
    expect(clone.sourceResourceId).toBe('resource-b');
    expect(clone.thread.resourceId).toBe('target-resource');
    await expect(memoryStore.getObservationalMemory(null, 'target-resource')).resolves.toMatchObject({
      activeObservations: 'resource-b observations',
    });
    await expect(
      memory.getWorkingMemorySnapshot({
        threadId: clone.thread.id,
        resourceId: clone.thread.resourceId,
      }),
    ).resolves.toMatchObject({ value: JSON.stringify({ source: 'resource-b' }) });
  });

  it('fails before cloning when post-clone memory could be copied without a source snapshot', async () => {
    const memory = new Memory({ storage: new InMemoryStore() });
    const memoryStore = await getMemoryStore(memory);
    await memory.createThread({ threadId: 'unsafe-source', resourceId: 'source-resource' });
    Object.defineProperty(memoryStore, 'supportsThreadCloneSourceSnapshot', { value: false });
    const cloneThread = vi.spyOn(memoryStore, 'cloneThread');

    await expect(
      memory.cloneThread({ sourceThreadId: 'unsafe-source', newThreadId: 'must-not-exist' }),
    ).rejects.toThrow('cannot atomically snapshot source ownership');

    expect(cloneThread).not.toHaveBeenCalled();
    await expect(memoryStore.getThreadById({ threadId: 'must-not-exist' })).resolves.toBeNull();
  });

  it('keeps message-only cloning available on adapters without source snapshots', async () => {
    const memory = new Memory({ storage: new InMemoryStore() });
    const memoryStore = await getMemoryStore(memory);
    await memory.createThread({ threadId: 'message-only-source', resourceId: 'source-resource' });
    Object.defineProperty(memoryStore, 'supportsThreadCloneSourceSnapshot', { value: false });
    Object.defineProperty(memoryStore, 'supportsObservationalMemory', { value: false });

    const clone = await memory.cloneThread({
      sourceThreadId: 'message-only-source',
      newThreadId: 'message-only-clone',
    });

    expect(clone.thread.id).toBe('message-only-clone');
    await expect(memoryStore.getThreadById({ threadId: clone.thread.id })).resolves.toMatchObject({
      resourceId: 'source-resource',
    });
  });

  it('rejects and conditionally rolls back a clone when an advertised snapshot is missing', async () => {
    const memory = new Memory({ storage: new InMemoryStore() });
    const memoryStore = await getMemoryStore(memory);
    await memory.createThread({ threadId: 'invalid-output-source', resourceId: 'source-resource' });
    const cloneThread = memoryStore.cloneThread.bind(memoryStore);
    vi.spyOn(memoryStore, 'cloneThread').mockImplementation(async args => {
      const result = await cloneThread(args);
      delete result.sourceResourceId;
      return result;
    });

    await expect(
      memory.cloneThread({ sourceThreadId: 'invalid-output-source', newThreadId: 'invalid-output-clone' }),
    ).rejects.toThrow('cloneThread did not return sourceResourceId');

    await expect(memoryStore.getThreadById({ threadId: 'invalid-output-clone' })).resolves.toBeNull();
  });
});
