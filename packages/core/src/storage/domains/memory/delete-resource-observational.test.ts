import { describe, expect, it } from 'vitest';

import { InMemoryStore } from '../../mock';

describe('MemoryStorage.deleteResource observational memory erasure', () => {
  it('erases the resource-scoped record and preserves thread-scoped records', async () => {
    const storage = new InMemoryStore({ id: 'memory-delete-resource-om' });
    const memory = await storage.getStore('memory');
    if (!memory) throw new Error('Expected in-memory storage domain.');
    const store = memory as unknown as {
      initializeObservationalMemory: (input: {
        threadId: string | null;
        resourceId: string;
        scope: 'thread' | 'resource';
      }) => Promise<unknown>;
      getObservationalMemory: (threadId: string | null, resourceId: string) => Promise<unknown>;
      deleteResource: (input: { resourceId: string; observationalMemoryRecordIds?: string[] }) => Promise<void>;
      saveResource: (input: { resource: unknown }) => Promise<unknown>;
    };

    await store.saveResource({
      resource: {
        id: 'resource-1',
        workingMemory: '{"name":"Tyler"}',
        metadata: {},
        createdAt: new Date('2026-01-01T00:00:00.000Z'),
        updatedAt: new Date('2026-01-01T00:00:00.000Z'),
      },
    });
    await store.initializeObservationalMemory({ threadId: null, resourceId: 'resource-1', scope: 'resource' });
    await store.initializeObservationalMemory({ threadId: 'thread-1', resourceId: 'resource-1', scope: 'thread' });

    const observationalMemoryRecordIds: string[] = [];
    await store.deleteResource({ resourceId: 'resource-1', observationalMemoryRecordIds });

    // Resource erasure clears the resource-scoped observational record; the
    // thread-scoped record stays with its (preserved) thread.
    await expect(store.getObservationalMemory(null, 'resource-1')).resolves.toBeNull();
    await expect(store.getObservationalMemory('thread-1', 'resource-1')).resolves.not.toBeNull();
    expect(observationalMemoryRecordIds).toHaveLength(1);
  });
});
