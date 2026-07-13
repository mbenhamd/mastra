import { describe, expect, it } from 'vitest';
import { MockMemory } from './mock';

describe('MockMemory resource deletion', () => {
  it('deletes resource-scoped working memory through the public API', async () => {
    const memory = new MockMemory({ enableWorkingMemory: true });

    await memory.updateWorkingMemory({
      threadId: 'thread-delete-resource',
      resourceId: 'resource-delete',
      workingMemory: 'private working memory',
    });
    await expect(
      memory.getWorkingMemory({
        threadId: 'thread-delete-resource',
        resourceId: 'resource-delete',
      }),
    ).resolves.toBe('private working memory');

    await memory.deleteResource('resource-delete');
    await expect(memory.deleteResource('resource-delete')).resolves.toBeUndefined();
    await expect(
      memory.getWorkingMemory({
        threadId: 'thread-delete-resource',
        resourceId: 'resource-delete',
      }),
    ).resolves.toBeNull();
  });
});
