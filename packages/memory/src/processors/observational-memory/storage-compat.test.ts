import type { MemoryStorage } from '@mastra/core/storage';
import { describe, expect, it, vi } from 'vitest';

import { updateResourceFromObservationalMemory, updateThreadFromObservationalMemory } from './storage-compat';

describe('observational-memory storage compatibility', () => {
  it('falls back to established storage methods on older Core peers', async () => {
    const resource = {
      id: 'resource-1',
      workingMemory: 'preserved',
      metadata: {},
      createdAt: new Date(),
      updatedAt: new Date(),
    };
    const thread = {
      id: 'thread-1',
      resourceId: 'resource-1',
      title: 'Updated title',
      metadata: { preserved: true },
      createdAt: new Date(),
      updatedAt: new Date(),
    };
    const updateResource = vi.fn().mockResolvedValue(resource);
    const updateThread = vi.fn().mockResolvedValue(thread);
    const storage = { updateResource, updateThread } as unknown as MemoryStorage;

    await expect(
      updateResourceFromObservationalMemory(storage, {
        resourceId: 'resource-1',
        workingMemory: 'preserved',
        guard: { recordId: 'generation-1', resourceId: 'resource-1', threadId: null },
      }),
    ).resolves.toBe(resource);
    await expect(
      updateThreadFromObservationalMemory(storage, {
        id: 'thread-1',
        title: 'Updated title',
        metadata: { preserved: true },
        guard: { recordId: 'generation-1', resourceId: 'resource-1', threadId: 'thread-1' },
      }),
    ).resolves.toBe(thread);

    expect(updateResource).toHaveBeenCalledWith({
      resourceId: 'resource-1',
      workingMemory: 'preserved',
    });
    expect(updateThread).toHaveBeenCalledWith({
      id: 'thread-1',
      title: 'Updated title',
      metadata: { preserved: true },
    });
  });
});
