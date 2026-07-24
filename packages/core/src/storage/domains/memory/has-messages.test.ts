import { describe, expect, it, vi } from 'vitest';

import { InMemoryStore } from '../../mock';

describe('MemoryStorage.hasMessages', () => {
  it('keeps custom adapters compatible through the bounded list fallback', async () => {
    const storage = new InMemoryStore({ id: 'memory-has-messages-fallback' });
    const memory = await storage.getStore('memory');
    if (!memory) throw new Error('Expected in-memory storage domain.');
    const listMessages = vi.spyOn(memory, 'listMessages');
    await memory.saveMessages({
      messages: [
        {
          id: 'message-1',
          threadId: 'thread-1',
          resourceId: 'resource-1',
          role: 'user',
          type: 'v2',
          content: { format: 2, parts: [{ type: 'text', text: 'hello' }] },
          createdAt: new Date('2026-01-01T00:00:00.000Z'),
        },
      ],
    });

    await expect(memory.hasMessages({ threadId: 'thread-1', resourceId: 'resource-1' })).resolves.toBe(true);
    await expect(memory.hasMessages({ threadId: 'thread-1', resourceId: 'resource-2' })).resolves.toBe(false);

    expect(listMessages).toHaveBeenNthCalledWith(1, {
      threadId: 'thread-1',
      resourceId: 'resource-1',
      perPage: 1,
    });
    expect(listMessages).toHaveBeenNthCalledWith(2, {
      threadId: 'thread-1',
      resourceId: 'resource-2',
      perPage: 1,
    });
  });
});
