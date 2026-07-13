import { randomUUID } from 'node:crypto';
import type { MastraDBMessage } from '@mastra/core/memory';
import type { MastraStorage } from '@mastra/core/storage';
import { describe, expect, it } from 'vitest';

/**
 * Opt-in conformance coverage for storage adapters that implement
 * MemoryStorage.deleteResource.
 */
export function createResourceDeletionTest({ storage }: { storage: MastraStorage }) {
  describe('Resource deletion', () => {
    it('deletes only the requested resource record and is idempotent', async () => {
      await storage.init();
      const memory = (await storage.getStore('memory'))!;
      const marker = `delete-resource-${randomUUID()}`;
      const resourceId = `${marker}-target`;
      const survivorId = `${marker}-survivor`;
      const threadId = `${marker}-thread`;
      const messageId = `${marker}-message`;
      const createdAt = new Date();

      try {
        await memory.saveResource({
          resource: {
            id: resourceId,
            workingMemory: 'private working memory',
            metadata: { marker },
            createdAt,
            updatedAt: createdAt,
          },
        });
        await memory.saveResource({
          resource: {
            id: survivorId,
            workingMemory: 'surviving working memory',
            metadata: { marker },
            createdAt,
            updatedAt: createdAt,
          },
        });
        await memory.saveThread({
          thread: {
            id: threadId,
            resourceId,
            title: 'Preserved thread',
            metadata: { marker, workingMemory: 'preserved thread metadata' },
            createdAt,
            updatedAt: createdAt,
          },
        });
        await memory.saveMessages({
          messages: [
            {
              id: messageId,
              threadId,
              resourceId,
              role: 'user',
              content: { format: 2, parts: [{ type: 'text', text: 'Preserved message' }] },
              createdAt,
            } satisfies MastraDBMessage,
          ],
        });

        await memory.deleteResource({ resourceId });
        await expect(memory.deleteResource({ resourceId })).resolves.toBeUndefined();

        await expect(memory.getResourceById({ resourceId })).resolves.toBeNull();
        await expect(memory.getResourceById({ resourceId: survivorId })).resolves.toMatchObject({
          id: survivorId,
          workingMemory: 'surviving working memory',
        });
        await expect(memory.getThreadById({ threadId })).resolves.toMatchObject({
          id: threadId,
          resourceId,
          metadata: { marker, workingMemory: 'preserved thread metadata' },
        });
        await expect(memory.listMessagesById({ messageIds: [messageId] })).resolves.toMatchObject({
          messages: [expect.objectContaining({ id: messageId, resourceId })],
        });
      } finally {
        await memory.deleteThread({ threadId });
        await memory.deleteResource({ resourceId: survivorId });
        await storage.close?.();
      }
    });
  });
}
