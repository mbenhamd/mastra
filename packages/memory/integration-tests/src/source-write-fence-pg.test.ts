import { randomUUID } from 'node:crypto';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { MockLanguageModelV2 } from '@internal/ai-sdk-v5/test';
import { Memory } from '@mastra/memory';
import { PostgresStore } from '@mastra/pg';
import { $ } from 'execa';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

const __dirname = fileURLToPath(import.meta.url);
const connectionString = process.env.DB_URL || 'postgres://postgres:password@localhost:5434/mastra';

/**
 * PF-4233: a legitimate execution-derived thread write that carries managed
 * working memory must still succeed after an ordinary reflection advanced the
 * OM generation. The captured source-write guard names the record that was
 * active when the execution started; reflection archives (but retains) it, so
 * it stays a valid source fence. Only erasure (retraction) revokes it.
 */
describe('source-write fence with managed working memory on PostgreSQL', () => {
  const schemaName = `pf4233_wm_${Date.now()}`;
  let store: PostgresStore;

  beforeAll(async () => {
    if (!process.env.DB_URL) {
      await $({ cwd: join(__dirname, '..'), stdio: 'inherit', detached: true })`docker compose up -d postgres --wait`;
    }
    store = new PostgresStore({ id: 'pf4233-wm', connectionString, schemaName });
    await store.init();
  });

  afterAll(async () => {
    await store?.db.none(`DROP SCHEMA IF EXISTS "${schemaName}" CASCADE`).catch(() => undefined);
    await store?.close();
  });

  const createMemory = (workingMemoryScope: 'thread' | 'resource') =>
    new Memory({
      storage: store,
      options: {
        lastMessages: false,
        workingMemory: { enabled: true, scope: workingMemoryScope },
        observationalMemory: {
          scope: 'thread',
          sourceWriteFencing: 'required',
          model: new MockLanguageModelV2({}) as never,
        },
      },
    });

  it.each(['thread', 'resource'] as const)(
    'accepts a guarded %s working-memory thread update after reflection advances the generation',
    async workingMemoryScope => {
      const memory = createMemory(workingMemoryScope);
      const memoryStore = (await store.getStore('memory'))!;
      const threadId = `thread-${randomUUID()}`;
      const resourceId = `resource-${randomUUID()}`;

      const guard = (await memory.prepareObservationalMemorySourceWriteGuard(threadId, resourceId))!;
      expect(guard).toMatchObject({ threadId, resourceId });
      await memory.saveThread({
        thread: { id: threadId, resourceId, title: '', metadata: {}, createdAt: new Date(), updatedAt: new Date() },
        observationalMemorySourceWriteGuard: guard,
      });

      // An ordinary reflection archives the captured record and activates a new generation.
      const captured = (await memoryStore.getObservationalMemory(threadId, resourceId))!;
      expect(captured.id).toBe(guard.recordId);
      await memoryStore.createReflectionGeneration({ currentRecord: captured, reflection: 'reflected', tokenCount: 1 });
      expect((await memoryStore.getObservationalMemory(threadId, resourceId))!.id).not.toBe(guard.recordId);

      // A late, legitimate title/metadata write of the same execution carries managed working memory.
      await memory.updateThread({
        id: threadId,
        title: 'generated title',
        metadata: { workingMemory: '# Notes\n- still valid' },
        observationalMemorySourceWriteGuard: guard,
      });

      await expect(memory.getThreadById({ threadId })).resolves.toMatchObject({ title: 'generated title' });
      await expect(memory.getWorkingMemory({ threadId, resourceId })).resolves.toContain('still valid');
    },
  );

  it('still rejects the same write once the captured scope was erased', async () => {
    const memory = createMemory('thread');
    const memoryStore = (await store.getStore('memory'))!;
    const threadId = `thread-${randomUUID()}`;
    const resourceId = `resource-${randomUUID()}`;

    const guard = (await memory.prepareObservationalMemorySourceWriteGuard(threadId, resourceId))!;
    await memory.saveThread({
      thread: { id: threadId, resourceId, title: '', metadata: {}, createdAt: new Date(), updatedAt: new Date() },
      observationalMemorySourceWriteGuard: guard,
    });
    await memoryStore.retractObservationalMemory({ resourceId, threadId });

    await expect(
      memory.updateThread({
        id: threadId,
        title: 'erased title',
        metadata: { workingMemory: '# Notes\n- must not land' },
        observationalMemorySourceWriteGuard: guard,
      }),
    ).rejects.toThrow();
    await expect(memory.getThreadById({ threadId })).resolves.toMatchObject({ title: '' });
  });
});
