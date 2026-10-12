import { describe, expect, it } from 'vitest';

import { InMemoryStore } from '../../mock';
import { HarnessStorageLeaseConflictError } from '../harness';
import type { SessionRecord } from '../harness';

describe('MemoryStorage atomic observational-memory retraction', () => {
  it('refuses a lease-fenced deletion by an owner whose Harness session was taken over (PF-5373)', async () => {
    const storage = new InMemoryStore({ id: 'memory-lease-fenced-deletion' });
    const memory = await storage.getStore('memory');
    const harness = await storage.getStore('harness');
    if (!memory || !harness) throw new Error('Expected in-memory storage domains.');
    expect(memory.supportsSessionLeaseFencedMessageDeletion).toBe(true);
    const createdAt = new Date('2026-01-01T00:00:00.000Z');
    const resourceId = 'lease-fence-resource';

    // Owner A held the session's lease; it lapsed and owner B took the session
    // over and captured its source-write guard on the thread's OM record.
    const setup = async (name: string) => {
      const threadId = `lease-fence-thread-${name}`;
      const sessionId = `lease-fence-session-${name}`;
      await harness.createOrLoadActiveSession(sessionRecord(sessionId, resourceId, threadId), {
        initialLease: { ownerId: 'owner-a', ttlMs: 1 },
      });
      await new Promise(resolve => setTimeout(resolve, 5));
      await harness.acquireSessionLease({ sessionId, ownerId: 'owner-b', ttlMs: 60_000 });
      await memory.saveThread({
        thread: { id: threadId, resourceId, title: 'lease fence', metadata: {}, createdAt, updatedAt: createdAt },
      });
      await memory.saveMessages({
        messages: [
          {
            id: `${threadId}-pruned`,
            threadId,
            resourceId,
            role: 'user',
            content: { format: 2, parts: [{ type: 'text', text: 'prune me' }] },
            createdAt,
          },
        ],
      });
      const record = await memory.initializeObservationalMemory({ threadId, resourceId, scope: 'thread', config: {} });
      const ownerBGuard = { recordId: record.id, threadId, resourceId };
      const ownerBWrite = () =>
        memory.saveMessages({
          messages: [
            {
              id: `${threadId}-owner-b`,
              threadId,
              resourceId,
              role: 'user',
              content: { format: 2, parts: [{ type: 'text', text: 'owner b turn' }] },
              createdAt,
            },
          ],
          observationalMemorySourceWriteGuard: ownerBGuard,
        });
      return { threadId, sessionId, prunedId: `${threadId}-pruned`, ownerBWrite };
    };

    // Reproduction: without the fence the evicted owner's delete applies and
    // retracts the record, and the new owner's guarded write then conflicts.
    const unfenced = await setup('unfenced');
    await memory.deleteMessages([unfenced.prunedId], { retractObservationalMemory: true });
    await expect(unfenced.ownerBWrite()).rejects.toThrow(/source write guard/i);

    // With the fence the evicted owner is refused and nothing changes.
    const fenced = await setup('fenced');
    const fence = { harnessName: 'default', sessionId: fenced.sessionId };
    await expect(
      memory.deleteMessages([fenced.prunedId], {
        retractObservationalMemory: true,
        sessionLeaseFence: { ...fence, ownerId: 'owner-a' },
      }),
    ).rejects.toBeInstanceOf(HarnessStorageLeaseConflictError);
    await expect(memory.listMessagesById({ messageIds: [fenced.prunedId] })).resolves.toMatchObject({
      messages: [expect.objectContaining({ id: fenced.prunedId })],
    });
    await expect(fenced.ownerBWrite()).resolves.toBeDefined();
    // The fence covers only the session's own thread: a batch that names a
    // message of another thread is refused whole.
    const foreignId = `${unfenced.threadId}-foreign`;
    await memory.saveMessages({
      messages: [
        {
          id: foreignId,
          threadId: unfenced.threadId,
          resourceId,
          role: 'user',
          content: { format: 2, parts: [{ type: 'text', text: 'another thread' }] },
          createdAt,
        },
      ],
    });
    await expect(
      memory.deleteMessages([fenced.prunedId, foreignId], {
        retractObservationalMemory: true,
        sessionLeaseFence: { ...fence, ownerId: 'owner-b' },
      }),
    ).rejects.toThrow(/not in the thread of the fenced session/);
    await expect(memory.listMessagesById({ messageIds: [fenced.prunedId, foreignId] })).resolves.toMatchObject({
      messages: [expect.anything(), expect.anything()],
    });
    // The owner holding the lease deletes as before.
    await memory.deleteMessages([fenced.prunedId], {
      retractObservationalMemory: true,
      sessionLeaseFence: { ...fence, ownerId: 'owner-b' },
    });
    await expect(memory.listMessagesById({ messageIds: [fenced.prunedId] })).resolves.toEqual({ messages: [] });
  });

  it('accepts a captured source guard after reflection archives that record', async () => {
    const storage = new InMemoryStore({ id: 'memory-observational-source-guard-reflection' });
    const memory = await storage.getStore('memory');
    if (!memory) throw new Error('Expected in-memory storage domain.');
    const createdAt = new Date('2026-01-01T00:00:00.000Z');
    const threadId = 'source-guard-thread';
    const resourceId = 'source-guard-resource';
    await memory.saveThread({
      thread: { id: threadId, resourceId, title: 'source guard', metadata: {}, createdAt, updatedAt: createdAt },
    });
    const record = await memory.initializeObservationalMemory({
      threadId,
      resourceId,
      scope: 'thread',
      config: {},
    });
    const sourceWriteGuard = { recordId: record.id, threadId, resourceId };

    await memory.createReflectionGeneration({ currentRecord: record, reflection: 'reflected', tokenCount: 1 });
    await memory.saveMessages({
      messages: [
        {
          id: 'delayed-source-message',
          threadId,
          resourceId,
          role: 'user',
          content: { format: 2, parts: [{ type: 'text', text: 'delayed source write' }] },
          createdAt,
        },
      ],
      observationalMemorySourceWriteGuard: sourceWriteGuard,
    });

    await expect(memory.listMessages({ threadId, resourceId })).resolves.toMatchObject({
      messages: [expect.objectContaining({ id: 'delayed-source-message' })],
    });
  });

  it('rejects captured source guards after their thread moves to another resource', async () => {
    const storage = new InMemoryStore({ id: 'memory-observational-source-guard-thread-owner' });
    const memory = await storage.getStore('memory');
    if (!memory) throw new Error('Expected in-memory storage domain.');
    const createdAt = new Date('2026-01-01T00:00:00.000Z');
    const originalResourceId = 'source-guard-original-resource';
    const nextResourceId = 'source-guard-next-resource';
    const threadGuardId = 'source-guard-thread-owner';
    const resourceGuardThreadId = 'source-guard-resource-owner';

    await memory.saveThread({
      thread: {
        id: threadGuardId,
        resourceId: originalResourceId,
        title: 'thread-scoped source guard',
        metadata: {},
        createdAt,
        updatedAt: createdAt,
      },
    });
    await memory.saveThread({
      thread: {
        id: resourceGuardThreadId,
        resourceId: originalResourceId,
        title: 'resource-scoped source guard',
        metadata: {},
        createdAt,
        updatedAt: createdAt,
      },
    });

    const threadRecord = await memory.initializeObservationalMemory({
      threadId: threadGuardId,
      resourceId: originalResourceId,
      scope: 'thread',
      config: {},
    });
    const resourceRecord = await memory.initializeObservationalMemory({
      threadId: null,
      resourceId: originalResourceId,
      scope: 'resource',
      config: {},
    });

    await memory.updateThreadResourceId({ threadId: threadGuardId, resourceId: nextResourceId });
    await memory.updateThreadResourceId({ threadId: resourceGuardThreadId, resourceId: nextResourceId });

    await expect(
      memory.saveMessages({
        messages: [
          {
            id: 'stale-thread-owner-message',
            threadId: threadGuardId,
            resourceId: originalResourceId,
            role: 'user',
            content: { format: 2, parts: [{ type: 'text', text: 'must not cross thread owner' }] },
            createdAt,
          },
        ],
        observationalMemorySourceWriteGuard: {
          recordId: threadRecord.id,
          threadId: threadGuardId,
          resourceId: originalResourceId,
        },
      }),
    ).rejects.toThrow('source write guard');

    await expect(
      memory.saveMessages({
        messages: [
          {
            id: 'stale-resource-owner-message',
            threadId: resourceGuardThreadId,
            resourceId: originalResourceId,
            role: 'user',
            content: { format: 2, parts: [{ type: 'text', text: 'must not cross resource owner' }] },
            createdAt,
          },
        ],
        observationalMemorySourceWriteGuard: {
          recordId: resourceRecord.id,
          threadId: null,
          resourceId: originalResourceId,
        },
      }),
    ).rejects.toThrow('source write guard');

    await expect(
      memory.listMessages({ threadId: threadGuardId, resourceId: originalResourceId }),
    ).resolves.toMatchObject({
      messages: [],
    });
    await expect(
      memory.listMessages({ threadId: resourceGuardThreadId, resourceId: originalResourceId }),
    ).resolves.toMatchObject({ messages: [] });
  });

  it('rejects a guarded whole-row save when an ungoverned thread owner changed', async () => {
    const storage = new InMemoryStore({ id: 'memory-observational-source-guard-whole-row-owner' });
    const memory = await storage.getStore('memory');
    if (!memory) throw new Error('Expected in-memory storage domain.');
    const createdAt = new Date('2026-01-01T00:00:00.000Z');
    const threadId = 'source-guard-whole-row-owner-thread';
    const originalResourceId = 'source-guard-whole-row-owner-original';
    const movedResourceId = 'source-guard-whole-row-owner-moved';

    await memory.saveThread({
      thread: {
        id: threadId,
        resourceId: originalResourceId,
        title: 'original',
        metadata: {},
        createdAt,
        updatedAt: createdAt,
      },
    });
    const record = await memory.initializeObservationalMemory({
      threadId,
      resourceId: originalResourceId,
      scope: 'thread',
      config: {},
    });
    await memory.updateThreadResourceId({ threadId, resourceId: movedResourceId });

    await expect(
      memory.mutateThreadWithWorkingMemory({
        mutation: {
          type: 'save',
          thread: {
            id: threadId,
            resourceId: originalResourceId,
            title: 'stale whole-row save',
            metadata: {},
            createdAt,
            updatedAt: createdAt,
          },
        },
        workingMemory: { type: 'require-ungoverned' },
        observationalMemorySourceWriteGuard: {
          recordId: record.id,
          threadId,
          resourceId: originalResourceId,
        },
      }),
    ).rejects.toThrow('guarded thread owner changed');
    await expect(memory.getThreadById({ threadId })).resolves.toMatchObject({
      resourceId: movedResourceId,
      title: 'original',
    });
  });

  it('rejects a source guard permanently after scoped retraction without saving a marker', async () => {
    const storage = new InMemoryStore({ id: 'memory-observational-source-guard-retraction' });
    const memory = await storage.getStore('memory');
    if (!memory) throw new Error('Expected in-memory storage domain.');
    const createdAt = new Date('2026-01-01T00:00:00.000Z');
    const threadId = 'retracted-source-guard-thread';
    const resourceId = 'retracted-source-guard-resource';
    await memory.saveThread({
      thread: { id: threadId, resourceId, title: 'source guard', metadata: {}, createdAt, updatedAt: createdAt },
    });
    const record = await memory.initializeObservationalMemory({
      threadId,
      resourceId,
      scope: 'thread',
      config: {},
    });
    const sourceWriteGuard = { recordId: record.id, threadId, resourceId };

    await memory.retractObservationalMemory({ resourceId, threadId });
    await expect(
      memory.saveMessages({
        messages: [
          {
            id: '__temporal_gap_retracted-source-message',
            threadId,
            resourceId,
            role: 'signal',
            type: 'data-temporal-gap',
            content: {
              format: 2,
              metadata: { reminderType: 'temporal-gap', systemReminder: { type: 'temporal-gap' } },
              parts: [{ type: 'text', text: 'must not persist' }],
            },
            createdAt,
          },
        ],
        observationalMemorySourceWriteGuard: sourceWriteGuard,
      }),
    ).rejects.toThrow('source write guard');
    await expect(memory.listMessages({ threadId, resourceId })).resolves.toMatchObject({ messages: [] });
  });

  it('atomically validates a source guard for thread creation and active-record loading', async () => {
    const storage = new InMemoryStore({ id: 'memory-observational-source-guard-atomic-load' });
    const memory = await storage.getStore('memory');
    if (!memory) throw new Error('Expected in-memory storage domain.');
    const createdAt = new Date('2026-01-01T00:00:00.000Z');
    const threadId = 'atomic-source-guard-thread';
    const resourceId = 'atomic-source-guard-resource';
    const initialRecord = await memory.initializeObservationalMemory({
      threadId,
      resourceId,
      scope: 'thread',
      config: {},
    });
    const sourceWriteGuard = { recordId: initialRecord.id, threadId, resourceId };

    await expect(
      memory.saveThread({
        thread: { id: threadId, resourceId, title: 'first turn', metadata: {}, createdAt, updatedAt: createdAt },
        observationalMemorySourceWriteGuard: sourceWriteGuard,
      }),
    ).resolves.toMatchObject({ id: threadId, title: 'first turn' });

    const activeRecord = await memory.createReflectionGeneration({
      currentRecord: initialRecord,
      reflection: 'active generation',
      tokenCount: 1,
    });
    await expect(
      memory.getObservationalMemoryForSourceWrite({
        threadId,
        resourceId,
        sourceWriteGuard,
      }),
    ).resolves.toMatchObject({ id: activeRecord.id });

    await memory.retractObservationalMemory({ resourceId, threadId });
    await memory.deleteThread({ threadId });
    await expect(
      memory.saveThread({
        thread: {
          id: threadId,
          resourceId,
          title: 'erased title',
          metadata: { erased: true },
          createdAt,
          updatedAt: createdAt,
        },
        observationalMemorySourceWriteGuard: sourceWriteGuard,
      }),
    ).rejects.toThrow('source write guard');
    await expect(memory.getThreadById({ threadId })).resolves.toBeNull();
    await expect(
      memory.getObservationalMemoryForSourceWrite({ threadId, resourceId, sourceWriteGuard }),
    ).rejects.toThrow('source write guard');
  });

  it('rejects mismatched thread ownership before initializing a source-write record', async () => {
    const storage = new InMemoryStore({ id: 'memory-observational-source-guard-initial-owner' });
    const memory = await storage.getStore('memory');
    if (!memory) throw new Error('Expected in-memory storage domain.');
    const createdAt = new Date('2026-01-01T00:00:00.000Z');
    const threadId = 'source-guard-initial-owner-thread';
    const ownerResourceId = 'source-guard-initial-owner-resource';
    const mismatchedResourceId = 'source-guard-initial-mismatched-resource';

    await memory.saveThread({
      thread: {
        id: threadId,
        resourceId: ownerResourceId,
        title: 'owned thread',
        metadata: {},
        createdAt,
        updatedAt: createdAt,
      },
    });

    await expect(
      memory.initializeObservationalMemory({
        threadId,
        resourceId: mismatchedResourceId,
        scope: 'thread',
        config: {},
      }),
    ).rejects.toThrow('does not own the thread');
    await expect(memory.getObservationalMemory(threadId, mismatchedResourceId)).resolves.toBeNull();

    await expect(
      memory.initializeObservationalMemory({
        threadId,
        resourceId: ownerResourceId,
        scope: 'thread',
        config: {},
      }),
    ).resolves.toMatchObject({ threadId, resourceId: ownerResourceId });
  });

  it('atomically replaces a captured predecessor with an idempotent empty successor', async () => {
    const storage = new InMemoryStore({ id: 'memory-observational-source-guard-successor' });
    const memory = await storage.getStore('memory');
    if (!memory) throw new Error('Expected in-memory storage domain.');
    const createdAt = new Date('2026-01-01T00:00:00.000Z');
    const threadId = 'successor-thread';
    const resourceId = 'successor-resource';
    await memory.saveThread({
      thread: { id: threadId, resourceId, title: 'successor', metadata: {}, createdAt, updatedAt: createdAt },
    });
    const predecessor = await memory.initializeObservationalMemory({
      threadId,
      resourceId,
      scope: 'thread',
      config: { policy: 'retain' },
    });
    const successorInput = {
      predecessor: { recordId: predecessor.id, threadId, resourceId },
      successorRecordId: 'successor-record-id',
    };

    const first = await memory.retractObservationalMemory({
      resourceId,
      threadId,
      sourceWriteSuccessor: successorInput,
    });
    expect(first.sourceWriteGuard).toEqual({
      recordId: successorInput.successorRecordId,
      threadId,
      resourceId,
    });
    await expect(memory.getObservationalMemory(threadId, resourceId)).resolves.toMatchObject({
      id: successorInput.successorRecordId,
      activeObservations: '',
      config: { policy: 'retain' },
      metadata: {
        sourceWriteSuccessor: {
          predecessor: successorInput.predecessor,
        },
      },
    });

    const successor = await memory.getObservationalMemory(threadId, resourceId);
    if (!successor) throw new Error('Expected the successor record to remain addressable.');
    const newerGeneration = await memory.createReflectionGeneration({
      currentRecord: successor,
      reflection: 'newer generation after successor receipt',
      tokenCount: 1,
    });

    await expect(
      memory.retractObservationalMemory({ resourceId, threadId, sourceWriteSuccessor: successorInput }),
    ).resolves.toMatchObject({ sourceWriteGuard: first.sourceWriteGuard });
    await expect(memory.getObservationalMemory(threadId, resourceId)).resolves.toMatchObject({
      id: newerGeneration.id,
      activeObservations: 'newer generation after successor receipt',
    });

    await memory.saveMessages({
      messages: [
        {
          id: 'successor-source-message',
          threadId,
          resourceId,
          role: 'user',
          content: { format: 2, parts: [{ type: 'text', text: 'write after edit' }] },
          createdAt,
        },
      ],
      observationalMemorySourceWriteGuard: first.sourceWriteGuard,
    });

    await memory.retractObservationalMemory({ resourceId, threadId });
    await expect(
      memory.retractObservationalMemory({ resourceId, threadId, sourceWriteSuccessor: successorInput }),
    ).rejects.toThrow('captured observational memory predecessor');
  });

  it('clears unmarked observer working memory inside a deletion only under the caller policy', async () => {
    const storage = new InMemoryStore({ id: 'memory-observational-policy-managed-delete' });
    const memory = await storage.getStore('memory');
    if (!memory) throw new Error('Expected in-memory storage domain.');
    const createdAt = new Date('2026-01-01T00:00:00.000Z');
    const threadId = 'policy-thread';
    const resourceId = 'policy-resource';
    await memory.saveThread({
      thread: { id: threadId, resourceId, title: '', metadata: {}, createdAt, updatedAt: createdAt },
    });
    await memory.saveMessages({
      messages: ['policy-deleted', 'policy-kept'].map(id => ({
        id,
        threadId,
        resourceId,
        role: 'user' as const,
        content: { format: 2 as const, parts: [{ type: 'text' as const, text: id }] },
        createdAt,
      })),
    });
    const seed = async (workingMemory: string) => {
      const record = await memory.initializeObservationalMemory({
        threadId: null,
        resourceId,
        scope: 'resource',
        config: {},
      });
      await memory.updateResourceFromObservationalMemory({
        resourceId,
        workingMemory,
        guard: { recordId: record.id, threadId: null, resourceId },
      });
    };

    // Without the policy an unmarked record leaves working memory alone.
    await seed('{"codeword":"KEPT"}');
    await memory.deleteMessages(['policy-kept'], { retractObservationalMemory: true });
    await expect(memory.getResourceById({ resourceId })).resolves.toMatchObject({
      workingMemory: '{"codeword":"KEPT"}',
    });

    await seed('{"codeword":"KESTREL"}');
    const receipts: Array<{ result: { clearedResourceWorkingMemory: boolean } }> = [];
    await memory.deleteMessages(['policy-deleted'], {
      retractObservationalMemory: true,
      managedWorkingMemoryScope: 'resource',
      observationalMemoryRetractions: receipts as any,
    });
    expect(receipts).toMatchObject([{ result: { clearedResourceWorkingMemory: true } }]);
    await expect(memory.getResourceById({ resourceId })).resolves.toMatchObject({ workingMemory: undefined });
    await expect(memory.getObservationalMemory(null, resourceId)).resolves.toBeNull();
  });

  it('fences a late observer write and keeps the successor through a policy-managed retraction', async () => {
    const storage = new InMemoryStore({ id: 'memory-observational-policy-successor' });
    const memory = await storage.getStore('memory');
    if (!memory) throw new Error('Expected in-memory storage domain.');
    const createdAt = new Date('2026-01-01T00:00:00.000Z');
    const threadId = 'policy-successor-thread';
    const resourceId = 'policy-successor-resource';
    await memory.saveThread({
      thread: { id: threadId, resourceId, title: '', metadata: {}, createdAt, updatedAt: createdAt },
    });
    const predecessor = await memory.initializeObservationalMemory({
      threadId,
      resourceId,
      scope: 'thread',
      config: {},
    });
    const writeGuard = { recordId: predecessor.id, threadId, resourceId };
    await memory.updateResourceFromObservationalMemory({
      resourceId,
      workingMemory: '{"codeword":"ORION"}',
      guard: writeGuard,
    });
    const successorRecordId = `${predecessor.id}-successor`;
    const retract = () =>
      memory.retractObservationalMemory({
        resourceId,
        threadId,
        managedWorkingMemoryScope: 'resource',
        sourceWriteSuccessor: { predecessor: writeGuard, successorRecordId },
      });

    await expect(retract()).resolves.toMatchObject({
      clearedResourceWorkingMemory: true,
      sourceWriteGuard: { recordId: successorRecordId },
    });
    // A late observer write naming the retracted generation cannot resurrect it.
    await expect(
      memory.updateResourceFromObservationalMemory({
        resourceId,
        workingMemory: '{"codeword":"ORION","resurrected":true}',
        guard: writeGuard,
      }),
    ).rejects.toThrow();
    await expect(memory.getResourceById({ resourceId })).resolves.toMatchObject({ workingMemory: undefined });
    // The successor survives and is the generation a source write resolves.
    await expect(
      memory.getObservationalMemoryForSourceWrite({
        threadId,
        resourceId,
        sourceWriteGuard: { recordId: successorRecordId, threadId, resourceId },
      }),
    ).resolves.toMatchObject({ id: successorRecordId });
    // A replay finds the successor and clears nothing learned since.
    await memory.updateResource({ resourceId, workingMemory: 'Learned since.' });
    await expect(retract()).resolves.toMatchObject({ clearedResourceWorkingMemory: false });
    await expect(memory.getResourceById({ resourceId })).resolves.toMatchObject({ workingMemory: 'Learned since.' });
  });

  describe('a not-managed (null-marked) record', () => {
    const setup = async (id: string) => {
      const storage = new InMemoryStore({ id });
      const memory = await storage.getStore('memory');
      if (!memory) throw new Error('Expected in-memory storage domain.');
      const createdAt = new Date('2026-01-01T00:00:00.000Z');
      const threadId = `${id}-thread`;
      const resourceId = `${id}-resource`;
      await memory.saveThread({
        thread: {
          id: threadId,
          resourceId,
          title: '',
          metadata: { workingMemory: 'Thread profile from elsewhere.' },
          createdAt,
          updatedAt: createdAt,
        },
      });
      // Created while the observer did not manage working memory; the policy has since changed.
      const record = await memory.initializeObservationalMemory({
        threadId: null,
        resourceId,
        scope: 'resource',
        config: { _managedWorkingMemoryScope: null },
      });
      const guard = { recordId: record.id, threadId: null, resourceId };
      const marker = async () =>
        ((await memory.getObservationalMemory(null, resourceId))?.config as Record<string, unknown>)
          ._managedWorkingMemoryScope;
      return { memory, threadId, resourceId, guard, marker };
    };

    it.each([
      [
        'a guarded resource write',
        async ({ memory, resourceId, guard }: Awaited<ReturnType<typeof setup>>) =>
          memory.updateResourceFromObservationalMemory({ resourceId, workingMemory: 'FROM-DELETED-MSG', guard }),
      ],
      [
        'a guarded revisioned update',
        async ({ memory, resourceId, guard }: Awaited<ReturnType<typeof setup>>) => {
          const current = await memory.getWorkingMemorySnapshot({ scope: 'resource', resourceId });
          await memory.applyWorkingMemoryUpdate({
            scope: 'resource',
            resourceId,
            value: 'FROM-DELETED-MSG',
            expectedRevision: current.revision,
            source: 'observer',
            observationalMemoryGuard: guard,
          });
        },
      ],
    ])('is stamped managed by %s, so a retraction clears what it wrote', async (_name, write) => {
      const context = await setup(`stamp-${_name.replaceAll(' ', '-')}`);
      await write(context);
      await expect(context.marker()).resolves.toBe('resource');
      await expect(
        context.memory.retractObservationalMemory({ resourceId: context.resourceId, threadId: context.threadId }),
      ).resolves.toMatchObject({ clearedScopes: ['resource'], clearedResourceWorkingMemory: true });
      await expect(context.memory.getResourceById({ resourceId: context.resourceId })).resolves.toMatchObject({
        workingMemory: undefined,
      });
    });

    it.each([
      [
        'a rejected guarded update (stale revision)',
        async ({ memory, resourceId, guard }: Awaited<ReturnType<typeof setup>>) => {
          await memory.updateResource({ resourceId, workingMemory: 'Profile from elsewhere.' });
          await expect(
            memory.applyWorkingMemoryUpdate({
              scope: 'resource',
              resourceId,
              value: 'FROM-DELETED-MSG',
              expectedRevision: 999,
              source: 'observer',
              observationalMemoryGuard: guard,
            }),
          ).rejects.toThrow();
        },
      ],
      [
        'an OM thread-metadata update',
        async ({ memory, threadId, resourceId, guard }: Awaited<ReturnType<typeof setup>>) => {
          await memory.updateResource({ resourceId, workingMemory: 'Profile from elsewhere.' });
          await memory.updateThreadFromObservationalMemory({
            id: threadId,
            metadata: { workingMemory: 'Thread profile from elsewhere.', mastra: { om: { threadTitle: 'T' } } },
            guard: { ...guard, threadId: null },
          });
        },
      ],
      [
        'a guarded resource write that changes nothing',
        async ({ memory, resourceId, guard }: Awaited<ReturnType<typeof setup>>) => {
          await memory.updateResource({ resourceId, workingMemory: 'Profile from elsewhere.' });
          await memory.updateResourceFromObservationalMemory({
            resourceId,
            workingMemory: 'Profile from elsewhere.',
            guard,
          });
        },
      ],
      [
        'a rejected guarded resource write (revisioned working memory)',
        async ({ memory, resourceId, guard }: Awaited<ReturnType<typeof setup>>) => {
          await memory.applyWorkingMemoryUpdate({
            scope: 'resource',
            resourceId,
            value: 'Profile from elsewhere.',
            expectedRevision: 0,
            source: 'owner',
          });
          await expect(
            memory.updateResourceFromObservationalMemory({ resourceId, workingMemory: 'FROM-DELETED-MSG', guard }),
          ).rejects.toThrow();
        },
      ],
    ])('stays not managed after %s, and its retraction keeps memory it never wrote', async (_name, write) => {
      const context = await setup(`keep-${_name.replaceAll(' ', '-')}`);
      await write(context);
      await expect(context.marker()).resolves.toBeNull();
      await context.memory.retractObservationalMemory({
        resourceId: context.resourceId,
        threadId: context.threadId,
        managedWorkingMemoryScope: 'resource',
      });
      await expect(context.memory.getResourceById({ resourceId: context.resourceId })).resolves.toMatchObject({
        workingMemory: 'Profile from elsewhere.',
      });
      await expect(context.memory.getThreadById({ threadId: context.threadId })).resolves.toMatchObject({
        metadata: expect.objectContaining({ workingMemory: 'Thread profile from elsewhere.' }),
      });
    });

    it('is stamped thread-managed by a guarded thread-scope update, so a retraction clears every thread profile', async () => {
      const context = await setup('stamp-thread-scope');
      const current = await context.memory.getWorkingMemorySnapshot({
        scope: 'thread',
        resourceId: context.resourceId,
        threadId: context.threadId,
      });
      await context.memory.applyWorkingMemoryUpdate({
        scope: 'thread',
        resourceId: context.resourceId,
        threadId: context.threadId,
        value: 'FROM-DELETED-MSG',
        expectedRevision: current.revision,
        source: 'observer',
        observationalMemoryGuard: context.guard,
      });
      await expect(context.marker()).resolves.toBe('thread');
      await context.memory.retractObservationalMemory({ resourceId: context.resourceId, threadId: context.threadId });
      const thread = await context.memory.getThreadById({ threadId: context.threadId });
      expect(thread?.metadata).not.toHaveProperty('workingMemory');
    });

    it('stays not managed after a guarded update that changes nothing', async () => {
      const context = await setup('keep-no-op');
      await context.memory.updateResource({ resourceId: context.resourceId, workingMemory: 'Profile from elsewhere.' });
      const current = await context.memory.getWorkingMemorySnapshot({
        scope: 'resource',
        resourceId: context.resourceId,
      });
      await context.memory.applyWorkingMemoryUpdate({
        scope: 'resource',
        resourceId: context.resourceId,
        value: 'Profile from elsewhere.',
        expectedRevision: current.revision,
        source: 'observer',
        observationalMemoryGuard: context.guard,
      });
      await expect(context.marker()).resolves.toBeNull();
    });

    it('never lets a guarded write and a concurrent retraction leave its working memory unretracted', async () => {
      const context = await setup('race-legacy-write');
      await Promise.allSettled([
        context.memory.updateResourceFromObservationalMemory({
          resourceId: context.resourceId,
          workingMemory: 'FROM-DELETED-MSG',
          guard: context.guard,
        }),
        context.memory.retractObservationalMemory({ resourceId: context.resourceId, threadId: context.threadId }),
      ]);
      const resource = await context.memory.getResourceById({ resourceId: context.resourceId });
      // Either the write landed first (stamped, then cleared) or the
      // retraction did (the write is refused); never stored and unretracted.
      expect(resource?.workingMemory).not.toBe('FROM-DELETED-MSG');
    });

    it('keeps a guarded write atomic with its stamp when updateResource is overridden to await before writing', async () => {
      const context = await setup('race-async-override');
      const updateResource = context.memory.updateResource.bind(context.memory);
      context.memory.updateResource = async args => {
        await Promise.resolve();
        return updateResource(args);
      };
      await Promise.allSettled([
        context.memory.updateResourceFromObservationalMemory({
          resourceId: context.resourceId,
          workingMemory: 'FROM-DELETED-MSG',
          guard: context.guard,
        }),
        context.memory.retractObservationalMemory({ resourceId: context.resourceId, threadId: context.threadId }),
      ]);
      const resource = await context.memory.getResourceById({ resourceId: context.resourceId });
      expect(resource?.workingMemory).not.toBe('FROM-DELETED-MSG');
    });
  });

  it.each([
    ['null', null, 'Profile from elsewhere.'],
    ['a primitive', 'not-an-object', 'Profile from elsewhere.'],
    ['an array', [], 'Profile from elsewhere.'],
    ['an object without the marker (control)', {}, undefined],
  ])(
    'applies the policy fallback to a record whose config is %s only when it is a marker-free object',
    async (_name, config, expected) => {
      const storage = new InMemoryStore({ id: `memory-observational-config-${_name}` });
      const memory = await storage.getStore('memory');
      if (!memory) throw new Error('Expected in-memory storage domain.');
      const createdAt = new Date('2026-01-01T00:00:00.000Z');
      const threadId = 'config-shape-thread';
      const resourceId = 'config-shape-resource';
      await memory.saveThread({
        thread: { id: threadId, resourceId, title: '', metadata: {}, createdAt, updatedAt: createdAt },
      });
      await memory.updateResource({ resourceId, workingMemory: 'Profile from elsewhere.' });
      await memory.initializeObservationalMemory({
        threadId: null,
        resourceId,
        scope: 'resource',
        config: config as any,
      });
      await memory.retractObservationalMemory({ resourceId, threadId, managedWorkingMemoryScope: 'resource' });
      await expect(memory.getResourceById({ resourceId })).resolves.toMatchObject({ workingMemory: expected });
    },
  );

  it('never applies the policy fallback to a record explicitly marked as not managing working memory', async () => {
    const storage = new InMemoryStore({ id: 'memory-observational-policy-null-marker' });
    const memory = await storage.getStore('memory');
    if (!memory) throw new Error('Expected in-memory storage domain.');
    const createdAt = new Date('2026-01-01T00:00:00.000Z');
    const threadId = 'null-marker-thread';
    const resourceId = 'null-marker-resource';
    await memory.saveThread({
      thread: { id: threadId, resourceId, title: '', metadata: {}, createdAt, updatedAt: createdAt },
    });
    await memory.updateResource({ resourceId, workingMemory: 'Profile the observer never wrote.' });
    await memory.initializeObservationalMemory({
      threadId: null,
      resourceId,
      scope: 'resource',
      config: { _managedWorkingMemoryScope: null },
    });
    await expect(
      memory.retractObservationalMemory({ resourceId, threadId, managedWorkingMemoryScope: 'resource' }),
    ).resolves.toMatchObject({ clearedScopes: ['resource'], clearedResourceWorkingMemory: false });
    await expect(memory.getResourceById({ resourceId })).resolves.toMatchObject({
      workingMemory: 'Profile the observer never wrote.',
    });
  });

  it('does not clear sibling thread memory when a resource record owns only resource memory', async () => {
    const storage = new InMemoryStore({ id: 'memory-observational-retraction-mixed-scope' });
    const memory = await storage.getStore('memory');
    if (!memory) throw new Error('Expected in-memory storage domain.');
    const resourceId = 'mixed-scope-resource';
    const editedThreadId = 'mixed-scope-edited';
    const siblingThreadId = 'mixed-scope-sibling';
    const createdAt = new Date('2026-01-01T00:00:00.000Z');
    for (const id of [editedThreadId, siblingThreadId]) {
      await memory.saveThread({
        thread: { id, resourceId, title: id, metadata: {}, createdAt, updatedAt: createdAt },
      });
    }
    await memory.initializeObservationalMemory({
      config: { _managedWorkingMemoryScope: 'resource' },
      resourceId,
      scope: 'resource',
      threadId: null,
    });
    const editedRecord = await memory.initializeObservationalMemory({
      config: { _managedWorkingMemoryScope: 'thread' },
      resourceId,
      scope: 'thread',
      threadId: editedThreadId,
    });
    const siblingRecord = await memory.initializeObservationalMemory({
      config: { _managedWorkingMemoryScope: 'thread' },
      resourceId,
      scope: 'thread',
      threadId: siblingThreadId,
    });
    await memory.applyWorkingMemoryUpdate({
      scope: 'thread',
      resourceId,
      threadId: editedThreadId,
      value: '{"task":"edited"}',
      expectedRevision: 0,
      source: 'observer',
      observationalMemoryGuard: { recordId: editedRecord.id, resourceId, threadId: editedThreadId },
    });
    const siblingSnapshot = await memory.applyWorkingMemoryUpdate({
      scope: 'thread',
      resourceId,
      threadId: siblingThreadId,
      value: '{"task":"sibling"}',
      expectedRevision: 0,
      source: 'observer',
      observationalMemoryGuard: { recordId: siblingRecord.id, resourceId, threadId: siblingThreadId },
    });

    await memory.retractObservationalMemory({ resourceId, threadId: editedThreadId });

    await expect(
      memory.getWorkingMemorySnapshot({ scope: 'thread', resourceId, threadId: editedThreadId }),
    ).resolves.toMatchObject({ value: null, revision: 2 });
    await expect(
      memory.getWorkingMemorySnapshot({ scope: 'thread', resourceId, threadId: siblingThreadId }),
    ).resolves.toEqual(siblingSnapshot);
    await expect(memory.getObservationalMemory(siblingThreadId, resourceId)).resolves.toMatchObject({
      id: siblingRecord.id,
    });
  });

  it('clears every derived surface and fences an in-flight generation', async () => {
    const storage = new InMemoryStore({ id: 'memory-observational-retraction' });
    const memory = await storage.getStore('memory');
    if (!memory) throw new Error('Expected in-memory storage domain.');

    const createdAt = new Date('2026-01-01T00:00:00.000Z');
    await memory.saveResource({
      resource: {
        id: 'resource-1',
        workingMemory: '{"privateCodename":"ZEPHYR-9"}',
        metadata: { preserved: true },
        createdAt,
        updatedAt: createdAt,
      },
    });
    await memory.saveThread({
      thread: {
        id: 'thread-1',
        resourceId: 'resource-1',
        title: 'ZEPHYR-9 planning',
        metadata: {
          preserved: true,
          mastra: {
            preserved: true,
            om: {
              threadTitle: 'ZEPHYR-9 planning',
              extracted: { privateCodename: 'ZEPHYR-9' },
            },
          },
        },
        createdAt,
        updatedAt: createdAt,
      },
    });
    const resourceRecord = await memory.initializeObservationalMemory({
      config: { _managedWorkingMemoryScope: 'resource' },
      threadId: null,
      resourceId: 'resource-1',
      scope: 'resource',
    });
    await memory.initializeObservationalMemory({
      threadId: 'thread-1',
      resourceId: 'resource-1',
      scope: 'thread',
    });

    await expect(
      memory.retractObservationalMemory({ resourceId: 'resource-1', threadId: 'thread-1' }),
    ).resolves.toEqual({
      clearedScopes: ['resource', 'thread'],
      clearedResourceWorkingMemory: true,
      clearedThreadMetadata: true,
    });
    await expect(memory.getObservationalMemory(null, 'resource-1')).resolves.toBeNull();
    await expect(memory.getObservationalMemory('thread-1', 'resource-1')).resolves.toBeNull();
    await expect(memory.getResourceById({ resourceId: 'resource-1' })).resolves.toMatchObject({
      workingMemory: undefined,
      metadata: { preserved: true },
    });
    await expect(memory.getThreadById({ threadId: 'thread-1' })).resolves.toMatchObject({
      title: '',
      metadata: { preserved: true, mastra: { preserved: true } },
    });

    const staleGuard = {
      recordId: resourceRecord.id,
      threadId: null,
      resourceId: 'resource-1',
    };
    await expect(
      memory.updateResourceFromObservationalMemory({
        resourceId: 'resource-1',
        workingMemory: 'stale private codename',
        guard: staleGuard,
      }),
    ).rejects.toThrow('no longer current');
    await expect(
      memory.createReflectionGeneration({
        currentRecord: resourceRecord,
        reflection: 'stale reflected private codename',
        tokenCount: 5,
      }),
    ).rejects.toThrow('no longer current');
  });

  it('preserves externally managed profile memory when OM does not own it', async () => {
    const storage = new InMemoryStore({ id: 'memory-observational-retraction-no-om' });
    const memory = await storage.getStore('memory');
    if (!memory) throw new Error('Expected in-memory storage domain.');
    const createdAt = new Date('2026-01-01T00:00:00.000Z');
    await memory.saveResource({
      resource: {
        id: 'resource-external',
        workingMemory: 'Verified profile supplied outside Observational Memory.',
        metadata: { source: 'profile-service' },
        createdAt,
        updatedAt: createdAt,
      },
    });
    await memory.saveThread({
      thread: {
        id: 'thread-external',
        resourceId: 'resource-external',
        title: 'User title',
        metadata: { preserved: true },
        createdAt,
        updatedAt: createdAt,
      },
    });
    await memory.initializeObservationalMemory({
      config: {},
      threadId: null,
      resourceId: 'resource-external',
      scope: 'resource',
    });

    await expect(
      memory.retractObservationalMemory({
        resourceId: 'resource-external',
        threadId: 'thread-external',
      }),
    ).resolves.toEqual({
      clearedScopes: ['resource'],
      clearedResourceWorkingMemory: false,
      clearedThreadMetadata: false,
    });
    await expect(memory.getResourceById({ resourceId: 'resource-external' })).resolves.toMatchObject({
      workingMemory: 'Verified profile supplied outside Observational Memory.',
      metadata: { source: 'profile-service' },
    });
    await expect(memory.getThreadById({ threadId: 'thread-external' })).resolves.toMatchObject({
      title: 'User title',
      metadata: { preserved: true },
    });
  });

  it('clears observer-managed thread working memory and resource-scoped sibling cursors', async () => {
    const storage = new InMemoryStore({ id: 'memory-observational-retraction-owned-thread-state' });
    const memory = await storage.getStore('memory');
    if (!memory) throw new Error('Expected in-memory storage domain.');
    const createdAt = new Date('2026-01-01T00:00:00.000Z');

    for (const suffix of ['a', 'b']) {
      await memory.saveThread({
        thread: {
          id: `thread-${suffix}`,
          resourceId: 'resource-shared',
          title: `Derived ${suffix}`,
          metadata: {
            workingMemory: `observer-managed-${suffix}`,
            mastra: { om: { threadTitle: `Derived ${suffix}`, lastObservedMessageId: `message-${suffix}` } },
          },
          createdAt,
          updatedAt: createdAt,
        },
      });
    }
    await memory.initializeObservationalMemory({
      config: { _managedWorkingMemoryScope: 'thread' },
      threadId: null,
      resourceId: 'resource-shared',
      scope: 'resource',
    });

    await expect(
      memory.retractObservationalMemory({ resourceId: 'resource-shared', threadId: 'thread-a' }),
    ).resolves.toMatchObject({
      clearedScopes: ['resource'],
      clearedResourceWorkingMemory: false,
      clearedThreadMetadata: true,
    });
    for (const suffix of ['a', 'b']) {
      await expect(memory.getThreadById({ threadId: `thread-${suffix}` })).resolves.toMatchObject({
        title: '',
        metadata: { mastra: {} },
      });
      expect((await memory.getThreadById({ threadId: `thread-${suffix}` }))?.metadata).not.toHaveProperty(
        'workingMemory',
      );
    }
  });

  it('retracts both source and destination observations when a message moves', async () => {
    const storage = new InMemoryStore({ id: 'memory-observational-retraction-move' });
    const memory = await storage.getStore('memory');
    if (!memory) throw new Error('Expected in-memory storage domain.');
    const createdAt = new Date('2026-01-01T00:00:00.000Z');

    for (const suffix of ['source', 'destination']) {
      await memory.saveThread({
        thread: {
          id: `thread-${suffix}`,
          resourceId: `resource-${suffix}`,
          title: suffix,
          metadata: {},
          createdAt,
          updatedAt: createdAt,
        },
      });
      await memory.initializeObservationalMemory({
        threadId: `thread-${suffix}`,
        resourceId: `resource-${suffix}`,
        scope: 'thread',
      });
    }
    await memory.saveMessages({
      messages: [
        {
          id: 'message-move',
          threadId: 'thread-source',
          resourceId: 'resource-source',
          role: 'user',
          content: { format: 2, parts: [{ type: 'text', text: 'Move me' }] },
          createdAt,
        },
      ],
    });

    await memory.updateMessages({
      messages: [
        {
          id: 'message-move',
          threadId: 'thread-destination',
          resourceId: 'resource-destination',
        },
      ],
      retractObservationalMemory: true,
    });

    await expect(memory.getObservationalMemory('thread-source', 'resource-source')).resolves.toBeNull();
    await expect(memory.getObservationalMemory('thread-destination', 'resource-destination')).resolves.toBeNull();
  });

  it.each(['update', 'delete'] as const)(
    'uses the persisted thread OM owner for message %s retraction',
    async operation => {
      const storage = new InMemoryStore({ id: `memory-observational-retraction-owner-${operation}` });
      const memory = await storage.getStore('memory');
      if (!memory) throw new Error('Expected in-memory storage domain.');
      const createdAt = new Date('2026-01-01T00:00:00.000Z');
      const messageResourceId = `message-resource-${operation}`;
      const ownerResourceId = `owner-resource-${operation}`;
      const threadId = `owner-thread-${operation}`;
      const messageId = `owner-message-${operation}`;

      for (const [id, workingMemory] of [
        [messageResourceId, 'unrelated message-resource memory'],
        [ownerResourceId, 'observer-managed owner memory'],
      ] as const) {
        await memory.saveResource({
          resource: { id, workingMemory, metadata: {}, createdAt, updatedAt: createdAt },
        });
      }
      await memory.saveThread({
        thread: {
          id: threadId,
          resourceId: ownerResourceId,
          title: threadId,
          metadata: {},
          createdAt,
          updatedAt: createdAt,
        },
      });
      await memory.initializeObservationalMemory({
        config: { _managedWorkingMemoryScope: 'resource' },
        resourceId: ownerResourceId,
        scope: 'thread',
        threadId,
      });
      await memory.saveMessages({
        messages: [
          {
            id: messageId,
            threadId,
            resourceId: messageResourceId,
            role: 'user',
            content: { format: 2, parts: [{ type: 'text', text: 'Retract by owner' }] },
            createdAt,
          },
        ],
      });

      const retractions: Array<{ input: { resourceId: string; threadId: string } }> = [];
      if (operation === 'update') {
        await memory.updateMessages({
          messages: [{ id: messageId, content: { content: 'authoritative edit' } }],
          retractObservationalMemory: true,
          observationalMemoryRetractions: retractions as any,
        });
      } else {
        await memory.deleteMessages([messageId], {
          retractObservationalMemory: true,
          observationalMemoryRetractions: retractions as any,
        });
      }

      expect(retractions).toMatchObject([{ input: { resourceId: ownerResourceId, threadId } }]);
      await expect(memory.getObservationalMemory(threadId, ownerResourceId)).resolves.toBeNull();
      await expect(memory.getResourceById({ resourceId: ownerResourceId })).resolves.toMatchObject({
        workingMemory: undefined,
      });
      await expect(memory.getResourceById({ resourceId: messageResourceId })).resolves.toMatchObject({
        workingMemory: 'unrelated message-resource memory',
      });
    },
  );

  it('uses the last duplicate update for both the message move and OM retraction coordinates', async () => {
    const storage = new InMemoryStore({ id: 'memory-observational-retraction-duplicate-update' });
    const memory = await storage.getStore('memory');
    if (!memory) throw new Error('Expected in-memory storage domain.');
    const createdAt = new Date('2026-01-01T00:00:00.000Z');
    const messageId = 'message-duplicate-update';
    const source = {
      resourceId: 'resource-duplicate-source',
      threadId: 'thread-duplicate-source',
      workingMemory: 'source observer memory',
    };
    const discarded = {
      resourceId: 'resource-duplicate-discarded',
      threadId: 'thread-duplicate-discarded',
      workingMemory: 'discarded observer memory',
    };
    const canonical = {
      resourceId: 'resource-duplicate-canonical',
      threadId: 'thread-duplicate-canonical',
      workingMemory: 'canonical observer memory',
    };

    for (const coordinate of [source, discarded, canonical]) {
      await memory.saveResource({
        resource: {
          id: coordinate.resourceId,
          workingMemory: coordinate.workingMemory,
          metadata: {},
          createdAt,
          updatedAt: createdAt,
        },
      });
      await memory.saveThread({
        thread: {
          id: coordinate.threadId,
          resourceId: coordinate.resourceId,
          title: coordinate.threadId,
          metadata: {},
          createdAt,
          updatedAt: createdAt,
        },
      });
      await memory.initializeObservationalMemory({
        config: { _managedWorkingMemoryScope: 'resource' },
        resourceId: coordinate.resourceId,
        scope: 'thread',
        threadId: coordinate.threadId,
      });
    }
    await memory.saveMessages({
      messages: [
        {
          id: messageId,
          threadId: source.threadId,
          resourceId: source.resourceId,
          role: 'user',
          content: { format: 2, parts: [{ type: 'text', text: 'Move me once' }] },
          createdAt,
        },
      ],
    });

    const retractions: Array<{ input: { resourceId: string; threadId: string } }> = [];
    const updatedMessages = await memory.updateMessages({
      messages: [
        {
          id: messageId,
          threadId: discarded.threadId,
          resourceId: discarded.resourceId,
          content: { content: 'discarded update' },
        },
        {
          id: messageId,
          threadId: canonical.threadId,
          resourceId: canonical.resourceId,
          content: { content: 'canonical update' },
        },
      ],
      retractObservationalMemory: true,
      observationalMemoryRetractions: retractions as any,
    });

    expect(updatedMessages).toMatchObject([
      {
        id: messageId,
        threadId: canonical.threadId,
        resourceId: canonical.resourceId,
        content: { content: 'canonical update' },
      },
    ]);
    expect(updatedMessages).toHaveLength(1);
    expect(
      new Set(retractions.map(retraction => `${retraction.input.resourceId}\u0000${retraction.input.threadId}`)),
    ).toEqual(
      new Set([`${source.resourceId}\u0000${source.threadId}`, `${canonical.resourceId}\u0000${canonical.threadId}`]),
    );
    expect(retractions).toHaveLength(2);

    await expect(memory.listMessagesById({ messageIds: [messageId] })).resolves.toMatchObject({
      messages: [
        {
          id: messageId,
          threadId: canonical.threadId,
          resourceId: canonical.resourceId,
          content: { content: 'canonical update' },
        },
      ],
    });
    await expect(memory.getObservationalMemory(source.threadId, source.resourceId)).resolves.toBeNull();
    await expect(memory.getObservationalMemory(canonical.threadId, canonical.resourceId)).resolves.toBeNull();
    await expect(memory.getResourceById({ resourceId: source.resourceId })).resolves.toMatchObject({
      workingMemory: undefined,
    });
    await expect(memory.getResourceById({ resourceId: canonical.resourceId })).resolves.toMatchObject({
      workingMemory: undefined,
    });

    await expect(memory.getObservationalMemory(discarded.threadId, discarded.resourceId)).resolves.not.toBeNull();
    await expect(memory.getResourceById({ resourceId: discarded.resourceId })).resolves.toMatchObject({
      workingMemory: discarded.workingMemory,
    });
  });

  it('rejects generation guards whose coordinates do not match the target row', async () => {
    const storage = new InMemoryStore({ id: 'memory-observational-retraction-guard-identity' });
    const memory = await storage.getStore('memory');
    if (!memory) throw new Error('Expected in-memory storage domain.');
    const createdAt = new Date('2026-01-01T00:00:00.000Z');

    for (const resourceId of ['resource-guard', 'resource-other']) {
      await memory.saveResource({
        resource: {
          id: resourceId,
          workingMemory: 'preserved',
          metadata: {},
          createdAt,
          updatedAt: createdAt,
        },
      });
    }
    for (const threadId of ['thread-guard', 'thread-other']) {
      await memory.saveThread({
        thread: {
          id: threadId,
          resourceId: 'resource-guard',
          title: 'Preserved title',
          metadata: {},
          createdAt,
          updatedAt: createdAt,
        },
      });
    }
    await memory.saveThread({
      thread: {
        id: 'thread-cross-resource',
        resourceId: 'resource-other',
        title: 'Preserved cross-resource title',
        metadata: {},
        createdAt,
        updatedAt: createdAt,
      },
    });

    const resourceRecord = await memory.initializeObservationalMemory({
      threadId: null,
      resourceId: 'resource-guard',
      scope: 'resource',
    });
    const threadRecord = await memory.initializeObservationalMemory({
      threadId: 'thread-guard',
      resourceId: 'resource-guard',
      scope: 'thread',
    });

    await expect(
      memory.updateResourceFromObservationalMemory({
        resourceId: 'resource-other',
        workingMemory: 'must not be written',
        guard: {
          recordId: resourceRecord.id,
          threadId: null,
          resourceId: 'resource-guard',
        },
      }),
    ).rejects.toThrow('does not match the target resource');
    await expect(
      memory.updateThreadFromObservationalMemory({
        id: 'thread-other',
        title: 'Must not be written',
        metadata: { leaked: true },
        guard: {
          recordId: threadRecord.id,
          threadId: 'thread-guard',
          resourceId: 'resource-guard',
        },
      }),
    ).rejects.toThrow('does not match the target thread');
    await expect(
      memory.updateThreadFromObservationalMemory({
        id: 'thread-cross-resource',
        title: 'Must not cross the resource boundary',
        metadata: { leaked: true },
        guard: {
          recordId: resourceRecord.id,
          threadId: null,
          resourceId: 'resource-guard',
        },
      }),
    ).rejects.toThrow('does not match the target thread resource');

    await expect(memory.getResourceById({ resourceId: 'resource-other' })).resolves.toMatchObject({
      workingMemory: 'preserved',
    });
    await expect(memory.getThreadById({ threadId: 'thread-other' })).resolves.toMatchObject({
      title: 'Preserved title',
      metadata: {},
    });
    await expect(memory.getThreadById({ threadId: 'thread-cross-resource' })).resolves.toMatchObject({
      title: 'Preserved cross-resource title',
      metadata: {},
    });
  });

  it('retracts observer-derived resource state when a thread is deleted directly', async () => {
    const storage = new InMemoryStore({ id: 'memory-observational-retraction-delete-thread' });
    const memory = await storage.getStore('memory');
    if (!memory) throw new Error('Expected in-memory storage domain.');
    const createdAt = new Date('2026-01-01T00:00:00.000Z');
    await memory.saveResource({
      resource: {
        id: 'resource-delete-thread',
        workingMemory: '{"privateCodename":"ZEPHYR-9"}',
        metadata: {},
        createdAt,
        updatedAt: createdAt,
      },
    });
    await memory.saveThread({
      thread: {
        id: 'thread-delete',
        resourceId: 'resource-delete-thread',
        title: 'Private thread',
        metadata: {},
        createdAt,
        updatedAt: createdAt,
      },
    });
    await memory.initializeObservationalMemory({
      config: { _managedWorkingMemoryScope: 'resource' },
      threadId: 'thread-delete',
      resourceId: 'resource-delete-thread',
      scope: 'thread',
    });

    await memory.deleteThread({ threadId: 'thread-delete' });

    await expect(memory.getThreadById({ threadId: 'thread-delete' })).resolves.toBeNull();
    await expect(memory.getObservationalMemory('thread-delete', 'resource-delete-thread')).resolves.toBeNull();
    await expect(memory.getResourceById({ resourceId: 'resource-delete-thread' })).resolves.toMatchObject({
      workingMemory: undefined,
    });
  });

  it('retracts a prepared thread record when deletion wins before thread creation', async () => {
    const storage = new InMemoryStore({ id: 'memory-observational-retraction-prepared-delete-thread' });
    const memory = await storage.getStore('memory');
    if (!memory) throw new Error('Expected in-memory storage domain.');
    const threadId = 'thread-prepared-delete';
    const resourceId = 'resource-prepared-delete';
    const record = await memory.initializeObservationalMemory({
      config: {},
      threadId,
      resourceId,
      scope: 'thread',
    });
    const sourceWriteGuard = { recordId: record.id, threadId, resourceId };
    const retractions: Array<{
      input: { resourceId: string; threadId: string };
      result: { clearedScopes: Array<'resource' | 'thread'> };
    }> = [];

    await memory.deleteThread({ threadId, observationalMemoryRetractions: retractions });

    expect(retractions).toMatchObject([
      {
        input: { resourceId, threadId },
        result: { clearedScopes: ['thread'] },
      },
    ]);
    await expect(memory.getObservationalMemory(threadId, resourceId)).resolves.toBeNull();
    await expect(
      memory.saveMessages({
        messages: [
          {
            id: 'prepared-delete-message',
            threadId,
            resourceId,
            role: 'user',
            content: { format: 2, parts: [{ type: 'text', text: 'must be rejected after deletion' }] },
            createdAt: new Date('2026-01-01T00:00:00.000Z'),
          },
        ],
        observationalMemorySourceWriteGuard: sourceWriteGuard,
      }),
    ).rejects.toThrow('source write guard');
  });

  it('retracts observer-derived state inside authoritative message mutations', async () => {
    const storage = new InMemoryStore({ id: 'memory-observational-retraction-message-mutations' });
    const memory = await storage.getStore('memory');
    if (!memory) throw new Error('Expected in-memory storage domain.');
    const createdAt = new Date('2026-01-01T00:00:00.000Z');

    for (const operation of ['update', 'delete'] as const) {
      const resourceId = `resource-${operation}`;
      const threadId = `thread-${operation}`;
      const messageId = `message-${operation}`;
      await memory.saveResource({
        resource: {
          id: resourceId,
          workingMemory: '{"privateCodename":"ZEPHYR-9"}',
          metadata: {},
          createdAt,
          updatedAt: createdAt,
        },
      });
      await memory.saveThread({
        thread: {
          id: threadId,
          resourceId,
          title: 'Private thread',
          metadata: {},
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
            content: { format: 2, parts: [{ type: 'text', text: 'Private fact' }] },
            createdAt,
          },
        ],
      });
      await memory.initializeObservationalMemory({
        config: { _managedWorkingMemoryScope: 'resource' },
        threadId,
        resourceId,
        scope: 'thread',
      });

      if (operation === 'update') {
        await memory.updateMessages({
          messages: [
            {
              id: messageId,
              content: { content: 'Corrected fact' },
            },
          ],
          retractObservationalMemory: true,
        });
      } else {
        await memory.deleteMessages([messageId], { retractObservationalMemory: true });
      }

      await expect(memory.getObservationalMemory(threadId, resourceId)).resolves.toBeNull();
      await expect(memory.getResourceById({ resourceId })).resolves.toMatchObject({
        workingMemory: undefined,
      });
    }
  });

  it('rolls back an in-memory edit and leaves no receipt when the authoritative mutation fails', async () => {
    const storage = new InMemoryStore({ id: 'memory-observational-retraction-rollback' });
    const memory = await storage.getStore('memory');
    if (!memory) throw new Error('Expected in-memory storage domain.');
    const createdAt = new Date('2026-01-01T00:00:00.000Z');
    const resourceId = 'resource-rollback';
    const threadId = 'thread-rollback';
    const messageId = 'message-rollback';
    await memory.saveResource({
      resource: {
        id: resourceId,
        workingMemory: 'observer-managed',
        metadata: {},
        createdAt,
        updatedAt: createdAt,
      },
    });
    await memory.saveThread({
      thread: {
        id: threadId,
        resourceId,
        title: 'Preserved',
        metadata: {},
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
          content: { format: 2, parts: [{ type: 'text', text: 'Original fact' }] },
          createdAt,
        },
      ],
    });
    await memory.initializeObservationalMemory({
      config: { _managedWorkingMemoryScope: 'resource' },
      threadId,
      resourceId,
      scope: 'thread',
    });

    const circularContent: Record<string, unknown> = {};
    circularContent.self = circularContent;
    const receipts: Array<unknown> = [];
    await expect(
      memory.updateMessages({
        messages: [{ id: messageId, content: circularContent as any }],
        retractObservationalMemory: true,
        observationalMemoryRetractions: receipts as any,
      }),
    ).rejects.toThrow();

    expect(receipts).toEqual([]);
    await expect(memory.getObservationalMemory(threadId, resourceId)).resolves.not.toBeNull();
    await expect(memory.getResourceById({ resourceId })).resolves.toMatchObject({
      workingMemory: 'observer-managed',
    });
    await expect(memory.listMessagesById({ messageIds: [messageId] })).resolves.toMatchObject({
      messages: [
        {
          id: messageId,
          content: { parts: [{ type: 'text', text: 'Original fact' }] },
        },
      ],
    });
  });
});

function sessionRecord(id: string, resourceId: string, threadId: string): SessionRecord {
  return {
    harnessName: 'default',
    id,
    resourceId,
    threadId,
    origin: 'top-level',
    ownsThread: false,
    modeId: 'build',
    modelId: 'model-1',
    subagentModelOverrides: {},
    permissionRules: { categories: {}, tools: {} },
    sessionGrants: { categories: [], tools: [] },
    tokenUsage: { promptTokens: 0, completionTokens: 0, totalTokens: 0 },
    pendingQueue: [],
    state: {},
    createdAt: 1_000,
    lastActivityAt: 1_000,
    version: 0,
  };
}
