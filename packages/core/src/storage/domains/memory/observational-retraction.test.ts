import { describe, expect, it } from 'vitest';

import { InMemoryStore } from '../../mock';

describe('MemoryStorage atomic observational-memory retraction', () => {
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
