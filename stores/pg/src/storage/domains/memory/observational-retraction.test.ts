import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { PostgresStore } from '../..';
import { connectionString } from '../../test-utils';
import type { MemoryPG } from '.';

describe('MemoryPG observational-memory retraction', () => {
  const schemaName = `om_retraction_${Date.now()}`;
  const resourceId = `resource-${Date.now()}`;
  const threadId = `thread-${Date.now()}`;
  const createdAt = new Date('2026-01-01T00:00:00.000Z');
  let observerStore: PostgresStore;
  let retractorStore: PostgresStore;
  let observerMemory: MemoryPG;
  let retractorMemory: MemoryPG;

  beforeAll(async () => {
    observerStore = new PostgresStore({
      id: 'om-retraction-observer',
      connectionString,
      schemaName,
    });
    await observerStore.init();
    retractorStore = new PostgresStore({
      id: 'om-retraction-retractor',
      connectionString,
      disableInit: true,
      schemaName,
    });
    await retractorStore.init();
    observerMemory = (await observerStore.getStore('memory')) as MemoryPG;
    retractorMemory = (await retractorStore.getStore('memory')) as MemoryPG;
  });

  afterAll(async () => {
    await Promise.allSettled([observerStore?.close(), retractorStore?.close()]);
    const cleanup = new Pool({ connectionString });
    try {
      await cleanup.query(`DROP SCHEMA IF EXISTS "${schemaName}" CASCADE`);
    } finally {
      await cleanup.end();
    }
  });

  it('atomically clears derived state and rejects stale writes from another store instance', async () => {
    await observerMemory.saveResource({
      resource: {
        id: resourceId,
        workingMemory: '{"privateCodename":"ZEPHYR-9"}',
        metadata: { preserved: true },
        createdAt,
        updatedAt: createdAt,
      },
    });
    await observerMemory.saveThread({
      thread: {
        id: threadId,
        resourceId,
        title: 'ZEPHYR-9 planning',
        metadata: {
          preserved: true,
          mastra: {
            preserved: true,
            om: {
              extracted: { privateCodename: 'ZEPHYR-9' },
              threadTitle: 'ZEPHYR-9 planning',
            },
          },
        },
        createdAt,
        updatedAt: createdAt,
      },
    });
    const staleResourceRecord = await observerMemory.initializeObservationalMemory({
      config: {
        observationThreshold: 5000,
        reflectionThreshold: 40000,
        _managedWorkingMemoryScope: 'resource',
      },
      resourceId,
      scope: 'resource',
      threadId: null,
    });
    await observerMemory.updateActiveObservations({
      id: staleResourceRecord.id,
      lastObservedAt: createdAt,
      observations: 'The private codename is ZEPHYR-9.',
      observedMessageIds: ['deleted-message'],
      tokenCount: 8,
    });
    await observerMemory.initializeObservationalMemory({
      config: { observationThreshold: 5000, reflectionThreshold: 40000 },
      resourceId,
      scope: 'thread',
      threadId,
    });

    await expect(retractorMemory.retractObservationalMemory({ resourceId, threadId })).resolves.toEqual({
      clearedScopes: ['resource', 'thread'],
      clearedResourceWorkingMemory: true,
      clearedThreadMetadata: true,
    });

    await expect(retractorMemory.getObservationalMemory(null, resourceId)).resolves.toBeNull();
    await expect(retractorMemory.getObservationalMemory(threadId, resourceId)).resolves.toBeNull();
    await expect(retractorMemory.getResourceById({ resourceId })).resolves.toMatchObject({
      workingMemory: null,
      metadata: { preserved: true },
    });
    await expect(retractorMemory.getThreadById({ threadId })).resolves.toMatchObject({
      title: '',
      metadata: { preserved: true, mastra: { preserved: true } },
    });

    const freshResourceRecord = await retractorMemory.initializeObservationalMemory({
      config: { observationThreshold: 5000, reflectionThreshold: 40000 },
      resourceId,
      scope: 'resource',
      threadId: null,
    });
    const freshThreadRecord = await retractorMemory.initializeObservationalMemory({
      config: { observationThreshold: 5000, reflectionThreshold: 40000 },
      resourceId,
      scope: 'thread',
      threadId,
    });
    expect(freshResourceRecord.id).not.toBe(staleResourceRecord.id);

    const mismatchedResourceId = `${resourceId}-mismatched-target`;
    await expect(
      retractorMemory.updateResourceFromObservationalMemory({
        resourceId: mismatchedResourceId,
        workingMemory: 'must not be written',
        guard: {
          recordId: freshResourceRecord.id,
          resourceId,
          threadId: null,
        },
      }),
    ).rejects.toThrow(/does not match the target resource/i);
    await expect(retractorMemory.getResourceById({ resourceId: mismatchedResourceId })).resolves.toBeNull();

    const mismatchedThreadId = `${threadId}-mismatched-target`;
    await retractorMemory.saveThread({
      thread: {
        id: mismatchedThreadId,
        resourceId,
        title: 'Preserved title',
        metadata: {},
        createdAt,
        updatedAt: createdAt,
      },
    });
    await expect(
      retractorMemory.updateThreadFromObservationalMemory({
        id: mismatchedThreadId,
        title: 'Must not be written',
        metadata: { leaked: true },
        guard: {
          recordId: freshThreadRecord.id,
          resourceId,
          threadId,
        },
      }),
    ).rejects.toThrow(/does not match the target thread/i);
    await expect(retractorMemory.getThreadById({ threadId: mismatchedThreadId })).resolves.toMatchObject({
      title: 'Preserved title',
      metadata: {},
    });

    await expect(
      observerMemory.updateResourceFromObservationalMemory({
        resourceId,
        workingMemory: '{"privateCodename":"ZEPHYR-9"}',
        guard: {
          recordId: staleResourceRecord.id,
          resourceId,
          threadId: null,
        },
      }),
    ).rejects.toThrow(/generation is no longer current/i);
    await expect(
      observerMemory.createReflectionGeneration({
        currentRecord: staleResourceRecord,
        reflection: 'The private codename is ZEPHYR-9.',
        tokenCount: 8,
      }),
    ).rejects.toThrow(/generation is no longer current/i);
    await expect(retractorMemory.getResourceById({ resourceId })).resolves.toMatchObject({
      workingMemory: null,
    });
    await expect(retractorMemory.getObservationalMemory(null, resourceId)).resolves.toMatchObject({
      id: freshResourceRecord.id,
      activeObservations: '',
    });
  });

  it('serializes direct OM clears with guarded derived-state writes', async () => {
    const lockedResourceId = `${resourceId}-clear-lock`;
    const lockedThreadId = `${threadId}-clear-lock`;
    await observerMemory.initializeObservationalMemory({
      config: {},
      resourceId: lockedResourceId,
      scope: 'thread',
      threadId: lockedThreadId,
    });

    const blocker = new Pool({ connectionString });
    const client = await blocker.connect();
    let transactionOpen = false;
    try {
      await client.query('BEGIN');
      transactionOpen = true;
      await client.query('SELECT pg_advisory_xact_lock(hashtextextended($1, 0))', [
        `mastra:observational-memory:${lockedResourceId}`,
      ]);

      const clearPromise = retractorMemory.clearObservationalMemory(lockedThreadId, lockedResourceId);
      const completedWhileLockHeld = await Promise.race([
        clearPromise.then(() => true),
        new Promise<false>(resolve => setTimeout(() => resolve(false), 250)),
      ]);
      expect(completedWhileLockHeld).toBe(false);

      await client.query('COMMIT');
      transactionOpen = false;
      await clearPromise;
      await expect(retractorMemory.getObservationalMemory(lockedThreadId, lockedResourceId)).resolves.toBeNull();
    } finally {
      if (transactionOpen) {
        await client.query('ROLLBACK');
      }
      client.release();
      await blocker.end();
    }
  });

  it('reports every committed resource-scoped generation when deleting a resource', async () => {
    const deletionResourceId = `${resourceId}-delete-resource`;
    const preservedThreadId = `${threadId}-delete-resource`;
    await observerMemory.saveResource({
      resource: {
        id: deletionResourceId,
        workingMemory: '{"privateCodename":"DELETE-ME"}',
        metadata: {},
        createdAt,
        updatedAt: createdAt,
      },
    });
    const initialResourceRecord = await observerMemory.initializeObservationalMemory({
      config: {},
      resourceId: deletionResourceId,
      scope: 'resource',
      threadId: null,
    });
    const currentResourceRecord = await observerMemory.createReflectionGeneration({
      currentRecord: initialResourceRecord,
      reflection: 'Current resource observation.',
      tokenCount: 4,
    });
    const preservedThreadRecord = await observerMemory.initializeObservationalMemory({
      config: {},
      resourceId: deletionResourceId,
      scope: 'thread',
      threadId: preservedThreadId,
    });
    const observationalMemoryRecordIds: string[] = [];

    await retractorMemory.deleteResource({ resourceId: deletionResourceId, observationalMemoryRecordIds });

    expect(new Set(observationalMemoryRecordIds)).toEqual(
      new Set([initialResourceRecord.id, currentResourceRecord.id]),
    );
    await expect(retractorMemory.getResourceById({ resourceId: deletionResourceId })).resolves.toBeNull();
    await expect(retractorMemory.getObservationalMemory(null, deletionResourceId)).resolves.toBeNull();
    await expect(retractorMemory.getObservationalMemory(preservedThreadId, deletionResourceId)).resolves.toMatchObject({
      id: preservedThreadRecord.id,
    });
  });

  it('retracts OM in the same transaction as authoritative message mutations', async () => {
    const mutationResourceId = `${resourceId}-message-mutation`;
    const mutationThreadId = `${threadId}-message-mutation`;
    const updateMessageId = `${threadId}-update-message`;
    const deleteMessageId = `${threadId}-delete-message`;
    await observerMemory.saveResource({
      resource: {
        id: mutationResourceId,
        workingMemory: '{"privateCodename":"ZEPHYR-9"}',
        metadata: {},
        createdAt,
        updatedAt: createdAt,
      },
    });
    await observerMemory.saveThread({
      thread: {
        id: mutationThreadId,
        resourceId: mutationResourceId,
        title: 'Private thread',
        metadata: {},
        createdAt,
        updatedAt: createdAt,
      },
    });
    await observerMemory.saveMessages({
      messages: [
        {
          id: updateMessageId,
          threadId: mutationThreadId,
          resourceId: mutationResourceId,
          role: 'user',
          content: { format: 2, parts: [{ type: 'text', text: 'Private fact' }] },
          createdAt,
        },
        {
          id: deleteMessageId,
          threadId: mutationThreadId,
          resourceId: mutationResourceId,
          role: 'user',
          content: { format: 2, parts: [{ type: 'text', text: 'Delete me' }] },
          createdAt,
        },
      ],
    });
    await observerMemory.initializeObservationalMemory({
      config: { _managedWorkingMemoryScope: 'resource' },
      resourceId: mutationResourceId,
      scope: 'thread',
      threadId: mutationThreadId,
    });

    const blocker = new Pool({ connectionString });
    const client = await blocker.connect();
    let transactionOpen = false;
    try {
      await client.query('BEGIN');
      transactionOpen = true;
      await client.query('SELECT pg_advisory_xact_lock(hashtextextended($1, 0))', [
        `mastra:observational-memory:${mutationResourceId}`,
      ]);

      const updateRetractions: Array<{
        input: { resourceId: string; threadId: string };
        result: { clearedScopes: Array<'resource' | 'thread'> };
      }> = [];
      const updatePromise = retractorMemory.updateMessages({
        messages: [{ id: updateMessageId, content: { content: 'Corrected fact' } }],
        retractObservationalMemory: true,
        observationalMemoryRetractions: updateRetractions as any,
      });
      const completedWhileLockHeld = await Promise.race([
        updatePromise.then(() => true),
        new Promise<false>(resolve => setTimeout(() => resolve(false), 250)),
      ]);
      expect(completedWhileLockHeld).toBe(false);
      expect(updateRetractions).toEqual([]);

      await client.query('COMMIT');
      transactionOpen = false;
      await updatePromise;
      expect(updateRetractions).toMatchObject([
        {
          input: { resourceId: mutationResourceId, threadId: mutationThreadId },
          result: { clearedScopes: ['thread'] },
        },
      ]);
    } finally {
      if (transactionOpen) {
        await client.query('ROLLBACK');
      }
      client.release();
      await blocker.end();
    }

    await expect(retractorMemory.getObservationalMemory(mutationThreadId, mutationResourceId)).resolves.toBeNull();
    await expect(retractorMemory.getResourceById({ resourceId: mutationResourceId })).resolves.toMatchObject({
      workingMemory: null,
    });

    await observerMemory.updateResource({
      resourceId: mutationResourceId,
      workingMemory: '{"privateCodename":"ORION-4"}',
    });
    await observerMemory.initializeObservationalMemory({
      config: { _managedWorkingMemoryScope: 'resource' },
      resourceId: mutationResourceId,
      scope: 'thread',
      threadId: mutationThreadId,
    });
    const deleteRetractions: Array<{
      input: { resourceId: string; threadId: string };
      result: { clearedScopes: Array<'resource' | 'thread'> };
    }> = [];
    await retractorMemory.deleteMessages([deleteMessageId], {
      retractObservationalMemory: true,
      observationalMemoryRetractions: deleteRetractions as any,
    });
    expect(deleteRetractions).toMatchObject([
      {
        input: { resourceId: mutationResourceId, threadId: mutationThreadId },
        result: { clearedScopes: ['thread'] },
      },
    ]);

    await expect(retractorMemory.getObservationalMemory(mutationThreadId, mutationResourceId)).resolves.toBeNull();
    await expect(retractorMemory.getResourceById({ resourceId: mutationResourceId })).resolves.toMatchObject({
      workingMemory: null,
    });
    await expect(retractorMemory.listMessagesById({ messageIds: [deleteMessageId] })).resolves.toEqual({
      messages: [],
    });
  });

  it('preserves externally managed profile memory when OM does not own it', async () => {
    const externalResourceId = `${resourceId}-external`;
    const externalThreadId = `${threadId}-external`;
    await observerMemory.saveResource({
      resource: {
        id: externalResourceId,
        workingMemory: 'Verified profile supplied outside Observational Memory.',
        metadata: { source: 'profile-service' },
        createdAt,
        updatedAt: createdAt,
      },
    });
    await observerMemory.saveThread({
      thread: {
        id: externalThreadId,
        resourceId: externalResourceId,
        title: 'User title',
        metadata: { preserved: true },
        createdAt,
        updatedAt: createdAt,
      },
    });
    await observerMemory.initializeObservationalMemory({
      config: {},
      resourceId: externalResourceId,
      scope: 'resource',
      threadId: null,
    });

    await expect(
      retractorMemory.retractObservationalMemory({
        resourceId: externalResourceId,
        threadId: externalThreadId,
      }),
    ).resolves.toEqual({
      clearedScopes: ['resource'],
      clearedResourceWorkingMemory: false,
      clearedThreadMetadata: false,
    });
    await expect(retractorMemory.getResourceById({ resourceId: externalResourceId })).resolves.toMatchObject({
      workingMemory: 'Verified profile supplied outside Observational Memory.',
      metadata: { source: 'profile-service' },
    });
    await expect(retractorMemory.getThreadById({ threadId: externalThreadId })).resolves.toMatchObject({
      title: 'User title',
      metadata: { preserved: true },
    });
  });

  it('clears observer-managed thread memory and every sibling cursor for resource OM', async () => {
    const sharedResourceId = `${resourceId}-shared-cursors`;
    const sharedThreadIds = [`${threadId}-shared-a`, `${threadId}-shared-b`];
    await observerMemory.saveResource({
      resource: {
        id: sharedResourceId,
        workingMemory: 'Externally managed resource memory',
        metadata: {},
        createdAt,
        updatedAt: createdAt,
      },
    });
    for (const [index, sharedThreadId] of sharedThreadIds.entries()) {
      await observerMemory.saveThread({
        thread: {
          id: sharedThreadId,
          resourceId: sharedResourceId,
          title: `Derived ${index}`,
          metadata: {
            workingMemory: `observer-managed-${index}`,
            mastra: {
              preserved: true,
              om: { threadTitle: `Derived ${index}`, lastObservedMessageId: `message-${index}` },
            },
          },
          createdAt,
          updatedAt: createdAt,
        },
      });
    }
    await observerMemory.initializeObservationalMemory({
      config: { _managedWorkingMemoryScope: 'thread' },
      resourceId: sharedResourceId,
      scope: 'resource',
      threadId: null,
    });

    await expect(
      retractorMemory.retractObservationalMemory({
        resourceId: sharedResourceId,
        threadId: sharedThreadIds[0]!,
      }),
    ).resolves.toMatchObject({
      clearedScopes: ['resource'],
      clearedResourceWorkingMemory: false,
      clearedThreadMetadata: true,
    });
    await expect(retractorMemory.getResourceById({ resourceId: sharedResourceId })).resolves.toMatchObject({
      workingMemory: 'Externally managed resource memory',
    });
    for (const sharedThreadId of sharedThreadIds) {
      const thread = await retractorMemory.getThreadById({ threadId: sharedThreadId });
      expect(thread).toMatchObject({
        title: '',
        metadata: { mastra: { preserved: true } },
      });
      expect(thread?.metadata).not.toHaveProperty('workingMemory');
      expect(thread?.metadata?.mastra).not.toHaveProperty('om');
    }
  });

  it('resolves nullable message resources and retracts both sides of a move', async () => {
    const sourceResourceId = `${resourceId}-move-source`;
    const destinationResourceId = `${resourceId}-move-destination`;
    const sourceThreadId = `${threadId}-move-source`;
    const destinationThreadId = `${threadId}-move-destination`;
    const messageId = `${threadId}-move-message`;

    for (const [moveResourceId, moveThreadId] of [
      [sourceResourceId, sourceThreadId],
      [destinationResourceId, destinationThreadId],
    ] as const) {
      await observerMemory.saveThread({
        thread: {
          id: moveThreadId,
          resourceId: moveResourceId,
          title: moveThreadId,
          metadata: {},
          createdAt,
          updatedAt: createdAt,
        },
      });
      await observerMemory.initializeObservationalMemory({
        config: {},
        resourceId: moveResourceId,
        scope: 'thread',
        threadId: moveThreadId,
      });
    }
    await observerMemory.saveMessages({
      messages: [
        {
          id: messageId,
          threadId: sourceThreadId,
          resourceId: sourceResourceId,
          role: 'user',
          content: { format: 2, parts: [{ type: 'text', text: 'Move me' }] },
          createdAt,
        },
      ],
    });
    await observerStore.db.none(`UPDATE "${schemaName}"."mastra_messages" SET "resourceId" = NULL WHERE id = $1`, [
      messageId,
    ]);

    await retractorMemory.updateMessages({
      messages: [{ id: messageId, threadId: destinationThreadId }],
      retractObservationalMemory: true,
    });

    await expect(retractorMemory.getObservationalMemory(sourceThreadId, sourceResourceId)).resolves.toBeNull();
    await expect(
      retractorMemory.getObservationalMemory(destinationThreadId, destinationResourceId),
    ).resolves.toBeNull();
  });

  it('retracts observer-derived resource state when a thread is deleted directly', async () => {
    const deleteResourceId = `${resourceId}-delete-thread`;
    const deleteThreadId = `${threadId}-delete-thread`;
    await observerMemory.saveResource({
      resource: {
        id: deleteResourceId,
        workingMemory: '{"privateCodename":"ZEPHYR-9"}',
        metadata: {},
        createdAt,
        updatedAt: createdAt,
      },
    });
    await observerMemory.saveThread({
      thread: {
        id: deleteThreadId,
        resourceId: deleteResourceId,
        title: 'Private thread',
        metadata: {},
        createdAt,
        updatedAt: createdAt,
      },
    });
    await observerMemory.initializeObservationalMemory({
      config: { _managedWorkingMemoryScope: 'resource' },
      resourceId: deleteResourceId,
      scope: 'thread',
      threadId: deleteThreadId,
    });

    const retractions: Array<{
      input: { resourceId: string; threadId: string };
      result: { clearedScopes: Array<'resource' | 'thread'> };
    }> = [];
    await retractorMemory.deleteThread({
      threadId: deleteThreadId,
      observationalMemoryRetractions: retractions as any,
    });
    expect(retractions).toMatchObject([
      {
        input: { resourceId: deleteResourceId, threadId: deleteThreadId },
        result: { clearedScopes: ['thread'] },
      },
    ]);

    await expect(retractorMemory.getThreadById({ threadId: deleteThreadId })).resolves.toBeNull();
    await expect(retractorMemory.getObservationalMemory(deleteThreadId, deleteResourceId)).resolves.toBeNull();
    await expect(retractorMemory.getResourceById({ resourceId: deleteResourceId })).resolves.toMatchObject({
      workingMemory: null,
    });
  });
});
