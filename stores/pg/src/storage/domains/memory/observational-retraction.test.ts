import { MessageHistory } from '@mastra/core/processors';
import { ObservationalMemorySourceWriteConflictError } from '@mastra/core/storage';
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

  const readStoredThread = (storedThreadId: string) =>
    observerStore.db.one<{
      title: string;
      metadataText: string;
      updatedAt: Date;
      updatedAtZ: Date;
      rowVersion: string;
    }>(
      `SELECT title,
              metadata::text AS "metadataText",
              "updatedAt",
              "updatedAtZ",
              xmin::text AS "rowVersion"
       FROM "${schemaName}"."mastra_threads"
       WHERE id = $1`,
      [storedThreadId],
    );

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

  it('fences source writes to the archived record and the authoritative thread owner', async () => {
    const guardedResourceId = `${resourceId}-source-write-guard`;
    const guardedThreadId = `${threadId}-source-write-guard`;
    const movedResourceId = `${guardedResourceId}-moved`;
    const guardedThread = {
      id: guardedThreadId,
      resourceId: guardedResourceId,
      title: 'Source guard thread',
      metadata: {},
      createdAt,
      updatedAt: createdAt,
    };
    await observerMemory.saveThread({ thread: guardedThread });
    const predecessor = await observerMemory.initializeObservationalMemory({
      config: {},
      resourceId: guardedResourceId,
      scope: 'thread',
      threadId: guardedThreadId,
    });
    const sourceWriteGuard = {
      recordId: predecessor.id,
      threadId: guardedThreadId,
      resourceId: guardedResourceId,
    };

    await observerMemory.createReflectionGeneration({
      currentRecord: predecessor,
      reflection: 'Archived source guard remains valid.',
      tokenCount: 1,
    });
    await expect(
      observerMemory.saveMessages({
        messages: [
          {
            id: `${guardedThreadId}-archived-source-message`,
            threadId: guardedThreadId,
            resourceId: guardedResourceId,
            role: 'user',
            content: { format: 2, parts: [{ type: 'text', text: 'delayed source write' }] },
            createdAt,
          },
        ],
        observationalMemorySourceWriteGuard: sourceWriteGuard,
      }),
    ).resolves.toMatchObject({ messages: [expect.objectContaining({ threadId: guardedThreadId })] });

    await observerMemory.updateThreadResourceId({ threadId: guardedThreadId, resourceId: movedResourceId });
    const staleMessageId = `${guardedThreadId}-stale-source-message`;
    await expect(
      observerMemory.saveMessages({
        messages: [
          {
            id: staleMessageId,
            threadId: guardedThreadId,
            resourceId: guardedResourceId,
            role: 'user',
            content: { format: 2, parts: [{ type: 'text', text: 'must be rejected' }] },
            createdAt,
          },
        ],
        observationalMemorySourceWriteGuard: sourceWriteGuard,
      }),
    ).rejects.toThrow(/source write guard/i);
    await expect(observerMemory.listMessagesById({ messageIds: [staleMessageId] })).resolves.toEqual({ messages: [] });

    await observerStore.db.none(`DELETE FROM "${schemaName}"."mastra_threads" WHERE id = $1`, [guardedThreadId]);
    const missingThreadMessageId = `${guardedThreadId}-missing-thread-source-message`;
    await expect(
      observerMemory.saveMessages({
        messages: [
          {
            id: missingThreadMessageId,
            threadId: guardedThreadId,
            resourceId: guardedResourceId,
            role: 'user',
            content: { format: 2, parts: [{ type: 'text', text: 'must remain rejected' }] },
            createdAt,
          },
        ],
        observationalMemorySourceWriteGuard: sourceWriteGuard,
      }),
    ).rejects.toThrow(/source write guard/i);
    await expect(observerMemory.listMessagesById({ messageIds: [missingThreadMessageId] })).resolves.toEqual({
      messages: [],
    });
  });

  it('rejects a delayed MessageHistory source write after erasure without recreating the thread or OM', async () => {
    const erasedResourceId = `${resourceId}-delayed-history-writer`;
    const erasedThreadId = `${threadId}-delayed-history-writer`;
    const history = new MessageHistory({ storage: observerMemory, requireObservationalMemorySourceWriteGuard: true });
    const message = (id: string, text: string) => ({
      id,
      threadId: erasedThreadId,
      resourceId: erasedResourceId,
      role: 'user' as const,
      content: { format: 2 as const, parts: [{ type: 'text' as const, text }] },
      createdAt,
    });
    const guardFor = (recordId: string) => ({ recordId, threadId: erasedThreadId, resourceId: erasedResourceId });

    // Execution A captures its record before any source write and creates the thread through it.
    const recordA = await observerMemory.initializeObservationalMemory({
      config: {},
      resourceId: erasedResourceId,
      scope: 'thread',
      threadId: erasedThreadId,
    });
    await history.persistMessages({
      messages: [message(`${erasedThreadId}-a-1`, 'first turn')],
      threadId: erasedThreadId,
      resourceId: erasedResourceId,
      observationalMemorySourceWriteGuard: guardFor(recordA.id),
    });

    // Another store instance erases the conversation while A still holds its token.
    await retractorMemory.retractObservationalMemory({ resourceId: erasedResourceId, threadId: erasedThreadId });
    await retractorMemory.deleteThread({ threadId: erasedThreadId });

    // A's delayed source/marker write must not resurrect the thread, the row, or OM.
    await expect(
      history.persistMessages({
        messages: [message(`${erasedThreadId}-a-1`, 'first turn + late marker')],
        threadId: erasedThreadId,
        resourceId: erasedResourceId,
        observationalMemorySourceWriteGuard: guardFor(recordA.id),
      }),
    ).rejects.toBeInstanceOf(ObservationalMemorySourceWriteConflictError);
    await expect(observerMemory.getThreadById({ threadId: erasedThreadId })).resolves.toBeNull();
    await expect(observerMemory.getObservationalMemory(erasedThreadId, erasedResourceId)).resolves.toBeNull();
    await expect(observerMemory.listMessagesById({ messageIds: [`${erasedThreadId}-a-1`] })).resolves.toEqual({
      messages: [],
    });

    // A fresh execution prepares its own record and may write; A stays revoked.
    const recordB = await observerMemory.initializeObservationalMemory({
      config: {},
      resourceId: erasedResourceId,
      scope: 'thread',
      threadId: erasedThreadId,
    });
    await history.persistMessages({
      messages: [message(`${erasedThreadId}-b-1`, 'new conversation')],
      threadId: erasedThreadId,
      resourceId: erasedResourceId,
      observationalMemorySourceWriteGuard: guardFor(recordB.id),
    });
    await expect(
      history.persistMessages({
        messages: [message(`${erasedThreadId}-a-2`, 'stale after reuse')],
        threadId: erasedThreadId,
        resourceId: erasedResourceId,
        observationalMemorySourceWriteGuard: guardFor(recordA.id),
      }),
    ).rejects.toBeInstanceOf(ObservationalMemorySourceWriteConflictError);
    const stored = await observerMemory.listMessagesById({
      messageIds: [`${erasedThreadId}-a-1`, `${erasedThreadId}-a-2`, `${erasedThreadId}-b-1`],
    });
    expect(stored.messages.map(storedMessage => storedMessage.id)).toEqual([`${erasedThreadId}-b-1`]);
  });

  it('atomically validates source-bound thread creation and active-record refresh', async () => {
    const guardedResourceId = `${resourceId}-atomic-source-guard`;
    const guardedThreadId = `${threadId}-atomic-source-guard`;
    const sourceRecord = await observerMemory.initializeObservationalMemory({
      config: {},
      resourceId: guardedResourceId,
      scope: 'thread',
      threadId: guardedThreadId,
    });
    const sourceWriteGuard = {
      recordId: sourceRecord.id,
      threadId: guardedThreadId,
      resourceId: guardedResourceId,
    };

    await expect(
      observerMemory.saveThread({
        thread: {
          id: guardedThreadId,
          resourceId: guardedResourceId,
          title: 'first turn',
          metadata: {},
          createdAt,
          updatedAt: createdAt,
        },
        observationalMemorySourceWriteGuard: sourceWriteGuard,
      }),
    ).resolves.toMatchObject({ id: guardedThreadId, title: 'first turn' });

    const activeRecord = await observerMemory.createReflectionGeneration({
      currentRecord: sourceRecord,
      reflection: 'active generation',
      tokenCount: 1,
    });
    await expect(
      observerMemory.getObservationalMemoryForSourceWrite({
        threadId: guardedThreadId,
        resourceId: guardedResourceId,
        sourceWriteGuard,
      }),
    ).resolves.toMatchObject({ id: activeRecord.id });

    await retractorMemory.retractObservationalMemory({ resourceId: guardedResourceId, threadId: guardedThreadId });
    await retractorMemory.deleteThread({ threadId: guardedThreadId });
    await expect(
      observerMemory.saveThread({
        thread: {
          id: guardedThreadId,
          resourceId: guardedResourceId,
          title: 'erased title',
          metadata: { erased: true },
          createdAt,
          updatedAt: createdAt,
        },
        observationalMemorySourceWriteGuard: sourceWriteGuard,
      }),
    ).rejects.toThrow(/source write guard/i);
    await expect(observerMemory.getThreadById({ threadId: guardedThreadId })).resolves.toBeNull();
    await expect(
      observerMemory.getObservationalMemoryForSourceWrite({
        threadId: guardedThreadId,
        resourceId: guardedResourceId,
        sourceWriteGuard,
      }),
    ).rejects.toThrow(/source write guard/i);
  });

  it('rejects mismatched thread ownership before initializing a source-write record', async () => {
    const guardedThreadId = `${threadId}-initial-owner`;
    const ownerResourceId = `${resourceId}-initial-owner`;
    const mismatchedResourceId = `${resourceId}-initial-mismatched`;
    await observerMemory.saveThread({
      thread: {
        id: guardedThreadId,
        resourceId: ownerResourceId,
        title: 'owned thread',
        metadata: {},
        createdAt,
        updatedAt: createdAt,
      },
    });

    await expect(
      observerMemory.initializeObservationalMemory({
        config: {},
        resourceId: mismatchedResourceId,
        scope: 'thread',
        threadId: guardedThreadId,
      }),
    ).rejects.toThrow('does not own the thread');
    await expect(observerMemory.getObservationalMemory(guardedThreadId, mismatchedResourceId)).resolves.toBeNull();

    await expect(
      observerMemory.initializeObservationalMemory({
        config: {},
        resourceId: ownerResourceId,
        scope: 'thread',
        threadId: guardedThreadId,
      }),
    ).resolves.toMatchObject({ threadId: guardedThreadId, resourceId: ownerResourceId });
  });

  it('rejects a guarded whole-row save when an ungoverned thread owner changed', async () => {
    const guardedResourceId = `${resourceId}-whole-row-owner-original`;
    const movedResourceId = `${resourceId}-whole-row-owner-moved`;
    const guardedThreadId = `${threadId}-whole-row-owner`;
    await observerMemory.saveThread({
      thread: {
        id: guardedThreadId,
        resourceId: guardedResourceId,
        title: 'original',
        metadata: {},
        createdAt,
        updatedAt: createdAt,
      },
    });
    const sourceRecord = await observerMemory.initializeObservationalMemory({
      config: {},
      resourceId: guardedResourceId,
      scope: 'thread',
      threadId: guardedThreadId,
    });
    await observerMemory.updateThreadResourceId({ threadId: guardedThreadId, resourceId: movedResourceId });

    await expect(
      observerMemory.mutateThreadWithWorkingMemory({
        mutation: {
          type: 'save',
          thread: {
            id: guardedThreadId,
            resourceId: guardedResourceId,
            title: 'stale whole-row save',
            metadata: {},
            createdAt,
            updatedAt: createdAt,
          },
        },
        workingMemory: { type: 'require-ungoverned' },
        observationalMemorySourceWriteGuard: {
          recordId: sourceRecord.id,
          threadId: guardedThreadId,
          resourceId: guardedResourceId,
        },
      }),
    ).rejects.toThrow('guarded thread owner changed');
    await expect(observerMemory.getThreadById({ threadId: guardedThreadId })).resolves.toMatchObject({
      resourceId: movedResourceId,
      title: 'original',
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

  it('clears observer working memory of an unmarked record inside the deletion when the caller manages it', async () => {
    const policyResourceId = `${resourceId}-policy-managed`;
    const policyThreadId = `${threadId}-policy-managed`;
    const deletedMessageId = `${policyThreadId}-deleted`;
    const keptMessageId = `${policyThreadId}-kept`;
    await observerMemory.saveResource({
      resource: { id: policyResourceId, workingMemory: null as any, metadata: {}, createdAt, updatedAt: createdAt },
    });
    await observerMemory.saveThread({
      thread: {
        id: policyThreadId,
        resourceId: policyResourceId,
        title: '',
        metadata: {},
        createdAt,
        updatedAt: createdAt,
      },
    });
    await observerMemory.saveMessages({
      messages: [deletedMessageId, keptMessageId].map(id => ({
        id,
        threadId: policyThreadId,
        resourceId: policyResourceId,
        role: 'user' as const,
        content: { format: 2 as const, parts: [{ type: 'text' as const, text: id }] },
        createdAt,
      })),
    });
    // Written before the managed-scope marker existed: no `_managedWorkingMemoryScope`.
    const record = await observerMemory.initializeObservationalMemory({
      config: {},
      resourceId: policyResourceId,
      scope: 'resource',
      threadId: null,
    });
    const guard = { recordId: record.id, threadId: null, resourceId: policyResourceId };
    // The observer learned the deleted message's fact under the record's generation.
    await observerMemory.updateResourceFromObservationalMemory({
      resourceId: policyResourceId,
      workingMemory: '{"codeword":"KESTREL"}',
      guard,
    });

    // The deletion waits on the OM resource lock, and a second observer write
    // queues behind it: the write that landed before the deletion is cleared
    // by it, and the one after it is fenced off. Each wait is observed in
    // pg_stat_activity before the next step.
    const runId = Date.now();
    const deleterName = `pf5257-deleter-${runId}`;
    const writerName = `pf5257-writer-${runId}`;
    const namedStore = async (id: string, applicationName: string) => {
      const url = new URL(connectionString);
      url.searchParams.set('application_name', applicationName);
      const store = new PostgresStore({ id, connectionString: url.toString(), disableInit: true, schemaName });
      await store.init();
      return { store, memory: (await store.getStore('memory')) as MemoryPG };
    };
    const deleter = await namedStore('om-retraction-deleter', deleterName);
    const writer = await namedStore('om-retraction-writer', writerName);
    const inspector = new Pool({ connectionString });
    const blocker = new Pool({ connectionString });
    const waitForAdvisoryLockWait = async (applicationName: string) => {
      const deadline = Date.now() + 5_000;
      while (Date.now() < deadline) {
        const result = await inspector.query<{ waiting: boolean }>(
          `SELECT EXISTS (
             SELECT 1 FROM pg_stat_activity
             WHERE application_name = $1 AND wait_event_type = 'Lock'
               AND query LIKE '%pg_advisory_xact_lock%'
           ) AS waiting`,
          [applicationName],
        );
        if (result.rows[0]?.waiting) return;
        await new Promise(resolve => setTimeout(resolve, 20));
      }
      throw new Error(`Timed out waiting for ${applicationName} to block on the OM resource lock.`);
    };
    const client = await blocker.connect();
    let transactionOpen = false;
    const receipts: Array<{ result: { clearedScopes: string[]; clearedResourceWorkingMemory: boolean } }> = [];
    let lateWrite: Promise<unknown> | undefined;
    let deletion: Promise<void> | undefined;
    try {
      await client.query('BEGIN');
      transactionOpen = true;
      await client.query('SELECT pg_advisory_xact_lock(hashtextextended($1, 0))', [
        `mastra:observational-memory:${policyResourceId}`,
      ]);
      deletion = deleter.memory.deleteMessages([deletedMessageId], {
        retractObservationalMemory: true,
        managedWorkingMemoryScope: 'resource',
        observationalMemoryRetractions: receipts as any,
      });
      await waitForAdvisoryLockWait(deleterName);
      lateWrite = writer.memory
        .updateResourceFromObservationalMemory({
          resourceId: policyResourceId,
          workingMemory: '{"codeword":"KESTREL","resurrected":true}',
          guard,
        })
        .then(
          () => 'applied',
          (error: unknown) => error,
        );
      await waitForAdvisoryLockWait(writerName);
      expect(receipts).toEqual([]);
      await client.query('COMMIT');
      transactionOpen = false;
      await deletion;
    } finally {
      if (transactionOpen) await client.query('ROLLBACK');
      client.release();
      await deletion?.catch(() => {});
      await lateWrite;
      await Promise.allSettled([blocker.end(), deleter.store.close(), writer.store.close(), inspector.end()]);
    }
    // The late observer write names the retracted generation and is refused.
    const refused = await lateWrite;
    expect((refused as { cause?: { name?: string } } | undefined)?.cause?.name).toBe(
      'ObservationalMemoryGenerationConflictError',
    );

    expect(receipts).toMatchObject([{ result: { clearedScopes: ['resource'], clearedResourceWorkingMemory: true } }]);
    await expect(retractorMemory.getResourceById({ resourceId: policyResourceId })).resolves.toMatchObject({
      workingMemory: null,
    });
    await expect(retractorMemory.getObservationalMemory(null, policyResourceId)).resolves.toBeNull();
    await expect(retractorMemory.listMessagesById({ messageIds: [deletedMessageId] })).resolves.toEqual({
      messages: [],
    });
    await expect(retractorMemory.listMessagesById({ messageIds: [keptMessageId] })).resolves.toMatchObject({
      messages: [{ id: keptMessageId }],
    });

    // Without the caller's policy an unmarked record keeps the stricter
    // default: working memory it does not provably own stays.
    await observerMemory.updateResource({ resourceId: policyResourceId, workingMemory: 'Profile from elsewhere.' });
    await observerMemory.initializeObservationalMemory({
      config: {},
      resourceId: policyResourceId,
      scope: 'resource',
      threadId: null,
    });
    await retractorMemory.deleteMessages([keptMessageId], { retractObservationalMemory: true });
    await expect(retractorMemory.getResourceById({ resourceId: policyResourceId })).resolves.toMatchObject({
      workingMemory: 'Profile from elsewhere.',
    });
    await expect(retractorMemory.getObservationalMemory(null, policyResourceId)).resolves.toBeNull();
  });

  it('applies a thread-scope policy to an unmarked resource record across the resource, and never to a not-managed marker', async () => {
    const scopeResourceId = `${resourceId}-policy-thread-scope`;
    const threads = [`${threadId}-policy-thread-a`, `${threadId}-policy-thread-b`];
    await observerMemory.saveResource({
      resource: {
        id: scopeResourceId,
        workingMemory: 'Resource profile.',
        metadata: {},
        createdAt,
        updatedAt: createdAt,
      },
    });
    for (const id of threads) {
      await observerMemory.saveThread({
        thread: {
          id,
          resourceId: scopeResourceId,
          title: '',
          metadata: { workingMemory: `Observed in ${id}.`, preserved: true },
          createdAt,
          updatedAt: createdAt,
        },
      });
    }
    // An explicit "not managed" marker is never overridden by the policy.
    await observerMemory.initializeObservationalMemory({
      config: { _managedWorkingMemoryScope: null },
      resourceId: scopeResourceId,
      scope: 'resource',
      threadId: null,
    });
    await expect(
      retractorMemory.retractObservationalMemory({
        resourceId: scopeResourceId,
        threadId: threads[0]!,
        managedWorkingMemoryScope: 'thread',
      }),
    ).resolves.toMatchObject({ clearedScopes: ['resource'], clearedResourceWorkingMemory: false });
    for (const id of threads) {
      await expect(retractorMemory.getThreadById({ threadId: id })).resolves.toMatchObject({
        metadata: { workingMemory: `Observed in ${id}.` },
      });
    }

    // A record written before the marker existed takes the thread-scope policy:
    // every thread of the resource loses its observer working memory, the
    // resource document stays.
    await observerMemory.initializeObservationalMemory({
      config: {},
      resourceId: scopeResourceId,
      scope: 'resource',
      threadId: null,
    });
    await expect(
      retractorMemory.retractObservationalMemory({
        resourceId: scopeResourceId,
        threadId: threads[0]!,
        managedWorkingMemoryScope: 'thread',
      }),
    ).resolves.toMatchObject({
      clearedScopes: ['resource'],
      clearedResourceWorkingMemory: false,
      clearedThreadMetadata: true,
    });
    for (const id of threads) {
      const thread = await retractorMemory.getThreadById({ threadId: id });
      expect(thread?.metadata).toMatchObject({ preserved: true });
      expect(thread?.metadata).not.toHaveProperty('workingMemory');
    }
    await expect(retractorMemory.getResourceById({ resourceId: scopeResourceId })).resolves.toMatchObject({
      workingMemory: 'Resource profile.',
    });
  });

  describe('a not-managed (null-marked) record', () => {
    let caseNumber = 0;
    const setup = async () => {
      caseNumber += 1;
      const caseResourceId = `${resourceId}-null-marker-${caseNumber}`;
      const caseThreadId = `${threadId}-null-marker-${caseNumber}`;
      await observerMemory.saveThread({
        thread: {
          id: caseThreadId,
          resourceId: caseResourceId,
          title: '',
          metadata: { workingMemory: 'Thread profile from elsewhere.' },
          createdAt,
          updatedAt: createdAt,
        },
      });
      // Created while the observer did not manage working memory; the policy has since changed.
      const record = await observerMemory.initializeObservationalMemory({
        config: { _managedWorkingMemoryScope: null },
        resourceId: caseResourceId,
        scope: 'resource',
        threadId: null,
      });
      const guard = { recordId: record.id, threadId: null, resourceId: caseResourceId };
      const marker = async () =>
        ((await retractorMemory.getObservationalMemory(null, caseResourceId))?.config as Record<string, unknown>)
          ._managedWorkingMemoryScope;
      return { resourceId: caseResourceId, threadId: caseThreadId, guard, marker };
    };
    type Case = Awaited<ReturnType<typeof setup>>;

    it.each([
      [
        'a guarded resource write',
        async ({ resourceId: id, guard }: Case) =>
          observerMemory.updateResourceFromObservationalMemory({
            resourceId: id,
            workingMemory: 'FROM-DELETED-MSG',
            guard,
          }),
      ],
      [
        'a guarded revisioned update',
        async ({ resourceId: id, guard }: Case) => {
          const current = await observerMemory.getWorkingMemorySnapshot({ scope: 'resource', resourceId: id });
          await observerMemory.applyWorkingMemoryUpdate({
            scope: 'resource',
            resourceId: id,
            value: 'FROM-DELETED-MSG',
            expectedRevision: current.revision,
            source: 'observer',
            observationalMemoryGuard: guard,
          });
        },
      ],
    ])('is stamped managed by %s, so a retraction clears what it wrote', async (_name, write) => {
      const context = await setup();
      await write(context);
      await expect(context.marker()).resolves.toBe('resource');
      await expect(
        retractorMemory.retractObservationalMemory({ resourceId: context.resourceId, threadId: context.threadId }),
      ).resolves.toMatchObject({ clearedScopes: ['resource'], clearedResourceWorkingMemory: true });
      await expect(retractorMemory.getResourceById({ resourceId: context.resourceId })).resolves.toMatchObject({
        workingMemory: null,
      });
    });

    it.each([
      [
        'a rejected guarded update (stale revision)',
        async ({ resourceId: id, guard }: Case) => {
          await observerMemory.updateResource({ resourceId: id, workingMemory: 'Profile from elsewhere.' });
          await expect(
            observerMemory.applyWorkingMemoryUpdate({
              scope: 'resource',
              resourceId: id,
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
        async ({ resourceId: id, threadId: tid, guard }: Case) => {
          await observerMemory.updateResource({ resourceId: id, workingMemory: 'Profile from elsewhere.' });
          await observerMemory.updateThreadFromObservationalMemory({
            id: tid,
            metadata: { workingMemory: 'Thread profile from elsewhere.', mastra: { om: { threadTitle: 'T' } } },
            guard,
          });
        },
      ],
      [
        'a guarded resource write that changes nothing',
        async ({ resourceId: id, guard }: Case) => {
          await observerMemory.updateResource({ resourceId: id, workingMemory: 'Profile from elsewhere.' });
          await observerMemory.updateResourceFromObservationalMemory({
            resourceId: id,
            workingMemory: 'Profile from elsewhere.',
            guard,
          });
        },
      ],
      [
        'a rejected guarded resource write (revisioned working memory)',
        async ({ resourceId: id, guard }: Case) => {
          await observerMemory.applyWorkingMemoryUpdate({
            scope: 'resource',
            resourceId: id,
            value: 'Profile from elsewhere.',
            expectedRevision: 0,
            source: 'owner',
          });
          await expect(
            observerMemory.updateResourceFromObservationalMemory({
              resourceId: id,
              workingMemory: 'FROM-DELETED-MSG',
              guard,
            }),
          ).rejects.toThrow();
        },
      ],
    ])('stays not managed after %s, and its retraction keeps memory it never wrote', async (_name, write) => {
      const context = await setup();
      await write(context);
      await expect(context.marker()).resolves.toBeNull();
      await retractorMemory.retractObservationalMemory({
        resourceId: context.resourceId,
        threadId: context.threadId,
        managedWorkingMemoryScope: 'resource',
      });
      await expect(retractorMemory.getResourceById({ resourceId: context.resourceId })).resolves.toMatchObject({
        workingMemory: 'Profile from elsewhere.',
      });
      await expect(retractorMemory.getThreadById({ threadId: context.threadId })).resolves.toMatchObject({
        metadata: expect.objectContaining({ workingMemory: 'Thread profile from elsewhere.' }),
      });
    });

    it('is stamped thread-managed by a guarded thread-scope update, so a retraction clears every thread profile', async () => {
      const context = await setup();
      const current = await observerMemory.getWorkingMemorySnapshot({
        scope: 'thread',
        resourceId: context.resourceId,
        threadId: context.threadId,
      });
      await observerMemory.applyWorkingMemoryUpdate({
        scope: 'thread',
        resourceId: context.resourceId,
        threadId: context.threadId,
        value: 'FROM-DELETED-MSG',
        expectedRevision: current.revision,
        source: 'observer',
        observationalMemoryGuard: context.guard,
      });
      await expect(context.marker()).resolves.toBe('thread');
      await retractorMemory.retractObservationalMemory({ resourceId: context.resourceId, threadId: context.threadId });
      const thread = await retractorMemory.getThreadById({ threadId: context.threadId });
      expect(thread?.metadata).not.toHaveProperty('workingMemory');
    });
  });

  describe('concurrency of the not-managed marker stamp', () => {
    const lockWaitOf = async (inspector: Pool, applicationName: string) => {
      const deadline = Date.now() + 5_000;
      while (Date.now() < deadline) {
        const result = await inspector.query<{ waiting: boolean }>(
          `SELECT EXISTS (
             SELECT 1 FROM pg_stat_activity WHERE application_name = $1 AND wait_event_type = 'Lock'
           ) AS waiting`,
          [applicationName],
        );
        if (result.rows[0]?.waiting) return;
        await new Promise(resolve => setTimeout(resolve, 20));
      }
      throw new Error(`Timed out waiting for ${applicationName} to block on a row lock.`);
    };
    // Every store connection here gets a server-side lock_timeout, so a lock
    // wait the test does not release fails fast with 55P03 instead of hanging.
    const namedMemory = async (id: string, applicationName: string, lockTimeoutMs = 10_000) => {
      const url = new URL(connectionString);
      url.searchParams.set('application_name', applicationName);
      const store = new PostgresStore({
        id,
        connectionString: url.toString(),
        disableInit: true,
        schemaName,
        lock_timeout: lockTimeoutMs,
      });
      await store.init();
      return { store, memory: (await store.getStore('memory')) as MemoryPG };
    };
    const seedMarked = async (suffix: string, scope: 'resource' | 'thread', marker: 'resource' | null = null) => {
      const caseResourceId = `${resourceId}-stamp-concurrency-${suffix}`;
      const caseThreadId = scope === 'thread' ? `${threadId}-stamp-concurrency-${suffix}` : null;
      await observerMemory.saveResource({
        resource: { id: caseResourceId, workingMemory: 'Before.', metadata: {}, createdAt, updatedAt: createdAt },
      });
      if (caseThreadId) {
        await observerMemory.saveThread({
          thread: {
            id: caseThreadId,
            resourceId: caseResourceId,
            title: '',
            metadata: {},
            createdAt,
            updatedAt: createdAt,
          },
        });
      }
      const record = await observerMemory.initializeObservationalMemory({
        config: { _managedWorkingMemoryScope: marker, keep: 'unrelated' },
        resourceId: caseResourceId,
        scope,
        threadId: caseThreadId,
      });
      return {
        resourceId: caseResourceId,
        threadId: caseThreadId,
        record,
        guard: { recordId: record.id, threadId: caseThreadId, resourceId: caseResourceId },
      };
    };
    type Seeded = Awaited<ReturnType<typeof seedMarked>>;
    // Each guarded write, prepared (snapshot read) before the OM row is blocked.
    const guardedWrites = {
      'updateResourceFromObservationalMemory (legacy)':
        async (memory: MemoryPG, { resourceId: id, guard }: Seeded) =>
        () =>
          memory.updateResourceFromObservationalMemory({ resourceId: id, workingMemory: 'FROM-DELETED-MSG', guard }),
      'applyWorkingMemoryUpdate, resource scope': async (memory: MemoryPG, { resourceId: id, guard }: Seeded) => {
        const { revision } = await memory.getWorkingMemorySnapshot({ scope: 'resource', resourceId: id });
        return () =>
          memory.applyWorkingMemoryUpdate({
            scope: 'resource',
            resourceId: id,
            value: 'FROM-DELETED-MSG',
            expectedRevision: revision,
            source: 'observer',
            observationalMemoryGuard: guard,
          });
      },
      'applyWorkingMemoryUpdate, thread scope': async (
        memory: MemoryPG,
        { resourceId: id, threadId: tid, guard }: Seeded,
      ) => {
        const { revision } = await memory.getWorkingMemorySnapshot({ scope: 'thread', resourceId: id, threadId: tid! });
        return () =>
          memory.applyWorkingMemoryUpdate({
            scope: 'thread',
            resourceId: id,
            threadId: tid!,
            value: 'FROM-DELETED-MSG',
            expectedRevision: revision,
            source: 'observer',
            observationalMemoryGuard: guard,
          });
      },
    } as const;

    it.each([
      ['updateResourceFromObservationalMemory (legacy)', 'resource'],
      ['applyWorkingMemoryUpdate, resource scope', 'resource'],
      ['applyWorkingMemoryUpdate, thread scope', 'thread'],
    ] as const)('locks the OM row before the target row in a guarded write: %s', async (name, scope) => {
      const seeded = await seedMarked(`lock-order-${name.replace(/\W+/g, '-')}`, scope);
      const writerName = `pf5257-stamp-writer-${Date.now()}`;
      const writer = await namedMemory('stamp-lock-order-writer', writerName);
      const inspector = new Pool({ connectionString });
      const blocker = await inspector.connect();
      const probe = await inspector.connect();
      const targetTable = scope === 'thread' ? 'mastra_threads' : 'mastra_resources';
      const targetId = scope === 'thread' ? seeded.threadId : seeded.resourceId;
      let write: Promise<unknown> | undefined;
      try {
        const start = await guardedWrites[name](writer.memory, seeded);
        await blocker.query('BEGIN');
        await blocker.query(`SELECT 1 FROM "${schemaName}"."mastra_observational_memory" WHERE id = $1 FOR UPDATE`, [
          seeded.record.id,
        ]);
        write = start().catch((error: unknown) => error);
        await lockWaitOf(inspector, writerName);
        // Waiting on the OM row, the write holds no lock on its target row yet.
        await probe.query('BEGIN');
        await expect(
          probe.query(`SELECT 1 FROM "${schemaName}"."${targetTable}" WHERE id = $1 FOR UPDATE NOWAIT`, [targetId]),
        ).resolves.toMatchObject({ rowCount: 1 });
        await probe.query('ROLLBACK');
        await blocker.query('COMMIT');
        await expect(write).resolves.not.toBeInstanceOf(Error);
      } finally {
        await blocker.query('ROLLBACK').catch(() => {});
        await probe.query('ROLLBACK').catch(() => {});
        blocker.release();
        probe.release();
        await write;
        await Promise.allSettled([writer.store.close(), inspector.end()]);
      }
      const stored =
        scope === 'thread'
          ? (await retractorMemory.getThreadById({ threadId: seeded.threadId! }))?.metadata?.workingMemory
          : (await retractorMemory.getResourceById({ resourceId: seeded.resourceId }))?.workingMemory;
      expect(stored).toBe('FROM-DELETED-MSG');
      await expect(retractorMemory.getObservationalMemory(seeded.threadId, seeded.resourceId)).resolves.toMatchObject({
        config: expect.objectContaining({ _managedWorkingMemoryScope: scope }),
      });
    });

    it('leaves the OM row of an already-marked record unlocked during a guarded write', async () => {
      const seeded = await seedMarked('marked-no-lock', 'resource', 'resource');
      // lock_timeout counts only time spent waiting on a lock, so this bound
      // does not depend on runner load: a write that queued behind the OM-row
      // holder fails with 55P03, one that never waits succeeds.
      const writer = await namedMemory('stamp-marked-writer', `pf5324-marked-writer-${Date.now()}`, 2_000);
      const inspector = new Pool({ connectionString });
      const blocker = await inspector.connect();
      try {
        const start = await guardedWrites['applyWorkingMemoryUpdate, resource scope'](writer.memory, seeded);
        await blocker.query('BEGIN');
        await blocker.query(`SELECT 1 FROM "${schemaName}"."mastra_observational_memory" WHERE id = $1 FOR UPDATE`, [
          seeded.record.id,
        ]);
        // The record is never stamped, so the write must not queue behind an OM-row holder.
        await expect(start()).resolves.toMatchObject({ value: 'FROM-DELETED-MSG' });
      } finally {
        await blocker.query('ROLLBACK').catch(() => {});
        blocker.release();
        await Promise.allSettled([writer.store.close(), inspector.end()]);
      }
      await expect(retractorMemory.getResourceById({ resourceId: seeded.resourceId })).resolves.toMatchObject({
        workingMemory: 'FROM-DELETED-MSG',
      });
    });

    it('keeps a stamp that commits during a concurrent config merge, and the merged fields', async () => {
      const { resourceId: id, record } = await seedMarked('config-merge', 'resource');
      const mergerName = `pf5257-config-merger-${Date.now()}`;
      const merger = await namedMemory('stamp-config-merger', mergerName);
      const inspector = new Pool({ connectionString });
      const blocker = await inspector.connect();
      let merge: Promise<unknown> | undefined;
      try {
        // The stamp's transaction holds the OM row while the merge starts.
        await blocker.query('BEGIN');
        await blocker.query(`SELECT 1 FROM "${schemaName}"."mastra_observational_memory" WHERE id = $1 FOR UPDATE`, [
          record.id,
        ]);
        merge = merger.memory.updateObservationalMemoryConfig({ id: record.id, config: { merged: true } });
        await lockWaitOf(inspector, mergerName);
        await blocker.query(
          `UPDATE "${schemaName}"."mastra_observational_memory"
           SET config = jsonb_set(config::jsonb, '{_managedWorkingMemoryScope}', to_jsonb('resource'::text))
           WHERE id = $1`,
          [record.id],
        );
        await blocker.query('COMMIT');
        await merge;
      } finally {
        await blocker.query('ROLLBACK').catch(() => {});
        blocker.release();
        await merge?.catch(() => {});
        await Promise.allSettled([merger.store.close(), inspector.end()]);
      }
      await expect(retractorMemory.getObservationalMemory(null, id)).resolves.toMatchObject({
        config: expect.objectContaining({ _managedWorkingMemoryScope: 'resource', keep: 'unrelated', merged: true }),
      });
    });
  });

  it('forwards the policy through an atomic thread deletion', async () => {
    const deleteResourceId = `${resourceId}-policy-thread-delete`;
    const deleteThreadId = `${threadId}-policy-thread-delete`;
    await observerMemory.saveResource({
      resource: {
        id: deleteResourceId,
        workingMemory: 'Learned from the deleted thread.',
        metadata: {},
        createdAt,
        updatedAt: createdAt,
      },
    });
    await observerMemory.saveThread({
      thread: {
        id: deleteThreadId,
        resourceId: deleteResourceId,
        title: '',
        metadata: {},
        createdAt,
        updatedAt: createdAt,
      },
    });
    // Written before the marker existed.
    await observerMemory.initializeObservationalMemory({
      config: {},
      resourceId: deleteResourceId,
      scope: 'resource',
      threadId: null,
    });
    const receipts: Array<{ result: { clearedResourceWorkingMemory: boolean } }> = [];
    await retractorMemory.deleteThread({
      threadId: deleteThreadId,
      managedWorkingMemoryScope: 'resource',
      observationalMemoryRetractions: receipts as any,
    });
    expect(receipts).toMatchObject([{ result: { clearedResourceWorkingMemory: true } }]);
    await expect(retractorMemory.getResourceById({ resourceId: deleteResourceId })).resolves.toMatchObject({
      workingMemory: null,
    });
  });

  it('forwards the policy through an atomic message edit', async () => {
    const editResourceId = `${resourceId}-policy-edit`;
    const editThreadId = `${threadId}-policy-edit`;
    const editMessageId = `${editThreadId}-message`;
    await observerMemory.saveResource({
      resource: {
        id: editResourceId,
        workingMemory: 'Learned from the edited message.',
        metadata: {},
        createdAt,
        updatedAt: createdAt,
      },
    });
    await observerMemory.saveThread({
      thread: {
        id: editThreadId,
        resourceId: editResourceId,
        title: '',
        metadata: {},
        createdAt,
        updatedAt: createdAt,
      },
    });
    await observerMemory.saveMessages({
      messages: [
        {
          id: editMessageId,
          threadId: editThreadId,
          resourceId: editResourceId,
          role: 'user',
          content: { format: 2, parts: [{ type: 'text', text: 'Original fact' }] },
          createdAt,
        },
      ],
    });
    // Written before the marker existed.
    await observerMemory.initializeObservationalMemory({
      config: {},
      resourceId: editResourceId,
      scope: 'resource',
      threadId: null,
    });
    const receipts: Array<{ result: { clearedResourceWorkingMemory: boolean } }> = [];
    await retractorMemory.updateMessages({
      messages: [{ id: editMessageId, content: { content: 'Corrected fact' } }],
      retractObservationalMemory: true,
      managedWorkingMemoryScope: 'resource',
      observationalMemoryRetractions: receipts as any,
    });
    expect(receipts).toMatchObject([{ result: { clearedResourceWorkingMemory: true } }]);
    await expect(retractorMemory.getResourceById({ resourceId: editResourceId })).resolves.toMatchObject({
      workingMemory: null,
    });
  });

  it('keeps the source-write successor when a policy-managed retraction clears observer working memory', async () => {
    const successorResourceId = `${resourceId}-policy-successor`;
    const successorThreadId = `${threadId}-policy-successor`;
    await observerMemory.saveResource({
      resource: {
        id: successorResourceId,
        workingMemory: '{"codeword":"ORION"}',
        metadata: {},
        createdAt,
        updatedAt: createdAt,
      },
    });
    await observerMemory.saveThread({
      thread: {
        id: successorThreadId,
        resourceId: successorResourceId,
        title: '',
        metadata: {},
        createdAt,
        updatedAt: createdAt,
      },
    });
    const predecessor = await observerMemory.initializeObservationalMemory({
      config: {},
      resourceId: successorResourceId,
      scope: 'thread',
      threadId: successorThreadId,
    });
    const predecessorGuard = { recordId: predecessor.id, threadId: successorThreadId, resourceId: successorResourceId };
    const successorRecordId = `${predecessor.id}-successor`;
    const retraction = await retractorMemory.retractObservationalMemory({
      resourceId: successorResourceId,
      threadId: successorThreadId,
      managedWorkingMemoryScope: 'resource',
      sourceWriteSuccessor: { predecessor: predecessorGuard, successorRecordId },
    });
    expect(retraction).toMatchObject({
      clearedScopes: ['thread'],
      clearedResourceWorkingMemory: true,
      sourceWriteGuard: { recordId: successorRecordId },
    });
    await expect(retractorMemory.getResourceById({ resourceId: successorResourceId })).resolves.toMatchObject({
      workingMemory: null,
    });
    // The successor survives the retraction and is the generation a source
    // write now resolves.
    await expect(
      retractorMemory.getObservationalMemoryForSourceWrite({
        threadId: successorThreadId,
        resourceId: successorResourceId,
        sourceWriteGuard: { recordId: successorRecordId, threadId: successorThreadId, resourceId: successorResourceId },
      }),
    ).resolves.toMatchObject({ id: successorRecordId });
    // A replay finds the successor and clears nothing again.
    await observerMemory.updateResource({ resourceId: successorResourceId, workingMemory: 'Learned since.' });
    await expect(
      retractorMemory.retractObservationalMemory({
        resourceId: successorResourceId,
        threadId: successorThreadId,
        managedWorkingMemoryScope: 'resource',
        sourceWriteSuccessor: { predecessor: predecessorGuard, successorRecordId },
      }),
    ).resolves.toMatchObject({
      clearedResourceWorkingMemory: false,
      sourceWriteGuard: { recordId: successorRecordId },
    });
    await expect(retractorMemory.getResourceById({ resourceId: successorResourceId })).resolves.toMatchObject({
      workingMemory: 'Learned since.',
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

  it('does not rewrite formatted owner-only non-root protected memory during managed OM retraction', async () => {
    const protectedResourceId = `${resourceId}-owner-only-noop`;
    const protectedThreadId = `${threadId}-owner-only-noop`;
    await observerMemory.saveThread({
      thread: {
        id: protectedThreadId,
        resourceId: protectedResourceId,
        title: 'Owner title',
        metadata: { preserved: true },
        createdAt,
        updatedAt: createdAt,
      },
    });
    await observerMemory.initializeObservationalMemory({
      config: { _managedWorkingMemoryScope: 'thread' },
      resourceId: protectedResourceId,
      scope: 'thread',
      threadId: protectedThreadId,
    });
    const initialOwner = await observerMemory.applyWorkingMemoryUpdate({
      scope: 'thread',
      resourceId: protectedResourceId,
      threadId: protectedThreadId,
      value: '{"preference":{"format":"markdown","tone":"concise"},"profile":{"name":"Ada"}}',
      expectedRevision: 0,
      source: 'owner',
      protectPaths: ['/profile', '/preference'],
    });
    const ownerValue =
      '{\n  "profile": { "name": "Ada" },\n  "preference": { "tone": "concise", "format": "markdown" }\n}';
    const owner = await observerMemory.applyWorkingMemoryUpdate({
      scope: 'thread',
      resourceId: protectedResourceId,
      threadId: protectedThreadId,
      value: ownerValue,
      expectedRevision: initialOwner.revision,
      source: 'owner',
      protectPaths: ['/profile', '/preference'],
    });
    expect(owner).toMatchObject({
      value: ownerValue,
      revision: 2,
      protectedPaths: ['/preference', '/profile'],
    });
    expect(Object.keys(owner.provenance).sort()).toEqual(['', '/preference', '/profile'].sort());
    expect(Object.values(owner.provenance).every(entry => entry.source === 'owner')).toBe(true);
    await observerStore.db.none(
      `UPDATE "${schemaName}"."mastra_threads"
       SET "updatedAt" = $1, "updatedAtZ" = $2
       WHERE id = $3`,
      [createdAt.toISOString(), createdAt.toISOString(), protectedThreadId],
    );
    const before = await readStoredThread(protectedThreadId);

    await expect(
      retractorMemory.retractObservationalMemory({
        resourceId: protectedResourceId,
        threadId: protectedThreadId,
      }),
    ).resolves.toEqual({
      clearedScopes: ['thread'],
      clearedResourceWorkingMemory: false,
      clearedThreadMetadata: false,
    });

    await expect(retractorMemory.getObservationalMemory(protectedThreadId, protectedResourceId)).resolves.toBeNull();
    await expect(
      retractorMemory.getWorkingMemorySnapshot({
        scope: 'thread',
        resourceId: protectedResourceId,
        threadId: protectedThreadId,
      }),
    ).resolves.toEqual(owner);
    await expect(readStoredThread(protectedThreadId)).resolves.toEqual(before);
  });

  it('removes OM metadata while retaining owner-only protected thread memory', async () => {
    const protectedResourceId = `${resourceId}-owner-only-om`;
    const protectedThreadId = `${threadId}-owner-only-om`;
    await observerMemory.saveThread({
      thread: {
        id: protectedThreadId,
        resourceId: protectedResourceId,
        title: 'Original title',
        metadata: { preserved: true },
        createdAt,
        updatedAt: createdAt,
      },
    });
    const record = await observerMemory.initializeObservationalMemory({
      config: { _managedWorkingMemoryScope: 'thread' },
      resourceId: protectedResourceId,
      scope: 'thread',
      threadId: protectedThreadId,
    });
    const owner = await observerMemory.applyWorkingMemoryUpdate({
      scope: 'thread',
      resourceId: protectedResourceId,
      threadId: protectedThreadId,
      value: '{"owner":"Ada"}',
      expectedRevision: 0,
      source: 'owner',
      protectPaths: [''],
    });
    await observerMemory.updateThreadFromObservationalMemory({
      id: protectedThreadId,
      title: 'Derived title',
      metadata: { mastra: { om: { threadTitle: 'Derived title', currentTask: 'Observe only' } } },
      guard: {
        recordId: record.id,
        resourceId: protectedResourceId,
        threadId: protectedThreadId,
      },
    });
    await observerStore.db.none(
      `UPDATE "${schemaName}"."mastra_threads"
       SET "updatedAt" = $1, "updatedAtZ" = $2
       WHERE id = $3`,
      [createdAt.toISOString(), createdAt.toISOString(), protectedThreadId],
    );
    const before = await readStoredThread(protectedThreadId);
    const expectedMetadata = JSON.parse(before.metadataText) as {
      mastra: Record<string, unknown>;
      [key: string]: unknown;
    };
    expect(expectedMetadata.mastra).toHaveProperty('om');
    delete expectedMetadata.mastra.om;

    await expect(
      retractorMemory.retractObservationalMemory({
        resourceId: protectedResourceId,
        threadId: protectedThreadId,
      }),
    ).resolves.toEqual({
      clearedScopes: ['thread'],
      clearedResourceWorkingMemory: false,
      clearedThreadMetadata: true,
    });

    const after = await readStoredThread(protectedThreadId);
    expect(after.title).toBe('');
    expect(JSON.parse(after.metadataText)).toEqual(expectedMetadata);
    expect(after.updatedAt).not.toEqual(before.updatedAt);
    expect(after.updatedAtZ).not.toEqual(before.updatedAtZ);
    await expect(
      retractorMemory.getWorkingMemorySnapshot({
        scope: 'thread',
        resourceId: protectedResourceId,
        threadId: protectedThreadId,
      }),
    ).resolves.toEqual(owner);
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

  it('preserves sibling thread memory when resource and thread OM records own different scopes', async () => {
    const mixedResourceId = `${resourceId}-mixed-scopes`;
    const editedThreadId = `${threadId}-mixed-edited`;
    const siblingThreadId = `${threadId}-mixed-sibling`;
    await observerMemory.saveResource({
      resource: {
        id: mixedResourceId,
        workingMemory: 'observer-managed-resource',
        metadata: {},
        createdAt,
        updatedAt: createdAt,
      },
    });
    for (const [mixedThreadId, label] of [
      [editedThreadId, 'edited'],
      [siblingThreadId, 'sibling'],
    ] as const) {
      await observerMemory.saveThread({
        thread: {
          id: mixedThreadId,
          resourceId: mixedResourceId,
          title: `Derived ${label}`,
          metadata: {
            workingMemory: `observer-managed-${label}`,
            mastra: {
              preserved: true,
              om: { threadTitle: `Derived ${label}`, lastObservedMessageId: `message-${label}` },
            },
          },
          createdAt,
          updatedAt: createdAt,
        },
      });
    }
    await observerMemory.initializeObservationalMemory({
      config: { _managedWorkingMemoryScope: 'resource' },
      resourceId: mixedResourceId,
      scope: 'resource',
      threadId: null,
    });
    await observerMemory.initializeObservationalMemory({
      config: { _managedWorkingMemoryScope: 'thread' },
      resourceId: mixedResourceId,
      scope: 'thread',
      threadId: editedThreadId,
    });

    await expect(
      retractorMemory.retractObservationalMemory({
        resourceId: mixedResourceId,
        threadId: editedThreadId,
      }),
    ).resolves.toEqual({
      clearedScopes: ['resource', 'thread'],
      clearedResourceWorkingMemory: true,
      clearedThreadMetadata: true,
    });

    await expect(retractorMemory.getResourceById({ resourceId: mixedResourceId })).resolves.toMatchObject({
      workingMemory: null,
    });
    const editedThread = await retractorMemory.getThreadById({ threadId: editedThreadId });
    expect(editedThread).toMatchObject({
      title: '',
      metadata: { mastra: { preserved: true } },
    });
    expect(editedThread?.metadata).not.toHaveProperty('workingMemory');
    expect(editedThread?.metadata?.mastra).not.toHaveProperty('om');

    const siblingThread = await retractorMemory.getThreadById({ threadId: siblingThreadId });
    expect(siblingThread).toMatchObject({
      title: '',
      metadata: {
        workingMemory: 'observer-managed-sibling',
        mastra: { preserved: true },
      },
    });
    expect(siblingThread?.metadata?.mastra).not.toHaveProperty('om');
  });

  it('unions working-memory ownership across every generation at a thread coordinate', async () => {
    const generationsResourceId = `${resourceId}-generation-scopes`;
    const editedThreadId = `${threadId}-generation-edited`;
    const siblingThreadId = `${threadId}-generation-sibling`;
    await observerMemory.saveResource({
      resource: {
        id: generationsResourceId,
        workingMemory: 'observer-managed-resource',
        metadata: {},
        createdAt,
        updatedAt: createdAt,
      },
    });
    for (const [generationThreadId, label] of [
      [editedThreadId, 'edited'],
      [siblingThreadId, 'sibling'],
    ] as const) {
      await observerMemory.saveThread({
        thread: {
          id: generationThreadId,
          resourceId: generationsResourceId,
          title: `Derived ${label}`,
          metadata: {
            workingMemory: `observer-managed-${label}`,
            mastra: {
              preserved: true,
              om: { threadTitle: `Derived ${label}`, lastObservedMessageId: `message-${label}` },
            },
          },
          createdAt,
          updatedAt: createdAt,
        },
      });
    }
    const priorGeneration = await observerMemory.initializeObservationalMemory({
      config: { _managedWorkingMemoryScope: 'resource' },
      resourceId: generationsResourceId,
      scope: 'thread',
      threadId: editedThreadId,
    });
    const currentGeneration = await observerMemory.createReflectionGeneration({
      currentRecord: priorGeneration,
      reflection: 'Current generation.',
      tokenCount: 3,
    });
    await observerMemory.updateObservationalMemoryConfig({
      id: currentGeneration.id,
      config: { _managedWorkingMemoryScope: 'thread' },
    });

    await expect(
      retractorMemory.retractObservationalMemory({
        resourceId: generationsResourceId,
        threadId: editedThreadId,
      }),
    ).resolves.toEqual({
      clearedScopes: ['thread'],
      clearedResourceWorkingMemory: true,
      clearedThreadMetadata: true,
    });

    await expect(retractorMemory.getResourceById({ resourceId: generationsResourceId })).resolves.toMatchObject({
      workingMemory: null,
    });
    const editedThread = await retractorMemory.getThreadById({ threadId: editedThreadId });
    expect(editedThread).toMatchObject({
      title: '',
      metadata: { mastra: { preserved: true } },
    });
    expect(editedThread?.metadata).not.toHaveProperty('workingMemory');
    expect(editedThread?.metadata?.mastra).not.toHaveProperty('om');

    await expect(retractorMemory.getThreadById({ threadId: siblingThreadId })).resolves.toMatchObject({
      title: 'Derived sibling',
      metadata: {
        workingMemory: 'observer-managed-sibling',
        mastra: {
          preserved: true,
          om: { threadTitle: 'Derived sibling', lastObservedMessageId: 'message-sibling' },
        },
      },
    });
  });

  it.each([
    ['mastra-value', 'not-an-object', {}],
    ['om-value', { preserved: true, om: 'not-an-object' }, { preserved: true }],
  ])('retracts managed thread memory when the stored %s is malformed', async (suffix, mastra, expectedMastra) => {
    const malformedResourceId = `${resourceId}-malformed-${suffix}`;
    const malformedThreadId = `${threadId}-malformed-${suffix}`;
    await observerMemory.saveThread({
      thread: {
        id: malformedThreadId,
        resourceId: malformedResourceId,
        title: 'Preserved title',
        metadata: {
          workingMemory: 'observer-managed-thread',
          mastra,
        },
        createdAt,
        updatedAt: createdAt,
      },
    });
    await observerMemory.initializeObservationalMemory({
      config: { _managedWorkingMemoryScope: 'thread' },
      resourceId: malformedResourceId,
      scope: 'thread',
      threadId: malformedThreadId,
    });

    await expect(
      retractorMemory.retractObservationalMemory({
        resourceId: malformedResourceId,
        threadId: malformedThreadId,
      }),
    ).resolves.toEqual({
      clearedScopes: ['thread'],
      clearedResourceWorkingMemory: false,
      clearedThreadMetadata: true,
    });

    const thread = await retractorMemory.getThreadById({ threadId: malformedThreadId });
    expect(thread).toMatchObject({ title: 'Preserved title' });
    expect(thread?.metadata).toEqual({ mastra: expectedMastra });
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

  it.each(['update', 'delete'] as const)(
    'uses the persisted thread OM owner for message %s retraction',
    async operation => {
      const messageResourceId = `${resourceId}-message-owner-${operation}`;
      const ownerResourceId = `${resourceId}-thread-owner-${operation}`;
      const ownerThreadId = `${threadId}-owner-${operation}`;
      const ownerMessageId = `${threadId}-owner-message-${operation}`;

      for (const [id, workingMemory] of [
        [messageResourceId, 'unrelated message-resource memory'],
        [ownerResourceId, 'observer-managed owner memory'],
      ] as const) {
        await observerMemory.saveResource({
          resource: { id, workingMemory, metadata: {}, createdAt, updatedAt: createdAt },
        });
      }
      await observerMemory.saveThread({
        thread: {
          id: ownerThreadId,
          resourceId: ownerResourceId,
          title: ownerThreadId,
          metadata: {},
          createdAt,
          updatedAt: createdAt,
        },
      });
      await observerMemory.initializeObservationalMemory({
        config: { _managedWorkingMemoryScope: 'resource' },
        resourceId: ownerResourceId,
        scope: 'thread',
        threadId: ownerThreadId,
      });
      await observerMemory.saveMessages({
        messages: [
          {
            id: ownerMessageId,
            threadId: ownerThreadId,
            resourceId: messageResourceId,
            role: 'user',
            content: { format: 2, parts: [{ type: 'text', text: 'Retract by owner' }] },
            createdAt,
          },
        ],
      });

      const retractions: Array<{ input: { resourceId: string; threadId: string } }> = [];
      if (operation === 'update') {
        await retractorMemory.updateMessages({
          messages: [{ id: ownerMessageId, content: { content: 'authoritative edit' } }],
          retractObservationalMemory: true,
          observationalMemoryRetractions: retractions as any,
        });
      } else {
        await retractorMemory.deleteMessages([ownerMessageId], {
          retractObservationalMemory: true,
          observationalMemoryRetractions: retractions as any,
        });
      }

      expect(retractions).toMatchObject([{ input: { resourceId: ownerResourceId, threadId: ownerThreadId } }]);
      await expect(retractorMemory.getObservationalMemory(ownerThreadId, ownerResourceId)).resolves.toBeNull();
      await expect(retractorMemory.getResourceById({ resourceId: ownerResourceId })).resolves.toMatchObject({
        workingMemory: null,
      });
      await expect(retractorMemory.getResourceById({ resourceId: messageResourceId })).resolves.toMatchObject({
        workingMemory: 'unrelated message-resource memory',
      });
    },
  );

  it('deletes a nullable-resource orphan whose thread row is absent', async () => {
    const orphanMessageId = `${threadId}-missing-thread-orphan-message`;
    const missingThreadId = `${threadId}-missing-thread-orphan`;

    await observerStore.db.none(
      `INSERT INTO "${schemaName}"."mastra_messages"
         (id, thread_id, content, role, type, "createdAt", "resourceId")
       VALUES ($1, $2, $3, 'user', 'v2', $4, NULL)`,
      [orphanMessageId, missingThreadId, JSON.stringify({ format: 2, parts: [] }), createdAt],
    );

    await expect(
      retractorMemory.deleteMessages([orphanMessageId], { retractObservationalMemory: true }),
    ).resolves.toBeUndefined();
    await expect(
      observerStore.db.one<{ exists: boolean }>(
        `SELECT EXISTS(
           SELECT 1 FROM "${schemaName}"."mastra_messages" WHERE id = $1
         ) AS exists`,
        [orphanMessageId],
      ),
    ).resolves.toEqual({ exists: false });
  });

  it('uses the last duplicate update for both the message move and OM retraction coordinates', async () => {
    const duplicateMessageId = `${threadId}-duplicate-update-message`;
    const source = {
      resourceId: `${resourceId}-duplicate-source`,
      threadId: `${threadId}-duplicate-source`,
      workingMemory: 'source observer memory',
    };
    const discarded = {
      resourceId: `${resourceId}-duplicate-discarded`,
      threadId: `${threadId}-duplicate-discarded`,
      workingMemory: 'discarded observer memory',
    };
    const canonical = {
      resourceId: `${resourceId}-duplicate-canonical`,
      threadId: `${threadId}-duplicate-canonical`,
      workingMemory: 'canonical observer memory',
    };

    for (const coordinate of [source, discarded, canonical]) {
      await observerMemory.saveResource({
        resource: {
          id: coordinate.resourceId,
          workingMemory: coordinate.workingMemory,
          metadata: {},
          createdAt,
          updatedAt: createdAt,
        },
      });
      await observerMemory.saveThread({
        thread: {
          id: coordinate.threadId,
          resourceId: coordinate.resourceId,
          title: coordinate.threadId,
          metadata: {},
          createdAt,
          updatedAt: createdAt,
        },
      });
      await observerMemory.initializeObservationalMemory({
        config: { _managedWorkingMemoryScope: 'resource' },
        resourceId: coordinate.resourceId,
        scope: 'thread',
        threadId: coordinate.threadId,
      });
    }
    await observerMemory.saveMessages({
      messages: [
        {
          id: duplicateMessageId,
          threadId: source.threadId,
          resourceId: source.resourceId,
          role: 'user',
          content: { format: 2, parts: [{ type: 'text', text: 'Move me once' }] },
          createdAt,
        },
      ],
    });

    const retractions: Array<{ input: { resourceId: string; threadId: string } }> = [];
    const updatedMessages = await retractorMemory.updateMessages({
      messages: [
        {
          id: duplicateMessageId,
          threadId: discarded.threadId,
          resourceId: discarded.resourceId,
          content: { content: 'discarded update' },
        },
        {
          id: duplicateMessageId,
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
        id: duplicateMessageId,
        threadId: canonical.threadId,
        resourceId: canonical.resourceId,
        content: { content: 'canonical update' },
      },
    ]);
    expect(
      new Set(retractions.map(retraction => `${retraction.input.resourceId}\u0000${retraction.input.threadId}`)),
    ).toEqual(
      new Set([`${source.resourceId}\u0000${source.threadId}`, `${canonical.resourceId}\u0000${canonical.threadId}`]),
    );
    expect(retractions).toHaveLength(2);

    await expect(retractorMemory.getObservationalMemory(source.threadId, source.resourceId)).resolves.toBeNull();
    await expect(retractorMemory.getObservationalMemory(canonical.threadId, canonical.resourceId)).resolves.toBeNull();
    await expect(retractorMemory.getResourceById({ resourceId: source.resourceId })).resolves.toMatchObject({
      workingMemory: null,
    });
    await expect(retractorMemory.getResourceById({ resourceId: canonical.resourceId })).resolves.toMatchObject({
      workingMemory: null,
    });

    await expect(
      retractorMemory.getObservationalMemory(discarded.threadId, discarded.resourceId),
    ).resolves.not.toBeNull();
    await expect(retractorMemory.getResourceById({ resourceId: discarded.resourceId })).resolves.toMatchObject({
      workingMemory: discarded.workingMemory,
    });
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

  it('retracts a prepared thread record when deletion wins before thread creation', async () => {
    const preparedThreadId = `${threadId}-prepared-delete-thread`;
    const preparedResourceId = `${resourceId}-prepared-delete-thread`;
    const record = await observerMemory.initializeObservationalMemory({
      config: {},
      resourceId: preparedResourceId,
      scope: 'thread',
      threadId: preparedThreadId,
    });
    const sourceWriteGuard = {
      recordId: record.id,
      threadId: preparedThreadId,
      resourceId: preparedResourceId,
    };
    const retractions: Array<{
      input: { resourceId: string; threadId: string };
      result: { clearedScopes: Array<'resource' | 'thread'> };
    }> = [];

    await retractorMemory.deleteThread({
      threadId: preparedThreadId,
      observationalMemoryRetractions: retractions as any,
    });

    expect(retractions).toMatchObject([
      {
        input: { resourceId: preparedResourceId, threadId: preparedThreadId },
        result: { clearedScopes: ['thread'] },
      },
    ]);
    await expect(retractorMemory.getObservationalMemory(preparedThreadId, preparedResourceId)).resolves.toBeNull();
    await expect(
      retractorMemory.saveMessages({
        messages: [
          {
            id: `${preparedThreadId}-message`,
            threadId: preparedThreadId,
            resourceId: preparedResourceId,
            role: 'user',
            content: { format: 2, parts: [{ type: 'text', text: 'must be rejected after deletion' }] },
            createdAt,
          },
        ],
        observationalMemorySourceWriteGuard: sourceWriteGuard,
      }),
    ).rejects.toThrow('source write guard');
  });

  it('retracts observer-derived thread metadata stored in a legacy TEXT column', async () => {
    const legacyResourceId = `${resourceId}-legacy-text`;
    const legacyThreadId = `${threadId}-legacy-text`;
    await observerMemory.saveThread({
      thread: {
        id: legacyThreadId,
        resourceId: legacyResourceId,
        title: 'Legacy derived title',
        metadata: {
          preserved: true,
          workingMemory: 'Legacy derived memory',
          mastra: {
            preserved: true,
            om: { threadTitle: 'Legacy derived title' },
          },
        },
        createdAt,
        updatedAt: createdAt,
      },
    });
    await observerMemory.initializeObservationalMemory({
      config: { _managedWorkingMemoryScope: 'thread' },
      resourceId: legacyResourceId,
      scope: 'thread',
      threadId: legacyThreadId,
    });

    await observerStore.db.none(
      `ALTER TABLE "${schemaName}"."mastra_threads"
       ALTER COLUMN metadata TYPE TEXT USING metadata::text`,
    );
    try {
      await expect(
        retractorMemory.retractObservationalMemory({
          resourceId: legacyResourceId,
          threadId: legacyThreadId,
        }),
      ).resolves.toEqual({
        clearedScopes: ['thread'],
        clearedResourceWorkingMemory: false,
        clearedThreadMetadata: true,
      });
      const legacyThread = await retractorMemory.getThreadById({ threadId: legacyThreadId });
      expect(legacyThread).toMatchObject({ title: '' });
      expect(legacyThread?.metadata).toEqual({ preserved: true, mastra: { preserved: true } });
    } finally {
      await observerStore.db.none(
        `ALTER TABLE "${schemaName}"."mastra_threads"
         ALTER COLUMN metadata TYPE JSONB USING metadata::jsonb`,
      );
    }
  });
});
