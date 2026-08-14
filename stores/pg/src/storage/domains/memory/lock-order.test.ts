import type { MastraDBMessage } from '@mastra/core/memory';
import type {
  ObservationalMemoryRecord,
  StorageCloneThreadOutput,
  StorageObservationalMemoryCloneReceipt,
} from '@mastra/core/storage';
import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';

import { PostgresStore } from '../..';
import { connectionString } from '../../test-utils';
import type { MemoryPG } from '.';

const testRunId = `${process.pid}-${Date.now()}`;
const rollbackApplicationName = `pf1690-lock-a-${testRunId}`;
const competitorApplicationName = `pf1690-lock-b-${testRunId}`;

vi.setConfig({ testTimeout: 15_000, hookTimeout: 30_000 });

function connectionStringWithApplicationName(applicationName: string): string {
  const url = new URL(connectionString);
  url.searchParams.set('application_name', applicationName);
  return url.toString();
}

async function within<T>(promise: Promise<T>, label: string, timeoutMs = 8_000): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`${label} did not settle within ${timeoutMs}ms.`)), timeoutMs);
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

describe('MemoryPG lock ordering', () => {
  const schemaName = `memory_lock_order_${Date.now()}`;
  const createdAt = new Date('2026-01-01T00:00:00.000Z');
  let rollbackStore: PostgresStore;
  let competitorStore: PostgresStore;
  let rollbackMemory: MemoryPG;
  let competitorMemory: MemoryPG;
  let inspector: Pool;

  beforeAll(async () => {
    rollbackStore = new PostgresStore({
      id: 'lock-order-rollback',
      connectionString: connectionStringWithApplicationName(rollbackApplicationName),
      schemaName,
    });
    await rollbackStore.init();
    competitorStore = new PostgresStore({
      id: 'lock-order-competitor',
      connectionString: connectionStringWithApplicationName(competitorApplicationName),
      disableInit: true,
      schemaName,
    });
    await competitorStore.init();
    rollbackMemory = (await rollbackStore.getStore('memory')) as MemoryPG;
    competitorMemory = (await competitorStore.getStore('memory')) as MemoryPG;
    inspector = new Pool({ connectionString });
  });

  afterAll(async () => {
    await Promise.allSettled([rollbackStore?.close(), competitorStore?.close(), inspector?.end()]);
    const cleanup = new Pool({ connectionString });
    try {
      await cleanup.query(`DROP SCHEMA IF EXISTS "${schemaName}" CASCADE`);
    } finally {
      await cleanup.end();
    }
  });

  async function waitForAdvisoryLockWait(applicationName: string): Promise<void> {
    const deadline = Date.now() + 5_000;
    while (Date.now() < deadline) {
      const result = await inspector.query<{ waiting: boolean }>(
        `SELECT EXISTS (
           SELECT 1
           FROM pg_stat_activity
           WHERE application_name = $1
             AND wait_event_type = 'Lock'
             AND query LIKE '%pg_advisory_xact_lock%'
         ) AS waiting`,
        [applicationName],
      );
      if (result.rows[0]?.waiting) return;
      await new Promise(resolve => setTimeout(resolve, 20));
    }
    throw new Error(`Timed out waiting for ${applicationName} to block on an advisory lock.`);
  }

  async function blockObservationalMemory(resourceId: string) {
    const pool = new Pool({ connectionString });
    const client = await pool.connect();
    let transactionOpen = true;
    await client.query('BEGIN');
    await client.query('SELECT pg_advisory_xact_lock(hashtextextended($1, 0))', [
      `mastra:observational-memory:${resourceId}`,
    ]);
    return {
      async release() {
        if (!transactionOpen) return;
        await client.query('COMMIT');
        transactionOpen = false;
      },
      async close() {
        if (transactionOpen) {
          await client.query('ROLLBACK');
          transactionOpen = false;
        }
        client.release();
        await pool.end();
      },
    };
  }

  async function seedClone(
    suffix: string,
    metadata: Record<string, unknown> = {},
  ): Promise<StorageCloneThreadOutput & { rollbackReceipt: NonNullable<StorageCloneThreadOutput['rollbackReceipt']> }> {
    const sourceThreadId = `lock-source-${suffix}`;
    const sourceResourceId = `lock-source-resource-${suffix}`;
    const cloneThreadId = `lock-clone-${suffix}`;
    const cloneResourceId = `lock-clone-resource-${suffix}`;
    await rollbackMemory.saveThread({
      thread: {
        id: sourceThreadId,
        resourceId: sourceResourceId,
        title: 'Source',
        metadata: {},
        createdAt,
        updatedAt: createdAt,
      },
    });
    await rollbackMemory.saveMessages({
      messages: [
        {
          id: `lock-source-message-${suffix}`,
          threadId: sourceThreadId,
          resourceId: sourceResourceId,
          role: 'user',
          content: { format: 2, parts: [{ type: 'text', text: 'Source message' }] },
          createdAt,
        } satisfies MastraDBMessage,
      ],
    });
    const clone = await rollbackMemory.cloneThread({
      sourceThreadId,
      newThreadId: cloneThreadId,
      resourceId: cloneResourceId,
      metadata,
    });
    if (!clone.rollbackReceipt) throw new Error('Expected clone rollback receipt.');
    return clone as StorageCloneThreadOutput & {
      rollbackReceipt: NonNullable<StorageCloneThreadOutput['rollbackReceipt']>;
    };
  }

  async function seedCloneWithObservationalMemory(suffix: string): Promise<{
    clone: StorageCloneThreadOutput & {
      rollbackReceipt: NonNullable<StorageCloneThreadOutput['rollbackReceipt']>;
    };
    record: ObservationalMemoryRecord;
    receipt: StorageObservationalMemoryCloneReceipt;
  }> {
    const clone = await seedClone(suffix, {
      workingMemory: 'Observer-derived thread memory',
      mastra: { om: { threadTitle: 'Clone of Source' } },
    });
    const record = await rollbackMemory.initializeObservationalMemory({
      config: { _managedWorkingMemoryScope: 'thread' },
      resourceId: clone.thread.resourceId,
      scope: 'thread',
      threadId: clone.thread.id,
    });
    await rollbackMemory.clearObservationalMemory(clone.thread.id, clone.thread.resourceId);
    const receipt = await rollbackMemory.insertObservationalMemoryRecord(record);
    return { clone, record, receipt };
  }

  it('waits on OM before owning the cloned thread row', async () => {
    const clone = await seedClone('om-before-thread');
    const blocker = await blockObservationalMemory(clone.thread.resourceId);
    let rollbackPromise: ReturnType<MemoryPG['rollbackThreadClone']> | undefined;
    try {
      rollbackPromise = rollbackMemory.rollbackThreadClone({ thread: clone.rollbackReceipt });
      await waitForAdvisoryLockWait(rollbackApplicationName);

      const probe = await inspector.connect();
      let probeTransactionOpen = false;
      try {
        await probe.query('BEGIN');
        probeTransactionOpen = true;
        const result = await probe.query<{ id: string }>(
          `SELECT id FROM "${schemaName}"."mastra_threads" WHERE id = $1 FOR UPDATE NOWAIT`,
          [clone.thread.id],
        );
        expect(result.rows).toEqual([{ id: clone.thread.id }]);
        await probe.query('COMMIT');
        probeTransactionOpen = false;
      } finally {
        if (probeTransactionOpen) await probe.query('ROLLBACK');
        probe.release();
      }

      await blocker.release();
      await expect(within(rollbackPromise, 'rollback after OM release')).resolves.toEqual({ status: 'rolled_back' });
      await expect(rollbackMemory.getThreadById({ threadId: clone.thread.id })).resolves.toBeNull();
      await expect(rollbackMemory.listMessages({ threadId: clone.thread.id })).resolves.toMatchObject({ messages: [] });
    } finally {
      await blocker.close();
      if (rollbackPromise) await Promise.allSettled([rollbackPromise]);
    }
  });

  for (const operation of ['retract', 'delete-thread', 'update-message', 'delete-message'] as const) {
    it(`does not deadlock or partially roll back against concurrent ${operation}`, async () => {
      const { clone, record, receipt } = await seedCloneWithObservationalMemory(`race-${operation}`);
      const blocker = await blockObservationalMemory(clone.thread.resourceId);
      let competitorPromise: Promise<unknown> | undefined;
      let rollbackPromise: ReturnType<MemoryPG['rollbackThreadClone']> | undefined;
      try {
        switch (operation) {
          case 'retract':
            competitorPromise = competitorMemory.retractObservationalMemory({
              resourceId: clone.thread.resourceId,
              threadId: clone.thread.id,
            });
            break;
          case 'delete-thread':
            competitorPromise = competitorMemory.deleteThread({ threadId: clone.thread.id });
            break;
          case 'update-message':
            competitorPromise = competitorMemory.updateMessages({
              messages: [{ id: clone.clonedMessages[0]!.id, content: { content: 'Corrected clone message' } }],
              retractObservationalMemory: true,
            });
            break;
          case 'delete-message':
            competitorPromise = competitorMemory.deleteMessages([clone.clonedMessages[0]!.id], {
              retractObservationalMemory: true,
            });
            break;
        }
        await waitForAdvisoryLockWait(competitorApplicationName);

        rollbackPromise = rollbackMemory.rollbackThreadClone({
          thread: clone.rollbackReceipt,
          observationalMemory: receipt,
          unverifiedObservationalMemoryRecordId: record.id,
        });
        await waitForAdvisoryLockWait(rollbackApplicationName);
        await blocker.release();

        const [, rollback] = await within(
          Promise.all([competitorPromise, rollbackPromise]),
          `${operation} and rollback concurrency`,
        );
        expect(rollback).toMatchObject({ status: 'conflict' });
        await expect(
          rollbackMemory.getObservationalMemory(clone.thread.id, clone.thread.resourceId),
        ).resolves.toBeNull();

        const thread = await rollbackMemory.getThreadById({ threadId: clone.thread.id });
        const messages = await rollbackMemory.listMessages({ threadId: clone.thread.id });
        if (operation === 'delete-thread') {
          expect(thread).toBeNull();
          expect(messages.messages).toEqual([]);
        } else {
          expect(thread).not.toBeNull();
          expect(messages.messages).toHaveLength(operation === 'delete-message' ? 0 : 1);
          if (operation === 'update-message') {
            expect(JSON.stringify(messages.messages[0]?.content)).toContain('Corrected clone message');
          }
        }
      } finally {
        await blocker.close();
        await Promise.allSettled([competitorPromise, rollbackPromise].filter(Boolean) as Promise<unknown>[]);
      }
    });
  }

  it('acquires every OM lock before thread rows when moving a message from A to B', async () => {
    const sourceResourceId = 'public-move-resource-a';
    const destinationResourceId = 'public-move-resource-b';
    const sourceThreadId = 'public-move-thread-a';
    const destinationThreadId = 'public-move-thread-b';
    const messageId = 'public-move-message';

    for (const [threadId, resourceId] of [
      [sourceThreadId, sourceResourceId],
      [destinationThreadId, destinationResourceId],
    ] as const) {
      await rollbackMemory.saveResource({
        resource: {
          id: resourceId,
          workingMemory: `derived-${resourceId}`,
          metadata: {},
          createdAt,
          updatedAt: createdAt,
        },
      });
      await rollbackMemory.saveThread({
        thread: {
          id: threadId,
          resourceId,
          title: threadId,
          metadata: { mastra: { om: { threadTitle: threadId } } },
          createdAt,
          updatedAt: createdAt,
        },
      });
      await rollbackMemory.initializeObservationalMemory({
        config: { _managedWorkingMemoryScope: 'resource' },
        resourceId,
        scope: 'thread',
        threadId,
      });
    }
    await rollbackMemory.saveMessages({
      messages: [
        {
          id: messageId,
          threadId: sourceThreadId,
          resourceId: sourceResourceId,
          role: 'user',
          content: { format: 2, parts: [{ type: 'text', text: 'Move me' }] },
          createdAt,
        } satisfies MastraDBMessage,
      ],
    });

    const blocker = await blockObservationalMemory(destinationResourceId);
    let updatePromise: ReturnType<MemoryPG['updateMessages']> | undefined;
    try {
      updatePromise = rollbackMemory.updateMessages({
        messages: [
          {
            id: messageId,
            threadId: destinationThreadId,
            resourceId: destinationResourceId,
            content: { content: 'Moved safely' },
          },
        ],
        retractObservationalMemory: true,
      });
      await waitForAdvisoryLockWait(rollbackApplicationName);

      const probe = await inspector.connect();
      let probeTransactionOpen = false;
      try {
        await probe.query('BEGIN');
        probeTransactionOpen = true;
        const result = await probe.query<{ id: string }>(
          `SELECT id FROM "${schemaName}"."mastra_threads"
           WHERE id IN ($1, $2)
           ORDER BY id
           FOR UPDATE NOWAIT`,
          [sourceThreadId, destinationThreadId],
        );
        expect(result.rows.map(row => row.id)).toEqual([sourceThreadId, destinationThreadId]);
        await probe.query('COMMIT');
        probeTransactionOpen = false;
      } finally {
        if (probeTransactionOpen) await probe.query('ROLLBACK');
        probe.release();
      }

      await blocker.release();
      await expect(within(updatePromise, 'public A-to-B message move')).resolves.toMatchObject([
        { id: messageId, threadId: destinationThreadId, resourceId: destinationResourceId },
      ]);
      await expect(rollbackMemory.getObservationalMemory(sourceThreadId, sourceResourceId)).resolves.toBeNull();
      await expect(
        rollbackMemory.getObservationalMemory(destinationThreadId, destinationResourceId),
      ).resolves.toBeNull();
      await expect(rollbackMemory.getResourceById({ resourceId: sourceResourceId })).resolves.toMatchObject({
        workingMemory: null,
      });
      await expect(rollbackMemory.getResourceById({ resourceId: destinationResourceId })).resolves.toMatchObject({
        workingMemory: null,
      });
      await expect(rollbackMemory.listMessages({ threadId: sourceThreadId })).resolves.toMatchObject({ messages: [] });
      const destinationMessages = await rollbackMemory.listMessages({ threadId: destinationThreadId });
      expect(destinationMessages.messages).toHaveLength(1);
      expect(JSON.stringify(destinationMessages.messages[0]?.content)).toContain('Moved safely');
    } finally {
      await blocker.close();
      if (updatePromise) await Promise.allSettled([updatePromise]);
    }
  });

  it('rolls back a stale A-to-B message mutation instead of leaving B derived state current', async () => {
    const sourceResourceId = 'message-race-resource-a';
    const destinationResourceId = 'message-race-resource-b';
    const sourceThreadId = 'message-race-thread-a';
    const destinationThreadId = 'message-race-thread-b';
    const messageId = 'message-race-id';
    for (const resourceId of [sourceResourceId, destinationResourceId]) {
      await rollbackMemory.saveResource({
        resource: {
          id: resourceId,
          workingMemory: `derived-${resourceId}`,
          metadata: {},
          createdAt,
          updatedAt: createdAt,
        },
      });
    }
    for (const [threadId, resourceId] of [
      [sourceThreadId, sourceResourceId],
      [destinationThreadId, destinationResourceId],
    ] as const) {
      await rollbackMemory.saveThread({
        thread: { id: threadId, resourceId, title: threadId, metadata: {}, createdAt, updatedAt: createdAt },
      });
      await rollbackMemory.initializeObservationalMemory({
        config: { _managedWorkingMemoryScope: 'resource' },
        resourceId,
        scope: resourceId === sourceResourceId ? 'thread' : 'resource',
        threadId: resourceId === sourceResourceId ? threadId : null,
      });
    }
    await rollbackMemory.saveMessages({
      messages: [
        {
          id: messageId,
          threadId: sourceThreadId,
          resourceId: sourceResourceId,
          role: 'user',
          content: { format: 2, parts: [{ type: 'text', text: 'Original message' }] },
          createdAt,
        } satisfies MastraDBMessage,
      ],
    });

    const blocker = await blockObservationalMemory(sourceResourceId);
    const committedRetractions: unknown[] = [];
    let staleUpdate: ReturnType<MemoryPG['updateMessages']> | undefined;
    try {
      staleUpdate = rollbackMemory.updateMessages({
        messages: [{ id: messageId, content: { content: 'Stale correction' } }],
        retractObservationalMemory: true,
        observationalMemoryRetractions: committedRetractions as any,
      });
      await waitForAdvisoryLockWait(rollbackApplicationName);

      await competitorStore.db.none(
        `UPDATE "${schemaName}"."mastra_messages"
         SET thread_id = $1, "resourceId" = $2
         WHERE id = $3`,
        [destinationThreadId, destinationResourceId, messageId],
      );
      await blocker.release();

      await expect(within(staleUpdate, 'stale message update')).rejects.toThrow(/changed concurrently/i);
      expect(committedRetractions).toEqual([]);
      await expect(rollbackMemory.getObservationalMemory(sourceThreadId, sourceResourceId)).resolves.not.toBeNull();
      await expect(rollbackMemory.getObservationalMemory(null, destinationResourceId)).resolves.not.toBeNull();
      await expect(rollbackMemory.getResourceById({ resourceId: sourceResourceId })).resolves.toMatchObject({
        workingMemory: `derived-${sourceResourceId}`,
      });
      await expect(rollbackMemory.getResourceById({ resourceId: destinationResourceId })).resolves.toMatchObject({
        workingMemory: `derived-${destinationResourceId}`,
      });
      const row = await competitorStore.db.one<{ threadId: string; resourceId: string; content: unknown }>(
        `SELECT thread_id AS "threadId", "resourceId", content
         FROM "${schemaName}"."mastra_messages"
         WHERE id = $1`,
        [messageId],
      );
      expect(row).toMatchObject({ threadId: destinationThreadId, resourceId: destinationResourceId });
      expect(JSON.stringify(row.content)).toContain('Original message');
    } finally {
      await blocker.close();
      if (staleUpdate) await Promise.allSettled([staleUpdate]);
    }
  });

  it('rolls back deleteMessages when the target moves from A to B behind its lifecycle lock', async () => {
    const sourceResourceId = 'delete-message-race-resource-a';
    const destinationResourceId = 'delete-message-race-resource-b';
    const sourceThreadId = 'delete-message-race-thread-a';
    const destinationThreadId = 'delete-message-race-thread-b';
    const messageId = 'delete-message-race-id';
    for (const resourceId of [sourceResourceId, destinationResourceId]) {
      await rollbackMemory.saveResource({
        resource: {
          id: resourceId,
          workingMemory: `derived-${resourceId}`,
          metadata: {},
          createdAt,
          updatedAt: createdAt,
        },
      });
    }
    for (const [threadId, resourceId] of [
      [sourceThreadId, sourceResourceId],
      [destinationThreadId, destinationResourceId],
    ] as const) {
      await rollbackMemory.saveThread({
        thread: { id: threadId, resourceId, title: threadId, metadata: {}, createdAt, updatedAt: createdAt },
      });
      await rollbackMemory.initializeObservationalMemory({
        config: { _managedWorkingMemoryScope: 'resource' },
        resourceId,
        scope: resourceId === sourceResourceId ? 'thread' : 'resource',
        threadId: resourceId === sourceResourceId ? threadId : null,
      });
    }
    await rollbackMemory.saveMessages({
      messages: [
        {
          id: messageId,
          threadId: sourceThreadId,
          resourceId: sourceResourceId,
          role: 'user',
          content: { format: 2, parts: [{ type: 'text', text: 'Do not delete at B' }] },
          createdAt,
        } satisfies MastraDBMessage,
      ],
    });

    const blocker = await blockObservationalMemory(sourceResourceId);
    const committedRetractions: unknown[] = [];
    let deletePromise: ReturnType<MemoryPG['deleteMessages']> | undefined;
    try {
      deletePromise = rollbackMemory.deleteMessages([messageId], {
        retractObservationalMemory: true,
        observationalMemoryRetractions: committedRetractions as any,
      });
      await waitForAdvisoryLockWait(rollbackApplicationName);

      await competitorStore.db.none(
        `UPDATE "${schemaName}"."mastra_messages"
         SET thread_id = $1, "resourceId" = $2
         WHERE id = $3`,
        [destinationThreadId, destinationResourceId, messageId],
      );
      await blocker.release();

      await expect(within(deletePromise, 'stale message delete')).rejects.toThrow(/changed concurrently/i);
      expect(committedRetractions).toEqual([]);
      await expect(rollbackMemory.getObservationalMemory(sourceThreadId, sourceResourceId)).resolves.not.toBeNull();
      await expect(rollbackMemory.getObservationalMemory(null, destinationResourceId)).resolves.not.toBeNull();
      await expect(rollbackMemory.getResourceById({ resourceId: sourceResourceId })).resolves.toMatchObject({
        workingMemory: `derived-${sourceResourceId}`,
      });
      await expect(rollbackMemory.getResourceById({ resourceId: destinationResourceId })).resolves.toMatchObject({
        workingMemory: `derived-${destinationResourceId}`,
      });
      await expect(rollbackMemory.listMessages({ threadId: sourceThreadId })).resolves.toMatchObject({ messages: [] });
      const destinationMessages = await rollbackMemory.listMessages({ threadId: destinationThreadId });
      expect(destinationMessages.messages).toHaveLength(1);
      expect(JSON.stringify(destinationMessages.messages[0]?.content)).toContain('Do not delete at B');
    } finally {
      await blocker.close();
      if (deletePromise) await Promise.allSettled([deletePromise]);
    }
  });

  it('rolls back deleteThread when the row moves from resource A to B behind its lifecycle lock', async () => {
    const sourceResourceId = 'delete-thread-drift-resource-a';
    const destinationResourceId = 'delete-thread-drift-resource-b';
    const threadId = 'delete-thread-drift-id';
    for (const resourceId of [sourceResourceId, destinationResourceId]) {
      await rollbackMemory.saveResource({
        resource: {
          id: resourceId,
          workingMemory: `derived-${resourceId}`,
          metadata: {},
          createdAt,
          updatedAt: createdAt,
        },
      });
      await rollbackMemory.initializeObservationalMemory({
        config: { _managedWorkingMemoryScope: 'resource' },
        resourceId,
        scope: resourceId === sourceResourceId ? 'thread' : 'resource',
        threadId: resourceId === sourceResourceId ? threadId : null,
      });
    }
    await rollbackMemory.saveThread({
      thread: {
        id: threadId,
        resourceId: sourceResourceId,
        title: 'Source owner',
        metadata: {},
        createdAt,
        updatedAt: createdAt,
      },
    });

    const blocker = await blockObservationalMemory(sourceResourceId);
    const committedRetractions: unknown[] = [];
    let deletePromise: ReturnType<MemoryPG['deleteThread']> | undefined;
    try {
      deletePromise = rollbackMemory.deleteThread({
        threadId,
        observationalMemoryRetractions: committedRetractions as any,
      });
      await waitForAdvisoryLockWait(rollbackApplicationName);

      await competitorStore.db.none(
        `UPDATE "${schemaName}"."mastra_threads"
         SET "resourceId" = $1
         WHERE id = $2`,
        [destinationResourceId, threadId],
      );
      await blocker.release();

      await expect(within(deletePromise, 'stale thread delete')).rejects.toThrow(/changed resources/i);
      expect(committedRetractions).toEqual([]);
      await expect(rollbackMemory.getThreadById({ threadId })).resolves.toMatchObject({
        resourceId: destinationResourceId,
      });
      await expect(rollbackMemory.getObservationalMemory(threadId, sourceResourceId)).resolves.not.toBeNull();
      await expect(rollbackMemory.getObservationalMemory(null, destinationResourceId)).resolves.not.toBeNull();
      await expect(rollbackMemory.getResourceById({ resourceId: sourceResourceId })).resolves.toMatchObject({
        workingMemory: `derived-${sourceResourceId}`,
      });
      await expect(rollbackMemory.getResourceById({ resourceId: destinationResourceId })).resolves.toMatchObject({
        workingMemory: `derived-${destinationResourceId}`,
      });
    } finally {
      await blocker.close();
      if (deletePromise) await Promise.allSettled([deletePromise]);
    }
  });

  it('serializes deleteThread with saveThread reassignment so the destination state is not orphaned', async () => {
    const sourceResourceId = 'thread-race-resource-a';
    const destinationResourceId = 'thread-race-resource-b';
    const threadId = 'thread-race-id';
    for (const resourceId of [sourceResourceId, destinationResourceId]) {
      await rollbackMemory.saveResource({
        resource: {
          id: resourceId,
          workingMemory: `derived-${resourceId}`,
          metadata: {},
          createdAt,
          updatedAt: createdAt,
        },
      });
      await rollbackMemory.initializeObservationalMemory({
        config: { _managedWorkingMemoryScope: 'resource' },
        resourceId,
        scope: resourceId === sourceResourceId ? 'thread' : 'resource',
        threadId: resourceId === sourceResourceId ? threadId : null,
      });
    }
    await rollbackMemory.saveThread({
      thread: {
        id: threadId,
        resourceId: sourceResourceId,
        title: 'Source owner',
        metadata: {},
        createdAt,
        updatedAt: createdAt,
      },
    });

    const blocker = await blockObservationalMemory(sourceResourceId);
    let deletePromise: ReturnType<MemoryPG['deleteThread']> | undefined;
    let savePromise: ReturnType<MemoryPG['saveThread']> | undefined;
    try {
      deletePromise = rollbackMemory.deleteThread({ threadId });
      await waitForAdvisoryLockWait(rollbackApplicationName);

      savePromise = competitorMemory.saveThread({
        thread: {
          id: threadId,
          resourceId: destinationResourceId,
          title: 'Destination owner',
          metadata: { recreated: true },
          createdAt: new Date('2026-01-02T00:00:00.000Z'),
          updatedAt: new Date('2026-01-02T00:00:00.000Z'),
        },
      });
      await waitForAdvisoryLockWait(competitorApplicationName);
      await blocker.release();
      await within(Promise.all([deletePromise, savePromise]), 'delete and saveThread reassignment');

      await expect(rollbackMemory.getThreadById({ threadId })).resolves.toMatchObject({
        resourceId: destinationResourceId,
        title: 'Destination owner',
        metadata: { recreated: true },
      });
      await expect(rollbackMemory.getObservationalMemory(threadId, sourceResourceId)).resolves.toBeNull();
      await expect(rollbackMemory.getResourceById({ resourceId: sourceResourceId })).resolves.toMatchObject({
        workingMemory: null,
      });
      await expect(rollbackMemory.getObservationalMemory(null, destinationResourceId)).resolves.not.toBeNull();
      await expect(rollbackMemory.getResourceById({ resourceId: destinationResourceId })).resolves.toMatchObject({
        workingMemory: `derived-${destinationResourceId}`,
      });
    } finally {
      await blocker.close();
      await Promise.allSettled([deletePromise, savePromise].filter(Boolean) as Promise<unknown>[]);
    }
  });

  it('serializes deleteThread with combined ungoverned save reassignment before retracting source state', async () => {
    const sourceResourceId = 'combined-save-race-resource-a';
    const destinationResourceId = 'combined-save-race-resource-b';
    const threadId = 'combined-save-race-thread';
    for (const resourceId of [sourceResourceId, destinationResourceId]) {
      await rollbackMemory.saveResource({
        resource: {
          id: resourceId,
          workingMemory: `derived-${resourceId}`,
          metadata: {},
          createdAt,
          updatedAt: createdAt,
        },
      });
      await rollbackMemory.initializeObservationalMemory({
        config: { _managedWorkingMemoryScope: 'resource' },
        resourceId,
        scope: resourceId === sourceResourceId ? 'thread' : 'resource',
        threadId: resourceId === sourceResourceId ? threadId : null,
      });
    }
    await rollbackMemory.saveThread({
      thread: {
        id: threadId,
        resourceId: sourceResourceId,
        title: 'Source owner',
        metadata: {},
        createdAt,
        updatedAt: createdAt,
      },
    });

    const blocker = await blockObservationalMemory(sourceResourceId);
    let deletePromise: ReturnType<MemoryPG['deleteThread']> | undefined;
    let savePromise: ReturnType<MemoryPG['mutateThreadWithWorkingMemory']> | undefined;
    try {
      deletePromise = rollbackMemory.deleteThread({ threadId });
      await waitForAdvisoryLockWait(rollbackApplicationName);

      savePromise = competitorMemory.mutateThreadWithWorkingMemory({
        mutation: {
          type: 'save',
          thread: {
            id: threadId,
            resourceId: destinationResourceId,
            title: 'Destination owner',
            metadata: { recreated: true },
            createdAt: new Date('2026-01-02T00:00:00.000Z'),
            updatedAt: new Date('2026-01-02T00:00:00.000Z'),
          },
        },
        workingMemory: { type: 'require-ungoverned' },
      });
      await waitForAdvisoryLockWait(competitorApplicationName);
      await blocker.release();
      await within(Promise.all([deletePromise, savePromise]), 'delete and combined ungoverned save reassignment');

      await expect(rollbackMemory.getThreadById({ threadId })).resolves.toMatchObject({
        resourceId: destinationResourceId,
        title: 'Destination owner',
        metadata: { recreated: true },
      });
      await expect(rollbackMemory.getObservationalMemory(threadId, sourceResourceId)).resolves.toBeNull();
      await expect(rollbackMemory.getResourceById({ resourceId: sourceResourceId })).resolves.toMatchObject({
        workingMemory: null,
      });
      await expect(rollbackMemory.getObservationalMemory(null, destinationResourceId)).resolves.not.toBeNull();
      await expect(rollbackMemory.getResourceById({ resourceId: destinationResourceId })).resolves.toMatchObject({
        workingMemory: `derived-${destinationResourceId}`,
      });
    } finally {
      await blocker.close();
      await Promise.allSettled([deletePromise, savePromise].filter(Boolean) as Promise<unknown>[]);
    }
  });
});
