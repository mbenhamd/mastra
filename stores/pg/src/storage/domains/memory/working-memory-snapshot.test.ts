import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { PostgresStore } from '../..';
import { connectionString } from '../../test-utils';
import type { MemoryPG } from '.';

describe('MemoryPG revisioned Working Memory', () => {
  const schemaName = `wm_snapshot_${Date.now()}`;
  const resourceId = `resource-${Date.now()}`;
  const threadId = `thread-${Date.now()}`;
  let firstStore: PostgresStore;
  let secondStore: PostgresStore;
  let firstMemory: MemoryPG;
  let secondMemory: MemoryPG;

  beforeAll(async () => {
    firstStore = new PostgresStore({ id: 'wm-snapshot-first', connectionString, schemaName });
    await firstStore.init();
    secondStore = new PostgresStore({
      id: 'wm-snapshot-second',
      connectionString,
      disableInit: true,
      schemaName,
    });
    await secondStore.init();
    firstMemory = (await firstStore.getStore('memory')) as MemoryPG;
    secondMemory = (await secondStore.getStore('memory')) as MemoryPG;
  });

  afterAll(async () => {
    await Promise.allSettled([firstStore?.close(), secondStore?.close()]);
    const cleanup = new Pool({ connectionString });
    try {
      await cleanup.query(`DROP SCHEMA IF EXISTS "${schemaName}" CASCADE`);
    } finally {
      await cleanup.end();
    }
  });

  it('serializes owner corrections with observer writes across store instances', async () => {
    const createdAt = new Date('2026-01-01T00:00:00.000Z');
    await firstMemory.saveThread({
      thread: { id: threadId, resourceId, title: 'Thread', metadata: {}, createdAt, updatedAt: createdAt },
    });
    const owner = await firstMemory.applyWorkingMemoryUpdate({
      scope: 'resource',
      resourceId,
      value: '{"name":"Ada","focus":"proofs"}',
      expectedRevision: 0,
      source: 'owner',
      protectPaths: ['/name'],
    });

    await expect(
      secondMemory.applyWorkingMemoryUpdate({
        scope: 'resource',
        resourceId,
        value: '{"name":"Grace","focus":"compilers"}',
        expectedRevision: 0,
        source: 'observer',
      }),
    ).rejects.toMatchObject({ name: 'WorkingMemoryRevisionConflictError' });

    const observer = await secondMemory.applyWorkingMemoryUpdate({
      scope: 'resource',
      resourceId,
      value: '{"name":"Grace","focus":"compilers"}',
      expectedRevision: owner.revision,
      source: 'observer',
    });
    expect(JSON.parse(observer.value!)).toEqual({ name: 'Ada', focus: 'compilers' });
    await expect(firstMemory.getWorkingMemorySnapshot({ scope: 'resource', resourceId })).resolves.toEqual(observer);
  });

  it('allows exactly one concurrent writer for the same revision', async () => {
    const concurrentResourceId = `${resourceId}-concurrent`;
    const results = await Promise.allSettled([
      firstMemory.applyWorkingMemoryUpdate({
        scope: 'resource',
        resourceId: concurrentResourceId,
        value: '{"winner":"first"}',
        expectedRevision: 0,
        source: 'owner',
      }),
      secondMemory.applyWorkingMemoryUpdate({
        scope: 'resource',
        resourceId: concurrentResourceId,
        value: '{"winner":"second"}',
        expectedRevision: 0,
        source: 'owner',
      }),
    ]);

    expect(results.filter(result => result.status === 'fulfilled')).toHaveLength(1);
    const rejected = results.find(result => result.status === 'rejected');
    expect(rejected).toMatchObject({
      status: 'rejected',
      reason: { name: 'WorkingMemoryRevisionConflictError' },
    });
    await expect(
      firstMemory.getWorkingMemorySnapshot({ scope: 'resource', resourceId: concurrentResourceId }),
    ).resolves.toMatchObject({ revision: 1 });
  });

  it('preserves governed thread metadata across stale generic and OM updates', async () => {
    const protectedResourceId = `${resourceId}-stale-metadata`;
    const protectedThreadId = `${threadId}-stale-metadata`;
    const createdAt = new Date('2026-01-01T00:00:00.000Z');
    await firstMemory.saveThread({
      thread: {
        id: protectedThreadId,
        resourceId: protectedResourceId,
        title: 'Thread',
        metadata: {},
        createdAt,
        updatedAt: createdAt,
      },
    });
    const record = await firstMemory.initializeObservationalMemory({
      config: { _managedWorkingMemoryScope: 'thread' },
      resourceId: protectedResourceId,
      scope: 'thread',
      threadId: protectedThreadId,
    });
    const staleThread = await secondMemory.getThreadById({ threadId: protectedThreadId });
    if (!staleThread) throw new Error('Expected stale thread snapshot.');
    await firstMemory.applyWorkingMemoryUpdate({
      scope: 'thread',
      resourceId: protectedResourceId,
      threadId: protectedThreadId,
      value: '{"name":"Ada"}',
      expectedRevision: 0,
      source: 'owner',
      protectPaths: ['/name'],
    });

    await secondMemory.updateThreadFromObservationalMemory({
      id: protectedThreadId,
      metadata: { mastra: { om: { currentTask: 'Keep controls current' } } },
      guard: { recordId: record.id, resourceId: protectedResourceId, threadId: protectedThreadId },
    });
    await secondMemory.updateThread({
      id: protectedThreadId,
      title: 'Updated title',
      metadata: staleThread.metadata,
    });

    await expect(
      firstMemory.getWorkingMemorySnapshot({
        scope: 'thread',
        resourceId: protectedResourceId,
        threadId: protectedThreadId,
      }),
    ).resolves.toMatchObject({ value: '{"name":"Ada"}', revision: 1, protectedPaths: ['/name'] });
    await expect(firstMemory.getThreadById({ threadId: protectedThreadId })).resolves.toMatchObject({
      title: 'Updated title',
      metadata: { mastra: { om: { currentTask: 'Keep controls current' } } },
    });
  });

  it('rolls back when protected values make the merged observer value exceed its bound', async () => {
    const boundedResourceId = `${resourceId}-bounded`;
    const owner = await firstMemory.applyWorkingMemoryUpdate({
      scope: 'resource',
      resourceId: boundedResourceId,
      value: '{"keep":"1234"}',
      expectedRevision: 0,
      source: 'owner',
      protectPaths: ['/keep'],
    });

    await expect(
      secondMemory.applyWorkingMemoryUpdate({
        scope: 'resource',
        resourceId: boundedResourceId,
        value: '{"new":"5678"}',
        expectedRevision: owner.revision,
        source: 'observer',
        maxDataBytes: 20,
      }),
    ).rejects.toThrow('UTF-8 byte limit');
    await expect(
      firstMemory.getWorkingMemorySnapshot({ scope: 'resource', resourceId: boundedResourceId }),
    ).resolves.toEqual(owner);
  });

  it('fails closed when persisted metadata is not an object', async () => {
    const malformedResourceId = `${resourceId}-malformed-metadata`;
    await firstMemory.applyWorkingMemoryUpdate({
      scope: 'resource',
      resourceId: malformedResourceId,
      value: '{"name":"Ada"}',
      expectedRevision: 0,
      source: 'owner',
    });
    const client = new Pool({ connectionString });
    try {
      await client.query(
        `UPDATE "${schemaName}"."mastra_resources" SET metadata = '"not-an-object"'::jsonb WHERE id = $1`,
        [malformedResourceId],
      );
    } finally {
      await client.end();
    }

    await expect(
      firstMemory.getWorkingMemorySnapshot({ scope: 'resource', resourceId: malformedResourceId }),
    ).rejects.toThrow('Stored metadata is invalid');
  });

  it('retains protected owner values during atomic OM retraction', async () => {
    const protectedResourceId = `${resourceId}-retraction`;
    const protectedThreadId = `${threadId}-retraction`;
    const createdAt = new Date('2026-01-01T00:00:00.000Z');
    await firstMemory.saveThread({
      thread: {
        id: protectedThreadId,
        resourceId: protectedResourceId,
        title: 'Thread',
        metadata: {},
        createdAt,
        updatedAt: createdAt,
      },
    });
    const record = await firstMemory.initializeObservationalMemory({
      config: { _managedWorkingMemoryScope: 'resource' },
      resourceId: protectedResourceId,
      scope: 'resource',
      threadId: null,
    });
    const owner = await firstMemory.applyWorkingMemoryUpdate({
      scope: 'resource',
      resourceId: protectedResourceId,
      value: '{"name":"Ada"}',
      expectedRevision: 0,
      source: 'owner',
      protectPaths: ['/name'],
    });
    await firstMemory.applyWorkingMemoryUpdate({
      scope: 'resource',
      resourceId: protectedResourceId,
      threadId: protectedThreadId,
      value: '{"name":"Grace","temporaryTask":"draft"}',
      expectedRevision: owner.revision,
      source: 'observer',
      observationalMemoryGuard: {
        recordId: record.id,
        resourceId: protectedResourceId,
        threadId: null,
      },
    });

    await expect(
      secondMemory.retractObservationalMemory({
        resourceId: protectedResourceId,
        threadId: protectedThreadId,
      }),
    ).resolves.toMatchObject({ clearedResourceWorkingMemory: true });
    const retracted = await firstMemory.getWorkingMemorySnapshot({
      scope: 'resource',
      resourceId: protectedResourceId,
    });
    expect(JSON.parse(retracted.value!)).toEqual({ name: 'Ada' });
    expect(retracted.protectedPaths).toEqual(['/name']);
  });

  it('persists metadata-only observer provenance cleanup during OM retraction', async () => {
    const protectedResourceId = `${resourceId}-metadata-retraction`;
    const protectedThreadId = `${threadId}-metadata-retraction`;
    const createdAt = new Date('2026-01-01T00:00:00.000Z');
    await firstMemory.saveThread({
      thread: {
        id: protectedThreadId,
        resourceId: protectedResourceId,
        title: 'Thread',
        metadata: {},
        createdAt,
        updatedAt: createdAt,
      },
    });
    await firstMemory.initializeObservationalMemory({
      config: { _managedWorkingMemoryScope: 'resource' },
      resourceId: protectedResourceId,
      scope: 'resource',
      threadId: null,
    });
    await firstMemory.updateResource({
      resourceId: protectedResourceId,
      workingMemory: '{"name":"Ada"}',
      metadata: {
        mastra: {
          workingMemory: {
            revision: 2,
            protectedPaths: [''],
            provenance: {
              '': { source: 'owner', revision: 1, updatedAt: '2026-01-01T00:00:00.000Z' },
              '/stale': { source: 'observer', revision: 2, updatedAt: '2026-01-02T00:00:00.000Z' },
            },
          },
        },
      },
    });

    await expect(
      secondMemory.retractObservationalMemory({
        resourceId: protectedResourceId,
        threadId: protectedThreadId,
      }),
    ).resolves.toMatchObject({ clearedResourceWorkingMemory: false });
    await expect(
      firstMemory.getWorkingMemorySnapshot({ scope: 'resource', resourceId: protectedResourceId }),
    ).resolves.toMatchObject({
      value: '{"name":"Ada"}',
      revision: 3,
      provenance: { '': { source: 'owner' } },
    });
  });

  it('persists controls-only cleanup after an observer clears resource and thread values', async () => {
    for (const scope of ['resource', 'thread'] as const) {
      const nullResourceId = `${resourceId}-${scope}-null-retraction`;
      const nullThreadId = `${threadId}-${scope}-null-retraction`;
      const createdAt = new Date('2026-01-01T00:00:00.000Z');
      await firstMemory.saveThread({
        thread: {
          id: nullThreadId,
          resourceId: nullResourceId,
          title: 'Thread',
          metadata: {},
          createdAt,
          updatedAt: createdAt,
        },
      });
      const record = await firstMemory.initializeObservationalMemory({
        config: { _managedWorkingMemoryScope: scope },
        resourceId: nullResourceId,
        scope,
        threadId: scope === 'resource' ? null : nullThreadId,
      });
      const coordinates =
        scope === 'resource'
          ? ({ scope, resourceId: nullResourceId, threadId: nullThreadId } as const)
          : ({ scope, resourceId: nullResourceId, threadId: nullThreadId } as const);
      const guard = {
        recordId: record.id,
        resourceId: nullResourceId,
        threadId: scope === 'resource' ? null : nullThreadId,
      };
      const observed = await firstMemory.applyWorkingMemoryUpdate({
        ...coordinates,
        value: '{"temporary":true}',
        expectedRevision: 0,
        source: 'observer',
        observationalMemoryGuard: guard,
      });
      await firstMemory.applyWorkingMemoryUpdate({
        ...coordinates,
        value: null,
        expectedRevision: observed.revision,
        source: 'observer',
        observationalMemoryGuard: guard,
      });

      await secondMemory.retractObservationalMemory({ resourceId: nullResourceId, threadId: nullThreadId });

      await expect(firstMemory.getWorkingMemorySnapshot(coordinates)).resolves.toMatchObject({
        value: null,
        revision: 3,
        provenance: {},
      });
    }
  });

  it('filters inactive threads strictly before pagination', async () => {
    const prefix = `${threadId}-retention`;
    for (const [suffix, updatedAt] of [
      ['older', '2026-01-01T00:00:00.000Z'],
      ['boundary', '2026-02-01T00:00:00.000Z'],
      ['newer', '2026-03-01T00:00:00.000Z'],
    ] as const) {
      const date = new Date(updatedAt);
      await firstMemory.saveThread({
        thread: {
          id: `${prefix}-${suffix}`,
          resourceId: `${resourceId}-retention`,
          title: suffix,
          metadata: {},
          createdAt: date,
          updatedAt: date,
        },
      });
    }

    const result = await firstMemory.listThreads({
      filter: {
        resourceId: `${resourceId}-retention`,
        updatedBefore: new Date('2026-02-01T00:00:00.000Z'),
      },
      perPage: 1,
    });
    expect(result.threads.map(thread => thread.id)).toEqual([`${prefix}-older`]);
    expect(result.total).toBe(1);
    await expect(firstMemory.listThreads({ filter: { updatedBefore: new Date(Number.NaN) } })).rejects.toMatchObject({
      id: expect.stringContaining('INVALID_UPDATED_BEFORE'),
    });
  });

  it('retains protected thread Working Memory during atomic OM retraction', async () => {
    const protectedResourceId = `${resourceId}-thread-retraction`;
    const protectedThreadId = `${threadId}-thread-retraction`;
    const createdAt = new Date('2026-01-01T00:00:00.000Z');
    await firstMemory.saveThread({
      thread: {
        id: protectedThreadId,
        resourceId: protectedResourceId,
        title: 'Thread',
        metadata: {},
        createdAt,
        updatedAt: createdAt,
      },
    });
    const record = await firstMemory.initializeObservationalMemory({
      config: { _managedWorkingMemoryScope: 'thread' },
      resourceId: protectedResourceId,
      scope: 'thread',
      threadId: protectedThreadId,
    });
    const owner = await firstMemory.applyWorkingMemoryUpdate({
      scope: 'thread',
      resourceId: protectedResourceId,
      threadId: protectedThreadId,
      value: '{"preference":"concise"}',
      expectedRevision: 0,
      source: 'owner',
      protectPaths: ['/preference'],
    });
    await secondMemory.applyWorkingMemoryUpdate({
      scope: 'thread',
      resourceId: protectedResourceId,
      threadId: protectedThreadId,
      value: '{"preference":"verbose","temporaryTask":"draft"}',
      expectedRevision: owner.revision,
      source: 'observer',
      observationalMemoryGuard: {
        recordId: record.id,
        resourceId: protectedResourceId,
        threadId: protectedThreadId,
      },
    });

    await secondMemory.retractObservationalMemory({
      resourceId: protectedResourceId,
      threadId: protectedThreadId,
    });

    const retracted = await firstMemory.getWorkingMemorySnapshot({
      scope: 'thread',
      resourceId: protectedResourceId,
      threadId: protectedThreadId,
    });
    expect(JSON.parse(retracted.value!)).toEqual({ preference: 'concise' });
    expect(retracted.protectedPaths).toEqual(['/preference']);
    expect(retracted.provenance).toMatchObject({ '/preference': { source: 'owner' } });
  });
});
