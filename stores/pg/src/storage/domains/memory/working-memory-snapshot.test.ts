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

  it('guards generic thread, resource, whole-row, and legacy observer writes', async () => {
    const guardedResourceId = `${resourceId}-generic-guards`;
    const guardedThreadId = `${threadId}-generic-guards`;
    const createdAt = new Date('2026-01-01T00:00:00.000Z');
    await firstMemory.saveThread({
      thread: {
        id: guardedThreadId,
        resourceId: guardedResourceId,
        title: 'Thread',
        metadata: {},
        createdAt,
        updatedAt: createdAt,
      },
    });
    const threadOwner = await firstMemory.applyWorkingMemoryUpdate({
      scope: 'thread',
      resourceId: guardedResourceId,
      threadId: guardedThreadId,
      value: '{"preference":"concise"}',
      expectedRevision: 0,
      source: 'owner',
      protectPaths: ['/preference'],
    });
    const resourceOwner = await firstMemory.applyWorkingMemoryUpdate({
      scope: 'resource',
      resourceId: guardedResourceId,
      value: '{"name":"Ada"}',
      expectedRevision: 0,
      source: 'owner',
      protectPaths: ['/name'],
    });

    await secondMemory.updateThread({ id: guardedThreadId, metadata: { display: 'updated' } });
    await secondMemory.updateResource({ resourceId: guardedResourceId, metadata: { display: 'updated' } });
    await secondMemory.saveThread({
      thread: {
        id: guardedThreadId,
        resourceId: guardedResourceId,
        title: 'Saved',
        metadata: {},
        createdAt,
        updatedAt: createdAt,
      },
    });
    await secondMemory.saveResource({
      resource: { id: guardedResourceId, metadata: {}, createdAt, updatedAt: createdAt },
    });
    await expect(
      firstMemory.getWorkingMemorySnapshot({
        scope: 'thread',
        resourceId: guardedResourceId,
        threadId: guardedThreadId,
      }),
    ).resolves.toEqual(threadOwner);
    await expect(
      firstMemory.getWorkingMemorySnapshot({ scope: 'resource', resourceId: guardedResourceId }),
    ).resolves.toEqual(resourceOwner);

    await expect(
      secondMemory.updateThread({
        id: guardedThreadId,
        metadata: { workingMemory: '{"preference":"verbose"}' },
      }),
    ).rejects.toMatchObject({ name: 'WorkingMemoryValidationError' });
    await expect(
      secondMemory.updateResource({ resourceId: guardedResourceId, workingMemory: '{"name":"Grace"}' }),
    ).rejects.toMatchObject({ name: 'WorkingMemoryValidationError' });
    await expect(
      secondMemory.saveThread({
        thread: {
          id: guardedThreadId,
          resourceId: guardedResourceId,
          title: 'Rejected',
          metadata: { mastra: { workingMemory: { revision: 999 } } },
          createdAt,
          updatedAt: createdAt,
        },
      }),
    ).rejects.toMatchObject({ name: 'WorkingMemoryValidationError' });
    await expect(
      secondMemory.saveResource({
        resource: {
          id: guardedResourceId,
          workingMemory: '{"name":"Grace"}',
          metadata: {},
          createdAt,
          updatedAt: createdAt,
        },
      }),
    ).rejects.toMatchObject({ name: 'WorkingMemoryValidationError' });

    const record = await firstMemory.initializeObservationalMemory({
      config: { _managedWorkingMemoryScope: 'resource' },
      resourceId: guardedResourceId,
      scope: 'resource',
      threadId: null,
    });
    await expect(
      secondMemory.updateResourceFromObservationalMemory({
        resourceId: guardedResourceId,
        workingMemory: '{"name":"Grace"}',
        guard: { recordId: record.id, resourceId: guardedResourceId, threadId: null },
      }),
    ).rejects.toMatchObject({ name: 'WorkingMemoryValidationError' });
  });

  it('rejects cross-resource reassignment of governed threads while allowing ordinary reassignment', async () => {
    const guardedThreadId = `${threadId}-guarded-reassignment`;
    const originalResourceId = `${resourceId}-reassignment-a`;
    const reassignedResourceId = `${resourceId}-reassignment-b`;
    const createdAt = new Date('2026-01-01T00:00:00.000Z');
    await firstMemory.saveThread({
      thread: {
        id: guardedThreadId,
        resourceId: originalResourceId,
        title: 'Guarded thread',
        metadata: {},
        createdAt,
        updatedAt: createdAt,
      },
    });
    const snapshot = await firstMemory.applyWorkingMemoryUpdate({
      scope: 'thread',
      resourceId: originalResourceId,
      threadId: guardedThreadId,
      value: '{"owner":"Ada"}',
      expectedRevision: 0,
      source: 'owner',
      protectPaths: ['/owner'],
    });

    await expect(
      secondMemory.saveThread({
        thread: {
          id: guardedThreadId,
          resourceId: reassignedResourceId,
          title: 'Rejected reassignment',
          metadata: {},
          createdAt,
          updatedAt: new Date(),
        },
      }),
    ).rejects.toMatchObject({
      name: 'WorkingMemoryValidationError',
      message: 'Threads with revisioned working memory cannot be reassigned to another resource by saveThread.',
    });
    await expect(firstMemory.getThreadById({ threadId: guardedThreadId })).resolves.toMatchObject({
      resourceId: originalResourceId,
    });
    await expect(
      firstMemory.getWorkingMemorySnapshot({
        scope: 'thread',
        resourceId: originalResourceId,
        threadId: guardedThreadId,
      }),
    ).resolves.toEqual(snapshot);

    const ordinaryThreadId = `${threadId}-ordinary-reassignment`;
    await firstMemory.saveThread({
      thread: {
        id: ordinaryThreadId,
        resourceId: originalResourceId,
        title: 'Ordinary thread',
        metadata: {},
        createdAt,
        updatedAt: createdAt,
      },
    });
    await secondMemory.saveThread({
      thread: {
        id: ordinaryThreadId,
        resourceId: reassignedResourceId,
        title: 'Reassigned thread',
        metadata: {},
        createdAt,
        updatedAt: new Date(),
      },
    });
    await expect(firstMemory.getThreadById({ threadId: ordinaryThreadId })).resolves.toMatchObject({
      resourceId: reassignedResourceId,
    });
  });

  it('atomically moves governed thread Working Memory to its canonical resource', async () => {
    const transitionResourceId = `${resourceId}-scope-transition`;
    const transitionThreadId = `${threadId}-scope-transition`;
    const createdAt = new Date('2026-01-01T00:00:00.000Z');
    await firstMemory.saveThread({
      thread: {
        id: transitionThreadId,
        resourceId: transitionResourceId,
        title: 'Scope transition',
        metadata: {},
        createdAt,
        updatedAt: createdAt,
      },
    });
    const threadSnapshot = await firstMemory.applyWorkingMemoryUpdate({
      scope: 'thread',
      resourceId: transitionResourceId,
      threadId: transitionThreadId,
      value: 'stale thread copy',
      expectedRevision: 0,
      source: 'owner',
    });
    await firstMemory.applyWorkingMemoryUpdate({
      scope: 'resource',
      resourceId: transitionResourceId,
      value: 'current resource value',
      expectedRevision: 0,
      source: 'owner',
    });
    const thread = {
      id: transitionThreadId,
      resourceId: transitionResourceId,
      title: 'Scope transition',
      metadata: { preserved: true },
      createdAt,
      updatedAt: new Date(),
    };

    await expect(
      secondMemory.transitionThreadToResourceWorkingMemory({
        thread,
        value: 'canonical resource value',
        expectedRevision: 0,
      }),
    ).rejects.toMatchObject({ name: 'WorkingMemoryRevisionConflictError' });
    await expect(
      firstMemory.getWorkingMemorySnapshot({
        scope: 'thread',
        resourceId: transitionResourceId,
        threadId: transitionThreadId,
      }),
    ).resolves.toEqual(threadSnapshot);

    const currentResource = await firstMemory.getWorkingMemorySnapshot({
      scope: 'resource',
      resourceId: transitionResourceId,
    });
    const transitioned = await secondMemory.transitionThreadToResourceWorkingMemory({
      thread,
      value: 'canonical resource value',
      expectedRevision: currentResource.revision,
    });
    expect(transitioned.workingMemory).toMatchObject({ value: 'canonical resource value', revision: 2 });
    expect(transitioned.thread.metadata).toEqual({ preserved: true });
    await expect(
      firstMemory.getWorkingMemorySnapshot({
        scope: 'thread',
        resourceId: transitionResourceId,
        threadId: transitionThreadId,
      }),
    ).resolves.toEqual({ value: null, revision: 0, protectedPaths: [], provenance: {} });
  });

  it('serializes whole-row saves with owner compare-and-set updates for both scopes', async () => {
    for (const scope of ['resource', 'thread'] as const) {
      const concurrentResourceId = `${resourceId}-${scope}-save-cas`;
      const concurrentThreadId = `${threadId}-${scope}-save-cas`;
      const createdAt = new Date('2026-01-01T00:00:00.000Z');
      await firstMemory.saveThread({
        thread: {
          id: concurrentThreadId,
          resourceId: concurrentResourceId,
          title: 'Thread',
          metadata: {},
          createdAt,
          updatedAt: createdAt,
        },
      });
      const coordinates =
        scope === 'resource'
          ? ({ scope, resourceId: concurrentResourceId } as const)
          : ({ scope, resourceId: concurrentResourceId, threadId: concurrentThreadId } as const);
      const initial = await firstMemory.applyWorkingMemoryUpdate({
        ...coordinates,
        value: '{"version":1}',
        expectedRevision: 0,
        source: 'owner',
      });

      const save =
        scope === 'resource'
          ? secondMemory.saveResource({
              resource: { id: concurrentResourceId, metadata: {}, createdAt, updatedAt: createdAt },
            })
          : secondMemory.saveThread({
              thread: {
                id: concurrentThreadId,
                resourceId: concurrentResourceId,
                title: 'Saved concurrently',
                metadata: {},
                createdAt,
                updatedAt: createdAt,
              },
            });
      const [, owner] = await Promise.all([
        save,
        firstMemory.applyWorkingMemoryUpdate({
          ...coordinates,
          value: '{"version":2}',
          expectedRevision: initial.revision,
          source: 'owner',
        }),
      ]);

      await expect(firstMemory.getWorkingMemorySnapshot(coordinates)).resolves.toEqual(owner);
    }
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
