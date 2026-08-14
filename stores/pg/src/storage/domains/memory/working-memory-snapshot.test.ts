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

    const staleSourcePreparation = await secondMemory.prepareThreadToResourceWorkingMemoryTransition({
      threadId: transitionThreadId,
      resourceId: transitionResourceId,
    });
    const updatedThreadSnapshot = await firstMemory.applyWorkingMemoryUpdate({
      scope: 'thread',
      resourceId: transitionResourceId,
      threadId: transitionThreadId,
      value: 'newer thread copy',
      expectedRevision: threadSnapshot.revision,
      source: 'owner',
    });
    await expect(
      secondMemory.transitionThreadToResourceWorkingMemory({
        mutation: { type: 'save', thread },
        value: 'canonical resource value',
        preparation: staleSourcePreparation,
      }),
    ).rejects.toMatchObject({ name: 'WorkingMemoryRevisionConflictError' });
    await expect(
      firstMemory.getWorkingMemorySnapshot({
        scope: 'thread',
        resourceId: transitionResourceId,
        threadId: transitionThreadId,
      }),
    ).resolves.toEqual(updatedThreadSnapshot);

    const staleDestinationPreparation = await secondMemory.prepareThreadToResourceWorkingMemoryTransition({
      threadId: transitionThreadId,
      resourceId: transitionResourceId,
    });
    const currentResource = await firstMemory.applyWorkingMemoryUpdate({
      scope: 'resource',
      resourceId: transitionResourceId,
      value: 'newer resource value',
      expectedRevision: staleDestinationPreparation.destinationResource.snapshot.revision,
      source: 'owner',
    });
    await expect(
      secondMemory.transitionThreadToResourceWorkingMemory({
        mutation: { type: 'save', thread },
        value: 'canonical resource value',
        preparation: staleDestinationPreparation,
      }),
    ).rejects.toMatchObject({ name: 'WorkingMemoryRevisionConflictError' });
    await expect(
      firstMemory.getWorkingMemorySnapshot({ scope: 'resource', resourceId: transitionResourceId }),
    ).resolves.toEqual(currentResource);
    await expect(
      firstMemory.getWorkingMemorySnapshot({
        scope: 'thread',
        resourceId: transitionResourceId,
        threadId: transitionThreadId,
      }),
    ).resolves.toEqual(updatedThreadSnapshot);

    const preparation = await secondMemory.prepareThreadToResourceWorkingMemoryTransition({
      threadId: transitionThreadId,
      resourceId: transitionResourceId,
    });
    const transitioned = await secondMemory.transitionThreadToResourceWorkingMemory({
      mutation: { type: 'save', thread },
      value: 'canonical resource value',
      preparation,
    });
    expect(transitioned.workingMemory).toMatchObject({ value: 'canonical resource value', revision: 3 });
    expect(transitioned.thread.metadata).toEqual({ preserved: true });
    await expect(
      firstMemory.getWorkingMemorySnapshot({
        scope: 'thread',
        resourceId: transitionResourceId,
        threadId: transitionThreadId,
      }),
    ).resolves.toEqual({ value: null, revision: 0, protectedPaths: [], provenance: {} });
  });

  it('rejects stale transition preparations after governed participants are recreated at the same revision', async () => {
    const createdAt = new Date('2026-01-01T00:00:00.000Z');
    const sourceResourceId = `${resourceId}-source-aba`;
    const sourceThreadId = `${threadId}-source-aba`;
    await firstMemory.saveThread({
      thread: {
        id: sourceThreadId,
        resourceId: sourceResourceId,
        title: 'Original source',
        metadata: {},
        createdAt,
        updatedAt: createdAt,
      },
    });
    const originalSourceSnapshot = await firstMemory.applyWorkingMemoryUpdate({
      scope: 'thread',
      resourceId: sourceResourceId,
      threadId: sourceThreadId,
      value: '{"owner":"Ada"}',
      expectedRevision: 0,
      source: 'owner',
      protectPaths: ['/owner'],
    });
    const originalSourceThread = await firstMemory.getThreadById({ threadId: sourceThreadId });
    if (!originalSourceThread) throw new Error('Expected original governed source thread.');
    const sourceDestination = await firstMemory.applyWorkingMemoryUpdate({
      scope: 'resource',
      resourceId: sourceResourceId,
      value: '{"destination":"original"}',
      expectedRevision: 0,
      source: 'owner',
    });
    const staleSourcePreparation = await secondMemory.prepareThreadToResourceWorkingMemoryTransition({
      threadId: sourceThreadId,
      resourceId: sourceResourceId,
    });

    await firstMemory.deleteThread({ threadId: sourceThreadId });
    await firstMemory.saveThread({ thread: originalSourceThread });
    const freshSourcePreparation = await firstMemory.prepareThreadToResourceWorkingMemoryTransition({
      threadId: sourceThreadId,
      resourceId: sourceResourceId,
    });
    expect(freshSourcePreparation.sourceThread.workingMemoryIncarnation).not.toBe(
      staleSourcePreparation.sourceThread.workingMemoryIncarnation,
    );
    await firstMemory.deleteThread({ threadId: sourceThreadId });
    await firstMemory.saveThread({
      thread: {
        id: sourceThreadId,
        resourceId: sourceResourceId,
        title: 'Replacement source',
        metadata: {},
        createdAt,
        updatedAt: new Date(),
      },
    });
    await firstMemory.saveThread({
      thread: {
        ...originalSourceThread,
        title: 'Replacement source',
        metadata: { ...originalSourceThread.metadata, replacement: true },
        updatedAt: new Date(),
      },
    });
    const replacementSource = await firstMemory.getWorkingMemorySnapshot({
      scope: 'thread',
      resourceId: sourceResourceId,
      threadId: sourceThreadId,
    });
    expect(replacementSource).toEqual(originalSourceSnapshot);
    const replacementSourcePreparation = await firstMemory.prepareThreadToResourceWorkingMemoryTransition({
      threadId: sourceThreadId,
      resourceId: sourceResourceId,
    });
    expect(replacementSourcePreparation.sourceThread.workingMemoryIncarnation).not.toBe(
      staleSourcePreparation.sourceThread.workingMemoryIncarnation,
    );

    await expect(
      secondMemory.transitionThreadToResourceWorkingMemory({
        mutation: { type: 'update', id: sourceThreadId, resourceId: sourceResourceId },
        value: '{"destination":"transitioned"}',
        preparation: staleSourcePreparation,
      }),
    ).rejects.toMatchObject({ name: 'WorkingMemoryRevisionConflictError' });
    await expect(
      firstMemory.getWorkingMemorySnapshot({
        scope: 'thread',
        resourceId: sourceResourceId,
        threadId: sourceThreadId,
      }),
    ).resolves.toEqual(replacementSource);
    await expect(
      firstMemory.getWorkingMemorySnapshot({ scope: 'resource', resourceId: sourceResourceId }),
    ).resolves.toEqual(sourceDestination);

    const destinationResourceId = `${resourceId}-destination-aba`;
    const destinationThreadId = `${threadId}-destination-aba`;
    await firstMemory.saveThread({
      thread: {
        id: destinationThreadId,
        resourceId: destinationResourceId,
        title: 'Destination ABA source',
        metadata: {},
        createdAt,
        updatedAt: createdAt,
      },
    });
    const destinationSource = await firstMemory.applyWorkingMemoryUpdate({
      scope: 'thread',
      resourceId: destinationResourceId,
      threadId: destinationThreadId,
      value: '{"source":"preserved"}',
      expectedRevision: 0,
      source: 'owner',
    });
    const originalDestinationSnapshot = await firstMemory.applyWorkingMemoryUpdate({
      scope: 'resource',
      resourceId: destinationResourceId,
      value: '{"owner":"Ada"}',
      expectedRevision: 0,
      source: 'owner',
      protectPaths: ['/owner'],
    });
    const originalDestinationResource = await firstMemory.getResourceById({ resourceId: destinationResourceId });
    if (!originalDestinationResource) throw new Error('Expected original governed destination resource.');
    const staleDestinationPreparation = await secondMemory.prepareThreadToResourceWorkingMemoryTransition({
      threadId: destinationThreadId,
      resourceId: destinationResourceId,
    });

    await firstMemory.deleteResource({ resourceId: destinationResourceId });
    await firstMemory.saveResource({ resource: originalDestinationResource });
    const freshDestinationPreparation = await firstMemory.prepareThreadToResourceWorkingMemoryTransition({
      threadId: destinationThreadId,
      resourceId: destinationResourceId,
    });
    expect(freshDestinationPreparation.destinationResource.workingMemoryIncarnation).not.toBe(
      staleDestinationPreparation.destinationResource.workingMemoryIncarnation,
    );
    await firstMemory.deleteResource({ resourceId: destinationResourceId });
    await firstMemory.saveResource({
      resource: {
        id: destinationResourceId,
        workingMemory: null,
        metadata: { replacement: 'ungoverned' },
        createdAt,
        updatedAt: new Date(),
      },
    });
    await firstMemory.saveResource({
      resource: {
        ...originalDestinationResource,
        metadata: { ...originalDestinationResource.metadata, replacement: true },
        updatedAt: new Date(),
      },
    });
    const replacementDestination = await firstMemory.getWorkingMemorySnapshot({
      scope: 'resource',
      resourceId: destinationResourceId,
    });
    expect(replacementDestination).toEqual(originalDestinationSnapshot);
    const replacementDestinationPreparation = await firstMemory.prepareThreadToResourceWorkingMemoryTransition({
      threadId: destinationThreadId,
      resourceId: destinationResourceId,
    });
    expect(replacementDestinationPreparation.destinationResource.workingMemoryIncarnation).not.toBe(
      staleDestinationPreparation.destinationResource.workingMemoryIncarnation,
    );

    await expect(
      secondMemory.transitionThreadToResourceWorkingMemory({
        mutation: { type: 'update', id: destinationThreadId, resourceId: destinationResourceId },
        value: '{"owner":"transitioned"}',
        preparation: staleDestinationPreparation,
      }),
    ).rejects.toMatchObject({ name: 'WorkingMemoryRevisionConflictError' });
    await expect(
      firstMemory.getWorkingMemorySnapshot({
        scope: 'thread',
        resourceId: destinationResourceId,
        threadId: destinationThreadId,
      }),
    ).resolves.toEqual(destinationSource);
    await expect(
      firstMemory.getWorkingMemorySnapshot({ scope: 'resource', resourceId: destinationResourceId }),
    ).resolves.toEqual(replacementDestination);
  });

  it.each([
    ['source thread', 'delete/recreate'],
    ['source thread', 'same-row update'],
    ['destination resource', 'delete/recreate'],
    ['destination resource', 'same-row update'],
  ] as const)(
    'rejects a stale transition after the %s plain Working Memory changes by %s at revision zero',
    async (replacedParticipant, replacementMode) => {
      const participantKey = replacedParticipant === 'source thread' ? 'source' : 'destination';
      const replacementKey = replacementMode === 'delete/recreate' ? 'recreated' : 'mutated';
      const transitionResourceId = `${resourceId}-plain-${participantKey}-${replacementKey}`;
      const transitionThreadId = `${threadId}-plain-${participantKey}-${replacementKey}`;
      const createdAt = new Date('2026-01-01T00:00:00.000Z');
      await firstMemory.saveThread({
        thread: {
          id: transitionThreadId,
          resourceId: transitionResourceId,
          title: 'Original source',
          metadata: { workingMemory: '{"owner":"Ada"}', plain: 'source' },
          createdAt,
          updatedAt: createdAt,
        },
      });
      await firstMemory.saveResource({
        resource: {
          id: transitionResourceId,
          workingMemory: '{"destination":"original"}',
          metadata: { plain: 'destination' },
          createdAt,
          updatedAt: createdAt,
        },
      });
      const preparation = await secondMemory.prepareThreadToResourceWorkingMemoryTransition({
        threadId: transitionThreadId,
        resourceId: transitionResourceId,
      });
      expect(preparation.sourceThread).toMatchObject({
        snapshot: { value: '{"owner":"Ada"}', revision: 0 },
        workingMemoryIncarnation: null,
      });
      expect(preparation.destinationResource).toMatchObject({
        snapshot: { value: '{"destination":"original"}', revision: 0 },
        workingMemoryIncarnation: null,
      });

      if (replacedParticipant === 'source thread') {
        if (replacementMode === 'delete/recreate') {
          await firstMemory.deleteThread({ threadId: transitionThreadId });
          await firstMemory.saveThread({
            thread: {
              id: transitionThreadId,
              resourceId: transitionResourceId,
              title: 'Replacement source',
              metadata: { workingMemory: '{"owner":"Grace"}', replacement: true },
              createdAt,
              updatedAt: new Date(),
            },
          });
        } else {
          await firstMemory.updateThread({
            id: transitionThreadId,
            metadata: { workingMemory: '{"owner":"Grace"}', replacement: true },
          });
        }
      } else if (replacementMode === 'delete/recreate') {
        await firstMemory.deleteResource({ resourceId: transitionResourceId });
        await firstMemory.saveResource({
          resource: {
            id: transitionResourceId,
            workingMemory: '{"destination":"replacement"}',
            metadata: { replacement: true },
            createdAt,
            updatedAt: new Date(),
          },
        });
      } else {
        await firstMemory.updateResource({
          resourceId: transitionResourceId,
          workingMemory: '{"destination":"replacement"}',
          metadata: { replacement: true },
        });
      }
      const replacementSource = await firstMemory.getWorkingMemorySnapshot({
        scope: 'thread',
        resourceId: transitionResourceId,
        threadId: transitionThreadId,
      });
      const replacementDestination = await firstMemory.getWorkingMemorySnapshot({
        scope: 'resource',
        resourceId: transitionResourceId,
      });

      await expect(
        secondMemory.transitionThreadToResourceWorkingMemory({
          mutation: { type: 'update', id: transitionThreadId, resourceId: transitionResourceId },
          value: preparation.sourceThread.snapshot.value!,
          preparation,
        }),
      ).rejects.toMatchObject({ name: 'WorkingMemoryRevisionConflictError' });
      await expect(
        firstMemory.getWorkingMemorySnapshot({
          scope: 'thread',
          resourceId: transitionResourceId,
          threadId: transitionThreadId,
        }),
      ).resolves.toEqual(replacementSource);
      await expect(
        firstMemory.getWorkingMemorySnapshot({ scope: 'resource', resourceId: transitionResourceId }),
      ).resolves.toEqual(replacementDestination);
    },
  );

  it('merges resource working memory transitions into the thread row locked by PostgreSQL', async () => {
    const transitionResourceId = `${resourceId}-partial-transition`;
    const transitionThreadId = `${threadId}-partial-transition`;
    const createdAt = new Date('2026-01-01T00:00:00.000Z');
    await firstMemory.saveThread({
      thread: {
        id: transitionThreadId,
        resourceId: transitionResourceId,
        title: 'Initial title',
        metadata: { preserved: 'initial', mastra: { custom: true } },
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
    const preparation = await secondMemory.prepareThreadToResourceWorkingMemoryTransition({
      threadId: transitionThreadId,
      resourceId: transitionResourceId,
    });

    const blocker = new Pool({ connectionString });
    const client = await blocker.connect();
    let transactionOpen = false;
    try {
      await client.query('BEGIN');
      transactionOpen = true;
      await client.query('SELECT pg_advisory_xact_lock(hashtextextended($1, 0))', [
        `mastra:observational-memory:${transitionResourceId}`,
      ]);

      const transitionPromise = secondMemory.transitionThreadToResourceWorkingMemory({
        mutation: {
          type: 'update',
          id: transitionThreadId,
          resourceId: transitionResourceId,
        },
        value: 'first resource value',
        preparation,
      });
      const completedWhileLockHeld = await Promise.race([
        transitionPromise.then(() => true),
        new Promise<false>(resolve => setTimeout(() => resolve(false), 250)),
      ]);
      expect(completedWhileLockHeld).toBe(false);

      await firstMemory.updateThread({
        id: transitionThreadId,
        title: 'Concurrent title',
        metadata: { concurrent: true },
      });
      await client.query('COMMIT');
      transactionOpen = false;

      const transitioned = await transitionPromise;
      expect(transitioned.thread.title).toBe('Concurrent title');
      expect(transitioned.thread.metadata).toEqual({
        preserved: 'initial',
        concurrent: true,
        mastra: { custom: true },
      });
      expect(transitioned.workingMemory).toMatchObject({ value: 'first resource value', revision: 1 });

      const explicitPreparation = await secondMemory.prepareThreadToResourceWorkingMemoryTransition({
        threadId: transitionThreadId,
        resourceId: transitionResourceId,
      });
      const explicitlyUpdated = await secondMemory.transitionThreadToResourceWorkingMemory({
        mutation: {
          type: 'update',
          id: transitionThreadId,
          resourceId: transitionResourceId,
          title: 'Explicit transition title',
          metadata: { explicit: true, mastra: null },
        },
        value: 'second resource value',
        preparation: explicitPreparation,
      });
      expect(explicitlyUpdated.thread.title).toBe('Explicit transition title');
      expect(explicitlyUpdated.thread.metadata).toEqual({
        preserved: 'initial',
        concurrent: true,
        explicit: true,
        mastra: null,
      });
    } finally {
      if (transactionOpen) await client.query('ROLLBACK');
      client.release();
      await blocker.end();
    }
  });

  it('preserves a governed incarnation across an atomic whole-row observer no-op save', async () => {
    const atomicResourceId = `${resourceId}-atomic-save-incarnation`;
    const atomicThreadId = `${threadId}-atomic-save-incarnation`;
    const createdAt = new Date('2026-01-01T00:00:00.000Z');
    const created = await firstMemory.mutateThreadWithWorkingMemory({
      mutation: {
        type: 'save',
        thread: {
          id: atomicThreadId,
          resourceId: atomicResourceId,
          title: 'Created',
          metadata: { initial: true },
          createdAt,
          updatedAt: createdAt,
        },
      },
      workingMemory: {
        type: 'observer-update',
        resourceId: atomicResourceId,
        value: '{"version":1}',
        expectedRevision: 0,
      },
    });
    const before = await firstMemory.prepareThreadToResourceWorkingMemoryTransition({
      threadId: atomicThreadId,
      resourceId: atomicResourceId,
    });
    expect(before.sourceThread.workingMemoryIncarnation).not.toBeNull();

    const saved = await secondMemory.mutateThreadWithWorkingMemory({
      mutation: {
        type: 'save',
        thread: {
          id: atomicThreadId,
          resourceId: atomicResourceId,
          title: 'Saved',
          metadata: { saved: true },
          createdAt,
          updatedAt: new Date(),
        },
      },
      workingMemory: {
        type: 'observer-update',
        resourceId: atomicResourceId,
        value: created.workingMemory!.value,
        expectedRevision: created.workingMemory!.revision,
      },
    });
    const after = await firstMemory.prepareThreadToResourceWorkingMemoryTransition({
      threadId: atomicThreadId,
      resourceId: atomicResourceId,
    });

    expect(saved.workingMemory).toEqual(created.workingMemory);
    expect(saved.thread).toMatchObject({ title: 'Saved', metadata: { saved: true, workingMemory: '{"version":1}' } });
    expect(after.sourceThread.workingMemoryIncarnation).toBe(before.sourceThread.workingMemoryIncarnation);

    const changed = await secondMemory.mutateThreadWithWorkingMemory({
      mutation: {
        type: 'save',
        thread: {
          id: atomicThreadId,
          resourceId: atomicResourceId,
          title: 'Changed',
          metadata: { changed: true },
          createdAt,
          updatedAt: new Date(),
        },
      },
      workingMemory: {
        type: 'observer-update',
        resourceId: atomicResourceId,
        value: '{"version":2}',
        expectedRevision: saved.workingMemory!.revision,
      },
    });
    const afterChanged = await firstMemory.prepareThreadToResourceWorkingMemoryTransition({
      threadId: atomicThreadId,
      resourceId: atomicResourceId,
    });
    expect(changed.workingMemory).toMatchObject({
      value: '{"version":2}',
      revision: saved.workingMemory!.revision + 1,
    });
    expect(afterChanged.sourceThread.workingMemoryIncarnation).toBe(before.sourceThread.workingMemoryIncarnation);
    await expect(
      secondMemory.transitionThreadToResourceWorkingMemory({
        mutation: { type: 'update', id: atomicThreadId, resourceId: atomicResourceId },
        value: '{"version":1}',
        preparation: after,
      }),
    ).rejects.toMatchObject({ name: 'WorkingMemoryRevisionConflictError' });
  });

  it('atomically mutates thread rows with governed thread Working Memory', async () => {
    const atomicResourceId = `${resourceId}-atomic-thread-row`;
    const atomicThreadId = `${threadId}-atomic-thread-row`;
    const createdAt = new Date('2026-01-01T00:00:00.000Z');
    const created = await firstMemory.mutateThreadWithWorkingMemory({
      mutation: {
        type: 'save',
        thread: {
          id: atomicThreadId,
          resourceId: atomicResourceId,
          title: 'Created',
          metadata: { preserved: true },
          createdAt,
          updatedAt: createdAt,
        },
      },
      workingMemory: {
        type: 'observer-update',
        resourceId: atomicResourceId,
        value: '{"version":1}',
        expectedRevision: 0,
      },
    });
    expect(created.thread).toMatchObject({
      title: 'Created',
      metadata: { preserved: true, workingMemory: '{"version":1}' },
    });
    expect(created.workingMemory).toMatchObject({ value: '{"version":1}', revision: 1 });

    const owner = await firstMemory.applyWorkingMemoryUpdate({
      scope: 'thread',
      resourceId: atomicResourceId,
      threadId: atomicThreadId,
      value: '{"keep":"1234"}',
      expectedRevision: 1,
      source: 'owner',
      protectPaths: ['/keep'],
    });
    const beforeRejectedMutation = await firstMemory.getThreadById({ threadId: atomicThreadId });
    await expect(
      secondMemory.mutateThreadWithWorkingMemory({
        mutation: {
          type: 'save',
          thread: {
            id: atomicThreadId,
            resourceId: `${atomicResourceId}-other`,
            title: 'Reassigned',
            metadata: { changed: true },
            createdAt,
            updatedAt: new Date('2026-01-02T00:00:00.000Z'),
          },
        },
        workingMemory: {
          type: 'observer-update',
          resourceId: `${atomicResourceId}-other`,
          value: '{"version":2}',
          expectedRevision: owner.revision,
        },
      }),
    ).rejects.toThrow('cannot be reassigned');
    await expect(firstMemory.getThreadById({ threadId: atomicThreadId })).resolves.toEqual(beforeRejectedMutation);

    await expect(
      secondMemory.mutateThreadWithWorkingMemory({
        mutation: {
          type: 'save',
          thread: {
            id: atomicThreadId,
            resourceId: atomicResourceId,
            title: 'Rejected save',
            metadata: { changed: true },
            createdAt,
            updatedAt: new Date('2026-01-02T00:00:00.000Z'),
          },
        },
        workingMemory: {
          type: 'observer-update',
          resourceId: atomicResourceId,
          value: '{"new":"5678"}',
          expectedRevision: owner.revision,
          maxDataBytes: 20,
        },
      }),
    ).rejects.toThrow('UTF-8 byte limit');
    await expect(firstMemory.getThreadById({ threadId: atomicThreadId })).resolves.toEqual(beforeRejectedMutation);

    await expect(
      secondMemory.mutateThreadWithWorkingMemory({
        mutation: { type: 'update', id: atomicThreadId, title: 'Rejected', metadata: { changed: true } },
        workingMemory: {
          type: 'observer-update',
          resourceId: atomicResourceId,
          value: '{"new":"5678"}',
          expectedRevision: owner.revision,
          maxDataBytes: 20,
        },
      }),
    ).rejects.toThrow('UTF-8 byte limit');
    await expect(firstMemory.getThreadById({ threadId: atomicThreadId })).resolves.toEqual(beforeRejectedMutation);

    await expect(
      secondMemory.mutateThreadWithWorkingMemory({
        mutation: { type: 'update', id: atomicThreadId, title: 'Stale', metadata: { changed: true } },
        workingMemory: {
          type: 'observer-update',
          resourceId: atomicResourceId,
          value: '{"version":2}',
          expectedRevision: owner.revision - 1,
        },
      }),
    ).rejects.toMatchObject({ name: 'WorkingMemoryRevisionConflictError' });
    await expect(firstMemory.getThreadById({ threadId: atomicThreadId })).resolves.toEqual(beforeRejectedMutation);

    await expect(
      secondMemory.mutateThreadWithWorkingMemory({
        mutation: { type: 'update', id: atomicThreadId, title: 'Hidden', metadata: { changed: true } },
        workingMemory: { type: 'require-ungoverned' },
      }),
    ).rejects.toThrow('explicit workingMemory value');
    await expect(firstMemory.getThreadById({ threadId: atomicThreadId })).resolves.toEqual(beforeRejectedMutation);

    const updated = await secondMemory.mutateThreadWithWorkingMemory({
      mutation: { type: 'update', id: atomicThreadId, title: 'Updated', metadata: { changed: true } },
      workingMemory: {
        type: 'observer-update',
        resourceId: atomicResourceId,
        value: '{"keep":"ignored","version":2}',
        expectedRevision: owner.revision,
      },
    });
    expect(updated.thread).toMatchObject({
      title: 'Updated',
      metadata: { preserved: true, changed: true, workingMemory: '{"keep":"1234","version":2}' },
    });
    expect(updated.workingMemory).toMatchObject({ revision: owner.revision + 1 });
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

  it('persists bounded high-cardinality provenance with exact escaped owner markers', async () => {
    const boundedProvenanceResourceId = `${resourceId}-bounded-provenance`;
    const maxDataBytes = 70_000;
    const initial = await firstMemory.applyWorkingMemoryUpdate({
      scope: 'resource',
      resourceId: boundedProvenanceResourceId,
      value: '{}',
      expectedRevision: 0,
      source: 'owner',
      maxDataBytes,
    });
    const owner = await firstMemory.applyWorkingMemoryUpdate({
      scope: 'resource',
      resourceId: boundedProvenanceResourceId,
      value: JSON.stringify(
        Object.fromEntries([
          ...Array.from({ length: 4_998 }, (_, index) => [`k${index}`, index]),
          ['owner/name', 'Ada'],
          ['owner~field', 'mathematics'],
        ]),
      ),
      expectedRevision: initial.revision,
      source: 'owner',
      maxDataBytes,
      protectPaths: ['/owner~1name', '/owner~0field'],
    });
    const observer = await secondMemory.applyWorkingMemoryUpdate({
      scope: 'resource',
      resourceId: boundedProvenanceResourceId,
      value: JSON.stringify(
        Object.fromEntries([
          ...Array.from({ length: 4_998 }, (_, index) => [`k${index}`, index + 1]),
          ['owner/name', 'ignored'],
          ['owner~field', 'ignored'],
        ]),
      ),
      expectedRevision: owner.revision,
      source: 'observer',
      maxDataBytes,
    });

    await expect(
      firstMemory.getWorkingMemorySnapshot({ scope: 'resource', resourceId: boundedProvenanceResourceId }),
    ).resolves.toEqual(observer);
    expect(Object.keys(observer.provenance).sort()).toEqual(['', '/owner~0field', '/owner~1name'].sort());
    expect(observer.provenance['']).toMatchObject({ source: 'observer' });
    expect(observer.provenance['/owner~0field']).toMatchObject({ source: 'owner' });
    expect(observer.provenance['/owner~1name']).toMatchObject({ source: 'owner' });
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
