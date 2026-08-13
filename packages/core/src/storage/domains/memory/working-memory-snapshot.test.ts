import { describe, expect, it } from 'vitest';

import { InMemoryStore } from '../../mock';
import {
  applyWorkingMemorySnapshotUpdate,
  hasWorkingMemorySnapshotControls,
  normalizeWorkingMemoryPaths,
  readWorkingMemorySnapshot,
  retractObserverWorkingMemorySnapshot,
  WorkingMemoryRevisionConflictError,
  writeWorkingMemorySnapshotMetadata,
} from './working-memory-snapshot';

describe('revisioned Working Memory controls', () => {
  it('preserves owner-protected JSON paths across observer updates', () => {
    const owner = applyWorkingMemorySnapshotUpdate(
      { value: null, revision: 0, protectedPaths: [], provenance: {} },
      {
        value: JSON.stringify({ profile: { name: 'Ada', field: 'mathematics' }, focus: 'proofs' }),
        expectedRevision: 0,
        source: 'owner',
        protectPaths: ['/profile/name'],
      },
      '2026-01-01T00:00:00.000Z',
    );

    const observer = applyWorkingMemorySnapshotUpdate(
      owner,
      {
        value: JSON.stringify({ profile: { field: 'computing' }, focus: 'compilers' }),
        expectedRevision: owner.revision,
        source: 'observer',
      },
      '2026-01-02T00:00:00.000Z',
    );

    expect(JSON.parse(observer.value!)).toEqual({
      profile: { name: 'Ada', field: 'computing' },
      focus: 'compilers',
    });
    expect(observer.protectedPaths).toEqual(['/profile/name']);
    expect(observer.provenance['/profile/name']).toMatchObject({ source: 'owner' });
    expect(observer.provenance['/profile/field']).toMatchObject({ source: 'observer' });
  });

  it('keeps protected array-element provenance when an observer changes the array', () => {
    const owner = applyWorkingMemorySnapshotUpdate(
      { value: null, revision: 0, protectedPaths: [], provenance: {} },
      {
        value: JSON.stringify({ interests: ['proofs', 'history'] }),
        expectedRevision: 0,
        source: 'owner',
        protectPaths: ['/interests/0'],
      },
      '2026-01-01T00:00:00.000Z',
    );

    const observer = applyWorkingMemorySnapshotUpdate(
      owner,
      {
        value: JSON.stringify({ interests: ['compilers', 'music'] }),
        expectedRevision: owner.revision,
        source: 'observer',
      },
      '2026-01-02T00:00:00.000Z',
    );

    expect(JSON.parse(observer.value!)).toEqual({ interests: ['proofs', 'music'] });
    expect(observer.provenance['/interests']).toMatchObject({ source: 'observer' });
    expect(observer.provenance['/interests/0']).toMatchObject({ source: 'owner' });
  });

  it('preserves a protected root-array element when an observer proposes a scalar', () => {
    const owner = applyWorkingMemorySnapshotUpdate(
      { value: null, revision: 0, protectedPaths: [], provenance: {} },
      {
        value: JSON.stringify(['owner value', 'temporary value']),
        expectedRevision: 0,
        source: 'owner',
        protectPaths: ['/0'],
      },
    );

    const observer = applyWorkingMemorySnapshotUpdate(owner, {
      value: '"observer scalar"',
      expectedRevision: owner.revision,
      source: 'observer',
    });

    expect(JSON.parse(observer.value!)).toEqual(['owner value']);
    expect(observer.provenance['/0']).toMatchObject({ source: 'owner' });
  });

  it('preserves numeric object keys as objects during observer writes and retraction', () => {
    const owner = applyWorkingMemorySnapshotUpdate(
      { value: null, revision: 0, protectedPaths: [], provenance: {} },
      {
        value: JSON.stringify({ answers: { '0': 'owner value', stale: true } }),
        expectedRevision: 0,
        source: 'owner',
        protectPaths: ['/answers/0'],
      },
    );
    const observer = applyWorkingMemorySnapshotUpdate(owner, {
      value: JSON.stringify({ focus: 'new value' }),
      expectedRevision: owner.revision,
      source: 'observer',
    });

    expect(JSON.parse(observer.value!)).toEqual({ answers: { '0': 'owner value' }, focus: 'new value' });
    expect(JSON.parse(retractObserverWorkingMemorySnapshot(observer).value!)).toEqual({
      answers: { '0': 'owner value' },
    });
  });

  it('rejects stale revisions and observer control changes', () => {
    const current = { value: '{}', revision: 4, protectedPaths: [], provenance: {} } as const;
    expect(() =>
      applyWorkingMemorySnapshotUpdate(current, {
        value: '{}',
        expectedRevision: 3,
        source: 'owner',
      }),
    ).toThrow(WorkingMemoryRevisionConflictError);
    expect(() =>
      applyWorkingMemorySnapshotUpdate(current, {
        value: '{}',
        expectedRevision: 4,
        source: 'observer',
        protectPaths: ['/profile'],
      }),
    ).toThrow('cannot change protected paths');
  });

  it('rejects partial protection for malformed or scalar values', () => {
    const current = { value: null, revision: 0, protectedPaths: [], provenance: {} } as const;
    expect(() =>
      applyWorkingMemorySnapshotUpdate(current, {
        value: '{not-json',
        expectedRevision: 0,
        source: 'owner',
        protectPaths: ['/profile'],
      }),
    ).toThrow('JSON object or array');
    expect(() =>
      applyWorkingMemorySnapshotUpdate(current, {
        value: '"plain text"',
        expectedRevision: 0,
        source: 'owner',
        protectPaths: ['/profile'],
      }),
    ).toThrow('JSON object or array');
  });

  it('requires protected paths to exist until an owner explicitly unprotects them', () => {
    const owner = applyWorkingMemorySnapshotUpdate(
      { value: null, revision: 0, protectedPaths: [], provenance: {} },
      {
        value: '{"profile":{"name":"Ada"}}',
        expectedRevision: 0,
        source: 'owner',
        protectPaths: ['/profile/name'],
      },
    );

    expect(() =>
      applyWorkingMemorySnapshotUpdate(owner, {
        value: '{"profile":{}}',
        expectedRevision: owner.revision,
        source: 'owner',
      }),
    ).toThrow('must exist');
    expect(
      applyWorkingMemorySnapshotUpdate(owner, {
        value: '{"profile":{}}',
        expectedRevision: owner.revision,
        source: 'owner',
        unprotectPaths: ['/profile/name'],
      }),
    ).toMatchObject({ protectedPaths: [], value: '{"profile":{}}' });
  });

  it('rejects values beyond the configured UTF-8 byte limit', () => {
    const current = { value: null, revision: 0, protectedPaths: [], provenance: {} } as const;
    expect(() =>
      applyWorkingMemorySnapshotUpdate(current, {
        value: 'ééé',
        expectedRevision: 0,
        source: 'owner',
        maxDataBytes: 5,
      }),
    ).toThrow('UTF-8 byte limit');
    expect(() =>
      applyWorkingMemorySnapshotUpdate(current, {
        value: '',
        expectedRevision: 0,
        source: 'owner',
        maxDataBytes: 0,
      }),
    ).toThrow('positive safe integer');
  });

  it('applies the byte limit after protected owner values are merged', () => {
    const owner = applyWorkingMemorySnapshotUpdate(
      { value: null, revision: 0, protectedPaths: [], provenance: {} },
      {
        value: '{"keep":"1234"}',
        expectedRevision: 0,
        source: 'owner',
        protectPaths: ['/keep'],
      },
    );

    expect(() =>
      applyWorkingMemorySnapshotUpdate(owner, {
        value: '{"new":"5678"}',
        expectedRevision: owner.revision,
        source: 'observer',
        maxDataBytes: 20,
      }),
    ).toThrow('UTF-8 byte limit');
  });

  it('normalizes bounded RFC 6901 paths and rejects prototype paths', () => {
    expect(normalizeWorkingMemoryPaths(['/profile/name', '/profile', '/profile/name'])).toEqual(['/profile']);
    expect(() => normalizeWorkingMemoryPaths(['/profile/__proto__/polluted'])).toThrow('prototype fields');
    expect(() => normalizeWorkingMemoryPaths(['profile/name'])).toThrow('RFC 6901');
  });

  it('tracks objects with prototype-like data keys without creating invalid control pointers', () => {
    const initial = applyWorkingMemorySnapshotUpdate(
      { value: null, revision: 0, protectedPaths: [], provenance: {} },
      {
        value: '{"__proto__":"profile data","name":"Ada"}',
        expectedRevision: 0,
        source: 'owner',
        protectPaths: ['/name'],
      },
    );
    const updated = applyWorkingMemorySnapshotUpdate(initial, {
      value: '{"__proto__":"updated data","name":"Grace"}',
      expectedRevision: initial.revision,
      source: 'observer',
    });
    const metadata = writeWorkingMemorySnapshotMetadata({}, updated);

    expect(JSON.parse(updated.value!)).toEqual(JSON.parse('{"__proto__":"updated data","name":"Ada"}'));
    expect(updated.provenance['']).toMatchObject({ source: 'observer' });
    expect(updated.provenance['/name']).toMatchObject({ source: 'owner' });
    expect(() => readWorkingMemorySnapshot(updated.value, metadata)).not.toThrow();
  });

  it('keeps snapshots readable when JSON data keys exceed the control-pointer limit', () => {
    const initial = applyWorkingMemorySnapshotUpdate(
      { value: null, revision: 0, protectedPaths: [], provenance: {} },
      {
        value: '{}',
        expectedRevision: 0,
        source: 'observer',
      },
    );
    const longDataKey = 'x'.repeat(1024);
    const updated = applyWorkingMemorySnapshotUpdate(initial, {
      value: JSON.stringify({ [longDataKey]: true }),
      expectedRevision: initial.revision,
      source: 'observer',
    });

    expect(updated.provenance).toMatchObject({ '': { source: 'observer' } });
    expect(() =>
      readWorkingMemorySnapshot(updated.value, writeWorkingMemorySnapshotMetadata({}, updated)),
    ).not.toThrow();
  });

  it('distinguishes a cleared snapshot from the literal JSON null value', () => {
    const storedNull = applyWorkingMemorySnapshotUpdate(
      { value: null, revision: 0, protectedPaths: [], provenance: {} },
      { value: 'null', expectedRevision: 0, source: 'owner' },
    );
    const cleared = applyWorkingMemorySnapshotUpdate(storedNull, {
      value: null,
      expectedRevision: storedNull.revision,
      source: 'owner',
    });

    expect(storedNull).toMatchObject({ value: 'null', revision: 1 });
    expect(cleared).toMatchObject({ value: null, revision: 2 });
  });

  it('does not retain provenance tombstones for removed JSON paths', () => {
    const added = applyWorkingMemorySnapshotUpdate(
      { value: '{}', revision: 0, protectedPaths: [], provenance: {} },
      { value: '{"temporary":true}', expectedRevision: 0, source: 'observer' },
    );
    const removed = applyWorkingMemorySnapshotUpdate(added, {
      value: '{}',
      expectedRevision: added.revision,
      source: 'observer',
    });

    expect(added.provenance['/temporary']).toMatchObject({ source: 'observer' });
    expect(removed.provenance).not.toHaveProperty('/temporary');
  });

  it('round-trips controls without replacing unrelated metadata', () => {
    const snapshot = {
      value: '{"name":"Ada"}',
      revision: 2,
      protectedPaths: ['/name'],
      provenance: {
        '/name': { source: 'owner' as const, revision: 2, updatedAt: '2026-01-01T00:00:00.000Z' },
      },
    };
    const metadata = writeWorkingMemorySnapshotMetadata({ retained: true, mastra: { retained: true } }, snapshot);

    expect(hasWorkingMemorySnapshotControls(metadata)).toBe(true);
    expect(readWorkingMemorySnapshot(snapshot.value, metadata)).toEqual(snapshot);
    expect(metadata).toMatchObject({ retained: true, mastra: { retained: true } });
  });

  it('fails closed when stored controls are malformed', () => {
    const malformed = { mastra: { workingMemory: { revision: 'stale' } } };
    expect(hasWorkingMemorySnapshotControls(malformed)).toBe(true);
    expect(() => readWorkingMemorySnapshot('{"name":"Ada"}', malformed)).toThrow(
      'Stored working-memory controls are invalid',
    );
    expect(() => readWorkingMemorySnapshot('{"name":"Ada"}', { mastra: 'corrupt' })).toThrow(
      'Stored working-memory controls are invalid',
    );
  });

  it('fails closed instead of discarding malformed stored provenance', () => {
    const malformed = {
      mastra: {
        workingMemory: {
          revision: 2,
          protectedPaths: ['/name'],
          provenance: {
            '/name': { source: 'owner', revision: 3, updatedAt: 'not-a-timestamp' },
          },
        },
      },
    };

    expect(() => readWorkingMemorySnapshot('{"name":"Ada"}', malformed)).toThrow(
      'Stored working-memory controls are invalid',
    );
  });

  it('fails closed when a stored protected path is absent from the value', () => {
    const malformed = {
      mastra: {
        workingMemory: {
          revision: 1,
          protectedPaths: ['/profile/name'],
          provenance: {
            '/profile/name': {
              source: 'owner',
              revision: 1,
              updatedAt: '2026-01-01T00:00:00.000Z',
            },
          },
        },
      },
    };

    expect(() => readWorkingMemorySnapshot('{"profile":{}}', malformed)).toThrow(
      'Stored working-memory controls are invalid',
    );
  });

  it('retracts observer values but keeps owner-protected values', () => {
    const current = {
      value: JSON.stringify({ profile: { name: 'Ada', task: 'temporary' }, transient: true }),
      revision: 3,
      protectedPaths: ['/profile/name'],
      provenance: {
        '/profile/name': { source: 'owner' as const, revision: 2, updatedAt: '2026-01-01T00:00:00.000Z' },
        '/profile/task': { source: 'observer' as const, revision: 3, updatedAt: '2026-01-02T00:00:00.000Z' },
      },
    };

    const retracted = retractObserverWorkingMemorySnapshot(current);

    expect(JSON.parse(retracted.value!)).toEqual({ profile: { name: 'Ada' } });
    expect(retracted.revision).toBe(4);
    expect(retracted.provenance).toEqual({
      '/profile/name': { source: 'owner', revision: 2, updatedAt: '2026-01-01T00:00:00.000Z' },
    });
  });

  it('fails closed when a path-protected stored value is malformed', () => {
    const current = {
      value: '{not-json',
      revision: 1,
      protectedPaths: ['/profile/name'],
      provenance: {},
    };
    expect(retractObserverWorkingMemorySnapshot(current)).toBe(current);
  });

  it('keeps a root-protected opaque value while removing observer provenance', () => {
    const current = {
      value: 'Owner-authored notes',
      revision: 2,
      protectedPaths: [''],
      provenance: {
        '': { source: 'owner' as const, revision: 2, updatedAt: '2026-01-02T00:00:00.000Z' },
        '/stale': { source: 'observer' as const, revision: 1, updatedAt: '2026-01-01T00:00:00.000Z' },
      },
    };

    const retracted = retractObserverWorkingMemorySnapshot(current);

    expect(retracted.value).toBe('Owner-authored notes');
    expect(retracted.revision).toBe(3);
    expect(retracted.provenance).toEqual({
      '': { source: 'owner', revision: 2, updatedAt: '2026-01-02T00:00:00.000Z' },
    });
  });
});

describe('InMemoryMemory revisioned Working Memory', () => {
  it('enforces revisions, ownership coordinates, and protected observer writes', async () => {
    const storage = new InMemoryStore({ id: 'revisioned-working-memory' });
    const memory = await storage.getStore('memory');
    if (!memory) throw new Error('Expected in-memory storage domain.');
    const createdAt = new Date('2026-01-01T00:00:00.000Z');
    await memory.saveThread({
      thread: {
        id: 'thread-1',
        resourceId: 'resource-1',
        title: 'Thread',
        metadata: {},
        createdAt,
        updatedAt: createdAt,
      },
    });

    const owner = await memory.applyWorkingMemoryUpdate({
      scope: 'resource',
      resourceId: 'resource-1',
      value: '{"name":"Ada","focus":"proofs"}',
      expectedRevision: 0,
      source: 'owner',
      protectPaths: ['/name'],
    });
    await expect(
      memory.applyWorkingMemoryUpdate({
        scope: 'resource',
        resourceId: 'resource-1',
        value: '{"name":"Grace"}',
        expectedRevision: 0,
        source: 'observer',
      }),
    ).rejects.toThrow(WorkingMemoryRevisionConflictError);

    const observer = await memory.applyWorkingMemoryUpdate({
      scope: 'resource',
      resourceId: 'resource-1',
      value: '{"name":"Grace","focus":"compilers"}',
      expectedRevision: owner.revision,
      source: 'observer',
    });
    expect(JSON.parse(observer.value!)).toEqual({ name: 'Ada', focus: 'compilers' });

    await expect(
      memory.getWorkingMemorySnapshot({
        scope: 'thread',
        resourceId: 'resource-2',
        threadId: 'thread-1',
      }),
    ).rejects.toThrow('requested resource');
  });

  it('does not expose persisted controls through mutable update results', async () => {
    const storage = new InMemoryStore({ id: 'working-memory-control-aliasing' });
    const memory = await storage.getStore('memory');
    if (!memory) throw new Error('Expected in-memory storage domain.');

    const result = await memory.applyWorkingMemoryUpdate({
      scope: 'resource',
      resourceId: 'resource-control-aliasing',
      value: '{"name":"Ada"}',
      expectedRevision: 0,
      source: 'owner',
      protectPaths: ['/name'],
    });
    result.protectedPaths.length = 0;
    delete result.provenance['/name'];

    await expect(
      memory.getWorkingMemorySnapshot({ scope: 'resource', resourceId: 'resource-control-aliasing' }),
    ).resolves.toMatchObject({
      protectedPaths: ['/name'],
      provenance: { '/name': { source: 'owner' } },
    });
  });

  it('rolls back OM deletion when working-memory retraction validation fails', async () => {
    const storage = new InMemoryStore({ id: 'working-memory-retraction-rollback' });
    const memory = await storage.getStore('memory');
    if (!memory) throw new Error('Expected in-memory storage domain.');
    const resourceId = 'resource-retraction-rollback';
    const threadId = 'thread-retraction-rollback';
    const record = await memory.initializeObservationalMemory({
      config: { _managedWorkingMemoryScope: 'resource' },
      resourceId,
      scope: 'resource',
      threadId: null,
    });
    const createdAt = new Date('2026-01-01T00:00:00.000Z');
    await memory.saveResource({
      resource: {
        id: resourceId,
        workingMemory: '{"name":"Ada"}',
        metadata: { mastra: { workingMemory: { revision: 'invalid' } } },
        createdAt,
        updatedAt: createdAt,
      },
    });

    await expect(memory.retractObservationalMemory({ resourceId, threadId })).rejects.toThrow(
      'Stored working-memory controls are invalid',
    );
    await expect(memory.getObservationalMemory(null, resourceId)).resolves.toMatchObject({ id: record.id });
  });

  it('persists metadata-only observer cleanup after the resource value was cleared', async () => {
    const storage = new InMemoryStore({ id: 'working-memory-null-retraction' });
    const memory = await storage.getStore('memory');
    if (!memory) throw new Error('Expected in-memory storage domain.');
    const resourceId = 'resource-null-retraction';
    const threadId = 'thread-null-retraction';
    const createdAt = new Date('2026-01-01T00:00:00.000Z');
    await memory.saveThread({
      thread: { id: threadId, resourceId, title: 'Thread', metadata: {}, createdAt, updatedAt: createdAt },
    });
    const record = await memory.initializeObservationalMemory({
      config: { _managedWorkingMemoryScope: 'resource' },
      resourceId,
      scope: 'resource',
      threadId: null,
    });
    const guard = { recordId: record.id, resourceId, threadId: null } as const;
    const observed = await memory.applyWorkingMemoryUpdate({
      scope: 'resource',
      resourceId,
      threadId,
      value: '{"temporary":true}',
      expectedRevision: 0,
      source: 'observer',
      observationalMemoryGuard: guard,
    });
    await memory.applyWorkingMemoryUpdate({
      scope: 'resource',
      resourceId,
      threadId,
      value: null,
      expectedRevision: observed.revision,
      source: 'observer',
      observationalMemoryGuard: guard,
    });

    await memory.retractObservationalMemory({ resourceId, threadId });

    await expect(memory.getWorkingMemorySnapshot({ scope: 'resource', resourceId })).resolves.toMatchObject({
      value: null,
      revision: 3,
      provenance: {},
    });
  });

  it('applies updatedBefore strictly before pagination', async () => {
    const storage = new InMemoryStore({ id: 'working-memory-retention-filter' });
    const memory = await storage.getStore('memory');
    if (!memory) throw new Error('Expected in-memory storage domain.');
    for (const [id, updatedAt] of [
      ['older', '2026-01-01T00:00:00.000Z'],
      ['boundary', '2026-02-01T00:00:00.000Z'],
      ['newer', '2026-03-01T00:00:00.000Z'],
    ] as const) {
      await memory.saveThread({
        thread: {
          id,
          resourceId: 'resource-1',
          title: id,
          metadata: {},
          createdAt: new Date(updatedAt),
          updatedAt: new Date(updatedAt),
        },
      });
    }

    const result = await memory.listThreads({
      filter: { updatedBefore: new Date('2026-02-01T00:00:00.000Z') },
      perPage: 1,
    });
    expect(result.threads.map(thread => thread.id)).toEqual(['older']);
    expect(result.total).toBe(1);
    await expect(memory.listThreads({ filter: { updatedBefore: new Date(Number.NaN) } })).rejects.toThrow('valid Date');
  });

  it('retains protected thread Working Memory during OM retraction', async () => {
    const storage = new InMemoryStore({ id: 'thread-working-memory-retraction' });
    const memory = await storage.getStore('memory');
    if (!memory) throw new Error('Expected in-memory storage domain.');
    const createdAt = new Date('2026-01-01T00:00:00.000Z');
    const resourceId = 'thread-retraction-resource';
    const threadId = 'thread-retraction-thread';
    await memory.saveThread({
      thread: { id: threadId, resourceId, title: 'Thread', metadata: {}, createdAt, updatedAt: createdAt },
    });
    const record = await memory.initializeObservationalMemory({
      config: { _managedWorkingMemoryScope: 'thread' },
      resourceId,
      scope: 'thread',
      threadId,
    });
    const owner = await memory.applyWorkingMemoryUpdate({
      scope: 'thread',
      resourceId,
      threadId,
      value: '{"preference":"concise"}',
      expectedRevision: 0,
      source: 'owner',
      protectPaths: ['/preference'],
    });
    await memory.applyWorkingMemoryUpdate({
      scope: 'thread',
      resourceId,
      threadId,
      value: '{"preference":"verbose","temporaryTask":"draft"}',
      expectedRevision: owner.revision,
      source: 'observer',
      observationalMemoryGuard: { recordId: record.id, resourceId, threadId },
    });

    await memory.retractObservationalMemory({ resourceId, threadId });

    const retracted = await memory.getWorkingMemorySnapshot({ scope: 'thread', resourceId, threadId });
    expect(JSON.parse(retracted.value!)).toEqual({ preference: 'concise' });
    expect(retracted.protectedPaths).toEqual(['/preference']);
    expect(retracted.provenance).toMatchObject({ '/preference': { source: 'owner' } });
  });

  it('persists metadata-only resource provenance cleanup during OM retraction', async () => {
    const storage = new InMemoryStore({ id: 'resource-working-memory-metadata-retraction' });
    const memory = await storage.getStore('memory');
    if (!memory) throw new Error('Expected in-memory storage domain.');
    const resourceId = 'resource-metadata-retraction';
    const threadId = 'thread-metadata-retraction';
    const createdAt = new Date('2026-01-01T00:00:00.000Z');
    await memory.saveThread({
      thread: { id: threadId, resourceId, title: 'Thread', metadata: {}, createdAt, updatedAt: createdAt },
    });
    await memory.initializeObservationalMemory({
      config: { _managedWorkingMemoryScope: 'resource' },
      resourceId,
      scope: 'resource',
      threadId: null,
    });
    await memory.saveResource({
      resource: {
        id: resourceId,
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
        createdAt,
        updatedAt: createdAt,
      },
    });

    await expect(memory.retractObservationalMemory({ resourceId, threadId })).resolves.toMatchObject({
      clearedResourceWorkingMemory: false,
    });
    await expect(memory.getWorkingMemorySnapshot({ scope: 'resource', resourceId })).resolves.toMatchObject({
      value: '{"name":"Ada"}',
      revision: 3,
      provenance: { '': { source: 'owner' } },
    });
  });
});
