import { describe, expect, it } from 'vitest';

import { InMemoryStore } from '../../mock';
import {
  applyWorkingMemorySnapshotUpdate,
  assertWorkingMemorySnapshotUnchanged,
  hasWorkingMemorySnapshotControls,
  normalizeWorkingMemoryPaths,
  readWorkingMemoryIncarnation,
  readWorkingMemorySnapshot,
  reincarnateWorkingMemorySnapshotMetadata,
  retractObserverWorkingMemorySnapshot,
  WorkingMemoryRevisionConflictError,
  WorkingMemoryValidationError,
  writeWorkingMemorySnapshotMetadata,
} from './working-memory-snapshot';

describe('revisioned Working Memory controls', () => {
  const nestedJson = (depth: number, leaf: number) => `${'{"v":'.repeat(depth)}${leaf}${'}'.repeat(depth)}`;

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

  it('treats leading-zero pointer tokens as object keys, not array indexes', () => {
    const objectOwner = applyWorkingMemorySnapshotUpdate(
      { value: null, revision: 0, protectedPaths: [], provenance: {} },
      {
        value: JSON.stringify({ answers: { '01': 'owner value', stale: true } }),
        expectedRevision: 0,
        source: 'owner',
        protectPaths: ['/answers/01'],
      },
    );
    const unprotected = applyWorkingMemorySnapshotUpdate(objectOwner, {
      value: JSON.stringify({ answers: {} }),
      expectedRevision: objectOwner.revision,
      source: 'owner',
      unprotectPaths: ['/answers/01'],
    });

    expect(unprotected).toMatchObject({ value: '{"answers":{}}', protectedPaths: [] });
    expect(() =>
      applyWorkingMemorySnapshotUpdate(
        { value: null, revision: 0, protectedPaths: [], provenance: {} },
        {
          value: JSON.stringify(['zero', 'one']),
          expectedRevision: 0,
          source: 'owner',
          protectPaths: ['/01'],
        },
      ),
    ).toThrow('must exist');
  });

  it('reconstructs protected leading-zero object keys as objects during retraction', () => {
    const owner = applyWorkingMemorySnapshotUpdate(
      { value: null, revision: 0, protectedPaths: [], provenance: {} },
      {
        value: JSON.stringify({ '01': 'owner value', stale: true }),
        expectedRevision: 0,
        source: 'owner',
        protectPaths: ['/01'],
      },
    );
    const observer = applyWorkingMemorySnapshotUpdate(owner, {
      value: JSON.stringify({ transient: true }),
      expectedRevision: owner.revision,
      source: 'observer',
    });

    expect(JSON.parse(observer.value!)).toEqual({ '01': 'owner value', transient: true });
    expect(JSON.parse(retractObserverWorkingMemorySnapshot(observer).value!)).toEqual({ '01': 'owner value' });
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

  it('bounds high-cardinality provenance independently from value bytes while preserving escaped owner paths', () => {
    const maxDataBytes = 70_000;
    const ownerValue = JSON.stringify(
      Object.fromEntries([
        ...Array.from({ length: 4_998 }, (_, index) => [`k${index}`, index]),
        ['owner/name', 'Ada'],
        ['owner~field', 'mathematics'],
      ]),
    );
    expect(new TextEncoder().encode(ownerValue).byteLength).toBeLessThanOrEqual(maxDataBytes);

    const initial = applyWorkingMemorySnapshotUpdate(
      { value: null, revision: 0, protectedPaths: [], provenance: {} },
      { value: '{}', expectedRevision: 0, source: 'owner', maxDataBytes },
      '2026-01-01T00:00:00.000Z',
    );
    const owner = applyWorkingMemorySnapshotUpdate(
      initial,
      {
        value: ownerValue,
        expectedRevision: initial.revision,
        source: 'owner',
        maxDataBytes,
        protectPaths: ['/owner~1name', '/owner~0field'],
      },
      '2026-01-02T00:00:00.000Z',
    );

    expect(Object.keys(owner.provenance).sort()).toEqual(['', '/owner~0field', '/owner~1name'].sort());
    expect(owner.provenance['']).toMatchObject({ source: 'owner' });
    expect(owner.provenance['/owner~0field']).toMatchObject({ source: 'owner' });
    expect(owner.provenance['/owner~1name']).toMatchObject({ source: 'owner' });

    const observerValue = JSON.stringify(
      Object.fromEntries([
        ...Array.from({ length: 4_998 }, (_, index) => [`k${index}`, index + 1]),
        ['owner/name', 'ignored'],
        ['owner~field', 'ignored'],
      ]),
    );
    expect(new TextEncoder().encode(observerValue).byteLength).toBeLessThanOrEqual(maxDataBytes);
    const observer = applyWorkingMemorySnapshotUpdate(
      owner,
      {
        value: observerValue,
        expectedRevision: owner.revision,
        source: 'observer',
        maxDataBytes,
      },
      '2026-01-03T00:00:00.000Z',
    );

    expect(JSON.parse(observer.value!)).toMatchObject({
      k0: 1,
      'owner/name': 'Ada',
      'owner~field': 'mathematics',
    });
    expect(Object.keys(observer.provenance).sort()).toEqual(['', '/owner~0field', '/owner~1name'].sort());
    expect(observer.provenance['']).toMatchObject({ source: 'observer' });
    expect(observer.provenance['/owner~0field']).toMatchObject({ source: 'owner' });
    expect(observer.provenance['/owner~1name']).toMatchObject({ source: 'owner' });

    const metadata = writeWorkingMemorySnapshotMetadata({}, observer);
    expect(new TextEncoder().encode(JSON.stringify(metadata)).byteLength).toBeLessThan(1_024);
    expect(readWorkingMemorySnapshot(observer.value, metadata)).toEqual(observer);
  });

  it('normalizes bounded RFC 6901 paths and rejects prototype paths', () => {
    expect(normalizeWorkingMemoryPaths(['/profile/name', '/profile', '/profile/name'])).toEqual(['/profile']);
    expect(normalizeWorkingMemoryPaths(['/owner~1name', '/owner~0field'])).toEqual(['/owner~0field', '/owner~1name']);
    expect(() => normalizeWorkingMemoryPaths(['/profile/__proto__/polluted'])).toThrow('prototype fields');
    expect(() => normalizeWorkingMemoryPaths(['profile/name'])).toThrow('RFC 6901');
    expect(() => normalizeWorkingMemoryPaths(['/owner~2field'])).toThrow('RFC 6901');
    expect(() => normalizeWorkingMemoryPaths([`/${'x'.repeat(1_024)}`])).toThrow('bounded RFC 6901');
    expect(() => normalizeWorkingMemoryPaths(Array.from({ length: 257 }, (_, index) => `/k${index}`))).toThrow(
      'at most 256 protected paths',
    );
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

  it('preserves one opaque incarnation across revisions and retraction but rotates a new control lifetime', () => {
    const snapshot = {
      value: '{"name":"Ada","temporary":true}',
      revision: 1,
      protectedPaths: ['/name'],
      provenance: {
        '/name': { source: 'owner' as const, revision: 1, updatedAt: '2026-01-01T00:00:00.000Z' },
        '/temporary': { source: 'observer' as const, revision: 1, updatedAt: '2026-01-01T00:00:00.000Z' },
      },
    };
    const firstMetadata = writeWorkingMemorySnapshotMetadata({}, snapshot);
    const firstIncarnation = readWorkingMemoryIncarnation(firstMetadata);
    const retracted = retractObserverWorkingMemorySnapshot(snapshot);
    const revisedMetadata = writeWorkingMemorySnapshotMetadata(firstMetadata, retracted);
    expect(readWorkingMemoryIncarnation(revisedMetadata)).toBe(firstIncarnation);

    const reincarnatedMetadata = reincarnateWorkingMemorySnapshotMetadata(revisedMetadata)!;
    expect(readWorkingMemoryIncarnation(reincarnatedMetadata)).not.toBe(firstIncarnation);

    const legacyMetadata = structuredClone(revisedMetadata);
    delete (legacyMetadata.mastra.workingMemory as Record<string, unknown>).incarnation;
    expect(readWorkingMemorySnapshot(retracted.value, legacyMetadata)).toEqual(retracted);
    expect(() => readWorkingMemoryIncarnation(legacyMetadata)).toThrow('incarnation is invalid');
  });

  it('fails closed instead of serializing more than 256 protected paths', () => {
    const protectedPaths = Array.from({ length: 257 }, (_, index) => `/k${index}`);
    const value = JSON.stringify(Object.fromEntries(protectedPaths.map((_, index) => [`k${index}`, true])));

    expect(() =>
      writeWorkingMemorySnapshotMetadata({}, { value, revision: 1, protectedPaths, provenance: {} }),
    ).toThrow('at most 256 protected paths');
  });

  it('fails closed instead of serializing an invalid revision', () => {
    expect(() =>
      writeWorkingMemorySnapshotMetadata({}, { value: '{}', revision: -1, protectedPaths: [], provenance: {} }),
    ).toThrow('Stored working-memory controls are invalid');
  });

  it('rejects owner updates before unsafe JSON integers can be accepted or compare as unchanged', () => {
    expect(() =>
      applyWorkingMemorySnapshotUpdate(
        { value: null, revision: 0, protectedPaths: [], provenance: {} },
        {
          value: '{"count":9007199254740992}',
          expectedRevision: 0,
          source: 'owner',
        },
      ),
    ).toThrow('finite safe integers or finite decimals');

    expect(() =>
      applyWorkingMemorySnapshotUpdate(
        { value: '{"count":9007199254740992}', revision: 4, protectedPaths: [], provenance: {} },
        {
          value: '{"count":9007199254740993}',
          expectedRevision: 4,
          source: 'owner',
        },
      ),
    ).toThrow('finite safe integers or finite decimals');
  });

  it('rejects unsafe JSON integers during protected observer merges', () => {
    const current = {
      value: '{"profile":{"name":"Ada"},"count":1}',
      revision: 1,
      protectedPaths: ['/profile/name'],
      provenance: {
        '/profile/name': {
          source: 'owner' as const,
          revision: 1,
          updatedAt: '2026-01-01T00:00:00.000Z',
        },
      },
    };

    expect(() =>
      applyWorkingMemorySnapshotUpdate(current, {
        value: '{"profile":{"name":"Grace"},"count":9007199254740993}',
        expectedRevision: 1,
        source: 'observer',
      }),
    ).toThrow('finite safe integers or finite decimals');
  });

  it('rejects stored and serialized JSON values containing unsafe integers', () => {
    const snapshot = {
      value: '{"count":9007199254740993}',
      revision: 1,
      protectedPaths: [],
      provenance: {},
    };
    const metadata = {
      mastra: {
        workingMemory: {
          revision: snapshot.revision,
          protectedPaths: snapshot.protectedPaths,
          provenance: snapshot.provenance,
        },
      },
    };

    expect(() => readWorkingMemorySnapshot(snapshot.value, metadata)).toThrow(
      'finite safe integers or finite decimals',
    );
    expect(() => writeWorkingMemorySnapshotMetadata({}, snapshot)).toThrow('finite safe integers or finite decimals');
  });

  it('round-trips finite decimals and safe integers', () => {
    const snapshot = {
      value: '{"fraction":1.25,"integer":9007199254740991}',
      revision: 1,
      protectedPaths: [],
      provenance: {},
    };
    const metadata = writeWorkingMemorySnapshotMetadata({}, snapshot);

    expect(readWorkingMemorySnapshot(snapshot.value, metadata)).toEqual(snapshot);
  });

  it.each([
    ['array', () => JSON.stringify(Array.from({ length: 200_000 }, () => 0))],
    [
      'object',
      () => JSON.stringify(Object.fromEntries(Array.from({ length: 200_000 }, (_, index) => [`k${index}`, 0]))),
    ],
  ] as const)('validates a very large JSON %s without overflowing call arguments', (_kind, createValue) => {
    const value = createValue();
    const updatedAt = '2026-01-01T00:00:00.000Z';

    const updated = applyWorkingMemorySnapshotUpdate(
      { value: null, revision: 0, protectedPaths: [], provenance: {} },
      { value, expectedRevision: 0, source: 'owner' },
      updatedAt,
    );

    expect(updated).toMatchObject({
      value,
      revision: 1,
      protectedPaths: [],
      provenance: { '': { source: 'owner', revision: 1, updatedAt } },
    });
  });

  it.each([
    [
      'owner updates',
      (value: string) =>
        applyWorkingMemorySnapshotUpdate(
          { value: '{}', revision: 1, protectedPaths: [], provenance: {} },
          { value, expectedRevision: 1, source: 'owner' },
        ),
    ],
    [
      'observer updates',
      (value: string) =>
        applyWorkingMemorySnapshotUpdate(
          { value: '{}', revision: 1, protectedPaths: [], provenance: {} },
          { value, expectedRevision: 1, source: 'observer' },
        ),
    ],
    [
      'stored reads',
      (value: string) =>
        readWorkingMemorySnapshot(value, {
          mastra: { workingMemory: { revision: 1, protectedPaths: [], provenance: {} } },
        }),
    ],
    [
      'metadata serialization',
      (value: string) =>
        writeWorkingMemorySnapshotMetadata({}, { value, revision: 1, protectedPaths: [], provenance: {} }),
    ],
  ] as const)('rejects deeply nested JSON before %s can overflow', (_boundary, admit) => {
    let error: unknown;
    try {
      admit(nestedJson(5_000, 0));
    } catch (caught) {
      error = caught;
    }

    expect(error).toBeInstanceOf(WorkingMemoryValidationError);
    expect(error).toMatchObject({
      message: 'Working-memory JSON values may contain at most 256 nested arrays or objects.',
    });
  });

  it('accepts JSON at the 256-level nesting limit and rejects the next level', () => {
    const owner = applyWorkingMemorySnapshotUpdate(
      { value: null, revision: 0, protectedPaths: [], provenance: {} },
      { value: nestedJson(256, 0), expectedRevision: 0, source: 'owner' },
    );
    const observer = applyWorkingMemorySnapshotUpdate(owner, {
      value: nestedJson(256, 1),
      expectedRevision: owner.revision,
      source: 'observer',
    });
    const metadata = writeWorkingMemorySnapshotMetadata({}, observer);

    expect(readWorkingMemorySnapshot(observer.value, metadata)).toEqual(observer);
    expect(() =>
      readWorkingMemorySnapshot(nestedJson(257, 0), {
        mastra: { workingMemory: { revision: 1, protectedPaths: [], provenance: {} } },
      }),
    ).toThrow('Working-memory JSON values may contain at most 256 nested arrays or objects.');
  });

  it('rejects separately allocated cyclic ignored control properties with a validation error', () => {
    const createControl = () => {
      const control: Record<string, unknown> = { revision: 1, protectedPaths: [], provenance: {} };
      control.ignored = control;
      return control;
    };

    expect(() =>
      assertWorkingMemorySnapshotUnchanged({
        currentValue: '{}',
        currentMetadata: { mastra: { workingMemory: createControl() } },
        proposedValue: undefined,
        proposedValueProvided: false,
        proposedMetadata: { mastra: { workingMemory: createControl() } },
      }),
    ).toThrow(WorkingMemoryValidationError);
  });

  it('accepts equal acyclic ignored control properties deeper than shallow guard limits', () => {
    const createControl = () => {
      const control: Record<string, unknown> = { revision: 1, protectedPaths: [], provenance: {} };
      let cursor = control;
      for (let depth = 0; depth < 64; depth += 1) {
        const child: Record<string, unknown> = {};
        cursor.ignored = child;
        cursor = child;
      }
      cursor.value = 'leaf';
      return control;
    };

    expect(() =>
      assertWorkingMemorySnapshotUnchanged({
        currentValue: '{}',
        currentMetadata: { mastra: { workingMemory: createControl() } },
        proposedValue: undefined,
        proposedValueProvided: false,
        proposedMetadata: { mastra: { workingMemory: createControl() } },
      }),
    ).not.toThrow();
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
    malformed.mastra.workingMemory.provenance['/name'] = {
      source: 'owner',
      revision: 2,
      updatedAt: `${' '.repeat(41)}2026-01-01T00:00:00.000Z`,
    };
    expect(() => readWorkingMemorySnapshot('{"name":"Ada"}', malformed)).toThrow(
      'Stored working-memory controls are invalid',
    );
  });

  it('accepts exactly 257 provenance entries and fails closed above that deterministic cap', () => {
    const provenance = Object.fromEntries(
      Array.from({ length: 258 }, (_, index) => [
        `/k${index}`,
        { source: 'owner', revision: 1, updatedAt: '2026-01-01T00:00:00.000Z' },
      ]),
    );
    const overBudgetMetadata = {
      mastra: { workingMemory: { revision: 1, protectedPaths: [], provenance } },
    };
    const atBudgetMetadata = structuredClone(overBudgetMetadata);
    delete atBudgetMetadata.mastra.workingMemory.provenance['/k257'];

    expect(Object.keys(readWorkingMemorySnapshot('{}', atBudgetMetadata).provenance)).toHaveLength(257);
    expect(() => readWorkingMemorySnapshot('{}', overBudgetMetadata)).toThrow(
      'Stored working-memory controls are invalid',
    );
    expect(() =>
      writeWorkingMemorySnapshotMetadata(
        {},
        {
          value: '{}',
          revision: 1,
          protectedPaths: [],
          provenance,
        },
      ),
    ).toThrow('Stored working-memory controls are invalid');
  });

  it('fails closed when stored protected or provenance pointers exceed 1,024 characters', () => {
    const overlongPath = `/${'x'.repeat(1_024)}`;
    const entry = { source: 'owner', revision: 1, updatedAt: '2026-01-01T00:00:00.000Z' };

    expect(() =>
      readWorkingMemorySnapshot('{}', {
        mastra: { workingMemory: { revision: 1, protectedPaths: [overlongPath], provenance: {} } },
      }),
    ).toThrow('bounded RFC 6901');
    expect(() =>
      readWorkingMemorySnapshot('{}', {
        mastra: { workingMemory: { revision: 1, protectedPaths: [], provenance: { [overlongPath]: entry } } },
      }),
    ).toThrow('Stored working-memory controls are invalid');
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
  it('rejects owner path controls above their bounds without persisting a snapshot', async () => {
    const storage = new InMemoryStore({ id: 'working-memory-protected-path-bounds' });
    const memory = await storage.getStore('memory');
    if (!memory) throw new Error('Expected in-memory storage domain.');
    const resourceId = 'protected-path-bounds-resource';
    const empty = { value: null, revision: 0, protectedPaths: [], provenance: {} };

    await expect(
      memory.applyWorkingMemoryUpdate({
        scope: 'resource',
        resourceId,
        value: '{}',
        expectedRevision: 0,
        source: 'owner',
        protectPaths: Array.from({ length: 257 }, (_, index) => `/k${index}`),
      }),
    ).rejects.toThrow('at most 256 protected paths');
    await expect(memory.getWorkingMemorySnapshot({ scope: 'resource', resourceId })).resolves.toEqual(empty);

    await expect(
      memory.applyWorkingMemoryUpdate({
        scope: 'resource',
        resourceId,
        value: '{}',
        expectedRevision: 0,
        source: 'owner',
        protectPaths: [`/${'x'.repeat(1_024)}`],
      }),
    ).rejects.toThrow('bounded RFC 6901');
    await expect(memory.getWorkingMemorySnapshot({ scope: 'resource', resourceId })).resolves.toEqual(empty);
  });

  it('persists bounded provenance and retracts observer data without losing escaped owner paths', async () => {
    const storage = new InMemoryStore({ id: 'working-memory-bounded-provenance' });
    const memory = await storage.getStore('memory');
    if (!memory) throw new Error('Expected in-memory storage domain.');
    const resourceId = 'bounded-provenance-resource';
    const maxDataBytes = 70_000;
    const initial = await memory.applyWorkingMemoryUpdate({
      scope: 'resource',
      resourceId,
      value: '{}',
      expectedRevision: 0,
      source: 'owner',
      maxDataBytes,
    });
    const owner = await memory.applyWorkingMemoryUpdate({
      scope: 'resource',
      resourceId,
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
    const observer = await memory.applyWorkingMemoryUpdate({
      scope: 'resource',
      resourceId,
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

    await expect(memory.getWorkingMemorySnapshot({ scope: 'resource', resourceId })).resolves.toEqual(observer);
    expect(Object.keys(observer.provenance).sort()).toEqual(['', '/owner~0field', '/owner~1name'].sort());
    expect(JSON.parse(retractObserverWorkingMemorySnapshot(observer).value!)).toEqual({
      'owner/name': 'Ada',
      'owner~field': 'mathematics',
    });
  });

  it('rejects cross-resource reassignment of governed threads while allowing ordinary reassignment', async () => {
    const storage = new InMemoryStore({ id: 'working-memory-resource-reassignment' });
    const memory = await storage.getStore('memory');
    if (!memory) throw new Error('Expected in-memory storage domain.');
    const createdAt = new Date('2026-01-01T00:00:00.000Z');
    const guardedThreadId = 'guarded-reassignment-thread';
    const originalResourceId = 'guarded-reassignment-resource-a';
    const reassignedResourceId = 'guarded-reassignment-resource-b';

    await memory.saveThread({
      thread: {
        id: guardedThreadId,
        resourceId: originalResourceId,
        metadata: {},
        createdAt,
        updatedAt: createdAt,
      },
    });
    const snapshot = await memory.applyWorkingMemoryUpdate({
      scope: 'thread',
      resourceId: originalResourceId,
      threadId: guardedThreadId,
      value: '{"owner":"Ada"}',
      expectedRevision: 0,
      source: 'owner',
      protectPaths: ['/owner'],
    });

    await expect(
      memory.saveThread({
        thread: {
          id: guardedThreadId,
          resourceId: reassignedResourceId,
          metadata: {},
          createdAt,
          updatedAt: new Date(),
        },
      }),
    ).rejects.toMatchObject({
      name: 'WorkingMemoryValidationError',
      message: 'Threads with revisioned working memory cannot be reassigned to another resource by saveThread.',
    });
    await expect(memory.getThreadById({ threadId: guardedThreadId })).resolves.toMatchObject({
      resourceId: originalResourceId,
    });
    await expect(
      memory.getWorkingMemorySnapshot({
        scope: 'thread',
        resourceId: originalResourceId,
        threadId: guardedThreadId,
      }),
    ).resolves.toEqual(snapshot);

    const ordinaryThreadId = 'ordinary-reassignment-thread';
    await memory.saveThread({
      thread: {
        id: ordinaryThreadId,
        resourceId: originalResourceId,
        metadata: {},
        createdAt,
        updatedAt: createdAt,
      },
    });
    await memory.saveThread({
      thread: {
        id: ordinaryThreadId,
        resourceId: reassignedResourceId,
        metadata: {},
        createdAt,
        updatedAt: new Date(),
      },
    });
    await expect(memory.getThreadById({ threadId: ordinaryThreadId })).resolves.toMatchObject({
      resourceId: reassignedResourceId,
    });
  });

  it('atomically moves governed thread Working Memory to its canonical resource', async () => {
    const storage = new InMemoryStore({ id: 'working-memory-scope-transition' });
    const memory = await storage.getStore('memory');
    if (!memory) throw new Error('Expected in-memory storage domain.');
    const createdAt = new Date('2026-01-01T00:00:00.000Z');
    const resourceId = 'transition-resource';
    const threadId = 'transition-thread';
    await memory.saveThread({
      thread: { id: threadId, resourceId, metadata: {}, createdAt, updatedAt: createdAt },
    });
    const threadSnapshot = await memory.applyWorkingMemoryUpdate({
      scope: 'thread',
      resourceId,
      threadId,
      value: 'stale thread copy',
      expectedRevision: 0,
      source: 'owner',
    });
    const staleDestinationPreparation = await memory.prepareThreadToResourceWorkingMemoryTransition({
      threadId,
      resourceId,
    });
    const currentResource = await memory.applyWorkingMemoryUpdate({
      scope: 'resource',
      resourceId,
      value: 'current resource value',
      expectedRevision: 0,
      source: 'owner',
    });

    await expect(
      memory.transitionThreadToResourceWorkingMemory({
        mutation: {
          type: 'save',
          thread: {
            id: threadId,
            resourceId,
            metadata: { preserved: true },
            createdAt,
            updatedAt: new Date(),
          },
        },
        value: 'canonical resource value',
        preparation: staleDestinationPreparation,
      }),
    ).rejects.toMatchObject({ name: 'WorkingMemoryRevisionConflictError' });
    await expect(memory.getWorkingMemorySnapshot({ scope: 'thread', resourceId, threadId })).resolves.toEqual(
      threadSnapshot,
    );

    const currentPreparation = await memory.prepareThreadToResourceWorkingMemoryTransition({ threadId, resourceId });
    await expect(
      memory.transitionThreadToResourceWorkingMemory({
        mutation: {
          type: 'save',
          thread: {
            id: threadId,
            resourceId,
            metadata: { preserved: true },
            createdAt,
            updatedAt: new Date(),
          },
        },
        value: 'canonical resource value',
        preparation: {
          ...currentPreparation,
          sourceThread: {
            ...currentPreparation.sourceThread,
            snapshot: { ...currentPreparation.sourceThread.snapshot, revision: 0 },
          },
        },
      }),
    ).rejects.toMatchObject({ name: 'WorkingMemoryRevisionConflictError' });
    await expect(memory.getWorkingMemorySnapshot({ scope: 'resource', resourceId })).resolves.toEqual(currentResource);
    await expect(memory.getWorkingMemorySnapshot({ scope: 'thread', resourceId, threadId })).resolves.toEqual(
      threadSnapshot,
    );

    const transitioned = await memory.transitionThreadToResourceWorkingMemory({
      mutation: {
        type: 'save',
        thread: {
          id: threadId,
          resourceId,
          metadata: { preserved: true },
          createdAt,
          updatedAt: new Date(),
        },
      },
      value: 'canonical resource value',
      preparation: currentPreparation,
    });

    expect(transitioned.workingMemory).toMatchObject({ value: 'canonical resource value', revision: 2 });
    expect(transitioned.thread.metadata).toEqual({ preserved: true });
    await expect(memory.getWorkingMemorySnapshot({ scope: 'thread', resourceId, threadId })).resolves.toEqual({
      value: null,
      revision: 0,
      protectedPaths: [],
      provenance: {},
    });
  });

  it.each(['source thread', 'destination resource'] as const)(
    'rejects a stale transition after the %s control is recreated at the same revision',
    async replacedParticipant => {
      const storage = new InMemoryStore({ id: `working-memory-${replacedParticipant}-aba` });
      const memory = await storage.getStore('memory');
      if (!memory) throw new Error('Expected in-memory storage domain.');
      const createdAt = new Date('2026-01-01T00:00:00.000Z');
      const resourceId = `transition-${replacedParticipant}-resource`;
      const threadId = `transition-${replacedParticipant}-thread`;
      await memory.saveThread({
        thread: { id: threadId, resourceId, metadata: {}, createdAt, updatedAt: createdAt },
      });
      const source = await memory.applyWorkingMemoryUpdate({
        scope: 'thread',
        resourceId,
        threadId,
        value: '{"name":"Ada"}',
        expectedRevision: 0,
        source: 'owner',
        protectPaths: ['/name'],
      });
      const destination = await memory.applyWorkingMemoryUpdate({
        scope: 'resource',
        resourceId,
        value: '{"status":"original"}',
        expectedRevision: 0,
        source: 'owner',
        protectPaths: ['/status'],
      });
      const preparation = await memory.prepareThreadToResourceWorkingMemoryTransition({ threadId, resourceId });

      let expectedSource = source;
      let expectedDestination = destination;
      if (replacedParticipant === 'source thread') {
        await memory.deleteThread({ threadId });
        await memory.saveThread({
          thread: {
            id: threadId,
            resourceId,
            metadata: { replacement: true },
            createdAt,
            updatedAt: createdAt,
          },
        });
        expectedSource = await memory.applyWorkingMemoryUpdate({
          scope: 'thread',
          resourceId,
          threadId,
          value: '{"name":"Grace"}',
          expectedRevision: 0,
          source: 'owner',
          protectPaths: ['/name'],
        });
      } else {
        await memory.deleteResource({ resourceId });
        expectedDestination = await memory.applyWorkingMemoryUpdate({
          scope: 'resource',
          resourceId,
          value: '{"status":"replacement"}',
          expectedRevision: 0,
          source: 'owner',
          protectPaths: ['/status'],
        });
      }

      await expect(
        memory.transitionThreadToResourceWorkingMemory({
          mutation: { type: 'update', id: threadId, resourceId },
          value: source.value!,
          preparation,
        }),
      ).rejects.toMatchObject({ name: 'WorkingMemoryRevisionConflictError' });
      await expect(memory.getWorkingMemorySnapshot({ scope: 'thread', resourceId, threadId })).resolves.toEqual(
        expectedSource,
      );
      await expect(memory.getWorkingMemorySnapshot({ scope: 'resource', resourceId })).resolves.toEqual(
        expectedDestination,
      );
    },
  );

  it.each(['thread', 'resource'] as const)(
    're-incarnates caller-replayed %s controls on fresh and ungoverned rows',
    async scope => {
      const storage = new InMemoryStore({ id: `working-memory-${scope}-control-replay` });
      const memory = await storage.getStore('memory');
      if (!memory) throw new Error('Expected in-memory storage domain.');
      const createdAt = new Date('2026-01-01T00:00:00.000Z');
      const resourceId = `control-replay-${scope}-resource`;
      const threadId = `control-replay-${scope}-thread`;
      await memory.saveThread({
        thread: { id: threadId, resourceId, metadata: {}, createdAt, updatedAt: createdAt },
      });
      await memory.applyWorkingMemoryUpdate({
        scope,
        resourceId,
        ...(scope === 'thread' ? { threadId } : {}),
        value: '{"name":"Ada"}',
        expectedRevision: 0,
        source: 'owner',
        protectPaths: ['/name'],
      });
      const original = await memory.prepareThreadToResourceWorkingMemoryTransition({ threadId, resourceId });
      const originalIncarnation =
        scope === 'thread'
          ? original.sourceThread.workingMemoryIncarnation
          : original.destinationResource.workingMemoryIncarnation;
      expect(originalIncarnation).not.toBeNull();

      if (scope === 'thread') {
        const governed = await memory.getThreadById({ threadId });
        if (!governed) throw new Error('Expected governed thread.');
        await memory.deleteThread({ threadId });
        await memory.saveThread({ thread: governed });
        const fresh = await memory.prepareThreadToResourceWorkingMemoryTransition({ threadId, resourceId });
        expect(fresh.sourceThread.workingMemoryIncarnation).not.toBe(originalIncarnation);

        await memory.deleteThread({ threadId });
        await memory.saveThread({
          thread: { id: threadId, resourceId, metadata: {}, createdAt, updatedAt: createdAt },
        });
        await memory.updateThread({ id: threadId, metadata: governed.metadata });
        const reattached = await memory.prepareThreadToResourceWorkingMemoryTransition({ threadId, resourceId });
        expect(reattached.sourceThread.workingMemoryIncarnation).not.toBe(originalIncarnation);
      } else {
        const governed = await memory.getResourceById({ resourceId });
        if (!governed) throw new Error('Expected governed resource.');
        await memory.deleteResource({ resourceId });
        await memory.saveResource({ resource: governed });
        const fresh = await memory.prepareThreadToResourceWorkingMemoryTransition({ threadId, resourceId });
        expect(fresh.destinationResource.workingMemoryIncarnation).not.toBe(originalIncarnation);

        await memory.deleteResource({ resourceId });
        await memory.updateResource({ resourceId, metadata: {} });
        await memory.updateResource({
          resourceId,
          workingMemory: governed.workingMemory,
          metadata: governed.metadata,
        });
        const reattached = await memory.prepareThreadToResourceWorkingMemoryTransition({ threadId, resourceId });
        expect(reattached.destinationResource.workingMemoryIncarnation).not.toBe(originalIncarnation);
      }
    },
  );

  it('applies resource working memory transitions to the current in-memory thread row', async () => {
    const storage = new InMemoryStore({ id: 'working-memory-partial-scope-transition' });
    const memory = await storage.getStore('memory');
    if (!memory) throw new Error('Expected in-memory storage domain.');
    const createdAt = new Date('2026-01-01T00:00:00.000Z');
    const resourceId = 'partial-transition-resource';
    const threadId = 'partial-transition-thread';
    await memory.saveThread({
      thread: {
        id: threadId,
        resourceId,
        title: 'Initial title',
        metadata: { preserved: 'initial', mastra: { custom: true } },
        createdAt,
        updatedAt: createdAt,
      },
    });
    const threadSnapshot = await memory.applyWorkingMemoryUpdate({
      scope: 'thread',
      resourceId,
      threadId,
      value: 'stale thread copy',
      expectedRevision: 0,
      source: 'owner',
    });

    const preparation = await memory.prepareThreadToResourceWorkingMemoryTransition({ threadId, resourceId });
    const omittedFieldsTransition = {
      mutation: { type: 'update' as const, id: threadId, resourceId },
      value: 'first resource value',
      preparation,
    };
    await memory.updateThread({
      id: threadId,
      title: 'Concurrent title',
      metadata: { concurrent: true },
    });
    const transitioned = await memory.transitionThreadToResourceWorkingMemory(omittedFieldsTransition);

    expect(transitioned.thread.title).toBe('Concurrent title');
    expect(transitioned.thread.metadata).toEqual({
      preserved: 'initial',
      concurrent: true,
      mastra: { custom: true },
    });
    expect(transitioned.workingMemory).toMatchObject({ value: 'first resource value', revision: 1 });

    const explicitlyUpdated = await memory.transitionThreadToResourceWorkingMemory({
      mutation: {
        type: 'update',
        id: threadId,
        resourceId,
        title: 'Explicit transition title',
        metadata: { explicit: true, mastra: null },
      },
      value: 'second resource value',
      preparation: await memory.prepareThreadToResourceWorkingMemoryTransition({ threadId, resourceId }),
    });
    expect(explicitlyUpdated.thread.title).toBe('Explicit transition title');
    expect(explicitlyUpdated.thread.metadata).toEqual({
      preserved: 'initial',
      concurrent: true,
      explicit: true,
      mastra: null,
    });
  });

  it('atomically mutates thread rows with governed thread Working Memory', async () => {
    const storage = new InMemoryStore({ id: 'working-memory-atomic-thread-mutations' });
    const memory = await storage.getStore('memory');
    if (!memory) throw new Error('Expected in-memory storage domain.');
    const createdAt = new Date('2026-01-01T00:00:00.000Z');
    const resourceId = 'atomic-thread-resource';
    const threadId = 'atomic-thread';

    const created = await memory.mutateThreadWithWorkingMemory({
      mutation: {
        type: 'save',
        thread: {
          id: threadId,
          resourceId,
          title: 'Created',
          metadata: { preserved: true },
          createdAt,
          updatedAt: createdAt,
        },
      },
      workingMemory: {
        type: 'observer-update',
        resourceId,
        value: '{"version":1}',
        expectedRevision: 0,
      },
    });
    expect(created.thread).toMatchObject({
      title: 'Created',
      metadata: { preserved: true, workingMemory: '{"version":1}' },
    });
    expect(created.workingMemory).toMatchObject({ value: '{"version":1}', revision: 1 });

    const owner = await memory.applyWorkingMemoryUpdate({
      scope: 'thread',
      resourceId,
      threadId,
      value: '{"keep":"1234"}',
      expectedRevision: 1,
      source: 'owner',
      protectPaths: ['/keep'],
    });
    const beforeRejectedMutation = await memory.getThreadById({ threadId });
    await expect(
      memory.mutateThreadWithWorkingMemory({
        mutation: {
          type: 'save',
          thread: {
            id: threadId,
            resourceId: 'other-atomic-thread-resource',
            title: 'Reassigned',
            metadata: { changed: true },
            createdAt,
            updatedAt: new Date('2026-01-02T00:00:00.000Z'),
          },
        },
        workingMemory: {
          type: 'observer-update',
          resourceId: 'other-atomic-thread-resource',
          value: '{"version":2}',
          expectedRevision: owner.revision,
        },
      }),
    ).rejects.toThrow('cannot be reassigned');
    await expect(memory.getThreadById({ threadId })).resolves.toEqual(beforeRejectedMutation);

    await expect(
      memory.mutateThreadWithWorkingMemory({
        mutation: {
          type: 'save',
          thread: {
            id: threadId,
            resourceId,
            title: 'Rejected save',
            metadata: { changed: true },
            createdAt,
            updatedAt: new Date('2026-01-02T00:00:00.000Z'),
          },
        },
        workingMemory: {
          type: 'observer-update',
          resourceId,
          value: '{"new":"5678"}',
          expectedRevision: owner.revision,
          maxDataBytes: 20,
        },
      }),
    ).rejects.toThrow('UTF-8 byte limit');
    await expect(memory.getThreadById({ threadId })).resolves.toEqual(beforeRejectedMutation);

    await expect(
      memory.mutateThreadWithWorkingMemory({
        mutation: { type: 'update', id: threadId, title: 'Rejected', metadata: { changed: true } },
        workingMemory: {
          type: 'observer-update',
          resourceId,
          value: '{"new":"5678"}',
          expectedRevision: owner.revision,
          maxDataBytes: 20,
        },
      }),
    ).rejects.toThrow('UTF-8 byte limit');
    await expect(memory.getThreadById({ threadId })).resolves.toEqual(beforeRejectedMutation);

    await expect(
      memory.mutateThreadWithWorkingMemory({
        mutation: { type: 'update', id: threadId, title: 'Stale', metadata: { changed: true } },
        workingMemory: {
          type: 'observer-update',
          resourceId,
          value: '{"version":2}',
          expectedRevision: owner.revision - 1,
        },
      }),
    ).rejects.toMatchObject({ name: 'WorkingMemoryRevisionConflictError' });
    await expect(memory.getThreadById({ threadId })).resolves.toEqual(beforeRejectedMutation);

    await expect(
      memory.mutateThreadWithWorkingMemory({
        mutation: { type: 'update', id: threadId, title: 'Hidden', metadata: { changed: true } },
        workingMemory: { type: 'require-ungoverned' },
      }),
    ).rejects.toThrow('explicit workingMemory value');
    await expect(memory.getThreadById({ threadId })).resolves.toEqual(beforeRejectedMutation);

    const updated = await memory.mutateThreadWithWorkingMemory({
      mutation: { type: 'update', id: threadId, title: 'Updated', metadata: { changed: true } },
      workingMemory: {
        type: 'observer-update',
        resourceId,
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

  it('rejects generic mutations while preserving governed fields omitted from whole-row saves', async () => {
    const storage = new InMemoryStore({ id: 'working-memory-generic-write-guards' });
    const memory = await storage.getStore('memory');
    if (!memory) throw new Error('Expected in-memory storage domain.');
    const createdAt = new Date('2026-01-01T00:00:00.000Z');
    const resourceId = 'guarded-resource';
    const threadId = 'guarded-thread';
    await memory.saveThread({
      thread: { id: threadId, resourceId, title: 'Thread', metadata: {}, createdAt, updatedAt: createdAt },
    });
    const threadOwner = await memory.applyWorkingMemoryUpdate({
      scope: 'thread',
      resourceId,
      threadId,
      value: '{"preference":"concise"}',
      expectedRevision: 0,
      source: 'owner',
      protectPaths: ['/preference'],
    });
    const resourceOwner = await memory.applyWorkingMemoryUpdate({
      scope: 'resource',
      resourceId,
      value: '{"name":"Ada"}',
      expectedRevision: 0,
      source: 'owner',
      protectPaths: ['/name'],
    });

    await memory.updateThread({ id: threadId, metadata: { display: 'updated' } });
    await memory.updateResource({ resourceId, metadata: { display: 'updated' } });
    await memory.saveThread({
      thread: { id: threadId, resourceId, title: 'Saved', metadata: {}, createdAt, updatedAt: createdAt },
    });
    await memory.saveResource({ resource: { id: resourceId, metadata: {}, createdAt, updatedAt: createdAt } });

    await expect(memory.getWorkingMemorySnapshot({ scope: 'thread', resourceId, threadId })).resolves.toEqual(
      threadOwner,
    );
    await expect(memory.getWorkingMemorySnapshot({ scope: 'resource', resourceId })).resolves.toEqual(resourceOwner);

    await expect(
      memory.updateThread({ id: threadId, metadata: { workingMemory: '{"preference":"verbose"}' } }),
    ).rejects.toThrow('applyWorkingMemoryUpdate');
    await expect(memory.updateResource({ resourceId, workingMemory: '{"name":"Grace"}' })).rejects.toThrow(
      'applyWorkingMemoryUpdate',
    );
    await expect(
      memory.saveThread({
        thread: {
          id: threadId,
          resourceId,
          title: 'Rejected',
          metadata: { mastra: { workingMemory: { revision: 999 } } },
          createdAt,
          updatedAt: createdAt,
        },
      }),
    ).rejects.toThrow('applyWorkingMemoryUpdate');
    await expect(
      memory.saveResource({
        resource: {
          id: resourceId,
          workingMemory: '{"name":"Grace"}',
          metadata: {},
          createdAt,
          updatedAt: createdAt,
        },
      }),
    ).rejects.toThrow('applyWorkingMemoryUpdate');

    const record = await memory.initializeObservationalMemory({
      config: { _managedWorkingMemoryScope: 'resource' },
      resourceId,
      scope: 'resource',
      threadId: null,
    });
    await expect(
      memory.updateResourceFromObservationalMemory({
        resourceId,
        workingMemory: '{"name":"Grace"}',
        guard: { recordId: record.id, resourceId, threadId: null },
      }),
    ).rejects.toThrow('applyWorkingMemoryUpdate');
  });

  it('deep-clones and freezes governed metadata at in-memory adapter boundaries', async () => {
    const storage = new InMemoryStore({ id: 'working-memory-control-boundaries' });
    const memory = await storage.getStore('memory');
    if (!memory) throw new Error('Expected in-memory storage domain.');
    const createdAt = new Date('2026-01-01T00:00:00.000Z');
    const resourceId = 'boundary-resource';
    const threadId = 'boundary-thread';
    await memory.saveThread({
      thread: { id: threadId, resourceId, title: 'Thread', metadata: {}, createdAt, updatedAt: createdAt },
    });
    await memory.applyWorkingMemoryUpdate({
      scope: 'thread',
      resourceId,
      threadId,
      value: '{"preference":"concise"}',
      expectedRevision: 0,
      source: 'owner',
      protectPaths: ['/preference'],
    });
    await memory.applyWorkingMemoryUpdate({
      scope: 'resource',
      resourceId,
      value: '{"name":"Ada"}',
      expectedRevision: 0,
      source: 'owner',
      protectPaths: ['/name'],
    });

    const thread = await memory.getThreadById({ threadId });
    const resource = await memory.getResourceById({ resourceId });
    const threadControl = (thread?.metadata?.mastra as Record<string, unknown>).workingMemory as Record<
      string,
      unknown
    >;
    const resourceControl = (resource?.metadata?.mastra as Record<string, unknown>).workingMemory as Record<
      string,
      unknown
    >;
    expect(Object.isFrozen(threadControl)).toBe(true);
    expect(Object.isFrozen(threadControl.provenance)).toBe(true);
    expect(Object.isFrozen(resourceControl)).toBe(true);
    expect(() => {
      threadControl.revision = 99;
    }).toThrow(TypeError);
    expect(() => {
      resourceControl.revision = 99;
    }).toThrow(TypeError);

    await expect(memory.getWorkingMemorySnapshot({ scope: 'thread', resourceId, threadId })).resolves.toMatchObject({
      revision: 1,
      protectedPaths: ['/preference'],
    });
    await expect(memory.getWorkingMemorySnapshot({ scope: 'resource', resourceId })).resolves.toMatchObject({
      revision: 1,
      protectedPaths: ['/name'],
    });
  });

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
          updatedAt: id === 'older' ? (updatedAt as unknown as Date) : new Date(updatedAt),
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
