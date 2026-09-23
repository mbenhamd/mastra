import { describe, expect, it } from 'vitest';

import { TABLE_HARNESS_SESSIONS, TABLE_MESSAGES, TABLE_THREADS } from '../constants';
import { buildExecutionClosureManifest, digestClosureRows, verifyExecutionClosurePayload } from './manifest';
import { EXECUTION_CLOSURE_TABLES } from './tables';
import type { ExecutionClosureKey } from './types';

const KEY: ExecutionClosureKey = { harnessName: 'default', sessionId: 's1' };

function manifestFor(rows: Parameters<typeof buildExecutionClosureManifest>[0]['rows'], pins = []) {
  return buildExecutionClosureManifest({
    key: KEY,
    sessionIds: ['s1'],
    incarnations: { s1: 'inc-1' },
    threadIds: ['t1'],
    runIds: ['r1'],
    resourceIds: ['res1'],
    rows,
    pins,
  });
}

describe('digestClosureRows', () => {
  it('is deterministic across JSONB key orderings and driver value types', () => {
    // PG returns JSONB columns with normalized key order; a text column may
    // preserve insertion order. The digest must not drift between them.
    const a = [{ id: 'm1', payload: { b: 1, a: 2 }, nested: { y: [3, { z: 1, q: 2 }] } }];
    const b = [{ id: 'm1', payload: { a: 2, b: 1 }, nested: { y: [3, { q: 2, z: 1 }] } }];
    expect(digestClosureRows(a)).toBe(digestClosureRows(b));
  });

  it('changes when any byte of the row set changes', () => {
    const a = [{ id: 'm1', content: 'hello' }];
    const b = [{ id: 'm1', content: 'hellp' }];
    expect(digestClosureRows(a)).not.toBe(digestClosureRows(b));
  });

  it('is order-independent across rows but distinguishes row membership', () => {
    const rows = [
      { id: 'm1', v: 1 },
      { id: 'm2', v: 2 },
    ];
    expect(digestClosureRows([rows[1]!, rows[0]!])).toBe(digestClosureRows(rows));
    expect(digestClosureRows([rows[0]!])).not.toBe(digestClosureRows(rows));
  });

  it('canonicalizes Date, Uint8Array, bigint, and null consistently', () => {
    const d = new Date('2024-01-02T03:04:05.678Z');
    const rows = [{ id: 'm1', at: d, blob: new Uint8Array([1, 2, 3]), n: BigInt(9), missing: null }];
    // Same values, different JS representations of the same column value.
    const roundTripped = [
      { id: 'm1', at: '2024-01-02T03:04:05.678Z', blob: new Uint8Array([1, 2, 3]), n: '9', missing: null },
    ];
    expect(digestClosureRows(rows)).toBe(digestClosureRows(roundTripped));
    expect(digestClosureRows(rows)).toMatch(/^sha256:[0-9a-f]{64}$/);
  });
});

describe('buildExecutionClosureManifest + verifyExecutionClosurePayload', () => {
  it('round-trips a payload whose digests verify', () => {
    const rows = {
      [TABLE_HARNESS_SESSIONS]: [{ id: 's1', harness_name: 'default' }],
      [TABLE_THREADS]: [{ id: 't1' }],
      [TABLE_MESSAGES]: [
        { id: 'm1', thread_id: 't1' },
        { id: 'm2', thread_id: 't1' },
      ],
    };
    const manifest = manifestFor(rows);
    expect(manifest.completeness).toBe('complete');
    expect(manifest.tables.map(t => t.table)).toEqual(Object.keys(EXECUTION_CLOSURE_TABLES).sort());
    // Registered tables with no rows still get an explicit empty-set digest.
    for (const entry of manifest.tables) {
      const expected = rows[entry.table as keyof typeof rows] ?? [];
      expect(entry.rowCount).toBe(expected.length);
    }

    const verified = verifyExecutionClosurePayload(manifest, rows);
    expect(verified.mismatches).toEqual([]);
    expect(verified.ok).toBe(true);
  });

  it('marks the unit pinned when pins are present', () => {
    const manifest = manifestFor({}, [{ reason: 'session-thread-missing', detail: { threadId: 't9' } }]);
    expect(manifest.completeness).toBe('pinned');
    expect(manifest.pins[0]!.reason).toBe('session-thread-missing');
  });

  it('detects a tampered row via digest mismatch', () => {
    const rows = { [TABLE_MESSAGES]: [{ id: 'm1', content: 'original' }] };
    const manifest = manifestFor(rows);
    const tampered = { [TABLE_MESSAGES]: [{ id: 'm1', content: 'forged' }] };
    const verified = verifyExecutionClosurePayload(manifest, tampered);
    expect(verified.ok).toBe(false);
    expect(verified.mismatches.some(m => m.includes(TABLE_MESSAGES) && m.includes('digest'))).toBe(true);
  });

  it('detects a silently dropped row via count mismatch', () => {
    const rows = { [TABLE_MESSAGES]: [{ id: 'm1' }, { id: 'm2' }] };
    const manifest = manifestFor(rows);
    const truncated = { [TABLE_MESSAGES]: [{ id: 'm1' }] };
    const verified = verifyExecutionClosurePayload(manifest, truncated);
    expect(verified.ok).toBe(false);
    expect(verified.mismatches.some(m => m.includes('row count'))).toBe(true);
  });

  it('rejects unsupported format and version', () => {
    const manifest = manifestFor({});
    const badFormat = verifyExecutionClosurePayload({ ...manifest, format: 'other' as never }, {});
    expect(badFormat.ok).toBe(false);
    const badVersion = verifyExecutionClosurePayload({ ...manifest, version: 2 as never }, {});
    expect(badVersion.ok).toBe(false);
  });

  it('rejects unregistered manifest tables and unlisted payload tables', () => {
    const manifest = manifestFor({});
    const extraTable = {
      ...manifest,
      tables: [
        ...manifest.tables,
        { table: 'mastra_rogue' as never, role: 'state' as never, rowCount: 0, sha256: 'sha256:x' as never },
      ],
    };
    expect(verifyExecutionClosurePayload(extraTable, {}).ok).toBe(false);
    const extraRows = verifyExecutionClosurePayload(manifest, { mastra_rogue: [] } as never);
    expect(extraRows.ok).toBe(false);
  });
});
