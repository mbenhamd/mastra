import { randomUUID } from 'node:crypto';

import { createSampleMessageV2, createSampleResource, createSampleSessionRecord } from '@internal/storage-test-utils';
import {
  TABLE_HARNESS_OPERATION_TOMBSTONES,
  TABLE_HARNESS_SESSIONS,
  TABLE_HARNESS_WAKEUPS,
  TABLE_MESSAGES,
  TABLE_OBSERVATIONAL_MEMORY,
  TABLE_RESOURCES,
  TABLE_THREADS,
  TABLE_WORKFLOW_SNAPSHOT,
  verifyExecutionClosurePayload,
} from '@mastra/core/storage';
import { afterAll, describe, expect, it, vi } from 'vitest';

import { PostgresStore } from '..';
import { exportExecutionClosure, importExecutionClosure } from './execution-closure';
import { TEST_CONFIG } from './test-utils';

vi.setConfig({ testTimeout: 60_000, hookTimeout: 60_000 });

const HARNESS = 'default';

function closureStore(id: string, schemaName: string) {
  return new PostgresStore({
    ...TEST_CONFIG,
    id,
    schemaName,
    enabledDomains: ['harness', 'memory', 'workflows', 'threadState'],
    sessionRecordProjection: { enabled: true },
  });
}

async function seedClosure(store: PostgresStore, schemaName: string, rootId: string) {
  const harness = store.stores.harness!;
  const memory = store.stores.memory!;
  const workflows = store.stores.workflows!;
  const threadId = `thread-${rootId}`;
  const childThreadId = `thread-${rootId}-child`;
  const resourceId = `resource-${rootId}`;
  const runId = `run-${rootId}`;

  await memory.saveResource({ resource: createSampleResource({ id: resourceId }) });
  for (const [id, title] of [
    [threadId, 'closure thread'],
    [childThreadId, 'child thread'],
  ]) {
    await memory.saveThread({
      thread: {
        id,
        resourceId,
        title,
        metadata: {},
        createdAt: new Date(),
        updatedAt: new Date(),
      },
    });
  }
  await memory.saveMessages({
    messages: [
      createSampleMessageV2({ threadId, resourceId, content: { content: 'first' } }),
      createSampleMessageV2({ threadId, resourceId, content: { content: 'second' } }),
      createSampleMessageV2({ threadId: childThreadId, resourceId, content: { content: 'child' } }),
    ],
  });
  await harness.saveSession(
    createSampleSessionRecord({
      id: rootId,
      harnessName: HARNESS,
      resourceId,
      threadId,
      ownsThread: true,
      currentRun: {
        runId,
        harnessName: HARNESS,
        sessionId: rootId,
        resourceId,
        threadId,
        agentId: 'agent-1',
        modeId: 'build',
        modelId: 'model-1',
        operation: { kind: 'sync-generate', operationId: `op-${runId}` },
        status: 'running',
        startedAt: Date.now() - 1000,
        updatedAt: Date.now(),
      },
    }),
    { ifVersion: 0 },
  );
  await harness.saveSession(
    createSampleSessionRecord({
      id: `${rootId}-child`,
      harnessName: HARNESS,
      resourceId,
      threadId: childThreadId,
      parentSessionId: rootId,
      origin: 'subagent',
    }),
    { ifVersion: 0 },
  );
  await workflows.persistWorkflowSnapshot({
    workflowName: 'test-workflow',
    runId,
    snapshot: {
      runId,
      value: { step: 'done' },
      status: 'success',
      activePaths: [],
      suspendedPaths: {},
      serializedStepGraph: [],
      timestamp: Date.now(),
    } as never,
  });

  // One OM generation row bound to the thread — proves the memory/OM table is
  // inside the closure, not just enumerated.
  await store.db.none(
    `INSERT INTO "${schemaName}"."${TABLE_OBSERVATIONAL_MEMORY}" (
      id, "lookupKey", scope, "activeObservations", "originType", config,
      "generationCount", "pendingMessageTokens", "totalTokensObserved",
      "observationTokenCount", "isObserving", "isReflecting",
      "isBufferingObservation", "isBufferingReflection", "lastBufferedAtTokens",
      "threadId", "resourceId", "createdAt", "updatedAt"
    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)`,
    [
      `om-${rootId}`,
      `lookup-${threadId}`,
      'thread',
      '[]',
      'session',
      '{}',
      1,
      0,
      10,
      10,
      false,
      false,
      false,
      false,
      0,
      threadId,
      resourceId,
      new Date(),
      new Date(),
    ],
  );

  return { threadId, resourceId, runId };
}

describe('exportExecutionClosure', () => {
  const stores: PostgresStore[] = [];
  const store = async (name: string) => {
    const schemaName = `closure_${name}_${randomUUID().slice(0, 8)}`;
    const s = closureStore(`closure-${name}`, schemaName);
    await s.init();
    stores.push(s);
    return { s, schemaName };
  };

  afterAll(async () => {
    for (const s of stores) await s.close();
  });

  it('exports the full session subtree closure with a verifiable manifest', async () => {
    const { s, schemaName } = await store('full');
    const { threadId, resourceId, runId } = await seedClosure(s, schemaName, 's1');

    const { manifest, rows } = await exportExecutionClosure(
      s.db,
      { harnessName: HARNESS, sessionId: 's1' },
      { schemaName },
    );

    expect(manifest.sessionIds).toEqual(['s1', 's1-child']);
    expect(manifest.threadIds.sort()).toEqual([threadId, `thread-s1-child`].sort());
    expect(manifest.runIds).toEqual([runId]);
    expect(manifest.resourceIds).toEqual([resourceId]);
    expect(manifest.completeness).toBe('complete');
    expect(manifest.pins).toEqual([]);

    const byTable = Object.fromEntries(manifest.tables.map(t => [t.table, t]));
    expect(byTable[TABLE_HARNESS_SESSIONS]!.rowCount).toBe(2);
    expect(byTable[TABLE_MESSAGES]!.rowCount).toBe(3);
    expect(byTable[TABLE_THREADS]!.rowCount).toBe(2);
    expect(byTable[TABLE_OBSERVATIONAL_MEMORY]!.rowCount).toBe(1);
    expect(byTable[TABLE_WORKFLOW_SNAPSHOT]!.rowCount).toBe(1);
    expect(rows[TABLE_HARNESS_SESSIONS]!.map(r => r.id).sort()).toEqual(['s1', 's1-child']);

    // Every digest verifies against the exported rows — the manifest is a
    // truthful receipt, not a summary.
    const verified = verifyExecutionClosurePayload(manifest, rows);
    expect(verified.mismatches).toEqual([]);
    expect(verified.ok).toBe(true);
  });

  it('exports a consistent snapshot that a concurrent commit cannot interleave into', async () => {
    const { s, schemaName } = await store('race');
    const { threadId } = await seedClosure(s, schemaName, 's2');

    // Baseline export = the pre-write snapshot digest.
    const baseline = await exportExecutionClosure(s.db, { harnessName: HARNESS, sessionId: 's2' }, { schemaName });
    const baselineMessages = baseline.manifest.tables.find(t => t.table === TABLE_MESSAGES)!;

    // Writer inserts + commits while the export transaction is mid-flight.
    // Under REPEATABLE READ the export's snapshot predates the commit, so the
    // message digest must equal the pre-write row set — never a partial mix.
    const exporting = exportExecutionClosure(s.db, { harnessName: HARNESS, sessionId: 's2' }, { schemaName });
    await new Promise(r => setTimeout(r, 25));
    const memory = s.stores.memory!;
    await memory.saveMessages({
      messages: [createSampleMessageV2({ threadId, resourceId: 'resource-s2', content: { content: 'concurrent' } })],
    });
    const { manifest, rows } = await exporting;

    const during = manifest.tables.find(t => t.table === TABLE_MESSAGES)!;
    expect(during.rowCount).toBe(baselineMessages.rowCount);
    expect(during.sha256).toBe(baselineMessages.sha256);
    expect(verifyExecutionClosurePayload(manifest, rows).ok).toBe(true);

    const after = await exportExecutionClosure(s.db, { harnessName: HARNESS, sessionId: 's2' }, { schemaName });
    const afterMessages = after.manifest.tables.find(t => t.table === TABLE_MESSAGES)!;
    expect(afterMessages.rowCount).toBe(baselineMessages.rowCount + 1);
    expect(afterMessages.sha256).not.toBe(baselineMessages.sha256);
  });

  it('pins the unit when the session thread is missing instead of exporting a broken closure', async () => {
    const { s, schemaName } = await store('pin');
    const harness = s.stores.harness!;
    await harness.saveSession(
      createSampleSessionRecord({
        id: 's3',
        harnessName: HARNESS,
        resourceId: 'resource-s3',
        threadId: 'ghost-thread',
      }),
      { ifVersion: 0 },
    );

    const { manifest } = await exportExecutionClosure(s.db, { harnessName: HARNESS, sessionId: 's3' }, { schemaName });
    expect(manifest.completeness).toBe('pinned');
    expect(manifest.pins).toEqual([
      expect.objectContaining({ reason: 'session-thread-missing' }),
      expect.objectContaining({ reason: 'session-resource-missing' }),
    ]);
  });

  it('fails closed when the root session does not exist', async () => {
    const { s, schemaName } = await store('missing');
    await seedClosure(s, schemaName, 's4');
    await expect(
      exportExecutionClosure(s.db, { harnessName: HARNESS, sessionId: 'nonexistent' }, { schemaName }),
    ).rejects.toThrow(/not found/i);
  });

  it('keeps fence and authority rows in the payload with their roles recorded', async () => {
    const { s, schemaName } = await store('roles');
    await seedClosure(s, schemaName, 's5');

    // Seed one fence row (operation tombstone) and one authority row (wakeup)
    // directly — the export must carry them as evidence, not drop them.
    await s.db.none(
      `INSERT INTO "${schemaName}"."${TABLE_HARNESS_OPERATION_TOMBSTONES}" (id, harness_name, session_id, kind, resource_id, thread_id, terminal_at, compacted_at, expires_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
      [
        `tomb-${randomUUID()}`,
        HARNESS,
        's5',
        'turn',
        'resource-s5',
        'thread-s5',
        Date.now(),
        Date.now(),
        Date.now() + 86_400_000,
      ],
    );
    await s.db.none(
      `INSERT INTO "${schemaName}"."${TABLE_HARNESS_WAKEUPS}" (id, harness_name, source, source_id, fire_id, idempotency_key, payload_hash, admission_id, session_id, resource_id, thread_id, due_at, status, attempts, content, attachments, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)`,
      [
        `wake-${randomUUID()}`,
        HARNESS,
        'timer',
        'timer-1',
        'fire-1',
        `idem-${randomUUID()}`,
        'hash-1',
        `adm-${randomUUID()}`,
        's5',
        'resource-s5',
        'thread-s5',
        Date.now() + 60_000,
        'pending',
        0,
        'wakeup',
        '[]',
        Date.now(),
        Date.now(),
      ],
    );

    const { manifest } = await exportExecutionClosure(s.db, { harnessName: HARNESS, sessionId: 's5' }, { schemaName });
    const byTable = Object.fromEntries(manifest.tables.map(t => [t.table, t]));
    expect(byTable[TABLE_HARNESS_OPERATION_TOMBSTONES]!.role).toBe('fence');
    expect(byTable[TABLE_HARNESS_OPERATION_TOMBSTONES]!.rowCount).toBe(1);
    expect(byTable[TABLE_HARNESS_WAKEUPS]!.role).toBe('authority');
    expect(byTable[TABLE_HARNESS_WAKEUPS]!.rowCount).toBe(1);
    expect(byTable[TABLE_OBSERVATIONAL_MEMORY]!.role).toBe('state');
    expect(byTable[TABLE_RESOURCES]!.role).toBe('shared-resource');
  });
});

/** Sessions-table digest differs across a round trip (fresh incarnation +
 * cleared lease); every other table must re-export byte-identical. */
function comparableDigest(rows: Record<string, unknown>[] | undefined) {
  return (rows ?? [])
    .map(r => {
      const { session_incarnation: _i, owner_id: _o, lease_expires_at: _l, ...rest } = r;
      return rest;
    })
    .sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
}

describe('importExecutionClosure', () => {
  const stores: PostgresStore[] = [];
  const store = async (name: string) => {
    const schemaName = `import_${name}_${randomUUID().slice(0, 8)}`.replaceAll('-', '_');
    const s = closureStore(`import-${name}`, schemaName);
    await s.init();
    stores.push(s);
    return { s, schemaName };
  };

  afterAll(async () => {
    for (const s of stores) await s.close();
  });

  it('round-trips a closure into a fresh schema with fresh execution authority', async () => {
    const src = await store('rt-src');
    const dst = await store('rt-dst');
    const { threadId, resourceId, runId } = await seedClosure(src.s, src.schemaName, 'rs1');

    // Store-level API: each store binds its own schema.
    const exported = await src.s.exportExecutionClosure({ harnessName: HARNESS, sessionId: 'rs1' });
    const result = await dst.s.importExecutionClosure(exported);

    expect(result.status).toBe('imported');
    expect(result.inserted[TABLE_HARNESS_SESSIONS]).toBe(2);
    expect(result.inserted[TABLE_MESSAGES]).toBe(3);
    expect(Object.values(result.incarnations)).toHaveLength(2);
    for (const incarnation of Object.values(result.incarnations)) {
      expect(incarnation).toMatch(/^[0-9a-f-]{36}$/);
    }

    // The imported session is dormant: fresh incarnation, no lease/owner —
    // the exported incarnation cannot carry authority into the target.
    const sessions = await dst.s.db.manyOrNone<Record<string, unknown>>(
      `SELECT id, session_incarnation, owner_id, lease_expires_at FROM "${dst.schemaName}"."${TABLE_HARNESS_SESSIONS}" ORDER BY id`,
    );
    expect(sessions.map(r => r.id)).toEqual(['rs1', 'rs1-child']);
    for (const row of sessions) {
      expect(row.session_incarnation).toBe(result.incarnations[row.id as string]);
      expect(row.session_incarnation).not.toBe(exported.manifest.incarnations[row.id as string]);
      expect(row.owner_id).toBeNull();
      expect(row.lease_expires_at).toBeNull();
    }

    // Re-export from the target: the closure is intact and self-consistent.
    const reexported = await exportExecutionClosure(
      dst.s.db,
      { harnessName: HARNESS, sessionId: 'rs1' },
      { schemaName: dst.schemaName },
    );
    expect(reexported.manifest.completeness).toBe('complete');
    expect(reexported.manifest.threadIds.sort()).toEqual(exported.manifest.threadIds.sort());
    expect(reexported.manifest.runIds).toEqual([runId]);
    expect(reexported.manifest.resourceIds).toEqual([resourceId]);
    expect(verifyExecutionClosurePayload(reexported.manifest, reexported.rows).ok).toBe(true);
    expect(comparableDigest(reexported.rows[TABLE_MESSAGES])).toEqual(comparableDigest(exported.rows[TABLE_MESSAGES]));
    expect(comparableDigest(reexported.rows[TABLE_THREADS])).toEqual(comparableDigest(exported.rows[TABLE_THREADS]));
    const thread = (reexported.rows[TABLE_THREADS] ?? []).find(r => r.id === threadId);
    expect(thread).toBeDefined();
  });

  it('converges on re-import instead of duplicating or erroring', async () => {
    const src = await store('dup-src');
    const dst = await store('dup-dst');
    await seedClosure(src.s, src.schemaName, 'rs2');
    const exported = await exportExecutionClosure(
      src.s.db,
      { harnessName: HARNESS, sessionId: 'rs2' },
      { schemaName: src.schemaName },
    );

    const first = await importExecutionClosure(dst.s.db, exported, { schemaName: dst.schemaName });
    const second = await importExecutionClosure(dst.s.db, exported, { schemaName: dst.schemaName });

    expect(first.status).toBe('imported');
    expect(second.status).toBe('imported');
    // Lost-acknowledgement retry: every row already present -> nothing inserted,
    // nothing duplicated. The returned incarnation is the one stored by the
    // first commit, not a new id that was never written.
    expect(second.inserted[TABLE_HARNESS_SESSIONS]).toBe(0);
    expect(second.skipped[TABLE_HARNESS_SESSIONS]).toBe(2);
    expect(second.inserted[TABLE_MESSAGES]).toBe(0);
    expect(second.skipped[TABLE_MESSAGES]).toBe(3);
    const count = await dst.s.db.one<{ n: string }>(
      `SELECT count(*)::text AS n FROM "${dst.schemaName}"."${TABLE_HARNESS_SESSIONS}"`,
    );
    expect(count.n).toBe('2');
    const row = await dst.s.db.one<{ session_incarnation: string }>(
      `SELECT session_incarnation FROM "${dst.schemaName}"."${TABLE_HARNESS_SESSIONS}" WHERE id = 'rs2'`,
    );
    expect(row.session_incarnation).toBe(first.incarnations.rs2);
    expect(second.incarnations.rs2).toBe(first.incarnations.rs2);
  });

  it('fails closed on a tampered payload and writes nothing', async () => {
    const src = await store('corrupt-src');
    const dst = await store('corrupt-dst');
    await seedClosure(src.s, src.schemaName, 'rs3');
    const exported = await exportExecutionClosure(
      src.s.db,
      { harnessName: HARNESS, sessionId: 'rs3' },
      { schemaName: src.schemaName },
    );

    // Corrupt one message row — the manifest digest no longer matches.
    const tampered = {
      manifest: exported.manifest,
      rows: {
        ...exported.rows,
        [TABLE_MESSAGES]: [{ ...(exported.rows[TABLE_MESSAGES]?.[0] ?? {}), content: 'tampered' }],
      },
    };
    await expect(importExecutionClosure(dst.s.db, tampered, { schemaName: dst.schemaName })).rejects.toThrow(
      /manifest verification/i,
    );
    const count = await dst.s.db.one<{ n: string }>(
      `SELECT count(*)::text AS n FROM "${dst.schemaName}"."${TABLE_MESSAGES}"`,
    );
    expect(count.n).toBe('0');
  });

  it('never restores authority rows — wakeups stay absent after import', async () => {
    const src = await store('auth-src');
    const dst = await store('auth-dst');
    await seedClosure(src.s, src.schemaName, 'rs4');
    const now = Date.now();
    await src.s.db.none(
      `INSERT INTO "${src.schemaName}"."${TABLE_HARNESS_WAKEUPS}" (id, harness_name, source, source_id, fire_id, idempotency_key, payload_hash, admission_id, session_id, resource_id, thread_id, due_at, status, attempts, content, attachments, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)`,
      [
        `wake-${randomUUID()}`,
        HARNESS,
        'timer',
        'timer-1',
        'fire-1',
        `idem-${randomUUID()}`,
        'hash-1',
        `adm-${randomUUID()}`,
        'rs4',
        'resource-rs4',
        'thread-rs4',
        now + 60_000,
        'pending',
        0,
        'wakeup',
        '[]',
        now,
        now,
      ],
    );

    const exported = await exportExecutionClosure(
      src.s.db,
      { harnessName: HARNESS, sessionId: 'rs4' },
      { schemaName: src.schemaName },
    );
    expect(exported.manifest.tables.find(t => t.table === TABLE_HARNESS_WAKEUPS)!.rowCount).toBe(1);

    const result = await importExecutionClosure(dst.s.db, exported, { schemaName: dst.schemaName });
    // Exported as evidence; skipped on import — no stale delivery authority.
    expect(result.skipped[TABLE_HARNESS_WAKEUPS]).toBe(1);
    const count = await dst.s.db.one<{ n: string }>(
      `SELECT count(*)::text AS n FROM "${dst.schemaName}"."${TABLE_HARNESS_WAKEUPS}"`,
    );
    expect(count.n).toBe('0');
  });

  it('does not leak a foreign harness namespace into the export', async () => {
    const src = await store('iso-src');
    const dst = await store('iso-dst');
    await seedClosure(src.s, src.schemaName, 'rs6');
    const harness = src.s.stores.harness!;
    await harness.saveSession(
      createSampleSessionRecord({
        id: 'foreign-session',
        harnessName: 'other-harness',
        resourceId: 'resource-rs6',
        threadId: 'thread-rs6',
      }),
      { ifVersion: 0 },
    );

    const exported = await exportExecutionClosure(
      src.s.db,
      { harnessName: HARNESS, sessionId: 'rs6' },
      { schemaName: src.schemaName },
    );
    expect(exported.manifest.sessionIds).not.toContain('foreign-session');
    expect((exported.rows[TABLE_HARNESS_SESSIONS] ?? []).map(r => r.id)).not.toContain('foreign-session');

    const result = await importExecutionClosure(dst.s.db, exported, { schemaName: dst.schemaName });
    expect(result.status).toBe('imported');
    const foreign = await dst.s.db.oneOrNone(
      `SELECT id FROM "${dst.schemaName}"."${TABLE_HARNESS_SESSIONS}" WHERE id = 'foreign-session'`,
    );
    expect(foreign).toBeNull();
  });

  it('imports a pinned closure with status pinned and preserves the pins', async () => {
    const src = await store('pin-src');
    const dst = await store('pin-dst');
    const harness = src.s.stores.harness!;
    await harness.saveSession(
      createSampleSessionRecord({ id: 'rs5', harnessName: HARNESS, resourceId: 'ghost-res', threadId: 'ghost-thread' }),
      { ifVersion: 0 },
    );
    const exported = await exportExecutionClosure(
      src.s.db,
      { harnessName: HARNESS, sessionId: 'rs5' },
      { schemaName: src.schemaName },
    );
    expect(exported.manifest.completeness).toBe('pinned');

    const result = await importExecutionClosure(dst.s.db, exported, { schemaName: dst.schemaName });
    expect(result.status).toBe('pinned');
    expect(result.pins).toEqual(exported.manifest.pins);
    expect(result.inserted[TABLE_HARNESS_SESSIONS]).toBe(1);
  });
});
