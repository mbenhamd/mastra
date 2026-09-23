import { randomUUID } from 'node:crypto';

import { createSampleMessageV2, createSampleResource, createSampleSessionRecord } from '@internal/storage-test-utils';
import {
  TABLE_BACKGROUND_TASKS,
  TABLE_HARNESS_ATTACHMENTS,
  TABLE_HARNESS_ATTACHMENT_OPERATIONS,
  TABLE_HARNESS_CHANNEL_BINDINGS,
  TABLE_HARNESS_CHANNEL_OUTBOX,
  TABLE_HARNESS_MESSAGE_RESULTS,
  TABLE_HARNESS_OPERATION_TOMBSTONES,
  TABLE_HARNESS_PROVIDER_CALLBACK_BINDINGS,
  TABLE_HARNESS_SESSIONS,
  TABLE_HARNESS_SESSION_PROJECTION_INTENTS,
  TABLE_HARNESS_TERMINAL_INTENTS,
  TABLE_HARNESS_WAKEUPS,
  TABLE_MESSAGES,
  TABLE_OBSERVATIONAL_MEMORY,
  TABLE_RESOURCES,
  TABLE_THREADS,
  TABLE_THREAD_STATE,
  TABLE_WORKFLOW_SNAPSHOT,
  TABLE_WORKFLOW_SNAPSHOT_HANDOFF,
  encodeThreadStateScope,
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
    enabledDomains: ['harness', 'memory', 'workflows', 'threadState', 'backgroundTasks'],
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

  it('pins the export when a pending message result has an in-flight dispatch', async () => {
    const { s, schemaName } = await store('dispatch');
    await seedClosure(s, schemaName, 'd1');
    const now = Date.now();
    // A `dispatching`/`accepted` marker means the provider may already have
    // executed — the runtime never auto-replays those states, so the closure
    // pins instead of importing an unrecoverable dispatch as complete.
    await s.db.none(
      `INSERT INTO "${schemaName}"."${TABLE_HARNESS_MESSAGE_RESULTS}"
         (id, harness_name, session_id, resource_id, thread_id, signal_id, run_id,
          operation_kind, status, dispatch, created_at, updated_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11,$12)`,
      [
        `mr-${randomUUID()}`,
        HARNESS,
        'd1',
        'resource-d1',
        'thread-d1',
        'sig-inflight',
        'run-inflight',
        'signal',
        'pending',
        JSON.stringify({
          state: 'dispatching',
          attemptId: 'attempt-1',
          claimExpiresAt: now + 30_000,
          delivery: 'idle',
          runId: 'run-inflight',
        }),
        now,
        now,
      ],
    );
    // A provably undispatched reservation re-drives safely — it must not pin.
    await s.db.none(
      `INSERT INTO "${schemaName}"."${TABLE_HARNESS_MESSAGE_RESULTS}"
         (id, harness_name, session_id, resource_id, thread_id, signal_id,
          operation_kind, status, dispatch, created_at, updated_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11)`,
      [
        `mr-${randomUUID()}`,
        HARNESS,
        'd1',
        'resource-d1',
        'thread-d1',
        'sig-reserved',
        'signal',
        'pending',
        JSON.stringify({ state: 'reserved' }),
        now,
        now,
      ],
    );

    const { manifest } = await exportExecutionClosure(s.db, { harnessName: HARNESS, sessionId: 'd1' }, { schemaName });
    expect(manifest.completeness).toBe('pinned');
    expect(manifest.pins).toEqual([
      expect.objectContaining({
        reason: 'in-flight-dispatch',
        detail: expect.objectContaining({
          sessionId: 'd1',
          signalId: 'sig-inflight',
          runId: 'run-inflight',
          dispatchState: 'dispatching',
        }),
      }),
    ]);
  });

  it('counts a snapshot handoff as parent-run evidence instead of pinning the export', async () => {
    const { s, schemaName } = await store('handoffparent');
    const { runId } = await seedClosure(s, schemaName, 'hp1');
    const now = Date.now();
    const parentRun = 'run-hp1-parent';
    // A terminal effect on the exported run references a parent run outside
    // the session's own run set.
    await s.db.none(
      `INSERT INTO "${schemaName}"."mastra_workflow_terminal_effects_v2"
         (workflow_name, run_id, effect_kind, version, effect_key, source_event_key,
          terminal_status, parent_workflow_name, parent_run_id, parent_step_id,
          parent_execution_path, recovery_envelope_hash, retained_record_hash,
          resource_id, payload_hash, created_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`,
      [
        'test-workflow',
        runId,
        'destination',
        1,
        'effect-1',
        'evt-1',
        'success',
        'test-workflow',
        parentRun,
        'step-1',
        '[]',
        'envelope-hash',
        'record-hash',
        null,
        'payload-hash',
        now,
      ],
    );
    // The parent run's snapshot lives only in a snapshot handoff — the
    // importer materializes it as canonical state, so the closure is complete
    // even though no mastra_workflow_snapshot row exists for the parent.
    await s.db.none(
      `INSERT INTO "${schemaName}"."${TABLE_WORKFLOW_SNAPSHOT_HANDOFF}"
         (workflow_name, run_id, version, status, resource_id, snapshot,
          mutation_fence, created_at, updated_at, completed_at)
       VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7,$8,$9,$10)`,
      [
        'test-workflow',
        parentRun,
        1,
        'pending',
        null,
        JSON.stringify({ runId: parentRun, status: 'success', timestamp: now }),
        `fence-${parentRun}`,
        now,
        now,
        null,
      ],
    );

    const { manifest, rows } = await exportExecutionClosure(
      s.db,
      { harnessName: HARNESS, sessionId: 'hp1' },
      { schemaName },
    );
    expect(rows[TABLE_WORKFLOW_SNAPSHOT_HANDOFF]!.length).toBe(1);
    expect(manifest.pins.filter(p => p.reason === 'workflow-parent-run-missing')).toEqual([]);
  });

  it('pins the export when a blob-backed attachment retains inline bytes', async () => {
    const { s, schemaName } = await store('blobattachment');
    await seedClosure(s, schemaName, 'ba1');
    const now = Date.now();
    // Both a source blob_ref and retained legacy data_b64: loadAttachment
    // never reads data_b64 — it resolves bytes through the destination byte
    // owner with blob_ref — and import neither uploads the inline bytes nor
    // rewrites the reference, so the pin cannot be suppressed by inline data.
    await s.db.none(
      `INSERT INTO "${schemaName}"."${TABLE_HARNESS_ATTACHMENTS}"
         (harness_name, session_id, attachment_id, name, mime_type, size_bytes,
          sha256, source, created_at, data_b64, session_incarnation, blob_ref)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
      [
        HARNESS,
        'ba1',
        'att-1',
        'file.bin',
        'application/octet-stream',
        4,
        'deadbeef',
        'preupload',
        now,
        'aGk=',
        'inc-ba1',
        'blob://source/att-1',
      ],
    );

    const { manifest } = await exportExecutionClosure(s.db, { harnessName: HARNESS, sessionId: 'ba1' }, { schemaName });
    expect(manifest.completeness).toBe('pinned');
    expect(manifest.pins).toEqual([
      expect.objectContaining({
        reason: 'attachment-bytes-external',
        detail: expect.objectContaining({ sessionId: 'ba1', attachmentId: 'att-1' }),
      }),
    ]);
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

  it('scopes provider callback bindings to closure channels instead of leaking harness-wide routing', async () => {
    const { s, schemaName } = await store('callbacks');
    const { threadId, resourceId } = await seedClosure(s, schemaName, 'cb1');
    const now = Date.now();
    const insertBinding = (id: string, channelId: string, sessionId: string, bindingThreadId: string) =>
      s.db.none(
        `INSERT INTO "${schemaName}"."${TABLE_HARNESS_CHANNEL_BINDINGS}"
           (id, harness_name, channel_id, provider_id, status, platform,
            external_tenant_id, external_channel_id, external_thread_id,
            resource_id, thread_id, session_id, mode, generation,
            created_at, updated_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`,
        [
          id,
          HARNESS,
          channelId,
          'provider-1',
          'active',
          'test',
          'tenant',
          `ext-${channelId}`,
          'ext-thread',
          resourceId,
          bindingThreadId,
          sessionId,
          'default',
          1,
          now,
          now,
        ],
      );
    const insertCallback = (id: string, channelId: string) =>
      s.db.none(
        `INSERT INTO "${schemaName}"."${TABLE_HARNESS_PROVIDER_CALLBACK_BINDINGS}"
           (id, provider_id, selector_kind, selector_value, harness_name, channel_id,
            origin, status, created_at, updated_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9,$10)`,
        [
          id,
          'provider-1',
          'channel',
          channelId,
          HARNESS,
          channelId,
          JSON.stringify({ sessionId: 'origin-session' }),
          'active',
          now,
          now,
        ],
      );
    // The closure's channel: its callback binding is legitimate evidence.
    await insertBinding(`bind-${randomUUID()}`, 'chan-own', 'cb1', threadId);
    await insertCallback('cb-own', 'chan-own');
    // A foreign channel in the same harness namespace: its routing metadata
    // (selectors, origin, last_error) must not ride along in the payload.
    await insertBinding(`bind-${randomUUID()}`, 'chan-foreign', 'other-session', 'thread-foreign');
    await insertCallback('cb-foreign', 'chan-foreign');

    const { manifest, rows } = await exportExecutionClosure(
      s.db,
      { harnessName: HARNESS, sessionId: 'cb1' },
      { schemaName },
    );
    const callbacks = rows[TABLE_HARNESS_PROVIDER_CALLBACK_BINDINGS] ?? [];
    expect(callbacks.map(r => r.id)).toEqual(['cb-own']);
    const bindings = rows[TABLE_HARNESS_CHANNEL_BINDINGS] ?? [];
    expect(bindings.map(r => r.channel_id)).toEqual(['chan-own']);
    expect(verifyExecutionClosurePayload(manifest, rows).ok).toBe(true);
  });

  it('satisfies a session current run whose snapshot exists only as a handoff row', async () => {
    const { s, schemaName } = await store('currenthandoff');
    const { runId } = await seedClosure(s, schemaName, 'ch1');
    const now = Date.now();
    // The session's current run has no canonical snapshot row — its durable
    // snapshot travels only in a handoff, which the importer materializes as
    // canonical state. Completeness must count it or a self-contained
    // closure pins falsely.
    await s.db.none(`DELETE FROM "${schemaName}"."${TABLE_WORKFLOW_SNAPSHOT}" WHERE run_id = $1`, [runId]);
    await s.db.none(
      `INSERT INTO "${schemaName}"."${TABLE_WORKFLOW_SNAPSHOT_HANDOFF}"
         (workflow_name, run_id, version, status, resource_id, snapshot,
          mutation_fence, created_at, updated_at, completed_at)
       VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7,$8,$9,$10)`,
      [
        'test-workflow',
        runId,
        1,
        'pending',
        null,
        JSON.stringify({ runId, status: 'success', timestamp: now }),
        `fence-${runId}`,
        now,
        now,
        null,
      ],
    );

    const { manifest, rows } = await exportExecutionClosure(
      s.db,
      { harnessName: HARNESS, sessionId: 'ch1' },
      { schemaName },
    );
    expect(rows[TABLE_WORKFLOW_SNAPSHOT_HANDOFF]!.length).toBe(1);
    expect(manifest.pins.filter(p => p.reason === 'current-run-without-snapshot')).toEqual([]);
    expect(manifest.completeness).toBe('complete');
  });

  it('pins the export when an attachment retains only inline bytes', async () => {
    const { s, schemaName } = await store('inlineattachment');
    await seedClosure(s, schemaName, 'ia1');
    const now = Date.now();
    // A legacy inline-only row: no blob_ref, so `loadAttachment` can never
    // resolve bytes (it requires blob_ref + session_incarnation) and import
    // neither uploads the inline bytes nor mints a reference.
    await s.db.none(
      `INSERT INTO "${schemaName}"."${TABLE_HARNESS_ATTACHMENTS}"
         (harness_name, session_id, attachment_id, name, mime_type, size_bytes,
          sha256, source, created_at, data_b64, session_incarnation, blob_ref)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
      [
        HARNESS,
        'ia1',
        'att-inline',
        'file.bin',
        'application/octet-stream',
        4,
        'deadbeef',
        'preupload',
        now,
        'aGk=',
        'inc-ia1',
        null,
      ],
    );

    const { manifest } = await exportExecutionClosure(s.db, { harnessName: HARNESS, sessionId: 'ia1' }, { schemaName });
    expect(manifest.completeness).toBe('pinned');
    expect(manifest.pins).toEqual([
      expect.objectContaining({
        reason: 'attachment-bytes-inline',
        detail: expect.objectContaining({ sessionId: 'ia1', attachmentId: 'att-inline' }),
      }),
    ]);
  });

  it('pins the export while an attachment byte-owner operation is unsettled', async () => {
    const { s, schemaName } = await store('opspending');
    await seedClosure(s, schemaName, 'op1');
    const now = Date.now();
    const insertOperation = (id: string, kind: string, status: string) =>
      s.db.none(
        `INSERT INTO "${schemaName}"."${TABLE_HARNESS_ATTACHMENT_OPERATIONS}"
           (id, harness_name, session_id, attachment_id, session_incarnation,
            kind, status, size_bytes, sha256, attempts, created_at, updated_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
        [id, HARNESS, 'op1', `att-${id}`, 'inc-op1', kind, status, 4, 'deadbeef', 0, now, now],
      );
    // An in-flight PUT is the only durable proof of byte-owner work; a settled
    // DELETE is closed history that must not pin.
    await insertOperation('op-put', 'put', 'pending');
    await insertOperation('op-del', 'delete', 'completed');

    const { manifest } = await exportExecutionClosure(s.db, { harnessName: HARNESS, sessionId: 'op1' }, { schemaName });
    expect(manifest.completeness).toBe('pinned');
    expect(manifest.pins).toEqual([
      expect.objectContaining({
        reason: 'attachment-operation-unsettled',
        detail: expect.objectContaining({
          sessionId: 'op1',
          operationId: 'op-put',
          kind: 'put',
          status: 'pending',
        }),
      }),
    ]);
  });

  it('exports thread state stored under the encoded scope key', async () => {
    const { s, schemaName } = await store('threadstate');
    const { threadId, resourceId } = await seedClosure(s, schemaName, 'ts1');
    const now = new Date();
    // The physical `threadId` column stores `encodeThreadStateScope` output —
    // filtering by the raw thread id would silently drop every state row.
    const encoded = encodeThreadStateScope({ resourceId, threadId });
    const insertState = (physicalKey: string, type: string, value: unknown) =>
      s.db.none(
        `INSERT INTO "${schemaName}"."${TABLE_THREAD_STATE}"
           ("threadId", type, value, "createdAt", "updatedAt")
         VALUES ($1,$2,$3::jsonb,$4,$5)`,
        [physicalKey, type, JSON.stringify(value), now, now],
      );
    await insertState(encoded, 'task', { items: ['a'] });
    await insertState(encoded, 'goal', { objective: 'ship' });
    await insertState(encodeThreadStateScope({ resourceId, threadId: 'thread-foreign' }), 'task', { items: [] });

    const { manifest, rows } = await exportExecutionClosure(
      s.db,
      { harnessName: HARNESS, sessionId: 'ts1' },
      { schemaName },
    );
    const states = rows[TABLE_THREAD_STATE] ?? [];
    expect(states.map(r => r.type).sort()).toEqual(['goal', 'task']);
    expect(states.every(r => r.threadId === encoded)).toBe(true);
    expect(verifyExecutionClosurePayload(manifest, rows).ok).toBe(true);
  });

  it('exports a live background task bound to the closure run', async () => {
    const { s, schemaName } = await store('tasks');
    const { threadId, resourceId, runId } = await seedClosure(s, schemaName, 'bt1');
    const now = new Date();
    const insertTask = (id: string, status: string, taskRunId: string, taskThreadId: string) =>
      s.db.none(
        `INSERT INTO "${schemaName}"."${TABLE_BACKGROUND_TASKS}"
           (id, tool_call_id, tool_name, agent_id, run_id, thread_id, resource_id,
            status, args, retry_count, max_retries, timeout_ms,
            "createdAt", "startedAt", "suspendedAt")
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11,$12,$13,$14,$15)`,
        [
          id,
          `tc-${id}`,
          'tool-1',
          'agent-1',
          taskRunId,
          taskThreadId,
          resourceId,
          status,
          '{}',
          0,
          3,
          30_000,
          now,
          now,
          status === 'suspended' ? now : null,
        ],
      );
    // A suspended task on the session's run is recoverable execution state —
    // it must travel with the closure, not strand on the source.
    await insertTask('task-live', 'suspended', runId, threadId);
    // A foreign run/task must not leak into the payload.
    await insertTask('task-foreign', 'suspended', 'run-foreign', 'thread-foreign');

    const { manifest, rows } = await exportExecutionClosure(
      s.db,
      { harnessName: HARNESS, sessionId: 'bt1' },
      { schemaName },
    );
    const tasks = rows[TABLE_BACKGROUND_TASKS] ?? [];
    expect(tasks.map(r => r.id)).toEqual(['task-live']);
    expect(verifyExecutionClosurePayload(manifest, rows).ok).toBe(true);
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

  it('restages a pending session-record projection intent for imported sessions', async () => {
    const src = await store('proj-src');
    const dst = await store('proj-dst');
    await seedClosure(src.s, src.schemaName, 'pj1');

    const exported = await exportExecutionClosure(
      src.s.db,
      { harnessName: HARNESS, sessionId: 'pj1' },
      { schemaName: src.schemaName },
    );
    const result = await importExecutionClosure(dst.s.db, exported, { schemaName: dst.schemaName });
    expect(result.status).toBe('imported');

    // The imported projection fence triggers a deterministic restage: one
    // pending intent per fenced session, keyed to the persisted session row.
    // A restage that derived its timestamp from a column sessions do not have
    // would silently stage nothing.
    expect(result.inserted[TABLE_HARNESS_SESSION_PROJECTION_INTENTS]).toBe(2);
    const intents = await dst.s.db.manyOrNone<Record<string, unknown>>(
      `SELECT * FROM "${dst.schemaName}"."${TABLE_HARNESS_SESSION_PROJECTION_INTENTS}" ORDER BY session_id`,
    );
    expect(intents).toHaveLength(2);
    const sessions = await dst.s.db.manyOrNone<Record<string, unknown>>(
      `SELECT id, version, last_activity_at FROM "${dst.schemaName}"."${TABLE_HARNESS_SESSIONS}" ORDER BY id`,
    );
    for (const [i, session] of sessions.entries()) {
      expect(intents[i]!.session_id).toBe(session.id);
      expect(intents[i]!.status).toBe('pending');
      expect(intents[i]!.revision).toBe(Number(session.version));
      expect(intents[i]!.created_at).toBe(String(session.last_activity_at));
    }
  });

  it('preserves a positive terminalization claim generation while clearing live claim fields', async () => {
    const src = await store('claim-src');
    const dst = await store('claim-dst');
    const { runId } = await seedClosure(src.s, src.schemaName, 'cl1');
    const now = Date.now();
    await src.s.db.none(
      `INSERT INTO "${src.schemaName}"."mastra_workflow_terminalizations"
         (workflow_name, run_id, version, event_key, terminal_status, phase,
          owner_id, claim_token, claim_generation, lease_expires_at,
          created_at, updated_at, completed_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
      [
        'test-workflow',
        runId,
        1,
        'evt-1',
        'success',
        'terminalization_pending',
        'src-owner',
        'src-token',
        3,
        now + 60_000,
        now,
        now,
        null,
      ],
    );
    // `persistWorkflowSnapshot` already created the run's parent-revision row.

    const exported = await exportExecutionClosure(
      src.s.db,
      { harnessName: HARNESS, sessionId: 'cl1' },
      { schemaName: src.schemaName },
    );
    const result = await importExecutionClosure(dst.s.db, exported, { schemaName: dst.schemaName });
    expect(result.status).toBe('imported');

    const row = await dst.s.db.one<Record<string, unknown>>(
      `SELECT * FROM "${dst.schemaName}"."mastra_workflow_terminalizations"
       WHERE workflow_name = 'test-workflow' AND run_id = $1`,
      [runId],
    );
    // Live claim fields are cleared; the monotonic fencing generation is
    // preserved — the workflow decoder rejects claim_generation <= 0.
    expect(row.owner_id).toBeNull();
    expect(row.claim_token).toBeNull();
    expect(row.lease_expires_at).toBeNull();
    expect(row.claim_generation).toBe('3');
    expect(row.phase).toBe('terminalization_pending');

    // The imported record is readable and reclaimable: a fresh claimant
    // increments the preserved generation instead of failing to decode.
    const claim = await dst.s.stores.workflows!.claimWorkflowTerminalization({
      workflowName: 'test-workflow',
      runId,
      eventKey: 'evt-1',
      terminalStatus: 'success',
      ownerId: 'dst-owner',
      leaseMs: 60_000,
    });
    expect(claim).toMatchObject({ status: 'acquired', record: { claimGeneration: 4 } });

    // A retry after the claim converges — claim columns are lifecycle state.
    const second = await importExecutionClosure(dst.s.db, exported, { schemaName: dst.schemaName });
    expect(second.status).toBe('imported');
  });

  it('converges when a retry meets rows advanced by destination workers', async () => {
    const src = await store('adv-src');
    const dst = await store('adv-dst');
    await seedClosure(src.s, src.schemaName, 'adv1');
    const now = Date.now();
    const srcSession = await src.s.db.one<{ session_incarnation: string }>(
      `SELECT session_incarnation FROM "${src.schemaName}"."${TABLE_HARNESS_SESSIONS}" WHERE id = 'adv1'`,
    );
    // One live terminal intent travels in the closure as fence evidence.
    await src.s.db.none(
      `INSERT INTO "${src.schemaName}"."${TABLE_HARNESS_TERMINAL_INTENTS}"
         (id, admission_id, admission_hash, harness_name, session_id, resource_id, thread_id,
          session_incarnation, grant_key, grant_generation, signal_id, run_id, revision,
          finalizer_id, finalizer_version, terminal_result_json, projection_json, payload_bytes,
          status, attempts, claim_id, claim_expires_at, next_attempt_at, last_error_json,
          created_at, updated_at, acked_at, dead_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28)`,
      [
        'intent-adv1',
        'adm-adv1',
        'hash-adv1',
        HARNESS,
        'adv1',
        'resource-adv1',
        'thread-adv1',
        srcSession.session_incarnation,
        'grant-adv1',
        1,
        'sig-adv1',
        'run-adv1',
        1,
        'finalizer-1',
        'v1',
        '{}',
        '{}',
        16,
        'pending',
        0,
        null,
        null,
        null,
        null,
        now,
        now,
        null,
        null,
      ],
    );

    const exported = await exportExecutionClosure(
      src.s.db,
      { harnessName: HARNESS, sessionId: 'adv1' },
      { schemaName: src.schemaName },
    );
    const first = await importExecutionClosure(dst.s.db, exported, { schemaName: dst.schemaName });
    expect(first.status).toBe('imported');

    // Between the first commit and a lost-ack retry, destination workers
    // advance the imported rows: the intent is claimed and the session is
    // saved at a newer version (which also stages the next projection intent
    // and bumps its fence revision).
    await dst.s.db.none(
      `UPDATE "${dst.schemaName}"."${TABLE_HARNESS_TERMINAL_INTENTS}"
       SET status = 'claimed', claim_id = 'worker-1', claim_expires_at = $1,
           next_attempt_at = $1, updated_at = $1
       WHERE id = 'intent-adv1'`,
      [now + 30_000],
    );
    const harness = dst.s.stores.harness!;
    const loaded = await harness.loadSession({ harnessName: HARNESS, sessionId: 'adv1' });
    await harness.saveSession(
      { ...loaded!, tokenUsage: { promptTokens: 5, completionTokens: 5, totalTokens: 10 } },
      { ifVersion: loaded!.version },
    );

    const second = await importExecutionClosure(dst.s.db, exported, { schemaName: dst.schemaName });
    expect(second.status).toBe('imported');
    expect(second.incarnations.adv1).toBe(first.incarnations.adv1);
    expect(second.skipped[TABLE_HARNESS_SESSIONS]).toBe(2);
    expect(second.skipped[TABLE_HARNESS_TERMINAL_INTENTS]).toBe(1);

    // The worker's claim and session advance were never overwritten.
    const intent = await dst.s.db.one<Record<string, unknown>>(
      `SELECT status, claim_id FROM "${dst.schemaName}"."${TABLE_HARNESS_TERMINAL_INTENTS}" WHERE id = 'intent-adv1'`,
    );
    expect(intent.status).toBe('claimed');
    expect(intent.claim_id).toBe('worker-1');
    const session = await dst.s.db.one<{ version: number }>(
      `SELECT version FROM "${dst.schemaName}"."${TABLE_HARNESS_SESSIONS}" WHERE id = 'adv1'`,
    );
    expect(Number(session.version)).toBe(2);
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

  it('restores outbox rows — terminal receipts preserved, claimed work requeued claim-free', async () => {
    const src = await store('outbox-src');
    const dst = await store('outbox-dst');
    const { threadId, resourceId } = await seedClosure(src.s, src.schemaName, 'ob1');
    const now = Date.now();
    const insertOutbox = (id: string, status: string, extra: Record<string, unknown>) =>
      src.s.db.none(
        `INSERT INTO "${src.schemaName}"."${TABLE_HARNESS_CHANNEL_OUTBOX}"
           (id, harness_name, channel_id, provider_id, binding_id, binding_generation,
            idempotency_key, payload_hash, resource_id, thread_id, session_id,
            target, kind, operation_kind, payload, delivery_semantics,
            status, attempts, claim_id, claim_expires_at, sent_at,
            provider_message_id, created_at, updated_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb,$13,$14,$15::jsonb,$16,$17,$18,$19,$20,$21,$22,$23,$24)`,
        [
          id,
          HARNESS,
          'chan-1',
          'provider-1',
          'bind-1',
          1,
          `idem-${id}`,
          'hash-1',
          resourceId,
          threadId,
          'ob1',
          JSON.stringify({ channel: 'chan-1' }),
          'message',
          'send',
          JSON.stringify({ text: 'hi' }),
          'at-least-once',
          status,
          extra.attempts ?? 0,
          extra.claim_id ?? null,
          extra.claim_expires_at ?? null,
          extra.sent_at ?? null,
          extra.provider_message_id ?? null,
          now,
          now,
        ],
      );
    // A claimed row is live work mid-delivery: the stale source claim is
    // meaningless on the destination, so the row requeues as pending.
    await insertOutbox('ob-claimed', 'claimed', {
      attempts: 2,
      claim_id: 'src-worker',
      claim_expires_at: now + 30_000,
    });
    // A sent row is the idempotency receipt: losing it would let a re-enqueued
    // effect repeat a provider-visible action.
    await insertOutbox('ob-sent', 'sent', { sent_at: now - 1000, provider_message_id: 'pm-1' });

    const exported = await exportExecutionClosure(
      src.s.db,
      { harnessName: HARNESS, sessionId: 'ob1' },
      { schemaName: src.schemaName },
    );
    expect((exported.rows[TABLE_HARNESS_CHANNEL_OUTBOX] ?? []).map(r => r.id).sort()).toEqual([
      'ob-claimed',
      'ob-sent',
    ]);

    const result = await importExecutionClosure(dst.s.db, exported, { schemaName: dst.schemaName });
    expect(result.status).toBe('imported');
    expect(result.inserted[TABLE_HARNESS_CHANNEL_OUTBOX]).toBe(2);

    const restored = await dst.s.db.manyOrNone<Record<string, unknown>>(
      `SELECT id, status, claim_id, claim_expires_at, provider_message_id, sent_at
       FROM "${dst.schemaName}"."${TABLE_HARNESS_CHANNEL_OUTBOX}" ORDER BY id`,
    );
    const claimed = restored.find(r => r.id === 'ob-claimed')!;
    expect(claimed.status).toBe('pending');
    expect(claimed.claim_id).toBeNull();
    expect(claimed.claim_expires_at).toBeNull();
    const sent = restored.find(r => r.id === 'ob-sent')!;
    expect(sent.status).toBe('sent');
    expect(sent.provider_message_id).toBe('pm-1');
    expect(sent.sent_at).not.toBeNull();
  });

  it('restores a suspended background task the destination manager can re-drive', async () => {
    const src = await store('task-src');
    const dst = await store('task-dst');
    const { threadId, resourceId, runId } = await seedClosure(src.s, src.schemaName, 'bt2');
    const now = new Date();
    await src.s.db.none(
      `INSERT INTO "${src.schemaName}"."${TABLE_BACKGROUND_TASKS}"
         (id, tool_call_id, tool_name, agent_id, run_id, thread_id, resource_id,
          status, args, suspend_payload, retry_count, max_retries, timeout_ms,
          "createdAt", "startedAt", "suspendedAt")
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10::jsonb,$11,$12,$13,$14,$15,$16)`,
      [
        'task-1',
        'tc-1',
        'tool-1',
        'agent-1',
        runId,
        threadId,
        resourceId,
        'suspended',
        '{}',
        JSON.stringify({ approval: 'needed' }),
        1,
        3,
        30_000,
        now,
        now,
        now,
      ],
    );

    const exported = await exportExecutionClosure(
      src.s.db,
      { harnessName: HARNESS, sessionId: 'bt2' },
      { schemaName: src.schemaName },
    );
    const result = await importExecutionClosure(dst.s.db, exported, { schemaName: dst.schemaName });
    expect(result.status).toBe('imported');
    expect(result.inserted[TABLE_BACKGROUND_TASKS]).toBe(1);

    const task = await dst.s.db.one<Record<string, unknown>>(
      `SELECT id, status, suspend_payload, run_id FROM "${dst.schemaName}"."${TABLE_BACKGROUND_TASKS}" WHERE id = 'task-1'`,
    );
    expect(task.status).toBe('suspended');
    expect(task.run_id).toBe(runId);
    expect(task.suspend_payload).toMatchObject({ approval: 'needed' });
  });

  it('pins the import when a restaged projection intent exceeds the destination bound', async () => {
    const src = await store('oversize-src');
    const dst = await store('oversize-dst');
    await seedClosure(src.s, src.schemaName, 'ov1');
    const exported = await exportExecutionClosure(
      src.s.db,
      { harnessName: HARNESS, sessionId: 'ov1' },
      { schemaName: src.schemaName },
    );

    // The exported active fences restage post-images built under the source's
    // bound — a destination configured tighter could never build them either,
    // so the import pins the session instead of reporting the unit imported
    // while the read model silently misses a revision.
    const result = await importExecutionClosure(dst.s.db, exported, {
      schemaName: dst.schemaName,
      maxProjectionPayloadBytes: 1,
    });
    expect(result.status).toBe('pinned');
    expect(result.pins).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          reason: 'projection-restage-exceeds-bound',
          detail: expect.objectContaining({ sessionId: 'ov1' }),
        }),
      ]),
    );

    // Under a bound that can hold the post-image the same closure imports.
    const dst2 = await store('oversize-dst2');
    const wide = await importExecutionClosure(dst2.s.db, exported, { schemaName: dst2.schemaName });
    expect(wide.status).toBe('imported');
    expect(wide.inserted[TABLE_HARNESS_SESSION_PROJECTION_INTENTS]).toBe(2);
  });
});
