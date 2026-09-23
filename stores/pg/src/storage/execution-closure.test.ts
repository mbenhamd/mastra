import { randomUUID } from 'node:crypto';

import { createSampleMessageV2, createSampleResource, createSampleSessionRecord } from '@internal/storage-test-utils';
import {
  TABLE_BACKGROUND_TASKS,
  TABLE_HARNESS_ATTACHMENTS,
  TABLE_HARNESS_ATTACHMENT_OPERATIONS,
  TABLE_HARNESS_CHANNEL_ACTION_TOKENS,
  TABLE_HARNESS_CHANNEL_BINDINGS,
  TABLE_HARNESS_CHANNEL_INBOX,
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
  buildExecutionClosureManifest,
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

    // Seed one fence row (operation tombstone) and one requeued fence row
    // (wakeup) directly — the export must carry them as evidence, not drop
    // them.
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
    // Wakeups are durable session work — `fence`, not `authority`: a live
    // claim requeues on import instead of being dropped with the schedule.
    expect(byTable[TABLE_HARNESS_WAKEUPS]!.role).toBe('fence');
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

  it('pins the export while a source session holds a live lease', async () => {
    const { s, schemaName } = await store('lease');
    await seedClosure(s, schemaName, 'ls1');
    // A live owner/lease means a source worker still owns the subtree: the
    // snapshot is stale on arrival, so the closure pins rather than
    // split-brain the session across stores.
    await s.db.none(
      `UPDATE "${schemaName}"."${TABLE_HARNESS_SESSIONS}"
       SET owner_id = 'src-worker', lease_expires_at = $1 WHERE id = 'ls1'`,
      [Date.now() + 60_000],
    );

    const { manifest } = await exportExecutionClosure(s.db, { harnessName: HARNESS, sessionId: 'ls1' }, { schemaName });
    expect(manifest.completeness).toBe('pinned');
    expect(manifest.pins).toEqual([
      expect.objectContaining({
        reason: 'session-lease-active',
        detail: expect.objectContaining({ sessionId: 'ls1', ownerId: 'src-worker' }),
      }),
    ]);
  });

  it('does not pin on an expired lease', async () => {
    const { s, schemaName } = await store('leaseexpired');
    await seedClosure(s, schemaName, 'le1');
    await s.db.none(
      `UPDATE "${schemaName}"."${TABLE_HARNESS_SESSIONS}"
       SET owner_id = 'src-worker', lease_expires_at = $1 WHERE id = 'le1'`,
      [Date.now() - 1_000],
    );

    const { manifest } = await exportExecutionClosure(s.db, { harnessName: HARNESS, sessionId: 'le1' }, { schemaName });
    expect(manifest.pins.filter(p => p.reason === 'session-lease-active')).toEqual([]);
  });

  it('pins the export while an unrevoked channel action token is live', async () => {
    const { s, schemaName } = await store('token');
    const { resourceId } = await seedClosure(s, schemaName, 'tk1');
    const now = Date.now();
    const insertToken = (id: string, expiresAt: number | null, revokedAt: number | null) =>
      s.db.none(
        `INSERT INTO "${schemaName}"."${TABLE_HARNESS_CHANNEL_ACTION_TOKENS}"
           (action_token_id, harness_name, channel_id, provider_id, resource_id,
            owning_session_id, item_id, kind, binding_id, binding_generation,
            run_id, pending_requested_at, audience, metadata_hash, transport_hash,
            key_id, expires_at, revoked_at, revoked_reason, created_at, updated_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb,$14,$15,$16,$17,$18,$19,$20,$21)`,
        [
          id,
          HARNESS,
          'chan-1',
          'provider-1',
          resourceId,
          'tk1',
          `item-${id}`,
          'approval',
          'bind-1',
          1,
          'run-tk1',
          now - 5_000,
          JSON.stringify({ mode: 'default' }),
          'meta-hash',
          `transport-hash-${id}`,
          null,
          expiresAt,
          revokedAt,
          revokedAt === null ? null : 'revoked',
          now,
          now,
        ],
      );
    // An unexpired token binds live provider callbacks into this session —
    // the migrated session could never settle them, so the unit pins.
    await insertToken('token-live', now + 60_000, null);
    // A token with no expiry is live until revoked — it pins too.
    await insertToken('token-open', null, null);
    // Expired and revoked tokens are settled evidence — they must not pin.
    await insertToken('token-expired', now - 60_000, null);
    await insertToken('token-revoked', null, now - 1_000);

    const { manifest } = await exportExecutionClosure(s.db, { harnessName: HARNESS, sessionId: 'tk1' }, { schemaName });
    expect(manifest.completeness).toBe('pinned');
    expect(manifest.pins).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          reason: 'channel-action-token-active',
          detail: expect.objectContaining({ owningSessionId: 'tk1', actionTokenId: 'token-live' }),
        }),
        expect.objectContaining({
          reason: 'channel-action-token-active',
          detail: expect.objectContaining({ owningSessionId: 'tk1', actionTokenId: 'token-open' }),
        }),
      ]),
    );
    expect(manifest.pins).toHaveLength(2);
  });

  it('retires the exported incarnation on the source when the closure is complete', async () => {
    const { s, schemaName } = await store('fenceretire');
    await seedClosure(s, schemaName, 'fr1');
    const before = await s.db.one<{ session_incarnation: string; version: number }>(
      `SELECT session_incarnation, version FROM "${schemaName}"."${TABLE_HARNESS_SESSIONS}" WHERE id = 'fr1'`,
    );

    const { manifest } = await exportExecutionClosure(s.db, { harnessName: HARNESS, sessionId: 'fr1' }, { schemaName });
    expect(manifest.completeness).toBe('complete');
    // The manifest still describes the exported (pre-rotation) epoch — its
    // incarnation is what the importer rebinds fence rows away from.
    expect(manifest.incarnations.fr1).toBe(before.session_incarnation);

    const after = await s.db.one<{ session_incarnation: string; version: number }>(
      `SELECT session_incarnation, version FROM "${schemaName}"."${TABLE_HARNESS_SESSIONS}" WHERE id = 'fr1'`,
    );
    // The exported epoch is retired atomically with the snapshot: a worker
    // that acquires a fresh lease on the idle row can no longer execute
    // under the exported incarnation, and a stale `ifVersion` save
    // conflicts. The lease columns are untouched — the fence is the
    // incarnation, not a fabricated owner.
    expect(after.session_incarnation).not.toBe(before.session_incarnation);
    expect(after.session_incarnation).toMatch(/^[0-9a-f-]{36}$/);
    expect(after.version).toBe(before.version + 1);
  });

  it('leaves the source incarnation untouched when the closure is pinned', async () => {
    const { s, schemaName } = await store('fencepin');
    const harness = s.stores.harness!;
    await harness.saveSession(
      createSampleSessionRecord({
        id: 'fp1',
        harnessName: HARNESS,
        resourceId: 'resource-fp1',
        threadId: 'ghost-thread',
      }),
      { ifVersion: 0 },
    );
    const before = await s.db.one<{ session_incarnation: string; version: number }>(
      `SELECT session_incarnation, version FROM "${schemaName}"."${TABLE_HARNESS_SESSIONS}" WHERE id = 'fp1'`,
    );

    const { manifest } = await exportExecutionClosure(s.db, { harnessName: HARNESS, sessionId: 'fp1' }, { schemaName });
    expect(manifest.completeness).toBe('pinned');

    // A pinned unit is not a migration boundary — the caller resolves the
    // pins and re-exports, so the live incarnation must survive untouched
    // for the next export's fence rows to rebind.
    const after = await s.db.one<{ session_incarnation: string; version: number }>(
      `SELECT session_incarnation, version FROM "${schemaName}"."${TABLE_HARNESS_SESSIONS}" WHERE id = 'fp1'`,
    );
    expect(after.session_incarnation).toBe(before.session_incarnation);
    expect(after.version).toBe(before.version);
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

  it('restores unsettled wakeup work — a live claim requeues claim-free', async () => {
    const src = await store('wake-src');
    const dst = await store('wake-dst');
    const { threadId, resourceId } = await seedClosure(src.s, src.schemaName, 'wk1');
    const now = Date.now();
    const insertWakeup = (id: string, status: string, extra: Record<string, unknown>) =>
      src.s.db.none(
        `INSERT INTO "${src.schemaName}"."${TABLE_HARNESS_WAKEUPS}"
           (id, harness_name, source, source_id, fire_id, idempotency_key,
            payload_hash, admission_id, session_id, resource_id, thread_id,
            due_at, status, attempts, claim_id, claim_expires_at, claimed_at,
            next_attempt_at, missed_count, content, attachments, created_at, updated_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23)`,
        [
          id,
          HARNESS,
          'timer',
          'timer-1',
          `fire-${id}`,
          `idem-${id}`,
          'hash-1',
          `adm-${id}`,
          'wk1',
          resourceId,
          threadId,
          now + 60_000,
          status,
          extra.attempts ?? 0,
          extra.claim_id ?? null,
          extra.claim_expires_at ?? null,
          extra.claimed_at ?? null,
          extra.next_attempt_at ?? null,
          extra.missed_count ?? null,
          'wakeup',
          '[]',
          now,
          now,
        ],
      );
    // A mid-delivery claim is a source lease no destination worker can renew:
    // the row requeues as `due` with claim metadata cleared instead of
    // parking behind a lease that expires unreaped.
    await insertWakeup('wake-claimed', 'claimed', {
      attempts: 1,
      claim_id: 'src-worker',
      claim_expires_at: now + 30_000,
      claimed_at: now - 5_000,
      missed_count: 2,
    });
    // A settled row is durable evidence and must also survive the move.
    await insertWakeup('wake-done', 'completed', {});

    const exported = await exportExecutionClosure(
      src.s.db,
      { harnessName: HARNESS, sessionId: 'wk1' },
      { schemaName: src.schemaName },
    );
    expect((exported.rows[TABLE_HARNESS_WAKEUPS] ?? []).map(r => r.id).sort()).toEqual(['wake-claimed', 'wake-done']);

    const result = await importExecutionClosure(dst.s.db, exported, { schemaName: dst.schemaName });
    expect(result.status).toBe('imported');
    expect(result.inserted[TABLE_HARNESS_WAKEUPS]).toBe(2);

    const restored = await dst.s.db.manyOrNone<Record<string, unknown>>(
      `SELECT id, status, claim_id, claim_expires_at, claimed_at, missed_count
       FROM "${dst.schemaName}"."${TABLE_HARNESS_WAKEUPS}" ORDER BY id`,
    );
    const claimed = restored.find(r => r.id === 'wake-claimed')!;
    expect(claimed.status).toBe('due');
    expect(claimed.claim_id).toBeNull();
    expect(claimed.claim_expires_at).toBeNull();
    expect(claimed.claimed_at).toBeNull();
    const done = restored.find(r => r.id === 'wake-done')!;
    expect(done.status).toBe('completed');
  });

  it('never restores authority rows — channel bindings stay absent after import', async () => {
    const src = await store('auth-src');
    const dst = await store('auth-dst');
    const { threadId, resourceId } = await seedClosure(src.s, src.schemaName, 'rs4');
    const now = Date.now();
    await src.s.db.none(
      `INSERT INTO "${src.schemaName}"."${TABLE_HARNESS_CHANNEL_BINDINGS}"
         (id, harness_name, channel_id, provider_id, status, platform,
          external_tenant_id, external_channel_id, external_thread_id,
          resource_id, thread_id, session_id, mode, generation,
          created_at, updated_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`,
      [
        `bind-${randomUUID()}`,
        HARNESS,
        'chan-rs4',
        'provider-1',
        'active',
        'test',
        'tenant',
        'ext-chan',
        'ext-thread',
        resourceId,
        threadId,
        'rs4',
        'default',
        1,
        now,
        now,
      ],
    );

    const exported = await exportExecutionClosure(
      src.s.db,
      { harnessName: HARNESS, sessionId: 'rs4' },
      { schemaName: src.schemaName },
    );
    expect(exported.manifest.tables.find(t => t.table === TABLE_HARNESS_CHANNEL_BINDINGS)!.rowCount).toBe(1);

    const result = await importExecutionClosure(dst.s.db, exported, { schemaName: dst.schemaName });
    // Exported as evidence; skipped on import — the fresh incarnation
    // re-establishes its own bindings instead of reviving stale routing.
    expect(result.skipped[TABLE_HARNESS_CHANNEL_BINDINGS]).toBe(1);
    const count = await dst.s.db.one<{ n: string }>(
      `SELECT count(*)::text AS n FROM "${dst.schemaName}"."${TABLE_HARNESS_CHANNEL_BINDINGS}"`,
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

  it('restores unsettled channel inbox rows — claims clear, mid-delivery rows requeue', async () => {
    const src = await store('inbox-src');
    const dst = await store('inbox-dst');
    const { threadId, resourceId } = await seedClosure(src.s, src.schemaName, 'in1');
    const now = Date.now();
    const insertInbox = (id: string, status: string, extra: Record<string, unknown>) =>
      src.s.db.none(
        `INSERT INTO "${src.schemaName}"."${TABLE_HARNESS_CHANNEL_INBOX}"
           (id, harness_name, channel_id, provider_id, idempotency_key, payload_hash,
            admission_id, binding_id, resource_id, thread_id, session_id,
            external_message_id, received_at, updated_at, status, attempts,
            claim_id, claim_expires_at, request_context, content, attachments)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19::jsonb,$20,$21::jsonb)`,
        [
          id,
          HARNESS,
          'chan-1',
          'provider-1',
          `idem-${id}`,
          'hash-1',
          `adm-${id}`,
          'bind-1',
          resourceId,
          threadId,
          'in1',
          `ext-${id}`,
          now - 10_000,
          now,
          status,
          extra.attempts ?? 0,
          extra.claim_id ?? null,
          extra.claim_expires_at ?? null,
          '{}',
          'inbound',
          '[]',
        ],
      );
    // Mid-delivery on the source: the claim/lease is source authority, so the
    // row requeues as claimable `received` work instead of parking behind a
    // lease no destination worker can renew.
    await insertInbox('in-claimed', 'claimed', {
      attempts: 1,
      claim_id: 'src-worker',
      claim_expires_at: now + 30_000,
    });
    // Retryable failure is recovery work the destination re-drives.
    await insertInbox('in-failed', 'failed', { attempts: 2 });
    // A terminal row is the idempotency receipt for a provider redelivery.
    await insertInbox('in-accepted', 'accepted', {});

    const exported = await exportExecutionClosure(
      src.s.db,
      { harnessName: HARNESS, sessionId: 'in1' },
      { schemaName: src.schemaName },
    );
    expect((exported.rows[TABLE_HARNESS_CHANNEL_INBOX] ?? []).map(r => r.id).sort()).toEqual([
      'in-accepted',
      'in-claimed',
      'in-failed',
    ]);

    const result = await importExecutionClosure(dst.s.db, exported, { schemaName: dst.schemaName });
    expect(result.status).toBe('imported');
    expect(result.inserted[TABLE_HARNESS_CHANNEL_INBOX]).toBe(3);

    const restored = await dst.s.db.manyOrNone<Record<string, unknown>>(
      `SELECT id, status, claim_id, claim_expires_at FROM "${dst.schemaName}"."${TABLE_HARNESS_CHANNEL_INBOX}" ORDER BY id`,
    );
    const claimed = restored.find(r => r.id === 'in-claimed')!;
    expect(claimed.status).toBe('received');
    expect(claimed.claim_id).toBeNull();
    expect(claimed.claim_expires_at).toBeNull();
    expect(restored.find(r => r.id === 'in-failed')!.status).toBe('failed');
    expect(restored.find(r => r.id === 'in-accepted')!.status).toBe('accepted');
  });

  it('pins the import when the destination has terminal handoff disabled', async () => {
    const src = await store('nodest-src');
    const dst = await store('nodest-dst');
    await seedClosure(src.s, src.schemaName, 'nh1');
    const now = Date.now();
    const srcSession = await src.s.db.one<{ session_incarnation: string }>(
      `SELECT session_incarnation FROM "${src.schemaName}"."${TABLE_HARNESS_SESSIONS}" WHERE id = 'nh1'`,
    );
    // A live intent travels in the closure as fence evidence — a destination
    // that cannot claim it must pin rather than report the unit imported.
    await src.s.db.none(
      `INSERT INTO "${src.schemaName}"."${TABLE_HARNESS_TERMINAL_INTENTS}"
         (id, admission_id, admission_hash, harness_name, session_id, resource_id, thread_id,
          session_incarnation, grant_key, grant_generation, signal_id, run_id, revision,
          finalizer_id, finalizer_version, terminal_result_json, projection_json, payload_bytes,
          status, attempts, claim_id, claim_expires_at, consumer_id, next_attempt_at, last_error_json,
          created_at, updated_at, acked_at, dead_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29)`,
      [
        'intent-nh1',
        'adm-nh1',
        'hash-nh1',
        HARNESS,
        'nh1',
        'resource-nh1',
        'thread-nh1',
        srcSession.session_incarnation,
        'grant-nh1',
        1,
        'sig-nh1',
        'run-nh1',
        1,
        'finalizer-1',
        'v1',
        '{}',
        '{}',
        16,
        'claimed',
        1,
        'src-worker',
        now + 30_000,
        'src-consumer',
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
      { harnessName: HARNESS, sessionId: 'nh1' },
      { schemaName: src.schemaName },
    );
    const result = await importExecutionClosure(dst.s.db, exported, {
      schemaName: dst.schemaName,
      terminalHandoffEnabled: false,
    });
    expect(result.status).toBe('pinned');
    expect(result.pins).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          reason: 'terminal-handoff-disabled',
          detail: expect.objectContaining({ sessionId: 'nh1', intentId: 'intent-nh1' }),
        }),
      ]),
    );
    // The intent still restored as requeued fence evidence — the pin reports
    // capability, not a dropped row.
    const intent = await dst.s.db.one<Record<string, unknown>>(
      `SELECT status, claim_id, consumer_id FROM "${dst.schemaName}"."${TABLE_HARNESS_TERMINAL_INTENTS}" WHERE id = 'intent-nh1'`,
    );
    expect(intent.status).toBe('pending');
    expect(intent.claim_id).toBeNull();
    expect(intent.consumer_id).toBeNull();

    // With handoff enabled the same closure imports clean.
    const dst2 = await store('nodest-dst2');
    const enabled = await importExecutionClosure(dst2.s.db, exported, {
      schemaName: dst2.schemaName,
      terminalHandoffEnabled: true,
    });
    expect(enabled.status).toBe('imported');
  });

  it('pins the import when session-record projection is disabled at the destination', async () => {
    const src = await store('noproj-src');
    const dst = await store('noproj-dst');
    await seedClosure(src.s, src.schemaName, 'np1');
    const exported = await exportExecutionClosure(
      src.s.db,
      { harnessName: HARNESS, sessionId: 'np1' },
      { schemaName: src.schemaName },
    );
    // The seeded sessions carry active projection fences — a destination
    // whose projection domain is off could never apply a restaged intent, so
    // each fenced session pins instead of going silently stale.
    const result = await importExecutionClosure(dst.s.db, exported, {
      schemaName: dst.schemaName,
      projectionEnabled: false,
    });
    expect(result.status).toBe('pinned');
    expect(result.pins).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          reason: 'session-record-projection-disabled',
          detail: expect.objectContaining({ sessionId: 'np1' }),
        }),
        expect.objectContaining({
          reason: 'session-record-projection-disabled',
          detail: expect.objectContaining({ sessionId: 'np1-child' }),
        }),
      ]),
    );
    const intents = await dst.s.db.one<{ n: string }>(
      `SELECT count(*)::text AS n FROM "${dst.schemaName}"."${TABLE_HARNESS_SESSION_PROJECTION_INTENTS}"`,
    );
    expect(intents.n).toBe('0');
  });

  it('keeps a historical incarnation stamp on fence rows instead of reviving it', async () => {
    const src = await store('hist-src');
    const dst = await store('hist-dst');
    await seedClosure(src.s, src.schemaName, 'hi1');
    const now = Date.now();
    const srcSession = await src.s.db.one<{ session_incarnation: string }>(
      `SELECT session_incarnation FROM "${src.schemaName}"."${TABLE_HARNESS_SESSIONS}" WHERE id = 'hi1'`,
    );
    const insertIntent = (id: string, incarnation: string) =>
      src.s.db.none(
        `INSERT INTO "${src.schemaName}"."${TABLE_HARNESS_TERMINAL_INTENTS}"
           (id, admission_id, admission_hash, harness_name, session_id, resource_id, thread_id,
            session_incarnation, grant_key, grant_generation, signal_id, run_id, revision,
            finalizer_id, finalizer_version, terminal_result_json, projection_json, payload_bytes,
            status, attempts, created_at, updated_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22)`,
        [
          id,
          `adm-${id}`,
          `hash-${id}`,
          HARNESS,
          'hi1',
          'resource-hi1',
          'thread-hi1',
          incarnation,
          `grant-${id}`,
          1,
          `sig-${id}`,
          'run-hi1',
          1,
          'finalizer-1',
          'v1',
          '{}',
          '{}',
          16,
          'dead',
          0,
          now,
          now,
        ],
      );
    // Only the row stamped with the incarnation the source exported rebinds
    // to the destination incarnation — a row left behind by an older
    // incarnation keeps its stamp as dead evidence.
    await insertIntent('intent-current', srcSession.session_incarnation);
    await insertIntent('intent-historical', 'incarnation-superseded');

    const exported = await exportExecutionClosure(
      src.s.db,
      { harnessName: HARNESS, sessionId: 'hi1' },
      { schemaName: src.schemaName },
    );
    const result = await importExecutionClosure(dst.s.db, exported, { schemaName: dst.schemaName });
    expect(result.status).toBe('imported');

    const restored = await dst.s.db.manyOrNone<{ id: string; session_incarnation: string }>(
      `SELECT id, session_incarnation FROM "${dst.schemaName}"."${TABLE_HARNESS_TERMINAL_INTENTS}" ORDER BY id`,
    );
    const current = restored.find(r => r.id === 'intent-current')!;
    expect(current.session_incarnation).toBe(result.incarnations.hi1);
    const historical = restored.find(r => r.id === 'intent-historical')!;
    expect(historical.session_incarnation).toBe('incarnation-superseded');
  });

  it('fails a retry when the destination parent-revision regressed below the imported generation', async () => {
    const src = await store('rev-src');
    const dst = await store('rev-dst');
    const { runId } = await seedClosure(src.s, src.schemaName, 'pr1');
    const exported = await exportExecutionClosure(
      src.s.db,
      { harnessName: HARNESS, sessionId: 'pr1' },
      { schemaName: src.schemaName },
    );
    const first = await importExecutionClosure(dst.s.db, exported, { schemaName: dst.schemaName });
    expect(first.status).toBe('imported');

    // A foreign row can collide on the parent-revision PK while differing
    // only in registered lifecycle columns — a stored generation below the
    // imported one is not a valid successor and must fail closed.
    await dst.s.db.none(
      `UPDATE "${dst.schemaName}"."mastra_workflow_parent_revisions"
       SET generation = generation - 1
       WHERE workflow_name = 'test-workflow' AND run_id = $1 AND generation >= 1`,
      [runId],
    );
    await expect(importExecutionClosure(dst.s.db, exported, { schemaName: dst.schemaName })).rejects.toThrow(
      /conflicts with a different destination row/i,
    );

    // Forward progress is still a valid successor: restore the generation
    // ahead of the imported row and the retry converges again.
    await dst.s.db.none(
      `UPDATE "${dst.schemaName}"."mastra_workflow_parent_revisions"
       SET generation = generation + 5
       WHERE workflow_name = 'test-workflow' AND run_id = $1`,
      [runId],
    );
    const converged = await importExecutionClosure(dst.s.db, exported, { schemaName: dst.schemaName });
    expect(converged.status).toBe('imported');
  });

  it('converges when a destination worker only renewed the imported session lease', async () => {
    const src = await store('lease-src');
    const dst = await store('lease-dst');
    await seedClosure(src.s, src.schemaName, 'lr1');
    const exported = await exportExecutionClosure(
      src.s.db,
      { harnessName: HARNESS, sessionId: 'lr1' },
      { schemaName: src.schemaName },
    );
    const first = await importExecutionClosure(dst.s.db, exported, { schemaName: dst.schemaName });
    expect(first.status).toBe('imported');

    // A lease acquisition/renewal touches owner_id + lease_expires_at without
    // advancing version — a retry must read it as the same row, not a foreign
    // conflict.
    await dst.s.db.none(
      `UPDATE "${dst.schemaName}"."${TABLE_HARNESS_SESSIONS}"
       SET owner_id = 'dst-worker', lease_expires_at = $1 WHERE id = 'lr1'`,
      [Date.now() + 60_000],
    );
    const second = await importExecutionClosure(dst.s.db, exported, { schemaName: dst.schemaName });
    expect(second.status).toBe('imported');
    expect(second.incarnations.lr1).toBe(first.incarnations.lr1);
    expect(second.skipped[TABLE_HARNESS_SESSIONS]).toBe(2);

    // The destination's live lease was never rewritten by the retry.
    const row = await dst.s.db.one<{ owner_id: string }>(
      `SELECT owner_id FROM "${dst.schemaName}"."${TABLE_HARNESS_SESSIONS}" WHERE id = 'lr1'`,
    );
    expect(row.owner_id).toBe('dst-worker');
  });

  it('rejects a payload whose rows bind ids outside the declared closure scope', async () => {
    const src = await store('scope-src');
    const dst = await store('scope-dst');
    await seedClosure(src.s, src.schemaName, 'sc1');
    const exported = await exportExecutionClosure(
      src.s.db,
      { harnessName: HARNESS, sessionId: 'sc1' },
      { schemaName: src.schemaName },
    );

    // Forge a row bound to a thread the manifest never declared — the digest
    // alone cannot reject it because a hand-built manifest can recompute
    // digests over forged rows, so the verifier checks every payload row's
    // scope columns against the declared id sets.
    const forgedMessage = {
      ...(exported.rows[TABLE_MESSAGES]![0] as Record<string, unknown>),
      id: `forged-${randomUUID()}`,
      thread_id: 'thread-never-exported',
    };
    const forgedRows = {
      ...exported.rows,
      [TABLE_MESSAGES]: [...(exported.rows[TABLE_MESSAGES] ?? []), forgedMessage],
    };
    const forgedManifest = buildExecutionClosureManifest({
      key: exported.manifest.key,
      sessionIds: exported.manifest.sessionIds,
      incarnations: exported.manifest.incarnations,
      threadIds: exported.manifest.threadIds,
      runIds: exported.manifest.runIds,
      resourceIds: exported.manifest.resourceIds,
      channelIds: exported.manifest.channelIds,
      threadStateKeys: exported.manifest.threadStateKeys,
      runPairs: exported.manifest.runPairs,
      rows: forgedRows,
      pins: exported.manifest.pins,
    });

    const verified = verifyExecutionClosurePayload(forgedManifest, forgedRows);
    expect(verified.ok).toBe(false);
    expect(verified.mismatches).toEqual(
      expect.arrayContaining([expect.stringContaining('outside the declared closure scope')]),
    );
    await expect(
      importExecutionClosure(dst.s.db, { manifest: forgedManifest, rows: forgedRows }, { schemaName: dst.schemaName }),
    ).rejects.toThrow(/manifest verification/i);
  });
});
