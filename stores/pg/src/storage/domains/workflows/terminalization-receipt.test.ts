import { randomUUID } from 'node:crypto';
import {
  MAX_WORKFLOW_TERMINAL_DESTINATION_RECEIPTS_PER_EFFECT,
  createEmptyWorkflowSnapshot,
  createWorkflowTerminalGraphFingerprint,
  getWorkflowTerminalSnapshotRecordHash,
} from '@mastra/core/storage';
import { getWorkflowTerminalRecoveryEnvelopeHash } from '@mastra/core/workflows';
import type { WorkflowRunState } from '@mastra/core/workflows';
import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createTerminalRecoveryEnvelope } from './terminalization-test-utils';
import { WorkflowsPG } from '.';

describe('WorkflowsPG terminal destination receipts', () => {
  const pool = new Pool({
    host: process.env.POSTGRES_HOST || '127.0.0.1',
    port: Number(process.env.POSTGRES_PORT) || 5434,
    database: process.env.POSTGRES_DB || 'postgres',
    user: process.env.POSTGRES_USER || 'postgres',
    password: process.env.POSTGRES_PASSWORD || 'postgres',
  });
  const workflowsA = new WorkflowsPG({ pool });
  const workflowsB = new WorkflowsPG({ pool });

  beforeAll(async () => {
    await workflowsA.init();
  });

  afterAll(async () => {
    await pool.end();
  });

  async function cleanup(workflowName: string): Promise<void> {
    await pool.query(`DELETE FROM mastra_workflow_terminal_destination_receipts_v2 WHERE workflow_name = $1`, [
      workflowName,
    ]);
    await pool.query(`DELETE FROM mastra_workflow_terminal_effects_v2 WHERE workflow_name = $1`, [workflowName]);
    await pool.query(`DELETE FROM mastra_workflow_terminal_snapshots_v2 WHERE workflow_name = $1`, [workflowName]);
    await pool.query(`DELETE FROM mastra_workflow_terminal_recovery_ancestries WHERE workflow_name = $1`, [
      workflowName,
    ]);
    await pool.query(`DELETE FROM mastra_workflow_terminalizations WHERE workflow_name = $1`, [workflowName]);
    await pool.query(`DELETE FROM mastra_workflow_snapshot WHERE workflow_name = $1`, [workflowName]);
  }

  async function refreshTerminalSnapshotRecordHash(workflowName: string, runId: string): Promise<void> {
    const retained = await pool.query<{
      version: string;
      workflow_name: string;
      run_id: string;
      resource_id: string | null;
      terminal_status: 'success' | 'failed' | 'canceled';
      envelope_hash: string;
      created_at: string;
    }>(
      `SELECT version::text, workflow_name, run_id, resource_id, terminal_status,
              envelope_hash, created_at::text
       FROM mastra_workflow_terminal_snapshots_v2
       WHERE workflow_name = $1 AND run_id = $2`,
      [workflowName, runId],
    );
    const row = retained.rows[0];
    if (!row) throw new Error('Expected retained workflow terminal snapshot');
    const recordHash = getWorkflowTerminalSnapshotRecordHash({
      version: Number(row.version) as 1,
      workflowName: row.workflow_name,
      runId: row.run_id,
      ...(row.resource_id === null ? {} : { resourceId: row.resource_id }),
      terminalStatus: row.terminal_status,
      envelopeHash: row.envelope_hash,
      createdAt: Number(row.created_at),
    });
    await pool.query(
      `UPDATE mastra_workflow_terminal_snapshots_v2 SET record_hash = $1
       WHERE workflow_name = $2 AND run_id = $3`,
      [recordHash, workflowName, runId],
    );
  }

  async function createReadyRun(
    workflows: WorkflowsPG,
    run: { workflowName: string; runId: string },
    effect:
      | { kind: 'workflow-finish' }
      | {
          kind: 'parent-workflow-step-end';
          parentWorkflowName: string;
          parentRunId: string;
          parentStepId: string;
          parentExecutionPath: number[];
        } = { kind: 'workflow-finish' },
  ) {
    await workflows.persistWorkflowSnapshot({ ...run, snapshot: createEmptyWorkflowSnapshot(run.runId) });
    const claim = await workflows.claimWorkflowTerminalization({
      ...run,
      eventKey: `${run.runId}-event`,
      terminalStatus: 'failed',
      ownerId: `${run.runId}-owner`,
      leaseMs: 10_000,
    });
    if (claim.status !== 'acquired') throw new Error(`Expected acquired, received ${claim.status}`);
    const fence = {
      ...run,
      ownerId: claim.record.ownerId,
      claimToken: claim.record.claimToken,
      claimGeneration: claim.record.claimGeneration,
    };
    const snapshot = { ...createEmptyWorkflowSnapshot(run.runId), status: 'failed' as const };
    const parentGraph: WorkflowRunState['serializedStepGraph'] =
      effect.kind === 'parent-workflow-step-end'
        ? Array.from({ length: effect.parentExecutionPath[0]! + 1 }, (_, rootIndex) =>
            rootIndex === effect.parentExecutionPath[0]
              ? effect.parentExecutionPath.length === 1
                ? { type: 'step' as const, step: { id: effect.parentStepId, component: 'WORKFLOW' } }
                : {
                    type: 'parallel' as const,
                    steps: Array.from({ length: effect.parentExecutionPath[1]! + 1 }, (_, branchIndex) => ({
                      type: 'step' as const,
                      step: {
                        id:
                          branchIndex === effect.parentExecutionPath[1]
                            ? effect.parentStepId
                            : `filler-${rootIndex}-${branchIndex}`,
                        component: branchIndex === effect.parentExecutionPath[1] ? 'WORKFLOW' : 'STEP',
                      },
                    })),
                  }
              : { type: 'sleep' as const, id: `filler-${rootIndex}`, duration: 1 },
          )
        : [];
    const ancestry =
      effect.kind === 'parent-workflow-step-end'
        ? [
            {
              version: 1 as const,
              childWorkflowName: run.workflowName,
              childRunId: run.runId,
              parentWorkflowName: effect.parentWorkflowName,
              parentRunId: effect.parentRunId,
              parentGraphFingerprint: createWorkflowTerminalGraphFingerprint(parentGraph),
              source: {
                kind: 'step' as const,
                stepId: effect.parentStepId,
                executionPath: effect.parentExecutionPath,
              },
              inputPointer: { kind: 'parent-source-payload' as const, stepId: effect.parentStepId },
              resultPointer: {
                kind: 'retained-terminal-result' as const,
                workflowName: run.workflowName,
                runId: run.runId,
              },
              resumeMetadata: { wasResume: false, resumeSteps: [] },
            },
          ]
        : [];
    if (ancestry.length > 0) {
      const immediate = ancestry[0]!;
      await workflows.persistWorkflowSnapshot({
        workflowName: immediate.parentWorkflowName,
        runId: immediate.parentRunId,
        snapshot: { ...createEmptyWorkflowSnapshot(immediate.parentRunId), serializedStepGraph: parentGraph },
      });
      await workflows.persistWorkflowTerminalRecoveryAncestry({ ...run, ancestry });
    }
    await workflows.persistWorkflowTerminalState({
      ...fence,
      snapshot,
      recoveryEnvelope: createTerminalRecoveryEnvelope({ ...run, snapshot, terminalStatus: 'failed', ancestry }),
    });
    const prepared = await workflows.prepareWorkflowTerminalEffect({
      ...fence,
      expectedPhase: 'run_state_persisted',
      effect,
    });
    if (prepared.status !== 'prepared') throw new Error(`Expected prepared, received ${prepared.status}`);
    return { run, fence, effect: prepared.effect };
  }

  function receiptInput(ready: Awaited<ReturnType<typeof createReadyRun>>, consumerId = 'terminal-consumer') {
    return { ...ready.fence, effectKind: ready.effect.kind, consumerId };
  }

  function withStatefulEnvelope<T extends ReturnType<typeof receiptInput>>(
    operation: T,
    alternate: T,
  ): { operation: T; reads: () => Record<keyof T, number> } {
    const fields = [
      'workflowName',
      'runId',
      'ownerId',
      'claimToken',
      'claimGeneration',
      'effectKind',
      'consumerId',
    ] as const;
    const reads = Object.fromEntries(fields.map(field => [field, 0])) as Record<keyof T, number>;
    for (const field of fields) {
      const intended = operation[field];
      Object.defineProperty(operation, field, {
        enumerable: true,
        get: () => (reads[field]++ === 0 ? intended : alternate[field]),
      });
    }
    return { operation, reads: () => ({ ...reads }) };
  }

  it('atomically converges concurrent reservations and preserves independent consumers after run deletion', async () => {
    const workflowName = `receipt-concurrency-${randomUUID()}`;
    const ready = await createReadyRun(workflowsA, { workflowName, runId: 'run' });
    const input = receiptInput(ready, 'finish-dispatcher');

    try {
      expect(workflowsA.getWorkflowTerminalizationCapabilities()).toEqual({
        journalVersion: 1,
        producerOutboxVersion: 1,
        destinationReceiptVersion: 1,
        parentApplicationVersion: 1,
        recoveryVersion: 1,
      });
      const results = await Promise.all([
        workflowsA.reserveWorkflowTerminalDestinationReceipt(input),
        workflowsB.reserveWorkflowTerminalDestinationReceipt(input),
      ]);
      expect(results.map(result => result.status).sort()).toEqual(['already_exists', 'reserved']);
      const receipts = results.flatMap(result =>
        result.status === 'reserved' || result.status === 'already_exists' ? [result.receipt] : [],
      );
      expect(receipts).toHaveLength(2);
      expect(receipts[1]).toEqual(receipts[0]);
      const independent = await workflowsA.reserveWorkflowTerminalDestinationReceipt({
        ...input,
        consumerId: 'audit-projection',
      });
      expect(independent).toMatchObject({ status: 'reserved', receipt: { consumerId: 'audit-projection' } });
      if (independent.status !== 'reserved') throw new Error('Expected independent receipt');
      expect(independent.receipt.receiptKey).not.toBe(receipts[0]?.receiptKey);

      await workflowsA.deleteWorkflowRunById(ready.run);
      await expect(workflowsB.getWorkflowTerminalDestinationReceipt(input)).resolves.toEqual({
        status: 'found',
        receipt: receipts[0],
      });
    } finally {
      await cleanup(workflowName);
    }
  });

  it('atomically caps concurrent distinct consumers per effect', async () => {
    const workflowName = `receipt-boundary-${randomUUID()}`;
    const ready = await createReadyRun(workflowsA, { workflowName, runId: 'run' });

    try {
      for (let index = 0; index < MAX_WORKFLOW_TERMINAL_DESTINATION_RECEIPTS_PER_EFFECT - 1; index += 1) {
        await expect(
          workflowsA.reserveWorkflowTerminalDestinationReceipt(receiptInput(ready, `consumer-${index}`)),
        ).resolves.toMatchObject({ status: 'reserved' });
      }

      const boundary = await Promise.all([
        workflowsA.reserveWorkflowTerminalDestinationReceipt(receiptInput(ready, 'boundary-a')),
        workflowsB.reserveWorkflowTerminalDestinationReceipt(receiptInput(ready, 'boundary-b')),
      ]);
      expect(boundary.map(result => result.status).sort()).toEqual(['consumer_limit_reached', 'reserved']);
      const count = await pool.query<{ count: string }>(
        `SELECT count(*)::text AS count FROM mastra_workflow_terminal_destination_receipts_v2
         WHERE workflow_name = $1 AND run_id = $2`,
        [ready.run.workflowName, ready.run.runId],
      );
      expect(Number(count.rows[0]?.count)).toBe(MAX_WORKFLOW_TERMINAL_DESTINATION_RECEIPTS_PER_EFFECT);

      const winner = boundary.find(result => result.status === 'reserved');
      if (!winner || winner.status !== 'reserved') throw new Error('Expected one boundary reservation');
      await expect(
        workflowsB.reserveWorkflowTerminalDestinationReceipt(receiptInput(ready, winner.receipt.consumerId)),
      ).resolves.toMatchObject({ status: 'already_exists', receipt: { receiptKey: winner.receipt.receiptKey } });
      await expect(
        workflowsA.reserveWorkflowTerminalDestinationReceipt(receiptInput(ready, 'over-limit')),
      ).resolves.toEqual({ status: 'consumer_limit_reached' });
    } finally {
      await cleanup(workflowName);
    }
  });

  it('materializes every reserve and get operation field once before locking', async () => {
    const intendedName = `receipt-envelope-${randomUUID()}`;
    const alternateName = `receipt-envelope-alt-${randomUUID()}`;
    const intended = await createReadyRun(workflowsA, { workflowName: intendedName, runId: 'intended' });
    const alternate = await createReadyRun(workflowsA, { workflowName: alternateName, runId: 'alternate' });
    const intendedInput = receiptInput(intended, 'intended-consumer');
    const alternateInput = receiptInput(alternate, 'alternate-consumer');

    try {
      const reserve = withStatefulEnvelope({ ...intendedInput }, alternateInput);
      await expect(workflowsA.reserveWorkflowTerminalDestinationReceipt(reserve.operation)).resolves.toMatchObject({
        status: 'reserved',
        receipt: { workflowName: intendedName, runId: 'intended', consumerId: 'intended-consumer' },
      });
      expect(reserve.reads()).toEqual({
        workflowName: 1,
        runId: 1,
        ownerId: 1,
        claimToken: 1,
        claimGeneration: 1,
        effectKind: 1,
        consumerId: 1,
      });
      const get = withStatefulEnvelope({ ...intendedInput }, alternateInput);
      await expect(workflowsA.getWorkflowTerminalDestinationReceipt(get.operation)).resolves.toMatchObject({
        status: 'found',
        receipt: { workflowName: intendedName, runId: 'intended', consumerId: 'intended-consumer' },
      });
      expect(get.reads()).toEqual({
        workflowName: 1,
        runId: 1,
        ownerId: 1,
        claimToken: 1,
        claimGeneration: 1,
        effectKind: 1,
        consumerId: 1,
      });
    } finally {
      await cleanup(intendedName);
      await cleanup(alternateName);
    }
  });

  it('validates operation fields before looking up a missing run', async () => {
    const missing = {
      workflowName: `receipt-missing-${randomUUID()}`,
      runId: 'missing',
      ownerId: 'owner',
      claimToken: 'token',
      claimGeneration: 1,
      effectKind: 'workflow-finish' as const,
      consumerId: 'consumer',
    };
    await expect(
      workflowsA.reserveWorkflowTerminalDestinationReceipt({ ...missing, effectKind: 'invalid' as never }),
    ).rejects.toThrow('kind must be parent-workflow-step-end or workflow-finish');
    await expect(workflowsA.reserveWorkflowTerminalDestinationReceipt({ ...missing, consumerId: '' })).rejects.toThrow(
      'consumerId must be a well-formed non-empty string',
    );
    await expect(workflowsA.getWorkflowTerminalDestinationReceipt({ ...missing, claimToken: '' })).rejects.toThrow(
      'claimToken must be a well-formed non-empty string',
    );
    await expect(
      workflowsA.reserveWorkflowTerminalDestinationReceipt({
        ...missing,
        workflowName: 'w'.repeat(513),
      }),
    ).rejects.toThrow('workflowName must be a well-formed non-empty string no longer than 512 characters');
    await expect(
      workflowsA.getWorkflowTerminalDestinationReceipt({
        ...missing,
        runId: `run${String.fromCharCode(0xd800)}`,
      }),
    ).rejects.toThrow('runId must be a well-formed non-empty string no longer than 512 characters');
  });

  it('fails closed on corrupt effect and receipt evidence and detects dual logical rows', async () => {
    const workflowName = `receipt-corrupt-${randomUUID()}`;
    const ready = await createReadyRun(workflowsA, { workflowName, runId: 'run' });
    const input = receiptInput(ready);
    const reserved = await workflowsA.reserveWorkflowTerminalDestinationReceipt(input);
    if (reserved.status !== 'reserved') throw new Error('Expected reserved receipt');
    const stale = { ...input, claimToken: 'stale-token' };

    try {
      const retained = await pool.query<{ envelope_hash: string; envelope: unknown }>(
        `SELECT envelope_hash, envelope FROM mastra_workflow_terminal_snapshots_v2
         WHERE workflow_name = $1 AND run_id = $2`,
        [workflowName, ready.run.runId],
      );
      const currentSnapshot = await workflowsA.loadWorkflowSnapshot(ready.run);
      if (!currentSnapshot) throw new Error('Expected retained workflow snapshot');
      const replacementEnvelope = createTerminalRecoveryEnvelope({
        ...ready.run,
        snapshot: currentSnapshot,
        terminalStatus: 'failed',
        terminalResult: {
          status: 'failed',
          error: { name: 'Error', message: 'replacement terminal result' },
        },
      });
      await pool.query(
        `UPDATE mastra_workflow_terminal_snapshots_v2
         SET envelope_hash = $1, envelope = $2
         WHERE workflow_name = $3 AND run_id = $4`,
        [
          getWorkflowTerminalRecoveryEnvelopeHash(replacementEnvelope),
          JSON.stringify(replacementEnvelope),
          workflowName,
          ready.run.runId,
        ],
      );
      await refreshTerminalSnapshotRecordHash(workflowName, ready.run.runId);
      await expect(workflowsA.getWorkflowTerminalDestinationReceipt(input)).rejects.toThrow(
        'Invalid workflow terminal effect recovery link',
      );
      await pool.query(
        `UPDATE mastra_workflow_terminal_snapshots_v2
         SET envelope_hash = $1, envelope = $2
         WHERE workflow_name = $3 AND run_id = $4`,
        [retained.rows[0]!.envelope_hash, JSON.stringify(retained.rows[0]!.envelope), workflowName, ready.run.runId],
      );
      await refreshTerminalSnapshotRecordHash(workflowName, ready.run.runId);

      await pool.query(
        `UPDATE mastra_workflow_terminal_effects_v2 SET payload_hash = $1
         WHERE workflow_name = $2 AND run_id = $3 AND effect_kind = $4`,
        [`sha256:${'0'.repeat(64)}`, workflowName, ready.run.runId, ready.effect.kind],
      );
      await expect(workflowsA.getWorkflowTerminalDestinationReceipt(stale)).rejects.toThrow(
        'Invalid workflow terminal effect integrity',
      );
      await pool.query(
        `UPDATE mastra_workflow_terminal_effects_v2 SET payload_hash = $1
         WHERE workflow_name = $2 AND run_id = $3 AND effect_kind = $4`,
        [ready.effect.payloadHash, workflowName, ready.run.runId, ready.effect.kind],
      );

      await pool.query(
        `UPDATE mastra_workflow_terminal_destination_receipts_v2 SET destination_hash = $1
         WHERE receipt_key = $2`,
        [`sha256:${'0'.repeat(64)}`, reserved.receipt.receiptKey],
      );
      await expect(workflowsA.getWorkflowTerminalDestinationReceipt(stale)).rejects.toThrow(
        'Invalid workflow terminal destination receipt integrity',
      );
      await pool.query(
        `UPDATE mastra_workflow_terminal_destination_receipts_v2 SET destination_hash = $1
         WHERE receipt_key = $2`,
        [reserved.receipt.destinationHash, reserved.receipt.receiptKey],
      );
      await pool.query(
        `UPDATE mastra_workflow_terminal_destination_receipts_v2
         SET receipt_key = $1, workflow_name = $2, run_id = $3, effect_kind = $4
         WHERE effect_key = $5 AND consumer_id = $6`,
        [
          `wtr:v1:${'0'.repeat(64)}`,
          'corrupt-workflow-name',
          'corrupt-run-id',
          'corrupt-kind',
          reserved.receipt.effectKey,
          reserved.receipt.consumerId,
        ],
      );
      await expect(workflowsA.getWorkflowTerminalDestinationReceipt(stale)).rejects.toThrow(
        'Invalid workflow terminal destination receipt integrity',
      );
      await pool.query(
        `UPDATE mastra_workflow_terminal_destination_receipts_v2
         SET receipt_key = $1, workflow_name = $2, run_id = $3, effect_kind = $4
         WHERE effect_key = $5 AND consumer_id = $6`,
        [
          reserved.receipt.receiptKey,
          reserved.receipt.workflowName,
          reserved.receipt.runId,
          reserved.receipt.effectKind,
          reserved.receipt.effectKey,
          reserved.receipt.consumerId,
        ],
      );
      await pool.query(
        `INSERT INTO mastra_workflow_terminal_effects_v2
         SELECT workflow_name, run_id || '-duplicate-effect', effect_kind, version,
                effect_key || '-duplicate', source_event_key, terminal_status,
                parent_workflow_name, parent_run_id, parent_step_id, parent_execution_path,
                recovery_envelope_hash, retained_record_hash, resource_id, payload_hash, created_at
         FROM mastra_workflow_terminal_effects_v2 WHERE effect_key = $1`,
        [reserved.receipt.effectKey],
      );
      await pool.query(
        `INSERT INTO mastra_workflow_terminal_destination_receipts_v2
         SELECT version, workflow_name, run_id, effect_key || '-duplicate', consumer_id,
                receipt_key || '-duplicate', effect_kind, producer_payload_hash, destination_hash,
                application_state, dispatch_state, created_at, updated_at, applied_at,
                dispatch_pending_at, destination_applied_at, quarantined_at
         FROM mastra_workflow_terminal_destination_receipts_v2 WHERE receipt_key = $1`,
        [reserved.receipt.receiptKey],
      );
      await expect(workflowsA.getWorkflowTerminalDestinationReceipt(input)).rejects.toThrow(
        'Conflicting workflow terminal destination receipt storage',
      );
    } finally {
      await cleanup(workflowName);
    }
  });

  it('requires retained terminal state and binds parent destination paths', async () => {
    const workflowName = `receipt-parent-${randomUUID()}`;
    const first = await createReadyRun(
      workflowsA,
      { workflowName, runId: 'first' },
      {
        kind: 'parent-workflow-step-end',
        parentWorkflowName: workflowName,
        parentRunId: 'parent-run',
        parentStepId: 'nested',
        parentExecutionPath: [1, 23],
      },
    );
    const second = await createReadyRun(
      workflowsA,
      { workflowName, runId: 'second' },
      {
        kind: 'parent-workflow-step-end',
        parentWorkflowName: workflowName,
        parentRunId: 'parent-run',
        parentStepId: 'nested',
        parentExecutionPath: [12, 3],
      },
    );
    const missing = await createReadyRun(workflowsA, { workflowName, runId: 'missing' });

    try {
      await pool.query(`DELETE FROM mastra_workflow_terminal_snapshots_v2 WHERE workflow_name = $1 AND run_id = $2`, [
        workflowName,
        missing.run.runId,
      ]);
      await expect(
        workflowsA.reserveWorkflowTerminalDestinationReceipt(receiptInput(missing, 'finish-dispatcher')),
      ).resolves.toEqual({ status: 'missing_terminal_state' });
      const count = await pool.query<{ count: string }>(
        `SELECT count(*)::text AS count FROM mastra_workflow_terminal_destination_receipts_v2
         WHERE workflow_name = $1 AND run_id = $2`,
        [workflowName, missing.run.runId],
      );
      expect(count.rows[0]?.count).toBe('0');

      const firstReceipt = await workflowsA.reserveWorkflowTerminalDestinationReceipt(
        receiptInput(first, 'parent-application'),
      );
      const secondReceipt = await workflowsA.reserveWorkflowTerminalDestinationReceipt(
        receiptInput(second, 'parent-application'),
      );
      if (firstReceipt.status !== 'reserved' || secondReceipt.status !== 'reserved') {
        throw new Error('Expected parent receipts');
      }
      expect(secondReceipt.receipt.destinationHash).toMatch(/^sha256:[a-f0-9]{64}$/);
      expect(secondReceipt.receipt.destinationHash).not.toBe(firstReceipt.receipt.destinationHash);
    } finally {
      await cleanup(workflowName);
    }
  });

  it('rolls back receipt cleanup with the journal and all producer evidence', async () => {
    const workflowName = `receipt-cleanup-${randomUUID()}`;
    const ready = await createReadyRun(workflowsA, { workflowName, runId: 'run' });
    await workflowsA.reserveWorkflowTerminalDestinationReceipt(receiptInput(ready));
    await pool.query(
      `UPDATE mastra_workflow_terminalizations
       SET phase = 'complete', owner_id = NULL, claim_token = NULL, lease_expires_at = NULL,
           updated_at = floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint,
           completed_at = floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint
       WHERE workflow_name = $1 AND run_id = $2`,
      [workflowName, ready.run.runId],
    );
    const suffix = randomUUID().replaceAll('-', '');
    const functionName = `pf1770_cleanup_fail_${suffix}`;
    const triggerName = `pf1770_cleanup_trigger_${suffix}`;

    try {
      await pool.query(
        `CREATE FUNCTION ${functionName}() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN RAISE EXCEPTION 'PF1770 cleanup failure'; END $$`,
      );
      await pool.query(
        `CREATE TRIGGER ${triggerName} BEFORE DELETE ON mastra_workflow_terminal_destination_receipts_v2
         FOR EACH ROW EXECUTE FUNCTION ${functionName}()`,
      );
      await expect(
        workflowsA.deleteCompletedWorkflowTerminalizations({
          ...ready.run,
          olderThan: new Date(Date.now() + 60_000),
        }),
      ).rejects.toThrow('PF1770 cleanup failure');
      for (const table of [
        'mastra_workflow_terminalizations',
        'mastra_workflow_terminal_effects_v2',
        'mastra_workflow_terminal_snapshots_v2',
        'mastra_workflow_terminal_destination_receipts_v2',
      ]) {
        const count = await pool.query<{ count: string }>(
          `SELECT count(*)::text AS count FROM ${table} WHERE workflow_name = $1 AND run_id = $2`,
          [workflowName, ready.run.runId],
        );
        expect(count.rows[0]?.count).toBe('1');
      }
      const ancestryCount = await pool.query<{ count: string }>(
        `SELECT count(*)::text AS count FROM mastra_workflow_terminal_recovery_ancestries
         WHERE workflow_name = $1 AND run_id = $2`,
        [workflowName, ready.run.runId],
      );
      expect(ancestryCount.rows[0]?.count).toBe('0');
      await pool.query(`DROP TRIGGER ${triggerName} ON mastra_workflow_terminal_destination_receipts_v2`);
      await pool.query(`DROP FUNCTION ${functionName}()`);
      await expect(
        workflowsA.deleteCompletedWorkflowTerminalizations({
          ...ready.run,
          olderThan: new Date(Date.now() + 60_000),
        }),
      ).resolves.toEqual({ status: 'deleted', count: 1 });
    } finally {
      await pool.query(`DROP TRIGGER IF EXISTS ${triggerName} ON mastra_workflow_terminal_destination_receipts_v2`);
      await pool.query(`DROP FUNCTION IF EXISTS ${functionName}()`);
      await cleanup(workflowName);
    }
  });

  it('exports final receipt DDL and clears all terminal tables in one custom-schema reset', async () => {
    const schemaName = `receipt_${randomUUID().replaceAll('-', '').slice(0, 12)}`;
    await pool.query(`CREATE SCHEMA "${schemaName}"`);
    const workflows = new WorkflowsPG({ pool, schemaName });
    try {
      await workflows.init();
      const ready = await createReadyRun(workflows, { workflowName: 'custom-workflow', runId: 'custom-run' });
      await workflows.reserveWorkflowTerminalDestinationReceipt(receiptInput(ready));
      const ddl = WorkflowsPG.getExportDDL(schemaName).join('\n');
      expect(ddl).toContain(`"${schemaName}"."mastra_workflow_terminal_destination_receipts_v2"`);
      expect(ddl).toContain(`"${schemaName}"."mastra_workflow_terminal_recovery_ancestries"`);
      expect(ddl).toContain('"mastra_workflow_terminal_destination_receipts_v2_lookup_idx"');
      expect(ddl).not.toContain(`"${schemaName}_mastra_workflow_terminal_destination_receipts_v2_lookup_idx"`);
      expect(WorkflowsPG.prototype.init.toString()).not.toContain('ADD COLUMN IF NOT EXISTS "parent_execution_path"');
      const indexes = await pool.query<{ indexname: string }>(
        `SELECT indexname FROM pg_indexes WHERE schemaname = $1
         AND tablename = 'mastra_workflow_terminal_destination_receipts_v2'`,
        [schemaName],
      );
      expect(indexes.rows.map(row => row.indexname)).toContain(
        'mastra_workflow_terminal_destination_receipts_v2_lookup_idx',
      );

      await workflows.dangerouslyClearAll();
      for (const table of [
        'mastra_workflow_snapshot',
        'mastra_workflow_terminalizations',
        'mastra_workflow_terminal_effects_v2',
        'mastra_workflow_terminal_snapshots_v2',
        'mastra_workflow_terminal_recovery_ancestries',
        'mastra_workflow_terminal_destination_receipts_v2',
      ]) {
        const count = await pool.query<{ count: string }>(
          `SELECT count(*)::text AS count FROM "${schemaName}"."${table}"`,
        );
        expect(count.rows[0]?.count).toBe('0');
      }
    } finally {
      await pool.query(`DROP SCHEMA "${schemaName}" CASCADE`);
    }
  });

  it('upgrades a PF-1779 receipt topology without binding PF-1782 writes to its unversioned effects', async () => {
    const schemaName = `receipt_upgrade_${randomUUID().replaceAll('-', '').slice(0, 8)}`;
    const schema = `"${schemaName}"`;
    await pool.query(`CREATE SCHEMA ${schema}`);
    try {
      await pool.query(`
        CREATE TABLE ${schema}."mastra_workflow_terminal_effects" (
          "effect_key" TEXT PRIMARY KEY
        );
        CREATE TABLE ${schema}."mastra_workflow_terminal_destination_receipts" (
          "effect_key" TEXT NOT NULL,
          "consumer_id" TEXT NOT NULL,
          "receipt_key" TEXT NOT NULL UNIQUE,
          PRIMARY KEY ("effect_key", "consumer_id"),
          FOREIGN KEY ("effect_key")
            REFERENCES ${schema}."mastra_workflow_terminal_effects" ("effect_key") ON DELETE CASCADE
        );
        CREATE TABLE ${schema}."mastra_workflow_terminal_continuation_plans" (
          "receipt_key" TEXT PRIMARY KEY,
          "effect_key" TEXT NOT NULL,
          "consumer_id" TEXT NOT NULL,
          FOREIGN KEY ("effect_key")
            REFERENCES ${schema}."mastra_workflow_terminal_effects" ("effect_key") ON DELETE CASCADE,
          FOREIGN KEY ("effect_key", "consumer_id")
            REFERENCES ${schema}."mastra_workflow_terminal_destination_receipts" ("effect_key", "consumer_id")
              ON DELETE CASCADE,
          FOREIGN KEY ("receipt_key")
            REFERENCES ${schema}."mastra_workflow_terminal_destination_receipts" ("receipt_key") ON DELETE CASCADE
        );
        INSERT INTO ${schema}."mastra_workflow_terminal_effects" ("effect_key") VALUES ('old-effect');
        INSERT INTO ${schema}."mastra_workflow_terminal_destination_receipts"
          ("effect_key", "consumer_id", "receipt_key")
          VALUES ('old-effect', 'old-consumer', 'old-receipt');
      `);

      const workflows = new WorkflowsPG({ pool, schemaName });
      await workflows.init();
      const receiptForeignKeys = await pool.query<{ referenced_table: string }>(
        `SELECT referenced.relname AS referenced_table
         FROM pg_constraint AS constraint_row
         JOIN pg_class AS receipt ON receipt.oid = constraint_row.conrelid
         JOIN pg_namespace AS receipt_namespace ON receipt_namespace.oid = receipt.relnamespace
         JOIN pg_class AS referenced ON referenced.oid = constraint_row.confrelid
         WHERE constraint_row.contype = 'f'
           AND receipt_namespace.nspname = $1
           AND receipt.relname = 'mastra_workflow_terminal_destination_receipts_v2'`,
        [schemaName],
      );
      expect(receiptForeignKeys.rows).toEqual([{ referenced_table: 'mastra_workflow_terminal_effects_v2' }]);
      const ready = await createReadyRun(workflows, { workflowName: 'upgraded-workflow', runId: 'upgraded-run' });
      await expect(
        workflows.reserveWorkflowTerminalDestinationReceipt(receiptInput(ready, 'upgraded-consumer')),
      ).resolves.toMatchObject({ status: 'reserved' });

      const oldCount = await pool.query<{ count: string }>(
        `SELECT count(*)::text AS count
         FROM ${schema}."mastra_workflow_terminal_destination_receipts"`,
      );
      const upgradedCount = await pool.query<{ count: string }>(
        `SELECT count(*)::text AS count
         FROM ${schema}."mastra_workflow_terminal_destination_receipts_v2"
         WHERE workflow_name = 'upgraded-workflow' AND run_id = 'upgraded-run'`,
      );
      expect(oldCount.rows[0]?.count).toBe('1');
      expect(upgradedCount.rows[0]?.count).toBe('1');
    } finally {
      await pool.query(`DROP SCHEMA ${schema} CASCADE`);
    }
  });
});
