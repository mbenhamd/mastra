import { randomUUID } from 'node:crypto';
import {
  InMemoryDB,
  WorkflowsInMemory,
  createEmptyWorkflowSnapshot,
  createWorkflowTerminalGraphFingerprint,
  createWorkflowTerminalParentContinuationContract,
} from '@mastra/core/storage';
import type { WorkflowRunState, WorkflowTerminalRecoveryAncestryV1 } from '@mastra/core/workflows';
import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { createTerminalRecoveryEnvelope } from './terminalization-test-utils';
import { WorkflowsPG } from '.';

describe('WorkflowsPG terminalization journal', () => {
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
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(`DELETE FROM mastra_workflow_terminal_continuation_plans_v2 WHERE workflow_name = $1`, [
        workflowName,
      ]);
      await client.query(`DELETE FROM mastra_workflow_terminal_destination_receipts_v2 WHERE workflow_name = $1`, [
        workflowName,
      ]);
      await client.query(`DELETE FROM mastra_workflow_terminal_effects_v2 WHERE workflow_name = $1`, [workflowName]);
      await client.query(`DELETE FROM mastra_workflow_terminal_snapshots_v2 WHERE workflow_name = $1`, [workflowName]);
      await client.query(`DELETE FROM mastra_workflow_terminal_recovery_ancestries WHERE workflow_name = $1`, [
        workflowName,
      ]);
      await client.query(`DELETE FROM mastra_workflow_terminalizations WHERE workflow_name = $1`, [workflowName]);
      await client.query(`DELETE FROM mastra_workflow_snapshot WHERE workflow_name = $1`, [workflowName]);
      await client.query(`DELETE FROM mastra_workflow_parent_revisions WHERE workflow_name = $1`, [workflowName]);
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  function withStatefulIdentity<T extends { workflowName: string; runId: string }>(
    operation: T,
    alternate: { workflowName: string; runId: string },
  ): { operation: T; reads: () => { workflowName: number; runId: number } } {
    const intended = { workflowName: operation.workflowName, runId: operation.runId };
    let workflowNameReads = 0;
    let runIdReads = 0;
    Object.defineProperties(operation, {
      workflowName: {
        enumerable: true,
        get: () => (workflowNameReads++ === 0 ? intended.workflowName : alternate.workflowName),
      },
      runId: {
        enumerable: true,
        get: () => (runIdReads++ === 0 ? intended.runId : alternate.runId),
      },
    });
    return { operation, reads: () => ({ workflowName: workflowNameReads, runId: runIdReads }) };
  }

  it('validates every parent-application run identity before storage lookup', async () => {
    const missing = {
      workflowName: `parent-identity-${randomUUID()}`,
      runId: 'missing',
      ownerId: 'owner',
      claimToken: 'token',
      claimGeneration: 1,
    };
    const contract = createWorkflowTerminalParentContinuationContract({
      version: 1,
      terminalEffectKey: 'effect',
      terminalEffectPayloadHash: `sha256:${'0'.repeat(64)}`,
      executionMode: 'continuous',
      expectedParentRevision: 'pg:v1:1',
      graphFingerprint: createWorkflowTerminalGraphFingerprint([]),
      childTerminalStatus: 'success',
      observedParentStatus: 'success',
      source: { kind: 'step', stepId: 'step', executionPath: [0] },
      action: { kind: 'noop', reason: 'already-terminal' },
      patch: { kind: 'none' },
    });
    const invalidIdentities = [
      [
        { workflowName: 'w'.repeat(513) },
        'workflowName must be a well-formed non-empty string no longer than 512 characters',
      ],
      [
        { runId: `run${String.fromCharCode(0xd800)}` },
        'runId must be a well-formed non-empty string no longer than 512 characters',
      ],
    ] as const;

    for (const [identity, message] of invalidIdentities) {
      const operation = { ...missing, ...identity };
      await expect(workflowsA.getWorkflowTerminalParentContext(operation)).rejects.toThrow(message);
      await expect(workflowsA.getWorkflowTerminalContinuationPlan(operation)).rejects.toThrow(message);
      await expect(workflowsA.applyWorkflowTerminalParentEffect({ ...operation, contract })).rejects.toThrow(message);
    }
  });

  function recoveryEnvelope(
    snapshot: WorkflowRunState,
    identity: { workflowName: string; runId: string },
    terminalStatus: 'success' | 'failed' | 'canceled',
    ancestry: WorkflowTerminalRecoveryAncestryV1 = [],
    terminalResult?: unknown,
  ) {
    return createTerminalRecoveryEnvelope({
      ...identity,
      snapshot,
      terminalStatus,
      ancestry,
      terminalResult,
    });
  }

  function nestedAncestry({
    child,
    parent,
    parentSnapshot,
    source,
  }: {
    child: { workflowName: string; runId: string };
    parent: { workflowName: string; runId: string };
    parentSnapshot: WorkflowRunState;
    source:
      | { kind: 'step'; stepId: string; executionPath: number[] }
      | { kind: 'foreach-iteration'; stepId: string; containerPath: number[]; iterationIndex: number };
  }): WorkflowTerminalRecoveryAncestryV1 {
    return [
      {
        version: 1,
        childWorkflowName: child.workflowName,
        childRunId: child.runId,
        parentWorkflowName: parent.workflowName,
        parentRunId: parent.runId,
        parentGraphFingerprint: createWorkflowTerminalGraphFingerprint(parentSnapshot.serializedStepGraph),
        source,
        inputPointer: { kind: 'parent-source-payload', stepId: source.stepId },
        resultPointer: { kind: 'retained-terminal-result', workflowName: child.workflowName, runId: child.runId },
        resumeMetadata: { wasResume: false, resumeSteps: [] },
      },
    ];
  }

  it('fences a stale child plan across adapters when the parent terminal claim wins', async () => {
    const suffix = randomUUID();
    const child = { workflowName: `claim-race-child-${suffix}`, runId: 'child-run' };
    const parent = { workflowName: `claim-race-parent-${suffix}`, runId: 'parent-run' };
    const now = Date.now();
    const parentSnapshot: WorkflowRunState = {
      ...createEmptyWorkflowSnapshot(parent.runId),
      status: 'running',
      context: {
        nested: {
          status: 'running',
          payload: {},
          startedAt: now - 10,
          metadata: { nestedRunId: child.runId },
        },
      } as WorkflowRunState['context'],
      serializedStepGraph: [
        { type: 'step', step: { id: 'nested', component: 'WORKFLOW' } },
        { type: 'sleep', id: 'after-child', duration: 1 },
      ],
      activePaths: [0],
      activeStepsPath: { nested: [0] },
      timestamp: now - 10,
    };
    await workflowsA.persistWorkflowSnapshot({ ...parent, snapshot: parentSnapshot });
    await workflowsA.persistWorkflowSnapshot({ ...child, snapshot: createEmptyWorkflowSnapshot(child.runId) });
    try {
      const childClaim = await workflowsA.claimWorkflowTerminalization({
        ...child,
        eventKey: 'child-terminal',
        terminalStatus: 'success',
        ownerId: 'child-owner',
        leaseMs: 10_000,
      });
      if (childClaim.status !== 'acquired') throw new Error(`Expected acquired, received ${childClaim.status}`);
      const fence = {
        ...child,
        ownerId: childClaim.record.ownerId,
        claimToken: childClaim.record.claimToken,
        claimGeneration: childClaim.record.claimGeneration,
      };
      const childSnapshot: WorkflowRunState = {
        ...createEmptyWorkflowSnapshot(child.runId),
        status: 'success',
        result: { status: 'success', startedAt: now - 5, endedAt: now },
        timestamp: now,
      };
      const ancestry = nestedAncestry({
        child,
        parent,
        parentSnapshot,
        source: { kind: 'step', stepId: 'nested', executionPath: [0] },
      });
      await workflowsA.persistWorkflowTerminalRecoveryAncestry({ ...child, ancestry });
      await expect(
        workflowsA.persistWorkflowTerminalState({
          ...fence,
          snapshot: childSnapshot,
          recoveryEnvelope: recoveryEnvelope(childSnapshot, child, 'success', ancestry, childSnapshot.result),
        }),
      ).resolves.toMatchObject({ status: 'persisted' });
      const prepared = await workflowsA.prepareWorkflowTerminalEffect({
        ...fence,
        expectedPhase: 'run_state_persisted',
        effect: {
          kind: 'parent-workflow-step-end',
          parentWorkflowName: parent.workflowName,
          parentRunId: parent.runId,
          parentStepId: 'nested',
          parentExecutionPath: [0],
        },
      });
      if (prepared.status !== 'prepared' || prepared.effect.kind !== 'parent-workflow-step-end') {
        throw new Error('Expected parent effect');
      }
      const context = await workflowsA.getWorkflowTerminalParentContext(fence);
      if (context.status !== 'found') throw new Error(`Expected found, received ${context.status}`);
      const contract = createWorkflowTerminalParentContinuationContract({
        version: 1,
        terminalEffectKey: prepared.effect.effectKey,
        terminalEffectPayloadHash: prepared.effect.payloadHash,
        executionMode: 'continuous',
        expectedParentRevision: context.revision,
        graphFingerprint: createWorkflowTerminalGraphFingerprint(context.snapshot.serializedStepGraph),
        childTerminalStatus: 'success',
        observedParentStatus: 'running',
        source: { kind: 'step', stepId: 'nested', executionPath: [0] },
        action: {
          kind: 'run-entry',
          reason: 'next-step',
          target: { kind: 'entry', entryType: 'sleep', entryId: 'after-child', executionPath: [1] },
        },
        patch: {
          kind: 'merge-child-terminal',
          resultWrite: 'source-coordinate',
          resultSource: 'retained-child-terminal-envelope',
          payloadWrite: 'preserve-parent-step-payload',
          metadataWrite: 'merge-child-and-bind-nested-run-id',
          stateWrite: 'replace-context-__state-from-retained-child',
          requestContextWrite: 'merge-from-retained-child',
          activeStepsWrite: 'derive-from-source-coordinate',
          snapshotTimestampWrite: 'storage-clock',
          parentRunWrite: { kind: 'preserve' },
          loopWrite: { kind: 'preserve' },
        },
      });
      const before = await workflowsA.loadWorkflowSnapshot(parent);
      const beforeRevision = await pool.query<{ generation: string }>(
        `SELECT generation FROM mastra_workflow_parent_revisions WHERE workflow_name = $1 AND run_id = $2`,
        [parent.workflowName, parent.runId],
      );
      const parentClaim = await workflowsB.claimWorkflowTerminalization({
        ...parent,
        eventKey: 'parent-terminal',
        terminalStatus: 'failed',
        ownerId: 'parent-owner',
        leaseMs: 10_000,
      });
      if (parentClaim.status !== 'acquired') throw new Error(`Expected acquired, received ${parentClaim.status}`);
      const afterClaimRevision = await pool.query<{ generation: string }>(
        `SELECT generation FROM mastra_workflow_parent_revisions WHERE workflow_name = $1 AND run_id = $2`,
        [parent.workflowName, parent.runId],
      );
      expect(Number(afterClaimRevision.rows[0]!.generation)).toBe(Number(beforeRevision.rows[0]!.generation) + 1);

      await expect(workflowsA.getWorkflowTerminalParentContext(fence)).resolves.toEqual({ status: 'parent_conflict' });
      await expect(workflowsA.applyWorkflowTerminalParentEffect({ ...fence, contract })).resolves.toEqual({
        status: 'parent_conflict',
      });
      await expect(workflowsA.loadWorkflowSnapshot(parent)).resolves.toEqual(before);

      await expect(
        workflowsB.claimWorkflowTerminalization({
          ...parent,
          eventKey: 'parent-terminal',
          terminalStatus: 'failed',
          ownerId: parentClaim.record.ownerId,
          claimToken: parentClaim.record.claimToken,
          claimGeneration: parentClaim.record.claimGeneration,
          leaseMs: 10_000,
        }),
      ).resolves.toMatchObject({ status: 'renewed' });
      const afterRenewRevision = await pool.query<{ generation: string }>(
        `SELECT generation FROM mastra_workflow_parent_revisions WHERE workflow_name = $1 AND run_id = $2`,
        [parent.workflowName, parent.runId],
      );
      expect(afterRenewRevision.rows[0]!.generation).toBe(afterClaimRevision.rows[0]!.generation);
    } finally {
      await cleanup(child.workflowName);
      await cleanup(parent.workflowName);
    }
  });

  it('atomically applies and recovers the exact graph-bound parent contract', async () => {
    const suffix = randomUUID();
    const child = { workflowName: `parent-apply-child-${suffix}`, runId: 'child-run' };
    const parent = { workflowName: `parent-apply-parent-${suffix}`, runId: 'parent-run' };
    const now = Date.now();
    const parentSnapshot: WorkflowRunState = {
      ...createEmptyWorkflowSnapshot(parent.runId),
      status: 'running',
      value: { stale: true },
      context: {
        nested: {
          status: 'running',
          payload: { paperId: 'p1' },
          startedAt: now - 20,
          metadata: { nestedRunId: child.runId },
        },
        __state: { stale: true },
      } as WorkflowRunState['context'],
      serializedStepGraph: [
        { type: 'step', step: { id: 'nested', component: 'WORKFLOW' } },
        { type: 'sleep', id: 'after-child', duration: 10 },
      ],
      activePaths: [0],
      activeStepsPath: { nested: [0] },
      requestContext: { parent: true },
      timestamp: now - 20,
    };
    await workflowsA.persistWorkflowSnapshot({ ...parent, snapshot: parentSnapshot });
    await workflowsA.persistWorkflowSnapshot({ ...child, snapshot: createEmptyWorkflowSnapshot(child.runId) });
    try {
      const claimed = await workflowsA.claimWorkflowTerminalization({
        ...child,
        eventKey: 'terminal-event',
        terminalStatus: 'success',
        ownerId: 'worker',
        leaseMs: 10_000,
      });
      if (claimed.status !== 'acquired') throw new Error('Expected child claim');
      const fence = {
        ...child,
        ownerId: claimed.record.ownerId,
        claimToken: claimed.record.claimToken,
        claimGeneration: claimed.record.claimGeneration,
      };
      const childSnapshot: WorkflowRunState = {
        ...createEmptyWorkflowSnapshot(child.runId),
        status: 'success',
        result: {
          status: 'success',
          output: { answer: 42 },
          startedAt: now - 10,
          endedAt: now,
        },
        value: { final: true },
        context: { __state: { final: true } } as WorkflowRunState['context'],
        timestamp: now,
      };
      const ancestry = nestedAncestry({
        child,
        parent,
        parentSnapshot,
        source: { kind: 'step', stepId: 'nested', executionPath: [0] },
      });
      await workflowsA.persistWorkflowTerminalRecoveryAncestry({ ...child, ancestry });
      await expect(
        workflowsA.persistWorkflowTerminalState({
          ...fence,
          snapshot: childSnapshot,
          recoveryEnvelope: recoveryEnvelope(childSnapshot, child, 'success', ancestry, childSnapshot.result),
        }),
      ).resolves.toMatchObject({ status: 'persisted' });
      const prepared = await workflowsA.prepareWorkflowTerminalEffect({
        ...fence,
        expectedPhase: 'run_state_persisted',
        effect: {
          kind: 'parent-workflow-step-end',
          parentWorkflowName: parent.workflowName,
          parentRunId: parent.runId,
          parentStepId: 'nested',
          parentExecutionPath: [0],
        },
      });
      if (prepared.status !== 'prepared' || prepared.effect.kind !== 'parent-workflow-step-end') {
        throw new Error('Expected parent effect');
      }
      const context = await workflowsA.getWorkflowTerminalParentContext(fence);
      if (context.status !== 'found') throw new Error('Expected parent context');
      const alternateReceipt = await workflowsA.reserveWorkflowTerminalDestinationReceipt({
        ...fence,
        effectKind: 'parent-workflow-step-end',
        consumerId: 'pf1771.corruption-probe',
      });
      if (alternateReceipt.status !== 'reserved') throw new Error('Expected alternate corruption-probe receipt');
      const contract = createWorkflowTerminalParentContinuationContract({
        version: 1,
        terminalEffectKey: prepared.effect.effectKey,
        terminalEffectPayloadHash: prepared.effect.payloadHash,
        executionMode: 'continuous',
        expectedParentRevision: context.revision,
        graphFingerprint: createWorkflowTerminalGraphFingerprint(context.snapshot.serializedStepGraph),
        childTerminalStatus: 'success',
        observedParentStatus: 'running',
        source: { kind: 'step', stepId: 'nested', executionPath: [0] },
        action: {
          kind: 'run-entry',
          reason: 'next-step',
          target: { kind: 'entry', entryType: 'sleep', entryId: 'after-child', executionPath: [1] },
        },
        patch: {
          kind: 'merge-child-terminal',
          resultWrite: 'source-coordinate',
          resultSource: 'retained-child-terminal-envelope',
          payloadWrite: 'preserve-parent-step-payload',
          metadataWrite: 'merge-child-and-bind-nested-run-id',
          stateWrite: 'replace-context-__state-from-retained-child',
          requestContextWrite: 'merge-from-retained-child',
          activeStepsWrite: 'derive-from-source-coordinate',
          snapshotTimestampWrite: 'storage-clock',
          parentRunWrite: { kind: 'preserve' },
          loopWrite: { kind: 'preserve' },
        },
      });

      await pool.query(
        `UPDATE mastra_workflow_snapshot
         SET snapshot = jsonb_set(snapshot, '{serializedStepGraph}', '[null]'::jsonb, true)
         WHERE workflow_name = $1 AND run_id = $2`,
        [parent.workflowName, parent.runId],
      );
      await expect(workflowsA.applyWorkflowTerminalParentEffect({ ...fence, contract })).resolves.toEqual({
        status: 'corrupt_parent_state',
      });
      await pool.query(
        `UPDATE mastra_workflow_snapshot SET snapshot = $1
         WHERE workflow_name = $2 AND run_id = $3`,
        [JSON.stringify(parentSnapshot), parent.workflowName, parent.runId],
      );

      await pool.query(`DELETE FROM mastra_workflow_parent_revisions WHERE workflow_name = $1 AND run_id = $2`, [
        parent.workflowName,
        parent.runId,
      ]);
      await expect(workflowsA.getWorkflowTerminalParentContext(fence)).resolves.toEqual({
        status: 'corrupt_parent_state',
      });
      const revisionAfterContextRead = await pool.query(
        `SELECT generation FROM mastra_workflow_parent_revisions
         WHERE workflow_name = $1 AND run_id = $2`,
        [parent.workflowName, parent.runId],
      );
      expect(revisionAfterContextRead.rowCount).toBe(0);
      await pool.query(
        `INSERT INTO mastra_workflow_parent_revisions (workflow_name, run_id, generation, updated_at)
         VALUES ($1, $2, 0, $3)`,
        [parent.workflowName, parent.runId, Date.now()],
      );
      await expect(workflowsA.getWorkflowTerminalParentContext(fence)).resolves.toEqual({
        status: 'corrupt_parent_state',
      });
      const revisionAfterStaleContextRead = await pool.query<{ generation: string }>(
        `SELECT generation::text FROM mastra_workflow_parent_revisions
         WHERE workflow_name = $1 AND run_id = $2`,
        [parent.workflowName, parent.runId],
      );
      expect(revisionAfterStaleContextRead.rows).toEqual([{ generation: '0' }]);
      await expect(workflowsA.applyWorkflowTerminalParentEffect({ ...fence, contract })).resolves.toEqual({
        status: 'corrupt_parent_state',
      });
      const revisionAfterRejectedApply = await pool.query<{ generation: string }>(
        `SELECT generation::text FROM mastra_workflow_parent_revisions
         WHERE workflow_name = $1 AND run_id = $2`,
        [parent.workflowName, parent.runId],
      );
      expect(revisionAfterRejectedApply.rows).toEqual([{ generation: '0' }]);
      await pool.query(`DELETE FROM mastra_workflow_parent_revisions WHERE workflow_name = $1 AND run_id = $2`, [
        parent.workflowName,
        parent.runId,
      ]);
      await pool.query(
        `INSERT INTO mastra_workflow_parent_revisions (workflow_name, run_id, generation, updated_at)
         VALUES ($1, $2, 1, $3)`,
        [parent.workflowName, parent.runId, Date.now()],
      );

      await pool.query(
        `UPDATE mastra_workflow_terminal_snapshots_v2
         SET envelope = envelope #- '{finalState}'
         WHERE workflow_name = $1 AND run_id = $2`,
        [child.workflowName, child.runId],
      );
      await expect(workflowsA.applyWorkflowTerminalParentEffect({ ...fence, contract })).resolves.toEqual({
        status: 'corrupt_child_terminal_state',
      });
      await expect(workflowsA.getWorkflowTerminalContinuationPlan(fence)).resolves.toMatchObject({
        status: 'missing_receipt',
      });
      await pool.query(
        `UPDATE mastra_workflow_terminal_snapshots_v2
         SET envelope = jsonb_set(envelope, '{finalState}', '{"final":true}'::jsonb, true)
         WHERE workflow_name = $1 AND run_id = $2`,
        [child.workflowName, child.runId],
      );

      await pool.query(
        `UPDATE mastra_workflow_terminal_snapshots_v2
         SET envelope = jsonb_set(envelope, '{terminalResult,status}', '"failed"'::jsonb, true)
         WHERE workflow_name = $1 AND run_id = $2`,
        [child.workflowName, child.runId],
      );
      await expect(workflowsA.applyWorkflowTerminalParentEffect({ ...fence, contract })).resolves.toEqual({
        status: 'corrupt_child_terminal_state',
      });
      await expect(workflowsA.getWorkflowTerminalContinuationPlan(fence)).resolves.toMatchObject({
        status: 'missing_receipt',
      });
      await pool.query(
        `UPDATE mastra_workflow_terminal_snapshots_v2
         SET envelope = jsonb_set(envelope, '{terminalResult,status}', '"success"'::jsonb, true)
         WHERE workflow_name = $1 AND run_id = $2`,
        [child.workflowName, child.runId],
      );

      const retainedSnapshotBackup = await pool.query<{ envelope: unknown }>(
        `SELECT envelope FROM mastra_workflow_terminal_snapshots_v2
         WHERE workflow_name = $1 AND run_id = $2`,
        [child.workflowName, child.runId],
      );
      await pool.query(
        `UPDATE mastra_workflow_terminal_snapshots_v2
         SET envelope = '{}'::jsonb
         WHERE workflow_name = $1 AND run_id = $2`,
        [child.workflowName, child.runId],
      );
      await expect(workflowsA.applyWorkflowTerminalParentEffect({ ...fence, contract })).resolves.toEqual({
        status: 'corrupt_child_terminal_state',
      });
      await pool.query(
        `UPDATE mastra_workflow_terminal_snapshots_v2 SET envelope = $1
         WHERE workflow_name = $2 AND run_id = $3`,
        [JSON.stringify(retainedSnapshotBackup.rows[0]!.envelope), child.workflowName, child.runId],
      );

      await pool.query(
        `UPDATE mastra_workflow_snapshot
         SET snapshot = jsonb_set(snapshot, '{requestContext}', '[]'::jsonb, true)
         WHERE workflow_name = $1 AND run_id = $2`,
        [parent.workflowName, parent.runId],
      );
      await expect(workflowsA.applyWorkflowTerminalParentEffect({ ...fence, contract })).resolves.toEqual({
        status: 'corrupt_parent_state',
      });
      await expect(workflowsA.getWorkflowTerminalContinuationPlan(fence)).resolves.toMatchObject({
        status: 'missing_receipt',
      });
      await pool.query(
        `UPDATE mastra_workflow_snapshot SET snapshot = $1
         WHERE workflow_name = $2 AND run_id = $3`,
        [JSON.stringify(parentSnapshot), parent.workflowName, parent.runId],
      );

      const rollbackTrigger = `pf1771_plan_rollback_${suffix.replaceAll('-', '')}`;
      const parentBeforeRollback = await workflowsA.loadWorkflowSnapshot(parent);
      await pool.query(`
        CREATE FUNCTION ${rollbackTrigger}() RETURNS trigger AS $$
        BEGIN
          RAISE EXCEPTION 'PF-1771 rollback probe';
        END;
        $$ LANGUAGE plpgsql
      `);
      await pool.query(`
        CREATE TRIGGER ${rollbackTrigger}
        BEFORE INSERT ON mastra_workflow_terminal_continuation_plans_v2
        FOR EACH ROW EXECUTE FUNCTION ${rollbackTrigger}()
      `);
      try {
        await expect(workflowsA.applyWorkflowTerminalParentEffect({ ...fence, contract })).rejects.toThrow(
          'PF-1771 rollback probe',
        );
      } finally {
        await pool.query(`DROP TRIGGER IF EXISTS ${rollbackTrigger} ON mastra_workflow_terminal_continuation_plans_v2`);
        await pool.query(`DROP FUNCTION IF EXISTS ${rollbackTrigger}()`);
      }
      await expect(workflowsA.loadWorkflowSnapshot(parent)).resolves.toEqual(parentBeforeRollback);
      const contextAfterRollback = await workflowsA.getWorkflowTerminalParentContext(fence);
      expect(contextAfterRollback).toMatchObject({ status: 'found', revision: context.revision });
      await expect(workflowsA.getWorkflowTerminalContinuationPlan(fence)).resolves.toMatchObject({
        status: 'missing_receipt',
      });
      await expect(workflowsA.getWorkflowTerminalization(fence)).resolves.toMatchObject({
        status: 'found',
        record: { phase: 'parent_outbox_pending' },
      });

      const concurrent = await Promise.all([
        workflowsA.applyWorkflowTerminalParentEffect({ ...fence, contract }),
        workflowsB.applyWorkflowTerminalParentEffect({ ...fence, contract }),
      ]);
      expect(concurrent.map(result => result.status).sort()).toEqual(['already_applied', 'applied']);
      const applied = concurrent.find(result => result.status === 'applied');
      expect(applied).toMatchObject({
        plan: {
          contract: { contractHash: contract.contractHash },
          frameworkActionKey: expect.stringMatching(/^wta:v1:[a-f0-9]{64}$/),
        },
      });
      await expect(workflowsB.getWorkflowTerminalContinuationPlan(fence)).resolves.toMatchObject({
        status: 'found',
        applicationState: 'applied',
        dispatchState: 'pending',
      });
      await expect(workflowsA.loadWorkflowSnapshot(parent)).resolves.toMatchObject({
        status: 'running',
        value: { final: true },
        context: { nested: { status: 'success', output: { answer: 42 } }, __state: { final: true } },
      });
      const afterApply = await workflowsA.getWorkflowTerminalParentContext(fence);
      if (afterApply.status !== 'found') throw new Error('Expected applied parent context');
      expect(afterApply.revision).not.toBe(context.revision);
      const patchedSnapshot = await workflowsA.loadWorkflowSnapshot(parent);
      if (!patchedSnapshot) throw new Error('Expected patched parent snapshot');
      await workflowsA.deleteWorkflowRunById(parent);
      await workflowsA.persistWorkflowSnapshot({ ...parent, snapshot: patchedSnapshot });
      const recreated = await workflowsA.getWorkflowTerminalParentContext(fence);
      if (recreated.status !== 'found') throw new Error('Expected recreated parent context');
      expect(recreated.revision).not.toBe(afterApply.revision);
      expect(recreated.revision).not.toBe(context.revision);
      await expect(workflowsB.applyWorkflowTerminalParentEffect({ ...fence, contract })).resolves.toMatchObject({
        status: 'already_applied',
      });

      const { contractHash: _contractHash, ...contractSpec } = contract;
      const changed = createWorkflowTerminalParentContinuationContract({
        ...contractSpec,
        expectedParentRevision: 'pg:v1:999',
      });
      await expect(
        workflowsA.applyWorkflowTerminalParentEffect({ ...fence, contract: changed }),
      ).resolves.toMatchObject({
        status: 'contract_conflict',
        plan: { contractHash: contract.contractHash },
      });

      await expect(
        pool.query(
          `UPDATE mastra_workflow_terminal_continuation_plans_v2
           SET contract = contract - 'executionMode'
           WHERE effect_key = $1 AND consumer_id = 'mastra.parent-application.v1'`,
          [prepared.effect.effectKey],
        ),
      ).rejects.toThrow();
      await expect(
        pool.query(
          `UPDATE mastra_workflow_terminal_continuation_plans_v2
           SET contract = jsonb_set(contract, '{expectedParentRevision}', '"pg:v1:999"'::jsonb)
           WHERE effect_key = $1 AND consumer_id = 'mastra.parent-application.v1'`,
          [prepared.effect.effectKey],
        ),
      ).rejects.toThrow();

      await pool.query(
        `UPDATE mastra_workflow_terminal_destination_receipts_v2
         SET application_state = 'quarantined', dispatch_state = 'none',
             applied_at = NULL, dispatch_pending_at = NULL, destination_applied_at = NULL,
             quarantined_at = updated_at
         WHERE effect_key = $1 AND consumer_id = 'mastra.parent-application.v1'`,
        [prepared.effect.effectKey],
      );
      await expect(workflowsA.applyWorkflowTerminalParentEffect({ ...fence, contract })).rejects.toThrow(
        'Contradictory workflow terminal parent application evidence',
      );
      await pool.query(
        `UPDATE mastra_workflow_terminal_destination_receipts_v2
         SET application_state = 'applied', dispatch_state = 'pending',
             applied_at = updated_at, dispatch_pending_at = updated_at,
             destination_applied_at = NULL, quarantined_at = NULL
         WHERE effect_key = $1 AND consumer_id = 'mastra.parent-application.v1'`,
        [prepared.effect.effectKey],
      );

      await pool.query(
        `UPDATE mastra_workflow_terminal_continuation_plans_v2
         SET created_at = created_at + 1
         WHERE workflow_name = $1 AND run_id = $2`,
        [child.workflowName, child.runId],
      );
      await expect(workflowsA.getWorkflowTerminalContinuationPlan(fence)).rejects.toThrow(
        'Invalid workflow terminal continuation plan integrity',
      );
      await pool.query(
        `UPDATE mastra_workflow_terminal_continuation_plans_v2
         SET created_at = created_at - 1,
             receipt_key = $1,
             workflow_name = 'corrupted-workflow',
             run_id = 'corrupted-run'
         WHERE effect_key = $2 AND consumer_id = 'mastra.parent-application.v1'`,
        [alternateReceipt.receipt.receiptKey, prepared.effect.effectKey],
      );
      await expect(workflowsA.getWorkflowTerminalContinuationPlan(fence)).rejects.toThrow(
        'Invalid workflow terminal continuation plan integrity',
      );
      await expect(
        workflowsA.getWorkflowTerminalContinuationPlan({ ...fence, claimToken: 'stale-token' }),
      ).rejects.toThrow('Invalid workflow terminal continuation plan integrity');
    } finally {
      await cleanup(child.workflowName);
      await cleanup(parent.workflowName);
    }
  });

  it.each([
    ['single', 1, true],
    ['multiple', 2, false],
  ] as const)(
    'round-trips %s foreach recovery payloads through JSONB without plan leakage',
    async (_label, suspendedCount, expectsCompatibilityHoist) => {
      const suffix = randomUUID();
      const child = { workflowName: `foreach-child-${suffix}`, runId: 'child-run' };
      const parent = { workflowName: `foreach-parent-${suffix}`, runId: 'parent-run' };
      const now = Date.now();
      const sourceIndex = suspendedCount;
      const suspended = Array.from({ length: suspendedCount }, (_, index) => ({
        status: 'suspended',
        suspendPayload: {
          __streamState: { messageList: { memoryInfo: { resourceId: `resource-${index}` } } },
          __workflow_meta: {
            resumeLabels: { [`resume-${index}`]: { stepId: 'each', foreachIndex: index } },
          },
        },
      }));
      const parentSnapshot: WorkflowRunState = {
        ...createEmptyWorkflowSnapshot(parent.runId),
        status: 'running',
        value: { stale: true },
        context: {
          each: {
            status: 'running',
            payload: Array.from({ length: suspendedCount + 1 }, (_, index) => `input-${index}`),
            output: [...suspended, null],
            startedAt: now - 20,
            metadata: { __workflow_meta: { iterationRunIds: { [String(sourceIndex)]: child.runId } } },
          },
          __state: { stale: true },
        } as WorkflowRunState['context'],
        serializedStepGraph: [
          { type: 'foreach', step: { id: 'each', component: 'WORKFLOW' }, opts: { concurrency: 2 } },
        ],
        activePaths: [0, sourceIndex],
        activeStepsPath: { each: [0, sourceIndex] },
        timestamp: now - 20,
      };

      await workflowsA.persistWorkflowSnapshot({ ...parent, snapshot: parentSnapshot });
      await workflowsA.persistWorkflowSnapshot({ ...child, snapshot: createEmptyWorkflowSnapshot(child.runId) });
      try {
        const claimed = await workflowsA.claimWorkflowTerminalization({
          ...child,
          eventKey: 'terminal-event',
          terminalStatus: 'success',
          ownerId: 'worker',
          leaseMs: 10_000,
        });
        if (claimed.status !== 'acquired') throw new Error('Expected child claim');
        const fence = {
          ...child,
          ownerId: claimed.record.ownerId,
          claimToken: claimed.record.claimToken,
          claimGeneration: claimed.record.claimGeneration,
        };
        const childSnapshot: WorkflowRunState = {
          ...createEmptyWorkflowSnapshot(child.runId),
          status: 'success',
          result: { status: 'success', output: { answer: 42 }, startedAt: now - 10, endedAt: now },
          value: { final: true },
          context: { __state: { final: true } } as WorkflowRunState['context'],
          timestamp: now,
        };
        const ancestry = nestedAncestry({
          child,
          parent,
          parentSnapshot,
          source: { kind: 'foreach-iteration', stepId: 'each', containerPath: [0], iterationIndex: sourceIndex },
        });
        await workflowsA.persistWorkflowTerminalRecoveryAncestry({ ...child, ancestry });
        await expect(
          workflowsA.persistWorkflowTerminalState({
            ...fence,
            snapshot: childSnapshot,
            recoveryEnvelope: recoveryEnvelope(childSnapshot, child, 'success', ancestry, childSnapshot.result),
          }),
        ).resolves.toMatchObject({ status: 'persisted' });
        const prepared = await workflowsA.prepareWorkflowTerminalEffect({
          ...fence,
          expectedPhase: 'run_state_persisted',
          effect: {
            kind: 'parent-workflow-step-end',
            parentWorkflowName: parent.workflowName,
            parentRunId: parent.runId,
            parentStepId: 'each',
            parentExecutionPath: [0, sourceIndex],
          },
        });
        if (prepared.status !== 'prepared' || prepared.effect.kind !== 'parent-workflow-step-end') {
          throw new Error('Expected parent effect');
        }
        const context = await workflowsA.getWorkflowTerminalParentContext(fence);
        if (context.status !== 'found') throw new Error('Expected parent context');
        const contract = createWorkflowTerminalParentContinuationContract({
          version: 1,
          terminalEffectKey: prepared.effect.effectKey,
          terminalEffectPayloadHash: prepared.effect.payloadHash,
          executionMode: 'continuous',
          expectedParentRevision: context.revision,
          graphFingerprint: createWorkflowTerminalGraphFingerprint(context.snapshot.serializedStepGraph),
          childTerminalStatus: 'success',
          observedParentStatus: 'running',
          source: { kind: 'foreach-iteration', stepId: 'each', containerPath: [0], iterationIndex: sourceIndex },
          action: {
            kind: 'suspend-parent',
            reason: 'foreach-suspended',
            target: { kind: 'container', containerType: 'foreach', executionPath: [0] },
          },
          patch: {
            kind: 'merge-child-terminal',
            resultWrite: 'source-coordinate',
            resultSource: 'retained-child-terminal-envelope',
            payloadWrite: 'preserve-parent-step-payload',
            metadataWrite: 'merge-child-and-bind-nested-run-id',
            stateWrite: 'replace-context-__state-from-retained-child',
            requestContextWrite: 'merge-from-retained-child',
            activeStepsWrite: 'derive-from-source-coordinate',
            snapshotTimestampWrite: 'storage-clock',
            parentRunWrite: {
              kind: 'set-suspended',
              resultSource: 'aggregate-container',
              activePathSource: 'source-coordinate',
              suspendedPathsSource: 'aggregate-container',
              resumeLabelsSource: 'aggregate-container',
            },
            loopWrite: { kind: 'preserve' },
          },
        });

        await expect(workflowsA.applyWorkflowTerminalParentEffect({ ...fence, contract })).resolves.toMatchObject({
          status: 'applied',
        });
        const stored = await workflowsB.loadWorkflowSnapshot(parent);
        if (!stored) throw new Error('Expected stored foreach parent');
        const suspendPayload = (stored.context.each as any).suspendPayload;
        for (let index = 0; index < suspendedCount; index++) {
          expect(suspendPayload.__workflow_meta.iterationSuspendPayloads[String(index)]).toMatchObject({
            __streamState: { messageList: { memoryInfo: { resourceId: `resource-${index}` } } },
          });
        }
        expect(Boolean(suspendPayload.__streamState)).toBe(expectsCompatibilityHoist);
        const plan = await workflowsB.getWorkflowTerminalContinuationPlan(fence);
        expect(plan).toMatchObject({ status: 'found', applicationState: 'applied', dispatchState: 'pending' });
        expect(JSON.stringify(plan)).not.toContain('__streamState');
        expect(JSON.stringify(plan)).not.toContain('resource-0');

        suspendPayload.__workflow_meta.iterationSuspendPayloads['0'].__streamState.messageList.memoryInfo.resourceId =
          'caller-mutated';
        await expect(workflowsA.loadWorkflowSnapshot(parent)).resolves.toMatchObject({
          context: {
            each: {
              suspendPayload: {
                __workflow_meta: {
                  iterationSuspendPayloads: {
                    '0': { __streamState: { messageList: { memoryInfo: { resourceId: 'resource-0' } } } },
                  },
                },
              },
            },
          },
        });
      } finally {
        await cleanup(child.workflowName);
        await cleanup(parent.workflowName);
      }
    },
  );

  it('allows only one child effect to consume a shared PostgreSQL parent revision', async () => {
    const suffix = randomUUID();
    const parent = { workflowName: `parent-race-${suffix}`, runId: 'parent-run' };
    const leftChild = { workflowName: `parent-race-left-${suffix}`, runId: 'child-left' };
    const rightChild = { workflowName: `parent-race-right-${suffix}`, runId: 'child-right' };
    const now = Date.now();
    const parentSnapshot: WorkflowRunState = {
      ...createEmptyWorkflowSnapshot(parent.runId),
      status: 'running',
      context: {
        left: {
          status: 'running',
          payload: {},
          startedAt: now - 20,
          metadata: { nestedRunId: leftChild.runId },
        },
        right: {
          status: 'running',
          payload: {},
          startedAt: now - 20,
          metadata: { nestedRunId: rightChild.runId },
        },
      } as WorkflowRunState['context'],
      serializedStepGraph: [
        {
          type: 'parallel',
          steps: [
            { type: 'step', step: { id: 'left', component: 'WORKFLOW' } },
            { type: 'step', step: { id: 'right', component: 'WORKFLOW' } },
          ],
        },
      ],
      activePaths: [0, 0],
      activeStepsPath: { left: [0, 0], right: [0, 1] },
      timestamp: now - 20,
    };
    await workflowsA.persistWorkflowSnapshot({ ...parent, snapshot: parentSnapshot });
    await workflowsA.persistWorkflowSnapshot({ ...leftChild, snapshot: createEmptyWorkflowSnapshot(leftChild.runId) });
    await workflowsA.persistWorkflowSnapshot({
      ...rightChild,
      snapshot: createEmptyWorkflowSnapshot(rightChild.runId),
    });

    try {
      const prepareChild = async (
        workflows: WorkflowsPG,
        child: typeof leftChild,
        stepId: 'left' | 'right',
        executionPath: [0, 0] | [0, 1],
      ) => {
        const claim = await workflows.claimWorkflowTerminalization({
          ...child,
          eventKey: `terminal-${stepId}`,
          terminalStatus: 'success',
          ownerId: `worker-${stepId}`,
          leaseMs: 10_000,
        });
        if (claim.status !== 'acquired') throw new Error(`Expected ${stepId} child claim`);
        const fence = {
          ...child,
          ownerId: claim.record.ownerId,
          claimToken: claim.record.claimToken,
          claimGeneration: claim.record.claimGeneration,
        };
        const snapshot: WorkflowRunState = {
          ...createEmptyWorkflowSnapshot(child.runId),
          status: 'success',
          result: { status: 'success', output: stepId, startedAt: now - 10, endedAt: now },
          context: { __state: { [stepId]: true } } as WorkflowRunState['context'],
          value: { [stepId]: true },
          timestamp: now,
        };
        const ancestry = nestedAncestry({
          child,
          parent,
          parentSnapshot,
          source: { kind: 'step', stepId, executionPath },
        });
        await workflows.persistWorkflowTerminalRecoveryAncestry({ ...child, ancestry });
        await workflows.persistWorkflowTerminalState({
          ...fence,
          snapshot,
          recoveryEnvelope: recoveryEnvelope(snapshot, child, 'success', ancestry, snapshot.result),
        });
        const prepared = await workflows.prepareWorkflowTerminalEffect({
          ...fence,
          expectedPhase: 'run_state_persisted',
          effect: {
            kind: 'parent-workflow-step-end',
            parentWorkflowName: parent.workflowName,
            parentRunId: parent.runId,
            parentStepId: stepId,
            parentExecutionPath: executionPath,
          },
        });
        if (prepared.status !== 'prepared' || prepared.effect.kind !== 'parent-workflow-step-end') {
          throw new Error(`Expected ${stepId} parent effect`);
        }
        const context = await workflows.getWorkflowTerminalParentContext(fence);
        if (context.status !== 'found') throw new Error(`Expected ${stepId} parent context`);
        return { workflows, fence, effect: prepared.effect, context, stepId, executionPath };
      };

      const [left, right] = await Promise.all([
        prepareChild(workflowsA, leftChild, 'left', [0, 0]),
        prepareChild(workflowsB, rightChild, 'right', [0, 1]),
      ]);
      expect(left.context.revision).toBe(right.context.revision);
      const contractFor = (candidate: typeof left | typeof right) =>
        createWorkflowTerminalParentContinuationContract({
          version: 1,
          terminalEffectKey: candidate.effect.effectKey,
          terminalEffectPayloadHash: candidate.effect.payloadHash,
          executionMode: 'continuous',
          expectedParentRevision: left.context.revision,
          graphFingerprint: createWorkflowTerminalGraphFingerprint(parentSnapshot.serializedStepGraph),
          childTerminalStatus: 'success',
          observedParentStatus: 'running',
          source: { kind: 'step', stepId: candidate.stepId, executionPath: candidate.executionPath },
          action: {
            kind: 'wait',
            reason: 'parallel-aggregation',
            coordinate: { kind: 'container', containerType: 'parallel', executionPath: [0] },
          },
          patch: {
            kind: 'merge-child-terminal',
            resultWrite: 'source-coordinate',
            resultSource: 'retained-child-terminal-envelope',
            payloadWrite: 'preserve-parent-step-payload',
            metadataWrite: 'merge-child-and-bind-nested-run-id',
            stateWrite: 'replace-context-__state-from-retained-child',
            requestContextWrite: 'merge-from-retained-child',
            activeStepsWrite: 'derive-from-source-coordinate',
            snapshotTimestampWrite: 'storage-clock',
            parentRunWrite: { kind: 'preserve' },
            loopWrite: { kind: 'preserve' },
          },
        });

      const outcomes = await Promise.all([
        left.workflows.applyWorkflowTerminalParentEffect({ ...left.fence, contract: contractFor(left) }),
        right.workflows.applyWorkflowTerminalParentEffect({ ...right.fence, contract: contractFor(right) }),
      ]);
      expect(outcomes.map(result => result.status).sort()).toEqual(['applied', 'parent_conflict']);
      const evidence = await pool.query<{ receipt_count: string; plan_count: string }>(
        `SELECT
           (SELECT count(*) FROM mastra_workflow_terminal_destination_receipts_v2
            WHERE workflow_name = ANY($1::text[])) AS receipt_count,
           (SELECT count(*) FROM mastra_workflow_terminal_continuation_plans_v2
            WHERE workflow_name = ANY($1::text[])) AS plan_count`,
        [[leftChild.workflowName, rightChild.workflowName]],
      );
      expect(evidence.rows).toEqual([{ receipt_count: '1', plan_count: '1' }]);
      const snapshot = await workflowsA.loadWorkflowSnapshot(parent);
      expect([snapshot?.context.left?.status, snapshot?.context.right?.status].sort()).toEqual(['running', 'success']);
    } finally {
      await cleanup(leftChild.workflowName);
      await cleanup(rightChild.workflowName);
      await cleanup(parent.workflowName);
    }
  });

  it.each(['wait', 'noop', 'quarantine'] as const)(
    'persists the PostgreSQL %s receipt, journal, parent, and revision state family',
    async variant => {
      const suffix = randomUUID();
      const parent = { workflowName: `state-family-parent-${variant}-${suffix}`, runId: 'parent-run' };
      const child = { workflowName: `state-family-child-${variant}-${suffix}`, runId: 'child-run' };
      const now = Date.now();
      const sourceStepId = variant === 'wait' ? 'left' : 'nested';
      const sourcePath = variant === 'wait' ? ([0, 0] as const) : ([0] as const);
      const parentSnapshot: WorkflowRunState = {
        ...createEmptyWorkflowSnapshot(parent.runId),
        status: variant === 'noop' ? 'success' : 'running',
        context: {
          [sourceStepId]: {
            status: 'running',
            payload: {},
            startedAt: now - 20,
            metadata: { nestedRunId: child.runId },
          },
          ...(variant === 'wait' ? { right: { status: 'running', payload: {}, startedAt: now - 20 } } : {}),
          __state: { stale: true },
        } as WorkflowRunState['context'],
        serializedStepGraph:
          variant === 'wait'
            ? [
                {
                  type: 'parallel',
                  steps: [
                    { type: 'step', step: { id: 'left', component: 'WORKFLOW' } },
                    { type: 'step', step: { id: 'right' } },
                  ],
                },
              ]
            : [
                { type: 'step', step: { id: 'nested', component: 'WORKFLOW' } },
                { type: 'sleep', id: 'after-child', duration: 10 },
              ],
        activePaths: variant === 'noop' ? [] : [...sourcePath],
        activeStepsPath:
          variant === 'wait' ? { left: [0, 0], right: [0, 1] } : variant === 'noop' ? {} : { nested: [0] },
        timestamp: now + 60_000,
      };
      await workflowsA.persistWorkflowSnapshot({
        ...parent,
        snapshot: variant === 'noop' ? { ...parentSnapshot, status: 'running' } : parentSnapshot,
      });
      await workflowsA.persistWorkflowSnapshot({ ...child, snapshot: createEmptyWorkflowSnapshot(child.runId) });
      try {
        const claim = await workflowsA.claimWorkflowTerminalization({
          ...child,
          eventKey: `state-family-${variant}`,
          terminalStatus: 'success',
          ownerId: 'state-family-worker',
          leaseMs: 10_000,
        });
        if (claim.status !== 'acquired') throw new Error('Expected state-family claim');
        const fence = {
          ...child,
          ownerId: claim.record.ownerId,
          claimToken: claim.record.claimToken,
          claimGeneration: claim.record.claimGeneration,
        };
        const childSnapshot: WorkflowRunState = {
          ...createEmptyWorkflowSnapshot(child.runId),
          status: 'success',
          result: { status: 'success', output: variant, startedAt: now - 10, endedAt: now },
          context: { __state: { final: true } } as WorkflowRunState['context'],
          value: { final: true },
          timestamp: now,
        };
        const ancestry = nestedAncestry({
          child,
          parent,
          parentSnapshot,
          source: { kind: 'step', stepId: sourceStepId, executionPath: [...sourcePath] },
        });
        await workflowsA.persistWorkflowTerminalRecoveryAncestry({ ...child, ancestry });
        if (variant === 'noop') await workflowsA.persistWorkflowSnapshot({ ...parent, snapshot: parentSnapshot });
        await workflowsA.persistWorkflowTerminalState({
          ...fence,
          snapshot: childSnapshot,
          recoveryEnvelope: recoveryEnvelope(childSnapshot, child, 'success', ancestry, childSnapshot.result),
        });
        const prepared = await workflowsA.prepareWorkflowTerminalEffect({
          ...fence,
          expectedPhase: 'run_state_persisted',
          effect: {
            kind: 'parent-workflow-step-end',
            parentWorkflowName: parent.workflowName,
            parentRunId: parent.runId,
            parentStepId: sourceStepId,
            parentExecutionPath: [...sourcePath],
          },
        });
        if (prepared.status !== 'prepared' || prepared.effect.kind !== 'parent-workflow-step-end') {
          throw new Error('Expected state-family parent effect');
        }
        const context = await workflowsA.getWorkflowTerminalParentContext(fence);
        if (context.status !== 'found') throw new Error('Expected state-family parent context');
        const contractBase = {
          version: 1,
          terminalEffectKey: prepared.effect.effectKey,
          terminalEffectPayloadHash: prepared.effect.payloadHash,
          executionMode: 'continuous',
          expectedParentRevision: context.revision,
          graphFingerprint: createWorkflowTerminalGraphFingerprint(context.snapshot.serializedStepGraph),
          childTerminalStatus: 'success',
          observedParentStatus: variant === 'noop' ? ('success' as const) : ('running' as const),
          source: { kind: 'step' as const, stepId: sourceStepId, executionPath: [...sourcePath] },
        };
        const contract =
          variant === 'wait'
            ? createWorkflowTerminalParentContinuationContract({
                ...contractBase,
                action: {
                  kind: 'wait',
                  reason: 'parallel-aggregation',
                  coordinate: { kind: 'container', containerType: 'parallel', executionPath: [0] },
                },
                patch: {
                  kind: 'merge-child-terminal',
                  resultWrite: 'source-coordinate',
                  resultSource: 'retained-child-terminal-envelope',
                  payloadWrite: 'preserve-parent-step-payload',
                  metadataWrite: 'merge-child-and-bind-nested-run-id',
                  stateWrite: 'replace-context-__state-from-retained-child',
                  requestContextWrite: 'merge-from-retained-child',
                  activeStepsWrite: 'derive-from-source-coordinate',
                  snapshotTimestampWrite: 'storage-clock',
                  parentRunWrite: { kind: 'preserve' },
                  loopWrite: { kind: 'preserve' },
                },
              })
            : variant === 'noop'
              ? createWorkflowTerminalParentContinuationContract({
                  ...contractBase,
                  action: { kind: 'noop', reason: 'already-terminal' },
                  patch: { kind: 'none' },
                })
              : createWorkflowTerminalParentContinuationContract({
                  ...contractBase,
                  action: {
                    kind: 'quarantine',
                    reason: 'plan-conflict',
                    conflictDigest: `sha256:${'f'.repeat(64)}`,
                  },
                  patch: { kind: 'none' },
                });
        const before = await workflowsA.loadWorkflowSnapshot(parent);
        const result = await workflowsA.applyWorkflowTerminalParentEffect({ ...fence, contract });
        expect(result.status).toBe(variant === 'quarantine' ? 'quarantined' : 'applied');
        await expect(workflowsA.getWorkflowTerminalContinuationPlan(fence)).resolves.toMatchObject({
          status: 'found',
          applicationState: variant === 'quarantine' ? 'quarantined' : 'applied',
          dispatchState: 'none',
        });
        await expect(workflowsA.getWorkflowTerminalization(fence)).resolves.toMatchObject({
          status: 'found',
          record: {
            phase: variant === 'wait' || variant === 'noop' ? 'parent_effect_recorded' : 'parent_outbox_pending',
          },
        });
        const after = await workflowsA.loadWorkflowSnapshot(parent);
        if (variant === 'wait') {
          expect(after).toMatchObject({
            timestamp: parentSnapshot.timestamp,
            context: { left: { status: 'success' }, right: { status: 'running' } },
          });
          const evidenceClocks = await pool.query<{ applied_at: string; journal_updated_at: string }>(
            `SELECT receipt.applied_at, journal.updated_at AS journal_updated_at
             FROM mastra_workflow_terminal_destination_receipts_v2 AS receipt
             JOIN mastra_workflow_terminalizations AS journal
               ON journal.workflow_name = receipt.workflow_name AND journal.run_id = receipt.run_id
             WHERE receipt.workflow_name = $1 AND receipt.run_id = $2`,
            [child.workflowName, child.runId],
          );
          expect(Number(evidenceClocks.rows[0]!.applied_at)).toBeLessThan(parentSnapshot.timestamp);
          expect(Number(evidenceClocks.rows[0]!.journal_updated_at)).toBeLessThan(parentSnapshot.timestamp);
        } else {
          expect(after).toEqual(before);
        }
        const revision = await pool.query<{ generation: string }>(
          `SELECT generation FROM mastra_workflow_parent_revisions
           WHERE workflow_name = $1 AND run_id = $2`,
          [parent.workflowName, parent.runId],
        );
        const expectedGeneration = Number(context.revision.slice('pg:v1:'.length)) + (variant === 'wait' ? 1 : 0);
        expect(revision.rows).toEqual([{ generation: String(expectedGeneration) }]);
      } finally {
        await cleanup(child.workflowName);
        await cleanup(parent.workflowName);
      }
    },
  );

  it('uses database time and fences concurrent adapter instances', async () => {
    const workflowName = `terminalization-${Date.now()}`;
    const runId = 'run';
    const run = { workflowName, runId };
    await workflowsA.persistWorkflowSnapshot({ ...run, snapshot: createEmptyWorkflowSnapshot(runId) });

    try {
      expect(workflowsA.supportsWorkflowTerminalizationJournal()).toBe(true);
      const results = await (async () => {
        const localClock = vi.spyOn(Date, 'now').mockReturnValue(0);
        try {
          return await Promise.all([
            workflowsA.claimWorkflowTerminalization({
              ...run,
              eventKey: 'event',
              terminalStatus: 'failed',
              ownerId: 'worker-a',
              leaseMs: 1_000,
            }),
            workflowsB.claimWorkflowTerminalization({
              ...run,
              eventKey: 'event',
              terminalStatus: 'failed',
              ownerId: 'worker-b',
              leaseMs: 1_000,
            }),
          ]);
        } finally {
          localClock.mockRestore();
        }
      })();

      expect(results.map(result => result.status).sort()).toEqual(['acquired', 'leased']);
      const acquired = results.find(result => result.status === 'acquired');
      const leased = results.find(result => result.status === 'leased');
      if (acquired?.status !== 'acquired') throw new Error('claim owner not found');
      if (leased?.status !== 'leased') throw new Error('leased result not found');
      expect(acquired.record.createdAt).toBeGreaterThan(0);
      expect(leased.record).not.toHaveProperty('ownerId');
      expect(leased.record).not.toHaveProperty('claimToken');
      expect(leased.record).not.toHaveProperty('claimGeneration');

      await expect(
        workflowsB.claimWorkflowTerminalization({
          ...run,
          eventKey: 'event',
          terminalStatus: 'failed',
          ownerId: acquired.record.ownerId!,
          claimToken: acquired.record.claimToken,
          claimGeneration: acquired.record.claimGeneration,
          leaseMs: 100,
        }),
      ).resolves.toMatchObject({ status: 'renewed' });

      await new Promise(resolve => setTimeout(resolve, 500));
      const takeover = await workflowsB.claimWorkflowTerminalization({
        ...run,
        eventKey: 'event',
        terminalStatus: 'failed',
        ownerId: 'worker-c',
        leaseMs: 1_000,
      });
      expect(takeover).toMatchObject({
        status: 'acquired',
        record: { claimGeneration: acquired.record.claimGeneration + 1 },
      });
      if (takeover.status !== 'acquired') throw new Error('takeover failed');
      const terminalSnapshot = { ...createEmptyWorkflowSnapshot(runId), status: 'failed' as const };
      const terminalRecovery = recoveryEnvelope(terminalSnapshot, run, 'failed');

      await expect(
        workflowsA.persistWorkflowTerminalState({
          ...run,
          ownerId: acquired.record.ownerId!,
          claimToken: acquired.record.claimToken!,
          claimGeneration: acquired.record.claimGeneration,
          snapshot: terminalSnapshot,
          recoveryEnvelope: terminalRecovery,
        }),
      ).resolves.toMatchObject({ status: 'not_owner' });
      await expect(
        workflowsB.advanceWorkflowTerminalization({
          ...run,
          ownerId: takeover.record.ownerId!,
          claimToken: takeover.record.claimToken!,
          claimGeneration: takeover.record.claimGeneration,
          expectedPhase: 'terminalization_pending',
          nextPhase: 'run_state_persisted',
        }),
      ).resolves.toEqual({ status: 'invalid_transition' });
      await expect(
        workflowsB.persistWorkflowTerminalState({
          ...run,
          ownerId: takeover.record.ownerId!,
          claimToken: takeover.record.claimToken!,
          claimGeneration: takeover.record.claimGeneration,
          snapshot: terminalSnapshot,
          recoveryEnvelope: terminalRecovery,
        }),
      ).resolves.toMatchObject({ status: 'persisted', record: { phase: 'run_state_persisted' } });
      await expect(workflowsA.loadWorkflowSnapshot(run)).resolves.toMatchObject({ status: 'failed' });
    } finally {
      await cleanup(workflowName);
    }
  });

  it('observes terminalization without waiting for the writer advisory lock', async () => {
    const workflowName = `terminalization-read-only-${randomUUID()}`;
    const runId = 'run';
    const run = { workflowName, runId };
    await workflowsA.persistWorkflowSnapshot({ ...run, snapshot: createEmptyWorkflowSnapshot(runId) });
    await workflowsA.claimWorkflowTerminalization({
      ...run,
      eventKey: 'event',
      terminalStatus: 'failed',
      ownerId: 'worker-a',
      leaseMs: 1_000,
    });

    const lockClient = await pool.connect();
    try {
      await lockClient.query('BEGIN');
      await lockClient.query('SELECT pg_advisory_xact_lock(hashtextextended($1, 0))', [
        JSON.stringify([workflowName, runId]),
      ]);

      const observation = await Promise.race([
        workflowsB.getWorkflowTerminalization(run),
        new Promise<never>((_, reject) =>
          setTimeout(() => reject(new Error('observation waited for the writer lock')), 500),
        ),
      ]);
      expect(observation).toMatchObject({ status: 'found', record: { eventKey: 'event' } });
    } finally {
      await lockClient.query('ROLLBACK');
      lockClient.release();
      await cleanup(workflowName);
    }
  });

  it('rolls back the journal phase when canonical snapshot serialization fails', async () => {
    const workflowName = `terminalization-rollback-${randomUUID()}`;
    const runId = 'run';
    const run = { workflowName, runId };
    await workflowsA.persistWorkflowSnapshot({ ...run, snapshot: createEmptyWorkflowSnapshot(runId) });

    try {
      const claim = await workflowsA.claimWorkflowTerminalization({
        ...run,
        eventKey: 'terminal-event',
        terminalStatus: 'failed',
        ownerId: 'worker',
        leaseMs: 10_000,
      });
      if (claim.status !== 'acquired') throw new Error('claim failed');
      const validSnapshot = { ...createEmptyWorkflowSnapshot(runId), status: 'failed' as const };
      const validRecovery = recoveryEnvelope(validSnapshot, run, 'failed');
      await expect(
        workflowsA.persistWorkflowTerminalState({
          ...run,
          ownerId: claim.record.ownerId,
          claimToken: claim.record.claimToken,
          claimGeneration: claim.record.claimGeneration,
          snapshot: undefined as never,
          recoveryEnvelope: validRecovery,
        }),
      ).resolves.toEqual({ status: 'invalid_snapshot' });
      await expect(
        workflowsA.persistWorkflowTerminalState({
          ...run,
          ownerId: claim.record.ownerId,
          claimToken: claim.record.claimToken,
          claimGeneration: claim.record.claimGeneration,
          snapshot: {
            ...createEmptyWorkflowSnapshot(runId),
            status: 'failed',
            value: { unserializable: 1n },
          } as never,
          recoveryEnvelope: validRecovery,
        }),
      ).resolves.toEqual({ status: 'invalid_snapshot' });
      await expect(
        workflowsA.persistWorkflowTerminalState({
          ...run,
          ownerId: claim.record.ownerId,
          claimToken: claim.record.claimToken,
          claimGeneration: claim.record.claimGeneration,
          snapshot: { runId, status: 'failed', serializedStepGraph: [] } as unknown as WorkflowRunState,
          recoveryEnvelope: validRecovery,
        }),
      ).resolves.toEqual({ status: 'invalid_snapshot' });
      await expect(
        workflowsA.persistWorkflowTerminalState({
          ...run,
          ownerId: claim.record.ownerId,
          claimToken: claim.record.claimToken,
          claimGeneration: claim.record.claimGeneration,
          snapshot: validSnapshot,
          recoveryEnvelope: undefined as never,
        }),
      ).resolves.toEqual({ status: 'invalid_recovery_envelope' });
      await expect(workflowsA.getWorkflowTerminalization(run)).resolves.toMatchObject({
        status: 'found',
        record: { phase: 'terminalization_pending' },
      });
      await expect(workflowsA.loadWorkflowSnapshot(run)).resolves.toMatchObject({ status: 'pending' });
      const retained = await pool.query<{ count: string }>(
        `SELECT count(*)::text AS count FROM mastra_workflow_terminal_snapshots_v2
         WHERE workflow_name = $1 AND run_id = $2`,
        [run.workflowName, run.runId],
      );
      expect(retained.rows[0]?.count).toBe('0');
      const revision = await pool.query<{ generation: string; terminal_status: string | null }>(
        `SELECT generation::text, terminal_status FROM mastra_workflow_parent_revisions
         WHERE workflow_name = $1 AND run_id = $2`,
        [run.workflowName, run.runId],
      );
      expect(revision.rows).toEqual([{ generation: '2', terminal_status: 'failed' }]);
    } finally {
      await cleanup(workflowName);
    }
  });

  it('rejects terminal persistence when an existing snapshot loses revision evidence', async () => {
    const workflowName = `terminalization-revision-seed-${randomUUID()}`;
    const runId = 'run';
    const run = { workflowName, runId };
    await workflowsA.persistWorkflowSnapshot({ ...run, snapshot: createEmptyWorkflowSnapshot(runId) });

    try {
      const claim = await workflowsA.claimWorkflowTerminalization({
        ...run,
        eventKey: 'terminal-event',
        terminalStatus: 'failed',
        ownerId: 'worker',
        leaseMs: 10_000,
      });
      if (claim.status !== 'acquired') throw new Error('claim failed');
      await pool.query(`DELETE FROM mastra_workflow_parent_revisions WHERE workflow_name = $1 AND run_id = $2`, [
        run.workflowName,
        run.runId,
      ]);
      const snapshot = { ...createEmptyWorkflowSnapshot(runId), status: 'failed' as const };

      await expect(
        workflowsA.persistWorkflowTerminalState({
          ...run,
          ownerId: claim.record.ownerId,
          claimToken: claim.record.claimToken,
          claimGeneration: claim.record.claimGeneration,
          snapshot,
          recoveryEnvelope: recoveryEnvelope(snapshot, run, 'failed'),
        }),
      ).rejects.toThrow('missing parent revision evidence');
      const revision = await pool.query<{ generation: string; terminal_status: string | null }>(
        `SELECT generation::text, terminal_status FROM mastra_workflow_parent_revisions
         WHERE workflow_name = $1 AND run_id = $2`,
        [run.workflowName, run.runId],
      );
      expect(revision.rows).toEqual([]);
      await expect(workflowsA.getWorkflowTerminalization(run)).resolves.toMatchObject({
        status: 'found',
        record: { phase: 'terminalization_pending' },
      });
      await expect(workflowsA.loadWorkflowSnapshot(run)).resolves.toMatchObject({ status: 'pending' });
    } finally {
      await cleanup(workflowName);
    }
  });

  it.each([
    { label: 'explicit', existingResourceId: undefined, requestedResourceId: 'invalid\0resource' },
    { label: 'existing-row fallback', existingResourceId: '', requestedResourceId: undefined },
  ])('rejects an invalid $label resourceId before any terminal state write', async options => {
    const workflowName = `terminalization-invalid-resource-${options.label}-${randomUUID()}`;
    const runId = 'run';
    const run = { workflowName, runId };
    await workflowsA.persistWorkflowSnapshot({
      ...run,
      resourceId: options.existingResourceId,
      snapshot: createEmptyWorkflowSnapshot(runId),
    });

    try {
      const claim = await workflowsA.claimWorkflowTerminalization({
        ...run,
        eventKey: 'terminal-event',
        terminalStatus: 'failed',
        ownerId: 'worker',
        leaseMs: 10_000,
      });
      if (claim.status !== 'acquired') throw new Error('claim failed');
      const snapshot = { ...createEmptyWorkflowSnapshot(runId), status: 'failed' as const };

      await expect(
        workflowsA.persistWorkflowTerminalState({
          ...run,
          ownerId: claim.record.ownerId,
          claimToken: claim.record.claimToken,
          claimGeneration: claim.record.claimGeneration,
          snapshot,
          recoveryEnvelope: recoveryEnvelope(snapshot, run, 'failed'),
          resourceId: options.requestedResourceId,
        }),
      ).rejects.toThrow(/resourceId must be a well-formed non-empty string/);

      await expect(workflowsA.getWorkflowTerminalization(run)).resolves.toMatchObject({
        status: 'found',
        record: { phase: 'terminalization_pending' },
      });
      await expect(workflowsA.loadWorkflowSnapshot(run)).resolves.toMatchObject({ status: 'pending' });
      const retained = await pool.query<{ count: string }>(
        `SELECT count(*)::text AS count FROM mastra_workflow_terminal_snapshots_v2
         WHERE workflow_name = $1 AND run_id = $2`,
        [workflowName, runId],
      );
      expect(retained.rows[0]?.count).toBe('0');
    } finally {
      await cleanup(workflowName);
    }
  });

  it('validates the same materialized snapshot that it writes when toJSON changes terminal status', async () => {
    const workflowName = `terminalization-materialization-${randomUUID()}`;
    const runId = 'run';
    const run = { workflowName, runId };
    await workflowsA.persistWorkflowSnapshot({ ...run, snapshot: createEmptyWorkflowSnapshot(runId) });

    try {
      const claim = await workflowsA.claimWorkflowTerminalization({
        ...run,
        eventKey: 'terminal-event',
        terminalStatus: 'failed',
        ownerId: 'worker',
        leaseMs: 10_000,
      });
      if (claim.status !== 'acquired') throw new Error('claim failed');
      const snapshot = {
        ...createEmptyWorkflowSnapshot(runId),
        status: 'failed' as const,
        toJSON() {
          return { ...this, status: 'success', toJSON: undefined };
        },
      };

      await expect(
        workflowsA.persistWorkflowTerminalState({
          ...run,
          ownerId: claim.record.ownerId,
          claimToken: claim.record.claimToken,
          claimGeneration: claim.record.claimGeneration,
          snapshot,
          recoveryEnvelope: recoveryEnvelope(snapshot, run, 'failed'),
        }),
      ).resolves.toEqual({ status: 'invalid_snapshot' });
      await expect(workflowsA.getWorkflowTerminalization(run)).resolves.toMatchObject({
        status: 'found',
        record: { phase: 'terminalization_pending' },
      });
      await expect(workflowsA.loadWorkflowSnapshot(run)).resolves.toMatchObject({ status: 'pending' });
    } finally {
      await cleanup(workflowName);
    }
  });

  it('serializes an authorized terminal snapshot once with ordinary PG JSON semantics', async () => {
    const workflowName = `terminalization-capture-${randomUUID()}`;
    const run = { workflowName, runId: 'run' };
    await workflowsA.persistWorkflowSnapshot({ ...run, snapshot: createEmptyWorkflowSnapshot(run.runId) });
    try {
      const claim = await workflowsA.claimWorkflowTerminalization({
        ...run,
        eventKey: 'terminal-event',
        terminalStatus: 'failed',
        ownerId: 'worker',
        leaseMs: 10_000,
      });
      if (claim.status !== 'acquired') throw new Error('claim failed');
      const canonicalSnapshot = {
        ...createEmptyWorkflowSnapshot(run.runId),
        status: 'failed' as const,
        context: { __state: { captured: true } } as WorkflowRunState['context'],
        value: { captured: true },
      };
      let toJSONCalls = 0;
      const snapshot = {
        ...canonicalSnapshot,
        toJSON() {
          toJSONCalls += 1;
          return canonicalSnapshot;
        },
      };
      await expect(
        workflowsA.persistWorkflowTerminalState({
          ...run,
          ownerId: claim.record.ownerId,
          claimToken: claim.record.claimToken,
          claimGeneration: claim.record.claimGeneration,
          snapshot,
          recoveryEnvelope: recoveryEnvelope(canonicalSnapshot, run, 'failed'),
        }),
      ).resolves.toMatchObject({ status: 'persisted' });
      expect(toJSONCalls).toBe(1);
      await expect(workflowsA.loadWorkflowSnapshot(run)).resolves.toMatchObject({
        status: 'failed',
        context: { __state: { captured: true } },
        value: { captured: true },
      });
    } finally {
      await cleanup(workflowName);
    }
  });

  it('preserves fence-first precedence without executing hostile stale snapshot serialization', async () => {
    const workflowName = `terminalization-precedence-${randomUUID()}`;
    const run = { workflowName, runId: 'run' };
    const memory = new WorkflowsInMemory({ db: new InMemoryDB() });
    const stores = [memory, workflowsA] as const;
    try {
      for (const store of stores) {
        await store.persistWorkflowSnapshot({ ...run, snapshot: createEmptyWorkflowSnapshot(run.runId) });
      }
      const claims = await Promise.all(
        stores.map(store =>
          store.claimWorkflowTerminalization({
            ...run,
            eventKey: 'terminal-event',
            terminalStatus: 'failed',
            ownerId: 'owner-a',
            leaseMs: 10_000,
          }),
        ),
      );
      const results = await Promise.all(
        stores.map((store, index) => {
          const claim = claims[index]!;
          if (claim.status !== 'acquired') throw new Error(`Expected acquired, received ${claim.status}`);
          return store.persistWorkflowTerminalState({
            ...run,
            ownerId: 'owner-b',
            claimToken: claim.record.claimToken,
            claimGeneration: claim.record.claimGeneration,
            snapshot: { ...createEmptyWorkflowSnapshot(run.runId), status: 'failed' },
            recoveryEnvelope: {} as never,
          });
        }),
      );
      expect(results.map(result => result.status)).toEqual(['not_owner', 'not_owner']);

      const validSnapshot = { ...createEmptyWorkflowSnapshot(run.runId), status: 'failed' as const };
      const validRecovery = recoveryEnvelope(validSnapshot, run, 'failed');
      let toJSONCalls = 0;
      for (const [index, store] of stores.entries()) {
        await expect(
          store.persistWorkflowTerminalState({
            ...run,
            ownerId: 'owner-b',
            claimToken: claims[index]!.record.claimToken,
            claimGeneration: claims[index]!.record.claimGeneration,
            snapshot: {
              ...validSnapshot,
              value: { unserializable: 1n },
            } as never,
            recoveryEnvelope: validRecovery,
          }),
        ).resolves.toMatchObject({ status: 'not_owner' });
        await expect(
          store.persistWorkflowTerminalState({
            ...run,
            ownerId: 'owner-b',
            claimToken: claims[index]!.record.claimToken,
            claimGeneration: claims[index]!.record.claimGeneration,
            snapshot: {
              ...validSnapshot,
              toJSON() {
                toJSONCalls += 1;
                throw new Error('must not execute before authorization');
              },
            },
            recoveryEnvelope: validRecovery,
          }),
        ).resolves.toMatchObject({ status: 'not_owner' });
      }
      expect(toJSONCalls).toBe(0);
    } finally {
      await cleanup(workflowName);
    }
  });

  it('materializes accessor-backed operation identity once before locking and writing', async () => {
    const workflowName = `terminalization-envelope-${randomUUID()}`;
    const runId = 'run';
    const run = { workflowName, runId };
    const alternate = {
      workflowName: `terminalization-envelope-alternate-${randomUUID()}`,
      runId: 'alternate-run',
    };
    await workflowsA.persistWorkflowSnapshot({ ...run, snapshot: createEmptyWorkflowSnapshot(runId) });
    await workflowsA.persistWorkflowSnapshot({
      ...alternate,
      snapshot: createEmptyWorkflowSnapshot(alternate.runId),
    });

    try {
      const claim = await workflowsA.claimWorkflowTerminalization({
        ...run,
        eventKey: 'terminal-event',
        terminalStatus: 'failed',
        ownerId: 'worker',
        leaseMs: 10_000,
      });
      if (claim.status !== 'acquired') throw new Error('claim failed');
      const snapshot = { ...createEmptyWorkflowSnapshot(runId), status: 'failed' as const };
      const operation = {
        ...run,
        ownerId: claim.record.ownerId,
        claimToken: claim.record.claimToken,
        claimGeneration: claim.record.claimGeneration,
        snapshot,
        recoveryEnvelope: recoveryEnvelope(snapshot, run, 'failed'),
      };
      let workflowNameReads = 0;
      let runIdReads = 0;
      Object.defineProperties(operation, {
        workflowName: {
          enumerable: true,
          get: () => (workflowNameReads++ === 0 ? workflowName : alternate.workflowName),
        },
        runId: { enumerable: true, get: () => (runIdReads++ === 0 ? runId : alternate.runId) },
      });

      await expect(workflowsA.persistWorkflowTerminalState(operation)).resolves.toMatchObject({
        status: 'persisted',
        record: { phase: 'run_state_persisted' },
      });
      expect({ workflowNameReads, runIdReads }).toEqual({ workflowNameReads: 1, runIdReads: 1 });
      await expect(workflowsA.loadWorkflowSnapshot(run)).resolves.toMatchObject({ status: 'failed' });
      await expect(workflowsA.loadWorkflowSnapshot(alternate)).resolves.toMatchObject({ status: 'pending' });
      await expect(workflowsA.getWorkflowTerminalization(run)).resolves.toMatchObject({
        status: 'found',
        record: { phase: 'run_state_persisted' },
      });
      await expect(workflowsA.getWorkflowTerminalization(alternate)).resolves.toEqual({ status: 'missing_record' });
    } finally {
      await cleanup(workflowName);
      await cleanup(alternate.workflowName);
    }
  });

  it('materializes every journal mutation envelope before locking or selecting a run', async () => {
    const workflowNames: string[] = [];
    const makePair = async (label: string) => {
      const intended = { workflowName: `terminalization-envelope-${label}-${randomUUID()}`, runId: `${label}-run` };
      const alternate = {
        workflowName: `terminalization-envelope-${label}-alternate-${randomUUID()}`,
        runId: `${label}-alternate-run`,
      };
      workflowNames.push(intended.workflowName, alternate.workflowName);
      await workflowsA.persistWorkflowSnapshot({
        ...intended,
        snapshot: createEmptyWorkflowSnapshot(intended.runId),
      });
      await workflowsA.persistWorkflowSnapshot({
        ...alternate,
        snapshot: createEmptyWorkflowSnapshot(alternate.runId),
      });
      return { intended, alternate };
    };
    const claim = async (run: { workflowName: string; runId: string }, eventKey: string) => {
      const result = await workflowsA.claimWorkflowTerminalization({
        ...run,
        eventKey,
        terminalStatus: 'failed',
        ownerId: `${eventKey}-owner`,
        leaseMs: 10_000,
      });
      if (result.status !== 'acquired') throw new Error(`claim failed for ${eventKey}`);
      return result.record;
    };

    try {
      const claimPair = await makePair('claim');
      const claimOperation = withStatefulIdentity(
        {
          ...claimPair.intended,
          eventKey: 'claim-event',
          terminalStatus: 'failed' as const,
          ownerId: 'claim-owner',
          leaseMs: 10_000,
        },
        claimPair.alternate,
      );
      await expect(workflowsA.claimWorkflowTerminalization(claimOperation.operation)).resolves.toMatchObject({
        status: 'acquired',
        record: { eventKey: 'claim-event' },
      });
      expect(claimOperation.reads()).toEqual({ workflowName: 1, runId: 1 });
      await expect(workflowsA.getWorkflowTerminalization(claimPair.alternate)).resolves.toEqual({
        status: 'missing_record',
      });
      const getOperation = withStatefulIdentity({ ...claimPair.intended }, claimPair.alternate);
      await expect(workflowsA.getWorkflowTerminalization(getOperation.operation)).resolves.toMatchObject({
        status: 'found',
        record: { eventKey: 'claim-event' },
      });
      expect(getOperation.reads()).toEqual({ workflowName: 1, runId: 1 });

      const advancePair = await makePair('advance');
      const advanceClaim = await claim(advancePair.intended, 'advance-intended');
      await claim(advancePair.alternate, 'advance-alternate');
      await pool.query(
        `UPDATE mastra_workflow_terminalizations
         SET phase = 'finish_effect_recorded'
         WHERE workflow_name IN ($1, $2)`,
        [advancePair.intended.workflowName, advancePair.alternate.workflowName],
      );
      const advanceOperation = withStatefulIdentity(
        {
          ...advancePair.intended,
          ownerId: advanceClaim.ownerId,
          claimToken: advanceClaim.claimToken,
          claimGeneration: advanceClaim.claimGeneration,
          expectedPhase: 'finish_effect_recorded' as const,
          nextPhase: 'complete' as const,
        },
        advancePair.alternate,
      );
      await expect(workflowsA.advanceWorkflowTerminalization(advanceOperation.operation)).resolves.toMatchObject({
        status: 'advanced',
        record: { phase: 'complete' },
      });
      expect(advanceOperation.reads()).toEqual({ workflowName: 1, runId: 1 });
      await expect(workflowsA.getWorkflowTerminalization(advancePair.alternate)).resolves.toMatchObject({
        status: 'found',
        record: { phase: 'finish_effect_recorded' },
      });

      const releasePair = await makePair('release');
      const releaseClaim = await claim(releasePair.intended, 'release-intended');
      const alternateReleaseClaim = await claim(releasePair.alternate, 'release-alternate');
      const releaseOperation = withStatefulIdentity(
        {
          ...releasePair.intended,
          ownerId: releaseClaim.ownerId,
          claimToken: releaseClaim.claimToken,
          claimGeneration: releaseClaim.claimGeneration,
        },
        releasePair.alternate,
      );
      await expect(workflowsA.releaseWorkflowTerminalization(releaseOperation.operation)).resolves.toMatchObject({
        status: 'released',
      });
      expect(releaseOperation.reads()).toEqual({ workflowName: 1, runId: 1 });
      const releasedOwners = await pool.query<{ workflow_name: string; owner_id: string | null }>(
        `SELECT workflow_name, owner_id
         FROM mastra_workflow_terminalizations
         WHERE workflow_name IN ($1, $2)
         ORDER BY workflow_name`,
        [releasePair.intended.workflowName, releasePair.alternate.workflowName],
      );
      expect(Object.fromEntries(releasedOwners.rows.map(row => [row.workflow_name, row.owner_id]))).toEqual({
        [releasePair.intended.workflowName]: null,
        [releasePair.alternate.workflowName]: alternateReleaseClaim.ownerId,
      });

      const cleanupPair = await makePair('cleanup');
      await claim(cleanupPair.intended, 'cleanup-intended');
      await claim(cleanupPair.alternate, 'cleanup-alternate');
      await pool.query(
        `UPDATE mastra_workflow_terminalizations
         SET phase = 'complete', owner_id = NULL, claim_token = NULL,
             lease_expires_at = NULL, completed_at = updated_at
         WHERE workflow_name IN ($1, $2)`,
        [cleanupPair.intended.workflowName, cleanupPair.alternate.workflowName],
      );
      const cleanupOperation = withStatefulIdentity(
        { ...cleanupPair.intended, olderThan: new Date(Date.now() + 60_000) },
        cleanupPair.alternate,
      );
      await expect(workflowsA.deleteCompletedWorkflowTerminalizations(cleanupOperation.operation)).resolves.toEqual({
        status: 'deleted',
        count: 1,
      });
      expect(cleanupOperation.reads()).toEqual({ workflowName: 1, runId: 1 });
      await expect(workflowsA.getWorkflowTerminalization(cleanupPair.intended)).resolves.toEqual({
        status: 'missing_record',
      });
      await expect(workflowsA.getWorkflowTerminalization(cleanupPair.alternate)).resolves.toMatchObject({
        status: 'found',
        record: { phase: 'complete' },
      });
    } finally {
      await Promise.all(workflowNames.map(cleanup));
    }
  });

  it('validates claim and fence envelopes before reporting a missing PostgreSQL run', async () => {
    const missing = { workflowName: `missing-${randomUUID()}`, runId: 'missing-run' };
    await expect(
      workflowsA.claimWorkflowTerminalization({
        ...missing,
        eventKey: 'event',
        terminalStatus: 'failed',
        ownerId: 'owner',
        leaseMs: 0,
      }),
    ).rejects.toThrow(TypeError);
    const invalidFence = {
      ...missing,
      ownerId: 'owner',
      claimToken: '',
      claimGeneration: 0,
    };
    await expect(
      workflowsA.advanceWorkflowTerminalization({
        ...invalidFence,
        expectedPhase: 'finish_effect_recorded',
        nextPhase: 'complete',
      }),
    ).rejects.toThrow(TypeError);
    await expect(workflowsA.releaseWorkflowTerminalization(invalidFence)).rejects.toThrow(TypeError);
    await expect(
      workflowsA.persistWorkflowTerminalState({
        ...invalidFence,
        snapshot: { ...createEmptyWorkflowSnapshot(missing.runId), status: 'failed' },
        recoveryEnvelope: recoveryEnvelope(
          { ...createEmptyWorkflowSnapshot(missing.runId), status: 'failed' },
          missing,
          'failed',
        ),
      }),
    ).rejects.toThrow(TypeError);
  });

  it('keeps journal state isolated from rejected snapshot replacement and deletion', async () => {
    const workflowName = `terminalization-isolation-${Date.now()}`;
    const runId = 'run';
    const run = { workflowName, runId };
    await workflowsA.persistWorkflowSnapshot({ ...run, snapshot: createEmptyWorkflowSnapshot(runId) });

    try {
      const claim = await workflowsA.claimWorkflowTerminalization({
        ...run,
        eventKey: 'event',
        terminalStatus: 'failed',
        ownerId: 'worker',
        leaseMs: 1_000,
      });
      expect(claim.status).toBe('acquired');
      await expect(
        workflowsB.persistWorkflowSnapshot({ ...run, snapshot: createEmptyWorkflowSnapshot(runId) }),
      ).rejects.toThrow('Workflow parent revision conflict');
      await expect(workflowsB.loadWorkflowSnapshot(run)).resolves.toMatchObject({ status: 'pending' });
      await expect(workflowsB.getWorkflowTerminalization(run)).resolves.toMatchObject({
        status: 'found',
        record: { eventKey: 'event' },
      });

      await workflowsB.deleteWorkflowRunById(run);
      await expect(workflowsA.getWorkflowTerminalization(run)).resolves.toMatchObject({
        status: 'found',
        record: { eventKey: 'event' },
      });
    } finally {
      await cleanup(workflowName);
    }
  });

  it('exports the journal table and recovery indexes', () => {
    const ddl = WorkflowsPG.getExportDDL().join('\n');
    expect(ddl).toContain('mastra_workflow_terminalizations');
    expect(ddl).toContain('PRIMARY KEY ("workflow_name", "run_id")');
    expect(ddl).toContain('mastra_workflow_terminalizations_phase_lease_idx');
    expect(ddl).toContain('mastra_workflow_terminalizations_completed_idx');
    expect(ddl).toContain('mastra_workflow_terminal_destination_receipts_v2_lookup_idx');

    const schemaName = 'tenant_identifier';
    const customSchemaDDL = WorkflowsPG.getExportDDL(schemaName).join('\n');
    expect(customSchemaDDL).toContain(`"${schemaName}"."mastra_workflow_terminalizations"`);
    expect(customSchemaDDL).toContain('"mastra_workflow_terminalizations_phase_lease_idx"');

    const indexNames = WorkflowsPG.getDefaultIndexDefs(schemaName).map(index => index.name);
    expect(new Set(indexNames).size).toBe(indexNames.length);
    expect(indexNames.every(name => Buffer.byteLength(name, 'utf8') <= 63)).toBe(true);
  });

  it('creates both recovery indexes in a long custom schema', async () => {
    const schemaName = `tenant_identifier_${randomUUID().replaceAll('-', '').slice(0, 16)}`;
    await pool.query(`CREATE SCHEMA "${schemaName}"`);
    try {
      const workflows = new WorkflowsPG({ pool, schemaName });
      await workflows.init();
      const indexes = await pool.query<{ indexname: string }>(
        `SELECT indexname FROM pg_indexes WHERE schemaname = $1 AND tablename = $2 ORDER BY indexname`,
        [schemaName, 'mastra_workflow_terminalizations'],
      );
      expect(indexes.rows.map(row => row.indexname)).toEqual(
        expect.arrayContaining([
          'mastra_workflow_terminalizations_completed_idx',
          'mastra_workflow_terminalizations_phase_lease_idx',
        ]),
      );
    } finally {
      await pool.query(`DROP SCHEMA IF EXISTS "${schemaName}" CASCADE`);
    }
  });

  it('does not rescan or repair revision evidence after the migration marker is installed', async () => {
    const workflowName = `revision-upgrade-${randomUUID()}`;
    const runId = 'pre-upgrade-run';
    const now = new Date();
    await pool.query(
      `INSERT INTO mastra_workflow_snapshot
       (workflow_name, run_id, snapshot, "createdAt", "updatedAt", "createdAtZ", "updatedAtZ")
       VALUES ($1, $2, $3, $4, $5, $6, $7)`,
      [workflowName, runId, JSON.stringify(createEmptyWorkflowSnapshot(runId)), now, now, now, now],
    );
    try {
      await expect(
        pool.query(
          `SELECT generation FROM mastra_workflow_parent_revisions
           WHERE workflow_name = $1 AND run_id = $2`,
          [workflowName, runId],
        ),
      ).resolves.toMatchObject({ rows: [] });

      await Promise.all([workflowsA.init(), workflowsB.init()]);
      const afterInit = await pool.query<{ generation: string }>(
        `SELECT generation FROM mastra_workflow_parent_revisions
         WHERE workflow_name = $1 AND run_id = $2`,
        [workflowName, runId],
      );
      expect(afterInit.rows).toEqual([]);
      await expect(workflowsA.deleteWorkflowRunById({ workflowName, runId })).rejects.toThrow();
      const retained = await pool.query(
        `SELECT 1 FROM mastra_workflow_snapshot WHERE workflow_name = $1 AND run_id = $2`,
        [workflowName, runId],
      );
      expect(retained.rowCount).toBe(1);
    } finally {
      await cleanup(workflowName);
    }
  });

  it('enforces the receipt version and closed state matrix in fresh exported DDL', async () => {
    const schema = `r${randomUUID().replaceAll('-', '').slice(0, 4)}`;
    const table = `"${schema}"."mastra_workflow_terminal_destination_receipts_v2"`;
    const effectTable = `"${schema}"."mastra_workflow_terminal_effects_v2"`;
    await pool.query(`CREATE SCHEMA "${schema}"`);
    try {
      const ddl = WorkflowsPG.getExportDDL(schema).find(
        statement => statement.startsWith('CREATE TABLE') && statement.includes('terminal_destination_receipts'),
      );
      if (!ddl) throw new Error('receipt DDL missing');
      const effectDDL = WorkflowsPG.getExportDDL(schema).find(
        statement => statement.startsWith('CREATE TABLE') && statement.includes('terminal_effects_v2'),
      );
      if (!effectDDL) throw new Error('effect DDL missing');
      await pool.query(effectDDL);
      await pool.query(ddl);
      const insert = async (
        suffix: string,
        applicationState: string,
        dispatchState: string,
        updatedAt: number,
        appliedAt: number | null,
        dispatchPendingAt: number | null,
        destinationAppliedAt: number | null,
        quarantinedAt: number | null,
        version = 1,
      ) => {
        const effectKey = `effect-${suffix}`;
        const runId = `run-${suffix}`;
        const hash = `sha256:${'0'.repeat(64)}`;
        await pool.query(
          `INSERT INTO ${effectTable}
            (workflow_name, run_id, effect_kind, version, effect_key, source_event_key, terminal_status,
            recovery_envelope_hash, retained_record_hash, payload_hash, created_at)
           VALUES ('workflow', $1, 'workflow-finish', 1, $2, $3, 'failed', $4, $4, $4, 100)`,
          [runId, effectKey, `event-${suffix}`, hash],
        );
        return pool.query(
          `INSERT INTO ${table}
           (version, workflow_name, run_id, effect_key, consumer_id, receipt_key, effect_kind,
            producer_payload_hash, destination_hash, application_state, dispatch_state, created_at, updated_at,
            applied_at, dispatch_pending_at, destination_applied_at, quarantined_at)
           VALUES ($1, 'workflow', $2, $3, $4, $5, 'workflow-finish', 'payload', 'destination',
             $6, $7, 100, $8, $9, $10, $11, $12)`,
          [
            version,
            runId,
            effectKey,
            `consumer-${suffix}`,
            `receipt-${suffix}`,
            applicationState,
            dispatchState,
            updatedAt,
            appliedAt,
            dispatchPendingAt,
            destinationAppliedAt,
            quarantinedAt,
          ],
        );
      };

      await insert('reserved', 'reserved', 'none', 100, null, null, null, null);
      await insert('applied', 'applied', 'none', 110, 110, null, null, null);
      await insert('pending', 'applied', 'pending', 120, 110, 120, null, null);
      await insert('destination', 'applied', 'destination_applied', 130, 110, 120, 130, null);
      await insert('quarantined', 'quarantined', 'none', 140, null, null, null, 140);

      await expect(insert('bad-applied', 'applied', 'none', 110, null, null, null, null)).rejects.toThrow();
      await expect(insert('bad-pending', 'applied', 'pending', 120, 110, null, null, null)).rejects.toThrow();
      await expect(insert('predated-pending', 'applied', 'pending', 120, 90, 120, null, null)).rejects.toThrow();
      await expect(
        insert('bad-destination', 'applied', 'destination_applied', 130, 110, 120, null, null),
      ).rejects.toThrow();
      await expect(
        insert('predated-destination', 'applied', 'destination_applied', 130, 90, 120, 130, null),
      ).rejects.toThrow();
      await expect(insert('bad-quarantine', 'quarantined', 'none', 140, null, null, null, null)).rejects.toThrow();
      await expect(insert('bad-version', 'reserved', 'none', 100, null, null, null, null, 2)).rejects.toThrow();
      await expect(
        pool.query(`UPDATE ${table} SET created_at = 200 WHERE effect_key = 'effect-reserved'`),
      ).rejects.toThrow();
      await expect(
        pool.query(`UPDATE ${table} SET created_at = -1, updated_at = -1 WHERE effect_key = 'effect-reserved'`),
      ).rejects.toThrow();
    } finally {
      await pool.query(`DROP SCHEMA "${schema}" CASCADE`);
    }
  });

  it('exports the final graph-bound contract and revision schemas without obsolete raw-event plan columns', async () => {
    const schema = `p${randomUUID().replaceAll('-', '').slice(0, 4)}`;
    await pool.query(`CREATE SCHEMA "${schema}"`);
    try {
      const exportDDL = WorkflowsPG.getExportDDL(schema);
      for (const ddl of exportDDL) await pool.query(ddl);
      const tableDefinitions = new Map(
        [
          'mastra_workflow_terminal_effects_v2',
          'mastra_workflow_terminal_destination_receipts_v2',
          'mastra_workflow_terminal_continuation_plans_v2',
        ].map(table => [
          table,
          exportDDL.find(statement => statement.startsWith('CREATE TABLE') && statement.includes(`"${table}"`)),
        ]),
      );
      const revisionDDL = exportDDL.find(
        statement =>
          statement.includes('DO $mastra_workflow_parent_revision_export$') &&
          statement.includes('mastra_workflow_parent_revisions'),
      );
      for (const [table, ddl] of tableDefinitions) {
        if (!ddl) throw new Error(`${table} DDL missing`);
      }
      if (!revisionDDL) throw new Error('mastra_workflow_parent_revisions DDL missing');

      const planDDL = tableDefinitions.get('mastra_workflow_terminal_continuation_plans_v2')!;
      expect(planDDL).toContain('"contract_hash" TEXT NOT NULL');
      expect(planDDL).toContain('"contract" JSONB NOT NULL');
      expect(planDDL).toContain('"framework_action_key" TEXT');
      expect(planDDL).toContain("'continuous'");
      expect(planDDL).not.toContain('parent_result_mode');
      expect(planDDL).not.toContain('parent_iteration_index');
      expect(planDDL).not.toContain('plan_kind');
      expect(planDDL).not.toContain('targets');
      expect(planDDL).not.toContain('per-step-pause');

      expect(revisionDDL).toContain('PRIMARY KEY ("workflow_name", "run_id")');
      expect(revisionDDL).toContain('"generation" BIGINT NOT NULL');
      expect(revisionDDL).toContain('"terminal_status" TEXT');
      expect(revisionDDL).toContain("'tripwire', 'bailed'");
    } finally {
      await pool.query(`DROP SCHEMA "${schema}" CASCADE`);
    }
  });

  it('fails closed when a persisted journal row has an unknown version', async () => {
    const workflowName = `terminalization-invalid-${Date.now()}`;
    const runId = 'run';
    await pool.query(
      `INSERT INTO mastra_workflow_terminalizations
       (workflow_name, run_id, version, event_key, terminal_status, phase, owner_id, claim_token,
        claim_generation, lease_expires_at, created_at, updated_at, completed_at)
       VALUES ($1, $2, 99, 'event', 'failed', 'terminalization_pending', 'worker', 'token',
        1, 1000, 1, 1, NULL)`,
      [workflowName, runId],
    );

    try {
      await expect(workflowsA.getWorkflowTerminalization({ workflowName, runId })).rejects.toThrow(
        'Invalid workflow terminalization record',
      );
    } finally {
      await cleanup(workflowName);
    }
  });

  it('fails closed when completed timestamps contradict the persisted phase transition', async () => {
    const workflowName = `terminalization-invalid-completion-${randomUUID()}`;
    const runId = 'run';
    await pool.query(
      `INSERT INTO mastra_workflow_terminalizations
       (workflow_name, run_id, version, event_key, terminal_status, phase, owner_id, claim_token,
        claim_generation, lease_expires_at, created_at, updated_at, completed_at)
       VALUES ($1, $2, 1, 'event', 'failed', 'complete', NULL, NULL, 1, NULL, 1, 100, 50)`,
      [workflowName, runId],
    );

    try {
      await expect(workflowsA.getWorkflowTerminalization({ workflowName, runId })).rejects.toThrow(
        'Invalid workflow terminalization record',
      );
    } finally {
      await cleanup(workflowName);
    }
  });

  it('fails closed when an active lease exceeds the bounded lease contract', async () => {
    const workflowName = `terminalization-invalid-lease-${randomUUID()}`;
    const runId = 'run';
    await pool.query(
      `INSERT INTO mastra_workflow_terminalizations
       (workflow_name, run_id, version, event_key, terminal_status, phase, owner_id, claim_token,
        claim_generation, lease_expires_at, created_at, updated_at, completed_at)
       VALUES ($1, $2, 1, 'event', 'failed', 'terminalization_pending', 'worker', 'token',
        1, $3, 1, 1, NULL)`,
      [workflowName, runId, Number.MAX_SAFE_INTEGER],
    );

    try {
      await expect(workflowsA.getWorkflowTerminalization({ workflowName, runId })).rejects.toThrow(
        'Invalid workflow terminalization record',
      );
    } finally {
      await cleanup(workflowName);
    }
  });

  it('fails closed when persisted journal timestamps are ahead of database time', async () => {
    const workflowName = `terminalization-invalid-future-${randomUUID()}`;
    const runId = 'run';
    const futureUpdatedAt = Date.now() + 30 * 86_400_000;
    await pool.query(
      `INSERT INTO mastra_workflow_terminalizations
       (workflow_name, run_id, version, event_key, terminal_status, phase, owner_id, claim_token,
        claim_generation, lease_expires_at, created_at, updated_at, completed_at)
       VALUES ($1, $2, 1, 'event', 'failed', 'terminalization_pending', 'worker', 'token',
        1, $3, 1, $4, NULL)`,
      [workflowName, runId, futureUpdatedAt + 1_000, futureUpdatedAt],
    );

    try {
      await expect(workflowsA.getWorkflowTerminalization({ workflowName, runId })).rejects.toThrow(
        'Invalid workflow terminalization record',
      );
    } finally {
      await cleanup(workflowName);
    }
  });

  it('accepts exact completion and maximum lease timestamp boundaries', async () => {
    const completedWorkflow = `terminalization-valid-completion-${randomUUID()}`;
    const leaseWorkflow = `terminalization-valid-lease-${randomUUID()}`;
    const runId = 'run';
    await pool.query(
      `INSERT INTO mastra_workflow_terminalizations
       (workflow_name, run_id, version, event_key, terminal_status, phase, owner_id, claim_token,
        claim_generation, lease_expires_at, created_at, updated_at, completed_at)
       VALUES ($1, $3, 1, 'complete-event', 'failed', 'complete', NULL, NULL, 1, NULL, 1, 50, 50),
              ($2, $3, 1, 'lease-event', 'failed', 'terminalization_pending', 'worker', 'token',
               1, 86400001, 1, 1, NULL)`,
      [completedWorkflow, leaseWorkflow, runId],
    );

    try {
      await expect(
        workflowsA.getWorkflowTerminalization({ workflowName: completedWorkflow, runId }),
      ).resolves.toMatchObject({ status: 'found', record: { phase: 'complete', completedAt: 50 } });
      await expect(
        workflowsA.getWorkflowTerminalization({ workflowName: leaseWorkflow, runId }),
      ).resolves.toMatchObject({
        status: 'found',
        record: { phase: 'terminalization_pending', leaseExpiresAt: 86_400_001 },
      });
    } finally {
      await cleanup(completedWorkflow);
      await cleanup(leaseWorkflow);
    }
  });
});
