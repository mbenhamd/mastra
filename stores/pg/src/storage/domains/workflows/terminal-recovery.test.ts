import { randomUUID } from 'node:crypto';
import {
  InMemoryDB,
  WorkflowsInMemory,
  createEmptyWorkflowSnapshot,
  createWorkflowTerminalGraphFingerprint,
} from '@mastra/core/storage';
import type { WorkflowRunState, WorkflowTerminalRecoveryAncestryV1 } from '@mastra/core/workflows';
import { getWorkflowTerminalRecoveryEnvelopeHash } from '@mastra/core/workflows';
import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { createTerminalRecoveryEnvelope } from './terminalization-test-utils';
import { WorkflowsPG } from '.';

const NESTED_PARENT_GRAPH: WorkflowRunState['serializedStepGraph'] = [
  { type: 'step', step: { id: 'nested', component: 'WORKFLOW' } },
];

function createParentSnapshot(runId: string): WorkflowRunState {
  return { ...createEmptyWorkflowSnapshot(runId), serializedStepGraph: NESTED_PARENT_GRAPH };
}

describe('WorkflowsPG terminal recovery envelope parity', () => {
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

  async function cleanup(workflowNames: string[]): Promise<void> {
    for (const workflowName of workflowNames) {
      await pool.query(`DELETE FROM mastra_workflow_terminal_continuation_plans_v2 WHERE workflow_name = $1`, [
        workflowName,
      ]);
      await pool.query(`DELETE FROM mastra_workflow_terminal_destination_receipts WHERE workflow_name = $1`, [
        workflowName,
      ]);
      await pool.query(`DELETE FROM mastra_workflow_terminal_effects_v2 WHERE workflow_name = $1`, [workflowName]);
      await pool.query(`DELETE FROM mastra_workflow_terminal_snapshots_v2 WHERE workflow_name = $1`, [workflowName]);
      await pool.query(`DELETE FROM mastra_workflow_terminal_recovery_ancestries WHERE workflow_name = $1`, [
        workflowName,
      ]);
      await pool.query(`DELETE FROM mastra_workflow_terminalizations WHERE workflow_name = $1`, [workflowName]);
      await pool.query(`DELETE FROM mastra_workflow_snapshot WHERE workflow_name = $1`, [workflowName]);
      await pool.query(`DELETE FROM mastra_workflow_parent_revisions WHERE workflow_name = $1`, [workflowName]);
    }
  }

  function ancestry(
    child: { workflowName: string; runId: string },
    parent: { workflowName: string; runId: string },
    source: WorkflowTerminalRecoveryAncestryV1[0]['source'] = {
      kind: 'step',
      stepId: 'nested',
      executionPath: [0],
    },
  ): WorkflowTerminalRecoveryAncestryV1 {
    return [
      {
        version: 1,
        childWorkflowName: child.workflowName,
        childRunId: child.runId,
        parentWorkflowName: parent.workflowName,
        parentRunId: parent.runId,
        parentGraphFingerprint: createWorkflowTerminalGraphFingerprint(NESTED_PARENT_GRAPH),
        source,
        inputPointer: { kind: 'parent-source-payload', stepId: source.stepId },
        resultPointer: { kind: 'retained-terminal-result', workflowName: child.workflowName, runId: child.runId },
        resumeMetadata: { wasResume: false, resumeSteps: [] },
      },
    ];
  }

  async function claim(
    workflows: WorkflowsPG,
    run: { workflowName: string; runId: string },
    terminalStatus: 'success' | 'failed' | 'canceled' = 'failed',
  ) {
    const result = await workflows.claimWorkflowTerminalization({
      ...run,
      eventKey: `${run.runId}-terminal`,
      terminalStatus,
      ownerId: `${run.runId}-owner`,
      leaseMs: 10_000,
    });
    if (result.status !== 'acquired') throw new Error(`Expected acquired, received ${result.status}`);
    return {
      ...run,
      ownerId: result.record.ownerId,
      claimToken: result.record.claimToken,
      claimGeneration: result.record.claimGeneration,
    };
  }

  function nestedAdmission(
    parent: { workflowName: string; runId: string },
    child: { workflowName: string; runId: string },
  ) {
    return {
      ...parent,
      stepId: 'nested',
      nestedWorkflowName: child.workflowName,
      nestedRunId: child.runId,
      result: { status: 'running' as const, payload: {} },
      requestContext: {},
      recoveryAncestry: ancestry(child, parent),
    };
  }

  it('atomically admits graph-bound ownership and ancestry with read-only exact replay', async () => {
    const suffix = randomUUID();
    const parent = { workflowName: `admit-parent-${suffix}`, runId: 'parent-run' };
    const child = { workflowName: `admit-child-${suffix}`, runId: 'child-run' };
    const parentGraph: WorkflowRunState['serializedStepGraph'] = [
      { type: 'step', step: { id: 'nested', component: 'WORKFLOW' } },
    ];
    await workflowsA.persistWorkflowSnapshot({
      ...parent,
      snapshot: {
        ...createEmptyWorkflowSnapshot(parent.runId),
        serializedStepGraph: parentGraph,
        context: {
          nested: { status: 'running', payload: {}, metadata: {} },
        } as WorkflowRunState['context'],
      },
    });
    const childAncestry = ancestry(child, parent);
    childAncestry[0]!.parentGraphFingerprint = createWorkflowTerminalGraphFingerprint(parentGraph);
    const admission = {
      ...parent,
      stepId: 'nested',
      nestedWorkflowName: child.workflowName,
      nestedRunId: child.runId,
      result: { status: 'running' as const, payload: {} },
      requestContext: {},
      recoveryAncestry: childAncestry,
    };
    try {
      await expect(workflowsA.admitWorkflowNestedRun(admission)).resolves.toMatchObject({ status: 'admitted' });
      const before = await pool.query(
        `SELECT generation FROM mastra_workflow_parent_revisions WHERE workflow_name = $1 AND run_id = $2`,
        [parent.workflowName, parent.runId],
      );
      await expect(workflowsB.admitWorkflowNestedRun(admission)).resolves.toMatchObject({
        status: 'already_admitted',
      });
      const after = await pool.query(
        `SELECT generation FROM mastra_workflow_parent_revisions WHERE workflow_name = $1 AND run_id = $2`,
        [parent.workflowName, parent.runId],
      );
      expect(after.rows[0]?.generation).toBe(before.rows[0]?.generation);

      const conflictingChild = { workflowName: `other-child-${suffix}`, runId: 'other-run' };
      const conflictingAncestry = ancestry(conflictingChild, parent);
      conflictingAncestry[0]!.parentGraphFingerprint = createWorkflowTerminalGraphFingerprint(parentGraph);
      await expect(
        workflowsB.admitWorkflowNestedRun({
          ...admission,
          nestedWorkflowName: conflictingChild.workflowName,
          nestedRunId: conflictingChild.runId,
          recoveryAncestry: conflictingAncestry,
        }),
      ).resolves.toEqual({ status: 'ownership_conflict' });
      await expect(workflowsA.getWorkflowTerminalRecoveryAncestry(conflictingChild)).resolves.toEqual({
        status: 'missing_ancestry',
      });

      const forgedChild = { workflowName: `forged-child-${suffix}`, runId: 'forged-run' };
      const forgedAncestry = ancestry(forgedChild, parent);
      forgedAncestry[0]!.parentGraphFingerprint = createWorkflowTerminalGraphFingerprint([]);
      await expect(
        workflowsA.admitWorkflowNestedRun({
          ...admission,
          nestedWorkflowName: forgedChild.workflowName,
          nestedRunId: forgedChild.runId,
          recoveryAncestry: forgedAncestry,
        }),
      ).rejects.toThrow('does not match serialized parent graph');
      await expect(workflowsA.getWorkflowTerminalRecoveryAncestry(forgedChild)).resolves.toEqual({
        status: 'missing_ancestry',
      });
    } finally {
      await cleanup([parent.workflowName, child.workflowName, `other-child-${suffix}`, `forged-child-${suffix}`]);
    }
  });

  it('rolls back ancestry when the atomic parent ownership update fails', async () => {
    const suffix = randomUUID();
    const sqlSuffix = suffix.replaceAll('-', '');
    const parent = { workflowName: `rollback-parent-${suffix}`, runId: 'parent-run' };
    const child = { workflowName: `rollback-child-${suffix}`, runId: 'child-run' };
    const triggerName = `pf1782_admission_${sqlSuffix}`;
    const functionName = `pf1782_admission_fail_${sqlSuffix}`;
    const parentGraph: WorkflowRunState['serializedStepGraph'] = [
      { type: 'step', step: { id: 'nested', component: 'WORKFLOW' } },
    ];
    await workflowsA.persistWorkflowSnapshot({
      ...parent,
      snapshot: {
        ...createEmptyWorkflowSnapshot(parent.runId),
        serializedStepGraph: parentGraph,
        context: {
          nested: { status: 'running', payload: {}, metadata: {} },
        } as WorkflowRunState['context'],
      },
    });
    const childAncestry = ancestry(child, parent);
    childAncestry[0]!.parentGraphFingerprint = createWorkflowTerminalGraphFingerprint(parentGraph);
    await pool.query(
      `CREATE FUNCTION ${functionName}() RETURNS trigger LANGUAGE plpgsql AS
       $$ BEGIN RAISE EXCEPTION 'forced admission rollback'; END $$`,
    );
    await pool.query(
      `CREATE TRIGGER ${triggerName} BEFORE UPDATE ON mastra_workflow_snapshot
       FOR EACH ROW WHEN (NEW.workflow_name = '${parent.workflowName}')
       EXECUTE FUNCTION ${functionName}()`,
    );
    try {
      await expect(
        workflowsA.admitWorkflowNestedRun({
          ...parent,
          stepId: 'nested',
          nestedWorkflowName: child.workflowName,
          nestedRunId: child.runId,
          result: { status: 'running', payload: {} },
          requestContext: {},
          recoveryAncestry: childAncestry,
        }),
      ).rejects.toThrow('forced admission rollback');
      await expect(workflowsA.getWorkflowTerminalRecoveryAncestry(child)).resolves.toEqual({
        status: 'missing_ancestry',
      });
      await expect(workflowsA.loadWorkflowSnapshot(parent)).resolves.not.toHaveProperty(
        'context.nested.metadata.nestedRunId',
      );
    } finally {
      await pool.query(`DROP TRIGGER IF EXISTS ${triggerName} ON mastra_workflow_snapshot`);
      await pool.query(`DROP FUNCTION IF EXISTS ${functionName}()`);
      await cleanup([child.workflowName, parent.workflowName]);
    }
  });

  it('materializes identical rich recovery evidence in PostgreSQL and InMemory', async () => {
    const suffix = randomUUID();
    const memory = new WorkflowsInMemory({ db: new InMemoryDB() });
    const parent = { workflowName: `parity-parent-${suffix}`, runId: 'parent-run' };
    const child = { workflowName: `parity-child-${suffix}`, runId: 'child-run' };
    const childGraph: WorkflowRunState['serializedStepGraph'] = [
      { type: 'step', step: { id: 'leaf', component: 'STEP' } },
    ];
    const childAncestry = ancestry(child, parent);
    const stores = [memory, workflowsA] as const;
    try {
      for (const store of stores) {
        await store.persistWorkflowSnapshot({ ...parent, snapshot: createParentSnapshot(parent.runId) });
        await store.persistWorkflowSnapshot({
          ...child,
          snapshot: { ...createEmptyWorkflowSnapshot(child.runId), serializedStepGraph: childGraph },
        });
        await expect(
          store.persistWorkflowTerminalRecoveryAncestry({ ...child, ancestry: childAncestry }),
        ).resolves.toMatchObject({ status: 'persisted' });
      }

      const fences = await Promise.all(
        stores.map(async (store, index) => {
          const claimed = await store.claimWorkflowTerminalization({
            ...child,
            eventKey: 'parity-terminal-event',
            terminalStatus: 'failed',
            ownerId: `parity-owner-${index}`,
            leaseMs: 10_000,
          });
          if (claimed.status !== 'acquired') throw new Error(`Expected acquired, received ${claimed.status}`);
          return {
            ...child,
            ownerId: claimed.record.ownerId,
            claimToken: claimed.record.claimToken,
            claimGeneration: claimed.record.claimGeneration,
          };
        }),
      );
      const snapshot: WorkflowRunState = {
        ...createEmptyWorkflowSnapshot(child.runId),
        status: 'failed',
        serializedStepGraph: childGraph,
        context: { __state: { exact: ['state', 1] } } as unknown as WorkflowRunState['context'],
        value: { stale: 'must-be-replaced' },
        requestContext: { tenant: 'tenant-a' },
      };
      const recoveryEnvelope = createTerminalRecoveryEnvelope({
        ...child,
        snapshot,
        terminalStatus: 'failed',
        ancestry: childAncestry,
        terminalResult: {
          status: 'failed',
          error: new Error('parity failure', { cause: new Error('parity cause') }),
        },
      });
      for (const [index, store] of stores.entries()) {
        await expect(
          store.persistWorkflowTerminalState({ ...fences[index]!, snapshot, recoveryEnvelope }),
        ).resolves.toMatchObject({ status: 'persisted' });
        await expect(
          store.prepareWorkflowTerminalEffect({
            ...fences[index]!,
            expectedPhase: 'run_state_persisted',
            effect: {
              kind: 'parent-workflow-step-end',
              parentWorkflowName: parent.workflowName,
              parentRunId: parent.runId,
              parentStepId: 'nested',
              parentExecutionPath: [0],
            },
          }),
        ).resolves.toMatchObject({ status: 'prepared' });
      }
      const recovered = await Promise.all(
        stores.map((store, index) =>
          store.getWorkflowTerminalEffectForDispatch({
            ...fences[index]!,
            kind: 'parent-workflow-step-end',
          }),
        ),
      );
      expect(recovered[0]).toMatchObject({ status: 'found' });
      expect(recovered[1]).toMatchObject({ status: 'found' });
      if (recovered[0].status !== 'found' || recovered[1].status !== 'found') {
        throw new Error('Expected recovered terminal evidence');
      }
      expect(recovered[1].recovery).toMatchObject({
        envelopeHash: recovered[0].recovery.envelopeHash,
        envelope: recovered[0].recovery.envelope,
      });
      expect(recovered[1].effect).toMatchObject({
        effectKey: recovered[0].effect.effectKey,
        payloadHash: recovered[0].effect.payloadHash,
        recoveryEnvelopeHash: recovered[0].effect.recoveryEnvelopeHash,
      });
    } finally {
      await cleanup([child.workflowName, parent.workflowName]);
    }
  });

  it('persists immutable pre-terminal ancestry and an authenticated native-Error envelope without invoking toJSON', async () => {
    const suffix = randomUUID();
    const parent = { workflowName: `recovery-parent-${suffix}`, runId: 'parent-run' };
    const child = { workflowName: `recovery-child-${suffix}`, runId: 'child-run' };
    const childAncestry = ancestry(child, parent);
    const error = new Error('native terminal failure', { cause: new Error('native cause') });

    await workflowsA.persistWorkflowSnapshot({ ...parent, snapshot: createParentSnapshot(parent.runId) });
    await workflowsA.persistWorkflowSnapshot({ ...child, snapshot: createEmptyWorkflowSnapshot(child.runId) });
    try {
      const persisted = await Promise.all([
        workflowsA.persistWorkflowTerminalRecoveryAncestry({ ...child, ancestry: childAncestry }),
        workflowsB.persistWorkflowTerminalRecoveryAncestry({ ...child, ancestry: childAncestry }),
      ]);
      expect(persisted.map(result => result.status).sort()).toEqual(['already_persisted', 'persisted']);
      await expect(workflowsA.getWorkflowTerminalRecoveryAncestry(child)).resolves.toMatchObject({
        status: 'found',
        record: {
          workflowName: child.workflowName,
          runId: child.runId,
          ancestryHash: expect.stringMatching(/^sha256:[a-f0-9]{64}$/),
          ancestry: childAncestry,
        },
      });
      await expect(
        workflowsB.persistWorkflowTerminalRecoveryAncestry({
          ...child,
          ancestry: ancestry(child, { ...parent, runId: 'other-parent-run' }),
        }),
      ).resolves.toEqual({ status: 'ancestry_conflict' });

      const fence = await claim(workflowsA, child);
      const snapshot: WorkflowRunState = {
        ...createEmptyWorkflowSnapshot(child.runId),
        status: 'failed',
        context: { __state: { retained: true } } as unknown as WorkflowRunState['context'],
      };
      const toJSON = vi.fn(() => ({ forged: true }));
      await expect(
        workflowsA.persistWorkflowTerminalState({
          ...fence,
          snapshot,
          recoveryEnvelope: createTerminalRecoveryEnvelope({
            ...child,
            snapshot,
            terminalStatus: 'failed',
            ancestry: childAncestry,
            terminalResult: { status: 'failed', error: { message: 'must reject', toJSON } },
          }),
        }),
      ).resolves.toEqual({ status: 'invalid_recovery_envelope' });
      expect(toJSON).not.toHaveBeenCalled();
      const recoveryEnvelope = createTerminalRecoveryEnvelope({
        ...child,
        snapshot,
        terminalStatus: 'failed',
        ancestry: childAncestry,
        terminalResult: { status: 'failed', error },
      });
      await expect(
        workflowsA.persistWorkflowTerminalState({ ...fence, snapshot, recoveryEnvelope }),
      ).resolves.toMatchObject({ status: 'persisted', record: { phase: 'run_state_persisted' } });

      const retained = await pool.query<{ envelope_hash: `sha256:${string}`; envelope: unknown }>(
        `SELECT envelope_hash, envelope FROM mastra_workflow_terminal_snapshots_v2
         WHERE workflow_name = $1 AND run_id = $2`,
        [child.workflowName, child.runId],
      );
      expect(retained.rows[0]?.envelope_hash).toBe(
        getWorkflowTerminalRecoveryEnvelopeHash(retained.rows[0]!.envelope as never),
      );
      expect(retained.rows[0]?.envelope).toMatchObject({
        terminalResult: {
          status: 'failed',
          error: { name: 'Error', message: 'native terminal failure', cause: { message: 'native cause' } },
        },
      });

      await pool.query(
        `UPDATE mastra_workflow_terminal_snapshots_v2
         SET envelope = jsonb_set(envelope, '{finalState,retained}', 'false'::jsonb)
         WHERE workflow_name = $1 AND run_id = $2`,
        [child.workflowName, child.runId],
      );
      await expect(
        workflowsA.prepareWorkflowTerminalEffect({
          ...fence,
          expectedPhase: 'run_state_persisted',
          effect: {
            kind: 'parent-workflow-step-end',
            parentWorkflowName: parent.workflowName,
            parentRunId: parent.runId,
            parentStepId: 'nested',
            parentExecutionPath: [0],
          },
        }),
      ).rejects.toThrow('Invalid workflow terminal snapshot record');
    } finally {
      await cleanup([child.workflowName, parent.workflowName]);
    }
  });

  it('atomically assigns one nested owner per foreach iteration without losing sibling ownership', async () => {
    const workflowName = `ownership-${randomUUID()}`;
    const childWorkflowName = `each-${workflowName}`;
    const run = { workflowName, runId: 'parent-run' };
    const snapshot: WorkflowRunState = {
      ...createEmptyWorkflowSnapshot(run.runId),
      status: 'running',
      context: {
        each: {
          status: 'running',
          payload: ['a', 'b'],
          output: [null, null],
          metadata: {},
          startedAt: Date.now(),
        },
      } as unknown as WorkflowRunState['context'],
      serializedStepGraph: [{ type: 'foreach', step: { id: 'each', component: 'WORKFLOW' }, opts: { concurrency: 2 } }],
    };
    await workflowsA.persistWorkflowSnapshot({ ...run, snapshot });
    try {
      const admission = (forEachIndex: number, nestedRunId: string) => {
        const child = { workflowName: childWorkflowName, runId: nestedRunId };
        return {
          ...run,
          stepId: 'each',
          nestedWorkflowName: child.workflowName,
          nestedRunId,
          forEachIndex,
          result: { status: 'running' as const, payload: ['a', 'b'], output: [null, null], startedAt: 1 },
          requestContext: { ownership: true },
          recoveryAncestry: [
            {
              version: 1 as const,
              childWorkflowName: child.workflowName,
              childRunId: child.runId,
              parentWorkflowName: run.workflowName,
              parentRunId: run.runId,
              parentGraphFingerprint: createWorkflowTerminalGraphFingerprint(snapshot.serializedStepGraph),
              source: {
                kind: 'foreach-iteration' as const,
                stepId: 'each',
                containerPath: [0],
                iterationIndex: forEachIndex,
              },
              inputPointer: { kind: 'parent-source-payload' as const, stepId: 'each' },
              resultPointer: {
                kind: 'retained-terminal-result' as const,
                workflowName: child.workflowName,
                runId: child.runId,
              },
              resumeMetadata: { wasResume: false, resumeSteps: [] },
            },
          ],
        };
      };
      const concurrent = await Promise.all([
        workflowsA.admitWorkflowNestedRun(admission(0, 'child-a')),
        workflowsB.admitWorkflowNestedRun(admission(0, 'child-b')),
      ]);
      expect(concurrent.map(result => result.status).sort()).toEqual(['admitted', 'ownership_conflict']);
      const winner = concurrent.find(result => result.status === 'admitted');
      if (winner?.status !== 'admitted') throw new Error('Expected one ownership winner');
      const winnerId = (winner.stepResults.each as any).metadata.__workflow_meta.iterationRunIds['0'];
      await expect(workflowsA.admitWorkflowNestedRun(admission(0, winnerId))).resolves.toMatchObject({
        status: 'already_admitted',
      });
      await expect(workflowsB.admitWorkflowNestedRun(admission(1, 'child-c'))).resolves.toMatchObject({
        status: 'admitted',
      });
      const loserId = winnerId === 'child-a' ? 'child-b' : 'child-a';
      await expect(
        workflowsA.getWorkflowTerminalRecoveryAncestry({ workflowName: childWorkflowName, runId: loserId }),
      ).resolves.toEqual({ status: 'missing_ancestry' });
      await expect(workflowsA.loadWorkflowSnapshot(run)).resolves.toMatchObject({
        context: {
          each: {
            metadata: {
              __workflow_meta: { iterationRunIds: { '0': winnerId, '1': 'child-c' } },
            },
          },
        },
        requestContext: { ownership: true },
      });
    } finally {
      await cleanup([childWorkflowName, workflowName]);
    }
  });

  it('retains a completed recovery root while a recursively linked descendant lacks completed evidence', async () => {
    const suffix = randomUUID();
    const root = { workflowName: `cleanup-root-${suffix}`, runId: 'root-run' };
    const child = { workflowName: `cleanup-child-${suffix}`, runId: 'child-run' };
    const grandchild = { workflowName: `cleanup-grandchild-${suffix}`, runId: 'grandchild-run' };
    for (const run of [root, child, grandchild]) {
      await workflowsA.persistWorkflowSnapshot({ ...run, snapshot: createParentSnapshot(run.runId) });
    }
    try {
      await workflowsA.persistWorkflowTerminalRecoveryAncestry({ ...child, ancestry: ancestry(child, root) });
      await workflowsA.persistWorkflowTerminalRecoveryAncestry({
        ...grandchild,
        ancestry: [...ancestry(grandchild, child), ...ancestry(child, root)],
      });
      for (const run of [root, child]) {
        await claim(workflowsA, run);
      }
      await pool.query(
        `UPDATE mastra_workflow_terminalizations
         SET phase = 'complete', owner_id = NULL, claim_token = NULL, lease_expires_at = NULL,
             completed_at = updated_at
         WHERE workflow_name IN ($1, $2)`,
        [root.workflowName, child.workflowName],
      );

      await expect(
        workflowsA.deleteCompletedWorkflowTerminalizations({
          ...root,
          olderThan: new Date(Date.now() + 60_000),
        }),
      ).resolves.toEqual({ status: 'deleted', count: 0 });

      await claim(workflowsA, grandchild);
      await pool.query(
        `UPDATE mastra_workflow_terminalizations
         SET phase = 'complete', owner_id = NULL, claim_token = NULL, lease_expires_at = NULL,
             completed_at = updated_at
         WHERE workflow_name = $1 AND run_id = $2`,
        [grandchild.workflowName, grandchild.runId],
      );
      await expect(
        workflowsA.deleteCompletedWorkflowTerminalizations({
          ...root,
          olderThan: new Date(Date.now() + 60_000),
        }),
      ).resolves.toEqual({ status: 'deleted', count: 1 });
    } finally {
      await cleanup([grandchild.workflowName, child.workflowName, root.workflowName]);
    }
  });

  it('serializes completed-parent cleanup against pre-terminal ancestry admission', async () => {
    const suffix = randomUUID();
    const parent = { workflowName: `cleanup-race-parent-${suffix}`, runId: 'parent-run' };
    const child = { workflowName: `cleanup-race-child-${suffix}`, runId: 'child-run' };
    await workflowsA.persistWorkflowSnapshot({ ...parent, snapshot: createParentSnapshot(parent.runId) });
    await workflowsA.persistWorkflowSnapshot({ ...child, snapshot: createEmptyWorkflowSnapshot(child.runId) });
    try {
      const fence = await claim(workflowsA, parent);
      const snapshot: WorkflowRunState = {
        ...createParentSnapshot(parent.runId),
        status: 'failed',
        context: { __state: { terminal: true } } as unknown as WorkflowRunState['context'],
        value: { terminal: 'true' },
      };
      await expect(
        workflowsA.persistWorkflowTerminalState({
          ...fence,
          snapshot,
          recoveryEnvelope: createTerminalRecoveryEnvelope({
            ...parent,
            snapshot,
            terminalStatus: 'failed',
          }),
        }),
      ).resolves.toMatchObject({ status: 'persisted' });
      await pool.query(
        `UPDATE mastra_workflow_terminalizations
         SET phase = 'complete', owner_id = NULL, claim_token = NULL, lease_expires_at = NULL,
             updated_at = floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint,
             completed_at = floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint
         WHERE workflow_name = $1 AND run_id = $2`,
        [parent.workflowName, parent.runId],
      );

      const [cleanupResult, ancestryResult] = await Promise.allSettled([
        workflowsA.deleteCompletedWorkflowTerminalizations({
          ...parent,
          olderThan: new Date(Date.now() + 60_000),
        }),
        workflowsB.persistWorkflowTerminalRecoveryAncestry({ ...child, ancestry: ancestry(child, parent) }),
      ]);
      expect(cleanupResult.status).toBe('fulfilled');
      if (cleanupResult.status !== 'fulfilled') throw cleanupResult.reason;
      if (cleanupResult.value.count === 0) {
        expect(ancestryResult).toMatchObject({
          status: 'fulfilled',
          value: { status: expect.stringMatching(/^(persisted|already_persisted)$/) },
        });
      } else {
        expect(cleanupResult.value).toEqual({ status: 'deleted', count: 1 });
        expect(ancestryResult.status).toBe('rejected');
        const count = await pool.query<{ count: string }>(
          `SELECT count(*)::text AS count FROM mastra_workflow_terminal_recovery_ancestries
           WHERE workflow_name = $1 AND run_id = $2`,
          [child.workflowName, child.runId],
        );
        expect(count.rows[0]?.count).toBe('0');
      }
    } finally {
      await cleanup([child.workflowName, parent.workflowName]);
    }
  });

  it.each(['success', 'failed', 'canceled'] as const)(
    'rejects late atomic admission after cleanup of a %s parent without writes',
    async terminalStatus => {
      const suffix = randomUUID();
      const parent = { workflowName: `late-${terminalStatus}-parent-${suffix}`, runId: 'parent-run' };
      const child = { workflowName: `late-${terminalStatus}-child-${suffix}`, runId: 'child-run' };
      try {
        await workflowsA.persistWorkflowSnapshot({ ...parent, snapshot: createParentSnapshot(parent.runId) });
        const fence = await claim(workflowsA, parent, terminalStatus);
        const snapshot: WorkflowRunState = {
          ...createParentSnapshot(parent.runId),
          status: terminalStatus,
          context: { __state: { terminal: true } } as WorkflowRunState['context'],
          value: { terminal: true },
        };
        await workflowsA.persistWorkflowTerminalState({
          ...fence,
          snapshot,
          recoveryEnvelope: createTerminalRecoveryEnvelope({ ...parent, snapshot, terminalStatus }),
        });
        await pool.query(
          `UPDATE mastra_workflow_terminalizations
           SET phase = 'complete', owner_id = NULL, claim_token = NULL, lease_expires_at = NULL,
               completed_at = updated_at
           WHERE workflow_name = $1 AND run_id = $2`,
          [parent.workflowName, parent.runId],
        );
        await expect(
          workflowsA.deleteCompletedWorkflowTerminalizations({
            ...parent,
            olderThan: new Date(Date.now() + 60_000),
          }),
        ).resolves.toEqual({ status: 'deleted', count: 1 });
        const before = JSON.stringify(await workflowsA.loadWorkflowSnapshot(parent));
        const revisionBefore = await pool.query<{ generation: string }>(
          `SELECT generation::text FROM mastra_workflow_parent_revisions
           WHERE workflow_name = $1 AND run_id = $2`,
          [parent.workflowName, parent.runId],
        );

        await expect(workflowsB.admitWorkflowNestedRun(nestedAdmission(parent, child))).resolves.toEqual({
          status: 'parent_terminal',
        });
        expect(JSON.stringify(await workflowsA.loadWorkflowSnapshot(parent))).toBe(before);
        const revisionAfter = await pool.query<{ generation: string }>(
          `SELECT generation::text FROM mastra_workflow_parent_revisions
           WHERE workflow_name = $1 AND run_id = $2`,
          [parent.workflowName, parent.runId],
        );
        expect(revisionAfter.rows).toEqual(revisionBefore.rows);
        const childAncestry = await pool.query<{ count: string }>(
          `SELECT count(*)::text AS count FROM mastra_workflow_terminal_recovery_ancestries
           WHERE workflow_name = $1 AND run_id = $2`,
          [child.workflowName, child.runId],
        );
        expect(childAncestry.rows[0]?.count).toBe('0');
      } finally {
        await cleanup([child.workflowName, parent.workflowName]);
      }
    },
  );

  it('rejects admission for a missing parent without retaining a revision row', async () => {
    const suffix = randomUUID();
    const parent = { workflowName: `missing-parent-${suffix}`, runId: 'parent-run' };
    const child = { workflowName: `missing-child-${suffix}`, runId: 'child-run' };
    try {
      await expect(workflowsA.admitWorkflowNestedRun(nestedAdmission(parent, child))).resolves.toEqual({
        status: 'missing_run',
      });
      const revision = await pool.query<{ count: string }>(
        `SELECT count(*)::text AS count FROM mastra_workflow_parent_revisions
         WHERE workflow_name = $1 AND run_id = $2`,
        [parent.workflowName, parent.runId],
      );
      expect(revision.rows[0]?.count).toBe('0');
    } finally {
      await cleanup([child.workflowName, parent.workflowName]);
    }
  });

  it('retains a completed parent recovery root when atomic admission wins first', async () => {
    const suffix = randomUUID();
    const parent = { workflowName: `admission-first-parent-${suffix}`, runId: 'parent-run' };
    const child = { workflowName: `admission-first-child-${suffix}`, runId: 'child-run' };
    try {
      await workflowsA.persistWorkflowSnapshot({ ...parent, snapshot: createParentSnapshot(parent.runId) });
      await expect(workflowsA.admitWorkflowNestedRun(nestedAdmission(parent, child))).resolves.toMatchObject({
        status: 'admitted',
      });
      const fence = await claim(workflowsA, parent);
      const current = await workflowsA.loadWorkflowSnapshot(parent);
      if (!current) throw new Error('Expected admitted parent snapshot');
      const terminal: WorkflowRunState = {
        ...current,
        status: 'failed',
        context: { ...current.context, __state: { terminal: true } },
        value: { terminal: true },
      };
      await workflowsA.persistWorkflowTerminalState({
        ...fence,
        snapshot: terminal,
        recoveryEnvelope: createTerminalRecoveryEnvelope({ ...parent, snapshot: terminal, terminalStatus: 'failed' }),
      });
      await pool.query(
        `UPDATE mastra_workflow_terminalizations
         SET phase = 'complete', owner_id = NULL, claim_token = NULL, lease_expires_at = NULL,
             completed_at = updated_at
         WHERE workflow_name = $1 AND run_id = $2`,
        [parent.workflowName, parent.runId],
      );

      await expect(
        workflowsB.deleteCompletedWorkflowTerminalizations({
          ...parent,
          olderThan: new Date(Date.now() + 60_000),
        }),
      ).resolves.toEqual({ status: 'deleted', count: 0 });
      const retained = await pool.query<{ count: string }>(
        `SELECT count(*)::text AS count FROM mastra_workflow_terminal_recovery_ancestries
         WHERE workflow_name = $1 AND run_id = $2`,
        [child.workflowName, child.runId],
      );
      expect(retained.rows[0]?.count).toBe('1');
    } finally {
      await cleanup([child.workflowName, parent.workflowName]);
    }
  });

  it('exports, initializes, and clears recovery ancestry evidence in a custom schema', async () => {
    const schemaName = `recovery_${randomUUID().replaceAll('-', '').slice(0, 12)}`;
    const ddl = WorkflowsPG.getExportDDL(schemaName).join('\n');
    expect(ddl).toContain(`"${schemaName}"."mastra_workflow_terminal_recovery_ancestries"`);
    expect(ddl).toContain('"ancestry_hash" TEXT NOT NULL');
    expect(ddl).toContain('"immediate_parent_workflow_name" TEXT');
    expect(ddl).toContain('"immediate_parent_run_id" TEXT');

    await pool.query(`CREATE SCHEMA "${schemaName}"`);
    await pool.query(`CREATE TABLE "${schemaName}"."mastra_workflow_terminal_effects" (effect_key TEXT UNIQUE)`);
    await pool.query(`CREATE TABLE "${schemaName}"."mastra_workflow_terminal_snapshots" (legacy_shape TEXT)`);
    await pool.query(
      `CREATE TABLE "${schemaName}"."mastra_workflow_terminal_continuation_plans" (
         effect_key TEXT REFERENCES "${schemaName}"."mastra_workflow_terminal_effects" (effect_key)
       )`,
    );
    const workflows = new WorkflowsPG({ pool, schemaName });
    try {
      await workflows.init();
      const versionedTables = await pool.query<{ table_name: string }>(
        `SELECT table_name FROM information_schema.tables
         WHERE table_schema = $1 AND table_name IN ($2, $3, $4)`,
        [
          schemaName,
          'mastra_workflow_terminal_continuation_plans_v2',
          'mastra_workflow_terminal_effects_v2',
          'mastra_workflow_terminal_snapshots_v2',
        ],
      );
      expect(versionedTables.rows.map(row => row.table_name).sort()).toEqual([
        'mastra_workflow_terminal_continuation_plans_v2',
        'mastra_workflow_terminal_effects_v2',
        'mastra_workflow_terminal_snapshots_v2',
      ]);
      const planForeignKeys = await pool.query<{ definition: string }>(
        `SELECT pg_get_constraintdef(constraint_row.oid) AS definition
         FROM pg_constraint AS constraint_row
         JOIN pg_class AS table_row ON table_row.oid = constraint_row.conrelid
         JOIN pg_namespace AS namespace_row ON namespace_row.oid = table_row.relnamespace
         WHERE namespace_row.nspname = $1
           AND table_row.relname = 'mastra_workflow_terminal_continuation_plans_v2'
           AND constraint_row.contype = 'f'`,
        [schemaName],
      );
      expect(planForeignKeys.rows.map(row => row.definition).join('\n')).toContain(
        'mastra_workflow_terminal_effects_v2',
      );
      const indexes = await pool.query<{ indexname: string }>(
        `SELECT indexname FROM pg_indexes WHERE schemaname = $1 AND tablename = $2`,
        [schemaName, 'mastra_workflow_terminal_recovery_ancestries'],
      );
      expect(indexes.rows.map(row => row.indexname)).toContain(
        'mastra_workflow_terminal_recovery_ancestries_parent_idx',
      );
      const parent = { workflowName: 'custom-parent', runId: 'parent-run' };
      const child = { workflowName: 'custom-child', runId: 'child-run' };
      await workflows.persistWorkflowSnapshot({ ...parent, snapshot: createParentSnapshot(parent.runId) });
      await workflows.persistWorkflowSnapshot({ ...child, snapshot: createEmptyWorkflowSnapshot(child.runId) });
      await expect(
        workflows.persistWorkflowTerminalRecoveryAncestry({ ...child, ancestry: ancestry(child, parent) }),
      ).resolves.toMatchObject({ status: 'persisted' });
      const stored = await pool.query<{ immediate_parent_workflow_name: string; immediate_parent_run_id: string }>(
        `SELECT immediate_parent_workflow_name, immediate_parent_run_id
         FROM "${schemaName}"."mastra_workflow_terminal_recovery_ancestries"`,
      );
      expect(stored.rows).toEqual([
        { immediate_parent_workflow_name: parent.workflowName, immediate_parent_run_id: parent.runId },
      ]);
      await workflows.dangerouslyClearAll();
      const count = await pool.query<{ count: string }>(
        `SELECT count(*)::text AS count
         FROM "${schemaName}"."mastra_workflow_terminal_recovery_ancestries"`,
      );
      expect(count.rows[0]?.count).toBe('0');
    } finally {
      await pool.query(`DROP SCHEMA "${schemaName}" CASCADE`);
    }
  });
});
