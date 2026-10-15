import { randomUUID } from 'node:crypto';
import { createEmptyWorkflowSnapshot, createWorkflowTerminalGraphFingerprint } from '@mastra/core/storage';
import type { WorkflowRunState } from '@mastra/core/workflows';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { PostgresStore, WorkflowsPG } from '../../index';
import { createTerminalRecoveryEnvelope } from './terminalization-test-utils';

const connectionString = process.env.DB_URL || 'postgresql://postgres:postgres@localhost:5434/mastra';

type Run = { workflowName: string; runId: string };

const CLOSURE_TABLES = [
  'mastra_workflow_snapshot',
  'mastra_workflow_snapshot_handoffs',
  'mastra_workflow_terminalizations',
  'mastra_workflow_terminal_effects_v2',
  'mastra_workflow_terminal_snapshots_v2',
  'mastra_workflow_terminal_destination_receipts_v2',
  'mastra_workflow_terminal_continuation_plans_v2',
  'mastra_workflow_terminal_recovery_ancestries',
] as const;

describe('WorkflowsPG run closure delete', () => {
  let store: PostgresStore;
  let workflows: WorkflowsPG;
  let resource: string;

  beforeAll(async () => {
    store = new PostgresStore({ id: `workflow-run-closure-${randomUUID()}`, connectionString });
    await store.init();
    workflows = (await store.getStore('workflows'))!;
  }, 60000);

  afterAll(async () => {
    await store?.close();
  });

  const newRun = (): Run => ({ workflowName: `research-${randomUUID()}`, runId: randomUUID() });

  /** Row counts per closure table for the run; the parent revision is reported separately. */
  async function closureRows(run: Run): Promise<Record<string, number>> {
    const counts: Record<string, number> = {};
    for (const table of CLOSURE_TABLES) {
      const row = await store.db.one<{ count: string }>(
        `SELECT count(*)::text AS count FROM ${table} WHERE workflow_name = $1 AND run_id = $2`,
        [run.workflowName, run.runId],
      );
      counts[table] = Number(row.count);
    }
    return counts;
  }

  const noRows = Object.fromEntries(CLOSURE_TABLES.map(table => [table, 0]));

  const parentRevision = (run: Run) =>
    store.db.oneOrNone<{ generation: string }>(
      `SELECT generation::text FROM mastra_workflow_parent_revisions WHERE workflow_name = $1 AND run_id = $2`,
      [run.workflowName, run.runId],
    );

  /**
   * A nested run that settled and terminalized (journal, terminal snapshot,
   * parent effect, destination receipt, recovery ancestry); its terminal
   * owner still holds the lease.
   */
  async function settledTerminalizedRun(run: Run) {
    const parent = { workflowName: `${run.workflowName}-parent`, runId: `${run.runId}-parent` };
    const parentStepId = 'nested';
    const parentGraph = [
      { type: 'step', step: { id: parentStepId, component: 'WORKFLOW' } },
    ] as unknown as WorkflowRunState['serializedStepGraph'];
    await workflows.persistWorkflowSnapshot({
      ...parent,
      resourceId: resource,
      snapshot: { ...createEmptyWorkflowSnapshot(parent.runId), serializedStepGraph: parentGraph },
    });
    await workflows.persistWorkflowSnapshot({
      ...run,
      resourceId: resource,
      snapshot: createEmptyWorkflowSnapshot(run.runId),
    });
    const ancestry = [
      {
        version: 1 as const,
        childWorkflowName: run.workflowName,
        childRunId: run.runId,
        parentWorkflowName: parent.workflowName,
        parentRunId: parent.runId,
        parentGraphFingerprint: createWorkflowTerminalGraphFingerprint(parentGraph),
        source: { kind: 'step' as const, stepId: parentStepId, executionPath: [0] },
        inputPointer: { kind: 'parent-source-payload' as const, stepId: parentStepId },
        resultPointer: { kind: 'retained-terminal-result' as const, ...run },
        resumeMetadata: { wasResume: false, resumeSteps: [] },
      },
    ];
    await expect(workflows.persistWorkflowTerminalRecoveryAncestry({ ...run, ancestry })).resolves.toMatchObject({
      status: 'persisted',
    });
    const claim = await workflows.claimWorkflowTerminalization({
      ...run,
      eventKey: `${run.runId}-event`,
      terminalStatus: 'success',
      ownerId: `${run.runId}-owner`,
      leaseMs: 60_000,
    });
    if (claim.status !== 'acquired') throw new Error(`Expected acquired, received ${claim.status}`);
    const fence = {
      ...run,
      ownerId: claim.record.ownerId,
      claimToken: claim.record.claimToken,
      claimGeneration: claim.record.claimGeneration,
    };
    const snapshot = { ...createEmptyWorkflowSnapshot(run.runId), status: 'success' as const, result: { report: 'x' } };
    await expect(
      workflows.persistWorkflowTerminalState({
        ...fence,
        resourceId: resource,
        snapshot,
        recoveryEnvelope: createTerminalRecoveryEnvelope({ ...run, snapshot, terminalStatus: 'success', ancestry }),
      }),
    ).resolves.toMatchObject({ status: 'persisted' });
    const prepared = await workflows.prepareWorkflowTerminalEffect({
      ...fence,
      expectedPhase: 'run_state_persisted',
      effect: {
        kind: 'parent-workflow-step-end',
        parentWorkflowName: parent.workflowName,
        parentRunId: parent.runId,
        parentStepId,
        parentExecutionPath: [0],
      },
    });
    if (prepared.status !== 'prepared') throw new Error(`Expected prepared, received ${prepared.status}`);
    await expect(
      workflows.reserveWorkflowTerminalDestinationReceipt({
        ...fence,
        effectKind: prepared.effect.kind,
        consumerId: 'closure-test',
      }),
    ).resolves.toMatchObject({ status: 'reserved' });
    return { parent, fence, ancestry };
  }

  /** Claims, completes and optionally releases a product handoff over the run's current snapshot. */
  async function deliveredHandoff(run: Run, { release }: { release: boolean }) {
    const canonical = (await workflows.loadWorkflowSnapshot(run))!;
    const owner = { ...run, resourceId: resource, mutationFence: `fence-${randomUUID()}` };
    const pending = { ...canonical, value: { phase: 'claimed' } } as WorkflowRunState;
    await expect(
      workflows.claimWorkflowSnapshotHandoff({
        ...owner,
        expectedCanonical: { kind: 'present', resourceId: resource, snapshot: canonical },
        snapshot: pending,
      }),
    ).resolves.toMatchObject({ status: 'created' });
    const complete = () =>
      workflows.completeWorkflowSnapshotHandoff({
        ...owner,
        expectedResourceId: resource,
        expectedSnapshot: pending,
        snapshot: { ...canonical, value: { papersflowTerminalDelivery: { report: 'delivered' } } } as WorkflowRunState,
      });
    if (release) {
      await expect(complete()).resolves.toMatchObject({ status: 'completed' });
      await expect(
        workflows.releaseWorkflowSnapshotHandoff({ ...run, expectedClaimGeneration: 1 }),
      ).resolves.toMatchObject({ status: 'released' });
    }
    return { complete };
  }

  async function settledRun(run: Run, status: WorkflowRunState['status'] = 'success') {
    await workflows.persistWorkflowSnapshot({
      ...run,
      resourceId: resource,
      snapshot: { ...createEmptyWorkflowSnapshot(run.runId), status },
    });
  }

  beforeAll(() => {
    resource = `workspace-${randomUUID()}:user-1`;
  });

  it('advertises closure deletion', () => {
    expect(workflows.getWorkflowSnapshotHandoffCapabilities()).toMatchObject({ closureDeleteVersion: 1 });
  });

  it('erases a released delivery with its snapshot and every terminal row, keeping the parent revision', async () => {
    const run = newRun();
    const { parent, fence, ancestry } = await settledTerminalizedRun(run);
    await expect(workflows.releaseWorkflowTerminalization(fence)).resolves.toMatchObject({ status: 'released' });
    await deliveredHandoff(run, { release: true });
    expect(await closureRows(run)).toMatchObject({
      mastra_workflow_snapshot: 1,
      mastra_workflow_snapshot_handoffs: 1,
      mastra_workflow_terminalizations: 1,
      mastra_workflow_terminal_effects_v2: 1,
      mastra_workflow_terminal_snapshots_v2: 1,
      mastra_workflow_terminal_destination_receipts_v2: 1,
      mastra_workflow_terminal_recovery_ancestries: 1,
    });
    // The settled parent still owes the child's unfinished terminal journal its state.
    const parentSnapshot = (await workflows.loadWorkflowSnapshot(parent))!;
    await workflows.persistWorkflowSnapshot({
      ...parent,
      resourceId: resource,
      snapshot: { ...parentSnapshot, status: 'failed' },
    });
    await expect(workflows.deleteWorkflowRunClosure({ ...parent, expectedResourceId: resource })).resolves.toEqual({
      status: 'active',
    });
    const revisionBefore = Number((await parentRevision(run))!.generation);

    await expect(workflows.deleteWorkflowRunClosure({ ...run, expectedResourceId: resource })).resolves.toEqual({
      status: 'deleted',
      removed: { snapshot: true, handoff: true, terminalRows: 5 },
    });

    expect(await closureRows(run)).toEqual(noRows);
    // A recovery-ancestry write queued behind the delete cannot recreate a terminal row.
    await expect(workflows.persistWorkflowTerminalRecoveryAncestry({ ...run, ancestry })).resolves.toEqual({
      status: 'missing_run',
    });
    // The content-free run-id reuse fence stays, bumped like a run deletion.
    expect(Number((await parentRevision(run))!.generation)).toBeGreaterThan(revisionBefore);
    await expect(workflows.loadWorkflowSnapshot(parent)).resolves.not.toBeNull();
    await expect(workflows.deleteWorkflowRunClosure({ ...run, expectedResourceId: resource })).resolves.toEqual({
      status: 'missing',
    });
    // With the child erased, the parent's closure goes too.
    await expect(
      workflows.deleteWorkflowRunClosure({ ...parent, expectedResourceId: resource }),
    ).resolves.toMatchObject({ status: 'deleted' });
  });

  it('finds and erases a delivered handoff whose snapshot was already deleted', async () => {
    const run = newRun();
    await settledRun(run);
    const { complete } = await deliveredHandoff(run, { release: false });
    await complete();
    await expect(
      workflows.releaseWorkflowSnapshotHandoff({ ...run, expectedClaimGeneration: 1, deleteRun: true }),
    ).resolves.toMatchObject({ status: 'released', runDeleted: true });

    const listed = await workflows.listWorkflowRunClosuresByResource({ resourceId: resource, limit: 100 });
    expect(listed.status === 'listed' && listed.closures.filter(entry => entry.runId === run.runId)).toEqual([
      { ...run, snapshot: false, terminalSnapshot: false, handoff: { status: 'completed', released: true } },
    ]);
    await expect(workflows.deleteWorkflowRunClosure({ ...run, expectedResourceId: resource })).resolves.toEqual({
      status: 'deleted',
      removed: { snapshot: false, handoff: true, terminalRows: 0 },
    });
    expect(await closureRows(run)).toEqual(noRows);
  });

  it('refuses a pending handoff, a running snapshot, a live terminal lease and another resource without changes', async () => {
    const pendingRun = newRun();
    await settledRun(pendingRun);
    const { complete } = await deliveredHandoff(pendingRun, { release: false });
    await expect(workflows.deleteWorkflowRunClosure({ ...pendingRun, expectedResourceId: resource })).resolves.toEqual({
      status: 'fenced',
    });
    await complete();
    await expect(
      workflows.deleteWorkflowRunClosure({ ...pendingRun, expectedResourceId: resource }),
    ).resolves.toMatchObject({ status: 'deleted' });

    const runningRun = newRun();
    await settledRun(runningRun, 'running');
    await expect(workflows.deleteWorkflowRunClosure({ ...runningRun, expectedResourceId: resource })).resolves.toEqual({
      status: 'active',
    });

    const leasedRun = newRun();
    await settledTerminalizedRun(leasedRun);
    await expect(workflows.deleteWorkflowRunClosure({ ...leasedRun, expectedResourceId: resource })).resolves.toEqual({
      status: 'active',
    });

    await expect(
      workflows.deleteWorkflowRunClosure({ ...runningRun, expectedResourceId: `${resource}-other` }),
    ).resolves.toEqual({ status: 'resource_mismatch' });
    expect(await closureRows(runningRun)).toMatchObject({ mastra_workflow_snapshot: 1 });
    expect(await closureRows(leasedRun)).toMatchObject({
      mastra_workflow_snapshot: 1,
      mastra_workflow_terminalizations: 1,
      mastra_workflow_terminal_effects_v2: 1,
    });
  });

  it('starts a reused run id above the erased claims, so a stale release cannot delete the new run', async () => {
    const run = newRun();
    await settledRun(run);
    await deliveredHandoff(run, { release: true });
    await expect(workflows.deleteWorkflowRunClosure({ ...run, expectedResourceId: resource })).resolves.toMatchObject({
      status: 'deleted',
    });

    // The same run id runs again (starting as running) and is delivered again.
    await workflows.persistWorkflowSnapshot({
      ...run,
      resourceId: resource,
      snapshot: { ...createEmptyWorkflowSnapshot(run.runId), status: 'running' },
    });
    await expect(workflows.getWorkflowRunTerminalStatus(run)).resolves.toEqual({ status: 'nonterminal' });
    await workflows.persistWorkflowSnapshot({
      ...run,
      resourceId: resource,
      snapshot: { ...createEmptyWorkflowSnapshot(run.runId), status: 'success', result: { second: true } },
    });
    const canonical = (await workflows.loadWorkflowSnapshot(run))!;
    const owner = { ...run, resourceId: resource, mutationFence: `second-${randomUUID()}` };
    const pending = { ...canonical, value: { phase: 'claimed-again' } } as WorkflowRunState;
    await expect(
      workflows.claimWorkflowSnapshotHandoff({
        ...owner,
        expectedCanonical: { kind: 'present', resourceId: resource, snapshot: canonical },
        snapshot: pending,
      }),
    ).resolves.toMatchObject({ status: 'created', record: { claimGeneration: 2 } });
    await expect(
      workflows.completeWorkflowSnapshotHandoff({
        ...owner,
        expectedResourceId: resource,
        expectedSnapshot: pending,
        snapshot: canonical,
      }),
    ).resolves.toMatchObject({ status: 'completed' });

    // A delayed release from the erased lifetime names generation 1.
    await expect(
      workflows.releaseWorkflowSnapshotHandoff({ ...run, expectedClaimGeneration: 1, deleteRun: true }),
    ).resolves.toMatchObject({ status: 'conflict', record: { claimGeneration: 2 } });
    await expect(workflows.loadWorkflowSnapshot(run)).resolves.toMatchObject({ result: { second: true } });
    const stored = await store.db.one<{ claim_generation: string; released: boolean }>(
      `SELECT claim_generation::text, released_at IS NOT NULL AS released
       FROM mastra_workflow_snapshot_handoffs WHERE workflow_name = $1 AND run_id = $2`,
      [run.workflowName, run.runId],
    );
    expect(stored).toEqual({ claim_generation: '2', released: false });
  });

  it('answers missing for a run storage never held', async () => {
    await expect(workflows.deleteWorkflowRunClosure({ ...newRun(), expectedResourceId: resource })).resolves.toEqual({
      status: 'missing',
    });
  });

  it('pages closures by resource in code-point order, including released handoffs, through the resource indexes', async () => {
    const pageResource = `workspace-${randomUUID()}:user-2`;
    const workflowName = `paging-${randomUUID()}`;
    for (const runId of ['run-c', 'run-a', 'run-b']) {
      await workflows.persistWorkflowSnapshot({
        workflowName,
        runId,
        resourceId: pageResource,
        snapshot: { ...createEmptyWorkflowSnapshot(runId), status: 'success' },
      });
    }
    await workflows.persistWorkflowSnapshot({
      workflowName,
      runId: 'run-other',
      resourceId: `${pageResource}-other`,
      snapshot: createEmptyWorkflowSnapshot('run-other'),
    });
    const previous = resource;
    resource = pageResource;
    try {
      await deliveredHandoff({ workflowName, runId: 'run-b' }, { release: true });
    } finally {
      resource = previous;
    }

    const first = await workflows.listWorkflowRunClosuresByResource({ resourceId: pageResource, limit: 2 });
    expect(first).toEqual({
      status: 'listed',
      closures: [
        { workflowName, runId: 'run-a', snapshot: true, terminalSnapshot: false },
        {
          workflowName,
          runId: 'run-b',
          snapshot: true,
          terminalSnapshot: false,
          handoff: { status: 'completed', released: true },
        },
      ],
      hasMore: true,
      nextCursor: { workflowName, runId: 'run-b' },
    });
    const second = await workflows.listWorkflowRunClosuresByResource({
      resourceId: pageResource,
      limit: 2,
      after: first.status === 'listed' ? first.nextCursor : undefined,
    });
    expect(second).toEqual({
      status: 'listed',
      closures: [{ workflowName, runId: 'run-c', snapshot: true, terminalSnapshot: false }],
      hasMore: false,
    });

    // With sequential scans off, each resource lookup must be able to use its index.
    for (const [table, column] of [
      ['mastra_workflow_snapshot', '"resourceId"'],
      ['mastra_workflow_snapshot_handoffs', 'resource_id'],
      ['mastra_workflow_terminal_snapshots_v2', 'resource_id'],
    ] as const) {
      const plan = await store.db.tx(async t => {
        await t.none('SET LOCAL enable_seqscan = off');
        return t.any<{ 'QUERY PLAN': string }>(`EXPLAIN SELECT run_id FROM ${table} WHERE ${column} = $1`, [
          pageResource,
        ]);
      });
      expect(plan.map(row => row['QUERY PLAN']).join('\n')).toMatch(/_resource(_run)?_idx/);
    }
  });
});
