import { describe, expect, it } from 'vitest';
import type { WorkflowRunState } from '../../../workflows';
import { createWorkflowTerminalGraphFingerprint } from '../../../workflows/terminal-continuation';
import { createEmptyWorkflowSnapshot, isEmptyWorkflowSnapshot } from '../../workflow-snapshot';
import { WorkflowSnapshotHandoffFenceError } from '../../workflow-snapshot-handoff';
import { InMemoryDB } from '../inmemory-db';
import { WorkflowsInMemory } from './inmemory';
import { createTerminalRecoveryEnvelope } from './terminalization-test-utils';

const RESOURCE = 'workspace-1:user-1';

function setup() {
  const db = new InMemoryDB();
  return { db, workflows: new WorkflowsInMemory({ db }) };
}

type Run = { workflowName: string; runId: string };

/** Every row storage keeps for the run, by table; the parent revision is reported separately. */
function closureRows(db: InMemoryDB, run: Run) {
  const key = JSON.stringify([run.workflowName, run.runId]);
  const ofRun = (record: { workflowName: string; runId: string }) =>
    record.workflowName === run.workflowName && record.runId === run.runId;
  return {
    snapshot: db.workflows.has(key),
    handoff: db.workflowSnapshotHandoffs.has(key),
    journal: db.workflowTerminalizations.has(key),
    terminalSnapshot: db.workflowTerminalSnapshots.has(key),
    ancestry: db.workflowTerminalRecoveryAncestries.has(key),
    effects: [...db.workflowTerminalEffects.values()].filter(ofRun).length,
    receipts: [...db.workflowTerminalDestinationReceipts.values()].filter(ofRun).length,
    plans: [...db.workflowTerminalContinuationPlans.values()].filter(ofRun).length,
  };
}

/**
 * A nested run that settled and terminalized (journal, terminal snapshot,
 * parent effect, destination receipt, recovery ancestry), whose terminal
 * owner then let go of its lease.
 */
async function settledTerminalizedRun(workflows: WorkflowsInMemory, run: Run) {
  const parent = { workflowName: `${run.workflowName}-parent`, runId: `${run.runId}-parent` };
  const parentStepId = 'nested';
  const parentGraph: WorkflowRunState['serializedStepGraph'] = [
    { type: 'step', step: { id: parentStepId, component: 'WORKFLOW' } } as any,
  ];
  await workflows.persistWorkflowSnapshot({
    ...parent,
    resourceId: RESOURCE,
    snapshot: { ...createEmptyWorkflowSnapshot(parent.runId), serializedStepGraph: parentGraph },
  });
  await workflows.persistWorkflowSnapshot({
    ...run,
    resourceId: RESOURCE,
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
      resourceId: RESOURCE,
      snapshot,
      recoveryEnvelope: createTerminalRecoveryEnvelope({ ...run, snapshot, terminalStatus: 'success', ancestry }),
    }),
  ).resolves.toMatchObject({ status: 'persisted' });
  const prepared = await workflows.prepareWorkflowTerminalEffect({
    ...fence,
    expectedPhase: 'run_state_persisted',
    effect: { kind: 'parent-workflow-step-end', ...parentEffect(parent, parentStepId) },
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

function parentEffect(parent: Run, parentStepId: string) {
  return {
    parentWorkflowName: parent.workflowName,
    parentRunId: parent.runId,
    parentStepId,
    parentExecutionPath: [0],
  };
}

/** Claims, completes and optionally releases a product handoff over the run's current snapshot. */
async function deliveredHandoff(workflows: WorkflowsInMemory, run: Run, { release }: { release: boolean }) {
  const canonical = (await workflows.loadWorkflowSnapshot(run))!;
  const owner = { ...run, resourceId: RESOURCE, mutationFence: `${run.runId}-fence` };
  const pending = { ...canonical, value: { phase: 'claimed' } } as WorkflowRunState;
  await expect(
    workflows.claimWorkflowSnapshotHandoff({
      ...owner,
      expectedCanonical: { kind: 'present', resourceId: RESOURCE, snapshot: canonical },
      snapshot: pending,
    }),
  ).resolves.toMatchObject({ status: 'created' });
  const complete = () =>
    workflows.completeWorkflowSnapshotHandoff({
      ...owner,
      expectedResourceId: RESOURCE,
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

const NOTHING = {
  snapshot: false,
  handoff: false,
  journal: false,
  terminalSnapshot: false,
  ancestry: false,
  effects: 0,
  receipts: 0,
  plans: 0,
};

describe('WorkflowsInMemory run closure delete', () => {
  it('advertises closure deletion', () => {
    expect(setup().workflows.getWorkflowSnapshotHandoffCapabilities()).toMatchObject({ closureDeleteVersion: 1 });
  });

  it('erases a released delivery with its snapshot and every terminal row, keeping the parent revision', async () => {
    const { db, workflows } = setup();
    const run = { workflowName: 'research', runId: 'run-deleted' };
    const { parent, fence, ancestry } = await settledTerminalizedRun(workflows, run);
    await expect(workflows.releaseWorkflowTerminalization(fence)).resolves.toMatchObject({ status: 'released' });
    await deliveredHandoff(workflows, run, { release: true });
    expect(closureRows(db, run)).toMatchObject({
      snapshot: true,
      handoff: true,
      journal: true,
      terminalSnapshot: true,
      ancestry: true,
      effects: 1,
      receipts: 1,
    });
    // The settled parent still owes the child's unfinished terminal journal its state.
    const parentSnapshot = (await workflows.loadWorkflowSnapshot(parent))!;
    await workflows.persistWorkflowSnapshot({
      ...parent,
      resourceId: RESOURCE,
      snapshot: { ...parentSnapshot, status: 'failed' },
    });
    await expect(workflows.deleteWorkflowRunClosure({ ...parent, expectedResourceId: RESOURCE })).resolves.toEqual({
      status: 'active',
    });
    const revisionKey = JSON.stringify([run.workflowName, run.runId]);
    const revisionBefore = db.workflowTerminalParentRevisions.get(revisionKey)!.generation;

    const result = await workflows.deleteWorkflowRunClosure({ ...run, expectedResourceId: RESOURCE });

    expect(result).toEqual({ status: 'deleted', removed: { snapshot: true, handoff: true, terminalRows: 5 } });
    expect(closureRows(db, run)).toEqual(NOTHING);
    // A recovery-ancestry write queued behind the delete cannot recreate a terminal row.
    await expect(workflows.persistWorkflowTerminalRecoveryAncestry({ ...run, ancestry })).resolves.toEqual({
      status: 'missing_run',
    });
    expect(closureRows(db, run)).toEqual(NOTHING);
    expect(db.workflowTerminalParentRevisions.get(revisionKey)!.generation).toBeGreaterThan(revisionBefore);
    // The parent run is another closure and stays.
    await expect(workflows.loadWorkflowSnapshot(parent)).resolves.not.toBeNull();
    await expect(
      workflows.listWorkflowRunClosuresByResource({ resourceId: RESOURCE, limit: 10 }),
    ).resolves.toMatchObject({ closures: [{ ...parent, snapshot: true }] });
    // A repeat is an idempotent no-op.
    await expect(workflows.deleteWorkflowRunClosure({ ...run, expectedResourceId: RESOURCE })).resolves.toEqual({
      status: 'missing',
    });
    // With the child erased, the parent's closure goes too.
    await expect(
      workflows.deleteWorkflowRunClosure({ ...parent, expectedResourceId: RESOURCE }),
    ).resolves.toMatchObject({ status: 'deleted' });
  });

  it('finds and erases a delivered handoff whose snapshot was already deleted', async () => {
    const { db, workflows } = setup();
    const run = { workflowName: 'research', runId: 'run-orphan' };
    await workflows.persistWorkflowSnapshot({
      ...run,
      resourceId: RESOURCE,
      snapshot: { ...createEmptyWorkflowSnapshot(run.runId), status: 'success' },
    });
    await deliveredHandoff(workflows, run, { release: false }).then(({ complete }) => complete());
    await workflows.releaseWorkflowSnapshotHandoff({ ...run, expectedClaimGeneration: 1, deleteRun: true });
    expect(closureRows(db, run)).toMatchObject({ snapshot: false, handoff: true });

    await expect(workflows.listWorkflowRunClosuresByResource({ resourceId: RESOURCE, limit: 10 })).resolves.toEqual({
      status: 'listed',
      closures: [
        { ...run, snapshot: false, terminalSnapshot: false, handoff: { status: 'completed', released: true } },
      ],
      hasMore: false,
    });
    await expect(workflows.deleteWorkflowRunClosure({ ...run, expectedResourceId: RESOURCE })).resolves.toEqual({
      status: 'deleted',
      removed: { snapshot: false, handoff: true, terminalRows: 0 },
    });
    expect(closureRows(db, run)).toEqual(NOTHING);
  });

  it('refuses a pending handoff, a running snapshot, a live terminal lease and another resource without changes', async () => {
    const { db, workflows } = setup();
    const pendingRun = { workflowName: 'research', runId: 'run-pending' };
    await workflows.persistWorkflowSnapshot({
      ...pendingRun,
      resourceId: RESOURCE,
      snapshot: { ...createEmptyWorkflowSnapshot(pendingRun.runId), status: 'success' },
    });
    const { complete } = await deliveredHandoff(workflows, pendingRun, { release: false });
    await expect(workflows.deleteWorkflowRunClosure({ ...pendingRun, expectedResourceId: RESOURCE })).resolves.toEqual({
      status: 'fenced',
    });
    // Once completed (still unreleased) the handoff no longer fences erasure.
    await complete();
    await expect(
      workflows.deleteWorkflowRunClosure({ ...pendingRun, expectedResourceId: RESOURCE }),
    ).resolves.toMatchObject({ status: 'deleted' });

    const runningRun = { workflowName: 'research', runId: 'run-running' };
    await workflows.persistWorkflowSnapshot({
      ...runningRun,
      resourceId: RESOURCE,
      snapshot: { ...createEmptyWorkflowSnapshot(runningRun.runId), status: 'running' },
    });
    await expect(workflows.deleteWorkflowRunClosure({ ...runningRun, expectedResourceId: RESOURCE })).resolves.toEqual({
      status: 'active',
    });

    const leasedRun = { workflowName: 'research', runId: 'run-leased' };
    await settledTerminalizedRun(workflows, leasedRun);
    await expect(workflows.deleteWorkflowRunClosure({ ...leasedRun, expectedResourceId: RESOURCE })).resolves.toEqual({
      status: 'active',
    });

    await expect(
      workflows.deleteWorkflowRunClosure({ ...runningRun, expectedResourceId: 'workspace-2:user-1' }),
    ).resolves.toEqual({ status: 'resource_mismatch' });
    expect(closureRows(db, runningRun)).toMatchObject({ snapshot: true });
    expect(closureRows(db, leasedRun)).toMatchObject({ snapshot: true, journal: true, effects: 1 });
  });

  it('rechecks a run that a snapshot status getter replaced while being read', async () => {
    const { db, workflows } = setup();
    const run = { workflowName: 'research', runId: 'run-reentrant' };
    let armed = false;
    let replaced = false;
    // An inherited accessor survives the store's clone of the snapshot.
    const statusGetter = {
      get status() {
        if (armed && !replaced) {
          replaced = true;
          void workflows.persistWorkflowSnapshot({
            ...run,
            resourceId: 'workspace-2:user-1',
            snapshot: { ...createEmptyWorkflowSnapshot(run.runId), status: 'running' },
          });
        }
        return 'success';
      },
    };
    const { status: _ownStatus, ...fields } = createEmptyWorkflowSnapshot(run.runId);
    const settled = Object.assign(Object.create(statusGetter), fields) as WorkflowRunState;
    await workflows.persistWorkflowSnapshot({ ...run, resourceId: RESOURCE, snapshot: settled });
    armed = true;

    await expect(workflows.deleteWorkflowRunClosure({ ...run, expectedResourceId: RESOURCE })).resolves.toEqual({
      status: 'resource_mismatch',
    });
    expect(replaced).toBe(true);
    expect(closureRows(db, run)).toMatchObject({ snapshot: true });
  });

  it('starts a reused run id above the erased claims, so a stale release cannot delete the new run', async () => {
    const { workflows } = setup();
    const run = { workflowName: 'research', runId: 'run-reused' };
    await workflows.persistWorkflowSnapshot({
      ...run,
      resourceId: RESOURCE,
      snapshot: { ...createEmptyWorkflowSnapshot(run.runId), status: 'success' },
    });
    await deliveredHandoff(workflows, run, { release: true });
    await expect(workflows.deleteWorkflowRunClosure({ ...run, expectedResourceId: RESOURCE })).resolves.toMatchObject({
      status: 'deleted',
    });

    // The same run id runs again (starting as running) and is delivered again.
    await workflows.persistWorkflowSnapshot({
      ...run,
      resourceId: RESOURCE,
      snapshot: { ...createEmptyWorkflowSnapshot(run.runId), status: 'running' },
    });
    await expect(workflows.getWorkflowRunTerminalStatus(run)).resolves.toEqual({ status: 'nonterminal' });
    await workflows.persistWorkflowSnapshot({
      ...run,
      resourceId: RESOURCE,
      snapshot: { ...createEmptyWorkflowSnapshot(run.runId), status: 'success', result: { second: true } },
    });
    const canonical = (await workflows.loadWorkflowSnapshot(run))!;
    const owner = { ...run, resourceId: RESOURCE, mutationFence: 'second-fence' };
    const pending = { ...canonical, value: { phase: 'claimed-again' } } as WorkflowRunState;
    await expect(
      workflows.claimWorkflowSnapshotHandoff({
        ...owner,
        expectedCanonical: { kind: 'present', resourceId: RESOURCE, snapshot: canonical },
        snapshot: pending,
      }),
    ).resolves.toMatchObject({ status: 'created', record: { claimGeneration: 2 } });
    await expect(
      workflows.completeWorkflowSnapshotHandoff({
        ...owner,
        expectedResourceId: RESOURCE,
        expectedSnapshot: pending,
        snapshot: canonical,
      }),
    ).resolves.toMatchObject({ status: 'completed' });

    // A delayed release from the erased lifetime names generation 1.
    await expect(
      workflows.releaseWorkflowSnapshotHandoff({ ...run, expectedClaimGeneration: 1, deleteRun: true }),
    ).resolves.toMatchObject({ status: 'conflict', record: { claimGeneration: 2 } });
    await expect(workflows.loadWorkflowSnapshot(run)).resolves.toMatchObject({ result: { second: true } });
  });

  it('erases runs stored under empty names and reads a listing input only once', async () => {
    const { db, workflows } = setup();
    const run = { workflowName: '', runId: '' };
    await workflows.persistWorkflowSnapshot({
      ...run,
      resourceId: RESOURCE,
      snapshot: { ...createEmptyWorkflowSnapshot(run.runId), status: 'success' },
    });
    await workflows.persistWorkflowSnapshot({
      workflowName: 'research',
      runId: 'run-other',
      resourceId: 'workspace-2:user-1',
      snapshot: { ...createEmptyWorkflowSnapshot('run-other'), status: 'success' },
    });

    // A getter that answers differently on a second read must not move the query.
    let reads = 0;
    const input = {
      get resourceId() {
        reads++;
        return reads === 1 ? RESOURCE : 'workspace-2:user-1';
      },
      limit: 10,
    };
    await expect(workflows.listWorkflowRunClosuresByResource(input)).resolves.toMatchObject({
      closures: [{ ...run, snapshot: true }],
    });
    expect(reads).toBe(1);

    await expect(workflows.deleteWorkflowRunClosure({ ...run, expectedResourceId: RESOURCE })).resolves.toMatchObject({
      status: 'deleted',
    });
    expect(closureRows(db, run)).toEqual(NOTHING);
  });

  it('answers missing for a run storage never held', async () => {
    const { workflows } = setup();
    await expect(
      workflows.deleteWorkflowRunClosure({ workflowName: 'research', runId: 'never', expectedResourceId: RESOURCE }),
    ).resolves.toEqual({ status: 'missing' });
  });

  it('pages closures by resource in code-point order, including released handoffs', async () => {
    const { workflows } = setup();
    for (const runId of ['run-c', 'run-a', 'run-b']) {
      await workflows.persistWorkflowSnapshot({
        workflowName: 'research',
        runId,
        resourceId: RESOURCE,
        snapshot: { ...createEmptyWorkflowSnapshot(runId), status: 'success' },
      });
    }
    await workflows.persistWorkflowSnapshot({
      workflowName: 'research',
      runId: 'run-other',
      resourceId: 'workspace-2:user-1',
      snapshot: createEmptyWorkflowSnapshot('run-other'),
    });
    await deliveredHandoff(workflows, { workflowName: 'research', runId: 'run-b' }, { release: true });

    const first = await workflows.listWorkflowRunClosuresByResource({ resourceId: RESOURCE, limit: 2 });
    expect(first).toEqual({
      status: 'listed',
      closures: [
        { workflowName: 'research', runId: 'run-a', snapshot: true, terminalSnapshot: false },
        {
          workflowName: 'research',
          runId: 'run-b',
          snapshot: true,
          terminalSnapshot: false,
          handoff: { status: 'completed', released: true },
        },
      ],
      hasMore: true,
      nextCursor: { workflowName: 'research', runId: 'run-b' },
    });
    const second = await workflows.listWorkflowRunClosuresByResource({
      resourceId: RESOURCE,
      limit: 2,
      after: first.status === 'listed' ? first.nextCursor : undefined,
    });
    expect(second).toEqual({
      status: 'listed',
      closures: [{ workflowName: 'research', runId: 'run-c', snapshot: true, terminalSnapshot: false }],
      hasMore: false,
    });
  });
});

describe('WorkflowsInMemory run closure delete: retained absence fence (PF-6186)', () => {
  const FENCE = 'product-erasure-absent@1';
  const run: Run = { workflowName: 'research', runId: 'run-fenced' };
  const handoffOf = (db: InMemoryDB) => db.workflowSnapshotHandoffs.get(JSON.stringify([run.workflowName, run.runId]));

  /** Claims and completes a handoff over an absent run, as an erasure's absence fence. */
  async function fenceAbsence(
    workflows: WorkflowsInMemory,
    { fence = FENCE, resourceId = RESOURCE, snapshot = createEmptyWorkflowSnapshot(run.runId), complete = true } = {},
  ) {
    await expect(
      workflows.claimWorkflowSnapshotHandoff({
        ...run,
        expectedCanonical: { kind: 'absent' },
        resourceId,
        snapshot,
        mutationFence: fence,
      }),
    ).resolves.toMatchObject({ status: 'created' });
    if (!complete) return;
    await expect(
      workflows.completeWorkflowSnapshotHandoff({
        ...run,
        expectedResourceId: resourceId,
        expectedSnapshot: snapshot,
        resourceId,
        snapshot,
        mutationFence: fence,
      }),
    ).resolves.toMatchObject({ status: 'completed' });
  }

  async function settledRun(workflows: WorkflowsInMemory) {
    await workflows.persistWorkflowSnapshot({
      ...run,
      resourceId: RESOURCE,
      snapshot: { ...createEmptyWorkflowSnapshot(run.runId), status: 'success', result: { report: 'private' } },
    });
  }

  it('advertises fence retention', () => {
    expect(setup().workflows.getWorkflowSnapshotHandoffCapabilities()).toMatchObject({
      closureDeleteRetainFenceVersion: 1,
    });
  });

  it("keeps a newer worker's absence fence when a stale delete lands after it and its response is lost", async () => {
    const { db, workflows } = setup();
    await settledRun(workflows);
    // Worker B erases the closure and fences the run's absence.
    await expect(
      workflows.deleteWorkflowRunClosure({ ...run, expectedResourceId: RESOURCE, retainHandoffFence: FENCE }),
    ).resolves.toMatchObject({ status: 'deleted' });
    await fenceAbsence(workflows);
    // Worker A's delayed delete arrives afterwards; its caller never sees the
    // answer (lost response), so nothing would re-fence the run.
    await workflows.deleteWorkflowRunClosure({ ...run, expectedResourceId: RESOURCE, retainHandoffFence: FENCE });

    const listed = await workflows.listWorkflowSnapshotHandoffs({ status: 'completed' });
    expect(listed.records.filter(record => record.runId === run.runId)).toHaveLength(1);
    expect(handoffOf(db)).toMatchObject({ status: 'completed', mutationFence: FENCE });
    await expect(
      workflows.persistWorkflowSnapshot({
        ...run,
        resourceId: RESOURCE,
        snapshot: { ...createEmptyWorkflowSnapshot(run.runId), status: 'running' },
      }),
    ).rejects.toBeInstanceOf(WorkflowSnapshotHandoffFenceError);
    expect(closureRows(db, run)).toMatchObject({ snapshot: false, handoff: true });
  });

  it('empties a retained fence snapshot in place and keeps its fence, status and claim generation', async () => {
    const { db, workflows } = setup();
    await fenceAbsence(workflows, {
      snapshot: { ...createEmptyWorkflowSnapshot(run.runId), context: { private: { output: 'content' } } } as any,
    });
    const before = handoffOf(db)!;

    await expect(
      workflows.deleteWorkflowRunClosure({ ...run, expectedResourceId: RESOURCE, retainHandoffFence: FENCE }),
    ).resolves.toEqual({
      status: 'deleted',
      removed: { snapshot: false, handoff: false, terminalRows: 0 },
      retainedFence: true,
    });
    const after = handoffOf(db)!;
    expect(after).toMatchObject({
      mutationFence: before.mutationFence,
      status: 'completed',
      claimGeneration: before.claimGeneration,
      resourceId: RESOURCE,
    });
    expect(after.releasedAt).toBeUndefined();
    expect(isEmptyWorkflowSnapshot(after.snapshot, run.runId)).toBe(true);
    expect(JSON.stringify(after.snapshot)).not.toContain('content');
    // Already content-free: an idempotent no-op that still reports the fence.
    await expect(
      workflows.deleteWorkflowRunClosure({ ...run, expectedResourceId: RESOURCE, retainHandoffFence: FENCE }),
    ).resolves.toEqual({ status: 'missing', retainedFence: true });
    expect(handoffOf(db)).toMatchObject({ mutationFence: FENCE, status: 'completed' });
  });

  it('replaces a fence snapshot whose timestamp carries content', async () => {
    const { db, workflows } = setup();
    await fenceAbsence(workflows, {
      snapshot: { ...createEmptyWorkflowSnapshot(run.runId), timestamp: { userText: 'private content' } } as any,
    });
    await expect(
      workflows.deleteWorkflowRunClosure({ ...run, expectedResourceId: RESOURCE, retainHandoffFence: FENCE }),
    ).resolves.toMatchObject({ status: 'deleted', retainedFence: true });
    const after = handoffOf(db)!;
    expect(typeof (after.snapshot as { timestamp?: unknown }).timestamp).toBe('number');
    expect(JSON.stringify(after.snapshot)).not.toContain('private content');
    expect(after).toMatchObject({ mutationFence: FENCE, status: 'completed' });
  });

  it("clears the erased lifetime's terminal latch even when only the empty fence remains", async () => {
    const { db, workflows } = setup();
    await settledRun(workflows);
    const key = JSON.stringify([run.workflowName, run.runId]);
    await workflows.deleteWorkflowRunById(run);
    expect(db.workflowTerminalParentRevisions.get(key)?.terminalStatus).toBe('success');
    await fenceAbsence(workflows);
    const before = db.workflowTerminalParentRevisions.get(key)!;

    await expect(
      workflows.deleteWorkflowRunClosure({ ...run, expectedResourceId: RESOURCE, retainHandoffFence: FENCE }),
    ).resolves.toMatchObject({ retainedFence: true });
    const revision = db.workflowTerminalParentRevisions.get(key)!;
    expect(revision.terminalStatus).toBeNull();
    expect(revision.generation).toBeGreaterThan(before.generation);
    expect(handoffOf(db)).toMatchObject({ mutationFence: FENCE, status: 'completed' });
    // Now fully normalized: a repeat is a no-op.
    await expect(
      workflows.deleteWorkflowRunClosure({ ...run, expectedResourceId: RESOURCE, retainHandoffFence: FENCE }),
    ).resolves.toEqual({ status: 'missing', retainedFence: true });
    expect(db.workflowTerminalParentRevisions.get(key)!.generation).toBe(revision.generation);
  });

  it('still deletes a handoff under another fence, or when no fence is named', async () => {
    for (const retainHandoffFence of ['another-fence@1', undefined]) {
      const { db, workflows } = setup();
      await fenceAbsence(workflows);
      await expect(
        workflows.deleteWorkflowRunClosure({
          ...run,
          expectedResourceId: RESOURCE,
          ...(retainHandoffFence === undefined ? {} : { retainHandoffFence }),
        }),
      ).resolves.toEqual({ status: 'deleted', removed: { snapshot: false, handoff: true, terminalRows: 0 } });
      expect(closureRows(db, run)).toEqual(NOTHING);
    }
  });

  it('answers resource_mismatch for a matching fence recorded under another resource, without changes', async () => {
    const { db, workflows } = setup();
    await fenceAbsence(workflows, { resourceId: 'workspace-2:user-2' });
    const before = structuredClone(handoffOf(db));
    await expect(
      workflows.deleteWorkflowRunClosure({ ...run, expectedResourceId: RESOURCE, retainHandoffFence: FENCE }),
    ).resolves.toEqual({ status: 'resource_mismatch' });
    expect(handoffOf(db)).toEqual(before);
  });

  it('answers fenced for a pending fence, and deletes a released one', async () => {
    const pending = setup();
    await fenceAbsence(pending.workflows, { complete: false });
    await expect(
      pending.workflows.deleteWorkflowRunClosure({ ...run, expectedResourceId: RESOURCE, retainHandoffFence: FENCE }),
    ).resolves.toEqual({ status: 'fenced' });
    expect(handoffOf(pending.db)).toMatchObject({ status: 'pending', mutationFence: FENCE });

    const released = setup();
    await fenceAbsence(released.workflows);
    await expect(
      released.workflows.releaseWorkflowSnapshotHandoff({ ...run, expectedClaimGeneration: 1 }),
    ).resolves.toMatchObject({ status: 'released' });
    await expect(
      released.workflows.deleteWorkflowRunClosure({ ...run, expectedResourceId: RESOURCE, retainHandoffFence: FENCE }),
    ).resolves.toMatchObject({ status: 'deleted', removed: { handoff: true } });
    expect(handoffOf(released.db)).toBeUndefined();
  });
});
