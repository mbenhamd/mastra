import { describe, expect, it } from 'vitest';
import type { WorkflowRunState } from '../../../workflows';
import { createWorkflowTerminalGraphFingerprint } from '../../../workflows/terminal-continuation';
import { createEmptyWorkflowSnapshot } from '../../workflow-snapshot';
import { WorkflowStaleSnapshotPersistError } from '../../workflow-snapshot-handoff';
import { InMemoryDB } from '../inmemory-db';
import { WorkflowsInMemory } from './inmemory';

const RESOURCE = 'workspace-1:user-1';

function setup() {
  const db = new InMemoryDB();
  return { db, workflows: new WorkflowsInMemory({ db }) };
}

type Run = { workflowName: string; runId: string };

const NESTED_STEP = 'nested';
const NESTED_GRAPH: WorkflowRunState['serializedStepGraph'] = [
  { type: 'step', step: { id: NESTED_STEP, component: 'WORKFLOW' } } as any,
];

async function persist(workflows: WorkflowsInMemory, run: Run, status: WorkflowRunState['status']) {
  await workflows.persistWorkflowSnapshot({
    ...run,
    resourceId: RESOURCE,
    snapshot: {
      ...createEmptyWorkflowSnapshot(run.runId),
      serializedStepGraph: NESTED_GRAPH,
      status,
      result: { draft: `${run.runId} draft` },
    },
  });
}

/** A running child whose recovery ancestry names `parent` and whose terminal journal is claimed. */
async function pendingChild(workflows: WorkflowsInMemory, parent: Run): Promise<Run> {
  const child = { workflowName: `${parent.workflowName}-child`, runId: `${parent.runId}-child` };
  await persist(workflows, child, 'running');
  await expect(
    workflows.persistWorkflowTerminalRecoveryAncestry({
      ...child,
      ancestry: [
        {
          version: 1,
          childWorkflowName: child.workflowName,
          childRunId: child.runId,
          parentWorkflowName: parent.workflowName,
          parentRunId: parent.runId,
          parentGraphFingerprint: createWorkflowTerminalGraphFingerprint(NESTED_GRAPH),
          source: { kind: 'step', stepId: NESTED_STEP, executionPath: [0] },
          inputPointer: { kind: 'parent-source-payload', stepId: NESTED_STEP },
          resultPointer: { kind: 'retained-terminal-result', ...child },
          resumeMetadata: { wasResume: false, resumeSteps: [] },
        },
      ],
    }),
  ).resolves.toMatchObject({ status: 'persisted' });
  await expect(
    workflows.claimWorkflowTerminalization({
      ...child,
      eventKey: `${child.runId}-event`,
      terminalStatus: 'success',
      ownerId: `${child.runId}-owner`,
      leaseMs: 60_000,
    }),
  ).resolves.toMatchObject({ status: 'acquired' });
  return child;
}

const cancel = (workflows: WorkflowsInMemory, run: Run, expectedResourceId = RESOURCE) =>
  workflows.cancelWorkflowRunForErasure({ ...run, expectedResourceId });

describe('WorkflowsInMemory erasure cancel', () => {
  it('advertises the erasure cancel', () => {
    expect(setup().workflows.getWorkflowSnapshotHandoffCapabilities()).toMatchObject({ closureCancelVersion: 1 });
  });

  it('overwrites an unsettled run with an empty canceled snapshot that the closure delete then erases', async () => {
    const { db, workflows } = setup();
    const run = { workflowName: 'research', runId: 'run-suspended' };
    await persist(workflows, run, 'suspended');

    await expect(cancel(workflows, run)).resolves.toEqual({ status: 'cancelled' });
    const stored = await workflows.loadWorkflowSnapshot(run);
    expect(stored).toMatchObject({ status: 'canceled', runId: run.runId });
    expect(stored?.result).toBeUndefined();
    expect(db.workflows.get(JSON.stringify([run.workflowName, run.runId]))?.resourceId).toBe(RESOURCE);
    await expect(cancel(workflows, run)).resolves.toEqual({ status: 'settled' });
    await expect(workflows.deleteWorkflowRunClosure({ ...run, expectedResourceId: RESOURCE })).resolves.toMatchObject({
      status: 'deleted',
    });
  });

  it('refuses a parent a nested child still needs, and the child whose journal is claimed, without changes', async () => {
    const { workflows } = setup();
    const parent = { workflowName: 'deep-scan', runId: 'run-parent' };
    await persist(workflows, parent, 'running');
    const child = await pendingChild(workflows, parent);

    await expect(cancel(workflows, parent)).resolves.toEqual({ status: 'active' });
    await expect(cancel(workflows, child)).resolves.toEqual({ status: 'journaled' });
    await expect(workflows.loadWorkflowSnapshot(parent)).resolves.toMatchObject({
      status: 'running',
      result: { draft: `${parent.runId} draft` },
    });
    await expect(workflows.loadWorkflowSnapshot(child)).resolves.toMatchObject({ status: 'running' });
  });

  it('answers missing, resource_mismatch, fenced and settled without changes', async () => {
    const { workflows } = setup();
    await expect(cancel(workflows, { workflowName: 'research', runId: 'never-stored' })).resolves.toEqual({
      status: 'missing',
    });

    const running = { workflowName: 'research', runId: 'run-running' };
    await persist(workflows, running, 'running');
    await expect(cancel(workflows, running, 'workspace-2:user-1')).resolves.toEqual({ status: 'resource_mismatch' });

    const delivered = { workflowName: 'research', runId: 'run-delivered' };
    await persist(workflows, delivered, 'success');
    const canonical = (await workflows.loadWorkflowSnapshot(delivered))!;
    await expect(
      workflows.claimWorkflowSnapshotHandoff({
        ...delivered,
        resourceId: RESOURCE,
        mutationFence: 'fence-delivered',
        expectedCanonical: { kind: 'present', resourceId: RESOURCE, snapshot: canonical },
        snapshot: { ...canonical, value: { phase: 'claimed' } } as WorkflowRunState,
      }),
    ).resolves.toMatchObject({ status: 'created' });
    await expect(cancel(workflows, delivered)).resolves.toEqual({ status: 'fenced' });

    const settled = { workflowName: 'research', runId: 'run-failed' };
    await persist(workflows, settled, 'failed');
    await expect(cancel(workflows, settled)).resolves.toEqual({ status: 'settled' });

    await expect(workflows.loadWorkflowSnapshot(running)).resolves.toMatchObject({ status: 'running' });
    await expect(workflows.loadWorkflowSnapshot(settled)).resolves.toMatchObject({
      status: 'failed',
      result: { draft: `${settled.runId} draft` },
    });
  });

  it('moves the parent revision and latches canceled, so later snapshot and state writes are refused', async () => {
    const { db, workflows } = setup();
    const run = { workflowName: 'research', runId: 'run-latched' };
    const ordinary = { ...run, runId: 'run-ordinary-canceled' };
    await persist(workflows, ordinary, 'canceled');
    await workflows.updateWorkflowState({ ...ordinary, opts: { status: 'running' } });
    await expect(workflows.loadWorkflowSnapshot(ordinary)).resolves.toMatchObject({ status: 'running' });

    await persist(workflows, run, 'waiting');
    const key = JSON.stringify([run.workflowName, run.runId]);
    const before = db.workflowTerminalParentRevisions.get(key);

    // An ordinary snapshot's inherited status getter can cancel the run while
    // its parent revision is being advanced. The outer write must retain that fence.
    const { status: _initialStatus, ...initialSnapshot } = (await workflows.loadWorkflowSnapshot(run))!;
    let cancelling = false;
    let reentrantCancellation: ReturnType<typeof cancel> | undefined;
    const cancellingStatusReader = {
      get status() {
        if (!cancelling) {
          cancelling = true;
          reentrantCancellation = cancel(workflows, run);
        }
        return 'running';
      },
    };
    await workflows.persistWorkflowSnapshot({
      ...run,
      snapshot: Object.assign(Object.create(cancellingStatusReader), initialSnapshot),
    });
    await expect(reentrantCancellation).resolves.toEqual({ status: 'cancelled' });
    const after = db.workflowTerminalParentRevisions.get(key);

    // A queued engine write after the cancel cannot reopen the erased run.
    await expect(persist(workflows, run, 'running')).rejects.toBeInstanceOf(WorkflowStaleSnapshotPersistError);
    expect(after?.generation).toBe((before?.generation ?? 0) + 2);
    expect(after?.terminalStatus).toBe('canceled');
    expect(after?.erasureCancelled).toBe(true);
    for (const status of ['running', 'suspended'] as const) {
      await expect(workflows.updateWorkflowState({ ...run, opts: { status } })).rejects.toBeInstanceOf(
        WorkflowStaleSnapshotPersistError,
      );
      expect(db.workflowTerminalParentRevisions.get(key)).toEqual(after);
    }
    await expect(workflows.loadWorkflowSnapshot(run)).resolves.toMatchObject({ status: 'canceled' });
    await expect(workflows.updateWorkflowState({ ...run, opts: { status: 'canceled' } })).resolves.toMatchObject({
      status: 'canceled',
    });

    // Matching canceled writes remain valid. An inherited status getter can
    // reenter persist, so an omitted resource must use the latest row's owner.
    const canceled = (await workflows.loadWorkflowSnapshot(run))!;
    const { status: _status, ...snapshot } = canceled;
    const nextResource = 'workspace-2:user-1';
    let reentered = false;
    let reentrantWrite: Promise<void> | undefined;
    const statusReader = {
      get status() {
        if (!reentered) {
          reentered = true;
          reentrantWrite = workflows.persistWorkflowSnapshot({
            ...run,
            resourceId: nextResource,
            snapshot: canceled,
          });
        }
        return 'canceled';
      },
    };
    await workflows.persistWorkflowSnapshot({
      ...run,
      snapshot: Object.assign(Object.create(statusReader), snapshot),
    });
    await reentrantWrite;
    expect(reentered).toBe(true);
    expect(db.workflows.get(key)?.resourceId).toBe(nextResource);

    // The checked status must remain data even when a non-configurable,
    // non-enumerable accessor would return a different status on its next read.
    let statusReads = 0;
    const executionStateReader: Partial<WorkflowRunState> = {
      get cancelRequest() {
        this.status = 'running';
        return undefined;
      },
    };
    const changingStatusSnapshot = Object.defineProperty(
      Object.assign(Object.create(executionStateReader), snapshot),
      'status',
      { get: () => (statusReads++ === 0 ? 'canceled' : 'running') },
    );
    await workflows.persistWorkflowSnapshot({ ...run, snapshot: changingStatusSnapshot });
    expect((await workflows.loadWorkflowSnapshot(run))?.status).toBe('canceled');
    // A retained authority getter must not receive the live canceled snapshot.
    await expect(workflows.getWorkflowExecutionState(run)).resolves.toMatchObject({ status: 'canceled' });
    expect((await workflows.loadWorkflowSnapshot(run))?.status).toBe('canceled');
    await expect(cancel(workflows, run, nextResource)).resolves.toEqual({ status: 'settled' });
    await expect(
      workflows.deleteWorkflowRunClosure({ ...run, expectedResourceId: nextResource }),
    ).resolves.toMatchObject({
      status: 'deleted',
    });
    await persist(workflows, run, 'running');
    await workflows.updateWorkflowState({ ...run, opts: { status: 'suspended' } });
    await expect(workflows.loadWorkflowSnapshot(run)).resolves.toMatchObject({ status: 'suspended' });
  });

  it('refuses a child ancestry admission when the parent is cancelled while its graph is read', async () => {
    const { db, workflows } = setup();
    const parent = { workflowName: 'deep-scan', runId: 'run-parent-raced' };
    const child = { workflowName: 'deep-scan-child', runId: 'run-child-raced' };
    await persist(workflows, parent, 'running');
    await persist(workflows, child, 'running');
    // The parent's stored graph is read through a getter that cancels the
    // parent first, as a cancel interleaving with the admission would.
    const parentKey = JSON.stringify([parent.workflowName, parent.runId]);
    const record = db.workflows.get(parentKey)!;
    const { serializedStepGraph: _graph, ...rest } = record.snapshot as WorkflowRunState;
    let cancelled = false;
    const graphReader = {
      get serializedStepGraph() {
        if (!cancelled) {
          cancelled = true;
          void cancel(workflows, parent);
        }
        return NESTED_GRAPH;
      },
    };
    db.workflows.set(parentKey, { ...record, snapshot: Object.assign(Object.create(graphReader), rest) });

    await expect(
      workflows.persistWorkflowTerminalRecoveryAncestry({
        ...child,
        ancestry: [
          {
            version: 1,
            childWorkflowName: child.workflowName,
            childRunId: child.runId,
            parentWorkflowName: parent.workflowName,
            parentRunId: parent.runId,
            parentGraphFingerprint: createWorkflowTerminalGraphFingerprint(NESTED_GRAPH),
            source: { kind: 'step', stepId: NESTED_STEP, executionPath: [0] },
            inputPointer: { kind: 'parent-source-payload', stepId: NESTED_STEP },
            resultPointer: { kind: 'retained-terminal-result', ...child },
            resumeMetadata: { wasResume: false, resumeSteps: [] },
          },
        ],
      }),
    ).rejects.toThrow('Workflow terminal recovery ancestry parent evidence is unavailable');
    expect(cancelled).toBe(true);
    await expect(workflows.getWorkflowTerminalRecoveryAncestry(child)).resolves.toEqual({
      status: 'missing_ancestry',
    });
    await expect(workflows.loadWorkflowSnapshot(parent)).resolves.toMatchObject({ status: 'canceled' });
  });
});
