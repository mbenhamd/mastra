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

  it('preserves refused snapshots and fences an already settled run for erasure', async () => {
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
    await expect(workflows.updateWorkflowState({ ...settled, opts: { status: 'running' } })).rejects.toBeInstanceOf(
      WorkflowStaleSnapshotPersistError,
    );
    await expect(persist(workflows, settled, 'running')).rejects.toBeInstanceOf(WorkflowStaleSnapshotPersistError);
    await expect(workflows.updateWorkflowState({ ...settled, opts: { status: 'failed' } })).resolves.toMatchObject({
      status: 'failed',
      result: { draft: `${settled.runId} draft` },
    });
    await expect(cancel(workflows, settled)).resolves.toEqual({ status: 'settled' });
    await expect(
      workflows.deleteWorkflowRunClosure({ ...settled, expectedResourceId: RESOURCE }),
    ).resolves.toMatchObject({
      status: 'deleted',
    });
    await persist(workflows, settled, 'running');
    await expect(workflows.loadWorkflowSnapshot(settled)).resolves.toMatchObject({ status: 'running' });
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

    // A failed generation guard must not give its retained getter the live row.
    const generationReader: Partial<WorkflowRunState> = {
      get executionGeneration() {
        this.status = 'running';
        return 'retained-generation';
      },
    };
    await workflows.persistWorkflowSnapshot({
      ...run,
      snapshot: Object.assign(Object.create(generationReader), snapshot, { status: 'canceled' }),
    });
    await expect(
      workflows.persistWorkflowSnapshot({
        ...run,
        snapshot: canceled,
        expectedExecutionGeneration: 'other-generation',
      }),
    ).rejects.toBeInstanceOf(WorkflowStaleSnapshotPersistError);
    expect((await workflows.loadWorkflowSnapshot(run))?.status).toBe('canceled');

    // Status evaluation can introduce an enumerable accessor after the first
    // clone. Resolve it before storing, rather than on a later live-row read.
    const introducingStatusReader = {
      get status() {
        Object.defineProperty(this, 'executionGeneration', {
          enumerable: true,
          get() {
            this.status = 'running';
            return 'introduced-generation';
          },
        });
        return 'canceled';
      },
    };
    await workflows.persistWorkflowSnapshot({
      ...run,
      snapshot: Object.assign(Object.create(introducingStatusReader), snapshot),
    });
    await expect(workflows.getWorkflowExecutionState(run)).resolves.toEqual({
      status: 'canceled',
      executionGeneration: 'introduced-generation',
    });
    expect((await workflows.loadWorkflowSnapshot(run))?.status).toBe('canceled');

    // The selected run is stored as plain data: the accessor that would
    // reopen it during result merging is read once, at persist, so the
    // merged result keeps the row canceled.
    const { context: _context, ...resultFields } = canceled;
    const context = createEmptyWorkflowSnapshot(run.runId).context;
    const contextReader: Partial<WorkflowRunState> = {
      get context() {
        this.status = 'running';
        return context;
      },
    };
    await workflows.persistWorkflowSnapshot({
      ...run,
      snapshot: Object.assign(Object.create(contextReader), resultFields),
    });
    const resultUpdate = {
      ...run,
      stepId: 'step',
      result: { status: 'success', output: 'done' } as const,
      requestContext: {},
    };
    await expect(workflows.updateWorkflowResults(resultUpdate)).resolves.toEqual({
      step: { status: 'success', output: 'done' },
    });
    expect((await workflows.loadWorkflowSnapshot(run))?.status).toBe('canceled');

    let firstContextReceiver: object | undefined;
    const returnContextReader: Partial<WorkflowRunState> = {
      get context() {
        firstContextReceiver ??= this;
        if (this !== firstContextReceiver) this.status = 'running';
        return context;
      },
    };
    await workflows.persistWorkflowSnapshot({
      ...run,
      snapshot: Object.assign(Object.create(returnContextReader), resultFields),
    });
    await expect(workflows.updateWorkflowResults(resultUpdate)).resolves.toEqual({
      step: { status: 'success', output: 'done' },
    });
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

  it('refuses ancestry admission when its parent or child lifetime changes while the graph is read', async () => {
    const { workflows } = setup();
    const parent = { workflowName: 'deep-scan', runId: 'run-parent-raced' };
    const child = { workflowName: 'deep-scan-child', runId: 'run-child-raced' };
    await persist(workflows, parent, 'running');
    await persist(workflows, child, 'running');
    // The parent's stored graph is read through a getter that cancels the
    // parent first, as a cancel interleaving with the admission would.
    const { serializedStepGraph: _graph, ...rest } = (await workflows.loadWorkflowSnapshot(parent))!;
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
    await workflows.persistWorkflowSnapshot({
      ...parent,
      snapshot: Object.assign(Object.create(graphReader), rest),
    });

    const admit = () =>
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
      });
    await expect(admit()).rejects.toThrow('Workflow terminal recovery ancestry parent evidence is unavailable');
    expect(cancelled).toBe(true);
    await expect(workflows.getWorkflowTerminalRecoveryAncestry(child)).resolves.toEqual({
      status: 'missing_ancestry',
    });
    await expect(workflows.loadWorkflowSnapshot(parent)).resolves.toMatchObject({ status: 'canceled' });

    await workflows.deleteWorkflowRunClosure({ ...parent, expectedResourceId: RESOURCE });
    let childReplaced = false;
    let childCancellation: ReturnType<typeof cancel> | undefined;
    let childDeletion: ReturnType<WorkflowsInMemory['deleteWorkflowRunClosure']> | undefined;
    let childReplacement: Promise<void> | undefined;
    const replacingGraphReader = {
      get serializedStepGraph() {
        if (!childReplaced) {
          childReplaced = true;
          childCancellation = cancel(workflows, child);
          childDeletion = workflows.deleteWorkflowRunClosure({ ...child, expectedResourceId: RESOURCE });
          childReplacement = persist(workflows, child, 'running');
        }
        return NESTED_GRAPH;
      },
    };
    await workflows.persistWorkflowSnapshot({
      ...parent,
      resourceId: RESOURCE,
      snapshot: Object.assign(Object.create(replacingGraphReader), rest),
    });
    await expect(admit()).resolves.toEqual({ status: 'missing_run' });
    await expect(childCancellation).resolves.toEqual({ status: 'cancelled' });
    await expect(childDeletion).resolves.toMatchObject({ status: 'deleted' });
    await childReplacement;
    await expect(workflows.getWorkflowTerminalRecoveryAncestry(child)).resolves.toEqual({ status: 'missing_ancestry' });
    await expect(workflows.loadWorkflowSnapshot(child)).resolves.toMatchObject({ status: 'running' });
    // A fresh request can bind the replacement lifetime.
    await expect(admit()).resolves.toMatchObject({ status: 'persisted' });
  });

  it('reads the status on a detached copy, so a refused cancel changes nothing', async () => {
    const { db, workflows } = setup();
    const run = { workflowName: 'research', runId: 'run-status-accessor' };
    await persist(workflows, run, 'running');
    const key = JSON.stringify([run.workflowName, run.runId]);
    const record = db.workflows.get(key)!;
    const { status: _status, ...rest } = record.snapshot as WorkflowRunState;
    // An inherited status accessor that rewrites its receiver when read.
    const accessor = {
      get status() {
        Object.defineProperty(this, 'status', {
          value: 'success',
          enumerable: true,
          writable: true,
          configurable: true,
        });
        return 'running';
      },
    };
    const stored = Object.assign(Object.create(accessor), rest);
    db.workflows.set(key, { ...record, snapshot: stored });
    const revisionBefore = structuredClone(db.workflowTerminalParentRevisions.get(key));

    await expect(cancel(workflows, run, 'workspace-2:user-1')).resolves.toEqual({ status: 'resource_mismatch' });
    expect(db.workflows.get(key)?.snapshot).toBe(stored);
    expect(Object.getOwnPropertyDescriptor(stored, 'status')).toBeUndefined();
    expect(db.workflowTerminalParentRevisions.get(key)).toEqual(revisionBefore);
  });

  it('refuses a nested ownership bind that would reopen a cancelled run through a context accessor', async () => {
    const { db, workflows } = setup();
    const run = { workflowName: 'research', runId: 'run-bind-accessor' };
    await persist(workflows, run, 'suspended');
    await expect(cancel(workflows, run)).resolves.toEqual({ status: 'cancelled' });
    const key = JSON.stringify([run.workflowName, run.runId]);
    const record = db.workflows.get(key)!;
    const { context, ...rest } = record.snapshot as WorkflowRunState;
    // The stored canceled snapshot's context is read through an accessor
    // that reopens its receiver as running.
    const accessor = {
      get context() {
        (this as { status: string }).status = 'running';
        return context;
      },
    };
    db.workflows.set(key, { ...record, snapshot: Object.assign(Object.create(accessor), rest) });

    await expect(
      workflows.bindWorkflowNestedRunOwnership({
        ...run,
        stepId: 'nested',
        nestedRunId: 'run-bind-accessor-child',
        result: { status: 'running', payload: {} },
        requestContext: {},
      }),
    ).rejects.toBeInstanceOf(WorkflowStaleSnapshotPersistError);
    expect(db.workflowTerminalParentRevisions.get(key)).toMatchObject({ terminalStatus: 'canceled' });
    await expect(workflows.deleteWorkflowRunClosure({ ...run, expectedResourceId: RESOURCE })).resolves.toMatchObject({
      status: 'deleted',
    });
  });

  it('keeps nested stored state untouched when a status accessor runs during a refused cancel', async () => {
    const { db, workflows } = setup();
    const run = { workflowName: 'research', runId: 'run-nested-accessor' };
    await persist(workflows, run, 'running');
    const key = JSON.stringify([run.workflowName, run.runId]);
    const record = db.workflows.get(key)!;
    const { status: _status, ...rest } = record.snapshot as WorkflowRunState;
    const probe = { reads: 1 };
    // An inherited status accessor that writes through nested state.
    const accessor = {
      get status() {
        (this as { context: { probe: { reads: number } } }).context.probe.reads += 1;
        return 'running';
      },
    };
    const stored = Object.assign(Object.create(accessor), { ...rest, context: { ...rest.context, probe } });
    db.workflows.set(key, { ...record, snapshot: stored });
    const revisionBefore = structuredClone(db.workflowTerminalParentRevisions.get(key));

    await expect(cancel(workflows, run, 'workspace-2:user-1')).resolves.toEqual({ status: 'resource_mismatch' });
    expect(probe.reads).toBe(1);
    expect(db.workflowTerminalParentRevisions.get(key)).toEqual(revisionBefore);
  });

  it('latches a settled run without revision evidence before fencing it, and clears the fence on closure delete', async () => {
    const { db, workflows } = setup();
    const run = { workflowName: 'research', runId: 'run-settled-unlatched' };
    const key = JSON.stringify([run.workflowName, run.runId]);
    // A settled run stored before any revision evidence existed for it.
    db.workflows.set(key, {
      workflow_name: run.workflowName,
      run_id: run.runId,
      resourceId: RESOURCE,
      snapshot: { ...createEmptyWorkflowSnapshot(run.runId), status: 'success' },
      createdAt: new Date(),
      updatedAt: new Date(),
    });
    db.workflowTerminalParentRevisions.delete(key);

    await expect(cancel(workflows, run)).resolves.toEqual({ status: 'settled' });
    expect(db.workflowTerminalParentRevisions.get(key)).toMatchObject({
      terminalStatus: 'success',
      erasureCancelled: true,
    });
    await expect(persist(workflows, run, 'running')).rejects.toBeInstanceOf(WorkflowStaleSnapshotPersistError);
    await expect(workflows.deleteWorkflowRunClosure({ ...run, expectedResourceId: RESOURCE })).resolves.toMatchObject({
      status: 'deleted',
    });
    expect(db.workflowTerminalParentRevisions.get(key)).not.toHaveProperty('erasureCancelled');
    // The same run id is usable again.
    await persist(workflows, run, 'running');
    await expect(workflows.loadWorkflowSnapshot(run)).resolves.toMatchObject({ status: 'running' });
  });

  it('refuses a resume finalization that would reopen a settled run selected for erasure', async () => {
    const { db, workflows } = setup();
    const run = { workflowName: 'research', runId: 'run-resume-finalize' };
    await workflows.persistWorkflowSnapshot({
      ...run,
      resourceId: RESOURCE,
      snapshot: {
        ...createEmptyWorkflowSnapshot(run.runId),
        status: 'suspended',
        executionGeneration: 'gen-1',
        lifecycleResumeAttempt: 0,
        lifecycleStepStates: {},
      } as WorkflowRunState,
    });
    const fence = {
      ...run,
      resumeOperationHash: `sha256:${'0'.repeat(64)}` as `sha256:${string}`,
      executionGeneration: 'gen-1',
      lifecycleResumeAttempt: 0,
      lifecycleStepStates: {},
    };
    await expect(
      workflows.admitWorkflowResume({
        ...fence,
        nextLifecycleResumeAttempt: 1,
        operationReplayContext: { version: 1, steps: [] },
      }),
    ).resolves.toMatchObject({ status: 'admitted' });
    // A state update settles the run while its resume checkpoint is retained.
    await workflows.updateWorkflowState({ ...run, opts: { status: 'failed' } });
    await expect(cancel(workflows, run)).resolves.toEqual({ status: 'settled' });

    await expect(
      workflows.finalizeWorkflowResume({
        ...fence,
        lifecycleResumeAttempt: 1,
        shouldPersistSnapshot: true,
        snapshot: {
          ...createEmptyWorkflowSnapshot(run.runId),
          status: 'running',
          executionGeneration: 'gen-1',
          lifecycleResumeAttempt: 1,
          lifecycleStepStates: {},
        } as WorkflowRunState,
        receiptKey: 'receipt-1',
        result: { status: 'success', result: {} } as never,
      }),
    ).rejects.toBeInstanceOf(WorkflowStaleSnapshotPersistError);
    await expect(workflows.loadWorkflowSnapshot(run)).resolves.toMatchObject({ status: 'failed' });
    expect(db.workflowTerminalParentRevisions.get(JSON.stringify([run.workflowName, run.runId]))).toMatchObject({
      terminalStatus: 'failed',
    });
    await expect(workflows.deleteWorkflowRunClosure({ ...run, expectedResourceId: RESOURCE })).resolves.toMatchObject({
      status: 'deleted',
    });
  });

  it('never wedges erasure on a stateful status accessor: the checked status is the stored one', async () => {
    const { db, workflows } = setup();
    const run = { workflowName: 'research', runId: 'run-stateful-status' };
    await persist(workflows, run, 'failed');
    const key = JSON.stringify([run.workflowName, run.runId]);
    const record = db.workflows.get(key)!;
    const { status: _status, ...rest } = record.snapshot as WorkflowRunState;
    // Answers `failed` once, then `running` on every later read.
    let reads = 0;
    const flipping = {
      get status() {
        return reads++ === 0 ? 'failed' : 'running';
      },
    };
    db.workflows.set(key, { ...record, snapshot: Object.assign(Object.create(flipping), rest) });

    await expect(cancel(workflows, run)).resolves.toEqual({ status: 'settled' });
    await expect(workflows.loadWorkflowSnapshot(run)).resolves.toMatchObject({ status: 'failed' });
    // A queued write whose status flips after its first read stays refused or stored as checked.
    let writeReads = 0;
    const flippingWrite = {
      get status() {
        return writeReads++ === 0 ? 'failed' : 'running';
      },
    };
    await workflows.persistWorkflowSnapshot({
      ...run,
      resourceId: RESOURCE,
      snapshot: Object.assign(Object.create(flippingWrite), rest),
    });
    await expect(workflows.loadWorkflowSnapshot(run)).resolves.toMatchObject({ status: 'failed' });
    await expect(workflows.deleteWorkflowRunClosure({ ...run, expectedResourceId: RESOURCE })).resolves.toMatchObject({
      status: 'deleted',
    });
  });

  it('cancels a run an ordinary write reopened after it settled, so erasure cannot wedge', async () => {
    const { db, workflows } = setup();
    const run = { workflowName: 'research', runId: 'run-reopened' };
    await persist(workflows, run, 'success');
    // This store lets an ordinary write reopen a settled run; its latch stays.
    await workflows.updateWorkflowState({ ...run, opts: { status: 'running' } });
    const key = JSON.stringify([run.workflowName, run.runId]);
    expect(db.workflowTerminalParentRevisions.get(key)?.terminalStatus).toBe('success');

    await expect(cancel(workflows, run)).resolves.toEqual({ status: 'cancelled' });
    expect(db.workflowTerminalParentRevisions.get(key)).toMatchObject({
      terminalStatus: 'canceled',
      erasureCancelled: true,
    });
    await expect(workflows.deleteWorkflowRunClosure({ ...run, expectedResourceId: RESOURCE })).resolves.toMatchObject({
      status: 'deleted',
    });
  });

  it('keeps the built-in payload values of a settled run selected for erasure', async () => {
    const { workflows } = setup();
    const run = { workflowName: 'research', runId: 'run-built-ins' };
    const finishedAt = new Date('2026-10-18T00:00:00.000Z');
    await workflows.persistWorkflowSnapshot({
      ...run,
      resourceId: RESOURCE,
      snapshot: {
        ...createEmptyWorkflowSnapshot(run.runId),
        status: 'success',
        result: {
          finishedAt,
          counts: new Map([['papers', 3]]),
          failure: new Error('kept'),
          failures: new Map([['first', new Error('first kept')]]),
          tags: new Set([new Error('tag kept')]),
          source: new URL('https://example.org/paper'),
        },
      },
    });

    const before = (await workflows.loadWorkflowSnapshot(run))!;
    expect((before.result as { source: URL }).source.href).toBe('https://example.org/paper');
    await expect(cancel(workflows, run)).resolves.toEqual({ status: 'settled' });
    const stored = (await workflows.loadWorkflowSnapshot(run))!;
    const result = stored.result as {
      finishedAt: Date;
      counts: Map<string, number>;
      failure: Error;
      failures: Map<string, Error>;
      tags: Set<Error>;
      source: URL;
    };
    expect(result.finishedAt).toEqual(finishedAt);
    expect(result.counts.get('papers')).toBe(3);
    expect(result.failure.message).toBe('kept');
    expect(result.failures.get('first')?.message).toBe('first kept');
    expect([...result.tags][0]?.message).toBe('tag kept');
    expect(result.source.href).toBe('https://example.org/paper');
  });

  it('finishes the erasure of a run reopened and then settled with another status', async () => {
    const { db, workflows } = setup();
    const run = { workflowName: 'research', runId: 'run-resettled' };
    await persist(workflows, run, 'success');
    await workflows.updateWorkflowState({ ...run, opts: { status: 'running' } });
    await workflows.updateWorkflowState({ ...run, opts: { status: 'failed' } });
    const key = JSON.stringify([run.workflowName, run.runId]);
    expect(db.workflowTerminalParentRevisions.get(key)?.terminalStatus).toBe('success');

    await expect(cancel(workflows, run)).resolves.toEqual({ status: 'settled' });
    expect(db.workflowTerminalParentRevisions.get(key)).toMatchObject({
      terminalStatus: 'failed',
      erasureCancelled: true,
    });
    await expect(workflows.deleteWorkflowRunClosure({ ...run, expectedResourceId: RESOURCE })).resolves.toMatchObject({
      status: 'deleted',
    });
  });
});
