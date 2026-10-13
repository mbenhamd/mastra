import { describe, expect, it } from 'vitest';
import type { WorkflowRunState } from '../../../workflows';
import { InMemoryStore } from '../../mock';
import { WorkflowSnapshotHandoffFenceError } from '../../workflow-snapshot-handoff';

const snapshot = (runId: string, status: WorkflowRunState['status'], value: unknown = {}): WorkflowRunState =>
  ({
    runId,
    status,
    value,
    context: {},
    activePaths: [],
    activeStepsPath: {},
    suspendedPaths: {},
    resumeLabels: {},
    serializedStepGraph: [],
    waitingPaths: {},
    timestamp: Date.now(),
  }) as WorkflowRunState;

const isRunState = (value: unknown): value is WorkflowRunState =>
  typeof value === 'object' && value !== null && 'runId' in value && 'status' in value;

async function claimedRun(workflowName: string, runId: string, mutationFence = 'owner-fence') {
  const store = new InMemoryStore();
  const workflows = (await store.getStore('workflows'))!;
  const canonical = snapshot(runId, 'success', { native: true });
  await workflows.persistWorkflowSnapshot({ workflowName, runId, snapshot: canonical });
  const pending = snapshot(runId, 'waiting', { phase: 'claimed' });
  const owner = { workflowName, runId, resourceId: 'resource-1', mutationFence };
  const target = { workflowName, runId };
  const claim = (fence: string, value: unknown) =>
    workflows.claimWorkflowSnapshotHandoff({
      ...owner,
      mutationFence: fence,
      expectedCanonical: { kind: 'present', snapshot: canonical },
      snapshot: value === pending.value ? pending : isRunState(value) ? value : snapshot(runId, 'waiting', value),
    });
  await expect(claim(mutationFence, pending.value)).resolves.toMatchObject({
    status: 'created',
    record: { claimGeneration: 1 },
  });
  const complete = (fence = mutationFence, expectedSnapshot = pending) =>
    workflows.completeWorkflowSnapshotHandoff({
      ...owner,
      mutationFence: fence,
      expectedResourceId: owner.resourceId,
      expectedSnapshot,
      snapshot: snapshot(runId, 'success', { phase: 'delivered' }),
    });
  return { workflows, owner, target, pending, claim, complete };
}

describe('workflow snapshot handoff release', () => {
  it('advertises the release protocol version', async () => {
    const workflows = (await new InMemoryStore().getStore('workflows'))!;
    expect(workflows.getWorkflowSnapshotHandoffCapabilities()).toMatchObject({ handoffVersion: 1, releaseVersion: 1 });
  });

  it('releases a completed handoff without the fence, lifting every fence but keeping its outcome', async () => {
    const { workflows, owner, target, claim, complete } = await claimedRun('release-completed', 'run-1');
    await expect(complete()).resolves.toMatchObject({ status: 'completed' });
    await expect(workflows.deleteWorkflowRunById(target)).rejects.toBeInstanceOf(WorkflowSnapshotHandoffFenceError);

    const released = await workflows.releaseWorkflowSnapshotHandoff({ ...target, expectedClaimGeneration: 1 });
    expect(released).toMatchObject({
      status: 'released',
      runDeleted: false,
      record: { status: 'completed', claimGeneration: 1, snapshot: { value: { phase: 'delivered' } } },
    });
    expect(released.status === 'released' && typeof released.record.releasedAt).toBe('number');
    await expect(workflows.listWorkflowSnapshotHandoffs({ workflowName: target.workflowName })).resolves.toEqual({
      records: [],
      hasMore: false,
    });

    // A retry of the same delivery still finds its settled outcome; the run stays unfenced.
    await expect(claim(owner.mutationFence, { phase: 'retried' })).resolves.toMatchObject({
      status: 'completed',
      record: { claimGeneration: 1, snapshot: { value: { phase: 'delivered' } } },
    });

    await workflows.persistWorkflowSnapshot({
      ...target,
      snapshot: snapshot(target.runId, 'success', { phase: 'released-write' }),
    });
    await expect(workflows.loadWorkflowSnapshot(target)).resolves.toMatchObject({
      value: { phase: 'released-write' },
    });
    await workflows.deleteWorkflowRunById(target);
    await expect(workflows.loadWorkflowSnapshot(target)).resolves.toBeNull();
  });

  it('refuses to release an in-flight handoff without the matching fence', async () => {
    const { workflows, owner, target, pending, complete } = await claimedRun('release-pending', 'run-1');

    await expect(
      workflows.releaseWorkflowSnapshotHandoff({ ...target, expectedClaimGeneration: 1 }),
    ).resolves.toMatchObject({ status: 'conflict', record: { status: 'pending', mutationFence: 'owner-fence' } });
    await expect(
      workflows.releaseWorkflowSnapshotHandoff({ ...target, expectedClaimGeneration: 1, mutationFence: 'other-fence' }),
    ).resolves.toMatchObject({ status: 'conflict', record: { status: 'pending' } });
    await expect(workflows.deleteWorkflowRunById(target)).rejects.toBeInstanceOf(WorkflowSnapshotHandoffFenceError);

    await expect(
      workflows.releaseWorkflowSnapshotHandoff({
        ...target,
        expectedClaimGeneration: 1,
        mutationFence: owner.mutationFence,
      }),
    ).resolves.toMatchObject({ status: 'released', record: { status: 'pending' } });
    // A released pending claim takes no further steps.
    await expect(complete()).resolves.toMatchObject({ status: 'conflict' });
    await expect(
      workflows.transitionWorkflowSnapshotHandoff({
        ...owner,
        expectedResourceId: owner.resourceId,
        expectedSnapshot: pending,
        snapshot: snapshot(target.runId, 'waiting', { phase: 'late' }),
      }),
    ).resolves.toMatchObject({ status: 'conflict' });
    await workflows.deleteWorkflowRunById(target);
    await expect(workflows.loadWorkflowSnapshot(target)).resolves.toBeNull();
  });

  it('refuses a completed release whose supplied fence belongs to another owner', async () => {
    const { workflows, target, complete } = await claimedRun('release-foreign', 'run-1');
    await complete();
    await expect(
      workflows.releaseWorkflowSnapshotHandoff({ ...target, expectedClaimGeneration: 1, mutationFence: 'other-fence' }),
    ).resolves.toMatchObject({ status: 'conflict', record: { status: 'completed' } });
  });

  it('answers a retried release of the same claim with already_released, and a run without a row with missing', async () => {
    const { workflows, owner, target, complete } = await claimedRun('release-twice', 'run-1');
    await complete();
    const release = () => workflows.releaseWorkflowSnapshotHandoff({ ...owner, expectedClaimGeneration: 1 });

    const first = await release();
    expect(first).toMatchObject({ status: 'released' });
    const second = await release();
    expect(second).toMatchObject({ status: 'already_released', runDeleted: false });
    expect(second.status === 'already_released' && second.record).toEqual(first.status === 'released' && first.record);

    await expect(
      workflows.releaseWorkflowSnapshotHandoff({
        workflowName: 'never-claimed',
        runId: 'run-1',
        expectedClaimGeneration: 1,
      }),
    ).resolves.toEqual({ status: 'missing' });
  });

  it('refuses a replayed release once the run was claimed again under the same fence', async () => {
    const { workflows, owner, target, claim } = await claimedRun('release-replay', 'run-1');
    const release = { ...target, expectedClaimGeneration: 1, mutationFence: owner.mutationFence };
    await expect(workflows.releaseWorkflowSnapshotHandoff(release)).resolves.toMatchObject({ status: 'released' });

    await expect(claim(owner.mutationFence, { phase: 'reclaimed' })).resolves.toMatchObject({
      status: 'created',
      record: { status: 'pending', claimGeneration: 2, snapshot: { value: { phase: 'reclaimed' } } },
    });
    const replay = await workflows.releaseWorkflowSnapshotHandoff(release);
    expect(replay).toMatchObject({ status: 'conflict', record: { status: 'pending', claimGeneration: 2 } });
    expect(replay.status === 'conflict' && replay.record.releasedAt).toBeUndefined();
    await expect(workflows.listWorkflowSnapshotHandoffs({ workflowName: target.workflowName })).resolves.toMatchObject({
      records: [{ claimGeneration: 2, status: 'pending' }],
    });
    await expect(workflows.deleteWorkflowRunById(target)).rejects.toBeInstanceOf(WorkflowSnapshotHandoffFenceError);
  });

  it('refuses a stale fenceless release of a newer completed claim', async () => {
    const { workflows, target, claim, complete } = await claimedRun('release-stale', 'run-1');
    await complete();
    await workflows.releaseWorkflowSnapshotHandoff({ ...target, expectedClaimGeneration: 1 });
    const second = snapshot(target.runId, 'waiting', { phase: 'second' });
    const reclaimed = await claim('second-owner', second);
    expect(reclaimed).toMatchObject({ status: 'created', record: { claimGeneration: 2 } });
    await expect(complete('second-owner', second)).resolves.toMatchObject({
      status: 'completed',
      record: { claimGeneration: 2 },
    });

    await expect(
      workflows.releaseWorkflowSnapshotHandoff({ ...target, expectedClaimGeneration: 1 }),
    ).resolves.toMatchObject({ status: 'conflict', record: { status: 'completed', claimGeneration: 2 } });
    await expect(workflows.deleteWorkflowRunById(target)).rejects.toBeInstanceOf(WorkflowSnapshotHandoffFenceError);
    await expect(
      workflows.releaseWorkflowSnapshotHandoff({ ...target, expectedClaimGeneration: 2 }),
    ).resolves.toMatchObject({ status: 'released' });
  });

  it('deletes the run in the same step when asked, and a late retry never deletes later writes', async () => {
    const { workflows, target, complete } = await claimedRun('release-delete', 'run-1');
    await complete();
    const release = { ...target, expectedClaimGeneration: 1, deleteRun: true };

    await expect(workflows.releaseWorkflowSnapshotHandoff(release)).resolves.toMatchObject({
      status: 'released',
      runDeleted: true,
    });
    await expect(workflows.loadWorkflowSnapshot(target)).resolves.toBeNull();
    // The release lets native writes through; a delayed retry must not delete them.
    await workflows.persistWorkflowSnapshot({
      ...target,
      snapshot: snapshot(target.runId, 'success', { phase: 'after-release' }),
    });
    await expect(workflows.releaseWorkflowSnapshotHandoff(release)).resolves.toMatchObject({
      status: 'already_released',
      runDeleted: false,
    });
    await expect(workflows.loadWorkflowSnapshot(target)).resolves.toMatchObject({
      value: { phase: 'after-release' },
    });
  });

  it('replaces the kept record of a released completed claim when another fence claims the run', async () => {
    const { workflows, owner, target, claim, complete } = await claimedRun('release-overwrite', 'run-1');
    await complete();
    await workflows.releaseWorkflowSnapshotHandoff({ ...target, expectedClaimGeneration: 1 });

    await expect(claim('second-owner', { phase: 'second' })).resolves.toMatchObject({
      status: 'created',
      record: { claimGeneration: 2, status: 'pending', mutationFence: 'second-owner' },
    });
    // Only the latest fence's outcome is kept: the first owner's retry now meets the new claim.
    await expect(claim(owner.mutationFence, { phase: 'retried' })).resolves.toMatchObject({
      status: 'conflict',
      record: { claimGeneration: 2, mutationFence: 'second-owner' },
    });
  });

  it('settles either order of a transition and a release by the fence holder', async () => {
    const transitionInput = (
      owner: { workflowName: string; runId: string; resourceId: string; mutationFence: string },
      pending: WorkflowRunState,
    ) => ({
      ...owner,
      expectedResourceId: owner.resourceId,
      expectedSnapshot: pending,
      snapshot: snapshot(owner.runId, 'waiting', { phase: 'transitioned' }),
    });

    const first = await claimedRun('order-transition-first', 'run-1');
    await expect(
      first.workflows.transitionWorkflowSnapshotHandoff(transitionInput(first.owner, first.pending)),
    ).resolves.toMatchObject({ status: 'transitioned' });
    await expect(
      first.workflows.releaseWorkflowSnapshotHandoff({ ...first.owner, expectedClaimGeneration: 1 }),
    ).resolves.toMatchObject({ status: 'released', record: { snapshot: { value: { phase: 'transitioned' } } } });

    const second = await claimedRun('order-release-first', 'run-1');
    await expect(
      second.workflows.releaseWorkflowSnapshotHandoff({ ...second.owner, expectedClaimGeneration: 1 }),
    ).resolves.toMatchObject({ status: 'released', record: { snapshot: { value: { phase: 'claimed' } } } });
    await expect(
      second.workflows.transitionWorkflowSnapshotHandoff(transitionInput(second.owner, second.pending)),
    ).resolves.toMatchObject({ status: 'conflict', record: { snapshot: { value: { phase: 'claimed' } } } });
  });

  it('rejects malformed tokens, fences and identities before touching the row', async () => {
    const { workflows, owner, target } = await claimedRun('release-invalid', 'run-1');
    await expect(
      workflows.releaseWorkflowSnapshotHandoff({
        ...target,
        expectedClaimGeneration: 0,
        mutationFence: owner.mutationFence,
      }),
    ).rejects.toThrow(RangeError);
    await expect(
      workflows.releaseWorkflowSnapshotHandoff({ ...target, expectedClaimGeneration: 1, mutationFence: '' }),
    ).rejects.toThrow(TypeError);
    await expect(
      workflows.releaseWorkflowSnapshotHandoff({ ...target, runId: '\ud800', expectedClaimGeneration: 1 }),
    ).rejects.toThrow(TypeError);
    await expect(workflows.listWorkflowSnapshotHandoffs({ workflowName: target.workflowName })).resolves.toMatchObject({
      records: [{ status: 'pending' }],
    });
  });
});
