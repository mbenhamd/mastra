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

async function claimedRun(workflowName: string, runId: string) {
  const store = new InMemoryStore();
  const workflows = (await store.getStore('workflows'))!;
  const canonical = snapshot(runId, 'success', { native: true });
  await workflows.persistWorkflowSnapshot({ workflowName, runId, snapshot: canonical });
  const pending = snapshot(runId, 'waiting', { phase: 'claimed' });
  const owner = {
    workflowName,
    runId,
    resourceId: 'resource-1',
    mutationFence: 'owner-fence',
  };
  await expect(
    workflows.claimWorkflowSnapshotHandoff({
      ...owner,
      expectedCanonical: { kind: 'present', snapshot: canonical },
      snapshot: pending,
    }),
  ).resolves.toMatchObject({ status: 'created' });
  const complete = () =>
    workflows.completeWorkflowSnapshotHandoff({
      ...owner,
      expectedResourceId: owner.resourceId,
      expectedSnapshot: pending,
      snapshot: snapshot(runId, 'success', { phase: 'delivered' }),
    });
  return { workflows, owner, pending, complete };
}

describe('workflow snapshot handoff release', () => {
  it('advertises the release protocol version', async () => {
    const workflows = (await new InMemoryStore().getStore('workflows'))!;
    expect(workflows.getWorkflowSnapshotHandoffCapabilities()).toMatchObject({ handoffVersion: 1, releaseVersion: 1 });
  });

  it('releases a completed handoff without the fence and lets run deletion proceed', async () => {
    const { workflows, complete } = await claimedRun('release-completed', 'run-1');
    await expect(complete()).resolves.toMatchObject({ status: 'completed' });
    await expect(
      workflows.deleteWorkflowRunById({ workflowName: 'release-completed', runId: 'run-1' }),
    ).rejects.toBeInstanceOf(WorkflowSnapshotHandoffFenceError);

    await expect(
      workflows.releaseWorkflowSnapshotHandoff({ workflowName: 'release-completed', runId: 'run-1' }),
    ).resolves.toMatchObject({
      status: 'released',
      record: { status: 'completed', mutationFence: 'owner-fence', snapshot: { value: { phase: 'delivered' } } },
    });
    await expect(workflows.listWorkflowSnapshotHandoffs({ workflowName: 'release-completed' })).resolves.toEqual({
      records: [],
      hasMore: false,
    });

    await workflows.deleteWorkflowRunById({ workflowName: 'release-completed', runId: 'run-1' });
    await expect(
      workflows.loadWorkflowSnapshot({ workflowName: 'release-completed', runId: 'run-1' }),
    ).resolves.toBeNull();
  });

  it('refuses to release an in-flight handoff without the matching fence', async () => {
    const { workflows, owner } = await claimedRun('release-pending', 'run-1');
    const target = { workflowName: owner.workflowName, runId: owner.runId };

    await expect(workflows.releaseWorkflowSnapshotHandoff(target)).resolves.toMatchObject({
      status: 'conflict',
      record: { status: 'pending', mutationFence: 'owner-fence' },
    });
    await expect(
      workflows.releaseWorkflowSnapshotHandoff({ ...target, mutationFence: 'other-fence' }),
    ).resolves.toMatchObject({ status: 'conflict', record: { status: 'pending' } });
    await expect(workflows.deleteWorkflowRunById(target)).rejects.toBeInstanceOf(WorkflowSnapshotHandoffFenceError);

    await expect(
      workflows.releaseWorkflowSnapshotHandoff({ ...target, mutationFence: owner.mutationFence }),
    ).resolves.toMatchObject({ status: 'released', record: { status: 'pending' } });
    await workflows.deleteWorkflowRunById(target);
    await expect(workflows.loadWorkflowSnapshot(target)).resolves.toBeNull();
  });

  it('refuses a completed release whose supplied fence belongs to another owner', async () => {
    const { workflows, owner, complete } = await claimedRun('release-foreign', 'run-1');
    await complete();
    await expect(
      workflows.releaseWorkflowSnapshotHandoff({
        workflowName: owner.workflowName,
        runId: owner.runId,
        mutationFence: 'other-fence',
      }),
    ).resolves.toMatchObject({ status: 'conflict', record: { status: 'completed' } });
  });

  it('is idempotent: a repeated release reports the row missing and changes nothing', async () => {
    const { workflows, owner, complete } = await claimedRun('release-twice', 'run-1');
    await complete();
    const release = () => workflows.releaseWorkflowSnapshotHandoff(owner);

    await expect(release()).resolves.toMatchObject({ status: 'released' });
    await expect(release()).resolves.toEqual({ status: 'missing' });
    await workflows.deleteWorkflowRunById({ workflowName: owner.workflowName, runId: owner.runId });
    await expect(release()).resolves.toEqual({ status: 'missing' });
  });

  it('serializes a release against a concurrent transition by the fence holder', async () => {
    const { workflows, owner, pending } = await claimedRun('release-race', 'run-1');
    const next = snapshot(owner.runId, 'waiting', { phase: 'transitioned' });
    const [transition, release] = await Promise.all([
      workflows.transitionWorkflowSnapshotHandoff({
        ...owner,
        expectedResourceId: owner.resourceId,
        expectedSnapshot: pending,
        snapshot: next,
      }),
      workflows.releaseWorkflowSnapshotHandoff(owner),
    ]);

    expect(release.status).toBe('released');
    if (transition.status === 'transitioned') {
      expect(release).toMatchObject({ record: { snapshot: { value: { phase: 'transitioned' } } } });
    } else {
      expect(transition).toEqual({ status: 'missing' });
      expect(release).toMatchObject({ record: { snapshot: { value: { phase: 'claimed' } } } });
    }
    await expect(workflows.listWorkflowSnapshotHandoffs({ workflowName: owner.workflowName })).resolves.toEqual({
      records: [],
      hasMore: false,
    });
  });

  it('rejects malformed fence and identity input before touching the row', async () => {
    const { workflows, owner } = await claimedRun('release-invalid', 'run-1');
    await expect(workflows.releaseWorkflowSnapshotHandoff({ ...owner, mutationFence: '' })).rejects.toThrow(TypeError);
    await expect(
      workflows.releaseWorkflowSnapshotHandoff({ workflowName: owner.workflowName, runId: '\ud800' }),
    ).rejects.toThrow(TypeError);
    await expect(workflows.listWorkflowSnapshotHandoffs({ workflowName: owner.workflowName })).resolves.toMatchObject({
      records: [{ status: 'pending' }],
    });
  });
});
