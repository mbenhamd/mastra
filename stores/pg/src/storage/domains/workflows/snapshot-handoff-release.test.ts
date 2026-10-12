import { randomUUID } from 'node:crypto';
import { WorkflowSnapshotHandoffFenceError } from '@mastra/core/storage';
import type { WorkflowRunState } from '@mastra/core/workflows';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { PostgresStore, WorkflowsPG } from '../../index';

const connectionString = process.env.DB_URL || 'postgresql://postgres:postgres@localhost:5434/mastra';

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

describe('workflow snapshot handoff release in PostgreSQL', () => {
  let store: PostgresStore;
  let workflows: WorkflowsPG;

  beforeAll(async () => {
    store = new PostgresStore({ id: `workflow-handoff-release-${randomUUID()}`, connectionString });
    await store.init();
    workflows = (await store.getStore('workflows'))!;
  }, 60000);

  afterAll(async () => {
    await store?.close();
  });

  async function claimedRun(prefix: string) {
    const workflowName = `${prefix}-${randomUUID()}`;
    const runId = randomUUID();
    const canonical = snapshot(runId, 'success', { native: true });
    await workflows.persistWorkflowSnapshot({ workflowName, runId, snapshot: canonical });
    const pending = snapshot(runId, 'waiting', { phase: 'claimed' });
    const owner = { workflowName, runId, resourceId: 'resource-1', mutationFence: `owner-${randomUUID()}` };
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
    return { owner, target: { workflowName, runId }, pending, complete };
  }

  const handoffRows = (workflowName: string, runId: string) =>
    store.db.any<{ status: string }>(
      `SELECT status FROM mastra_workflow_snapshot_handoffs WHERE workflow_name = $1 AND run_id = $2`,
      [workflowName, runId],
    );

  it('advertises the release protocol version', () => {
    expect(workflows.getWorkflowSnapshotHandoffCapabilities()).toMatchObject({ handoffVersion: 1, releaseVersion: 1 });
  });

  it('releases a completed handoff without the fence and lets run deletion proceed', async () => {
    const { target, complete } = await claimedRun('release-completed');
    await expect(complete()).resolves.toMatchObject({ status: 'completed' });
    await expect(workflows.deleteWorkflowRunById(target)).rejects.toBeInstanceOf(WorkflowSnapshotHandoffFenceError);

    await expect(workflows.releaseWorkflowSnapshotHandoff(target)).resolves.toMatchObject({
      status: 'released',
      record: { status: 'completed', snapshot: { value: { phase: 'delivered' } } },
    });
    await expect(handoffRows(target.workflowName, target.runId)).resolves.toEqual([]);

    await workflows.deleteWorkflowRunById(target);
    await expect(workflows.loadWorkflowSnapshot(target)).resolves.toBeNull();
  });

  it('refuses to release an in-flight handoff without the matching fence', async () => {
    const { owner, target } = await claimedRun('release-pending');

    await expect(workflows.releaseWorkflowSnapshotHandoff(target)).resolves.toMatchObject({
      status: 'conflict',
      record: { status: 'pending', mutationFence: owner.mutationFence },
    });
    await expect(
      workflows.releaseWorkflowSnapshotHandoff({ ...target, mutationFence: 'other-fence' }),
    ).resolves.toMatchObject({ status: 'conflict', record: { status: 'pending' } });
    await expect(handoffRows(target.workflowName, target.runId)).resolves.toEqual([{ status: 'pending' }]);
    await expect(workflows.deleteWorkflowRunById(target)).rejects.toBeInstanceOf(WorkflowSnapshotHandoffFenceError);

    await expect(
      workflows.releaseWorkflowSnapshotHandoff({ ...target, mutationFence: owner.mutationFence }),
    ).resolves.toMatchObject({ status: 'released', record: { status: 'pending' } });
    await workflows.deleteWorkflowRunById(target);
    await expect(workflows.loadWorkflowSnapshot(target)).resolves.toBeNull();
  });

  it('refuses a completed release whose supplied fence belongs to another owner', async () => {
    const { target, complete } = await claimedRun('release-foreign');
    await complete();
    await expect(
      workflows.releaseWorkflowSnapshotHandoff({ ...target, mutationFence: 'other-fence' }),
    ).resolves.toMatchObject({ status: 'conflict', record: { status: 'completed' } });
    await expect(handoffRows(target.workflowName, target.runId)).resolves.toEqual([{ status: 'completed' }]);
  });

  it('is idempotent: concurrent and repeated releases remove the row exactly once', async () => {
    const { owner, target, complete } = await claimedRun('release-twice');
    await complete();

    const results = await Promise.all([
      workflows.releaseWorkflowSnapshotHandoff(owner),
      workflows.releaseWorkflowSnapshotHandoff(owner),
    ]);
    expect(results.map(result => result.status).sort()).toEqual(['missing', 'released']);
    await expect(workflows.releaseWorkflowSnapshotHandoff(owner)).resolves.toEqual({ status: 'missing' });
    await workflows.deleteWorkflowRunById(target);
    await expect(workflows.releaseWorkflowSnapshotHandoff(owner)).resolves.toEqual({ status: 'missing' });
  });

  it('serializes a release against a concurrent transition by the fence holder', async () => {
    const outcomes = new Set<string>();
    for (let attempt = 0; attempt < 8; attempt++) {
      const { owner, target, pending } = await claimedRun('release-transition-race');
      const [transition, release] = await Promise.all([
        workflows.transitionWorkflowSnapshotHandoff({
          ...owner,
          expectedResourceId: owner.resourceId,
          expectedSnapshot: pending,
          snapshot: snapshot(owner.runId, 'waiting', { phase: 'transitioned' }),
        }),
        workflows.releaseWorkflowSnapshotHandoff(owner),
      ]);
      outcomes.add(transition.status);

      expect(release.status).toBe('released');
      if (transition.status === 'transitioned') {
        // The release observed the transition's committed snapshot, not a lost update.
        expect(release).toMatchObject({ record: { snapshot: { value: { phase: 'transitioned' } } } });
      } else {
        expect(transition).toEqual({ status: 'missing' });
        expect(release).toMatchObject({ record: { snapshot: { value: { phase: 'claimed' } } } });
      }
      await expect(handoffRows(target.workflowName, target.runId)).resolves.toEqual([]);
      await workflows.deleteWorkflowRunById(target);
    }
    expect([...outcomes].every(status => status === 'transitioned' || status === 'missing')).toBe(true);
  });

  it('never releases an in-flight handoff when a fenceless release races its completion', async () => {
    for (let attempt = 0; attempt < 8; attempt++) {
      const { target, complete } = await claimedRun('release-complete-race');
      const [completed, release] = await Promise.all([complete(), workflows.releaseWorkflowSnapshotHandoff(target)]);
      expect(completed.status).toBe('completed');

      if (release.status === 'released') {
        expect(release.record.status).toBe('completed');
        await expect(handoffRows(target.workflowName, target.runId)).resolves.toEqual([]);
      } else {
        expect(release).toMatchObject({ status: 'conflict', record: { status: 'pending' } });
        await expect(handoffRows(target.workflowName, target.runId)).resolves.toEqual([{ status: 'completed' }]);
        await expect(workflows.releaseWorkflowSnapshotHandoff(target)).resolves.toMatchObject({ status: 'released' });
      }
      await workflows.deleteWorkflowRunById(target);
    }
  });

  it('lets a new claim fence the run again after release', async () => {
    const { owner, target, complete } = await claimedRun('release-reclaim');
    await complete();
    await workflows.releaseWorkflowSnapshotHandoff(target);
    const canonical = await workflows.loadWorkflowSnapshot(target);
    await expect(
      workflows.claimWorkflowSnapshotHandoff({
        ...target,
        expectedCanonical: { kind: 'present', snapshot: canonical! },
        snapshot: snapshot(owner.runId, 'waiting', { phase: 'reclaimed' }),
        mutationFence: 'second-owner',
      }),
    ).resolves.toMatchObject({ status: 'created' });
    await expect(workflows.deleteWorkflowRunById(target)).rejects.toBeInstanceOf(WorkflowSnapshotHandoffFenceError);
    await expect(
      workflows.releaseWorkflowSnapshotHandoff({ ...target, mutationFence: owner.mutationFence }),
    ).resolves.toMatchObject({ status: 'conflict', record: { mutationFence: 'second-owner' } });
  });
});
