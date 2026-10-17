import { describe, expect, it } from 'vitest';
import type { WorkflowRunState } from '../../../workflows';
import { createWorkflowTerminalGraphFingerprint } from '../../../workflows/terminal-continuation';
import { createEmptyWorkflowSnapshot } from '../../workflow-snapshot';
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
});
