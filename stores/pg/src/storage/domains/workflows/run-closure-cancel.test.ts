import { randomUUID } from 'node:crypto';
import { createEmptyWorkflowSnapshot, createWorkflowTerminalGraphFingerprint } from '@mastra/core/storage';
import type { WorkflowRunState } from '@mastra/core/workflows';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { PostgresStore, WorkflowsPG } from '../../index';

const connectionString = process.env.DB_URL || 'postgresql://postgres:postgres@localhost:5434/mastra';

type Run = { workflowName: string; runId: string };

const NESTED_STEP = 'nested';
const NESTED_GRAPH = [
  { type: 'step', step: { id: NESTED_STEP, component: 'WORKFLOW' } },
] as unknown as WorkflowRunState['serializedStepGraph'];

describe('WorkflowsPG erasure cancel', () => {
  let store: PostgresStore;
  let workflows: WorkflowsPG;
  const resource = `workspace-${randomUUID()}:user-1`;

  beforeAll(async () => {
    store = new PostgresStore({ id: `workflow-erasure-cancel-${randomUUID()}`, connectionString });
    await store.init();
    workflows = (await store.getStore('workflows'))!;
  }, 60000);

  afterAll(async () => {
    await store?.close();
  });

  const newRun = (): Run => ({ workflowName: `research-${randomUUID()}`, runId: randomUUID() });

  const persist = (run: Run, status: WorkflowRunState['status']) =>
    workflows.persistWorkflowSnapshot({
      ...run,
      resourceId: resource,
      snapshot: {
        ...createEmptyWorkflowSnapshot(run.runId),
        serializedStepGraph: NESTED_GRAPH,
        status,
        result: { draft: `${run.runId} draft` },
      },
    });

  const cancel = (run: Run, expectedResourceId = resource) =>
    workflows.cancelWorkflowRunForErasure({ ...run, expectedResourceId });

  const parentRevision = (run: Run) =>
    store.db.one<{ generation: string; terminal_status: string | null }>(
      `SELECT generation::text, terminal_status FROM mastra_workflow_parent_revisions
       WHERE workflow_name = $1 AND run_id = $2`,
      [run.workflowName, run.runId],
    );

  /** A running child whose recovery ancestry names `parent` and whose terminal journal is claimed. */
  async function pendingChild(parent: Run): Promise<Run> {
    const child = newRun();
    await persist(child, 'running');
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

  it('advertises the erasure cancel', () => {
    expect(workflows.getWorkflowSnapshotHandoffCapabilities()).toMatchObject({ closureCancelVersion: 1 });
  });

  it('overwrites an unsettled run with an empty canceled snapshot, latching it, so the closure delete erases it', async () => {
    const run = newRun();
    await persist(run, 'suspended');
    const before = await parentRevision(run);

    await expect(cancel(run)).resolves.toEqual({ status: 'cancelled' });
    const stored = await workflows.loadWorkflowSnapshot(run);
    expect(stored).toMatchObject({ status: 'canceled', runId: run.runId });
    expect(stored?.result).toBeUndefined();
    const row = await store.db.one<{ resource_id: string }>(
      `SELECT "resourceId" AS resource_id FROM mastra_workflow_snapshot WHERE workflow_name = $1 AND run_id = $2`,
      [run.workflowName, run.runId],
    );
    expect(row.resource_id).toBe(resource);
    const after = await parentRevision(run);
    expect(Number(after.generation)).toBe(Number(before.generation) + 1);
    expect(after.terminal_status).toBe('canceled');

    await expect(cancel(run)).resolves.toEqual({ status: 'settled' });
    await expect(workflows.deleteWorkflowRunClosure({ ...run, expectedResourceId: resource })).resolves.toMatchObject({
      status: 'deleted',
    });
  });

  it('refuses a parent a nested child still needs, and the child whose journal is claimed, without changes', async () => {
    const parent = newRun();
    await persist(parent, 'running');
    const child = await pendingChild(parent);
    const before = await parentRevision(parent);

    await expect(cancel(parent)).resolves.toEqual({ status: 'active' });
    await expect(cancel(child)).resolves.toEqual({ status: 'journaled' });
    await expect(workflows.loadWorkflowSnapshot(parent)).resolves.toMatchObject({
      status: 'running',
      result: { draft: `${parent.runId} draft` },
    });
    await expect(workflows.loadWorkflowSnapshot(child)).resolves.toMatchObject({ status: 'running' });
    await expect(parentRevision(parent)).resolves.toEqual(before);
  });

  it('answers missing, resource_mismatch, fenced and settled without changes', async () => {
    await expect(cancel(newRun())).resolves.toEqual({ status: 'missing' });

    const running = newRun();
    await persist(running, 'running');
    await expect(cancel(running, `workspace-${randomUUID()}:user-1`)).resolves.toEqual({
      status: 'resource_mismatch',
    });

    const delivered = newRun();
    await persist(delivered, 'success');
    const canonical = (await workflows.loadWorkflowSnapshot(delivered))!;
    await expect(
      workflows.claimWorkflowSnapshotHandoff({
        ...delivered,
        resourceId: resource,
        mutationFence: `fence-${randomUUID()}`,
        expectedCanonical: { kind: 'present', resourceId: resource, snapshot: canonical },
        snapshot: { ...canonical, value: { phase: 'claimed' } } as WorkflowRunState,
      }),
    ).resolves.toMatchObject({ status: 'created' });
    await expect(cancel(delivered)).resolves.toEqual({ status: 'fenced' });

    const settled = newRun();
    await persist(settled, 'failed');
    await expect(cancel(settled)).resolves.toEqual({ status: 'settled' });

    await expect(workflows.loadWorkflowSnapshot(running)).resolves.toMatchObject({ status: 'running' });
    await expect(workflows.loadWorkflowSnapshot(settled)).resolves.toMatchObject({
      status: 'failed',
      result: { draft: `${settled.runId} draft` },
    });
  });
});
