import { describe, expect, it } from 'vitest';
import type { WorkflowRunState } from '../../../workflows';
import { InMemoryStore } from '../../mock';

const makeSnapshot = (runId: string, status: WorkflowRunState['status']): WorkflowRunState =>
  ({
    runId,
    status,
    value: {},
    context: {},
    activePaths: [],
    activeStepsPath: {},
    suspendedPaths: {},
    resumeLabels: {},
    serializedStepGraph: [],
    waitingPaths: {},
    timestamp: Date.now(),
  }) as WorkflowRunState;

describe('WorkflowsInMemory getWorkflowRunById', () => {
  it('finds the newest matching run when workflowName is omitted', async () => {
    const store = new InMemoryStore();
    const workflows = (await store.getStore('workflows'))!;
    const runId = 'shared-run-id';

    await workflows.persistWorkflowSnapshot({
      workflowName: 'older-workflow',
      runId,
      snapshot: makeSnapshot(runId, 'running'),
      createdAt: new Date('2026-01-01T00:00:00.000Z'),
    });
    await workflows.persistWorkflowSnapshot({
      workflowName: 'newer-workflow',
      runId,
      snapshot: makeSnapshot(runId, 'success'),
      createdAt: new Date('2026-01-02T00:00:00.000Z'),
    });

    await expect(workflows.getWorkflowRunById({ runId })).resolves.toMatchObject({
      runId,
      workflowName: 'newer-workflow',
      snapshot: { status: 'success' },
    });
  });

  it('filters by an explicitly provided workflowName', async () => {
    const store = new InMemoryStore();
    const workflows = (await store.getStore('workflows'))!;
    await workflows.persistWorkflowSnapshot({
      workflowName: 'workflow-a',
      runId: 'run-1',
      snapshot: makeSnapshot('run-1', 'running'),
    });

    await expect(workflows.getWorkflowRunById({ runId: 'run-1', workflowName: 'workflow-a' })).resolves.toMatchObject({
      workflowName: 'workflow-a',
    });
    await expect(workflows.getWorkflowRunById({ runId: 'run-1', workflowName: 'workflow-other' })).resolves.toBeNull();
    await expect(workflows.getWorkflowRunById({ runId: 'run-1', workflowName: '' })).resolves.toMatchObject({
      workflowName: 'workflow-a',
    });
  });

  it('returns null for an unknown runId', async () => {
    const store = new InMemoryStore();
    const workflows = (await store.getStore('workflows'))!;

    await expect(workflows.getWorkflowRunById({ runId: 'missing' })).resolves.toBeNull();
  });
});
