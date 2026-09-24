import { describe, expect, it } from 'vitest';
import type { WorkflowRunState } from '../../../workflows';
import { InMemoryStore } from '../../mock';
import { STALE_EXECUTION_RESULT } from '../../types';
import { WorkflowStaleSnapshotPersistError } from '../../workflow-snapshot-handoff';

// PF-4385 tombstone reopen: a delayed result write from a deleted execution
// lifetime must not merge into the snapshot a reopened lifetime installed
// under the same runId. updateWorkflowResults fences the caller-supplied
// executionGeneration against the stored snapshot before any merge; a write
// that carries no generation keeps the legacy unguarded behavior. The fence
// resolves to the STALE_EXECUTION_RESULT sentinel — distinct from the `{}`
// missing-record fallback — so the evented processor can stop a stale
// handler instead of advancing with an inline result.
const makeSnapshot = (runId: string, executionGeneration?: string): WorkflowRunState =>
  ({
    runId,
    status: 'running',
    value: {},
    context: {},
    activePaths: [],
    activeStepsPath: {},
    suspendedPaths: {},
    resumeLabels: {},
    serializedStepGraph: [],
    waitingPaths: {},
    timestamp: Date.now(),
    ...(executionGeneration === undefined ? {} : { executionGeneration }),
  }) as WorkflowRunState;

const result = (output: unknown) =>
  ({
    status: 'success',
    output,
    payload: {},
    startedAt: 1,
    endedAt: 2,
  }) as any;

describe('WorkflowsInMemory updateWorkflowResults executionGeneration fence', () => {
  it('merges a result write carrying the snapshot execution generation', async () => {
    const store = new InMemoryStore();
    const workflows = (await store.getStore('workflows'))!;
    const workflowName = 'wf';
    const runId = 'run-matching';
    await workflows.persistWorkflowSnapshot({
      workflowName,
      runId,
      snapshot: makeSnapshot(runId, 'wfeg:lifetime-b'),
    });

    await expect(
      workflows.updateWorkflowResults({
        workflowName,
        runId,
        stepId: 'step-1',
        result: result({ data: 'fresh' }),
        requestContext: {},
        executionGeneration: 'wfeg:lifetime-b',
      }),
    ).resolves.toEqual({
      'step-1': expect.objectContaining({ status: 'success', output: { data: 'fresh' } }),
    });
  });

  it('fences a stale-lifetime write without merging into the reopened snapshot', async () => {
    const store = new InMemoryStore();
    const workflows = (await store.getStore('workflows'))!;
    const workflowName = 'wf';
    const runId = 'run-reopened';
    // Lifetime B reopened the runId after lifetime A was deleted; a delayed
    // result write still carrying A's generation must no-op.
    await workflows.persistWorkflowSnapshot({
      workflowName,
      runId,
      snapshot: makeSnapshot(runId, 'wfeg:lifetime-b'),
    });

    await expect(
      workflows.updateWorkflowResults({
        workflowName,
        runId,
        stepId: 'step-1',
        result: result({ data: 'stale' }),
        requestContext: {},
        executionGeneration: 'wfeg:lifetime-a',
      }),
    ).resolves.toBe(STALE_EXECUTION_RESULT);

    await expect(workflows.loadWorkflowSnapshot({ workflowName, runId })).resolves.toMatchObject({
      executionGeneration: 'wfeg:lifetime-b',
      context: {},
    });
  });

  it('fences a generation-carrying write against a snapshot with no stored lineage', async () => {
    const store = new InMemoryStore();
    const workflows = (await store.getStore('workflows'))!;
    const workflowName = 'wf';
    const runId = 'run-unversioned';
    await workflows.persistWorkflowSnapshot({ workflowName, runId, snapshot: makeSnapshot(runId) });

    await expect(
      workflows.updateWorkflowResults({
        workflowName,
        runId,
        stepId: 'step-1',
        result: result({ data: 'stale' }),
        requestContext: {},
        executionGeneration: 'wfeg:lifetime-a',
      }),
    ).resolves.toBe(STALE_EXECUTION_RESULT);
    await expect(workflows.loadWorkflowSnapshot({ workflowName, runId })).resolves.toMatchObject({
      context: {},
    });
  });

  it('keeps the {} missing-record fallback when no run exists', async () => {
    const store = new InMemoryStore();
    const workflows = (await store.getStore('workflows'))!;
    // A missing run record is not a stale-lifetime rejection — it is the
    // normal fallback for opted-out persistence and stays `{}` even when the
    // write carries a generation.
    await expect(
      workflows.updateWorkflowResults({
        workflowName: 'wf',
        runId: 'run-missing',
        stepId: 'step-1',
        result: result({ data: 'stale' }),
        requestContext: {},
        executionGeneration: 'wfeg:lifetime-a',
      }),
    ).resolves.toEqual({});
    await expect(workflows.loadWorkflowSnapshot({ workflowName: 'wf', runId: 'run-missing' })).resolves.toBeNull();
  });

  it('keeps unguarded merges for writes that carry no execution generation', async () => {
    const store = new InMemoryStore();
    const workflows = (await store.getStore('workflows'))!;
    const workflowName = 'wf';
    for (const [runId, generation] of [
      ['run-legacy-generated', 'wfeg:lifetime-b'],
      ['run-legacy-plain', undefined],
    ] as const) {
      await workflows.persistWorkflowSnapshot({
        workflowName,
        runId,
        snapshot: makeSnapshot(runId, generation),
      });
      await expect(
        workflows.updateWorkflowResults({
          workflowName,
          runId,
          stepId: 'step-1',
          result: result({ data: 'legacy' }),
          requestContext: {},
        }),
      ).resolves.toEqual({
        'step-1': expect.objectContaining({ status: 'success', output: { data: 'legacy' } }),
      });
    }
  });
});

// Generation-guarded persistWorkflowSnapshot (PF-4387): callers that merge
// and re-persist a full snapshot (e.g. the pruneSnapshot re-persist) CAS the
// write against the generation they fenced on, so a delete/reopen between
// the read and the write cannot be overwritten by the stale lifetime's row.
describe('WorkflowsInMemory persistWorkflowSnapshot expectedExecutionGeneration fence', () => {
  it('persists when the stored snapshot carries the expected generation', async () => {
    const store = new InMemoryStore();
    const workflows = (await store.getStore('workflows'))!;
    const workflowName = 'wf';
    const runId = 'run-guarded';
    await workflows.persistWorkflowSnapshot({
      workflowName,
      runId,
      snapshot: makeSnapshot(runId, 'wfeg:lifetime-a'),
    });

    await expect(
      workflows.persistWorkflowSnapshot({
        workflowName,
        runId,
        snapshot: { ...makeSnapshot(runId, 'wfeg:lifetime-a'), status: 'suspended' },
        expectedExecutionGeneration: 'wfeg:lifetime-a',
      }),
    ).resolves.toBeUndefined();
    await expect(workflows.loadWorkflowSnapshot({ workflowName, runId })).resolves.toMatchObject({
      status: 'suspended',
    });
  });

  it('rejects the persist against a reopened generation and keeps the live row', async () => {
    const store = new InMemoryStore();
    const workflows = (await store.getStore('workflows'))!;
    const workflowName = 'wf';
    const runId = 'run-reopened';
    // Lifetime B owns the row; a delayed re-persist still carrying A's
    // generation must not overwrite it.
    await workflows.persistWorkflowSnapshot({
      workflowName,
      runId,
      snapshot: makeSnapshot(runId, 'wfeg:lifetime-b'),
    });

    await expect(
      workflows.persistWorkflowSnapshot({
        workflowName,
        runId,
        snapshot: makeSnapshot(runId, 'wfeg:lifetime-a'),
        expectedExecutionGeneration: 'wfeg:lifetime-a',
      }),
    ).rejects.toBeInstanceOf(WorkflowStaleSnapshotPersistError);
    await expect(workflows.loadWorkflowSnapshot({ workflowName, runId })).resolves.toMatchObject({
      executionGeneration: 'wfeg:lifetime-b',
    });
  });

  it('rejects the persist when the run row was deleted instead of resurrecting it', async () => {
    const store = new InMemoryStore();
    const workflows = (await store.getStore('workflows'))!;
    const workflowName = 'wf';
    const runId = 'run-deleted';
    await workflows.persistWorkflowSnapshot({
      workflowName,
      runId,
      snapshot: makeSnapshot(runId, 'wfeg:lifetime-a'),
    });
    await workflows.deleteWorkflowRunById({ workflowName, runId });

    await expect(
      workflows.persistWorkflowSnapshot({
        workflowName,
        runId,
        snapshot: makeSnapshot(runId, 'wfeg:lifetime-a'),
        expectedExecutionGeneration: 'wfeg:lifetime-a',
      }),
    ).rejects.toBeInstanceOf(WorkflowStaleSnapshotPersistError);
    await expect(workflows.loadWorkflowSnapshot({ workflowName, runId })).resolves.toBeNull();
  });

  it('keeps unguarded persists for callers that pass no generation', async () => {
    const store = new InMemoryStore();
    const workflows = (await store.getStore('workflows'))!;
    const workflowName = 'wf';
    const runId = 'run-unguarded';
    await workflows.persistWorkflowSnapshot({
      workflowName,
      runId,
      snapshot: makeSnapshot(runId, 'wfeg:lifetime-b'),
    });

    await expect(
      workflows.persistWorkflowSnapshot({
        workflowName,
        runId,
        snapshot: makeSnapshot(runId, 'wfeg:lifetime-a'),
      }),
    ).resolves.toBeUndefined();
    await expect(workflows.loadWorkflowSnapshot({ workflowName, runId })).resolves.toMatchObject({
      executionGeneration: 'wfeg:lifetime-a',
    });
  });
});
