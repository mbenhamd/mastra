import { describe, expect, it } from 'vitest';
import { z } from 'zod/v4';
import { Mastra } from '../mastra';
import { InMemoryStore } from '../storage/mock';
import { WorkflowCancelRequestedError } from './cancel-request';
import { createWorkflow } from './create';
import { createStep } from './workflow';

const WORKFLOW_ID = 'cancel-request-wf';

function deferred<T = void>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(settle => {
    resolve = settle;
  });
  return { promise, resolve };
}

/** Observations shared by every process-local copy of the workflow. */
function createProbe() {
  return {
    stageOneStarted: deferred(),
    releaseStageOne: deferred(),
    stageOneExecutions: 0,
    stageOneAbortReason: undefined as unknown,
    stageTwoExecutions: 0,
  };
}
type Probe = ReturnType<typeof createProbe>;

/**
 * Two stages. Stage one runs until released or aborted, like a long model
 * call that honours its abort signal; stage two counts executions, so a test
 * can tell whether the run went past the boundary after stage one.
 */
function createStagedWorkflow(probe: Probe) {
  const stageOne = createStep({
    id: 'stage-one',
    inputSchema: z.object({ topic: z.string() }),
    outputSchema: z.object({ topic: z.string() }),
    execute: async ({ inputData, abortSignal }) => {
      probe.stageOneExecutions++;
      probe.stageOneStarted.resolve();
      await Promise.race([
        probe.releaseStageOne.promise,
        new Promise(settle => abortSignal.addEventListener('abort', settle, { once: true })),
      ]);
      if (abortSignal.aborted) probe.stageOneAbortReason = abortSignal.reason;
      return inputData;
    },
  });
  const stageTwo = createStep({
    id: 'stage-two',
    inputSchema: z.object({ topic: z.string() }),
    outputSchema: z.object({ topic: z.string() }),
    execute: async ({ inputData }) => {
      probe.stageTwoExecutions++;
      return inputData;
    },
  });
  return createWorkflow({
    id: WORKFLOW_ID,
    inputSchema: z.object({ topic: z.string() }),
    outputSchema: z.object({ topic: z.string() }),
    steps: [stageOne, stageTwo],
    options: { validateInputs: false },
  })
    .then(stageOne)
    .then(stageTwo)
    .commit();
}

/** One "process": its own Mastra instance and workflow copy over shared storage. */
function createProcess(storage: InMemoryStore, probe: Probe) {
  const workflow = createStagedWorkflow(probe);
  const mastra = new Mastra({ storage, workflows: { [WORKFLOW_ID]: workflow }, logger: false });
  return { mastra, workflow };
}

async function loadSnapshot(storage: InMemoryStore, runId: string) {
  const workflowsStore = await storage.getStore('workflows');
  return workflowsStore!.loadWorkflowSnapshot({ workflowName: WORKFLOW_ID, runId });
}

async function lineageOf(storage: InMemoryStore, runId: string) {
  const snapshot = await loadSnapshot(storage, runId);
  return {
    expectedExecutionGeneration: snapshot!.executionGeneration!,
    expectedLifecycleResumeAttempt: snapshot!.lifecycleResumeAttempt ?? 0,
  };
}

describe('Run.requestCancel()', () => {
  it('stops a run owned by another handle at its next step boundary', async () => {
    const storage = new InMemoryStore();
    const probe = createProbe();
    const owner = createProcess(storage, probe);
    const controller = createProcess(storage, probe);

    const ownerRun = await owner.workflow.createRun();
    const execution = ownerRun.start({ inputData: { topic: 'aspirin' } });
    await probe.stageOneStarted.promise;

    const remoteRun = await controller.workflow.createRun({ runId: ownerRun.runId });
    const outcome = await remoteRun.requestCancel({
      requestId: 'abort-op-1',
      ...(await lineageOf(storage, ownerRun.runId)),
    });
    expect(outcome).toMatchObject({ status: 'requested', cancelRequest: { requestId: 'abort-op-1' } });
    // The request does not settle the run: its owner is still executing.
    expect((await loadSnapshot(storage, ownerRun.runId))?.status).toBe('running');

    probe.releaseStageOne.resolve();
    const result = await execution;

    expect(result.status).toBe('canceled');
    expect(probe.stageTwoExecutions).toBe(0);
    const settled = await loadSnapshot(storage, ownerRun.runId);
    expect(settled?.status).toBe('canceled');
    expect(settled?.cancelRequest?.requestId).toBe('abort-op-1');
  });

  it('aborts the execution in progress when the requesting handle owns it', async () => {
    const storage = new InMemoryStore();
    const probe = createProbe();
    const owner = createProcess(storage, probe);

    const run = await owner.workflow.createRun();
    const execution = run.start({ inputData: { topic: 'aspirin' } });
    await probe.stageOneStarted.promise;

    const outcome = await run.requestCancel({ requestId: 'abort-op-1' });
    const result = await execution;

    expect(outcome.status).toBe('requested');
    expect(probe.stageOneAbortReason).toBeInstanceOf(WorkflowCancelRequestedError);
    expect(result.status).toBe('canceled');
    expect(probe.stageTwoExecutions).toBe(0);
    expect((await loadSnapshot(storage, run.runId))?.status).toBe('canceled');
  });

  it('never lands on a successor resume attempt', async () => {
    const storage = new InMemoryStore();
    const approval = createStep({
      id: 'approval',
      inputSchema: z.object({ topic: z.string() }),
      outputSchema: z.object({ topic: z.string() }),
      resumeSchema: z.object({ approved: z.boolean() }),
      execute: async ({ inputData, resumeData, suspend }) => {
        if (!resumeData) return suspend({});
        return inputData;
      },
    });
    const workflow = createWorkflow({
      id: WORKFLOW_ID,
      inputSchema: z.object({ topic: z.string() }),
      outputSchema: z.object({ topic: z.string() }),
      steps: [approval],
      options: { validateInputs: false },
    })
      .then(approval)
      .commit();
    new Mastra({ storage, workflows: { [WORKFLOW_ID]: workflow }, logger: false });

    const run = await workflow.createRun();
    expect((await run.start({ inputData: { topic: 'aspirin' } })).status).toBe('suspended');
    const suspendedLineage = await lineageOf(storage, run.runId);
    expect((await run.resume({ resumeData: { approved: true } })).status).toBe('success');

    // A delayed request for the suspended attempt must not touch the successor.
    const outcome = await run.requestCancel({ requestId: 'stale-abort', ...suspendedLineage });

    expect(outcome).toEqual({
      status: 'lineage_moved',
      executionGeneration: suspendedLineage.expectedExecutionGeneration,
      lifecycleResumeAttempt: suspendedLineage.expectedLifecycleResumeAttempt + 1,
    });
    const settled = await loadSnapshot(storage, run.runId);
    expect(settled?.status).toBe('success');
    expect(settled?.cancelRequest).toBeUndefined();
  });

  it('cancels a suspended lineage immediately, since no engine is executing it', async () => {
    const storage = new InMemoryStore();
    const approval = createStep({
      id: 'approval',
      inputSchema: z.object({ topic: z.string() }),
      outputSchema: z.object({ topic: z.string() }),
      resumeSchema: z.object({ approved: z.boolean() }),
      execute: async ({ inputData, resumeData, suspend }) => {
        if (!resumeData) return suspend({});
        return inputData;
      },
    });
    const workflow = createWorkflow({
      id: WORKFLOW_ID,
      inputSchema: z.object({ topic: z.string() }),
      outputSchema: z.object({ topic: z.string() }),
      steps: [approval],
      options: { validateInputs: false },
    })
      .then(approval)
      .commit();
    new Mastra({ storage, workflows: { [WORKFLOW_ID]: workflow }, logger: false });

    const run = await workflow.createRun();
    expect((await run.start({ inputData: { topic: 'aspirin' } })).status).toBe('suspended');

    const outcome = await run.requestCancel({ requestId: 'abort-op-1', ...(await lineageOf(storage, run.runId)) });

    expect(outcome).toEqual({ status: 'canceled' });
    expect((await loadSnapshot(storage, run.runId))?.status).toBe('canceled');
    await expect(run.requestCancel({ requestId: 'abort-op-2' })).resolves.toEqual({
      status: 'terminal',
      runStatus: 'canceled',
    });
  });

  it('makes a recovery restart commit canceled instead of re-executing the stranded lineage', async () => {
    const storage = new InMemoryStore();
    const probe = createProbe();
    const deadOwner = createProcess(storage, probe);
    const survivor = createProcess(storage, probe);

    // The owner is mid-stage when the request lands and never reaches another
    // boundary in this test: its process is treated as gone.
    const ownerRun = await deadOwner.workflow.createRun();
    const abandoned = ownerRun.start({ inputData: { topic: 'aspirin' } });
    await probe.stageOneStarted.promise;
    const remoteRun = await survivor.workflow.createRun({ runId: ownerRun.runId });
    expect(
      (await remoteRun.requestCancel({ requestId: 'abort-op-1', ...(await lineageOf(storage, ownerRun.runId)) }))
        .status,
    ).toBe('requested');

    await survivor.workflow.restartAllActiveWorkflowRuns();

    expect(probe.stageOneExecutions).toBe(1);
    expect(probe.stageTwoExecutions).toBe(0);
    expect((await loadSnapshot(storage, ownerRun.runId))?.status).toBe('canceled');

    // A late wake-up of the old owner finds the lineage settled.
    probe.releaseStageOne.resolve();
    expect((await abandoned).status).toBe('canceled');
    expect(probe.stageTwoExecutions).toBe(0);
  });

  it('makes resume commit canceled when the request lost the race with suspension', async () => {
    const storage = new InMemoryStore();
    let approvalExecutions = 0;
    const approval = createStep({
      id: 'approval',
      inputSchema: z.object({ topic: z.string() }),
      outputSchema: z.object({ topic: z.string() }),
      resumeSchema: z.object({ approved: z.boolean() }),
      execute: async ({ inputData, resumeData, suspend }) => {
        approvalExecutions++;
        if (!resumeData) return suspend({});
        return inputData;
      },
    });
    const workflow = createWorkflow({
      id: WORKFLOW_ID,
      inputSchema: z.object({ topic: z.string() }),
      outputSchema: z.object({ topic: z.string() }),
      steps: [approval],
      options: { validateInputs: false },
    })
      .then(approval)
      .commit();
    new Mastra({ storage, workflows: { [WORKFLOW_ID]: workflow }, logger: false });

    const run = await workflow.createRun();
    expect((await run.start({ inputData: { topic: 'aspirin' } })).status).toBe('suspended');
    // The request was recorded while the lineage ran, after its last boundary.
    const suspended = await loadSnapshot(storage, run.runId);
    const workflowsStore = await storage.getStore('workflows');
    await workflowsStore!.updateWorkflowState({
      workflowName: WORKFLOW_ID,
      runId: run.runId,
      opts: {
        cancelRequest: {
          version: 1,
          requestId: 'abort-op-1',
          executionGeneration: suspended!.executionGeneration!,
          lifecycleResumeAttempt: suspended!.lifecycleResumeAttempt ?? 0,
          requestedAt: Date.now(),
        },
      },
    });

    const result = await run.resume({ resumeData: { approved: true } });

    expect(result.status).toBe('canceled');
    expect(approvalExecutions).toBe(1);
    expect((await loadSnapshot(storage, run.runId))?.status).toBe('canceled');
  });
});
