import { describe, expect, it, vi } from 'vitest';
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

/**
 * A chunked stage: a `dountil` loop whose body runs once per chunk. Only the
 * first chunk waits for release, so later chunks would run straight through.
 */
function createChunkedWorkflow(probe: Probe) {
  const chunk = createStep({
    id: 'chunk',
    inputSchema: z.object({ chunks: z.number() }),
    outputSchema: z.object({ chunks: z.number() }),
    execute: async ({ inputData }) => {
      probe.stageOneExecutions++;
      if (inputData.chunks === 0) {
        probe.stageOneStarted.resolve();
        await probe.releaseStageOne.promise;
      }
      return { chunks: inputData.chunks + 1 };
    },
  });
  const report = createStep({
    id: 'report',
    inputSchema: z.object({ chunks: z.number() }),
    outputSchema: z.object({ chunks: z.number() }),
    execute: async ({ inputData }) => {
      probe.stageTwoExecutions++;
      return inputData;
    },
  });
  return createWorkflow({
    id: WORKFLOW_ID,
    inputSchema: z.object({ chunks: z.number() }),
    outputSchema: z.object({ chunks: z.number() }),
    steps: [chunk, report],
    options: { validateInputs: false },
  })
    .dountil(chunk, async ({ inputData }) => inputData.chunks >= 5)
    .then(report)
    .commit();
}

/**
 * A review step that suspends for approval, and on resume works until
 * released, then suspends again for the next round.
 */
function createReviewWorkflow(probe: Probe) {
  const review = createStep({
    id: 'review',
    inputSchema: z.object({ topic: z.string() }),
    outputSchema: z.object({ topic: z.string() }),
    resumeSchema: z.object({ round: z.number() }),
    suspendSchema: z.object({ round: z.number() }),
    execute: async ({ inputData, resumeData, suspend }) => {
      probe.stageOneExecutions++;
      if (!resumeData) return suspend({ round: 1 });
      probe.stageOneStarted.resolve();
      await probe.releaseStageOne.promise;
      await suspend({ round: resumeData.round + 1 });
      return inputData;
    },
  });
  return createWorkflow({
    id: WORKFLOW_ID,
    inputSchema: z.object({ topic: z.string() }),
    outputSchema: z.object({ topic: z.string() }),
    steps: [review],
    options: { validateInputs: false },
  })
    .then(review)
    .commit();
}

function registerProcess<TWorkflow extends { id: string }>(storage: InMemoryStore, workflow: TWorkflow) {
  new Mastra({ storage, workflows: { [WORKFLOW_ID]: workflow as never }, logger: false });
  return workflow;
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

    // The public run state names the execution to target.
    const observed = await controller.workflow.getWorkflowRunById(ownerRun.runId, { fields: [] });
    const remoteRun = await controller.workflow.createRun({ runId: ownerRun.runId });
    const outcome = await remoteRun.requestCancel({
      requestId: 'abort-op-1',
      expectedExecutionGeneration: observed!.executionGeneration!,
      expectedLifecycleResumeAttempt: observed!.lifecycleResumeAttempt!,
    });
    expect(outcome).toMatchObject({ status: 'requested', cancelRequest: { requestId: 'abort-op-1' } });
    // The controller-only handle never executes, so it is released at once.
    expect(await controller.workflow.createRun({ runId: ownerRun.runId })).not.toBe(remoteRun);
    // The request does not settle the run: its owner is still executing.
    expect(await controller.workflow.getWorkflowRunById(ownerRun.runId, { fields: [] })).toMatchObject({
      status: 'running',
      cancelRequest: { requestId: 'abort-op-1' },
    });

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

  it('stops a chunked loop before its next iteration starts', async () => {
    const storage = new InMemoryStore();
    const probe = createProbe();
    const owner = registerProcess(storage, createChunkedWorkflow(probe));
    const controller = registerProcess(storage, createChunkedWorkflow(probe));

    const ownerRun = await owner.createRun();
    const execution = ownerRun.start({ inputData: { chunks: 0 } });
    await probe.stageOneStarted.promise;
    const remoteRun = await controller.createRun({ runId: ownerRun.runId });
    expect(
      (await remoteRun.requestCancel({ requestId: 'abort-op-1', ...(await lineageOf(storage, ownerRun.runId)) }))
        .status,
    ).toBe('requested');

    probe.releaseStageOne.resolve();
    const result = await execution;

    // The loop is one entry; without a check at each iteration it would run
    // all five chunks before the entry boundary saw the request.
    expect(result.status).toBe('canceled');
    expect(probe.stageOneExecutions).toBe(1);
    expect(probe.stageTwoExecutions).toBe(0);
    expect((await loadSnapshot(storage, ownerRun.runId))?.status).toBe('canceled');
  });

  it('cancels a resumed attempt that suspends again after the request', async () => {
    const storage = new InMemoryStore();
    const probe = createProbe();
    const owner = registerProcess(storage, createReviewWorkflow(probe));
    const controller = registerProcess(storage, createReviewWorkflow(probe));

    const run = await owner.createRun();
    expect((await run.start({ inputData: { topic: 'aspirin' } })).status).toBe('suspended');
    const resumed = run.resume({ resumeData: { round: 1 } });
    await probe.stageOneStarted.promise;
    const resumedLineage = await lineageOf(storage, run.runId);
    expect(resumedLineage.expectedLifecycleResumeAttempt).toBe(1);
    const remoteRun = await controller.createRun({ runId: run.runId });
    expect((await remoteRun.requestCancel({ requestId: 'abort-op-1', ...resumedLineage })).status).toBe('requested');

    probe.releaseStageOne.resolve();
    const result = await resumed;

    expect(result.status).toBe('canceled');
    expect((await loadSnapshot(storage, run.runId))?.status).toBe('canceled');
    await expect(run.resume({ resumeData: { round: 2 } })).rejects.toThrow('This workflow run was not suspended');
    expect(probe.stageOneExecutions).toBe(2);
  });

  it('reports a lineage a restart claimed between its read and its write, and writes nothing', async () => {
    const storage = new InMemoryStore();
    const probe = createProbe();
    const deadOwner = createProcess(storage, probe);
    const survivor = createProcess(storage, probe);
    const workflowsStore = (await storage.getStore('workflows'))!;

    const ownerRun = await deadOwner.workflow.createRun();
    const abandoned = ownerRun.start({ inputData: { topic: 'aspirin' } });
    await probe.stageOneStarted.promise;
    const strandedLineage = await lineageOf(storage, ownerRun.runId);

    // The survivor's recovery restart claims the run after requestCancel()
    // read the stranded lineage and before its compare-and-set.
    let recovery: Promise<unknown> | undefined;
    const updateWorkflowState = workflowsStore.updateWorkflowState.bind(workflowsStore);
    const spy = vi.spyOn(workflowsStore, 'updateWorkflowState').mockImplementation(async args => {
      if (args.opts.cancelRequest && recovery === undefined) {
        recovery = (await survivor.workflow.createRun({ runId: ownerRun.runId })).restart();
        await vi.waitFor(async () => {
          expect((await loadSnapshot(storage, ownerRun.runId))?.executionGeneration).not.toBe(
            strandedLineage.expectedExecutionGeneration,
          );
        });
      }
      return updateWorkflowState(args);
    });

    const lateRun = await survivor.workflow.createRun({ runId: ownerRun.runId });
    const outcome = await lateRun.requestCancel({ requestId: 'stale-abort', ...strandedLineage });
    spy.mockRestore();

    const successor = await loadSnapshot(storage, ownerRun.runId);
    expect(outcome).toEqual({
      status: 'lineage_moved',
      executionGeneration: successor?.executionGeneration,
      lifecycleResumeAttempt: 0,
    });
    expect(successor?.cancelRequest).toBeUndefined();
    probe.releaseStageOne.resolve();
    expect(((await recovery) as { status: string }).status).toBe('success');
    expect((await abandoned).status).toBe('canceled');
    expect((await loadSnapshot(storage, ownerRun.runId))?.status).toBe('success');
  });

  it('keeps the first request for a lineage', async () => {
    const storage = new InMemoryStore();
    const probe = createProbe();
    const owner = createProcess(storage, probe);
    const controller = createProcess(storage, probe);

    const ownerRun = await owner.workflow.createRun();
    const execution = ownerRun.start({ inputData: { topic: 'aspirin' } });
    await probe.stageOneStarted.promise;
    const remoteRun = await controller.workflow.createRun({ runId: ownerRun.runId });
    const lineage = await lineageOf(storage, ownerRun.runId);
    await remoteRun.requestCancel({ requestId: 'abort-op-1', ...lineage });

    const repeated = await remoteRun.requestCancel({ requestId: 'abort-op-2', ...lineage });

    expect(repeated).toMatchObject({ status: 'already_requested', cancelRequest: { requestId: 'abort-op-1' } });
    probe.releaseStageOne.resolve();
    expect((await execution).status).toBe('canceled');
  });

  it('returns the cancellation when the owner honours the request while a restart is committing it', async () => {
    const storage = new InMemoryStore();
    const probe = createProbe();
    const owner = createProcess(storage, probe);
    const survivor = createProcess(storage, probe);
    const workflowsStore = (await storage.getStore('workflows'))!;

    const ownerRun = await owner.workflow.createRun();
    const execution = ownerRun.start({ inputData: { topic: 'aspirin' } });
    await probe.stageOneStarted.promise;
    const remoteRun = await survivor.workflow.createRun({ runId: ownerRun.runId });
    await remoteRun.requestCancel({ requestId: 'abort-op-1', ...(await lineageOf(storage, ownerRun.runId)) });

    // The owner wakes and commits `canceled` after the restart read the
    // requested lineage and before the restart's own compare-and-set.
    const updateWorkflowState = workflowsStore.updateWorkflowState.bind(workflowsStore);
    const spy = vi.spyOn(workflowsStore, 'updateWorkflowState').mockImplementation(async args => {
      if (args.opts.status === 'canceled') {
        probe.releaseStageOne.resolve();
        await vi.waitFor(async () => {
          expect((await loadSnapshot(storage, ownerRun.runId))?.status).toBe('canceled');
        });
      }
      return updateWorkflowState(args);
    });
    const restarted = await (await survivor.workflow.createRun({ runId: ownerRun.runId })).restart();
    spy.mockRestore();

    expect(restarted.status).toBe('canceled');
    expect((await execution).status).toBe('canceled');
    expect(probe.stageOneExecutions).toBe(1);
    expect(probe.stageTwoExecutions).toBe(0);
  });

  it('refuses storage whose step writes could drop the request, writing nothing', async () => {
    const storage = new InMemoryStore();
    const probe = createProbe();
    const owner = createProcess(storage, probe);
    const workflowsStore = (await storage.getStore('workflows'))!;
    vi.spyOn(workflowsStore, 'getWorkflowResumeCapabilities').mockReturnValue({});

    const run = await owner.workflow.createRun();
    const execution = run.start({ inputData: { topic: 'aspirin' } });
    await probe.stageOneStarted.promise;

    await expect(run.requestCancel({ requestId: 'abort-op-1' })).rejects.toThrow(
      'requestCancel() requires workflow storage with compare-and-set updates and fenced step writes',
    );
    expect((await loadSnapshot(storage, run.runId))?.cancelRequest).toBeUndefined();
    probe.releaseStageOne.resolve();
    expect((await execution).status).toBe('success');
  });

  it('stops before a sleep starts when the request lands with the waiting write', async () => {
    const storage = new InMemoryStore();
    let sleepDurationReads = 0;
    const makeWorkflow = () => {
      const prepare = createStep({
        id: 'prepare',
        inputSchema: z.object({ topic: z.string() }),
        outputSchema: z.object({ topic: z.string() }),
        execute: async ({ inputData }) => inputData,
      });
      return createWorkflow({
        id: WORKFLOW_ID,
        inputSchema: z.object({ topic: z.string() }),
        outputSchema: z.object({ topic: z.string() }),
        steps: [prepare],
        options: { validateInputs: false },
      })
        .then(prepare)
        .sleep(async () => {
          sleepDurationReads++;
          return 60_000;
        })
        .commit();
    };
    const owner = registerProcess(storage, makeWorkflow());
    const controller = registerProcess(storage, makeWorkflow());
    const workflowsStore = (await storage.getStore('workflows'))!;

    const run = await owner.createRun();
    // The request commits just before the owner persists the sleep's waiting
    // boundary, after the boundary that followed the previous step.
    const persist = workflowsStore.persistWorkflowStepUpdate.bind(workflowsStore);
    let requested: Promise<unknown> | undefined;
    vi.spyOn(workflowsStore, 'persistWorkflowStepUpdate').mockImplementation(async input => {
      if (input.snapshot.status === 'waiting' && requested === undefined) {
        requested = (async () =>
          (await controller.createRun({ runId: run.runId })).requestCancel({
            requestId: 'abort-op-1',
            ...(await lineageOf(storage, run.runId)),
          }))();
        expect(await requested).toMatchObject({ status: 'requested' });
      }
      return persist(input);
    });

    const result = await run.start({ inputData: { topic: 'aspirin' } });

    expect(result.status).toBe('canceled');
    expect(sleepDurationReads).toBe(0);
    expect((await loadSnapshot(storage, run.runId))?.status).toBe('canceled');
  });

  it('does not start a sleep on a lineage another owner settled after its waiting write', async () => {
    const storage = new InMemoryStore();
    let sleepDurationReads = 0;
    const prepare = createStep({
      id: 'prepare',
      inputSchema: z.object({ topic: z.string() }),
      outputSchema: z.object({ topic: z.string() }),
      execute: async ({ inputData }) => inputData,
    });
    const workflow = registerProcess(
      storage,
      createWorkflow({
        id: WORKFLOW_ID,
        inputSchema: z.object({ topic: z.string() }),
        outputSchema: z.object({ topic: z.string() }),
        steps: [prepare],
        options: { validateInputs: false },
      })
        .then(prepare)
        .sleep(async () => {
          sleepDurationReads++;
          return 60_000;
        })
        .commit(),
    );
    const workflowsStore = (await storage.getStore('workflows'))!;

    const run = await workflow.createRun();
    // A recovery restart elsewhere honours a request and commits `canceled`
    // right after the owner's waiting write.
    const persist = workflowsStore.persistWorkflowStepUpdate.bind(workflowsStore);
    vi.spyOn(workflowsStore, 'persistWorkflowStepUpdate').mockImplementation(async input => {
      const written = await persist(input);
      if (input.snapshot.status === 'waiting') {
        await workflowsStore.updateWorkflowState({
          workflowName: WORKFLOW_ID,
          runId: run.runId,
          opts: {
            status: 'canceled',
            expectedStatus: 'waiting',
            expectedExecutionGeneration: input.snapshot.executionGeneration!,
            expectedLifecycleResumeAttempt: input.snapshot.lifecycleResumeAttempt ?? 0,
          },
        });
      }
      return written;
    });

    const result = await run.start({ inputData: { topic: 'aspirin' } });

    expect(result.status).toBe('canceled');
    expect(sleepDurationReads).toBe(0);
  });

  it('returns the settled outcome when another process cancels a suspension while the owner unwinds', async () => {
    const storage = new InMemoryStore();
    const unwinding = deferred();
    const finishUnwinding = deferred();
    const makeWorkflow = (onFinish?: (result: { status: string }) => Promise<void>) => {
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
      return createWorkflow({
        id: WORKFLOW_ID,
        inputSchema: z.object({ topic: z.string() }),
        outputSchema: z.object({ topic: z.string() }),
        steps: [approval],
        options: { validateInputs: false, ...(onFinish ? { onFinish } : {}) },
      })
        .then(approval)
        .commit();
    };
    const owner = registerProcess(
      storage,
      makeWorkflow(async ({ status }) => {
        if (status !== 'suspended') return;
        unwinding.resolve();
        await finishUnwinding.promise;
      }),
    );
    const controller = registerProcess(storage, makeWorkflow());

    const run = await owner.createRun();
    const execution = run.start({ inputData: { topic: 'aspirin' } });
    await unwinding.promise;
    const remoteRun = await controller.createRun({ runId: run.runId });
    expect(
      await remoteRun.requestCancel({ requestId: 'abort-op-1', ...(await lineageOf(storage, run.runId)) }),
    ).toEqual({ status: 'canceled' });

    finishUnwinding.resolve();
    const result = await execution;

    expect(result.status).toBe('canceled');
    expect((await loadSnapshot(storage, run.runId))?.status).toBe('canceled');
  });

  it('stops a run its own handle is still preparing in onStart', async () => {
    const storage = new InMemoryStore();
    const probe = createProbe();
    const preparing = deferred();
    const finishPreparing = deferred();
    const stage = createStep({
      id: 'stage-one',
      inputSchema: z.object({ topic: z.string() }),
      outputSchema: z.object({ topic: z.string() }),
      execute: async ({ inputData }) => {
        probe.stageOneExecutions++;
        return inputData;
      },
    });
    const workflow = registerProcess(
      storage,
      createWorkflow({
        id: WORKFLOW_ID,
        inputSchema: z.object({ topic: z.string() }),
        outputSchema: z.object({ topic: z.string() }),
        steps: [stage],
        options: {
          validateInputs: false,
          onStart: async () => {
            preparing.resolve();
            await finishPreparing.promise;
          },
        },
      })
        .then(stage)
        .commit(),
    );

    const run = await workflow.createRun();
    const execution = run.start({ inputData: { topic: 'aspirin' } });
    await preparing.promise;
    const outcome = await run.requestCancel({ requestId: 'abort-op-1' });
    finishPreparing.resolve();
    const result = await execution;

    expect(outcome.status).toBe('requested');
    expect(result.status).toBe('canceled');
    expect(probe.stageOneExecutions).toBe(0);
    expect((await loadSnapshot(storage, run.runId))?.status).toBe('canceled');
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
