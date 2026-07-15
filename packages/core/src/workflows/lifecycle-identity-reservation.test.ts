import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { RequestContext } from '../di';
import { EventEmitterPubSub } from '../events/event-emitter';
import { Mastra } from '../mastra';
import { MockStore } from '../storage/mock';
import { createWorkflow } from './create';
import { ExecutionEngine } from './execution-engine';
import type { ExecutionGraph } from './execution-engine';
import type { SerializedStepFlowEntry, WorkflowRunState } from './types';
import { createStep } from './workflow';

const workflowId = 'lifecycle-identity-reservation';
const runId = 'shared-pending-run';

function makeWorkflow(executionEngine?: ExecutionEngine) {
  const step = createStep({
    id: 'only-step',
    inputSchema: z.object({}),
    outputSchema: z.object({}),
    execute: async () => ({}),
  });
  return createWorkflow({
    id: workflowId,
    inputSchema: z.object({}),
    outputSchema: z.object({}),
    steps: [step],
    executionEngine,
  })
    .then(step)
    .commit();
}

function legacyPendingSnapshot(id: string, serializedStepGraph: SerializedStepFlowEntry[]): WorkflowRunState {
  return {
    runId: id,
    status: 'pending',
    value: {},
    context: {},
    activePaths: [],
    activeStepsPath: {},
    serializedStepGraph,
    suspendedPaths: {},
    resumeLabels: {},
    waitingPaths: {},
    timestamp: Date.now(),
  };
}

class AdmissionInspectingEngine extends ExecutionEngine {
  readonly observations: Array<{
    kind: 'restart' | 'timeTravel';
    executionGeneration: string;
    snapshotGeneration?: string;
    snapshotStatus?: WorkflowRunState['status'];
  }> = [];

  constructor() {
    super({
      options: {
        validateInputs: true,
        shouldPersistSnapshot: () => true,
      },
    });
  }

  async execute<_TState, _TInput, TOutput>(params: {
    workflowId: string;
    runId: string;
    executionGeneration: string;
    restart?: unknown;
    timeTravel?: unknown;
    graph: ExecutionGraph;
  }): Promise<TOutput> {
    const workflowsStore = await this.mastra?.getStorage()?.getStore('workflows');
    const snapshot = await workflowsStore?.loadWorkflowSnapshot({
      workflowName: params.workflowId,
      runId: params.runId,
    });
    this.observations.push({
      kind: params.restart ? 'restart' : 'timeTravel',
      executionGeneration: params.executionGeneration,
      snapshotGeneration: snapshot?.executionGeneration,
      snapshotStatus: snapshot?.status,
    });
    return {
      status: 'success',
      steps: {},
      state: {},
      result: {},
    } as TOutput;
  }
}

describe('workflow lifecycle identity admission', () => {
  it('fences the losing creator when two independent handles race from an absent run id', async () => {
    const storage = new MockStore();
    const workflowsStore = await storage.getStore('workflows');
    const originalPersist = workflowsStore.persistWorkflowSnapshot.bind(workflowsStore);
    const originalLoad = workflowsStore.loadWorkflowSnapshot.bind(workflowsStore);
    const admittedGenerations = new Map<string, string>();
    let persistCalls = 0;
    let firstPersisted = false;
    let releaseSecondPersist!: () => void;
    const firstCreatorRead = new Promise<void>(resolve => {
      releaseSecondPersist = resolve;
    });

    workflowsStore.persistWorkflowSnapshot = async args => {
      const call = ++persistCalls;
      admittedGenerations.set(args.resourceId ?? 'unknown', args.snapshot.executionGeneration!);
      if (call === 2) await firstCreatorRead;
      await originalPersist(args);
      if (call === 1) firstPersisted = true;
    };
    workflowsStore.loadWorkflowSnapshot = async args => {
      const snapshot = await originalLoad(args);
      if (firstPersisted) releaseSecondPersist();
      return snapshot;
    };

    const workflowA = makeWorkflow();
    const workflowB = makeWorkflow();
    const mastraA = new Mastra({ logger: false, storage, workflows: { [workflowId]: workflowA } });
    const mastraB = new Mastra({ logger: false, storage, workflows: { [workflowId]: workflowB } });
    const [runA, runB] = await Promise.all([
      workflowA.createRun({ runId: 'absent-race-run', resourceId: 'creator-a' }),
      workflowB.createRun({ runId: 'absent-race-run', resourceId: 'creator-b' }),
    ]);
    const admitted = await originalLoad({ workflowName: workflowId, runId: 'absent-race-run' });
    const creatorAGeneration = admittedGenerations.get('creator-a');
    const creatorBGeneration = admittedGenerations.get('creator-b');
    expect(creatorAGeneration).toEqual(expect.any(String));
    expect(creatorBGeneration).toEqual(expect.any(String));
    expect(creatorAGeneration).not.toBe(creatorBGeneration);
    expect(admitted?.executionGeneration).toBe(creatorBGeneration);

    await expect(runA.start({ inputData: {} })).rejects.toThrow('lifecycle execution admission is stale');
    await expect(runB.start({ inputData: {} })).resolves.toMatchObject({ status: 'success' });
    await expect(originalLoad({ workflowName: workflowId, runId: 'absent-race-run' })).resolves.toMatchObject({
      executionGeneration: creatorBGeneration,
      status: 'success',
    });

    await Promise.all([mastraA.shutdown(), mastraB.shutdown()]);
  });

  it('reopens one persisted pending lineage across independent handles and reuses it on start', async () => {
    const storage = new MockStore();
    const originalWorkflow = makeWorkflow();
    const originalMastra = new Mastra({ logger: false, storage, workflows: { [workflowId]: originalWorkflow } });
    const originalRun = await originalWorkflow.createRun({ runId });
    const workflowsStore = await storage.getStore('workflows');
    const admitted = await workflowsStore.loadWorkflowSnapshot({ workflowName: workflowId, runId });

    expect(admitted).toMatchObject({
      status: 'pending',
      executionGeneration: expect.any(String),
      lifecycleResumeAttempt: 0,
      lifecycleStepStates: {},
    });
    await expect(originalRun.getLifecycleExecutionIdentity()).resolves.toMatchObject({
      executionGeneration: admitted!.executionGeneration,
    });

    const reopenedWorkflowA = makeWorkflow();
    const reopenedWorkflowB = makeWorkflow();
    const reopenedMastraA = new Mastra({
      logger: false,
      storage,
      workflows: { [workflowId]: reopenedWorkflowA },
    });
    const reopenedMastraB = new Mastra({
      logger: false,
      storage,
      workflows: { [workflowId]: reopenedWorkflowB },
    });
    const [runA, runB] = await Promise.all([
      reopenedWorkflowA.createRun({ runId }),
      reopenedWorkflowB.createRun({ runId }),
    ]);
    const [identityA, identityB] = await Promise.all([
      runA.getLifecycleExecutionIdentity(),
      runB.getLifecycleExecutionIdentity(),
    ]);

    expect(identityA).toEqual(identityB);
    expect(identityA.executionGeneration).toBe(admitted!.executionGeneration);

    await expect(runA.start({ inputData: {} })).resolves.toMatchObject({ status: 'success' });
    await expect(workflowsStore.loadWorkflowSnapshot({ workflowName: workflowId, runId })).resolves.toMatchObject({
      executionGeneration: admitted!.executionGeneration,
    });

    await Promise.all([originalMastra.shutdown(), reopenedMastraA.shutdown(), reopenedMastraB.shutdown()]);
  });

  it('fails closed for a persisted pending snapshot without lifecycle lineage', async () => {
    const storage = new MockStore();
    const workflow = makeWorkflow();
    const mastra = new Mastra({ logger: false, storage, workflows: { [workflowId]: workflow } });
    const workflowsStore = await storage.getStore('workflows');
    await workflowsStore.persistWorkflowSnapshot({
      workflowName: workflowId,
      runId: 'legacy-pending-run',
      snapshot: legacyPendingSnapshot('legacy-pending-run', workflow.serializedStepGraph),
    });

    await expect(workflow.createRun({ runId: 'legacy-pending-run' })).rejects.toThrow('complete lifecycle lineage');
    await expect(
      workflowsStore.loadWorkflowSnapshot({ workflowName: workflowId, runId: 'legacy-pending-run' }),
    ).resolves.not.toHaveProperty('executionGeneration');

    await mastra.shutdown();
  });

  it('exposes time-travel admission as durably active to a second run handle', async () => {
    let markEntered!: () => void;
    let releaseStep!: () => void;
    const entered = new Promise<void>(resolve => {
      markEntered = resolve;
    });
    const step = createStep({
      id: 'only-step',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      execute: async () => {
        markEntered();
        await new Promise<void>(resolve => {
          releaseStep = resolve;
        });
        return {};
      },
    });
    const workflow = createWorkflow({
      id: workflowId,
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      steps: [step],
    })
      .then(step)
      .commit();
    const storage = new MockStore();
    const pubsub = new EventEmitterPubSub();
    const publish = vi.spyOn(pubsub, 'publish');
    const mastra = new Mastra({ logger: false, storage, pubsub, workflows: { [workflowId]: workflow } });
    const run = await workflow.createRun({ runId: 'restart-cancel-admitted' });
    const workflowsStore = await storage.getStore('workflows');
    const pending = await workflowsStore.loadWorkflowSnapshot({ workflowName: workflowId, runId: run.runId });
    await workflowsStore.persistWorkflowSnapshot({
      workflowName: workflowId,
      runId: run.runId,
      snapshot: {
        ...pending!,
        status: 'success',
        context: { input: {} },
        activePaths: [0],
        activeStepsPath: { 'only-step': [0] },
      },
    });

    const restarting = run.timeTravel({ step: 'only-step', inputData: {}, requestContext: new RequestContext() });
    await entered;
    const admitted = await workflowsStore.loadWorkflowSnapshot({ workflowName: workflowId, runId: run.runId });
    expect(admitted).toMatchObject({ status: 'running', executionGeneration: expect.any(String) });

    const remoteWorkflow = makeWorkflow();
    const remoteMastra = new Mastra({
      logger: false,
      storage,
      pubsub,
      workflows: { [workflowId]: remoteWorkflow },
    });
    const remoteRun = await remoteWorkflow.createRun({ runId: run.runId });

    await remoteRun.cancel();

    await expect(
      workflowsStore.loadWorkflowSnapshot({ workflowName: workflowId, runId: run.runId }),
    ).resolves.toMatchObject({ status: 'canceled', executionGeneration: admitted?.executionGeneration });
    releaseStep();
    await expect(restarting).resolves.toMatchObject({ status: 'canceled' });
    const lifecycleEvents = publish.mock.calls
      .filter(([topic]) => topic.startsWith('workflow.lifecycle.v1.'))
      .map(([, event]) => event.data.event);
    expect(lifecycleEvents).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ type: 'workflow.canceled' }),
        expect.objectContaining({ type: 'workflow.finished', status: 'canceled' }),
      ]),
    );
    expect(lifecycleEvents).not.toEqual(
      expect.arrayContaining([expect.objectContaining({ type: 'workflow.finished', status: 'success' })]),
    );
    expect(lifecycleEvents.at(-1)).toMatchObject({ type: 'workflow.finished', status: 'canceled' });
    await expect(
      workflowsStore.loadWorkflowSnapshot({ workflowName: workflowId, runId: run.runId }),
    ).resolves.toMatchObject({ status: 'canceled' });
    await Promise.all([mastra.shutdown(), remoteMastra.shutdown()]);
  });

  it.each(['restart', 'timeTravel'] as const)(
    'resets a canceled default-run controller before a fresh %s lineage',
    async kind => {
      const storage = new MockStore();
      const workflow = makeWorkflow();
      const mastra = new Mastra({ logger: false, storage, workflows: { [workflowId]: workflow } });
      const freshRunId = `canceled-controller-${kind}`;
      const run = await workflow.createRun({ runId: freshRunId });
      const workflowsStore = await storage.getStore('workflows');
      const pending = await workflowsStore.loadWorkflowSnapshot({ workflowName: workflowId, runId: freshRunId });
      const firstGeneration = pending?.executionGeneration;
      await workflowsStore.persistWorkflowSnapshot({
        workflowName: workflowId,
        runId: freshRunId,
        snapshot: {
          ...pending!,
          status: 'running',
          context: { input: {} },
          activePaths: [0],
          activeStepsPath: { 'only-step': [0] },
        },
      });

      await run.cancel();
      expect(run.abortController.signal.aborted).toBe(true);
      await expect(
        workflowsStore.loadWorkflowSnapshot({ workflowName: workflowId, runId: freshRunId }),
      ).resolves.toMatchObject({ status: 'canceled', executionGeneration: firstGeneration });

      if (kind === 'restart') {
        // restart() intentionally accepts only an active snapshot. Reopen the
        // durable recovery marker while retaining this handle's canceled
        // controller to prove the fresh lineage replaces process-local state.
        await workflowsStore.updateWorkflowState({
          workflowName: workflowId,
          runId: freshRunId,
          opts: { status: 'running' },
        });
      }

      const result =
        kind === 'restart'
          ? await run.restart({ requestContext: new RequestContext() })
          : await run.timeTravel({ step: 'only-step', inputData: {}, requestContext: new RequestContext() });

      expect(run.abortController.signal.aborted).toBe(false);
      expect(result).toMatchObject({ status: 'success' });
      const completed = await workflowsStore.loadWorkflowSnapshot({ workflowName: workflowId, runId: freshRunId });
      expect(completed).toMatchObject({ status: 'success', executionGeneration: expect.any(String) });
      expect(completed?.executionGeneration).not.toBe(firstGeneration);

      await mastra.shutdown();
    },
  );

  it.each([
    ['restart', 'running'],
    ['timeTravel', 'success'],
  ] as const)('admits a fresh lineage and durable running marker before %s execution', async (kind, status) => {
    const storage = new MockStore();
    const engine = new AdmissionInspectingEngine();
    const workflow = makeWorkflow(engine);
    const mastra = new Mastra({ logger: false, storage, workflows: { [workflowId]: workflow } });
    const admittedRunId = `${kind}-admission-run`;
    const run = await workflow.createRun({ runId: admittedRunId });
    const workflowsStore = await storage.getStore('workflows');
    const pending = await workflowsStore.loadWorkflowSnapshot({ workflowName: workflowId, runId: admittedRunId });
    await workflowsStore.persistWorkflowSnapshot({
      workflowName: workflowId,
      runId: admittedRunId,
      snapshot: {
        ...pending!,
        status,
        context: { input: {} },
        activePaths: [0],
        activeStepsPath: { 'only-step': [0] },
        executionGeneration: 'previous-execution-generation',
      },
    });

    if (kind === 'restart') {
      await run.restart({ requestContext: new RequestContext() });
    } else {
      await run.timeTravel({ step: 'only-step', inputData: {}, requestContext: new RequestContext() });
    }

    expect(engine.observations).toHaveLength(1);
    expect(engine.observations[0]).toMatchObject({
      kind,
      executionGeneration: expect.any(String),
      snapshotGeneration: expect.any(String),
      snapshotStatus: 'running',
    });
    expect(engine.observations[0]!.snapshotGeneration).toBe(engine.observations[0]!.executionGeneration);
    expect(engine.observations[0]!.executionGeneration).not.toBe('previous-execution-generation');

    await mastra.shutdown();
  });
});
