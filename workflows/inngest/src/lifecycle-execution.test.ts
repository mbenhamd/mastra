import { EventEmitterPubSub } from '@mastra/core/events';
import { Mastra } from '@mastra/core/mastra';
import { MockStore } from '@mastra/core/storage';
import { Inngest } from 'inngest';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { z } from 'zod';
import { InngestExecutionEngine } from './execution-engine';
import { init } from './index';

function createDurableStep() {
  const results = new Map<string, unknown>();
  return {
    run: vi.fn(async (id: string, operation: () => Promise<unknown>) => {
      if (results.has(id)) {
        return results.get(id);
      }
      const result = await operation();
      results.set(id, result);
      return result;
    }),
    sleep: vi.fn(),
    sleepUntil: vi.fn(),
    invoke: vi.fn(),
  };
}

function createCloningDurableStep() {
  const results = new Map<string, unknown>();
  return {
    run: vi.fn(async (id: string, operation: () => Promise<unknown>) => {
      if (results.has(id)) return structuredClone(results.get(id));
      const result = await operation();
      results.set(id, structuredClone(result));
      return structuredClone(result);
    }),
    sleep: vi.fn(),
    sleepUntil: vi.fn(),
    invoke: vi.fn(),
  };
}

async function createFixture(
  id: string,
  options: {
    execute?: (params: { inputData: { value: string } }) => Promise<{ value: string }>;
    retries?: number;
    configurePubsubFactory?: boolean;
  } = {},
) {
  const inngest = new Inngest({ id: `mastra-${id}` });
  const { createWorkflow, createStep } = init(inngest);
  const pubsub = {
    publish: vi.fn(async () => {}),
    subscribe: vi.fn(async () => {}),
    unsubscribe: vi.fn(async () => {}),
  };
  const step = createStep({
    id: 'only-step',
    inputSchema: z.object({ value: z.string() }),
    outputSchema: z.object({ value: z.string() }),
    retries: options.retries,
    execute: options.execute ?? (async ({ inputData }) => inputData),
  });
  const workflow = createWorkflow({
    id,
    inputSchema: z.object({ value: z.string() }),
    outputSchema: z.object({ value: z.string() }),
    steps: [step],
    ...(options.configurePubsubFactory === false ? {} : { pubsubFactory: () => pubsub as any }),
  })
    .then(step)
    .commit();
  const mastra = new Mastra({ logger: false, storage: new MockStore(), workflows: { [id]: workflow as any } });
  workflow.__registerMastra(mastra);

  return { workflow, mastra, pubsub };
}

describe('Inngest workflow lifecycle execution identity', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('mints a direct-start generation once inside a durable step and persists it', async () => {
    const { workflow, mastra } = await createFixture('direct-lifecycle-start');
    const execute = vi.spyOn(InngestExecutionEngine.prototype, 'execute').mockResolvedValue({
      status: 'success',
      steps: {},
      state: {},
      result: { value: 'ok' },
    } as any);
    const inngestFunction = workflow.getFunction() as any;
    const durableStep = createDurableStep();
    const invocation = {
      event: { data: { inputData: { value: 'ok' }, initialState: {} } },
      step: durableStep,
      attempt: 0,
    };

    const first = await inngestFunction.fn(invocation);
    const second = await inngestFunction.fn(invocation);

    expect(first.runId).toBe(second.runId);
    expect(execute).toHaveBeenCalledTimes(2);
    const firstExecution = execute.mock.calls[0]![0] as any;
    const replayExecution = execute.mock.calls[1]![0] as any;
    expect(firstExecution.executionGeneration).toEqual(expect.any(String));
    expect(replayExecution.executionGeneration).toBe(firstExecution.executionGeneration);
    expect(firstExecution.lifecycleResumeAttempt).toBe(0);
    expect(firstExecution.lifecycleStepStates).toEqual({});

    const workflowsStore = await mastra.getStorage()!.getStore('workflows');
    const snapshot = await workflowsStore!.loadWorkflowSnapshot({
      workflowName: workflow.id,
      runId: first.runId,
    });
    expect(snapshot?.executionGeneration).toBe(firstExecution.executionGeneration);
    expect(snapshot?.lifecycleResumeAttempt).toBe(0);
    expect(snapshot?.lifecycleStepStates).toEqual({});
  });

  it('makes ordinary Inngest workflow lifecycle delivery replayable without a private factory', async () => {
    const { workflow } = await createFixture('default-lifecycle-pubsub', { configurePubsubFactory: false });
    const publish = vi.spyOn(workflow.inngest.realtime, 'publish').mockResolvedValue(undefined as any);
    const inngestFunction = workflow.getFunction() as any;

    const output = await inngestFunction.fn({
      event: { data: { inputData: { value: 'ok' }, initialState: {} } },
      step: createDurableStep(),
      attempt: 0,
    });

    expect(output.result.status).toBe('success');
    const lifecycleDeliveries = publish.mock.calls
      .map(([, event]) => event as any)
      .filter(event => event.type === 'workflow.lifecycle');
    expect(lifecycleDeliveries.length).toBeGreaterThan(0);
    expect(lifecycleDeliveries).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          index: expect.any(Number),
          logGeneration: expect.any(String),
          data: expect.objectContaining({ event: expect.objectContaining({ type: 'workflow.started' }) }),
        }),
        expect.objectContaining({
          data: expect.objectContaining({ event: expect.objectContaining({ type: 'workflow.finished' }) }),
        }),
      ]),
    );
  });

  it('routes a workflow pubsub factory through the remote handler and replays its lifecycle history', async () => {
    const { workflow } = await createFixture('run-scoped-lifecycle-pubsub', { configurePubsubFactory: false });
    const send = vi.spyOn(workflow.inngest, 'send').mockResolvedValue({ ids: ['start-event'] } as any);
    const transport = new EventEmitterPubSub();
    workflow.__setPubsubFactory(() => transport);
    const run = await workflow.createRun({ runId: 'run-scoped-lifecycle' });
    await run.startAsync({ inputData: { value: 'ok' } });
    const eventData = (send.mock.calls[0]![0] as any).data;

    await (workflow.getFunction() as any).fn({
      event: { data: eventData },
      step: createDurableStep(),
      attempt: 0,
    });

    const transitions: string[] = [];
    const unsubscribe = await run.watchLifecycle(
      event => {
        transitions.push(event.event.type);
      },
      { allowProcessLocalReplay: true },
    );
    await unsubscribe();

    expect(transitions).toEqual(
      expect.arrayContaining([
        'workflow.started',
        'step.started',
        'step.completed',
        'step.finished',
        'workflow.finished',
      ]),
    );
  });

  it('rejects process-local per-run pubsub configuration', async () => {
    const { workflow } = await createFixture('per-run-pubsub-rejected', { configurePubsubFactory: false });

    await expect(workflow.createRun({ pubsub: new EventEmitterPubSub() })).rejects.toThrow(
      'set pubsubFactory on the workflow instead',
    );
  });

  it('hydrates an existing run handle from its persisted status', async () => {
    const { workflow, mastra } = await createFixture('stored-run-status');
    const run = await workflow.createRun({ runId: 'stored-success-run' });
    const workflowsStore = await mastra.getStorage()!.getStore('workflows');
    const pending = await workflowsStore!.loadWorkflowSnapshot({
      workflowName: workflow.id,
      runId: run.runId,
    });
    await workflowsStore!.persistWorkflowSnapshot({
      workflowName: workflow.id,
      runId: run.runId,
      snapshot: {
        ...pending!,
        status: 'success',
        result: { value: 'done' },
        timestamp: Date.now(),
      },
    });

    const restored = await workflow.createRun({ runId: run.runId });

    expect(restored).toBe(run);
    expect(restored.workflowRunStatus).toBe('success');
  });

  it('admits one pending lineage and reuses it across independent handles and start', async () => {
    const { workflow, mastra } = await createFixture('pending-lineage-admission');
    const send = vi.spyOn(workflow.inngest, 'send').mockResolvedValue({ ids: ['start-event'] } as any);
    const runId = 'reopened-pending-run';
    const original = await workflow.createRun({ runId });
    const workflowsStore = await mastra.getStorage()!.getStore('workflows');
    const admitted = await workflowsStore!.loadWorkflowSnapshot({ workflowName: workflow.id, runId });

    expect(admitted).toMatchObject({
      status: 'pending',
      executionGeneration: expect.any(String),
      lifecycleResumeAttempt: 0,
      lifecycleStepStates: {},
    });

    workflow.runs.delete(runId);
    const reopenedA = await workflow.createRun({ runId });
    workflow.runs.delete(runId);
    const reopenedB = await workflow.createRun({ runId });
    expect(reopenedA).not.toBe(original);
    expect(reopenedB).not.toBe(reopenedA);

    const [identityA, identityB] = await Promise.all([
      reopenedA.getLifecycleExecutionIdentity(),
      reopenedB.getLifecycleExecutionIdentity(),
    ]);
    expect(identityA).toEqual(identityB);
    expect(identityA.executionGeneration).toBe(admitted!.executionGeneration);

    await reopenedA.startAsync({ inputData: { value: 'ok' } });
    expect((send.mock.calls[0]![0] as any).data).toMatchObject({
      executionGeneration: admitted!.executionGeneration,
      lifecycleResumeAttempt: 0,
      lifecycleStepStates: {},
    });
  });

  it('fails closed for a persisted pending snapshot without lifecycle lineage', async () => {
    const { workflow, mastra } = await createFixture('legacy-pending-lineage');
    const workflowsStore = await mastra.getStorage()!.getStore('workflows');
    const runId = 'legacy-pending-run';
    await workflowsStore!.persistWorkflowSnapshot({
      workflowName: workflow.id,
      runId,
      snapshot: {
        runId,
        status: 'pending',
        value: {},
        context: {},
        activePaths: [],
        activeStepsPath: {},
        serializedStepGraph: workflow.serializedStepGraph,
        suspendedPaths: {},
        resumeLabels: {},
        waitingPaths: {},
        timestamp: Date.now(),
      },
    });

    await expect(workflow.createRun({ runId })).rejects.toThrow('complete lifecycle lineage');
    await expect(
      workflowsStore!.loadWorkflowSnapshot({ workflowName: workflow.id, runId }),
    ).resolves.not.toHaveProperty('executionGeneration');
  });

  it('rejects a partial lifecycle tuple at the direct Inngest handler boundary', async () => {
    const { workflow } = await createFixture('partial-lifecycle-tuple');
    const execute = vi.spyOn(InngestExecutionEngine.prototype, 'execute');

    await expect(
      (workflow.getFunction() as any).fn({
        event: {
          data: {
            inputData: { value: 'ok' },
            initialState: {},
            runId: 'partial-tuple-run',
            executionGeneration: 'partial-generation',
          },
        },
        step: createDurableStep(),
        attempt: 0,
      }),
    ).rejects.toThrow('requires a complete lifecycle execution tuple');
    expect(execute).not.toHaveBeenCalled();
  });

  it('admits an exact supplied start tuple from a fresh pending snapshot', async () => {
    const { workflow } = await createFixture('supplied-pending-start');
    const run = await workflow.createRun({ runId: 'supplied-pending-run' });
    const identity = await run.getLifecycleExecutionIdentity();
    const workflowsStore = await workflow.mastra?.getStorage()?.getStore('workflows');
    const snapshot = await workflowsStore?.loadWorkflowSnapshot({
      workflowName: workflow.id,
      runId: run.runId,
    });
    const execute = vi.spyOn(InngestExecutionEngine.prototype, 'execute').mockResolvedValue({
      status: 'success',
      steps: {},
      state: {},
      result: { value: 'ok' },
    } as any);

    const durableStep = createCloningDurableStep();
    const invocation = {
      event: {
        data: {
          inputData: { value: 'ok' },
          initialState: {},
          runId: run.runId,
          executionGeneration: identity.executionGeneration,
          lifecycleResumeAttempt: snapshot?.lifecycleResumeAttempt,
          lifecycleStepStates: snapshot?.lifecycleStepStates,
        },
      },
      step: durableStep,
      attempt: 0,
    };
    const output = await (workflow.getFunction() as any).fn(invocation);
    const replay = await (workflow.getFunction() as any).fn(invocation);

    expect(output.result.status).toBe('success');
    expect(replay.result.status).toBe('success');
    expect(execute).toHaveBeenCalledTimes(2);
  });

  it('rejects a supplied start tuple that does not own the pending snapshot', async () => {
    const { workflow } = await createFixture('stale-supplied-start');
    await workflow.createRun({ runId: 'stale-supplied-run' });
    const execute = vi.spyOn(InngestExecutionEngine.prototype, 'execute');

    await expect(
      (workflow.getFunction() as any).fn({
        event: {
          data: {
            inputData: { value: 'ok' },
            initialState: {},
            runId: 'stale-supplied-run',
            executionGeneration: 'stale-generation',
            lifecycleResumeAttempt: 0,
            lifecycleStepStates: {},
          },
        },
        step: createDurableStep(),
        attempt: 0,
      }),
    ).rejects.toThrow('lifecycle execution tuple is stale');
    expect(execute).not.toHaveBeenCalled();
  });

  it('fences a memoized retry after a newer generation takes over', async () => {
    const { workflow, mastra } = await createFixture('memoized-generation-fence');
    const execute = vi.spyOn(InngestExecutionEngine.prototype, 'execute').mockResolvedValue({
      status: 'success',
      steps: {},
      state: {},
      result: { value: 'ok' },
    } as any);
    const durableStep = createDurableStep();
    const invocation = {
      event: {
        data: {
          inputData: { value: 'ok' },
          initialState: {},
          runId: 'memoized-generation-run',
        },
      },
      step: durableStep,
      attempt: 0,
    };

    await (workflow.getFunction() as any).fn(invocation);
    const workflowsStore = await mastra.getStorage()!.getStore('workflows');
    await workflowsStore!.updateWorkflowState({
      workflowName: workflow.id,
      runId: 'memoized-generation-run',
      opts: {
        status: 'running',
        executionGeneration: 'new-owner-generation',
        lifecycleResumeAttempt: 0,
        lifecycleStepStates: {},
      },
    });

    await expect((workflow.getFunction() as any).fn(invocation)).rejects.toThrow('lifecycle execution tuple is stale');
    expect(execute).toHaveBeenCalledTimes(1);
  });

  it('accepts monotonic persisted step attempts on a structured-clone replay', async () => {
    const { workflow, mastra } = await createFixture('memoized-step-state-replay');
    const execute = vi.spyOn(InngestExecutionEngine.prototype, 'execute').mockResolvedValue({
      status: 'success',
      steps: {},
      state: {},
      result: { value: 'ok' },
    } as any);
    const durableStep = createCloningDurableStep();
    const invocation = {
      event: {
        data: {
          inputData: { value: 'ok' },
          initialState: {},
          runId: 'memoized-step-state-run',
        },
      },
      step: durableStep,
      attempt: 0,
    };

    await (workflow.getFunction() as any).fn(invocation);
    const firstExecution = execute.mock.calls[0]![0] as any;
    const workflowsStore = await mastra.getStorage()!.getStore('workflows');
    const advancedStates = {
      '["only-step",[0],null,null]': { stepCallId: 'advanced-step-call', stepAttempt: 3 },
    };
    await workflowsStore!.updateWorkflowState({
      workflowName: workflow.id,
      runId: 'memoized-step-state-run',
      opts: { lifecycleStepStates: advancedStates },
    });

    await (workflow.getFunction() as any).fn(invocation);

    const replayExecution = execute.mock.calls[1]![0] as any;
    expect(replayExecution.executionGeneration).toBe(firstExecution.executionGeneration);
    expect(replayExecution.lifecycleResumeAttempt).toBe(firstExecution.lifecycleResumeAttempt);
    expect(replayExecution.lifecycleStepStates).toEqual(advancedStates);
  });

  it('rejects a direct resume event when the persisted run is not suspended', async () => {
    const { workflow, mastra } = await createFixture('direct-resume-status');
    const workflowsStore = await mastra.getStorage()!.getStore('workflows');
    await workflowsStore!.persistWorkflowSnapshot({
      workflowName: workflow.id,
      runId: 'already-complete-run',
      snapshot: {
        runId: 'already-complete-run',
        serializedStepGraph: [],
        status: 'success',
        value: {},
        context: { input: { value: 'before' } },
        activePaths: [],
        suspendedPaths: {},
        activeStepsPath: {},
        resumeLabels: {},
        waitingPaths: {},
        timestamp: Date.now(),
        executionGeneration: 'complete-generation',
        lifecycleResumeAttempt: 0,
        lifecycleStepStates: {},
      },
    });
    const execute = vi.spyOn(InngestExecutionEngine.prototype, 'execute');

    await expect(
      (workflow.getFunction() as any).fn({
        event: {
          data: {
            runId: 'already-complete-run',
            inputData: { value: 'after' },
            initialState: {},
            resume: {
              steps: ['only-step'],
              stepResults: {},
              resumePayload: { value: 'after' },
              resumePath: [0],
            },
          },
        },
        step: createDurableStep(),
        attempt: 0,
      }),
    ).rejects.toThrow('workflow run is not suspended');

    await expect(
      (workflow.getFunction() as any).fn({
        event: {
          data: {
            runId: 'already-complete-run',
            inputData: { value: 'after' },
            initialState: {},
            resume: {
              steps: ['only-step'],
              stepResults: {},
              resumePayload: { value: 'after' },
              resumePath: [0],
            },
            executionGeneration: 'complete-generation',
            lifecycleResumeAttempt: 1,
            lifecycleStepStates: {},
          },
        },
        step: createDurableStep(),
        attempt: 0,
      }),
    ).rejects.toThrow('lifecycle execution tuple is stale');
    expect(execute).not.toHaveBeenCalled();
  });

  it('executes an exactly pre-reserved supplied nested-style resume tuple', async () => {
    const { workflow, mastra } = await createFixture('supplied-resume-reservation');
    const workflowsStore = await mastra.getStorage()!.getStore('workflows');
    const lifecycleStepStates = {
      '["only-step",[0],null,null]': { stepCallId: 'supplied-step-call', stepAttempt: 2 },
    };
    await workflowsStore!.persistWorkflowSnapshot({
      workflowName: workflow.id,
      runId: 'supplied-resume-run',
      snapshot: {
        runId: 'supplied-resume-run',
        serializedStepGraph: [],
        status: 'running',
        value: {},
        context: {},
        activePaths: [],
        suspendedPaths: { 'only-step': [0] },
        activeStepsPath: {},
        resumeLabels: {},
        waitingPaths: {},
        timestamp: Date.now(),
        executionGeneration: 'supplied-resume-generation',
        lifecycleResumeAttempt: 2,
        lifecycleStepStates,
      },
    });
    const execute = vi.spyOn(InngestExecutionEngine.prototype, 'execute').mockImplementation(async () => {
      const reserved = await workflowsStore!.loadWorkflowSnapshot({
        workflowName: workflow.id,
        runId: 'supplied-resume-run',
      });
      expect(reserved).toMatchObject({
        status: 'running',
        executionGeneration: 'supplied-resume-generation',
        lifecycleResumeAttempt: 2,
        lifecycleStepStates,
      });
      return { status: 'success', steps: {}, state: {}, result: { value: 'ok' } } as any;
    });

    const output = await (workflow.getFunction() as any).fn({
      event: {
        data: {
          runId: 'supplied-resume-run',
          inputData: { value: 'after' },
          initialState: {},
          resume: {
            steps: ['only-step'],
            stepResults: {},
            resumePayload: { value: 'after' },
            resumePath: [0],
          },
          executionGeneration: 'supplied-resume-generation',
          lifecycleResumeAttempt: 2,
          lifecycleStepStates,
        },
      },
      step: createDurableStep(),
      attempt: 0,
    });

    expect(output.result.status).toBe('success');
    expect(execute).toHaveBeenCalledTimes(1);
  });

  it('retains the durable retry ordinal in lifecycle events and the final snapshot', async () => {
    let invocations = 0;
    const { workflow, mastra, pubsub } = await createFixture('lifecycle-retry-ordinal', {
      retries: 1,
      execute: async ({ inputData }) => {
        invocations += 1;
        if (invocations === 1) throw new Error('transient');
        return inputData;
      },
    });

    const output = await (workflow.getFunction() as any).fn({
      event: { data: { inputData: { value: 'ok' }, initialState: {}, runId: 'retry-run' } },
      step: createDurableStep(),
      attempt: 0,
    });

    expect(output.result.status).toBe('success');
    const transitions = pubsub.publish.mock.calls.map(([, event]) => (event as any).data?.event).filter(Boolean);
    expect(transitions).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ type: 'step.started', stepAttempt: 1 }),
        expect.objectContaining({ type: 'step.retrying', stepAttempt: 2 }),
        expect.objectContaining({ type: 'step.completed', stepAttempt: 2 }),
        expect.objectContaining({ type: 'step.finished', stepAttempt: 2 }),
      ]),
    );

    const workflowsStore = await mastra.getStorage()!.getStore('workflows');
    const snapshot = await workflowsStore!.loadWorkflowSnapshot({
      workflowName: workflow.id,
      runId: 'retry-run',
    });
    expect(Object.values(snapshot?.lifecycleStepStates ?? {})).toEqual([expect.objectContaining({ stepAttempt: 2 })]);
  });

  it('retains the final durable retry ordinal when every attempt fails', async () => {
    const { workflow, mastra, pubsub } = await createFixture('lifecycle-retry-exhausted', {
      retries: 1,
      execute: async () => {
        throw new Error('still transient');
      },
    });

    await expect(
      (workflow.getFunction() as any).fn({
        event: { data: { inputData: { value: 'nope' }, initialState: {}, runId: 'retry-exhausted-run' } },
        step: createDurableStep(),
        attempt: 0,
      }),
    ).rejects.toThrow('Workflow failed');

    const transitions = pubsub.publish.mock.calls.map(([, event]) => (event as any).data?.event).filter(Boolean);
    expect(transitions).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ type: 'step.started', stepAttempt: 1 }),
        expect.objectContaining({ type: 'step.retrying', stepAttempt: 2 }),
        expect.objectContaining({ type: 'step.failed', stepAttempt: 2 }),
        expect.objectContaining({ type: 'step.finished', stepAttempt: 2 }),
      ]),
    );

    const workflowsStore = await mastra.getStorage()!.getStore('workflows');
    const snapshot = await workflowsStore!.loadWorkflowSnapshot({
      workflowName: workflow.id,
      runId: 'retry-exhausted-run',
    });
    expect(Object.values(snapshot?.lifecycleStepStates ?? {})).toEqual([expect.objectContaining({ stepAttempt: 2 })]);
  });

  it('recovers a direct-resume lineage once and keeps it stable across replay', async () => {
    const { workflow, mastra } = await createFixture('direct-lifecycle-resume');
    const workflowsStore = await mastra.getStorage()!.getStore('workflows');
    await workflowsStore!.persistWorkflowSnapshot({
      workflowName: workflow.id,
      runId: 'resume-run',
      snapshot: {
        runId: 'resume-run',
        serializedStepGraph: [],
        status: 'suspended',
        value: {},
        context: { input: { value: 'before' } },
        activePaths: [],
        suspendedPaths: { 'only-step': [0] },
        activeStepsPath: {},
        resumeLabels: {},
        waitingPaths: {},
        timestamp: Date.now(),
        executionGeneration: 'persisted-resume-generation',
        lifecycleResumeAttempt: 2,
        lifecycleStepStates: {
          '0:only-step': { stepCallId: 'persisted-step-call', stepAttempt: 1 },
        },
      },
    });
    const execute = vi.spyOn(InngestExecutionEngine.prototype, 'execute').mockResolvedValue({
      status: 'success',
      steps: {},
      state: {},
      result: { value: 'ok' },
    } as any);
    const inngestFunction = workflow.getFunction() as any;
    const durableStep = createDurableStep();
    const invocation = {
      event: {
        data: {
          runId: 'resume-run',
          inputData: { value: 'after' },
          initialState: {},
          resume: {
            steps: ['only-step'],
            stepResults: {},
            resumePayload: { value: 'after' },
            resumePath: [0],
          },
        },
      },
      step: durableStep,
      attempt: 0,
    };

    await inngestFunction.fn(invocation);
    await inngestFunction.fn(invocation);

    const firstExecution = execute.mock.calls[0]![0] as any;
    const replayExecution = execute.mock.calls[1]![0] as any;
    expect(firstExecution.executionGeneration).toBe('persisted-resume-generation');
    expect(replayExecution.executionGeneration).toBe('persisted-resume-generation');
    expect(firstExecution.lifecycleResumeAttempt).toBe(3);
    expect(replayExecution.lifecycleResumeAttempt).toBe(3);
    expect(firstExecution.lifecycleStepStates).toEqual({
      '0:only-step': { stepCallId: 'persisted-step-call', stepAttempt: 1 },
    });
  });

  it('publishes terminal lifecycle records when a remote Inngest run is canceled', async () => {
    const { workflow, mastra, pubsub } = await createFixture('lifecycle-cancel');
    const send = vi.spyOn(workflow.inngest, 'send').mockResolvedValue({ ids: ['start-event'] } as any);
    const run = await workflow.createRun();
    await run.startAsync({ inputData: { value: 'ok' } });
    const startEvent = (send.mock.calls[0]![0] as any).data;

    await run.cancel();

    expect(send).toHaveBeenCalledTimes(2);
    expect(pubsub.publish).toHaveBeenNthCalledWith(
      1,
      expect.stringContaining(startEvent.executionGeneration),
      expect.objectContaining({
        data: expect.objectContaining({
          executionGeneration: startEvent.executionGeneration,
          event: { type: 'workflow.canceled', resumeAttempt: 0 },
        }),
      }),
      undefined,
    );
    expect(pubsub.publish).toHaveBeenNthCalledWith(
      2,
      expect.stringContaining(startEvent.executionGeneration),
      expect.objectContaining({
        data: expect.objectContaining({
          executionGeneration: startEvent.executionGeneration,
          event: { type: 'workflow.finished', resumeAttempt: 0, status: 'canceled' },
        }),
      }),
      undefined,
    );

    const workflowsStore = await mastra.getStorage()!.getStore('workflows');
    const snapshot = await workflowsStore!.loadWorkflowSnapshot({
      workflowName: workflow.id,
      runId: run.runId,
    });
    expect(snapshot?.status).toBe('canceled');
    expect(snapshot?.executionGeneration).toBe(startEvent.executionGeneration);
  });

  it.each(['success', 'failed'] as const)(
    'does not dispatch or overwrite cancellation for an already-%s Inngest run',
    async terminalStatus => {
      const { workflow, mastra, pubsub } = await createFixture(`inngest-cancel-${terminalStatus}`);
      const send = vi.spyOn(workflow.inngest, 'send').mockResolvedValue({ ids: ['cancel-event'] } as any);
      const run = await workflow.createRun({ runId: `already-${terminalStatus}-run` });
      const workflowsStore = await mastra.getStorage()!.getStore('workflows');
      await workflowsStore!.updateWorkflowState({
        workflowName: workflow.id,
        runId: run.runId,
        opts: { status: terminalStatus },
      });

      await run.cancel();

      expect(send).not.toHaveBeenCalled();
      expect(pubsub.publish).not.toHaveBeenCalled();
      await expect(
        workflowsStore!.loadWorkflowSnapshot({ workflowName: workflow.id, runId: run.runId }),
      ).resolves.toMatchObject({ status: terminalStatus });
    },
  );

  it('reloads after cancellation dispatch and preserves a concurrently completed Inngest run', async () => {
    const { workflow, mastra, pubsub } = await createFixture('inngest-cancel-concurrent-terminal');
    const run = await workflow.createRun({ runId: 'cancel-completes-during-send' });
    const workflowsStore = await mastra.getStorage()!.getStore('workflows');
    vi.spyOn(workflow.inngest, 'send').mockImplementation(async () => {
      await workflowsStore!.updateWorkflowState({
        workflowName: workflow.id,
        runId: run.runId,
        opts: { status: 'success' },
      });
      return { ids: ['cancel-event'] } as any;
    });

    await run.cancel();

    await expect(
      workflowsStore!.loadWorkflowSnapshot({ workflowName: workflow.id, runId: run.runId }),
    ).resolves.toMatchObject({ status: 'success' });
    expect(pubsub.publish).not.toHaveBeenCalled();
  });

  it('correlates cancellation to the triggering run id', async () => {
    const { workflow } = await createFixture('run-scoped-cancel-trigger');
    const inngestFunction = workflow.getFunction() as any;

    expect(inngestFunction.opts.cancelOn).toEqual([
      {
        event: `cancel.workflow.${workflow.id}`,
        if: 'async.data.runId == event.data.runId && async.data.executionGeneration == event.data.executionGeneration && async.data.lifecycleResumeAttempt == event.data.lifecycleResumeAttempt',
      },
    ]);
  });

  it('reserves lifecycle identity when an Inngest run is canceled before start', async () => {
    const { workflow, mastra, pubsub } = await createFixture('lifecycle-cancel-before-start');
    vi.spyOn(workflow.inngest, 'send').mockResolvedValue({ ids: ['cancel-event'] } as any);
    const run = await workflow.createRun({ runId: 'pending-cancel-run' });

    await run.cancel();

    const identity = await run.getLifecycleExecutionIdentity();
    expect(pubsub.publish).toHaveBeenCalledTimes(2);
    expect(pubsub.publish).toHaveBeenNthCalledWith(
      1,
      identity.topic,
      expect.objectContaining({
        data: expect.objectContaining({
          executionGeneration: identity.executionGeneration,
          event: { type: 'workflow.canceled', resumeAttempt: 0 },
        }),
      }),
      undefined,
    );
    const workflowsStore = await mastra.getStorage()!.getStore('workflows');
    await expect(
      workflowsStore!.loadWorkflowSnapshot({ workflowName: workflow.id, runId: run.runId }),
    ).resolves.toMatchObject({
      status: 'canceled',
      executionGeneration: identity.executionGeneration,
    });
  });

  it('emits canonical terminal lifecycle events for a nested Inngest workflow step', async () => {
    const inngest = new Inngest({ id: 'mastra-nested-lifecycle' });
    const { createWorkflow, createStep } = init(inngest);
    const childStep = createStep({
      id: 'child-step',
      inputSchema: z.object({ value: z.string() }),
      outputSchema: z.object({ value: z.string() }),
      execute: async ({ inputData }) => inputData,
    });
    const child = createWorkflow({
      id: 'child-workflow',
      inputSchema: z.object({ value: z.string() }),
      outputSchema: z.object({ value: z.string() }),
      steps: [childStep],
    })
      .then(childStep)
      .commit();
    const parent = createWorkflow({
      id: 'parent-workflow',
      inputSchema: z.object({ value: z.string() }),
      outputSchema: z.object({ value: z.string() }),
      steps: [child as any],
    })
      .then(child as any)
      .commit();
    const mastra = new Mastra({ logger: false, storage: new MockStore(), workflows: { parent: parent as any } });
    parent.__registerMastra(mastra);
    const pubsub = {
      publish: vi.fn(async () => {}),
      subscribe: vi.fn(async () => {}),
      unsubscribe: vi.fn(async () => {}),
    };
    parent.__setPubsubFactory(() => pubsub as any);
    const durableStep = createDurableStep();
    durableStep.invoke.mockResolvedValue({
      result: { status: 'success', result: { value: 'nested-ok' }, steps: {}, state: {} },
      runId: 'child-run',
    });

    const output = await (parent.getFunction() as any).fn({
      event: { data: { inputData: { value: 'ok' }, initialState: {}, runId: 'parent-run' } },
      step: durableStep,
      attempt: 0,
    });

    expect(output.result.status).toBe('success');
    const childTransitions = pubsub.publish.mock.calls
      .map(([, event]) => (event as any).data?.event)
      .filter(event => event?.stepId === 'child-workflow');
    expect(childTransitions).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ type: 'step.started', stepAttempt: 1 }),
        expect.objectContaining({ type: 'step.completed', stepAttempt: 1 }),
        expect.objectContaining({ type: 'step.finished', stepAttempt: 1, status: 'success' }),
      ]),
    );
  });

  it('rejects a nested resume when the child snapshot is not suspended', async () => {
    const inngest = new Inngest({ id: 'mastra-nested-resume-status' });
    const { createWorkflow, createStep } = init(inngest);
    const childStep = createStep({
      id: 'child-step',
      inputSchema: z.object({ value: z.string() }),
      outputSchema: z.object({ value: z.string() }),
      execute: async ({ inputData }) => inputData,
    });
    const child = createWorkflow({
      id: 'child-resume-status',
      inputSchema: z.object({ value: z.string() }),
      outputSchema: z.object({ value: z.string() }),
      steps: [childStep],
    })
      .then(childStep)
      .commit();
    const invoke = vi.fn();
    const durableStep = {
      ...createDurableStep(),
      invoke,
    };
    const mastra = new Mastra({ logger: false, storage: new MockStore() });
    const workflowsStore = await mastra.getStorage()!.getStore('workflows');
    await workflowsStore!.persistWorkflowSnapshot({
      workflowName: child.id,
      runId: 'completed-child-run',
      snapshot: {
        runId: 'completed-child-run',
        serializedStepGraph: [],
        status: 'success',
        value: {},
        context: {},
        activePaths: [],
        suspendedPaths: {},
        activeStepsPath: {},
        resumeLabels: {},
        waitingPaths: {},
        timestamp: Date.now(),
        executionGeneration: 'completed-child-generation',
        lifecycleResumeAttempt: 0,
        lifecycleStepStates: {},
      },
    });
    const engine = new InngestExecutionEngine(mastra, durableStep as any, 0, {});

    const result = await engine.executeWorkflowStep({
      step: child as any,
      stepResults: {
        'child-resume-status': {
          status: 'suspended',
          suspendPayload: { __workflow_meta: { runId: 'completed-child-run' } },
        },
      },
      executionContext: {
        workflowId: 'parent',
        runId: 'parent-run',
        executionPath: [0],
        suspendedPaths: {},
        state: {},
      } as any,
      resume: { steps: ['child-resume-status'], resumePayload: { value: 'after' } },
      prevOutput: {},
      inputData: { value: 'after' },
      pubsub: { publish: vi.fn() } as any,
      startedAt: Date.now(),
    } as any);

    expect(result).toMatchObject({
      status: 'failed',
      error: expect.objectContaining({ message: expect.stringContaining('workflow run is not suspended') }),
    });
    expect(invoke).not.toHaveBeenCalled();
  });

  it('rejects resume dispatch when the persisted run is not suspended', async () => {
    const { workflow, mastra } = await createFixture('lifecycle-resume-status');
    const run = await workflow.createRun({ runId: 'completed-run' });
    const workflowsStore = await mastra.getStorage()!.getStore('workflows');
    await workflowsStore!.persistWorkflowSnapshot({
      workflowName: workflow.id,
      runId: run.runId,
      snapshot: {
        runId: run.runId,
        serializedStepGraph: run.serializedStepGraph,
        status: 'success',
        value: {},
        context: { input: { value: 'before' } },
        activePaths: [],
        suspendedPaths: {},
        activeStepsPath: {},
        resumeLabels: {},
        waitingPaths: {},
        timestamp: Date.now(),
        executionGeneration: 'completed-resume-generation',
        lifecycleResumeAttempt: 0,
        lifecycleStepStates: {},
      },
    });
    const send = vi.spyOn(workflow.inngest, 'send');

    await expect(run.resumeAsync({ resumeData: { value: 'after' }, step: 'only-step' })).rejects.toThrow(
      'workflow run is not suspended',
    );
    expect(send).not.toHaveBeenCalled();
  });

  it('rolls a failed start dispatch back without changing the reserved lineage', async () => {
    const { workflow, mastra } = await createFixture('lifecycle-start-rollback');
    const send = vi
      .spyOn(workflow.inngest, 'send')
      .mockRejectedValueOnce(new Error('dispatch failed'))
      .mockResolvedValueOnce({ ids: ['retry-event'] } as any);
    const run = await workflow.createRun();
    const identity = await run.getLifecycleExecutionIdentity();

    await expect(run.startAsync({ inputData: { value: 'first' } })).rejects.toThrow('dispatch failed');

    const workflowsStore = await mastra.getStorage()!.getStore('workflows');
    const rolledBack = await workflowsStore!.loadWorkflowSnapshot({
      workflowName: workflow.id,
      runId: run.runId,
    });
    expect(rolledBack?.status).toBe('pending');
    expect(rolledBack?.executionGeneration).toBe(identity.executionGeneration);
    expect(run.workflowRunStatus).toBe('pending');

    await run.startAsync({ inputData: { value: 'second' } });
    const retriedEvent = (send.mock.calls[1]![0] as any).data;
    expect(retriedEvent.executionGeneration).toBe(identity.executionGeneration);
    expect(retriedEvent.lifecycleResumeAttempt).toBe(0);
  });

  it('rolls time travel back when Inngest accepts no event id', async () => {
    const { workflow, mastra } = await createFixture('lifecycle-time-travel-rollback');
    const run = await workflow.createRun();
    const workflowsStore = await mastra.getStorage()!.getStore('workflows');
    await workflowsStore!.persistWorkflowSnapshot({
      workflowName: workflow.id,
      runId: run.runId,
      snapshot: {
        runId: run.runId,
        serializedStepGraph: run.serializedStepGraph,
        status: 'success',
        value: {},
        context: { input: { value: 'before' } },
        activePaths: [],
        suspendedPaths: {},
        activeStepsPath: {},
        resumeLabels: {},
        waitingPaths: {},
        timestamp: Date.now(),
        executionGeneration: 'completed-generation',
        lifecycleResumeAttempt: 0,
        lifecycleStepStates: {},
      },
    });
    vi.spyOn(workflow.inngest, 'send').mockResolvedValue({ ids: [] } as any);

    await expect(run.timeTravel({ step: 'only-step', inputData: { value: 'after' } })).rejects.toThrow(
      'Event ID is not set',
    );

    const snapshot = await workflowsStore!.loadWorkflowSnapshot({
      workflowName: workflow.id,
      runId: run.runId,
    });
    expect(snapshot?.status).toBe('success');
    expect(snapshot?.executionGeneration).toBe('completed-generation');
    expect(run.workflowRunStatus).toBe('success');
  });
});
