import { MastraNonRetryableError } from '@mastra/core/error';
import { Mastra } from '@mastra/core/mastra';
import { MockStore } from '@mastra/core/storage';
import { Inngest, NonRetriableError } from 'inngest';
import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod';
import { InngestExecutionEngine } from './execution-engine';
import { init } from './index';

function createEngine() {
  const inngestStep = {
    run: vi.fn(async (_id: string, fn: () => Promise<unknown>) => fn()),
    sleep: vi.fn(),
    sleepUntil: vi.fn(),
  };

  return new InngestExecutionEngine(undefined as any, inngestStep as any, 0, {} as any);
}

describe('InngestExecutionEngine.executeStepWithRetry', () => {
  it('does not retry MastraNonRetryableError failures', async () => {
    const engine = createEngine();
    let calls = 0;

    const result = await engine.executeStepWithRetry(
      'workflow.test.step.fatal',
      async () => {
        calls++;
        throw new MastraNonRetryableError('permanent failure');
      },
      { retries: 3, delay: 0, workflowId: 'test-workflow', runId: 'test-run' },
    );

    expect(calls).toBe(1);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error.nonRetryable).toBe(true);
    }
  });

  it('does not retry Inngest NonRetriableError failures', async () => {
    const engine = createEngine();
    let calls = 0;

    const result = await engine.executeStepWithRetry(
      'workflow.test.step.fatal',
      async () => {
        calls++;
        throw new NonRetriableError('permanent failure');
      },
      { retries: 3, delay: 0, workflowId: 'test-workflow', runId: 'test-run' },
    );

    expect(calls).toBe(1);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error.nonRetryable).toBe(true);
    }
  });

  it('does not retry when a wrapped error carries a NonRetriableError cause', async () => {
    const engine = createEngine();
    let calls = 0;

    const result = await engine.executeStepWithRetry(
      'workflow.test.step.fatal',
      async () => {
        calls++;
        throw new Error('wrapped failure', { cause: new NonRetriableError('permanent failure') });
      },
      { retries: 3, delay: 0, workflowId: 'test-workflow', runId: 'test-run' },
    );

    expect(calls).toBe(1);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error.nonRetryable).toBe(true);
    }
  });

  it('retries transient errors until retry attempts are exhausted', async () => {
    const engine = createEngine();
    let calls = 0;

    const result = await engine.executeStepWithRetry(
      'workflow.test.step.transient',
      async () => {
        calls++;
        throw new Error('transient failure');
      },
      { retries: 3, delay: 0, workflowId: 'test-workflow', runId: 'test-run' },
    );

    expect(calls).toBe(4);
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error.nonRetryable).toBeUndefined();
    }
  });

  it('surfaces the correct retryCount on each retry attempt', async () => {
    const engine = createEngine();
    const seenRetryCounts: number[] = [];
    const receivedRetryCounts: number[] = [];
    const durableOperation = vi.spyOn(engine, 'wrapDurableOperation');

    const result = await engine.executeStepWithRetry(
      'workflow.test-wf.step.my-step',
      async retryCount => {
        receivedRetryCounts.push(retryCount);
        seenRetryCounts.push(engine.getOrGenerateRetryCount('my-step'));
        throw new Error('transient failure');
      },
      { retries: 3, delay: 0, workflowId: 'test-wf', runId: 'test-run' },
    );

    expect(seenRetryCounts).toEqual([0, 1, 2, 3]);
    expect(receivedRetryCounts).toEqual([0, 1, 2, 3]);
    expect(durableOperation.mock.calls.map(([operationId]) => operationId)).toEqual([
      'workflow.test-wf.step.my-step.attempt.0',
      'workflow.test-wf.step.my-step.attempt.1',
      'workflow.test-wf.step.my-step.attempt.2',
      'workflow.test-wf.step.my-step.attempt.3',
    ]);
    expect(result).toMatchObject({ ok: false, error: { retryCount: 3 } });
  });

  it('surfaces correct retryCount when workflowId contains ".step."', async () => {
    const engine = createEngine();
    const seenRetryCounts: number[] = [];

    await engine.executeStepWithRetry(
      'workflow.my.step.workflow.step.my-step',
      async () => {
        seenRetryCounts.push(engine.getOrGenerateRetryCount('my-step'));
        throw new Error('transient failure');
      },
      { retries: 2, delay: 0, workflowId: 'my.step.workflow', runId: 'test-run' },
    );

    expect(seenRetryCounts).toEqual([0, 1, 2]);
  });

  it('isolates retryCount across concurrent .foreach() iterations', async () => {
    const engine = createEngine();
    const seenByIteration: Record<string, number[]> = { a: [], b: [] };

    await Promise.all([
      engine.executeStepWithRetry(
        'workflow.wf.step.shared-step',
        async () => {
          seenByIteration['a']!.push(engine.getOrGenerateRetryCount('shared-step'));
          throw new Error('transient');
        },
        { retries: 2, delay: 0, workflowId: 'wf', runId: 'run-a' },
      ),
      engine.executeStepWithRetry(
        'workflow.wf.step.shared-step',
        async () => {
          seenByIteration['b']!.push(engine.getOrGenerateRetryCount('shared-step'));
          throw new Error('transient');
        },
        { retries: 2, delay: 0, workflowId: 'wf', runId: 'run-b' },
      ),
    ]);

    expect(seenByIteration['a']).toEqual([0, 1, 2]);
    expect(seenByIteration['b']).toEqual([0, 1, 2]);
  });
});

function createNestedResumeFixture(
  suspendedPaths: Record<string, number[]>,
  options: {
    /** foreach iteration index on the execution context. */
    foreachIndex?: number;
    /** Execution path of the nested workflow step. */
    executionPath?: number[];
    /** One-based loop iteration count attached by the core handler. */
    iterationCount?: number;
    /** Input value used to distinguish loop iterations in the terminal guard. */
    inputValue?: string;
    /** 'fresh' calls executeWorkflowStep without resume data. */
    mode?: 'resume' | 'fresh';
    /** Overrides the mocked step.invoke implementation. */
    invokeImpl?: (id: string, opts: any) => Promise<any>;
    /** Registers a spy logger on the engine via __registerMastra. */
    withLoggerSpy?: boolean;
    /** Runs the nested workflow through the time-travel branch. */
    timeTravel?: boolean;
    /** Run id recorded in the parent step's time-travel metadata. */
    timeTravelRunId?: string;
    /** Lifecycle generation used to derive a fresh nested run identity. */
    executionGeneration?: string;
  } = {},
) {
  const {
    foreachIndex,
    executionPath = [0],
    iterationCount,
    inputValue = 'start',
    mode = 'resume',
    invokeImpl,
    withLoggerSpy,
    timeTravel = false,
    timeTravelRunId,
    executionGeneration,
  } = options;
  const inngest = new Inngest({ id: 'nested-resume-test' });
  const { createWorkflow, createStep } = init(inngest);
  const suspendedStep = createStep({
    id: 'suspended-child-step',
    inputSchema: z.object({ value: z.string() }),
    outputSchema: z.object({ value: z.string() }),
    execute: async ({ inputData }) => inputData,
  });
  const nestedWorkflow = createWorkflow({
    id: 'nested-resume-workflow',
    inputSchema: z.object({ value: z.string() }),
    outputSchema: z.object({ value: z.string() }),
    steps: [suspendedStep],
  })
    .then(suspendedStep)
    .commit();

  const nestedRunId = 'nested-run';
  const parentRunId = 'parent-run';
  const nestedStepResults = Object.fromEntries(
    Object.keys(suspendedPaths).map(stepId => [stepId, { status: 'suspended', payload: { value: 'before-suspend' } }]),
  );
  const loadWorkflowSnapshot = vi.fn().mockResolvedValue({
    runId: nestedRunId,
    resourceId: 'nested-resource',
    status: 'suspended',
    value: { count: 1 },
    context: nestedStepResults,
    suspendedPaths,
    executionGeneration: 'nested-execution-generation',
    lifecycleResumeAttempt: 0,
    lifecycleStepStates: {},
  });
  const admitWorkflowResume = vi.fn().mockResolvedValue({ status: 'admitted' });
  const logger = { error: vi.fn(), warn: vi.fn(), info: vi.fn(), debug: vi.fn() };
  const mastra = {
    getStorage: () => ({
      getStore: async () => ({
        loadWorkflowSnapshot,
        admitWorkflowResume,
        getWorkflowResumeCapabilities: () => ({ atomicResumeVersion: 1, fencedStepUpdateVersion: 1 }),
      }),
    }),
    ...(withLoggerSpy ? { getLogger: () => logger } : {}),
  } as unknown as Mastra;
  const invoke = vi.fn(
    invokeImpl ??
      (async (_id: string, opts: any) => ({
        result: { status: 'success', result: { value: 'resumed' }, state: { count: 2 } },
        runId: opts?.data?.runId ?? nestedRunId,
      })),
  );
  const inngestStep = {
    invoke,
    run: vi.fn(async (_id: string, fn: () => Promise<unknown>) => fn()),
    sleep: vi.fn(),
    sleepUntil: vi.fn(),
  };
  const engine = new InngestExecutionEngine(mastra, inngestStep as any, 0, {} as any);
  if (withLoggerSpy) {
    engine.__registerMastra(mastra);
  }
  const resumePayload = { approved: true };
  const parentStepResult =
    mode === 'fresh'
      ? ({
          status: 'running',
          payload: { value: inputValue },
          ...(timeTravelRunId === undefined ? {} : { suspendPayload: { __workflow_meta: { runId: timeTravelRunId } } }),
          ...(iterationCount === undefined ? {} : { metadata: { iterationCount } }),
        } as any)
      : ({
          status: 'suspended',
          suspendPayload: { __workflow_meta: { runId: nestedRunId } },
          ...(iterationCount === undefined ? {} : { metadata: { iterationCount } }),
        } as any);
  const executionContext = {
    workflowId: 'parent-workflow',
    runId: parentRunId,
    executionPath,
    suspendedPaths: {},
    state: {},
    ...(executionGeneration === undefined ? {} : { executionGeneration }),
    ...(foreachIndex !== undefined ? { foreachIndex } : {}),
  } as any;
  const execute = () =>
    engine.executeWorkflowStep({
      step: nestedWorkflow as any,
      stepResults: { [nestedWorkflow.id]: parentStepResult },
      executionContext,
      ...(mode === 'resume' ? { resume: { steps: [nestedWorkflow.id], resumePayload } } : {}),
      ...(timeTravel
        ? {
            timeTravel: {
              steps: [nestedWorkflow.id, suspendedStep.id],
              inputData: { value: inputValue },
              nestedStepResults: {},
            } as any,
          }
        : {}),
      prevOutput: {},
      inputData: { value: inputValue },
      pubsub: { publish: vi.fn().mockResolvedValue(undefined) } as any,
      startedAt: Date.now(),
    });

  return {
    execute,
    engine,
    invoke,
    loadWorkflowSnapshot,
    logger,
    nestedRunId,
    parentRunId,
    nestedWorkflow,
    resumePayload,
    suspendedStep,
    executionContext,
  };
}

describe('InngestExecutionEngine.executeWorkflowStep', () => {
  it('restores the suspended child path when resuming with only the nested workflow id', async () => {
    const fixture = createNestedResumeFixture({ 'suspended-child-step': [1, 0] });
    const { execute, invoke, loadWorkflowSnapshot, nestedRunId, nestedWorkflow, resumePayload, suspendedStep } =
      fixture;

    await execute();

    expect(loadWorkflowSnapshot).toHaveBeenCalledWith({
      workflowName: nestedWorkflow.id,
      runId: nestedRunId,
    });
    expect(invoke).toHaveBeenCalledTimes(1);
    expect(invoke.mock.calls[0]?.[1].data).not.toHaveProperty('initialState');
    expect(invoke.mock.calls[0]?.[1].data.resume).toEqual({
      runId: nestedRunId,
      steps: [suspendedStep.id],
      resumePayload,
      resumePath: [1, 0],
    });
  });

  it('replays the memoized invoke on the delivery pass when the child is no longer suspended', async () => {
    // step.invoke parks the parent until the child finishes, so Inngest re-executes
    // the resume block to deliver the memoized result — by which point the child has
    // no suspended paths left. That pass must replay, not fail.
    const fixture = createNestedResumeFixture({});
    const { execute, invoke } = fixture;
    fixture.loadWorkflowSnapshot.mockResolvedValue({
      runId: fixture.nestedRunId,
      resourceId: 'nested-resource',
      status: 'success',
      value: { count: 2 },
      context: {},
      suspendedPaths: {},
      executionGeneration: 'nested-execution-generation',
      lifecycleResumeAttempt: 0,
      lifecycleStepStates: {},
    });

    const result = await execute();

    expect(invoke).toHaveBeenCalledTimes(1);
    expect(invoke.mock.calls[0]?.[1].data).not.toHaveProperty('resume');
    expect(invoke.mock.calls[0]?.[1].data).toHaveProperty('initialState');
    expect(result).toMatchObject({ status: 'success', output: { value: 'resumed' } });
  });

  it('does not guess a resume target with multiple suspended children', async () => {
    const { execute, invoke } = createNestedResumeFixture({ 'first-child': [1, 0], 'second-child': [1, 1] });

    const result = await execute();

    expect(invoke).not.toHaveBeenCalled();
    expect(result).toMatchObject({
      status: 'failed',
      error: expect.objectContaining({
        message:
          'Cannot infer nested resume step for nested-resume-workflow/nested-run: expected exactly one suspended step',
      }),
    });
  });

  it('uses the restored nested run id when resuming a foreach iteration', async () => {
    const resumeFixture = createNestedResumeFixture({ 'suspended-child-step': [1, 0] }, { foreachIndex: 2 });
    await resumeFixture.execute();
    expect(resumeFixture.loadWorkflowSnapshot).toHaveBeenCalledWith({
      workflowName: resumeFixture.nestedWorkflow.id,
      runId: resumeFixture.nestedRunId,
    });
    expect(resumeFixture.invoke.mock.calls[0]?.[1].data.runId).toBe(resumeFixture.nestedRunId);

    const freshFixture = createNestedResumeFixture({}, { mode: 'fresh', foreachIndex: 2 });
    await freshFixture.execute();
    expect(freshFixture.invoke).toHaveBeenCalledTimes(1);
    const foreachRunId = freshFixture.invoke.mock.calls[0]?.[1].data.runId;
    expect(foreachRunId).toEqual(expect.stringMatching(/^nested-/));
    expect(foreachRunId).not.toBe(freshFixture.parentRunId);
    await freshFixture.execute();
    expect(freshFixture.invoke.mock.calls[1]?.[1].data.runId).toBe(foreachRunId);
  });

  it('invokes a fresh nested run with a deterministic child run id', async () => {
    const fixture = createNestedResumeFixture({}, { mode: 'fresh' });
    const { execute, invoke, parentRunId } = fixture;

    const result = await execute();
    const freshRunId = invoke.mock.calls[0]?.[1].data.runId;

    expect(invoke).toHaveBeenCalledTimes(1);
    expect(freshRunId).toEqual(expect.stringMatching(/^nested-/));
    expect(freshRunId).not.toBe(parentRunId);
    expect(invoke.mock.calls[0]?.[1].data).toHaveProperty('initialState');
    expect(invoke.mock.calls[0]?.[1].data).not.toHaveProperty('resume');
    expect(result).toMatchObject({ status: 'success', output: { value: 'resumed' } });

    await execute();
    expect(invoke.mock.calls[1]?.[1].data.runId).toBe(freshRunId);
  });

  it('keeps fresh loop child runs distinct and resumes the recorded child run', async () => {
    const terminalRuns = new Map<string, string>();
    const executedRunIds: string[] = [];
    const invokeImpl = vi.fn(async (_id: string, options: any) => {
      const childRunId = options.data.runId as string;
      const inputValue = options.data.inputData.value as string;
      const terminalInput = terminalRuns.get(childRunId);
      if (terminalInput !== undefined) {
        if (terminalInput !== inputValue) {
          throw new Error(`Cannot start fresh nested workflow ${childRunId} after it reached terminal status`);
        }
        return {
          result: { status: 'success', result: { value: inputValue }, state: { value: inputValue } },
          runId: childRunId,
        };
      }

      terminalRuns.set(childRunId, inputValue);
      executedRunIds.push(childRunId);
      return {
        result: { status: 'success', result: { value: inputValue }, state: { value: inputValue } },
        runId: childRunId,
      };
    });
    const loopExecutionPath = [2, 0];
    const firstIteration = createNestedResumeFixture(
      {},
      {
        mode: 'fresh',
        executionPath: loopExecutionPath,
        inputValue: 'first',
        iterationCount: 1,
        invokeImpl,
      },
    );
    const secondIteration = createNestedResumeFixture(
      {},
      {
        mode: 'fresh',
        executionPath: loopExecutionPath,
        inputValue: 'second',
        iterationCount: 2,
        invokeImpl,
      },
    );

    await expect(firstIteration.execute()).resolves.toMatchObject({ status: 'success' });
    await expect(secondIteration.execute()).resolves.toMatchObject({ status: 'success' });

    const [firstRunId, secondRunId] = executedRunIds;
    expect(executedRunIds).toHaveLength(2);
    expect(firstRunId).toEqual(expect.stringMatching(/^nested-/));
    expect(secondRunId).toEqual(expect.stringMatching(/^nested-/));
    expect(firstRunId).not.toBe(secondRunId);
    expect([...terminalRuns.keys()]).toEqual(executedRunIds);

    const replay = createNestedResumeFixture(
      {},
      {
        mode: 'fresh',
        executionPath: loopExecutionPath,
        inputValue: 'second',
        iterationCount: 2,
        invokeImpl,
      },
    );
    await expect(replay.execute()).resolves.toMatchObject({ status: 'success' });
    expect(invokeImpl.mock.calls[2]?.[1].data.runId).toBe(secondRunId);
    expect(executedRunIds).toEqual([firstRunId, secondRunId]);

    const resumed = createNestedResumeFixture(
      { 'suspended-child-step': [1, 0] },
      {
        executionPath: loopExecutionPath,
        inputValue: 'second',
        iterationCount: 2,
        invokeImpl,
      },
    );
    await expect(resumed.execute()).resolves.toMatchObject({ status: 'success' });
    expect(resumed.loadWorkflowSnapshot).toHaveBeenCalledWith({
      workflowName: resumed.nestedWorkflow.id,
      runId: resumed.nestedRunId,
    });
    expect(invokeImpl.mock.calls[3]?.[1].data.runId).toBe(resumed.nestedRunId);
    expect(invokeImpl.mock.calls[3]?.[1].data.resume).toMatchObject({ runId: resumed.nestedRunId });
  });

  it('resumes a nested workflow suspended on a later loop iteration', async () => {
    const inngest = new Inngest({ id: 'native-loop-nested-resume-test' });
    const handlers = new Map<string, (context: any) => Promise<any>>();
    vi.spyOn(inngest, 'createFunction').mockImplementation(((config: any, handler: any) => {
      handlers.set(config.id, handler);
      return { id: config.id } as any;
    }) as any);
    vi.spyOn(inngest.realtime, 'publish').mockResolvedValue(undefined as any);

    const { createWorkflow, createStep } = init(inngest);
    const schema = z.object({ value: z.number() });
    const childAction = vi.fn(async ({ inputData, resumeData, suspend }) => {
      if (inputData.value === 1 && !resumeData) {
        await suspend({ reason: 'second iteration approval' });
        return inputData;
      }
      return resumeData ?? { value: inputData.value + 1 };
    });
    const childStep = createStep({
      id: 'loop-resume-child-step',
      inputSchema: schema,
      outputSchema: schema,
      suspendSchema: z.object({ reason: z.string() }),
      resumeSchema: schema,
      execute: childAction,
    });
    const child = createWorkflow({ id: 'loop-resume-child', inputSchema: schema, outputSchema: schema })
      .then(childStep)
      .commit();
    const parent = createWorkflow({ id: 'loop-resume-parent', inputSchema: schema, outputSchema: schema })
      .dountil(child as any, async ({ inputData }) => inputData.value >= 2)
      .commit();
    const storage = new MockStore();
    const mastra = new Mastra({ logger: false, storage, workflows: { parent } });
    parent.__registerMastra(mastra);
    parent.getFunction();

    const invocationPayloads: Array<{ functionId: string; data: any }> = [];
    const createStepContext = () => {
      const step = {
        run: async (id: string, callback: () => Promise<unknown>) => {
          return callback();
        },
        invoke: async (id: string, { function: fn, data }: any) => {
          const serializedData = JSON.parse(JSON.stringify(data));
          invocationPayloads.push({ functionId: fn.id, data: serializedData });
          const handler = handlers.get(fn.id);
          if (!handler) throw new Error(`Missing native handler for ${fn.id}`);
          const result = await handler({
            event: { data: structuredClone(serializedData) },
            step: createStepContext(),
            attempt: 0,
          });
          return result;
        },
        sleep: vi.fn(),
        sleepUntil: vi.fn(),
      };
      return step;
    };

    const parentHandler = handlers.get('workflow.loop-resume-parent');
    if (!parentHandler) throw new Error('Missing native parent workflow handler');
    const parentRunId = 'loop-resume-parent-run';
    const initial = await parentHandler({
      event: { data: { runId: parentRunId, inputData: { value: 0 } } },
      step: createStepContext(),
      attempt: 0,
    });
    expect(initial.result.status).toBe('suspended');
    expect(childAction).toHaveBeenCalledTimes(2);

    const workflowsStore = await storage.getStore('workflows');
    const suspendedSnapshot = await workflowsStore!.loadWorkflowSnapshot({
      workflowName: parent.id,
      runId: parentRunId,
    });

    const run = await parent.createRun({ runId: parentRunId });
    let resumedResult: any;
    vi.spyOn(inngest, 'send').mockImplementation(async (event: any) => {
      const data = structuredClone(event.data);
      const response = await parentHandler({ event: { data }, step: createStepContext(), attempt: 0 });
      resumedResult = response.result;
      return { ids: ['loop-resume-parent-event'] } as any;
    });
    vi.spyOn(run as any, 'getRunOutput').mockImplementation(async () => ({ output: { result: resumedResult } }));

    const resumed = await run.resume({ step: child.id, resumeData: { value: 2 } });
    expect(resumed).toMatchObject({ status: 'success', result: { value: 2 } });
    expect(childAction).toHaveBeenCalledTimes(3);
    expect(childAction.mock.calls[2]?.[0]).toMatchObject({ inputData: { value: 1 }, resumeData: { value: 2 } });
    expect(suspendedSnapshot?.context?.[child.id]).toMatchObject({
      status: 'suspended',
      metadata: { iterationCount: 2 },
    });
  });

  it('keeps a shared nested trigger distinct across native parallel graph paths', async () => {
    const inngest = new Inngest({ id: 'native-shared-nested-test' });
    const handlers = new Map<string, (context: any) => Promise<any>>();
    vi.spyOn(inngest, 'createFunction').mockImplementation(((config: any, handler: any) => {
      handlers.set(config.id, handler);
      return { id: config.id } as any;
    }) as any);
    vi.spyOn(inngest.realtime, 'publish').mockResolvedValue(undefined as any);

    const { createWorkflow, createStep } = init(inngest);
    const schema = z.object({ value: z.string() });
    const leaf = createStep({
      id: 'shared-leaf-step',
      inputSchema: schema,
      outputSchema: schema,
      execute: async ({ inputData }) => inputData,
    });
    const sharedLeaf = createWorkflow({
      id: 'shared-leaf-workflow',
      inputSchema: schema,
      outputSchema: schema,
    })
      .then(leaf)
      .commit();
    const branchA = createWorkflow({ id: 'branch-a', inputSchema: schema, outputSchema: schema })
      .then(sharedLeaf)
      .commit();
    const branchB = createWorkflow({ id: 'branch-b', inputSchema: schema, outputSchema: schema })
      .then(sharedLeaf)
      .commit();
    const parent = createWorkflow({
      id: 'shared-parent',
      inputSchema: schema,
      outputSchema: z.object({ branchA: schema, branchB: schema }),
    })
      .parallel([branchA, branchB])
      .commit();
    const mastra = new Mastra({ logger: false, storage: new MockStore(), workflows: { parent } });
    parent.__registerMastra(mastra);
    parent.getFunction();

    const invocationPayloads: Array<{ functionId: string; data: any }> = [];
    const invokeResults = new Map<string, any>();
    const step = {
      run: async (id: string, callback: () => Promise<unknown>) => {
        if (invokeResults.has(`run:${id}`)) return invokeResults.get(`run:${id}`);
        const result = await callback();
        invokeResults.set(`run:${id}`, structuredClone(result));
        return result;
      },
      invoke: async (id: string, { function: fn, data }: any) => {
        const serializedData = JSON.parse(JSON.stringify(data));
        invocationPayloads.push({ functionId: fn.id, data: serializedData });
        if (invokeResults.has(`invoke:${id}`)) return invokeResults.get(`invoke:${id}`);
        const handler = handlers.get(fn.id);
        if (!handler) throw new Error(`Missing native handler for ${fn.id}`);
        const result = await handler({ event: { data: serializedData }, step, attempt: 0 });
        invokeResults.set(`invoke:${id}`, structuredClone(result));
        return result;
      },
      sleep: vi.fn(),
      sleepUntil: vi.fn(),
    };
    const parentHandler = handlers.get('workflow.shared-parent');
    if (!parentHandler) throw new Error('Missing native parent workflow handler');
    const invocation = {
      event: { data: { runId: 'shared-parent-run', inputData: { value: 'ok' } } },
      step,
      attempt: 0,
    };

    const first = await parentHandler(invocation);
    expect(first.result.status).toBe('success');
    const firstInvocationPayloads = structuredClone(invocationPayloads);
    const replay = await parentHandler(invocation);
    expect(replay.result.status).toBe('success');
    expect(invocationPayloads).toEqual(firstInvocationPayloads);

    const branchPayloads = invocationPayloads.filter(({ functionId }) =>
      ['workflow.branch-a', 'workflow.branch-b'].includes(functionId),
    );
    const sharedPayloads = invocationPayloads.filter(
      ({ functionId }) => functionId === 'workflow.shared-leaf-workflow',
    );
    const firstBranchPayloads = branchPayloads.slice(0, 2);
    const branchRunIds = firstBranchPayloads.map(({ data }) => data.runId as string);
    const sharedRunIds = sharedPayloads.map(({ data }) => data.runId as string);

    expect(firstBranchPayloads).toHaveLength(2);
    expect(new Set(branchRunIds).size).toBe(2);
    expect(branchRunIds[0]).toEqual(expect.stringMatching(/^nested-/));
    expect(branchRunIds[1]).toEqual(expect.stringMatching(/^nested-/));
    expect(sharedPayloads).toHaveLength(2);
    expect(new Set(sharedRunIds).size).toBe(2);
    expect(sharedRunIds[0]).toEqual(expect.stringMatching(/^nested-/));
    expect(sharedRunIds[1]).toEqual(expect.stringMatching(/^nested-/));

    const workflowsStore = await mastra.getStorage()!.getStore('workflows');
    for (const runId of sharedRunIds) {
      const snapshot = await workflowsStore!.loadWorkflowSnapshot({
        workflowName: sharedLeaf.id,
        runId,
      });
      expect(snapshot).toMatchObject({
        runId,
        status: 'success',
        result: { value: 'ok' },
        context: { [leaf.id]: { status: 'success', output: { value: 'ok' } } },
      });
    }
  });

  it('time travels a completed nested workflow under a fresh parent generation', async () => {
    const inngest = new Inngest({ id: 'native-time-travel-completed-child-test' });
    const handlers = new Map<string, (context: any) => Promise<any>>();
    vi.spyOn(inngest, 'createFunction').mockImplementation(((config: any, handler: any) => {
      handlers.set(config.id, handler);
      return { id: config.id } as any;
    }) as any);
    vi.spyOn(inngest.realtime, 'publish').mockResolvedValue(undefined as any);

    const { createWorkflow, createStep } = init(inngest);
    const schema = z.object({ value: z.string() });
    const childStep = createStep({
      id: 'completed-child-step',
      inputSchema: schema,
      outputSchema: schema,
      execute: async ({ inputData }) => inputData,
    });
    const child = createWorkflow({ id: 'completed-child', inputSchema: schema, outputSchema: schema })
      .then(childStep)
      .commit();
    const parent = createWorkflow({ id: 'completed-parent', inputSchema: schema, outputSchema: schema })
      .then(child)
      .commit();
    const storage = new MockStore();
    const mastra = new Mastra({ logger: false, storage, workflows: { parent } });
    parent.__registerMastra(mastra);
    parent.getFunction();

    const invocationPayloads: Array<{ functionId: string; data: any }> = [];
    const createStepContext = () => {
      const memo = new Map<string, any>();
      const step = {
        run: async (id: string, callback: () => Promise<unknown>) => {
          const key = `run:${id}`;
          if (memo.has(key)) return memo.get(key);
          const result = await callback();
          memo.set(key, structuredClone(result));
          return result;
        },
        invoke: async (id: string, { function: fn, data }: any) => {
          const key = `invoke:${id}`;
          if (memo.has(key)) return memo.get(key);
          const serializedData = JSON.parse(JSON.stringify(data));
          invocationPayloads.push({ functionId: fn.id, data: serializedData });
          const handler = handlers.get(fn.id);
          if (!handler) throw new Error(`Missing native handler for ${fn.id}`);
          const result = await handler({ event: { data: serializedData }, step, attempt: 0 });
          memo.set(key, structuredClone(result));
          return result;
        },
        sleep: vi.fn(),
        sleepUntil: vi.fn(),
      };
      return step;
    };

    const parentHandler = handlers.get('workflow.completed-parent');
    if (!parentHandler) throw new Error('Missing native parent workflow handler');
    const parentRunId = 'completed-parent-run';
    const initialStep = createStepContext();
    const initial = await parentHandler({
      event: { data: { runId: parentRunId, inputData: { value: 'before' } } },
      step: initialStep,
      attempt: 0,
    });
    expect(initial.result.status).toBe('success');

    const workflowsStore = await storage.getStore('workflows');
    const originalParentSnapshot = await workflowsStore!.loadWorkflowSnapshot({
      workflowName: parent.id,
      runId: parentRunId,
    });
    const originalChildPayload = invocationPayloads.find(({ functionId }) => functionId === 'workflow.completed-child');
    expect(originalChildPayload).toBeDefined();
    const originalChildRunId = originalChildPayload!.data.runId as string;
    const originalChildSnapshot = await workflowsStore!.loadWorkflowSnapshot({
      workflowName: child.id,
      runId: originalChildRunId,
    });
    expect(originalChildSnapshot).toMatchObject({ status: 'success', result: { value: 'before' } });

    const run = await parent.createRun({ runId: parentRunId });
    let timeTravelResult: any;
    let timeTravelEventData: any;
    let timeTravelStep: any;
    vi.spyOn(inngest, 'send').mockImplementation(async (event: any) => {
      const data = JSON.parse(JSON.stringify(event.data));
      timeTravelEventData = data;
      timeTravelStep = createStepContext();
      const response = await parentHandler({ event: { data }, step: timeTravelStep, attempt: 0 });
      timeTravelResult = response.result;
      return { ids: ['completed-parent-time-travel-event'] } as any;
    });
    vi.spyOn(run as any, 'getRunOutput').mockImplementation(async () => ({ output: { result: timeTravelResult } }));

    const travelled = await run.timeTravel({ step: child.id, inputData: { value: 'after' } });
    expect(travelled).toMatchObject({ status: 'success', result: { value: 'after' } });

    const timeTravelChildPayloads = invocationPayloads.filter(
      ({ functionId }) => functionId === 'workflow.completed-child',
    );
    expect(timeTravelChildPayloads).toHaveLength(2);
    const timeTravelChildRunId = timeTravelChildPayloads[1]!.data.runId as string;
    expect(timeTravelChildRunId).toEqual(expect.stringMatching(/^nested-/));
    expect(timeTravelChildRunId).not.toBe(originalChildRunId);
    expect(timeTravelEventData.executionGeneration).toBeDefined();
    expect(timeTravelEventData.executionGeneration).not.toBe(originalParentSnapshot?.executionGeneration);

    const preservedChildSnapshot = await workflowsStore!.loadWorkflowSnapshot({
      workflowName: child.id,
      runId: originalChildRunId,
    });
    const freshChildSnapshot = await workflowsStore!.loadWorkflowSnapshot({
      workflowName: child.id,
      runId: timeTravelChildRunId,
    });
    expect(preservedChildSnapshot).toMatchObject({ status: 'success', result: { value: 'before' } });
    expect(freshChildSnapshot).toMatchObject({ status: 'success', result: { value: 'after' } });

    const replayBefore = structuredClone(invocationPayloads);
    const replayedChild = await handlers.get('workflow.completed-child')?.({
      event: { data: timeTravelChildPayloads[1]!.data },
      step: timeTravelStep,
      attempt: 0,
    });
    expect(replayedChild?.result.status).toBe('success');
    expect(invocationPayloads).toEqual(replayBefore);
  });

  it('time travels a completed nested workflow to a later leaf using the original child snapshot', async () => {
    const inngest = new Inngest({ id: 'native-time-travel-completed-child-later-step-test' });
    const handlers = new Map<string, (context: any) => Promise<any>>();
    vi.spyOn(inngest, 'createFunction').mockImplementation(((config: any, handler: any) => {
      handlers.set(config.id, handler);
      return { id: config.id } as any;
    }) as any);
    vi.spyOn(inngest.realtime, 'publish').mockResolvedValue(undefined as any);

    const { createWorkflow, createStep } = init(inngest);
    const schema = z.object({ value: z.string() });
    const firstStep = createStep({
      id: 'completed-child-first-step',
      inputSchema: schema,
      outputSchema: schema,
      execute: async ({ inputData }) => inputData,
    });
    const laterStep = createStep({
      id: 'completed-child-later-step',
      inputSchema: schema,
      outputSchema: schema,
      execute: async ({ inputData }) => ({ value: `${inputData.value}:later` }),
    });
    const child = createWorkflow({ id: 'completed-child-later', inputSchema: schema, outputSchema: schema })
      .then(firstStep)
      .then(laterStep)
      .commit();
    const parent = createWorkflow({ id: 'completed-parent-later', inputSchema: schema, outputSchema: schema })
      .then(child)
      .commit();
    const storage = new MockStore();
    const mastra = new Mastra({ logger: false, storage, workflows: { parent } });
    parent.__registerMastra(mastra);
    parent.getFunction();

    const invocationPayloads: Array<{ functionId: string; data: any }> = [];
    const createStepContext = () => {
      const memo = new Map<string, any>();
      const step = {
        run: async (id: string, callback: () => Promise<unknown>) => {
          const key = `run:${id}`;
          if (memo.has(key)) return memo.get(key);
          const result = await callback();
          memo.set(key, structuredClone(result));
          return result;
        },
        invoke: async (id: string, { function: fn, data }: any) => {
          const key = `invoke:${id}`;
          if (memo.has(key)) return memo.get(key);
          const serializedData = JSON.parse(JSON.stringify(data));
          invocationPayloads.push({ functionId: fn.id, data: serializedData });
          const handler = handlers.get(fn.id);
          if (!handler) throw new Error(`Missing native handler for ${fn.id}`);
          const result = await handler({ event: { data: structuredClone(serializedData) }, step, attempt: 0 });
          memo.set(key, structuredClone(result));
          return result;
        },
        sleep: vi.fn(),
        sleepUntil: vi.fn(),
      };
      return step;
    };

    const parentHandler = handlers.get('workflow.completed-parent-later');
    if (!parentHandler) throw new Error('Missing native parent workflow handler');
    const parentRunId = 'completed-parent-later-run';
    const initial = await parentHandler({
      event: { data: { runId: parentRunId, inputData: { value: 'before' } } },
      step: createStepContext(),
      attempt: 0,
    });
    expect(initial.result.status).toBe('success');
    expect(initial.result.result).toEqual({ value: 'before:later' });

    const workflowsStore = await storage.getStore('workflows');
    const originalParentSnapshot = await workflowsStore!.loadWorkflowSnapshot({
      workflowName: parent.id,
      runId: parentRunId,
    });
    const originalChildPayload = invocationPayloads.find(
      ({ functionId }) => functionId === 'workflow.completed-child-later',
    );
    expect(originalChildPayload).toBeDefined();
    const originalChildRunId = originalChildPayload!.data.runId as string;
    expect(originalParentSnapshot?.context?.[child.id]).toMatchObject({
      status: 'success',
      metadata: { nestedRunId: originalChildRunId },
    });
    const originalChildSnapshot = await workflowsStore!.loadWorkflowSnapshot({
      workflowName: child.id,
      runId: originalChildRunId,
    });
    expect(originalChildSnapshot).toMatchObject({
      status: 'success',
      result: { value: 'before:later' },
      context: {
        [firstStep.id]: { status: 'success', output: { value: 'before' } },
        [laterStep.id]: { status: 'success', output: { value: 'before:later' } },
      },
    });

    const run = await parent.createRun({ runId: parentRunId });
    let timeTravelResult: any;
    let timeTravelEventData: any;
    let timeTravelStep: any;
    vi.spyOn(inngest, 'send').mockImplementation(async (event: any) => {
      const data = JSON.parse(JSON.stringify(event.data));
      timeTravelEventData = data;
      timeTravelStep = createStepContext();
      const response = await parentHandler({ event: { data }, step: timeTravelStep, attempt: 0 });
      timeTravelResult = response.result;
      return { ids: ['completed-parent-later-time-travel-event'] } as any;
    });
    vi.spyOn(run as any, 'getRunOutput').mockImplementation(async () => ({ output: { result: timeTravelResult } }));

    const travelled = await run.timeTravel({ step: [child.id, laterStep.id] });
    expect(travelled).toMatchObject({ status: 'success', result: { value: 'before:later' } });
    expect((travelled.steps as any)?.[child.id]?.metadata?.nestedRunId).toBeUndefined();

    const timeTravelChildPayloads = invocationPayloads.filter(
      ({ functionId }) => functionId === 'workflow.completed-child-later',
    );
    expect(timeTravelChildPayloads).toHaveLength(2);
    const timeTravelChildRunId = timeTravelChildPayloads[1]!.data.runId as string;
    expect(timeTravelChildRunId).toEqual(expect.stringMatching(/^nested-/));
    expect(timeTravelChildRunId).not.toBe(originalChildRunId);
    expect(timeTravelEventData.executionGeneration).toBeDefined();
    expect(timeTravelEventData.executionGeneration).not.toBe(originalParentSnapshot?.executionGeneration);
    expect(timeTravelChildPayloads[1]!.data.timeTravel.stepResults[firstStep.id]).toMatchObject({
      status: 'success',
      output: { value: 'before' },
    });

    const preservedChildSnapshot = await workflowsStore!.loadWorkflowSnapshot({
      workflowName: child.id,
      runId: originalChildRunId,
    });
    const freshChildSnapshot = await workflowsStore!.loadWorkflowSnapshot({
      workflowName: child.id,
      runId: timeTravelChildRunId,
    });
    expect(preservedChildSnapshot).toMatchObject({
      status: 'success',
      result: { value: 'before:later' },
      context: { [laterStep.id]: { output: { value: 'before:later' } } },
    });
    expect(freshChildSnapshot).toMatchObject({
      status: 'success',
      result: { value: 'before:later' },
      context: {
        [firstStep.id]: { status: 'success', output: { value: 'before' } },
        [laterStep.id]: { status: 'success', output: { value: 'before:later' } },
      },
    });

    const replayBefore = structuredClone(invocationPayloads);
    const replayedChild = await handlers.get('workflow.completed-child-later')?.({
      event: { data: structuredClone(timeTravelChildPayloads[1]!.data) },
      step: timeTravelStep,
      attempt: 0,
    });
    expect(replayedChild?.result).toMatchObject({ status: 'success', result: { value: 'before:later' } });
    expect(invocationPayloads).toEqual(replayBefore);
  });

  it('time travels a suspended nested leaf under a fresh child run identity', async () => {
    const inngest = new Inngest({ id: 'native-time-travel-suspended-child-test' });
    const handlers = new Map<string, (context: any) => Promise<any>>();
    vi.spyOn(inngest, 'createFunction').mockImplementation(((config: any, handler: any) => {
      handlers.set(config.id, handler);
      return { id: config.id } as any;
    }) as any);
    vi.spyOn(inngest.realtime, 'publish').mockResolvedValue(undefined as any);

    const { createWorkflow, createStep } = init(inngest);
    const schema = z.object({ value: z.string() });
    const leaf = createStep({
      id: 'suspended-leaf-step',
      inputSchema: schema,
      outputSchema: schema,
      suspendSchema: z.object({ reason: z.string() }),
      resumeSchema: schema,
      execute: async ({ inputData, resumeData, suspend }) => {
        if (!resumeData) {
          await suspend({ reason: 'waiting for approval' });
          return inputData;
        }
        return resumeData;
      },
    });
    const child = createWorkflow({ id: 'suspended-child', inputSchema: schema, outputSchema: schema })
      .then(leaf)
      .commit();
    const parent = createWorkflow({ id: 'suspended-parent', inputSchema: schema, outputSchema: schema })
      .then(child)
      .commit();
    const storage = new MockStore();
    const mastra = new Mastra({ logger: false, storage, workflows: { parent } });
    parent.__registerMastra(mastra);
    parent.getFunction();

    const invocationPayloads: Array<{ functionId: string; data: any }> = [];
    const createStepContext = () => {
      const memo = new Map<string, any>();
      const step = {
        run: async (id: string, callback: () => Promise<unknown>) => {
          const key = `run:${id}`;
          if (memo.has(key)) return memo.get(key);
          const result = await callback();
          memo.set(key, structuredClone(result));
          return result;
        },
        invoke: async (id: string, { function: fn, data }: any) => {
          const key = `invoke:${id}`;
          if (memo.has(key)) return memo.get(key);
          const serializedData = JSON.parse(JSON.stringify(data));
          invocationPayloads.push({ functionId: fn.id, data: serializedData });
          const handler = handlers.get(fn.id);
          if (!handler) throw new Error(`Missing native handler for ${fn.id}`);
          const result = await handler({ event: { data: serializedData }, step, attempt: 0 });
          memo.set(key, structuredClone(result));
          return result;
        },
        sleep: vi.fn(),
        sleepUntil: vi.fn(),
      };
      return step;
    };

    const parentHandler = handlers.get('workflow.suspended-parent');
    if (!parentHandler) throw new Error('Missing native parent workflow handler');
    const parentRunId = 'suspended-parent-run';
    const initialStep = createStepContext();
    const initial = await parentHandler({
      event: { data: { runId: parentRunId, inputData: { value: 'before' } } },
      step: initialStep,
      attempt: 0,
    });
    expect(initial.result.status).toBe('suspended');

    const workflowsStore = await storage.getStore('workflows');
    const loadWorkflowSnapshot = vi.spyOn(workflowsStore!, 'loadWorkflowSnapshot');
    const originalParentSnapshot = await workflowsStore!.loadWorkflowSnapshot({
      workflowName: parent.id,
      runId: parentRunId,
    });
    const originalChildPayload = invocationPayloads.find(({ functionId }) => functionId === 'workflow.suspended-child');
    expect(originalChildPayload).toBeDefined();
    const originalChildRunId = originalChildPayload!.data.runId as string;
    const originalChildSnapshot = await workflowsStore!.loadWorkflowSnapshot({
      workflowName: child.id,
      runId: originalChildRunId,
    });
    expect(originalChildSnapshot).toMatchObject({
      status: 'suspended',
      context: { [leaf.id]: { status: 'suspended' } },
    });
    loadWorkflowSnapshot.mockClear();

    const run = await parent.createRun({ runId: parentRunId });
    let timeTravelResult: any;
    let timeTravelEventData: any;
    vi.spyOn(inngest, 'send').mockImplementation(async (event: any) => {
      const data = JSON.parse(JSON.stringify(event.data));
      timeTravelEventData = data;
      const response = await parentHandler({ event: { data }, step: createStepContext(), attempt: 0 });
      timeTravelResult = response.result;
      return { ids: ['suspended-parent-time-travel-event'] } as any;
    });
    vi.spyOn(run as any, 'getRunOutput').mockImplementation(async () => ({ output: { result: timeTravelResult } }));

    const travelled = await run.timeTravel({
      step: [child.id, leaf.id],
      resumeData: { value: 'after' },
    });
    expect(travelled).toMatchObject({ status: 'success', result: { value: 'after' } });

    const timeTravelChildPayloads = invocationPayloads.filter(
      ({ functionId }) => functionId === 'workflow.suspended-child',
    );
    expect(timeTravelChildPayloads).toHaveLength(2);
    const timeTravelChildRunId = timeTravelChildPayloads[1]!.data.runId as string;
    expect(timeTravelChildRunId).toEqual(expect.stringMatching(/^nested-/));
    expect(timeTravelChildRunId).not.toBe(originalChildRunId);
    expect(timeTravelEventData.executionGeneration).toBeDefined();
    expect(timeTravelEventData.executionGeneration).not.toBe(originalParentSnapshot?.executionGeneration);
    expect(timeTravelEventData.timeTravel.steps).toEqual([child.id, leaf.id]);

    expect(
      loadWorkflowSnapshot.mock.calls.some(
        ([request]) => request.workflowName === child.id && request.runId === originalChildRunId,
      ),
    ).toBe(true);
    const preservedChildSnapshot = await workflowsStore!.loadWorkflowSnapshot({
      workflowName: child.id,
      runId: originalChildRunId,
    });
    const freshChildSnapshot = await workflowsStore!.loadWorkflowSnapshot({
      workflowName: child.id,
      runId: timeTravelChildRunId,
    });
    expect(preservedChildSnapshot).toMatchObject({
      status: 'suspended',
      context: { [leaf.id]: { status: 'suspended' } },
    });
    expect(freshChildSnapshot).toMatchObject({
      status: 'success',
      result: { value: 'after' },
      context: { [leaf.id]: { status: 'success', output: { value: 'after' } } },
    });
  });

  it.each([
    { name: 'restored', timeTravelRunId: 'restored-time-travel-run' },
    { name: 'fresh', timeTravelRunId: undefined },
  ])('uses the $name nested run id for time travel source lookup and a fresh trigger', async ({ timeTravelRunId }) => {
    const executionGeneration = 'time-travel-parent-generation';
    const freshFixture = createNestedResumeFixture({}, { mode: 'fresh', executionGeneration });
    await freshFixture.execute();
    const freshRunId = freshFixture.invoke.mock.calls[0]?.[1].data.runId;
    const fixture = createNestedResumeFixture(
      {},
      { mode: 'fresh', timeTravel: true, timeTravelRunId, executionGeneration },
    );

    await fixture.execute();

    const timeTravelSourceRunIdUsed = fixture.loadWorkflowSnapshot.mock.calls[0]?.[0].runId;
    const timeTravelExecutionRunId = fixture.invoke.mock.calls[0]?.[1].data.runId;
    expect(timeTravelSourceRunIdUsed).toBe(timeTravelRunId ?? freshRunId);
    expect(timeTravelExecutionRunId).toEqual(expect.any(String));
    expect(fixture.loadWorkflowSnapshot).toHaveBeenCalledWith({
      workflowName: fixture.nestedWorkflow.id,
      runId: timeTravelRunId ?? freshRunId,
    });
    expect(timeTravelExecutionRunId).toBe(freshRunId);
    if (timeTravelRunId !== undefined) {
      expect(timeTravelExecutionRunId).not.toBe(timeTravelRunId);
    }
  });

  it('logs the underlying error before flattening it into a failed result', async () => {
    const fixture = createNestedResumeFixture(
      { 'suspended-child-step': [1, 0] },
      {
        mode: 'fresh',
        withLoggerSpy: true,
        invokeImpl: async () => {
          throw new Error('child blew up');
        },
      },
    );
    const { execute, logger } = fixture;

    const result = await execute();

    expect(result).toMatchObject({ status: 'failed' });
    expect(logger.error).toHaveBeenCalledTimes(1);
    expect(logger.error.mock.calls[0]?.[0]).toContain('child blew up');
  });
});
