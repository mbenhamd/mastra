import { describe, expect, it, vi } from 'vitest';
import { createRestartExecutionParams } from '../../utils';
import { processWorkflowConditional, processWorkflowParallel } from './parallel';

function makeParallelStep(ids: string[]) {
  return {
    type: 'parallel' as const,
    steps: ids.map(id => ({ type: 'step' as const, step: { id } })),
  };
}

function makeArgs(overrides: Record<string, any> = {}) {
  return {
    workflowId: 'workflow',
    runId: 'run-1',
    executionGeneration: 'parallel-execution-generation',
    lifecycleResumeAttempt: 0,
    lifecycleStepStates: {},
    executionPath: [0, 0],
    stepResults: {},
    activeStepsPath: {},
    resumeSteps: [],
    prevResult: { status: 'success', output: {} },
    requestContext: {},
    state: {},
    ...overrides,
  } as any;
}

function recorder() {
  const published: any[] = [];
  return {
    published,
    pubsub: {
      publish: async (_topic: string, event: any) => {
        published.push(event);
      },
    } as any,
  };
}

function runPaths(published: any[]) {
  return published.filter(event => event.type === 'workflow.step.run').map(event => event.data.executionPath);
}

describe('processWorkflowParallel restart branch routing', () => {
  it.each([
    ['only C', { C: [0, 2] }, [[0, 2]]],
    [
      'B and C',
      { B: [0, 1], C: [0, 2] },
      [
        [0, 1],
        [0, 2],
      ],
    ],
    [
      'non-prefix A and C',
      { A: [0, 0], C: [0, 2] },
      [
        [0, 0],
        [0, 2],
      ],
    ],
  ] as const)('restarts %s at original graph coordinates', async (_label, activeStepsPath, expected) => {
    const { published, pubsub } = recorder();
    const step = makeParallelStep(['A', 'B', 'C']);

    await processWorkflowParallel(
      makeArgs({
        executionPath: [0, 2],
        restart: { activeStepsPath, isParallelOrConditionalRestarted: false },
      }),
      { pubsub, step },
    );

    expect(runPaths(published)).toEqual(expected);
    expect(
      published.map(event => {
        const branchIndex = event.data.executionPath.at(-1) as number;
        return step.steps[branchIndex]!.step.id;
      }),
    ).toEqual(Object.keys(activeStepsPath));
    expect(published.every(event => event.data.restart?.isParallelOrConditionalRestarted === true)).toBe(true);
  });

  it('accepts the existing stored activity-path shape and derives branch coordinates independently', async () => {
    const { published, pubsub } = recorder();

    await processWorkflowParallel(
      makeArgs({
        executionPath: [0, 0],
        restart: {
          activeStepsPath: { step1: [0], step2: [1] },
          isParallelOrConditionalRestarted: false,
        },
      }),
      { pubsub, step: makeParallelStep(['step1', 'step2']) },
    );

    expect(runPaths(published)).toEqual([
      [0, 0],
      [0, 1],
    ]);
  });

  it('preserves magic-key IDs from the pending nested producer through parallel restart routing', async () => {
    const { published, pubsub } = recorder();
    const step = makeParallelStep(['__proto__', 'constructor', 'ordinary']);
    const restart = createRestartExecutionParams({
      snapshot: {
        runId: 'pending-nested',
        status: 'pending',
        context: { input: { status: 'success', payload: {} } },
        activePaths: [],
        activeStepsPath: {},
        serializedStepGraph: [],
        suspendedPaths: {},
        resumeLabels: {},
        value: {},
        waitingPaths: {},
        timestamp: 1,
      } as any,
      graph: { steps: [step] } as any,
    });

    expect(restart.activePaths).toEqual([0]);
    expect(Object.keys(restart.activeStepsPath)).toEqual(['__proto__', 'constructor', 'ordinary']);
    expect(Object.prototype.hasOwnProperty.call(restart.activeStepsPath, '__proto__')).toBe(true);
    expect(Object.prototype.hasOwnProperty.call(restart.activeStepsPath, 'constructor')).toBe(true);
    expect(restart.activeStepsPath['__proto__']).toEqual([0]);
    expect(restart.activeStepsPath.constructor).toEqual([0]);
    expect(restart.activeStepsPath.ordinary).toEqual([0]);
    await processWorkflowParallel(makeArgs({ executionPath: restart.activePaths, restart }), { pubsub, step });

    expect(runPaths(published)).toEqual([
      [0, 0],
      [0, 1],
      [0, 2],
    ]);
  });

  it('preserves the enclosing execution prefix while replacing only the branch index', async () => {
    const { published, pubsub } = recorder();

    await processWorkflowParallel(
      makeArgs({
        executionPath: [4, 7, 0],
        restart: { activeStepsPath: { C: [4, 7, 2] }, isParallelOrConditionalRestarted: false },
      }),
      { pubsub, step: makeParallelStep(['A', 'B', 'C']) },
    );

    expect(runPaths(published)).toEqual([[4, 7, 2]]);
  });

  it.each([Object.create({ C: [0, 2] }), new Date()])(
    'rejects a custom-prototype activity record',
    async activeStepsPath => {
      const { published, pubsub } = recorder();

      await expect(
        processWorkflowParallel(makeArgs({ restart: { activeStepsPath, isParallelOrConditionalRestarted: false } }), {
          pubsub,
          step: makeParallelStep(['A', 'B', 'C']),
        }),
      ).rejects.toThrow('Invalid parallel restart state');

      expect(published).toEqual([]);
    },
  );

  it('rejects accessor and unknown own keys before publishing', async () => {
    const getter = vi.fn(() => [0, 2]);
    const accessorPath = {};
    Object.defineProperty(accessorPath, 'C', { enumerable: true, get: getter });

    for (const activeStepsPath of [accessorPath, { unknown: [0] }]) {
      const { published, pubsub } = recorder();
      await expect(
        processWorkflowParallel(makeArgs({ restart: { activeStepsPath, isParallelOrConditionalRestarted: false } }), {
          pubsub,
          step: makeParallelStep(['A', 'B', 'C']),
        }),
      ).rejects.toThrow('Invalid parallel restart state');
      expect(published).toEqual([]);
    }

    expect(getter).not.toHaveBeenCalled();
  });

  it('rejects proxy maps and paths before invoking reflection traps', async () => {
    const trap = vi.fn(() => {
      throw new Error('proxy trap must not run');
    });
    const handler = {
      get: trap,
      getOwnPropertyDescriptor: trap,
      getPrototypeOf: trap,
      ownKeys: trap,
    } as ProxyHandler<Record<string, unknown>>;

    const proxyMap = new Proxy({ C: [0, 2] }, handler);
    const proxyPath = new Proxy([0, 2], handler as ProxyHandler<number[]>);
    const revoked = Proxy.revocable({ C: [0, 2] }, {});
    revoked.revoke();

    for (const activeStepsPath of [proxyMap, { C: proxyPath }, revoked.proxy]) {
      const { published, pubsub } = recorder();
      await expect(
        processWorkflowParallel(makeArgs({ restart: { activeStepsPath, isParallelOrConditionalRestarted: false } }), {
          pubsub,
          step: makeParallelStep(['A', 'B', 'C']),
        }),
      ).rejects.toThrow('Invalid parallel restart state');
      expect(published).toEqual([]);
    }

    expect(trap).not.toHaveBeenCalled();
  });

  it('rejects accessor path indices without invoking them', async () => {
    const getter = vi.fn(() => 2);
    const path: number[] = [];
    Object.defineProperty(path, '0', { enumerable: true, get: getter });
    path.length = 1;
    const { published, pubsub } = recorder();

    await expect(
      processWorkflowParallel(
        makeArgs({ restart: { activeStepsPath: { C: path }, isParallelOrConditionalRestarted: false } }),
        { pubsub, step: makeParallelStep(['A', 'B', 'C']) },
      ),
    ).rejects.toThrow('Invalid parallel restart state');

    expect(getter).not.toHaveBeenCalled();
    expect(published).toEqual([]);
  });

  it.each([
    ['non-array', true],
    ['wrong prefix', [9, 2]],
    ["another branch's stored path", [1]],
    ['wrong full branch index', [0, 1]],
    ['sparse path', Object.assign(new Array(2), { 0: 0 })],
    ['overlong path', [0, 2, 0]],
    ['unsafe integer path', [0, Number.MAX_SAFE_INTEGER + 1]],
  ])('rejects a %s before publishing', async (_label, path) => {
    const { published, pubsub } = recorder();

    await expect(
      processWorkflowParallel(
        makeArgs({ restart: { activeStepsPath: { C: path }, isParallelOrConditionalRestarted: false } }),
        { pubsub, step: makeParallelStep(['A', 'B', 'C']) },
      ),
    ).rejects.toThrow('Invalid parallel restart state');
    expect(published).toEqual([]);
  });

  it('preserves ordinary fan-out and per-step behavior', async () => {
    const step = makeParallelStep(['A', 'B', 'C']);
    const ordinary = recorder();
    await processWorkflowParallel(makeArgs({ executionPath: [0] }), { pubsub: ordinary.pubsub, step });
    expect(runPaths(ordinary.published)).toEqual([
      [0, 0],
      [0, 1],
      [0, 2],
    ]);

    const perStep = recorder();
    await processWorkflowParallel(makeArgs({ executionPath: [0], perStep: true }), { pubsub: perStep.pubsub, step });
    expect(runPaths(perStep.published)).toEqual([[0, 0]]);
  });

  it('persists every branch attempt together before publishing the fan-out', async () => {
    const { published, pubsub } = recorder();
    const updateWorkflowState = vi.fn(async () => undefined);

    await processWorkflowParallel(makeArgs({ executionPath: [0] }), {
      pubsub,
      workflowsStore: { updateWorkflowState } as any,
      step: makeParallelStep(['A', 'B']),
    });

    expect(updateWorkflowState).toHaveBeenCalledTimes(1);
    const persistedStates = updateWorkflowState.mock.calls[0]![0].opts.lifecycleStepStates;
    expect(Object.values(persistedStates)).toHaveLength(2);
    expect(Object.values(persistedStates)).toEqual([
      expect.objectContaining({ stepAttempt: 1 }),
      expect.objectContaining({ stepAttempt: 1 }),
    ]);
    expect(new Set(Object.values(persistedStates).map((state: any) => state.stepCallId)).size).toBe(2);

    expect(published).toHaveLength(2);
    for (const event of published) {
      expect(event.data.lifecycleStepAttemptReserved).toBe(true);
      expect(event.data.lifecycleStepStates).toEqual(persistedStates);
    }
  });

  it('carries lifecycle lineage on a skipped conditional branch without storage', async () => {
    const { published, pubsub } = recorder();
    const step = {
      type: 'conditional' as const,
      steps: makeParallelStep(['selected', 'skipped']).steps,
      conditions: [],
    } as any;

    await processWorkflowConditional(makeArgs({ executionPath: [0] }), {
      pubsub,
      step,
      stepExecutor: { evaluateConditions: vi.fn(async () => [0]) } as any,
    });

    const skipped = published.find(event => event.type === 'workflow.step.end');
    expect(skipped.data).toMatchObject({
      executionGeneration: 'parallel-execution-generation',
      lifecycleResumeAttempt: 0,
      lifecycleStepStates: {},
      prevResult: { status: 'skipped' },
    });
  });
});
