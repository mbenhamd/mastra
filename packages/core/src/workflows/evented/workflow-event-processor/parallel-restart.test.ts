import { describe, expect, it, vi } from 'vitest';
import { createRestartExecutionParams } from '../../utils';
import { processWorkflowParallel } from './parallel';

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

    await processWorkflowParallel(
      makeArgs({
        executionPath: [0, 2],
        restart: { activeStepsPath, isParallelOrConditionalRestarted: false },
      }),
      { pubsub, step: makeParallelStep(['A', 'B', 'C']) },
    );

    expect(runPaths(published)).toEqual(expected);
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

  it('appends real branch indices to a pending nested parallel container path', async () => {
    const { published, pubsub } = recorder();
    const step = makeParallelStep(['A', 'B', 'C']);
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
    expect(restart.activeStepsPath).toEqual({ A: [0], B: [0], C: [0] });
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

  it('ignores unknown and inherited activity keys without invoking accessors', async () => {
    const { published, pubsub } = recorder();
    const getter = vi.fn(() => [0, 2]);
    const activeStepsPath = { unknown: [9] };
    Object.defineProperty(activeStepsPath, 'C', { enumerable: true, get: getter });

    await processWorkflowParallel(makeArgs({ restart: { activeStepsPath, isParallelOrConditionalRestarted: false } }), {
      pubsub,
      step: makeParallelStep(['__proto__', 'constructor', 'C']),
    });

    expect(published).toEqual([]);
    expect(getter).not.toHaveBeenCalled();
  });

  it('recognizes own magic-key branch IDs after JSON transport', async () => {
    const { published, pubsub } = recorder();
    const activeStepsPath = JSON.parse('{"__proto__":[0],"constructor":[1]}');

    await processWorkflowParallel(makeArgs({ restart: { activeStepsPath, isParallelOrConditionalRestarted: false } }), {
      pubsub,
      step: makeParallelStep(['__proto__', 'constructor', 'ordinary']),
    });

    expect(runPaths(published)).toEqual([
      [0, 0],
      [0, 1],
    ]);
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
});
