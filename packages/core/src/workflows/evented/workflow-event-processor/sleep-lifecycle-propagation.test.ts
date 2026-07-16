import { afterEach, describe, expect, it, vi } from 'vitest';
import { processWorkflowSleep, processWorkflowSleepUntil, processWorkflowWaitForEvent } from './sleep';

const lifecycleExecution = {
  executionGeneration: 'sleep-generation',
  lifecycleResumeAttempt: 2,
  lifecycleStepStates: {
    '["sleep",[0],null,null]': { stepCallId: 'sleep-call', stepAttempt: 1 },
  },
};

function baseArgs() {
  return {
    ...lifecycleExecution,
    workflow: {
      stepGraph: [{ type: 'step', step: { id: 'waiting-step' } }],
    },
    workflowId: 'sleep-workflow',
    runId: 'sleep-run',
    executionPath: [0],
    stepResults: {},
    activeStepsPath: {},
    resumeSteps: [],
    prevResult: { status: 'success', output: { input: true } },
    requestContext: {},
  } as any;
}

function capturingPubsub() {
  const events: any[] = [];
  return {
    events,
    pubsub: {
      publish: vi.fn(async (_topic: string, event: any) => {
        events.push(event);
      }),
    } as any,
  };
}

afterEach(() => {
  vi.useRealTimers();
});

describe('evented delayed transition lifecycle propagation', () => {
  it('carries the exact lifecycle tuple through wait-for-event dispatch', async () => {
    const { events, pubsub } = capturingPubsub();
    const args = baseArgs();

    await processWorkflowWaitForEvent(args, {
      pubsub,
      eventName: 'approved',
      currentState: {
        status: 'waiting',
        runId: args.runId,
        timestamp: Date.now(),
        value: {},
        context: { 'waiting-step': { payload: { waiting: true } } },
        activePaths: [],
        waitingPaths: { approved: [0] },
        suspendedPaths: {},
        activeStepsPath: {},
        resumeLabels: {},
      } as any,
    });

    expect(events).toContainEqual(
      expect.objectContaining({
        type: 'workflow.step.run',
        data: expect.objectContaining(lifecycleExecution),
      }),
    );
  });

  it.each([
    ['sleep', processWorkflowSleep, 'resolveSleep'],
    ['sleepUntil', processWorkflowSleepUntil, 'resolveSleepUntil'],
  ] as const)('carries the exact lifecycle tuple through %s timers', async (_name, process, resolver) => {
    vi.useFakeTimers();
    const { events, pubsub } = capturingPubsub();
    const stepExecutor = { [resolver]: vi.fn(async () => 0) } as any;

    await process(baseArgs(), {
      pubsub,
      stepExecutor,
      step: { type: resolver === 'resolveSleep' ? 'sleep' : 'sleepUntil', id: 'delay' } as any,
    });
    await vi.runAllTimersAsync();

    expect(events).toContainEqual(
      expect.objectContaining({
        type: 'workflow.step.run',
        data: expect.objectContaining(lifecycleExecution),
      }),
    );
  });
});
