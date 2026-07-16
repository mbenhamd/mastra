import { describe, expect, it } from 'vitest';
import { mergeWorkflowStepLifecycleStates } from '../../lifecycle-events';
import { processWorkflowConditional, processWorkflowParallel } from './parallel';

/**
 * Regression coverage for broker redelivery of `workflow.parallel` /
 * `workflow.conditional`.
 *
 * The serial step path retains one semantic attempt across an at-least-once
 * redelivery (see loop-iteration-count.test.ts "retains one semantic step
 * attempt across broker redelivery"). The branch-fanout path must match: a
 * redelivered container event carries the pre-dispatch baseline while durable
 * state already holds the reserved attempt, so re-reserving would both
 * double-count `stepAttempt` and re-fire the branch under a fresh lifecycle
 * identity.
 *
 * That matters beyond a counter drift: `stepAttempt` is part of the semantic
 * key behind `stepCallId`/eventId, so a re-reserved branch replays
 * `step.started` under a *different* eventId and consumer-side eventId dedup
 * cannot collapse it — defeating the at-least-once contract's own mitigation.
 */

type LifecycleStates = Record<string, { stepCallId: string; stepAttempt: number }>;

function makeHarness() {
  const published: any[] = [];
  let persisted: LifecycleStates | undefined;
  const pubsub = {
    publish: async (_topic: string, event: any) => {
      published.push(event);
    },
  } as any;
  const workflowsStore = {
    updateWorkflowState: async ({ opts }: { opts: { lifecycleStepStates?: LifecycleStates } }) => {
      persisted = structuredClone(opts.lifecycleStepStates);
      return undefined;
    },
  } as any;
  return {
    published,
    pubsub,
    workflowsStore,
    getPersisted: () => persisted,
  };
}

/**
 * Reproduce exactly what WorkflowEventProcessor#dispatch hands a processor: an
 * incoming-only baseline alongside the incoming-merged-with-persisted state.
 * A broker redelivery replays the *same* producer payload, so `incoming` is
 * identical across both deliveries while `persisted` has advanced.
 */
function dispatchArgs({
  incoming,
  persisted,
  baseArgs,
}: {
  incoming: LifecycleStates;
  persisted: LifecycleStates | undefined;
  baseArgs: Record<string, unknown>;
}) {
  return {
    ...baseArgs,
    lifecycleIncomingStepStates: mergeWorkflowStepLifecycleStates(incoming, undefined),
    lifecycleAttemptBaselineCaptured: true,
    lifecycleStepStates: mergeWorkflowStepLifecycleStates(incoming, persisted),
  } as any;
}

const baseArgs = {
  workflowId: 'wf',
  runId: 'redelivered-run',
  executionGeneration: 'redelivered-generation',
  lifecycleResumeAttempt: 0,
  executionPath: [0],
  stepResults: {},
  resumeSteps: [],
  prevResult: { status: 'success', output: {} },
  requestContext: {},
};

function startedBranches(published: any[]) {
  return published
    .filter(event => event.type === 'workflow.step.run')
    .map(event => ({
      path: event.data.executionPath,
      states: event.data.lifecycleStepStates,
    }));
}

describe('branch lifecycle reservation across broker redelivery', () => {
  it('retains one semantic step attempt when workflow.parallel is redelivered', async () => {
    const { published, pubsub, workflowsStore, getPersisted } = makeHarness();
    const step = {
      type: 'parallel' as const,
      steps: [
        { type: 'step' as const, step: { id: 'branch-a' } },
        { type: 'step' as const, step: { id: 'branch-b' } },
      ],
    } as any;

    // First delivery: producer carried no lifecycle state, nothing persisted.
    await processWorkflowParallel(
      dispatchArgs({ incoming: {}, persisted: undefined, baseArgs: { ...baseArgs, activeStepsPath: {} } }),
      { pubsub, workflowsStore, step },
    );
    const afterFirst = structuredClone(getPersisted())!;
    expect(Object.values(afterFirst).map(state => state.stepAttempt)).toEqual([1, 1]);

    // Broker redelivery: the SAME producer payload (incoming still {}) arrives
    // again while durable state already holds the reserved attempt.
    published.length = 0;
    await processWorkflowParallel(
      dispatchArgs({ incoming: {}, persisted: getPersisted(), baseArgs: { ...baseArgs, activeStepsPath: {} } }),
      { pubsub, workflowsStore, step },
    );

    const afterSecond = getPersisted()!;
    expect(afterSecond).toEqual(afterFirst);
    expect(Object.values(afterSecond).map(state => state.stepAttempt)).toEqual([1, 1]);

    // The redelivered fanout must republish the same lifecycle identity, so
    // consumer-side eventId dedup can collapse it.
    for (const branch of startedBranches(published)) {
      expect(branch.states).toEqual(afterFirst);
    }
  });

  it('retains one semantic step attempt when workflow.conditional is redelivered', async () => {
    const { published, pubsub, workflowsStore, getPersisted } = makeHarness();
    const step = {
      type: 'conditional' as const,
      steps: [
        { type: 'step' as const, step: { id: 'branch-a' } },
        { type: 'step' as const, step: { id: 'branch-b' } },
      ],
      conditions: [() => true, () => true],
    } as any;
    const stepExecutor = {
      evaluateConditions: async () => [0, 1],
    } as any;

    await processWorkflowConditional(
      dispatchArgs({ incoming: {}, persisted: undefined, baseArgs: { ...baseArgs, activeStepsPath: {} } }),
      { pubsub, workflowsStore, stepExecutor, step },
    );
    const afterFirst = structuredClone(getPersisted())!;
    expect(Object.values(afterFirst).map(state => state.stepAttempt)).toEqual([1, 1]);

    published.length = 0;
    await processWorkflowConditional(
      dispatchArgs({ incoming: {}, persisted: getPersisted(), baseArgs: { ...baseArgs, activeStepsPath: {} } }),
      { pubsub, workflowsStore, stepExecutor, step },
    );

    const afterSecond = getPersisted()!;
    expect(afterSecond).toEqual(afterFirst);
    expect(Object.values(afterSecond).map(state => state.stepAttempt)).toEqual([1, 1]);

    for (const branch of startedBranches(published)) {
      expect(branch.states).toEqual(afterFirst);
    }
  });

  it('reserves the next semantic attempt when the producer carries the current baseline', async () => {
    const { published, pubsub, workflowsStore, getPersisted } = makeHarness();
    const step = {
      type: 'parallel' as const,
      steps: [{ type: 'step' as const, step: { id: 'branch-a' } }],
    } as any;

    await processWorkflowParallel(
      dispatchArgs({ incoming: {}, persisted: undefined, baseArgs: { ...baseArgs, activeStepsPath: {} } }),
      { pubsub, workflowsStore, step },
    );
    const afterFirst = structuredClone(getPersisted())!;

    // An explicit retry/restart producer carries the CURRENT baseline (N+1),
    // which must still reserve the next semantic attempt rather than be
    // mistaken for a redelivery.
    published.length = 0;
    await processWorkflowParallel(
      dispatchArgs({ incoming: afterFirst, persisted: getPersisted(), baseArgs: { ...baseArgs, activeStepsPath: {} } }),
      { pubsub, workflowsStore, step },
    );

    expect(Object.values(getPersisted()!).map(state => state.stepAttempt)).toEqual([2]);
  });
});
