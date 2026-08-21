import { EventEmitterPubSub } from '@mastra/core/events';
import { Mastra } from '@mastra/core/mastra';
import { MockStore } from '@mastra/core/storage';
import { Inngest } from 'inngest';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { z } from 'zod';
import { InngestExecutionEngine } from './execution-engine';
import type { InngestRun } from './run';
import { init } from './index';

/**
 * Upstream #21549 asserts that a slim resume event (one that carries neither
 * `initialState` nor `resume.stepResults`) still reaches the execution engine
 * with the run's persisted state, and that the rehydrating read happens outside
 * `step.run` so a memoized retry cannot replay stale state.
 *
 * The fork satisfies that contract more strictly under PF-2056: the handler
 * always loads the authoritative snapshot outside `step.run` and derives
 * `initialState` / `resume.stepResults` from the atomically admitted resume
 * checkpoint, ignoring anything an event claims to carry. These tests assert the
 * fork's semantics, including the deliberate inversion of upstream's
 * "legacy events keep their own state" case.
 */
describe('Inngest workflow resume snapshot rehydration', () => {
  let inngest: Inngest;
  let sendMock: ReturnType<typeof vi.fn>;

  const persistedState = { count: 1 };
  const persistedStepResults = {
    input: { value: 'hello' },
    'suspended-step': { status: 'suspended' },
  } as Record<string, unknown>;

  beforeEach(() => {
    sendMock = vi.fn().mockResolvedValue({ ids: ['evt_resume_rehydration'] });
    inngest = new Inngest({ id: 'resume-rehydration-test', baseUrl: 'http://localhost:9999' });
    (inngest as any).send = sendMock;
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  function buildWorkflow() {
    const { createWorkflow, createStep } = init(inngest);

    const step = createStep({
      id: 'suspended-step',
      inputSchema: z.object({ value: z.string() }),
      resumeSchema: z.object({ approved: z.boolean() }),
      suspendSchema: z.object({}),
      outputSchema: z.object({ value: z.string() }),
      execute: async ({ inputData, resumeData, suspend }) => {
        if (!resumeData) return suspend({});
        return inputData;
      },
    });

    const workflow = createWorkflow({
      id: 'resume-rehydration-workflow',
      inputSchema: z.object({ value: z.string() }),
      outputSchema: z.object({ value: z.string() }),
      steps: [step],
    });
    workflow.then(step).commit();

    return workflow;
  }

  /**
   * Seeds a suspended run and dispatches a real `resumeAsync()` so the store
   * holds the atomically admitted resume checkpoint the fork's handler fences
   * against. Returns the exact event Inngest would deliver to the worker.
   */
  async function dispatchSuspendedResume() {
    const workflow = buildWorkflow();
    const mastra = new Mastra({
      logger: false,
      storage: new MockStore(),
      workflows: { 'resume-rehydration-workflow': workflow as any },
    });
    workflow.__setPubsubFactory(() => new EventEmitterPubSub());

    const run = (await workflow.createRun()) as unknown as InngestRun;
    const workflowsStore = (await mastra.getStorage()!.getStore('workflows'))!;
    await workflowsStore.persistWorkflowSnapshot({
      workflowName: 'resume-rehydration-workflow',
      runId: run.runId,
      resourceId: 'resource-from-snapshot',
      snapshot: {
        runId: run.runId,
        resourceId: 'resource-from-snapshot',
        serializedStepGraph: run.serializedStepGraph,
        status: 'suspended',
        value: persistedState,
        context: persistedStepResults as any,
        activePaths: [],
        suspendedPaths: { 'suspended-step': [0] },
        activeStepsPath: {},
        resumeLabels: {},
        waitingPaths: {},
        timestamp: Date.now(),
        executionGeneration: 'resume-rehydration-execution',
        lifecycleResumeAttempt: 0,
        lifecycleStepStates: {},
      },
    });

    await run.resumeAsync({ step: 'suspended-step', resumeData: { approved: true } });
    const dispatched = sendMock.mock.calls.at(-1)![0] as { data: Record<string, any> };

    return { workflow, workflowsStore, run, dispatched };
  }

  async function runHandler(
    workflow: ReturnType<typeof buildWorkflow>,
    eventData: Record<string, unknown>,
  ): Promise<{ stepRunIds: string[] }> {
    const inngestFunction = workflow.getFunction() as unknown as {
      fn: (context: {
        event: { data: Record<string, unknown> };
        step: { run: <T>(id: string, operation: () => Promise<T>) => Promise<T> };
        attempt: number;
      }) => Promise<unknown>;
    };
    const stepRunIds: string[] = [];
    await inngestFunction.fn({
      event: { data: eventData },
      step: {
        run: async (id, operation) => {
          stepRunIds.push(id);
          return operation();
        },
      },
      attempt: 0,
    });
    return { stepRunIds };
  }

  it('rehydrates state omitted from a slim resume event from the admitted resume checkpoint', async () => {
    const { workflow, workflowsStore, dispatched } = await dispatchSuspendedResume();

    // The dispatched event is slim: PF-2056 removed event-carried execution state.
    expect(dispatched.data).not.toHaveProperty('initialState');
    expect(dispatched.data).not.toHaveProperty('stepResults');
    expect(dispatched.data.resume).not.toHaveProperty('stepResults');

    const loadWorkflowSnapshot = vi.spyOn(workflowsStore, 'loadWorkflowSnapshot');
    const execute = vi.spyOn(InngestExecutionEngine.prototype, 'execute').mockResolvedValue({
      status: 'success',
      input: { value: 'hello' },
      steps: {},
      state: { count: 2 },
      result: { value: 'done' },
    } as never);

    await runHandler(workflow, dispatched.data);

    expect(loadWorkflowSnapshot).toHaveBeenCalledWith(
      expect.objectContaining({ workflowName: 'resume-rehydration-workflow', runId: dispatched.data.runId }),
    );
    expect(execute).toHaveBeenCalledTimes(1);
    expect(execute).toHaveBeenCalledWith(
      expect.objectContaining({
        initialState: persistedState,
        resume: expect.objectContaining({ stepResults: persistedStepResults }),
      }),
    );
  });

  it('reads the authoritative snapshot outside step.run so a memoized retry cannot replay stale state', async () => {
    const { workflow, workflowsStore, dispatched } = await dispatchSuspendedResume();

    let insideStepRun = false;
    const loadDepths: boolean[] = [];
    const originalLoad = workflowsStore.loadWorkflowSnapshot.bind(workflowsStore);
    vi.spyOn(workflowsStore, 'loadWorkflowSnapshot').mockImplementation((async (args: any) => {
      loadDepths.push(insideStepRun);
      return originalLoad(args);
    }) as never);
    vi.spyOn(InngestExecutionEngine.prototype, 'execute').mockResolvedValue({
      status: 'success',
      input: { value: 'hello' },
      steps: {},
      state: { count: 2 },
      result: { value: 'done' },
    } as never);

    const inngestFunction = workflow.getFunction() as unknown as {
      fn: (context: {
        event: { data: Record<string, unknown> };
        step: { run: <T>(id: string, operation: () => Promise<T>) => Promise<T> };
        attempt: number;
      }) => Promise<unknown>;
    };
    await inngestFunction.fn({
      event: { data: dispatched.data },
      step: {
        run: async (_id, operation) => {
          insideStepRun = true;
          try {
            return await operation();
          } finally {
            insideStepRun = false;
          }
        },
      },
      attempt: 0,
    });

    expect(loadDepths.length).toBeGreaterThan(0);
    expect(loadDepths).toContain(false);
  });

  it('ignores execution state carried by a legacy resume event in favour of the snapshot', async () => {
    const { workflow, dispatched } = await dispatchSuspendedResume();
    const execute = vi.spyOn(InngestExecutionEngine.prototype, 'execute').mockResolvedValue({
      status: 'success',
      input: { value: 'hello' },
      steps: {},
      state: { count: 2 },
      result: { value: 'done' },
    } as never);

    // A legacy producer may still stamp state onto the event. Neither field is a
    // resume-operation hash input, so the event still passes admission fencing —
    // the fork must simply refuse to trust it.
    const legacyEventData = {
      ...dispatched.data,
      initialState: { count: 999 },
      resume: { ...(dispatched.data.resume as Record<string, unknown>), stepResults: { spoofed: true } },
    };

    await runHandler(workflow, legacyEventData);

    expect(execute).toHaveBeenCalledTimes(1);
    expect(execute).toHaveBeenCalledWith(
      expect.objectContaining({
        initialState: persistedState,
        resume: expect.objectContaining({ stepResults: persistedStepResults }),
      }),
    );
    const executeArgs = execute.mock.calls[0]![0] as { initialState: unknown; resume: { stepResults: unknown } };
    expect(executeArgs.initialState).not.toEqual({ count: 999 });
    expect(executeArgs.resume.stepResults).not.toHaveProperty('spoofed');
  });
});
