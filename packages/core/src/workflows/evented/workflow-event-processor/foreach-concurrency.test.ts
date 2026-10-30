import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { RequestContext } from '../../../di';
import { EventEmitterPubSub } from '../../../events/event-emitter';
import { Mastra } from '../../../mastra';
import { MASTRA_AUTH_TOKEN_KEY } from '../../../request-context';
import { InMemoryStore } from '../../../storage';
import { createEmptyWorkflowSnapshot } from '../../../storage/workflow-snapshot';
import { createEventedWorkflow } from '../../create';
import type { ForeachConcurrencyContext } from '../../types';
import { createStep } from '../../workflow';
import { processWorkflowForEach } from './loop';
import { WorkflowEventProcessor } from '.';

/**
 * Regression coverage for execution-time foreach concurrency resolution in the
 * evented engine.
 *
 * `opts.concurrency` may be a resolver function instead of a static number
 * (used by durable agents to derive tool-call concurrency from serialized run
 * state). The evented processor must invoke the resolver when kicking off the
 * initial iteration batch — with the foreach input and the run's init data —
 * rather than treating the function as a missing static value (which would
 * silently serialize every iteration).
 */

function makeForeachStep(concurrency: number | ((ctx: ForeachConcurrencyContext) => number)) {
  return {
    type: 'foreach' as const,
    step: { id: 'body' },
    opts: { concurrency },
  } as any;
}

async function kickOffForeach({
  concurrency,
  items,
  initData,
  currentOutput,
  currentSuspendPayload,
  restart = false,
  executionPath = [0],
  requestContext = {},
  updateWorkflowResults = vi.fn(),
}: {
  concurrency: number | ((ctx: ForeachConcurrencyContext) => number);
  items: unknown[];
  initData?: unknown;
  currentOutput?: unknown[];
  currentSuspendPayload?: unknown;
  restart?: boolean;
  executionPath?: number[];
  requestContext?: Record<string, unknown>;
  updateWorkflowResults?: ReturnType<typeof vi.fn>;
}) {
  const published: any[] = [];
  const pubsub = {
    publish: async (_topic: string, event: any) => {
      published.push(event);
    },
  } as any;
  const mastra = { getStorage: () => ({ getStore: async () => ({ updateWorkflowResults }) }) } as any;

  await processWorkflowForEach(
    {
      workflowId: 'wf',
      runId: 'run-1',
      executionPath,
      stepResults: {
        ...(initData === undefined ? {} : { input: initData }),
        ...(currentOutput === undefined
          ? {}
          : {
              body: {
                status: 'success',
                output: currentOutput,
                startedAt: 1,
                payload: items,
                suspendPayload: currentSuspendPayload,
              },
            }),
      } as any,
      activeStepsPath: {},
      resumeSteps: [],
      restart: restart ? {} : undefined,
      prevResult: { status: 'success', output: items, startedAt: 1, endedAt: 2, payload: {} },
      requestContext,
    } as any,
    { pubsub, mastra, step: makeForeachStep(concurrency) },
  );

  return published;
}

describe('processWorkflowForEach concurrency resolution', () => {
  it('kicks off the initial batch using a resolver function', async () => {
    const resolverCalls: { inputData: unknown; initData: unknown }[] = [];
    const items = [1, 2, 3, 4, 5];
    const initData = { options: { toolCallConcurrency: 3 } };

    const runEvents = await kickOffForeach({
      concurrency: ctx => {
        resolverCalls.push({ inputData: ctx.inputData, initData: ctx.getInitData() });
        return 3;
      },
      items,
      initData,
    });

    // Resolver decides the initial batch size at execution time.
    expect(runEvents).toHaveLength(3);
    expect(runEvents.map(e => e.data.executionPath)).toEqual([
      [0, 0],
      [0, 1],
      [0, 2],
    ]);
    // Resolver sees the foreach input and the run's init data.
    expect(resolverCalls).toEqual([{ inputData: items, initData }]);
  });

  it.each([1, 2])('reschedules unfinished restart slots with concurrency %i', async concurrency => {
    const completed = { status: 'success', output: 'first' };
    const events = await kickOffForeach({
      concurrency,
      items: [1, 2, 3],
      currentOutput: [completed, null, null],
      restart: true,
    });

    expect(events).toHaveLength(concurrency);
    expect(events.map(event => event.data.executionPath)).toEqual(
      Array.from({ length: concurrency }, (_, index) => [0, index + 1]),
    );
    for (const event of events) {
      expect(event.data.restart).toBeUndefined();
      expect(event.data.stepResults.body.output[0]).toEqual(completed);
    }
  });

  it('strips the live auth token from restart writes without removing it from iteration events', async () => {
    const requestContext = { [MASTRA_AUTH_TOKEN_KEY]: 'test-only-token', tenant: 'acme' };
    const updateWorkflowResults = vi.fn();
    const events = await kickOffForeach({
      concurrency: 1,
      items: [1, 2],
      currentOutput: ['first', null],
      restart: true,
      requestContext,
      updateWorkflowResults,
    });

    expect(updateWorkflowResults).toHaveBeenCalledTimes(2);
    for (const [write] of updateWorkflowResults.mock.calls) {
      expect(write.requestContext).toEqual({ tenant: 'acme' });
    }
    expect(events).toHaveLength(1);
    expect(events[0].data.requestContext).toEqual(requestContext);
    expect(requestContext[MASTRA_AUTH_TOKEN_KEY]).toBe('test-only-token');
  });

  it('advances the workflow path when restarting an already-complete foreach', async () => {
    const events = await kickOffForeach({
      concurrency: 1,
      items: [1, 2],
      currentOutput: ['first', 'second'],
      executionPath: [0, 1],
      restart: true,
    });
    expect(events).toHaveLength(1);
    expect(events[0].data.executionPath).toEqual([1]);
    expect(events[0].data.prevResult.output).toEqual(['first', 'second']);
  });

  it('launches a normal initial batch on a pre-first-step restart', async () => {
    const events = await kickOffForeach({ concurrency: 2, items: [1, 2, 3], restart: true });
    expect(events.map(event => event.data.executionPath)).toEqual([
      [0, 0],
      [0, 1],
    ]);
    expect(events.every(event => event.data.restart === undefined)).toBe(true);
  });

  it('launches the next unscheduled item normally on restart', async () => {
    const events = await kickOffForeach({
      concurrency: 1,
      items: [1, 2, 3],
      currentOutput: ['first'],
      executionPath: [0, 0],
      restart: true,
    });
    expect(events).toHaveLength(1);
    expect(events[0].data.executionPath).toEqual([0, 1]);
    expect(events[0].data.restart).toBeUndefined();
  });

  it('keeps suspended restart iterations within the concurrency limit', async () => {
    const events = await kickOffForeach({
      concurrency: 1,
      items: [1, 2],
      // PF-2010 (PF-6640 sync): a suspended foreach iteration is only recognised as the engine's
      // suspension envelope (suspendedAt plus its __workflow_meta path); anything else fails closed.
      currentOutput: [
        {
          status: 'suspended',
          suspendedAt: 1,
          suspendPayload: { approval: true, __workflow_meta: { path: ['body'] } },
        },
        null,
      ],
      executionPath: [0, 0],
      restart: true,
    });
    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({
      type: 'workflow.step.end',
      data: { executionPath: [0], prevResult: { status: 'suspended' } },
    });
  });

  it('still supports static concurrency numbers', async () => {
    const runEvents = await kickOffForeach({ concurrency: 2, items: [1, 2, 3] });
    expect(runEvents).toHaveLength(2);
  });

  it('passes the current RequestContext to the resolver', async () => {
    const runEvents = await kickOffForeach({
      concurrency: context => (context.requestContext?.get('forceSequential') === true ? 1 : 3),
      items: [1, 2, 3],
      requestContext: { forceSequential: true },
    });

    expect(runEvents).toHaveLength(1);
  });

  it('falls back to sequential kick-off when the resolver returns an invalid value', async () => {
    const runEvents = await kickOffForeach({ concurrency: () => -5, items: [1, 2, 3] });
    expect(runEvents).toHaveLength(1);
  });

  it('advances past completed user output containing the foreach queue property', async () => {
    const userOutput = { __mastra_foreach_queued__: true, value: 'user-data' };
    const published: any[] = [];
    await processWorkflowForEach(
      {
        workflowId: 'wf',
        runId: 'run-1',
        executionPath: [0],
        stepResults: {
          body: { status: 'success', output: [userOutput], payload: [1] },
        },
        activeStepsPath: {},
        resumeSteps: [],
        prevResult: { status: 'success', output: [1] },
        requestContext: {},
      } as any,
      {
        pubsub: { publish: async (_topic: string, event: any) => void published.push(event) } as any,
        mastra: { getStorage: () => undefined } as any,
        step: makeForeachStep(1),
      },
    );

    expect(published).toMatchObject([
      {
        type: 'workflow.step.run',
        data: { executionPath: [1], prevResult: { status: 'success', output: [userOutput] } },
      },
    ]);
  });

  it('preserves both successful retries when concurrent completion handlers read the same failed progress', async () => {
    const storage = new InMemoryStore();
    const mastra = new Mastra({ logger: false, storage });
    const publish = vi.spyOn(mastra.pubsub, 'publish').mockResolvedValue(undefined);
    const workflowsStore = (await storage.getStore('workflows'))!;
    const snapshot = createEmptyWorkflowSnapshot('concurrent-retry');
    // The completion handlers forward this lineage to updateWorkflowResults;
    // the persisted row must declare it or the stale-lifetime fence drops the merge.
    snapshot.executionGeneration = 'retry-generation';
    snapshot.context.body = {
      status: 'failed',
      output: [null, null],
      payload: [0, 1],
      suspendPayload: {
        __workflow_meta: {
          foreachOutput: [
            { status: 'failed', error: 'first attempt failed at 0' },
            { status: 'failed', error: 'first attempt failed at 1' },
          ],
        },
      },
    } as any;
    await workflowsStore.persistWorkflowSnapshot({ workflowName: 'wf', runId: snapshot.runId, snapshot });

    // Both completions observe the pre-retry snapshot before either can write.
    // The real store merge must retain each fresh result despite that stale read.
    const loadSnapshot = workflowsStore.loadWorkflowSnapshot.bind(workflowsStore);
    let releaseReads!: () => void;
    const readsReady = new Promise<void>(resolve => {
      releaseReads = resolve;
    });
    let readCount = 0;
    const loadSpy = vi.spyOn(workflowsStore, 'loadWorkflowSnapshot').mockImplementation(async args => {
      const result = await loadSnapshot(args);
      if (++readCount === 2) releaseReads();
      await readsReady;
      return result;
    });
    class ExposedProcessor extends WorkflowEventProcessor {
      completeIteration(args: any) {
        return this.processWorkflowStepEnd(args);
      }
    }
    const processor = new ExposedProcessor({ mastra });

    try {
      await Promise.all(
        [0, 1].map(index =>
          processor.completeIteration({
            workflow: { id: 'wf', stepGraph: [makeForeachStep(2)], options: {} },
            workflowId: 'wf',
            runId: snapshot.runId,
            executionGeneration: 'retry-generation',
            executionPath: [0, index],
            stepResults: snapshot.context,
            activeStepsPath: {},
            resumeSteps: [],
            timeTravel: true,
            prevResult: { status: 'success', output: index, startedAt: 1, endedAt: 2 },
            requestContext: {},
          }),
        ),
      );

      const persisted = await loadSnapshot({ workflowName: 'wf', runId: snapshot.runId });
      expect(persisted?.context.body).toMatchObject({ status: 'success', output: [0, 1] });
      expect(publish.mock.calls.filter(([_topic, event]) => event.type === 'workflow.fail')).toEqual([]);
      expect(publish.mock.calls.map(([_topic, event]) => event)).toContainEqual(
        expect.objectContaining({
          type: 'workflow.step.run',
          data: expect.objectContaining({
            executionPath: [1],
            prevResult: expect.objectContaining({ output: [0, 1] }),
          }),
        }),
      );
    } finally {
      releaseReads();
      loadSpy.mockRestore();
      publish.mockRestore();
      await mastra.shutdown();
    }
  });

  it('retries queued failures at reduced concurrency while preserving a fresh retry failure', async () => {
    const executions = [0, 0];
    const body = createStep({
      id: 'dynamic-retry-body',
      inputSchema: z.number(),
      outputSchema: z.number(),
      execute: async ({ inputData }) => {
        executions[inputData]! += 1;
        if (executions[inputData] === 1) throw new Error(`initial failure at ${inputData}`);
        if (inputData === 1 && executions[inputData] === 2) throw new Error('current retry failed');
        return inputData;
      },
    });
    const workflow = createEventedWorkflow({
      id: 'dynamic-concurrency-retry',
      inputSchema: z.array(z.number()),
      outputSchema: z.array(z.number()),
      retryConfig: { attempts: 0 },
    })
      .foreach(body, { concurrency: ({ requestContext }) => (requestContext?.get('sequentialRetry') ? 1 : 2) })
      .commit();
    const mastra = new Mastra({
      logger: false,
      storage: new InMemoryStore(),
      pubsub: new EventEmitterPubSub(),
      workflows: { [workflow.id]: workflow },
    });
    await mastra.startWorkers();

    try {
      const run = await workflow.createRun();
      const initial = await run.start({ inputData: [0, 1] });
      expect(initial.status).toBe('failed');
      expect(executions).toEqual([1, 1]);

      const requestContext = new RequestContext([['sequentialRetry', true]]);
      const retried = await run.timeTravel({ step: body.id, requestContext });
      // The second coordinate's old failure must not stop the first retry
      // from advancing to it. Its new failure must still fail this attempt.
      expect(executions).toEqual([2, 2]);
      expect(retried).toMatchObject({ status: 'failed', error: { message: 'current retry failed' } });

      const completed = await run.timeTravel({ step: body.id, requestContext });
      expect(completed).toMatchObject({ status: 'success', result: [0, 1] });
      expect(executions).toEqual([2, 3]);
    } finally {
      await mastra.shutdown();
    }
  });

  // Upstream #25639 cases, with suspended iterations written as the fork's engine suspension
  // envelope (a user output carrying `status` is not a suspension).
  it('suspends instead of replacing an iteration that still occupies its concurrency slot', async () => {
    const events = await kickOffForeach({
      concurrency: 1,
      items: [1, 2],
      currentOutput: [
        {
          status: 'suspended',
          suspendedAt: 1,
          suspendPayload: { approval: true, __workflow_meta: { path: ['body'] } },
        },
      ],
    });

    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({
      type: 'workflow.step.end',
      data: { prevResult: { status: 'suspended' } },
    });
  });

  it('preserves foreach output metadata when all active iterations are suspended', async () => {
    const suspendedIteration = {
      status: 'suspended',
      suspendedAt: 1,
      suspendPayload: { approval: true, __workflow_meta: { path: ['body'] } },
    };
    const events = await kickOffForeach({
      concurrency: 1,
      items: [1, 2, 3],
      currentOutput: [{ status: 'success', output: 'first' }, suspendedIteration],
      currentSuspendPayload: { __workflow_meta: { marker: 'kept' } },
    });

    expect(events).toHaveLength(1);
    // The fork rebuilds `foreachOutput` from the live suspended iterations; other step metadata is kept.
    expect(events[0]).toMatchObject({
      type: 'workflow.step.end',
      data: {
        prevResult: {
          status: 'suspended',
          suspendPayload: {
            __workflow_meta: { marker: 'kept', foreachIndex: 1, foreachOutput: { 1: suspendedIteration } },
          },
        },
      },
    });
  });

  it('starts the next iteration after a concurrency slot completes', async () => {
    const events = await kickOffForeach({
      concurrency: 1,
      items: [1, 2],
      currentOutput: [{ status: 'success', output: 'first' }],
    });

    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({
      type: 'workflow.step.run',
      data: { executionPath: [0, 1] },
    });
  });

  it('fills a free slot while another iteration remains suspended', async () => {
    const events = await kickOffForeach({
      concurrency: 2,
      items: [1, 2, 3],
      currentOutput: [
        {
          status: 'suspended',
          suspendedAt: 1,
          suspendPayload: { approval: true, __workflow_meta: { path: ['body'] } },
        },
        { status: 'success', output: 'second' },
      ],
    });

    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({
      type: 'workflow.step.run',
      data: { executionPath: [0, 2] },
    });
  });
});
