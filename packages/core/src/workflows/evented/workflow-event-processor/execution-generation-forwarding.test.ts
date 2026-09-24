import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { EventEmitterPubSub } from '../../../events/event-emitter';
import { Mastra } from '../../../mastra';
import type { WorkflowsStorage } from '../../../storage/domains/workflows/base';
import { MockStore } from '../../../storage/mock';
import { STALE_EXECUTION_RESULT } from '../../../storage/types';
import { createStep, createWorkflow } from '../../evented';
import { WorkflowEventProcessor } from '.';

/**
 * PF-4387: every `updateWorkflowResults` callsite must forward the run's
 * lifecycle `executionGeneration` so storage boundaries can fence delayed
 * result writes from a deleted lifetime (PF-4385 generation-aware tombstone
 * reopen). A real evented run drives the callsites; the spy asserts the
 * forwarded identity on each one the run reaches.
 */
type RecordedResultWrite = {
  workflowName: string;
  runId: string;
  stepId: string;
  result: any;
  executionGeneration?: string;
};

function recordResultWrites(workflowsStore: WorkflowsStorage): RecordedResultWrite[] {
  const seen: RecordedResultWrite[] = [];
  const updateWorkflowResults = workflowsStore.updateWorkflowResults.bind(workflowsStore);
  vi.spyOn(workflowsStore, 'updateWorkflowResults').mockImplementation(async args => {
    seen.push({
      workflowName: args.workflowName,
      runId: args.runId,
      stepId: args.stepId,
      result: args.result,
      executionGeneration: args.executionGeneration,
    });
    return updateWorkflowResults(args);
  });
  return seen;
}

describe('updateWorkflowResults executionGeneration forwarding', () => {
  it('forwards the run executionGeneration on every result write', async () => {
    const storage = new MockStore();
    const pubsub = new EventEmitterPubSub();
    const step = createStep({
      id: 'forwarding-step',
      inputSchema: z.object({}),
      outputSchema: z.object({ value: z.string() }),
      execute: async () => ({ value: 'done' }),
    });
    const workflow = createWorkflow({
      id: `forwarding-${Math.random().toString(36).slice(2)}`,
      inputSchema: z.object({}),
      outputSchema: z.object({ value: z.string() }),
    })
      .then(step)
      .commit();
    const mastra = new Mastra({ logger: false, storage, pubsub, workflows: { [workflow.id]: workflow } });
    const workflowsStore = (await storage.getStore('workflows'))!;
    const seen = recordResultWrites(workflowsStore);

    await mastra.startWorkers();
    try {
      const runId = `run-${Math.random().toString(36).slice(2)}`;
      const run = await workflow.createRun({ runId });
      const pending = (await workflowsStore.loadWorkflowSnapshot({ workflowName: workflow.id, runId }))!;
      const generation = pending.executionGeneration!;
      expect(generation).toBeTruthy();

      const result = await run.start({ inputData: {} });
      expect(result.status).toBe('success');

      expect(seen.length).toBeGreaterThan(0);
      expect(seen.every(call => call.executionGeneration === generation)).toBe(true);
    } finally {
      await mastra.shutdown();
    }
  });

  it('forwards the run executionGeneration on foreach iteration writes', async () => {
    const storage = new MockStore();
    const pubsub = new EventEmitterPubSub();
    const body = createStep({
      id: 'foreach-body',
      inputSchema: z.number(),
      outputSchema: z.number(),
      execute: async ({ inputData }) => inputData * 2,
    });
    const workflow = createWorkflow({
      id: `foreach-forwarding-${Math.random().toString(36).slice(2)}`,
      inputSchema: z.array(z.number()),
      outputSchema: z.array(z.number()),
    })
      .foreach(body)
      .commit();
    const mastra = new Mastra({ logger: false, storage, pubsub, workflows: { [workflow.id]: workflow } });
    const workflowsStore = (await storage.getStore('workflows'))!;
    const seen = recordResultWrites(workflowsStore);

    await mastra.startWorkers();
    try {
      const runId = `run-${Math.random().toString(36).slice(2)}`;
      const run = await workflow.createRun({ runId });
      const pending = (await workflowsStore.loadWorkflowSnapshot({ workflowName: workflow.id, runId }))!;
      const generation = pending.executionGeneration!;
      expect(generation).toBeTruthy();

      const result = await run.start({ inputData: [1, 2] });
      expect(result.status).toBe('success');

      // processWorkflowForEach persists the aggregate foreach result (array
      // output) on kick-off, on each subsequent iteration, and on completion —
      // every one of those loop.ts writes must carry the run's generation.
      expect(seen.some(call => call.stepId === body.id && Array.isArray(call.result?.output))).toBe(true);
      expect(seen.every(call => call.executionGeneration === generation)).toBe(true);
    } finally {
      await mastra.shutdown();
    }
  });

  it('forwards the run executionGeneration on loop iteration writes', async () => {
    const storage = new MockStore();
    const pubsub = new EventEmitterPubSub();
    const body = createStep({
      id: 'loop-body',
      inputSchema: z.object({}),
      outputSchema: z.object({ value: z.number() }),
      execute: async () => ({ value: 1 }),
    });
    const workflow = createWorkflow({
      id: `loop-forwarding-${Math.random().toString(36).slice(2)}`,
      inputSchema: z.object({}),
      outputSchema: z.object({ value: z.number() }),
    })
      .dountil(body, async ({ iterationCount }) => iterationCount >= 2)
      .commit();
    const mastra = new Mastra({ logger: false, storage, pubsub, workflows: { [workflow.id]: workflow } });
    const workflowsStore = (await storage.getStore('workflows'))!;
    const seen = recordResultWrites(workflowsStore);

    await mastra.startWorkers();
    try {
      const runId = `run-${Math.random().toString(36).slice(2)}`;
      const run = await workflow.createRun({ runId });
      const pending = (await workflowsStore.loadWorkflowSnapshot({ workflowName: workflow.id, runId }))!;
      const generation = pending.executionGeneration!;
      expect(generation).toBeTruthy();

      const result = await run.start({ inputData: {} });
      expect(result.status).toBe('success');

      // persistNextIteration records the next-iteration marker before
      // re-dispatching the loop body — the one loop.ts write this run reaches.
      expect(seen.some(call => call.stepId === body.id && call.result?.metadata?.iterationCount === 1)).toBe(true);
      expect(seen.every(call => call.executionGeneration === generation)).toBe(true);
    } finally {
      await mastra.shutdown();
    }
  });

  it('forwards the parent executionGeneration on the nested-run ownership fallback', async () => {
    const storage = new MockStore();
    const pubsub = new EventEmitterPubSub();
    const childStep = createStep({
      id: 'nested-child-step',
      inputSchema: z.object({}),
      outputSchema: z.object({ value: z.string() }),
      execute: async () => ({ value: 'child-done' }),
    });
    const nestedWorkflow = createWorkflow({
      id: `nested-forwarding-child-${Math.random().toString(36).slice(2)}`,
      inputSchema: z.object({}),
      outputSchema: z.object({ value: z.string() }),
      steps: [childStep],
    })
      .then(childStep)
      .commit();
    const workflow = createWorkflow({
      id: `nested-forwarding-parent-${Math.random().toString(36).slice(2)}`,
      inputSchema: z.object({}),
      outputSchema: z.object({ value: z.string() }),
      steps: [nestedWorkflow],
    })
      .then(nestedWorkflow)
      .commit();
    const mastra = new Mastra({ logger: false, storage, pubsub, workflows: { [workflow.id]: workflow } });
    const workflowsStore = (await storage.getStore('workflows'))!;
    const seen = recordResultWrites(workflowsStore);
    // Force the pre-recovery nested-ownership lane: without terminal recovery
    // the processor binds the child through bindWorkflowNestedRunOwnership,
    // and a store without that lane falls back to updateWorkflowResults with
    // the parent run's own executionGeneration.
    vi.spyOn(workflowsStore, 'getWorkflowTerminalizationCapabilities').mockReturnValue({});
    const bindSpy = vi
      .spyOn(workflowsStore, 'bindWorkflowNestedRunOwnership')
      .mockResolvedValue({ status: 'unsupported' });

    await mastra.startWorkers();
    try {
      const runId = `run-${Math.random().toString(36).slice(2)}`;
      const run = await workflow.createRun({ runId });
      const pending = (await workflowsStore.loadWorkflowSnapshot({ workflowName: workflow.id, runId }))!;
      const generation = pending.executionGeneration!;
      expect(generation).toBeTruthy();

      const result = await run.start({ inputData: {} });
      expect(result.status).toBe('success');

      // The nested start bound through the mocked-unsupported lane, so the
      // fallback updateWorkflowResults write carried the parent's generation.
      expect(bindSpy).toHaveBeenCalled();
      // The fallback write targets the parent run under the nested step id.
      expect(seen.some(call => call.runId === runId && call.stepId === nestedWorkflow.id)).toBe(true);
      // Every recorded write carries the generation of the run it targets:
      // the parent's own generation on parent-targeted calls (the fallback),
      // the nested run's own generation on child-targeted calls.
      const targetGenerations = new Map<string, string | undefined>();
      for (const call of seen) {
        if (!targetGenerations.has(call.runId)) {
          const snapshot = await workflowsStore.loadWorkflowSnapshot({
            workflowName: call.workflowName,
            runId: call.runId,
          });
          targetGenerations.set(call.runId, snapshot?.executionGeneration);
        }
        const expected = targetGenerations.get(call.runId);
        expect(expected).toBeTruthy();
        expect(call.executionGeneration).toBe(expected);
      }
    } finally {
      await mastra.shutdown();
    }
  });

  it('forwards the run executionGeneration on suspension writes', async () => {
    const storage = new MockStore();
    const pubsub = new EventEmitterPubSub();
    const suspendingStep = createStep({
      id: 'suspending-step',
      inputSchema: z.object({}),
      suspendSchema: z.object({ reason: z.string() }),
      outputSchema: z.object({ value: z.string() }),
      execute: async ({ suspend }) => {
        await suspend({ reason: 'waiting' });
        return { value: 'resumed' };
      },
    });
    const workflow = createWorkflow({
      id: `suspend-forwarding-${Math.random().toString(36).slice(2)}`,
      inputSchema: z.object({}),
      outputSchema: z.object({ value: z.string() }),
    })
      .then(suspendingStep)
      .commit();
    const mastra = new Mastra({ logger: false, storage, pubsub, workflows: { [workflow.id]: workflow } });
    const workflowsStore = (await storage.getStore('workflows'))!;
    const seen = recordResultWrites(workflowsStore);

    await mastra.startWorkers();
    try {
      const runId = `run-${Math.random().toString(36).slice(2)}`;
      const run = await workflow.createRun({ runId });
      const pending = (await workflowsStore.loadWorkflowSnapshot({ workflowName: workflow.id, runId }))!;
      const generation = pending.executionGeneration!;
      expect(generation).toBeTruthy();

      const result = await run.start({ inputData: {} });
      expect(result.status).toBe('suspended');

      // Suspension persists the step result and the '__state' context entry.
      expect(seen.some(call => call.stepId === '__state')).toBe(true);
      expect(seen.every(call => call.executionGeneration === generation)).toBe(true);
    } finally {
      await mastra.shutdown();
    }
  });

  it('stops a fenced stale-lifetime write before advancing stepResults or publishing engine events', async () => {
    const storage = new MockStore();
    const pubsub = new EventEmitterPubSub();
    const step = createStep({
      id: 'fenced-step',
      inputSchema: z.object({}),
      outputSchema: z.object({ value: z.string() }),
      execute: async () => ({ value: 'done' }),
    });
    const workflow = createWorkflow({
      id: `stale-stop-${Math.random().toString(36).slice(2)}`,
      inputSchema: z.object({}),
      outputSchema: z.object({ value: z.string() }),
    })
      .then(step)
      .commit();
    const mastra = new Mastra({ logger: false, storage, pubsub, workflows: { [workflow.id]: workflow } });
    const workflowsStore = (await storage.getStore('workflows'))!;

    // A real run stamps the snapshot's execution generation, so dispatch does
    // not skip the event — the fence below is what rejects the write.
    const runId = `run-${Math.random().toString(36).slice(2)}`;
    await workflow.createRun({ runId });
    const pending = (await workflowsStore.loadWorkflowSnapshot({ workflowName: workflow.id, runId }))!;
    const generation = pending.executionGeneration!;
    expect(generation).toBeTruthy();

    // Answer every result write the way an adapter guarding a reopened
    // lifetime does: the stored snapshot belongs to a different generation.
    const resultWrites = vi.spyOn(workflowsStore, 'updateWorkflowResults').mockResolvedValue(STALE_EXECUTION_RESULT);
    const engineEvents: string[] = [];
    const publish = pubsub.publish.bind(pubsub);
    vi.spyOn(pubsub, 'publish').mockImplementation(async (topic, event) => {
      if (topic === 'workflows') engineEvents.push((event as { type: string }).type);
      return publish(topic, event);
    });

    const processor = new WorkflowEventProcessor({ mastra });
    const handled = await processor.handle({
      type: 'workflow.step.end',
      id: 'evt-stale-write',
      runId,
      createdAt: new Date(),
      data: {
        workflowId: workflow.id,
        runId,
        executionGeneration: generation,
        executionPath: [0],
        prevResult: {
          status: 'success',
          output: { value: 'done' },
          payload: {},
          startedAt: 1,
          endedAt: 2,
        },
        stepResults: {},
        activeStepsPath: {},
        resumeSteps: [],
        requestContext: {},
        lifecycleStepStates: {},
      },
    });

    // The handler reached the write (dispatch did not skip it), acknowledged
    // the event, and stopped: no engine-advancing publish and nothing merged.
    expect(resultWrites).toHaveBeenCalledTimes(1);
    expect(handled).toEqual({ ok: true });
    expect(engineEvents).toEqual([]);
    await expect(workflowsStore.loadWorkflowSnapshot({ workflowName: workflow.id, runId })).resolves.toMatchObject({
      executionGeneration: generation,
      context: {},
    });
  });

  it('gates step lifecycle publications on the persisted execution generation', async () => {
    const storage = new MockStore();
    const pubsub = new EventEmitterPubSub();
    const step = createStep({
      id: 'gated-step',
      inputSchema: z.object({}),
      outputSchema: z.object({ value: z.string() }),
      execute: async () => ({ value: 'done' }),
    });
    const workflow = createWorkflow({
      id: `stale-publish-${Math.random().toString(36).slice(2)}`,
      inputSchema: z.object({}),
      outputSchema: z.object({ value: z.string() }),
    })
      .then(step)
      .commit();
    const mastra = new Mastra({ logger: false, storage, pubsub, workflows: { [workflow.id]: workflow } });
    const workflowsStore = (await storage.getStore('workflows'))!;

    // A real run stamps the snapshot with the reopened lifetime's generation;
    // the arriving step-end event still carries the deleted lifetime's.
    const runId = `run-${Math.random().toString(36).slice(2)}`;
    await workflow.createRun({ runId });
    const pending = (await workflowsStore.loadWorkflowSnapshot({ workflowName: workflow.id, runId }))!;
    const reopenedGeneration = pending.executionGeneration!;
    expect(reopenedGeneration).toBeTruthy();

    const resultWrites = vi.spyOn(workflowsStore, 'updateWorkflowResults');
    const lifecycleEvents: string[] = [];
    const engineEvents: string[] = [];
    const publish = pubsub.publish.bind(pubsub);
    vi.spyOn(pubsub, 'publish').mockImplementation(async (topic, event) => {
      const type = (event as { type: string }).type;
      if (type === 'workflow.lifecycle') lifecycleEvents.push(type);
      if (topic === 'workflows') engineEvents.push(type);
      return publish(topic, event);
    });

    const processor = new WorkflowEventProcessor({ mastra });
    const handled = await processor.handle({
      type: 'workflow.step.end',
      id: 'evt-stale-publish',
      runId,
      createdAt: new Date(),
      data: {
        workflowId: workflow.id,
        runId,
        executionGeneration: 'wfeg:deleted-lifetime',
        executionPath: [0],
        prevResult: {
          status: 'success',
          output: { value: 'done' },
          payload: {},
          startedAt: 1,
          endedAt: 2,
        },
        stepResults: {},
        activeStepsPath: {},
        resumeSteps: [],
        requestContext: {},
        lifecycleStepStates: {},
      },
    });

    // The stale delivery stopped before any lifecycle or engine event — no
    // step.completed/step.finished for a step the reopened run never ran —
    // and never reached a result write at all.
    expect(handled).toEqual({ ok: true });
    expect(resultWrites).not.toHaveBeenCalled();
    expect(lifecycleEvents).toEqual([]);
    expect(engineEvents).toEqual([]);
  });

  it('stops the suspension flow when the guarded state update rejects a stale generation', async () => {
    const storage = new MockStore();
    const pubsub = new EventEmitterPubSub();
    const suspendingStep = createStep({
      id: 'guarded-suspend-step',
      inputSchema: z.object({}),
      suspendSchema: z.object({ reason: z.string() }),
      outputSchema: z.object({ value: z.string() }),
      execute: async () => ({ value: 'done' }),
    });
    const workflow = createWorkflow({
      id: `stale-suspend-${Math.random().toString(36).slice(2)}`,
      inputSchema: z.object({}),
      outputSchema: z.object({ value: z.string() }),
    })
      .then(suspendingStep)
      .commit();
    const mastra = new Mastra({ logger: false, storage, pubsub, workflows: { [workflow.id]: workflow } });
    const workflowsStore = (await storage.getStore('workflows'))!;

    const runId = `run-${Math.random().toString(36).slice(2)}`;
    await workflow.createRun({ runId });
    const pending = (await workflowsStore.loadWorkflowSnapshot({ workflowName: workflow.id, runId }))!;
    const generation = pending.executionGeneration!;
    expect(generation).toBeTruthy();

    // Answer the state CAS the way a reopened-lifetime adapter does: the
    // persisted generation moved, so the guard rejects the update.
    const stateUpdates = vi.spyOn(workflowsStore, 'updateWorkflowState').mockResolvedValue(undefined);
    const engineEvents: string[] = [];
    const publish = pubsub.publish.bind(pubsub);
    vi.spyOn(pubsub, 'publish').mockImplementation(async (topic, event) => {
      if (topic === 'workflows') engineEvents.push((event as { type: string }).type);
      return publish(topic, event);
    });

    const processor = new WorkflowEventProcessor({ mastra });
    const handled = await processor.handle({
      type: 'workflow.step.end',
      id: 'evt-stale-suspend',
      runId,
      createdAt: new Date(),
      data: {
        workflowId: workflow.id,
        runId,
        executionGeneration: generation,
        executionPath: [0],
        prevResult: {
          status: 'suspended',
          output: undefined,
          payload: {},
          startedAt: 1,
          suspendPayload: { reason: 'waiting' },
        },
        stepResults: {},
        activeStepsPath: {},
        resumeSteps: [],
        requestContext: {},
        lifecycleStepStates: {},
      },
    });

    // The suspension state write ran under the same CAS the result writes
    // carry, and its rejection stopped the flow before workflow.suspend could
    // publish for the stale lifetime.
    expect(handled).toEqual({ ok: true });
    expect(stateUpdates).toHaveBeenCalled();
    const suspensionUpdate = stateUpdates.mock.calls.find(
      ([args]) => (args as { opts?: { status?: string } }).opts?.status === 'suspended',
    );
    expect(suspensionUpdate).toBeTruthy();
    expect(
      (
        suspensionUpdate![0] as {
          opts: { expectedExecutionGeneration?: string };
        }
      ).opts.expectedExecutionGeneration,
    ).toBe(generation);
    expect(engineEvents).not.toContain('workflow.suspend');
  });

  it('stops a top-level step-end when the run row was deleted instead of advancing it', async () => {
    const storage = new MockStore();
    const pubsub = new EventEmitterPubSub();
    const step = createStep({
      id: 'deleted-run-step',
      inputSchema: z.object({}),
      outputSchema: z.object({ value: z.string() }),
      execute: async () => ({ value: 'done' }),
    });
    const workflow = createWorkflow({
      id: `deleted-run-${Math.random().toString(36).slice(2)}`,
      inputSchema: z.object({}),
      outputSchema: z.object({ value: z.string() }),
    })
      .then(step)
      .commit();
    const mastra = new Mastra({ logger: false, storage, pubsub, workflows: { [workflow.id]: workflow } });
    const workflowsStore = (await storage.getStore('workflows'))!;

    const runId = `run-${Math.random().toString(36).slice(2)}`;
    await workflow.createRun({ runId });
    const pending = (await workflowsStore.loadWorkflowSnapshot({ workflowName: workflow.id, runId }))!;
    const generation = pending.executionGeneration!;
    expect(generation).toBeTruthy();

    // The delayed step-end arrives after the run row was deleted but before
    // any replacement exists — the store can only answer `{}`.
    await workflowsStore.deleteWorkflowRunById({ workflowName: workflow.id, runId });

    const engineEvents: string[] = [];
    const publish = pubsub.publish.bind(pubsub);
    vi.spyOn(pubsub, 'publish').mockImplementation(async (topic, event) => {
      if (topic === 'workflows') engineEvents.push((event as { type: string }).type);
      return publish(topic, event);
    });

    const processor = new WorkflowEventProcessor({ mastra });
    const handled = await processor.handle({
      type: 'workflow.step.end',
      id: 'evt-deleted-run',
      runId,
      createdAt: new Date(),
      data: {
        workflowId: workflow.id,
        runId,
        executionGeneration: generation,
        executionPath: [0],
        prevResult: {
          status: 'success',
          output: { value: 'done' },
          payload: {},
          startedAt: 1,
          endedAt: 2,
        },
        stepResults: {},
        activeStepsPath: {},
        resumeSteps: [],
        requestContext: {},
        lifecycleStepStates: {},
      },
    });

    // A top-level evented run always persists its initial row, so the `{}`
    // response means this lifetime was deleted: the handler stops rather than
    // publishing workflow.step.run for a run that no longer exists.
    expect(handled).toEqual({ ok: true });
    expect(engineEvents).not.toContain('workflow.step.run');
  });

  it('halts the suspension flow when the prune re-persist loses the generation CAS', async () => {
    const storage = new MockStore();
    const pubsub = new EventEmitterPubSub();
    const suspendingStep = createStep({
      id: 'pruned-suspend-step',
      inputSchema: z.object({}),
      suspendSchema: z.object({ reason: z.string() }),
      outputSchema: z.object({ value: z.string() }),
      execute: async () => ({ value: 'done' }),
    });
    const workflow = createWorkflow({
      id: `pruned-suspend-${Math.random().toString(36).slice(2)}`,
      inputSchema: z.object({}),
      outputSchema: z.object({ value: z.string() }),
      options: {
        pruneSnapshot: ({ snapshot }) => {
          const { serializedStepGraph: _graph, ...pruned } = snapshot;
          return pruned;
        },
      },
    })
      .then(suspendingStep)
      .commit();
    const mastra = new Mastra({ logger: false, storage, pubsub, workflows: { [workflow.id]: workflow } });
    const workflowsStore = (await storage.getStore('workflows'))!;

    const runId = `run-${Math.random().toString(36).slice(2)}`;
    await workflow.createRun({ runId });
    const pending = (await workflowsStore.loadWorkflowSnapshot({ workflowName: workflow.id, runId }))!;
    const generation = pending.executionGeneration!;
    expect(generation).toBeTruthy();

    // Simulate the reopen landing between the prune's snapshot read and its
    // re-persist: the guarded write must be rejected and the handler must
    // stop before publishing the stale suspension.
    const persistWorkflowSnapshot = workflowsStore.persistWorkflowSnapshot.bind(workflowsStore);
    const persistSpy = vi.spyOn(workflowsStore, 'persistWorkflowSnapshot').mockImplementation(async args => {
      if (args.expectedExecutionGeneration !== undefined) {
        await workflowsStore.deleteWorkflowRunById({ workflowName: args.workflowName, runId: args.runId });
      }
      return persistWorkflowSnapshot(args);
    });
    const engineEvents: string[] = [];
    const publish = pubsub.publish.bind(pubsub);
    vi.spyOn(pubsub, 'publish').mockImplementation(async (topic, event) => {
      if (topic === 'workflows') engineEvents.push((event as { type: string }).type);
      return publish(topic, event);
    });

    const processor = new WorkflowEventProcessor({ mastra });
    const handled = await processor.handle({
      type: 'workflow.step.end',
      id: 'evt-prune-race',
      runId,
      createdAt: new Date(),
      data: {
        workflowId: workflow.id,
        runId,
        executionGeneration: generation,
        executionPath: [0],
        prevResult: {
          status: 'suspended',
          output: undefined,
          payload: {},
          startedAt: 1,
          suspendPayload: { reason: 'waiting' },
        },
        stepResults: {},
        activeStepsPath: {},
        resumeSteps: [],
        requestContext: {},
        lifecycleStepStates: {},
      },
    });

    expect(handled).toEqual({ ok: true });
    const guardedPersist = persistSpy.mock.calls.find(([args]) => args.expectedExecutionGeneration !== undefined);
    expect(guardedPersist).toBeTruthy();
    expect(guardedPersist![0].expectedExecutionGeneration).toBe(generation);
    // The CAS rejected the stale re-persist, the reopened/absent row was not
    // overwritten, and no stale suspension was published.
    expect(engineEvents).not.toContain('workflow.suspend');
    await expect(workflowsStore.loadWorkflowSnapshot({ workflowName: workflow.id, runId })).resolves.toBeNull();
  });

  it('publishes workflow.suspend for a transient nested run whose guarded state write finds no row', async () => {
    const storage = new MockStore();
    const pubsub = new EventEmitterPubSub();
    const suspendingStep = createStep({
      id: 'transient-suspend-step',
      inputSchema: z.object({}),
      suspendSchema: z.object({ reason: z.string() }),
      outputSchema: z.object({ value: z.string() }),
      execute: async () => ({ value: 'done' }),
    });
    const workflow = createWorkflow({
      id: `transient-suspend-${Math.random().toString(36).slice(2)}`,
      inputSchema: z.object({}),
      outputSchema: z.object({ value: z.string() }),
      // The CHILD's own persistence option is what makes this run transient:
      // a durable parent's flag must not be consulted for this decision.
      options: { shouldPersistSnapshot: () => false },
      steps: [suspendingStep],
    })
      .then(suspendingStep)
      .commit();
    const outerWorkflow = createWorkflow({
      id: `transient-outer-${Math.random().toString(36).slice(2)}`,
      inputSchema: z.object({}),
      outputSchema: z.object({ value: z.string() }),
      steps: [workflow],
    })
      .then(workflow)
      .commit();
    const mastra = new Mastra({
      logger: false,
      storage,
      pubsub,
      workflows: { [workflow.id]: workflow, [outerWorkflow.id]: outerWorkflow },
    });
    const workflowsStore = (await storage.getStore('workflows'))!;

    // The nested run is transient via the CHILD's own shouldPersistSnapshot:
    // no snapshot row exists, so updateWorkflowResults answers the {}
    // missing-record fallback and updateWorkflowState finds no row. The
    // durable parent below must not flip that decision — this is the
    // durable-parent/transient-child combination that used to halt here.
    const runId = `run-${Math.random().toString(36).slice(2)}`;
    const resultWrites = vi.spyOn(workflowsStore, 'updateWorkflowResults');
    const stateUpdates = vi.spyOn(workflowsStore, 'updateWorkflowState');
    const engineEvents: string[] = [];
    const publish = pubsub.publish.bind(pubsub);
    vi.spyOn(pubsub, 'publish').mockImplementation(async (topic, event) => {
      if (topic === 'workflows') engineEvents.push((event as { type: string }).type);
      return publish(topic, event);
    });

    const processor = new WorkflowEventProcessor({ mastra });
    const handled = await processor.handle({
      type: 'workflow.step.end',
      id: 'evt-transient-suspend',
      runId,
      createdAt: new Date(),
      data: {
        workflowId: workflow.id,
        runId,
        executionGeneration: 'wfeg:transient-nested',
        executionPath: [0],
        prevResult: {
          status: 'suspended',
          output: undefined,
          payload: {},
          startedAt: 1,
          suspendPayload: { reason: 'waiting' },
        },
        stepResults: {},
        activeStepsPath: {},
        resumeSteps: [],
        requestContext: {},
        lifecycleStepStates: {},
        parentWorkflow: {
          workflowId: outerWorkflow.id,
          runId: 'outer-run',
          executionGeneration: 'wfeg:outer',
          lifecycleResumeAttempt: 0,
          lifecycleStepStates: {},
          executionPath: [0],
          resume: false,
          stepResults: {},
          stepId: 'nested-workflow-step',
          stepGraph: [],
          activeStepsPath: {},
          resumeSteps: [],
          resumeData: undefined,
          input: {},
          shouldPersistSnapshot: true,
        },
      },
    });

    expect(handled).toEqual({ ok: true });
    // A genuinely transient child writes nothing durable — no `__state`
    // result write and no updateWorkflowState — yet its suspension still
    // publishes workflow.suspend so the durable parent keeps advancing.
    const stateWrite = resultWrites.mock.calls.find(([args]) => (args as { stepId?: string }).stepId === '__state');
    expect(stateWrite).toBeUndefined();
    expect(stateUpdates).not.toHaveBeenCalled();
    expect(engineEvents).toContain('workflow.suspend');
  });

  it('holds step lifecycle publications until the fenced result write lands', async () => {
    const storage = new MockStore();
    const pubsub = new EventEmitterPubSub();
    const step = createStep({
      id: 'post-cas-publish-step',
      inputSchema: z.object({}),
      outputSchema: z.object({ value: z.string() }),
      execute: async () => ({ value: 'done' }),
    });
    const workflow = createWorkflow({
      id: `post-cas-publish-${Math.random().toString(36).slice(2)}`,
      inputSchema: z.object({}),
      outputSchema: z.object({ value: z.string() }),
    })
      .then(step)
      .commit();
    const mastra = new Mastra({ logger: false, storage, pubsub, workflows: { [workflow.id]: workflow } });
    const workflowsStore = (await storage.getStore('workflows'))!;

    const runId = `run-${Math.random().toString(36).slice(2)}`;
    await workflow.createRun({ runId });
    const pending = (await workflowsStore.loadWorkflowSnapshot({ workflowName: workflow.id, runId }))!;
    const generation = pending.executionGeneration!;
    expect(generation).toBeTruthy();

    // The projection preflight passes (the event's generation still matches),
    // but the reopen lands before the result write: the fenced write rejects
    // and no lifecycle event may have announced the step beforehand.
    const resultWrites = vi.spyOn(workflowsStore, 'updateWorkflowResults').mockResolvedValue(STALE_EXECUTION_RESULT);
    const lifecycleEvents: string[] = [];
    const engineEvents: string[] = [];
    const publish = pubsub.publish.bind(pubsub);
    vi.spyOn(pubsub, 'publish').mockImplementation(async (topic, event) => {
      const type = (event as { type: string }).type;
      if (type === 'workflow.lifecycle') lifecycleEvents.push(type);
      if (topic === 'workflows') engineEvents.push(type);
      return publish(topic, event);
    });

    const processor = new WorkflowEventProcessor({ mastra });
    const handled = await processor.handle({
      type: 'workflow.step.end',
      id: 'evt-post-cas-publish',
      runId,
      createdAt: new Date(),
      data: {
        workflowId: workflow.id,
        runId,
        executionGeneration: generation,
        executionPath: [0],
        prevResult: {
          status: 'success',
          output: { value: 'done' },
          payload: {},
          startedAt: 1,
          endedAt: 2,
        },
        stepResults: {},
        activeStepsPath: {},
        resumeSteps: [],
        requestContext: {},
        lifecycleStepStates: {},
      },
    });

    expect(handled).toEqual({ ok: true });
    expect(resultWrites).toHaveBeenCalledTimes(1);
    expect(lifecycleEvents).toEqual([]);
    expect(engineEvents).toEqual([]);
  });

  it('stops a top-level foreach delivery when the run row was deleted', async () => {
    const storage = new MockStore();
    const pubsub = new EventEmitterPubSub();
    const body = createStep({
      id: 'deleted-foreach-body',
      inputSchema: z.number(),
      outputSchema: z.number(),
      execute: async ({ inputData }) => inputData * 2,
    });
    const workflow = createWorkflow({
      id: `deleted-foreach-${Math.random().toString(36).slice(2)}`,
      inputSchema: z.array(z.number()),
      outputSchema: z.array(z.number()),
    })
      .foreach(body)
      .commit();
    const mastra = new Mastra({ logger: false, storage, pubsub, workflows: { [workflow.id]: workflow } });
    const workflowsStore = (await storage.getStore('workflows'))!;

    const runId = `run-${Math.random().toString(36).slice(2)}`;
    await workflow.createRun({ runId });
    const pending = (await workflowsStore.loadWorkflowSnapshot({ workflowName: workflow.id, runId }))!;
    const generation = pending.executionGeneration!;
    expect(generation).toBeTruthy();

    // The delayed iteration completion arrives after the run row was deleted:
    // the store can only answer `{}`, which is terminal for a top-level run.
    await workflowsStore.deleteWorkflowRunById({ workflowName: workflow.id, runId });

    const engineEvents: string[] = [];
    const publish = pubsub.publish.bind(pubsub);
    vi.spyOn(pubsub, 'publish').mockImplementation(async (topic, event) => {
      if (topic === 'workflows') engineEvents.push((event as { type: string }).type);
      return publish(topic, event);
    });

    const processor = new WorkflowEventProcessor({ mastra });
    const handled = await processor.handle({
      type: 'workflow.step.end',
      id: 'evt-deleted-foreach',
      runId,
      createdAt: new Date(),
      data: {
        workflowId: workflow.id,
        runId,
        executionGeneration: generation,
        executionPath: [0, 0],
        prevResult: {
          status: 'success',
          output: 2,
          payload: 1,
          startedAt: 1,
          endedAt: 2,
        },
        stepResults: {},
        activeStepsPath: {},
        resumeSteps: [],
        requestContext: {},
        lifecycleStepStates: {},
      },
    });

    // The `{}` result write halted the handler before it could publish
    // another iteration's step.run/step.end for the deleted run.
    expect(handled).toEqual({ ok: true });
    expect(engineEvents).not.toContain('workflow.step.run');
    expect(engineEvents).not.toContain('workflow.step.end');
  });

  it('stops a durable nested run whose row is missing instead of publishing its suspension', async () => {
    const storage = new MockStore();
    const pubsub = new EventEmitterPubSub();
    const suspendingStep = createStep({
      id: 'durable-suspend-step',
      inputSchema: z.object({}),
      suspendSchema: z.object({ reason: z.string() }),
      outputSchema: z.object({ value: z.string() }),
      execute: async () => ({ value: 'done' }),
    });
    const workflow = createWorkflow({
      id: `durable-suspend-${Math.random().toString(36).slice(2)}`,
      inputSchema: z.object({}),
      outputSchema: z.object({ value: z.string() }),
      steps: [suspendingStep],
    })
      .then(suspendingStep)
      .commit();
    const outerWorkflow = createWorkflow({
      id: `durable-outer-${Math.random().toString(36).slice(2)}`,
      inputSchema: z.object({}),
      outputSchema: z.object({ value: z.string() }),
      steps: [workflow],
    })
      .then(workflow)
      .commit();
    const mastra = new Mastra({
      logger: false,
      storage,
      pubsub,
      workflows: { [workflow.id]: workflow, [outerWorkflow.id]: outerWorkflow },
    });
    const workflowsStore = (await storage.getStore('workflows'))!;

    // The child is durable (no shouldPersistSnapshot opt-out): its snapshot
    // row existed and was deleted before this delayed delivery arrived, so
    // the store answers the same `{}` a transient run legitimately produces.
    const runId = `run-${Math.random().toString(36).slice(2)}`;
    await workflow.createRun({ runId });
    await workflowsStore.deleteWorkflowRunById({ workflowName: workflow.id, runId });

    const engineEvents: string[] = [];
    const publish = pubsub.publish.bind(pubsub);
    vi.spyOn(pubsub, 'publish').mockImplementation(async (topic, event) => {
      if (topic === 'workflows') engineEvents.push((event as { type: string }).type);
      return publish(topic, event);
    });

    const processor = new WorkflowEventProcessor({ mastra });
    const handled = await processor.handle({
      type: 'workflow.step.end',
      id: 'evt-durable-suspend',
      runId,
      createdAt: new Date(),
      data: {
        workflowId: workflow.id,
        runId,
        executionGeneration: 'wfeg:durable-nested',
        executionPath: [0],
        prevResult: {
          status: 'suspended',
          output: undefined,
          payload: {},
          startedAt: 1,
          suspendPayload: { reason: 'waiting' },
        },
        stepResults: {},
        activeStepsPath: {},
        resumeSteps: [],
        requestContext: {},
        lifecycleStepStates: {},
        parentWorkflow: {
          workflowId: outerWorkflow.id,
          runId: 'outer-run',
          executionGeneration: 'wfeg:outer',
          lifecycleResumeAttempt: 0,
          lifecycleStepStates: {},
          executionPath: [0],
          resume: false,
          stepResults: {},
          stepId: 'nested-workflow-step',
          stepGraph: [],
          activeStepsPath: {},
          resumeSteps: [],
          resumeData: undefined,
          input: {},
          // The PARENT's persistence flag must not be consulted for the
          // child's missing-row decision: a transient parent cannot launder
          // a deleted durable child row into a legitimate opt-out.
          shouldPersistSnapshot: false,
        },
      },
    });

    // A durable child's missing row is a deletion, not a persistence opt-out:
    // no workflow.suspend may be published or forwarded to the parent.
    expect(handled).toEqual({ ok: true });
    expect(engineEvents).not.toContain('workflow.suspend');
  });

  it('halts the suspension flow on a stale-persist rejection from a foreign core instance', async () => {
    const storage = new MockStore();
    const pubsub = new EventEmitterPubSub();
    const suspendingStep = createStep({
      id: 'foreign-stale-step',
      inputSchema: z.object({}),
      suspendSchema: z.object({ reason: z.string() }),
      outputSchema: z.object({ value: z.string() }),
      execute: async () => ({ value: 'done' }),
    });
    const workflow = createWorkflow({
      id: `foreign-stale-${Math.random().toString(36).slice(2)}`,
      inputSchema: z.object({}),
      outputSchema: z.object({ value: z.string() }),
      options: {
        pruneSnapshot: ({ snapshot }) => {
          const { serializedStepGraph: _graph, ...pruned } = snapshot;
          return pruned;
        },
      },
    })
      .then(suspendingStep)
      .commit();
    const mastra = new Mastra({ logger: false, storage, pubsub, workflows: { [workflow.id]: workflow } });
    const workflowsStore = (await storage.getStore('workflows'))!;

    const runId = `run-${Math.random().toString(36).slice(2)}`;
    await workflow.createRun({ runId });
    const pending = (await workflowsStore.loadWorkflowSnapshot({ workflowName: workflow.id, runId }))!;
    const generation = pending.executionGeneration!;
    expect(generation).toBeTruthy();

    // A store adapter built against a second @mastra/core instance rejects
    // the guarded re-persist with its own copy of the error class: instanceof
    // fails here, but the stable code still marks the CAS rejection.
    const staleError = Object.assign(new TypeError('stale persist'), {
      code: 'WORKFLOW_SNAPSHOT_PERSIST_STALE_GENERATION',
      workflowName: workflow.id,
      runId,
    });
    vi.spyOn(workflowsStore, 'persistWorkflowSnapshot').mockRejectedValue(staleError);
    const engineEvents: string[] = [];
    const publish = pubsub.publish.bind(pubsub);
    vi.spyOn(pubsub, 'publish').mockImplementation(async (topic, event) => {
      if (topic === 'workflows') engineEvents.push((event as { type: string }).type);
      return publish(topic, event);
    });

    const processor = new WorkflowEventProcessor({ mastra });
    const handled = await processor.handle({
      type: 'workflow.step.end',
      id: 'evt-foreign-stale',
      runId,
      createdAt: new Date(),
      data: {
        workflowId: workflow.id,
        runId,
        executionGeneration: generation,
        executionPath: [0],
        prevResult: {
          status: 'suspended',
          output: undefined,
          payload: {},
          startedAt: 1,
          suspendPayload: { reason: 'waiting' },
        },
        stepResults: {},
        activeStepsPath: {},
        resumeSteps: [],
        requestContext: {},
        lifecycleStepStates: {},
      },
    });

    expect(handled).toEqual({ ok: true });
    expect(engineEvents).not.toContain('workflow.suspend');
  });
});
