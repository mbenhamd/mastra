/**
 * Tests for the cross-process workflow event guard in the push-subscription
 * callback inside `startWorkers()`.
 *
 * When two Mastra instances share a push-only pubsub (mimicking Unix socket
 * IPC between mc processes), events for internal workflows (execution-workflow,
 * agentic-loop) registered on one instance must NOT be processed by the other.
 * Without the guard the WEP would call errorWorkflow() → publish workflow.fail
 * → processWorkflowFail → workflows-finish, erroneously terminating the
 * correct instance's run.
 */
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { MockLanguageModelV2 } from '@internal/ai-sdk-v5/test';
import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';

import { Agent } from '../agent';
import { EventEmitterPubSub } from '../events/event-emitter';
import type { PubSubDeliveryMode } from '../events/pubsub';
import type { Event } from '../events/types';
import { UnixSocketPubSub } from '../events/unix-socket-pubsub';
import { Mastra } from '../mastra';
import { MockStore } from '../storage/mock';
import { createStep, createWorkflow } from '../workflows/evented';

/** Push-only wrapper — mimics mc's SignalsPubSub delivery semantics. */
class PushOnlyPubSub extends EventEmitterPubSub {
  override get supportedModes(): ReadonlyArray<PubSubDeliveryMode> {
    return ['push'];
  }
}

function makeStartEvent(workflowId: string, runId: string, internal = true): Event {
  return {
    type: 'workflow.start',
    runId,
    data: {
      workflowId,
      runId,
      executionPath: [0],
      stepResults: {},
      prevResult: { status: 'success', output: {} },
      activeSteps: {},
      requestContext: {},
      ...(internal ? { __mastraInternalWorkflow: true } : {}),
    },
  } as Event;
}

function makeNoopWorkflow(id: string) {
  const wf = createWorkflow({
    id,
    inputSchema: z.object({}),
    outputSchema: z.object({}),
  });
  wf.then(
    createStep({
      id: 'noop',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      execute: async () => ({}),
    }) as any,
  ).commit();
  return wf;
}

describe('cross-process workflow event guard', () => {
  it('registers real agent execution and loop workflows without test-only ownership setup', async () => {
    const agent = new Agent({
      id: 'owned-agent',
      name: 'Owned agent',
      instructions: 'Reply briefly.',
      model: new MockLanguageModelV2({
        doGenerate: async () => ({
          content: [{ type: 'text', text: 'owned' }],
          finishReason: 'stop',
          usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
          warnings: [],
        }),
      }),
    });
    const mastra = new Mastra({
      logger: false,
      storage: new MockStore(),
      pubsub: new PushOnlyPubSub(),
      agents: { ownedAgent: agent },
    });
    const register = vi.spyOn(mastra, '__registerInternalWorkflow');
    const unregister = vi.spyOn(mastra, '__unregisterInternalWorkflow');

    await mastra.startWorkers();
    await mastra.getAgent('ownedAgent').generate('hello');

    expect(register.mock.calls.some(([workflow, runId]) => workflow.id === 'execution-workflow' && !!runId)).toBe(true);
    expect(register.mock.calls.some(([workflow, runId]) => workflow.id === 'agentic-loop' && !!runId)).toBe(true);
    expect(unregister.mock.calls.some(([workflowId]) => workflowId === 'execution-workflow')).toBe(true);
    expect(unregister.mock.calls.some(([workflowId]) => workflowId === 'agentic-loop')).toBe(true);
    await mastra.shutdown();
  });

  it('skips events for internal workflows not owned by this instance', async () => {
    const sharedPubSub = new PushOnlyPubSub();

    // Instance A: owns the internal workflow
    const mastraA = new Mastra({
      logger: false,
      storage: new MockStore(),
      workflows: {} as any,
      pubsub: sharedPubSub,
    });

    // Instance B: does NOT own the workflow — simulates a different process
    const mastraB = new Mastra({
      logger: false,
      storage: new MockStore(),
      workflows: {} as any,
      pubsub: sharedPubSub,
    });

    mastraA.__registerInternalWorkflow(makeNoopWorkflow('execution-workflow') as any, 'run-1');

    await mastraA.startWorkers();
    await mastraB.startWorkers();

    const spyA = vi.spyOn(mastraA, 'handleWorkflowEvent');
    const spyB = vi.spyOn(mastraB, 'handleWorkflowEvent');

    await sharedPubSub.publish('workflows', makeStartEvent('execution-workflow', 'run-1'));
    await vi.waitFor(() => expect(spyA).toHaveBeenCalled(), { timeout: 1000, interval: 10 });
    // Instance B should NOT process any events (guard skips all of them)
    expect(spyB).not.toHaveBeenCalled();

    await mastraA.shutdown();
    await mastraB.shutdown();
  });

  it('still processes events for public workflows on all instances', async () => {
    const sharedPubSub = new PushOnlyPubSub();

    const publicWf = makeNoopWorkflow('my-public-workflow');

    // Both instances register the same public workflow
    const mastraA = new Mastra({
      logger: false,
      storage: new MockStore(),
      workflows: { myWorkflow: publicWf } as any,
      pubsub: sharedPubSub,
    });
    const mastraB = new Mastra({
      logger: false,
      storage: new MockStore(),
      workflows: { myWorkflow: publicWf } as any,
      pubsub: sharedPubSub,
    });

    await mastraA.startWorkers();
    await mastraB.startWorkers();

    const spyA = vi.spyOn(mastraA, 'handleWorkflowEvent');
    const spyB = vi.spyOn(mastraB, 'handleWorkflowEvent');

    await sharedPubSub.publish('workflows', makeStartEvent('my-public-workflow', 'run-pub', false));
    await vi.waitFor(
      () => {
        expect(spyA).toHaveBeenCalled();
        expect(spyB).toHaveBeenCalled();
      },
      { timeout: 1000, interval: 10 },
    );

    await mastraA.shutdown();
    await mastraB.shutdown();
  });

  it('still processes nested events whose public root is registered on each instance', async () => {
    const sharedPubSub = new PushOnlyPubSub();
    const mastraA = new Mastra({
      logger: false,
      storage: new MockStore(),
      workflows: { publicRoot: makeNoopWorkflow('public-root') } as any,
      pubsub: sharedPubSub,
    });
    const mastraB = new Mastra({
      logger: false,
      storage: new MockStore(),
      workflows: { publicRoot: makeNoopWorkflow('public-root') } as any,
      pubsub: sharedPubSub,
    });
    await mastraA.startWorkers();
    await mastraB.startWorkers();
    const spyA = vi.spyOn(mastraA, 'handleWorkflowEvent');
    const spyB = vi.spyOn(mastraB, 'handleWorkflowEvent');

    await sharedPubSub.publish('workflows', {
      ...makeStartEvent('nested-child', 'nested-run', false),
      data: {
        ...makeStartEvent('nested-child', 'nested-run', false).data,
        parentWorkflow: {
          workflowId: 'public-root',
          runId: 'public-run',
          executionPath: [0],
          stepResults: {},
          stepGraph: [],
        },
      },
    } as Event);

    await vi.waitFor(() => {
      expect(spyA).toHaveBeenCalled();
      expect(spyB).toHaveBeenCalled();
    });
    await mastraA.shutdown();
    await mastraB.shutdown();
  });

  it('does not produce workflow.fail when only one instance owns the internal workflow', async () => {
    const sharedPubSub = new PushOnlyPubSub();

    const mastraOwner = new Mastra({
      logger: false,
      storage: new MockStore(),
      workflows: {} as any,
      pubsub: sharedPubSub,
    });
    const mastraOther = new Mastra({
      logger: false,
      storage: new MockStore(),
      workflows: {} as any,
      pubsub: sharedPubSub,
    });

    mastraOwner.__registerInternalWorkflow(makeNoopWorkflow('execution-workflow') as any, 'run-2');

    await mastraOwner.startWorkers();
    await mastraOther.startWorkers();

    // Collect all workflow.fail events on the shared pubsub
    const failEvents: Event[] = [];
    await sharedPubSub.subscribe('workflows', async event => {
      if (event.type === 'workflow.fail') failEvents.push(event);
    });

    await sharedPubSub.publish('workflows', makeStartEvent('execution-workflow', 'run-2'));
    // Allow async event processing to settle
    await new Promise(r => setTimeout(r, 50));
    // The non-owning instance should NOT have caused a workflow.fail
    expect(failEvents).toHaveLength(0);

    await mastraOwner.shutdown();
    await mastraOther.shutdown();
  });

  it('skips nested workflow events whose root parent belongs to a different instance', async () => {
    const sharedPubSub = new PushOnlyPubSub();

    const mastraOwner = new Mastra({
      logger: false,
      storage: new MockStore(),
      workflows: {} as any,
      pubsub: sharedPubSub,
    });
    const mastraOther = new Mastra({
      logger: false,
      storage: new MockStore(),
      workflows: {} as any,
      pubsub: sharedPubSub,
    });

    mastraOwner.__registerInternalWorkflow(makeNoopWorkflow('execution-workflow') as any, 'run-owner');

    await mastraOwner.startWorkers();
    await mastraOther.startWorkers();

    const spyOwner = vi.spyOn(mastraOwner, 'handleWorkflowEvent');
    const spyOther = vi.spyOn(mastraOther, 'handleWorkflowEvent');

    // Simulate a nested workflow event (agentic-loop inside execution-workflow)
    // whose root parentWorkflow points to owner's execution-workflow run.
    const nestedEvent: Event = {
      type: 'workflow.step.run',
      runId: 'nested-run-1',
      data: {
        workflowId: 'agentic-loop',
        runId: 'nested-run-1',
        executionPath: [0],
        stepResults: {},
        prevResult: { status: 'success', output: {} },
        activeSteps: {},
        requestContext: {},
        __mastraInternalWorkflow: true,
        parentWorkflow: {
          workflowId: 'execution-workflow',
          runId: 'run-owner',
          executionPath: [0],
          stepResults: {},
          resume: false,
          stepId: 'stream',
          stepGraph: [],
        },
      },
    } as Event;

    await sharedPubSub.publish('workflows', nestedEvent);
    await vi.waitFor(() => expect(spyOwner).toHaveBeenCalled(), { timeout: 1000, interval: 10 });
    // The non-owning instance must NOT process nested events
    expect(spyOther).not.toHaveBeenCalled();

    await mastraOwner.shutdown();
    await mastraOther.shutdown();
  });

  it('skips events where runId does not match any registered internal workflow', async () => {
    const sharedPubSub = new PushOnlyPubSub();

    const mastra = new Mastra({
      logger: false,
      storage: new MockStore(),
      workflows: {} as any,
      pubsub: sharedPubSub,
    });

    mastra.__registerInternalWorkflow(makeNoopWorkflow('execution-workflow') as any, 'run-A');

    await mastra.startWorkers();

    const spy = vi.spyOn(mastra, 'handleWorkflowEvent');

    // Event for run-A: should be processed (owned). Wait for the terminal
    // workflow.end event so all cascading events have been delivered before
    // we clear the spy for the second assertion.
    await sharedPubSub.publish('workflows', makeStartEvent('execution-workflow', 'run-A'));
    await vi.waitFor(
      () => {
        const calls = spy.mock.calls.flat();
        expect(calls.some((c: any) => c?.type === 'workflow.end' && c?.data?.runId === 'run-A')).toBe(true);
      },
      { timeout: 2000, interval: 10 },
    );

    spy.mockClear();

    // Event for run-B: should be skipped (not owned — different runId)
    await sharedPubSub.publish('workflows', makeStartEvent('execution-workflow', 'run-B'));
    // Allow async event processing to settle
    await new Promise(r => setTimeout(r, 100));
    expect(spy).not.toHaveBeenCalled();

    await mastra.shutdown();
  });

  it('logs only structural ownership fields when a foreign event is skipped', async () => {
    const sharedPubSub = new PushOnlyPubSub();
    const debug = vi.fn();
    const mastra = new Mastra({
      logger: {
        debug,
        info: vi.fn(),
        warn: vi.fn(),
        error: vi.fn(),
        trackException: vi.fn(),
      } as any,
      storage: new MockStore(),
      workflows: {} as any,
      pubsub: sharedPubSub,
    });
    await mastra.startWorkers();
    const event = makeStartEvent('foreign-workflow', 'foreign-run');
    (event.data as Record<string, unknown>).payload = { secret: 'must-not-enter-logs' };

    await sharedPubSub.publish('workflows', event);

    await vi.waitFor(() => {
      expect(debug).toHaveBeenCalledWith('Skipping workflow event not owned by this Mastra instance', {
        workflowId: 'foreign-workflow',
        eventType: 'workflow.start',
        ownership: 'foreign',
      });
    });
    expect(JSON.stringify(debug.mock.calls)).not.toContain('must-not-enter-logs');
    await mastra.shutdown();
  });
});

/**
 * Tests for the `mastra.pubsub` proxy's `localOnly` tagging logic. The proxy
 * decides which publishes can short-circuit the broker round-trip because
 * only the publishing process consumes them. Getting this wrong causes
 * cumulative `workflow.step.end` payloads (often 9 MB+) to be serialised
 * across the unix socket on every event — manifesting as ECANCELED,
 * `condition is not a function`, or missing `getFullOutput`.
 */
describe('mastra.pubsub proxy localOnly tagging', () => {
  /**
   * Wraps an EventEmitterPubSub so we can assert which calls received
   * `localOnly: true` in their options.
   */
  class RecordingPushOnlyPubSub extends PushOnlyPubSub {
    calls: Array<{ topic: string; event: Event; localOnly: boolean }> = [];
    override async publish(
      topic: string,
      event: Omit<Event, 'id' | 'createdAt'>,
      options?: { localOnly?: boolean },
    ): Promise<void> {
      this.calls.push({ topic, event: event as Event, localOnly: Boolean(options?.localOnly) });
      await super.publish(topic, event, options);
    }
  }

  class RecordingUnixSocketPubSub extends UnixSocketPubSub {
    calls: Array<{ topic: string; event: Event; localOnly: boolean }> = [];

    override async publish(
      topic: string,
      event: Omit<Event, 'id' | 'createdAt'>,
      options?: { localOnly?: boolean },
    ): Promise<void> {
      this.calls.push({ topic, event: event as Event, localOnly: Boolean(options?.localOnly) });
      await super.publish(topic, event, options);
    }
  }

  async function waitForHandledEvent(
    spy: ReturnType<typeof vi.spyOn>,
    type: Event['type'],
    runId: string,
  ): Promise<void> {
    let result: Promise<unknown> | undefined;
    await vi.waitFor(() => {
      const index = spy.mock.calls.findIndex(
        ([event]) => (event as Event).type === type && (event as Event).runId === runId,
      );
      expect(index).toBeGreaterThanOrEqual(0);
      result = spy.mock.results[index]?.value as Promise<unknown> | undefined;
      expect(result).toBeDefined();
    });
    await result;
  }

  function makeStepRunEvent(
    workflowId: string,
    runId: string,
    parentWorkflow?: { workflowId: string; runId: string; parentWorkflow?: unknown },
  ): Omit<Event, 'id' | 'createdAt'> {
    return {
      type: 'workflow.step.run',
      runId,
      data: {
        workflowId,
        runId,
        executionPath: [0],
        stepResults: {},
        prevResult: { status: 'success', output: {} },
        activeSteps: {},
        requestContext: {},
        ...(parentWorkflow ? { parentWorkflow } : {}),
      },
    } as Event;
  }

  it('tags internal workflow publishes as localOnly', async () => {
    const pubsub = new RecordingPushOnlyPubSub();
    const mastra = new Mastra({ logger: false, storage: new MockStore(), workflows: {} as any, pubsub });
    mastra.__registerInternalWorkflow(makeNoopWorkflow('execution-workflow') as any, 'run-1');

    await mastra.pubsub.publish('workflows', makeStartEvent('execution-workflow', 'run-1'));

    expect(pubsub.calls).toHaveLength(1);
    expect(pubsub.calls[0]).toMatchObject({ topic: 'workflows', localOnly: true });
    await mastra.shutdown();
  });

  it('does NOT tag publishes for workflows owned by no one as localOnly', async () => {
    const pubsub = new RecordingPushOnlyPubSub();
    const mastra = new Mastra({ logger: false, storage: new MockStore(), workflows: {} as any, pubsub });

    await mastra.pubsub.publish('workflows', makeStartEvent('foreign-workflow', 'run-foreign'));

    expect(pubsub.calls).toHaveLength(1);
    expect(pubsub.calls[0]!.localOnly).toBe(false);
    await mastra.shutdown();
  });

  it('walks parentWorkflow chain so nested workflow events inherit the root owner', async () => {
    const pubsub = new RecordingPushOnlyPubSub();
    const mastra = new Mastra({ logger: false, storage: new MockStore(), workflows: {} as any, pubsub });
    // Owner registers the root agentic-loop run.
    mastra.__registerInternalWorkflow(makeNoopWorkflow('agentic-loop') as any, 'root-run');

    // The nested `executionWorkflow` step is NOT registered itself, but its
    // parent chain points to the registered agentic-loop.
    const nestedEvent = makeStepRunEvent('executionWorkflow', 'nested-run', {
      workflowId: 'agentic-loop',
      runId: 'root-run',
    });
    await mastra.pubsub.publish('workflows', nestedEvent);

    expect(pubsub.calls).toHaveLength(1);
    expect(pubsub.calls[0]!.localOnly).toBe(true);

    await mastra.shutdown();
  });

  it('walks parentWorkflow chain across multiple levels of nesting', async () => {
    const pubsub = new RecordingPushOnlyPubSub();
    const mastra = new Mastra({ logger: false, storage: new MockStore(), workflows: {} as any, pubsub });
    mastra.__registerInternalWorkflow(makeNoopWorkflow('execution-workflow') as any, 'root-run');

    const deeplyNested = makeStepRunEvent('innerWorkflow', 'inner-run', {
      workflowId: 'agentic-loop',
      runId: 'middle-run',
      parentWorkflow: {
        workflowId: 'execution-workflow',
        runId: 'root-run',
      },
    });
    await mastra.pubsub.publish('workflows', deeplyNested);

    expect(pubsub.calls[0]!.localOnly).toBe(true);

    await mastra.shutdown();
  });

  it('keeps real scheduled public workflow starts portable', async () => {
    const pubsub = new RecordingPushOnlyPubSub();
    const storage = new MockStore();
    const mastra = new Mastra({
      logger: false,
      storage,
      workflows: { scheduled: makeNoopWorkflow('scheduled') } as any,
      scheduler: { enabled: true, tickIntervalMs: 600_000 },
      pubsub,
    });
    await mastra.startWorkers();
    await vi.waitFor(() => expect(mastra.scheduler).toBeDefined());
    const schedules = (await storage.getStore('schedules'))!;
    const now = Date.now();
    await schedules.createSchedule({
      id: 'real-scheduler-route',
      target: { type: 'workflow', workflowId: 'scheduled' },
      cron: '0 0 1 1 *',
      status: 'active',
      nextFireAt: now - 1,
      createdAt: now,
      updatedAt: now,
    });

    await mastra.scheduler!.tick();
    expect(pubsub.calls).toContainEqual(expect.objectContaining({ topic: 'workflows', localOnly: false }));
    await mastra.shutdown();
  });

  it('keeps public workflow stream events portable', async () => {
    const pubsub = new RecordingPushOnlyPubSub();
    const mastra = new Mastra({ logger: false, storage: new MockStore(), workflows: {} as any, pubsub });

    await mastra.pubsub.publish('workflow.events.v2.run-xyz', {
      type: 'watch',
      runId: 'run-xyz',
      data: { chunk: 'whatever' },
    } as unknown as Event);

    expect(pubsub.calls[0]!.localOnly).toBe(false);
    await mastra.shutdown();
  });

  it('tags stream events for owned internal runs as localOnly', async () => {
    const pubsub = new RecordingPushOnlyPubSub();
    const mastra = new Mastra({ logger: false, storage: new MockStore(), workflows: {} as any, pubsub });
    mastra.__registerInternalWorkflow(makeNoopWorkflow('agentic-loop') as any, 'run-xyz');

    await mastra.pubsub.publish('workflow.events.v2.run-xyz', {
      type: 'watch',
      runId: 'run-xyz',
      data: { chunk: 'whatever' },
    } as unknown as Event);

    expect(pubsub.calls[0]!.localOnly).toBe(true);

    await mastra.shutdown();
  });

  it('keeps streams local for unscoped and parent-owned internal runs', async () => {
    const pubsub = new RecordingPushOnlyPubSub();
    const mastra = new Mastra({ logger: false, storage: new MockStore(), workflows: {} as any, pubsub });
    mastra.__registerInternalWorkflow(makeNoopWorkflow('__background-task') as any);
    mastra.__registerInternalWorkflow(makeNoopWorkflow('agentic-loop') as any, 'root-run');

    await mastra.pubsub.publish('workflows', makeStartEvent('__background-task', 'task-run'));
    await mastra.pubsub.publish(
      'workflows',
      makeStepRunEvent('nested-internal', 'nested-run', { workflowId: 'agentic-loop', runId: 'root-run' }),
    );
    await mastra.pubsub.publish('workflow.events.v2.task-run', {
      type: 'watch',
      runId: 'task-run',
      data: { chunk: 'task' },
    } as unknown as Event);
    await mastra.pubsub.publish('workflow.events.v2.nested-run', {
      type: 'watch',
      runId: 'nested-run',
      data: { chunk: 'nested' },
    } as unknown as Event);

    const streamCalls = pubsub.calls.filter(call => call.topic.startsWith('workflow.events.v2.'));
    expect(streamCalls).toHaveLength(2);
    expect(streamCalls.every(call => call.localOnly)).toBe(true);

    await mastra.shutdown();
  });

  it('tags workflows-finish events for owned runs as localOnly', async () => {
    const pubsub = new RecordingPushOnlyPubSub();
    const mastra = new Mastra({ logger: false, storage: new MockStore(), workflows: {} as any, pubsub });
    mastra.__registerInternalWorkflow(makeNoopWorkflow('execution-workflow') as any, 'run-finish');

    await mastra.pubsub.publish('workflows-finish', {
      type: 'workflow.end',
      runId: 'run-finish',
      data: { workflowId: 'execution-workflow', runId: 'run-finish' },
    } as Event);

    expect(pubsub.calls[0]!.localOnly).toBe(true);

    await mastra.pubsub.publish('workflow.events.v2.run-finish', {
      type: 'watch',
      runId: 'run-finish',
      data: { type: 'late-before-semantic-completion' },
    } as unknown as Event);
    expect(pubsub.calls[1]!.localOnly).toBe(true);
    await mastra.shutdown();
  });

  it.each([
    ['first', 'second'],
    ['second', 'first'],
  ])('keeps a shared run local after unregistering %s until %s also unregisters', async (firstId, secondId) => {
    const pubsub = new RecordingPushOnlyPubSub();
    const mastra = new Mastra({ logger: false, storage: new MockStore(), workflows: {} as any, pubsub });
    mastra.__registerInternalWorkflow(makeNoopWorkflow(firstId) as any, 'shared-run');
    mastra.__registerInternalWorkflow(makeNoopWorkflow(secondId) as any, 'shared-run');

    mastra.__unregisterInternalWorkflow(firstId, 'shared-run');
    await mastra.pubsub.publish('workflow.events.v2.shared-run', {
      type: 'watch',
      runId: 'shared-run',
      data: { type: 'still-owned' },
    } as unknown as Event);
    expect(pubsub.calls.at(-1)!.localOnly).toBe(true);

    mastra.__unregisterInternalWorkflow(secondId, 'shared-run');
    await mastra.pubsub.publish('workflow.events.v2.shared-run', {
      type: 'watch',
      runId: 'shared-run',
      data: { type: 'released' },
    } as unknown as Event);
    expect(pubsub.calls.at(-1)!.localOnly).toBe(false);
    await mastra.shutdown();
  });

  it('keeps a still-active internal run local beyond the former registration-age TTL', async () => {
    vi.useFakeTimers();
    try {
      const pubsub = new RecordingPushOnlyPubSub();
      const mastra = new Mastra({ logger: false, storage: new MockStore(), workflows: {} as any, pubsub });
      mastra.__registerInternalWorkflow(makeNoopWorkflow('long-running') as any, 'long-run');

      vi.advanceTimersByTime(30 * 60 * 1000 + 1);
      mastra.__registerInternalWorkflow(makeNoopWorkflow('unrelated') as any, 'other-run');
      await mastra.pubsub.publish('workflow.events.v2.long-run', {
        type: 'watch',
        runId: 'long-run',
        data: { type: 'still-active' },
      } as unknown as Event);

      expect(pubsub.calls.at(-1)!.localOnly).toBe(true);
      expect(mastra.__hasInternalWorkflow('long-running', 'long-run')).toBe(true);
      await mastra.shutdown();
    } finally {
      vi.useRealTimers();
    }
  });

  it('keeps cancel output local until semantic end handling completes, then releases the run', async () => {
    const pubsub = new RecordingPushOnlyPubSub();
    const mastra = new Mastra({ logger: false, storage: new MockStore(), workflows: {} as any, pubsub });
    mastra.__registerInternalWorkflow(makeNoopWorkflow('cancel-workflow') as any, 'cancel-run');
    await mastra.startWorkers();
    const handle = vi.spyOn(mastra, 'handleWorkflowEvent');

    await mastra.pubsub.publish('workflows', {
      ...makeStartEvent('cancel-workflow', 'cancel-run'),
      type: 'workflow.cancel',
    } as Event);
    await waitForHandledEvent(handle, 'workflow.end', 'cancel-run');

    const internalCalls = pubsub.calls.filter(call =>
      ['workflows', 'workflows-finish', 'workflow.events.v2.cancel-run'].includes(call.topic),
    );
    expect(internalCalls.some(call => call.topic === 'workflow.events.v2.cancel-run')).toBe(true);
    expect(internalCalls.some(call => call.topic === 'workflows-finish')).toBe(true);
    expect(internalCalls.every(call => call.localOnly)).toBe(true);

    await mastra.pubsub.publish('workflow.events.v2.cancel-run', {
      type: 'watch',
      runId: 'cancel-run',
      data: { type: 'late-after-end' },
    } as unknown as Event);
    expect(pubsub.calls.at(-1)!.localOnly).toBe(false);
    await mastra.shutdown();
  });

  it('retains ownership when a failure event is skipped after cancellation persisted', async () => {
    const pubsub = new RecordingPushOnlyPubSub();
    const storage = new MockStore();
    const mastra = new Mastra({ logger: false, storage, workflows: {} as any, pubsub });
    mastra.__registerInternalWorkflow(makeNoopWorkflow('cancel-race') as any, 'cancel-race-run');
    const workflows = await storage.getStore('workflows');
    await workflows?.persistWorkflowSnapshot({
      workflowName: 'cancel-race',
      runId: 'cancel-race-run',
      snapshot: {
        runId: 'cancel-race-run',
        status: 'canceled',
        value: {},
        context: {},
        serializedStepGraph: [],
        activePaths: [],
        activeStepsPath: {},
        suspendedPaths: {},
        resumeLabels: {},
        waitingPaths: {},
        timestamp: Date.now(),
      },
    });

    const handled = await mastra.handleWorkflowEvent({
      ...makeStartEvent('cancel-race', 'cancel-race-run'),
      type: 'workflow.fail',
      data: {
        ...makeStartEvent('cancel-race', 'cancel-race-run').data,
        prevResult: { status: 'failed', error: new Error('concurrent failure') },
      },
    } as Event);
    expect(handled).toEqual({ ok: true });

    await mastra.pubsub.publish('workflow.events.v2.cancel-race-run', {
      type: 'watch',
      runId: 'cancel-race-run',
      data: { type: 'cancel-handler-still-emitting' },
    } as unknown as Event);
    expect(pubsub.calls.at(-1)!.localOnly).toBe(true);
    await mastra.shutdown();
  });

  it('keeps output local while concurrent failure and cancellation handlers drain', async () => {
    const pubsub = new RecordingPushOnlyPubSub();
    const mastra = new Mastra({ logger: false, storage: new MockStore(), workflows: {} as any, pubsub });
    mastra.__registerInternalWorkflow(makeNoopWorkflow('terminal-race') as any, 'terminal-race-run');
    const cancelEvent = {
      ...makeStartEvent('terminal-race', 'terminal-race-run'),
      type: 'workflow.cancel',
    } as Event;
    const failEvent = {
      ...makeStartEvent('terminal-race', 'terminal-race-run'),
      type: 'workflow.fail',
      data: {
        ...makeStartEvent('terminal-race', 'terminal-race-run').data,
        prevResult: { status: 'failed', error: new Error('concurrent failure') },
      },
    } as Event;

    await Promise.all([mastra.handleWorkflowEvent(failEvent), mastra.handleWorkflowEvent(cancelEvent)]);

    const streamCalls = pubsub.calls.filter(call => call.topic === 'workflow.events.v2.terminal-race-run');
    expect(streamCalls.length).toBeGreaterThan(0);
    expect(streamCalls.every(call => call.localOnly)).toBe(true);

    await mastra.pubsub.publish('workflow.events.v2.terminal-race-run', {
      type: 'watch',
      runId: 'terminal-race-run',
      data: { type: 'late-after-terminal-race' },
    } as unknown as Event);
    expect(pubsub.calls.at(-1)!.localOnly).toBe(false);
    await mastra.shutdown();
  });

  it('does not let an older finalizer release a newer registration with the same run ID', async () => {
    const pubsub = new RecordingPushOnlyPubSub();
    const mastra = new Mastra({ logger: false, storage: new MockStore(), workflows: {} as any, pubsub });
    const firstGeneration = mastra.__registerInternalWorkflow(makeNoopWorkflow('reused-run') as any, 'same-run')!;
    const activeGeneration = mastra.__beginInternalWorkflowEvent('reused-run', 'same-run')!;
    mastra.__unregisterInternalWorkflow('reused-run', 'same-run', firstGeneration);

    const secondGeneration = mastra.__registerInternalWorkflow(makeNoopWorkflow('reused-run') as any, 'same-run')!;
    mastra.__endInternalWorkflowEvent('reused-run', 'same-run', activeGeneration, true);
    await mastra.pubsub.publish('workflow.events.v2.same-run', {
      type: 'watch',
      runId: 'same-run',
      data: { type: 'new-generation-still-owned' },
    } as unknown as Event);
    expect(pubsub.calls.at(-1)!.localOnly).toBe(true);

    mastra.__unregisterInternalWorkflow('reused-run', 'same-run', secondGeneration);
    await mastra.pubsub.publish('workflow.events.v2.same-run', {
      type: 'watch',
      runId: 'same-run',
      data: { type: 'new-generation-released' },
    } as unknown as Event);
    expect(pubsub.calls.at(-1)!.localOnly).toBe(false);
    await mastra.shutdown();
  });

  it('keeps nested events local while retained root ownership drains', async () => {
    const pubsub = new RecordingPushOnlyPubSub();
    const mastra = new Mastra({ logger: false, storage: new MockStore(), workflows: {} as any, pubsub });
    const generation = mastra.__registerInternalWorkflow(makeNoopWorkflow('draining-root') as any, 'root-run')!;
    const activeGeneration = mastra.__beginInternalWorkflowEvent('draining-root', 'root-run')!;
    mastra.__unregisterInternalWorkflow('draining-root', 'root-run', generation);

    const nestedEvent = makeStepRunEvent('nested-child', 'nested-run', {
      workflowId: 'draining-root',
      runId: 'root-run',
    });
    (nestedEvent.data as Record<string, unknown>).__mastraInternalWorkflow = true;
    expect(mastra.__shouldProcessWorkflowEvent(nestedEvent as Event)).toBe(true);

    mastra.__endInternalWorkflowEvent('draining-root', 'root-run', activeGeneration, true);
    expect(mastra.__shouldProcessWorkflowEvent(nestedEvent as Event)).toBe(false);
    await mastra.shutdown();
  });

  it.each([
    ['success', false],
    ['failed', true],
  ] as const)('releases an internal run after semantic %s handling', async (expectedStatus, shouldFail) => {
    const pubsub = new RecordingPushOnlyPubSub();
    const workflow = createWorkflow({
      id: `terminal-${expectedStatus}`,
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    });
    workflow
      .then(
        createStep({
          id: 'terminal-step',
          inputSchema: z.object({}),
          outputSchema: z.object({}),
          execute: async () => {
            if (shouldFail) throw new Error('expected terminal failure');
            return {};
          },
        }) as any,
      )
      .commit();
    const mastra = new Mastra({ logger: false, storage: new MockStore(), workflows: {} as any, pubsub });
    const runId = `terminal-${expectedStatus}-run`;
    mastra.__registerInternalWorkflow(workflow as any, runId);
    await mastra.startWorkers();

    const run = await workflow.createRun({ runId });
    const result = await run.start({ inputData: {} });
    expect(result.status).toBe(expectedStatus);

    // The user-facing result can resolve from `workflows-finish` one microtask
    // before the outer event handler drains its lifecycle barrier.
    await new Promise<void>(resolve => setImmediate(resolve));
    await mastra.pubsub.publish(`workflow.events.v2.${runId}`, {
      type: 'watch',
      runId,
      data: { type: 'late-after-terminal-result' },
    } as unknown as Event);
    expect(pubsub.calls.at(-1)!.localOnly).toBe(false);
    await mastra.shutdown();
  });

  it('retains ownership while suspended and releases it after resume completes', async () => {
    const pubsub = new RecordingPushOnlyPubSub();
    const workflow = createWorkflow({
      id: 'suspend-resume-workflow',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    });
    workflow
      .then(
        createStep({
          id: 'pause',
          inputSchema: z.object({}),
          outputSchema: z.object({}),
          suspendSchema: z.object({ reason: z.string() }),
          resumeSchema: z.object({ continue: z.boolean() }),
          execute: async ({ resumeData, suspend }) => {
            if (!resumeData?.continue) {
              await suspend({ reason: 'wait' });
            }
            return {};
          },
        }) as any,
      )
      .commit();
    const mastra = new Mastra({ logger: false, storage: new MockStore(), workflows: {} as any, pubsub });
    mastra.__registerInternalWorkflow(workflow as any, 'suspend-run');
    await mastra.startWorkers();
    const run = await workflow.createRun({ runId: 'suspend-run' });

    const suspended = await run.start({ inputData: {} });
    expect(suspended.status).toBe('suspended');
    await mastra.pubsub.publish('workflow.events.v2.suspend-run', {
      type: 'watch',
      runId: 'suspend-run',
      data: { type: 'while-suspended' },
    } as unknown as Event);
    expect(pubsub.calls.at(-1)!.localOnly).toBe(true);

    const resumed = await run.resume({
      resumeData: { continue: true },
      step: 'pause',
    });
    expect(resumed.status).toBe('success');
    await mastra.pubsub.publish('workflow.events.v2.suspend-run', {
      type: 'watch',
      runId: 'suspend-run',
      data: { type: 'late-after-resume' },
    } as unknown as Event);
    expect(pubsub.calls.at(-1)!.localOnly).toBe(false);
    await mastra.shutdown();
  });

  it('keeps the same cancel lifecycle inside one Unix-socket instance', async () => {
    const tempDir = await mkdtemp(join(tmpdir(), 'mastra-owned-run-'));
    const socketPath = join(tempDir, 'events.sock');
    const ownerPubsub = new RecordingUnixSocketPubSub(socketPath);
    const remotePubsub = new UnixSocketPubSub(socketPath);
    const remoteEvents = vi.fn();
    const mastra = new Mastra({
      logger: false,
      storage: new MockStore(),
      workflows: {} as any,
      pubsub: ownerPubsub,
    });
    try {
      mastra.__registerInternalWorkflow(makeNoopWorkflow('unix-cancel') as any, 'unix-run');
      await mastra.startWorkers();
      await remotePubsub.subscribe('workflows', remoteEvents);
      await remotePubsub.subscribe('workflows-finish', remoteEvents);
      await remotePubsub.subscribe('workflow.events.v2.unix-run', remoteEvents);
      const handle = vi.spyOn(mastra, 'handleWorkflowEvent');

      await mastra.pubsub.publish('workflows', {
        ...makeStartEvent('unix-cancel', 'unix-run'),
        type: 'workflow.cancel',
      } as Event);
      await waitForHandledEvent(handle, 'workflow.end', 'unix-run');
      await new Promise(resolve => setTimeout(resolve, 50));

      const lifecycleCalls = ownerPubsub.calls.filter(call =>
        ['workflows', 'workflows-finish', 'workflow.events.v2.unix-run'].includes(call.topic),
      );
      expect(lifecycleCalls.some(call => call.topic === 'workflow.events.v2.unix-run')).toBe(true);
      expect(lifecycleCalls.some(call => call.topic === 'workflows-finish')).toBe(true);
      expect(lifecycleCalls.every(call => call.localOnly)).toBe(true);
      expect(remoteEvents).not.toHaveBeenCalled();

      await mastra.pubsub.publish('workflow.events.v2.unix-run', {
        type: 'watch',
        runId: 'unix-run',
        data: { type: 'late-after-end' },
      } as unknown as Event);
      await vi.waitFor(() => expect(remoteEvents).toHaveBeenCalledTimes(1));
      expect(ownerPubsub.calls.at(-1)!.localOnly).toBe(false);
    } finally {
      await mastra.shutdown();
      await remotePubsub.close();
      await rm(tempDir, { recursive: true, force: true });
    }
  });

  it('does NOT touch unrelated topics', async () => {
    const pubsub = new RecordingPushOnlyPubSub();
    const mastra = new Mastra({ logger: false, storage: new MockStore(), workflows: {} as any, pubsub });

    await mastra.pubsub.publish('some-other-topic', { type: 'whatever', runId: 'x', data: {} } as Event);

    expect(pubsub.calls[0]!.localOnly).toBe(false);
    await mastra.shutdown();
  });

  it('preserves caller-supplied localOnly options for unrelated topics', async () => {
    const pubsub = new RecordingPushOnlyPubSub();
    const mastra = new Mastra({ logger: false, storage: new MockStore(), workflows: {} as any, pubsub });

    await mastra.pubsub.publish('some-other-topic', { type: 'whatever', runId: 'x', data: {} } as Event, {
      localOnly: true,
    });

    expect(pubsub.calls[0]!.localOnly).toBe(true);
    await mastra.shutdown();
  });

  it('bounds cyclic parentWorkflow traversal', async () => {
    const pubsub = new RecordingPushOnlyPubSub();
    const mastra = new Mastra({ logger: false, storage: new MockStore(), workflows: {} as any, pubsub });
    const first: { workflowId: string; runId: string; parentWorkflow?: unknown } = {
      workflowId: 'foreign-parent-a',
      runId: 'foreign-run-a',
    };
    const second: { workflowId: string; runId: string; parentWorkflow?: unknown } = {
      workflowId: 'foreign-parent-b',
      runId: 'foreign-run-b',
      parentWorkflow: first,
    };
    first.parentWorkflow = second;

    await mastra.pubsub.publish('workflows', makeStepRunEvent('foreign-child', 'foreign-child-run', first));

    expect(pubsub.calls).toHaveLength(1);
    expect(pubsub.calls[0]!.localOnly).toBe(false);
    await mastra.shutdown();
  });
});
