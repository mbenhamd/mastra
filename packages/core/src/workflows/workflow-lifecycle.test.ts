import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { InMemoryServerCache } from '../cache/inmemory';
import { CachingPubSub } from '../events/caching-pubsub';
import { EventEmitterPubSub } from '../events/event-emitter';
import { PubSub } from '../events/pubsub';
import type { Event, EventCallback, PublishEvent } from '../events/types';
import { Mastra } from '../mastra';
import { MockStore } from '../storage/mock';
import { createEventedWorkflow, createWorkflow } from './create';
import {
  getWorkflowLifecycleEventId,
  getWorkflowLifecycleTopic,
  publishWorkflowLifecycleEvent,
} from './lifecycle-events';
import type { WorkflowLifecycleExecutionIdentity, WorkflowLifecycleRecord } from './lifecycle-events';
import { createStep } from './workflow';
import { watchWorkflowLifecycleEvents } from './workflow-lifecycle';

const workflowId = 'review-workflow';
const runId = 'review-run';
const executionGeneration = 'review-execution-generation';
const identity = { workflowId, runId, executionGeneration };
const topic = getWorkflowLifecycleTopic(identity);
const indexedReplay = { retentionMs: 60_000, maxEvents: 100 };
const allowProcessLocalReplay = { allowProcessLocalReplay: true };
const stubLogGeneration = 'stub-log-generation';

function processLocalPubSub(inner = new EventEmitterPubSub()) {
  return new CachingPubSub(inner, new InMemoryServerCache(), { indexedReplay });
}

class QueuedWorkflowPubSub extends EventEmitterPubSub {
  private queued: Array<{ topic: string; event: PublishEvent; options?: { localOnly?: boolean } }> = [];

  override async publish(topic: string, event: PublishEvent, options?: { localOnly?: boolean }): Promise<void> {
    if (topic === 'workflows') {
      this.queued.push({ topic, event, options });
      return;
    }
    await super.publish(topic, event, options);
  }

  async flushQueued(): Promise<void> {
    while (this.queued.length > 0) {
      const queued = this.queued.splice(0);
      for (const item of queued) await super.publish(item.topic, item.event, item.options);
    }
  }
}

function lifecycleRecord(label: string, target: WorkflowLifecycleExecutionIdentity | typeof identity = identity) {
  return {
    schemaVersion: 1,
    workflowId: target.workflowId,
    runId: target.runId,
    executionGeneration: target.executionGeneration,
    event: { type: 'workflow.started', resumeAttempt: 0, input: { label } },
  } satisfies WorkflowLifecycleRecord;
}

async function publishLifecycle(pubsub: PubSub, label: string, target = identity) {
  await publishWorkflowLifecycleEvent({
    pubsub,
    ...target,
    event: { type: 'workflow.started', resumeAttempt: 0, input: { label } },
  });
}

function event(index: number): Event {
  const data = lifecycleRecord(String(index));
  return {
    type: 'workflow.lifecycle',
    id: getWorkflowLifecycleEventId(data),
    runId,
    createdAt: new Date(`2026-07-15T00:00:0${index}.000Z`),
    index,
    logGeneration: stubLogGeneration,
    data,
  };
}

class IndexedReplayStub extends PubSub {
  override get indexedReplay() {
    return { scope: 'process' as const, retentionMs: 60_000, maxEvents: 100 };
  }

  override async getIndexedReplayRange() {
    return { ...this.indexedReplay, logGeneration: stubLogGeneration, firstCursor: 0, nextCursor: 100 };
  }

  callback?: EventCallback;
  offset?: number;
  unsubscribe = vi.fn(async () => {});

  async publish(): Promise<void> {}

  async subscribe(_topic: string, callback: EventCallback): Promise<void> {
    this.callback = callback;
  }

  override async subscribeFromOffset(_topic: string, offset: number, callback: EventCallback): Promise<void> {
    this.offset = offset;
    this.callback = callback;
  }

  async flush(): Promise<void> {}

  async deliver(deliveredEvent: Event, ack?: () => Promise<void>, nack?: () => Promise<void>): Promise<void> {
    await this.callback?.(deliveredEvent, ack, nack);
  }
}

describe('workflow lifecycle watching', () => {
  const makeWorkflow = (engine: 'default' | 'evented') => {
    const step = createStep({
      id: 'only-step',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      execute: async () => ({}),
    });
    const create = engine === 'evented' ? createEventedWorkflow : createWorkflow;
    const workflow = create({
      id: workflowId,
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      steps: [step],
    });
    workflow.then(step).commit();
    return workflow;
  };

  it('exposes replayable lifecycle delivery on a default Run', async () => {
    const pubsub = processLocalPubSub();
    const workflow = makeWorkflow('default');
    const run = await workflow.createRun({ runId, pubsub });
    const runIdentity = await run.getLifecycleExecutionIdentity();
    await publishLifecycle(pubsub, 'default', runIdentity);

    const cursors: number[] = [];
    const unwatch = await run.watchLifecycle(event => {
      cursors.push(event.cursor);
    }, allowProcessLocalReplay);

    expect(cursors).toEqual([0]);
    await unwatch();
  });

  it('inherits the registered Mastra PubSub for a default Run', async () => {
    const pubsub = processLocalPubSub();
    const workflow = makeWorkflow('default');
    const mastra = new Mastra({
      workflows: { [workflowId]: workflow },
      storage: new MockStore(),
      pubsub,
    });
    const run = await workflow.createRun({ runId: 'mastra-pubsub-run' });
    const runIdentity = await run.getLifecycleExecutionIdentity();
    await publishLifecycle(pubsub, 'registered-default', runIdentity);

    const received: number[] = [];
    const unwatch = await run.watchLifecycle(delivery => {
      received.push(delivery.cursor);
    }, allowProcessLocalReplay);

    expect(received).toEqual([0]);
    await unwatch();
    await mastra.shutdown();
  });

  it('reconnects to a saved old generation after the persisted snapshot is taken over', async () => {
    const pubsub = processLocalPubSub();
    const storage = new MockStore();
    const workflow = makeWorkflow('default');
    const mastra = new Mastra({
      workflows: { [workflowId]: workflow },
      storage,
      pubsub,
    });
    const takeoverRunId = 'old-generation-reconnect';
    const run = await workflow.createRun({ runId: takeoverRunId });
    const oldIdentity = await run.getLifecycleExecutionIdentity();
    await publishLifecycle(pubsub, 'old-generation', oldIdentity);
    const workflowsStore = await storage.getStore('workflows');
    await workflowsStore.updateWorkflowState({
      workflowName: workflowId,
      runId: takeoverRunId,
      opts: {
        executionGeneration: 'new-takeover-generation',
        lifecycleResumeAttempt: 0,
        lifecycleStepStates: {},
      },
    });

    const received: string[] = [];
    const unwatch = await run.watchLifecycle(
      delivery => {
        received.push((delivery.event as { input?: { label?: string } }).input?.label ?? 'missing');
      },
      {
        executionGeneration: oldIdentity.executionGeneration,
        ...allowProcessLocalReplay,
      },
    );

    expect(received).toEqual(['old-generation']);
    await unwatch();
    await mastra.shutdown();
  });

  it('envelopes lifecycle events emitted by an executing default workflow', async () => {
    const executingRunId = 'executing-run';
    const pubsub = processLocalPubSub();
    const workflow = makeWorkflow('default');
    const run = await workflow.createRun({ runId: executingRunId, pubsub });
    const received: Array<{ cursor: number; eventId: string; type: string }> = [];
    const unwatch = await run.watchLifecycle(lifecycle => {
      received.push({ cursor: lifecycle.cursor, eventId: lifecycle.eventId, type: lifecycle.event.type });
    }, allowProcessLocalReplay);

    const result = await run.start({ inputData: {} });

    expect(result.status).toBe('success');
    await vi.waitFor(() => expect(received.some(item => item.type === 'step.finished')).toBe(true));
    expect(received.some(item => item.type === 'step.started')).toBe(true);
    expect(received.map(item => item.cursor)).toEqual(received.map((_, index) => index));
    expect(new Set(received.map(item => item.eventId)).size).toBe(received.length);
    await unwatch();
  });

  it('uses Mastra PubSub for an evented Run lifecycle subscription', async () => {
    const pubsub = processLocalPubSub();
    const storage = new MockStore();
    const workflow = makeWorkflow('evented');
    new Mastra({
      workflows: { [workflowId]: workflow },
      storage,
      pubsub,
    });
    const run = await workflow.createRun({ runId });
    const workflowsStore = await storage.getStore('workflows');
    await expect(workflowsStore.loadWorkflowSnapshot({ workflowName: workflowId, runId })).resolves.toMatchObject({
      status: 'pending',
      executionGeneration: expect.any(String),
      lifecycleResumeAttempt: 0,
      lifecycleStepStates: {},
    });
    const runIdentity = await run.getLifecycleExecutionIdentity();
    const reopened = await workflow.createRun({ runId });
    await expect(reopened.getLifecycleExecutionIdentity()).resolves.toEqual(runIdentity);
    await publishLifecycle(pubsub, 'evented', runIdentity);

    const cursors: number[] = [];
    const unwatch = await run.watchLifecycle(event => {
      cursors.push(event.cursor);
    }, allowProcessLocalReplay);

    expect(cursors).toEqual([0]);
    await unwatch();
  });

  it('publishes terminal lifecycle events when an evented run is canceled before start', async () => {
    const pubsub = processLocalPubSub();
    const storage = new MockStore();
    const workflow = makeWorkflow('evented');
    const mastra = new Mastra({
      workflows: { [workflowId]: workflow },
      storage,
      pubsub,
    });
    const workflowEvents = async (event: Event) => {
      await mastra.handleWorkflowEvent(event);
    };
    await pubsub.subscribe('workflows', workflowEvents);
    const canceledRunId = 'cancel-before-start';
    const run = await workflow.createRun({ runId: canceledRunId });
    const received: string[] = [];
    const unwatch = await run.watchLifecycle(event => {
      received.push(event.event.type);
    }, allowProcessLocalReplay);

    await run.cancel();

    await vi.waitFor(() => {
      expect(received).toEqual(expect.arrayContaining(['workflow.canceled', 'workflow.finished']));
    });
    expect(received).not.toContain('workflow.started');
    const workflowsStore = await storage.getStore('workflows');
    await expect(
      workflowsStore.loadWorkflowSnapshot({ workflowName: workflowId, runId: canceledRunId }),
    ).resolves.toMatchObject({ status: 'canceled', executionGeneration: expect.any(String) });

    await unwatch();
    await pubsub.unsubscribe('workflows', workflowEvents);
    await mastra.shutdown();
  });

  it('keeps a queued evented cancellation nonterminal until the remote worker consumes it', async () => {
    const queuedTransport = new QueuedWorkflowPubSub();
    const pubsub = processLocalPubSub(queuedTransport);
    const storage = new MockStore();
    const workflow = makeWorkflow('evented');
    const mastra = new Mastra({
      workflows: { [workflowId]: workflow },
      storage,
      pubsub,
    });
    const workflowEvents = async (event: Event) => {
      await mastra.handleWorkflowEvent(event);
    };
    await pubsub.subscribe('workflows', workflowEvents);
    const queuedRunId = 'queued-cancel-before-start';
    const run = await workflow.createRun({ runId: queuedRunId });
    const workflowsStore = await storage.getStore('workflows');

    await run.cancel();

    await expect(
      workflowsStore.loadWorkflowSnapshot({ workflowName: workflowId, runId: queuedRunId }),
    ).resolves.toMatchObject({ status: 'pending' });
    await queuedTransport.flushQueued();
    await vi.waitFor(async () => {
      await expect(
        workflowsStore.loadWorkflowSnapshot({ workflowName: workflowId, runId: queuedRunId }),
      ).resolves.toMatchObject({ status: 'canceled' });
    });

    await pubsub.unsubscribe('workflows', workflowEvents);
    await mastra.shutdown();
  });

  it('keeps a remote default cancellation authoritative after a blocking step returns', async () => {
    let markStepEntered!: () => void;
    let releaseStep!: () => void;
    const stepEntered = new Promise<void>(resolve => {
      markStepEntered = resolve;
    });
    const stepRelease = new Promise<void>(resolve => {
      releaseStep = resolve;
    });
    const makeBlockingWorkflow = (block: boolean) => {
      const step = createStep({
        id: 'blocking-default-step',
        inputSchema: z.object({}),
        outputSchema: z.object({}),
        execute: async () => {
          if (block) {
            markStepEntered();
            await stepRelease;
          }
          return {};
        },
      });
      return createWorkflow({
        id: 'remote-default-cancel-workflow',
        inputSchema: z.object({}),
        outputSchema: z.object({}),
        steps: [step],
      })
        .then(step)
        .commit();
    };

    const pubsub = processLocalPubSub();
    const storage = new MockStore();
    const executionWorkflow = makeBlockingWorkflow(true);
    const cancellationWorkflow = makeBlockingWorkflow(false);
    const executionMastra = new Mastra({ logger: false, storage, pubsub });
    const cancellationMastra = new Mastra({ logger: false, storage, pubsub });
    executionWorkflow.__registerMastra(executionMastra);
    cancellationWorkflow.__registerMastra(cancellationMastra);
    const remoteRunId = 'remote-default-cancel-run';

    try {
      const executionRun = await executionWorkflow.createRun({ runId: remoteRunId });
      const execution = executionRun.start({ inputData: {} });
      await stepEntered;

      const cancellationRun = await cancellationWorkflow.createRun({ runId: remoteRunId });
      await cancellationRun.cancel();
      releaseStep();

      await expect(execution).resolves.toMatchObject({ status: 'canceled' });
      const workflowsStore = await storage.getStore('workflows');
      await expect(
        workflowsStore.loadWorkflowSnapshot({ workflowName: executionWorkflow.id, runId: remoteRunId }),
      ).resolves.toMatchObject({ status: 'canceled' });
    } finally {
      releaseStep?.();
      await executionMastra.shutdown();
      await cancellationMastra.shutdown();
    }
  });

  it('keeps a remote evented cancellation authoritative after a blocking step returns', async () => {
    let markStepEntered!: () => void;
    let releaseStep!: () => void;
    const stepEntered = new Promise<void>(resolve => {
      markStepEntered = resolve;
    });
    const stepRelease = new Promise<void>(resolve => {
      releaseStep = resolve;
    });
    const makeBlockingWorkflow = (block: boolean) => {
      const step = createStep({
        id: 'blocking-step',
        inputSchema: z.object({}),
        outputSchema: z.object({}),
        execute: async () => {
          if (block) {
            markStepEntered();
            await stepRelease;
          }
          return {};
        },
      });
      return createEventedWorkflow({
        id: 'remote-cancel-workflow',
        inputSchema: z.object({}),
        outputSchema: z.object({}),
        steps: [step],
      })
        .then(step)
        .commit();
    };

    const pubsub = processLocalPubSub();
    const storage = new MockStore();
    const executionWorkflow = makeBlockingWorkflow(true);
    const cancellationWorkflow = makeBlockingWorkflow(false);
    const executionMastra = new Mastra({
      logger: false,
      workflows: { remoteCancelWorkflow: executionWorkflow },
      storage,
      pubsub,
    });
    const cancellationMastra = new Mastra({
      logger: false,
      workflows: { remoteCancelWorkflow: cancellationWorkflow },
      storage,
      pubsub,
    });
    const routeWorkflowEvent = async (workflowEvent: Event) => {
      if (workflowEvent.type === 'workflow.cancel') {
        await cancellationMastra.handleWorkflowEvent(workflowEvent);
      } else {
        await executionMastra.handleWorkflowEvent(workflowEvent);
      }
    };
    await pubsub.subscribe('workflows', routeWorkflowEvent);

    const blockingRunId = 'remote-cancel-blocking-run';
    const executionRun = await executionWorkflow.createRun({ runId: blockingRunId });
    const identity = await executionRun.getLifecycleExecutionIdentity();
    const execution = executionRun.start({ inputData: {} });
    await stepEntered;

    const workflowsStore = await storage.getStore('workflows');
    await expect(
      workflowsStore.loadWorkflowSnapshot({ workflowName: executionWorkflow.id, runId: blockingRunId }),
    ).resolves.toMatchObject({
      status: 'running',
      activeStepsPath: { 'blocking-step': [0] },
      lifecycleStepStates: expect.objectContaining({
        '["blocking-step",[0],null,null]': expect.objectContaining({ stepAttempt: 1 }),
      }),
    });

    const cancellationRun = await cancellationWorkflow.createRun({ runId: blockingRunId });
    await cancellationRun.cancel();
    await vi.waitFor(async () => {
      await expect(
        workflowsStore.loadWorkflowSnapshot({ workflowName: executionWorkflow.id, runId: blockingRunId }),
      ).resolves.toMatchObject({ status: 'canceled', executionGeneration: identity.executionGeneration });
    });

    releaseStep();
    await expect(execution).resolves.toMatchObject({ status: 'canceled' });

    const lifecycleEvents = (await pubsub.getHistory(identity.topic)).map(
      retained => (retained.data as WorkflowLifecycleRecord).event,
    );
    expect(lifecycleEvents.map(event => event.type)).toEqual([
      'workflow.started',
      'step.started',
      'step.canceled',
      'step.finished',
      'workflow.canceled',
      'workflow.finished',
    ]);
    expect(lifecycleEvents.at(-1)).toMatchObject({ type: 'workflow.finished', status: 'canceled' });
    expect(lifecycleEvents).not.toEqual(
      expect.arrayContaining([
        expect.objectContaining({ type: 'step.completed' }),
        expect.objectContaining({ type: 'workflow.finished', status: 'success' }),
      ]),
    );

    await pubsub.unsubscribe('workflows', routeWorkflowEvent);
    await Promise.all([executionMastra.shutdown(), cancellationMastra.shutdown()]);
  });

  it('resets evented cancellation state for a fresh time-travel generation', async () => {
    let markStepEntered!: () => void;
    let releaseStep!: () => void;
    const stepEntered = new Promise<void>(resolve => {
      markStepEntered = resolve;
    });
    const stepRelease = new Promise<void>(resolve => {
      releaseStep = resolve;
    });
    const step = createStep({
      id: 'generation-reset-step',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      execute: async () => {
        markStepEntered();
        await stepRelease;
        return {};
      },
    });
    const workflow = createEventedWorkflow({
      id: 'generation-reset-workflow',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      steps: [step],
    })
      .then(step)
      .commit();
    const pubsub = processLocalPubSub();
    const storage = new MockStore();
    const mastra = new Mastra({ logger: false, workflows: { [workflow.id]: workflow }, storage, pubsub });
    const processWorkflowEvent = async (workflowEvent: Event) => {
      await mastra.handleWorkflowEvent(workflowEvent);
    };
    await pubsub.subscribe('workflows', processWorkflowEvent);
    const cancelEvents: Event[] = [];
    const captureCancel = async (workflowEvent: Event) => {
      if (workflowEvent.type === 'workflow.cancel') cancelEvents.push(workflowEvent);
    };
    await pubsub.subscribe('workflows', captureCancel);

    const run = await workflow.createRun({ runId: 'generation-reset-run' });
    const firstGeneration = (await run.getLifecycleExecutionIdentity()).executionGeneration;
    await run.cancel();
    const workflowsStore = await storage.getStore('workflows');
    await vi.waitFor(async () => {
      await expect(
        workflowsStore.loadWorkflowSnapshot({ workflowName: workflow.id, runId: run.runId }),
      ).resolves.toMatchObject({ status: 'canceled', executionGeneration: firstGeneration });
    });
    expect(run.abortController.signal.aborted).toBe(true);

    const canceledSnapshot = await workflowsStore.loadWorkflowSnapshot({ workflowName: workflow.id, runId: run.runId });
    await workflowsStore.persistWorkflowSnapshot({
      workflowName: workflow.id,
      runId: run.runId,
      snapshot: {
        ...canceledSnapshot!,
        status: 'success',
        context: { input: {}, 'generation-reset-step': { status: 'success', output: {} } },
        activePaths: [0],
        activeStepsPath: { 'generation-reset-step': [0] },
      },
    });

    const timeTravel = run.timeTravel({ step: 'generation-reset-step', inputData: {} });
    await stepEntered;
    expect(run.abortController.signal.aborted).toBe(false);
    const secondGenerationSnapshot = await workflowsStore.loadWorkflowSnapshot({
      workflowName: workflow.id,
      runId: run.runId,
    });
    expect(secondGenerationSnapshot).toMatchObject({ status: 'running', executionGeneration: expect.any(String) });
    expect(secondGenerationSnapshot?.executionGeneration).not.toBe(firstGeneration);

    await run.cancel();
    await vi.waitFor(async () => {
      await expect(
        workflowsStore.loadWorkflowSnapshot({ workflowName: workflow.id, runId: run.runId }),
      ).resolves.toMatchObject({
        status: 'canceled',
        executionGeneration: secondGenerationSnapshot?.executionGeneration,
      });
    });
    releaseStep();
    await expect(timeTravel).resolves.toMatchObject({ status: 'canceled' });
    expect(cancelEvents.map(event => event.data.executionGeneration)).toEqual([
      firstGeneration,
      secondGenerationSnapshot?.executionGeneration,
    ]);

    await pubsub.unsubscribe('workflows', captureCancel);
    await pubsub.unsubscribe('workflows', processWorkflowEvent);
    await mastra.shutdown();
  });

  it.each(['restart', 'timeTravel'] as const)(
    'detaches a stale evented AbortController before a fresh %s generation',
    async kind => {
      let markStepEntered!: () => void;
      let releaseStep!: () => void;
      const stepEntered = new Promise<void>(resolve => {
        markStepEntered = resolve;
      });
      const stepRelease = new Promise<void>(resolve => {
        releaseStep = resolve;
      });
      const step = createStep({
        id: 'stale-controller-step',
        inputSchema: z.object({}),
        outputSchema: z.object({}),
        execute: async () => {
          markStepEntered();
          await stepRelease;
          return {};
        },
      });
      const workflow = createEventedWorkflow({
        id: `stale-controller-${kind}-workflow`,
        inputSchema: z.object({}),
        outputSchema: z.object({}),
        steps: [step],
      })
        .then(step)
        .commit();
      const pubsub = processLocalPubSub();
      const storage = new MockStore();
      const mastra = new Mastra({ logger: false, workflows: { [workflow.id]: workflow }, storage, pubsub });
      const processWorkflowEvent = async (workflowEvent: Event) => {
        await mastra.handleWorkflowEvent(workflowEvent);
      };
      const cancelEvents: Event[] = [];
      const captureCancel = async (workflowEvent: Event) => {
        if (workflowEvent.type === 'workflow.cancel') cancelEvents.push(workflowEvent);
      };
      await pubsub.subscribe('workflows', processWorkflowEvent);
      await pubsub.subscribe('workflows', captureCancel);

      try {
        const run = await workflow.createRun({ runId: `stale-controller-${kind}-run` });
        const staleController = run.abortController;
        const workflowsStore = await storage.getStore('workflows');
        const pending = await workflowsStore.loadWorkflowSnapshot({ workflowName: workflow.id, runId: run.runId });
        await workflowsStore.persistWorkflowSnapshot({
          workflowName: workflow.id,
          runId: run.runId,
          snapshot: {
            ...pending!,
            status: kind === 'restart' ? 'running' : 'success',
            context: {
              input: { status: 'success', payload: {} },
              'stale-controller-step': { status: 'success', payload: {} },
            },
            activePaths: [0],
            activeStepsPath: { 'stale-controller-step': [0] },
          },
        });

        const freshExecution =
          kind === 'restart'
            ? run.restart()
            : run.timeTravel({
                step: 'stale-controller-step',
                inputData: {},
              });
        await stepEntered;
        expect(run.abortController).not.toBe(staleController);
        expect(run.abortController.signal.aborted).toBe(false);

        staleController.abort();
        await new Promise(resolve => setTimeout(resolve, 0));
        releaseStep();

        await expect(freshExecution).resolves.toMatchObject({ status: 'success' });
        expect(cancelEvents).toHaveLength(0);
        expect(run.abortController.signal.aborted).toBe(false);
        await expect(
          workflowsStore.loadWorkflowSnapshot({ workflowName: workflow.id, runId: run.runId }),
        ).resolves.toMatchObject({ status: 'success' });
      } finally {
        releaseStep?.();
        await pubsub.unsubscribe('workflows', captureCancel);
        await pubsub.unsubscribe('workflows', processWorkflowEvent);
        await mastra.shutdown();
      }
    },
  );

  it.each([
    ['default', 'success'],
    ['default', 'failed'],
    ['evented', 'success'],
    ['evented', 'failed'],
  ] as const)(
    'does not overwrite an existing %s/%s terminal snapshot when canceled',
    async (engine, terminalStatus) => {
      const pubsub = processLocalPubSub();
      const storage = new MockStore();
      const workflow = makeWorkflow(engine);
      const mastra = new Mastra({
        workflows: { [workflowId]: workflow },
        storage,
        pubsub,
      });
      const terminalRunId = `${engine}-already-terminal`;
      const run = await workflow.createRun({ runId: terminalRunId });
      const runIdentity = await run.getLifecycleExecutionIdentity();
      const workflowsStore = await storage.getStore('workflows');
      await workflowsStore.updateWorkflowState({
        workflowName: workflowId,
        runId: terminalRunId,
        opts: { status: terminalStatus },
      });

      await run.cancel();

      await expect(
        workflowsStore.loadWorkflowSnapshot({ workflowName: workflowId, runId: terminalRunId }),
      ).resolves.toMatchObject({ status: terminalStatus });
      await expect(pubsub.getHistory(runIdentity.topic)).resolves.toEqual([]);
      await mastra.shutdown();
    },
  );

  it('emits cancellation lifecycle for a reopened running default run with no local execution', async () => {
    const pubsub = processLocalPubSub();
    const storage = new MockStore();
    const originalWorkflow = makeWorkflow('default');
    const originalMastra = new Mastra({
      workflows: { [workflowId]: originalWorkflow },
      storage,
      pubsub,
    });
    const reopenedRunId = 'reopened-running-cancel';
    const originalRun = await originalWorkflow.createRun({ runId: reopenedRunId });
    const identity = await originalRun.getLifecycleExecutionIdentity();
    const workflowsStore = await storage.getStore('workflows');
    await workflowsStore.updateWorkflowState({
      workflowName: workflowId,
      runId: reopenedRunId,
      opts: { status: 'running' },
    });

    const reopenedWorkflow = makeWorkflow('default');
    const reopenedMastra = new Mastra({
      workflows: { [workflowId]: reopenedWorkflow },
      storage,
      pubsub,
    });
    const reopenedRun = await reopenedWorkflow.createRun({ runId: reopenedRunId });
    const snapshotReads = vi.spyOn(workflowsStore, 'loadWorkflowSnapshot');
    await reopenedRun.cancel();
    expect(snapshotReads).toHaveBeenCalledTimes(1);

    const history = await pubsub.getHistory(identity.topic);
    expect(history.map(item => (item.data as WorkflowLifecycleRecord).event.type)).toEqual([
      'workflow.canceled',
      'workflow.finished',
    ]);
    await expect(
      workflowsStore.loadWorkflowSnapshot({ workflowName: workflowId, runId: reopenedRunId }),
    ).resolves.toMatchObject({ status: 'canceled' });
    await Promise.all([originalMastra.shutdown(), reopenedMastra.shutdown()]);
  });

  it('carries the resumed lifecycle tuple on an evented cancellation', async () => {
    const pubsub = processLocalPubSub();
    const storage = new MockStore();
    const workflow = makeWorkflow('evented');
    const mastra = new Mastra({
      workflows: { [workflowId]: workflow },
      storage,
      pubsub,
    });
    const resumedRunId = 'resumed-cancel-run';
    const run = await workflow.createRun({ runId: resumedRunId });
    const lifecycleStepStates = {
      '["only-step",[0],null,null]': { stepCallId: 'resumed-step-call', stepAttempt: 2 },
    };
    const workflowsStore = await storage.getStore('workflows');
    await workflowsStore.updateWorkflowState({
      workflowName: workflowId,
      runId: resumedRunId,
      opts: {
        status: 'running',
        lifecycleResumeAttempt: 2,
        lifecycleStepStates,
      },
    });
    const cancelEvents: Event[] = [];
    const captureCancel = async (event: Event) => {
      if (event.type === 'workflow.cancel') cancelEvents.push(event);
    };
    await pubsub.subscribe('workflows', captureCancel);

    await run.cancel();

    expect(cancelEvents).toHaveLength(1);
    expect(cancelEvents[0]?.data).toMatchObject({
      runId: resumedRunId,
      lifecycleResumeAttempt: 2,
      lifecycleStepStates,
    });
    await pubsub.unsubscribe('workflows', captureCancel);
    await mastra.shutdown();
  });

  it('replays strictly after the committed cursor and keeps live delivery ordered', async () => {
    const cache = new InMemoryServerCache();
    const pubsub = new CachingPubSub(new EventEmitterPubSub(), cache, { indexedReplay });

    await publishLifecycle(pubsub, 'zero');
    await publishLifecycle(pubsub, 'one');
    await publishLifecycle(pubsub, 'two');

    const received: Array<{ cursor: number; eventId: string; workflowId: string; runId: string }> = [];
    let resolveLive!: () => void;
    const liveDelivered = new Promise<void>(resolve => {
      resolveLive = resolve;
    });

    const unwatch = await watchWorkflowLifecycleEvents({
      pubsub,
      workflowId,
      runId,
      executionGeneration,
      options: {
        afterCursor: 0,
        afterLogGeneration: (await pubsub.getIndexedReplayRange(topic))!.logGeneration,
        ...allowProcessLocalReplay,
      },
      callback: lifecycle => {
        received.push({
          cursor: lifecycle.cursor,
          eventId: lifecycle.eventId,
          workflowId: lifecycle.workflowId,
          runId: lifecycle.runId,
        });
        if (lifecycle.cursor === 3) resolveLive();
      },
    });

    expect(received.map(item => item.cursor)).toEqual([1, 2]);

    await publishLifecycle(pubsub, 'three');
    await liveDelivered;

    expect(received.map(item => item.cursor)).toEqual([1, 2, 3]);
    expect(received.every(item => item.workflowId === workflowId && item.runId === runId)).toBe(true);

    const history = await pubsub.getHistory(topic);
    expect(received.map(item => item.eventId)).toEqual(history.slice(1).map(item => item.id));

    await unwatch();
  });

  it('awaits successful processing before ack and serializes concurrent deliveries', async () => {
    const pubsub = new IndexedReplayStub();
    const ackFirst = vi.fn(async () => {});
    const ackSecond = vi.fn(async () => {});
    const entered: number[] = [];
    let releaseFirst!: () => void;
    const firstCanFinish = new Promise<void>(resolve => {
      releaseFirst = resolve;
    });

    const unwatch = await watchWorkflowLifecycleEvents({
      pubsub,
      workflowId,
      runId,
      executionGeneration,
      options: { afterCursor: 4, afterLogGeneration: stubLogGeneration, ...allowProcessLocalReplay },
      callback: async lifecycle => {
        entered.push(lifecycle.cursor);
        if (lifecycle.cursor === 5) await firstCanFinish;
      },
    });

    expect(pubsub.offset).toBe(5);
    const firstDelivery = pubsub.deliver(event(5), ackFirst);
    const secondDelivery = pubsub.deliver(event(6), ackSecond);
    await vi.waitFor(() => expect(entered).toEqual([5]));
    expect(ackFirst).not.toHaveBeenCalled();
    expect(ackSecond).not.toHaveBeenCalled();

    releaseFirst();
    await Promise.all([firstDelivery, secondDelivery]);

    expect(entered).toEqual([5, 6]);
    expect(ackFirst).toHaveBeenCalledTimes(1);
    expect(ackSecond).toHaveBeenCalledTimes(1);
    await unwatch();
  });

  it('nacks a failed live delivery without acknowledging it', async () => {
    const pubsub = new IndexedReplayStub();
    const ack = vi.fn(async () => {});
    const nack = vi.fn(async () => {});

    const unwatch = await watchWorkflowLifecycleEvents({
      pubsub,
      workflowId,
      runId,
      executionGeneration,
      options: allowProcessLocalReplay,
      callback: async () => {
        throw new Error('projection failed');
      },
    });

    await expect(pubsub.deliver(event(0), ack, nack)).rejects.toThrow('projection failed');

    expect(ack).not.toHaveBeenCalled();
    expect(nack).toHaveBeenCalledTimes(1);
    await unwatch();
  });

  it('nacks a poison lifecycle record instead of advancing the cursor', async () => {
    const pubsub = new IndexedReplayStub();
    const ack = vi.fn(async () => {});
    const nack = vi.fn(async () => {});
    const callback = vi.fn();
    const poison = {
      ...event(0),
      data: lifecycleRecord('wrong-workflow', { ...identity, workflowId: 'another-workflow' }),
    };

    const unwatch = await watchWorkflowLifecycleEvents({
      pubsub,
      workflowId,
      runId,
      executionGeneration,
      options: allowProcessLocalReplay,
      callback,
    });

    await expect(pubsub.deliver(poison, ack, nack)).rejects.toMatchObject({ reason: 'identity-mismatch' });
    expect(callback).not.toHaveBeenCalled();
    expect(ack).not.toHaveBeenCalled();
    expect(nack).toHaveBeenCalledTimes(1);
    await unwatch();
  });

  it('stops an active exact subscription after one poison lifecycle record', async () => {
    const inner = new EventEmitterPubSub();
    const unsubscribe = vi.spyOn(inner, 'unsubscribe');
    const pubsub = new CachingPubSub(inner, new InMemoryServerCache(), { indexedReplay });
    const callback = vi.fn();
    let unsubscribeCountAtOnError = 0;
    let onErrorCompleted = false;
    const activeError = vi.fn(async () => {
      unsubscribeCountAtOnError = unsubscribe.mock.calls.length;
      // Terminal observers may synchronously drain or tear down their owner.
      // This must not self-wait on the delivery chain invoking onError.
      await pubsub.flush();
      onErrorCompleted = true;
    });
    const unwatch = await watchWorkflowLifecycleEvents({
      pubsub,
      workflowId,
      runId,
      executionGeneration,
      options: { ...allowProcessLocalReplay, onError: activeError },
      callback,
    });
    const poisonRecord = { ...lifecycleRecord('poison'), schemaVersion: 2 };

    await pubsub.publish(topic, {
      type: 'workflow.lifecycle',
      id: 'poison-lifecycle-event',
      runId,
      createdAt: new Date('2026-07-15T00:01:00.000Z'),
      data: poisonRecord,
    });

    await vi.waitFor(() => expect(onErrorCompleted).toBe(true));
    // Broker teardown completes before the public terminal observer runs.
    expect(unsubscribeCountAtOnError).toBe(1);
    expect(activeError.mock.calls[0]?.[0]).toMatchObject({
      id: 'WORKFLOW_LIFECYCLE_REPLAY_INTEGRITY_FAILURE',
      details: { reason: 'malformed-retained-event' },
    });
    expect(callback).not.toHaveBeenCalled();

    await publishLifecycle(pubsub, 'after-poison');
    await new Promise(resolve => setTimeout(resolve, 0));
    expect(activeError).toHaveBeenCalledTimes(1);
    expect(callback).not.toHaveBeenCalled();

    await unwatch();
  });

  it('nacks a lifecycle record whose transport id does not match its semantic identity', async () => {
    const pubsub = new IndexedReplayStub();
    const ack = vi.fn(async () => {});
    const nack = vi.fn(async () => {});
    const callback = vi.fn();
    const forged = { ...event(0), id: 'forged-lifecycle-event-id' };

    const unwatch = await watchWorkflowLifecycleEvents({
      pubsub,
      workflowId,
      runId,
      executionGeneration,
      options: allowProcessLocalReplay,
      callback,
    });

    await expect(pubsub.deliver(forged, ack, nack)).rejects.toMatchObject({ reason: 'identity-mismatch' });
    expect(callback).not.toHaveBeenCalled();
    expect(ack).not.toHaveBeenCalled();
    expect(nack).toHaveBeenCalledTimes(1);
    await unwatch();
  });

  it('requires an explicit opt-in for process-local replay', async () => {
    await expect(
      watchWorkflowLifecycleEvents({
        pubsub: processLocalPubSub(),
        workflowId,
        runId,
        executionGeneration,
        callback: () => {},
      }),
    ).rejects.toMatchObject({ id: 'WORKFLOW_LIFECYCLE_DURABLE_REPLAY_REQUIRED' });
  });

  it('surfaces a live callback failure when the transport cannot nack', async () => {
    const logger = { error: vi.fn() };
    const pubsub = new CachingPubSub(
      new EventEmitterPubSub(undefined, { logger: logger as any }),
      new InMemoryServerCache(),
      { indexedReplay },
    );
    const unwatch = await watchWorkflowLifecycleEvents({
      pubsub,
      workflowId,
      runId,
      executionGeneration,
      options: allowProcessLocalReplay,
      callback: async () => {
        throw new Error('projection failed without redelivery');
      },
    });

    await publishLifecycle(pubsub, 'live-failure');

    await vi.waitFor(() => expect(logger.error).toHaveBeenCalledTimes(1));
    await unwatch();
  });

  it('rejects subscription setup when replayed processing fails', async () => {
    const pubsub = processLocalPubSub();
    await publishLifecycle(pubsub, 'replay-failure');

    await expect(
      watchWorkflowLifecycleEvents({
        pubsub,
        workflowId,
        runId,
        executionGeneration,
        options: allowProcessLocalReplay,
        callback: async () => {
          throw new Error('durable projection unavailable');
        },
      }),
    ).rejects.toThrow('durable projection unavailable');
  });

  it('fails closed when indexed replay is unavailable', async () => {
    await expect(
      watchWorkflowLifecycleEvents({
        pubsub: new EventEmitterPubSub(),
        workflowId,
        runId,
        executionGeneration,
        callback: () => {},
      }),
    ).rejects.toMatchObject({ id: 'WORKFLOW_LIFECYCLE_REPLAY_UNAVAILABLE' });
  });

  it('requires the retained log generation when resuming from a cursor', async () => {
    await expect(
      watchWorkflowLifecycleEvents({
        pubsub: processLocalPubSub(),
        workflowId,
        runId,
        executionGeneration,
        options: { afterCursor: 0, ...allowProcessLocalReplay },
        callback: () => {},
      }),
    ).rejects.toMatchObject({ id: 'WORKFLOW_LIFECYCLE_LOG_GENERATION_REQUIRED' });
  });

  it('maps retained log reset to a distinct generation mismatch', async () => {
    const pubsub = processLocalPubSub();
    await publishLifecycle(pubsub, 'old-generation');
    const oldGeneration = (await pubsub.getIndexedReplayRange(topic))!.logGeneration;
    await pubsub.clearTopic(topic);
    await publishLifecycle(pubsub, 'new-generation');

    await expect(
      watchWorkflowLifecycleEvents({
        pubsub,
        workflowId,
        runId,
        executionGeneration,
        options: {
          afterCursor: 0,
          afterLogGeneration: oldGeneration,
          ...allowProcessLocalReplay,
        },
        callback: () => {},
      }),
    ).rejects.toMatchObject({ id: 'WORKFLOW_LIFECYCLE_LOG_GENERATION_MISMATCH' });
  });

  it('reports an active generation reset once and stops the stale lifecycle subscription', async () => {
    const logger = { error: vi.fn() };
    const pubsub = new CachingPubSub(
      new EventEmitterPubSub(undefined, { logger: logger as any }),
      new InMemoryServerCache(),
      { indexedReplay },
    );
    const received: Array<{ cursor: number; logGeneration: string }> = [];
    const activeError = vi.fn(async () => {});
    const unwatch = await watchWorkflowLifecycleEvents({
      pubsub,
      workflowId,
      runId,
      executionGeneration,
      options: { ...allowProcessLocalReplay, onError: activeError },
      callback: lifecycle => {
        received.push({ cursor: lifecycle.cursor, logGeneration: lifecycle.logGeneration });
      },
    });

    await publishLifecycle(pubsub, 'old-generation');
    await vi.waitFor(() => expect(received).toHaveLength(1));
    const deliveredGeneration = received[0]!.logGeneration;
    expect(deliveredGeneration).toBe((await pubsub.getIndexedReplayRange(topic))!.logGeneration);

    await pubsub.clearTopic(topic);
    await publishLifecycle(pubsub, 'new-generation');
    await vi.waitFor(() => {
      expect(activeError).toHaveBeenCalledWith(
        expect.objectContaining({ id: 'WORKFLOW_LIFECYCLE_LOG_GENERATION_MISMATCH' }),
      );
    });

    await publishLifecycle(pubsub, 'new-generation-second');
    await Promise.resolve();
    expect(received).toEqual([{ cursor: 0, logGeneration: deliveredGeneration }]);
    expect(activeError).toHaveBeenCalledTimes(1);
    await unwatch();
  });

  it('rejects an invalid committed cursor', async () => {
    await expect(
      watchWorkflowLifecycleEvents({
        pubsub: new IndexedReplayStub(),
        workflowId,
        runId,
        executionGeneration,
        options: { afterCursor: 1.5, ...allowProcessLocalReplay },
        callback: () => {},
      }),
    ).rejects.toMatchObject({ id: 'WORKFLOW_LIFECYCLE_INVALID_CURSOR' });

    await expect(
      watchWorkflowLifecycleEvents({
        pubsub: new IndexedReplayStub(),
        workflowId,
        runId,
        executionGeneration,
        options: { afterCursor: Number.MAX_SAFE_INTEGER, ...allowProcessLocalReplay },
        callback: () => {},
      }),
    ).rejects.toMatchObject({ id: 'WORKFLOW_LIFECYCLE_INVALID_CURSOR' });
  });
});
