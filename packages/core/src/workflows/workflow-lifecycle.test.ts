import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { InMemoryServerCache } from '../cache/inmemory';
import { CachingPubSub } from '../events/caching-pubsub';
import { EventEmitterPubSub } from '../events/event-emitter';
import { PubSub } from '../events/pubsub';
import type { Event, EventCallback } from '../events/types';
import { Mastra } from '../mastra';
import { MockStore } from '../storage/mock';
import { ChunkFrom } from '../stream/types';
import { createEventedWorkflow, createWorkflow } from './create';
import type { WorkflowStreamEvent } from './types';
import { createStep } from './workflow';
import { watchWorkflowLifecycleEvents } from './workflow-lifecycle';

const workflowId = 'review-workflow';
const runId = 'review-run';
const topic = `workflow.events.v2.${runId}`;
const indexedReplay = { retentionMs: 60_000, maxEvents: 100 };
const allowProcessLocalReplay = { allowProcessLocalReplay: true };
const stubLogGeneration = 'stub-log-generation';

function processLocalPubSub(inner = new EventEmitterPubSub()) {
  return new CachingPubSub(inner, new InMemoryServerCache(), { indexedReplay });
}

function streamEvent(label: string): WorkflowStreamEvent {
  return {
    type: 'workflow-start',
    from: ChunkFrom.WORKFLOW,
    runId,
    payload: { workflowId: `${workflowId}-${label}` },
  };
}

function event(index: number): Event {
  return {
    type: 'watch',
    id: `event-${index}`,
    runId,
    createdAt: new Date(`2026-07-15T00:00:0${index}.000Z`),
    index,
    logGeneration: stubLogGeneration,
    data: streamEvent(String(index)),
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
    await pubsub.publish(topic, { type: 'watch', runId, data: streamEvent('default') });

    const cursors: number[] = [];
    const unwatch = await run.watchLifecycle(event => {
      cursors.push(event.cursor);
    }, allowProcessLocalReplay);

    expect(cursors).toEqual([0]);
    await unwatch();
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
    await vi.waitFor(() => expect(received.some(item => item.type === 'workflow-step-finish')).toBe(true));
    expect(received.some(item => item.type === 'workflow-step-start')).toBe(true);
    expect(received.map(item => item.cursor)).toEqual(received.map((_, index) => index));
    expect(new Set(received.map(item => item.eventId)).size).toBe(received.length);
    await unwatch();
  });

  it('uses Mastra PubSub for an evented Run lifecycle subscription', async () => {
    const pubsub = processLocalPubSub();
    const workflow = makeWorkflow('evented');
    new Mastra({
      workflows: { [workflowId]: workflow },
      storage: new MockStore(),
      pubsub,
    });
    const run = await workflow.createRun({ runId });
    await pubsub.publish(topic, { type: 'watch', runId, data: streamEvent('evented') });

    const cursors: number[] = [];
    const unwatch = await run.watchLifecycle(event => {
      cursors.push(event.cursor);
    }, allowProcessLocalReplay);

    expect(cursors).toEqual([0]);
    await unwatch();
  });

  it('replays strictly after the committed cursor and keeps live delivery ordered', async () => {
    const cache = new InMemoryServerCache();
    const pubsub = new CachingPubSub(new EventEmitterPubSub(), cache, { indexedReplay });

    await pubsub.publish(topic, { type: 'watch', runId, data: streamEvent('zero') });
    await pubsub.publish(topic, { type: 'watch', runId, data: streamEvent('one') });
    await pubsub.publish(topic, { type: 'watch', runId, data: streamEvent('two') });

    const received: Array<{ cursor: number; eventId: string; workflowId: string; runId: string }> = [];
    let resolveLive!: () => void;
    const liveDelivered = new Promise<void>(resolve => {
      resolveLive = resolve;
    });

    const unwatch = await watchWorkflowLifecycleEvents({
      pubsub,
      workflowId,
      runId,
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

    await pubsub.publish(topic, { type: 'watch', runId, data: streamEvent('three') });
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

  it('requires an explicit opt-in for process-local replay', async () => {
    await expect(
      watchWorkflowLifecycleEvents({
        pubsub: processLocalPubSub(),
        workflowId,
        runId,
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
      options: allowProcessLocalReplay,
      callback: async () => {
        throw new Error('projection failed without redelivery');
      },
    });

    await pubsub.publish(topic, { type: 'watch', runId, data: streamEvent('live-failure') });

    await vi.waitFor(() => expect(logger.error).toHaveBeenCalledTimes(1));
    await unwatch();
  });

  it('rejects subscription setup when replayed processing fails', async () => {
    const pubsub = processLocalPubSub();
    await pubsub.publish(topic, { type: 'watch', runId, data: streamEvent('replay-failure') });

    await expect(
      watchWorkflowLifecycleEvents({
        pubsub,
        workflowId,
        runId,
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
        options: { afterCursor: 0, ...allowProcessLocalReplay },
        callback: () => {},
      }),
    ).rejects.toMatchObject({ id: 'WORKFLOW_LIFECYCLE_LOG_GENERATION_REQUIRED' });
  });

  it('maps retained log reset to a distinct generation mismatch', async () => {
    const pubsub = processLocalPubSub();
    await pubsub.publish(topic, { type: 'watch', runId, data: streamEvent('old-generation') });
    const oldGeneration = (await pubsub.getIndexedReplayRange(topic))!.logGeneration;
    await pubsub.clearTopic(topic);
    await pubsub.publish(topic, { type: 'watch', runId, data: streamEvent('new-generation') });

    await expect(
      watchWorkflowLifecycleEvents({
        pubsub,
        workflowId,
        runId,
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
      options: { ...allowProcessLocalReplay, onError: activeError },
      callback: lifecycle => {
        received.push({ cursor: lifecycle.cursor, logGeneration: lifecycle.logGeneration });
      },
    });

    await pubsub.publish(topic, { type: 'watch', runId, data: streamEvent('old-generation') });
    await vi.waitFor(() => expect(received).toHaveLength(1));
    const deliveredGeneration = received[0]!.logGeneration;
    expect(deliveredGeneration).toBe((await pubsub.getIndexedReplayRange(topic))!.logGeneration);

    await pubsub.clearTopic(topic);
    await pubsub.publish(topic, { type: 'watch', runId, data: streamEvent('new-generation') });
    await vi.waitFor(() => {
      expect(activeError).toHaveBeenCalledWith(
        expect.objectContaining({ id: 'WORKFLOW_LIFECYCLE_LOG_GENERATION_MISMATCH' }),
      );
    });

    await pubsub.publish(topic, { type: 'watch', runId, data: streamEvent('new-generation-second') });
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
        options: { afterCursor: 1.5, ...allowProcessLocalReplay },
        callback: () => {},
      }),
    ).rejects.toMatchObject({ id: 'WORKFLOW_LIFECYCLE_INVALID_CURSOR' });

    await expect(
      watchWorkflowLifecycleEvents({
        pubsub: new IndexedReplayStub(),
        workflowId,
        runId,
        options: { afterCursor: Number.MAX_SAFE_INTEGER, ...allowProcessLocalReplay },
        callback: () => {},
      }),
    ).rejects.toMatchObject({ id: 'WORKFLOW_LIFECYCLE_INVALID_CURSOR' });
  });
});
