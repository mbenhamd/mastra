import type { Event } from '@mastra/core/events';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { InngestPubSub } from './pubsub';
import { InngestRun, unwrapWorkflowRealtimeData } from './run';

const realtimeMocks = vi.hoisted(() => ({
  handlers: [] as Array<(message: any) => Promise<void> | void>,
  cancel: vi.fn(async () => {}),
  subscribe: vi.fn(),
}));

function createFixture() {
  const publish = vi.fn(async () => {});
  const inngest = { realtime: { publish } } as any;
  return {
    inngest,
    publish,
    pubsub: new InngestPubSub(inngest, 'workflow-id', realtimeMocks.subscribe as any),
  };
}

describe('InngestPubSub', () => {
  beforeEach(() => {
    realtimeMocks.handlers.length = 0;
    realtimeMocks.cancel.mockClear();
    realtimeMocks.subscribe.mockReset();
    realtimeMocks.subscribe.mockImplementation(async (_options: unknown, handler: (message: any) => void) => {
      realtimeMocks.handlers.push(handler);
      return { cancel: realtimeMocks.cancel };
    });
  });

  it('routes canonical lifecycle topics by full execution identity and preserves the replay envelope', async () => {
    const { pubsub, publish } = createFixture();
    const topic = 'workflow.lifecycle.v1.workflow-id.run-1.execution-1';
    const otherTopic = 'workflow.lifecycle.v1.workflow-id.run-1.execution-2';
    const createdAt = new Date('2026-07-15T10:00:00.000Z');
    const received: Event[] = [];

    await pubsub.subscribe(topic, event => {
      received.push(event);
    });
    await pubsub.subscribe(otherTopic, () => {});
    await pubsub.publish(topic, {
      type: 'workflow-start',
      runId: 'run-1',
      data: { payload: { runId: 'run-1' } },
      id: 'lifecycle-event-id',
      createdAt,
      index: 14,
      logGeneration: 'log-generation-1',
    });

    expect(realtimeMocks.subscribe.mock.calls[0]![0]).toMatchObject({
      channel: expect.stringMatching(/^workflow-lifecycle:[0-9a-f]{32}$/),
      topics: ['lifecycle'],
    });
    expect(realtimeMocks.subscribe.mock.calls[1]![0].channel).not.toBe(
      realtimeMocks.subscribe.mock.calls[0]![0].channel,
    );
    expect(publish).toHaveBeenCalledWith(
      expect.objectContaining({
        channel: realtimeMocks.subscribe.mock.calls[0]![0].channel,
        topic: 'lifecycle',
      }),
      expect.objectContaining({
        id: 'lifecycle-event-id',
        createdAt,
        index: 14,
        logGeneration: 'log-generation-1',
      }),
    );

    const wireEvent = JSON.parse(JSON.stringify(publish.mock.calls[0]![1]));
    await realtimeMocks.handlers[0]!({ data: wireEvent });

    expect(received).toEqual([
      expect.objectContaining({
        id: 'lifecycle-event-id',
        createdAt,
        index: 14,
        logGeneration: 'log-generation-1',
      }),
    ]);
  });

  it('rejects malformed lifecycle envelopes instead of synthesizing identity', async () => {
    const { pubsub, publish } = createFixture();
    const topic = 'workflow.lifecycle.v1.workflow-id.run-1.execution-1';
    const callback = vi.fn();

    await pubsub.subscribe(topic, callback);

    await expect(
      realtimeMocks.handlers[0]!({
        data: {
          type: 'workflow-start',
          runId: 'run-1',
          data: {},
          createdAt: '2026-07-15T10:00:00.000Z',
          index: 0,
          logGeneration: 'generation-a',
        },
      }),
    ).rejects.toThrow('missing canonical identity fields');
    await expect(
      pubsub.publish(topic, {
        type: 'workflow-start',
        runId: 'run-1',
        data: {},
      }),
    ).rejects.toThrow('missing replay identity');

    expect(callback).not.toHaveBeenCalled();
    expect(publish).not.toHaveBeenCalled();
  });

  it('preserves identity, timestamp, and cursor between publish and live delivery', async () => {
    const { pubsub, publish } = createFixture();
    const topic = 'workflow.events.v2.run-1';
    const createdAt = new Date('2026-07-15T10:00:00.000Z');
    const received: Event[] = [];

    await pubsub.subscribe(topic, event => {
      received.push(event);
    });
    await pubsub.publish(topic, {
      type: 'watch',
      runId: 'run-1',
      data: { type: 'workflow-start', payload: { runId: 'run-1' } },
      id: 'stable-event-id',
      createdAt,
      index: 14,
    });

    expect(publish).toHaveBeenCalledWith(
      expect.objectContaining({ channel: 'workflow:workflow-id:run-1', topic: 'watch' }),
      expect.objectContaining({ id: 'stable-event-id', createdAt, index: 14 }),
    );

    const wireEvent = JSON.parse(JSON.stringify(publish.mock.calls[0]![1]));
    await realtimeMocks.handlers[0]!({ data: wireEvent, createdAt: new Date('2026-07-15T10:01:00.000Z') });

    expect(received).toHaveLength(1);
    expect(received[0]).toMatchObject({ id: 'stable-event-id', createdAt, index: 14, deliveryAttempt: 1 });
    expect(received[0]!.createdAt).toBeInstanceOf(Date);
  });

  it('contains async subscriber rejection instead of creating an unhandled rejection', async () => {
    const { pubsub } = createFixture();
    const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {});

    await pubsub.subscribe('workflow.events.v2.run-1', async () => {
      throw new Error('subscriber failed');
    });

    await expect(
      realtimeMocks.handlers[0]!({
        data: {
          type: 'watch',
          runId: 'run-1',
          data: {},
          id: 'event-id',
          createdAt: '2026-07-15T10:00:00.000Z',
        },
        createdAt: new Date(),
      }),
    ).resolves.toBeUndefined();
    expect(consoleError).toHaveBeenCalledWith('InngestPubSub subscriber error:', expect.any(Error));
    consoleError.mockRestore();
  });

  it('keeps local-only delivery in-process and preserves the caller envelope', async () => {
    const { pubsub, publish } = createFixture();
    const topic = 'agent.stream.run-1';
    const createdAt = new Date('2026-07-15T10:00:00.000Z');
    const received: Event[] = [];

    await pubsub.subscribe(topic, event => {
      received.push(event);
    });
    await pubsub.publish(
      topic,
      {
        type: 'chunk',
        runId: 'run-1',
        data: { text: 'hello' },
        id: 'local-event-id',
        createdAt,
        index: 7,
      },
      { localOnly: true },
    );

    expect(publish).not.toHaveBeenCalled();
    expect(received).toMatchObject([{ id: 'local-event-id', createdAt, index: 7 }]);
  });

  it('keeps watch() payload compatibility and gives watchLifecycle() the configured replay transport', async () => {
    const { inngest } = createFixture();
    const subscribeFromOffset = vi.fn(async () => {});
    const unsubscribe = vi.fn(async () => {});
    const getIndexedReplayRange = vi.fn(async () => ({
      scope: 'durable' as const,
      retentionMs: 60_000,
      maxEvents: 100,
      logGeneration: 'generation-a',
      firstCursor: 0,
      nextCursor: 0,
    }));
    const replayPubsub = {
      supportsIndexedReplay: true,
      indexedReplay: { scope: 'durable', retentionMs: 60_000, maxEvents: 100 },
      getIndexedReplayRange,
      subscribeFromOffset,
      unsubscribe,
    } as any;
    const run = new InngestRun(
      {
        workflowId: 'workflow-id',
        runId: 'run-1',
        executionEngine: {} as any,
        executionGraph: {} as any,
        serializedStepGraph: [],
        mastra: {} as any,
        workflowSteps: {},
        workflowEngineType: 'inngest' as any,
        pubsub: replayPubsub,
      },
      inngest,
    );
    const innerEvent = { type: 'workflow-start', payload: { runId: 'run-1' } };
    expect(
      unwrapWorkflowRealtimeData({
        type: 'watch',
        runId: 'run-1',
        data: innerEvent,
        id: 'stable-event-id',
        createdAt: '2026-07-15T10:00:00.000Z',
        index: 3,
      }),
    ).toEqual(innerEvent);

    const stopLifecycle = await run.watchLifecycle(vi.fn());
    expect(subscribeFromOffset).toHaveBeenCalledWith(
      'workflow.events.v2.run-1',
      0,
      expect.any(Function),
      expect.objectContaining({ logGeneration: 'generation-a' }),
    );
    await stopLifecycle();
    expect(unsubscribe).toHaveBeenCalledWith('workflow.events.v2.run-1', expect.any(Function));
  });
});
