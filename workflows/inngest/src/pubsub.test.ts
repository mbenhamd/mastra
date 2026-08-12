import { AGENT_CONTROL_TOPIC } from '@mastra/core/agent/durable';
import type { Event } from '@mastra/core/events';
import { getWorkflowLifecycleTopic } from '@mastra/core/workflows';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { InngestPubSub } from './pubsub';
import { InngestRun, unwrapWorkflowRealtimeData } from './run';

const realtimeMocks = vi.hoisted(() => ({
  handlers: [] as Array<(message: any) => Promise<void> | void>,
  cancel: vi.fn(async () => {}),
  subscribe: vi.fn(),
}));

function createFixture(workflowId = 'workflow-id') {
  const publish = vi.fn(async () => {});
  const inngest = { realtime: { publish } } as any;
  return {
    inngest,
    publish,
    pubsub: new InngestPubSub(inngest, workflowId, realtimeMocks.subscribe as any),
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
    const workflowId = 'workflow.id/review';
    const runId = 'run.1/review';
    const { pubsub, publish } = createFixture(workflowId);
    const topic = getWorkflowLifecycleTopic({ workflowId, runId, executionGeneration: 'execution.1/review' });
    const otherTopic = getWorkflowLifecycleTopic({ workflowId, runId, executionGeneration: 'execution.2/review' });
    const createdAt = new Date('2026-07-15T10:00:00.000Z');
    const received: Event[] = [];

    await pubsub.subscribe(topic, event => {
      received.push(event);
    });
    await pubsub.subscribe(otherTopic, () => {});
    await pubsub.publish(topic, {
      type: 'workflow-start',
      runId,
      data: { payload: { runId } },
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
        runId,
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

  it('shares one realtime connection when subscribers race on the same topic', async () => {
    const { pubsub } = createFixture();
    let resolveStream: ((stream: { cancel: typeof realtimeMocks.cancel }) => void) | undefined;
    realtimeMocks.subscribe.mockImplementationOnce(
      async (_options: unknown, handler: (message: any) => Promise<void> | void) => {
        realtimeMocks.handlers.push(handler);
        return new Promise<{ cancel: typeof realtimeMocks.cancel }>(resolve => {
          resolveStream = resolve;
        });
      },
    );
    const first = vi.fn();
    const second = vi.fn();

    const firstSubscription = pubsub.subscribe('workflow.events.v2.run-1', first);
    const secondSubscription = pubsub.subscribe('workflow.events.v2.run-1', second);

    expect(realtimeMocks.subscribe).toHaveBeenCalledTimes(1);
    resolveStream?.({ cancel: realtimeMocks.cancel });
    await Promise.all([firstSubscription, secondSubscription]);

    await realtimeMocks.handlers[0]!({
      data: {
        type: 'watch',
        runId: 'run-1',
        data: {},
        id: 'raced-event',
        createdAt: '2026-07-15T10:00:00.000Z',
      },
    });
    expect(first).toHaveBeenCalledTimes(1);
    expect(second).toHaveBeenCalledTimes(1);

    await pubsub.unsubscribe('workflow.events.v2.run-1', first);
    expect(realtimeMocks.cancel).not.toHaveBeenCalled();
    await pubsub.unsubscribe('workflow.events.v2.run-1', second);
    expect(realtimeMocks.cancel).toHaveBeenCalledTimes(1);
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

  it('routes durable-agent control events over the run channel and preserves the envelope', async () => {
    const { pubsub, publish } = createFixture();
    const received: Event[] = [];
    const runId = 'run.1';
    const runtimeBindingId = 'binding.1';
    const topic = AGENT_CONTROL_TOPIC(runId, runtimeBindingId);

    await pubsub.subscribe(topic, event => {
      received.push(event);
    });
    await pubsub.publish(topic, {
      type: 'abort-request',
      runId,
      data: { runtimeBindingId },
      id: 'abort-event-id',
      createdAt: new Date('2026-07-15T10:00:00.000Z'),
    });

    expect(realtimeMocks.subscribe).toHaveBeenCalledWith(
      expect.objectContaining({ channel: 'agent:run%2E1.binding%2E1', topics: ['agent-control'] }),
      expect.any(Function),
    );
    expect(publish).toHaveBeenCalledWith(
      expect.objectContaining({ channel: 'agent:run%2E1.binding%2E1', topic: 'agent-control' }),
      expect.objectContaining({ id: 'abort-event-id', runId, type: 'abort-request' }),
    );

    await realtimeMocks.handlers[0]!({ data: JSON.parse(JSON.stringify(publish.mock.calls[0]![1])) });
    expect(received).toEqual([expect.objectContaining({ id: 'abort-event-id', type: 'abort-request' })]);
  });

  it('rethrows failed durable-agent control delivery so retained replay can recover it', async () => {
    const { pubsub, publish } = createFixture();
    publish.mockRejectedValueOnce(new Error('realtime unavailable'));
    const runId = 'run-1';
    const runtimeBindingId = 'binding-1';

    await expect(
      pubsub.publish(AGENT_CONTROL_TOPIC(runId, runtimeBindingId), {
        type: 'abort-request',
        runId,
        data: { runtimeBindingId },
      }),
    ).rejects.toThrow('realtime unavailable');
  });

  it('rethrows failed terminal-result delivery so the durable final step can retry', async () => {
    const { pubsub, publish } = createFixture();
    publish.mockRejectedValueOnce(new Error('realtime unavailable'));

    await expect(
      pubsub.publish('agent.stream.run-terminal', {
        type: 'chunk',
        runId: 'run-terminal',
        data: {
          type: 'data-terminal-tool-result',
          id: 'run-terminal:terminal-tool-result:1',
          data: { status: 'success', items: [] },
        },
      }),
    ).rejects.toThrow('realtime unavailable');
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
        mastra: { getStorage: () => undefined } as any,
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

    const lifecycleIdentity = await run.getLifecycleExecutionIdentity();
    const stopLifecycle = await run.watchLifecycle(vi.fn());
    expect(subscribeFromOffset).toHaveBeenCalledWith(
      lifecycleIdentity.topic,
      0,
      expect.any(Function),
      expect.objectContaining({ logGeneration: 'generation-a' }),
    );
    await stopLifecycle();
    expect(unsubscribe).toHaveBeenCalledWith(lifecycleIdentity.topic, expect.any(Function));
  });
});
