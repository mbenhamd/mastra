import { AGENT_CONTROL_TOPIC, createDurableAgentStream } from '@mastra/core/agent/durable';
import type { Event } from '@mastra/core/events';
import { getWorkflowLifecycleTopic } from '@mastra/core/workflows';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { InngestPubSub, OVERSIZED_EVENT_CHUNK_TYPE } from './pubsub';
import { InngestRun, unwrapWorkflowRealtimeData } from './run';

const realtimeMocks = vi.hoisted(() => ({
  handlers: [] as Array<(message: any) => Promise<void> | void>,
  close: vi.fn(() => {}),
  subscribe: vi.fn(),
}));

function createFixture(workflowId = 'workflow-id') {
  const publish = vi.fn(async () => {});
  const inngest = { realtime: { publish } } as any;
  return {
    inngest,
    publish,
    pubsub: new InngestPubSub(inngest, workflowId, undefined, realtimeMocks.subscribe as any),
  };
}

describe('InngestPubSub', () => {
  beforeEach(() => {
    realtimeMocks.handlers.length = 0;
    realtimeMocks.close.mockClear();
    realtimeMocks.subscribe.mockReset();
    realtimeMocks.subscribe.mockImplementation(
      async ({ onMessage }: { onMessage: (message: any) => Promise<void> | void }) => {
        realtimeMocks.handlers.push(onMessage);
        return { close: realtimeMocks.close };
      },
    );
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
    let resolveStream: ((stream: { close: typeof realtimeMocks.close }) => void) | undefined;
    realtimeMocks.subscribe.mockImplementationOnce(
      async ({ onMessage }: { onMessage: (message: any) => Promise<void> | void }) => {
        realtimeMocks.handlers.push(onMessage);
        return new Promise<{ close: typeof realtimeMocks.close }>(resolve => {
          resolveStream = resolve;
        });
      },
    );
    const first = vi.fn();
    const second = vi.fn();

    const firstSubscription = pubsub.subscribe('workflow.events.v2.run-1', first);
    const secondSubscription = pubsub.subscribe('workflow.events.v2.run-1', second);

    expect(realtimeMocks.subscribe).toHaveBeenCalledTimes(1);
    resolveStream?.({ close: realtimeMocks.close });
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
    expect(realtimeMocks.close).not.toHaveBeenCalled();
    await pubsub.unsubscribe('workflow.events.v2.run-1', second);
    expect(realtimeMocks.close).toHaveBeenCalledTimes(1);
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
      expect.objectContaining({
        channel: 'agent:run%2E1.binding%2E1',
        topics: ['agent-control'],
        onMessage: expect.any(Function),
      }),
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

// Injected instead of mocking 'inngest/realtime' so the suites above keep their own realtime fixture.
const subscribeMock = vi.fn();

function setup() {
  const published: unknown[] = [];
  const inngest = {
    realtime: {
      publish: vi.fn(async (_ref: unknown, data: unknown) => {
        published.push(data);
      }),
    },
  };
  return { pubsub: new InngestPubSub(inngest as any, 'wf', undefined, subscribeMock as any), published, inngest };
}

const huge = 'x'.repeat(700_000);

describe('InngestPubSub Realtime size cap (#20671)', () => {
  beforeEach(() => {
    subscribeMock.mockReset();
  });

  it('publishes agent events under the cap unchanged', async () => {
    const { pubsub, published } = setup();
    const event = { type: 'finish', runId: 'r1', data: { output: { text: 'hi', steps: [{ text: 'hi' }] } } };
    await pubsub.publish('agent.stream.r1', event);
    expect(published).toEqual([expect.objectContaining(event)]);
  });

  it('reduces an oversized finish to the fields stream consumers read', async () => {
    const { pubsub, published } = setup();
    await pubsub.publish('agent.stream.r1', {
      type: 'finish',
      runId: 'r1',
      data: {
        stepResult: { reason: 'stop' },
        output: { text: 'done', usage: { totalTokens: 3 }, steps: [{ toolResults: [{ result: huge }] }] },
      },
    });
    expect(published).toEqual([
      expect.objectContaining({
        type: 'finish',
        runId: 'r1',
        data: { stepResult: { reason: 'stop' }, output: { text: 'done', usage: { totalTokens: 3 }, steps: [] } },
      }),
    ]);
  });

  it('keeps the final text when stepResult alone is oversized', async () => {
    const { pubsub, published } = setup();
    await pubsub.publish('agent.stream.r1', {
      type: 'finish',
      runId: 'r1',
      data: {
        stepResult: { reason: 'stop', blob: huge },
        output: { text: 'done', usage: { totalTokens: 3 }, steps: [] },
      },
    });
    expect(published).toEqual([
      expect.objectContaining({
        type: 'finish',
        runId: 'r1',
        data: { stepResult: { reason: 'stop' }, output: { steps: [], text: 'done', usage: { totalTokens: 3 } } },
      }),
    ]);
  });

  it('replaces an oversized non-terminal event with a placeholder chunk and logs a warning', async () => {
    const { pubsub, published } = setup();
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    await pubsub.publish('agent.stream.r1', {
      type: 'chunk',
      runId: 'r1',
      data: { type: 'tool-result', payload: { result: huge } },
    });
    expect(published).toEqual([
      expect.objectContaining({
        type: 'chunk',
        runId: 'r1',
        data: {
          type: OVERSIZED_EVENT_CHUNK_TYPE,
          transient: true,
          data: { eventType: 'chunk', chunkType: 'tool-result', bytes: expect.any(Number), limit: 480 * 1024 },
        },
      }),
    ]);
    expect(warn).toHaveBeenCalledOnce();
    warn.mockRestore();
  });

  it('uses the provided logger instead of the console', async () => {
    const logger = { warn: vi.fn() };
    const inngest = { realtime: { publish: vi.fn(async () => {}) } };
    const pubsub = new InngestPubSub(inngest as any, 'wf', logger as any);
    await pubsub.publish('agent.stream.r1', { type: 'step-start', runId: 'r1', data: { request: huge } });
    expect(logger.warn).toHaveBeenCalledWith(expect.stringContaining('step-start'));
  });

  it('salvages a truncated terminal message and turns a truncated non-terminal one into a placeholder', async () => {
    let onMessage: (m: unknown) => void = () => {};
    subscribeMock.mockImplementation(async (opts: { onMessage: (m: unknown) => void }) => {
      onMessage = opts.onMessage;
      return { close: vi.fn() };
    });
    const { pubsub } = setup();
    const received: any[] = [];
    await pubsub.subscribe('agent.stream.r1', event => received.push(event));

    onMessage({ data: '{"type":"chunk","runId":"r1","data":{"type":"tool-result","payload":{"result":"xxxx' });
    onMessage({ data: '{"type":"finish","runId":"r1","data":{"stepResult":{"reason":"stop"},"output":{"steps":[{"te' });

    expect(received).toHaveLength(2);
    expect(received[0]).toMatchObject({
      type: 'chunk',
      data: { type: OVERSIZED_EVENT_CHUNK_TYPE, data: { eventType: 'chunk', chunkType: 'tool-result' } },
    });
    expect(received[1]).toMatchObject({
      type: 'finish',
      runId: 'r1',
      data: { output: { steps: [] }, stepResult: { reason: 'stop' } },
    });
  });

  it('keeps an oversized error readable so the stream can close with onError', async () => {
    const { pubsub, published } = setup();
    await pubsub.publish('agent.stream.r1', {
      type: 'error',
      runId: 'r1',
      data: { error: { name: 'ProviderError', message: huge, stack: huge } },
    });
    const sent = published[0] as any;
    expect(sent.data.error.name).toBe('ProviderError');
    expect(typeof sent.data.error.message).toBe('string');
    expect(sent.data.error.stack).toBeUndefined();
    expect(Buffer.byteLength(JSON.stringify(sent))).toBeLessThan(480 * 1024);
  });

  it('salvages a truncated error into an envelope with error.message', async () => {
    let onMessage: (m: unknown) => void = () => {};
    subscribeMock.mockImplementation(async (opts: { onMessage: (m: unknown) => void }) => {
      onMessage = opts.onMessage;
      return { close: vi.fn() };
    });
    const { pubsub } = setup();
    const received: any[] = [];
    await pubsub.subscribe('agent.stream.r1', event => received.push(event));
    onMessage({ data: '{"type":"error","runId":"r1","data":{"error":{"name":"E","message":"xxxx' });
    expect(received[0]).toMatchObject({ type: 'error', runId: 'r1', data: { error: { name: 'Error' } } });
    expect(typeof received[0].data.error.message).toBe('string');
  });

  it.each([
    ['oversized', (sent: unknown) => sent],
    ['truncated by Realtime', (sent: unknown) => JSON.stringify(sent).slice(0, 200)],
  ])('closes an attached stream and fires onError for an %s error', async (_label, deliver) => {
    let onMessage: (m: unknown) => void = () => {};
    subscribeMock.mockImplementation(async (opts: { onMessage: (m: unknown) => void }) => {
      onMessage = opts.onMessage;
      return { close: vi.fn() };
    });
    const { pubsub, published } = setup();
    const onError = vi.fn();
    const { output, ready, cleanup } = createDurableAgentStream({
      pubsub,
      runId: 'r1',
      messageId: 'm1',
      model: { modelId: 'm', provider: 'p', version: 'v3' },
      onError,
    });
    await ready;

    const event = { type: 'error', runId: 'r1', data: { error: { name: 'ProviderError', message: huge } } };
    await pubsub.publish('agent.stream.r1', event);
    // Without a reduced envelope Realtime would deliver the full event cut off as a string.
    onMessage({ data: deliver(published[0]) });

    const chunks: any[] = [];
    for await (const chunk of output.fullStream) chunks.push(chunk);
    cleanup();
    expect(chunks.some(c => c.type === 'error')).toBe(true);
    expect(onError).toHaveBeenCalledTimes(1);
  });
});
