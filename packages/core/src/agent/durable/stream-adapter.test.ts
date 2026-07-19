import { describe, expect, it, vi } from 'vitest';
import type { Event, EventCallback, PublishEvent } from '../../events';
import { PubSub } from '../../events';
import type { IMastraLogger } from '../../logger';
import { AGENT_STREAM_TOPIC, AgentStreamEventTypes } from './constants';
import { createDurableAgentStream } from './stream-adapter';
import type { DurableAgentStreamOptions } from './stream-adapter';

class AwaitablePubSub extends PubSub {
  private callbacks = new Map<string, Set<EventCallback>>();

  async publish(topic: string, event: PublishEvent): Promise<void> {
    const delivered: Event = {
      ...event,
      id: event.id ?? crypto.randomUUID(),
      createdAt: event.createdAt ?? new Date(),
    };
    await Promise.all([...(this.callbacks.get(topic) ?? [])].map(callback => callback(delivered)));
  }

  async subscribe(topic: string, callback: EventCallback): Promise<void> {
    const callbacks = this.callbacks.get(topic) ?? new Set<EventCallback>();
    callbacks.add(callback);
    this.callbacks.set(topic, callbacks);
  }

  async unsubscribe(topic: string, callback: EventCallback): Promise<void> {
    const callbacks = this.callbacks.get(topic);
    callbacks?.delete(callback);
    if (callbacks?.size === 0) this.callbacks.delete(topic);
  }

  async flush(): Promise<void> {}
}

function finishData(text = 'done') {
  return {
    output: {
      text,
      usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
      steps: [],
    },
    stepResult: {
      reason: 'stop' as const,
      warnings: [],
      isContinued: false,
    },
  };
}

function createStream(
  pubsub: PubSub,
  runId: string,
  callbacks: Partial<
    Pick<
      DurableAgentStreamOptions,
      'closeOnSuspend' | 'logger' | 'onChunk' | 'onError' | 'onFinish' | 'onStreamFinished' | 'onSuspended'
    >
  > = {},
) {
  return createDurableAgentStream({
    pubsub,
    runId,
    messageId: `${runId}-message`,
    model: { modelId: 'test-model', provider: 'test', version: 'v3' },
    ...callbacks,
  });
}

describe('createDurableAgentStream callback delivery', () => {
  it('delivers a redelivered chunk to onChunk exactly once', async () => {
    const pubsub = new AwaitablePubSub();
    const onChunk = vi.fn();
    const runId = 'chunk-redelivery';
    const { ready, cleanup } = createStream(pubsub, runId, { onChunk });
    await ready;

    const chunkEvent = {
      type: AgentStreamEventTypes.CHUNK,
      id: 'chunk-1',
      runId,
      createdAt: new Date(),
      data: { type: 'text-delta', payload: { text: 'same logical chunk' } },
    };
    await pubsub.publish(AGENT_STREAM_TOPIC(runId), chunkEvent);
    await pubsub.publish(AGENT_STREAM_TOPIC(runId), { ...chunkEvent, deliveryAttempt: 2 });
    await pubsub.publish(AGENT_STREAM_TOPIC(runId), { ...chunkEvent, id: 'chunk-2' });

    expect(onChunk).toHaveBeenCalledTimes(2);
    cleanup();
  });

  it('lets the first terminal event settle callbacks exactly once', async () => {
    const pubsub = new AwaitablePubSub();
    const onChunk = vi.fn();
    const onFinish = vi.fn();
    const onError = vi.fn();
    const runId = 'first-terminal-wins';
    const { ready, cleanup } = createStream(pubsub, runId, { onChunk, onFinish, onError });
    await ready;

    const terminal = {
      type: AgentStreamEventTypes.FINISH,
      id: 'finish-1',
      runId,
      createdAt: new Date(),
      data: finishData(),
    };
    await pubsub.publish(AGENT_STREAM_TOPIC(runId), terminal);
    await pubsub.publish(AGENT_STREAM_TOPIC(runId), { ...terminal, deliveryAttempt: 2 });
    await pubsub.publish(AGENT_STREAM_TOPIC(runId), {
      type: AgentStreamEventTypes.ERROR,
      id: 'late-error',
      runId,
      data: { error: { name: 'Error', message: 'late duplicate terminal' } },
    });
    await pubsub.publish(AGENT_STREAM_TOPIC(runId), {
      type: AgentStreamEventTypes.CHUNK,
      id: 'late-chunk',
      runId,
      data: { type: 'text-delta', payload: { text: 'late' } },
    });

    expect(onFinish).toHaveBeenCalledTimes(1);
    expect(onError).not.toHaveBeenCalled();
    expect(onChunk).not.toHaveBeenCalled();
    cleanup();
  });

  it('contains callback failures without reopening delivery', async () => {
    const pubsub = new AwaitablePubSub();
    const logger = { error: vi.fn() } as unknown as IMastraLogger;
    const onChunk = vi.fn().mockRejectedValue(new Error('chunk callback failed'));
    const onFinish = vi.fn().mockRejectedValue(new Error('finish callback failed'));
    const onStreamFinished = vi.fn();
    const runId = 'callback-failures';
    const { ready, cleanup } = createStream(pubsub, runId, {
      logger,
      onChunk,
      onFinish,
      onStreamFinished,
    });
    await ready;

    const chunkEvent = {
      type: AgentStreamEventTypes.CHUNK,
      id: 'throwing-chunk',
      runId,
      createdAt: new Date(),
      data: { type: 'text-delta', payload: { text: 'still accepted' } },
    };
    await pubsub.publish(AGENT_STREAM_TOPIC(runId), chunkEvent);
    await pubsub.publish(AGENT_STREAM_TOPIC(runId), { ...chunkEvent, deliveryAttempt: 2 });
    await pubsub.publish(AGENT_STREAM_TOPIC(runId), {
      type: AgentStreamEventTypes.FINISH,
      id: 'throwing-finish',
      runId,
      data: finishData(),
    });

    expect(onChunk).toHaveBeenCalledTimes(1);
    expect(onFinish).toHaveBeenCalledTimes(1);
    expect(onStreamFinished).toHaveBeenCalledTimes(1);
    expect(logger.error).toHaveBeenCalledTimes(2);
    cleanup();
  });

  it('closes a settled suspend stream when onSuspended fails and ignores redelivery', async () => {
    const pubsub = new AwaitablePubSub();
    const logger = { error: vi.fn() } as unknown as IMastraLogger;
    const onSuspended = vi.fn().mockRejectedValue(new Error('suspended callback failed'));
    const runId = 'suspended-callback-failure';
    const { output, ready, cleanup } = createStream(pubsub, runId, {
      closeOnSuspend: true,
      logger,
      onSuspended,
    });
    await ready;

    const fullOutputPromise = output.getFullOutput();
    await pubsub.publish(AGENT_STREAM_TOPIC(runId), {
      type: AgentStreamEventTypes.CHUNK,
      id: 'suspended-tool-chunk',
      runId,
      data: {
        type: 'tool-call-suspended',
        runId,
        payload: { toolCallId: 'tool-call-1', toolName: 'approvalTool' },
      },
    });

    const suspendedEvent = {
      type: AgentStreamEventTypes.SUSPENDED,
      id: 'suspended-terminal',
      runId,
      data: {
        type: 'approval' as const,
        toolCallId: 'tool-call-1',
        toolName: 'approvalTool',
      },
    };
    await pubsub.publish(AGENT_STREAM_TOPIC(runId), suspendedEvent);
    await pubsub.publish(AGENT_STREAM_TOPIC(runId), {
      ...suspendedEvent,
      deliveryAttempt: 2,
    });

    await expect(fullOutputPromise).resolves.toMatchObject({ finishReason: 'suspended' });
    expect(onSuspended).toHaveBeenCalledTimes(1);
    expect(logger.error).toHaveBeenCalledTimes(1);
    cleanup();
  });
});
