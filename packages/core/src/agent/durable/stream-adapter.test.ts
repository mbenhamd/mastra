import { describe, expect, it, vi } from 'vitest';
import type { Event, EventCallback, PublishEvent } from '../../events';
import { PubSub } from '../../events';
import type { IMastraLogger } from '../../logger';
import { createTerminalToolResultPartId } from '../message-list';
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

  it('recovers a terminal result from FINISH when replay starts after its data chunk', async () => {
    const pubsub = new AwaitablePubSub();
    const onChunk = vi.fn();
    const onFinish = vi.fn();
    const runId = 'finish-terminal-recovery';
    const { ready, cleanup } = createStream(pubsub, runId, { onChunk, onFinish });
    await ready;

    const terminalToolResult = {
      status: 'success' as const,
      items: [
        {
          toolName: 'answer_tool',
          toolCallId: 'answer-call',
          status: 'success' as const,
          value: { answer: 'already complete' },
        },
      ],
    };
    const terminalEnvelope = {
      id: createTerminalToolResultPartId(runId, 0),
      data: terminalToolResult,
    };
    await pubsub.publish(AGENT_STREAM_TOPIC(runId), {
      type: AgentStreamEventTypes.FINISH,
      id: 'finish-with-terminal',
      runId,
      data: {
        ...finishData(),
        terminalToolResult: terminalEnvelope,
        terminalStepFinishChunk: {
          type: 'step-finish',
          payload: { reason: 'tool-calls', terminalToolResult },
        },
      },
    });

    expect(onChunk).toHaveBeenCalledTimes(2);
    expect(onChunk).toHaveBeenNthCalledWith(1, {
      type: 'data-terminal-tool-result',
      ...terminalEnvelope,
    });
    expect(onChunk).toHaveBeenNthCalledWith(2, {
      type: 'step-finish',
      payload: { reason: 'tool-calls', terminalToolResult },
    });
    expect(onFinish).toHaveBeenCalledWith(expect.objectContaining({ terminalToolResult }));
    cleanup();
  });

  it('deduplicates terminal chunks by their stable data id across publish retries and FINISH', async () => {
    const pubsub = new AwaitablePubSub();
    const onChunk = vi.fn();
    const runId = 'terminal-chunk-retry';
    const { ready, cleanup } = createStream(pubsub, runId, { onChunk });
    await ready;

    const terminalToolResult = {
      status: 'success' as const,
      items: [
        {
          toolName: 'answer_tool',
          toolCallId: 'answer-call',
          status: 'success' as const,
          value: { answer: 'once' },
        },
      ],
    };
    const terminalChunk = {
      type: 'data-terminal-tool-result',
      id: createTerminalToolResultPartId(runId, 0),
      data: terminalToolResult,
    };
    await pubsub.publish(AGENT_STREAM_TOPIC(runId), {
      type: AgentStreamEventTypes.CHUNK,
      id: 'terminal-envelope-1',
      runId,
      data: terminalChunk,
    });
    await pubsub.publish(AGENT_STREAM_TOPIC(runId), {
      type: AgentStreamEventTypes.CHUNK,
      id: 'terminal-envelope-2',
      runId,
      data: terminalChunk,
    });
    expect(onChunk).not.toHaveBeenCalled();
    await pubsub.publish(AGENT_STREAM_TOPIC(runId), {
      type: AgentStreamEventTypes.FINISH,
      id: 'finish-after-terminal-retry',
      runId,
      data: {
        ...finishData(),
        terminalToolResult: { id: terminalChunk.id, data: terminalToolResult },
        terminalStepFinishChunk: {
          type: 'step-finish',
          payload: { reason: 'tool-calls', terminalToolResult },
        },
      },
    });

    expect(onChunk).toHaveBeenCalledTimes(2);
    expect(onChunk).toHaveBeenNthCalledWith(1, terminalChunk);
    expect(onChunk).toHaveBeenNthCalledWith(2, {
      type: 'step-finish',
      payload: { reason: 'tool-calls', terminalToolResult },
    });
    cleanup();
  });

  it('fails closed when FINISH reuses a terminal id with different data', async () => {
    const pubsub = new AwaitablePubSub();
    const onChunk = vi.fn();
    const onError = vi.fn();
    const onFinish = vi.fn();
    const runId = 'terminal-envelope-mismatch';
    const terminalId = createTerminalToolResultPartId(runId, 0);
    const { ready, cleanup } = createStream(pubsub, runId, { onChunk, onError, onFinish });
    await ready;

    await pubsub.publish(AGENT_STREAM_TOPIC(runId), {
      type: AgentStreamEventTypes.CHUNK,
      id: 'terminal-first',
      runId,
      data: {
        type: 'data-terminal-tool-result',
        id: terminalId,
        data: { status: 'success', items: [{ toolName: 'a', toolCallId: '1', status: 'success', value: 'A' }] },
      },
    });
    await pubsub.publish(AGENT_STREAM_TOPIC(runId), {
      type: AgentStreamEventTypes.FINISH,
      id: 'terminal-finish-mismatch',
      runId,
      data: {
        ...finishData(),
        terminalToolResult: {
          id: terminalId,
          data: { status: 'success', items: [{ toolName: 'a', toolCallId: '1', status: 'success', value: 'B' }] },
        },
        terminalStepFinishChunk: {
          type: 'step-finish',
          payload: {
            reason: 'tool-calls',
            terminalToolResult: {
              status: 'success',
              items: [{ toolName: 'a', toolCallId: '1', status: 'success', value: 'B' }],
            },
          },
        },
      },
    });

    expect(onChunk).not.toHaveBeenCalled();
    expect(onFinish).not.toHaveBeenCalled();
    expect(onError).toHaveBeenCalledWith(
      expect.objectContaining({ error: expect.objectContaining({ name: 'TerminalToolResultIntegrityError' }) }),
    );
    cleanup();
  });

  it('does not expose a staged terminal answer when the durable run errors', async () => {
    const pubsub = new AwaitablePubSub();
    const onChunk = vi.fn();
    const onError = vi.fn();
    const runId = 'terminal-then-error';
    const { ready, cleanup } = createStream(pubsub, runId, { onChunk, onError });
    await ready;

    await pubsub.publish(AGENT_STREAM_TOPIC(runId), {
      type: AgentStreamEventTypes.CHUNK,
      id: 'staged-terminal',
      runId,
      data: {
        type: 'data-terminal-tool-result',
        id: `${runId}:terminal-tool-result:1`,
        data: { status: 'success', items: [{ toolName: 'a', toolCallId: '1', status: 'success', value: 'A' }] },
      },
    });
    await pubsub.publish(AGENT_STREAM_TOPIC(runId), {
      type: AgentStreamEventTypes.ERROR,
      id: 'error-after-staged-terminal',
      runId,
      data: { error: { name: 'Error', message: 'finalization failed' } },
    });

    expect(onChunk).not.toHaveBeenCalled();
    expect(onError).toHaveBeenCalledWith(
      expect.objectContaining({ error: expect.objectContaining({ message: 'finalization failed' }) }),
    );
    cleanup();
  });

  it('fails closed when FINISH omits a staged terminal result', async () => {
    const pubsub = new AwaitablePubSub();
    const onChunk = vi.fn();
    const onError = vi.fn();
    const onFinish = vi.fn();
    const runId = 'terminal-finish-omission';
    const { ready, cleanup } = createStream(pubsub, runId, { onChunk, onError, onFinish });
    await ready;

    await pubsub.publish(AGENT_STREAM_TOPIC(runId), {
      type: AgentStreamEventTypes.CHUNK,
      id: 'staged-terminal',
      runId,
      data: {
        type: 'data-terminal-tool-result',
        id: `${runId}:terminal-tool-result:1`,
        data: { status: 'success', items: [{ toolName: 'a', toolCallId: '1', status: 'success', value: 'A' }] },
      },
    });
    await pubsub.publish(AGENT_STREAM_TOPIC(runId), {
      type: AgentStreamEventTypes.FINISH,
      id: 'finish-without-terminal',
      runId,
      data: finishData(),
    });

    expect(onChunk).not.toHaveBeenCalled();
    expect(onFinish).not.toHaveBeenCalled();
    expect(onError).toHaveBeenCalledWith(
      expect.objectContaining({ error: expect.objectContaining({ name: 'TerminalToolResultIntegrityError' }) }),
    );
    cleanup();
  });

  it('keeps a terminal chunk hidden until reconnect replay reaches authoritative FINISH', async () => {
    const pubsub = new AwaitablePubSub();
    const onChunk = vi.fn();
    const runId = 'terminal-missing-finish';
    const { ready, cleanup } = createStream(pubsub, runId, { onChunk });
    await ready;

    await pubsub.publish(AGENT_STREAM_TOPIC(runId), {
      type: AgentStreamEventTypes.CHUNK,
      id: 'terminal-without-finish',
      runId,
      data: {
        type: 'data-terminal-tool-result',
        id: `${runId}:terminal-tool-result:1`,
        data: { status: 'success', items: [{ toolName: 'a', toolCallId: '1', status: 'success', value: 'A' }] },
      },
    });

    expect(onChunk).not.toHaveBeenCalled();
    cleanup();
  });

  it('commits terminal and deferred step-finish chunks atomically in order', async () => {
    const pubsub = new AwaitablePubSub();
    const onChunk = vi.fn();
    const runId = 'terminal-atomic-order';
    const { ready, cleanup } = createStream(pubsub, runId, { onChunk });
    await ready;
    const terminalData = {
      status: 'success' as const,
      items: [{ toolName: 'a', toolCallId: '1', status: 'success' as const, value: 'A' }],
    };
    const terminalEnvelope = { id: createTerminalToolResultPartId(runId, 0), data: terminalData };

    await pubsub.publish(AGENT_STREAM_TOPIC(runId), {
      type: AgentStreamEventTypes.CHUNK,
      id: 'terminal-order-chunk',
      runId,
      data: { type: 'data-terminal-tool-result', ...terminalEnvelope },
    });
    await pubsub.publish(AGENT_STREAM_TOPIC(runId), {
      type: AgentStreamEventTypes.CHUNK,
      id: 'terminal-order-step-finish',
      runId,
      data: { type: 'step-finish', payload: { reason: 'tool-calls', terminalToolResult: terminalData } },
    });
    expect(onChunk).not.toHaveBeenCalled();

    await pubsub.publish(AGENT_STREAM_TOPIC(runId), {
      type: AgentStreamEventTypes.FINISH,
      id: 'terminal-order-finish',
      runId,
      data: {
        ...finishData(),
        terminalToolResult: terminalEnvelope,
        terminalStepFinishChunk: {
          type: 'step-finish',
          payload: { reason: 'tool-calls', terminalToolResult: terminalData },
        },
      },
    });

    expect(onChunk.mock.calls.map(([chunk]) => chunk.type)).toEqual(['data-terminal-tool-result', 'step-finish']);
    cleanup();
  });

  it('reconstructs terminal-before-step ordering from authoritative FINISH alone', async () => {
    const pubsub = new AwaitablePubSub();
    const onChunk = vi.fn();
    const runId = 'terminal-finish-only-order';
    const { ready, cleanup } = createStream(pubsub, runId, { onChunk });
    await ready;
    const terminalEnvelope = {
      id: createTerminalToolResultPartId(runId, 0),
      data: {
        status: 'success' as const,
        items: [{ toolName: 'a', toolCallId: '1', status: 'success' as const, value: 'A' }],
      },
    };

    await pubsub.publish(AGENT_STREAM_TOPIC(runId), {
      type: AgentStreamEventTypes.FINISH,
      id: 'authoritative-finish',
      runId,
      data: {
        ...finishData(),
        terminalToolResult: terminalEnvelope,
        terminalStepFinishChunk: {
          type: 'step-finish',
          payload: { reason: 'tool-calls', terminalToolResult: terminalEnvelope.data },
        },
      },
    });

    expect(onChunk.mock.calls.map(([chunk]) => chunk.type)).toEqual(['data-terminal-tool-result', 'step-finish']);
    cleanup();
  });

  it.each([
    [
      'terminal result without step-finish',
      {
        terminalToolResult: {
          id: 'unpaired-terminal:terminal-tool-result:1',
          data: {
            status: 'success' as const,
            items: [{ toolName: 'a', toolCallId: '1', status: 'success' as const, value: 'A' }],
          },
        },
      },
    ],
    [
      'step-finish without terminal result',
      { terminalStepFinishChunk: { type: 'step-finish', payload: { reason: 'tool-calls' } } },
    ],
  ])('fails closed for an authoritative FINISH with %s', async (_label, terminalFields) => {
    const pubsub = new AwaitablePubSub();
    const onChunk = vi.fn();
    const onError = vi.fn();
    const onFinish = vi.fn();
    const runId = `unpaired-terminal-${_label}`;
    const { ready, cleanup } = createStream(pubsub, runId, { onChunk, onError, onFinish });
    await ready;

    await pubsub.publish(AGENT_STREAM_TOPIC(runId), {
      type: AgentStreamEventTypes.FINISH,
      id: `finish-${runId}`,
      runId,
      data: { ...finishData(), ...terminalFields },
    });

    expect(onChunk).not.toHaveBeenCalled();
    expect(onFinish).not.toHaveBeenCalled();
    expect(onError).toHaveBeenCalledWith(
      expect.objectContaining({ error: expect.objectContaining({ name: 'TerminalToolResultIntegrityError' }) }),
    );
    cleanup();
  });

  it('fails closed when a terminal chunk omits its stable data id', async () => {
    const pubsub = new AwaitablePubSub();
    const onChunk = vi.fn();
    const onError = vi.fn();
    const onFinish = vi.fn();
    const runId = 'terminal-envelope-missing-id';
    const { ready, cleanup } = createStream(pubsub, runId, { onChunk, onError, onFinish });
    await ready;

    await pubsub.publish(AGENT_STREAM_TOPIC(runId), {
      type: AgentStreamEventTypes.CHUNK,
      id: 'terminal-without-data-id',
      runId,
      data: {
        type: 'data-terminal-tool-result',
        data: { status: 'success', items: [{ toolName: 'a', toolCallId: '1', status: 'success', value: 'A' }] },
      },
    });

    expect(onChunk).not.toHaveBeenCalled();
    expect(onFinish).not.toHaveBeenCalled();
    expect(onError).toHaveBeenCalledWith(
      expect.objectContaining({ error: expect.objectContaining({ name: 'TerminalToolResultIntegrityError' }) }),
    );
    cleanup();
  });

  describe.each(['chunk', 'finish'] as const)('terminal %s data validation', source => {
    it.each([
      ['undefined', undefined],
      ['null', null],
      ['wrong status', { status: 'failed', items: [] }],
      ['empty items', { status: 'success', items: [] }],
      [
        'missing item value',
        { status: 'success', items: [{ toolName: 'answer', toolCallId: 'call-1', status: 'success' }] },
      ],
      [
        'oversized data',
        {
          status: 'success',
          items: [
            {
              toolName: 'answer',
              toolCallId: 'call-1',
              status: 'success',
              value: { text: 'x'.repeat(65 * 1024) },
            },
          ],
        },
      ],
    ])('fails closed for %s', async (_label, terminalData) => {
      const pubsub = new AwaitablePubSub();
      const onChunk = vi.fn();
      const onError = vi.fn();
      const onFinish = vi.fn();
      const runId = `terminal-invalid-${source}-${_label}`;
      const { ready, cleanup } = createStream(pubsub, runId, { onChunk, onError, onFinish });
      await ready;
      const envelope = { id: `${runId}:terminal-tool-result:1`, data: terminalData };

      await pubsub.publish(
        AGENT_STREAM_TOPIC(runId),
        source === 'chunk'
          ? {
              type: AgentStreamEventTypes.CHUNK,
              id: `event-${runId}`,
              runId,
              data: { type: 'data-terminal-tool-result', ...envelope },
            }
          : {
              type: AgentStreamEventTypes.FINISH,
              id: `event-${runId}`,
              runId,
              data: {
                ...finishData(),
                terminalToolResult: envelope,
                terminalStepFinishChunk: { type: 'step-finish', payload: { reason: 'tool-calls' } },
              },
            },
      );

      expect(onChunk).not.toHaveBeenCalled();
      expect(onFinish).not.toHaveBeenCalled();
      expect(onError).toHaveBeenCalledWith(
        expect.objectContaining({ error: expect.objectContaining({ name: 'TerminalToolResultIntegrityError' }) }),
      );
      cleanup();
    });
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
