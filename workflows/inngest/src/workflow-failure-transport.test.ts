import { AGENT_CONTROL_TOPIC } from '@mastra/core/agent/durable';
import type { PubSub } from '@mastra/core/events';
import { describe, expect, it, vi } from 'vitest';
import { finalizeDurableAgentFailureTransport } from './workflow';

describe('durable-agent failure transport finalization', () => {
  it('retries failed ERROR publication before clearing retained abort intent', async () => {
    const publish = vi
      .fn()
      .mockRejectedValueOnce(new Error('transient realtime failure'))
      .mockResolvedValueOnce(undefined);
    const clearTopicOrThrow = vi.fn().mockResolvedValue(undefined);
    const pubsub = { publish, clearTopicOrThrow } as unknown as PubSub;
    const input = { runId: 'failure-run', runtimeBindingId: 'failure-binding' };

    await expect(finalizeDurableAgentFailureTransport(pubsub, input, new Error('workflow failed'))).rejects.toThrow(
      'transient realtime failure',
    );
    expect(clearTopicOrThrow).not.toHaveBeenCalled();

    await expect(finalizeDurableAgentFailureTransport(pubsub, input, new Error('workflow failed'))).resolves.toBe(
      undefined,
    );
    expect(publish).toHaveBeenCalledTimes(2);
    expect(clearTopicOrThrow).toHaveBeenCalledOnce();
    expect(clearTopicOrThrow).toHaveBeenCalledWith(AGENT_CONTROL_TOPIC('failure-run', 'failure-binding'));
  });
});
