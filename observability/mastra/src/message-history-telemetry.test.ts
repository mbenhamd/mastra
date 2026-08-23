import { randomUUID } from 'node:crypto';

import { MockLanguageModelV2 } from '@internal/ai-sdk-v5/test';
import { Agent } from '@mastra/core/agent';
import { Mastra } from '@mastra/core/mastra';
import { MockMemory } from '@mastra/core/memory';
import { SpanType } from '@mastra/core/observability';
import type { ProcessLLMRequestArgs, Processor } from '@mastra/core/processors';
import { MockStore } from '@mastra/core/storage';
import { describe, expect, it, vi } from 'vitest';

import { Observability } from './default';
import { TestExporter } from './exporters';

class RestoredHistoryObserver implements Processor {
  readonly id = 'restored-history-observer';
  readonly name = 'Restored history observer';

  processLLMRequest(_args: ProcessLLMRequestArgs) {
    return undefined;
  }
}

function orderedByStartTime<T extends { startTime?: Date | string }>(spans: T[]): T[] {
  return [...spans].sort(
    (left, right) => new Date(left.startTime ?? 0).getTime() - new Date(right.startTime ?? 0).getTime(),
  );
}

describe('MessageHistory provider telemetry (real Agent boundary)', () => {
  it('grows provider and processor role measurements on the second turn restored from memory', async () => {
    const exporter = new TestExporter();
    const observability = new Observability({
      configs: {
        test: {
          serviceName: 'message-history-telemetry-it',
          exporters: [exporter],
        },
      },
    });
    const memory = new MockMemory({ enableMessageHistory: true });
    const threadId = randomUUID();
    const resourceId = 'message-history-telemetry-resource';
    await memory.createThread({ threadId, resourceId });

    const doGenerate = vi.fn(async () => ({
      content: [
        {
          type: 'text' as const,
          text: doGenerate.mock.calls.length === 1 ? 'FIRST_ASSISTANT_HISTORY_CANARY' : 'SECOND_ASSISTANT_REPLY',
        },
      ],
      finishReason: 'stop' as const,
      usage: { inputTokens: 10, outputTokens: 5, totalTokens: 15 },
      warnings: [],
    }));
    const agent = new Agent({
      id: 'message-history-telemetry-agent',
      name: 'Message history telemetry agent',
      instructions: 'Answer briefly.',
      model: new MockLanguageModelV2({
        provider: 'history-provider',
        modelId: 'history-model',
        doGenerate,
      }),
      memory,
      inputProcessors: [new RestoredHistoryObserver()],
    });
    const mastra = new Mastra({
      agents: { agent },
      observability,
      storage: new MockStore(),
    });
    const registeredAgent = mastra.getAgent('agent');

    try {
      await registeredAgent.generate('FIRST_USER_HISTORY_CANARY', {
        memory: { thread: threadId, resource: resourceId },
      });
      await registeredAgent.generate('SECOND_USER_HISTORY_CANARY', {
        memory: { thread: threadId, resource: resourceId },
      });

      expect(doGenerate).toHaveBeenCalledTimes(2);
      const firstProviderPrompt = JSON.stringify(doGenerate.mock.calls[0]?.[0].prompt);
      const secondProviderPrompt = JSON.stringify(doGenerate.mock.calls[1]?.[0].prompt);
      expect(firstProviderPrompt).toContain('FIRST_USER_HISTORY_CANARY');
      expect(firstProviderPrompt).not.toContain('FIRST_ASSISTANT_HISTORY_CANARY');
      expect(secondProviderPrompt).toContain('FIRST_USER_HISTORY_CANARY');
      expect(secondProviderPrompt).toContain('FIRST_ASSISTANT_HISTORY_CANARY');
      expect(secondProviderPrompt).toContain('SECOND_USER_HISTORY_CANARY');

      const [firstInference, secondInference] = orderedByStartTime(exporter.getSpansByType(SpanType.MODEL_INFERENCE));
      expect(firstInference?.attributes).toMatchObject({
        measurementState: 'measured',
        providerUserMessageCount: 1,
        providerAssistantMessageCount: 0,
      });
      expect(secondInference?.attributes).toMatchObject({
        measurementState: 'measured',
        providerUserMessageCount: 2,
        providerAssistantMessageCount: 1,
      });
      expect(secondInference?.attributes?.providerMessageCount).toBeGreaterThan(
        firstInference?.attributes?.providerMessageCount ?? Number.POSITIVE_INFINITY,
      );
      expect(secondInference?.attributes?.providerMessageBytes).toBeGreaterThan(
        firstInference?.attributes?.providerMessageBytes ?? Number.POSITIVE_INFINITY,
      );
      expect(secondInference?.attributes?.providerUserMessageBytes).toBeGreaterThan(
        firstInference?.attributes?.providerUserMessageBytes ?? Number.POSITIVE_INFINITY,
      );
      expect(secondInference?.attributes?.providerAssistantMessageBytes).toBeGreaterThan(
        firstInference?.attributes?.providerAssistantMessageBytes ?? Number.POSITIVE_INFINITY,
      );

      const requestSpans = orderedByStartTime(
        exporter.getSpansByType(SpanType.PROCESSOR_RUN).filter(span => span.entityId === 'restored-history-observer'),
      );
      expect(requestSpans).toHaveLength(2);
      const [firstRequest, secondRequest] = requestSpans;
      expect(firstRequest?.attributes).toMatchObject({
        processorMeasurementState: 'measured',
        processorInputUserMessageCount: 1,
        processorInputAssistantMessageCount: 0,
      });
      expect(secondRequest?.attributes).toMatchObject({
        processorMeasurementState: 'measured',
        processorInputUserMessageCount: 2,
        processorInputAssistantMessageCount: 1,
      });
      expect(secondRequest?.attributes?.processorInputMessageCount).toBeGreaterThan(
        firstRequest?.attributes?.processorInputMessageCount ?? Number.POSITIVE_INFINITY,
      );
      expect(secondRequest?.attributes?.processorInputMessageBytes).toBeGreaterThan(
        firstRequest?.attributes?.processorInputMessageBytes ?? Number.POSITIVE_INFINITY,
      );
      expect(secondRequest?.attributes?.processorInputUserMessageBytes).toBeGreaterThan(
        firstRequest?.attributes?.processorInputUserMessageBytes ?? Number.POSITIVE_INFINITY,
      );
      expect(secondRequest?.attributes?.processorInputAssistantMessageBytes).toBeGreaterThan(
        firstRequest?.attributes?.processorInputAssistantMessageBytes ?? Number.POSITIVE_INFINITY,
      );
      expect(exporter.getIncompleteSpans()).toHaveLength(0);
    } finally {
      await observability.shutdown();
    }
  });
});
