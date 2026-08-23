/**
 * Harness subagent request-lineage integration.
 *
 * This crosses the real supervisor Agent -> spawn_subagent -> child Agent
 * provider pipeline and inspects completed MODEL_INFERENCE spans from the real
 * TestExporter. The models are deterministic provider mocks; Harness, Agent,
 * observability, and span serialization are production implementations.
 */
import { MockLanguageModelV2, convertArrayToReadableStream } from '@internal/ai-sdk-v5/test';
import { Agent } from '@mastra/core/agent';
import { Harness } from '@mastra/core/harness/v1';
import { Mastra } from '@mastra/core/mastra';
import { MockMemory } from '@mastra/core/memory';
import { SpanType } from '@mastra/core/observability';
import { InMemoryStore } from '@mastra/core/storage';
import { describe, expect, it, vi } from 'vitest';

import { Observability } from './default';
import { TestExporter } from './exporters';

const usage = { inputTokens: 4, outputTokens: 2, totalTokens: 6 };

function toolCallModel(options: {
  provider: string;
  modelId: string;
  responseId: string;
  toolCallId: string;
  toolName: string;
  input: object;
}) {
  const doStream = vi.fn(async () => ({
    rawCall: { rawPrompt: null, rawSettings: {} },
    warnings: [],
    stream: convertArrayToReadableStream([
      { type: 'stream-start', warnings: [] },
      { type: 'response-metadata', id: options.responseId, modelId: options.modelId, timestamp: new Date(0) },
      {
        type: 'tool-call',
        toolCallType: 'function',
        toolCallId: options.toolCallId,
        toolName: options.toolName,
        input: JSON.stringify(options.input),
        providerExecuted: false,
      },
      { type: 'finish', finishReason: 'tool-calls', usage },
    ]),
  }));
  return {
    doStream,
    model: new MockLanguageModelV2({
      provider: options.provider,
      modelId: options.modelId,
      doStream,
    }),
  };
}

async function settle(exporter: TestExporter, maxMs = 2_000): Promise<void> {
  let previousCount = -1;
  for (let waited = 0; waited < maxMs; waited += 20) {
    const count = exporter.getAllSpans().length;
    if (count > 0 && count === previousCount) return;
    previousCount = count;
    await new Promise(resolve => setTimeout(resolve, 20));
  }
}

describe('Harness subagent provider lineage (real exporter)', () => {
  it('joins supervisor and child inference spans with only the approved opaque app key', async () => {
    const lineageKey = 'turnCorrelationId';
    const lineageValue = 'opaque-turn-5049';
    const functionalRequestId = 'FUNCTIONAL_REQUEST_ID_CANARY';
    const userIdCanary = 'PRIVATE_USER_ID_CANARY';
    const contentCanary = 'PRIVATE_INSTRUCTION_CONTENT_CANARY';
    const exporter = new TestExporter();

    const rootProvider = toolCallModel({
      provider: 'root-provider',
      modelId: 'root-model',
      responseId: 'root-response',
      toolCallId: 'spawn-child',
      toolName: 'spawn_subagent',
      input: {
        agentType: 'worker',
        task: 'Return the verified child outcome.',
        delivery: 'final',
      },
    });
    const childProvider = toolCallModel({
      provider: 'child-provider',
      modelId: 'child-model',
      responseId: 'child-response',
      toolCallId: 'report-child-outcome',
      toolName: 'report_subagent_outcome',
      input: {
        outcome: 'completed',
        summary: 'child lineage verified',
        evidence: [{ kind: 'analysis', description: 'Deterministic provider fixture completed.' }],
      },
    });

    const storage = new InMemoryStore();
    const rootAgent = new Agent({
      id: 'root-agent',
      name: 'root-agent',
      instructions: 'Delegate once.',
      model: rootProvider.model,
      memory: new MockMemory({ storage }),
    });
    const childAgent = new Agent({
      id: 'child-agent',
      name: 'child-agent',
      instructions: 'Report the completed outcome.',
      model: childProvider.model,
      memory: new MockMemory({ storage }),
    });
    const mastra = new Mastra({
      agents: { 'root-agent': rootAgent, 'child-agent': childAgent } as any,
      storage,
      observability: new Observability({
        configs: {
          test: {
            serviceName: 'harness-subagent-lineage-it',
            exporters: [exporter],
            requestContextKeys: [lineageKey],
          },
        },
      }),
    });
    const harness = new Harness({
      mastra,
      modes: [
        { id: 'root-mode', agentId: 'root-agent' },
        { id: 'child-mode', agentId: 'child-agent' },
      ],
      defaultModeId: 'root-mode',
      subagents: {
        inheritRequestContextAppKeys: [lineageKey],
        maxDepth: 1,
        types: {
          worker: {
            agentId: 'child-agent',
            modeId: 'child-mode',
            description: 'Deterministic lineage worker',
            defaultModelId: 'child-model',
          },
        },
      },
    });

    try {
      const session = await harness.session({ resourceId: 'lineage-user', threadId: { fresh: true } });
      const result = await session.message({
        content: 'Delegate the lineage check.',
        requestContext: {
          app: {
            [lineageKey]: lineageValue,
            'papersflow.requestId': functionalRequestId,
            'papersflow.userId': userIdCanary,
            instructions: contentCanary,
          },
        },
      });
      expect(result.text).toBe('child lineage verified');
      expect(rootProvider.doStream).toHaveBeenCalledTimes(1);
      expect(childProvider.doStream).toHaveBeenCalledTimes(1);

      await settle(exporter);
      const inferences = exporter.getSpansByType(SpanType.MODEL_INFERENCE);
      expect(inferences).toHaveLength(2);
      expect(new Set(inferences.map(span => span.attributes?.model))).toEqual(new Set(['root-model', 'child-model']));
      for (const inference of inferences) {
        expect(inference.requestContext?.[lineageKey]).toBe(lineageValue);
        expect(inference.metadata?.[lineageKey]).toBe(lineageValue);
        const exportedCorrelation = JSON.stringify({
          requestContext: inference.requestContext,
          metadata: inference.metadata,
        });
        expect(exportedCorrelation).not.toContain('papersflow.requestId');
        expect(exportedCorrelation).not.toContain(functionalRequestId);
        expect(exportedCorrelation).not.toContain('papersflow.userId');
        expect(exportedCorrelation).not.toContain(userIdCanary);
        expect(exportedCorrelation).not.toContain(contentCanary);
      }
      expect(exporter.getIncompleteSpans()).toHaveLength(0);
    } finally {
      await harness.shutdown();
    }
  });
});
