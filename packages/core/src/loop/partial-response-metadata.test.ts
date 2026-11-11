/**
 * Providers such as OpenRouter split response metadata across several `response-metadata`
 * chunks (an id-only chunk, then a modelId-only chunk). Later chunks must not erase fields
 * an earlier chunk already supplied (upstream #26584, PF-8317).
 */

import type { LanguageModelV2 } from '@ai-sdk/provider-v5';
import { MockLanguageModelV2, convertArrayToReadableStream } from '@internal/ai-sdk-v5/test';
import { describe, expect, it } from 'vitest';
import { Agent } from '../agent';

// MockLanguageModelV2: AIMock speaks the OpenAI wire format, whose provider emits a single
// response-metadata chunk, so it cannot express split metadata.
function createSplitMetadataModel() {
  return new MockLanguageModelV2({
    doStream: async () => ({
      stream: convertArrayToReadableStream([
        { type: 'stream-start', warnings: [] },
        { type: 'response-metadata', id: 'gen-repro', timestamp: new Date(0) },
        { type: 'response-metadata', modelId: 'provider-model' },
        { type: 'text-start', id: 'text-1' },
        { type: 'text-delta', id: 'text-1', delta: 'Hello' },
        { type: 'text-end', id: 'text-1' },
        { type: 'finish', finishReason: 'stop', usage: { inputTokens: 5, outputTokens: 5, totalTokens: 10 } },
      ]),
      rawCall: { rawPrompt: null, rawSettings: {} },
    }),
  });
}

describe('partial response-metadata chunks', () => {
  it('keeps the response id when a later chunk only carries the modelId', async () => {
    const agent = new Agent({
      id: 'partial-metadata-agent',
      name: 'Partial Metadata Agent',
      instructions: 'Test',
      model: createSplitMetadataModel() as LanguageModelV2,
    });

    const stepResponses: any[] = [];
    const finishResponses: any[] = [];
    const result = await agent.stream('hi', {
      onStepFinish: (payload: any) => {
        stepResponses.push(payload.response);
      },
      onFinish: (payload: any) => {
        finishResponses.push(payload.response);
      },
    });
    await result.consumeStream();
    const response = await result.response;

    expect(stepResponses.length).toBeGreaterThan(0);
    expect(finishResponses).toHaveLength(1);
    for (const observed of [response, ...stepResponses, ...finishResponses]) {
      expect(observed.id).toBe('gen-repro');
      expect(observed.modelId).toBe('provider-model');
      expect(observed.timestamp).toEqual(new Date(0));
    }
  });
});
