/**
 * PF-7309: Anthropic's strict native output format rewrites free-form objects (`z.record()`) to
 * `additionalProperties: false`, so the model can only return `{}` for them. Record-bearing schemas
 * are injected into the prompt instead, and the populated reply is validated by Mastra.
 */
import { convertArrayToReadableStream, MockLanguageModelV2 } from '@internal/ai-sdk-v5/test';
import { describe, expect, it } from 'vitest';
import { z } from 'zod/v4';
import { Agent } from '../agent';

const recordSchema = z.object({ studyId: z.string(), fields: z.record(z.string(), z.string()) });
const reply = { studyId: 's1', fields: { population: 'adults', outcome: 'mortality' } };

function createAnthropicModel(captured: { options?: any }) {
  return new MockLanguageModelV2({
    provider: 'anthropic.messages',
    modelId: 'claude-haiku-5-5',
    doGenerate: async options => {
      captured.options = options;
      return {
        rawCall: { rawPrompt: null, rawSettings: {} },
        finishReason: 'stop' as const,
        usage: { inputTokens: 10, outputTokens: 20, totalTokens: 30 },
        content: [{ type: 'text' as const, text: JSON.stringify(reply) }],
        warnings: [],
      };
    },
    doStream: async options => {
      captured.options = options;
      return {
        rawCall: { rawPrompt: null, rawSettings: {} },
        warnings: [],
        stream: convertArrayToReadableStream([
          { type: 'stream-start', warnings: [] },
          { type: 'response-metadata', id: 'id-0', modelId: 'claude-haiku-5-5', timestamp: new Date(0) },
          { type: 'text-start', id: 'text-1' },
          { type: 'text-delta', id: 'text-1', delta: JSON.stringify(reply) },
          { type: 'text-end', id: 'text-1' },
          { type: 'finish', finishReason: 'stop', usage: { inputTokens: 10, outputTokens: 20, totalTokens: 30 } },
        ]),
      };
    },
  });
}

describe('Anthropic structured output with free-form records (PF-7309)', () => {
  it('sends the schema in the prompt and returns the populated record', async () => {
    const captured: { options?: any } = {};
    const agent = new Agent({ id: 'test', name: 'test', instructions: 'test', model: createAnthropicModel(captured) });

    const result = await agent.generate('extract', { structuredOutput: { schema: recordSchema } });

    expect(captured.options.responseFormat).toBeUndefined();
    const promptText = captured.options.prompt
      .flatMap((message: any) =>
        Array.isArray(message.content) ? message.content.map((part: any) => part.text ?? '') : [message.content],
      )
      .join('\n');
    expect(promptText).toContain('"fields":{"type":"object"');
    expect(result.object).toEqual(reply);
  });
});
