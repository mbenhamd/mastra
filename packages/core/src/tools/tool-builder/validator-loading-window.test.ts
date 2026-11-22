import { MockLanguageModelV1 } from '@internal/ai-sdk-v4/test';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { Agent } from '../../agent';
import { Mastra } from '../../mastra';
import { RequestContext } from '../../request-context';
import { jsonSchemaValidatorLoading, toStandardSchema } from '../../schema';
import { createTool } from '../tool';
import { CoreToolBuilder } from './builder';

/**
 * Holds the JSON Schema validator startup window open: another adapter copy's load, registered in the
 * process-wide set the way a bundled copy registers it, that settles only when released.
 */
function holdValidatorLoading() {
  const loads = (globalThis as Record<symbol, Set<Promise<void>>>)[
    Symbol.for('mastra.schemaCompat.jsonSchemaValidatorLoads')
  ]!;
  let release!: () => void;
  const load = new Promise<void>(resolve => {
    release = resolve;
  });
  loads.add(load);
  void load.then(() => {
    loads.delete(load);
  });
  return release;
}

const nameSchema = toStandardSchema({
  type: 'object',
  properties: { name: { type: 'string' } },
  required: ['name'],
  additionalProperties: false,
});

describe('tool input validation while the JSON Schema validator is loading', () => {
  let release: (() => void) | undefined;
  afterEach(() => {
    release?.();
    release = undefined;
  });

  it('refuses an AI SDK v4 tool call with invalid input, never running the tool', async () => {
    const execute = vi.fn(async () => ({ found: true }));
    const findUser = createTool({ id: 'find_user', description: 'Find a user', inputSchema: nameSchema, execute });
    const model = new MockLanguageModelV1({
      doGenerate: async () => ({
        rawCall: { rawPrompt: null, rawSettings: {} },
        finishReason: 'tool-calls',
        usage: { promptTokens: 10, completionTokens: 20 },
        text: undefined,
        toolCalls: [
          { toolCallType: 'function', toolCallId: 'call-1', toolName: 'findUser', args: JSON.stringify({ name: 42 }) },
        ],
      }),
    });
    const agent = new Mastra({
      agents: {
        finder: new Agent({ id: 'finder', name: 'Finder', instructions: 'Find users.', model, tools: { findUser } }),
      },
      logger: false,
    }).getAgent('finder');

    // Hold the window for the whole call, so the refusal can only come from the loading window.
    release = holdValidatorLoading();
    await expect(agent.generateLegacy('Find Dero', { maxSteps: 1, toolChoice: 'required' })).rejects.toThrow(
      /Invalid arguments for tool findUser/,
    );
    expect(jsonSchemaValidatorLoading()).toBe(true);

    // Settle the window: a call that got past validation would now reach execute.
    release();
    await new Promise(resolve => setTimeout(resolve, 50));
    expect(execute).not.toHaveBeenCalled();
  });

  it('holds approval input validation until the validator has loaded, then refuses invalid input', async () => {
    const tool = createTool({
      id: 'find_user',
      description: 'Find a user',
      inputSchema: nameSchema,
      execute: async () => ({}),
    });
    const coreTool = new CoreToolBuilder({
      originalTool: tool,
      options: { name: 'find_user', requestContext: new RequestContext() },
    }).build() as { validateInput?: (input: unknown) => unknown };

    release = holdValidatorLoading();
    let settled = false;
    const validation = Promise.resolve(coreTool.validateInput!({ name: 42 })).then(result => {
      settled = true;
      return result as { error?: unknown };
    });
    await new Promise(resolve => setTimeout(resolve, 0));
    expect(settled).toBe(false);

    release();
    expect((await validation).error).toBeDefined();
  });
});
