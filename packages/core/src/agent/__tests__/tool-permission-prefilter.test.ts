import { MockLanguageModelV2 } from '@internal/ai-sdk-v5/test';
import { describe, expect, it } from 'vitest';
import { z } from 'zod/v4';

import { RequestContext } from '../../request-context';
import { createTool } from '../../tools';
import { Agent } from '../agent';
import { readPreconvertedDeniedToolNames, TOOL_PERMISSION_POLICY_KEY } from '../tool-permission-prefilter';

function createModel(): MockLanguageModelV2 {
  return new MockLanguageModelV2({
    doGenerate: async () => ({
      rawCall: { rawPrompt: null, rawSettings: {} },
      finishReason: 'stop',
      usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
      content: [{ type: 'text', text: 'ok' }],
      warnings: [],
    }),
  });
}

describe('tool permission conversion prefilter', () => {
  it('omits denied assigned tools before conversion while preserving allow and ask tools', async () => {
    const allowed = createTool({
      id: 'allowed-tool',
      inputSchema: z.object({ value: z.string() }),
      execute: async input => input,
    });
    const ask = createTool({
      id: 'ask-tool',
      inputSchema: z.object({ value: z.string() }),
      execute: async input => input,
    });
    const deniedTarget = createTool({
      id: 'denied-tool',
      inputSchema: z.object({ expensive: z.string() }),
      execute: async input => input,
    });

    let conversionAccessArmed = false;
    const denied = new Proxy(deniedTarget, {
      get(target, property, receiver) {
        if (conversionAccessArmed && (property === 'requireApproval' || property === 'background')) {
          throw new Error(`denied tool reached conversion property ${String(property)}`);
        }
        return Reflect.get(target, property, receiver);
      },
    });

    const agent = new Agent({
      id: 'permission-prefilter-agent',
      name: 'Permission Prefilter Agent',
      instructions: 'Test tool assembly.',
      model: createModel(),
      tools: { allowed, ask, denied },
    });
    conversionAccessArmed = true;

    const requestContext = new RequestContext();
    requestContext.set(TOOL_PERMISSION_POLICY_KEY, (toolName: string) => {
      if (toolName === 'denied') return 'deny';
      if (toolName === 'ask') return 'ask';
      return 'allow';
    });

    const tools = await agent.getToolsForExecution({ requestContext, runId: 'prefilter-run' });

    expect(Object.keys(tools).sort()).toEqual(['allowed', 'ask']);
    expect(readPreconvertedDeniedToolNames(requestContext, 'prefilter-run')).toEqual(['denied']);
  });

  it('deduplicates denied names per run without leaking them across run ids', async () => {
    const denied = createTool({
      id: 'denied-tool',
      inputSchema: z.object({}),
      execute: async () => ({ ok: true }),
    });
    const agent = new Agent({
      id: 'permission-prefilter-runs-agent',
      name: 'Permission Prefilter Runs Agent',
      instructions: 'Test tool assembly.',
      model: createModel(),
      tools: { denied },
    });
    const requestContext = new RequestContext();
    requestContext.set(TOOL_PERMISSION_POLICY_KEY, () => 'deny');

    await agent.getToolsForExecution({ requestContext, runId: 'run-a' });
    await agent.getToolsForExecution({ requestContext, runId: 'run-a' });
    await agent.getToolsForExecution({ requestContext, runId: 'run-b' });

    expect(readPreconvertedDeniedToolNames(requestContext, 'run-a')).toEqual(['denied']);
    expect(readPreconvertedDeniedToolNames(requestContext, 'run-b')).toEqual(['denied']);
    expect(readPreconvertedDeniedToolNames(requestContext, 'missing')).toEqual([]);
  });
});
