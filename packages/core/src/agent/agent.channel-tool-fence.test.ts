/**
 * Harness v1 §14.7 admission-time model-visible channel tool strip.
 *
 * On a turn admitted under an active harness ChannelBinding, the legacy
 * AgentChannels direct-provider tools (add_reaction/remove_reaction) post
 * straight to the platform, bypassing the harness durable outbox / permission /
 * event-ordering guarantees. They MUST be fenced out of the model-visible tool
 * surface, and their names reserved so no OTHER tool source can re-expose them
 * on the same turn. These tests drive Agent.convertTools() directly with a
 * crafted channel-bound RequestContext.
 */
import { MockLanguageModelV2 } from '@internal/ai-sdk-v5/test';
import { describe, expect, it } from 'vitest';
import { z } from 'zod';


import type { AgentChannels } from '../channels/agent-channels';
import { RequestContext } from '../request-context';
import { createTool } from '../tools';

import { Agent } from './agent';
import {
  enforceChannelToolFence,
  isHarnessChannelBoundTurn,
  readChannelToolFence,
  stampChannelToolFence,
} from './channel-tool-fence';

function makeModel() {
  return new MockLanguageModelV2({
    doStream: async () => ({
      stream: new ReadableStream({
        start(controller) {
          controller.close();
        },
      }),
      rawCall: { rawPrompt: null, rawSettings: {} },
    }),
  });
}

function makeAgent() {
  return new Agent({
    id: 'fence-agent',
    name: 'Fence Agent',
    instructions: 'test agent',
    model: makeModel(),
  });
}

/** Inject a fake AgentChannels that exposes the named channel tools. */
function injectChannels(agent: Agent, toolNames: string[]) {
  const tools: Record<string, unknown> = {};
  for (const name of toolNames) {
    tools[name] = createTool({
      id: name,
      description: `legacy channel tool ${name}`,
      inputSchema: z.object({ messageId: z.string() }),
      execute: async () => ({ ok: true }),
    });
  }
  const fake = {
    getTools: () => tools,
    getInputProcessors: () => [],
    __setAgent: () => {},
  };
  agent.setChannels(fake as unknown as AgentChannels);
}

function channelBoundContext(): RequestContext {
  return new RequestContext([
    ['harness', { sessionId: 's-1' }],
    ['channel', { origin: 'inbound', channelId: 'support', providerId: 'slack', platform: 'slack' }],
  ]);
}

function plainContext(): RequestContext {
  return new RequestContext([['harness', { sessionId: 's-1' }]]);
}

async function convert(agent: Agent, requestContext: RequestContext, extra: Record<string, unknown> = {}) {
  return (
    agent as unknown as {
      convertTools(args: Record<string, unknown>): Promise<Record<string, unknown>>;
    }
  ).convertTools({
    requestContext,
    methodType: 'generate',
    toolsets: {},
    clientTools: {},
    inputProcessors: [],
    ...extra,
  });
}

describe('Agent §14.7 channel tool strip', () => {
  it('omits legacy channel tools from the model-visible surface on a channel-bound turn', async () => {
    const agent = makeAgent();
    injectChannels(agent, ['add_reaction', 'remove_reaction']);
    const tools = await convert(agent, channelBoundContext());
    expect(tools.add_reaction).toBeUndefined();
    expect(tools.remove_reaction).toBeUndefined();
  });

  it('leaves channel tools intact on a non-channel-bound harness turn', async () => {
    const agent = makeAgent();
    injectChannels(agent, ['add_reaction', 'remove_reaction']);
    const tools = await convert(agent, plainContext());
    expect(tools.add_reaction).toBeDefined();
    expect(tools.remove_reaction).toBeDefined();
  });

  it('strips a per-turn toolset tool that collides with a reserved channel tool name on a channel-bound turn', async () => {
    const agent = makeAgent();
    injectChannels(agent, ['add_reaction', 'remove_reaction']);
    const tools = await convert(agent, channelBoundContext(), {
      toolsets: {
        userSet: {
          add_reaction: createTool({
            id: 'add_reaction',
            description: 'a user-supplied tool spoofing the channel reaction name',
            inputSchema: z.object({ x: z.string() }),
            execute: async () => ({ ok: true }),
          }),
          safe_tool: createTool({
            id: 'safe_tool',
            description: 'an unrelated user tool',
            inputSchema: z.object({ x: z.string() }),
            execute: async () => ({ ok: true }),
          }),
        },
      },
    });
    // The reserved name is fenced regardless of source/permission policy...
    expect(tools.add_reaction).toBeUndefined();
    // ...but unrelated user tools are untouched.
    expect(tools.safe_tool).toBeDefined();
  });

  it('does not strip the reserved names on a non-channel turn (toolset tool survives)', async () => {
    const agent = makeAgent();
    injectChannels(agent, ['add_reaction', 'remove_reaction']);
    const tools = await convert(agent, plainContext(), {
      toolsets: {
        userSet: {
          add_reaction: createTool({
            id: 'add_reaction',
            description: 'user tool, allowed off-channel',
            inputSchema: z.object({ x: z.string() }),
            execute: async () => ({ ok: true }),
          }),
        },
      },
    });
    // Off-channel, the legacy channel tool and the user toolset entry collide on the
    // same key; the toolset spread wins (later in the merge) — the point of THIS test
    // is only that the §14.7 fence does NOT engage off-channel.
    expect(tools.add_reaction).toBeDefined();
  });

  it('stamps the reserved channel tool names onto the channel-bound requestContext (INC-1b)', async () => {
    const agent = makeAgent();
    injectChannels(agent, ['add_reaction', 'remove_reaction']);
    const ctx = channelBoundContext();
    await convert(agent, ctx);
    // The reserved set is stamped so the downstream processor-tool sites can re-fence.
    const stamped = readChannelToolFence(ctx);
    expect(stamped).toBeInstanceOf(Set);
    expect([...(stamped ?? [])].sort()).toEqual(['add_reaction', 'remove_reaction']);
  });

  it('does NOT stamp a fence on a non-channel-bound turn', async () => {
    const agent = makeAgent();
    injectChannels(agent, ['add_reaction', 'remove_reaction']);
    const ctx = plainContext();
    await convert(agent, ctx);
    expect(readChannelToolFence(ctx)).toBeUndefined();
  });
});

describe('§14.7 channel-tool-fence helpers (INC-1b)', () => {
  it('isHarnessChannelBoundTurn requires BOTH harness and channel slots', () => {
    expect(isHarnessChannelBoundTurn(new RequestContext([['harness', {}], ['channel', { origin: 'inbound' }]]))).toBe(
      true,
    );
    expect(isHarnessChannelBoundTurn(new RequestContext([['harness', {}]]))).toBe(false);
    expect(isHarnessChannelBoundTurn(new RequestContext([['channel', { origin: 'inbound' }]]))).toBe(false);
    expect(isHarnessChannelBoundTurn(new RequestContext([]))).toBe(false);
  });

  it('stamp/read round-trips the reserved set; read is undefined when absent or wrong type', () => {
    const ctx = new RequestContext([]);
    expect(readChannelToolFence(ctx)).toBeUndefined();
    expect(readChannelToolFence(undefined)).toBeUndefined();
    const names = new Set(['add_reaction']);
    stampChannelToolFence(ctx, names);
    expect(readChannelToolFence(ctx)).toBe(names);
    // A non-Set value under the key is ignored.
    ctx.set('__harnessChannelReservedTools' as never, 'not-a-set' as never);
    expect(readChannelToolFence(ctx)).toBeUndefined();
  });

  it('enforceChannelToolFence drops reserved names, leaves others, and is a no-op when nothing collides', () => {
    const warnings: string[] = [];
    const logger = { warn: (m: string) => warnings.push(m) };

    const tools: Record<string, unknown> = { add_reaction: {}, safe_tool: {}, remove_reaction: {} };
    enforceChannelToolFence(tools, new Set(['add_reaction', 'remove_reaction']), logger);
    expect(Object.keys(tools)).toEqual(['safe_tool']);
    expect(warnings).toHaveLength(2);

    const clean: Record<string, unknown> = { only_safe: {} };
    enforceChannelToolFence(clean, new Set(['add_reaction']));
    expect(Object.keys(clean)).toEqual(['only_safe']);
  });
});
