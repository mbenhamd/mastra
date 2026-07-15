/**
 * DurableAgent tool hook tests (PF-2006).
 *
 * There was previously no hook coverage anywhere under `agent/durable/`. These
 * tests pin how `beforeToolCall`/`afterToolCall` behave on the durable path.
 *
 * The durable path applies the hook layer once, at preparation time
 * (`preparation.ts` → `getToolsForExecution({ hooks })`), and the durable
 * tool-call step dispatches from that same preparation-time surface
 * (`registryEntry.tools` / `replacementToolSurface`, see
 * `workflows/steps/tool-call.ts`) rather than from the surface an input
 * processor returns to the `llm-execution` step. That step's tool surface only
 * decides what the MODEL is shown.
 *
 * Consequence, pinned by the last two tests: a durable input processor's
 * tool-surface mutations never reach execution at all. This is a durable
 * limitation that is broader than tool hooks (it applies with or without
 * hooks), so it is documented here rather than worked around in the hook layer.
 */

import { MockLanguageModelV2, convertArrayToReadableStream } from '@internal/ai-sdk-v5/test';
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { z } from 'zod';
import { EventEmitterPubSub } from '../../../events/event-emitter';
import { createTool } from '../../../tools';
import type { CoreTool } from '../../../tools/types';
import { Agent } from '../../agent';
import { createDurableAgent } from '../create-durable-agent';

function createToolCallThenTextModel(toolName: string, args: Record<string, unknown>, finalText: string) {
  let callCount = 0;
  return new MockLanguageModelV2({
    doStream: async () => {
      callCount++;
      if (callCount === 1) {
        return {
          stream: convertArrayToReadableStream([
            { type: 'stream-start', warnings: [] },
            { type: 'response-metadata', id: 'id-0', modelId: 'mock-model-id', timestamp: new Date(0) },
            {
              type: 'tool-call',
              toolCallId: 'call-1',
              toolName,
              input: JSON.stringify(args),
              providerExecuted: false,
            },
            {
              type: 'finish',
              finishReason: 'tool-calls',
              usage: { inputTokens: 15, outputTokens: 10, totalTokens: 25 },
            },
          ]),
          rawCall: { rawPrompt: null, rawSettings: {} },
        };
      }
      return {
        stream: convertArrayToReadableStream([
          { type: 'stream-start', warnings: [] },
          { type: 'response-metadata', id: 'id-1', modelId: 'mock-model-id', timestamp: new Date(0) },
          { type: 'text-start', id: 'text-1' },
          { type: 'text-delta', id: 'text-1', delta: finalText },
          { type: 'text-end', id: 'text-1' },
          {
            type: 'finish',
            finishReason: 'stop',
            usage: { inputTokens: 20, outputTokens: 15, totalTokens: 35 },
          },
        ]),
        rawCall: { rawPrompt: null, rawSettings: {} },
      };
    },
  });
}

async function drain(stream: ReadableStream<any>) {
  const out: any[] = [];
  for await (const c of stream) out.push(c);
  return out;
}

describe('DurableAgent tool hooks', () => {
  let pubsub: EventEmitterPubSub;

  beforeEach(() => {
    pubsub = new EventEmitterPubSub();
  });

  afterEach(async () => {
    await pubsub.close();
  });

  it('runs configured hooks exactly once around a durable tool call', async () => {
    const execute = vi.fn(async () => ({ ok: true }));
    const beforeToolCall = vi.fn();
    const afterToolCall = vi.fn();

    const baseAgent = new Agent({
      id: 'durable-hook-agent',
      name: 'Durable Hook Agent',
      instructions: 'use the tool',
      model: createToolCallThenTextModel('plainTool', {}, 'done') as any,
      tools: {
        plainTool: createTool({ id: 'plainTool', description: 'plain', inputSchema: z.object({}), execute }),
      },
      hooks: { beforeToolCall, afterToolCall },
    });
    const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

    const { output, cleanup } = await durableAgent.stream('run it');
    await drain(output.fullStream as unknown as ReadableStream<any>);
    await cleanup();

    expect(execute).toHaveBeenCalledOnce();
    expect(beforeToolCall).toHaveBeenCalledOnce();
    expect(beforeToolCall).toHaveBeenCalledWith(expect.objectContaining({ toolName: 'plainTool' }));
    expect(afterToolCall).toHaveBeenCalledOnce();
    expect(afterToolCall).toHaveBeenCalledWith(
      expect.objectContaining({ toolName: 'plainTool', output: { ok: true } }),
    );
  });

  it('runs per-call hooks exactly once, overriding the configured hooks', async () => {
    const execute = vi.fn(async () => ({ ok: true }));
    const configuredBefore = vi.fn();
    const runBefore = vi.fn();
    const afterToolCall = vi.fn();

    const baseAgent = new Agent({
      id: 'durable-run-hook-agent',
      name: 'Durable Run Hook Agent',
      instructions: 'use the tool',
      model: createToolCallThenTextModel('plainTool', {}, 'done') as any,
      tools: {
        plainTool: createTool({ id: 'plainTool', description: 'plain', inputSchema: z.object({}), execute }),
      },
      hooks: { beforeToolCall: configuredBefore, afterToolCall },
    });
    const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

    const { output, cleanup } = await durableAgent.stream('run it', {
      hooks: { beforeToolCall: runBefore },
    });
    await drain(output.fullStream as unknown as ReadableStream<any>);
    await cleanup();

    expect(execute).toHaveBeenCalledOnce();
    expect(configuredBefore).not.toHaveBeenCalled();
    expect(runBefore).toHaveBeenCalledOnce();
    expect(afterToolCall).toHaveBeenCalledOnce();
  });

  it('lets beforeToolCall short-circuit a durable tool call', async () => {
    const execute = vi.fn(async () => ({ ok: true }));
    const beforeToolCall = vi.fn(() => ({ proceed: false as const, output: { message: 'blocked' } }));
    const afterToolCall = vi.fn();

    const baseAgent = new Agent({
      id: 'durable-shortcircuit-agent',
      name: 'Durable Short Circuit Agent',
      instructions: 'use the tool',
      model: createToolCallThenTextModel('guardedTool', {}, 'done') as any,
      tools: {
        guardedTool: createTool({ id: 'guardedTool', description: 'guarded', inputSchema: z.object({}), execute }),
      },
      hooks: { beforeToolCall, afterToolCall },
    });
    const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

    const { output, cleanup } = await durableAgent.stream('run it');
    await drain(output.fullStream as unknown as ReadableStream<any>);
    await cleanup();

    expect(beforeToolCall).toHaveBeenCalledOnce();
    expect(execute).not.toHaveBeenCalled();
    expect(afterToolCall).not.toHaveBeenCalled();
  });

  // ---------------------------------------------------------------------------
  // Known durable limitation (NOT a hook defect) — see file header.
  // These two tests pin current behavior so a change is caught deliberately.
  // ---------------------------------------------------------------------------

  it('KNOWN GAP: a processor-ADDED durable tool is shown to the model but is not executable', async () => {
    const execute = vi.fn(async () => ({ message: 'dynamic' }));
    const beforeToolCall = vi.fn();
    const afterToolCall = vi.fn();
    const dynamicTool = createTool({
      id: 'dynamicTool',
      description: 'processor-added durable tool',
      inputSchema: z.object({}),
      execute,
    });

    const baseAgent = new Agent({
      id: 'durable-added-tool-agent',
      name: 'Durable Added Tool Agent',
      instructions: 'use the tool',
      model: createToolCallThenTextModel('dynamicTool', {}, 'done') as any,
      inputProcessors: [
        {
          id: 'add-dynamic-tool',
          processInputStep: ({ tools }) => ({ tools: { ...tools, dynamicTool } }),
        },
      ],
      hooks: { beforeToolCall, afterToolCall },
    });
    const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

    const { output, cleanup } = await durableAgent.stream('run it');
    const chunks = await drain(output.fullStream as unknown as ReadableStream<any>);
    await cleanup();

    // The durable tool-call step resolves executables from the preparation-time
    // registry, which never saw the processor-added tool: the call fails with
    // ToolNotFoundError instead of running. Hooks are moot — there is nothing to
    // wrap. Wrapping the llm-execution step's surface would NOT fix this.
    const toolError = chunks.find((c: any) => c.type === 'tool-error');
    expect(toolError?.payload?.error?.name).toBe('ToolNotFoundError');
    expect(execute).not.toHaveBeenCalled();
    expect(beforeToolCall).not.toHaveBeenCalled();
    expect(afterToolCall).not.toHaveBeenCalled();
  });

  it('KNOWN GAP: a processor-DECORATED durable tool executes the undecorated original, hooked exactly once', async () => {
    const baseExecute = vi.fn(async () => ({ message: 'base' }));
    const decoratorCalls: string[] = [];
    const beforeToolCall = vi.fn();
    const afterToolCall = vi.fn();

    const baseAgent = new Agent({
      id: 'durable-decorated-tool-agent',
      name: 'Durable Decorated Tool Agent',
      instructions: 'use the tool',
      model: createToolCallThenTextModel('decoratedTool', {}, 'done') as any,
      tools: {
        decoratedTool: createTool({
          id: 'decoratedTool',
          description: 'decorated durable tool',
          inputSchema: z.object({}),
          execute: baseExecute,
        }),
      },
      inputProcessors: [
        {
          id: 'decorate-tool',
          processInputStep: ({ tools }) => {
            const target = (tools as Record<string, CoreTool>).decoratedTool!;
            const inner = target.execute!;
            return {
              tools: {
                ...tools,
                decoratedTool: {
                  ...target,
                  execute: async (input: any, ctx: any) => {
                    decoratorCalls.push('decorator');
                    return inner(input, ctx);
                  },
                } as CoreTool,
              },
            };
          },
        },
      ],
      hooks: { beforeToolCall, afterToolCall },
    });
    const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

    const { output, cleanup } = await durableAgent.stream('run it');
    await drain(output.fullStream as unknown as ReadableStream<any>);
    await cleanup();

    // The decorator never reaches execution (same registry-dispatch reason as
    // above), so the hook layer is NOT inverted on the durable path: it still
    // sits outside the executed tool and fires exactly once.
    expect(decoratorCalls).toEqual([]);
    expect(baseExecute).toHaveBeenCalledOnce();
    expect(beforeToolCall).toHaveBeenCalledOnce();
    expect(afterToolCall).toHaveBeenCalledOnce();
    expect(afterToolCall).toHaveBeenCalledWith(
      expect.objectContaining({ toolName: 'decoratedTool', output: { message: 'base' } }),
    );
  });
});
