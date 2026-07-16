/**
 * DurableAgent tool hook tests (PF-2006).
 *
 * There was previously no hook coverage anywhere under `agent/durable/`. These
 * tests pin how `beforeToolCall`/`afterToolCall` behave on the durable path.
 *
 * The durable path applies the hook layer through
 * `getToolsForExecution({ hooks })`, and the durable tool-call step dispatches
 * from that registry-backed or worker-reconstructed executable surface
 * (`registryEntry.tools` / `replacementToolSurface`, see
 * `workflows/steps/tool-call.ts`) rather than from the surface an input
 * processor returns to the `llm-execution` step. To keep those surfaces
 * truthful across worker reconstruction, durable input processors may remove
 * tools but cannot add, replace, decorate, or mutate executable definitions.
 * Around-call behavior belongs in the reconstructible hook layer.
 */

import { MockLanguageModelV2, convertArrayToReadableStream } from '@internal/ai-sdk-v5/test';
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { z } from 'zod';
import { EventEmitterPubSub } from '../../../events/event-emitter';
import type { ProcessInputStepArgs } from '../../../processors';
import { createTool } from '../../../tools';
import type { CoreTool } from '../../../tools/types';
import { Agent } from '../../agent';
import { createToolSurfaceFence, enforceReconstructibleToolSurface } from '../../tool-surface-fence';
import { createDurableAgent } from '../create-durable-agent';

function createToolCallThenTextModel(
  toolName: string,
  args: Record<string, unknown>,
  finalText: string,
  onVisibleTools?: (toolNames: string[]) => void,
) {
  let callCount = 0;
  return new MockLanguageModelV2({
    doStream: async options => {
      onVisibleTools?.((options.tools ?? []).map(tool => tool.name));
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

  it('runs a configured short-circuit hook exactly once for a durable replacement toolset', async () => {
    const execute = vi.fn(async () => ({ ok: true }));
    const beforeToolCall = vi.fn(() => ({ proceed: false as const, output: { message: 'blocked' } }));
    const afterToolCall = vi.fn();
    const replacementTool = createTool({
      id: 'replacementTool',
      description: 'replacement',
      inputSchema: z.object({}),
      execute,
    });

    const baseAgent = new Agent({
      id: 'durable-replacement-configured-hook-agent',
      name: 'Durable Replacement Configured Hook Agent',
      instructions: 'use the tool',
      model: createToolCallThenTextModel('replacementTool', {}, 'done') as any,
      hooks: { beforeToolCall, afterToolCall },
    });
    const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

    const { output, cleanup } = await durableAgent.stream('run it', {
      toolsets: { mode: { replacementTool } },
      toolsetsMode: 'replace',
    });
    await drain(output.fullStream as unknown as ReadableStream<any>);
    await cleanup();

    expect(beforeToolCall).toHaveBeenCalledOnce();
    expect(beforeToolCall).toHaveBeenCalledWith(expect.objectContaining({ toolName: 'replacementTool' }));
    expect(execute).not.toHaveBeenCalled();
    expect(afterToolCall).not.toHaveBeenCalled();
  });

  it('runs before, execute, and after exactly once for a proceeding durable replacement toolset', async () => {
    const calls: string[] = [];
    const execute = vi.fn(async () => {
      calls.push('execute');
      return { ok: true };
    });
    const beforeToolCall = vi.fn(() => {
      calls.push('before');
    });
    const afterToolCall = vi.fn(() => {
      calls.push('after');
    });
    const replacementTool = createTool({
      id: 'replacementTool',
      description: 'replacement',
      inputSchema: z.object({}),
      execute,
    });

    const baseAgent = new Agent({
      id: 'durable-replacement-proceeding-hook-agent',
      name: 'Durable Replacement Proceeding Hook Agent',
      instructions: 'use the tool',
      model: createToolCallThenTextModel('replacementTool', {}, 'done') as any,
      hooks: { beforeToolCall, afterToolCall },
    });
    const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

    const { output, cleanup } = await durableAgent.stream('run it', {
      toolsets: { mode: { replacementTool } },
      toolsetsMode: 'replace',
    });
    await drain(output.fullStream as unknown as ReadableStream<any>);
    await cleanup();

    expect(beforeToolCall).toHaveBeenCalledOnce();
    expect(execute).toHaveBeenCalledOnce();
    expect(afterToolCall).toHaveBeenCalledOnce();
    expect(calls).toEqual(['before', 'execute', 'after']);
  });

  it('runs a per-call short-circuit hook exactly once for a durable replacement toolset', async () => {
    const execute = vi.fn(async () => ({ ok: true }));
    const configuredBefore = vi.fn();
    const runBefore = vi.fn(() => ({ proceed: false as const, output: { message: 'blocked' } }));
    const afterToolCall = vi.fn();
    const replacementTool = createTool({
      id: 'replacementTool',
      description: 'replacement',
      inputSchema: z.object({}),
      execute,
    });

    const baseAgent = new Agent({
      id: 'durable-replacement-run-hook-agent',
      name: 'Durable Replacement Run Hook Agent',
      instructions: 'use the tool',
      model: createToolCallThenTextModel('replacementTool', {}, 'done') as any,
      hooks: { beforeToolCall: configuredBefore, afterToolCall },
    });
    const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

    const { output, cleanup } = await durableAgent.stream('run it', {
      toolsets: { mode: { replacementTool } },
      toolsetsMode: 'replace',
      hooks: { beforeToolCall: runBefore },
    });
    await drain(output.fullStream as unknown as ReadableStream<any>);
    await cleanup();

    expect(configuredBefore).not.toHaveBeenCalled();
    expect(runBefore).toHaveBeenCalledOnce();
    expect(runBefore).toHaveBeenCalledWith(expect.objectContaining({ toolName: 'replacementTool' }));
    expect(execute).not.toHaveBeenCalled();
    expect(afterToolCall).not.toHaveBeenCalled();
  });

  it('fails before provider execution when a processor adds a durable tool', async () => {
    const execute = vi.fn(async () => ({ message: 'dynamic' }));
    const beforeToolCall = vi.fn();
    const afterToolCall = vi.fn();
    const visibleToolNames: string[][] = [];
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
      model: createToolCallThenTextModel('dynamicTool', {}, 'done', tools => visibleToolNames.push(tools)) as any,
      inputProcessors: [
        {
          id: 'add-dynamic-tool',
          processInputStep: ({ tools }) => {
            (tools as Record<string, CoreTool>).dynamicTool = dynamicTool;
            return { tools };
          },
        },
      ],
      hooks: { beforeToolCall, afterToolCall },
    });
    const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

    const { output, cleanup } = await durableAgent.stream('run it');
    const chunks = await drain(output.fullStream as unknown as ReadableStream<any>);
    await cleanup();

    const error = chunks.find((c: any) => c.type === 'error');
    expect(error?.payload?.error?.message).toMatch(/cannot add executable tool "dynamicTool"/);
    expect(visibleToolNames).toEqual([]);
    expect(execute).not.toHaveBeenCalled();
    expect(beforeToolCall).not.toHaveBeenCalled();
    expect(afterToolCall).not.toHaveBeenCalled();
  });

  it('fails before provider execution when a processor decorates a durable tool', async () => {
    const baseExecute = vi.fn(async () => ({ message: 'base' }));
    const decoratorCalls: string[] = [];
    const beforeToolCall = vi.fn();
    const afterToolCall = vi.fn();
    const processInputStep = vi.fn(({ tools }: ProcessInputStepArgs) => {
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
    });

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
          processInputStep,
        },
      ],
      hooks: { beforeToolCall, afterToolCall },
    });
    const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

    const { output, cleanup } = await durableAgent.stream('run it');
    const chunks = await drain(output.fullStream as unknown as ReadableStream<any>);
    await cleanup();

    const error = chunks.find((c: any) => c.type === 'error');
    expect(error?.payload?.error?.message).toMatch(/cannot replace executable tool "decoratedTool"/);
    expect(processInputStep).toHaveBeenCalled();
    expect(decoratorCalls).toEqual([]);
    expect(baseExecute).not.toHaveBeenCalled();
    expect(beforeToolCall).not.toHaveBeenCalled();
    expect(afterToolCall).not.toHaveBeenCalled();
  });

  it('fails before provider execution when a processor replaces a durable tool', async () => {
    const baseExecute = vi.fn(async () => ({ message: 'base' }));
    const replacementExecute = vi.fn(async () => ({ message: 'replacement' }));
    const visibleToolNames: string[][] = [];
    const replacementTool = createTool({
      id: 'stableTool',
      description: 'replacement implementation',
      inputSchema: z.object({}),
      execute: replacementExecute,
    });
    const baseAgent = new Agent({
      id: 'durable-replaced-tool-agent',
      name: 'Durable Replaced Tool Agent',
      instructions: 'use the tool',
      model: createToolCallThenTextModel('stableTool', {}, 'done', tools => visibleToolNames.push(tools)) as any,
      tools: {
        stableTool: createTool({
          id: 'stableTool',
          description: 'registered implementation',
          inputSchema: z.object({}),
          execute: baseExecute,
        }),
      },
      inputProcessors: [
        {
          id: 'replace-tool',
          processInputStep: ({ tools }) => ({ tools: { ...tools, stableTool: replacementTool } }),
        },
      ],
    });
    const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

    const { output, cleanup } = await durableAgent.stream('run it');
    const chunks = await drain(output.fullStream as unknown as ReadableStream<any>);
    await cleanup();

    const error = chunks.find((c: any) => c.type === 'error');
    expect(error?.payload?.error?.message).toMatch(/cannot replace executable tool "stableTool"/);
    expect(visibleToolNames).toEqual([]);
    expect(baseExecute).not.toHaveBeenCalled();
    expect(replacementExecute).not.toHaveBeenCalled();
  });

  it('restores an in-place executable mutation before failing the durable run', async () => {
    const baseExecute = vi.fn(async () => ({ message: 'base' }));
    const mutatedExecute = vi.fn(async () => ({ message: 'mutated' }));
    const registeredTool = createTool({
      id: 'mutatedTool',
      description: 'registered implementation',
      inputSchema: z.object({}),
      execute: baseExecute,
    });
    const registeredExecute = registeredTool.execute;
    const baseAgent = new Agent({
      id: 'durable-mutated-tool-agent',
      name: 'Durable Mutated Tool Agent',
      instructions: 'use the tool',
      model: createToolCallThenTextModel('mutatedTool', {}, 'done') as any,
      tools: { mutatedTool: registeredTool },
      inputProcessors: [
        {
          id: 'mutate-tool-in-place',
          processInputStep: ({ tools }) => {
            (tools as Record<string, CoreTool>).mutatedTool!.execute = mutatedExecute;
            return { tools };
          },
        },
      ],
    });
    const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

    const { output, cleanup } = await durableAgent.stream('run it');
    const chunks = await drain(output.fullStream as unknown as ReadableStream<any>);
    await cleanup();

    const error = chunks.find((c: any) => c.type === 'error');
    expect(error?.payload?.error?.message).toMatch(/cannot mutate executable tool "mutatedTool"/);
    expect(registeredTool.execute).toBe(registeredExecute);
    expect(baseExecute).not.toHaveBeenCalled();
    expect(mutatedExecute).not.toHaveBeenCalled();
  });

  it('keeps an irreversible processor mutation from poisoning the next durable run', async () => {
    const execute = vi.fn(async () => ({ message: 'trusted' }));
    const registeredTool = createTool({
      id: 'retrySafeTool',
      description: 'registered implementation',
      inputSchema: z.object({}),
      execute,
    });
    let processorCalls = 0;
    const baseAgent = new Agent({
      id: 'durable-irreversible-mutation-agent',
      name: 'Durable Irreversible Mutation Agent',
      instructions: 'use the tool',
      model: createToolCallThenTextModel('retrySafeTool', {}, 'done') as any,
      tools: { retrySafeTool: registeredTool },
      inputProcessors: [
        {
          id: 'freeze-first-tool-view',
          processInputStep: ({ tools }) => {
            processorCalls++;
            if (processorCalls === 1) Object.freeze((tools as Record<string, CoreTool>).retrySafeTool);
            return { tools };
          },
        },
      ],
    });
    const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

    const first = await durableAgent.stream('run it');
    const firstChunks = await drain(first.output.fullStream as unknown as ReadableStream<any>);
    await first.cleanup();

    expect(firstChunks.find((chunk: any) => chunk.type === 'error')?.payload?.error?.message).toMatch(
      /cannot mutate executable tool "retrySafeTool"/,
    );
    expect(Object.isExtensible(registeredTool)).toBe(true);
    expect(execute).not.toHaveBeenCalled();

    const retry = await durableAgent.stream('run it again');
    await drain(retry.output.fullStream as unknown as ReadableStream<any>);
    await retry.cleanup();

    expect(execute).toHaveBeenCalledOnce();
  });

  it('rejects a late mutation from a processor-returned activeTools Proxy', async () => {
    const baseExecute = vi.fn(async () => ({ message: 'base' }));
    const injectedExecute = vi.fn(async () => ({ message: 'injected' }));
    const visibleToolNames: string[][] = [];
    const registeredTool = createTool({
      id: 'stableTool',
      description: 'trusted description',
      inputSchema: z.object({}),
      execute: baseExecute,
    });
    let processorTool: CoreTool | undefined;
    let processorExecute: CoreTool['execute'];
    let processorDescription: string | undefined;
    const baseAgent = new Agent({
      id: 'durable-active-tools-proxy-agent',
      name: 'Durable Active Tools Proxy Agent',
      instructions: 'use the stable tool',
      model: createToolCallThenTextModel('stableTool', {}, 'done', tools => visibleToolNames.push(tools)) as any,
      tools: { stableTool: registeredTool },
      inputProcessors: [
        {
          id: 'late-active-tools-mutation',
          processInputStep: ({ tools }) => {
            processorTool = (tools as Record<string, CoreTool>).stableTool;
            processorExecute = processorTool?.execute;
            processorDescription = processorTool?.description;
            return {
              tools,
              activeTools: new Proxy(['stableTool'], {
                get(target, key, receiver) {
                  processorTool!.execute = injectedExecute;
                  processorTool!.description = 'injected description';
                  return Reflect.get(target, key, receiver);
                },
              }),
            };
          },
        },
      ],
    });
    const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

    const { output, cleanup } = await durableAgent.stream('run it');
    const chunks = await drain(output.fullStream as unknown as ReadableStream<any>);
    await cleanup();

    const error = chunks.find((c: any) => c.type === 'error');
    expect(error?.payload?.error?.message).toMatch(/cannot mutate executable tool "stableTool"/);
    expect(processorTool?.execute).toBe(processorExecute);
    expect(processorTool?.description).toBe(processorDescription);
    expect(visibleToolNames).toEqual([]);
    expect(baseExecute).not.toHaveBeenCalled();
    expect(injectedExecute).not.toHaveBeenCalled();
  });

  it('rejects a late mutation from a processor-returned toolChoice Proxy', async () => {
    const baseExecute = vi.fn(async () => ({ message: 'base' }));
    const injectedExecute = vi.fn(async () => ({ message: 'injected' }));
    const visibleToolNames: string[][] = [];
    let processorTool: CoreTool | undefined;
    let processorExecute: CoreTool['execute'];
    const baseAgent = new Agent({
      id: 'durable-tool-choice-proxy-agent',
      name: 'Durable Tool Choice Proxy Agent',
      instructions: 'use the stable tool',
      model: createToolCallThenTextModel('stableTool', {}, 'done', tools => visibleToolNames.push(tools)) as any,
      tools: {
        stableTool: createTool({
          id: 'stableTool',
          description: 'trusted description',
          inputSchema: z.object({}),
          execute: baseExecute,
        }),
      },
      inputProcessors: [
        {
          id: 'late-tool-choice-mutation',
          processInputStep: ({ tools }) => {
            processorTool = (tools as Record<string, CoreTool>).stableTool;
            processorExecute = processorTool?.execute;
            return {
              tools,
              toolChoice: new Proxy(
                { type: 'tool' as const, toolName: 'stableTool' },
                {
                  get(target, key, receiver) {
                    processorTool!.execute = injectedExecute;
                    return Reflect.get(target, key, receiver);
                  },
                },
              ),
            };
          },
        },
      ],
    });
    const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

    const { output, cleanup } = await durableAgent.stream('run it');
    const chunks = await drain(output.fullStream as unknown as ReadableStream<any>);
    await cleanup();

    const error = chunks.find((c: any) => c.type === 'error');
    expect(error?.payload?.error?.message).toMatch(/cannot mutate executable tool "stableTool"/);
    expect(processorTool?.execute).toBe(processorExecute);
    expect(visibleToolNames).toEqual([]);
    expect(baseExecute).not.toHaveBeenCalled();
    expect(injectedExecute).not.toHaveBeenCalled();
  });

  it('allows a processor to narrow the durable tool surface', async () => {
    const keptExecute = vi.fn(async () => ({ message: 'kept' }));
    const removedExecute = vi.fn(async () => ({ message: 'removed' }));
    const visibleToolNames: string[][] = [];
    const baseAgent = new Agent({
      id: 'durable-narrowed-tool-agent',
      name: 'Durable Narrowed Tool Agent',
      instructions: 'use the kept tool',
      model: createToolCallThenTextModel('keptTool', {}, 'done', tools => visibleToolNames.push(tools)) as any,
      tools: {
        keptTool: createTool({
          id: 'keptTool',
          description: 'kept',
          inputSchema: z.object({}),
          execute: keptExecute,
        }),
        removedTool: createTool({
          id: 'removedTool',
          description: 'removed',
          inputSchema: z.object({}),
          execute: removedExecute,
        }),
      },
      inputProcessors: [
        {
          id: 'narrow-tools',
          processInputStep: ({ tools }) => {
            const { removedTool: _removedTool, ...keptTools } = tools as Record<string, CoreTool>;
            return { tools: keptTools };
          },
        },
      ],
    });
    const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

    const { output, cleanup } = await durableAgent.stream('run it', {
      activeTools: ['keptTool', 'removedTool'],
    });
    const chunks = await drain(output.fullStream as unknown as ReadableStream<any>);
    await cleanup();

    expect(chunks.find((chunk: any) => chunk.type === 'error')).toBeUndefined();
    expect(visibleToolNames).not.toHaveLength(0);
    expect(visibleToolNames.every(names => names.includes('keptTool') && !names.includes('removedTool'))).toBe(true);
    expect(keptExecute).toHaveBeenCalledOnce();
    expect(removedExecute).not.toHaveBeenCalled();
  });

  it('allows a processor to return the unchanged durable tool surface', async () => {
    const execute = vi.fn(async () => ({ message: 'same' }));
    const processInputStep = vi.fn(({ tools }: ProcessInputStepArgs) => ({ tools }));
    const baseAgent = new Agent({
      id: 'durable-same-tool-agent',
      name: 'Durable Same Tool Agent',
      instructions: 'use the tool',
      model: createToolCallThenTextModel('sameTool', {}, 'done') as any,
      tools: {
        sameTool: createTool({
          id: 'sameTool',
          description: 'same',
          inputSchema: z.object({}),
          execute,
        }),
      },
      inputProcessors: [{ id: 'same-tools', processInputStep }],
    });
    const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

    const { output, cleanup } = await durableAgent.stream('run it');
    const chunks = await drain(output.fullStream as unknown as ReadableStream<any>);
    await cleanup();

    expect(chunks.find((chunk: any) => chunk.type === 'error')).toBeUndefined();
    expect(processInputStep).toHaveBeenCalled();
    expect(execute).toHaveBeenCalledOnce();
  });

  it('does not treat a function-owned last invocation context as executable definition state', async () => {
    type ContextRecordingExecute = CoreTool['execute'] & { lastContext?: unknown };
    let execute: ContextRecordingExecute;
    execute = Object.assign(async (_input: unknown, context: unknown) => {
      execute.lastContext = context;
      return { message: 'same' };
    }, {});
    const baseAgent = new Agent({
      id: 'durable-function-runtime-context-agent',
      name: 'Durable Function Runtime Context Agent',
      instructions: 'use the tool',
      model: createToolCallThenTextModel('contextTool', {}, 'done') as any,
      tools: {
        contextTool: createTool({
          id: 'contextTool',
          description: 'records its last invocation context',
          inputSchema: z.object({}),
          execute,
        }),
      },
      inputProcessors: [{ id: 'same-tools', processInputStep: ({ tools }) => ({ tools }) }],
    });
    const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

    const first = await durableAgent.stream('run it');
    const firstChunks = await drain(first.output.fullStream as unknown as ReadableStream<any>);
    await first.cleanup();

    expect(firstChunks.find((chunk: any) => chunk.type === 'error')).toBeUndefined();
    expect(execute.lastContext).toBeDefined();

    const second = await durableAgent.stream('run it again');
    const secondChunks = await drain(second.output.fullStream as unknown as ReadableStream<any>);
    await second.cleanup();

    expect(secondChunks.find((chunk: any) => chunk.type === 'error')).toBeUndefined();
  });

  it('accepts a registered tool surface reconstructed on the current worker', async () => {
    const execute = vi.fn(async () => ({ ok: true }));
    const baseAgent = new Agent({
      id: 'durable-reconstructed-tool-agent',
      name: 'Durable Reconstructed Tool Agent',
      instructions: 'use the tool',
      model: createToolCallThenTextModel('reconstructedTool', {}, 'done') as any,
      tools: {
        reconstructedTool: createTool({
          id: 'reconstructedTool',
          description: 'registered tool',
          inputSchema: z.object({}),
          execute,
        }),
      },
      hooks: { beforeToolCall: vi.fn() },
    });
    const firstWorkerSurface = await baseAgent.getToolsForExecution({});
    const reconstructedSurface = await baseAgent.getToolsForExecution({});
    const fence = createToolSurfaceFence(reconstructedSurface);

    expect(reconstructedSurface.reconstructedTool).not.toBe(firstWorkerSurface.reconstructedTool);
    expect(enforceReconstructibleToolSurface({ ...reconstructedSurface }, fence)).toEqual(reconstructedSurface);
  });
});
