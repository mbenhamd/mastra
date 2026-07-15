import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { unwrapToolsFromHooks, wrapToolsWithHooks } from './tool-hooks';
import type { CoreTool, ToolHooks } from './types';

const metadata = { agentId: 'agent-id', agentName: 'Agent name' };

function makeTool(execute: CoreTool['execute']): CoreTool {
  return {
    parameters: z.object({}),
    execute,
  };
}

describe('wrapToolsWithHooks', () => {
  it('is idempotent when the final surface is wrapped repeatedly', async () => {
    const execute = vi.fn(async () => 'result');
    const beforeToolCall = vi.fn();
    const afterToolCall = vi.fn();
    const hooks: ToolHooks = { beforeToolCall, afterToolCall };

    const once = wrapToolsWithHooks({ example: makeTool(execute) }, hooks, metadata);
    const twice = wrapToolsWithHooks(once, hooks, metadata);
    const output = await twice.example!.execute?.({}, {} as any);

    expect(output).toBe('result');
    expect(execute).toHaveBeenCalledOnce();
    expect(beforeToolCall).toHaveBeenCalledOnce();
    expect(afterToolCall).toHaveBeenCalledOnce();
  });

  it('retains exact-once hooks when a processor shallow-clones a wrapped tool', async () => {
    const execute = vi.fn(async () => 'result');
    const beforeToolCall = vi.fn();
    const afterToolCall = vi.fn();
    const hooks: ToolHooks = { beforeToolCall, afterToolCall };

    const initial = wrapToolsWithHooks({ example: makeTool(execute) }, hooks, metadata);
    const processorResult = { ...initial.example! };
    const final = wrapToolsWithHooks({ example: processorResult }, hooks, metadata);
    await final.example!.execute?.({}, {} as any);

    expect(execute).toHaveBeenCalledOnce();
    expect(beforeToolCall).toHaveBeenCalledOnce();
    expect(afterToolCall).toHaveBeenCalledOnce();
  });

  it('lets a processor decorate an existing executor without nesting hooks', async () => {
    const execute = vi.fn(async () => 'result');
    const decorate = vi.fn();
    const beforeToolCall = vi.fn();
    const afterToolCall = vi.fn();
    const hooks: ToolHooks = { beforeToolCall, afterToolCall };

    const initial = wrapToolsWithHooks({ example: makeTool(execute) }, hooks, metadata);
    const processorTools = unwrapToolsFromHooks(initial);
    const processorResult = {
      ...processorTools.example!,
      execute: async (...args: Parameters<NonNullable<CoreTool['execute']>>) => {
        decorate();
        return processorTools.example!.execute!(...args);
      },
    };
    const final = wrapToolsWithHooks({ example: processorResult }, hooks, metadata);
    await final.example!.execute?.({}, {} as any);

    expect(execute).toHaveBeenCalledOnce();
    expect(decorate).toHaveBeenCalledOnce();
    expect(beforeToolCall).toHaveBeenCalledOnce();
    expect(afterToolCall).toHaveBeenCalledOnce();
  });

  it('replaces an earlier hook binding without nesting it', async () => {
    const execute = vi.fn(async () => 'result');
    const configuredBefore = vi.fn();
    const runBefore = vi.fn();

    const configured = wrapToolsWithHooks(
      { example: makeTool(execute) },
      { beforeToolCall: configuredBefore },
      metadata,
    );
    const final = wrapToolsWithHooks(configured, { beforeToolCall: runBefore }, metadata);
    await final.example!.execute?.({}, {} as any);

    expect(execute).toHaveBeenCalledOnce();
    expect(configuredBefore).not.toHaveBeenCalled();
    expect(runBefore).toHaveBeenCalledOnce();
  });

  it('wraps a processor replacement with a new executor', async () => {
    const initialExecute = vi.fn(async () => 'initial');
    const replacementExecute = vi.fn(async () => 'replacement');
    const beforeToolCall = vi.fn();
    const hooks: ToolHooks = { beforeToolCall };

    wrapToolsWithHooks({ example: makeTool(initialExecute) }, hooks, metadata);
    const final = wrapToolsWithHooks({ example: makeTool(replacementExecute) }, hooks, metadata);
    const output = await final.example!.execute?.({}, {} as any);

    expect(output).toBe('replacement');
    expect(initialExecute).not.toHaveBeenCalled();
    expect(replacementExecute).toHaveBeenCalledOnce();
    expect(beforeToolCall).toHaveBeenCalledOnce();
  });

  it('preserves the original receiver through processor exposure and hook rebinding', async () => {
    class ReceiverSensitiveTool {
      readonly parameters = z.object({});
      readonly #value = 'bound';

      async execute() {
        return this.#value;
      }
    }

    const configuredBefore = vi.fn();
    const runBefore = vi.fn();
    const tool = new ReceiverSensitiveTool() as unknown as CoreTool;
    const initial = wrapToolsWithHooks({ example: tool }, { beforeToolCall: configuredBefore }, metadata);
    const processorTools = unwrapToolsFromHooks(initial);
    expect(await processorTools.example!.execute?.({}, {} as any)).toBe('bound');

    const final = wrapToolsWithHooks(processorTools, { beforeToolCall: runBefore }, metadata);
    const output = await final.example!.execute?.({}, {} as any);

    expect(output).toBe('bound');
    expect(configuredBefore).not.toHaveBeenCalled();
    expect(runBefore).toHaveBeenCalledOnce();
  });
});
