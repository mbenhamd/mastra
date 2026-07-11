import { MockLanguageModelV2 } from '@internal/ai-sdk-v5/test';
import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';

import { RequestContext } from '../request-context';
import { createTool } from '../tools';
import { convertArrayToReadableStream } from './__tests__/mock-model';
import { Agent } from './agent';
import { readToolSurfaceFence, stageToolSurfaceFenceRestore } from './tool-surface-fence';

const usage = { inputTokens: 1, outputTokens: 1, totalTokens: 2 };

function testTool(id: string, execute = vi.fn().mockResolvedValue(id)) {
  return createTool({
    id,
    description: id,
    inputSchema: z.object({}),
    execute,
  });
}

describe('Agent replacement toolsets', () => {
  it('exposes only replacement toolsets and re-fences processor mutations and forced hidden calls', async () => {
    const assignedExecute = vi.fn().mockResolvedValue('assigned');
    const injectedExecute = vi.fn().mockResolvedValue('injected');
    const visibleToolNames: string[][] = [];
    let callCount = 0;
    const model = new MockLanguageModelV2({
      doGenerate: async options => {
        visibleToolNames.push((options.tools ?? []).map(tool => tool.name));
        callCount++;
        if (callCount === 1) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            finishReason: 'tool-calls' as const,
            usage,
            content: [
              {
                type: 'tool-call' as const,
                toolCallId: 'hidden-call',
                toolName: 'assignedTool',
                input: '{}',
              },
            ],
            warnings: [],
          };
        }
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          finishReason: 'stop' as const,
          usage,
          content: [{ type: 'text' as const, text: 'done' }],
          warnings: [],
        };
      },
    });
    const agent = new Agent({
      id: 'replacement-agent',
      name: 'replacement-agent',
      instructions: 'test',
      model,
      tools: { assignedTool: testTool('assignedTool', assignedExecute) },
      inputProcessors: [
        {
          id: 'attempt-tool-expansion',
          processInputStep: ({ tools }) => ({
            tools: { ...tools, injectedTool: testTool('injectedTool', injectedExecute) },
          }),
        },
      ],
    });

    const result = await agent.generate('test', {
      maxSteps: 3,
      toolsets: { mode: { modeTool: testTool('modeTool') } },
      toolsetsMode: 'replace',
    });

    expect(visibleToolNames).toEqual([['modeTool'], ['modeTool']]);
    expect(assignedExecute).not.toHaveBeenCalled();
    expect(injectedExecute).not.toHaveBeenCalled();
    expect(result.text).toBe('done');
  });

  it('restores an allowed tool after a processor mutates its execute function in place', async () => {
    const approvedExecute = vi.fn().mockResolvedValue('approved');
    const injectedExecute = vi.fn().mockResolvedValue('injected');
    const modeTool = testTool('modeTool', approvedExecute);
    let callCount = 0;
    const model = new MockLanguageModelV2({
      doGenerate: async () => {
        callCount++;
        return callCount === 1
          ? {
              rawCall: { rawPrompt: null, rawSettings: {} },
              finishReason: 'tool-calls' as const,
              usage,
              content: [
                { type: 'tool-call' as const, toolCallId: 'mode-call', toolName: 'modeTool', input: '{}' },
              ],
              warnings: [],
            }
          : {
              rawCall: { rawPrompt: null, rawSettings: {} },
              finishReason: 'stop' as const,
              usage,
              content: [{ type: 'text' as const, text: 'done' }],
              warnings: [],
            };
      },
    });
    const agent = new Agent({
      id: 'in-place-mutation-agent',
      name: 'in-place-mutation-agent',
      instructions: 'test',
      model,
      inputProcessors: [
        {
          id: 'mutate-tool-in-place',
          processInputStep: ({ tools }) => {
            tools.modeTool!.execute = injectedExecute;
            return { tools };
          },
        },
      ],
    });

    await agent.generate('test', {
      maxSteps: 2,
      toolsetsMode: 'replace',
      toolsets: { mode: { modeTool } },
    });

    expect(approvedExecute).toHaveBeenCalledOnce();
    expect(injectedExecute).not.toHaveBeenCalled();
  });

  it('preserves merge as the default and supports an explicitly empty replacement', async () => {
    const model = new MockLanguageModelV2({
      doGenerate: async options => ({
        rawCall: { rawPrompt: null, rawSettings: {} },
        finishReason: 'stop' as const,
        usage,
        content: [
          {
            type: 'text' as const,
            text: (options.tools ?? [])
              .map(tool => tool.name)
              .sort()
              .join(','),
          },
        ],
        warnings: [],
      }),
    });
    const agent = new Agent({
      id: 'merge-agent',
      name: 'merge-agent',
      instructions: 'test',
      model,
      tools: { assignedTool: testTool('assignedTool') },
      defaultOptions: {
        toolsets: { defaults: { defaultTool: testTool('defaultTool') } },
      },
    });

    const merged = await agent.generate('merge', {
      toolsets: { call: { callTool: testTool('callTool') } },
    });
    const empty = await agent.generate('empty', { toolsets: {}, toolsetsMode: 'replace' });

    expect(merged.text).toBe('assignedTool,callTool,defaultTool');
    expect(empty.text).toBe('');
  });

  it('replaces default toolsets when replacement mode is inherited from agent defaults', async () => {
    const model = new MockLanguageModelV2({
      doGenerate: async options => ({
        rawCall: { rawPrompt: null, rawSettings: {} },
        finishReason: 'stop' as const,
        usage,
        content: [{ type: 'text' as const, text: (options.tools ?? []).map(tool => tool.name).join(',') }],
        warnings: [],
      }),
    });
    const agent = new Agent({
      id: 'default-replacement-agent',
      name: 'default-replacement-agent',
      instructions: 'test',
      model,
      tools: { assignedTool: testTool('assignedTool') },
      defaultOptions: {
        toolsetsMode: 'replace',
        toolsets: { defaults: { defaultTool: testTool('defaultTool') } },
      },
    });
    const callTool = testTool('callTool');

    const generated = await agent.generate('test', { toolsets: { call: { callTool } } });
    const reconstructed = await agent.getToolsForExecution({ toolsets: { call: { callTool } } });

    expect(generated.text).toBe('callTool');
    expect(Object.keys(reconstructed)).toEqual(['callTool']);
  });

  it('caps a rebuilt replacement surface with a persisted resume fence', async () => {
    const visibleToolNames: string[][] = [];
    const model = new MockLanguageModelV2({
      doGenerate: async options => {
        visibleToolNames.push((options.tools ?? []).map(tool => tool.name));
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          finishReason: 'stop' as const,
          usage,
          content: [{ type: 'text' as const, text: 'done' }],
          warnings: [],
        };
      },
    });
    const agent = new Agent({ id: 'resume-agent', name: 'resume-agent', instructions: 'test', model });
    const requestContext = new RequestContext();
    stageToolSurfaceFenceRestore(requestContext, 'resume-run', ['originalTool']);

    await agent.generate('resume', {
      runId: 'resume-run',
      requestContext,
      toolsetsMode: 'replace',
      toolsets: {
        mode: {
          originalTool: testTool('originalTool'),
          addedAfterSuspend: testTool('addedAfterSuspend'),
        },
      },
    });

    expect(visibleToolNames).toEqual([['originalTool']]);
  });

  it('fails closed when a persisted resume tool implementation cannot be reconstructed', async () => {
    const doGenerate = vi.fn();
    const agent = new Agent({
      id: 'missing-resume-tool-agent',
      name: 'missing-resume-tool-agent',
      instructions: 'test',
      model: new MockLanguageModelV2({ doGenerate }),
    });
    const requestContext = new RequestContext();
    stageToolSurfaceFenceRestore(requestContext, 'missing-tool-run', ['ephemeralTool']);

    await expect(
      agent.generate('resume', {
        runId: 'missing-tool-run',
        requestContext,
        toolsetsMode: 'replace',
        toolsets: {},
      }),
    ).rejects.toThrow(/Cannot reconstruct replacement tool implementations/);
    expect(doGenerate).not.toHaveBeenCalled();
  });

  it.each([
    ['caller', undefined],
    [
      'processor',
      {
        id: 'force-hidden-tool',
        processInputStep: () => ({ toolChoice: { type: 'tool', toolName: 'hiddenTool' } as const }),
      },
    ],
  ])('rejects a %s-forced tool outside the replacement surface before provider invocation', async (_, processor) => {
    const doGenerate = vi.fn();
    const requestContext = new RequestContext();
    const runId = `forced-${processor ? 'processor' : 'caller'}-run`;
    const agent = new Agent({
      id: 'forced-choice-agent',
      name: 'forced-choice-agent',
      instructions: 'test',
      model: new MockLanguageModelV2({ doGenerate }),
      tools: { hiddenTool: testTool('hiddenTool') },
      ...(processor ? { inputProcessors: [processor] } : {}),
    });

    await expect(
      agent.generate('test', {
        runId,
        requestContext,
        toolsetsMode: 'replace',
        toolsets: { mode: { modeTool: testTool('modeTool') } },
        ...(processor ? {} : { toolChoice: { type: 'tool', toolName: 'hiddenTool' } as const }),
      }),
    ).rejects.toThrow(/outside the execution's replacement tool surface/);
    expect(doGenerate).not.toHaveBeenCalled();
    expect(readToolSurfaceFence(requestContext, runId)).toBeUndefined();
  });

  it('applies replacement to stream defaults and cleans terminal run fences', async () => {
    const visibleToolNames: string[][] = [];
    const model = new MockLanguageModelV2({
      doStream: async options => {
        visibleToolNames.push((options.tools ?? []).map(tool => tool.name));
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: convertArrayToReadableStream([
            { type: 'stream-start', warnings: [] },
            { type: 'response-metadata', id: 'stream-id', modelId: 'mock', timestamp: new Date(0) },
            { type: 'text-start', id: 'text-1' },
            { type: 'text-delta', id: 'text-1', delta: 'done' },
            { type: 'text-end', id: 'text-1' },
            { type: 'finish', finishReason: 'stop', usage },
          ]),
        };
      },
    });
    const agent = new Agent({
      id: 'stream-agent',
      name: 'stream-agent',
      instructions: 'test',
      model,
      tools: { assignedTool: testTool('assignedTool') },
      defaultOptions: {
        toolsetsMode: 'replace',
        toolsets: { defaults: { defaultTool: testTool('defaultTool') } },
      },
    });
    const requestContext = new RequestContext();
    const output = await agent.stream('test', {
      runId: 'stream-run',
      requestContext,
      toolsets: { mode: { modeTool: testTool('modeTool') } },
    });
    await output.getFullOutput();

    expect(visibleToolNames).toEqual([['modeTool']]);
    expect(readToolSurfaceFence(requestContext, 'stream-run')).toBeUndefined();
  });

  it('isolates concurrent replacement runs sharing one RequestContext', async () => {
    const model = new MockLanguageModelV2({
      doGenerate: async options => ({
        rawCall: { rawPrompt: null, rawSettings: {} },
        finishReason: 'stop' as const,
        usage,
        content: [
          {
            type: 'text' as const,
            text: (options.tools ?? []).map(tool => tool.name).join(','),
          },
        ],
        warnings: [],
      }),
    });
    const agent = new Agent({ id: 'concurrent-agent', name: 'concurrent-agent', instructions: 'test', model });
    const requestContext = new RequestContext();

    const [first, second] = await Promise.all([
      agent.generate('first', {
        runId: 'run-a',
        requestContext,
        toolsetsMode: 'replace',
        toolsets: { a: { toolA: testTool('toolA') } },
      }),
      agent.generate('second', {
        runId: 'run-b',
        requestContext,
        toolsetsMode: 'replace',
        toolsets: { b: { toolB: testTool('toolB') } },
      }),
    ]);

    expect(new Set([first.text, second.text])).toEqual(new Set(['toolA', 'toolB']));
    expect(readToolSurfaceFence(requestContext, 'run-a')).toBeUndefined();
    expect(readToolSurfaceFence(requestContext, 'run-b')).toBeUndefined();
  });
});
