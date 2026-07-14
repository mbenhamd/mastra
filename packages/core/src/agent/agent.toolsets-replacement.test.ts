import { MockLanguageModelV2 } from '@internal/ai-sdk-v5/test';
import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';

import { Mastra } from '../mastra';
import { RequestContext } from '../request-context';
import { InMemoryStore } from '../storage';
import { createTool } from '../tools';
import { convertArrayToReadableStream } from './__tests__/mock-model';
import { Agent } from './agent';
import {
  clearToolSurfaceFence,
  readToolSurfaceFence,
  stageToolSurfaceFenceRestore,
  stampToolSurfaceFence,
} from './tool-surface-fence';

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
              content: [{ type: 'tool-call' as const, toolCallId: 'mode-call', toolName: 'modeTool', input: '{}' }],
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

  it('does not retain a stateful processor tool container after enforcing the replacement surface', async () => {
    const modeTool = testTool('modeTool');
    const injectedTool = testTool('injectedTool');
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
    let ownKeysCalls = 0;
    const processorTools = new Proxy<Record<string, unknown>>(
      {},
      {
        ownKeys: () => {
          ownKeysCalls++;
          return ownKeysCalls === 1 ? ['modeTool'] : ['modeTool', 'injectedTool'];
        },
        getOwnPropertyDescriptor: (_target, key) => ({
          value: key === 'modeTool' ? modeTool : injectedTool,
          writable: true,
          enumerable: true,
          configurable: true,
        }),
        get: (_target, key) => (key === 'modeTool' ? modeTool : injectedTool),
      },
    );
    const agent = new Agent({
      id: 'stateful-container-agent',
      name: 'stateful-container-agent',
      instructions: 'test',
      model,
      inputProcessors: [
        {
          id: 'stateful-container',
          processInputStep: () => ({ tools: processorTools }),
        },
      ],
    });

    await agent.generate('test', {
      toolsetsMode: 'replace',
      toolsets: { mode: { modeTool } },
    });

    expect(visibleToolNames).toEqual([['modeTool']]);
    expect(ownKeysCalls).toBe(1);
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

  it('fails closed when any implementation in a persisted resume tool surface cannot be reconstructed', async () => {
    const doGenerate = vi.fn();
    const agent = new Agent({
      id: 'missing-resume-tool-agent',
      name: 'missing-resume-tool-agent',
      instructions: 'test',
      model: new MockLanguageModelV2({ doGenerate }),
    });
    const requestContext = new RequestContext();
    stageToolSurfaceFenceRestore(requestContext, 'missing-tool-run', ['originalTool', 'ephemeralTool']);

    await expect(
      agent.generate('resume', {
        runId: 'missing-tool-run',
        requestContext,
        toolsetsMode: 'replace',
        toolsets: { mode: { originalTool: testTool('originalTool') } },
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

  it.each(['caller', 'processor'] as const)(
    'rejects a shorthand %s-forced tool outside the replacement surface before provider invocation',
    async source => {
      const doGenerate = vi.fn();
      const requestContext = new RequestContext();
      const agent = new Agent({
        id: 'shorthand-forced-choice-agent',
        name: 'shorthand-forced-choice-agent',
        instructions: 'test',
        model: new MockLanguageModelV2({ doGenerate }),
        ...(source === 'processor'
          ? {
              inputProcessors: [
                {
                  id: 'force-hidden-tool-shorthand',
                  processInputStep: () => ({ toolChoice: { toolName: 'hiddenTool' } as any }),
                },
              ],
            }
          : {}),
      });

      await expect(
        agent.generate('test', {
          runId: `shorthand-${source}-run`,
          requestContext,
          toolsetsMode: 'replace',
          toolsets: { mode: { modeTool: testTool('modeTool') } },
          ...(source === 'caller' ? { toolChoice: { toolName: 'hiddenTool' } as any } : {}),
        }),
      ).rejects.toThrow(/outside the execution's replacement tool surface/);
      expect(doGenerate).not.toHaveBeenCalled();
    },
  );

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

  it('does not eagerly consume an ordinary stream without a replacement fence', async () => {
    let pulled = false;
    const model = new MockLanguageModelV2({
      doStream: async () => ({
        rawCall: { rawPrompt: null, rawSettings: {} },
        warnings: [],
        stream: new ReadableStream({
          pull(controller) {
            pulled = true;
            controller.enqueue({ type: 'finish', finishReason: 'stop', usage });
            controller.close();
          },
        }),
      }),
    });
    const agent = new Agent({ id: 'lazy-stream-agent', name: 'lazy-stream-agent', instructions: 'test', model });

    const output = await agent.stream('test');
    await Promise.resolve();
    expect(pulled).toBe(false);

    await output.getFullOutput();
    expect(pulled).toBe(true);
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

  it('assembles replacement execution tools without retaining a caller-visible run fence', async () => {
    const model = new MockLanguageModelV2({
      doGenerate: async () => ({
        rawCall: { rawPrompt: null, rawSettings: {} },
        finishReason: 'stop' as const,
        usage,
        content: [],
        warnings: [],
      }),
    });
    const agent = new Agent({ id: 'assembly-agent', name: 'assembly-agent', instructions: 'test', model });
    const requestContext = new RequestContext();
    const modeTool = testTool('modeTool');
    const options = {
      runId: 'assembly-run',
      requestContext,
      toolsetsMode: 'replace' as const,
      toolsets: { mode: { modeTool } },
    };

    expect(Object.keys(await agent.getToolsForExecution(options))).toEqual(['modeTool']);
    expect(Object.keys(await agent.getToolsForExecution(options))).toEqual(['modeTool']);
    expect(readToolSurfaceFence(requestContext, 'assembly-run')).toBeUndefined();

    const directFence = stampToolSurfaceFence(
      requestContext,
      'assembly-run',
      { directTool: testTool('directTool') },
      'direct-owner',
    );
    expect(directFence.allowedNames).toEqual(['directTool']);
    expect(clearToolSurfaceFence(requestContext, 'assembly-run', 'direct-owner')).toBe(true);
  });

  it('rejects a colliding run ID without clearing the first execution fence', async () => {
    let releaseFirst!: () => void;
    let markStarted!: () => void;
    const firstStarted = new Promise<void>(resolve => {
      markStarted = resolve;
    });
    const firstGate = new Promise<void>(resolve => {
      releaseFirst = resolve;
    });
    const visibleToolNames: string[][] = [];
    const model = new MockLanguageModelV2({
      doGenerate: async options => {
        visibleToolNames.push((options.tools ?? []).map(tool => tool.name));
        markStarted();
        await firstGate;
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          finishReason: 'stop' as const,
          usage,
          content: [{ type: 'text' as const, text: 'done' }],
          warnings: [],
        };
      },
    });
    const agent = new Agent({ id: 'colliding-agent', name: 'colliding-agent', instructions: 'test', model });
    const requestContext = new RequestContext();
    const first = agent.generate('first', {
      runId: 'shared-run',
      requestContext,
      toolsetsMode: 'replace',
      toolsets: { first: { firstTool: testTool('firstTool') } },
    });
    await firstStarted;

    await expect(
      agent.generate('second', {
        runId: 'shared-run',
        requestContext,
        toolsetsMode: 'replace',
        toolsets: { second: { secondTool: testTool('secondTool') } },
      }),
    ).rejects.toThrow(/another execution|active replacement tool surface|concurrent execution/);
    expect(readToolSurfaceFence(requestContext, 'shared-run')?.allowedNames).toEqual(['firstTool']);

    releaseFirst();
    await first;
    expect(visibleToolNames).toEqual([['firstTool']]);
    expect(readToolSurfaceFence(requestContext, 'shared-run')).toBeUndefined();
  });

  it(
    'retains the original replacement surface across direct Agent suspension and resume',
    { timeout: 30_000 },
    async () => {
      const visibleToolNames: string[][] = [];
      const approvedExecute = vi.fn().mockResolvedValue('approved');
      let callCount = 0;
      const model = new MockLanguageModelV2({
        doStream: async options => {
          visibleToolNames.push((options.tools ?? []).map(tool => tool.name));
          callCount++;
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: convertArrayToReadableStream(
              callCount === 1
                ? [
                    { type: 'stream-start', warnings: [] },
                    { type: 'response-metadata', id: 'initial', modelId: 'mock', timestamp: new Date(0) },
                    {
                      type: 'tool-call',
                      toolCallId: 'approval-call',
                      toolName: 'approvalTool',
                      input: '{}',
                      providerExecuted: false,
                    },
                    { type: 'finish', finishReason: 'tool-calls', usage },
                  ]
                : [
                    { type: 'stream-start', warnings: [] },
                    { type: 'response-metadata', id: 'resumed', modelId: 'mock', timestamp: new Date(0) },
                    { type: 'text-start', id: 'text-1' },
                    { type: 'text-delta', id: 'text-1', delta: 'done' },
                    { type: 'text-end', id: 'text-1' },
                    { type: 'finish', finishReason: 'stop', usage },
                  ],
            ),
          };
        },
      });
      const agent = new Agent({
        id: 'resume-replacement-agent',
        name: 'resume-replacement-agent',
        instructions: 'test',
        model,
        tools: { adminTool: testTool('adminTool') },
      });
      new Mastra({ agents: { agent }, logger: false, storage: new InMemoryStore() });
      const requestContext = new RequestContext();
      const approvalTool = createTool({
        id: 'approvalTool',
        description: 'approvalTool',
        inputSchema: z.object({}),
        requireApproval: true,
        execute: approvedExecute,
      });
      const initial = await agent.stream('start', {
        runId: 'resume-replacement-run',
        requestContext,
        toolsetsMode: 'replace',
        toolsets: { approval: { approvalTool } },
      });
      const suspended = await initial.getFullOutput();
      expect(suspended.finishReason).toBe('suspended');

      const resumed = await agent.resumeStream(
        { approved: true },
        { runId: initial.runId, toolCallId: 'approval-call', requestContext },
      );
      await resumed.getFullOutput();

      expect(visibleToolNames).toEqual([['approvalTool'], ['approvalTool']]);
      expect(approvedExecute).toHaveBeenCalledOnce();
      expect(readToolSurfaceFence(requestContext, initial.runId)).toBeUndefined();
    },
  );

  it(
    'fails closed when a replacement stream resumes with a fresh context and no reconstruction inputs',
    { timeout: 30_000 },
    async () => {
      const visibleToolNames: string[][] = [];
      const model = new MockLanguageModelV2({
        doStream: async options => {
          visibleToolNames.push((options.tools ?? []).map(tool => tool.name));
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: convertArrayToReadableStream([
              { type: 'stream-start', warnings: [] },
              { type: 'response-metadata', id: 'initial', modelId: 'mock', timestamp: new Date(0) },
              {
                type: 'tool-call',
                toolCallId: 'approval-call',
                toolName: 'approvalTool',
                input: '{}',
                providerExecuted: false,
              },
              { type: 'finish', finishReason: 'tool-calls', usage },
            ]),
          };
        },
      });
      const agent = new Agent({
        id: 'fresh-context-resume-agent',
        name: 'fresh-context-resume-agent',
        instructions: 'test',
        model,
      });
      new Mastra({ agents: { agent }, logger: false, storage: new InMemoryStore() });
      const initialContext = new RequestContext();
      const approvalTool = createTool({
        id: 'approvalTool',
        description: 'approvalTool',
        inputSchema: z.object({}),
        requireApproval: true,
        execute: vi.fn(),
      });
      const initial = await agent.stream('start', {
        runId: 'fresh-context-replacement-run',
        requestContext: initialContext,
        toolsetsMode: 'replace',
        toolsets: { approval: { approvalTool } },
      });
      const suspended = await initial.getFullOutput();
      expect(suspended.finishReason).toBe('suspended');

      await expect(
        agent.resumeStream({ approved: true }, { runId: initial.runId, toolCallId: 'approval-call' }),
      ).rejects.toThrow(/without toolsetsMode "replace"/);
      expect(visibleToolNames).toEqual([['approvalTool']]);
    },
  );

  it('fails closed when replacement generate resumes with a fresh context and no reconstruction inputs', async () => {
    const doGenerate = vi.fn(async options => ({
      rawCall: { rawPrompt: null, rawSettings: {} },
      finishReason: 'tool-calls' as const,
      usage,
      content: [
        {
          type: 'tool-call' as const,
          toolCallId: 'approval-call',
          toolName: (options.tools ?? [])[0]!.name,
          input: '{}',
        },
      ],
      warnings: [],
    }));
    const agent = new Agent({
      id: 'fresh-context-generate-agent',
      name: 'fresh-context-generate-agent',
      instructions: 'test',
      model: new MockLanguageModelV2({ doGenerate }),
    });
    new Mastra({ agents: { agent }, logger: false, storage: new InMemoryStore() });
    const approvalTool = createTool({
      id: 'approvalTool',
      description: 'approvalTool',
      inputSchema: z.object({}),
      requireApproval: true,
      execute: vi.fn(),
    });
    const initial = await agent.generate('start', {
      runId: 'fresh-context-generate-run',
      requestContext: new RequestContext(),
      toolsetsMode: 'replace',
      toolsets: { approval: { approvalTool } },
    });
    expect(initial.finishReason).toBe('suspended');

    await expect(
      agent.resumeGenerate({ approved: true }, { runId: initial.runId, toolCallId: 'approval-call' }),
    ).rejects.toThrow(/without toolsetsMode "replace"/);
    expect(doGenerate).toHaveBeenCalledOnce();
  });
});
