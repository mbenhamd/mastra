import { convertArrayToReadableStream, MockLanguageModelV2 } from '@internal/ai-sdk-v5/test';
import { describe, expect, it } from 'vitest';
import { z } from 'zod/v4';
import { Agent } from '../agent';
import { Mastra } from '../mastra';
import { MockMemory } from '../memory/mock';
import { RequestContext } from '../request-context';
import { createTool } from '../tools';
import { LocalFilesystem, Workspace } from '../workspace';
import type { InputProcessor, ProcessInputStepArgs } from './index';

/**
 * Tests that tools returned by processors in processInputStep receive requestContext.
 *
 * Issue: https://github.com/mastra-ai/mastra/issues/12967
 *
 * When a processor (like ToolSearchProcessor) dynamically adds tools via processInputStep,
 * those raw Mastra Tool objects need to be converted through makeCoreTool with the
 * original requestContext so that tool.execute() receives it in the context.
 */
describe('Processor-returned tools receive requestContext', () => {
  const createToolCallingModel = (toolName: string, onProviderTools?: (tools: unknown) => void) => {
    let callCount = 0;

    return new MockLanguageModelV2({
      doStream: async ({ tools }) => {
        callCount++;
        onProviderTools?.(tools);

        return {
          stream: convertArrayToReadableStream(
            callCount === 1
              ? [
                  { type: 'stream-start', warnings: [] },
                  { type: 'response-metadata', id: 'id-0', modelId: 'mock-model-id', timestamp: new Date(0) },
                  {
                    type: 'tool-call',
                    toolCallId: 'call-1',
                    toolName,
                    input: JSON.stringify({ query: 'test' }),
                  },
                  {
                    type: 'finish',
                    finishReason: 'tool-calls',
                    usage: { inputTokens: 10, outputTokens: 5, totalTokens: 15 },
                  },
                ]
              : [
                  { type: 'stream-start', warnings: [] },
                  { type: 'response-metadata', id: 'id-1', modelId: 'mock-model-id', timestamp: new Date(0) },
                  { type: 'text-start', id: 'text-1' },
                  { type: 'text-delta', id: 'text-1', delta: 'Done.' },
                  { type: 'text-end', id: 'text-1' },
                  {
                    type: 'finish',
                    finishReason: 'stop',
                    usage: { inputTokens: 15, outputTokens: 10, totalTokens: 25 },
                  },
                ],
          ),
          rawCall: { rawPrompt: [], rawSettings: {} },
          warnings: [],
        };
      },
    });
  };

  it('should forward requestContext to tools added by processInputStep', async () => {
    let capturedRequestContext: RequestContext | undefined;

    // A tool that captures the requestContext it receives during execute
    const dynamicTool = createTool({
      id: 'dynamic-tool',
      description: 'A dynamically loaded tool that reads requestContext',
      inputSchema: z.object({
        query: z.string(),
      }),
      execute: async (input, { requestContext }) => {
        capturedRequestContext = requestContext;
        return `Result for: ${input.query}`;
      },
    });

    // A processor that adds a tool dynamically (simulating ToolSearchProcessor)
    const toolInjectorProcessor = {
      id: 'tool-injector-processor',
      processInputStep: async (args: ProcessInputStepArgs) => {
        return {
          tools: {
            ...(args.tools ?? {}),
            'dynamic-tool': dynamicTool,
          },
        };
      },
    } as InputProcessor;

    const mockModel = new MockLanguageModelV2({
      doStream: async ({ prompt }) => {
        const hasToolResults = prompt.some(
          (msg: any) =>
            msg.role === 'tool' ||
            (Array.isArray(msg.content) && msg.content.some((c: any) => c.type === 'tool-result')),
        );

        if (!hasToolResults) {
          return {
            stream: convertArrayToReadableStream([
              { type: 'stream-start', warnings: [] },
              { type: 'response-metadata', id: 'id-0', modelId: 'mock-model-id', timestamp: new Date(0) },
              {
                type: 'tool-call',
                toolCallId: 'call-1',
                toolName: 'dynamic-tool',
                input: JSON.stringify({ query: 'test' }),
              },
              {
                type: 'finish',
                finishReason: 'tool-calls',
                usage: { inputTokens: 10, outputTokens: 5, totalTokens: 15 },
              },
            ]),
            rawCall: { rawPrompt: [], rawSettings: {} },
            warnings: [],
          };
        } else {
          return {
            stream: convertArrayToReadableStream([
              { type: 'stream-start', warnings: [] },
              { type: 'response-metadata', id: 'id-1', modelId: 'mock-model-id', timestamp: new Date(0) },
              { type: 'text-start', id: 'text-1' },
              { type: 'text-delta', id: 'text-1', delta: 'Done.' },
              { type: 'text-end', id: 'text-1' },
              {
                type: 'finish',
                finishReason: 'stop',
                usage: { inputTokens: 15, outputTokens: 10, totalTokens: 25 },
              },
            ]),
            rawCall: { rawPrompt: [], rawSettings: {} },
            warnings: [],
          };
        }
      },
    });

    const agent = new Agent({
      id: 'test-agent-rc',
      name: 'Test Agent RC',
      instructions: 'Use tools when asked',
      model: mockModel as any,
      inputProcessors: [toolInjectorProcessor],
    });

    const requestContext = new RequestContext();
    requestContext.set('userId', 'user-123');
    requestContext.set('apiKey', 'secret-key');

    const stream = await agent.stream('Call the dynamic tool', {
      maxSteps: 5,
      requestContext,
    });

    for await (const _chunk of stream.fullStream) {
      // drain the stream
    }

    // The key assertion: the tool should have received the requestContext
    expect(capturedRequestContext).toBeDefined();
    expect(capturedRequestContext!.get('userId')).toBe('user-123');
    expect(capturedRequestContext!.get('apiKey')).toBe('secret-key');
  });

  it('should forward requestContext to dynamically added tools alongside existing tools', async () => {
    let dynamicToolContext: RequestContext | undefined;
    let existingToolContext: RequestContext | undefined;

    const existingTool = createTool({
      id: 'existing-tool',
      description: 'An existing tool',
      inputSchema: z.object({ input: z.string() }),
      execute: async (_input, { requestContext }) => {
        existingToolContext = requestContext;
        return 'existing result';
      },
    });

    const addedTool = createTool({
      id: 'added-tool',
      description: 'A dynamically added tool',
      inputSchema: z.object({ input: z.string() }),
      execute: async (_input, { requestContext }) => {
        dynamicToolContext = requestContext;
        return 'added result';
      },
    });

    const toolInjectorProcessor = {
      id: 'tool-injector',
      processInputStep: async (args: ProcessInputStepArgs) => {
        return {
          tools: {
            ...(args.tools ?? {}),
            'added-tool': addedTool,
          },
        };
      },
    } as InputProcessor;

    // LLM calls added-tool first, then existing-tool
    let callCount = 0;
    const mockModel = new MockLanguageModelV2({
      doStream: async () => {
        callCount++;

        if (callCount === 1) {
          // First call: invoke the dynamically added tool
          return {
            stream: convertArrayToReadableStream([
              { type: 'stream-start', warnings: [] },
              { type: 'response-metadata', id: 'id-0', modelId: 'mock', timestamp: new Date(0) },
              {
                type: 'tool-call',
                toolCallId: 'call-1',
                toolName: 'added-tool',
                input: JSON.stringify({ input: 'test1' }),
              },
              {
                type: 'finish',
                finishReason: 'tool-calls',
                usage: { inputTokens: 10, outputTokens: 5, totalTokens: 15 },
              },
            ]),
            rawCall: { rawPrompt: [], rawSettings: {} },
            warnings: [],
          };
        } else if (callCount === 2) {
          // Second call: invoke the existing tool
          return {
            stream: convertArrayToReadableStream([
              { type: 'stream-start', warnings: [] },
              { type: 'response-metadata', id: 'id-1', modelId: 'mock', timestamp: new Date(0) },
              {
                type: 'tool-call',
                toolCallId: 'call-2',
                toolName: 'existing-tool',
                input: JSON.stringify({ input: 'test2' }),
              },
              {
                type: 'finish',
                finishReason: 'tool-calls',
                usage: { inputTokens: 10, outputTokens: 5, totalTokens: 15 },
              },
            ]),
            rawCall: { rawPrompt: [], rawSettings: {} },
            warnings: [],
          };
        } else {
          // Final call: just produce text
          return {
            stream: convertArrayToReadableStream([
              { type: 'stream-start', warnings: [] },
              { type: 'response-metadata', id: 'id-2', modelId: 'mock', timestamp: new Date(0) },
              { type: 'text-start', id: 'text-1' },
              { type: 'text-delta', id: 'text-1', delta: 'Done.' },
              { type: 'text-end', id: 'text-1' },
              {
                type: 'finish',
                finishReason: 'stop',
                usage: { inputTokens: 5, outputTokens: 5, totalTokens: 10 },
              },
            ]),
            rawCall: { rawPrompt: [], rawSettings: {} },
            warnings: [],
          };
        }
      },
    });

    const requestContext = new RequestContext();
    requestContext.set('tenantId', 'tenant-abc');

    const agent = new Agent({
      id: 'test-agent-both',
      name: 'Test Agent Both',
      instructions: 'Use tools',
      model: mockModel as any,
      tools: { 'existing-tool': existingTool },
      inputProcessors: [toolInjectorProcessor],
    });

    const stream = await agent.stream('Call both tools', {
      maxSteps: 5,
      requestContext,
    });

    for await (const _chunk of stream.fullStream) {
      // drain
    }

    // Both tools should have received requestContext
    expect(dynamicToolContext).toBeDefined();
    expect(dynamicToolContext!.get('tenantId')).toBe('tenant-abc');

    expect(existingToolContext).toBeDefined();
    expect(existingToolContext!.get('tenantId')).toBe('tenant-abc');
  });

  it('forwards Mastra, memory, workspace, and thread context to processor-added tools', async () => {
    let capturedContext: Record<string, any> | undefined;
    const memory = new MockMemory();
    const workspace = new Workspace({
      id: 'processor-workspace',
      name: 'Processor Workspace',
      filesystem: new LocalFilesystem({ basePath: process.cwd() }),
    });
    const mastra = new Mastra();
    const dynamicTool = createTool({
      id: 'context-tool',
      description: 'Capture the complete dynamically loaded tool context',
      inputSchema: z.object({ query: z.string() }),
      execute: async (_input, context) => {
        capturedContext = context;
        return 'captured';
      },
    });
    const processor = {
      id: 'context-tool-injector',
      processInputStep: async (args: ProcessInputStepArgs) => ({
        tools: { ...(args.tools ?? {}), 'context-tool': dynamicTool },
      }),
    } as InputProcessor;
    const requestContext = new RequestContext();
    requestContext.set('tenantId', 'tenant-context');
    const providerTools: unknown[] = [];
    const agent = new Agent({
      id: 'processor-context-agent',
      name: 'Processor Context Agent',
      instructions: 'Use the context tool.',
      model: createToolCallingModel('context-tool', tools => providerTools.push(tools)),
      mastra,
      memory,
      workspace,
      inputProcessors: [processor],
    });

    const stream = await agent.stream('Capture the context', {
      maxSteps: 2,
      requestContext,
      memory: { thread: 'thread-context', resource: 'resource-context' },
    });
    const errors: unknown[] = [];
    for await (const chunk of stream.fullStream) {
      if (chunk.type === 'error') {
        errors.push(chunk.payload.error);
      }
    }

    expect(errors).toEqual([]);
    expect(capturedContext).toBeDefined();
    expect(capturedContext?.mastra).toBeDefined();
    expect(capturedContext?.mastra).not.toBe(mastra);
    expect(capturedContext?.mastra.getLogger()).toBe(mastra.getLogger());
    expect(capturedContext?.memory).toBe(memory);
    expect(capturedContext?.workspace).toBe(workspace);
    expect(capturedContext?.agent?.threadId).toBe('thread-context');
    expect(capturedContext?.agent?.resourceId).toBe('resource-context');
    expect(capturedContext?.requestContext).toBe(requestContext);
    expect(capturedContext?.writer?.write).toEqual(expect.any(Function));
    expect(capturedContext?.writer?.custom).toEqual(expect.any(Function));

    const serializedProviderTools = JSON.stringify(providerTools[0]);
    expect(serializedProviderTools).not.toContain('tenant-context');
    expect(serializedProviderTools).not.toContain('thread-context');
    expect(serializedProviderTools).not.toContain('resource-context');
    expect(serializedProviderTools).not.toContain('processor-workspace');
  });

  it('keeps optional context absent for processor-added tools', async () => {
    let capturedContext: Record<string, any> | undefined;
    const dynamicTool = createTool({
      id: 'optional-context-tool',
      description: 'Capture absent optional context',
      inputSchema: z.object({ query: z.string() }),
      execute: async (_input, context) => {
        capturedContext = context;
        return 'captured';
      },
    });
    const processor = {
      id: 'optional-context-tool-injector',
      processInputStep: async () => ({ tools: { 'optional-context-tool': dynamicTool } }),
    } as InputProcessor;
    const agent = new Agent({
      id: 'processor-optional-context-agent',
      name: 'Processor Optional Context Agent',
      instructions: 'Use the optional context tool.',
      model: createToolCallingModel('optional-context-tool'),
      inputProcessors: [processor],
    });

    const stream = await agent.stream('Capture absent context', { maxSteps: 2 });
    await stream.consumeStream();

    expect(capturedContext).toBeDefined();
    expect(capturedContext?.mastra).toBeUndefined();
    expect(capturedContext?.memory).toBeUndefined();
    expect(capturedContext?.workspace).toBeUndefined();
    expect(capturedContext?.agent?.threadId).toBeUndefined();
    expect(capturedContext?.agent?.resourceId).toBeUndefined();
    expect(capturedContext?.requestContext).toBeInstanceOf(RequestContext);
  });
});
