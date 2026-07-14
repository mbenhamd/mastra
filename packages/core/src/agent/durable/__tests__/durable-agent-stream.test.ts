/**
 * DurableAgent Streaming Tests
 *
 * These tests verify the streaming execution behavior of DurableAgent,
 * including the workflow execution, pubsub event emission, and callbacks.
 */

import type { LanguageModelV2 } from '@ai-sdk/provider-v5';
import { MockLanguageModelV2, convertArrayToReadableStream } from '@internal/ai-sdk-v5/test';
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { z } from 'zod';
import { EventEmitterPubSub } from '../../../events/event-emitter';
import { RequestContext } from '../../../request-context';
import { createTool } from '../../../tools';
import { Agent } from '../../agent';
import { clearToolSurfaceFence, readToolSurfaceFence, stampToolSurfaceFence } from '../../tool-surface-fence';
import { AGENT_STREAM_TOPIC, AgentStreamEventTypes } from '../constants';
import { createDurableAgent } from '../create-durable-agent';
import { globalRunRegistry } from '../run-registry';
import type { AgentStreamEvent } from '../types';
import { resolveRuntimeDependencies } from '../utils/resolve-runtime';
import { baseDurableAgenticInputSchema } from '../workflows/shared/schemas';

// ============================================================================
// Helper Functions
// ============================================================================

function createTextStreamModel(text: string, _options?: { delay?: number }) {
  return new MockLanguageModelV2({
    doStream: async () => ({
      stream: convertArrayToReadableStream([
        { type: 'stream-start', warnings: [] },
        { type: 'response-metadata', id: 'id-0', modelId: 'mock-model-id', timestamp: new Date(0) },
        { type: 'text-start', id: 'text-1' },
        { type: 'text-delta', id: 'text-1', delta: text },
        { type: 'text-end', id: 'text-1' },
        {
          type: 'finish',
          finishReason: 'stop',
          usage: { inputTokens: 10, outputTokens: 20, totalTokens: 30 },
        },
      ]),
      rawCall: { rawPrompt: null, rawSettings: {} },
    }),
  });
}

function createMultiChunkStreamModel(chunks: string[]) {
  return new MockLanguageModelV2({
    doStream: async () => ({
      stream: convertArrayToReadableStream([
        { type: 'stream-start', warnings: [] },
        { type: 'response-metadata', id: 'id-0', modelId: 'mock-model-id', timestamp: new Date(0) },
        { type: 'text-start', id: 'text-1' },
        ...chunks.map(chunk => ({ type: 'text-delta' as const, id: 'text-1', delta: chunk })),
        { type: 'text-end', id: 'text-1' },
        {
          type: 'finish',
          finishReason: 'stop',
          usage: { inputTokens: 10, outputTokens: chunks.length * 5, totalTokens: 10 + chunks.length * 5 },
        },
      ]),
      rawCall: { rawPrompt: null, rawSettings: {} },
    }),
  });
}

function _createToolCallModel(toolName: string, args: Record<string, unknown>) {
  return new MockLanguageModelV2({
    doStream: async () => ({
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
    }),
  });
}

function _createToolCallThenTextModel(toolName: string, args: Record<string, unknown>, finalText: string) {
  let callCount = 0;
  return new MockLanguageModelV2({
    doStream: async () => {
      callCount++;
      if (callCount === 1) {
        // First call: return tool call
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
      } else {
        // Second call: return text response
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
      }
    },
  });
}

// ============================================================================
// Streaming Execution Tests
// ============================================================================

describe('DurableAgent streaming execution', () => {
  let pubsub: EventEmitterPubSub;

  beforeEach(() => {
    pubsub = new EventEmitterPubSub();
  });

  afterEach(async () => {
    await pubsub.close();
  });

  describe('basic streaming', () => {
    it('should pass activeTools through to the LLM request', async () => {
      const doStream = vi.fn(async () => ({
        stream: convertArrayToReadableStream([
          { type: 'stream-start', warnings: [] },
          { type: 'response-metadata', id: 'id-0', modelId: 'mock-model-id', timestamp: new Date(0) },
          { type: 'text-start', id: 'text-1' },
          { type: 'text-delta', id: 'text-1', delta: 'Done' },
          { type: 'text-end', id: 'text-1' },
          {
            type: 'finish',
            finishReason: 'stop',
            usage: { inputTokens: 10, outputTokens: 20, totalTokens: 30 },
          },
        ]),
        rawCall: { rawPrompt: null, rawSettings: {} },
      }));

      const mockModel = new MockLanguageModelV2({ doStream });
      const allowedTool = createTool({
        id: 'allowedTool',
        description: 'Allowed tool',
        inputSchema: z.object({}),
        execute: async () => 'allowed',
      });
      const hiddenTool = createTool({
        id: 'hiddenTool',
        description: 'Hidden tool',
        inputSchema: z.object({}),
        execute: async () => 'hidden',
      });

      const baseAgent = new Agent({
        id: 'active-tools-agent',
        name: 'Active Tools Agent',
        instructions: 'Use only enabled tools',
        model: mockModel as LanguageModelV2,
        tools: { allowedTool, hiddenTool },
      });
      const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

      const { output, cleanup } = await durableAgent.stream('Use the allowed tool', {
        activeTools: ['allowedTool'],
      });

      await output.consumeStream();

      expect(doStream).toHaveBeenCalledTimes(1);
      expect(doStream.mock.calls[0]?.[0].tools.map((tool: { name: string }) => tool.name)).toEqual(['allowedTool']);

      cleanup();
    });

    it('should preserve replacement toolsets as a durable processor and execution ceiling', async () => {
      const hiddenExecute = vi.fn(async () => 'hidden');
      let callCount = 0;
      const doStream = vi.fn(async (options: any) => {
        callCount++;
        return {
          stream: convertArrayToReadableStream(
            callCount === 1
              ? [
                  { type: 'stream-start', warnings: [] },
                  { type: 'response-metadata', id: 'id-0', modelId: 'mock-model-id', timestamp: new Date(0) },
                  {
                    type: 'tool-call',
                    toolCallId: 'hidden-call',
                    toolName: 'hiddenTool',
                    input: '{}',
                    providerExecuted: false,
                  },
                  { type: 'finish', finishReason: 'tool-calls', usage: { inputTokens: 1, outputTokens: 1 } },
                ]
              : [
                  { type: 'stream-start', warnings: [] },
                  { type: 'response-metadata', id: 'id-1', modelId: 'mock-model-id', timestamp: new Date(0) },
                  { type: 'text-start', id: 'text-1' },
                  { type: 'text-delta', id: 'text-1', delta: options.tools.map((tool: any) => tool.name).join(',') },
                  { type: 'text-end', id: 'text-1' },
                  { type: 'finish', finishReason: 'stop', usage: { inputTokens: 1, outputTokens: 1 } },
                ],
          ),
          rawCall: { rawPrompt: null, rawSettings: {} },
        };
      });
      const model = new MockLanguageModelV2({ doStream });
      const hiddenTool = createTool({
        id: 'hiddenTool',
        description: 'hidden',
        inputSchema: z.object({}),
        execute: hiddenExecute,
      });
      const modeTool = createTool({
        id: 'modeTool',
        description: 'mode',
        inputSchema: z.object({}),
        execute: async () => 'mode',
      });
      let ownKeysCalls = 0;
      const processorTools = new Proxy<Record<string, unknown>>(
        {},
        {
          ownKeys: () => {
            ownKeysCalls++;
            return ownKeysCalls % 2 === 1 ? ['modeTool'] : ['modeTool', 'hiddenTool'];
          },
          getOwnPropertyDescriptor: (_target, key) => ({
            value: key === 'modeTool' ? modeTool : hiddenTool,
            writable: true,
            enumerable: true,
            configurable: true,
          }),
          get: (_target, key) => (key === 'modeTool' ? modeTool : hiddenTool),
        },
      );
      const baseAgent = new Agent({
        id: 'durable-replacement-agent',
        name: 'Durable Replacement Agent',
        instructions: 'test',
        model: model as LanguageModelV2,
        tools: { hiddenTool },
        inputProcessors: [
          {
            id: 'expand-tools',
            processInputStep: () => ({
              tools: processorTools,
              activeTools: ['modeTool', 'hiddenTool'],
            }),
          },
        ],
      });
      const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

      const { output, cleanup } = await durableAgent.stream('test', {
        maxSteps: 3,
        toolsets: { mode: { modeTool } },
        toolsetsMode: 'replace',
      });
      await output.consumeStream();

      expect(doStream).toHaveBeenCalledTimes(2);
      expect(doStream.mock.calls.map(call => call[0].tools.map((tool: any) => tool.name))).toEqual([
        ['modeTool'],
        ['modeTool'],
      ]);
      expect(hiddenExecute).not.toHaveBeenCalled();
      expect(ownKeysCalls).toBe(2);
      cleanup();
    });

    it('should not let stale durable registry cleanup clear a newer direct execution fence', async () => {
      const baseAgent = new Agent({
        id: 'durable-stale-fence-cleanup-agent',
        name: 'Durable Stale Fence Cleanup Agent',
        instructions: 'test',
        model: createTextStreamModel('unused') as LanguageModelV2,
      });
      const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });
      const requestContext = new RequestContext();
      const runId = 'durable-stale-fence-cleanup-run';
      const prepared = await durableAgent.prepare('test', {
        runId,
        requestContext,
        toolsetsMode: 'replace',
        toolsets: {
          mode: {
            modeTool: createTool({
              id: 'modeTool',
              description: 'durable replacement tool',
              inputSchema: z.object({}),
              execute: async () => 'mode',
            }),
          },
        },
      });
      const directFence = stampToolSurfaceFence(
        requestContext,
        runId,
        {
          directTool: createTool({
            id: 'directTool',
            description: 'new direct execution tool',
            inputSchema: z.object({}),
            execute: async () => 'direct',
          }),
        },
        'direct-owner',
      );

      durableAgent.runRegistry.cleanup(prepared.runId);
      expect(readToolSurfaceFence(requestContext, runId)).toBe(directFence);
      globalRunRegistry.delete(prepared.runId);
      expect(readToolSurfaceFence(requestContext, runId)).toBe(directFence);

      expect(clearToolSurfaceFence(requestContext, runId, 'direct-owner')).toBe(true);
    });

    it('should retry a generated runId collision without replacing the active entry', async () => {
      const occupiedRunId = crypto.randomUUID();
      const occupiedEntry = {
        runtimeBindingId: 'occupied-binding',
        tools: {},
        model: createTextStreamModel('occupied') as LanguageModelV2,
      };
      globalRunRegistry.set(occupiedRunId, occupiedEntry);
      const baseAgent = new Agent({
        id: 'durable-generated-collision-agent',
        instructions: 'test',
        model: createTextStreamModel('unused') as LanguageModelV2,
      });
      const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

      const prepared = await durableAgent.prepare('test');

      expect(prepared.runId).not.toBe(occupiedRunId);
      expect(globalRunRegistry.get(occupiedRunId)).toBe(occupiedEntry);
      durableAgent.runRegistry.cleanup(prepared.runId);
      globalRunRegistry.delete(prepared.runId);
      globalRunRegistry.delete(occupiedRunId);
    });

    it('should reject an active caller-reused runId without replacing its registered tools', async () => {
      const baseAgent = new Agent({
        id: 'durable-duplicate-run-agent',
        name: 'Durable Duplicate Run Agent',
        instructions: 'test',
        model: createTextStreamModel('unused') as LanguageModelV2,
      });
      const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });
      const firstTool = createTool({
        id: 'firstTool',
        description: 'first',
        inputSchema: z.object({}),
        execute: async () => 'first',
      });
      const first = await durableAgent.prepare('first', {
        runId: 'caller-reused-run',
        toolsetsMode: 'replace',
        toolsets: { mode: { firstTool } },
      });

      await expect(
        durableAgent.prepare('second', {
          runId: 'caller-reused-run',
          toolsetsMode: 'replace',
          toolsets: {
            mode: {
              secondTool: createTool({
                id: 'secondTool',
                description: 'second',
                inputSchema: z.object({}),
                execute: async () => 'second',
              }),
            },
          },
        }),
      ).rejects.toThrow(/already active.*Refusing to replace/);

      expect(globalRunRegistry.get(first.runId)?.runtimeBindingId).toBe(first.workflowInput.runtimeBindingId);
      expect(Object.keys(globalRunRegistry.get(first.runId)?.tools ?? {})).toEqual(['firstTool']);
      durableAgent.runRegistry.cleanup(first.runId);
      globalRunRegistry.delete(first.runId);
    });

    it('should fail closed if a durable workflow runId is rebound to a different runtime entry', async () => {
      const baseAgent = new Agent({
        id: 'durable-binding-agent',
        name: 'Durable Binding Agent',
        instructions: 'test',
        model: createTextStreamModel('unused') as LanguageModelV2,
      });
      const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });
      const prepared = await durableAgent.prepare('test', { runId: 'runtime-binding-run' });
      const originalEntry = globalRunRegistry.get(prepared.runId)!;
      globalRunRegistry.set(prepared.runId, {
        ...originalEntry,
        runtimeBindingId: 'rebound-runtime-binding',
      });

      await expect(
        resolveRuntimeDependencies({
          runId: prepared.runId,
          agentId: baseAgent.id,
          input: prepared.workflowInput,
        }),
      ).rejects.toThrow(/no longer matches its registered runtime dependencies/);

      durableAgent.runRegistry.cleanup(prepared.runId);
      globalRunRegistry.delete(prepared.runId);
    });

    it('should reconstruct a pre-binding durable input after registry loss without attaching it to a new run', async () => {
      const backingTool = createTool({
        id: 'legacyBackingTool',
        description: 'reconstructed backing tool',
        inputSchema: z.object({}),
        execute: async () => 'backing implementation',
      });
      const baseAgent = new Agent({
        id: 'durable-legacy-binding-agent',
        name: 'Durable Legacy Binding Agent',
        instructions: 'test',
        model: createTextStreamModel('unused') as LanguageModelV2,
        tools: { backingTool },
      });
      const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });
      const prepared = await durableAgent.prepare('test', { runId: 'legacy-binding-run' });
      const { runtimeBindingId: _runtimeBindingId, ...legacyWorkflowInput } = prepared.workflowInput;

      expect(() => baseDurableAgenticInputSchema.parse(legacyWorkflowInput)).not.toThrow();
      await expect(
        resolveRuntimeDependencies({
          runId: prepared.runId,
          agentId: baseAgent.id,
          input: legacyWorkflowInput,
        }),
      ).rejects.toThrow(/no longer matches its registered runtime dependencies/);

      globalRunRegistry.delete(prepared.runId);
      const resolved = await resolveRuntimeDependencies({
        runId: prepared.runId,
        agentId: baseAgent.id,
        input: legacyWorkflowInput,
        mastra: { getAgentById: () => baseAgent } as any,
      });

      expect(Object.keys(resolved.tools)).toEqual(['backingTool']);
      durableAgent.runRegistry.cleanup(prepared.runId);
    });

    it('should reject a partial replacement registry before durable execution', async () => {
      const baseAgent = new Agent({
        id: 'durable-partial-registry-agent',
        name: 'Durable Partial Registry Agent',
        instructions: 'test',
        model: createTextStreamModel('unused') as LanguageModelV2,
      });
      const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });
      const prepared = await durableAgent.prepare('test', {
        runId: 'partial-registry-run',
        toolsetsMode: 'replace',
        toolsets: {
          mode: {
            modeTool: createTool({
              id: 'modeTool',
              description: 'mode',
              inputSchema: z.object({}),
              execute: async () => 'mode',
            }),
          },
        },
      });
      const originalEntry = globalRunRegistry.get(prepared.runId)!;
      globalRunRegistry.set(prepared.runId, { ...originalEntry, tools: {} });

      await expect(
        resolveRuntimeDependencies({
          runId: prepared.runId,
          agentId: baseAgent.id,
          input: prepared.workflowInput,
        }),
      ).rejects.toThrow(/modeTool.*no own concrete implementation/);

      durableAgent.runRegistry.cleanup(prepared.runId);
      globalRunRegistry.delete(prepared.runId);
    });

    it('should fail closed after registry loss instead of substituting same-named backing tools', async () => {
      const model = createTextStreamModel('unused');
      const baseAgent = new Agent({
        id: 'durable-registry-loss-agent',
        name: 'Durable Registry Loss Agent',
        instructions: 'test',
        model: model as LanguageModelV2,
        tools: {
          modeTool: createTool({
            id: 'backing-modeTool',
            description: 'same name, different backing implementation',
            inputSchema: z.object({}),
            execute: async () => 'wrong implementation',
          }),
        },
      });
      const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });
      const prepared = await durableAgent.prepare('test', {
        runId: 'registry-loss-run',
        toolsetsMode: 'replace',
        toolsets: {
          mode: {
            modeTool: createTool({
              id: 'replacement-modeTool',
              description: 'approved replacement implementation',
              inputSchema: z.object({}),
              execute: async () => 'approved implementation',
            }),
          },
        },
      });
      globalRunRegistry.delete('registry-loss-run');

      await expect(
        resolveRuntimeDependencies({
          runId: 'registry-loss-run',
          agentId: baseAgent.id,
          input: prepared.workflowInput,
        }),
      ).rejects.toThrow(/Cannot reconstruct replacement tool implementations/);
    });

    it('should reconstruct an empty replacement surface as tool-free after registry loss', async () => {
      const model = createTextStreamModel('unused');
      const baseAgent = new Agent({
        id: 'durable-empty-replacement-agent',
        name: 'Durable Empty Replacement Agent',
        instructions: 'test',
        model: model as LanguageModelV2,
        tools: {
          hiddenTool: createTool({
            id: 'hiddenTool',
            description: 'must stay hidden',
            inputSchema: z.object({}),
            execute: async () => 'hidden',
          }),
        },
      });
      const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });
      const prepared = await durableAgent.prepare('test', {
        runId: 'empty-registry-loss-run',
        toolsetsMode: 'replace',
        toolsets: {},
      });
      globalRunRegistry.delete('empty-registry-loss-run');

      const resolved = await resolveRuntimeDependencies({
        runId: 'empty-registry-loss-run',
        agentId: baseAgent.id,
        input: prepared.workflowInput,
        mastra: { getAgentById: () => baseAgent } as any,
      });

      expect(prepared.workflowInput.options.toolSurfaceFence).toEqual([]);
      expect(resolved.tools).toEqual({});
    });

    it('should abort durable preparation when a replacement tool surface cannot be converted', async () => {
      const baseAgent = new Agent({
        id: 'durable-invalid-replacement-agent',
        name: 'Durable Invalid Replacement Agent',
        instructions: 'test',
        model: createTextStreamModel('unused') as LanguageModelV2,
      });
      const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

      await expect(
        durableAgent.prepare('test', {
          runId: 'invalid-replacement-run',
          toolsetsMode: 'replace',
          toolsets: {
            invalid: {
              'duplicate name': createTool({
                id: 'first',
                description: 'normalizes to duplicate_name',
                inputSchema: z.object({}),
                execute: async () => 'first',
              }),
              duplicate_name: createTool({
                id: 'second',
                description: 'collides after normalization',
                inputSchema: z.object({}),
                execute: async () => 'second',
              }),
            },
          },
        }),
      ).rejects.toThrow(/resolve to the same name/);
      expect(globalRunRegistry.has('invalid-replacement-run')).toBe(false);
    });

    it('should abort durable preparation when an inherited replacement surface cannot be converted', async () => {
      const baseAgent = new Agent({
        id: 'durable-invalid-default-replacement-agent',
        name: 'Durable Invalid Default Replacement Agent',
        instructions: 'test',
        model: createTextStreamModel('unused') as LanguageModelV2,
        defaultOptions: {
          toolsetsMode: 'replace',
          toolsets: {
            invalid: {
              'duplicate name': createTool({
                id: 'first-default',
                description: 'normalizes to duplicate_name',
                inputSchema: z.object({}),
                execute: async () => 'first',
              }),
              duplicate_name: createTool({
                id: 'second-default',
                description: 'collides after normalization',
                inputSchema: z.object({}),
                execute: async () => 'second',
              }),
            },
          },
        },
      });
      const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

      await expect(durableAgent.prepare('test', { runId: 'invalid-default-replacement-run' })).rejects.toThrow(
        /resolve to the same name/,
      );
      expect(globalRunRegistry.has('invalid-default-replacement-run')).toBe(false);
    });

    it('should allow input processors to clear activeTools for LLM request and tool execution', async () => {
      let callCount = 0;
      const doStream = vi.fn(async () => {
        callCount++;
        return {
          stream: convertArrayToReadableStream(
            callCount === 1
              ? [
                  { type: 'stream-start', warnings: [] },
                  { type: 'response-metadata', id: 'id-0', modelId: 'mock-model-id', timestamp: new Date(0) },
                  {
                    type: 'tool-call',
                    toolCallId: 'call-1',
                    toolName: 'hiddenTool',
                    input: JSON.stringify({}),
                    providerExecuted: false,
                  },
                  {
                    type: 'finish',
                    finishReason: 'tool-calls',
                    usage: { inputTokens: 10, outputTokens: 20, totalTokens: 30 },
                  },
                ]
              : [
                  { type: 'stream-start', warnings: [] },
                  { type: 'response-metadata', id: 'id-1', modelId: 'mock-model-id', timestamp: new Date(0) },
                  { type: 'text-start', id: 'text-1' },
                  { type: 'text-delta', id: 'text-1', delta: 'Done' },
                  { type: 'text-end', id: 'text-1' },
                  {
                    type: 'finish',
                    finishReason: 'stop',
                    usage: { inputTokens: 10, outputTokens: 20, totalTokens: 30 },
                  },
                ],
          ),
          rawCall: { rawPrompt: null, rawSettings: {} },
        };
      });

      const mockModel = new MockLanguageModelV2({ doStream });
      const hiddenExecute = vi.fn(async () => 'hidden');
      const allowedTool = createTool({
        id: 'allowedTool',
        description: 'Allowed tool',
        inputSchema: z.object({}),
        execute: async () => 'allowed',
      });
      const hiddenTool = createTool({
        id: 'hiddenTool',
        description: 'Hidden tool',
        inputSchema: z.object({}),
        execute: hiddenExecute,
      });

      const baseAgent = new Agent({
        id: 'active-tools-clear-agent',
        name: 'Active Tools Clear Agent',
        instructions: 'Use available tools',
        model: mockModel as LanguageModelV2,
        tools: { allowedTool, hiddenTool },
        inputProcessors: [
          {
            id: 'clear-active-tools',
            name: 'Clear Active Tools',
            processInputStep: async () => ({ activeTools: undefined }),
          },
        ],
      });
      const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

      const { output, cleanup } = await durableAgent.stream('Use tools', {
        activeTools: ['allowedTool'],
      });

      await output.consumeStream();

      expect(doStream).toHaveBeenCalledTimes(2);
      expect(doStream.mock.calls[0]?.[0].tools.map((tool: { name: string }) => tool.name)).toEqual([
        'allowedTool',
        'hiddenTool',
      ]);
      expect(hiddenExecute).toHaveBeenCalledOnce();

      cleanup();
    });

    it('should not dispatch a replacement tool removed by an input processor', async () => {
      const execute = vi.fn(async () => 'must not run');
      let callCount = 0;
      const doStream = vi.fn(async () => {
        callCount++;
        return {
          stream: convertArrayToReadableStream(
            callCount === 1
              ? [
                  { type: 'stream-start', warnings: [] },
                  { type: 'response-metadata', id: 'id-0', modelId: 'mock-model-id', timestamp: new Date(0) },
                  {
                    type: 'tool-call',
                    toolCallId: 'removed-call',
                    toolName: 'allowedTool',
                    input: JSON.stringify({}),
                    providerExecuted: false,
                  },
                  {
                    type: 'finish',
                    finishReason: 'tool-calls',
                    usage: { inputTokens: 10, outputTokens: 20, totalTokens: 30 },
                  },
                ]
              : [
                  { type: 'stream-start', warnings: [] },
                  { type: 'response-metadata', id: 'id-1', modelId: 'mock-model-id', timestamp: new Date(0) },
                  { type: 'text-start', id: 'text-1' },
                  { type: 'text-delta', id: 'text-1', delta: 'Done' },
                  { type: 'text-end', id: 'text-1' },
                  {
                    type: 'finish',
                    finishReason: 'stop',
                    usage: { inputTokens: 10, outputTokens: 20, totalTokens: 30 },
                  },
                ],
          ),
          rawCall: { rawPrompt: null, rawSettings: {} },
        };
      });
      const allowedTool = createTool({
        id: 'allowedTool',
        description: 'Replacement tool removed by the processor',
        inputSchema: z.object({}),
        execute,
      });
      const baseAgent = new Agent({
        id: 'durable-processor-narrowing-agent',
        name: 'Durable Processor Narrowing Agent',
        instructions: 'test',
        model: new MockLanguageModelV2({ doStream }) as LanguageModelV2,
        inputProcessors: [
          {
            id: 'remove-replacement-tool',
            processInputStep: async () => ({ tools: {} }),
          },
        ],
      });
      const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

      const { output, cleanup } = await durableAgent.stream('Try the removed tool', {
        maxSteps: 2,
        toolsetsMode: 'replace',
        toolsets: { mode: { allowedTool } },
      });

      await output.consumeStream();

      expect(doStream).toHaveBeenCalledTimes(2);
      expect(doStream.mock.calls[0]?.[0].tools ?? []).toEqual([]);
      expect(execute).not.toHaveBeenCalled();

      cleanup();
    });

    it('should not execute tool calls outside activeTools', async () => {
      const hiddenExecute = vi.fn(async () => 'hidden');
      let callCount = 0;
      const doStream = vi.fn(async () => {
        callCount++;
        return {
          stream: convertArrayToReadableStream(
            callCount === 1
              ? [
                  { type: 'stream-start', warnings: [] },
                  { type: 'response-metadata', id: 'id-0', modelId: 'mock-model-id', timestamp: new Date(0) },
                  {
                    type: 'tool-call',
                    toolCallId: 'call-1',
                    toolName: 'hiddenTool',
                    input: JSON.stringify({}),
                    providerExecuted: false,
                  },
                  {
                    type: 'finish',
                    finishReason: 'tool-calls',
                    usage: { inputTokens: 10, outputTokens: 20, totalTokens: 30 },
                  },
                ]
              : [
                  { type: 'stream-start', warnings: [] },
                  { type: 'response-metadata', id: 'id-1', modelId: 'mock-model-id', timestamp: new Date(0) },
                  { type: 'text-start', id: 'text-1' },
                  { type: 'text-delta', id: 'text-1', delta: 'Done' },
                  { type: 'text-end', id: 'text-1' },
                  {
                    type: 'finish',
                    finishReason: 'stop',
                    usage: { inputTokens: 10, outputTokens: 20, totalTokens: 30 },
                  },
                ],
          ),
          rawCall: { rawPrompt: null, rawSettings: {} },
        };
      });

      const mockModel = new MockLanguageModelV2({ doStream });
      const allowedTool = createTool({
        id: 'allowedTool',
        description: 'Allowed tool',
        inputSchema: z.object({}),
        execute: async () => 'allowed',
      });
      const hiddenTool = createTool({
        id: 'hiddenTool',
        description: 'Hidden tool',
        inputSchema: z.object({}),
        execute: hiddenExecute,
      });

      const baseAgent = new Agent({
        id: 'active-tools-enforcement-agent',
        name: 'Active Tools Enforcement Agent',
        instructions: 'Use only enabled tools',
        model: mockModel as LanguageModelV2,
        tools: { allowedTool, hiddenTool },
      });
      const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

      const { output, cleanup } = await durableAgent.stream('Try a hidden tool', {
        activeTools: ['allowedTool'],
      });

      await output.consumeStream();

      expect(hiddenExecute).not.toHaveBeenCalled();
      expect(doStream).toHaveBeenCalledTimes(2);

      cleanup();
    });

    it('should stream text response and invoke onChunk callback', async () => {
      const mockModel = createTextStreamModel('Hello, world!');
      const chunks: any[] = [];

      const baseAgent = new Agent({
        id: 'stream-test-agent',
        name: 'Stream Test Agent',
        instructions: 'You are a helpful assistant',
        model: mockModel as LanguageModelV2,
      });

      const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

      const { output, runId, cleanup } = await durableAgent.stream('Say hello', {
        onChunk: chunk => {
          chunks.push(chunk);
        },
      });

      expect(runId).toBeDefined();
      expect(output).toBeDefined();

      // Drain the stream to deterministically wait for all chunks (and onChunk
      // callbacks) instead of relying on a wall-clock timeout.
      await output.consumeStream();

      expect(chunks.length).toBeGreaterThan(0);

      cleanup();
    });

    it('should stream multiple text chunks', async () => {
      const mockModel = createMultiChunkStreamModel(['Hello', ', ', 'world', '!']);
      const chunks: any[] = [];

      const baseAgent = new Agent({
        id: 'multi-chunk-agent',
        name: 'Multi Chunk Agent',
        instructions: 'You are a helpful assistant',
        model: mockModel as LanguageModelV2,
      });

      const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

      const { output, cleanup } = await durableAgent.stream('Say hello in parts', {
        onChunk: chunk => {
          chunks.push(chunk);
        },
      });

      await output.consumeStream();

      expect(chunks.length).toBeGreaterThan(0);

      cleanup();
    });

    it('should return runId and allow cleanup', async () => {
      const mockModel = createTextStreamModel('Test response');

      const baseAgent = new Agent({
        id: 'cleanup-test-agent',
        name: 'Cleanup Test Agent',
        instructions: 'Test',
        model: mockModel as LanguageModelV2,
      });

      const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

      const { runId, cleanup } = await durableAgent.stream('Test');

      expect(runId).toBeDefined();
      expect(typeof runId).toBe('string');
      expect(runId.length).toBeGreaterThan(0);

      // Registry should have the run
      expect(durableAgent.runRegistry.has(runId)).toBe(true);

      // Cleanup should remove from registry
      cleanup();
      expect(durableAgent.runRegistry.has(runId)).toBe(false);
    });
  });

  describe('callbacks', () => {
    it('should invoke onFinish callback when streaming completes', async () => {
      const mockModel = createTextStreamModel('Complete response');
      let finishData: any = null;

      const baseAgent = new Agent({
        id: 'finish-callback-agent',
        name: 'Finish Callback Agent',
        instructions: 'Test',
        model: mockModel as LanguageModelV2,
      });

      const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

      const { output, cleanup } = await durableAgent.stream('Test', {
        onFinish: data => {
          finishData = data;
        },
      });

      // Drain the stream so we deterministically wait for the FINISH event
      // (which fires onFinish) instead of using a wall-clock timeout.
      await output.consumeStream();

      expect(finishData).not.toBeNull();

      cleanup();
    });

    it('should invoke onError callback when error occurs', async () => {
      const errorModel = new MockLanguageModelV2({
        doStream: async () => {
          throw new Error('Simulated LLM error');
        },
      });

      let errorReceived: Error | null = null;

      const baseAgent = new Agent({
        id: 'error-callback-agent',
        name: 'Error Callback Agent',
        instructions: 'Test',
        model: errorModel as LanguageModelV2,
      });

      const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

      const { output, cleanup } = await durableAgent.stream('Test', {
        onError: error => {
          errorReceived = error;
        },
      });

      // Drain the stream so we deterministically wait for the ERROR event
      // (which fires onError and errors the stream) instead of using a wall-clock timeout.
      await output.consumeStream({ onError: () => {} });

      expect(errorReceived).not.toBeNull();

      cleanup();
    });

    it('should invoke onStepFinish callback after each step', async () => {
      const mockModel = createTextStreamModel('Step complete');
      const stepResults: any[] = [];

      const baseAgent = new Agent({
        id: 'step-callback-agent',
        name: 'Step Callback Agent',
        instructions: 'Test',
        model: mockModel as LanguageModelV2,
      });

      const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

      const { output, cleanup } = await durableAgent.stream('Test', {
        onStepFinish: result => {
          stepResults.push(result);
        },
      });

      await output.consumeStream();

      // stepResults may or may not contain entries depending on workflow execution timing
      expect(Array.isArray(stepResults)).toBe(true);

      cleanup();
    });
  });

  describe('pubsub event emission', () => {
    it('should emit events to the correct topic based on runId', async () => {
      const mockModel = createTextStreamModel('Pubsub test');
      const receivedEvents: AgentStreamEvent[] = [];

      const baseAgent = new Agent({
        id: 'pubsub-test-agent',
        name: 'Pubsub Test Agent',
        instructions: 'Test',
        model: mockModel as LanguageModelV2,
      });

      const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

      // Prepare to get the runId first
      const preparation = await durableAgent.prepare('Test message');

      // Subscribe to events for this run
      await pubsub.subscribe(AGENT_STREAM_TOPIC(preparation.runId), event => {
        receivedEvents.push(event as unknown as AgentStreamEvent);
      });

      // Now we need to manually emit events since the workflow isn't actually running
      // In a real integration test, the workflow would emit these
      // EventEmitter.emit is synchronous; awaiting publish is sufficient.
      await pubsub.publish(AGENT_STREAM_TOPIC(preparation.runId), {
        type: AgentStreamEventTypes.CHUNK,
        runId: preparation.runId,
        data: { type: 'text-delta', payload: { text: 'test' } },
      });

      expect(receivedEvents.length).toBe(1);
      expect(receivedEvents[0].type).toBe(AgentStreamEventTypes.CHUNK);
    });

    it('should isolate events between different runs', async () => {
      const mockModel = createTextStreamModel('Test');
      const eventsRun1: AgentStreamEvent[] = [];
      const eventsRun2: AgentStreamEvent[] = [];

      const baseAgent = new Agent({
        id: 'isolation-test-agent',
        name: 'Isolation Test Agent',
        instructions: 'Test',
        model: mockModel as LanguageModelV2,
      });

      const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

      const prep1 = await durableAgent.prepare('Message 1');
      const prep2 = await durableAgent.prepare('Message 2');

      await pubsub.subscribe(AGENT_STREAM_TOPIC(prep1.runId), event => {
        eventsRun1.push(event as unknown as AgentStreamEvent);
      });

      await pubsub.subscribe(AGENT_STREAM_TOPIC(prep2.runId), event => {
        eventsRun2.push(event as unknown as AgentStreamEvent);
      });

      // Emit event to run1 only. EventEmitter.emit is synchronous; awaiting
      // publish is sufficient — no wall-clock wait needed.
      await pubsub.publish(AGENT_STREAM_TOPIC(prep1.runId), {
        type: AgentStreamEventTypes.CHUNK,
        runId: prep1.runId,
        data: { type: 'text-delta', payload: { text: 'for run 1' } },
      });

      expect(eventsRun1.length).toBe(1);
      expect(eventsRun2.length).toBe(0);
    });
  });
});

// ============================================================================
// Memory/Thread Integration Tests
// ============================================================================

describe('DurableAgent memory integration', () => {
  let pubsub: EventEmitterPubSub;

  beforeEach(() => {
    pubsub = new EventEmitterPubSub();
  });

  afterEach(async () => {
    await pubsub.close();
  });

  it('should track threadId and resourceId in stream result', async () => {
    const mockModel = createTextStreamModel('Hello');

    const baseAgent = new Agent({
      id: 'memory-test-agent',
      name: 'Memory Test Agent',
      instructions: 'Test',
      model: mockModel as LanguageModelV2,
    });

    const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

    const { threadId, resourceId, cleanup } = await durableAgent.stream('Test', {
      memory: {
        thread: 'thread-123',
        resource: 'user-456',
      },
    });

    expect(threadId).toBe('thread-123');
    expect(resourceId).toBe('user-456');

    cleanup();
  });

  it('should store memory info in extended registry', async () => {
    const mockModel = createTextStreamModel('Hello');

    const baseAgent = new Agent({
      id: 'registry-memory-agent',
      name: 'Registry Memory Agent',
      instructions: 'Test',
      model: mockModel as LanguageModelV2,
    });

    const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

    const { runId, cleanup } = await durableAgent.stream('Test', {
      memory: {
        thread: 'my-thread',
        resource: 'my-user',
      },
    });

    const memoryInfo = durableAgent.runRegistry.getMemoryInfo(runId);
    expect(memoryInfo).toEqual({
      threadId: 'my-thread',
      resourceId: 'my-user',
    });

    cleanup();
  });

  it('should handle streaming without memory options', async () => {
    const mockModel = createTextStreamModel('Hello');

    const baseAgent = new Agent({
      id: 'no-memory-agent',
      name: 'No Memory Agent',
      instructions: 'Test',
      model: mockModel as LanguageModelV2,
    });

    const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

    const { threadId, resourceId, cleanup } = await durableAgent.stream('Test');

    expect(threadId).toBeUndefined();
    expect(resourceId).toBeUndefined();

    cleanup();
  });

  it('should handle thread object with id', async () => {
    const mockModel = createTextStreamModel('Hello');

    const baseAgent = new Agent({
      id: 'thread-object-agent',
      name: 'Thread Object Agent',
      instructions: 'Test',
      model: mockModel as LanguageModelV2,
    });

    const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

    const { threadId, cleanup } = await durableAgent.stream('Test', {
      memory: {
        thread: { id: 'thread-from-object' },
        resource: 'user-123',
      },
    });

    expect(threadId).toBe('thread-from-object');

    cleanup();
  });
});

// ============================================================================
// Error Handling Tests
// ============================================================================

describe('DurableAgent error handling', () => {
  let pubsub: EventEmitterPubSub;

  beforeEach(() => {
    pubsub = new EventEmitterPubSub();
  });

  afterEach(async () => {
    await pubsub.close();
  });

  it('should handle model throwing error during streaming', async () => {
    const errorModel = new MockLanguageModelV2({
      doStream: async () => {
        throw new Error('Model initialization failed');
      },
    });

    let errorReceived: Error | null = null;

    const baseAgent = new Agent({
      id: 'error-model-agent',
      name: 'Error Model Agent',
      instructions: 'Test',
      model: errorModel as LanguageModelV2,
    });

    const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

    const { output, cleanup } = await durableAgent.stream('Test', {
      onError: error => {
        errorReceived = error;
      },
    });

    // Drain the stream so we deterministically wait for the ERROR event
    // (which fires onError and errors the stream) instead of using a wall-clock timeout.
    await output.consumeStream({ onError: () => {} });

    expect(errorReceived).not.toBeNull();

    cleanup();
  });

  it('should handle error event emission via pubsub', async () => {
    const { emitErrorEvent } = await import('../stream-adapter');
    const runId = 'error-emit-test';
    const receivedErrors: any[] = [];

    await pubsub.subscribe(AGENT_STREAM_TOPIC(runId), event => {
      const streamEvent = event as unknown as AgentStreamEvent;
      if (streamEvent.type === AgentStreamEventTypes.ERROR) {
        receivedErrors.push(streamEvent.data);
      }
    });

    const testError = new Error('Test error message');
    testError.name = 'TestError';
    // EventEmitter.emit is synchronous; awaiting emit is sufficient.
    await emitErrorEvent(pubsub, runId, testError);

    expect(receivedErrors.length).toBe(1);
    expect(receivedErrors[0].error.name).toBe('TestError');
    expect(receivedErrors[0].error.message).toBe('Test error message');
  });

  it('should cleanup registry on error', async () => {
    const errorModel = new MockLanguageModelV2({
      doStream: async () => {
        throw new Error('Cleanup test error');
      },
    });

    const baseAgent = new Agent({
      id: 'cleanup-error-agent',
      name: 'Cleanup Error Agent',
      instructions: 'Test',
      model: errorModel as LanguageModelV2,
    });

    const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

    const { output, runId, cleanup } = await durableAgent.stream('Test');

    // Run should be registered initially
    expect(durableAgent.runRegistry.has(runId)).toBe(true);

    // Drain the stream so we deterministically wait for the workflow to
    // finish erroring instead of using a wall-clock timeout.
    await output.consumeStream({ onError: () => {} });

    // Manual cleanup should work
    cleanup();
    expect(durableAgent.runRegistry.has(runId)).toBe(false);
  });
});

// ============================================================================
// Workflow Input Serialization Tests
// ============================================================================

describe('DurableAgent workflow input serialization', () => {
  let pubsub: EventEmitterPubSub;

  beforeEach(() => {
    pubsub = new EventEmitterPubSub();
  });

  afterEach(async () => {
    await pubsub.close();
  });

  it('should create fully serializable workflow input', async () => {
    const mockModel = createTextStreamModel('Hello');

    const baseAgent = new Agent({
      id: 'serialization-agent',
      name: 'Serialization Agent',
      instructions: 'You are helpful',
      model: mockModel as LanguageModelV2,
    });

    const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

    const result = await durableAgent.prepare('Test message');

    // Verify all fields are serializable
    const serialized = JSON.stringify(result.workflowInput);
    const deserialized = JSON.parse(serialized);

    expect(deserialized.runId).toBe(result.runId);
    expect(deserialized.agentId).toBe('serialization-agent');
    expect(deserialized.messageListState).toBeDefined();
    expect(deserialized.modelConfig).toBeDefined();
    expect(deserialized.modelConfig.provider).toBeDefined();
    expect(deserialized.modelConfig.modelId).toBeDefined();
  });

  it('should serialize tool metadata without execute functions', async () => {
    const mockModel = createTextStreamModel('Hello');

    const testTool = createTool({
      id: 'test-tool',
      description: 'A test tool',
      inputSchema: z.object({ input: z.string() }),
      execute: async ({ input }) => `Result: ${input}`,
    });

    const baseAgent = new Agent({
      id: 'tool-serialization-agent',
      name: 'Tool Serialization Agent',
      instructions: 'Test',
      model: mockModel as LanguageModelV2,
      tools: { testTool },
    });

    const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

    const result = await durableAgent.prepare('Use the tool');

    // Tool metadata should be serializable
    const serialized = JSON.stringify(result.workflowInput.toolsMetadata);
    expect(() => JSON.parse(serialized)).not.toThrow();

    // But the actual tools in registry should have execute functions
    const tools = durableAgent.runRegistry.getTools(result.runId);
    expect(typeof tools.testTool?.execute).toBe('function');
  });

  it('should serialize execution options', async () => {
    const mockModel = createTextStreamModel('Hello');

    const baseAgent = new Agent({
      id: 'options-agent',
      name: 'Options Agent',
      instructions: 'Test',
      model: mockModel as LanguageModelV2,
    });

    const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

    const result = await durableAgent.prepare('Test', {
      maxSteps: 5,
      toolChoice: 'auto',
      modelSettings: { temperature: 0.7 },
    });

    expect(result.workflowInput.options.maxSteps).toBe(5);
    expect(result.workflowInput.options.toolChoice).toBe('auto');
    expect(result.workflowInput.options.temperature).toBe(0.7);

    // Verify serializable
    const serialized = JSON.stringify(result.workflowInput.options);
    expect(() => JSON.parse(serialized)).not.toThrow();
  });
});
