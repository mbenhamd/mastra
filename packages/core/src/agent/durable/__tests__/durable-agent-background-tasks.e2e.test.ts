/**
 * DurableAgent background tasks with memory (fork).
 *
 * Upstream replaced this file's recorded-gateway suite with cross-engine aimock
 * scenarios (#27531, `loop/test-utils/aimock/scenarios/background-task-*`) and
 * deleted its recordings. The fork kept one case on a deterministic model
 * (PF-1875 sync): cross-turn memory plus background-task delivery on a durable
 * agent, which it asserts independently of LLM-recording selection.
 */

import type { LanguageModelV2 } from '@ai-sdk/provider-v5';
import { MockLanguageModelV2, convertArrayToReadableStream } from '@internal/ai-sdk-v5/test';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { z } from 'zod';
import { Mastra } from '../../../mastra';
import { MockMemory } from '../../../memory/mock';
import { MockStore } from '../../../storage';
import { createTool } from '../../../tools';
import { Agent } from '../../agent';
import { createDurableAgent } from '../create-durable-agent';

function createMemoryBackgroundModel(): LanguageModelV2 {
  let call = 0;
  const responses = [
    [
      { type: 'stream-start' as const, warnings: [] },
      { type: 'response-metadata' as const, id: 'research-call', modelId: 'mock', timestamp: new Date(0) },
      {
        type: 'tool-call' as const,
        toolCallId: 'call-research',
        toolName: 'research',
        input: JSON.stringify({ topic: 'neural networks' }),
        providerExecuted: false,
      },
      {
        type: 'finish' as const,
        finishReason: 'tool-calls' as const,
        usage: { inputTokens: 10, outputTokens: 5, totalTokens: 15 },
      },
    ],
    [
      { type: 'stream-start' as const, warnings: [] },
      { type: 'response-metadata' as const, id: 'research-response', modelId: 'mock', timestamp: new Date(0) },
      { type: 'text-start' as const, id: 'research-text' },
      { type: 'text-delta' as const, id: 'research-text', delta: 'Research started.' },
      { type: 'text-end' as const, id: 'research-text' },
      {
        type: 'finish' as const,
        finishReason: 'stop' as const,
        usage: { inputTokens: 15, outputTokens: 5, totalTokens: 20 },
      },
    ],
    [
      { type: 'stream-start' as const, warnings: [] },
      { type: 'response-metadata' as const, id: 'greet-call', modelId: 'mock', timestamp: new Date(0) },
      {
        type: 'tool-call' as const,
        toolCallId: 'call-greet',
        toolName: 'greet',
        input: JSON.stringify({ name: 'Bob' }),
        providerExecuted: false,
      },
      {
        type: 'finish' as const,
        finishReason: 'tool-calls' as const,
        usage: { inputTokens: 20, outputTokens: 5, totalTokens: 25 },
      },
    ],
    [
      { type: 'stream-start' as const, warnings: [] },
      { type: 'response-metadata' as const, id: 'greet-response', modelId: 'mock', timestamp: new Date(0) },
      { type: 'text-start' as const, id: 'greet-text' },
      { type: 'text-delta' as const, id: 'greet-text', delta: 'Hello, Bob!' },
      { type: 'text-end' as const, id: 'greet-text' },
      {
        type: 'finish' as const,
        finishReason: 'stop' as const,
        usage: { inputTokens: 25, outputTokens: 5, totalTokens: 30 },
      },
    ],
  ];

  return new MockLanguageModelV2({
    doStream: async () => {
      const response = responses[call++];
      if (!response) throw new Error(`Unexpected memory background model call ${call}.`);
      return {
        rawCall: { rawPrompt: null, rawSettings: {} },
        warnings: [],
        stream: convertArrayToReadableStream(response),
      };
    },
  }) as LanguageModelV2;
}

let testStorage: any;
beforeEach(() => {
  testStorage = new MockStore();
});

afterEach(async () => {
  const backgroundTasksStore = await testStorage?.getStore('backgroundTasks');
  await backgroundTasksStore?.dangerouslyClearAll();
});

describe('DurableAgent Background Tasks with memory', () => {
  const researchTool = createTool({
    id: 'research',
    description: 'Research a topic. This takes a while, use it when the user asks to research something.',
    inputSchema: z.object({
      topic: z.string().describe('The topic to research'),
    }),
    outputSchema: z.object({
      summary: z.string(),
    }),
    execute: async ({ topic }) => {
      await new Promise(resolve => setTimeout(resolve, 500));
      return { summary: `Research complete on "${topic}": This is a comprehensive summary.` };
    },
    background: { enabled: true },
  });

  const greetTool = createTool({
    id: 'greet',
    description: 'Greet a person by name. Use this when the user asks to greet someone.',
    inputSchema: z.object({
      name: z.string().describe('The name to greet'),
    }),
    outputSchema: z.object({
      greeting: z.string(),
    }),
    execute: async ({ name }) => {
      return { greeting: `Hello, ${name}!` };
    },
  });

  it('background task works alongside memory — second prompt processes while bg task runs', async () => {
    const mockMemory = new MockMemory();
    const threadId = 'durable-bg-memory-test-thread';
    const resourceId = 'durable-bg-memory-test-user';

    const memoryBaseAgent = new Agent({
      id: 'durable-bg-memory-agent',
      name: 'Durable Background Memory Agent',
      instructions:
        'You are a helpful assistant. ' +
        'When asked to research something, use the research tool. ' +
        'When asked to greet someone, use the greet tool. ' +
        'Always respond concisely.',
      // This test exercises cross-turn memory and background-task delivery.
      // A deterministic model keeps those runtime assertions independent of
      // fuzzy LLM-recording selection as completion callbacks update memory.
      model: createMemoryBackgroundModel(),
      tools: { research: researchTool, greet: greetTool },
      memory: mockMemory,
      backgroundTasks: {
        tools: { research: true },
      },
    });

    const memoryDurableAgent = createDurableAgent({ agent: memoryBaseAgent });

    const memoryMastra = new Mastra({
      agents: { 'durable-bg-memory-agent': memoryDurableAgent as any },
      backgroundTasks: {
        enabled: true,
        globalConcurrency: 5,
        perAgentConcurrency: 3,
      },
      storage: testStorage,
    });
    await memoryMastra.startWorkers();

    try {
      const stream1 = await memoryDurableAgent.stream('Please research "neural networks" for me', {
        memory: { thread: threadId, resource: resourceId },
      });

      const chunks1: any[] = [];
      for await (const chunk of stream1.fullStream) {
        chunks1.push(chunk);
      }

      const bgStarted = chunks1.find(c => c.type === 'background-task-started');
      expect(bgStarted).toBeDefined();
      expect(bgStarted.payload.toolName).toBe('research');

      stream1.cleanup();

      const stream2 = await memoryDurableAgent.stream('Now greet someone named Bob', {
        memory: { thread: threadId, resource: resourceId },
      });

      const chunks2: any[] = [];
      for await (const chunk of stream2.fullStream) {
        chunks2.push(chunk);
      }

      const bgStarted2 = chunks2.find(c => c.type === 'background-task-started');
      expect(bgStarted2).toBeUndefined();

      const toolResult2 = chunks2.find(c => c.type === 'tool-result' && c.payload?.toolName === 'greet');
      expect(toolResult2).toBeDefined();

      const fullOutput2 = await stream2.output.getFullOutput();
      expect(fullOutput2.text!.toLowerCase()).toContain('bob');

      stream2.cleanup();

      await new Promise(resolve => setTimeout(resolve, 2000));

      const manager = memoryMastra.backgroundTaskManager!;
      const tasks = await manager.listTasks({ toolName: 'research', status: 'completed' });
      expect(tasks.total).toBeGreaterThan(0);
      expect((tasks.tasks[0]!.result as any).summary).toContain('neural networks');

      const { messages } = await mockMemory.recall({
        threadId,
        resourceId,
      });

      expect(messages.length).toBeGreaterThan(0);

      const userMessages = messages.filter((m: any) => m.role === 'user');
      expect(userMessages.length).toBeGreaterThanOrEqual(2);

      const assistantMessages = messages.filter((m: any) => m.role === 'assistant');
      expect(assistantMessages.length).toBeGreaterThanOrEqual(2);

      const allContent = messages
        .map((m: any) => {
          if (typeof m.content === 'string') return m.content;
          if (Array.isArray(m.content)) {
            return m.content.map((p: any) => p.text || p.result || JSON.stringify(p)).join(' ');
          }
          return JSON.stringify(m.content);
        })
        .join(' ')
        .toLowerCase();

      expect(allContent).toContain('neural networks');
      expect(allContent).toContain('bob');
    } finally {
      await memoryMastra.backgroundTaskManager?.shutdown();
      await memoryMastra.stopWorkers();
    }
  }, 60_000);
});
