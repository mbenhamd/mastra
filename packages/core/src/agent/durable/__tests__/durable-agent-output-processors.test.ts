/**
 * DurableAgent output processor tests for tool chunks.
 *
 * Verifies that tool-result and tool-error chunks are processed through
 * output processors (Bug 4 parity fix).
 */

import type { LanguageModelV2 } from '@ai-sdk/provider-v5';
import { MockLanguageModelV2, convertArrayToReadableStream } from '@internal/ai-sdk-v5/test';
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { z } from 'zod';
import { EventEmitterPubSub } from '../../../events/event-emitter';
import { Mastra } from '../../../mastra';
import { ProcessorRunner } from '../../../processors/runner';
import { InMemoryStore } from '../../../storage';
import { createTool } from '../../../tools';
import { Agent } from '../../agent';
import { createDurableAgent } from '../create-durable-agent';

function createToolCallingModel(toolName: string, toolArgs: Record<string, unknown>, toolCallCount = 1) {
  let callCount = 0;
  return new MockLanguageModelV2({
    doStream: async () => {
      callCount++;
      if (callCount === 1) {
        // First call: invoke the tool
        return {
          stream: convertArrayToReadableStream([
            { type: 'stream-start', warnings: [] },
            { type: 'response-metadata', id: 'resp-1', modelId: 'mock', timestamp: new Date(0) },
            ...Array.from({ length: toolCallCount }, (_, index) => ({
              type: 'tool-call' as const,
              id: `tc-${index + 1}`,
              toolCallType: 'function' as const,
              toolCallId: `tc-${index + 1}`,
              toolName,
              args: JSON.stringify(toolArgs),
            })),
            {
              type: 'finish',
              finishReason: 'tool-calls',
              usage: { inputTokens: 10, outputTokens: 20, totalTokens: 30 },
            },
          ]),
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
        };
      }
      // Second call: respond with text
      return {
        stream: convertArrayToReadableStream([
          { type: 'stream-start', warnings: [] },
          { type: 'response-metadata', id: 'resp-2', modelId: 'mock', timestamp: new Date(0) },
          { type: 'text-start', id: 'text-1' },
          { type: 'text-delta', id: 'text-1', delta: 'Done.' },
          { type: 'text-end', id: 'text-1' },
          {
            type: 'finish',
            finishReason: 'stop',
            usage: { inputTokens: 10, outputTokens: 5, totalTokens: 15 },
          },
        ]),
        rawCall: { rawPrompt: null, rawSettings: {} },
        warnings: [],
      };
    },
  });
}

async function drain(stream: ReadableStream<any>) {
  const out: any[] = [];
  for await (const c of stream) out.push(c);
  return out;
}

describe('DurableAgent output processors for tool chunks', () => {
  let pubsub: EventEmitterPubSub;

  beforeEach(() => {
    pubsub = new EventEmitterPubSub();
  });

  afterEach(async () => {
    await pubsub.close();
  });

  it('tool-result chunks are processed through output processors', async () => {
    const processedChunks: any[] = [];
    const toolChunkRunnerCalls = new Map<ProcessorRunner, number>();
    const originalProcessPart = ProcessorRunner.prototype.processPart;
    const processPart = vi.spyOn(ProcessorRunner.prototype, 'processPart').mockImplementation(function (part, ...args) {
      if (part.type === 'tool-result' || part.type === 'tool-error') {
        toolChunkRunnerCalls.set(this, (toolChunkRunnerCalls.get(this) ?? 0) + 1);
      }
      return originalProcessPart.call(this, part, ...args);
    });

    const outputProcessor = {
      id: 'test-redactor',
      name: 'Test Redactor',
      processOutputStream: vi.fn().mockImplementation(async ({ part }) => {
        if (part.type === 'tool-result') {
          processedChunks.push(part);
          // Modify the chunk — e.g. redact the result
          return {
            ...part,
            payload: {
              ...part.payload,
              result: '[REDACTED]',
            },
          };
        }
        return part;
      }),
    };

    const weatherTool = createTool({
      id: 'getWeather',
      description: 'Get weather',
      inputSchema: z.object({ city: z.string() }),
      outputSchema: z.object({ temp: z.number() }),
      execute: async () => ({ temp: 72 }),
    });

    const baseAgent = new Agent({
      id: 'processor-agent',
      name: 'Processor Agent',
      instructions: 'You are a helpful agent.',
      model: createToolCallingModel('getWeather', { city: 'NYC' }, 2) as LanguageModelV2,
      tools: { getWeather: weatherTool },
      outputProcessors: [outputProcessor as any],
    });

    const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });
    new Mastra({
      agents: { 'processor-agent': durableAgent as any },
      logger: false,
      storage: new InMemoryStore(),
      pubsub,
    });

    try {
      const result = await durableAgent.stream('What is the weather in NYC?', {
        maxSteps: 3,
      });

      const chunks = await drain(result.fullStream);

      // Both the mapping and durable emission runners should see each tool-result chunk.
      expect(processedChunks).toHaveLength(4);
      expect(processedChunks.every(chunk => chunk.type === 'tool-result')).toBe(true);

      // The emitted tool-result chunks should have the redacted result.
      const toolResultChunks = chunks.filter((c: any) => c.type === 'tool-result');
      expect(toolResultChunks).toHaveLength(2);
      expect(toolResultChunks.every((chunk: any) => chunk.payload.result === '[REDACTED]')).toBe(true);
      expect([...toolChunkRunnerCalls.values()].sort()).toEqual([2, 2]);
    } finally {
      processPart.mockRestore();
    }
  });

  it('output processor can block tool-result chunks with tripwire', async () => {
    const blockingProcessor = {
      id: 'test-blocker',
      name: 'Test Blocker',
      processOutputStream: vi.fn().mockImplementation(async ({ part }) => {
        if (part.type === 'tool-result') {
          return null; // Block the chunk
        }
        return part;
      }),
      tripwire: {
        reason: 'Content blocked by policy',
      },
    };

    const weatherTool = createTool({
      id: 'getWeather',
      description: 'Get weather',
      inputSchema: z.object({ city: z.string() }),
      outputSchema: z.object({ temp: z.number() }),
      execute: async () => ({ temp: 72 }),
    });

    const baseAgent = new Agent({
      id: 'blocker-agent',
      name: 'Blocker Agent',
      instructions: 'You are a helpful agent.',
      model: createToolCallingModel('getWeather', { city: 'NYC' }) as LanguageModelV2,
      tools: { getWeather: weatherTool },
      outputProcessors: [blockingProcessor as any],
    });

    const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });
    new Mastra({
      agents: { 'blocker-agent': durableAgent as any },
      logger: false,
      storage: new InMemoryStore(),
      pubsub,
    });

    const result = await durableAgent.stream('What is the weather in NYC?', {
      maxSteps: 3,
    });

    const chunks = await drain(result.fullStream);

    // The tool-result chunk should have been blocked (not emitted)
    const toolResultChunks = chunks.filter((c: any) => c.type === 'tool-result');
    expect(toolResultChunks).toHaveLength(0);
  });
});
