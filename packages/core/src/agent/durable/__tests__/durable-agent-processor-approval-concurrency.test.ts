/**
 * DurableAgent + processor-injected approval tools (issue #24377).
 *
 * The foreach concurrency gate only inspects the tool metadata serialized at
 * run start (`toolsMetadata` from the agent's static `tools`). A tool that an
 * input processor adds per step — `ToolSearchProcessor` in the report, a plain
 * injector here — is executable but invisible to that gate, so two calls to an
 * approval-gated tool could suspend in parallel inside one foreach. That state
 * could not be drained: the second approval hung forever.
 *
 * These tests pin the observable contract: one approval at a time, the sibling
 * runs only after the first is approved, and the run reaches a terminal result.
 *
 * The focused resolver tests pin the concurrency decision directly. These
 * end-to-end variants additionally cover both durable engines so the evented
 * engine cannot release an unapproved sibling while the default engine drains
 * the approvals sequentially to a terminal result.
 */

import type { LanguageModelV2 } from '@ai-sdk/provider-v5';
import { MockLanguageModelV2, convertArrayToReadableStream } from '@internal/ai-sdk-v5/test';
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { z } from 'zod';
import { EventEmitterPubSub } from '../../../events/event-emitter';
import { Mastra } from '../../../mastra';
import { MockMemory } from '../../../memory/mock';
import { InMemoryStore } from '../../../storage/mock';
import { createTool } from '../../../tools';
import { Agent } from '../../agent';
import { createDurableAgent } from '../create-durable-agent';
import { createEventedAgent } from '../create-evented-agent';
import { globalRunRegistry } from '../run-registry';

/** Input processor that adds one approval-gated tool to every step's tool set. */
function createApprovalToolInjector(tool: unknown) {
  return {
    id: 'approval-tool-injector',
    name: 'approval-tool-injector',
    processInputStep: async ({ tools }: { tools?: Record<string, unknown> }) => ({
      tools: { ...(tools ?? {}), approvalEcho: tool },
    }),
  } as any;
}

/**
 * Emits two same-turn calls to `approvalEcho` until a result is in context, then
 * finishes. Content-driven (never a call counter) so the durable engine's step
 * replay after a suspension re-derives the identical batch.
 */
function makeParallelCallModel() {
  return new MockLanguageModelV2({
    doStream: async ({ prompt }) => {
      const hasEchoResult = JSON.stringify(prompt).includes('echoed');
      if (hasEchoResult) {
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: convertArrayToReadableStream<any>([
            { type: 'stream-start', warnings: [] },
            { type: 'response-metadata', id: 'par-1', modelId: 'mock-model', timestamp: new Date(0) },
            { type: 'text-start', id: 'par-text' },
            { type: 'text-delta', id: 'par-text', delta: 'Done.' },
            { type: 'text-end', id: 'par-text' },
            { type: 'finish', finishReason: 'stop', usage: { inputTokens: 10, outputTokens: 5, totalTokens: 15 } },
          ]),
        };
      }
      return {
        rawCall: { rawPrompt: null, rawSettings: {} },
        warnings: [],
        stream: convertArrayToReadableStream<any>([
          { type: 'stream-start', warnings: [] },
          { type: 'response-metadata', id: 'par-0', modelId: 'mock-model', timestamp: new Date(0) },
          {
            type: 'tool-call',
            toolCallType: 'function',
            toolCallId: 'tc-alpha',
            toolName: 'approvalEcho',
            input: JSON.stringify({ message: 'Alpha' }),
            providerExecuted: false,
          },
          {
            type: 'tool-call',
            toolCallType: 'function',
            toolCallId: 'tc-beta',
            toolName: 'approvalEcho',
            input: JSON.stringify({ message: 'Beta' }),
            providerExecuted: false,
          },
          {
            type: 'finish',
            finishReason: 'tool-calls',
            usage: { inputTokens: 10, outputTokens: 20, totalTokens: 30 },
          },
        ]),
      };
    },
  });
}

describe('durable processor-injected approval tool concurrency', () => {
  let pubsub: EventEmitterPubSub;

  beforeEach(() => {
    pubsub = new EventEmitterPubSub();
  });

  afterEach(async () => {
    globalRunRegistry.clear();
    await pubsub.close();
  });

  function buildAgent(toolExecutions: { count: number }, mockMemory: MockMemory) {
    const approvalEcho = createTool({
      id: 'approvalEcho',
      description: 'Echoes a message after approval',
      inputSchema: z.object({ message: z.string() }),
      requireApproval: true,
      execute: async ({ message }) => {
        toolExecutions.count++;
        return { echoed: message };
      },
    });

    // `approvalEcho` is deliberately NOT in the agent's static tools: it only
    // reaches the model through the processor, which is the whole point.
    return new Agent({
      id: 'processorApprovalAgent',
      name: 'processorApprovalAgent',
      instructions: 'Call the echo tool twice in parallel.',
      model: makeParallelCallModel() as LanguageModelV2,
      inputProcessors: [createApprovalToolInjector(approvalEcho)],
      memory: mockMemory,
    });
  }

  /**
   * Collects chunks until `stopOn` is satisfied or `deadlineMs` elapses.
   *
   * A deadline is required because a suspended durable run keeps its stream
   * open: without it, waiting for a chunk that will never arrive (the bug this
   * test pins) would hang until the test timeout instead of failing fast.
   */
  const collectUntil = async (stream: AsyncIterable<any>, stopOn: (chunks: any[]) => boolean, deadlineMs: number) => {
    const chunks: any[] = [];
    const iterator = stream[Symbol.asyncIterator]();
    const startedAt = Date.now();
    while (Date.now() - startedAt < deadlineMs) {
      const remaining = deadlineMs - (Date.now() - startedAt);
      const next = await Promise.race([
        iterator.next(),
        new Promise<{ done: true; value?: undefined }>(resolve => setTimeout(() => resolve({ done: true }), remaining)),
      ]);
      if (next.done) break;
      chunks.push(next.value);
      if (stopOn(chunks)) break;
    }
    return chunks;
  };

  const approvalsIn = (chunks: any[]) => chunks.filter(c => c.type === 'tool-call-approval');

  it.each(['durable', 'evented'] as const)(
    '%s engine refuses a processor-injected approval tool (PF-2586 tool-surface fence)',
    async engine => {
      const storage = new InMemoryStore();
      const mockMemory = new MockMemory();
      const memory = {
        thread: `processor-approval-${engine}-thread`,
        resource: `processor-approval-${engine}-resource`,
      };
      const toolExecutions = { count: 0 };

      const baseAgent = buildAgent(toolExecutions, mockMemory);
      const durableAgent =
        engine === 'evented'
          ? createEventedAgent({ agent: baseAgent, pubsub })
          : createDurableAgent({ agent: baseAgent, pubsub });
      new Mastra({ agents: { durableAgent }, storage, logger: false });

      // Fork (PF-2586, kept in PF-5928): durable input processors cannot add executable tools
      // (reconstructible tool-surface fence), so the injected approval tool is refused before any
      // approval is raised or the tool runs; the run ends with the fence error instead.
      const first = await durableAgent.stream('Echo twice', { memory, maxSteps: 6 });
      const chunks = await collectUntil(first.fullStream, seen => seen.some(c => c.type === 'finish'), 15000);
      expect(
        chunks.filter(c => c.type === 'error').map(c => String(c.payload?.error?.message ?? c.payload?.error)),
      ).toEqual([expect.stringContaining('Durable input processors cannot add executable tool "approvalEcho"')]);
      expect(approvalsIn(chunks)).toHaveLength(0);
      expect(toolExecutions.count).toBe(0);
    },
    40000,
  );
});
