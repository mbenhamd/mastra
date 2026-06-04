/**
 * Harness v1 — END-TO-END TURN throughput benchmark (deterministic, no network).
 *
 * Drives a REAL `Agent` (over a deterministic `MockLanguageModelV2`) through a
 * real `Harness` + `InMemoryStore`, exercising the genuine
 *   ai-sdk → provider → transform → loop → fullStream → harness event mapping
 * path — the same builder the real-agent E2E test uses (session.real-agent.e2e.test.ts).
 *
 * This is a `*.bench.ts` file. The package vitest `test.include` glob is
 * `src/**\/*.test.ts`, so `pnpm test` / `vitest run` NEVER pick this up. It runs
 * ONLY under an explicit `vitest bench` invocation (separate `**\/*.bench.{js,ts}`
 * glob). All Tinybench options are bounded so a bench run cannot hang in CI.
 *
 * Run:
 *   pnpm --filter ./packages/core exec vitest bench src/harness/v1/turn-throughput.bench.ts --run
 */

import type { LanguageModelV2StreamPart } from '@ai-sdk/provider-v5';
import { afterAll, bench, describe } from 'vitest';
import { z } from 'zod';

import { convertArrayToReadableStream, MockLanguageModelV2 } from '../../agent/__tests__/mock-model';
import { Agent } from '../../agent';
import { InMemoryStore } from '../../storage';
import { createTool } from '../../tools';

import { Harness } from './harness';

const testUsage = { inputTokens: 10, outputTokens: 20, totalTokens: 30 };

/** A raw provider stream that emits text in N deltas then finishes with `stop`. */
function textStream(deltas: string[]) {
  return convertArrayToReadableStream<LanguageModelV2StreamPart>([
    { type: 'stream-start', warnings: [] },
    { type: 'response-metadata', id: 'id-text', modelId: 'mock-model-id', timestamp: new Date(0) },
    { type: 'text-start', id: 'text-1' },
    ...deltas.map(delta => ({ type: 'text-delta' as const, id: 'text-1', delta })),
    { type: 'text-end', id: 'text-1' },
    { type: 'finish', finishReason: 'stop', usage: testUsage },
  ]);
}

function toolCallStream(toolCallId: string, toolName: string, inputJson: string) {
  return convertArrayToReadableStream<LanguageModelV2StreamPart>([
    { type: 'stream-start', warnings: [] },
    { type: 'response-metadata', id: 'id-tool', modelId: 'mock-model-id', timestamp: new Date(0) },
    { type: 'tool-call', toolCallId, toolName, input: inputJson, providerExecuted: false },
    { type: 'finish', finishReason: 'tool-calls', usage: testUsage },
  ]);
}

function newHarness(agent: Agent<any, any, any>) {
  return new Harness({
    agents: { default: agent } as any,
    storage: new InMemoryStore(),
    modes: [{ id: 'default', agentId: 'default' }],
    defaultModeId: 'default',
  });
}

// Bounded Tinybench options — keep total wall time small so CI can never hang.
const BOUNDED = { time: 1500, iterations: 50, warmupIterations: 5, warmupTime: 250 } as const;

// ---------------------------------------------------------------------------
// Case 1 — plain text turn (3 deltas, no tools).
// ---------------------------------------------------------------------------
describe('Harness v1 turn throughput — text turn', () => {
  const model = new MockLanguageModelV2({
    doStream: async () => ({
      rawCall: { rawPrompt: null, rawSettings: {} },
      warnings: [],
      stream: textStream(['Hello', ', ', 'world!']),
    }),
  });
  const agent = new Agent({ id: 'default', name: 'default', instructions: 'reply', model });
  const harness = newHarness(agent);

  afterAll(async () => {
    await harness.shutdown();
  });

  bench(
    'sequential message() turn (3 text deltas, no tools)',
    async () => {
      const session = await harness.session({ resourceId: 'bench-text', threadId: { fresh: true } });
      await session.message({ content: 'hi' });
    },
    BOUNDED,
  );
});

// ---------------------------------------------------------------------------
// Case 2 — one tool round-trip (tool-call → tool execute → stop).
// ---------------------------------------------------------------------------
describe('Harness v1 turn throughput — tool round-trip', () => {
  const findUser = createTool({
    id: 'findUser',
    description: 'look up a user',
    inputSchema: z.object({ name: z.string() }),
    execute: async input => ({ name: (input as { name: string }).name, ok: true }),
  });

  let callCount = 0;
  const model = new MockLanguageModelV2({
    doStream: async () => {
      callCount++;
      if (callCount % 2 === 1) {
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: toolCallStream(`call-${callCount}`, 'findUser', '{"name":"Dero"}'),
        };
      }
      return {
        rawCall: { rawPrompt: null, rawSettings: {} },
        warnings: [],
        stream: textStream(['Found ', 'Dero']),
      };
    },
  });
  const agent = new Agent({ id: 'default', name: 'default', instructions: 'use findUser', model, tools: { findUser } });
  const harness = newHarness(agent);

  afterAll(async () => {
    await harness.shutdown();
  });

  bench(
    'turn with 1 tool round-trip (suspend-free 2-step loop)',
    async () => {
      const session = await harness.session({ resourceId: 'bench-tool', threadId: { fresh: true } });
      await session.message({ content: 'find Dero' });
    },
    BOUNDED,
  );
});
