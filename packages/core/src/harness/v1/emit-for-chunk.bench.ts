/**
 * Harness v1 — EVENT DISPATCH benchmark (`Session._emitForChunk`, session.ts).
 *
 * Streams a single high-volume turn (N single-char text deltas) and measures the
 * per-chunk emit + subscriber fan-out cost. The harness maps each provider chunk
 * to a `text_delta` harness event and fans it out to every subscriber, so a turn
 * with many deltas isolates the dispatch hot path. A second case varies subscriber
 * count to expose fan-out scaling.
 *
 * `*.bench.ts` → ignored by `pnpm test` (include glob is `src/**\/*.test.ts`);
 * runs only under `vitest bench`. Bounded Tinybench options below cap wall time.
 *
 * Run:
 *   pnpm --filter ./packages/core exec vitest bench src/harness/v1/emit-for-chunk.bench.ts --run
 */

import type { LanguageModelV2StreamPart } from '@ai-sdk/provider-v5';
import { afterAll, bench, describe } from 'vitest';

import { Agent } from '../../agent';
// eslint-disable-next-line no-restricted-imports -- bench harness drives the shared mock model
import { convertArrayToReadableStream, MockLanguageModelV2 } from '../../agent/__tests__/mock-model';
import { InMemoryStore } from '../../storage';

import type { HarnessEvent } from './events';
import { Harness } from './harness';

const testUsage = { inputTokens: 10, outputTokens: 20, totalTokens: 30 };

function manyDeltaStream(count: number) {
  const deltas = Array.from({ length: count }, () => 'x');
  return convertArrayToReadableStream<LanguageModelV2StreamPart>([
    { type: 'stream-start', warnings: [] },
    { type: 'response-metadata', id: 'id-text', modelId: 'mock-model-id', timestamp: new Date(0) },
    { type: 'text-start', id: 'text-1' },
    ...deltas.map(delta => ({ type: 'text-delta' as const, id: 'text-1', delta })),
    { type: 'text-end', id: 'text-1' },
    { type: 'finish', finishReason: 'stop', usage: testUsage },
  ]);
}

function buildHarness(deltaCount: number) {
  const model = new MockLanguageModelV2({
    doStream: async () => ({
      rawCall: { rawPrompt: null, rawSettings: {} },
      warnings: [],
      stream: manyDeltaStream(deltaCount),
    }),
  });
  const agent = new Agent({ id: 'default', name: 'default', instructions: 'reply', model });
  return new Harness({
    agents: { default: agent } as any,
    storage: new InMemoryStore(),
    modes: [{ id: 'default', agentId: 'default' }],
    defaultModeId: 'default',
  });
}

// Each iteration streams DELTA_COUNT chunks, so keep iteration count modest.
const DELTA_COUNT = 2000;
const BOUNDED = { time: 2000, iterations: 20, warmupIterations: 3, warmupTime: 250 } as const;

describe('Harness v1 event dispatch — _emitForChunk fan-out', () => {
  const harness = buildHarness(DELTA_COUNT);

  afterAll(async () => {
    await harness.shutdown();
  });

  bench(
    `${DELTA_COUNT} text-delta chunks — 0 subscribers`,
    async () => {
      const session = await harness.session({ resourceId: 'bench-emit-0', threadId: { fresh: true } });
      await session.message({ content: 'go' });
    },
    BOUNDED,
  );

  bench(
    `${DELTA_COUNT} text-delta chunks — 5 subscribers (no-op listeners)`,
    async () => {
      const session = await harness.session({ resourceId: 'bench-emit-5', threadId: { fresh: true } });
      const unsubs: Array<() => void> = [];
      for (let i = 0; i < 5; i++) {
        unsubs.push(session.subscribe((_e: HarnessEvent) => {}));
      }
      await session.message({ content: 'go' });
      for (const off of unsubs) off();
    },
    BOUNDED,
  );
});
