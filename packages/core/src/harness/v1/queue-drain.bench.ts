/**
 * Harness v1 — QUEUE DRAIN benchmark (`Session._maybeDrainQueue`, session.ts).
 *
 * `queue()` admits an item then fire-and-forget kicks `_maybeDrainQueue`, a
 * while-loop over `pendingQueue` running `_scheduleNextQueueHead` (expire +
 * priority-rotate CAS) then `_runQueuedTurn`/`_completeQueuedTurn` per item. This
 * bench enqueues N trivial turns (instant `stop`, no deltas) so the loop +
 * scheduler + per-turn settlement overhead dominates, and awaits them all.
 *
 * NOTE: default `sessions.maxQueueDepth` is 100 with `reject` backpressure
 * (harness.ts:152/852). This bench raises `maxQueueDepth` so the full batch is
 * admitted; otherwise excess `queue()` calls would reject with
 * `HarnessQueueFullError`. The cap itself is exercised elsewhere; here we measure
 * drain throughput, not backpressure.
 *
 * `*.bench.ts` → ignored by `pnpm test`; runs only under `vitest bench`.
 *
 * Run:
 *   pnpm --filter ./packages/core exec vitest bench src/harness/v1/queue-drain.bench.ts --run
 */

import type { LanguageModelV2StreamPart } from '@ai-sdk/provider-v5';
import { afterEach, bench, describe } from 'vitest';

import { convertArrayToReadableStream, MockLanguageModelV2 } from '../../agent/__tests__/mock-model';
import { Agent } from '../../agent';
import { InMemoryStore } from '../../storage';

import { Harness } from './harness';

const testUsage = { inputTokens: 1, outputTokens: 1, totalTokens: 2 };

/** Instant `stop` stream — no text deltas, so per-turn work is minimal. */
function instantStopStream() {
  return convertArrayToReadableStream<LanguageModelV2StreamPart>([
    { type: 'stream-start', warnings: [] },
    { type: 'response-metadata', id: 'id-stop', modelId: 'mock-model-id', timestamp: new Date(0) },
    { type: 'finish', finishReason: 'stop', usage: testUsage },
  ]);
}

function buildHarness(maxQueueDepth: number) {
  const model = new MockLanguageModelV2({
    doStream: async () => ({
      rawCall: { rawPrompt: null, rawSettings: {} },
      warnings: [],
      stream: instantStopStream(),
    }),
  });
  const agent = new Agent({ id: 'default', name: 'default', instructions: 'reply', model });
  return new Harness({
    agents: { default: agent } as any,
    storage: new InMemoryStore(),
    modes: [{ id: 'default', agentId: 'default' }],
    defaultModeId: 'default',
    sessions: { maxQueueDepth },
  });
}

const QUEUE_DEPTH = 200;

// Each iteration drains QUEUE_DEPTH turns — keep iterations small + fixed.
const BOUNDED = { time: 0, iterations: 5, warmupIterations: 1, warmupTime: 0 } as const;

describe('Harness v1 queue drain — _maybeDrainQueue', () => {
  // A fresh harness per bench so leaked drain timers cannot bleed across runs.
  let harness: Harness | undefined;

  afterEach(async () => {
    if (harness) {
      await harness.shutdown();
      harness = undefined;
    }
  });

  bench(
    `drain ${QUEUE_DEPTH}-item queue (trivial turns)`,
    async () => {
      harness = buildHarness(QUEUE_DEPTH + 10);
      const session = await harness.session({ resourceId: 'bench-queue', threadId: { fresh: true } });
      const pending: Array<Promise<unknown>> = [];
      for (let i = 0; i < QUEUE_DEPTH; i++) {
        pending.push(session.queue({ content: `q-${i}` }));
      }
      await Promise.all(pending);
      await harness.shutdown();
      harness = undefined;
    },
    BOUNDED,
  );
});
