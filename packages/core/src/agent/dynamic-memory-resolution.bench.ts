/**
 * Provider-free regression benchmark for dynamic Agent memory resolution.
 *
 * Each case runs one complete turn with the named resolver delay. The callback
 * fails unless the resolver runs exactly once, so wall time should grow by one
 * delay interval rather than by the number of execution surfaces that consume
 * memory.
 *
 * Run:
 *   pnpm --filter ./packages/core exec vitest bench src/agent/dynamic-memory-resolution.bench.ts --run
 */

import { MockLanguageModelV2 } from '@internal/ai-sdk-v5/test';
import { bench, describe } from 'vitest';

import { MockMemory } from '../memory/mock';
import { InMemoryStore } from '../storage';

import { Agent } from './agent';

const resolverDelays = [0, 25, 100] as const;
const bounded = { time: 0, iterations: 3, warmupIterations: 1, warmupTime: 0, throws: true } as const;

function createScenario(resolverDelay: number) {
  const memory = new MockMemory({ storage: new InMemoryStore() });
  let resolverCalls = 0;
  const agent = new Agent({
    id: `dynamic-memory-bench-${resolverDelay}`,
    name: `Dynamic Memory Bench ${resolverDelay}`,
    instructions: 'Reply once.',
    model: new MockLanguageModelV2({
      doGenerate: async () => ({
        rawCall: { rawPrompt: null, rawSettings: {} },
        finishReason: 'stop',
        usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
        content: [{ type: 'text', text: 'done' }],
        warnings: [],
      }),
    }),
    memory: async () => {
      resolverCalls += 1;
      if (resolverDelay > 0) {
        await new Promise(resolve => setTimeout(resolve, resolverDelay));
      }
      return memory;
    },
  });

  return async () => {
    const callsBefore = resolverCalls;
    await agent.generate('go', {
      memory: {
        resource: 'benchmark-resource',
        thread: `benchmark-thread-${resolverDelay}`,
      },
    });
    const calls = resolverCalls - callsBefore;
    if (calls !== 1) {
      throw new Error(`Expected 1 dynamic memory resolution at ${resolverDelay}ms, received ${calls}`);
    }
  };
}

describe('dynamic Agent memory resolution', () => {
  for (const resolverDelay of resolverDelays) {
    bench(`${resolverDelay}ms resolver / 1 resolution`, createScenario(resolverDelay), bounded);
  }
});
