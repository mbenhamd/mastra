/**
 * Provider-free regression benchmark for transient processor workflow storage.
 *
 * Each case streams the named number of text deltas through two result-only
 * processors. The benchmark name records the required processor-workflow
 * storage-call count; Tinybench reports wall time for the full turn.
 *
 * Run:
 *   pnpm --filter ./packages/core exec vitest bench src/agent/processor-workflow-storage-noise.bench.ts --run
 */

import { convertArrayToReadableStream, MockLanguageModelV2 } from '@internal/ai-sdk-v5/test';
import { bench, describe } from 'vitest';

import { Mastra } from '../mastra';
import type { Processor } from '../processors';
import { InMemoryStore } from '../storage';

import { Agent } from './agent';

const partCounts = [1, 10, 100, 500, 2_000] as const;
const bounded = { time: 0, iterations: 1, warmupIterations: 0, warmupTime: 0 } as const;

function createModel(textDeltaCount: number) {
  return new MockLanguageModelV2({
    doStream: async () => ({
      rawCall: { rawPrompt: null, rawSettings: {} },
      warnings: [],
      stream: convertArrayToReadableStream([
        { type: 'stream-start', warnings: [] },
        { type: 'response-metadata', id: 'id-0', modelId: 'mock-model-id', timestamp: new Date(0) },
        { type: 'text-start', id: 'text-1' },
        ...Array.from({ length: textDeltaCount }, () => ({
          type: 'text-delta' as const,
          id: 'text-1',
          delta: 'x',
        })),
        { type: 'text-end', id: 'text-1' },
        { type: 'finish', finishReason: 'stop', usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 } },
      ]),
    }),
  });
}

function createScenario(textDeltaCount: number) {
  const agentId = `processor-storage-bench-${textDeltaCount}`;
  const processorWorkflowId = `${agentId}-output-processor`;
  const passthroughResult = async ({ messages }: Parameters<NonNullable<Processor['processOutputResult']>>[0]) =>
    messages;
  const storage = new InMemoryStore();
  const agent = new Agent({
    id: agentId,
    name: agentId,
    instructions: 'test',
    model: createModel(textDeltaCount),
    outputProcessors: [
      { id: 'first-result-only', processOutputResult: passthroughResult },
      { id: 'second-result-only', processOutputResult: passthroughResult },
    ],
  });
  const mastra = new Mastra({ agents: { [agentId]: agent }, storage, logger: false });
  let processorWorkflowStorageReads = 0;
  const instrumentation = storage.getStore('workflows').then(workflowsStore => {
    if (!workflowsStore) throw new Error('Workflow storage is required for the benchmark');
    const loadWorkflowSnapshot = workflowsStore.loadWorkflowSnapshot.bind(workflowsStore);
    workflowsStore.loadWorkflowSnapshot = async input => {
      if (input.workflowName === processorWorkflowId) processorWorkflowStorageReads++;
      return loadWorkflowSnapshot(input);
    };
  });

  return async () => {
    await instrumentation;
    const readsBefore = processorWorkflowStorageReads;
    const stream = await mastra.getAgent(agentId).stream('go');
    for await (const _part of stream.fullStream) {
      // Consume the complete turn inside the measured benchmark callback.
    }
    const reads = processorWorkflowStorageReads - readsBefore;
    if (reads !== 0) {
      throw new Error(`Expected 0 processor workflow storage reads for ${textDeltaCount} parts, received ${reads}`);
    }
  };
}

describe('result-only processor stream scaling', () => {
  for (const partCount of partCounts) {
    bench(`${partCount} text deltas / 0 processor workflow storage reads`, createScenario(partCount), bounded);
  }
});
