/**
 * Regression test for https://github.com/mastra-ai/mastra/issues/17137
 *
 * When an agent is registered to a Mastra instance that has storage configured,
 * calling agent.generate()/stream() must not read or write workflow storage for
 * the throwaway internal `execution-workflow`. It is an implementation detail,
 * is not resumable, and uses an explicitly transient execution.
 */
import { convertArrayToReadableStream, MockLanguageModelV2 } from '@internal/ai-sdk-v5/test';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { Mastra } from '../../mastra';
import { InMemoryStore } from '../../storage';
import { Agent } from '../agent';

function createDummyModel() {
  return new MockLanguageModelV2({
    doGenerate: async () => ({
      rawCall: { rawPrompt: null, rawSettings: {} },
      finishReason: 'stop',
      usage: { inputTokens: 10, outputTokens: 20, totalTokens: 30 },
      content: [{ type: 'text', text: 'Dummy response' }],
      warnings: [],
    }),
    doStream: async () => ({
      rawCall: { rawPrompt: null, rawSettings: {} },
      warnings: [],
      stream: convertArrayToReadableStream([
        { type: 'stream-start', warnings: [] },
        { type: 'response-metadata', id: 'id-0', modelId: 'mock-model-id', timestamp: new Date(0) },
        { type: 'text-start', id: 'text-1' },
        { type: 'text-delta', id: 'text-1', delta: 'Dummy response' },
        { type: 'text-end', id: 'text-1' },
        { type: 'finish', finishReason: 'stop', usage: { inputTokens: 10, outputTokens: 20, totalTokens: 30 } },
      ]),
    }),
  });
}

function buildAgentWithStorage() {
  const storage = new InMemoryStore();
  const agent = new Agent({
    id: 'noise-agent',
    name: 'noise-agent',
    instructions: 'test',
    model: createDummyModel(),
  });
  const mastra = new Mastra({
    agents: { agent },
    storage,
    logger: false,
  });
  return { mastra, storage };
}

describe('agent execution-workflow storage noise (issue #17137)', () => {
  beforeAll(() => {
    vi.stubEnv('MASTRA_EVENTED_EXECUTION', 'false');
  });

  afterAll(() => {
    vi.unstubAllEnvs();
  });

  it.each(['generate', 'stream'] as const)(
    'performs zero workflow-storage operations for the internal execution-workflow on %s',
    async method => {
      const { mastra, storage } = buildAgentWithStorage();
      const workflowsStore = (await storage.getStore('workflows'))!;
      const operations = {
        loadWorkflowSnapshot: vi.spyOn(workflowsStore, 'loadWorkflowSnapshot'),
        getWorkflowRunById: vi.spyOn(workflowsStore, 'getWorkflowRunById'),
        persistWorkflowSnapshot: vi.spyOn(workflowsStore, 'persistWorkflowSnapshot'),
        persistWorkflowStepUpdate: vi.spyOn(workflowsStore, 'persistWorkflowStepUpdate'),
        updateWorkflowResults: vi.spyOn(workflowsStore, 'updateWorkflowResults'),
        updateWorkflowState: vi.spyOn(workflowsStore, 'updateWorkflowState'),
        deleteWorkflowRunById: vi.spyOn(workflowsStore, 'deleteWorkflowRunById'),
      };

      if (method === 'generate') {
        await mastra.getAgent('agent').generate('Hello!');
      } else {
        const stream = await mastra.getAgent('agent').stream('Hello!');
        for await (const _part of stream.fullStream) {
          // Consume the stream so every internal workflow phase completes.
        }
      }

      const executionWorkflowCalls = Object.entries(operations).flatMap(([operation, spy]) =>
        spy.mock.calls.flatMap(([input]) =>
          input.workflowName === 'execution-workflow' ? [{ operation, input }] : [],
        ),
      );
      expect(executionWorkflowCalls).toEqual([]);
    },
  );
});
