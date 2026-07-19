/**
 * Regression test for the processor-workflow variant of
 * https://github.com/mastra-ai/mastra/issues/17137 (follow-up to #17344).
 *
 * #17344 fixed the internal `execution-workflow`, but agents that use memory or any
 * input/output processors also build an internal *processor* workflow
 * (Agent.combineProcessorsIntoWorkflow, executed by ProcessorRunner.executeWorkflowAsProcessor).
 * That workflow never received the parent Mastra reference, so its createRun() ->
 * getWorkflowRunById() saw no storage and emitted, on every run:
 *   "Cannot get workflow run. Mastra storage is not initialized"
 * before falling back to in-memory state.
 *
 * When an agent with a processor is registered to a Mastra instance that has storage
 * configured, calling agent.generate()/stream() must:
 *   1. NOT take the no-storage branch for the internal `<agentId>-input-processor` workflow
 *      (it now receives the parent Mastra reference).
 *   2. NOT persist a workflow snapshot for that throwaway internal processor workflow.
 */
import { convertArrayToReadableStream, MockLanguageModelV2 } from '@internal/ai-sdk-v5/test';
import { describe, expect, it, vi } from 'vitest';
import { Mastra } from '../../mastra';
import { setProcessorWorkflowPhases } from '../../processors';
import type { Processor } from '../../processors';
import { ProcessorStepInputSchema, ProcessorStepOutputSchema } from '../../processors/step-schema';
import { InMemoryStore } from '../../storage';
import { createStep, createWorkflow } from '../../workflows';
import { Workflow } from '../../workflows/workflow';
import { Agent } from '../agent';

const AGENT_ID = 'processor-noise-agent';
const PROCESSOR_WORKFLOW_ID = `${AGENT_ID}-input-processor`;

function createDummyModel(textDeltaCount = 1) {
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
        ...Array.from({ length: textDeltaCount }, (_, index) => ({
          type: 'text-delta' as const,
          id: 'text-1',
          delta: `Dummy response ${index}`,
        })),
        { type: 'text-end', id: 'text-1' },
        { type: 'finish', finishReason: 'stop', usage: { inputTokens: 10, outputTokens: 20, totalTokens: 30 } },
      ]),
    }),
  });
}

// Two minimal no-op processors force the agent to build the internal
// `createWorkflow(...)` processor workflow without pulling in @mastra/memory.
const noopInputProcessor: Processor = {
  id: 'noop-input-processor',
  processInput: async ({ messages }) => messages,
};
const secondNoopInputProcessor: Processor = {
  id: 'second-noop-input-processor',
  processInput: async ({ messages }) => messages,
};

function buildAgentWithProcessor(idGenerator?: () => string) {
  const storage = new InMemoryStore();
  const agent = new Agent({
    id: AGENT_ID,
    name: AGENT_ID,
    instructions: 'test',
    model: createDummyModel(),
    inputProcessors: [noopInputProcessor, secondNoopInputProcessor],
  });
  const mastra = new Mastra({
    agents: { [AGENT_ID]: agent },
    idGenerator,
    storage,
    logger: false,
  });
  return { mastra, storage };
}

function spyOnWorkflowAdmissions() {
  const workflowIds: string[] = [];
  const original = (Workflow.prototype as unknown as { createRun: (...args: unknown[]) => unknown }).createRun;
  const spy = vi.spyOn(Workflow.prototype as unknown as Record<string, any>, 'createRun').mockImplementation(function (
    this: any,
    ...args: unknown[]
  ) {
    workflowIds.push(this.id);
    return original.apply(this, args);
  });
  return { spy, workflowIds };
}

describe('agent processor-workflow storage noise (issue #17137 follow-up to #17344)', () => {
  it('does not admit a result-only output processor workflow for streamed parts', async () => {
    const storage = new InMemoryStore();
    const processOutputResult = vi.fn(async ({ messages }) => messages);
    const agent = new Agent({
      id: AGENT_ID,
      name: AGENT_ID,
      instructions: 'test',
      model: createDummyModel(12),
      outputProcessors: [{ id: 'result-only-output-processor', processOutputResult }],
    });
    const mastra = new Mastra({
      agents: { [AGENT_ID]: agent },
      storage,
      logger: false,
    });
    const workflowsStore = (await storage.getStore('workflows'))!;
    const read = vi.spyOn(workflowsStore, 'loadWorkflowSnapshot');
    const admissions = spyOnWorkflowAdmissions();

    try {
      const stream = await mastra.getAgent(AGENT_ID).stream('Hello!');
      for await (const _part of stream.fullStream) {
        // Consume the provider-free stream so output stream/result processors finish.
      }
    } finally {
      admissions.spy.mockRestore();
    }

    const processorWorkflowReads = read.mock.calls.filter(
      ([input]) => input.workflowName === `${AGENT_ID}-output-processor`,
    );
    expect(admissions.workflowIds.filter(id => id === `${AGENT_ID}-output-processor`)).toEqual([]);
    expect(processorWorkflowReads).toEqual([]);
    expect(processOutputResult).toHaveBeenCalledTimes(1);
  });

  it('skips unsupported stream phases for a combined result-only processor workflow', async () => {
    const storage = new InMemoryStore();
    const firstResult = vi.fn(async ({ messages }) => messages);
    const secondResult = vi.fn(async ({ messages }) => messages);
    const agent = new Agent({
      id: AGENT_ID,
      name: AGENT_ID,
      instructions: 'test',
      model: createDummyModel(12),
      outputProcessors: [
        { id: 'first-result-only-processor', processOutputResult: firstResult },
        { id: 'second-result-only-processor', processOutputResult: secondResult },
      ],
    });
    const mastra = new Mastra({ agents: { [AGENT_ID]: agent }, storage, logger: false });
    const workflowsStore = (await storage.getStore('workflows'))!;
    const read = vi.spyOn(workflowsStore, 'loadWorkflowSnapshot');
    const admissions = spyOnWorkflowAdmissions();

    try {
      const stream = await mastra.getAgent(AGENT_ID).stream('Hello!');
      for await (const _part of stream.fullStream) {
        // Consume the provider-free stream so output stream/result processors finish.
      }
    } finally {
      admissions.spy.mockRestore();
    }

    const processorWorkflowReads = read.mock.calls.filter(
      ([input]) => input.workflowName === `${AGENT_ID}-output-processor`,
    );
    expect(admissions.workflowIds.filter(id => id === `${AGENT_ID}-output-processor`)).toEqual([
      `${AGENT_ID}-output-processor`,
    ]);
    expect(processorWorkflowReads).toEqual([]);
    expect(firstResult).toHaveBeenCalledTimes(1);
    expect(secondResult).toHaveBeenCalledTimes(1);
  });

  it('keeps a phase-restricted custom workflow outside a mixed synthetic chain', async () => {
    const finalResult = vi.fn(async ({ messages }) => messages);
    const resultWorkflow = setProcessorWorkflowPhases(
      createWorkflow({
        id: 'custom-result-only-workflow',
        inputSchema: ProcessorStepInputSchema,
        outputSchema: ProcessorStepOutputSchema,
      })
        .then(createStep({ id: 'custom-final-result', processOutputResult: finalResult }))
        .commit(),
      ['outputResult'],
    );
    const streamPart = vi.fn(async ({ part }) => part);
    const streamProcessor: Processor = {
      id: 'stream-processor',
      processOutputStream: streamPart,
    };
    const agent = new Agent({
      id: AGENT_ID,
      name: AGENT_ID,
      instructions: 'test',
      model: createDummyModel(12),
    });
    const storage = new InMemoryStore();
    const mastra = new Mastra({ agents: { [AGENT_ID]: agent }, storage, logger: false });
    const workflowsStore = (await storage.getStore('workflows'))!;
    const read = vi.spyOn(workflowsStore, 'loadWorkflowSnapshot');
    const persist = vi.spyOn(workflowsStore, 'persistWorkflowSnapshot');
    const createCustomRun = vi.spyOn(resultWorkflow, 'createRun');

    expect(resultWorkflow.mastra).toBeUndefined();

    const stream = await mastra.getAgent(AGENT_ID).stream('Hello!', {
      outputProcessors: [resultWorkflow, streamProcessor],
    });
    for await (const _part of stream.fullStream) {
      // Consume all phases so an accidental nested admission is observable.
    }

    expect(streamPart.mock.calls.length).toBeGreaterThan(1);
    expect(resultWorkflow.mastra).toBe(mastra);
    expect(createCustomRun).toHaveBeenCalledTimes(1);
    expect(read.mock.calls.filter(([input]) => input.workflowName === resultWorkflow.id)).toEqual([]);
    expect(persist.mock.calls.filter(([input]) => input.workflowName === resultWorkflow.id)).toEqual([]);
    expect(finalResult).toHaveBeenCalledTimes(1);
  });

  it('preserves mixed processor order and state without durable lifecycle reads', async () => {
    const storage = new InMemoryStore();
    const resultOrder: string[] = [];
    let textDeltaCountInResult = 0;
    const streamAwareProcessor: Processor = {
      id: 'stream-aware-processor',
      processOutputStream: async ({ part, state }) => {
        if (part.type === 'text-delta') {
          state.textDeltaCount = Number(state.textDeltaCount ?? 0) + 1;
        }
        return part;
      },
      processOutputResult: async ({ messages, state }) => {
        textDeltaCountInResult = Number(state.textDeltaCount ?? 0);
        resultOrder.push('stream-aware');
        return messages;
      },
    };
    const resultOnlyProcessor: Processor = {
      id: 'result-only-processor',
      processOutputResult: async ({ messages }) => {
        resultOrder.push('result-only');
        return messages;
      },
    };
    const agent = new Agent({
      id: AGENT_ID,
      name: AGENT_ID,
      instructions: 'test',
      model: createDummyModel(12),
      outputProcessors: [streamAwareProcessor, resultOnlyProcessor],
    });
    const mastra = new Mastra({ agents: { [AGENT_ID]: agent }, storage, logger: false });
    const workflowsStore = (await storage.getStore('workflows'))!;
    const read = vi.spyOn(workflowsStore, 'loadWorkflowSnapshot');
    const streamedText: string[] = [];
    const admissions = spyOnWorkflowAdmissions();

    try {
      const stream = await mastra.getAgent(AGENT_ID).stream('Hello!');
      for await (const part of stream.fullStream) {
        if (part.type === 'text-delta') streamedText.push(part.payload.text);
      }
    } finally {
      admissions.spy.mockRestore();
    }

    expect(streamedText).toHaveLength(12);
    expect(textDeltaCountInResult).toBe(12);
    expect(resultOrder).toEqual(['stream-aware', 'result-only']);
    expect(admissions.workflowIds.filter(id => id === `${AGENT_ID}-output-processor`)).toEqual([]);
    expect(read.mock.calls.filter(([input]) => input.workflowName === `${AGENT_ID}-output-processor`)).toEqual([]);
  });

  it('does not read storage (getWorkflowRunById) for the internal processor workflow on generate', async () => {
    // Explicit transient execution skips both createRun collision lookup and
    // execution-time lifecycle reconciliation when Mastra mints the run ID.
    const seen: Array<{ id: string; hasStorage: boolean }> = [];
    const original = (Workflow.prototype as unknown as { getWorkflowRunById: (...a: unknown[]) => unknown })
      .getWorkflowRunById;
    const spy = vi
      .spyOn(Workflow.prototype as unknown as Record<string, any>, 'getWorkflowRunById')
      .mockImplementation(async function (this: any, ...args: unknown[]) {
        seen.push({ id: this.id, hasStorage: Boolean(this.mastra?.getStorage?.()) });
        return original.apply(this, args);
      });

    try {
      const { mastra } = buildAgentWithProcessor();
      await mastra.getAgent(AGENT_ID).generate('Hello!');
    } finally {
      spy.mockRestore();
    }

    const processorLookups = seen.filter(s => s.id === PROCESSOR_WORKFLOW_ID);
    expect(processorLookups).toEqual([]);
  });

  it('uses registered storage for a custom-generated processor run ID', async () => {
    const runId = 'deterministic-processor-run-id';
    const { mastra, storage } = buildAgentWithProcessor(() => runId);
    const workflowsStore = (await storage.getStore('workflows'))!;
    const read = vi.spyOn(workflowsStore, 'getWorkflowRunById');

    await mastra.getAgent(AGENT_ID).generate('Hello!');

    expect(read).toHaveBeenCalledWith({
      runId,
      workflowName: PROCESSOR_WORKFLOW_ID,
    });
  });

  it('does not persist a snapshot for the internal processor workflow on generate', async () => {
    const { mastra, storage } = buildAgentWithProcessor();

    await mastra.getAgent(AGENT_ID).generate('Hello!');

    const workflowsStore = await storage.getStore('workflows');
    const { runs, total } = await workflowsStore!.listWorkflowRuns({ workflowName: PROCESSOR_WORKFLOW_ID });
    expect(total).toBe(0);
    expect(runs).toEqual([]);
  });
});
