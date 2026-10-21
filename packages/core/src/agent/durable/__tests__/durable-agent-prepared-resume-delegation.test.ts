/**
 * PF-6182 (S1): a resume refreshes the delegation tools a run built for its
 * first request (upstream #26686), so a delegation after the resume uses the
 * resume's hooks. That must hold for a run started through `prepare()`, whose
 * registry entry is a private copy of the prepared tools.
 */
import type { LanguageModelV2 } from '@ai-sdk/provider-v5';
import { MockLanguageModelV2, convertArrayToReadableStream } from '@internal/ai-sdk-v5/test';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { z } from 'zod';
import { EventEmitterPubSub } from '../../../events/event-emitter';
import { Mastra } from '../../../mastra';
import { MockStore } from '../../../storage/mock';
import { createTool } from '../../../tools';
import { Agent } from '../../agent';
import { createDurableAgent } from '../create-durable-agent';
import { globalRunRegistry } from '../run-registry';

const usage = { inputTokens: 10, outputTokens: 20, totalTokens: 30 };

function toolCall(id: string, toolName: string, input: object) {
  return convertArrayToReadableStream([
    { type: 'stream-start', warnings: [] },
    { type: 'response-metadata', id, modelId: 'mock-model', timestamp: new Date(0) },
    {
      type: 'tool-call',
      toolCallType: 'function',
      toolCallId: id,
      toolName,
      input: JSON.stringify(input),
      providerExecuted: false,
    },
    { type: 'finish', finishReason: 'tool-calls', usage },
  ]);
}

function text(id: string, delta: string) {
  return convertArrayToReadableStream([
    { type: 'stream-start', warnings: [] },
    { type: 'response-metadata', id, modelId: 'mock-model', timestamp: new Date(0) },
    { type: 'text-start', id: `${id}-text` },
    { type: 'text-delta', id: `${id}-text`, delta },
    { type: 'text-end', id: `${id}-text` },
    { type: 'finish', finishReason: 'stop', usage },
  ]);
}

/** Approval first, then a delegation, then the answer. */
function supervisorModel() {
  let calls = 0;
  return new MockLanguageModelV2({
    doStream: async () => {
      calls += 1;
      const stream =
        calls === 1
          ? toolCall('call-approval', 'approvalTool', { value: 'x' })
          : calls === 2
            ? toolCall('call-delegate', 'agent-researchAgent', { prompt: 'research dolphins' })
            : text('final', 'Done');
      return { stream, rawCall: { rawPrompt: null, rawSettings: {} }, warnings: [] };
    },
  });
}

describe('DurableAgent prepared run resume delegation (PF-6182)', () => {
  let pubsub: EventEmitterPubSub;

  beforeEach(() => {
    pubsub = new EventEmitterPubSub();
  });

  afterEach(async () => {
    globalRunRegistry.clear();
    await pubsub.close();
  });

  it("uses the resume's delegation hooks for a delegation after the resume of a prepared run", async () => {
    const subAgent = new Agent({
      id: 'researchAgent',
      name: 'researchAgent',
      description: 'Sub-agent researchAgent',
      instructions: 'You are a helpful sub-agent.',
      model: new MockLanguageModelV2({
        doStream: async () => ({
          stream: text('sub', 'Dolphins are marine mammals.'),
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
        }),
      }) as LanguageModelV2,
    });
    const approvalTool = createTool({
      id: 'approvalTool',
      description: 'approval-gated tool',
      inputSchema: z.object({ value: z.string() }),
      requireApproval: true,
      execute: async () => 'ok',
    });
    const supervisor = new Agent({
      id: 'prepared-resume-supervisor',
      name: 'prepared-resume-supervisor',
      instructions: 'You orchestrate sub-agents.',
      model: supervisorModel() as LanguageModelV2,
      agents: { researchAgent: subAgent },
      tools: { approvalTool },
    });
    const durableAgent = createDurableAgent({ agent: supervisor, pubsub });
    new Mastra({ logger: false, storage: new MockStore(), agents: { preparedResumeSupervisor: durableAgent } });

    const staleHook = vi.fn(() => ({ proceed: true as const }));
    const freshHook = vi.fn(() => ({ proceed: true as const }));
    let suspended: unknown;
    const onSuspended = (data: unknown) => {
      suspended = data;
    };
    const options = { maxSteps: 5, delegation: { onDelegationStart: staleHook }, onSuspended };

    const prepared = await durableAgent.prepare('Research dolphins', options);
    const initial = await durableAgent.stream('Research dolphins', { ...options, runId: prepared.runId });
    await vi.waitFor(() => expect(suspended).toBeDefined());

    let finished: unknown;
    const resumed = await durableAgent.resume(
      initial.runId,
      { approved: true },
      {
        delegation: { onDelegationStart: freshHook },
        onFinish: data => {
          finished = data;
        },
      },
    );
    await vi.waitFor(() => expect(finished).toBeDefined());

    expect(freshHook).toHaveBeenCalledTimes(1);
    expect(staleHook).not.toHaveBeenCalled();

    resumed.cleanup();
    initial.cleanup();
  });
});
