/**
 * Native suspension receipt on the true EventedAgent (S3).
 *
 * Uses a registered EventedAgent with an asserted evented engine and holds
 * suspension persistence behind a barrier:
 * - `stream()` still returns its handle asynchronously while held;
 * - an immediate warm `resume()` waits for the persisted-finish receipt;
 * - `generate()` cannot settle while suspended until the receipt lands;
 * - actor, request context, and ownership survive the resume.
 */

import { MockLanguageModelV2, convertArrayToReadableStream } from '@internal/ai-sdk-v5/test';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { EventEmitterPubSub } from '../../../events/event-emitter';
import { Mastra } from '../../../mastra';
import { RequestContext } from '../../../request-context';
import { InMemoryStore } from '../../../storage';
import { createTool } from '../../../tools';
import { Agent } from '../../agent';
import { createEventedAgent } from '../create-evented-agent';

const openPubsubs: EventEmitterPubSub[] = [];
const ACTOR = { actorKind: 'system', sourceWorkflow: 'receipt-gate' } as const;
const CONTEXT_MARKER = 'receipt-marker';

afterEach(async () => {
  vi.unstubAllEnvs();
  await Promise.all(openPubsubs.splice(0).map(pubsub => pubsub.close()));
});

/** First model turn emits the approval-gated tool call; every later turn answers with text. */
function approvalThenTextModel(toolCallId = 'receipt-call', finalText = 'recovered.') {
  let callCount = 0;
  return new MockLanguageModelV2({
    doStream: async () => {
      callCount += 1;
      if (callCount === 1) {
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: convertArrayToReadableStream([
            { type: 'stream-start', warnings: [] },
            { type: 'response-metadata', id: 'tool-turn', modelId: 'mock-model', timestamp: new Date(0) },
            {
              type: 'tool-call',
              toolCallType: 'function',
              toolCallId,
              toolName: 'gatedTool',
              input: JSON.stringify({ value: 'persisted' }),
              providerExecuted: false,
            },
            {
              type: 'finish',
              finishReason: 'tool-calls',
              usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
            },
          ]),
        };
      }
      return {
        rawCall: { rawPrompt: null, rawSettings: {} },
        warnings: [],
        stream: convertArrayToReadableStream([
          { type: 'stream-start', warnings: [] },
          { type: 'response-metadata', id: 'text-turn', modelId: 'mock-model', timestamp: new Date(0) },
          { type: 'text-start', id: 'text-1' },
          { type: 'text-delta', id: 'text-1', delta: finalText },
          { type: 'text-end', id: 'text-1' },
          {
            type: 'finish',
            finishReason: 'stop',
            usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
          },
        ]),
      };
    },
  });
}

const seenActors: unknown[] = [];
const seenMarkers: unknown[] = [];

function createAgent(storage: InMemoryStore, model: any, execute: any) {
  const tool = createTool({
    id: 'gatedTool',
    description: 'Requires an explicit approval',
    inputSchema: z.object({ value: z.string() }),
    requireApproval: true,
    execute,
  });
  const baseAgent = new Agent({
    id: 'receipt-gate-agent',
    name: 'receipt-gate-agent',
    instructions: 'Use the gated tool.',
    model: model as any,
    tools: { gatedTool: tool },
  });
  const pubsub = new EventEmitterPubSub();
  openPubsubs.push(pubsub);
  const agent = createEventedAgent({ agent: baseAgent, pubsub });
  const mastra = new Mastra({ logger: false, storage, agents: { [agent.id]: agent as any } });
  return { agent, mastra };
}

/** Hold every suspended-status workflow persist behind a releasable barrier. */
async function gateSuspendedPersists(storage: InMemoryStore) {
  const workflows = (await storage.getStore('workflows'))!;
  let release!: () => void;
  const gate = new Promise<void>(resolve => {
    release = resolve;
  });
  let held = 0;
  const persist = workflows.persistWorkflowSnapshot.bind(workflows);
  vi.spyOn(workflows, 'persistWorkflowSnapshot').mockImplementation(async (state: any) => {
    const snapshot = typeof state?.snapshot === 'string' ? JSON.parse(state.snapshot) : state?.snapshot;
    if (snapshot?.status === 'suspended') {
      held += 1;
      await gate;
    }
    return persist(state);
  });
  return { release, held: () => held };
}

function memoryOptions() {
  return { memory: { thread: 'thread-1', resource: 'resource-1' } } as const;
}

function streamOptions(runId: string) {
  const requestContext = new RequestContext();
  requestContext.set(CONTEXT_MARKER, 'marker-value');
  return {
    runId,
    requireToolApproval: true,
    ...memoryOptions(),
    actor: { ...ACTOR },
    requestContext,
  } as any;
}

/**
 * Resume authority is fresh per segment: a resumed segment never recovers the
 * initial actor/context from serialized options, so the resume call re-supplies
 * them. Preservation means the resumed tool execution observes exactly these.
 */
function resumeOptions(toolCallId: string) {
  const requestContext = new RequestContext();
  requestContext.set(CONTEXT_MARKER, 'marker-value');
  return {
    toolCallId,
    ...memoryOptions(),
    actor: { ...ACTOR },
    requestContext,
  } as any;
}

describe('EventedAgent native suspension receipt', () => {
  it('returns the stream handle while held; warm resume waits for the receipt and preserves actor/context/ownership', async () => {
    const storage = new InMemoryStore();
    const runId = 'receipt-gate-stream-run';
    const toolCallId = 'receipt-gate-stream-call';
    const execute = vi.fn(async ({ value }: any, options: any) => {
      seenActors.push(options?.actor);
      seenMarkers.push(options?.requestContext?.get?.(CONTEXT_MARKER));
      return { applied: value };
    });
    const { agent } = createAgent(storage, approvalThenTextModel(toolCallId), execute);
    expect((agent.getWorkflow() as any).engineType).toBe('evented');
    const barrier = await gateSuspendedPersists(storage);

    // stream() must return its handle asynchronously even though the
    // suspension persist is held: awaiting it here while the gate is closed
    // proves the public handle does not wait for the receipt.
    const started = await agent.stream('run it', streamOptions(runId));

    let suspendedChunk = false;
    for await (const chunk of started.fullStream as AsyncIterable<any>) {
      if (chunk.type === 'tool-call-suspended' || chunk.type === 'tool-call-approval') {
        suspendedChunk = true;
        break;
      }
    }
    expect(suspendedChunk).toBe(true);
    // Do NOT clean up the suspended stream here: cleanup would tear down
    // the run before the held suspension receipt lands (the harness skips
    // cleanup for suspended drains for the same reason). Both handles are
    // cleaned up after the resume completes.

    // Immediate warm resume on the live registry entry must wait for the
    // native persisted-finish receipt instead of racing persistence.
    let resumeSettled = false;
    const resumePromise = agent.resume(runId, { approved: true }, resumeOptions(toolCallId)).then((result: any) => {
      resumeSettled = true;
      return result;
    });
    // The gate must actually engage: a suspended persist is held before the
    // resume is allowed to proceed past the receipt wait.
    await vi.waitFor(() => expect(barrier.held()).toBeGreaterThanOrEqual(1));
    await new Promise(resolve => setTimeout(resolve, 300));
    expect(resumeSettled).toBe(false);
    expect(execute).not.toHaveBeenCalled();

    barrier.release();
    const resumed = await resumePromise;
    await resumed.output.consumeStream();
    expect(await resumed.output.text).toContain('recovered.');
    expect(execute).toHaveBeenCalledTimes(1);
    started.cleanup();
    resumed.cleanup();

    // Actor, request context, and ownership survive the gated resume.
    expect(seenActors).toEqual([{ ...ACTOR }]);
    expect(seenMarkers).toEqual(['marker-value']);
    await expect(agent.listSuspendedRuns({ resourceId: 'resource-1' })).resolves.toMatchObject({
      total: 0,
      runs: [],
    });
    expect(barrier.held()).toBeGreaterThanOrEqual(1);
  }, 30_000);

  it('generate cannot settle while suspended until the persisted receipt lands', async () => {
    const storage = new InMemoryStore();
    const runId = 'receipt-gate-generate-run';
    const toolCallId = 'receipt-gate-generate-call';
    const execute = vi.fn(async ({ value }: any) => ({ applied: value }));
    const { agent } = createAgent(storage, approvalThenTextModel(toolCallId), execute);
    expect((agent.getWorkflow() as any).engineType).toBe('evented');
    const barrier = await gateSuspendedPersists(storage);

    let settled = false;
    const generatePromise = agent.generate('run it', streamOptions(runId)).then((result: any) => {
      settled = true;
      return result;
    });
    await new Promise(resolve => setTimeout(resolve, 300));
    expect(settled).toBe(false);

    barrier.release();
    const output = await generatePromise;
    expect(settled).toBe(true);
    expect(output.finishReason).toBe('suspended');
    expect(output.runId).toBe(runId);
    expect(barrier.held()).toBeGreaterThanOrEqual(1);

    // The gated run remains resumable with its ownership intact.
    const resumed = await agent.resume(runId, { approved: true }, { toolCallId, ...memoryOptions() });
    await resumed.output.consumeStream();
    expect(execute).toHaveBeenCalledTimes(1);
    expect(await resumed.output.text).toContain('recovered.');
    resumed.cleanup();
  }, 30_000);
});
