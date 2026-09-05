/**
 * Harness v1 — REAL-agent E2E (NOT MockAgent).
 *
 * Every other harness v1 test hands the harness a duck-typed `MockAgent` /
 * `FakeAgent` that overrides `stream` / `generate` / `resumeStream` WITHOUT
 * calling super, hand-staging a fabricated `MastraModelOutput`. That validates
 * the harness call shape, but it never exercises the real
 *   ai-sdk → provider → transform → loop → fullStream
 * path. The harness chunk→event mapping (`_emitForChunk`, session.ts:4225) reads
 * `chunk.payload.text` / `.toolCallId` / `.args` / `.result` off the REAL
 * `payload`-nested chunks that `aisdk/v5/transform.ts` produces — none of which
 * the MockAgent path can produce.
 *
 * This file binds a REAL `Agent` (driven by a deterministic
 * `MockLanguageModelV2`, the same provider-level mock the agent/loop tests use)
 * into a real `Harness` over `InMemoryStore`, and drives
 * `session.message` / `session.signal` / `session.queue` / `session.respondTo*`
 * through the genuine loop. It asserts:
 *
 *   S1  real streamed text-delta chunks surface as harness `text_delta` events
 *   S2  a real tool-call surfaces `tool_start`/`tool_end` with JSON-safe
 *       projected payloads, and the tool sees the harness RequestContext
 *   S3  suspend via an approval-requesting tool → `respondToToolApproval` →
 *       resume drains the continuation LIVE (§10.4: a live `tool_end` +
 *       post-approval `text_delta` precede the terminal `agent_end:complete`)
 *   S4  `signal_completed` / `queue_completed` settlement evidence is written
 *   S5  provider/model error (before run start AND mid-stream) surfaces as the
 *       right harness error / `agent_end:error` (redacted public error), and
 *       the in-process `message()` rejection is REDACTED too (§13.3f.1): its
 *       `.message` is generic and the raw provider detail is kept on `.cause`
 *   S6  abort terminalizes the real run: an external `abortActiveWork()` while
 *       the provider stream is live produces `agent_end:aborted` and
 *       `run_completed:interrupted`; a MID-RUN abort fired from inside a tool
 *       drives the same terminal; an ALREADY-aborted caller signal rejects
 *       `message()` pre-dispatch with no harness events.
 *
 * No `MockAgent` / `FakeAgent` is used in this file — the agent is the real
 * `Agent` class; only the LANGUAGE MODEL is mocked (deterministic, no network).
 */

import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod';

import { Agent } from '../../agent';
import { convertArrayToReadableStream, MockLanguageModelV2 } from '../../agent/__tests__/mock-model';
import { AgentThreadOutputDrainError } from '../../agent/thread-stream-runtime';
import { PubSub } from '../../events/pubsub';
import type { Event, EventCallback, SubscribeOptions } from '../../events/types';
import { Mastra } from '../../mastra';
import { MockMemory } from '../../memory/mock';
import { InMemoryStore } from '../../storage';
import { InMemoryHarness } from '../../storage/domains/harness/inmemory';
import { InMemoryDB } from '../../storage/domains/inmemory-db';
import { createTool } from '../../tools';
import { askUser } from '../../tools/builtin';

import type { HarnessEvent } from './events';
import { Harness } from './harness';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const testUsage = { inputTokens: 10, outputTokens: 20, totalTokens: 30 };

/** A raw provider stream that emits text in N deltas then finishes with `stop`. */
function textStream(deltas: string[]) {
  return convertArrayToReadableStream([
    { type: 'stream-start', warnings: [] },
    { type: 'response-metadata', id: 'id-text', modelId: 'mock-model-id', timestamp: new Date(0) },
    { type: 'text-start', id: 'text-1' },
    ...deltas.map(delta => ({ type: 'text-delta', id: 'text-1', delta })),
    { type: 'text-end', id: 'text-1' },
    { type: 'finish', finishReason: 'stop', usage: testUsage },
  ]);
}

/**
 * A raw provider stream that emits reasoning deltas, then text deltas, then
 * finishes `stop`. The provider-level `reasoning-delta` chunk carries `delta`
 * (AI-SDK v5 `LanguageModelV2StreamPart`); the v5 transform maps it into the
 * mastra chunk `{ type:'reasoning-delta', payload:{ text } }` the harness reads.
 */
function reasoningTextStream(reasoningDeltas: string[], textDeltas: string[]) {
  return convertArrayToReadableStream([
    { type: 'stream-start', warnings: [] },
    { type: 'response-metadata', id: 'id-reason', modelId: 'mock-model-id', timestamp: new Date(0) },
    { type: 'reasoning-start', id: 'reason-1' },
    ...reasoningDeltas.map(delta => ({ type: 'reasoning-delta', id: 'reason-1', delta })),
    { type: 'reasoning-end', id: 'reason-1' },
    { type: 'text-start', id: 'text-1' },
    ...textDeltas.map(delta => ({ type: 'text-delta', id: 'text-1', delta })),
    { type: 'text-end', id: 'text-1' },
    { type: 'finish', finishReason: 'stop', usage: testUsage },
  ]);
}

/** A raw provider stream that calls `toolName` with `inputJson`, finishing `tool-calls`. */
function toolCallStream(toolCallId: string, toolName: string, inputJson: string) {
  return convertArrayToReadableStream([
    { type: 'stream-start', warnings: [] },
    { type: 'response-metadata', id: 'id-tool', modelId: 'mock-model-id', timestamp: new Date(0) },
    { type: 'tool-call', toolCallId, toolName, input: inputJson, providerExecuted: false },
    { type: 'finish', finishReason: 'tool-calls', usage: testUsage },
  ]);
}

/**
 * A raw provider stream that emits optional reasoning and visible text before
 * a terminal tool call. This mirrors a specialist showing live progress while
 * still ending through Harness's required structured outcome contract.
 */
function reasoningTextToolCallStream(
  reasoningDeltas: string[],
  textDeltas: string[],
  toolCallId: string,
  toolName: string,
  inputJson: string,
) {
  return convertArrayToReadableStream([
    { type: 'stream-start', warnings: [] },
    { type: 'response-metadata', id: 'id-text-tool', modelId: 'mock-model-id', timestamp: new Date(0) },
    ...(reasoningDeltas.length === 0
      ? []
      : [
          { type: 'reasoning-start', id: 'reason-1' },
          ...reasoningDeltas.map(delta => ({ type: 'reasoning-delta', id: 'reason-1', delta })),
          { type: 'reasoning-end', id: 'reason-1' },
        ]),
    { type: 'text-start', id: 'text-1' },
    ...textDeltas.map(delta => ({ type: 'text-delta', id: 'text-1', delta })),
    { type: 'text-end', id: 'text-1' },
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

class DelayedDeliveryPubSub extends PubSub {
  private readonly subscribers = new Map<string, Set<EventCallback>>();
  private rejectedEvent = false;
  private matchingEventCount = 0;

  constructor(
    private readonly delayMs: number,
    private readonly rejectEventType?: 'run-registered' | 'run-completed' | 'run-suspended',
    private readonly rejectEventOccurrence = 1,
    /** Undefined rejects before delivery; a number delivers first, waits, then rejects. */
    private readonly rejectAfterDeliveryDelayMs?: number,
  ) {
    super();
  }

  override get supportedModes(): ReadonlyArray<'pull' | 'push'> {
    return ['pull', 'push'];
  }

  async publish(topic: string, event: Omit<Event, 'id' | 'createdAt'>): Promise<void> {
    await new Promise(resolve => setTimeout(resolve, this.delayMs));
    const eventType = (event.data as { type?: string } | undefined)?.type;
    const matchesRejectedEvent = this.rejectEventType !== undefined && eventType === this.rejectEventType;
    if (matchesRejectedEvent) {
      this.matchingEventCount++;
    }
    const shouldReject =
      !this.rejectedEvent && this.matchingEventCount === this.rejectEventOccurrence && matchesRejectedEvent;
    if (shouldReject) {
      this.rejectedEvent = true;
    }
    if (shouldReject && this.rejectAfterDeliveryDelayMs === undefined) {
      throw new Error(`injected ${this.rejectEventType} publication failure`);
    }
    const delivered = { ...event, id: crypto.randomUUID(), createdAt: new Date() };
    await Promise.all([...(this.subscribers.get(topic) ?? [])].map(callback => callback(delivered)));
    if (shouldReject) {
      await new Promise(resolve => setTimeout(resolve, this.rejectAfterDeliveryDelayMs));
      throw new Error(`injected ${this.rejectEventType} publication failure after delivery`);
    }
  }

  async subscribe(topic: string, callback: EventCallback, _options?: SubscribeOptions): Promise<void> {
    const callbacks = this.subscribers.get(topic) ?? new Set<EventCallback>();
    callbacks.add(callback);
    this.subscribers.set(topic, callbacks);
  }

  async unsubscribe(topic: string, callback: EventCallback): Promise<void> {
    this.subscribers.get(topic)?.delete(callback);
  }

  async flush(): Promise<void> {}
}

describe('DelayedDeliveryPubSub rejection injection', () => {
  it('does not reject an untyped event when rejection injection is omitted', async () => {
    const pubsub = new DelayedDeliveryPubSub(0);
    const subscriber = vi.fn();
    await pubsub.subscribe('test-topic', subscriber);

    await expect(
      pubsub.publish('test-topic', { type: 'test-event', runId: 'test-run', data: {} }),
    ).resolves.toBeUndefined();
    expect(subscriber).toHaveBeenCalledOnce();
  });

  it('still rejects the configured event type', async () => {
    const pubsub = new DelayedDeliveryPubSub(0, 'run-suspended');
    const subscriber = vi.fn();
    await pubsub.subscribe('test-topic', subscriber);

    await expect(
      pubsub.publish('test-topic', {
        type: 'test-event',
        runId: 'test-run',
        data: { type: 'run-suspended' },
      }),
    ).rejects.toThrow('injected run-suspended publication failure');
    expect(subscriber).not.toHaveBeenCalled();
  });
});

function sequentialApprovalFixture() {
  const firstExecute = vi.fn(async () => ({ first: true }));
  const secondExecute = vi.fn(async () => ({ second: true }));
  const firstApproval = createTool({
    id: 'firstApproval',
    description: 'first approval',
    inputSchema: z.object({}),
    requireApproval: true,
    execute: firstExecute,
  });
  const secondApproval = createTool({
    id: 'secondApproval',
    description: 'second approval',
    inputSchema: z.object({}),
    requireApproval: true,
    execute: secondExecute,
  });
  let callCount = 0;
  const model = new MockLanguageModelV2({
    doStream: async () => {
      callCount++;
      return {
        rawCall: { rawPrompt: null, rawSettings: {} },
        warnings: [],
        stream:
          callCount === 1
            ? toolCallStream('first-approval-call', 'firstApproval', '{}')
            : callCount === 2
              ? toolCallStream('second-approval-call', 'secondApproval', '{}')
              : textStream(['unexpected retry']),
      };
    },
  });
  const agent = new Agent({
    id: 'default',
    name: 'default',
    instructions: 'request both approvals in order',
    model,
    tools: { firstApproval, secondApproval },
  });
  return { agent, firstExecute, secondExecute };
}

/** Wait until `predicate()` is true, polling the microtask/timer queue. */
async function waitFor(predicate: () => boolean, label: string, timeoutMs = 2000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${label}`);
    await new Promise(resolve => setImmediate(resolve));
  }
}

// ===========================================================================
// S1 — real streamed text-delta → harness text_delta events
// ===========================================================================

describe('Harness v1 real-agent E2E — S1 streamed text', () => {
  it('maps N real provider text-delta chunks to N harness text_delta events (concatenated == model text)', async () => {
    const deltas = ['Hello', ', ', 'world!'];
    const model = new MockLanguageModelV2({
      doStream: async () => ({
        rawCall: { rawPrompt: null, rawSettings: {} },
        warnings: [],
        stream: textStream(deltas),
      }),
    });
    const agent = new Agent({ id: 'default', name: 'default', instructions: 'reply', model });
    const harness = newHarness(agent);
    try {
      const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));

      const result = (await session.message({ content: 'hi' })) as any;

      // The real loop concatenated the deltas into the final text.
      expect(result.text).toBe(deltas.join(''));
      // The real loop carries genuine usage (NOT the MockAgent's fabricated 1/1/2).
      expect(result.usage.totalTokens).toBe(testUsage.totalTokens);

      const textDeltas = events.filter(e => e.type === 'text_delta') as Array<{ delta: string; runId: string }>;
      expect(textDeltas).toHaveLength(deltas.length);
      expect(textDeltas.map(e => e.delta)).toEqual(deltas);
      expect(textDeltas.map(e => e.delta).join('')).toBe(result.text);

      // agent_start precedes first text_delta; agent_end:complete follows last.
      const types = events.map(e => e.type);
      const startIdx = types.indexOf('agent_start');
      const firstDeltaIdx = types.indexOf('text_delta');
      const endIdx = types.lastIndexOf('agent_end');
      expect(startIdx).toBeGreaterThanOrEqual(0);
      expect(startIdx).toBeLessThan(firstDeltaIdx);
      expect(endIdx).toBeGreaterThan(types.lastIndexOf('text_delta'));
      const agentEnd = events.find(e => e.type === 'agent_end') as { finishReason: string; runId: string };
      expect(agentEnd.finishReason).toBe('complete');
      // runId is consistent across the turn's events.
      expect(textDeltas.every(e => e.runId === agentEnd.runId)).toBe(true);
    } finally {
      await harness.shutdown();
    }
  });

  it('rejects instead of hanging when the terminal event cannot be published', async () => {
    const model = new MockLanguageModelV2({
      doStream: async () => ({
        rawCall: { rawPrompt: null, rawSettings: {} },
        warnings: [],
        stream: textStream(['done']),
      }),
    });
    const agent = new Agent({ id: 'default', name: 'default', instructions: 'reply', model });
    const pubsub = new DelayedDeliveryPubSub(0, 'run-completed');
    const mastra = new Mastra({ agents: { default: agent }, storage: new InMemoryStore(), pubsub });
    const harness = new Harness({
      mastra,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });
    try {
      const session = await harness.session({ resourceId: 'u-terminal-failure', threadId: { fresh: true } });

      const rejection = await session.message({ content: 'finish' }).then(
        () => undefined,
        error => error,
      );

      expect(rejection).toMatchObject({ name: 'HarnessExecutionError' });
      expect((rejection as { cause?: unknown }).cause).toBeInstanceOf(AgentThreadOutputDrainError);
      expect(((rejection as { cause: AgentThreadOutputDrainError }).cause as AgentThreadOutputDrainError).reason).toBe(
        'terminal-publish-failed',
      );
    } finally {
      await harness.shutdown();
    }
  });
});

// ===========================================================================
// S2 — real tool-call/tool-result → tool_start/tool_end + JSON-safe payloads
//      + the tool sees the harness RequestContext
// ===========================================================================

describe('Harness v1 real-agent E2E — S2 tool round-trip', () => {
  it('emits tool_start/tool_end with JSON-safe projected payloads and passes RequestContext to the tool', async () => {
    const seenHarnessCtx: unknown[] = [];
    const findUser = createTool({
      id: 'findUser',
      description: 'look up a user',
      inputSchema: z.object({ name: z.string() }),
      // Return a non-JSON-native value (Date) to prove projection happens at emit.
      execute: async (input, context) => {
        // The harness injects its context under the 'harness' key (session.tool-context.test.ts).
        seenHarnessCtx.push(context?.requestContext?.get('harness'));
        return { name: (input as { name: string }).name, lastSeen: new Date(0) };
      },
    });

    let callCount = 0;
    const model = new MockLanguageModelV2({
      doStream: async () => {
        callCount++;
        if (callCount === 1) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream('call-1', 'findUser', '{"name":"Dero Israel"}'),
          };
        }
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: textStream(['Found ', 'Dero Israel']),
        };
      },
    });

    const agent = new Agent({
      id: 'default',
      name: 'default',
      instructions: 'use findUser',
      model,
      tools: { findUser },
    });
    const harness = newHarness(agent);
    try {
      const session = await harness.session({ resourceId: 'u-tool', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));

      const result = (await session.message({ content: 'find Dero Israel' })) as any;

      const toolStart = events.find(e => e.type === 'tool_start') as
        | { toolCallId: string; toolName: string; input: unknown; runId: string }
        | undefined;
      const toolEnd = events.find(e => e.type === 'tool_end') as
        | { toolCallId: string; toolName: string; output: any; isError: boolean; runId: string }
        | undefined;

      expect(toolStart).toBeDefined();
      expect(toolStart!.toolName).toBe('findUser');
      expect(toolStart!.toolCallId).toBe('call-1');
      // input is the parsed args, JSON-safe.
      expect(toolStart!.input).toEqual({ name: 'Dero Israel' });

      expect(toolEnd).toBeDefined();
      expect(toolEnd!.toolCallId).toBe('call-1');
      expect(toolEnd!.isError).toBe(false);
      expect(toolEnd!.output.name).toBe('Dero Israel');
      // The Date was projected to an ISO string at emit (projectToolEventPayloadForJson) —
      // a raw Date instance must NOT cross the harness event boundary.
      expect(toolEnd!.output.lastSeen).toBe(new Date(0).toISOString());
      expect(toolEnd!.output.lastSeen).not.toBeInstanceOf(Date);

      // Ordering: tool_start → tool_end → text_delta.
      const types = events.map(e => e.type);
      expect(types.indexOf('tool_start')).toBeLessThan(types.indexOf('tool_end'));
      expect(types.indexOf('tool_end')).toBeLessThan(types.lastIndexOf('text_delta'));

      // The tool actually ran inside the real loop and saw the harness context.
      expect(seenHarnessCtx).toHaveLength(1);
      expect(seenHarnessCtx[0]).toBeDefined();
      expect((seenHarnessCtx[0] as { sessionId?: string }).sessionId).toBe(session.id);

      // The real FullOutput carries real toolCalls/toolResults (proving the loop ran,
      // not a fabricated empty-array output).
      expect(Array.isArray(result.toolCalls)).toBe(true);
      expect(result.toolCalls.length).toBeGreaterThan(0);
      expect(result.toolResults.length).toBeGreaterThan(0);
    } finally {
      await harness.shutdown();
    }
  });

  it('uses display transforms for both live and durable Harness tool events', async () => {
    const privateTool = createTool({
      id: 'privateTool',
      description: 'returns a private payload',
      inputSchema: z.object({ secret: z.string(), label: z.string() }),
      execute: async () => ({ secretOutput: 'PF_PRIVATE_TOOL_OUTPUT', count: 3 }),
    });
    let callCount = 0;
    const model = new MockLanguageModelV2({
      doStream: async () => {
        callCount++;
        return callCount === 1
          ? {
              rawCall: { rawPrompt: null, rawSettings: {} },
              warnings: [],
              stream: toolCallStream(
                'private-call-1',
                'privateTool',
                '{"secret":"PF_PRIVATE_TOOL_INPUT","label":"Safe label"}',
              ),
            }
          : {
              rawCall: { rawPrompt: null, rawSettings: {} },
              warnings: [],
              stream: textStream(['done']),
            };
      },
    });
    const agent = new Agent({
      id: 'default',
      name: 'default',
      instructions: 'use privateTool',
      model,
      tools: { privateTool },
      transform: {
        targets: ['display'],
        terminalToolResultPolicy: 'pass-through',
        transformToolPayload: context => {
          if (context.phase === 'input-available') return { label: 'Safe label' };
          if (context.phase === 'output-available') return { success: true, summary: 'Safe result', count: 3 };
          return undefined;
        },
      },
    });
    const storage = new InMemoryHarness({ db: new InMemoryDB() });
    const harness = new Harness({
      agents: { default: agent } as any,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage },
    });
    try {
      const session = await harness.session({ resourceId: 'u-redacted-tool', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(event => events.push(event));
      await session.message({ content: 'run the private tool' });
      await session._flushEventPersistence();

      const liveToolEvents = events.filter(event => event.type === 'tool_start' || event.type === 'tool_end');
      expect(liveToolEvents).toEqual([
        expect.objectContaining({ type: 'tool_start', input: { label: 'Safe label' } }),
        expect.objectContaining({
          type: 'tool_end',
          output: { success: true, summary: 'Safe result', count: 3 },
        }),
      ]);
      const replayState = await storage.getSessionEventReplayState({
        sessionId: session.id,
        resourceId: session.resourceId,
        threadId: session.threadId,
      });
      expect(replayState).not.toBeNull();
      const rows = await storage.listSessionEvents({
        sessionId: session.id,
        resourceId: session.resourceId,
        threadId: session.threadId,
        epoch: replayState!.epoch,
        afterSequence: 0,
        limit: 100,
      });
      const durableToolEvents = rows
        .map(row => row.event)
        .filter(event => event.type === 'tool_start' || event.type === 'tool_end');
      expect(durableToolEvents).toEqual(liveToolEvents);
      expect(JSON.stringify({ liveToolEvents, durableToolEvents })).not.toContain('PF_PRIVATE_TOOL');
    } finally {
      await harness.shutdown();
    }
  });
});

describe('Harness v1 real-agent E2E — replacement mode tool boundary', () => {
  it('excludes backing tools and restores the configured implementation after processor mutation', async () => {
    const modeExecute = vi.fn(async () => 'mode');
    const substitutedExecute = vi.fn(async () => 'substituted');
    const hiddenExecute = vi.fn(async () => 'hidden');
    const modeTool = createTool({
      id: 'modeTool',
      description: 'approved mode tool',
      inputSchema: z.object({}),
      execute: modeExecute,
    });
    const substitutedModeTool = createTool({
      id: 'substitutedModeTool',
      description: 'processor substitution attempt',
      inputSchema: z.object({}),
      execute: substitutedExecute,
    });
    const assignedHidden = createTool({
      id: 'assignedHidden',
      description: 'backing tool hidden by the mode',
      inputSchema: z.object({}),
      execute: hiddenExecute,
    });
    const visibleTools: string[][] = [];
    let callCount = 0;
    const model = new MockLanguageModelV2({
      doStream: async options => {
        visibleTools.push((options.tools ?? []).map(tool => tool.name));
        callCount++;
        if (callCount === 1) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream('mode-call', 'modeTool', '{}'),
          };
        }
        if (callCount === 2) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream('hidden-call', 'assignedHidden', '{}'),
          };
        }
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: textStream(['done']),
        };
      },
    });
    const agent = new Agent({
      id: 'replacement-agent',
      name: 'replacement-agent',
      instructions: 'test the replacement boundary',
      model,
      tools: { assignedHidden },
      inputProcessors: [
        {
          id: 'attempt-replacement-expansion',
          processInputStep: ({ tools }) => ({
            tools: { ...tools, modeTool: substitutedModeTool, assignedHidden },
          }),
        },
      ],
    });
    const harness = new Harness({
      agents: { default: agent } as any,
      storage: new InMemoryStore(),
      modes: [
        {
          id: 'default',
          agentId: 'default',
          tools: { modeTool },
          harnessBuiltins: 'exclude',
        },
      ],
      defaultModeId: 'default',
    });

    try {
      const session = await harness.session({ resourceId: 'replacement-user', threadId: { fresh: true } });
      const result = (await session.message({ content: 'run' })) as any;

      expect(visibleTools).toEqual([['modeTool'], ['modeTool'], ['modeTool']]);
      expect(modeExecute).toHaveBeenCalledOnce();
      expect(substitutedExecute).not.toHaveBeenCalled();
      expect(hiddenExecute).not.toHaveBeenCalled();
      expect(result.text).toBe('done');
    } finally {
      await harness.shutdown();
    }
  });

  it('captures, hides, and reapplies the replacement fence across approval suspend and resume', async () => {
    const modeExecute = vi.fn(async () => 'approved');
    const substitutedExecute = vi.fn(async () => 'substituted');
    const modeTool = Object.freeze(
      createTool({
        id: 'modeTool',
        description: 'approval-bearing mode tool',
        inputSchema: z.object({}),
        requireApproval: true,
        execute: modeExecute,
      }),
    );
    const substitutedModeTool = createTool({
      id: 'substitutedModeTool',
      description: 'processor substitution attempt',
      inputSchema: z.object({}),
      execute: substitutedExecute,
    });
    const visibleTools: string[][] = [];
    let callCount = 0;
    const model = new MockLanguageModelV2({
      doStream: async options => {
        visibleTools.push((options.tools ?? []).map(tool => tool.name));
        callCount++;
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: callCount === 1 ? toolCallStream('approval-call', 'modeTool', '{}') : textStream(['resumed done']),
        };
      },
    });
    const agent = new Agent({
      id: 'replacement-resume-agent',
      name: 'replacement-resume-agent',
      instructions: 'test replacement resume',
      model,
      inputProcessors: [
        {
          id: 'attempt-resume-substitution',
          processInputStep: ({ tools }) => ({ tools: { ...tools, modeTool: substitutedModeTool } }),
        },
      ],
    });
    const harness = new Harness({
      agents: { default: agent } as any,
      storage: new InMemoryStore(),
      modes: [
        {
          id: 'default',
          agentId: 'default',
          tools: { modeTool },
          harnessBuiltins: 'exclude',
        },
      ],
      defaultModeId: 'default',
    });

    try {
      const session = await harness.session({ resourceId: 'replacement-resume-user', threadId: { fresh: true } });
      const suspended = (await session.message({ content: 'run approval tool' })) as any;

      expect(suspended.finishReason).toBe('suspended');
      expect(session.getRecord().pendingResume?.toolSurfaceFence).toEqual(['modeTool']);
      expect(session.getDisplayState().pending).not.toHaveProperty('toolSurfaceFence');

      const resumed = (await session.respondToToolApproval({ approved: true })) as any;

      expect(resumed.text).toBe('resumed done');
      expect(visibleTools).toEqual([['modeTool'], ['modeTool']]);
      expect(modeExecute).toHaveBeenCalledOnce();
      expect(substitutedExecute).not.toHaveBeenCalled();
      expect(session.getRecord().pendingResume).toBeUndefined();
    } finally {
      await harness.shutdown();
    }
  });

  it('retains a per-call replacement tool implementation across approval suspend and resume', async () => {
    const baseExecute = vi.fn(async () => 'base');
    const ephemeralExecute = vi.fn(async () => 'approved ephemeral result');
    const mutatedExecute = vi.fn(async () => 'mutated after suspension');
    const baseTool = createTool({
      id: 'baseTool',
      description: 'replacement mode base tool',
      inputSchema: z.object({}),
      execute: baseExecute,
    });
    const ephemeralApproval = createTool({
      id: 'ephemeralApproval',
      description: 'per-call approval tool',
      inputSchema: z.object({}),
      requireApproval: true,
      execute: ephemeralExecute,
    });
    const visibleTools: string[][] = [];
    let callCount = 0;
    const model = new MockLanguageModelV2({
      doStream: async options => {
        visibleTools.push((options.tools ?? []).map(tool => tool.name));
        callCount++;
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream:
            callCount === 1
              ? toolCallStream('ephemeral-call', 'ephemeralApproval', '{}')
              : textStream(['ephemeral resumed']),
        };
      },
    });
    const agent = new Agent({
      id: 'replacement-per-call-resume-agent',
      name: 'replacement-per-call-resume-agent',
      instructions: 'test per-call replacement resume',
      model,
    });
    const harness = new Harness({
      agents: { default: agent } as any,
      storage: new InMemoryStore(),
      modes: [
        {
          id: 'default',
          agentId: 'default',
          tools: { baseTool },
          harnessBuiltins: 'exclude',
        },
      ],
      defaultModeId: 'default',
    });

    try {
      const session = await harness.session({ resourceId: 'replacement-per-call-user', threadId: { fresh: true } });
      const suspended = (await session.message({
        content: 'run ephemeral approval tool',
        additionalTools: { ephemeralApproval },
      })) as any;

      expect(suspended.finishReason).toBe('suspended');
      expect(session.getRecord().pendingResume?.toolSurfaceFence).toEqual(['baseTool', 'ephemeralApproval']);
      ephemeralApproval.execute = mutatedExecute;

      const resumed = (await session.respondToToolApproval({ approved: true })) as any;

      expect(resumed.text).toBe('ephemeral resumed');
      expect(visibleTools).toEqual([
        ['baseTool', 'ephemeralApproval'],
        ['baseTool', 'ephemeralApproval'],
      ]);
      expect(ephemeralExecute).toHaveBeenCalledOnce();
      expect(mutatedExecute).not.toHaveBeenCalled();
      expect(baseExecute).not.toHaveBeenCalled();
    } finally {
      await harness.shutdown();
    }
  });

  it('retains the replacement surface when ask_user pre-registers the pending question', async () => {
    let callCount = 0;
    const model = new MockLanguageModelV2({
      doStream: async () => ({
        rawCall: { rawPrompt: null, rawSettings: {} },
        warnings: [],
        stream:
          ++callCount === 1
            ? toolCallStream(
                'question-call',
                'ask_user',
                JSON.stringify({
                  question: 'Continue?',
                  options: [{ label: 'yes' }, { label: 'no' }],
                  selectionMode: 'single_select',
                }),
              )
            : textStream(['question resumed']),
      }),
    });
    const agent = new Agent({
      id: 'replacement-question-resume-agent',
      name: 'replacement-question-resume-agent',
      instructions: 'ask one question',
      model,
    });
    const harness = new Harness({
      agents: { default: agent } as any,
      storage: new InMemoryStore(),
      modes: [
        {
          id: 'default',
          agentId: 'default',
          tools: { ask_user: askUser },
          harnessBuiltins: 'exclude',
        },
      ],
      defaultModeId: 'default',
    });

    try {
      const session = await harness.session({ resourceId: 'replacement-question-user', threadId: { fresh: true } });
      const suspended = (await session.message({ content: 'ask before continuing' })) as any;

      expect(suspended.finishReason).toBe('suspended');
      expect(session.getRecord().pendingResume).toMatchObject({
        kind: 'question',
        toolSurfaceFence: ['ask_user'],
      });

      const resumed = (await session.respondToQuestion({ answer: 'yes' })) as any;

      expect(resumed.text).toBe('question resumed');
      expect(session.getRecord().pendingResume).toBeUndefined();
    } finally {
      await harness.shutdown();
    }
  });

  it('clears a pre-registered question when its suspended terminal cannot be published', async () => {
    let activeRunId: string | undefined;
    const preRegisteredQuestion = createTool({
      id: 'preRegisteredQuestion',
      description: 'register a question before suspending',
      inputSchema: z.object({ questionId: z.string() }),
      outputSchema: z.object({ answer: z.unknown() }),
      suspendSchema: z.object({}),
      resumeSchema: z.object({ answer: z.unknown() }),
      execute: async (_input, ctx) => {
        const input = _input as { questionId: string };
        const resumeData = ctx.agent?.resumeData as { answer: unknown } | undefined;
        if (resumeData !== undefined) return resumeData;
        const harnessContext = ctx.requestContext?.get('harness') as
          | {
              registerQuestion?: (params: {
                questionId: string;
                question: string;
                runId: string;
                toolCallId: string;
              }) => Promise<void>;
            }
          | undefined;
        if (!activeRunId || !ctx.agent?.suspend || !harnessContext?.registerQuestion) {
          throw new Error('expected a Harness agent execution context');
        }
        await harnessContext.registerQuestion({
          questionId: input.questionId,
          question: 'Continue?',
          runId: activeRunId,
          toolCallId: input.questionId,
        });
        await ctx.agent.suspend({});
        return { answer: undefined };
      },
    });
    let callCount = 0;
    const model = new MockLanguageModelV2({
      doStream: async () => {
        callCount++;
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream:
            callCount <= 2
              ? toolCallStream(
                  `failed-question-call-${callCount}`,
                  'preRegisteredQuestion',
                  JSON.stringify({ questionId: `failed-question-call-${callCount}` }),
                )
              : textStream(['question resumed after retry']),
        };
      },
    });
    const agent = new Agent({
      id: 'failed-question-terminal-agent',
      name: 'failed-question-terminal-agent',
      instructions: 'ask one question',
      model,
    });
    const pubsub = new DelayedDeliveryPubSub(25, 'run-suspended');
    const mastra = new Mastra({ agents: { default: agent }, storage: new InMemoryStore(), pubsub });
    const harness = new Harness({
      mastra,
      modes: [
        {
          id: 'default',
          agentId: 'default',
          tools: { preRegisteredQuestion },
          harnessBuiltins: 'exclude',
        },
      ],
      defaultModeId: 'default',
    });

    try {
      const session = await harness.session({ resourceId: 'failed-question-terminal-user', threadId: { fresh: true } });
      let earlyResponse: Promise<unknown> | undefined;
      session.subscribe(event => {
        if (event.type === 'agent_start') activeRunId = event.runId;
        if (event.type !== 'question_pending' || earlyResponse !== undefined) return;
        earlyResponse = session.respondToQuestion({ answer: 'yes' }).then(
          () => undefined,
          error => error,
        );
      });
      const rejection = await session.message({ content: 'ask before continuing' }).then(
        () => undefined,
        error => error,
      );

      await waitFor(() => earlyResponse !== undefined, 'early question response attempt');
      expect(earlyResponse).toBeDefined();
      await expect(earlyResponse).resolves.toMatchObject({ name: 'HarnessBusyError', code: 'harness.busy' });
      expect(rejection).toMatchObject({ name: 'HarnessExecutionError' });
      expect((rejection as { cause?: unknown }).cause).toBeInstanceOf(AgentThreadOutputDrainError);
      expect(session.getRecord().pendingResume).toBeUndefined();
      expect(session.isBusy()).toBe(false);

      const suspended = (await session.message({ content: 'ask again' })) as any;
      expect(suspended.finishReason).toBe('suspended');
      expect(session.getRecord().pendingResume).toMatchObject({
        kind: 'question',
        toolSurfaceFence: ['preRegisteredQuestion'],
      });
      const resumed = (await session.respondToQuestion({ answer: 'yes' })) as any;
      expect(resumed.text).toBe('question resumed after retry');
      expect(session.getRecord().pendingResume).toBeUndefined();
    } finally {
      await harness.shutdown();
    }
  });

  it('fails closed after the suspended run loses its process-local replacement implementations', async () => {
    const approvedExecute = vi.fn(async () => 'must not run');
    const approvalTool = createTool({
      id: 'approvalTool',
      description: 'replacement approval tool',
      inputSchema: z.object({}),
      requireApproval: true,
      execute: approvedExecute,
    });
    const model = new MockLanguageModelV2({
      doStream: async () => ({
        rawCall: { rawPrompt: null, rawSettings: {} },
        warnings: [],
        stream: toolCallStream('approval-call', 'approvalTool', '{}'),
      }),
    });
    const agent = new Agent({
      id: 'replacement-registry-loss-agent',
      name: 'replacement-registry-loss-agent',
      instructions: 'test replacement registry loss',
      model,
    });
    const storage = new InMemoryStore();
    const modes = [
      {
        id: 'default',
        agentId: 'default',
        tools: { approvalTool },
        harnessBuiltins: 'exclude' as const,
      },
    ];
    const harness = new Harness({
      agents: { default: agent } as any,
      storage,
      modes,
      defaultModeId: 'default',
    });
    let rehydratedHarness: Harness | undefined;

    try {
      const session = await harness.session({ resourceId: 'replacement-loss-user', threadId: { fresh: true } });
      const suspended = (await session.message({ content: 'run approval tool' })) as any;
      expect(suspended.finishReason).toBe('suspended');
      expect(session.getRecord().pendingResume?.toolSurfaceFence).toEqual(['approvalTool']);

      const threadId = session.threadId;
      await harness.shutdown();
      rehydratedHarness = new Harness({
        agents: { default: agent } as any,
        storage,
        modes,
        defaultModeId: 'default',
      });
      const rehydrated = await rehydratedHarness.session({
        resourceId: 'replacement-loss-user',
        threadId,
      });
      expect(rehydrated.getRecord().pendingResume?.toolSurfaceFence).toEqual(['approvalTool']);

      await expect(rehydrated.respondToToolApproval({ approved: true })).rejects.toThrow(
        /original tool implementations are no longer available/,
      );
      expect(approvedExecute).not.toHaveBeenCalled();
      expect(rehydrated.getRecord().pendingResume?.resumedAt).toBeUndefined();
    } finally {
      await rehydratedHarness?.shutdown();
      await harness.shutdown();
    }
  });
});

// ===========================================================================
// S3 — real approval suspend → respondToToolApproval → resume terminalizes
// ===========================================================================

describe('Harness v1 real-agent E2E — S3 approval suspend/resume', () => {
  it('parks and resumes an admitted streaming turn whose tool owns the review boundary', async () => {
    const appliedWrites: string[] = [];
    const reviewedWrite = createTool({
      id: 'reviewedWrite',
      description: 'write only after editable user review',
      inputSchema: z.object({ name: z.string() }),
      outputSchema: z.object({ applied: z.boolean(), name: z.string() }),
      suspendSchema: z.object({ name: z.string() }),
      resumeSchema: z.object({ approved: z.boolean() }),
      execute: async (input, ctx) => {
        const resumeData = ctx.agent?.resumeData as { approved: boolean } | undefined;
        if (resumeData === undefined) {
          if (!ctx.agent?.suspend) throw new Error('expected a Harness agent execution context');
          await ctx.agent.suspend({ name: input.name });
          return { applied: false, name: input.name };
        }
        if (resumeData.approved) appliedWrites.push(input.name);
        return { applied: resumeData.approved, name: input.name };
      },
    });
    let callCount = 0;
    const model = new MockLanguageModelV2({
      doStream: async () => {
        callCount++;
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream:
            callCount === 1
              ? toolCallStream('reviewed-write-call', 'reviewedWrite', '{"name":"Reviewed draft"}')
              : textStream(['The reviewed write was applied.']),
        };
      },
    });
    const agent = new Agent({
      id: 'default',
      name: 'default',
      instructions: 'Use reviewedWrite for the requested mutation.',
      model,
      tools: { reviewedWrite },
    });
    const harness = newHarness(agent);

    try {
      const session = await harness.session({ resourceId: 'u-reviewed-write', threadId: { fresh: true } });
      const output = await session.message({
        admissionId: 'reviewed-write-admission',
        content: 'Save the reviewed draft.',
        stream: true,
      });
      const suspended = await output.getFullOutput();

      expect(suspended.finishReason).toBe('suspended');
      await vi.waitFor(() => {
        expect(session.getRecord().pendingResume).toMatchObject({
          kind: 'tool-suspension',
          runId: suspended.runId,
          toolCallId: 'reviewed-write-call',
          toolName: 'reviewedWrite',
        });
      });
      expect(appliedWrites).toEqual([]);

      const resumed = (await session.respondToToolSuspension({
        resumeData: { approved: true },
      })) as any;
      expect(resumed.text).toBe('The reviewed write was applied.');
      expect(appliedWrites).toEqual(['Reviewed draft']);
      expect(session.getRecord().pendingResume).toBeUndefined();
    } finally {
      await harness.shutdown();
    }
  });

  it('rejects and leaves no resumable state when a suspended terminal event cannot be published', async () => {
    const approvalTool = createTool({
      id: 'failedSuspendApproval',
      description: 'approval with a rejected suspended terminal event',
      inputSchema: z.object({}),
      requireApproval: true,
      execute: async () => ({ approved: true }),
    });
    const model = new MockLanguageModelV2({
      doStream: async () => ({
        rawCall: { rawPrompt: null, rawSettings: {} },
        warnings: [],
        stream: toolCallStream('failed-suspend-call', 'failedSuspendApproval', '{}'),
      }),
    });
    const agent = new Agent({
      id: 'default',
      name: 'default',
      instructions: 'use failedSuspendApproval',
      model,
      tools: { failedSuspendApproval: approvalTool },
    });
    const pubsub = new DelayedDeliveryPubSub(0, 'run-suspended');
    const mastra = new Mastra({ agents: { default: agent }, storage: new InMemoryStore(), pubsub });
    const harness = new Harness({
      mastra,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });
    try {
      const session = await harness.session({ resourceId: 'u-suspend-failure', threadId: { fresh: true } });

      const rejection = await session.message({ content: 'request approval' }).then(
        () => undefined,
        error => error,
      );

      expect(rejection).toMatchObject({ name: 'HarnessExecutionError' });
      expect((rejection as { cause?: unknown }).cause).toBeInstanceOf(AgentThreadOutputDrainError);
      expect(((rejection as { cause: AgentThreadOutputDrainError }).cause as AgentThreadOutputDrainError).reason).toBe(
        'terminal-publish-failed',
      );
      expect(session.getRecord().pendingResume).toBeUndefined();
      expect(session.isBusy()).toBe(false);
    } finally {
      await harness.shutdown();
    }
  });

  it.each([
    ['run-completed', 1, 'terminal-publish-failed', undefined, 'before delivery'],
    ['run-registered', 2, 'registration-publish-failed', undefined, 'before delivery'],
    ['run-registered', 2, 'registration-publish-failed', 25, 'after partial delivery'],
  ] as const)(
    'terminalizes an executed resume before surfacing a %s publication failure %s',
    async (rejectedEventType, rejectedEventOccurrence, expectedReason, rejectAfterDeliveryDelayMs, _deliveryLabel) => {
      const execute = vi.fn(async () => ({ approved: true }));
      const approvalTool = createTool({
        id: 'failedCompletedTerminalApproval',
        description: 'approval whose resumed completion terminal cannot be published',
        inputSchema: z.object({}),
        requireApproval: true,
        execute,
      });
      let callCount = 0;
      const model = new MockLanguageModelV2({
        doStream: async () => {
          callCount++;
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream:
              callCount === 1
                ? toolCallStream('failed-completed-terminal-call', 'failedCompletedTerminalApproval', '{}')
                : textStream(['completed despite terminal publication failure']),
          };
        },
      });
      const agent = new Agent({
        id: 'default',
        name: 'default',
        instructions: 'use failedCompletedTerminalApproval',
        model,
        tools: { failedCompletedTerminalApproval: approvalTool },
      });
      const pubsub = new DelayedDeliveryPubSub(
        0,
        rejectedEventType,
        rejectedEventOccurrence,
        rejectAfterDeliveryDelayMs,
      );
      const mastra = new Mastra({ agents: { default: agent }, storage: new InMemoryStore(), pubsub });
      const harness = new Harness({
        mastra,
        modes: [{ id: 'default', agentId: 'default' }],
        defaultModeId: 'default',
      });
      try {
        const session = await harness.session({
          resourceId: `u-resume-${rejectedEventType}-failure-${rejectAfterDeliveryDelayMs ?? 'before'}`,
          threadId: { fresh: true },
        });
        const events: HarnessEvent[] = [];
        session.subscribe(event => events.push(event));
        const suspended = (await session.message({ content: 'request approval' })) as any;
        expect(suspended.finishReason).toBe('suspended');

        const rejection = await session.respondToToolApproval({ approved: true }).then(
          () => undefined,
          error => error,
        );

        expect(rejection).toMatchObject({ name: 'HarnessExecutionError' });
        expect((rejection as { cause?: unknown }).cause).toBeInstanceOf(AgentThreadOutputDrainError);
        expect(
          ((rejection as { cause: AgentThreadOutputDrainError }).cause as AgentThreadOutputDrainError).reason,
        ).toBe(expectedReason);
        expect(execute).toHaveBeenCalledOnce();
        expect(session.getRecord().pendingResume).toBeUndefined();
        expect(session.isBusy()).toBe(false);
        expect(
          events.filter(event => event.type === 'tool_end' && event.toolCallId === 'failed-completed-terminal-call'),
        ).toEqual([
          expect.objectContaining({
            type: 'tool_end',
            toolName: 'failedCompletedTerminalApproval',
            output: { approved: true },
            isError: false,
          }),
        ]);
        await expect(session.respondToToolApproval({ approved: true })).rejects.toBeTruthy();
        expect(execute).toHaveBeenCalledOnce();
      } finally {
        await harness.shutdown();
      }
    },
  );

  it.each([
    ['run-suspended publication', 'run-suspended', 'direct response', undefined, 'terminal-publish-failed'],
    ['run-suspended publication', 'run-suspended', 'inbox response', 'resuspend-response', 'terminal-publish-failed'],
    ['run-registered publication', 'run-registered', 'direct response', undefined, 'registration-publish-failed'],
    [
      'run-registered publication',
      'run-registered',
      'inbox response',
      'registration-response',
      'registration-publish-failed',
    ],
  ] as const)(
    'terminalizes an undeliverable re-suspension after a failed %s (%s; %s)',
    async (_failureLabel, rejectedEventType, _responseLabel, responseId, expectedReason) => {
      const { agent, firstExecute, secondExecute } = sequentialApprovalFixture();
      const pubsub = new DelayedDeliveryPubSub(0, rejectedEventType, 2);
      const mastra = new Mastra({ agents: { default: agent }, storage: new InMemoryStore(), pubsub });
      const harness = new Harness({
        mastra,
        modes: [{ id: 'default', agentId: 'default' }],
        defaultModeId: 'default',
      });
      try {
        const session = await harness.session({
          resourceId: `u-resuspend-${rejectedEventType}-failure-${responseId ?? 'direct'}`,
          threadId: { fresh: true },
        });
        const events: HarnessEvent[] = [];
        session.subscribe(event => events.push(event));
        const suspended = (await session.message({ content: 'request first approval' })) as any;
        expect(suspended.finishReason).toBe('suspended');
        const pending = session.getRecord().pendingResume!;

        const rejection = await session
          .respondToToolApproval({
            approved: true,
            ...(responseId !== undefined
              ? {
                  responseId,
                  itemId: pending.itemId ?? pending.toolCallId,
                  runId: pending.runId,
                  toolCallId: pending.toolCallId,
                  pendingRequestedAt: pending.requestedAt,
                }
              : {}),
          })
          .then(
            () => undefined,
            error => error,
          );

        expect(rejection).toMatchObject({ name: 'HarnessExecutionError' });
        expect((rejection as { cause?: unknown }).cause).toBeInstanceOf(AgentThreadOutputDrainError);
        expect((rejection as { cause: AgentThreadOutputDrainError }).cause.reason).toBe(expectedReason);
        expect(firstExecute).toHaveBeenCalledOnce();
        expect(secondExecute).not.toHaveBeenCalled();
        expect(session.getRecord().pendingResume).toBeUndefined();
        expect(session.isBusy()).toBe(false);
        if (responseId !== undefined) {
          expect(session.getRecord().inboxResponseReceipts?.[responseId]).toMatchObject({
            status: 'failed',
            retryable: false,
          });
        }
        expect(events).toContainEqual(
          expect.objectContaining({ type: 'resume_failed', runId: suspended.runId, retryable: false }),
        );
        expect(events).toContainEqual(
          expect.objectContaining({ type: 'agent_end', runId: suspended.runId, finishReason: 'error' }),
        );
        const firstToolEnds = events.filter(
          event => event.type === 'tool_end' && event.toolCallId === 'first-approval-call',
        );
        expect(firstToolEnds).toEqual([
          expect.objectContaining({
            type: 'tool_end',
            toolName: 'firstApproval',
            output: { first: true },
            isError: false,
          }),
        ]);
        const firstToolEndIndex = events.indexOf(firstToolEnds[0]!);
        const failedAgentEndIndex = events.findIndex(
          event => event.type === 'agent_end' && event.runId === suspended.runId && event.finishReason === 'error',
        );
        expect(failedAgentEndIndex).toBeGreaterThan(firstToolEndIndex);
        if (rejectedEventType === 'run-suspended') {
          const secondStartIndex = events.findIndex(
            event => event.type === 'tool_start' && event.toolCallId === 'second-approval-call',
          );
          const secondEndIndex = events.findIndex(
            event => event.type === 'tool_end' && event.toolCallId === 'second-approval-call',
          );
          const failedAgentEndAfterSecondIndex = events.findIndex(
            event => event.type === 'agent_end' && event.runId === suspended.runId && event.finishReason === 'error',
          );
          const runCompletedIndex = events.findIndex(
            event => event.type === 'run_completed' && event.runId === suspended.runId,
          );
          expect(secondStartIndex).toBeGreaterThanOrEqual(0);
          expect(secondEndIndex).toBeGreaterThan(secondStartIndex);
          expect(failedAgentEndAfterSecondIndex).toBeGreaterThan(secondEndIndex);
          expect(runCompletedIndex).toBeGreaterThan(failedAgentEndAfterSecondIndex);
          expect(events[secondEndIndex]).toMatchObject({
            type: 'tool_end',
            toolName: 'secondApproval',
            isError: true,
            output: { aborted: true },
          });
          expect(
            events.filter(event => event.type === 'tool_end' && event.toolCallId === 'second-approval-call'),
          ).toHaveLength(1);
          expect(events[runCompletedIndex]).toMatchObject({
            type: 'run_completed',
            status: 'failed',
            toolRollup: {
              errors: 1,
              perTool: {
                firstApproval: { count: 1, errors: 0 },
                secondApproval: { count: 1, errors: 1 },
              },
            },
          });
        } else {
          const runCompleted = events.find(event => event.type === 'run_completed' && event.runId === suspended.runId);
          expect(runCompleted).toMatchObject({
            type: 'run_completed',
            status: 'failed',
            toolRollup: {
              count: 1,
              errors: 0,
              perTool: { firstApproval: { count: 1, errors: 0 } },
            },
          });
        }
        await expect(session.respondToToolApproval({ approved: true })).rejects.toBeTruthy();
        expect(firstExecute).toHaveBeenCalledOnce();
        expect(secondExecute).not.toHaveBeenCalled();
      } finally {
        await harness.shutdown();
      }
    },
  );

  it('a requireApproval tool suspends the real run; respondToToolApproval resumes it to complete', async () => {
    const findUser = createTool({
      id: 'findUser',
      description: 'look up a user',
      inputSchema: z.object({ name: z.string() }),
      requireApproval: true,
      execute: async input => {
        return { name: (input as { name: string }).name, email: 'dero@mail.com' };
      },
    });

    // Step 1: tool-call (suspends on approval). Step 2: post-approval text.
    let callCount = 0;
    const model = new MockLanguageModelV2({
      doStream: async () => {
        callCount++;
        if (callCount === 1) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream('call-approve', 'findUser', '{"name":"Dero Israel"}'),
          };
        }
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: textStream(['User is ', 'Dero Israel']),
        };
      },
    });

    const agent = new Agent({
      id: 'default',
      name: 'default',
      instructions: 'use findUser',
      model,
      tools: { findUser },
      // autoResume OFF (default): the suspend must surface to the harness so
      // respondToToolApproval drives the resume.
    });
    const harness = newHarness(agent);
    try {
      const session = await harness.session({ resourceId: 'u-approve', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));

      const suspended = (await session.message({ content: 'find Dero Israel' })) as any;
      expect(suspended.finishReason).toBe('suspended');

      // Pre-resume turn events (drained from the live subscription): the
      // approval-required tool surfaced a tool_start, an explicit
      // tool_approval_required, and a terminal agent_end:suspended.
      const preResumeTypes = events.map(e => e.type);
      expect(preResumeTypes).toContain('tool_start');
      expect(preResumeTypes).toContain('tool_approval_required');
      expect(events.some(e => e.type === 'agent_end' && (e as any).finishReason === 'suspended')).toBe(true);
      const toolStart = events.find(e => e.type === 'tool_start') as { toolName: string; input: unknown };
      expect(toolStart.toolName).toBe('findUser');
      expect(toolStart.input).toEqual({ name: 'Dero Israel' });

      // A real tool suspended mid-loop → harness captured a pending resume.
      const pending = session.getRecord().pendingResume;
      expect(pending).toBeDefined();
      expect(pending!.kind).toBe('tool-approval');
      expect(pending!.toolName).toBe('findUser');
      expect(pending!.runId).toBe(suspended.runId);

      const eventsAtSuspend = events.length;

      // Approve → real resumeStream runs the approved tool + post-approval turn.
      await session.respondToToolApproval({ approved: true });

      await waitFor(
        () => events.some(e => e.type === 'agent_end' && (e as any).finishReason === 'complete'),
        'agent_end:complete after resume',
      );
      // The resume terminalized and cleared the pending registration.
      expect(session.getRecord().pendingResume).toBeUndefined();

      // §10.4 (harnessv1/sections/10-events/04-ordering-guarantees.md): a
      // suspension event is "followed by either a tool_end (after resume) or an
      // agent_end (after abort)" on the live subscriber stream. The resume run
      // reuses the suspended run's `runId`, which the long-lived thread
      // subscription releases the suspended segment's run id after its terminal
      // delivery, so the re-registered resume segment is queued on that same
      // sole consumer. Its output-drain barrier keeps the approved `tool_end`
      // and post-approval `text_delta` ahead of `agent_end:complete`. (A UI
      // streams the continuation after approval; it does not have to read the
      // tool result from display-state / FullOutput.)
      const postResumeEvents = events.slice(eventsAtSuspend);
      const postResumeTypes = postResumeEvents.map(e => e.type);

      // The approved tool's result surfaced as a live `tool_end` after resume.
      const postResumeToolEnd = postResumeEvents.find(e => e.type === 'tool_end') as
        | { toolName: string; toolCallId: string; output: any; isError: boolean }
        | undefined;
      expect(postResumeToolEnd).toBeDefined();
      expect(postResumeToolEnd!.toolName).toBe('findUser');
      expect(postResumeToolEnd!.toolCallId).toBe('call-approve');
      expect(postResumeToolEnd!.isError).toBe(false);
      expect(postResumeToolEnd!.output).toEqual({ name: 'Dero Israel', email: 'dero@mail.com' });

      // The post-approval model text surfaced as live `text_delta`s.
      const postResumeText = (postResumeEvents.filter(e => e.type === 'text_delta') as Array<{ delta: string }>)
        .map(e => e.delta)
        .join('');
      expect(postResumeText).toBe('User is Dero Israel');

      // §10.4 ordering: the live `tool_end` precedes the terminal
      // `agent_end:complete` on the subscriber stream.
      const resumeToolEndIdx = postResumeTypes.indexOf('tool_end');
      const resumeAgentEndIdx = postResumeTypes.lastIndexOf('agent_end');
      expect(resumeToolEndIdx).toBeGreaterThanOrEqual(0);
      expect(resumeToolEndIdx).toBeLessThan(resumeAgentEndIdx);

      // After the resume terminalizes, the display state's pending slot clears
      // (the captured approval registration is consumed).
      expect(session.getDisplayState().pending == null).toBe(true);
    } finally {
      await harness.shutdown();
    }
  });

  it('synthesizes once after an approved tool resumes into a silent terminal response', async () => {
    const execute = vi.fn(async () => ({ approved: true }));
    const approvalTool = createTool({
      id: 'silentApproval',
      description: 'approval followed by a silent model response',
      inputSchema: z.object({}),
      requireApproval: true,
      execute,
    });
    let providerCalls = 0;
    const providerOptions: any[] = [];
    const model = new MockLanguageModelV2({
      doStream: async options => {
        providerOptions.push(options);
        providerCalls += 1;
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream:
            providerCalls === 1
              ? toolCallStream('silent-approval-call', 'silentApproval', '{}')
              : providerCalls === 2
                ? textStream([])
                : textStream(['Approved action completed.']),
        };
      },
    });
    const agent = new Agent({
      id: 'default',
      name: 'default',
      instructions: 'use silentApproval and report its outcome',
      model,
      tools: { silentApproval: approvalTool },
    });
    const harness = newHarness(agent);
    try {
      const session = await harness.session({ resourceId: 'u-silent-approval', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(event => events.push(event));

      const suspended = (await session.message({ content: 'run the approved action' })) as any;
      expect(suspended.finishReason).toBe('suspended');
      const eventsAtSuspend = events.length;

      await session.respondToToolApproval({ approved: true });

      const resumedEvents = events.slice(eventsAtSuspend);
      const visibleText = (resumedEvents.filter(event => event.type === 'text_delta') as Array<{ delta: string }>)
        .map(event => event.delta)
        .join('');
      expect(visibleText).toBe('Approved action completed.');
      expect(execute).toHaveBeenCalledOnce();
      expect(providerCalls).toBe(3);
      expect(providerOptions[2]?.tools ?? []).toHaveLength(0);
      expect(session.getRecord().pendingResume).toBeUndefined();
    } finally {
      await harness.shutdown();
    }
  });

  it('waits for delayed subscription delivery before terminalizing a resumed run', async () => {
    const approvalTool = createTool({
      id: 'delayedApproval',
      description: 'approval over delayed pubsub',
      inputSchema: z.object({}),
      requireApproval: true,
      execute: async () => ({ approved: true }),
    });
    let callCount = 0;
    const model = new MockLanguageModelV2({
      doStream: async () => {
        callCount++;
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream:
            callCount === 1
              ? toolCallStream('delayed-approval-call', 'delayedApproval', '{}')
              : textStream(['delayed ', 'resume']),
        };
      },
    });
    const agent = new Agent({
      id: 'default',
      name: 'default',
      instructions: 'use delayedApproval',
      model,
      tools: { delayedApproval: approvalTool },
    });
    const pubsub = new DelayedDeliveryPubSub(15);
    const mastra = new Mastra({ agents: { default: agent }, storage: new InMemoryStore(), pubsub });
    const harness = new Harness({
      mastra,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });
    try {
      const session = await harness.session({ resourceId: 'u-delayed', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(event => events.push(event));
      const suspended = (await session.message({ content: 'run delayed approval' })) as any;
      expect(suspended.finishReason).toBe('suspended');
      const eventsAtSuspend = events.length;

      await session.respondToToolApproval({ approved: true });

      const resumedEvents = events.slice(eventsAtSuspend);
      const resumedTypes = resumedEvents.map(event => event.type);
      const toolEndIndex = resumedTypes.indexOf('tool_end');
      const lastTextIndex = resumedTypes.lastIndexOf('text_delta');
      const agentEndIndex = resumedTypes.lastIndexOf('agent_end');
      expect(toolEndIndex).toBeGreaterThanOrEqual(0);
      expect(lastTextIndex).toBeGreaterThan(toolEndIndex);
      expect(agentEndIndex).toBeGreaterThan(lastTextIndex);
      expect(
        resumedEvents
          .slice(agentEndIndex + 1)
          .filter(event => event.type === 'tool_start' || event.type === 'tool_end' || event.type === 'text_delta'),
      ).toEqual([]);
      expect(session.getRecord().pendingResume).toBeUndefined();
    } finally {
      await harness.shutdown();
    }
  });
});

// ===========================================================================
// S4 — signal_completed / queue_completed settlement evidence
// ===========================================================================

describe('Harness v1 real-agent E2E — S4 settlement evidence', () => {
  it('signal() settles signal_completed and lookupMessageResult reports completed', async () => {
    const model = new MockLanguageModelV2({
      doStream: async () => ({
        rawCall: { rawPrompt: null, rawSettings: {} },
        warnings: [],
        stream: textStream(['done']),
      }),
    });
    const agent = new Agent({ id: 'default', name: 'default', instructions: 'reply', model });
    const harness = newHarness(agent);
    try {
      const session = await harness.session({ resourceId: 'u-signal', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));

      const handle = await session.signal({ content: 'go' });
      const result = await handle.result;
      expect(result.finishReason).toBe('stop');

      await waitFor(() => events.some(e => e.type === 'signal_completed'), 'signal_completed');
      const completed = events.find(e => e.type === 'signal_completed') as { runId: string } | undefined;
      expect(completed).toBeDefined();
      expect(typeof completed!.runId).toBe('string');
      expect(completed!.runId.length).toBeGreaterThan(0);

      const lookup = await session.lookupMessageResult(handle.id);
      expect(lookup && 'status' in lookup ? lookup.status : null).toBe('completed');
    } finally {
      await harness.shutdown();
    }
  });

  it('queue() settles queue_completed carrying queuedItemId + signalId', async () => {
    const model = new MockLanguageModelV2({
      doStream: async () => ({
        rawCall: { rawPrompt: null, rawSettings: {} },
        warnings: [],
        stream: textStream(['queued reply']),
      }),
    });
    const agent = new Agent({ id: 'default', name: 'default', instructions: 'reply', model });
    const harness = newHarness(agent);
    try {
      const session = await harness.session({ resourceId: 'u-queue', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));

      const result = (await session.queue({ content: 'do work' })) as any;
      expect(result.text).toBe('queued reply');

      await waitFor(() => events.some(e => e.type === 'queue_completed'), 'queue_completed');
      const completed = events.find(e => e.type === 'queue_completed') as
        | { queuedItemId: string; signalId: string; runId: string }
        | undefined;
      expect(completed).toBeDefined();
      expect(completed!.queuedItemId.length).toBeGreaterThan(0);
      expect(completed!.signalId.length).toBeGreaterThan(0);
      expect(completed!.runId.length).toBeGreaterThan(0);
      expect(session.getRecord().pendingQueue).toEqual([]);
    } finally {
      await harness.shutdown();
    }
  });

  it.each([
    ['run-suspended', 'terminal-publish-failed'],
    ['run-registered', 'registration-publish-failed'],
  ] as const)(
    'fails owned signal evidence when a resumed segment cannot publish %s',
    async (rejectedEventType, expectedReason) => {
      const { agent, firstExecute, secondExecute } = sequentialApprovalFixture();
      const pubsub = new DelayedDeliveryPubSub(0, rejectedEventType, 2);
      const mastra = new Mastra({ agents: { default: agent }, storage: new InMemoryStore(), pubsub });
      const harness = new Harness({
        mastra,
        modes: [{ id: 'default', agentId: 'default' }],
        defaultModeId: 'default',
      });
      try {
        const session = await harness.session({
          resourceId: `u-resuspend-signal-${rejectedEventType}-failure`,
          threadId: { fresh: true },
        });
        const events: HarnessEvent[] = [];
        session.subscribe(event => events.push(event));

        const handle = await session.signal({ content: 'request first approval' });
        const suspended = await handle.result;
        expect(suspended.finishReason).toBe('suspended');
        await waitFor(
          () => session.getRecord().pendingResume !== undefined && !session.isRunning(),
          'owned signal suspension to become resumable',
        );

        const rejection = await session.respondToToolApproval({ approved: true }).then(
          () => undefined,
          error => error,
        );

        expect(rejection).toMatchObject({ name: 'HarnessExecutionError' });
        expect((rejection as { cause?: unknown }).cause).toBeInstanceOf(AgentThreadOutputDrainError);
        expect((rejection as { cause: AgentThreadOutputDrainError }).cause.reason).toBe(expectedReason);
        expect(firstExecute).toHaveBeenCalledOnce();
        expect(secondExecute).not.toHaveBeenCalled();
        expect(session.getRecord().pendingResume).toBeUndefined();
        expect(session.isBusy()).toBe(false);
        expect(await session.lookupMessageResult(handle.id)).toMatchObject({
          status: 'failed',
          signalId: handle.id,
          runId: handle.runId,
        });
        expect(events).toContainEqual(
          expect.objectContaining({ type: 'signal_failed', signalId: handle.id, runId: handle.runId }),
        );
        expect(events).toContainEqual(
          expect.objectContaining({ type: 'resume_failed', runId: handle.runId, retryable: false }),
        );
        await expect(session.respondToToolApproval({ approved: true })).rejects.toBeTruthy();
        expect(firstExecute).toHaveBeenCalledOnce();
        expect(secondExecute).not.toHaveBeenCalled();
      } finally {
        await harness.shutdown();
      }
    },
  );

  it.each([
    ['run-suspended', 'terminal-publish-failed'],
    ['run-registered', 'registration-publish-failed'],
  ] as const)(
    'fails and removes queued work when a resumed segment cannot publish %s',
    async (rejectedEventType, expectedReason) => {
      const { agent, firstExecute, secondExecute } = sequentialApprovalFixture();
      const pubsub = new DelayedDeliveryPubSub(0, rejectedEventType, 2);
      const mastra = new Mastra({ agents: { default: agent }, storage: new InMemoryStore(), pubsub });
      const harness = new Harness({
        mastra,
        modes: [{ id: 'default', agentId: 'default' }],
        defaultModeId: 'default',
      });
      try {
        const session = await harness.session({
          resourceId: `u-resuspend-queue-${rejectedEventType}-failure`,
          threadId: { fresh: true },
        });
        const events: HarnessEvent[] = [];
        session.subscribe(event => events.push(event));
        const queuedOutcome = session.queue({ content: 'request first approval' }).then(
          value => ({ value }),
          error => ({ error }),
        );
        await waitFor(
          () => session.getRecord().pendingResume?.queuedItemId !== undefined && !session.isRunning(),
          'queued suspension to become resumable',
        );
        const pending = session.getRecord().pendingResume!;
        const queuedItemId = pending.queuedItemId!;

        const rejection = await session.respondToToolApproval({ approved: true }).then(
          () => undefined,
          error => error,
        );
        const outcome = await queuedOutcome;

        expect(rejection).toMatchObject({ name: 'HarnessExecutionError' });
        expect((rejection as { cause?: unknown }).cause).toBeInstanceOf(AgentThreadOutputDrainError);
        expect((rejection as { cause: AgentThreadOutputDrainError }).cause.reason).toBe(expectedReason);
        expect(outcome).toEqual({ error: expect.objectContaining({ code: 'harness.internal' }) });
        expect(firstExecute).toHaveBeenCalledOnce();
        expect(secondExecute).not.toHaveBeenCalled();
        expect(session.getRecord().pendingResume).toBeUndefined();
        expect(session.getRecord().pendingQueue).toEqual([]);
        expect(session.isBusy()).toBe(false);
        expect(await session.lookupQueueResult(queuedItemId)).toMatchObject({
          status: 'failed',
          queuedItemId,
          runId: pending.runId,
        });
        expect(events).toContainEqual(
          expect.objectContaining({ type: 'queue_failed', queuedItemId, runId: pending.runId }),
        );
        expect(events).toContainEqual(
          expect.objectContaining({ type: 'resume_failed', runId: pending.runId, retryable: false }),
        );
        expect(events.filter(event => event.type === 'tool_end' && event.toolCallId === 'first-approval-call')).toEqual(
          [
            expect.objectContaining({
              type: 'tool_end',
              toolName: 'firstApproval',
              output: { first: true },
              isError: false,
              queuedItemId,
            }),
          ],
        );
        for (const terminalEvent of events.filter(
          event =>
            event.runId === pending.runId &&
            (event.type === 'resume_failed' || event.type === 'agent_end' || event.type === 'run_completed'),
        )) {
          expect(terminalEvent).toMatchObject({ queuedItemId });
        }
        if (rejectedEventType === 'run-suspended') {
          expect(events).toContainEqual(
            expect.objectContaining({
              type: 'tool_end',
              toolCallId: 'second-approval-call',
              queuedItemId,
              isError: true,
            }),
          );
        }
        await expect(session.respondToToolApproval({ approved: true })).rejects.toBeTruthy();
        expect(firstExecute).toHaveBeenCalledOnce();
        expect(secondExecute).not.toHaveBeenCalled();
      } finally {
        await harness.shutdown();
      }
    },
  );

  it('force-closes streamed tools and releases a queued resume when undeliverable terminalization storage fails', async () => {
    const { agent, firstExecute, secondExecute } = sequentialApprovalFixture();
    const pubsub = new DelayedDeliveryPubSub(0, 'run-suspended', 2);
    const mastra = new Mastra({ agents: { default: agent }, storage: new InMemoryStore(), pubsub });
    const harness = new Harness({
      mastra,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });
    try {
      const session = await harness.session({
        resourceId: 'u-resuspend-queue-terminalization-storage-failure',
        threadId: { fresh: true },
      });
      const events: HarnessEvent[] = [];
      session.subscribe(event => events.push(event));
      const firstOutcome = session.queue({ content: 'request first approval' }).then(
        value => ({ value }),
        error => ({ error }),
      );
      await waitFor(
        () => session.getRecord().pendingResume?.queuedItemId !== undefined && !session.isRunning(),
        'first queued suspension to become resumable',
      );
      const firstPending = session.getRecord().pendingResume!;
      const firstQueuedItemId = firstPending.queuedItemId!;
      const secondOutcome = session.queue({ content: 'next queued item' });

      vi.spyOn(session as any, '_terminalizeUndeliverableResuspension').mockRejectedValueOnce(
        new Error('injected terminalization storage failure'),
      );
      const rejection = await session.respondToToolApproval({ approved: true }).then(
        () => undefined,
        error => error,
      );

      expect(rejection).toMatchObject({ name: 'HarnessExecutionError' });
      expect(firstExecute).toHaveBeenCalledOnce();
      expect(secondExecute).not.toHaveBeenCalled();
      expect(session.getRecord().pendingResume).toMatchObject({
        queuedItemId: firstQueuedItemId,
        resumedAt: expect.any(Number),
      });
      const secondToolStartIndex = events.findIndex(
        event => event.type === 'tool_start' && event.toolCallId === 'second-approval-call',
      );
      const secondToolEnds = events.filter(
        event => event.type === 'tool_end' && event.toolCallId === 'second-approval-call',
      );
      expect(secondToolStartIndex).toBeGreaterThanOrEqual(0);
      expect(secondToolEnds).toEqual([
        expect.objectContaining({
          type: 'tool_end',
          toolName: 'secondApproval',
          queuedItemId: firstQueuedItemId,
          isError: true,
          output: expect.objectContaining({ aborted: true }),
        }),
      ]);
      expect(events.indexOf(secondToolEnds[0]!)).toBeGreaterThan(secondToolStartIndex);
      expect({
        currentQueuedItemId: (session as any)._currentQueuedItemId,
        draining: (session as any)._draining,
        hasActiveTurn: (session as any)._currentTurnAbortController !== undefined,
        hasRecoveryTimer: (session as any)._queuedResumeRecoveryTimer !== undefined,
      }).toEqual({
        // The recovery kick releases the completed resume owner, then the
        // durable pending re-establishes this correlation while it waits for
        // the stale deadline. The armed timer—not an external API call—owns
        // the next transition.
        currentQueuedItemId: firstQueuedItemId,
        draining: false,
        hasActiveTurn: false,
        hasRecoveryTimer: true,
      });

      await (session as any)._flushUpdate((record: any) => ({
        ...record,
        pendingResume: {
          ...record.pendingResume,
          resumedAt: Date.now() - 30_001,
          resumeRecoveryAt: Date.now() - 1,
        },
      }));
      await session._kickQueueDrain();

      expect(await firstOutcome).toEqual({
        error: expect.objectContaining({ code: 'harness.resume_recovery_stale' }),
      });
      await expect(secondOutcome).resolves.toMatchObject({ text: 'unexpected retry' });
      expect(session.getRecord().pendingResume).toBeUndefined();
      expect(session.getRecord().pendingQueue).toEqual([]);
      expect(session.getRecord().queueAdmissionReceipts?.[firstQueuedItemId]).toMatchObject({ status: 'failed' });
      expect(firstExecute).toHaveBeenCalledOnce();
      expect(secondExecute).not.toHaveBeenCalled();
    } finally {
      await harness.shutdown();
    }
  });
});

// ===========================================================================
// S5 — provider/model error (before run start AND mid-stream)
// ===========================================================================

describe('Harness v1 real-agent E2E — S5 provider error', () => {
  it('(a) error BEFORE the stream: agent_end:error fires (redacted boundary) AND message() rejects with a REDACTED error (raw detail only on .cause)', async () => {
    const RAW = 'connect ECONNREFUSED 10.0.0.5:5432 — secret provider detail';
    const model = new MockLanguageModelV2({
      doStream: async () => {
        throw new Error(RAW);
      },
    });
    const agent = new Agent({ id: 'default', name: 'default', instructions: 'reply', model });
    const harness = newHarness(agent);
    try {
      const session = await harness.session({ resourceId: 'u-err-pre', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));

      const thrown = await session.message({ content: 'hi' }).then(
        () => undefined,
        (e: unknown) => e,
      );

      // The default-path message() rejects (it does not swallow a pre-stream
      // provider failure).
      expect(thrown).toBeInstanceOf(Error);

      // The turn-event boundary IS the redacted boundary: agent_start then a
      // terminal agent_end with finishReason 'error' (no spurious complete).
      await waitFor(
        () => events.some(e => e.type === 'agent_end' && (e as any).finishReason === 'error'),
        'agent_end:error (pre-stream)',
        4000,
      );
      const ended = events.find(e => e.type === 'agent_end') as { finishReason: string };
      expect(ended.finishReason).toBe('error');
      expect(events.some(e => e.type === 'agent_end' && (e as any).finishReason === 'complete')).toBe(false);

      // §13.3f.1 (harnessv1/.../06-error-envelope.md, 07-... ): `message()` is a
      // public §4.2b boundary, and SDK promise rejections are explicitly among
      // the surfaces that must expose only a redacted projection — raw
      // driver/SQL/path/`err.message` text MUST NOT cross it. The harness wraps a
      // raw provider failure in a `HarnessExecutionError` (the same
      // both-satisfying shape as `HarnessStorageError`): its public `.message` is
      // the generic, already-redacted `harness.internal` text, and the raw
      // original is kept LOCAL-ONLY on `.cause` so the trusted Doxa caller still
      // has full fidelity for logging / retry classification. A naive caller that
      // surfaces `.message` (e.g. spawn-subagent-tool projecting a child failure)
      // therefore cannot leak provider detail.
      expect((thrown as Error).name).toBe('HarnessExecutionError');
      expect((thrown as Error).message).toBe('An internal harness error occurred');
      expect((thrown as Error).message).not.toContain('ECONNREFUSED');
      // The raw provider detail is preserved local-only on `.cause`.
      const cause = (thrown as { cause?: unknown }).cause as Error | undefined;
      expect(cause).toBeInstanceOf(Error);
      expect(cause!.message).toBe(RAW);
      expect(cause!.message).toContain('ECONNREFUSED');
    } finally {
      await harness.shutdown();
    }
  });

  it('(b) error DURING the stream emits partial text_delta then agent_end:error', async () => {
    const model = new MockLanguageModelV2({
      doStream: async () => ({
        rawCall: { rawPrompt: null, rawSettings: {} },
        warnings: [],
        stream: convertArrayToReadableStream([
          { type: 'stream-start', warnings: [] },
          { type: 'response-metadata', id: 'id-mid', modelId: 'mock-model-id', timestamp: new Date(0) },
          { type: 'text-start', id: 'text-1' },
          { type: 'text-delta', id: 'text-1', delta: 'partial' },
          { type: 'error', error: new Error('mid-stream provider blowup — host db.internal:5432') },
        ]),
      }),
    });
    const agent = new Agent({ id: 'default', name: 'default', instructions: 'reply', model });
    const harness = newHarness(agent);
    try {
      const session = await harness.session({ resourceId: 'u-err-mid', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));

      await session.message({ content: 'hi' }).catch(() => {});

      await waitFor(() => events.some(e => e.type === 'agent_end'), 'agent_end (mid-stream error)', 4000);
      const textDeltas = events.filter(e => e.type === 'text_delta') as Array<{ delta: string }>;
      // The partial delta emitted before the error chunk must have surfaced.
      expect(textDeltas.map(e => e.delta).join('')).toContain('partial');
      const ended = events.find(e => e.type === 'agent_end') as { finishReason: string };
      // A mid-stream error terminalizes as error (not complete).
      expect(['error', 'aborted']).toContain(ended.finishReason);
      // No spurious 'complete' terminal for an errored run.
      expect(events.filter(e => e.type === 'agent_end' && (e as any).finishReason === 'complete')).toHaveLength(0);
    } finally {
      await harness.shutdown();
    }
  });
});

// ===========================================================================
// S6 — abort terminalizes the real run
//
// Three distinct real behaviors, all verified at runtime against the real Agent
// over InMemoryStore (instrumented probe):
//
//   (a) EXTERNAL active-work abort — abortActiveWork() fires while a provider
//       stream is live. The provider observes its abort signal, errors with an
//       AbortError, and the AI SDK may append a synthetic `finish:stop` while
//       unwinding. The earlier abort remains authoritative: the full output is
//       aborted and the Harness terminal is interrupted, never completed.
//
//   (b) MID-RUN abort — abort fired from INSIDE a running tool, AFTER the run is
//       dispatched. This is the path that exercises the real
//       `turnAbortController`: the live turn aborts and the harness emits a
//       terminal `agent_end:aborted` (observed sequence:
//       agent_start → tool_start → agent_end:aborted → tool_end). `message()`
//       still rejects with the `agent_aborted` error (the active-turn waiter
//       loses the race), but the genuine terminal event fires. This is the
//       coverage the file header claims, and it catches a regression if the
//       loop ever stops terminalizing an aborted run.
//
//   (c) ALREADY-aborted caller signal — `_beginTurn` (session.ts:1397) aborts
//       the turn controller synchronously before the run is ever dispatched, so
//       `message()` rejects pre-dispatch with `agent_aborted` and emits ZERO
//       harness events (no agent_start, no agent_end). This characterizes the
//       real "nothing was launched" contract; it must NOT pretend a terminal
//       `agent_end:aborted` was reached, because none is.
// ===========================================================================

describe('Harness v1 real-agent E2E — S6 abort', () => {
  it('(a) abortActiveWork while the provider streams terminalizes the real run as interrupted', async () => {
    let providerAbortSignal: AbortSignal | undefined;
    const model = new MockLanguageModelV2({
      doStream: async options => {
        providerAbortSignal = options.abortSignal;
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: new ReadableStream({
            start(controller) {
              controller.enqueue({ type: 'stream-start', warnings: [] });
              controller.enqueue({
                type: 'response-metadata',
                id: 'id-external-abort',
                modelId: 'mock-model-id',
                timestamp: new Date(0),
              });
              controller.enqueue({ type: 'text-start', id: 'text-external-abort' });
              controller.enqueue({
                type: 'text-delta',
                id: 'text-external-abort',
                delta: 'partial response',
              });

              const abort = () => {
                const error = new Error('Aborted');
                error.name = 'AbortError';
                controller.error(error);
              };
              if (options.abortSignal?.aborted) abort();
              else options.abortSignal?.addEventListener('abort', abort, { once: true });
            },
          }),
        };
      },
    });
    const agent = new Agent({ id: 'default', name: 'default', instructions: 'reply', model });
    const harness = newHarness(agent);
    try {
      const session = await harness.session({ resourceId: 'u-abort-active-work', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(event => events.push(event));

      const output = await session.message({ content: 'go', stream: true });
      await waitFor(() => events.some(event => event.type === 'text_delta'), 'provider text before active-work abort');
      expect(providerAbortSignal?.aborted).toBe(false);

      await session.abortActiveWork({ reason: 'user_requested', settleTimeoutMs: 2_000 });
      expect(providerAbortSignal?.aborted).toBe(true);

      const full = await output.getFullOutput();
      expect(full.finishReason).toBe('aborted');
      await waitFor(
        () => events.some(event => event.type === 'run_completed'),
        'run_completed (external active-work abort)',
      );
      const runId = full.runId;
      expect(events.filter(event => event.type === 'agent_end' && event.runId === runId)).toEqual([
        expect.objectContaining({ finishReason: 'aborted' }),
      ]);
      expect(events.filter(event => event.type === 'run_completed' && event.runId === runId)).toEqual([
        expect.objectContaining({ finishReason: 'aborted', status: 'interrupted' }),
      ]);
      expect(
        events.filter(
          event =>
            event.runId === runId &&
            ((event.type === 'agent_end' && event.finishReason === 'complete') ||
              (event.type === 'run_completed' && event.status === 'completed')),
        ),
      ).toHaveLength(0);
    } finally {
      await harness.shutdown();
    }
  });

  it('(b) a MID-RUN abort (from inside a running tool) terminalizes the real run as agent_end:aborted', async () => {
    const ac = new AbortController();
    // The tool aborts the run while it is executing, then yields so the loop
    // observes the abort mid-flight (real turnAbortController path).
    const abortingTool = createTool({
      id: 'abortNow',
      description: 'aborts the in-flight run',
      inputSchema: z.object({}),
      execute: async () => {
        ac.abort();
        await new Promise(resolve => setTimeout(resolve, 20));
        return { aborted: true };
      },
    });

    // Step 1: a tool-call that triggers the abort. Step 2 would be post-tool
    // text — it must never be reached because the run aborts inside the tool.
    let callCount = 0;
    const model = new MockLanguageModelV2({
      doStream: async () => {
        callCount++;
        if (callCount === 1) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream('call-abort', 'abortNow', '{}'),
          };
        }
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: textStream(['should not be reached']),
        };
      },
    });

    const agent = new Agent({
      id: 'default',
      name: 'default',
      instructions: 'use abortNow',
      model,
      tools: { abortNow: abortingTool },
    });
    const harness = newHarness(agent);
    try {
      const session = await harness.session({ resourceId: 'u-abort-mid', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));

      // The default-path message() rejects when the live turn is aborted: the
      // active-turn waiter rejects with the `agent_aborted` error.
      const thrown = await session.message({ content: 'go', abortSignal: ac.signal }).then(
        () => undefined,
        (e: unknown) => e,
      );
      expect(thrown).toBeInstanceOf(Error);
      expect((thrown as Error).message).toContain('agent_aborted');

      // The real turnAbortController path emitted a terminal agent_end:aborted.
      // No `.catch(()=>{})` swallow and no `if (ended)` guard — a missing or
      // wrong-reason terminal FAILS the test (this is the regression the
      // scenario is meant to catch).
      await waitFor(() => events.some(e => e.type === 'agent_end'), 'agent_end (mid-run abort)');
      const ended = events.find(e => e.type === 'agent_end') as { finishReason: string };
      expect(ended).toBeDefined();
      expect(ended.finishReason).toBe('aborted');

      // The run was actually dispatched: agent_start preceded the abort terminal.
      const types = events.map(e => e.type);
      expect(types.indexOf('agent_start')).toBeGreaterThanOrEqual(0);
      expect(types.indexOf('agent_start')).toBeLessThan(types.indexOf('agent_end'));
      // The aborting tool actually started inside the loop before the terminal.
      expect(types).toContain('tool_start');
      // No spurious complete terminal for an aborted turn.
      expect(events.filter(e => e.type === 'agent_end' && (e as any).finishReason === 'complete')).toHaveLength(0);
    } finally {
      await harness.shutdown();
    }
  });

  it('(c) an ALREADY-aborted caller signal rejects message() pre-dispatch with agent_aborted and emits NO harness events', async () => {
    const model = new MockLanguageModelV2({
      doStream: async () => ({
        rawCall: { rawPrompt: null, rawSettings: {} },
        warnings: [],
        stream: textStream(['should not finish']),
      }),
    });
    const agent = new Agent({ id: 'default', name: 'default', instructions: 'reply', model });
    const harness = newHarness(agent);
    try {
      const session = await harness.session({ resourceId: 'u-abort', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));

      const ac = new AbortController();
      ac.abort();

      // `_beginTurn` aborts the turn controller synchronously before dispatch,
      // so message() rejects with the `agent_aborted` error — verified at
      // runtime, NOT swallowed.
      const thrown = await session.message({ content: 'go', abortSignal: ac.signal }).then(
        () => undefined,
        (e: unknown) => e,
      );
      expect(thrown).toBeInstanceOf(Error);
      expect((thrown as Error).message).toContain('agent_aborted');

      // Let any scheduled setTimeout(0) abort-fanout flush, then assert NO run
      // was ever dispatched: zero events, in particular no agent_start and no
      // agent_end. (This is the real contract; the prior S6 masked a guaranteed
      // 4s waitFor timeout and asserted nothing.)
      await new Promise(resolve => setTimeout(resolve, 10));
      expect(events).toHaveLength(0);
      expect(events.some(e => e.type === 'agent_start')).toBe(false);
      expect(events.some(e => e.type === 'agent_end')).toBe(false);
    } finally {
      await harness.shutdown();
    }
  });
});

// ===========================================================================
// S7 — REAL parent agent spawns a REAL subagent through the genuine
//      spawn_subagent → child.message() → ai-sdk loop path, and the CHILD's
//      streamed events surface on the PARENT's subscriber as `subagent_*`.
//
// Every prior subagent test (session.spawn-subagent.test.ts,
// session.subagent-events.test.ts) drives this surface with a `FakeAgent`
// (a duck-typed Agent that overrides `stream` to hand back a fabricated
// `MastraModelOutput`) or by calling `_emitSubagentEvent` directly. NONE of
// them exercise the real chain:
//
//   parent model emits a spawn_subagent tool-call
//     → the built-in spawn tool runs `child.message({ content: task })`
//       → the REAL child Agent runs its OWN deterministic-model loop
//         → the child's real `text-delta` / `tool-call` / `tool-result`
//           chunks flow through the child session's `_emitForChunk`
//           → the child session publishes real `agent_start` / `text_delta`
//             / `tool_start` / `tool_end` harness events
//             → the spawn tool's `child.subscribe(...)` bridge re-emits them
//               as `subagent_*` on the PARENT via `_emitSubagentEvent`.
//
// Both agents are the real `Agent` class; only their LANGUAGE MODELS are the
// deterministic `MockLanguageModelV2` provider mock. No `FakeAgent` /
// `MockAgent` here.
// ===========================================================================

/**
 * A raw provider stream that emits a tool-result-bearing turn: a `tool-call`
 * to `toolName`, then finishes `tool-calls`. (Same shape as `toolCallStream`,
 * kept distinct for readability in the child's two-call sequence.)
 */
function childToolCallStream(toolCallId: string, toolName: string, inputJson: string) {
  return toolCallStream(toolCallId, toolName, inputJson);
}

describe('Harness v1 real-agent E2E — S7 real subagent streaming', () => {
  it('completes a child outcome report when its mode excludes optional harness builtins', async () => {
    const readFact = vi.fn(async () => ({ answer: 42 }));
    const lookupFact = createTool({
      id: 'lookupFact',
      description: 'read the answer',
      inputSchema: z.object({}),
      execute: readFact,
    });
    const childToolSurfaces: string[][] = [];
    let childCalls = 0;
    const childAgent = new Agent({
      id: 'child-agent',
      name: 'child-agent',
      instructions: 'Read the fact, then report the completed outcome.',
      model: new MockLanguageModelV2({
        doStream: async options => {
          childToolSurfaces.push((options.tools ?? []).map(tool => tool.name).sort());
          childCalls++;
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream:
              childCalls === 1
                ? toolCallStream('read-fact', 'lookupFact', '{}')
                : childCalls === 2
                  ? toolCallStream(
                      'child-outcome',
                      'report_subagent_outcome',
                      JSON.stringify({
                        outcome: 'completed',
                        summary: 'The answer is 42.',
                        evidence: [
                          {
                            kind: 'tool-result',
                            toolName: 'lookupFact',
                            toolCallId: 'read-fact',
                            status: 'success',
                            description: 'The fact read returned 42.',
                          },
                        ],
                      }),
                    )
                  : textStream(['The outcome report did not complete.']),
          };
        },
      }),
    });
    let parentCalls = 0;
    const parentAgent = new Agent({
      id: 'parent-agent',
      name: 'parent-agent',
      instructions: 'Delegate the fact lookup.',
      model: new MockLanguageModelV2({
        doStream: async () => {
          parentCalls++;
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream:
              parentCalls === 1
                ? toolCallStream(
                    'parent-spawn',
                    'spawn_subagent',
                    JSON.stringify({ agentType: 'reader', task: 'Read the answer.', delivery: 'final' }),
                  )
                : textStream(['The delegation did not complete.']),
          };
        },
      }),
    });
    const harness = new Harness({
      agents: { 'parent-agent': parentAgent, 'child-agent': childAgent },
      storage: new InMemoryStore(),
      modes: [
        { id: 'default', agentId: 'parent-agent' },
        {
          id: 'read-only',
          agentId: 'child-agent',
          tools: { lookupFact },
          harnessBuiltins: 'exclude',
          permissions: { categories: {}, tools: { lookupFact: 'allow' } },
        },
      ],
      defaultModeId: 'default',
      subagents: {
        maxDepth: 2,
        types: {
          reader: {
            agentId: 'child-agent',
            modeId: 'read-only',
            description: 'Read a fact',
            toolAllowlist: ['lookupFact'],
          },
        },
      },
    });

    try {
      const session = await harness.session({ resourceId: 'reader-user', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(event => events.push(event));
      const result = (await session.message({ content: 'Delegate the answer lookup.' })) as any;

      expect(childToolSurfaces).toEqual([
        ['lookupFact', 'report_subagent_outcome'],
        ['lookupFact', 'report_subagent_outcome'],
      ]);
      expect(readFact).toHaveBeenCalledOnce();
      expect(childCalls).toBe(2);
      expect(parentCalls).toBe(1);
      expect(result.text).toBe('The answer is 42.');
      expect(result.terminalToolResult).toMatchObject({
        status: 'success',
        items: [{ toolName: 'spawn_subagent', value: { kind: 'subagent-direct-answer', text: 'The answer is 42.' } }],
      });
      expect(events.find(event => event.type === 'subagent_end')).toMatchObject({
        isError: false,
        output: { status: 'success', outcome: 'completed' },
      });
    } finally {
      await harness.shutdown();
    }
  });

  it('a REAL parent spawning a REAL subagent surfaces the child’s streamed text + real tool round-trip as subagent_* events with correct attribution', async () => {
    // --- REAL child tool (runs inside the child agent's real loop) ----------
    // Returns a non-JSON-native value (Date) to prove the subagent_tool_end
    // output is projected JSON-safe at the parent event boundary.
    const childTool = createTool({
      id: 'lookupFact',
      description: 'look up a fact for the subagent',
      inputSchema: z.object({ topic: z.string() }),
      execute: async input => {
        return { topic: (input as { topic: string }).topic, value: 42, fetchedAt: new Date(0) };
      },
    });
    const continueAfterSummary = createTool({
      id: 'continueAfterSummary',
      description: 'Acknowledge that the visible specialist summary streamed before the terminal report.',
      inputSchema: z.object({}),
      execute: async () => ({ acknowledged: true }),
    });

    // --- REAL child agent: model emits a real tool-call, then reasoning+text --
    const childDeltas = ['The ', 'answer ', 'is ', '42.'];
    const childReasoning = ['Let me ', 'check the fact.'];
    let childCall = 0;
    const childModel = new MockLanguageModelV2({
      doStream: async () => {
        childCall++;
        if (childCall === 1) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: childToolCallStream('child-tc-1', 'lookupFact', '{"topic":"the answer"}'),
          };
        }
        if (childCall === 2) {
          // Visible progress is a non-terminal step. The required outcome tool
          // must be called by itself on the following provider step.
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: reasoningTextToolCallStream(
              childReasoning,
              childDeltas,
              'child-progress-tc',
              'continueAfterSummary',
              '{}',
            ),
          };
        }
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: toolCallStream(
            'child-outcome-tc',
            'report_subagent_outcome',
            JSON.stringify({
              outcome: 'completed',
              summary: childDeltas.join(''),
              evidence: [
                {
                  kind: 'tool-result',
                  toolName: 'lookupFact',
                  toolCallId: 'child-tc-1',
                  status: 'success',
                  description: 'The fact lookup completed successfully.',
                },
              ],
            }),
          ),
        };
      },
    });
    const childAgent = new Agent({
      id: 'child-agent',
      name: 'child-agent',
      instructions: 'use lookupFact then summarize',
      model: childModel,
      tools: { lookupFact: childTool, continueAfterSummary },
    });

    // --- REAL parent agent: one model turn delegates; child answer is terminal -
    let parentCall = 0;
    const parentModel = new MockLanguageModelV2({
      doStream: async () => {
        parentCall++;
        if (parentCall === 1) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream(
              'parent-spawn-tc',
              'spawn_subagent',
              JSON.stringify({
                agentType: 'explore',
                task: 'Find the answer to the question.',
                delivery: 'final',
              }),
            ),
          };
        }
        throw new Error('redundant parent model continuation must not run');
      },
    });
    const storage = new InMemoryStore();
    const parentMemory = new MockMemory({ storage });
    const parentAgent = new Agent({
      id: 'parent-agent',
      name: 'parent-agent',
      instructions: 'delegate to a subagent',
      model: parentModel,
      memory: parentMemory,
    });

    // --- REAL harness wiring both real agents + a real subagent type --------
    const harness = new Harness({
      agents: { 'parent-agent': parentAgent, 'child-agent': childAgent } as any,
      storage,
      modes: [
        { id: 'default', agentId: 'parent-agent' },
        { id: 'explore-mode', agentId: 'child-agent' },
      ],
      defaultModeId: 'default',
      subagents: {
        maxDepth: 2,
        types: {
          explore: {
            agentId: 'child-agent',
            modeId: 'explore-mode',
            description: 'Read-only fact lookup',
            defaultModelId: 'openai/gpt-4o-mini',
            workspace: 'inherit',
          },
        },
      },
    });

    try {
      const session = await harness.session({ resourceId: 'u-subagent', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));

      const result = (await session.message({ content: 'answer my question via a subagent' })) as any;

      // The parent's real loop ran the spawn tool and delivered the specialist
      // answer directly, without paying for a second parent model request.
      expect(parentCall).toBe(1);
      expect(result.text).toBe(childDeltas.join(''));
      expect(result.terminalToolResult?.items).toEqual([
        expect.objectContaining({
          toolName: 'spawn_subagent',
          toolCallId: 'parent-spawn-tc',
          value: expect.objectContaining({
            kind: 'subagent-direct-answer',
            text: childDeltas.join(''),
          }),
        }),
      ]);
      expect(Array.isArray(result.toolCalls)).toBe(true);
      expect(result.toolCalls.some((c: any) => (c.toolName ?? c.payload?.toolName) === 'spawn_subagent')).toBe(true);

      // --- subagent_start -----------------------------------------------------
      const subStart = events.find(e => e.type === 'subagent_start') as
        | {
            agentType: string;
            task: string;
            parentId: string;
            depth: number;
            subagentSessionId: string;
            toolCallId: string;
          }
        | undefined;
      expect(subStart).toBeDefined();
      expect(subStart!.agentType).toBe('explore');
      expect(subStart!.task).toBe('Find the answer to the question.');
      expect(subStart!.parentId).toBe(session.id);
      expect(subStart!.depth).toBe(1);
      expect(typeof subStart!.subagentSessionId).toBe('string');
      expect(subStart!.subagentSessionId.length).toBeGreaterThan(0);
      expect(subStart!.subagentSessionId).not.toBe(session.id);
      // The bridge stamps the PARENT's spawn tool-call id on every child event.
      expect(subStart!.toolCallId).toBe('parent-spawn-tc');

      const childSessionId = subStart!.subagentSessionId;

      // --- subagent_text_delta (the child's REAL streamed deltas) ------------
      const subTextDeltas = events.filter(e => e.type === 'subagent_text_delta') as Array<{
        delta: string;
        depth: number;
        parentId: string;
        subagentSessionId: string;
      }>;
      expect(subTextDeltas.length).toBe(childDeltas.length);
      expect(subTextDeltas.map(e => e.delta)).toEqual(childDeltas);
      expect(subTextDeltas.map(e => e.delta).join('')).toBe(childDeltas.join(''));
      // Attribution: all child deltas carry the parent id, child session id, depth 1.
      expect(subTextDeltas.every(e => e.parentId === session.id)).toBe(true);
      expect(subTextDeltas.every(e => e.subagentSessionId === childSessionId)).toBe(true);
      expect(subTextDeltas.every(e => e.depth === 1)).toBe(true);

      // --- subagent_reasoning_delta (the child's REAL streamed reasoning) ----
      // GAP-B: the child streamed reasoning before its summary; those surface as
      // subagent_reasoning_delta with the same attribution as text deltas.
      const subReasoningDeltas = events.filter(e => e.type === 'subagent_reasoning_delta') as Array<{
        delta: string;
        depth: number;
        parentId: string;
        subagentSessionId: string;
      }>;
      expect(subReasoningDeltas.map(e => e.delta)).toEqual(childReasoning);
      expect(subReasoningDeltas.every(e => e.parentId === session.id)).toBe(true);
      expect(subReasoningDeltas.every(e => e.subagentSessionId === childSessionId)).toBe(true);
      expect(subReasoningDeltas.every(e => e.depth === 1)).toBe(true);

      // --- subagent_tool_start / subagent_tool_end (child's REAL tool) -------
      const subToolStart = events.find(e => e.type === 'subagent_tool_start') as
        | { toolName: string; innerToolCallId: string; input: any; parentId: string; depth: number }
        | undefined;
      const subToolEnd = events.find(e => e.type === 'subagent_tool_end') as
        | {
            toolName: string;
            innerToolCallId: string;
            output: any;
            isError: boolean;
            durationMs?: number;
            parentId: string;
          }
        | undefined;
      expect(subToolStart).toBeDefined();
      expect(subToolStart!.toolName).toBe('lookupFact');
      expect(subToolStart!.innerToolCallId).toBe('child-tc-1');
      // GAP-A: the subagent tool's projected input args reach the parent stream.
      expect(subToolStart!.input).toEqual({ topic: 'the answer' });
      expect(subToolStart!.parentId).toBe(session.id);
      expect(subToolStart!.depth).toBe(1);

      expect(subToolEnd).toBeDefined();
      expect(subToolEnd!.toolName).toBe('lookupFact');
      expect(subToolEnd!.innerToolCallId).toBe('child-tc-1');
      expect(subToolEnd!.isError).toBe(false);
      expect(subToolEnd!.output.topic).toBe('the answer');
      expect(subToolEnd!.output.value).toBe(42);
      // JSON-safe projection: the child tool's Date crossed as an ISO string.
      expect(subToolEnd!.output.fetchedAt).toBe(new Date(0).toISOString());
      expect(subToolEnd!.output.fetchedAt).not.toBeInstanceOf(Date);
      expect(typeof subToolEnd!.durationMs).toBe('number');
      expect(subToolEnd!.durationMs).toBeGreaterThanOrEqual(0);

      // --- subagent_end -------------------------------------------------------
      const subEnd = events.find(e => e.type === 'subagent_end') as
        | { output: any; isError: boolean; durationMs: number; parentId: string; depth: number }
        | undefined;
      expect(subEnd).toBeDefined();
      expect(subEnd!.isError).toBe(false);
      expect(typeof subEnd!.durationMs).toBe('number');
      expect(subEnd!.durationMs).toBeGreaterThanOrEqual(0);
      expect(subEnd!.parentId).toBe(session.id);
      expect(subEnd!.depth).toBe(1);
      // The bounded child summary preserves the final text without copying the
      // child's raw FullOutput/provider/tool bodies into the parent ledger.
      expect((subEnd!.output as { text?: string }).text).toBe(childDeltas.join(''));

      // --- Ordering: start → (child events) → end ----------------------------
      const subTypes = events.filter(e => e.type.startsWith('subagent_')).map(e => e.type);
      const startIdx = subTypes.indexOf('subagent_start');
      const endIdx = subTypes.lastIndexOf('subagent_end');
      const toolStartIdx = subTypes.indexOf('subagent_tool_start');
      const toolEndIdx = subTypes.indexOf('subagent_tool_end');
      const firstTextIdx = subTypes.indexOf('subagent_text_delta');
      expect(startIdx).toBe(0);
      expect(endIdx).toBe(subTypes.length - 1);
      // The child ran its tool BEFORE it streamed its post-tool summary text.
      expect(toolStartIdx).toBeGreaterThan(startIdx);
      expect(toolStartIdx).toBeLessThan(toolEndIdx);
      expect(toolEndIdx).toBeLessThan(firstTextIdx);
      expect(firstTextIdx).toBeLessThan(endIdx);
      // Exactly one start and one end for the single spawned child.
      expect(subTypes.filter(t => t === 'subagent_start')).toHaveLength(1);
      expect(subTypes.filter(t => t === 'subagent_end')).toHaveLength(1);

      const directAnswerEvents = events.filter(e => e.type === 'text_delta') as Array<{ delta: string }>;
      expect(directAnswerEvents.map(event => event.delta)).toEqual([childDeltas.join('')]);

      const liveMessages = await session.listMessages();
      const persistedAnswerOccurrences = liveMessages
        .flatMap(message => message.content)
        .filter(part => part.type === 'text' && part.text === childDeltas.join(''));
      expect(persistedAnswerOccurrences).toHaveLength(1);

      // The child ran three real provider steps: lookup, visible summary, then
      // the report-only terminal action.
      expect(childCall).toBe(3);
    } finally {
      await harness.shutdown();
    }
  });

  it('direct-delivers a child domain terminal after one parent call, one child call, and one tool execution', async () => {
    let sandboxExecutions = 0;
    const terminalAnalysis = createTool({
      id: 'runAnalysis',
      description: 'compute and return the complete answer',
      inputSchema: z.object({ expression: z.string() }),
      terminalResult: {
        isSuccess: output => output.success,
        project: output => ({ text: output.answer }),
        outputSchema: z.object({ text: z.string() }),
      },
      execute: async () => {
        sandboxExecutions++;
        return { success: true, answer: 'The computed total is **10**.' };
      },
    });
    let childCalls = 0;
    const childModel = new MockLanguageModelV2({
      doStream: async () => {
        childCalls++;
        if (childCalls > 1) throw new Error('redundant child narration turn must not run');
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: toolCallStream('analysis-call-1', 'runAnalysis', '{"expression":"4 + 6"}'),
        };
      },
    });
    const childAgent = new Agent({
      id: 'analysis-agent',
      name: 'analysis-agent',
      instructions: 'run the analysis tool once',
      model: childModel,
      tools: { runAnalysis: terminalAnalysis },
    });
    let parentCalls = 0;
    const parentModel = new MockLanguageModelV2({
      doStream: async () => {
        parentCalls++;
        if (parentCalls > 1) throw new Error('redundant parent narration turn must not run');
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: toolCallStream(
            'analysis-spawn-1',
            'spawn_subagent',
            JSON.stringify({
              agentType: 'analysis',
              task: 'Compute 4 + 6 and return the complete result.',
              delivery: 'final',
            }),
          ),
        };
      },
    });
    const parentAgent = new Agent({
      id: 'parent-analysis-agent',
      name: 'parent-analysis-agent',
      instructions: 'delegate exact calculations',
      model: parentModel,
    });
    const harness = new Harness({
      agents: {
        'parent-analysis-agent': parentAgent,
        'analysis-agent': childAgent,
      } as any,
      storage: new InMemoryStore(),
      modes: [
        { id: 'default', agentId: 'parent-analysis-agent' },
        { id: 'analysis-mode', agentId: 'analysis-agent' },
      ],
      defaultModeId: 'default',
      subagents: {
        maxDepth: 2,
        types: {
          analysis: {
            agentId: 'analysis-agent',
            modeId: 'analysis-mode',
            description: 'Exact bounded analysis',
            defaultModelId: 'openai/gpt-4o-mini',
            workspace: 'inherit',
          },
        },
      },
    });
    try {
      const session = await harness.session({ resourceId: 'u-terminal-analysis', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(event => events.push(event));
      const result = (await session.message({ content: 'What is 4 + 6?' })) as any;

      expect({ parentCalls, childCalls, sandboxExecutions }).toEqual({
        parentCalls: 1,
        childCalls: 1,
        sandboxExecutions: 1,
      });
      expect(result.text).toBe('The computed total is **10**.');
      expect(result.terminalToolResult?.items).toEqual([
        expect.objectContaining({
          toolName: 'spawn_subagent',
          value: expect.objectContaining({
            kind: 'subagent-direct-answer',
            text: 'The computed total is **10**.',
          }),
        }),
      ]);
      expect(events.filter(event => event.type === 'subagent_tool_start')).toHaveLength(1);
      expect(events.filter(event => event.type === 'subagent_tool_end')).toHaveLength(1);
      expect(events.filter(event => event.type === 'subagent_end')).toHaveLength(1);
      expect(
        events.filter(event => event.type === 'text_delta').map(event => (event as { delta: string }).delta),
      ).toEqual(['The computed total is **10**.']);
    } finally {
      await harness.shutdown();
    }
  });
});

// ===========================================================================
// S8 — Multi-turn conversation builds REAL thread context.
//
// Every prior real-agent scenario opens a fresh thread and sends exactly ONE
// message(). Nothing asserts that turn N+1's real ai-sdk prompt carries turn
// N's exchange. `MockLanguageModelV2.doStream` receives the provider call
// `_options` (incl. `.prompt`, the message array the loop assembled from real
// thread history), so prompt growth across turns is directly assertable.
//
// This drives THREE sequential message() turns on ONE session and proves the
// real loop threads prior user+assistant content into later prompts — not
// isolated, stateless model calls.
//
// NOTE: conversation history threading is a `Memory` feature, not a bare-agent
// one. A real Agent with NO `memory` configured logs "No memory is configured
// but resourceId and threadId were passed" and assembles each turn's prompt
// from ONLY the system instruction + the new user message (verified at runtime:
// turn 3's prompt contained neither prior user content nor prior replies). So
// this scenario wires a real `MockMemory` (the same store the agent memory
// tests use) onto the agent — the harness passes `memory: { thread, resource }`
// into every turn's exec options (session.ts), so real history accretes.
// ===========================================================================

describe('Harness v1 real-agent E2E — S8 multi-turn context threading', () => {
  it('three sequential message() turns share a thread and later prompts carry earlier turns', async () => {
    // Capture every provider prompt the real loop assembled, in call order.
    const prompts: unknown[][] = [];
    let turn = 0;
    const replies = ['Noted, Dero.', 'I will remember it.', 'Your name is Dero.'];
    const model = new MockLanguageModelV2({
      doStream: async _options => {
        // `_options.prompt` is the message array the agent loop built from real
        // thread history + the new user content (title-generation.test.ts reads
        // `options.prompt` the same way).
        prompts.push((_options as { prompt: unknown[] }).prompt);
        const reply = replies[turn] ?? 'ok';
        turn++;
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: textStream([reply]),
        };
      },
    });
    const agent = new Agent({
      id: 'default',
      name: 'default',
      instructions: 'remember details',
      model,
      memory: new MockMemory(),
    });
    const harness = newHarness(agent);
    try {
      const session = await harness.session({ resourceId: 'u-multi', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));

      const r1 = (await session.message({ content: 'my name is Dero' })) as any;
      const r2 = (await session.message({ content: 'please remember it' })) as any;
      const r3 = (await session.message({ content: 'what is my name?' })) as any;

      // (a) each turn resolved with its expected real reply.
      expect(r1.text).toBe(replies[0]);
      expect(r2.text).toBe(replies[1]);
      expect(r3.text).toBe(replies[2]);

      // (b) three real provider calls were made (one per message turn).
      expect(prompts).toHaveLength(3);

      // (c) prompt for turn 3 carries the prior turns' user content — proving
      // real thread-history threading, not isolated stateless calls.
      const turn3Json = JSON.stringify(prompts[2]);
      expect(turn3Json).toContain('my name is Dero');
      expect(turn3Json).toContain('please remember it');
      // turn 3's prompt also carries an earlier assistant reply.
      expect(turn3Json).toContain(replies[0]);

      // (d) the assembled prompt strictly grows turn over turn (history accretes).
      expect(prompts[1]!.length).toBeGreaterThan(prompts[0]!.length);
      expect(prompts[2]!.length).toBeGreaterThan(prompts[1]!.length);
      // turn 1's prompt has no later turns' content.
      expect(JSON.stringify(prompts[0])).not.toContain('what is my name?');

      // (e) all three turns ran on the SAME thread with DISTINCT runIds.
      const ends = events.filter(e => e.type === 'agent_end') as Array<{ runId: string; finishReason: string }>;
      expect(ends).toHaveLength(3);
      expect(ends.every(e => e.finishReason === 'complete')).toBe(true);
      const runIds = ends.map(e => e.runId);
      expect(new Set(runIds).size).toBe(3);
    } finally {
      await harness.shutdown();
    }
  });
});

// ===========================================================================
// S9 — A queue of DISTINCT items drains FIFO through the REAL loop.
//
// session.queue.test.ts drives FIFO ordering with a MockAgent staging
// fabricated outputs; the real-agent S4 queue case is a single item. This
// enqueues THREE distinct items back-to-back on an idle session and asserts
// they drain in A→B→C order through the genuine loop with distinct results.
// ===========================================================================

describe('Harness v1 real-agent E2E — S9 queue FIFO drain (real loop)', () => {
  it('three distinct queued items drain FIFO with distinct results and empty the pendingQueue', async () => {
    // Stateful model: distinct reply keyed on the user content the loop sent.
    const model = new MockLanguageModelV2({
      doStream: async _options => {
        const promptJson = JSON.stringify((_options as { prompt: unknown[] }).prompt);
        const which = promptJson.includes('task-C') ? 'reply-C' : promptJson.includes('task-B') ? 'reply-B' : 'reply-A';
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: textStream([which]),
        };
      },
    });
    const agent = new Agent({ id: 'default', name: 'default', instructions: 'reply', model });
    const harness = newHarness(agent);
    try {
      const session = await harness.session({ resourceId: 'u-queue-fifo', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));

      // Fire all three back-to-back without awaiting; the loop drains FIFO.
      const pA = session.queue({ content: 'task-A' });
      const pB = session.queue({ content: 'task-B' });
      const pC = session.queue({ content: 'task-C' });
      const [rA, rB, rC] = (await Promise.all([pA, pB, pC])) as any[];

      // (a) distinct results, each its own queued item's reply.
      expect(rA.text).toBe('reply-A');
      expect(rB.text).toBe('reply-B');
      expect(rC.text).toBe('reply-C');

      // (b) exactly three queue_completed events, in A,B,C drain order.
      await waitFor(
        () => events.filter(e => e.type === 'queue_completed').length === 3,
        'three queue_completed events',
      );
      const completed = events.filter(e => e.type === 'queue_completed') as Array<{
        queuedItemId: string;
        signalId: string;
        runId: string;
      }>;
      expect(completed).toHaveLength(3);
      // (c) distinct queuedItemIds / runIds across the three drained turns.
      expect(new Set(completed.map(c => c.queuedItemId)).size).toBe(3);
      expect(new Set(completed.map(c => c.runId)).size).toBe(3);
      expect(completed.every(c => c.signalId.length > 0)).toBe(true);

      // (d) ordering: each turn's agent_start/agent_end nest in FIFO order.
      // The three runs ran sequentially, so their agent_end runIds appear in
      // the SAME order as the queued items drained.
      const ends = events.filter(e => e.type === 'agent_end') as Array<{ runId: string }>;
      expect(ends.map(e => e.runId)).toEqual(completed.map(c => c.runId));

      // (e) the pending queue fully drained.
      expect(session.getRecord().pendingQueue).toEqual([]);
    } finally {
      await harness.shutdown();
    }
  });
});

// ===========================================================================
// S10 — A signal INTERLEAVES a LIVE real run (§21 shared terminal).
//
// session.signal.test.ts holds the run open with a MockAgent fabricated
// output; willInterleave / the shared-terminal settlement are only exercised
// against a mock. Here a REAL run is genuinely in-flight (a tool blocks on an
// external barrier), and a signal() fired during that window must report
// willInterleave:true with the active run's id, then settle off the SHARED
// run terminal.
//
// CHARACTERIZATION: per-segment DISTINCT-answer attribution for the
// interleaved content is a documented runtime refinement (session.ts shared-
// terminal comment). This asserts only the GUARANTEED contract — willInterleave
// + shared runId + override-rejection + shared-terminal settlement — NOT a
// distinct per-segment answer for the interleaved signal.
// ===========================================================================

describe('Harness v1 real-agent E2E — S10 signal interleaves a live run', () => {
  it('a signal fired during a live real run reports willInterleave + shared runId, rejects overrides, and settles off the shared terminal', async () => {
    let releaseBarrier!: () => void;
    const barrier = new Promise<void>(resolve => {
      releaseBarrier = resolve;
    });
    let toolStarted = false;
    // A real tool that blocks the run open on an external barrier, so a real
    // run is genuinely in-flight while we fire the interleaving signal.
    const holdTool = createTool({
      id: 'holdOpen',
      description: 'holds the run open until released',
      inputSchema: z.object({}),
      execute: async () => {
        toolStarted = true;
        await barrier;
        return { held: true };
      },
    });

    let callCount = 0;
    const model = new MockLanguageModelV2({
      doStream: async () => {
        callCount++;
        if (callCount === 1) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream('call-hold', 'holdOpen', '{}'),
          };
        }
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: textStream(['parent done']),
        };
      },
    });
    const agent = new Agent({
      id: 'default',
      name: 'default',
      instructions: 'use holdOpen',
      model,
      tools: { holdOpen: holdTool },
    });
    const harness = newHarness(agent);
    try {
      const session = await harness.session({ resourceId: 'u-interleave', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));

      // Fire the parent run WITHOUT awaiting — it blocks inside holdOpen.
      const parentPromise = session.message({ content: 'go' }) as Promise<any>;
      void parentPromise.catch(() => {});

      // Wait until the run is genuinely in-flight (tool executing).
      await waitFor(() => toolStarted, 'holdOpen tool started');
      const parentRunId = (events.find(e => e.type === 'agent_start') as { runId: string }).runId;

      // (a) a signal fired NOW interleaves the live run, sharing its runId.
      const handle = await session.signal({ content: 'steer' });
      expect(handle.willInterleave).toBe(true);
      expect(handle.runId).toBe(parentRunId);

      // (b) a per-turn override on an active-delivery signal rejects against the
      // REAL in-flight run (HarnessOverrideConflictError, session.ts:5858-5862).
      await expect(session.signal({ content: 'x', mode: 'default' })).rejects.toMatchObject({
        name: 'HarnessOverrideConflictError',
      });
      await expect(
        session.signal({ content: 'x', additionalTools: { holdOpen: holdTool } as any }),
      ).rejects.toMatchObject({ name: 'HarnessOverrideConflictError' });

      // Release the barrier — the real run finishes.
      releaseBarrier();
      const parentResult = (await parentPromise) as any;
      expect(parentResult.finishReason).toBe('stop');

      // (c) the interleaved signal settled off the SHARED run terminal: its
      // result resolves to the same run's AgentResult.
      const signalResult = (await handle.result) as any;
      expect(signalResult.finishReason).toBe('stop');

      // (d) a signal_completed for the interleaved signal carries the shared runId.
      await waitFor(
        () => events.some(e => e.type === 'signal_completed' && (e as any).runId === parentRunId),
        'signal_completed on shared runId',
      );

      // (e) lookup reports the interleaved signal completed.
      const lookup = await session.lookupMessageResult(handle.id);
      expect(lookup && 'status' in lookup ? lookup.status : null).toBe('completed');
    } finally {
      // Ensure the barrier is released so shutdown never hangs.
      releaseBarrier();
      await harness.shutdown();
    }
  });
});

// ===========================================================================
// S11 — Multi-tool turn: TWO tool round-trips in ONE real run.
//
// S2 is exactly one tool call. The real loop's ability to run two distinct
// tool round-trips across loop iterations in a single turn (toolA → toolB →
// text) is unverified at the harness event boundary. This drives it.
// ===========================================================================

describe('Harness v1 real-agent E2E — S11 multi-tool single turn', () => {
  it('two tool round-trips in one real run surface two ordered tool_start/tool_end pairs', async () => {
    const seenHarnessCtx: unknown[] = [];
    const toolA = createTool({
      id: 'stepA',
      description: 'first step',
      inputSchema: z.object({ x: z.number() }),
      execute: async (input, context) => {
        seenHarnessCtx.push(context?.requestContext?.get('harness'));
        return { a: (input as { x: number }).x + 1 };
      },
    });
    const toolB = createTool({
      id: 'stepB',
      description: 'second step',
      inputSchema: z.object({ y: z.number() }),
      execute: async (input, context) => {
        seenHarnessCtx.push(context?.requestContext?.get('harness'));
        return { b: (input as { y: number }).y * 2 };
      },
    });

    let callCount = 0;
    const model = new MockLanguageModelV2({
      doStream: async () => {
        callCount++;
        if (callCount === 1) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream('tc-a', 'stepA', '{"x":1}'),
          };
        }
        if (callCount === 2) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream('tc-b', 'stepB', '{"y":3}'),
          };
        }
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: textStream(['both ', 'done']),
        };
      },
    });
    const agent = new Agent({
      id: 'default',
      name: 'default',
      instructions: 'use stepA then stepB',
      model,
      tools: { stepA: toolA, stepB: toolB },
    });
    const harness = newHarness(agent);
    try {
      const session = await harness.session({ resourceId: 'u-multitool', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));

      const result = (await session.message({ content: 'run both steps' })) as any;
      expect(result.text).toBe('both done');

      // (a) exactly two tool_start and two tool_end events, in [A,B] order.
      const toolStarts = events.filter(e => e.type === 'tool_start') as Array<{
        toolName: string;
        toolCallId: string;
      }>;
      const toolEnds = events.filter(e => e.type === 'tool_end') as Array<{
        toolName: string;
        toolCallId: string;
        isError: boolean;
        output: any;
      }>;
      expect(toolStarts.map(e => e.toolName)).toEqual(['stepA', 'stepB']);
      expect(toolEnds.map(e => e.toolName)).toEqual(['stepA', 'stepB']);
      expect(toolEnds.every(e => e.isError === false)).toBe(true);
      expect(toolEnds[0]!.output).toEqual({ a: 2 });
      expect(toolEnds[1]!.output).toEqual({ b: 6 });

      // (b) ordering: stepA_end precedes stepB_start precedes final text.
      const types = events.map(e => e.type);
      const aEndIdx = events.findIndex(e => e.type === 'tool_end' && (e as any).toolName === 'stepA');
      const bStartIdx = events.findIndex(e => e.type === 'tool_start' && (e as any).toolName === 'stepB');
      expect(aEndIdx).toBeGreaterThanOrEqual(0);
      expect(aEndIdx).toBeLessThan(bStartIdx);
      expect(bStartIdx).toBeLessThan(types.lastIndexOf('text_delta'));

      // (c) the real FullOutput carries >=2 tool calls/results.
      expect(result.toolCalls.length).toBeGreaterThanOrEqual(2);
      expect(result.toolResults.length).toBeGreaterThanOrEqual(2);

      // (d) BOTH tools saw the harness RequestContext for this session.
      expect(seenHarnessCtx).toHaveLength(2);
      expect((seenHarnessCtx[0] as { sessionId?: string }).sessionId).toBe(session.id);
      expect((seenHarnessCtx[1] as { sessionId?: string }).sessionId).toBe(session.id);
    } finally {
      await harness.shutdown();
    }
  });
});

// ===========================================================================
// S12 — Approval → resume → a SECOND (non-approval) tool → complete.
//
// S3's resume turn is text-only. The path where the resumed run calls ANOTHER
// tool before completing (§10.4 live-drain past a fresh tool) is uncovered.
// ===========================================================================

describe('Harness v1 real-agent E2E — S12 resume runs a fresh tool', () => {
  it('after approving the first tool, the resumed run runs a SECOND tool then completes', async () => {
    const approveTool = createTool({
      id: 'findUser',
      description: 'look up a user (needs approval)',
      inputSchema: z.object({ name: z.string() }),
      requireApproval: true,
      execute: async input => ({ name: (input as { name: string }).name, id: 7 }),
    });
    const followTool = createTool({
      id: 'recordResult',
      description: 'record the looked-up user (no approval)',
      inputSchema: z.object({ id: z.number() }),
      execute: async input => ({ recorded: (input as { id: number }).id }),
    });

    let callCount = 0;
    const model = new MockLanguageModelV2({
      doStream: async () => {
        callCount++;
        if (callCount === 1) {
          // suspends on approval
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream('tc-approve', 'findUser', '{"name":"Dero"}'),
          };
        }
        if (callCount === 2) {
          // post-approval continuation calls a fresh, non-approval tool
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream('tc-follow', 'recordResult', '{"id":7}'),
          };
        }
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: textStream(['recorded']),
        };
      },
    });
    const agent = new Agent({
      id: 'default',
      name: 'default',
      instructions: 'find then record',
      model,
      tools: { findUser: approveTool, recordResult: followTool },
    });
    const harness = newHarness(agent);
    try {
      const session = await harness.session({ resourceId: 'u-resume-tool', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));

      const suspended = (await session.message({ content: 'find and record Dero' })) as any;
      expect(suspended.finishReason).toBe('suspended');
      expect(events.some(e => e.type === 'tool_approval_required')).toBe(true);
      expect(session.getRecord().pendingResume).toBeDefined();

      const eventsAtSuspend = events.length;

      await session.respondToToolApproval({ approved: true });
      await waitFor(
        () => events.some(e => e.type === 'agent_end' && (e as any).finishReason === 'complete'),
        'agent_end:complete after resume + second tool',
      );

      const postResume = events.slice(eventsAtSuspend);
      const postTypes = postResume.map(e => e.type);

      // (a) the approved tool (findUser) surfaced its tool_end after resume.
      const approvedEnd = postResume.find(e => e.type === 'tool_end' && (e as any).toolName === 'findUser') as
        | { output: any; isError: boolean }
        | undefined;
      expect(approvedEnd).toBeDefined();
      expect(approvedEnd!.isError).toBe(false);
      expect(approvedEnd!.output).toEqual({ name: 'Dero', id: 7 });

      // (b) the SECOND, fresh tool ran in the continuation: a recordResult
      // tool_start + tool_end appear AFTER the approved tool's tool_end.
      const followStartIdx = postResume.findIndex(
        e => e.type === 'tool_start' && (e as any).toolName === 'recordResult',
      );
      const followEndIdx = postResume.findIndex(e => e.type === 'tool_end' && (e as any).toolName === 'recordResult');
      const approvedEndIdx = postResume.findIndex(e => e.type === 'tool_end' && (e as any).toolName === 'findUser');
      expect(followStartIdx).toBeGreaterThanOrEqual(0);
      expect(followEndIdx).toBeGreaterThan(followStartIdx);
      expect(approvedEndIdx).toBeLessThan(followStartIdx);
      const followEnd = postResume[followEndIdx] as any;
      expect(followEnd.output).toEqual({ recorded: 7 });

      // (c) the terminal text + complete follow the second tool.
      const postText = (postResume.filter(e => e.type === 'text_delta') as Array<{ delta: string }>)
        .map(e => e.delta)
        .join('');
      expect(postText).toBe('recorded');
      expect(followEndIdx).toBeLessThan(postTypes.lastIndexOf('text_delta'));
      expect(postTypes.lastIndexOf('text_delta')).toBeLessThan(postTypes.lastIndexOf('agent_end'));

      // (d) the pending resume registration cleared.
      expect(session.getRecord().pendingResume).toBeUndefined();
    } finally {
      await harness.shutdown();
    }
  });
});

// ===========================================================================
// S13 — Subagent runs a tool, returns, THEN the PARENT runs its OWN tool.
//
// S7's parent has no tools — it only spawns then emits text. A parent that
// consumes the subagent result and then runs its OWN tool (proving the parent
// resumed its own loop post-delegation) is uncovered.
// ===========================================================================

describe('Harness v1 real-agent E2E — S13 parent runs a tool after the subagent', () => {
  it('a parent runs its OWN tool after the subagent returns, surfacing a parent-level tool_end (not subagent_*)', async () => {
    // Child: one tool + text (same shape as S7's child).
    const childTool = createTool({
      id: 'lookupFact',
      description: 'look up a fact',
      inputSchema: z.object({ topic: z.string() }),
      execute: async input => ({ topic: (input as { topic: string }).topic, value: 42 }),
    });
    const childDeltas = ['answer ', '42'];
    let childCall = 0;
    const childModel = new MockLanguageModelV2({
      doStream: async () => {
        childCall++;
        if (childCall === 1) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream('child-tc', 'lookupFact', '{"topic":"the answer"}'),
          };
        }
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: textStream(childDeltas),
        };
      },
    });
    const childAgent = new Agent({
      id: 'child-agent',
      name: 'child-agent',
      instructions: 'use lookupFact then summarize',
      model: childModel,
      tools: { lookupFact: childTool },
    });

    // Parent: a REAL tool that records the subagent's returned text.
    let recordedInput: unknown;
    const parentTool = createTool({
      id: 'recordResult',
      description: 'record the subagent summary',
      inputSchema: z.object({ summary: z.string() }),
      execute: async input => {
        recordedInput = input;
        return { stored: true };
      },
    });

    // Parent model: call-1 spawn_subagent, call-2 a tool-call to recordResult,
    // call-3 final text.
    let parentCall = 0;
    const parentModel = new MockLanguageModelV2({
      doStream: async () => {
        parentCall++;
        if (parentCall === 1) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream(
              'parent-spawn-tc',
              'spawn_subagent',
              JSON.stringify({ agentType: 'explore', task: 'Find the answer.' }),
            ),
          };
        }
        if (parentCall === 2) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream('parent-record-tc', 'recordResult', '{"summary":"answer 42"}'),
          };
        }
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: textStream(['parent done']),
        };
      },
    });
    const parentAgent = new Agent({
      id: 'parent-agent',
      name: 'parent-agent',
      instructions: 'delegate then record',
      model: parentModel,
      tools: { recordResult: parentTool },
    });

    const harness = new Harness({
      agents: { 'parent-agent': parentAgent, 'child-agent': childAgent } as any,
      storage: new InMemoryStore(),
      modes: [
        { id: 'default', agentId: 'parent-agent' },
        { id: 'explore-mode', agentId: 'child-agent' },
      ],
      defaultModeId: 'default',
      subagents: {
        maxDepth: 2,
        types: {
          explore: {
            agentId: 'child-agent',
            modeId: 'explore-mode',
            description: 'Read-only fact lookup',
            defaultModelId: 'openai/gpt-4o-mini',
            workspace: 'inherit',
          },
        },
      },
    });

    try {
      const session = await harness.session({ resourceId: 'u-sub-then-tool', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));

      const result = (await session.message({ content: 'delegate then record' })) as any;
      expect(result.text).toBe('parent done');

      // (a) the full subagent_* sequence ran (start..end).
      const subStart = events.find(e => e.type === 'subagent_start');
      const subEnd = events.find(e => e.type === 'subagent_end');
      expect(subStart).toBeDefined();
      expect(subEnd).toBeDefined();

      // (b) AFTER subagent_end, a PARENT-level tool_start/tool_end (NOT
      // subagent_*) for recordResult — proving the parent resumed its OWN loop
      // with a fresh tool post-delegation.
      const subEndIdx = events.findIndex(e => e.type === 'subagent_end');
      const parentToolStart = events.find(
        (e, i) => i > subEndIdx && e.type === 'tool_start' && (e as any).toolName === 'recordResult',
      ) as { toolName: string; toolCallId: string } | undefined;
      const parentToolEnd = events.find(
        (e, i) => i > subEndIdx && e.type === 'tool_end' && (e as any).toolName === 'recordResult',
      ) as { toolName: string; output: any; isError: boolean } | undefined;
      expect(parentToolStart).toBeDefined();
      expect(parentToolStart!.toolCallId).toBe('parent-record-tc');
      expect(parentToolEnd).toBeDefined();
      expect(parentToolEnd!.isError).toBe(false);
      expect(parentToolEnd!.output).toEqual({ stored: true });

      // The parent tool is a plain tool_end, not a subagent_tool_end.
      expect(parentToolEnd!.toolName).toBe('recordResult');

      // (c) the parent tool's input reflects the (model-relayed) subagent summary.
      expect(recordedInput).toEqual({ summary: 'answer 42' });

      // (d) the child's real loop ran (tool round-trip + summary).
      expect(childCall).toBe(2);

      // (e) terminal complete after the parent tool.
      const ends = events.filter(e => e.type === 'agent_end') as Array<{ finishReason: string }>;
      expect(ends.some(e => e.finishReason === 'complete')).toBe(true);
    } finally {
      await harness.shutdown();
    }
  });
});

// ===========================================================================
// Shared helpers for the S14+ subagent / sentinel / reasoning scenarios.
// ===========================================================================

/**
 * A raw provider stream that interleaves a tool-call, text deltas, then a
 * SECOND tool-call, finishing `tool-calls`. Exercises the real streamed
 * tool→text→tool ordering inside a single turn segment.
 */
function toolTextToolStream(
  firstCallId: string,
  firstToolName: string,
  firstInputJson: string,
  textDeltas: string[],
  secondCallId: string,
  secondToolName: string,
  secondInputJson: string,
) {
  return convertArrayToReadableStream([
    { type: 'stream-start', warnings: [] },
    { type: 'response-metadata', id: 'id-ttt', modelId: 'mock-model-id', timestamp: new Date(0) },
    {
      type: 'tool-call',
      toolCallId: firstCallId,
      toolName: firstToolName,
      input: firstInputJson,
      providerExecuted: false,
    },
    { type: 'text-start', id: 'text-1' },
    ...textDeltas.map(delta => ({ type: 'text-delta', id: 'text-1', delta })),
    { type: 'text-end', id: 'text-1' },
    {
      type: 'tool-call',
      toolCallId: secondCallId,
      toolName: secondToolName,
      input: secondInputJson,
      providerExecuted: false,
    },
    { type: 'finish', finishReason: 'tool-calls', usage: testUsage },
  ]);
}

// ===========================================================================
// S14 — SUBAGENT TOOL ERROR: a REAL child whose tool THROWS.
//
// S7 covers the happy child-tool round-trip. Nothing covers a child tool that
// throws mid-loop. The UI relies on observing the child's failed tool as a
// subagent_tool_end{isError:true} and a clean subagent_end, with the PARENT
// run continuing uncorrupted (the child failure is a tool-error payload, not a
// parent abort — spawn-subagent-tool.ts catches the child failure).
// ===========================================================================

describe('Harness v1 real-agent E2E — S14 subagent tool error', () => {
  it('a real subagent whose tool throws surfaces subagent_tool_end{isError:true} + the error detail, and the parent run completes uncorrupted', async () => {
    const THROWN = 'lookupFact exploded: db.internal:5432 unreachable';
    const childTool = createTool({
      id: 'lookupFact',
      description: 'look up a fact (throws)',
      inputSchema: z.object({ topic: z.string() }),
      execute: async () => {
        throw new Error(THROWN);
      },
    });
    const continueAfterSummary = createTool({
      id: 'continueAfterSummary',
      description: 'Acknowledge that the blocked specialist summary streamed before its terminal report.',
      inputSchema: z.object({}),
      execute: async () => ({ acknowledged: true }),
    });
    // Child: call-1 the throwing tool, call-2 a recovery summary. A real agent
    // loop feeds the tool error back to the model and continues. The child
    // reports a truthful blocked outcome; the parent remains free to continue.
    const childDeltas = ['Could ', 'not ', 'look it up.'];
    let childCall = 0;
    const childModel = new MockLanguageModelV2({
      doStream: async () => {
        childCall++;
        if (childCall === 1) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream('child-throw-tc', 'lookupFact', '{"topic":"the answer"}'),
          };
        }
        if (childCall === 2) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: reasoningTextToolCallStream(
              [],
              childDeltas,
              'child-blocked-progress-tc',
              'continueAfterSummary',
              '{}',
            ),
          };
        }
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: toolCallStream(
            'child-blocked-outcome-tc',
            'report_subagent_outcome',
            JSON.stringify({
              outcome: 'blocked',
              summary: childDeltas.join(''),
              evidence: [
                {
                  kind: 'tool-result',
                  toolName: 'lookupFact',
                  toolCallId: 'child-throw-tc',
                  status: 'error',
                  description: 'The lookup tool failed before returning a fact.',
                },
              ],
              issue: {
                code: 'lookup.unavailable',
                message: 'The fact lookup dependency was unavailable.',
                retryable: true,
              },
            }),
          ),
        };
      },
    });
    const childAgent = new Agent({
      id: 'child-agent',
      name: 'child-agent',
      instructions: 'use lookupFact',
      model: childModel,
      tools: { lookupFact: childTool, continueAfterSummary },
    });

    let parentCall = 0;
    const parentModel = new MockLanguageModelV2({
      doStream: async () => {
        parentCall++;
        if (parentCall === 1) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream(
              'parent-spawn-tc',
              'spawn_subagent',
              JSON.stringify({ agentType: 'explore', task: 'Find the answer.' }),
            ),
          };
        }
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: textStream(['Parent ', 'continued.']),
        };
      },
    });
    const parentAgent = new Agent({
      id: 'parent-agent',
      name: 'parent-agent',
      instructions: 'delegate',
      model: parentModel,
    });

    const harness = new Harness({
      agents: { 'parent-agent': parentAgent, 'child-agent': childAgent } as any,
      storage: new InMemoryStore(),
      modes: [
        { id: 'default', agentId: 'parent-agent' },
        { id: 'explore-mode', agentId: 'child-agent' },
      ],
      defaultModeId: 'default',
      subagents: {
        maxDepth: 2,
        types: {
          explore: {
            agentId: 'child-agent',
            modeId: 'explore-mode',
            description: 'fact lookup',
            defaultModelId: 'openai/gpt-4o-mini',
            workspace: 'inherit',
          },
        },
      },
    });

    try {
      const session = await harness.session({ resourceId: 'u-sub-err', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));

      const result = (await session.message({ content: 'delegate to a subagent' })) as any;

      // The PARENT run completed cleanly — a child tool error does NOT corrupt
      // or abort the parent turn.
      expect(result.text).toBe('Parent continued.');
      const parentEnds = events.filter(e => e.type === 'agent_end') as Array<{ finishReason: string }>;
      expect(parentEnds.some(e => e.finishReason === 'complete')).toBe(true);
      expect(parentEnds.every(e => e.finishReason !== 'error' && e.finishReason !== 'aborted')).toBe(true);

      // The child's failed tool surfaced as subagent_tool_end{isError:true},
      // carrying the projected error detail ({name,message}, JSON-safe).
      const subToolEnd = events.find(e => e.type === 'subagent_tool_end') as
        | { toolName: string; output: any; isError: boolean; innerToolCallId: string }
        | undefined;
      expect(subToolEnd).toBeDefined();
      expect(subToolEnd!.toolName).toBe('lookupFact');
      expect(subToolEnd!.innerToolCallId).toBe('child-throw-tc');
      expect(subToolEnd!.isError).toBe(true);
      // The thrown Error projected to a JSON-safe {name,message,...}; the raw
      // detail is faithfully preserved (a tool's OWN error is NOT redacted to
      // harness.internal — session.ts:4399-4403).
      expect(JSON.stringify(subToolEnd!.output)).toContain(THROWN);
      expect(subToolEnd!.output).not.toBeInstanceOf(Error);

      // The subagent terminalized exactly once with a truthful blocked result.
      // Its dependency failure does not corrupt the parent run.
      const subEnds = events.filter(e => e.type === 'subagent_end') as Array<{ isError: boolean; output: any }>;
      expect(subEnds).toHaveLength(1);
      expect(subEnds[0]!.isError).toBe(true);
      expect(subEnds[0]!.output).toMatchObject({
        outcome: 'blocked',
        issue: { code: 'lookup.unavailable', retryable: true },
      });
      expect((subEnds[0]!.output as { text?: string }).text).toBe(childDeltas.join(''));

      // The child ran the failed lookup, streamed its blocked summary, then
      // emitted a report-only terminal action.
      expect(childCall).toBe(3);
    } finally {
      await harness.shutdown();
    }
  });
});

// ===========================================================================
// S15 — NESTED SUBAGENT depth>1: parent → child → grandchild.
//
// A real parent spawns a real child that ITSELF spawns a real grandchild
// (maxDepth=2). This characterizes how deep subagent events propagate and what
// `parentId` / `depth` carry at each hop.
//
// FINDING (documented, asserted against the code's REAL behavior): the
// spawn-subagent bridge (spawn-subagent-tool.ts:156-227) only re-emits the
// child's PRIMITIVE turn events (agent_start / text_delta / reasoning_delta /
// tool_start / tool_end) up to its own parent — it does NOT re-emit the child's
// `subagent_*` events. So a grandchild's `subagent_*` events surface ONLY on
// the CHILD session's subscriber (parentId = child.id, depth = 2), NOT on the
// ROOT session. `parentId` is therefore the IMMEDIATE parent, never the root.
// ===========================================================================

describe('Harness v1 real-agent E2E — S15 nested subagent depth>1', () => {
  it('a grandchild’s subagent_* events surface on the CHILD with depth 2 + parentId = immediate (child) session, NOT the root', async () => {
    // --- grandchild: a leaf agent that just streams text --------------------
    const grandDeltas = ['grand ', 'result'];
    const grandModel = new MockLanguageModelV2({
      doStream: async () => ({
        rawCall: { rawPrompt: null, rawSettings: {} },
        warnings: [],
        stream: textStream(grandDeltas),
      }),
    });
    const grandAgent = new Agent({
      id: 'grand-agent',
      name: 'grand-agent',
      instructions: 'reply',
      model: grandModel,
    });

    // --- child: spawns the grandchild, then streams its own summary ---------
    let childCall = 0;
    const childModel = new MockLanguageModelV2({
      doStream: async () => {
        childCall++;
        if (childCall === 1) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream(
              'child-spawn-tc',
              'spawn_subagent',
              JSON.stringify({ agentType: 'deep', task: 'Go one level deeper.' }),
            ),
          };
        }
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: textStream(['child ', 'done']),
        };
      },
    });
    const childAgent = new Agent({
      id: 'child-agent',
      name: 'child-agent',
      instructions: 'delegate deeper',
      model: childModel,
    });

    // --- parent: spawns the child, then streams its final text --------------
    let parentCall = 0;
    const parentModel = new MockLanguageModelV2({
      doStream: async () => {
        parentCall++;
        if (parentCall === 1) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream(
              'parent-spawn-tc',
              'spawn_subagent',
              JSON.stringify({ agentType: 'explore', task: 'Delegate to a child that delegates.' }),
            ),
          };
        }
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: textStream(['parent ', 'done']),
        };
      },
    });
    const parentAgent = new Agent({
      id: 'parent-agent',
      name: 'parent-agent',
      instructions: 'delegate',
      model: parentModel,
    });

    // maxDepth=2 admits depth-1 (child) and depth-2 (grandchild).
    const harness = new Harness({
      agents: {
        'parent-agent': parentAgent,
        'child-agent': childAgent,
        'grand-agent': grandAgent,
      } as any,
      storage: new InMemoryStore(),
      modes: [
        { id: 'default', agentId: 'parent-agent' },
        { id: 'explore-mode', agentId: 'child-agent' },
        { id: 'deep-mode', agentId: 'grand-agent' },
      ],
      defaultModeId: 'default',
      subagents: {
        maxDepth: 2,
        types: {
          explore: {
            agentId: 'child-agent',
            modeId: 'explore-mode',
            description: 'delegates deeper',
            defaultModelId: 'openai/gpt-4o-mini',
            workspace: 'inherit',
          },
          deep: {
            agentId: 'grand-agent',
            modeId: 'deep-mode',
            description: 'leaf',
            defaultModelId: 'openai/gpt-4o-mini',
            workspace: 'inherit',
          },
        },
      },
    });

    // Capture EVERY descendant session's events so we can see where each hop's
    // events land. Wrap harness.session so every subagent-tool child session
    // gets a subscriber.
    const rootEvents: HarnessEvent[] = [];
    const perSession = new Map<string, HarnessEvent[]>();
    const origSession = harness.session.bind(harness);
    (harness as any).session = async (opts: any) => {
      const s = await origSession(opts);
      if ((opts as any)?.origin === 'subagent-tool') {
        const arr: HarnessEvent[] = [];
        perSession.set(s.id, arr);
        s.subscribe(e => arr.push(e));
      }
      return s;
    };

    try {
      const session = await harness.session({ resourceId: 'u-nested', threadId: { fresh: true } });
      session.subscribe(e => rootEvents.push(e));

      const result = (await session.message({ content: 'delegate nested' })) as any;
      expect(result.text).toBe('parent done');

      // --- Root sees the CHILD as a depth-1 subagent --------------------------
      const rootSubStart = rootEvents.find(e => e.type === 'subagent_start') as
        | { parentId: string; depth: number; subagentSessionId: string; agentType: string }
        | undefined;
      expect(rootSubStart).toBeDefined();
      expect(rootSubStart!.agentType).toBe('explore');
      expect(rootSubStart!.depth).toBe(1);
      expect(rootSubStart!.parentId).toBe(session.id);
      const childSessionId = rootSubStart!.subagentSessionId;

      // The CHILD's text surfaced on the root as depth-1 subagent_text_delta.
      const rootChildText = rootEvents.filter(e => e.type === 'subagent_text_delta') as Array<{
        delta: string;
        depth: number;
        subagentSessionId: string;
      }>;
      expect(rootChildText.every(e => e.depth === 1)).toBe(true);
      expect(rootChildText.every(e => e.subagentSessionId === childSessionId)).toBe(true);
      expect(rootChildText.map(e => e.delta).join('')).toBe('child done');

      // FINDING: the grandchild's subagent_* events do NOT reach the root —
      // the bridge only re-emits the child's primitive events upward.
      const rootSubStarts = rootEvents.filter(e => e.type === 'subagent_start');
      expect(rootSubStarts).toHaveLength(1); // only the child, NOT the grandchild
      const rootDepths = (rootEvents.filter(e => e.type.startsWith('subagent_')) as Array<{ depth?: number }>).map(
        e => e.depth,
      );
      expect(rootDepths.every(d => d === 1)).toBe(true); // root never sees depth 2

      // --- The grandchild's subagent_* events surface on the CHILD session ---
      const childArr = perSession.get(childSessionId);
      expect(childArr).toBeDefined();
      const grandSubStart = childArr!.find(e => e.type === 'subagent_start') as
        | { parentId: string; depth: number; subagentSessionId: string; agentType: string }
        | undefined;
      expect(grandSubStart).toBeDefined();
      expect(grandSubStart!.agentType).toBe('deep');
      // depth is 2 at the grandchild hop.
      expect(grandSubStart!.depth).toBe(2);
      // parentId is the IMMEDIATE parent (the child session), NOT the root.
      expect(grandSubStart!.parentId).toBe(childSessionId);
      expect(grandSubStart!.parentId).not.toBe(session.id);
      const grandSessionId = grandSubStart!.subagentSessionId;
      expect(grandSessionId).not.toBe(childSessionId);
      expect(grandSessionId).not.toBe(session.id);

      // The grandchild's text surfaced on the CHILD as depth-2 subagent_text_delta.
      const childGrandText = childArr!.filter(e => e.type === 'subagent_text_delta') as Array<{
        delta: string;
        depth: number;
        subagentSessionId: string;
      }>;
      expect(childGrandText.every(e => e.depth === 2)).toBe(true);
      expect(childGrandText.every(e => e.subagentSessionId === grandSessionId)).toBe(true);
      expect(childGrandText.map(e => e.delta).join('')).toBe(grandDeltas.join(''));

      // The child ran its real loop (spawn + summary).
      expect(childCall).toBe(2);
    } finally {
      await harness.shutdown();
    }
  });
});

// ===========================================================================
// S16 — TWO subagents from one parent: events de-multiplex by subagentSessionId.
//
// A real parent spawns TWO subagents back-to-back. The UI keys child progress
// off `subagentSessionId`; this asserts the two children's events are cleanly
// de-multiplexed with NO cross-attribution (each child's text only carries its
// own session id).
// ===========================================================================

describe('Harness v1 real-agent E2E — S16 two subagents de-multiplex by subagentSessionId', () => {
  it('two real subagents spawned from one parent keep their events attributed to their own subagentSessionId', async () => {
    const makeChild = (id: string, text: string) => {
      let childCall = 0;
      const continueAfterSummary = createTool({
        id: 'continueAfterSummary',
        description: 'Acknowledge the visible specialist summary before the terminal report.',
        inputSchema: z.object({}),
        execute: async () => ({ acknowledged: true }),
      });
      const model = new MockLanguageModelV2({
        doStream: async () => {
          childCall += 1;
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream:
              childCall === 1
                ? reasoningTextToolCallStream([], [text], `${id}-progress-tc`, 'continueAfterSummary', '{}')
                : toolCallStream(
                    `${id}-outcome-tc`,
                    'report_subagent_outcome',
                    JSON.stringify({
                      outcome: 'completed',
                      summary: text,
                      evidence: [
                        {
                          kind: 'tool-result',
                          toolName: 'continueAfterSummary',
                          toolCallId: `${id}-progress-tc`,
                          status: 'success',
                          description: `${id} completed its deterministic assignment.`,
                        },
                      ],
                    }),
                  ),
          };
        },
      });
      return new Agent({ id, name: id, instructions: 'reply', model, tools: { continueAfterSummary } });
    };
    const childA = makeChild('child-a', 'alpha');
    const childB = makeChild('child-b', 'beta');

    // Parent: spawn A (call-1), spawn B (call-2), final text (call-3).
    let parentCall = 0;
    const parentModel = new MockLanguageModelV2({
      doStream: async () => {
        parentCall++;
        if (parentCall === 1) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream(
              'parent-spawn-a',
              'spawn_subagent',
              JSON.stringify({ agentType: 'alpha', task: 'Task A.' }),
            ),
          };
        }
        if (parentCall === 2) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream(
              'parent-spawn-b',
              'spawn_subagent',
              JSON.stringify({ agentType: 'beta', task: 'Task B.' }),
            ),
          };
        }
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: textStream(['both ', 'spawned']),
        };
      },
    });
    const parentAgent = new Agent({
      id: 'parent-agent',
      name: 'parent-agent',
      instructions: 'spawn two subagents',
      model: parentModel,
    });

    const harness = new Harness({
      agents: { 'parent-agent': parentAgent, 'child-a': childA, 'child-b': childB } as any,
      storage: new InMemoryStore(),
      modes: [
        { id: 'default', agentId: 'parent-agent' },
        { id: 'alpha-mode', agentId: 'child-a' },
        { id: 'beta-mode', agentId: 'child-b' },
      ],
      defaultModeId: 'default',
      subagents: {
        maxDepth: 2,
        types: {
          alpha: {
            agentId: 'child-a',
            modeId: 'alpha-mode',
            description: 'A',
            defaultModelId: 'openai/gpt-4o-mini',
            workspace: 'inherit',
          },
          beta: {
            agentId: 'child-b',
            modeId: 'beta-mode',
            description: 'B',
            defaultModelId: 'openai/gpt-4o-mini',
            workspace: 'inherit',
          },
        },
      },
    });

    try {
      const session = await harness.session({ resourceId: 'u-two-sub', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));

      const result = (await session.message({ content: 'spawn two' })) as any;
      expect(result.text).toBe('both spawned');

      // Two distinct subagent_start events, one per child, distinct sessions.
      const subStarts = events.filter(e => e.type === 'subagent_start') as Array<{
        agentType: string;
        subagentSessionId: string;
        toolCallId: string;
      }>;
      expect(subStarts).toHaveLength(2);
      const byType = new Map(subStarts.map(s => [s.agentType, s]));
      const aId = byType.get('alpha')!.subagentSessionId;
      const bId = byType.get('beta')!.subagentSessionId;
      expect(aId).toBeDefined();
      expect(bId).toBeDefined();
      expect(aId).not.toBe(bId);
      // The two spawns carry the parent's two distinct tool-call ids.
      expect(byType.get('alpha')!.toolCallId).toBe('parent-spawn-a');
      expect(byType.get('beta')!.toolCallId).toBe('parent-spawn-b');

      // De-multiplexing: each child's text deltas carry ONLY its own session id.
      const textDeltas = events.filter(e => e.type === 'subagent_text_delta') as Array<{
        delta: string;
        subagentSessionId: string;
      }>;
      const aText = textDeltas
        .filter(e => e.subagentSessionId === aId)
        .map(e => e.delta)
        .join('');
      const bText = textDeltas
        .filter(e => e.subagentSessionId === bId)
        .map(e => e.delta)
        .join('');
      expect(aText).toBe('alpha');
      expect(bText).toBe('beta');
      // No delta is attributed to neither child (no cross-attribution).
      expect(textDeltas.every(e => e.subagentSessionId === aId || e.subagentSessionId === bId)).toBe(true);

      // Two clean subagent_end events, one per child session.
      const subEnds = events.filter(e => e.type === 'subagent_end') as Array<{
        subagentSessionId: string;
        isError: boolean;
        output: any;
      }>;
      expect(subEnds).toHaveLength(2);
      expect(subEnds.every(e => e.isError === false)).toBe(true);
      const endById = new Map(subEnds.map(e => [e.subagentSessionId, e]));
      expect((endById.get(aId)!.output as { text?: string }).text).toBe('alpha');
      expect((endById.get(bId)!.output as { text?: string }).text).toBe('beta');
    } finally {
      await harness.shutdown();
    }
  });
});

// ===========================================================================
// S17 — SUBAGENT INTERLEAVING tool→text→tool in the child's real stream.
//
// S7's child is tool→text. This drives a child whose ONE turn segment streams
// tool-call → text → second tool-call, asserting the PARENT sees the
// subagent_* events in the real streamed order.
//
// CHARACTERIZATION (real AI-SDK v5 step semantics, NOT a harness bug): within a
// single model step the provider streams its OUTPUT chunks live — both
// `tool-call`s and the text between them — but the tools EXECUTE after the
// step's output stream drains, so the `tool-result` chunks (→ subagent_tool_end)
// arrive BATCHED at the end of the step, after BOTH tool_starts. The observed,
// genuine order is therefore:
//   subagent_tool_start(tc-1) → text → subagent_tool_start(tc-2)
//     → subagent_tool_end(tc-1) → subagent_tool_end(tc-2)
// i.e. the model's streamed tool_start↔text interleave is faithfully preserved
// (tc-1 start before the text, tc-2 start after it), and both tool_ends settle
// in call order at step end. We assert that real ordering, not a
// start/end-per-tool model the streaming loop does not produce.
// ===========================================================================

describe('Harness v1 real-agent E2E — S17 subagent interleaves tool→text→tool', () => {
  it('a real subagent that calls a tool, streams text, then calls a second tool surfaces subagent_* events in streamed order', async () => {
    const toolOne = createTool({
      id: 'first',
      description: 'first tool',
      inputSchema: z.object({ a: z.number() }),
      execute: async input => ({ one: (input as { a: number }).a }),
    });
    const toolTwo = createTool({
      id: 'second',
      description: 'second tool',
      inputSchema: z.object({ b: z.number() }),
      execute: async input => ({ two: (input as { b: number }).b }),
    });

    const childText = ['between ', 'the ', 'tools'];
    let childCall = 0;
    const childModel = new MockLanguageModelV2({
      doStream: async () => {
        childCall++;
        if (childCall === 1) {
          // ONE segment: tool → text → tool, finishing tool-calls.
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolTextToolStream('child-tc-1', 'first', '{"a":1}', childText, 'child-tc-2', 'second', '{"b":2}'),
          };
        }
        // Post-tools summary.
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: textStream(['done']),
        };
      },
    });
    const childAgent = new Agent({
      id: 'child-agent',
      name: 'child-agent',
      instructions: 'first, talk, second',
      model: childModel,
      tools: { first: toolOne, second: toolTwo },
    });

    let parentCall = 0;
    const parentModel = new MockLanguageModelV2({
      doStream: async () => {
        parentCall++;
        if (parentCall === 1) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream(
              'parent-spawn-tc',
              'spawn_subagent',
              JSON.stringify({ agentType: 'explore', task: 'Interleave.' }),
            ),
          };
        }
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: textStream(['ok']),
        };
      },
    });
    const parentAgent = new Agent({
      id: 'parent-agent',
      name: 'parent-agent',
      instructions: 'delegate',
      model: parentModel,
    });

    const harness = new Harness({
      agents: { 'parent-agent': parentAgent, 'child-agent': childAgent } as any,
      storage: new InMemoryStore(),
      modes: [
        { id: 'default', agentId: 'parent-agent' },
        { id: 'explore-mode', agentId: 'child-agent' },
      ],
      defaultModeId: 'default',
      subagents: {
        maxDepth: 2,
        types: {
          explore: {
            agentId: 'child-agent',
            modeId: 'explore-mode',
            description: 'interleaves',
            defaultModelId: 'openai/gpt-4o-mini',
            workspace: 'inherit',
          },
        },
      },
    });

    try {
      const session = await harness.session({ resourceId: 'u-interleave-sub', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));

      await session.message({ content: 'delegate interleaved' });

      // Project the subagent stream to a compact ordered shape so we can assert
      // the real streamed tool→text→tool ordering.
      const subSeq = (
        events.filter(
          e => e.type === 'subagent_tool_start' || e.type === 'subagent_tool_end' || e.type === 'subagent_text_delta',
        ) as Array<{ type: string; toolName?: string; innerToolCallId?: string; delta?: string }>
      ).map(e =>
        e.type === 'subagent_text_delta'
          ? { kind: 'text', delta: e.delta, id: undefined as string | undefined }
          : { kind: e.type, delta: undefined as string | undefined, id: e.innerToolCallId },
      );

      // Indices of the structural milestones.
      const firstStartIdx = subSeq.findIndex(s => s.kind === 'subagent_tool_start' && s.id === 'child-tc-1');
      const secondStartIdx = subSeq.findIndex(s => s.kind === 'subagent_tool_start' && s.id === 'child-tc-2');
      const firstEndIdx = subSeq.findIndex(s => s.kind === 'subagent_tool_end' && s.id === 'child-tc-1');
      const secondEndIdx = subSeq.findIndex(s => s.kind === 'subagent_tool_end' && s.id === 'child-tc-2');
      // The BETWEEN-tools text indices (the child also streams a trailing "done"
      // summary text after both tools, so scope to the deltas before tc-2 start).
      const interleaveTextIdxs = subSeq
        .map((s, i) => ({ s, i }))
        .filter(({ s, i }) => s.kind === 'text' && i < secondStartIdx)
        .map(({ i }) => i);

      // All milestones present.
      expect(firstStartIdx).toBeGreaterThanOrEqual(0);
      expect(secondStartIdx).toBeGreaterThanOrEqual(0);
      expect(firstEndIdx).toBeGreaterThanOrEqual(0);
      expect(secondEndIdx).toBeGreaterThanOrEqual(0);
      expect(interleaveTextIdxs.length).toBe(childText.length);

      // The model's streamed tool_start↔text interleave is preserved: the first
      // tool_start precedes the between-tools text, which precedes the second
      // tool_start (tool→text→tool, in the real streamed output order).
      expect(firstStartIdx).toBeLessThan(interleaveTextIdxs[0]!);
      expect(interleaveTextIdxs[interleaveTextIdxs.length - 1]!).toBeLessThan(secondStartIdx);

      // Step semantics: both tool_ends settle in call order AFTER both starts
      // (see header characterization — tool-results batch at step end).
      expect(firstEndIdx).toBeGreaterThan(secondStartIdx);
      expect(secondEndIdx).toBeGreaterThan(firstEndIdx);

      // The interleaved (between-tools) text was the child's between-tools deltas.
      const interleavedText = interleaveTextIdxs.map(i => subSeq[i]!.delta).join('');
      expect(interleavedText).toBe(childText.join(''));

      // Both child tools ran with their own ids + results, in call order.
      const subToolEnds = events.filter(e => e.type === 'subagent_tool_end') as Array<{
        innerToolCallId: string;
        toolName: string;
        output: any;
      }>;
      expect(subToolEnds.map(e => e.innerToolCallId)).toEqual(['child-tc-1', 'child-tc-2']);
      expect(subToolEnds[0]!.output).toEqual({ one: 1 });
      expect(subToolEnds[1]!.output).toEqual({ two: 2 });
    } finally {
      await harness.shutdown();
    }
  });
});

// ===========================================================================
// S18 — SUBAGENT TOOL ARGS (GAP-A regression): subagent_tool_start.input.
//
// The emit surface was enriched so subagent_tool_start carries the projected
// `input` args. S7 already checks this incidentally; S18 isolates it as a
// dedicated regression matching exactly what the child called the tool with.
// ===========================================================================

describe('Harness v1 real-agent E2E — S18 subagent_tool_start carries projected input (GAP-A)', () => {
  it('subagent_tool_start.input equals the child’s tool args, JSON-safe', async () => {
    let seenInput: unknown;
    const childTool = createTool({
      id: 'query',
      description: 'query',
      inputSchema: z.object({ q: z.string(), limit: z.number() }),
      execute: async input => {
        seenInput = input;
        return { ok: true };
      },
    });
    let childCall = 0;
    const childModel = new MockLanguageModelV2({
      doStream: async () => {
        childCall++;
        if (childCall === 1) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream('child-args-tc', 'query', '{"q":"find me","limit":5}'),
          };
        }
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: textStream(['done']),
        };
      },
    });
    const childAgent = new Agent({
      id: 'child-agent',
      name: 'child-agent',
      instructions: 'query',
      model: childModel,
      tools: { query: childTool },
    });

    let parentCall = 0;
    const parentModel = new MockLanguageModelV2({
      doStream: async () => {
        parentCall++;
        if (parentCall === 1) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream(
              'parent-spawn-tc',
              'spawn_subagent',
              JSON.stringify({ agentType: 'explore', task: 'Query.' }),
            ),
          };
        }
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: textStream(['ok']),
        };
      },
    });
    const parentAgent = new Agent({
      id: 'parent-agent',
      name: 'parent-agent',
      instructions: 'delegate',
      model: parentModel,
    });

    const harness = new Harness({
      agents: { 'parent-agent': parentAgent, 'child-agent': childAgent } as any,
      storage: new InMemoryStore(),
      modes: [
        { id: 'default', agentId: 'parent-agent' },
        { id: 'explore-mode', agentId: 'child-agent' },
      ],
      defaultModeId: 'default',
      subagents: {
        maxDepth: 2,
        types: {
          explore: {
            agentId: 'child-agent',
            modeId: 'explore-mode',
            description: 'query',
            defaultModelId: 'openai/gpt-4o-mini',
            workspace: 'inherit',
          },
        },
      },
    });

    try {
      const session = await harness.session({ resourceId: 'u-sub-args', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));

      await session.message({ content: 'delegate query' });

      const subToolStart = events.find(e => e.type === 'subagent_tool_start') as
        | { toolName: string; innerToolCallId: string; input: any }
        | undefined;
      expect(subToolStart).toBeDefined();
      expect(subToolStart!.toolName).toBe('query');
      expect(subToolStart!.innerToolCallId).toBe('child-args-tc');
      // GAP-A: the projected input args reached the parent stream, equal to what
      // the child actually called the tool with.
      expect(subToolStart!.input).toEqual({ q: 'find me', limit: 5 });
      expect(subToolStart!.input).toEqual(seenInput);
    } finally {
      await harness.shutdown();
    }
  });
});

// ===========================================================================
// S19 — SENTINEL on the LIVE subscriber path (not just replay).
//
// session.events.test.ts pins the oversized/unserializable sentinel on the
// REPLAY path with a FakeAgent. This drives a REAL tool whose output exceeds a
// configured files.maxEventPayloadBytes and asserts a LIVE subscriber's
// tool_end carries the oversized sentinel, NOT the raw value.
// ===========================================================================

describe('Harness v1 real-agent E2E — S19 oversized tool payload sentinel on the live path', () => {
  it('a real tool whose output exceeds files.maxEventPayloadBytes surfaces the oversized sentinel to a LIVE subscriber', async () => {
    // A tool returning a payload comfortably over a tiny 64-byte cap.
    const bigTool = createTool({
      id: 'big',
      description: 'returns an oversized payload',
      inputSchema: z.object({}),
      execute: async () => ({ blob: 'x'.repeat(500) }),
    });
    let callCount = 0;
    const model = new MockLanguageModelV2({
      doStream: async () => {
        callCount++;
        if (callCount === 1) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream('big-tc', 'big', '{}'),
          };
        }
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: textStream(['summarized']),
        };
      },
    });
    const agent = new Agent({
      id: 'default',
      name: 'default',
      instructions: 'use big',
      model,
      tools: { big: bigTool },
    });

    // files.maxEventPayloadBytes flows to _internalMaxEventPayloadBytes and is
    // applied AT EMIT (session.ts:4380-4384), so the LIVE subscriber sees the
    // sentinel — not just the replay path.
    const harness = new Harness({
      agents: { default: agent } as any,
      storage: new InMemoryStore(),
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      files: { maxEventPayloadBytes: 64 },
    });
    try {
      const session = await harness.session({ resourceId: 'u-oversized', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));

      await session.message({ content: 'go big' });

      const toolEnd = events.find(e => e.type === 'tool_end') as
        | { toolName: string; output: any; isError: boolean }
        | undefined;
      expect(toolEnd).toBeDefined();
      expect(toolEnd!.toolName).toBe('big');
      expect(toolEnd!.isError).toBe(false);
      // LIVE subscriber sees ONLY the bounded sentinel, never the raw 500-byte blob.
      expect(toolEnd!.output).toEqual({ __mastraHarness: 'oversized-tool-payload' });
      expect(JSON.stringify(toolEnd!.output)).not.toContain('xxxx');
    } finally {
      await harness.shutdown();
    }
  });
});

// ===========================================================================
// S20 — REAL top-level TOOL ERROR reaches a live subscriber as tool_end{isError}.
//
// S2 only covers the success path. A REAL top-level tool that throws must
// surface tool_end{isError:true} with the error detail to a live subscriber,
// and the run must continue (the loop feeds the error back to the model).
// ===========================================================================

describe('Harness v1 real-agent E2E — S20 top-level tool error', () => {
  it('a real top-level tool that throws surfaces tool_end{isError:true} with the error detail to a subscriber', async () => {
    const THROWN = 'findUser failed: connection refused at db.internal:5432';
    const findUser = createTool({
      id: 'findUser',
      description: 'look up a user (throws)',
      inputSchema: z.object({ name: z.string() }),
      execute: async () => {
        throw new Error(THROWN);
      },
    });
    let callCount = 0;
    const model = new MockLanguageModelV2({
      doStream: async () => {
        callCount++;
        if (callCount === 1) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream('err-tc', 'findUser', '{"name":"Dero"}'),
          };
        }
        // The loop feeds the tool error back to the model; it streams a recovery.
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: textStream(['Could not find the user.']),
        };
      },
    });
    const agent = new Agent({
      id: 'default',
      name: 'default',
      instructions: 'use findUser',
      model,
      tools: { findUser },
    });
    const harness = newHarness(agent);
    try {
      const session = await harness.session({ resourceId: 'u-tool-err', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));

      const result = (await session.message({ content: 'find Dero' })) as any;

      const toolEnd = events.find(e => e.type === 'tool_end') as
        | { toolName: string; toolCallId: string; output: any; isError: boolean }
        | undefined;
      expect(toolEnd).toBeDefined();
      expect(toolEnd!.toolName).toBe('findUser');
      expect(toolEnd!.toolCallId).toBe('err-tc');
      expect(toolEnd!.isError).toBe(true);
      // The tool's OWN error is faithfully preserved (NOT redacted to
      // harness.internal — session.ts:4399-4403), JSON-safe.
      expect(JSON.stringify(toolEnd!.output)).toContain(THROWN);
      expect(toolEnd!.output).not.toBeInstanceOf(Error);

      // The run continued past the tool error to a clean terminal.
      const ends = events.filter(e => e.type === 'agent_end') as Array<{ finishReason: string }>;
      expect(ends.some(e => e.finishReason === 'complete')).toBe(true);
      expect(result.text).toBe('Could not find the user.');
    } finally {
      await harness.shutdown();
    }
  });
});

// ===========================================================================
// S21 — REASONING DELTA (GAP-B regression): top-level reasoning_delta.
//
// The emit surface now streams reasoning. A real Agent whose model emits
// reasoning-delta chunks must surface harness reasoning_delta events whose
// concatenation == the reasoning text, BEFORE the text deltas. (The subagent
// variant — subagent_reasoning_delta — is already asserted in S7.)
// ===========================================================================

describe('Harness v1 real-agent E2E — S21 reasoning_delta streams (GAP-B)', () => {
  it('a real Agent emitting reasoning-delta chunks surfaces reasoning_delta events == the reasoning text, before text_delta', async () => {
    const reasoning = ['Let me ', 'think ', 'carefully.'];
    const text = ['The ', 'answer.'];
    const model = new MockLanguageModelV2({
      doStream: async () => ({
        rawCall: { rawPrompt: null, rawSettings: {} },
        warnings: [],
        stream: reasoningTextStream(reasoning, text),
      }),
    });
    const agent = new Agent({ id: 'default', name: 'default', instructions: 'reason then reply', model });
    const harness = newHarness(agent);
    try {
      const session = await harness.session({ resourceId: 'u-reason', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));

      const result = (await session.message({ content: 'think' })) as any;

      const reasoningDeltas = events.filter(e => e.type === 'reasoning_delta') as Array<{
        delta: string;
        runId: string;
      }>;
      // GAP-B: each reasoning-delta chunk surfaced as a reasoning_delta event.
      expect(reasoningDeltas.map(e => e.delta)).toEqual(reasoning);
      expect(reasoningDeltas.map(e => e.delta).join('')).toBe(reasoning.join(''));

      // The text deltas surfaced separately and the final text is the text run.
      const textDeltas = events.filter(e => e.type === 'text_delta') as Array<{ delta: string }>;
      expect(textDeltas.map(e => e.delta)).toEqual(text);
      expect(result.text).toBe(text.join(''));

      // Ordering: all reasoning_delta events precede the first text_delta.
      const types = events.map(e => e.type);
      const lastReasoningIdx = types.lastIndexOf('reasoning_delta');
      const firstTextIdx = types.indexOf('text_delta');
      expect(lastReasoningIdx).toBeGreaterThanOrEqual(0);
      expect(firstTextIdx).toBeGreaterThan(lastReasoningIdx);

      // The reasoning_delta runId matches the turn's agent_end runId.
      const agentEnd = events.find(e => e.type === 'agent_end') as { runId: string };
      expect(reasoningDeltas.every(e => e.runId === agentEnd.runId)).toBe(true);
    } finally {
      await harness.shutdown();
    }
  });
});

// ===========================================================================
// S22 — EMPTY FINAL SYNTHESIS: successful tool work never leaves the user
// silent, and the one recovery request cannot repeat an action even when the
// provider ignores toolChoice. This runs through the real Session + Agent loop,
// including composition with Agent.defaultOptions hooks.
// ===========================================================================

describe('Harness v1 real-agent E2E — S22 empty final synthesis', () => {
  it('adds one tool-free response step while preserving configured agent hooks', async () => {
    let providerCalls = 0;
    let toolExecutions = 0;
    let configuredIterations = 0;
    let configuredPrepareSteps = 0;
    const providerOptions: any[] = [];
    const applyChange = createTool({
      id: 'applyChange',
      description: 'Apply one test change.',
      inputSchema: z.object({}),
      execute: async () => {
        toolExecutions += 1;
        return { applied: true };
      },
    });
    const model = new MockLanguageModelV2({
      doStream: async options => {
        providerOptions.push(options);
        providerCalls += 1;
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream:
            providerCalls === 1
              ? toolCallStream('apply-1', 'applyChange', '{}')
              : providerCalls === 2
                ? textStream([])
                : textStream(['Applied the change.']),
        };
      },
    });
    const agent = new Agent({
      id: 'default',
      name: 'default',
      instructions: 'apply the change and report the result',
      model,
      tools: { applyChange },
      defaultOptions: {
        onIterationComplete: async () => {
          configuredIterations += 1;
          return undefined;
        },
        prepareStep: async () => {
          configuredPrepareSteps += 1;
          return { activeTools: ['applyChange'] };
        },
      },
    });
    const harness = newHarness(agent);
    try {
      const session = await harness.session({ resourceId: 'u-empty-synthesis', threadId: { fresh: true } });
      const result = (await session.message({ content: 'apply it' })) as any;

      expect(result.text).toBe('Applied the change.');
      expect(providerCalls).toBe(3);
      expect(toolExecutions).toBe(1);
      expect(configuredIterations).toBe(3);
      expect(configuredPrepareSteps).toBe(3);
      expect(providerOptions[0]?.tools).toHaveLength(1);
      expect(providerOptions[2]?.tools ?? []).toHaveLength(0);
    } finally {
      await harness.shutdown();
    }
  });

  it('hard-stops after one tool-free recovery request when the provider tries to repeat the action', async () => {
    let providerCalls = 0;
    let toolExecutions = 0;
    const applyChange = createTool({
      id: 'applyChange',
      description: 'Apply one test change.',
      inputSchema: z.object({}),
      execute: async () => {
        toolExecutions += 1;
        return { applied: true };
      },
    });
    const model = new MockLanguageModelV2({
      doStream: async () => {
        providerCalls += 1;
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream:
            providerCalls === 1
              ? toolCallStream('apply-1', 'applyChange', '{}')
              : providerCalls === 2
                ? textStream([])
                : providerCalls === 3
                  ? toolCallStream('apply-2', 'applyChange', '{}')
                  : textStream(['UNEXPECTED_EXTRA_RECOVERY_STEP']),
        };
      },
    });
    const agent = new Agent({
      id: 'default',
      name: 'default',
      instructions: 'apply the change and report the result',
      model,
      tools: { applyChange },
      defaultOptions: {
        // Feedback on the recovery response would normally request a two-phase
        // stop. The Harness composer must discard it at the hard-stop point.
        onIterationComplete: async () => ({ feedback: 'Configured feedback.' }),
        prepareStep: async () => ({ activeTools: ['applyChange'] }),
      },
    });
    const harness = newHarness(agent);
    try {
      const session = await harness.session({ resourceId: 'u-empty-synthesis-cap', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(event => {
        events.push(event);
      });
      const result = (await session.message({ content: 'apply it once' })) as any;

      expect(providerCalls).toBe(3);
      expect(toolExecutions).toBe(1);
      expect(events.filter(event => event.type === 'tool_start')).toHaveLength(1);
      // A provider that violates the response-only request remains silent; the
      // hard security boundary wins over replaying the tool or buying another call.
      expect(result.text).toBe('');
      expect(result.text).not.toContain('UNEXPECTED_EXTRA_RECOVERY_STEP');
    } finally {
      await harness.shutdown();
    }
  });

  it('does not spend provider retry or fallback budget after admitting recovery', async () => {
    let primaryCalls = 0;
    let backupCalls = 0;
    let toolExecutions = 0;
    const applyChange = createTool({
      id: 'applyChange',
      description: 'Apply one test change.',
      inputSchema: z.object({}),
      execute: async () => {
        toolExecutions += 1;
        return { applied: true };
      },
    });
    const primary = new MockLanguageModelV2({
      doStream: async () => {
        primaryCalls += 1;
        if (primaryCalls === 1) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream('apply-1', 'applyChange', '{}'),
          };
        }
        if (primaryCalls === 2) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: textStream([]),
          };
        }
        throw new Error('recovery provider failed');
      },
    });
    const backup = new MockLanguageModelV2({
      doStream: async () => {
        backupCalls += 1;
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: textStream(['UNEXPECTED_FALLBACK_RECOVERY']),
        };
      },
    });
    const agent = new Agent({
      id: 'default',
      name: 'default',
      instructions: 'apply the change and report the result',
      model: [
        { model: primary, maxRetries: 2 },
        { model: backup, maxRetries: 2 },
      ],
      tools: { applyChange },
    });
    const harness = newHarness(agent);
    try {
      const session = await harness.session({ resourceId: 'u-empty-synthesis-retry', threadId: { fresh: true } });
      let rejection: unknown;
      try {
        await session.message({ content: 'apply it once' });
      } catch (error) {
        rejection = error;
      }

      expect(rejection).toMatchObject({
        name: 'HarnessExecutionError',
        message: 'An internal harness error occurred',
      });
      expect((rejection as { cause?: Error }).cause?.message).toBe('recovery provider failed');
      expect(primaryCalls).toBe(3);
      expect(backupCalls).toBe(0);
      expect(toolExecutions).toBe(1);
    } finally {
      await harness.shutdown();
    }
  });

  it('still synthesizes when a configured hook rejects on every iteration', async () => {
    let providerCalls = 0;
    let toolExecutions = 0;
    let configuredIterations = 0;
    const applyChange = createTool({
      id: 'applyChange',
      description: 'Apply one test change.',
      inputSchema: z.object({}),
      execute: async () => {
        toolExecutions += 1;
        return { applied: true };
      },
    });
    const model = new MockLanguageModelV2({
      doStream: async () => {
        providerCalls += 1;
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream:
            providerCalls === 1
              ? toolCallStream('apply-1', 'applyChange', '{}')
              : providerCalls === 2
                ? textStream([])
                : textStream(['Applied despite the optional hook failure.']),
        };
      },
    });
    const agent = new Agent({
      id: 'default',
      name: 'default',
      instructions: 'apply the change and report the result',
      model,
      tools: { applyChange },
      defaultOptions: {
        onIterationComplete: async () => {
          configuredIterations += 1;
          throw new Error('configured hook failed');
        },
      },
    });
    const harness = newHarness(agent);
    try {
      const session = await harness.session({ resourceId: 'u-empty-synthesis-hook-error', threadId: { fresh: true } });
      const result = (await session.message({ content: 'apply it' })) as any;

      expect(result.text).toBe('Applied despite the optional hook failure.');
      expect(providerCalls).toBe(3);
      expect(toolExecutions).toBe(1);
      expect(configuredIterations).toBe(3);
    } finally {
      await harness.shutdown();
    }
  });
});
