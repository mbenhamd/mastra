/**
 * Harness v1 — event surface (§10).
 *
 * Covers Session.subscribe() + Harness.subscribe() lifecycle:
 *   - subscribers receive events emitted after subscribe() returns
 *   - unsubscribe stops delivery
 *   - mode_changed / model_changed / session_closed lifecycle events
 *   - agent_start / message_* / tool_input_* / tool_start / tool_end /
 *     agent_end produced while draining a streaming agent's fullStream
 *   - suspension_required / suspension_resolved on suspend/resume round-trip
 *   - throwing subscriber is isolated; other subscribers still see events
 *   - harness-level subscribers see session_created and forwarded session events
 *   - event ids share a single epoch and are monotonic
 */

import { describe, expect, it, vi } from 'vitest';

import { Agent } from '../../agent';
import { InMemoryHarness } from '../../storage/domains/harness/inmemory';
import { InMemoryDB } from '../../storage/domains/inmemory-db';
import { buildFakeOutput } from './__test-utils__/fake-output';

import { parseHarnessEventId } from './events';
import type { HarnessEvent } from './events';
import { Harness } from './harness';

// ---------------------------------------------------------------------------
// Fake agent that drives a programmable fullStream + getFullOutput.
// ---------------------------------------------------------------------------

class FakeAgent extends Agent<any, any, any> {
  chunks: any[] = [];
  fullOutput: any = {
    text: 'ok',
    usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
    finishReason: 'stop',
    object: undefined,
    steps: [],
    warnings: [],
    providerMetadata: undefined,
    request: {},
    reasoning: [],
    reasoningText: undefined,
    toolCalls: [],
    toolResults: [],
    sources: [],
    files: [],
    response: { id: 'r', timestamp: new Date(), modelId: 'fake', messages: [], uiMessages: [] },
    totalUsage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
    error: undefined,
    tripwire: undefined,
    traceId: undefined,
    spanId: undefined,
    runId: 'fake-run',
    suspendPayload: undefined,
    messages: [],
    rememberedMessages: [],
  };

  constructor(name: string) {
    super({ id: name, name, instructions: 'fake', model: 'openai/gpt-4o-mini' as any });
  }

  async stream(_messages: any, options?: any): Promise<any> {
    const out = buildFakeOutput({
      runId: options?.runId ?? this.fullOutput.runId,
      fullOutput: this.fullOutput,
      chunks: this.chunks,
    });
    this._internalRegisterStreamRun(out, (options ?? {}) as any);
    return out;
  }

  async generate(_messages: any, _options?: any): Promise<any> {
    return this.fullOutput;
  }

  async resumeStream(_resumeData: any, options?: any): Promise<any> {
    return this.stream(undefined, options);
  }
}

function setup(opts?: { persistTransientStreamingEvents?: boolean; maxEventPayloadBytes?: number }) {
  const agent = new FakeAgent('default');
  const storage = new InMemoryHarness({ db: new InMemoryDB() });
  const harness = new Harness({
    agents: { default: agent } as any,
    modes: [
      { id: 'default', agentId: 'default' },
      { id: 'other', agentId: 'default' },
    ],
    defaultModeId: 'default',
    sessions: {
      storage,
      ...(opts?.persistTransientStreamingEvents !== undefined
        ? { persistTransientStreamingEvents: opts.persistTransientStreamingEvents }
        : {}),
    },
    ...(opts?.maxEventPayloadBytes !== undefined ? { files: { maxEventPayloadBytes: opts.maxEventPayloadBytes } } : {}),
  });
  return { harness, agent, storage };
}

describe('Session.subscribe()', () => {
  it('delivers events emitted after subscribe()', async () => {
    const { harness } = setup();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    const events: HarnessEvent[] = [];
    const off = session.subscribe(e => {
      events.push(e);
    });

    await session.message({ content: 'hi' });

    const types = events.map(e => e.type);
    expect(types).toContain('agent_start');
    expect(types).toContain('agent_end');
    expect(events.every(e => e.sessionId === session.id)).toBe(true);
    off();
  });

  it('stops delivering after unsubscribe()', async () => {
    const { harness } = setup();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    const events: HarnessEvent[] = [];
    const off = session.subscribe(e => {
      events.push(e);
    });
    off();
    await session.message({ content: 'hi' });

    expect(events).toEqual([]);
  });

  it('emits state_changed with full state + changedKeys on setState, and skips no-op writes (§10.2)', async () => {
    const { harness } = setup();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    const events: HarnessEvent[] = [];
    session.subscribe(e => {
      if (e.type === 'state_changed') events.push(e);
    });

    await session.setState({ a: 1, b: 2 });
    await session.setState({ b: 2 }); // b unchanged → no key changes → no event
    await session.setState({ a: 1, c: 3 }); // only c changes

    expect(events).toHaveLength(2);
    const first = events[0] as { state: Record<string, unknown>; changedKeys: string[] };
    expect(first.state).toEqual({ a: 1, b: 2 });
    expect(first.changedKeys.sort()).toEqual(['a', 'b']);
    const second = events[1] as { state: Record<string, unknown>; changedKeys: string[] };
    expect(second.state).toEqual({ a: 1, b: 2, c: 3 });
    expect(second.changedKeys).toEqual(['c']);
  });

  it('emits state_changed with the "$" root sentinel for scalar/array root changes (§10.2)', async () => {
    const { harness } = setup();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    const events: Array<{ state: unknown; changedKeys: string[] }> = [];
    session.subscribe(e => {
      if (e.type === 'state_changed') events.push(e as { state: unknown; changedKeys: string[] });
    });

    // Scalar root transition 1 -> 2: collapses to no top-level keys, but the
    // root value genuinely changed, so it must still emit under the '$' sentinel.
    await session.setState<number>(() => 1);
    await session.setState<number>(() => 1); // no-op, same scalar → suppressed
    await session.setState<number>(() => 2);
    // Array root transition ['a'] -> ['b']: same — sentinel-keyed.
    await session.setState<string[]>(() => ['a']);
    await session.setState<string[]>(() => ['a']); // no-op, equal array → suppressed
    await session.setState<string[]>(() => ['b']);

    expect(events.map(e => ({ state: e.state, changedKeys: e.changedKeys }))).toEqual([
      { state: 1, changedKeys: ['$'] },
      { state: 2, changedKeys: ['$'] },
      { state: ['a'], changedKeys: ['$'] },
      { state: ['b'], changedKeys: ['$'] },
    ]);
  });

  it('keeps object-root diffs unchanged when transitioning from a scalar root (§10.2)', async () => {
    const { harness } = setup();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    const events: Array<{ state: unknown; changedKeys: string[] }> = [];
    session.subscribe(e => {
      if (e.type === 'state_changed') events.push(e as { state: unknown; changedKeys: string[] });
    });

    // scalar -> object root: not both plain objects → '$' sentinel.
    await session.setState<unknown>(() => 5);
    await session.setState<unknown>(() => ({ a: 1 }));
    // object -> object: per-key diff (unchanged behavior).
    await session.setState<{ a: number; b?: number }>(prev => ({ ...prev, b: 2 }));

    expect(events).toHaveLength(3);
    expect(events[0]).toMatchObject({ state: 5, changedKeys: ['$'] });
    expect(events[1]).toMatchObject({ state: { a: 1 }, changedKeys: ['$'] });
    expect(events[2]).toMatchObject({ state: { a: 1, b: 2 }, changedKeys: ['b'] });
  });

  it('isolates a throwing subscriber from other subscribers', async () => {
    const { harness } = setup();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    const ok: HarnessEvent[] = [];
    session.subscribe(() => {
      throw new Error('boom');
    });
    session.subscribe(e => {
      ok.push(e);
    });

    // Producer must not throw.
    await expect(session.message({ content: 'hi' })).resolves.toBeDefined();

    expect(ok.some(e => e.type === 'agent_start')).toBe(true);
    expect(ok.some(e => e.type === 'agent_end')).toBe(true);
  });

  it('emits mode_changed and model_changed with previous ids', async () => {
    const { harness } = setup();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    const events: HarnessEvent[] = [];
    session.subscribe(e => {
      events.push(e);
    });

    await session.switchMode({ mode: 'other' });
    await session.models.switch({ model: 'gpt-5' });

    const mode = events.find(e => e.type === 'mode_changed');
    const model = events.find(e => e.type === 'model_changed');
    expect(mode).toMatchObject({ type: 'mode_changed', modeId: 'other', previousModeId: 'default' });
    expect(model).toMatchObject({ type: 'model_changed', modelId: 'gpt-5' });
  });

  it('skips mode_changed when the modeId is unchanged', async () => {
    const { harness } = setup();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    const events: HarnessEvent[] = [];
    session.subscribe(e => {
      events.push(e);
    });

    await session.switchMode({ mode: 'default' });
    expect(events.find(e => e.type === 'mode_changed')).toBeUndefined();
  });

  it('produces monotonic ids that share a single epoch', async () => {
    const { harness } = setup();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    const events: HarnessEvent[] = [];
    session.subscribe(e => {
      events.push(e);
    });
    await session.message({ content: 'hi' });

    expect(events.length).toBeGreaterThan(1);
    const parsed = events.map(e => parseHarnessEventId(e.id));
    expect(events.every(e => e.id.startsWith('harness-v1:'))).toBe(true);
    const epochs = new Set(parsed.map(e => e.epoch));
    expect(epochs.size).toBe(1);
    const seqs = parsed.map(e => e.sequence);
    for (let i = 1; i < seqs.length; i += 1) {
      expect(seqs[i]).toBeGreaterThan(seqs[i - 1]!);
    }
  });

  it('resumes the durable event epoch and sequence when a session is rehydrated', async () => {
    const { harness, agent, storage } = setup();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    await session.message({ content: 'hi' });
    await session._flushEventPersistence();
    await harness.shutdown();
    await session._flushEventPersistence();

    const state = await storage.getSessionEventReplayState({
      sessionId: session.id,
      resourceId: session.resourceId,
      threadId: session.threadId,
    });
    expect(state).not.toBeNull();

    const resumed = new Harness({
      agents: { default: agent } as any,
      modes: [
        { id: 'default', agentId: 'default' },
        { id: 'other', agentId: 'default' },
      ],
      defaultModeId: 'default',
      sessions: { storage },
    });
    try {
      const hydrated = await resumed.session({ resourceId: session.resourceId, threadId: session.threadId });
      const events: HarnessEvent[] = [];
      hydrated.subscribe(e => {
        events.push(e);
      });

      await hydrated.message({ content: 'again' });

      expect(events.length).toBeGreaterThan(0);
      const first = parseHarnessEventId(events[0]!.id);
      expect(first.epoch).toBe(state!.epoch);
      expect(first.sequence).toBeGreaterThan(state!.newestSequence);
    } finally {
      await resumed.shutdown();
    }
  });
});

describe('Session events — fullStream drain', () => {
  it('emits text_delta for each streamed text chunk (§10.2 — no message-boundary events)', async () => {
    const { harness, agent } = setup();
    agent.chunks = [
      { type: 'text-start', payload: { id: 'msg-1' }, runId: 'fake-run' },
      { type: 'text-delta', payload: { id: 'msg-1', text: 'hel' }, runId: 'fake-run' },
      { type: 'text-delta', payload: { id: 'msg-1', text: 'lo' }, runId: 'fake-run' },
      { type: 'text-end', payload: { id: 'msg-1' }, runId: 'fake-run' },
    ];
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    const events: HarnessEvent[] = [];
    session.subscribe(e => {
      events.push(e);
    });
    await session.message({ content: 'hi' });

    // §10.2 TurnEvent has only `text_delta` (no message_start/update/end).
    const types = events.map(e => e.type);
    expect(types).not.toContain('message_start');
    expect(types).not.toContain('message_update');
    expect(types).not.toContain('message_end');
    const textDeltas = events.filter(e => e.type === 'text_delta') as Array<
      Extract<HarnessEvent, { type: 'text_delta' }>
    >;
    expect(textDeltas.map(e => e.delta)).toEqual(['hel', 'lo']);
    expect(textDeltas.every(e => e.runId === 'fake-run')).toBe(true);
  });

  it('emits reasoning_delta for each streamed reasoning chunk, gating on non-empty text (§10.2)', async () => {
    const { harness, agent } = setup();
    // Mastra-level fullStream chunks. `reasoning-delta` carries `payload.text`
    // (stream/types.ts ReasoningDeltaPayload), mirroring `text-delta`.
    agent.chunks = [
      { type: 'reasoning-start', payload: { id: 'r-1' }, runId: 'fake-run' },
      { type: 'reasoning-delta', payload: { id: 'r-1', text: 'think' }, runId: 'fake-run' },
      { type: 'reasoning-delta', payload: { id: 'r-1', text: 'ing' }, runId: 'fake-run' },
      // Empty reasoning text must not produce an event (mirrors text-delta).
      { type: 'reasoning-delta', payload: { id: 'r-1', text: '' }, runId: 'fake-run' },
      { type: 'reasoning-end', payload: { id: 'r-1' }, runId: 'fake-run' },
      { type: 'text-delta', payload: { id: 'msg-1', text: 'answer' }, runId: 'fake-run' },
    ];
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    const events: HarnessEvent[] = [];
    session.subscribe(e => {
      events.push(e);
    });
    await session.message({ content: 'hi' });

    const reasoningDeltas = events.filter(e => e.type === 'reasoning_delta') as Array<
      Extract<HarnessEvent, { type: 'reasoning_delta' }>
    >;
    expect(reasoningDeltas.map(e => e.delta)).toEqual(['think', 'ing']);
    expect(reasoningDeltas.every(e => e.runId === 'fake-run')).toBe(true);
  });

  it('emits NO reasoning_delta when the model streams no reasoning (additive — never forced on)', async () => {
    const { harness, agent } = setup();
    agent.chunks = [{ type: 'text-delta', payload: { id: 'msg-1', text: 'no thinking here' }, runId: 'fake-run' }];
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    const events: HarnessEvent[] = [];
    session.subscribe(e => {
      events.push(e);
    });
    await session.message({ content: 'hi' });

    expect(events.map(e => e.type)).not.toContain('reasoning_delta');
  });

  const streamChunks = [
    { type: 'text-start', payload: { id: 'm' }, runId: 'fake-run' },
    { type: 'text-delta', payload: { id: 'm', text: 'hi' }, runId: 'fake-run' },
    { type: 'text-end', payload: { id: 'm' }, runId: 'fake-run' },
  ];
  async function persistedEventTypes(
    storage: InMemoryHarness,
    session: { id: string; resourceId: string; threadId: string },
  ): Promise<string[]> {
    const state = await storage.getSessionEventReplayState({
      sessionId: session.id,
      resourceId: session.resourceId,
      threadId: session.threadId,
    });
    if (!state) return [];
    const rows = await storage.listSessionEvents({
      sessionId: session.id,
      resourceId: session.resourceId,
      threadId: session.threadId,
      epoch: state.epoch,
      afterSequence: 0,
      limit: 1000,
    });
    return rows.map(row => (row.event as { type: string }).type);
  }

  it('persists text_delta to the durable event log by default (§10.5)', async () => {
    const { harness, agent, storage } = setup();
    agent.chunks = streamChunks;
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    await session.message({ content: 'go' });
    await session._flushEventPersistence();
    expect(await persistedEventTypes(storage, session)).toContain('text_delta');
  });

  it('skips persisting text_delta when persistTransientStreamingEvents=false, keeping live delivery + durable events (§10.5)', async () => {
    const { harness, agent, storage } = setup({ persistTransientStreamingEvents: false });
    agent.chunks = streamChunks;
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const liveDeltas: string[] = [];
    session.subscribe(e => {
      if (e.type === 'text_delta') liveDeltas.push((e as Extract<HarnessEvent, { type: 'text_delta' }>).delta);
    });
    await session.message({ content: 'go' });
    await session._flushEventPersistence();

    // Live subscribers still receive the delta in real time...
    expect(liveDeltas).toContain('hi');
    const persisted = await persistedEventTypes(storage, session);
    // ...but it is NOT written to the durable event log (no per-token write amplification)...
    expect(persisted).not.toContain('text_delta');
    // ...while durable lifecycle events still persist.
    expect(persisted).toContain('agent_end');
  });

  it('mints a FRESH event epoch on rehydrate when transient deltas are not persisted (no seq reuse, §10.5)', async () => {
    // Codex-found edge: a skipped tail delta advances the live seq but not the persisted
    // newest, so reusing `newestSequence + 1` on rehydrate would reuse a seq the client
    // already saw. With persistence off, rehydrate must NOT reuse the cursor — a fresh
    // epoch means a prior-epoch Last-Event-ID gets a 412 stale_epoch + snapshot recovery.
    const storage = new InMemoryHarness({ db: new InMemoryDB() });
    const make = () => {
      const agent = new FakeAgent('default');
      agent.chunks = streamChunks;
      return new Harness({
        agents: { default: agent } as any,
        modes: [{ id: 'default', agentId: 'default' }],
        defaultModeId: 'default',
        sessions: { storage, persistTransientStreamingEvents: false },
      });
    };

    const h1 = make();
    const s1 = await h1.session({ resourceId: 'u1', threadId: { fresh: true } });
    let epoch1: string | undefined;
    s1.subscribe(e => {
      if (e.type === 'agent_end') epoch1 = parseHarnessEventId(e.id).epoch;
    });
    await s1.message({ content: 'first' });
    await s1._flushEventPersistence();
    await h1.shutdown();

    // Rehydrate in a fresh harness (new owner) over the same storage.
    const h2 = make();
    const s2 = await h2.session({ sessionId: s1.id });
    let epoch2: string | undefined;
    s2.subscribe(e => {
      if (e.type === 'agent_end') epoch2 = parseHarnessEventId(e.id).epoch;
    });
    await s2.message({ content: 'second' });
    await h2.shutdown();

    expect(epoch1).toBeDefined();
    expect(epoch2).toBeDefined();
    expect(epoch2).not.toBe(epoch1);
  });

  it('attributes text_delta/tool_start/tool_end to the active run when chunks omit runId (§10.2)', async () => {
    // Real AI SDK stream parts surfaced by the long-lived thread subscription
    // do NOT carry their own runId. agent_start stamps `_currentRunId`, so
    // `_emitForChunk` attributes these chunks to the active run rather than
    // silently dropping them (regression guard — these previously vanished
    // unless the test manually included runId).
    const { harness, agent } = setup();
    agent.chunks = [
      { type: 'text-start', payload: { id: 'm' } },
      { type: 'text-delta', payload: { id: 'm', text: 'no-run-id' } },
      { type: 'tool-call', payload: { toolCallId: 'tc9', toolName: 'lookup', args: { q: 'x' } } },
      { type: 'tool-result', payload: { toolCallId: 'tc9', result: { ok: true } } },
      { type: 'text-end', payload: { id: 'm' } },
    ];
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    const events: HarnessEvent[] = [];
    session.subscribe(e => events.push(e));
    await session.message({ content: 'hi' });

    const textDelta = events.find(e => e.type === 'text_delta') as
      | Extract<HarnessEvent, { type: 'text_delta' }>
      | undefined;
    expect(textDelta?.delta).toBe('no-run-id');
    expect(typeof textDelta?.runId).toBe('string');
    expect((textDelta?.runId ?? '').length).toBeGreaterThan(0);

    expect(events.find(e => e.type === 'tool_start')).toMatchObject({
      type: 'tool_start',
      toolCallId: 'tc9',
      toolName: 'lookup',
      input: { q: 'x' },
    });
    expect(events.find(e => e.type === 'tool_end')).toMatchObject({
      type: 'tool_end',
      toolCallId: 'tc9',
      output: { ok: true },
    });
  });

  it('emits token_usage_changed with the cumulative session total after a turn (§10.2)', async () => {
    const { harness } = setup();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    const events: HarnessEvent[] = [];
    session.subscribe(e => {
      events.push(e);
    });
    await session.message({ content: 'hi' });

    const usageEvents = events.filter(e => e.type === 'token_usage_changed') as Array<
      Extract<HarnessEvent, { type: 'token_usage_changed' }>
    >;
    expect(usageEvents.length).toBeGreaterThan(0);
    // §10.2 StateEvent — `usage` is the running cumulative total, matching the
    // session's own accounting.
    const last = usageEvents[usageEvents.length - 1]!;
    expect(last.usage.totalTokens).toBeGreaterThan(0);
    expect(last.usage.totalTokens).toBe(session.getTokenUsage().totalTokens);
  });

  it('emits tool_start and tool_end around a tool-call/tool-result pair', async () => {
    const { harness, agent } = setup();
    agent.chunks = [
      {
        type: 'tool-call',
        payload: { toolCallId: 'tc1', toolName: 'lookup', args: { q: 'mastra' } },
        runId: 'fake-run',
      },
      {
        type: 'tool-result',
        payload: { toolCallId: 'tc1', result: { hits: 3 } },
        runId: 'fake-run',
      },
    ];
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    const events: HarnessEvent[] = [];
    session.subscribe(e => {
      events.push(e);
    });
    await session.message({ content: 'hi' });

    const start = events.find(e => e.type === 'tool_start');
    const end = events.find(e => e.type === 'tool_end');
    // §10.2 ToolEvent: tool_start carries runId + input; tool_end carries
    // runId + toolName + output.
    expect(start).toMatchObject({
      type: 'tool_start',
      runId: 'fake-run',
      toolCallId: 'tc1',
      toolName: 'lookup',
      input: { q: 'mastra' },
    });
    expect(end).toMatchObject({
      type: 'tool_end',
      runId: 'fake-run',
      toolCallId: 'tc1',
      toolName: 'lookup',
      isError: false,
      output: { hits: 3 },
    });
  });

  it('persists tool error events without poisoning event replay', async () => {
    const { harness, agent, storage } = setup();
    agent.chunks = [
      {
        type: 'tool-call',
        payload: { toolCallId: 'tc1', toolName: 'lookup', args: { q: 'mastra' } },
        runId: 'fake-run',
      },
      {
        type: 'tool-error',
        payload: { toolCallId: 'tc1', error: new Error('lookup failed') },
        runId: 'fake-run',
      },
    ];
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    await session.message({ content: 'hi' });

    await expect(session._flushEventPersistence()).resolves.toBeUndefined();
    const state = await storage.getSessionEventReplayState({
      sessionId: session.id,
      resourceId: session.resourceId,
      threadId: session.threadId,
    });
    expect(state).not.toBeNull();
    const rows = await storage.listSessionEvents({
      sessionId: session.id,
      resourceId: session.resourceId,
      threadId: session.threadId,
      epoch: state!.epoch,
      afterSequence: 0,
      limit: 100,
    });
    expect(rows.map(row => row.event).find((event: any) => event.type === 'tool_end')).toMatchObject({
      type: 'tool_end',
      toolCallId: 'tc1',
      toolName: 'lookup',
      isError: true,
      output: { name: 'Error', code: 'Error', message: 'lookup failed' },
    });
  });

  it('persists repeated object references and undefined event fields without poisoning replay', async () => {
    const { harness, agent, storage } = setup();
    class Box {
      constructor(readonly value: string) {}
    }
    const shared = { ok: true };
    agent.chunks = [
      {
        type: 'tool-call',
        payload: { toolCallId: 'tc1', toolName: 'lookup', args: { q: 'mastra' } },
        runId: 'fake-run',
      },
      {
        type: 'tool-result',
        payload: {
          toolCallId: 'tc1',
          result: {
            first: shared,
            second: shared,
            at: new Date('2026-05-19T00:00:00.000Z'),
            boxed: new Box('ok'),
            omitted: undefined,
          },
        },
        runId: 'fake-run',
      },
    ];
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    await session.message({ content: 'hi' });

    await expect(session._flushEventPersistence()).resolves.toBeUndefined();
    const state = await storage.getSessionEventReplayState({
      sessionId: session.id,
      resourceId: session.resourceId,
      threadId: session.threadId,
    });
    expect(state).not.toBeNull();
    const rows = await storage.listSessionEvents({
      sessionId: session.id,
      resourceId: session.resourceId,
      threadId: session.threadId,
      epoch: state!.epoch,
      afterSequence: 0,
      limit: 100,
    });
    const toolEnd = rows.map(row => row.event).find((event: any) => event.type === 'tool_end') as any;
    expect(toolEnd).toMatchObject({
      type: 'tool_end',
      toolCallId: 'tc1',
      isError: false,
      output: {
        first: { ok: true },
        second: { ok: true },
        at: '2026-05-19T00:00:00.000Z',
        boxed: { value: 'ok' },
      },
    });
    expect(toolEnd.output).not.toHaveProperty('omitted');
  });

  it('does not synthesize a built-in event from a data-task-updated chunk (§10.2/§10.3)', async () => {
    // §10.2 has no `task_updated` built-in. Tools surface task lists via §10.3
    // custom (dotted) events; the harness no longer bridges `data-*` writer
    // chunks into harness-owned built-in events.
    const { harness, agent } = setup();
    const tasks = [
      { content: 'A', activeForm: 'Doing A', status: 'pending' as const },
      { content: 'B', activeForm: 'Doing B', status: 'completed' as const },
    ];
    agent.chunks = [{ type: 'data-task-updated', data: { tasks }, runId: 'fake-run' }];
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    const types: string[] = [];
    session.subscribe(e => types.push((e as { type: string }).type));
    await session.message({ content: 'do it' });

    expect(types).not.toContain('task_updated');
    expect(types).not.toContain('tool_update');
    expect(types).not.toContain('shell_output');
  });
});

describe('Session events — suspension round-trip', () => {
  it('emits tool_approval_required on capture; resume clears the pending (no suspension_resolved event, §10.2)', async () => {
    const { harness, agent } = setup();
    agent.fullOutput = {
      ...agent.fullOutput,
      finishReason: 'suspended',
      suspendPayload: {
        toolCallId: 'tc1',
        toolName: 'do_thing',
        args: { x: 1 },
      },
    };
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    const events: HarnessEvent[] = [];
    session.subscribe(e => {
      events.push(e);
    });

    await session.message({ content: 'do it' });
    // §10.2: a tool-approval suspend surfaces as `tool_approval_required`.
    expect(events.some(e => e.type === 'tool_approval_required')).toBe(true);

    // Flip the agent so the resumed run completes.
    agent.fullOutput = { ...agent.fullOutput, finishReason: 'stop', suspendPayload: undefined };

    await session.respondToToolApproval({ approved: true });
    // §10.2 defines NO suspension_resolved event — resolution is observed via
    // the cleared pending state (+ the resumed run's agent_end), not an event.
    expect(events.some(e => (e as { type: string }).type === 'suspension_resolved')).toBe(false);
    expect(session.getRecord().pendingResume).toBeUndefined();
  });
});

describe('Harness.subscribe()', () => {
  it('delivers session_created when a session is opened', async () => {
    const { harness } = setup();
    const events: HarnessEvent[] = [];
    harness.subscribe(e => {
      events.push(e);
    });

    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    const created = events.find(e => e.type === 'session_created');
    expect(created).toMatchObject({
      type: 'session_created',
      sessionId: session.id,
      resourceId: 'u1',
      modeId: 'default',
    });
  });

  it('forwards session-level events to harness subscribers without re-stamping', async () => {
    const { harness } = setup();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    const sessionEvents: HarnessEvent[] = [];
    const harnessEvents: HarnessEvent[] = [];
    session.subscribe(e => {
      sessionEvents.push(e);
    });
    harness.subscribe(e => {
      harnessEvents.push(e);
    });

    await session.message({ content: 'hi' });

    // Every session-level event reaches the harness subscriber too.
    const sessionStart = sessionEvents.find(e => e.type === 'agent_start');
    const harnessStart = harnessEvents.find(e => e.type === 'agent_start');
    expect(sessionStart).toBeDefined();
    expect(harnessStart).toBeDefined();
    // Forwarded events keep their original id (no double-stamping).
    expect(harnessStart!.id).toBe(sessionStart!.id);
    expect(harnessStart!.sessionId).toBe(session.id);
  });

  it('emits session_closed when a session is closed', async () => {
    const { harness } = setup();
    const events: HarnessEvent[] = [];
    harness.subscribe(e => {
      events.push(e);
    });

    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    await session.close();

    const closed = events.find(e => e.type === 'session_closed');
    expect(closed).toMatchObject({
      type: 'session_closed',
      sessionId: session.id,
      reason: 'requested',
    });
  });

  it('emits session_evicted on shutdown', async () => {
    const { harness } = setup();
    const events: HarnessEvent[] = [];
    harness.subscribe(e => {
      events.push(e);
    });

    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    await harness.shutdown();

    const evicted = events.find(e => e.type === 'session_evicted');
    expect(evicted).toMatchObject({
      type: 'session_evicted',
      sessionId: session.id,
      reason: 'shutdown',
    });
  });
});

describe('Session events — tool payload JSON-safety at emit (live === replay)', () => {
  // Capture the LIVE-subscriber event and the durable/replayed row for the same
  // tool call, then assert they are byte-for-byte equal. The previous
  // regression delivered the RAW runtime object (Date instance, class instance,
  // present `undefined` prop, shared aliased ref) to live subscribers while only
  // the persist path normalized through JSON — so live !== replay.
  async function captureLiveAndReplay(opts: {
    storage: any;
    session: any;
    sendMessage: () => Promise<void>;
    liveEvents: HarnessEvent[];
  }): Promise<{ live: any; replay: any }> {
    await opts.sendMessage();
    await expect(opts.session._flushEventPersistence()).resolves.toBeUndefined();
    const state = await opts.storage.getSessionEventReplayState({
      sessionId: opts.session.id,
      resourceId: opts.session.resourceId,
      threadId: opts.session.threadId,
    });
    expect(state).not.toBeNull();
    const rows = await opts.storage.listSessionEvents({
      sessionId: opts.session.id,
      resourceId: opts.session.resourceId,
      threadId: opts.session.threadId,
      epoch: state!.epoch,
      afterSequence: 0,
      limit: 100,
    });
    const live = opts.liveEvents.find(e => (e as any).type === 'tool_end') as any;
    const replay = rows.map((row: any) => row.event).find((event: any) => event.type === 'tool_end') as any;
    expect(live).toBeDefined();
    expect(replay).toBeDefined();
    return { live, replay };
  }

  it('projects Date/Map/Set/class-instance/undefined/aliased output identically for live and replay', async () => {
    const { harness, agent, storage } = setup();
    class Box {
      constructor(readonly value: string) {}
    }
    const shared = { ok: true };
    agent.chunks = [
      {
        type: 'tool-call',
        payload: { toolCallId: 'tc1', toolName: 'lookup', args: { q: 'mastra' } },
        runId: 'fake-run',
      },
      {
        type: 'tool-result',
        payload: {
          toolCallId: 'tc1',
          result: {
            first: shared,
            second: shared,
            at: new Date('2026-05-19T00:00:00.000Z'),
            boxed: new Box('ok'),
            map: new Map([['a', 1]]),
            set: new Set([1, 2]),
            omitted: undefined,
          },
        },
        runId: 'fake-run',
      },
    ];
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const liveEvents: HarnessEvent[] = [];
    session.subscribe(e => {
      liveEvents.push(e);
    });

    const { live, replay } = await captureLiveAndReplay({
      storage,
      session,
      liveEvents,
      sendMessage: () => session.message({ content: 'hi' }),
    });

    // Stable JSON-safe shape: Date->ISO string, Map/Set->{}, class->plain
    // object, `undefined` prop dropped, aliased ref split into copies.
    expect(live.output).toEqual({
      first: { ok: true },
      second: { ok: true },
      at: '2026-05-19T00:00:00.000Z',
      boxed: { value: 'ok' },
      map: {},
      set: {},
    });
    expect(live.output).not.toHaveProperty('omitted');
    // The live value is a normalized clone, NOT the raw runtime object.
    expect(live.output.at).not.toBeInstanceOf(Date);
    expect(live.output.boxed).not.toBeInstanceOf(Box);
    expect(live.output.first).not.toBe(live.output.second);
    // Live === replay by construction.
    expect(live.output).toEqual(replay.output);
    expect(live.isError).toBe(false);
  });

  it('preserves a tool-error name/code/message identically for live and replay (not flattened to harness.internal)', async () => {
    const { harness, agent, storage } = setup();
    class LookupError extends Error {
      readonly code = 'tool.lookup_failed';
      constructor(message: string) {
        super(message);
        this.name = 'LookupError';
      }
    }
    agent.chunks = [
      { type: 'tool-call', payload: { toolCallId: 'tc1', toolName: 'lookup', args: { q: 'x' } }, runId: 'fake-run' },
      { type: 'tool-error', payload: { toolCallId: 'tc1', error: new LookupError('boom') }, runId: 'fake-run' },
    ];
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const liveEvents: HarnessEvent[] = [];
    session.subscribe(e => {
      liveEvents.push(e);
    });

    const { live, replay } = await captureLiveAndReplay({
      storage,
      session,
      liveEvents,
      sendMessage: () => session.message({ content: 'hi' }),
    });

    // §13.3f.1: the tool's OWN error keeps faithful name/code/message; it is NOT
    // reduced to the reserved harness.internal public-error shape.
    expect(live.output).toEqual({ name: 'LookupError', code: 'tool.lookup_failed', message: 'boom' });
    // Live never receives the raw Error instance (no stack/cause/prototype leak).
    expect(live.output).not.toBeInstanceOf(Error);
    expect(live.output).toEqual(replay.output);
    expect(live.isError).toBe(true);
  });

  it('replaces a bigint output with a stable sentinel identically for live and replay (no turn crash, no persistence split)', async () => {
    const { harness, agent, storage } = setup();
    agent.chunks = [
      { type: 'tool-call', payload: { toolCallId: 'tc1', toolName: 'calc', args: { n: 1 } }, runId: 'fake-run' },
      { type: 'tool-result', payload: { toolCallId: 'tc1', result: { big: 9007199254740993n } }, runId: 'fake-run' },
    ];
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const liveEvents: HarnessEvent[] = [];
    session.subscribe(e => {
      liveEvents.push(e);
    });

    const { live, replay } = await captureLiveAndReplay({
      storage,
      session,
      liveEvents,
      sendMessage: () => session.message({ content: 'hi' }),
    });

    expect(live.output).toEqual({ __mastraHarness: 'unserializable-tool-payload' });
    expect(live.output).toEqual(replay.output);
  });

  it('replaces an output exceeding files.maxEventPayloadBytes with a stable oversized sentinel identically for live and replay', async () => {
    // Tiny cap so a modest string blows the budget. The serialized output is
    // well over 64 bytes, so it is replaced by the oversized-payload sentinel.
    const { harness, agent, storage } = setup({ maxEventPayloadBytes: 64 });
    const big = 'x'.repeat(2000);
    agent.chunks = [
      { type: 'tool-call', payload: { toolCallId: 'tc1', toolName: 'fetch', args: { q: 'big' } }, runId: 'fake-run' },
      { type: 'tool-result', payload: { toolCallId: 'tc1', result: { blob: big } }, runId: 'fake-run' },
    ];
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const liveEvents: HarnessEvent[] = [];
    session.subscribe(e => {
      liveEvents.push(e);
    });

    const { live, replay } = await captureLiveAndReplay({
      storage,
      session,
      liveEvents,
      sendMessage: () => session.message({ content: 'hi' }),
    });

    // Bounded, detectable marker — NOT the raw payload, NOT the unserializable
    // sentinel — identical on the live wire and the durable replay row.
    expect(live.output).toEqual({ __mastraHarness: 'oversized-tool-payload' });
    expect(live.output).toEqual(replay.output);
  });

  it('passes a payload under files.maxEventPayloadBytes through verbatim identically for live and replay', async () => {
    const { harness, agent, storage } = setup({ maxEventPayloadBytes: 64 });
    // Serialized as {"ok":"hi"} — 11 bytes, well under the 64-byte cap.
    agent.chunks = [
      { type: 'tool-call', payload: { toolCallId: 'tc1', toolName: 'fetch', args: { q: 'small' } }, runId: 'fake-run' },
      { type: 'tool-result', payload: { toolCallId: 'tc1', result: { ok: 'hi' } }, runId: 'fake-run' },
    ];
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const liveEvents: HarnessEvent[] = [];
    session.subscribe(e => {
      liveEvents.push(e);
    });

    const { live, replay } = await captureLiveAndReplay({
      storage,
      session,
      liveEvents,
      sendMessage: () => session.message({ content: 'hi' }),
    });

    expect(live.output).toEqual({ ok: 'hi' });
    expect(live.output).toEqual(replay.output);
  });

  it('applies NO cap when files.maxEventPayloadBytes is unset (opt-in; byte-identical to pre-feature)', async () => {
    const { harness, agent, storage } = setup();
    const result = { items: Array.from({ length: 50 }, (_, i) => ({ id: i, label: `row-${i}` })) };
    agent.chunks = [
      { type: 'tool-call', payload: { toolCallId: 'tc1', toolName: 'list', args: {} }, runId: 'fake-run' },
      { type: 'tool-result', payload: { toolCallId: 'tc1', result }, runId: 'fake-run' },
    ];
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const liveEvents: HarnessEvent[] = [];
    session.subscribe(e => {
      liveEvents.push(e);
    });

    const { live, replay } = await captureLiveAndReplay({
      storage,
      session,
      liveEvents,
      sendMessage: () => session.message({ content: 'hi' }),
    });

    // No cap configured => no sentinel substitution at any size; verbatim.
    expect(live.output).toEqual(result);
    expect(live.output).toEqual(replay.output);
  });

  it('replaces a circular output with a stable sentinel identically for live and replay', async () => {
    const { harness, agent, storage } = setup();
    const cyclic: any = { name: 'root' };
    cyclic.self = cyclic;
    agent.chunks = [
      { type: 'tool-call', payload: { toolCallId: 'tc1', toolName: 'walk', args: {} }, runId: 'fake-run' },
      { type: 'tool-result', payload: { toolCallId: 'tc1', result: cyclic }, runId: 'fake-run' },
    ];
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const liveEvents: HarnessEvent[] = [];
    session.subscribe(e => {
      liveEvents.push(e);
    });

    const { live, replay } = await captureLiveAndReplay({
      storage,
      session,
      liveEvents,
      sendMessage: () => session.message({ content: 'hi' }),
    });

    expect(live.output).toEqual({ __mastraHarness: 'unserializable-tool-payload' });
    expect(live.output).toEqual(replay.output);
  });

  it('drops a function-valued output prop identically for live and replay', async () => {
    const { harness, agent, storage } = setup();
    agent.chunks = [
      { type: 'tool-call', payload: { toolCallId: 'tc1', toolName: 'fn', args: {} }, runId: 'fake-run' },
      {
        type: 'tool-result',
        payload: { toolCallId: 'tc1', result: { kept: 'yes', cb: () => 'drop me' } },
        runId: 'fake-run',
      },
    ];
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const liveEvents: HarnessEvent[] = [];
    session.subscribe(e => {
      liveEvents.push(e);
    });

    const { live, replay } = await captureLiveAndReplay({
      storage,
      session,
      liveEvents,
      sendMessage: () => session.message({ content: 'hi' }),
    });

    expect(live.output).toEqual({ kept: 'yes' });
    expect(live.output).not.toHaveProperty('cb');
    expect(live.output).toEqual(replay.output);
  });

  it('projects a top-level-undefined output to null (a valid no-result, NOT the unserializable sentinel) identically for live and replay', async () => {
    const { harness, agent, storage } = setup();
    // A void / side-effect tool returns top-level `undefined`. That is a
    // legitimate "no result", not a serialization failure, so it must NOT
    // collapse to the unserializable sentinel (which a UI may read as an error
    // state). It projects to `null`, identically on live and replay.
    agent.chunks = [
      { type: 'tool-call', payload: { toolCallId: 'tc1', toolName: 'sideEffect', args: {} }, runId: 'fake-run' },
      { type: 'tool-result', payload: { toolCallId: 'tc1', result: undefined }, runId: 'fake-run' },
    ];
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const liveEvents: HarnessEvent[] = [];
    session.subscribe(e => {
      liveEvents.push(e);
    });

    const { live, replay } = await captureLiveAndReplay({
      storage,
      session,
      liveEvents,
      sendMessage: () => session.message({ content: 'hi' }),
    });

    expect(live.output).toBeNull();
    // Distinct from the bigint/cycle sentinel — a void result is not a failure.
    expect(live.output).not.toEqual({ __mastraHarness: 'unserializable-tool-payload' });
    expect(live.output).toEqual(replay.output);
  });

  it('projects tool_start.input identically for live and replay', async () => {
    const { harness, agent, storage } = setup();
    agent.chunks = [
      {
        type: 'tool-call',
        payload: { toolCallId: 'tc1', toolName: 'lookup', args: { when: new Date('2026-05-19T00:00:00.000Z') } },
        runId: 'fake-run',
      },
      { type: 'tool-result', payload: { toolCallId: 'tc1', result: { ok: true } }, runId: 'fake-run' },
    ];
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const liveEvents: HarnessEvent[] = [];
    session.subscribe(e => {
      liveEvents.push(e);
    });

    await session.message({ content: 'hi' });
    await expect(session._flushEventPersistence()).resolves.toBeUndefined();
    const state = await storage.getSessionEventReplayState({
      sessionId: session.id,
      resourceId: session.resourceId,
      threadId: session.threadId,
    });
    const rows = await storage.listSessionEvents({
      sessionId: session.id,
      resourceId: session.resourceId,
      threadId: session.threadId,
      epoch: state!.epoch,
      afterSequence: 0,
      limit: 100,
    });
    const liveStart = liveEvents.find(e => (e as any).type === 'tool_start') as any;
    const replayStart = rows.map((row: any) => row.event).find((event: any) => event.type === 'tool_start') as any;
    expect(liveStart.input).toEqual({ when: '2026-05-19T00:00:00.000Z' });
    expect(liveStart.input.when).not.toBeInstanceOf(Date);
    expect(liveStart.input).toEqual(replayStart.input);
  });

  it('projects tool_approval_required.input identically for live and replay (raw suspend args carry a Date)', async () => {
    const { harness, agent, storage } = setup();
    // The suspended output hands the harness RAW runtime args. Without projecting
    // at the emit site, the first live subscriber would see the live `Date`
    // while the durable/replayed row (snapshot through JSON) would hold the ISO
    // string — the same live!==replay divergence the tool_start/tool_end fix
    // closed. `input` is part of the §10.2 public contract (JSON-safe).
    agent.fullOutput = {
      ...agent.fullOutput,
      finishReason: 'suspended',
      suspendPayload: {
        toolCallId: 'tc1',
        toolName: 'do_thing',
        args: { when: new Date('2026-05-19T00:00:00.000Z'), nested: { ok: true } },
      },
    };
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const liveEvents: HarnessEvent[] = [];
    session.subscribe(e => {
      liveEvents.push(e);
    });

    await session.message({ content: 'do it' });
    await expect(session._flushEventPersistence()).resolves.toBeUndefined();
    const state = await storage.getSessionEventReplayState({
      sessionId: session.id,
      resourceId: session.resourceId,
      threadId: session.threadId,
    });
    const rows = await storage.listSessionEvents({
      sessionId: session.id,
      resourceId: session.resourceId,
      threadId: session.threadId,
      epoch: state!.epoch,
      afterSequence: 0,
      limit: 100,
    });
    const live = liveEvents.find(e => (e as any).type === 'tool_approval_required') as any;
    const replay = rows
      .map((row: any) => row.event)
      .find((event: any) => event.type === 'tool_approval_required') as any;
    expect(live).toBeDefined();
    expect(replay).toBeDefined();
    // Date -> ISO string on BOTH paths; the live subscriber never sees a raw Date.
    expect(live.input).toEqual({ when: '2026-05-19T00:00:00.000Z', nested: { ok: true } });
    expect(live.input.when).not.toBeInstanceOf(Date);
    expect(live.input).toEqual(replay.input);
  });
});

describe('Session synthetic tool_end on unsettled tools (§10.2)', () => {
  it('emits a synthetic aborted tool_end for a tool that never produced a result before the turn ended', async () => {
    const { harness, agent } = setup();
    // A tool-call chunk with NO matching tool-result → the tool is still active
    // when the turn ends. The harness must synthesize a terminal tool_end so the
    // consumer gets a tool_start → tool_end pair for every tool.
    agent.chunks = [
      { type: 'tool-call', payload: { toolCallId: 'tc-dangle', toolName: 'lookup', args: {} }, runId: 'fake-run' },
    ];
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));
      await session.message({ content: 'hi' });

      const starts = events.filter(e => e.type === 'tool_start') as any[];
      const ends = events.filter(e => e.type === 'tool_end') as any[];
      expect(starts.map(e => e.toolCallId)).toContain('tc-dangle');
      const end = ends.find(e => e.toolCallId === 'tc-dangle');
      expect(end).toBeDefined();
      expect(end.isError).toBe(true);
      expect(end.toolName).toBe('lookup');
      // Attributed to the session's actual run id (not necessarily the chunk's
      // hardcoded one); just assert it is a real run id.
      expect(typeof end.runId).toBe('string');
      expect(end.runId.length).toBeGreaterThan(0);
      expect((end.output as any).aborted).toBe(true);
      // Exactly one terminal (no duplicate from a later real result).
      expect(ends.filter(e => e.toolCallId === 'tc-dangle')).toHaveLength(1);
    } finally {
      await harness.shutdown();
    }
  });

  it('suppresses (one-shot) a late real tool_end that arrives after a synthetic aborted tool_end', async () => {
    const { harness } = setup();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));
      // Simulate: a tool was active on run-x when the turn ended → synthetic abort.
      (session as any)._currentRunId = 'run-x';
      (session as any)._activeTools.set('tc1', {
        toolCallId: 'tc1',
        toolName: 'lookup',
        args: {},
        startedAt: Date.now(),
      });
      (session as any)._emitAbortedToolEnds();

      // A late REAL tool-result for the same (runId, toolCallId) arrives on the drain.
      (session as any)._emitForChunk({
        type: 'tool-result',
        payload: { toolCallId: 'tc1', toolName: 'lookup', result: { ok: true } },
        runId: 'run-x',
      });
      let ends = events.filter(e => e.type === 'tool_end' && (e as any).toolCallId === 'tc1') as any[];
      expect(ends).toHaveLength(1); // late real terminal suppressed
      expect(ends[0].isError).toBe(true); // the synthetic aborted one
      expect((ends[0].output as any).aborted).toBe(true);

      // One-shot: a SECOND late result for the same key is no longer suppressed.
      (session as any)._emitForChunk({
        type: 'tool-result',
        payload: { toolCallId: 'tc1', toolName: 'lookup', result: {} },
        runId: 'run-x',
      });
      ends = events.filter(e => e.type === 'tool_end' && (e as any).toolCallId === 'tc1') as any[];
      expect(ends).toHaveLength(2);
    } finally {
      await harness.shutdown();
    }
  });

  it('does NOT synthesize a tool_end when the tool settled normally (no duplicate)', async () => {
    const { harness, agent } = setup();
    agent.chunks = [
      { type: 'tool-call', payload: { toolCallId: 'tc-ok', toolName: 'lookup', args: {} }, runId: 'fake-run' },
      { type: 'tool-result', payload: { toolCallId: 'tc-ok', result: { ok: true } }, runId: 'fake-run' },
    ];
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));
      await session.message({ content: 'hi' });
      const ends = (events.filter(e => e.type === 'tool_end') as any[]).filter(e => e.toolCallId === 'tc-ok');
      expect(ends).toHaveLength(1);
      expect(ends[0].isError).toBe(false); // the real result, not a synthetic abort
    } finally {
      await harness.shutdown();
    }
  });
});

describe('Session event-persistence failure surfacing (§10.2)', () => {
  it('emits a live storage_error (once) when appendSessionEvent fails, instead of silently degrading', async () => {
    const { harness, storage } = setup();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));
      // Fail the first durable append → trips the latch.
      vi.spyOn(storage, 'appendSessionEvent').mockRejectedValueOnce(new Error('disk full'));

      await session.message({ content: 'hi' });
      // Drain the persistence tail so the latch-trip catch (which emits) has run.
      await session._flushEventPersistence().catch(() => {});

      const storageErrors = events.filter(e => e.type === 'storage_error') as any[];
      expect(storageErrors).toHaveLength(1);
      expect(storageErrors[0].operation).toBe('session_event_append');
      expect(storageErrors[0].error.code).toBe('harness.storage');
      expect(storageErrors[0].resourceId).toBe('u1');

      // A second turn does not emit another storage_error (latch already surfaced).
      await session.message({ content: 'again' }).catch(() => {});
      await session._flushEventPersistence().catch(() => {});
      expect(events.filter(e => e.type === 'storage_error')).toHaveLength(1);
    } finally {
      // shutdown surfaces the latched event-persistence error we injected.
      await harness.shutdown().catch(() => {});
    }
  });
});
