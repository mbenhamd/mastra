/**
 * Tests for Session.getDisplayState() — spec §4.2 / §4.4 `SessionDisplayState`.
 *
 * Verifies the new v1 shape:
 *   - Identity fields are populated from the live record.
 *   - Run fields (`isRunning`, `currentRunId`) flip during a turn.
 *   - Activity maps (`activeTools`, `toolInputBuffers`, `activeSubagents`)
 *     are fresh projections per call.
 *   - Token usage accumulates across turns.
 *   - Pending interrupts surface as full `pendingResume` (not booleans).
 *   - Queue depth + `currentQueuedItemId` track the running queued item.
 */

import { describe, expect, it } from 'vitest';
import { MockAgent } from './__test-utils__/mock-agent';
import { setupHarness } from './__test-utils__/setup';
import { toHarnessDisplayStateSnapshotV1 } from './display-state';

async function waitFor(condition: () => boolean | Promise<boolean>, label: string): Promise<void> {
  for (let i = 0; i < 80; i++) {
    if (await condition()) return;
    await new Promise(resolve => setTimeout(resolve, 10));
  }
  throw new Error(`Timed out waiting for ${label}`);
}

describe('Session.getDisplayState — shape', () => {
  it('reports the documented identity fields', async () => {
    const { harness } = setupHarness();
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    const ds = session.getDisplayState();
    expect(ds.sessionId).toBe(session.id);
    expect(ds.threadId).toBe(session.threadId);
    expect(ds.resourceId).toBe('u');
    expect(ds.lifecycleState).toBe('live');
    expect(ds.modeId).toBeTypeOf('string');
    expect(ds.modelId).toBeTypeOf('string');
    expect(ds.createdAt).toBe(session.createdAt);
    expect(ds.lastActivityAt).toBeTypeOf('number');
  });

  it('idle state has no run fields, empty activity maps, zero usage', async () => {
    const { harness } = setupHarness();
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    const ds = session.getDisplayState();
    expect(ds.isRunning).toBe(false);
    expect(ds.currentRunId).toBeUndefined();
    expect(ds.currentMessageId).toBeUndefined();
    expect(ds.currentTraceId).toBeUndefined();
    expect(ds.activeTools).toEqual({});
    expect(ds.toolInputBuffers).toEqual({});
    expect(ds.activeSubagents).toEqual({});
    expect(ds.assistantDrafts).toEqual({});
    expect(ds.tokenUsage).toEqual({ promptTokens: 0, completionTokens: 0, totalTokens: 0 });
    expect(ds.pending).toBeNull();
    expect(ds.queueDepth).toBe(0);
    expect(ds.currentQueuedItemId).toBeUndefined();
    expect(ds.goal).toBeUndefined();
    expect(ds.currentRun).toBeUndefined();
  });

  it('isRunning flips true while a turn is in flight, and currentRunId is captured', async () => {
    const { harness, agent } = setupHarness();
    let release!: () => void;
    const hold = new Promise<void>(r => {
      release = r;
    });
    // Stage a runId-bearing chunk BEFORE the hold so the run reports its runId
    // mid-flight (the mock holds after streaming `chunks`, before the terminal).
    agent.enqueueRun({
      runId: 'run-display',
      finishReason: 'stop',
      text: 'ok',
      chunks: [{ type: 'text-delta', payload: { id: 'm', text: 'hi' }, runId: 'run-display' }],
      holdUntil: hold,
    });

    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    const inFlight = session.message({ content: 'hi' });

    // Drive the event loop until the run's id has been captured mid-flight.
    while (session.getDisplayState().currentRunId === undefined) await Promise.resolve();

    const mid = session.getDisplayState();
    expect(mid.isRunning).toBe(true);
    // §5.1b SessionRunProjection populated while the run is in flight.
    expect(mid.currentRun).toBeDefined();
    expect(mid.currentRun).toMatchObject({
      runId: mid.currentRunId, // mirrors the captured run id
      status: 'running',
      // PF-817: a signal-backed live run links to its originating signalId.
      operation: { kind: 'signal', signalId: expect.any(String) },
      modeId: mid.modeId,
      modelId: mid.modelId,
    });
    expect(typeof mid.currentRun!.agentId).toBe('string');
    expect(typeof mid.currentRun!.startedAt).toBe('number');
    // currentRun survives the JSON-safe wire snapshot (encoder must not drop it).
    expect(toHarnessDisplayStateSnapshotV1(mid).currentRun).toEqual(mid.currentRun);

    release();
    await inFlight;

    const after = session.getDisplayState();
    expect(after.isRunning).toBe(false);
    expect(after.currentRunId).toBeUndefined();
    expect(after.currentRun).toBeUndefined();
    // Token usage accumulated from the run's totalUsage.
    expect(after.tokenUsage.totalTokens).toBeGreaterThanOrEqual(2);
  });

  it('projects coalesced assistant drafts while a response is streaming', async () => {
    const { harness, agent } = setupHarness();
    let release!: () => void;
    const hold = new Promise<void>(r => {
      release = r;
    });
    agent.enqueueRun({
      runId: 'run-draft-live',
      finishReason: 'stop',
      text: 'hello world',
      chunks: [
        { type: 'text-start', payload: { id: 'msg-draft' }, runId: 'run-draft-live' },
        { type: 'text-delta', payload: { id: 'msg-draft', text: 'hello ' }, runId: 'run-draft-live' },
        { type: 'text-delta', payload: { id: 'msg-draft', text: 'world' }, runId: 'run-draft-live' },
      ],
      holdUntil: hold,
    });

    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    const inFlight = session.message({ content: 'hi' });
    await waitFor(
      () => session.getDisplayState().assistantDrafts?.['run-draft-live']?.text === 'hello world',
      'live assistant draft',
    );

    const draft = session.getDisplayState().assistantDrafts!['run-draft-live']!;
    expect(draft).toMatchObject({
      runId: 'run-draft-live',
      sessionId: session.id,
      resourceId: 'u',
      threadId: session.threadId,
      messageId: 'msg-draft',
      text: 'hello world',
      status: 'streaming',
    });
    expect(toHarnessDisplayStateSnapshotV1(session.getDisplayState()).assistantDrafts['run-draft-live']).toEqual(draft);

    release();
    await inFlight;
  });

  it('persists assistant drafts for reload recovery and terminalizes them on completion', async () => {
    const { harness, agent, storage } = setupHarness();
    agent.enqueueRun({
      runId: 'run-draft-durable',
      finishReason: 'stop',
      text: 'durable answer',
      chunks: [
        { type: 'text-start', payload: { id: 'msg-durable' }, runId: 'run-draft-durable' },
        { type: 'text-delta', payload: { id: 'msg-durable', text: 'durable ' }, runId: 'run-draft-durable' },
        { type: 'text-delta', payload: { id: 'msg-durable', text: 'answer' }, runId: 'run-draft-durable' },
      ],
    });

    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await session.message({ content: 'hi' });

    await waitFor(async () => {
      const stored = await storage.loadSession({ sessionId: session.id });
      return stored?.assistantDrafts?.['run-draft-durable']?.status === 'completed';
    }, 'durable assistant draft completion');

    const stored = await storage.loadSession({ sessionId: session.id });
    expect(stored?.assistantDrafts?.['run-draft-durable']).toMatchObject({
      runId: 'run-draft-durable',
      sessionId: session.id,
      resourceId: 'u',
      threadId: session.threadId,
      messageId: 'msg-durable',
      text: 'durable answer',
      status: 'completed',
      finishReason: 'complete',
    });

    const reloaded = await harness.session({ sessionId: session.id, resourceId: 'u' });
    expect(reloaded.getDisplayState().assistantDrafts?.['run-draft-durable']?.text).toBe('durable answer');
    expect(reloaded.getDisplayState().assistantDrafts?.['run-draft-durable']?.status).toBe('completed');
  });

  it('persists assistant drafts even when transient streaming deltas are not persisted', async () => {
    const { harness, agent, storage } = setupHarness({ sessions: { persistTransientStreamingEvents: false } });
    agent.enqueueRun({
      runId: 'run-draft-no-deltas',
      finishReason: 'stop',
      text: 'overlay recovered',
      chunks: [
        { type: 'text-start', payload: { id: 'msg-no-deltas' }, runId: 'run-draft-no-deltas' },
        { type: 'text-delta', payload: { id: 'msg-no-deltas', text: 'overlay ' }, runId: 'run-draft-no-deltas' },
        { type: 'text-delta', payload: { id: 'msg-no-deltas', text: 'recovered' }, runId: 'run-draft-no-deltas' },
      ],
    });

    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await session.message({ content: 'hi' });
    await session._flushEventPersistence();

    const state = await storage.getSessionEventReplayState({
      sessionId: session.id,
      resourceId: 'u',
      threadId: session.threadId,
    });
    const rows = await storage.listSessionEvents({
      sessionId: session.id,
      resourceId: 'u',
      threadId: session.threadId,
      epoch: state!.epoch,
      afterSequence: 0,
      limit: 100,
    });
    expect(rows.some(row => (row.event as { type?: string }).type === 'text_delta')).toBe(false);

    const stored = await storage.loadSession({ sessionId: session.id });
    expect(stored?.assistantDrafts?.['run-draft-no-deltas']).toMatchObject({
      text: 'overlay recovered',
      status: 'completed',
      finishReason: 'complete',
    });
  });

  it('preserves streamed reasoning text separately from assistant text', async () => {
    const { harness, agent, storage } = setupHarness();
    agent.enqueueRun({
      runId: 'run-draft-reasoning',
      finishReason: 'stop',
      text: 'answer',
      chunks: [
        { type: 'reasoning-delta', payload: { id: 'reasoning-1', text: 'thinking ' }, runId: 'run-draft-reasoning' },
        { type: 'reasoning-delta', payload: { id: 'reasoning-1', text: 'through' }, runId: 'run-draft-reasoning' },
        { type: 'text-delta', payload: { id: 'message-1', text: 'answer' }, runId: 'run-draft-reasoning' },
      ],
    });

    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await session.message({ content: 'hi' });
    await session._flushEventPersistence();

    const stored = await storage.loadSession({ sessionId: session.id });
    expect(stored?.assistantDrafts?.['run-draft-reasoning']).toMatchObject({
      text: 'answer',
      reasoningText: 'thinking through',
      messageId: 'message-1',
      status: 'completed',
    });
  });

  it('does not use reasoning stream ids as assistant draft message ids', async () => {
    const { harness, agent, storage } = setupHarness();
    agent.enqueueRun({
      runId: 'run-reasoning-id',
      finishReason: 'stop',
      text: '',
      chunks: [
        { type: 'reasoning-delta', payload: { id: 'reasoning-only-1', text: 'thinking' }, runId: 'run-reasoning-id' },
      ],
    });

    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await session.message({ content: 'hi' });
    await session._flushEventPersistence();

    const stored = await storage.loadSession({ sessionId: session.id });
    expect(stored?.assistantDrafts?.['run-reasoning-id']).toMatchObject({
      text: '',
      reasoningText: 'thinking',
      status: 'completed',
      finishReason: 'complete',
    });
    expect(stored?.assistantDrafts?.['run-reasoning-id']).not.toHaveProperty('messageId');
  });

  it('terminalizes drafts when agent_end is emitted directly', async () => {
    const { harness, storage } = setupHarness();
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    const internals = session as unknown as {
      _recordAssistantDraftDelta: (opts: {
        runId: string;
        kind: 'text' | 'reasoning';
        delta: string;
        messageId?: string;
      }) => void;
      _emitTurnEvent: (event: {
        type: 'agent_end';
        runId: string;
        finishReason: 'error';
        usage: { promptTokens: number; completionTokens: number; totalTokens: number };
      }) => void;
      _flushAssistantDraftsNow: () => Promise<void>;
    };

    internals._recordAssistantDraftDelta({
      runId: 'run-direct-error',
      kind: 'text',
      delta: 'partial',
      messageId: 'message-direct-error',
    });
    internals._emitTurnEvent({
      type: 'agent_end',
      runId: 'run-direct-error',
      finishReason: 'error',
      usage: { promptTokens: 0, completionTokens: 0, totalTokens: 0 },
    });
    await internals._flushAssistantDraftsNow();

    const stored = await storage.loadSession({ sessionId: session.id });
    expect(stored?.assistantDrafts?.['run-direct-error']).toMatchObject({
      text: 'partial',
      status: 'failed',
      finishReason: 'error',
      messageId: 'message-direct-error',
    });
  });

  it('does not terminalize an unrelated stale draft for a different agent_end run', async () => {
    const { harness } = setupHarness();
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    const internals = session as unknown as {
      _assistantDrafts: Map<string, Record<string, unknown>>;
      _emitTurnEvent: (event: {
        type: 'agent_end';
        runId: string;
        finishReason: 'complete';
        usage: { promptTokens: number; completionTokens: number; totalTokens: number };
      }) => void;
    };

    internals._assistantDrafts.set('run-stale', {
      runId: 'run-stale',
      sessionId: session.id,
      resourceId: 'u',
      threadId: session.threadId,
      messageId: 'message-stale',
      text: 'stale partial',
      status: 'streaming',
      startedAt: Date.now(),
      updatedAt: Date.now(),
    });
    internals._emitTurnEvent({
      type: 'agent_end',
      runId: 'run-other',
      finishReason: 'complete',
      usage: { promptTokens: 0, completionTokens: 0, totalTokens: 0 },
    });

    expect(session.getDisplayState().assistantDrafts?.['run-stale']).toMatchObject({
      text: 'stale partial',
      status: 'streaming',
    });
    expect(session.getDisplayState().assistantDrafts?.['run-stale']).not.toHaveProperty('finishReason');
  });

  it('does not reopen terminal drafts when late deltas arrive', async () => {
    const { harness, storage } = setupHarness();
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    const internals = session as unknown as {
      _recordAssistantDraftDelta: (opts: {
        runId: string;
        kind: 'text' | 'reasoning';
        delta: string;
        messageId?: string;
      }) => void;
      _emitTurnEvent: (event: {
        type: 'agent_end';
        runId: string;
        finishReason: 'complete';
        usage: { promptTokens: number; completionTokens: number; totalTokens: number };
      }) => void;
      _flushAssistantDraftsNow: () => Promise<void>;
    };

    internals._recordAssistantDraftDelta({ runId: 'run-late', kind: 'text', delta: 'first' });
    internals._emitTurnEvent({
      type: 'agent_end',
      runId: 'run-late',
      finishReason: 'complete',
      usage: { promptTokens: 0, completionTokens: 0, totalTokens: 0 },
    });
    internals._recordAssistantDraftDelta({ runId: 'run-late', kind: 'text', delta: ' late' });
    await internals._flushAssistantDraftsNow();

    const stored = await storage.loadSession({ sessionId: session.id });
    expect(stored?.assistantDrafts?.['run-late']).toMatchObject({
      text: 'first late',
      status: 'completed',
      finishReason: 'complete',
    });
    expect(stored?.assistantDrafts?.['run-late']?.terminalAt).toBeTypeOf('number');
  });

  it('flushes pending assistant drafts when awaiting the session flush chain', async () => {
    const { harness, storage } = setupHarness();
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    const internals = session as unknown as {
      _recordAssistantDraftDelta: (opts: {
        runId: string;
        kind: 'text' | 'reasoning';
        delta: string;
        messageId?: string;
      }) => void;
      _internalAwaitFlushChain: () => Promise<void>;
    };

    internals._recordAssistantDraftDelta({
      runId: 'run-flush-chain',
      kind: 'text',
      delta: 'flush me',
      messageId: 'message-flush-chain',
    });
    await internals._internalAwaitFlushChain();

    const stored = await storage.loadSession({ sessionId: session.id });
    expect(stored?.assistantDrafts?.['run-flush-chain']).toMatchObject({
      text: 'flush me',
      status: 'streaming',
      messageId: 'message-flush-chain',
    });
  });

  it('terminalizes an aborted in-flight assistant draft as interrupted', async () => {
    const { harness, agent, storage } = setupHarness();
    let release!: () => void;
    const hold = new Promise<void>(resolve => {
      release = resolve;
    });
    agent.enqueueRun({
      runId: 'run-draft-abort',
      finishReason: 'stop',
      text: 'partial',
      chunks: [
        { type: 'text-start', payload: { id: 'msg-abort' }, runId: 'run-draft-abort' },
        { type: 'text-delta', payload: { id: 'msg-abort', text: 'partial' }, runId: 'run-draft-abort' },
      ],
      holdUntil: hold,
    });

    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    const inFlight = session.message({ content: 'hi' });
    await waitFor(
      () => session.getDisplayState().assistantDrafts?.['run-draft-abort']?.text === 'partial',
      'abort draft',
    );

    session.abort({ reason: 'user-stop' });
    await inFlight;
    release();
    await session._flushEventPersistence();

    const stored = await storage.loadSession({ sessionId: session.id });
    expect(stored?.assistantDrafts?.['run-draft-abort']).toMatchObject({
      text: 'partial',
      status: 'interrupted',
      finishReason: 'aborted',
    });
  });

  it('bounds very large assistant drafts and marks truncation', async () => {
    const { harness, agent, storage } = setupHarness();
    const longText = 'x'.repeat(128_010);
    agent.enqueueRun({
      runId: 'run-draft-long',
      finishReason: 'stop',
      text: longText,
      chunks: [{ type: 'text-delta', payload: { id: 'msg-long', text: longText }, runId: 'run-draft-long' }],
    });

    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await session.message({ content: 'hi' });
    await session._flushEventPersistence();

    const stored = await storage.loadSession({ sessionId: session.id });
    const draft = stored?.assistantDrafts?.['run-draft-long'];
    expect(draft?.text).toHaveLength(128_000);
    expect(draft?.text).toBe('x'.repeat(128_000));
    expect(draft?.truncated).toBe(true);
  });

  it('notifies display subscribers when a draft terminalizes without another display event', async () => {
    const { harness, agent } = setupHarness();
    agent.enqueueRun({
      runId: 'run-draft-subscribe',
      finishReason: 'stop',
      text: 'done',
      chunks: [{ type: 'text-delta', payload: { id: 'msg-subscribe', text: 'done' }, runId: 'run-draft-subscribe' }],
    });

    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    const statuses: string[] = [];
    const unsubscribe = session.subscribeDisplayState(state => {
      const status = state.assistantDrafts['run-draft-subscribe']?.status;
      if (status !== undefined) statuses.push(status);
    });
    try {
      await session.message({ content: 'hi' });
      await waitFor(() => statuses.includes('completed'), 'completed draft display refresh');
    } finally {
      unsubscribe();
    }
    expect(statuses).toContain('streaming');
    expect(statuses).toContain('completed');
  });

  it('accumulates token usage across multiple turns', async () => {
    const { harness, agent } = setupHarness();
    agent.enqueueRun({ runId: 'r1', finishReason: 'stop' });
    agent.enqueueRun({ runId: 'r2', finishReason: 'stop' });

    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await session.message({ content: 'one' });
    const after1 = session.getDisplayState().tokenUsage.totalTokens;
    await session.message({ content: 'two' });
    const after2 = session.getDisplayState().tokenUsage.totalTokens;
    expect(after2).toBeGreaterThan(after1);
  });

  it('surfaces full pending payload (kind + payload), not booleans', async () => {
    const { harness, agent } = setupHarness();
    agent.enqueueRun({
      finishReason: 'suspended',
      runId: 'run-pending',
      suspendPayload: {
        toolCallId: 'tc-1',
        toolName: 'ask_user',
        args: { question: 'pick one', options: [{ label: 'a' }, { label: 'b' }] },
      },
    });

    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await session.message({ content: 'ask' });

    const ds = session.getDisplayState();
    expect(ds.pending).not.toBeNull();
    expect(ds.pending?.kind).toBe('question');
    expect(ds.pending?.toolCallId).toBe('tc-1');
    expect(ds.pending?.payload).toEqual({
      question: 'pick one',
      options: [{ label: 'a' }, { label: 'b' }],
    });
    expect((ds.pending as unknown as Record<string, unknown>).runtimeDependencies).toBeUndefined();
    expect(session.getRecord().pendingResume?.runtimeDependencies).toBeDefined();
    // Legacy boolean fields are gone — make sure consumers know to use `pending`.
    expect((ds as unknown as Record<string, unknown>).hasPendingQuestion).toBeUndefined();
  });

  it('returns fresh activity collections on each call (no shared mutation)', async () => {
    const { harness } = setupHarness();
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    const a = session.getDisplayState();
    const b = session.getDisplayState();
    expect(a.activeTools).not.toBe(b.activeTools);
    expect(a.toolInputBuffers).not.toBe(b.toolInputBuffers);
    expect(a.activeSubagents).not.toBe(b.activeSubagents);
    expect(a.tokenUsage).not.toBe(b.tokenUsage);
  });

  it('parentSessionId is omitted for top-level sessions', async () => {
    const { harness } = setupHarness();
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    expect(session.getDisplayState().parentSessionId).toBeUndefined();
  });
});

describe('Session.getDisplayState — currentRun §5.1b fidelity (F11b)', () => {
  it('reports a `waiting` run sourced from pendingResume when a turn is suspended', async () => {
    const { harness, agent } = setupHarness();
    agent.enqueueRun({
      finishReason: 'suspended',
      runId: 'run-waiting',
      suspendPayload: {
        toolCallId: 'tc-q',
        toolName: 'ask_user',
        args: { question: 'pick' },
      },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await session.message({ content: 'ask' });

    // The live run id is cleared on suspend, but the projection now reflects the
    // suspended run as `waiting`, sourced from the persisted pendingResume.
    const ds = session.getDisplayState();
    const pendingRunId = session.getRecord().pendingResume!.runId;
    expect(ds.currentRunId).toBeUndefined();
    expect(ds.currentRun).toBeDefined();
    expect(ds.currentRun).toMatchObject({
      runId: pendingRunId, // the harness-assigned run id of the suspended turn
      status: 'waiting',
      operation: { kind: 'signal' }, // a signal turn, not a queued item
    });
    expect(ds.currentRun!.operation.queuedItemId).toBeUndefined();
    expect(typeof ds.currentRun!.modeId).toBe('string');
    expect(typeof ds.currentRun!.agentId).toBe('string');
    expect(ds.currentRun!.startedAt).toBeTypeOf('number');
    // Survives the JSON-safe wire snapshot.
    expect(toHarnessDisplayStateSnapshotV1(ds).currentRun).toEqual(ds.currentRun);
  });

  it('reports a `resuming` run while a response is mid-resume (resumedAt set, run not yet live)', async () => {
    const { harness, agent } = setupHarness();
    agent.enqueueRun({
      finishReason: 'suspended',
      runId: 'run-resume',
      suspendPayload: { toolCallId: 'tc-q', toolName: 'ask_user', args: { question: 'pick' } },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await session.message({ content: 'ask' });

    // Stage the resume run WITHOUT a runId-bearing chunk before the hold, so the
    // resume writes pendingResume.resumedAt (its idempotency marker) but the agent
    // has not emitted agent_start yet — the exact `resuming` window.
    let release!: () => void;
    const hold = new Promise<void>(r => {
      release = r;
    });
    agent.enqueueRun({ runId: 'run-resume', finishReason: 'stop', text: 'done', holdUntil: hold });
    const resuming = session.respondToQuestion({ answer: 'a' });
    try {
      while (session.getRecord().pendingResume?.resumedAt === undefined) await Promise.resolve();
      const ds = session.getDisplayState();
      const pendingRunId = session.getRecord().pendingResume!.runId;
      expect(ds.currentRunId).toBeUndefined(); // agent_start not emitted yet
      expect(ds.currentRun).toMatchObject({ runId: pendingRunId, status: 'resuming' });
    } finally {
      release();
      await resuming;
    }
  });

  it('reflects the run EFFECTIVE mode/model (per-turn override), not the session default', async () => {
    const { harness, agents } = setupHarness({
      agents: { default: new MockAgent({ id: 'default' }), fast: new MockAgent({ id: 'fast-agent' }) },
      modes: [
        { id: 'default', agentId: 'default' },
        { id: 'fast', agentId: 'fast' },
      ],
      defaultModeId: 'default',
    });
    let release!: () => void;
    const hold = new Promise<void>(r => {
      release = r;
    });
    // The overridden run executes on the 'fast' mode's agent.
    agents.fast.enqueueRun({
      runId: 'run-fast',
      finishReason: 'stop',
      text: 'ok',
      chunks: [{ type: 'text-delta', payload: { id: 'm', text: 'hi' }, runId: 'run-fast' }],
      holdUntil: hold,
    });

    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    expect(session.getDisplayState().modeId).toBe('default'); // session default
    const inFlight = session.message({ content: 'hi', mode: 'fast', model: 'fast-model' });
    try {
      while (session.getDisplayState().currentRunId === undefined) await Promise.resolve();
      const mid = session.getDisplayState();
      // Session default stays 'default'; the RUN reports its effective identity.
      expect(mid.modeId).toBe('default');
      expect(mid.currentRun).toMatchObject({
        runId: mid.currentRunId,
        status: 'running',
        modeId: 'fast',
        modelId: 'fast-model',
        agentId: 'fast', // the 'fast' mode's configured agentId
      });
    } finally {
      release();
      await inFlight;
    }
  });

  it('reflects a QUEUED turn’s effective mode override in the live run projection', async () => {
    const { harness, agents } = setupHarness({
      agents: { default: new MockAgent({ id: 'default' }), fast: new MockAgent({ id: 'fast-agent' }) },
      modes: [
        { id: 'default', agentId: 'default' },
        { id: 'fast', agentId: 'fast' },
      ],
      defaultModeId: 'default',
    });
    let release!: () => void;
    const hold = new Promise<void>(r => {
      release = r;
    });
    agents.fast.enqueueRun({
      runId: 'run-q',
      finishReason: 'stop',
      text: 'ok',
      chunks: [{ type: 'text-delta', payload: { id: 'm', text: 'hi' }, runId: 'run-q' }],
      holdUntil: hold,
    });

    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    const queued = session.queue({ content: 'hi', mode: 'fast' });
    try {
      while (session.getDisplayState().currentRunId === undefined) await Promise.resolve();
      const mid = session.getDisplayState();
      expect(mid.currentRun).toMatchObject({
        status: 'running',
        operation: { kind: 'queue' },
        modeId: 'fast', // the queued item's own mode, not the session default
        agentId: 'fast',
      });
      expect(mid.currentRun!.operation.queuedItemId).toBeTypeOf('string');
    } finally {
      release();
      await queued;
    }
  });

  it('reflects the suspended turn’s effective model (captured in runtimeDependencies)', async () => {
    const { harness, agent } = setupHarness();
    agent.enqueueRun({
      finishReason: 'suspended',
      runId: 'run-mw',
      suspendPayload: { toolCallId: 'tc-q', toolName: 'ask_user', args: { question: 'pick' } },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    // Per-turn model override on a turn that suspends — the waiting projection must
    // report the run's effective model, not the session default.
    await session.message({ content: 'ask', model: 'override-model' });

    const ds = session.getDisplayState();
    expect(ds.currentRun?.status).toBe('waiting');
    expect(ds.currentRun?.modelId).toBe('override-model');
  });

  it('does NOT mask a genuinely-running unrelated turn started while suspended', async () => {
    // Suspend turn A (leaves pendingResume), then start a NEW turn B (default
    // message() is not busy-blocked by a pending). The projection must report the
    // live run B as `running`, not the stale pending A as `waiting` — the pending
    // owns the projection only when there is no live run or the live run IS its
    // resume (same runId).
    const { harness, agent } = setupHarness();
    agent.enqueueRun({
      finishReason: 'suspended',
      runId: 'run-A',
      suspendPayload: { toolCallId: 'tc-q', toolName: 'ask_user', args: { question: 'pick' } },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await session.message({ content: 'ask' });
    expect(session.getDisplayState().currentRun?.status).toBe('waiting'); // pending owns it (no live run)
    const pendingRunId = session.getRecord().pendingResume!.runId;

    let release!: () => void;
    const hold = new Promise<void>(r => {
      release = r;
    });
    agent.enqueueRun({
      runId: 'run-B',
      finishReason: 'stop',
      text: 'ok',
      chunks: [{ type: 'text-delta', payload: { id: 'm', text: 'hi' }, runId: 'run-B' }],
      holdUntil: hold,
    });
    const runB = session.message({ content: 'unrelated' });
    try {
      while (session.getDisplayState().currentRunId === undefined) await Promise.resolve();
      const mid = session.getDisplayState();
      // The LIVE run B is surfaced as running — not masked by pending A.
      expect(mid.currentRunId).not.toBe(pendingRunId);
      expect(mid.currentRun).toMatchObject({ runId: mid.currentRunId, status: 'running' });
    } finally {
      release();
      await runB.catch(() => {});
    }
  });
});
