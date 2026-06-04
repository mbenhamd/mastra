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
import { setupHarness } from './__test-utils__/setup';
import { MockAgent } from './__test-utils__/mock-agent';
import { toHarnessDisplayStateSnapshotV1 } from './display-state';

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
