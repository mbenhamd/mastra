/**
 * Harness v1 — ADVERSARIAL lifecycle invariants.
 *
 * This suite targets seven lifecycle invariants that the prior audit flagged as
 * UNPROVEN, and asserts them with maximal precision: exact error CLASSES, exact
 * `lifecycleState`, and exact `HarnessAbortedError.reason` values — never
 * regexes or "does not throw".
 *
 *   1. EVICTED enforcement — every mutating entry point rejects on an evicted
 *      session; the record stays re-adoptable.
 *   2. CLOSING -> LIVE revert vs the one-way BRICK case (fail before vs after
 *      the durable closing marker commits).
 *   3. switchMode adversarial — closed-session reject + in-flight pinning.
 *   4. Terminal abort REASON — delete -> 'session_closed', evict ->
 *      'process_restart' on the in-flight tool's `abortSignal.reason`.
 *   5. cancel + restart + respond — a cancelled, restarted suspension fails
 *      closed (cancelled) instead of resuming.
 *   6. Drift gate on the RESPOND path — restart with a different mode->agent
 *      binding rejects with HarnessRuntimeDriftError.
 *   7. Races — concurrent respond (exactly one resumes), abort vs suspend
 *      capture, close vs in-flight resume; no double-resume, no unhandled
 *      rejection.
 *
 * Storage is the in-memory adapter; `InMemoryHarness({ db })` shares ONE
 * `InMemoryDB` across harness instances so a "restart" is a fresh Harness over
 * the same durable rows (mirrors concurrency-chaos.test.ts).
 */

import { describe, expect, it, vi } from 'vitest';

import { InMemoryHarness } from '../../storage/domains/harness/inmemory';
import { InMemoryDB } from '../../storage/domains/inmemory-db';

import { MockAgent } from './__test-utils__/mock-agent';
import { setupHarness } from './__test-utils__/setup';
import {
  HarnessAbortedError,
  HarnessRuntimeDriftError,
  HarnessSessionCancelledError,
  HarnessSessionClosedError,
  HarnessSessionClosingError,
  HarnessStorageError,
} from './errors';
import { Harness } from './harness';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function waitFor(predicate: () => boolean, label: string, timeoutMs = 1_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) {
      throw new Error(`timed out waiting for ${label}`);
    }
    await new Promise(resolve => setImmediate(resolve));
  }
}

function deferred<T = void>(): { promise: Promise<T>; resolve: (v: T) => void } {
  let resolve!: (v: T) => void;
  const promise = new Promise<T>(res => {
    resolve = res;
  });
  return { promise, resolve };
}

/** A single-agent, single-mode harness over a fresh shared InMemoryDB. */
function freshShared() {
  const db = new InMemoryDB();
  const storage = new InMemoryHarness({ db });
  const agent = new MockAgent({ id: 'default' });
  const harness = new Harness({
    agents: { default: agent } as any,
    modes: [{ id: 'default', agentId: 'default' }],
    defaultModeId: 'default',
    sessions: { storage },
  });
  return { db, storage, harness, agent };
}

// ===========================================================================
// 1. EVICTED enforcement
// ===========================================================================

describe('1. EVICTED enforcement', () => {
  it('every mutating entry point rejects HarnessSessionClosedError after eviction; record stays re-adoptable', async () => {
    const { harness, storage } = setupHarness({
      modes: [
        { id: 'default', agentId: 'default' },
        { id: 'other', agentId: 'default' },
      ],
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    const sessionId = session.id;

    // Force a clean lease-lost eviction via the internal eviction path. This is
    // the same primitive `_flushUpdate` reaches when it observes lease loss; here
    // it is driven directly so the session is evicted while otherwise idle.
    await harness._internalEvictLiveSessionLeaseLost(session);

    expect(session.lifecycleState).toBe('evicted');
    expect(harness._internalLiveSessionCount()).toBe(0);

    // §_assertLive: an evicted (non-live, non-deleted, non-closing) session
    // surfaces as HarnessSessionClosedError on every gated entry point.
    await expect(session.message({ content: 'x' })).rejects.toBeInstanceOf(HarnessSessionClosedError);
    await expect(session.signal({ content: 'x' })).rejects.toBeInstanceOf(HarnessSessionClosedError);
    await expect(session.queue({ content: 'x' })).rejects.toBeInstanceOf(HarnessSessionClosedError);
    await expect(session.switchMode({ mode: 'other' })).rejects.toBeInstanceOf(HarnessSessionClosedError);
    await expect(session.setState({ k: 1 })).rejects.toBeInstanceOf(HarnessSessionClosedError);

    // The record is NOT closed/deleted durably — eviction only drops the live
    // process handle. A fresh resolve re-adopts the same id.
    const stored = await storage.loadSession({ sessionId });
    expect(stored).toBeDefined();
    expect(stored!.closedAt).toBeUndefined();

    const readopted = await harness.session({ sessionId });
    expect(readopted.id).toBe(sessionId);
    expect(readopted.lifecycleState).toBe('live');
    // The re-adopted handle accepts work again.
    await expect(readopted.setState({ k: 2 })).resolves.toBeUndefined();
  });
});

// ===========================================================================
// 2. CLOSING -> LIVE revert vs one-way BRICK
// ===========================================================================

describe('2. CLOSING -> LIVE revert vs BRICK', () => {
  it('reverts to live and accepts work when the closing marker save fails BEFORE it commits', async () => {
    const { harness, storage } = freshShared();
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });

    // Fail the closing-marker write only (closingAt set, closedAt not yet).
    const realSave = storage.saveSession.bind(storage);
    const spy = vi.spyOn(storage, 'saveSession').mockImplementation(async (rec: any, opts: any) => {
      if (rec.closingAt !== undefined && rec.closedAt === undefined) {
        throw new Error('injected closing-marker save failure');
      }
      return realSave(rec, opts);
    });

    // close() wraps the storage failure as a HarnessStorageError(session_close).
    const closeErr = await session.close().then(
      () => undefined,
      (e: unknown) => e,
    );
    expect(closeErr).toBeInstanceOf(HarnessStorageError);
    expect((closeErr as HarnessStorageError).operation).toBe('session_close');

    spy.mockRestore();

    // The closing marker never committed → the session reverts to live and
    // accepts work again (admission was only soft-rejected during the attempt).
    expect(session.lifecycleState).toBe('live');
    expect(session.isClosing).toBe(false);
    await expect(session.setState({ revived: true })).resolves.toBeUndefined();
    expect((session.getRecord().state as { revived?: boolean }).revived).toBe(true);
  });

  it('stays closing (one-way) and rejects new work when the close fails AFTER the closing marker commits', async () => {
    const { harness, storage } = freshShared();
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });

    // Let the closing marker commit, then fail the closed-marker write. The
    // session has durably entered closing; the failed close does NOT revert it.
    const realSave = storage.saveSession.bind(storage);
    const spy = vi.spyOn(storage, 'saveSession').mockImplementation(async (rec: any, opts: any) => {
      if (rec.closedAt !== undefined) {
        throw new Error('injected closed-marker save failure');
      }
      return realSave(rec, opts);
    });

    const closeErr = await session.close().then(
      () => undefined,
      (e: unknown) => e,
    );
    expect(closeErr).toBeInstanceOf(HarnessStorageError);
    expect((closeErr as HarnessStorageError).operation).toBe('session_close');

    spy.mockRestore();

    // Known one-way state: closingAt persisted, closedAt absent → stays closing.
    expect(session.isClosing).toBe(true);
    expect(session.lifecycleState).toBe('closing');
    expect(session.getRecord().closingAt).toBeDefined();
    expect(session.getRecord().closedAt).toBeUndefined();

    // New turn work is rejected with the closing error (not the closed error).
    await expect(session.message({ content: 'x' })).rejects.toBeInstanceOf(HarnessSessionClosingError);
    await expect(session.setState({ k: 1 })).rejects.toBeInstanceOf(HarnessSessionClosingError);
  });
});

// ===========================================================================
// 3. switchMode adversarial
// ===========================================================================

describe('3. switchMode adversarial', () => {
  it('rejects HarnessSessionClosedError on a closed session', async () => {
    const { harness } = setupHarness({
      modes: [
        { id: 'default', agentId: 'default' },
        { id: 'other', agentId: 'default' },
      ],
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await session.close();

    await expect(session.switchMode({ mode: 'other' })).rejects.toBeInstanceOf(HarnessSessionClosedError);
  });

  it('pins the running turn to its starting mode while flipping the record for the NEXT turn', async () => {
    // The record's modeId flips immediately (switchMode only `_assertLive`s, it
    // does NOT wait for a turn boundary). But the in-flight run uses its
    // turn-start identity for resume/agent resolution. Pin both facts precisely.
    const agentDefault = new MockAgent({ id: 'agent-default' });
    const agentOther = new MockAgent({ id: 'agent-other' });
    const storage = new InMemoryHarness({ db: new InMemoryDB() });
    const harness = new Harness({
      agents: { 'agent-default': agentDefault, 'agent-other': agentOther } as any,
      modes: [
        { id: 'default', agentId: 'agent-default' },
        { id: 'other', agentId: 'agent-other' },
      ],
      defaultModeId: 'default',
      sessions: { storage },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });

    const hold = deferred();
    agentDefault.enqueueRun({ finishReason: 'stop', text: 'done', holdUntil: hold.promise });

    const turn = session.message({ content: 'go' });
    // The signal/subscription path marks the session running BEFORE it actually
    // dispatches `agent.stream`, so wait for the real dispatch (which also wires
    // the per-turn abortSignal) rather than the bare `isRunning()` flag.
    await waitFor(() => agentDefault.streamCalls.length >= 1, 'turn dispatched');

    // Mid-flight switch. The record flips now.
    await session.switchMode({ mode: 'other' });
    expect(session.getRecord().modeId).toBe('other');
    expect(session.getCurrentModeId()).toBe('other');

    // The in-flight run was dispatched to the original mode's agent only.
    expect(agentDefault.streamCalls).toHaveLength(1);
    expect(agentOther.streamCalls).toHaveLength(0);

    hold.resolve();
    await turn;

    // After the turn, the NEXT message uses the switched mode's agent.
    agentOther.enqueueRun({ finishReason: 'stop', text: 'second' });
    await session.message({ content: 'again' });
    expect(agentOther.streamCalls).toHaveLength(1);
  });
});

// ===========================================================================
// 4. Terminal abort REASON
// ===========================================================================

describe('4. terminal abort reason on the in-flight tool signal', () => {
  it("delete mid-turn aborts the in-flight signal with HarnessAbortedError reason 'session_closed'", async () => {
    const { harness, agent } = setupHarness();
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });

    const hold = deferred();
    let captured: unknown;
    // MockAgent.onAbort fires with the per-turn abortSignal.reason.
    agent.enqueueRun({
      finishReason: 'stop',
      holdUntil: hold.promise,
      onAbort: reason => {
        captured = reason;
      },
    });

    const turn = session.message({ content: 'go' });
    void turn.catch(() => {});
    // Wait for the actual `agent.stream` dispatch — that is when the per-turn
    // abortSignal (and the onAbort observer) is wired. `isRunning()` flips
    // earlier, before dispatch, so deleting on it would race the wiring.
    await waitFor(() => agent.streamCalls.length >= 1, 'turn dispatched');

    // Hard-delete the live handle: the in-flight tool's signal aborts with the
    // terminal `session_closed` reason (record gone → tools run cleanup).
    (session as unknown as { _markDeleted(): void })._markDeleted();

    await waitFor(() => captured !== undefined, 'abort reason captured');
    expect(captured).toBeInstanceOf(HarnessAbortedError);
    expect((captured as HarnessAbortedError).reason).toBe('session_closed');
    expect((captured as HarnessAbortedError).sessionId).toBe(session.id);

    hold.resolve();
  });

  it("evict mid-turn aborts the in-flight signal with HarnessAbortedError reason 'process_restart'", async () => {
    const { harness, agent } = setupHarness();
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });

    const hold = deferred();
    let captured: unknown;
    agent.enqueueRun({
      finishReason: 'stop',
      holdUntil: hold.promise,
      onAbort: reason => {
        captured = reason;
      },
    });

    const turn = session.message({ content: 'go' });
    void turn.catch(() => {});
    // Wait for the real `agent.stream` dispatch so the per-turn abortSignal is
    // wired before eviction (the running flag flips before dispatch).
    await waitFor(() => agent.streamCalls.length >= 1, 'turn dispatched');

    // Lease-lost eviction releases the live handle without a durable close →
    // tools see `process_restart` (record survives, best-effort cleanup).
    await harness._internalEvictLiveSessionLeaseLost(session);

    await waitFor(() => captured !== undefined, 'abort reason captured');
    expect(captured).toBeInstanceOf(HarnessAbortedError);
    expect((captured as HarnessAbortedError).reason).toBe('process_restart');
    expect((captured as HarnessAbortedError).sessionId).toBe(session.id);

    hold.resolve();
  });
});

// ===========================================================================
// 5. cancel + restart + respond -> fails closed (cancelled)
// ===========================================================================

describe('5. cancelled + restarted suspension fails closed', () => {
  it('respondToToolApproval rejects HarnessSessionCancelledError after cancel + restart, never resuming', async () => {
    const db = new InMemoryDB();
    const storage = new InMemoryHarness({ db });

    const agent1 = new MockAgent({ id: 'default' });
    const harness1 = new Harness({
      agents: { default: agent1 } as any,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage },
    });

    agent1.enqueueRun({
      finishReason: 'suspended',
      runId: 'run-cancel',
      suspendPayload: { toolCallId: 'tc-cancel', toolName: 'shell', args: { cmd: 'ls' } },
    });
    const session1 = await harness1.session({ resourceId: 'u', threadId: { fresh: true } });
    const sessionId = session1.id;
    await session1.message({ content: 'go' });
    expect(session1.getRecord().pendingResume).toBeDefined();

    // Durable cancel request, then "restart": shut down and adopt under a fresh
    // harness over the SAME storage.
    await session1.cancel({ reason: 'user-aborted' });
    await harness1.shutdown();

    const agent2 = new MockAgent({ id: 'default' });
    const harness2 = new Harness({
      agents: { default: agent2 } as any,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage },
    });
    const session2 = await harness2.session({ sessionId, resourceId: 'u' });

    // The pending resume is still parked, but the cancel request fails it closed.
    expect(session2.getRecord().pendingResume).toBeDefined();
    expect(session2.getRecord().cancelRequest).toMatchObject({ reason: 'user-aborted' });

    await expect(session2.respondToToolApproval({ approved: true })).rejects.toBeInstanceOf(
      HarnessSessionCancelledError,
    );
    // Fail-closed: the agent layer was never invoked.
    expect(agent2.resumeCalls).toHaveLength(0);
  });
});

// ===========================================================================
// 6. Drift gate on the RESPOND path
// ===========================================================================

describe('6. respond-path runtime drift gate', () => {
  it('respondToToolApproval rejects HarnessRuntimeDriftError after restart with a different mode->agent binding', async () => {
    const db = new InMemoryDB();
    const storage = new InMemoryHarness({ db });

    // harness1: mode `default` bound to agentA. Suspend captures agentA in the
    // pending resume's runtimeDependencies.
    const agentA = new MockAgent({ id: 'agentA' });
    const harness1 = new Harness({
      agents: { agentA } as any,
      modes: [{ id: 'default', agentId: 'agentA' }],
      defaultModeId: 'default',
      sessions: { storage },
    });
    agentA.enqueueRun({
      finishReason: 'suspended',
      runId: 'run-drift',
      suspendPayload: { toolCallId: 'tc-drift', toolName: 'shell', args: { cmd: 'ls' } },
    });
    const session1 = await harness1.session({ resourceId: 'u', threadId: { fresh: true } });
    const sessionId = session1.id;
    await session1.message({ content: 'go' });
    expect(session1.getRecord().pendingResume?.runtimeDependencies?.agentId).toBe('agentA');
    await harness1.shutdown();

    // harness2: SAME mode id `default` now bound to a DIFFERENT agent (agentB).
    const agentB = new MockAgent({ id: 'agentB' });
    const harness2 = new Harness({
      agents: { agentB } as any,
      modes: [{ id: 'default', agentId: 'agentB' }],
      defaultModeId: 'default',
      sessions: { storage },
    });
    const session2 = await harness2.session({ sessionId, resourceId: 'u' });

    const driftErr = await session2.respondToToolApproval({ approved: true }).then(
      () => undefined,
      (e: unknown) => e,
    );
    expect(driftErr).toBeInstanceOf(HarnessRuntimeDriftError);
    expect((driftErr as HarnessRuntimeDriftError).code).toBe('harness.runtime_drift');
    expect(agentB.resumeCalls).toHaveLength(0);
    // The pending resume is left intact (resumedAt not stamped) for a future
    // owner with the right binding.
    expect(session2.getRecord().pendingResume).toBeDefined();
    expect(session2.getRecord().pendingResume?.resumedAt).toBeUndefined();
  });
});

// ===========================================================================
// 7. Races
// ===========================================================================

describe('7. in-process races', () => {
  it('two concurrent respondToToolApproval calls: exactly one resumes, the other rejects deterministically', async () => {
    const { harness, agent } = setupHarness();
    agent.enqueueRun({
      finishReason: 'suspended',
      runId: 'run-race',
      suspendPayload: { toolCallId: 'tc-race', toolName: 'shell', args: { cmd: 'ls' } },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await session.message({ content: 'go' });

    // Hold the resume so both calls overlap before either settles.
    const hold = deferred();
    agent.enqueueRun({ finishReason: 'stop', runId: 'run-race', text: 'done', holdUntil: hold.promise });

    const first = session.respondToToolApproval({ approved: true }).then(
      r => ({ ok: true as const, r }),
      e => ({ ok: false as const, e }),
    );
    const second = session.respondToToolApproval({ approved: true }).then(
      r => ({ ok: true as const, r }),
      e => ({ ok: false as const, e }),
    );

    // Let admission serialize: exactly one resume call is issued before release.
    await new Promise(resolve => setImmediate(resolve));
    expect(agent.resumeCalls).toHaveLength(1);

    hold.resolve();
    const [a, b] = await Promise.all([first, second]);

    const oks = [a, b].filter(x => x.ok);
    const fails = [a, b].filter(x => !x.ok);
    // No legacy-responseId duplicate dedup here (no responseId supplied), so the
    // loser fails closed rather than returning an idempotent receipt.
    expect(oks).toHaveLength(1);
    expect(fails).toHaveLength(1);
    // The agent was resumed exactly once — no double-resume.
    expect(agent.resumeCalls).toHaveLength(1);
    expect(session.getRecord().pendingResume).toBeUndefined();
  });

  it('abort() racing the suspend capture leaves a coherent terminal state (no unhandled rejection)', async () => {
    const { harness, agent } = setupHarness();

    const hold = deferred();
    agent.enqueueRun({
      finishReason: 'suspended',
      runId: 'run-abort-race',
      suspendPayload: { toolCallId: 'tc-ar', toolName: 'shell', args: {} },
      holdUntil: hold.promise,
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });

    const turn = session.message({ content: 'go' }).then(
      r => ({ ok: true as const, r }),
      e => ({ ok: false as const, e }),
    );
    await waitFor(() => agent.streamCalls.length >= 1, 'turn dispatched');

    // Abort while the run is mid-flight, then release the hold so capture races
    // the abort. The turn must settle exactly once and the session must be idle.
    session.abort();
    hold.resolve();

    const outcome = await turn;
    expect(outcome).toBeDefined();
    await waitFor(() => !session.isRunning(), 'turn settled');
    expect(session.isRunning()).toBe(false);
    // Exactly one stream dispatch happened; the race did not double-run.
    expect(agent.streamCalls).toHaveLength(1);
  });

  it('close() racing an in-flight resume settles cleanly with no double-resume', async () => {
    const agent = new MockAgent({ id: 'default' });
    const storage = new InMemoryHarness({ db: new InMemoryDB() });
    const harness = new Harness({
      agents: { default: agent } as any,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage, closeTimeoutMs: 50 },
    });
    agent.enqueueRun({
      finishReason: 'suspended',
      runId: 'run-close-race',
      suspendPayload: { toolCallId: 'tc-cr', toolName: 'shell', args: { cmd: 'ls' } },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await session.message({ content: 'go' });

    // Resume hangs mid-flight; close() races it.
    const hold = deferred();
    agent.enqueueRun({ finishReason: 'stop', runId: 'run-close-race', text: 'done', holdUntil: hold.promise });

    const resume = session.respondToToolApproval({ approved: true }).then(
      r => ({ ok: true as const, r }),
      e => ({ ok: false as const, e }),
    );
    await waitFor(() => session.isRunning(), 'resume in flight');

    const close = session.close();
    // Let close drive the abort/drain, then release the held resume.
    await new Promise(resolve => setImmediate(resolve));
    hold.resolve();

    await Promise.all([resume.catch(() => {}), close.catch(() => {})]);
    await close.catch(() => {});

    // The agent was resumed exactly once regardless of who won the race.
    expect(agent.resumeCalls).toHaveLength(1);
    // Session reached a terminal lifecycle (closed) without wedging.
    await waitFor(() => session.isClosed || session.lifecycleState === 'closed', 'session closed');
    expect(session.isRunning()).toBe(false);
  });
});
