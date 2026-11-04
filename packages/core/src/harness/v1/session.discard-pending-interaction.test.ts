/**
 * Harness v1 — `session.discardPendingInteraction(...)` (PF-7532).
 *
 * One row per path: normal discard, durable reload (committed and crash
 * between phases), stale generation, concurrent newer/active turn, replay of
 * the call and of a retried answer, the restored-after-failed-resume shape,
 * and a typed storage failure.
 */

import { describe, expect, it, vi } from 'vitest';

import { Agent } from '../../agent';
import { InMemoryHarness } from '../../storage/domains/harness/inmemory';
import type { PendingResume } from '../../storage/domains/harness/types';
import { InMemoryDB } from '../../storage/domains/inmemory-db';
import type { MastraModelOutput } from '../../stream/base/output';
import { buildFakeOutput } from './__test-utils__/fake-output';
import { MockAgent } from './__test-utils__/mock-agent';

import {
  HarnessBusyError,
  HarnessInboxResponseConflictError,
  HarnessPendingInteractionDiscardedError,
  HarnessStorageError,
} from './errors';
import { Harness } from './harness';
import { Session } from './session';

interface RunSpec {
  finishReason: string;
  suspendPayload?: { toolCallId: string; toolName: string; args?: unknown };
  text?: string;
  runId?: string;
  holdUntil?: Promise<void>;
  /** Side effect of the approved tool running inside the resumed segment. */
  executeTool?: () => void;
  /** The run's stream ends, then its FullOutput rejects with this error. */
  failAfterStream?: Error;
}

class FakeAgent extends Agent<any, any, any> {
  runs: RunSpec[] = [];
  resumeCalls: unknown[] = [];
  streamCalls = 0;

  constructor() {
    super({ id: 'fake', name: 'fake', instructions: 'fake', model: 'openai/gpt-4o-mini' as any });
  }

  enqueueRun(spec: RunSpec): void {
    this.runs.push({ runId: 'fake-run', text: 'ok', ...spec });
  }

  private buildOutput(spec: RunSpec, runIdOverride?: string): MastraModelOutput {
    const runId = runIdOverride ?? spec.runId;
    const totalUsage = { inputTokens: 0, outputTokens: 0, totalTokens: 0 };
    return buildFakeOutput({
      runId,
      fullOutput: {
        text: spec.text ?? '',
        usage: totalUsage,
        finishReason: spec.finishReason,
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
        totalUsage,
        error: undefined,
        tripwire: undefined,
        traceId: undefined,
        spanId: undefined,
        suspendPayload: spec.suspendPayload,
        messages: [],
        rememberedMessages: [],
      },
      ...(spec.failAfterStream ? { failAfterStream: spec.failAfterStream } : {}),
    });
  }

  async stream(_messages: any, options?: any): Promise<any> {
    this.streamCalls += 1;
    const spec = this.runs.shift();
    if (!spec) throw new Error('FakeAgent: no run enqueued for stream()');
    await spec.holdUntil;
    const out = this.buildOutput(spec, options?.runId);
    this._internalRegisterStreamRun(out, (options ?? {}) as any);
    return out;
  }

  async resumeStream(resumeData: any, options?: any): Promise<any> {
    this.resumeCalls.push(resumeData);
    const spec = this.runs.shift();
    if (!spec) throw new Error('FakeAgent: no run enqueued for resumeStream()');
    await spec.holdUntil;
    spec.executeTool?.();
    const out = this.buildOutput(spec, options?.runId);
    this._internalRegisterStreamRun(out, (options ?? {}) as any);
    return out;
  }
}

function newHarness(agent: FakeAgent, storage: InMemoryHarness, sessionOptions: Record<string, unknown> = {}) {
  return new Harness({
    agents: { default: agent } as any,
    modes: [{ id: 'default', agentId: 'default' }],
    defaultModeId: 'default',
    sessions: { storage, ...sessionOptions },
  });
}

function setup(sessionOptions: Record<string, unknown> = {}) {
  const agent = new FakeAgent();
  const storage = new InMemoryHarness({ db: new InMemoryDB() });
  return { agent, storage, harness: newHarness(agent, storage, sessionOptions) };
}

/** Fail exactly the discard's phase-two clear (the claim commits). */
function failDiscardClear(storage: InMemoryHarness) {
  const save = storage.saveSession.bind(storage);
  return vi.spyOn(storage, 'saveSession').mockImplementation(async (record, options) => {
    if (record.pendingResume === undefined) throw new Error('process died before the clear');
    return save(record, options);
  });
}

function approvalRun(runId: string, toolCallId: string, toolName = 'start_research_workflow'): RunSpec {
  return { finishReason: 'suspended', runId, suspendPayload: { toolCallId, toolName, args: { topic: 'x' } } };
}

function generationOf(pending: PendingResume) {
  return {
    kind: pending.kind,
    itemId: pending.itemId ?? pending.toolCallId,
    runId: pending.runId,
    toolCallId: pending.toolCallId,
    pendingRequestedAt: pending.requestedAt,
  };
}

function answerIdentity(pending: PendingResume) {
  const { itemId, runId, toolCallId, pendingRequestedAt } = generationOf(pending);
  return { itemId, runId, toolCallId, pendingRequestedAt };
}

async function suspendedSession(harness: Harness, agent: FakeAgent) {
  agent.enqueueRun(approvalRun('run-A', 'tc-A'));
  const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
  await session.message({ content: 'start the research run' });
  const pending = session.getRecord().pendingResume!;
  expect(pending).toBeDefined();
  return { session, pending };
}

async function waitFor(predicate: () => boolean, label: string, timeoutMs = 3_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${label}`);
    await new Promise(resolve => setTimeout(resolve, 10));
  }
}

describe('Session.discardPendingInteraction', () => {
  it('discards the current pending interaction without resuming or aborting', async () => {
    const { harness, agent } = setup();
    const { session, pending } = await suspendedSession(harness, agent);
    const events: Array<{ type: string; finishReason?: string }> = [];
    session.subscribe(event => events.push(event as (typeof events)[number]));

    const result = await session.discardPendingInteraction({
      ...generationOf(pending),
      responseId: 'discard-1',
      reason: 'run_settled',
    });

    expect(result).toMatchObject({
      status: 'discarded',
      itemId: generationOf(pending).itemId,
      responseId: 'discard-1',
    });
    expect(session.getRecord().pendingResume).toBeUndefined();
    expect(session.getDisplayState().pending).toBeNull();
    expect(agent.resumeCalls).toHaveLength(0);
    expect(events.some(event => event.finishReason === 'aborted')).toBe(false);
    expect(session.isRunning()).toBe(false);
    // Not answerable any more: a fresh answer reports the discard, no resume.
    await expect(
      session.respondToToolApproval({ ...answerIdentity(pending), responseId: 'decline-1', approved: false }),
    ).rejects.toBeInstanceOf(HarnessPendingInteractionDiscardedError);
    expect(agent.resumeCalls).toHaveLength(0);
    await harness.shutdown();
  });

  it('stays resolved after a reload, including a discard whose clear never committed', async () => {
    const { harness, agent, storage } = setup();
    const { session, pending } = await suspendedSession(harness, agent);
    const sessionId = session.id;
    const save = storage.saveSession.bind(storage);
    // Crash shape: the claim commits, the clear does not.
    const saveSpy = vi.spyOn(storage, 'saveSession').mockImplementation(async (record, options) => {
      if (record.pendingResume === undefined) throw new Error('process died before the clear');
      return save(record, options);
    });
    await expect(
      session.discardPendingInteraction({ ...generationOf(pending), responseId: 'discard-1', reason: 'run_settled' }),
    ).rejects.toBeInstanceOf(HarnessStorageError);
    expect(session.getRecord().pendingResume?.expiryStartedAt).toBeDefined();
    saveSpy.mockRestore();
    await harness.shutdown();

    const agent2 = new FakeAgent();
    const harness2 = newHarness(agent2, storage);
    const restored = await harness2.session({ sessionId, resourceId: 'u' });
    // Hydration completes the claimed row as the discard (not as an expiry).
    await waitFor(() => restored.getRecord().pendingResume === undefined, 'claimed discard completed on reload');
    expect(Object.keys(restored.getRecord().expiredPendingInteractions ?? {})).toHaveLength(0);
    expect(restored.getDisplayState().pending).toBeNull();
    await expect(
      restored.discardPendingInteraction({ ...generationOf(pending), responseId: 'discard-1', reason: 'run_settled' }),
    ).resolves.toMatchObject({ status: 'already_resolved', responseId: 'discard-1' });
    await expect(
      restored.respondToToolApproval({ ...answerIdentity(pending), responseId: 'approve-late', approved: true }),
    ).rejects.toBeInstanceOf(HarnessPendingInteractionDiscardedError);
    expect(agent2.resumeCalls).toHaveLength(0);
    await harness2.shutdown();

    // A committed discard reloads with no pending and replays as resolved.
    const harness3 = newHarness(new FakeAgent(), storage);
    const reloaded = await harness3.session({ sessionId, resourceId: 'u' });
    expect(reloaded.getRecord().pendingResume).toBeUndefined();
    await expect(
      reloaded.discardPendingInteraction({ ...generationOf(pending), responseId: 'discard-2', reason: 'run_settled' }),
    ).resolves.toMatchObject({ status: 'already_resolved', responseId: 'discard-1' });
    await harness3.shutdown();
  });

  it('returns not_current for an older generation and leaves the newer interaction untouched', async () => {
    const { harness, agent } = setup();
    const { session, pending: older } = await suspendedSession(harness, agent);
    agent.enqueueRun({ finishReason: 'stop', runId: 'run-A', text: 'approved' });
    await session.respondToToolApproval({ approved: true });
    agent.enqueueRun(approvalRun('run-B', 'tc-B'));
    await session.message({ content: 'next' });
    const newer = session.getRecord().pendingResume!;
    expect(newer.toolCallId).toBe('tc-B');
    const versionBefore = session.getRecord().version;

    await expect(
      session.discardPendingInteraction({ ...generationOf(older), responseId: 'stale-1', reason: 'run_settled' }),
    ).resolves.toEqual({ status: 'not_current', itemId: generationOf(older).itemId, kind: 'tool-approval' });
    expect(session.getRecord().pendingResume).toEqual(newer);
    expect(session.getRecord().version).toBe(versionBefore);

    agent.enqueueRun({ finishReason: 'stop', runId: 'run-B', text: 'done' });
    await expect(session.respondToToolApproval({ approved: true })).resolves.toMatchObject({ finishReason: 'stop' });
    await harness.shutdown();
  });

  it('never aborts an active turn or touches its subagents', async () => {
    const { harness, agent } = setup();
    const { session, pending } = await suspendedSession(harness, agent);
    const cancelSubagents = vi.spyOn(session as any, '_cancelOwnedSubagentSessions');

    // A resume that is admitted and running owns the interaction: busy, untouched.
    let releaseResume!: () => void;
    agent.enqueueRun({
      finishReason: 'stop',
      runId: 'run-A',
      text: 'resumed',
      holdUntil: new Promise<void>(resolve => (releaseResume = resolve)),
    });
    const resuming = session.respondToToolApproval({ approved: true });
    await waitFor(() => agent.resumeCalls.length === 1, 'resume started');
    await expect(
      session.discardPendingInteraction({ ...generationOf(pending), responseId: 'discard-1', reason: 'run_settled' }),
    ).rejects.toBeInstanceOf(HarnessBusyError);
    releaseResume();
    await expect(resuming).resolves.toMatchObject({ finishReason: 'stop' });

    // A newer turn running during a stale discard is not aborted.
    let releaseTurn!: () => void;
    agent.enqueueRun({
      finishReason: 'stop',
      runId: 'run-C',
      text: 'newer turn',
      holdUntil: new Promise<void>(resolve => (releaseTurn = resolve)),
    });
    const newerTurn = session.message({ content: 'newer' });
    await waitFor(() => session.isRunning(), 'newer turn running');
    await expect(
      session.discardPendingInteraction({ ...generationOf(pending), responseId: 'discard-2', reason: 'run_settled' }),
    ).resolves.toMatchObject({ status: 'not_current' });
    expect(session.isRunning()).toBe(true);
    releaseTurn();
    await expect(newerTurn).resolves.toMatchObject({ finishReason: 'stop' });
    expect(cancelSubagents).not.toHaveBeenCalled();
    await harness.shutdown();
  });

  it('replays the call and a retried answer with the same responseId without new writes', async () => {
    const { harness, agent, storage } = setup();
    const { session, pending } = await suspendedSession(harness, agent);
    const input = { ...generationOf(pending), responseId: 'discard-1', reason: 'run_settled' };
    const first = await session.discardPendingInteraction(input);
    expect(first.status).toBe('discarded');
    const saveSpy = vi.spyOn(storage, 'saveSession');

    const replay = await session.discardPendingInteraction(input);
    expect(replay).toEqual({ ...first, status: 'already_resolved' });
    await expect(session.discardPendingInteraction({ ...input, responseId: 'discard-other' })).resolves.toEqual({
      ...first,
      status: 'already_resolved',
    });
    await expect(
      session.respondToToolApproval({ ...answerIdentity(pending), responseId: 'discard-1', approved: false }),
    ).rejects.toBeInstanceOf(HarnessPendingInteractionDiscardedError);
    expect(saveSpy).not.toHaveBeenCalled();
    expect(agent.resumeCalls).toHaveLength(0);
    // The same responseId for a different discard payload conflicts.
    await expect(session.discardPendingInteraction({ ...input, reason: 'other' })).rejects.toBeInstanceOf(
      HarnessInboxResponseConflictError,
    );
    await harness.shutdown();
  });

  it('resolves an approval restored after a failed resume so the session can settle and delete (PF-7532)', async () => {
    const { harness, agent } = setup();
    const { session, pending } = await suspendedSession(harness, agent);
    // The approved tool runs (the research workflow starts), then the resumed
    // segment fails before the harness observes the tool result, so
    // `_revertFailedResumeAdmission` restores the approval as answerable.
    let toolExecutions = 0;
    agent.enqueueRun({
      finishReason: 'error',
      runId: 'run-A',
      executeTool: () => {
        toolExecutions += 1;
      },
      failAfterStream: new Error('continuation model call failed'),
    });
    await expect(
      session.respondToToolApproval({ ...answerIdentity(pending), responseId: 'approve-1', approved: true }),
    ).rejects.toBeTruthy();
    expect(toolExecutions).toBe(1);
    expect(session.getRecord().pendingResume?.resumedAt).toBeUndefined();
    expect(session.getRecord().inboxResponseReceipts?.['approve-1']).toMatchObject({
      status: 'failed',
      retryable: true,
    });

    await expect(
      session.discardPendingInteraction({ ...generationOf(pending), responseId: 'discard-1', reason: 'run_settled' }),
    ).resolves.toMatchObject({ status: 'discarded' });
    expect(session.getRecord().pendingResume).toBeUndefined();
    expect(session.getRecord().inboxResponseReceipts?.['approve-1']).toMatchObject({
      status: 'failed',
      retryable: false,
      error: { code: 'harness.pending_interaction_discarded' },
    });
    // A retry of the original approval no longer resumes the executed tool.
    await expect(
      session.respondToToolApproval({ ...answerIdentity(pending), responseId: 'approve-1', approved: true }),
    ).rejects.toBeInstanceOf(HarnessPendingInteractionDiscardedError);
    expect(agent.resumeCalls).toHaveLength(1);
    expect(toolExecutions).toBe(1);

    await session.close();
    await expect(harness.deleteSession({ sessionId: session.id, resourceId: 'u' })).resolves.toBeUndefined();
    await harness.shutdown();
  });

  it('surfaces a storage failure as HarnessStorageError and leaves the interaction answerable', async () => {
    const { harness, agent, storage } = setup();
    const { session, pending } = await suspendedSession(harness, agent);
    const save = storage.saveSession.bind(storage);
    const saveSpy = vi.spyOn(storage, 'saveSession').mockImplementationOnce(async () => {
      throw new Error('storage unavailable');
    });
    const input = { ...generationOf(pending), responseId: 'discard-1', reason: 'run_settled' };

    const failure = await session.discardPendingInteraction(input).then(
      () => undefined,
      (err: unknown) => err,
    );
    expect(failure).toBeInstanceOf(HarnessStorageError);
    expect(failure).toMatchObject({
      operation: 'inbox_response',
      subject: { kind: 'inbox_response', id: 'discard-1' },
    });
    expect(session.getRecord().pendingResume).toEqual(pending);
    expect(session.getRecord().inboxResponseReceipts?.['discard-1']).toBeUndefined();

    saveSpy.mockImplementation(save);
    await expect(session.discardPendingInteraction(input)).resolves.toMatchObject({ status: 'discarded' });
    expect(session.getRecord().pendingResume).toBeUndefined();
    await harness.shutdown();
  });

  it('cold recovery of a half-committed discard does not start queued work', async () => {
    const { harness, agent, storage } = setup();
    const { session, pending } = await suspendedSession(harness, agent);
    const successor = session.queue({ content: 'queued successor' });
    void successor.catch(() => {});
    await waitFor(() => session.getRecord().pendingQueue.length === 1, 'successor queued');
    const sessionId = session.id;
    const clearFails = failDiscardClear(storage);
    await expect(
      session.discardPendingInteraction({ ...generationOf(pending), responseId: 'discard-1', reason: 'run_settled' }),
    ).rejects.toBeInstanceOf(HarnessStorageError);
    clearFails.mockRestore();
    await harness.shutdown();

    // The claimed row is due for the cold sweep at once; its original
    // interaction deadline (the default TTL) is still far in the future.
    const stored = (await storage.loadSession({ sessionId }))!;
    expect(stored.pendingResume?.expiryStartedAt).toBeDefined();
    expect(stored.pendingQueue).toHaveLength(1);
    expect(Date.now()).toBeLessThan(pending.expiresAt);

    const agent2 = new FakeAgent();
    agent2.enqueueRun({ finishReason: 'stop', runId: 'run-S', text: 'successor ran' });
    const harness2 = newHarness(agent2, storage);
    (harness2 as any)._stopPendingInteractionExpirySweepLoop();
    const drainKicks = vi.spyOn(Session.prototype as any, '_maybeDrainQueue');
    await harness2._tickPendingInteractionExpirySweep();
    await new Promise(resolve => setTimeout(resolve, 100));

    const recovered = (await storage.loadSession({ sessionId }))!;
    expect(recovered.pendingResume).toBeUndefined();
    expect(Object.keys(recovered.expiredPendingInteractions ?? {})).toHaveLength(0);
    expect(recovered.pendingQueue).toHaveLength(1);
    expect(agent2.streamCalls).toBe(0);
    // The sweep's transient session must not even start queue processing.
    expect(drainKicks).not.toHaveBeenCalled();
    drainKicks.mockRestore();
    await harness2.shutdown();
  });

  it('expiry racing a claimed discard finishes it as the discard, with no expiry evidence', async () => {
    const { harness, agent, storage } = setup({ pendingInteractionTtlMs: 600 });
    const { session, pending } = await suspendedSession(harness, agent);
    const clearFails = failDiscardClear(storage);
    await expect(
      session.discardPendingInteraction({ ...generationOf(pending), responseId: 'discard-1', reason: 'run_settled' }),
    ).rejects.toBeInstanceOf(HarnessStorageError);
    await waitFor(() => Date.now() > pending.expiresAt + 5, 'deadline passed');
    clearFails.mockRestore();
    (session as any)._clearPendingInteractionExpiryTimer();

    await (session as any)._expirePendingInteraction({
      kind: pending.kind,
      itemId: generationOf(pending).itemId,
      runId: pending.runId,
      toolCallId: pending.toolCallId,
      requestedAt: pending.requestedAt,
      expiresAt: pending.expiresAt,
    });

    expect(session.getRecord().pendingResume).toBeUndefined();
    expect(Object.keys(session.getRecord().expiredPendingInteractions ?? {})).toHaveLength(0);
    await expect(
      session.discardPendingInteraction({ ...generationOf(pending), responseId: 'discard-1', reason: 'run_settled' }),
    ).resolves.toMatchObject({ status: 'already_resolved' });
    await harness.shutdown();
  });

  it('an answer between the discard phases after the deadline gets the discard, not an expiry', async () => {
    const { harness, agent, storage } = setup({ pendingInteractionTtlMs: 600 });
    const { session, pending } = await suspendedSession(harness, agent);
    const clearFails = failDiscardClear(storage);
    await expect(
      session.discardPendingInteraction({ ...generationOf(pending), responseId: 'discard-1', reason: 'run_settled' }),
    ).rejects.toBeInstanceOf(HarnessStorageError);
    await waitFor(() => Date.now() > pending.expiresAt + 5, 'deadline passed');
    clearFails.mockRestore();
    (session as any)._clearPendingInteractionExpiryTimer();

    // A legacy answer (no generation identity) reaches the deadline path.
    await expect(session.respondToToolApproval({ approved: false })).rejects.toBeInstanceOf(
      HarnessPendingInteractionDiscardedError,
    );
    expect(session.getRecord().pendingResume).toBeUndefined();
    expect(Object.keys(session.getRecord().expiredPendingInteractions ?? {})).toHaveLength(0);
    // A generation-bound answer replays the same outcome.
    await expect(
      session.respondToToolApproval({ ...answerIdentity(pending), responseId: 'late-1', approved: false }),
    ).rejects.toBeInstanceOf(HarnessPendingInteractionDiscardedError);
    expect(agent.resumeCalls).toHaveLength(0);
    await harness.shutdown();
  });

  it('a discard that loses to an answer admission writes nothing and reports busy', async () => {
    const { harness, agent, storage } = setup();
    const { session, pending } = await suspendedSession(harness, agent);
    const save = storage.saveSession.bind(storage);
    let releaseAdmission!: () => void;
    const admissionGate = new Promise<void>(resolve => (releaseAdmission = resolve));
    let admissionSeen = false;
    let saves = 0;
    vi.spyOn(storage, 'saveSession').mockImplementation(async (record, options) => {
      if (!admissionSeen && record.pendingResume?.resumedAt !== undefined) {
        admissionSeen = true;
        await admissionGate;
      }
      saves += 1;
      return save(record, options);
    });
    let releaseResume!: () => void;
    agent.enqueueRun({
      finishReason: 'stop',
      runId: 'run-A',
      text: 'resumed',
      holdUntil: new Promise<void>(resolve => (releaseResume = resolve)),
    });
    const resuming = session.respondToToolApproval({ approved: true });
    await waitFor(() => admissionSeen, 'answer admission save in flight');
    // Classification still sees an unresumed interaction; the claim queues
    // behind the in-flight admission and must lose without writing.
    const discarding = session.discardPendingInteraction({
      ...generationOf(pending),
      responseId: 'discard-1',
      reason: 'run_settled',
    });
    releaseAdmission();
    await expect(discarding).rejects.toBeInstanceOf(HarnessBusyError);
    await waitFor(() => agent.resumeCalls.length === 1, 'resume started');
    expect(saves).toBe(1);
    expect(session.getRecord().inboxResponseReceipts?.['discard-1']).toBeUndefined();
    releaseResume();
    await expect(resuming).resolves.toMatchObject({ finishReason: 'stop' });
    await harness.shutdown();
  });

  it('keeps the discard receipt through receipt pruning (64 failed answers)', async () => {
    const { harness, agent, storage } = setup();
    const { session, pending } = await suspendedSession(harness, agent);
    for (let index = 0; index < 64; index += 1) {
      await expect(
        session.respondToToolApproval({ ...answerIdentity(pending), responseId: `approve-${index}`, approved: true }),
      ).rejects.toBeTruthy();
    }
    expect(Object.keys(session.getRecord().inboxResponseReceipts ?? {})).toHaveLength(64);
    const input = { ...generationOf(pending), responseId: 'discard-1', reason: 'run_settled' };

    // Crash shape: the claim must still be identifiable as a discard.
    const clearFails = failDiscardClear(storage);
    await expect(session.discardPendingInteraction(input)).rejects.toBeInstanceOf(HarnessStorageError);
    clearFails.mockRestore();
    expect(session.getRecord().inboxResponseReceipts?.['discard-1']).toMatchObject({ status: 'failed' });

    await expect(session.discardPendingInteraction(input)).resolves.toMatchObject({
      status: 'already_resolved',
      responseId: 'discard-1',
    });
    expect(session.getRecord().pendingResume).toBeUndefined();
    expect(Object.keys(session.getRecord().expiredPendingInteractions ?? {})).toHaveLength(0);
    await expect(session.discardPendingInteraction(input)).resolves.toMatchObject({ status: 'already_resolved' });
    await harness.shutdown();
  });

  it('round-trips a "__proto__" responseId through storage and replays it', async () => {
    const { harness, agent, storage } = setup();
    const { session, pending } = await suspendedSession(harness, agent);
    const input = { ...generationOf(pending), responseId: '__proto__', reason: 'run_settled' };
    await expect(session.discardPendingInteraction(input)).resolves.toMatchObject({
      status: 'discarded',
      responseId: '__proto__',
    });
    const sessionId = session.id;
    await harness.shutdown();

    const harness2 = newHarness(new FakeAgent(), storage);
    const restored = await harness2.session({ sessionId, resourceId: 'u' });
    expect(Object.prototype.hasOwnProperty.call(restored.getRecord().inboxResponseReceipts ?? {}, '__proto__')).toBe(
      true,
    );
    await expect(restored.discardPendingInteraction(input)).resolves.toMatchObject({
      status: 'already_resolved',
      responseId: '__proto__',
    });
    await harness2.shutdown();
  });

  it('a late answer for a discarded generation gets the discard while another interaction is pending', async () => {
    const { harness, agent } = setup();
    const { session, pending: first } = await suspendedSession(harness, agent);
    await expect(
      session.discardPendingInteraction({ ...generationOf(first), responseId: 'discard-1', reason: 'run_settled' }),
    ).resolves.toMatchObject({ status: 'discarded' });
    agent.enqueueRun(approvalRun('run-B', 'tc-B'));
    await session.message({ content: 'next' });
    const second = session.getRecord().pendingResume!;
    expect(second.toolCallId).toBe('tc-B');

    await expect(
      session.respondToToolApproval({ ...answerIdentity(first), responseId: 'late-1', approved: false }),
    ).rejects.toBeInstanceOf(HarnessPendingInteractionDiscardedError);
    expect(session.getRecord().pendingResume).toEqual(second);
    expect(agent.resumeCalls).toHaveLength(0);
    await harness.shutdown();
  });

  it('a claimed discard whose clear failed completes on the live timer long before the original deadline', async () => {
    const { harness, agent, storage } = setup();
    const { session, pending } = await suspendedSession(harness, agent);
    const clearFails = failDiscardClear(storage);
    await expect(
      session.discardPendingInteraction({ ...generationOf(pending), responseId: 'discard-1', reason: 'run_settled' }),
    ).rejects.toBeInstanceOf(HarnessStorageError);
    clearFails.mockRestore();

    await waitFor(() => session.getRecord().pendingResume === undefined, 'timer completed the discard', 5_000);
    expect(Date.now()).toBeLessThan(pending.expiresAt);
    expect(Object.keys(session.getRecord().expiredPendingInteractions ?? {})).toHaveLength(0);
    await expect(
      session.discardPendingInteraction({ ...generationOf(pending), responseId: 'discard-1', reason: 'run_settled' }),
    ).resolves.toMatchObject({ status: 'already_resolved' });
    await harness.shutdown();
  });

  it('an answer whose admission queues behind a committing discard gets the discard', async () => {
    const { harness, agent, storage } = setup();
    const { session, pending } = await suspendedSession(harness, agent);
    const save = storage.saveSession.bind(storage);
    let releaseClaim!: () => void;
    const claimGate = new Promise<void>(resolve => (releaseClaim = resolve));
    let claimSeen = false;
    vi.spyOn(storage, 'saveSession').mockImplementation(async (record, options) => {
      if (!claimSeen && record.pendingResume?.expiryStartedAt !== undefined) {
        claimSeen = true;
        await claimGate;
      }
      return save(record, options);
    });
    const discarding = session.discardPendingInteraction({
      ...generationOf(pending),
      responseId: 'discard-1',
      reason: 'run_settled',
    });
    await waitFor(() => claimSeen, 'discard claim save in flight');
    // The answer passes its pre-admission discard checks (the claim is not
    // committed yet) and queues its admission behind the claim.
    const answering = session
      .respondToToolApproval({ ...answerIdentity(pending), responseId: 'approve-1', approved: true })
      .then(
        () => undefined,
        (err: unknown) => err,
      );
    await new Promise(resolve => setTimeout(resolve, 50));
    releaseClaim();

    await expect(discarding).resolves.toMatchObject({ status: 'discarded' });
    expect(await answering).toBeInstanceOf(HarnessPendingInteractionDiscardedError);
    expect(agent.resumeCalls).toHaveLength(0);
    await harness.shutdown();
  });

  it('an answer that reaches the expiry boundary behind a committing discard gets the discard', async () => {
    const { harness, agent, storage } = setup({ pendingInteractionTtlMs: 600 });
    const { session, pending } = await suspendedSession(harness, agent);
    (session as any)._clearPendingInteractionExpiryTimer();
    await waitFor(() => Date.now() > pending.expiresAt + 5, 'deadline passed');
    const save = storage.saveSession.bind(storage);
    let releaseClaim!: () => void;
    const claimGate = new Promise<void>(resolve => (releaseClaim = resolve));
    let claimSeen = false;
    vi.spyOn(storage, 'saveSession').mockImplementation(async (record, options) => {
      if (!claimSeen && record.pendingResume?.expiryStartedAt !== undefined) {
        claimSeen = true;
        await claimGate;
      }
      return save(record, options);
    });
    const discarding = session.discardPendingInteraction({
      ...generationOf(pending),
      responseId: 'discard-1',
      reason: 'run_settled',
    });
    await waitFor(() => claimSeen, 'discard claim save in flight');
    // Past the deadline, the answer enters the expiry path behind the claim.
    const answering = session
      .respondToToolApproval({ ...answerIdentity(pending), responseId: 'approve-1', approved: true })
      .then(
        () => undefined,
        (err: unknown) => err,
      );
    await new Promise(resolve => setTimeout(resolve, 50));
    releaseClaim();

    await expect(discarding).resolves.toMatchObject({ status: 'discarded' });
    expect(await answering).toBeInstanceOf(HarnessPendingInteractionDiscardedError);
    expect(Object.keys(session.getRecord().expiredPendingInteractions ?? {})).toHaveLength(0);
    expect(agent.resumeCalls).toHaveLength(0);
    await harness.shutdown();
  });

  it('a replayed message admission does not re-park a discarded suspension', async () => {
    const { harness, agent, storage } = setup();
    agent.enqueueRun(approvalRun('run-M', 'tc-M'));
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    const write = storage.writeMessageResultEvidence.bind(storage);
    let evidenceFailed = false;
    const evidenceSpy = vi.spyOn(storage, 'writeMessageResultEvidence').mockImplementation(async (record, options) => {
      if (!evidenceFailed && record.status === 'completed') {
        evidenceFailed = true;
        throw new Error('completed evidence write failed');
      }
      return write(record, options);
    });
    const opts = { content: 'start the research run', admissionId: 'adm-discard' };
    await session.message(opts).then(
      () => undefined,
      () => undefined,
    );
    expect(evidenceFailed).toBe(true);
    evidenceSpy.mockRestore();
    const pending = session.getRecord().pendingResume!;
    expect(pending).toBeDefined();

    await expect(
      session.discardPendingInteraction({ ...generationOf(pending), responseId: 'discard-1', reason: 'run_settled' }),
    ).resolves.toMatchObject({ status: 'discarded' });

    const retry = await session.message(opts).then(
      (result: unknown) => result,
      (err: unknown) => err,
    );
    expect(retry).toBeInstanceOf(HarnessPendingInteractionDiscardedError);
    expect(session.getRecord().pendingResume).toBeUndefined();
    // The settled admission keeps replaying the discard outcome.
    await expect(session.message(opts)).rejects.toBeInstanceOf(HarnessPendingInteractionDiscardedError);
    expect(session.getRecord().pendingResume).toBeUndefined();
    await expect(session.respondToToolApproval({ approved: true })).rejects.toBeTruthy();
    expect(agent.resumeCalls).toHaveLength(0);
    await harness.shutdown();
  });

  /** Block the first discard-claim save until `release()`. */
  function gateDiscardClaim(storage: InMemoryHarness, opts: { failClear?: boolean } = {}) {
    const save = storage.saveSession.bind(storage);
    let release!: () => void;
    const gate = new Promise<void>(resolve => (release = resolve));
    const state = { claimSeen: false, release: () => release() };
    vi.spyOn(storage, 'saveSession').mockImplementation(async (record, options) => {
      if (!state.claimSeen && record.pendingResume?.expiryStartedAt !== undefined) {
        state.claimSeen = true;
        await gate;
      }
      if (opts.failClear === true && record.pendingResume === undefined) {
        throw new Error('process died before the clear');
      }
      return save(record, options);
    });
    return state;
  }

  it('an answer reusing the discard responseId that queues behind the claim gets the discard', async () => {
    const { harness, agent, storage } = setup();
    const { session, pending } = await suspendedSession(harness, agent);
    const gate = gateDiscardClaim(storage);
    const discarding = session.discardPendingInteraction({
      ...generationOf(pending),
      responseId: 'shared-1',
      reason: 'run_settled',
    });
    await waitFor(() => gate.claimSeen, 'discard claim save in flight');
    const answering = session
      .respondToToolApproval({ ...answerIdentity(pending), responseId: 'shared-1', approved: true })
      .then(
        () => undefined,
        (err: unknown) => err,
      );
    await new Promise(resolve => setTimeout(resolve, 50));
    gate.release();

    await expect(discarding).resolves.toMatchObject({ status: 'discarded' });
    expect(await answering).toBeInstanceOf(HarnessPendingInteractionDiscardedError);
    expect(session.getRecord().inboxResponseReceipts?.['shared-1']?.discardedAt).toBeDefined();
    expect(agent.resumeCalls).toHaveLength(0);
    await harness.shutdown();
  });

  it('an answer whose terminal-admission probe races the discard gets the discard and stores no refusal', async () => {
    const storage = new InMemoryHarness({
      db: new InMemoryDB(),
      terminalHandoff: { enabled: true },
      sessionRecordProjection: { enabled: true },
    });
    const agent = new MockAgent({ id: 'default' });
    agent.enqueueRun({
      finishReason: 'suspended',
      suspendPayload: { toolCallId: 'tc-T', toolName: 'start_research_workflow', args: { topic: 'x' } },
    });
    const harness = new Harness({
      agents: { default: agent } as any,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: {
        storage,
        terminalHandoff: {
          finalizer: {
            id: 'doxa.chat',
            version: '2026-09-20',
            finalize: async () => ({ projectionKind: 'chat.summary', projectionId: 'response-1', payload: {} }),
          },
        },
      },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    const first = await session.message({
      content: 'needs approval',
      admissionId: 'discard-handoff-admission',
      executionAuthorityGrant: { key: 'usage-claim-discard', generation: 1 },
      terminalAdmissionSeed: { v: 1 },
    });
    expect(first.finishReason).toBe('suspended');
    const pending = session.getRecord().pendingResume!;

    // Hold the answer's terminal-admission probe while the discard commits
    // and cancels that admission.
    let probeCalls = 0;
    let releaseProbe!: () => void;
    const probeGate = new Promise<void>(resolve => (releaseProbe = resolve));
    const byRun = storage.loadTerminalAdmissionByRun.bind(storage);
    vi.spyOn(storage, 'loadTerminalAdmissionByRun').mockImplementation(async (input: any) => {
      probeCalls += 1;
      if (probeCalls === 1) await probeGate;
      return byRun(input);
    });
    const answering = session
      .respondToToolApproval({ ...answerIdentity(pending), responseId: 'approve-1', approved: true })
      .then(
        () => undefined,
        (err: unknown) => err,
      );
    await waitFor(() => probeCalls === 1, 'answer terminal-admission probe in flight');
    await expect(
      session.discardPendingInteraction({ ...generationOf(pending), responseId: 'discard-1', reason: 'run_settled' }),
    ).resolves.toMatchObject({ status: 'discarded' });
    releaseProbe();

    expect(await answering).toBeInstanceOf(HarnessPendingInteractionDiscardedError);
    expect(session.getRecord().inboxResponseReceipts?.['approve-1']).toBeUndefined();
    await expect(
      session.respondToToolApproval({ ...answerIdentity(pending), responseId: 'approve-1', approved: true }),
    ).rejects.toBeInstanceOf(HarnessPendingInteractionDiscardedError);
    expect(agent.resumeCalls).toHaveLength(0);
    await harness.shutdown();
  });

  it('an overdue answer that drives a failing discard completion gets the discard', async () => {
    const { harness, agent, storage } = setup({ pendingInteractionTtlMs: 600 });
    const { session, pending } = await suspendedSession(harness, agent);
    (session as any)._clearPendingInteractionExpiryTimer();
    await waitFor(() => Date.now() > pending.expiresAt + 5, 'deadline passed');
    const gate = gateDiscardClaim(storage, { failClear: true });
    const discarding = session
      .discardPendingInteraction({ ...generationOf(pending), responseId: 'discard-1', reason: 'run_settled' })
      .then(
        result => result,
        (err: unknown) => err,
      );
    await waitFor(() => gate.claimSeen, 'discard claim save in flight');
    const answering = session
      .respondToToolApproval({ ...answerIdentity(pending), responseId: 'approve-1', approved: true })
      .then(
        () => undefined,
        (err: unknown) => err,
      );
    await new Promise(resolve => setTimeout(resolve, 50));
    gate.release();

    expect(await discarding).toBeInstanceOf(HarnessStorageError);
    expect(await answering).toBeInstanceOf(HarnessPendingInteractionDiscardedError);
    expect(Object.keys(session.getRecord().expiredPendingInteractions ?? {})).toHaveLength(0);
    expect(agent.resumeCalls).toHaveLength(0);
    await harness.shutdown();
  });

  it('keeps the claim when origin-signal evidence cannot be written, and recovery completes it later', async () => {
    const { harness, agent, storage } = setup();
    agent.enqueueRun(approvalRun('run-S', 'tc-S'));
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    const handle = await session.signal({ content: 'start the research run' });
    await handle.result;
    await waitFor(() => session.getRecord().pendingResume !== undefined, 'signal turn parked');
    const pending = session.getRecord().pendingResume!;
    expect(pending.originSignalId).toBe(handle.id);

    const write = storage.writeMessageResultEvidence.bind(storage);
    const evidenceSpy = vi.spyOn(storage, 'writeMessageResultEvidence').mockImplementation(async (record, options) => {
      if (record.status === 'failed') throw new Error('signal evidence write failed');
      return write(record, options);
    });
    const input = { ...generationOf(pending), responseId: 'discard-1', reason: 'run_settled' };
    await expect(session.discardPendingInteraction(input)).rejects.toBeInstanceOf(HarnessStorageError);
    expect(session.getRecord().pendingResume?.expiryStartedAt).toBeDefined();
    // Not resolved yet: a replay re-drives the settlement and still fails.
    await expect(session.discardPendingInteraction(input)).rejects.toBeInstanceOf(HarnessStorageError);
    expect(session.getRecord().pendingResume).toBeDefined();

    evidenceSpy.mockRestore();
    await waitFor(() => session.getRecord().pendingResume === undefined, 'recovery completed the discard', 5_000);
    const evidence = await session.lookupMessageResult(handle.id);
    expect(evidence && 'status' in evidence ? evidence.status : null).toBe('failed');
    await expect(session.discardPendingInteraction(input)).resolves.toMatchObject({ status: 'already_resolved' });
    await harness.shutdown();
  });

  function terminalHandoffSetup() {
    const storage = new InMemoryHarness({
      db: new InMemoryDB(),
      terminalHandoff: { enabled: true },
      sessionRecordProjection: { enabled: true },
    });
    const agent = new MockAgent({ id: 'default' });
    const harness = new Harness({
      agents: { default: agent } as any,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: {
        storage,
        terminalHandoff: {
          finalizer: {
            id: 'doxa.chat',
            version: '2026-09-20',
            finalize: async () => ({ projectionKind: 'chat.summary', projectionId: 'response-1', payload: {} }),
          },
        },
      },
    });
    return { storage, agent, harness };
  }

  it('clamps the claim to requestedAt when the local clock is behind the parking host', async () => {
    const { harness, agent, storage } = setup();
    const { session, pending } = await suspendedSession(harness, agent);
    const sessionId = session.id;
    const skewed = vi.spyOn(Date, 'now').mockReturnValue(pending.requestedAt - 50);
    const result = await session.discardPendingInteraction({
      ...generationOf(pending),
      responseId: 'discard-1',
      reason: 'run_settled',
    });
    skewed.mockRestore();
    expect(result).toMatchObject({ status: 'discarded' });
    await harness.shutdown();

    const harness2 = newHarness(new FakeAgent(), storage);
    const reopened = await harness2.session({ sessionId, resourceId: 'u' });
    expect(reopened.getRecord().pendingResume).toBeUndefined();
    await expect(
      reopened.discardPendingInteraction({ ...generationOf(pending), responseId: 'discard-1', reason: 'run_settled' }),
    ).resolves.toMatchObject({ status: 'already_resolved' });
    await harness2.shutdown();
  });

  it('a terminal-handoff message retry after a discard gets the discard, not a cancellation', async () => {
    const { agent, harness } = terminalHandoffSetup();
    agent.enqueueRun({
      finishReason: 'suspended',
      suspendPayload: { toolCallId: 'tc-T', toolName: 'start_research_workflow', args: { topic: 'x' } },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    const opts = {
      content: 'needs approval',
      admissionId: 'adm-T',
      executionAuthorityGrant: { key: 'usage-claim-discard', generation: 1 },
      terminalAdmissionSeed: { v: 1 },
    } as const;
    expect((await session.message(opts)).finishReason).toBe('suspended');
    const pending = session.getRecord().pendingResume!;
    await expect(
      session.discardPendingInteraction({ ...generationOf(pending), responseId: 'discard-1', reason: 'run_settled' }),
    ).resolves.toMatchObject({ status: 'discarded' });

    await expect(session.message(opts)).rejects.toBeInstanceOf(HarnessPendingInteractionDiscardedError);
    await expect(session.message({ ...opts, onTerminalCommit: async () => {} })).rejects.toBeInstanceOf(
      HarnessPendingInteractionDiscardedError,
    );
    expect(agent.resumeCalls).toHaveLength(0);
    await harness.shutdown();
  });

  it('signal results, duplicate signals and duplicate queue items replay the typed discard', async () => {
    const { harness, agent } = setup();
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });

    agent.enqueueRun(approvalRun('run-S', 'tc-S'));
    const handle = await session.signal({ content: 'signal turn', admissionId: 'sig-1' });
    const original = handle.result.then(
      () => undefined,
      (err: unknown) => err,
    );
    await waitFor(() => session.getRecord().pendingResume !== undefined, 'signal turn parked');
    const signalPending = session.getRecord().pendingResume!;
    await expect(
      session.discardPendingInteraction({
        ...generationOf(signalPending),
        responseId: 'discard-s',
        reason: 'run_settled',
      }),
    ).resolves.toMatchObject({ status: 'discarded' });
    expect(await original).toBeInstanceOf(HarnessPendingInteractionDiscardedError);
    const duplicateSignal = await session.signal({ content: 'signal turn', admissionId: 'sig-1' });
    await expect(duplicateSignal.result).rejects.toBeInstanceOf(HarnessPendingInteractionDiscardedError);

    agent.enqueueRun(approvalRun('run-Q', 'tc-Q'));
    const queued = session.queue({ content: 'queued turn', admissionId: 'q-1' }).then(
      () => undefined,
      (err: unknown) => err,
    );
    await waitFor(() => session.getRecord().pendingResume !== undefined, 'queued turn parked');
    const queuePending = session.getRecord().pendingResume!;
    await expect(
      session.discardPendingInteraction({
        ...generationOf(queuePending),
        responseId: 'discard-q',
        reason: 'run_settled',
      }),
    ).resolves.toMatchObject({ status: 'discarded' });
    expect(await queued).toBeInstanceOf(HarnessPendingInteractionDiscardedError);
    await expect(session.queue({ content: 'queued turn', admissionId: 'q-1' })).rejects.toBeInstanceOf(
      HarnessPendingInteractionDiscardedError,
    );
    expect(agent.resumeCalls).toHaveLength(0);
    await harness.shutdown();
  });

  it('a plain message retry after a discard gets the discard, not the cached suspension', async () => {
    const { harness, agent } = setup();
    agent.enqueueRun(approvalRun('run-M', 'tc-M'));
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    const opts = { content: 'start the research run', admissionId: 'adm-plain' };
    expect((await session.message(opts)).finishReason).toBe('suspended');
    const pending = session.getRecord().pendingResume!;
    await expect(
      session.discardPendingInteraction({ ...generationOf(pending), responseId: 'discard-1', reason: 'run_settled' }),
    ).resolves.toMatchObject({ status: 'discarded' });

    await expect(session.message(opts)).rejects.toBeInstanceOf(HarnessPendingInteractionDiscardedError);
    expect(session.getRecord().pendingResume).toBeUndefined();
    expect(agent.resumeCalls).toHaveLength(0);
    await harness.shutdown();
  });

  it('a strict discard that joins a failing best-effort signal settlement still settles the signal', async () => {
    const { harness, agent, storage } = setup();
    agent.enqueueRun(approvalRun('run-S', 'tc-S'));
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    const handle = await session.signal({ content: 'signal turn' });
    void handle.result.catch(() => {});
    await waitFor(() => session.getRecord().pendingResume !== undefined, 'signal turn parked');
    const pending = session.getRecord().pendingResume!;

    // cancel() starts the best-effort origin-signal settlement; hold its
    // write, let the discard join it, then fail it (best effort swallows).
    const write = storage.writeMessageResultEvidence.bind(storage);
    let releaseWrite!: () => void;
    const writeGate = new Promise<void>(resolve => (releaseWrite = resolve));
    let heldWrite = false;
    vi.spyOn(storage, 'writeMessageResultEvidence').mockImplementation(async (record, options) => {
      if (!heldWrite && record.status === 'failed') {
        heldWrite = true;
        await writeGate;
        throw new Error('signal evidence write failed');
      }
      return write(record, options);
    });
    const cancelling = session.cancel({ reason: 'user_cancelled' }).catch(() => undefined);
    await waitFor(() => heldWrite, 'cancel settlement write in flight');
    const discarding = session
      .discardPendingInteraction({ ...generationOf(pending), responseId: 'discard-1', reason: 'run_settled' })
      .then(
        result => result,
        (err: unknown) => err,
      );
    await new Promise(resolve => setTimeout(resolve, 50));
    releaseWrite();
    await cancelling;

    expect(await discarding).toMatchObject({ status: 'discarded' });
    const evidence = await session.lookupMessageResult(handle.id);
    expect(evidence && 'status' in evidence ? evidence.status : null).toBe('failed');
    await harness.shutdown();
  });

  it('round-trips discard fields whose reason imitates the error message', async () => {
    const tricky = 'Pending interaction "x" for session "y" was discarded (z): "w"';
    const rebuilt = HarnessPendingInteractionDiscardedError.fromProjection(
      new HarnessPendingInteractionDiscardedError('sess-1', 'item-1', tricky),
    );
    expect({ sessionId: rebuilt.sessionId, itemId: rebuilt.itemId, reason: rebuilt.reason }).toEqual({
      sessionId: 'sess-1',
      itemId: 'item-1',
      reason: tricky,
    });

    const { harness, agent } = setup();
    const { session, pending } = await suspendedSession(harness, agent);
    await expect(
      session.respondToToolApproval({ ...answerIdentity(pending), responseId: 'approve-1', approved: true }),
    ).rejects.toBeTruthy();
    await expect(
      session.discardPendingInteraction({ ...generationOf(pending), responseId: 'discard-1', reason: tricky }),
    ).resolves.toMatchObject({ status: 'discarded' });
    const replayed = await session
      .respondToToolApproval({ ...answerIdentity(pending), responseId: 'approve-1', approved: true })
      .then(
        () => undefined,
        (err: unknown) => err,
      );
    expect(replayed).toBeInstanceOf(HarnessPendingInteractionDiscardedError);
    expect(replayed).toMatchObject({ sessionId: session.id, itemId: generationOf(pending).itemId, reason: tricky });
    await harness.shutdown();
  });

  it('a discard on a cancelled session keeps the cancelled interaction as audit evidence and the session deletes', async () => {
    const { harness, agent } = setup();
    const { session, pending } = await suspendedSession(harness, agent);
    await session.cancel({ reason: 'user_cancelled' });
    expect(session.getRecord().pendingResume).toBeDefined();

    await expect(
      session.discardPendingInteraction({ ...generationOf(pending), responseId: 'discard-1', reason: 'run_settled' }),
    ).resolves.toMatchObject({ status: 'discarded' });
    expect(session.getRecord().pendingResume).toBeUndefined();
    expect(session.getRecord().inboxResponseReceipts?.['discard-1']?.cancelledPendingResume).toMatchObject({
      kind: pending.kind,
      runId: pending.runId,
      toolCallId: pending.toolCallId,
      requestedAt: pending.requestedAt,
      toolName: pending.toolName,
      cancelRequest: { reason: 'user_cancelled' },
    });

    await session.close();
    await expect(harness.deleteSession({ sessionId: session.id, resourceId: 'u' })).resolves.toBeUndefined();
    await harness.shutdown();
  });

  it('a large cancelled interaction keeps its discard claim marker until settlement succeeds', async () => {
    const { harness, agent, storage } = setup();
    agent.enqueueRun({
      finishReason: 'suspended',
      runId: 'run-B',
      suspendPayload: {
        toolCallId: 'tc-B',
        toolName: 'start_research_workflow',
        args: { blob: 'x'.repeat(500_000) },
      },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    const handle = await session.signal({ content: 'large approval' });
    void handle.result.catch(() => {});
    await waitFor(() => session.getRecord().pendingResume !== undefined, 'signal turn parked');
    const pending = session.getRecord().pendingResume!;
    expect(JSON.stringify(pending.payload).length).toBeGreaterThan(400_000);

    const write = storage.writeMessageResultEvidence.bind(storage);
    let failSignalWrites = true;
    vi.spyOn(storage, 'writeMessageResultEvidence').mockImplementation(async (record, options) => {
      if (failSignalWrites && record.status === 'failed') throw new Error('signal evidence write failed');
      return write(record, options);
    });
    await session.cancel({ reason: 'user_cancelled' });
    expect(session.getRecord().pendingResume).toBeDefined();

    const input = { ...generationOf(pending), responseId: 'discard-1', reason: 'run_settled' };
    await expect(session.discardPendingInteraction(input)).rejects.toBeInstanceOf(HarnessStorageError);
    expect(session.getRecord().pendingResume?.expiryStartedAt).toBeDefined();
    const audit = session.getRecord().inboxResponseReceipts?.['discard-1']?.cancelledPendingResume;
    expect(audit).toMatchObject({ toolCallId: 'tc-B', cancelRequest: { reason: 'user_cancelled' } });
    expect(JSON.stringify(audit).length).toBeLessThan(2_000);
    // Still unresolved while the signal evidence cannot be written.
    await expect(session.discardPendingInteraction(input)).rejects.toBeInstanceOf(HarnessStorageError);
    expect(session.getRecord().pendingResume).toBeDefined();

    failSignalWrites = false;
    await expect(session.discardPendingInteraction(input)).resolves.toMatchObject({ status: 'already_resolved' });
    expect(session.getRecord().pendingResume).toBeUndefined();
    const evidence = await session.lookupMessageResult(handle.id);
    expect(evidence && 'status' in evidence ? evidence.status : null).toBe('failed');
    await harness.shutdown();
  });

  it('decodes discard projections written in the earlier message format', () => {
    const legacy = HarnessPendingInteractionDiscardedError.fromProjection({
      message: 'Pending interaction "item-1" for session "sess-1" was discarded (run_settled)',
    });
    expect(legacy).toBeInstanceOf(HarnessPendingInteractionDiscardedError);
    expect({ sessionId: legacy.sessionId, itemId: legacy.itemId, reason: legacy.reason }).toEqual({
      sessionId: 'sess-1',
      itemId: 'item-1',
      reason: 'run_settled',
    });
    expect(legacy.message).toBe('Pending interaction "item-1" for session "sess-1" was discarded (run_settled)');
  });
});
