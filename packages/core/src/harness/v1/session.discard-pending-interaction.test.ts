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

import {
  HarnessBusyError,
  HarnessInboxResponseConflictError,
  HarnessPendingInteractionDiscardedError,
  HarnessStorageError,
} from './errors';
import { Harness } from './harness';

interface RunSpec {
  finishReason: string;
  suspendPayload?: { toolCallId: string; toolName: string; args?: unknown };
  text?: string;
  runId?: string;
  holdUntil?: Promise<void>;
}

class FakeAgent extends Agent<any, any, any> {
  runs: RunSpec[] = [];
  resumeCalls: unknown[] = [];

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
    });
  }

  async stream(_messages: any, options?: any): Promise<any> {
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
    const out = this.buildOutput(spec, options?.runId);
    this._internalRegisterStreamRun(out, (options ?? {}) as any);
    return out;
  }
}

function newHarness(agent: FakeAgent, storage: InMemoryHarness) {
  return new Harness({
    agents: { default: agent } as any,
    modes: [{ id: 'default', agentId: 'default' }],
    defaultModeId: 'default',
    sessions: { storage },
  });
}

function setup() {
  const agent = new FakeAgent();
  const storage = new InMemoryHarness({ db: new InMemoryDB() });
  return { agent, storage, harness: newHarness(agent, storage) };
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
    // The approved resume fails in-process; the approval is restored answerable.
    await expect(
      session.respondToToolApproval({ ...answerIdentity(pending), responseId: 'approve-1', approved: true }),
    ).rejects.toBeTruthy();
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
    ).rejects.toMatchObject({ code: 'harness.pending_interaction_discarded' });
    expect(agent.resumeCalls).toHaveLength(1);

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
});
