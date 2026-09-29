import { randomUUID } from 'node:crypto';

import { createSampleSessionRecord } from '@internal/storage-test-utils';
import { HarnessStorageLeaseConflictError, TABLE_HARNESS_MESSAGE_RESULTS } from '@mastra/core/storage';
import type {
  AgentSignalDispatchState,
  AgentSignalResultEvidence,
  HarnessRunSummary,
  HarnessStorage,
  SessionRecord,
} from '@mastra/core/storage';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';

import { PostgresStore } from '../..';
import { TEST_CONFIG } from '../../test-utils';

vi.setConfig({ testTimeout: 60_000, hookTimeout: 60_000 });

const HARNESS = 'default';
const INTERRUPTED = { code: 'harness.run_interrupted', message: 'interrupted on adoption' };

async function createSession(
  harness: HarnessStorage,
  id: string,
  ownerId: string,
  overrides: Partial<SessionRecord> = {},
): Promise<SessionRecord> {
  const record = createSampleSessionRecord({
    id,
    harnessName: HARNESS,
    resourceId: `resource-${id}`,
    threadId: `thread-${id}`,
    ...overrides,
  });
  const result = await harness.createOrLoadActiveSession(record, { initialLease: { ownerId, ttlMs: 60_000 } });
  if (!result.created) throw new Error(`expected a fresh session for ${id}`);
  return (await harness.loadSession({ harnessName: HARNESS, sessionId: id }))!;
}

function runSummary(session: SessionRecord, runId: string, status: HarnessRunSummary['status']): HarnessRunSummary {
  return {
    harnessName: HARNESS,
    runId,
    sessionId: session.id,
    resourceId: session.resourceId,
    threadId: session.threadId,
    agentId: 'agent',
    modeId: 'default',
    modelId: 'model',
    status,
    finishReason: status === 'failed' ? 'error' : 'aborted',
    reconstructed: true,
    completedAt: Date.now(),
    usage: { promptTokens: 0, completionTokens: 0, totalTokens: 0 },
    createdAt: Date.now(),
  };
}

function pendingMessage(session: SessionRecord, tag: string): AgentSignalResultEvidence {
  return {
    status: 'pending',
    signalId: `signal-${tag}`,
    runId: `run-${tag}`,
    operationKind: 'message',
    admissionId: `admission-${tag}`,
    admissionHash: `admission-hash-${tag}`,
    harnessName: HARNESS,
    sessionId: session.id,
    resourceId: session.resourceId,
    threadId: session.threadId,
    createdAt: Date.now(),
    updatedAt: Date.now(),
  };
}

describe('HarnessPG orphaned-dispatch recovery', () => {
  const schemaName = `pf4598_recovery_${randomUUID().replaceAll('-', '_')}`;
  const store = new PostgresStore({
    ...TEST_CONFIG,
    id: 'pg-harness-dispatch-recovery-store',
    schemaName,
    enabledDomains: ['harness'],
    sessionRecordProjection: { enabled: true },
    terminalHandoff: { enabled: true },
  });
  const harness = () => store.stores.harness!;

  beforeAll(async () => {
    await store.init();
  });

  beforeEach(async () => {
    await harness().dangerouslyClearAll();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  afterAll(async () => {
    await store.db.none(`DROP SCHEMA IF EXISTS "${schemaName}" CASCADE`).catch(() => {});
    await store.close();
  });

  it('discovers lapsed sessions by the caller clock and settles and publishes orphaned messages', async () => {
    const live = await createSession(harness(), 'live-owner', 'owner-live');
    const orphan = await createSession(harness(), 'orphaned', 'owner-dead');
    await harness().writeMessageResultEvidence(pendingMessage(live, 'live'));
    const orphanEvidence = pendingMessage(orphan, 'orphan');
    await harness().writeMessageResultEvidence(orphanEvidence);
    await harness().releaseSessionLease({ harnessName: HARNESS, sessionId: orphan.id, ownerId: 'owner-dead' });
    // A run summary does not prove the admission settled: still recoverable.
    const summarized = await createSession(harness(), 'summarized', 'owner-done');
    const summarizedEvidence = pendingMessage(summarized, 'summarized');
    await harness().writeMessageResultEvidence(summarizedEvidence);
    await harness().saveRunSummary({ summary: runSummary(summarized, summarizedEvidence.runId!, 'failed') });
    await harness().releaseSessionLease({ harnessName: HARNESS, sessionId: summarized.id, ownerId: 'owner-done' });
    // A queue parked behind an unexpired approval wait cannot be advanced.
    const blocked = await createSession(harness(), 'blocked-queue', 'owner-blocked', {
      pendingQueue: [{ id: 'queued-1', enqueuedAt: Date.now(), content: 'later', attachments: [] }],
      pendingResume: {
        kind: 'tool-approval',
        itemId: 'approval-1',
        runId: 'run-blocked',
        toolCallId: 'tool-call-1',
        source: 'parent',
        requestedAt: Date.now(),
        expiresAt: Date.now() + 60 * 60_000,
      },
    });
    // Its parked turn is waiting for a response: never listed for adoption,
    // but visible to close through `includeParkedResume`.
    await harness().writeMessageResultEvidence(pendingMessage(blocked, 'blocked'));
    await harness().releaseSessionLease({ harnessName: HARNESS, sessionId: blocked.id, ownerId: 'owner-blocked' });
    const blockedScope = {
      harnessName: HARNESS,
      sessionId: blocked.id,
      resourceId: blocked.resourceId,
      threadId: blocked.threadId,
    };
    await expect(
      harness().listPendingMessageAdmissions({ ...blockedScope, now: Date.now(), limit: 10 }),
    ).resolves.toEqual({ items: [] });
    await expect(
      harness().listPendingMessageAdmissions({
        ...blockedScope,
        now: Date.now(),
        limit: 10,
        includeParkedResume: true,
      }),
    ).resolves.toMatchObject({ items: [{ evidence: { signalId: 'signal-blocked', status: 'pending' } }] });
    // A closing session whose only turn is still claimed by another process
    // cannot be closed yet, so it is not discoverable until the claim expires.
    // Its queued work must not list it (as not closing) either.
    const closingClaimed = await createSession(harness(), 'closing-claimed', 'owner-closing', {
      closingAt: Date.now(),
      closeDeadlineAt: Date.now() + 60_000,
      pendingQueue: [{ id: 'queued-closing', enqueuedAt: Date.now(), content: 'later', attachments: [] }],
    });
    const claimedEvidence = pendingMessage(closingClaimed, 'closing-claimed');
    await harness().writeMessageResultEvidence(claimedEvidence);
    await harness().compareAndSwapSignalDispatch({
      harnessName: HARNESS,
      sessionId: closingClaimed.id,
      resourceId: closingClaimed.resourceId,
      threadId: closingClaimed.threadId,
      signalId: claimedEvidence.signalId,
      admissionId: claimedEvidence.admissionId!,
      admissionHash: claimedEvidence.admissionHash!,
      operationKind: 'message',
      expected: { state: 'reserved' },
      next: {
        state: 'dispatching',
        attemptId: 'attempt-closing',
        claimExpiresAt: Date.now() + 60_000,
        delivery: 'idle',
        runId: claimedEvidence.runId!,
      },
      updatedAt: Date.now(),
    });
    await harness().releaseSessionLease({
      harnessName: HARNESS,
      sessionId: closingClaimed.id,
      ownerId: 'owner-closing',
    });
    // Nor is a closing session whose turn is parked for a response that has
    // not expired: its close is refused until then.
    const closingParked = await createSession(harness(), 'closing-parked', 'owner-parked', {
      closingAt: Date.now(),
      closeDeadlineAt: Date.now() + 60_000,
      pendingResume: {
        kind: 'tool-approval',
        itemId: 'approval-parked',
        runId: 'run-closing-parked',
        toolCallId: 'tool-call-parked',
        source: 'parent',
        requestedAt: Date.now(),
        expiresAt: Date.now() + 60 * 60_000,
      },
    });
    await harness().writeMessageResultEvidence(pendingMessage(closingParked, 'closing-parked'));
    await harness().releaseSessionLease({
      harnessName: HARNESS,
      sessionId: closingParked.id,
      ownerId: 'owner-parked',
    });

    const realNow = Date.now();
    const listed = (now: number) =>
      harness()
        .listRecoverableSessions({ harnessName: HARNESS, now, limit: 10 })
        .then(page => page.items.map(item => item.sessionId));
    await expect(listed(realNow)).resolves.toEqual([orphan.id, summarized.id]);
    // Leases and claims are judged by the caller's clock, like `_flushUpdate`.
    await expect(listed(realNow + 10 * 60_000)).resolves.toEqual([
      closingClaimed.id,
      live.id,
      orphan.id,
      summarized.id,
    ]);
    // Once the parked interactions are due, close (or adoption) can expire
    // them: the closing session and the parked queue are listed.
    await expect(listed(realNow + 61 * 60_000)).resolves.toEqual([
      blocked.id,
      closingClaimed.id,
      closingParked.id,
      live.id,
      orphan.id,
      summarized.id,
    ]);

    // Lease expiries are stamped on the caller's clock too.
    vi.spyOn(Date, 'now').mockReturnValue(realNow + 10 * 60_000);
    const scope = {
      harnessName: HARNESS,
      sessionId: orphan.id,
      resourceId: orphan.resourceId,
      threadId: orphan.threadId,
    };
    const lease = await harness().acquireSessionLease({ ...scope, ownerId: 'adopter', ttlMs: 60_000 });
    expect(lease.expiresAt).toBe(realNow + 10 * 60_000 + 60_000);
    vi.restoreAllMocks();

    const pending = await harness().listPendingMessageAdmissions({ ...scope, now: Date.now(), limit: 10 });
    expect(pending.items).toMatchObject([{ evidence: { signalId: orphanEvidence.signalId }, dispatchClaim: 'none' }]);
    const settle = (ownerId: string) =>
      harness().compareAndSwapSignalTerminal({
        ...scope,
        signalId: orphanEvidence.signalId,
        admissionId: orphanEvidence.admissionId!,
        admissionHash: orphanEvidence.admissionHash!,
        operationKind: 'message',
        expected: { state: 'reserved' },
        terminal: { status: 'failed', signalId: orphanEvidence.signalId, error: INTERRUPTED },
        leaseOwner: { ownerId },
        updatedAt: Date.now(),
      });
    // Settlement is fenced by the recovering owner's lease, judged right
    // before the write — after any lock wait, not before it.
    await expect(settle('not-the-owner')).resolves.toMatchObject({ applied: false });
    const locker = await (
      store.db as unknown as {
        connect(): Promise<{ query(sql: string, values?: unknown[]): Promise<unknown>; release(): void }>;
      }
    ).connect();
    try {
      await locker.query('BEGIN');
      await locker.query(
        `SELECT 1 FROM "${schemaName}"."${TABLE_HARNESS_MESSAGE_RESULTS}" WHERE signal_id = $1 FOR UPDATE`,
        [orphanEvidence.signalId],
      );
      const waiting = settle('adopter');
      await new Promise(resolve => setTimeout(resolve, 250));
      vi.spyOn(Date, 'now').mockReturnValue(lease.expiresAt + 1);
      await locker.query('ROLLBACK');
      await expect(waiting).resolves.toMatchObject({ applied: false });
    } finally {
      vi.restoreAllMocks();
      locker.release();
    }
    await expect(settle('adopter')).resolves.toMatchObject({ applied: true });
    await expect(
      harness().loadMessageResultEvidence({ ...scope, signalId: orphanEvidence.signalId }),
    ).resolves.toMatchObject({
      status: 'failed',
      runId: orphanEvidence.runId,
      operationKind: 'message',
      error: INTERRUPTED,
    });

    // The settled row stays recoverable until its completion is published.
    const unpublished = await harness().listPendingMessageAdmissions({ ...scope, now: Date.now(), limit: 10 });
    expect(unpublished.items).toMatchObject([{ evidence: { signalId: orphanEvidence.signalId, status: 'failed' } }]);
    await harness().releaseSessionLease({ ...scope, ownerId: 'adopter' });
    await expect(listed(Date.now())).resolves.toContain(orphan.id);
    await harness().saveRunSummary({ summary: runSummary(orphan, orphanEvidence.runId!, 'interrupted') });
    await expect(harness().listPendingMessageAdmissions({ ...scope, now: Date.now(), limit: 10 })).resolves.toEqual({
      items: [],
    });
    await expect(listed(Date.now())).resolves.toEqual([summarized.id]);
  });

  it('commits an aborted terminal intent over failed evidence and leaves a live claim untouched', async () => {
    const session = await createSession(harness(), 'terminal', 'owner-dead');
    const scope = {
      harnessName: HARNESS,
      sessionId: session.id,
      resourceId: session.resourceId,
      threadId: session.threadId,
    };
    const stamp = async (tag: string, claimExpiresAt: number) => {
      const evidence = pendingMessage(session, tag);
      const admission = {
        ...scope,
        sessionIncarnation: session.sessionIncarnation!,
        admissionId: evidence.admissionId!,
        admissionHash: evidence.admissionHash!,
        signalId: evidence.signalId,
        runId: evidence.runId!,
        executionGrant: { key: `grant-${tag}`, generation: 1 },
        finalizerId: 'doxa.chat',
        finalizerVersion: '1',
        seed: { tag },
      };
      await harness().writeMessageResultEvidence(evidence);
      await harness().admitTerminalHandoff(admission);
      const dispatch = {
        state: 'dispatching' as const,
        attemptId: `attempt-${tag}`,
        claimExpiresAt,
        delivery: 'idle' as const,
        runId: evidence.runId!,
      };
      await harness().compareAndSwapSignalDispatch({
        ...scope,
        signalId: evidence.signalId,
        admissionId: evidence.admissionId!,
        admissionHash: evidence.admissionHash!,
        operationKind: 'message',
        expected: { state: 'reserved' },
        next: dispatch,
        updatedAt: Date.now(),
      });
      return { evidence, admission, dispatch };
    };
    const expired = await stamp('expired', Date.now() - 1_000);
    const live = await stamp('live', Date.now() + 60_000);
    // Recovery's fallback settlement never lands over a pending terminal
    // admission, even when the dispatch it expects still matches — nor from
    // a store sharing the schema that does not admit terminal handoffs.
    const plainStore = new PostgresStore({
      ...TEST_CONFIG,
      id: 'pg-harness-dispatch-recovery-plain-store',
      schemaName,
      enabledDomains: ['harness'],
    });
    try {
      await plainStore.init();
      for (const candidate of [harness(), plainStore.stores.harness!]) {
        await expect(
          candidate.compareAndSwapSignalTerminal({
            ...scope,
            signalId: live.evidence.signalId,
            admissionId: live.evidence.admissionId!,
            admissionHash: live.evidence.admissionHash!,
            operationKind: 'message',
            expected: live.dispatch,
            leaseOwner: { ownerId: 'owner-dead' },
            terminal: { status: 'failed', signalId: live.evidence.signalId, error: INTERRUPTED },
            updatedAt: Date.now(),
          }),
        ).resolves.toMatchObject({ applied: false });
      }
    } finally {
      await plainStore.close();
    }

    const claims = (now: number) =>
      harness()
        .listPendingMessageAdmissions({ ...scope, now, limit: 10 })
        .then(page => page.items.map(item => [item.evidence.signalId, item.dispatchClaim]));
    await expect(claims(Date.now())).resolves.toEqual([
      ['signal-expired', 'expired'],
      ['signal-live', 'live'],
    ]);
    // Claims are stamped by `Session.message()` on the caller clock; the
    // caller's `now` decides their expiry.
    await expect(claims(Date.now() + 120_000)).resolves.toEqual([
      ['signal-expired', 'expired'],
      ['signal-live', 'expired'],
    ]);

    const terminalResult = {
      status: 'aborted' as const,
      runId: expired.evidence.runId!,
      completedAt: Date.now(),
      error: INTERRUPTED,
    };
    const commitInterrupted = (expectedDispatch: AgentSignalDispatchState | null) =>
      harness().commitTerminalHandoff({
        admission: expired.admission,
        resultEvidence: {
          ...expired.evidence,
          status: 'failed',
          error: INTERRUPTED,
          dispatch: expired.dispatch,
          updatedAt: Date.now(),
        },
        terminalResult,
        projection: { projectionKind: 'chat.summary', projectionId: 'interrupted', payload: { status: 'aborted' } },
        recovery: { expectedDispatch, leaseOwner: { ownerId: 'owner-dead' } },
      });
    // A dispatch that changed since recovery observed it is never overwritten.
    await expect(commitInterrupted(null)).resolves.toMatchObject({ status: 'conflict' });
    await expect(
      harness().loadMessageResultEvidence({ ...scope, signalId: expired.evidence.signalId }),
    ).resolves.toMatchObject({ status: 'pending', dispatch: expired.dispatch });
    const receipt = await commitInterrupted(expired.dispatch);
    expect(receipt.status).toBe('committed');
    expect(receipt.intent?.terminalResult).toEqual(terminalResult);
    await expect(
      harness().loadMessageResultEvidence({ ...scope, signalId: expired.evidence.signalId }),
    ).resolves.toMatchObject({ status: 'failed', error: INTERRUPTED });
    // A stalled owner cannot admit a terminal handoff for a turn recovery
    // already settled, nor without holding the session lease.
    const lateEvidence = pendingMessage(session, 'late');
    await harness().writeMessageResultEvidence(lateEvidence);
    const lateAdmission = {
      ...expired.admission,
      admissionId: lateEvidence.admissionId!,
      admissionHash: lateEvidence.admissionHash!,
      signalId: lateEvidence.signalId,
      runId: lateEvidence.runId!,
      executionGrant: { key: 'grant-late', generation: 1 },
    };
    await expect(
      harness().admitTerminalHandoff(lateAdmission, { leaseOwner: { ownerId: 'not-the-owner' } }),
    ).resolves.toMatchObject({ status: 'fenced' });
    await harness().compareAndSwapSignalTerminal({
      ...scope,
      signalId: lateEvidence.signalId,
      admissionId: lateEvidence.admissionId!,
      admissionHash: lateEvidence.admissionHash!,
      operationKind: 'message',
      expected: { state: 'reserved' },
      terminal: { status: 'failed', signalId: lateEvidence.signalId, error: INTERRUPTED },
      leaseOwner: { ownerId: 'owner-dead' },
      updatedAt: Date.now(),
    });
    await expect(
      harness().admitTerminalHandoff(lateAdmission, { leaseOwner: { ownerId: 'owner-dead' } }),
    ).resolves.toMatchObject({ status: 'fenced' });
    await expect(
      harness().loadTerminalAdmission({
        harnessName: HARNESS,
        sessionId: session.id,
        admissionId: lateAdmission.admissionId,
        executionGrant: lateAdmission.executionGrant,
      }),
    ).resolves.toBeNull();

    // A reservation lands only under the lease of the open session.
    const reserve = (evidence: AgentSignalResultEvidence, ownerId: string) =>
      harness().writeMessageResultEvidence(evidence, { leaseOwner: { ownerId } });
    const fenced = pendingMessage(session, 'fenced');
    await expect(reserve(fenced, 'not-the-owner')).rejects.toBeInstanceOf(HarnessStorageLeaseConflictError);
    await expect(harness().loadMessageResultEvidence({ ...scope, signalId: fenced.signalId })).resolves.toBeNull();
    await expect(reserve(pendingMessage(session, 'reserved'), 'owner-dead')).resolves.toMatchObject({ created: true });
    const closed = await createSession(harness(), 'closed', 'owner-closed');
    await harness().saveSession(
      { ...closed, closedAt: Date.now() },
      { harnessName: HARNESS, ownerId: 'owner-closed', ifVersion: closed.version },
    );
    await expect(reserve(pendingMessage(closed, 'closed'), 'owner-closed')).rejects.toBeInstanceOf(
      HarnessStorageLeaseConflictError,
    );

    const remaining = await harness().listPendingMessageAdmissions({ ...scope, now: Date.now(), limit: 10 });
    expect(remaining.items.map(item => [item.evidence.signalId, item.evidence.status, item.dispatchClaim])).toEqual([
      ['signal-expired', 'failed', 'none'],
      ['signal-late', 'failed', 'none'],
      ['signal-live', 'pending', 'live'],
      ['signal-reserved', 'pending', 'none'],
    ]);
  });

  it('pages both recovery scans by cursor and rejects invalid scan input', async () => {
    const sessions: SessionRecord[] = [];
    for (const tag of ['page-a', 'page-b', 'page-c']) {
      const session = await createSession(harness(), tag, `owner-${tag}`);
      await harness().writeMessageResultEvidence(pendingMessage(session, tag));
      await harness().releaseSessionLease({ harnessName: HARNESS, sessionId: session.id, ownerId: `owner-${tag}` });
      sessions.push(session);
    }
    const now = Date.now();
    const first = await harness().listRecoverableSessions({ harnessName: HARNESS, now, limit: 2 });
    expect(first.items.map(item => item.sessionId)).toEqual(['page-a', 'page-b']);
    expect(first.nextCursor).toEqual({ sessionId: 'page-b' });
    const last = await harness().listRecoverableSessions({
      harnessName: HARNESS,
      now,
      limit: 2,
      cursor: first.nextCursor,
    });
    expect(last).toEqual({ items: [expect.objectContaining({ sessionId: 'page-c' })] });

    const owner = sessions[0]!;
    const scope = { harnessName: HARNESS, sessionId: owner.id, resourceId: owner.resourceId, threadId: owner.threadId };
    for (const tag of ['page-a-2', 'page-a-3']) await harness().writeMessageResultEvidence(pendingMessage(owner, tag));
    const rows = await harness().listPendingMessageAdmissions({ ...scope, now, limit: 2 });
    expect(rows.items.map(item => item.evidence.signalId)).toEqual(['signal-page-a', 'signal-page-a-2']);
    expect(rows.nextCursor).toEqual({ signalId: 'signal-page-a-2' });
    const rest = await harness().listPendingMessageAdmissions({ ...scope, now, limit: 2, cursor: rows.nextCursor });
    expect(rest.items.map(item => item.evidence.signalId)).toEqual(['signal-page-a-3']);
    expect(rest.nextCursor).toBeUndefined();

    await expect(harness().listRecoverableSessions({ harnessName: HARNESS, now, limit: 0 })).rejects.toBeInstanceOf(
      RangeError,
    );
    await expect(harness().listPendingMessageAdmissions({ ...scope, now: -1, limit: 2 })).rejects.toBeInstanceOf(
      RangeError,
    );
  });
});
