import { randomUUID } from 'node:crypto';

import { createSampleSessionRecord } from '@internal/storage-test-utils';
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
    await harness().releaseSessionLease({ harnessName: HARNESS, sessionId: blocked.id, ownerId: 'owner-blocked' });

    const realNow = Date.now();
    const listed = (now: number) =>
      harness()
        .listRecoverableSessions({ harnessName: HARNESS, now, limit: 10 })
        .then(page => page.items.map(item => item.sessionId));
    await expect(listed(realNow)).resolves.toEqual([orphan.id, summarized.id]);
    // Leases and claims are judged by the caller's clock, like `_flushUpdate`.
    await expect(listed(realNow + 10 * 60_000)).resolves.toEqual([live.id, orphan.id, summarized.id]);

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
        leaseOwner: { ownerId, now: Date.now() },
        updatedAt: Date.now(),
      });
    // Settlement is fenced by the recovering owner's lease.
    await expect(settle('not-the-owner')).resolves.toMatchObject({ applied: false });
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
    await stamp('live', Date.now() + 60_000);

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
        recovery: { expectedDispatch, leaseOwner: { ownerId: 'owner-dead', now: Date.now() } },
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
    const remaining = await harness().listPendingMessageAdmissions({ ...scope, now: Date.now(), limit: 10 });
    expect(remaining.items.map(item => [item.evidence.signalId, item.evidence.status, item.dispatchClaim])).toEqual([
      ['signal-expired', 'failed', 'none'],
      ['signal-live', 'pending', 'live'],
    ]);
  });
});
