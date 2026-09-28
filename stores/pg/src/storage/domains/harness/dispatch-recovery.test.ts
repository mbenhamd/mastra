import { randomUUID } from 'node:crypto';

import { createSampleSessionRecord } from '@internal/storage-test-utils';
import {
  HarnessStorageLeaseConflictError,
  type AgentSignalResultEvidence,
  type HarnessStorage,
  type SessionRecord,
} from '@mastra/core/storage';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';

import { PostgresStore } from '../..';
import { TEST_CONFIG } from '../../test-utils';

vi.setConfig({ testTimeout: 60_000, hookTimeout: 60_000 });

const HARNESS = 'default';
const INTERRUPTED = { code: 'harness.run_interrupted', message: 'interrupted on adoption' };

async function createSession(harness: HarnessStorage, id: string, ownerId: string): Promise<SessionRecord> {
  const record = createSampleSessionRecord({
    id,
    harnessName: HARNESS,
    resourceId: `resource-${id}`,
    threadId: `thread-${id}`,
  });
  const result = await harness.createOrLoadActiveSession(record, { initialLease: { ownerId, ttlMs: 60_000 } });
  if (!result.created) throw new Error(`expected a fresh session for ${id}`);
  return (await harness.loadSession({ harnessName: HARNESS, sessionId: id }))!;
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

  it('discovers a lapsed session by the database clock and settles its orphaned message as interrupted', async () => {
    const live = await createSession(harness(), 'live-owner', 'owner-live');
    const orphan = await createSession(harness(), 'orphaned', 'owner-dead');
    await harness().writeMessageResultEvidence(pendingMessage(live, 'live'));
    const orphanEvidence = pendingMessage(orphan, 'orphan');
    await harness().writeMessageResultEvidence(orphanEvidence);
    await harness().releaseSessionLease({ harnessName: HARNESS, sessionId: orphan.id, ownerId: 'owner-dead' });
    // Its only pending row belongs to a run that already reached a terminal:
    // adoption skips it, so discovery must not return it at all.
    const summarized = await createSession(harness(), 'summarized', 'owner-done');
    const summarizedEvidence = pendingMessage(summarized, 'summarized');
    await harness().writeMessageResultEvidence(summarizedEvidence);
    await harness().saveRunSummary({
      summary: {
        harnessName: HARNESS,
        runId: summarizedEvidence.runId!,
        sessionId: summarized.id,
        resourceId: summarized.resourceId,
        threadId: summarized.threadId,
        agentId: 'agent',
        modeId: 'default',
        modelId: 'model',
        status: 'completed',
        finishReason: 'complete',
        reconstructed: false,
        completedAt: Date.now(),
        usage: { promptTokens: 1, completionTokens: 1, totalTokens: 2 },
        createdAt: Date.now(),
      },
    });
    await harness().releaseSessionLease({ harnessName: HARNESS, sessionId: summarized.id, ownerId: 'owner-done' });

    // A caller whose wall clock runs ten minutes ahead must not see the live
    // lease as lapsed: discovery and the lease CAS read the database clock.
    const realNow = Date.now();
    vi.spyOn(Date, 'now').mockReturnValue(realNow + 10 * 60_000);
    await expect(harness().listRecoverableSessions({ harnessName: HARNESS, limit: 10 })).resolves.toEqual({
      items: [
        {
          harnessName: HARNESS,
          sessionId: orphan.id,
          resourceId: orphan.resourceId,
          threadId: orphan.threadId,
          pendingMessageAdmission: true,
          pendingQueue: false,
          closing: false,
        },
      ],
    });
    await expect(
      harness().acquireSessionLease({ harnessName: HARNESS, sessionId: live.id, ownerId: 'adopter', ttlMs: 60_000 }),
    ).rejects.toBeInstanceOf(HarnessStorageLeaseConflictError);
    await expect(
      harness().saveSession(
        { ...live, lastActivityAt: live.lastActivityAt + 1 },
        { harnessName: HARNESS, ownerId: 'adopter', ifVersion: live.version },
      ),
    ).rejects.toBeInstanceOf(HarnessStorageLeaseConflictError);
    vi.restoreAllMocks();

    const scope = {
      harnessName: HARNESS,
      sessionId: orphan.id,
      resourceId: orphan.resourceId,
      threadId: orphan.threadId,
    };
    const pending = await harness().listPendingMessageAdmissions({ ...scope, limit: 10 });
    expect(pending.items).toMatchObject([{ evidence: { signalId: orphanEvidence.signalId }, dispatchClaim: 'none' }]);

    const lease = await harness().acquireSessionLease({ ...scope, ownerId: 'adopter', ttlMs: 60_000 });
    expect(lease.expiresAt).toBeGreaterThan(realNow);
    const settled = await harness().compareAndSwapSignalTerminal({
      ...scope,
      signalId: orphanEvidence.signalId,
      admissionId: orphanEvidence.admissionId!,
      admissionHash: orphanEvidence.admissionHash!,
      operationKind: 'message',
      expected: { state: 'reserved' },
      terminal: { status: 'failed', signalId: orphanEvidence.signalId, error: INTERRUPTED },
      updatedAt: Date.now(),
    });
    expect(settled.applied).toBe(true);
    await expect(
      harness().loadMessageResultEvidence({ ...scope, signalId: orphanEvidence.signalId }),
    ).resolves.toMatchObject({
      status: 'failed',
      runId: orphanEvidence.runId,
      operationKind: 'message',
      error: INTERRUPTED,
    });
    await expect(harness().listRecoverableSessions({ harnessName: HARNESS, limit: 10 })).resolves.toEqual({
      items: [],
    });

    // An adoption pass over the summarized session leaves its row pending (it
    // is not relabelled) and the session is still not rediscovered.
    await harness().acquireSessionLease({
      harnessName: HARNESS,
      sessionId: summarized.id,
      ownerId: 'adopter',
      ttlMs: 60_000,
    });
    await harness().releaseSessionLease({ harnessName: HARNESS, sessionId: summarized.id, ownerId: 'adopter' });
    const rediscovered = await harness().listRecoverableSessions({ harnessName: HARNESS, limit: 10 });
    expect(rediscovered.items.map(item => item.sessionId)).not.toContain(summarized.id);
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

    const pending = await harness().listPendingMessageAdmissions({ ...scope, limit: 10 });
    expect(pending.items.map(item => [item.evidence.signalId, item.dispatchClaim])).toEqual([
      ['signal-expired', 'expired'],
      ['signal-live', 'live'],
    ]);

    const terminalResult = {
      status: 'aborted' as const,
      runId: expired.evidence.runId!,
      completedAt: Date.now(),
      error: INTERRUPTED,
    };
    const receipt = await harness().commitTerminalHandoff({
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
    });
    expect(receipt.status).toBe('committed');
    expect(receipt.intent?.terminalResult).toEqual(terminalResult);
    await expect(
      harness().loadMessageResultEvidence({ ...scope, signalId: expired.evidence.signalId }),
    ).resolves.toMatchObject({ status: 'failed', error: INTERRUPTED });
    const remaining = await harness().listPendingMessageAdmissions({ ...scope, limit: 10 });
    expect(remaining.items.map(item => [item.evidence.signalId, item.dispatchClaim])).toEqual([
      ['signal-live', 'live'],
    ]);
  });
});
