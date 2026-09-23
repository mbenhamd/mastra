import { describe, expect, it } from 'vitest';

import { projectHarnessPublicError } from '../../../harness/v1/events';
import { InMemoryDB } from '../inmemory-db';
import {
  HarnessTerminalHandoffClaimConflictError,
  HarnessTerminalHandoffFencedError,
  HarnessTerminalHandoffIdentityConflictError,
  HarnessTerminalHandoffUnsupportedError,
  HarnessTerminalHandoffValidationError,
  HarnessTerminalFinalizationPendingError,
  InMemoryHarness,
  harnessTerminalAdmissionId,
  harnessTerminalIntentId,
  type AgentSignalResultEvidence,
  type HarnessTerminalAdmissionInput,
  type HarnessTerminalIntent,
  type SessionRecord,
} from './index';

function session(overrides: Partial<SessionRecord> = {}): SessionRecord {
  return {
    harnessName: 'default',
    id: 'session-1',
    resourceId: 'resource-1',
    threadId: 'thread-1',
    sessionIncarnation: 'incarnation-1',
    origin: 'top-level',
    ownsThread: false,
    modeId: 'build',
    modelId: 'model-1',
    subagentModelOverrides: {},
    permissionRules: { categories: {}, tools: {} },
    sessionGrants: { categories: [], tools: [] },
    tokenUsage: { promptTokens: 0, completionTokens: 0, totalTokens: 0 },
    pendingQueue: [],
    state: {},
    createdAt: 1_000,
    lastActivityAt: 1_000,
    version: 0,
    ...overrides,
  };
}

function admission(): HarnessTerminalAdmissionInput {
  return {
    harnessName: 'default',
    sessionId: 'session-1',
    resourceId: 'resource-1',
    threadId: 'thread-1',
    sessionIncarnation: 'incarnation-1',
    admissionId: 'admission-1',
    admissionHash: 'admission-hash-1',
    signalId: 'signal-1',
    runId: 'run-1',
    executionGrant: { key: 'grant-1', generation: 1 },
    finalizerId: 'doxa.chat',
    finalizerVersion: '1',
    seed: { admissionId: 'admission-1', mode: 'build' },
    createdAt: 2_000,
  };
}

function pendingEvidence(input: HarnessTerminalAdmissionInput): AgentSignalResultEvidence {
  return {
    status: 'pending',
    signalId: input.signalId,
    runId: input.runId,
    operationKind: 'message',
    admissionId: input.admissionId,
    admissionHash: input.admissionHash,
    harnessName: input.harnessName,
    sessionId: input.sessionId,
    resourceId: input.resourceId,
    threadId: input.threadId,
    createdAt: 2_000,
    updatedAt: 2_000,
  };
}

describe('native chat terminal handoff', () => {
  it('commits canonical evidence and the exact delivery intent atomically and replays it', async () => {
    const storage = new InMemoryHarness({ db: new InMemoryDB(), terminalHandoff: { enabled: true } });
    await storage.saveSession(session(), { ownerId: 'owner-1', ifVersion: 0 });
    const input = admission();
    await storage.writeMessageResultEvidence(pendingEvidence(input));

    const admitted = await storage.admitTerminalHandoff(input);
    expect(admitted.status).toBe('created');
    const projection = { projectionKind: 'chat.summary', projectionId: 'summary-1', payload: { text: 'done' } };
    const terminalResult = { status: 'completed' as const, runId: input.runId, completedAt: 3_000 };
    const resultEvidence: AgentSignalResultEvidence = {
      ...pendingEvidence(input),
      status: 'completed',
      result: { text: 'provider output' },
      updatedAt: 3_000,
    };

    const committed = await storage.commitTerminalHandoff({
      admission: input,
      resultEvidence,
      terminalResult,
      projection,
    });
    expect(committed.status).toBe('committed');
    expect(committed.intent).toMatchObject({
      id: harnessTerminalIntentId(harnessTerminalAdmissionId(input)),
      projection: expect.objectContaining({ payloadHash: expect.any(String), payloadJson: '{"text":"done"}' }),
    });
    await expect(
      storage.loadMessageResultEvidence({
        harnessName: input.harnessName,
        sessionId: input.sessionId,
        resourceId: input.resourceId,
        threadId: input.threadId,
        signalId: input.signalId,
      }),
    ).resolves.toMatchObject({ status: 'completed', result: { text: 'provider output' } });

    const replay = await storage.commitTerminalHandoff({
      admission: input,
      resultEvidence,
      terminalResult,
      projection,
    });
    expect(replay.status).toBe('duplicate');
    expect(replay.intent?.id).toBe(harnessTerminalIntentId(harnessTerminalAdmissionId(input)));
    expect(committed.admission.id).toBe(harnessTerminalAdmissionId(input));

    await expect(storage.getTerminalQueuePressure({ harnessName: input.harnessName })).resolves.toEqual({
      pendingIntents: 1,
      pendingBytes: committed.intent!.projection.payloadBytes,
    });
    const claim = await storage.claimTerminalIntents({
      harnessName: input.harnessName,
      consumerId: 'delivery-worker-1',
      limit: 1,
      now: 4_000,
    });
    expect(claim.intents).toHaveLength(1);
    await storage.ackTerminalIntent({
      harnessName: input.harnessName,
      intentId: claim.intents[0]!.id,
      sessionId: input.sessionId,
      sessionIncarnation: input.sessionIncarnation,
      revision: claim.intents[0]!.revision,
      payloadHash: claim.intents[0]!.projection.payloadHash,
      claimId: claim.intents[0]!.claimId!,
      consumerId: 'delivery-worker-1',
      now: 4_001,
    });
    await expect(storage.getTerminalQueuePressure({ harnessName: input.harnessName })).resolves.toEqual({
      pendingIntents: 0,
      pendingBytes: 0,
    });
  });

  it('retains a no-row cancellation fence and rejects a stale session-incarnation callback', async () => {
    const storage = new InMemoryHarness({ db: new InMemoryDB(), terminalHandoff: { enabled: true } });
    await storage.saveSession(session(), { ownerId: 'owner-1', ifVersion: 0 });
    const input = admission();
    await storage.cancelTerminalHandoff({
      harnessName: input.harnessName,
      sessionId: input.sessionId,
      sessionIncarnation: input.sessionIncarnation,
      admissionId: input.admissionId,
      admissionHash: input.admissionHash,
      executionGrant: input.executionGrant,
      reason: { code: 'cancelled', message: 'user cancelled' },
      cancelledAt: 2_100,
    });
    await expect(storage.admitTerminalHandoff(input)).resolves.toMatchObject({ status: 'cancelled' });

    const staleInput = {
      ...input,
      admissionId: 'admission-2',
      admissionHash: 'admission-hash-2',
      signalId: 'signal-2',
      runId: 'run-2',
      executionGrant: { key: 'grant-2', generation: 1 },
    };
    await storage.writeMessageResultEvidence(pendingEvidence(staleInput));
    await storage.admitTerminalHandoff(staleInput);
    await storage.deleteSession({
      harnessName: 'default',
      sessionId: 'session-1',
      ifVersion: 1,
      expectedResourceId: 'resource-1',
      expectedThreadId: 'thread-1',
      expectedParentSessionId: null,
      expectedCreatedAt: 1_000,
    });
    await storage.createOrLoadActiveSession(session({ sessionIncarnation: 'incarnation-2' }), {
      initialLease: { ownerId: 'owner-2', ttlMs: 30_000 },
    });
    await expect(
      storage.commitTerminalHandoff({
        admission: staleInput,
        resultEvidence: {
          ...pendingEvidence(staleInput),
          status: 'completed',
          result: { text: 'stale' },
          updatedAt: 3_000,
        },
        terminalResult: { status: 'completed', runId: staleInput.runId, completedAt: 3_000 },
        projection: { projectionKind: 'chat.summary', projectionId: 'summary-1', payload: { text: 'stale' } },
      }),
    ).rejects.toBeInstanceOf(HarnessTerminalHandoffFencedError);
  });

  it('lets a committed winner survive a late cancel and a pending cancel fence a late commit', async () => {
    const storage = new InMemoryHarness({ db: new InMemoryDB(), terminalHandoff: { enabled: true } });
    await storage.saveSession(session(), { ownerId: 'owner-1', ifVersion: 0 });
    const winner = admission();
    await storage.writeMessageResultEvidence(pendingEvidence(winner));
    await storage.admitTerminalHandoff(winner);
    await storage.commitTerminalHandoff({
      admission: winner,
      resultEvidence: { ...pendingEvidence(winner), status: 'completed', result: { text: 'done' }, updatedAt: 3_000 },
      terminalResult: { status: 'completed', runId: winner.runId, completedAt: 3_000 },
      projection: { projectionKind: 'chat.summary', projectionId: 'summary-1', payload: { text: 'done' } },
    });
    await expect(
      storage.cancelTerminalHandoff({
        harnessName: winner.harnessName,
        sessionId: winner.sessionId,
        sessionIncarnation: winner.sessionIncarnation,
        admissionId: winner.admissionId,
        admissionHash: winner.admissionHash,
        executionGrant: winner.executionGrant,
        reason: { code: 'cancelled', message: 'too late' },
      }),
    ).resolves.toMatchObject({ status: 'committed' });

    const loser = {
      ...admission(),
      admissionId: 'admission-2',
      admissionHash: 'admission-hash-2',
      signalId: 'signal-2',
      runId: 'run-2',
      executionGrant: { key: 'grant-2', generation: 1 },
    };
    await storage.writeMessageResultEvidence(pendingEvidence(loser));
    await storage.admitTerminalHandoff(loser);
    await storage.cancelTerminalHandoff({
      harnessName: loser.harnessName,
      sessionId: loser.sessionId,
      sessionIncarnation: loser.sessionIncarnation,
      admissionId: loser.admissionId,
      admissionHash: loser.admissionHash,
      executionGrant: loser.executionGrant,
      reason: { code: 'cancelled', message: 'cancelled first' },
    });
    await expect(
      storage.commitTerminalHandoff({
        admission: loser,
        resultEvidence: { ...pendingEvidence(loser), status: 'completed', result: { text: 'x' }, updatedAt: 3_000 },
        terminalResult: { status: 'completed', runId: loser.runId, completedAt: 3_000 },
        projection: { projectionKind: 'chat.summary', projectionId: 'summary-2', payload: { text: 'x' } },
      }),
    ).resolves.toMatchObject({ status: 'cancelled' });
  });

  it('fences a grant tombstone across scopes and keeps only the live claim able to settle', async () => {
    const storage = new InMemoryHarness({ db: new InMemoryDB(), terminalHandoff: { enabled: true, maxAttempts: 2 } });
    await storage.saveSession(session(), { ownerId: 'owner-1', ifVersion: 0 });
    const other = session({
      id: 'session-2',
      resourceId: 'resource-2',
      threadId: 'thread-2',
      sessionIncarnation: 'incarnation-2',
    });
    await storage.saveSession(other, { ownerId: 'owner-2', ifVersion: 0 });
    const input = admission();
    await storage.cancelTerminalHandoff({
      harnessName: input.harnessName,
      sessionId: input.sessionId,
      sessionIncarnation: input.sessionIncarnation,
      admissionId: input.admissionId,
      admissionHash: input.admissionHash,
      executionGrant: input.executionGrant,
      reason: { code: 'cancelled', message: 'revoked' },
    });
    // The tombstone is grant-scoped: a replay under a different session is fenced.
    await expect(
      storage.admitTerminalHandoff({ ...input, sessionId: 'session-2', sessionIncarnation: 'incarnation-2' }),
    ).resolves.toMatchObject({ status: 'cancelled' });

    const live = { ...input, executionGrant: { key: 'grant-live', generation: 1 } };
    await storage.writeMessageResultEvidence(pendingEvidence(live));
    await storage.admitTerminalHandoff(live);
    await storage.commitTerminalHandoff({
      admission: live,
      resultEvidence: { ...pendingEvidence(live), status: 'completed', result: { text: 'done' }, updatedAt: 3_000 },
      terminalResult: { status: 'completed', runId: live.runId, completedAt: 3_000 },
      projection: { projectionKind: 'chat.summary', projectionId: 'summary-1', payload: { text: 'done' } },
    });
    const claimed = await storage.claimTerminalIntents({
      harnessName: live.harnessName,
      consumerId: 'worker-a',
      limit: 1,
      now: 4_000,
      leaseMs: 1_000,
    });
    const stale: HarnessTerminalIntent = claimed.intents[0]!;
    const reclaimed = (
      await storage.claimTerminalIntents({
        harnessName: live.harnessName,
        consumerId: 'worker-b',
        limit: 1,
        now: 5_500,
      })
    ).intents[0]!;
    await expect(
      storage.ackTerminalIntent({
        harnessName: live.harnessName,
        intentId: stale.id,
        sessionId: stale.sessionId,
        sessionIncarnation: stale.sessionIncarnation,
        revision: stale.revision,
        payloadHash: stale.projection.payloadHash,
        claimId: stale.claimId!,
        consumerId: 'worker-a',
        now: 6_000,
      }),
    ).rejects.toBeInstanceOf(HarnessTerminalHandoffClaimConflictError);
    await expect(
      storage.ackTerminalIntent({
        harnessName: live.harnessName,
        intentId: reclaimed.id,
        sessionId: reclaimed.sessionId,
        sessionIncarnation: reclaimed.sessionIncarnation,
        revision: reclaimed.revision,
        payloadHash: reclaimed.projection.payloadHash,
        claimId: reclaimed.claimId!,
        consumerId: 'worker-b',
        now: 6_000,
      }),
    ).resolves.toMatchObject({ status: 'acked' });
    await expect(storage.getTerminalQueuePressure({ harnessName: live.harnessName })).resolves.toEqual({
      pendingIntents: 0,
      pendingBytes: 0,
    });
  });

  it('retries a failed claim, dead-letters at maxAttempts, and releases queue pressure', async () => {
    const storage = new InMemoryHarness({
      db: new InMemoryDB(),
      terminalHandoff: { enabled: true, maxAttempts: 2 },
    });
    await storage.saveSession(session(), { ownerId: 'owner-1', ifVersion: 0 });
    const input = admission();
    await storage.writeMessageResultEvidence(pendingEvidence(input));
    await storage.admitTerminalHandoff(input);
    await storage.commitTerminalHandoff({
      admission: input,
      resultEvidence: { ...pendingEvidence(input), status: 'completed', result: { text: 'done' }, updatedAt: 3_000 },
      terminalResult: { status: 'completed', runId: input.runId, completedAt: 3_000 },
      projection: { projectionKind: 'chat.summary', projectionId: 'summary-1', payload: { text: 'done' } },
    });
    const committed = (
      await storage.claimTerminalIntents({
        harnessName: 'default',
        consumerId: 'worker-a',
        limit: 1,
        now: 4_000,
        leaseMs: 10_000,
      })
    ).intents[0]!;

    const failed = await storage.failTerminalIntent({
      harnessName: committed.harnessName,
      intentId: committed.id,
      sessionId: committed.sessionId,
      sessionIncarnation: committed.sessionIncarnation,
      revision: committed.revision,
      payloadHash: committed.projection.payloadHash,
      claimId: committed.claimId!,
      consumerId: 'worker-a',
      now: 5_000,
      error: { code: 'delivery_failed', message: 'sink unavailable' },
    });
    expect(failed.status).toBe('failed');
    expect(failed.intent.nextAttemptAt).toBeGreaterThan(5_000);
    expect(failed.intent.lastError).toMatchObject({ code: 'delivery_failed' });
    await expect(storage.getTerminalQueuePressure({ harnessName: 'default' })).resolves.toMatchObject({
      pendingIntents: 1,
    });

    const reclaimed = (
      await storage.claimTerminalIntents({
        harnessName: 'default',
        consumerId: 'worker-b',
        limit: 1,
        now: failed.intent.nextAttemptAt!,
        leaseMs: 10_000,
      })
    ).intents[0]!;
    const dead = await storage.failTerminalIntent({
      harnessName: reclaimed.harnessName,
      intentId: reclaimed.id,
      sessionId: reclaimed.sessionId,
      sessionIncarnation: reclaimed.sessionIncarnation,
      revision: reclaimed.revision,
      payloadHash: reclaimed.projection.payloadHash,
      claimId: reclaimed.claimId!,
      consumerId: 'worker-b',
      now: 7_000,
      error: { code: 'delivery_failed', message: 'still unavailable' },
    });
    expect(dead.status).toBe('dead');
    await expect(storage.getTerminalQueuePressure({ harnessName: 'default' })).resolves.toEqual({
      pendingIntents: 0,
      pendingBytes: 0,
    });
    await expect(
      storage.claimTerminalIntents({ harnessName: 'default', consumerId: 'worker-c', limit: 1, now: 8_000 }),
    ).resolves.toMatchObject({ intents: [] });
  });

  it('rejects a terminal commit when pending capacity is exhausted and lets it retry after drain', async () => {
    const storage = new InMemoryHarness({
      db: new InMemoryDB(),
      terminalHandoff: { enabled: true, maxPendingIntents: 1, maxPendingBytes: 1_000_000 },
    });
    await storage.saveSession(session(), { ownerId: 'owner-1', ifVersion: 0 });
    const first = admission();
    await storage.writeMessageResultEvidence(pendingEvidence(first));
    await storage.admitTerminalHandoff(first);
    await storage.commitTerminalHandoff({
      admission: first,
      resultEvidence: { ...pendingEvidence(first), status: 'completed', result: { text: 'one' }, updatedAt: 3_000 },
      terminalResult: { status: 'completed', runId: first.runId, completedAt: 3_000 },
      projection: { projectionKind: 'chat.summary', projectionId: 'summary-1', payload: { text: 'one' } },
    });

    const second = {
      ...admission(),
      admissionId: 'admission-2',
      admissionHash: 'admission-hash-2',
      signalId: 'signal-2',
      runId: 'run-2',
      executionGrant: { key: 'grant-2', generation: 1 },
    };
    await storage.writeMessageResultEvidence(pendingEvidence(second));
    await storage.admitTerminalHandoff(second);
    const secondCommit = {
      admission: second,
      resultEvidence: {
        ...pendingEvidence(second),
        status: 'completed' as const,
        result: { text: 'two' },
        updatedAt: 3_000,
      },
      terminalResult: { status: 'completed' as const, runId: second.runId, completedAt: 3_000 },
      projection: { projectionKind: 'chat.summary', projectionId: 'summary-2', payload: { text: 'two' } },
    };
    await expect(storage.commitTerminalHandoff(secondCommit)).rejects.toThrow('queue capacity is exhausted');

    const claimed = (
      await storage.claimTerminalIntents({ harnessName: 'default', consumerId: 'worker-a', limit: 1, now: 4_000 })
    ).intents[0]!;
    await storage.ackTerminalIntent({
      harnessName: claimed.harnessName,
      intentId: claimed.id,
      sessionId: claimed.sessionId,
      sessionIncarnation: claimed.sessionIncarnation,
      revision: claimed.revision,
      payloadHash: claimed.projection.payloadHash,
      claimId: claimed.claimId!,
      consumerId: 'worker-a',
      now: 5_000,
    });
    await expect(storage.commitTerminalHandoff(secondCommit)).resolves.toMatchObject({ status: 'committed' });
  });

  it('keeps finalization-pending typed at the public error projection', () => {
    const error = new HarnessTerminalFinalizationPendingError(4_000, new Error('provider detail'));
    expect(projectHarnessPublicError(error)).toMatchObject({
      code: 'harness.terminal_pending',
    });
  });

  it('mints and preserves a session incarnation when only terminal handoff is enabled', async () => {
    const storage = new InMemoryHarness({ db: new InMemoryDB(), terminalHandoff: { enabled: true } });
    await storage.saveSession(session({ sessionIncarnation: undefined }), { ownerId: 'owner-1', ifVersion: 0 });
    const stored = await storage.loadSession({ harnessName: 'default', sessionId: 'session-1' });
    expect(stored?.sessionIncarnation).toEqual(expect.any(String));

    // An update that omits the incarnation preserves the minted fence.
    await storage.saveSession(session({ sessionIncarnation: undefined }), { ownerId: 'owner-1', ifVersion: 1 });
    await expect(storage.loadSession({ harnessName: 'default', sessionId: 'session-1' })).resolves.toMatchObject({
      sessionIncarnation: stored!.sessionIncarnation,
    });

    const input = admission();
    input.sessionIncarnation = stored!.sessionIncarnation!;
    await storage.writeMessageResultEvidence(pendingEvidence(input));
    await expect(storage.admitTerminalHandoff(input)).resolves.toMatchObject({ status: 'created' });
    await expect(
      storage.commitTerminalHandoff({
        admission: input,
        resultEvidence: { ...pendingEvidence(input), status: 'completed', result: { text: 'ok' }, updatedAt: 3_000 },
        terminalResult: { status: 'completed', runId: input.runId, completedAt: 3_000 },
        projection: { projectionKind: 'chat.summary', projectionId: 'summary-1', payload: { text: 'ok' } },
      }),
    ).resolves.toMatchObject({ status: 'committed' });
  });

  it('namespaces delivery intents by the durable admission identity across sessions', async () => {
    const storage = new InMemoryHarness({ db: new InMemoryDB(), terminalHandoff: { enabled: true } });
    await storage.saveSession(session(), { ownerId: 'owner-1', ifVersion: 0 });
    const other = session({
      id: 'session-2',
      resourceId: 'resource-2',
      threadId: 'thread-2',
      sessionIncarnation: 'incarnation-2',
    });
    await storage.saveSession(other, { ownerId: 'owner-2', ifVersion: 0 });

    // Both sessions reuse the same caller admissionId under different grants.
    const first = admission();
    const secondInput = {
      ...admission(),
      sessionId: 'session-2',
      resourceId: 'resource-2',
      threadId: 'thread-2',
      sessionIncarnation: 'incarnation-2',
      executionGrant: { key: 'grant-2', generation: 1 },
    };
    for (const input of [first, secondInput]) {
      await storage.writeMessageResultEvidence(pendingEvidence(input));
      await storage.admitTerminalHandoff(input);
      const committed = await storage.commitTerminalHandoff({
        admission: input,
        resultEvidence: {
          ...pendingEvidence(input),
          status: 'completed',
          result: { text: `done ${input.sessionId}` },
          updatedAt: 3_000,
        },
        terminalResult: { status: 'completed', runId: input.runId, completedAt: 3_000 },
        projection: { projectionKind: 'chat.summary', projectionId: `summary-${input.sessionId}`, payload: {} },
      });
      expect(committed.status).toBe('committed');
      expect(committed.intent?.id).toBe(harnessTerminalIntentId(harnessTerminalAdmissionId(input)));
    }
    await expect(storage.getTerminalQueuePressure({ harnessName: 'default' })).resolves.toMatchObject({
      pendingIntents: 2,
    });
  });

  it('mints and persists an incarnation when a legacy row is loaded or updated', async () => {
    const db = new InMemoryDB();
    // A store written before terminal handoff existed: no incarnation is minted.
    const legacy = new InMemoryHarness({ db, terminalHandoff: { enabled: false } });
    await legacy.saveSession(session({ sessionIncarnation: undefined }), { ownerId: 'owner-1', ifVersion: 0 });
    expect(
      (await legacy.loadSession({ harnessName: 'default', sessionId: 'session-1' }))?.sessionIncarnation,
    ).toBeUndefined();

    // Enabling terminal handoff repairs the legacy row on first load; the mint
    // is persisted so every later reader sees the same winner.
    const enabled = new InMemoryHarness({ db, terminalHandoff: { enabled: true } });
    const loaded = await enabled.loadSession({ harnessName: 'default', sessionId: 'session-1' });
    expect(loaded?.sessionIncarnation).toEqual(expect.any(String));
    await expect(enabled.loadSession({ harnessName: 'default', sessionId: 'session-1' })).resolves.toMatchObject({
      sessionIncarnation: loaded!.sessionIncarnation,
    });
    await expect(
      enabled.loadSessionByThread({ harnessName: 'default', threadId: 'thread-1', resourceId: 'resource-1' }),
    ).resolves.toMatchObject({ sessionIncarnation: loaded!.sessionIncarnation });

    // An update that omits the incarnation keeps the minted fence.
    await enabled.saveSession(session({ sessionIncarnation: undefined }), { ownerId: 'owner-1', ifVersion: 1 });
    await expect(enabled.loadSession({ harnessName: 'default', sessionId: 'session-1' })).resolves.toMatchObject({
      sessionIncarnation: loaded!.sessionIncarnation,
    });
  });

  it('mints an incarnation on the first update of a legacy row before any load repairs it', async () => {
    const db = new InMemoryDB();
    const legacy = new InMemoryHarness({ db, terminalHandoff: { enabled: false } });
    await legacy.saveSession(session({ sessionIncarnation: undefined }), { ownerId: 'owner-1', ifVersion: 0 });

    const enabled = new InMemoryHarness({ db, terminalHandoff: { enabled: true } });
    await enabled.saveSession(session({ sessionIncarnation: undefined }), { ownerId: 'owner-1', ifVersion: 1 });
    const upgraded = await enabled.loadSession({ harnessName: 'default', sessionId: 'session-1' });
    expect(upgraded?.sessionIncarnation).toEqual(expect.any(String));
  });

  it('binds a grant generation to one admission across sessions', async () => {
    const storage = new InMemoryHarness({ db: new InMemoryDB(), terminalHandoff: { enabled: true } });
    await storage.saveSession(session(), { ownerId: 'owner-1', ifVersion: 0 });
    const other = session({
      id: 'session-2',
      resourceId: 'resource-2',
      threadId: 'thread-2',
      sessionIncarnation: 'incarnation-2',
    });
    await storage.saveSession(other, { ownerId: 'owner-2', ifVersion: 0 });

    const first = admission();
    await expect(storage.admitTerminalHandoff(first)).resolves.toMatchObject({ status: 'created' });

    // The same grant admitted under a different session conflicts rather than
    // creating a second competing admission.
    const competing = {
      ...admission(),
      sessionId: 'session-2',
      resourceId: 'resource-2',
      threadId: 'thread-2',
      sessionIncarnation: 'incarnation-2',
      admissionId: 'admission-2',
      admissionHash: 'admission-hash-2',
    };
    const conflict = await storage.admitTerminalHandoff(competing);
    expect(conflict.status).toBe('conflict');
    expect(conflict.admission.id).toBe(harnessTerminalAdmissionId(first));
  });

  it('resolves a committed grant winner across sessions before cancelling', async () => {
    const storage = new InMemoryHarness({ db: new InMemoryDB(), terminalHandoff: { enabled: true } });
    await storage.saveSession(session(), { ownerId: 'owner-1', ifVersion: 0 });
    const other = session({
      id: 'session-2',
      resourceId: 'resource-2',
      threadId: 'thread-2',
      sessionIncarnation: 'incarnation-2',
    });
    await storage.saveSession(other, { ownerId: 'owner-2', ifVersion: 0 });

    const winner = admission();
    await storage.writeMessageResultEvidence(pendingEvidence(winner));
    await storage.admitTerminalHandoff(winner);
    await storage.commitTerminalHandoff({
      admission: winner,
      resultEvidence: { ...pendingEvidence(winner), status: 'completed', result: { text: 'done' }, updatedAt: 3_000 },
      terminalResult: { status: 'completed', runId: winner.runId, completedAt: 3_000 },
      projection: { projectionKind: 'chat.summary', projectionId: 'summary-1', payload: { text: 'done' } },
    });

    // Cancelling the same grant under another session cannot tombstone over the
    // committed winner — the grant-scoped resolution validates the caller's
    // claimed identity and fails closed instead.
    await expect(
      storage.cancelTerminalHandoff({
        harnessName: 'default',
        sessionId: 'session-2',
        sessionIncarnation: 'incarnation-2',
        admissionId: 'admission-2',
        admissionHash: 'admission-hash-2',
        executionGrant: winner.executionGrant,
        reason: { code: 'cancelled', message: 'stale cancel' },
        cancelledAt: 4_000,
      }),
    ).rejects.toBeInstanceOf(HarnessTerminalHandoffIdentityConflictError);
    await expect(
      storage.loadTerminalAdmission({
        harnessName: 'default',
        sessionId: winner.sessionId,
        admissionId: winner.admissionId,
        executionGrant: winner.executionGrant,
      }),
    ).resolves.toMatchObject({ status: 'committed' });
    // The committed intent stays deliverable — no split committed+cancelled outcome.
    await expect(storage.getTerminalQueuePressure({ harnessName: 'default' })).resolves.toMatchObject({
      pendingIntents: 1,
    });
  });

  it('loads a terminal admission by run across pending and committed statuses', async () => {
    const storage = new InMemoryHarness({ db: new InMemoryDB(), terminalHandoff: { enabled: true } });
    await storage.saveSession(session(), { ownerId: 'owner-1', ifVersion: 0 });
    const input = admission();
    await storage.writeMessageResultEvidence(pendingEvidence(input));
    await storage.admitTerminalHandoff(input);

    const byRun = {
      harnessName: input.harnessName,
      sessionId: input.sessionId,
      runId: input.runId,
      sessionIncarnation: input.sessionIncarnation,
    };
    // The settlement-retry probe must see the row in ANY status: a commit that
    // sealed before the caller's bookkeeping finished is 'committed', not
    // 'pending', and still owns the run.
    await expect(storage.loadTerminalAdmissionByRun(byRun)).resolves.toMatchObject({ status: 'pending' });

    await storage.commitTerminalHandoff({
      admission: input,
      resultEvidence: { ...pendingEvidence(input), status: 'completed', result: { text: 'done' }, updatedAt: 3_000 },
      terminalResult: { status: 'completed', runId: input.runId, completedAt: 3_000 },
      projection: { projectionKind: 'chat.summary', projectionId: 'summary-1', payload: {} },
    });
    await expect(storage.loadPendingTerminalAdmission(byRun)).resolves.toBeNull();
    await expect(storage.loadTerminalAdmissionByRun(byRun)).resolves.toMatchObject({ status: 'committed' });
    await expect(storage.loadTerminalAdmissionByRun({ ...byRun, runId: 'run-other' })).resolves.toBeNull();
    // A run id is only deterministic within its session incarnation — a
    // deleted-then-recreated session id must never resolve the prior
    // incarnation's admission.
    await expect(
      storage.loadTerminalAdmissionByRun({ ...byRun, sessionIncarnation: 'incarnation-2' }),
    ).resolves.toBeNull();
    await expect(
      storage.loadPendingTerminalAdmission({ ...byRun, sessionIncarnation: 'incarnation-2' }),
    ).resolves.toBeNull();
  });

  it('conflicts a second grant admitted to the same session run', async () => {
    const storage = new InMemoryHarness({ db: new InMemoryDB(), terminalHandoff: { enabled: true } });
    await storage.saveSession(session(), { ownerId: 'owner-1', ifVersion: 0 });
    const first = admission();
    await expect(storage.admitTerminalHandoff(first)).resolves.toMatchObject({ status: 'created' });

    // A different grant aiming at the same (session, incarnation, run) —
    // recovery resolves admissions by that tuple, so a second row would make
    // the first-match probe nondeterministic.
    const second = {
      ...admission(),
      admissionId: 'admission-2',
      admissionHash: 'admission-hash-2',
      signalId: 'signal-2',
      executionGrant: { key: 'grant-2', generation: 1 },
    };
    await expect(storage.admitTerminalHandoff(second)).resolves.toMatchObject({
      status: 'conflict',
      admission: expect.objectContaining({ admissionId: first.admissionId, runId: first.runId }),
    });
    await expect(
      storage.loadTerminalAdmissionByRun({
        harnessName: first.harnessName,
        sessionId: first.sessionId,
        runId: first.runId,
        sessionIncarnation: first.sessionIncarnation,
      }),
    ).resolves.toMatchObject({ admissionId: first.admissionId });
  });

  it('rejects a terminal result bound to a different run before writing any rows', async () => {
    const storage = new InMemoryHarness({ db: new InMemoryDB(), terminalHandoff: { enabled: true } });
    await storage.saveSession(session(), { ownerId: 'owner-1', ifVersion: 0 });
    const input = admission();
    await storage.writeMessageResultEvidence(pendingEvidence(input));
    await storage.admitTerminalHandoff(input);

    // The intent's top-level runId comes from the admission; a result naming
    // another run would persist two disagreeing run identities.
    await expect(
      storage.commitTerminalHandoff({
        admission: input,
        resultEvidence: { ...pendingEvidence(input), status: 'completed', result: { text: 'done' }, updatedAt: 3_000 },
        terminalResult: { status: 'completed', runId: 'run-other', completedAt: 3_000 },
        projection: { projectionKind: 'chat.summary', projectionId: 'summary-1', payload: {} },
      }),
    ).rejects.toBeInstanceOf(HarnessTerminalHandoffValidationError);

    // The admission is untouched — a correctly bound retry still commits.
    await expect(
      storage.loadPendingTerminalAdmission({
        harnessName: input.harnessName,
        sessionId: input.sessionId,
        runId: input.runId,
        sessionIncarnation: input.sessionIncarnation,
      }),
    ).resolves.toMatchObject({ status: 'pending' });
    await expect(
      storage.commitTerminalHandoff({
        admission: input,
        resultEvidence: { ...pendingEvidence(input), status: 'completed', result: { text: 'done' }, updatedAt: 3_000 },
        terminalResult: { status: 'completed', runId: input.runId, completedAt: 3_000 },
        projection: { projectionKind: 'chat.summary', projectionId: 'summary-1', payload: {} },
      }),
    ).resolves.toMatchObject({ status: 'committed' });
  });

  it('preserves completed canonical evidence when a retried commit finds no intent row', async () => {
    const db = new InMemoryDB();
    const storage = new InMemoryHarness({ db, terminalHandoff: { enabled: true } });
    await storage.saveSession(session(), { ownerId: 'owner-1', ifVersion: 0 });
    const input = admission();
    await storage.writeMessageResultEvidence(pendingEvidence(input));
    await storage.admitTerminalHandoff(input);

    // A commit that crashed after writing canonical evidence but before the
    // intent row leaves: admission pending, evidence completed, intent absent.
    const storedEvidence = [...db.harnessMessageResultEvidence.values()].find(row => row.signalId === input.signalId)!;
    storedEvidence.status = 'completed';
    storedEvidence.result = { text: 'canonical winner' };

    // A retried commit carrying a DIFFERENT result must conflict rather than
    // overwrite the sealed canonical evidence.
    await expect(
      storage.commitTerminalHandoff({
        admission: input,
        resultEvidence: {
          ...pendingEvidence(input),
          status: 'completed',
          result: { text: 'impostor' },
          updatedAt: 3_000,
        },
        terminalResult: { status: 'completed', runId: input.runId, completedAt: 3_000 },
        projection: { projectionKind: 'chat.summary', projectionId: 'summary-1', payload: { text: 'done' } },
      }),
    ).rejects.toBeInstanceOf(HarnessTerminalHandoffIdentityConflictError);
    await expect(
      storage.loadMessageResultEvidence({
        harnessName: input.harnessName,
        sessionId: input.sessionId,
        resourceId: input.resourceId,
        threadId: input.threadId,
        signalId: input.signalId,
      }),
    ).resolves.toMatchObject({ status: 'completed', result: { text: 'canonical winner' } });

    // The faithful retry completes the half-applied commit — same result, so
    // the evidence compare passes and the intent is finally written.
    const retried = await storage.commitTerminalHandoff({
      admission: input,
      resultEvidence: {
        ...pendingEvidence(input),
        status: 'completed',
        result: { text: 'canonical winner' },
        updatedAt: 3_000,
      },
      terminalResult: { status: 'completed', runId: input.runId, completedAt: 3_000 },
      projection: { projectionKind: 'chat.summary', projectionId: 'summary-1', payload: { text: 'done' } },
    });
    expect(retried.status).toBe('committed');
    expect(retried.intent?.id).toBe(harnessTerminalIntentId(harnessTerminalAdmissionId(input)));
  });

  it('leaves admission pending and evidence untouched when the commit payload cannot be cloned', async () => {
    const db = new InMemoryDB();
    const storage = new InMemoryHarness({ db, terminalHandoff: { enabled: true } });
    await storage.saveSession(session(), { ownerId: 'owner-1', ifVersion: 0 });
    const input = admission();
    await storage.writeMessageResultEvidence(pendingEvidence(input));
    await storage.admitTerminalHandoff(input);

    // A function inside the provider result fails structuredClone — the throw
    // must land BEFORE any durable row mutates, or a committed admission with
    // pending evidence and no intent violates the atomic commit contract.
    await expect(
      storage.commitTerminalHandoff({
        admission: input,
        resultEvidence: {
          ...pendingEvidence(input),
          status: 'completed',
          result: { text: 'x', callback: () => 'uncloneable' },
          updatedAt: 3_000,
        },
        terminalResult: { status: 'completed', runId: input.runId, completedAt: 3_000 },
        projection: { projectionKind: 'chat.summary', projectionId: 'summary-1', payload: { text: 'done' } },
      }),
    ).rejects.toBeTruthy();
    await expect(
      storage.loadTerminalAdmission({
        harnessName: input.harnessName,
        sessionId: input.sessionId,
        admissionId: input.admissionId,
        executionGrant: input.executionGrant,
      }),
    ).resolves.toMatchObject({ status: 'pending' });
    await expect(
      storage.loadMessageResultEvidence({
        harnessName: input.harnessName,
        sessionId: input.sessionId,
        resourceId: input.resourceId,
        threadId: input.threadId,
        signalId: input.signalId,
      }),
    ).resolves.toMatchObject({ status: 'pending' });
    await expect(
      storage.loadTerminalIntent({
        harnessName: input.harnessName,
        intentId: harnessTerminalIntentId(harnessTerminalAdmissionId(input)),
      }),
    ).resolves.toBeNull();
    await expect(storage.getTerminalQueuePressure({ harnessName: input.harnessName })).resolves.toEqual({
      pendingIntents: 0,
      pendingBytes: 0,
    });
  });

  it('rejects a deeply nested admission seed with a typed validation error instead of RangeError', async () => {
    const storage = new InMemoryHarness({ db: new InMemoryDB(), terminalHandoff: { enabled: true } });
    await storage.saveSession(session(), { ownerId: 'owner-1', ifVersion: 0 });
    let deepSeed: Record<string, unknown> = { leaf: true };
    for (let i = 0; i < 10_000; i++) deepSeed = { nested: deepSeed };
    const input = { ...admission(), seed: deepSeed };
    await storage.writeMessageResultEvidence(pendingEvidence(input));
    await expect(storage.admitTerminalHandoff(input)).rejects.toBeInstanceOf(HarnessTerminalHandoffValidationError);
    // The rejection must be the typed depth error, not a stack-overflow RangeError.
    await expect(storage.admitTerminalHandoff(input)).rejects.toThrow('nesting levels');
    await expect(
      storage.loadTerminalAdmission({
        harnessName: input.harnessName,
        sessionId: input.sessionId,
        admissionId: input.admissionId,
        executionGrant: input.executionGrant,
      }),
    ).resolves.toBeNull();
  });

  it('reports terminal handoff as unsupported and rejects terminal operations when disabled', async () => {
    const disabled = new InMemoryHarness({ db: new InMemoryDB() });
    expect(disabled.supportsTerminalHandoff).toBe(false);
    const input = admission();
    const expected = HarnessTerminalHandoffUnsupportedError;
    await expect(disabled.admitTerminalHandoff(input)).rejects.toBeInstanceOf(expected);
    await expect(
      disabled.loadTerminalAdmission({
        harnessName: input.harnessName,
        sessionId: input.sessionId,
        admissionId: input.admissionId,
        executionGrant: input.executionGrant,
      }),
    ).rejects.toBeInstanceOf(expected);
    await expect(
      disabled.loadPendingTerminalAdmission({
        harnessName: input.harnessName,
        sessionId: input.sessionId,
        runId: input.runId,
        sessionIncarnation: input.sessionIncarnation,
      }),
    ).rejects.toBeInstanceOf(expected);
    await expect(
      disabled.loadTerminalAdmissionByRun({
        harnessName: input.harnessName,
        sessionId: input.sessionId,
        runId: input.runId,
        sessionIncarnation: input.sessionIncarnation,
      }),
    ).rejects.toBeInstanceOf(expected);
    await expect(
      disabled.commitTerminalHandoff({
        admission: input,
        resultEvidence: pendingEvidence(input),
        terminalResult: { status: 'completed', runId: input.runId, completedAt: 3_000 },
        projection: { projectionKind: 'chat.summary', projectionId: 'summary-1', payload: {} },
      }),
    ).rejects.toBeInstanceOf(expected);
    await expect(
      disabled.loadTerminalIntent({
        harnessName: input.harnessName,
        intentId: harnessTerminalIntentId(harnessTerminalAdmissionId(input)),
      }),
    ).rejects.toBeInstanceOf(expected);
    await expect(
      disabled.claimTerminalIntents({ harnessName: input.harnessName, consumerId: 'w-1', limit: 1, now: 4_000 }),
    ).rejects.toBeInstanceOf(expected);
    await expect(disabled.getTerminalQueuePressure({ harnessName: input.harnessName })).rejects.toBeInstanceOf(
      expected,
    );

    const enabled = new InMemoryHarness({ db: new InMemoryDB(), terminalHandoff: { enabled: true } });
    expect(enabled.supportsTerminalHandoff).toBe(true);
  });

  it('binds claim settlement to the claiming consumer', async () => {
    const storage = new InMemoryHarness({ db: new InMemoryDB(), terminalHandoff: { enabled: true } });
    await storage.saveSession(session(), { ownerId: 'owner-1', ifVersion: 0 });
    const input = admission();
    await storage.writeMessageResultEvidence(pendingEvidence(input));
    await storage.admitTerminalHandoff(input);
    await storage.commitTerminalHandoff({
      admission: input,
      resultEvidence: { ...pendingEvidence(input), status: 'completed', result: { text: 'done' }, updatedAt: 3_000 },
      terminalResult: { status: 'completed', runId: input.runId, completedAt: 3_000 },
      projection: { projectionKind: 'chat.summary', projectionId: 'summary-1', payload: { text: 'done' } },
    });
    const claimed = (
      await storage.claimTerminalIntents({
        harnessName: input.harnessName,
        consumerId: 'worker-a',
        limit: 1,
        now: 4_000,
        leaseMs: 10_000,
      })
    ).intents[0]!;

    // A caller holding the live claim id under a different consumer must not
    // settle the lease — the claim is bound to the consumer that minted it.
    const foreignIdentity = {
      harnessName: input.harnessName,
      intentId: claimed.id,
      sessionId: claimed.sessionId,
      sessionIncarnation: claimed.sessionIncarnation,
      revision: claimed.revision,
      payloadHash: claimed.projection.payloadHash,
      claimId: claimed.claimId!,
      consumerId: 'worker-b',
      now: 4_500,
    };
    await expect(storage.ackTerminalIntent(foreignIdentity)).rejects.toBeInstanceOf(
      HarnessTerminalHandoffClaimConflictError,
    );
    await expect(storage.renewTerminalIntent({ ...foreignIdentity, leaseMs: 10_000 })).rejects.toBeInstanceOf(
      HarnessTerminalHandoffClaimConflictError,
    );
    await expect(
      storage.failTerminalIntent({ ...foreignIdentity, error: { code: 'x', message: 'x' } }),
    ).rejects.toBeInstanceOf(HarnessTerminalHandoffClaimConflictError);

    // The owning consumer settles normally.
    await expect(storage.ackTerminalIntent({ ...foreignIdentity, consumerId: 'worker-a' })).resolves.toMatchObject({
      status: 'acked',
    });
  });

  it('rejects a cancel whose admission identity does not match the grant-bound admission', async () => {
    const storage = new InMemoryHarness({ db: new InMemoryDB(), terminalHandoff: { enabled: true } });
    await storage.saveSession(session(), { ownerId: 'owner-1', ifVersion: 0 });
    const input = admission();
    await storage.writeMessageResultEvidence(pendingEvidence(input));
    await storage.admitTerminalHandoff(input);

    const cancel = {
      harnessName: input.harnessName,
      sessionId: input.sessionId,
      sessionIncarnation: input.sessionIncarnation,
      admissionId: input.admissionId,
      admissionHash: input.admissionHash,
      executionGrant: input.executionGrant,
      reason: { code: 'cancelled', message: 'stop' },
      cancelledAt: 2_100,
    };
    // A grant binds exactly one admission — a cancel naming a different
    // admission/session/incarnation under the same grant is a conflict, not
    // a fence to mint.
    await expect(storage.cancelTerminalHandoff({ ...cancel, admissionId: 'admission-foreign' })).rejects.toBeInstanceOf(
      HarnessTerminalHandoffIdentityConflictError,
    );
    await expect(storage.cancelTerminalHandoff({ ...cancel, admissionHash: 'hash-foreign' })).rejects.toBeInstanceOf(
      HarnessTerminalHandoffIdentityConflictError,
    );
    await expect(storage.cancelTerminalHandoff({ ...cancel, sessionId: 'session-foreign' })).rejects.toBeInstanceOf(
      HarnessTerminalHandoffIdentityConflictError,
    );
    await expect(
      storage.cancelTerminalHandoff({ ...cancel, sessionIncarnation: 'incarnation-foreign' }),
    ).rejects.toBeInstanceOf(HarnessTerminalHandoffIdentityConflictError);

    // A malformed cancel must not mint fencing evidence either.
    await expect(storage.cancelTerminalHandoff({ ...cancel, admissionId: '' })).rejects.toBeInstanceOf(
      HarnessTerminalHandoffValidationError,
    );
    await expect(
      storage.cancelTerminalHandoff({
        ...cancel,
        executionGrant: { key: '', generation: 1 },
      }),
    ).rejects.toBeInstanceOf(HarnessTerminalHandoffValidationError);

    // No tombstone was written by the rejected cancels — the matching cancel
    // still creates the fence (not a `duplicate` against phantom evidence).
    await expect(storage.cancelTerminalHandoff(cancel)).resolves.toMatchObject({ status: 'cancelled' });
    await expect(storage.cancelTerminalHandoff(cancel)).resolves.toMatchObject({ status: 'duplicate' });
  });
});
