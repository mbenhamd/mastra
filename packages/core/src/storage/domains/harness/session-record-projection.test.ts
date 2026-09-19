import { describe, expect, it } from 'vitest';

import { InMemoryDB } from '../inmemory-db';
import { buildHarnessSessionRecordPostImage } from './session-record-projection';
import type { SessionRecord } from './types';
import { HarnessStorageSessionProjectionIncarnationError, InMemoryHarness } from './index';

describe('native session record projection', () => {
  it('keeps the post-image bounded and delivers revisions in order across recreation', async () => {
    const record = sampleSession({
      pendingQueue: [{ id: 'queued-1', enqueuedAt: 1000, content: 'private text', attachments: [] }],
      pendingResume: {
        kind: 'tool-approval',
        itemId: 'pending-1',
        runId: 'run-1',
        toolCallId: 'call-1',
        toolName: 'write-file',
        source: 'parent',
        requestedAt: 1000,
        expiresAt: 2000,
      },
      currentRun: {
        runId: 'run-1',
        harnessName: 'default',
        sessionId: 'session-1',
        resourceId: 'resource-1',
        threadId: 'thread-1',
        agentId: 'agent-1',
        operation: { kind: 'sync-generate', operationId: 'operation-1' },
        modeId: 'build',
        modelId: 'model-1',
        status: 'running',
        startedAt: 1000,
        updatedAt: 1000,
      },
      assistantDrafts: {
        'run-1': {
          runId: 'run-1',
          sessionId: 'session-1',
          resourceId: 'resource-1',
          threadId: 'thread-1',
          text: 'private draft text',
          status: 'streaming',
          startedAt: 1000,
          updatedAt: 1000,
        },
      },
    });
    const { payload } = buildHarnessSessionRecordPostImage(record, { maxPayloadBytes: 64 * 1024 });
    expect(payload).toMatchObject({
      sessionId: 'session-1',
      queueDepth: 1,
      pendingResume: { itemId: 'pending-1', runId: 'run-1' },
      currentRun: { runId: 'run-1', status: 'running' },
    });
    expect(payload).not.toHaveProperty('assistantDrafts');
    expect(JSON.stringify(payload)).not.toContain('private draft text');
    expect(JSON.stringify(payload)).not.toContain('private text');

    const db = new InMemoryDB();
    const storage = new InMemoryHarness({
      db,
      sessionRecordProjection: { enabled: true, maxPendingIntents: 10 },
    });
    const created = await storage.createOrLoadActiveSession(record, {
      initialLease: { ownerId: 'owner-1', ttlMs: 60_000 },
    });
    const incarnation = created.record.sessionIncarnation;
    expect(incarnation).toBeTruthy();
    const firstNow = Date.now() + 1;

    const firstClaim = await storage.claimSessionRecordProjectionIntents({
      claimId: 'claim-1',
      limit: 1,
      now: firstNow,
      claimTtlMs: 10_000,
    });
    expect(firstClaim).toHaveLength(1);
    expect(firstClaim[0]).toMatchObject({ revision: 1, sessionIncarnation: incarnation, status: 'claimed' });

    const loaded = await storage.loadSession({ sessionId: record.id });
    if (!loaded) throw new Error('expected created session');
    await storage.saveSession({ ...loaded, lastActivityAt: 2_001 }, { ownerId: 'owner-1', ifVersion: 1 });
    await expect(
      storage.claimSessionRecordProjectionIntents({
        claimId: 'claim-2',
        limit: 1,
        now: firstNow + 2,
        claimTtlMs: 10_000,
      }),
    ).resolves.toHaveLength(0);

    await expect(
      storage.ackSessionRecordProjection({
        operationId: firstClaim[0]!.operationId,
        sessionId: record.id,
        sessionIncarnation: incarnation!,
        revision: 1,
        payloadDigest: firstClaim[0]!.payloadDigest,
        claimId: 'claim-1',
        acknowledgedAt: firstNow + 3,
      }),
    ).resolves.toMatchObject({ status: 'applied' });
    const secondClaim = await storage.claimSessionRecordProjectionIntents({
      claimId: 'claim-3',
      limit: 1,
      now: firstNow + 4,
      claimTtlMs: 10_000,
    });
    expect(secondClaim).toHaveLength(1);
    expect(secondClaim[0]).toMatchObject({ revision: 2, sessionIncarnation: incarnation, status: 'claimed' });

    await storage.deleteSession({ sessionId: record.id, ifVersion: 2 });
    await expect(
      storage.ackSessionRecordProjection({
        operationId: secondClaim[0]!.operationId,
        sessionId: record.id,
        sessionIncarnation: incarnation!,
        revision: 2,
        payloadDigest: secondClaim[0]!.payloadDigest,
        claimId: 'claim-3',
        acknowledgedAt: firstNow + 5,
      }),
    ).resolves.toMatchObject({ status: 'fenced' });

    const recreated = await storage.saveSession(sampleSession({}), { ownerId: 'owner-2', ifVersion: 0 });
    expect(recreated.version).toBe(1);
    const recreatedRecord = await storage.loadSession({ sessionId: record.id });
    expect(recreatedRecord?.sessionIncarnation).toBeTruthy();
    expect(recreatedRecord?.sessionIncarnation).not.toBe(incarnation);
  });

  it('rejects invalid projection inputs and fails closed on legacy rows without provenance', async () => {
    expect(() =>
      buildHarnessSessionRecordPostImage(sampleSession({ id: 'x'.repeat(1025) }), { maxPayloadBytes: 64 * 1024 }),
    ).toThrow(RangeError);
    expect(() =>
      buildHarnessSessionRecordPostImage(sampleSession({ lastActivityAt: Number.NaN }), { maxPayloadBytes: 64 * 1024 }),
    ).toThrow(RangeError);

    const db = new InMemoryDB();
    const projectedStorage = new InMemoryHarness({
      db,
      sessionRecordProjection: { enabled: true, maxPendingIntents: 10 },
    });
    const invalidId = 'x'.repeat(1025);
    await expect(
      projectedStorage.saveSession(sampleSession({ id: invalidId }), { ownerId: 'owner-1', ifVersion: 0 }),
    ).rejects.toThrow(RangeError);
    await expect(projectedStorage.loadSession({ sessionId: invalidId })).resolves.toBeNull();

    const legacy = new InMemoryHarness({ db });
    await legacy.saveSession(sampleSession(), { ownerId: 'legacy-owner', ifVersion: 0 });
    const projected = new InMemoryHarness({ db, sessionRecordProjection: true });
    const existing = await projected.loadSession({ sessionId: 'session-1' });
    if (!existing) throw new Error('expected legacy session');
    await expect(
      projected.saveSession(
        { ...existing, lastActivityAt: existing.lastActivityAt + 1 },
        {
          ownerId: 'legacy-owner',
          ifVersion: existing.version,
        },
      ),
    ).rejects.toBeInstanceOf(HarnessStorageSessionProjectionIncarnationError);
    await expect(projected.deleteSession({ sessionId: existing.id })).rejects.toBeInstanceOf(
      HarnessStorageSessionProjectionIncarnationError,
    );
  });
});

function sampleSession(overrides: Partial<SessionRecord> = {}): SessionRecord {
  return {
    harnessName: 'default',
    id: 'session-1',
    resourceId: 'resource-1',
    threadId: 'thread-1',
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
    createdAt: 1000,
    lastActivityAt: 1000,
    version: 0,
    ...overrides,
  };
}
