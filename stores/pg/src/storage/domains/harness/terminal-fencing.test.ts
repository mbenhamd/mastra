import { randomUUID } from 'node:crypto';

import { createSampleSessionRecord } from '@internal/storage-test-utils';
import {
  HarnessStorageLeaseConflictError,
  HarnessStorageSessionClosedError,
  HarnessTerminalHandoffFencedError,
  HarnessTerminalHandoffIdentityConflictError,
  TABLE_HARNESS_MESSAGE_RESULTS,
  TABLE_HARNESS_SESSIONS,
  TABLE_HARNESS_TERMINAL_ADMISSIONS,
  TABLE_HARNESS_TERMINAL_TOMBSTONES,
} from '@mastra/core/storage';
import type {
  AgentSignalResultEvidence,
  HarnessStorage,
  HarnessTerminalAdmissionInput,
  SessionRecord,
} from '@mastra/core/storage';
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';

import { PostgresStore } from '../..';
import { EXPORTED_FENCE_AUTHORITY, EXPORTED_FENCE_EXPIRES_AT } from '../../exported-fence';
import { TEST_CONFIG } from '../../test-utils';

vi.setConfig({ testTimeout: 60_000, hookTimeout: 60_000 });

const HARNESS = 'default';
const INTERRUPTED = { code: 'harness.run_interrupted', message: 'interrupted on adoption' };

function terminalStore(id: string, schemaName: string, extra: Record<string, unknown> = {}) {
  return new PostgresStore({
    ...TEST_CONFIG,
    id,
    schemaName,
    enabledDomains: ['harness'],
    sessionRecordProjection: { enabled: true },
    terminalHandoff: { enabled: true },
    ...extra,
  });
}

async function createNativeSession(
  harness: HarnessStorage,
  id: string,
  ownerId = `owner-${id}`,
  ttlMs = 60_000,
): Promise<SessionRecord> {
  const record = createSampleSessionRecord({
    id,
    harnessName: HARNESS,
    resourceId: `resource-${id}`,
    threadId: `thread-${id}`,
  });
  const result = await harness.createOrLoadActiveSession(record, { initialLease: { ownerId, ttlMs } });
  if (!result.created) throw new Error(`expected a fresh session for ${id}`);
  return (await harness.loadSession({ harnessName: HARNESS, sessionId: id }))!;
}

function admissionFor(session: SessionRecord, tag: string): HarnessTerminalAdmissionInput {
  return {
    harnessName: HARNESS,
    sessionId: session.id,
    resourceId: session.resourceId,
    threadId: session.threadId,
    sessionIncarnation: session.sessionIncarnation!,
    admissionId: `admission-${tag}`,
    admissionHash: `admission-hash-${tag}`,
    signalId: `signal-${tag}`,
    runId: `run-${tag}`,
    executionGrant: { key: `grant-${tag}`, generation: 1 },
    finalizerId: 'doxa.chat',
    finalizerVersion: '1',
    seed: { admissionId: `admission-${tag}` },
    createdAt: Date.now(),
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
    createdAt: Date.now(),
    updatedAt: Date.now(),
  };
}

function revocation(input: HarnessTerminalAdmissionInput) {
  return {
    harnessName: input.harnessName,
    sessionId: input.sessionId,
    admissionId: input.admissionId,
    executionGrant: input.executionGrant,
    reason: { code: 'doxa.turn_released', message: 'released before admission' },
  };
}

async function tombstoneNullability(store: PostgresStore, schemaName: string) {
  const rows = await store.db.manyOrNone<{ column_name: string; is_nullable: string }>(
    `SELECT column_name, is_nullable FROM information_schema.columns
      WHERE table_schema = $1 AND table_name = $2 AND column_name IN ('session_incarnation', 'admission_hash')
      ORDER BY column_name`,
    [schemaName, TABLE_HARNESS_TERMINAL_TOMBSTONES],
  );
  return Object.fromEntries(rows.map(row => [row.column_name, row.is_nullable]));
}

async function makeTombstoneIdentityRequired(store: PostgresStore, schemaName: string) {
  await store.db.none(
    `ALTER TABLE "${schemaName}"."${TABLE_HARNESS_TERMINAL_TOMBSTONES}"
       ALTER COLUMN session_incarnation SET NOT NULL,
       ALTER COLUMN admission_hash SET NOT NULL`,
  );
}

describe('HarnessPG pre-admission grant revocation', () => {
  const schemaName = `pf4981_revoke_${randomUUID().replaceAll('-', '_')}`;
  const store = terminalStore('pg-harness-revoke-store', schemaName);
  const harness = () => store.stores.harness!;
  const extraSchemas: string[] = [];

  beforeAll(async () => {
    await store.init();
  });

  beforeEach(async () => {
    await harness().dangerouslyClearAll();
  });

  afterAll(async () => {
    for (const extra of [schemaName, ...extraSchemas]) {
      await store.db.none(`DROP SCHEMA IF EXISTS "${extra}" CASCADE`).catch(() => {});
    }
    await store.close();
  });

  it('fences a never-admitted grant with an identity-less tombstone; the late admission is cancelled', async () => {
    expect(harness().supportsTerminalGrantRevocation).toBe(true);
    const session = await createNativeSession(harness(), 'revoke-first');
    const input = admissionFor(session, 'revoke-first');

    const revoked = await harness().revokeTerminalGrant({ ...revocation(input), revokedAt: 1_700_000_000_000 });
    expect(revoked).toMatchObject({ status: 'revoked', revokedAt: 1_700_000_000_000 });
    await expect(harness().revokeTerminalGrant(revocation(input))).resolves.toEqual({
      ...revoked,
      status: 'duplicate',
    });
    const row = await store.db.one<Record<string, unknown>>(
      `SELECT session_id, session_incarnation, admission_id, admission_hash, reason_json
         FROM "${schemaName}"."${TABLE_HARNESS_TERMINAL_TOMBSTONES}" WHERE id = $1`,
      [revoked.tombstoneId],
    );
    expect(row).toMatchObject({
      session_id: session.id,
      session_incarnation: null,
      admission_id: input.admissionId,
      admission_hash: null,
    });

    await harness().writeMessageResultEvidence(pendingEvidence(input));
    await expect(
      harness().admitTerminalHandoff(input, { leaseOwner: { ownerId: session.ownerId! } }),
    ).resolves.toMatchObject({ status: 'cancelled' });
    // The lease holder's undispatched reservation was settled in the same transaction.
    await expect(
      harness().loadMessageResultEvidence({
        harnessName: HARNESS,
        sessionId: session.id,
        resourceId: session.resourceId,
        threadId: session.threadId,
        signalId: input.signalId,
      }),
    ).resolves.toMatchObject({ status: 'failed', error: { code: 'harness.terminal_cancelled' } });
    const admissions = await store.db.one<{ count: string }>(
      `SELECT COUNT(*)::text AS count FROM "${schemaName}"."${TABLE_HARNESS_TERMINAL_ADMISSIONS}"`,
    );
    expect(admissions.count).toBe('0');
    await expect(
      harness().cancelTerminalHandoff({
        harnessName: HARNESS,
        sessionId: session.id,
        sessionIncarnation: session.sessionIncarnation!,
        admissionId: input.admissionId,
        admissionHash: input.admissionHash,
        executionGrant: input.executionGrant,
        reason: { code: 'cancelled', message: 'stop' },
      }),
    ).resolves.toMatchObject({ status: 'duplicate', tombstoneId: revoked.tombstoneId });
  });

  it('returns an existing admission unchanged and conflicts on a foreign identity', async () => {
    const session = await createNativeSession(harness(), 'admit-first');
    const input = admissionFor(session, 'admit-first');
    await harness().writeMessageResultEvidence(pendingEvidence(input));
    const admitted = await harness().admitTerminalHandoff(input);
    expect(admitted.status).toBe('created');

    await expect(harness().revokeTerminalGrant(revocation(input))).resolves.toMatchObject({
      status: 'admitted',
      admission: { id: admitted.admission.id, status: 'pending' },
    });
    const tombstones = await store.db.one<{ count: string }>(
      `SELECT COUNT(*)::text AS count FROM "${schemaName}"."${TABLE_HARNESS_TERMINAL_TOMBSTONES}"`,
    );
    expect(tombstones.count).toBe('0');
    await expect(
      harness().revokeTerminalGrant({ ...revocation(input), admissionId: 'admission-foreign' }),
    ).rejects.toBeInstanceOf(HarnessTerminalHandoffIdentityConflictError);
  });

  it('serializes concurrent revoke and admit of one grant to exactly one winner', async () => {
    const session = await createNativeSession(harness(), 'revoke-race');
    for (let round = 0; round < 12; round++) {
      const input = admissionFor(session, `revoke-race-${round}`);
      await harness().writeMessageResultEvidence(pendingEvidence(input));
      const [revoked, admitted] = await Promise.all([
        harness().revokeTerminalGrant(revocation(input)),
        harness().admitTerminalHandoff(input),
      ]);
      // Either the revocation won (the admission is cancelled and nothing is
      // admitted) or the admission won (the revocation reports it unchanged).
      const outcome = `${revoked.status}/${admitted.status}`;
      expect(['revoked/cancelled', 'admitted/created']).toContain(outcome);
      const stored = await harness().loadTerminalAdmission({
        harnessName: HARNESS,
        sessionId: session.id,
        admissionId: input.admissionId,
        executionGrant: input.executionGrant,
      });
      expect(stored === null).toBe(outcome === 'revoked/cancelled');
    }
  });

  it('serializes with closure export and refuses a session that export handed off', async () => {
    const session = await createNativeSession(harness(), 'handed-off');
    const sessions = `"${schemaName}"."${TABLE_HARNESS_SESSIONS}"`;

    // An export whose REPEATABLE READ snapshot predates the revocation cannot
    // retire the session afterwards: it fails and re-exports with the tombstone.
    let retirementError: unknown;
    await store.db
      .tx(async t => {
        await t.none('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ');
        await t.one(`SELECT id FROM ${sessions} WHERE harness_name = $1 AND id = $2`, [HARNESS, session.id]);
        await harness().revokeTerminalGrant(revocation(admissionFor(session, 'during-export')));
        try {
          await t.none(`UPDATE ${sessions} SET version = version + 1 WHERE harness_name = $1 AND id = $2`, [
            HARNESS,
            session.id,
          ]);
        } catch (error) {
          retirementError = error;
          throw error;
        }
      })
      .catch(() => {});
    expect(retirementError).toMatchObject({ code: '40001' });

    // Once an export retired the session here, its admissions belong to the
    // destination store: a revocation here would fence nothing.
    await store.db.none(
      `UPDATE ${sessions} SET owner_id = $3, lease_expires_at = $4, closed_at = $5
        WHERE harness_name = $1 AND id = $2`,
      [HARNESS, session.id, EXPORTED_FENCE_AUTHORITY, EXPORTED_FENCE_EXPIRES_AT, Date.now()],
    );
    const after = admissionFor(session, 'after-export');
    await expect(harness().revokeTerminalGrant(revocation(after))).rejects.toBeInstanceOf(
      HarnessTerminalHandoffFencedError,
    );
    const tombstones = await store.db.one<{ count: string }>(
      `SELECT COUNT(*)::text AS count FROM "${schemaName}"."${TABLE_HARNESS_TERMINAL_TOMBSTONES}" WHERE grant_key = $1`,
      [after.executionGrant.key],
    );
    expect(tombstones.count).toBe('0');
  });

  it.each(['init', 'first use'] as const)(
    'relaxes a legacy NOT NULL tombstone table on %s, and legacy tombstones still fence',
    async path => {
      const legacySchema = `pf4981_legacy_${path === 'init' ? 'init' : 'lazy'}_${randomUUID().slice(0, 8)}`;
      extraSchemas.push(legacySchema);
      const creator = terminalStore(`pg-legacy-creator-${path}`, legacySchema);
      await creator.init();
      const upgraded = terminalStore(`pg-legacy-upgraded-${path}`, legacySchema);
      try {
        const session = await createNativeSession(
          creator.stores.harness!,
          `legacy-${path === 'init' ? 'init' : 'lazy'}`,
        );
        const legacy = admissionFor(session, 'legacy');
        await creator.stores.harness!.cancelTerminalHandoff({
          harnessName: HARNESS,
          sessionId: session.id,
          sessionIncarnation: session.sessionIncarnation!,
          admissionId: legacy.admissionId,
          admissionHash: legacy.admissionHash,
          executionGrant: legacy.executionGrant,
          reason: { code: 'cancelled', message: 'legacy cancel' },
        });
        await makeTombstoneIdentityRequired(creator, legacySchema);
        expect(await tombstoneNullability(creator, legacySchema)).toEqual({
          admission_hash: 'NO',
          session_incarnation: 'NO',
        });

        if (path === 'init') await upgraded.init();
        const harnessUpgraded = upgraded.stores.harness!;
        await expect(
          harnessUpgraded.revokeTerminalGrant(revocation(admissionFor(session, 'fresh'))),
        ).resolves.toMatchObject({ status: 'revoked' });
        expect(await tombstoneNullability(creator, legacySchema)).toEqual({
          admission_hash: 'YES',
          session_incarnation: 'YES',
        });
        await harnessUpgraded.writeMessageResultEvidence(pendingEvidence(legacy));
        await expect(harnessUpgraded.admitTerminalHandoff(legacy)).resolves.toMatchObject({ status: 'cancelled' });
      } finally {
        await upgraded.close();
        await creator.close();
      }
    },
  );

  it('leaves an external-schema tombstone table to the operator migration', async () => {
    const externalSchema = `pf4981_external_${randomUUID().slice(0, 8)}`;
    extraSchemas.push(externalSchema);
    const creator = terminalStore('pg-external-creator', externalSchema);
    await creator.init();
    const external = terminalStore('pg-external-store', externalSchema, { disableInit: true });
    try {
      const session = await createNativeSession(creator.stores.harness!, 'external');
      await makeTombstoneIdentityRequired(creator, externalSchema);
      const input = admissionFor(session, 'external');

      // No runtime DDL: the revocation fails on the constraint and writes nothing.
      await expect(external.stores.harness!.revokeTerminalGrant(revocation(input))).rejects.toThrow();
      expect(await tombstoneNullability(creator, externalSchema)).toEqual({
        admission_hash: 'NO',
        session_incarnation: 'NO',
      });
      const before = await creator.db.one<{ count: string }>(
        `SELECT COUNT(*)::text AS count FROM "${externalSchema}"."${TABLE_HARNESS_TERMINAL_TOMBSTONES}"`,
      );
      expect(before.count).toBe('0');

      // After the operator applies the migration the revocation succeeds.
      await creator.db.none(
        `ALTER TABLE "${externalSchema}"."${TABLE_HARNESS_TERMINAL_TOMBSTONES}"
           ALTER COLUMN session_incarnation DROP NOT NULL,
           ALTER COLUMN admission_hash DROP NOT NULL`,
      );
      await expect(external.stores.harness!.revokeTerminalGrant(revocation(input))).resolves.toMatchObject({
        status: 'revoked',
      });
    } finally {
      await external.close();
      await creator.close();
    }
  });
});

describe('HarnessPG lease-fenced dispatch compare-and-swap', () => {
  const schemaName = `pf4981_dispatch_${randomUUID().replaceAll('-', '_')}`;
  const store = terminalStore('pg-harness-dispatch-lease-store', schemaName);
  const harness = () => store.stores.harness!;

  beforeAll(async () => {
    await store.init();
  });

  beforeEach(async () => {
    await harness().dangerouslyClearAll();
  });

  afterAll(async () => {
    await store.db.none(`DROP SCHEMA IF EXISTS "${schemaName}" CASCADE`).catch(() => {});
    await store.close();
  });

  function stamp(input: HarnessTerminalAdmissionInput, ownerId: string) {
    return harness().compareAndSwapSignalDispatch({
      harnessName: HARNESS,
      sessionId: input.sessionId,
      resourceId: input.resourceId,
      threadId: input.threadId,
      signalId: input.signalId,
      admissionId: input.admissionId,
      admissionHash: input.admissionHash,
      operationKind: 'message',
      expected: { state: 'reserved' },
      next: {
        state: 'dispatching',
        attemptId: `terminal-dispatch-${ownerId}`,
        claimExpiresAt: Date.now() + 30_000,
        delivery: 'idle',
        runId: input.runId,
      },
      leaseOwner: { ownerId },
      updatedAt: Date.now(),
    });
  }

  async function dispatchState(input: HarnessTerminalAdmissionInput) {
    const evidence = await harness().loadMessageResultEvidence({
      harnessName: HARNESS,
      sessionId: input.sessionId,
      resourceId: input.resourceId,
      threadId: input.threadId,
      signalId: input.signalId,
    });
    return (evidence as AgentSignalResultEvidence).dispatch?.state ?? 'unstamped';
  }

  it('refuses an owner that does not hold the lease, or a closed session, and writes nothing', async () => {
    const session = await createNativeSession(harness(), 'foreign-lease', 'owner-a');
    const input = admissionFor(session, 'foreign-lease');
    await harness().writeMessageResultEvidence(pendingEvidence(input));

    await expect(stamp(input, 'owner-b')).rejects.toMatchObject({
      name: 'HarnessStorageLeaseConflictError',
      heldBy: 'owner-a',
    });
    expect(await dispatchState(input)).toBe('unstamped');
    await expect(stamp(input, 'owner-a')).resolves.toMatchObject({ applied: true });
    expect(await dispatchState(input)).toBe('dispatching');

    const closedSession = await createNativeSession(harness(), 'closed-lease', 'owner-c');
    const closedInput = admissionFor(closedSession, 'closed-lease');
    await harness().writeMessageResultEvidence(pendingEvidence(closedInput));
    await store.db.none(
      `UPDATE "${schemaName}"."${TABLE_HARNESS_SESSIONS}" SET closed_at = $3 WHERE harness_name = $1 AND id = $2`,
      [HARNESS, closedSession.id, Date.now()],
    );
    await expect(stamp(closedInput, 'owner-c')).rejects.toBeInstanceOf(HarnessStorageSessionClosedError);
    expect(await dispatchState(closedInput)).toBe('unstamped');
  });

  it('holds the lease in place until the stamp commits, so an adopter sees the claim', async () => {
    // A lease that lapses untaken still names its owner, so the stamp applies;
    // an adoption racing it waits on the session row until the stamp commits.
    const session = await createNativeSession(harness(), 'lease-wait', 'owner-wait', 50);
    const input = admissionFor(session, 'lease-wait');
    await harness().writeMessageResultEvidence(pendingEvidence(input));
    await new Promise(resolve => setTimeout(resolve, 100));

    let releaseRow!: () => void;
    const rowReleased = new Promise<void>(resolve => (releaseRow = resolve));
    let rowLocked!: () => void;
    const locked = new Promise<void>(resolve => (rowLocked = resolve));
    const holder = store.db.tx(async t => {
      await t.one(
        `SELECT id FROM "${schemaName}"."${TABLE_HARNESS_MESSAGE_RESULTS}"
          WHERE harness_name = $1 AND session_id = $2 AND signal_id = $3 FOR UPDATE`,
        [HARNESS, session.id, input.signalId],
      );
      rowLocked();
      await rowReleased;
    });
    await locked;

    const order: string[] = [];
    const stamped = stamp(input, 'owner-wait').then(result => {
      order.push('stamp');
      return result;
    });
    const waitForLockWaiters = async (count: number) => {
      for (let attempt = 0; attempt < 300; attempt++) {
        const waiting = await store.db.one<{ count: number }>(
          `SELECT COUNT(*)::int AS count FROM pg_stat_activity
            WHERE datname = current_database() AND wait_event_type = 'Lock'`,
        );
        if (waiting.count >= count) return;
        await new Promise(resolve => setTimeout(resolve, 10));
      }
      throw new Error(`expected ${count} lock waiters`);
    };
    await waitForLockWaiters(1);
    const adopted = harness()
      .acquireSessionLease({ sessionId: session.id, ownerId: 'adopter', ttlMs: 60_000 })
      .then(() => order.push('adopt'));
    await waitForLockWaiters(2);
    releaseRow();
    await holder;

    await expect(stamped).resolves.toMatchObject({ applied: true });
    await adopted;
    expect(order).toEqual(['stamp', 'adopt']);
    expect(await dispatchState(input)).toBe('dispatching');
    // From here on the stale owner is refused.
    const next = admissionFor(session, 'lease-wait-next');
    await harness().writeMessageResultEvidence(pendingEvidence(next));
    await expect(stamp(next, 'owner-wait')).rejects.toBeInstanceOf(HarnessStorageLeaseConflictError);
  });

  it('lets exactly one of a racing stamp and recovery settlement win, without deadlock', async () => {
    const session = await createNativeSession(harness(), 'stamp-race', 'owner-race');
    for (let round = 0; round < 10; round++) {
      const input = admissionFor(session, `stamp-race-${round}`);
      await harness().writeMessageResultEvidence(pendingEvidence(input));
      const [stamped, settled] = await Promise.all([
        stamp(input, 'owner-race'),
        harness().compareAndSwapSignalTerminal({
          harnessName: HARNESS,
          sessionId: input.sessionId,
          resourceId: input.resourceId,
          threadId: input.threadId,
          signalId: input.signalId,
          admissionId: input.admissionId,
          admissionHash: input.admissionHash,
          operationKind: 'message',
          expected: { state: 'reserved' },
          leaseOwner: { ownerId: 'owner-race' },
          terminal: { status: 'failed', signalId: input.signalId, runId: input.runId, error: INTERRUPTED },
          updatedAt: Date.now(),
        }),
      ]);
      expect([stamped.applied, settled.applied].filter(Boolean)).toHaveLength(1);
      const evidence = await harness().loadMessageResultEvidence({
        harnessName: HARNESS,
        sessionId: input.sessionId,
        resourceId: input.resourceId,
        threadId: input.threadId,
        signalId: input.signalId,
      });
      expect(evidence).toMatchObject(
        stamped.applied ? { status: 'pending' } : { status: 'failed', error: INTERRUPTED },
      );
    }
  });
});
