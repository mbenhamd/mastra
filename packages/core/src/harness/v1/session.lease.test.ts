/**
 * Lease renewal and extension coverage for Harness v1 sessions.
 *
 * The Harness owns a periodic heartbeat for normal live sessions. `extendLease`
 * is the explicit tool/runtime escape hatch for work that may exceed the
 * default lease TTL or block the event loop long enough to miss a heartbeat.
 */

import { describe, expect, it, vi } from 'vitest';

import { HarnessStorageLeaseConflictError } from '../../storage/domains/harness/base';
import { InMemoryHarness } from '../../storage/domains/harness/inmemory';
import { InMemoryDB } from '../../storage/domains/inmemory-db';
import type { Workspace } from '../../workspace';

import { setupHarness } from './__test-utils__/setup';
import {
  HarnessConfigError,
  HarnessSessionClosedError,
  HarnessSessionLockedError,
  HarnessStorageError,
  HarnessValidationError,
} from './errors';
import type { Session } from './session';
import type { WorkspaceProvider } from './workspace-provider';

interface SessionLeaseInternals {
  _leaseExtensionDeadline?: number;
  _beginClosing(): void;
  _getEffectiveLeaseTtlMs(defaultTtlMs: number): number;
  _enqueueLeaseRenewal(run: () => Promise<void>): Promise<void>;
}

function asInternals(session: Session): SessionLeaseInternals {
  return session as unknown as SessionLeaseInternals;
}

describe('Session.extendLease', () => {
  it('rejects invalid ttl values', async () => {
    const { harness } = setupHarness();
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });

    await expect(session.extendLease({ ttlMs: Number.NaN })).rejects.toBeInstanceOf(HarnessValidationError);
    await expect(session.extendLease({ ttlMs: Number.POSITIVE_INFINITY })).rejects.toBeInstanceOf(
      HarnessValidationError,
    );
    await expect(session.extendLease({ ttlMs: 0 })).rejects.toBeInstanceOf(HarnessValidationError);
    await expect(session.extendLease({ ttlMs: -10 })).rejects.toBeInstanceOf(HarnessValidationError);
    await expect(session.extendLease({ ttlMs: 1.5 })).rejects.toBeInstanceOf(HarnessValidationError);
    await expect(session.extendLease({ ttlMs: 25 * 60 * 60 * 1_000 })).rejects.toBeInstanceOf(HarnessValidationError);
  });

  it('renews storage and records a deadline that heartbeat renewal respects', async () => {
    const { harness, storage } = setupHarness();
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });

    const before = (await storage.loadSession({ sessionId: session.id }))!.leaseExpiresAt!;
    await session.extendLease({ ttlMs: 5 * 60_000 });
    const afterExtend = (await storage.loadSession({ sessionId: session.id }))!.leaseExpiresAt!;

    expect(afterExtend).toBeGreaterThan(before);
    expect(asInternals(session)._leaseExtensionDeadline).toBe(afterExtend);
    expect(asInternals(session)._getEffectiveLeaseTtlMs(30_000)).toBeGreaterThan(30_000);
  });

  it('emits a storage_error event for a swallowed background lease-renewal failure (§10.2)', async () => {
    const { harness, storage } = setupHarness();
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    const events: Array<{ type: string; operation?: string; retryable?: boolean; error?: { code: string } }> = [];
    harness.subscribe(e => events.push(e as never));

    // A non-conflict storage failure during background renewal never reaches a
    // caller — it must surface as a storage_error observer event. §5.8: the
    // background sweep renews via the subtree primitive (root + descendants).
    storage.renewSessionLeaseSubtree = async () => {
      throw new HarnessStorageError({
        operation: 'session_lease_renew',
        cause: new Error('db unavailable'),
        retryable: true,
        sessionId: session.id,
      });
    };
    await (
      asInternals(harness) as { _renewLiveSessionLeaseSubtree: (s: unknown, sess: unknown) => Promise<void> }
    )._renewLiveSessionLeaseSubtree(storage, session);

    const storageError = events.find(e => e.type === 'storage_error');
    expect(storageError).toBeDefined();
    expect(storageError!.operation).toBe('session_lease_renew');
    expect(storageError!.retryable).toBe(true);
    expect(storageError!.error?.code).toBe('harness.storage');
  });

  it('does not shrink an active longer extension with a shorter follow-up', async () => {
    const { harness, storage } = setupHarness();
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });

    await session.extendLease({ ttlMs: 10 * 60_000 });
    const afterLong = (await storage.loadSession({ sessionId: session.id }))!.leaseExpiresAt!;
    await session.extendLease({ ttlMs: 60_000 });
    const afterShort = (await storage.loadSession({ sessionId: session.id }))!.leaseExpiresAt!;

    expect(afterShort).toBeGreaterThanOrEqual(afterLong - 2_000);
  });

  it('withExtendedLease invokes the wrapped function and returns its result', async () => {
    const { harness } = setupHarness();
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });

    await expect(session.withExtendedLease(async () => 42, { ttlMs: 60_000 })).resolves.toBe(42);
  });

  it('throws if an explicit extension is queued after close', async () => {
    const { harness } = setupHarness();
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });

    await session.close();

    await expect(session.extendLease({ ttlMs: 60_000 })).rejects.toBeInstanceOf(HarnessSessionClosedError);
  });

  it('allows explicit extension while a closing session drains admitted work', async () => {
    const { harness } = setupHarness();
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    asInternals(session)._beginClosing();

    await expect(session.extendLease({ ttlMs: 60_000 })).resolves.toBeUndefined();
  });

  it('evicts the local session if explicit extension observes lease loss', async () => {
    const { harness, storage } = setupHarness();
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await storage.releaseSessionLease({ sessionId: session.id, harnessName: 'default', ownerId: harness.ownerId });
    await storage.acquireSessionLease({
      sessionId: session.id,
      harnessName: 'default',
      ownerId: 'other-process',
      ttlMs: 30_000,
    });
    const events: string[] = [];
    const unsubscribe = session.subscribe(event => events.push(event.type));

    await expect(session.extendLease({ ttlMs: 60_000 })).rejects.toBeInstanceOf(HarnessSessionLockedError);

    unsubscribe();
    expect(events).toContain('session_evicted');
    expect(harness._internalLiveSessionCount()).toBe(0);
    expect(session.lifecycleState).toBe('evicted');
  });

  it('blocks stale writes after the local lease has expired', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(2_000_000);
    try {
      const { harness } = setupHarness();
      const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
      vi.setSystemTime(2_031_000);

      await expect(session.setState({ stale: true })).rejects.toBeInstanceOf(HarnessSessionLockedError);
      expect(session.lifecycleState).toBe('evicted');
      expect(harness._internalLiveSessionCount()).toBe(0);
    } finally {
      vi.useRealTimers();
    }
  });
});

describe('Lease heartbeat coordination', () => {
  it('renews live session leases before the TTL expires and stops after shutdown', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(1_000_000);
    try {
      const { harness, storage } = setupHarness();
      const renew = vi.spyOn(storage, 'renewSessionLeaseSubtree');
      const session = await harness.session({ threadId: 't1', resourceId: 'r1' });

      await vi.advanceTimersByTimeAsync(10_000);

      // §5.8: a live root renews its whole subtree via renewSessionLeaseSubtree.
      expect(renew).toHaveBeenCalledWith({
        harnessName: 'default',
        rootSessionId: session.id,
        ownerId: harness.ownerId,
        ttlMs: 30_000,
      });
      expect(session.getRecord().leaseExpiresAt).toBe(1_040_000);

      const renewCount = renew.mock.calls.length;
      await harness.shutdown();
      await vi.advanceTimersByTimeAsync(30_000);
      expect(renew).toHaveBeenCalledTimes(renewCount);
    } finally {
      vi.useRealTimers();
    }
  });

  it('uses the extended TTL when the heartbeat renews during an active extension', async () => {
    const { harness, storage } = setupHarness();
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await session.extendLease({ ttlMs: 5 * 60_000 });
    const extendedAt = (await storage.loadSession({ sessionId: session.id }))!.leaseExpiresAt!;

    await asInternals(session)._enqueueLeaseRenewal(async () => {
      const effectiveTtl = asInternals(session)._getEffectiveLeaseTtlMs(30_000);
      const lease = await storage.renewSessionLease({
        harnessName: session.getRecord().harnessName,
        sessionId: session.id,
        ownerId: session.getRecord().ownerId!,
        ttlMs: effectiveTtl,
      });
      session._markLeaseRenewed(lease.expiresAt);
    });

    const afterHeartbeat = (await storage.loadSession({ sessionId: session.id }))!.leaseExpiresAt!;
    expect(afterHeartbeat).toBeGreaterThanOrEqual(extendedAt - 2_000);
  });

  it('keeps cancelled live sessions renewed until they close', async () => {
    const { harness, session, storage } = await (async () => {
      const setup = setupHarness();
      const session = await setup.harness.session({ resourceId: 'u', threadId: { fresh: true } });
      return { ...setup, session };
    })();
    await session.cancel({ reason: 'no-renew' });
    const renew = vi.spyOn(storage, 'renewSessionLeaseSubtree');

    await (harness as unknown as { _renewLiveSessionLeases(): Promise<void> })._renewLiveSessionLeases();

    expect(renew).toHaveBeenCalledWith({
      harnessName: 'default',
      rootSessionId: session.id,
      ownerId: harness.ownerId,
      ttlMs: 30_000,
    });
  });

  it('renews a child subtree through the root and applies the subtree expiry to live descendants (§5.8)', async () => {
    const { harness, storage } = setupHarness();
    const parent = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    const child = await harness.session({
      resourceId: 'u',
      threadId: { fresh: true },
      parentSessionId: parent.id,
    } as never);

    const subtree = vi.spyOn(storage, 'renewSessionLeaseSubtree');
    const perSession = vi.spyOn(storage, 'renewSessionLease');

    await (harness as unknown as { _renewLiveSessionLeases(): Promise<void> })._renewLiveSessionLeases();

    // §5.8: the root renews the whole subtree; the child is never renewed on its
    // own row, and gets the same atomic expiry as the root.
    expect(subtree).toHaveBeenCalledWith(
      expect.objectContaining({ rootSessionId: parent.id, ownerId: harness.ownerId }),
    );
    expect(subtree.mock.calls.some(([arg]) => (arg as { rootSessionId: string }).rootSessionId === child.id)).toBe(
      false,
    );
    expect(perSession).not.toHaveBeenCalled();
    expect(child.getRecord().leaseExpiresAt).toBe(parent.getRecord().leaseExpiresAt);
  });

  it('routes child extendLease through the root subtree, never the child row (§5.8)', async () => {
    const { harness, storage } = setupHarness();
    const parent = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    const child = await harness.session({
      resourceId: 'u',
      threadId: { fresh: true },
      parentSessionId: parent.id,
    } as never);

    const subtree = vi.spyOn(storage, 'renewSessionLeaseSubtree');
    const perSession = vi.spyOn(storage, 'renewSessionLease');

    await child.extendLease({ ttlMs: 5 * 60_000 });

    expect(subtree).toHaveBeenCalledWith(
      expect.objectContaining({ rootSessionId: parent.id, ownerId: harness.ownerId }),
    );
    expect(perSession).not.toHaveBeenCalled();
  });

  it('fences the whole live subtree when root renewal observes a lease conflict (§5.8)', async () => {
    const { harness, storage } = setupHarness();
    const parent = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    const child = await harness.session({
      resourceId: 'u',
      threadId: { fresh: true },
      parentSessionId: parent.id,
    } as never);
    expect(harness._internalLiveSessionCount()).toBe(2);

    // The subtree renewal loses ownership / observes a split — both the root and
    // the live child must be fenced, not just the root.
    storage.renewSessionLeaseSubtree = async () => {
      throw new HarnessStorageLeaseConflictError(parent.id, 'other-process', Date.now() + 30_000);
    };

    await (harness as unknown as { _renewLiveSessionLeases(): Promise<void> })._renewLiveSessionLeases();

    expect(parent.lifecycleState).toBe('evicted');
    expect(child.lifecycleState).toBe('evicted');
    expect(harness._internalLiveSessionCount()).toBe(0);
  });

  it('restarts the heartbeat when shutdown fails and restores live sessions', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(3_000_000);
    try {
      const { harness, storage } = setupHarness();
      const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
      const append = vi.spyOn(storage, 'appendSessionEvent').mockRejectedValueOnce(new Error('append failed'));

      await expect(harness.shutdown()).rejects.toMatchObject({
        name: 'HarnessStorageError',
        cause: expect.objectContaining({ message: 'append failed' }),
      });
      append.mockRestore();
      expect(session.lifecycleState).toBe('live');

      const renew = vi.spyOn(storage, 'renewSessionLeaseSubtree');
      await vi.advanceTimersByTimeAsync(10_000);

      expect(renew).toHaveBeenCalledWith({
        harnessName: 'default',
        rootSessionId: session.id,
        ownerId: harness.ownerId,
        ttlMs: 30_000,
      });
    } finally {
      vi.useRealTimers();
    }
  });

  it('evicts a live session fail-closed when renewal observes lease loss', async () => {
    const { harness, session, storage } = await (async () => {
      const setup = setupHarness();
      const session = await setup.harness.session({ resourceId: 'u', threadId: { fresh: true } });
      return { ...setup, session };
    })();
    const stored = (await storage.loadSession({ sessionId: session.id, harnessName: 'default' }))!;
    await storage.releaseSessionLease({ sessionId: session.id, harnessName: 'default', ownerId: harness.ownerId });
    await storage.acquireSessionLease({
      sessionId: session.id,
      harnessName: 'default',
      ownerId: 'other-process',
      ttlMs: 30_000,
    });
    const events: string[] = [];
    const unsubscribe = session.subscribe(event => events.push(event.type));

    await (harness as unknown as { _renewLiveSessionLeases(): Promise<void> })._renewLiveSessionLeases();

    unsubscribe();
    expect(events).toContain('session_evicted');
    expect(harness._internalLiveSessionCount()).toBe(0);
    expect(session.lifecycleState).toBe('evicted');
    await expect(storage.loadSession({ sessionId: stored.id, harnessName: 'default' })).resolves.toMatchObject({
      ownerId: 'other-process',
    });
  });
});

describe('Lease takeover contention', () => {
  it('blocks a second Harness while an extended lease is active', async () => {
    const sharedStorage = new InMemoryHarness({ db: new InMemoryDB() });
    const { harness } = setupHarness({ sessions: { storage: sharedStorage } });
    const session = await harness.session({ threadId: 't-lease', resourceId: 'u' });

    await session.extendLease({ ttlMs: 5 * 60_000 });

    const { harness: otherHarness } = setupHarness({ sessions: { storage: sharedStorage } });
    await expect(otherHarness.session({ sessionId: session.id })).rejects.toBeInstanceOf(HarnessSessionLockedError);
  });

  it('lets a fresh Harness take over once the prior owner lease has expired', async () => {
    const db = new InMemoryDB();
    const storage = new InMemoryHarness({ db });
    const { harness } = setupHarness({ sessions: { storage } });
    const session = await harness.session({ threadId: 't-expired', resourceId: 'u' });
    const stored = db.harnessSessions.get(`default\u0000${session.id}`);
    if (!stored) throw new Error('expected stored session');
    db.harnessSessions.set(`default\u0000${session.id}`, { ...stored, leaseExpiresAt: Date.now() - 60_000 });

    const { harness: otherHarness } = setupHarness({ sessions: { storage } });
    const taken = await otherHarness.session({ sessionId: session.id });

    expect(taken.id).toBe(session.id);
    expect(taken._internalOwnerId).toBe(otherHarness.ownerId);
  });

  it('lockMode "wait" blocks then takes over when the prior lease expires within the wait window (§5.8)', async () => {
    const db = new InMemoryDB();
    const storage = new InMemoryHarness({ db });
    const { harness } = setupHarness({ sessions: { storage } });
    const session = await harness.session({ threadId: 't-wait', resourceId: 'u' });

    // Hold the lease but make it expire ~30ms out; the prior owner renews on the
    // 10s keep-alive cadence, so it will not refresh within the wait window.
    const key = `default ${session.id}`;
    const stored = db.harnessSessions.get(key);
    if (!stored) throw new Error('expected stored session');
    db.harnessSessions.set(key, { ...stored, leaseExpiresAt: Date.now() + 30 });

    const { harness: waiter } = setupHarness({ sessions: { storage, lockMode: 'wait', lockWaitMs: 2_000 } });
    const taken = await waiter.session({ sessionId: session.id });

    expect(taken.id).toBe(session.id);
    expect(taken._internalOwnerId).toBe(waiter.ownerId);
  });

  it('lockMode "wait" still fails closed when the wait window elapses with the lease held (§5.8)', async () => {
    const sharedStorage = new InMemoryHarness({ db: new InMemoryDB() });
    const { harness } = setupHarness({ sessions: { storage: sharedStorage } });
    const session = await harness.session({ threadId: 't-wait-fail', resourceId: 'u' });
    await session.extendLease({ ttlMs: 5 * 60_000 });

    const { harness: waiter } = setupHarness({
      sessions: { storage: sharedStorage, lockMode: 'wait', lockWaitMs: 40 },
    });
    await expect(waiter.session({ sessionId: session.id })).rejects.toBeInstanceOf(HarnessSessionLockedError);
  });

  it('rejects lockMode "steal" at construction until the operator fence is implemented (§5.8)', () => {
    // `'steal'` is reserved but not implemented; the public API must reject it
    // rather than silently degrade to `'fail'`.
    expect(() =>
      setupHarness({
        sessions: { storage: new InMemoryHarness({ db: new InMemoryDB() }), lockMode: 'steal' as never },
      }),
    ).toThrow(HarnessConfigError);
  });

  it('recovers from a cross-owner CAS conflict by re-applying once under proven ownership (§5.8)', async () => {
    const db = new InMemoryDB();
    const storage = new InMemoryHarness({ db });
    const { harness } = setupHarness({ sessions: { storage } });
    const session = await harness.session({ threadId: 't-cas', resourceId: 'u' });

    const key = [...db.harnessSessions.keys()].find(k => k.endsWith(session.id));
    if (!key) throw new Error('expected stored session');
    const before = db.harnessSessions.get(key)!;

    // Simulate a concurrent durable write bumping the version under the same
    // owner: the flush's CAS fails once, ownership renewal proves we still hold
    // the lease, and the pure updater re-applies against the reloaded state.
    db.harnessSessions.set(key, { ...before, version: before.version + 1, state: { concurrent: true } });

    await (session as unknown as { _flushUpdate(u: (p: any) => any): Promise<void> })._flushUpdate(p => ({
      ...p,
      state: { ...((p.state as Record<string, unknown>) ?? {}), mine: true },
    }));

    const after = db.harnessSessions.get(key)!;
    expect(after.version).toBe(before.version + 2);
    expect((after.state as Record<string, unknown> | undefined)?.concurrent).toBe(true);
    expect((after.state as Record<string, unknown> | undefined)?.mine).toBe(true);
  });

  it('fences the session when a CAS conflict coincides with lease loss (§5.8)', async () => {
    const db = new InMemoryDB();
    const storage = new InMemoryHarness({ db });
    const { harness } = setupHarness({ sessions: { storage } });
    const session = await harness.session({ threadId: 't-cas-lost', resourceId: 'u' });

    const key = [...db.harnessSessions.keys()].find(k => k.endsWith(session.id));
    if (!key) throw new Error('expected stored session');
    const before = db.harnessSessions.get(key)!;

    // Bump the version AND hand the lease to a different owner: the flush's CAS
    // fails, renewal cannot prove ownership, and the session is fenced.
    db.harnessSessions.set(key, {
      ...before,
      version: before.version + 1,
      ownerId: 'other-owner',
      leaseExpiresAt: Date.now() + 5 * 60_000,
    });

    await expect(
      (session as unknown as { _flushUpdate(u: (p: any) => any): Promise<void> })._flushUpdate(p => ({
        ...p,
        state: { ...((p.state as Record<string, unknown>) ?? {}), mine: true },
      })),
    ).rejects.toBeInstanceOf(HarnessSessionLockedError);
  });
});

describe('pre-eviction warning (§O3)', () => {
  it('emits session_eviction_warning immediately before a pressure eviction', async () => {
    const { harness } = setupHarness({ sessions: { maxLive: 1 } });
    const events: Array<{ type: string; reason?: string; sessionId?: string }> = [];
    harness.subscribe(e => events.push({ type: e.type, reason: (e as any).reason, sessionId: e.sessionId }));

    const a = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    // Creating a second session exceeds maxLive=1 → evicts A under pressure.
    await harness.session({ resourceId: 'u', threadId: { fresh: true } });

    const forA = events.filter(
      e => e.sessionId === a.id && (e.type === 'session_eviction_warning' || e.type === 'session_evicted'),
    );
    expect(forA.map(e => e.type)).toEqual(['session_eviction_warning', 'session_evicted']);
    expect(forA.every(e => e.reason === 'pressure')).toBe(true);
  });

  it('does NOT warn for a forced lease_lost eviction', async () => {
    const { harness, storage } = setupHarness();
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    const events: string[] = [];
    session.subscribe(e => events.push(e.type));
    // Steal the lease so the next extension observes lease loss → forced eviction.
    await storage.releaseSessionLease({ sessionId: session.id, harnessName: 'default', ownerId: harness.ownerId });
    await storage.acquireSessionLease({
      sessionId: session.id,
      harnessName: 'default',
      ownerId: 'other',
      ttlMs: 30_000,
    });
    await expect(session.extendLease({ ttlMs: 60_000 })).rejects.toBeInstanceOf(HarnessSessionLockedError);

    expect(events).toContain('session_evicted');
    expect(events).not.toContain('session_eviction_warning');
  });
});

describe('subtree lease renewal with a revocation-fenced descendant', () => {
  it('keeps the root renewable and leaves the fenced child lapsed', async () => {
    const db = new InMemoryDB();
    const storage = new InMemoryHarness({ db, terminalHandoff: { enabled: true } });
    const { harness } = setupHarness({ sessions: { storage } });
    const root = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    const rootRecord = (await storage.loadSession({ sessionId: root.id }))!;
    await storage.createOrLoadActiveSession(
      {
        ...rootRecord,
        id: 'fenced-child',
        threadId: 'thread-fenced-child',
        parentSessionId: root.id,
        origin: 'subagent',
        version: 0,
      },
      { initialLease: { ownerId: harness.ownerId, ttlMs: 60_000 } },
    );
    const key = [...db.harnessSessions.keys()].find(k => k.endsWith('fenced-child'))!;
    const lapsedAt = Date.now() - 1_000;
    db.harnessSessions.set(key, { ...db.harnessSessions.get(key)!, leaseExpiresAt: lapsedAt });
    await expect(
      storage.revokeTerminalGrant({
        harnessName: rootRecord.harnessName,
        sessionId: 'fenced-child',
        admissionId: 'admission-fenced-child',
        executionGrant: { key: 'grant-fenced-child', generation: 1 },
        reason: { code: 'doxa.turn_released', message: 'owner lapsed' },
        requireOwnerLeaseLapsed: { ownerId: harness.ownerId },
      }),
    ).resolves.toMatchObject({ status: 'revoked' });

    for (let pass = 0; pass < 3; pass++) {
      await root.extendLease({ ttlMs: 5 * 60_000 });
      expect(root.lifecycleState).toBe('live');
      expect((await storage.loadSession({ sessionId: 'fenced-child' }))!.leaseExpiresAt).toBe(lapsedAt);
    }
    expect([...db.harnessTerminalTombstones.values()].filter(t => t.sessionId === 'fenced-child')).toHaveLength(1);
  });

  it('fences a live child the renewal left out, so its lease proof fails until it is re-acquired', async () => {
    const db = new InMemoryDB();
    const storage = new InMemoryHarness({ db, terminalHandoff: { enabled: true } });
    const { harness } = setupHarness({ sessions: { storage } });
    const root = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    const child = await harness.session({ resourceId: 'u', threadId: { fresh: true }, parentSessionId: root.id });
    const key = [...db.harnessSessions.keys()].find(k => k.endsWith(child.id))!;
    const lapsedAt = Date.now() - 1_000;
    db.harnessSessions.set(key, { ...db.harnessSessions.get(key)!, leaseExpiresAt: lapsedAt });
    const childRecord = (await storage.loadSession({ sessionId: child.id }))!;
    await expect(
      storage.revokeTerminalGrant({
        harnessName: childRecord.harnessName,
        sessionId: child.id,
        admissionId: 'admission-revoked',
        executionGrant: { key: 'grant-revoked', generation: 1 },
        reason: { code: 'doxa.turn_released', message: 'owner lapsed' },
        requireOwnerLeaseLapsed: { ownerId: harness.ownerId },
      }),
    ).resolves.toMatchObject({ status: 'revoked' });

    await root.extendLease({ ttlMs: 5 * 60_000 });
    expect(root.lifecycleState).toBe('live');
    expect(child.lifecycleState).toBe('evicted');
    const callback = vi.fn(async () => 'ran');
    await expect(child.withExtendedLease(callback, { ttlMs: 60_000 })).rejects.toThrow();
    expect(callback).not.toHaveBeenCalled();
    expect((await storage.loadSession({ sessionId: child.id }))!.leaseExpiresAt).toBe(lapsedAt);

    // Re-acquiring the child gives it a fresh lease; a fresh grant is admitted.
    const readopted = await harness.session({ sessionId: child.id });
    expect(readopted.lifecycleState).toBe('live');
    const record = (await storage.loadSession({ sessionId: child.id }))!;
    const fresh = {
      harnessName: record.harnessName,
      sessionId: record.id,
      resourceId: record.resourceId,
      threadId: record.threadId,
      sessionIncarnation: record.sessionIncarnation!,
      admissionId: 'admission-fresh',
      admissionHash: 'admission-hash-fresh',
      signalId: 'signal-fresh',
      runId: 'run-fresh',
      executionGrant: { key: 'grant-fresh', generation: 1 },
      finalizerId: 'doxa.chat',
      finalizerVersion: '1',
      seed: { admissionId: 'admission-fresh' },
      createdAt: Date.now(),
    };
    await storage.writeMessageResultEvidence({
      status: 'pending',
      signalId: fresh.signalId,
      runId: fresh.runId,
      operationKind: 'message',
      admissionId: fresh.admissionId,
      admissionHash: fresh.admissionHash,
      harnessName: fresh.harnessName,
      sessionId: fresh.sessionId,
      resourceId: fresh.resourceId,
      threadId: fresh.threadId,
      createdAt: Date.now(),
      updatedAt: Date.now(),
    });
    await expect(
      storage.admitTerminalHandoff(fresh, { leaseOwner: { ownerId: harness.ownerId } }),
    ).resolves.toMatchObject({ status: 'created' });
  });

  async function fencedChildFixture(opts: Parameters<typeof setupHarness>[0] = {}) {
    const db = new InMemoryDB();
    const storage = new InMemoryHarness({ db, terminalHandoff: { enabled: true } });
    const { harness, agent } = setupHarness({ ...opts, sessions: { storage } });
    const root = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    const sibling = await harness.session({ resourceId: 'u', threadId: { fresh: true }, parentSessionId: root.id });
    const child = await harness.session({ resourceId: 'u', threadId: { fresh: true }, parentSessionId: root.id });
    const grandchild = await harness.session({
      resourceId: 'u',
      threadId: { fresh: true },
      parentSessionId: child.id,
    });
    const fence = async () => {
      const key = [...db.harnessSessions.keys()].find(k => k.endsWith(child.id))!;
      db.harnessSessions.set(key, { ...db.harnessSessions.get(key)!, leaseExpiresAt: Date.now() - 1_000 });
      await expect(
        storage.revokeTerminalGrant({
          harnessName: child.getRecord().harnessName,
          sessionId: child.id,
          admissionId: 'admission-revoked',
          executionGrant: { key: 'grant-revoked', generation: 1 },
          reason: { code: 'doxa.turn_released', message: 'owner lapsed' },
          requireOwnerLeaseLapsed: { ownerId: harness.ownerId },
        }),
      ).resolves.toMatchObject({ status: 'revoked' });
      return key;
    };
    return { db, storage, harness, agent, root, sibling, child, grandchild, fence };
  }

  const settlesWithin = <T>(promise: Promise<T>, ms: number) =>
    Promise.race([
      promise.then(
        () => 'settled' as const,
        () => 'settled' as const,
      ),
      new Promise<'pending'>(resolve => setTimeout(() => resolve('pending'), ms)),
    ]);

  it('does not deadlock when the fenced child proves its lease from its own draft checkpoint', async () => {
    const { db, root, child, grandchild, fence } = await fencedChildFixture();
    const key = await fence();
    // Another same-owner write advanced the child's stored version, so its next
    // draft checkpoint conflicts and proves the subtree lease through the root.
    db.harnessSessions.set(key, { ...db.harnessSessions.get(key)!, version: db.harnessSessions.get(key)!.version + 1 });
    const internals = child as unknown as {
      _recordAssistantDraftDelta(opts: { runId: string; kind: 'text'; delta: string }): void;
      _startAssistantDraftFlush(): Promise<void>;
    };
    internals._recordAssistantDraftDelta({ runId: 'run-draft', kind: 'text', delta: 'partial answer' });
    const checkpoint = internals._startAssistantDraftFlush();

    expect(await settlesWithin(checkpoint, 2_000)).toBe('settled');
    await expect(checkpoint).rejects.toBeInstanceOf(HarnessSessionLockedError);
    expect(child.lifecycleState).toBe('evicted');
    expect(grandchild.lifecycleState).toBe('evicted');
    expect(await settlesWithin(root.extendLease({ ttlMs: 5 * 60_000 }), 2_000)).toBe('settled');
    expect(root.lifecycleState).toBe('live');
  });

  it.each(['root', 'sibling'] as const)(
    'renews for a %s promptly while the fenced child workspace teardown is slow, and runs no callback after a takeover',
    async target => {
      let releaseDestroy!: () => void;
      const destroyGate = new Promise<void>(resolve => (releaseDestroy = resolve));
      const workspace = {
        id: 'ws',
        name: 'ws',
        status: 'ready',
        createdAt: new Date(),
        lastAccessedAt: new Date(),
        async init() {},
        async destroy() {
          await destroyGate;
        },
      } as unknown as Workspace;
      const provider: WorkspaceProvider = {
        providerId: 'slow-destroy',
        resumable: true,
        create: async () => workspace,
        resume: async () => workspace,
      };
      const { db, storage, harness, root, sibling, child, grandchild, fence } = await fencedChildFixture({
        workspace: { kind: 'per-session', provider },
      });
      await child.getWorkspace();
      await fence();

      const ownersSeenByCallback: Array<string | undefined> = [];
      const subject = target === 'root' ? root : sibling;
      const extended = subject.withExtendedLease(
        async () => {
          ownersSeenByCallback.push((await storage.loadSession({ sessionId: root.id }))!.ownerId);
        },
        { ttlMs: 60_000 },
      );
      try {
        expect(await settlesWithin(extended, 1_000)).toBe('settled');
        expect(child.lifecycleState).toBe('evicted');
        expect(grandchild.lifecycleState).toBe('evicted');

        // The root lease then lapses and another owner takes it while the
        // child's teardown is still stuck.
        const rootKey = [...db.harnessSessions.keys()].find(k => k.endsWith(root.id))!;
        db.harnessSessions.set(rootKey, { ...db.harnessSessions.get(rootKey)!, leaseExpiresAt: Date.now() - 1 });
        await storage.acquireSessionLease({ sessionId: root.id, ownerId: 'owner-b', ttlMs: 60_000 });
      } finally {
        releaseDestroy();
      }
      await extended.catch(() => {});
      await new Promise(resolve => setTimeout(resolve, 50));
      // The callback ran once, while this harness still held the root lease.
      expect(ownersSeenByCallback).toEqual([harness.ownerId]);
    },
  );

  it('lets an abort listener of the fenced child resolve its thread only to a fresh session', async () => {
    const { harness, agent, root, child, fence } = await fencedChildFixture();
    let releaseRun!: () => void;
    let resolvedInListener: Promise<Session> | undefined;
    agent.runs.push({
      holdUntil: new Promise<void>(resolve => (releaseRun = resolve)),
      onAbort: () => {
        resolvedInListener ??= harness.session({ resourceId: child.resourceId, threadId: child.threadId });
      },
    });
    const turn = child.message({ content: 'long work' });
    await vi.waitFor(() => expect(agent.streamCalls).toHaveLength(1));
    await fence();

    await root.extendLease({ ttlMs: 5 * 60_000 });
    expect(child.lifecycleState).toBe('evicted');
    expect(resolvedInListener).toBeDefined();
    const resolved = await resolvedInListener!;
    expect(resolved).not.toBe(child);
    expect(resolved.lifecycleState).toBe('live');
    releaseRun();
    await turn.catch(() => {});
  });

  it('settles a fenced grandchild checkpoint that reaches its lease proof after the fence', async () => {
    const { db, storage, harness, root, grandchild, fence } = await fencedChildFixture();
    await fence();
    let releaseSave!: () => void;
    const saveGate = new Promise<void>(resolve => (releaseSave = resolve));
    const save = storage.saveSession.bind(storage);
    const spy = vi.spyOn(storage, 'saveSession').mockImplementation(async (record, opts) => {
      if (record.id === grandchild.id) await saveGate;
      return save(record, opts);
    });
    // A competing same-owner write advanced the grandchild's stored version.
    const key = [...db.harnessSessions.keys()].find(k => k.endsWith(grandchild.id))!;
    db.harnessSessions.set(key, { ...db.harnessSessions.get(key)!, version: db.harnessSessions.get(key)!.version + 1 });
    const internals = grandchild as unknown as {
      _recordAssistantDraftDelta(opts: { runId: string; kind: 'text'; delta: string }): void;
      _startAssistantDraftFlush(): Promise<void>;
    };
    internals._recordAssistantDraftDelta({ runId: 'run-draft', kind: 'text', delta: 'partial answer' });
    const checkpoint = internals._startAssistantDraftFlush();
    await vi.waitFor(() => expect(spy.mock.calls.some(([record]) => record.id === grandchild.id)).toBe(true));

    // The root renewal fences the revoked child and the grandchild while the
    // grandchild's save is still pending; the save then conflicts.
    await root.extendLease({ ttlMs: 5 * 60_000 });
    expect(grandchild.lifecycleState).toBe('evicted');
    releaseSave();

    expect(await settlesWithin(checkpoint, 2_000)).toBe('settled');
    await expect(checkpoint).rejects.toBeInstanceOf(HarnessSessionLockedError);
    const reacquired = harness.session({ sessionId: grandchild.id });
    expect(await settlesWithin(reacquired, 2_000)).toBe('settled');
    expect((await reacquired).lifecycleState).toBe('live');
    spy.mockRestore();
  });

  it('settles a checkpoint whose proof was queued before the fence when pressure then evicts the root', async () => {
    const db = new InMemoryDB();
    const storage = new InMemoryHarness({ db, terminalHandoff: { enabled: true } });
    const { harness } = setupHarness({ sessions: { storage, maxLive: 3 } });
    const root = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    const child = await harness.session({ resourceId: 'u', threadId: { fresh: true }, parentSessionId: root.id });
    const grandchild = await harness.session({
      resourceId: 'u',
      threadId: { fresh: true },
      parentSessionId: child.id,
    });
    const keyOf = (id: string) => [...db.harnessSessions.keys()].find(k => k.endsWith(id))!;
    db.harnessSessions.set(keyOf(child.id), {
      ...db.harnessSessions.get(keyOf(child.id))!,
      leaseExpiresAt: Date.now() - 1_000,
    });
    await storage.revokeTerminalGrant({
      harnessName: child.getRecord().harnessName,
      sessionId: child.id,
      admissionId: 'admission-revoked',
      executionGrant: { key: 'grant-revoked', generation: 1 },
      reason: { code: 'doxa.turn_released', message: 'owner lapsed' },
      requireOwnerLeaseLapsed: { ownerId: harness.ownerId },
    });

    // Two root renewals whose storage responses the test releases one by one.
    const gates: Array<{ promise: Promise<void>; release: () => void }> = [0, 1].map(() => {
      let release!: () => void;
      return { promise: new Promise<void>(resolve => (release = resolve)), release };
    });
    const renew = storage.renewSessionLeaseSubtree.bind(storage);
    let renewCalls = 0;
    const renewSpy = vi.spyOn(storage, 'renewSessionLeaseSubtree').mockImplementation(async input => {
      const gate = gates[renewCalls++];
      if (gate) await gate.promise;
      return renew(input);
    });
    const first = root.extendLease({ ttlMs: 5 * 60_000 }).catch(() => {});
    await vi.waitFor(() => expect(renewCalls).toBe(1));
    const second = root.extendLease({ ttlMs: 5 * 60_000 }).catch(() => {});

    // The grandchild's checkpoint conflicts and queues its proof behind both.
    db.harnessSessions.set(keyOf(grandchild.id), {
      ...db.harnessSessions.get(keyOf(grandchild.id))!,
      version: db.harnessSessions.get(keyOf(grandchild.id))!.version + 1,
    });
    const internals = grandchild as unknown as {
      _recordAssistantDraftDelta(opts: { runId: string; kind: 'text'; delta: string }): void;
      _startAssistantDraftFlush(): Promise<void>;
    };
    internals._recordAssistantDraftDelta({ runId: 'run-draft', kind: 'text', delta: 'partial answer' });
    const checkpoint = internals._startAssistantDraftFlush();
    await new Promise(resolve => setTimeout(resolve, 20));

    // The first renewal fences the child and grandchild.
    gates[0]!.release();
    await first;
    expect(grandchild.lifecycleState).toBe('evicted');
    await vi.waitFor(() => expect(renewCalls).toBe(2));

    // While the second renewal's response is delayed, pressure evicts the root.
    for (let i = 0; i < 4 && root.lifecycleState !== 'evicted'; i++) {
      await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    }
    await vi.waitFor(() => expect(root.lifecycleState).toBe('evicted'));
    gates[1]!.release();
    await second;

    expect(await settlesWithin(checkpoint, 2_000)).toBe('settled');
    await expect(checkpoint).rejects.toBeInstanceOf(HarnessSessionLockedError);
    renewSpy.mockRestore();
    const reacquired = harness.session({ sessionId: grandchild.id });
    expect(await settlesWithin(reacquired, 2_000)).toBe('settled');
    expect((await reacquired).lifecycleState).toBe('live');
  });

  it('evicts the live grandchild below a fenced child as lease_lost and stops renewing it in storage', async () => {
    const { storage, root, child, grandchild, fence } = await fencedChildFixture();
    await fence();
    const evictions: unknown[] = [];
    grandchild.subscribe(event => {
      if (event.type === 'session_evicted') evictions.push((event as { reason?: unknown }).reason);
    });
    const grandchildExpiry = (await storage.loadSession({ sessionId: grandchild.id }))!.leaseExpiresAt;

    for (let pass = 0; pass < 2; pass++) {
      await root.extendLease({ ttlMs: 5 * 60_000 });
      expect((await storage.loadSession({ sessionId: grandchild.id }))!.leaseExpiresAt).toBe(grandchildExpiry);
    }
    expect(child.lifecycleState).toBe('evicted');
    expect(grandchild.lifecycleState).toBe('evicted');
    await vi.waitFor(() => expect(evictions).toEqual(['lease_lost']));
  });

  it('fences a session created below the fenced child after storage built its report', async () => {
    const { storage, harness, root, child, fence } = await fencedChildFixture();
    await fence();
    const renew = storage.renewSessionLeaseSubtree.bind(storage);
    let late: Session | undefined;
    const spy = vi.spyOn(storage, 'renewSessionLeaseSubtree').mockImplementation(async input => {
      const lease = await renew(input);
      // Admitted under the child after the walk, before the Harness applies it.
      late ??= await harness.session({ resourceId: 'u', threadId: { fresh: true }, parentSessionId: child.id });
      return lease;
    });

    await root.extendLease({ ttlMs: 5 * 60_000 });
    spy.mockRestore();
    expect(late).toBeDefined();
    expect(child.lifecycleState).toBe('evicted');
    expect(late!.lifecycleState).toBe('evicted');
  });

  it('keeps a fenced child that was re-acquired while the renewal was in flight', async () => {
    const { storage, harness, root, child, grandchild, fence } = await fencedChildFixture();
    await fence();
    const renew = storage.renewSessionLeaseSubtree.bind(storage);
    let replacement: Session | undefined;
    const spy = vi.spyOn(storage, 'renewSessionLeaseSubtree').mockImplementation(async input => {
      const lease = await renew(input);
      if (replacement === undefined) {
        // The reported child is evicted and re-acquired with a fresh lease
        // before the stale report reaches the Harness.
        await (harness as unknown as { _evictLiveSession(s: Session, r: 'idle'): Promise<void> })._evictLiveSession(
          child,
          'idle',
        );
        replacement = await harness.session({ sessionId: child.id });
      }
      return lease;
    });

    await root.extendLease({ ttlMs: 5 * 60_000 });
    spy.mockRestore();
    expect(replacement).toBeDefined();
    expect(replacement).not.toBe(child);
    expect(replacement!.lifecycleState).toBe('live');
    // Storage left the grandchild out of this renewal, so it is fenced rather
    // than reported renewed; it is re-acquired with a fresh lease when next used.
    expect(grandchild.lifecycleState).toBe('evicted');
  });
});
