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

import { setupHarness } from './__test-utils__/setup';
import {
  HarnessConfigError,
  HarnessSessionClosedError,
  HarnessSessionLockedError,
  HarnessStorageError,
  HarnessValidationError,
} from './errors';
import type { Session } from './session';

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
