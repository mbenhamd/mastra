import { MastraError } from '@mastra/core/error';
import type { StorageDomains } from '@mastra/core/storage';
import type { Pool, PoolClient, QueryResult } from 'pg';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { PostgresStore } from './index';

type Deferred = {
  promise: Promise<void>;
  resolve: () => void;
};

type InitOutcome =
  | { status: 'fulfilled' }
  | {
      status: 'rejected';
      reason: unknown;
    };

type Checkout = {
  queries: string[];
  release: ReturnType<typeof vi.fn>;
};

function deferred(): Deferred {
  let resolve!: () => void;
  const promise = new Promise<void>(resolvePromise => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

function emptyResult(): QueryResult {
  return {
    rows: [],
    rowCount: 0,
    command: 'QUERY',
    oid: 0,
    fields: [],
  } as QueryResult;
}

function settle(promise: Promise<void>): Promise<InitOutcome> {
  return promise.then(
    () => ({ status: 'fulfilled' as const }),
    reason => ({ status: 'rejected' as const, reason }),
  );
}

function createPool(
  onQuery: (sql: string, checkout: number) => Promise<QueryResult> = async () => emptyResult(),
  onRelease: (checkout: number) => void = () => undefined,
  onBaseQuery: (sql: string) => Promise<QueryResult> = async () => emptyResult(),
): {
  pool: Pool;
  connect: ReturnType<typeof vi.fn>;
  baseQuery: ReturnType<typeof vi.fn>;
  checkouts: Checkout[];
} {
  const checkouts: Checkout[] = [];
  const connect = vi.fn(async () => {
    const checkoutNumber = checkouts.length + 1;
    const checkout: Checkout = {
      queries: [],
      release: vi.fn(() => onRelease(checkoutNumber)),
    };
    checkouts.push(checkout);

    const client = {
      query: vi.fn(async (sql: string) => {
        checkout.queries.push(sql);
        return onQuery(sql, checkoutNumber);
      }),
      release: checkout.release,
    } as unknown as PoolClient;

    return client;
  });

  const baseQuery = vi.fn(async (sql: string) => onBaseQuery(sql));
  const pool = {
    connect,
    query: baseQuery,
  } as unknown as Pool;

  return { pool, connect, baseQuery, checkouts };
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe('PostgresStore pinned init drain', () => {
  it('waits across a real sibling async gap before draining, releasing, and permitting retry', async () => {
    const agentsPaused = deferred();
    const resumeAgents = deferred();
    const agentsTailStarted = deferred();
    const settleAgentsTail = deferred();
    const firstQueryFailed = deferred();
    const firstAgentsAttemptFinished = deferred();
    const events: string[] = [];
    const firstError = new Error('first domain failed');
    const tailError = new Error('queued sibling tail failed');
    const unhandledRejections: unknown[] = [];
    const onUnhandledRejection = (reason: unknown) => unhandledRejections.push(reason);
    const releaseReentries: Promise<void>[] = [];
    const schemaName = 'pf3555_async_gap';
    let agentsTailHandled = false;
    let scoresFailed = false;
    let store!: PostgresStore;

    const runAgentsTail = async (sql: string, route: string): Promise<QueryResult> => {
      const isAgentsTail = sql.includes(`CREATE TABLE IF NOT EXISTS "${schemaName}"."mastra_agent_versions"`);
      if (!isAgentsTail || agentsTailHandled) {
        return emptyResult();
      }

      agentsTailHandled = true;
      events.push(`agents-tail-started-${route}`);
      agentsTailStarted.resolve();
      await settleAgentsTail.promise;
      events.push(`agents-tail-settled-${route}`);
      throw tailError;
    };

    const { pool, connect, baseQuery, checkouts } = createPool(
      async (sql, checkout) => {
        const isScoresCreate = sql.includes(`CREATE TABLE IF NOT EXISTS "${schemaName}"."mastra_scorers"`);
        if (checkout === 1 && isScoresCreate && !scoresFailed) {
          scoresFailed = true;
          events.push('scores-error');
          firstQueryFailed.resolve();
          throw firstError;
        }
        return runAgentsTail(sql, `pinned-${checkout}`);
      },
      checkout => {
        events.push(`release-${checkout}`);
        // release() is synchronous. Re-entering through the public client here
        // mutation-protects the required cleanup order: unpin before release.
        releaseReentries.push(store.db.none(`RELEASE_REENTRY_${checkout}`).then(() => undefined));
      },
      sql => runAgentsTail(sql, 'base-pool'),
    );
    store = new PostgresStore({ id: 'pinned-init-drain', pool, schemaName });

    const scores = store.stores.scores!;
    const agents = store.stores.agents!;
    const scoresInit = vi.spyOn(scores, 'init');
    const originalAgentsInit = agents.init.bind(agents);
    let agentsAttempt = 0;
    const agentsInit = vi.spyOn(agents, 'init').mockImplementation(async () => {
      agentsAttempt += 1;
      const currentAttempt = agentsAttempt;
      try {
        if (currentAttempt === 1) {
          events.push('agents-paused');
          agentsPaused.resolve();
          await resumeAgents.promise;
          events.push('agents-resumed');
        }
        await originalAgentsInit();
      } finally {
        if (currentAttempt === 1) {
          events.push('agents-attempt-finished');
          firstAgentsAttemptFinished.resolve();
        }
      }
    });

    // Keep the production domain map and real Scores/Agents implementations,
    // while silencing unrelated domains so this race has an exact query head.
    for (const domain of Object.values(store.stores)) {
      if (domain && domain !== scores && domain !== agents) {
        vi.spyOn(domain, 'init').mockResolvedValue();
      }
    }

    process.on('unhandledRejection', onUnhandledRejection);

    let firstSettled = false;
    let overlappingSettled = false;
    let firstOutcomePromise: Promise<InitOutcome> | undefined;
    let overlappingOutcomePromise: Promise<InitOutcome> | undefined;
    try {
      firstOutcomePromise = settle(store.init()).then(outcome => {
        firstSettled = true;
        return outcome;
      });

      await Promise.all([agentsPaused.promise, firstQueryFailed.promise]);
      await new Promise<void>(resolve => setImmediate(resolve));
      expect(firstSettled).toBe(false);
      expect(connect).toHaveBeenCalledOnce();
      expect(checkouts[0]?.release).not.toHaveBeenCalled();
      expect(baseQuery).not.toHaveBeenCalled();

      overlappingOutcomePromise = settle(store.init()).then(outcome => {
        overlappingSettled = true;
        return outcome;
      });
      await new Promise<void>(resolve => setImmediate(resolve));

      // A call made after the first domain failed is still the same in-flight
      // init until the sibling task and pinned queue have both settled.
      expect(connect).toHaveBeenCalledOnce();
      expect(scoresInit).toHaveBeenCalledOnce();
      expect(agentsInit).toHaveBeenCalledOnce();
      expect(overlappingSettled).toBe(false);

      resumeAgents.resolve();
      await agentsTailStarted.promise;
      await new Promise<void>(resolve => setImmediate(resolve));
      expect(firstSettled).toBe(false);
      expect(overlappingSettled).toBe(false);
      expect(checkouts[0]?.release).not.toHaveBeenCalled();
      expect(baseQuery).not.toHaveBeenCalled();
      expect(checkouts[0]?.queries.some(sql => sql.includes('mastra_agent_versions'))).toBe(true);

      settleAgentsTail.resolve();
      const [firstOutcome, overlappingOutcome] = await Promise.all([firstOutcomePromise, overlappingOutcomePromise]);
      await Promise.all(releaseReentries);

      expect(firstOutcome.status).toBe('rejected');
      expect(overlappingOutcome.status).toBe('rejected');
      if (firstOutcome.status === 'rejected' && overlappingOutcome.status === 'rejected') {
        expect(firstOutcome.reason).toBeInstanceOf(MastraError);
        expect((firstOutcome.reason as MastraError).cause).toBe(firstError);
        expect(overlappingOutcome.reason).toBe(firstOutcome.reason);
      }

      expect(events).toContain('agents-tail-settled-pinned-1');
      expect(events.indexOf('agents-tail-settled-pinned-1')).toBeLessThan(events.indexOf('release-1'));
      expect(events.indexOf('agents-attempt-finished')).toBeLessThan(events.indexOf('release-1'));
      expect(checkouts[0]?.release).toHaveBeenCalledOnce();
      expect(checkouts[0]?.queries).not.toContain('RELEASE_REENTRY_1');
      expect(baseQuery.mock.calls.map(([sql]) => sql)).toEqual(['RELEASE_REENTRY_1']);

      await store.db.none('AFTER_FAILED_INIT');
      expect(baseQuery.mock.calls.map(([sql]) => sql)).toEqual(['RELEASE_REENTRY_1', 'AFTER_FAILED_INIT']);

      const retryOutcome = await settle(store.init());
      expect(retryOutcome).toEqual({ status: 'fulfilled' });
      await Promise.all(releaseReentries);
      expect(connect).toHaveBeenCalledTimes(2);
      expect(scoresInit).toHaveBeenCalledTimes(2);
      expect(agentsInit).toHaveBeenCalledTimes(2);
      expect(checkouts[1]?.queries.length).toBeGreaterThan(3);
      expect(checkouts[1]?.release).toHaveBeenCalledOnce();
      expect(checkouts[1]?.queries).not.toContain('RELEASE_REENTRY_2');
      expect(baseQuery.mock.calls.map(([sql]) => sql)).toEqual([
        'RELEASE_REENTRY_1',
        'AFTER_FAILED_INIT',
        'RELEASE_REENTRY_2',
      ]);

      await new Promise<void>(resolve => setImmediate(resolve));
      expect(unhandledRejections).toEqual([]);
    } finally {
      resumeAgents.resolve();
      settleAgentsTail.resolve();
      await firstAgentsAttemptFinished.promise;
      if (firstOutcomePromise) await firstOutcomePromise;
      if (overlappingOutcomePromise) await overlappingOutcomePromise;
      await Promise.all(releaseReentries);
      await new Promise<void>(resolve => setImmediate(resolve));
      process.off('unhandledRejection', onUnhandledRejection);
    }
  });

  it('keeps successful callers joined until cleanup releases the pinned client', async () => {
    const tailStarted = deferred();
    const settleTail = deferred();
    const events: string[] = [];
    const { pool, connect, baseQuery, checkouts } = createPool(
      async sql => {
        if (sql === 'SUCCESS_TAIL') {
          events.push('tail-started');
          tailStarted.resolve();
          await settleTail.promise;
          events.push('tail-settled');
        }
        return emptyResult();
      },
      checkout => events.push(`release-${checkout}`),
      async sql => {
        events.push(`base-${sql}`);
        return emptyResult();
      },
    );
    const store = new PostgresStore({ id: 'pinned-init-success-cleanup', pool });
    const domainInit = vi.fn(async () => {
      // Model a successful initializer that leaves already-enqueued driver
      // work for the adapter's cleanup drain.
      void store.db.none('SUCCESS_TAIL');
    });
    store.stores = { memory: { init: domainInit } } as unknown as StorageDomains;

    let firstResolved = false;
    const firstInit = store.init().then(() => {
      firstResolved = true;
    });

    await tailStarted.promise;
    await new Promise<void>(resolve => setImmediate(resolve));
    const firstResolvedBeforeTail = firstResolved;

    let overlappingResolved = false;
    const overlappingInit = Promise.all([store.init(), store.init()]).then(async () => {
      overlappingResolved = true;
      await store.db.none('RUNTIME_AFTER_INIT');
    });

    await new Promise<void>(resolve => setImmediate(resolve));
    const overlappingResolvedBeforeTail = overlappingResolved;
    const releasesBeforeTail = checkouts[0]?.release.mock.calls.length;
    const baseQueriesBeforeTail = baseQuery.mock.calls.length;

    settleTail.resolve();
    await Promise.all([firstInit, overlappingInit]);

    expect({
      overlappingResolvedBeforeTail,
      runtimeQueriesOnPinned: checkouts[0]?.queries.filter(sql => sql === 'RUNTIME_AFTER_INIT'),
      runtimeQueriesOnBase: baseQuery.mock.calls.map(([sql]) => sql),
    }).toEqual({
      overlappingResolvedBeforeTail: false,
      runtimeQueriesOnPinned: [],
      runtimeQueriesOnBase: ['RUNTIME_AFTER_INIT'],
    });
    expect(firstResolvedBeforeTail).toBe(false);
    expect(releasesBeforeTail).toBe(0);
    expect(baseQueriesBeforeTail).toBe(0);
    expect(connect).toHaveBeenCalledOnce();
    expect(domainInit).toHaveBeenCalledOnce();
    expect(checkouts[0]?.release).toHaveBeenCalledOnce();
    expect(events.indexOf('tail-settled')).toBeLessThan(events.indexOf('release-1'));
    expect(events.indexOf('release-1')).toBeLessThan(events.indexOf('base-RUNTIME_AFTER_INIT'));
  });

  it('adds no driver queries to successful or already-initialized calls', async () => {
    const { pool, connect, checkouts } = createPool();
    const store = new PostgresStore({ id: 'pinned-init-success-count', pool });
    store.stores = {
      memory: {
        init: async () => {
          await store.db.none('DOMAIN_ONE');
          await store.db.none('DOMAIN_TWO');
        },
      },
    } as unknown as StorageDomains;

    await store.init();

    expect(connect).toHaveBeenCalledOnce();
    expect(checkouts[0]?.queries).toHaveLength(5);
    expect(checkouts[0]?.queries.filter(sql => sql.includes('pg_catalog.pg_tables'))).toHaveLength(1);
    expect(checkouts[0]?.queries.filter(sql => sql.includes('pg_catalog.pg_attribute'))).toHaveLength(1);
    expect(checkouts[0]?.queries.filter(sql => sql.includes('pg_catalog.pg_index'))).toHaveLength(1);
    expect(checkouts[0]?.queries.slice(-2)).toEqual(['DOMAIN_ONE', 'DOMAIN_TWO']);
    expect(checkouts[0]?.release).toHaveBeenCalledOnce();

    await store.init();

    expect(connect).toHaveBeenCalledOnce();
    expect(checkouts[0]?.queries).toHaveLength(5);
  });
});
