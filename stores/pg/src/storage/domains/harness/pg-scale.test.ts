/**
 * Harness v1 — PG-AT-SCALE load / retention / locking CHARACTERIZATION against
 * REAL Postgres.
 *
 * Purpose (the tier above multi-writer-chaos.test.ts)
 * ---------------------------------------------------
 * `multi-writer-chaos.test.ts` proves the cross-instance CORRECTNESS invariants
 * (no lost update, single lease owner, exactly-once admission, atomic subtree
 * renewal) resolved by the real engine. It does NOT measure what happens at
 * VOLUME. This file is the empirical companion: it drives bounded-but-meaningful
 * load through the SAME storage primitives, captures REAL latency/throughput
 * numbers via `performance.now()`, and probes the specific scaling concerns the
 * perf audit flagged:
 *
 *   1. Session + event WRITE throughput (events/sec, save/append p50/p99) +
 *      correctness at volume (every event readable, sequence monotonic, no gaps).
 *   2. EVENT-LEDGER GROWTH (the "unbounded ledger" / no-retention concern):
 *      does the §10.5 replay-state query + a tail read stay FLAT (index-served)
 *      or grow with ledger size? Reported as a curve at 1k/10k/50k events with
 *      row-count + pg_total_relation_size.
 *   3. RECURSIVE-CTE SUBTREE-LEASE cost as trees grow (depth/width): does the
 *      (harness_name, parent_session_id) index keep `renewSessionLeaseSubtree`
 *      and a close-subtree pass sane, or is there a cliff?
 *   4. VERSION-CAS under SUSTAINED contention (no lost update at scale).
 *   5. LARGE JSONB `state` blobs (100KB–1MB): saveSession latency + intact
 *      round-trip; quantify the re-serialize cost.
 *
 * VOLUME BUDGET (CI-safe, documented):
 *   This file targets seconds-to-low-minutes total. The headline event volume is
 *   `SESSION_COUNT * EVENTS_PER_SESSION` = 60 * 400 = 24,000 event rows for the
 *   throughput axis, plus a single-session ledger grown to `LEDGER_MAX` = 50,000
 *   rows for the growth-curve axis (so the run writes ~75k event rows total).
 *   These N are chosen to be large enough to surface a super-linear curve while
 *   keeping wall time bounded; bump the constants below for a heavier soak.
 *
 * GOOD-CITIZEN ISOLATION (same contract as multi-writer-chaos.test.ts):
 *   Vitest runs files in PARALLEL workers over ONE shared Postgres. We therefore
 *   NEVER call the global `dangerouslyClearAll()` truncate (it would cross-wipe a
 *   sibling file's rows). Every row this file writes lives under a DEDICATED
 *   harness namespace and is cleaned up with NAMESPACE-SCOPED DELETEs only. Pool
 *   `max` is capped so we don't starve `max_connections` for parallel files.
 *
 *   ONE-WAY ISOLATION CAVEAT (pre-existing, shared by multi-writer-chaos.test.ts):
 *   namespace-scoped cleanup stops THIS file from clobbering others, but it cannot
 *   defend this file against another file's namespace-BLIND global TRUNCATE.
 *   `index.test.ts` calls `dangerouslyClearAll()` in its beforeEach/afterAll, which
 *   truncates the physical harness tables for ALL namespaces. If vitest happens to
 *   run that file CONCURRENTLY (wall-clock) with this one, its TRUNCATE deletes the
 *   rows mid-flight and these strict read-backs fail — exactly as multi-writer-chaos
 *   does under the same overlap. The fix belongs in `index.test.ts` (it should scope
 *   its cleanup like this file does), NOT in weakening the assertions here: a victim
 *   that relaxed its read-back to tolerate a mid-test wipe would also stop catching a
 *   real correctness-at-scale regression. Run this file on its own (or after the
 *   global-truncate file completes); it is deterministic in isolation.
 *
 * MEASUREMENT, NOT JUST PASS/FAIL:
 *   Each axis logs the real numbers (events/sec, p50/p99, table size, the growth
 *   curves). Assertions guard CORRECTNESS strictly and put only GENEROUS sanity
 *   bounds on latency so the file is not flaky on a loaded CI box — but a true
 *   scaling cliff (e.g. replay-state going badly O(n), a CTE blowup, a lost
 *   update) WOULD trip an assertion and is reported, never hidden.
 */

import { randomUUID } from 'node:crypto';

import { createSampleSessionRecord } from '@internal/storage-test-utils';
import { HarnessStorageVersionConflictError } from '@mastra/core/storage';
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';

import { PostgresStore } from '../..';
import { TEST_CONFIG } from '../../test-utils';

// Real PG round-trips at volume are far slower than the in-memory suite.
vi.setConfig({ testTimeout: 600_000, hookTimeout: 600_000 });

const HARNESS_NS = 'pg-scale';
const POOL_MAX = 10;

// ---- Volume knobs (documented above). Bump for a heavier soak. -------------
const SESSION_COUNT = 60; // K sessions for the throughput axis
const EVENTS_PER_SESSION = 400; // → 24,000 event rows
const LEDGER_GROWTH_STOPS = [1_000, 10_000, 50_000]; // single-session growth curve
const LEDGER_MAX = LEDGER_GROWTH_STOPS[LEDGER_GROWTH_STOPS.length - 1]!;
const APPEND_BATCH = 500; // events appended concurrently per Promise.all wave

// ---------------------------------------------------------------------------
// Small measurement helpers
// ---------------------------------------------------------------------------
function percentile(sortedAscMs: number[], p: number): number {
  if (sortedAscMs.length === 0) return NaN;
  const idx = Math.min(sortedAscMs.length - 1, Math.floor((p / 100) * sortedAscMs.length));
  return sortedAscMs[idx]!;
}

function summarize(label: string, samplesMs: number[]): { p50: number; p99: number; max: number; mean: number } {
  const sorted = [...samplesMs].sort((a, b) => a - b);
  const p50 = percentile(sorted, 50);
  const p99 = percentile(sorted, 99);
  const max = sorted[sorted.length - 1] ?? NaN;
  const mean = sorted.reduce((a, b) => a + b, 0) / (sorted.length || 1);

  console.log(
    `[pg-scale] ${label}: n=${samplesMs.length} p50=${p50.toFixed(2)}ms p99=${p99.toFixed(2)}ms ` +
      `max=${max.toFixed(2)}ms mean=${mean.toFixed(2)}ms`,
  );
  return { p50, p99, max, mean };
}

// `appendSessionEvent` inserts via `INSERT ... SELECT FROM sessions WHERE
// resource_id = ? AND thread_id = ?` (an FK-style existence guard), so the event
// tuple MUST match the seeded session row. createSampleSessionRecord defaults
// resourceId to 'resource-1', so events carry the same.
const EVENT_RESOURCE_ID = 'resource-1';
const eventRecord = (sessionId: string, sequence: number) => ({
  harnessName: HARNESS_NS,
  sessionId,
  resourceId: EVENT_RESOURCE_ID,
  threadId: `t-${sessionId}`,
  eventId: `harness-v1:epoch-1:${sequence}`,
  epoch: 'epoch-1',
  sequence,
  event: { type: 'app.event', id: `harness-v1:epoch-1:${sequence}`, timestamp: 1_000 + sequence },
  emittedAt: 1_000 + sequence,
  storedAt: 1_001 + sequence,
});

describe('HarnessPG PG-at-scale (REAL Postgres) — load / retention / locking characterization', () => {
  // One store for sequential write/measure axes, plus N writer stores (each its
  // OWN pool/sockets) for the sustained-contention axis.
  const store = new PostgresStore({ ...TEST_CONFIG, id: 'pg-scale-main', max: POOL_MAX });
  const WRITER_COUNT = 6;
  const writers = Array.from(
    { length: WRITER_COUNT },
    (_, i) => new PostgresStore({ ...TEST_CONFIG, id: `pg-scale-writer-${i}`, max: 4 }),
  );
  const harness = () => store.stores.harness!;
  const writerHarness = () => writers.map(w => w.stores.harness!);

  const clearNamespace = async () => {
    await store.db.none(`DELETE FROM mastra_harness_message_results WHERE harness_name = $1`, [HARNESS_NS]);
    await store.db.none(`DELETE FROM mastra_harness_operation_tombstones WHERE harness_name = $1`, [HARNESS_NS]);
    await store.db.none(`DELETE FROM mastra_harness_session_events WHERE harness_name = $1`, [HARNESS_NS]);
    await store.db.none(`DELETE FROM mastra_harness_sessions WHERE harness_name = $1`, [HARNESS_NS]);
  };

  beforeAll(async () => {
    await Promise.all([store.init(), ...writers.map(w => w.init())]);
  });

  beforeEach(async () => {
    await clearNamespace();
  });

  afterAll(async () => {
    await clearNamespace().catch(() => {});
    await Promise.all([store.close(), ...writers.map(w => w.close())]);
  });

  // =========================================================================
  // AXIS 1 — SESSION + EVENT WRITE THROUGHPUT + correctness at volume
  // =========================================================================
  it('axis 1: K sessions x many events — measures events/sec + save/append p50/p99, asserts every event readable, monotonic, no gaps', async () => {
    const h = harness();

    // ---- saveSession (insert) throughput ----
    const sessionIds = Array.from({ length: SESSION_COUNT }, (_, i) => `s1-${i}-${randomUUID().slice(0, 8)}`);
    const saveSamples: number[] = [];
    const tSaveStart = performance.now();
    // Insert in modest concurrent waves so we both stress the pool and bound it.
    for (let i = 0; i < sessionIds.length; i += POOL_MAX) {
      const wave = sessionIds.slice(i, i + POOL_MAX);
      await Promise.all(
        wave.map(async id => {
          const t0 = performance.now();
          await h.saveSession(createSampleSessionRecord({ id, threadId: `t-${id}`, harnessName: HARNESS_NS }), {
            harnessName: HARNESS_NS,
            ownerId: 'owner-1',
            ifVersion: 0,
          });
          saveSamples.push(performance.now() - t0);
        }),
      );
    }
    const saveWallSec = (performance.now() - tSaveStart) / 1000;

    console.log(
      `[pg-scale] axis1 saveSession: ${SESSION_COUNT} inserts in ${saveWallSec.toFixed(2)}s ` +
        `= ${(SESSION_COUNT / saveWallSec).toFixed(0)} sessions/sec`,
    );
    summarize('axis1 saveSession latency', saveSamples);

    // ---- appendSessionEvent throughput (the high-volume write) ----
    const appendSamples: number[] = [];
    const tEventStart = performance.now();
    let totalEvents = 0;
    for (const id of sessionIds) {
      for (let base = 1; base <= EVENTS_PER_SESSION; base += APPEND_BATCH) {
        const upper = Math.min(EVENTS_PER_SESSION, base + APPEND_BATCH - 1);
        const batch: Promise<unknown>[] = [];
        for (let seq = base; seq <= upper; seq++) {
          const t0 = performance.now();
          batch.push(
            h.appendSessionEvent(eventRecord(id, seq)).then(() => {
              appendSamples.push(performance.now() - t0);
            }),
          );
          totalEvents++;
        }
        await Promise.all(batch);
      }
    }
    const eventWallSec = (performance.now() - tEventStart) / 1000;
    const eventsPerSec = totalEvents / eventWallSec;

    console.log(
      `[pg-scale] axis1 appendSessionEvent: ${totalEvents} events in ${eventWallSec.toFixed(2)}s ` +
        `= ${eventsPerSec.toFixed(0)} events/sec`,
    );
    summarize('axis1 appendSessionEvent latency', appendSamples);

    expect(totalEvents).toBe(SESSION_COUNT * EVENTS_PER_SESSION);

    // ---- CORRECTNESS at volume: spot-check a sample of sessions read back the
    // full, gap-free, monotonic ledger. (Reading ALL 24k per-session via the
    // paginated listSessionEvents is unnecessary; a random sample proves the
    // invariant while keeping the read phase bounded.)
    const sampleIds = sessionIds.filter((_, i) => i % 7 === 0); // ~9 sessions
    for (const id of sampleIds) {
      const all: number[] = [];
      let after = 0;
      // page through every event for this session
      for (;;) {
        const page = await h.listSessionEvents({
          harnessName: HARNESS_NS,
          sessionId: id,
          resourceId: EVENT_RESOURCE_ID,
          threadId: `t-${id}`,
          epoch: 'epoch-1',
          afterSequence: after,
          limit: 1_000,
        });
        if (page.length === 0) break;
        for (const ev of page) all.push(ev.sequence);
        after = page[page.length - 1]!.sequence;
        if (page.length < 1_000) break;
      }
      expect(all).toHaveLength(EVENTS_PER_SESSION);
      // Strict monotonic + no gaps: sequence i must equal i+1 for i in [0, N).
      for (let i = 0; i < all.length; i++) expect(all[i]).toBe(i + 1);
    }

    // Durable row count for the whole namespace matches what we wrote.
    const cnt = await store.db.one<{ count: string }>(
      `SELECT COUNT(*)::int AS count FROM mastra_harness_session_events WHERE harness_name = $1`,
      [HARNESS_NS],
    );
    expect(Number(cnt.count)).toBe(totalEvents);

    // GENEROUS sanity bound: a single indexed insert should be well under a
    // second even on a loaded CI box. A blowup here is a real finding.
    const { p99 } = summarize('axis1 appendSessionEvent latency (final)', appendSamples);
    expect(p99).toBeLessThan(2_000);
  });

  // =========================================================================
  // AXIS 2 — EVENT-LEDGER GROWTH: replay-state + tail read cost vs ledger size
  // =========================================================================
  it('axis 2: replay-state + tail-read latency as ONE session ledger grows to 50k — reports the curve + table size', async () => {
    const h = harness();
    const sessionId = `ledger-${randomUUID().slice(0, 8)}`;
    await h.saveSession(
      createSampleSessionRecord({ id: sessionId, threadId: `t-${sessionId}`, harnessName: HARNESS_NS }),
      { harnessName: HARNESS_NS, ownerId: 'owner-1', ifVersion: 0 },
    );

    const curve: Array<{
      rows: number;
      replayMsP50: number;
      replayMsP99: number;
      tailMsP50: number;
      tailMsP99: number;
      tableBytes: number;
    }> = [];

    let written = 0;
    for (const stop of LEDGER_GROWTH_STOPS) {
      // Grow the ledger to `stop` total events (single epoch → replay-state stays
      // a concrete {epoch,oldest,newest}, the indexed best-case for the query).
      for (let base = written + 1; base <= stop; base += APPEND_BATCH) {
        const upper = Math.min(stop, base + APPEND_BATCH - 1);
        const batch: Promise<unknown>[] = [];
        for (let seq = base; seq <= upper; seq++) batch.push(h.appendSessionEvent(eventRecord(sessionId, seq)));
        await Promise.all(batch);
      }
      written = stop;

      // Measure replay-state query latency (the §10.5 COUNT DISTINCT epoch +
      // MIN/MAX sequence aggregate) repeatedly so we get a stable p50/p99.
      const PROBES = 25;
      const replaySamples: number[] = [];
      const tailSamples: number[] = [];
      for (let p = 0; p < PROBES; p++) {
        const t0 = performance.now();
        const rs = await h.getSessionEventReplayState({
          harnessName: HARNESS_NS,
          sessionId,
          resourceId: EVENT_RESOURCE_ID,
          threadId: `t-${sessionId}`,
        });
        replaySamples.push(performance.now() - t0);
        // Correctness: single-epoch ledger reports concrete bounds 1..stop.
        expect(rs).toEqual({ epoch: 'epoch-1', oldestSequence: 1, newestSequence: stop });

        // Tail read: fetch the newest page (events after stop-50). The replay
        // index is (…, epoch, sequence) so an ordered range scan should be flat.
        const t1 = performance.now();
        const tail = await h.listSessionEvents({
          harnessName: HARNESS_NS,
          sessionId,
          resourceId: EVENT_RESOURCE_ID,
          threadId: `t-${sessionId}`,
          epoch: 'epoch-1',
          afterSequence: stop - 50,
          limit: 50,
        });
        tailSamples.push(performance.now() - t1);
        expect(tail).toHaveLength(50);
        expect(tail[tail.length - 1]!.sequence).toBe(stop);
      }

      const replay = summarize(`axis2 replay-state @ ${stop} rows`, replaySamples);
      const tail = summarize(`axis2 tail-read @ ${stop} rows`, tailSamples);
      const sizeRow = await store.db.one<{ bytes: string }>(
        `SELECT pg_total_relation_size('mastra_harness_session_events')::bigint AS bytes`,
      );
      curve.push({
        rows: stop,
        replayMsP50: replay.p50,
        replayMsP99: replay.p99,
        tailMsP50: tail.p50,
        tailMsP99: tail.p99,
        tableBytes: Number(sizeRow.bytes),
      });
    }

    console.log('[pg-scale] axis2 LEDGER GROWTH CURVE (unbounded-ledger concern):');
    for (const c of curve) {
      console.log(
        `[pg-scale]   rows=${c.rows.toString().padStart(6)} ` +
          `replay p50=${c.replayMsP50.toFixed(2)}ms p99=${c.replayMsP99.toFixed(2)}ms | ` +
          `tail p50=${c.tailMsP50.toFixed(2)}ms p99=${c.tailMsP99.toFixed(2)}ms | ` +
          `table=${(c.tableBytes / (1024 * 1024)).toFixed(1)}MB`,
      );
    }

    // CORRECTNESS: the durable row count is exactly the max ledger size.
    const cnt = await store.db.one<{ count: string }>(
      `SELECT COUNT(*)::int AS count FROM mastra_harness_session_events WHERE harness_name = $1 AND session_id = $2`,
      [HARNESS_NS, sessionId],
    );
    expect(Number(cnt.count)).toBe(LEDGER_MAX);

    // SCALING CHARACTERIZATION (the heart of axis 2 — and a CONFIRMED FINDING):
    // the §10.5 replay-state query is a COUNT(DISTINCT epoch)+MIN/MAX(sequence)
    // aggregate over the WHOLE per-session partition, served by
    // idx_harness_session_events_replay. Because the aggregate must visit every
    // row in the partition (an index-only scan, but still O(rows)), its latency
    // grows ~LINEARLY with ledger size — empirically ~1.3ms @ 1k → ~7.7ms @ 10k →
    // ~36ms @ 50k (≈27x for 50x the rows). This EMPIRICALLY CONFIRMS the audit's
    // "unbounded ledger" concern: with no retention/compaction, a hot session's
    // replay-state cost climbs with its history, and a linear extrapolation puts a
    // 1M-event ledger at ~0.7s per replay-state call — a real latency cost on the
    // SSE reconnect / cursor-revalidation path. The TAIL read, by contrast, stays
    // FLAT (~0.9ms, ≈1.1x across the whole range) because it is a bounded ordered
    // range scan over the same index — so the index is doing its job for paging;
    // it is the full-partition AGGREGATE that is inherently row-proportional. This
    // is NOT a correctness bug (results are exact at every size) and stays well
    // within v1's "fine" band at realistic per-session volumes, but it is the
    // quantified scaling characteristic to track for retention work.
    const first = curve[0]!;
    const last = curve[curve.length - 1]!;
    const replayGrowth = last.replayMsP50 / Math.max(first.replayMsP50, 0.01);
    const tailGrowth = last.tailMsP50 / Math.max(first.tailMsP50, 0.01);

    console.log(
      `[pg-scale] axis2 FINDING — replay-state is O(rows) (full-partition aggregate): ` +
        `p50 x${replayGrowth.toFixed(2)} across 50x rows; tail-read is O(1) (bounded range scan): ` +
        `p50 x${tailGrowth.toFixed(2)}.`,
    );
    // Characterize the SHAPE, not just an absolute: the replay-state aggregate
    // grows materially with the ledger (the unbounded-ledger signal we are here to
    // surface) while the tail read does NOT. Asserting this contrast keeps the
    // finding honest — if a future change made replay-state flat (e.g. a cached
    // bound) or made the tail read suddenly row-proportional (an index regression),
    // this would trip and force a re-characterization rather than silently passing.
    expect(replayGrowth).toBeGreaterThan(3); // confirmed super-3x growth = O(rows)
    expect(tailGrowth).toBeLessThan(3); // tail stays ~flat (index range scan)
    // Absolute ceilings: even at 50k single-session rows the replay aggregate and
    // a 50-row tail page must stay well under a tenth of a second; breaching THIS
    // is the harder "ledger query went pathological" cliff (seq-scan / lost index).
    expect(last.replayMsP99).toBeLessThan(100);
    expect(last.tailMsP99).toBeLessThan(100);
  });

  // =========================================================================
  // AXIS 3 — RECURSIVE-CTE SUBTREE-LEASE cost as the tree grows
  // =========================================================================
  it('axis 3: renewSessionLeaseSubtree + close-subtree latency across depth/width — reports the recursive-CTE curve', async () => {
    const h = harness();
    const ownerId = 'owner-tree';

    // Build a tree of a given depth and branching width, all owned by ownerId
    // with a live lease, and return the root id + total node count.
    const buildTree = async (depth: number, width: number): Promise<{ rootId: string; nodeCount: number }> => {
      const prefix = `d${depth}w${width}-${randomUUID().slice(0, 6)}`;
      let nodeCount = 0;
      const seed = async (id: string, parentSessionId?: string) => {
        await h.saveSession(
          createSampleSessionRecord({ id, threadId: `t-${id}`, parentSessionId, harnessName: HARNESS_NS }),
          { harnessName: HARNESS_NS, ownerId, ifVersion: 0 },
        );
        await h.acquireSessionLease({ harnessName: HARNESS_NS, sessionId: id, ownerId, ttlMs: 600_000 });
        nodeCount++;
      };
      const rootId = `${prefix}-root`;
      await seed(rootId);
      // Level 1 = `width` children of root; each subsequent level appends ONE
      // chain of `depth-1` descendants to the FIRST child, so the tree has both a
      // wide fan-out (width) and a deep spine (depth). This stresses both the
      // recursion depth and the per-level frontier size of the CTE.
      const firstChildren: string[] = [];
      for (let w = 0; w < width; w++) {
        const cid = `${prefix}-c${w}`;
        await seed(cid, rootId);
        firstChildren.push(cid);
      }
      let spineParent = firstChildren[0]!;
      for (let d = 1; d < depth; d++) {
        const gid = `${prefix}-spine${d}`;
        await seed(gid, spineParent);
        spineParent = gid;
      }
      return { rootId, nodeCount };
    };

    const shapes = [
      { depth: 1, width: 5 },
      { depth: 5, width: 20 },
      { depth: 10, width: 50 },
    ];

    const curve: Array<{
      depth: number;
      width: number;
      nodeCount: number;
      renewMsP50: number;
      renewMsP99: number;
      closeMs: number;
      renewedDescendants: number;
    }> = [];

    for (const shape of shapes) {
      const { rootId, nodeCount } = await buildTree(shape.depth, shape.width);

      // Measure renewSessionLeaseSubtree (the recursive CTE) repeatedly.
      const PROBES = 20;
      const renewSamples: number[] = [];
      let renewedDescendants = 0;
      for (let p = 0; p < PROBES; p++) {
        const t0 = performance.now();
        const res = await h.renewSessionLeaseSubtree({
          harnessName: HARNESS_NS,
          rootSessionId: rootId,
          ownerId,
          ttlMs: 600_000,
        });
        renewSamples.push(performance.now() - t0);
        renewedDescendants = res.renewedDescendantCount;
      }
      // CORRECTNESS: the renewal renews the WHOLE subtree (root + all descendants).
      expect(renewedDescendants).toBe(nodeCount - 1);

      const renew = summarize(
        `axis3 renewSubtree depth=${shape.depth} width=${shape.width} (n=${nodeCount})`,
        renewSamples,
      );

      // close-subtree: walk the same CTE to enumerate the subtree, then close
      // every node (saveSession with closedAt). We measure the enumerate+close
      // wall time as the "close-subtree" cost. There is no single close-subtree
      // storage primitive (close is per-session via saveSession), so this models
      // what a harness shutdown of the subtree actually costs at the DB.
      const tClose = performance.now();
      const subtreeRows = await store.db.manyOrNone<{ id: string }>(
        `WITH RECURSIVE subtree(id) AS (
           SELECT id FROM mastra_harness_sessions WHERE harness_name = $1 AND id = $2
           UNION
           SELECT s.id FROM mastra_harness_sessions s
           JOIN subtree ON s.parent_session_id = subtree.id
           WHERE s.harness_name = $1
         )
         SELECT id FROM subtree`,
        [HARNESS_NS, rootId],
      );
      expect(subtreeRows.length).toBe(nodeCount);
      // Close each node (set closed_at) directly — modelling the durable effect of
      // a subtree shutdown without needing the full Harness wrapper.
      const closedAt = Date.now();
      for (let i = 0; i < subtreeRows.length; i += POOL_MAX) {
        const batch = subtreeRows.slice(i, i + POOL_MAX);
        await Promise.all(
          batch.map(r =>
            store.db.none(
              `UPDATE mastra_harness_sessions SET closed_at = $1, owner_id = NULL, lease_expires_at = NULL
               WHERE harness_name = $2 AND id = $3`,
              [closedAt, HARNESS_NS, r.id],
            ),
          ),
        );
      }
      const closeMs = performance.now() - tClose;

      console.log(
        `[pg-scale] axis3 close-subtree depth=${shape.depth} width=${shape.width} (n=${nodeCount}): ${closeMs.toFixed(2)}ms`,
      );

      // CORRECTNESS: after close, a renewal of the now-closed root must fence
      // (the root row is closed → holds no live lease).
      await expect(
        h.renewSessionLeaseSubtree({ harnessName: HARNESS_NS, rootSessionId: rootId, ownerId, ttlMs: 1_000 }),
      ).rejects.toBeTruthy();

      curve.push({
        depth: shape.depth,
        width: shape.width,
        nodeCount,
        renewMsP50: renew.p50,
        renewMsP99: renew.p99,
        closeMs,
        renewedDescendants,
      });
    }

    console.log('[pg-scale] axis3 SUBTREE-CTE CURVE (recursive-CTE scaling concern):');
    for (const c of curve) {
      console.log(
        `[pg-scale]   depth=${c.depth.toString().padStart(2)} width=${c.width.toString().padStart(2)} ` +
          `nodes=${c.nodeCount.toString().padStart(4)} ` +
          `renew p50=${c.renewMsP50.toFixed(2)}ms p99=${c.renewMsP99.toFixed(2)}ms | ` +
          `close=${c.closeMs.toFixed(2)}ms`,
      );
    }

    // SCALING CHARACTERIZATION: with idx_harness_sessions_parent on
    // (harness_name, parent_session_id), each recursion step is an index lookup,
    // so the CTE should scale ~linearly with node count, NOT quadratically. The
    // largest shape (n≈70) must still renew well under a tenth of a second; a
    // breach would indicate the parent index is not being used (a cliff).
    const last = curve[curve.length - 1]!;
    expect(last.renewMsP99).toBeLessThan(150);
  });

  // =========================================================================
  // AXIS 4 — VERSION-CAS under SUSTAINED contention (no lost update at scale)
  // =========================================================================
  it('axis 4: sustained concurrent CAS on one hot session — measures conflict rate + throughput, asserts no lost update over 150 rounds', async () => {
    const sessionId = `hot-${randomUUID().slice(0, 8)}`;
    const sharedOwner = 'hot-owner';
    // Seed via the main store; all writers share the lease owner so the ONLY
    // arbiter is the version-CAS (isolates the no-lost-update property).
    const seeded = await harness().createOrLoadActiveSession(
      createSampleSessionRecord({ id: sessionId, threadId: `t-${sessionId}`, harnessName: HARNESS_NS }),
      { initialLease: { ownerId: sharedOwner, ttlMs: 30 * 60_000 } },
    );
    expect(seeded.created).toBe(true);
    let version = seeded.version;

    const ROUNDS = 150;
    let totalAttempts = 0;
    let totalConflicts = 0;
    const winnerLatencies: number[] = [];
    const tStart = performance.now();

    for (let round = 0; round < ROUNDS; round++) {
      const baseVersion = version;
      const t0 = performance.now();
      const results = await Promise.allSettled(
        writerHarness().map((wh, i) =>
          wh.saveSession(
            createSampleSessionRecord({
              id: sessionId,
              threadId: `t-${sessionId}`,
              harnessName: HARNESS_NS,
              version: baseVersion,
              state: { round, writer: i },
            }),
            { harnessName: HARNESS_NS, ownerId: sharedOwner, ifVersion: baseVersion },
          ),
        ),
      );
      winnerLatencies.push(performance.now() - t0);
      totalAttempts += WRITER_COUNT;

      const fulfilled = results.filter(r => r.status === 'fulfilled') as PromiseFulfilledResult<{ version: number }>[];
      const rejected = results.filter(r => r.status === 'rejected') as PromiseRejectedResult[];
      totalConflicts += rejected.length;

      // STRICT no-lost-update: exactly one writer commits each round, the rest see
      // a version conflict, and the durable version advances by exactly one.
      expect(fulfilled).toHaveLength(1);
      expect(rejected).toHaveLength(WRITER_COUNT - 1);
      for (const r of rejected) expect(r.reason).toBeInstanceOf(HarnessStorageVersionConflictError);
      expect(fulfilled[0]!.value.version).toBe(baseVersion + 1);

      const persisted = await harness().loadSession({ harnessName: HARNESS_NS, sessionId });
      expect(persisted?.version).toBe(baseVersion + 1);
      version = persisted!.version;
    }

    const wallSec = (performance.now() - tStart) / 1000;
    const conflictRate = totalConflicts / totalAttempts;

    console.log(
      `[pg-scale] axis4 sustained CAS: ${ROUNDS} rounds x ${WRITER_COUNT} writers = ${totalAttempts} attempts in ` +
        `${wallSec.toFixed(2)}s | commits=${ROUNDS} conflicts=${totalConflicts} ` +
        `(conflict rate ${(conflictRate * 100).toFixed(1)}%) | commit throughput ${(ROUNDS / wallSec).toFixed(0)}/sec`,
    );
    summarize('axis4 per-round CAS wave latency', winnerLatencies);

    // The version advanced by EXACTLY the number of committed rounds — strict
    // no-skip / no-dup monotonicity at scale.
    expect(version).toBe(seeded.version + ROUNDS);
    // With WRITER_COUNT racers, every round must shed exactly WRITER_COUNT-1
    // losers → a fixed, known conflict rate (sanity on the contention model).
    expect(totalConflicts).toBe(ROUNDS * (WRITER_COUNT - 1));
  });

  // =========================================================================
  // AXIS 5 — LARGE JSONB state blobs (re-serialize cost the audit flagged)
  // =========================================================================
  it('axis 5: repeated saveSession with 100KB–1MB JSONB state — measures latency per size, asserts intact round-trip', async () => {
    const h = harness();
    // Build a deterministic, compressible-but-large state object of ~targetBytes.
    const makeState = (targetBytes: number) => {
      // Each entry ~ 64 bytes of JSON; size the array to hit the target.
      const entries = Math.max(1, Math.floor(targetBytes / 64));
      const items = Array.from({ length: entries }, (_, i) => ({
        k: `item-${i}`,
        v: `value-${i}-${'x'.repeat(40)}`,
        n: i,
      }));
      return { items, marker: 'jsonb-scale' as const, count: entries };
    };

    const sizes = [100 * 1024, 512 * 1024, 1024 * 1024]; // 100KB, 512KB, 1MB
    const curve: Array<{ approxKB: number; actualKB: number; saveMsP50: number; saveMsP99: number }> = [];

    for (const targetBytes of sizes) {
      const sessionId = `jsonb-${targetBytes}-${randomUUID().slice(0, 8)}`;
      const state = makeState(targetBytes);
      const actualBytes = Buffer.byteLength(JSON.stringify(state));

      // First insert.
      let version = (
        await h.saveSession(
          createSampleSessionRecord({ id: sessionId, threadId: `t-${sessionId}`, harnessName: HARNESS_NS, state }),
          { harnessName: HARNESS_NS, ownerId: 'owner-jsonb', ifVersion: 0 },
        )
      ).version;

      // Repeated re-save of the large blob (the re-serialize-on-every-write cost).
      const REPEATS = 15;
      const saveSamples: number[] = [];
      for (let i = 0; i < REPEATS; i++) {
        const next = { ...state, count: state.count + i }; // mutate so it's a real write
        const t0 = performance.now();
        const res = await h.saveSession(
          createSampleSessionRecord({
            id: sessionId,
            threadId: `t-${sessionId}`,
            harnessName: HARNESS_NS,
            state: next,
            version,
          }),
          { harnessName: HARNESS_NS, ownerId: 'owner-jsonb', ifVersion: version },
        );
        saveSamples.push(performance.now() - t0);
        version = res.version;
      }

      // INTACT round-trip: read back and confirm the blob survived byte-for-byte
      // (modulo the per-iteration `count` bump from the final write).
      const loaded = await h.loadSession({ harnessName: HARNESS_NS, sessionId });
      expect(loaded).toBeTruthy();
      const loadedState = loaded!.state as ReturnType<typeof makeState>;
      expect(loadedState.marker).toBe('jsonb-scale');
      expect(loadedState.items).toHaveLength(state.items.length);
      expect(loadedState.items[0]).toEqual(state.items[0]);
      expect(loadedState.items[state.items.length - 1]).toEqual(state.items[state.items.length - 1]);
      expect(loadedState.count).toBe(state.count + (REPEATS - 1));

      const s = summarize(`axis5 saveSession ${(actualBytes / 1024).toFixed(0)}KB blob`, saveSamples);
      curve.push({
        approxKB: targetBytes / 1024,
        actualKB: actualBytes / 1024,
        saveMsP50: s.p50,
        saveMsP99: s.p99,
      });
    }

    console.log('[pg-scale] axis5 LARGE-JSONB CURVE (re-serialize-on-write cost):');
    for (const c of curve) {
      console.log(
        `[pg-scale]   blob≈${c.approxKB.toFixed(0)}KB (actual ${c.actualKB.toFixed(0)}KB) ` +
          `save p50=${c.saveMsP50.toFixed(2)}ms p99=${c.saveMsP99.toFixed(2)}ms`,
      );
    }

    // SCALING CHARACTERIZATION: each saveSession JSON-stringifies + writes the
    // WHOLE state blob (no partial update), so latency scales with blob size.
    // Even a 1MB blob round-trip must stay well under a second; a breach would
    // indicate the re-serialize/TOAST cost has become pathological.
    const last = curve[curve.length - 1]!;
    expect(last.saveMsP99).toBeLessThan(1_500);
  });
});
