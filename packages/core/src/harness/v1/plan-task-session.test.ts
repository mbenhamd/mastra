/**
 * TM-3/TM-4 plan-task SESSION operations (§5.1k / §6.4) against REAL InMemory
 * storage through a fake `PlanTaskSessionPort`.
 *
 * This exercises the seam the built-in tools call: each `planTask*` op fences on
 * the session owner + version, runs the TM-4 hierarchy semantics, commits
 * transaction-shaped, and emits the `papersflow.plan_task.updated` event. The
 * fake port records the emitted event payloads and reads the session version off
 * the live record, so storage integration + rollup + cycle + single-in_progress
 * + atomicity are all covered without driving a full agent turn (the turn-gated
 * `_emitCustomEvent` wiring is covered separately in session.plan-task.test.ts).
 */

import { describe, expect, it, beforeEach } from 'vitest';

import { InMemoryDB } from '../../storage/domains/inmemory-db';
import { InMemoryHarness } from '../../storage/domains/harness/inmemory';
import type { JsonValue, SessionRecord } from '../../storage/domains/harness/types';
import { HarnessPlanTaskCycleError, HarnessPlanTaskInProgressConflictError } from './plan-task-hierarchy';
import {
  planTaskAdd,
  planTaskCheck,
  planTaskComplete,
  planTaskDecompose,
  planTaskReparent,
  planTaskUpdate,
  PLAN_TASK_UPDATED_EVENT,
  type PlanTaskSessionPort,
} from './plan-task-session';

const OWNER = 'owner-1';
const SESSION_ID = 's1';

function sessionRecord(): SessionRecord {
  return {
    harnessName: 'default',
    id: SESSION_ID,
    resourceId: 'r1',
    threadId: 't1',
    origin: 'top-level',
    ownsThread: true,
    modeId: 'default',
    modelId: 'm1',
    subagentModelOverrides: {},
    permissionRules: { categories: {}, tools: {} },
    sessionGrants: { categories: [], tools: [] },
    tokenUsage: { promptTokens: 0, completionTokens: 0, totalTokens: 0 },
    pendingQueue: [],
    state: {},
    createdAt: 0,
    lastActivityAt: 0,
    version: 1,
  };
}

interface Harness {
  storage: InMemoryHarness;
  port: PlanTaskSessionPort;
  events: Array<{ op: string; affectedTaskIds: string[] }>;
}

async function setup(): Promise<Harness> {
  const storage = new InMemoryHarness({ db: new InMemoryDB() });
  await storage.createOrLoadActiveSession(sessionRecord(), { initialLease: { ownerId: OWNER, ttlMs: 60_000 } });
  const events: Array<{ op: string; affectedTaskIds: string[] }> = [];
  const port: PlanTaskSessionPort = {
    id: SESSION_ID,
    resourceId: 'r1',
    threadId: 't1',
    harnessName: 'default',
    storage,
    ownerId: OWNER,
    sessionVersion: 1,
    emitPlanTaskEvent: (payload: JsonValue) => {
      events.push(payload as { op: string; affectedTaskIds: string[] });
    },
  };
  return { storage, port, events };
}

async function listAll(storage: InMemoryHarness) {
  const out = await storage.listPlanTasks({ sessionId: SESSION_ID, limit: 1000 });
  return new Map(out.tasks.map(t => [t.taskId, t]));
}

// ---------------------------------------------------------------------------
// Tools / ops basic behavior
// ---------------------------------------------------------------------------

describe('planTaskAdd', () => {
  it('adds a root task and emits the custom event', async () => {
    const { storage, port, events } = await setup();
    const view = await planTaskAdd(port, { content: 'do thing' });
    expect(view.content).toBe('do thing');
    expect(view.status).toBe('pending');
    expect(view.statusSource).toBe('explicit');
    const stored = await listAll(storage);
    expect(stored.get(view.taskId)?.content).toBe('do thing');
    expect(events).toEqual([{ op: 'add', affectedTaskIds: [view.taskId] }]);
  });

  it('appends order among siblings', async () => {
    const { port } = await setup();
    const a = await planTaskAdd(port, { content: 'a' });
    const b = await planTaskAdd(port, { content: 'b' });
    expect(b.order).toBe(a.order + 1);
  });

  it('rejects an unknown parent', async () => {
    const { port } = await setup();
    await expect(planTaskAdd(port, { content: 'x', parentTaskId: 'nope' })).rejects.toThrow(/unknown parent/);
  });

  it('flips a parent to derived and rolls its status up to the new child', async () => {
    const { storage, port } = await setup();
    const parent = await planTaskAdd(port, { content: 'parent' });
    const child = await planTaskAdd(port, { content: 'child', parentTaskId: parent.taskId, status: 'in_progress' });
    const stored = await listAll(storage);
    expect(stored.get(parent.taskId)?.statusSource).toBe('derived');
    expect(stored.get(parent.taskId)?.status).toBe('in_progress'); // rolled up from child
    expect(stored.get(child.taskId)?.status).toBe('in_progress');
  });
});

describe('planTaskDecompose', () => {
  it('adds N children atomically under a parent and flips it derived', async () => {
    const { storage, port, events } = await setup();
    const parent = await planTaskAdd(port, { content: 'p' });
    const children = await planTaskDecompose(port, parent.taskId, [{ content: 'c1' }, { content: 'c2' }]);
    expect(children).toHaveLength(2);
    const stored = await listAll(storage);
    expect(stored.size).toBe(3);
    expect(stored.get(parent.taskId)?.statusSource).toBe('derived');
    expect(stored.get(parent.taskId)?.status).toBe('pending');
    expect(events.at(-1)).toEqual({ op: 'decompose', affectedTaskIds: [parent.taskId, ...children.map(c => c.taskId)] });
  });

  it('rejects empty children list', async () => {
    const { port } = await setup();
    const parent = await planTaskAdd(port, { content: 'p' });
    await expect(planTaskDecompose(port, parent.taskId, [])).rejects.toThrow(/at least one child/);
  });
});

describe('planTaskComplete + rollup', () => {
  it('completing all children completes the derived parent (cancelled counts as ok)', async () => {
    const { storage, port } = await setup();
    const parent = await planTaskAdd(port, { content: 'p' });
    const [c1, c2] = await planTaskDecompose(port, parent.taskId, [{ content: 'c1' }, { content: 'c2' }]);
    await planTaskComplete(port, c1!.taskId);
    let stored = await listAll(storage);
    expect(stored.get(parent.taskId)?.status).toBe('pending'); // not all done yet
    await planTaskUpdate(port, c2!.taskId, { status: 'cancelled' });
    stored = await listAll(storage);
    expect(stored.get(parent.taskId)?.status).toBe('completed'); // completed + cancelled
    expect(stored.get(parent.taskId)?.statusSource).toBe('derived');
  });

  it('a failed child makes the derived parent failed', async () => {
    const { storage, port } = await setup();
    const parent = await planTaskAdd(port, { content: 'p' });
    const [c1] = await planTaskDecompose(port, parent.taskId, [{ content: 'c1' }, { content: 'c2' }]);
    await planTaskUpdate(port, c1!.taskId, { status: 'failed' });
    const stored = await listAll(storage);
    expect(stored.get(parent.taskId)?.status).toBe('failed');
  });

  it('explicit terminal parent status is NEVER overwritten by rollup', async () => {
    const { storage, port } = await setup();
    // Parent explicitly cancelled BEFORE it gains a child → stays cancelled.
    const parent = await planTaskAdd(port, { content: 'p', status: 'cancelled' });
    const child = await planTaskAdd(port, { content: 'c', parentTaskId: parent.taskId, status: 'in_progress' });
    const stored = await listAll(storage);
    expect(stored.get(parent.taskId)?.status).toBe('cancelled');
    expect(stored.get(parent.taskId)?.statusSource).toBe('explicit');
    expect(stored.get(child.taskId)?.status).toBe('in_progress');
  });
});

describe('blockedBy → blocked rollup', () => {
  it('an unsatisfied blockedBy dep marks a leaf-with-deps parent blocked', async () => {
    const { storage, port } = await setup();
    const dep = await planTaskAdd(port, { content: 'dep' });
    const parent = await planTaskAdd(port, { content: 'p' });
    const child = await planTaskAdd(port, { content: 'c', parentTaskId: parent.taskId });
    // Give the parent a dep that's still pending → parent rolls to blocked.
    await planTaskUpdate(port, parent.taskId, { blockedBy: [dep.taskId] });
    let stored = await listAll(storage);
    expect(stored.get(parent.taskId)?.status).toBe('blocked');
    // Completing the dep releases the block → back to pending (child pending).
    await planTaskComplete(port, dep.taskId);
    stored = await listAll(storage);
    expect(stored.get(parent.taskId)?.status).toBe('pending');
    void child;
  });
});

// ---------------------------------------------------------------------------
// Cycle prevention
// ---------------------------------------------------------------------------

describe('cycle prevention', () => {
  it('reparent rejects a cycle (task under its own descendant)', async () => {
    const { port } = await setup();
    const a = await planTaskAdd(port, { content: 'a' });
    const b = await planTaskAdd(port, { content: 'b', parentTaskId: a.taskId });
    await expect(planTaskReparent(port, a.taskId, b.taskId)).rejects.toThrow(HarnessPlanTaskCycleError);
  });

  it('reparent to root and back works (no false cycle)', async () => {
    const { storage, port } = await setup();
    const a = await planTaskAdd(port, { content: 'a' });
    const b = await planTaskAdd(port, { content: 'b', parentTaskId: a.taskId });
    await planTaskReparent(port, b.taskId, null);
    let stored = await listAll(storage);
    expect(stored.get(b.taskId)?.parentTaskId).toBeUndefined();
    await planTaskReparent(port, b.taskId, a.taskId);
    stored = await listAll(storage);
    expect(stored.get(b.taskId)?.parentTaskId).toBe(a.taskId);
  });

  it('task_update rejects a blockedBy dependency cycle', async () => {
    const { port } = await setup();
    const a = await planTaskAdd(port, { content: 'a' });
    const b = await planTaskAdd(port, { content: 'b', blockedBy: [a.taskId] });
    // a blockedBy [b] would close a→b→a.
    await expect(planTaskUpdate(port, a.taskId, { blockedBy: [b.taskId] })).rejects.toThrow(HarnessPlanTaskCycleError);
  });
});

// ---------------------------------------------------------------------------
// Single in_progress per root
// ---------------------------------------------------------------------------

describe('per-root single in_progress', () => {
  it('rejects a second in_progress in the same root', async () => {
    const { port } = await setup();
    const root = await planTaskAdd(port, { content: 'root' });
    const c1 = await planTaskAdd(port, { content: 'c1', parentTaskId: root.taskId });
    const c2 = await planTaskAdd(port, { content: 'c2', parentTaskId: root.taskId });
    await planTaskUpdate(port, c1.taskId, { status: 'in_progress' });
    await expect(planTaskUpdate(port, c2.taskId, { status: 'in_progress' })).rejects.toThrow(
      HarnessPlanTaskInProgressConflictError,
    );
  });

  it('allows in_progress across DIFFERENT roots', async () => {
    const { port } = await setup();
    const rootA = await planTaskAdd(port, { content: 'rootA' });
    const rootB = await planTaskAdd(port, { content: 'rootB' });
    await planTaskUpdate(port, rootA.taskId, { status: 'in_progress' });
    await expect(planTaskUpdate(port, rootB.taskId, { status: 'in_progress' })).resolves.toBeDefined();
  });
});

// ---------------------------------------------------------------------------
// Bounded read (task_check / plan_task_check)
// ---------------------------------------------------------------------------

describe('planTaskCheck — bounded read', () => {
  let h: Harness;
  let root: string;
  beforeEach(async () => {
    h = await setup();
    const r = await planTaskAdd(h.port, { content: 'root' });
    root = r.taskId;
    await planTaskDecompose(h.port, root, [{ content: 'c1' }, { content: 'c2' }, { content: 'c3' }]);
  });

  it('limits the number of returned nodes and reports truncation', async () => {
    const res = await planTaskCheck(h.port, { rootTaskId: root, limit: 2 });
    expect(res.tasks.length).toBe(2);
    expect(res.truncated).toBe(true);
  });

  it('honors depth (depth 0 returns only the root)', async () => {
    const res = await planTaskCheck(h.port, { rootTaskId: root, depth: 0, limit: 50 });
    expect(res.tasks.map(t => t.taskId)).toEqual([root]);
    expect(res.truncated).toBe(false);
  });

  it('filters by status (rollup makes the derived root in_progress too)', async () => {
    const all = await planTaskCheck(h.port, { rootTaskId: root, limit: 50 });
    const firstChild = all.tasks.find(t => t.parentTaskId === root)!;
    await planTaskUpdate(h.port, firstChild.taskId, { status: 'in_progress' });
    const res = await planTaskCheck(h.port, { rootTaskId: root, status: 'in_progress', limit: 50 });
    // The explicitly-set child plus its parent root, which rolled up to
    // in_progress (the derived root reflects its in_progress child).
    expect(new Set(res.tasks.map(t => t.taskId))).toEqual(new Set([firstChild.taskId, root]));
  });

  it('does not emit an event (read-only)', async () => {
    const before = h.events.length;
    await planTaskCheck(h.port, { limit: 10 });
    expect(h.events.length).toBe(before);
  });
});

// ---------------------------------------------------------------------------
// Transaction atomicity
// ---------------------------------------------------------------------------

describe('transaction atomicity', () => {
  it('a rejected decompose (cycle in commit) leaves NO rows written', async () => {
    // Force a multi-row op to fail mid-commit by racing the session version so
    // the fence rejects: bump the stored session version out from under the port.
    const { storage, port } = await setup();
    const parent = await planTaskAdd(port, { content: 'p' });
    const before = await listAll(storage);
    // Make the fence stale: the port reports version 1 but storage advances to 2.
    const rec = (await storage.loadSession({ sessionId: SESSION_ID }))!;
    await storage.saveSession({ ...rec, lastActivityAt: 1 }, { ownerId: OWNER, ifVersion: 1 });
    // Now a decompose under the stale port version must reject and write nothing.
    await expect(
      planTaskDecompose(port, parent.taskId, [{ content: 'c1' }, { content: 'c2' }]),
    ).rejects.toThrow();
    const after = await listAll(storage);
    expect(after.size).toBe(before.size); // only the original parent
  });

  it('the custom event for the mutating ops uses the dotted type', () => {
    expect(PLAN_TASK_UPDATED_EVENT).toBe('papersflow.plan_task.updated');
    expect(PLAN_TASK_UPDATED_EVENT.includes('.')).toBe(true);
  });
});
