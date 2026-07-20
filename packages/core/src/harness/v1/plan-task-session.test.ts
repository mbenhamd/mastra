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

import { InMemoryHarness } from '../../storage/domains/harness/inmemory';
import type { JsonValue, SessionRecord } from '../../storage/domains/harness/types';
import { InMemoryDB } from '../../storage/domains/inmemory-db';
import { HarnessPlanTaskCycleError, HarnessPlanTaskInProgressConflictError } from './plan-task-hierarchy';
import {
  clampPlanTaskCheckLimit,
  planTaskAdd,
  planTaskCheck,
  planTaskComplete,
  planTaskDecompose,
  planTaskDelegate,
  planTaskReconcileDelegation,
  planTaskReparent,
  planTaskReset,
  planTaskUpdate,
  PLAN_TASK_CONTENT_MAX_BYTES,
  PLAN_TASK_CHECK_DEFAULT_LIMIT,
  PLAN_TASK_CHECK_MAX_LIMIT,
  PLAN_TASK_CHECK_MAX_OUTPUT_BYTES,
  PLAN_TASK_MAX_NODES,
  PLAN_TASK_UPDATED_EVENT,
} from './plan-task-session';
import type { PlanTaskSessionPort, PlanTaskSummary, PlanTaskUpdatedPayload } from './plan-task-session';

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
  db: InMemoryDB;
  storage: InMemoryHarness;
  port: PlanTaskSessionPort;
  events: PlanTaskUpdatedPayload[];
  summaries: PlanTaskSummary[];
}

async function setup(): Promise<Harness> {
  const db = new InMemoryDB();
  const storage = new InMemoryHarness({ db });
  await storage.createOrLoadActiveSession(sessionRecord(), { initialLease: { ownerId: OWNER, ttlMs: 60_000 } });
  const events: PlanTaskUpdatedPayload[] = [];
  const summaries: PlanTaskSummary[] = [];
  const port: PlanTaskSessionPort = {
    id: SESSION_ID,
    resourceId: 'r1',
    threadId: 't1',
    harnessName: 'default',
    storage,
    ownerId: OWNER,
    sessionVersion: 1,
    emitPlanTaskEvent: (payload: JsonValue) => {
      events.push(payload as unknown as PlanTaskUpdatedPayload);
    },
    setPlanTaskSummary: summary => {
      summaries.push(summary);
    },
  };
  return { db, storage, port, events, summaries };
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
    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({ op: 'add', affectedTaskIds: [view.taskId] });
  });

  it('appends order among siblings', async () => {
    const { port } = await setup();
    const a = await planTaskAdd(port, { content: 'a' });
    const b = await planTaskAdd(port, { content: 'b' });
    expect(b.order).toBe(a.order + 1);
  });

  it('enforces the UTF-8 task-label budget on add, decompose, and update', async () => {
    const { port } = await setup();
    // 257 two-byte code points stay below the character cap but exceed 512 UTF-8 bytes.
    const oversized = 'é'.repeat(Math.floor(PLAN_TASK_CONTENT_MAX_BYTES / 2) + 1);
    await expect(planTaskAdd(port, { content: oversized })).rejects.toThrow(/UTF-8 bytes/);

    const root = await planTaskAdd(port, { content: 'root' });
    await expect(planTaskDecompose(port, root.taskId, [{ content: oversized }])).rejects.toThrow(/UTF-8 bytes/);
    await expect(planTaskUpdate(port, root.taskId, { content: oversized })).rejects.toThrow(/UTF-8 bytes/);
    await expect(planTaskUpdate(port, root.taskId, { content: '' })).rejects.toThrow(/non-empty string/);
  });

  it('admits exactly the transaction-safe node budget and rejects the next add without a partial row', async () => {
    const { storage, port } = await setup();
    const root = await planTaskAdd(port, { content: 'root' });
    await planTaskDecompose(
      port,
      root.taskId,
      Array.from({ length: PLAN_TASK_MAX_NODES - 1 }, (_, index) => ({ content: `child-${index}` })),
    );

    await expect(planTaskAdd(port, { content: 'over budget' })).rejects.toThrow(/limit is 100 nodes/);
    expect((await listAll(storage)).size).toBe(PLAN_TASK_MAX_NODES);
  });

  it('rejects an over-budget atomic decompose before writing any child', async () => {
    const { storage, port } = await setup();
    const root = await planTaskAdd(port, { content: 'root' });
    await planTaskDecompose(
      port,
      root.taskId,
      Array.from({ length: PLAN_TASK_MAX_NODES - 2 }, (_, index) => ({ content: `existing-${index}` })),
    );
    const before = await listAll(storage);

    await expect(
      planTaskDecompose(port, root.taskId, [{ content: 'too-many-a' }, { content: 'too-many-b' }]),
    ).rejects.toThrow(/limit is 100 nodes/);
    expect(await listAll(storage)).toEqual(before);
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
    expect(events.at(-1)).toMatchObject({
      op: 'decompose',
      affectedTaskIds: [parent.taskId, ...children.map(c => c.taskId)],
    });
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

  it('a terminal parent cannot gain children that its status would mask', async () => {
    const { port } = await setup();
    const parent = await planTaskAdd(port, { content: 'p', status: 'cancelled' });
    await expect(
      planTaskAdd(port, { content: 'c', parentTaskId: parent.taskId, status: 'in_progress' }),
    ).rejects.toThrow('cannot receive new children');
  });
});

describe('planTaskReset', () => {
  it('atomically clears every root, returns delegation links, and refreshes the empty summary', async () => {
    const { storage, port, events, summaries } = await setup();
    const firstRoot = await planTaskAdd(port, { content: 'first root' });
    const [delegatedChild] = await planTaskDecompose(port, firstRoot.taskId, [{ content: 'delegated child' }]);
    const secondRoot = await planTaskAdd(port, { content: 'second root' });
    await planTaskDelegate(port, {
      taskId: delegatedChild!.taskId,
      subagentSessionId: 'child-session-1',
      subagentTypeId: 'critic',
    });

    const result = await planTaskReset(port);

    expect(result).toEqual({
      deletedCount: 3,
      delegatedSubagentSessionIds: ['child-session-1'],
    });
    expect((await listAll(storage)).size).toBe(0);
    expect(summaries.at(-1)).toEqual({
      total: 0,
      byStatus: {},
      inProgressTaskIds: [],
      rootCount: 0,
    });
    expect(events.at(-1)).toMatchObject({
      op: 'reset',
      affectedTaskIds: [firstRoot.taskId, secondRoot.taskId],
      deltas: [],
    });
  });

  it('is idempotent when the plan is already empty', async () => {
    const { port, events, summaries } = await setup();
    await expect(planTaskReset(port)).resolves.toEqual({
      deletedCount: 0,
      delegatedSubagentSessionIds: [],
    });
    expect(events).toHaveLength(0);
    expect(summaries.at(-1)?.total).toBe(0);
  });

  it('leaves the complete forest intact when the session fence is stale', async () => {
    const { storage, port } = await setup();
    await planTaskAdd(port, { content: 'root one' });
    await planTaskAdd(port, { content: 'root two' });
    const record = (await storage.loadSession({ sessionId: SESSION_ID }))!;
    await storage.saveSession({ ...record, lastActivityAt: 1 }, { ownerId: OWNER, ifVersion: 1 });

    await expect(planTaskReset(port)).rejects.toThrow();
    expect((await listAll(storage)).size).toBe(2);
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

  // Finding 2: a LEAF (no children) with an unsatisfied blockedBy dep must
  // surface 'blocked' — rollup is not limited to nodes that already have
  // children. The leaf keeps statusSource 'explicit' and reverts when released.
  it('a leaf with an unsatisfied blockedBy dep returns its canonical rolled-up post-image', async () => {
    const { storage, port, events } = await setup();
    const dep = await planTaskAdd(port, { content: 'dep' });
    const leaf = await planTaskAdd(port, { content: 'leaf' });
    // leaf is a childless explicit 'pending' node; give it a pending dep.
    const returned = await planTaskUpdate(port, leaf.taskId, { blockedBy: [dep.taskId] });
    let stored = await listAll(storage);
    expect(stored.get(leaf.taskId)?.status).toBe('blocked');
    // A blockedBy overlay flips the node derived so it reverts when the dep clears.
    expect(stored.get(leaf.taskId)?.statusSource).toBe('derived');
    expect(returned).toMatchObject({ status: 'blocked', statusSource: 'derived' });
    expect(events.at(-1)?.deltas.find(delta => delta.taskId === leaf.taskId)).toMatchObject({
      status: returned.status,
      statusSource: returned.statusSource,
    });
    // Completing the dep releases the block → leaf reverts to pending.
    await planTaskComplete(port, dep.taskId);
    stored = await listAll(storage);
    expect(stored.get(leaf.taskId)?.status).toBe('pending');
  });

  it('rejects creating an in_progress task whose dependency is unsatisfied', async () => {
    const { storage, port } = await setup();
    const dependency = await planTaskAdd(port, { content: 'dependency' });
    await expect(
      planTaskAdd(port, {
        content: 'cannot start yet',
        status: 'in_progress',
        blockedBy: [dependency.taskId],
      }),
    ).rejects.toThrow(/cannot start/);
    expect((await listAll(storage)).size).toBe(1);
  });

  it('rejects moving a blocked task to in_progress until its dependency settles', async () => {
    const { storage, port } = await setup();
    const dependency = await planTaskAdd(port, { content: 'dependency' });
    const dependent = await planTaskAdd(port, { content: 'dependent', blockedBy: [dependency.taskId] });

    await expect(planTaskUpdate(port, dependent.taskId, { status: 'in_progress' })).rejects.toThrow(/cannot start/);
    expect((await listAll(storage)).get(dependent.taskId)?.status).toBe('blocked');

    await planTaskComplete(port, dependency.taskId);
    await expect(planTaskUpdate(port, dependent.taskId, { status: 'in_progress' })).resolves.toMatchObject({
      status: 'in_progress',
    });
  });

  it.each(['failed', 'cancelled'] as const)(
    'keeps a dependent blocked when its prerequisite becomes %s',
    async prerequisiteStatus => {
      const { storage, port } = await setup();
      const prerequisite = await planTaskAdd(port, { content: 'compile with service' });
      const repair = await planTaskAdd(port, {
        content: 'repair source diagnostics',
        blockedBy: [prerequisite.taskId],
      });

      await planTaskUpdate(port, prerequisite.taskId, { status: prerequisiteStatus });
      expect((await listAll(storage)).get(repair.taskId)?.status).toBe('blocked');
      await expect(planTaskUpdate(port, repair.taskId, { status: 'in_progress' })).rejects.toThrow(
        /unsatisfied blockedBy dependencies/,
      );
    },
  );
});

// ---------------------------------------------------------------------------
// Finding 1: explicit NON-terminal parent re-derives from children
// ---------------------------------------------------------------------------

describe('parent status remains hierarchy-owned after decomposition', () => {
  it('rejects re-marking a non-leaf parent and still rolls child failure up', async () => {
    const { storage, port } = await setup();
    const parent = await planTaskAdd(port, { content: 'p' });
    const [c1] = await planTaskDecompose(port, parent.taskId, [{ content: 'c1' }, { content: 'c2' }]);
    await expect(planTaskUpdate(port, parent.taskId, { status: 'pending' })).rejects.toThrow(
      'status is owned by hierarchy rollup',
    );
    let stored = await listAll(storage);
    expect(stored.get(parent.taskId)?.status).toBe('pending');
    expect(stored.get(parent.taskId)?.statusSource).toBe('derived');
    await planTaskUpdate(port, c1!.taskId, { status: 'failed' });
    stored = await listAll(storage);
    expect(stored.get(parent.taskId)?.status).toBe('failed');
    expect(stored.get(parent.taskId)?.statusSource).toBe('derived');
  });

  it('rejects explicit terminal completion of a parent with unfinished children', async () => {
    const { storage, port } = await setup();
    const parent = await planTaskAdd(port, { content: 'p' });
    const [c1] = await planTaskDecompose(port, parent.taskId, [{ content: 'c1' }, { content: 'c2' }]);
    await expect(planTaskUpdate(port, parent.taskId, { status: 'cancelled' })).rejects.toThrow(
      'status is owned by hierarchy rollup',
    );
    await planTaskUpdate(port, c1!.taskId, { status: 'failed' });
    const stored = await listAll(storage);
    expect(stored.get(parent.taskId)?.status).toBe('failed');
    expect(stored.get(parent.taskId)?.statusSource).toBe('derived');
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

  it('task_add rejects a child blocked by its own parent', async () => {
    const { port } = await setup();
    const parent = await planTaskAdd(port, { content: 'parent' });
    await expect(
      planTaskAdd(port, { content: 'child', parentTaskId: parent.taskId, blockedBy: [parent.taskId] }),
    ).rejects.toThrow(HarnessPlanTaskCycleError);
  });

  it('decompose rejects a child blocked by the parent it rolls up into', async () => {
    const { storage, port } = await setup();
    const parent = await planTaskAdd(port, { content: 'parent' });
    await expect(
      planTaskDecompose(port, parent.taskId, [{ content: 'child', blockedBy: [parent.taskId] }]),
    ).rejects.toThrow(HarnessPlanTaskCycleError);
    expect((await listAll(storage)).size).toBe(1);
  });

  it('reparent rejects creating a hierarchy/dependency deadlock', async () => {
    const { port } = await setup();
    const parent = await planTaskAdd(port, { content: 'parent' });
    const child = await planTaskAdd(port, { content: 'child', blockedBy: [parent.taskId] });
    await expect(planTaskReparent(port, child.taskId, parent.taskId)).rejects.toThrow(HarnessPlanTaskCycleError);
  });

  // Finding 4: decompose children[].blockedBy must get the SAME validation as
  // task_add / task_update (unknown / cross-session / cycle rejection).
  it('decompose rejects a child blockedBy referencing an unknown task', async () => {
    const { port } = await setup();
    const parent = await planTaskAdd(port, { content: 'p' });
    await expect(
      planTaskDecompose(port, parent.taskId, [{ content: 'c1', blockedBy: ['does-not-exist'] }]),
    ).rejects.toThrow(/unknown dependency/);
  });

  it('decompose accepts a child blockedBy referencing a known in-session task', async () => {
    const { storage, port } = await setup();
    const dep = await planTaskAdd(port, { content: 'dep' });
    const parent = await planTaskAdd(port, { content: 'p' });
    const [c1] = await planTaskDecompose(port, parent.taskId, [{ content: 'c1', blockedBy: [dep.taskId] }]);
    const stored = await listAll(storage);
    expect(stored.get(c1!.taskId)?.blockedBy).toEqual([dep.taskId]);
  });

  it('decompose resolves call-local sibling dependency keys atomically', async () => {
    const { storage, port } = await setup();
    const parent = await planTaskAdd(port, { content: 'parallel pipeline' });
    const [research, synthesis] = await planTaskDecompose(port, parent.taskId, [
      { content: 'research', localKey: 'research' },
      { content: 'synthesis', localKey: 'synthesis', blockedByLocalKeys: ['research'] },
    ]);

    expect(research?.decompositionLocalKey).toBe('research');
    expect(synthesis?.decompositionLocalKey).toBe('synthesis');
    expect((await listAll(storage)).get(synthesis!.taskId)?.blockedBy).toEqual([research!.taskId]);
  });

  it('decompose rejects duplicate or unknown call-local sibling keys before writing', async () => {
    const { storage, port } = await setup();
    const parent = await planTaskAdd(port, { content: 'pipeline' });

    await expect(
      planTaskDecompose(port, parent.taskId, [
        { content: 'a', localKey: 'same' },
        { content: 'b', localKey: 'same' },
      ]),
    ).rejects.toThrow(/duplicate localKey/);
    await expect(
      planTaskDecompose(port, parent.taskId, [{ content: 'a', localKey: 'a', blockedByLocalKeys: ['missing'] }]),
    ).rejects.toThrow(/unknown sibling localKey/);
    expect((await listAll(storage)).size).toBe(1);
  });

  it('decompose rejects a dependency cycle expressed through sibling local keys', async () => {
    const { storage, port } = await setup();
    const parent = await planTaskAdd(port, { content: 'pipeline' });
    await expect(
      planTaskDecompose(port, parent.taskId, [
        { content: 'a', localKey: 'a', blockedByLocalKeys: ['b'] },
        { content: 'b', localKey: 'b', blockedByLocalKeys: ['a'] },
      ]),
    ).rejects.toThrow(HarnessPlanTaskCycleError);
    expect((await listAll(storage)).size).toBe(1);
  });
});

describe('task_add idempotency', () => {
  it('returns the original durable task without a phantom summary or duplicate event', async () => {
    const { storage, port, events, summaries } = await setup();
    const first = await planTaskAdd(port, { content: 'compile report', idempotencyKey: 'turn-1-task-1' });
    const eventCount = events.length;
    const summaryCount = summaries.length;

    const retry = await planTaskAdd(port, {
      content: 'compile report',
      idempotencyKey: 'turn-1-task-1',
    });

    expect(retry).toEqual(first);
    expect((await listAll(storage)).size).toBe(1);
    expect(events).toHaveLength(eventCount);
    expect(summaries).toHaveLength(summaryCount);
    expect(summaries.at(-1)?.total).toBe(1);
  });

  it('replays the immutable create request after dependency rollup changes the stored status', async () => {
    const { storage, port, events, summaries } = await setup();
    const dependency = await planTaskAdd(port, { content: 'prepare source' });
    const input = {
      content: 'compile report',
      blockedBy: [dependency.taskId],
      idempotencyKey: 'turn-1-task-2',
    };
    const first = await planTaskAdd(port, input);
    expect(first.status).toBe('blocked');
    const eventCount = events.length;
    const summaryCount = summaries.length;

    const retry = await planTaskAdd(port, input);

    expect(retry).toEqual(first);
    expect((await listAll(storage)).size).toBe(2);
    expect(events).toHaveLength(eventCount);
    expect(summaries).toHaveLength(summaryCount);
  });

  it('replays the original create request after later updates and completion', async () => {
    const { storage, port, events, summaries } = await setup();
    const input = { content: 'compile report', priority: 1, idempotencyKey: 'turn-1-task-3' };
    const first = await planTaskAdd(port, input);
    await planTaskUpdate(port, first.taskId, { content: 'compile revised report', priority: 2 });
    await planTaskComplete(port, first.taskId);
    const eventCount = events.length;
    const summaryCount = summaries.length;

    const retry = await planTaskAdd(port, input);

    expect(retry).toMatchObject({
      taskId: first.taskId,
      content: 'compile revised report',
      priority: 2,
      status: 'completed',
    });
    expect((await listAll(storage)).size).toBe(1);
    expect(events).toHaveLength(eventCount);
    expect(summaries).toHaveLength(summaryCount);
  });

  it('rejects reuse of an idempotency key with different task input', async () => {
    const { storage, port } = await setup();
    await planTaskAdd(port, { content: 'compile report', idempotencyKey: 'turn-1-task-1' });

    await expect(
      planTaskAdd(port, { content: 'rewrite conclusions', idempotencyKey: 'turn-1-task-1' }),
    ).rejects.toThrow(/already used with different task input/);
    expect((await listAll(storage)).size).toBe(1);
  });

  it('preserves key-only replay for pre-hash rows without lazily binding mutable state', async () => {
    const { db, storage, port, events, summaries } = await setup();
    const first = await planTaskAdd(port, { content: 'legacy task', idempotencyKey: 'legacy-key' });
    for (const [key, task] of db.harnessPlanTasks) {
      if (task.taskId === first.taskId) {
        const { idempotencyInputHash: _removed, ...legacyTask } = task;
        db.harnessPlanTasks.set(key, legacyTask);
      }
    }
    await planTaskUpdate(port, first.taskId, { content: 'mutated after creation', priority: 9 });
    const eventCount = events.length;
    const summaryCount = summaries.length;

    const replay = await planTaskAdd(port, {
      content: 'different input cannot be authenticated for a legacy row',
      idempotencyKey: 'legacy-key',
    });

    expect(replay).toMatchObject({
      taskId: first.taskId,
      content: 'mutated after creation',
      priority: 9,
    });
    expect((await listAll(storage)).get(first.taskId)?.idempotencyInputHash).toBeUndefined();
    expect(events).toHaveLength(eventCount);
    expect(summaries).toHaveLength(summaryCount);
  });

  it('admits an idempotent retry even when the plan is already at the node cap', async () => {
    const { port } = await setup();
    const first = await planTaskAdd(port, { content: 'first', idempotencyKey: 'stable' });
    for (let i = 1; i < PLAN_TASK_MAX_NODES; i++) {
      await planTaskAdd(port, { content: `task-${i}` });
    }
    await expect(planTaskAdd(port, { content: 'first', idempotencyKey: 'stable' })).resolves.toEqual(first);
  });
});

// ---------------------------------------------------------------------------
// startedAt (span-summary O7)
// ---------------------------------------------------------------------------

describe('plan-task startedAt (O7)', () => {
  it('sets startedAt once on first in_progress, preserves it through completion', async () => {
    const { storage, port } = await setup();
    const t = await planTaskAdd(port, { content: 'work' });
    expect((await listAll(storage)).get(t.taskId)?.startedAt).toBeUndefined();

    await planTaskUpdate(port, t.taskId, { status: 'in_progress' });
    const started = (await listAll(storage)).get(t.taskId)?.startedAt;
    expect(typeof started).toBe('number');

    // Re-confirming in_progress does not move startedAt.
    await planTaskUpdate(port, t.taskId, { status: 'in_progress', activeForm: 'working' });
    expect((await listAll(storage)).get(t.taskId)?.startedAt).toBe(started);

    // Completing sets completedAt and keeps the original startedAt.
    await planTaskUpdate(port, t.taskId, { status: 'completed' });
    const done = (await listAll(storage)).get(t.taskId);
    expect(done?.completedAt).toBeGreaterThanOrEqual(started!);
    expect(done?.startedAt).toBe(started);
  });

  it('leaves startedAt unset for a task that never started', async () => {
    const { storage, port } = await setup();
    const t = await planTaskAdd(port, { content: 'never started' });
    await planTaskUpdate(port, t.taskId, { content: 'renamed' });
    expect((await listAll(storage)).get(t.taskId)?.startedAt).toBeUndefined();
  });

  it('stamps startedAt when a task is CREATED directly as in_progress', async () => {
    const { storage, port } = await setup();
    const t = await planTaskAdd(port, { content: 'born running', status: 'in_progress' });
    expect((await listAll(storage)).get(t.taskId)?.startedAt).toEqual(expect.any(Number));
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

  // Finding 5: re-confirming an already-in_progress child is idempotent — its
  // own derived rollup ancestor must NOT count as a competing in_progress.
  it('re-setting an already-in_progress child to in_progress is idempotent (derived parent is not a rival)', async () => {
    const { storage, port } = await setup();
    const parent = await planTaskAdd(port, { content: 'p' });
    const [c1] = await planTaskDecompose(port, parent.taskId, [{ content: 'c1' }, { content: 'c2' }]);
    await planTaskUpdate(port, c1!.taskId, { status: 'in_progress' });
    // The parent has now rolled up to derived in_progress.
    let stored = await listAll(storage);
    expect(stored.get(parent.taskId)?.status).toBe('in_progress');
    expect(stored.get(parent.taskId)?.statusSource).toBe('derived');
    // Re-confirm c1 in_progress — must NOT throw on the derived parent.
    await expect(planTaskUpdate(port, c1!.taskId, { status: 'in_progress' })).resolves.toBeDefined();
  });

  // Finding 3: a reparent that merges two roots, each holding their own explicit
  // in_progress, must re-run the per-root single-in_progress invariant and reject.
  it('reparent rejects merging two roots that each hold an explicit in_progress', async () => {
    const { port } = await setup();
    const rootA = await planTaskAdd(port, { content: 'rootA' });
    const rootB = await planTaskAdd(port, { content: 'rootB' });
    await planTaskUpdate(port, rootA.taskId, { status: 'in_progress' });
    await planTaskUpdate(port, rootB.taskId, { status: 'in_progress' });
    // Moving rootB under rootA would put two explicit in_progress in one root.
    await expect(planTaskReparent(port, rootB.taskId, rootA.taskId)).rejects.toThrow(
      HarnessPlanTaskInProgressConflictError,
    );
  });

  it('reparent allows a move that keeps a single in_progress per root', async () => {
    const { storage, port } = await setup();
    const rootA = await planTaskAdd(port, { content: 'rootA' });
    const rootB = await planTaskAdd(port, { content: 'rootB' });
    await planTaskUpdate(port, rootA.taskId, { status: 'in_progress' });
    // rootB has no in_progress → moving it under rootA is fine.
    await expect(planTaskReparent(port, rootB.taskId, rootA.taskId)).resolves.toBeUndefined();
    const stored = await listAll(storage);
    expect(stored.get(rootB.taskId)?.parentTaskId).toBe(rootA.taskId);
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
    expect(res.nextCursor).toBe(res.tasks[1]?.taskId);
  });

  it('continues from nextCursor without repeating tasks', async () => {
    const first = await planTaskCheck(h.port, { rootTaskId: root, limit: 2 });
    const second = await planTaskCheck(h.port, {
      rootTaskId: root,
      limit: 2,
      cursor: first.nextCursor,
    });
    const combinedIds = [...first.tasks, ...second.tasks].map(task => task.taskId);
    expect(combinedIds).toHaveLength(4);
    expect(combinedIds[0]).toBe(root);
    expect(new Set(combinedIds).size).toBe(4);
    expect(second.truncated).toBe(false);
    expect(second.nextCursor).toBeUndefined();
  });

  it('caps the serialized response and exposes a continuation for large delegation results', async () => {
    const { port } = await setup();
    const largeResult = {
      status: 'success' as const,
      text: 'x'.repeat(40 * 1024),
      textTruncated: false,
      finishReason: 'stop',
      stepCount: 1,
      toolCallCount: 0,
      toolResultCount: 0,
    };
    for (let index = 0; index < 3; index += 1) {
      const task = await planTaskAdd(port, { content: `large result ${index}` });
      const subagentSessionId = `subagent-${index}`;
      await planTaskDelegate(port, {
        taskId: task.taskId,
        subagentSessionId,
      });
      await planTaskReconcileDelegation(port, {
        taskId: task.taskId,
        subagentSessionId,
        outcome: 'completed',
        result: largeResult,
      });
    }

    const first = await planTaskCheck(port, { limit: PLAN_TASK_CHECK_MAX_LIMIT });
    expect(new TextEncoder().encode(JSON.stringify(first)).byteLength).toBeLessThanOrEqual(
      PLAN_TASK_CHECK_MAX_OUTPUT_BYTES,
    );
    expect(first.tasks).toHaveLength(1);
    expect(first.truncated).toBe(true);
    expect(first.nextCursor).toBe(first.tasks[0]?.taskId);

    const second = await planTaskCheck(port, {
      limit: PLAN_TASK_CHECK_MAX_LIMIT,
      cursor: first.nextCursor,
    });
    expect(new TextEncoder().encode(JSON.stringify(second)).byteLength).toBeLessThanOrEqual(
      PLAN_TASK_CHECK_MAX_OUTPUT_BYTES,
    );
    expect(second.tasks).toHaveLength(1);
    expect(second.tasks[0]?.taskId).not.toBe(first.tasks[0]?.taskId);
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

  it('does not emit an event nor refresh the summary (read-only)', async () => {
    const beforeEvents = h.events.length;
    const beforeSummaries = h.summaries.length;
    await planTaskCheck(h.port, { limit: 10 });
    expect(h.events.length).toBe(beforeEvents);
    expect(h.summaries.length).toBe(beforeSummaries);
  });

  it('rejects an unknown root instead of presenting it as an empty plan', async () => {
    await expect(planTaskCheck(h.port, { rootTaskId: 'missing-task', limit: 10 })).rejects.toMatchObject({
      name: 'HarnessValidationError',
      field: 'rootTaskId',
    });
  });
});

// ---------------------------------------------------------------------------
// TM-5: enriched event payload (per-task deltas, incl. rollup cascade)
// ---------------------------------------------------------------------------

/** A value is JSON-safe iff it round-trips through JSON without loss. */
function assertJsonSafe(value: unknown): void {
  expect(JSON.parse(JSON.stringify(value))).toEqual(value);
}

describe('TM-5 enriched event payload', () => {
  it('add carries a compact delta with status/source/order/content + parentTaskId', async () => {
    const { port, events } = await setup();
    const parent = await planTaskAdd(port, { content: 'parent' });
    const child = await planTaskAdd(port, { content: 'child', parentTaskId: parent.taskId, status: 'in_progress' });

    const payload = events.at(-1)!;
    expect(payload.op).toBe('add');
    const childDelta = payload.deltas.find(d => d.taskId === child.taskId)!;
    expect(childDelta).toMatchObject({
      taskId: child.taskId,
      parentTaskId: parent.taskId,
      status: 'in_progress',
      statusSource: 'explicit',
      content: 'child',
    });
    expect(typeof childDelta.order).toBe('number');
    assertJsonSafe(payload);
  });

  it('add cascades the rollup: the parent that flipped derived appears in deltas with its rolled-up status', async () => {
    const { port, events } = await setup();
    const parent = await planTaskAdd(port, { content: 'parent' });
    // Adding an in_progress child flips the parent to derived AND rolls it up.
    const child = await planTaskAdd(port, { content: 'child', parentTaskId: parent.taskId, status: 'in_progress' });

    const payload = events.at(-1)!;
    const ids = payload.deltas.map(d => d.taskId).sort();
    expect(ids).toEqual([parent.taskId, child.taskId].sort());
    const parentDelta = payload.deltas.find(d => d.taskId === parent.taskId)!;
    // The parent's status CHANGED via rollup (pending → in_progress) and its
    // source flipped to derived — included even though it was not directly edited.
    expect(parentDelta).toMatchObject({ status: 'in_progress', statusSource: 'derived' });
    // A pure-rollup parent delta omits content (compact).
    expect(parentDelta.content).toBeUndefined();
  });

  it('complete cascades: a derived parent whose status changes is in the deltas, bounded to the affected set', async () => {
    const { port, events } = await setup();
    const parent = await planTaskAdd(port, { content: 'p' });
    const [c1, c2] = await planTaskDecompose(port, parent.taskId, [{ content: 'c1' }, { content: 'c2' }]);
    // An unrelated, untouched root must NOT appear in the complete's deltas.
    const other = await planTaskAdd(port, { content: 'other' });
    await planTaskComplete(port, c1!.taskId);
    // c1 completed but c2 still pending → parent stays pending (no parent delta yet).
    let payload = events.at(-1)!;
    expect(payload.deltas.map(d => d.taskId)).toEqual([c1!.taskId]);

    await planTaskComplete(port, c2!.taskId);
    payload = events.at(-1)!;
    const ids = new Set(payload.deltas.map(d => d.taskId));
    // c2 (edited) + parent (rolled up to completed) — and NOT the unrelated root.
    expect(ids).toEqual(new Set([c2!.taskId, parent.taskId]));
    expect(ids.has(other.taskId)).toBe(false);
    const parentDelta = payload.deltas.find(d => d.taskId === parent.taskId)!;
    expect(parentDelta).toMatchObject({ status: 'completed', statusSource: 'derived' });
    assertJsonSafe(payload);
  });

  it('decompose deltas cover the created children + the parent that flipped derived', async () => {
    const { port, events } = await setup();
    const parent = await planTaskAdd(port, { content: 'p' });
    const children = await planTaskDecompose(port, parent.taskId, [{ content: 'c1' }, { content: 'c2' }]);
    const payload = events.at(-1)!;
    const ids = new Set(payload.deltas.map(d => d.taskId));
    expect(ids).toEqual(new Set([parent.taskId, ...children.map(c => c.taskId)]));
    // Children carry content (this op created them); each has the parent edge.
    for (const c of children) {
      const d = payload.deltas.find(x => x.taskId === c.taskId)!;
      expect(d.parentTaskId).toBe(parent.taskId);
      expect(d.content).toBe(c.content);
    }
  });
});

// ---------------------------------------------------------------------------
// TM-5: bounded display-state summary (pushed to the session on mutation)
// ---------------------------------------------------------------------------

describe('TM-5 plan-task summary', () => {
  it('counts/byStatus/inProgressTaskIds/rootCount track add → decompose → complete', async () => {
    const { port, summaries } = await setup();

    const r1 = await planTaskAdd(port, { content: 'r1' });
    expect(summaries.at(-1)).toEqual({
      total: 1,
      byStatus: { pending: 1 },
      inProgressTaskIds: [],
      rootCount: 1,
    });

    // Second root.
    const r2 = await planTaskAdd(port, { content: 'r2' });
    expect(summaries.at(-1)).toMatchObject({ total: 2, rootCount: 2, byStatus: { pending: 2 } });

    // Decompose r1 into two children: 4 nodes, 2 roots; r1 flips derived but
    // stays pending (children pending).
    const [c1, c2] = await planTaskDecompose(port, r1.taskId, [{ content: 'c1' }, { content: 'c2' }]);
    expect(summaries.at(-1)).toMatchObject({ total: 4, rootCount: 2, byStatus: { pending: 4 } });

    // Set c1 in_progress → its derived root r1 rolls up to in_progress too.
    await planTaskUpdate(port, c1!.taskId, { status: 'in_progress' });
    let s = summaries.at(-1)!;
    expect(s.total).toBe(4);
    expect(s.byStatus.in_progress).toBe(2); // c1 + derived r1
    expect(s.byStatus.pending).toBe(2); // c2 + r2
    expect(new Set(s.inProgressTaskIds)).toEqual(new Set([c1!.taskId, r1.taskId]));

    // Complete c1 and c2 → r1 rolls up to completed.
    await planTaskUpdate(port, c1!.taskId, { status: 'completed' });
    await planTaskComplete(port, c2!.taskId);
    s = summaries.at(-1)!;
    expect(s.byStatus.completed).toBe(3); // c1 + c2 + derived r1
    expect(s.byStatus.pending).toBe(1); // r2
    expect(s.inProgressTaskIds).toEqual([]);
    void r2;
  });

  it('is BOUNDED — it does NOT embed the full task tree, only counts + active ids + root count', async () => {
    const { port, summaries } = await setup();
    const root = await planTaskAdd(port, { content: 'root' });
    await planTaskDecompose(port, root.taskId, [{ content: 'c1' }, { content: 'c2' }, { content: 'c3' }]);
    const s = summaries.at(-1)!;
    // Exactly the bounded keys — no per-task array / content / tree embedded.
    expect(Object.keys(s).sort()).toEqual(['byStatus', 'inProgressTaskIds', 'rootCount', 'total']);
    // inProgressTaskIds is bounded by active focus, NOT total tasks.
    expect(s.total).toBe(4);
    expect(s.inProgressTaskIds.length).toBe(0);
    assertJsonSafe(s);
  });

  it('the summary is computed from the in-memory post-image (no extra storage read on mutation)', async () => {
    const { storage, port, summaries } = await setup();
    // Count storage list calls to prove the summary refresh adds none beyond the
    // op's own loadAllTasks.
    let listCalls = 0;
    const origList = storage.listPlanTasks.bind(storage);
    (storage as unknown as { listPlanTasks: typeof storage.listPlanTasks }).listPlanTasks = (args => {
      listCalls += 1;
      return origList(args);
    }) as typeof storage.listPlanTasks;

    await planTaskAdd(port, { content: 'a' });
    // One add does exactly one paged load for rollup/cycle work; the summary
    // refresh reuses that post-image, so no SECOND list call is made.
    expect(listCalls).toBe(1);
    expect(summaries).toHaveLength(1);
  });
});

// ---------------------------------------------------------------------------
// countPlanTasksByStatus — exact aggregate for the display-state seed (Finding 1)
// ---------------------------------------------------------------------------

describe('countPlanTasksByStatus — exact aggregate', () => {
  it('counts the whole bounded plan exactly', async () => {
    const { storage, port } = await setup();
    // Build the largest valid plan: one root plus 99 children.
    const root = await planTaskAdd(port, { content: 'root' });
    const total = 99;
    for (let i = 0; i < total; i++) {
      await planTaskAdd(port, {
        content: `c${i}`,
        parentTaskId: root.taskId,
        // status set explicitly so byStatus is deterministic; avoid a 2nd in_progress
        // sibling (the single-in-progress rule) by keeping them pending/completed.
        status: i % 2 === 0 ? 'pending' : 'completed',
      });
    }

    const counts = await storage.countPlanTasksByStatus({ sessionId: SESSION_ID });
    expect(counts.total).toBe(PLAN_TASK_MAX_NODES);
    // root flips derived; its rolled-up status depends on children, but the count
    // total is what matters for the LIE this fixes.
    const sumByStatus = Object.values(counts.byStatus).reduce((a, b) => a + (b ?? 0), 0);
    expect(sumByStatus).toBe(PLAN_TASK_MAX_NODES);
    // Exactly one root (the parent); all children resolve their parent.
    expect(counts.rootCount).toBe(1);
  });

  it('counts orphan nodes (unresolvable parent) as roots, matching computePlanTaskSummary', async () => {
    const { storage, port } = await setup();
    await planTaskAdd(port, { content: 'a' });
    await planTaskAdd(port, { content: 'b' });
    const counts = await storage.countPlanTasksByStatus({ sessionId: SESSION_ID });
    expect(counts.total).toBe(2);
    expect(counts.rootCount).toBe(2);
    expect(counts.byStatus.pending).toBe(2);
  });

  it('returns an empty summary for a session with no plan tasks', async () => {
    const { storage } = await setup();
    const counts = await storage.countPlanTasksByStatus({ sessionId: SESSION_ID });
    expect(counts).toEqual({ total: 0, byStatus: {}, rootCount: 0 });
  });
});

// ---------------------------------------------------------------------------
// clampPlanTaskCheckLimit — hostile limit cannot pull the whole tree (Finding 3)
// ---------------------------------------------------------------------------

describe('clampPlanTaskCheckLimit', () => {
  it('saturates a huge limit at the hard cap', () => {
    expect(clampPlanTaskCheckLimit(1_000_000)).toBe(PLAN_TASK_CHECK_MAX_LIMIT);
    expect(clampPlanTaskCheckLimit(Number.MAX_SAFE_INTEGER)).toBe(PLAN_TASK_CHECK_MAX_LIMIT);
  });

  it('falls back to the default for missing / non-positive / NaN / Infinity', () => {
    expect(clampPlanTaskCheckLimit(undefined)).toBe(PLAN_TASK_CHECK_DEFAULT_LIMIT);
    expect(clampPlanTaskCheckLimit(0)).toBe(PLAN_TASK_CHECK_DEFAULT_LIMIT);
    expect(clampPlanTaskCheckLimit(-50)).toBe(PLAN_TASK_CHECK_DEFAULT_LIMIT);
    expect(clampPlanTaskCheckLimit(Number.NaN)).toBe(PLAN_TASK_CHECK_DEFAULT_LIMIT);
    expect(clampPlanTaskCheckLimit(Number.POSITIVE_INFINITY)).toBe(PLAN_TASK_CHECK_DEFAULT_LIMIT);
  });

  it('floors a fractional limit and passes a sane in-range value through', () => {
    expect(clampPlanTaskCheckLimit(10.9)).toBe(10);
    expect(clampPlanTaskCheckLimit(50)).toBe(50);
  });

  it('planTaskCheck never reads more than the cap even when handed a hostile limit', async () => {
    const { storage, port } = await setup();
    const root = await planTaskAdd(port, { content: 'root' });
    // Fill the largest valid plan; a hostile caller still cannot raise the read
    // limit above the plan's transaction-safe node budget.
    const n = PLAN_TASK_MAX_NODES - 1;
    for (let i = 0; i < n; i++) {
      await planTaskAdd(port, { content: `c${i}`, parentTaskId: root.taskId });
    }
    const observedLimits: number[] = [];
    const origLoad = storage.loadPlanTaskSubtree.bind(storage);
    (storage as unknown as { loadPlanTaskSubtree: typeof storage.loadPlanTaskSubtree }).loadPlanTaskSubtree = (args => {
      observedLimits.push(args.limit);
      return origLoad(args);
    }) as typeof storage.loadPlanTaskSubtree;

    const res = await planTaskCheck(port, { rootTaskId: root.taskId, limit: 10_000_000 });
    expect(observedLimits).toContain(PLAN_TASK_CHECK_MAX_LIMIT);
    expect(Math.max(...observedLimits)).toBe(PLAN_TASK_CHECK_MAX_LIMIT);
    expect(res.tasks.length).toBeLessThanOrEqual(PLAN_TASK_CHECK_MAX_LIMIT);
    expect(res.truncated).toBe(false);
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
    await expect(planTaskDecompose(port, parent.taskId, [{ content: 'c1' }, { content: 'c2' }])).rejects.toThrow();
    const after = await listAll(storage);
    expect(after.size).toBe(before.size); // only the original parent
  });

  it('the custom event for the mutating ops uses the dotted type', () => {
    expect(PLAN_TASK_UPDATED_EVENT).toBe('papersflow.plan_task.updated');
    expect(PLAN_TASK_UPDATED_EVENT.includes('.')).toBe(true);
  });
});
