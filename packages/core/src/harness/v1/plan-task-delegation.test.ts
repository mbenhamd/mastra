/**
 * TM-6 — durable subtask → subagent DELEGATION pure ops (§5.1k / §5.6).
 *
 * Exercises the FENCED storage writes + rollup that back delegation, against
 * REAL InMemory storage through a fake `PlanTaskSessionPort` (the same seam the
 * §5.1k ops use). The subagent-spawn + completion-hook + reconcile-on-rehydrate
 * orchestration is covered separately in session.plan-task-delegation.test.ts;
 * here we lock the storage/rollup truth-table semantics of:
 *   - `planTaskDelegate`           (write the link + drive in_progress, fenced)
 *   - `planTaskReconcileDelegation` (roll up from a terminal subagent outcome)
 */

import { describe, expect, it } from 'vitest';

import { InMemoryHarness } from '../../storage/domains/harness/inmemory';
import type { JsonValue, SessionRecord } from '../../storage/domains/harness/types';
import { InMemoryDB } from '../../storage/domains/inmemory-db';
import { HarnessPlanTaskInProgressConflictError } from './plan-task-hierarchy';
import {
  planTaskAdd,
  planTaskDecompose,
  planTaskDelegate,
  planTaskReconcileDelegation,
  planTaskUpdate,
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

interface Fixture {
  storage: InMemoryHarness;
  port: PlanTaskSessionPort;
  events: PlanTaskUpdatedPayload[];
  summaries: PlanTaskSummary[];
}

async function setup(): Promise<Fixture> {
  const storage = new InMemoryHarness({ db: new InMemoryDB() });
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
  return { storage, port, events, summaries };
}

async function listAll(storage: InMemoryHarness) {
  const out = await storage.listPlanTasks({ sessionId: SESSION_ID, limit: 1000 });
  return new Map(out.tasks.map(t => [t.taskId, t]));
}

describe('planTaskDelegate', () => {
  it('sets delegatedSubagentSessionId + drives the task in_progress under the fence', async () => {
    const { storage, port, events } = await setup();
    const task = await planTaskAdd(port, { content: 'research' });
    const view = await planTaskDelegate(port, { taskId: task.taskId, subagentSessionId: 'child-1' });

    expect(view.status).toBe('in_progress');
    expect(view.statusSource).toBe('explicit');
    expect(view.delegatedSubagentSessionId).toBe('child-1');

    const stored = await listAll(storage);
    const row = stored.get(task.taskId)!;
    expect(row.delegatedSubagentSessionId).toBe('child-1');
    expect(row.status).toBe('in_progress');

    // Emits the delegate op event with the link in the delta.
    const evt = events.at(-1)!;
    expect(evt.op).toBe('delegate');
    expect(evt.affectedTaskIds).toEqual([task.taskId]);
    expect(evt.deltas.find(d => d.taskId === task.taskId)?.delegatedSubagentSessionId).toBe('child-1');
  });

  it('rejects a second in_progress in the same root (single-in_progress invariant)', async () => {
    const { port } = await setup();
    const a = await planTaskAdd(port, { content: 'a' });
    const b = await planTaskAdd(port, { content: 'b', parentTaskId: a.taskId });
    await planTaskUpdate(port, b.taskId, { status: 'in_progress' });
    // a and b share root a; delegating a (→ in_progress) conflicts with b.
    await expect(planTaskDelegate(port, { taskId: a.taskId, subagentSessionId: 'c' })).rejects.toBeInstanceOf(
      HarnessPlanTaskInProgressConflictError,
    );
  });

  it('rejects re-delegating to a different subagent session', async () => {
    const { port } = await setup();
    const t = await planTaskAdd(port, { content: 't' });
    await planTaskDelegate(port, { taskId: t.taskId, subagentSessionId: 'c1' });
    await expect(planTaskDelegate(port, { taskId: t.taskId, subagentSessionId: 'c2' })).rejects.toThrow(
      /already delegated/,
    );
  });

  it('rejects an unknown task and an empty subagent id', async () => {
    const { port } = await setup();
    await expect(planTaskDelegate(port, { taskId: 'nope', subagentSessionId: 'c' })).rejects.toThrow(/unknown task/);
    const t = await planTaskAdd(port, { content: 't' });
    await expect(planTaskDelegate(port, { taskId: t.taskId, subagentSessionId: '' })).rejects.toThrow(
      /non-empty string/,
    );
  });
});

describe('planTaskReconcileDelegation', () => {
  it('rolls a delegated task up to completed and cascades to the parent', async () => {
    const { storage, port } = await setup();
    const parent = await planTaskAdd(port, { content: 'parent' });
    const child = await planTaskDecompose(port, parent.taskId, [{ content: 'delegated child' }]);
    const childId = child[0]!.taskId;
    await planTaskDelegate(port, { taskId: childId, subagentSessionId: 'sub-1' });

    const res = await planTaskReconcileDelegation(port, {
      taskId: childId,
      subagentSessionId: 'sub-1',
      outcome: 'completed',
    });
    expect(res.reconciled).toBe(true);
    expect(res.view?.status).toBe('completed');

    const stored = await listAll(storage);
    expect(stored.get(childId)?.status).toBe('completed');
    expect(stored.get(childId)?.statusSource).toBe('explicit');
    // Parent is derived and rolls up to completed (only child completed).
    expect(stored.get(parent.taskId)?.status).toBe('completed');
  });

  it('rolls a delegated task up to failed and cascades failed to the parent', async () => {
    const { storage, port } = await setup();
    const parent = await planTaskAdd(port, { content: 'parent' });
    const children = await planTaskDecompose(port, parent.taskId, [{ content: 'a' }, { content: 'b' }]);
    const a = children[0]!.taskId;
    await planTaskDelegate(port, { taskId: a, subagentSessionId: 'sub-a' });

    await planTaskReconcileDelegation(port, { taskId: a, subagentSessionId: 'sub-a', outcome: 'failed' });

    const stored = await listAll(storage);
    expect(stored.get(a)?.status).toBe('failed');
    // any child failed → parent failed (TM-4 truth table).
    expect(stored.get(parent.taskId)?.status).toBe('failed');
  });

  it('is idempotent — a second reconcile after a terminal status is a no-op', async () => {
    const { port } = await setup();
    const t = await planTaskAdd(port, { content: 't' });
    await planTaskDelegate(port, { taskId: t.taskId, subagentSessionId: 'sub-1' });
    const first = await planTaskReconcileDelegation(port, {
      taskId: t.taskId,
      subagentSessionId: 'sub-1',
      outcome: 'completed',
    });
    expect(first.reconciled).toBe(true);
    const second = await planTaskReconcileDelegation(port, {
      taskId: t.taskId,
      subagentSessionId: 'sub-1',
      outcome: 'failed',
    });
    expect(second.reconciled).toBe(false);
    expect(second.view?.status).toBe('completed'); // not clobbered by the late failed signal
  });

  it('ignores a stale subagent id (task re-delegated to a different session)', async () => {
    const { storage, port } = await setup();
    const t = await planTaskAdd(port, { content: 't' });
    await planTaskDelegate(port, { taskId: t.taskId, subagentSessionId: 'sub-current' });
    // A late terminal callback from an abandoned session id must NOT clobber.
    const res = await planTaskReconcileDelegation(port, {
      taskId: t.taskId,
      subagentSessionId: 'sub-old',
      outcome: 'completed',
    });
    expect(res.reconciled).toBe(false);
    expect((await listAll(storage)).get(t.taskId)?.status).toBe('in_progress');
  });

  it('no-ops for an unknown task', async () => {
    const { port } = await setup();
    const res = await planTaskReconcileDelegation(port, {
      taskId: 'nope',
      subagentSessionId: 'sub-1',
      outcome: 'completed',
    });
    expect(res.reconciled).toBe(false);
  });
});
