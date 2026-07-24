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
  capturePlanTaskDelegationScope,
  planTaskAdd,
  planTaskDecompose,
  planTaskDelegate,
  planTaskCheck,
  planTaskReconcileDelegation,
  planTaskReparent,
  planTaskUpdate,
  PLAN_TASK_DELEGATED_BODY_MAX_BYTES,
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

  it('freezes the delegated assignment so an old child cannot settle renamed work', async () => {
    const { storage, port } = await setup();
    const task = await planTaskAdd(port, {
      content: 'Repair the methods section',
      activeForm: 'Repairing methods',
      priority: 1,
    });
    await planTaskDelegate(port, {
      taskId: task.taskId,
      subagentSessionId: 'child-methods',
      taskBody: 'Repair the methods section',
    });

    await expect(planTaskUpdate(port, task.taskId, { content: 'Rewrite the results section' })).rejects.toThrow(
      /assignment is immutable until settlement/,
    );
    await expect(planTaskUpdate(port, task.taskId, { activeForm: 'Rewriting results' })).rejects.toThrow(
      /assignment is immutable until settlement/,
    );
    await expect(planTaskUpdate(port, task.taskId, { priority: 99 })).rejects.toThrow(
      /assignment is immutable until settlement/,
    );

    const stored = await listAll(storage);
    expect(stored.get(task.taskId)).toMatchObject({
      content: 'Repair the methods section',
      activeForm: 'Repairing methods',
      priority: 1,
      delegatedSubagentSessionId: 'child-methods',
      status: 'in_progress',
    });
  });

  it('persists one absolute deadline with the ownership edge and preserves it through settlement', async () => {
    const { port } = await setup();
    const task = await planTaskAdd(port, { content: 'bounded research' });
    const startedAt = 1_800_000_000_000;
    const deadlineAt = startedAt + 90_000;

    const delegated = await planTaskDelegate(port, {
      taskId: task.taskId,
      subagentSessionId: 'child-deadline',
      startedAt,
      deadlineAt,
    });
    expect(delegated.delegation).toMatchObject({
      subagentSessionId: 'child-deadline',
      attemptId: expect.stringMatching(/^delegation-/),
      parentToolCallId: `delegate:${task.taskId}`,
      status: 'running',
      startedAt,
      deadlineAt,
    });

    const settled = await planTaskReconcileDelegation(port, {
      taskId: task.taskId,
      subagentSessionId: 'child-deadline',
      outcome: 'failed',
    });
    expect(settled.view?.delegation).toMatchObject({
      subagentSessionId: 'child-deadline',
      status: 'failed',
      startedAt,
      deadlineAt,
    });
  });

  it('rejects a delegation deadline that does not follow its start timestamp', async () => {
    const { port } = await setup();
    const task = await planTaskAdd(port, { content: 'invalid deadline' });
    await expect(
      planTaskDelegate(port, {
        taskId: task.taskId,
        subagentSessionId: 'child-invalid-deadline',
        startedAt: 42,
        deadlineAt: 42,
      }),
    ).rejects.toThrow(/later than startedAt/);
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

  it('requires includeSubtree for a non-leaf and settles the complete owned subtree', async () => {
    const { storage, port } = await setup();
    const root = await planTaskAdd(port, { content: 'root' });
    const [child] = await planTaskDecompose(port, root.taskId, [{ content: 'child' }]);
    const [grandchild] = await planTaskDecompose(port, child!.taskId, [{ content: 'grandchild' }]);

    await expect(planTaskDelegate(port, { taskId: root.taskId, subagentSessionId: 'sub-root' })).rejects.toThrow(
      /requires includeSubtree/,
    );

    await planTaskDelegate(port, {
      taskId: root.taskId,
      subagentSessionId: 'sub-root',
      includeSubtree: true,
    });
    const result = await planTaskReconcileDelegation(port, {
      taskId: root.taskId,
      subagentSessionId: 'sub-root',
      outcome: 'completed',
    });

    expect(result.reconciled).toBe(true);
    const stored = await listAll(storage);
    expect(stored.get(root.taskId)?.status).toBe('completed');
    expect(stored.get(child!.taskId)?.status).toBe('completed');
    expect(stored.get(grandchild!.taskId)?.status).toBe('completed');
    expect(stored.get(root.taskId)?.delegatedSubagentSessionId).toBeUndefined();
  });

  it('rejects overlapping ancestor/descendant delegation while allowing disjoint siblings', async () => {
    const { port } = await setup();
    const root = await planTaskAdd(port, { content: 'root' });
    const [a, b] = await planTaskDecompose(port, root.taskId, [{ content: 'a' }, { content: 'b' }]);

    await planTaskDelegate(port, { taskId: a!.taskId, subagentSessionId: 'sub-a' });
    await planTaskDelegate(port, { taskId: b!.taskId, subagentSessionId: 'sub-b' });
    await expect(
      planTaskDelegate(port, {
        taskId: root.taskId,
        subagentSessionId: 'sub-root',
        includeSubtree: true,
      }),
    ).rejects.toThrow(/delegated descendant/);

    await expect(
      planTaskDelegate(port, {
        taskId: a!.taskId,
        subagentSessionId: 'sub-a-2',
      }),
    ).rejects.toThrow(/already.*delegated/);
  });

  it('freezes execution state, structure, and assignment across an actively delegated subtree', async () => {
    const { port } = await setup();
    const root = await planTaskAdd(port, { content: 'root' });
    const [child] = await planTaskDecompose(port, root.taskId, [{ content: 'child' }]);
    const [grandchild] = await planTaskDecompose(port, child!.taskId, [{ content: 'grandchild' }]);
    await planTaskDelegate(port, {
      taskId: root.taskId,
      subagentSessionId: 'sub-root',
      includeSubtree: true,
    });

    await expect(planTaskAdd(port, { content: 'late child', parentTaskId: child!.taskId })).rejects.toThrow(
      /child-owned until settlement/,
    );
    await expect(planTaskDecompose(port, child!.taskId, [{ content: 'late child' }])).rejects.toThrow(
      /child-owned until settlement/,
    );
    await expect(planTaskReparent(port, grandchild!.taskId, null)).rejects.toThrow(/child-owned until settlement/);
    await expect(planTaskUpdate(port, grandchild!.taskId, { status: 'failed' })).rejects.toThrow(
      /execution state is child-owned/,
    );
    await expect(planTaskUpdate(port, grandchild!.taskId, { blockedBy: [] })).rejects.toThrow(
      /execution state is child-owned/,
    );
    await expect(planTaskUpdate(port, grandchild!.taskId, { content: 'clearer label' })).rejects.toThrow(
      /assignment is immutable/,
    );
  });

  it('requires every external dependency of an included descendant to be terminal-ok', async () => {
    const { port } = await setup();
    const external = await planTaskAdd(port, { content: 'external prerequisite' });
    const root = await planTaskAdd(port, { content: 'root' });
    await planTaskDecompose(port, root.taskId, [{ content: 'blocked descendant', blockedBy: [external.taskId] }]);

    await expect(capturePlanTaskDelegationScope(port, root.taskId, true)).rejects.toThrow(
      /has not completed successfully/,
    );

    await planTaskUpdate(port, external.taskId, { status: 'completed' });
    const snapshot = await capturePlanTaskDelegationScope(port, root.taskId, true);
    await expect(
      planTaskDelegate(port, {
        taskId: root.taskId,
        subagentSessionId: 'sub-root',
        includeSubtree: true,
        expectedScopeFingerprint: snapshot.fingerprint,
      }),
    ).resolves.toMatchObject({ delegatedSubagentSessionId: 'sub-root' });
  });

  it('rejects a stale scope snapshot after preflight instead of settling unseen tree changes', async () => {
    const { storage, port } = await setup();
    const root = await planTaskAdd(port, { content: 'root' });
    const [child] = await planTaskDecompose(port, root.taskId, [{ content: 'original child' }]);
    const snapshot = await capturePlanTaskDelegationScope(port, root.taskId, true);

    await planTaskUpdate(port, child!.taskId, { content: 'changed during child allocation' });
    await expect(
      planTaskDelegate(port, {
        taskId: root.taskId,
        subagentSessionId: 'orphan-child',
        includeSubtree: true,
        taskBody: 'stale rendered subtree',
        expectedScopeFingerprint: snapshot.fingerprint,
      }),
    ).rejects.toThrow(/changed while its delegated child was being allocated/);
    expect((await listAll(storage)).get(root.taskId)?.delegatedSubagentSessionId).toBeUndefined();
  });

  it('rejects an unknown task and an empty subagent id', async () => {
    const { port } = await setup();
    await expect(planTaskDelegate(port, { taskId: 'nope', subagentSessionId: 'c' })).rejects.toThrow(/unknown task/);
    const t = await planTaskAdd(port, { content: 't' });
    await expect(planTaskDelegate(port, { taskId: t.taskId, subagentSessionId: '' })).rejects.toThrow(
      /non-empty string/,
    );
  });

  it('rejects an oversized immutable delegation body before writing the link', async () => {
    const { storage, port } = await setup();
    const task = await planTaskAdd(port, { content: 'bounded task' });
    await expect(
      planTaskDelegate(port, {
        taskId: task.taskId,
        subagentSessionId: 'sub-oversized',
        taskBody: 'x'.repeat(PLAN_TASK_DELEGATED_BODY_MAX_BYTES + 1),
      }),
    ).rejects.toThrow(/UTF-8 bytes/);
    expect((await listAll(storage)).get(task.taskId)?.delegatedSubagentSessionId).toBeUndefined();
  });
});

describe('planTaskReconcileDelegation', () => {
  it('persists the bounded terminal result for later plan reads and settlement events', async () => {
    const { port, events } = await setup();
    const task = await planTaskAdd(port, { content: 'inspect both sources' });
    await planTaskDelegate(port, {
      taskId: task.taskId,
      subagentSessionId: 'sub-result',
      taskBody: 'compare source A with source B',
    });
    const result = {
      status: 'success' as const,
      text: 'Source A and source B disagree on the timeout default.',
      textTruncated: false,
      finishReason: 'stop',
      stepCount: 3,
      toolCallCount: 2,
      toolResultCount: 2,
      usage: { inputTokens: 120, outputTokens: 36, totalTokens: 156 },
    };

    const first = await planTaskReconcileDelegation(port, {
      taskId: task.taskId,
      subagentSessionId: 'sub-result',
      outcome: 'completed',
      result,
    });

    expect(first.view).toMatchObject({
      status: 'completed',
      delegation: {
        subagentSessionId: 'sub-result',
        status: 'completed',
        result,
      },
    });
    expect(first.view?.delegatedSubagentSessionId).toBeUndefined();
    expect(first.view?.delegation?.settledAt).toEqual(expect.any(Number));

    const checked = await planTaskCheck(port, { rootTaskId: task.taskId });
    expect(checked.tasks).toHaveLength(1);
    expect(checked.tasks[0]?.delegation).toMatchObject({
      subagentSessionId: 'sub-result',
      status: 'completed',
      result,
    });
    expect(events.at(-1)).toMatchObject({
      op: 'delegate_settled',
      deltas: [
        {
          taskId: task.taskId,
          delegatedSubagentSessionId: null,
          delegation: {
            subagentSessionId: 'sub-result',
            status: 'completed',
            result,
          },
        },
      ],
    });

    const duplicate = await planTaskReconcileDelegation(port, {
      taskId: task.taskId,
      subagentSessionId: 'sub-result',
      outcome: 'failed',
    });
    expect(duplicate.reconciled).toBe(false);
    expect(duplicate.view?.delegation).toMatchObject({
      subagentSessionId: 'sub-result',
      status: 'completed',
      result,
    });
  });

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

  it('persists a blocked delegation as blocked without misclassifying an external failure as task failure', async () => {
    const { storage, port } = await setup();
    const parent = await planTaskAdd(port, { content: 'repair and compile the project' });
    const [child] = await planTaskDecompose(port, parent.taskId, [{ content: 'compile the edited LaTeX source' }]);
    await planTaskDelegate(port, { taskId: child!.taskId, subagentSessionId: 'sub-compiler' });

    const result = {
      status: 'error' as const,
      outcome: 'blocked' as const,
      text: 'Source was re-read, but compilation could not run because the compiler service was unavailable.',
      textTruncated: false,
      finishReason: 'stop',
      stepCount: 3,
      toolCallCount: 2,
      toolResultCount: 2,
      evidence: [
        {
          kind: 'tool-result' as const,
          toolName: 'compile_latex',
          toolCallId: 'compile-blocked-1',
          status: 'error' as const,
          description: 'Compiler service returned unavailable before producing a diagnostic.',
        },
      ],
      issue: {
        code: 'compiler.unavailable',
        message: 'Compiler service unavailable.',
        retryable: true,
      },
      error: {
        code: 'compiler.unavailable',
        message: 'Compiler service unavailable.',
        messageTruncated: false,
      },
    };

    const reconciled = await planTaskReconcileDelegation(port, {
      taskId: child!.taskId,
      subagentSessionId: 'sub-compiler',
      outcome: 'blocked',
      result,
    });

    expect(reconciled).toMatchObject({
      reconciled: true,
      view: {
        status: 'blocked',
        delegation: {
          subagentSessionId: 'sub-compiler',
          status: 'blocked',
          result,
        },
      },
    });
    expect(reconciled.view?.delegatedSubagentSessionId).toBeUndefined();
    const stored = await listAll(storage);
    expect(stored.get(child!.taskId)?.status).toBe('blocked');
    expect(stored.get(parent.taskId)?.status).toBe('blocked');
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
