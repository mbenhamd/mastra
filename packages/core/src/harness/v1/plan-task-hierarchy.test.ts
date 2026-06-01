/**
 * TM-4 plan-task hierarchy semantics (§5.1k) — pure logic.
 *
 * Locks the RATIFIED rollup truth-table (every precedence branch +
 * explicit-never-overwritten), `blockedBy`→blocked, cycle detection across
 * parentTaskId and blockedBy, and per-root single-in_progress. These run over
 * `HarnessPlanTask[]` snapshots with no storage so each branch is isolated.
 */

import { describe, expect, it } from 'vitest';

import type { HarnessPlanTask, HarnessPlanTaskStatus } from '../../storage/domains/harness/types';
import {
  assertNoBlockedByCycle,
  assertNoParentCycle,
  assertSingleInProgress,
  deriveStatus,
  HarnessPlanTaskCycleError,
  HarnessPlanTaskInProgressConflictError,
  hasUnsatisfiedDep,
  indexPlanTasks,
  rollupTree,
  rootOf,
} from './plan-task-hierarchy';

function task(partial: Partial<HarnessPlanTask> & { taskId: string }): HarnessPlanTask {
  return {
    harnessName: 'default',
    sessionId: 's1',
    resourceId: 'r1',
    threadId: 't1',
    order: 0,
    status: 'pending',
    statusSource: 'explicit',
    content: partial.taskId,
    createdAt: 0,
    updatedAt: 0,
    version: 1,
    ...partial,
  };
}

const statusOfFrom =
  (map: Record<string, HarnessPlanTaskStatus>) =>
  (id: string): HarnessPlanTaskStatus =>
    map[id] ?? 'pending';

// ---------------------------------------------------------------------------
// deriveStatus — rollup truth table
// ---------------------------------------------------------------------------

describe('deriveStatus — ratified precedence', () => {
  const parent = task({ taskId: 'p', statusSource: 'derived' });

  it('any child failed → failed (highest precedence)', () => {
    const statusOf = statusOfFrom({ a: 'failed', b: 'in_progress', c: 'completed' });
    expect(deriveStatus(parent, ['a', 'b', 'c'], statusOf)).toBe('failed');
  });

  it('no failed, any in_progress → in_progress', () => {
    const statusOf = statusOfFrom({ a: 'in_progress', b: 'blocked', c: 'completed' });
    expect(deriveStatus(parent, ['a', 'b', 'c'], statusOf)).toBe('in_progress');
  });

  it('no failed/in_progress, any child blocked → blocked', () => {
    const statusOf = statusOfFrom({ a: 'blocked', b: 'pending', c: 'completed' });
    expect(deriveStatus(parent, ['a', 'b', 'c'], statusOf)).toBe('blocked');
  });

  it('own unsatisfied blockedBy dep → blocked even when children are fine', () => {
    const dependent = task({ taskId: 'p', statusSource: 'derived', blockedBy: ['dep'] });
    const statusOf = statusOfFrom({ a: 'completed', dep: 'pending' });
    expect(deriveStatus(dependent, ['a'], statusOf)).toBe('blocked');
  });

  it('all children completed (some completed) → completed', () => {
    const statusOf = statusOfFrom({ a: 'completed', b: 'completed' });
    expect(deriveStatus(parent, ['a', 'b'], statusOf)).toBe('completed');
  });

  it('cancelled children count as skipped/ok: completed + cancelled → completed', () => {
    const statusOf = statusOfFrom({ a: 'completed', b: 'cancelled' });
    expect(deriveStatus(parent, ['a', 'b'], statusOf)).toBe('completed');
  });

  it('all children cancelled → cancelled', () => {
    const statusOf = statusOfFrom({ a: 'cancelled', b: 'cancelled' });
    expect(deriveStatus(parent, ['a', 'b'], statusOf)).toBe('cancelled');
  });

  it('otherwise (some pending, none of the above) → pending', () => {
    const statusOf = statusOfFrom({ a: 'pending', b: 'pending' });
    expect(deriveStatus(parent, ['a', 'b'], statusOf)).toBe('pending');
  });

  it('childless derived node: unsatisfied dep → blocked, else pending', () => {
    const childless = task({ taskId: 'p', statusSource: 'derived', blockedBy: ['dep'] });
    expect(deriveStatus(childless, [], statusOfFrom({ dep: 'in_progress' }))).toBe('blocked');
    expect(deriveStatus(childless, [], statusOfFrom({ dep: 'completed' }))).toBe('pending');
  });
});

describe('hasUnsatisfiedDep', () => {
  it('pending/in_progress/blocked dep blocks; completed/cancelled/failed dep releases', () => {
    const t = task({ taskId: 'x', blockedBy: ['d'] });
    expect(hasUnsatisfiedDep(t, statusOfFrom({ d: 'pending' }))).toBe(true);
    expect(hasUnsatisfiedDep(t, statusOfFrom({ d: 'in_progress' }))).toBe(true);
    expect(hasUnsatisfiedDep(t, statusOfFrom({ d: 'blocked' }))).toBe(true);
    expect(hasUnsatisfiedDep(t, statusOfFrom({ d: 'completed' }))).toBe(false);
    expect(hasUnsatisfiedDep(t, statusOfFrom({ d: 'cancelled' }))).toBe(false);
    expect(hasUnsatisfiedDep(t, statusOfFrom({ d: 'failed' }))).toBe(false);
  });
  it('no deps → never blocked', () => {
    expect(hasUnsatisfiedDep(task({ taskId: 'x' }), statusOfFrom({}))).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// rollupTree — bottom-up + explicit never overwritten
// ---------------------------------------------------------------------------

describe('rollupTree', () => {
  it('explicit terminal status is NEVER overwritten by derived rollup', () => {
    // p is explicit-completed; its child is pending. Rollup must NOT touch p.
    const tasks = [
      task({ taskId: 'p', status: 'completed', statusSource: 'explicit' }),
      task({ taskId: 'c', parentTaskId: 'p', status: 'pending', statusSource: 'explicit' }),
    ];
    const { changed } = rollupTree(tasks);
    expect(changed.has('p')).toBe(false);
  });

  it('derived parent reflects children bottom-up across two levels', () => {
    // root(derived) -> mid(derived) -> leaf(explicit completed)
    const tasks = [
      task({ taskId: 'root', status: 'pending', statusSource: 'derived' }),
      task({ taskId: 'mid', parentTaskId: 'root', status: 'pending', statusSource: 'derived' }),
      task({ taskId: 'leaf', parentTaskId: 'mid', status: 'completed', statusSource: 'explicit' }),
    ];
    const { changed } = rollupTree(tasks);
    expect(changed.get('mid')).toBe('completed');
    expect(changed.get('root')).toBe('completed');
  });

  it('staged explicit status drives rollup of the derived parent', () => {
    const tasks = [
      task({ taskId: 'p', status: 'pending', statusSource: 'derived' }),
      task({ taskId: 'c1', parentTaskId: 'p', status: 'pending', statusSource: 'explicit' }),
      task({ taskId: 'c2', parentTaskId: 'p', status: 'pending', statusSource: 'explicit' }),
    ];
    // Stage c1 -> in_progress (as the caller just set it).
    const staged = new Map<string, HarnessPlanTaskStatus>([['c1', 'in_progress']]);
    const stagedSrc = new Map<string, 'explicit' | 'derived'>([['c1', 'explicit']]);
    const { changed } = rollupTree(tasks, staged, stagedSrc);
    expect(changed.get('p')).toBe('in_progress');
  });

  it('blockedBy makes a derived parent blocked', () => {
    const tasks = [
      task({ taskId: 'p', status: 'pending', statusSource: 'derived', blockedBy: ['dep'] }),
      task({ taskId: 'c', parentTaskId: 'p', status: 'completed', statusSource: 'explicit' }),
      task({ taskId: 'dep', status: 'in_progress', statusSource: 'explicit' }),
    ];
    const { changed } = rollupTree(tasks);
    expect(changed.get('p')).toBe('blocked');
  });
});

// ---------------------------------------------------------------------------
// Cycle prevention
// ---------------------------------------------------------------------------

describe('assertNoParentCycle', () => {
  const tasks = [
    task({ taskId: 'a' }),
    task({ taskId: 'b', parentTaskId: 'a' }),
    task({ taskId: 'c', parentTaskId: 'b' }),
  ];
  const index = indexPlanTasks(tasks);

  it('rejects making a task its own parent', () => {
    expect(() => assertNoParentCycle(index, 'a', 'a')).toThrow(HarnessPlanTaskCycleError);
  });
  it('rejects making a task a child of its own descendant', () => {
    // move a under c (c is a descendant of a) → cycle
    expect(() => assertNoParentCycle(index, 'a', 'c')).toThrow(HarnessPlanTaskCycleError);
  });
  it('allows a valid reparent', () => {
    expect(() => assertNoParentCycle(index, 'c', 'a')).not.toThrow();
  });
});

describe('assertNoBlockedByCycle', () => {
  it('rejects a self dependency', () => {
    const index = indexPlanTasks([task({ taskId: 'a' })]);
    expect(() => assertNoBlockedByCycle(index, 'a', ['a'], () => undefined)).toThrow(HarnessPlanTaskCycleError);
  });
  it('rejects a transitive dependency cycle (a→b→c→a)', () => {
    const tasks = [
      task({ taskId: 'a' }),
      task({ taskId: 'b', blockedBy: ['a'] }),
      task({ taskId: 'c', blockedBy: ['b'] }),
    ];
    const index = indexPlanTasks(tasks);
    // Adding a blockedBy ['c'] closes the cycle a→c→b→a.
    expect(() => assertNoBlockedByCycle(index, 'a', ['c'], id => (id === 'a' ? ['c'] : undefined))).toThrow(
      HarnessPlanTaskCycleError,
    );
  });
  it('allows a valid dependency edge', () => {
    const tasks = [task({ taskId: 'a' }), task({ taskId: 'b' })];
    const index = indexPlanTasks(tasks);
    expect(() => assertNoBlockedByCycle(index, 'a', ['b'], () => undefined)).not.toThrow();
  });
});

// ---------------------------------------------------------------------------
// rootOf + single in_progress
// ---------------------------------------------------------------------------

describe('rootOf', () => {
  it('walks parent edges to the root', () => {
    const index = indexPlanTasks([
      task({ taskId: 'a' }),
      task({ taskId: 'b', parentTaskId: 'a' }),
      task({ taskId: 'c', parentTaskId: 'b' }),
    ]);
    expect(rootOf(index, 'c')).toBe('a');
    expect(rootOf(index, 'a')).toBe('a');
  });
});

describe('assertSingleInProgress', () => {
  const tasks = [
    task({ taskId: 'rootA' }),
    task({ taskId: 'a1', parentTaskId: 'rootA' }),
    task({ taskId: 'a2', parentTaskId: 'rootA' }),
    task({ taskId: 'rootB' }),
    task({ taskId: 'b1', parentTaskId: 'rootB' }),
  ];
  const index = indexPlanTasks(tasks);

  it('rejects a second in_progress in the SAME root', () => {
    const statusOf = statusOfFrom({ a1: 'in_progress' });
    expect(() => assertSingleInProgress(index, 'a2', statusOf)).toThrow(HarnessPlanTaskInProgressConflictError);
  });
  it('allows in_progress in a DIFFERENT root', () => {
    const statusOf = statusOfFrom({ a1: 'in_progress' });
    expect(() => assertSingleInProgress(index, 'b1', statusOf)).not.toThrow();
  });
  it('allows when nothing else is in_progress', () => {
    expect(() => assertSingleInProgress(index, 'a1', statusOfFrom({}))).not.toThrow();
  });
});
