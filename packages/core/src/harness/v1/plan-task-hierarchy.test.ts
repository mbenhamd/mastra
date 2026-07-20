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
  assertNoPlanTaskCombinedCycle,
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
  it('only completed releases; pending/in_progress/blocked/cancelled/failed block', () => {
    const t = task({ taskId: 'x', blockedBy: ['d'] });
    expect(hasUnsatisfiedDep(t, statusOfFrom({ d: 'pending' }))).toBe(true);
    expect(hasUnsatisfiedDep(t, statusOfFrom({ d: 'in_progress' }))).toBe(true);
    expect(hasUnsatisfiedDep(t, statusOfFrom({ d: 'blocked' }))).toBe(true);
    expect(hasUnsatisfiedDep(t, statusOfFrom({ d: 'completed' }))).toBe(false);
    expect(hasUnsatisfiedDep(t, statusOfFrom({ d: 'cancelled' }))).toBe(true);
    expect(hasUnsatisfiedDep(t, statusOfFrom({ d: 'failed' }))).toBe(true);
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

  // Finding 1: an explicit NON-terminal parent must STILL re-derive from
  // children. Only an explicit TERMINAL status is immune.
  it('explicit NON-terminal parent re-derives from children (a failed child rolls it to failed)', () => {
    const tasks = [
      // p was re-marked explicit 'pending' by the model, but it has children.
      task({ taskId: 'p', status: 'pending', statusSource: 'explicit' }),
      task({ taskId: 'c', parentTaskId: 'p', status: 'failed', statusSource: 'explicit' }),
    ];
    const { changed, source } = rollupTree(tasks);
    expect(changed.get('p')).toBe('failed');
    // It is now owned by child rollup → derived.
    expect(source.get('p')).toBe('derived');
  });

  it('explicit terminal parent is STILL immune even with a failed child', () => {
    const tasks = [
      task({ taskId: 'p', status: 'completed', statusSource: 'explicit' }),
      task({ taskId: 'c', parentTaskId: 'p', status: 'failed', statusSource: 'explicit' }),
    ];
    const { changed } = rollupTree(tasks);
    expect(changed.has('p')).toBe(false);
  });

  // Finding 2: a non-terminal LEAF (no children) with an unsatisfied blockedBy
  // dep must surface 'blocked' even though it started statusSource 'explicit'.
  it('a non-terminal explicit LEAF with an unsatisfied blockedBy dep rolls to blocked', () => {
    const tasks = [
      task({ taskId: 'leaf', status: 'pending', statusSource: 'explicit', blockedBy: ['dep'] }),
      task({ taskId: 'dep', status: 'in_progress', statusSource: 'explicit' }),
    ];
    const { changed, source } = rollupTree(tasks);
    expect(changed.get('leaf')).toBe('blocked');
    // A blockedBy overlay flips the node derived so it reverts to `pending` when
    // the dep clears (a childless derived node has no anchor to any other status).
    expect(source.get('leaf')).toBe('derived');
  });

  it('a derived childless leaf reverts to pending when its blockedBy dep clears', () => {
    const tasks = [
      // leaf already rolled to blocked/derived; dep now completed.
      task({ taskId: 'leaf', status: 'blocked', statusSource: 'derived', blockedBy: ['dep'] }),
      task({ taskId: 'dep', status: 'completed', statusSource: 'explicit' }),
    ];
    const { changed } = rollupTree(tasks);
    expect(changed.get('leaf')).toBe('pending');
  });

  it('does NOT downgrade an explicit in_progress parent whose children are all pending', () => {
    const tasks = [
      task({ taskId: 'p', status: 'in_progress', statusSource: 'explicit' }),
      task({ taskId: 'c1', parentTaskId: 'p', status: 'pending', statusSource: 'explicit' }),
      task({ taskId: 'c2', parentTaskId: 'p', status: 'pending', statusSource: 'explicit' }),
    ];
    // Derived would be 'pending' (lower precedence) — must NOT clobber explicit in_progress.
    const { changed } = rollupTree(tasks);
    expect(changed.has('p')).toBe(false);
  });

  it('a non-terminal explicit leaf is NOT clobbered to pending when it has no unsatisfied dep', () => {
    const tasks = [
      // Explicit in_progress leaf, no children, no deps → must keep in_progress.
      task({ taskId: 'leaf', status: 'in_progress', statusSource: 'explicit' }),
    ];
    const { changed } = rollupTree(tasks);
    expect(changed.has('leaf')).toBe(false);
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

describe('assertNoPlanTaskCombinedCycle', () => {
  it('rejects a child blocked by its own parent (implicit rollup cycle)', () => {
    const tasks = [
      task({ taskId: 'parent' }),
      task({ taskId: 'child', parentTaskId: 'parent', blockedBy: ['parent'] }),
    ];
    expect(() => assertNoPlanTaskCombinedCycle(tasks, 'blockedBy')).toThrow(HarnessPlanTaskCycleError);
  });

  it('rejects a transitive cycle spanning hierarchy and blockedBy edges', () => {
    const tasks = [
      task({ taskId: 'root' }),
      task({ taskId: 'middle', parentTaskId: 'root' }),
      task({ taskId: 'leaf', parentTaskId: 'middle', blockedBy: ['root'] }),
    ];
    expect(() => assertNoPlanTaskCombinedCycle(tasks, 'blockedBy')).toThrow(HarnessPlanTaskCycleError);
  });

  it('allows a dependency on a completed task outside the parent chain', () => {
    const tasks = [
      task({ taskId: 'dependency', status: 'completed' }),
      task({ taskId: 'parent' }),
      task({ taskId: 'child', parentTaskId: 'parent', blockedBy: ['dependency'] }),
    ];
    expect(() => assertNoPlanTaskCombinedCycle(tasks, 'blockedBy')).not.toThrow();
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

  // Finding 5: a DERIVED rollup in_progress ancestor is NOT a competing focus —
  // it just mirrors its explicit in_progress child. Re-confirming that child
  // must not see its own rolled-up parent as a rival.
  it('ignores a DERIVED in_progress ancestor (so re-confirming the in_progress child is idempotent)', () => {
    const tree = [
      task({ taskId: 'root', status: 'in_progress', statusSource: 'derived' }), // rolled up from c1
      task({ taskId: 'c1', parentTaskId: 'root', status: 'in_progress', statusSource: 'explicit' }),
    ];
    const idx = indexPlanTasks(tree);
    const statusOf = (id: string) => idx.byId.get(id)?.status ?? 'pending';
    const sourceOf = (id: string) => idx.byId.get(id)?.statusSource ?? ('explicit' as const);
    // Re-setting c1 in_progress must NOT throw on the derived 'root' ancestor.
    expect(() => assertSingleInProgress(idx, 'c1', statusOf, sourceOf)).not.toThrow();
  });

  it('still rejects two EXPLICIT in_progress in the same root', () => {
    const tree = [
      task({ taskId: 'root', status: 'pending', statusSource: 'derived' }),
      task({ taskId: 'c1', parentTaskId: 'root', status: 'in_progress', statusSource: 'explicit' }),
      task({ taskId: 'c2', parentTaskId: 'root', status: 'pending', statusSource: 'explicit' }),
    ];
    const idx = indexPlanTasks(tree);
    const statusOf = (id: string) => (id === 'c2' ? 'in_progress' : (idx.byId.get(id)?.status ?? 'pending'));
    const sourceOf = (id: string) => idx.byId.get(id)?.statusSource ?? ('explicit' as const);
    expect(() => assertSingleInProgress(idx, 'c2', statusOf, sourceOf)).toThrow(HarnessPlanTaskInProgressConflictError);
  });

  it('ignores delegated background work when admitting one foreground focus', () => {
    const tree = [
      task({ taskId: 'root', status: 'in_progress', statusSource: 'derived' }),
      task({
        taskId: 'delegated',
        parentTaskId: 'root',
        status: 'in_progress',
        statusSource: 'explicit',
        delegatedSubagentSessionId: 'child-session',
      }),
      task({ taskId: 'foreground', parentTaskId: 'root' }),
    ];
    const idx = indexPlanTasks(tree);
    expect(() =>
      assertSingleInProgress(
        idx,
        'foreground',
        id => (id === 'foreground' ? 'in_progress' : (idx.byId.get(id)?.status ?? 'pending')),
        id => idx.byId.get(id)?.statusSource ?? 'explicit',
        id => idx.byId.get(id)?.delegatedSubagentSessionId !== undefined,
      ),
    ).not.toThrow();
  });
});

// ---------------------------------------------------------------------------
// PF-787 triple-check — deep / diamond / cascade edge cases
// (pure-hierarchy units the prior suite left to higher-level session tests)
// ---------------------------------------------------------------------------

describe('rollupTree — deep chains (stack-safety + multi-level cascade)', () => {
  // A single linear chain root → … → leaf. Each parent is explicit-pending
  // (non-terminal → recomputed); the leaf carries the explicit status.
  function deepChain(depth: number, leafStatus: HarnessPlanTaskStatus): HarnessPlanTask[] {
    const tasks: HarnessPlanTask[] = [];
    for (let i = 0; i < depth; i++) {
      const isLeaf = i === depth - 1;
      tasks.push(
        task({
          taskId: `n${i}`,
          parentTaskId: i === 0 ? undefined : `n${i - 1}`,
          status: isLeaf ? leafStatus : 'pending',
          statusSource: 'explicit',
        }),
      );
    }
    return tasks;
  }

  it('does NOT overflow the stack and rolls a failed leaf up to the root across a 6000-deep chain', () => {
    const tasks = deepChain(6000, 'failed');
    // The fix replaced recursive depth computation with an iterative walk — a chain
    // this deep would blow the JS call stack under the old recursion.
    const { changed } = rollupTree(tasks);
    expect(changed.get('n0')).toBe('failed'); // root inherits the deep failure
    expect(changed.get('n2999')).toBe('failed'); // a mid-chain node too
  });

  it('cascades an in_progress leaf up through >5 intermediate levels', () => {
    const tasks = deepChain(8, 'in_progress');
    const { changed } = rollupTree(tasks);
    for (let i = 0; i < 7; i++) expect(changed.get(`n${i}`)).toBe('in_progress');
    expect(changed.get('n7')).toBeUndefined(); // the explicit leaf itself is unchanged
  });
});

describe('blockedBy — diamond graphs', () => {
  // Diamond: a → {b, c} → d (d depends on b and c, both depend on a).
  const diamond = () =>
    indexPlanTasks([
      task({ taskId: 'a' }),
      task({ taskId: 'b', blockedBy: ['a'] }),
      task({ taskId: 'c', blockedBy: ['a'] }),
      task({ taskId: 'd', blockedBy: ['b', 'c'] }),
    ]);

  it('allows an acyclic diamond (d depends on both b and c)', () => {
    expect(() => assertNoBlockedByCycle(diamond(), 'd', ['b', 'c'], () => undefined)).not.toThrow();
  });

  it('rejects closing the diamond into a cycle (a depends on d → a→d→b→a)', () => {
    expect(() => assertNoBlockedByCycle(diamond(), 'a', ['d'], id => (id === 'a' ? ['d'] : undefined))).toThrow(
      HarnessPlanTaskCycleError,
    );
  });
});

describe('blockedBy — only a successful dependency releases the block', () => {
  const dependent = task({ taskId: 'x', blockedBy: ['dep'] });

  it('a failed dependency keeps downstream work blocked', () => {
    expect(deriveStatus(dependent, [], statusOfFrom({ dep: 'failed' }))).toBe('blocked');
    expect(hasUnsatisfiedDep(dependent, statusOfFrom({ dep: 'failed' }))).toBe(true);
  });

  it('a cancelled dependency keeps downstream work blocked too', () => {
    expect(deriveStatus(dependent, [], statusOfFrom({ dep: 'cancelled' }))).toBe('blocked');
  });

  it('a still-pending dependency keeps it blocked', () => {
    expect(deriveStatus(dependent, [], statusOfFrom({ dep: 'pending' }))).toBe('blocked');
  });
});
