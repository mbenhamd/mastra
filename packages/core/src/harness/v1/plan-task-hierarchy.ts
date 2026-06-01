/**
 * TM-4 plan-task hierarchy semantics (HARNESS_V1_SPEC.md §5.1k).
 *
 * Pure, storage-agnostic logic the live `Session` runs over a plan tree it has
 * loaded from `HarnessStorage`:
 *
 *   - status ROLLUP (derived parent status from children + `blockedBy` deps),
 *   - cycle PREVENTION across BOTH `parentTaskId` and `blockedBy` edges,
 *   - per-root SINGLE `in_progress` enforcement.
 *
 * The session computes the next tree shape here, then commits the changed rows
 * through `mutatePlanTasksForSession` under the session-owner fence (§5.8). This
 * module never touches storage; it operates on `HarnessPlanTask[]` snapshots so
 * it is trivially testable and identical regardless of the backing adapter.
 */

import type { HarnessPlanTask, HarnessPlanTaskStatus } from '../../storage/domains/harness/types';
import { HarnessValidationError } from './errors';

/** Terminal statuses an explicit caller/model action may set and that rollup
 * must never silently overwrite. A `derived` parent, by contrast, is always
 * recomputed. */
export const TERMINAL_PLAN_TASK_STATUSES: ReadonlySet<HarnessPlanTaskStatus> = new Set<HarnessPlanTaskStatus>([
  'completed',
  'cancelled',
  'failed',
]);

/**
 * Thrown when a requested edge would create a cycle across `parentTaskId`
 * (reparent) or `blockedBy` (dependency). Typed so the tool layer can surface a
 * clean validation result instead of a generic error.
 */
export class HarnessPlanTaskCycleError extends HarnessValidationError {
  readonly edgeKind: 'parent' | 'blockedBy';
  readonly taskId: string;
  readonly targetId: string;
  constructor(edgeKind: 'parent' | 'blockedBy', taskId: string, targetId: string) {
    super(
      edgeKind === 'parent' ? 'parentTaskId' : 'blockedBy',
      `would create a ${edgeKind} cycle between task "${taskId}" and "${targetId}"`,
    );
    // `name` on the base is the literal 'HarnessValidationError'; assign through
    // a cast so the subclass can carry its own name without widening the type.
    (this as { name: string }).name = 'HarnessPlanTaskCycleError';
    this.edgeKind = edgeKind;
    this.taskId = taskId;
    this.targetId = targetId;
  }
}

/**
 * Thrown when setting a task `in_progress` while another task in the SAME root
 * subtree is already `in_progress`. The single-in_progress-per-root invariant is
 * enforced by auto-transitioning is NOT chosen here: the spec-faithful behavior
 * is REJECT (the model must complete/pause the current focus first), so the
 * model's plan stays an explicit, auditable record of what it chose to work on.
 */
export class HarnessPlanTaskInProgressConflictError extends HarnessValidationError {
  readonly taskId: string;
  readonly conflictingTaskId: string;
  readonly rootTaskId: string;
  constructor(taskId: string, conflictingTaskId: string, rootTaskId: string) {
    super(
      'status',
      `cannot set task "${taskId}" in_progress: task "${conflictingTaskId}" in the same root "${rootTaskId}" is already in_progress`,
    );
    (this as { name: string }).name = 'HarnessPlanTaskInProgressConflictError';
    this.taskId = taskId;
    this.conflictingTaskId = conflictingTaskId;
    this.rootTaskId = rootTaskId;
  }
}

// ---------------------------------------------------------------------------
// Tree index
// ---------------------------------------------------------------------------

export interface PlanTaskIndex {
  byId: Map<string, HarnessPlanTask>;
  childrenByParent: Map<string, string[]>;
  roots: string[];
}

/** Build adjacency indexes from a flat task list. */
export function indexPlanTasks(tasks: HarnessPlanTask[]): PlanTaskIndex {
  const byId = new Map<string, HarnessPlanTask>();
  const childrenByParent = new Map<string, string[]>();
  const roots: string[] = [];
  for (const task of tasks) byId.set(task.taskId, task);
  for (const task of tasks) {
    if (task.parentTaskId !== undefined && byId.has(task.parentTaskId)) {
      const siblings = childrenByParent.get(task.parentTaskId) ?? [];
      siblings.push(task.taskId);
      childrenByParent.set(task.parentTaskId, siblings);
    } else {
      // A task whose parent is missing is treated as a root for rollup
      // purposes (defensive — should not happen under the fence).
      roots.push(task.taskId);
    }
  }
  return { byId, childrenByParent, roots };
}

/** Walk parent edges to the root of `taskId`'s subtree. Cycle-safe. */
export function rootOf(index: PlanTaskIndex, taskId: string): string {
  const visited = new Set<string>();
  let current = taskId;
  while (true) {
    if (visited.has(current)) return current; // defensive cycle guard
    visited.add(current);
    const task = index.byId.get(current);
    if (!task || task.parentTaskId === undefined || !index.byId.has(task.parentTaskId)) {
      return current;
    }
    current = task.parentTaskId;
  }
}

// ---------------------------------------------------------------------------
// Cycle prevention
// ---------------------------------------------------------------------------

/**
 * Reject a reparent that would make `taskId` its own ancestor. `newParentId`
 * must not be `taskId` itself, nor any descendant of `taskId`. Walks UP from
 * `newParentId` looking for `taskId`.
 */
export function assertNoParentCycle(index: PlanTaskIndex, taskId: string, newParentId: string): void {
  if (newParentId === taskId) {
    throw new HarnessPlanTaskCycleError('parent', taskId, newParentId);
  }
  const visited = new Set<string>();
  let current: string | undefined = newParentId;
  while (current !== undefined) {
    if (current === taskId) {
      throw new HarnessPlanTaskCycleError('parent', taskId, newParentId);
    }
    if (visited.has(current)) break; // pre-existing cycle elsewhere — not ours
    visited.add(current);
    current = index.byId.get(current)?.parentTaskId;
  }
}

/**
 * Reject a `blockedBy` edge set that would create a dependency cycle. The
 * dependency graph is "A blockedBy B" meaning B must finish before A; a cycle
 * means a set of tasks can never start. `proposedBlockedBy` is the FULL new
 * dependency list for `taskId`; we check whether any of them can (transitively)
 * reach back to `taskId`.
 */
export function assertNoBlockedByCycle(
  index: PlanTaskIndex,
  taskId: string,
  proposedBlockedBy: readonly string[],
  blockedByOverride: (id: string) => readonly string[] | undefined,
): void {
  for (const dep of proposedBlockedBy) {
    if (dep === taskId) {
      throw new HarnessPlanTaskCycleError('blockedBy', taskId, dep);
    }
    // DFS from `dep` following blockedBy edges; if we reach `taskId`, the new
    // edge `taskId -> dep` closes a cycle.
    const stack = [dep];
    const visited = new Set<string>();
    while (stack.length > 0) {
      const node = stack.pop()!;
      if (node === taskId) {
        throw new HarnessPlanTaskCycleError('blockedBy', taskId, dep);
      }
      if (visited.has(node)) continue;
      visited.add(node);
      const deps = node === taskId ? proposedBlockedBy : (blockedByOverride(node) ?? index.byId.get(node)?.blockedBy);
      if (deps) for (const d of deps) stack.push(d);
    }
  }
}

// ---------------------------------------------------------------------------
// Single in_progress per root
// ---------------------------------------------------------------------------

/**
 * Assert no OTHER task in the same root subtree as `taskId` is currently
 * `in_progress`. Used before setting `taskId` itself `in_progress`. `statusOf`
 * lets callers reflect a staged (not-yet-committed) status map.
 */
export function assertSingleInProgress(
  index: PlanTaskIndex,
  taskId: string,
  statusOf: (id: string) => HarnessPlanTaskStatus,
): void {
  const root = rootOf(index, taskId);
  for (const task of index.byId.values()) {
    if (task.taskId === taskId) continue;
    if (statusOf(task.taskId) !== 'in_progress') continue;
    if (rootOf(index, task.taskId) === root) {
      throw new HarnessPlanTaskInProgressConflictError(taskId, task.taskId, root);
    }
  }
}

// ---------------------------------------------------------------------------
// Rollup (RATIFIED truth-table)
// ---------------------------------------------------------------------------

/**
 * Derive ONE node's status from its children + its own `blockedBy` deps, given
 * a way to read the (possibly staged) status of any task. Precedence (highest
 * first), per the ratified truth table:
 *
 *   1. any child `failed`                              → failed
 *   2. else any child `in_progress`                    → in_progress
 *   3. else `blocked` (own unsatisfied `blockedBy` dep,
 *      or any child `blocked`)                         → blocked
 *   4. else all children terminal-ok (`completed`,
 *      `cancelled` counts as skipped/ok) AND at least
 *      one `completed`                                 → completed
 *   5. else all children `cancelled`                   → cancelled
 *   6. else                                            → pending
 *
 * A node with NO children derives purely from its `blockedBy` deps: an
 * unsatisfied dep → blocked, otherwise its status is left to the caller (a
 * childless node has no children to roll up from, so derivation only forces
 * `blocked`; see `rollupTree`).
 */
export function deriveStatus(
  task: HarnessPlanTask,
  childIds: readonly string[],
  statusOf: (id: string) => HarnessPlanTaskStatus,
): HarnessPlanTaskStatus {
  const depBlocked = hasUnsatisfiedDep(task, statusOf);

  if (childIds.length === 0) {
    // Childless derived node: only a dep can force it off `pending`.
    return depBlocked ? 'blocked' : 'pending';
  }

  let anyFailed = false;
  let anyInProgress = false;
  let anyBlocked = false;
  let anyCompleted = false;
  let allTerminalOk = true; // every child completed or cancelled
  let allCancelled = true;

  for (const childId of childIds) {
    const s = statusOf(childId);
    if (s === 'failed') anyFailed = true;
    if (s === 'in_progress') anyInProgress = true;
    if (s === 'blocked') anyBlocked = true;
    if (s === 'completed') anyCompleted = true;
    if (s !== 'completed' && s !== 'cancelled') allTerminalOk = false;
    if (s !== 'cancelled') allCancelled = false;
  }

  if (anyFailed) return 'failed';
  if (anyInProgress) return 'in_progress';
  if (depBlocked || anyBlocked) return 'blocked';
  if (allTerminalOk && anyCompleted) return 'completed';
  if (allCancelled) return 'cancelled';
  return 'pending';
}

/** True when any `blockedBy` dependency is not yet satisfied (i.e. not in a
 * terminal-ok state). A `failed`/`cancelled` dep does NOT keep a task blocked —
 * the work it waited on is over; the model decides what to do next. Only a dep
 * still `pending` / `in_progress` / `blocked` keeps a task blocked. */
export function hasUnsatisfiedDep(
  task: HarnessPlanTask,
  statusOf: (id: string) => HarnessPlanTaskStatus,
): boolean {
  if (!task.blockedBy || task.blockedBy.length === 0) return false;
  for (const depId of task.blockedBy) {
    const s = statusOf(depId);
    // 'completed' satisfies; 'cancelled'/'failed' release the block (the awaited
    // work is terminal). Anything else (pending/in_progress/blocked) blocks.
    if (s !== 'completed' && s !== 'cancelled' && s !== 'failed') return true;
  }
  return false;
}

export interface RollupResult {
  /** taskId -> new derived status, only for rows whose status actually changed. */
  changed: Map<string, HarnessPlanTaskStatus>;
}

/**
 * Recompute every `derived` node bottom-up over the whole tree. Explicit
 * terminal statuses are NEVER overwritten (statusSource:'explicit' wins). A
 * node is recomputed iff its `statusSource === 'derived'`. Returns only the
 * rows whose status changed, so the caller commits a minimal write set.
 *
 * `stagedStatus` carries statuses already decided in the current mutation (e.g.
 * the explicit status the caller just set) so rollup sees the post-mutation
 * world; `stagedStatusSource` likewise. Both fall back to the stored row.
 */
export function rollupTree(
  tasks: HarnessPlanTask[],
  stagedStatus?: Map<string, HarnessPlanTaskStatus>,
  stagedStatusSource?: Map<string, 'explicit' | 'derived'>,
): RollupResult {
  const index = indexPlanTasks(tasks);
  // Working status map: start from staged-or-stored.
  const working = new Map<string, HarnessPlanTaskStatus>();
  for (const task of tasks) {
    working.set(task.taskId, stagedStatus?.get(task.taskId) ?? task.status);
  }
  const sourceOf = (id: string): 'explicit' | 'derived' =>
    stagedStatusSource?.get(id) ?? index.byId.get(id)?.statusSource ?? 'explicit';
  const statusOf = (id: string): HarnessPlanTaskStatus => working.get(id) ?? 'pending';

  // Process children before parents: order nodes by descending depth.
  const depth = new Map<string, number>();
  const computeDepth = (id: string): number => {
    if (depth.has(id)) return depth.get(id)!;
    const parent = index.byId.get(id)?.parentTaskId;
    const d = parent !== undefined && index.byId.has(parent) ? computeDepth(parent) + 1 : 0;
    depth.set(id, d);
    return d;
  };
  const ordered = [...index.byId.keys()].sort((a, b) => computeDepth(b) - computeDepth(a));

  for (const id of ordered) {
    if (sourceOf(id) !== 'derived') continue; // explicit status wins, never overwritten
    const task = index.byId.get(id)!;
    const childIds = index.childrenByParent.get(id) ?? [];
    const next = deriveStatus(task, childIds, statusOf);
    working.set(id, next);
  }

  const changed = new Map<string, HarnessPlanTaskStatus>();
  for (const task of tasks) {
    const next = working.get(task.taskId)!;
    const prev = stagedStatus?.get(task.taskId) ?? task.status;
    if (next !== prev) changed.set(task.taskId, next);
  }
  return { changed };
}
