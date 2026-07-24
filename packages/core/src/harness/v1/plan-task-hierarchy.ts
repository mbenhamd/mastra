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
 * Thrown when setting a foreground task `in_progress` while another foreground
 * task in the SAME root subtree is already `in_progress`. The
 * single-foreground-in-progress-per-root invariant is
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

/**
 * Reject a cycle in the COMPLETE execution-dependency graph.
 *
 * `blockedBy` is not the only dependency carried by a plan tree: a derived
 * parent also waits for every direct child before it can complete. Therefore
 * the graph contains both `task -> blockedBy target` and the implicit rollup
 * edges `parent -> child`. Validating those edge families independently misses
 * deadlocks such as a child blocked by its own parent, or reparenting a task
 * beneath a node that transitively waits for it.
 *
 * Callers pass the mutation edge family so the validation error points at the
 * field the model can repair. Unknown dependency ids are validated by the
 * operation layer and are ignored defensively here.
 */
export function assertNoPlanTaskCombinedCycle(
  tasks: readonly HarnessPlanTask[],
  mutationEdgeKind: 'parent' | 'blockedBy',
): void {
  const index = indexPlanTasks([...tasks]);
  const state = new Map<string, 'visiting' | 'visited'>();

  const visit = (taskId: string): void => {
    state.set(taskId, 'visiting');
    const task = index.byId.get(taskId);
    const targets = [...(task?.blockedBy ?? []), ...(index.childrenByParent.get(taskId) ?? [])];
    for (const targetId of targets) {
      if (!index.byId.has(targetId)) continue;
      const targetState = state.get(targetId);
      if (targetState === 'visiting') {
        throw new HarnessPlanTaskCycleError(mutationEdgeKind, taskId, targetId);
      }
      if (targetState !== 'visited') visit(targetId);
    }
    state.set(taskId, 'visited');
  };

  for (const taskId of index.byId.keys()) {
    if (state.get(taskId) === undefined) visit(taskId);
  }
}

// ---------------------------------------------------------------------------
// Single in_progress per root
// ---------------------------------------------------------------------------

/**
 * Assert no OTHER FOREGROUND task in the same root subtree as `taskId` is
 * currently EXPLICITLY `in_progress`. Used before setting `taskId` itself
 * `in_progress`.
 * `statusOf` lets callers reflect a staged (not-yet-committed) status map.
 *
 * The invariant is "one EXPLICIT foreground work item per root" (§5.1k): a
 * DERIVED `in_progress` ancestor merely reflects its child and a delegated task
 * is bounded background work owned by the subagent concurrency pool. Neither is
 * a competing parent-agent focus. `sourceOf` reports the (possibly staged)
 * statusSource; `isBackground` identifies delegated work. Both are optional so
 * pure callers retain the stricter historical check unless they have those
 * fields available.
 */
export function assertSingleInProgress(
  index: PlanTaskIndex,
  taskId: string,
  statusOf: (id: string) => HarnessPlanTaskStatus,
  sourceOf?: (id: string) => 'explicit' | 'derived',
  isBackground?: (id: string) => boolean,
): void {
  const root = rootOf(index, taskId);
  for (const task of index.byId.values()) {
    if (task.taskId === taskId) continue;
    if (statusOf(task.taskId) !== 'in_progress') continue;
    // Only an EXPLICIT in_progress is a competing focus; a derived rollup
    // in_progress just mirrors its in_progress child.
    const source = sourceOf?.(task.taskId) ?? task.statusSource ?? 'explicit';
    if (source !== 'explicit') continue;
    if (isBackground?.(task.taskId) === true) continue;
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
 * unsatisfied dep → `blocked`, otherwise `pending` (the floor for a node with
 * nothing to roll up from). `rollupTree` decides WHETHER to apply this derived
 * value over an explicit status — it overrides an explicit non-terminal node
 * only when the derived value has higher precedence or is a `blockedBy` overlay,
 * so an explicit `in_progress` leaf with no deps is left untouched.
 */
export function deriveStatus(
  task: HarnessPlanTask,
  childIds: readonly string[],
  statusOf: (id: string) => HarnessPlanTaskStatus,
): HarnessPlanTaskStatus {
  const depBlocked = hasUnsatisfiedDep(task, statusOf);

  if (childIds.length === 0) {
    // Childless node: only a dep can force it off `pending`. A childless derived
    // node reverts to `pending` when its dep clears (it has no child to anchor
    // any other status).
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

/**
 * Status precedence used to decide whether a derived rollup value supersedes an
 * EXPLICIT non-terminal status (higher wins). Mirrors the `deriveStatus`
 * truth-table ordering: a child `failed` (top) overrides everything; a derived
 * `pending` (bottom) never downgrades an explicit `in_progress`/`blocked`.
 * Terminal explicit statuses are handled separately (immune), so their relative
 * order here only matters for derived-vs-derived comparisons.
 */
function precedence(status: HarnessPlanTaskStatus): number {
  switch (status) {
    case 'failed':
      return 5;
    case 'in_progress':
      return 4;
    case 'blocked':
      return 3;
    case 'completed':
      return 2;
    case 'cancelled':
      return 1;
    case 'pending':
    default:
      return 0;
  }
}

/**
 * True when any `blockedBy` dependency has not SUCCEEDED. `blockedBy` is a
 * success dependency by default: only `completed` releases it. A failed or
 * cancelled prerequisite must not silently make downstream work runnable (for
 * example, a source-repair task must stay blocked when the compiler SERVICE,
 * rather than the source, failed). Work that intentionally runs after any
 * terminal outcome belongs in an explicit workflow branch, not an ambiguous
 * dependency edge.
 */
export function hasUnsatisfiedDep(task: HarnessPlanTask, statusOf: (id: string) => HarnessPlanTaskStatus): boolean {
  if (!task.blockedBy || task.blockedBy.length === 0) return false;
  for (const depId of task.blockedBy) {
    const s = statusOf(depId);
    if (s !== 'completed') return true;
  }
  return false;
}

export interface RollupResult {
  /** taskId -> new derived status, only for rows whose status actually changed. */
  changed: Map<string, HarnessPlanTaskStatus>;
  /**
   * taskId -> the statusSource the caller should persist for a CHANGED row.
   * `'derived'` when the new status came from CHILD rollup (the node's status is
   * now owned by its children). `'explicit'` when the change is a pure
   * `blockedBy` overlay on a childless node — the node keeps its explicit
   * identity so it reverts to its own status when the dep releases. Only entries
   * for ids present in `changed` are meaningful.
   */
  source: Map<string, 'explicit' | 'derived'>;
}

/**
 * Recompute every node bottom-up over the whole tree EXCEPT those carrying an
 * explicit TERMINAL status (completed/cancelled/failed), which are NEVER
 * overwritten (the ratified "explicit terminal wins" rule). An explicit
 * NON-terminal node (a parent re-marked `pending`, or a leaf with a `blockedBy`
 * dep) IS re-derived: a parent re-reflects its children, and a leaf surfaces
 * `blocked` while a dep is unsatisfied.
 *
 * statusSource of a CHANGED row: `'derived'` when the new status came from child
 * rollup (the node has children whose roll-up now owns it); `'explicit'` when it
 * is only a `blockedBy` overlay on a childless node (the node keeps its own
 * identity and reverts when the dep releases).
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

  // Process children before parents: order nodes by descending depth. Depth is
  // computed by walking the parent chain ITERATIVELY (not recursively) so an
  // arbitrarily deep plan tree can never overflow the JS call stack, and with a
  // `seen` guard so a (defensively-handled) parent cycle terminates instead of
  // looping — recursion here had neither property.
  const depth = new Map<string, number>();
  const computeDepth = (id: string): number => {
    const cached = depth.get(id);
    if (cached !== undefined) return cached;
    // Collect the uncached ancestor chain [id, parent, …] until we hit a cached
    // node, a root (no in-index parent), or a cycle; then assign depths downward.
    const chain: string[] = [];
    const seen = new Set<string>();
    let cur: string | undefined = id;
    let baseDepth = 0; // depth to assign to the topmost chain entry
    while (cur !== undefined) {
      const cachedCur = depth.get(cur);
      if (cachedCur !== undefined) {
        baseDepth = cachedCur + 1; // chain entries hang BELOW the cached anchor
        break;
      }
      if (seen.has(cur)) {
        baseDepth = 0; // cycle: treat the chain top as a root (defensive)
        break;
      }
      seen.add(cur);
      const parent: string | undefined = index.byId.get(cur)?.parentTaskId;
      const nextParent: string | undefined = parent !== undefined && index.byId.has(parent) ? parent : undefined;
      chain.push(cur);
      if (nextParent === undefined) {
        baseDepth = 0; // `cur` is a true root → depth 0
        break;
      }
      cur = nextParent;
    }
    // chain is [id, …, top]; `top` gets baseDepth, each child one deeper.
    let d = baseDepth;
    for (let i = chain.length - 1; i >= 0; i--) {
      depth.set(chain[i]!, d);
      d += 1;
    }
    return depth.get(id)!;
  };
  const ordered = [...index.byId.keys()].sort((a, b) => computeDepth(b) - computeDepth(a));

  // Source the rollup decided for each recomputed node (only meaningful for ids
  // it actually recomputed; defaults to the node's current source otherwise).
  const derivedSource = new Map<string, 'explicit' | 'derived'>();
  for (const id of ordered) {
    const current = statusOf(id);
    const source = sourceOf(id);
    // Explicit TERMINAL status is immune — never recomputed.
    if (source === 'explicit' && TERMINAL_PLAN_TASK_STATUSES.has(current)) continue;

    const task = index.byId.get(id)!;
    const childIds = index.childrenByParent.get(id) ?? [];
    const next = deriveStatus(task, childIds, statusOf);

    if (source === 'derived') {
      // Fully rollup-owned: always take the derived value.
      working.set(id, next);
      derivedSource.set(id, 'derived');
      continue;
    }

    // Explicit NON-terminal node: the model's chosen status holds UNLESS the
    // derived value supersedes it. It supersedes when it has strictly higher
    // precedence (e.g. a child `failed` overrides explicit `in_progress`), or
    // when it is a `blockedBy` overlay (an unsatisfied own-dep always surfaces
    // as `blocked` on a non-terminal node). An equal-or-lower derived value
    // (e.g. children all `pending` while the model marked the parent
    // `in_progress`) does NOT downgrade the explicit status.
    const depOverlay = next === 'blocked' && hasUnsatisfiedDep(task, statusOf);
    if (depOverlay || precedence(next) > precedence(current)) {
      working.set(id, next);
      // A child rollup makes the node rollup-owned (→ derived); a pure dep
      // overlay also flips it derived so it reverts to `pending` when the dep
      // clears (a childless derived node has no anchor to any other status).
      derivedSource.set(id, 'derived');
    }
    // else: keep the explicit status (no working.set; derivedSource untouched).
  }

  const changed = new Map<string, HarnessPlanTaskStatus>();
  const source = new Map<string, 'explicit' | 'derived'>();
  for (const task of tasks) {
    const next = working.get(task.taskId)!;
    const prev = stagedStatus?.get(task.taskId) ?? task.status;
    if (next !== prev) {
      changed.set(task.taskId, next);
      source.set(task.taskId, derivedSource.get(task.taskId) ?? sourceOf(task.taskId));
    }
  }
  return { changed, source };
}
