import type { HarnessPlanTask, LoadPlanTaskSubtreeInput, UpdatePlanTaskInput } from './types';

/**
 * Shared, adapter-agnostic plan-task logic (§5.1k) so the in-memory, PG, and
 * LibSQL adapters apply identical patch and subtree-walk semantics. Storage of
 * the rows differs per adapter; this normalization does not. Keeping one copy
 * here is what makes the cross-adapter `loadPlanTaskSubtree` depth/status/limit
 * behavior match exactly.
 */

function cloneJsonValue<T>(value: T): T {
  return value === undefined ? value : (structuredClone(value) as T);
}

/**
 * Apply an `UpdatePlanTaskInput['patch']` to a plan task, producing the next
 * row with `version` set to `nextVersion` and `updatedAt` refreshed. `clear*`
 * flags delete the optional field; a provided value sets it.
 */
export function applyPlanTaskPatch(
  existing: HarnessPlanTask,
  patch: UpdatePlanTaskInput['patch'],
  nextVersion: number,
): HarnessPlanTask {
  const next: HarnessPlanTask = structuredClone(existing);
  if (patch.parentTaskId !== undefined) next.parentTaskId = patch.parentTaskId;
  if (patch.clearParentTaskId === true) delete next.parentTaskId;
  if (patch.order !== undefined) next.order = patch.order;
  if (patch.status !== undefined) next.status = patch.status;
  if (patch.statusSource !== undefined) next.statusSource = patch.statusSource;
  if (patch.content !== undefined) next.content = patch.content;
  if (patch.activeForm !== undefined) next.activeForm = patch.activeForm;
  if (patch.clearActiveForm === true) delete next.activeForm;
  if (patch.priority !== undefined) next.priority = patch.priority;
  if (patch.clearPriority === true) delete next.priority;
  if (patch.blockedBy !== undefined) next.blockedBy = [...patch.blockedBy];
  if (patch.clearBlockedBy === true) delete next.blockedBy;
  if (patch.origin !== undefined) next.origin = patch.origin;
  if (patch.delegatedSubagentSessionId !== undefined) {
    next.delegatedSubagentSessionId = patch.delegatedSubagentSessionId;
  }
  if (patch.metadata !== undefined) next.metadata = cloneJsonValue(patch.metadata);
  if (patch.clearMetadata === true) delete next.metadata;
  if (patch.completedAt !== undefined) next.completedAt = patch.completedAt;
  if (patch.clearCompletedAt === true) delete next.completedAt;
  next.version = nextVersion;
  next.updatedAt = Date.now();
  return next;
}

/** Order by (parentTaskId, order, taskId) — roots (no parent) sort first, stable. */
export function comparePlanTaskOrder(a: HarnessPlanTask, b: HarnessPlanTask): number {
  const ap = a.parentTaskId ?? '';
  const bp = b.parentTaskId ?? '';
  if (ap !== bp) return ap < bp ? -1 : 1;
  if (a.order !== b.order) return a.order - b.order;
  return a.taskId.localeCompare(b.taskId);
}

/**
 * Depth-first preorder bounded subtree walk (§5.1k `loadPlanTaskSubtree`). Input
 * `tasks` is the session's full task set (any order); the walk sorts siblings by
 * `comparePlanTaskOrder`, starts from `rootTaskId` (or session roots when
 * omitted), honors `depth` and `status`, caps at `limit`, and reports
 * `truncated` when the bound clipped the subtree. A visited set defensively
 * guards a `parentTaskId` cycle (cycle PREVENTION is TM-4).
 */
export function walkPlanTaskSubtree(
  tasks: HarnessPlanTask[],
  opts: Pick<LoadPlanTaskSubtreeInput, 'rootTaskId' | 'depth' | 'status' | 'limit'>,
): { tasks: HarnessPlanTask[]; truncated: boolean } {
  const { rootTaskId, depth, status, limit } = opts;
  const byId = new Map<string, HarnessPlanTask>();
  const childrenByParent = new Map<string, HarnessPlanTask[]>();
  const roots: HarnessPlanTask[] = [];
  for (const task of tasks) {
    byId.set(task.taskId, task);
    if (task.parentTaskId !== undefined) {
      const siblings = childrenByParent.get(task.parentTaskId) ?? [];
      siblings.push(task);
      childrenByParent.set(task.parentTaskId, siblings);
    } else {
      roots.push(task);
    }
  }
  for (const siblings of childrenByParent.values()) siblings.sort(comparePlanTaskOrder);
  roots.sort(comparePlanTaskOrder);

  const result: HarnessPlanTask[] = [];
  let truncated = false;
  const visited = new Set<string>();
  const startNodes: Array<{ task: HarnessPlanTask; depth: number }> =
    rootTaskId === undefined
      ? roots.map(task => ({ task, depth: 0 }))
      : byId.has(rootTaskId)
        ? [{ task: byId.get(rootTaskId)!, depth: 0 }]
        : [];
  const stack = [...startNodes].reverse();
  while (stack.length > 0) {
    const { task, depth: nodeDepth } = stack.pop()!;
    if (visited.has(task.taskId)) continue;
    visited.add(task.taskId);
    const matchesStatus = status === undefined || task.status === status;
    if (matchesStatus) {
      if (result.length >= limit) {
        truncated = true;
        break;
      }
      result.push(structuredClone(task));
    }
    if (depth === undefined || nodeDepth < depth) {
      const children = childrenByParent.get(task.taskId) ?? [];
      for (let i = children.length - 1; i >= 0; i--) {
        stack.push({ task: children[i]!, depth: nodeDepth + 1 });
      }
    }
  }
  return { tasks: result, truncated };
}
