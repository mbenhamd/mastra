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
 * New idempotent creates must persist the immutable input hash alongside the
 * key. Existing rows created before the hash column was introduced may still
 * omit it; callers validate only the incoming create so those rows remain
 * readable and retain their key-only replay behavior.
 */
export function assertPlanTaskCreateIdempotencyInput(
  task: Pick<HarnessPlanTask, 'idempotencyKey' | 'idempotencyInputHash'>,
): void {
  if (
    task.idempotencyKey !== undefined &&
    (typeof task.idempotencyInputHash !== 'string' || task.idempotencyInputHash.length === 0)
  ) {
    throw new TypeError('Plan task idempotencyInputHash must be a non-empty string when idempotencyKey is provided');
  }
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
  if (patch.clearDelegatedSubagentSessionId === true) delete next.delegatedSubagentSessionId;
  if (patch.delegatedSubagentTypeId !== undefined) {
    next.delegatedSubagentTypeId = patch.delegatedSubagentTypeId;
  }
  if (patch.clearDelegatedSubagentTypeId === true) delete next.delegatedSubagentTypeId;
  if (patch.metadata !== undefined) next.metadata = cloneJsonValue(patch.metadata);
  if (patch.clearMetadata === true) delete next.metadata;
  if (patch.startedAt !== undefined) next.startedAt = patch.startedAt;
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

/** Decoded keyset position of a `listPlanTasks` cursor. */
export interface PlanTaskCursor {
  parent: string;
  order: number;
  taskId: string;
}

/**
 * Opaque base64 keyset cursor over the `comparePlanTaskOrder` sort key
 * (parentTaskId, order, taskId). SHARED by every adapter so the cursor TOKEN is
 * byte-identical and pagination semantics — including continuation when the
 * cursor's own row was deleted between pages — match across InMemory/PG/LibSQL.
 */
export function encodePlanTaskCursor(task: HarnessPlanTask): string {
  return Buffer.from(JSON.stringify({ p: task.parentTaskId ?? '', o: task.order, t: task.taskId }), 'utf-8').toString(
    'base64',
  );
}

export function decodePlanTaskCursor(cursor: string): PlanTaskCursor {
  let parsed: { p?: unknown; o?: unknown; t?: unknown };
  try {
    parsed = JSON.parse(Buffer.from(cursor, 'base64').toString('utf-8'));
  } catch {
    throw new Error('Invalid plan-task cursor: not a decodable keyset token');
  }
  // Validate the decoded shape — a foreign/truncated token can base64-decode to
  // valid JSON of the wrong shape, which would otherwise compare as undefined and
  // silently corrupt pagination. Fail clearly instead.
  if (
    typeof parsed?.p !== 'string' ||
    typeof parsed?.o !== 'number' ||
    !Number.isFinite(parsed.o) ||
    typeof parsed?.t !== 'string'
  ) {
    throw new Error('Invalid plan-task cursor: malformed keyset payload');
  }
  return { parent: parsed.p, order: parsed.o, taskId: parsed.t };
}

/**
 * True when `task` sorts STRICTLY AFTER `cursor` under `comparePlanTaskOrder`.
 * Mirrors the SQL keyset WHERE predicate so the in-memory adapter keyset-CONTINUES
 * from the cursor position even when the cursor's own row was deleted (the old
 * `findIndex(taskId === cursor)` returned -1 and silently restarted from the head).
 */
export function planTaskAfterCursor(task: HarnessPlanTask, cursor: PlanTaskCursor): boolean {
  const p = task.parentTaskId ?? '';
  if (p !== cursor.parent) return p > cursor.parent;
  if (task.order !== cursor.order) return task.order > cursor.order;
  return task.taskId.localeCompare(cursor.taskId) > 0;
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
