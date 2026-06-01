/**
 * TM-3/TM-4 plan-task session operations (HARNESS_V1_SPEC.md §5.1k / §6.4).
 *
 * The live `Session` is the single serialized writer for its plan tree: every
 * mutation routes through here under the session-owner fence (§5.8). This module
 * owns the orchestration that sits BETWEEN the model-facing tools (TM-3) and the
 * durable storage mutators (TM-2):
 *
 *   - apply the requested mutation (add / decompose / reparent / update /
 *     complete) to the loaded tree,
 *   - enforce TM-4 integrity (cycle prevention across parent + blockedBy,
 *     per-root single in_progress),
 *   - recompute derived parent statuses bottom-up (rollup truth-table),
 *   - commit the minimal changed row set transaction-shaped via
 *     `mutatePlanTasksForSession` (all-or-nothing under one fence),
 *   - emit the `papersflow.plan_task.updated` custom event (§10.3).
 *
 * It depends only on a small `PlanTaskSessionPort` so it stays unit-testable
 * without constructing a whole `Session`.
 */

import { randomUUID } from 'node:crypto';

import type { HarnessStorage } from '../../storage/domains/harness';
import type {
  HarnessPlanTask,
  HarnessPlanTaskStatus,
  JsonValue,
  LoadPlanTaskSubtreeResult,
  PlanTaskMutationOp,
} from '../../storage/domains/harness/types';
import { HarnessValidationError } from './errors';
import {
  assertNoBlockedByCycle,
  assertNoParentCycle,
  assertSingleInProgress,
  indexPlanTasks,
  rollupTree,
  TERMINAL_PLAN_TASK_STATUSES,
} from './plan-task-hierarchy';

/** The custom event type emitted on every mutating plan-task operation (§10.3). */
export const PLAN_TASK_UPDATED_EVENT = 'papersflow.plan_task.updated';

/**
 * Minimal slice of `Session` the plan-task ops need. The session implements this
 * with its private internals; tests can pass a fake.
 */
export interface PlanTaskSessionPort {
  readonly id: string;
  readonly resourceId: string;
  readonly threadId: string;
  readonly harnessName: string;
  readonly storage: HarnessStorage;
  readonly ownerId: string;
  /** Current SessionRecord.version — the `ifSessionVersion` for the fence. */
  readonly sessionVersion: number;
  /** Emit the plan-task custom event (delegates to `_emitCustomEvent`). */
  emitPlanTaskEvent(payload: JsonValue): void;
  /**
   * Hand the session the freshly-recomputed bounded plan-task summary after a
   * mutation (TM-5). The session caches it for `getDisplayState()` so the
   * display snapshot never has to load the tree itself. Optional so a fake port
   * (and the read-only `task_check`) can ignore it.
   */
  setPlanTaskSummary?(summary: PlanTaskSummary): void;
}

/**
 * Bounded plan-task SUMMARY surfaced on the display-state snapshot (§4.2 / §5.1k,
 * TM-5). Counts + the active `in_progress` task ids + the root count — NOT the
 * full tree. A UI gets the live shape from this cheap summary, then drives detail
 * off the `papersflow.plan_task.updated` event deltas and the bounded
 * `plan_task_check` read. JSON-safe (primitives + a small id array).
 */
export interface PlanTaskSummary {
  /** Total number of plan-task nodes in the session tree. */
  total: number;
  /** Count of nodes in each status (only present statuses appear). */
  byStatus: Partial<Record<HarnessPlanTaskStatus, number>>;
  /** Ids of the tasks currently `in_progress` (bounded: one per root by §5.1k). */
  inProgressTaskIds: string[];
  /** Number of root nodes (no resolvable parent). */
  rootCount: number;
}

/**
 * Compute the bounded {@link PlanTaskSummary} from the post-mutation tree the op
 * already holds in memory — so the summary refresh costs NO extra storage I/O
 * (the op loaded the tree for rollup/cycle work regardless). Roots are nodes with
 * no `parentTaskId` resolvable in the set (matches `indexPlanTasks`).
 */
export function computePlanTaskSummary(
  tasks: HarnessPlanTask[],
  finalStatusOverride?: Map<string, HarnessPlanTaskStatus>,
): PlanTaskSummary {
  const byId = new Set(tasks.map(t => t.taskId));
  const byStatus: Partial<Record<HarnessPlanTaskStatus, number>> = {};
  const inProgressTaskIds: string[] = [];
  let rootCount = 0;
  for (const t of tasks) {
    const status = finalStatusOverride?.get(t.taskId) ?? t.status;
    byStatus[status] = (byStatus[status] ?? 0) + 1;
    if (status === 'in_progress') inProgressTaskIds.push(t.taskId);
    if (t.parentTaskId === undefined || !byId.has(t.parentTaskId)) rootCount += 1;
  }
  return { total: tasks.length, byStatus, inProgressTaskIds, rootCount };
}

/**
 * Recompute + push the bounded summary to the session from the committed
 * post-image. `postTasks` carries the structural shape; `deltas` carry the
 * authoritative FINAL status of every task the op changed (including the rollup
 * cascade), so we overlay them. Costs no extra I/O.
 */
function refreshSummary(port: PlanTaskSessionPort, postTasks: HarnessPlanTask[], deltas: PlanTaskDelta[]): void {
  if (!port.setPlanTaskSummary) return;
  const override = new Map<string, HarnessPlanTaskStatus>();
  for (const d of deltas) override.set(d.taskId, d.status);
  port.setPlanTaskSummary(computePlanTaskSummary(postTasks, override));
}

/**
 * Compact per-task delta carried in the `papersflow.plan_task.updated` event
 * (§10.3, TM-5). One entry per task the op CHANGED — the directly-edited rows
 * AND every task whose DERIVED status flipped from the rollup cascade — so a UI
 * can apply an incremental patch without re-reading the tree. Bounded to the
 * affected set; JSON-safe (primitive fields only). `content` is included only
 * when this op set it (add/decompose/content edit), so a pure status rollup
 * stays compact.
 */
export interface PlanTaskDelta {
  taskId: string;
  parentTaskId?: string;
  status: HarnessPlanTaskStatus;
  statusSource: 'explicit' | 'derived';
  order: number;
  content?: string;
}

/** The full event payload for `papersflow.plan_task.updated` (§10.3, TM-5). */
export interface PlanTaskUpdatedPayload {
  op: string;
  affectedTaskIds: string[];
  /** Per-task post-image deltas for every changed task (bounded, JSON-safe). */
  deltas: PlanTaskDelta[];
}

/** A plan-task as the model sees it through the tool boundary. */
export interface PlanTaskView {
  taskId: string;
  parentTaskId?: string;
  order: number;
  status: HarnessPlanTaskStatus;
  statusSource: 'explicit' | 'derived';
  content: string;
  activeForm?: string;
  priority?: number;
  blockedBy?: string[];
}

function toView(task: HarnessPlanTask): PlanTaskView {
  const view: PlanTaskView = {
    taskId: task.taskId,
    order: task.order,
    status: task.status,
    statusSource: task.statusSource,
    content: task.content,
  };
  if (task.parentTaskId !== undefined) view.parentTaskId = task.parentTaskId;
  if (task.activeForm !== undefined) view.activeForm = task.activeForm;
  if (task.priority !== undefined) view.priority = task.priority;
  if (task.blockedBy !== undefined) view.blockedBy = [...task.blockedBy];
  return view;
}

// ---------------------------------------------------------------------------
// Tree loading
// ---------------------------------------------------------------------------

/** Load the session's full plan tree (paged) for in-memory rollup + cycle work.
 * The model-facing READ surface is the bounded `task_check`; this internal load
 * is the authoritative set the writer reasons over before committing. */
async function loadAllTasks(port: PlanTaskSessionPort): Promise<HarnessPlanTask[]> {
  const all: HarnessPlanTask[] = [];
  let cursor: string | undefined;
  // Page defensively; a single plan tree is small but unbounded by contract.
  for (;;) {
    const page = await port.storage.listPlanTasks({
      harnessName: port.harnessName,
      sessionId: port.id,
      limit: 500,
      cursor,
    });
    all.push(...page.tasks);
    if (!page.cursor || page.tasks.length === 0) break;
    cursor = page.cursor;
  }
  return all;
}

// ---------------------------------------------------------------------------
// Shared commit helper
// ---------------------------------------------------------------------------

interface StagedChange {
  /** Explicit statuses set by the caller in this op (status + 'explicit'). */
  explicitStatus: Map<string, HarnessPlanTaskStatus>;
  /** Nodes whose statusSource the caller flipped to 'derived' (gained children). */
  derivedNodes: Set<string>;
}

interface CommitPlan {
  ops: PlanTaskMutationOp[];
  /**
   * Post-image deltas (TM-5) for every task this op CHANGED: the directly-edited
   * rows (structural ops) plus every task whose status the rollup flipped. Built
   * from the post-mutation `postTasks` image folded with the final status/source
   * decisions, so it reflects exactly what landed.
   */
  deltas: PlanTaskDelta[];
}

/**
 * Given the post-mutation task set + the staged status/source decisions, run
 * rollup and return the FULL ordered op list to commit (the caller's structural
 * ops PLUS the derived-status updates rollup produced), alongside the bounded
 * per-task deltas for the event payload. The caller passes the structural ops
 * (create/reparent updates) that change tree shape; this folds in the status
 * writes.
 */
function buildCommitOps(
  postTasks: HarnessPlanTask[],
  structuralOps: PlanTaskMutationOp[],
  staged: StagedChange,
): CommitPlan {
  // Stage the status map rollup should see: explicit statuses + the source flips.
  const stagedStatus = new Map<string, HarnessPlanTaskStatus>(staged.explicitStatus);
  const stagedSource = new Map<string, 'explicit' | 'derived'>();
  for (const [id] of staged.explicitStatus) stagedSource.set(id, 'explicit');
  for (const id of staged.derivedNodes) {
    if (!stagedSource.has(id)) stagedSource.set(id, 'derived');
  }
  const { changed } = rollupTree(postTasks, stagedStatus, stagedSource);

  // Build a quick index of the post-mutation rows so we can attach the right
  // ifVersion to each status update.
  const byId = new Map(postTasks.map(t => [t.taskId, t]));

  // Collect every taskId that needs a status/source write: explicit sets, source
  // flips, and rollup-derived changes. A single update op per task carries all of
  // status + statusSource + completedAt.
  const statusWrites = new Map<
    string,
    {
      status?: HarnessPlanTaskStatus;
      statusSource?: 'explicit' | 'derived';
      completedAt?: number;
      clearCompletedAt?: boolean;
    }
  >();
  const upsert = (id: string) => {
    let w = statusWrites.get(id);
    if (!w) {
      w = {};
      statusWrites.set(id, w);
    }
    return w;
  };

  for (const [id, status] of staged.explicitStatus) {
    const w = upsert(id);
    w.status = status;
    w.statusSource = 'explicit';
  }
  for (const id of staged.derivedNodes) {
    const w = upsert(id);
    if (w.statusSource === undefined) w.statusSource = 'derived';
  }
  for (const [id, status] of changed) {
    const w = upsert(id);
    w.status = status;
    // rollup only recomputes derived nodes, so the source is derived.
    if (w.statusSource === undefined) w.statusSource = 'derived';
  }

  // completedAt bookkeeping: set when a status becomes 'completed', clear when it
  // moves away from 'completed'.
  const now = Date.now();
  for (const [id, w] of statusWrites) {
    if (w.status === undefined) continue;
    const prev = byId.get(id)?.status;
    if (w.status === 'completed' && prev !== 'completed') w.completedAt = now;
    else if (w.status !== 'completed' && prev === 'completed') w.clearCompletedAt = true;
  }

  // Build the final op list: structural ops first (they may create rows the
  // status writes target), then a status update per affected row.
  const ops: PlanTaskMutationOp[] = [...structuralOps];
  // Track ifVersion increments for rows already updated by a structural op so a
  // following status write uses the right OCC token within the same transaction.
  const structuralVersionBump = new Map<string, number>();
  for (const op of structuralOps) {
    if (op.kind === 'update') {
      structuralVersionBump.set(op.taskId, (structuralVersionBump.get(op.taskId) ?? 0) + 1);
    }
  }

  for (const [id, w] of statusWrites) {
    if (w.status === undefined && w.statusSource === undefined) continue;
    const row = byId.get(id);
    // A row created in this same transaction (structural create) has version 1
    // and no stored row yet; fold the status into that create instead of a
    // separate update so the transaction stays consistent.
    const createOp = structuralOps.find(o => o.kind === 'create' && o.task.taskId === id) as
      | Extract<PlanTaskMutationOp, { kind: 'create' }>
      | undefined;
    if (createOp) {
      if (w.status !== undefined) createOp.task.status = w.status;
      if (w.statusSource !== undefined) createOp.task.statusSource = w.statusSource;
      if (w.completedAt !== undefined) createOp.task.completedAt = w.completedAt;
      if (w.clearCompletedAt) delete createOp.task.completedAt;
      continue;
    }
    if (!row) continue;
    const baseVersion = row.version + (structuralVersionBump.get(id) ?? 0);
    ops.push({
      kind: 'update',
      taskId: id,
      ifVersion: baseVersion,
      patch: {
        ...(w.status !== undefined ? { status: w.status } : {}),
        ...(w.statusSource !== undefined ? { statusSource: w.statusSource } : {}),
        ...(w.completedAt !== undefined ? { completedAt: w.completedAt } : {}),
        ...(w.clearCompletedAt ? { clearCompletedAt: true } : {}),
      },
    });
  }

  // ---- TM-5 deltas ---------------------------------------------------------
  // The set of CHANGED tasks: every task targeted by a structural op (create /
  // update) plus every task whose status/source was written (explicit set,
  // source flip, or rollup cascade). For each we project a bounded post-image
  // from `byId` (the post-mutation tree) folded with the final status decision.
  const changedIds = new Set<string>(statusWrites.keys());
  // Tasks whose content this op set, so the delta carries it (a pure rollup of
  // an unrelated task omits content to stay compact).
  const contentTouched = new Set<string>();
  for (const op of structuralOps) {
    if (op.kind === 'create') {
      changedIds.add(op.task.taskId);
      contentTouched.add(op.task.taskId);
    } else if (op.kind === 'update') {
      changedIds.add(op.taskId);
      if (op.patch.content !== undefined) contentTouched.add(op.taskId);
    }
  }

  const deltas: PlanTaskDelta[] = [];
  for (const id of changedIds) {
    const row = byId.get(id);
    if (!row) continue; // defensive: should always resolve in the post-image
    const w = statusWrites.get(id);
    const delta: PlanTaskDelta = {
      taskId: id,
      status: w?.status ?? row.status,
      statusSource: w?.statusSource ?? row.statusSource,
      order: row.order,
    };
    if (row.parentTaskId !== undefined) delta.parentTaskId = row.parentTaskId;
    if (contentTouched.has(id)) delta.content = row.content;
    deltas.push(delta);
  }

  return { ops, deltas };
}

function fence(port: PlanTaskSessionPort) {
  return {
    harnessName: port.harnessName,
    sessionId: port.id,
    ownerId: port.ownerId,
    ifSessionVersion: port.sessionVersion,
  };
}

/**
 * Emit the `papersflow.plan_task.updated` event (§10.3). `affectedTaskIds`
 * preserves the op-meaningful order (e.g. `[parent, ...children]` for
 * decompose); `deltas` carries the bounded per-task post-image for EVERY changed
 * task — the directly-edited rows plus every task whose derived status the
 * rollup cascade flipped — so a UI can patch incrementally without a re-read.
 */
function emit(port: PlanTaskSessionPort, op: string, affectedTaskIds: string[], deltas: PlanTaskDelta[]): void {
  const payload: PlanTaskUpdatedPayload = { op, affectedTaskIds, deltas };
  port.emitPlanTaskEvent(payload as unknown as JsonValue);
}

// ---------------------------------------------------------------------------
// Operations
// ---------------------------------------------------------------------------

export interface AddTaskInput {
  content: string;
  parentTaskId?: string;
  order?: number;
  priority?: number;
  activeForm?: string;
  status?: HarnessPlanTaskStatus;
  blockedBy?: string[];
  idempotencyKey?: string;
}

export async function planTaskAdd(port: PlanTaskSessionPort, input: AddTaskInput): Promise<PlanTaskView> {
  if (typeof input.content !== 'string' || input.content.length === 0) {
    throw new HarnessValidationError('content', 'task content must be a non-empty string');
  }
  const tasks = await loadAllTasks(port);
  const index = indexPlanTasks(tasks);
  if (input.parentTaskId !== undefined && !index.byId.has(input.parentTaskId)) {
    throw new HarnessValidationError('parentTaskId', `unknown parent task "${input.parentTaskId}"`);
  }
  if (input.blockedBy && input.blockedBy.length > 0) {
    for (const dep of input.blockedBy) {
      if (!index.byId.has(dep)) throw new HarnessValidationError('blockedBy', `unknown dependency task "${dep}"`);
    }
  }

  const taskId = `task-${randomUUID()}`;
  const status: HarnessPlanTaskStatus = input.status ?? 'pending';
  const order = input.order ?? nextOrder(tasks, input.parentTaskId);
  const newTask: HarnessPlanTask = {
    taskId,
    harnessName: port.harnessName,
    sessionId: port.id,
    resourceId: port.resourceId,
    threadId: port.threadId,
    order,
    status,
    statusSource: 'explicit',
    content: input.content,
    createdAt: 0,
    updatedAt: 0,
    version: 1,
    ...(input.parentTaskId !== undefined ? { parentTaskId: input.parentTaskId } : {}),
    ...(input.activeForm !== undefined ? { activeForm: input.activeForm } : {}),
    ...(input.priority !== undefined ? { priority: input.priority } : {}),
    ...(input.blockedBy !== undefined ? { blockedBy: [...input.blockedBy] } : {}),
    ...(input.idempotencyKey !== undefined ? { idempotencyKey: input.idempotencyKey } : {}),
  };

  // blockedBy cycle check (a new task cannot depend on something that depends on it —
  // impossible for a brand-new id, but a self-reference / future-proofing guard).
  if (newTask.blockedBy) {
    assertNoBlockedByCycle(indexWith(index, newTask), taskId, newTask.blockedBy, () => undefined);
  }

  const postTasks = [...tasks, newTask];
  const staged: StagedChange = { explicitStatus: new Map(), derivedNodes: new Set() };
  if (status === 'in_progress') {
    assertSingleInProgress(indexWith(index, newTask), taskId, id =>
      id === taskId ? status : (index.byId.get(id)?.status ?? 'pending'),
    );
    staged.explicitStatus.set(taskId, status);
  }
  // A new child flips its parent to 'derived' (unless the parent is
  // explicit-terminal). The parent then reflects its children via rollup.
  if (input.parentTaskId !== undefined) maybeFlipParentDerived(index, input.parentTaskId, staged);

  const structuralOps: PlanTaskMutationOp[] = [{ kind: 'create', task: newTask }];
  const { ops, deltas } = buildCommitOps(postTasks, structuralOps, staged);
  await port.storage.mutatePlanTasksForSession({ fence: fence(port), ops });
  refreshSummary(port, postTasks, deltas);
  emit(port, 'add', [taskId], deltas);
  return toView(newTask);
}

export interface DecomposeChildInput {
  content: string;
  order?: number;
  priority?: number;
  activeForm?: string;
  blockedBy?: string[];
}

export async function planTaskDecompose(
  port: PlanTaskSessionPort,
  parentTaskId: string,
  children: DecomposeChildInput[],
): Promise<PlanTaskView[]> {
  if (!Array.isArray(children) || children.length === 0) {
    throw new HarnessValidationError('children', 'decompose requires at least one child task');
  }
  const tasks = await loadAllTasks(port);
  const index = indexPlanTasks(tasks);
  if (!index.byId.has(parentTaskId)) {
    throw new HarnessValidationError('parentTaskId', `unknown parent task "${parentTaskId}"`);
  }
  const baseOrder = nextOrder(tasks, parentTaskId);
  const created: HarnessPlanTask[] = [];
  children.forEach((child, i) => {
    if (typeof child.content !== 'string' || child.content.length === 0) {
      throw new HarnessValidationError('children.content', 'each child needs non-empty content');
    }
    created.push({
      taskId: `task-${randomUUID()}`,
      harnessName: port.harnessName,
      sessionId: port.id,
      resourceId: port.resourceId,
      threadId: port.threadId,
      parentTaskId,
      order: child.order ?? baseOrder + i,
      status: 'pending',
      statusSource: 'explicit',
      content: child.content,
      createdAt: 0,
      updatedAt: 0,
      version: 1,
      ...(child.activeForm !== undefined ? { activeForm: child.activeForm } : {}),
      ...(child.priority !== undefined ? { priority: child.priority } : {}),
      ...(child.blockedBy !== undefined ? { blockedBy: [...child.blockedBy] } : {}),
    });
  });

  const postTasks = [...tasks, ...created];
  const staged: StagedChange = { explicitStatus: new Map(), derivedNodes: new Set() };
  maybeFlipParentDerived(index, parentTaskId, staged);

  const structuralOps: PlanTaskMutationOp[] = created.map(task => ({ kind: 'create', task }));
  const { ops, deltas } = buildCommitOps(postTasks, structuralOps, staged);
  await port.storage.mutatePlanTasksForSession({ fence: fence(port), ops });
  refreshSummary(port, postTasks, deltas);
  const ids = created.map(t => t.taskId);
  emit(port, 'decompose', [parentTaskId, ...ids], deltas);
  return created.map(toView);
}

export async function planTaskReparent(
  port: PlanTaskSessionPort,
  taskId: string,
  newParentTaskId: string | null,
  order?: number,
): Promise<void> {
  const tasks = await loadAllTasks(port);
  const index = indexPlanTasks(tasks);
  const task = index.byId.get(taskId);
  if (!task) throw new HarnessValidationError('taskId', `unknown task "${taskId}"`);
  if (newParentTaskId !== null) {
    if (!index.byId.has(newParentTaskId)) {
      throw new HarnessValidationError('newParentTaskId', `unknown parent task "${newParentTaskId}"`);
    }
    // Cycle prevention: the new parent must not be the task itself or a descendant.
    assertNoParentCycle(index, taskId, newParentTaskId);
  }

  const oldParentId = task.parentTaskId;
  const moved: HarnessPlanTask = {
    ...task,
    ...(newParentTaskId !== null ? { parentTaskId: newParentTaskId } : {}),
    order: order ?? task.order,
  };
  if (newParentTaskId === null) delete moved.parentTaskId;
  const postTasks = tasks.map(t => (t.taskId === taskId ? moved : t));

  const staged: StagedChange = { explicitStatus: new Map(), derivedNodes: new Set() };
  // New parent gains a child → derived (unless explicit-terminal).
  if (newParentTaskId !== null) maybeFlipParentDerived(index, newParentTaskId, staged);

  const structuralOps: PlanTaskMutationOp[] = [
    {
      kind: 'update',
      taskId,
      ifVersion: task.version,
      patch:
        newParentTaskId === null
          ? { clearParentTaskId: true, order: moved.order }
          : { parentTaskId: newParentTaskId, order: moved.order },
    },
  ];
  const { ops, deltas } = buildCommitOps(postTasks, structuralOps, staged);
  await port.storage.mutatePlanTasksForSession({ fence: fence(port), ops });
  refreshSummary(port, postTasks, deltas);
  const affected = [taskId, ...(newParentTaskId ? [newParentTaskId] : []), ...(oldParentId ? [oldParentId] : [])];
  emit(port, 'reparent', affected, deltas);
}

export interface UpdateTaskInput {
  status?: HarnessPlanTaskStatus;
  content?: string;
  priority?: number;
  activeForm?: string;
  blockedBy?: string[];
}

export async function planTaskUpdate(
  port: PlanTaskSessionPort,
  taskId: string,
  patch: UpdateTaskInput,
): Promise<PlanTaskView> {
  const tasks = await loadAllTasks(port);
  const index = indexPlanTasks(tasks);
  const task = index.byId.get(taskId);
  if (!task) throw new HarnessValidationError('taskId', `unknown task "${taskId}"`);

  // blockedBy cycle check against the proposed full dependency list.
  if (patch.blockedBy !== undefined) {
    for (const dep of patch.blockedBy) {
      if (!index.byId.has(dep)) throw new HarnessValidationError('blockedBy', `unknown dependency task "${dep}"`);
    }
    assertNoBlockedByCycle(index, taskId, patch.blockedBy, id => (id === taskId ? patch.blockedBy : undefined));
  }

  const updated: HarnessPlanTask = {
    ...task,
    ...(patch.content !== undefined ? { content: patch.content } : {}),
    ...(patch.priority !== undefined ? { priority: patch.priority } : {}),
    ...(patch.activeForm !== undefined ? { activeForm: patch.activeForm } : {}),
    ...(patch.blockedBy !== undefined ? { blockedBy: [...patch.blockedBy] } : {}),
    // Reflect an explicit status set in this op so the returned view + the
    // rollup post-image agree with what gets committed.
    ...(patch.status !== undefined ? { status: patch.status, statusSource: 'explicit' as const } : {}),
  };
  const postTasks = tasks.map(t => (t.taskId === taskId ? updated : t));

  const staged: StagedChange = { explicitStatus: new Map(), derivedNodes: new Set() };
  if (patch.status !== undefined) {
    if (patch.status === 'in_progress') {
      assertSingleInProgress(index, taskId, id =>
        id === taskId ? 'in_progress' : (index.byId.get(id)?.status ?? 'pending'),
      );
    }
    staged.explicitStatus.set(taskId, patch.status);
  }

  const structuralOps: PlanTaskMutationOp[] = [];
  const fieldPatch: PlanTaskMutationOp = {
    kind: 'update',
    taskId,
    ifVersion: task.version,
    patch: {
      ...(patch.content !== undefined ? { content: patch.content } : {}),
      ...(patch.priority !== undefined ? { priority: patch.priority } : {}),
      ...(patch.activeForm !== undefined ? { activeForm: patch.activeForm } : {}),
      ...(patch.blockedBy !== undefined ? { blockedBy: [...patch.blockedBy] } : {}),
    },
  };
  // Only push a field-only structural op if it carries non-status fields; the
  // status write is folded in by buildCommitOps via the staged explicit map.
  if (Object.keys(fieldPatch.kind === 'update' ? fieldPatch.patch : {}).length > 0) {
    structuralOps.push(fieldPatch);
  }
  const { ops, deltas } = buildCommitOps(postTasks, structuralOps, staged);
  await port.storage.mutatePlanTasksForSession({ fence: fence(port), ops });
  refreshSummary(port, postTasks, deltas);
  emit(port, 'update', [taskId], deltas);
  return toView(updated);
}

export async function planTaskComplete(port: PlanTaskSessionPort, taskId: string): Promise<PlanTaskView> {
  return planTaskUpdate(port, taskId, { status: 'completed' });
}

export interface CheckInput {
  rootTaskId?: string;
  depth?: number;
  status?: HarnessPlanTaskStatus;
  limit?: number;
}

/** The bounded anti-forgetting read. Default `limit` keeps the model from ever
 * loading the whole tree by accident. */
export async function planTaskCheck(
  port: PlanTaskSessionPort,
  input: CheckInput,
): Promise<{ tasks: PlanTaskView[]; truncated: boolean }> {
  const limit = input.limit ?? 25;
  const result: LoadPlanTaskSubtreeResult = await port.storage.loadPlanTaskSubtree({
    harnessName: port.harnessName,
    sessionId: port.id,
    rootTaskId: input.rootTaskId,
    depth: input.depth,
    status: input.status,
    limit,
  });
  return { tasks: result.tasks.map(toView), truncated: result.truncated };
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Next sibling order = max(existing sibling order) + 1, or 0. */
function nextOrder(tasks: HarnessPlanTask[], parentTaskId: string | undefined): number {
  let max = -1;
  for (const t of tasks) {
    if ((t.parentTaskId ?? undefined) === parentTaskId && t.order > max) max = t.order;
  }
  return max + 1;
}

/** Mark a parent 'derived' (so rollup owns its status) unless it carries an
 * explicit terminal status the model deliberately set — those always win. */
function maybeFlipParentDerived(
  index: ReturnType<typeof indexPlanTasks>,
  parentTaskId: string,
  staged: StagedChange,
): void {
  const parent = index.byId.get(parentTaskId);
  if (!parent) return;
  if (parent.statusSource === 'explicit' && TERMINAL_PLAN_TASK_STATUSES.has(parent.status)) return;
  if (parent.statusSource === 'derived') return; // already derived
  staged.derivedNodes.add(parentTaskId);
}

/** Build an index that includes a not-yet-stored task (for cycle/in-progress
 * checks on a create). */
function indexWith(index: ReturnType<typeof indexPlanTasks>, extra: HarnessPlanTask) {
  const tasks = [...index.byId.values(), extra];
  return indexPlanTasks(tasks);
}
