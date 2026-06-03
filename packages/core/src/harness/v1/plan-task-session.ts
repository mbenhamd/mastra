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
  /** TM-6: present when this op set/changed the delegation link. */
  delegatedSubagentSessionId?: string;
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
  /** TM-6: the subagent session this task was durably delegated to, if any. */
  delegatedSubagentSessionId?: string;
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
  if (task.delegatedSubagentSessionId !== undefined) {
    view.delegatedSubagentSessionId = task.delegatedSubagentSessionId;
  }
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
  const { changed, source: rollupSource } = rollupTree(postTasks, stagedStatus, stagedSource);

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
      startedAt?: number;
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
    // Rollup reports the source it decided per node: 'derived' for a child
    // rollup, 'explicit' for a pure blockedBy overlay on a childless node (which
    // keeps its own identity so it reverts when the dep clears). An explicit set
    // staged earlier in this op still wins (w.statusSource already 'explicit').
    if (w.statusSource === undefined) w.statusSource = rollupSource.get(id) ?? 'derived';
  }

  // Timestamp bookkeeping. Key off the PERSISTED `completedAt`/`startedAt`
  // presence on the post-image row — NOT off a `prev` status, because the
  // explicit-set callers pre-apply the new `status` into `postTasks` (so a
  // status-based "did it just transition" check would always see the new value
  // and never fire). completedAt: set when entering 'completed' if not already
  // stamped, clear when leaving. startedAt (span-summary O7): set ONCE the first
  // time a task is in_progress (preserved across later oscillation so it always
  // marks when work first began).
  const now = Date.now();
  for (const [id, w] of statusWrites) {
    if (w.status === undefined) continue;
    const row = byId.get(id);
    if (w.status === 'completed') {
      if (row?.completedAt === undefined) w.completedAt = now;
    } else if (row?.completedAt !== undefined) {
      w.clearCompletedAt = true;
    }
    if (w.status === 'in_progress' && row?.startedAt === undefined) w.startedAt = now;
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
      if (w.startedAt !== undefined) createOp.task.startedAt = w.startedAt;
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
        ...(w.startedAt !== undefined ? { startedAt: w.startedAt } : {}),
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
  // TM-6: tasks whose delegation link this op set, so the delta carries it.
  const delegationTouched = new Set<string>();
  for (const op of structuralOps) {
    if (op.kind === 'create') {
      changedIds.add(op.task.taskId);
      contentTouched.add(op.task.taskId);
    } else if (op.kind === 'update') {
      changedIds.add(op.taskId);
      if (op.patch.content !== undefined) contentTouched.add(op.taskId);
      if (op.patch.delegatedSubagentSessionId !== undefined) delegationTouched.add(op.taskId);
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
    if (delegationTouched.has(id) && row.delegatedSubagentSessionId !== undefined) {
      delta.delegatedSubagentSessionId = row.delegatedSubagentSessionId;
    }
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

/**
 * Emit the `papersflow.plan_task.updated` event BEST-EFFORT (TM-6). The event
 * is turn-gated (`_emitCustomEvent` rejects when no turn is in flight), but a
 * rollup-from-delegation lands when the delegated subagent terminalizes —
 * which may be OUTSIDE any parent turn (or even after restart). The durable
 * truth is the committed storage write + refreshed summary; the event is a
 * live convenience. So swallow a turn-gate rejection rather than failing the
 * reconcile. A live subscriber that misses the out-of-turn delta re-reads via
 * `plan_task_check` / the display summary.
 */
function emitBestEffort(
  port: PlanTaskSessionPort,
  op: string,
  affectedTaskIds: string[],
  deltas: PlanTaskDelta[],
): void {
  try {
    emit(port, op, affectedTaskIds, deltas);
  } catch {
    // out-of-turn delegated rollup — durable write already committed.
  }
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

  // Validate each child's `blockedBy` exactly as task_add / task_update do:
  // dependency ids must resolve within THIS session's tree (so unknown +
  // cross-session deps reject) and must not close a dependency cycle. New
  // siblings created in the same decompose are visible to each other, so a child
  // may depend on a sibling created in the same call.
  const depIndex = indexWith(index, ...created);
  for (const child of created) {
    if (!child.blockedBy || child.blockedBy.length === 0) continue;
    for (const dep of child.blockedBy) {
      if (!depIndex.byId.has(dep)) {
        throw new HarnessValidationError('children.blockedBy', `unknown dependency task "${dep}"`);
      }
    }
    assertNoBlockedByCycle(depIndex, child.taskId, child.blockedBy, id => {
      const c = created.find(t => t.taskId === id);
      return c?.blockedBy;
    });
  }

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

  // Re-run the per-root single-in_progress invariant on the POST-move tree: a
  // move can merge two roots that each held their own explicit in_progress into
  // one root with two. Build the index over the post-image so `rootOf` resolves
  // through the new edge, and only count EXPLICIT in_progress (a derived rollup
  // in_progress just mirrors an explicit child — see assertSingleInProgress).
  const postIndex = indexPlanTasks(postTasks);
  for (const candidate of postTasks) {
    if (candidate.status !== 'in_progress' || candidate.statusSource !== 'explicit') continue;
    assertSingleInProgress(
      postIndex,
      candidate.taskId,
      id => postIndex.byId.get(id)?.status ?? 'pending',
      id => postIndex.byId.get(id)?.statusSource ?? 'explicit',
    );
  }

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

// ---------------------------------------------------------------------------
// TM-6 — durable subtask → subagent DELEGATION (§5.1k / §5.6).
//
// `task_delegate` hands a plan node (and, optionally, its subtree subset) to a
// subagent SESSION whose completion the plan task tracks across turns and
// restarts. The durable link is the persisted `delegatedSubagentSessionId`
// field on the plan task — NOT an in-memory await. While the subagent runs the
// task stays `in_progress (delegated)`; when the subagent session terminalizes
// the task rolls up `completed` / `failed` and the TM-4 truth-table cascades to
// ancestors. The spawn + hook + reconcile orchestration lives on `Session` /
// `Harness`; these pure ops own only the FENCED storage writes + rollup so they
// stay unit-testable against a fake port.
// ---------------------------------------------------------------------------

/**
 * Terminal outcome of a delegated subagent SESSION as observed by the parent
 * (live completion hook OR reconcile-on-rehydrate). Maps to a plan-task status:
 *
 *   - `'completed'` → the plan task becomes `completed` (rollup cascades),
 *   - `'failed'`    → the plan task becomes `failed` (covers subagent error,
 *     abort/cancel, and a fail-closed "subagent session gone/deleted").
 */
export type DelegatedSubagentOutcome = 'completed' | 'failed';

export interface DelegateTaskInput {
  /** Existing plan task to delegate. */
  taskId: string;
  /** The created/resolved subagent session id to link durably. */
  subagentSessionId: string;
  /**
   * Subagent TYPE id, persisted alongside the session id so a reattached
   * delegated subagent can re-resolve its SubagentDefinition (tools / workspace)
   * after rehydrate (§9). Omitted ⇒ no type recorded (overrides not restorable).
   */
  subagentTypeId?: string;
}

/**
 * Write `delegatedSubagentSessionId` onto the plan task and mark it
 * `in_progress` (delegated), all under the session-owner fence (§5.6 / §5.8).
 * The single-in_progress-per-root invariant is enforced exactly as a manual
 * `task_update({ status: 'in_progress' })` would be. The field write rides the
 * same `mutatePlanTasksForSession` transaction as the status write so the link
 * and the status can never diverge across a crash.
 */
export async function planTaskDelegate(port: PlanTaskSessionPort, input: DelegateTaskInput): Promise<PlanTaskView> {
  if (typeof input.subagentSessionId !== 'string' || input.subagentSessionId.length === 0) {
    throw new HarnessValidationError('subagentSessionId', 'subagentSessionId must be a non-empty string');
  }
  const tasks = await loadAllTasks(port);
  const index = indexPlanTasks(tasks);
  const task = index.byId.get(input.taskId);
  if (!task) throw new HarnessValidationError('taskId', `unknown task "${input.taskId}"`);
  if (task.delegatedSubagentSessionId !== undefined && task.delegatedSubagentSessionId !== input.subagentSessionId) {
    throw new HarnessValidationError(
      'taskId',
      `task "${input.taskId}" is already delegated to subagent session "${task.delegatedSubagentSessionId}"`,
    );
  }

  // Delegation drives the task `in_progress` — enforce the per-root single
  // in_progress invariant (the delegated unit is now the active focus).
  assertSingleInProgress(index, input.taskId, id =>
    id === input.taskId ? 'in_progress' : (index.byId.get(id)?.status ?? 'pending'),
  );

  const updated: HarnessPlanTask = {
    ...task,
    status: 'in_progress',
    statusSource: 'explicit',
    delegatedSubagentSessionId: input.subagentSessionId,
    ...(input.subagentTypeId !== undefined ? { delegatedSubagentTypeId: input.subagentTypeId } : {}),
  };
  const postTasks = tasks.map(t => (t.taskId === input.taskId ? updated : t));

  const staged: StagedChange = { explicitStatus: new Map(), derivedNodes: new Set() };
  staged.explicitStatus.set(input.taskId, 'in_progress');

  // The field write is a structural op (carries the link); the status write is
  // folded in by buildCommitOps via the staged explicit map so both land in one
  // transaction with the right per-row OCC token.
  const structuralOps: PlanTaskMutationOp[] = [
    {
      kind: 'update',
      taskId: input.taskId,
      ifVersion: task.version,
      patch: {
        delegatedSubagentSessionId: input.subagentSessionId,
        ...(input.subagentTypeId !== undefined ? { delegatedSubagentTypeId: input.subagentTypeId } : {}),
      },
    },
  ];
  const { ops, deltas } = buildCommitOps(postTasks, structuralOps, staged);
  await port.storage.mutatePlanTasksForSession({ fence: fence(port), ops });
  refreshSummary(port, postTasks, deltas);
  emit(port, 'delegate', [input.taskId], deltas);
  return toView(updated);
}

export interface ReconcileDelegationInput {
  taskId: string;
  /** Terminal outcome observed from the delegated subagent session. */
  outcome: DelegatedSubagentOutcome;
  /** Expected delegated session id; a mismatch means the link moved — no-op. */
  subagentSessionId: string;
}

export interface ReconcileDelegationResult {
  /** True when a status write was committed (false = already terminal / stale link). */
  reconciled: boolean;
  view?: PlanTaskView;
}

/**
 * Roll a delegated plan task up from its subagent session's TERMINAL outcome
 * (TM-6). Called by the live completion hook AND by reconcile-on-rehydrate, so
 * it is idempotent: if the task already carries an explicit terminal status (a
 * prior reconcile committed, possibly before a crash) it is a no-op. The status
 * write is `explicit` so it survives the rollup pass on its own subtree, and the
 * TM-4 truth-table then cascades the change to ancestors. The event emit is
 * BEST-EFFORT because a delegated subagent can terminalize outside any parent
 * turn — the durable write + summary refresh are the authority.
 *
 * A stale `subagentSessionId` (the task was re-delegated to a different session)
 * is a no-op so a late terminal callback from an abandoned session cannot
 * clobber the live delegation.
 */
export async function planTaskReconcileDelegation(
  port: PlanTaskSessionPort,
  input: ReconcileDelegationInput,
): Promise<ReconcileDelegationResult> {
  const tasks = await loadAllTasks(port);
  const index = indexPlanTasks(tasks);
  const task = index.byId.get(input.taskId);
  if (!task) return { reconciled: false };
  // Link moved (re-delegated) — ignore the stale terminal signal.
  if (task.delegatedSubagentSessionId !== input.subagentSessionId) return { reconciled: false };
  // Idempotent: a task already driven to an explicit terminal status by a prior
  // reconcile (e.g. before a crash) needs no second write.
  if (task.statusSource === 'explicit' && TERMINAL_PLAN_TASK_STATUSES.has(task.status)) {
    return { reconciled: false, view: toView(task) };
  }

  const nextStatus: HarnessPlanTaskStatus = input.outcome === 'completed' ? 'completed' : 'failed';
  const updated: HarnessPlanTask = { ...task, status: nextStatus, statusSource: 'explicit' };
  const postTasks = tasks.map(t => (t.taskId === input.taskId ? updated : t));

  const staged: StagedChange = { explicitStatus: new Map(), derivedNodes: new Set() };
  staged.explicitStatus.set(input.taskId, nextStatus);

  const { ops, deltas } = buildCommitOps(postTasks, [], staged);
  await port.storage.mutatePlanTasksForSession({ fence: fence(port), ops });
  refreshSummary(port, postTasks, deltas);
  emitBestEffort(port, 'delegate_settled', [input.taskId], deltas);
  return { reconciled: true, view: toView(updated) };
}

export interface CheckInput {
  rootTaskId?: string;
  depth?: number;
  status?: HarnessPlanTaskStatus;
  limit?: number;
}

/** Default page size for the bounded read. */
export const PLAN_TASK_CHECK_DEFAULT_LIMIT = 25;
/** Hard cap on the bounded read so a hostile/large `limit` can never pull the
 * whole tree and defeat the anti-forgetting purpose (§5.1k). */
export const PLAN_TASK_CHECK_MAX_LIMIT = 200;

/**
 * Clamp the caller-supplied `limit` to a positive integer in
 * `[1, PLAN_TASK_CHECK_MAX_LIMIT]`. A missing/NaN/non-positive value falls back
 * to the default; anything larger saturates at the cap. Centralizes the bound so
 * the storage read can never be handed an arbitrarily large limit.
 */
export function clampPlanTaskCheckLimit(limit: number | undefined): number {
  if (limit === undefined || !Number.isFinite(limit)) return PLAN_TASK_CHECK_DEFAULT_LIMIT;
  const floored = Math.floor(limit);
  if (floored < 1) return PLAN_TASK_CHECK_DEFAULT_LIMIT;
  return Math.min(floored, PLAN_TASK_CHECK_MAX_LIMIT);
}

/** The bounded anti-forgetting read. Default `limit` keeps the model from ever
 * loading the whole tree by accident; a hostile `limit` is clamped to a hard
 * cap so the read stays bounded regardless of caller input. */
export async function planTaskCheck(
  port: PlanTaskSessionPort,
  input: CheckInput,
): Promise<{ tasks: PlanTaskView[]; truncated: boolean }> {
  const limit = clampPlanTaskCheckLimit(input.limit);
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

/** Build an index that includes one or more not-yet-stored tasks (for
 * cycle/in-progress/dependency checks on a create or decompose). */
function indexWith(index: ReturnType<typeof indexPlanTasks>, ...extra: HarnessPlanTask[]) {
  const tasks = [...index.byId.values(), ...extra];
  return indexPlanTasks(tasks);
}
