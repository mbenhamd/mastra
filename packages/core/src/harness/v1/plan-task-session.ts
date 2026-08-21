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

import { createHash, randomUUID } from 'node:crypto';

import type { HarnessStorage } from '../../storage/domains/harness';
import type {
  HarnessPlanTask,
  HarnessPlanTaskStatus,
  JsonValue,
  LoadPlanTaskSubtreeResult,
  PlanTaskMutationOp,
} from '../../storage/domains/harness/types';
import { assertJsonValue, sha256CanonicalJsonChecked } from './canonical-json';
import { HarnessValidationError } from './errors';
import {
  assertNoBlockedByCycle,
  assertNoPlanTaskCombinedCycle,
  assertNoParentCycle,
  assertSingleInProgress,
  hasUnsatisfiedDep,
  indexPlanTasks,
  rollupTree,
  TERMINAL_PLAN_TASK_STATUSES,
} from './plan-task-hierarchy';
import { harnessSubagentResultSummarySchema } from './terminal-subagent-result';
import type { HarnessSubagentResultSummary } from './terminal-subagent-result';

/** The custom event type emitted on every mutating plan-task operation (§10.3). */
export const PLAN_TASK_UPDATED_EVENT = 'papersflow.plan_task.updated';

/**
 * A plan is deliberately small and operational. This bound guarantees that a
 * single logical mutation (including a full reset or worst-case ancestor
 * rollup) fits the transaction contract implemented by every supported Harness
 * storage adapter. Larger bodies belong in workspace artifacts, not in the
 * task tracker.
 */
export const PLAN_TASK_MAX_NODES = 100;
/** Task labels stay concise; larger working material belongs in artifacts. */
export const PLAN_TASK_CONTENT_MAX_BYTES = 512;
export const PLAN_TASK_ACTIVE_FORM_MAX_BYTES = 256;
export const PLAN_TASK_IDEMPOTENCY_KEY_MAX_BYTES = 256;
/** Hard provider-prompt budget for one immutable delegated admission body. */
export const PLAN_TASK_DELEGATED_BODY_MAX_BYTES = 64 * 1024;
/** Default wall-clock budget for one durable delegated attempt. */
export const DEFAULT_PLAN_TASK_DELEGATION_TIMEOUT_MS = 30 * 60 * 1000;

function utf8Bytes(value: string): number {
  return new TextEncoder().encode(value).byteLength;
}

function assertBoundedPlanText(field: string, value: unknown, maxBytes: number, requireNonEmpty = false): void {
  if (typeof value !== 'string' || (requireNonEmpty && value.length === 0)) {
    throw new HarnessValidationError(field, requireNonEmpty ? 'must be a non-empty string' : 'must be a string');
  }
  const bytes = utf8Bytes(value);
  if (bytes > maxBytes) {
    throw new HarnessValidationError(field, `must be at most ${maxBytes} UTF-8 bytes; received ${bytes}`);
  }
}

function assertBoundedDependencies(field: string, dependencies: readonly string[] | undefined): void {
  if (dependencies !== undefined && dependencies.length > PLAN_TASK_MAX_NODES) {
    throw new HarnessValidationError(field, `must contain at most ${PLAN_TASK_MAX_NODES} task ids`);
  }
}

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
  /** TM-6: string when linked, null when a terminal attempt cleared the link. */
  delegatedSubagentSessionId?: string | null;
  /** Bounded durable delegation state/result when the link changed. */
  delegation?: PlanTaskView['delegation'] | null;
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
  /** Echoed only by task_decompose so callers can map local dependency labels to generated ids. */
  decompositionLocalKey?: string;
  /** Durable bounded state/result from the current or latest delegation. */
  delegation?: {
    subagentSessionId: string;
    /** Stable identity for this exact child-owned attempt. */
    attemptId: string;
    /** Exact model tool call that created the durable ownership edge. */
    parentToolCallId: string;
    /** Originating parent run when delegation was initiated by an agent tool. */
    parentRunId?: string;
    status: 'running' | DelegatedSubagentOutcome;
    /** Original wall-clock start; recovery never resets this timestamp. */
    startedAt: number;
    /** Absolute wall-clock deadline for this exact immutable attempt. */
    deadlineAt: number;
    result?: HarnessSubagentResultSummary;
    settledAt?: number;
  };
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
  const delegation = readDelegationAttemptMetadata(task);
  if (delegation !== undefined) {
    view.delegation = {
      subagentSessionId: delegation.subagentSessionId,
      attemptId: delegation.attemptId,
      parentToolCallId: delegation.parentToolCallId,
      ...(delegation.parentRunId !== undefined ? { parentRunId: delegation.parentRunId } : {}),
      status: delegation.settlement?.outcome ?? 'running',
      startedAt: delegation.startedAt,
      deadlineAt: delegation.deadlineAt,
      ...(delegation.settlement?.result !== undefined ? { result: delegation.settlement.result } : {}),
      ...(delegation.settlement?.settledAt !== undefined ? { settledAt: delegation.settlement.settledAt } : {}),
    };
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

function assertPlanTaskCapacity(currentCount: number, additions: number): void {
  if (currentCount + additions > PLAN_TASK_MAX_NODES) {
    throw new HarnessValidationError(
      'planTasks',
      `plan task limit is ${PLAN_TASK_MAX_NODES} nodes; requested mutation would create ${currentCount + additions}`,
    );
  }
}

function assertParentAcceptsChildren(index: ReturnType<typeof indexPlanTasks>, parentTaskId: string): void {
  const parent = index.byId.get(parentTaskId);
  if (parent?.statusSource === 'explicit' && TERMINAL_PLAN_TASK_STATUSES.has(parent.status)) {
    throw new HarnessValidationError('parentTaskId', `terminal task "${parentTaskId}" cannot receive new children`);
  }
}

/** Find the delegated task that owns `taskId` (itself or an ancestor subtree). */
function delegatedOwnerOf(index: ReturnType<typeof indexPlanTasks>, taskId: string): HarnessPlanTask | undefined {
  const visited = new Set<string>();
  let currentId: string | undefined = taskId;
  while (currentId !== undefined && !visited.has(currentId)) {
    visited.add(currentId);
    const current = index.byId.get(currentId);
    if (!current) return undefined;
    if (current.delegatedSubagentSessionId !== undefined) return current;
    currentId = current.parentTaskId;
  }
  return undefined;
}

function assertParentStructureMutable(index: ReturnType<typeof indexPlanTasks>, parentTaskId: string): void {
  const owner = delegatedOwnerOf(index, parentTaskId);
  if (owner === undefined) return;
  throw new HarnessValidationError(
    'parentTaskId',
    `task "${parentTaskId}" belongs to delegated subtree "${owner.taskId}"; its structure is child-owned until settlement`,
  );
}

function subtreeContainsDelegation(index: ReturnType<typeof indexPlanTasks>, rootTaskId: string): boolean {
  const queue = [rootTaskId];
  const visited = new Set<string>();
  while (queue.length > 0) {
    const taskId = queue.shift()!;
    if (visited.has(taskId)) continue;
    visited.add(taskId);
    if (index.byId.get(taskId)?.delegatedSubagentSessionId !== undefined) return true;
    queue.push(...(index.childrenByParent.get(taskId) ?? []));
  }
  return false;
}

function subtreeTaskIds(index: ReturnType<typeof indexPlanTasks>, rootTaskId: string): string[] {
  const result: string[] = [];
  const queue = [rootTaskId];
  const visited = new Set<string>();
  while (queue.length > 0) {
    const taskId = queue.shift()!;
    if (visited.has(taskId)) continue;
    visited.add(taskId);
    if (!index.byId.has(taskId)) continue;
    result.push(taskId);
    queue.push(...(index.childrenByParent.get(taskId) ?? []));
  }
  return result;
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
    // Fold status/timestamp fields into an existing structural update for the
    // same row. Besides avoiding a redundant OCC hop, this guarantees a plan
    // capped at PLAN_TASK_MAX_NODES can never expand into N+1 transaction ops.
    const updateOp = structuralOps.find(o => o.kind === 'update' && o.taskId === id) as
      | Extract<PlanTaskMutationOp, { kind: 'update' }>
      | undefined;
    if (updateOp) {
      Object.assign(updateOp.patch, {
        ...(w.status !== undefined ? { status: w.status } : {}),
        ...(w.statusSource !== undefined ? { statusSource: w.statusSource } : {}),
        ...(w.startedAt !== undefined ? { startedAt: w.startedAt } : {}),
        ...(w.completedAt !== undefined ? { completedAt: w.completedAt } : {}),
        ...(w.clearCompletedAt ? { clearCompletedAt: true } : {}),
      });
      continue;
    }
    if (!row) continue;
    ops.push({
      kind: 'update',
      taskId: id,
      ifVersion: row.version,
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
      if (op.patch.delegatedSubagentSessionId !== undefined || op.patch.clearDelegatedSubagentSessionId === true)
        delegationTouched.add(op.taskId);
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
    if (delegationTouched.has(id)) {
      delta.delegatedSubagentSessionId = row.delegatedSubagentSessionId ?? null;
      delta.delegation = toView(row).delegation ?? null;
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
 * Emit the `papersflow.plan_task.updated` event BEST-EFFORT (TM-6). Session's
 * framework-owned event path supports out-of-turn settlement and persists it;
 * this catch only prevents a post-commit serialization/event-ledger failure
 * from changing the already-authoritative task outcome.
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
    // Durable task mutation already committed; replay/summary stays authority.
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

function taskAddInputHash(input: AddTaskInput): string {
  // Hash only normalized CALL input. Never derive replay identity from the
  // current row: rollup, updates, completion, and reparenting intentionally
  // mutate those fields after creation.
  return sha256CanonicalJsonChecked({
    content: input.content,
    parentTaskId: input.parentTaskId ?? null,
    order: input.order ?? null,
    priority: input.priority ?? null,
    activeForm: input.activeForm ?? null,
    status: input.status ?? 'pending',
    blockedBy: input.blockedBy ?? [],
  });
}

export async function planTaskAdd(port: PlanTaskSessionPort, input: AddTaskInput): Promise<PlanTaskView> {
  assertBoundedPlanText('content', input.content, PLAN_TASK_CONTENT_MAX_BYTES, true);
  if (input.activeForm !== undefined) {
    assertBoundedPlanText('activeForm', input.activeForm, PLAN_TASK_ACTIVE_FORM_MAX_BYTES);
  }
  if (input.idempotencyKey !== undefined) {
    assertBoundedPlanText('idempotencyKey', input.idempotencyKey, PLAN_TASK_IDEMPOTENCY_KEY_MAX_BYTES, true);
  }
  assertBoundedDependencies('blockedBy', input.blockedBy);
  const idempotencyInputHash = input.idempotencyKey === undefined ? undefined : taskAddInputHash(input);
  const tasks = await loadAllTasks(port);
  // Resolve the idempotency key at the orchestration layer before capacity,
  // rollup, summary, and event work. Storage's transaction-shaped create is an
  // idempotent no-op too, but it cannot tell this caller which generated taskId
  // won; returning the newly-generated phantom id would make retries point at a
  // row that does not exist and would corrupt the cached summary.
  if (input.idempotencyKey !== undefined) {
    const existing = tasks.find(task => task.idempotencyKey === input.idempotencyKey);
    if (existing) {
      // Rows created before idempotency input hashes were introduced retain a
      // null hash after the additive storage migration. Their original call
      // input cannot be reconstructed from the mutable task row (even an
      // omitted order is materialized), so preserve the former key-only replay
      // behavior for those rows. Never lazily bind a legacy row to a retry:
      // only newly-created rows have authoritative immutable input evidence.
      if (existing.idempotencyInputHash !== undefined && existing.idempotencyInputHash !== idempotencyInputHash) {
        throw new HarnessValidationError(
          'idempotencyKey',
          `idempotency key "${input.idempotencyKey}" was already used with different task input`,
        );
      }
      return toView(existing);
    }
  }
  assertPlanTaskCapacity(tasks.length, 1);
  const index = indexPlanTasks(tasks);
  if (input.parentTaskId !== undefined && !index.byId.has(input.parentTaskId)) {
    throw new HarnessValidationError('parentTaskId', `unknown parent task "${input.parentTaskId}"`);
  }
  if (input.parentTaskId !== undefined) {
    assertParentAcceptsChildren(index, input.parentTaskId);
    assertParentStructureMutable(index, input.parentTaskId);
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
    ...(idempotencyInputHash !== undefined ? { idempotencyInputHash } : {}),
  };

  // blockedBy cycle check (a new task cannot depend on something that depends on it —
  // impossible for a brand-new id, but a self-reference / future-proofing guard).
  if (newTask.blockedBy) {
    assertNoBlockedByCycle(indexWith(index, newTask), taskId, newTask.blockedBy, () => undefined);
  }

  const postTasks = [...tasks, newTask];
  assertNoPlanTaskCombinedCycle(postTasks, 'blockedBy');
  const staged: StagedChange = { explicitStatus: new Map(), derivedNodes: new Set() };
  if (status === 'in_progress') {
    if (hasUnsatisfiedDep(newTask, id => index.byId.get(id)?.status ?? 'pending')) {
      throw new HarnessValidationError(
        'status',
        `task "${taskId}" has unsatisfied blockedBy dependencies and cannot start`,
      );
    }
    assertSingleInProgress(
      indexWith(index, newTask),
      taskId,
      id => (id === taskId ? status : (index.byId.get(id)?.status ?? 'pending')),
      id => index.byId.get(id)?.statusSource ?? 'explicit',
      id => index.byId.get(id)?.delegatedSubagentSessionId !== undefined,
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
  /** Call-local stable label used only to resolve sibling dependencies. */
  localKey?: string;
  blockedBy?: string[];
  /** `localKey` values of sibling children created by this same atomic call. */
  blockedByLocalKeys?: string[];
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
  assertPlanTaskCapacity(tasks.length, children.length);
  const index = indexPlanTasks(tasks);
  if (!index.byId.has(parentTaskId)) {
    throw new HarnessValidationError('parentTaskId', `unknown parent task "${parentTaskId}"`);
  }
  assertParentAcceptsChildren(index, parentTaskId);
  assertParentStructureMutable(index, parentTaskId);
  const baseOrder = nextOrder(tasks, parentTaskId);
  const taskIds = children.map(() => `task-${randomUUID()}`);
  const localKeyToTaskId = new Map<string, string>();
  children.forEach((child, index) => {
    if (child.localKey === undefined) return;
    if (typeof child.localKey !== 'string' || child.localKey.length === 0 || child.localKey.length > 64) {
      throw new HarnessValidationError('children.localKey', 'localKey must contain 1 to 64 characters');
    }
    if (localKeyToTaskId.has(child.localKey)) {
      throw new HarnessValidationError('children.localKey', `duplicate localKey "${child.localKey}"`);
    }
    localKeyToTaskId.set(child.localKey, taskIds[index]!);
  });
  const created: HarnessPlanTask[] = [];
  children.forEach((child, i) => {
    assertBoundedPlanText('children.content', child.content, PLAN_TASK_CONTENT_MAX_BYTES, true);
    if (child.activeForm !== undefined) {
      assertBoundedPlanText('children.activeForm', child.activeForm, PLAN_TASK_ACTIVE_FORM_MAX_BYTES);
    }
    assertBoundedDependencies('children.blockedBy', child.blockedBy);
    assertBoundedDependencies('children.blockedByLocalKeys', child.blockedByLocalKeys);
    const localDependencies = (child.blockedByLocalKeys ?? []).map(localKey => {
      const dependencyTaskId = localKeyToTaskId.get(localKey);
      if (dependencyTaskId === undefined) {
        throw new HarnessValidationError('children.blockedByLocalKeys', `unknown sibling localKey "${localKey}"`);
      }
      return dependencyTaskId;
    });
    const blockedBy = Array.from(new Set([...(child.blockedBy ?? []), ...localDependencies]));
    created.push({
      taskId: taskIds[i]!,
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
      ...(child.blockedBy !== undefined || child.blockedByLocalKeys !== undefined ? { blockedBy } : {}),
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
  assertNoPlanTaskCombinedCycle(postTasks, 'blockedBy');
  const staged: StagedChange = { explicitStatus: new Map(), derivedNodes: new Set() };
  maybeFlipParentDerived(index, parentTaskId, staged);

  const structuralOps: PlanTaskMutationOp[] = created.map(task => ({ kind: 'create', task }));
  const { ops, deltas } = buildCommitOps(postTasks, structuralOps, staged);
  await port.storage.mutatePlanTasksForSession({ fence: fence(port), ops });
  refreshSummary(port, postTasks, deltas);
  const ids = created.map(t => t.taskId);
  emit(port, 'decompose', [parentTaskId, ...ids], deltas);
  return created.map((task, index) => ({
    ...toView(task),
    ...(children[index]?.localKey !== undefined ? { decompositionLocalKey: children[index].localKey } : {}),
  }));
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
  const delegatedOwner = delegatedOwnerOf(index, taskId);
  if (delegatedOwner !== undefined || subtreeContainsDelegation(index, taskId)) {
    throw new HarnessValidationError(
      'taskId',
      `task "${taskId}" belongs to or contains active delegated work; its structure is child-owned until settlement`,
    );
  }
  if (newParentTaskId !== null) {
    if (!index.byId.has(newParentTaskId)) {
      throw new HarnessValidationError('newParentTaskId', `unknown parent task "${newParentTaskId}"`);
    }
    // Cycle prevention: the new parent must not be the task itself or a descendant.
    assertNoParentCycle(index, taskId, newParentTaskId);
    assertParentAcceptsChildren(index, newParentTaskId);
    assertParentStructureMutable(index, newParentTaskId);
  }

  const oldParentId = task.parentTaskId;
  const moved: HarnessPlanTask = {
    ...task,
    ...(newParentTaskId !== null ? { parentTaskId: newParentTaskId } : {}),
    order: order ?? task.order,
  };
  if (newParentTaskId === null) delete moved.parentTaskId;
  const postTasks = tasks.map(t => (t.taskId === taskId ? moved : t));
  assertNoPlanTaskCombinedCycle(postTasks, 'parent');

  // Re-run the per-root single-in_progress invariant on the POST-move tree: a
  // move can merge two roots that each held their own explicit in_progress into
  // one root with two. Build the index over the post-image so `rootOf` resolves
  // through the new edge, and only count EXPLICIT in_progress (a derived rollup
  // in_progress just mirrors an explicit child — see assertSingleInProgress).
  const postIndex = indexPlanTasks(postTasks);
  for (const candidate of postTasks) {
    if (candidate.status !== 'in_progress' || candidate.statusSource !== 'explicit') continue;
    if (candidate.delegatedSubagentSessionId !== undefined) continue;
    assertSingleInProgress(
      postIndex,
      candidate.taskId,
      id => postIndex.byId.get(id)?.status ?? 'pending',
      id => postIndex.byId.get(id)?.statusSource ?? 'explicit',
      id => postIndex.byId.get(id)?.delegatedSubagentSessionId !== undefined,
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
  if (patch.content !== undefined) {
    assertBoundedPlanText('content', patch.content, PLAN_TASK_CONTENT_MAX_BYTES, true);
  }
  if (patch.activeForm !== undefined) {
    assertBoundedPlanText('activeForm', patch.activeForm, PLAN_TASK_ACTIVE_FORM_MAX_BYTES);
  }
  assertBoundedDependencies('blockedBy', patch.blockedBy);

  const childIds = index.childrenByParent.get(taskId) ?? [];
  if (patch.status !== undefined && childIds.length > 0) {
    throw new HarnessValidationError(
      'status',
      `task "${taskId}" has children and its status is owned by hierarchy rollup`,
    );
  }
  const delegatedOwner = delegatedOwnerOf(index, taskId);
  if (
    delegatedOwner !== undefined &&
    (patch.status !== undefined ||
      patch.content !== undefined ||
      patch.priority !== undefined ||
      patch.activeForm !== undefined ||
      patch.blockedBy !== undefined)
  ) {
    const field =
      patch.status !== undefined
        ? 'status'
        : patch.content !== undefined
          ? 'content'
          : patch.priority !== undefined
            ? 'priority'
            : patch.activeForm !== undefined
              ? 'activeForm'
              : 'blockedBy';
    throw new HarnessValidationError(
      field,
      `task "${taskId}" belongs to delegated task "${delegatedOwner.taskId}"; execution state is child-owned and its assignment is immutable until settlement`,
    );
  }

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
  if (patch.blockedBy !== undefined) assertNoPlanTaskCombinedCycle(postTasks, 'blockedBy');

  const staged: StagedChange = { explicitStatus: new Map(), derivedNodes: new Set() };
  if (patch.status !== undefined) {
    if (patch.status === 'in_progress') {
      if (hasUnsatisfiedDep(updated, id => index.byId.get(id)?.status ?? 'pending')) {
        throw new HarnessValidationError(
          'status',
          `task "${taskId}" has unsatisfied blockedBy dependencies and cannot start`,
        );
      }
      assertSingleInProgress(
        index,
        taskId,
        id => (id === taskId ? 'in_progress' : (index.byId.get(id)?.status ?? 'pending')),
        id => index.byId.get(id)?.statusSource ?? 'explicit',
        id => index.byId.get(id)?.delegatedSubagentSessionId !== undefined,
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
  const committedDelta = deltas.find(delta => delta.taskId === taskId);
  return toView(
    committedDelta === undefined
      ? updated
      : {
          ...updated,
          status: committedDelta.status,
          statusSource: committedDelta.statusSource,
        },
  );
}

export async function planTaskComplete(port: PlanTaskSessionPort, taskId: string): Promise<PlanTaskView> {
  return planTaskUpdate(port, taskId, { status: 'completed' });
}

/**
 * Result of an application-authored plan reset. A conversation edit/regenerate
 * can discard the branch that produced the current plan, so callers need both
 * the number of removed rows and the durable child-session links that must be
 * cancelled before replacement work is admitted.
 */
export interface ResetPlanTasksResult {
  deletedCount: number;
  delegatedSubagentSessionIds: string[];
}

/**
 * Atomically clear the complete plan forest for this session.
 *
 * This is deliberately NOT model-facing: a normal agent turn should update its
 * plan through the bounded task tools. Product orchestration uses reset when a
 * user edits, regenerates, or deletes conversation history and the old plan is
 * therefore no longer grounded in the authoritative message branch.
 *
 * Natural roots are deleted first. Corrupt parent cycles have no natural root,
 * so we additionally pick one representative from every still-unvisited
 * component; storage's cycle-safe subtree deletion then removes that component.
 * All operations commit under one session fence, preventing a partially-cleared
 * forest from becoming visible.
 */
export async function planTaskReset(port: PlanTaskSessionPort): Promise<ResetPlanTasksResult> {
  const tasks = await loadAllTasks(port);
  const delegatedSubagentSessionIds = Array.from(
    new Set(
      tasks
        .map(task => task.delegatedSubagentSessionId)
        .filter((sessionId): sessionId is string => sessionId !== undefined),
    ),
  );

  if (tasks.length === 0) {
    port.setPlanTaskSummary?.(computePlanTaskSummary([]));
    return { deletedCount: 0, delegatedSubagentSessionIds };
  }

  const byId = new Set(tasks.map(task => task.taskId));
  const childrenByParent = new Map<string, string[]>();
  for (const task of tasks) {
    if (task.parentTaskId === undefined) continue;
    const children = childrenByParent.get(task.parentTaskId) ?? [];
    children.push(task.taskId);
    childrenByParent.set(task.parentTaskId, children);
  }

  const visited = new Set<string>();
  const deletionRoots: string[] = [];
  const markComponent = (rootTaskId: string) => {
    const queue = [rootTaskId];
    while (queue.length > 0) {
      const taskId = queue.shift()!;
      if (visited.has(taskId)) continue;
      visited.add(taskId);
      for (const childTaskId of childrenByParent.get(taskId) ?? []) {
        if (!visited.has(childTaskId)) queue.push(childTaskId);
      }
    }
  };

  for (const task of tasks) {
    if (task.parentTaskId !== undefined && byId.has(task.parentTaskId)) continue;
    deletionRoots.push(task.taskId);
    markComponent(task.taskId);
  }
  // Defensive coverage for a corrupt component consisting entirely of a
  // parentTaskId cycle. Healthy trees never enter this branch.
  for (const task of tasks) {
    if (visited.has(task.taskId)) continue;
    deletionRoots.push(task.taskId);
    markComponent(task.taskId);
  }

  await port.storage.mutatePlanTasksForSession({
    fence: fence(port),
    ops: deletionRoots.map(rootTaskId => ({ kind: 'deleteSubtree' as const, rootTaskId })),
  });
  port.setPlanTaskSummary?.(computePlanTaskSummary([]));
  // A reset may run while the session is idle, where custom events are
  // intentionally turn-gated. The durable empty tree + display summary remain
  // authoritative; a live in-turn reset also gets the incremental reset hint.
  emitBestEffort(port, 'reset', deletionRoots, []);
  return { deletedCount: tasks.length, delegatedSubagentSessionIds };
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
 *   - `'blocked'`   → the plan task becomes `blocked` (external service/input/dependency),
 *   - `'failed'`    → the plan task becomes `failed` (covers subagent error,
 *     abort/cancel, and a fail-closed "subagent session gone/deleted").
 */
export type DelegatedSubagentOutcome = 'completed' | 'blocked' | 'failed';

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
  /**
   * Delegate the complete descendant set as one child-owned execution unit.
   * Required for non-leaf tasks so one delegated attempt cannot settle a
   * hierarchy it did not receive.
   */
  includeSubtree?: boolean;
  /** Exact immutable signal body admitted for this delegation attempt. */
  taskBody?: string;
  /** Internal original start timestamp. Omitted callers receive `Date.now()`. */
  startedAt?: number;
  /** Internal absolute attempt deadline. Omitted callers receive the bounded default. */
  deadlineAt?: number;
  /** Fingerprint captured before child allocation; rejects a stale scope commit. */
  expectedScopeFingerprint?: string;
  /** Internal correlation to the exact parent assistant tool call. */
  parentToolCallId?: string;
  /** Internal correlation to the parent provider run. */
  parentRunId?: string;
  /**
   * Exact JSON app-key subset approved for the delegated child turn. Kept out
   * of the model-facing plan projection, but persisted so restart reattachment
   * reproduces the original signal admission byte-for-byte.
   */
  requestContextApp?: Record<string, JsonValue>;
}

export interface PlanTaskDelegationScopeSnapshot {
  view: PlanTaskView;
  tasks: HarnessPlanTask[];
  fingerprint: string;
}

const DELEGATION_ATTEMPT_METADATA_KEY = 'mastraHarnessDelegationAttemptV1';

interface DelegationAttemptMetadata {
  attemptId: string;
  taskBody: string;
  taskBodySha256: string;
  requestContextApp?: Record<string, JsonValue>;
  requestContextAppSha256?: string;
  subagentSessionId: string;
  parentToolCallId: string;
  parentRunId?: string;
  includeSubtree: boolean;
  startedAt: number;
  deadlineAt: number;
  hadPriorMetadata: boolean;
  priorMetadata?: JsonValue;
  settlement?: {
    outcome: DelegatedSubagentOutcome;
    settledAt: number;
    result?: HarnessSubagentResultSummary;
  };
}

function sha256Text(value: string): string {
  return createHash('sha256').update(value, 'utf8').digest('hex');
}

function delegationAttemptMetadata(
  task: HarnessPlanTask,
  input: DelegateTaskInput,
  taskBody: string,
  startedAt: number,
  deadlineAt: number,
): JsonValue {
  const previousAttempt = readDelegationAttemptMetadata(task);
  const priorMetadata =
    previousAttempt === undefined
      ? task.metadata
      : previousAttempt.hadPriorMetadata
        ? previousAttempt.priorMetadata
        : undefined;
  const attempt: Record<string, JsonValue> = {
    attemptId: `delegation-${sha256Text(
      JSON.stringify([task.sessionId, task.taskId, input.subagentSessionId, startedAt]),
    ).slice(0, 32)}`,
    taskBody,
    taskBodySha256: sha256Text(taskBody),
    subagentSessionId: input.subagentSessionId,
    parentToolCallId: input.parentToolCallId ?? `delegate:${task.taskId}`,
    includeSubtree: input.includeSubtree === true,
    startedAt,
    deadlineAt,
    hadPriorMetadata: priorMetadata !== undefined,
  };
  if (input.requestContextApp !== undefined) {
    const requestContextApp = assertJsonValue(input.requestContextApp, 'requestContextApp');
    if (requestContextApp === null || typeof requestContextApp !== 'object' || Array.isArray(requestContextApp)) {
      throw new HarnessValidationError('requestContextApp', 'must be a JSON object');
    }
    attempt.requestContextApp = requestContextApp;
    attempt.requestContextAppSha256 = sha256CanonicalJsonChecked(requestContextApp);
  }
  if (input.parentRunId !== undefined) attempt.parentRunId = input.parentRunId;
  if (priorMetadata !== undefined) attempt.priorMetadata = priorMetadata;
  return { [DELEGATION_ATTEMPT_METADATA_KEY]: attempt };
}

function settledDelegationAttemptMetadata(
  attempt: DelegationAttemptMetadata,
  outcome: DelegatedSubagentOutcome,
  result: HarnessSubagentResultSummary | undefined,
): JsonValue {
  const settlement: Record<string, JsonValue> = { outcome, settledAt: Date.now() };
  if (result !== undefined) settlement.result = JSON.parse(JSON.stringify(result)) as JsonValue;
  const encoded: Record<string, JsonValue> = {
    attemptId: attempt.attemptId,
    taskBody: attempt.taskBody,
    taskBodySha256: attempt.taskBodySha256,
    subagentSessionId: attempt.subagentSessionId,
    parentToolCallId: attempt.parentToolCallId,
    includeSubtree: attempt.includeSubtree,
    startedAt: attempt.startedAt,
    deadlineAt: attempt.deadlineAt,
    hadPriorMetadata: attempt.hadPriorMetadata,
    settlement,
  };
  if (attempt.parentRunId !== undefined) encoded.parentRunId = attempt.parentRunId;
  if (attempt.hadPriorMetadata) encoded.priorMetadata = attempt.priorMetadata!;
  return { [DELEGATION_ATTEMPT_METADATA_KEY]: encoded };
}

/**
 * Read and integrity-check the immutable request persisted with a delegation
 * link. Recovery uses these exact bytes instead of reconstructing a prompt from
 * mutable task labels or a fresh subtree rendering.
 */
export function readDelegationAttemptMetadata(
  task: HarnessPlanTask,
  expectedSubagentSessionId?: string,
): DelegationAttemptMetadata | undefined {
  const envelope = task.metadata;
  if (envelope === null || typeof envelope !== 'object' || Array.isArray(envelope)) return undefined;
  const raw = envelope[DELEGATION_ATTEMPT_METADATA_KEY];
  if (raw === null || typeof raw !== 'object' || Array.isArray(raw)) return undefined;
  if (
    typeof raw.attemptId !== 'string' ||
    raw.attemptId.length === 0 ||
    typeof raw.taskBody !== 'string' ||
    typeof raw.taskBodySha256 !== 'string' ||
    typeof raw.subagentSessionId !== 'string' ||
    typeof raw.parentToolCallId !== 'string' ||
    raw.parentToolCallId.length === 0 ||
    (raw.parentRunId !== undefined && (typeof raw.parentRunId !== 'string' || raw.parentRunId.length === 0)) ||
    typeof raw.includeSubtree !== 'boolean' ||
    typeof raw.startedAt !== 'number' ||
    !Number.isSafeInteger(raw.startedAt) ||
    typeof raw.deadlineAt !== 'number' ||
    !Number.isSafeInteger(raw.deadlineAt) ||
    raw.deadlineAt <= raw.startedAt ||
    typeof raw.hadPriorMetadata !== 'boolean' ||
    (expectedSubagentSessionId !== undefined && raw.subagentSessionId !== expectedSubagentSessionId) ||
    sha256Text(raw.taskBody) !== raw.taskBodySha256
  ) {
    return undefined;
  }
  if (raw.hadPriorMetadata && !Object.prototype.hasOwnProperty.call(raw, 'priorMetadata')) return undefined;
  const hasRequestContextApp = Object.prototype.hasOwnProperty.call(raw, 'requestContextApp');
  const hasRequestContextAppSha256 = Object.prototype.hasOwnProperty.call(raw, 'requestContextAppSha256');
  if (hasRequestContextApp !== hasRequestContextAppSha256) return undefined;
  let requestContextApp: Record<string, JsonValue> | undefined;
  if (hasRequestContextApp) {
    if (
      raw.requestContextApp === null ||
      typeof raw.requestContextApp !== 'object' ||
      Array.isArray(raw.requestContextApp) ||
      typeof raw.requestContextAppSha256 !== 'string'
    ) {
      return undefined;
    }
    try {
      const normalized = assertJsonValue(raw.requestContextApp, 'delegation.requestContextApp');
      if (
        normalized === null ||
        typeof normalized !== 'object' ||
        Array.isArray(normalized) ||
        sha256CanonicalJsonChecked(normalized) !== raw.requestContextAppSha256
      ) {
        return undefined;
      }
      requestContextApp = normalized;
    } catch {
      return undefined;
    }
  }
  let settlement: DelegationAttemptMetadata['settlement'];
  if (raw.settlement !== undefined) {
    if (raw.settlement === null || typeof raw.settlement !== 'object' || Array.isArray(raw.settlement))
      return undefined;
    if (
      (raw.settlement.outcome !== 'completed' &&
        raw.settlement.outcome !== 'blocked' &&
        raw.settlement.outcome !== 'failed') ||
      typeof raw.settlement.settledAt !== 'number' ||
      !Number.isFinite(raw.settlement.settledAt)
    ) {
      return undefined;
    }
    const parsedResult =
      raw.settlement.result === undefined
        ? undefined
        : harnessSubagentResultSummarySchema.safeParse(raw.settlement.result);
    if (parsedResult !== undefined && !parsedResult.success) return undefined;
    settlement = {
      outcome: raw.settlement.outcome,
      settledAt: raw.settlement.settledAt,
      ...(parsedResult?.success ? { result: parsedResult.data } : {}),
    };
  }
  return {
    attemptId: raw.attemptId,
    taskBody: raw.taskBody,
    taskBodySha256: raw.taskBodySha256,
    ...(requestContextApp !== undefined
      ? { requestContextApp, requestContextAppSha256: raw.requestContextAppSha256 as string }
      : {}),
    subagentSessionId: raw.subagentSessionId,
    parentToolCallId: raw.parentToolCallId,
    ...(typeof raw.parentRunId === 'string' ? { parentRunId: raw.parentRunId } : {}),
    includeSubtree: raw.includeSubtree,
    startedAt: raw.startedAt,
    deadlineAt: raw.deadlineAt,
    hadPriorMetadata: raw.hadPriorMetadata,
    ...(raw.hadPriorMetadata ? { priorMetadata: raw.priorMetadata as JsonValue } : {}),
    ...(settlement !== undefined ? { settlement } : {}),
  };
}

function assertDelegationScopeEligible(
  index: ReturnType<typeof indexPlanTasks>,
  task: HarnessPlanTask,
  includeSubtree: boolean,
): void {
  if (task.delegatedSubagentSessionId !== undefined) {
    throw new HarnessValidationError(
      'taskId',
      `task "${task.taskId}" is already delegated to active subagent session "${task.delegatedSubagentSessionId}"`,
    );
  }

  const ancestorOwner = task.parentTaskId === undefined ? undefined : delegatedOwnerOf(index, task.parentTaskId);
  if (ancestorOwner !== undefined) {
    throw new HarnessValidationError(
      'taskId',
      `task "${task.taskId}" belongs to delegated subtree "${ancestorOwner.taskId}" and cannot be delegated separately`,
    );
  }

  const childIds = index.childrenByParent.get(task.taskId) ?? [];
  if (childIds.length > 0 && !includeSubtree) {
    throw new HarnessValidationError(
      'includeSubtree',
      `task "${task.taskId}" has descendants; non-leaf delegation requires includeSubtree: true`,
    );
  }

  if (!includeSubtree) return;
  const scopeTaskIds = subtreeTaskIds(index, task.taskId);
  const scopeSet = new Set(scopeTaskIds);
  for (const descendantId of scopeTaskIds.slice(1)) {
    const descendant = index.byId.get(descendantId);
    if (descendant?.delegatedSubagentSessionId === undefined) continue;
    throw new HarnessValidationError(
      'taskId',
      `task "${task.taskId}" contains active delegated descendant "${descendantId}"`,
    );
  }

  // The child receives and owns the complete selected unit. A dependency that
  // points outside it must already have succeeded; otherwise success could mark
  // an internally blocked descendant complete while its prerequisite is still
  // pending in the parent plan.
  for (const scopeTaskId of scopeTaskIds) {
    const scopeTask = index.byId.get(scopeTaskId)!;
    for (const dependencyId of scopeTask.blockedBy ?? []) {
      if (scopeSet.has(dependencyId)) continue;
      const dependencyStatus = index.byId.get(dependencyId)?.status;
      if (dependencyStatus === 'completed') continue;
      throw new HarnessValidationError(
        'taskId',
        `delegated subtree "${task.taskId}" has descendant "${scopeTaskId}" blocked by external task "${dependencyId}" that has not completed successfully`,
      );
    }
  }
}

function delegationScopeFingerprint(
  index: ReturnType<typeof indexPlanTasks>,
  taskId: string,
  includeSubtree: boolean,
): string {
  const scopeIds = includeSubtree ? subtreeTaskIds(index, taskId) : [taskId];
  const scopeSet = new Set(scopeIds);
  const scope = scopeIds
    .map(id => index.byId.get(id))
    .filter((task): task is HarnessPlanTask => task !== undefined)
    .map(task => ({
      taskId: task.taskId,
      version: task.version,
      parentTaskId: task.parentTaskId ?? null,
      order: task.order,
      status: task.status,
      statusSource: task.statusSource,
      content: task.content,
      activeForm: task.activeForm ?? null,
      priority: task.priority ?? null,
      blockedBy: task.blockedBy ?? [],
      delegatedSubagentSessionId: task.delegatedSubagentSessionId ?? null,
    }))
    .sort((a, b) => a.taskId.localeCompare(b.taskId));
  const externalDependencies = Array.from(
    new Set(scope.flatMap(task => task.blockedBy).filter(dependencyId => !scopeSet.has(dependencyId))),
  )
    .map(dependencyId => ({
      taskId: dependencyId,
      status: index.byId.get(dependencyId)?.status ?? null,
    }))
    .sort((a, b) => a.taskId.localeCompare(b.taskId));
  return sha256Text(JSON.stringify({ includeSubtree, scope, externalDependencies }));
}

/** Capture the exact execution scope before allocating a durable child. */
export async function capturePlanTaskDelegationScope(
  port: PlanTaskSessionPort,
  taskId: string,
  includeSubtree = false,
): Promise<PlanTaskDelegationScopeSnapshot> {
  const tasks = await loadAllTasks(port);
  const index = indexPlanTasks(tasks);
  const task = index.byId.get(taskId);
  if (!task) throw new HarnessValidationError('taskId', `unknown task "${taskId}"`);
  if (hasUnsatisfiedDep(task, id => index.byId.get(id)?.status ?? 'pending')) {
    throw new HarnessValidationError(
      'taskId',
      `task "${taskId}" has unsatisfied blockedBy dependencies and cannot start`,
    );
  }
  assertSingleInProgress(
    index,
    taskId,
    id => index.byId.get(id)?.status ?? 'pending',
    id => index.byId.get(id)?.statusSource ?? 'explicit',
    id => index.byId.get(id)?.delegatedSubagentSessionId !== undefined,
  );
  assertDelegationScopeEligible(index, task, includeSubtree);
  const scopeIds = includeSubtree ? subtreeTaskIds(index, taskId) : [taskId];
  return {
    view: toView(task),
    tasks: scopeIds.map(id => index.byId.get(id)!).filter(Boolean),
    fingerprint: delegationScopeFingerprint(index, taskId, includeSubtree),
  };
}

/**
 * Preflight a delegation before the caller reserves capacity or creates a child
 * session. The commit path repeats these checks against its serialized
 * post-read, so this is an optimization and a clear early error rather than a
 * substitute for the fenced write.
 */
export async function assertPlanTaskDelegatable(
  port: PlanTaskSessionPort,
  taskId: string,
  includeSubtree = false,
): Promise<PlanTaskView> {
  return (await capturePlanTaskDelegationScope(port, taskId, includeSubtree)).view;
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
  if (hasUnsatisfiedDep(task, id => index.byId.get(id)?.status ?? 'pending')) {
    throw new HarnessValidationError(
      'taskId',
      `task "${input.taskId}" has unsatisfied blockedBy dependencies and cannot start`,
    );
  }

  // Delegation is bounded BACKGROUND execution, not a second parent-agent
  // foreground focus. Existing delegated siblings are skipped, while any
  // foreground task in this root remains an eligibility conflict.
  assertSingleInProgress(
    index,
    input.taskId,
    id => index.byId.get(id)?.status ?? 'pending',
    id => index.byId.get(id)?.statusSource ?? 'explicit',
    id => index.byId.get(id)?.delegatedSubagentSessionId !== undefined,
  );
  assertDelegationScopeEligible(index, task, input.includeSubtree === true);
  if (
    input.expectedScopeFingerprint !== undefined &&
    delegationScopeFingerprint(index, input.taskId, input.includeSubtree === true) !== input.expectedScopeFingerprint
  ) {
    throw new HarnessValidationError(
      'taskId',
      `task "${input.taskId}" changed while its delegated child was being allocated; retry with a fresh scope`,
    );
  }

  const taskBody = input.taskBody ?? task.content;
  assertBoundedPlanText('taskBody', taskBody, PLAN_TASK_DELEGATED_BODY_MAX_BYTES, true);
  const startedAt = input.startedAt ?? Date.now();
  const deadlineAt = input.deadlineAt ?? startedAt + DEFAULT_PLAN_TASK_DELEGATION_TIMEOUT_MS;
  if (!Number.isSafeInteger(startedAt) || startedAt < 0) {
    throw new HarnessValidationError('startedAt', 'must be a non-negative safe integer timestamp');
  }
  if (!Number.isSafeInteger(deadlineAt) || deadlineAt <= startedAt) {
    throw new HarnessValidationError('deadlineAt', 'must be a safe integer timestamp later than startedAt');
  }
  const metadata = delegationAttemptMetadata(task, input, taskBody, startedAt, deadlineAt);

  const updated: HarnessPlanTask = {
    ...task,
    status: 'in_progress',
    statusSource: 'explicit',
    delegatedSubagentSessionId: input.subagentSessionId,
    ...(input.subagentTypeId !== undefined ? { delegatedSubagentTypeId: input.subagentTypeId } : {}),
    metadata,
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
        metadata,
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
  /** Bounded sanitized terminal report retained for parent synthesis. */
  result?: HarnessSubagentResultSummary;
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
  // A duplicate terminal callback after the exact link was cleared returns the
  // canonical terminal post-image without writing again. An active different
  // link is still a stale callback and must not expose or clobber that attempt.
  if (task.delegatedSubagentSessionId !== input.subagentSessionId) {
    if (
      task.delegatedSubagentSessionId === undefined &&
      task.statusSource === 'explicit' &&
      TERMINAL_PLAN_TASK_STATUSES.has(task.status)
    ) {
      return { reconciled: false, view: toView(task) };
    }
    return { reconciled: false };
  }
  const nextStatus: HarnessPlanTaskStatus = input.outcome;
  const attemptMetadata = readDelegationAttemptMetadata(task, input.subagentSessionId);
  const settledMetadata =
    attemptMetadata === undefined
      ? undefined
      : settledDelegationAttemptMetadata(attemptMetadata, input.outcome, input.result);
  const settledTaskIds = subtreeTaskIds(index, input.taskId);

  // Overlapping delegation scopes are rejected at both preflight and commit.
  // Fail closed if older/corrupt data still contains a separately-owned child;
  // this attempt must never settle another subagent's task.
  for (const descendantId of settledTaskIds.slice(1)) {
    const descendant = index.byId.get(descendantId);
    if (descendant?.delegatedSubagentSessionId === undefined) continue;
    throw new HarnessValidationError(
      'taskId',
      `delegated subtree "${input.taskId}" contains separately delegated descendant "${descendantId}"`,
    );
  }

  const settledSet = new Set(settledTaskIds);
  const postTasks = tasks.map(row => {
    if (!settledSet.has(row.taskId)) return row;
    // Preserve already-successful/skipped descendants. Failed descendants are
    // retryable and therefore become completed after a successful reattempt.
    const preserveTerminalOk = row.status === 'completed' || row.status === 'cancelled';
    const updated: HarnessPlanTask = preserveTerminalOk
      ? { ...row }
      : { ...row, status: nextStatus, statusSource: 'explicit' };
    if (row.taskId === input.taskId) {
      delete updated.delegatedSubagentSessionId;
      delete updated.delegatedSubagentTypeId;
      if (settledMetadata === undefined) delete updated.metadata;
      else updated.metadata = settledMetadata;
    }
    return updated;
  });
  const updated = postTasks.find(row => row.taskId === input.taskId)!;

  const staged: StagedChange = { explicitStatus: new Map(), derivedNodes: new Set() };
  for (const settledTaskId of settledTaskIds) {
    const row = index.byId.get(settledTaskId);
    if (row?.status === 'completed' || row?.status === 'cancelled') continue;
    staged.explicitStatus.set(settledTaskId, nextStatus);
  }

  const { ops, deltas } = buildCommitOps(
    postTasks,
    [
      {
        kind: 'update',
        taskId: input.taskId,
        ifVersion: task.version,
        patch: {
          clearDelegatedSubagentSessionId: true,
          clearDelegatedSubagentTypeId: true,
          ...(settledMetadata !== undefined ? { metadata: settledMetadata } : { clearMetadata: true }),
        },
      },
    ],
    staged,
  );
  await port.storage.mutatePlanTasksForSession({ fence: fence(port), ops });
  refreshSummary(port, postTasks, deltas);
  emitBestEffort(port, 'delegate_settled', settledTaskIds, deltas);
  return { reconciled: true, view: toView(updated) };
}

export interface CheckInput {
  rootTaskId?: string;
  depth?: number;
  status?: HarnessPlanTaskStatus;
  limit?: number;
  /** Opaque continuation returned by the previous check with the same filters. */
  cursor?: string;
}

/** Default page size for the bounded read. */
export const PLAN_TASK_CHECK_DEFAULT_LIMIT = 25;
/** Hard cap on the bounded read so a hostile/large `limit` can never pull the
 * whole tree and defeat the anti-forgetting purpose (§5.1k). */
export const PLAN_TASK_CHECK_MAX_LIMIT = PLAN_TASK_MAX_NODES;
/** Hard serialized model-output budget for one plan_task_check result. */
export const PLAN_TASK_CHECK_MAX_OUTPUT_BYTES = 64 * 1024;

export interface PlanTaskCheckResult {
  tasks: PlanTaskView[];
  truncated: boolean;
  /** Present when another page exists for the same filter set. */
  nextCursor?: string;
}

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
export async function planTaskCheck(port: PlanTaskSessionPort, input: CheckInput): Promise<PlanTaskCheckResult> {
  const limit = clampPlanTaskCheckLimit(input.limit);
  // Read the complete transaction-bounded plan (at most 100 nodes) so the
  // model-facing cursor is stable across byte-budgeted pages. The serialized
  // tool result below remains independently capped.
  const read: Promise<LoadPlanTaskSubtreeResult> = port.storage.loadPlanTaskSubtree({
    harnessName: port.harnessName,
    sessionId: port.id,
    rootTaskId: input.rootTaskId,
    depth: input.depth,
    status: input.status,
    limit: PLAN_TASK_CHECK_MAX_LIMIT,
  });
  const rootRead =
    input.rootTaskId === undefined
      ? undefined
      : port.storage.loadPlanTaskSubtree({
          harnessName: port.harnessName,
          sessionId: port.id,
          rootTaskId: input.rootTaskId,
          depth: 0,
          limit: 1,
        });
  const [result, root] = await Promise.all([read, rootRead]);
  if (input.rootTaskId !== undefined && root?.tasks[0]?.taskId !== input.rootTaskId) {
    throw new HarnessValidationError('rootTaskId', `unknown task "${input.rootTaskId}"`);
  }
  const views = result.tasks.map(toView);
  let start = 0;
  if (input.cursor !== undefined) {
    const cursorIndex = views.findIndex(task => task.taskId === input.cursor);
    if (cursorIndex < 0) {
      throw new HarnessValidationError('cursor', 'cursor does not belong to this plan-task check result');
    }
    start = cursorIndex + 1;
  }

  const candidates = views.slice(start, start + limit);
  const tasks: PlanTaskView[] = [];
  for (const candidate of candidates) {
    const proposed = [...tasks, candidate];
    // Measure the largest envelope this page could return. Using `truncated:
    // true` plus a cursor guarantees the eventual (possibly final) envelope is
    // no larger than the admitted candidate measurement.
    const projectedBytes = utf8Bytes(
      JSON.stringify({ tasks: proposed, truncated: true, nextCursor: candidate.taskId }),
    );
    if (projectedBytes > PLAN_TASK_CHECK_MAX_OUTPUT_BYTES) break;
    tasks.push(candidate);
  }
  if (tasks.length === 0 && candidates.length > 0) {
    throw new HarnessValidationError(
      'plan_task_check',
      `one plan task exceeds the ${PLAN_TASK_CHECK_MAX_OUTPUT_BYTES}-byte check response budget`,
    );
  }

  const hasKnownNextPage = start + tasks.length < views.length;
  const truncated = result.truncated || hasKnownNextPage;
  const nextCursor = hasKnownNextPage ? tasks.at(-1)?.taskId : undefined;
  return {
    tasks,
    truncated,
    ...(nextCursor !== undefined ? { nextCursor } : {}),
  };
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
