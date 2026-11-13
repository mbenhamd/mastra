import { randomUUID } from 'node:crypto';
import type { HistoryLoadWatch } from '../../memory/history-load-watch';
import type { TracingContext } from '../../observability';
import type { RequestContext } from '../../request-context';
import { HARNESS_RUN_INTERRUPTED_ERROR_CODE } from '../../storage/domains/harness';
import type {
  AgentSignalResultEvidence,
  HarnessPlanTask,
  HarnessStorage,
  OperationAdmissionTombstone,
  SessionRecord,
} from '../../storage/domains/harness';
import type { ObservationalMemorySourceWriteGuard } from '../../storage/types';
import { sha256CanonicalJsonChecked as sha256CanonicalJson } from './canonical-json';
import {
  HarnessSubagentConcurrencyLimitError,
  HarnessSubagentDepthExceededError,
  HarnessValidationError,
} from './errors';
import { projectHarnessPublicError } from './events';
import type {
  EmitInput,
  EventEmitter,
  HarnessEvent,
  SubagentAnswerDeltaEvent,
  SubagentEndEvent,
  SubagentReasoningDeltaEvent,
  SubagentStartEvent,
  SubagentTextDeltaEvent,
  SubagentToolEndEvent,
  SubagentToolStartEvent,
} from './events';
import type { Harness } from './harness';
import { TERMINAL_PLAN_TASK_STATUSES } from './plan-task-hierarchy';
import {
  capturePlanTaskDelegationScope,
  PLAN_TASK_DELEGATED_BODY_MAX_BYTES,
  PLAN_TASK_MAX_NODES,
  planTaskDelegate,
  planTaskReconcileDelegation,
  readDelegationAttemptMetadata,
  readPendingDelegationSettlementNotice,
} from './plan-task-session';
import type {
  DelegatedSettlementNoticeMode,
  DelegatedSubagentOutcome,
  PendingDelegationSettlementNotice,
  PlanTaskSessionPort,
  PlanTaskView,
} from './plan-task-session';
import { projectInheritedRequestContextApp } from './request-context-input';
import type {
  ActiveSubagentState,
  JsonValue,
  Session,
  SessionLifecycleState,
  SignalAdmissionIdentity,
} from './session';
import type { QueueHead } from './session-queue-head';
import { harnessSubagentBudgetExhaustedError } from './subagent-budget';
import type { HarnessSubagentBudgetExhaustion } from './subagent-budget';
import { endDelegatedSubagentSpan, startDelegatedSubagentSpan } from './subagent-tracing';
import type { DelegationDriveSettlement } from './subagent-tracing';
import {
  HARNESS_SUBAGENT_OUTCOME_REPORT_TOOL_ID,
  parseHarnessSubagentBudgetExhausted,
  parseHarnessSubagentOutcomeReport,
  parseHarnessTerminalToolResultText,
  summarizeHarnessSubagentEventResult,
  summarizeHarnessSubagentResult,
  verifyHarnessSubagentTerminalCompletion,
} from './terminal-subagent-result';
import type { HarnessSubagentResultSummary } from './terminal-subagent-result';
import type { SubagentDefinition } from './types';

/**
 * TM-6 `includeSubtree` transfers the complete valid plan. The plan writer
 * already enforces this bound, so a normal delegated child never receives a
 * partial execution unit that the parent would later settle as complete.
 */
export const DELEGATED_SUBTREE_MAX_NODES = PLAN_TASK_MAX_NODES;

export const DELEGATION_DEADLINE_REACHED = Symbol('delegation-deadline-reached');

/** Race one durable delegation result against its persisted absolute deadline. */
export async function waitForDelegationResult<T>(
  promise: Promise<T>,
  deadlineAt: number,
): Promise<T | typeof DELEGATION_DEADLINE_REACHED> {
  const remainingMs = Math.max(0, deadlineAt - Date.now());
  if (remainingMs === 0) return DELEGATION_DEADLINE_REACHED;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const deadline = new Promise<typeof DELEGATION_DEADLINE_REACHED>(resolve => {
    timer = setTimeout(() => resolve(DELEGATION_DEADLINE_REACHED), remainingMs);
    if (typeof timer.unref === 'function') timer.unref();
  });
  try {
    return await Promise.race([promise, deadline]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}

/** The public error of a delegated task its parent cancelled (`cancelled` outcome). */
export const DELEGATED_SUBAGENT_CANCELLED_ERROR = {
  code: 'harness.subagent_cancelled',
  message: 'The delegated subagent was cancelled before it completed',
} as const;

export function summarizeDelegationDeadline(deadlineAt: number): HarnessSubagentResultSummary {
  const summary = summarizeHarnessSubagentEventResult(
    summarizeHarnessSubagentResult(undefined, {
      isError: true,
      error: {
        code: 'harness.delegation_timeout',
        message: `The delegated subagent did not complete before its durable deadline (${deadlineAt})`,
      },
    }),
  );
  return { ...summary, outcome: 'failed' };
}

/** The deterministic admission id of a parent's `task_delegate` signal to its child. */
export function delegationAdmissionId(parentSessionId: string, taskId: string): string {
  return `harness-delegate:${parentSessionId}:${taskId}`;
}

/** The child's signal evidence id for one admitted delegation signal. */
export function delegationSignalId(child: SessionRecord, admissionId: string): string {
  const digest = sha256CanonicalJson({
    kind: 'channel-signal-admission',
    harnessName: child.harnessName,
    sessionId: child.id,
    resourceId: child.resourceId,
    threadId: child.threadId,
    admissionId,
  });
  return `harness-channel-signal-${digest.slice(0, 32)}`;
}

/**
 * Settled evidence recovery publishes (as message recovery does): a completed
 * result that was not erased, or an interruption. Other failures were
 * reported by the run that failed.
 */
export function owesRecoveredCompletion(
  evidence: AgentSignalResultEvidence,
): evidence is AgentSignalResultEvidence & { runId: string } {
  return (
    evidence.runId !== undefined &&
    ((evidence.status === 'completed' && evidence.erased !== true) ||
      (evidence.status === 'failed' && evidence.error.code === HARNESS_RUN_INTERRUPTED_ERROR_CODE))
  );
}

/** PF-7473: true when the caller's admission check now fails. */
export function admissionLost(assertAdmitted: () => void): boolean {
  try {
    assertAdmitted();
    return false;
  } catch {
    return true;
  }
}

/**
 * True when `parentSessionId` itself asked to stop exactly this child: the
 * `subagent` tool's cancel or `cancelSubagent`, which alone record the
 * harness-owned `targeted` marker. The reason is not consulted. A delegation
 * timeout, a cancel cascading down from the parent's own cancellation, and a
 * cancel from anyone else carry no marker, so those settle the delegated task
 * `failed`.
 */
export function isParentRequestedSubagentCancel(
  cancelRequest: SessionRecord['cancelRequest'],
  parentSessionId: string,
): boolean {
  return cancelRequest?.targeted === true && cancelRequest.requestedBy === parentSessionId;
}

/** TM-6 reconcile-on-rehydrate retry (transient/CAS/locked failures). */
export const DELEGATION_RECONCILE_RETRY_BASE_MS = 500;
export const DELEGATION_RECONCILE_RETRY_MAX_MS = 30_000;

/**
 * The slice of `Session` the delegation driver reads and calls. `Session`
 * builds it in `_delegationDriverPort()`. The driver's state stays on
 * `Session` and is reached through this port: the claim, settling and
 * listener sets, the reconcile timers and flags, the in-flight execution
 * count, and the notice watches and `remind` starts that settlement-notice
 * delivery also reads. Two driver members stay on `Session` (see
 * `_delegationDriverPort()`).
 */
export interface DelegationDriverSessionPort {
  readonly id: string;
  readonly resourceId: string;
  readonly threadId: string;
  readonly subagentDepth: number;
  readonly harness: Harness;
  readonly storage: HarnessStorage;
  readonly emitter: EventEmitter;
  readonly queueHead: QueueHead;
  readonly record: SessionRecord;
  readonly state: SessionLifecycleState;
  readonly activeSubagents: Map<string, ActiveSubagentState>;
  readonly activeWorkAdmissionEpoch: number;
  readonly delegatedSettlementListeners: Set<(subagentSessionId: string) => void>;
  readonly delegationDriverClaims: Set<string>;
  readonly delegationNoticeWatches: Map<string, HistoryLoadWatch>;
  delegationReconcileCapacityBlocked: boolean;
  delegationReconcileInFlight: Promise<void> | undefined;
  delegationReconcileRerunRequested: boolean;
  readonly delegationReconcileRetryTimers: Set<ReturnType<typeof setTimeout>>;
  readonly remindSettlementsStarting: Set<Promise<void>>;
  readonly settlingDelegations: Set<string>;
  subagentExecutionsInFlight: number;
  activeWorkAdmissionIsCurrent(epoch: number): boolean;
  assertActiveWorkAdmission(epoch: number, operation: string, signal?: AbortSignal): void;
  assertLive(method: string): void;
  assertNotDeleted(): void;
  captureActiveWorkAdmission(operation: string): number;
  delegationNoticeClaimable(notice: PendingDelegationSettlementNotice): boolean;
  disposeKeptDelegationNoticeWatches(drop: (key: string) => boolean): void;
  notifyDisplayRefresh(): void;
  notifyMaybeIdle(): void;
  planTaskPort(): PlanTaskSessionPort;
  publishRecoveredRunCompletion(evidence: AgentSignalResultEvidence): Promise<void>;
  serializePlanTaskWrite<T>(run: () => Promise<T>): Promise<T>;
  signalAdmissionIdentity(admissionId: string): SignalAdmissionIdentity;
  startDelegationSettlementNotice(
    notice: PendingDelegationSettlementNotice,
    admissionEpoch: number,
    retryAttempt: number,
  ): void;
  cancelOwnedSubagentSessions(
    subagentSessionIds: Iterable<string>,
    reason: string,
    assertAdmitted?: () => void,
    targeted?: boolean,
  ): Promise<{ cancelled: string[]; unavailable: string[] }>;
  driveDelegatedSubagent(
    taskId: string,
    child: Session,
    taskBody: string,
    agentType: string,
    modelId: string,
    startedAt: number,
    deadlineAt: number,
    parentToolCallId: string,
    requestContextApp?: Record<string, JsonValue>,
    parentTracingContext?: TracingContext,
  ): Promise<void>;
  cancelAndCloseDelegatedChildForDeadline(childRecord: SessionRecord): Promise<boolean>;
  listDelegatedSubagentSessionIds(): Promise<string[]>;
  delegationLinkIsCurrent(taskId: string, childId: string): Promise<boolean>;
  reconcileDelegationsOnHydratePass(attempt: number): Promise<void>;
  reconcileDelegationsOnHydrate(attempt?: number): Promise<void>;
  delegatingRunSourceWriteGuard: (
    callerRequestContext: RequestContext<unknown> | undefined,
  ) => ObservationalMemorySourceWriteGuard | undefined;
  readonly currentRunModeId: string | undefined;
}

export function emitSubagentEvent(
  port: DelegationDriverSessionPort,
  event:
    | Omit<SubagentStartEvent, 'id' | 'timestamp' | 'sessionId' | 'parentId'>
    | Omit<SubagentTextDeltaEvent, 'id' | 'timestamp' | 'sessionId' | 'parentId'>
    | Omit<SubagentReasoningDeltaEvent, 'id' | 'timestamp' | 'sessionId' | 'parentId'>
    | Omit<SubagentAnswerDeltaEvent, 'id' | 'timestamp' | 'sessionId' | 'parentId'>
    | Omit<SubagentToolStartEvent, 'id' | 'timestamp' | 'sessionId' | 'parentId'>
    | Omit<SubagentToolEndEvent, 'id' | 'timestamp' | 'sessionId' | 'parentId'>
    | Omit<SubagentEndEvent, 'id' | 'timestamp' | 'sessionId' | 'parentId'>,
): HarnessEvent {
  const stamped = { ...event, parentId: port.id } as EmitInput;
  if (port.queueHead.currentId !== undefined && (stamped as { queuedItemId?: string }).queuedItemId === undefined) {
    return port.emitter.emit({ ...stamped, queuedItemId: port.queueHead.currentId } as EmitInput);
  }
  return port.emitter.emit(stamped);
}

export function internalUpdateSubagentProgress(
  port: DelegationDriverSessionPort,
  key: string,
  event: HarnessEvent,
): void {
  const entry = port.activeSubagents.get(key);
  if (!entry) return;
  switch (event.type) {
    case 'agent_start':
      entry.status = 'running';
      break;
    case 'tool_approval_required':
    case 'tool_suspension_required':
    case 'question_pending':
    case 'plan_approval_required':
      entry.status = 'awaiting_input';
      break;
    case 'tool_start':
      entry.currentToolName = event.toolName;
      entry.toolCalls = (entry.toolCalls ?? 0) + 1;
      break;
    case 'tool_end':
      // Best-effort clear: only when the ending tool is the one we last showed
      // (concurrent inner tools just leave the most-recent name until it ends).
      if (entry.currentToolName === event.toolName) entry.currentToolName = undefined;
      break;
    case 'agent_end':
      entry.status =
        event.finishReason === 'error' || event.finishReason === 'aborted'
          ? 'failed'
          : event.finishReason === 'complete'
            ? 'completed'
            : entry.status;
      entry.usage = event.usage;
      break;
    default:
      return; // text/reasoning deltas don't change the projection
  }
  entry.updatedAt = Date.now();
  port.notifyDisplayRefresh();
}

/**
 * @internal — mirror a delegated child's pending interaction onto the
 * PARENT event stream. The owning inbox remains the child session; the
 * bridged event only supplies parent activity/replay consumers with the
 * correlation needed to render the wait and route the response correctly.
 */
export function emitDelegatedSubagentPendingEvent(
  port: DelegationDriverSessionPort,
  event: Extract<
    HarnessEvent,
    {
      type: 'tool_approval_required' | 'tool_suspension_required' | 'question_pending' | 'plan_approval_required';
    }
  >,
  correlation: { parentToolCallId: string; subagentSessionId: string },
): void {
  const { id: _id, timestamp: _timestamp, sessionId: _sessionId, source: _source, ...payload } = event;
  // SuspensionSource is a discriminated union: parent events do not declare
  // the child-only correlation keys, so strip them from a widened copy rather
  // than destructuring properties that are absent on one union member.
  const bridgedPayload = { ...payload } as Record<string, unknown>;
  delete bridgedPayload.subagentToolCallId;
  delete bridgedPayload.subagentSessionId;
  port.emitter.emit({
    ...bridgedPayload,
    source: 'subagent',
    subagentToolCallId: correlation.parentToolCallId,
    subagentSessionId: correlation.subagentSessionId,
  } as EmitInput);
}

export function internalTryReserveSubagentExecution(
  port: DelegationDriverSessionPort,
):
  | { reserved: true; inFlight: number; maxConcurrent?: number }
  | { reserved: false; inFlight: number; maxConcurrent: number } {
  const maxConcurrent = port.harness._getSubagentMaxConcurrent();
  if (maxConcurrent !== undefined && port.subagentExecutionsInFlight >= maxConcurrent) {
    return { reserved: false, inFlight: port.subagentExecutionsInFlight, maxConcurrent };
  }
  port.subagentExecutionsInFlight += 1;
  return {
    reserved: true,
    inFlight: port.subagentExecutionsInFlight,
    ...(maxConcurrent !== undefined ? { maxConcurrent } : {}),
  };
}

export function internalReleaseSubagentExecution(port: DelegationDriverSessionPort): void {
  // Every caller releases in a `finally`. Clamp defensively so a late cleanup
  // can never turn an accidental duplicate release into an unlimited bypass.
  port.subagentExecutionsInFlight = Math.max(0, port.subagentExecutionsInFlight - 1);
  if (port.delegationReconcileCapacityBlocked && port.state === 'live') {
    port.delegationReconcileCapacityBlocked = false;
    scheduleDelegationReconcileRetry(port, 0);
  }
  port.notifyMaybeIdle();
}

export function internalOnDelegatedSettlement(
  port: DelegationDriverSessionPort,
  listener: (subagentSessionId: string) => void,
): () => void {
  port.delegatedSettlementListeners.add(listener);
  return () => {
    port.delegatedSettlementListeners.delete(listener);
  };
}

export function internalAssertSubagentControlAdmitted(
  port: DelegationDriverSessionPort,
  callerSignal?: AbortSignal,
): void {
  port.assertLive('subagent control');
  port.assertActiveWorkAdmission(port.captureActiveWorkAdmission('subagent control'), 'subagent control', callerSignal);
}

export async function internalCancelOwnedSubagent(
  port: DelegationDriverSessionPort,
  subagentSessionId: string,
  reason: string,
  callerSignal?: AbortSignal,
): Promise<'delivered' | 'delivered_incomplete' | 'unavailable'> {
  port.assertNotDeleted();
  // Only a live parent whose turn is still admitted may cancel: a call that
  // resumes after eviction, Stop or reset must not act on the child.
  const epoch = port.captureActiveWorkAdmission('subagent cancel');
  const assertAdmitted = () => {
    port.assertLive('subagent cancel');
    port.assertActiveWorkAdmission(epoch, 'subagent cancel', callerSignal);
  };
  assertAdmitted();
  const { unavailable } = await port.cancelOwnedSubagentSessions([subagentSessionId], reason, assertAdmitted, true);
  if (unavailable.includes(subagentSessionId)) return 'unavailable';
  // A cold delegated child has no live driver in this process; reconciliation
  // maps its committed cancelRequest to a cancelled plan task. A live driver's
  // delegation is skipped by the reconcile pass, so this is safe either way.
  scheduleDelegationReconcileRetry(port, 0);
  // The child committed its cancel, but a descendant below it could not be
  // reached; a repeated cancel resumes that cascade.
  return unavailable.length > 0 ? 'delivered_incomplete' : 'delivered';
}

export async function listDelegatedSubagentSessionIds(port: DelegationDriverSessionPort): Promise<string[]> {
  const ids = new Set<string>();
  let cursor: string | undefined;
  for (;;) {
    const page = await port.storage.listPlanTasks({
      harnessName: port.record.harnessName,
      sessionId: port.id,
      limit: 500,
      cursor,
    });
    for (const task of page.tasks) {
      if (task.delegatedSubagentSessionId !== undefined) ids.add(task.delegatedSubagentSessionId);
    }
    if (!page.cursor || page.tasks.length === 0) break;
    cursor = page.cursor;
  }
  return Array.from(ids);
}

/**
 * Continue a durable cancellation through every owned delegation edge. This
 * remains callable after `cancelRequest` committed so a restart between the
 * parent CAS and descendant propagation can safely resume the cascade.
 */
export async function cascadeCancellationToOwnedSubagents(
  port: DelegationDriverSessionPort,
  reason: string,
): Promise<{ unavailable: string[] }> {
  const activeChildIds = Array.from(port.activeSubagents.values(), state => state.subagentSessionId);
  let delegatedChildIds: string[] = [];
  let discoveryFailed = false;
  try {
    // A restarted parent may have no process-local `_activeSubagents` entry
    // even though its plan still durably owns detached descendants. Discover
    // those links before cascading so cancellation reaches the whole cold
    // delegation tree rather than only whichever generation happened to be
    // hydrated first.
    delegatedChildIds = await port.listDelegatedSubagentSessionIds();
  } catch {
    // Mark this session as incomplete: its caller must preserve the ownership
    // edge and retry because we cannot prove that every descendant was found.
    discoveryFailed = true;
  }
  const cancellation = await port.cancelOwnedSubagentSessions([...activeChildIds, ...delegatedChildIds], reason);
  return {
    unavailable: Array.from(new Set([...(discoveryFailed ? [port.id] : []), ...cancellation.unavailable])),
  };
}

/** TM-6 — cancel any pending reconcile-on-rehydrate retry timers on teardown. */
export function clearDelegationReconcileTimers(port: DelegationDriverSessionPort): void {
  for (const timer of port.delegationReconcileRetryTimers) clearTimeout(timer);
  port.delegationReconcileRetryTimers.clear();
  port.delegationReconcileRerunRequested = false;
  for (const watch of port.delegationNoticeWatches.values()) watch.dispose();
  port.delegationNoticeWatches.clear();
}

export function internalRearmDelegationReconcile(port: DelegationDriverSessionPort): void {
  scheduleDelegationReconcileRetry(port, 0);
}

/**
 * Liveness, admission, subagent type, and §8 depth checks shared by
 * `task_delegate` and its preflight. Reads only: nothing is written.
 */
export function preflightPlanTaskDelegate(
  port: DelegationDriverSessionPort,
  input: { agentType: string; modelOverride?: string },
  callerSignal?: AbortSignal,
): { admissionEpoch: number; def: SubagentDefinition; childDepth: number } {
  port.assertLive('task_delegate');
  const admissionEpoch = port.captureActiveWorkAdmission('task_delegate');
  port.assertActiveWorkAdmission(admissionEpoch, 'task_delegate', callerSignal);
  const def = port.harness._getSubagentType(input.agentType);
  if (!def) {
    throw new HarnessValidationError('agentType', `unknown subagent type "${input.agentType}"`);
  }
  if (input.modelOverride !== undefined && typeof input.modelOverride !== 'string') {
    throw new HarnessValidationError('modelOverride', 'must be a string when provided');
  }
  // §8 depth cap — fail closed BEFORE creating any child record (mirrors the
  // spawn tool's pre-creation gate).
  const childDepth = port.subagentDepth + 1;
  const maxDepth = port.harness._getSubagentMaxDepth();
  if (childDepth > maxDepth) {
    throw new HarnessSubagentDepthExceededError(maxDepth, childDepth);
  }
  return { admissionEpoch, def, childDepth };
}

export function planTaskDelegatePreflight(
  port: DelegationDriverSessionPort,
  input: { agentType: string; task?: string; modelOverride?: string },
  callerSignal?: AbortSignal,
): number {
  if (input.task !== undefined) {
    const bytes = new TextEncoder().encode(input.task).byteLength;
    if (bytes > PLAN_TASK_DELEGATED_BODY_MAX_BYTES) {
      throw new HarnessValidationError(
        'task',
        `must be at most ${PLAN_TASK_DELEGATED_BODY_MAX_BYTES} UTF-8 bytes; received ${bytes}`,
      );
    }
  }
  return preflightPlanTaskDelegate(port, input, callerSignal).admissionEpoch;
}

/**
 * @internal — render a delegated subtree subset as a LOSSLESS structured block
 * for the subagent. Each node keeps its stable `taskId`, parent edge, status,
 * sibling order, and `blockedBy` ids, indented by its TRUE depth (computed from
 * the in-subset parent chain). Nodes are ordered as a stable pre-order
 * traversal (siblings by `order`, then `taskId`) so the same subtree always
 * renders identically. A clipped subtree (the storage `truncated` flag, or a
 * node whose parent fell outside the returned page) appends an explicit
 * truncation marker so the subagent knows its unit is incomplete.
 */
export function renderDelegatedSubtree(
  port: DelegationDriverSessionPort,
  rootTaskId: string,
  tasks: HarnessPlanTask[],
  storageTruncated: boolean,
): string {
  const byId = new Map(tasks.map(t => [t.taskId, t]));
  const childrenOf = new Map<string | undefined, HarnessPlanTask[]>();
  let parentDropped = false;
  for (const t of tasks) {
    // A node whose parent is not in the returned page (other than the root,
    // whose parent is intentionally outside the subset) means the subtree was
    // clipped above this node — flag it as truncated.
    const parentKey =
      t.taskId === rootTaskId
        ? undefined
        : t.parentTaskId !== undefined && byId.has(t.parentTaskId)
          ? t.parentTaskId
          : undefined;
    if (t.taskId !== rootTaskId && (t.parentTaskId === undefined || !byId.has(t.parentTaskId))) {
      parentDropped = true;
    }
    const bucket = childrenOf.get(parentKey);
    if (bucket) bucket.push(t);
    else childrenOf.set(parentKey, [t]);
  }
  for (const bucket of childrenOf.values()) {
    bucket.sort((a, b) =>
      a.order !== b.order ? a.order - b.order : a.taskId < b.taskId ? -1 : a.taskId > b.taskId ? 1 : 0,
    );
  }
  const lines: string[] = [];
  const walk = (node: HarnessPlanTask, depth: number): void => {
    const indent = '  '.repeat(depth);
    const fields = [`id=${node.taskId}`, `status=${node.status}`, `order=${node.order}`];
    if (node.blockedBy !== undefined && node.blockedBy.length > 0) {
      fields.push(`blockedBy=${node.blockedBy.join(',')}`);
    }
    lines.push(`${indent}- [${fields.join(' ')}] ${node.content}`);
    for (const child of childrenOf.get(node.taskId) ?? []) walk(child, depth + 1);
  };
  const root = byId.get(rootTaskId);
  if (root) walk(root, 0);
  let block = `Delegated subtree (lossless — taskId/status/order/blockedBy preserved):\n${lines.join('\n')}`;
  if (storageTruncated || parentDropped) {
    block += `\n(TRUNCATED: the subtree was clipped at ${DELEGATED_SUBTREE_MAX_NODES} nodes; some descendants are not shown.)`;
  }
  return block;
}

/**
 * @internal — deterministic `admissionId` for a delegated subagent signal.
 * Stable across restarts so re-issuing it re-attaches to the original durable
 * signal result (including a suspended run) instead of dispatching duplicate
 * specialist work.
 */
export function sessionDelegationAdmissionId(port: DelegationDriverSessionPort, taskId: string): string {
  return delegationAdmissionId(port.id, taskId);
}

export async function reconcileDelegatedTask(
  port: DelegationDriverSessionPort,
  taskId: string,
  subagentSessionId: string,
  outcome: DelegatedSubagentOutcome,
  result?: HarnessSubagentResultSummary,
  notify?: { childCancelRequest: SessionRecord['cancelRequest'] },
): Promise<boolean> {
  if (port.state === 'closed') return false;
  // Before the first await, so a wait that begins while this write is in
  // flight counts the child as running when it began.
  port.settlingDelegations.add(subagentSessionId);
  const noticeMode =
    notify === undefined ? undefined : delegatedSettlementNoticeMode(port, notify.childCancelRequest, outcome);
  // A Stop that starts after this point must still suppress the wake.
  const noticeAdmissionEpoch = port.activeWorkAdmissionEpoch;
  let releaseRemindSettlement: (() => void) | undefined;
  try {
    const durableResult =
      result ??
      (outcome === 'cancelled'
        ? summarizeHarnessSubagentEventResult(
            summarizeHarnessSubagentResult(undefined, {
              isError: true,
              error: { ...DELEGATED_SUBAGENT_CANCELLED_ERROR },
            }),
          )
        : outcome !== 'completed'
          ? {
              ...summarizeHarnessSubagentEventResult(
                summarizeHarnessSubagentResult(undefined, {
                  isError: true,
                  error: {
                    code: outcome === 'blocked' ? 'harness.delegation_blocked' : 'harness.delegation_failed',
                    message: `The delegated task ${outcome} without usable terminal result evidence`,
                  },
                }),
              ),
              outcome,
            }
          : undefined);
    const reconcileResult = await port.serializePlanTaskWrite(() => {
      if (noticeMode === 'remind') {
        const starting = new Promise<void>(resolve => {
          releaseRemindSettlement = resolve;
        });
        port.remindSettlementsStarting.add(starting);
        void starting.then(() => port.remindSettlementsStarting.delete(starting));
      }
      return planTaskReconcileDelegation(port.planTaskPort(), {
        taskId,
        subagentSessionId,
        outcome,
        ...(durableResult !== undefined ? { result: durableResult } : {}),
        ...(noticeMode !== undefined ? { notice: noticeMode } : {}),
        // Before the public delegate_settled event: a wait that event starts
        // must not take this settlement as one from after its call.
        onCommitted: () => {
          port.settlingDelegations.delete(subagentSessionId);
          for (const listener of Array.from(port.delegatedSettlementListeners)) {
            try {
              listener(subagentSessionId);
            } catch {
              // A listener only wakes a reader early. One that throws must not
              // skip the other listeners, the notice below, or reach the
              // reconcile retry for a settlement that is already durable.
            }
          }
        },
      });
    });
    if (reconcileResult.reconciled) port.notifyDisplayRefresh();
    if (reconcileResult.notice !== undefined) {
      port.startDelegationSettlementNotice(reconcileResult.notice, noticeAdmissionEpoch, 0);
    }
    return reconcileResult.reconciled;
  } catch {
    // Keep a live recovery path even when no future parent message or hydrate
    // occurs. The durable child signal evidence is the authority; a bounded
    // scan retry re-reads the exact link and settles it idempotently.
    scheduleDelegationReconcileRetry(port, 0);
    return false;
  } finally {
    port.settlingDelegations.delete(subagentSessionId);
    releaseRemindSettlement?.();
  }
}

/**
 * Notice mode for a detached settlement, or undefined for none. While this
 * parent is itself cancelled, nothing is owed. Otherwise a `completed`
 * settlement always owes its notice, even when a cancel landed on the child
 * after its result was read. This parent's own targeted cancel (whatever its
 * reason) suppresses only the `cancelled` settlement it caused, and its own
 * Stop/reset cascade owes none. The driver's durable delegation timeout is an
 * untargeted cancel and a real settlement the parent model must learn about.
 */
export function delegatedSettlementNoticeMode(
  port: DelegationDriverSessionPort,
  childCancelRequest: SessionRecord['cancelRequest'],
  outcome: DelegatedSubagentOutcome,
): DelegatedSettlementNoticeMode | undefined {
  const configured = port.harness._getSubagentDelegatedSettlementNotice();
  if (configured === 'none') return undefined;
  if (port.record.cancelRequest !== undefined) return undefined;
  if (outcome === 'completed') return configured;
  if (isParentRequestedSubagentCancel(childCancelRequest, port.id)) {
    return outcome === 'cancelled' ? undefined : configured;
  }
  if (childCancelRequest?.requestedBy === port.id && childCancelRequest.reason !== 'delegation_timeout') {
    return undefined;
  }
  return configured;
}

/**
 * Make child closure the prerequisite for clearing its plan ownership edge.
 * Returns false on a transient/foreign-lease failure so the caller can retain
 * the link and retry; a closed child is already safe once its recovered
 * completion is published. The child's close publishes it while the link
 * still names the child (PF-7499); a closed child that still owes it is
 * reopened and closed again, so the task never settles unpublished.
 */
export async function closeDelegatedChildForSettlement(
  port: DelegationDriverSessionPort,
  childRecord: SessionRecord,
  taskId: string,
): Promise<boolean> {
  try {
    if (childRecord.closedAt !== undefined && !(await delegationCompletionOwed(port, childRecord, taskId))) {
      return true;
    }
    const child = await port.harness.session({ sessionId: childRecord.id, resourceId: port.resourceId });
    await child.close();
    return true;
  } catch {
    return false;
  }
}

/**
 * Re-read the child's committed cancel request, from any source, or
 * `undefined` when none is committed. A failed read rejects, so recovery
 * retries instead of settling.
 */
export async function loadDelegationCancel(
  port: DelegationDriverSessionPort,
  childId: string,
): Promise<SessionRecord['cancelRequest']> {
  const latest = await port.storage.loadSession({ harnessName: port.record.harnessName, sessionId: childId });
  return latest?.cancelRequest;
}

/**
 * Settle a delegated task from a cancel committed on its child. Only this
 * parent's targeted marker, not the reason, makes it `cancelled`; any other
 * cancel fails it, keeping a framework timeout's precise deadline result.
 */
export async function reconcileDelegationFromCancel(
  port: DelegationDriverSessionPort,
  taskId: string,
  childId: string,
  cancelRequest: NonNullable<SessionRecord['cancelRequest']>,
  deadlineAt: number | undefined,
): Promise<void> {
  const parentCancelled = isParentRequestedSubagentCancel(cancelRequest, port.id);
  await reconcileDelegatedTask(
    port,
    taskId,
    childId,
    parentCancelled ? 'cancelled' : 'failed',
    !parentCancelled && cancelRequest.reason === 'delegation_timeout' && deadlineAt !== undefined
      ? summarizeDelegationDeadline(deadlineAt)
      : undefined,
    { childCancelRequest: cancelRequest },
  );
}

export function loadDelegationEvidence(
  port: DelegationDriverSessionPort,
  childRecord: SessionRecord,
  taskId: string,
): Promise<AgentSignalResultEvidence | OperationAdmissionTombstone | null> {
  return port.storage.loadMessageResultEvidence({
    harnessName: childRecord.harnessName,
    sessionId: childRecord.id,
    resourceId: childRecord.resourceId,
    threadId: childRecord.threadId,
    signalId: delegationSignalId(childRecord, sessionDelegationAdmissionId(port, taskId)),
  });
}

/** Whether a closed delegated child's settled run still lacks its published completion. */
export async function delegationCompletionOwed(
  port: DelegationDriverSessionPort,
  childRecord: SessionRecord,
  taskId: string,
): Promise<boolean> {
  if (!port.storage.supportsDispatchRecovery) return false;
  const evidence = await loadDelegationEvidence(port, childRecord, taskId);
  if (!evidence || !('status' in evidence) || !owesRecoveredCompletion(evidence)) return false;
  return (await port.storage.loadRunSummary({ harnessName: childRecord.harnessName, runId: evidence.runId })) === null;
}

export async function loadDelegationOutcomeFromEvidence(
  port: DelegationDriverSessionPort,
  childRecord: SessionRecord,
  taskId: string,
  deadlineAt: number,
): Promise<{ outcome: DelegatedSubagentOutcome; result: HarnessSubagentResultSummary } | undefined> {
  const evidence = await loadDelegationEvidence(port, childRecord, taskId);
  if (!evidence || !('status' in evidence)) return undefined;
  if (evidence.status === 'completed' || evidence.status === 'failed') {
    const settledAt = evidence.updatedAt;
    if (!Number.isSafeInteger(settledAt) || settledAt > deadlineAt) {
      return { outcome: 'failed', result: summarizeDelegationDeadline(deadlineAt) };
    }
  }
  if (evidence.status === 'completed') {
    const rawResult = (evidence as { result?: unknown }).result;
    const finishReason = (rawResult as { finishReason?: unknown } | undefined)?.finishReason;
    const declaredReport = parseHarnessSubagentOutcomeReport(rawResult);
    const declaredTerminalText = parseHarnessTerminalToolResultText(
      (rawResult as { terminalToolResult?: unknown } | undefined)?.terminalToolResult,
    );
    const report = verifyHarnessSubagentTerminalCompletion(rawResult);
    const outcome: DelegatedSubagentOutcome = report !== undefined ? report.outcome : 'failed';
    // A budget stop persisted with the child's result keeps its typed code
    // when a crash interrupted the live settlement.
    const budgetExhausted = report === undefined ? parseHarnessSubagentBudgetExhausted(rawResult) : undefined;
    const result = summarizeHarnessSubagentResult(rawResult, {
      isError: outcome !== 'completed',
      ...(budgetExhausted !== undefined
        ? {
            budgetExhausted,
            error: harnessSubagentBudgetExhaustedError(childRecord.subagentTypeId ?? 'delegated', budgetExhausted),
          }
        : outcome !== 'completed'
          ? {
              error: {
                code:
                  report?.issue?.code ??
                  ((declaredReport !== undefined || declaredTerminalText !== undefined) && report === undefined
                    ? 'harness.subagent_evidence_unverified'
                    : report === undefined
                      ? 'harness.subagent_outcome_missing'
                      : 'harness.subagent_incomplete'),
                message:
                  report?.issue?.message ??
                  (report === undefined
                    ? declaredReport !== undefined || declaredTerminalText !== undefined
                      ? 'The delegated subagent terminal completion did not match framework execution receipts'
                      : `The delegated subagent ended without ${HARNESS_SUBAGENT_OUTCOME_REPORT_TOOL_ID}`
                    : `The delegated subagent reported ${report.outcome} after provider finish reason "${String(finishReason ?? 'missing')}"`),
              },
            }
          : {}),
    });
    return { outcome, result };
  }
  if (evidence.status === 'failed') {
    return {
      outcome: 'failed',
      result: summarizeHarnessSubagentEventResult(
        summarizeHarnessSubagentResult(undefined, { isError: true, error: evidence.error }),
      ),
    };
  }
  return undefined; // 'pending' — no terminal outcome yet.
}

/**
 * @internal — publish a recovered delegation run's completion once, from the
 * canonical settled evidence: `agent_end` and the reconstructed
 * `run_completed`, acknowledged by the durable run summary (as message
 * recovery does). A run whose summary exists is already published. Throws
 * when publication fails, so the caller keeps the child open and the task
 * linked, and a later scan retries it.
 */
export async function publishRecoveredDelegationCompletion(
  port: DelegationDriverSessionPort,
  admissionId: string,
): Promise<void> {
  if (!port.storage.supportsDispatchRecovery) return;
  const loaded = await port.storage.loadMessageResultEvidence({
    harnessName: port.record.harnessName,
    sessionId: port.id,
    resourceId: port.resourceId,
    threadId: port.threadId,
    signalId: port.signalAdmissionIdentity(admissionId).signalId,
  });
  if (!loaded || !('status' in loaded) || !owesRecoveredCompletion(loaded)) return;
  await port.publishRecoveredRunCompletion(loaded);
}

/**
 * Publish the recovered completion of each delegation the parent's plan
 * still links to this child. The link is cleared only when the task
 * settles, and every settlement closes the child first, so a closing child
 * finds every delegation whose completion can still be owed.
 */
export async function publishOwedDelegationCompletions(port: DelegationDriverSessionPort): Promise<void> {
  const parentSessionId = port.record.parentSessionId;
  if (parentSessionId === undefined) return;
  let cursor: string | undefined;
  for (;;) {
    const page = await port.storage.listPlanTasks({
      harnessName: port.record.harnessName,
      sessionId: parentSessionId,
      limit: 500,
      cursor,
    });
    for (const task of page.tasks) {
      if (task.delegatedSubagentSessionId !== port.id) continue;
      await publishRecoveredDelegationCompletion(port, delegationAdmissionId(parentSessionId, task.taskId));
    }
    if (!page.cursor || page.tasks.length === 0) return;
    cursor = page.cursor;
  }
}

export async function reconcileDelegationsOnHydrate(port: DelegationDriverSessionPort, attempt = 0): Promise<void> {
  const current = port.delegationReconcileInFlight;
  if (current !== undefined) {
    // Do not lose a timer/capacity-release wakeup merely because a slower scan
    // is still reading another delegated task. The owner of `current` loops
    // once more before releasing the singleflight.
    port.delegationReconcileRerunRequested = true;
    return current;
  }
  const run = (async () => {
    do {
      port.delegationReconcileRerunRequested = false;
      await port.reconcileDelegationsOnHydratePass(attempt);
    } while (port.state === 'live' && port.delegationReconcileRerunRequested);
  })();
  port.delegationReconcileInFlight = run;
  try {
    await run;
  } finally {
    if (port.delegationReconcileInFlight === run) port.delegationReconcileInFlight = undefined;
    // A request can land after the loop's final condition but before this
    // singleflight is cleared. Preserve that narrow wakeup with a fresh timer.
    if (port.state === 'live' && port.delegationReconcileRerunRequested) {
      port.delegationReconcileRerunRequested = false;
      scheduleDelegationReconcileRetry(port, 0);
    }
  }
}

export async function reconcileDelegationsOnHydratePass(
  port: DelegationDriverSessionPort,
  attempt: number,
): Promise<void> {
  if (port.state !== 'live') return;
  let admissionEpoch: number;
  try {
    admissionEpoch = port.captureActiveWorkAdmission('delegation reconciliation');
  } catch {
    // Stop/reset owns the branch. Its cancellation/reset pass will settle or
    // preserve every durable edge; recovery must not attach new work behind it.
    return;
  }
  let cursor: string | undefined;
  const delegated: HarnessPlanTask[] = [];
  const owedNotices: PendingDelegationSettlementNotice[] = [];
  const owedKeys = new Set<string>();
  // PF-7750: only watches kept before the listing began may be disposed; one
  // kept while it ran belongs to a notice the listing may have missed.
  const keptBeforeListing = new Set(port.delegationNoticeWatches.keys());
  try {
    for (;;) {
      const page = await port.storage.listPlanTasks({
        harnessName: port.record.harnessName,
        sessionId: port.id,
        limit: 500,
        cursor,
      });
      for (const t of page.tasks) {
        if (t.delegatedSubagentSessionId !== undefined && !TERMINAL_PLAN_TASK_STATUSES.has(t.status)) {
          delegated.push(t);
        }
        // A settlement committed before a crash can still owe its notice.
        // PF-7679: a notice under a live claim is being delivered; one whose
        // claimer lost the lease or is gone is taken over.
        const owedNotice = readPendingDelegationSettlementNotice(t);
        if (owedNotice !== undefined) owedKeys.add(`${owedNotice.subagentSessionId}:${owedNotice.digest}`);
        if (owedNotice !== undefined && port.delegationNoticeClaimable(owedNotice)) owedNotices.push(owedNotice);
      }
      if (!page.cursor || page.tasks.length === 0) break;
      cursor = page.cursor;
    }
  } catch {
    // Best-effort: the listing itself failed transiently — re-arm a
    // retry so a delegated task is not stranded by a flaky read.
    scheduleDelegationReconcileRetry(port, attempt);
    return;
  }

  // PF-7679: a row watch kept for a retry ends once its notice is no longer
  // owed (another owner or path resolved it), so no watch outlives it.
  port.disposeKeptDelegationNoticeWatches(key => keptBeforeListing.has(key) && !owedKeys.has(key));
  for (const owedNotice of owedNotices) {
    port.startDelegationSettlementNotice(owedNotice, admissionEpoch, attempt);
  }

  let needsRetry = false;
  for (const task of delegated) {
    const childId = task.delegatedSubagentSessionId!;
    if (!port.activeWorkAdmissionIsCurrent(admissionEpoch)) return;
    // A live driver in this process already owns the exact delegation. This
    // also keeps an explicit/manual recovery kick from double-reserving it.
    if (port.delegationDriverClaims.has(childId) || port.activeSubagents.has(`delegate:${childId}`)) continue;
    let childRecord: SessionRecord | null;
    try {
      childRecord = await port.storage.loadSession({ harnessName: port.record.harnessName, sessionId: childId });
    } catch {
      // Transient storage error reading the child — retry the whole scan later
      // rather than silently leaving this task stuck `in_progress`.
      needsRetry = true;
      continue;
    }
    // Subagent session GONE/deleted → fail closed.
    if (!childRecord) {
      await reconcileDelegatedTask(port, task.taskId, childId, 'failed', undefined, {
        childCancelRequest: undefined,
      });
      continue;
    }
    const delegationAttempt = readDelegationAttemptMetadata(task, childId);
    // A CANCELLED subagent never settles from its run, regardless of how the
    // finishReason landed — mirrors the live driver's `cancelRequest` override
    // (a run can race to `complete` after a cancel). This parent's own cancel
    // settles the task `cancelled`; any other cancel fails it. Checked before
    // the evidence so a `completed` evidence row from that race cannot mask
    // the cancel. Preserve the framework-owned timeout reason across the crash
    // window between the durable cancellation CAS and task reconciliation;
    // otherwise recovery degrades a precise timeout into the generic
    // delegation-failed result.
    if (childRecord.cancelRequest !== undefined) {
      if (!(await closeDelegatedChildForSettlement(port, childRecord, task.taskId))) {
        needsRetry = true;
        continue;
      }
      await reconcileDelegationFromCancel(
        port,
        task.taskId,
        childId,
        childRecord.cancelRequest,
        delegationAttempt?.deadlineAt,
      );
      continue;
    }
    if (delegationAttempt === undefined) {
      if (!(await closeDelegatedChildForSettlement(port, childRecord, task.taskId))) {
        needsRetry = true;
        continue;
      }
      await reconcileDelegatedTask(port, task.taskId, childId, 'failed', undefined, {
        childCancelRequest: childRecord.cancelRequest,
      });
      continue;
    }
    // DURABLE terminal evidence is the authority — a delegated run that ERRORED
    // before closing recovers as `failed`, not `completed`. Its durable
    // settlement timestamp must also fall within the immutable attempt
    // deadline; a late provider result cannot retroactively win recovery.
    let durable: { outcome: DelegatedSubagentOutcome; result: HarnessSubagentResultSummary } | undefined;
    try {
      durable = await loadDelegationOutcomeFromEvidence(port, childRecord, task.taskId, delegationAttempt.deadlineAt);
    } catch {
      // Unread evidence is not absent evidence: the closed-child fallback
      // below would settle the task without its owed publication.
      needsRetry = true;
      continue;
    }
    if (durable !== undefined) {
      if (!(await closeDelegatedChildForSettlement(port, childRecord, task.taskId))) {
        needsRetry = true;
        continue;
      }
      // A cancel committed while the evidence read was pending wins over any
      // evidence outcome, by the same rule as a cancel committed before it.
      let committedCancel: SessionRecord['cancelRequest'];
      try {
        committedCancel = await loadDelegationCancel(port, childId);
      } catch {
        needsRetry = true;
        continue;
      }
      if (committedCancel !== undefined) {
        await reconcileDelegationFromCancel(port, task.taskId, childId, committedCancel, delegationAttempt.deadlineAt);
        continue;
      }
      await reconcileDelegatedTask(port, task.taskId, childId, durable.outcome, durable.result, {
        childCancelRequest: undefined,
      });
      continue;
    }
    // A close marker cannot prove useful work completed. Without terminal
    // signal evidence, fail closed rather than fabricating success.
    if (childRecord.closedAt !== undefined) {
      await reconcileDelegatedTask(port, task.taskId, childId, 'failed', undefined, {
        childCancelRequest: childRecord.cancelRequest,
      });
      continue;
    }
    // The absolute timestamp was committed with the ownership edge. Recovery
    // must never grant a fresh duration after restart. Terminal evidence above
    // still wins when it already proves the child settled.
    if (Date.now() >= delegationAttempt.deadlineAt) {
      if (!(await port.cancelAndCloseDelegatedChildForDeadline(childRecord))) {
        needsRetry = true;
        continue;
      }
      // The first cancel wins: a targeted cancel committed while the evidence
      // read was pending survives the deadline cancel and settles `cancelled`,
      // as in the live deadline branch. A later one never replaces the timeout.
      let targetedCancel: SessionRecord['cancelRequest'];
      try {
        const committedCancel = await loadDelegationCancel(port, childId);
        targetedCancel = isParentRequestedSubagentCancel(committedCancel, port.id) ? committedCancel : undefined;
      } catch {
        needsRetry = true;
        continue;
      }
      await reconcileDelegatedTask(
        port,
        task.taskId,
        childId,
        targetedCancel !== undefined ? 'cancelled' : 'failed',
        targetedCancel !== undefined ? undefined : summarizeDelegationDeadline(delegationAttempt.deadlineAt),
        { childCancelRequest: targetedCancel },
      );
      continue;
    }
    if (!port.activeWorkAdmissionIsCurrent(admissionEpoch)) return;
    // Reserve from the same pool as a newly delegated/inline child before
    // hydrating and attaching. A capacity miss is retried and is also kicked
    // immediately when any current child releases a slot.
    const reservation = internalTryReserveSubagentExecution(port);
    if (!reservation.reserved) {
      port.delegationReconcileCapacityBlocked = true;
      needsRetry = true;
      continue;
    }
    port.delegationDriverClaims.add(childId);

    // Subagent still active → re-attach by re-issuing the admitted signal with
    // the deterministic admission id. The durable dispatch fence observes the
    // original execution or waits through HITL; it does not enqueue a second.
    let child: Session | undefined;
    try {
      child = await port.harness.session({ sessionId: childId, resourceId: port.resourceId });
    } catch {
      port.delegationDriverClaims.delete(childId);
      internalReleaseSubagentExecution(port);
      // Could not re-acquire the child (locked elsewhere / transient) — retry
      // the scan later so the task is not stranded.
      needsRetry = true;
      continue;
    }
    // Hydrating the child crossed an async ownership boundary. Stop/reset may
    // have invalidated this recovery pass or removed/replaced the exact link;
    // re-read both authorities before starting the admitted signal driver.
    let linkStillCurrent = false;
    try {
      linkStillCurrent = await port.delegationLinkIsCurrent(task.taskId, childId);
    } catch {
      port.delegationDriverClaims.delete(childId);
      internalReleaseSubagentExecution(port);
      needsRetry = true;
      continue;
    }
    if (!port.activeWorkAdmissionIsCurrent(admissionEpoch) || !linkStillCurrent) {
      port.delegationDriverClaims.delete(childId);
      internalReleaseSubagentExecution(port);
      if (!port.activeWorkAdmissionIsCurrent(admissionEpoch)) return;
      continue;
    }
    // PF-7499: a dispatch the dead owner already claimed or accepted is
    // interrupted, never replayed under the same run id. The settled
    // evidence then rolls the task up through the durable path.
    let orphan: 'reattach' | 'retry' | 'interrupted';
    try {
      orphan = await child._interruptOrphanedDelegationSignal(sessionDelegationAdmissionId(port, task.taskId));
    } catch {
      orphan = 'retry';
    }
    if (orphan !== 'reattach') {
      port.delegationDriverClaims.delete(childId);
      internalReleaseSubagentExecution(port);
      const interrupted =
        orphan === 'interrupted'
          ? await loadDelegationOutcomeFromEvidence(
              port,
              child.getRecord(),
              task.taskId,
              delegationAttempt.deadlineAt,
            ).catch(() => undefined)
          : undefined;
      if (
        interrupted === undefined ||
        !(await closeDelegatedChildForSettlement(port, child.getRecord(), task.taskId))
      ) {
        needsRetry = true;
        continue;
      }
      await reconcileDelegatedTask(port, task.taskId, childId, interrupted.outcome, interrupted.result, {
        childCancelRequest: child.getRecord().cancelRequest,
      });
      continue;
    }
    // §9 — restore the SubagentDefinition overrides on the reattached child. The
    // `tools` / `workspace` overrides are applied in-memory at delegate time and
    // are not persisted on the child, so re-resolve them from the durable
    // subagent type id before re-driving (unknown/unset type ⇒ mode-derived
    // surface, the documented fallback).
    if (task.delegatedSubagentTypeId !== undefined) {
      const def = port.harness._getSubagentType(task.delegatedSubagentTypeId);
      if (def) {
        child._subagentInheritWorkspace = (def.workspace ?? 'inherit') === 'inherit';
        if (def.tools) child._subagentToolsOverride = def.tools;
        if (def.toolAllowlist) child._subagentToolAllowlist = def.toolAllowlist;
      }
    }
    const agentType = task.delegatedSubagentTypeId ?? childRecord.subagentTypeId ?? 'delegated';
    child._subagentParentToolCallId = delegationAttempt.parentToolCallId;
    port.activeSubagents.set(`delegate:${child.id}`, {
      subagentSessionId: child.id,
      agentType,
      task: delegationAttempt.taskBody,
      parentToolCallId: delegationAttempt.parentToolCallId,
      startedAt: delegationAttempt.startedAt,
      status: childRecord.pendingResume !== undefined ? 'awaiting_input' : 'running',
    });
    void port.driveDelegatedSubagent(
      task.taskId,
      child,
      delegationAttempt.taskBody,
      agentType,
      childRecord.modelId,
      delegationAttempt.startedAt,
      delegationAttempt.deadlineAt,
      delegationAttempt.parentToolCallId,
      delegationAttempt.requestContextApp,
    );
  }
  if (needsRetry) scheduleDelegationReconcileRetry(port, attempt);
}

export async function delegationLinkIsCurrent(
  port: DelegationDriverSessionPort,
  taskId: string,
  childId: string,
): Promise<boolean> {
  const snapshot = await port.storage.loadPlanTaskSubtree({
    harnessName: port.record.harnessName,
    sessionId: port.id,
    rootTaskId: taskId,
    limit: 1,
  });
  const current = snapshot.tasks.find(task => task.taskId === taskId);
  return current?.delegatedSubagentSessionId === childId && !TERMINAL_PLAN_TASK_STATUSES.has(current.status);
}

export function scheduleDelegationReconcileRetry(port: DelegationDriverSessionPort, attempt: number): void {
  if (port.state !== 'live') return;
  if (port.delegationReconcileRetryTimers.size > 0) {
    if (attempt !== 0) return;
    for (const existing of port.delegationReconcileRetryTimers) clearTimeout(existing);
    port.delegationReconcileRetryTimers.clear();
  }
  const next = attempt + 1;
  const exponent = Math.min(Math.max(0, attempt), 16);
  const backoffMs = Math.min(DELEGATION_RECONCILE_RETRY_BASE_MS * 2 ** exponent, DELEGATION_RECONCILE_RETRY_MAX_MS);
  const timer = setTimeout(() => {
    port.delegationReconcileRetryTimers.delete(timer);
    if (port.state !== 'live') return;
    void port.reconcileDelegationsOnHydrate(next);
  }, backoffMs);
  if (typeof timer.unref === 'function') timer.unref();
  port.delegationReconcileRetryTimers.add(timer);
}

export async function cancelSubagent(
  port: DelegationDriverSessionPort,
  opts: { subagentSessionId: string; reason?: string },
): Promise<void> {
  port.assertNotDeleted();
  // Ownership guard: only cancel a subagent THIS session is actively running.
  const isOwnChild = Array.from(port.activeSubagents.values()).some(
    s => s.subagentSessionId === opts.subagentSessionId,
  );
  if (!isOwnChild) return;
  const child = port.harness._internalGetLiveSession(opts.subagentSessionId);
  if (!child) return; // not live in this process — best-effort no-op
  try {
    await child._cancelInternal(
      { reason: opts.reason ?? 'subagent_cancelled', requestedBy: port.id },
      port.id,
      undefined,
      true,
    );
  } catch {
    // Best-effort: a late race (child already settling/closed) must not throw.
  }
}

export async function cancelOwnedSubagentSessions(
  port: DelegationDriverSessionPort,
  subagentSessionIds: Iterable<string>,
  reason: string,
  assertAdmitted?: () => void,
  targeted = false,
): Promise<{ cancelled: string[]; unavailable: string[] }> {
  const cancelled: string[] = [];
  const unavailable: string[] = [];
  for (const childId of new Set(subagentSessionIds)) {
    if (childId === port.id) {
      unavailable.push(childId);
      continue;
    }
    let child = port.harness._internalGetLiveSession(childId);
    if (child === undefined) {
      let stored: SessionRecord | null;
      try {
        stored = await port.storage.loadSession({
          harnessName: port.record.harnessName,
          sessionId: childId,
        });
      } catch {
        unavailable.push(childId);
        continue;
      }
      if (stored === null || stored.closedAt !== undefined || stored.closingAt !== undefined) {
        continue;
      }
      if (stored.parentSessionId !== port.id || stored.resourceId !== port.resourceId) {
        unavailable.push(childId);
        continue;
      }
      try {
        child = await port.harness.session({ sessionId: childId, resourceId: port.resourceId });
      } catch {
        unavailable.push(childId);
        continue;
      }
    }
    if (child.parentSessionId !== port.id || child.resourceId !== port.resourceId) {
      unavailable.push(childId);
      continue;
    }
    assertAdmitted?.();
    try {
      const alreadyCancelled = child.getRecord().cancelRequest !== undefined;
      const nested = await child._cancelInternal({ reason, requestedBy: port.id }, port.id, assertAdmitted, targeted);
      if (!alreadyCancelled) cancelled.push(childId);
      unavailable.push(...nested.unavailable);
    } catch (error) {
      // The caller lost admission mid-cancel: nothing was written; surface it.
      if (assertAdmitted !== undefined && admissionLost(assertAdmitted)) throw error;
      unavailable.push(childId);
    }
  }
  return { cancelled: Array.from(new Set(cancelled)), unavailable: Array.from(new Set(unavailable)) };
}

export async function sessionPlanTaskDelegate(
  port: DelegationDriverSessionPort,
  input: {
    taskId: string;
    agentType: string;
    task?: string;
    includeSubtree?: boolean;
    modelOverride?: string;
  },
  callerSignal?: AbortSignal,
  parentToolCallId?: string,
  parentRunId?: string,
  callerRequestContext?: RequestContext,
  parentTracingContext?: TracingContext,
  callerAdmissionEpoch?: number,
): Promise<PlanTaskView & { subagentSessionId: string }> {
  const preflight = preflightPlanTaskDelegate(port, input, callerSignal);
  const { def, childDepth } = preflight;
  const admissionEpoch = callerAdmissionEpoch ?? preflight.admissionEpoch;
  port.assertActiveWorkAdmission(admissionEpoch, 'task_delegate', callerSignal);
  const delegatedRequestContext = projectInheritedRequestContextApp(
    callerRequestContext,
    port.harness._getSubagentInheritedRequestContextAppKeys(),
  );
  const sourceWriteGuard = port.delegatingRunSourceWriteGuard(callerRequestContext);

  // Reject unknown, already-running, or dependency-blocked tasks before
  // reserving capacity or creating a durable child. The fenced link commit
  // repeats the same eligibility checks after child creation.
  const scopeSnapshot = await port.serializePlanTaskWrite(() =>
    capturePlanTaskDelegationScope(port.planTaskPort(), input.taskId, input.includeSubtree === true),
  );
  port.assertActiveWorkAdmission(admissionEpoch, 'task_delegate', callerSignal);
  let taskBody = input.task ?? scopeSnapshot.view.content;
  if (input.includeSubtree) {
    // Surface the delegated subtree SUBSET so the subagent sees the unit it
    // owns. This is a LOSSLESS transfer: every node keeps its stable taskId,
    // parent edge, status, sibling order, and blockedBy ids, rendered by true
    // depth (computed from the parent chain, not a flat one-level indent). A
    // clipped subtree appends an explicit truncation marker so the subagent
    // knows the unit it received is incomplete.
    taskBody = `${taskBody}\n\n${renderDelegatedSubtree(port, input.taskId, scopeSnapshot.tasks, false)}`;
  }
  port.assertActiveWorkAdmission(admissionEpoch, 'task_delegate', callerSignal);

  // Durable delegation and inline spawn share the same parent-local
  // backpressure reservation. Claim it before child creation/linking so the
  // detached API cannot bypass `subagents.maxConcurrent`.
  const reservation = internalTryReserveSubagentExecution(port);
  if (!reservation.reserved) {
    throw new HarnessSubagentConcurrencyLimitError(reservation.maxConcurrent, reservation.inFlight);
  }

  // Allocate the child identity and commit the durable plan ownership edge
  // BEFORE creating the child row. A crash in this order leaves a link to a
  // missing child, which recovery deterministically fails closed. The former
  // create-then-link order could leave an unreferenced active descendant whose
  // old-owner lease later poisoned root subtree renewal.
  const childSessionId = `sess-${randomUUID()}`;
  // Claim before the link write awaits storage. A reconciliation wakeup that
  // observes the committed link before child allocation must not classify the
  // intentionally-not-yet-created child as missing.
  port.delegationDriverClaims.add(childSessionId);
  const delegationStartedAt = Date.now();
  const delegationDeadlineAt = delegationStartedAt + port.harness._getSubagentDelegationTimeoutMs();
  let view: PlanTaskView;
  try {
    view = await port.serializePlanTaskWrite(() =>
      planTaskDelegate(port.planTaskPort(), {
        taskId: input.taskId,
        subagentSessionId: childSessionId,
        subagentTypeId: input.agentType,
        includeSubtree: input.includeSubtree === true,
        taskBody,
        startedAt: delegationStartedAt,
        deadlineAt: delegationDeadlineAt,
        expectedScopeFingerprint: scopeSnapshot.fingerprint,
        ...(parentToolCallId !== undefined ? { parentToolCallId } : {}),
        ...(parentRunId !== undefined ? { parentRunId } : {}),
        parentModeId: port.currentRunModeId ?? port.record.modeId,
        ...(delegatedRequestContext !== undefined ? { requestContextApp: delegatedRequestContext.app } : {}),
        ...(sourceWriteGuard !== undefined ? { sourceWriteGuard } : {}),
      }),
    );
    port.assertActiveWorkAdmission(admissionEpoch, 'task_delegate', callerSignal);
  } catch (error) {
    // No child exists yet. If the link committed before active-work
    // invalidation won, clear it to a durable failed terminal state.
    await reconcileDelegatedTask(port, input.taskId, childSessionId, 'failed');
    port.delegationDriverClaims.delete(childSessionId);
    internalReleaseSubagentExecution(port);
    throw error;
  }

  // Create the durable subagent SESSION (reuse the subagent-tool creation path:
  // origin 'subagent-tool' + parentSessionId so cascade/depth/lease-sharing all
  // apply per §5.6). It is NOT auto-closed at delegate time — its lifecycle is
  // tracked by the plan task across turns.
  let child: Session;
  try {
    child = await port.harness.session({
      sessionId: childSessionId,
      resourceId: port.resourceId,
      threadId: { fresh: true },
      parentSessionId: port.id,
      origin: 'subagent-tool',
      // §9 — an unset `def.modeId` inherits the PARENT's current mode (as
      // documented), not the harness default mode.
      modeId: def.modeId ?? port.record.modeId,
      modelId: input.modelOverride ?? def.defaultModelId,
      subagentDepth: childDepth,
      // M4 — persist the type so the durable delegated child's per-subagent
      // overrides (tools / workspace / toolAllowlist) survive a direct-by-id
      // hydrate, not only this parent's delegation-reattach.
      subagentTypeId: input.agentType,
    });
  } catch (error) {
    await reconcileDelegatedTask(port, input.taskId, childSessionId, 'failed');
    port.delegationDriverClaims.delete(childSessionId);
    internalReleaseSubagentExecution(port);
    throw error;
  }
  const subagentWorkspaceMode = def.workspace ?? 'inherit';
  child._subagentInheritWorkspace = subagentWorkspaceMode === 'inherit';
  if (def.tools) child._subagentToolsOverride = def.tools;
  if (def.toolAllowlist) child._subagentToolAllowlist = def.toolAllowlist;
  const delegationParentToolCallId = view.delegation?.parentToolCallId ?? `delegate:${input.taskId}`;
  child._subagentParentToolCallId = delegationParentToolCallId;

  // Stop/revision may have won while child creation was awaiting storage.
  // The unlinked child has done no provider work, so close it and fail the
  // stale tool call before it can create an ownership edge.
  try {
    port.assertActiveWorkAdmission(admissionEpoch, 'task_delegate', callerSignal);
  } catch (error) {
    // The plan ownership edge was committed before child allocation. Close
    // the child before clearing that edge: if close fails, retaining the link
    // keeps the active descendant discoverable and retryable by recovery.
    let childClosed = false;
    try {
      await child.close();
      childClosed = true;
    } catch {
      scheduleDelegationReconcileRetry(port, 0);
    }
    if (childClosed) await reconcileDelegatedTask(port, input.taskId, child.id, 'failed');
    port.delegationDriverClaims.delete(childSessionId);
    internalReleaseSubagentExecution(port);
    throw error;
  }

  // A Stop can begin while the fenced link transaction is in flight. It may
  // have completed discovery before this new link became visible, so the
  // delegate itself must settle the now-stale attempt and never start it.
  try {
    port.assertActiveWorkAdmission(admissionEpoch, 'task_delegate', callerSignal);
    const linkStillCurrent = await port.delegationLinkIsCurrent(input.taskId, child.id);
    port.assertActiveWorkAdmission(admissionEpoch, 'task_delegate', callerSignal);
    if (!linkStillCurrent) {
      throw new HarnessValidationError(
        'taskId',
        `delegation ownership for task "${input.taskId}" changed during child allocation`,
      );
    }
  } catch (error) {
    try {
      await child._cancelInternal({ reason: 'parent_work_aborted', requestedBy: port.id }, port.id);
    } catch {
      // The exact task link below still fails closed even if child cleanup
      // races another owner; reconciliation remains retryable from storage.
    }
    let childClosed = false;
    try {
      await child.close();
      childClosed = true;
    } catch {
      scheduleDelegationReconcileRetry(port, 0);
    }
    if (childClosed) await reconcileDelegatedTask(port, input.taskId, child.id, 'failed');
    port.delegationDriverClaims.delete(childSessionId);
    internalReleaseSubagentExecution(port);
    throw error;
  }

  // Track for display (mirrors the spawn tool) keyed by the subagent id so a
  // delegated subagent renders while it runs.
  port.activeSubagents.set(`delegate:${child.id}`, {
    subagentSessionId: child.id,
    agentType: input.agentType,
    task: taskBody,
    parentToolCallId: delegationParentToolCallId,
    startedAt: delegationStartedAt,
    status: 'running',
  });

  // Arm the live completion hook. Delegation uses an admitted SIGNAL instead
  // of `message()`: signal evidence remains `pending` while the child is
  // suspended, carries the pending interaction's `originSignalId`, and settles
  // only after a later resume terminalizes. That gives detached HITL a durable
  // join handle across reconnects/restarts rather than falsely completing and
  // closing the child at the first `finishReason: suspended` segment.
  void port.driveDelegatedSubagent(
    input.taskId,
    child,
    taskBody,
    input.agentType,
    child.getRecord().modelId,
    delegationStartedAt,
    delegationDeadlineAt,
    delegationParentToolCallId,
    delegatedRequestContext?.app,
    parentTracingContext,
  );

  return { ...view, subagentSessionId: child.id };
}

export async function driveDelegatedSubagent(
  port: DelegationDriverSessionPort,
  taskId: string,
  child: Session,
  taskBody: string,
  agentType: string,
  modelId: string,
  startedAt: number,
  deadlineAt: number,
  parentToolCallId: string,
  requestContextApp?: Record<string, JsonValue>,
  parentTracingContext?: TracingContext,
): Promise<void> {
  let outcome: DelegatedSubagentOutcome = 'failed';
  let rawResult: unknown;
  let publicError: { code: string; message: string } | undefined;
  let budgetExhausted: HarnessSubagentBudgetExhaustion | undefined;
  // §SA2 — fold the detached child's live progress into the parent's display
  // projection (the delegate path has no event bridge of its own, unlike the
  // spawn tool). Unsubscribed in the finally below.
  const progressKey = `delegate:${child.id}`;
  const childDepth = child.subagentDepth;
  const innerToolNames = new Map<string, string>();
  child._subagentParentToolCallId = parentToolCallId;
  // PF-5620: stream the child's answer (its report `summary`) like the spawn tool does.
  child._subagentAnswerDeltaListener = (delta, reportToolCallId) => {
    emitSubagentEvent(port, {
      type: 'subagent_answer_delta',
      toolCallId: parentToolCallId,
      subagentSessionId: child.id,
      agentType,
      innerToolCallId: reportToolCallId,
      delta,
      depth: childDepth,
    });
  };
  const unsubProgress = child.subscribe(event => {
    internalUpdateSubagentProgress(port, progressKey, event);
    switch (event.type) {
      case 'text_delta':
        if (event.delta.length > 0) {
          emitSubagentEvent(port, {
            type: 'subagent_text_delta',
            toolCallId: parentToolCallId,
            subagentSessionId: child.id,
            agentType,
            delta: event.delta,
            depth: childDepth,
          });
        }
        break;
      case 'reasoning_delta':
        if (event.delta.length > 0) {
          emitSubagentEvent(port, {
            type: 'subagent_reasoning_delta',
            toolCallId: parentToolCallId,
            subagentSessionId: child.id,
            agentType,
            delta: event.delta,
            depth: childDepth,
          });
        }
        break;
      case 'tool_start':
        innerToolNames.set(event.toolCallId, event.toolName);
        emitSubagentEvent(port, {
          type: 'subagent_tool_start',
          toolCallId: parentToolCallId,
          subagentSessionId: child.id,
          agentType,
          innerToolCallId: event.toolCallId,
          toolName: event.toolName,
          input: event.input,
          depth: childDepth,
        });
        break;
      case 'tool_end': {
        const toolName = innerToolNames.get(event.toolCallId) ?? event.toolName;
        innerToolNames.delete(event.toolCallId);
        emitSubagentEvent(port, {
          type: 'subagent_tool_end',
          toolCallId: parentToolCallId,
          subagentSessionId: child.id,
          agentType,
          innerToolCallId: event.toolCallId,
          toolName,
          output: event.output,
          isError: event.isError ?? false,
          ...(event.durationMs !== undefined ? { durationMs: event.durationMs } : {}),
          depth: childDepth,
        });
        break;
      }
      case 'tool_approval_required':
      case 'tool_suspension_required':
      case 'question_pending':
      case 'plan_approval_required':
        emitDelegatedSubagentPendingEvent(port, event, {
          parentToolCallId,
          subagentSessionId: child.id,
        });
        break;
    }
  });
  emitSubagentEvent(port, {
    type: 'subagent_start',
    toolCallId: parentToolCallId,
    subagentSessionId: child.id,
    agentType,
    task: taskBody,
    modelId,
    depth: childDepth,
  });
  // The delegation outlives the delegating tool call, so its child runs
  // attach to a detached-lifetime delegation span in the parent trace
  // (subagent-tracing.ts). It covers the whole drive through settlement and
  // is started immediately before the try blocks whose last finally ends it.
  // A restart re-drive has no live parent span; its child runs start their
  // own trace.
  const delegationSpan = startDelegatedSubagentSpan(parentTracingContext, agentType);
  child._subagentTracingContext = delegationSpan !== undefined ? { currentSpan: delegationSpan } : undefined;
  try {
    // The deadline covers admission/dispatch as well as the eventual result.
    // Racing only the returned result leaves a storage/provider stall inside
    // signal() itself able to retain the parent reservation forever.
    const terminal = await waitForDelegationResult(
      child
        .signal({
          content: taskBody,
          admissionId: sessionDelegationAdmissionId(port, taskId),
          ...(requestContextApp !== undefined ? { requestContext: { app: requestContextApp } } : {}),
        })
        .then(dispatched => dispatched.result),
      deadlineAt,
    );
    if (terminal === DELEGATION_DEADLINE_REACHED) {
      // Commit cancellation before settlement so the child cannot continue
      // mutating tools after its parent task has released ownership.
      try {
        await child._cancelInternal({ reason: 'delegation_timeout', requestedBy: port.id }, port.id);
      } catch {
        // Child closure below remains the ownership prerequisite. A failed
        // cancel/close retains the durable link for restart reconciliation.
      }
      // The first cancel request wins: a targeted cancel committed before the
      // deadline still settles `cancelled`, as restart reconcile would.
      if (isParentRequestedSubagentCancel(child.getRecord().cancelRequest, port.id)) {
        outcome = 'cancelled';
        publicError = { ...DELEGATED_SUBAGENT_CANCELLED_ERROR };
      } else {
        outcome = 'failed';
        publicError = {
          code: 'harness.delegation_timeout',
          message: `The delegated subagent did not complete before its durable deadline (${deadlineAt})`,
        };
      }
    } else {
      rawResult = terminal;
      const finishReason = (rawResult as { finishReason?: string } | undefined)?.finishReason;
      const declaredReport = parseHarnessSubagentOutcomeReport(rawResult);
      const declaredTerminalText = parseHarnessTerminalToolResultText(
        (rawResult as { terminalToolResult?: unknown } | undefined)?.terminalToolResult,
      );
      const report = verifyHarnessSubagentTerminalCompletion(rawResult);
      // Provider finishReason is transport termination, not semantic task success.
      // A delegated task settles only from the exact framework-owned report
      // terminal envelope. Cancellation still overrides any concurrently
      // produced report; a terminal tool normally ends the provider step as
      // `tool-calls`, so requiring `stop` would reject every real first-call
      // terminal delivery.
      outcome =
        child.getRecord().cancelRequest === undefined && report !== undefined
          ? report.outcome
          : isParentRequestedSubagentCancel(child.getRecord().cancelRequest, port.id)
            ? 'cancelled'
            : 'failed';
      // The framework stopped the child at its per-type budget before it
      // reported an outcome: a typed partial result, not a generic failure.
      budgetExhausted =
        child.getRecord().cancelRequest === undefined && report === undefined
          ? (parseHarnessSubagentBudgetExhausted(rawResult) ?? child._internalSubagentBudgetExhaustion())
          : undefined;
      if (budgetExhausted !== undefined) {
        publicError = harnessSubagentBudgetExhaustedError(agentType, budgetExhausted);
      } else if (outcome !== 'completed') {
        publicError = {
          code:
            child.getRecord().cancelRequest !== undefined
              ? 'harness.subagent_cancelled'
              : (report?.issue?.code ??
                ((declaredReport !== undefined || declaredTerminalText !== undefined) && report === undefined
                  ? 'harness.subagent_evidence_unverified'
                  : report === undefined
                    ? 'harness.subagent_outcome_missing'
                    : 'harness.subagent_incomplete')),
          message:
            child.getRecord().cancelRequest !== undefined
              ? 'The delegated subagent was cancelled before it completed'
              : (report?.issue?.message ??
                (report === undefined
                  ? declaredReport !== undefined || declaredTerminalText !== undefined
                    ? 'The delegated subagent terminal completion did not match framework execution receipts'
                    : `The delegated subagent ended without ${HARNESS_SUBAGENT_OUTCOME_REPORT_TOOL_ID}`
                  : `The delegated subagent reported ${report.outcome} after provider finish reason "${finishReason ?? 'missing'}"`)),
        };
      }
    }
  } catch (error) {
    // Rejected signal/result, cancellation, expiry, or crash-orphan recovery.
    // Only this parent's own cancel of the child settles as skipped work, and
    // its error names the cancel rather than the rejection it caused.
    const parentCancelled = isParentRequestedSubagentCancel(child.getRecord().cancelRequest, port.id);
    outcome = parentCancelled ? 'cancelled' : 'failed';
    publicError = parentCancelled ? { ...DELEGATED_SUBAGENT_CANCELLED_ERROR } : projectHarnessPublicError(error);
  } finally {
    child._subagentAnswerDeltaListener = undefined;
    unsubProgress();
    // In the same step as the active entry goes, so the child never reads as
    // neither active nor settling here.
    port.settlingDelegations.add(child.id);
    port.activeSubagents.delete(`delegate:${child.id}`);
    child._subagentTracingContext = undefined;
  }
  // How the drive settled the delegation, recorded on the delegation span.
  // Anything but `settled` leaves the durable task to restart reconciliation.
  let settlement: DelegationDriveSettlement = 'error';
  try {
    // The parent's cancel overrides any report the child produced, so the
    // summary leaves it out, as restart reconcile does.
    const summary = summarizeHarnessSubagentResult(outcome === 'cancelled' ? undefined : rawResult, {
      isError: outcome !== 'completed',
      ...(publicError !== undefined ? { error: publicError } : {}),
      ...(budgetExhausted !== undefined ? { budgetExhausted } : {}),
    });
    // Close the terminal child BEFORE clearing its durable plan link. If the
    // process dies after close, recovery can still settle from signal evidence;
    // if close fails, retaining the link keeps ownership discoverable and
    // retryable instead of creating an unlinked active descendant.
    try {
      await child.close();
    } catch {
      settlement = 'close_failed';
      scheduleDelegationReconcileRetry(port, 0);
      return;
    }
    const reconciled = await reconcileDelegatedTask(port, taskId, child.id, outcome, summary, {
      childCancelRequest: child.getRecord().cancelRequest,
    });
    if (!reconciled) {
      settlement = 'not_reconciled';
      return;
    }
    settlement = 'settled';
    // A successful subagent_end is a durable settlement notification, not a
    // provider-stop notification. Emit only after child closure and the
    // fenced plan write both committed so reconnecting UI cannot show a
    // completed child while the authoritative task remains in_progress.
    emitSubagentEvent(port, {
      type: 'subagent_end',
      toolCallId: parentToolCallId,
      subagentSessionId: child.id,
      agentType,
      output: summarizeHarnessSubagentEventResult(summary),
      isError: outcome !== 'completed',
      durationMs: Math.max(0, Date.now() - startedAt),
      depth: childDepth,
    });
  } finally {
    port.settlingDelegations.delete(child.id);
    port.delegationDriverClaims.delete(child.id);
    internalReleaseSubagentExecution(port);
    endDelegatedSubagentSpan(delegationSpan, outcome, settlement);
  }
}

export async function cancelAndCloseDelegatedChildForDeadline(
  port: DelegationDriverSessionPort,
  childRecord: SessionRecord,
): Promise<boolean> {
  if (childRecord.closedAt !== undefined) return true;
  try {
    const child = await port.harness.session({ sessionId: childRecord.id, resourceId: port.resourceId });
    await child._cancelInternal({ reason: 'delegation_timeout', requestedBy: port.id }, port.id);
    await child.close();
    return true;
  } catch {
    return false;
  }
}
