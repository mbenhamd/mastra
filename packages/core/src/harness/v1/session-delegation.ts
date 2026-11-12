import type { HistoryLoadWatch } from '../../memory/history-load-watch';
import type { TracingContext } from '../../observability';
import { HARNESS_RUN_INTERRUPTED_ERROR_CODE } from '../../storage/domains/harness';
import type {
  AgentSignalResultEvidence,
  HarnessPlanTask,
  HarnessStorage,
  OperationAdmissionTombstone,
  SessionRecord,
} from '../../storage/domains/harness';
import { sha256CanonicalJsonChecked as sha256CanonicalJson } from './canonical-json';
import { HarnessSubagentDepthExceededError, HarnessValidationError } from './errors';
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
  PLAN_TASK_DELEGATED_BODY_MAX_BYTES,
  PLAN_TASK_MAX_NODES,
  planTaskReconcileDelegation,
  readDelegationAttemptMetadata,
  readPendingDelegationSettlementNotice,
} from './plan-task-session';
import type {
  DelegatedSettlementNoticeMode,
  DelegatedSubagentOutcome,
  PendingDelegationSettlementNotice,
  PlanTaskSessionPort,
} from './plan-task-session';
import type {
  ActiveSubagentState,
  JsonValue,
  Session,
  SessionLifecycleState,
  SignalAdmissionIdentity,
} from './session';
import type { QueueHead } from './session-queue-head';
import { harnessSubagentBudgetExhaustedError } from './subagent-budget';
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
 * delivery also reads.
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
