import type { AgentExecutionOptionsBase } from '../../agent/agent.types';
import { parseToolApprovalDecision } from '../../agent/tool-call-identity';
import type { ToolApprovalDecision } from '../../agent/tool-call-identity';
import {
  captureSuspendedToolSurfaceFenceLease,
  clearSuspendedToolSurfaceFence,
  readToolSurfaceFence,
} from '../../agent/tool-surface-fence';
import type { ToolSurfaceFence } from '../../agent/tool-surface-fence';
import type { RequestContext } from '../../request-context';
import { isHarnessWakeReservedKey } from '../../storage/domains/harness';
import type {
  AgentSignalResultEvidence,
  AgentSignalResultStatus,
  HarnessRuntimeDependencyRefs,
  HarnessStorage,
  HarnessTerminalCommitReceipt,
  HarnessTerminalHandoffError,
  HarnessTerminalIdentity,
  InboxResponseReceipt,
  PendingInteractionExpiryGeneration,
  PendingResume,
  PersistedRequestContextInput,
  QueueAdmissionReceipt,
  QueuedItem,
  SaveAttachmentReferenceInput,
  SessionRecord,
} from '../../storage/domains/harness';
import type { HarnessRunToolReceipt } from '../../storage/domains/harness/types';
import type { ObservationalMemorySourceWriteGuard } from '../../storage/types';
import type { FullOutput } from '../../stream/base/output';
import { ASK_USER_TOOL_ID, SUBMIT_PLAN_TOOL_ID } from '../../tools/builtin';
import { assertJsonValue, jsonValuesEqual, sha256CanonicalJsonChecked as sha256CanonicalJson } from './canonical-json';
import {
  HarnessBusyError,
  HarnessError,
  HarnessInboxItemNotFoundError,
  HarnessInboxResponseConflictError,
  HarnessPendingInteractionDiscardedError,
  HarnessPendingInteractionExpiredError,
  HarnessSessionCancelledError,
  HarnessSessionClosedError,
  HarnessSessionClosingError,
  HarnessSessionCorruptError,
  HarnessSessionDeletedError,
  HarnessSessionLockedError,
  HarnessStorageError,
  HarnessValidationError,
  redactPublicBoundaryRejection,
} from './errors';
import { projectHarnessPublicError, projectToolEventPayloadForJson } from './events';
import type { EmitInput, EventEmitter, HarnessEvent } from './events';
import type { Harness } from './harness';
import type { ResumeResponseMode, SessionLifecycleState, SignalSettlementAdmission, TokenUsage } from './session';
import {
  createDeferred,
  QUEUE_ACCEPTED_RECOVERY_STALE_MS,
  QUEUE_POST_RUN_FINALIZATION_RETRY_MS,
} from './session-durable-wait';
import type { Deferred } from './session-durable-wait';
import {
  publicErrorProjectionToError,
  QueuePostRunFinalizationPendingError,
  ResumeRecoveryStaleError,
} from './session-errors';
import type { MessageTokenLedger } from './session-message-token-ledger';
import type { QueueHead } from './session-queue-head';
import type { ReplacementToolSurfaceSlot } from './session-replacement-tool-surface-slot';
import type { RunOutcomeCache } from './session-run-outcome-cache';
import type { RejectedTerminalRun } from './session-terminal-finalizers';
import { clonePersistedRequestContext } from './session-turn-options';
import type { HarnessSubagentBudgetMeter } from './subagent-budget';
import type {
  AgentResult,
  DiscardPendingInteractionOptions,
  DiscardPendingInteractionResult,
  InboxResponseOptions,
  InboxResponseResult,
  RegisterPlanApprovalParams,
  RegisterQuestionParams,
  RegisterSandboxAccessParams,
} from './types';

export type QueueResumeRecoveryResult =
  | { status: 'none' }
  | { status: 'completed'; result: AgentResult }
  | { status: 'stale' };

export type InboxReceiptResponseOptions = Extract<InboxResponseOptions, { responseId: string }>;

export type LegacyInboxResponseOptions = Extract<InboxResponseOptions, { responseId?: undefined }>;

export const ASK_USER_TOOL_NAME = ASK_USER_TOOL_ID;

export const SUBMIT_PLAN_TOOL_NAME = SUBMIT_PLAN_TOOL_ID;

/**
 * [PF-7749] The pending-interaction slot as seen by a tool pre-registration:
 * `free` (nothing parked), `held` (this same call is already parked, so the
 * registration is a no-op), or `occupied` (another interaction holds it — a
 * second suspending call in the same step, or the answered interaction a
 * resumed run is still parked on). An occupied slot is left to the suspend
 * capture, which parks the call exactly as an unregistered one.
 */
export function preRegistrationSlot(
  current: PendingResume | undefined,
  kind: PendingResume['kind'],
  runId: string,
  toolCallId: string,
): 'free' | 'held' | 'occupied' {
  if (current === undefined) return 'free';
  return current.kind === kind && current.runId === runId && current.toolCallId === toolCallId ? 'held' : 'occupied';
}

export function compactJsonObject<T extends Record<string, unknown>>(value: T): Record<string, unknown> {
  return Object.fromEntries(Object.entries(value).filter(([, entry]) => entry !== undefined));
}

export const DISCARD_PENDING_INTERACTION_KINDS: ReadonlySet<PendingResume['kind']> = new Set([
  'tool-approval',
  'tool-suspension',
  'question',
  'plan-approval',
  'sandbox-access',
]);

export const MAX_DISCARD_PENDING_INTERACTION_REASON_LENGTH = 512;

/**
 * Audit copy of a cancelled session's pending interaction for its discard
 * receipt: cancellation keeps `pendingResume` as audit evidence, and discard
 * must clear it for the session to settle and delete.
 */
export function cancelledPendingAudit(
  pending: PendingResume,
  cancelRequest: NonNullable<SessionRecord['cancelRequest']>,
): NonNullable<InboxResponseReceipt['cancelledPendingResume']> {
  // Identity and audit fields only — never payloads, suspend data or args —
  // so the copy cannot push the claim write toward the record size budget.
  return {
    kind: pending.kind,
    itemId: pending.itemId ?? pending.toolCallId,
    runId: pending.runId,
    toolCallId: pending.toolCallId,
    requestedAt: pending.requestedAt,
    expiresAt: pending.expiresAt,
    ...(pending.toolName !== undefined ? { toolName: boundedAuditString(pending.toolName) } : {}),
    cancelRequest: {
      requestedAt: cancelRequest.requestedAt,
      ...(cancelRequest.reason !== undefined ? { reason: boundedAuditString(cancelRequest.reason) } : {}),
      ...(cancelRequest.requestedBy !== undefined
        ? { requestedBy: boundedAuditString(cancelRequest.requestedBy) }
        : {}),
    },
  };
}

const MAX_AUDIT_STRING_LENGTH = 256;

const AUDIT_TRUNCATION_MARKER = '…[truncated]';

function boundedAuditString(value: string): string {
  return value.length <= MAX_AUDIT_STRING_LENGTH
    ? value
    : `${value.slice(0, MAX_AUDIT_STRING_LENGTH - AUDIT_TRUNCATION_MARKER.length)}${AUDIT_TRUNCATION_MARKER}`;
}

/**
 * What `_expirePendingInteraction` did: recorded an expiry, finished a
 * discard-owned generation as its discard (no expiry evidence), or nothing.
 */
export type PendingExpiryOutcome = 'expired' | 'discarded' | 'none';

export function discardReceiptResult(
  status: 'discarded' | 'already_resolved',
  receipt: InboxResponseReceipt,
): DiscardPendingInteractionResult {
  return {
    status,
    itemId: receipt.itemId,
    kind: receipt.kind,
    responseId: receipt.responseId,
    discardedAt: receipt.discardedAt ?? receipt.updatedAt,
  };
}

/** One session can expose only one pending interaction at a time; keep a small
 * durable tail so late reconnect responses receive an exact expiry result
 * without allowing tombstones to grow without bound. */
const MAX_EXPIRED_PENDING_INTERACTIONS = 100;

// §10.2 receipts stay bounded on BOTH axes (PF-2250, live-diagnosed): the map
// itself is idempotency state, so only a recent window is needed, and a stored
// result must never carry provider payloads large enough to push the session
// record past a storage document limit (Azure Responses reasoning blobs made a
// single applied receipt ~400KB and broke every later saveSession with an
// HTTP 500).
export const MAX_INBOX_RESPONSE_RECEIPTS = 64;

export function pruneInboxResponseReceipts(
  receipts: Record<string, InboxResponseReceipt>,
): Record<string, InboxResponseReceipt> {
  const entries = Object.entries(receipts);
  if (entries.length <= MAX_INBOX_RESPONSE_RECEIPTS) return receipts;
  entries.sort((left, right) => (right[1].updatedAt ?? 0) - (left[1].updatedAt ?? 0));
  // `discardPendingInteraction` receipts are the durable replay and
  // crash-recovery record of a discard. Its claim converts the generation's
  // other receipts in the same write (sharing its timestamp), so plain recency
  // could evict it at once. Retain the newest few discard receipts first; the
  // remaining slots keep the ordinary recency order.
  const pinned = entries
    .filter(([, receipt]) => receipt.discardedAt !== undefined)
    .slice(0, MAX_RETAINED_DISCARD_RECEIPTS);
  const pinnedIds = new Set(pinned.map(([responseId]) => responseId));
  const rest = entries.filter(([responseId]) => !pinnedIds.has(responseId));
  return Object.fromEntries([...pinned, ...rest.slice(0, MAX_INBOX_RESPONSE_RECEIPTS - pinned.length)]);
}

/** Discard receipts retained ahead of recency pruning (bounded). */
const MAX_RETAINED_DISCARD_RECEIPTS = 8;

export function getOwnRecordValue<T>(record: Record<string, T> | undefined, key: string): T | undefined {
  if (!record || !Object.prototype.hasOwnProperty.call(record, key)) return undefined;
  return record[key];
}

export function appendExpiredPendingInteraction(
  current: SessionRecord['expiredPendingInteractions'],
  receipt: NonNullable<SessionRecord['expiredPendingInteractions']>[string],
): NonNullable<SessionRecord['expiredPendingInteractions']> {
  const next = { ...(current ?? {}), [receipt.generationKey]: receipt };
  const retained = Object.values(next)
    .sort((a, b) => b.expiredAt - a.expiredAt || b.generationKey.localeCompare(a.generationKey))
    .slice(0, MAX_EXPIRED_PENDING_INTERACTIONS);
  return Object.fromEntries(retained.map(entry => [entry.generationKey, entry]));
}

export function pendingInteractionGenerationKey(input: {
  kind: PendingResume['kind'];
  itemId?: string;
  runId: string;
  toolCallId: string;
  requestedAt: number;
}): string {
  return sha256CanonicalJson([input.kind, input.itemId ?? null, input.runId, input.toolCallId, input.requestedAt]);
}

/** The exact pending generation a discard is fenced to. */
export interface DiscardGeneration {
  kind: PendingResume['kind'];
  itemId: string;
  runId: string;
  toolCallId: string;
  pendingRequestedAt: number;
}

export function receiptMatchesDiscardGeneration(receipt: InboxResponseReceipt, generation: DiscardGeneration): boolean {
  return (
    receipt.kind === generation.kind &&
    receipt.itemId === generation.itemId &&
    receipt.runId === generation.runId &&
    receipt.toolCallId === generation.toolCallId &&
    receipt.pendingRequestedAt === generation.pendingRequestedAt
  );
}

/** Whether `receipt`'s generation was resolved by `discardPendingInteraction`. */
export function receiptGenerationDiscarded(record: SessionRecord, receipt: InboxResponseReceipt): boolean {
  return (
    findDiscardReceiptIn(record, {
      kind: receipt.kind,
      itemId: receipt.itemId,
      runId: receipt.runId,
      toolCallId: receipt.toolCallId,
      pendingRequestedAt: receipt.pendingRequestedAt,
    }) !== undefined
  );
}

/** The `discardPendingInteraction` receipt for `generation` in `record`, if any. */
export function findDiscardReceiptIn(
  record: SessionRecord,
  generation: DiscardGeneration,
): InboxResponseReceipt | undefined {
  return Object.values(record.inboxResponseReceipts ?? {}).find(
    receipt => receipt.discardedAt !== undefined && receiptMatchesDiscardGeneration(receipt, generation),
  );
}

export interface PendingInteractionsSessionPort {
  accountStagedTerminalUsage: (
    runId: string,
    staged: FullOutput<unknown>,
    queuedItemId?: string | undefined,
  ) => Promise<void>;
  assertLive: (_method: string) => void;
  assertOpenForTurn: (_method: string) => void;
  cancelTerminalizedPendingAdmission: (
    pending: PendingResume,
    error: { code: string; message: string },
    options?: { settleAsDiscard?: boolean | undefined },
  ) => Promise<void>;
  captureTurnRunId: (full: FullOutput<unknown>) => void;
  clearPendingDurableTurnFlushErrorIfRepaired: (full?: FullOutput<unknown> | undefined) => void;
  commitTerminalHandoff: (
    identity: HarnessTerminalIdentity,
    full: FullOutput<unknown> | RejectedTerminalRun,
    options: {
      modeId?: string | undefined;
      modelId?: string | undefined;
      onReceipt?: ((receipt: HarnessTerminalCommitReceipt) => void) | undefined;
      onFailure?: ((error: HarnessTerminalHandoffError) => void) | undefined;
    },
  ) => Promise<HarnessTerminalCommitReceipt | undefined>;
  commitWakeTerminalWithRetry: (
    identity: HarnessTerminalIdentity,
    outcome: FullOutput<unknown> | RejectedTerminalRun,
    options: { modeId?: string | undefined; modelId?: string | undefined },
    activeDeleted: Promise<never>,
    turnAbortSignal: AbortSignal | undefined,
  ) => Promise<void>;
  readonly currentAgentRequestContext: RequestContext<unknown> | undefined;
  readonly currentRunId: string | undefined;
  readonly currentTurnAbortController: AbortController | undefined;
  readonly currentTurnReplacementToolSurface:
    | { toolSurface: Pick<AgentExecutionOptionsBase<unknown>, 'toolsets' | 'toolsetsMode'>; fence: ToolSurfaceFence }
    | undefined;
  readonly currentTurnRequestContext: PersistedRequestContextInput | undefined;
  readonly currentTurnSourceWriteGuard: ObservationalMemorySourceWriteGuard | undefined;
  readonly currentTurnYolo: boolean;
  deferQueuedTurnRetry: (err: QueuePostRunFinalizationPendingError) => void;
  emit: (event: EmitInput) => HarnessEvent;
  emitAbortedToolEnds: ({ force }?: { force?: boolean | undefined }) => void;
  emitAgentEnd: (opts: {
    runId: string | undefined;
    finishReason: 'error' | 'complete' | 'aborted' | 'suspended';
    full?: FullOutput<unknown> | undefined;
  }) => void;
  readonly emitter: EventEmitter;
  emitTurnEvent: (event: EmitInput) => HarnessEvent;
  ensureQueuedItemContext: (queuedItemId: string) => void;
  finalizeQueuedRunCompletion: (
    item: QueuedItem,
    full: FullOutput<unknown>,
    modeId?: string | undefined,
    activeTurnWaiter?: Promise<never> | undefined,
    opts?: { skipTokenAccounting?: boolean | undefined; onAgentEnd?: (() => void) | undefined },
  ) => Promise<FullOutput<unknown>>;
  finalizeResumedTurnOutcome: (
    pending: PendingResume,
    full: FullOutput<unknown>,
    options: {
      responseId?: string | undefined;
      pendingQueuedItemId?: string | undefined;
      completingQueuedItemId?: string | undefined;
      modeFlipTarget?: string | undefined;
      previousModeId: string;
      resumeModeId: string;
      resumeModelId?: string | undefined;
      deletedTurnWaiter: Promise<never>;
      turnAbortSignal?: AbortSignal | undefined;
    },
  ) => Promise<void>;
  flushUpdate: (
    update: (prev: SessionRecord) => SessionRecord,
    opts?:
      | {
          attachmentReferences?: SaveAttachmentReferenceInput[] | undefined;
          ifVersion?: number | undefined;
          tokenUsageDelta?: TokenUsage | (() => TokenUsage | undefined) | undefined;
          skipIfUnchanged?: boolean | undefined;
          keepPendingResume?: boolean | undefined;
        }
      | undefined,
  ) => Promise<void>;
  readonly harness: Harness;
  readonly id: string;
  markQueuedPostRunFinalized: (
    queuedItemId: string,
    opts?: { tokenUsageDelta?: TokenUsage | undefined },
  ) => Promise<void>;
  maybeDrainQueue: () => Promise<void>;
  readonly messageTokenLedger: MessageTokenLedger;
  modelIdForQueuedItem: (queuedItemId: string | undefined) => string;
  notifyMaybeIdle: () => void;
  readonly ownedSignalTurnOrigin: { runId: string; signalId: string } | undefined;
  pendingInteractionExpiryAt: number | undefined;
  pendingInteractionExpiryTimer: NodeJS.Timeout | undefined;
  persistTokenUsageDelta: (delta: TokenUsage | undefined) => Promise<void>;
  queuedResumeRecoveryTimer: NodeJS.Timeout | undefined;
  readonly queueHead: QueueHead;
  raceActiveTurnWaiter: <T>(promise: Promise<T>, activeTurnWaiter?: Promise<never> | undefined) => Promise<T>;
  readonly record: SessionRecord;
  readonly replacementToolSurface: ReplacementToolSurfaceSlot;
  readonly resourceId: string;
  resume: (
    expectedKind: 'question' | 'tool-approval' | 'tool-suspension' | 'plan-approval' | 'sandbox-access',
    resumeData: unknown,
    responseOptions?: InboxResponseOptions,
  ) => Promise<AgentResult | InboxResponseResult>;
  readonly runOutcomes: RunOutcomeCache;
  readonly runToolReceipts: Map<
    string,
    {
      receipts: HarnessRunToolReceipt[];
      exact: Set<string>;
      replaceable: Set<string>;
      bytes: number;
      overflow: boolean;
    }
  >;
  settleSignalResult: (
    signalId: string,
    outcome:
      | { status: 'completed'; runId: string; result: AgentResult }
      | { status: 'failed'; runId?: string | undefined; error: { code: string; message: string } },
    admission?: SignalSettlementAdmission | undefined,
    options?: { strict?: boolean | undefined } | undefined,
  ) => Promise<AgentSignalResultEvidence | undefined>;
  stageCompletedTerminalOutput: (
    runId: string,
    full: FullOutput<unknown>,
    opts?: { tolerateUnknownRun?: boolean | undefined; provisional?: boolean | undefined },
  ) => Promise<void>;
  readonly state: SessionLifecycleState;
  readonly storage: HarnessStorage;
  subagentBudgetMeter: () => HarnessSubagentBudgetMeter | undefined;
  readonly subagentDepth: number;
  readonly subagentParentToolCallId: string | undefined;
  terminalFailure: (
    err: unknown,
    onFailure?: ((error: HarnessTerminalHandoffError) => void) | undefined,
    runId?: string | undefined,
  ) => HarnessTerminalHandoffError;
  readonly threadId: string;
  withActiveDeletedWaiter: <T>(fn: (activeTurnWaiter: Promise<never>) => Promise<T>) => Promise<T>;
  writeQueueSignalResultEvidence: (status: AgentSignalResultStatus) => Promise<void>;
}

function parseDiscardPendingInteractionOptions(opts: unknown): DiscardPendingInteractionOptions {
  if (opts === null || typeof opts !== 'object') {
    throw new HarnessValidationError('discardPendingInteraction()', 'options must be an object');
  }
  const read = (key: string): unknown => getOwnRecordValue(opts as Record<string, unknown>, key);
  const requireString = (key: string, maxLength?: number): string => {
    const value = read(key);
    if (typeof value !== 'string' || value.length === 0) {
      throw new HarnessValidationError(`discardPendingInteraction().${key}`, `${key} must be a non-empty string`);
    }
    if (maxLength !== undefined && value.length > maxLength) {
      throw new HarnessValidationError(
        `discardPendingInteraction().${key}`,
        `${key} must be at most ${maxLength} characters`,
      );
    }
    return value;
  };
  const kind = read('kind');
  if (typeof kind !== 'string' || !DISCARD_PENDING_INTERACTION_KINDS.has(kind as PendingResume['kind'])) {
    throw new HarnessValidationError('discardPendingInteraction().kind', 'kind must be a pending interaction kind');
  }
  const pendingRequestedAt = read('pendingRequestedAt');
  if (typeof pendingRequestedAt !== 'number' || !Number.isSafeInteger(pendingRequestedAt) || pendingRequestedAt < 0) {
    throw new HarnessValidationError(
      'discardPendingInteraction().pendingRequestedAt',
      'pendingRequestedAt must be a non-negative integer',
    );
  }
  return {
    kind: kind as PendingResume['kind'],
    itemId: requireString('itemId'),
    runId: requireString('runId'),
    toolCallId: requireString('toolCallId'),
    pendingRequestedAt,
    responseId: requireString('responseId'),
    reason: requireString('reason', MAX_DISCARD_PENDING_INTERACTION_REASON_LENGTH),
  };
}

function discardGeneration(input: DiscardPendingInteractionOptions): DiscardGeneration {
  return {
    kind: input.kind,
    itemId: input.itemId,
    runId: input.runId,
    toolCallId: input.toolCallId,
    pendingRequestedAt: input.pendingRequestedAt,
  };
}

function pendingMatchesDiscardGeneration(pending: PendingResume, generation: DiscardGeneration): boolean {
  return (
    pending.kind === generation.kind &&
    (pending.itemId ?? pending.toolCallId) === generation.itemId &&
    pending.runId === generation.runId &&
    pending.toolCallId === generation.toolCallId &&
    pending.requestedAt === generation.pendingRequestedAt
  );
}

/**
 * The `discardPendingInteraction` receipt whose claim (`expiryStartedAt`) is
 * stamped on `pending` in `record`, if that generation is discard-owned.
 */
function discardClaimReceipt(record: SessionRecord, pending: PendingResume): InboxResponseReceipt | undefined {
  if (pending.expiryStartedAt === undefined) return undefined;
  const generation: DiscardGeneration = {
    kind: pending.kind,
    itemId: pending.itemId ?? pending.toolCallId,
    runId: pending.runId,
    toolCallId: pending.toolCallId,
    pendingRequestedAt: pending.requestedAt,
  };
  return Object.values(record.inboxResponseReceipts ?? {}).find(
    receipt => receipt.discardedAt === pending.expiryStartedAt && receiptMatchesDiscardGeneration(receipt, generation),
  );
}

/** Spend to persist with a pending interaction, so a restart cannot reset it. */
export function subagentBudgetSpentProjection(
  port: PendingInteractionsSessionPort,
): Pick<PendingResume, 'subagentBudgetSpent'> {
  const meter = port.subagentBudgetMeter();
  return meter === undefined ? {} : { subagentBudgetSpent: meter.spent() };
}

export function pendingInteractionExpiresAt(port: PendingInteractionsSessionPort, pending: PendingResume): number {
  if (
    !Number.isSafeInteger(pending.expiresAt) ||
    pending.expiresAt < 0 ||
    !Number.isSafeInteger(pending.requestedAt) ||
    pending.requestedAt < 0 ||
    pending.expiresAt < pending.requestedAt
  ) {
    throw new HarnessSessionCorruptError({
      reason: 'pending_state_corrupt',
      sessionId: port.id,
      resourceId: port.resourceId,
      threadId: port.threadId,
    });
  }
  return pending.expiresAt;
}

export function pendingInteractionDueAt(port: PendingInteractionsSessionPort, pending: PendingResume): number {
  if (pending.resumedAt === undefined) return pendingInteractionExpiresAt(port, pending);
  if (pending.resumeRecoveryAt === undefined) {
    // At-least-once resume marker without a recovery stamp: rows written by
    // builds that predate `resumeRecoveryAt` (the admission CAS now stamps
    // both fields atomically, so current code can no longer produce this
    // shape). The marker stays RECOVERABLE — its effective deadline is the
    // admission time plus the accepted-recovery stale window, exactly what
    // the stamping writer would have persisted. Classifying it corrupt would
    // permanently wedge the session on hydrate, the precise outcome the
    // crash-mid-resume recovery contract forbids (queue not parked).
    if (!Number.isSafeInteger(pending.resumedAt) || pending.resumedAt < 0) {
      throw new HarnessSessionCorruptError({
        reason: 'pending_state_corrupt',
        sessionId: port.id,
        resourceId: port.resourceId,
        threadId: port.threadId,
      });
    }
    return pending.resumedAt + QUEUE_ACCEPTED_RECOVERY_STALE_MS;
  }
  if (!Number.isSafeInteger(pending.resumeRecoveryAt) || pending.resumeRecoveryAt < pending.resumedAt) {
    throw new HarnessSessionCorruptError({
      reason: 'pending_state_corrupt',
      sessionId: port.id,
      resourceId: port.resourceId,
      threadId: port.threadId,
    });
  }
  return pending.resumeRecoveryAt;
}

export function pendingInteractionGeneration(
  port: PendingInteractionsSessionPort,
  pending: PendingResume,
): PendingInteractionExpiryGeneration {
  const dueAt = pendingInteractionDueAt(port, pending);
  return {
    harnessName: port.record.harnessName,
    sessionId: port.id,
    resourceId: port.resourceId,
    kind: pending.kind,
    itemId: pending.itemId ?? pending.toolCallId,
    runId: pending.runId,
    toolCallId: pending.toolCallId,
    requestedAt: pending.requestedAt,
    dueAt,
    expiresAt: pending.expiresAt,
    ...(pending.expiryStartedAt !== undefined ? { expiryStartedAt: pending.expiryStartedAt } : {}),
    ...(pending.resumedAt !== undefined ? { resumedAt: pending.resumedAt } : {}),
    ...(pending.resumeRecoveryAt !== undefined ? { resumeRecoveryAt: pending.resumeRecoveryAt } : {}),
    ...(pending.resumeRecoveryStartedAt !== undefined
      ? { resumeRecoveryStartedAt: pending.resumeRecoveryStartedAt }
      : {}),
  };
}

/**
 * Freeze this interaction's deadline onto the pending row. The TTL is
 * resolved per kind (`sessions.pendingInteractionTtlMsByKind`, falling back
 * to `sessions.pendingInteractionTtlMs`) at capture time only, so a later
 * config change never moves an already-parked interaction's deadline and a
 * restart reads the same `expiresAt` back out of storage.
 */
export function newPendingInteractionTiming(
  port: PendingInteractionsSessionPort,
  kind: PendingResume['kind'],
): Pick<PendingResume, 'requestedAt' | 'expiresAt'> {
  const requestedAt = Date.now();
  return {
    requestedAt,
    expiresAt: requestedAt + port.harness._internalPendingInteractionTtlMsForKind(kind),
  };
}

export function clearPendingInteractionExpiryTimer(port: PendingInteractionsSessionPort): void {
  if (port.pendingInteractionExpiryTimer !== undefined) {
    clearTimeout(port.pendingInteractionExpiryTimer);
    port.pendingInteractionExpiryTimer = undefined;
  }
  port.pendingInteractionExpiryAt = undefined;
}

/**
 * The timer is only a liveness wake-up. The callback re-checks the exact
 * pending identity inside `_flushUpdate`, which is the authority for an
 * expiry-vs-response race and remains correct after a restart.
 */
export function syncPendingInteractionExpiryTimer(port: PendingInteractionsSessionPort): void {
  const pending = port.record.pendingResume;
  if (port.state !== 'live' || pending === undefined) {
    clearPendingInteractionExpiryTimer(port);
    return;
  }
  const dueAt = pendingInteractionDueAt(port, pending);
  if (port.pendingInteractionExpiryTimer !== undefined && port.pendingInteractionExpiryAt === dueAt) return;
  clearPendingInteractionExpiryTimer(port);
  port.pendingInteractionExpiryAt = dueAt;
  const identity = pendingInteractionGeneration(port, pending);
  port.pendingInteractionExpiryTimer = setTimeout(
    () => {
      port.pendingInteractionExpiryTimer = undefined;
      port.pendingInteractionExpiryAt = undefined;
      void reconcilePendingInteractionDeadlineGeneration(port, identity).catch(err => {
        console.error('[harness/v1] pending interaction expiry persistence failed:', err);
        if (port.state !== 'live') return;
        port.emitter.emit({
          type: 'storage_error',
          operation: 'session_save',
          retryable: true,
          error: { code: 'harness.storage', message: 'pending interaction expiry persistence failed' },
          resourceId: port.resourceId,
          threadId: port.threadId,
          harnessName: port.record.harnessName,
        } as EmitInput);
        // A transient save failure must retry without requiring another user
        // action. Back off before re-arming: the persisted deadline is now in
        // the past, so an immediate `_sync` would otherwise create a hot
        // zero-delay retry loop while storage remains unavailable.
        port.pendingInteractionExpiryAt = dueAt;
        port.pendingInteractionExpiryTimer = setTimeout(() => {
          port.pendingInteractionExpiryTimer = undefined;
          port.pendingInteractionExpiryAt = undefined;
          syncPendingInteractionExpiryTimer(port);
        }, 1_000);
        port.pendingInteractionExpiryTimer.unref?.();
      });
    },
    pending.expiryStartedAt !== undefined || pending.resumeRecoveryStartedAt !== undefined
      ? 1_000
      : Math.max(0, dueAt - Date.now()),
  );
  port.pendingInteractionExpiryTimer.unref?.();
}

export async function reconcilePendingInteractionExpiryOnHydrate(port: PendingInteractionsSessionPort): Promise<void> {
  const pending = port.record.pendingResume;
  if (pending === undefined) {
    clearPendingInteractionExpiryTimer(port);
    return;
  }
  if (pending.resumedAt !== undefined) {
    const recovery = await maybeRecoverStaleQueuedResume(port, { kickDrainOnStale: false });
    if (recovery.status !== 'none') return;
    if (pending.resumeRecoveryAt === undefined) {
      // Legacy at-least-once marker (no recovery stamp): resolution belongs
      // to the interactive lanes — the next respond surfaces the
      // deterministic `harness.resume_recovery_stale` outcome and durably
      // clears the marker. Hydration only adopts; it neither terminalizes
      // the marker nor races the caller's next respond with an expiry timer.
      return;
    }
  }
  const dueAt = pendingInteractionDueAt(port, pending);
  if (Date.now() < dueAt) {
    syncPendingInteractionExpiryTimer(port);
    return;
  }
  await reconcilePendingInteractionDeadlineGeneration(port, pendingInteractionGeneration(port, pending));
}

export async function internalExpirePendingInteractionGeneration(
  port: PendingInteractionsSessionPort,
  expected: PendingInteractionExpiryGeneration,
  opts: { drainQueue?: boolean } = {},
): Promise<boolean> {
  if (
    expected.harnessName !== port.record.harnessName ||
    expected.sessionId !== port.id ||
    expected.resourceId !== port.resourceId
  ) {
    return false;
  }
  return reconcilePendingInteractionDeadlineGeneration(port, expected, opts);
}

export async function reconcilePendingInteractionDeadlineGeneration(
  port: PendingInteractionsSessionPort,
  expected: PendingInteractionExpiryGeneration,
  opts: { drainQueue?: boolean; whileClosing?: boolean } = {},
): Promise<boolean> {
  // A `discardPendingInteraction` claim whose clear did not commit (crash or
  // storage failure between its phases) completes as that discard, not as
  // an expiry. Only rows carrying a matching discard receipt reach this.
  const current = port.record.pendingResume;
  const discardClaim = current === undefined ? undefined : discardClaimForPending(port, current);
  if (
    current !== undefined &&
    discardClaim !== undefined &&
    current.kind === expected.kind &&
    (current.itemId ?? current.toolCallId) === expected.itemId &&
    current.runId === expected.runId &&
    current.toolCallId === expected.toolCallId &&
    current.requestedAt === expected.requestedAt
  ) {
    return completeDiscardedPendingInteraction(port, current, discardClaim, { drainQueue: opts.drainQueue });
  }
  if (expected.resumedAt !== undefined) {
    return recoverStaleResumeAdmission(port, expected, opts);
  }
  const outcome = await expirePendingInteraction(
    port,
    {
      kind: expected.kind,
      itemId: expected.itemId,
      runId: expected.runId,
      toolCallId: expected.toolCallId,
      requestedAt: expected.requestedAt,
      expiresAt: expected.expiresAt,
    },
    opts,
  );
  return outcome !== 'none';
}

export async function expirePendingInteraction(
  port: PendingInteractionsSessionPort,
  expected: {
    kind: PendingResume['kind'];
    itemId: string;
    runId: string;
    toolCallId: string;
    requestedAt: number;
    expiresAt: number;
  },
  opts: { drainQueue?: boolean; whileClosing?: boolean } = {},
): Promise<PendingExpiryOutcome> {
  // A close may expire an overdue interaction: the parked turn could never
  // be answered once the session closes.
  if (port.state !== 'live' && !(opts.whileClosing === true && port.state === 'closing')) return 'none';
  const expiredAt = Date.now();
  if (expiredAt < expected.expiresAt) {
    syncPendingInteractionExpiryTimer(port);
    return 'none';
  }
  const expiryError = new HarnessPendingInteractionExpiredError(port.id, expected.itemId, expected.expiresAt);
  const projectedError = projectHarnessPublicError(expiryError);
  const generationKey = pendingInteractionGenerationKey(expected);
  let expiryStarted = false;
  let expiredPending: PendingResume | undefined;
  let failedQueueReceipt: QueueAdmissionReceipt | undefined;
  let discardOwned: { pending: PendingResume; receipt: InboxResponseReceipt } | undefined;

  await port.flushUpdate(prev => {
    expiryStarted = false;
    expiredPending = undefined;
    failedQueueReceipt = undefined;
    discardOwned = undefined;
    const current = prev.pendingResume;
    if (
      current === undefined ||
      (current.resumedAt !== undefined && current.expiryStartedAt === undefined) ||
      current.kind !== expected.kind ||
      current.runId !== expected.runId ||
      current.toolCallId !== expected.toolCallId ||
      (current.itemId ?? current.toolCallId) !== expected.itemId ||
      current.requestedAt !== expected.requestedAt
    ) {
      return prev;
    }
    // A `discardPendingInteraction` claim owns this generation (its claim
    // moved `expiresAt`, so an expiry captured earlier still lands here):
    // never record expiry evidence for it; finish it as the discard below.
    const discardReceipt = discardClaimReceipt(prev, current);
    if (discardReceipt !== undefined) {
      discardOwned = { pending: current, receipt: discardReceipt };
      return prev;
    }
    if (pendingInteractionExpiresAt(port, current) !== expected.expiresAt) return prev;

    expiryStarted = true;
    const expiryStartedAt = current.expiryStartedAt ?? expiredAt;
    expiredPending = { ...current, expiryStartedAt };
    const next: SessionRecord = { ...prev, pendingResume: expiredPending };
    next.expiredPendingInteractions = appendExpiredPendingInteraction(prev.expiredPendingInteractions, {
      generationKey,
      itemId: expected.itemId,
      kind: expected.kind,
      runId: expected.runId,
      toolCallId: expected.toolCallId,
      requestedAt: expected.requestedAt,
      expiresAt: expected.expiresAt,
      expiredAt: expiryStartedAt,
      error: projectedError,
    });

    for (const [responseId, receipt] of Object.entries(prev.inboxResponseReceipts ?? {})) {
      if (
        receipt.itemId !== expected.itemId ||
        receipt.kind !== expected.kind ||
        receipt.runId !== expected.runId ||
        receipt.toolCallId !== expected.toolCallId ||
        receipt.pendingRequestedAt !== expected.requestedAt ||
        receipt.status !== 'failed' ||
        receipt.retryable !== true
      ) {
        continue;
      }
      next.inboxResponseReceipts = {
        ...(next.inboxResponseReceipts ?? prev.inboxResponseReceipts ?? {}),
        [responseId]: {
          ...receipt,
          status: 'failed',
          error: projectedError,
          retryable: false,
          failedAt: receipt.failedAt ?? expiryStartedAt,
          updatedAt: expiryStartedAt,
        },
      };
    }

    if (current.queuedItemId !== undefined) {
      next.pendingQueue = (prev.pendingQueue ?? []).filter(item => item.id !== current.queuedItemId);
      const receipt = prev.queueAdmissionReceipts?.[current.queuedItemId];
      failedQueueReceipt = receipt;
      if (receipt && receipt.status !== 'completed' && receipt.status !== 'failed' && receipt.status !== 'dead') {
        failedQueueReceipt = {
          ...receipt,
          status: 'failed',
          error: projectedError,
          failedAt: receipt.failedAt ?? expiredAt,
          updatedAt: expiredAt,
        };
        next.queueAdmissionReceipts = {
          ...(prev.queueAdmissionReceipts ?? {}),
          [current.queuedItemId]: failedQueueReceipt,
        };
      }
    }
    return next;
  });

  if (discardOwned !== undefined) {
    const owned: { pending: PendingResume; receipt: InboxResponseReceipt } = discardOwned;
    await completeDiscardedPendingInteraction(port, owned.pending, owned.receipt, { drainQueue: opts.drainQueue });
    return 'discarded';
  }
  if (!expiryStarted || expiredPending === undefined) return 'none';
  port.replacementToolSurface.drop(expiredPending.runId);
  const activeController = port.currentTurnAbortController;
  if (activeController !== undefined) activeController.abort(expiryError);

  const queuedItemId = expiredPending.queuedItemId;
  if (queuedItemId !== undefined) {
    const settledReceipt = port.record.queueAdmissionReceipts?.[queuedItemId] ?? failedQueueReceipt;
    if (settledReceipt?.signalId !== undefined) {
      await port.writeQueueSignalResultEvidence({
        status: 'failed',
        signalId: settledReceipt.signalId,
        runId: settledReceipt.runId,
        error: projectedError,
      });
    }
    const resolver = port.queueHead.takeResolver(queuedItemId);
    if (resolver) {
      resolver.reject(expiryError);
    }
    port.queueHead.releaseIf(queuedItemId);
  }

  if (expiredPending.originSignalId !== undefined) {
    await port.settleSignalResult(expiredPending.originSignalId, {
      status: 'failed',
      runId: expiredPending.runId,
      error: projectedError,
    });
  }
  // An expired suspension may have deferred a terminal admission — settle the
  // pending grant alongside the signal evidence so the handoff cannot strand
  // pending forever behind an interaction that no longer exists.
  await port.cancelTerminalizedPendingAdmission(expiredPending, projectedError);

  // Phase two clears the still-discoverable pending row only after every
  // durable queue/signal settlement has succeeded. A crash or storage error
  // before this CAS leaves `expiryStartedAt` indexed for hydrate/sweep retry.
  let finalized = false;
  await port.flushUpdate(prev => {
    finalized = false;
    const current = prev.pendingResume;
    if (
      current === undefined ||
      current.kind !== expected.kind ||
      current.runId !== expected.runId ||
      current.toolCallId !== expected.toolCallId ||
      (current.itemId ?? current.toolCallId) !== expected.itemId ||
      current.requestedAt !== expected.requestedAt ||
      current.expiresAt !== expected.expiresAt ||
      current.expiryStartedAt !== expiredPending!.expiryStartedAt
    ) {
      return prev;
    }
    finalized = true;
    const next: SessionRecord = { ...prev };
    delete next.pendingResume;
    return next;
  });
  if (!finalized) return 'none';

  if (queuedItemId !== undefined) {
    const settledReceipt = port.record.queueAdmissionReceipts?.[queuedItemId] ?? failedQueueReceipt;
    port.emit({
      type: 'queue_failed',
      queuedItemId,
      ...(settledReceipt?.runId ? { runId: settledReceipt.runId } : {}),
      ...(settledReceipt?.signalId ? { signalId: settledReceipt.signalId } : {}),
      ...(settledReceipt?.admissionId ? { admissionId: settledReceipt.admissionId } : {}),
      error: projectedError,
    });
  }
  port.emitAbortedToolEnds({ force: true });
  port.emitTurnEvent({
    type: 'resume_failed',
    runId: expiredPending.runId,
    retryable: false,
    error: projectedError,
  });
  // An already-running pre-registered ask/plan tool owns its terminal event;
  // the abort above makes that path finish. A parked suspension has no live
  // owner, so expiry closes its run here.
  if (activeController === undefined) {
    releaseParkedRun(port, expiredPending);
    port.emitAgentEnd({ runId: expiredPending.runId, finishReason: 'error' });
  }
  port.notifyMaybeIdle();
  if (port.state === 'live' && opts.drainQueue !== false) void port.maybeDrainQueue();
  return 'expired';
}

/**
 * Resolve a response admission whose owner disappeared after persisting
 * `resumedAt` but before committing terminal provider evidence. Re-dispatch
 * is intentionally forbidden: the provider/tool may already have performed
 * side effects. Instead, a two-phase exact-generation transition fails all
 * related receipts/signals and terminalizes the abandoned run.
 */
export async function recoverStaleResumeAdmission(
  port: PendingInteractionsSessionPort,
  expected: PendingInteractionExpiryGeneration,
  opts: { drainQueue?: boolean } = {},
): Promise<boolean> {
  if (expected.resumedAt === undefined) return false;
  // Legacy at-least-once markers carry no recovery stamp; their effective
  // deadline is the synthesized one `_pendingInteractionDueAt` folded into
  // `dueAt` (admission + stale window). Stamped rows keep the exact-identity
  // requirement between the generation's dueAt and the persisted stamp.
  const expectedRecoveryDueAt = expected.resumeRecoveryAt ?? expected.resumedAt + QUEUE_ACCEPTED_RECOVERY_STALE_MS;
  if (expected.dueAt !== expectedRecoveryDueAt) {
    return false;
  }
  const currentPending = port.record.pendingResume;
  let expectedQueuedItemId: string | undefined;
  if (
    currentPending?.resumedAt === expected.resumedAt &&
    currentPending.runId === expected.runId &&
    currentPending.toolCallId === expected.toolCallId
  ) {
    const queuedItemId = queuedItemIdForPendingResume(port, currentPending);
    expectedQueuedItemId = queuedItemId;
    if (queuedItemId !== undefined && port.record.queueAdmissionReceipts?.[queuedItemId]?.status === 'completed') {
      const recovery = await maybeRecoverStaleQueuedResume(port, { kickDrainOnStale: opts.drainQueue !== false });
      return recovery.status === 'completed';
    }
  }
  if (Date.now() < expectedRecoveryDueAt) {
    syncPendingInteractionExpiryTimer(port);
    return false;
  }
  // A legitimately slow resume still owns this live session. Only a cold
  // owner (or a hydrated session with no active turn) may classify the
  // persisted admission as abandoned.
  if (port.currentTurnAbortController !== undefined) {
    port.pendingInteractionExpiryAt = expectedRecoveryDueAt;
    port.pendingInteractionExpiryTimer = setTimeout(() => {
      port.pendingInteractionExpiryTimer = undefined;
      port.pendingInteractionExpiryAt = undefined;
      syncPendingInteractionExpiryTimer(port);
    }, 1_000);
    port.pendingInteractionExpiryTimer.unref?.();
    return false;
  }

  // A committed admission or staged completed output means the provider run
  // finished before its owner died. Recover it and finish settlement and
  // bookkeeping instead of failing the interaction as abandoned.
  const recoveryIncarnation = port.record.sessionIncarnation;
  if (
    port.storage.supportsTerminalHandoff &&
    recoveryIncarnation !== undefined &&
    currentPending !== undefined &&
    currentPending.resumedAt === expected.resumedAt &&
    currentPending.runId === expected.runId &&
    currentPending.toolCallId === expected.toolCallId
  ) {
    const boundAdmission = await port.storage.loadTerminalAdmissionByRun({
      harnessName: port.record.harnessName,
      sessionId: port.id,
      runId: expected.runId,
      sessionIncarnation: recoveryIncarnation,
    });
    if (boundAdmission?.status === 'committed' || boundAdmission?.status === 'pending') {
      const stored = await port.storage.loadMessageResultEvidence({
        harnessName: port.record.harnessName,
        sessionId: port.id,
        resourceId: port.resourceId,
        threadId: port.threadId,
        signalId: boundAdmission.signalId,
      });
      const parked = port.record.pendingResume;
      if (
        stored !== null &&
        'status' in stored &&
        ((stored.status === 'completed' && stored.result !== undefined) ||
          (stored.status === 'pending' && stored.stagedResult !== undefined)) &&
        parked !== undefined &&
        parked.runId === expected.runId &&
        parked.toolCallId === expected.toolCallId &&
        parked.resumedAt === expected.resumedAt
      ) {
        const full = (stored.status === 'completed' ? stored.result : stored.stagedResult) as FullOutput<unknown>;
        // Repopulate the local run cache so the per-generation usage marker
        // still dedupes a concurrent settlement retry.
        port.runOutcomes.remember(parked.runId, { ok: true, full });
        const pendingQueuedItemId = queuedItemIdForPendingResume(port, parked);
        const resumeRuntimeDependencies = runtimeDependenciesForPendingResume(port, parked);
        // The resume-data payload lived in the dead caller, but a
        // `responseId` respond persisted it on the generation's inbox
        // receipt — recover an approved plan transition from there (and the
        // pending's declared fallback) instead of clearing the interaction
        // while leaving the old mode and permission policy active.
        let modeFlipTarget: string | undefined;
        if (parked.kind === 'plan-approval') {
          const generationReceipt = Object.values(port.record.inboxResponseReceipts ?? {}).find(
            candidate =>
              candidate.status === 'accepted' &&
              candidate.kind === parked.kind &&
              candidate.runId === parked.runId &&
              candidate.toolCallId === parked.toolCallId &&
              candidate.pendingRequestedAt === parked.requestedAt &&
              candidate.itemId === (parked.itemId ?? parked.toolCallId),
          );
          try {
            modeFlipTarget = resolvePlanApprovalModeFlipTarget(
              port,
              parked,
              generationReceipt?.response as { approved?: boolean; transitionToMode?: string } | undefined,
            );
          } catch {
            // The target mode vanished from config since admission —
            // recovery must stay total; skip the flip rather than brick the
            // session on a mode that no longer exists.
          }
        }
        await port.withActiveDeletedWaiter(async activeTurnWaiter => {
          await port.finalizeResumedTurnOutcome(parked, full, {
            pendingQueuedItemId,
            completingQueuedItemId: expectedQueuedItemId,
            modeFlipTarget,
            previousModeId: port.record.modeId,
            resumeModeId: modeIdForPendingResume(port, parked),
            resumeModelId: resumeRuntimeDependencies.modelId,
            deletedTurnWaiter: activeTurnWaiter,
          });
        });
        if (expectedQueuedItemId !== undefined) void port.maybeDrainQueue();
        return true;
      }
    }
  }

  const recoveryError = new ResumeRecoveryStaleError();
  const projectedError = projectHarnessPublicError(recoveryError);
  const recoveryAt = Date.now();
  let recoveryStarted = false;
  let recoveringPending: PendingResume | undefined;
  let recoveringQueuedItemId: string | undefined;
  let failedQueueReceipt: QueueAdmissionReceipt | undefined;

  await port.flushUpdate(prev => {
    recoveryStarted = false;
    recoveringPending = undefined;
    recoveringQueuedItemId = undefined;
    failedQueueReceipt = undefined;
    const current = prev.pendingResume;
    if (
      current === undefined ||
      current.kind !== expected.kind ||
      current.runId !== expected.runId ||
      current.toolCallId !== expected.toolCallId ||
      (current.itemId ?? current.toolCallId) !== expected.itemId ||
      current.requestedAt !== expected.requestedAt ||
      current.expiresAt !== expected.expiresAt ||
      current.expiryStartedAt !== expected.expiryStartedAt ||
      current.resumedAt !== expected.resumedAt ||
      current.resumeRecoveryAt !== expected.resumeRecoveryAt ||
      current.resumeRecoveryStartedAt !== expected.resumeRecoveryStartedAt
    ) {
      return prev;
    }

    recoveryStarted = true;
    const resumeRecoveryStartedAt = current.resumeRecoveryStartedAt ?? recoveryAt;
    recoveringPending = { ...current, resumeRecoveryStartedAt };
    const next: SessionRecord = { ...prev, pendingResume: recoveringPending };

    for (const [responseId, receipt] of Object.entries(prev.inboxResponseReceipts ?? {})) {
      if (
        receipt.itemId !== expected.itemId ||
        receipt.kind !== expected.kind ||
        receipt.runId !== expected.runId ||
        receipt.toolCallId !== expected.toolCallId ||
        receipt.pendingRequestedAt !== expected.requestedAt ||
        receipt.status !== 'accepted'
      ) {
        continue;
      }
      next.inboxResponseReceipts = {
        ...(next.inboxResponseReceipts ?? prev.inboxResponseReceipts ?? {}),
        [responseId]: {
          ...receipt,
          status: 'failed',
          error: projectedError,
          retryable: false,
          failedAt: receipt.failedAt ?? resumeRecoveryStartedAt,
          updatedAt: resumeRecoveryStartedAt,
        },
      };
    }

    recoveringQueuedItemId =
      current.queuedItemId ??
      (prev.pendingQueue ?? []).find(item => {
        const receipt = prev.queueAdmissionReceipts?.[item.id];
        return (receipt?.status === 'accepted' || receipt?.status === 'completed') && receipt.runId === current.runId;
      })?.id;
    if (recoveringQueuedItemId !== undefined) {
      next.pendingQueue = (prev.pendingQueue ?? []).filter(item => item.id !== recoveringQueuedItemId);
      const receipt = prev.queueAdmissionReceipts?.[recoveringQueuedItemId];
      if (receipt && receipt.status !== 'completed' && receipt.status !== 'failed' && receipt.status !== 'dead') {
        failedQueueReceipt = {
          ...receipt,
          status: 'failed',
          error: projectedError,
          failedAt: receipt.failedAt ?? resumeRecoveryStartedAt,
          updatedAt: resumeRecoveryStartedAt,
        };
        next.queueAdmissionReceipts = {
          ...(prev.queueAdmissionReceipts ?? {}),
          [recoveringQueuedItemId]: failedQueueReceipt,
        };
      }
    }
    return next;
  });

  if (!recoveryStarted || recoveringPending === undefined) {
    if (
      expectedQueuedItemId !== undefined &&
      port.queueHead.currentId === expectedQueuedItemId &&
      port.record.pendingResume?.queuedItemId !== expectedQueuedItemId &&
      !port.record.pendingQueue.some(item => item.id === expectedQueuedItemId)
    ) {
      port.queueHead.release();
      port.notifyMaybeIdle();
    }
    return false;
  }

  await accountStagedResumeUsage(port, recoveringPending, recoveringQueuedItemId);

  if (recoveringQueuedItemId !== undefined) {
    const settledReceipt = port.record.queueAdmissionReceipts?.[recoveringQueuedItemId] ?? failedQueueReceipt;
    if (settledReceipt?.signalId !== undefined) {
      await port.writeQueueSignalResultEvidence({
        status: 'failed',
        signalId: settledReceipt.signalId,
        runId: settledReceipt.runId,
        error: projectedError,
      });
    }
  }
  if (recoveringPending.originSignalId !== undefined) {
    await port.settleSignalResult(recoveringPending.originSignalId, {
      status: 'failed',
      runId: recoveringPending.runId,
      error: projectedError,
    });
  }
  // The abandoned resume may have deferred a terminal admission — settle the
  // pending grant alongside the signal evidence so the handoff cannot strand
  // pending forever behind an interaction being terminalized here.
  await port.cancelTerminalizedPendingAdmission(recoveringPending, projectedError);

  let finalized = false;
  await port.flushUpdate(prev => {
    finalized = false;
    const current = prev.pendingResume;
    if (
      current === undefined ||
      current.kind !== expected.kind ||
      current.runId !== expected.runId ||
      current.toolCallId !== expected.toolCallId ||
      (current.itemId ?? current.toolCallId) !== expected.itemId ||
      current.requestedAt !== expected.requestedAt ||
      current.expiresAt !== expected.expiresAt ||
      current.resumedAt !== expected.resumedAt ||
      current.resumeRecoveryAt !== expected.resumeRecoveryAt ||
      current.resumeRecoveryStartedAt !== recoveringPending!.resumeRecoveryStartedAt
    ) {
      return prev;
    }
    finalized = true;
    const next: SessionRecord = { ...prev };
    delete next.pendingResume;
    return next;
  });
  if (!finalized) return false;

  port.replacementToolSurface.drop(recoveringPending.runId);
  if (recoveringQueuedItemId !== undefined) {
    const settledReceipt = port.record.queueAdmissionReceipts?.[recoveringQueuedItemId] ?? failedQueueReceipt;
    port.emit({
      type: 'queue_failed',
      queuedItemId: recoveringQueuedItemId,
      ...(settledReceipt?.runId ? { runId: settledReceipt.runId } : {}),
      ...(settledReceipt?.signalId ? { signalId: settledReceipt.signalId } : {}),
      ...(settledReceipt?.admissionId ? { admissionId: settledReceipt.admissionId } : {}),
      error: projectedError,
    });
    const resolver = port.queueHead.takeResolver(recoveringQueuedItemId);
    if (resolver) {
      resolver.reject(recoveryError);
    }
    port.queueHead.releaseIf(recoveringQueuedItemId);
  }
  port.emitAbortedToolEnds({ force: true });
  port.emitTurnEvent({
    type: 'resume_failed',
    runId: recoveringPending.runId,
    retryable: false,
    error: projectedError,
  });
  port.emitAgentEnd({ runId: recoveringPending.runId, finishReason: 'error' });
  port.notifyMaybeIdle();
  if (port.state === 'live' && opts.drainQueue !== false) void port.maybeDrainQueue();
  return true;
}

/**
 * §10.2 suspension event: project a captured `PendingResume` into the matching
 * pending shape (tool_approval_required / tool_suspension_required /
 * question_pending / plan_approval_required). Sandbox/path-access prompts
 * project to `question_pending` (§10.2 defines no dedicated sandbox event).
 * Emitted after the durable-parking barrier so subscribers can reconstruct
 * the pending state from storage.
 */
export function emitPendingEvent(port: PendingInteractionsSessionPort, pending: PendingResume): void {
  const sourceFields =
    pending.source === 'subagent'
      ? {
          source: 'subagent' as const,
          subagentToolCallId: pending.subagentToolCallId ?? '',
          subagentSessionId: port.id,
        }
      : { source: 'parent' as const };
  const base = {
    runId: pending.runId,
    itemId: pending.itemId ?? '',
    requestedAt: pending.requestedAt,
    expiresAt: pendingInteractionExpiresAt(port, pending),
    toolCallId: pending.toolCallId,
    ...sourceFields,
  };
  const p = pending.payload ?? {};
  switch (pending.kind) {
    case 'tool-approval':
      port.emitTurnEvent({
        type: 'tool_approval_required',
        ...base,
        toolName: pending.toolName ?? '',
        ...(p.toolCategory !== undefined ? { toolCategory: p.toolCategory } : {}),
        approvalReasons: p.approvalReasons ?? [],
        // §10.2 (events.ts:202 "Public payloads are JSON-safe projections"):
        // `p.input` is the raw runtime `payload.args` from the suspended
        // output (Date/Map/class/bigint). Project it at emit so the first
        // live subscriber and the durable/replayed row carry the identical
        // value — the same live===replay guarantee as tool_start/tool_end.
        input: projectToolEventPayloadForJson(
          p.input,
          'tool_approval_required.input',
          port.harness._internalMaxEventPayloadBytes,
        ),
      } as EmitInput);
      return;
    case 'tool-suspension':
      port.emitTurnEvent({
        type: 'tool_suspension_required',
        ...base,
        toolName: pending.toolName ?? '',
        // §10.2: `suspendData` is the tool's raw `suspendPayload` and can hold
        // runtime objects, so project it at emit for live===replay parity.
        suspendData: projectToolEventPayloadForJson(
          p.suspendData,
          'tool_suspension_required.suspendData',
          port.harness._internalMaxEventPayloadBytes,
        ),
      } as EmitInput);
      return;
    case 'question':
      port.emitTurnEvent({
        type: 'question_pending',
        ...base,
        question: p.question ?? '',
        ...(p.options !== undefined ? { options: p.options } : {}),
        ...(p.selectionMode !== undefined ? { selectionMode: p.selectionMode } : {}),
      } as EmitInput);
      return;
    case 'plan-approval':
      port.emitTurnEvent({
        type: 'plan_approval_required',
        ...base,
        title: p.title ?? '',
        plan: p.plan ?? '',
      } as EmitInput);
      return;
    case 'sandbox-access':
      // §10.2: sandbox/path-access prompts project to question_pending.
      port.emitTurnEvent({
        type: 'question_pending',
        ...base,
        question: p.sandboxAccess?.reason ?? `Allow ${p.sandboxAccess?.semanticType ?? 'sandbox'} access?`,
      } as EmitInput);
      return;
  }
}

export function tokenUsageDeltaFromFullOutput(
  port: PendingInteractionsSessionPort,
  full: FullOutput<unknown>,
  accountedCumulative?: TokenUsage,
): TokenUsage | undefined {
  const usage = (full as { totalUsage?: unknown; usage?: unknown }).totalUsage ?? (full as { usage?: unknown }).usage;
  if (usage && typeof usage === 'object') {
    const u = usage as {
      promptTokens?: number;
      completionTokens?: number;
      totalTokens?: number;
      inputTokens?: number;
      outputTokens?: number;
    };
    const prompt = u.promptTokens ?? u.inputTokens;
    const completion = u.completionTokens ?? u.outputTokens;
    const promptNumber = typeof prompt === 'number' && Number.isInteger(prompt) && prompt >= 0 ? prompt : undefined;
    const completionNumber =
      typeof completion === 'number' && Number.isInteger(completion) && completion >= 0 ? completion : undefined;
    const delta: TokenUsage = { promptTokens: 0, completionTokens: 0, totalTokens: 0 };
    const incremented =
      promptNumber !== undefined ||
      completionNumber !== undefined ||
      (typeof u.totalTokens === 'number' && Number.isInteger(u.totalTokens) && u.totalTokens >= 0);
    if (promptNumber !== undefined) delta.promptTokens += promptNumber;
    if (completionNumber !== undefined) delta.completionTokens += completionNumber;
    const derivedTotalTokens = (promptNumber ?? 0) + (completionNumber ?? 0);
    if (typeof u.totalTokens === 'number' && Number.isInteger(u.totalTokens) && u.totalTokens >= 0) {
      delta.totalTokens += u.totalTokens < derivedTotalTokens ? derivedTotalTokens : u.totalTokens;
    } else if (promptNumber !== undefined || completionNumber !== undefined) {
      // Providers that only emit `inputTokens`/`outputTokens` leave `totalTokens`
      // off; derive it so the aggregate stays consistent with its parts.
      delta.totalTokens += derivedTotalTokens;
    }
    if (!incremented) return undefined;
    if (accountedCumulative === undefined) return delta;

    // A resumed Mastra stream restores its prior usage counter, so FullOutput
    // is cumulative across suspend boundaries. Subtract the durable baseline
    // component-wise. If a provider resets a counter, treat the new value as
    // this segment's usage rather than producing a negative delta.
    const promptTokens =
      delta.promptTokens >= accountedCumulative.promptTokens
        ? delta.promptTokens - accountedCumulative.promptTokens
        : delta.promptTokens;
    const completionTokens =
      delta.completionTokens >= accountedCumulative.completionTokens
        ? delta.completionTokens - accountedCumulative.completionTokens
        : delta.completionTokens;
    const rawTotalTokens =
      delta.totalTokens >= accountedCumulative.totalTokens
        ? delta.totalTokens - accountedCumulative.totalTokens
        : delta.totalTokens;
    const resumedDelta = {
      promptTokens,
      completionTokens,
      totalTokens: Math.max(rawTotalTokens, promptTokens + completionTokens),
    };
    return resumedDelta.promptTokens === 0 && resumedDelta.completionTokens === 0 && resumedDelta.totalTokens === 0
      ? undefined
      : resumedDelta;
  }
  return undefined;
}

export function messageSuspendedTokenUsageDelta(
  port: PendingInteractionsSessionPort,
  full: FullOutput<unknown>,
): TokenUsage | undefined {
  if (
    full.runId &&
    (port.messageTokenLedger.isAccounted(full.runId) || port.messageTokenLedger.isReserved(full.runId))
  ) {
    return undefined;
  }
  // The in-memory accounted sets are empty after a reopen even though the
  // suspended run's cumulative usage is durably recorded on the parked
  // pending. Subtract that durable baseline so a cached-duplicate replay
  // cannot re-apply usage the session already persisted.
  const parked = port.record.pendingResume;
  const durableBaseline = parked !== undefined && parked.runId === full.runId ? parked.accountedTokenUsage : undefined;
  return tokenUsageDeltaFromFullOutput(port, full, durableBaseline);
}

export function reserveMessageSuspendedTokenUsage(
  port: PendingInteractionsSessionPort,
  full: FullOutput<unknown>,
): {
  tokenUsageDelta?: TokenUsage;
  reservedRunId?: string;
  reservation?: Deferred<void>;
} {
  const tokenUsageDelta = messageSuspendedTokenUsageDelta(port, full);
  if (tokenUsageDelta !== undefined && full.runId) {
    const reservation = createDeferred<void>();
    port.messageTokenLedger.reserve(full.runId, reservation);
    void reservation.promise.catch(() => {});
    return { tokenUsageDelta, reservedRunId: full.runId, reservation };
  }
  return { tokenUsageDelta };
}

export function commitMessageSuspendedTokenUsage(
  port: PendingInteractionsSessionPort,
  reservation: {
    tokenUsageDelta?: TokenUsage;
    reservedRunId?: string;
    reservation?: Deferred<void>;
  },
): void {
  if (reservation.reservedRunId !== undefined) {
    port.messageTokenLedger.commit(reservation.reservedRunId, reservation.reservation);
    reservation.reservation?.resolve();
  }
}

export function rollbackMessageSuspendedTokenUsage(
  port: PendingInteractionsSessionPort,
  reservation: {
    reservedRunId?: string;
    reservation?: Deferred<void>;
  },
): void {
  if (reservation.reservedRunId !== undefined) {
    port.messageTokenLedger.rollback(reservation.reservedRunId, reservation.reservation);
    reservation.reservation?.resolve();
  }
}

export async function waitForMessageSuspendedTokenUsageOwner(
  port: PendingInteractionsSessionPort,
  full: FullOutput<unknown>,
  activeTurnWaiter?: Promise<never>,
): Promise<void> {
  if (!full.runId) return;
  while (!port.messageTokenLedger.isAccounted(full.runId)) {
    const reservation = port.messageTokenLedger.reservationFor(full.runId);
    if (reservation === undefined) return;
    await port.raceActiveTurnWaiter(reservation.promise, activeTurnWaiter);
  }
}

export async function captureMessageSuspendWithTokenUsage(
  port: PendingInteractionsSessionPort,
  full: FullOutput<unknown>,
  queuedItemId: string | undefined,
  modeId: string,
  modelId: string,
  activeTurnWaiter?: Promise<never>,
  opts: { restoreFence?: { expectedKey?: string } } = {},
): Promise<void> {
  await waitForMessageSuspendedTokenUsageOwner(port, full, activeTurnWaiter);
  const reservation = reserveMessageSuspendedTokenUsage(port, full);
  try {
    await port.raceActiveTurnWaiter(
      maybeCaptureSuspend(port, full, queuedItemId, modeId, modelId, {
        tokenUsageDelta: reservation.tokenUsageDelta,
        restoreFence: opts.restoreFence,
      }),
      activeTurnWaiter,
    );
    commitMessageSuspendedTokenUsage(port, reservation);
  } catch (err) {
    rollbackMessageSuspendedTokenUsage(port, reservation);
    throw err;
  }
}

/**
 * A suspended signal remains in `pendingResume` as immutable audit/recovery
 * evidence after session cancellation, but its durable signal result must be
 * failed immediately. Otherwise `signal()` callers (including detached task
 * delegation drivers) wait until the interaction TTL and retain concurrency
 * reservations for up to ten minutes.
 */
export async function settlePendingInteractionCancellation(
  port: PendingInteractionsSessionPort,
  reason?: string,
): Promise<void> {
  const pending = port.record.pendingResume;
  if (pending === undefined) return;
  const cancellationError = new HarnessSessionCancelledError(port.id, reason);
  const projectedError = projectHarnessPublicError(cancellationError);
  const queuedItemId = queuedItemIdForPendingResume(port, pending);
  if (queuedItemId !== undefined) {
    const receipt = port.record.queueAdmissionReceipts?.[queuedItemId];
    if (receipt?.signalId !== undefined) {
      await port.writeQueueSignalResultEvidence({
        status: 'failed',
        signalId: receipt.signalId,
        runId: receipt.runId,
        error: projectedError,
      });
    }
  }
  if (pending.originSignalId !== undefined) {
    await port.settleSignalResult(pending.originSignalId, {
      status: 'failed',
      runId: pending.runId,
      error: projectedError,
    });
  }
  // A cancelled suspended turn may have deferred a terminal admission —
  // settle the pending grant so its retained observers and durable row do
  // not outlive the owning interaction.
  await port.cancelTerminalizedPendingAdmission(pending, projectedError);
}

/**
 * Abort/branch-revision counterpart to cancellation settlement. The pending
 * interaction is terminalized and removed without setting a parent
 * `cancelRequest`, so the conversation session remains reusable. Durable
 * signal evidence is failed before the exact-generation clear; a crash in
 * between leaves the pending row visible and safely retryable.
 */
export async function discardPendingInteractionForAbort(
  port: PendingInteractionsSessionPort,
  reason: string,
): Promise<boolean> {
  const pending = port.record.pendingResume;
  if (pending === undefined) return false;
  const abortError = new HarnessSessionCancelledError(port.id, reason);
  const projectedError = projectHarnessPublicError(abortError);
  const failedAt = Date.now();
  const queuedItemId = queuedItemIdForPendingResume(port, pending);
  const queueReceipt = queuedItemId === undefined ? undefined : port.record.queueAdmissionReceipts?.[queuedItemId];

  // First win a durable waiting/resuming -> terminalizing transition. Resume
  // admission rejects any generation carrying expiryStartedAt, so evidence is
  // never failed for a response that can still proceed. If the process dies
  // after this claim, the ordinary pending-deadline recovery finishes it.
  let discardClaimed = false;
  await port.flushUpdate(prev => {
    discardClaimed = false;
    const current = prev.pendingResume;
    if (
      current === undefined ||
      current.kind !== pending.kind ||
      current.runId !== pending.runId ||
      current.toolCallId !== pending.toolCallId ||
      (current.itemId ?? current.toolCallId) !== (pending.itemId ?? pending.toolCallId) ||
      current.requestedAt !== pending.requestedAt ||
      current.expiresAt !== pending.expiresAt ||
      current.resumedAt !== pending.resumedAt ||
      current.expiryStartedAt !== undefined
    ) {
      return prev;
    }
    discardClaimed = true;
    return {
      ...prev,
      pendingResume: { ...current, expiryStartedAt: failedAt },
    };
  });
  if (!discardClaimed) return false;

  if (queueReceipt?.signalId !== undefined) {
    await port.writeQueueSignalResultEvidence({
      status: 'failed',
      signalId: queueReceipt.signalId,
      runId: queueReceipt.runId,
      error: projectedError,
    });
  }
  if (pending.originSignalId !== undefined) {
    await port.settleSignalResult(pending.originSignalId, {
      status: 'failed',
      runId: pending.runId,
      error: projectedError,
    });
  }
  // A discarded suspension may have deferred a terminal admission — settle
  // the pending grant alongside the other durable evidence BEFORE the
  // pending row clears below. If cancellation fails here the claim marker
  // (`expiryStartedAt`) stays on the row, the interaction remains
  // discoverable, and the pending-deadline recovery finishes the cancel.
  await port.cancelTerminalizedPendingAdmission(pending, projectedError);

  let discarded = false;
  await port.flushUpdate(prev => {
    discarded = false;
    const current = prev.pendingResume;
    if (
      current === undefined ||
      current.kind !== pending.kind ||
      current.runId !== pending.runId ||
      current.toolCallId !== pending.toolCallId ||
      (current.itemId ?? current.toolCallId) !== (pending.itemId ?? pending.toolCallId) ||
      current.requestedAt !== pending.requestedAt ||
      current.expiresAt !== pending.expiresAt ||
      current.resumedAt !== pending.resumedAt ||
      current.expiryStartedAt !== failedAt
    ) {
      return prev;
    }
    discarded = true;
    const next: SessionRecord = { ...prev };
    delete next.pendingResume;
    // A response may have durably won admission (`resumedAt` + accepted
    // inbox receipt) immediately before Stop invalidated the branch. Because
    // no provider resume is allowed to start afterward, terminalize every
    // accepted receipt for this exact pending generation instead of leaving a
    // permanent "accepted" receipt that a reconnect could mistake for work
    // still in flight.
    const inboxResponseReceipts = { ...(prev.inboxResponseReceipts ?? {}) };
    let inboxReceiptsChanged = false;
    for (const [responseId, receipt] of Object.entries(inboxResponseReceipts)) {
      if (
        receipt.status === 'accepted' &&
        receipt.kind === pending.kind &&
        receipt.runId === pending.runId &&
        receipt.toolCallId === pending.toolCallId &&
        receipt.pendingRequestedAt === pending.requestedAt
      ) {
        inboxResponseReceipts[responseId] = {
          ...receipt,
          status: 'failed',
          error: projectedError,
          failedAt: receipt.failedAt ?? failedAt,
          updatedAt: failedAt,
          retryable: false,
        };
        inboxReceiptsChanged = true;
      }
    }
    if (inboxReceiptsChanged) next.inboxResponseReceipts = inboxResponseReceipts;
    if (queuedItemId !== undefined) {
      next.pendingQueue = (prev.pendingQueue ?? []).filter(item => item.id !== queuedItemId);
      const receipt = prev.queueAdmissionReceipts?.[queuedItemId];
      if (receipt && receipt.status !== 'completed' && receipt.status !== 'failed' && receipt.status !== 'dead') {
        next.queueAdmissionReceipts = {
          ...(prev.queueAdmissionReceipts ?? {}),
          [queuedItemId]: {
            ...receipt,
            status: 'failed',
            error: projectedError,
            failedAt: receipt.failedAt ?? failedAt,
            updatedAt: failedAt,
          },
        };
      }
    }
    return next;
  });
  if (!discarded) return false;

  clearPendingInteractionExpiryTimer(port);
  port.replacementToolSurface.drop(pending.runId);
  if (queuedItemId !== undefined) {
    const resolver = port.queueHead.takeResolver(queuedItemId);
    if (resolver) {
      resolver.reject(abortError);
    }
    port.queueHead.releaseIf(queuedItemId);
    port.emit({
      type: 'queue_failed',
      queuedItemId,
      ...(queueReceipt?.runId ? { runId: queueReceipt.runId } : {}),
      ...(queueReceipt?.signalId ? { signalId: queueReceipt.signalId } : {}),
      ...(queueReceipt?.admissionId ? { admissionId: queueReceipt.admissionId } : {}),
      error: projectedError,
    });
  }
  port.emitAbortedToolEnds({ force: true });
  port.emitTurnEvent({
    type: 'resume_failed',
    runId: pending.runId,
    retryable: false,
    error: projectedError,
  });
  if (port.currentTurnAbortController === undefined) {
    releaseParkedRun(port, pending);
    port.emitAgentEnd({ runId: pending.runId, finishReason: 'error' });
  }
  port.notifyMaybeIdle();
  return true;
}

export function pendingRunToolReceiptProjection(
  port: PendingInteractionsSessionPort,
  runId: string,
): Pick<PendingResume, 'toolReceipts' | 'toolReceiptsOverflow'> {
  const state = port.runToolReceipts.get(runId);
  if (state === undefined) return {};
  return {
    ...(state.receipts.length > 0 ? { toolReceipts: state.receipts.map(receipt => ({ ...receipt })) } : {}),
    ...(state.overflow ? { toolReceiptsOverflow: true } : {}),
  };
}

/**
 * Deferred terminal settlement for a message that suspended. The admission's
 * deterministic `runId` is the resumed run's id, so the durable row is
 * recoverable without persisting extra correlation on `pendingResume`.
 * The probe is by-run across statuses: a cancelled or fenced grant must still
 * surface its terminal outcome (the commit helper throws it and drains
 * retained observers) instead of letting the resume clear `pendingResume`
 * and report success on a revoked grant; a committed row replays its durable
 * receipt through the same barrier.
 */
export async function settleResumedTerminalHandoff(
  port: PendingInteractionsSessionPort,
  pending: PendingResume,
  full: FullOutput<unknown>,
  options: { modeId?: string; modelId?: string },
  deletedTurnWaiter: Promise<never>,
  turnAbortSignal: AbortSignal | undefined,
): Promise<void> {
  // Probe durable state even when no finalizer is registered on this session
  // instance — a reopened session must still observe a bound admission and
  // fail closed through the commit barrier rather than report success on an
  // unsettled (or dead) grant.
  const admission = await probeResumeAdmissionByRun(port, pending);
  if (!admission) return;
  // PF-7631: a resumed wake turn's terminal commit is a wake terminal commit:
  // the same bounded re-drive with an `error` event per failed attempt. If it
  // still fails, the resume rejects and the parked interaction stays, so a
  // retry of the response or stale-resume recovery settles it later.
  if (isHarnessWakeReservedKey(admission.admissionId)) {
    await port.commitWakeTerminalWithRetry(admission, full, options, deletedTurnWaiter, turnAbortSignal);
    return;
  }
  await port.commitTerminalHandoff(admission, full, options);
}

/**
 * Stale-resume recovery terminalizes a resumed turn without delivering its
 * reply, but the resumed run may already have staged completed output and
 * consumed tokens. Count that usage before any evidence rewrite.
 */
export async function accountStagedResumeUsage(
  port: PendingInteractionsSessionPort,
  pending: PendingResume,
  queuedItemId: string | undefined,
): Promise<void> {
  const admission = await probeResumeAdmissionByRun(port, pending);
  if (!admission) return;
  const stored = await port.storage.loadMessageResultEvidence({
    harnessName: port.record.harnessName,
    sessionId: port.id,
    resourceId: port.resourceId,
    threadId: port.threadId,
    signalId: admission.signalId,
  });
  if (
    stored === null ||
    !('status' in stored) ||
    stored.status !== 'pending' ||
    stored.stagedResult === undefined ||
    stored.runId !== pending.runId
  ) {
    return;
  }
  await port.accountStagedTerminalUsage(pending.runId, stored.stagedResult as FullOutput<unknown>, queuedItemId);
}

/**
 * By-run admission probe shared by the settle and cancel paths. A probe
 * failure never reaches `_commitTerminalHandoff`'s drain, so the
 * indeterminate outcome is reported to the run's retained observers here
 * while the admission stays pending/recoverable.
 */
export async function probeResumeAdmissionByRun(port: PendingInteractionsSessionPort, pending: PendingResume) {
  const sessionIncarnation = port.record.sessionIncarnation;
  if (!port.storage.supportsTerminalHandoff || sessionIncarnation === undefined) return undefined;
  try {
    return await port.storage.loadTerminalAdmissionByRun({
      harnessName: port.record.harnessName,
      sessionId: port.id,
      runId: pending.runId,
      sessionIncarnation,
    });
  } catch (err) {
    throw port.terminalFailure(err, undefined, pending.runId);
  }
}

/**
 * Resolve an approved plan-approval mode transition. A caller-supplied
 * `transitionToMode` on the response overrides the submitting mode's
 * declared `transitionsTo` (captured into `pending.transitionModeId` at
 * suspend time); a same-mode or unapproved response flips nothing. Throws
 * when the target mode no longer exists — recovery callers that must stay
 * total catch and skip the flip.
 */
export function resolvePlanApprovalModeFlipTarget(
  port: PendingInteractionsSessionPort,
  pending: PendingResume,
  data: { approved?: boolean; transitionToMode?: string } | undefined,
): string | undefined {
  if (data?.approved !== true) return undefined;
  const candidate = data.transitionToMode ?? pending.transitionModeId;
  if (!candidate || candidate === port.record.modeId) return undefined;
  port.harness._getMode(candidate);
  return candidate;
}

/**
 * Settlement-only retry for a resume whose provider run already completed but
 * whose post-output bookkeeping threw. `resumedAt` is durable so the caller
 * cannot re-run `resumeStream`; instead the cached completed `FullOutput` is
 * committed through `_finalizeResumedTurnOutcome`, which is idempotent for
 * every step the failed attempt did not reach. Returns the completed result
 * when it handled the retry, `undefined` when there is nothing recoverable
 * (no cached output, no pending admission, or the run re-suspended) and the
 * caller should fall through to the ordinary already-responded handling.
 */
export async function maybeFinalizeRetriedResumedTerminal(
  port: PendingInteractionsSessionPort,
  pending: PendingResume,
  resumeData: unknown,
  options: {
    expectedKind: PendingResume['kind'];
    responseMode: ResumeResponseMode;
    responseId?: string;
  },
): Promise<AgentResult | InboxResponseResult | undefined> {
  // Probe durable state even without a locally-registered finalizer — a
  // reopened session must still reconcile a bound admission (the commit
  // barrier replays a sealed winner or fails closed on a live one).
  const sessionIncarnation = port.record.sessionIncarnation;
  if (!port.storage.supportsTerminalHandoff || sessionIncarnation === undefined) return undefined;
  // Probe by run regardless of status: a commit may have sealed the grant
  // while the post-commit bookkeeping (the pendingResume-clearing flush)
  // failed — the admission is then 'committed' rather than 'pending', and
  // the retry must still finish the remaining bookkeeping. A cancelled or
  // fenced row means the interaction was already terminalized elsewhere.
  const admission = await port.storage.loadTerminalAdmissionByRun({
    harnessName: port.record.harnessName,
    sessionId: port.id,
    runId: pending.runId,
    sessionIncarnation,
  });
  if (!admission || (admission.status !== 'pending' && admission.status !== 'committed')) {
    return undefined;
  }
  const cached = port.runOutcomes.get(pending.runId);
  let full: FullOutput<unknown>;
  if (cached !== undefined && cached.ok && cached.full.finishReason !== 'suspended') {
    full = cached.full;
  } else {
    // A restart emptied the local run cache — canonical evidence still
    // carries the committed or staged completed output, so
    // recover it instead of reporting the resume as abandoned. The cached
    // entry is repopulated so the per-generation usage marker still dedupes
    // a second retry.
    const stored = await port.storage.loadMessageResultEvidence({
      harnessName: port.record.harnessName,
      sessionId: port.id,
      resourceId: port.resourceId,
      threadId: port.threadId,
      signalId: admission.signalId,
    });
    if (
      !stored ||
      !('status' in stored) ||
      !(
        (stored.status === 'completed' && stored.result !== undefined) ||
        (stored.status === 'pending' && stored.stagedResult !== undefined)
      )
    ) {
      return undefined;
    }
    full = (stored.status === 'completed' ? stored.result : stored.stagedResult) as FullOutput<unknown>;
    port.runOutcomes.remember(pending.runId, { ok: true, full });
  }

  const { expectedKind, responseMode, responseId } = options;
  const pendingQueuedItemId = queuedItemIdForPendingResume(port, pending);
  const resumeModeId = modeIdForPendingResume(port, pending);
  const resumeRuntimeDependencies = runtimeDependenciesForPendingResume(port, pending);
  const modeFlipTarget =
    expectedKind === 'plan-approval'
      ? resolvePlanApprovalModeFlipTarget(port, pending, resumeData as { approved: boolean; transitionToMode?: string })
      : undefined;
  const completingQueuedItemId = pendingQueuedItemId;
  await port.withActiveDeletedWaiter(async activeTurnWaiter => {
    try {
      await port.raceActiveTurnWaiter(port.stageCompletedTerminalOutput(pending.runId, full), activeTurnWaiter);
      await port.finalizeResumedTurnOutcome(pending, full, {
        responseId,
        pendingQueuedItemId,
        completingQueuedItemId,
        modeFlipTarget,
        previousModeId: port.record.modeId,
        resumeModeId,
        resumeModelId: resumeRuntimeDependencies.modelId,
        deletedTurnWaiter: activeTurnWaiter,
      });
    } catch (err) {
      if (err instanceof QueuePostRunFinalizationPendingError && completingQueuedItemId !== undefined) {
        port.deferQueuedTurnRetry(err);
      }
      throw redactPublicBoundaryRejection(err);
    }
  });
  if (completingQueuedItemId !== undefined) {
    void port.maybeDrainQueue();
  }
  if (responseMode === 'inbox-receipt') {
    const receipt =
      responseId !== undefined ? getOwnRecordValue(port.record.inboxResponseReceipts, responseId) : undefined;
    if (receipt) return inboxReceiptResult(port, receipt, false);
  }
  return full as AgentResult;
}

export async function maybeCaptureSuspend(
  port: PendingInteractionsSessionPort,
  full: FullOutput<unknown>,
  queuedItemId = port.queueHead.currentId,
  modeId = port.record.modeId,
  modelId = port.modelIdForQueuedItem(queuedItemId),
  opts: {
    tokenUsageDelta?: TokenUsage;
    originSignalId?: string;
    restoreFence?: { expectedKey?: string };
  } = {},
): Promise<void> {
  if (full.finishReason !== 'suspended') return;
  port.captureTurnRunId(full);
  const payload = full.suspendPayload as
    | { toolCallId: string; toolName: string; args?: unknown; suspendPayload?: unknown; approvalReasons?: string[] }
    | undefined;
  if (!payload || !full.runId) {
    await port.persistTokenUsageDelta(opts.tokenUsageDelta);
    port.clearPendingDurableTurnFlushErrorIfRepaired(full);
    return;
  }

  const pending = pendingResumeFromSuspendedOutput(
    port,
    full,
    payload,
    queuedItemId,
    modeId,
    modelId,
    opts.originSignalId,
  );
  if (pending === undefined) return;
  const suspendedFenceLease = port.currentAgentRequestContext
    ? captureSuspendedToolSurfaceFenceLease(port.currentAgentRequestContext, full.runId)
    : undefined;
  // For a fenced cached-restore the token delta is evaluated at flush time
  // against the generation that actually survived the CAS: a skipped write
  // applies no usage, and an applied write reconciles the cached output's
  // cumulative counter against the durable per-run baseline still on the
  // record (the in-memory accounted sets are empty after a reopen).
  let writeApplied = false;
  const flushTokenUsageDelta =
    opts.restoreFence === undefined
      ? opts.tokenUsageDelta
      : () => {
          if (!writeApplied) return undefined;
          const baseline =
            port.record.pendingResume !== undefined && port.record.pendingResume.runId === full.runId
              ? port.record.pendingResume.accountedTokenUsage
              : undefined;
          return tokenUsageDeltaFromFullOutput(port, full, baseline);
        };
  const existing = port.record.pendingResume;
  if (existing && existing.runId === full.runId && existing.toolCallId === payload.toolCallId) {
    if (pending.toolSurfaceFence !== undefined) retainCurrentReplacementToolSurface(port, full.runId);
    await port.flushUpdate(
      prev => {
        // CAS retries re-run this updater against newer state — the apply
        // flag must reflect the attempt that actually won, not a failed one.
        writeApplied = false;
        const current = prev.pendingResume;
        if (opts.restoreFence !== undefined) {
          const currentKey = current === undefined ? undefined : pendingInteractionGenerationKey(current);
          if (currentKey !== opts.restoreFence.expectedKey) return prev;
        }
        // A tool-registered pending (ctx.registerQuestion / registerPlanApproval
        // / registerSandboxAccess) is written mid-turn without the turn's
        // origin. Settlement and queue routing read `queuedItemId` /
        // `originSignalId` off the parked pending, so adopt the captured
        // origin when the registered pending lacks it.
        const originToAdopt = {
          ...(current?.queuedItemId === undefined && pending.queuedItemId !== undefined
            ? { queuedItemId: pending.queuedItemId }
            : {}),
          ...(current?.originSignalId === undefined && pending.originSignalId !== undefined
            ? { originSignalId: pending.originSignalId }
            : {}),
        };
        if (
          current === undefined ||
          current.runId !== full.runId ||
          current.toolCallId !== payload.toolCallId ||
          (pending.accountedTokenUsage === undefined &&
            pending.toolReceipts === undefined &&
            pending.toolReceiptsOverflow !== true &&
            pending.subagentBudgetSpent === undefined &&
            Object.keys(originToAdopt).length === 0)
        ) {
          return prev;
        }
        writeApplied = true;
        return {
          ...prev,
          pendingResume: {
            ...current,
            ...originToAdopt,
            ...(pending.accountedTokenUsage !== undefined ? { accountedTokenUsage: pending.accountedTokenUsage } : {}),
            ...(pending.toolReceipts !== undefined ? { toolReceipts: pending.toolReceipts } : {}),
            ...(pending.toolReceiptsOverflow === true ? { toolReceiptsOverflow: true } : {}),
            ...(pending.subagentBudgetSpent !== undefined ? { subagentBudgetSpent: pending.subagentBudgetSpent } : {}),
          },
        };
      },
      { tokenUsageDelta: flushTokenUsageDelta },
    );
    if (port.currentAgentRequestContext && suspendedFenceLease) {
      clearSuspendedToolSurfaceFence(port.currentAgentRequestContext, full.runId, suspendedFenceLease);
    }
    port.clearPendingDurableTurnFlushErrorIfRepaired(full);
    return;
  }
  // A cached-suspension restore (restoreFence) reads `pendingResume` before
  // awaiting a durable probe; a resume can clear and re-park a NEWER
  // generation — or clear it entirely on commit — in that gap. Re-check the
  // pre-probe generation key inside the CAS updater so the stale restore
  // cannot resurrect a cleared interaction or overwrite the newer pending:
  // the durable park is authoritative.
  let restored = true;
  await port.flushUpdate(
    prev => {
      restored = true;
      writeApplied = false;
      if (opts.restoreFence !== undefined) {
        const current = prev.pendingResume;
        const currentKey = current === undefined ? undefined : pendingInteractionGenerationKey(current);
        if (currentKey !== opts.restoreFence.expectedKey) {
          restored = false;
          return prev;
        }
      }
      writeApplied = true;
      return { ...prev, pendingResume: pending };
    },
    { tokenUsageDelta: flushTokenUsageDelta },
  );
  if (port.currentAgentRequestContext && suspendedFenceLease) {
    clearSuspendedToolSurfaceFence(port.currentAgentRequestContext, full.runId, suspendedFenceLease);
  }
  // Announce the suspension only when the durable write actually landed this
  // pending — a fenced-out restore or a size-budget drop must not surface a
  // pending event for an interaction storage never recorded.
  if (!restored || port.record.pendingResume !== pending) return;
  port.clearPendingDurableTurnFlushErrorIfRepaired(full);

  // Emit the §10.2 pending event AFTER the durable-parking barrier (§5.4) so
  // any subscriber observing it can reconstruct the pending state from storage.
  emitPendingEvent(port, pending);
}

export function pendingResumeFromSuspendedOutput(
  port: PendingInteractionsSessionPort,
  full: FullOutput<unknown>,
  payload: {
    toolCallId: string;
    toolName: string;
    args?: unknown;
    suspendPayload?: unknown;
    approvalReasons?: string[];
  },
  queuedItemId = port.queueHead.currentId,
  modeId = port.record.modeId,
  modelId = port.modelIdForQueuedItem(queuedItemId),
  originSignalId?: string,
): PendingResume | undefined {
  if (!full.runId) return undefined;
  const kind = classifyResumeKind(port, payload);
  const accountedTokenUsage = tokenUsageDeltaFromFullOutput(port, full);
  const toolReceiptProjection = pendingRunToolReceiptProjection(port, full.runId);
  const pending: PendingResume = {
    kind,
    itemId: `${kind}:${payload.toolCallId}`,
    runId: full.runId,
    toolCallId: payload.toolCallId,
    toolName: payload.toolName,
    ...pendingResumeSourceFields(port),
    ...newPendingInteractionTiming(port, kind),
    ...(queuedItemId !== undefined ? { queuedItemId } : {}),
    ...(originSignalId !== undefined ? { originSignalId } : {}),
    modeId,
    // §4.2e — carry the active turn's yolo so the resumed run keeps auto-granting.
    ...(port.currentTurnYolo ? { yolo: true } : {}),
    runtimeDependencies: port.harness._runtimeDependenciesForMode(modeId, modelId),
    // §5.1 — persist the suspended turn's caller app bag so a resume rebuilds the
    // SAME context (a re-suspend re-captures it because resume re-stashes it).
    ...(port.currentTurnRequestContext
      ? { requestContext: clonePersistedRequestContext(port.currentTurnRequestContext) }
      : {}),
    ...(port.currentTurnSourceWriteGuard
      ? { observationalMemorySourceWriteGuard: port.currentTurnSourceWriteGuard }
      : {}),
    ...(currentToolSurfaceFenceSnapshot(port, full.runId)
      ? { toolSurfaceFence: currentToolSurfaceFenceSnapshot(port, full.runId) }
      : {}),
    ...(accountedTokenUsage ? { accountedTokenUsage } : {}),
    ...toolReceiptProjection,
    ...subagentBudgetSpentProjection(port),
    payload: buildResumePayload(port, kind, payload),
  };

  if (pending.toolSurfaceFence !== undefined) retainCurrentReplacementToolSurface(port, full.runId);

  if (kind === 'plan-approval') {
    const mode = port.harness._getMode(modeId);
    if (mode.transitionsTo) pending.transitionModeId = mode.transitionsTo;
  }
  return pending;
}

/** Correlate child-side pending interactions with their parent invocation. */
export function pendingResumeSourceFields(
  port: PendingInteractionsSessionPort,
): Pick<PendingResume, 'source' | 'subagentToolCallId'> {
  if (port.subagentDepth <= 0) return { source: 'parent' };
  return {
    source: 'subagent',
    ...(port.subagentParentToolCallId !== undefined ? { subagentToolCallId: port.subagentParentToolCallId } : {}),
  };
}

export function classifyResumeKind(
  port: PendingInteractionsSessionPort,
  payload: { toolName: string; suspendPayload?: unknown },
): PendingResume['kind'] {
  if (payload.toolName === ASK_USER_TOOL_NAME) return 'question';
  if (payload.toolName === SUBMIT_PLAN_TOOL_NAME) return 'plan-approval';
  if ('suspendPayload' in payload && payload.suspendPayload !== undefined) return 'tool-suspension';
  return 'tool-approval';
}

export function buildResumePayload(
  port: PendingInteractionsSessionPort,
  kind: PendingResume['kind'],
  payload: { args?: unknown; suspendPayload?: unknown; approvalReasons?: string[] },
): PendingResume['payload'] {
  // The payload is persisted and published as JSON: an argument the model
  // omitted (an optional `selectionMode`, `options`, `title`, a tool with no
  // args, a suspension without data) is left out rather than stored as an
  // explicit `undefined`, which strict JSON consumers read as a non-JSON
  // payload. Absent stays absent; present values are unchanged.
  switch (kind) {
    case 'tool-approval':
      return {
        ...(payload.args !== undefined ? { input: payload.args } : {}),
        ...(payload.approvalReasons !== undefined ? { approvalReasons: payload.approvalReasons } : {}),
      };
    case 'tool-suspension':
      return {
        ...(payload.args !== undefined ? { input: payload.args } : {}),
        ...(payload.suspendPayload !== undefined ? { suspendData: payload.suspendPayload } : {}),
      };
    case 'question': {
      const args = (payload.args ?? {}) as {
        question?: string;
        options?: { label: string; description?: string }[];
        selectionMode?: 'single_select' | 'multi_select';
      };
      return {
        ...(args.question !== undefined ? { question: args.question } : {}),
        ...(args.options !== undefined ? { options: args.options } : {}),
        ...(args.selectionMode !== undefined ? { selectionMode: args.selectionMode } : {}),
      };
    }
    case 'plan-approval': {
      const args = (payload.args ?? {}) as { title?: string; plan?: string };
      return {
        ...(args.title !== undefined ? { title: args.title } : {}),
        ...(args.plan !== undefined ? { plan: args.plan } : {}),
      };
    }
  }
}

export async function respondToToolApproval(
  port: PendingInteractionsSessionPort,
  opts: ToolApprovalDecision & InboxResponseOptions,
): Promise<AgentResult | InboxResponseResult> {
  const approvalDecision =
    opts.editedArgs !== undefined
      ? parseToolApprovalDecision({ approved: opts.approved, editedArgs: opts.editedArgs })
      : undefined;
  if (opts.editedArgs !== undefined && !approvalDecision) {
    throw new HarnessValidationError(
      'respond[tool-approval].editedArgs',
      'editedArgs requires an approved decision and a JSON object',
    );
  }
  return port.resume(
    'tool-approval',
    compactJsonObject({
      approved: opts.approved,
      reason: opts.reason,
      editedArgs: approvalDecision?.editedArgs,
    }),
    opts,
  );
}

export async function respondToToolSuspension(
  port: PendingInteractionsSessionPort,
  opts: { resumeData: unknown } & InboxResponseOptions,
): Promise<AgentResult | InboxResponseResult> {
  return port.resume('tool-suspension', opts.resumeData, opts);
}

export async function respondToQuestion(
  port: PendingInteractionsSessionPort,
  opts: { answer: unknown } & InboxResponseOptions,
): Promise<AgentResult | InboxResponseResult> {
  return port.resume('question', { answer: opts.answer }, opts);
}

export async function respondToSandboxAccess(
  port: PendingInteractionsSessionPort,
  opts: { approved: boolean; reason?: string } & InboxResponseOptions,
): Promise<AgentResult | InboxResponseResult> {
  return port.resume(
    'sandbox-access',
    compactJsonObject({
      approved: opts.approved,
      reason: opts.reason,
    }),
    opts,
  );
}

export async function respondToPlanApproval(
  port: PendingInteractionsSessionPort,
  opts: {
    approved: boolean;
    revision?: string;
    transitionToMode?: string;
  } & InboxResponseOptions,
): Promise<AgentResult | InboxResponseResult> {
  if (opts.transitionToMode !== undefined) {
    // Validate eagerly so callers see a clean error rather than a CAS-time
    // throw from inside the resume flow.
    port.harness._getMode(opts.transitionToMode);
  }
  return port.resume(
    'plan-approval',
    compactJsonObject({
      approved: opts.approved,
      revision: opts.revision,
      transitionToMode: opts.transitionToMode,
    }),
    opts,
  );
}

export async function discardPendingInteraction(
  port: PendingInteractionsSessionPort,
  opts: DiscardPendingInteractionOptions,
): Promise<DiscardPendingInteractionResult> {
  port.assertLive('discardPendingInteraction()');
  const input = parseDiscardPendingInteractionOptions(opts);
  const responseHash = sha256CanonicalJson({
    operation: 'discard',
    kind: input.kind,
    itemId: input.itemId,
    runId: input.runId,
    toolCallId: input.toolCallId,
    pendingRequestedAt: input.pendingRequestedAt,
    reason: input.reason,
  });
  try {
    // One retry: a lost CAS re-classifies against the reloaded record.
    for (let attempt = 0; attempt < 2; attempt++) {
      const observed = classifyPendingInteractionDiscard(port, input, responseHash);
      if (observed.status === 'finish') {
        await completeDiscardedPendingInteraction(port, observed.pending, observed.receipt);
        return discardReceiptResult('already_resolved', observed.receipt);
      }
      if (observed.status !== 'claimable') return observed.result;
      const claimed = await claimPendingInteractionDiscard(port, input, responseHash, observed.pending);
      if (claimed !== undefined) {
        await completeDiscardedPendingInteraction(port, claimed.pending, claimed.receipt);
        return discardReceiptResult('discarded', claimed.receipt);
      }
    }
    throw new HarnessBusyError(port.id, 'in_flight');
  } catch (err) {
    throw discardPendingInteractionFailure(port, err, input.responseId);
  }
}

export function classifyPendingInteractionDiscard(
  port: PendingInteractionsSessionPort,
  input: DiscardPendingInteractionOptions,
  responseHash: string,
):
  | { status: 'claimable'; pending: PendingResume }
  | { status: 'finish'; pending: PendingResume; receipt: InboxResponseReceipt }
  | { status: 'resolved'; result: DiscardPendingInteractionResult } {
  const generation = discardGeneration(input);
  const notCurrent = (): { status: 'resolved'; result: DiscardPendingInteractionResult } => ({
    status: 'resolved',
    result: { status: 'not_current', itemId: input.itemId, kind: input.kind },
  });
  const pending = port.record.pendingResume;
  const pendingIsGeneration = pending !== undefined && pendingMatchesDiscardGeneration(pending, generation);
  const ownReceipt = getOwnRecordValue(port.record.inboxResponseReceipts, input.responseId);
  if (ownReceipt !== undefined) {
    if (
      ownReceipt.discardedAt === undefined ||
      ownReceipt.responseHash !== responseHash ||
      !receiptMatchesDiscardGeneration(ownReceipt, generation)
    ) {
      throw new HarnessInboxResponseConflictError(port.id, input.itemId, input.responseId);
    }
    if (pendingIsGeneration && pending!.expiryStartedAt === ownReceipt.discardedAt) {
      return { status: 'finish', pending: pending!, receipt: ownReceipt };
    }
    return { status: 'resolved', result: discardReceiptResult('already_resolved', ownReceipt) };
  }
  if (!pendingIsGeneration) {
    const prior = findDiscardReceipt(port, generation);
    return prior !== undefined
      ? { status: 'resolved', result: discardReceiptResult('already_resolved', prior) }
      : notCurrent();
  }
  if (pending!.expiryStartedAt !== undefined) {
    // Another terminalizer (expiry, Stop, or an earlier discard whose clear
    // has not committed) already claimed this generation.
    const claim = discardClaimForPending(port, pending!);
    return claim !== undefined ? { status: 'finish', pending: pending!, receipt: claim } : notCurrent();
  }
  // A resume admitted for this generation, or a still-running tool that
  // registered it, owns the interaction. Resolving it underneath would need a
  // turn abort, which this operation never performs.
  if (
    pending!.resumedAt !== undefined ||
    (port.currentTurnAbortController !== undefined &&
      (pending!.kind === 'question' || pending!.kind === 'plan-approval' || pending!.kind === 'sandbox-access'))
  ) {
    throw new HarnessBusyError(port.id, 'in_flight');
  }
  return { status: 'claimable', pending: pending! };
}

/**
 * Phase one: a single session CAS fenced to the exact unresumed, unclaimed
 * generation stamps the terminalizing claim (`expiryStartedAt`, which resume
 * admission rejects), writes the discard receipt, and finalizes every
 * unsettled inbox receipt of that generation as discarded.
 */
export async function claimPendingInteractionDiscard(
  port: PendingInteractionsSessionPort,
  input: DiscardPendingInteractionOptions,
  responseHash: string,
  observed: PendingResume,
): Promise<{ pending: PendingResume; receipt: InboxResponseReceipt } | undefined> {
  const generation = discardGeneration(input);
  const discardError = new HarnessPendingInteractionDiscardedError(port.id, input.itemId, input.reason);
  const projectedError = projectHarnessPublicError(discardError);
  let claimed: { pending: PendingResume; receipt: InboxResponseReceipt } | undefined;
  await port.flushUpdate(
    prev => {
      claimed = undefined;
      const current = prev.pendingResume;
      if (
        current === undefined ||
        !pendingMatchesDiscardGeneration(current, generation) ||
        current.expiresAt !== observed.expiresAt ||
        current.resumedAt !== undefined ||
        current.expiryStartedAt !== undefined ||
        getOwnRecordValue(prev.inboxResponseReceipts, input.responseId) !== undefined
      ) {
        return prev;
      }
      // Never below `requestedAt`: the parking host's clock may be ahead of
      // ours (lease handoff, NTP step), and `expiresAt < requestedAt` is a
      // corrupt pending row.
      const discardedAt = Math.max(Date.now(), current.requestedAt);
      const receipt: InboxResponseReceipt = {
        responseId: input.responseId,
        responseHash,
        resumeAttemptId: input.responseId,
        itemId: input.itemId,
        ...(current.queuedItemId !== undefined ? { queuedItemId: current.queuedItemId } : {}),
        kind: input.kind,
        runId: input.runId,
        toolCallId: input.toolCallId,
        pendingRequestedAt: input.pendingRequestedAt,
        response: { discarded: true, reason: input.reason },
        status: 'failed',
        error: projectedError,
        retryable: false,
        acceptedAt: discardedAt,
        failedAt: discardedAt,
        discardedAt,
        ...(prev.cancelRequest !== undefined
          ? { cancelledPendingResume: cancelledPendingAudit(current, prev.cancelRequest) }
          : {}),
        updatedAt: discardedAt,
      };
      // Built with data-property semantics only (entries / computed keys),
      // never `map[id] = ...`, so a caller id such as "__proto__" stays an
      // own, serializable receipt instead of hitting the prototype setter.
      const inboxResponseReceipts: Record<string, InboxResponseReceipt> = Object.fromEntries([
        ...Object.entries(prev.inboxResponseReceipts ?? {}).map(([responseId, candidate]) =>
          receiptMatchesDiscardGeneration(candidate, generation) &&
          (candidate.status === 'accepted' || (candidate.status === 'failed' && candidate.retryable === true))
            ? ([
                responseId,
                {
                  ...candidate,
                  status: 'failed',
                  error: projectedError,
                  retryable: false,
                  failedAt: discardedAt,
                  updatedAt: discardedAt,
                } satisfies InboxResponseReceipt,
              ] as const)
            : ([responseId, candidate] as const),
        ),
        [input.responseId, receipt] as const,
      ]);
      // The claim also moves the row's deadline to now, so a completion that
      // fails after this commit is due at once for the live timer, hydrate,
      // and the cold sweep instead of waiting for the original TTL. Expiry
      // never acts on a discard-owned row, so nothing else reads it.
      claimed = { pending: { ...current, expiryStartedAt: discardedAt, expiresAt: discardedAt }, receipt };
      return { ...prev, pendingResume: claimed.pending, inboxResponseReceipts };
    },
    { skipIfUnchanged: true, keepPendingResume: true },
  );
  return claimed;
}

/**
 * Settlement + phase two for a discard claim. Idempotent: replays, the
 * pending-deadline reconciler (live timer, hydrate, cold sweep) and the
 * original caller may all drive it; the clear is fenced to the claim.
 */
export async function completeDiscardedPendingInteraction(
  port: PendingInteractionsSessionPort,
  claimedPending: PendingResume,
  receipt: InboxResponseReceipt,
  opts: { drainQueue?: boolean } = {},
): Promise<boolean> {
  const discardedAt = receipt.discardedAt;
  if (discardedAt === undefined || claimedPending.expiryStartedAt !== discardedAt) return false;
  const discardError = discardedErrorForReceipt(port, receipt);
  const projectedError = projectHarnessPublicError(discardError);
  const generation = {
    kind: claimedPending.kind,
    itemId: claimedPending.itemId ?? claimedPending.toolCallId,
    runId: claimedPending.runId,
    toolCallId: claimedPending.toolCallId,
    pendingRequestedAt: claimedPending.requestedAt,
  };
  const queuedItemId = queuedItemIdForPendingResume(port, claimedPending);
  const queueReceipt = queuedItemId === undefined ? undefined : port.record.queueAdmissionReceipts?.[queuedItemId];
  if (queueReceipt?.signalId !== undefined) {
    await port.writeQueueSignalResultEvidence({
      status: 'failed',
      signalId: queueReceipt.signalId,
      runId: queueReceipt.runId,
      error: projectedError,
    });
  }
  if (claimedPending.originSignalId !== undefined) {
    // Required, not best effort: if the signal evidence cannot be written,
    // the claim stays as the (immediately due) recovery marker.
    await port.settleSignalResult(
      claimedPending.originSignalId,
      { status: 'failed', runId: claimedPending.runId, error: projectedError },
      undefined,
      { strict: true },
    );
  }
  // Settle a deferred terminal admission before the claim marker clears, so
  // a failure here leaves the claimed row discoverable for retry.
  await port.cancelTerminalizedPendingAdmission(claimedPending, projectedError, { settleAsDiscard: true });

  let cleared = false;
  let settledQueueReceipt: QueueAdmissionReceipt | undefined;
  await port.flushUpdate(
    prev => {
      cleared = false;
      settledQueueReceipt = undefined;
      const current = prev.pendingResume;
      if (
        current === undefined ||
        !pendingMatchesDiscardGeneration(current, generation) ||
        current.expiryStartedAt !== discardedAt
      ) {
        return prev;
      }
      cleared = true;
      const next: SessionRecord = { ...prev };
      delete next.pendingResume;
      if (queuedItemId !== undefined) {
        next.pendingQueue = (prev.pendingQueue ?? []).filter(item => item.id !== queuedItemId);
        const queued = prev.queueAdmissionReceipts?.[queuedItemId];
        settledQueueReceipt = queued;
        if (queued && queued.status !== 'completed' && queued.status !== 'failed' && queued.status !== 'dead') {
          settledQueueReceipt = {
            ...queued,
            status: 'failed',
            error: projectedError,
            failedAt: queued.failedAt ?? discardedAt,
            updatedAt: discardedAt,
          };
          next.queueAdmissionReceipts = {
            ...(prev.queueAdmissionReceipts ?? {}),
            [queuedItemId]: settledQueueReceipt,
          };
        }
      }
      return next;
    },
    { skipIfUnchanged: true },
  );
  if (!cleared) return false;

  clearPendingInteractionExpiryTimer(port);
  port.replacementToolSurface.drop(claimedPending.runId);
  if (queuedItemId !== undefined) {
    const resolver = port.queueHead.takeResolver(queuedItemId);
    if (resolver) {
      resolver.reject(discardError);
    }
    port.queueHead.releaseIf(queuedItemId);
    port.emit({
      type: 'queue_failed',
      queuedItemId,
      ...(settledQueueReceipt?.runId ? { runId: settledQueueReceipt.runId } : {}),
      ...(settledQueueReceipt?.signalId ? { signalId: settledQueueReceipt.signalId } : {}),
      ...(settledQueueReceipt?.admissionId ? { admissionId: settledQueueReceipt.admissionId } : {}),
      error: projectedError,
    });
  }
  // The parked run has no live owner (the claim refuses discards while a
  // turn owns the interaction); close it here. Tool rows and agent_end
  // belong to a live turn when one is running, so leave those alone.
  const turnActive = port.currentTurnAbortController !== undefined;
  if (!turnActive) releaseParkedRun(port, claimedPending);
  if (!turnActive) port.emitAbortedToolEnds({ force: true });
  port.emitTurnEvent({
    type: 'resume_failed',
    runId: claimedPending.runId,
    retryable: false,
    error: projectedError,
  });
  if (!turnActive) port.emitAgentEnd({ runId: claimedPending.runId, finishReason: 'error' });
  port.notifyMaybeIdle();
  if (port.state === 'live' && opts.drainQueue !== false) void port.maybeDrainQueue();
  return true;
}

/**
 * PF-8122: a suspension settled with no live turn (discarded, expired or stopped)
 * leaves its agent run parked on the thread, and a parked run holds the
 * thread until the suspended-run TTL sweeps it, so the next message cannot
 * start a fresh run. Abort that parked run so the thread is free again.
 */
export function releaseParkedRun(port: PendingInteractionsSessionPort, pending: PendingResume): void {
  try {
    const { agent } = port.harness._resolveAgentForRuntimeDependencies(
      runtimeDependenciesForPendingResume(port, pending),
      'settled pending interaction release',
    );
    agent.abortRunStream(pending.runId);
  } catch (err) {
    // The interaction is already settled; the TTL sweep still releases the run.
    console.error('[harness/v1] parked run release after a settled pending interaction failed:', err);
  }
}

/** The discard receipt whose claim is stamped on `pending`, if any. */
export function discardClaimForPending(
  port: PendingInteractionsSessionPort,
  pending: PendingResume,
): InboxResponseReceipt | undefined {
  return discardClaimReceipt(port.record, pending);
}

export function findDiscardReceipt(
  port: PendingInteractionsSessionPort,
  generation: DiscardGeneration,
): InboxResponseReceipt | undefined {
  return findDiscardReceiptIn(port.record, generation);
}

/** Throw the typed discard outcome when `pending`'s generation was discarded. */
export function throwIfPendingGenerationDiscarded(port: PendingInteractionsSessionPort, pending: PendingResume): void {
  const discard = findDiscardReceipt(port, {
    kind: pending.kind,
    itemId: pending.itemId ?? pending.toolCallId,
    runId: pending.runId,
    toolCallId: pending.toolCallId,
    pendingRequestedAt: pending.requestedAt,
  });
  if (discard !== undefined) throw discardedErrorForReceipt(port, discard);
}

export function discardedErrorForReceipt(
  port: PendingInteractionsSessionPort,
  receipt: InboxResponseReceipt,
): HarnessPendingInteractionDiscardedError {
  const reason = (receipt.response as { reason?: unknown } | undefined)?.reason;
  return new HarnessPendingInteractionDiscardedError(
    port.id,
    receipt.itemId,
    typeof reason === 'string' ? reason : 'discarded',
  );
}

export function discardPendingInteractionFailure(
  port: PendingInteractionsSessionPort,
  err: unknown,
  responseId: string,
): unknown {
  // Storage errors raised inside the discard's own writes (e.g. the claim
  // rejected by the record size budget) carry the discard's response subject.
  if (err instanceof HarnessStorageError && err.operation !== 'inbox_response') {
    return new HarnessStorageError({
      operation: 'inbox_response',
      cause: err.cause ?? err,
      retryable: err.retryable,
      sessionId: port.id,
      resourceId: port.resourceId,
      threadId: port.threadId,
      harnessName: port.record.harnessName,
      subject: { kind: 'inbox_response', id: responseId },
    });
  }
  if (
    err instanceof HarnessError ||
    err instanceof HarnessStorageError ||
    err instanceof HarnessValidationError ||
    err instanceof HarnessInboxResponseConflictError ||
    err instanceof HarnessSessionClosedError ||
    err instanceof HarnessSessionClosingError ||
    err instanceof HarnessSessionDeletedError ||
    err instanceof HarnessSessionLockedError
  ) {
    return err;
  }
  return new HarnessStorageError({
    operation: 'inbox_response',
    cause: err,
    retryable: true,
    sessionId: port.id,
    resourceId: port.resourceId,
    threadId: port.threadId,
    harnessName: port.record.harnessName,
    subject: { kind: 'inbox_response', id: responseId },
  });
}

/**
 * The discard receipt owning the generation this answer targeted: the
 * answer's explicit generation fields when present, else the pending
 * interaction it observed on entry.
 */
export function discardReceiptForAnswer(
  port: PendingInteractionsSessionPort,
  expectedKind: PendingResume['kind'],
  responseOptions: InboxResponseOptions,
  observed: PendingResume | undefined,
): InboxResponseReceipt | undefined {
  const options = responseOptions as Record<string, unknown>;
  const itemId = getOwnRecordValue(options, 'itemId');
  const runId = getOwnRecordValue(options, 'runId');
  const toolCallId = getOwnRecordValue(options, 'toolCallId');
  const pendingRequestedAt = getOwnRecordValue(options, 'pendingRequestedAt');
  if (
    typeof itemId === 'string' &&
    typeof runId === 'string' &&
    typeof toolCallId === 'string' &&
    typeof pendingRequestedAt === 'number'
  ) {
    return findDiscardReceipt(port, { kind: expectedKind, itemId, runId, toolCallId, pendingRequestedAt });
  }
  if (observed === undefined || observed.kind !== expectedKind) return undefined;
  return findDiscardReceipt(port, {
    kind: observed.kind,
    itemId: observed.itemId ?? observed.toolCallId,
    runId: observed.runId,
    toolCallId: observed.toolCallId,
    pendingRequestedAt: observed.requestedAt,
  });
}

export function resolveStoredInboxResponse(
  port: PendingInteractionsSessionPort,
  expectedKind: PendingResume['kind'],
  resumeData: unknown,
  responseOptions: InboxResponseOptions,
): InboxResponseResult | undefined {
  const responseId = getOwnRecordValue(responseOptions as Record<string, unknown>, 'responseId');
  if (typeof responseId !== 'string') return undefined;
  const receipt = getOwnRecordValue(port.record.inboxResponseReceipts, responseId);
  if (receipt === undefined) return undefined;
  if (receipt.kind !== expectedKind) {
    throw new HarnessInboxResponseConflictError(port.id, receipt.itemId, responseId);
  }
  const requestedItemId = getOwnRecordValue(responseOptions as Record<string, unknown>, 'itemId');
  if (requestedItemId !== undefined && typeof requestedItemId !== 'string') {
    throw new HarnessValidationError(`respond[${expectedKind}].itemId`, 'itemId must be a string');
  }
  if (requestedItemId !== undefined && receipt.itemId !== requestedItemId) {
    throw new HarnessInboxItemNotFoundError(
      port.id,
      requestedItemId,
      expectedKind === 'sandbox-access' ? undefined : expectedKind,
    );
  }
  const requestedRunId = getOwnRecordValue(responseOptions as Record<string, unknown>, 'runId');
  const requestedToolCallId = getOwnRecordValue(responseOptions as Record<string, unknown>, 'toolCallId');
  const requestedPendingRequestedAt = getOwnRecordValue(
    responseOptions as Record<string, unknown>,
    'pendingRequestedAt',
  );
  if (
    receipt.runId !== requestedRunId ||
    receipt.toolCallId !== requestedToolCallId ||
    receipt.pendingRequestedAt !== requestedPendingRequestedAt
  ) {
    throw new HarnessInboxResponseConflictError(port.id, receipt.itemId, responseId);
  }
  // A retried answer reusing the responseId of a `discardPendingInteraction`
  // replays the discard outcome rather than a payload conflict.
  if (receipt.discardedAt !== undefined) {
    throw discardedErrorForReceipt(port, receipt);
  }
  const attemptedHash = computeInboxResponseHash(port, {
    kind: expectedKind,
    itemId: receipt.itemId,
    runId: receipt.runId,
    pendingRequestedAt: receipt.pendingRequestedAt,
    response: resumeData,
    ...(expectedKind === 'tool-approval' ? { approvalScope: responseOptions.approvalScope ?? 'once' } : {}),
  });
  if (attemptedHash !== receipt.responseHash) {
    throw new HarnessInboxResponseConflictError(port.id, receipt.itemId, responseId);
  }
  throwStoredInboxResponseFailure(port, receipt);
  return inboxReceiptResult(port, receipt, true);
}

export function assertMatchingInboxReceipt(
  port: PendingInteractionsSessionPort,
  receipt: InboxResponseReceipt,
  input: { kind: PendingResume['kind']; itemId: string; responseId: string; responseHash: string },
): void {
  if (receipt.kind !== input.kind || receipt.itemId !== input.itemId || receipt.responseHash !== input.responseHash) {
    throw new HarnessInboxResponseConflictError(port.id, input.itemId, input.responseId);
  }
}

export function inboxReceiptResult(
  port: PendingInteractionsSessionPort,
  receipt: InboxResponseReceipt,
  duplicate: boolean,
): InboxResponseResult {
  return {
    itemId: receipt.itemId,
    kind: receipt.kind,
    status: receipt.status === 'applied' ? 'applied' : 'accepted',
    responseId: receipt.responseId,
    duplicate,
  };
}

export function throwStoredInboxResponseFailure(
  port: PendingInteractionsSessionPort,
  receipt: InboxResponseReceipt,
): void {
  if (receipt.status !== 'failed' && receipt.status !== 'dead') return;
  if (receipt.status === 'failed' && receipt.retryable === true) return;
  throw publicErrorProjectionToError(
    receipt.error ?? { code: 'harness.inbox_response_failed', message: 'inbox response failed' },
  );
}

export async function applyInboxReceiptFromCompletedQueue(
  port: PendingInteractionsSessionPort,
  receipt: InboxResponseReceipt,
): Promise<InboxResponseReceipt | undefined> {
  if (receipt.queuedItemId === undefined) return undefined;
  const completed = port.record.queueAdmissionReceipts?.[receipt.queuedItemId];
  if (completed?.status !== 'completed' || completed.runId !== receipt.runId || completed.result === undefined) {
    return undefined;
  }
  await markInboxResponseApplied(port, receipt.responseId, completed.result as AgentResult);
  return getOwnRecordValue(port.record.inboxResponseReceipts, receipt.responseId) ?? receipt;
}

export async function markInboxResponseApplied(
  port: PendingInteractionsSessionPort,
  responseId: string,
  result: AgentResult,
): Promise<void> {
  const appliedAt = Date.now();
  await port.flushUpdate(prev => {
    const receipt = getOwnRecordValue(prev.inboxResponseReceipts, responseId);
    if (!receipt || receipt.status === 'applied' || receiptGenerationDiscarded(prev, receipt)) return prev;
    return {
      ...prev,
      inboxResponseReceipts: {
        ...(prev.inboxResponseReceipts ?? {}),
        [responseId]: {
          ...receipt,
          status: 'applied',
          result,
          appliedAt: receipt.appliedAt ?? appliedAt,
          updatedAt: appliedAt,
        },
      },
    };
  });
}

export async function recordInboxResponsePreDispatchFailure(
  port: PendingInteractionsSessionPort,
  input: {
    responseId: string;
    responseHash: string;
    itemId: string;
    queuedItemId?: string;
    kind: PendingResume['kind'];
    pending: PendingResume;
    response: unknown;
    approvalScope?: 'once' | 'always';
  },
  err: unknown,
): Promise<void> {
  const failedAt = Date.now();
  await port.flushUpdate(prev => {
    // Never persist a contrary refusal for a generation a discard owns.
    if (
      findDiscardReceiptIn(prev, {
        kind: input.kind,
        itemId: input.itemId,
        runId: input.pending.runId,
        toolCallId: input.pending.toolCallId,
        pendingRequestedAt: input.pending.requestedAt,
      }) !== undefined
    ) {
      return prev;
    }
    const currentReceipt = getOwnRecordValue(prev.inboxResponseReceipts, input.responseId);
    if (currentReceipt !== undefined) {
      assertMatchingInboxReceipt(port, currentReceipt, {
        kind: input.kind,
        itemId: input.itemId,
        responseId: input.responseId,
        responseHash: input.responseHash,
      });
      return prev;
    }
    return {
      ...prev,
      inboxResponseReceipts: {
        ...(prev.inboxResponseReceipts ?? {}),
        [input.responseId]: {
          responseId: input.responseId,
          responseHash: input.responseHash,
          resumeAttemptId: input.responseId,
          itemId: input.itemId,
          ...(input.queuedItemId !== undefined ? { queuedItemId: input.queuedItemId } : {}),
          kind: input.kind,
          runId: input.pending.runId,
          toolCallId: input.pending.toolCallId,
          pendingRequestedAt: input.pending.requestedAt,
          response: input.response,
          ...(input.approvalScope !== undefined ? { approvalScope: input.approvalScope } : {}),
          status: 'failed',
          error: projectHarnessPublicError(err),
          retryable: false,
          acceptedAt: failedAt,
          failedAt,
          updatedAt: failedAt,
        } satisfies InboxResponseReceipt,
      },
    };
  });
}

export function computeInboxResponseHash(
  port: PendingInteractionsSessionPort,
  input: {
    kind: PendingResume['kind'];
    itemId: string;
    runId: string;
    pendingRequestedAt: number;
    response: unknown;
    approvalScope?: 'once' | 'always';
  },
): string {
  return sha256CanonicalJson(input);
}

export function queuedItemIdForPendingResume(
  port: PendingInteractionsSessionPort,
  pending: PendingResume,
): string | undefined {
  if (pending.queuedItemId !== undefined) return pending.queuedItemId;
  if (port.queueHead.currentId !== undefined) return port.queueHead.currentId;
  return (port.record.pendingQueue ?? []).find(item => {
    const receipt = port.record.queueAdmissionReceipts?.[item.id];
    return (receipt?.status === 'accepted' || receipt?.status === 'completed') && receipt.runId === pending.runId;
  })?.id;
}

export function modeIdForPendingResume(port: PendingInteractionsSessionPort, pending: PendingResume): string {
  const queuedItemId = queuedItemIdForPendingResume(port, pending);
  const queuedItem = queuedItemId ? port.record.pendingQueue.find(item => item.id === queuedItemId) : undefined;
  const receipt = queuedItemId ? port.record.queueAdmissionReceipts?.[queuedItemId] : undefined;
  return pending.modeId ?? receipt?.modeId ?? queuedItem?.mode ?? port.record.modeId;
}

export function runtimeDependenciesForPendingResume(
  port: PendingInteractionsSessionPort,
  pending: PendingResume,
): HarnessRuntimeDependencyRefs {
  const queuedItemId = queuedItemIdForPendingResume(port, pending);
  const queuedItem = queuedItemId ? port.record.pendingQueue.find(item => item.id === queuedItemId) : undefined;
  const receipt = queuedItemId ? port.record.queueAdmissionReceipts?.[queuedItemId] : undefined;
  const modeId = modeIdForPendingResume(port, pending);
  const modelId = queuedItem?.model ?? port.record.modelId;
  return pending.runtimeDependencies ?? receipt?.runtimeDependencies ?? { modeId, ...(modelId ? { modelId } : {}) };
}

export async function maybeRecoverStaleQueuedResume(
  port: PendingInteractionsSessionPort,
  { kickDrainOnStale = true }: { kickDrainOnStale?: boolean } = {},
): Promise<QueueResumeRecoveryResult> {
  if (port.currentTurnAbortController !== undefined) return { status: 'none' };
  const pending = port.record.pendingResume;
  if (pending?.resumedAt === undefined) return { status: 'none' };
  const queuedItemId = queuedItemIdForPendingResume(port, pending);
  if (queuedItemId === undefined) return { status: 'none' };
  // §10.2: no queue_item_replayed event on crash-recovery — the recovered
  // turn flows through normal drain/agent_start; settlement evidence stays durable.
  port.ensureQueuedItemContext(queuedItemId);

  const currentReceipt = port.record.queueAdmissionReceipts?.[queuedItemId];
  if (currentReceipt?.status === 'completed') {
    const queuedItem = port.record.pendingQueue.find(item => item.id === queuedItemId);
    const shouldRunPostRunSideEffects = queuedItem !== undefined && currentReceipt.postRunFinalizedAt === undefined;
    let alreadyAccounted = false;
    if (shouldRunPostRunSideEffects) {
      let tokenUsageDelta: TokenUsage | undefined;
      try {
        const full = currentReceipt.result as FullOutput<unknown>;
        port.captureTurnRunId(full);
        tokenUsageDelta = tokenUsageDeltaFromFullOutput(port, full, pending.accountedTokenUsage);
        alreadyAccounted = true;
        port.messageTokenLedger.markAccounted(pending.runId);
        await port.markQueuedPostRunFinalized(queuedItemId, { tokenUsageDelta });
      } catch (err) {
        port.deferQueuedTurnRetry(
          new QueuePostRunFinalizationPendingError(Date.now() + QUEUE_POST_RUN_FINALIZATION_RETRY_MS, err),
        );
        return { status: 'none' };
      }
    }
    await port.flushUpdate(prev => {
      const current = prev.pendingResume;
      if (
        !current ||
        current.runId !== pending.runId ||
        current.toolCallId !== pending.toolCallId ||
        current.resumedAt !== pending.resumedAt
      ) {
        return prev;
      }
      const next: SessionRecord = {
        ...prev,
        pendingQueue: (prev.pendingQueue ?? []).filter(item => item.id !== queuedItemId),
      };
      delete next.pendingResume;
      return next;
    });
    if (shouldRunPostRunSideEffects && queuedItem) {
      await port.finalizeQueuedRunCompletion(
        queuedItem,
        currentReceipt.result as FullOutput<unknown>,
        pending.modeId ?? currentReceipt.modeId ?? queuedItem.mode ?? port.record.modeId,
        undefined,
        { skipTokenAccounting: alreadyAccounted },
      );
    }
    port.queueHead.release();
    const resolver = port.queueHead.takeResolver(queuedItemId);
    if (resolver) {
      resolver.resolve(currentReceipt.result as AgentResult);
    }
    port.notifyMaybeIdle();
    void port.maybeDrainQueue();
    return { status: 'completed', result: currentReceipt.result as AgentResult };
  }

  const retryAt = pending.resumedAt + QUEUE_ACCEPTED_RECOVERY_STALE_MS;
  if (Date.now() < retryAt) {
    if (port.queuedResumeRecoveryTimer === undefined) {
      const delayMs = Math.max(0, retryAt - Date.now());
      port.queuedResumeRecoveryTimer = setTimeout(() => {
        port.queuedResumeRecoveryTimer = undefined;
        void port.maybeDrainQueue();
      }, delayMs);
      port.queuedResumeRecoveryTimer.unref?.();
    }
    return { status: 'none' };
  }

  if (port.queuedResumeRecoveryTimer !== undefined) {
    clearTimeout(port.queuedResumeRecoveryTimer);
    port.queuedResumeRecoveryTimer = undefined;
  }

  const recovered = await recoverStaleResumeAdmission(port, pendingInteractionGeneration(port, pending), {
    drainQueue: kickDrainOnStale,
  });
  return recovered ? { status: 'stale' } : { status: 'none' };
}

/**
 * [PF-7749] Whether a tool's interaction pre-registration belongs to this
 * session's active run. A nested agent-as-tool run inherits this session's
 * Harness slot but is not this session's run; its interaction stays with
 * the suspend capture of the run that owns it. A registration without a run
 * id targets the active run.
 */
export function ownsPreRegistrationRun(port: PendingInteractionsSessionPort, runId: string | undefined): boolean {
  return runId === undefined || runId === port.currentRunId;
}

/**
 * [PF-8058] The origin a tool pre-registration stamps on its pending: the
 * owned `signal()` turn's signal, when `runId` is the run that turn
 * dispatched. This is the `originSignalId` the suspend capture would add.
 */
export function preRegistrationOrigin(
  port: PendingInteractionsSessionPort,
  runId: string,
): { originSignalId?: string } {
  const origin = port.ownedSignalTurnOrigin;
  return origin !== undefined && origin.runId === runId ? { originSignalId: origin.signalId } : {};
}

export async function registerQuestion(
  port: PendingInteractionsSessionPort,
  params: RegisterQuestionParams & { runId?: string; toolCallId?: string; modeId?: string; modelId?: string },
): Promise<void> {
  port.assertOpenForTurn('ctx.registerQuestion');
  if (typeof params.questionId !== 'string' || params.questionId.length === 0) {
    throw new HarnessValidationError('ctx.registerQuestion.questionId', 'must be a non-empty string');
  }
  if (typeof params.question !== 'string' || params.question.length === 0) {
    throw new HarnessValidationError('ctx.registerQuestion.question', 'must be a non-empty string');
  }
  if (
    params.selectionMode !== undefined &&
    params.selectionMode !== 'single_select' &&
    params.selectionMode !== 'multi_select'
  ) {
    throw new HarnessValidationError('ctx.registerQuestion.selectionMode', 'must be single_select or multi_select');
  }
  const runId = params.runId ?? port.currentRunId;
  const toolCallId = params.toolCallId ?? params.questionId;
  if (!runId) {
    throw new HarnessValidationError('ctx.registerQuestion.runId', 'active run id is required');
  }
  if (!ownsPreRegistrationRun(port, params.runId)) return;
  const pending: PendingResume = {
    kind: 'question',
    itemId: params.questionId,
    runId,
    toolCallId,
    toolName: ASK_USER_TOOL_NAME,
    ...pendingResumeSourceFields(port),
    ...preRegistrationOrigin(port, runId),
    ...newPendingInteractionTiming(port, 'question'),
    modeId: params.modeId ?? port.record.modeId,
    // §4.2e — carry the active turn's yolo so a later tool approval on the
    // resumed run is still auto-granted (pre-registered pendings win over the
    // suspend-capture merge, so yolo must be stamped here too).
    ...(port.currentTurnYolo ? { yolo: true } : {}),
    runtimeDependencies: port.harness._runtimeDependenciesForMode(
      params.modeId ?? port.record.modeId,
      params.modelId ?? port.modelIdForQueuedItem(port.queueHead.currentId),
    ),
    // §5.1 — carry the active turn's caller app bag onto the pending resume.
    ...(port.currentTurnRequestContext
      ? { requestContext: clonePersistedRequestContext(port.currentTurnRequestContext) }
      : {}),
    ...(port.currentTurnSourceWriteGuard
      ? { observationalMemorySourceWriteGuard: port.currentTurnSourceWriteGuard }
      : {}),
    ...(currentToolSurfaceFenceSnapshot(port, runId)
      ? { toolSurfaceFence: currentToolSurfaceFenceSnapshot(port, runId) }
      : {}),
    ...subagentBudgetSpentProjection(port),
    payload: {
      question: params.question,
      ...(params.options ? { options: params.options } : {}),
      ...(params.selectionMode ? { selectionMode: params.selectionMode } : {}),
    },
  };
  let registered = false;
  let occupied = false;
  await port.flushUpdate(prev => {
    const slot = preRegistrationSlot(prev.pendingResume, 'question', runId, toolCallId);
    registered = slot === 'free';
    occupied = slot === 'occupied';
    return registered ? { ...prev, pendingResume: pending } : prev;
  });
  if (occupied) return;
  if (pending.toolSurfaceFence !== undefined) retainCurrentReplacementToolSurface(port, runId);
  if (!registered) return;
  if (port.record.pendingResume) emitPendingEvent(port, port.record.pendingResume);
}

export async function registerSandboxAccess(
  port: PendingInteractionsSessionPort,
  params: RegisterSandboxAccessParams & { runId?: string; toolCallId?: string; modeId?: string; modelId?: string },
): Promise<void> {
  port.assertOpenForTurn('ctx.registerSandboxAccess');
  if (typeof params.requestId !== 'string' || params.requestId.length === 0) {
    throw new HarnessValidationError('ctx.registerSandboxAccess.requestId', 'must be a non-empty string');
  }
  const validSemanticTypes = new Set(['file', 'command', 'network', 'mcp', 'custom']);
  if (!validSemanticTypes.has(params.semanticType)) {
    throw new HarnessValidationError(
      'ctx.registerSandboxAccess.semanticType',
      "must be one of 'file' | 'command' | 'network' | 'mcp' | 'custom'",
    );
  }
  if (params.reason !== undefined && typeof params.reason !== 'string') {
    throw new HarnessValidationError('ctx.registerSandboxAccess.reason', 'must be a string when provided');
  }
  const sanitizedPayload =
    params.payload !== undefined ? assertJsonValue(params.payload, 'ctx.registerSandboxAccess.payload') : undefined;
  const runId = params.runId ?? port.currentRunId;
  const toolCallId = params.toolCallId ?? params.requestId;
  if (!runId) {
    throw new HarnessValidationError('ctx.registerSandboxAccess.runId', 'active run id is required');
  }
  const modeId = params.modeId ?? port.record.modeId;
  const pending: PendingResume = {
    kind: 'sandbox-access',
    itemId: params.requestId,
    runId,
    toolCallId,
    ...pendingResumeSourceFields(port),
    ...preRegistrationOrigin(port, runId),
    ...newPendingInteractionTiming(port, 'sandbox-access'),
    modeId,
    // §4.2e — see _registerQuestion: carry yolo through pre-registered pendings.
    ...(port.currentTurnYolo ? { yolo: true } : {}),
    runtimeDependencies: port.harness._runtimeDependenciesForMode(
      modeId,
      params.modelId ?? port.modelIdForQueuedItem(port.queueHead.currentId),
    ),
    // §5.1 — carry the active turn's caller app bag onto the pending resume.
    ...(port.currentTurnRequestContext
      ? { requestContext: clonePersistedRequestContext(port.currentTurnRequestContext) }
      : {}),
    ...(port.currentTurnSourceWriteGuard
      ? { observationalMemorySourceWriteGuard: port.currentTurnSourceWriteGuard }
      : {}),
    ...(currentToolSurfaceFenceSnapshot(port, runId)
      ? { toolSurfaceFence: currentToolSurfaceFenceSnapshot(port, runId) }
      : {}),
    ...subagentBudgetSpentProjection(port),
    payload: {
      sandboxAccess: {
        semanticType: params.semanticType,
        ...(params.reason !== undefined ? { reason: params.reason } : {}),
        ...(sanitizedPayload !== undefined ? { payload: sanitizedPayload } : {}),
      },
    },
  };
  let registered = false;
  await port.flushUpdate(prev => {
    const current = prev.pendingResume;
    if (current) {
      const currentSandboxAccess = current.payload?.sandboxAccess;
      if (
        current.kind === 'sandbox-access' &&
        current.runId === runId &&
        current.toolCallId === toolCallId &&
        current.itemId === params.requestId &&
        currentSandboxAccess?.semanticType === params.semanticType &&
        currentSandboxAccess?.reason === params.reason &&
        jsonValuesEqual(currentSandboxAccess?.payload, sanitizedPayload)
      ) {
        return prev;
      }
      throw new HarnessValidationError('ctx.registerSandboxAccess', `pending resume is already "${current.kind}"`);
    }
    registered = true;
    return { ...prev, pendingResume: pending };
  });
  if (pending.toolSurfaceFence !== undefined) retainCurrentReplacementToolSurface(port, runId);
  if (!registered) return;
  // §10.2: sandbox/path-access has no dedicated event — it projects to
  // question_pending via the captured pending resume.
  if (port.record.pendingResume) emitPendingEvent(port, port.record.pendingResume);
}

export async function registerPlanApproval(
  port: PendingInteractionsSessionPort,
  params: RegisterPlanApprovalParams & { runId?: string; toolCallId?: string; modeId?: string; modelId?: string },
): Promise<void> {
  port.assertOpenForTurn('ctx.registerPlanApproval');
  if (typeof params.planId !== 'string' || params.planId.length === 0) {
    throw new HarnessValidationError('ctx.registerPlanApproval.planId', 'must be a non-empty string');
  }
  if (params.title !== undefined && typeof params.title !== 'string') {
    throw new HarnessValidationError('ctx.registerPlanApproval.title', 'must be a string when provided');
  }
  if (typeof params.plan !== 'string' || params.plan.length === 0) {
    throw new HarnessValidationError('ctx.registerPlanApproval.plan', 'must be a non-empty string');
  }
  const runId = params.runId ?? port.currentRunId;
  const toolCallId = params.toolCallId ?? params.planId;
  if (!runId) {
    throw new HarnessValidationError('ctx.registerPlanApproval.runId', 'active run id is required');
  }
  if (!ownsPreRegistrationRun(port, params.runId)) return;
  const submittingModeId = params.modeId ?? port.record.modeId;
  const submittingMode = port.harness._getMode(submittingModeId);
  const pending: PendingResume = {
    kind: 'plan-approval',
    itemId: params.planId,
    runId,
    toolCallId,
    toolName: SUBMIT_PLAN_TOOL_NAME,
    ...pendingResumeSourceFields(port),
    ...preRegistrationOrigin(port, runId),
    ...newPendingInteractionTiming(port, 'plan-approval'),
    modeId: submittingModeId,
    // §4.2e — see _registerQuestion: carry yolo through pre-registered pendings.
    ...(port.currentTurnYolo ? { yolo: true } : {}),
    runtimeDependencies: port.harness._runtimeDependenciesForMode(
      submittingModeId,
      params.modelId ?? port.modelIdForQueuedItem(port.queueHead.currentId),
    ),
    // §5.1 — carry the active turn's caller app bag onto the pending resume.
    ...(port.currentTurnRequestContext
      ? { requestContext: clonePersistedRequestContext(port.currentTurnRequestContext) }
      : {}),
    ...(port.currentTurnSourceWriteGuard
      ? { observationalMemorySourceWriteGuard: port.currentTurnSourceWriteGuard }
      : {}),
    ...(currentToolSurfaceFenceSnapshot(port, runId)
      ? { toolSurfaceFence: currentToolSurfaceFenceSnapshot(port, runId) }
      : {}),
    ...subagentBudgetSpentProjection(port),
    payload: {
      ...(params.title !== undefined ? { title: params.title } : {}),
      plan: params.plan,
    },
    ...(submittingMode.transitionsTo ? { transitionModeId: submittingMode.transitionsTo } : {}),
  };
  let registered = false;
  let occupied = false;
  await port.flushUpdate(prev => {
    const slot = preRegistrationSlot(prev.pendingResume, 'plan-approval', runId, toolCallId);
    registered = slot === 'free';
    occupied = slot === 'occupied';
    return registered ? { ...prev, pendingResume: pending } : prev;
  });
  if (occupied) return;
  if (pending.toolSurfaceFence !== undefined) retainCurrentReplacementToolSurface(port, runId);
  if (!registered) return;
  if (port.record.pendingResume) emitPendingEvent(port, port.record.pendingResume);
}

/** Retain the admission-time definition snapshot for a later same-process resume. */
export function retainCurrentReplacementToolSurface(port: PendingInteractionsSessionPort, runId: string): void {
  const current = port.currentTurnReplacementToolSurface;
  if (!current) return;
  port.replacementToolSurface.retain(runId, current.fence);
}

export function currentToolSurfaceFenceSnapshot(
  port: PendingInteractionsSessionPort,
  runId: string,
): string[] | undefined {
  const fence = readToolSurfaceFence(port.currentAgentRequestContext, runId);
  return fence ? [...fence.allowedNames] : undefined;
}
