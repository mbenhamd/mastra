import type { InboxResponseReceipt, PendingResume, SessionRecord } from '../../storage/domains/harness';
import { ASK_USER_TOOL_ID, SUBMIT_PLAN_TOOL_ID } from '../../tools/builtin';
import { sha256CanonicalJsonChecked as sha256CanonicalJson } from './canonical-json';
import type { AgentResult, DiscardPendingInteractionResult, InboxResponseOptions } from './types';

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
