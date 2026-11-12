import type { InboxResponseReceipt, PendingResume, SessionRecord } from '../../storage/domains/harness';
import { ASK_USER_TOOL_ID, SUBMIT_PLAN_TOOL_ID } from '../../tools/builtin';
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
