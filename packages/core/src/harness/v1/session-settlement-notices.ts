import type { MastraMemory } from '../../memory/memory';
import { isObservationalMemoryEnabled } from '../../memory/types';
import { ObservationalMemorySourceWriteConflictError } from '../../storage/domains/memory/base';
import type { Harness } from './harness';
import type { PendingDelegationSettlementNotice } from './plan-task-session';
import { TASK_CHECK_TOOL_ID } from './plan-task-tool';

/** True when the memory's OM requires every source write to carry a captured guard. */
export function requiresObservationalMemorySourceWriteGuard(memory: MastraMemory): boolean {
  const config = memory.getMergedThreadConfig().observationalMemory;
  return isObservationalMemoryEnabled(config) && typeof config === 'object' && config.sourceWriteFencing === 'required';
}

/** PF-8034: the store refused a guarded write because the guard no longer holds. */
export function isObservationalMemorySourceWriteConflict(error: unknown): boolean {
  return (
    error instanceof ObservationalMemorySourceWriteConflictError ||
    (error instanceof Error && error.name === 'ObservationalMemorySourceWriteConflictError')
  );
}

/**
 * PF-7679: how one settlement notice delivery ended. `wake`: an accepted wake
 * turn. `history`: a row the parent's run loaded (or a `remind` row). `fenced`:
 * a Stop, edit or cancel removed it without a wake. `left_owed`: still owed
 * (closing, lease lost, a failure, or a claim given up); reconcile retries
 * while the parent is live. `lost`: dispatched without a confirmed wake run,
 * counted as delivered. `superseded`: no longer owed by this delivery.
 * `obsolete` (PF-8034): the memory requires OM source-write fencing and the
 * delegating run's guard is missing or revoked (its record was retracted, as
 * when the transcript is deleted). Acknowledged without a row or a wake.
 */
export type DelegationNoticeOutcome = 'wake' | 'history' | 'fenced' | 'left_owed' | 'lost' | 'superseded' | 'obsolete';

export type NoticeClaimResult = 'claimed' | 'claim_lost' | 'fenced' | 'left_owed';

export type NoticeFenceSite = 'owed_reread' | 'row_save' | 'quiescence' | 'claim' | 'dispatch' | 'restore';

/** PF-7679: a struct, so every guarded notice transition decides on each part. */
export interface NoticeFence {
  lifecycle: 'live' | 'closing' | 'closed' | 'deleted' | 'shutting_down';
  /** A Stop or edit since the settlement (the pinned admission epoch is stale). */
  stale: boolean;
  cancelled: boolean;
  /** This process holds the session lease, by its last known expiry. */
  leaseHeld: boolean;
}

export interface NoticeDelivery {
  key: string;
  notice: PendingDelegationSettlementNotice;
  /** The admission epoch pinned for this notice's key. */
  epoch: number;
}

/**
 * PF-7679: settlement-notice claims (`claimId`) each Harness instance holds and
 * has not resolved yet. A stored claim with that instance's `ownerId` whose id
 * is absent here belongs to an earlier tenure, a replaced session object, or a
 * delivery that gave it up, so it may be taken over.
 */
const liveNoticeClaims = new WeakMap<Harness, Set<string>>();

/** Record a notice claim as live in `harness`, before its write is queued. */
export function holdNoticeClaim(harness: Harness, claimId: string): void {
  let held = liveNoticeClaims.get(harness);
  if (held === undefined) {
    held = new Set();
    liveNoticeClaims.set(harness, held);
  }
  held.add(claimId);
}

/** The claim is resolved (cleared, delivered, refused, or given up). */
export function releaseNoticeClaim(harness: Harness, claimId: string): void {
  liveNoticeClaims.get(harness)?.delete(claimId);
}

/** Whether a stored notice claim may be taken over by `harness`. */
export function noticeClaimTakeoverEligible(harness: Harness, claim: { claimId: string; ownerId: string }): boolean {
  return claim.ownerId !== harness.ownerId || liveNoticeClaims.get(harness)?.has(claim.claimId) !== true;
}

/**
 * PF-7631: a settlement wake turn has no caller to retry its terminal commit,
 * so its owning turn re-drives it: at most this many attempts, backing off
 * from the base delay, doubling, capped at the max delay. After the last one
 * the admission is released to dispatch recovery (close or the next owner).
 */
export const WAKE_TERMINAL_COMMIT_MAX_ATTEMPTS = 4;

export const WAKE_TERMINAL_COMMIT_RETRY_BASE_MS = 100;

export const WAKE_TERMINAL_COMMIT_RETRY_MAX_MS = 1_000;

/** PF-8202: rounds a turn waits for `remind` notice saves before it loads history. */
export const REMIND_NOTICE_BARRIER_ROUNDS = 4;

/** PF-8202: a stalled `remind` save must not hold every later turn; its row reaches the next one. */
export const REMIND_NOTICE_BARRIER_TIMEOUT_MS = 5_000;

export function delegationSettlementNoticeText(notice: { taskId: string; outcome: string }): string {
  return `Delegated plan task ${JSON.stringify(notice.taskId)} settled with outcome "${notice.outcome}". Call ${TASK_CHECK_TOOL_ID} with rootTaskId ${JSON.stringify(notice.taskId)} to read its result before continuing that work.`;
}
