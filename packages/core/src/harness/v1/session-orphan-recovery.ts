/**
 * Session recovery of orphaned message dispatches.
 *
 * A message admission stays pending in storage when the process that dispatched
 * it went away before recording a result. These functions scan the session's
 * pending admissions, classify each one (active, finished, claimed by another
 * process, or orphaned), interrupt the orphans through the session's terminal
 * handoff, publish completions that were recorded but never published, and keep
 * the earliest due recheck. Admission itself, the terminal handoff and the
 * delegated-signal orphans stay with `Session` and the delegation driver.
 *
 * It reaches the session only through `OrphanRecoverySessionPort`, which
 * `Session` builds in `_orphanRecoveryPort()`.
 */

import type { Agent } from '../../agent';
import {
  HARNESS_DISPATCH_RECOVERY_SCAN_MAX_LIMIT,
  HARNESS_RUN_INTERRUPTED_ERROR_CODE,
  isHarnessWakeReservedKey,
} from '../../storage/domains/harness';
import type {
  AgentSignalResultEvidence,
  HarnessStorage,
  PendingMessageAdmission,
  PendingMessageAdmissionScanCursor,
  SessionRecord,
} from '../../storage/domains/harness';
import { HarnessConfigError } from './errors';
import { projectHarnessPublicError } from './events';
import type { EmitInput, HarnessEvent } from './events';
import type { Harness } from './harness';
import type { MessageAdmissionStart, SessionLifecycleState } from './session';
import { ORPHANED_DISPATCH_RETRY_MS } from './session-durable-wait';
import { HARNESS_RUN_INTERRUPTED_MESSAGE } from './session-errors';
import type { RunCompletionWaiters } from './session-run-completion-waiters';
import type { RunOutcomeCache } from './session-run-outcome-cache';
import type { TerminalStagingLedger } from './session-terminal-staging-ledger';
import type { WakeAdmissions } from './session-wake-admissions';

export interface OrphanRecoverySessionPort {
  commitInterruptedTerminalHandoff: (
    evidence: AgentSignalResultEvidence,
    runId: string,
    error: { code: string; message: string },
    leaseOwner: { ownerId: string },
  ) => Promise<'committed' | 'completed' | 'unowned' | 'skipped'>;
  emitTurnEvent: (event: EmitInput) => HarnessEvent;
  readonly harness: Harness;
  readonly id: string;
  readonly messageAdmissionStarts: Map<string, MessageAdmissionStart>;
  orphanRecheckAt: number | undefined;
  orphanRecheckSchedules: number;
  orphanRecovery: Promise<{ blockedBy?: { holder: string; retryAt: number } | undefined }> | undefined;
  publishRecoveredRunCompletion: (evidence: AgentSignalResultEvidence) => Promise<void>;
  readonly record: SessionRecord;
  readonly resourceId: string;
  readonly runCompletionWaiters: RunCompletionWaiters;
  readonly runOutcomes: RunOutcomeCache;
  readonly state: SessionLifecycleState;
  readonly storage: HarnessStorage;
  readonly terminalStagingLedger: TerminalStagingLedger;
  readonly threadId: string;
  readonly wakeAdmissions: WakeAdmissions;
}

export async function recoverOrphanedMessageDispatches(
  port: OrphanRecoverySessionPort,
): Promise<{ blockedBy?: { holder: string; retryAt: number } }> {
  if (port.orphanRecovery !== undefined) return port.orphanRecovery;
  const recovery = recoverOrphanedMessageDispatchesOnce(port)
    .catch(error => {
      // A failed attempt stays due, so the next resolution or close retries it.
      scheduleOrphanRecheck(port, Date.now());
      throw error;
    })
    .finally(() => {
      if (port.orphanRecovery === recovery) port.orphanRecovery = undefined;
    });
  port.orphanRecovery = recovery;
  return recovery;
}

export async function recheckOrphanedDispatchesIfDue(port: OrphanRecoverySessionPort): Promise<void> {
  if (port.orphanRecheckAt === undefined || Date.now() < port.orphanRecheckAt) return;
  if (port.state !== 'live') return;
  await recoverOrphanedMessageDispatches(port);
}

/** Keep the earliest pending recheck. */
export function scheduleOrphanRecheck(port: OrphanRecoverySessionPort, at: number): void {
  port.orphanRecheckAt = port.orphanRecheckAt === undefined ? at : Math.min(port.orphanRecheckAt, at);
  port.orphanRecheckSchedules++;
}

export async function recoverOrphanedMessageDispatchesOnce(port: OrphanRecoverySessionPort): Promise<{
  blockedBy?: { holder: string; retryAt: number };
}> {
  if (!port.storage.supportsDispatchRecovery) return {};
  const schedulesAtStart = port.orphanRecheckSchedules;
  const orphaned: AgentSignalResultEvidence[] = [];
  const unpublished: AgentSignalResultEvidence[] = [];
  let blockedBy: { holder: string; retryAt: number } | undefined;
  const block = (holder: string, retryAt: number) => {
    if (blockedBy === undefined || retryAt < blockedBy.retryAt) blockedBy = { holder, retryAt };
  };
  // Released wake admissions this full scan still lists as pending. Only ids
  // released before the scan began can be judged by it.
  const releasedAtScanStart = port.wakeAdmissions.releasedSnapshot();
  const releasedStillPending = new Set<string>();
  let cursor: PendingMessageAdmissionScanCursor | undefined;
  do {
    const page = await port.storage.listPendingMessageAdmissions({
      harnessName: port.record.harnessName,
      sessionId: port.id,
      resourceId: port.resourceId,
      threadId: port.threadId,
      now: Date.now(),
      limit: HARNESS_DISPATCH_RECOVERY_SCAN_MAX_LIMIT,
      ...(cursor !== undefined ? { cursor } : {}),
    });
    for (const admission of page.items) {
      const { evidence } = admission;
      if (evidence.admissionId !== undefined && port.wakeAdmissions.isReleased(evidence.admissionId)) {
        releasedStillPending.add(evidence.admissionId);
      }
      if (
        evidence.status === 'completed' ||
        (evidence.status === 'failed' && evidence.error.code === HARNESS_RUN_INTERRUPTED_ERROR_CODE)
      ) {
        unpublished.push(evidence);
        continue;
      }
      const state = classifyPendingMessageDispatch(port, admission);
      if (state === 'orphaned') orphaned.push(evidence);
      if (state === 'claimed' && evidence.dispatch?.state === 'dispatching') {
        block(evidence.dispatch.attemptId, evidence.dispatch.claimExpiresAt);
      }
      if (state === 'finished') block('unsettled-admission', Date.now() + ORPHANED_DISPATCH_RETRY_MS);
    }
    cursor = page.nextCursor;
  } while (cursor !== undefined);
  // A released wake admission settled by any path (its own detached commit,
  // another owner, or a settlement below) leaves the set, so it stays bounded
  // by the wakes still pending here.
  for (const admissionId of releasedAtScanStart) {
    if (!releasedStillPending.has(admissionId)) port.wakeAdmissions.forgetReleased(admissionId);
  }
  for (const evidence of orphaned) {
    const wakeAdmission = evidence.admissionId !== undefined && isHarnessWakeReservedKey(evidence.admissionId);
    let settled: boolean;
    try {
      settled = await interruptOrphanedMessageDispatch(port, evidence);
    } catch (error) {
      // PF-7631: a wake turn has no caller to report to. Its settlement
      // failure is surfaced, and it blocks a close (typed refusal) while
      // its admission stays durable for a retry or the next owner.
      if (!wakeAdmission) throw error;
      port.emitTurnEvent({
        type: 'error',
        ...(evidence.runId !== undefined ? { runId: evidence.runId } : {}),
        signalId: evidence.signalId,
        error: projectHarnessPublicError(error),
      });
      block('unsettled-admission', Date.now() + ORPHANED_DISPATCH_RETRY_MS);
      continue;
    }
    if (settled && wakeAdmission) port.wakeAdmissions.forgetReleased(evidence.admissionId!);
    const latest = await port.storage.loadMessageResultEvidence({
      harnessName: port.record.harnessName,
      sessionId: port.id,
      resourceId: port.resourceId,
      threadId: port.threadId,
      signalId: evidence.signalId,
    });
    if (settled && latest !== null && 'status' in latest && latest.status !== 'pending') {
      // Cancellation may have won over a staged output. Publish the settled
      // canonical result, never the pending snapshot recovery started with.
      unpublished.push(latest);
      continue;
    }
    // The settlement did not commit (a fresh claim, a moved lease, or a
    // finalizer this process cannot run). A turn still pending blocks.
    if (latest === null || !('status' in latest) || latest.status !== 'pending') continue;
    const claim = latest.dispatch;
    if (claim?.state === 'dispatching' && claim.claimExpiresAt > Date.now()) {
      block(claim.attemptId, claim.claimExpiresAt);
    } else {
      block('unsettled-admission', Date.now() + ORPHANED_DISPATCH_RETRY_MS);
    }
  }
  for (const evidence of unpublished) {
    await port.publishRecoveredRunCompletion(evidence);
  }
  // This recovery replaces the recheck it ran for, but a recheck scheduled
  // while it ran is for a turn it may not have seen: keep the earliest.
  if (port.orphanRecheckSchedules === schedulesAtStart) {
    port.orphanRecheckAt = blockedBy?.retryAt;
  } else if (blockedBy !== undefined) {
    scheduleOrphanRecheck(port, blockedBy.retryAt);
  }
  return blockedBy !== undefined ? { blockedBy } : {};
}

/**
 * `active`: a run for it is in flight in this process (close drains it and
 * its own abort settles the turn), it awaits a response, or the row is
 * malformed — left alone. `finished`: its run finished here but the result
 * was never recorded — never interrupted (a same-admission retry completes
 * it from this process's cache), but it blocks a close. A suspended output
 * is not a result: once its interaction is gone the turn is settled like
 * any other. `claimed`: another process's dispatch claim is still live —
 * left alone until it expires.
 * `orphaned`: interrupt it.
 */
export function classifyPendingMessageDispatch(
  port: OrphanRecoverySessionPort,
  { evidence, dispatchClaim }: PendingMessageAdmission,
): 'active' | 'finished' | 'claimed' | 'orphaned' {
  const runId = evidence.runId;
  if (
    evidence.status !== 'pending' ||
    evidence.operationKind !== 'message' ||
    evidence.admissionId === undefined ||
    evidence.admissionHash === undefined ||
    runId === undefined
  ) {
    return 'active';
  }
  // A suspension parked for resume is waiting on a response, not orphaned.
  if (port.record.pendingResume?.runId === runId) return 'active';
  // A run in flight here is live (or settling) in this process.
  if (
    port.messageAdmissionStarts.has(evidence.admissionId) ||
    port.wakeAdmissions.isInFlight(evidence.admissionId) ||
    port.runCompletionWaiters.has(runId)
  ) {
    return 'active';
  }
  let agent: Agent | undefined;
  try {
    agent = port.harness.getAgentForMode(evidence.modeId ?? port.record.modeId);
  } catch (error) {
    // An unregistered mode/agent cannot be running this run in this process.
    if (!(error instanceof HarnessConfigError)) throw error;
  }
  if (agent?.getRunOutput(runId)) return 'active';
  if (port.wakeAdmissions.isReleased(evidence.admissionId)) return 'orphaned';
  const completed = port.runOutcomes.get(runId);
  if (completed !== undefined && !(completed.ok && completed.full.finishReason === 'suspended')) return 'finished';
  return dispatchClaim === 'live' ? 'claimed' : 'orphaned';
}

export async function interruptOrphanedMessageDispatch(
  port: OrphanRecoverySessionPort,
  evidence: AgentSignalResultEvidence,
): Promise<boolean> {
  const runId = evidence.runId!;
  const error = { code: HARNESS_RUN_INTERRUPTED_ERROR_CODE, message: HARNESS_RUN_INTERRUPTED_MESSAGE };
  // The adapter judges the lease when the settlement commits.
  const leaseOwner = { ownerId: port.harness.ownerId };
  const terminal = await port.commitInterruptedTerminalHandoff(evidence, runId, error, leaseOwner);
  if (terminal === 'completed' || terminal === 'committed') return true;
  if (terminal === 'skipped') return false;
  const swapped = await port.storage.compareAndSwapSignalTerminal({
    harnessName: port.record.harnessName,
    sessionId: port.id,
    resourceId: port.resourceId,
    threadId: port.threadId,
    signalId: evidence.signalId,
    admissionId: evidence.admissionId!,
    admissionHash: evidence.admissionHash!,
    operationKind: 'message',
    expected: evidence.dispatch ?? { state: 'reserved' },
    leaseOwner,
    terminal: {
      status: 'failed',
      signalId: evidence.signalId,
      runId,
      error,
      ...(evidence.modeId !== undefined ? { modeId: evidence.modeId } : {}),
      ...(evidence.modelId !== undefined ? { modelId: evidence.modelId } : {}),
    },
    updatedAt: Date.now(),
  });
  // A lost CAS means the dispatch changed or the lease moved: its new owner
  // settles and reports the turn.
  if (swapped.applied) {
    port.terminalStagingLedger.forgetRun(runId);
  }
  return swapped.applied;
}
