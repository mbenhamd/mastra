/**
 * Delegation settlement-notice delivery (PF-7595 / PF-7679 / PF-7750 / PF-8034 /
 * PF-8202), moved out of `Session` (PF-8326).
 *
 * `SessionSettlementNoticeDelivery` owns the delivery state no other part of the
 * session touches (notices in flight, first admission epochs, the `remind`
 * delivery set and its drain) and reaches the session only through
 * `SettlementNoticeDeliverySessionPort`, which `Session` builds in
 * `_settlementNoticeDeliveryPort()`. Session state another concern also writes
 * stays on `Session` and is read through the port: the shutdown barrier's
 * delivery set, the row watches (disposed by the reconcile pass and teardown) and
 * the `remind` settlements the delegation driver marks.
 *
 * Several members are also reached through the port although they live here
 * (`settleDelegationNoticeOutcome`, `logDelegationNotice`, `withNoticeFence`,
 * ...): `Session` keeps a delegate for each, and calling through it keeps one
 * call path that a spy on the session instance observes.
 */

import { randomUUID } from 'node:crypto';

import type { Agent } from '../../agent';
import { createSignal } from '../../agent/signals';
import { watchHistoryLoad } from '../../memory/history-load-watch';
import type { HistoryLoadWatch } from '../../memory/history-load-watch';
import type { MastraMemory } from '../../memory/memory';
import type { RequestContext } from '../../request-context';
import type { HarnessStorage, PersistedRequestContextInput, SessionRecord } from '../../storage/domains/harness';
import { assertObservationalMemorySourceWriteGuardSupported } from '../../storage/domains/memory/base';
import type { ObservationalMemorySourceWriteGuard } from '../../storage/types';
import { sha256CanonicalJsonChecked as sha256CanonicalJson } from './canonical-json';
import { HarnessBusyError } from './errors';
import type { Harness } from './harness';
import {
  planTaskAcknowledgeDelegationSettlementNotice,
  planTaskClaimDelegationSettlementNotice,
  planTaskClearDelegationSettlementNoticeClaim,
  readPendingDelegationSettlementNotice,
} from './plan-task-session';
import type {
  DelegationNoticeClaim,
  PendingDelegationSettlementNotice,
  PlanTaskSessionPort,
} from './plan-task-session';
import type { SessionLifecycleState } from './session';
import { DELEGATION_RECONCILE_RETRY_BASE_MS, DELEGATION_RECONCILE_RETRY_MAX_MS } from './session-delegation';
import {
  DelegationNoticeWakeNotSentError,
  REMIND_NOTICE_BARRIER_ROUNDS,
  REMIND_NOTICE_BARRIER_TIMEOUT_MS,
  delegationSettlementNoticeText,
  holdNoticeClaim,
  isObservationalMemorySourceWriteConflict,
  noticeClaimTakeoverEligible,
  releaseNoticeClaim,
  requiresObservationalMemorySourceWriteGuard,
} from './session-settlement-notices';
import type {
  DelegationNoticeOutcome,
  NoticeClaimResult,
  NoticeDelivery,
  NoticeFence,
  NoticeFenceSite,
  WakeReminderRowOrigin,
} from './session-settlement-notices';
import { NativeSignalAcceptanceTimeoutError } from './session-signal-acceptance';
import type { SessionInjectSystemReminderOptions } from './types';

/** The slice of `Session` that settlement-notice delivery reads and calls. */
export interface SettlementNoticeDeliverySessionPort {
  readonly id: string;
  readonly resourceId: string;
  readonly threadId: string;
  /** Session lifecycle state (`_state`). */
  readonly state: SessionLifecycleState;
  readonly isClosing: boolean;
  readonly harness: Harness;
  /** The current session record (`_record`, reassigned by every flush). */
  readonly record: SessionRecord;
  readonly storage: HarnessStorage;
  /** PF-7631: bumped by every `switchMode()` that changes the mode (`_modeSwitchEpoch`). */
  readonly modeSwitchEpoch: number;
  readonly currentRunModeId: string | undefined;
  readonly currentTurnRequestContext: PersistedRequestContextInput | undefined;
  /** PF-7595: running notice deliveries, awaited by the shutdown barrier (`_delegationNoticeDeliveries`). */
  readonly delegationNoticeDeliveries: Set<Promise<void>>;
  /** PF-7679: kept history-load watches per owed notice key (`_delegationNoticeWatches`). */
  readonly delegationNoticeWatches: Map<string, HistoryLoadWatch>;
  /** PF-8202: `remind` settlements committed but not yet started (`_remindSettlementsStarting`). */
  readonly remindSettlementsStarting: Set<Promise<void>>;
  isRunning(): boolean;
  activeWorkAdmissionIsCurrent(epoch: number): boolean;
  sessionLeaseHeld(): boolean;
  buildRequestContext(turn: {
    modeId: string;
    modelId: string;
    abortSignal: AbortSignal;
    persistedRequestContext?: PersistedRequestContextInput;
  }): Promise<RequestContext>;
  injectSystemReminder(
    content: string,
    opts: SessionInjectSystemReminderOptions,
    internal: {
      signalId: string;
      admissionEpoch: number;
      wakeNoticeId: string;
      wakeSettlement?: Pick<PendingDelegationSettlementNotice, 'taskId' | 'subagentSessionId' | 'outcome'>;
      wakeReminderRow?: WakeReminderRowOrigin;
      claimNotice: () => Promise<boolean>;
      noticeRowDelivered: () => boolean;
    },
  ): Promise<unknown>;
  nextStateTransition(timeoutMs: number): Promise<void>;
  notifyMaybeIdle(): void;
  planTaskPort(): PlanTaskSessionPort;
  scheduleDelegationReconcileRetry(attempt: number): void;
  serializePlanTaskWrite<T>(run: () => Promise<T>): Promise<T>;
  // Members of this module, called through their `Session` delegates.
  settleDelegationNoticeOutcome(
    outcome: DelegationNoticeOutcome,
    notice: PendingDelegationSettlementNotice,
    retryAttempt: number,
  ): void;
  logDelegationNotice(
    level: 'debug' | 'warn' | 'error',
    message: string,
    notice: PendingDelegationSettlementNotice,
    error?: unknown,
  ): void;
  withNoticeFence<T>(
    delivery: NoticeDelivery,
    site: NoticeFenceSite,
    body: (fence: NoticeFence) => Promise<T>,
  ): Promise<T>;
  resolveDelegationNoticeMemory(
    notice: PendingDelegationSettlementNotice,
  ): Promise<{ agent: Agent; memory: MastraMemory; modeId: string } | undefined>;
  awaitDelegationNoticeQuiescence(live: () => boolean): Promise<void>;
  delegationNoticeClaimable(notice: PendingDelegationSettlementNotice): boolean;
  delegationNoticeIsOwed(notice: PendingDelegationSettlementNotice): Promise<boolean>;
  delegationNoticeBackoff(attempt: number, live: () => boolean): Promise<boolean>;
}

export class SessionSettlementNoticeDelivery {
  /** Settlement notices (child id + digest) this process is delivering right now. */
  readonly _delegationNoticesInFlight = new Set<string>();
  /**
   * PF-8202 — `remind` deliveries in flight. A turn waits for them before its
   * run loads history, so a row being saved is in that history and is not
   * acknowledged behind the turn's back. `remind` never waits for the parent
   * to go idle, so this cannot wait on the turn itself.
   */
  private readonly _remindDeliveries = new Set<Promise<void>>();
  /**
   * PF-8202 — whether the one drain of the two sets above is running, and each
   * waiting turn's wake-up with the drain rounds it still waits for. Every turn
   * gets its own rounds from the point it joins, and leaves at its own
   * deadline, so a save that never settles holds one drain and no waiters.
   */
  private _remindNoticeDraining = false;
  private readonly _remindNoticeDrainWaiters = new Map<() => void, number>();
  /**
   * First admission epoch seen for each owed notice in this process, so a retry
   * or a later reconcile pass cannot launder a Stop that happened in between.
   */
  private readonly _delegationNoticeEpochs = new Map<string, number>();

  constructor(private readonly port: SettlementNoticeDeliverySessionPort) {}

  /**
   * PF-8202: wait for the `remind` settlements committed but not yet started,
   * and for the `remind` deliveries in flight, so their rows are saved before a
   * turn's run loads history. A delivery that finishes may be followed by a
   * reconcile pass that starts another, so this repeats; the bound keeps a busy
   * reconcile loop from holding the turn, and one overall deadline keeps a
   * stalled save from doing so. Anything later reaches the next turn. A `remind`
   * delivery never waits for a turn, so this cannot deadlock. Never rejects:
   * each delivery settles its own outcome.
   */
  async _awaitRemindNoticeSaves(): Promise<void> {
    if (this._remindNoticeSavesSettled()) return;
    await new Promise<void>(resolve => {
      const wake = () => {
        clearTimeout(timer);
        this._remindNoticeDrainWaiters.delete(wake);
        resolve();
      };
      const timer = setTimeout(wake, REMIND_NOTICE_BARRIER_TIMEOUT_MS);
      (timer as { unref?: () => void }).unref?.();
      this._remindNoticeDrainWaiters.set(wake, REMIND_NOTICE_BARRIER_ROUNDS);
      if (!this._remindNoticeDraining) void this._drainRemindNoticeSaves();
    });
  }

  private _remindNoticeSavesSettled(): boolean {
    return this.port.remindSettlementsStarting.size === 0 && this._remindDeliveries.size === 0;
  }

  /**
   * Runs while any turn waits. Each round waits for everything in both sets at
   * that point, then counts down every waiter's rounds; a waiter wakes when its
   * rounds run out or both sets are empty. The flag clears in the same step
   * that sees no waiters, so a turn that joins later starts a new drain.
   */
  private async _drainRemindNoticeSaves(): Promise<void> {
    this._remindNoticeDraining = true;
    while (this._remindNoticeDrainWaiters.size > 0) {
      if (this._remindNoticeSavesSettled()) {
        for (const wake of [...this._remindNoticeDrainWaiters.keys()]) wake();
        break;
      }
      await Promise.allSettled(this.port.remindSettlementsStarting);
      await Promise.allSettled(this._remindDeliveries);
      for (const [wake, rounds] of [...this._remindNoticeDrainWaiters]) {
        if (rounds <= 1) wake();
        else this._remindNoticeDrainWaiters.set(wake, rounds - 1);
      }
    }
    this._remindNoticeDraining = false;
  }

  /**
   * Deliver one owed settlement notice as a `system-reminder` signal, at most
   * once per child session id + settlement digest (`key`, hashed into a
   * deterministic signal id, so every redelivery rewrites one message row).
   *
   * 1. Re-read the task: a notice another pass acknowledged, or one held by a
   *    live claim, is not delivered again.
   * 2. Persist the reminder row on the parent thread through the agent's
   *    memory, as the runtime's `persist` signal behaviour does. Nothing is
   *    delivered into a running turn, so the runtime never hands it to an
   *    ungated follow-up run; the next parent turn reads it from history. A
   *    history-load watch on the row, registered before the save, records any
   *    run in this process that loads the row into its context.
   * 3. `remind` acknowledges the notice and never starts a turn.
   * 4. `wake` (PF-7679) waits, unclaimed, until the parent has no running turn,
   *    parked interaction or queued turn, so a close, shutdown or crash during
   *    that wait leaves the notice owed. A Stop or edit since the settlement, or
   *    a run that loaded the row (the watch fired), acknowledges without waking.
   *    Otherwise the wake turn builds its context and claims the notice: the
   *    claim (`notice.claim`, valid while its claimer holds the session lease)
   *    keeps the notice durable, and only its holder dispatches, after a
   *    same-tick re-check and lease check. An accepted wake acknowledges the
   *    notice under the claim. A wake not sent clears its claim (a stale fence
   *    clears it as `remind`) and the delivery waits again; a claim that cannot
   *    be cleared is marked dead in this process, so reconcile takes it over.
   *
   * Under OM `sourceWriteFencing: 'required'` (PF-8034) the row goes to the
   * delegating mode's memory under the source-write guard of the parent run
   * that delegated the task, recorded with the delegation; a wake run in that
   * mode reuses it (`_resolveDelegationNoticeTarget`). A notice with no guard
   * for fenced memory, or whose guard was revoked since (its OM record
   * retracted, as when the transcript is deleted), is obsolete: acknowledged
   * without a row or a wake, never under a new guard.
   *
   * Every exit is a typed outcome (`DelegationNoticeOutcome`). A cancelled
   * parent drops the notice. A closed or torn-down parent, or one whose Harness
   * is shutting down, leaves it owed for restart reconcile; a running delivery
   * is awaited by the shutdown barrier (PF-7595). A notice left owed while the
   * parent is live re-arms reconciliation with the caller's backoff attempt.
   */
  _startDelegationSettlementNotice(
    notice: PendingDelegationSettlementNotice,
    admissionEpoch: number,
    retryAttempt: number,
  ): void {
    // Tracked so shutdown's bounded settle awaits a reminder write in flight.
    const delivery = this._deliverDelegationSettlementNotice(notice, admissionEpoch).then(
      outcome => this.port.settleDelegationNoticeOutcome(outcome, notice, retryAttempt),
      (error: unknown) => {
        this.port.logDelegationNotice('error', 'settlement notice delivery failed', notice, error);
        this.port.settleDelegationNoticeOutcome('left_owed', notice, retryAttempt);
      },
    );
    this.port.delegationNoticeDeliveries.add(delivery);
    const remind = notice.mode === 'remind';
    if (remind) this._remindDeliveries.add(delivery);
    const untrack = () => {
      this.port.delegationNoticeDeliveries.delete(delivery);
      if (remind) this._remindDeliveries.delete(delivery);
    };
    void delivery.then(untrack, untrack);
  }

  /** PF-7679: act on a delivery's typed outcome. No outcome falls through as success. */
  _settleDelegationNoticeOutcome(
    outcome: DelegationNoticeOutcome,
    notice: PendingDelegationSettlementNotice,
    retryAttempt: number,
  ): void {
    switch (outcome) {
      case 'wake':
      case 'history':
      case 'fenced':
      case 'superseded':
        return;
      case 'left_owed':
        if (this.port.state === 'live' && !this.port.isClosing && !this.port.harness._internalIsShuttingDown()) {
          this.port.scheduleDelegationReconcileRetry(retryAttempt);
        }
        return;
      case 'obsolete':
        // Acknowledged durably, like a delivered notice: reconcile does not
        // see it again. Retraction wins over the reminder.
        this.port.logDelegationNotice(
          'warn',
          "settlement notice dropped: the delegating run's source-write guard is missing or revoked",
          notice,
        );
        return;
      case 'lost':
        this.port.logDelegationNotice(
          'error',
          'settlement notice wake dispatched without a confirmed wake run; counted as delivered',
          notice,
        );
        return;
      default: {
        const unexpected: never = outcome;
        throw new Error(`unexpected settlement notice outcome: ${String(unexpected)}`);
      }
    }
  }

  _logDelegationNotice(
    level: 'debug' | 'warn' | 'error',
    message: string,
    notice: PendingDelegationSettlementNotice,
    error?: unknown,
  ): void {
    try {
      this.port.harness.mastra.getLogger()[level](`Harness ${message}`, {
        sessionId: this.port.id,
        taskId: notice.taskId,
        subagentSessionId: notice.subagentSessionId,
        digest: notice.digest,
        ...(error !== undefined ? { error } : {}),
      });
    } catch {
      // Logging never changes the delivery outcome.
    }
  }

  /** PF-7679: the delivery's fence, read synchronously in the tick of the guarded action. */
  private _readNoticeFence(delivery: NoticeDelivery): NoticeFence {
    const lifecycle: NoticeFence['lifecycle'] =
      this.port.state === 'deleted'
        ? 'deleted'
        : this.port.state !== 'live'
          ? 'closed'
          : this.port.harness._internalIsShuttingDown()
            ? 'shutting_down'
            : this.port.isClosing
              ? 'closing'
              : 'live';
    return {
      lifecycle,
      stale: !this.port.activeWorkAdmissionIsCurrent(delivery.epoch),
      cancelled: this.port.record.cancelRequest !== undefined,
      leaseHeld: this.port.sessionLeaseHeld(),
    };
  }

  /**
   * PF-7679: run a notice write at the head of the plan-task write chain (the
   * chain Stop's downgrade uses), with the fence read when it gets there. The
   * body must not queue another serialized write.
   */
  _withNoticeFence<T>(
    delivery: NoticeDelivery,
    site: NoticeFenceSite,
    body: (fence: NoticeFence) => Promise<T>,
  ): Promise<T> {
    return this.port.serializePlanTaskWrite(() => {
      const fence = this._readNoticeFence(delivery);
      if (fence.lifecycle !== 'live' || fence.stale || fence.cancelled || !fence.leaseHeld) {
        this.port.logDelegationNotice(
          'debug',
          `settlement notice fence at ${site}: ${fence.lifecycle}${fence.stale ? ', stale' : ''}${
            fence.cancelled ? ', cancelled' : ''
          }${fence.leaseHeld ? '' : ', lease not held'}`,
          delivery.notice,
        );
      }
      return body(fence);
    });
  }

  private async _deliverDelegationSettlementNotice(
    notice: PendingDelegationSettlementNotice,
    admissionEpoch: number,
  ): Promise<DelegationNoticeOutcome> {
    const key = `${notice.subagentSessionId}:${notice.digest}`;
    // A shutting-down Harness starts no new delivery work; the notice stays
    // owed for the next owner's reconcile.
    const live = () =>
      this.port.state === 'live' && !this.port.isClosing && !this.port.harness._internalIsShuttingDown();
    if (!live()) return 'left_owed';
    // Another pass of this session is delivering it.
    if (this._delegationNoticesInFlight.has(key)) return 'superseded';
    this._delegationNoticesInFlight.add(key);
    let outcome: DelegationNoticeOutcome = 'left_owed';
    try {
      outcome = await this._runDelegationSettlementNotice(notice, admissionEpoch, key, live);
      return outcome;
    } finally {
      this._delegationNoticesInFlight.delete(key);
      // The row's watch is kept for this session's own retry only while the
      // notice is left owed in a live session that holds the lease (a lost
      // lease hands the notice to the next holder); teardown disposes the rest.
      if (outcome !== 'left_owed' || !live() || !this.port.sessionLeaseHeld()) {
        this.port.delegationNoticeWatches.get(key)?.dispose();
        this.port.delegationNoticeWatches.delete(key);
      }
    }
  }

  private async _runDelegationSettlementNotice(
    notice: PendingDelegationSettlementNotice,
    admissionEpoch: number,
    key: string,
    live: () => boolean,
  ): Promise<DelegationNoticeOutcome> {
    const firstEpoch = this._delegationNoticeEpochs.get(key);
    if (firstEpoch === undefined) this._delegationNoticeEpochs.set(key, admissionEpoch);
    else admissionEpoch = firstEpoch;
    const delivery: NoticeDelivery = { key, notice, epoch: admissionEpoch };
    const ids = { taskId: notice.taskId, subagentSessionId: notice.subagentSessionId, digest: notice.digest };
    const takeover = (claim: DelegationNoticeClaim) => noticeClaimTakeoverEligible(this.port.harness, claim);
    // The claim this delivery holds (its write may have landed), until resolved.
    let claimId: string | undefined;
    let watch = this.port.delegationNoticeWatches.get(key);
    const backoff = (attempt: number): Promise<boolean> => this.port.delegationNoticeBackoff(attempt, live);
    // Unserialized body: the caller runs it inside `_withNoticeFence`. A held
    // claim removes only itself; an unclaimed delivery removes a notice with no
    // claim or a claim reconcile may take over.
    const removeNotice = async (): Promise<boolean> => {
      const removed = await planTaskAcknowledgeDelegationSettlementNotice(
        this.port.planTaskPort(),
        claimId !== undefined ? { ...ids, claimId } : { ...ids, takeover },
      );
      if (removed) this._delegationNoticeEpochs.delete(key);
      return removed;
    };
    // Delivered-ack, or the fenced ack of a Stop/edit/cancel. No write after a
    // hard delete or once the lease is lost: the next holder re-checks.
    const acknowledge = (site: NoticeFenceSite): Promise<'removed' | 'not_owed' | 'no_write'> =>
      this.port.withNoticeFence(delivery, site, async fence => {
        if (fence.lifecycle === 'deleted' || !fence.leaseHeld) return 'no_write';
        return (await removeNotice()) ? 'removed' : 'not_owed';
      });
    const acknowledged = async (
      site: NoticeFenceSite,
      outcome: 'history' | 'fenced' | 'obsolete',
    ): Promise<DelegationNoticeOutcome> => {
      const result = await acknowledge(site);
      return result === 'removed' ? outcome : result === 'not_owed' ? 'superseded' : 'left_owed';
    };
    const claim = (id: string): Promise<NoticeClaimResult> =>
      this.port.withNoticeFence(delivery, 'claim', async fence => {
        if (fence.lifecycle !== 'live' || !fence.leaseHeld) return 'left_owed';
        // A Stop, edit or cancel since the settlement: never claim, never wake.
        if (fence.stale || fence.cancelled) return (await removeNotice()) ? 'fenced' : 'claim_lost';
        return planTaskClaimDelegationSettlementNotice(this.port.planTaskPort(), {
          ...ids,
          claim: { claimId: id, ownerId: this.port.harness.ownerId, claimedAt: Date.now() },
          takeover,
        });
      });
    // Clear this delivery's claim without a delivery (a stale or cancelled fence
    // clears it as `remind`). The claim leaves the live set either way: cleared
    // (or never landed, the notice still owed and claimable), gone, or (no
    // write, or a failed one) dead, so reconcile may take it over.
    const resolveClaim = async (): Promise<'cleared' | 'superseded' | 'dead'> => {
      const id = claimId!;
      try {
        return await this.port.withNoticeFence(delivery, 'restore', async fence => {
          if (fence.lifecycle === 'deleted' || !fence.leaseHeld) return 'dead';
          const cleared = await planTaskClearDelegationSettlementNoticeClaim(this.port.planTaskPort(), {
            ...ids,
            claimId: id,
            mode: fence.stale || fence.cancelled ? 'remind' : notice.mode,
          });
          if (cleared) return 'cleared';
          // Not ours: a claim write that never landed leaves the notice owed
          // with no claim, or a dead claim of this owner. A moved digest, a
          // claim live here, or a foreign claim (its owner may hold the lease
          // in storage) supersedes: this delivery never claims over it again.
          const current = await this._loadDelegationNotice(notice);
          const reclaimable =
            current !== undefined &&
            (current.claim === undefined ||
              (current.claim.ownerId === this.port.harness.ownerId && this.port.delegationNoticeClaimable(current)));
          return reclaimable ? 'cleared' : 'superseded';
        });
      } catch (error) {
        this.port.logDelegationNotice('warn', 'settlement notice claim could not be cleared', notice, error);
        return 'dead';
      } finally {
        claimId = undefined;
        releaseNoticeClaim(this.port.harness, id);
      }
    };
    // The wake ran (or a run loaded the row, or a fence ended it): acknowledge
    // under the claim. A fenced-out ack leaves the claim for the next holder. A
    // failed ack is retried with the reconcile backoff while the claim stays
    // live, so no retry of this process re-delivers it. Past the backoff cap,
    // or once the lease, the session or the epoch is lost, the claim is left
    // dead: reconcile or the next holder takes it over (a possible duplicate).
    const finishClaimed = async (outcome: 'wake' | 'history' | 'fenced' | 'lost'): Promise<DelegationNoticeOutcome> => {
      const id = claimId!;
      try {
        for (let attempt = 0; ; attempt += 1) {
          let failure: unknown;
          try {
            const result = await acknowledge('dispatch');
            if (result === 'no_write') {
              this.port.logDelegationNotice(
                'error',
                'settlement notice acknowledgement fenced out; claim left',
                notice,
              );
            }
            return outcome;
          } catch (error) {
            failure = error;
          }
          this.port.logDelegationNotice(
            'warn',
            `settlement notice acknowledgement failed (attempt ${attempt + 1})`,
            notice,
            failure,
          );
          const fence = this._readNoticeFence(delivery);
          const retry =
            fence.lifecycle === 'live' &&
            fence.leaseHeld &&
            !fence.stale &&
            !fence.cancelled &&
            (await backoff(attempt));
          if (!retry) {
            this.port.logDelegationNotice(
              'error',
              'settlement notice acknowledgement given up; claim left dead, so it may be taken over and wake twice',
              notice,
              failure,
            );
            return 'left_owed';
          }
        }
      } finally {
        claimId = undefined;
        releaseNoticeClaim(this.port.harness, id);
      }
    };
    try {
      if (!(await this.port.delegationNoticeIsOwed(notice))) {
        this._delegationNoticeEpochs.delete(key);
        return 'superseded';
      }
      const entry = this._readNoticeFence(delivery);
      if (entry.lifecycle !== 'live' || !entry.leaseHeld) return 'left_owed';
      if (entry.cancelled) return acknowledged('owed_reread', 'fenced');
      const signalId = `harness-delegation-notice-${sha256CanonicalJson({ sessionId: this.port.id, key }).slice(0, 32)}`;
      const content = delegationSettlementNoticeText(notice);
      const attributes = { type: 'delegated-task-settled' };
      const metadata = { taskId: notice.taskId, subagentSessionId: notice.subagentSessionId, digest: notice.digest };
      // Write the reminder row directly, as the runtime's `persist` behaviour
      // does: no synthetic broadcast run occupies the thread, so a following
      // idle wake is not mistaken for an interleave, and nothing is handed to
      // an ungated follow-up run.
      // PF-7631: the mode marker is taken before resolution, so any switch from
      // here on disqualifies the wake grant.
      const modeEpoch = this.port.modeSwitchEpoch;
      const target = await this._resolveDelegationNoticeTarget(notice);
      // Shutdown may have begun while the notice was re-read and memory resolved.
      if (!live()) return 'left_owed';
      // PF-8034: fenced memory with no guard of the delegating run for it.
      if (target === 'obsolete') return acknowledged('row_save', 'obsolete');
      const { resolved, sourceWriteGuard } = target;
      // PF-7631: which agent's memory saved the row, set only once it is written.
      let reminderRow: WakeReminderRowOrigin | undefined;
      if (resolved !== undefined) {
        // PF-7679: watch before the save, so a run that loads history while the
        // row is written either sees it (and is recorded) or does not.
        if (watch === undefined) {
          watch = watchHistoryLoad(this.port.threadId, this.port.resourceId, signalId, () =>
            this.port.notifyMaybeIdle(),
          );
          this.port.delegationNoticeWatches.set(key, watch);
        }
        // PF-8034: an adapter that cannot check a guard is a configuration
        // error, not a revocation: the notice stays owed.
        if (sourceWriteGuard !== undefined) {
          assertObservationalMemorySourceWriteGuardSupported(await resolved.memory.storage.getStore('memory'));
        }
        try {
          await resolved.memory.saveMessages({
            messages: [
              createSignal({
                type: 'system-reminder',
                id: signalId,
                contents: content,
                attributes,
                metadata,
              }).toDBMessage({
                resourceId: this.port.resourceId,
                threadId: this.port.threadId,
              }),
            ],
            ...(sourceWriteGuard ? { observationalMemorySourceWriteGuard: sourceWriteGuard } : {}),
          });
        } catch (error) {
          // PF-8034: the delegating run's guard was revoked (retraction wins).
          // Any other failure leaves the notice owed, as before.
          if (sourceWriteGuard !== undefined && isObservationalMemorySourceWriteConflict(error)) {
            return acknowledged('row_save', 'obsolete');
          }
          throw error;
        }
        reminderRow = { ...resolved, modeEpoch };
      }
      if (!live()) return 'left_owed';
      if (notice.mode === 'remind') return acknowledged('row_save', reminderRow !== undefined ? 'history' : 'fenced');
      const rowDelivered = () => watch?.loaded() === true;
      let claimWriteFailures = 0;
      for (;;) {
        // PF-7679: wait for quiescence before the claim, so a close, shutdown or
        // crash during the wait leaves the notice owed. A run that already
        // loaded the row ends the wait: the notice was delivered, even if a
        // close follows before that run ends.
        await this.port.awaitDelegationNoticeQuiescence(() => live() && !rowDelivered());
        if (rowDelivered()) return acknowledged('quiescence', 'history');
        const fence = this._readNoticeFence(delivery);
        if (fence.lifecycle !== 'live' || !fence.leaseHeld) return 'left_owed';
        // A Stop or edit since the settlement: the row stays; no wake.
        if (fence.stale || fence.cancelled) return acknowledged('quiescence', 'fenced');
        // Live before its write is queued, so a racing reconcile never takes
        // this claim over.
        const id = randomUUID();
        holdNoticeClaim(this.port.harness, id);
        // The wake turn races the claim write against an abort; the outcome
        // below waits for the write itself, so it knows whether it claimed.
        let claimWrite: Promise<NoticeClaimResult> | undefined;
        try {
          await this.port.injectSystemReminder(
            content,
            {
              attributes,
              metadata,
              // PF-8034: a wake run in the mode whose memory took the row writes
              // under the guard the row was saved with; another mode's memory
              // never sees that guard.
              ...(sourceWriteGuard !== undefined && reminderRow?.modeId === this.port.record.modeId
                ? { observationalMemorySourceWriteGuard: sourceWriteGuard }
                : {}),
            },
            {
              signalId,
              admissionEpoch,
              wakeNoticeId: signalId,
              // Only the claim's holder may wake.
              claimNotice: () => {
                claimWrite = claim(id).then(
                  result => {
                    if (result === 'claimed') claimId = id;
                    else releaseNoticeClaim(this.port.harness, id);
                    return result;
                  },
                  (error: unknown) => {
                    // The write may have landed: resolve it as held.
                    claimId = id;
                    throw error;
                  },
                );
                return claimWrite.then(result => result === 'claimed');
              },
              noticeRowDelivered: rowDelivered,
              // PF-7631: the wake grant is considered only over a saved reminder
              // row, and kept only while `_wakeGrantAllowed` holds.
              ...(reminderRow !== undefined
                ? {
                    wakeSettlement: {
                      taskId: notice.taskId,
                      subagentSessionId: notice.subagentSessionId,
                      outcome: notice.outcome,
                    },
                    wakeReminderRow: reminderRow,
                  }
                : {}),
            },
          );
          return finishClaimed('wake');
        } catch (err) {
          let claimResult: NoticeClaimResult | 'failed' | undefined;
          if (claimWrite === undefined) releaseNoticeClaim(this.port.harness, id);
          else claimResult = await claimWrite.catch(() => 'failed' as const);
          if (claimId === undefined) {
            if (claimResult === 'claim_lost') return 'superseded';
            if (claimResult === 'fenced') return 'fenced';
            if (claimResult === 'left_owed') return 'left_owed';
            // A turn or foreign run took the slot before the claim: wait again.
            if (err instanceof HarnessBusyError && live()) {
              await this.port.nextStateTransition(250);
              continue;
            }
            this.port.logDelegationNotice('warn', 'settlement notice wake not sent', notice, err);
            return 'left_owed';
          }
          if (err instanceof DelegationNoticeWakeNotSentError) {
            // A run loaded the row after the claim: that run is the delivery.
            if (err.reason === 'delivered') return finishClaimed('history');
            // A Stop or edit after the claim: the row stays; no wake.
            if (err.reason === 'stale') return finishClaimed('fenced');
            // The dispatch may have started a run (T20): counted as delivered.
            if (err.reason === 'unconfirmed') return finishClaimed('lost');
          }
          if (err instanceof NativeSignalAcceptanceTimeoutError) return finishClaimed('lost');
          // Claimed but not sent: clear the claim before anything else.
          const resolution = await resolveClaim();
          if (resolution === 'superseded') return 'superseded';
          if (resolution === 'dead') return 'left_owed';
          if (claimResult === 'failed') {
            // The claim write failed (a storage error or a version conflict):
            // the notice is still owed, so back off and re-read it by claiming
            // again, up to the reconcile backoff cap.
            this.port.logDelegationNotice('warn', 'settlement notice claim write failed', notice, err);
            if (await backoff(claimWriteFailures++)) continue;
            return 'left_owed';
          }
          if (err instanceof DelegationNoticeWakeNotSentError && err.reason === 'slot_lost' && live()) {
            // A run this process cannot see (a foreign lease holder, or one not
            // yet registered here) refuses every retry: back off as the
            // pre-claim busy path does. A close or Stop ends the loop above.
            await this.port.nextStateTransition(250);
            continue;
          }
          this.port.logDelegationNotice('warn', 'settlement notice wake not sent', notice, err);
          return 'left_owed';
        }
      }
    } catch (error) {
      // Before the claim, or after it is resolved, the notice stays durable and
      // the row is rewritten in place on retry, so copies cannot accumulate.
      this.port.logDelegationNotice('warn', 'settlement notice delivery left owed', notice, error);
      if (claimId !== undefined) await resolveClaim();
      return 'left_owed';
    }
  }

  /**
   * Memory that receives a settlement reminder row. The active mode's agent
   * memory comes first (a per-turn override may run another agent). When that
   * agent has none (the parent switched to a memory-less mode that cannot
   * delegate), the reminder goes to the memory of the mode that delegated the
   * task: binding requires memory there for `remind`, and the parent's next
   * turn in that mode reads the row from its thread history. Memory resolves
   * with the parent's own request context, as its turns do, so a dynamic memory
   * resolver selects the parent's store. Undefined only when neither mode has
   * memory (a Stop-downgraded `wake` notice of a memory-less parent, or a
   * delegating mode removed from the configuration since).
   */
  async _resolveDelegationNoticeMemory(
    notice: PendingDelegationSettlementNotice,
  ): Promise<{ agent: Agent; memory: MastraMemory; modeId: string } | undefined> {
    const activeModeId = this.port.currentRunModeId ?? this.port.record.modeId;
    const modeIds = [activeModeId];
    if (notice.parentModeId !== undefined && notice.parentModeId !== activeModeId) {
      if (this.port.harness._hasMode(notice.parentModeId)) modeIds.push(notice.parentModeId);
    }
    for (const modeId of modeIds) {
      const resolved = await this._resolveModeNoticeMemory(modeId);
      if (resolved !== undefined) return resolved;
    }
    return undefined;
  }

  /** One mode's agent memory, resolved with the parent's own request context. */
  private async _resolveModeNoticeMemory(
    modeId: string,
  ): Promise<{ agent: Agent; memory: MastraMemory; modeId: string } | undefined> {
    const agent = this.port.harness.getAgentForMode(modeId);
    const requestContext = await this.port.buildRequestContext({
      modeId,
      modelId: this.port.record.modelId,
      abortSignal: new AbortController().signal,
      ...(this.port.currentTurnRequestContext !== undefined
        ? { persistedRequestContext: this.port.currentTurnRequestContext }
        : {}),
    });
    const memory = await agent.getMemory({ requestContext });
    return memory !== undefined && memory !== null ? { agent, memory, modeId } : undefined;
  }

  /**
   * PF-8034: where a settlement reminder row goes, and under which guard.
   * Memory that requires no OM source-write fencing keeps the order of
   * `_resolveDelegationNoticeMemory` and is written unguarded. The recorded
   * guard belongs to the delegating mode's memory, so a notice that carries
   * one goes there first when that memory requires it. When the memory chosen
   * requires fencing and the guard is not its own, a delegating mode whose
   * memory requires none takes the row unguarded, as its run had no fence;
   * otherwise the recorded guard is tried (its memory rejects it if it is not
   * its own). With no guard at all, the notice is obsolete.
   */
  private async _resolveDelegationNoticeTarget(notice: PendingDelegationSettlementNotice): Promise<
    | {
        resolved?: { agent: Agent; memory: MastraMemory; modeId: string };
        sourceWriteGuard?: ObservationalMemorySourceWriteGuard;
      }
    | 'obsolete'
  > {
    const delegatingModeId =
      notice.parentModeId !== undefined && this.port.harness._hasMode(notice.parentModeId)
        ? notice.parentModeId
        : undefined;
    let delegating: Promise<{ agent: Agent; memory: MastraMemory; modeId: string } | undefined> | undefined;
    const resolveDelegating = () =>
      delegatingModeId === undefined
        ? Promise.resolve(undefined)
        : (delegating ??= this._resolveModeNoticeMemory(delegatingModeId));
    const guard = notice.sourceWriteGuard;
    if (guard !== undefined) {
      const own = await resolveDelegating();
      if (own !== undefined && requiresObservationalMemorySourceWriteGuard(own.memory)) {
        return { resolved: own, sourceWriteGuard: guard };
      }
    }
    const resolved = await this.port.resolveDelegationNoticeMemory(notice);
    if (resolved === undefined || !requiresObservationalMemorySourceWriteGuard(resolved.memory)) {
      return resolved === undefined ? {} : { resolved };
    }
    const unfenced = await resolveDelegating();
    if (unfenced !== undefined && !requiresObservationalMemorySourceWriteGuard(unfenced.memory)) {
      return { resolved: unfenced };
    }
    return guard !== undefined ? { resolved, sourceWriteGuard: guard } : 'obsolete';
  }

  /**
   * PF-7679: wait until the parent has no running turn, no parked interaction
   * and no queued turn, or until the delivery is no longer live. Each of those
   * ends through `_notifyMaybeIdle`; the bounded poll only re-reads the record.
   */
  async _awaitDelegationNoticeQuiescence(live: () => boolean): Promise<void> {
    while (
      live() &&
      (this.port.isRunning() ||
        this.port.record.pendingResume !== undefined ||
        (this.port.record.pendingQueue ?? []).length > 0)
    ) {
      await this.port.nextStateTransition(1_000);
    }
  }

  /**
   * PF-7679: whether this Harness may deliver an owed notice: it carries no
   * claim, or a claim reconcile may take over (its claimer lost the lease, or
   * it is not live in this Harness).
   */
  _delegationNoticeClaimable(notice: PendingDelegationSettlementNotice): boolean {
    return notice.claim === undefined || noticeClaimTakeoverEligible(this.port.harness, notice.claim);
  }

  /** Exact re-read: the stored notice, while the one with this digest is still owed on its task. */
  private async _loadDelegationNotice(
    notice: PendingDelegationSettlementNotice,
  ): Promise<PendingDelegationSettlementNotice | undefined> {
    const snapshot = await this.port.storage.loadPlanTaskSubtree({
      harnessName: this.port.record.harnessName,
      sessionId: this.port.id,
      rootTaskId: notice.taskId,
      limit: 1,
    });
    const task = snapshot.tasks.find(row => row.taskId === notice.taskId);
    const current = task === undefined ? undefined : readPendingDelegationSettlementNotice(task);
    return current?.subagentSessionId === notice.subagentSessionId && current.digest === notice.digest
      ? current
      : undefined;
  }

  /** Exact re-read: the notice with this digest is still owed on its task, and claimable. */
  async _delegationNoticeIsOwed(notice: PendingDelegationSettlementNotice): Promise<boolean> {
    const current = await this._loadDelegationNotice(notice);
    return current !== undefined && this.port.delegationNoticeClaimable(current);
  }

  /**
   * PF-7750: wait the reconcile backoff for `attempt` (0-based) of a notice
   * write retry, unless the session stops being live. False once the backoff
   * passes its cap, or not live.
   */
  async _delegationNoticeBackoff(attempt: number, live: () => boolean): Promise<boolean> {
    const backoffMs = DELEGATION_RECONCILE_RETRY_BASE_MS * 2 ** attempt;
    if (backoffMs > DELEGATION_RECONCILE_RETRY_MAX_MS) return false;
    const until = Date.now() + backoffMs;
    while (live() && Date.now() < until) await this.port.nextStateTransition(until - Date.now());
    return live();
  }

  /** PF-7679: dispose the kept row watches whose key `drop` selects, unless a delivery of it runs. */
  _disposeKeptDelegationNoticeWatches(drop: (key: string) => boolean): void {
    for (const [key, watch] of this.port.delegationNoticeWatches) {
      if (!drop(key) || this._delegationNoticesInFlight.has(key)) continue;
      watch.dispose();
      this.port.delegationNoticeWatches.delete(key);
    }
  }
}
