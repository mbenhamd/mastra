/**
 * The terminal staging ledger of a `Session` (PF-8389): per run, whether its
 * completed output is staged durably, whether its usage joined the durable
 * session counter (the usage fence), whether this process admitted it under
 * native terminal handoff, and the pending terminal admission known here.
 *
 * `Session` creates one `TerminalStagingLedger` and changes these collections
 * only through its operations. Each operation is exactly the statements the
 * session ran inline before, in the same order; nothing here awaits, calls
 * back into the session, or adds a check the session did not make:
 *
 *   - `forgetRun` is the one way a settled run leaves the ledger: it drops
 *     the run's fence, staged mark, cached admission and native mark together.
 *   - The admissions map holds only pending admissions: `cachePending` caches
 *     only a pending one, and `admitNative` refuses (returns `false`, changing
 *     nothing) one that is not.
 *   - `markAccounted` does not check that the run is staged: one caller checks
 *     that first, and the constructor restores the durable fences as they are.
 *   - `accountedRunIds` is the live set, which the fence sweep iterates across
 *     its awaits while it retires entries; `accountedSnapshot` is a copy taken
 *     when it is called, for the session save.
 */

import type { HarnessTerminalAdmissionRecord } from '../../storage/domains/harness';

export class TerminalStagingLedger {
  /** Pending native completions whose usage has joined the durable session counter. */
  private readonly _terminalTokenAccountedRunIds = new Set<string>();
  private readonly _stagedTerminalRunIds = new Set<string>();
  /** Runs this process admitted under native terminal handoff. */
  private readonly _nativeTerminalRunIds = new Set<string>();
  /**
   * Pending terminal admissions known to this process, by run id, so staging a
   * run's output is one storage transaction. Storage re-validates identity and
   * status under its locks, so a stale entry only yields `not_pending`.
   */
  private readonly _terminalStagingAdmissions = new Map<string, HarnessTerminalAdmissionRecord>();

  /**
   * Drop the run's usage fence, staged mark, cached admission and native mark,
   * once the run is settled. The session calls it where this process commits a
   * run's terminal handoff (`_runTerminalHandoffCommit`), cancels a wake grant
   * before dispatch (`_cancelWakeTerminalAdmission`), and where recovery
   * settles an orphaned dispatch (`_interruptOrphanedMessageDispatch`,
   * `_commitInterruptedTerminalHandoff`), including a wake admission this
   * process admitted and then released to recovery.
   */
  forgetRun(runId: string): void {
    this._terminalTokenAccountedRunIds.delete(runId);
    this._stagedTerminalRunIds.delete(runId);
    this._terminalStagingAdmissions.delete(runId);
    this._nativeTerminalRunIds.delete(runId);
  }

  /**
   * Mark the run as admitted natively here and cache its admission, only if
   * the admission is pending. Otherwise it changes nothing and returns `false`,
   * so the admissions map holds only pending admissions, as with
   * `cachePending`. Both callers pass only pending admissions.
   */
  admitNative(runId: string, admission: HarnessTerminalAdmissionRecord): boolean {
    if (admission.status !== 'pending') return false;
    this._nativeTerminalRunIds.add(runId);
    this._terminalStagingAdmissions.set(runId, admission);
    return true;
  }

  /** Cache a loaded admission for the run, only if it is pending. */
  cachePending(runId: string, admission: HarnessTerminalAdmissionRecord | null): void {
    if (admission?.status === 'pending') this._terminalStagingAdmissions.set(runId, admission);
  }

  /** Drop the run's cached admission. */
  forgetAdmission(runId: string): void {
    this._terminalStagingAdmissions.delete(runId);
  }

  /** Mark the run's completed output as staged. */
  markStaged(runId: string): void {
    this._stagedTerminalRunIds.add(runId);
  }

  /** Add the run's usage fence. */
  markAccounted(runId: string): void {
    this._terminalTokenAccountedRunIds.add(runId);
  }

  /** Mark the run as staged, then add its usage fence. */
  markStagedAndAccounted(runId: string): void {
    this._stagedTerminalRunIds.add(runId);
    this._terminalTokenAccountedRunIds.add(runId);
  }

  /** Retire a prior run's usage fence. */
  sweepFence(runId: string): void {
    this._terminalTokenAccountedRunIds.delete(runId);
  }

  /** The pending admission cached for the run, if any. */
  admissionFor(runId: string): HarnessTerminalAdmissionRecord | undefined {
    return this._terminalStagingAdmissions.get(runId);
  }

  /** Whether the run's completed output is staged. */
  isStaged(runId: string): boolean {
    return this._stagedTerminalRunIds.has(runId);
  }

  /** Whether the run has a usage fence. */
  isAccounted(runId: string): boolean {
    return this._terminalTokenAccountedRunIds.has(runId);
  }

  /** Whether this process admitted the run natively. */
  isNative(runId: string): boolean {
    return this._nativeTerminalRunIds.has(runId);
  }

  /** The live set of fenced run ids (not a copy). */
  get accountedRunIds(): ReadonlySet<string> {
    return this._terminalTokenAccountedRunIds;
  }

  /** A copy of the fenced run ids for a session save, or `undefined` when there are none. */
  accountedSnapshot(): string[] | undefined {
    return this._terminalTokenAccountedRunIds.size > 0 ? [...this._terminalTokenAccountedRunIds] : undefined;
  }

  /** How many admissions are cached. */
  get admissionCount(): number {
    return this._terminalStagingAdmissions.size;
  }

  /** How many runs are marked as admitted natively. */
  get nativeRunCount(): number {
    return this._nativeTerminalRunIds.size;
  }
}
