/**
 * The message-token ledger of a `Session` (PF-8451): which runs' message-token
 * usage this process has already counted, and which runs a suspended-turn
 * capture is counting right now.
 *
 * `Session` creates one `MessageTokenLedger` and changes these collections
 * only through its operations. Each operation is exactly the statements the
 * session ran inline before, in the same order; nothing here awaits, calls
 * back into the session, or adds a check the session did not make.
 *
 * **What must hold for a run's message-token usage to be counted once in this
 * process.**
 *
 *   1. Once a run's usage has been added to the live counter (or handed to the
 *      save that adds it), the run is marked accounted, and no message path
 *      adds it again. `markAccounted` establishes this at each site that
 *      counts a run, and `commit` at the end of a suspended capture. The
 *      session's readers enforce it: `_recordMessageTurnCompletion` returns
 *      early for an accounted run, and `_messageSuspendedTokenUsageDelta`
 *      yields no delta for one.
 *   2. While one suspended capture is counting a run, no other capture
 *      computes a delta for it. `reserve` establishes this; the delta read
 *      checks `isReserved`, and `_waitForMessageSuspendedTokenUsageOwner`
 *      waits on `reservationFor` until the run is accounted or no capture
 *      holds it. `commit` hands the run over to point 1; `rollback` releases
 *      it uncounted, so a later capture may count it.
 *   3. The ledger is memory only and bounded. `evict` drops a run from all
 *      three collections when it leaves the session's completed-runs cache
 *      (64 runs), and a reopened session starts empty. Across a reopen or an
 *      eviction, exactly-once rests on the durable baselines instead (the
 *      parked pending's `accountedTokenUsage`, the terminal staging ledger's
 *      usage fence, the queue receipt's `postRunFinalizedAt`).
 *
 * **What a caller can still break from outside.**
 *
 *   - Counting a run without marking it: `_recordTurnCompletion` adds usage
 *     and never consults or marks this ledger; its callers rely on the
 *     durable baselines of point 3.
 *   - Marking a run before its usage is saved, which is safe where it happens
 *     today: three sites (`_finalizeResumedTurnOutcome`,
 *     `_maybeRecoverStaleQueuedResume`, `_finalizeCompletedQueuedTurn`) mark
 *     the run before the awaited queue-marker save that carries the delta.
 *     The counter moves only if that save succeeds; if it fails, the retry is
 *     gated by the durable `postRunFinalizedAt` and recomputes the delta, so
 *     the in-memory mark only stops a duplicate message path from counting.
 *     A new site that marks early without such a durable retry would lose
 *     the count.
 *   - `_recordMessageTurnCompletion` reads `isAccounted` but not `isReserved`.
 *   - Evicting a run while a capture holds its reservation: a second capture
 *     can then reserve and count the run again, and the first capture's
 *     `commit` deletes the second's reservation mark while keeping its
 *     deferred (the double-reserve case below).
 *
 * **Kept as found, not a rule:** the run-id set and the reservations map are
 * not kept in lockstep. `commit` and `rollback` delete the run id
 * unconditionally but delete the deferred only if it is the one they were
 * given. Which semantics is right when a run is reserved twice is an open
 * question for its own issue.
 */

/** The deferred a suspended capture holds while it counts a run. */
type Reservation = { readonly promise: Promise<void> };

export class MessageTokenLedger {
  /**
   * Message admission retries can observe the run outcome cache before the original
   * message continuation has accounted usage. Track run ids accounted through
   * either path so the retry can persist before writing evidence without the
   * original continuation double-counting later.
   */
  private readonly _messageTokenAccountedRunIds = new Set<string>();
  private readonly _messageTokenAccountingRunIds = new Set<string>();
  private readonly _messageTokenAccountingReservations = new Map<string, Reservation>();

  /** Mark the run's message-token usage as counted. */
  markAccounted(runId: string): void {
    this._messageTokenAccountedRunIds.add(runId);
  }

  /** A suspended capture starts counting the run under `reservation`. */
  reserve(runId: string, reservation: Reservation): void {
    this._messageTokenAccountingRunIds.add(runId);
    this._messageTokenAccountingReservations.set(runId, reservation);
  }

  /** The capture holding `reservation` counted the run. */
  commit(runId: string, reservation: Reservation | undefined): void {
    this._messageTokenAccountingRunIds.delete(runId);
    if (this._messageTokenAccountingReservations.get(runId) === reservation) {
      this._messageTokenAccountingReservations.delete(runId);
    }
    this._messageTokenAccountedRunIds.add(runId);
  }

  /** The capture holding `reservation` failed; the run stays uncounted. */
  rollback(runId: string, reservation: Reservation | undefined): void {
    this._messageTokenAccountingRunIds.delete(runId);
    if (this._messageTokenAccountingReservations.get(runId) === reservation) {
      this._messageTokenAccountingReservations.delete(runId);
    }
  }

  /** The run left the session's completed-runs cache. */
  evict(runId: string): void {
    this._messageTokenAccountedRunIds.delete(runId);
    this._messageTokenAccountingRunIds.delete(runId);
    this._messageTokenAccountingReservations.delete(runId);
  }

  isAccounted(runId: string): boolean {
    return this._messageTokenAccountedRunIds.has(runId);
  }

  isReserved(runId: string): boolean {
    return this._messageTokenAccountingRunIds.has(runId);
  }

  reservationFor(runId: string): Reservation | undefined {
    return this._messageTokenAccountingReservations.get(runId);
  }
}
