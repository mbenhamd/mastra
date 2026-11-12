/**
 * The run completion waiters of a `Session` (PF-8485): per run, the one
 * promise every caller of `_awaitRunCompletion` for that run shares, with
 * what settles it.
 *
 * **The rule this owner enforces.** A waiter leaves the map only in the same
 * synchronous step in which it is settled (`fail`, `resolveIfCurrent`,
 * `rejectIfCurrent`, `rejectAllExcept`), or through `dropIfCurrent`, which
 * removes only the waiter whose promise its caller names. No caller holds the
 * settle functions, the map, or an iterator over it, so no caller can remove
 * a waiter and leave it pending, settle one that stays registered, or delete
 * from the map while iterating it.
 *
 * Settling here is a plain `resolve`/`reject` call: it runs no continuation
 * synchronously (continuations run as later microtasks), so a statement after
 * an operation in the same synchronous block sees the same state whether the
 * settle happened just before it or just after.
 *
 * **What a caller can still break.** `dropIfCurrent` removes without settling:
 * the session uses it only in `_handleRunTerminal`'s `finally`, after that
 * path has settled the waiter, so it removes something only if that path
 * threw before settling. A caller holding a promise from `promiseFor` can
 * name it to `dropIfCurrent`, as it could before.
 *
 * **What changed from the base.** Only the `getFullOutput` failure path of
 * `_handleRunTerminal` behaves differently. It reads the run's waiter (W1)
 * before `await out.getFullOutput()`, and on failure it used to delete
 * whatever waiter the map held after that `await` but reject only W1. It now
 * calls `fail`, so the waiter it removes is the one it rejects. The
 * difference needs a second waiter (W2) registered during that `await`. That
 * happens only if W1 was settled and removed and the run's recorded failure
 * was then evicted from the session's 64-run completed cache, so a new
 * `_awaitRunCompletion` registered W2 and started a second watcher. Then:
 *
 *   1. If W2's watcher reaches `_handleRunTerminal` after this failure path
 *      ran: before, W2 was removed unsettled and its caller waited forever.
 *      Now W2 is rejected with this path's error.
 *   2. If W2's watcher captured W2 before this failure path ran: before, W2
 *      was removed unsettled here and then settled by its own watcher through
 *      its captured handle, with that watcher's outcome. Now W2 is rejected
 *      here with this path's error, and the second watcher's
 *      `resolveIfCurrent`/`rejectIfCurrent` does nothing. The outcomes differ
 *      only if the two watchers read different outputs (a new run under the
 *      same id). On one output, `getFullOutput` drains the stream once and
 *      rejects every caller with the same captured error.
 *
 * In both cases W2 now carries the failure the session records for the run
 * in the same step, which is what a caller arriving afterwards gets from the
 * completed cache.
 */

import type { FullOutput } from '../../stream/base/output';

type RunCompletionWaiter = {
  promise: Promise<FullOutput<unknown>>;
  resolve: (full: FullOutput<unknown>) => void;
  reject: (err: unknown) => void;
};

export class RunCompletionWaiters {
  /**
   * At most one waiter per run, keyed by `runId`: the shared promise with its
   * settle functions. An entry is removed only by the operation that settles
   * it, or by `dropIfCurrent`.
   */
  private readonly _waiters = new Map<string, RunCompletionWaiter>();

  /** Whether a waiter for the run is registered. */
  has(runId: string): boolean {
    return this._waiters.has(runId);
  }

  /** The promise of the run's registered waiter, if any. */
  promiseFor(runId: string): Promise<FullOutput<unknown>> | undefined {
    return this._waiters.get(runId)?.promise;
  }

  /**
   * The run's shared promise: the registered one, or a new one registered
   * now. `created` says whether this call registered it.
   */
  register(runId: string): { promise: Promise<FullOutput<unknown>>; created: boolean } {
    const existing = this._waiters.get(runId);
    if (existing) return { promise: existing.promise, created: false };
    let resolve!: (full: FullOutput<unknown>) => void;
    let reject!: (err: unknown) => void;
    const promise = new Promise<FullOutput<unknown>>((res, rej) => {
      resolve = res;
      reject = rej;
    });
    this._waiters.set(runId, { promise, resolve, reject });
    return { promise, created: true };
  }

  /** Remove the run's waiter, if any, and reject it with `err`. */
  fail(runId: string, err: unknown): void {
    const waiter = this._waiters.get(runId);
    this._waiters.delete(runId);
    waiter?.reject(err);
  }

  /** If the run's waiter is still the one whose promise is `promise`, remove it and resolve it with `full`. */
  resolveIfCurrent(runId: string, promise: Promise<FullOutput<unknown>> | undefined, full: FullOutput<unknown>): void {
    const waiter = this._current(runId, promise);
    if (waiter === undefined) return;
    this._waiters.delete(runId);
    waiter.resolve(full);
  }

  /** If the run's waiter is still the one whose promise is `promise`, remove it and reject it with `err`. */
  rejectIfCurrent(runId: string, promise: Promise<FullOutput<unknown>> | undefined, err: unknown): void {
    const waiter = this._current(runId, promise);
    if (waiter === undefined) return;
    this._waiters.delete(runId);
    waiter.reject(err);
  }

  /** Remove the run's waiter without settling it, only if it is still the one whose promise is `promise`. */
  dropIfCurrent(runId: string, promise: Promise<FullOutput<unknown>> | undefined): void {
    if (this._waiters.get(runId)?.promise === promise) this._waiters.delete(runId);
  }

  /**
   * Reject every waiter whose run `keep` does not hold back, with `reason`,
   * in registration order, removing each before rejecting it.
   */
  rejectAllExcept(keep: (runId: string) => boolean, reason: unknown): void {
    for (const [runId, waiter] of this._waiters) {
      if (keep(runId)) continue;
      this._waiters.delete(runId);
      waiter.reject(reason);
    }
  }

  private _current(runId: string, promise: Promise<FullOutput<unknown>> | undefined): RunCompletionWaiter | undefined {
    if (promise === undefined) return undefined;
    const waiter = this._waiters.get(runId);
    return waiter?.promise === promise ? waiter : undefined;
  }
}
