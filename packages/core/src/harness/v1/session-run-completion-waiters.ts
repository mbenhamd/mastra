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
 */

import type { FullOutput } from '../../stream/base/output';

type RunCompletionWaiter = {
  promise: Promise<FullOutput<unknown>>;
  resolve: (full: FullOutput<unknown>) => void;
  reject: (err: unknown) => void;
};

export class RunCompletionWaiters {
  /**
   * Per-run completion promises, keyed by `runId`. `_watchRunCompletion()`
   * resolves or rejects the matching entry after the runtime output finishes.
   * Entries left over on `close()` are rejected so callers don't hang.
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
