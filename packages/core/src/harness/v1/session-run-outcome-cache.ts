/**
 * The completed-run cache of a `Session` (PF-8768): the outcome of each run
 * that ended in this process, kept so duplicate admissions, late waiters and
 * resume settlement that converge on the same run all observe it.
 *
 * **The rules this owner enforces.**
 * - A run's outcome is remembered once. Only an outcome whose output
 *   suspended may be replaced: a resumed run ends again under the same run id,
 *   and its newer outcome replaces the suspended one and becomes the newest
 *   entry. Any other outcome stays.
 * - The cache holds at most {@link MAX_COMPLETED_RUNS} runs. Remembering one
 *   more evicts the oldest first, and each evicted run is reported to
 *   `onEvict` after it has left the cache.
 * - A completed outcome carries the pending generation whose resume usage was
 *   already accounted. Replacing the outcome forgets that mark.
 * - A failure the run's own output reported (`runFailed`) can be demoted to an
 *   ordinary failure, never promoted again.
 */

import type { FullOutput } from '../../stream/base/output';

/** Bound on the completed-run cache (oldest runs evicted first). */
export const MAX_COMPLETED_RUNS = 64;

/** A run's outcome as it is remembered. */
export type RunOutcome = { ok: true; full: FullOutput<unknown> } | { ok: false; err: unknown; runFailed?: true };

type CachedRunOutcome =
  | { ok: true; full: FullOutput<unknown>; resumeAccountingKey?: string }
  | { ok: false; err: unknown; runFailed?: true };

export class RunOutcomeCache {
  private readonly _entries = new Map<string, CachedRunOutcome>();

  constructor(private readonly onEvict: (runId: string) => void) {}

  /** The run's remembered outcome, if any. */
  get(runId: string): Readonly<RunOutcome> | undefined {
    return this._entries.get(runId);
  }

  /** Whether an outcome is remembered for the run. */
  has(runId: string): boolean {
    return this._entries.has(runId);
  }

  /**
   * Remember the run's outcome unless one is already remembered; a suspended
   * outcome is replaced and the run becomes the newest entry. Evicts the
   * oldest runs beyond the bound, reporting each to `onEvict`.
   */
  remember(runId: string, outcome: RunOutcome): void {
    const existing = this._entries.get(runId);
    if (existing !== undefined) {
      // A resumed run produces a new output for the same runId each time it
      // ends. A cached 'suspended' entry is an intermediate generation, not a
      // terminal result — replace it so retries and settlement observe the
      // newest suspension/terminal output instead of reverting to an obsolete
      // earlier suspension point.
      const existingSuspended = existing.ok && existing.full.finishReason === 'suspended';
      if (!existingSuspended) return;
      this._entries.delete(runId);
    }
    this._entries.set(runId, outcome);
    while (this._entries.size > MAX_COMPLETED_RUNS) {
      const oldest = this._entries.keys().next().value;
      if (oldest === undefined) return;
      this._entries.delete(oldest);
      this.onEvict(oldest);
    }
  }

  /** Whether the run's completed outcome already accounted the resume usage of `generationKey`. */
  isResumeAccounted(runId: string, generationKey: string): boolean {
    const entry = this._entries.get(runId);
    return entry !== undefined && entry.ok === true && entry.resumeAccountingKey === generationKey;
  }

  /** Mark the resume usage of `generationKey` accounted on the run's completed outcome, if it has one. */
  noteResumeAccounted(runId: string, generationKey: string): void {
    const entry = this._entries.get(runId);
    if (entry !== undefined && entry.ok === true) entry.resumeAccountingKey = generationKey;
  }

  /**
   * Demote the run's remembered failure to an ordinary failure (no longer a
   * failure the run's own output reported). Returns whether an outcome was
   * remembered for the run at all.
   */
  demoteRunFailure(runId: string): boolean {
    const entry = this._entries.get(runId);
    if (entry === undefined) return false;
    if (!entry.ok) delete entry.runFailed;
    return true;
  }
}
