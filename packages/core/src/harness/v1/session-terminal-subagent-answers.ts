/**
 * Framework-staged direct subagent answers of a `Session` (PF-8548), held per
 * run until the matching authoritative step-finish supplies the message id.
 *
 * **The rule this owner enforces.** A run has at most one candidate answer.
 * `offer` for a run with no candidate stores it; `offer` with the same part id
 * as the candidate replaces the candidate's answer; `offer` with a different
 * part id removes the candidate and stores nothing, because more than one
 * candidate for a run is ambiguous and the Harness must not guess which to
 * render. A conflict does not close the run to candidates: the next `offer`
 * finds no candidate and stores its own. `take` hands the candidate out once.
 */

import type { HarnessSubagentDirectAnswer } from './terminal-subagent-result';

export interface TerminalSubagentAnswerCandidate {
  partId: string;
  answer: HarnessSubagentDirectAnswer;
}

export class TerminalSubagentAnswers {
  private readonly _candidates = new Map<string, TerminalSubagentAnswerCandidate>();

  /** Offer a candidate for `runId`; a candidate with another part id makes the run's candidate ambiguous and removes it. */
  offer(runId: string, partId: string, answer: HarnessSubagentDirectAnswer): void {
    const candidate = this._candidates.get(runId);
    if (candidate && candidate.partId !== partId) {
      this._candidates.delete(runId);
      return;
    }
    this._candidates.set(runId, { partId, answer });
  }

  /** Remove and return the candidate for `runId`, if any. */
  take(runId: string): TerminalSubagentAnswerCandidate | undefined {
    const candidate = this._candidates.get(runId);
    if (candidate) this._candidates.delete(runId);
    return candidate;
  }

  /** Discard the candidate for `runId`, if any. */
  drop(runId: string): void {
    this._candidates.delete(runId);
  }
}
