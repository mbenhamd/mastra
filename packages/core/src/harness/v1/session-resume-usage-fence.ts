/**
 * The durable resume-usage fence of a `Session` (PF-8719). A resumed run's
 * usage delta reaches the live counter before the pending-clearing flush, and
 * every flush persists the live counter. While the fence is armed, each write
 * folds the post-resume baseline into the still-parked pending generation, so
 * the durable `accountedTokenUsage` advances atomically with whichever write
 * commits first and a cold settlement retry computes a zero delta.
 *
 * **The rules this owner enforces.**
 * - At most one baseline is armed, for one pending generation; `arm` replaces
 *   whatever was armed before.
 * - The baseline applies only to the generation it was armed for:
 *   `baselineFor` returns it only when the parked generation is that one.
 * - After a committed write, `disarmUnlessParked` disarms it once its
 *   generation is no longer the one parked (none parked, or another).
 *
 * Generation keys are passed lazily: the owner computes one only while it is
 * armed, as the session did inline.
 */

import type { TokenUsage } from './session';

export class ResumeUsageFence {
  private _armed: { generationKey: string; runUsage: TokenUsage } | undefined;

  /** Arm the fence for `generationKey` with the run's post-resume usage, replacing any armed baseline. */
  arm(generationKey: string, runUsage: TokenUsage): void {
    this._armed = { generationKey, runUsage };
  }

  /** Whether the fence is armed for exactly `generationKey`. */
  isArmedFor(generationKey: string): boolean {
    return this._armed?.generationKey === generationKey;
  }

  /** The armed baseline when the parked generation (`parkedGenerationKey()`) is the armed one. */
  baselineFor(parkedGenerationKey: () => string): TokenUsage | undefined {
    const armed = this._armed;
    if (armed === undefined) return undefined;
    return parkedGenerationKey() === armed.generationKey ? armed.runUsage : undefined;
  }

  /** Disarm unless the generation still parked (`undefined` when none is) is the armed one. */
  disarmUnlessParked(parkedGenerationKey: (() => string) | undefined): void {
    const armed = this._armed;
    if (armed === undefined) return;
    if (parkedGenerationKey === undefined || parkedGenerationKey() !== armed.generationKey) {
      this._armed = undefined;
    }
  }
}
