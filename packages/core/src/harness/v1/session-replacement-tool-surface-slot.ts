/**
 * The retained replacement tool surface of a `Session` (PF-8547): the
 * admission-time definition snapshot of a turn whose mode replaces the tool
 * surface, kept so a same-process resume of that run can run against the
 * tools it was admitted with.
 *
 * **The rule this owner enforces.** A session retains at most one replacement
 * surface, for one run. `retain(runId, fence)` replaces whatever was retained
 * before, for that run or any other. A retained surface leaves only through
 * `drop` for its own run or through `dispose`; `drop` for any other run leaves
 * it in place. The session cannot hold two, and cannot remove one it does not
 * name.
 *
 * Before this owner the session kept a map and enforced "at most one" by
 * clearing it immediately before its only `set`; every other write deleted by
 * run id or cleared the map. A map that never holds more than one entry
 * behaves as this slot: a lookup or delete by run id hits the entry only when
 * the run ids are equal.
 */

import type { ToolSurfaceFence } from '../../agent/tool-surface-fence';

export class ReplacementToolSurfaceSlot {
  private _retained?: { runId: string; fence: ToolSurfaceFence };

  /** Retain `fence` for `runId`, replacing any surface retained before. */
  retain(runId: string, fence: ToolSurfaceFence): void {
    this._retained = { runId, fence };
  }

  /** The surface retained for `runId`, if the slot holds that run. */
  fenceFor(runId: string): ToolSurfaceFence | undefined {
    return this._retained?.runId === runId ? this._retained.fence : undefined;
  }

  /** Release the surface retained for `runId`; a surface retained for another run stays. */
  drop(runId: string): void {
    if (this._retained?.runId === runId) this._retained = undefined;
  }

  /** Release whatever is retained (session close, delete or eviction). */
  dispose(): void {
    this._retained = undefined;
  }
}
