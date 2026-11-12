/**
 * The settlement wake admissions of a `Session` (PF-8389): which wake
 * admissions are between their pending reservation and dispatch in this
 * process, and which ones this process released to dispatch recovery.
 *
 * `Session` creates one `WakeAdmissions` and changes these sets only through
 * its operations. Each operation is exactly the statements the session ran
 * inline before; nothing here awaits, calls back into the session, or adds a
 * check the session did not make:
 *
 *   - `enter` is the per-admission mutex: it refuses (returns `false`) an id
 *     already in flight and otherwise admits it. The caller throws its busy
 *     error on a refusal, as it did after its own check.
 *   - `leave` ends an admission's in-flight window.
 *   - `release` moves an admission from in flight to released, in that order.
 *   - `forgetReleased` drops a released admission once recovery settled it.
 */

export class WakeAdmissions {
  /**
   * PF-7631: admission ids of settlement wake turns between their pending
   * reservation and dispatch, so recovery never interrupts one in flight here.
   */
  private readonly _wakeTerminalAdmissionsInFlight = new Set<string>();
  /**
   * PF-7631: wake admissions this process admitted and then gave up on before
   * their terminal commit (a Stop, delete or eviction during admission or
   * dispatch, or an exhausted commit re-drive). No run of theirs is live here,
   * so dispatch recovery settles them at once rather than waiting out a
   * dispatch claim this process stamped itself.
   */
  private readonly _releasedWakeAdmissions = new Set<string>();

  /** Admit `admissionId` as in flight, unless it already is (then `false`, and nothing changes). */
  enter(admissionId: string): boolean {
    if (this._wakeTerminalAdmissionsInFlight.has(admissionId)) {
      return false;
    }
    this._wakeTerminalAdmissionsInFlight.add(admissionId);
    return true;
  }

  /** End `admissionId`'s in-flight window. */
  leave(admissionId: string): void {
    this._wakeTerminalAdmissionsInFlight.delete(admissionId);
  }

  /** Hand `admissionId` from in flight to released. */
  release(admissionId: string): void {
    this._wakeTerminalAdmissionsInFlight.delete(admissionId);
    this._releasedWakeAdmissions.add(admissionId);
  }

  /** Drop a released admission. */
  forgetReleased(admissionId: string): void {
    this._releasedWakeAdmissions.delete(admissionId);
  }

  /** Whether `admissionId` is in flight here. */
  isInFlight(admissionId: string): boolean {
    return this._wakeTerminalAdmissionsInFlight.has(admissionId);
  }

  /** Whether `admissionId` was released to dispatch recovery. */
  isReleased(admissionId: string): boolean {
    return this._releasedWakeAdmissions.has(admissionId);
  }

  /** A copy of the released admission ids, in release order. */
  releasedSnapshot(): string[] {
    return [...this._releasedWakeAdmissions];
  }

  /** How many admissions are in flight. */
  get inFlightCount(): number {
    return this._wakeTerminalAdmissionsInFlight.size;
  }
}
