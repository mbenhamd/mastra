/**
 * Terminal finalizer handoff: the lookup key of a finalizer registration and
 * the outcome a finalizer commits for a native run whose own output rejected.
 */

/** Lookup key for a terminal finalizer registration by its id and version. */
export function terminalFinalizerKey(id: string, version: string): string {
  return JSON.stringify([id, version]);
}

/**
 * A dispatched native terminal run whose own output rejected (a provider or
 * agent error). Settled through the terminal commit as a durable `failed`
 * result; the finalizer receives no output.
 */
export class RejectedTerminalRun {
  constructor(readonly error: unknown) {}
}
