/**
 * Outcome classification for a failed storage operation.
 *
 * A store that can tell what a failed operation left behind records one of
 * these values under `details.persistenceFailure` on the `MastraError` it
 * throws:
 *
 * - `transient`: the operation did not apply (a read, or a write the store
 *   proves was not committed) and failed for a reason a later attempt can get
 *   past, such as a dropped connection, a lock or serialization conflict, a
 *   timeout or exhausted server resources.
 * - `permanent`: the operation did not apply and repeating the same request
 *   fails the same way, such as a constraint violation, invalid data or a
 *   storage contract error.
 * - `commit_unknown`: the write may have committed even though the caller saw
 *   a failure, for example when the connection dropped while a COMMIT was in
 *   flight.
 *
 * The classification describes the single failed storage operation, not the
 * workflow operation around it: earlier writes of the same resume or step may
 * already be durable. Callers decide from a durable read of the run, never
 * from the classification alone, and must not repeat a `commit_unknown` write
 * without first reconciling it against that read. Stores never retry a
 * `commit_unknown` write themselves.
 *
 * An error without a classification (`undefined`) comes from a store that
 * does not report one, or from code outside a store; treat it as
 * unclassified rather than as any of the three outcomes.
 */
export type StoragePersistenceFailure = 'transient' | 'permanent' | 'commit_unknown';

/** The `MastraError.details` key that carries a {@link StoragePersistenceFailure}. */
export const STORAGE_PERSISTENCE_FAILURE_DETAIL = 'persistenceFailure';

const STORAGE_PERSISTENCE_FAILURES: ReadonlySet<string> = new Set<StoragePersistenceFailure>([
  'transient',
  'permanent',
  'commit_unknown',
]);

const MAX_PERSISTENCE_FAILURE_CAUSE_DEPTH = 8;

/** True when `value` is one of the three storage persistence failure outcomes. */
export function isStoragePersistenceFailure(value: unknown): value is StoragePersistenceFailure {
  return typeof value === 'string' && STORAGE_PERSISTENCE_FAILURES.has(value);
}

/**
 * Returns the storage persistence failure classification carried by `error`,
 * or by the nearest error in its `cause` chain that carries one. Workflow and
 * framework errors that wrap a storage failure keep it as their cause, so the
 * classification survives the wrap.
 */
export function getStoragePersistenceFailure(error: unknown): StoragePersistenceFailure | undefined {
  let candidate = error;
  for (let depth = 0; depth < MAX_PERSISTENCE_FAILURE_CAUSE_DEPTH; depth += 1) {
    if (typeof candidate !== 'object' || candidate === null) return undefined;
    const details = (candidate as { details?: unknown }).details;
    if (typeof details === 'object' && details !== null) {
      const value = (details as Record<string, unknown>)[STORAGE_PERSISTENCE_FAILURE_DETAIL];
      if (isStoragePersistenceFailure(value)) return value;
    }
    candidate = (candidate as { cause?: unknown }).cause;
  }
  return undefined;
}
