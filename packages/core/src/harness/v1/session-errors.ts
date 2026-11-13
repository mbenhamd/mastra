/**
 * Errors and error texts `Session` settles turns with, and the conversion of a
 * stored public error projection back into an error.
 */

import { HarnessError, HarnessPendingInteractionDiscardedError } from './errors';

/**
 * Settlement message recorded (with `HARNESS_RUN_INTERRUPTED_ERROR_CODE`) when
 * recovery finds an admitted `message()` dispatch whose owner died mid-run.
 */
export const HARNESS_RUN_INTERRUPTED_MESSAGE =
  'The run was interrupted when the process that owned it stopped; it was not re-run.';

const DISCARDED_ERROR_CODE = 'harness.pending_interaction_discarded';

export function publicErrorProjectionToError(error: { code: string; message: string }): Error {
  // Every stored-error replay (receipts, signal/queue/message evidence)
  // comes through here: a discard outcome replays as its typed error, even
  // after the discard receipt itself has been pruned.
  if (error.code === DISCARDED_ERROR_CODE) return HarnessPendingInteractionDiscardedError.fromProjection(error);
  const projected = new Error(error.message);
  projected.name = error.code;
  (projected as Error & { code: string }).code = error.code;
  return projected;
}

export class ResumeRecoveryStaleError extends HarnessError {
  readonly code = 'harness.resume_recovery_stale';

  constructor() {
    super('resume was admitted but no live owner or durable terminal result is available');
    this.name = 'harness.resume_recovery_stale';
  }
}

export class QueuePostRunFinalizationPendingError extends HarnessError {
  readonly code = 'harness.queue_post_run_finalization_pending';
  readonly retryAt: number;
  readonly cause: unknown;

  constructor(retryAt: number, cause: unknown) {
    super('queued turn completed and is waiting for post-run finalization to persist');
    this.name = 'harness.queue_post_run_finalization_pending';
    this.retryAt = retryAt;
    this.cause = cause;
  }
}
