import type { WorkflowsStorage } from '../storage/domains/workflows/base';
import type { WorkflowCancelRequestV1, WorkflowRunState, WorkflowRunStatus } from './types';

/** Statuses in which an engine may still be executing the lineage. */
export const WORKFLOW_CANCEL_REQUEST_EXECUTING_STATUSES: readonly WorkflowRunStatus[] = ['running', 'waiting'];

const TERMINAL_STATUSES = new Set<WorkflowRunStatus>([
  'success',
  'failed',
  'canceled',
  'tripwire',
  'bailed',
  'skipped',
]);

/** Input for `Run.requestCancel()`. */
export interface WorkflowCancelRequestInput {
  /** Caller idempotency key, for example a product abort operation id. */
  requestId: string;
  /**
   * Lineage the request targets. Defaults to the lineage this Run handle is
   * currently executing; a handle with no lineage of its own must name one.
   */
  expectedExecutionGeneration?: string;
  expectedLifecycleResumeAttempt?: number;
}

/**
 * Typed outcome of `Run.requestCancel()`.
 *
 * A request binds to one execution lineage. Once that lineage is gone — a
 * restart minted a new generation, a resume moved to the next attempt, or a
 * checkpointed resume rolled back to its suspension — the request no longer
 * applies, and a caller that still wants the run stopped requests again for
 * the lineage it now observes.
 */
export type WorkflowCancelRequestOutcome =
  /**
   * The running or waiting lineage carries the request; its engine honours it
   * at the next step boundary, or `restart()` does after the engine is gone.
   * The lineage may still settle another way when the request lands after its
   * last boundary, so callers confirm the outcome from the durable run.
   */
  | { status: 'requested'; cancelRequest: WorkflowCancelRequestV1 }
  /**
   * That lineage already carries a request, which is kept. Two callers racing
   * to record the first request can both see `requested`; the later write's
   * `requestId` is the one stored, and either way the same lineage stops.
   */
  | { status: 'already_requested'; cancelRequest: WorkflowCancelRequestV1 }
  /**
   * A pending, suspended or paused lineage no engine was executing: it is
   * canceled now. A pending run this handle is already starting is treated
   * as running instead.
   */
  | { status: 'canceled' }
  /** The run already ended. */
  | { status: 'terminal'; runStatus: WorkflowRunStatus }
  /** Another generation or resume attempt owns the run; nothing was written. */
  | { status: 'lineage_moved'; executionGeneration?: string; lifecycleResumeAttempt?: number }
  | { status: 'not_found' };

/**
 * Abort reason used when a durable cancel request stops an execution. The
 * engine commits `canceled` itself at the boundary that observes the request;
 * a request that arrives after the lineage persisted another outcome does not
 * relabel that outcome in memory.
 */
export class WorkflowCancelRequestedError extends Error {
  readonly cancelRequest: WorkflowCancelRequestV1;
  /**
   * Set when `Run.cancel()` later committed `canceled` durably for the same
   * execution. An abort signal keeps its first reason, so this records that
   * the stronger, already-durable cancellation now owns the outcome.
   */
  durableCancelCommitted = false;

  constructor(cancelRequest: WorkflowCancelRequestV1) {
    super(`Workflow cancellation requested (${cancelRequest.requestId})`);
    this.name = 'WorkflowCancelRequestedError';
    this.cancelRequest = cancelRequest;
  }
}

/**
 * True when a durable cancel request, and nothing that committed `canceled`
 * durably since, aborted the signal.
 */
export function isWorkflowCancelRequestAbort(signal: AbortSignal | undefined): boolean {
  return (
    signal?.aborted === true &&
    signal.reason instanceof WorkflowCancelRequestedError &&
    !signal.reason.durableCancelCommitted
  );
}

/** Records that `Run.cancel()` committed `canceled` durably on a signal a request already aborted. */
export function markDurableCancelCommitted(signal: AbortSignal): void {
  if (signal.reason instanceof WorkflowCancelRequestedError) signal.reason.durableCancelCommitted = true;
}

export function isTerminalWorkflowRunStatus(status: WorkflowRunStatus | undefined): boolean {
  return status !== undefined && TERMINAL_STATUSES.has(status);
}

/** Reads a persisted cancel request, rejecting any malformed value. */
export function materializeWorkflowCancelRequest(value: unknown): WorkflowCancelRequestV1 | undefined {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return undefined;
  const request = value as Partial<WorkflowCancelRequestV1>;
  if (
    request.version !== 1 ||
    typeof request.requestId !== 'string' ||
    request.requestId.length === 0 ||
    typeof request.executionGeneration !== 'string' ||
    request.executionGeneration.length === 0 ||
    typeof request.lifecycleResumeAttempt !== 'number' ||
    !Number.isSafeInteger(request.lifecycleResumeAttempt) ||
    request.lifecycleResumeAttempt < 0 ||
    typeof request.requestedAt !== 'number' ||
    !Number.isFinite(request.requestedAt)
  ) {
    return undefined;
  }
  return {
    version: 1,
    requestId: request.requestId,
    executionGeneration: request.executionGeneration,
    lifecycleResumeAttempt: request.lifecycleResumeAttempt,
    requestedAt: request.requestedAt,
  };
}

/** Returns the persisted request when it targets exactly this execution lineage. */
export function workflowCancelRequestFor(
  value: unknown,
  lineage: { executionGeneration?: string; lifecycleResumeAttempt?: number },
): WorkflowCancelRequestV1 | undefined {
  const request = materializeWorkflowCancelRequest(value);
  return request !== undefined &&
    lineage.executionGeneration !== undefined &&
    request.executionGeneration === lineage.executionGeneration &&
    request.lifecycleResumeAttempt === (lineage.lifecycleResumeAttempt ?? 0)
    ? request
    : undefined;
}

/**
 * Commits `canceled` for exactly one lineage with a compare-and-set on status,
 * generation and resume attempt. Returns undefined when the lineage moved or
 * the run left `expectedStatus`, so a successor is never canceled.
 */
export async function commitWorkflowLineageCancellation({
  workflowsStore,
  workflowName,
  runId,
  expectedStatus,
  executionGeneration,
  lifecycleResumeAttempt,
}: {
  workflowsStore: WorkflowsStorage;
  workflowName: string;
  runId: string;
  expectedStatus: WorkflowRunStatus | readonly WorkflowRunStatus[];
  executionGeneration: string;
  lifecycleResumeAttempt: number;
}): Promise<WorkflowRunState | undefined> {
  return workflowsStore.updateWorkflowState({
    workflowName,
    runId,
    opts: {
      status: 'canceled',
      expectedStatus: typeof expectedStatus === 'string' ? expectedStatus : [...expectedStatus],
      expectedExecutionGeneration: executionGeneration,
      expectedLifecycleResumeAttempt: lifecycleResumeAttempt,
    },
  });
}
