import { ErrorCategory, ErrorDomain, MastraError } from '../error';

/**
 * Stable ids for workflow lifecycle preconditions that a run's stored state
 * refused. Callers match on `MastraError.id`; the message text is kept for
 * readability and is not part of the contract.
 *
 * - `WORKFLOW_RUN_NOT_ACTIVE`: restart() found a snapshot that is no longer
 *   running or waiting.
 * - `WORKFLOW_SNAPSHOT_NOT_FOUND`: resume(), restart() or timeTravel() found no
 *   stored snapshot for the run.
 * - `WORKFLOW_RUN_NOT_SUSPENDED`: resume() loaded a snapshot that is no longer
 *   suspended, typically because another caller already resumed it. A lost
 *   resume compare-and-set keeps its own `WORKFLOW_RESUME_ALREADY_CLAIMED`.
 * - `WORKFLOW_RUN_STILL_RUNNING`: timeTravel() found a run that is still
 *   running.
 *
 * The stored status is reported as `details.actualStatus`; `details.status`
 * is not used because server error handling reads it as an HTTP status.
 */
export type WorkflowLifecycleErrorId =
  | 'WORKFLOW_RUN_NOT_ACTIVE'
  | 'WORKFLOW_SNAPSHOT_NOT_FOUND'
  | 'WORKFLOW_RUN_NOT_SUSPENDED'
  | 'WORKFLOW_RUN_STILL_RUNNING';

/**
 * Stable ids for a lost run claim: another caller or process owns the run, so
 * this call stood down without executing any step. Callers match on
 * `MastraError.id`; the message text is kept for readability and is not part
 * of the contract.
 *
 * - `WORKFLOW_RESUME_ALREADY_CLAIMED`: resume() lost the compare-and-set that
 *   claims a suspended run, because another caller already claimed this
 *   suspension.
 * - `WORKFLOW_RESTART_NOT_CLAIMED`: restart() or timeTravel() lost the
 *   compare-and-set that adopts a new lifecycle generation, because another
 *   generation already owns the run.
 * - `WORKFLOW_RESTART_RUN_OWNED`: restart() found the run still being executed
 *   by another process whose ownership lease has not expired.
 */
export type WorkflowClaimErrorId =
  | 'WORKFLOW_RESUME_ALREADY_CLAIMED'
  | 'WORKFLOW_RESTART_NOT_CLAIMED'
  | 'WORKFLOW_RESTART_RUN_OWNED';

export function workflowRunNotActiveError({
  workflowId,
  runId,
  status,
}: {
  workflowId?: string;
  runId?: string;
  status: string | undefined;
}): MastraError {
  return new MastraError({
    id: 'WORKFLOW_RUN_NOT_ACTIVE' satisfies WorkflowLifecycleErrorId,
    domain: ErrorDomain.MASTRA_WORKFLOW,
    category: ErrorCategory.USER,
    text: 'This workflow run was not active',
    details: {
      ...(workflowId !== undefined ? { workflowId } : {}),
      ...(runId !== undefined ? { runId } : {}),
      actualStatus: status ?? 'unknown',
    },
  });
}

export function workflowSnapshotNotFoundError({
  workflowId,
  runId,
  text,
}: {
  workflowId: string;
  runId: string;
  text: string;
}): MastraError {
  return new MastraError({
    id: 'WORKFLOW_SNAPSHOT_NOT_FOUND' satisfies WorkflowLifecycleErrorId,
    domain: ErrorDomain.MASTRA_WORKFLOW,
    category: ErrorCategory.USER,
    text,
    details: { workflowId, runId },
  });
}

export function workflowRunNotSuspendedError({
  workflowId,
  runId,
  status,
}: {
  workflowId: string;
  runId: string;
  status: string | undefined;
}): MastraError {
  return new MastraError({
    id: 'WORKFLOW_RUN_NOT_SUSPENDED' satisfies WorkflowLifecycleErrorId,
    domain: ErrorDomain.MASTRA_WORKFLOW,
    category: ErrorCategory.USER,
    text: 'This workflow run was not suspended',
    details: { workflowId, runId, actualStatus: status ?? 'unknown' },
  });
}

export function workflowRunStillRunningError({
  workflowId,
  runId,
}: {
  workflowId: string;
  runId: string;
}): MastraError {
  return new MastraError({
    id: 'WORKFLOW_RUN_STILL_RUNNING' satisfies WorkflowLifecycleErrorId,
    domain: ErrorDomain.MASTRA_WORKFLOW,
    category: ErrorCategory.USER,
    text: 'This workflow run is still running, cannot time travel',
    details: { workflowId, runId, actualStatus: 'running' },
  });
}
