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
 */
export type WorkflowLifecycleErrorId =
  'WORKFLOW_RUN_NOT_ACTIVE' | 'WORKFLOW_SNAPSHOT_NOT_FOUND' | 'WORKFLOW_RUN_NOT_SUSPENDED';

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
      status: status ?? 'unknown',
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
    details: { workflowId, runId, status: status ?? 'unknown' },
  });
}
