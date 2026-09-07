import { ErrorCategory, ErrorDomain, MastraError } from '../error';
import type { IMastraLogger } from '../logger';
import type { WorkflowsStorage } from '../storage/domains/workflows/base';
import type { WorkflowRunState } from './types';

export async function claimWorkflowResume({
  workflowsStore,
  snapshot,
  executionGeneration,
  workflowId,
  runId,
  logger,
  allowUnclaimedResumes,
}: {
  workflowsStore: WorkflowsStorage | undefined;
  snapshot: WorkflowRunState;
  executionGeneration: string;
  workflowId: string;
  runId: string;
  logger?: IMastraLogger;
  allowUnclaimedResumes?: boolean;
}): Promise<void> {
  if (!workflowsStore) return;

  if (!workflowsStore.supportsConcurrentUpdates()) {
    if (!allowUnclaimedResumes)
      logger?.warn(
        `[Workflow ${workflowId}] The configured workflow storage does not support concurrent updates, so concurrent resume() calls for run ${runId} cannot be de-duplicated atomically. Concurrent resumes may execute downstream steps more than once.`,
      );
    return;
  }

  const claimed = await workflowsStore.updateWorkflowState({
    workflowName: workflowId,
    runId,
    opts: {
      status: 'running',
      expectedStatus: 'suspended',
      expectedExecutionGeneration: executionGeneration,
      expectedLifecycleResumeAttempt: snapshot.lifecycleResumeAttempt ?? 0,
      lifecycleResumeAttempt: (snapshot.lifecycleResumeAttempt ?? 0) + 1,
    },
  });

  if (claimed) return;

  const current = await workflowsStore.loadWorkflowSnapshot({ workflowName: workflowId, runId });
  if (!current) throw new Error('No snapshot found for this workflow run: ' + workflowId + ' ' + runId);

  throw new MastraError({
    id: 'WORKFLOW_RESUME_ALREADY_CLAIMED',
    domain: ErrorDomain.MASTRA_WORKFLOW,
    category: ErrorCategory.USER,
    text:
      `This suspended workflow run was already resumed by another caller. Workflow "${workflowId}" run "${runId}" ` +
      `changed before this resume could claim it. ` +
      `Only one resume() call may continue a given suspension; re-read the run state before resuming again.`,
    details: {
      workflowId,
      runId,
      expectedStatus: 'suspended',
      actualStatus: current.status ?? 'unknown',
      expectedExecutionGeneration: executionGeneration,
      actualExecutionGeneration: current.executionGeneration ?? 'unknown',
      expectedLifecycleResumeAttempt: snapshot.lifecycleResumeAttempt ?? 0,
      actualLifecycleResumeAttempt: current.lifecycleResumeAttempt ?? 0,
    },
  });
}
