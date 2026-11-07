import { ErrorCategory, ErrorDomain, MastraError } from '../error';
import type { IMastraLogger } from '../logger';
import type { WorkflowsStorage } from '../storage/domains/workflows/base';
import { workflowSnapshotNotFoundError } from './lifecycle-errors';
import type { WorkflowResumeIntentV1, WorkflowRunState } from './types';

/**
 * Builds the resume input record a `resume()` claim stores with its
 * compare-and-set. The attempt is the one the claim installs.
 */
export function createWorkflowResumeIntent({
  snapshot,
  executionGeneration,
  steps,
  resumePayload,
  label,
  forEachIndex,
}: {
  snapshot: WorkflowRunState;
  executionGeneration: string;
  steps: string[];
  resumePayload: unknown;
  label?: string;
  forEachIndex?: number;
}): WorkflowResumeIntentV1 {
  const resumePath = Object.prototype.hasOwnProperty.call(snapshot.suspendedPaths ?? {}, steps[0] ?? '')
    ? snapshot.suspendedPaths[steps[0]!]
    : undefined;
  return {
    version: 1,
    executionGeneration,
    lifecycleResumeAttempt: (snapshot.lifecycleResumeAttempt ?? 0) + 1,
    steps: [...steps],
    ...(resumePath === undefined ? {} : { resumePath: [...resumePath] }),
    ...(resumePayload === undefined ? {} : { resumePayload }),
    ...(label === undefined ? {} : { label }),
    ...(forEachIndex === undefined ? {} : { forEachIndex }),
  };
}

/**
 * Returns the stored resume input `restart()` must continue, or `undefined`
 * when a restart re-executes the run from its snapshot as before.
 *
 * Only an accepted resume that has not settled qualifies: the run is still
 * `running` under the generation and attempt the claim installed, and the
 * claimed step still holds its suspended record. A step write that settles
 * the resume, a later resume claim and a restart claim (new generation) all
 * break the match, so the input is applied at most once per claim.
 */
export function getRestartableWorkflowResumeIntent(snapshot: WorkflowRunState): WorkflowResumeIntentV1 | undefined {
  const intent = snapshot.resumeIntent;
  if (!intent || typeof intent !== 'object' || intent.version !== 1) return undefined;
  if (snapshot.status !== 'running') return undefined;
  if (
    typeof snapshot.executionGeneration !== 'string' ||
    intent.executionGeneration !== snapshot.executionGeneration ||
    intent.lifecycleResumeAttempt !== (snapshot.lifecycleResumeAttempt ?? 0)
  ) {
    return undefined;
  }
  if (
    !Array.isArray(intent.steps) ||
    intent.steps.length === 0 ||
    intent.steps.some(step => typeof step !== 'string' || step.length === 0)
  ) {
    return undefined;
  }
  if (
    intent.resumePath !== undefined &&
    (!Array.isArray(intent.resumePath) || intent.resumePath.some(index => !Number.isSafeInteger(index) || index < 0))
  ) {
    return undefined;
  }
  const stepId = intent.steps[0]!;
  if (!Object.prototype.hasOwnProperty.call(snapshot.context ?? {}, stepId)) return undefined;
  const stepResult = snapshot.context[stepId] as { status?: unknown } | undefined;
  if (stepResult?.status !== 'suspended') return undefined;
  return intent;
}

export async function claimWorkflowResume({
  workflowsStore,
  snapshot,
  executionGeneration,
  workflowId,
  runId,
  logger,
  allowUnclaimedResumes,
  resumeIntent,
}: {
  workflowsStore: WorkflowsStorage | undefined;
  snapshot: WorkflowRunState;
  executionGeneration: string;
  workflowId: string;
  runId: string;
  logger?: IMastraLogger;
  allowUnclaimedResumes?: boolean;
  /**
   * Resume input stored in the same compare-and-set, so a restart after a
   * crash can continue this resume. Stores without concurrent updates write
   * no claim and therefore no intent.
   */
  resumeIntent?: WorkflowResumeIntentV1;
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
      ...(resumeIntent === undefined ? {} : { resumeIntent }),
    },
  });

  if (claimed) return;

  const current = await workflowsStore.loadWorkflowSnapshot({ workflowName: workflowId, runId });
  if (!current) {
    throw workflowSnapshotNotFoundError({
      workflowId,
      runId,
      text: 'No snapshot found for this workflow run: ' + workflowId + ' ' + runId,
    });
  }

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
