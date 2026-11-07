import { ErrorCategory, ErrorDomain, MastraError } from '../error';
import type { IMastraLogger } from '../logger';
import type { WorkflowsStorage } from '../storage/domains/workflows/base';
import { deepEqual } from '../utils/deep-equal';
import type { ExecutionEngineOptions } from './execution-engine';
import { cloneLifecyclePayload } from './handlers/entry';
import { workflowSnapshotNotFoundError } from './lifecycle-errors';
import type { WorkflowResumeIntentV1, WorkflowRunState } from './types';
import { omitPriorCompletionFields } from './utils';

/** The parts of an execution engine's options that decide what a step write persists. */
export interface ResumeAnswerPersistence {
  options: Pick<ExecutionEngineOptions, 'pruneSnapshot' | 'shouldPersistSnapshot'> | undefined;
  /** A run-scoped override of `options.shouldPersistSnapshot`, which wins as it does for step writes. */
  persistenceOverride?: ExecutionEngineOptions['shouldPersistSnapshot'];
}

/**
 * Whether a resume claim may store the accepted resume data. A claim stores either exactly the
 * caller's data or nothing: the data is kept only where the resumed step's own `running` write
 * would persist it unchanged. The probe is that write's snapshot: the run `running` and the
 * resumed step recorded as `handlers/step.ts` records it. When `shouldPersistSnapshot` would not
 * persist it, or `pruneSnapshot` removes or changes the data, the claim stores no resume input
 * and a restart behaves as before.
 *
 * A throwing hook propagates, so the caller fails before its claim.
 */
export function resumeAnswerIsPersistable({
  snapshot,
  stepId,
  resumePayload,
  persistence,
}: {
  snapshot: WorkflowRunState;
  stepId: string;
  resumePayload: unknown;
  persistence: ResumeAnswerPersistence;
}): boolean {
  const options = persistence.options;
  const persistencePredicate = persistence.persistenceOverride ?? options?.shouldPersistSnapshot;
  const prunes = !!options?.pruneSnapshot && resumePayload !== undefined;
  if (!persistencePredicate && !prunes) return true;

  const prior = (snapshot.context?.[stepId] ?? {}) as Record<string, unknown>;
  const resumePath = snapshot.suspendedPaths?.[stepId];
  const probe: WorkflowRunState = {
    ...snapshot,
    status: 'running',
    context: {
      ...snapshot.context,
      [stepId]: {
        ...omitPriorCompletionFields(prior),
        resumePayload,
        ...(prior.suspendPayload !== undefined ? { suspendPayload: prior.suspendPayload } : {}),
        resumedAt: Date.now(),
        status: 'running',
      },
    } as WorkflowRunState['context'],
    activeStepsPath: {
      ...snapshot.activeStepsPath,
      ...(resumePath === undefined ? {} : { [stepId]: resumePath }),
    },
  };

  if (
    persistencePredicate &&
    !persistencePredicate({
      stepResults: probe.context as Parameters<ExecutionEngineOptions['shouldPersistSnapshot']>[0]['stepResults'],
      workflowStatus: 'running',
    })
  ) {
    return false;
  }
  if (!prunes) return true;
  // The hook gets its own copy, as at the step-write persist site, so it cannot change the loaded
  // snapshot or the caller's data.
  const pruned = options!.pruneSnapshot!({
    snapshot: cloneLifecyclePayload(probe) as WorkflowRunState,
    workflowStatus: 'running',
  });
  const persisted = (pruned?.context?.[stepId] as { resumePayload?: unknown } | undefined)?.resumePayload;
  return deepEqual(persisted, resumePayload);
}

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
