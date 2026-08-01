import { isProxy } from 'node:util/types';
import type { SingleStepEntry, StepFlowEntry } from '../..';
import { RequestContext } from '../../../di';
import type { PubSub } from '../../../events';
import type { WorkflowsStorage } from '../../../storage/domains/workflows/base';
import {
  getOrCreateWorkflowStepLifecycleState,
  mergeWorkflowStepLifecycleStates,
  requireWorkflowExecutionGeneration,
} from '../../lifecycle-events';
import { getSingleStepEntryId } from '../../utils';
import { resolveCurrentState } from '../helpers';
import type { StepExecutor } from '../step-executor';
import type { ProcessorArgs } from '.';

function pathsEqual(left: readonly number[], right: readonly number[]) {
  return left.length === right.length && left.every((value, index) => value === right[index]);
}

async function reserveBranchLifecycleAttempts({
  workflowsStore,
  workflowId,
  runId,
  executionGeneration: suppliedExecutionGeneration,
  lifecycleResumeAttempt = 0,
  lifecycleStepStates,
  lifecycleIncomingStepStates,
  lifecycleAttemptBaselineCaptured = false,
  branches,
}: {
  workflowsStore?: WorkflowsStorage;
  workflowId: string;
  runId: string;
  executionGeneration?: string;
  lifecycleResumeAttempt?: number;
  lifecycleStepStates?: Record<string, { stepCallId: string; stepAttempt: number }>;
  lifecycleIncomingStepStates?: Record<string, { stepCallId: string; stepAttempt: number }>;
  lifecycleAttemptBaselineCaptured?: boolean;
  branches: Array<{ stepId: string; executionPath: number[] }>;
}): Promise<{
  executionGeneration?: string;
  lifecycleResumeAttempt: number;
  lifecycleStepStates: Record<string, { stepCallId: string; stepAttempt: number }>;
  lifecycleStepAttemptReserved: boolean;
}> {
  const states = mergeWorkflowStepLifecycleStates(lifecycleStepStates, undefined);
  if (!workflowsStore || branches.length === 0) {
    return {
      executionGeneration: suppliedExecutionGeneration,
      lifecycleResumeAttempt,
      lifecycleStepStates: states,
      lifecycleStepAttemptReserved: false,
    };
  }

  const executionGeneration = requireWorkflowExecutionGeneration(
    suppliedExecutionGeneration,
    `Evented workflow branch reservation ${workflowId}/${runId}`,
  );
  for (const branch of branches) {
    const { key, state } = getOrCreateWorkflowStepLifecycleState({
      workflowId,
      runId,
      executionGeneration,
      stepId: branch.stepId,
      executionPath: branch.executionPath,
      states,
    });
    // Mirror the serial dispatch guard (see processWorkflowStepRun). Every
    // producer carries the attempt baseline that existed before this semantic
    // branch dispatch. The first delivery advances N -> N+1. A broker
    // redelivery sees durable N+1 while still carrying N, so it reuses the same
    // lifecycle identity instead of re-firing the branch under a fresh attempt.
    const incomingStepAttempt = lifecycleAttemptBaselineCaptured
      ? (lifecycleIncomingStepStates?.[key]?.stepAttempt ?? 0)
      : undefined;
    const retainedRedeliveryAttempt = incomingStepAttempt !== undefined && state.stepAttempt > incomingStepAttempt;
    if (!retainedRedeliveryAttempt) {
      state.stepAttempt += 1;
    }
  }

  await workflowsStore.updateWorkflowState({
    workflowName: workflowId,
    runId,
    opts: {
      executionGeneration,
      lifecycleResumeAttempt,
      lifecycleStepStates: states,
    },
  });

  return {
    executionGeneration,
    lifecycleResumeAttempt,
    lifecycleStepStates: states,
    lifecycleStepAttemptReserved: true,
  };
}

function readParallelRestartPath(value: unknown, maxLength: number): number[] | undefined {
  if (value === null || typeof value !== 'object' || isProxy(value) || !Array.isArray(value)) return undefined;

  const lengthDescriptor = Object.getOwnPropertyDescriptor(value, 'length');
  if (
    !lengthDescriptor ||
    !('value' in lengthDescriptor) ||
    !Number.isSafeInteger(lengthDescriptor.value) ||
    lengthDescriptor.value > maxLength ||
    Object.getOwnPropertySymbols(value).length > 0 ||
    Object.getOwnPropertyNames(value).length !== lengthDescriptor.value + 1
  ) {
    return undefined;
  }

  const path: number[] = [];
  for (let index = 0; index < lengthDescriptor.value; index++) {
    const descriptor = Object.getOwnPropertyDescriptor(value, String(index));
    if (
      !descriptor?.enumerable ||
      !('value' in descriptor) ||
      !Number.isSafeInteger(descriptor.value) ||
      descriptor.value < 0
    ) {
      return undefined;
    }
    path.push(descriptor.value);
  }
  return path;
}

function validateParallelRestartPaths({
  activeStepsPath,
  allowContainerPath,
  restartContainerPath,
  step,
}: {
  activeStepsPath: Record<string, number[]>;
  allowContainerPath: boolean;
  restartContainerPath: number[];
  step: Extract<StepFlowEntry, { type: 'parallel' }>;
}) {
  if (
    activeStepsPath === null ||
    typeof activeStepsPath !== 'object' ||
    isProxy(activeStepsPath) ||
    Array.isArray(activeStepsPath)
  ) {
    throw new Error('Invalid parallel restart state: active step paths must be an own-property data record.');
  }
  const activeStepsPathPrototype = Object.getPrototypeOf(activeStepsPath);
  if (
    (activeStepsPathPrototype !== Object.prototype && activeStepsPathPrototype !== null) ||
    Object.getOwnPropertySymbols(activeStepsPath).length > 0
  ) {
    throw new Error('Invalid parallel restart state: active step paths must be an own-property data record.');
  }

  const branchIndices = new Map(
    step.steps.flatMap((nestedStep, index) =>
      nestedStep ? ([[getSingleStepEntryId(nestedStep), index]] as const) : [],
    ),
  );

  for (const stepId of Object.getOwnPropertyNames(activeStepsPath)) {
    const branchIndex = branchIndices.get(stepId);
    if (branchIndex === undefined) {
      throw new Error(`Invalid parallel restart state: active step "${stepId}" is not a branch of this parallel step.`);
    }

    const descriptor = Object.getOwnPropertyDescriptor(activeStepsPath, stepId);
    const path =
      descriptor?.enumerable && 'value' in descriptor
        ? readParallelRestartPath(descriptor.value, restartContainerPath.length + 1)
        : undefined;
    const fullBranchPath = restartContainerPath.concat(branchIndex);
    const storedBranchPath = [branchIndex];
    if (
      !path ||
      (!pathsEqual(path, fullBranchPath) &&
        !pathsEqual(path, storedBranchPath) &&
        !(allowContainerPath && pathsEqual(path, restartContainerPath)))
    ) {
      throw new Error(
        `Invalid parallel restart state: active step "${stepId}" must store its branch path or the pending parallel container path.`,
      );
    }
  }
}

export async function processWorkflowParallel(
  {
    workflowId,
    runId,
    executionPath,
    stepResults,
    activeStepsPath,
    resumeSteps,
    timeTravel,
    restart,
    prevResult,
    resumeData,
    parentWorkflow,
    requestContext,
    perStep,
    state,
    outputOptions,
    executionGeneration,
    lifecycleResumeAttempt,
    lifecycleStepStates,
    lifecycleIncomingStepStates,
    lifecycleAttemptBaselineCaptured,
  }: ProcessorArgs,
  {
    pubsub,
    workflowsStore,
    step,
  }: {
    pubsub: PubSub;
    workflowsStore?: WorkflowsStorage;
    step: Extract<StepFlowEntry, { type: 'parallel' }>;
  },
) {
  const pathsToRun = new Set<string>();
  // Get current state from stepResults or passed state
  const currentState = resolveCurrentState({ stepResults, state });
  const restartContainerPath = restart
    ? executionPath.length === 1
      ? executionPath
      : executionPath.slice(0, -1)
    : undefined;
  if (restart) {
    validateParallelRestartPaths({
      activeStepsPath: restart.activeStepsPath,
      allowContainerPath: executionPath.length === 1,
      restartContainerPath: restartContainerPath!,
      step,
    });
  }
  for (let i = 0; i < step.steps.length; i++) {
    const nestedStep = step.steps[i];
    if (nestedStep) {
      const nestedStepId = getSingleStepEntryId(nestedStep);
      //if restart, only run the step if it's in the active steps path
      if (restart) {
        const descriptor = Object.getOwnPropertyDescriptor(restart.activeStepsPath, nestedStepId);
        if (descriptor) {
          pathsToRun.add(nestedStepId);
        }
      } else {
        pathsToRun.add(nestedStepId);
      }
      if (perStep) {
        break;
      }
    }
  }

  const branches = step.steps.flatMap((nestedStep, idx) => {
    if (!pathsToRun.has(getSingleStepEntryId(nestedStep))) return [];
    return [
      {
        nestedStep,
        executionPath: restart ? restartContainerPath!.concat([idx]) : executionPath.concat([idx]),
      },
    ];
  });
  const lifecycleReservation = await reserveBranchLifecycleAttempts({
    workflowsStore,
    workflowId,
    runId,
    executionGeneration,
    lifecycleResumeAttempt,
    lifecycleStepStates,
    lifecycleIncomingStepStates,
    lifecycleAttemptBaselineCaptured,
    branches: branches.map(({ nestedStep, executionPath }) => ({
      stepId: getSingleStepEntryId(nestedStep),
      executionPath,
    })),
  });

  await Promise.all(
    // Keep the original branch index when only a subset is restarted. Filtering
    // first would renumber B/C to 0/1 and route persisted coordinates to the
    // wrong graph branch. This translates official fix d5c11e3ba5045969caa7272a7bd1fd141c93ab6c.
    branches.map(async ({ executionPath: branchExecutionPath }) => {
      return pubsub.publish('workflows', {
        type: 'workflow.step.run',
        runId,
        data: {
          ...lifecycleReservation,
          workflowId,
          runId,
          executionPath: branchExecutionPath,
          resumeSteps,
          stepResults,
          prevResult,
          resumeData,
          timeTravel,
          restart: restart ? { ...restart, isParallelOrConditionalRestarted: true } : undefined,
          parentWorkflow,
          activeStepsPath,
          requestContext,
          perStep,
          state: currentState,
          outputOptions,
        },
      });
    }),
  );
}

export async function processWorkflowConditional(
  {
    workflowId,
    runId,
    executionPath,
    stepResults,
    activeStepsPath,
    resumeSteps,
    timeTravel,
    restart,
    prevResult,
    resumeData,
    parentWorkflow,
    requestContext,
    perStep,
    state,
    outputOptions,
    executionGeneration,
    lifecycleResumeAttempt,
    lifecycleStepStates,
    lifecycleIncomingStepStates,
    lifecycleAttemptBaselineCaptured,
  }: ProcessorArgs,
  {
    pubsub,
    workflowsStore,
    stepExecutor,
    step,
  }: {
    pubsub: PubSub;
    workflowsStore?: WorkflowsStorage;
    stepExecutor: StepExecutor;
    step: Extract<StepFlowEntry, { type: 'conditional' }>;
  },
) {
  // Get current state from stepResults or passed state
  const currentState = resolveCurrentState({ stepResults, state });

  // Create a proper RequestContext from the plain object passed in ProcessorArgs
  const reqContext = new RequestContext(Object.entries(requestContext ?? {}) as any);

  const idxs = await stepExecutor.evaluateConditions({
    workflowId,
    step,
    runId,
    stepResults,
    state: currentState,
    requestContext: reqContext,
    input: prevResult?.status === 'success' ? prevResult.output : undefined,
    resumeData,
  });

  const truthyIdxs: Record<number, boolean> = {};
  for (let i = 0; i < idxs.length; i++) {
    truthyIdxs[idxs[i]!] = true;
  }

  let onlyStepToRun: SingleStepEntry | undefined;

  if (perStep) {
    const stepsToRun = step.steps.filter((_, idx) => truthyIdxs[idx]);
    onlyStepToRun = stepsToRun[0];
  }

  if (onlyStepToRun) {
    const onlyStepToRunId = getSingleStepEntryId(onlyStepToRun);
    const stepIndex = step.steps.findIndex(child => getSingleStepEntryId(child) === onlyStepToRunId);
    const branchExecutionPath = executionPath.concat([stepIndex]);
    const lifecycleReservation = await reserveBranchLifecycleAttempts({
      workflowsStore,
      workflowId,
      runId,
      executionGeneration,
      lifecycleResumeAttempt,
      lifecycleStepStates,
      lifecycleIncomingStepStates,
      lifecycleAttemptBaselineCaptured,
      branches: [{ stepId: onlyStepToRunId, executionPath: branchExecutionPath }],
    });
    activeStepsPath[onlyStepToRunId] = executionPath.concat([stepIndex]);
    await pubsub.publish('workflows', {
      type: 'workflow.step.run',
      runId,
      data: {
        ...lifecycleReservation,
        workflowId,
        runId,
        executionPath: branchExecutionPath,
        resumeSteps,
        stepResults,
        timeTravel,
        restart,
        prevResult,
        resumeData,
        parentWorkflow,
        activeStepsPath,
        requestContext,
        perStep,
        state: currentState,
        outputOptions,
      },
    });
  } else {
    const selectedBranches = step.steps.flatMap((branch, idx) =>
      truthyIdxs[idx] && branch
        ? [{ stepId: getSingleStepEntryId(branch), executionPath: executionPath.concat([idx]) }]
        : [],
    );
    const lifecycleReservation = await reserveBranchLifecycleAttempts({
      workflowsStore,
      workflowId,
      runId,
      executionGeneration,
      lifecycleResumeAttempt,
      lifecycleStepStates,
      lifecycleIncomingStepStates,
      lifecycleAttemptBaselineCaptured,
      branches: selectedBranches,
    });
    const lifecycleExecution = {
      executionGeneration: lifecycleReservation.executionGeneration,
      lifecycleResumeAttempt: lifecycleReservation.lifecycleResumeAttempt,
      lifecycleStepStates: lifecycleReservation.lifecycleStepStates,
    };
    await Promise.all(
      step.steps.map(async (child, idx) => {
        if (truthyIdxs[idx]) {
          if (child) {
            activeStepsPath[getSingleStepEntryId(child)] = executionPath.concat([idx]);
          }
          return pubsub.publish('workflows', {
            type: 'workflow.step.run',
            runId,
            data: {
              ...lifecycleReservation,
              workflowId,
              runId,
              executionPath: executionPath.concat([idx]),
              resumeSteps,
              stepResults,
              timeTravel,
              restart: restart ? { ...restart, isParallelOrConditionalRestarted: true } : undefined,
              prevResult,
              resumeData,
              parentWorkflow,
              activeStepsPath,
              requestContext,
              perStep,
              state: currentState,
              outputOptions,
            },
          });
        } else {
          return pubsub.publish('workflows', {
            type: 'workflow.step.end',
            runId,
            data: {
              ...lifecycleExecution,
              workflowId,
              runId,
              executionPath: executionPath.concat([idx]),
              resumeSteps,
              stepResults,
              prevResult: { status: 'skipped' },
              resumeData,
              parentWorkflow,
              activeStepsPath,
              requestContext,
              perStep,
              state: currentState,
              outputOptions,
            },
          });
        }
      }),
    );
  }
}
