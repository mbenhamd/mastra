import type { StepFlowEntry } from '../..';
import { RequestContext } from '../../../di';
import type { PubSub } from '../../../events';
import { resolveCurrentState } from '../helpers';
import type { StepExecutor } from '../step-executor';
import type { ProcessorArgs } from '.';

function pathsEqual(left: readonly number[], right: readonly number[]) {
  return left.length === right.length && left.every((value, index) => value === right[index]);
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
  const branchIndices = new Map(
    step.steps.flatMap((nestedStep, index) =>
      nestedStep.type === 'step' ? ([[nestedStep.step.id, index]] as const) : [],
    ),
  );

  for (const stepId of Object.getOwnPropertyNames(activeStepsPath)) {
    const branchIndex = branchIndices.get(stepId);
    if (branchIndex === undefined) {
      throw new Error(`Invalid parallel restart state: active step "${stepId}" is not a branch of this parallel step.`);
    }

    const descriptor = Object.getOwnPropertyDescriptor(activeStepsPath, stepId)!;
    const path = 'value' in descriptor ? descriptor.value : undefined;
    const isDenseIntegerPath =
      Array.isArray(path) &&
      Object.keys(path).length === path.length &&
      path.every(
        (value, index) => Object.prototype.hasOwnProperty.call(path, index) && Number.isInteger(value) && value >= 0,
      );
    const fullBranchPath = restartContainerPath.concat(branchIndex);
    const storedBranchPath = [branchIndex];
    if (
      !isDenseIntegerPath ||
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
  }: ProcessorArgs,
  {
    pubsub,
    step,
  }: {
    pubsub: PubSub;
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
    if (nestedStep?.type === 'step') {
      //if restart, only run the step if it's in the active steps path
      if (restart) {
        const descriptor = Object.getOwnPropertyDescriptor(restart.activeStepsPath, nestedStep.step.id);
        if (descriptor) {
          pathsToRun.add(nestedStep.step.id);
        }
      } else {
        pathsToRun.add(nestedStep.step.id);
      }
      if (perStep) {
        break;
      }
    }
  }

  await Promise.all(
    // Keep the original branch index when only a subset is restarted. Filtering
    // first would renumber B/C to 0/1 and route persisted coordinates to the
    // wrong graph branch. This translates official fix d5c11e3ba5045969caa7272a7bd1fd141c93ab6c.
    step.steps?.map(async (nestedStep, idx) => {
      if (!pathsToRun.has(nestedStep.step.id)) return;
      return pubsub.publish('workflows', {
        type: 'workflow.step.run',
        runId,
        data: {
          workflowId,
          runId,
          executionPath: restart ? restartContainerPath!.concat([idx]) : executionPath.concat([idx]),
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
  }: ProcessorArgs,
  {
    pubsub,
    stepExecutor,
    step,
  }: {
    pubsub: PubSub;
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

  let onlyStepToRun: Extract<StepFlowEntry, { type: 'step' }> | undefined;

  if (perStep) {
    const stepsToRun = step.steps.filter((_, idx) => truthyIdxs[idx]);
    onlyStepToRun = stepsToRun[0];
  }

  if (onlyStepToRun) {
    const stepIndex = step.steps.findIndex(step => step.step.id === onlyStepToRun.step.id);
    activeStepsPath[onlyStepToRun.step.id] = executionPath.concat([stepIndex]);
    await pubsub.publish('workflows', {
      type: 'workflow.step.run',
      runId,
      data: {
        workflowId,
        runId,
        executionPath: executionPath.concat([stepIndex]),
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
    await Promise.all(
      step.steps.map(async (step, idx) => {
        if (truthyIdxs[idx]) {
          if (step?.type === 'step') {
            activeStepsPath[step.step.id] = executionPath.concat([idx]);
          }
          return pubsub.publish('workflows', {
            type: 'workflow.step.run',
            runId,
            data: {
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
