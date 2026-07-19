import type { ActorSignal } from '../../auth/ee';
import type { RequestContext } from '../../di';
import { MastraError, ErrorDomain, ErrorCategory, getErrorFromUnknown } from '../../error';
import type { MastraScorers } from '../../evals';
import { runScorer } from '../../evals/hooks';
import type { PubSub } from '../../events/pubsub';
import {
  EntityType,
  SpanType,
  wrapMastra,
  createObservabilityContext,
  resolveObservabilityContext,
} from '../../observability';
import type { ObservabilityContext, Span } from '../../observability';
import { executeWithContext } from '../../observability/utils';
import { ToolStream } from '../../tools/stream';
import type { DynamicArgument } from '../../types';
import { PUBSUB_SYMBOL, STREAM_FORMAT_SYMBOL, TRANSIENT_EXECUTION_SYMBOL } from '../constants';
import type { DefaultExecutionEngine } from '../default';
import {
  getOrCreateWorkflowStepLifecycleState,
  publishWorkflowLifecycleEvent,
  requireWorkflowExecutionGeneration,
  workflowLifecycleEventsAreSuppressed,
} from '../lifecycle-events';
import type { Step, SuspendOptions } from '../step';
import { getStepResult } from '../step';
import type {
  ExecutionContext,
  OutputWriter,
  RestartExecutionParams,
  SerializedStepFlowEntry,
  StepExecutionResult,
  StepResult,
  TimeTravelExecutionParams,
} from '../types';
import {
  validateStepInput,
  createDeprecationProxy,
  runCountDeprecationMessage,
  validateStepResumeData,
  validateStepSuspendData,
  validateStepStateData,
  validateStepRequestContext,
} from '../utils';

export interface ExecuteStepParams extends ObservabilityContext {
  workflowId: string;
  runId: string;
  resourceId?: string;
  step: Step<string, any, any, any, any, any, any>;
  stepResults: Record<string, StepResult<any, any, any, any>>;
  executionContext: ExecutionContext;
  restart?: RestartExecutionParams;
  timeTravel?: TimeTravelExecutionParams;
  resume?: {
    steps: string[];
    resumePayload: any;
    label?: string;
    forEachIndex?: number;
  };
  prevOutput: any;
  pubsub: PubSub;
  abortController: AbortController;
  requestContext: RequestContext;
  actor?: ActorSignal;
  skipEmits?: boolean;
  outputWriter?: OutputWriter;
  disableScorers?: boolean;
  serializedStepGraph: SerializedStepFlowEntry[];
  iterationCount?: number;
  perStep?: boolean;
  /** @internal Let foreach release its queue slot before awaiting canonical result delivery. */
  deferLifecycleResult?: (emission: Promise<void>) => void;
}

export async function executeStep(
  engine: DefaultExecutionEngine,
  params: ExecuteStepParams,
): Promise<StepExecutionResult> {
  const {
    workflowId,
    runId,
    resourceId,
    step,
    stepResults,
    executionContext,
    restart,
    resume,
    timeTravel,
    prevOutput,
    pubsub,
    abortController,
    requestContext,
    actor,
    skipEmits = false,
    outputWriter,
    disableScorers,
    serializedStepGraph,
    iterationCount,
    perStep,
    deferLifecycleResult,
    ...rest
  } = params;
  const observabilityContext = resolveObservabilityContext(rest);

  const executionGeneration = requireWorkflowExecutionGeneration(
    executionContext.executionGeneration,
    `Workflow step ${workflowId}/${runId}/${step.id}`,
  );
  const lifecycleStepStates = executionContext.lifecycleStepStates ?? {};
  executionContext.executionGeneration = executionGeneration;
  executionContext.lifecycleStepStates = lifecycleStepStates;
  const suppressLifecycleEvents = workflowLifecycleEventsAreSuppressed(pubsub);
  const lifecycleStepState = suppressLifecycleEvents
    ? // `ToolStream` does not serialize `callId` for workflow-step output. The
      // empty local placeholder therefore avoids lifecycle identity work without
      // removing any stream field; only the suppressed lifecycle records use it.
      { stepCallId: '', stepAttempt: 0 }
    : getOrCreateWorkflowStepLifecycleState({
        workflowId,
        runId,
        executionGeneration,
        stepId: step.id,
        executionPath: executionContext.executionPath,
        foreachIndex: executionContext.foreachIndex,
        iterationCount,
        states: lifecycleStepStates,
      }).state;
  const stepCallId = lifecycleStepState.stepCallId;

  const { inputData, validationError: inputValidationError } = await validateStepInput({
    prevOutput,
    step,
    validateInputs: engine.options?.validateInputs ?? true,
  });

  const { validationError: requestContextValidationError } = await validateStepRequestContext({
    requestContext,
    step,
    validateInputs: engine.options?.validateInputs ?? true,
  });

  // Combine validation errors - input validation takes precedence
  const validationError = inputValidationError || requestContextValidationError;

  const { resumeData: timeTravelResumeData, validationError: timeTravelResumeValidationError } =
    await validateStepResumeData({
      resumeData: timeTravel?.stepResults[step.id]?.status === 'suspended' ? timeTravel?.resumeData : undefined,
      step,
    });

  const isTimeTravelResume = timeTravel?.stepResults[step.id]?.status === 'suspended';
  const isExplicitResume = resume?.steps[0] === step.id;
  let isResume = false;
  let resumeDataToUse: unknown;
  if (isTimeTravelResume && !timeTravelResumeValidationError) {
    resumeDataToUse = timeTravelResumeData;
    isResume = true;
  } else if (isTimeTravelResume && timeTravelResumeValidationError) {
    engine.getLogger().warn('Time travel resume data validation failed', {
      stepId: step.id,
      error: timeTravelResumeValidationError.message,
    });
  } else if (isExplicitResume) {
    resumeDataToUse = resume?.resumePayload;
    isResume = true;
  }

  // Extract suspend data if this step was previously suspended
  let suspendDataToUse =
    stepResults[step.id]?.status === 'suspended' ? stepResults[step.id]?.suspendPayload : undefined;

  // A suspended foreach step's step-level suspendPayload only carries the FIRST suspended
  // iteration's payload. When resuming a specific iteration, use that iteration's own payload
  // from `__workflow_meta.foreachOutput` so parallel suspensions don't read a sibling's data
  // (e.g. another tool call's suspended run id).
  const foreachIndex = executionContext.foreachIndex;
  if (suspendDataToUse && foreachIndex !== undefined) {
    const iterationResult = suspendDataToUse.__workflow_meta?.foreachOutput?.[foreachIndex];
    if (iterationResult?.status === 'suspended' && iterationResult.suspendPayload) {
      suspendDataToUse = iterationResult.suspendPayload;
    }
  }

  // Filter out internal workflow metadata before exposing to step code
  if (suspendDataToUse && '__workflow_meta' in suspendDataToUse) {
    const { __workflow_meta, ...userSuspendData } = suspendDataToUse;
    suspendDataToUse = userSuspendData;
  }

  const startTime = isResume ? undefined : Date.now();
  const resumeTime = isResume ? Date.now() : undefined;

  const stepInfo = {
    ...stepResults[step.id],
    ...(isResume ? { resumePayload: resumeDataToUse } : { payload: inputData }),
    ...(startTime ? { startedAt: startTime } : {}),
    ...(resumeTime ? { resumedAt: resumeTime } : {}),
    status: 'running',
    ...(iterationCount ? { metadata: { iterationCount } } : {}),
  };

  executionContext.activeStepsPath[step.id] = executionContext.executionPath;

  const stepSpan = await engine.createStepSpan({
    parentSpan: observabilityContext.tracingContext.currentSpan,
    stepId: step.id,
    operationId: `workflow.${workflowId}.run.${runId}.step.${step.id}.span.start`,
    options: {
      name: `workflow step: '${step.id}'`,
      type: SpanType.WORKFLOW_STEP,
      entityType: EntityType.WORKFLOW_STEP,
      entityId: step.id,
      input: inputData,
      tracingPolicy: engine.options?.tracingPolicy,
      requestContext,
    },
    executionContext,
  });

  const operationId = `workflow.${workflowId}.run.${runId}.step.${step.id}.running_ev`;
  if (!suppressLifecycleEvents) {
    lifecycleStepState.stepAttempt += 1;
  }
  await engine.onStepExecutionStart({
    step,
    inputData,
    pubsub,
    executionContext,
    stepCallId,
    stepInfo,
    operationId,
    skipEmits: skipEmits || suppressLifecycleEvents,
  });
  if (!suppressLifecycleEvents) {
    await publishWorkflowLifecycleEvent({
      pubsub,
      workflowId,
      runId,
      executionGeneration,
      event: isResume
        ? {
            type: 'step.resumed',
            stepId: step.id,
            stepCallId,
            stepAttempt: lifecycleStepState.stepAttempt,
            resumeData: resumeDataToUse,
          }
        : {
            type: 'step.started',
            stepId: step.id,
            stepCallId,
            stepAttempt: lifecycleStepState.stepAttempt,
            input: inputData,
          },
    });
  }

  if (!executionContext.transientExecution) {
    await engine.persistStepUpdate({
      workflowId,
      runId,
      resourceId,
      serializedStepGraph,
      stepResults: {
        ...stepResults,
        [step.id]: stepInfo,
      } as Record<string, StepResult<any, any, any, any>>,
      executionContext,
      workflowStatus: 'running',
      requestContext,
      phase: 'start',
    });
  }

  // Check if this is a nested workflow that requires special handling
  if (engine.isNestedWorkflowStep(step)) {
    const workflowResult = await engine.executeWorkflowStep({
      step,
      stepResults,
      executionContext,
      resume,
      timeTravel,
      prevOutput,
      inputData,
      pubsub,
      startedAt: startTime ?? Date.now(),
      abortController,
      requestContext,
      actor,
      ...observabilityContext,
      outputWriter,
      stepSpan: stepSpan as Span<SpanType.WORKFLOW_STEP> | undefined,
      perStep,
    });

    // If executeWorkflowStep returns a result, wrap it in StepExecutionResult
    if (workflowResult !== null) {
      // End the step span with the nested workflow result
      if (stepSpan) {
        if (workflowResult.status === 'failed') {
          await engine.errorStepSpan({
            span: stepSpan as Span<SpanType.WORKFLOW_STEP>,
            operationId: `workflow.${workflowId}.run.${runId}.step.${step.id}.span.error`,
            errorOptions: {
              error:
                workflowResult.error instanceof Error ? workflowResult.error : new Error(String(workflowResult.error)),
              attributes: { status: 'failed' },
            },
          });
        } else {
          // For success, suspended, paused, tripwire - end the span normally
          // Only 'success' has .output, others may have suspendOutput or nothing
          const output =
            workflowResult.status === 'success' ? workflowResult.output : (workflowResult as any).suspendOutput;

          await engine.endStepSpan({
            span: stepSpan as Span<SpanType.WORKFLOW_STEP>,
            operationId: `workflow.${workflowId}.run.${runId}.step.${step.id}.span.end`,
            endOptions: {
              output,
              attributes: { status: workflowResult.status },
            },
          });
        }
      }

      const stepResult = { ...stepInfo, ...workflowResult } as StepResult<any, any, any, any>;
      const authoritativeDisposition = await engine.getAuthoritativeExecutionDisposition({
        workflowId,
        runId,
        transientExecution: executionContext.transientExecution,
        executionGeneration,
      });
      if (authoritativeDisposition) {
        delete executionContext.activeStepsPath[step.id];
        const canceledStepResult = {
          ...stepResult,
          status: 'canceled',
          endedAt: Date.now(),
        } as unknown as StepResult<any, any, any, any>;
        return {
          result: canceledStepResult,
          stepResults: { [step.id]: canceledStepResult },
          mutableContext: engine.buildMutableContext(executionContext),
          requestContext: engine.serializeRequestContext(requestContext),
        };
      }
      const emitOperationId = `workflow.${workflowId}.run.${runId}.step.${step.id}.emit_result`;
      await engine.wrapDurableOperation(emitOperationId, async () => {
        await emitStepResultEvents({
          stepId: step.id,
          stepCallId,
          stepAttempt: lifecycleStepState.stepAttempt,
          workflowId,
          executionGeneration,
          execResults: stepResult,
          pubsub,
          runId,
          // Inngest's nested-workflow hook already emits the legacy result
          // stream inside its own durable operation. Add only the canonical
          // lifecycle transitions here.
          emitLegacy: false,
        });
      });
      return {
        result: stepResult,
        stepResults: { [step.id]: stepResult },
        mutableContext: engine.buildMutableContext(executionContext),
        requestContext: engine.serializeRequestContext(requestContext),
      };
    }
  }

  const runStep = async (data: any) => {
    // Wrap data with a Proxy to show deprecation warning for runCount
    const proxiedData = createDeprecationProxy(data, {
      paramName: 'runCount',
      deprecationMessage: runCountDeprecationMessage,
      logger: engine.getLogger(),
    });

    return executeWithContext({ span: stepSpan, fn: () => step.execute(proxiedData) });
  };

  let execResults: any;

  const retries = step.retries ?? executionContext.retryConfig.attempts ?? 0;
  const delay = executionContext.retryConfig.delay ?? 0;
  const initialStepAttempt = lifecycleStepState.stepAttempt;

  // Use executeStepWithRetry to handle retry logic
  // Default engine: internal retry loop
  // Inngest engine: throws RetryAfterError for external retry handling
  const stepRetryResult = await engine.executeStepWithRetry(
    `workflow.${workflowId}.step.${step.id}`,
    async retryCount => {
      if (validationError) {
        throw validationError;
      }

      lifecycleStepState.stepAttempt = initialStepAttempt + retryCount;
      if (retryCount > 0 && !suppressLifecycleEvents) {
        await publishWorkflowLifecycleEvent({
          pubsub,
          workflowId,
          runId,
          executionGeneration,
          event: {
            type: 'step.retrying',
            stepId: step.id,
            stepCallId,
            stepAttempt: lifecycleStepState.stepAttempt,
          },
        });
      }

      let timeTravelSteps: string[] = [];
      if (timeTravel && timeTravel.steps.length > 0) {
        timeTravelSteps = timeTravel.steps[0] === step.id ? timeTravel.steps.slice(1) : [];
      }

      let suspended: { payload: any } | undefined;
      let bailed: { payload: any } | undefined;
      const contextMutations: {
        suspendedPaths: Record<string, number[]>;
        resumeLabels: Record<string, { stepId: string; foreachIndex?: number }>;
        stateUpdate: any;
        requestContextUpdate: Record<string, any> | null;
      } = {
        suspendedPaths: {},
        resumeLabels: {},
        stateUpdate: null,
        requestContextUpdate: null,
      };

      // For nested workflow steps, pass raw mastra - the nested workflow will
      // register it on its own engine and wrap it fresh for its own steps.
      // For regular steps, wrap mastra with current step span for proper tracing.
      const isNestedWorkflow = step.component === 'WORKFLOW';
      const mastraForStep = engine.mastra
        ? isNestedWorkflow
          ? engine.mastra
          : wrapMastra(engine.mastra, { currentSpan: stepSpan })
        : undefined;

      const output = await runStep({
        runId,
        resourceId,
        workflowId,
        mastra: mastraForStep,
        requestContext,
        actor,
        inputData,
        state: executionContext.state,
        setState: async (state: any) => {
          const { stateData, validationError: stateValidationError } = await validateStepStateData({
            stateData: state,
            step,
            validateInputs: engine.options?.validateInputs ?? true,
          });
          if (stateValidationError) {
            throw stateValidationError;
          }
          // executionContext.state = stateData;
          contextMutations.stateUpdate = stateData;
        },
        retryCount,
        resumeData: resumeDataToUse,
        suspendData: suspendDataToUse,
        ...createObservabilityContext({ currentSpan: stepSpan }),
        getInitData: () => stepResults?.input as any,
        getStepResult: getStepResult.bind(null, stepResults),
        suspend: async (suspendPayload?: any, suspendOptions?: SuspendOptions): Promise<void> => {
          if (executionContext.transientExecution) {
            throw new Error('Transient workflow runs cannot suspend');
          }
          const { suspendData, validationError: suspendValidationError } = await validateStepSuspendData({
            suspendData: suspendPayload,
            step,
            validateInputs: engine.options?.validateInputs ?? true,
          });
          if (suspendValidationError) {
            throw suspendValidationError;
          }
          // Capture mutations for return value (needed for Inngest replay)
          contextMutations.suspendedPaths[step.id] = executionContext.executionPath;
          // Also apply directly for Default engine
          executionContext.suspendedPaths[step.id] = executionContext.executionPath;

          if (suspendOptions?.resumeLabel) {
            const resumeLabel = Array.isArray(suspendOptions.resumeLabel)
              ? suspendOptions.resumeLabel
              : [suspendOptions.resumeLabel];
            for (const label of resumeLabel) {
              const labelData = {
                stepId: step.id,
                foreachIndex: executionContext.foreachIndex,
              };
              // Capture for return value
              contextMutations.resumeLabels[label] = labelData;
              // Apply directly for Default engine
              executionContext.resumeLabels[label] = labelData;
            }
          }

          suspended = { payload: suspendData };
        },
        bail: (result: any) => {
          bailed = { payload: result };
        },
        abort: () => {
          abortController?.abort();
        },
        // Only pass resume data if this step was actually suspended before
        // This prevents pending nested workflows from trying to resume instead of start
        resume:
          stepResults[step.id]?.status === 'suspended'
            ? {
                steps: resume?.steps?.slice(1) || [],
                resumePayload: resume?.resumePayload,
                runId: stepResults[step.id]?.suspendPayload?.__workflow_meta?.runId,
                label: resume?.label,
                forEachIndex: resume?.forEachIndex,
              }
            : undefined,
        // Only pass restart data if this step is part of activeStepsPath
        // This prevents pending nested workflows from trying to restart instead of start
        restart: !!restart?.activeStepsPath?.[step.id],
        timeTravel:
          timeTravelSteps.length > 0
            ? {
                inputData: timeTravel?.inputData,
                steps: timeTravelSteps,
                nestedStepResults: timeTravel?.nestedStepResults,
                resumeData: timeTravel?.resumeData,
              }
            : undefined,
        [PUBSUB_SYMBOL]: pubsub,
        [STREAM_FORMAT_SYMBOL]: executionContext.format,
        [TRANSIENT_EXECUTION_SYMBOL]: executionContext.transientExecution,
        engine: engine.getEngineContext(),
        abortSignal: abortController?.signal,
        writer: new ToolStream(
          {
            prefix: 'workflow-step',
            callId: stepCallId,
            name: step.id,
            runId,
          },
          outputWriter,
        ),
        outputWriter,
        // Disable scorers must be explicitly set to false they are on by default
        scorers: disableScorers === false ? undefined : step.scorers,
        validateInputs: engine.options?.validateInputs,
        perStep,
      });

      // Capture requestContext state after step execution (only for engines that need it)
      if (engine.requiresDurableContextSerialization()) {
        contextMutations.requestContextUpdate = engine.serializeRequestContext(requestContext);
      }

      const isNestedWorkflowStep = step.component === 'WORKFLOW';

      const nestedWflowStepPaused = isNestedWorkflowStep && perStep;

      return {
        output,
        suspended,
        bailed,
        contextMutations,
        nestedWflowStepPaused,
        lifecycleStepAttempt: lifecycleStepState.stepAttempt,
      };
    },
    { retries, delay, stepSpan, workflowId, runId },
  );

  // Check if step execution failed
  if (!stepRetryResult.ok) {
    const { retryCount: finalRetryCount = 0, ...failure } = stepRetryResult.error;
    lifecycleStepState.stepAttempt = initialStepAttempt + finalRetryCount;
    execResults = failure;
  } else {
    const { result: durableResult } = stepRetryResult;

    // Inngest restores the durable operation result without re-running the
    // retry loop. Retain its final attempt number so persisted snapshots and
    // later resumes agree with the lifecycle events already published.
    lifecycleStepState.stepAttempt = durableResult.lifecycleStepAttempt;

    // Apply context mutations from the durable operation result
    // For Default: these were already applied during execution, this is a no-op
    // For Inngest: on replay, the wrapped function didn't re-execute, so we restore from the memoized result
    Object.assign(executionContext.suspendedPaths, durableResult.contextMutations.suspendedPaths);
    Object.assign(executionContext.resumeLabels, durableResult.contextMutations.resumeLabels);

    // Restore requestContext from memoized result (only for engines that need it)
    if (engine.requiresDurableContextSerialization() && durableResult.contextMutations.requestContextUpdate) {
      requestContext.clear();
      for (const [key, value] of Object.entries(durableResult.contextMutations.requestContextUpdate)) {
        requestContext.set(key, value);
      }
    }

    if (step.scorers) {
      await runScorersForStep({
        engine,
        scorers: step.scorers,
        runId,
        input: inputData,
        output: durableResult.output,
        workflowId,
        stepId: step.id,
        requestContext,
        disableScorers,
        ...createObservabilityContext({ currentSpan: stepSpan }),
      });
    }

    if (durableResult.suspended) {
      execResults = {
        status: 'suspended',
        suspendPayload: durableResult.suspended.payload,
        ...(durableResult.output ? { suspendOutput: durableResult.output } : {}),
        suspendedAt: Date.now(),
      };
    } else if (durableResult.bailed) {
      execResults = { status: 'bailed', output: durableResult.bailed.payload, endedAt: Date.now() };
    } else if (durableResult.nestedWflowStepPaused) {
      execResults = { status: 'paused' };
    } else {
      execResults = { status: 'success', output: durableResult.output, endedAt: Date.now() };
    }
  }

  delete executionContext.activeStepsPath[step.id];

  if (abortController.signal.aborted) {
    execResults = { ...execResults, status: 'canceled', endedAt: Date.now() };
  }

  const authoritativeDisposition = await engine.getAuthoritativeExecutionDisposition({
    workflowId,
    runId,
    transientExecution: executionContext.transientExecution,
    executionGeneration,
  });
  if (authoritativeDisposition) {
    // The durable terminal owner has already emitted its workflow terminal.
    // Convert the local result to a stop signal, but suppress this worker's
    // delayed step terminal so nothing is appended after workflow.finished.
    execResults = { ...execResults, status: 'canceled', endedAt: Date.now() };
  }

  if (!authoritativeDisposition && !suppressLifecycleEvents) {
    const emitOperationId = `workflow.${workflowId}.run.${runId}.step.${step.id}.emit_result`;
    const lifecycleResultEmission = engine.wrapDurableOperation(emitOperationId, async () => {
      await emitStepResultEvents({
        stepId: step.id,
        stepCallId,
        stepAttempt: lifecycleStepState.stepAttempt,
        workflowId,
        executionGeneration,
        execResults: { ...stepInfo, ...execResults } as StepResult<any, any, any, any>,
        pubsub,
        runId,
        emitLegacy: !skipEmits,
      });
    });
    if (deferLifecycleResult) {
      deferLifecycleResult(lifecycleResultEmission);
    } else {
      await lifecycleResultEmission;
    }
  }

  if (execResults.status != 'failed') {
    await engine.endStepSpan({
      span: stepSpan,
      operationId: `workflow.${workflowId}.run.${runId}.step.${step.id}.span.end`,
      endOptions: {
        output: execResults.output,
        attributes: {
          status: execResults.status,
        },
      },
    });
  }

  const stepResult = { ...stepInfo, ...execResults } as StepResult<any, any, any, any>;

  return {
    result: stepResult,
    stepResults: { [step.id]: stepResult },
    mutableContext: engine.buildMutableContext({
      ...executionContext,
      state: stepRetryResult.ok
        ? (stepRetryResult.result.contextMutations.stateUpdate ?? executionContext.state)
        : executionContext.state,
    }),
    requestContext: engine.serializeRequestContext(requestContext),
  };
}

export interface RunScorersParams extends ObservabilityContext {
  engine: DefaultExecutionEngine;
  scorers: DynamicArgument<MastraScorers>;
  runId: string;
  input: any;
  output: any;
  requestContext: RequestContext;
  workflowId: string;
  stepId: string;
  disableScorers?: boolean;
}

export async function runScorersForStep(params: RunScorersParams): Promise<void> {
  const { engine, scorers, runId, input, output, workflowId, stepId, requestContext, disableScorers, ...rest } = params;
  const observabilityContext = resolveObservabilityContext(rest);

  let scorersToUse = scorers;
  if (typeof scorersToUse === 'function') {
    try {
      scorersToUse = await scorersToUse({
        requestContext: requestContext,
      });
    } catch (e) {
      const errorInstance = getErrorFromUnknown(e, { serializeStack: false });
      const mastraError = new MastraError(
        {
          id: 'WORKFLOW_FAILED_TO_FETCH_SCORERS',
          domain: ErrorDomain.MASTRA_WORKFLOW,
          category: ErrorCategory.USER,
          details: {
            runId,
            workflowId,
            stepId,
          },
        },
        errorInstance,
      );
      engine.getLogger()?.trackException(mastraError);
      engine.getLogger()?.error('Error fetching scorers: ' + errorInstance?.stack);
    }
  }

  if (!disableScorers && scorersToUse && Object.keys(scorersToUse || {}).length > 0) {
    for (const [_id, scorerObject] of Object.entries(scorersToUse || {})) {
      if (engine.mastra) {
        scorerObject.scorer.__registerMastra(engine.mastra);
        engine.mastra.addScorer(scorerObject.scorer, undefined, { source: 'code' });
      }
      runScorer({
        scorerId: scorerObject.scorer.id,
        scorerObject: scorerObject,
        runId: runId,
        input: input,
        output: output,
        requestContext,
        entity: {
          id: workflowId,
          stepId: stepId,
        },
        structuredOutput: true,
        source: 'LIVE',
        entityType: 'WORKFLOW',
        ...observabilityContext,
      });
    }
  }
}

/**
 * Emit step result events (suspended, result, finish).
 * Shared between Default and Inngest execution engines.
 */
export async function emitStepResultEvents(params: {
  stepId: string;
  stepCallId?: string;
  stepAttempt?: number;
  workflowId?: string;
  executionGeneration?: string;
  execResults:
    | StepResult<any, any, any, any>
    | { status: 'canceled'; output?: unknown; error?: unknown; suspendPayload?: unknown };
  pubsub: PubSub;
  runId: string;
  /** Preserve historical watch suppression while still emitting canonical lifecycle events. */
  emitLegacy?: boolean;
}): Promise<void> {
  const {
    stepId,
    stepCallId,
    stepAttempt,
    workflowId,
    executionGeneration,
    execResults,
    pubsub,
    runId,
    emitLegacy = true,
  } = params;
  const lifecycleStatus = (execResults as any).status === 'bailed' ? ('success' as const) : execResults.status;
  const payloadBase = stepCallId
    ? { id: stepId, stepCallId, ...(stepAttempt === undefined ? {} : { stepAttempt }) }
    : { id: stepId };

  if (execResults.status === 'suspended') {
    if (emitLegacy) {
      await pubsub.publish(`workflow.events.v2.${runId}`, {
        type: 'watch',
        runId,
        data: { type: 'workflow-step-suspended', payload: { ...payloadBase, ...execResults } },
      });
    }
    if (stepCallId && stepAttempt && workflowId && executionGeneration) {
      await publishWorkflowLifecycleEvent({
        pubsub,
        workflowId,
        runId,
        executionGeneration,
        event: {
          type: 'step.suspended',
          stepId,
          stepCallId,
          stepAttempt,
          suspendPayload: execResults.suspendPayload,
        },
      });
    }
  } else {
    if (emitLegacy) {
      await pubsub.publish(`workflow.events.v2.${runId}`, {
        type: 'watch',
        runId,
        data: { type: 'workflow-step-result', payload: { ...payloadBase, ...execResults } },
      });
      await pubsub.publish(`workflow.events.v2.${runId}`, {
        type: 'watch',
        runId,
        data: {
          type: 'workflow-step-finish',
          payload: { ...payloadBase, status: execResults.status, metadata: {} },
        },
      });
    }
    if (stepCallId && stepAttempt && workflowId && executionGeneration) {
      const identity = { stepId, stepCallId, stepAttempt };
      if (lifecycleStatus === 'success') {
        await publishWorkflowLifecycleEvent({
          pubsub,
          workflowId,
          runId,
          executionGeneration,
          event: { type: 'step.completed', ...identity, output: (execResults as any).output },
        });
      } else if (lifecycleStatus === 'failed') {
        await publishWorkflowLifecycleEvent({
          pubsub,
          workflowId,
          runId,
          executionGeneration,
          event: { type: 'step.failed', ...identity, error: (execResults as any).error },
        });
      } else if (lifecycleStatus === 'canceled') {
        await publishWorkflowLifecycleEvent({
          pubsub,
          workflowId,
          runId,
          executionGeneration,
          event: { type: 'step.canceled', ...identity },
        });
      }

      if (lifecycleStatus === 'success' || lifecycleStatus === 'failed' || lifecycleStatus === 'canceled') {
        await publishWorkflowLifecycleEvent({
          pubsub,
          workflowId,
          runId,
          executionGeneration,
          event: { type: 'step.finished', ...identity, status: lifecycleStatus },
        });
      }
    }
  }
}
