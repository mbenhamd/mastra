import { randomUUID } from 'node:crypto';
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
  resolveExportedSpanId,
} from '../../observability';
import type { ObservabilityContext, Span } from '../../observability';
import { executeWithContext } from '../../observability/utils';
import type { PersistWorkflowStepUpdateResult } from '../../storage/types';
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
import type { WorkflowLifecycleEvent } from '../lifecycle-events';
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
  WorkflowRunStatus,
} from '../types';
import {
  validateStepInput,
  createDeprecationProxy,
  omitPriorCompletionFields,
  runCountDeprecationMessage,
  validateStepResumeData,
  validateStepSuspendData,
  validateStepStateData,
  validateStepRequestContext,
} from '../utils';
import { prepareStepSnapshot } from './entry';
import type { PersistStepUpdateParams } from './entry';

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
    skipEmits: skipEmitsParam = false,
    outputWriter,
    disableScorers,
    serializedStepGraph,
    iterationCount,
    perStep,
    deferLifecycleResult,
    ...rest
  } = params;
  const skipEmits = skipEmitsParam || engine.options.emitStepEvents === false;
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
  const nestedRunId =
    step.component === 'WORKFLOW' && executionContext.foreachIndex !== undefined ? randomUUID() : undefined;

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

  // Capture the result before recording this invocation as running. Resumed
  // nested workflows need the suspended metadata after the shared map is
  // updated for the serialized start write.
  const priorStepResult = stepResults[step.id];
  const priorSuspendedStepResult = priorStepResult?.status === 'suspended' ? priorStepResult : undefined;

  // Extract suspend data if this step was previously suspended
  let suspendDataToUse = priorSuspendedStepResult?.suspendPayload;

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
    // Drop prior completion/suspend fields so they cannot linger across re-entry
    // (e.g. suspendPayload/suspendedAt after resume, or startedAt > suspendedAt on loops).
    ...omitPriorCompletionFields((stepResults[step.id] ?? {}) as Record<string, unknown>),
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
  const persistenceTracingSpan = stepSpan ?? observabilityContext.tracingContext.currentSpan;

  if (!suppressLifecycleEvents) {
    lifecycleStepState.stepAttempt += 1;
  }

  if (!executionContext.transientExecution) {
    // Ordinary branches share progress until the serialized write boundary.
    // Foreach iterations retain separate results because they share a step id.
    const startStepResults =
      executionContext.foreachIndex === undefined ? stepResults : { ...stepResults, [step.id]: stepInfo };
    if (executionContext.foreachIndex === undefined) {
      stepResults[step.id] = stepInfo as StepResult<any, any, any, any>;
    }
    const startPersist = await engine.persistStepUpdate({
      workflowId,
      runId,
      resourceId,
      serializedStepGraph,
      stepResults: startStepResults as Record<string, StepResult<any, any, any, any>>,
      executionContext,
      workflowStatus: 'running',
      requestContext,
      phase: 'start',
    });
    // A start write can be acknowledged after another worker cancels the run.
    // Recheck before emitting the start event or entering user step code.
    const startDisposition =
      startPersist && startPersist.status !== 'persisted' && startPersist.status !== 'protected_state'
        ? (startPersist.disposition ?? 'canceled')
        : await engine.getAuthoritativeExecutionDisposition({ workflowId, runId, executionGeneration });
    if (startDisposition) {
      delete executionContext.activeStepsPath[step.id];
      const canceledStepResult = {
        ...omitPriorCompletionFields(stepInfo),
        status: 'canceled',
        endedAt: Date.now(),
      } as unknown as StepResult<any, any, any, any>;
      await engine.endStepSpan({
        span: stepSpan,
        operationId: `workflow.${workflowId}.run.${runId}.step.${step.id}.span.end`,
        endOptions: { attributes: { status: 'canceled' } },
      });
      return {
        result: canceledStepResult,
        stepResults: { [step.id]: canceledStepResult },
        mutableContext: engine.buildMutableContext(executionContext),
        requestContext: engine.serializeRequestContext(requestContext),
      };
    }
  }

  if (!suppressLifecycleEvents) {
    await engine.onStepExecutionStart({
      step,
      inputData,
      pubsub,
      executionContext,
      stepCallId,
      stepInfo,
      operationId: `workflow.${workflowId}.run.${runId}.step.${step.id}.running_ev`,
      skipEmits,
    });
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

  // Check if this is a nested workflow that requires special handling
  if (engine.isNestedWorkflowStep(step)) {
    // The shared map records this invocation as running before the platform
    // hook runs. Preserve the suspended parent result for engines that inspect
    // the map to recover the nested run id and resume path.
    const nestedStepResults = priorSuspendedStepResult
      ? { ...stepResults, [step.id]: priorSuspendedStepResult }
      : stepResults;
    const workflowResult = await engine.executeWorkflowStep({
      step,
      stepResults: nestedStepResults,
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

      const stepResult = {
        ...omitPriorCompletionFields(stepInfo),
        ...workflowResult,
      } as StepResult<any, any, any, any>;
      const nestedPublicationBlocked = await persistThenPublishStepResult({
        engine,
        workflowId,
        runId,
        resourceId,
        serializedStepGraph,
        stepResults,
        executionContext,
        requestContext,
        tracingContext:
          stepResult.status === 'suspended' && persistenceTracingSpan
            ? {
                traceId: persistenceTracingSpan.traceId,
                spanId: resolveExportedSpanId(persistenceTracingSpan),
                parentSpanId: persistenceTracingSpan.getParentSpanId(),
              }
            : undefined,
        stepId: step.id,
        stepCallId,
        stepAttempt: lifecycleStepState.stepAttempt,
        execResults: stepResult,
        pubsub,
        suppressLifecycleEvents,
        emitLegacy: false,
        phase: 'nested-step-result',
      });
      if (nestedPublicationBlocked) {
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
      return {
        result: stepResult,
        stepResults: { [step.id]: stepResult },
        mutableContext: engine.buildMutableContext(executionContext),
        // Serialize requestContext only for engines that restore it from
        // serialized results (Inngest memoization); the default engine keeps
        // the original reference and never reads this field.
        requestContext: engine.requiresDurableContextSerialization()
          ? engine.serializeRequestContext(requestContext)
          : undefined,
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
        runId: nestedRunId ?? runId,
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
        resume: priorSuspendedStepResult
          ? {
              steps: resume?.steps?.slice(1) || [],
              resumePayload: resume?.resumePayload,
              runId: priorSuspendedStepResult.suspendPayload?.__workflow_meta?.runId,
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

  if (stepRetryResult.ok && stepRetryResult.result.contextMutations.stateUpdate != null) {
    if (executionContext.foreachIndex === undefined) {
      // Parallel branches share the engine-owned state object. Merge the
      // update in place before the result enters the persistence queue so a
      // sibling cannot snapshot stale state alongside newer step results.
      Object.assign(executionContext.state, stepRetryResult.result.contextMutations.stateUpdate);
    } else {
      // Foreach iterations retain their isolated state update until the
      // iteration result is applied by the parent control-flow handler.
      executionContext.state = { ...executionContext.state, ...stepRetryResult.result.contextMutations.stateUpdate };
    }
  }

  delete executionContext.activeStepsPath[step.id];

  if (abortController.signal.aborted) {
    execResults = { ...execResults, status: 'canceled', endedAt: Date.now() };
  }

  const stepResultForFence = {
    ...omitPriorCompletionFields(stepInfo),
    ...execResults,
  } as StepResult<any, any, any, any>;
  const publicationBlocked = await persistThenPublishStepResult({
    engine,
    workflowId,
    runId,
    resourceId,
    serializedStepGraph,
    stepResults,
    executionContext,
    requestContext,
    tracingContext:
      stepResultForFence.status === 'suspended' && persistenceTracingSpan
        ? {
            traceId: persistenceTracingSpan.traceId,
            spanId: resolveExportedSpanId(persistenceTracingSpan),
            parentSpanId: persistenceTracingSpan.getParentSpanId(),
          }
        : undefined,
    stepId: step.id,
    stepCallId,
    stepAttempt: lifecycleStepState.stepAttempt,
    execResults: stepResultForFence,
    pubsub,
    suppressLifecycleEvents,
    emitLegacy: !skipEmits,
    deferLifecycleResult,
    phase: 'step-result',
  });
  if (publicationBlocked) {
    // The durable terminal owner has already emitted its workflow terminal.
    // Convert the local result to a stop signal, but suppress this worker's
    // delayed step terminal so nothing is appended after workflow.finished.
    execResults = { ...execResults, status: 'canceled', endedAt: Date.now() };
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

  if (nestedRunId) {
    execResults.metadata = { ...execResults.metadata, nestedRunId };
  }

  const stepResult = {
    ...omitPriorCompletionFields(stepInfo),
    ...execResults,
  } as StepResult<any, any, any, any>;

  return {
    result: stepResult,
    stepResults: { [step.id]: stepResult },
    mutableContext: engine.buildMutableContext({
      ...executionContext,
      state: stepRetryResult.ok
        ? (stepRetryResult.result.contextMutations.stateUpdate ?? executionContext.state)
        : executionContext.state,
    }),
    // Serialize requestContext only for engines that restore it from
    // serialized results (Inngest memoization); the default engine keeps
    // the original reference and never reads this field, so serializing
    // here would probe every stored value with JSON.stringify on every step.
    requestContext: engine.requiresDurableContextSerialization()
      ? engine.serializeRequestContext(requestContext)
      : undefined,
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
        mastra: engine.mastra,
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
  /** Canonical events accepted with the fenced snapshot. An empty array suppresses them. */
  canonicalEvents?: WorkflowLifecycleEvent[];
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
    canonicalEvents,
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
    if (canonicalEvents !== undefined && workflowId && executionGeneration) {
      for (const event of canonicalEvents) {
        await publishWorkflowLifecycleEvent({ pubsub, workflowId, runId, executionGeneration, event });
      }
    } else if (stepCallId && stepAttempt && workflowId && executionGeneration) {
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
    if (canonicalEvents !== undefined && workflowId && executionGeneration) {
      for (const event of canonicalEvents) {
        await publishWorkflowLifecycleEvent({ pubsub, workflowId, runId, executionGeneration, event });
      }
    } else if (stepCallId && stepAttempt && workflowId && executionGeneration) {
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

function collectStepResultLifecycleEvents(params: {
  stepId: string;
  stepCallId?: string;
  stepAttempt?: number;
  execResults:
    | StepResult<any, any, any, any>
    | { status: string; output?: unknown; error?: unknown; suspendPayload?: unknown };
}): WorkflowLifecycleEvent[] {
  const { stepId, stepCallId, stepAttempt, execResults } = params;
  if (!stepCallId || !stepAttempt) return [];
  const identity = { stepId, stepCallId, stepAttempt };
  if (execResults.status === 'suspended') {
    return [
      {
        type: 'step.suspended',
        ...identity,
        suspendPayload: (execResults as { suspendPayload?: unknown }).suspendPayload,
      },
    ];
  }
  const lifecycleStatus = execResults.status === 'bailed' ? ('success' as const) : execResults.status;
  const events: WorkflowLifecycleEvent[] = [];
  if (lifecycleStatus === 'success') {
    events.push({ type: 'step.completed', ...identity, output: (execResults as { output?: unknown }).output });
  } else if (lifecycleStatus === 'failed') {
    const rawError = (execResults as { error?: unknown }).error;
    const persistedError =
      rawError instanceof Error ? getErrorFromUnknown(rawError, { serializeStack: false }).toJSON() : rawError;
    events.push({ type: 'step.failed', ...identity, error: persistedError });
  } else if (lifecycleStatus === 'canceled') {
    events.push({ type: 'step.canceled', ...identity });
  }
  if (lifecycleStatus === 'success' || lifecycleStatus === 'failed' || lifecycleStatus === 'canceled') {
    events.push({
      type: 'step.finished',
      ...identity,
      status: lifecycleStatus,
    });
  }
  return events;
}

async function persistThenPublishStepResult(params: {
  engine: DefaultExecutionEngine;
  workflowId: string;
  runId: string;
  resourceId?: string;
  serializedStepGraph: SerializedStepFlowEntry[];
  stepResults: Record<string, StepResult<any, any, any, any>>;
  executionContext: ExecutionContext;
  requestContext: ExecuteStepParams['requestContext'];
  tracingContext?: {
    traceId?: string;
    spanId?: string;
    parentSpanId?: string;
  };
  stepId: string;
  stepCallId?: string;
  stepAttempt?: number;
  execResults: StepResult<any, any, any, any>;
  pubsub: PubSub;
  suppressLifecycleEvents: boolean;
  emitLegacy: boolean;
  deferLifecycleResult?: (emission: Promise<void>) => void;
  phase: string;
}): Promise<boolean> {
  // Sibling branches share this map. Record the local result before awaiting
  // persistence/publication so a sibling cannot overwrite it with "running".
  // Foreach iterations share one step id and retain their aggregate separately.
  if (params.executionContext.foreachIndex === undefined) {
    params.stepResults[params.stepId] = params.execResults;
  }
  const lifecycleEvents = collectStepResultLifecycleEvents(params);
  let canonicalEventsForPublication: WorkflowLifecycleEvent[] | undefined;
  const isCompositeChild =
    params.executionContext.foreachIndex !== undefined || params.executionContext.executionPath.length > 1;
  const stepStatus = params.execResults.status as WorkflowRunStatus;
  const workflowStatus =
    !isCompositeChild && (stepStatus === 'suspended' || stepStatus === 'paused') ? stepStatus : 'running';
  if (!params.executionContext.transientExecution) {
    const persistenceParams: PersistStepUpdateParams = {
      workflowId: params.workflowId,
      runId: params.runId,
      resourceId: params.resourceId,
      serializedStepGraph: params.serializedStepGraph,
      stepResults:
        params.executionContext.foreachIndex === undefined
          ? params.stepResults
          : { ...params.stepResults, [params.stepId]: params.execResults },
      executionContext: params.executionContext,
      workflowStatus,
      requestContext: params.requestContext,
      tracingContext: params.tracingContext,
      lifecycleEvents,
      phase: params.phase,
    };
    const outcome: PersistWorkflowStepUpdateResult | void = await params.engine.persistStepUpdate(persistenceParams);
    // An accepted write can be acknowledged after a remote cancellation has
    // already published its terminal event. Keep the publication-time check
    // until storage and delivery share an ordered lifecycle authority.
    const needsAuthorityFallback =
      !outcome ||
      outcome.status === 'persisted' ||
      (outcome.status === 'protected_state' && workflowStatus === 'running');
    if (needsAuthorityFallback) {
      const executionGeneration = requireWorkflowExecutionGeneration(
        params.executionContext.executionGeneration,
        `Workflow step result ${params.workflowId}/${params.runId}/${params.stepId}`,
      );
      const disposition = await params.engine.getAuthoritativeExecutionDisposition({
        workflowId: params.workflowId,
        runId: params.runId,
        executionGeneration,
      });
      if (disposition) return true;
    }
    const isProtectedIntermediateWrite = outcome?.status === 'protected_state' && workflowStatus === 'running';
    if (outcome && outcome.status !== 'persisted' && !isProtectedIntermediateWrite) {
      return true;
    }
    if (outcome?.status === 'persisted') {
      canonicalEventsForPublication = outcome.acceptedEvents ?? [];
    } else if (params.engine.options?.pruneSnapshot) {
      canonicalEventsForPublication = prepareStepSnapshot(params.engine, persistenceParams).prunedLifecycleEvents ?? [];
    }
  }

  if (params.suppressLifecycleEvents) {
    return false;
  }

  const executionGeneration = params.executionContext.executionGeneration;
  const emitOperationId = `workflow.${params.workflowId}.run.${params.runId}.step.${params.stepId}.emit_result`;
  const lifecycleResultEmission = params.engine.wrapDurableOperation(emitOperationId, async () => {
    await emitStepResultEvents({
      stepId: params.stepId,
      stepCallId: params.stepCallId,
      stepAttempt: params.stepAttempt,
      workflowId: params.workflowId,
      executionGeneration,
      execResults: params.execResults,
      pubsub: params.pubsub,
      runId: params.runId,
      emitLegacy: params.emitLegacy,
      canonicalEvents: canonicalEventsForPublication,
    });
  });
  if (params.deferLifecycleResult) {
    params.deferLifecycleResult(lifecycleResultEmission);
  } else {
    await lifecycleResultEmission;
  }
  return false;
}
