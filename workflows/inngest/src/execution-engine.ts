import { AsyncLocalStorage } from 'node:async_hooks';
import { randomUUID } from 'node:crypto';
import type { ActorSignal } from '@mastra/core/auth/ee';
import type { RequestContext } from '@mastra/core/di';
import { getErrorFromUnknown, MastraNonRetryableError } from '@mastra/core/error';
import type { SerializedError } from '@mastra/core/error';
import type { PubSub } from '@mastra/core/events';
import type { Mastra } from '@mastra/core/mastra';
import type { EntityType } from '@mastra/core/observability';
import {
  DefaultExecutionEngine,
  createTimeTravelExecutionParams,
  requireWorkflowExecutionGeneration,
} from '@mastra/core/workflows';
import type {
  ExecutionContext,
  Step,
  StepResult,
  StepFailure,
  ExecutionEngineOptions,
  TimeTravelExecutionParams,
  WorkflowResult,
} from '@mastra/core/workflows';
import type { Inngest, BaseContext } from 'inngest';
import { NonRetriableError, StepError } from 'inngest';
import { NESTED_WORKFLOW_OUTPUT_MODE } from './nested-workflow-output';
import { inngestWorkflowResumeOperationHash } from './resume-operation';
import { InngestWorkflow } from './workflow';

const RESUMED_CHILD_TERMINAL_STATUSES = new Set(['success', 'failed', 'canceled', 'tripwire', 'bailed', 'skipped']);

function isNonRetryableStepFailure(error: unknown): boolean {
  if (error instanceof MastraNonRetryableError || error instanceof NonRetriableError) {
    return true;
  }

  if (error instanceof Error && error.cause !== undefined && isNonRetryableStepFailure(error.cause)) {
    return true;
  }

  if (error && typeof error === 'object') {
    const record = error as {
      nonRetryable?: true;
      error?: unknown;
      name?: string;
      isNonRetryable?: boolean;
    };

    if (record.nonRetryable) {
      return true;
    }

    if (record.name === 'MastraNonRetryableError' || record.isNonRetryable) {
      return true;
    }

    if (record.error !== undefined && isNonRetryableStepFailure(record.error)) {
      return true;
    }
  }

  return false;
}

const retryCountStorage = new AsyncLocalStorage<number>();

export class InngestExecutionEngine extends DefaultExecutionEngine {
  private inngestStep: BaseContext<Inngest>['step'];
  private inngestAttempts: number;

  constructor(
    mastra: Mastra,
    inngestStep: BaseContext<Inngest>['step'],
    inngestAttempts: number = 0,
    options: ExecutionEngineOptions,
  ) {
    super({ mastra, options });
    this.inngestStep = inngestStep;
    this.inngestAttempts = inngestAttempts;
  }

  override getOrGenerateRetryCount(_stepId: string): number {
    return retryCountStorage.getStore() ?? 0;
  }

  // =============================================================================
  // Hook Overrides
  // =============================================================================

  /**
   * Format errors while preserving Error instances and their custom properties.
   * Uses getErrorFromUnknown to ensure all error properties are preserved.
   */
  protected formatResultError(
    error: Error | string | undefined,
    lastOutput: StepResult<any, any, any, any>,
  ): SerializedError {
    const outputError = (lastOutput as StepFailure<any, any, any, any>)?.error;
    const errorSource = error || outputError;
    const errorInstance = getErrorFromUnknown(errorSource, {
      serializeStack: true, // Include stack in JSON for better debugging in Inngest
      fallbackMessage: 'Unknown workflow error',
    });
    return errorInstance.toJSON();
  }

  /**
   * Detect InngestWorkflow instances for special nested workflow handling
   */
  isNestedWorkflowStep(step: Step<any, any, any>): boolean {
    return step instanceof InngestWorkflow;
  }

  /**
   * Inngest requires requestContext serialization for memoization.
   * When steps are replayed, the original function doesn't re-execute,
   * so requestContext modifications must be captured and restored.
   */
  requiresDurableContextSerialization(): boolean {
    return true;
  }

  /**
   * Execute a step with retry logic for Inngest.
   * Retries are handled via step-level retry (RetryAfterError thrown INSIDE step.run()).
   * After retries exhausted, error propagates here and we return a failed result.
   */
  async executeStepWithRetry<T>(
    stepId: string,
    runStep: (retryCount: number) => Promise<T>,
    params: {
      retries: number;
      delay: number;
      stepSpan?: any;
      workflowId: string;
      runId: string;
    },
  ): Promise<
    | { ok: true; result: T }
    | {
        ok: false;
        error: { status: 'failed'; error: Error; endedAt: number; nonRetryable?: true; retryCount?: number };
      }
  > {
    for (let i = 0; i < params.retries + 1; i++) {
      if (i > 0 && params.delay) {
        await new Promise(resolve => setTimeout(resolve, params.delay));
      }
      try {
        // Every attempt needs its own Inngest step id. On function replay the
        // failed attempt is restored from durable history and the loop advances
        // to the next id without re-running the user callback.
        const result = await retryCountStorage.run(i, () =>
          this.wrapDurableOperation(`${stepId}.attempt.${i}`, () => runStep(i)),
        );
        return { ok: true, result };
      } catch (e) {
        const isNonRetryable = isNonRetryableStepFailure(e);

        if (isNonRetryable || i === params.retries) {
          // After step-level retries exhausted, extract failure from error cause
          const cause = (e as any)?.cause;
          if (cause?.status === 'failed') {
            params.stepSpan?.error({
              error: e,
              attributes: { status: 'failed' },
            });
            // Ensure cause.error is an Error instance
            if (cause.error && !(cause.error instanceof Error)) {
              cause.error = getErrorFromUnknown(cause.error, { serializeStack: false });
            }
            return {
              ok: false,
              error: {
                ...cause,
                retryCount: i,
                ...(isNonRetryable && { nonRetryable: true as const }),
              },
            };
          }

          // Fallback for other errors - preserve the original error instance
          const errorInstance = getErrorFromUnknown(e, {
            serializeStack: false,
            fallbackMessage: 'Unknown step execution error',
          });
          params.stepSpan?.error({
            error: errorInstance,
            attributes: { status: 'failed' },
          });
          return {
            ok: false,
            error: {
              status: 'failed',
              error: errorInstance,
              endedAt: Date.now(),
              retryCount: i,
              ...(isNonRetryable && { nonRetryable: true as const }),
            },
          };
        }
      }
    }
    // Should never reach here, but TypeScript needs it
    return {
      ok: false,
      error: { status: 'failed', error: new Error('Unknown error'), endedAt: Date.now(), retryCount: params.retries },
    };
  }

  /**
   * Use Inngest's sleep primitive for durability
   */
  async executeSleepDuration(duration: number, sleepId: string, workflowId: string): Promise<void> {
    await this.inngestStep.sleep(`workflow.${workflowId}.sleep.${sleepId}`, duration < 0 ? 0 : duration);
  }

  /**
   * Use Inngest's sleepUntil primitive for durability
   */
  async executeSleepUntilDate(date: Date, sleepUntilId: string, workflowId: string): Promise<void> {
    await this.inngestStep.sleepUntil(`workflow.${workflowId}.sleepUntil.${sleepUntilId}`, date);
  }

  /**
   * Wrap durable operations in Inngest step.run() for durability.
   *
   * IMPORTANT: Errors are wrapped with a cause structure before throwing.
   * This is necessary because Inngest's error serialization (serialize-error-cjs)
   * only captures standard Error properties (message, name, stack, code, cause).
   * Custom properties like statusCode, responseHeaders from AI SDK errors would
   * be lost. By putting our serialized error (via getErrorFromUnknown with toJSON())
   * in the cause property, we ensure custom properties survive serialization.
   * The cause property is in serialize-error-cjs's allowlist, and when the cause
   * object is finally JSON.stringify'd, our error's toJSON() is called.
   */
  async wrapDurableOperation<T>(operationId: string, operationFn: () => Promise<T>): Promise<T> {
    const result = await this.inngestStep.run(operationId, async () => {
      try {
        const fnResult = await operationFn();
        return fnResult;
      } catch (e) {
        const errorInstance = getErrorFromUnknown(e, {
          serializeStack: false,
          fallbackMessage: 'Unknown step execution error',
        });
        const isNonRetryable = isNonRetryableStepFailure(e);
        throw new Error(errorInstance.message, {
          cause: {
            status: 'failed',
            error: errorInstance,
            endedAt: Date.now(),
            ...(isNonRetryable && { nonRetryable: true as const }),
          },
        });
      }
    });
    return result as T;
  }

  /**
   * Provide Inngest step primitive in engine context
   */
  getEngineContext(): Record<string, any> {
    return { step: this.inngestStep };
  }

  /**
   * For Inngest, lifecycle callbacks are invoked in the workflow's finalize step
   * (wrapped in step.run for durability), not in execute(). Override to skip.
   */
  public async invokeLifecycleCallbacks(_result: {
    status: any;
    result?: any;
    error?: any;
    steps: Record<string, any>;
    tripwire?: any;
    runId: string;
    workflowId: string;
    resourceId?: string;
    input?: any;
    requestContext: RequestContext;
    state: Record<string, any>;
  }): Promise<void> {
    // No-op: Inngest handles callbacks in workflow.ts finalize step
  }

  /**
   * Actually invoke the lifecycle callbacks. Called from workflow.ts finalize step.
   */
  public async invokeLifecycleCallbacksInternal(result: {
    status: any;
    result?: any;
    error?: any;
    steps: Record<string, any>;
    tripwire?: any;
    runId: string;
    workflowId: string;
    resourceId?: string;
    input?: any;
    requestContext: RequestContext;
    state: Record<string, any>;
  }): Promise<void> {
    return super.invokeLifecycleCallbacks(result);
  }

  // =============================================================================
  // Durable Span Lifecycle Hooks
  // =============================================================================

  /**
   * Create a step span durably - on first execution, creates and exports span.
   * On replay, returns cached span data without re-creating.
   */
  async createStepSpan(params: {
    parentSpan: any;
    stepId: string;
    operationId: string;
    options: {
      name: string;
      type: any;
      input?: unknown;
      entityType?: string;
      entityId?: string;
      tracingPolicy?: any;
    };
    executionContext: ExecutionContext;
  }): Promise<any> {
    const { executionContext, operationId, options, parentSpan } = params;

    // Use the actual parent span's ID if provided (e.g., for steps inside control-flow),
    // otherwise fall back to workflow span
    const parentSpanId = parentSpan?.id ?? executionContext.tracingIds?.workflowSpanId;

    // Use wrapDurableOperation to memoize span creation
    const exportedSpan = await this.wrapDurableOperation(operationId, async () => {
      const observability = this.mastra?.observability?.getSelectedInstance({});
      if (!observability) return undefined;

      // Create span using tracingIds for traceId, and actual parent span for parentSpanId
      const span = observability.startSpan({
        ...options,
        entityType: options.entityType as EntityType | undefined,
        traceId: executionContext.tracingIds?.traceId,
        parentSpanId,
      });

      // Return serializable form
      return span?.exportSpan();
    });

    // Return a rebuilt span that can have .end()/.error() called later
    if (exportedSpan) {
      const observability = this.mastra?.observability?.getSelectedInstance({});
      return observability?.rebuildSpan(exportedSpan);
    }

    return undefined;
  }

  /**
   * End a step span durably.
   */
  async endStepSpan(params: {
    span: any;
    operationId: string;
    endOptions: {
      output?: unknown;
      attributes?: Record<string, unknown>;
    };
  }): Promise<void> {
    const { span, operationId, endOptions } = params;
    if (!span) return;

    await this.wrapDurableOperation(operationId, async () => {
      span.end(endOptions);
    });
  }

  /**
   * Record error on step span durably.
   */
  async errorStepSpan(params: {
    span: any;
    operationId: string;
    errorOptions: {
      error: Error;
      attributes?: Record<string, unknown>;
    };
  }): Promise<void> {
    const { span, operationId, errorOptions } = params;
    if (!span) return;

    await this.wrapDurableOperation(operationId, async () => {
      span.error(errorOptions);
    });
  }

  /**
   * Create a generic child span durably (for control-flow operations).
   * On first execution, creates and exports span. On replay, returns cached span data.
   */
  async createChildSpan(params: {
    parentSpan: any;
    operationId: string;
    options: {
      name: string;
      type: any;
      input?: unknown;
      attributes?: Record<string, unknown>;
    };
    executionContext: ExecutionContext;
  }): Promise<any> {
    const { executionContext, operationId, options, parentSpan } = params;

    // Use the actual parent span's ID if provided, otherwise fall back to workflow span
    const parentSpanId = parentSpan?.id ?? executionContext.tracingIds?.workflowSpanId;

    // Use wrapDurableOperation to memoize span creation
    const exportedSpan = await this.wrapDurableOperation(operationId, async () => {
      const observability = this.mastra?.observability?.getSelectedInstance({});
      if (!observability) return undefined;

      // Create span using tracingIds for traceId, and actual parent span for parentSpanId
      const span = observability.startSpan({
        ...options,
        traceId: executionContext.tracingIds?.traceId,
        parentSpanId,
        tracingPolicy: this.options?.tracingPolicy,
      });

      // Return serializable form
      return span?.exportSpan();
    });

    // Return a rebuilt span that can have .end()/.error() called later
    if (exportedSpan) {
      const observability = this.mastra?.observability?.getSelectedInstance({});
      return observability?.rebuildSpan(exportedSpan);
    }

    return undefined;
  }

  /**
   * End a generic child span durably (for control-flow operations).
   */
  async endChildSpan(params: {
    span: any;
    operationId: string;
    endOptions?: {
      output?: unknown;
      attributes?: Record<string, unknown>;
    };
  }): Promise<void> {
    const { span, operationId, endOptions } = params;
    if (!span) return;

    await this.wrapDurableOperation(operationId, async () => {
      span.end(endOptions);
    });
  }

  /**
   * Record error on a generic child span durably (for control-flow operations).
   */
  async errorChildSpan(params: {
    span: any;
    operationId: string;
    errorOptions: {
      error: Error;
      attributes?: Record<string, unknown>;
    };
  }): Promise<void> {
    const { span, operationId, errorOptions } = params;
    if (!span) return;

    await this.wrapDurableOperation(operationId, async () => {
      span.error(errorOptions);
    });
  }

  /**
   * Execute nested InngestWorkflow using inngestStep.invoke() for durability.
   * This MUST be called directly (not inside step.run()) due to Inngest constraints.
   *
   * @param params - The nested workflow step and its current execution state.
   * @returns The nested workflow step result, or null when the step is not an Inngest workflow.
   */
  async executeWorkflowStep(params: {
    step: Step<string, any, any>;
    stepResults: Record<string, StepResult<any, any, any, any>>;
    executionContext: ExecutionContext;
    resume?: {
      steps: string[];
      resumePayload: any;
      runId?: string;
    };
    timeTravel?: TimeTravelExecutionParams;
    prevOutput: any;
    inputData: any;
    pubsub: PubSub;
    startedAt: number;
    perStep?: boolean;
    stepSpan?: any;
    actor?: ActorSignal;
    requestContext?: RequestContext;
  }): Promise<StepResult<any, any, any, any> | null> {
    // Only handle InngestWorkflow instances
    if (!(params.step instanceof InngestWorkflow)) {
      return null;
    }

    const {
      step,
      stepResults,
      executionContext,
      resume,
      timeTravel,
      prevOutput,
      inputData,
      pubsub,
      startedAt,
      perStep,
      stepSpan,
      actor,
      requestContext: parentRequestContext,
    } = params;
    const forwardedRequestContext = parentRequestContext
      ? this.serializeRequestContext(parentRequestContext)
      : (inputData?.requestContextEntries ?? {});

    // Build trace context to propagate to nested workflow
    const nestedTracingContext = executionContext.tracingIds?.traceId
      ? {
          traceId: executionContext.tracingIds.traceId,
          parentSpanId: stepSpan?.id,
        }
      : undefined;

    const resumeStepId = resume?.steps?.[0];
    const resumeStepResult = resumeStepId ? stepResults[resumeStepId] : undefined;
    const resumeStepIsSuspended = resumeStepResult?.status === 'suspended';
    const explicitNestedResume = resumeStepId === step.id && resumeStepIsSuspended;
    const resumeRunId = resumeStepResult?.suspendPayload?.__workflow_meta?.runId;
    let result: WorkflowResult<any, any, any, any>;
    let runId: string;

    const isTimeTravel = !!(timeTravel && timeTravel.steps?.length > 1 && timeTravel.steps[0] === step.id);

    try {
      const workflowsStoreForResume =
        resumeRunId && !explicitNestedResume ? await this.mastra?.getStorage()?.getStore('workflows') : undefined;
      const legacyNestedResumeSnapshot =
        resumeStepIsSuspended && resumeRunId && workflowsStoreForResume
          ? await workflowsStoreForResume.loadWorkflowSnapshot({
              workflowName: step.id,
              runId: resumeRunId,
            })
          : undefined;
      const legacyResumeSource =
        legacyNestedResumeSnapshot?.status === 'running' && legacyNestedResumeSnapshot.resumeCheckpoint
          ? legacyNestedResumeSnapshot.resumeCheckpoint.snapshot
          : legacyNestedResumeSnapshot?.status === 'suspended'
            ? legacyNestedResumeSnapshot
            : undefined;
      const legacyNestedResumeOwned =
        resumeStepId !== undefined &&
        (Object.prototype.hasOwnProperty.call(legacyResumeSource?.suspendedPaths ?? {}, resumeStepId) ||
          legacyNestedResumeSnapshot?.resumeResultReceipt?.operationReplayContext.steps[0] === resumeStepId);
      // The execution handler normally sends a parent-qualified path whose
      // first segment is this nested workflow ID. Keep accepting the older
      // direct-engine shape, where the path starts at the suspended child step,
      // only when the referenced run and suspended path are owned by this
      // workflow. The status and ownership checks prevent stale resume metadata
      // from turning a later sibling or repeated occurrence into a resume.
      const isResume =
        resume !== undefined &&
        resume.steps.length > 0 &&
        (explicitNestedResume || (resumeRunId !== undefined && legacyNestedResumeOwned));

      if (isResume && resume) {
        runId = resumeRunId ?? randomUUID();
        const workflowsStore = workflowsStoreForResume ?? (await this.mastra?.getStorage()?.getStore('workflows'));
        const snapshot: any =
          legacyNestedResumeSnapshot ??
          (await workflowsStore?.loadWorkflowSnapshot({
            workflowName: step.id,
            runId: runId,
          }));
        if (!snapshot) {
          throw new NonRetriableError(`Cannot resume nested workflow run ${step.id}/${runId}: snapshot not found`);
        }
        const retainedReceipt = snapshot.resumeResultReceipt;
        const resumeSource =
          snapshot.status === 'running' && snapshot.resumeCheckpoint
            ? snapshot.resumeCheckpoint.snapshot
            : snapshot.status === 'suspended'
              ? snapshot
              : undefined;
        if (!resumeSource && !retainedReceipt) {
          throw new NonRetriableError(
            `Cannot resume nested workflow run ${step.id}/${runId}: workflow run is not suspended`,
          );
        }
        const resumeCapabilities = workflowsStore?.getWorkflowResumeCapabilities();
        if (
          !workflowsStore ||
          resumeCapabilities?.atomicResumeVersion !== 1 ||
          resumeCapabilities.fencedStepUpdateVersion !== 1
        ) {
          throw new NonRetriableError(
            `Workflow storage for nested run ${step.id}/${runId} does not support atomic resume admission and fenced step updates`,
          );
        }
        const resourceId = snapshot.resourceId ?? resumeSource?.resourceId;
        const outputOptions = { includeState: true, includeResumeLabels: true };
        const requestedNestedResumeSteps = explicitNestedResume ? resume.steps.slice(1) : [...resume.steps];
        const parentExecution = {
          workflowId: executionContext.workflowId,
          runId: executionContext.runId,
          executionGeneration: executionContext.executionGeneration,
          lifecycleResumeAttempt: executionContext.lifecycleResumeAttempt,
        };
        const operationHashFor = (steps: string[], resumePath?: number[]) =>
          inngestWorkflowResumeOperationHash({
            workflowId: step.id,
            runId,
            parentExecution,
            resourceId,
            inputData,
            steps,
            resumePayload: resume.resumePayload,
            resumePath,
            requestContext: forwardedRequestContext,
            outputOptions,
            tracingOptions: nestedTracingContext,
            perStep: perStep ?? false,
          });

        let exactReceiptReplay = false;
        if (retainedReceipt) {
          const replaySteps =
            requestedNestedResumeSteps.length > 0
              ? requestedNestedResumeSteps
              : retainedReceipt.operationReplayContext.steps;
          const replayOperationHash = operationHashFor(replaySteps, retainedReceipt.operationReplayContext.resumePath);
          const replayReceiptKey = `miwr:v1:${replayOperationHash.slice('sha256:'.length)}`;
          exactReceiptReplay =
            retainedReceipt.runId === runId &&
            retainedReceipt.receiptKey === replayReceiptKey &&
            retainedReceipt.resumeOperationHash === replayOperationHash;
          if (exactReceiptReplay) {
            result = retainedReceipt.result as WorkflowResult<any, any, any, any>;
            executionContext.state = retainedReceipt.result.state;
          } else if (!resumeSource) {
            throw new NonRetriableError(
              `Cannot resume nested workflow run ${step.id}/${runId}: terminal result belongs to a different resume operation`,
            );
          }
        }

        if (!exactReceiptReplay) {
          const nestedResumeSteps = [...requestedNestedResumeSteps];
          if (nestedResumeSteps.length === 0) {
            const suspendedStepIds = Object.keys(resumeSource!.suspendedPaths ?? {});
            if (suspendedStepIds.length !== 1) {
              throw new NonRetriableError(
                `Cannot infer nested resume step for ${step.id}/${runId}: expected exactly one suspended step`,
              );
            }
            nestedResumeSteps.push(suspendedStepIds[0]!);
          }
          const lifecycleExecution = {
            executionGeneration: requireWorkflowExecutionGeneration(
              resumeSource!.executionGeneration,
              `Nested Inngest workflow resume ${step.id}/${runId}`,
            ),
            lifecycleResumeAttempt: (resumeSource!.lifecycleResumeAttempt ?? 0) + 1,
            lifecycleStepStates: resumeSource!.lifecycleStepStates ?? {},
          };
          const resumePath = resumeSource!.suspendedPaths?.[nestedResumeSteps[0]!] as number[] | undefined;
          const resumeOperationHash = operationHashFor(nestedResumeSteps, resumePath);
          const receiptKey = `miwr:v1:${resumeOperationHash.slice('sha256:'.length)}`;
          const admission = await workflowsStore.admitWorkflowResume({
            workflowName: step.id,
            runId,
            resourceId,
            resumeOperationHash,
            operationReplayContext: {
              version: 1,
              steps: nestedResumeSteps,
              ...(resumePath === undefined ? {} : { resumePath }),
            },
            executionGeneration: lifecycleExecution.executionGeneration,
            lifecycleResumeAttempt: resumeSource!.lifecycleResumeAttempt ?? 0,
            lifecycleStepStates: resumeSource!.lifecycleStepStates ?? {},
            nextLifecycleResumeAttempt: lifecycleExecution.lifecycleResumeAttempt,
            requestContext: forwardedRequestContext,
          });
          if (admission.status !== 'admitted' && admission.status !== 'already_admitted') {
            throw new NonRetriableError(
              `Cannot resume nested workflow run ${step.id}/${runId}: atomic admission failed with ${admission.status}`,
            );
          }

          let invokeResp: any;
          try {
            invokeResp = await this.inngestStep.invoke(`workflow.${executionContext.workflowId}.step.${step.id}`, {
              function: step.getFunction(),
              data: {
                inputData,
                resourceId,
                requestContext: forwardedRequestContext,
                runId: runId,
                resume: {
                  runId: runId,
                  steps: nestedResumeSteps,
                  resumePayload: resume.resumePayload,
                  resumePath,
                },
                receiptKey,
                resumeOperationHash,
                parentExecution,
                outputOptions,
                perStep,
                tracingOptions: nestedTracingContext,
                actor,
                ...lifecycleExecution,
              },
            });
          } catch (invokeError) {
            // The child lifecycle tuple is admitted immediately before invoke.
            if (invokeError instanceof StepError) {
              const authoritativeChild = await workflowsStore.loadWorkflowSnapshot({
                workflowName: step.id,
                runId,
              });
              const terminalReceipt = authoritativeChild?.resumeResultReceipt;
              const hasAuthoritativeTerminalResult =
                terminalReceipt?.runId === runId &&
                terminalReceipt?.receiptKey === receiptKey &&
                terminalReceipt.resumeOperationHash === resumeOperationHash &&
                terminalReceipt.executionGeneration === lifecycleExecution.executionGeneration &&
                terminalReceipt.lifecycleResumeAttempt === lifecycleExecution.lifecycleResumeAttempt &&
                RESUMED_CHILD_TERMINAL_STATUSES.has(terminalReceipt.result.status);
              if (!hasAuthoritativeTerminalResult) {
                throw new NonRetriableError(
                  `Nested workflow ${step.id}/${runId} failed without authoritative terminal resume evidence`,
                  { cause: invokeError },
                );
              }
            } else if (admission.status === 'admitted') {
              try {
                const rollback = await workflowsStore.rollbackWorkflowResume({
                  workflowName: step.id,
                  runId,
                  resourceId,
                  resumeOperationHash,
                  ...lifecycleExecution,
                });
                if (rollback.status !== 'rolled_back' && rollback.status !== 'already_rolled_back') {
                  console.error(`Failed to roll back nested workflow resume admission: ${rollback.status}`);
                }
              } catch (rollbackError) {
                console.error('Failed to roll back nested workflow resume admission:', rollbackError);
              }
            }
            throw invokeError;
          }
          result = invokeResp.result;
          runId = invokeResp.runId;
          executionContext.state = invokeResp.result.state;
        }
      } else if (isTimeTravel) {
        const workflowsStoreForTimeTravel = await this.mastra?.getStorage()?.getStore('workflows');
        const snapshot: any = (await workflowsStoreForTimeTravel?.loadWorkflowSnapshot({
          workflowName: step.id,
          runId: executionContext.runId,
        })) ?? { context: {} };
        const timeTravelParams = createTimeTravelExecutionParams({
          steps: timeTravel.steps.slice(1),
          inputData: timeTravel.inputData,
          resumeData: timeTravel.resumeData,
          context: (timeTravel.nestedStepResults?.[step.id] ?? {}) as any,
          nestedStepsContext: (timeTravel.nestedStepResults ?? {}) as any,
          snapshot,
          graph: step.buildExecutionGraph(),
        });
        const invokeResp = (await this.inngestStep.invoke(`workflow.${executionContext.workflowId}.step.${step.id}`, {
          function: step.getFunction(),
          data: {
            timeTravel: timeTravelParams,
            initialState: executionContext.state ?? {},
            requestContext: forwardedRequestContext,
            runId: executionContext.runId,
            outputOptions: { includeState: true, includeResumeLabels: true },
            nestedWorkflowOutputMode: NESTED_WORKFLOW_OUTPUT_MODE.COMPACT,
            perStep,
            tracingOptions: nestedTracingContext,
            actor,
          },
        })) as any;
        result = invokeResp.result;
        runId = invokeResp.runId;
        executionContext.state = invokeResp.result.state;
      } else {
        // Name the child run on its trigger event. `cancelOn` matches a cancel
        // event against `data.runId` on the trigger, so a nested run invoked
        // without one cannot be cancelled by id — and it would take the
        // unnamed-run branch, warning about advice the caller cannot act on.
        const nestedRunId = randomUUID();
        const invokeResp = (await this.inngestStep.invoke(`workflow.${executionContext.workflowId}.step.${step.id}`, {
          function: step.getFunction(),
          data: {
            inputData,
            initialState: executionContext.state ?? {},
            requestContext: forwardedRequestContext,
            runId: nestedRunId,
            outputOptions: { includeState: true, includeResumeLabels: true },
            nestedWorkflowOutputMode: NESTED_WORKFLOW_OUTPUT_MODE.COMPACT,
            perStep,
            tracingOptions: nestedTracingContext,
            actor,
          },
        })) as any;
        result = invokeResp.result;
        runId = invokeResp.runId;
        executionContext.state = invokeResp.result.state;
      }
    } catch (e) {
      // Nested workflow threw an error (likely from finalization step).
      // Compact nested failures carry the workflow result and runId in the cause.
      const errorCause = e && typeof e === 'object' && 'cause' in e ? e.cause : undefined;

      // Try to extract runId from error cause or generate new one
      if (
        errorCause &&
        typeof errorCause === 'object' &&
        typeof (errorCause as { status?: unknown }).status === 'string'
      ) {
        const failedResult = errorCause as WorkflowResult<any, any, any, any> & { runId?: string };
        result = failedResult;
        // Compact nested failures carry the child run identifier beside the
        // workflow result so cancellation and resume stay scoped to it.
        runId = failedResult.runId || randomUUID();
      } else {
        // Fallback: if we can't get the result from error, construct a basic failed result
        runId = runId! || randomUUID();
        result = {
          status: 'failed',
          error: e instanceof Error ? e : new Error(String(e)),
        } as WorkflowResult<any, any, any, any>;
      }
    }

    const res = await this.inngestStep.run(
      `workflow.${executionContext.workflowId}.step.${step.id}.nestedwf-results`,
      async () => {
        if (result.status === 'failed') {
          await pubsub.publish(`workflow.events.v2.${executionContext.runId}`, {
            type: 'watch',
            runId: executionContext.runId,
            data: {
              type: 'workflow-step-result',
              payload: {
                id: step.id,
                status: 'failed',
                error: result?.error,
                payload: prevOutput,
              },
            },
          });

          return { executionContext, result: { status: 'failed', error: result?.error, endedAt: Date.now() } };
        } else if (result.status === 'suspended') {
          const suspendedSteps = Object.entries(result.steps).filter(([_stepName, stepResult]) => {
            const stepRes: StepResult<any, any, any, any> = stepResult as StepResult<any, any, any, any>;
            return stepRes?.status === 'suspended';
          });

          for (const [stepName, stepResult] of suspendedSteps) {
            const suspendPath: string[] = [stepName, ...(stepResult?.suspendPayload?.__workflow_meta?.path ?? [])];
            executionContext.suspendedPaths[step.id] = executionContext.executionPath;

            // Re-register the nested run's resume labels on the parent so the outer snapshot is
            // self-describing about every parked leaf (e.g. `resumeLabels[toolCallId]`). Without
            // this a caller can only target the outer step, and concurrent suspensions inside the
            // nested workflow become impossible to disambiguate. Mirrors `Workflow.execute()` in
            // packages/core/src/workflows/workflow.ts.
            for (const label of Object.keys((result as any)?.resumeLabels ?? {})) {
              executionContext.resumeLabels[label] = { stepId: step.id };
            }

            // Keep the nested workflow metadata (foreachIndex, foreachOutput, resumeLabels) when
            // propagating a suspension to the parent — only runId and path change as we move up.
            // Per-iteration `__streamState` blobs are stripped from the propagated copies: they can
            // be large and resume reads them from the nested run's own snapshot, so the parent only
            // needs the identifying fields.
            const nestedMeta = (stepResult as any)?.suspendPayload?.__workflow_meta ?? {};
            const propagatedForeachOutput = Array.isArray(nestedMeta.foreachOutput)
              ? nestedMeta.foreachOutput.map((entry: any) => {
                  if (entry?.status !== 'suspended' || !entry.suspendPayload) return entry;
                  const { __streamState: _streamState, ...suspendPayload } = entry.suspendPayload;
                  return { ...entry, suspendPayload };
                })
              : undefined;

            await pubsub.publish(`workflow.events.v2.${executionContext.runId}`, {
              type: 'watch',
              runId: executionContext.runId,
              data: {
                type: 'workflow-step-suspended',
                payload: {
                  id: step.id,
                  status: 'suspended',
                },
              },
            });

            return {
              executionContext,
              result: {
                status: 'suspended',
                suspendedAt: Date.now(),
                payload: stepResult.payload,
                suspendPayload: {
                  ...(stepResult as any)?.suspendPayload,
                  __workflow_meta: {
                    ...nestedMeta,
                    ...(propagatedForeachOutput ? { foreachOutput: propagatedForeachOutput } : {}),
                    runId: runId,
                    path: suspendPath,
                  },
                },
              },
            };
          }

          return {
            executionContext,
            result: {
              status: 'suspended',
              suspendedAt: Date.now(),
              payload: {},
            },
          };
        } else if (result.status === 'tripwire') {
          await pubsub.publish(`workflow.events.v2.${executionContext.runId}`, {
            type: 'watch',
            runId: executionContext.runId,
            data: {
              type: 'workflow-step-result',
              payload: {
                id: step.id,
                status: 'tripwire',
                error: result?.tripwire?.reason,
                payload: prevOutput,
              },
            },
          });

          return {
            executionContext,
            result: {
              status: 'tripwire',
              tripwire: result?.tripwire,
              endedAt: Date.now(),
            },
          };
        } else if (perStep || result.status === 'paused') {
          await pubsub.publish(`workflow.events.v2.${executionContext.runId}`, {
            type: 'watch',
            runId: executionContext.runId,
            data: {
              type: 'workflow-step-result',
              payload: {
                id: step.id,
                status: 'paused',
              },
            },
          });

          await pubsub.publish(`workflow.events.v2.${executionContext.runId}`, {
            type: 'watch',
            runId: executionContext.runId,
            data: {
              type: 'workflow-step-finish',
              payload: {
                id: step.id,
                metadata: {},
              },
            },
          });
          return { executionContext, result: { status: 'paused' } };
        }

        await pubsub.publish(`workflow.events.v2.${executionContext.runId}`, {
          type: 'watch',
          runId: executionContext.runId,
          data: {
            type: 'workflow-step-result',
            payload: {
              id: step.id,
              status: 'success',
              output: result?.result,
            },
          },
        });

        await pubsub.publish(`workflow.events.v2.${executionContext.runId}`, {
          type: 'watch',
          runId: executionContext.runId,
          data: {
            type: 'workflow-step-finish',
            payload: {
              id: step.id,
              metadata: {},
            },
          },
        });

        return { executionContext, result: { status: 'success', output: result?.result, endedAt: Date.now() } };
      },
    );

    Object.assign(executionContext, res.executionContext);
    return {
      ...res.result,
      startedAt,
      payload: inputData,
      resumedAt: resume?.steps[0] === step.id ? startedAt : undefined,
      resumePayload: resume?.steps[0] === step.id ? resume?.resumePayload : undefined,
    } as StepResult<any, any, any, any>;
  }
}
