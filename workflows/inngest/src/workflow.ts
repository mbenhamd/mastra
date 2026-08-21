import { randomUUID } from 'node:crypto';
import { AGENT_CONTROL_TOPIC, emitErrorEvent } from '@mastra/core/agent/durable';
import { InMemoryServerCache } from '@mastra/core/cache';
import { RequestContext } from '@mastra/core/di';
import { getErrorFromUnknown } from '@mastra/core/error';
import { CachingPubSub } from '@mastra/core/events';
import type { PubSub } from '@mastra/core/events';
import type { Mastra } from '@mastra/core/mastra';
import { SpanType, EntityType } from '@mastra/core/observability';
import type { WorkflowRuns } from '@mastra/core/storage';
import {
  Workflow,
  createWorkflowExecutionGeneration,
  getEntryWorkflow,
  isSingleStepEntry,
  requireWorkflowExecutionGeneration,
} from '@mastra/core/workflows';
import type {
  Step,
  StepResult,
  WorkflowConfig,
  StepFlowEntry,
  WorkflowResult,
  WorkflowResumeResultDataV1,
  WorkflowRunState,
  WorkflowRunStatus,
  WorkflowStreamEvent,
  Run,
  RunWithRawInput,
} from '@mastra/core/workflows';
import { TRANSIENT_EXECUTION_SYMBOL } from '@mastra/core/workflows/_constants';
import type { PROCESSOR_EXECUTION_SYMBOL } from '@mastra/core/workflows/_constants';
import { NonRetriableError } from 'inngest';
import type { Inngest } from 'inngest';
import { InngestExecutionEngine } from './execution-engine';
import {
  compactNestedWorkflowResult,
  NESTED_WORKFLOW_OUTPUT_MODE,
  resolveNestedWorkflowOutputMode,
} from './nested-workflow-output';
import { InngestPubSub } from './pubsub';
import { inngestWorkflowResumeOperationHash } from './resume-operation';
import { InngestRun } from './run';
import type {
  InngestEngineType,
  InngestFlowControlConfig,
  InngestFlowCronConfig,
  InngestWorkflowConfig,
  InngestWorkflowPubSubFactory,
  InngestWorkflowRunState,
} from './types';

export const INNGEST_WORKFLOW_LIFECYCLE_REPLAY = {
  retentionMs: 24 * 60 * 60 * 1000,
  maxEvents: 10_000,
} as const;

/** Publish terminal failure before clearing retained abort intent. Both operations reject so Inngest retries the checkpoint. */
export async function finalizeDurableAgentFailureTransport(
  pubsub: PubSub,
  input: { runId: string; runtimeBindingId?: string },
  error: Error,
): Promise<void> {
  await emitErrorEvent(pubsub, input.runId, error);
  if (typeof input.runtimeBindingId === 'string' && input.runtimeBindingId.length > 0) {
    await pubsub.clearTopicOrThrow(AGENT_CONTROL_TOPIC(input.runId, input.runtimeBindingId));
  }
}

export function createInngestWorkflowTerminalPayload(result: {
  status: WorkflowRunStatus;
  result?: unknown;
  error?: unknown;
}): Extract<WorkflowStreamEvent, { type: 'workflow-finish' }>['payload'] & {
  status: WorkflowRunStatus;
  result?: unknown;
  error?: unknown;
} {
  const terminalError =
    result.status === 'failed' && result.error !== undefined
      ? getErrorFromUnknown(result.error, { serializeStack: true }).toJSON()
      : undefined;
  const errorMessage = terminalError?.message;

  return {
    workflowStatus: result.status,
    output: {
      usage: {
        inputTokens: 0,
        outputTokens: 0,
        totalTokens: 0,
      },
    },
    metadata: terminalError === undefined ? {} : { error: terminalError, errorMessage },
    status: result.status,
    result: result.status === 'success' ? result.result : undefined,
    error: terminalError,
  };
}

function requireLifecycleEventTuple(params: {
  workflowId: string;
  runId: string;
  executionGeneration: unknown;
  lifecycleResumeAttempt: unknown;
  lifecycleStepStates: unknown;
}) {
  const owner = `Inngest workflow event ${params.workflowId}/${params.runId}`;
  const executionGeneration = requireWorkflowExecutionGeneration(params.executionGeneration, owner);
  if (
    typeof params.lifecycleResumeAttempt !== 'number' ||
    !Number.isSafeInteger(params.lifecycleResumeAttempt) ||
    params.lifecycleResumeAttempt < 0
  ) {
    throw new NonRetriableError(`${owner} requires a non-negative lifecycle resume attempt`);
  }
  if (
    typeof params.lifecycleStepStates !== 'object' ||
    params.lifecycleStepStates === null ||
    Array.isArray(params.lifecycleStepStates)
  ) {
    throw new NonRetriableError(`${owner} requires lifecycle step states`);
  }

  const lifecycleStepStates: Record<string, { stepCallId: string; stepAttempt: number }> = {};
  for (const [coordinate, value] of Object.entries(params.lifecycleStepStates)) {
    if (
      typeof value !== 'object' ||
      value === null ||
      Array.isArray(value) ||
      typeof (value as { stepCallId?: unknown }).stepCallId !== 'string' ||
      (value as { stepCallId: string }).stepCallId.length === 0 ||
      typeof (value as { stepAttempt?: unknown }).stepAttempt !== 'number' ||
      !Number.isSafeInteger((value as { stepAttempt: number }).stepAttempt) ||
      (value as { stepAttempt: number }).stepAttempt < 1
    ) {
      throw new NonRetriableError(`${owner} has invalid lifecycle step state at ${coordinate}`);
    }
    lifecycleStepStates[coordinate] = {
      stepCallId: (value as { stepCallId: string }).stepCallId,
      stepAttempt: (value as { stepAttempt: number }).stepAttempt,
    };
  }

  return {
    executionGeneration,
    lifecycleResumeAttempt: params.lifecycleResumeAttempt,
    lifecycleStepStates,
  };
}

function lifecycleStepStatesEqual(
  left: Record<string, { stepCallId: string; stepAttempt: number }> | undefined,
  right: Record<string, { stepCallId: string; stepAttempt: number }>,
): boolean {
  const leftEntries = Object.entries(left ?? {});
  const rightKeys = Object.keys(right);
  return (
    leftEntries.length === rightKeys.length &&
    leftEntries.every(
      ([coordinate, state]) =>
        right[coordinate]?.stepCallId === state.stepCallId && right[coordinate]?.stepAttempt === state.stepAttempt,
    )
  );
}

/**
 * Resolves the nested `InngestWorkflow` wrapped by a graph entry, if any.
 * Handles both plain single-step entries and `loop` / `foreach` entries whose
 * body is a `SingleStepEntry` wrapper (so `{ type: 'step', step: workflow }`
 * bodies are unwrapped correctly).
 */
function getNestedInngestWorkflow(entry: StepFlowEntry): InngestWorkflow | null {
  let nested: unknown = null;
  if (entry.type === 'loop' || entry.type === 'foreach') {
    nested = getEntryWorkflow(entry.step);
  } else if (isSingleStepEntry(entry)) {
    nested = getEntryWorkflow(entry);
  }
  return nested instanceof InngestWorkflow ? nested : null;
}

export class InngestWorkflow<
  TEngineType = InngestEngineType,
  TSteps extends Step<string, any, any, any, any, any, TEngineType, any>[] = Step<
    string,
    unknown,
    unknown,
    unknown,
    unknown,
    unknown,
    TEngineType,
    unknown
  >[],
  TWorkflowId extends string = string,
  TState = unknown,
  TInput = unknown,
  TOutput = unknown,
  TPrevSchema = TInput,
  TRequestContext extends Record<string, any> | unknown = unknown,
  TRawInput = TInput,
> extends Workflow<TEngineType, TSteps, TWorkflowId, TState, TInput, TOutput, TPrevSchema, TRequestContext, TRawInput> {
  #mastra: Mastra;
  public inngest: Inngest;

  private function: ReturnType<Inngest['createFunction']> | undefined;
  private cronFunction: ReturnType<Inngest['createFunction']> | undefined;
  private readonly flowControlConfig?: InngestFlowControlConfig;
  private readonly cronConfig?: InngestFlowCronConfig<TRawInput, TState>;
  private readonly fallbackLifecycleCache = new InMemoryServerCache();
  /**
   * Optional override that lets a host (e.g. `createInngestAgent`) provide the
   * PubSub instance used by workflow steps. Lifecycle delivery requires exact
   * indexed replay, so factories that return a `CachingPubSub` must configure
   * its `indexedReplay` option.
   */
  #pubsubFactory?: InngestWorkflowPubSubFactory;

  constructor(
    params: InngestWorkflowConfig<
      TWorkflowId,
      TState,
      TInput,
      TOutput,
      TSteps & Step<string, any, any, any, any, any, InngestEngineType, any>[],
      TRequestContext,
      TRawInput
    >,
    inngest: Inngest,
  ) {
    const {
      concurrency,
      rateLimit,
      throttle,
      debounce,
      priority,
      cron,
      inputData,
      initialState,
      schedule,
      pubsubFactory,
      ...workflowParams
    } = params;

    if (schedule !== undefined) {
      throw new TypeError(
        'Inngest workflows do not support the Core schedule option; use cron, inputData, and initialState instead',
      );
    }

    super(workflowParams as WorkflowConfig<TWorkflowId, TState, TInput, TOutput, TSteps, TRequestContext>);

    this.engineType = 'inngest';

    const flowControlEntries = Object.entries({ concurrency, rateLimit, throttle, debounce, priority }).filter(
      ([_, value]) => value !== undefined,
    );

    this.flowControlConfig = flowControlEntries.length > 0 ? Object.fromEntries(flowControlEntries) : undefined;

    this.#mastra = params.mastra!;
    this.inngest = inngest;
    this.#pubsubFactory = pubsubFactory;

    if (cron) {
      this.cronConfig = { cron, inputData, initialState };
    }
  }

  async listWorkflowRuns(args?: {
    fromDate?: Date;
    toDate?: Date;
    perPage?: number | false;
    page?: number;
    resourceId?: string;
  }) {
    const storage = this.#mastra?.getStorage();
    if (!storage) {
      this.logger.debug('Cannot get workflow runs. Mastra engine is not initialized');
      return { runs: [], total: 0 };
    }

    const workflowsStore = await storage.getStore('workflows');
    if (!workflowsStore) {
      return { runs: [], total: 0 };
    }
    return workflowsStore.listWorkflowRuns({ workflowName: this.id, ...(args ?? {}) }) as unknown as WorkflowRuns;
  }

  /**
   * Override the PubSub used inside the durable workflow function. Callers like
   * `createInngestAgent` use this to route workflow event publishes through the
   * agent's `CachingPubSub`, so `observe()` can replay cached history.
   *
   * The factory receives the workflow's own default `InngestPubSub` (constructed
   * with this workflow's id) as input. Hosts should wrap that instance rather
   * than substitute it, so workflow-event channels (which encode the workflow
   * id) remain workflow-local. Returning a `CachingPubSub` wrapping the default
   * is the canonical pattern.
   *
   * The factory is propagated to every nested `InngestWorkflow` in the step
   * graph. Nested workflows run as their own Inngest functions and resolve
   * their own pubsub at runtime; each invocation passes its own workflow-local
   * default into the same factory, so the host can share cross-workflow state
   * (e.g. a single agent-scoped cache) without collapsing per-workflow channel
   * isolation.
   */
  __setPubsubFactory(factory: InngestWorkflowPubSubFactory) {
    this.#pubsubFactory = factory;
    const updateNested = (step: StepFlowEntry) => {
      const nested = getNestedInngestWorkflow(step);
      if (nested) {
        nested.__setPubsubFactory(factory);
      } else if (step.type === 'parallel' || step.type === 'conditional') {
        for (const subStep of step.steps) {
          updateNested(subStep);
        }
      }
    };
    for (const step of this.executionGraph.steps) {
      updateNested(step);
    }
  }

  /**
   * Test-only accessor for the configured pubsub factory. Lets tests verify that
   * a host (e.g. `createInngestAgent`) wired the workflow to its agent pubsub
   * without having to drive a real Inngest invocation.
   */
  __getPubsubFactory(): InngestWorkflowPubSubFactory | undefined {
    return this.#pubsubFactory;
  }

  private lifecyclePubsub(defaultPubsub: PubSub): PubSub {
    const candidate = this.#pubsubFactory?.(defaultPubsub) ?? defaultPubsub;
    let replayable: PubSub;
    if (candidate.indexedReplay) {
      replayable = candidate;
    } else if (candidate instanceof CachingPubSub) {
      throw new TypeError(
        'Inngest workflow lifecycle events require CachingPubSub indexedReplay; configure indexedReplay on the existing wrapper',
      );
    } else {
      replayable = new CachingPubSub(candidate, this.#mastra?.serverCache ?? this.fallbackLifecycleCache, {
        indexedReplay: INNGEST_WORKFLOW_LIFECYCLE_REPLAY,
      });
    }

    return replayable;
  }

  __registerMastra(mastra: Mastra) {
    super.__registerMastra(mastra);
    this.#mastra = mastra;
    this.executionEngine.__registerMastra(mastra);
    const updateNested = (step: StepFlowEntry) => {
      const nested = getNestedInngestWorkflow(step);
      if (nested) {
        nested.__registerMastra(mastra);
      } else if (step.type === 'parallel' || step.type === 'conditional') {
        for (const subStep of step.steps) {
          updateNested(subStep);
        }
      }
    };

    if (this.executionGraph.steps.length) {
      for (const step of this.executionGraph.steps) {
        updateNested(step);
      }
    }
  }

  async createRun(options?: {
    runId?: string;
    resourceId?: string;
    disableScorers?: boolean;
    /** Inngest functions execute remotely, so per-run PubSub objects cannot be reconstructed. */
    pubsub?: PubSub;
    /** @internal Accepted for substitutability; Inngest processor workflows remain durable. */
    [PROCESSOR_EXECUTION_SYMBOL]?: boolean;
    /** @internal Inngest cannot inherit process-local execution from a parent workflow. */
    [TRANSIENT_EXECUTION_SYMBOL]?: boolean;
  }): Promise<RunWithRawInput<TEngineType, TSteps, TState, TInput, TOutput, TRequestContext, TRawInput>> {
    if (options?.[TRANSIENT_EXECUTION_SYMBOL] === true) {
      throw new TypeError('Inngest workflows cannot run inside transient workflows');
    }
    if (options?.pubsub) {
      throw new TypeError(
        'Inngest createRun({ pubsub }) is unsupported because remote function replicas cannot reconstruct a per-run PubSub object; set pubsubFactory on the workflow instead',
      );
    }
    const runIdToUse = options?.runId || randomUUID();
    const workflowsStore = await this.mastra?.getStorage()?.getStore('workflows');
    const existingSnapshot = (await workflowsStore?.loadWorkflowSnapshot({
      workflowName: this.id,
      runId: runIdToUse,
    })) as InngestWorkflowRunState | null | undefined;
    const resourceId = options?.resourceId ?? existingSnapshot?.resourceId;
    const persistedDisableScorers = existingSnapshot?.runOptions?.disableScorers;
    const defaultPubsub = new InngestPubSub(this.inngest, this.id);
    const pubsub = this.lifecyclePubsub(defaultPubsub);

    // Return a new Run instance with object parameters
    const existingInMemoryRun = this.runs.get(runIdToUse);
    const newRun = new InngestRun<TEngineType, TSteps, TState, TInput, TOutput, TRequestContext>(
      {
        workflowId: this.id,
        runId: runIdToUse,
        resourceId,
        executionEngine: this.executionEngine,
        executionGraph: this.executionGraph,
        serializedStepGraph: this.serializedStepGraph,
        mastra: this.#mastra,
        retryConfig: this.retryConfig,
        cleanup: () => this.runs.delete(runIdToUse),
        workflowSteps: this.steps,
        workflowEngineType: this.engineType,
        validateInputs: this.options.validateInputs,
        inputSchema: this.inputSchema,
        stateSchema: this.stateSchema,
        requestContextSchema: this.requestContextSchema,
        disableScorers: persistedDisableScorers ?? options?.disableScorers,
        pubsub,
      },
      this.inngest,
    );
    const run = (existingInMemoryRun ?? newRun) as Run<TEngineType, TSteps, TState, TInput, TOutput, TRequestContext>;

    this.runs.set(runIdToUse, run);

    const shouldPersistSnapshot = this.options.shouldPersistSnapshot({
      workflowStatus: run.workflowRunStatus,
      stepResults: {},
    });

    const existsInStorage = Boolean(existingSnapshot);

    if (existingSnapshot?.status) {
      run.workflowRunStatus = existingSnapshot.status as WorkflowRunStatus;
    }

    if (!existsInStorage && shouldPersistSnapshot) {
      const lifecycleExecution = {
        executionGeneration: createWorkflowExecutionGeneration(),
        lifecycleResumeAttempt: 0,
        lifecycleStepStates: {},
      };
      await workflowsStore?.persistWorkflowSnapshot({
        workflowName: this.id,
        runId: runIdToUse,
        resourceId,
        snapshot: {
          runId: runIdToUse,
          resourceId,
          ...lifecycleExecution,
          status: 'pending',
          value: {},
          context: {},
          activePaths: [],
          activeStepsPath: {},
          waitingPaths: {},
          serializedStepGraph: this.serializedStepGraph,
          suspendedPaths: {},
          resumeLabels: {},
          result: undefined,
          error: undefined,
          timestamp: Date.now(),
          ...(run.disableScorers !== undefined
            ? {
                runOptions: {
                  disableScorers: run.disableScorers,
                },
              }
            : {}),
        } as InngestWorkflowRunState,
      });
    }

    if (
      workflowsStore &&
      ((!existsInStorage && shouldPersistSnapshot) || (existsInStorage && existingSnapshot?.status === 'pending'))
    ) {
      await run.getLifecycleExecutionIdentity();
    }

    return run as RunWithRawInput<TEngineType, TSteps, TState, TInput, TOutput, TRequestContext, TRawInput>;
  }

  //createCronFunction is only called if cronConfig.cron is defined.
  private createCronFunction() {
    if (this.cronFunction) {
      return this.cronFunction;
    }
    this.cronFunction = this.inngest.createFunction(
      {
        id: `workflow.${this.id}.cron`,
        retries: 0,
        triggers: { cron: this.cronConfig?.cron ?? '' },
        ...this.flowControlConfig,
      },
      async () => {
        const run = await this.createRun();
        // @ts-expect-error - cron inputData type mismatch
        const result = await run.start({
          inputData: this.cronConfig?.inputData,
          initialState: this.cronConfig?.initialState,
        });
        return { result, runId: run.runId };
      },
    );
    return this.cronFunction;
  }

  /**
   * Gets the durable Inngest function that executes this workflow.
   *
   * @returns The memoized Inngest function for this workflow.
   */
  getFunction(): ReturnType<Inngest['createFunction']> {
    if (this.function) {
      return this.function;
    }

    // Always set function-level retries to 0, since retries are handled at the step level via executeStepWithRetry
    // which uses either step.retries or retryConfig.attempts (step.retries takes precedence).
    // step.retries is not accessible at function level, so we handle retries manually in executeStepWithRetry.
    // This is why we set retries to 0 here.
    this.function = this.inngest.createFunction(
      {
        id: `workflow.${this.id}`,
        retries: 0,
        cancelOn: [
          {
            event: `cancel.workflow.${this.id}`,
            if: 'async.data.runId == event.data.runId && async.data.executionGeneration == event.data.executionGeneration && async.data.lifecycleResumeAttempt == event.data.lifecycleResumeAttempt',
          },
        ],
        triggers: { event: `workflow.${this.id}` },
        // Spread flow control configuration
        ...this.flowControlConfig,
      },
      /**
       * Executes a workflow invocation from its Inngest trigger event.
       *
       * @param context - The Inngest event, durable step tools, and current attempt.
       * @returns The workflow result and run identifier returned to Inngest.
       */
      async ({ event, step, attempt }) => {
        let {
          inputData,
          initialState,
          runId,
          resourceId,
          resume,
          outputOptions,
          format,
          timeTravel,
          perStep,
          tracingOptions,
          actor,
          disableScorers,
          receiptKey,
          resumeOperationHash: suppliedResumeOperationHash,
          parentExecution,
          executionGeneration: suppliedExecutionGeneration,
          lifecycleResumeAttempt: suppliedLifecycleResumeAttempt,
          lifecycleStepStates: suppliedLifecycleStepStates,
          nestedWorkflowOutputMode: requestedNestedWorkflowOutputMode,
        } = event.data;
        const nestedWorkflowOutputMode = resolveNestedWorkflowOutputMode(requestedNestedWorkflowOutputMode);
        const shouldCompactNestedWorkflowOutput = nestedWorkflowOutputMode === NESTED_WORKFLOW_OUTPUT_MODE.COMPACT;

        if (!runId) {
          // Reached when a trigger event arrives without a run id — an event sent
          // directly rather than through `createRun()`, which always supplies one.
          // The id generated here never reaches the trigger event that `cancelOn`
          // matches against, so `cancel.workflow.${this.id}` cannot target this
          // run. Warn rather than reject: an unnamed run is still a valid way to
          // start a workflow, it just can't be cancelled by id afterwards.
          runId = await step.run(`workflow.${this.id}.runIdGen`, async () => {
            return randomUUID();
          });
          this.logger.warn?.(
            `Workflow "${this.id}" was triggered without a runId, so run "${runId}" cannot be cancelled by id. ` +
              `Send \`data.runId\` on the trigger event (or start the run with createRun()) to make it cancellable.`,
          );
        }

        const hasSuppliedLifecycleState =
          suppliedExecutionGeneration !== undefined ||
          suppliedLifecycleResumeAttempt !== undefined ||
          suppliedLifecycleStepStates !== undefined;

        // Resolve lifecycle identity inside an Inngest durable step. Run-created
        // events supply the exact lineage reserved before dispatch. Direct or
        // cron starts mint it once here, while legacy/direct resume events
        // recover and advance the persisted lineage exactly once under replay.
        const lifecycleExecution = await step.run(`workflow.${this.id}.lifecycle.execution`, async () => {
          if (hasSuppliedLifecycleState) {
            if (
              suppliedExecutionGeneration === undefined ||
              suppliedLifecycleResumeAttempt === undefined ||
              suppliedLifecycleStepStates === undefined
            ) {
              throw new NonRetriableError(
                `Inngest workflow event ${this.id}/${runId} requires a complete lifecycle execution tuple`,
              );
            }
            const suppliedLifecycleExecution = requireLifecycleEventTuple({
              workflowId: this.id,
              runId,
              executionGeneration: suppliedExecutionGeneration,
              lifecycleResumeAttempt: suppliedLifecycleResumeAttempt,
              lifecycleStepStates: suppliedLifecycleStepStates,
            });
            const workflowsStore = await this.#mastra?.getStorage()?.getStore('workflows');
            if (!workflowsStore) {
              throw new NonRetriableError(`Workflow storage is required to execute run ${runId}`);
            }
            const snapshot = await workflowsStore.loadWorkflowSnapshot({
              workflowName: this.id,
              runId,
            });
            if (!snapshot) {
              throw new NonRetriableError(`Cannot execute run ${runId}: snapshot not found`);
            }
            resourceId = resourceId ?? snapshot.resourceId;
            const resumeOperationHash = resume
              ? inngestWorkflowResumeOperationHash({
                  workflowId: this.id,
                  runId,
                  parentExecution,
                  resourceId,
                  inputData,
                  steps: resume.steps ?? [],
                  resumePayload: resume.resumePayload,
                  resumePath: resume.resumePath,
                  requestContext: event.data.requestContext ?? {},
                  outputOptions,
                  tracingOptions,
                  perStep: perStep ?? false,
                  disableScorers,
                  format,
                })
              : undefined;
            if (
              snapshot.executionGeneration !== suppliedLifecycleExecution.executionGeneration ||
              snapshot.lifecycleResumeAttempt !== suppliedLifecycleExecution.lifecycleResumeAttempt ||
              !lifecycleStepStatesEqual(snapshot.lifecycleStepStates, suppliedLifecycleExecution.lifecycleStepStates)
            ) {
              throw new NonRetriableError(`Cannot execute run ${runId}: lifecycle execution tuple is stale`);
            }
            if (
              resume ? snapshot.status !== 'running' : snapshot.status !== 'pending' && snapshot.status !== 'running'
            ) {
              throw new NonRetriableError(`Cannot execute run ${runId}: lifecycle execution is not admitted`);
            }
            if (
              resume &&
              (suppliedResumeOperationHash !== resumeOperationHash ||
                snapshot.resumeCheckpoint?.resumeOperationHash !== resumeOperationHash)
            ) {
              throw new NonRetriableError(`Cannot execute run ${runId}: resume operation identity is stale`);
            }
            return suppliedLifecycleExecution;
          }

          if (resume) {
            const workflowsStore = await this.#mastra?.getStorage()?.getStore('workflows');
            if (!workflowsStore) {
              throw new NonRetriableError(`Workflow storage is required to resume run ${runId}`);
            }
            const snapshot = await workflowsStore.loadWorkflowSnapshot({
              workflowName: this.id,
              runId,
            });
            if (!snapshot) {
              throw new NonRetriableError(`Cannot resume run ${runId}: snapshot not found`);
            }
            if (snapshot.status !== 'suspended') {
              throw new NonRetriableError(`Cannot resume run ${runId}: workflow run is not suspended`);
            }
            resourceId = resourceId ?? snapshot.resourceId;
            const resumeOperationHash = inngestWorkflowResumeOperationHash({
              workflowId: this.id,
              runId,
              parentExecution,
              resourceId,
              inputData,
              steps: resume.steps ?? [],
              resumePayload: resume.resumePayload,
              resumePath: resume.resumePath,
              requestContext: event.data.requestContext ?? {},
              outputOptions,
              tracingOptions,
              perStep: perStep ?? false,
              disableScorers,
              format,
            });
            if (suppliedResumeOperationHash !== undefined && suppliedResumeOperationHash !== resumeOperationHash) {
              throw new NonRetriableError(`Cannot resume run ${runId}: resume operation identity is stale`);
            }
            const restoredLifecycleExecution = {
              executionGeneration: requireWorkflowExecutionGeneration(
                snapshot.executionGeneration,
                `Inngest workflow resume ${this.id}/${runId}`,
              ),
              lifecycleResumeAttempt: (snapshot.lifecycleResumeAttempt ?? 0) + 1,
              lifecycleStepStates: snapshot.lifecycleStepStates ?? {},
            };
            const resumeCapabilities = workflowsStore.getWorkflowResumeCapabilities();
            if (resumeCapabilities.atomicResumeVersion !== 1 || resumeCapabilities.fencedStepUpdateVersion !== 1) {
              throw new NonRetriableError(
                `Workflow storage for ${this.id}/${runId} does not support atomic resume admission and fenced step updates`,
              );
            }
            const admission = await workflowsStore.admitWorkflowResume({
              workflowName: this.id,
              runId,
              resourceId,
              resumeOperationHash,
              operationReplayContext: {
                version: 1,
                steps: resume.steps ?? [],
                ...(resume.resumePath === undefined ? {} : { resumePath: resume.resumePath }),
                ...(resume.label === undefined ? {} : { label: resume.label }),
              },
              executionGeneration: restoredLifecycleExecution.executionGeneration,
              lifecycleResumeAttempt: snapshot.lifecycleResumeAttempt ?? 0,
              lifecycleStepStates: snapshot.lifecycleStepStates ?? {},
              nextLifecycleResumeAttempt: restoredLifecycleExecution.lifecycleResumeAttempt,
              requestContext: event.data.requestContext ?? snapshot.requestContext,
            });
            if (admission.status !== 'admitted' && admission.status !== 'already_admitted') {
              throw new NonRetriableError(
                `Cannot resume run ${runId}: atomic admission failed with ${admission.status}`,
              );
            }
            return restoredLifecycleExecution;
          }

          const freshLifecycleExecution = {
            executionGeneration: createWorkflowExecutionGeneration(),
            lifecycleResumeAttempt: 0,
            lifecycleStepStates: {},
          };
          const workflowsStore = await this.#mastra?.getStorage()?.getStore('workflows');
          if (workflowsStore) {
            const snapshot = await workflowsStore.loadWorkflowSnapshot({
              workflowName: this.id,
              runId,
            });
            if (snapshot) {
              if (snapshot.status !== 'pending') {
                throw new NonRetriableError(`Cannot start run ${runId}: workflow run is not pending`);
              }
              return requireLifecycleEventTuple({
                workflowId: this.id,
                runId,
                executionGeneration: snapshot.executionGeneration,
                lifecycleResumeAttempt: snapshot.lifecycleResumeAttempt,
                lifecycleStepStates: snapshot.lifecycleStepStates,
              });
            }
            if (
              this.options.shouldPersistSnapshot({
                workflowStatus: 'pending',
                stepResults: {},
              })
            ) {
              await workflowsStore.persistWorkflowSnapshot({
                workflowName: this.id,
                runId,
                resourceId,
                snapshot: {
                  runId,
                  ...freshLifecycleExecution,
                  status: 'pending',
                  value: initialState ?? {},
                  context: inputData === undefined ? {} : { input: inputData },
                  activePaths: [],
                  activeStepsPath: {},
                  waitingPaths: {},
                  serializedStepGraph: this.serializedStepGraph,
                  suspendedPaths: {},
                  resumeLabels: {},
                  result: undefined,
                  error: undefined,
                  timestamp: Date.now(),
                },
              });
            }
          }
          return freshLifecycleExecution;
        });

        // Create InngestPubSub instance. Publishes go through `inngest.realtime.publish()`
        // (Inngest SDK v4 client API), which auto-includes the current runId from the
        // function's async context.
        //
        // The default is constructed with `this.id` so workflow-event channels stay
        // workflow-local (InngestPubSub encodes the workflowId in `workflow:<id>:<runId>`).
        // Hosts (e.g. `createInngestAgent`) can override via `__setPubsubFactory` to
        // wrap this default - typically with a `CachingPubSub` so `observe()` can replay
        // cached history - without disturbing per-workflow channel isolation.
        const defaultPubsub = new InngestPubSub(this.inngest, this.id);
        const pubsub = this.lifecyclePubsub(defaultPubsub);

        // Create requestContext before execute so we can reuse it in finalize
        const requestContext: RequestContext = new RequestContext(Object.entries(event.data.requestContext ?? {}));

        // Store mastra reference for use in proxy closure
        const mastra = this.#mastra;
        const tracingPolicy = this.options.tracingPolicy;

        // Create the workflow root span durably - exports SPAN_STARTED immediately on first execution
        // On replay, returns memoized ExportedSpan data without re-creating the span
        const workflowSpanData = await step.run(`workflow.${this.id}.span.start`, async () => {
          const observability = mastra?.observability?.getSelectedInstance({ requestContext });
          if (!observability) return undefined;

          const span = observability.startSpan({
            type: SpanType.WORKFLOW_RUN,
            name: `workflow run: '${this.id}'`,
            entityType: EntityType.WORKFLOW_RUN,
            entityId: this.id,
            entityName: this.id,
            input: inputData,
            metadata: {
              resourceId,
              runId,
            },
            tracingPolicy,
            tracingOptions,
            requestContext,
          });

          return span?.exportSpan();
        });

        const engine = new InngestExecutionEngine(this.#mastra, step, attempt, this.options);

        // `step.run` memoizes lifecycle resolution across retries. Re-read the
        // authoritative snapshot outside that memoized step immediately before
        // execution so a stale retry cannot execute after another generation
        // has taken over the same run id.
        const workflowsStore = await this.#mastra?.getStorage()?.getStore('workflows');
        const authoritativeSnapshot = await workflowsStore?.loadWorkflowSnapshot({
          workflowName: this.id,
          runId,
        });
        if (hasSuppliedLifecycleState && !authoritativeSnapshot) {
          throw new NonRetriableError(`Cannot execute run ${runId}: snapshot not found`);
        }
        resourceId = resourceId ?? authoritativeSnapshot?.resourceId;
        const resumeOperationHash = resume
          ? inngestWorkflowResumeOperationHash({
              workflowId: this.id,
              runId,
              parentExecution,
              resourceId,
              inputData,
              steps: resume.steps ?? [],
              resumePayload: resume.resumePayload,
              resumePath: resume.resumePath,
              requestContext: event.data.requestContext ?? {},
              outputOptions,
              tracingOptions,
              perStep: perStep ?? false,
              disableScorers,
              format,
            })
          : undefined;
        if (
          resume &&
          suppliedResumeOperationHash !== undefined &&
          suppliedResumeOperationHash !== resumeOperationHash
        ) {
          throw new NonRetriableError(`Cannot execute run ${runId}: resume operation identity is stale`);
        }
        const replayReceipt = authoritativeSnapshot?.resumeResultReceipt;
        if (
          resume &&
          replayReceipt &&
          replayReceipt.resumeOperationHash === resumeOperationHash &&
          replayReceipt.executionGeneration === lifecycleExecution.executionGeneration &&
          replayReceipt.lifecycleResumeAttempt === lifecycleExecution.lifecycleResumeAttempt
        ) {
          const replayResult = replayReceipt.result as unknown as WorkflowResult<TState, TInput, TOutput, TSteps>;
          if (replayResult.status === 'failed') {
            throw new NonRetriableError(`Workflow failed`, { cause: replayResult });
          }
          return { result: replayResult, runId };
        }
        if (resume && authoritativeSnapshot?.resumeCheckpoint?.resumeOperationHash !== resumeOperationHash) {
          throw new NonRetriableError(`Cannot execute run ${runId}: resume operation identity is stale`);
        }
        if (
          authoritativeSnapshot &&
          (authoritativeSnapshot.executionGeneration !== lifecycleExecution.executionGeneration ||
            authoritativeSnapshot.lifecycleResumeAttempt !== lifecycleExecution.lifecycleResumeAttempt)
        ) {
          throw new NonRetriableError(`Cannot execute run ${runId}: lifecycle execution tuple is stale`);
        }
        if (authoritativeSnapshot?.lifecycleStepStates) {
          lifecycleExecution.lifecycleStepStates = Object.fromEntries(
            Object.entries(authoritativeSnapshot.lifecycleStepStates).map(([coordinate, state]) => [
              coordinate,
              { ...state },
            ]),
          );
        }
        const authoritativeResumeSource = resume
          ? (authoritativeSnapshot?.resumeCheckpoint?.snapshot ?? authoritativeSnapshot)
          : undefined;
        const executionResume = resume
          ? {
              ...resume,
              stepResults: authoritativeResumeSource?.context ?? {},
              stepExecutionPath: authoritativeResumeSource?.stepExecutionPath,
            }
          : undefined;
        const executionInitialState = resume ? (authoritativeResumeSource?.value ?? initialState) : initialState;

        let result: WorkflowResult<TState, TInput, TOutput, TSteps>;
        try {
          result = await engine.execute<TState, TInput, WorkflowResult<TState, TInput, TOutput, TSteps>>({
            workflowId: this.id,
            runId,
            ...lifecycleExecution,
            resumeOperationHash,
            resourceId,
            disableScorers,
            graph: this.executionGraph,
            serializedStepGraph: this.serializedStepGraph,
            input: inputData,
            initialState: executionInitialState,
            pubsub,
            retryConfig: this.retryConfig,
            requestContext,
            actor,
            resume: executionResume,
            timeTravel,
            perStep,
            format,
            abortController: new AbortController(),
            // For Inngest, we don't pass workflowSpan - step spans use tracingIds instead
            workflowSpan: undefined,
            // Pass tracing IDs for durable span operations
            tracingIds: workflowSpanData
              ? {
                  traceId: workflowSpanData.traceId,
                  workflowSpanId: workflowSpanData.id,
                }
              : undefined,
            outputOptions,
            outputWriter: async (chunk: WorkflowStreamEvent) => {
              try {
                await pubsub.publish(`workflow.events.v2.${runId}`, {
                  type: 'watch',
                  runId,
                  data: chunk,
                });
              } catch (err) {
                this.logger.debug?.('Failed to publish watch event:', err);
              }
            },
          });
        } catch (executionError) {
          // Execution threw an exception (not just returned failed status)
          // Create a failed result to pass to finalize
          result = {
            status: 'failed',
            steps: {},
            state: executionInitialState ?? {},
            error: executionError instanceof Error ? executionError : new Error(String(executionError)),
          } as WorkflowResult<TState, TInput, TOutput, TSteps>;
        }

        const returnedResult = shouldCompactNestedWorkflowOutput ? compactNestedWorkflowResult(result) : result;

        // Final step to invoke lifecycle callbacks and end workflow span.
        // This step is memoized by step.run.
        let finalizeError: unknown;
        let finalizeErrored = false;
        try {
          /**
           * Finalizes workflow lifecycle reporting in a memoized Inngest step.
           *
           * @returns The workflow result, or only its status for compact nested invocations.
           */
          const finalizeWorkflow = async () => {
            // For durable agent workflows, emit error event on failure so the
            // client's stream can receive the error and close properly.
            if (result.status === 'failed' && inputData?.__workflowKind === 'durable-agent' && inputData?.runId) {
              const error = result.error instanceof Error ? result.error : new Error(String(result.error));
              await finalizeDurableAgentFailureTransport(pubsub, inputData, error);
            }

            if (result.status !== 'paused') {
              // Invoke lifecycle callbacks (onFinish and onError)
              await engine.invokeLifecycleCallbacksInternal({
                status: result.status,
                result: 'result' in result ? result.result : undefined,
                error: 'error' in result ? result.error : undefined,
                steps: result.steps,
                tripwire: 'tripwire' in result ? result.tripwire : undefined,
                runId,
                workflowId: this.id,
                resourceId,
                input: inputData,
                requestContext,
                state: result.state ?? executionInitialState ?? {},
              });
            }

            // End the workflow span with appropriate status
            // The workflow span was already created and SPAN_STARTED was exported in the span.start step
            if (workflowSpanData) {
              const observability = mastra?.observability?.getSelectedInstance({ requestContext });
              if (observability) {
                // Rebuild the span from cached data to call end/error
                const workflowSpan = observability.rebuildSpan(workflowSpanData);

                if (result.status === 'failed') {
                  workflowSpan.error({
                    error: result.error instanceof Error ? result.error : new Error(String(result.error)),
                    attributes: { status: 'failed' },
                  });
                } else {
                  workflowSpan.end({
                    output: result.status === 'success' ? result.result : undefined,
                    attributes: { status: result.status },
                  });
                }
              }
            }

            // Persist/finalize before publishing workflow-finish so neither the
            // realtime nor polling path can observe a stale suspended result.
            const shouldPersistFinalSnapshot = this.options.shouldPersistSnapshot({
              workflowStatus: result.status,
              stepResults: result.steps,
            });
            const workflowsStore = await mastra?.getStorage()?.getStore('workflows');
            const existingSnapshot = (await workflowsStore?.loadWorkflowSnapshot({
              workflowName: this.id,
              runId,
            })) as InngestWorkflowRunState | undefined;
            const serializedError =
              result.status === 'failed'
                ? getErrorFromUnknown(result.error, { serializeStack: true }).toJSON()
                : undefined;
            const snapshotContext = toSnapshotContext(result.steps);
            const finalLifecycleExecution = {
              ...lifecycleExecution,
              lifecycleStepStates: existingSnapshot?.lifecycleStepStates ?? lifecycleExecution.lifecycleStepStates,
            };
            const finalSnapshot: InngestWorkflowRunState = {
              ...existingSnapshot,
              runId,
              resourceId: existingSnapshot?.resourceId ?? resourceId,
              requestContext: existingSnapshot?.requestContext ?? Object.fromEntries(requestContext.entries()),
              status: result.status,
              value: result.state ?? executionInitialState ?? {},
              context: snapshotContext,
              activePaths: [],
              activeStepsPath: {},
              serializedStepGraph: this.serializedStepGraph,
              suspendedPaths: existingSnapshot?.suspendedPaths ?? {},
              waitingPaths: {},
              resumeLabels: result.resumeLabels ?? existingSnapshot?.resumeLabels ?? {},
              result: result.status === 'success' ? toSnapshotResult(result.result) : undefined,
              error: serializedError,
              // Persist the durable tracing anchor so a later resume can rebuild the
              // workflow span lineage (upstream #21566) without a live span handle.
              tracingContext: workflowSpanData
                ? {
                    traceId: workflowSpanData.traceId,
                    spanId: workflowSpanData.id,
                  }
                : existingSnapshot?.tracingContext,
              timestamp: Date.now(),
              ...finalLifecycleExecution,
              ...(disableScorers !== undefined
                ? {
                    runOptions: {
                      ...existingSnapshot?.runOptions,
                      disableScorers,
                    },
                  }
                : {}),
            };
            const workflowResult: WorkflowResumeResultDataV1 = {
              status: result.status,
              input: inputData,
              steps: snapshotContext,
              state: result.state ?? executionInitialState ?? {},
              ...(result.status === 'success' ? { result: result.result } : {}),
              ...(serializedError ? { error: serializedError } : {}),
              ...('tripwire' in result && result.tripwire ? { tripwire: result.tripwire } : {}),
              ...('stepExecutionPath' in result && result.stepExecutionPath
                ? { stepExecutionPath: result.stepExecutionPath }
                : {}),
              ...(result.resumeLabels ? { resumeLabels: result.resumeLabels } : {}),
            };
            let publishedWorkflowResult = workflowResult;
            const resolvedResumeReceiptKey =
              resume && resumeOperationHash
                ? (receiptKey ?? `miwr:v1:${resumeOperationHash.slice('sha256:'.length)}`)
                : undefined;

            if (resume) {
              const resumeCapabilities = workflowsStore?.getWorkflowResumeCapabilities();
              if (
                !workflowsStore ||
                resumeCapabilities?.atomicResumeVersion !== 1 ||
                resumeCapabilities.fencedStepUpdateVersion !== 1
              ) {
                throw new NonRetriableError(`Workflow storage is required to finalize resumed run ${runId}`);
              }
              if (!resolvedResumeReceiptKey || !resumeOperationHash) {
                throw new NonRetriableError(`Cannot finalize resumed run ${runId}: receipt identity is missing`);
              }
              const finalization = await workflowsStore.finalizeWorkflowResume({
                workflowName: this.id,
                runId,
                resourceId: existingSnapshot?.resourceId ?? resourceId,
                resumeOperationHash,
                ...finalLifecycleExecution,
                shouldPersistSnapshot: shouldPersistFinalSnapshot,
                snapshot: finalSnapshot,
                receiptKey: resolvedResumeReceiptKey,
                result: workflowResult,
              });
              if (finalization.status !== 'finalized' && finalization.status !== 'already_finalized') {
                throw new NonRetriableError(
                  `Cannot finalize resumed run ${runId}: atomic finalization failed with ${finalization.status}`,
                );
              }
              if (finalization.receipt) publishedWorkflowResult = finalization.receipt.result;
            } else if (shouldPersistFinalSnapshot && workflowsStore) {
              await workflowsStore.persistWorkflowSnapshot({
                workflowName: this.id,
                runId,
                resourceId,
                snapshot: finalSnapshot,
              });
            }

            // Publish workflow-finish event for realtime subscribers (best-effort)
            try {
              await pubsub.publish(`workflow.events.v2.${runId}`, {
                type: 'watch',
                runId,
                data: {
                  type: 'workflow-finish',
                  payload: {
                    ...createInngestWorkflowTerminalPayload(result),
                    ...(resume
                      ? {
                          workflowResult: publishedWorkflowResult,
                          receiptKey: resolvedResumeReceiptKey,
                          resumeOperationHash,
                          executionGeneration: finalLifecycleExecution.executionGeneration,
                          lifecycleResumeAttempt: finalLifecycleExecution.lifecycleResumeAttempt,
                        }
                      : {}),
                  },
                },
              });
            } catch (publishError) {
              this.logger.debug?.('Failed to publish workflow-finish event:', publishError);
            }

            if (
              result.status !== 'failed' &&
              publishedWorkflowResult.status === 'failed' &&
              publishedWorkflowResult.error?.name === 'WorkflowResumeResultTooLargeError'
            ) {
              throw new NonRetriableError(`Workflow resume result exceeded the durable receipt limit`, {
                cause: publishedWorkflowResult,
              });
            }

            // Throw after span ended for failed workflows
            if (result.status === 'failed') {
              throw new NonRetriableError(`Workflow failed`, {
                cause: shouldCompactNestedWorkflowOutput ? { ...returnedResult, runId } : result,
              });
            }

            return shouldCompactNestedWorkflowOutput ? { status: result.status } : result;
          };
          await step.run(`workflow.${this.id}.finalize`, finalizeWorkflow);
        } catch (error) {
          finalizeErrored = true;
          finalizeError = error;
        } finally {
          // Keep this outside step.run memoization, but guaranteed on all paths.
          const observability = mastra?.observability?.getSelectedInstance({ requestContext });
          if (observability) {
            try {
              await observability.flush();
            } catch (flushError) {
              this.logger.debug?.('Failed to flush observability:', flushError);
            }
          }
        }

        if (finalizeErrored) {
          throw finalizeError;
        }

        return { result: returnedResult, runId };
      },
    );
    return this.function;
  }

  getNestedFunctions(steps: StepFlowEntry[]): ReturnType<Inngest['createFunction']>[] {
    return steps.flatMap(step => {
      const nested = getNestedInngestWorkflow(step);
      if (nested) {
        return [nested.getFunction(), ...nested.getNestedFunctions(nested.executionGraph.steps)];
      }
      if (step.type === 'parallel' || step.type === 'conditional') {
        return this.getNestedFunctions(step.steps);
      }

      return [];
    });
  }

  getFunctions(): ReturnType<Inngest['createFunction']>[] {
    return [
      this.getFunction(),
      ...(this.cronConfig?.cron ? [this.createCronFunction()] : []),
      ...this.getNestedFunctions(this.executionGraph.steps),
    ];
  }
}

/**
 * Converts runtime step results to the serialized context shape expected by WorkflowRunState.
 * StepResult is a structural subset of SerializedStepResult (widening), so no data
 * transformation is needed — this bridges the generic type mismatch at the persistence boundary.
 */
function toSnapshotContext(steps: Record<string, StepResult<any, any, any, any>>): WorkflowRunState['context'] {
  return steps as unknown as WorkflowRunState['context'];
}

/**
 * Converts a workflow output value to the record shape expected by WorkflowRunState.result.
 * Workflow outputs are generic (TOutput) but the snapshot schema stores them as Record<string, any>.
 */
function toSnapshotResult(output: unknown): WorkflowRunState['result'] {
  return output as WorkflowRunState['result'];
}
