import { randomUUID } from 'node:crypto';
import { emitErrorEvent } from '@mastra/core/agent/durable';
import { InMemoryServerCache } from '@mastra/core/cache';
import { RequestContext } from '@mastra/core/di';
import { CachingPubSub } from '@mastra/core/events';
import type { PubSub } from '@mastra/core/events';
import type { Mastra } from '@mastra/core/mastra';
import { SpanType, EntityType } from '@mastra/core/observability';
import type { WorkflowRuns } from '@mastra/core/storage';
import {
  Workflow,
  createWorkflowExecutionGeneration,
  requireWorkflowExecutionGeneration,
} from '@mastra/core/workflows';
import type {
  Step,
  StepResult,
  WorkflowConfig,
  StepFlowEntry,
  WorkflowResult,
  WorkflowRunState,
  WorkflowRunStatus,
  WorkflowStreamEvent,
  Run,
  RunWithRawInput,
} from '@mastra/core/workflows';
import { NonRetriableError } from 'inngest';
import type { Inngest } from 'inngest';
import { InngestExecutionEngine } from './execution-engine';
import { InngestPubSub } from './pubsub';
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

export class InngestWorkflow<
  TEngineType = InngestEngineType,
  TSteps extends Step<string, any, any, any, any, any, TEngineType>[] = Step<
    string,
    unknown,
    unknown,
    unknown,
    unknown,
    unknown,
    TEngineType
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
      TSteps & Step<string, any, any, any, any, any, InngestEngineType>[],
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
      if (
        (step.type === 'step' || step.type === 'loop' || step.type === 'foreach') &&
        step.step instanceof InngestWorkflow
      ) {
        step.step.__setPubsubFactory(factory);
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
      if (
        (step.type === 'step' || step.type === 'loop' || step.type === 'foreach') &&
        step.step instanceof InngestWorkflow
      ) {
        step.step.__registerMastra(mastra);
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
  }): Promise<RunWithRawInput<TEngineType, TSteps, TState, TInput, TOutput, TRequestContext, TRawInput>> {
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
    const persistedDisableScorers = existingSnapshot?.runOptions?.disableScorers;
    const defaultPubsub = new InngestPubSub(this.inngest, this.id);
    const pubsub = this.lifecyclePubsub(defaultPubsub);

    // Return a new Run instance with object parameters
    const existingInMemoryRun = this.runs.get(runIdToUse);
    const newRun = new InngestRun<TEngineType, TSteps, TState, TInput, TOutput, TRequestContext>(
      {
        workflowId: this.id,
        runId: runIdToUse,
        resourceId: options?.resourceId,
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
        resourceId: options?.resourceId,
        snapshot: {
          runId: runIdToUse,
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
        },
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
          executionGeneration: suppliedExecutionGeneration,
          lifecycleResumeAttempt: suppliedLifecycleResumeAttempt,
          lifecycleStepStates: suppliedLifecycleStepStates,
        } = event.data;

        if (!runId) {
          runId = await step.run(`workflow.${this.id}.runIdGen`, async () => {
            return randomUUID();
          });
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
            const restoredLifecycleExecution = {
              executionGeneration: requireWorkflowExecutionGeneration(
                snapshot.executionGeneration,
                `Inngest workflow resume ${this.id}/${runId}`,
              ),
              lifecycleResumeAttempt: (snapshot.lifecycleResumeAttempt ?? 0) + 1,
              lifecycleStepStates: snapshot.lifecycleStepStates ?? {},
            };
            await workflowsStore.updateWorkflowState({
              workflowName: this.id,
              runId,
              opts: {
                status: 'running',
                ...restoredLifecycleExecution,
              },
            });
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

        let result: WorkflowResult<TState, TInput, TOutput, TSteps>;
        try {
          result = await engine.execute<TState, TInput, WorkflowResult<TState, TInput, TOutput, TSteps>>({
            workflowId: this.id,
            runId,
            ...lifecycleExecution,
            resourceId,
            disableScorers,
            graph: this.executionGraph,
            serializedStepGraph: this.serializedStepGraph,
            input: inputData,
            initialState,
            pubsub,
            retryConfig: this.retryConfig,
            requestContext,
            actor,
            resume,
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
            state: initialState ?? {},
            error: executionError instanceof Error ? executionError : new Error(String(executionError)),
          } as WorkflowResult<TState, TInput, TOutput, TSteps>;
        }

        // Final step to invoke lifecycle callbacks and end workflow span.
        // This step is memoized by step.run.
        let finalizeError: unknown;
        let finalizeErrored = false;
        try {
          await step.run(`workflow.${this.id}.finalize`, async () => {
            // For durable agent workflows, emit error event on failure so the
            // client's stream can receive the error and close properly.
            if (result.status === 'failed' && inputData?.__workflowKind === 'durable-agent' && inputData?.runId) {
              const error = result.error instanceof Error ? result.error : new Error(String(result.error));
              try {
                await emitErrorEvent(pubsub, inputData.runId, error);
              } catch (e) {
                this.logger.debug?.('Failed to emit error event:', e);
              }
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
                state: result.state ?? initialState ?? {},
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

            // Ensure final snapshot is persisted BEFORE publishing workflow-finish
            // This fixes a race condition where getRunOutput reads the snapshot before it's fully written
            const shouldPersistFinalSnapshot = this.options.shouldPersistSnapshot({
              workflowStatus: result.status,
              stepResults: result.steps,
            });
            if (shouldPersistFinalSnapshot) {
              const workflowsStore = await mastra?.getStorage()?.getStore('workflows');
              if (workflowsStore) {
                // For suspended workflows, read existing snapshot to preserve suspendedPaths and resumeLabels
                // which were set correctly by the handlers during execution
                let existingSnapshot:
                  | { suspendedPaths?: Record<string, number[]>; resumeLabels?: Record<string, any> }
                  | undefined;
                if (result.status === 'suspended') {
                  existingSnapshot =
                    (await workflowsStore.loadWorkflowSnapshot({
                      workflowName: this.id,
                      runId,
                    })) ?? undefined;
                }

                await workflowsStore.persistWorkflowSnapshot({
                  workflowName: this.id,
                  runId,
                  resourceId,
                  snapshot: {
                    runId,
                    status: result.status,
                    value: result.state ?? initialState ?? {},
                    context: toSnapshotContext(result.steps),
                    activePaths: [],
                    activeStepsPath: {},
                    serializedStepGraph: this.serializedStepGraph,
                    suspendedPaths: existingSnapshot?.suspendedPaths ?? {},
                    waitingPaths: {},
                    resumeLabels: existingSnapshot?.resumeLabels ?? result.resumeLabels ?? {},
                    result: result.status === 'success' ? toSnapshotResult(result.result) : undefined,
                    error: result.status === 'failed' ? result.error : undefined,
                    timestamp: Date.now(),
                    ...lifecycleExecution,
                    ...(disableScorers !== undefined
                      ? {
                          runOptions: {
                            disableScorers,
                          },
                        }
                      : {}),
                  } as InngestWorkflowRunState,
                });
              }
            }

            // Publish workflow-finish event for realtime subscribers (best-effort)
            try {
              await pubsub.publish(`workflow.events.v2.${runId}`, {
                type: 'watch',
                runId,
                data: {
                  type: 'workflow-finish',
                  payload: {
                    status: result.status,
                    result: result.status === 'success' ? result.result : undefined,
                    error: result.status === 'failed' ? result.error : undefined,
                  },
                },
              });
            } catch (publishError) {
              this.logger.debug?.('Failed to publish workflow-finish event:', publishError);
            }

            // Throw after span ended for failed workflows
            if (result.status === 'failed') {
              throw new NonRetriableError(`Workflow failed`, {
                cause: result,
              });
            }

            return result;
          });
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

        return { result, runId };
      },
    );
    return this.function;
  }

  getNestedFunctions(steps: StepFlowEntry[]): ReturnType<Inngest['createFunction']>[] {
    return steps.flatMap(step => {
      if (step.type === 'step' || step.type === 'loop' || step.type === 'foreach') {
        if (step.step instanceof InngestWorkflow) {
          return [step.step.getFunction(), ...step.step.getNestedFunctions(step.step.executionGraph.steps)];
        }
        return [];
      } else if (step.type === 'parallel' || step.type === 'conditional') {
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
