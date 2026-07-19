import type { PubSub } from '@mastra/core/events';
import { Workflow, createStep } from '@mastra/core/workflows';
import type { RunWithRawInput, Step, WorkflowConfig } from '@mastra/core/workflows';
import { TRANSIENT_EXECUTION_SYMBOL } from '@mastra/core/workflows/_constants';
import type { PROCESSOR_EXECUTION_SYMBOL } from '@mastra/core/workflows/_constants';
import type { Client } from '@temporalio/client';
import { TemporalRun } from './run';
import type { TemporalEngineType } from './types';

export type TemporalWorkflowParams = {
  client: Client;
  taskQueue: string;
  startToCloseTimeout?: string;
};

export class TemporalWorkflow<
  TSteps extends Step<string, any, any, any, any, any, TemporalEngineType, any>[] = Step<
    string,
    unknown,
    unknown,
    unknown,
    unknown,
    unknown,
    TemporalEngineType
  >[],
  TWorkflowId extends string = string,
  TState = unknown,
  TInput = unknown,
  TOutput = unknown,
  TPrevSchema = TInput,
  TRequestContext extends Record<string, any> | unknown = unknown,
> extends Workflow<TemporalEngineType, TSteps, TWorkflowId, TState, TInput, TOutput, TPrevSchema, TRequestContext> {
  private readonly temporalClient: Client;
  readonly taskQueue: string;
  readonly startToCloseTimeout: string;

  constructor(
    params: WorkflowConfig<TWorkflowId, TState, TInput, TOutput, TSteps, TRequestContext>,
    temporalParams: TemporalWorkflowParams,
  ) {
    super(params);

    this.engineType = 'temporal';
    this.temporalClient = temporalParams.client;
    this.taskQueue = temporalParams.taskQueue;
    this.startToCloseTimeout = temporalParams.startToCloseTimeout ?? '1 minute';
  }

  async createRun(options?: {
    runId?: string;
    resourceId?: string;
    disableScorers?: boolean;
    pubsub?: PubSub;
    /** @internal Accepted for substitutability; Temporal processor workflows remain durable. */
    [PROCESSOR_EXECUTION_SYMBOL]?: boolean;
    /** @internal Temporal cannot inherit process-local execution from a parent workflow. */
    [TRANSIENT_EXECUTION_SYMBOL]?: boolean;
  }): Promise<RunWithRawInput<TemporalEngineType, TSteps, TState, TInput, TOutput, TRequestContext, TInput>> {
    if (options?.[TRANSIENT_EXECUTION_SYMBOL] === true) {
      throw new TypeError('Temporal workflows cannot run inside transient workflows');
    }
    const runId = options?.runId ?? crypto.randomUUID();
    const run = new TemporalRun<TSteps, TState, TInput, TOutput, TRequestContext>(
      {
        workflowId: this.id,
        runId,
        resourceId: options?.resourceId,
        stateSchema: this.stateSchema,
        inputSchema: this.inputSchema,
        requestContextSchema: this.requestContextSchema,
        executionEngine: this.executionEngine,
        executionGraph: this.executionGraph,
        mastra: this.mastra,
        retryConfig: this.retryConfig,
        serializedStepGraph: this.serializedStepGraph,
        disableScorers: options?.disableScorers,
        tracingPolicy: this.options.tracingPolicy,
        workflowSteps: this.steps,
        validateInputs: this.options.validateInputs,
        workflowEngineType: this.engineType,
        cleanup: undefined,
        pubsub: options?.pubsub,
      },
      {
        client: this.temporalClient,
        taskQueue: this.taskQueue,
      },
    );

    this.runs.set(runId, run);
    return run as RunWithRawInput<TemporalEngineType, TSteps, TState, TInput, TOutput, TRequestContext, TInput>;
  }
}

export function createWorkflow<
  TSteps extends Step<string, any, any, any, any, any, TemporalEngineType, any>[] = Step<
    string,
    unknown,
    unknown,
    unknown,
    unknown,
    unknown,
    TemporalEngineType
  >[],
  TWorkflowId extends string = string,
  TState = unknown,
  TInput = unknown,
  TOutput = unknown,
  TRequestContext extends Record<string, any> | unknown = unknown,
>(
  config: WorkflowConfig<TWorkflowId, TState, TInput, TOutput, TSteps, TRequestContext>,
  temporal: TemporalWorkflowParams,
) {
  return new TemporalWorkflow<TSteps, TWorkflowId, TState, TInput, TOutput, TInput, TRequestContext>(config, temporal);
}

export function init(temporalParams: TemporalWorkflowParams) {
  return {
    createWorkflow: <
      TWorkflowId extends string,
      TState = unknown,
      TInput = unknown,
      TOutput = unknown,
      TSteps extends Step[] = Step[],
      TRequestContext extends Record<string, any> | unknown = unknown,
    >(
      config: WorkflowConfig<TWorkflowId, TState, TInput, TOutput, TSteps, TRequestContext>,
    ) => createWorkflow<TSteps, TWorkflowId, TState, TInput, TOutput, TRequestContext>(config, temporalParams),
    createStep,
  };
}
