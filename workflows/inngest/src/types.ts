import type { PublicSchema } from '@mastra/core/schema';
import type { CreateWorkflowParams, InferSchemaOutput, Step, WorkflowConfig } from '@mastra/core/workflows';
import type { Inngest } from 'inngest';

// Extract Inngest's native flow control configuration types from createFunction first argument
export type InngestCreateFunctionConfig = Parameters<Inngest['createFunction']>[0];

// Extract specific flow control properties (excluding batching)
export type InngestFlowControlConfig = Pick<
  InngestCreateFunctionConfig,
  'concurrency' | 'rateLimit' | 'throttle' | 'debounce' | 'priority'
>;

// Cron config for scheduled workflows
export type InngestFlowCronConfig<TInputData, TInitialState> = {
  cron?: string;
  inputData?: TInputData;
  initialState?: TInitialState;
};

// Union type for Inngest workflows with flow control
export type InngestWorkflowConfig<
  TWorkflowId extends string,
  TState,
  TInput,
  TOutput,
  TSteps extends Step<string, any, any, any, any, any, InngestEngineType>[],
  TRequestContext extends Record<string, any> | unknown = unknown,
> = WorkflowConfig<TWorkflowId, TState, TInput, TOutput, TSteps, TRequestContext> &
  InngestFlowControlConfig &
  InngestFlowCronConfig<TInput, TState>;

/**
 * Schema-typed Inngest workflow configuration.
 */
export type CreateInngestWorkflowParams<
  TWorkflowId extends string = string,
  TStateSchema extends PublicSchema<any> | undefined = undefined,
  TInputSchema extends PublicSchema<any> = PublicSchema<any>,
  TOutputSchema extends PublicSchema<any> = PublicSchema<any>,
  TSteps extends Step[] = Step[],
  TRequestContextSchema extends PublicSchema<any> | undefined = undefined,
> = CreateWorkflowParams<TWorkflowId, TStateSchema, TInputSchema, TOutputSchema, TSteps, TRequestContextSchema> &
  InngestFlowControlConfig &
  InngestFlowCronConfig<InferSchemaOutput<TInputSchema>, InferSchemaOutput<TStateSchema>>;

// Compile-time compatibility assertion
export type _AssertInngestCompatibility =
  InngestFlowControlConfig extends Pick<Parameters<Inngest['createFunction']>[0], keyof InngestFlowControlConfig>
    ? true
    : never;
export const _compatibilityCheck: _AssertInngestCompatibility = true;

export type InngestEngineType = {
  step: any;
};
