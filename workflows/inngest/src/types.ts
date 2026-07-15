import type { PubSub } from '@mastra/core/events';
import type { InferPublicSchemaInput, PublicSchema } from '@mastra/core/schema';
import type {
  CreateWorkflowParams,
  InferSchemaOutput,
  Step,
  WorkflowConfig,
  WorkflowRunState,
} from '@mastra/core/workflows';
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

export type InngestWorkflowPubSubFactory = (defaultPubsub: PubSub) => PubSub;

// Union type for Inngest workflows with flow control
export type InngestWorkflowConfig<
  TWorkflowId extends string,
  TState,
  TInput,
  TOutput,
  TSteps extends Step<string, any, any, any, any, any, InngestEngineType>[],
  TRequestContext extends Record<string, any> | unknown = unknown,
  TRawInput = TInput,
> = Omit<WorkflowConfig<TWorkflowId, TState, TInput, TOutput, TSteps, TRequestContext>, 'inputSchema'> & {
  inputSchema: PublicSchema<TInput, TRawInput>;
} & InngestFlowControlConfig &
  InngestFlowCronConfig<TRawInput, TState> & {
    /**
     * Configure the transport used by both run handles and remote Inngest
     * function replicas. Plain transports are automatically wrapped with exact
     * indexed replay. A returned `CachingPubSub` must already advertise
     * `indexedReplay`; otherwise configuration fails closed instead of adding a
     * second cache wrapper with ambiguous ownership.
     */
    pubsubFactory?: InngestWorkflowPubSubFactory;
  };

/**
 * Inngest-specific durable run options stored alongside the native workflow snapshot.
 * These values must survive worker replacement so later resume/time-travel events keep
 * the same execution semantics as the original run.
 */
export type InngestWorkflowRunState = WorkflowRunState & {
  runOptions?: {
    disableScorers?: boolean;
  };
};

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
  InngestFlowCronConfig<InferPublicSchemaInput<TInputSchema>, InferSchemaOutput<TStateSchema>> & {
    /**
     * Configure the transport used by both run handles and remote Inngest
     * function replicas. Plain transports are automatically wrapped with exact
     * indexed replay. A returned `CachingPubSub` must already advertise
     * `indexedReplay`; otherwise configuration fails closed instead of adding a
     * second cache wrapper with ambiguous ownership.
     */
    pubsubFactory?: InngestWorkflowPubSubFactory;
  };

// Compile-time compatibility assertion
export type _AssertInngestCompatibility =
  InngestFlowControlConfig extends Pick<Parameters<Inngest['createFunction']>[0], keyof InngestFlowControlConfig>
    ? true
    : never;
export const _compatibilityCheck: _AssertInngestCompatibility = true;

export type InngestEngineType = {
  step: any;
};
