import type { ToolSet } from '@internal/ai-sdk-v5';
import { TOOL_PERMISSION_POLICY_KEY } from '../../../agent/tool-permission-prefilter';
import type { ToolPermissionPolicy } from '../../../agent/tool-permission-prefilter';
import { InternalSpans } from '../../../observability';
import { createWorkflow as createDirectWorkflow, createEventedWorkflow } from '../../../workflows/create';
import type { OuterLLMRun } from '../../types';
import { pruneAgentLoopSnapshot } from '../prune-snapshot';
import { llmIterationOutputSchema } from '../schema';
import type { LLMIterationData } from '../schema';
import { createBackgroundTaskCheckStep } from './background-task-check-step';
import { createGoalStep } from './goal-step';
import { createIsTaskCompleteStep } from './is-task-complete-step';
import { createLLMExecutionStep } from './llm-execution-step';
import { createLLMMappingStep } from './llm-mapping-step';
import { createSignalDrainStep } from './signal-drain-step';
import {
  normalizeToolCallConcurrency,
  resolveCalledBatchToolCallConcurrency,
  resolveToolCallConcurrency,
} from './tool-call-concurrency';
import type { ToolCallForeachOptions } from './tool-call-concurrency';
import { createToolCallStep } from './tool-call-step';

export const AGENTIC_EXECUTION_WORKFLOW_ID = 'executionWorkflow';

export function createAgenticExecutionWorkflow<Tools extends ToolSet = ToolSet, OUTPUT = undefined>({
  models,
  _internal,
  ...rest
}: OuterLLMRun<Tools, OUTPUT>) {
  // Upstream shipped the fork's called-batch narrowing (perf: c86ba4ac5af8) as the
  // opt-in `'called'` strategy with an `'available'` default, so the fork's
  // behaviour is now expressed by CONFIGURING the strategy, not by diverging on the
  // default. `normalizeToolCallConcurrency` owns that default (and its test pins it);
  // resolving it here instead of re-deriving keeps the loop, the durable engine
  // (agent/durable/workflows/shared/tool-call-concurrency.ts) and the documented
  // `ToolCallConcurrency` contract in loop/types.ts on one answer.
  // Surfaces that want the fork's parallel batches pass
  // `toolCallConcurrency: { limit, strategy: 'called' }`.
  const { limit: configuredToolCallConcurrency, strategy: toolCallConcurrencyStrategy } = normalizeToolCallConcurrency(
    rest.toolCallConcurrency,
  );
  const permissionPolicy = rest.requestContext?.get(TOOL_PERMISSION_POLICY_KEY) as ToolPermissionPolicy | undefined;
  const toolCallForeachOptions: ToolCallForeachOptions = {
    // This initial value is a conservative fallback for resume paths that can enter
    // a suspended foreach before llm-execution recomputes the effective step tools.
    // Use the 'available' strategy here regardless of the configured strategy: the
    // called tool set is not known yet, and map-tool-calls narrows it before the
    // foreach actually consumes this value.
    concurrency: resolveToolCallConcurrency({
      requireToolApproval: rest.requireToolApproval,
      tools: rest.tools,
      activeTools: rest.activeTools as string[] | undefined,
      permissionPolicy,
      configuredConcurrency: configuredToolCallConcurrency,
    }),
  };

  const llmExecutionStep = createLLMExecutionStep({
    models,
    _internal,
    toolCallForeachOptions,
    ...rest,
  });

  const toolCallStep = createToolCallStep({
    models,
    _internal,
    ...rest,
  });

  const llmMappingStep = createLLMMappingStep(
    {
      models,
      _internal,
      ...rest,
    },
    llmExecutionStep,
  );

  const backgroundTaskCheckStep = createBackgroundTaskCheckStep({
    models,
    _internal,
    ...rest,
  });

  const signalDrainStep = createSignalDrainStep({
    models,
    _internal,
    ...rest,
  });

  const isTaskCompleteStep = createIsTaskCompleteStep({
    models,
    _internal,
    ...rest,
  });

  const goalStep = createGoalStep({
    models,
    _internal,
    ...rest,
  });

  const createWorkflow = process.env.MASTRA_EVENTED_EXECUTION === 'true' ? createEventedWorkflow : createDirectWorkflow;

  return createWorkflow({
    id: AGENTIC_EXECUTION_WORKFLOW_ID,
    inputSchema: llmIterationOutputSchema,
    outputSchema: llmIterationOutputSchema,
    options: {
      tracingPolicy: {
        // mark all workflow spans related to the
        // VNext execution as internal
        internal: InternalSpans.WORKFLOW,
      },
      shouldPersistSnapshot: params => {
        // We need a persisted snapshot record to support `resumeStream()`.
        // - Create the initial record early ("pending")
        // - Update it when execution is suspended ("paused"/"suspended")
        // Avoid persisting "running" snapshots so we don't overwrite an existing suspended snapshot.
        return (
          params.workflowStatus === 'pending' ||
          params.workflowStatus === 'paused' ||
          params.workflowStatus === 'suspended'
        );
      },
      // Agent-loop snapshots are pure resume artifacts — strip everything a
      // resume never reads (stale suspend payloads, duplicated message
      // arrays, AI SDK step history) before persisting.
      pruneSnapshot: pruneAgentLoopSnapshot,
      validateInputs: false,
    },
  })
    .then(llmExecutionStep)
    .map(
      async ({ inputData }) => {
        const typedInputData = inputData as LLMIterationData<Tools, OUTPUT>;
        const toolCalls = typedInputData.output.toolCalls || [];
        // Recompute concurrency now that the model has emitted its tool calls.
        //
        // Default ('available'): resolve from the step's effective active tool
        // set (set by llm-execution-step), NOT from the tools the model actually
        // called. A registered approval/suspending tool that the model did not
        // call this step still forces sequential execution.
        //
        // Opt-in ('called'): resolve from the tools the model actually called
        // this step — see resolveCalledBatchToolCallConcurrency for the safety
        // argument. A pure-safe batch parallelizes even while an approval/suspend
        // tool stays registered; a batch that calls one still serializes; run-wide
        // requireToolApproval still forces sequential.
        const stepTools = ((_internal?.stepTools as Tools | undefined) ?? rest.tools) as Tools | undefined;
        toolCallForeachOptions.concurrency =
          toolCallConcurrencyStrategy === 'called'
            ? resolveCalledBatchToolCallConcurrency({
                toolCalls,
                requireToolApproval: rest.requireToolApproval,
                tools: stepTools,
                permissionPolicy,
                configuredConcurrency: configuredToolCallConcurrency,
              })
            : resolveToolCallConcurrency({
                requireToolApproval: rest.requireToolApproval,
                tools: stepTools,
                activeTools: _internal?.stepActiveTools as string[] | undefined,
                permissionPolicy,
                configuredConcurrency: configuredToolCallConcurrency,
              });
        return toolCalls;
      },
      { id: 'map-tool-calls' },
    )
    .foreach(toolCallStep, toolCallForeachOptions)
    .then(llmMappingStep)
    .then(backgroundTaskCheckStep)
    .then(signalDrainStep)
    .then(isTaskCompleteStep)
    .then(goalStep)
    .commit();
}
