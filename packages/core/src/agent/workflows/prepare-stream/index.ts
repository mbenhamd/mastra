import { z } from 'zod/v4';
import type { BackgroundTaskManager } from '../../../background-tasks';
import type { AgentBackgroundConfig } from '../../../background-tasks/types';
import type { SystemMessage } from '../../../llm';
import type { ToolCallConcurrency } from '../../../loop/types';
import { createRunScope } from '../../../mastra/run-scope';
import type { MastraMemory } from '../../../memory/memory';
import type { MemoryConfigInternal, StorageThreadType } from '../../../memory/types';
import type { Span, SpanType } from '../../../observability';
import { InternalSpans } from '../../../observability';
import type { RequestContext } from '../../../request-context';
import { MastraModelOutput } from '../../../stream';
import type { RequireToolApproval, ToolPayloadTransformPolicy } from '../../../tools';
import { createEventedWorkflow, createWorkflow as createDirectWorkflow } from '../../../workflows/create';
import type { Workspace } from '../../../workspace/workspace';
import type { InnerAgentExecutionOptions } from '../../agent.types';
import type { MastraDBMessage } from '../../message-list';
import type { SaveQueueManager } from '../../save-queue';
import type { CreatedAgentSignal } from '../../signals';
import type { AgentMethodType } from '../../types';
import { createMapResultsStep } from './map-results-step';
import { createPrepareMemoryStep } from './prepare-memory-step';
import { createPrepareToolsStep } from './prepare-tools-step';
import { INITIAL_SIGNAL_ECHOES_KEY } from './run-scope-keys';
import type { AgentCapabilities } from './schema';
import { createStreamStep } from './stream-step';

interface CreatePrepareStreamWorkflowOptions<OUTPUT = undefined> {
  capabilities: AgentCapabilities;
  options: InnerAgentExecutionOptions<OUTPUT>;
  threadFromArgs?: (Partial<StorageThreadType> & { id: string }) | undefined;
  resourceId?: string;
  runId: string;
  requestContext: RequestContext;
  agentSpan?: Span<SpanType.AGENT_RUN>;
  methodType: AgentMethodType;
  instructions: SystemMessage;
  /** MCP server guidance to include as a separate system message. */
  mcpServerGuidance?: string;
  memoryConfig?: MemoryConfigInternal;
  memory?: MastraMemory;
  returnScorerData?: boolean;
  saveQueueManager?: SaveQueueManager;
  requireToolApproval?: RequireToolApproval;
  toolCallConcurrency?: ToolCallConcurrency;
  resumeContext?: {
    resumeData: any;
    snapshot: any;
  };
  agentId: string;
  agentName?: string;
  toolCallId?: string;
  /** Persisted conversation behind the exact suspended tool call being resumed. */
  processorResumeMessages?: MastraDBMessage[];
  workspace?: Workspace;
  backgroundTaskManager?: BackgroundTaskManager;
  agentBackgroundConfig?: AgentBackgroundConfig;
  toolPayloadTransform?: ToolPayloadTransformPolicy;
  /**
   * When true, the in-loop `backgroundTaskCheckStep` skips its wait for
   * running tasks. Used when an outer caller (e.g. `agent.streamUntilIdle`)
   * drives continuation from outside the loop.
   */
  skipBgTaskWait?: boolean;
  drainPendingSignals?: (runId: string, scope?: 'pending' | 'pre-run') => CreatedAgentSignal[];
  /** Fork (PF-348): signal echoes replayed into the first LLM iteration of this run. */
  initialSignalEchoes?: CreatedAgentSignal[];
}

export function createPrepareStreamWorkflow<OUTPUT = undefined>({
  capabilities,
  options,
  threadFromArgs,
  resourceId,
  runId,
  requestContext,
  agentSpan,
  methodType,
  instructions,
  mcpServerGuidance,
  memoryConfig,
  memory,
  returnScorerData,
  saveQueueManager,
  requireToolApproval,
  toolCallConcurrency,
  resumeContext,
  agentId,
  agentName,
  toolCallId,
  processorResumeMessages,
  workspace,
  backgroundTaskManager,
  agentBackgroundConfig,
  toolPayloadTransform,
  skipBgTaskWait,
  drainPendingSignals,
  initialSignalEchoes,
}: CreatePrepareStreamWorkflowOptions<OUTPUT>) {
  // Per-run scope shared between prepare-stream steps. Class instances
  // (MessageList, Tools), Maps, and closures live here instead of step
  // outputs — see ./run-scope.ts.
  //
  // This scope is a closure local to this workflow factory and is NOT
  // registered with `Mastra.__createRunScope`. The agentic-loop workflow uses
  // a separate runId-keyed scope on the Mastra instance (created via
  // `__registerInternalWorkflow`); the bridge between them is
  // `hydrateRunScopeFromInternal` in `loop/workflows/stream.ts`, which copies
  // bootstrap state from `_internal` into the Mastra scope after the loop
  // workflow registers. Prepare-stream and the agentic loop deliberately do
  // not share runtime state — each owns its own per-run scratch space.
  const runScope = createRunScope();
  if (initialSignalEchoes?.length) {
    runScope.set(INITIAL_SIGNAL_ECHOES_KEY, initialSignalEchoes);
  }

  const prepareToolsStep = createPrepareToolsStep({
    capabilities,
    options,
    threadFromArgs,
    resourceId,
    runId,
    requestContext,
    agentSpan,
    methodType,
    memory,
    isResume: !!resumeContext,
    backgroundTaskEnabled: backgroundTaskManager?.config?.enabled,
    ...(processorResumeMessages !== undefined ? { processorResumeMessages } : {}),
    runScope,
  });

  const prepareMemoryStep = createPrepareMemoryStep({
    capabilities,
    options,
    threadFromArgs,
    resourceId,
    runId,
    requestContext,
    methodType,
    instructions,
    mcpServerGuidance,
    memoryConfig,
    memory,
    isResume: !!resumeContext,
    runScope,
  });

  const streamStep = createStreamStep({
    capabilities,
    runId,
    returnScorerData,
    requireToolApproval,
    toolCallConcurrency,
    resumeContext,
    agentId,
    agentName,
    toolCallId,
    methodType,
    saveQueueManager,
    memoryConfig,
    memory,
    resourceId,
    autoResumeSuspendedTools: options.autoResumeSuspendedTools,
    workspace,
    backgroundTaskManager,
    agentBackgroundConfig,
    toolPayloadTransform,
    toolHooks: capabilities.resolveToolHooks(options.hooks),
    skipBgTaskWait,
    drainPendingSignals,
    runScope,
  });

  const mapResultsStep = createMapResultsStep({
    capabilities,
    options,
    resourceId,
    threadId: threadFromArgs?.id,
    runId,
    requestContext,
    memory,
    memoryConfig,
    agentSpan,
    agentId,
    methodType,
    saveQueueManager,
    runScope,
  });

  // Internal toggle: the default is direct (in-process) execution which avoids
  // the requestContext serialisation cycle (toJSON → reconstruct) that drops
  // non-serialisable values (functions, circular-ref objects like the controller
  // context). Set MASTRA_EVENTED_EXECUTION=true to opt in to the evented
  // workflow engine for cross-process coordination via pubsub.
  const useEventedExecution = process.env.MASTRA_EVENTED_EXECUTION === 'true';
  const factory = useEventedExecution ? createEventedWorkflow : createDirectWorkflow;

  return factory({
    id: 'execution-workflow',
    inputSchema: z.object({}),
    outputSchema: z.instanceof(MastraModelOutput<OUTPUT>),
    steps: [prepareToolsStep, prepareMemoryStep, streamStep],
    options: {
      ...(useEventedExecution ? {} : { executionMode: 'transient' as const }),
      tracingPolicy: {
        internal: InternalSpans.WORKFLOW,
      },
      // This is an internal, non-resumable workflow created per agent generate/stream call.
      // Direct execution is transient and performs no lifecycle storage I/O. Evented execution
      // must stay durable for dispatch, so it retains the no-write policy and cleanup below.
      shouldPersistSnapshot: () => false,
      validateInputs: false,
    },
  })
    .parallel([prepareToolsStep, prepareMemoryStep])
    .map(mapResultsStep)
    .then(streamStep)
    .commit();
}
