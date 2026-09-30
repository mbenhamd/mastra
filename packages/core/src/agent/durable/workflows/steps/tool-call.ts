import { z } from 'zod';
import { executeAdoptedBackgroundOperation } from '../../../../background-tasks/adoption';
import type { ToolBackgroundConfig } from '../../../../background-tasks/types';
import { ErrorCategory, ErrorDomain, MastraError } from '../../../../error';
import type { PubSub } from '../../../../events/pubsub';
import { normalizeModelOutput } from '../../../../loop/shared/normalize-model-output';
import { readToolResultFromMessageList } from '../../../../loop/shared/read-tool-result';
import { dispatchBackgroundTool } from '../../../../loop/shared/steps/background-dispatch-core';
import { applyBackgroundToolResult } from '../../../../loop/shared/steps/background-task-result-core';
import { executeToolCall } from '../../../../loop/shared/steps/execute-tool-core';
import { processAndEmitChunk } from '../../../../loop/shared/steps/process-chunk-core';
import { resolveFrameworkSuspendedToolIdentity } from '../../../../loop/shared/suspended-tool-run-id';
import type { ResolvedSuspendedToolIdentity } from '../../../../loop/shared/suspended-tool-run-id';
import { applyToolPayloadTransformToChunk } from '../../../../loop/shared/tool-payload-transform';
import { notifyToolDenied } from '../../../../loop/workflows/agentic-execution/tool-permission-notify';
import type { Mastra } from '../../../../mastra';
import type { MastraMemory } from '../../../../memory/memory';
import type { MemoryConfig } from '../../../../memory/types';
import { EntityType, SpanType, createObservabilityContext } from '../../../../observability';
import type { ExportedSpan, ObservabilityContext } from '../../../../observability';
import type { ProcessorState, ProcessorStreamWriter } from '../../../../processors';
import { BACKGROUND_WORK_CONTEXT } from '../../../../processors/background-work-signals';
import { ProcessorRunner, outputProcessorsSupportStream } from '../../../../processors/runner';
import type { RequestContext } from '../../../../request-context';
import type { ChunkType, ProviderMetadata } from '../../../../stream/types';
import { ChunkFrom } from '../../../../stream/types';
import {
  getTransformedToolPayload,
  hasTransformedToolPayload,
  withToolPayloadTransformProviderMetadata,
} from '../../../../tools/payload-transform';
import { findProviderToolByName } from '../../../../tools/provider-tool-utils';
import { ToolStream } from '../../../../tools/stream';
import { getToolTitle } from '../../../../tools/tool-title';
import type { CoreTool } from '../../../../tools/types';
import { resolveToolOutputValidationSchema, validateToolOutput } from '../../../../tools/validation';
import { ensureSerializable } from '../../../../utils';
import { PUBSUB_SYMBOL } from '../../../../workflows/constants';
import type { SuspendOptions } from '../../../../workflows/step';
import { createStep } from '../../../../workflows/workflow';
import { stopGoalActivity } from '../../../goal';
import type { MessageList } from '../../../message-list';
import type { SaveQueueManager } from '../../../save-queue';
import { resolveDeclineReason } from '../../../tool-approval';
import {
  createToolCallIdentityDigest,
  parseToolApprovalDecision,
  parseToolApprovalGrant,
} from '../../../tool-call-identity';
import {
  ON_BEFORE_TOOL_EXECUTION_KEY,
  ON_BEFORE_TOOL_EXECUTION_REQUIRED_KEY,
  TOOL_PERMISSION_DENIED_ERROR_NAME,
  TOOL_PERMISSION_POLICY_KEY,
  TOOL_PERMISSION_POLICY_REQUIRED_KEY,
} from '../../../tool-permission-prefilter';
import type {
  BeforeToolExecutionHook,
  ToolPermissionDecision,
  ToolPermissionPolicy,
} from '../../../tool-permission-prefilter';
import { createToolSurfaceFence, materializeToolSurfaceFence } from '../../../tool-surface-fence';
import { TripWire } from '../../../trip-wire';
import { ensureRemoteAbortListener } from '../../abort-transport';
import { DurableStepIds } from '../../constants';
import { getBoundRunRegistryEntry, globalRunRegistry, markRunActive } from '../../run-registry';
import { emitSuspendedEvent, emitChunkEvent } from '../../stream-adapter';
import type {
  DurableToolCallInput,
  DurableAgenticWorkflowInput,
  DurableToolCallOutput,
  SerializableDurableOptions,
  SerializableToolMetadata,
  AgentSuspendedEventData,
  RunRegistryEntry,
} from '../../types';
import {
  rebuildRunToolsFromMastra,
  resolveTool,
  restoreRequestContext,
  toolApprovalRequirement,
} from '../../utils/resolve-runtime';
import { serializeError } from '../../utils/serialize-state';
import {
  assertDurableToolHookPolicyAvailable,
  throwDurableToolHookPolicyUnavailable,
} from '../../utils/tool-hook-policy';

/**
 * Input schema for the durable tool call step.
 * Each tool call flows through this schema when using .foreach()
 */
const durableToolCallInputSchema = z.object({
  iterationCount: z.number().int().nonnegative().optional(),
  toolCallId: z.string(),
  toolName: z.string(),
  args: z.record(z.string(), z.any()),
  providerMetadata: z.record(z.string(), z.any()).optional(),
  providerExecuted: z.boolean().optional(),
  output: z.any().optional(),
  activeTools: z.array(z.string()).nullable().optional(),
  // Exported MODEL_STEP span so the TOOL_CALL nests under the LLM call
  stepSpanData: z.any().optional(),
});

/**
 * Output schema for the durable tool call step.
 *
 * NOTE on field declarations: nothing strips undeclared fields today — both
 * loop builders run with `validateInputs: false` and the workflows engine has
 * no output-side validation — so undeclared fields still cross step
 * boundaries at runtime. Every field the step emits is declared anyway for
 * type/schema honesty and so the contract survives if validation is ever
 * (re-)enabled (e.g. by the Phase 2 evented port, which may use different
 * validation defaults). If validation is enabled, an undeclared field would
 * be silently stripped at the boundary — declare new output fields here.
 */
const durableToolCallOutputSchema = durableToolCallInputSchema.extend({
  resumeTargetToolCallId: z.string().optional(),
  result: z.any().optional(),
  serverExecuted: z.boolean().optional(),
  modelOutputComputed: z.boolean().optional(),
  // Set when execution was interrupted by request abort (not a tool error); no result/error
  // so the mapping step leaves the call incomplete.
  // Mirrors the non-durable tool-call output schema.
  aborted: z.boolean().optional(),
  // Set when a processToolResult processor blocked the result via tripwire; no result
  // crosses the boundary and the mapping step leaves the call incomplete.
  resultBlocked: z.boolean().optional(),
  error: z
    .object({
      name: z.string(),
      message: z.string(),
      stack: z.string().optional(),
    })
    .optional(),
  disposition: z.literal('denied').optional(),
  // Approval decision for a `requireApproval` tool; a declined call carries its
  // `output-denied` marker across the boundary in this field.
  approval: z
    .object({
      id: z.string(),
      approved: z.boolean(),
      reason: z.string().optional(),
    })
    .optional(),
  // Non-transient data-* chunks emitted by output processors via writer.custom()
  // during this tool call. The tool-call step's messageList is a local copy whose
  // mutations don't cross the step boundary, so these are carried on the output
  // record and persisted into the authoritative messageList by the mapping step
  // (#19375 parity port).
  processorDataParts: z
    .array(
      z.object({
        type: z.string(),
        data: z.any().optional(),
        messageId: z.string().optional(),
      }),
    )
    .optional(),
  // Payload-transform metadata captured from the emitted tool-result/tool-error
  // chunk (L18b): merged into providerMetadata by the mapping step before
  // commitToolResult so transcript/display targets apply on recall.
  transformMetadata: z.record(z.string(), z.any()).optional(),
  // Set when a delegation onDelegationComplete hook called ctx.bail() during
  // this tool call. Carried on the step output (not requestContext) because
  // the evented engine rehydrates a fresh RequestContext per step, so the
  // wrapper's by-reference flag write never reaches the mapping step there.
  delegationBailed: z.boolean().optional(),
});

/**
 * Flush messages to memory before suspending.
 * Mirrors the base Agent's flushMessagesBeforeSuspension() to ensure
 * the thread exists and all pending messages are persisted.
 *
 * Skips entirely when memoryConfig.readOnly is set, mirroring the readOnly
 * guard on the durable finish path — a readOnly run shouldn't get a thread
 * created or messages written just because it happened to suspend mid-run.
 */
async function flushMessagesBeforeSuspension({
  saveQueueManager,
  messageList,
  memory,
  threadId,
  resourceId,
  memoryConfig,
  threadExists,
  onThreadCreated,
}: {
  saveQueueManager?: SaveQueueManager;
  messageList?: MessageList;
  memory?: MastraMemory;
  threadId?: string;
  resourceId?: string;
  memoryConfig?: MemoryConfig;
  threadExists?: boolean;
  onThreadCreated?: () => void;
}) {
  if (!saveQueueManager || !messageList || !threadId || memoryConfig?.readOnly) {
    return;
  }

  try {
    // Ensure thread exists before flushing messages
    if (memory && !threadExists && resourceId) {
      const thread = await memory.getThreadById?.({ threadId });
      if (!thread) {
        await memory.createThread?.({
          threadId,
          resourceId,
          memoryConfig,
        });
      }
      onThreadCreated?.();
    }

    // Flush all pending messages immediately
    await saveQueueManager.flushMessages(messageList, threadId, memoryConfig);
  } catch {
    // Log but don't throw — suspension should proceed even if flush fails
  }
}

// Concurrent durable tool calls share processor-owned state. Serialize only
// their output-chunk pipeline so each chunk completes one span segment before
// the next starts, while the tool executions themselves remain concurrent.
const outputProcessorQueues = new WeakMap<RunRegistryEntry, Promise<void>>();

/**
 * Run a tool-result or tool-error chunk through the run's output processor
 * pipeline and emit it (or a tripwire when blocked) via pubsub. Returns the
 * processed chunk, or `null` if a processor blocked it.
 *
 * Thin glue over the shared per-chunk pipeline core; mirrors the regular
 * agent's `processAndEnqueueChunk` in llm-mapping-step.ts.
 */
async function processChunkThroughOutputProcessors(
  chunk: ChunkType,
  registryEntry: RunRegistryEntry | undefined,
  pubsub: PubSub | undefined,
  runId: string,
  agentName: string,
  logger: any,
  messageList?: MessageList,
  observabilityContext?: ObservabilityContext,
  collectDataPart?: (part: { type: string; data?: unknown; messageId?: string }) => void,
): Promise<ChunkType | null> {
  // Serialize per-registry-entry processor pipelines so concurrently running
  // foreach tool calls emit their chunks in order.
  const previous = registryEntry ? (outputProcessorQueues.get(registryEntry) ?? Promise.resolve()) : Promise.resolve();
  let releaseQueue!: () => void;
  const queueTail = new Promise<void>(resolve => {
    releaseQueue = resolve;
  });
  if (registryEntry) {
    outputProcessorQueues.set(registryEntry, queueTail);
    await previous.catch(() => {});
  }

  try {
    if (registryEntry?.processorStates && registryEntry.outputProcessorRunner === undefined) {
      registryEntry.outputProcessorRunner = outputProcessorsSupportStream(registryEntry.outputProcessors)
        ? new ProcessorRunner({
            inputProcessors: [],
            outputProcessors: registryEntry.outputProcessors,
            logger,
            agentName,
            processorStates: registryEntry.processorStates,
          })
        : null;
    }
    const runner = registryEntry?.outputProcessorRunner ?? undefined;

    // Keep one stable writer per run/pubsub pair (fork PF-2236: no per-chunk
    // writer allocation), but route non-transient data-* chunks to the
    // collector of the invocation that currently holds the serialized queue
    // (#19375): each tool-call step invocation owns its own
    // `processorDataParts` for persistence by the mapping step. The slot is
    // cleared in `finally` below so a late write streams without being
    // attributed to another step's output.
    let writerCache = registryEntry?.outputProcessorWriter;
    if (pubsub && (!writerCache || writerCache.pubsub !== pubsub || writerCache.runId !== runId)) {
      const cache: NonNullable<RunRegistryEntry['outputProcessorWriter']> = {
        pubsub,
        runId,
        writer: {
          custom: async (
            data: { type: string; data?: unknown; transient?: boolean },
            writerOptions?: { messageId?: string },
          ) => {
            // Collect non-transient data-* chunks for persistence by the
            // mapping step (#19375 parity port); transient chunks stay
            // stream-only.
            if (data.type.startsWith('data-') && !data.transient) {
              cache.collect?.({ type: data.type, data: data.data, messageId: writerOptions?.messageId });
            }
            await emitChunkEvent(pubsub, runId, data as ChunkType);
          },
        },
      };
      writerCache = cache;
      if (registryEntry) registryEntry.outputProcessorWriter = cache;
    }
    if (writerCache) writerCache.collect = collectDataPart;
    const streamWriter: ProcessorStreamWriter | undefined = pubsub ? writerCache?.writer : undefined;

    return await processAndEmitChunk(chunk, {
      runner,
      processorStates: registryEntry?.processorStates as Map<string, ProcessorState> | undefined,
      observabilityContext,
      requestContext: registryEntry?.requestContext,
      messageList,
      streamWriter,
      emitChunk: async c => {
        if (pubsub) {
          await emitChunkEvent(pubsub, runId, c);
        }
      },
      onProcessorError: error => {
        logger?.warn?.(`[DurableAgent] Output processor error for tool chunk: ${error}`);
        // Fail closed: drop the chunk instead of emitting the unprocessed
        // original — a throwing redaction processor must not leak the raw
        // value. The regular loop registers no fallback at all (processor
        // failures propagate and fail the request); this engine keeps the
        // run alive but suppresses the chunk.
        return null;
      },
      // The finish chunk that normally ends stream-processor spans never reaches
      // this pipeline, so end the spans opened for this chunk here.
      endSpansAfterProcessing: true,
    });
  } finally {
    // Detach this invocation's collector before the next queued call binds its own.
    const writerCache = registryEntry?.outputProcessorWriter;
    if (writerCache && writerCache.collect === collectDataPart) {
      writerCache.collect = undefined;
    }
    releaseQueue();
    if (registryEntry && outputProcessorQueues.get(registryEntry) === queueTail) {
      outputProcessorQueues.delete(registryEntry);
    }
  }
}

/**
 * Create a durable tool call step.
 *
 * This step mirrors the base Agent's createToolCallStep pattern:
 * 1. Resolves the tool from the run registry or Mastra
 * 2. Checks if approval is required (global or per-tool)
 * 3. If approval required, emits suspended event, persists messages, and suspends
 * 4. Executes the tool with a suspend callback for in-execution suspension
 * 5. Emits tool-result or tool-error chunks via PubSub
 * 6. Returns the result or error
 *
 * Tool suspension is handled via workflow suspend/resume mechanism:
 * - Tool approval: step suspends with approval payload
 * - In-execution suspension: tool calls suspend() callback, step suspends with suspension payload
 * - Message persistence: messages are flushed before any suspension
 *
 * Deliberate divergence from the main loop: main cannot pause its
 * request-scoped loop, so pending client tools ride the finish payload for
 * browser-side handling and results come back on a follow-up request (#21688
 * family). Durable has first-class suspension — the whole workflow parks at
 * this step and resumes with the decision/result — so none of main's
 * pending-tool plumbing applies here.
 */
export interface DurableToolPermissionResolverInput {
  runId: string;
  agentId: string;
  toolCallId: string;
  toolName: string;
  args: Record<string, unknown>;
  requestContext: RequestContext | undefined;
  isResume: boolean;
}

export type DurableToolPermissionResolver = (
  input: DurableToolPermissionResolverInput,
) => ToolPermissionDecision | Promise<ToolPermissionDecision>;

export interface CreateDurableToolCallStepOptions {
  /**
   * Trusted worker-local policy resolver for engines that can replay on a
   * different process. The callback is code/configuration, never workflow
   * state. Throwing or returning an invalid decision fails closed as `deny`.
   */
  resolveToolPermission?: DurableToolPermissionResolver;
}

function normalizeToolPermissionDecision(candidate: unknown): ToolPermissionDecision {
  return candidate === 'allow' || candidate === 'ask' || candidate === 'deny' ? candidate : 'deny';
}

function combineToolPermissionDecisions(decisions: ToolPermissionDecision[]): ToolPermissionDecision | undefined {
  if (decisions.includes('deny')) return 'deny';
  if (decisions.includes('ask')) return 'ask';
  if (decisions.includes('allow')) return 'allow';
  return undefined;
}

export function createDurableToolCallStep(options: CreateDurableToolCallStepOptions = {}) {
  const { resolveToolPermission } = options;
  return createStep({
    id: DurableStepIds.TOOL_CALL,
    inputSchema: durableToolCallInputSchema,
    outputSchema: durableToolCallOutputSchema,
    execute: async params => {
      const {
        inputData,
        mastra,
        suspend,
        resumeData: workflowResumeData,
        suspendData,
        requestContext,
        actor,
        getInitData,
      } = params;

      // Access pubsub via symbol
      const pubsub = (params as any)[PUBSUB_SYMBOL] as PubSub | undefined;

      const typedInput = inputData as DurableToolCallInput;
      const {
        iterationCount = 0,
        toolCallId,
        toolName,
        args: rawArgs,
        providerExecuted,
        output,
        activeTools,
      } = typedInput;

      // Extract the model-facing resume controls before validating or executing the tool.
      // A fresh provider call gets a new toolCallId, so the original call and run IDs are
      // required to bind its resumeData to the exact persisted suspension.
      let resumeDataFromArgs: any = undefined;
      let suspendedToolCallId: string | undefined;
      let suppliedSuspendedToolRunId: string | undefined;
      let modelClaimedSuspendedIdentity = false;
      let args: any = rawArgs;
      if (typeof rawArgs === 'object' && rawArgs !== null) {
        const {
          resumeData: resumeDataFromInput,
          suspendedToolCallId: suspendedToolCallIdFromInput,
          suspendedToolRunId: suspendedToolRunIdFromInput,
          ...argsFromInput
        } = rawArgs as Record<string, any>;
        args = argsFromInput;
        resumeDataFromArgs = resumeDataFromInput;
        modelClaimedSuspendedIdentity =
          suspendedToolCallIdFromInput !== undefined || suspendedToolRunIdFromInput !== undefined;
        if (resumeDataFromInput !== undefined && resumeDataFromInput !== null) {
          suspendedToolCallId =
            typeof suspendedToolCallIdFromInput === 'string' && suspendedToolCallIdFromInput.length > 0
              ? suspendedToolCallIdFromInput
              : undefined;
          suppliedSuspendedToolRunId =
            typeof suspendedToolRunIdFromInput === 'string' && suspendedToolRunIdFromInput.length > 0
              ? suspendedToolRunIdFromInput
              : undefined;
        }
      }
      // Non-transient data-* chunks emitted by output processors via
      // writer.custom() during this tool call. This step's messageList is a
      // local copy whose mutations don't cross the step boundary, so parts are
      // collected here and carried on the output record for the mapping step
      // to persist into the authoritative messageList (#19375 parity port).
      const processorDataParts: Array<{ type: string; data?: unknown; messageId?: string }> = [];
      const collectProcessorDataPart = (part: { type: string; data?: unknown; messageId?: string }) => {
        processorDataParts.push(part);
      };

      let resumeData = resumeDataFromArgs ?? workflowResumeData;
      let isFreshTurnResume = resumeDataFromArgs !== undefined && resumeDataFromArgs !== null;
      const metadataToolCallId = suspendedToolCallId ?? toolCallId;
      // Get context from init data (the parent workflow input)
      const initData = getInitData<{
        runId: string;
        runtimeBindingId?: string;
        agentId: string;
        runtimeResolution?: 'registry-required';
        options: SerializableDurableOptions;
        toolsMetadata: SerializableToolMetadata[];
        messageListState: DurableAgenticWorkflowInput['messageListState'];
        state: {
          threadId?: string;
          resourceId?: string;
          memoryConfig?: MemoryConfig;
          threadExists?: boolean;
        };
        requestContextEntries?: Record<string, unknown>;
        agentSpanData?: unknown;
        modelSpanData?: unknown;
      }>();

      const { runId, runtimeBindingId, options: agentOptions, state } = initData;
      const logger = (mastra as any)?.getLogger?.();
      let registryEntry = getBoundRunRegistryEntry(runId, runtimeBindingId);
      assertDurableToolHookPolicyAvailable({
        serialized: agentOptions.toolHookPolicy,
        registryEntry,
      });
      if (agentOptions.toolHookPolicy !== undefined && !registryEntry?.tools) {
        throwDurableToolHookPolicyUnavailable();
      }
      const resumeIdentityDigest = createToolCallIdentityDigest({ toolCallId: metadataToolCallId, toolName, args });
      const identityDigest = createToolCallIdentityDigest({ toolCallId, toolName, args });

      // End the open MODEL_STEP + MODEL_GENERATION + AGENT_RUN as `suspended` before
      // pausing — stores persist only span-end events, so an un-ended root is dropped if
      // the run is never resumed. On resume a fresh root is opened (see DurableAgent.resume).
      const endSpansAsSuspended = (info: { toolCallId?: string; toolName?: string; reason?: string }) => {
        try {
          const obs = (mastra as Mastra | undefined)?.observability?.getSelectedInstance({ requestContext });
          if (!obs) return;
          const output = {
            status: 'suspended' as const,
            reason: info.reason,
            toolName: info.toolName,
            toolCallId: info.toolCallId,
          };
          // After a prior resume, end the resume spans (registry override) — they are the
          // active root for this segment. Otherwise end the threaded originals.
          const reg = globalRunRegistry.get(runId);
          const agentSpanData = reg?.resumeAgentSpanData ?? initData.agentSpanData;
          const modelSpanData = reg?.resumeModelSpanData ?? initData.modelSpanData;
          if (typedInput.stepSpanData) {
            obs.rebuildSpan(typedInput.stepSpanData as ExportedSpan<SpanType.MODEL_STEP>)?.end({ output });
          }
          if (modelSpanData) {
            obs.rebuildSpan(modelSpanData as ExportedSpan<SpanType.MODEL_GENERATION>)?.end({ output });
          }
          if (agentSpanData) {
            obs.rebuildSpan(agentSpanData as ExportedSpan<SpanType.AGENT_RUN>)?.end({ output });
          }
        } catch (error) {
          // Span bookkeeping must never break suspension.
          logger?.warn?.(`[DurableAgent] Failed to end spans on suspend: ${error}`);
        }
      };

      // Provider-executed tools are handled entirely by the stream path
      // (tool-call and tool-result chunks in llm-execution.ts), so skip client
      // execution — mirrors the non-durable tool-call step. When the provider
      // already delivered the output in the same stream, thread it through as
      // the result; a deferred result (e.g. Anthropic web_search resolving in
      // a later stream) must not fall through to client execution, which would
      // try to run the provider tool client-side and fail with
      // ToolNotFoundError. The deferred result is patched into the messageList
      // by llm-execution's tool-result handling when it arrives in a later
      // stream (#14282 parity port).
      if (providerExecuted) {
        return {
          ...typedInput,
          ...(output !== undefined ? { result: output } : {}),
        };
      }

      // 1. Resolve the tool from the binding-checked registry first. Built-in
      // durable runs fail closed if that exact runtime entry was lost.
      if (
        (!registryEntry || registryEntry.isPlaceholder === true) &&
        initData.runtimeResolution === 'registry-required'
      ) {
        throw new MastraError({
          id: 'DURABLE_AGENT_RUNTIME_REGISTRY_MISSING',
          domain: ErrorDomain.AGENT,
          category: ErrorCategory.SYSTEM,
          text: `DurableAgent runtime dependencies are unavailable for run "${runId}". Resume the run through DurableAgent so recovery checks can restore them.`,
          details: { agentId: initData.agentId, runId },
        });
      }
      // Resolve by provider-tool
      // model-facing name (e.g. `web_search` resolves to `webSearch` when the
      // provider tool advertises the snake-case name), then by id, then fall
      // back to the Mastra-wide tool registry (exact name, provider-tool
      // name, then by id). Mirrors the non-durable tool-call step.
      // Replacement (fenced) runs skip every fallback stage: they dispatch only
      // from the immutable surface captured at preparation.
      if ((!registryEntry || registryEntry.isPlaceholder === true) && agentOptions.toolSurfaceFence !== undefined) {
        throw new Error(
          `[DurableAgent:${initData.agentId}] Cannot reconstruct replacement tool implementations for run ${runId} after the run registry was lost. Refusing to substitute backing-agent tools by name.`,
        );
      }
      if (pubsub) {
        try {
          await ensureRemoteAbortListener(pubsub, runId, runtimeBindingId);
        } catch (error) {
          logger?.warn?.('Failed to subscribe to cross-process abort requests', {
            runId,
            error: error instanceof Error ? error.message : String(error),
          });
        }
        registryEntry = getBoundRunRegistryEntry(runId, runtimeBindingId);
      }
      const replacementToolNames =
        agentOptions.toolSurfaceFence !== undefined ? new Set(agentOptions.toolSurfaceFence) : undefined;
      // For a replacement run, never dispatch from the mutable registry object.
      // Select from the immutable surface bound to the fenced originals captured at
      // preparation; fall back to re-materializing the fence when that surface is
      // unavailable. Either way an in-place processor mutation of `registryEntry.tools`
      // cannot swap the executable the model was shown a fenced original for.
      let toolSourceMap: Record<string, CoreTool> | undefined = registryEntry?.tools;
      if (registryEntry && replacementToolNames) {
        // Revalidate at the side-effect boundary. A crash/restart can resume
        // directly at this step after the LLM step's earlier validation, and this
        // rebuild also fails closed on a partial registry.
        toolSourceMap =
          registryEntry.replacementToolSurface ??
          (materializeToolSurfaceFence(createToolSurfaceFence(registryEntry.tools, replacementToolNames)) as Record<
            string,
            CoreTool
          >);
      }
      let tool = replacementToolNames?.has(toolName) === false ? undefined : toolSourceMap?.[toolName];
      const observability = (mastra as Mastra | undefined)?.observability?.getSelectedInstance({ requestContext });

      // Parent per-chunk processor spans under the active durable agent segment.
      const processorAgentSpanData = registryEntry?.resumeAgentSpanData ?? initData.agentSpanData;
      const processorAgentSpan =
        registryEntry?.resumeAgentSpan ??
        registryEntry?.agentSpan ??
        (processorAgentSpanData && observability
          ? observability.rebuildSpan(processorAgentSpanData as ExportedSpan<SpanType.AGENT_RUN>)
          : undefined);
      const processorObservabilityContext = processorAgentSpan
        ? createObservabilityContext({ currentSpan: processorAgentSpan })
        : undefined;
      let mastraTools: Record<string, any> | undefined;
      // Tools rebuilt from the Mastra instance when the per-process registry is
      // empty (cross-process worker). Populated lazily below; reused for
      // workspace/memory resolution further down.
      let rebuiltTools: Record<string, any> | undefined;
      let rebuiltWorkspace: any;
      let rebuiltMemory: any;
      let rebuiltSaveQueueManager: any;
      // RequestContext the rebuilt tools were built with (their closures
      // capture it) — checked for the delegation bail flag after execution.
      let rebuiltRequestContext: RequestContext | undefined;

      if (!tool && replacementToolNames === undefined) {
        tool = findProviderToolByName(toolSourceMap as any, toolName) as typeof tool;
      }

      if (!tool && replacementToolNames === undefined) {
        tool = Object.values(toolSourceMap ?? {}).find(
          (t: any) => t && typeof t === 'object' && 'id' in t && t.id === toolName,
        ) as typeof tool;
      }

      // Per-execution hooks are burned into the exact tool wrappers captured
      // at preparation. Falling back to a Mastra-wide or freshly rebuilt tool
      // here would execute outside that policy even when its marker matches.
      if (!tool && agentOptions.toolHookPolicy !== undefined) {
        throwDurableToolHookPolicyUnavailable();
      }

      if (!tool && replacementToolNames === undefined && initData.runtimeResolution !== 'registry-required') {
        tool = resolveTool(toolName, mastra as Mastra);
      }

      if (!tool && mastra && replacementToolNames === undefined && initData.runtimeResolution !== 'registry-required') {
        mastraTools = (mastra as Mastra).listTools?.() as Record<string, any> | undefined;
        if (mastraTools) {
          tool = findProviderToolByName(mastraTools as any, toolName) as typeof tool;
          if (!tool) {
            tool = Object.values(mastraTools).find(
              (t: any) => t && typeof t === 'object' && 'id' in t && t.id === toolName,
            ) as typeof tool;
          }
        }
      }

      // Cross-process fallback: workspace/skill tools are per-request closures
      // never registered at the Mastra-instance level, so the lookups above miss
      // them when the durable steps run on a separate process (e.g. the
      // @mastra/inngest connect() worker) whose registry is empty. Rebuild the
      // full toolset from the agent — the same rebuild the LLM step already does
      // via resolveRuntimeDependencies — and retry. This is the root-cause fix
      // for `ToolNotFoundError` on skill/mastra_workspace_* tools cross-process.
      // Replacement (fenced) runs never rebuild: caller-supplied replacement
      // implementations cannot be reconstructed from the backing agent. On a
      // remote worker, rebuilding is also the only way to obtain the save queue
      // needed to persist suspension metadata.
      const needsSaveQueueForFlush = !registryEntry?.saveQueueManager && !!state?.threadId;
      if (
        (!tool || needsSaveQueueForFlush) &&
        mastra &&
        replacementToolNames === undefined &&
        initData.runtimeResolution !== 'registry-required'
      ) {
        const rebuilt = await rebuildRunToolsFromMastra({
          mastra: mastra as Mastra,
          runId,
          agentId: initData.agentId,
          state: state as any,
          options: agentOptions,
          toolsMetadata: initData.toolsMetadata,
          messageListState: initData.messageListState,
          requestContextEntries: initData.requestContextEntries,
          requestContext,
          logger,
        });
        if (rebuilt) {
          rebuiltTools = rebuilt.tools;
          rebuiltWorkspace = rebuilt.workspace;
          rebuiltMemory = rebuilt.memory;
          rebuiltSaveQueueManager = rebuilt.saveQueueManager;
          rebuiltRequestContext = rebuilt.requestContext;
          // Keep an already-resolved tool: we may have rebuilt purely to obtain the
          // SaveQueueManager, and the registry's instance is the live per-request closure.
          if (!tool) {
            tool = rebuiltTools[toolName] as typeof tool;
          }
          if (!tool) {
            tool = findProviderToolByName(rebuiltTools as any, toolName) as typeof tool;
          }
          if (!tool) {
            tool = Object.values(rebuiltTools).find(
              (t: any) => t && typeof t === 'object' && 'id' in t && t.id === toolName,
            ) as typeof tool;
          }
        }
      }

      // Resolve the key the tool is registered under for activeTools filtering.
      // Prefer the per-run tool source key (exact name then identity match),
      // and fall back to the Mastra-wide registry when the tool was resolved
      // there. Without this fallback, a globally-registered tool like
      // `webSearch` invoked by its model-facing name `web_search` would be
      // hidden whenever `activeTools` was set, because the key from
      // the per-run tool source would be `undefined`.
      const toolKey =
        toolSourceMap?.[toolName] || rebuiltTools?.[toolName]
          ? toolName
          : (Object.entries(toolSourceMap ?? {}).find(([, registeredTool]) => registeredTool === tool)?.[0] ??
            Object.entries(rebuiltTools ?? {}).find(([, registeredTool]) => registeredTool === tool)?.[0] ??
            Object.entries(mastraTools ?? {}).find(([, registeredTool]) => registeredTool === tool)?.[0]);
      const effectiveActiveTools = activeTools === null ? undefined : (activeTools ?? agentOptions.activeTools);
      const activeToolKey = toolKey ?? toolName;
      const isHiddenByActiveTools = effectiveActiveTools !== undefined && !effectiveActiveTools.includes(activeToolKey);

      if (!tool || isHiddenByActiveTools) {
        const registeredToolNames = Object.keys(rebuiltTools ?? toolSourceMap ?? {});
        const fenceScopedToolNames =
          replacementToolNames === undefined
            ? registeredToolNames
            : registeredToolNames.filter(name => replacementToolNames.has(name));
        const availableToolNames =
          effectiveActiveTools === undefined
            ? fenceScopedToolNames
            : replacementToolNames === undefined
              ? effectiveActiveTools
              : effectiveActiveTools.filter(name => replacementToolNames.has(name));
        const availableToolsStr =
          availableToolNames.length > 0 ? ` Available tools: ${availableToolNames.join(', ')}` : '';
        const error = {
          name: 'ToolNotFoundError',
          message: `Tool "${toolName}" not found.${availableToolsStr}. Call tools by their exact name only — never add prefixes, namespaces, or colons.`,
        };
        if (pubsub) {
          await emitChunkEvent(pubsub, runId, {
            type: 'tool-error',
            runId,
            from: ChunkFrom.AGENT,
            payload: { toolCallId, toolName, args, error },
          });
        }
        return {
          ...typedInput,
          error,
        };
      }

      // Get memory-related state for message persistence. Fall back to the
      // values rebuilt from Mastra above (cross-process worker), so workspace
      // tools receive their `workspace` and message flushing still works.
      const saveQueueManager = registryEntry?.saveQueueManager ?? rebuiltSaveQueueManager;
      const memory = registryEntry?.memory ?? rebuiltMemory;
      const workspace = registryEntry?.workspace ?? rebuiltWorkspace;
      let threadExists = state?.threadExists ?? false;

      // Reconstruct MessageList from workflow state if available
      // Note: In foreach mode, the message list from the registry may be available
      // but for durability, we access what's available through the registry
      let messageList: MessageList | undefined;
      // For local execution, the bound global entry may be an ExtendedRunRegistry entry
      // that stores the MessageList. Reuse the already binding-checked value.
      const extendedEntry = registryEntry as any;
      if (extendedEntry?.messageList) {
        messageList = extendedEntry.messageList;
      }

      const doFlush = async () => {
        await flushMessagesBeforeSuspension({
          saveQueueManager,
          messageList,
          memory,
          threadId: state?.threadId,
          resourceId: state?.resourceId,
          memoryConfig: state?.memoryConfig,
          threadExists,
          onThreadCreated: () => {
            threadExists = true;
          },
        });
      };

      const workflowSuspendRecord =
        suspendData && typeof suspendData === 'object' && !Array.isArray(suspendData)
          ? (suspendData as Record<string, unknown>)
          : undefined;

      // A model-generated resume happens in a new workflow run, so its workflow
      // suspendData cannot authenticate the earlier call. Read only the exact original
      // call ID from persisted assistant metadata; never fall back by tool name.
      const getStoredSuspendRecord = (originalToolCallId: string): Record<string, unknown> | undefined => {
        if (!messageList) return undefined;
        const assistantMessages = [...messageList.get.all.db()]
          .reverse()
          .filter(message => message.role === 'assistant');
        for (const message of assistantMessages) {
          const metadata =
            typeof message.content.metadata === 'object' && message.content.metadata !== null
              ? (message.content.metadata as Record<string, any>)
              : undefined;
          for (const metadataKey of ['pendingToolApprovals', 'suspendedTools'] as const) {
            const entry = metadata?.[metadataKey]?.[originalToolCallId];
            if (entry && typeof entry === 'object' && !Array.isArray(entry)) {
              return entry as Record<string, unknown>;
            }
          }

          const part = message.content.parts?.find(candidate => {
            if (!('data' in candidate)) return false;
            return (
              (candidate.type === 'data-tool-call-approval' || candidate.type === 'data-tool-call-suspended') &&
              (candidate.data as { toolCallId?: unknown; resumed?: unknown }).toolCallId === originalToolCallId &&
              !(candidate.data as { resumed?: unknown }).resumed
            );
          });
          const partData = part && 'data' in part ? part.data : undefined;
          if (partData && typeof partData === 'object' && !Array.isArray(partData)) {
            return partData as Record<string, unknown>;
          }
        }
        return undefined;
      };

      // Upstream #21729 / #24258: `resumeData` is an always-exposed optional
      // field on generated agent/workflow tool schemas, so models fill it on a
      // fresh delegation. With no suspension anywhere (no workflow resume data,
      // no authoritative envelope, no stored suspension metadata), no
      // model-claimed suspended coordinates and no approval-shaped payload,
      // there is nothing to resume: drop the payload and run the delegation
      // fresh. Every other shape stays on the fail-closed evidence checks
      // below (fork PF-1703), so no identity or grant is ever accepted from it.
      // Mirrors loop/workflows/agentic-execution/tool-call-step.ts.
      if (
        isFreshTurnResume &&
        (toolName?.startsWith('agent-') || toolName?.startsWith('workflow-')) &&
        workflowResumeData === undefined &&
        workflowSuspendRecord === undefined &&
        !modelClaimedSuspendedIdentity &&
        getStoredSuspendRecord(toolCallId) === undefined &&
        parseToolApprovalDecision(resumeDataFromArgs) === undefined
      ) {
        resumeDataFromArgs = undefined;
        resumeData = undefined;
        isFreshTurnResume = false;
      }
      const storedSuspendRecord =
        isFreshTurnResume && suspendedToolCallId ? getStoredSuspendRecord(suspendedToolCallId) : undefined;
      const suspendRecord = isFreshTurnResume ? storedSuspendRecord : workflowSuspendRecord;
      const suspensionType = suspendRecord?.type;
      const hasKnownSuspendType = suspensionType === 'approval' || suspensionType === 'suspension';
      const hasCommonSuspendIdentity =
        suspendRecord !== undefined &&
        suspendRecord.version === 1 &&
        suspendRecord.stepId === DurableStepIds.TOOL_CALL &&
        suspendRecord.toolCallId === metadataToolCallId &&
        suspendRecord.toolName === toolName;
      const hasMatchingFreshTurnIdentity =
        isFreshTurnResume &&
        suspendedToolCallId !== undefined &&
        suppliedSuspendedToolRunId !== undefined &&
        hasCommonSuspendIdentity &&
        suspendRecord?.originRunId === suppliedSuspendedToolRunId &&
        suspendRecord.runId === suppliedSuspendedToolRunId &&
        typeof suspendRecord.iterationCount === 'number' &&
        Number.isInteger(suspendRecord.iterationCount) &&
        suspendRecord.iterationCount >= 0 &&
        suspendRecord.identityDigest === resumeIdentityDigest;
      const hasMatchingWorkflowIdentity =
        !isFreshTurnResume &&
        hasCommonSuspendIdentity &&
        suspendRecord?.runId === runId &&
        suspendRecord.iterationCount === iterationCount &&
        suspendRecord.identityDigest === identityDigest;
      const hasMatchingSuspendIdentity = hasMatchingFreshTurnIdentity || hasMatchingWorkflowIdentity;
      const hasResumeAttempt =
        isFreshTurnResume || workflowResumeData !== undefined || workflowSuspendRecord !== undefined;
      const hasInvalidSuspendEnvelope = hasResumeAttempt && (!hasKnownSuspendType || !hasMatchingSuspendIdentity);
      const isAuthenticatedResume = hasMatchingSuspendIdentity;
      const isApprovalResume = suspensionType === 'approval' && hasMatchingSuspendIdentity;
      const approvalDecision = parseToolApprovalDecision(resumeData);
      const hasValidApprovalDecision = isApprovalResume && approvalDecision !== undefined;
      const isToolExecutionApprovalResume =
        hasValidApprovalDecision && suspendRecord?.approvalSource === 'tool-execution';
      const persistedApprovalGrant =
        suspensionType === 'suspension' && hasMatchingSuspendIdentity
          ? parseToolApprovalGrant(suspendRecord?.approval, metadataToolCallId)
          : undefined;

      // 2. Approval policy input. Prefer the live policy on the in-process
      //    registry (which preserves the function form with real
      //    toolName/args); fall back to the JSON-safe boolean shadow on the
      //    serialized workflow input for cross-process engines.
      const registryRequireToolApproval = registryEntry?.requireToolApproval;
      const effectiveRequireToolApproval =
        registryRequireToolApproval !== undefined ? registryRequireToolApproval : agentOptions.requireToolApproval;
      // Preserve approval context across cross-process execution and restarts.
      const approvalRequestContext =
        registryEntry?.requestContext ?? restoreRequestContext(initData.requestContextEntries, requestContext);

      // Add suspended-tool / pending-approval metadata to the last assistant
      // message so `extractSuspendedToolsFromMessages` can detect it on the
      // next turn (autoResumeSuspendedTools) or on page-refresh resume.
      // Mirrors the regular agent's `addToolMetadata()`.
      const addToolMetadata = (opts: {
        type: 'approval' | 'suspension';
        approvalSource?: 'tool-gate' | 'tool-execution';
        approval?: { id: string; approved: boolean; reason?: string };
        resumeSchema?: string;
        suspendPayload?: unknown;
        delegatedRunId?: string;
        approvalToolName?: string;
        approvalArgs?: unknown;
      }) => {
        if (!messageList) return;
        const metadataKey = opts.type === 'suspension' ? 'suspendedTools' : 'pendingToolApprovals';
        const entry = {
          version: 1,
          originRunId: runId,
          stepId: DurableStepIds.TOOL_CALL,
          iterationCount,
          toolCallId,
          toolName: opts.approvalToolName ?? toolName,
          identityDigest,
          args: opts.approvalArgs ?? args,
          ...(opts.approvalToolName ? { parentToolName: toolName, parentArgs: args } : {}),
          type: opts.type,
          // `runId` is the outer resumable durable run. When a delegated
          // sub-agent/workflow suspends, its inner suspended run is preserved
          // separately as `delegatedRunId` so the resume leg can recover it
          // (mirrors the regular engine's tool-call-step metadata shape).
          runId,
          ...(opts.approvalSource ? { approvalSource: opts.approvalSource } : {}),
          ...(opts.approval ? { approval: opts.approval } : {}),
          ...(opts.delegatedRunId && opts.delegatedRunId !== runId ? { delegatedRunId: opts.delegatedRunId } : {}),
          ...(opts.type === 'suspension' ? { suspendPayload: opts.suspendPayload } : {}),
          ...(opts.resumeSchema ? { resumeSchema: opts.resumeSchema } : {}),
        };

        const carriesToolCall = (msg: any) =>
          msg.role === 'assistant' &&
          (msg.content?.parts ?? []).some(
            (part: any) => part?.type === 'tool-invocation' && part.toolInvocation?.toolCallId === toolCallId,
          );

        const responseMessages = messageList.get.response.db();
        const lastAssistantMessage = [...responseMessages].reverse().find(carriesToolCall);
        if (lastAssistantMessage?.content) {
          let metadata: Record<string, any>;
          if (
            typeof lastAssistantMessage.content.metadata === 'object' &&
            lastAssistantMessage.content.metadata !== null
          ) {
            metadata = lastAssistantMessage.content.metadata as Record<string, any>;
          } else {
            metadata = {};
            lastAssistantMessage.content.metadata = metadata;
          }
          metadata[metadataKey] = metadata[metadataKey] || {};
          metadata[metadataKey][toolCallId] = entry;
          return;
        }

        // The response view is empty: a sibling parallel tool call already
        // suspended and its pre-suspension flush drained the unsaved response
        // messages. Without a fallback this sibling's entry is silently lost
        // and only the first suspension survives in persisted metadata. Merge
        // the entry into the assistant message that carries this tool call via
        // updateMessageMetadataByToolCallId, which also re-marks the message
        // unsaved so the following flush persists this write too.
        const allMessages = messageList.get.all.db();
        const target = [...allMessages].reverse().find(carriesToolCall);
        if (!target?.content) {
          logger?.warn?.(
            `[DurableAgent] addToolMetadata could not find an assistant message for tool call ${toolCallId} (${toolName}); ${metadataKey} entry was not persisted.`,
          );
          return;
        }
        const existingMeta =
          typeof target.content.metadata === 'object' && target.content.metadata !== null
            ? (target.content.metadata as Record<string, any>)
            : {};
        const existingEntries = (existingMeta[metadataKey] ?? {}) as Record<string, any>;
        messageList.updateMessageMetadataByToolCallId(toolCallId, {
          [metadataKey]: { ...existingEntries, [toolCallId]: entry },
        });
      };

      const removeToolMetadata = async (
        target: { toolCallId?: string; toolName: string; runId?: string },
        type: 'suspension' | 'approval',
      ) => {
        if (!messageList) return;

        const metadataKey = type === 'suspension' ? 'suspendedTools' : 'pendingToolApprovals';
        const expectedPartType = type === 'suspension' ? 'data-tool-call-suspended' : 'data-tool-call-approval';
        const entryMatches = (entry: any, fallbackToolCallId?: string): boolean => {
          const entryToolCallId = typeof entry?.toolCallId === 'string' ? entry.toolCallId : fallbackToolCallId;
          const entryToolName = entry?.parentToolName ?? entry?.toolName;
          const entryRunId = type === 'approval' ? entry?.delegatedRunId : (entry?.delegatedRunId ?? entry?.runId);
          if (target.toolCallId) return entryToolCallId === target.toolCallId;
          return entryToolName === target.toolName && !!target.runId && entryRunId === target.runId;
        };

        const changedMessages = [];
        for (const message of messageList.get.all.db()) {
          if (message.role !== 'assistant') continue;

          let messageChanged = false;
          const metadata =
            typeof message.content.metadata === 'object' && message.content.metadata !== null
              ? (message.content.metadata as Record<string, any>)
              : undefined;
          const entries = metadata?.[metadataKey] as Record<string, any> | undefined;
          if (entries) {
            for (const [key, entry] of Object.entries(entries)) {
              if (entryMatches(entry, key)) {
                delete entries[key];
                messageChanged = true;
              }
            }
            if (Object.keys(entries).length === 0) delete metadata![metadataKey];
          }

          message.content.parts = message.content.parts?.map(part => {
            if (part.type !== expectedPartType || !entryMatches(part.data)) return part;
            if ((part.data as { resumed?: boolean }).resumed) return part;
            messageChanged = true;
            return { ...part, data: { ...(part.data as any), resumed: true } };
          });

          if (messageChanged) changedMessages.push(message);
        }

        if (changedMessages.length === 0) return;
        messageList.add(changedMessages, 'response');
        await doFlush();
      };

      // Authenticate durable resume evidence before reevaluating dynamic policy or dispatching work.
      if (hasInvalidSuspendEnvelope || (isApprovalResume && !hasValidApprovalDecision)) {
        return {
          ...typedInput,
          error: {
            name: 'DurableResumeValidationError',
            message: 'Durable tool resume evidence did not match the suspended tool call',
          },
        };
      }

      if (isApprovalResume && approvalDecision?.editedArgs !== undefined) {
        return {
          ...typedInput,
          error: {
            name: 'DurableResumeValidationError',
            message: 'Edited approval arguments are not supported by durable agents',
          },
        };
      }

      const resumeTarget = metadataToolCallId !== toolCallId ? { resumeTargetToolCallId: metadataToolCallId } : {};

      if (hasValidApprovalDecision && approvalDecision.approved === false) {
        // Remove pending-approval metadata since we're resuming with a decision.
        await removeToolMetadata({ toolCallId: metadataToolCallId, toolName }, 'approval');
        const approval = {
          id: metadataToolCallId,
          approved: false as const,
          reason: approvalDecision.reason ?? resolveDeclineReason(resumeData),
        };
        if (pubsub) {
          try {
            const deniedChunk = await applyToolPayloadTransformToChunk(
              {
                type: 'tool-output-denied' as const,
                runId,
                from: ChunkFrom.AGENT,
                payload: { toolCallId: metadataToolCallId, toolName, args, approval },
              },
              {
                policy: registryEntry?.toolPayloadTransform,
                tools: registryEntry?.tools,
                logger: logger as any,
              },
            );
            const processed = await processChunkThroughOutputProcessors(
              deniedChunk as ChunkType,
              registryEntry,
              pubsub,
              runId,
              initData.agentId,
              logger,
              messageList,
              processorObservabilityContext,
            );
            if (processed) await emitChunkEvent(pubsub, runId, processed);
          } catch (emitError) {
            logger?.warn?.(`[DurableAgent] Failed to emit tool-output-denied chunk for ${toolName}: ${emitError}`);
          }
        }
        return {
          ...typedInput,
          args,
          ...resumeTarget,
          approval,
        };
      }

      // Re-evaluate the host's per-tool policy at the side-effect boundary on
      // every attempt, including an approved resume. A durable snapshot stores
      // only `permissionPolicyRequired`; it never stores an allow/ask decision.
      // Resume must use the newly supplied RequestContext so a parked `ask` can
      // become `deny`. The original registry context is a valid fallback only
      // for a fresh in-process call, never for a resume where it may be stale.
      const requestPermissionPolicy = requestContext?.get?.(TOOL_PERMISSION_POLICY_KEY);
      const registryPermissionPolicy = !isAuthenticatedResume
        ? registryEntry?.requestContext?.get(TOOL_PERMISSION_POLICY_KEY)
        : undefined;
      const permissionPolicy =
        typeof requestPermissionPolicy === 'function'
          ? (requestPermissionPolicy as ToolPermissionPolicy)
          : typeof registryPermissionPolicy === 'function'
            ? (registryPermissionPolicy as ToolPermissionPolicy)
            : undefined;
      // The revalidation hook resolves through the same request→registry
      // fallback, INDEPENDENTLY of the policy: a hook-only configuration must
      // still find the registry-held closure when a transported context
      // arrives with functions stripped.
      const requestOnBeforeToolExecution = requestContext?.get?.(ON_BEFORE_TOOL_EXECUTION_KEY);
      const registryOnBeforeToolExecution = !isAuthenticatedResume
        ? registryEntry?.requestContext?.get(ON_BEFORE_TOOL_EXECUTION_KEY)
        : undefined;
      const onBeforeToolExecution =
        typeof requestOnBeforeToolExecution === 'function'
          ? (requestOnBeforeToolExecution as BeforeToolExecutionHook)
          : typeof registryOnBeforeToolExecution === 'function'
            ? (registryOnBeforeToolExecution as BeforeToolExecutionHook)
            : undefined;
      const permissionContext =
        typeof requestPermissionPolicy === 'function' || typeof requestOnBeforeToolExecution === 'function'
          ? requestContext
          : typeof registryPermissionPolicy === 'function' || typeof registryOnBeforeToolExecution === 'function'
            ? registryEntry?.requestContext
            : requestContext;
      const toolPermissionDecisions: ToolPermissionDecision[] = [];
      let snapshotPolicyDecision: ToolPermissionDecision | undefined;
      if (permissionPolicy) {
        try {
          snapshotPolicyDecision = normalizeToolPermissionDecision(await permissionPolicy(toolName));
          toolPermissionDecisions.push(snapshotPolicyDecision);
        } catch {
          toolPermissionDecisions.push('deny');
        }
      }
      if (resolveToolPermission) {
        try {
          toolPermissionDecisions.push(
            normalizeToolPermissionDecision(
              await resolveToolPermission({
                runId,
                agentId: initData.agentId,
                toolCallId: metadataToolCallId,
                toolName,
                args,
                requestContext,
                isResume: isAuthenticatedResume,
              }),
            ),
          );
        } catch {
          toolPermissionDecisions.push('deny');
        }
      }
      // §4.2e per-tool revalidation — the harness `sessions.onBeforeToolExecution`
      // hook is threaded on the request context (a session-bound closure, not
      // durable state, so it only survives in-process replay like the policy
      // resolver). Throwing or an unrecognized decision fails closed as `deny`.
      if (typeof onBeforeToolExecution === 'function') {
        try {
          const beforeDecision = await onBeforeToolExecution({
            toolName,
            toolCallId: metadataToolCallId,
            args,
            isResume: isAuthenticatedResume,
            policyDecision: snapshotPolicyDecision,
          });
          if (beforeDecision !== undefined && beforeDecision !== 'allow') {
            toolPermissionDecisions.push('deny');
          }
        } catch {
          toolPermissionDecisions.push('deny');
        }
      } else if (
        agentOptions.onBeforeToolExecutionRequired === true ||
        requestContext?.get?.(ON_BEFORE_TOOL_EXECUTION_REQUIRED_KEY) === true ||
        registryEntry?.requestContext?.get?.(ON_BEFORE_TOOL_EXECUTION_REQUIRED_KEY) === true
      ) {
        // A hook that was threaded at turn-build cannot be reconstructed on this
        // context (transported or restored request context, cold worker). Like
        // `permissionPolicyRequired` below, that is an authorization failure,
        // not an implicit allow.
        toolPermissionDecisions.push('deny');
      }
      // The awaited hook can span real I/O (e.g. a grant-store read). A run
      // aborted inside that window must not proceed to approval or dispatch —
      // mirror the in-flight abort outcome (an error result; the loop's abort
      // arbitration stops the iteration with finishReason 'abort').
      if (registryEntry?.abortSignal?.aborted) {
        return {
          ...typedInput,
          args,
          ...resumeTarget,
          error: serializeError(
            (registryEntry.abortSignal as AbortSignal & { reason?: unknown }).reason ??
              new DOMException('The operation was aborted.', 'AbortError'),
          ),
        };
      }
      if (
        toolPermissionDecisions.length === 0 &&
        (agentOptions.permissionPolicyRequired === true ||
          requestContext?.get?.(TOOL_PERMISSION_POLICY_REQUIRED_KEY) === true)
      ) {
        // A configured policy that cannot be reconstructed is an authorization
        // failure, not an implicit allow. This is the cold Inngest/restart seam.
        toolPermissionDecisions.push('deny');
      }
      const toolPermissionDecision = combineToolPermissionDecisions(toolPermissionDecisions);

      const yoloAutoApprove = permissionContext?.get?.('__mastra_yoloAutoApprove') === true;
      const unsupportedAskOnSuspensionResume =
        toolPermissionDecision === 'ask' &&
        !yoloAutoApprove &&
        isAuthenticatedResume &&
        !hasValidApprovalDecision &&
        persistedApprovalGrant === undefined;

      if (toolPermissionDecision === 'deny' || unsupportedAskOnSuspensionResume) {
        if (hasValidApprovalDecision) {
          await removeToolMetadata({ toolCallId: metadataToolCallId, toolName }, 'approval');
        } else if (isAuthenticatedResume && !isApprovalResume) {
          await removeToolMetadata({ toolCallId: metadataToolCallId, toolName }, 'suspension');
        }
        notifyToolDenied(permissionContext, { toolName, stage: 'action', toolCallId });
        return {
          ...typedInput,
          args,
          ...resumeTarget,
          disposition: 'denied' as const,
          result: unsupportedAskOnSuspensionResume
            ? `Tool "${toolName}" was not resumed because the session permission policy requires a new approval.`
            : `Tool "${toolName}" was denied by the session permission policy.`,
        };
      }

      // 2. Check whether a fresh tool call requires approval. An authenticated
      // resume uses its persisted decision; live permission checks above still apply.
      // Internal transport keys are filtered from the policy's requestContext view.
      const approvalRequirement = !isAuthenticatedResume
        ? await toolApprovalRequirement(tool, effectiveRequireToolApproval, args, {
            requestContext: Object.fromEntries(
              [...approvalRequestContext.entries()].filter(([key]) => key !== '__mastra_requireToolApproval'),
            ),
            // Use the same rebuilt-workspace fallback as execution (above), so
            // workspace-aware approval policies see their workspace cross-process.
            workspace,
            logger,
            toolName,
          })
        : { required: false, reasons: [] };
      const policyAsk = toolPermissionDecision === 'ask' && !yoloAutoApprove;
      const approvalReasons = [...approvalRequirement.reasons];
      if (policyAsk) approvalReasons.push('policy');
      const requiresApproval = approvalRequirement.required || policyAsk;

      // Durable execution owns a distinct approval path from the standard
      // agent loop. Keep both paths on the same invariant: schema-invalid
      // provider input is returned to the model for repair before a human is
      // asked to approve it. execute() still performs authoritative validation
      // immediately before side effects; transformed preflight data is not
      // reused because schema transforms need not be idempotent.
      if (requiresApproval && !isAuthenticatedResume && typeof tool.validateInput === 'function') {
        const preflightValidation = await tool.validateInput(args);
        if (preflightValidation.error !== undefined) {
          return {
            ...typedInput,
            args,
            result:
              preflightValidation.error instanceof Error
                ? serializeError(preflightValidation.error)
                : ensureSerializable(preflightValidation.error),
          };
        }
      }

      if (requiresApproval && !isAuthenticatedResume) {
        const resumeSchema = JSON.stringify({
          type: 'object',
          properties: {
            approved: { type: 'boolean' },
            reason: { type: 'string' },
          },
          required: ['approved'],
        });

        // Persist active goal time before exposing the approval wait.
        await stopGoalActivity({ agentId: initData.agentId, runId });

        // Emit approval chunk via PubSub (mirrors base agent's controller.enqueue).
        // Apply the tool payload transform first so display targets never see raw
        // args on the approval prompt (parity with the main loop's approval chunk).
        if (pubsub) {
          const approvalChunk = await applyToolPayloadTransformToChunk(
            {
              type: 'tool-call-approval' as const,
              runId,
              from: ChunkFrom.AGENT,
              payload: {
                version: 1 as const,
                originRunId: runId,
                stepId: DurableStepIds.TOOL_CALL,
                type: 'approval' as const,
                approvalSource: 'tool-gate' as const,
                identityDigest,
                toolCallId,
                toolName,
                args,
                resumeSchema,
                ...(approvalReasons.length > 0 ? { approvalReasons } : {}),
              },
            },
            {
              policy: registryEntry?.toolPayloadTransform,
              tools: registryEntry?.tools,
              logger: logger as any,
            },
          );
          await emitChunkEvent(pubsub, runId, approvalChunk);
        }

        // Emit suspended event for the stream adapter
        if (pubsub) {
          await emitSuspendedEvent(pubsub, runId, {
            toolCallId,
            toolName,
            args,
            identityDigest,
            type: 'approval',
            approvalSource: 'tool-gate',
            resumeSchema,
          });
        }

        // Add approval metadata to message before persisting
        addToolMetadata({ type: 'approval', approvalSource: 'tool-gate', resumeSchema });

        // Flush messages before suspension
        await doFlush();

        // End the trace's open spans as suspended before pausing.
        endSpansAsSuspended({ toolCallId, toolName, reason: 'approval' });

        // Suspend and wait for approval
        return suspend(
          {
            version: 1,
            type: 'approval',
            approvalSource: 'tool-gate',
            runId,
            iterationCount,
            stepId: DurableStepIds.TOOL_CALL,
            toolCallId,
            toolName,
            args,
            identityDigest,
            ...(approvalReasons.length > 0 ? { approvalReasons } : {}),
          },
          {
            resumeLabel: toolCallId,
          },
        );
      }

      // Remove pending-approval metadata when resuming with a validated approval
      // decision (the declined path above already removed it before returning).
      if (hasValidApprovalDecision) {
        await removeToolMetadata({ toolCallId: metadataToolCallId, toolName }, 'approval');
      }

      // Preserve approval provenance even when a dynamic approval predicate changes between
      // suspension and resume, or when the approval was requested from inside tool execution.
      const approvalGrant = hasValidApprovalDecision
        ? ({
            approval: {
              id: metadataToolCallId,
              approved: true as const,
              ...(approvalDecision.reason !== undefined ? { reason: approvalDecision.reason } : {}),
            },
          } as const)
        : persistedApprovalGrant
          ? ({ approval: persistedApprovalGrant } as const)
          : undefined;

      // Suspension provenance comes from the authenticated persisted/workflow
      // envelope, not from payload presence: `resume()` / `resume(undefined)`
      // are valid resumes too. Payload-shaped detection would let an empty
      // resumed terminal-capable tool bypass the terminal-result guard.
      const isResumingFromSuspension = suspensionType === 'suspension' && hasMatchingSuspendIdentity;

      // 3. Check for background task execution
      const bgManager = registryEntry?.backgroundTaskManager;
      const bgConfig = registryEntry?.backgroundTasksConfig;
      const toolBgConfig = (tool as any).backgroundConfig as ToolBackgroundConfig | undefined;
      const llmBgOverrides =
        typeof args === 'object' && args !== null && '_background' in args ? (args as any)._background : undefined;

      // Strip _background from args before execution (same as non-durable path)
      const cleanedArgs = { ...args };
      const isAgentTool = toolName?.startsWith('agent-');
      if ('_background' in cleanedArgs) {
        delete (cleanedArgs as any)._background;
      }

      // Parity with the regular loop (tool-call-step.ts): stamp the caller's
      // thread/resource identity onto agent-tool args so the sub-agent wrapper
      // derives `${resourceId}-${agentName}` instead of falling back to the
      // parent agent's id (issue #23903). Always overwrite — LLM-hallucinated
      // ids must not leak into sub-agents. In the durable world the scope
      // context doesn't exist; serialized workflow state is its equivalent.
      if (toolName?.startsWith('agent-') && 'prompt' in cleanedArgs) {
        cleanedArgs.threadId = state?.threadId;
        cleanedArgs.resourceId = state?.resourceId;
      }

      // The model's suspended-identity claims were extracted from rawArgs up
      // front (the early strip keeps them out of the executed args), so read
      // them from that extraction — cleanedArgs no longer carries them. This
      // mirrors upstream's cleanedArgs read, which worked there because only
      // `resumeData` was stripped early.
      const modelSuppliedSuspendedToolRunId = suppliedSuspendedToolRunId;
      const modelSuppliedSuspendedToolCallId = suspendedToolCallId;

      // Delegated identity is trusted only after it is tied to framework-persisted
      // suspension state. The suspend payload remains the primary per-tool-call source.
      const isResumableTool = toolName?.startsWith('agent-') || toolName?.startsWith('workflow-');
      const needsRunIdLookup = isResumableTool && (resumeData !== undefined || !!approvalGrant);
      // Nullish model data follows the framework resume path; false, 0, and empty strings remain valid model payloads.
      const hasModelResumeData = resumeDataFromArgs != null;
      const resolvedSuspensionIdentity: ResolvedSuspendedToolIdentity | undefined = needsRunIdLookup
        ? resolveFrameworkSuspendedToolIdentity({
            toolCallId,
            toolName,
            resumeSource: hasModelResumeData ? 'model' : 'framework',
            modelSuppliedSuspendedToolCallId: hasModelResumeData ? modelSuppliedSuspendedToolCallId : undefined,
            modelSuppliedSuspendedToolRunId: hasModelResumeData ? modelSuppliedSuspendedToolRunId : undefined,
            suspendData,
            messages: messageList?.get.all.db() ?? [],
          })
        : undefined;
      const suspendedToolRunId = resolvedSuspensionIdentity?.runId;
      // When the delegation tool is itself approval-gated, an `{ approved: true }`
      // resume is ambiguous: it can answer this step's pre-execution gate (execute
      // fresh) or a delegated approval raised mid-execution by the sub-agent. A
      // framework-resolved inner run id disambiguates the delegated approval.
      const isDelegatedApprovalResume = !!approvalGrant && !!suspendedToolRunId;
      if ((isResumingFromSuspension || isDelegatedApprovalResume) && suspendedToolRunId) {
        cleanedArgs.suspendedToolRunId = suspendedToolRunId;
      }

      if (isResumingFromSuspension) {
        const cleanupTarget = isResumableTool ? resolvedSuspensionIdentity : { toolCallId, toolName };
        if (cleanupTarget) {
          await removeToolMetadata(cleanupTarget, resolvedSuspensionIdentity?.type ?? 'suspension');
        }
      }

      // Fire onInputAvailable lifecycle hook before execution (matches non-durable path).
      if (tool && 'onInputAvailable' in tool && typeof (tool as any).onInputAvailable === 'function') {
        try {
          await (tool as any).onInputAvailable({
            toolCallId,
            input: cleanedArgs,
            messages: messageList ? messageList.get.input.aiV5.model() : [],
          });
        } catch (hookError) {
          logger?.error?.('Error calling onInputAvailable', hookError);
        }
      }

      // Execute the tool
      if (!tool.execute) {
        return {
          ...typedInput,
          args,
          ...resumeTarget,
          result: undefined,
          ...(approvalGrant ?? {}),
        };
      }

      // Rebuild the forwarded model_step span and pass it as the tool's tracing context so
      // the TOOL_CALL span nests under the LLM call (matches the non-durable path).
      const stepSpan =
        typedInput.stepSpanData && observability
          ? observability.rebuildSpan(typedInput.stepSpanData as ExportedSpan<SpanType.MODEL_STEP>)
          : undefined;
      const toolTracingContext = stepSpan ? { currentSpan: stepSpan } : undefined;

      // Track whether the tool's suspend callback was invoked so we can skip
      // emitting a spurious tool-result after tool.execute() returns (the
      // workflow engine's suspend() sets an internal flag but does not throw,
      // so execution continues past the suspend call).
      let wasSuspended = false;

      // Forward abort signal from the run registry so tools can observe
      // cancellation (mirrors the non-durable tool-call-step).
      const toolAbortSignal = registryEntry?.abortSignal;

      // Provide outputWriter so context.writer.write() / context.writer.custom()
      // emit chunks through pubsub (matching the regular agent's tool streaming).
      const outputWriter = pubsub
        ? async (chunk: any) => {
            await emitChunkEvent(pubsub, runId, chunk as ChunkType);
          }
        : undefined;

      const toolOptions = {
        toolCallId,
        messages: [],
        workspace,
        requestContext,
        mcp: registryEntry?.mcp,
        tracingContext: toolTracingContext,
        // Use the actor supplied for this workflow segment (so FGA checks inside
        // tool execution see the same actor as the non-durable Agent path). A
        // resumed segment must never recover the initial actor from serialized
        // agent options.
        actor,
        // Delegated approval decisions must also flow to the wrapper tool: it only
        // resumes the inner suspended run when resumeData is present. Likewise a
        // tool-execution approval resume forwards its decision payload to the tool.
        resumeData:
          isResumingFromSuspension || isToolExecutionApprovalResume || isDelegatedApprovalResume
            ? resumeData
            : undefined,
        suspendedToolRunId,
        // The payload this tool call suspended with (see `toolCallSuspended` below), so a
        // resumed tool can continue from its own state — mirrors the non-durable step.
        ...(isResumingFromSuspension &&
        suspendData != null &&
        typeof suspendData === 'object' &&
        'toolCallSuspended' in suspendData
          ? { suspendPayload: (suspendData as { toolCallSuspended?: unknown }).toolCallSuspended }
          : {}),
        ...(toolAbortSignal ? { abortSignal: toolAbortSignal } : {}),
        outputWriter,
        // Raw `Tool` instances resolved from the Mastra registry (the cross-process
        // fallback path) are not wrapped by CoreToolBuilder, so they only get a
        // `writer` if we construct it here — mirrors the non-durable tool-call-step.
        // Registry tools go through CoreToolBuilder, which builds its own ToolStream.
        writer: new ToolStream({ prefix: 'tool', callId: toolCallId, name: toolName, runId }, outputWriter),

        // In-execution suspend callback — allows tools to suspend mid-execution
        suspend: async (suspendPayload: any, suspendOptions?: SuspendOptions) => {
          wasSuspended = true;
          // When a delegated sub-agent requests approval, the delegation tool
          // wrapper passes its inner suspended run id via `suspendOptions.runId`
          // (see the agent-tool wrapper's `suspend(..., { runId, isAgentSuspend })`).
          // Persist it with the approval so the resume leg targets that inner
          // run instead of restarting the sub-agent from scratch.
          const delegatedRunId =
            typeof suspendOptions?.runId === 'string' && suspendOptions.runId !== runId
              ? suspendOptions.runId
              : undefined;
          if (suspendOptions?.requireToolApproval) {
            const innerApproval =
              typeof suspendOptions.requireToolApproval === 'object' && suspendOptions.requireToolApproval
                ? suspendOptions.requireToolApproval
                : typeof suspendPayload?.requireToolApproval === 'object' && suspendPayload?.requireToolApproval
                  ? suspendPayload.requireToolApproval
                  : null;

            const approvalToolName = innerApproval?.toolName ?? toolName;
            const approvalArgs = innerApproval?.args !== undefined ? innerApproval.args : args;

            // Tool is requesting approval during execution
            const approvalResumeSchema = JSON.stringify({
              type: 'object',
              properties: {
                approved: { type: 'boolean' },
                reason: { type: 'string' },
              },
              required: ['approved'],
            });

            await stopGoalActivity({ agentId: initData.agentId, runId });

            if (pubsub) {
              const approvalChunk = await applyToolPayloadTransformToChunk(
                {
                  type: 'tool-call-approval' as const,
                  runId,
                  from: ChunkFrom.AGENT,
                  payload: {
                    version: 1 as const,
                    originRunId: runId,
                    stepId: DurableStepIds.TOOL_CALL,
                    type: 'approval' as const,
                    approvalSource: 'tool-execution' as const,
                    identityDigest,
                    toolCallId,
                    toolName: approvalToolName,
                    args: approvalArgs,
                    parentToolName: toolName,
                    parentArgs: args,
                    resumeSchema: approvalResumeSchema,
                  },
                },
                {
                  policy: registryEntry?.toolPayloadTransform,
                  tools: registryEntry?.tools,
                  logger: logger as any,
                },
              );
              await emitChunkEvent(pubsub, runId, approvalChunk);
            }

            if (pubsub) {
              await emitSuspendedEvent(pubsub, runId, {
                toolCallId,
                toolName: approvalToolName,
                args: approvalArgs,
                identityDigest,
                type: 'approval',
                approvalSource: 'tool-execution',
                resumeSchema: approvalResumeSchema,
              });
            }

            // Add approval metadata to message before persisting
            addToolMetadata({
              type: 'approval',
              approvalSource: 'tool-execution',
              resumeSchema: approvalResumeSchema,
              delegatedRunId,
              approvalToolName,
              approvalArgs,
            });

            await doFlush();

            endSpansAsSuspended({ toolCallId, toolName: approvalToolName, reason: 'approval' });

            return suspend(
              {
                version: 1,
                type: 'approval',
                approvalSource: 'tool-execution',
                runId,
                iterationCount,
                stepId: DurableStepIds.TOOL_CALL,
                toolCallId,
                toolName,
                args,
                identityDigest,
                requireToolApproval: { toolCallId, toolName: approvalToolName, args: approvalArgs },
                // Persist the inner suspended run id in the workflow snapshot,
                // partitioned per tool call (resumeLabel = toolCallId), so the
                // resume leg can recover it even if message metadata is stale.
                ...(delegatedRunId ? { suspendedToolRunId: delegatedRunId } : {}),
              },
              { resumeLabel: toolCallId },
            );
          } else {
            // General tool suspension (e.g., tool calls context.agent.suspend())
            const suspendedEventData: AgentSuspendedEventData = {
              toolCallId,
              toolName,
              args,
              identityDigest,
              ...(approvalGrant ?? {}),
              suspendPayload,
              type: 'suspension',
              resumeSchema: suspendOptions?.resumeSchema,
            };

            if (pubsub) {
              const suspensionChunk = await applyToolPayloadTransformToChunk(
                {
                  type: 'tool-call-suspended' as const,
                  runId,
                  from: ChunkFrom.AGENT,
                  payload: {
                    version: 1 as const,
                    originRunId: runId,
                    stepId: DurableStepIds.TOOL_CALL,
                    type: 'suspension' as const,
                    identityDigest,
                    ...(approvalGrant ?? {}),
                    toolCallId,
                    toolName,
                    suspendPayload,
                    args,
                    resumeSchema: suspendOptions?.resumeSchema,
                  },
                },
                {
                  policy: registryEntry?.toolPayloadTransform,
                  tools: registryEntry?.tools,
                  logger: logger as any,
                },
              );
              await emitChunkEvent(pubsub, runId, suspensionChunk);

              await emitSuspendedEvent(pubsub, runId, suspendedEventData);
            }

            // Add suspension metadata to message before persisting
            addToolMetadata({
              type: 'suspension',
              approval: approvalGrant?.approval,
              suspendPayload,
              resumeSchema: suspendOptions?.resumeSchema,
              delegatedRunId,
            });

            await doFlush();

            endSpansAsSuspended({ toolCallId, toolName, reason: 'suspension' });

            return suspend(
              {
                version: 1,
                type: 'suspension',
                runId,
                iterationCount,
                stepId: DurableStepIds.TOOL_CALL,
                toolCallSuspended: suspendPayload,
                toolCallId,
                toolName,
                args,
                identityDigest,
                ...(approvalGrant ?? {}),
                resumeLabel: suspendOptions?.resumeLabel,
                // Persist the inner suspended run id in the workflow snapshot,
                // partitioned per tool call (resumeLabel = toolCallId), so the
                // resume leg continues the delegate's suspended run instead of
                // restarting it (#20496; mirrors the approval branch above).
                ...(delegatedRunId ? { suspendedToolRunId: delegatedRunId } : {}),
              },
              { resumeLabel: toolCallId },
            );
          }
        },
      };

      // Live-attempt barrier only. Durable recovery must use persisted task and
      // transcript state because this Promise does not survive workflow replay.
      let resolveReconciliation!: (outcome: { error?: unknown }) => void;
      const reconciliationComplete = new Promise<{ error?: unknown }>(resolve => {
        resolveReconciliation = resolve;
      });
      const backgroundResultMetadata = (taskId: string, status: 'running' | 'completed' | 'failed') => ({
        ...typedInput.providerMetadata,
        mastra: {
          ...(typeof typedInput.providerMetadata?.mastra === 'object' ? typedInput.providerMetadata.mastra : {}),
          backgroundTask: { taskId, status },
        },
      });
      // Background task dispatch via the shared dispatch ladder with the
      // durable policy: steps replay under at-least-once redelivery, so an
      // already-running task is restarted to reattach hooks and
      // ladder failures degrade to sync execution to preserve forward
      // progress across transport/store boundaries.
      // Fork (PF-4402): the authenticated resume gate for background dispatch.
      // Mirrors tool-call.ts.fork's `isSuspendedBgResume`: a general suspension
      // resume or an in-tool approval resume may reattach to (or wake) the
      // suspended background task; anything else dispatches fresh work.
      const isSuspendedBackgroundResume = isResumingFromSuspension || isToolExecutionApprovalResume;
      const bgOutcome = await dispatchBackgroundTool({
        existingRunningTask: 'restart',
        dispatchFailure: 'fallback-to-sync',
        backgroundTaskManager: bgManager,
        agentBackgroundConfig: bgConfig,
        managerConfig: bgManager?.config,
        toolBackgroundConfig: toolBgConfig,
        llmBgOverrides,
        args: cleanedArgs,
        toolName,
        toolCallId,
        agentId: initData.agentId,
        threadId: state?.threadId,
        resourceId: state?.resourceId,
        runId,
        // Only a resume of a previously-suspended call may reattach to a
        // suspended background task; a fresh call must dispatch its own.
        // Fork (PF-4402): an in-tool approval resume reattaches the same way —
        // the approval decision IS the resume payload for a tool-execution
        // suspension ("preserves the grant" contract).
        resumeData: isSuspendedBackgroundResume ? resumeData : undefined,
        // Carry the authenticated resume intent separately from the payload:
        // `resume(undefined)` (a user resume with no data) is a valid resume
        // and must still wake the suspended task instead of dispatching a
        // duplicate or silently reattaching. Payload presence alone cannot
        // discriminate (fork contract, mirrors tool-call.ts.fork).
        isAuthenticatedResume: isSuspendedBackgroundResume,
        logger: logger as any,
        adoptPersistedTask: true,
        emitTaskStarted: async task => {
          // Emit background-task-started chunk via PubSub
          if (pubsub) {
            await emitChunkEvent(pubsub, runId, {
              type: 'background-task-started' as any,
              runId,
              from: ChunkFrom.AGENT,
              payload: {
                taskId: task.id,
                toolName,
                toolCallId,
              },
            });
          }
        },
        taskContext: info => ({
          executor: {
            execute: async (taskArgs: any, taskContext: any) => {
              const taskId = info.getTaskId()!;
              const execution = await executeAdoptedBackgroundOperation({
                taskId,
                disposition: info.disposition === 'awaited' ? 'awaited' : 'deferred',
                abortSignal: taskContext?.abortSignal ?? toolAbortSignal,
                execute: async background => {
                  // Fork (PF-4402): every attempt is a fresh side-effect
                  // boundary — the gate above ran only before dispatch, so
                  // retries must revalidate or a mid-flight grant revocation
                  // never takes effect. A denial throws
                  // TOOL_PERMISSION_DENIED_ERROR_NAME, which the bg-task
                  // workflow classifies as non-retryable.
                  if (typeof onBeforeToolExecution === 'function') {
                    let attemptDecision: 'allow' | 'deny' | void;
                    try {
                      attemptDecision = await onBeforeToolExecution({
                        toolName,
                        toolCallId: metadataToolCallId,
                        args: taskArgs,
                        isResume: taskContext?.resumeData !== undefined || isAuthenticatedResume,
                        policyDecision: snapshotPolicyDecision,
                      });
                    } catch {
                      attemptDecision = 'deny';
                    }
                    if (registryEntry?.abortSignal?.aborted || taskContext?.abortSignal?.aborted) {
                      throw (
                        (
                          (taskContext?.abortSignal ?? registryEntry?.abortSignal) as
                            | (AbortSignal & { reason?: unknown })
                            | undefined
                        )?.reason ?? new DOMException('The operation was aborted.', 'AbortError')
                      );
                    }
                    if (attemptDecision !== undefined && attemptDecision !== 'allow') {
                      notifyToolDenied(permissionContext, {
                        toolName,
                        stage: 'action',
                        toolCallId,
                      });
                      throw Object.assign(
                        new Error(`Tool "${toolName}" was denied by the pre-execution permission hook.`),
                        { name: TOOL_PERMISSION_DENIED_ERROR_NAME },
                      );
                    }
                  }
                  return tool.execute!(taskArgs, {
                    ...toolOptions,
                    isBackgroundTask: true,
                    abortSignal: taskContext?.abortSignal ?? toolAbortSignal,
                    background,
                    [BACKGROUND_WORK_CONTEXT]: {
                      originRunId: runId,
                      originToolCallId: toolCallId,
                      taskId,
                      invocationKind: isAgentTool ? 'agent' : 'tool',
                      disposition: info.disposition === 'awaited' ? 'awaited' : 'deferred',
                    },
                    ...(taskContext?.resumeData !== undefined ? { resumeData: taskContext.resumeData } : {}),
                    // Framework-resolved delegated run id recovered from persisted
                    // suspension state (#23739) — never the model-authored one.
                    suspendedToolRunId: taskContext?.suspendedToolRunId,
                    suspend: async (data?: unknown, options?: SuspendOptions) => {
                      await toolOptions.suspend?.(data, options);
                      return taskContext?.suspend?.(data, options);
                    },
                    outputWriter: async (chunk: any) => {
                      await taskContext?.onProgress?.(chunk);
                      return toolOptions.outputWriter?.(chunk);
                    },
                  } as any);
                },
                onCancelError: error => logger?.warn('Failed to cancel adopted background operation', error),
              });

              if (!execution.adopted) {
                return execution.result;
              }

              const outputValidation = validateToolOutput(
                resolveToolOutputValidationSchema(tool),
                execution.result,
                toolName,
                false,
              );
              return outputValidation.error ?? outputValidation.data;
            },
          },
          onChunk: (chunk: any) => {
            if (!pubsub) return;
            try {
              const bgRunId = chunk.payload.runId;
              // Emit tool-call chunk so UIs can render the invocation inline
              if (bgRunId !== runId || (bgRunId === runId && resumeData != null)) {
                void emitChunkEvent(pubsub, bgRunId, {
                  type: 'tool-call',
                  runId: bgRunId,
                  from: ChunkFrom.AGENT,
                  payload: {
                    toolCallId: chunk.payload.toolCallId,
                    toolName: chunk.payload.toolName,
                    args: cleanedArgs,
                    title: getToolTitle(tool),
                  },
                });
              }

              if (chunk.type === 'background-task-completed') {
                void emitChunkEvent(pubsub, bgRunId, {
                  type: 'tool-result',
                  runId: bgRunId,
                  from: ChunkFrom.AGENT,
                  payload: {
                    toolCallId: chunk.payload.toolCallId,
                    toolName: chunk.payload.toolName,
                    args: cleanedArgs,
                    result: chunk.payload.result,
                    providerMetadata: backgroundResultMetadata(chunk.payload.taskId, 'completed'),
                  },
                });
              } else if (chunk.type === 'background-task-failed') {
                void emitChunkEvent(pubsub, bgRunId, {
                  type: 'tool-error',
                  runId: bgRunId,
                  from: ChunkFrom.AGENT,
                  payload: {
                    toolCallId: chunk.payload.toolCallId,
                    toolName: chunk.payload.toolName,
                    error: chunk.payload.error,
                    args: cleanedArgs,
                    providerMetadata: backgroundResultMetadata(chunk.payload.taskId, 'failed'),
                  },
                });
              }
            } catch {
              // PubSub may be closed — ignore
            }
          },

          onResult: async (params: any) => {
            if (!messageList) {
              if (info.disposition === 'awaited') {
                const error = new Error('Cannot reconcile an awaited background task without a message list');
                resolveReconciliation({ error });
                throw error;
              }
              return;
            }

            try {
              // Resolve the mapping tool at completion time: the registry entry
              // may have been rebuilt (or expired) if the task finished after a
              // process restart.
              const liveEntry = globalRunRegistry.get(runId);
              const mappingTool = liveEntry?.tools?.[toolName] ?? tool;
              await applyBackgroundToolResult({
                params,
                currentRunId: runId,
                hasResumeData: resumeData != null,
                args: cleanedArgs,
                messageList,
                approvalGrant: approvalGrant as Record<string, unknown> | undefined,
                baseProviderMetadata: typedInput.providerMetadata as any,
                // Transcript payload transforms (L22 parity port). The policy and
                // tool-level transform are resolved at completion time from the
                // live registry — NOT captured at dispatch — because the entry may
                // be rebuilt after a process restart. The run-level policy carries
                // a closure and cannot be rehydrated across restarts (only
                // tool-level transforms survive via registry re-resolution) — a
                // limitation shared with the sync tool-call path.
                transformForTranscript: async result => {
                  const failed = params.status === 'failed';
                  const transformCarrier = await applyToolPayloadTransformToChunk(
                    {
                      type: failed ? 'tool-error' : 'tool-result',
                      payload: {
                        toolCallId: params.toolCallId,
                        toolName: params.toolName,
                        args: cleanedArgs,
                        ...(failed ? { error: params.error } : { result: params.result }),
                      },
                      metadata: {} as Record<string, any>,
                    },
                    {
                      policy: liveEntry?.toolPayloadTransform,
                      toolTransform: (mappingTool as { transform?: any })?.transform,
                      tools: liveEntry?.tools,
                      logger: logger as any,
                      transformInput: {
                        providerMetadata: typedInput.providerMetadata as Record<string, unknown> | undefined,
                      },
                    },
                  );
                  const transcriptArgsTransform = getTransformedToolPayload(
                    transformCarrier.metadata,
                    'transcript',
                    'input-available',
                  );
                  const transcriptResultTransform = getTransformedToolPayload(
                    transformCarrier.metadata,
                    'transcript',
                    failed ? 'error' : 'output-available',
                  );
                  return {
                    transcriptArgs: hasTransformedToolPayload(transcriptArgsTransform)
                      ? transcriptArgsTransform.transformed
                      : cleanedArgs,
                    transcriptResult: hasTransformedToolPayload(transcriptResultTransform)
                      ? transcriptResultTransform.transformed
                      : result,
                    providerMetadata: withToolPayloadTransformProviderMetadata(
                      typedInput.providerMetadata as any,
                      transformCarrier.metadata,
                    ) as any,
                  };
                },
                toModelOutput: mappingTool.toModelOutput,
                // Respect a custom idGenerator for the fallback appended message —
                // parity with main, which reads generateId from its run scope.
                generateId: mastra ? () => (mastra as Mastra).generateId() : undefined,
                logger: logger as any,
                flush: async () => {
                  if (saveQueueManager && state?.threadId && !state?.memoryConfig?.readOnly) {
                    await saveQueueManager.flushMessages(messageList, state.threadId, state.memoryConfig);
                  }
                },
              });
              resolveReconciliation({});
            } catch (error) {
              resolveReconciliation({ error });
              throw error;
            }
          },

          onExecution: async (params: any) => {
            if (!messageList) return;

            messageList.updateMessageMetadataByToolCallId(params.toolCallId, {
              mode: 'stream',
              backgroundTasks: {
                [params.toolCallId]: {
                  startedAt: params.startedAt,
                  suspendedAt: params.suspendedAt,
                  taskId: params.taskId,
                },
              },
            });

            // Flush to storage so the metadata update (especially suspendedAt)
            // is persisted. Unlike the regular agent which has a single long-lived
            // messageList, the durable agent's workflow state is serialized before
            // this async callback fires, so we must flush directly.
            if (saveQueueManager && state?.threadId && !state?.memoryConfig?.readOnly) {
              await saveQueueManager.flushMessages(messageList, state.threadId, state.memoryConfig);
            }
          },

          onComplete: toolBgConfig?.onComplete ?? bgConfig?.onTaskComplete,
          onFailed: toolBgConfig?.onFailed ?? bgConfig?.onTaskFailed,
        }),
        // Fork (PF-4402): the hook closure cannot survive cross-process dispatch
        // or cold recovery — persist the requirement so a statically-resolved
        // executor fails closed instead of skipping revalidation.
        requiresToolPermissionHook: typeof onBeforeToolExecution === 'function',
      });

      if (bgOutcome.status !== 'sync') {
        if (bgOutcome.disposition === 'awaited') {
          const completedTask = await bgOutcome.waitForCompletion({
            abortSignal: toolAbortSignal,
            includeSuspended: true,
          });
          if (completedTask.status === 'suspended') {
            // The task's executor requests parent suspension through the
            // native suspend callback before the background workflow records
            // its own suspended state; the flag also covers a resumed caller
            // observing an already-suspended task before execution. Mirror the
            // plain tool path: return through the durable native suspension
            // path before terminal reconciliation or the non-completed error
            // below. The wasSuspended guard avoids duplicate suspension
            // emission when this execution already suspended live; the payload
            // stays the task's authenticated suspend payload with the approval
            // provenance, resume label (toolCallId), and task identity intact.
            if (!wasSuspended) {
              await toolOptions.suspend?.(completedTask.suspendPayload, undefined);
            }
            return {
              ...typedInput,
              args: cleanedArgs,
              result: bgOutcome.placeholder,
              providerMetadata: backgroundResultMetadata(bgOutcome.taskId, 'running'),
              ...(approvalGrant ?? {}),
            };
          }
          // Cancellation deregisters the task context without calling onResult, so there is no reconciliation to await.
          if (completedTask.status !== 'cancelled') {
            const reconciliation = await reconciliationComplete;
            if (reconciliation.error) {
              throw reconciliation.error;
            }
          }

          if (completedTask.status !== 'completed') {
            throw new Error(
              completedTask.error?.message ??
                `Background task ${completedTask.status.replace('_', ' ')}: ${completedTask.id}`,
            );
          }

          return {
            ...typedInput,
            args: cleanedArgs,
            result: completedTask.result,
            providerMetadata: backgroundResultMetadata(bgOutcome.taskId, 'completed'),
            // Fork (PF-4402): the authenticated grant must survive every awaited
            // outcome, not just a fresh `started` dispatch. A resumed/restarted/
            // reattached leg that completes here still needs the grant on this
            // record — llm-mapping deserializes the pre-tool LLM snapshot and
            // commits approval from `toolResult.approval`, so omitting it loses
            // the approval provenance on recall.
            ...(approvalGrant ?? {}),
          };
        }

        if (bgOutcome.status === 'started') {
          // Return placeholder result so the LLM can continue
          return {
            ...typedInput,
            args: cleanedArgs,
            result: bgOutcome.placeholder,
            providerMetadata: backgroundResultMetadata(bgOutcome.taskId, 'running'),
            ...(approvalGrant ?? {}),
          };
        }
        return {
          ...typedInput,
          args: cleanedArgs,
          result: bgOutcome.placeholder,
          providerMetadata: backgroundResultMetadata(bgOutcome.taskId, 'running'),
          // Fork (PF-4402): an in-tool approval resume must keep its grant on
          // the tool result (fork's inline resume path spread the grant; the
          // "preserves the grant" contract).
          ...(approvalGrant ?? {}),
        };
      }

      // Read-and-clear the delegation bail signal (`ctx.bail()` from an
      // onDelegationComplete hook). The sub-agent tool wrapper writes the
      // flag by-reference to the RequestContext instance captured when the
      // tool was BUILT — the registry's live instance in-process, or the
      // context restored by rebuildRunToolsFromMastra cross-process. On the
      // evented engine every step rehydrates its own RequestContext copy from
      // its event payload, so that write never reaches the llm-mapping step's
      // instance and bail used to cost one extra LLM turn (G3). Consuming the
      // flag here — same process and same instances as tool execution — and
      // carrying it on the serializable step output stops the loop in the
      // same iteration on every engine.
      const consumeDelegationBailSignal = (): boolean => {
        let bailed = false;
        for (const rc of [registryEntry?.requestContext, rebuiltRequestContext, requestContext]) {
          if (rc?.get('__mastra_delegationBailed')) {
            bailed = true;
            rc.set('__mastra_delegationBailed', false);
          }
        }
        return bailed;
      };

      try {
        const outcome = await executeToolCall({
          tool: tool as any,
          args: cleanedArgs,
          toolOptions,
          toolCallId,
          toolName,
          abortSignal: toolAbortSignal,
          // Run-activity tracking brackets live execution (durable-only bookkeeping).
          acquireExecution: () => markRunActive(runId),
          logger: logger as any,
        });

        if (outcome.status === 'aborted') {
          // Mid-flight cancellation: leave the call incomplete (no result/error,
          // no chunk emission) so the mapping step doesn't fake-complete it on
          // resume. Mirrors the non-durable tool-call step.
          return {
            ...typedInput,
            aborted: true,
          };
        }

        if (outcome.status === 'error') {
          // Route through the catch below so error serialization and the
          // tool-error chunk emission stay on the single existing path.
          throw outcome.error;
        }

        let result = outcome.result;

        // Compute model-facing output while invocation-scoped execution metadata is still available.
        // Durable step outputs are serialized before the LLM mapping step, which strips symbols and
        // other non-JSON side channels used by tools such as MCP structured-output tools. Map from
        // the raw pre-serialization result for the same reason.
        let providerMetadata = typedInput.providerMetadata as ProviderMetadata | undefined;
        let modelOutputComputed: boolean | undefined;
        const mappingTool = globalRunRegistry.get(runId)?.tools?.[toolName] ?? tool;
        const toModelOutput = mappingTool.toModelOutput;
        if (toModelOutput) {
          modelOutputComputed = true;
          const mappingSpan = stepSpan?.createChildSpan({
            type: SpanType.MAPPING,
            name: `tool output mapping: '${toolName}'`,
            entityType: EntityType.TOOL,
            entityId: toolName,
            entityName: toolName,
            input: outcome.rawResult,
            attributes: {
              mappingType: 'toModelOutput',
              toolCallId,
            },
          });
          try {
            const modelOutput = normalizeModelOutput(await toModelOutput(outcome.rawResult));
            mappingSpan?.end({ output: modelOutput });

            if (modelOutput != null) {
              const existingMastra = (providerMetadata as any)?.mastra;
              providerMetadata = {
                ...providerMetadata,
                mastra: { ...existingMastra, modelOutput },
              };
            }
          } catch (mappingError) {
            mappingSpan?.error({ error: mappingError as Error, endSpan: true });
            logger?.warn?.(`[DurableAgent] toModelOutput failed for tool "${toolName}": ${mappingError}`);
          }
        }

        // Run processToolResult hooks before the tool-result chunk is emitted.
        // In this engine subscribers receive tool-result chunks HERE, at
        // tool-call time — running the hook later in llm-mapping would protect
        // only the transcript after the raw value had already reached the
        // stream. Processors mutate via messageList.updateToolInvocation, but
        // llm-mapping re-derives the transcript from the llm-execution snapshot
        // plus the serialized step outputs, so the processed value must travel
        // through the returned `result` field. Requires the live in-process
        // registry (processor states are unserializable) — a cross-process
        // resume skips, same as the chunk pipeline below.
        if (!wasSuspended && registryEntry?.outputProcessors?.length && registryEntry.processorStates && messageList) {
          const resultProcessorRunner = new ProcessorRunner({
            inputProcessors: [],
            outputProcessors: registryEntry.outputProcessors,
            logger,
            agentName: initData.agentId,
            processorStates: registryEntry.processorStates,
          });
          try {
            await resultProcessorRunner.runProcessToolResult({
              // The accumulated StepResult[] is not reconstructable at
              // tool-call time in this engine (only serialized iteration state
              // exists), so hooks that inspect prior steps see an empty array.
              steps: [],
              stepNumber: 0,
              messages: messageList.get.all.db(),
              messageList,
              toolName,
              toolCallId,
              toolArgs: cleanedArgs,
              result,
              ...(processorObservabilityContext ?? {}),
              requestContext: registryEntry.requestContext,
              retryCount: 0,
              writer: pubsub
                ? {
                    custom: async (
                      data: { type: string; data?: unknown; transient?: boolean },
                      writerOptions?: { messageId?: string },
                    ) => {
                      if (data.type.startsWith('data-') && !data.transient) {
                        collectProcessorDataPart({
                          type: data.type,
                          data: data.data,
                          messageId: writerOptions?.messageId,
                        });
                      }
                      await emitChunkEvent(pubsub, runId, data as ChunkType);
                    },
                  }
                : undefined,
              abortSignal: toolAbortSignal,
            });
            // Sync any processor mutation back so the emitted chunk and the
            // serialized step output both carry the post-processor value.
            const postProcessorResult = readToolResultFromMessageList(messageList, toolCallId);
            if (postProcessorResult !== undefined && postProcessorResult !== result) {
              result = postProcessorResult;
            }
          } catch (processorError) {
            if (processorError instanceof TripWire) {
              // Blocked: emit a tripwire chunk instead of the tool-result and
              // leave the call incomplete (no result). llm-mapping skips
              // `resultBlocked` entries the way it skips `aborted` ones, so
              // the invocation stays in 'call' state — mirroring the main
              // loop, where a tripwire skips both commit and emission.
              if (pubsub) {
                try {
                  await emitChunkEvent(pubsub, runId, {
                    type: 'tripwire',
                    runId,
                    from: ChunkFrom.AGENT,
                    payload: {
                      reason: processorError.message || 'Tool result blocked by processor',
                      retry: processorError.options?.retry,
                      metadata: processorError.options?.metadata,
                      processorId: processorError.processorId,
                    },
                  } as ChunkType);
                } catch (emitError) {
                  logger?.warn?.(`[DurableAgent] Failed to emit tripwire chunk for ${toolName}: ${emitError}`);
                }
              }
              return {
                ...typedInput,
                resultBlocked: true,
                ...(processorDataParts.length ? { processorDataParts } : {}),
              };
            }
            // A non-tripwire processor failure must not kill the run in this
            // engine (run and stream lifecycles are decoupled) — but it must
            // fail closed: continuing with the raw result would leak the
            // unprocessed value past a throwing redaction processor. The
            // regular loop rethrows here (runToolResultProcessors), so no
            // engine emits or persists the raw value; this engine substitutes
            // an error placeholder for both emission and persistence and
            // keeps the run alive.
            logger?.warn?.(`[DurableAgent] processToolResult failed for tool "${toolName}": ${processorError}`);
            result = { error: 'Tool result processing failed' };
          }
        }

        // Emit tool-result chunk (non-fatal — result is returned regardless).
        // Skip emission when the tool called suspend() — the workflow engine's
        // suspend() sets a flag but does NOT throw, so execution continues past
        // the suspend call and tool.execute() returns undefined. Emitting a
        // tool-result with undefined would produce a spurious entry that
        // confuses downstream consumers (e.g. MastraModelOutput.toolResults).
        let transformMetadata: DurableToolCallOutput['transformMetadata'];
        if (pubsub && !wasSuspended) {
          try {
            const resultChunk = await applyToolPayloadTransformToChunk(
              {
                type: 'tool-result' as const,
                runId,
                from: ChunkFrom.AGENT,
                payload: { toolCallId, toolName, args, result, providerMetadata },
              },
              {
                policy: registryEntry?.toolPayloadTransform,
                tools: registryEntry?.tools,
                logger: logger as any,
              },
            );
            // Capture the transform metadata for the step output (L18b) —
            // this step's messageList is a local copy, so llm-mapping layers
            // it into the persisted providerMetadata from the output record.
            transformMetadata = (resultChunk as { metadata?: Record<string, any> })
              .metadata as DurableToolCallOutput['transformMetadata'];
            // Runs through output processors (tripwire/blocking/redaction) and emits
            await processChunkThroughOutputProcessors(
              resultChunk,
              registryEntry,
              pubsub,
              runId,
              initData.agentId,
              logger,
              messageList,
              processorObservabilityContext,
              collectProcessorDataPart,
            );
          } catch (emitError) {
            logger?.warn?.(`[DurableAgent] Failed to emit tool-result chunk for ${toolName}: ${emitError}`);
          }
        }

        return {
          ...typedInput,
          args,
          ...resumeTarget,
          providerMetadata,
          result,
          ...(!wasSuspended ? { serverExecuted: true } : {}),
          modelOutputComputed,
          ...(isResumingFromSuspension ? { resumedFromSuspension: true as const } : {}),
          ...(approvalGrant ?? {}),
          ...(processorDataParts.length ? { processorDataParts } : {}),
          ...(transformMetadata ? { transformMetadata } : {}),
          ...(consumeDelegationBailSignal() ? { delegationBailed: true } : {}),
        };
      } catch (error) {
        // Re-throw FGA authorization errors instead of swallowing them —
        // an authorization denial must fail the run, not be serialized as a
        // recoverable tool error for the LLM to retry (mirrors the
        // non-durable tool-call step).
        if (error instanceof Error && error.name === 'FGADeniedError') {
          throw error;
        }
        const toolError = serializeError(error);
        const delegationBailed =
          requestContext?.get('__mastra_delegationBailed') === true ||
          registryEntry?.requestContext?.get('__mastra_delegationBailed') === true;

        // Emit tool-error chunk (non-fatal — error result is returned regardless)
        let errorTransformMetadata: DurableToolCallOutput['transformMetadata'];
        if (pubsub && !wasSuspended) {
          try {
            const errorChunk = await applyToolPayloadTransformToChunk(
              {
                type: 'tool-error' as const,
                runId,
                from: ChunkFrom.AGENT,
                payload: { toolCallId, toolName, args, error: toolError },
              },
              {
                policy: registryEntry?.toolPayloadTransform,
                tools: registryEntry?.tools,
                logger: logger as any,
              },
            );
            // Capture the transform metadata for the step output (L18b) — see
            // the tool-result path above.
            errorTransformMetadata = (errorChunk as { metadata?: Record<string, any> })
              .metadata as DurableToolCallOutput['transformMetadata'];
            // Runs through output processors (tripwire/blocking/redaction) and emits
            await processChunkThroughOutputProcessors(
              errorChunk,
              registryEntry,
              pubsub,
              runId,
              initData.agentId,
              logger,
              messageList,
              processorObservabilityContext,
              collectProcessorDataPart,
            );
          } catch (emitError) {
            logger?.warn?.(`[DurableAgent] Failed to emit tool-error chunk for ${toolName}: ${emitError}`);
          }
        }

        return {
          ...typedInput,
          args,
          ...resumeTarget,
          error: toolError,
          ...(delegationBailed ? { delegationBailed: true } : {}),
          ...(isResumingFromSuspension ? { resumedFromSuspension: true as const } : {}),
          ...(approvalGrant ?? {}),
          ...(processorDataParts.length ? { processorDataParts } : {}),
          ...(errorTransformMetadata ? { transformMetadata: errorTransformMetadata } : {}),
          // A hook may bail on a FAILED delegation too (success: false).
          ...(consumeDelegationBailSignal() ? { delegationBailed: true } : {}),
        };
      }
    },
  });
}
