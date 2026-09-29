import type { ToolSet } from '@internal/ai-sdk-v5';
import { z } from 'zod/v4';
import { stopGoalActivity } from '../../../agent/goal';
import { resolveDeclineReason } from '../../../agent/tool-approval';
import {
  createToolCallIdentityDigest,
  parseToolApprovalDecision,
  parseToolApprovalGrant,
  toolApprovalEditedArgsSchema,
} from '../../../agent/tool-call-identity';
import type { ToolApprovalGrant } from '../../../agent/tool-call-identity';
import {
  ON_BEFORE_TOOL_EXECUTION_KEY,
  ON_BEFORE_TOOL_EXECUTION_REQUIRED_KEY,
  TOOL_PERMISSION_DENIED_ERROR_NAME,
  TOOL_PERMISSION_POLICY_KEY,
  type BeforeToolExecutionHook,
  type ToolPermissionPolicy,
} from '../../../agent/tool-permission-prefilter';
import { MastraFGAPermissions } from '../../../auth/ee';
import { executeAdoptedBackgroundOperation } from '../../../background-tasks/adoption';
import type { BackgroundTaskProgressChunk, ToolBackgroundConfig } from '../../../background-tasks/types';
import type { MastraDBMessage } from '../../../memory';
import { BACKGROUND_WORK_CONTEXT, notifyBackgroundWorkTerminal } from '../../../processors/background-work-signals';
import { RequestContext } from '../../../request-context';
import { toStandardSchema, standardSchemaToJSONSchema } from '../../../schema';
import { safeEnqueue } from '../../../stream/base';
import { ChunkFrom } from '../../../stream/types';
import type { ChunkType, ProviderMetadata } from '../../../stream/types';
import { resolveToolApprovalRequirement } from '../../../tools/approval';
import {
  getTransformedToolPayload,
  hasTransformedToolPayload,
  transformToolPayloadForTargets,
  withToolPayloadTransformMetadata,
  withToolPayloadTransformProviderMetadata,
} from '../../../tools/payload-transform';
import { findProviderToolByName } from '../../../tools/provider-tool-utils';
import { getToolTitle } from '../../../tools/tool-title';
import type { MastraToolInvocationOptions, RequireToolApproval } from '../../../tools/types';
import { resolveToolOutputValidationSchema, validateToolOutput } from '../../../tools/validation';
import { ensureSerializable } from '../../../utils';
import type { InnerOutput, SuspendOptions } from '../../../workflows/step';
import { createStep } from '../../../workflows/workflow';
import type { RunScopeContext } from '../../run-scope-access';
import { readScoped, writeScoped } from '../../run-scope-access';
import {
  AGENT_BACKGROUND_CONFIG_KEY,
  BACKGROUND_TASK_MANAGER_CONFIG_KEY,
  BACKGROUND_TASK_MANAGER_KEY,
  EDITED_APPROVAL_RESUME_LOADER_KEY,
  EAGER_TOOL_EXECUTION_KEY,
  GENERATE_ID_KEY,
  MEMORY_CONFIG_KEY,
  MEMORY_KEY,
  NOW_KEY,
  RESOURCE_ID_KEY,
  SAVE_QUEUE_MANAGER_KEY,
  STEP_ACTIVE_TOOLS_KEY,
  STEP_MODEL_MESSAGES_KEY,
  STEP_TOOLS_KEY,
  STEP_WORKSPACE_KEY,
  THREAD_EXISTS_KEY,
  THREAD_ID_KEY,
  TOOL_APPROVAL_VERDICTS_KEY,
  TOOL_PAYLOAD_TRANSFORM_KEY,
} from '../../run-scope-keys';
import { dispatchBackgroundTool } from '../../shared/steps/background-dispatch-core';
import { applyBackgroundToolResult } from '../../shared/steps/background-task-result-core';
import { executeToolCall } from '../../shared/steps/execute-tool-core';
import { resolveFrameworkSuspendedToolIdentity } from '../../shared/suspended-tool-run-id';
import type { ResolvedSuspendedToolIdentity } from '../../shared/suspended-tool-run-id';
import { applyToolPayloadTransformToChunk } from '../../shared/tool-payload-transform';
import { raceAgainstAbort } from '../../timeout';
import type { OuterLLMRun } from '../../types';
import { serializeToolError, ToolNotFoundError } from '../errors';
import { toolCallInputSchema, toolCallOutputSchema } from '../schema';
import {
  EAGER_TOOL_ABORT_SIGNAL,
  EAGER_TOOL_BAILOUT,
  EAGER_TOOL_EXECUTION_MARKER,
  eagerToolCallAlreadyAnnouncedInput,
  EagerToolExecutionNotRun,
  eagerToolCallDidNotExecute,
  eagerToolCallSuspensionIntent,
} from './eager-tool-execution';
import type { EagerSuspensionIntent, EagerToolBailout } from './eager-tool-execution';
import { notifyToolDenied, TOOL_DENIED_CALLBACK_KEY, type ToolDeniedCallback } from './tool-permission-notify';

type AddToolMetadataOptions = {
  toolCallId: string;
  toolName: string;
  args: unknown;
  parentToolName?: string;
  parentArgs?: unknown;
  resumeSchema: string;
  suspendedToolRunId?: string;
  approval?: ToolApprovalGrant;
  approvalSource?: 'tool-gate' | 'tool-execution';
  approvalInputIdentityDigest?: string;
  approvedArgs?: Record<string, unknown>;
  metadata?: Record<string, unknown>;
} & (
  | {
      type: 'approval';
      suspendPayload?: never;
    }
  | {
      type: 'suspension';
      suspendPayload: unknown;
    }
);

type HarnessToolContextSlot = {
  registerQuestion?: (params: Record<string, unknown>) => Promise<void>;
  registerPlanApproval?: (params: Record<string, unknown>) => Promise<void>;
};

function buildToolRequestContext(
  requestContext: RequestContext,
  opts: { runId: string; toolCallId: string },
): RequestContext {
  const harness = requestContext.get('harness') as HarnessToolContextSlot | undefined;
  if (!harness?.registerQuestion && !harness?.registerPlanApproval) return requestContext;

  const overlay = new RequestContext<unknown>(
    Array.from(requestContext.entries() as IterableIterator<[string, unknown]>),
  );
  overlay.set('harness', {
    ...harness,
    ...(harness.registerQuestion
      ? {
          registerQuestion: async (params: Record<string, unknown>) =>
            harness.registerQuestion!({
              ...params,
              runId: typeof params.runId === 'string' ? params.runId : opts.runId,
              toolCallId: typeof params.toolCallId === 'string' ? params.toolCallId : opts.toolCallId,
            }),
        }
      : {}),
    ...(harness.registerPlanApproval
      ? {
          registerPlanApproval: async (params: Record<string, unknown>) =>
            harness.registerPlanApproval!({
              ...params,
              runId: typeof params.runId === 'string' ? params.runId : opts.runId,
              toolCallId: typeof params.toolCallId === 'string' ? params.toolCallId : opts.toolCallId,
            }),
        }
      : {}),
  });
  return overlay;
}

export function createToolCallStep<Tools extends ToolSet = ToolSet, OUTPUT = undefined>({
  tools,
  messageList,
  options,
  outputWriter,
  controller,
  runId,
  streamState,
  modelSpanTracker,
  _internal,
  logger,
  agentId,
  agentVersionId,
  mastra,
  requireToolApproval: requireToolApprovalFromFactory,
  requestContext: factoryRequestContext,
  actor,
  mcp,
}: OuterLLMRun<Tools, OUTPUT>) {
  // Function-valued requestContext entries do not survive `RequestContext.toJSON()`
  // across the evented engine's event bus. The factory closure captured the live
  // turn context, so these reads are authoritative — same pattern as
  // `requireToolApproval` above. The execute-time requestContext remains the
  // fallback for direct callers that seed the entries there.
  const toolPermissionPolicyFromFactory = factoryRequestContext?.get(TOOL_PERMISSION_POLICY_KEY) as
    | ToolPermissionPolicy
    | undefined;
  const onBeforeToolExecutionFromFactory = factoryRequestContext?.get(ON_BEFORE_TOOL_EXECUTION_KEY) as
    | BeforeToolExecutionHook
    | undefined;
  const onToolDeniedFromFactory = factoryRequestContext?.get(TOOL_DENIED_CALLBACK_KEY) as
    | ToolDeniedCallback
    | undefined;
  return createStep({
    id: 'toolCallStep',
    inputSchema: toolCallInputSchema,
    outputSchema: toolCallOutputSchema,
    execute: async executionContext => {
      const { suspend, resumeData: workflowResumeData, suspendData, requestContext } = executionContext;
      let inputData = executionContext.inputData;
      // Eager dispatch invokes this step with its own signal, chained to the run's, so a
      // call started for a model attempt that is later discarded can be cancelled on its
      // own. Every other caller falls back to the run signal, unchanged.
      const callerAbortSignal = (executionContext as unknown as Record<symbol, AbortSignal | undefined>)[
        EAGER_TOOL_ABORT_SIGNAL
      ];
      const abortSignal =
        callerAbortSignal && options?.abortSignal
          ? AbortSignal.any([callerAbortSignal, options.abortSignal])
          : (callerAbortSignal ?? options?.abortSignal);
      // Resolve run-scoped state from either the Mastra-managed RunScope (production
      // path via loop.ts hydration) or the legacy `_internal` bag (tests).
      const scopeCtx: RunScopeContext = { mastra, runId, _internal };
      const isEagerExecution = Boolean((executionContext as any)[EAGER_TOOL_EXECUTION_MARKER]);
      // Present only on an eager dispatch. Marked before bailing out, so the dispatcher
      // can reject a settlement whose bailout the tool swallowed.
      const eagerBailout = (executionContext as unknown as Record<symbol, EagerToolBailout | undefined>)[
        EAGER_TOOL_BAILOUT
      ];
      // Adopt an execution the LLM step started eagerly for this call, if any. The
      // eager invocation itself carries the marker so it never adopts itself.
      // Set when the eager attempt this invocation replaces already ran the tool's
      // `onInputAvailable`, so the hook stays at one call per adoption.
      let inputAlreadyAnnouncedEagerly = false;
      // Set when the eager attempt was abandoned because the tool suspended at runtime.
      // Stashed here and consumed further down, where the suspension helper's dependencies
      // (`args`, `transformChunk`, `flushMessagesBeforeSuspension`) exist.
      let eagerSuspensionIntent: EagerSuspensionIntent | undefined;
      if (!isEagerExecution) {
        // Take rather than read: adoption is exactly-once, so a later iteration that
        // reuses this toolCallId executes again instead of replaying a stale result.
        const eagerExecution = readScoped(scopeCtx, EAGER_TOOL_EXECUTION_KEY, 'eagerToolExecutionCoordinator')?.take(
          inputData.toolCallId,
        );
        if (eagerExecution) {
          try {
            return (await eagerExecution) as any;
          } catch (error) {
            // The eager attempt produced nothing adoptable: it was cancelled while
            // still queued, or it turned out to need suspension. Run it normally
            // instead. In the suspension case the body did start, so the hook it
            // already announced must not be announced a second time.
            if (!eagerToolCallDidNotExecute(error)) throw error;
            inputAlreadyAnnouncedEagerly = eagerToolCallAlreadyAnnouncedInput(error);
            eagerSuspensionIntent = eagerToolCallSuspensionIntent(error);
          }
        }
      }
      // Use tools from the scope (set by llmExecutionStep via prepareStep/processInputStep)
      // when available. This avoids serialization — execute functions live off-the-wire.
      // Fall back to the original tools from the closure if not set.
      const stepTools = (readScoped(scopeCtx, STEP_TOOLS_KEY, 'stepTools') as Tools | undefined) || tools;
      const stepActiveTools = readScoped(scopeCtx, STEP_ACTIVE_TOOLS_KEY, 'stepActiveTools');
      const tool =
        stepTools?.[inputData.toolName] ||
        findProviderToolByName(stepTools, inputData.toolName) ||
        Object.values(stepTools || {})?.find((t: any) => `id` in t && t.id === inputData.toolName);
      const transformSource = {
        policy: readScoped(scopeCtx, TOOL_PAYLOAD_TRANSFORM_KEY, 'toolPayloadTransform'),
        toolTransform: (tool as { transform?: unknown } | undefined)?.transform as any,
      };
      const transformChunk = async (
        chunk: ChunkType<OUTPUT>,
        phase: 'input-available' | 'approval' | 'suspend' | 'output-available' | 'error',
        extra?: { output?: unknown; error?: unknown; suspendPayload?: unknown },
      ): Promise<ChunkType<OUTPUT>> => {
        const payload = 'payload' in chunk ? (chunk.payload as Record<string, any>) : {};
        const transformInput = payload.args ?? inputData.args;
        const transformToolName = typeof payload.toolName === 'string' ? payload.toolName : inputData.toolName;
        const transformToolCallId = typeof payload.toolCallId === 'string' ? payload.toolCallId : inputData.toolCallId;
        const transformProviderMetadata =
          (payload.providerMetadata as Record<string, unknown> | undefined) ??
          (inputData.providerMetadata as Record<string, unknown> | undefined);

        const inputTransform = await transformToolPayloadForTargets(
          {
            phase: 'input-available',
            toolName: transformToolName,
            toolCallId: transformToolCallId,
            input: transformInput,
            providerMetadata: transformProviderMetadata,
          },
          transformSource,
          logger,
        );
        const transform =
          phase === 'input-available'
            ? undefined
            : await transformToolPayloadForTargets(
                {
                  phase,
                  toolName: transformToolName,
                  toolCallId: transformToolCallId,
                  input: transformInput,
                  output: extra?.output,
                  error: extra?.error,
                  suspendPayload: extra?.suspendPayload,
                  providerMetadata: transformProviderMetadata,
                },
                transformSource,
                logger,
              );

        const transformedChunk = withToolPayloadTransformMetadata(
          withToolPayloadTransformMetadata(chunk, inputTransform),
          transform,
        ) as ChunkType<OUTPUT>;
        if (transformedChunk.type !== 'tool-call-approval' && transformedChunk.type !== 'tool-call-suspended') {
          return transformedChunk;
        }

        const displayInputTransform = getTransformedToolPayload(
          transformedChunk.metadata,
          'display',
          'input-available',
        );
        const displayPhaseTransform = getTransformedToolPayload(transformedChunk.metadata, 'display', phase);
        const resumeArgs =
          phase === 'approval'
            ? hasTransformedToolPayload(displayPhaseTransform)
              ? displayPhaseTransform.transformed
              : transformInput
            : hasTransformedToolPayload(displayInputTransform)
              ? displayInputTransform.transformed
              : transformInput;

        return {
          ...transformedChunk,
          payload: {
            ...transformedChunk.payload,
            resumeIdentityDigest: createToolCallIdentityDigest({
              toolCallId: transformToolCallId,
              toolName: transformToolName,
              args: resumeArgs,
            }),
          },
        } as ChunkType<OUTPUT>;
      };

      const addToolMetadata = ({
        toolCallId,
        toolName,
        args,
        parentToolName,
        parentArgs,
        suspendPayload,
        resumeSchema,
        type,
        suspendedToolRunId,
        approval,
        approvalSource,
        approvalInputIdentityDigest,
        approvedArgs,
        metadata: toolStateTransformMetadata,
      }: AddToolMetadataOptions) => {
        const metadataKey = type === 'suspension' ? 'suspendedTools' : 'pendingToolApprovals';
        const inputTransform = getTransformedToolPayload(toolStateTransformMetadata, 'transcript', 'input-available');
        const approvalTransform = getTransformedToolPayload(toolStateTransformMetadata, 'transcript', 'approval');
        const suspendTransform = getTransformedToolPayload(toolStateTransformMetadata, 'transcript', 'suspend');
        const transformedArgs =
          type === 'approval'
            ? hasTransformedToolPayload(approvalTransform)
              ? approvalTransform.transformed
              : hasTransformedToolPayload(inputTransform)
                ? inputTransform.transformed
                : args
            : hasTransformedToolPayload(inputTransform)
              ? inputTransform.transformed
              : args;
        const transformedSuspendPayload =
          type === 'suspension'
            ? hasTransformedToolPayload(suspendTransform)
              ? suspendTransform.transformed
              : suspendPayload
            : undefined;
        const entry = {
          version: 1,
          originRunId: runId,
          stepId: 'toolCallStep',
          toolCallId,
          toolName,
          identityDigest: createToolCallIdentityDigest({ toolCallId, toolName, args }),
          resumeIdentityDigest: createToolCallIdentityDigest({ toolCallId, toolName, args: transformedArgs }),
          args: transformedArgs,
          ...(parentToolName ? { parentToolName, parentArgs } : {}),
          type,
          runId,
          ...(suspendedToolRunId && suspendedToolRunId !== runId ? { delegatedRunId: suspendedToolRunId } : {}),
          ...(approval ? { approval } : {}),
          ...(approvalSource ? { approvalSource } : {}),
          ...(approvalInputIdentityDigest ? { approvalInputIdentityDigest } : {}),
          ...(approvedArgs ? { approvedArgs } : {}),
          ...(type === 'suspension' ? { suspendPayload: transformedSuspendPayload } : {}),
          resumeSchema,
          ...(toolStateTransformMetadata ? { metadata: toolStateTransformMetadata } : {}),
        };
        const carriesToolCall = (message: MastraDBMessage) =>
          message.role === 'assistant' &&
          (message.content?.parts ?? []).some(
            part => part.type === 'tool-invocation' && part.toolInvocation.toolCallId === toolCallId,
          );

        const responseMessages = messageList.get.response.db();
        const responseMessage = [...responseMessages].reverse().find(carriesToolCall);
        if (responseMessage?.content) {
          const metadata =
            typeof responseMessage.content.metadata === 'object' && responseMessage.content.metadata !== null
              ? (responseMessage.content.metadata as Record<string, any>)
              : {};
          responseMessage.content.metadata = metadata;
          metadata[metadataKey] = metadata[metadataKey] || {};
          metadata[metadataKey][toolCallId] = entry;
          return;
        }

        const target = [...messageList.get.all.db()].reverse().find(carriesToolCall);
        if (!target?.content) {
          logger?.warn?.(
            `addToolMetadata could not find an assistant message for tool call ${toolCallId} (${toolName}); ${metadataKey} entry was not persisted.`,
          );
          return;
        }
        const existingMetadata =
          typeof target.content.metadata === 'object' && target.content.metadata !== null
            ? (target.content.metadata as Record<string, any>)
            : {};
        const existingEntries = (existingMetadata[metadataKey] ?? {}) as Record<string, any>;
        const updated = messageList.updateMessageMetadataByToolCallId(toolCallId, {
          [metadataKey]: { ...existingEntries, [toolCallId]: entry },
        });
        if (!updated) {
          logger?.debug?.(
            `addToolMetadata could not update the assistant message for tool call ${toolCallId} (${toolName}); ${metadataKey} entry was not persisted.`,
          );
        }
      };

      const removeToolMetadata = async (
        target: { toolCallId?: string; toolName: string; runId?: string },
        type: 'suspension' | 'approval',
      ) => {
        const { saveQueueManager, memoryConfig, threadId } = _internal || {};
        if (!saveQueueManager || !threadId) return;

        const metadataKey = type === 'suspension' ? 'suspendedTools' : 'pendingToolApprovals';
        const expectedPartType = type === 'suspension' ? 'data-tool-call-suspended' : 'data-tool-call-approval';
        const entryMatches = (entry: any, fallbackToolCallId?: string): boolean => {
          const entryToolCallId = typeof entry?.toolCallId === 'string' ? entry.toolCallId : fallbackToolCallId;
          const entryToolName = entry?.parentToolName ?? entry?.toolName;
          const entryRunId = type === 'approval' ? entry?.delegatedRunId : (entry?.delegatedRunId ?? entry?.runId);
          if (target.toolCallId) return entryToolCallId === target.toolCallId;
          return entryToolName === target.toolName && !!target.runId && entryRunId === target.runId;
        };

        const changedMessages: MastraDBMessage[] = [];
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
        // `add` re-marks the mutated messages unsaved in the response view so the
        // flush below persists the removal. It is mandatory: the real save queue persists only
        // `snapshotUnsavedMessages`, whose membership depends on tracked unsaved sets, so the
        // in-place mutation alone does not re-enrol a previously persisted message.
        // `merge: false`: these are existing messages re-enrolled by id. They must be
        // replaced in place, never appended onto the latest (current-turn) assistant
        // message — a recalled, unsealed suspended message would otherwise have its
        // parts duplicated into the resumed turn's response.
        messageList.add(changedMessages, 'response', { merge: false });
        try {
          await saveQueueManager.flushMessages(messageList, threadId, memoryConfig);
        } catch (error) {
          logger?.error('Error removing tool suspension metadata:', error);
        }
      };

      // Helper function to flush messages before suspension
      const flushMessagesBeforeSuspension = async () => {
        const saveQueueManager = readScoped(scopeCtx, SAVE_QUEUE_MANAGER_KEY, 'saveQueueManager');
        const memoryConfig = readScoped(scopeCtx, MEMORY_CONFIG_KEY, 'memoryConfig');
        const threadId = readScoped(scopeCtx, THREAD_ID_KEY, 'threadId');
        const resourceId = readScoped(scopeCtx, RESOURCE_ID_KEY, 'resourceId');
        const memory = readScoped(scopeCtx, MEMORY_KEY, 'memory');

        if (!saveQueueManager || !threadId || memoryConfig?.readOnly) {
          return;
        }

        try {
          // Ensure thread exists before flushing messages
          const threadExists = readScoped(scopeCtx, THREAD_EXISTS_KEY, 'threadExists');
          if (memory && !threadExists && resourceId) {
            const thread = await memory.getThreadById?.({ threadId });
            if (!thread) {
              // Thread doesn't exist yet, create it now
              await memory.createThread?.({
                threadId,
                resourceId,
                memoryConfig,
              });
            }
            writeScoped(scopeCtx, THREAD_EXISTS_KEY, 'threadExists', true);
          }

          // Flush all pending messages immediately
          await saveQueueManager.flushMessages(messageList, threadId, memoryConfig);
        } catch (error) {
          logger?.error('Error flushing messages before suspension:', error);
        }
      };

      // Provider-executed tools are handled entirely by the stream path
      // (tool-call and tool-result chunks in llm-execution-step), so skip client execution.
      if (inputData.providerExecuted) {
        return inputData;
      }

      // Resolve the tool key for activeTools enforcement (may differ from toolName when matched by id)
      const toolKey = stepTools?.[inputData.toolName]
        ? inputData.toolName
        : Object.entries(stepTools || {}).find(([_, t]: [string, any]) => t === tool)?.[0];

      // Reject if tool doesn't exist or isn't in the active set for this step
      const isHiddenByActiveTools = stepActiveTools && toolKey && !stepActiveTools.includes(toolKey);
      if (!tool || isHiddenByActiveTools) {
        const availableToolNames = stepActiveTools ?? Object.keys(stepTools || {});
        const availableToolsStr =
          availableToolNames.length > 0 ? ` Available tools: ${availableToolNames.join(', ')}` : '';
        return {
          // The workflow step output crosses the evented engine's pubsub boundary, where
          // `JSON.stringify` reduces Error instances to `{}`. Serialize to a plain object
          // here so `name`/`message`/`stack` survive and the consumer can reify the Error.
          error: serializeToolError(
            new ToolNotFoundError(
              `Tool "${inputData.toolName}" not found.${availableToolsStr}. Call tools by their exact name only — never add prefixes, namespaces, or colons.`,
            ),
          ),
          ...inputData,
        };
      }

      if (tool && 'onInputAvailable' in tool && !inputAlreadyAnnouncedEagerly) {
        try {
          await tool?.onInputAvailable?.({
            toolCallId: inputData.toolCallId,
            input: inputData.args,
            messages: messageList.get.input.aiV5.model(),
            abortSignal,
          });
        } catch (error) {
          logger?.error('Error calling onInputAvailable', error);
        }
        // Announced before `execute`, so a bailout raised from inside the tool body has
        // already fired the hook. Record it so the foreach's re-run does not fire it
        // again for the same call.
        if (eagerBailout) eagerBailout.inputAvailableCalled = true;
      }

      if (!tool.execute) {
        return inputData;
      }

      let approvalGrant: { approval: ToolApprovalGrant } | undefined;
      let verifiedApprovedArgs: Record<string, unknown> | undefined;
      let resumeTargetToolCallId: string | undefined;
      let resumedFromSuspension = false;

      try {
        // The factory closure value is authoritative when set: a function-valued policy
        // doesn't survive `RequestContext.toJSON()` across the evented engine's event bus,
        // so reading only from requestContext would lose it. Fall back to requestContext for
        // direct callers (e.g. legacy tests) that seed the value there.
        const requireToolApproval =
          requireToolApprovalFromFactory ?? requestContext.get('__mastra_requireToolApproval');

        let resumeDataFromArgs: any = undefined;
        let args: any = inputData.args;

        if (typeof inputData.args === 'object' && inputData.args !== null) {
          const { resumeData: resumeDataFromInput, ...argsFromInput } = inputData.args;
          args = argsFromInput;
          resumeDataFromArgs = resumeDataFromInput;
        }

        let resumeData = resumeDataFromArgs ?? workflowResumeData;
        // An APPROVAL decision may only arrive through the workflow resume boundary
        // (`agent.resumeStream` / the durable resume envelope). `resumeData` the model wrote
        // into its own tool arguments is untrusted: the pending approval's coordinates —
        // toolCallId, args, even `identityDigest` — are persisted on the
        // `data-tool-call-approval` part the model can read back, so identity alone does not
        // establish human consent. Suspension-typed args-borne resume (autoResumeSuspendedTools)
        // is unaffected; only consent is boundary-only.
        let isModelAuthoredResumeData = resumeDataFromArgs !== undefined;

        // Match the nullish fallback above: null/undefined use framework identity, while other falsy values are valid model payloads.
        let isResumeToolCall = resumeDataFromArgs != null;
        const isAgentTool = inputData.toolName?.startsWith('agent-');
        const isWorkflowTool = inputData.toolName?.startsWith('workflow-');
        const modelSuspendedToolCallIdClaim =
          typeof args?.suspendedToolCallId === 'string' && args.suspendedToolCallId.length > 0
            ? args.suspendedToolCallId
            : undefined;
        if (args && typeof args === 'object' && Object.hasOwn(args, 'suspendedToolCallId')) {
          const { suspendedToolCallId: _suspendedToolCallId, ...argsWithoutCallId } = args;
          args = argsWithoutCallId;
        }
        const modelSuspendedToolRunIdClaim =
          typeof args?.suspendedToolRunId === 'string' && args.suspendedToolRunId.length > 0
            ? args.suspendedToolRunId
            : undefined;
        if (args && typeof args === 'object' && Object.hasOwn(args, 'suspendedToolRunId')) {
          const { suspendedToolRunId: _suspendedToolRunId, ...argsWithoutRunId } = args;
          args = argsWithoutRunId;
        }
        // Single effective target from source provenance BEFORE lookup. A framework-driven
        // null/undefined placeholder ignores model coordinates; a model-driven resume uses them
        // (authenticated below). Untrusted claims are captured separately for evidence but never
        // choose the framework target, so validating B while routing A is impossible.
        const isFrameworkDrivenResume = !isResumeToolCall;
        const metadataToolCallId = isFrameworkDrivenResume
          ? inputData.toolCallId
          : (modelSuspendedToolCallIdClaim ?? inputData.toolCallId);
        const suppliedSuspendedToolRunId = isFrameworkDrivenResume ? undefined : modelSuspendedToolRunIdClaim;
        // Evidence of ignored model claims on framework resumes (never used above for routing).
        void modelSuspendedToolCallIdClaim;
        void modelSuspendedToolRunIdClaim;
        // A framework null placeholder already fell through via `??` above. Authorship becomes
        // framework only when no stray model coordinates were supplied; a null that still names a
        // run/call coordinate keeps its untrusted evidence so boundary-only consent stays fail-closed.
        if (
          resumeDataFromArgs === null &&
          workflowResumeData !== undefined &&
          isFrameworkDrivenResume &&
          modelSuspendedToolCallIdClaim === undefined &&
          modelSuspendedToolRunIdClaim === undefined
        ) {
          isModelAuthoredResumeData = false;
        }
        let identityArgs = args;
        let expectedIdentityDigest = createToolCallIdentityDigest({
          toolCallId: metadataToolCallId,
          toolName: inputData.toolName,
          args: identityArgs,
        });
        // Keep the digest for the arguments supplied to this workflow execution separate
        // from the canonical approved arguments restored below. A new suspension created
        // by an auto-resume must bind to those supplied arguments; an authoritative snapshot
        // replay keeps the digest carried by that snapshot.
        const workflowInputIdentityDigest = expectedIdentityDigest;
        let expectedResumeIdentity = {
          version: 1,
          originRunId: runId,
          stepId: 'toolCallStep',
          toolCallId: metadataToolCallId,
          toolName: inputData.toolName,
          identityDigest: expectedIdentityDigest,
        } as const;
        let approvedArgsResume:
          | {
              approvalInputIdentityDigest: string;
              approvedArgs: Record<string, unknown>;
            }
          | undefined;
        const matchesExpectedResumeIdentity = (value: unknown) => {
          if (!value || typeof value !== 'object' || Array.isArray(value)) return undefined;
          const record = value as Record<string, unknown>;
          return Object.entries(expectedResumeIdentity).every(
            ([key, expected]) => Object.hasOwn(record, key) && record[key] === expected,
          );
        };
        const hasStoredIdentityEnvelope = (value: unknown, expectedType: 'approval' | 'suspension') => {
          if (!value || typeof value !== 'object' || Array.isArray(value)) return false;
          const record = value as Record<string, unknown>;
          return (
            Object.hasOwn(record, 'version') &&
            record.version === 1 &&
            Object.hasOwn(record, 'originRunId') &&
            typeof record.originRunId === 'string' &&
            record.originRunId.length > 0 &&
            Object.hasOwn(record, 'runId') &&
            typeof record.runId === 'string' &&
            record.runId.length > 0 &&
            Object.hasOwn(record, 'type') &&
            record.type === expectedType &&
            Object.hasOwn(record, 'stepId') &&
            record.stepId === 'toolCallStep' &&
            Object.hasOwn(record, 'toolCallId') &&
            record.toolCallId === metadataToolCallId &&
            Object.hasOwn(record, 'toolName') &&
            record.toolName === inputData.toolName &&
            Object.hasOwn(record, 'identityDigest')
          );
        };
        const getStoredResumeIdentityMatch = (value: unknown, expectedType: 'approval' | 'suspension') => {
          if (!hasStoredIdentityEnvelope(value, expectedType)) return undefined;
          const record = value as Record<string, unknown>;
          if (record.identityDigest === expectedIdentityDigest) return 'canonical' as const;
          if (record.resumeIdentityDigest === expectedIdentityDigest) return 'resume' as const;
          return undefined;
        };
        const getStoredCanonicalArgs = (message: MastraDBMessage, record: Record<string, unknown>) => {
          if (typeof record.identityDigest !== 'string') return undefined;
          const structuredPart = message.content.parts?.find(
            part => part.type === 'tool-invocation' && part.toolInvocation?.toolCallId === metadataToolCallId,
          );
          const structuredArgs =
            structuredPart?.type === 'tool-invocation' ? structuredPart.toolInvocation?.args : undefined;
          const legacyArgs = message.content.toolInvocations?.find(
            invocation => invocation.toolCallId === metadataToolCallId,
          )?.args;
          const candidates = [
            structuredArgs,
            legacyArgs,
            Object.hasOwn(record, 'args') ? record.args : undefined,
            // Edited approvals retain their canonical arguments separately when
            // transcript transforms redact the visible invocation and metadata
            // `args`. The identity digest authenticates this candidate exactly as
            // it does the ordinary structured/legacy candidates above.
            Object.hasOwn(record, 'approvedArgs') ? record.approvedArgs : undefined,
          ].filter(candidate => candidate !== undefined);
          return candidates.find(
            candidate =>
              createToolCallIdentityDigest({
                toolCallId: metadataToolCallId,
                toolName: inputData.toolName,
                args: candidate,
              }) === record.identityDigest,
          );
        };
        const readStoredResumeMetadata = (
          stored: unknown,
          type: 'approval' | 'suspension',
          message: MastraDBMessage,
        ) => {
          const storedRecord =
            stored && typeof stored === 'object' && !Array.isArray(stored)
              ? (stored as Record<string, unknown>)
              : undefined;
          const identityMatch = getStoredResumeIdentityMatch(stored, type);
          return {
            type,
            // Delegated entries persist the OUTER resumable run under `runId` and
            // the delegate's inner suspended run under `delegatedRunId`. Resume
            // validation and the wrapper handoff both need the inner run, so
            // surface it here (mirrors auto-resume-system-message).
            runId:
              typeof storedRecord?.delegatedRunId === 'string'
                ? storedRecord.delegatedRunId
                : typeof storedRecord?.runId === 'string'
                  ? storedRecord.runId
                  : undefined,
            originRunId: typeof storedRecord?.originRunId === 'string' ? storedRecord.originRunId : undefined,
            identityDigest: typeof storedRecord?.identityDigest === 'string' ? storedRecord.identityDigest : undefined,
            identityMatches: identityMatch !== undefined,
            identityMatch,
            canonicalArgs: storedRecord ? getStoredCanonicalArgs(message, storedRecord) : undefined,
            approval: parseToolApprovalGrant(storedRecord?.approval, metadataToolCallId),
            approvalSource:
              storedRecord?.approvalSource === 'tool-gate' || storedRecord?.approvalSource === 'tool-execution'
                ? storedRecord.approvalSource
                : undefined,
            approvalInputIdentityDigest:
              typeof storedRecord?.approvalInputIdentityDigest === 'string'
                ? storedRecord.approvalInputIdentityDigest
                : undefined,
            approvedArgs: storedRecord?.approvedArgs,
          };
        };
        const getStoredResumeMetadata = (toolCallId: string) => {
          const messages = [...messageList.get.all.db()].reverse().filter(message => message.role === 'assistant');
          for (const message of messages) {
            const metadata =
              typeof message.content.metadata === 'object' && message.content.metadata !== null
                ? (message.content.metadata as Record<string, any>)
                : undefined;
            if (metadata?.pendingToolApprovals && Object.hasOwn(metadata.pendingToolApprovals, toolCallId)) {
              const stored = metadata.pendingToolApprovals[toolCallId];
              return readStoredResumeMetadata(stored, 'approval', message);
            }
            if (metadata?.suspendedTools && Object.hasOwn(metadata.suspendedTools, toolCallId)) {
              const stored = metadata.suspendedTools[toolCallId];
              return readStoredResumeMetadata(stored, 'suspension', message);
            }

            const foundPart = message.content.parts?.find(
              part =>
                (part.type === 'data-tool-call-suspended' || part.type === 'data-tool-call-approval') &&
                (part.data as any).toolCallId === toolCallId &&
                !(part.data as any).resumed,
            );
            if (foundPart?.type === 'data-tool-call-approval') {
              return readStoredResumeMetadata(foundPart.data, 'approval', message);
            }
            if (foundPart?.type === 'data-tool-call-suspended') {
              return readStoredResumeMetadata(foundPart.data, 'suspension', message);
            }
          }
          return undefined;
        };
        let storedResumeMetadata = resumeData !== undefined ? getStoredResumeMetadata(metadataToolCallId) : undefined;
        let hasSuspendedToolRunIdMismatch =
          suppliedSuspendedToolRunId !== undefined &&
          storedResumeMetadata?.runId !== undefined &&
          suppliedSuspendedToolRunId !== storedResumeMetadata.runId;
        const hasStoredOriginRunMismatch =
          storedResumeMetadata?.originRunId !== undefined && storedResumeMetadata.originRunId !== runId;
        const authoritativeResumeEnvelope =
          suspendData && typeof suspendData === 'object' && !Array.isArray(suspendData)
            ? (suspendData as Record<string, unknown>).toolCallResume
            : undefined;
        const hasAuthoritativeResumeEnvelope = authoritativeResumeEnvelope !== undefined;
        const editedApprovalResumeLoader = readScoped(
          scopeCtx,
          EDITED_APPROVAL_RESUME_LOADER_KEY,
          'editedApprovalResumeLoader',
        );
        const hasEditedApprovalMarker =
          storedResumeMetadata?.approval !== undefined || storedResumeMetadata?.approvalSource === 'tool-gate';
        const needsEditedApprovalResumeRecovery =
          !hasAuthoritativeResumeEnvelope &&
          !isAgentTool &&
          !isWorkflowTool &&
          resumeData !== undefined &&
          storedResumeMetadata?.type === 'suspension' &&
          storedResumeMetadata.identityMatches === true &&
          storedResumeMetadata.identityMatch === 'resume' &&
          hasEditedApprovalMarker &&
          storedResumeMetadata.canonicalArgs === undefined &&
          storedResumeMetadata.approvedArgs === undefined &&
          typeof storedResumeMetadata.runId === 'string' &&
          typeof storedResumeMetadata.originRunId === 'string' &&
          typeof storedResumeMetadata.identityDigest === 'string';
        if (
          needsEditedApprovalResumeRecovery &&
          editedApprovalResumeLoader &&
          storedResumeMetadata &&
          typeof storedResumeMetadata.runId === 'string' &&
          typeof storedResumeMetadata.originRunId === 'string' &&
          typeof storedResumeMetadata.identityDigest === 'string'
        ) {
          const recovered = await editedApprovalResumeLoader({
            runId: storedResumeMetadata.runId,
            originRunId: storedResumeMetadata.originRunId,
            toolCallId: metadataToolCallId,
            toolName: inputData.toolName,
            identityDigest: storedResumeMetadata.identityDigest,
            requestContext,
            threadId: readScoped(scopeCtx, THREAD_ID_KEY, 'threadId'),
            resourceId: readScoped(scopeCtx, RESOURCE_ID_KEY, 'resourceId'),
            actor,
          });
          if (recovered) {
            // Keep the secret snapshot data local to this execution. Recalled
            // transcript metadata is intentionally redacted and must not be
            // repopulated with private approval arguments.
            storedResumeMetadata = {
              ...storedResumeMetadata,
              canonicalArgs: recovered.approvedArgs,
              approvedArgs: recovered.approvedArgs,
              approvalInputIdentityDigest: recovered.approvalInputIdentityDigest,
            };
          }
        }
        // A tool that suspends after an edited approval retains both the original
        // workflow-input identity and the approved arguments. Authenticate the
        // original input before restoring those arguments from the snapshot.
        const approvedArgsEnvelope = authoritativeResumeEnvelope as
          | {
              approvalInputIdentityDigest?: unknown;
              approvedArgs?: unknown;
              identityDigest?: unknown;
            }
          | undefined;
        const hasApprovedArgsEnvelope =
          approvedArgsEnvelope?.approvedArgs !== undefined &&
          typeof approvedArgsEnvelope.approvalInputIdentityDigest === 'string' &&
          toolApprovalEditedArgsSchema.safeParse(approvedArgsEnvelope.approvedArgs).success &&
          createToolCallIdentityDigest({
            toolCallId: metadataToolCallId,
            toolName: inputData.toolName,
            args: approvedArgsEnvelope.approvedArgs,
          }) === approvedArgsEnvelope.identityDigest;
        const hasStoredApprovedArgsFields =
          storedResumeMetadata?.approvedArgs !== undefined ||
          storedResumeMetadata?.approvalInputIdentityDigest !== undefined;
        const hasStoredApprovedArgs =
          !hasAuthoritativeResumeEnvelope &&
          storedResumeMetadata?.type === 'suspension' &&
          storedResumeMetadata.approvedArgs !== undefined &&
          typeof storedResumeMetadata.approvalInputIdentityDigest === 'string' &&
          toolApprovalEditedArgsSchema.safeParse(storedResumeMetadata.approvedArgs).success &&
          createToolCallIdentityDigest({
            toolCallId: metadataToolCallId,
            toolName: inputData.toolName,
            args: storedResumeMetadata.approvedArgs,
          }) === storedResumeMetadata.identityDigest;
        const authoritativeIdentityMatches =
          hasAuthoritativeResumeEnvelope &&
          (matchesExpectedResumeIdentity(authoritativeResumeEnvelope) ||
            (hasApprovedArgsEnvelope &&
              matchesExpectedResumeIdentity({
                ...(authoritativeResumeEnvelope as object),
                identityDigest: approvedArgsEnvelope.approvalInputIdentityDigest,
              })));
        // Some providers materialize optional workflow/agent-tool control fields
        // as null on a fresh call. When the workflow engine is explicitly resuming
        // that call, its resume data must win over the provider placeholder. This
        // only applies when the provider supplied no non-empty resume coordinate;
        // model-driven resumes keep their identity evidence and remain fail-closed
        // in the checks below. The authoritative envelope is still validated before
        // the workflow resume data can be consumed.
        const isIdentityFreeProviderNullResume =
          isResumeToolCall &&
          resumeDataFromArgs === null &&
          modelSuspendedToolCallIdClaim === undefined &&
          modelSuspendedToolRunIdClaim === undefined;
        if (isIdentityFreeProviderNullResume && workflowResumeData !== undefined) {
          resumeData = workflowResumeData;
          isResumeToolCall = false;
          // The effective payload now comes from the workflow boundary, not the provider
          // placeholder, so it is no longer model-authored.
          isModelAuthoredResumeData = false;
        }

        // Null is also a valid resumed tool payload, so a fresh provider null is
        // normalized to undefined only when no workflow, durable, or caller-supplied
        // resume evidence exists.
        const isEvidenceFreeNullResumePlaceholder =
          isIdentityFreeProviderNullResume &&
          workflowResumeData === undefined &&
          !hasAuthoritativeResumeEnvelope &&
          storedResumeMetadata === undefined;
        if (isEvidenceFreeNullResumePlaceholder) {
          resumeData = undefined;
          isResumeToolCall = false;
        }
        // Upstream #21729: `resumeData` is an always-exposed optional field on the
        // generated agent/workflow tool schema, so models fill it on a fresh
        // delegation. With no suspension anywhere (no workflow resume data, no
        // authoritative envelope, no stored suspension metadata), no model-claimed
        // suspended coordinates and no approval-shaped payload, there is nothing to
        // resume: drop the payload and run the delegation fresh. Everything else —
        // any coordinate claim, stored state or approval shape — stays on the
        // fail-closed evidence checks below, so no identity or grant is accepted.
        const isEvidenceFreeModelDelegationResume =
          (isAgentTool || isWorkflowTool) &&
          isModelAuthoredResumeData &&
          resumeDataFromArgs != null &&
          workflowResumeData === undefined &&
          !hasAuthoritativeResumeEnvelope &&
          storedResumeMetadata === undefined &&
          modelSuspendedToolCallIdClaim === undefined &&
          modelSuspendedToolRunIdClaim === undefined &&
          parseToolApprovalDecision(resumeDataFromArgs) === undefined;
        if (isEvidenceFreeModelDelegationResume) {
          resumeData = undefined;
          isResumeToolCall = false;
          isModelAuthoredResumeData = false;
        }
        const authoritativeResumeType = authoritativeIdentityMatches
          ? (authoritativeResumeEnvelope as { type?: unknown }).type
          : undefined;
        const authoritativeApprovalSource = authoritativeIdentityMatches
          ? (authoritativeResumeEnvelope as { approvalSource?: unknown }).approvalSource
          : undefined;
        const hasKnownAuthoritativeResumeType =
          authoritativeResumeType === 'approval' || authoritativeResumeType === 'suspension';
        const hasAuthoritativeWorkflowResumeData =
          workflowResumeData !== undefined &&
          authoritativeIdentityMatches &&
          hasKnownAuthoritativeResumeType &&
          // A null placeholder that still names a model run coordinate carries untrusted evidence:
          // the authoritative win is blocked so the stray claim cannot launder a workflow payload.
          // Target selection already ignores the claim; this guard preserves fail-closed rejection.
          !(resumeDataFromArgs === null && modelSuspendedToolRunIdClaim !== undefined);
        if (hasAuthoritativeWorkflowResumeData) {
          // A validated workflow snapshot is the resume boundary. Provider-replayed args can
          // retain an older resume payload and suspended-run coordinate; preserve the mapped
          // tool call identity, but let the boundary payload win and clear that stale mismatch.
          resumeData = workflowResumeData;
          isModelAuthoredResumeData = false;
          hasSuspendedToolRunIdMismatch = false;
        }
        const effectiveResumeType = hasAuthoritativeResumeEnvelope
          ? hasKnownAuthoritativeResumeType
            ? authoritativeResumeType
            : undefined
          : storedResumeMetadata?.type;
        const effectiveApprovalSource =
          authoritativeApprovalSource === 'tool-gate' || authoritativeApprovalSource === 'tool-execution'
            ? authoritativeApprovalSource
            : storedResumeMetadata?.approvalSource;
        const approvalDecision = parseToolApprovalDecision(resumeData);
        const hasApprovalResumeShape = approvalDecision !== undefined;
        const approvalDeclineReason = resolveDeclineReason(resumeData);
        const authoritativeDelegatedRunId =
          suspendData &&
          typeof suspendData === 'object' &&
          !Array.isArray(suspendData) &&
          typeof (suspendData as { suspendedToolRunId?: unknown }).suspendedToolRunId === 'string' &&
          (suspendData as { suspendedToolRunId: string }).suspendedToolRunId.length > 0
            ? (suspendData as { suspendedToolRunId: string }).suspendedToolRunId
            : undefined;
        const isDelegatedApprovalResume =
          hasApprovalResumeShape &&
          effectiveResumeType === 'approval' &&
          !hasSuspendedToolRunIdMismatch &&
          ((authoritativeIdentityMatches && authoritativeDelegatedRunId !== undefined) ||
            ((isAgentTool || isWorkflowTool) &&
              storedResumeMetadata?.identityMatches === true &&
              storedResumeMetadata.runId &&
              storedResumeMetadata.originRunId &&
              storedResumeMetadata.runId !== storedResumeMetadata.originRunId));
        // Delegated agent/workflow suspension resumes authenticate the exact supported canonical
        // entry or the authoritative snapshot before routing. Partial, wrong-version, wrong-step, or
        // bad-digest evidence retains metadata and returns an error — never coordinate-only trust.
        // Historical upstream envelope-free provenance is not permission compatibility: current
        // native producers always write the full envelope, so a missing envelope fails closed.
        const hasResumeIdentityMismatch =
          ((approvedArgsEnvelope?.approvedArgs !== undefined ||
            approvedArgsEnvelope?.approvalInputIdentityDigest !== undefined) &&
            !hasApprovedArgsEnvelope) ||
          (!hasAuthoritativeResumeEnvelope && hasStoredApprovedArgsFields && !hasStoredApprovedArgs) ||
          (hasAuthoritativeResumeEnvelope && (!authoritativeIdentityMatches || !hasKnownAuthoritativeResumeType)) ||
          (!hasAuthoritativeResumeEnvelope && storedResumeMetadata?.identityMatches === false);
        const isKnownApprovalResume =
          effectiveResumeType === 'approval' &&
          (authoritativeResumeType === 'approval' || storedResumeMetadata?.identityMatches === true) &&
          !isDelegatedApprovalResume;
        const isKnownSuspensionResume =
          effectiveResumeType === 'suspension' &&
          (authoritativeResumeType === 'suspension' || storedResumeMetadata?.identityMatches === true);
        resumedFromSuspension = isKnownSuspensionResume;
        const isApprovalResumeData = hasApprovalResumeShape && isKnownApprovalResume && !isModelAuthoredResumeData;
        const isAnyResume = Boolean(isApprovalResumeData || isKnownSuspensionResume || isDelegatedApprovalResume);
        const isToolExecutionApprovalResume = isApprovalResumeData && effectiveApprovalSource === 'tool-execution';
        const persistedApprovalGrant =
          effectiveResumeType === 'suspension'
            ? hasAuthoritativeResumeEnvelope && authoritativeIdentityMatches
              ? parseToolApprovalGrant(
                  (authoritativeResumeEnvelope as Record<string, unknown>).approval,
                  metadataToolCallId,
                )
              : storedResumeMetadata?.identityMatches === true
                ? storedResumeMetadata.approval
                : undefined
            : undefined;
        const needsCanonicalArgsRestore =
          storedResumeMetadata?.identityMatch === 'resume' &&
          !(authoritativeIdentityMatches && hasApprovedArgsEnvelope);
        const isCanonicalArgsUnavailable =
          needsCanonicalArgsRestore && storedResumeMetadata?.canonicalArgs === undefined;
        const hasInvalidResumeData =
          resumeData !== undefined &&
          (hasResumeIdentityMismatch ||
            hasSuspendedToolRunIdMismatch ||
            isCanonicalArgsUnavailable ||
            (hasStoredOriginRunMismatch && !isResumeToolCall) ||
            (!isDelegatedApprovalResume &&
              ((effectiveResumeType === 'approval' && !isApprovalResumeData) ||
                (effectiveResumeType !== 'approval' && effectiveResumeType !== 'suspension'))));

        if (hasInvalidResumeData) {
          return {
            ...inputData,
            error: new Error('Tool resume evidence did not match the suspended tool call'),
          };
        }

        if (needsCanonicalArgsRestore) {
          args = structuredClone(storedResumeMetadata!.canonicalArgs);
          expectedIdentityDigest = createToolCallIdentityDigest({
            toolCallId: metadataToolCallId,
            toolName: inputData.toolName,
            args,
          });
          expectedResumeIdentity = {
            ...expectedResumeIdentity,
            identityDigest: expectedIdentityDigest,
          };
        }
        if (authoritativeIdentityMatches && hasApprovedArgsEnvelope) {
          args = structuredClone(approvedArgsEnvelope.approvedArgs);
          identityArgs = structuredClone(args);
          verifiedApprovedArgs = structuredClone(args);
          approvedArgsResume = {
            approvalInputIdentityDigest: approvedArgsEnvelope.approvalInputIdentityDigest as string,
            approvedArgs: structuredClone(args),
          };
          expectedIdentityDigest = approvedArgsEnvelope.identityDigest as string;
          expectedResumeIdentity = { ...expectedResumeIdentity, identityDigest: expectedIdentityDigest };
          inputData = { ...inputData, args: structuredClone(args) };
        } else if (hasStoredApprovedArgs) {
          args = structuredClone(storedResumeMetadata!.approvedArgs);
          identityArgs = structuredClone(args);
          verifiedApprovedArgs = structuredClone(args);
          approvedArgsResume = {
            // This branch is a new suspension after an auto-resume. Bind its next resume
            // envelope to the input that initiated this execution, which may be transcript-redacted.
            approvalInputIdentityDigest: workflowInputIdentityDigest,
            approvedArgs: structuredClone(args),
          };
          expectedIdentityDigest = createToolCallIdentityDigest({
            toolCallId: metadataToolCallId,
            toolName: inputData.toolName,
            args,
          });
          expectedResumeIdentity = { ...expectedResumeIdentity, identityDigest: expectedIdentityDigest };
          inputData = { ...inputData, args: structuredClone(args) };
        }
        if (
          verifiedApprovedArgs === undefined &&
          !isAgentTool &&
          !isWorkflowTool &&
          isKnownSuspensionResume &&
          persistedApprovalGrant !== undefined &&
          storedResumeMetadata?.identityMatch === 'canonical' &&
          args !== null &&
          typeof args === 'object' &&
          !Array.isArray(args)
        ) {
          // Canonical args are already authenticated by the suspension identity and persisted
          // approval grant. Keep a detached copy only for result/persistence consumers; do not
          // turn this provenance marker into resume authorization or a new approval envelope.
          verifiedApprovedArgs = structuredClone(args);
        }
        const resumeTarget =
          metadataToolCallId !== inputData.toolCallId ? { resumeTargetToolCallId: metadataToolCallId } : {};
        resumeTargetToolCallId = resumeTarget.resumeTargetToolCallId;

        // Check if approval is required
        // requireApproval can be:
        // - boolean (from Mastra createTool or mapped from AI SDK needsApproval: true)
        // - undefined (no approval needed)
        // If needsApprovalFn exists, evaluate it with the tool args and context
        if (isApprovalResumeData && approvalDecision.approved === false) {
          await removeToolMetadata({ toolCallId: metadataToolCallId, toolName: inputData.toolName }, 'approval');

          return {
            ...inputData,
            args: identityArgs,
            // Keep the provider's current call ID so its invocation receives a result, and carry
            // the original pending call separately so persistence can mark that approval denied.
            ...resumeTarget,
            ...(verifiedApprovedArgs ? { approvedArgs: structuredClone(verifiedApprovedArgs) } : {}),
            approval: {
              id: metadataToolCallId,
              approved: false,
              reason: approvalDeclineReason,
            },
          };
        }

        const validateInput = (
          tool as {
            validateInput?: (
              params: unknown,
            ) => { data?: unknown; error?: unknown } | Promise<{ data?: unknown; error?: unknown }>;
          }
        ).validateInput;
        const supportsEditedApprovalArgs =
          !isAgentTool &&
          !isWorkflowTool &&
          (tool as { approvalInputEditing?: unknown }).approvalInputEditing === 'object' &&
          typeof validateInput === 'function';
        if (effectiveResumeType === 'approval' && approvalDecision?.editedArgs !== undefined) {
          if (effectiveApprovalSource !== 'tool-gate' || isDelegatedApprovalResume || isAgentTool || isWorkflowTool) {
            return { ...inputData, error: new Error('Edited approval arguments require a regular tool-gate approval') };
          }
          if (!supportsEditedApprovalArgs) {
            return { ...inputData, error: new Error('Edited approval arguments require a tool input validator') };
          }
          const baseArgs = args == null ? {} : args;
          if (typeof baseArgs !== 'object' || Array.isArray(baseArgs)) {
            return { ...inputData, error: new Error('Edited approval arguments require object tool input') };
          }
          const editedArgs = { ...baseArgs, ...approvalDecision.editedArgs };
          let validation;
          try {
            validation = await validateInput(editedArgs);
          } finally {
            await removeToolMetadata({ toolCallId: metadataToolCallId, toolName: inputData.toolName }, 'approval');
          }
          if (validation.error !== undefined) {
            return {
              ...inputData,
              ...resumeTarget,
              result:
                validation.error instanceof Error
                  ? serializeToolError(validation.error)
                  : ensureSerializable(validation.error),
            };
          }
          // Validation may transform input. Execution owns those transforms, so
          // retain the untransformed approved input in identity and snapshots.
          const originalIdentityDigest = expectedIdentityDigest;
          args = editedArgs;
          identityArgs = structuredClone(args);
          expectedIdentityDigest = createToolCallIdentityDigest({
            toolCallId: metadataToolCallId,
            toolName: inputData.toolName,
            args,
          });
          expectedResumeIdentity = { ...expectedResumeIdentity, identityDigest: expectedIdentityDigest };
          approvedArgsResume = {
            approvalInputIdentityDigest: approvedArgsResume?.approvalInputIdentityDigest ?? originalIdentityDigest,
            approvedArgs: structuredClone(args),
          };
          verifiedApprovedArgs = structuredClone(args);
          inputData = { ...inputData, args: structuredClone(args) };
        }

        if (isApprovalResumeData) {
          await removeToolMetadata({ toolCallId: metadataToolCallId, toolName: inputData.toolName }, 'approval');
        } else if (isKnownSuspensionResume) {
          await removeToolMetadata({ toolCallId: metadataToolCallId, toolName: inputData.toolName }, 'suspension');
        }

        // Only framework-persisted runs are applied here. A model-supplied run id
        // is authenticated by resolveFrameworkSuspendedToolIdentity below and
        // applied there when (and only when) it resolves — never blindly.
        const validatedSuspendedToolRunId = storedResumeMetadata?.runId;
        if ((isAgentTool || isWorkflowTool) && validatedSuspendedToolRunId !== undefined) {
          args.suspendedToolRunId = validatedSuspendedToolRunId;
        }

        // Per-tool permission policy gate (§4.2e). A caller (e.g. the harness)
        // may thread a resolver on the request context that returns
        // 'allow' | 'ask' | 'deny' for a tool name. `deny` blocks the call with a
        // non-aborting result the model can react to; `ask` forces approval (it is
        // OR'd with tool-owned/global approval, never suppressing them); `allow`
        // defers entirely to the tool's own approval config. The factory-captured
        // value is authoritative because the function does not survive the
        // evented engine's requestContext transport.
        const toolPermissionPolicyResolver =
          toolPermissionPolicyFromFactory ??
          (requestContext.get(TOOL_PERMISSION_POLICY_KEY) as ToolPermissionPolicy | undefined);
        const toolPermissionPolicy = toolPermissionPolicyResolver?.(inputData.toolName);
        // §O4 deny observability reads the callback off the request context;
        // prefer the factory-captured one so the event survives evented
        // transport too.
        const deniedNotifyContext =
          onToolDeniedFromFactory === undefined
            ? requestContext
            : {
                get: (key: string) =>
                  key === TOOL_DENIED_CALLBACK_KEY ? onToolDeniedFromFactory : requestContext.get(key),
              };
        if (toolPermissionPolicy === 'deny') {
          // §O4 — surface WHY a tool was blocked (action-time deny is otherwise
          // opaque: only a generic result reaches the model). Optional, sync,
          // fire-and-forget, isolated; a non-harness caller threads no callback.
          notifyToolDenied(deniedNotifyContext, {
            toolName: inputData.toolName,
            stage: 'action',
            toolCallId: inputData.toolCallId,
          });
          return {
            ...inputData,
            ...resumeTarget,
            disposition: 'denied' as const,
            result: `Tool "${inputData.toolName}" was denied by the session permission policy.`,
          };
        }

        // §4.2e per-tool revalidation — an awaited hook the caller (the harness)
        // may thread on the request context so authorization can be re-checked
        // against durable state captured after this turn's snapshot (e.g. a
        // grant revoked or expired mid-turn). Runs after the policy deny
        // short-circuit and before approval resolution/`execute()`. Throwing
        // or returning an unrecognized decision fails closed as deny. When the
        // REQUIRED marker is present without the function (transported or
        // restored context), the call is denied rather than silently
        // unrevalidated.
        const onBeforeToolExecution =
          onBeforeToolExecutionFromFactory ??
          (requestContext.get(ON_BEFORE_TOOL_EXECUTION_KEY) as BeforeToolExecutionHook | undefined);
        if (typeof onBeforeToolExecution === 'function') {
          let beforeDecision: 'allow' | 'deny' | void;
          try {
            beforeDecision = await onBeforeToolExecution({
              toolName: inputData.toolName,
              toolCallId: inputData.toolCallId,
              args,
              isResume: isAnyResume,
              policyDecision: toolPermissionPolicy,
            });
          } catch {
            beforeDecision = 'deny';
          }
          if (beforeDecision !== undefined && beforeDecision !== 'allow') {
            notifyToolDenied(deniedNotifyContext, {
              toolName: inputData.toolName,
              stage: 'action',
              toolCallId: inputData.toolCallId,
            });
            return {
              ...inputData,
              ...resumeTarget,
              disposition: 'denied' as const,
              result: `Tool "${inputData.toolName}" was denied by the pre-execution permission hook.`,
            };
          }
          // The awaited hook can span real I/O (e.g. a grant-store read). A
          // request aborted inside that window must not proceed to approval or
          // dispatch — the hook's verdict is moot once the turn is cancelled.
          if (options?.abortSignal?.aborted) {
            return {
              aborted: true,
              abortError: serializeToolError(
                (options.abortSignal as AbortSignal & { reason?: unknown }).reason ??
                  new DOMException('The operation was aborted.', 'AbortError'),
              ),
              ...inputData,
            };
          }
        } else if (
          requestContext.get(ON_BEFORE_TOOL_EXECUTION_REQUIRED_KEY) === true ||
          factoryRequestContext?.get(ON_BEFORE_TOOL_EXECUTION_REQUIRED_KEY) === true
        ) {
          notifyToolDenied(deniedNotifyContext, {
            toolName: inputData.toolName,
            stage: 'action',
            toolCallId: inputData.toolCallId,
          });
          return {
            ...inputData,
            ...resumeTarget,
            disposition: 'denied' as const,
            result: `Tool "${inputData.toolName}" was denied by the pre-execution permission hook.`,
          };
        }

        // §4.2e per-turn `yolo`: a caller (the harness queued-turn drain) may thread
        // `__mastra_yoloAutoApprove` to clear the POLICY-level approval reason — i.e.
        // an effective `ask` from the permission gate. Per spec it suppresses ONLY the
        // `policy` reason; it NEVER suppresses a tool-owned reason (a tool's static
        // `requireApproval` / its `needsApprovalFn` callback), and (since `deny`
        // already returned above) it can never run a denied tool. Mirrors how a grant
        // clears the policy ask at the resolver — yolo does it per-run here.
        const yoloAutoApprove = requestContext.get('__mastra_yoloAutoApprove') === true;

        // Reuse the called-strategy scheduling verdict when available so approval policies
        // are evaluated exactly once per call. Other paths retain execution-time evaluation.
        const approvalVerdicts = readScoped(scopeCtx, TOOL_APPROVAL_VERDICTS_KEY, 'toolApprovalVerdicts');
        const cachedApprovalRequirement = approvalVerdicts?.get(inputData.toolCallId);
        approvalVerdicts?.delete(inputData.toolCallId);
        // Upstream #24763: reuse the scheduling evaluation (verdict AND its reasons) so a
        // possibly stateful policy runs exactly once per call.
        const approvalRequirement =
          cachedApprovalRequirement ??
          (await resolveToolApprovalRequirement({
            tool,
            args,
            // #17337 — pass through unboxed: the global may be a per-call FUNCTION
            // policy; resolveToolApprovalRequirement evaluates it (fail-safe true).
            requireToolApproval: requireToolApproval as RequireToolApproval | undefined,
            requestContext,
            workspace: _internal?.stepWorkspace,
            logger,
            toolName: inputData.toolName,
          }));
        // §4.2e additive reasons: tool-owned reasons (tool-config / tool-fn) from
        // the requirement, plus a `policy` reason when the session permission gate
        // forces `ask` AND per-run `yolo` did not clear it. Surfaced on the approval
        // chunk + suspend payload so the pending approval can show WHY (matches the
        // durable agent path). Tool-owned reasons survive yolo.
        const policyAsk = toolPermissionPolicy === 'ask' && !yoloAutoApprove;
        const approvalReasons: string[] = [...approvalRequirement.reasons];
        if (policyAsk) approvalReasons.push('policy');
        // The cached requirement covers only tool-owned and run-level approval. The
        // session policy `ask` is additive and never cached (so per-turn yolo can
        // still clear it); a cached `required: false` must never short-circuit it.
        const toolRequiresApproval = approvalRequirement.required || policyAsk;

        // execute() is intentionally deferred until after approval, but its
        // schema validation must not be deferred with it. Otherwise an invalid
        // provider call can be presented to a user, accepted, and only then
        // collapse into a validation result on the resumed leg. Return that
        // same result to the model now so it can repair the call before anyone
        // is asked to approve it. Keep execute() validation as the final
        // authority and do not reuse transformed data here: transforms are not
        // guaranteed to be idempotent.
        if (toolRequiresApproval && resumeData === undefined && typeof validateInput === 'function') {
          const preflightValidation = await validateInput(args);
          if (preflightValidation.error !== undefined) {
            return {
              result:
                preflightValidation.error instanceof Error
                  ? serializeToolError(preflightValidation.error)
                  : ensureSerializable(preflightValidation.error),
              ...inputData,
            };
          }
        }

        // On resume, the live `requireToolApproval` policy may be gone: function-form
        // policies do not survive RequestContext serialization, and decline/approve
        // helpers typically only pass `{ runId, toolCallId }` — not the original option.
        // The suspend payload still records that this step waited for approval, so treat
        // that as authoritative for the resume decision (especially declines).
        //
        // Nested sub-agent/workflow approvals also write `requireToolApproval` on the
        // outer suspend payload, but they additionally set `suspendedToolRunId`. Those
        // must resume into the nested tool path — not the outer approval short-circuit —
        // even when a live outer `requireToolApproval` policy is still present.
        const isDelegatedApproval = authoritativeDelegatedRunId !== undefined;
        const suspendedForApproval = Boolean(
          suspendData &&
          typeof suspendData === 'object' &&
          (suspendData as { requireToolApproval?: unknown }).requireToolApproval &&
          !isDelegatedApproval,
        );
        // `approvalDecision` / `hasApprovalResumeShape` are resolved above from the
        // identity-verified resume envelope (authoritative `toolCallResume` or the
        // persisted approval metadata), which is this fork's trust boundary for a
        // resume decision: an approval-typed resume whose identity does not match is
        // rejected as invalid resume evidence before reaching this gate.
        const isApprovalResume =
          resumeData != null && typeof resumeData === 'object' && 'approved' in (resumeData as Record<string, unknown>);
        // Gate the resume branch on either a live policy or a prior outer approval suspend.
        // Without this, `declineToolCall` falls through to `execute` when the policy was
        // lost (#20470). Do not key only on `approved` in resumeData — generic tool
        // resumes can carry that field for unrelated reasons (same guard as durable).
        const approvalGated =
          !isDelegatedApproval && (toolRequiresApproval || (suspendedForApproval && isApprovalResume));

        // Schema for tool call approval - used for both streaming and metadata
        const approvalInputSchema = z.object({
          approved: z
            .boolean()
            .describe(
              'Controls if the tool call is approved or not, should be true when approved and false when declined',
            ),
          reason: z
            .string()
            .optional()
            .describe('Optional explanation for the decision, surfaced to the model when the tool call is declined'),
        });
        const approvalSchema = toStandardSchema(approvalInputSchema);
        const toolGateApprovalSchema = toStandardSchema(
          supportsEditedApprovalArgs
            ? approvalInputSchema.extend({
                editedArgs: toolApprovalEditedArgsSchema.optional().describe('Shallow JSON input patch'),
              })
            : approvalInputSchema,
        );

        // The real suspension sequence, extracted so it has exactly one implementation with
        // two entry points: the tool's own `suspend()` closure below, and the hand-back of an
        // eager attempt that suspended at runtime. Defined here because it closes over
        // `args`, `transformChunk`, `flushMessagesBeforeSuspension` and `approvalSchema`, and
        // called before approval gating so a handed-back call is never re-gated or re-run.
        const raiseToolSuspension = async (suspendPayload: any, options?: SuspendOptions): Promise<any> => {
          const delegatedSuspendedToolCallId =
            isAgentTool && typeof options?.suspendedToolCallId === 'string' && options.suspendedToolCallId.length > 0
              ? options.suspendedToolCallId
              : undefined;
          if (options?.requireToolApproval) {
            const innerApproval =
              typeof options.requireToolApproval === 'object' && options.requireToolApproval
                ? options.requireToolApproval
                : typeof suspendPayload?.requireToolApproval === 'object' && suspendPayload?.requireToolApproval
                  ? suspendPayload.requireToolApproval
                  : null;

            const approvalToolName = innerApproval?.toolName ?? inputData.toolName;
            const approvalArgs = innerApproval?.args !== undefined ? innerApproval.args : inputData.args;

            await stopGoalActivity({
              agentId,
              runId,
              now: readScoped(scopeCtx, NOW_KEY, 'now'),
            });
            const approvalChunk = await transformChunk(
              {
                type: 'tool-call-approval',
                runId,
                from: ChunkFrom.AGENT,
                payload: {
                  ...expectedResumeIdentity,
                  type: 'approval',
                  approvalSource: 'tool-execution',
                  toolCallId: metadataToolCallId,
                  toolName: approvalToolName,
                  args: approvalArgs,
                  parentToolName: inputData.toolName,
                  parentArgs: inputData.args,
                  resumeSchema: JSON.stringify(standardSchemaToJSONSchema(approvalSchema)),
                },
              },
              'approval',
            );
            if (outputWriter) {
              await outputWriter(approvalChunk);
            } else {
              safeEnqueue(controller, approvalChunk);
            }

            // Add approval metadata to message before persisting
            addToolMetadata({
              toolCallId: metadataToolCallId,
              toolName: approvalToolName,
              args: approvalArgs,
              ...(approvalToolName !== inputData.toolName || approvalArgs !== inputData.args
                ? { parentToolName: inputData.toolName, parentArgs: inputData.args }
                : {}),
              type: 'approval',
              approvalSource: 'tool-execution',
              suspendedToolRunId: options.runId,
              resumeSchema: JSON.stringify(standardSchemaToJSONSchema(approvalSchema)),
              metadata: approvalChunk.metadata,
            });

            // Flush messages before suspension to ensure they are persisted
            await flushMessagesBeforeSuspension();

            return suspend(
              {
                // Top-level type marker (upstream contract): delegated-approval
                // resume routing keys off `type: 'approval'` next to the
                // identity envelope below. Both shapes are always written so
                // envelope readers and marker readers agree.
                type: 'approval',
                toolCallResume: {
                  ...expectedResumeIdentity,
                  ...(approvedArgsResume ?? {}),
                  type: 'approval',
                  approvalSource: 'tool-execution',
                },
                requireToolApproval: {
                  toolCallId: metadataToolCallId,
                  toolName: approvalToolName,
                  args: approvalArgs,
                },
                __streamState: streamState.serialize(),
                __agentId: agentId,
                ...(agentVersionId ? { __agentVersionId: agentVersionId } : {}),
                // Persist the inner suspended run id in the workflow snapshot, partitioned per
                // tool call (resumeLabel = toolCallId). Persisted message metadata exposes the
                // same id as delegatedRunId for cold reloads, while the snapshot remains the
                // runtime source for routing this targeted resume.
                suspendedToolRunId: options.runId,
                ...(delegatedSuspendedToolCallId ? { suspendedToolCallId: delegatedSuspendedToolCallId } : {}),
              },
              {
                resumeLabel: metadataToolCallId,
              },
            );
          } else {
            const suspensionChunk = await transformChunk(
              {
                type: 'tool-call-suspended',
                runId,
                from: ChunkFrom.AGENT,
                payload: {
                  ...expectedResumeIdentity,
                  type: 'suspension',
                  ...(approvalGrant ?? {}),
                  toolCallId: metadataToolCallId,
                  toolName: inputData.toolName,
                  suspendPayload,
                  args,
                  resumeSchema: options?.resumeSchema,
                },
              },
              'suspend',
              { suspendPayload },
            );
            safeEnqueue(controller, suspensionChunk);

            // Add suspension metadata to message before persisting
            addToolMetadata({
              toolCallId: metadataToolCallId,
              toolName: inputData.toolName,
              args: approvedArgsResume?.approvedArgs ?? args,
              suspendPayload,
              suspendedToolRunId: options?.runId,
              type: 'suspension',
              approval: approvalGrant?.approval,
              ...(approvedArgsResume ?? {}),
              resumeSchema: options?.resumeSchema,
              metadata: suspensionChunk.metadata,
            });

            // Flush messages before suspension to ensure they are persisted
            await flushMessagesBeforeSuspension();

            return await suspend(
              {
                toolCallResume: {
                  ...expectedResumeIdentity,
                  ...(approvedArgsResume ?? {}),
                  type: 'suspension',
                  ...(approvalGrant ?? {}),
                },
                toolCallSuspended: suspendPayload,
                __streamState: streamState.serialize(),
                __agentId: agentId,
                ...(agentVersionId ? { __agentVersionId: agentVersionId } : {}),
                toolCallId: metadataToolCallId,
                toolName: inputData.toolName,
                resumeLabel: options?.resumeLabel,
                suspendedToolRunId: options?.runId,
                ...(delegatedSuspendedToolCallId ? { suspendedToolCallId: delegatedSuspendedToolCallId } : {}),
              },
              {
                resumeLabel: metadataToolCallId,
              },
            );
          }
        };

        // An eager attempt that suspended at runtime hands its intent back here. The body
        // already ran once on that attempt, so raise the real suspension from this — the
        // owning foreach iteration — instead of running the tool again. The intent wins over
        // any value or error the tool produced after swallowing the bailout.
        // This deliberately skips `approvalGated` below: an approval-requiring tool is never
        // dispatched eagerly (the eligibility check excludes every approval source), so there is
        // no approval decision to re-make here. A runtime `suspend({ requireToolApproval })` is
        // still honoured — the intent's options carry it into the approval branch of the helper.
        if (eagerSuspensionIntent) {
          return await raiseToolSuspension(eagerSuspensionIntent.suspendPayload, eagerSuspensionIntent.options);
        }

        if (approvalGated) {
          // An authenticated resume (suspension payload, delegated approval)
          // executes even when it carries no approval decision: re-gating it
          // would suspend again instead of resuming. Fresh gated calls (no
          // resume at all) still suspend for approval here.
          if (!approvalDecision && !isAnyResume) {
            await stopGoalActivity({
              agentId,
              runId,
              now: readScoped(scopeCtx, NOW_KEY, 'now'),
            });
            const approvalChunk = await transformChunk(
              {
                type: 'tool-call-approval',
                runId,
                from: ChunkFrom.AGENT,
                payload: {
                  ...expectedResumeIdentity,
                  type: 'approval',
                  approvalSource: 'tool-gate',
                  toolCallId: inputData.toolCallId,
                  toolName: inputData.toolName,
                  args: inputData.args,
                  resumeSchema: JSON.stringify(standardSchemaToJSONSchema(toolGateApprovalSchema)),
                  ...(approvalReasons.length > 0 ? { approvalReasons } : {}),
                },
              },
              'approval',
            );
            if (outputWriter) {
              await outputWriter(approvalChunk);
            } else {
              safeEnqueue(controller, approvalChunk);
            }

            // Add approval metadata to message before persisting
            addToolMetadata({
              toolCallId: inputData.toolCallId,
              toolName: inputData.toolName,
              args: inputData.args,
              type: 'approval',
              approvalSource: 'tool-gate',
              resumeSchema: JSON.stringify(standardSchemaToJSONSchema(toolGateApprovalSchema)),
              metadata: approvalChunk.metadata,
            });

            // Flush messages before suspension to ensure they are persisted
            await flushMessagesBeforeSuspension();

            return suspend(
              {
                toolCallResume: {
                  ...expectedResumeIdentity,
                  ...(approvedArgsResume ?? {}),
                  type: 'approval',
                  approvalSource: 'tool-gate',
                },
                requireToolApproval: {
                  toolCallId: inputData.toolCallId,
                  toolName: inputData.toolName,
                  args: inputData.args,
                  ...(approvalReasons.length > 0 ? { approvalReasons } : {}),
                },
                __streamState: streamState.serialize(),
                __agentId: agentId,
                ...(agentVersionId ? { __agentVersionId: agentVersionId } : {}),
              },
              {
                resumeLabel: inputData.toolCallId,
              },
            );
          } else if (!isAnyResume) {
            await removeToolMetadata({ toolCallId: metadataToolCallId, toolName: inputData.toolName }, 'approval');

            // Return the approval decision (not a `result` string) so it persists as
            // `state: 'output-denied'` with `approval`. The denial reason carries the
            // caller-supplied reason when one was provided, otherwise the default string
            // so downstream consumers/UI keep the same message.
            if (!hasApprovalResumeShape || approvalDecision.approved === false) {
              return {
                ...inputData,
                args: identityArgs,
                ...(metadataToolCallId !== inputData.toolCallId ? { resumeTargetToolCallId: metadataToolCallId } : {}),
                approval: {
                  id: metadataToolCallId,
                  approved: false,
                  reason: approvalDeclineReason,
                },
              };
            }
          }
        }

        // Avoid passing approval sentinels to tools. Delegated agent/workflow
        // resumes still receive their resume data so wrappers call resumeStream
        // instead of starting the sub-run from scratch.
        const shouldTreatResumeDataAsApproval =
          hasApprovalResumeShape && !isKnownSuspensionResume && !isDelegatedApprovalResume;
        // Preserve the approval decision on resolved output so persistence can
        // distinguish an approved gated call from an ordinary tool result.
        // `isKnownApprovalResume` (not only the live policy) keeps
        // approve-after-policy-loss tagging intact. The grant additionally requires
        // `isApprovalResumeData`: consent arrives only through the authenticated workflow
        // boundary (identity-verified, non-model-authored approval resume). A model-authored
        // `approved: true` on a fresh or unverified call is rejected as invalid resume evidence
        // above and must never be credited with a grant here.
        approvalGrant =
          isApprovalResumeData && shouldTreatResumeDataAsApproval && approvalDecision?.approved === true
            ? {
                approval: {
                  id: metadataToolCallId,
                  approved: true,
                  ...(approvalDecision.reason !== undefined ? { reason: approvalDecision.reason } : {}),
                },
              }
            : persistedApprovalGrant
              ? { approval: persistedApprovalGrant }
              : undefined;
        const shouldStripApprovalResumeData =
          (toolRequiresApproval || isKnownApprovalResume) &&
          shouldTreatResumeDataAsApproval &&
          !isToolExecutionApprovalResume;
        const resumeDataToPassToToolOptions = shouldStripApprovalResumeData ? undefined : resumeData;
        const toolRequestContext = buildToolRequestContext(requestContext, {
          runId,
          toolCallId: inputData.toolCallId,
        });

        const toolOptions: MastraToolInvocationOptions = {
          abortSignal,
          runId,
          toolCallId: inputData.toolCallId,
          // Agent tools receive the exact processor-adjusted prompt visible to the parent model.
          // Regular tools retain the input-only context expected by the AI SDK tool contract.
          messages: isAgentTool
            ? (readScoped(scopeCtx, STEP_MODEL_MESSAGES_KEY, 'stepModelMessages') ?? messageList.get.all.aiV5.model())
            : messageList.get.input.aiV5.model(),
          outputWriter,
          // Pass current step span as parent for tool call spans
          tracingContext: modelSpanTracker?.getTracingContext(),
          // Pass workspace from the run scope (set by llmExecutionStep via prepareStep/processInputStep)
          workspace: readScoped(scopeCtx, STEP_WORKSPACE_KEY, 'stepWorkspace'),
          // Forward requestContext so tools receive values set by the workflow step
          requestContext: toolRequestContext,
          actor,
          mcp,
          // Let tools that read thread history mid-stream (e.g. forked subagents
          // cloning the parent thread) drain the save queue so the store reflects
          // the latest user/assistant messages before they read.
          flushMessages: (() => {
            const sqm = readScoped(scopeCtx, SAVE_QUEUE_MANAGER_KEY, 'saveQueueManager');
            const tid = readScoped(scopeCtx, THREAD_ID_KEY, 'threadId');
            const mcfg = readScoped(scopeCtx, MEMORY_CONFIG_KEY, 'memoryConfig');
            return sqm && tid ? () => sqm.flushMessages(messageList, tid, mcfg) : undefined;
          })(),
          suspend: async (suspendPayload: any, options?: SuspendOptions) => {
            // A tool can suspend at runtime without declaring a suspend schema, so the
            // eager eligibility whitelist cannot see it coming. Bail here, before any
            // suspension side effect (chunk, metadata, flush) has happened, so the call
            // is genuinely handed back to the ordinary foreach rather than half-suspended
            // on this path. Bailing after the chunk was emitted would leave a suspension
            // announced that never suspends.
            if (isEagerExecution) {
              // Recorded before the throw, because the throw is not enough on its own: it
              // unwinds through the tool's body, and a tool that catches it would otherwise
              // return normally and have that return adopted as the call's result.
              const reason = `"${inputData.toolName}" requested suspension`;
              // What the tool asked for rides out with the bailout, so the owning foreach
              // iteration can raise the real suspension instead of running the body again.
              // An explicit pick, not the whole `SuspendOptions`, which is unbounded.
              const suspension: EagerSuspensionIntent = {
                suspendPayload,
                options: {
                  resumeLabel: options?.resumeLabel,
                  resumeSchema: options?.resumeSchema,
                  runId: options?.runId,
                  requireToolApproval: options?.requireToolApproval,
                },
              };
              if (eagerBailout) {
                eagerBailout.reason = reason;
                eagerBailout.suspension = suspension;
              }
              throw new EagerToolExecutionNotRun(reason, {
                inputAvailableCalled: eagerBailout?.inputAvailableCalled,
                suspension,
              });
            }
            return await raiseToolSuspension(suspendPayload, options);
          },
          resumeData: resumeDataToPassToToolOptions,
          // The payload this tool call suspended with (see `toolCallSuspended` above), so a
          // resumed tool can continue from its own state instead of re-deriving it.
          ...(resumeDataToPassToToolOptions != null &&
          suspendData &&
          typeof suspendData === 'object' &&
          'toolCallSuspended' in suspendData
            ? { suspendPayload: (suspendData as { toolCallSuspended?: unknown }).toolCallSuspended }
            : {}),
        };

        let parentStepSuspended = false;
        const requestParentSuspension = async (
          suspendPayload: unknown,
          suspendOptions?: SuspendOptions,
        ): Promise<InnerOutput> => {
          if (parentStepSuspended) return undefined as unknown as InnerOutput;
          await toolOptions.suspend?.(suspendPayload, suspendOptions);
          parentStepSuspended = true;
          // `toolOptions.suspend` has already registered the parent step's
          // suspension. Return the branded workflow sentinel so this path
          // cannot be interpreted as a successful tool result.
          return undefined as unknown as InnerOutput;
        };
        // Pre-strip captures from validation above: `args` was already stripped
        // of these coordinates there, so re-reading them here would always see
        // `undefined` and the framework resolver could never authenticate a
        // model-driven delegated resume. Framework resumes ignore model claims entirely
        // (single effective target established above), so only model-driven claims are forwarded.
        const modelSuppliedSuspendedToolCallId = isResumeToolCall ? modelSuspendedToolCallIdClaim : undefined;
        const modelSuppliedSuspendedToolRunId = isResumeToolCall ? modelSuspendedToolRunIdClaim : undefined;
        if (args && typeof args === 'object') {
          delete args.suspendedToolCallId;
          delete args.suspendedToolRunId;
        }

        // Delegated identity is trusted only after it is tied to framework-persisted
        // suspension state. Defined, not nullish: an authenticated delegated resume may carry a
        // `null` payload (`resume(null)` is a valid resume), so the lookup must still run for
        // authenticated `null`s and resolve the exact persisted identity before metadata retirement.
        // Only an evidence-free provider null — already normalized to `undefined` above when no
        // workflow, durable, or caller-supplied resume evidence exists — stays a fresh placeholder.
        // `isAnyResume` additionally covers authenticated resumes that carry no payload at all
        // (`resume(undefined)` must still wake the suspended delegation instead of starting a fresh run).
        const needsRunIdLookup =
          (resumeDataToPassToToolOptions !== undefined || isAnyResume) && (isAgentTool || isWorkflowTool);
        let resolvedSuspensionIdentity: ResolvedSuspendedToolIdentity | undefined;
        if (needsRunIdLookup) {
          resolvedSuspensionIdentity = resolveFrameworkSuspendedToolIdentity({
            toolCallId: metadataToolCallId,
            toolName: inputData.toolName,
            resumeSource: isResumeToolCall ? 'model' : 'framework',
            modelSuppliedSuspendedToolCallId: isResumeToolCall ? modelSuppliedSuspendedToolCallId : undefined,
            modelSuppliedSuspendedToolRunId: isResumeToolCall ? modelSuppliedSuspendedToolRunId : undefined,
            suspendData,
            messages: messageList.get.all.db(),
          });
          // Single-target enforcement: identity was already authenticated for metadataToolCallId
          // above. A resolver result for any other call is never routed or retired — it is ignored
          // so validation, routing, and retirement stay bound to the same authenticated target.
          if (resolvedSuspensionIdentity && resolvedSuspensionIdentity.toolCallId !== metadataToolCallId) {
            resolvedSuspensionIdentity = undefined;
          }
          if (
            resolvedSuspensionIdentity &&
            storedResumeMetadata?.runId !== undefined &&
            resolvedSuspensionIdentity.runId !== storedResumeMetadata.runId
          ) {
            resolvedSuspensionIdentity = undefined;
          }

          if (resolvedSuspensionIdentity) {
            // Agentic execution disables input validation, so a resumed call can carry
            // `args: null`; guard the mutation so the invalid-arguments check below still runs.
            if (args && typeof args === 'object') {
              args.suspendedToolRunId = resolvedSuspensionIdentity.runId;
            }
            toolOptions.suspendedToolRunId = resolvedSuspensionIdentity.runId;
          } else if (
            (isAgentTool || isWorkflowTool) &&
            storedResumeMetadata?.identityMatches === true &&
            storedResumeMetadata.runId !== undefined
          ) {
            // Identity was authenticated before cleanup, so the verified stored run is re-applied
            // when the shared resolver has nothing left to authenticate (e.g. cleanup already
            // retired the entry). Only identity-verified stored runs qualify.
            if (args && typeof args === 'object') {
              args.suspendedToolRunId = storedResumeMetadata.runId;
            }
            toolOptions.suspendedToolRunId = storedResumeMetadata.runId;
          }
          // Agent delegation resumes need both coordinates of the child
          // suspension. The run id selects the child agentic-loop snapshot;
          // this label selects the exact tool call inside that snapshot. It is
          // durable per foreach iteration, unlike shared message metadata.
          const childToolCallId =
            (suspendData as any)?.suspendedToolCallId ?? (suspendData as any)?.toolCallSuspended?.toolCallId;
          if (isAgentTool && typeof childToolCallId === 'string' && childToolCallId.length > 0) {
            args.suspendedToolCallId = childToolCallId;
            // The builder strips model-authored resume coordinates from args, so
            // carry the framework-resolved child call id on the options as well.
            toolOptions.suspendedToolCallId = childToolCallId;
          }
        }

        // Clear the suspension entry for BOTH resume conventions: `resumeData` embedded in the
        // LLM's re-emitted args (autoResumeSuspendedTools) and the workflow-level resumeData that
        // `agent.resumeStream(resumeData, { runId, toolCallId })` delivers. `isResumeToolCall` only
        // covers the former — it stays args-specific because the runId lookup above depends on
        // that narrower meaning.
        // Nullish, not truthy: `false` / `0` / `''` are valid resume payloads for a tool whose
        // resumeSchema is a primitive (e.g. a boolean decline), and they must clear the entry too.
        //
        // Keyed on `approvalGated`, not the live `toolRequiresApproval`, for the same reason as
        // `approvalGrant` above: on an approve-after-policy-loss resume the live policy is gone
        // while the suspension was an approval one, which cleans up its own metadata in the
        // branch above. Using the live policy here would run the generic suspension cleanup on
        // top of it, and `removeToolMetadata`'s toolCallId -> toolName fallback could then drop a
        // concurrently suspended sibling that shares this tool name.
        if ((!approvalGated || isAnyResume) && resumeData != null) {
          // Single-target retirement: always retire the authenticated effective target established
          // before lookup. Never retire an independently resolved other call.
          const cleanupTarget = { toolCallId: metadataToolCallId, toolName: inputData.toolName };
          if (cleanupTarget) {
            await removeToolMetadata(
              cleanupTarget,
              resolvedSuspensionIdentity?.type ?? storedResumeMetadata?.type ?? 'suspension',
            );
          }
        }

        if (args === null || args === undefined) {
          return {
            error: serializeToolError(
              new Error(
                `Tool "${inputData.toolName}" received invalid arguments — the provided JSON could not be parsed. Please provide valid JSON arguments.`,
              ),
            ),
            ...inputData,
          };
        }

        if (isAgentTool) {
          if (typeof args === 'object' && args !== null && 'prompt' in args) {
            args.threadId = readScoped(scopeCtx, THREAD_ID_KEY, 'threadId');
            args.resourceId = readScoped(scopeCtx, RESOURCE_ID_KEY, 'resourceId');
          }
        }

        // FGA authorization check before tool execution
        const toolFgaProvider = mastra?.getServer?.()?.fga;
        if (toolFgaProvider) {
          const fgaUser = requestContext?.get('user');
          const { builtToolEnforcesFGAProvider, checkFGA, getBuiltToolFGAResourceId, getStandaloneToolFGAResourceId } =
            await import('../../../auth/ee/fga-check');
          const builtResourceId = getBuiltToolFGAResourceId(tool);
          // CoreToolBuilder owns the check only when it will use this exact
          // provider. Converted tools without a builder provider (for example,
          // browser tools) retain their canonical identity but are authorized
          // here; raw tools fail closed to the standalone identity.
          if (!builtToolEnforcesFGAProvider(tool, toolFgaProvider)) {
            await checkFGA({
              fgaProvider: toolFgaProvider,
              user: fgaUser,
              resource: {
                type: 'tool',
                id: builtResourceId ?? getStandaloneToolFGAResourceId(inputData.toolName),
              },
              permission: MastraFGAPermissions.TOOLS_EXECUTE,
              actor,
              requestContext: toolRequestContext,
            });
          }
        }

        const llmBgOverrides =
          typeof args === 'object' && args !== null && '_background' in args ? args._background : undefined;

        if (llmBgOverrides) {
          delete args._background;
        }

        // --- Background task dispatch ---
        const agentBgConfig = readScoped(scopeCtx, AGENT_BACKGROUND_CONFIG_KEY, 'agentBackgroundConfig');
        // Settled by `onResult` once the authoritative background result has
        // been reconciled into the message list (or reconciliation threw).
        // Lives outside `taskContext` because the `awaited` disposition below
        // must block the turn on it after the ladder returns.
        let resolveReconciliation!: (outcome: { error?: unknown }) => void;
        const reconciliationComplete = new Promise<{ error?: unknown }>(resolve => {
          resolveReconciliation = resolve;
        });
        const backgroundResultMetadata = (taskId: string, status: 'running' | 'completed' | 'failed') => ({
          ...inputData.providerMetadata,
          mastra: {
            ...inputData.providerMetadata?.mastra,
            backgroundTask: { taskId, status },
          },
        });
        const bgOutcome = await dispatchBackgroundTool({
          // The in-process engine's released contract: no
          // checkIfRunning probe (dispatch here is only re-entered by caller
          // action, never redelivered), and a dispatch failure propagates as
          // a tool error instead of silently degrading to sync execution.
          existingRunningTask: 'dispatch-duplicate',
          dispatchFailure: 'propagate',
          backgroundTaskManager: readScoped(scopeCtx, BACKGROUND_TASK_MANAGER_KEY, 'backgroundTaskManager'),
          agentBackgroundConfig: agentBgConfig,
          managerConfig: readScoped(scopeCtx, BACKGROUND_TASK_MANAGER_CONFIG_KEY, 'backgroundTaskManagerConfig'),
          toolBackgroundConfig: (tool as any).backgroundConfig as ToolBackgroundConfig | undefined,
          llmBgOverrides,
          args,
          toolName: inputData.toolName,
          toolCallId: inputData.toolCallId,
          agentId,
          threadId: readScoped(scopeCtx, THREAD_ID_KEY, 'threadId'),
          resourceId: readScoped(scopeCtx, RESOURCE_ID_KEY, 'resourceId'),
          runId,
          resumeData: resumeDataToPassToToolOptions,
          // Carry the verified resume intent separately from the payload, as the durable caller
          // already does: an authenticated resume with no payload at all (`resume(undefined)` is a
          // valid resume) must still wake the suspended background task instead of dispatching a
          // duplicate. Payload presence alone is not the resume discriminator; `isAnyResume` is true
          // only for identity-verified approval/suspension/delegated resumes, so fresh calls still
          // dispatch their own task.
          isAuthenticatedResume: isAnyResume,
          // Awaited resumes wait for task admission; cancel the wait with the run.
          // (Durable omits this: it waits via waitForNextTask instead.)
          waitForResumeAdmission: true,
          abortSignal,
          // Fork (PF-4402): the permission hook cannot survive worker transport —
          // persist the requirement so a statically-resolved executor fails closed.
          requiresToolPermissionHook: typeof onBeforeToolExecution === 'function',
          logger,
          emitTaskStarted: async task => {
            // Emit background-task-started chunk. Use safeEnqueue: the
            // agent stream may have closed by the time this fires (e.g.
            // when the controller closes mid-dispatch in a long-lived
            // streamUntilIdle wrapper) — without the guard, the throw
            // bubbles up through the AI-SDK-v5 tool builder and gets
            // wrapped as `TOOL_EXECUTION_FAILED: Invalid state:
            // Controller is already closed`.
            const backgroundTaskStartedChunk = {
              type: 'background-task-started' as const,
              runId,
              from: ChunkFrom.AGENT,
              payload: {
                taskId: task.id,
                toolName: inputData.toolName,
                toolCallId: inputData.toolCallId,
              },
            };
            safeEnqueue(controller, backgroundTaskStartedChunk);
            try {
              await options?.onChunk?.(backgroundTaskStartedChunk);
            } catch (error) {
              logger?.warn?.('Error invoking onChunk for background-task-started', {
                toolCallId: inputData.toolCallId,
                toolName: inputData.toolName,
                error,
                errorMessage: error instanceof Error ? error.message : undefined,
                errorStack: error instanceof Error ? error.stack : undefined,
              });
            }
          },
          taskContext: info => {
            const toolBgConfig = (tool as any).backgroundConfig as ToolBackgroundConfig | undefined;
            // Resolve the tool executor from the current closure
            const stepTools = (readScoped(scopeCtx, STEP_TOOLS_KEY, 'stepTools') as Tools | undefined) || tools;
            const resolvedTool =
              stepTools?.[inputData.toolName] ||
              Object.values(stepTools || {})?.find((t: any) => 'id' in t && t.id === inputData.toolName);
            if (!resolvedTool?.execute) {
              throw new ToolNotFoundError(inputData.toolName);
            }
            let backgroundChunkTransformQueue: Promise<void> = Promise.resolve();
            const emittedReplayedToolCalls = new Set<string>();

            return {
              // Fork: lets the manager run a queued awaited nested dispatch inline
              // when its own ancestor holds the saturated reservation.
              awaited: info.disposition === 'awaited',
              // Executor — uses the tool from the current closure
              executor: {
                execute: async (
                  bgArgs: Record<string, unknown>,
                  opts?: {
                    abortSignal?: AbortSignal;
                    onProgress?: (chunk: BackgroundTaskProgressChunk) => Promise<void>;
                    suspend?: (data?: unknown, options?: SuspendOptions) => Promise<void>;
                    resumeData?: unknown;
                    suspendedToolRunId?: string;
                  },
                ) => {
                  if (typeof onBeforeToolExecution === 'function') {
                    let attemptDecision: 'allow' | 'deny' | void;
                    try {
                      attemptDecision = await onBeforeToolExecution({
                        toolName: inputData.toolName,
                        toolCallId: inputData.toolCallId,
                        args: bgArgs,
                        // The step-level resume (approval/suspension) counts
                        // even when this attempt itself carries no resumeData —
                        // e.g. an approved call dispatched into background
                        // execution for the first time.
                        isResume: isAnyResume || opts?.resumeData !== undefined,
                        policyDecision: toolPermissionPolicy,
                      });
                    } catch {
                      attemptDecision = 'deny';
                    }
                    if (opts?.abortSignal?.aborted || options?.abortSignal?.aborted) {
                      throw (
                        (
                          (opts?.abortSignal ?? options?.abortSignal) as
                            | (AbortSignal & { reason?: unknown })
                            | undefined
                        )?.reason ?? new DOMException('The operation was aborted.', 'AbortError')
                      );
                    }
                    if (attemptDecision !== undefined && attemptDecision !== 'allow') {
                      notifyToolDenied(deniedNotifyContext, {
                        toolName: inputData.toolName,
                        stage: 'action',
                        toolCallId: inputData.toolCallId,
                      });
                      throw Object.assign(
                        new Error(`Tool "${inputData.toolName}" was denied by the pre-execution permission hook.`),
                        { name: TOOL_PERMISSION_DENIED_ERROR_NAME },
                      );
                    }
                  }
                  // Override the agent loop's `suspend`/`resumeData` (which
                  // would suspend the AGENT run via tool-call-approval) with
                  // the bg-task workflow's, so calling `suspend()` from the
                  // tool pauses the bg-task run instead.
                  const taskId = info.getTaskId()!;
                  const execution = await executeAdoptedBackgroundOperation({
                    taskId,
                    disposition: info.disposition === 'awaited' ? 'awaited' : 'deferred',
                    abortSignal: opts?.abortSignal ?? options?.abortSignal,
                    execute: background =>
                      resolvedTool.execute!(bgArgs, {
                        ...toolOptions,
                        isBackgroundTask: true,
                        background,
                        [BACKGROUND_WORK_CONTEXT]: {
                          originRunId: runId,
                          originToolCallId: inputData.toolCallId,
                          taskId,
                          invocationKind: isAgentTool ? 'agent' : 'tool',
                          disposition: info.disposition === 'awaited' ? 'awaited' : 'deferred',
                        },
                        ...(opts?.resumeData !== undefined ? { resumeData: opts.resumeData } : {}),
                        // Framework-resolved delegated run id recovered from persisted
                        // suspension state (#23739) — never the model-authored one.
                        suspendedToolRunId: opts?.suspendedToolRunId,
                        suspend: async (data?: unknown, options?: SuspendOptions) => {
                          await toolOptions.suspend?.(data, options);
                          parentStepSuspended = true;
                          return opts?.suspend?.(data, options);
                        },
                        outputWriter: async (chunk: any) => {
                          await opts?.onProgress?.(chunk);
                          return toolOptions.outputWriter?.(chunk);
                        },
                        abortSignal: opts?.abortSignal ?? options?.abortSignal,
                      } as any),
                    onCancelError: error => logger?.warn('Failed to cancel adopted background operation', error),
                  });

                  let rawResult = execution.result;
                  if (execution.adopted) {
                    const outputValidation = validateToolOutput(
                      resolveToolOutputValidationSchema(resolvedTool),
                      rawResult,
                      inputData.toolName,
                      false,
                    );
                    rawResult = outputValidation.error ?? outputValidation.data;
                  }
                  const result = ensureSerializable(rawResult);

                  if ('onOutput' in resolvedTool && typeof (resolvedTool as any).onOutput === 'function') {
                    try {
                      await (resolvedTool as any).onOutput({
                        toolCallId: inputData.toolCallId,
                        toolName: inputData.toolName,
                        output: result,
                        abortSignal: opts?.abortSignal,
                      });
                    } catch (error) {
                      logger?.error('Error calling onOutput', error);
                    }
                  }

                  return result;
                },
              },

              // Synthetic tool-call/tool-result emitter. Bg-task lifecycle
              // chunks (running/output/completed/failed/cancelled) are NOT
              // re-emitted here — `bgManager.stream(...)` is the single
              // source of truth for those. We only emit the synthetic
              // tool-call (at dispatch time) and tool-result / tool-error
              // chunks so UIs rendering this stream can show the tool's
              // outcome inline with the conversation.
              onChunk: chunk => {
                backgroundChunkTransformQueue = backgroundChunkTransformQueue
                  .then(async () => {
                    const bgRunId = chunk.payload.runId;
                    const replayKey = `${bgRunId}:${chunk.payload.toolCallId}`;
                    if (
                      (bgRunId !== runId || (bgRunId === runId && workflowResumeData != null)) &&
                      !emittedReplayedToolCalls.has(replayKey)
                    ) {
                      safeEnqueue(
                        controller,
                        await transformChunk(
                          {
                            type: 'tool-call',
                            runId: bgRunId,
                            from: ChunkFrom.AGENT,
                            payload: {
                              toolCallId: chunk.payload.toolCallId,
                              toolName: chunk.payload.toolName,
                              args: inputData.args,
                              providerMetadata: inputData.providerMetadata as ProviderMetadata | undefined,
                              providerExecuted: inputData.providerExecuted,
                              title: getToolTitle(resolvedTool),
                            },
                          },
                          'input-available',
                        ),
                      );
                      emittedReplayedToolCalls.add(replayKey);
                    }

                    if (chunk.type === 'background-task-completed') {
                      safeEnqueue(
                        controller,
                        await transformChunk(
                          {
                            type: 'tool-result',
                            runId: bgRunId,
                            from: ChunkFrom.AGENT,
                            payload: {
                              toolCallId: chunk.payload.toolCallId,
                              toolName: chunk.payload.toolName,
                              args: inputData.args,
                              result: chunk.payload.result,
                              providerMetadata: backgroundResultMetadata(chunk.payload.taskId, 'completed'),
                              providerExecuted: inputData.providerExecuted,
                            },
                          },
                          'output-available',
                        ),
                      );
                    } else if (chunk.type === 'background-task-failed') {
                      safeEnqueue(
                        controller,
                        await transformChunk(
                          {
                            type: 'tool-error',
                            runId: bgRunId,
                            from: ChunkFrom.AGENT,
                            payload: {
                              toolCallId: chunk.payload.toolCallId,
                              toolName: chunk.payload.toolName,
                              error: chunk.payload.error,
                              args: inputData.args,
                              providerMetadata: backgroundResultMetadata(chunk.payload.taskId, 'failed'),
                              providerExecuted: inputData.providerExecuted,
                            },
                          },
                          'error',
                        ),
                      );
                    }
                  })
                  .catch(error => {
                    logger?.warn?.('Error transforming background task stream chunk', {
                      toolCallId: chunk.payload.toolCallId,
                      toolName: chunk.payload.toolName,
                      runId: chunk.payload.runId,
                      error,
                      errorMessage: error instanceof Error ? error.message : undefined,
                      errorStack: error instanceof Error ? error.stack : undefined,
                    });
                  });
              },

              // Result injector — updates the existing tool-invocation in the
              // message list (keyed by toolCallId) with the real result, then
              // flushes to memory. This matters because the initial turn
              // persisted a placeholder ("Background task started...") as the
              // tool-result for the same toolCallId; appending a second
              // tool-result would leave two conflicting entries in memory and
              // the LLM on the next turn would re-dispatch the tool thinking
              // the research was still running.
              onResult: async params => {
                try {
                  await applyBackgroundToolResult({
                    params,
                    currentRunId: runId,
                    hasResumeData: workflowResumeData != null,
                    args: verifiedApprovedArgs ?? args,
                    messageList,
                    approvalGrant: approvalGrant as Record<string, unknown> | undefined,
                    baseProviderMetadata: inputData.providerMetadata as ProviderMetadata | undefined,
                    transformForTranscript: async result => {
                      const transformCarrier = await applyToolPayloadTransformToChunk(
                        {
                          type: params.status === 'failed' ? 'tool-error' : 'tool-result',
                          payload: {
                            toolCallId: params.toolCallId,
                            toolName: params.toolName,
                            args,
                            ...(params.status === 'failed' ? { error: params.error } : { result: params.result }),
                          },
                          metadata: {} as Record<string, any>,
                        },
                        {
                          policy: transformSource.policy,
                          toolTransform: transformSource.toolTransform,
                          logger,
                          transformInput: {
                            providerMetadata: inputData.providerMetadata as Record<string, unknown> | undefined,
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
                        params.status === 'failed' ? 'error' : 'output-available',
                      );
                      return {
                        transcriptArgs: hasTransformedToolPayload(transcriptArgsTransform)
                          ? transcriptArgsTransform.transformed
                          : args,
                        transcriptResult: hasTransformedToolPayload(transcriptResultTransform)
                          ? transcriptResultTransform.transformed
                          : result,
                        providerMetadata: withToolPayloadTransformProviderMetadata(
                          inputData.providerMetadata as ProviderMetadata | undefined,
                          transformCarrier.metadata,
                        ) as ProviderMetadata | undefined,
                      };
                    },
                    toModelOutput: (resolvedTool as { toModelOutput?: (output: unknown) => unknown } | undefined)
                      ?.toModelOutput,
                    generateId: readScoped(scopeCtx, GENERATE_ID_KEY, 'generateId'),
                    logger,
                    flush: async () => {
                      const sqm = readScoped(scopeCtx, SAVE_QUEUE_MANAGER_KEY, 'saveQueueManager');
                      const tid = readScoped(scopeCtx, THREAD_ID_KEY, 'threadId');
                      const mcfg = readScoped(scopeCtx, MEMORY_CONFIG_KEY, 'memoryConfig');
                      // readOnly runs must not persist the patched background result
                      // to memory — mirrors the durable engine's readOnly flush guard.
                      if (sqm && tid && !mcfg?.readOnly) {
                        await sqm.flushMessages(messageList, tid, mcfg);
                      }
                    },
                  });

                  resolveReconciliation({});
                  void notifyBackgroundWorkTerminal(mastra, {
                    originRunId: runId,
                    originToolCallId: params.toolCallId,
                    ...(params.runId !== runId ? { executorRunId: params.runId } : {}),
                    taskId: params.taskId,
                    invocationKind: isAgentTool ? 'agent' : 'tool',
                    disposition: info.disposition === 'awaited' ? 'awaited' : 'deferred',
                    status: params.status === 'failed' ? 'failed' : 'completed',
                  });
                } catch (error) {
                  resolveReconciliation({ error });
                  throw error;
                }
              },
              // Execution injector — records background task lifecycle metadata on the
              // assistant message without changing the model-visible tool result.
              onExecution: async params => {
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
              },

              // Per-task callbacks
              onComplete: toolBgConfig?.onComplete ?? agentBgConfig?.onTaskComplete,
              onFailed: toolBgConfig?.onFailed ?? agentBgConfig?.onTaskFailed,
            };
          },
        });

        if (bgOutcome.status !== 'sync') {
          // `awaited` disposition: block the turn on the authoritative
          // background result instead of returning the placeholder. The
          // reconciliation promise gate ensures `onResult` has patched the
          // message list before the result is returned to the model.
          // Restarted tasks are included (main only had started/resumed —
          // restart-reattach was added during the shared-core extraction):
          // a replayed awaited call still owes the model the real result.
          if (bgOutcome.disposition === 'awaited') {
            const completedTask = await bgOutcome.waitForCompletion({ abortSignal, includeSuspended: true });
            if (completedTask.status === 'suspended') {
              // The task's executor normally requests this parent suspension before the
              // background workflow records its own suspended state. The guard also covers
              // a resumed caller that observes an already-suspended task before execution.
              return await requestParentSuspension(completedTask.suspendPayload);
            }
            // Cancellation deregisters the task context without calling onResult, so there is no reconciliation to await.
            if (completedTask.status !== 'cancelled') {
              const reconciliationTimeoutMs = Number.isFinite(completedTask.timeoutMs)
                ? completedTask.timeoutMs
                : 30_000;
              let reconciliationTimer: ReturnType<typeof setTimeout> | undefined;
              const reconciliationTimeout = new Promise<never>((_, reject) => {
                reconciliationTimer = setTimeout(
                  () => {
                    reject(
                      new Error(
                        `Background task "${completedTask.id}" result reconciliation timed out after ${reconciliationTimeoutMs}ms`,
                      ),
                    );
                  },
                  Math.max(0, reconciliationTimeoutMs),
                );
              });
              try {
                const reconciliation = await raceAgainstAbort(
                  Promise.race([reconciliationComplete, reconciliationTimeout]),
                  options?.abortSignal,
                );
                if (reconciliation.error) {
                  throw reconciliation.error;
                }
              } finally {
                if (reconciliationTimer !== undefined) clearTimeout(reconciliationTimer);
              }
            }

            if (completedTask.status !== 'completed') {
              throw new Error(
                completedTask.error?.message ??
                  `Background task ${completedTask.status.replace('_', ' ')}: ${completedTask.id}`,
              );
            }

            return {
              result: ensureSerializable(completedTask.result),
              ...inputData,
              // Preserve the resume/approval envelope on the authoritative
              // result exactly like the sync path below: the model and the
              // message list observe the same provenance for bg and sync legs.
              ...resumeTarget,
              ...(approvalGrant ?? {}),
              ...(verifiedApprovedArgs ? { approvedArgs: verifiedApprovedArgs } : {}),
              providerMetadata: backgroundResultMetadata(bgOutcome.taskId, 'completed'),
            };
          }
          if (bgOutcome.status === 'started') {
            // Return placeholder result so the LLM can continue
            return {
              result: bgOutcome.placeholder,
              ...inputData,
              ...resumeTarget,
              providerMetadata: backgroundResultMetadata(bgOutcome.taskId, 'running'),
              ...(approvalGrant ?? {}),
              ...(verifiedApprovedArgs ? { approvedArgs: verifiedApprovedArgs } : {}),
            };
          }
          return {
            result: bgOutcome.placeholder,
            ...inputData,
            ...resumeTarget,
            providerMetadata: backgroundResultMetadata(bgOutcome.taskId, 'running'),
            ...(approvalGrant ?? {}),
            ...(verifiedApprovedArgs ? { approvedArgs: verifiedApprovedArgs } : {}),
          };
        }

        const outcome = await executeToolCall({
          tool: tool as any,
          args,
          toolOptions,
          toolCallId: inputData.toolCallId,
          toolName: inputData.toolName,
          abortSignal,
          // The tool asked to suspend or bail and then swallowed the throw. Its return value
          // is the return value of a call that was never supposed to complete here, so bail
          // before it is published: `onOutput` is a side effect the foreach will produce
          // again when it runs the call for real. "The eager attempt must not run this" is
          // control flow, not a tool failure: turning it into a resolved `{ error }` would
          // defeat the fail-safe, since adoption awaits the eager promise and would record
          // that error as the tool's result instead of running the call normally.
          assertNotBailedOut: error => {
            if (error !== undefined && eagerToolCallDidNotExecute(error)) {
              throw error;
            }
            if (eagerBailout?.reason) {
              throw new EagerToolExecutionNotRun(eagerBailout.reason, {
                inputAvailableCalled: eagerBailout.inputAvailableCalled,
                // A swallowed suspend produces its rejection here rather than in the closure,
                // so the intent has to be re-attached or the hand-back loses it.
                suspension: eagerBailout.suspension,
              });
            }
          },
          logger,
        });
        if (outcome.status === 'aborted') {
          return {
            aborted: true,
            // The tool threw while the request was aborted: preserve its
            // serialized cancellation payload for terminal-only rendering.
            // (The mapping step still leaves the persisted call incomplete.)
            abortError: serializeToolError(outcome.error),
            ...inputData,
          };
        }
        if (outcome.status === 'error') {
          return {
            error: serializeToolError(outcome.error),
            ...inputData,
            // A failed execution still carries resume/approval provenance so
            // persistence and retries observe the same envelope as successes.
            ...resumeTarget,
            ...(resumedFromSuspension ? { resumedFromSuspension: true as const } : {}),
            ...(approvalGrant ?? {}),
            ...(verifiedApprovedArgs ? { approvedArgs: verifiedApprovedArgs } : {}),
          };
        }

        return {
          result: outcome.result,
          ...inputData,
          ...resumeTarget,
          ...(resumedFromSuspension ? { resumedFromSuspension: true as const } : {}),
          ...(approvalGrant ?? {}),
          ...(verifiedApprovedArgs ? { approvedArgs: verifiedApprovedArgs } : {}),
        };
      } catch (error) {
        // Re-throw FGA authorization errors instead of swallowing them
        if (error instanceof Error && error.name === 'FGADeniedError') {
          throw error;
        }
        // "The eager attempt must not run this" is control flow, not a tool failure.
        // Turning it into a resolved `{ error }` would defeat the fail-safe: adoption
        // awaits the eager promise and would record that error as the tool's result
        // instead of running the call normally.
        if (eagerToolCallDidNotExecute(error)) {
          throw error;
        }
        // The tool caught the bailout and threw something else without a `cause`: the
        // call still bailed, so hand back the recorded intent instead of a tool failure.
        if (eagerBailout?.reason) {
          throw new EagerToolExecutionNotRun(eagerBailout.reason, {
            inputAvailableCalled: eagerBailout.inputAvailableCalled,
            suspension: eagerBailout.suspension,
          });
        }
        // A throw while the request is aborted is a mid-flight cancellation, not a genuine
        // model-visible failure. Recording it as a normal error result would complete the
        // invocation in history and let the model continue after cancellation. Preserve a
        // serialized terminal-only error so stream consumers can still render the tool's
        // authoritative cancellation payload, while the mapping step leaves the persisted
        // call incomplete and bails. Key off the abort signal, not the error type:
        // CoreToolBuilder wraps the AbortError in a TOOL_EXECUTION_FAILED MastraError.
        if (abortSignal?.aborted) {
          // Log the discarded error for observability (control flow unchanged).
          logger?.debug?.('Tool execution interrupted by request abort; leaving the tool call incomplete', {
            toolName: inputData.toolName,
            toolCallId: inputData.toolCallId,
            error: error instanceof Error ? error.message : String(error),
          });
          return {
            aborted: true,
            abortError: serializeToolError(error),
            ...inputData,
          };
        }
        return {
          error: serializeToolError(error),
          ...inputData,
          ...(resumeTargetToolCallId ? { resumeTargetToolCallId } : {}),
          ...(resumedFromSuspension ? { resumedFromSuspension: true as const } : {}),
          ...(approvalGrant ?? {}),
          ...(verifiedApprovedArgs ? { approvedArgs: verifiedApprovedArgs } : {}),
        };
      }
    },
  });
}
