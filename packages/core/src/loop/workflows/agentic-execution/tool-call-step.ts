import { randomUUID } from 'node:crypto';
import type { ToolSet } from '@internal/ai-sdk-v5';
import { z } from 'zod/v4';
import {
  createToolCallIdentityDigest,
  parseToolApprovalDecision,
  parseToolApprovalGrant,
} from '../../../agent/tool-call-identity';
import type { ToolApprovalGrant } from '../../../agent/tool-call-identity';
import { MastraFGAPermissions } from '../../../auth/ee';
import { createBackgroundTask } from '../../../background-tasks/create';
import { resolveBackgroundConfig } from '../../../background-tasks/resolve-config';
import type { BackgroundTaskProgressChunk, ToolBackgroundConfig } from '../../../background-tasks/types';
import type { MastraDBMessage } from '../../../memory';
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
import type { MastraToolInvocationOptions, RequireToolApproval } from '../../../tools/types';
import { ensureSerializable } from '../../../utils';
import type { SuspendOptions } from '../../../workflows/step';
import { createStep } from '../../../workflows/workflow';
import type { OuterLLMRun } from '../../types';
import { ToolNotFoundError } from '../errors';
import { toolCallInputSchema, toolCallOutputSchema } from '../schema';
import { notifyToolDenied } from './tool-permission-notify';

type AddToolMetadataOptions = {
  toolCallId: string;
  toolName: string;
  args: unknown;
  resumeSchema: string;
  suspendedToolRunId?: string;
  approval?: ToolApprovalGrant;
  approvalSource?: 'tool-gate' | 'tool-execution';
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
  mastra,
}: OuterLLMRun<Tools, OUTPUT>) {
  return createStep({
    id: 'toolCallStep',
    inputSchema: toolCallInputSchema,
    outputSchema: toolCallOutputSchema,
    execute: async ({ inputData, suspend, resumeData: workflowResumeData, suspendData, requestContext }) => {
      // Use tools from _internal.stepTools if available (set by llmExecutionStep via prepareStep/processInputStep)
      // This avoids serialization issues - _internal is a mutable object that preserves execute functions
      // Fall back to the original tools from the closure if not set
      const stepTools = (_internal?.stepTools as Tools) || tools;
      const stepActiveTools = _internal?.stepActiveTools;
      const tool =
        stepTools?.[inputData.toolName] ||
        findProviderToolByName(stepTools, inputData.toolName) ||
        Object.values(stepTools || {})?.find((t: any) => `id` in t && t.id === inputData.toolName);
      const transformSource = {
        policy: _internal?.toolPayloadTransform,
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
        suspendPayload,
        resumeSchema,
        type,
        suspendedToolRunId,
        approval,
        approvalSource,
        metadata: toolStateTransformMetadata,
      }: AddToolMetadataOptions) => {
        const metadataKey = type === 'suspension' ? 'suspendedTools' : 'pendingToolApprovals';
        // Find the last assistant message in the response (which should contain this tool call)
        const responseMessages = messageList.get.response.db();
        const lastAssistantMessage = [...responseMessages].reverse().find(msg => msg.role === 'assistant');

        if (lastAssistantMessage) {
          const content = lastAssistantMessage.content;
          if (!content) return;
          // Add metadata to indicate this tool call is pending approval
          const metadata =
            typeof lastAssistantMessage.content.metadata === 'object' && lastAssistantMessage.content.metadata !== null
              ? (lastAssistantMessage.content.metadata as Record<string, any>)
              : {};
          const metadataByToolCallId: Record<string, any> = {};
          if (metadata[metadataKey] && typeof metadata[metadataKey] === 'object') {
            for (const [key, value] of Object.entries(metadata[metadataKey])) {
              Object.defineProperty(metadataByToolCallId, key, {
                configurable: true,
                enumerable: true,
                value,
                writable: true,
              });
            }
          }
          metadata[metadataKey] = metadataByToolCallId;
          const inputTransform = getTransformedToolPayload(toolStateTransformMetadata, 'transcript', 'input-available');
          const approvalTransform = getTransformedToolPayload(toolStateTransformMetadata, 'transcript', 'approval');
          const suspendTransform = getTransformedToolPayload(toolStateTransformMetadata, 'transcript', 'suspend');
          const transformedArgs =
            type === 'approval' && hasTransformedToolPayload(approvalTransform)
              ? approvalTransform.transformed
              : hasTransformedToolPayload(inputTransform)
                ? inputTransform.transformed
                : args;
          const transformedSuspendPayload =
            type === 'suspension' && hasTransformedToolPayload(suspendTransform)
              ? suspendTransform.transformed
              : suspendPayload;
          Object.defineProperty(metadataByToolCallId, toolCallId, {
            configurable: true,
            enumerable: true,
            value: {
              version: 1,
              originRunId: runId,
              stepId: 'toolCallStep',
              toolCallId,
              toolName,
              identityDigest: createToolCallIdentityDigest({ toolCallId, toolName, args }),
              resumeIdentityDigest: createToolCallIdentityDigest({ toolCallId, toolName, args: transformedArgs }),
              args: transformedArgs,
              type,
              runId: suspendedToolRunId ?? runId, // Store the runId so we can resume after page refresh
              ...(approval ? { approval } : {}),
              ...(approvalSource ? { approvalSource } : {}),
              ...(type === 'suspension' ? { suspendPayload: transformedSuspendPayload } : {}),
              resumeSchema,
              ...(toolStateTransformMetadata ? { metadata: toolStateTransformMetadata } : {}),
            },
            writable: true,
          });
          lastAssistantMessage.content.metadata = metadata;
        }
      };

      const removeToolMetadata = async (toolCallId: string, toolName: string, type: 'suspension' | 'approval') => {
        const { saveQueueManager, memoryConfig, threadId } = _internal || {};

        const getMetadata = (message: MastraDBMessage) => {
          const content = message.content;
          if (!content) return undefined;
          const metadata =
            typeof content.metadata === 'object' && content.metadata !== null
              ? (content.metadata as Record<string, any>)
              : undefined;
          return metadata;
        };

        const metadataKey = type === 'suspension' ? 'suspendedTools' : 'pendingToolApprovals';

        // Find and update the assistant message to remove approval metadata
        // At this point, messages have been persisted, so we look in all messages
        const allMessages = messageList.get.all.db();
        const lastAssistantMessage = [...allMessages].reverse().find(msg => {
          const metadata = getMetadata(msg);
          const suspendedTools = metadata?.[metadataKey] as Record<string, any> | undefined;
          const foundTool = Boolean(suspendedTools && Object.hasOwn(suspendedTools, toolCallId));
          if (foundTool) {
            return true;
          }
          const dataToolSuspendedParts = msg.content.parts?.filter(
            part => part.type === 'data-tool-call-suspended' || part.type === 'data-tool-call-approval',
          );
          if (dataToolSuspendedParts && dataToolSuspendedParts.length > 0) {
            const foundTool = dataToolSuspendedParts.find((part: any) => part.data.toolCallId === toolCallId);
            if (foundTool) {
              return true;
            }
          }
          return false;
        });

        if (lastAssistantMessage) {
          const metadata = getMetadata(lastAssistantMessage);
          let suspendedTools = metadata?.[metadataKey] as Record<string, any> | undefined;
          if (!suspendedTools) {
            suspendedTools = lastAssistantMessage.content.parts
              ?.filter(part => part.type === 'data-tool-call-suspended' || part.type === 'data-tool-call-approval')
              ?.reduce(
                (acc, part) => {
                  if (part.type === 'data-tool-call-suspended' || part.type === 'data-tool-call-approval') {
                    acc[(part.data as any).toolCallId] = part.data;
                  }
                  return acc;
                },
                Object.create(null) as Record<string, any>,
              );
          }

          if (suspendedTools && typeof suspendedTools === 'object') {
            if (metadata) {
              delete suspendedTools[toolCallId];
            } else {
              lastAssistantMessage.content.parts = lastAssistantMessage.content.parts?.map(part => {
                if (part.type === 'data-tool-call-suspended' || part.type === 'data-tool-call-approval') {
                  if ((part.data as any).toolCallId === toolCallId) {
                    return {
                      ...part,
                      data: {
                        ...(part.data as any),
                        resumed: true,
                      },
                    };
                  }
                }
                return part;
              });
            }

            // If no more pending suspensions, remove the whole object
            if (metadata && Object.keys(suspendedTools).length === 0) {
              delete metadata[metadataKey];
            }

            // Flush to persist the metadata removal
            if (saveQueueManager && threadId) {
              try {
                await saveQueueManager.flushMessages(messageList, threadId, memoryConfig);
              } catch (error) {
                logger?.error('Error removing tool suspension metadata:', error);
              }
            }
          }
        }
      };

      // Helper function to flush messages before suspension
      const flushMessagesBeforeSuspension = async () => {
        const { saveQueueManager, memoryConfig, threadId, resourceId, memory } = _internal || {};

        if (!saveQueueManager || !threadId) {
          return;
        }

        try {
          // Ensure thread exists before flushing messages
          if (memory && !_internal.threadExists && resourceId) {
            const thread = await memory.getThreadById?.({ threadId });
            if (!thread) {
              // Thread doesn't exist yet, create it now
              await memory.createThread?.({
                threadId,
                resourceId,
                memoryConfig,
              });
            }
            _internal.threadExists = true;
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
          error: new ToolNotFoundError(
            `Tool "${inputData.toolName}" not found.${availableToolsStr}. Call tools by their exact name only — never add prefixes, namespaces, or colons.`,
          ),
          ...inputData,
        };
      }

      if (tool && 'onInputAvailable' in tool) {
        try {
          await tool?.onInputAvailable?.({
            toolCallId: inputData.toolCallId,
            input: inputData.args,
            messages: messageList.get.input.aiV5.model(),
            abortSignal: options?.abortSignal,
          });
        } catch (error) {
          logger?.error('Error calling onInputAvailable', error);
        }
      }

      if (!tool.execute) {
        return inputData;
      }

      let approvalGrant: { approval: ToolApprovalGrant } | undefined;
      let resumeTargetToolCallId: string | undefined;

      try {
        const requireToolApproval = requestContext.get('__mastra_requireToolApproval');

        let resumeDataFromArgs: any = undefined;
        let args: any = inputData.args;

        if (typeof inputData.args === 'object' && inputData.args !== null) {
          const { resumeData: resumeDataFromInput, ...argsFromInput } = inputData.args;
          args = argsFromInput;
          resumeDataFromArgs = resumeDataFromInput;
        }

        const resumeData = resumeDataFromArgs !== undefined ? resumeDataFromArgs : workflowResumeData;

        const isResumeToolCall = resumeDataFromArgs !== undefined;
        const isAgentTool = inputData.toolName?.startsWith('agent-');
        const isWorkflowTool = inputData.toolName?.startsWith('workflow-');
        const suspendedToolCallId =
          isResumeToolCall && typeof args?.suspendedToolCallId === 'string' && args.suspendedToolCallId.length > 0
            ? args.suspendedToolCallId
            : undefined;
        if (suspendedToolCallId !== undefined) {
          const { suspendedToolCallId: _suspendedToolCallId, ...argsWithoutCallId } = args;
          args = argsWithoutCallId;
        }
        const suppliedSuspendedToolRunId =
          isResumeToolCall && typeof args?.suspendedToolRunId === 'string' && args.suspendedToolRunId.length > 0
            ? args.suspendedToolRunId
            : undefined;
        if (suppliedSuspendedToolRunId !== undefined) {
          const { suspendedToolRunId: _suspendedToolRunId, ...argsWithoutRunId } = args;
          args = argsWithoutRunId;
        }
        const metadataToolCallId = suspendedToolCallId ?? inputData.toolCallId;
        let identityArgs = args;
        let expectedIdentityDigest = createToolCallIdentityDigest({
          toolCallId: metadataToolCallId,
          toolName: inputData.toolName,
          args: identityArgs,
        });
        let expectedResumeIdentity = {
          version: 1,
          originRunId: runId,
          stepId: 'toolCallStep',
          toolCallId: metadataToolCallId,
          toolName: inputData.toolName,
          identityDigest: expectedIdentityDigest,
        } as const;
        const matchesExpectedResumeIdentity = (value: unknown) => {
          if (!value || typeof value !== 'object' || Array.isArray(value)) return undefined;
          const record = value as Record<string, unknown>;
          return Object.entries(expectedResumeIdentity).every(
            ([key, expected]) => Object.hasOwn(record, key) && record[key] === expected,
          );
        };
        const getStoredResumeIdentityMatch = (value: unknown, expectedType: 'approval' | 'suspension') => {
          if (!value || typeof value !== 'object' || Array.isArray(value)) return undefined;
          const record = value as Record<string, unknown>;
          const hasValidIdentityEnvelope =
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
            Object.hasOwn(record, 'identityDigest');
          if (!hasValidIdentityEnvelope) return undefined;
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
            runId: storedRecord?.runId,
            originRunId: storedRecord?.originRunId,
            identityMatches: identityMatch !== undefined,
            identityMatch,
            canonicalArgs: storedRecord ? getStoredCanonicalArgs(message, storedRecord) : undefined,
            approval: parseToolApprovalGrant(storedRecord?.approval, metadataToolCallId),
            approvalSource:
              storedRecord?.approvalSource === 'tool-gate' || storedRecord?.approvalSource === 'tool-execution'
                ? storedRecord.approvalSource
                : undefined,
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
        const storedResumeMetadata = resumeData !== undefined ? getStoredResumeMetadata(metadataToolCallId) : undefined;
        const hasSuspendedToolRunIdMismatch =
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
        const authoritativeIdentityMatches =
          hasAuthoritativeResumeEnvelope && matchesExpectedResumeIdentity(authoritativeResumeEnvelope);
        const authoritativeResumeType = authoritativeIdentityMatches
          ? (authoritativeResumeEnvelope as { type?: unknown }).type
          : undefined;
        const authoritativeApprovalSource = authoritativeIdentityMatches
          ? (authoritativeResumeEnvelope as { approvalSource?: unknown }).approvalSource
          : undefined;
        const hasKnownAuthoritativeResumeType =
          authoritativeResumeType === 'approval' || authoritativeResumeType === 'suspension';
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
        const approvalDeclineReason =
          approvalDecision?.reason !== undefined ? approvalDecision.reason : 'Tool call was not approved by the user';
        const isDelegatedApprovalResume =
          hasApprovalResumeShape &&
          (isAgentTool || isWorkflowTool) &&
          effectiveResumeType === 'approval' &&
          storedResumeMetadata?.identityMatches === true &&
          storedResumeMetadata.runId &&
          storedResumeMetadata.originRunId &&
          storedResumeMetadata.runId !== storedResumeMetadata.originRunId &&
          !hasSuspendedToolRunIdMismatch;
        const hasResumeIdentityMismatch =
          (hasAuthoritativeResumeEnvelope && (!authoritativeIdentityMatches || !hasKnownAuthoritativeResumeType)) ||
          (!hasAuthoritativeResumeEnvelope && storedResumeMetadata?.identityMatches === false);
        const isKnownApprovalResume =
          effectiveResumeType === 'approval' &&
          (authoritativeResumeType === 'approval' || storedResumeMetadata?.identityMatches === true) &&
          !isDelegatedApprovalResume;
        const isKnownSuspensionResume =
          effectiveResumeType === 'suspension' &&
          (authoritativeResumeType === 'suspension' || storedResumeMetadata?.identityMatches === true);
        const isApprovalResumeData = hasApprovalResumeShape && isKnownApprovalResume;
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
        const needsCanonicalArgsRestore = storedResumeMetadata?.identityMatch === 'resume';
        const isCanonicalArgsUnavailable =
          needsCanonicalArgsRestore && storedResumeMetadata.canonicalArgs === undefined;
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
          args = structuredClone(storedResumeMetadata.canonicalArgs);
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
        const resumeTarget =
          metadataToolCallId !== inputData.toolCallId ? { resumeTargetToolCallId: metadataToolCallId } : {};
        resumeTargetToolCallId = resumeTarget.resumeTargetToolCallId;

        // Check if approval is required
        // requireApproval can be:
        // - boolean (from Mastra createTool or mapped from AI SDK needsApproval: true)
        // - undefined (no approval needed)
        // If needsApprovalFn exists, evaluate it with the tool args and context
        if (isApprovalResumeData && approvalDecision.approved === false) {
          await removeToolMetadata(metadataToolCallId, inputData.toolName, 'approval');

          return {
            ...inputData,
            args: identityArgs,
            // Keep the provider's current call ID so its invocation receives a result, and carry
            // the original pending call separately so persistence can mark that approval denied.
            ...resumeTarget,
            approval: {
              id: metadataToolCallId,
              approved: false,
              reason: approvalDeclineReason,
            },
          };
        }

        if (isApprovalResumeData) {
          await removeToolMetadata(metadataToolCallId, inputData.toolName, 'approval');
        } else if (isKnownSuspensionResume) {
          await removeToolMetadata(metadataToolCallId, inputData.toolName, 'suspension');
        }

        const validatedSuspendedToolRunId = suppliedSuspendedToolRunId ?? storedResumeMetadata?.runId;
        if ((isAgentTool || isWorkflowTool) && validatedSuspendedToolRunId !== undefined) {
          args.suspendedToolRunId = validatedSuspendedToolRunId;
        }

        // Per-tool permission policy gate (§4.2e). A caller (e.g. the harness)
        // may thread a resolver on the request context that returns
        // 'allow' | 'ask' | 'deny' for a tool name. `deny` blocks the call with a
        // non-aborting result the model can react to; `ask` forces approval (it is
        // OR'd with tool-owned/global approval, never suppressing them); `allow`
        // defers entirely to the tool's own approval config.
        const toolPermissionPolicy = (
          requestContext.get('__mastra_toolPermissionPolicy') as
            | ((toolName: string) => 'allow' | 'ask' | 'deny')
            | undefined
        )?.(inputData.toolName);
        if (toolPermissionPolicy === 'deny') {
          // §O4 — surface WHY a tool was blocked (action-time deny is otherwise
          // opaque: only a generic result reaches the model). Optional, sync,
          // fire-and-forget, isolated; a non-harness caller threads no callback.
          notifyToolDenied(requestContext, {
            toolName: inputData.toolName,
            stage: 'action',
            toolCallId: inputData.toolCallId,
          });
          return {
            ...inputData,
            ...resumeTarget,
            result: `Tool "${inputData.toolName}" was denied by the session permission policy.`,
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

        const approvalRequirement = await resolveToolApprovalRequirement({
          tool,
          args,
          // #17337 — pass through unboxed: the global may be a per-call FUNCTION
          // policy; resolveToolApprovalRequirement evaluates it (fail-safe true).
          requireToolApproval: requireToolApproval as RequireToolApproval | undefined,
          requestContext,
          workspace: _internal?.stepWorkspace,
          logger,
          toolName: inputData.toolName,
        });
        // §4.2e additive reasons: tool-owned reasons (tool-config / tool-fn) from
        // the requirement, plus a `policy` reason when the session permission gate
        // forces `ask` AND per-run `yolo` did not clear it. Surfaced on the approval
        // chunk + suspend payload so the pending approval can show WHY (matches the
        // durable agent path). Tool-owned reasons survive yolo.
        const policyAsk = toolPermissionPolicy === 'ask' && !yoloAutoApprove;
        const approvalReasons: string[] = [...approvalRequirement.reasons];
        if (policyAsk) approvalReasons.push('policy');
        const toolRequiresApproval = approvalRequirement.required || policyAsk;

        // Schema for tool call approval - used for both streaming and metadata
        const approvalSchema = toStandardSchema(
          z.object({
            approved: z
              .boolean()
              .describe(
                'Controls if the tool call is approved or not, should be true when approved and false when declined',
              ),
          }),
        );

        if (toolRequiresApproval) {
          if (resumeData === undefined) {
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
                  resumeSchema: JSON.stringify(standardSchemaToJSONSchema(approvalSchema)),
                  ...(approvalReasons.length > 0 ? { approvalReasons } : {}),
                },
              },
              'approval',
            );
            safeEnqueue(controller, approvalChunk);

            // Add approval metadata to message before persisting
            addToolMetadata({
              toolCallId: inputData.toolCallId,
              toolName: inputData.toolName,
              args: inputData.args,
              type: 'approval',
              approvalSource: 'tool-gate',
              resumeSchema: JSON.stringify(standardSchemaToJSONSchema(approvalSchema)),
              metadata: approvalChunk.metadata,
            });

            // Flush messages before suspension to ensure they are persisted
            await flushMessagesBeforeSuspension();

            return suspend(
              {
                toolCallResume: {
                  ...expectedResumeIdentity,
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
              },
              {
                resumeLabel: inputData.toolCallId,
              },
            );
          } else if (!isApprovalResumeData && !isKnownSuspensionResume && !isDelegatedApprovalResume) {
            await removeToolMetadata(metadataToolCallId, inputData.toolName, 'approval');

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
        approvalGrant =
          (toolRequiresApproval || isKnownApprovalResume) &&
          shouldTreatResumeDataAsApproval &&
          approvalDecision?.approved === true
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
          abortSignal: options?.abortSignal,
          runId,
          toolCallId: inputData.toolCallId,
          // Pass all messages (input + response + memory) so sub-agents (agent-* tools) receive
          // the full conversation context and can make better decisions. Each sub-agent invocation
          // uses a fresh unique thread, so storing this context in that thread is scoped and safe.
          messages: isAgentTool ? messageList.get.all.aiV5.model() : messageList.get.input.aiV5.model(),
          outputWriter,
          // Pass current step span as parent for tool call spans
          tracingContext: modelSpanTracker?.getTracingContext(),
          // Pass workspace from _internal (set by llmExecutionStep via prepareStep/processInputStep)
          workspace: _internal?.stepWorkspace,
          // Forward requestContext so tools receive values set by the workflow step
          requestContext: toolRequestContext,
          // Let tools that read thread history mid-stream (e.g. forked subagents
          // cloning the parent thread) drain the save queue so the store reflects
          // the latest user/assistant messages before they read.
          flushMessages:
            _internal?.saveQueueManager && _internal?.threadId
              ? () => _internal.saveQueueManager!.flushMessages(messageList, _internal.threadId, _internal.memoryConfig)
              : undefined,
          suspend: async (suspendPayload: any, options?: SuspendOptions) => {
            if (options?.requireToolApproval) {
              const approvalChunk = await transformChunk(
                {
                  type: 'tool-call-approval',
                  runId,
                  from: ChunkFrom.AGENT,
                  payload: {
                    ...expectedResumeIdentity,
                    type: 'approval',
                    approvalSource: 'tool-execution',
                    toolCallId: inputData.toolCallId,
                    toolName: inputData.toolName,
                    args: inputData.args,
                    resumeSchema: JSON.stringify(
                      standardSchemaToJSONSchema(
                        toStandardSchema(
                          z.object({
                            approved: z
                              .boolean()
                              .describe(
                                'Controls if the tool call is approved or not, should be true when approved and false when declined',
                              ),
                          }),
                        ),
                      ),
                    ),
                  },
                },
                'approval',
              );
              safeEnqueue(controller, approvalChunk);

              // Add approval metadata to message before persisting
              addToolMetadata({
                toolCallId: inputData.toolCallId,
                toolName: inputData.toolName,
                args: inputData.args,
                type: 'approval',
                approvalSource: 'tool-execution',
                suspendedToolRunId: options.runId,
                resumeSchema: JSON.stringify(
                  standardSchemaToJSONSchema(
                    toStandardSchema(
                      z.object({
                        approved: z
                          .boolean()
                          .describe(
                            'Controls if the tool call is approved or not, should be true when approved and false when declined',
                          ),
                      }),
                    ),
                  ),
                ),
                metadata: approvalChunk.metadata,
              });

              // Flush messages before suspension to ensure they are persisted
              await flushMessagesBeforeSuspension();

              return suspend(
                {
                  toolCallResume: {
                    ...expectedResumeIdentity,
                    type: 'approval',
                    approvalSource: 'tool-execution',
                  },
                  requireToolApproval: {
                    toolCallId: inputData.toolCallId,
                    toolName: inputData.toolName,
                    args: inputData.args,
                  },
                  __streamState: streamState.serialize(),
                },
                {
                  resumeLabel: inputData.toolCallId,
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
                args,
                suspendPayload,
                suspendedToolRunId: options?.runId,
                type: 'suspension',
                approval: approvalGrant?.approval,
                resumeSchema: options?.resumeSchema,
                metadata: suspensionChunk.metadata,
              });

              // Flush messages before suspension to ensure they are persisted
              await flushMessagesBeforeSuspension();

              return await suspend(
                {
                  toolCallResume: {
                    ...expectedResumeIdentity,
                    type: 'suspension',
                    ...(approvalGrant ?? {}),
                  },
                  toolCallSuspended: suspendPayload,
                  __streamState: streamState.serialize(),
                  toolName: inputData.toolName,
                  resumeLabel: options?.resumeLabel,
                },
                {
                  resumeLabel: metadataToolCallId,
                },
              );
            }
          },
          resumeData: resumeDataToPassToToolOptions,
        };

        //if resuming a subAgent or workflow tool, we want to find the runId from when it got suspended.
        // Also look up the runId when the LLM provided resumeData in args (isResumeToolCall)
        // but omitted suspendedToolRunId — without it, workflow tools start a fresh run and re-suspend.
        const needsRunIdLookup = resumeDataToPassToToolOptions !== undefined && (isAgentTool || isWorkflowTool);
        if (needsRunIdLookup) {
          let suspendedToolRunId = '';
          const shouldUsePartsFallback = !isResumeToolCall || !args.suspendedToolRunId;
          const messages = messageList.get.all.db();
          const assistantMessages = [...messages].reverse().filter(message => message.role === 'assistant');

          for (const message of assistantMessages) {
            const suspendedTools = message.content.metadata?.suspendedTools as Record<string, any> | undefined;
            const pendingToolApprovals = message.content.metadata?.pendingToolApprovals as
              | Record<string, any>
              | undefined;
            const storedTool =
              suspendedTools && Object.hasOwn(suspendedTools, metadataToolCallId)
                ? suspendedTools[metadataToolCallId]
                : pendingToolApprovals && Object.hasOwn(pendingToolApprovals, metadataToolCallId)
                  ? pendingToolApprovals[metadataToolCallId]
                  : undefined;
            if (storedTool) {
              suspendedToolRunId = storedTool.runId;
              break;
            }

            if (shouldUsePartsFallback) {
              const dataToolSuspendedParts = message.content.parts?.filter(
                part =>
                  (part.type === 'data-tool-call-suspended' || part.type === 'data-tool-call-approval') &&
                  !(part.data as any).resumed,
              );
              if (dataToolSuspendedParts && dataToolSuspendedParts.length > 0) {
                const foundTool = dataToolSuspendedParts.find(
                  (part: any) => part.data.toolCallId === metadataToolCallId,
                );
                if (foundTool) {
                  suspendedToolRunId = (foundTool as any).data.runId;
                  break;
                }
              }
            }
          }

          if (suspendedToolRunId) {
            args.suspendedToolRunId = suspendedToolRunId;
          }
        }

        if (resumeData !== undefined && (isResumeToolCall || isAgentTool || isWorkflowTool)) {
          await removeToolMetadata(metadataToolCallId, inputData.toolName, 'approval');
          await removeToolMetadata(metadataToolCallId, inputData.toolName, 'suspension');
        }

        if (args === null || args === undefined) {
          return {
            error: new Error(
              `Tool "${inputData.toolName}" received invalid arguments — the provided JSON could not be parsed. Please provide valid JSON arguments.`,
            ),
            ...inputData,
          };
        }

        if (isAgentTool) {
          if (typeof args === 'object' && args !== null && 'prompt' in args) {
            args.threadId = _internal?.threadId;
            args.resourceId = _internal?.resourceId;
          }
        }

        // FGA authorization check before tool execution
        const toolFgaProvider = mastra?.getServer?.()?.fga;
        if (toolFgaProvider) {
          const fgaUser = requestContext?.get('user');
          const { checkFGA, FGADeniedError } = await import('../../../auth/ee/fga-check');
          if (!fgaUser) {
            throw new FGADeniedError(
              { id: 'unknown' },
              { type: 'tool', id: inputData.toolName },
              MastraFGAPermissions.TOOLS_EXECUTE,
            );
          }
          await checkFGA({
            fgaProvider: toolFgaProvider,
            user: fgaUser,
            resource: { type: 'tool', id: inputData.toolName },
            permission: MastraFGAPermissions.TOOLS_EXECUTE,
          });
        }

        const llmBgOverrides =
          typeof args === 'object' && args !== null && '_background' in args ? args._background : undefined;

        if (llmBgOverrides) {
          delete args._background;
        }

        // --- Background task dispatch ---
        const backgroundTaskManager = _internal?.backgroundTaskManager;
        const agentBgConfigCheck = _internal?.agentBackgroundConfig;
        // Skip background dispatch entirely when disabled (e.g., for sub-agents whose
        // entire invocation is itself dispatched as a background task by the parent)
        if (backgroundTaskManager && !agentBgConfigCheck?.disabled && typeof args === 'object' && args !== null) {
          const toolBgConfig = (tool as any).backgroundConfig as ToolBackgroundConfig | undefined;
          const agentBgConfig = agentBgConfigCheck;
          const managerConfig = _internal?.backgroundTaskManagerConfig;

          const bgResolved = resolveBackgroundConfig({
            llmBgOverrides,
            toolName: inputData.toolName,
            toolConfig: toolBgConfig,
            agentConfig: agentBgConfig,
            managerConfig,
          });

          if (bgResolved.runInBackground) {
            // Resolve the tool executor from the current closure
            const stepTools = (_internal?.stepTools as Tools) || tools;
            const resolvedTool =
              stepTools?.[inputData.toolName] ||
              Object.values(stepTools || {})?.find((t: any) => 'id' in t && t.id === inputData.toolName);
            if (!resolvedTool?.execute) {
              throw new ToolNotFoundError(inputData.toolName);
            }
            let backgroundChunkTransformQueue: Promise<void> = Promise.resolve();
            const emittedReplayedToolCalls = new Set<string>();

            // Create a self-contained background task with per-stream hooks
            const bgTask = createBackgroundTask(backgroundTaskManager, {
              toolName: inputData.toolName,
              toolCallId: inputData.toolCallId,
              args: args as Record<string, unknown>,
              agentId,
              threadId: _internal?.threadId,
              resourceId: _internal?.resourceId,
              timeoutMs: bgResolved.timeoutMs,
              maxRetries: bgResolved.maxRetries,
              runId,
              context: {
                // Executor — uses the tool from the current closure
                executor: {
                  execute: (
                    bgArgs: Record<string, unknown>,
                    opts?: {
                      abortSignal?: AbortSignal;
                      onProgress?: (chunk: BackgroundTaskProgressChunk) => Promise<void>;
                      suspend?: (data?: unknown, options?: SuspendOptions) => Promise<void>;
                      resumeData?: unknown;
                    },
                  ) => {
                    // Override the agent loop's `suspend`/`resumeData` (which
                    // would suspend the AGENT run via tool-call-approval) with
                    // the bg-task workflow's, so calling `suspend()` from the
                    // tool pauses the bg-task run instead.
                    return resolvedTool.execute!(bgArgs, {
                      ...toolOptions,
                      ...(opts?.resumeData !== undefined ? { resumeData: opts.resumeData } : {}),
                      suspend: async (data?: unknown, options?: SuspendOptions) => {
                        await toolOptions.suspend?.(data, options);
                        return opts?.suspend?.(data, options);
                      },
                      outputWriter: async (chunk: any) => {
                        await opts?.onProgress?.(chunk);
                        return toolOptions.outputWriter?.(chunk);
                      },
                      abortSignal: opts?.abortSignal,
                    } as any);
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
                        (bgRunId !== runId || (bgRunId === runId && workflowResumeData)) &&
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
                                providerMetadata: inputData.providerMetadata as ProviderMetadata | undefined,
                                providerExecuted: inputData.providerExecuted,
                              },
                            },
                            'output-available',
                            { output: chunk.payload.result },
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
                                providerMetadata: inputData.providerMetadata as ProviderMetadata | undefined,
                                providerExecuted: inputData.providerExecuted,
                              },
                            },
                            'error',
                            { error: chunk.payload.error },
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
                  const result =
                    params.status === 'failed'
                      ? `Background task failed: ${params.error?.message ?? 'Unknown error'}`
                      : params.result;
                  let transformCarrier = withToolPayloadTransformMetadata(
                    { metadata: {} as Record<string, any> },
                    await transformToolPayloadForTargets(
                      {
                        phase: 'input-available',
                        toolName: params.toolName,
                        toolCallId: params.toolCallId,
                        input: args,
                        providerMetadata: inputData.providerMetadata as Record<string, unknown> | undefined,
                      },
                      transformSource,
                      logger,
                    ),
                  );
                  transformCarrier = withToolPayloadTransformMetadata(
                    transformCarrier,
                    await transformToolPayloadForTargets(
                      {
                        phase: params.status === 'failed' ? 'error' : 'output-available',
                        toolName: params.toolName,
                        toolCallId: params.toolCallId,
                        input: args,
                        output: params.status === 'failed' ? undefined : params.result,
                        error: params.status === 'failed' ? params.error : undefined,
                        providerMetadata: inputData.providerMetadata as Record<string, unknown> | undefined,
                      },
                      transformSource,
                      logger,
                    ),
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
                  const transcriptArgs = hasTransformedToolPayload(transcriptArgsTransform)
                    ? transcriptArgsTransform.transformed
                    : args;
                  const transcriptResult = hasTransformedToolPayload(transcriptResultTransform)
                    ? transcriptResultTransform.transformed
                    : result;
                  const providerMetadata = withToolPayloadTransformProviderMetadata(
                    inputData.providerMetadata as ProviderMetadata | undefined,
                    transformCarrier.metadata,
                  ) as ProviderMetadata | undefined;

                  const updated = messageList.updateToolInvocation(
                    {
                      type: 'tool-invocation',
                      toolInvocation: {
                        state: 'result',
                        toolCallId: params.toolCallId,
                        toolName: params.toolName,
                        args,
                        result,
                        // Preserve the approval decision for an approved approval-gated tool that
                        // ran in the background so it round-trips on recall, matching the sync path
                        // and the "started" placeholder above.
                        ...(approvalGrant ?? {}),
                      },
                      ...(providerMetadata ? { providerMetadata } : {}),
                    },
                    {
                      mode: 'stream',
                      backgroundTasks: {
                        [params.toolCallId]: {
                          startedAt: params.startedAt,
                          completedAt: params.completedAt,
                          taskId: params.taskId,
                        },
                      },
                    },
                  );

                  // Fallback: no matching tool-invocation was found in the
                  // current message list (can happen if the initial run's
                  // message list was cleared, e.g. because the task completed
                  // after the process restarted and hooks were reattached
                  // without the original call). Append a standalone tool
                  // message so memory still records the result, even if it
                  // means a duplicate entry for that toolCallId.
                  if (!updated) {
                    if (params.runId !== runId || (params.runId === runId && workflowResumeData)) {
                      messageList.add(
                        [
                          {
                            role: 'tool' as const,
                            type: 'tool-call',
                            id: _internal?.generateId?.() ?? randomUUID(),
                            createdAt: new Date(),
                            content: [
                              {
                                type: 'tool-call' as const,
                                toolCallId: params.toolCallId,
                                toolName: params.toolName,
                                args: transcriptArgs,
                              },
                            ],
                          },
                        ],
                        'response',
                      );
                    }
                    messageList.add(
                      [
                        {
                          role: 'tool' as const,
                          content: [
                            {
                              type: 'tool-result' as const,
                              toolCallId: params.toolCallId,
                              toolName: params.toolName,
                              result: transcriptResult,
                              isError: params.status === 'failed',
                            },
                          ],
                        },
                      ],
                      'response',
                    );
                  }

                  // Flush to memory if available
                  if (_internal?.saveQueueManager && _internal?.threadId) {
                    await _internal.saveQueueManager.flushMessages(
                      messageList,
                      _internal.threadId,
                      _internal.memoryConfig,
                    );
                  }
                },
                // Execution injector — updates the existing tool-invocation in the
                // message list (keyed by toolCallId) background task startedAt.
                onExecution: async params => {
                  const inputTransform = await transformToolPayloadForTargets(
                    {
                      phase: 'input-available',
                      toolName: params.toolName,
                      toolCallId: params.toolCallId,
                      input: args,
                      providerMetadata: inputData.providerMetadata as Record<string, unknown> | undefined,
                    },
                    transformSource,
                    logger,
                  );
                  const transformCarrier = withToolPayloadTransformMetadata(
                    { metadata: {} as Record<string, any> },
                    inputTransform,
                  );
                  const providerMetadata = withToolPayloadTransformProviderMetadata(
                    inputData.providerMetadata as ProviderMetadata | undefined,
                    transformCarrier.metadata,
                  ) as ProviderMetadata | undefined;

                  messageList.updateToolInvocation(
                    {
                      type: 'tool-invocation',
                      toolInvocation: {
                        state: 'call',
                        toolCallId: params.toolCallId,
                        toolName: params.toolName,
                        args,
                      },
                      ...(providerMetadata ? { providerMetadata } : {}),
                    },
                    {
                      mode: 'stream',
                      backgroundTasks: {
                        [params.toolCallId]: {
                          startedAt: params.startedAt,
                          suspendedAt: params.suspendedAt,
                          taskId: params.taskId,
                        },
                      },
                    },
                  );
                },

                // Per-task callbacks
                onComplete: toolBgConfig?.onComplete ?? agentBgConfig?.onTaskComplete,
                onFailed: toolBgConfig?.onFailed ?? agentBgConfig?.onTaskFailed,
              },
            });

            const isSuspended = await bgTask.checkIfSuspended({
              toolCallId: inputData.toolCallId,
              runId,
              agentId,
              threadId: _internal?.threadId,
              resourceId: _internal?.resourceId,
              toolName: inputData.toolName,
            });
            if (isSuspended && resumeDataToPassToToolOptions !== undefined) {
              const task = await bgTask.resume(resumeDataToPassToToolOptions);

              return {
                result: `Background task resumed. Task ID: ${task.id}. The tool "${inputData.toolName}" is running in the background. You will be notified when it completes.`,
                ...inputData,
                ...resumeTarget,
                ...(approvalGrant ?? {}),
              };
            }

            const { task, fallbackToSync } = await bgTask.dispatch();

            if (!fallbackToSync) {
              // Emit background-task-started chunk. Use safeEnqueue: the
              // agent stream may have closed by the time this fires (e.g.
              // when the controller closes mid-dispatch in a long-lived
              // streamUntilIdle wrapper) — without the guard, the throw
              // bubbles up through the AI-SDK-v5 tool builder and gets
              // wrapped as `TOOL_EXECUTION_FAILED: Invalid state:
              // Controller is already closed`.
              safeEnqueue(controller, {
                type: 'background-task-started' as any,
                runId,
                from: ChunkFrom.AGENT,
                payload: {
                  taskId: task.id,
                  toolName: inputData.toolName,
                  toolCallId: inputData.toolCallId,
                },
              });

              // Return placeholder result so the LLM can continue
              return {
                result: `Background task started. Task ID: ${task.id}. The tool "${inputData.toolName}" is running in the background. You will be notified when it completes.`,
                ...inputData,
                ...resumeTarget,
                ...(approvalGrant ?? {}),
              };
            }
            // fallbackToSync: concurrency limit hit, fall through to synchronous execution
          }
        }

        const rawResult = await tool.execute(args, toolOptions);
        const result = ensureSerializable(rawResult);

        // Call onOutput hook after successful execution
        if (tool && 'onOutput' in tool && typeof (tool as any).onOutput === 'function') {
          try {
            await (tool as any).onOutput({
              toolCallId: inputData.toolCallId,
              toolName: inputData.toolName,
              output: result,
              abortSignal: options?.abortSignal,
            });
          } catch (error) {
            logger?.error('Error calling onOutput', error);
          }
        }

        return { result, ...inputData, ...resumeTarget, ...(approvalGrant ?? {}) };
      } catch (error) {
        // Re-throw FGA authorization errors instead of swallowing them
        if (error instanceof Error && error.name === 'FGADeniedError') {
          throw error;
        }
        return {
          error: error as Error,
          ...inputData,
          ...(resumeTargetToolCallId ? { resumeTargetToolCallId } : {}),
          ...(approvalGrant ?? {}),
        };
      }
    },
  });
}
