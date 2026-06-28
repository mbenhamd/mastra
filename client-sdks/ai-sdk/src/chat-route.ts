import {
  createUIMessageStream as createUIMessageStreamV5,
  createUIMessageStreamResponse as createUIMessageStreamResponseV5,
} from '@internal/ai-sdk-v5';
import type { UIMessageStreamOptions as UIMessageStreamOptionsV5 } from '@internal/ai-sdk-v5';
import {
  createUIMessageStream as createUIMessageStreamV6,
  createUIMessageStreamResponse as createUIMessageStreamResponseV6,
  isToolUIPart,
} from '@internal/ai-v6';
import type { UIMessageStreamOptions as UIMessageStreamOptionsV6 } from '@internal/ai-v6';
import type { AgentExecutionOptions, AgentExecutionOptionsBase } from '@mastra/core/agent';
import type { Mastra } from '@mastra/core/mastra';
import type { RequestContext } from '@mastra/core/request-context';
import { registerApiRoute } from '@mastra/core/server';
import type { MastraModelOutput } from '@mastra/core/stream';
import { toAISdkStream } from './convert-streams';
import { APPROVAL_ID_SEPARATOR } from './helpers';
import type {
  SupportedUIMessage,
  V5UIMessage,
  V5UIMessageStream,
  V6UIMessage,
  V6UIMessageStream,
} from './public-types';

/**
 * Scans a v6 UIMessage array for the most recent 'approval-responded' tool
 * part in the last trailing assistant message only. When found, splits the
 * composite approvalId ("${runId}::${toolCallId}") to recover the runId
 * needed for resumeStream.
 *
 * Only the last trailing assistant message is inspected so that approval
 * responses from earlier turns are never re-processed. Within that message,
 * parts are scanned in reverse so the decision the user just acted on wins
 * over any earlier 'approval-responded' parts that have not yet transitioned
 * to 'output-available'.
 *
 * Returns null when no approval response is present (normal chat turn).
 */
export function extractV6NativeApproval(
  messages: V6UIMessage[],
): { resumeData: Record<string, unknown>; runId: string } | null {
  // Only inspect the actual trailing message. If a user has already sent a
  // follow-up turn after an approval response, we must treat that as a normal
  // chat submission rather than replaying the stale approval response.
  const lastAssistantMsg = messages.at(-1);
  if (!lastAssistantMsg || lastAssistantMsg.role !== 'assistant') return null;

  const parts = lastAssistantMsg.parts ?? [];
  for (let i = parts.length - 1; i >= 0; i--) {
    const part = parts[i]!;
    if (!isToolUIPart(part) || part.state !== 'approval-responded') continue;

    const lastSep = part.approval.id.lastIndexOf(APPROVAL_ID_SEPARATOR);
    if (lastSep === -1) continue;
    const runId = part.approval.id.slice(0, lastSep);
    if (!runId) continue;

    return {
      resumeData: {
        approved: part.approval.approved,
        ...(part.approval.reason != null ? { reason: part.approval.reason } : {}),
      },
      runId,
    };
  }

  return null;
}

export type ChatStreamHandlerParams<
  UI_MESSAGE extends SupportedUIMessage = SupportedUIMessage,
  OUTPUT = undefined,
> = AgentExecutionOptions<OUTPUT> & {
  messages: UI_MESSAGE[];
  resumeData?: Record<string, any>;
  /** The trigger for the request - sent by AI SDK's useChat hook */
  trigger?: 'submit-message' | 'regenerate-message';
};

/**
 * Extracted from the second parameter of `Mastra.getAgentById` so the type
 * stays in sync with core automatically.
 */
export type AgentVersionOptions = NonNullable<Parameters<Mastra['getAgentById']>[1]>;

export type ChatStreamHandlerOptions<UI_MESSAGE extends SupportedUIMessage = SupportedUIMessage, OUTPUT = undefined> = {
  mastra: Mastra;
  agentId: string;
  agentVersion?: AgentVersionOptions;
  params: ChatStreamHandlerParams<UI_MESSAGE, OUTPUT>;
  defaultOptions?: AgentExecutionOptions<OUTPUT>;
  version?: 'v5' | 'v6';
  sendStart?: boolean;
  sendFinish?: boolean;
  sendReasoning?: boolean;
  sendSources?: boolean;
  onError?: (error: unknown) => string;
  messageMetadata?: UI_MESSAGE extends V6UIMessage
    ? UIMessageStreamOptionsV6<UI_MESSAGE>['messageMetadata']
    : UI_MESSAGE extends V5UIMessage
      ? UIMessageStreamOptionsV5<UI_MESSAGE>['messageMetadata']
      : never;
};

type ChatStreamHandlerOptionsV5<UI_MESSAGE extends V5UIMessage = V5UIMessage, OUTPUT = undefined> = Omit<
  ChatStreamHandlerOptions<UI_MESSAGE, OUTPUT>,
  'version' | 'messageMetadata'
> & {
  version?: 'v5';
  messageMetadata?: UIMessageStreamOptionsV5<UI_MESSAGE>['messageMetadata'];
};

type ChatStreamHandlerOptionsV6<UI_MESSAGE extends V6UIMessage = V6UIMessage, OUTPUT = undefined> = Omit<
  ChatStreamHandlerOptions<UI_MESSAGE, OUTPUT>,
  'version' | 'messageMetadata'
> & {
  version: 'v6';
  messageMetadata?: UIMessageStreamOptionsV6<UI_MESSAGE>['messageMetadata'];
};

type JsonPrimitive = string | number | boolean | null;
type JsonValue = JsonPrimitive | { [key: string]: JsonValue } | JsonValue[];

export type HarnessChatAttachmentRef = {
  attachmentId: string;
  resourceId: string;
  ownerSessionId?: string;
  bytes?: number;
  sha256?: string;
  source?: 'inline' | 'preupload' | 'url' | 'provider';
  kind?: 'file' | 'primitive' | 'element';
  name?: string;
  mimeType?: string;
  primitiveType?: 'text' | 'markdown' | 'json' | 'table' | 'chart-data' | 'selection' | 'citation';
  elementType?: string;
  renderer?: unknown;
  schemaId?: string;
  metadata?: Record<string, JsonValue>;
  object?: unknown;
};

export type HarnessChatMessageOptions = {
  content: string;
  stream: true;
  output?: undefined;
  sync?: undefined;
  admissionId?: string;
  abortSignal?: AbortSignal;
  mode?: string;
  model?: string;
  attachments?: HarnessChatAttachmentRef[];
  additionalTools?: Record<string, unknown>;
  requestContext?: { app: Record<string, JsonValue> };
};

export class HarnessChatStreamValidationError extends Error {
  constructor(
    public readonly path: string,
    message: string,
  ) {
    super(`${path}: ${message}`);
    this.name = 'HarnessChatStreamValidationError';
  }
}

export type HarnessChatStreamTrigger = 'submit-message' | 'regenerate-message';

export type HarnessChatStreamHandlerParams<UI_MESSAGE extends SupportedUIMessage = SupportedUIMessage> = {
  messages: UI_MESSAGE[];
  requestContext?: unknown;
  trigger?: HarnessChatStreamTrigger;
  admissionId?: HarnessChatMessageOptions['admissionId'];
  abortSignal?: HarnessChatMessageOptions['abortSignal'];
  mode?: HarnessChatMessageOptions['mode'];
  model?: HarnessChatMessageOptions['model'];
  attachments?: HarnessChatMessageOptions['attachments'];
  additionalTools?: HarnessChatMessageOptions['additionalTools'];
};

export type HarnessChatSessionLike<OUTPUT = undefined> = {
  // Keep overloaded Harness Session.message implementations assignable without
  // importing the fork-only Harness option types into this SDK's public API.
  message(options: any): Promise<MastraModelOutput<OUTPUT>>;
};

export type HarnessChatStreamHandlerOptions<
  UI_MESSAGE extends SupportedUIMessage = SupportedUIMessage,
  OUTPUT = undefined,
> = {
  session: HarnessChatSessionLike<OUTPUT>;
  params: HarnessChatStreamHandlerParams<UI_MESSAGE>;
  version?: 'v5' | 'v6';
  sendStart?: boolean;
  sendFinish?: boolean;
  sendReasoning?: boolean;
  sendSources?: boolean;
  onError?: (error: unknown) => string;
  messageMetadata?: UI_MESSAGE extends V6UIMessage
    ? UIMessageStreamOptionsV6<UI_MESSAGE>['messageMetadata']
    : UI_MESSAGE extends V5UIMessage
      ? UIMessageStreamOptionsV5<UI_MESSAGE>['messageMetadata']
      : never;
};

type HarnessChatStreamHandlerOptionsV5<UI_MESSAGE extends V5UIMessage = V5UIMessage, OUTPUT = undefined> = Omit<
  HarnessChatStreamHandlerOptions<UI_MESSAGE, OUTPUT>,
  'version' | 'messageMetadata'
> & {
  version?: 'v5';
  messageMetadata?: UIMessageStreamOptionsV5<UI_MESSAGE>['messageMetadata'];
};

type HarnessChatStreamHandlerOptionsV6<UI_MESSAGE extends V6UIMessage = V6UIMessage, OUTPUT = undefined> = Omit<
  HarnessChatStreamHandlerOptions<UI_MESSAGE, OUTPUT>,
  'version' | 'messageMetadata'
> & {
  version: 'v6';
  messageMetadata?: UIMessageStreamOptionsV6<UI_MESSAGE>['messageMetadata'];
};

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function assertJsonValue(value: unknown, path: string): JsonValue {
  if (value === null) return value;
  switch (typeof value) {
    case 'string':
    case 'boolean':
      return value;
    case 'number':
      if (Number.isFinite(value) && !Object.is(value, -0)) return value;
      break;
    case 'object':
      if (Array.isArray(value)) {
        const out: JsonValue[] = [];
        for (let index = 0; index < value.length; index += 1) {
          if (!(index in value)) {
            throw new HarnessChatStreamValidationError(`${path}[${index}]`, 'sparse arrays are not allowed');
          }
          out.push(assertJsonValue(value[index], `${path}[${index}]`));
        }
        return out;
      }
      if (isPlainObject(value)) {
        const out: Record<string, JsonValue> = {};
        for (const [key, child] of Object.entries(value)) {
          if (child !== undefined) out[key] = assertJsonValue(child, `${path}.${key}`);
        }
        return out;
      }
      break;
  }
  throw new HarnessChatStreamValidationError(path, 'must be JSON-serializable');
}

function validateHarnessChatRequestContext(input: unknown): { app: Record<string, JsonValue> } | undefined {
  if (input === undefined) return undefined;
  const path = 'handleHarnessChatStream.requestContext';
  if (!isPlainObject(input)) {
    throw new HarnessChatStreamValidationError(path, 'must be a plain object whose only allowed property is "app"');
  }
  for (const key of Reflect.ownKeys(input)) {
    if (key !== 'app') {
      throw new HarnessChatStreamValidationError(
        `${path}.${typeof key === 'symbol' ? key.toString() : key}`,
        'callers may only supply requestContext.app; all other keys are infrastructure-owned',
      );
    }
  }
  const app = input.app;
  if (app === undefined) return undefined;
  if (!isPlainObject(app)) {
    throw new HarnessChatStreamValidationError(`${path}.app`, 'must be a JSON object');
  }
  return { app: assertJsonValue(app, `${path}.app`) as Record<string, JsonValue> };
}

function textFromUIMessage(message: SupportedUIMessage): string {
  const content = (message as { content?: unknown }).content;
  if (typeof content === 'string') return content;

  const parts = (message as { parts?: unknown }).parts;
  if (Array.isArray(parts)) {
    let text = '';
    for (let index = 0; index < parts.length; index += 1) {
      const part = parts[index];
      if (isPlainObject(part) && part.type === 'text' && typeof part.text === 'string') {
        text += part.text;
        continue;
      }
      if (isPlainObject(part) && part.type === 'file') {
        throw new HarnessChatStreamValidationError(
          `handleHarnessChatStream.params.messages.parts[${index}]`,
          'file parts must be uploaded as Harness attachments before session admission',
        );
      }
      if (isPlainObject(part) && typeof part.type === 'string') {
        throw new HarnessChatStreamValidationError(
          `handleHarnessChatStream.params.messages.parts[${index}]`,
          `unsupported UI message part "${part.type}"`,
        );
      }
    }
    if (text.length > 0) return text;
  }

  throw new Error('A Harness chat stream requires a user message with text content');
}

function resolveHarnessChatPrompt(messages: SupportedUIMessage[], trigger: HarnessChatStreamTrigger | undefined) {
  if (!Array.isArray(messages)) {
    throw new Error('Messages must be an array of UIMessage objects');
  }
  if (messages.length === 0) {
    throw new Error('Messages must include at least one UIMessage');
  }

  const lastMessage = messages[messages.length - 1]!;
  const lastMessageId = lastMessage.role === 'assistant' ? lastMessage.id : undefined;

  if (trigger === 'regenerate-message') {
    const search = lastMessage.role === 'assistant' ? messages.slice(0, -1) : messages;
    const userMessage = [...search].reverse().find(message => message.role === 'user');
    if (userMessage === undefined) {
      throw new Error('Regenerate requests require a prior user message for Harness session admission');
    }
    return { content: textFromUIMessage(userMessage), lastMessageId, userMessage };
  }

  if (lastMessage.role !== 'user') {
    throw new Error(
      'Harness chat streams only support submit-message requests here; resume and human-in-the-loop (HITL) responses must use native Harness routes',
    );
  }

  return { content: textFromUIMessage(lastMessage), lastMessageId, userMessage: lastMessage };
}

/**
 * Framework-agnostic handler for streaming agent chat in AI SDK-compatible format.
 * Use this function directly when you need to handle chat streaming outside of Hono or Mastra's own apiRoutes feature.
 *
 * @example
 * ```ts
 * // Next.js App Router
 * import { handleChatStream } from '@mastra/ai-sdk';
 * import { createUIMessageStreamResponse } from 'ai';
 * import { mastra } from '@/src/mastra';
 *
 * export async function POST(req: Request) {
 *   const params = await req.json();
 *   const stream = await handleChatStream({
 *     mastra,
 *     agentId: 'weatherAgent',
 *     params,
 *   });
 *   return createUIMessageStreamResponse({ stream });
 * }
 * ```
 */
export function handleChatStream<UI_MESSAGE extends V5UIMessage = V5UIMessage, OUTPUT = undefined>(
  options: ChatStreamHandlerOptionsV5<UI_MESSAGE, OUTPUT>,
): Promise<V5UIMessageStream<UI_MESSAGE>>;
export function handleChatStream<UI_MESSAGE extends V6UIMessage = V6UIMessage, OUTPUT = undefined>(
  options: ChatStreamHandlerOptionsV6<UI_MESSAGE, OUTPUT>,
): Promise<V6UIMessageStream<UI_MESSAGE>>;
export async function handleChatStream<OUTPUT = undefined>({
  mastra,
  agentId,
  agentVersion,
  params,
  defaultOptions,
  version = 'v5',
  sendStart = true,
  sendFinish = true,
  sendReasoning = false,
  sendSources = false,
  onError,
  messageMetadata,
}: ChatStreamHandlerOptions<any, OUTPUT>): Promise<ReadableStream<any>> {
  const { messages, resumeData, runId, requestContext, trigger, ...rest } = params;

  if (resumeData && !runId) {
    throw new Error('runId is required when resumeData is provided');
  }

  const agentObj = agentVersion ? await mastra.getAgentById(agentId, agentVersion) : mastra.getAgentById(agentId);
  if (!agentObj) {
    throw new Error(`Agent ${agentId} not found`);
  }

  if (!Array.isArray(messages)) {
    throw new Error('Messages must be an array of UIMessage objects');
  }

  // For v6: if the user called approve() on the client, AI SDK v6 re-submits the
  // conversation with the tool part transitioned to 'approval-responded'. Detect
  // this and route to resumeStream instead of stream.
  const nativeApproval = version === 'v6' && !resumeData ? extractV6NativeApproval(messages as V6UIMessage[]) : null;

  const effectiveResumeData = nativeApproval?.resumeData ?? resumeData;
  const effectiveRunId = nativeApproval?.runId ?? runId;

  // Capture the last assistant message ID for the stream response.
  // This helps the frontend identify which message the response corresponds to.
  let lastMessageId: string | undefined;
  let messagesToSend = messages;

  if (messages.length > 0) {
    const lastMessage = messages[messages.length - 1]!;
    if (lastMessage?.role === 'assistant') {
      lastMessageId = lastMessage.id;

      // For regeneration, remove the last assistant message so the LLM generates fresh text
      if (trigger === 'regenerate-message') {
        messagesToSend = messages.slice(0, -1);
      }
    }
  }

  const { structuredOutput: restStructuredOutput, ...restOptions } = rest;
  const { structuredOutput: defaultStructuredOutput, ...defaultOptionsRest } = defaultOptions ?? {};
  const structuredOutput = restStructuredOutput ?? defaultStructuredOutput;

  const mergedProviderOptions = {
    ...defaultOptions?.providerOptions,
    ...restOptions.providerOptions,
  };

  const baseOptions = {
    ...defaultOptionsRest,
    ...restOptions,
    ...(effectiveRunId && { runId: effectiveRunId }),
    requestContext: requestContext || defaultOptions?.requestContext,
    ...(Object.keys(mergedProviderOptions).length > 0 && { providerOptions: mergedProviderOptions }),
  };

  const result = effectiveResumeData
    ? structuredOutput
      ? await agentObj.resumeStream(effectiveResumeData, { ...baseOptions, structuredOutput })
      : await agentObj.resumeStream(effectiveResumeData, baseOptions as AgentExecutionOptionsBase<unknown>)
    : structuredOutput
      ? await agentObj.stream(messagesToSend, { ...baseOptions, structuredOutput })
      : await agentObj.stream(messagesToSend, baseOptions as AgentExecutionOptionsBase<unknown>);

  if (version === 'v6') {
    return createUIMessageStreamV6<any>({
      originalMessages: messages,
      execute: async ({ writer }) => {
        for await (const part of toAISdkStream(result, {
          from: 'agent',
          version: 'v6',
          lastMessageId,
          sendStart,
          sendFinish,
          sendReasoning,
          sendSources,
          onError,
          messageMetadata: messageMetadata as UIMessageStreamOptionsV6<V6UIMessage>['messageMetadata'],
        })) {
          writer.write(part);
        }
      },
    }) as ReadableStream<any>;
  }

  return createUIMessageStreamV5<any>({
    originalMessages: messages,
    execute: async ({ writer }) => {
      for await (const part of toAISdkStream(result, {
        from: 'agent',
        lastMessageId,
        sendStart,
        sendFinish,
        sendReasoning,
        sendSources,
        onError,
        messageMetadata: messageMetadata as UIMessageStreamOptionsV5<V5UIMessage>['messageMetadata'],
      })) {
        writer.write(part);
      }
    },
  }) as ReadableStream<any>;
}

/**
 * Framework-agnostic handler for admitting AI SDK UI chat through a Harness v1
 * session while returning AI SDK-compatible UI message stream chunks.
 *
 * `trigger: "regenerate-message"` is supported as a fresh Harness admission
 * with no reused `admissionId`. Human-in-the-loop (HITL) resume/approval
 * requests are intentionally not mapped here; use the native Harness
 * inbox/session routes so durable resume semantics stay owned by Harness.
 */
export function handleHarnessChatStream<UI_MESSAGE extends V5UIMessage = V5UIMessage, OUTPUT = undefined>(
  options: HarnessChatStreamHandlerOptionsV5<UI_MESSAGE, OUTPUT>,
): Promise<V5UIMessageStream<UI_MESSAGE>>;
export function handleHarnessChatStream<UI_MESSAGE extends V6UIMessage = V6UIMessage, OUTPUT = undefined>(
  options: HarnessChatStreamHandlerOptionsV6<UI_MESSAGE, OUTPUT>,
): Promise<V6UIMessageStream<UI_MESSAGE>>;
export async function handleHarnessChatStream<OUTPUT = undefined>({
  session,
  params,
  version = 'v5',
  sendStart = true,
  sendFinish = true,
  sendReasoning = false,
  sendSources = false,
  onError,
  messageMetadata,
}: HarnessChatStreamHandlerOptions<any, OUTPUT>): Promise<ReadableStream<any>> {
  const { messages, requestContext, trigger, admissionId, abortSignal, mode, model, attachments, additionalTools } =
    params;
  const prompt = resolveHarnessChatPrompt(messages, trigger);
  const normalizedRequestContext = validateHarnessChatRequestContext(requestContext);
  if (additionalTools !== undefined && trigger !== 'regenerate-message') {
    throw new HarnessChatStreamValidationError(
      'handleHarnessChatStream.params.additionalTools',
      'additionalTools require a fresh Harness turn and cannot be combined with normal submit admissionId',
    );
  }
  const effectiveAdmissionId = trigger === 'regenerate-message' ? undefined : (admissionId ?? prompt.userMessage.id);

  const messageOptions: HarnessChatMessageOptions = {
    content: prompt.content,
    stream: true,
    ...(normalizedRequestContext !== undefined ? { requestContext: normalizedRequestContext } : {}),
    ...(effectiveAdmissionId !== undefined ? { admissionId: effectiveAdmissionId } : {}),
    ...(abortSignal !== undefined ? { abortSignal } : {}),
    ...(mode !== undefined ? { mode } : {}),
    ...(model !== undefined ? { model } : {}),
    ...(attachments !== undefined ? { attachments } : {}),
    ...(additionalTools !== undefined ? { additionalTools } : {}),
  };
  const result = await session.message(messageOptions);

  if (version === 'v6') {
    return createUIMessageStreamV6<any>({
      originalMessages: messages,
      execute: async ({ writer }) => {
        for await (const part of toAISdkStream(result, {
          from: 'agent',
          version: 'v6',
          lastMessageId: prompt.lastMessageId,
          sendStart,
          sendFinish,
          sendReasoning,
          sendSources,
          onError,
          messageMetadata: messageMetadata as UIMessageStreamOptionsV6<V6UIMessage>['messageMetadata'],
        })) {
          writer.write(part);
        }
      },
    }) as ReadableStream<any>;
  }

  return createUIMessageStreamV5<any>({
    originalMessages: messages,
    execute: async ({ writer }) => {
      for await (const part of toAISdkStream(result, {
        from: 'agent',
        lastMessageId: prompt.lastMessageId,
        sendStart,
        sendFinish,
        sendReasoning,
        sendSources,
        onError,
        messageMetadata: messageMetadata as UIMessageStreamOptionsV5<V5UIMessage>['messageMetadata'],
      })) {
        writer.write(part);
      }
    },
  }) as ReadableStream<any>;
}

export type chatRouteOptions<OUTPUT = undefined> = {
  defaultOptions?: AgentExecutionOptions<OUTPUT>;
  version?: 'v5' | 'v6';
  agentVersion?: AgentVersionOptions;
} & (
  | {
      path: `${string}:agentId${string}`;
      agent?: never;
    }
  | {
      path: string;
      agent: string;
    }
) & {
    sendStart?: boolean;
    sendFinish?: boolean;
    sendReasoning?: boolean;
    sendSources?: boolean;
    onError?: (error: unknown) => string;
  };

/**
 * Creates a chat route handler for streaming agent conversations using the AI SDK format.
 *
 * This function registers an HTTP POST endpoint that accepts messages, executes an agent, and streams the response back to the client in AI SDK-compatible format.
 *
 * @param {chatRouteOptions} options - Configuration options for the chat route
 * @param {string} [options.path='/chat/:agentId'] - The route path. Include `:agentId` for dynamic routing
 * @param {string} [options.agent] - Fixed agent ID when not using dynamic routing
 * @param {AgentExecutionOptions} [options.defaultOptions] - Default options passed to agent execution
 * @param {boolean} [options.sendStart=true] - Whether to send start events in the stream
 * @param {boolean} [options.sendFinish=true] - Whether to send finish events in the stream
 * @param {boolean} [options.sendReasoning=false] - Whether to include reasoning steps in the stream
 * @param {boolean} [options.sendSources=false] - Whether to include source citations in the stream
 * @param {(error: unknown) => string} [options.onError] - Custom error serializer streamed to the client. When omitted, errors are passed through a default serializer that strips sensitive fields (e.g. `APICallError.requestBodyValues`, which holds the system prompt) before they reach the client.
 *
 * @returns {ReturnType<typeof registerApiRoute>} A registered API route handler
 *
 * @throws {Error} When path doesn't include `:agentId` and no fixed agent is specified
 * @throws {Error} When agent ID is missing at runtime
 * @throws {Error} When specified agent is not found in Mastra instance
 *
 * @example
 * // Dynamic agent routing
 * chatRoute({
 *   path: '/chat/:agentId',
 * });
 *
 * @example
 * // Fixed agent with custom path
 * chatRoute({
 *   path: '/api/support-chat',
 *   agent: 'support-agent',
 *   defaultOptions: {
 *     maxSteps: 5,
 *   },
 * });
 *
 * @remarks
 * - The route handler expects a JSON body with a `messages` array
 * - Messages should follow the format: `{ role: 'user' | 'assistant' | 'system', content: string }`
 * - The response is a Server-Sent Events (SSE) stream compatible with AI SDK v5
 * - If both `agent` and `:agentId` are present, a warning is logged and the fixed `agent` takes precedence
 * - Request context from the incoming request overrides `defaultOptions.requestContext` if both are present
 */
export function chatRoute<OUTPUT = undefined>({
  path = '/chat/:agentId',
  agent,
  defaultOptions,
  version = 'v5',
  agentVersion,
  sendStart = true,
  sendFinish = true,
  sendReasoning = false,
  sendSources = false,
  onError,
}: chatRouteOptions<OUTPUT>): ReturnType<typeof registerApiRoute> {
  if (!agent && !path.includes('/:agentId')) {
    throw new Error('Path must include :agentId to route to the correct agent or pass the agent explicitly');
  }

  return registerApiRoute(path, {
    method: 'POST',
    openapi: {
      summary: 'Chat with an agent',
      description: 'Send messages to an agent and stream the response in the AI SDK format',
      tags: ['ai-sdk'],
      parameters: [
        {
          name: 'agentId',
          in: 'path',
          required: true,
          description: 'The ID of the agent to chat with',
          schema: {
            type: 'string',
          },
        },
        {
          name: 'versionId',
          in: 'query',
          required: false,
          description: 'Specific agent version ID to use. Mutually exclusive with status.',
          schema: {
            type: 'string',
          },
        },
        {
          name: 'status',
          in: 'query',
          required: false,
          description:
            'Which stored config version to resolve: draft (latest) or published (active version). Mutually exclusive with versionId.',
          schema: {
            type: 'string',
            enum: ['draft', 'published'],
          },
        },
      ],
      requestBody: {
        required: true,
        content: {
          'application/json': {
            schema: {
              type: 'object',
              properties: {
                resumeData: {
                  type: 'object',
                  description: 'Resume data for the agent',
                },
                runId: {
                  type: 'string',
                  description: 'The run ID required when resuming an agent execution',
                },
                messages: {
                  type: 'array',
                  description: 'Array of messages in the conversation',
                  items: {
                    type: 'object',
                    properties: {
                      role: {
                        type: 'string',
                        enum: ['user', 'assistant', 'system'],
                        description: 'The role of the message sender',
                      },
                      content: {
                        type: 'string',
                        description: 'The content of the message',
                      },
                    },
                    required: ['role', 'content'],
                  },
                },
              },
              required: ['messages'],
            },
          },
        },
      },
      responses: {
        '200': {
          description: 'Streaming response from the agent',
          content: {
            'text/plain': {
              schema: {
                type: 'string',
                description: 'Server-sent events stream containing the agent response',
              },
            },
          },
        },
        '400': {
          description: 'Bad request - invalid input',
          content: {
            'application/json': {
              schema: {
                type: 'object',
                properties: {
                  error: {
                    type: 'string',
                  },
                },
              },
            },
          },
        },
        '404': {
          description: 'Agent not found',
          content: {
            'application/json': {
              schema: {
                type: 'object',
                properties: {
                  error: {
                    type: 'string',
                  },
                },
              },
            },
          },
        },
      },
    },
    handler: async c => {
      const params = (await c.req.json()) as ChatStreamHandlerParams<SupportedUIMessage, OUTPUT>;
      const mastra = c.get('mastra');
      const contextRequestContext = (c as any).get('requestContext') as RequestContext | undefined;

      let agentToUse: string | undefined = agent;
      if (!agent) {
        const agentId = c.req.param('agentId');
        agentToUse = agentId;
      }

      if (c.req.param('agentId') && agent) {
        mastra
          .getLogger()
          ?.warn(
            `Fixed agent ID was set together with an agentId path parameter. This can lead to unexpected behavior.`,
          );
      }

      // Prioritize requestContext from middleware/route options over body
      const effectiveRequestContext = contextRequestContext || defaultOptions?.requestContext || params.requestContext;

      if (
        (contextRequestContext && defaultOptions?.requestContext) ||
        (contextRequestContext && params.requestContext) ||
        (defaultOptions?.requestContext && params.requestContext)
      ) {
        mastra
          .getLogger()
          ?.warn(`Multiple "requestContext" sources provided. Using priority: middleware > route options > body.`);
      }

      if (!agentToUse) {
        throw new Error('Agent ID is required');
      }

      // Resolve agent version from query params, falling back to static option
      const queryVersionId = c.req.query('versionId');
      const rawStatus = c.req.query('status');

      if (queryVersionId && rawStatus) {
        throw new Error('Query parameters "versionId" and "status" are mutually exclusive');
      }

      if (rawStatus && rawStatus !== 'draft' && rawStatus !== 'published') {
        throw new Error('Query parameter "status" must be "draft" or "published"');
      }

      const queryStatus = rawStatus as 'draft' | 'published' | undefined;
      const effectiveAgentVersion: AgentVersionOptions | undefined = queryVersionId
        ? { versionId: queryVersionId }
        : queryStatus
          ? { status: queryStatus }
          : agentVersion;

      const handlerOptions = {
        mastra,
        agentId: agentToUse,
        agentVersion: effectiveAgentVersion,
        params: {
          ...params,
          requestContext: effectiveRequestContext,
          abortSignal: c.req.raw.signal,
        } as any,
        defaultOptions,
        sendStart,
        sendFinish,
        sendReasoning,
        sendSources,
        onError,
      };

      if (version === 'v6') {
        const uiMessageStream = await handleChatStream({
          ...handlerOptions,
          version: 'v6',
        });

        return createUIMessageStreamResponseV6({ stream: uiMessageStream });
      }

      const uiMessageStream = await handleChatStream(handlerOptions);
      return createUIMessageStreamResponseV5({ stream: uiMessageStream });
    },
  });
}
