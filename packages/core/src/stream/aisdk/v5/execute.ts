import { isProxy } from 'node:util/types';
import { injectJsonInstructionIntoMessages } from '@ai-sdk/provider-utils-v5';
import type { LanguageModelV2Prompt } from '@ai-sdk/provider-v5';
import { APICallError } from '@internal/ai-sdk-v5';
import type { IdGenerator, ToolChoice, ToolSet } from '@internal/ai-sdk-v5';
import { prepareJsonSchemaForOpenAIStrictMode } from '@mastra/schema-compat';
import type { StructuredOutputOptions } from '../../../agent/types';
import type { ModelMethodType } from '../../../llm/model/model.loop.types';
import { modelSupportsStructuredOutput } from '../../../llm/model/provider-registry';
import type { MastraLanguageModel, SharedProviderOptions } from '../../../llm/model/shared.types';
import type { LoopOptions } from '../../../loop/types';
import type { PreparedModelRequestMetrics } from '../../../observability';
import { createExactJsonMeasurementSnapshot } from '../../../observability/content-free-measurement';
import type { ExactJsonMeasurementSnapshot } from '../../../observability/content-free-measurement';
import { DEFAULT_MAX_RETRY_AFTER_MS, getRetryAfterMs, waitDelay } from '../../../utils/retry-after';
import { getResponseFormat } from '../../base/schema';
import type { LanguageModelV2StreamResult, OnResult } from '../../types';
import { prepareToolsAndToolChoice } from './compat';
import type { ModelSpecVersion } from './compat';
import { AISDKV5InputStream } from './input';

type JsonPromptInjection = StructuredOutputOptions<unknown>['jsonPromptInjection'];
type ResolvedJsonPromptInjection = Exclude<JsonPromptInjection, 'auto'>;

/**
 * p-retry's own defaults, pinned explicitly so the delay it schedules between
 * model-call attempts can be computed when honoring a provider `Retry-After`.
 */
const RETRY_MIN_TIMEOUT_MS = 1_000;
const RETRY_BACKOFF_FACTOR = 2;

/**
 * Whether a failed model call will be retried. Used by both `onFailedAttempt` and
 * `shouldRetry` so a terminal error never waits out a provider delay it will not use.
 */
function isRetryableModelError(error: unknown): boolean {
  if (APICallError.isInstance(error)) {
    return error.isRetryable;
  }
  return true;
}

export function resolveJsonPromptInjection(
  value: JsonPromptInjection,
  capability: boolean | undefined,
): ResolvedJsonPromptInjection {
  if (value !== 'auto') return value;
  return capability === true ? undefined : 'inline';
}

function buildJsonInstruction(schema: unknown) {
  return `Return your response as JSON matching this schema:\n\n${JSON.stringify(schema)}\n\nReturn only valid JSON. Do not include markdown or explanatory text.`;
}

function injectJsonInstructionIntoLatestUserMessage({
  messages,
  schema,
}: {
  messages: LanguageModelV2Prompt;
  schema: unknown;
}): LanguageModelV2Prompt {
  const instruction = buildJsonInstruction(schema);
  const prompt = messages.map(message => ({
    ...message,
    content: Array.isArray(message.content) ? [...message.content] : message.content,
  })) as LanguageModelV2Prompt;

  for (let i = prompt.length - 1; i >= 0; i--) {
    const message = prompt[i];
    if (message?.role !== 'user') {
      continue;
    }

    message.content = Array.isArray(message.content)
      ? [...message.content, { type: 'text', text: instruction }]
      : [
          { type: 'text', text: String(message.content ?? '') },
          { type: 'text', text: instruction },
        ];
    return prompt;
  }

  return [...prompt, { role: 'user', content: [{ type: 'text', text: instruction }] }] as LanguageModelV2Prompt;
}

function omit<T extends object, K extends keyof T>(obj: T, keys: K[]): Omit<T, K> {
  const newObj = { ...obj };
  for (const key of keys) {
    delete newObj[key];
  }
  return newObj;
}

type ProviderMessageRole = 'system' | 'user' | 'assistant' | 'tool' | 'other';

const measurementEncoder = new TextEncoder();

type OwnProperty = { state: 'absent' } | { state: 'data'; value: unknown } | { state: 'unsupported' };

function isDescriptorSafeObject(value: unknown): value is object {
  if (typeof value !== 'object' || value === null) return false;
  try {
    return !isProxy(value);
  } catch {
    return false;
  }
}

function ownProperty(value: unknown, key: string): OwnProperty {
  if (!isDescriptorSafeObject(value)) return { state: 'unsupported' };
  try {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (descriptor === undefined) return { state: 'absent' };
    return 'value' in descriptor ? { state: 'data', value: descriptor.value } : { state: 'unsupported' };
  } catch {
    return { state: 'unsupported' };
  }
}

function providerMessageRole(message: unknown): ProviderMessageRole {
  if (!isDescriptorSafeObject(message)) return 'other';
  try {
    if (Array.isArray(message)) return 'other';
  } catch {
    return 'other';
  }
  const roleProperty = ownProperty(message, 'role');
  const role = roleProperty.state === 'data' ? roleProperty.value : undefined;
  return role === 'system' || role === 'user' || role === 'assistant' || role === 'tool' ? role : 'other';
}

function arrayLength(value: unknown): number | undefined {
  if (!isDescriptorSafeObject(value)) return undefined;
  try {
    if (!Array.isArray(value)) return undefined;
    const descriptor = Object.getOwnPropertyDescriptor(value, 'length');
    const length = descriptor && 'value' in descriptor ? descriptor.value : undefined;
    return Number.isSafeInteger(length) && length >= 0 ? length : undefined;
  } catch {
    return undefined;
  }
}

/** Serialize only values already detached by createExactJsonMeasurementSnapshot. */
function detachedJsonUtf8ByteLength(value: unknown): number | undefined {
  try {
    const serialized = JSON.stringify(value);
    return serialized === undefined ? undefined : measurementEncoder.encode(serialized).byteLength;
  } catch {
    return undefined;
  }
}

function providerMessageMeasurements(prompt: unknown, measureBytes: boolean) {
  const roles: Record<ProviderMessageRole, { count: number; bytes: number | undefined }> = {
    system: { count: 0, bytes: measureBytes ? 0 : undefined },
    user: { count: 0, bytes: measureBytes ? 0 : undefined },
    assistant: { count: 0, bytes: measureBytes ? 0 : undefined },
    tool: { count: 0, bytes: measureBytes ? 0 : undefined },
    other: { count: 0, bytes: measureBytes ? 0 : undefined },
  };
  const length = arrayLength(prompt) ?? 0;
  let totalBytes: number | undefined = measureBytes ? 2 + Math.max(0, length - 1) : undefined;
  for (let index = 0; index < length; index += 1) {
    const item = ownProperty(prompt, String(index));
    const message = item.state === 'data' ? item.value : undefined;
    const role = providerMessageRole(message);
    const roleMetrics = roles[role];
    roleMetrics.count += 1;
    if (!measureBytes) continue;
    const measured = detachedJsonUtf8ByteLength(message);
    if (measured === undefined) {
      roleMetrics.bytes = undefined;
      totalBytes = undefined;
      continue;
    }
    if (roleMetrics.bytes !== undefined) roleMetrics.bytes += measured;
    if (totalBytes !== undefined) totalBytes += measured;
  }
  return { count: length, roles, totalBytes };
}

const measuredProviderReasoningEfforts = new Set(['none', 'low', 'medium', 'high', 'xhigh', 'max']);

function plainRecord(value: unknown): Record<string, unknown> | undefined {
  if (!isDescriptorSafeObject(value)) return undefined;
  try {
    if (Array.isArray(value)) return undefined;
    const prototype = Reflect.getPrototypeOf(value);
    return prototype === Object.prototype || prototype === null ? (value as Record<string, unknown>) : undefined;
  } catch {
    return undefined;
  }
}

function preparedProviderReasoning(providerOptions: unknown) {
  const options = plainRecord(providerOptions);
  if (!options) return { providerReasoningEffortState: 'provider_default' as const };
  const candidates: unknown[] = [];
  let unsupported = false;
  for (const [providerKey, effortPath] of [
    ['azure', ['reasoningEffort']],
    ['openai', ['reasoningEffort']],
    ['openrouter', ['reasoning', 'effort']],
  ] as const) {
    const providerProperty = ownProperty(options, providerKey);
    if (providerProperty.state === 'absent') continue;
    if (providerProperty.state === 'unsupported') {
      unsupported = true;
      continue;
    }
    let current = plainRecord(providerProperty.value);
    if (current === undefined) {
      unsupported = true;
      continue;
    }
    for (const [index, key] of effortPath.entries()) {
      const property = ownProperty(current, key);
      if (property.state === 'absent') {
        current = undefined;
        break;
      }
      if (property.state === 'unsupported') {
        unsupported = true;
        current = undefined;
        break;
      }
      if (index === effortPath.length - 1) {
        if (property.value !== undefined) candidates.push(property.value);
      } else {
        current = plainRecord(property.value);
        if (current === undefined) {
          unsupported = true;
          break;
        }
      }
    }
  }
  if (unsupported) return { providerReasoningEffortState: 'unknown' as const };
  const effort = candidates[0];
  if (candidates.length !== 1 || typeof effort !== 'string' || !measuredProviderReasoningEfforts.has(effort)) {
    return candidates.length === 0
      ? { providerReasoningEffortState: 'provider_default' as const }
      : { providerReasoningEffortState: 'unknown' as const };
  }
  return {
    providerReasoningEffortState: 'measured' as const,
    providerReasoningEffort: effort as 'none' | 'low' | 'medium' | 'high' | 'xhigh' | 'max',
  };
}

type PreparedRequestMeasurement = Omit<
  PreparedModelRequestMetrics,
  'providerAttempt' | 'providerPreparationMs' | 'providerDispatchTimestampMs'
>;

function measuredSnapshotRecord(measurement: ExactJsonMeasurementSnapshot): Record<string, unknown> | undefined {
  return measurement.state === 'measured' ? plainRecord(measurement.snapshot) : undefined;
}

function preparedRequestMeasurement({
  providerRequest,
  prompt,
  responseSchema,
  responseSchemaInline,
}: {
  providerRequest: Record<string, unknown>;
  prompt: LanguageModelV2Prompt;
  responseSchema: unknown;
  responseSchemaInline: boolean;
}): PreparedRequestMeasurement {
  const measurementStartedAtMs = Date.now();
  // AbortSignal is a live cancellation handle, not provider request content.
  // Exclude it before detaching the exact JSON-safe envelope; every value used
  // after adapter dispatch then comes from this immutable snapshot or a
  // content-free primitive captured alongside it.
  const providerRequestMeasurement = createExactJsonMeasurementSnapshot(omit(providerRequest, ['abortSignal']));
  const providerRequestSnapshot = measuredSnapshotRecord(providerRequestMeasurement);
  const detachedPromptProperty = providerRequestSnapshot ? ownProperty(providerRequestSnapshot, 'prompt') : undefined;
  const detachedPrompt = detachedPromptProperty?.state === 'data' ? detachedPromptProperty.value : undefined;
  // Counts remain available when exact byte measurement is bounded out, but
  // only through descriptor reads guarded against Proxy traps. No content,
  // accessors, iterators, or serialization hooks are invoked.
  const fallbackMessageMeasurements = providerMessageMeasurements(prompt, false);
  const messageMeasurements = providerRequestSnapshot
    ? providerMessageMeasurements(detachedPrompt, true)
    : fallbackMessageMeasurements;
  const system = messageMeasurements.roles.system;
  const user = messageMeasurements.roles.user;
  const assistant = messageMeasurements.roles.assistant;
  const tool = messageMeasurements.roles.tool;
  const other = messageMeasurements.roles.other;
  const providerMessageBytes = messageMeasurements.totalBytes;
  const providerRequestBytes =
    providerRequestMeasurement.state === 'measured' ? providerRequestMeasurement.utf8ByteLength : undefined;
  const detachedToolsProperty = providerRequestSnapshot ? ownProperty(providerRequestSnapshot, 'tools') : undefined;
  const detachedTools = detachedToolsProperty?.state === 'data' ? detachedToolsProperty.value : undefined;
  const detachedProviderToolCount =
    detachedToolsProperty?.state === 'absent' ||
    (detachedToolsProperty?.state === 'data' && detachedToolsProperty.value === undefined)
      ? 0
      : detachedToolsProperty?.state === 'data'
        ? arrayLength(detachedToolsProperty.value)
        : undefined;
  const providerToolsProperty = ownProperty(providerRequest, 'tools');
  const fallbackProviderToolCount =
    providerToolsProperty.state === 'absent' ||
    (providerToolsProperty.state === 'data' && providerToolsProperty.value === undefined)
      ? 0
      : providerToolsProperty.state === 'data'
        ? arrayLength(providerToolsProperty.value)
        : undefined;
  const measuredProviderToolCount = providerRequestSnapshot ? detachedProviderToolCount : fallbackProviderToolCount;
  const providerToolCount = measuredProviderToolCount ?? 0;
  const providerToolSchemaBytes =
    providerRequestSnapshot && providerToolCount > 0 ? detachedJsonUtf8ByteLength(detachedTools) : undefined;
  const providerToolSchemaState =
    measuredProviderToolCount === undefined
      ? ('unknown' as const)
      : providerToolCount === 0
        ? ('not_applicable' as const)
        : providerToolSchemaBytes === undefined
          ? ('unknown' as const)
          : ('measured' as const);
  const responseSchemaMeasurement =
    responseSchema === undefined ? undefined : createExactJsonMeasurementSnapshot(responseSchema);
  const providerResponseSchemaBytes =
    responseSchemaMeasurement?.state === 'measured' ? responseSchemaMeasurement.utf8ByteLength : undefined;
  const providerResponseSchemaState =
    responseSchema === undefined
      ? ('not_applicable' as const)
      : providerResponseSchemaBytes === undefined
        ? ('unknown' as const)
        : responseSchemaInline
          ? ('inline_in_prompt' as const)
          : ('measured' as const);
  const measurementState =
    providerMessageBytes === undefined ||
    providerRequestBytes === undefined ||
    system.bytes === undefined ||
    user.bytes === undefined ||
    assistant.bytes === undefined ||
    tool.bytes === undefined ||
    other.bytes === undefined ||
    providerToolSchemaState === 'unknown' ||
    providerResponseSchemaState === 'unknown'
      ? ('unknown' as const)
      : ('measured' as const);
  const detachedProviderOptionsProperty = providerRequestSnapshot
    ? ownProperty(providerRequestSnapshot, 'providerOptions')
    : undefined;
  const providerReasoning = providerRequestSnapshot
    ? preparedProviderReasoning(
        detachedProviderOptionsProperty?.state === 'data' ? detachedProviderOptionsProperty.value : undefined,
      )
    : { providerReasoningEffortState: 'unknown' as const };
  const providerMeasurementMs = Math.max(0, Date.now() - measurementStartedAtMs);

  return {
    measurementState,
    providerBreakdownState: 'serialized_components_non_additive',
    providerMessageCount: messageMeasurements.count,
    ...(providerMessageBytes === undefined ? {} : { providerMessageBytes }),
    providerSystemMessageCount: system.count,
    ...(system.bytes === undefined ? {} : { providerSystemMessageBytes: system.bytes }),
    providerUserMessageCount: user.count,
    ...(user.bytes === undefined ? {} : { providerUserMessageBytes: user.bytes }),
    providerAssistantMessageCount: assistant.count,
    ...(assistant.bytes === undefined ? {} : { providerAssistantMessageBytes: assistant.bytes }),
    providerToolMessageCount: tool.count,
    ...(tool.bytes === undefined ? {} : { providerToolMessageBytes: tool.bytes }),
    providerOtherMessageCount: other.count,
    ...(other.bytes === undefined ? {} : { providerOtherMessageBytes: other.bytes }),
    ...(system.bytes === undefined ? {} : { providerInstructionBytes: system.bytes }),
    providerToolCount,
    ...(providerToolSchemaBytes === undefined ? {} : { providerToolSchemaBytes }),
    providerToolSchemaState,
    ...(providerResponseSchemaBytes === undefined ? {} : { providerResponseSchemaBytes }),
    providerResponseSchemaState,
    ...providerReasoning,
    ...(providerRequestBytes === undefined ? {} : { providerRequestBytes }),
    providerMeasurementMs,
  };
}

type ExecutionProps<OUTPUT = undefined> = {
  runId: string;
  model: MastraLanguageModel;
  providerOptions?: SharedProviderOptions;
  inputMessages: LanguageModelV2Prompt;
  tools?: ToolSet;
  toolChoice?: ToolChoice<ToolSet>;
  activeTools?: string[];
  options?: {
    abortSignal?: AbortSignal;
  };
  includeRawChunks?: boolean;
  modelSettings?: LoopOptions['modelSettings'];
  onResult: OnResult;
  /** Receives only content-free measurements after the provider request has been dispatched. */
  onPreparedRequest?: (metrics: PreparedModelRequestMetrics) => void;
  /** Starts one tracing boundary immediately before each provider adapter invocation. */
  onProviderAttemptStart?: (providerAttempt: number) => void;
  /** Closes only the failed provider-attempt boundary so a retry can open another. */
  onProviderAttemptError?: (input: { error: unknown; providerAttempt: number }) => void;
  structuredOutput?: StructuredOutputOptions<OUTPUT>;
  /**
  Additional HTTP headers to be sent with the request.
  Only applicable for HTTP-based providers.
  */
  headers?: Record<string, string | undefined>;
  shouldThrowError?: boolean;
  methodType: ModelMethodType;
  generateId?: IdGenerator;
};

export function execute<OUTPUT = undefined>({
  runId,
  model,
  providerOptions,
  inputMessages,
  tools,
  toolChoice,
  activeTools,
  options,
  onResult,
  onPreparedRequest,
  onProviderAttemptStart,
  onProviderAttemptError,
  includeRawChunks,
  modelSettings,
  structuredOutput,
  headers,
  shouldThrowError,
  methodType,
  generateId,
}: ExecutionProps<OUTPUT>) {
  const preparationStartedAtMs = Date.now();
  const v5 = new AISDKV5InputStream({
    component: 'LLM',
    name: model.modelId,
    generateId,
  });

  // Determine target version based on model's specificationVersion
  // V3 (AI SDK v6) and V4 (AI SDK v7) models need 'provider' type, V2 models need 'provider-defined'
  const targetVersion: ModelSpecVersion =
    model.specificationVersion === 'v4' ? 'v4' : model.specificationVersion === 'v3' ? 'v3' : 'v2';

  const toolsAndToolChoice = prepareToolsAndToolChoice({
    tools,
    toolChoice,
    activeTools,
    targetVersion,
  });

  const structuredOutputMode = structuredOutput?.schema
    ? structuredOutput?.model
      ? 'processor'
      : 'direct'
    : undefined;

  const responseFormat = structuredOutput?.schema
    ? getResponseFormat(structuredOutput?.schema, {
        model: {
          provider: model.provider,
          modelId: model.modelId,
          supportsStructuredOutputs: true,
        },
      })
    : undefined;

  let prompt = inputMessages;
  const jsonPromptInjection = structuredOutput?.jsonPromptInjection;
  const modelRoute = `${model.provider.split('.')[0]}/${model.modelId}`;
  const resolvedJsonPromptInjection = resolveJsonPromptInjection(
    jsonPromptInjection,
    jsonPromptInjection === 'auto' ? modelSupportsStructuredOutput(modelRoute) : undefined,
  );
  const injectionMode = resolvedJsonPromptInjection === true ? 'system' : resolvedJsonPromptInjection;

  // For direct mode (no model provided for structuring agent), inject JSON schema instruction if opting out of native response format with jsonPromptInjection
  if (structuredOutputMode === 'direct' && responseFormat?.type === 'json' && injectionMode) {
    prompt =
      injectionMode === 'inline'
        ? injectJsonInstructionIntoLatestUserMessage({
            messages: inputMessages,
            schema: responseFormat.schema,
          })
        : injectJsonInstructionIntoMessages({
            messages: inputMessages,
            schema: responseFormat.schema,
          });
  }

  // For processor mode without agent reuse, inject a custom prompt to inform the main agent
  // about the structured output schema that the structuring agent will use.
  if (
    structuredOutputMode === 'processor' &&
    responseFormat?.type === 'json' &&
    responseFormat?.schema &&
    !structuredOutput?.useAgent
  ) {
    prompt = injectJsonInstructionIntoMessages({
      messages: inputMessages,
      schema: responseFormat.schema,
      schemaPrefix: `Your response will be processed by another agent to extract structured data. Please ensure your response contains comprehensive information for all the following fields that will be extracted:\n`,
      schemaSuffix: `\n\nYou don't need to format your response as JSON unless the user asks you to. Just ensure your natural language response includes relevant information for each field in the schema above.`,
    });
  }

  /**
   * Enable OpenAI's strict JSON schema mode to ensure schema compliance.
   * Without this, OpenAI may omit required fields or violate type constraints.
   * @see https://platform.openai.com/docs/guides/structured-outputs#structured-outputs-vs-json-mode
   * @see https://ai-sdk.dev/docs/ai-sdk-core/generating-structured-data#accessing-reasoning
   */
  const isOpenAIStrictMode = model.provider.startsWith('openai') && responseFormat?.type === 'json' && !injectionMode;

  // For OpenAI strict mode, ensure all properties are required and additionalProperties: false
  if (isOpenAIStrictMode && responseFormat?.schema) {
    responseFormat.schema = prepareJsonSchemaForOpenAIStrictMode(responseFormat.schema);
  }

  const providerOptionsToUse: SharedProviderOptions | undefined = isOpenAIStrictMode
    ? {
        ...(providerOptions ?? {}),
        openai: {
          strictJsonSchema: true,
          ...(providerOptions?.openai ?? {}),
        },
      }
    : providerOptions;

  const providerResponseFormat = structuredOutputMode === 'direct' && !injectionMode ? responseFormat : undefined;
  const responseSchema = responseFormat?.type === 'json' ? responseFormat.schema : undefined;
  const responseSchemaInline =
    responseSchema !== undefined &&
    (injectionMode !== undefined || (structuredOutputMode === 'processor' && !structuredOutput?.useAgent));
  const responseSchemaForMeasurement =
    providerResponseFormat?.type === 'json'
      ? providerResponseFormat.schema
      : responseSchemaInline
        ? responseSchema
        : undefined;
  const preparationBeforeProviderCallbackMs = Math.max(0, Date.now() - preparationStartedAtMs);

  const stream = v5.initialize({
    runId,
    onResult,
    createStream: async () => {
      const preparationResumedAtMs = Date.now();
      try {
        const filteredModelSettings = omit(modelSettings || {}, ['maxRetries', 'headers']);
        const abortSignal = options?.abortSignal;

        const pRetry = await import('p-retry');
        return await pRetry.default(
          async attemptNumber => {
            // Starts after p-retry has completed any backoff, so retry delay is
            // never reported as provider request preparation.
            const attemptPreparationStartedAtMs = Date.now();
            const fn = (methodType === 'stream' ? model.doStream : model.doGenerate).bind(model);
            // Preserve the pre-observability retry contract: every provider
            // attempt receives a fresh top-level request object. Some adapters
            // annotate their input synchronously, and reusing one object here
            // would let a failed attempt contaminate the next retry.
            const providerRequest = {
              ...toolsAndToolChoice,
              prompt,
              providerOptions: providerOptionsToUse,
              abortSignal,
              includeRawChunks,
              responseFormat: providerResponseFormat,
              ...filteredModelSettings,
              headers,
            };
            let requestMeasurement: PreparedRequestMeasurement | undefined;
            if (onPreparedRequest) {
              try {
                // Detach all measurement inputs before the adapter takes
                // ownership of providerRequest. Adapters may mutate it
                // synchronously or retain it beyond this invocation.
                requestMeasurement = preparedRequestMeasurement({
                  providerRequest,
                  prompt,
                  responseSchema: responseSchemaForMeasurement,
                  responseSchemaInline,
                });
              } catch {
                // Measurement must never prevent or delay a provider failure.
              }
            }
            try {
              onProviderAttemptStart?.(attemptNumber);
            } catch {
              // Tracing must never prevent the provider call.
            }
            // Cast needed: V2 and V3 call options are structurally compatible but typed differently
            // (e.g., tool types differ: V2 uses 'provider-defined', V3 uses 'provider')
            const providerDispatchTimestampMs = Date.now();
            try {
              let streamResultPromise: unknown;
              try {
                // Invoke the adapter before notifying observers so they cannot add
                // to provider dispatch latency. Post-dispatch reporting consumes
                // only the detached, content-free measurement captured before the
                // adapter received the live request object.
                streamResultPromise = (fn as Function)(providerRequest);
              } finally {
                if (onPreparedRequest && requestMeasurement) {
                  try {
                    onPreparedRequest({
                      ...requestMeasurement,
                      providerAttempt: attemptNumber,
                      providerDispatchTimestampMs,
                      // Each retry receives a fresh request object, but only the
                      // first attempt owns shared prompt/tool preparation. Every
                      // attempt reports its own request construction and detached
                      // measurement work without including p-retry backoff.
                      providerPreparationMs:
                        Math.max(0, providerDispatchTimestampMs - attemptPreparationStartedAtMs) +
                        (attemptNumber === 1
                          ? preparationBeforeProviderCallbackMs +
                            Math.max(0, attemptPreparationStartedAtMs - preparationResumedAtMs)
                          : 0),
                    });
                  } catch {
                    // Measurement and observers must never prevent the provider call.
                  }
                }
              }
              const streamResult = await streamResultPromise;

              // We have to cast this because doStream is missing the warnings property in its return type even though it exists
              return streamResult as unknown as LanguageModelV2StreamResult;
            } catch (error) {
              try {
                onProviderAttemptError?.({ error, providerAttempt: attemptNumber });
              } catch {
                // Tracing must never change retry or terminal error behavior.
              }
              throw error;
            }
          },
          {
            retries: modelSettings?.maxRetries ?? 2,
            signal: abortSignal,
            // Pinned to p-retry's own defaults so the backoff it schedules stays
            // unchanged while remaining computable in onFailedAttempt below.
            minTimeout: RETRY_MIN_TIMEOUT_MS,
            factor: RETRY_BACKOFF_FACTOR,
            async onFailedAttempt(context) {
              // Runs before shouldRetry, so bail on anything that will not be retried:
              // a terminal error must not wait out a provider delay it will never use.
              if (context.retriesLeft <= 0 || !isRetryableModelError(context.error)) return;

              const retryAfterMs = getRetryAfterMs(context.error);
              if (retryAfterMs === undefined) return;

              // p-retry applies its own exponential delay after this hook resolves, so
              // wait only the remainder. Total wait lands on
              // max(backoff, min(Retry-After, cap)), the same rule
              // StreamErrorRetryProcessor applies, keeping a hostile or very large
              // Retry-After from wedging the run.
              const boundedRetryAfterMs = Math.min(retryAfterMs, DEFAULT_MAX_RETRY_AFTER_MS);
              const scheduledBackoffMs = RETRY_MIN_TIMEOUT_MS * RETRY_BACKOFF_FACTOR ** (context.attemptNumber - 1);
              await waitDelay(boundedRetryAfterMs - scheduledBackoffMs, abortSignal);
            },
            shouldRetry(context) {
              return isRetryableModelError(context.error);
            },
          },
        );
      } catch (error) {
        if (shouldThrowError) {
          throw error;
        }

        return {
          stream: new ReadableStream({
            start: async controller => {
              controller.enqueue({
                type: 'error',
                error,
              });
              controller.close();
            },
          }),
          warnings: [],
          request: {},
          rawResponse: {},
        };
      }
    },
  });

  return stream;
}
