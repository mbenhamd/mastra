/**
 * Model Span Tracing
 *
 * Provides span tracking for Model generations, including:
 * - MODEL_STEP spans (one per Model API call - includes processors and tool executions)
 * - MODEL_INFERENCE spans (the provider call itself - model latency only)
 * - MODEL_CHUNK spans (individual streaming chunks within an inference)
 *
 * Hierarchy: MODEL_GENERATION -> MODEL_STEP -> MODEL_INFERENCE -> MODEL_CHUNK
 *
 * Processors and tool executions remain children of MODEL_STEP (siblings of
 * MODEL_INFERENCE), so MODEL_INFERENCE measures pure model time.
 */

import { TransformStream } from 'node:stream/web';
import { isProxy } from 'node:util/types';
import { RequestContext } from '@mastra/core/di';
import { coreFeatures } from '@mastra/core/features';
import { SpanType } from '@mastra/core/observability';
import type {
  Span,
  EndGenerationOptions,
  EndSpanOptions,
  ErrorSpanOptions,
  ModelInferenceFinishPayload,
  ModelInferenceContext,
  ProviderInferenceOutcome,
  PreparedModelRequestAggregateMetrics,
  PreparedModelRequestMetrics,
  ProviderUsageMeasurementState,
  TracingContext,
  UpdateSpanOptions,
  UsageStats,
} from '@mastra/core/observability';
import type { ChunkType, StepStartPayload, StepFinishPayload } from '@mastra/core/stream';

/**
 * Feature gate for MODEL_INFERENCE spans. When the installed @mastra/core
 * predates the feature flag, `SpanType.MODEL_INFERENCE` resolves to undefined
 * at runtime; in that case the tracker falls back to parenting MODEL_CHUNK
 * spans directly under MODEL_STEP (the pre-MODEL_INFERENCE behavior).
 *
 * Read at every call so tests can toggle the flag between cases. The set
 * lookup is O(1) and not on a hot path.
 */
function supportsModelInference(): boolean {
  return coreFeatures.has('model-inference-span');
}

import { extractUsageMetrics } from './usage';

type StepInputPreview = Array<{ role: string; content: string }> | Record<string, unknown> | string | undefined;

/** Observability must never become part of provider or processor control flow. */
function runSpanOperation<T>(operation: () => T): T | undefined {
  try {
    return operation();
  } catch {
    return undefined;
  }
}

function ownStringDataProperty(value: unknown, key: string): string | undefined {
  if (!value || typeof value !== 'object' || isProxy(value)) return undefined;
  try {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    return descriptor && 'value' in descriptor && typeof descriptor.value === 'string' ? descriptor.value : undefined;
  } catch {
    return undefined;
  }
}

function isDescriptorSafeAbortError(error: unknown): boolean {
  return ownStringDataProperty(error, 'name') === 'AbortError';
}

function boundedProviderFinishError(outcome: Extract<ProviderInferenceOutcome, 'error' | 'abort'>): Error {
  const error = new Error(outcome === 'abort' ? 'Provider inference aborted' : 'Provider inference failed');
  error.name = outcome === 'abort' ? 'AbortError' : 'ProviderInferenceError';
  error.stack = undefined;
  return error;
}

function nonnegativeInteger(value: unknown): number {
  return typeof value === 'number' && Number.isInteger(value) && value >= 0 ? value : 0;
}

function inheritedRequestContext(span: Span<SpanType.MODEL_GENERATION> | undefined): RequestContext | undefined {
  const snapshot = runSpanOperation(() => span?.requestContext);
  if (!snapshot || typeof snapshot !== 'object' || isProxy(snapshot)) return undefined;
  const entries: Array<[string, unknown]> = [];
  const keys = runSpanOperation(() => Reflect.ownKeys(snapshot));
  if (!keys) return undefined;
  for (const key of keys) {
    if (typeof key !== 'string') return undefined;
    const descriptor = runSpanOperation(() => Object.getOwnPropertyDescriptor(snapshot, key));
    if (!descriptor || !('value' in descriptor)) return undefined;
    entries.push([key, descriptor.value]);
  }
  return runSpanOperation(() => new RequestContext(entries));
}

function parseGatewayCost(providerMetadata: EndGenerationOptions['providerMetadata']): number | undefined {
  const rawCost = providerMetadata?.gateway?.cost;
  const cost =
    typeof rawCost === 'number'
      ? rawCost
      : typeof rawCost === 'string' && rawCost.trim().length > 0
        ? Number(rawCost)
        : undefined;

  return typeof cost === 'number' && Number.isFinite(cost) && cost >= 0 ? cost : undefined;
}

function getGatewayCostContext({ stepProviderMetadata }: Pick<EndGenerationOptions, 'stepProviderMetadata'>) {
  if (!stepProviderMetadata) {
    return undefined;
  }

  if (!stepProviderMetadata.some(metadata => metadata?.gateway !== undefined)) {
    return undefined;
  }

  const costs: number[] = [];
  for (const metadata of stepProviderMetadata) {
    const cost = parseGatewayCost(metadata);
    if (cost === undefined) {
      return undefined;
    }
    costs.push(cost);
  }

  const estimatedCost = costs.reduce((total, cost) => total + cost, 0);
  if (!Number.isFinite(estimatedCost)) {
    return undefined;
  }

  return {
    estimatedCost,
    costUnit: 'USD',
    costMetadata: {
      source: 'provider_reported',
      sdkProvider: 'vercel_ai_gateway',
      sdkCostField: 'gateway.cost',
      scope: 'query_total',
      reportedStepCount: costs.length,
    },
  };
}

function hasReportedUsage(rawUsage: unknown, usage: UsageStats): boolean {
  if (
    usage.inputTokens !== undefined ||
    usage.outputTokens !== undefined ||
    Object.values(usage.inputDetails ?? {}).some(value => value !== undefined) ||
    Object.values(usage.outputDetails ?? {}).some(value => value !== undefined)
  ) {
    return true;
  }
  if (!rawUsage || typeof rawUsage !== 'object') return false;
  return [
    'inputTokens',
    'outputTokens',
    'totalTokens',
    'promptTokens',
    'completionTokens',
    'cachedInputTokens',
    'cacheCreationInputTokens',
    'reasoningTokens',
  ].some(key => {
    try {
      const descriptor = Object.getOwnPropertyDescriptor(rawUsage, key);
      return descriptor !== undefined && 'value' in descriptor && typeof descriptor.value === 'number';
    } catch {
      return false;
    }
  });
}

function providerUsageStates(rawUsage: unknown, usage: UsageStats) {
  const state: ProviderUsageMeasurementState = hasReportedUsage(rawUsage, usage) ? 'reported' : 'provider_not_reported';
  return {
    providerUsageState: state,
    providerCacheReadUsageState: usage.inputDetails?.cacheRead === undefined ? 'provider_not_reported' : 'reported',
    providerCacheWriteUsageState: usage.inputDetails?.cacheWrite === undefined ? 'provider_not_reported' : 'reported',
    providerReasoningUsageState: usage.outputDetails?.reasoning === undefined ? 'provider_not_reported' : 'reported',
  } as const;
}

function emptyPreparedRequestAggregate(): PreparedModelRequestAggregateMetrics {
  return {
    providerAggregateMeasurementState: 'not_applicable',
    providerBreakdownState: 'serialized_components_non_additive',
    providerInferenceCount: 0,
    providerMeasuredInferenceCount: 0,
    providerUnknownInferenceCount: 0,
    providerMessageCountTotal: 0,
    providerMessageBytesTotal: 0,
    providerSystemMessageCountTotal: 0,
    providerSystemMessageBytesTotal: 0,
    providerUserMessageCountTotal: 0,
    providerUserMessageBytesTotal: 0,
    providerAssistantMessageCountTotal: 0,
    providerAssistantMessageBytesTotal: 0,
    providerToolMessageCountTotal: 0,
    providerToolMessageBytesTotal: 0,
    providerOtherMessageCountTotal: 0,
    providerOtherMessageBytesTotal: 0,
    providerInstructionBytesTotal: 0,
    providerToolCountTotal: 0,
    providerToolSchemaBytesTotal: 0,
    providerResponseSchemaBytesTotal: 0,
    providerRequestBytesTotal: 0,
    providerPreparationMsTotal: 0,
    providerMeasurementMsTotal: 0,
  };
}

function appendCompleteTotal(previousCount: number, previous: number | undefined, current: number | undefined) {
  if (previousCount === 0) return current;
  if (previous === undefined || current === undefined) return undefined;
  return previous + current;
}

function appendPreparedRequestAggregate(
  previous: PreparedModelRequestAggregateMetrics,
  metric: PreparedModelRequestMetrics,
): PreparedModelRequestAggregateMetrics {
  const previousCount = previous.providerInferenceCount;
  const measured = previous.providerMeasuredInferenceCount + (metric.measurementState === 'measured' ? 1 : 0);
  const unknown = previous.providerUnknownInferenceCount + (metric.measurementState === 'unknown' ? 1 : 0);
  const inferenceCount = previousCount + 1;
  const state =
    measured === inferenceCount ? ('measured' as const) : measured === 0 ? ('unknown' as const) : ('partial' as const);
  const toolSchemaBytes = metric.providerToolSchemaState === 'not_applicable' ? 0 : metric.providerToolSchemaBytes;
  const responseSchemaBytes =
    metric.providerResponseSchemaState === 'not_applicable' ? 0 : metric.providerResponseSchemaBytes;

  return {
    providerAggregateMeasurementState: state,
    providerBreakdownState: 'serialized_components_non_additive',
    providerInferenceCount: inferenceCount,
    providerMeasuredInferenceCount: measured,
    providerUnknownInferenceCount: unknown,
    providerMessageCountTotal: appendCompleteTotal(
      previousCount,
      previous.providerMessageCountTotal,
      metric.providerMessageCount,
    ),
    providerMessageBytesTotal: appendCompleteTotal(
      previousCount,
      previous.providerMessageBytesTotal,
      metric.providerMessageBytes,
    ),
    providerSystemMessageCountTotal: appendCompleteTotal(
      previousCount,
      previous.providerSystemMessageCountTotal,
      metric.providerSystemMessageCount,
    ),
    providerSystemMessageBytesTotal: appendCompleteTotal(
      previousCount,
      previous.providerSystemMessageBytesTotal,
      metric.providerSystemMessageBytes,
    ),
    providerUserMessageCountTotal: appendCompleteTotal(
      previousCount,
      previous.providerUserMessageCountTotal,
      metric.providerUserMessageCount,
    ),
    providerUserMessageBytesTotal: appendCompleteTotal(
      previousCount,
      previous.providerUserMessageBytesTotal,
      metric.providerUserMessageBytes,
    ),
    providerAssistantMessageCountTotal: appendCompleteTotal(
      previousCount,
      previous.providerAssistantMessageCountTotal,
      metric.providerAssistantMessageCount,
    ),
    providerAssistantMessageBytesTotal: appendCompleteTotal(
      previousCount,
      previous.providerAssistantMessageBytesTotal,
      metric.providerAssistantMessageBytes,
    ),
    providerToolMessageCountTotal: appendCompleteTotal(
      previousCount,
      previous.providerToolMessageCountTotal,
      metric.providerToolMessageCount,
    ),
    providerToolMessageBytesTotal: appendCompleteTotal(
      previousCount,
      previous.providerToolMessageBytesTotal,
      metric.providerToolMessageBytes,
    ),
    providerOtherMessageCountTotal: appendCompleteTotal(
      previousCount,
      previous.providerOtherMessageCountTotal,
      metric.providerOtherMessageCount,
    ),
    providerOtherMessageBytesTotal: appendCompleteTotal(
      previousCount,
      previous.providerOtherMessageBytesTotal,
      metric.providerOtherMessageBytes,
    ),
    providerInstructionBytesTotal: appendCompleteTotal(
      previousCount,
      previous.providerInstructionBytesTotal,
      metric.providerInstructionBytes,
    ),
    providerToolCountTotal: appendCompleteTotal(
      previousCount,
      previous.providerToolCountTotal,
      metric.providerToolCount,
    ),
    providerToolSchemaBytesTotal: appendCompleteTotal(
      previousCount,
      previous.providerToolSchemaBytesTotal,
      toolSchemaBytes,
    ),
    providerResponseSchemaBytesTotal: appendCompleteTotal(
      previousCount,
      previous.providerResponseSchemaBytesTotal,
      responseSchemaBytes,
    ),
    providerRequestBytesTotal: appendCompleteTotal(
      previousCount,
      previous.providerRequestBytesTotal,
      metric.providerRequestBytes,
    ),
    providerPreparationMsTotal: appendCompleteTotal(
      previousCount,
      previous.providerPreparationMsTotal,
      metric.providerPreparationMs,
    ),
    providerMeasurementMsTotal: appendCompleteTotal(
      previousCount,
      previous.providerMeasurementMsTotal,
      metric.providerMeasurementMs,
    ),
  };
}

/** Reserve one real provider attempt when request measurement is unavailable. */
function appendUnknownPreparedRequestAggregate(
  previous: PreparedModelRequestAggregateMetrics,
): PreparedModelRequestAggregateMetrics {
  const previousCount = previous.providerInferenceCount;
  const inferenceCount = previousCount + 1;
  const measured = previous.providerMeasuredInferenceCount;
  const unknown = previous.providerUnknownInferenceCount + 1;
  return {
    providerAggregateMeasurementState: measured === 0 ? 'unknown' : 'partial',
    providerBreakdownState: 'serialized_components_non_additive',
    providerInferenceCount: inferenceCount,
    providerMeasuredInferenceCount: measured,
    providerUnknownInferenceCount: unknown,
    providerMessageCountTotal: appendCompleteTotal(previousCount, previous.providerMessageCountTotal, undefined),
    providerMessageBytesTotal: appendCompleteTotal(previousCount, previous.providerMessageBytesTotal, undefined),
    providerSystemMessageCountTotal: appendCompleteTotal(
      previousCount,
      previous.providerSystemMessageCountTotal,
      undefined,
    ),
    providerSystemMessageBytesTotal: appendCompleteTotal(
      previousCount,
      previous.providerSystemMessageBytesTotal,
      undefined,
    ),
    providerUserMessageCountTotal: appendCompleteTotal(
      previousCount,
      previous.providerUserMessageCountTotal,
      undefined,
    ),
    providerUserMessageBytesTotal: appendCompleteTotal(
      previousCount,
      previous.providerUserMessageBytesTotal,
      undefined,
    ),
    providerAssistantMessageCountTotal: appendCompleteTotal(
      previousCount,
      previous.providerAssistantMessageCountTotal,
      undefined,
    ),
    providerAssistantMessageBytesTotal: appendCompleteTotal(
      previousCount,
      previous.providerAssistantMessageBytesTotal,
      undefined,
    ),
    providerToolMessageCountTotal: appendCompleteTotal(
      previousCount,
      previous.providerToolMessageCountTotal,
      undefined,
    ),
    providerToolMessageBytesTotal: appendCompleteTotal(
      previousCount,
      previous.providerToolMessageBytesTotal,
      undefined,
    ),
    providerOtherMessageCountTotal: appendCompleteTotal(
      previousCount,
      previous.providerOtherMessageCountTotal,
      undefined,
    ),
    providerOtherMessageBytesTotal: appendCompleteTotal(
      previousCount,
      previous.providerOtherMessageBytesTotal,
      undefined,
    ),
    providerInstructionBytesTotal: appendCompleteTotal(
      previousCount,
      previous.providerInstructionBytesTotal,
      undefined,
    ),
    providerToolCountTotal: appendCompleteTotal(previousCount, previous.providerToolCountTotal, undefined),
    providerToolSchemaBytesTotal: appendCompleteTotal(previousCount, previous.providerToolSchemaBytesTotal, undefined),
    providerResponseSchemaBytesTotal: appendCompleteTotal(
      previousCount,
      previous.providerResponseSchemaBytesTotal,
      undefined,
    ),
    providerRequestBytesTotal: appendCompleteTotal(previousCount, previous.providerRequestBytesTotal, undefined),
    providerPreparationMsTotal: appendCompleteTotal(previousCount, previous.providerPreparationMsTotal, undefined),
    providerMeasurementMsTotal: appendCompleteTotal(previousCount, previous.providerMeasurementMsTotal, undefined),
  };
}

function preparedRequestAggregateFromAttributes(
  attributes: Partial<PreparedModelRequestAggregateMetrics> | undefined,
): PreparedModelRequestAggregateMetrics | undefined {
  if (
    attributes?.providerBreakdownState !== 'serialized_components_non_additive' ||
    !['measured', 'partial', 'unknown', 'not_applicable'].includes(attributes.providerAggregateMeasurementState ?? '')
  ) {
    return undefined;
  }

  const requiredTotals = [
    attributes.providerInferenceCount,
    attributes.providerMeasuredInferenceCount,
    attributes.providerUnknownInferenceCount,
  ];
  if (!requiredTotals.every(value => typeof value === 'number' && Number.isInteger(value) && value >= 0)) {
    return undefined;
  }

  const inferenceCount = attributes.providerInferenceCount!;
  const measuredCount = attributes.providerMeasuredInferenceCount!;
  const unknownCount = attributes.providerUnknownInferenceCount!;
  const expectedState =
    inferenceCount === 0
      ? 'not_applicable'
      : measuredCount === inferenceCount
        ? 'measured'
        : measuredCount === 0
          ? 'unknown'
          : 'partial';
  if (
    measuredCount + unknownCount !== inferenceCount ||
    attributes.providerAggregateMeasurementState !== expectedState
  ) {
    return undefined;
  }

  const optionalIntegerTotals = [
    attributes.providerMessageCountTotal,
    attributes.providerMessageBytesTotal,
    attributes.providerSystemMessageCountTotal,
    attributes.providerSystemMessageBytesTotal,
    attributes.providerUserMessageCountTotal,
    attributes.providerUserMessageBytesTotal,
    attributes.providerAssistantMessageCountTotal,
    attributes.providerAssistantMessageBytesTotal,
    attributes.providerToolMessageCountTotal,
    attributes.providerToolMessageBytesTotal,
    attributes.providerOtherMessageCountTotal,
    attributes.providerOtherMessageBytesTotal,
    attributes.providerInstructionBytesTotal,
    attributes.providerToolCountTotal,
    attributes.providerToolSchemaBytesTotal,
    attributes.providerResponseSchemaBytesTotal,
    attributes.providerRequestBytesTotal,
  ];
  if (!optionalIntegerTotals.every(value => value === undefined || (Number.isInteger(value) && value >= 0))) {
    return undefined;
  }
  const optionalDurations = [attributes.providerPreparationMsTotal, attributes.providerMeasurementMsTotal];
  if (
    !optionalDurations.every(
      value => value === undefined || (typeof value === 'number' && Number.isFinite(value) && value >= 0),
    )
  ) {
    return undefined;
  }

  return attributes as PreparedModelRequestAggregateMetrics;
}

function formatPreviewLabel(label: unknown, fallback: string): string {
  return typeof label === 'string' && label.length > 0 ? label : fallback;
}

function formatToolResultPreviewValue(value: unknown): string {
  if (typeof value === 'string') return value;

  if (value && typeof value === 'object' && 'type' in value && (value as { type?: unknown }).type === 'text') {
    const textValue = (value as { value?: unknown }).value;
    if (typeof textValue === 'string') return textValue;
  }

  return JSON.stringify(value);
}

function summarizePart(part: unknown): string {
  if (typeof part === 'string') {
    return part;
  }

  if (!part || typeof part !== 'object') {
    return '';
  }

  if ('text' in part && typeof part.text === 'string') {
    return part.text;
  }

  if ('parts' in part && Array.isArray(part.parts)) {
    return part.parts.map(summarizePart).filter(Boolean).join('');
  }

  if ('inlineData' in part && part.inlineData && typeof part.inlineData === 'object') {
    return `[${formatPreviewLabel((part.inlineData as { mimeType?: unknown }).mimeType, 'binary')}]`;
  }

  if ('image_url' in part) {
    return '[image]';
  }

  if ('functionCall' in part && part.functionCall && typeof part.functionCall === 'object') {
    return `[tool: ${formatPreviewLabel((part.functionCall as { name?: unknown }).name, 'unknown')}]`;
  }

  if ('function_call' in part && part.function_call && typeof part.function_call === 'object') {
    return `[tool: ${formatPreviewLabel((part.function_call as { name?: unknown }).name, 'unknown')}]`;
  }

  if ('function' in part && part.function && typeof part.function === 'object') {
    return `[tool: ${formatPreviewLabel((part.function as { name?: unknown }).name, 'unknown')}]`;
  }

  if ('toolName' in part && !('type' in part && (part as { type: unknown }).type === 'tool-result')) {
    return `[tool: ${formatPreviewLabel((part as { toolName?: unknown }).toolName, 'unknown')}]`;
  }

  if ('type' in part && typeof part.type === 'string') {
    switch (part.type) {
      case 'image':
        return '[image]';
      case 'file':
        return '[file]';
      case 'reasoning':
        return '[reasoning]';
      case 'tool-call':
        return `[tool: ${formatPreviewLabel((part as { toolName?: unknown }).toolName, 'unknown')}]`;
      case 'tool-result': {
        const toolResult = part as {
          providerMetadata?: Record<string, unknown>;
          providerOptions?: Record<string, unknown>;
        };
        const mastraMeta = (toolResult.providerMetadata?.mastra ?? toolResult.providerOptions?.mastra) as
          | Record<string, unknown>
          | undefined;
        const toolName = formatPreviewLabel((part as { toolName?: unknown }).toolName, 'unknown');
        if (mastraMeta?.modelOutput !== undefined) {
          return formatToolResultPreviewValue(mastraMeta.modelOutput);
        }
        return `[tool-result: ${toolName}]`;
      }
      default:
        return `[${part.type}]`;
    }
  }

  if ('content' in part && typeof part.content === 'string') {
    return part.content;
  }

  return '[object]';
}

function summarizeMessageContent(content: unknown): string {
  if (typeof content === 'string') {
    return content;
  }

  if (Array.isArray(content)) {
    return content.map(summarizePart).filter(Boolean).join('');
  }

  if (content && typeof content === 'object') {
    if ('parts' in content && Array.isArray(content.parts)) {
      return content.parts.map(summarizePart).filter(Boolean).join('');
    }

    return summarizePart(content);
  }

  if (content == null) {
    return '';
  }

  return String(content);
}

function appendToolPreview(preview: string, toolCalls: unknown): string {
  if (!Array.isArray(toolCalls) || toolCalls.length === 0) {
    return preview;
  }

  const toolPreview = toolCalls
    .map(toolCall => summarizePart(toolCall))
    .filter(Boolean)
    .join(' ');

  if (!toolPreview) {
    return preview;
  }

  return preview ? `${preview} ${toolPreview}` : toolPreview;
}

function appendPreview(preview: string, addition: string): string {
  if (!addition) {
    return preview;
  }

  return preview ? `${preview} ${addition}` : addition;
}

function normalizeMessages(messages: unknown[]): Array<{ role: string; content: string }> {
  return messages.map(message => {
    if (!message || typeof message !== 'object') {
      return { role: 'user', content: summarizeMessageContent(message) };
    }

    const role = typeof (message as { role?: unknown }).role === 'string' ? (message as { role: string }).role : 'user';

    const baseContent = summarizeMessageContent((message as { content?: unknown }).content);
    const contentWithToolArrays = appendToolPreview(
      appendToolPreview(baseContent, (message as { toolCalls?: unknown }).toolCalls),
      (message as { tool_calls?: unknown }).tool_calls,
    );
    const functionCall = (message as { functionCall?: unknown }).functionCall;
    const functionCallPreview = functionCall === undefined ? '' : summarizePart({ functionCall });
    const functionCallSnakeCase = (message as { function_call?: unknown }).function_call;
    const functionCallSnakeCasePreview =
      functionCallSnakeCase === undefined ? '' : summarizePart({ function_call: functionCallSnakeCase });
    const contentWithFunctionCall = appendPreview(
      appendPreview(contentWithToolArrays, functionCallPreview),
      functionCallSnakeCasePreview,
    );

    return { role, content: contentWithFunctionCall };
  });
}

function summarizeRequestBody(body: unknown): StepInputPreview {
  if (body == null) {
    return undefined;
  }

  if (typeof body !== 'object') {
    return typeof body === 'string' ? body : String(body);
  }

  if (Array.isArray((body as { messages?: unknown }).messages)) {
    return normalizeMessages((body as { messages: unknown[] }).messages);
  }

  if (Array.isArray((body as { input?: unknown }).input)) {
    return normalizeMessages((body as { input: unknown[] }).input);
  }

  if (Array.isArray((body as { contents?: unknown }).contents)) {
    return (body as { contents: Array<{ role?: unknown; parts?: unknown[] }> }).contents.map(item => ({
      role: typeof item?.role === 'string' ? item.role : 'user',
      content: Array.isArray(item?.parts) ? item.parts.map(summarizePart).filter(Boolean).join('') : '',
    }));
  }

  const summary: Record<string, unknown> = {};

  if (typeof (body as { model?: unknown }).model === 'string') {
    summary.model = (body as { model: string }).model;
  }

  const bodyKeys = Object.keys(body as Record<string, unknown>).filter(key => key !== 'body');
  if (bodyKeys.length > 0) {
    summary.keys = bodyKeys;
  }

  return Object.keys(summary).length > 0 ? summary : '[request body]';
}

/**
 * Extract a shallow conversation preview for model_step span input.
 */
function extractStepInput(payload?: StepStartPayload): StepInputPreview {
  if (Array.isArray(payload?.inputMessages)) {
    return normalizeMessages(payload.inputMessages);
  }

  const request = payload?.request;
  if (!request) return undefined;

  const { body } = request;
  if (body == null) return request;

  try {
    const parsed = typeof body === 'string' ? JSON.parse(body) : body;
    return summarizeRequestBody(parsed);
  } catch {
    // body was not valid JSON; return as-is
    return request;
  }
}

/**
 * Manages MODEL_STEP and MODEL_CHUNK span tracking for streaming Model responses.
 *
 * Should be instantiated once per MODEL_GENERATION span and shared across
 * all streaming steps (including after tool calls).
 */
export class ModelSpanTracker {
  #modelSpan?: Span<SpanType.MODEL_GENERATION>;
  #currentStepSpan?: Span<SpanType.MODEL_STEP>;
  #currentInferenceSpan?: Span<SpanType.MODEL_INFERENCE>;
  #currentChunkSpan?: Span<SpanType.MODEL_CHUNK>;
  #currentChunkType?: string;
  #accumulator: Record<string, any> = {};
  #providerChunkTimestamps = new WeakMap<object, Date>();
  #stepIndex: number = 0;
  #chunkSequence: number = 0;
  #completionStartTime?: Date;
  /** First provider content for the currently open inference only. */
  #currentInferenceCompletionStartTime?: Date;
  #currentStepInputIsFinal: boolean = false;
  /** When true, step-finish chunks don't auto-close the step span (for durable execution) */
  #deferStepClose: boolean = false;
  /** Stored step-finish payload when defer mode is enabled */
  #pendingStepFinishPayload?: StepFinishPayload<any, any>;
  /** Provider finish observed before core-normalized response identity is ready. */
  #pendingInferenceFinishPayload?: ModelInferenceFinishPayload;
  /** Provider-boundary timestamp retained while core finishes response attribution. */
  #pendingInferenceEndTime?: Date;
  /** Whether post-inference work has finished before step-finish was observed. */
  #deferredStepCloseRequested: boolean = false;
  /** Provider-returned identity for the currently open inference. */
  #currentInferenceResponse?: { responseModel?: string; responseId?: string };
  /** Core-normalized response identity takes precedence over raw stream metadata. */
  #hasNormalizedInferenceResponse: boolean = false;
  /** Static request-side context applied to every MODEL_INFERENCE span */
  #inferenceContext?: ModelInferenceContext;
  /** Generation-level request context inherited by provider-attempt spans. */
  #requestContext?: RequestContext;
  /** Content-free rollup for all provider calls, including rebuilt durable spans. */
  #preparedRequestAggregate: PreparedModelRequestAggregateMetrics;
  /** Conservative aggregate: one missing provider report keeps the turn missing. */
  #providerUsageState?: ProviderUsageMeasurementState;
  /** Whether this generation reached a provider inference boundary. */
  #providerInferenceStarted: boolean;
  /** Provider-attempt lifecycle is independent of exporter span creation. */
  #currentInferenceLifecycleOpen: boolean = false;
  /** Aggregate immediately before reserving the current unknown attempt. */
  #preparedRequestAggregateBeforeCurrentInference?: PreparedModelRequestAggregateMetrics;
  /** Whether the current attempt's unknown reservation was replaced by measurement. */
  #currentInferencePreparedRequestRecorded: boolean = false;
  #providerOutcomeCounts: {
    providerSucceededInferenceCount: number;
    providerErrorInferenceCount: number;
    providerAbortedInferenceCount: number;
  };
  /** Terminal control-flow reason observed without a normal generation finish. */
  #terminalGenerationFinishReason?: string;
  /** Whether the current step is expected to reach a provider boundary. */
  #currentStepInferenceApplicable: boolean = true;

  constructor(modelSpan?: Span<SpanType.MODEL_GENERATION>) {
    this.#modelSpan = modelSpan;
    this.#requestContext = inheritedRequestContext(modelSpan);
    const modelAttributes = runSpanOperation(() => modelSpan?.attributes);
    this.#preparedRequestAggregate =
      runSpanOperation(() => preparedRequestAggregateFromAttributes(modelAttributes)) ??
      emptyPreparedRequestAggregate();
    const restoredProviderUsageState = runSpanOperation(() => modelAttributes?.providerUsageState);
    this.#providerUsageState =
      restoredProviderUsageState === 'reported' ||
      restoredProviderUsageState === 'provider_not_reported' ||
      restoredProviderUsageState === 'not_applicable'
        ? restoredProviderUsageState
        : undefined;
    this.#providerOutcomeCounts = {
      providerSucceededInferenceCount: nonnegativeInteger(modelAttributes?.providerSucceededInferenceCount),
      providerErrorInferenceCount: nonnegativeInteger(modelAttributes?.providerErrorInferenceCount),
      providerAbortedInferenceCount: nonnegativeInteger(modelAttributes?.providerAbortedInferenceCount),
    };
    this.#providerInferenceStarted =
      this.#preparedRequestAggregate.providerInferenceCount > 0 ||
      this.#providerUsageState === 'reported' ||
      this.#providerUsageState === 'provider_not_reported';
  }

  /**
   * Set request-side context applied to subsequent MODEL_INFERENCE spans.
   * No-op when paired with an older @mastra/core that lacks the feature flag.
   */
  setInferenceContext(context: ModelInferenceContext): void {
    this.#inferenceContext = context;
  }

  /**
   * Attach the content-free measurement captured from the exact object passed
   * to the provider adapter. The inference span is already open at this point.
   */
  recordPreparedRequest(metrics: PreparedModelRequestMetrics): void {
    const aggregateBase =
      this.#currentInferenceLifecycleOpen &&
      !this.#currentInferencePreparedRequestRecorded &&
      this.#preparedRequestAggregateBeforeCurrentInference
        ? this.#preparedRequestAggregateBeforeCurrentInference
        : this.#preparedRequestAggregate;
    const aggregate = runSpanOperation(() => appendPreparedRequestAggregate(aggregateBase, metrics));
    if (!aggregate) return;
    this.#preparedRequestAggregate = aggregate;
    if (this.#currentInferenceLifecycleOpen) this.#currentInferencePreparedRequestRecorded = true;
    runSpanOperation(() => this.#currentInferenceSpan?.update({ attributes: metrics }));
    runSpanOperation(() => this.#modelSpan?.update({ attributes: aggregate }));
  }

  /** Record response identity after core has normalized provider-side fallbacks. */
  recordResponseMetadata(metadata: { responseId?: string; responseModel?: string }): void {
    if (this.#hasNormalizedInferenceResponse) return;
    const responseId = typeof metadata.responseId === 'string' ? metadata.responseId : undefined;
    const responseModel = typeof metadata.responseModel === 'string' ? metadata.responseModel : undefined;
    if (responseId === undefined && responseModel === undefined) return;
    const attributes = {
      ...(responseId === undefined ? {} : { responseId }),
      ...(responseModel === undefined ? {} : { responseModel }),
    };
    this.#hasNormalizedInferenceResponse = true;
    this.#currentInferenceResponse = { ...this.#currentInferenceResponse, ...attributes };
    runSpanOperation(() => this.#currentInferenceSpan?.update({ attributes }));
  }

  #startProviderInferenceLifecycle(): boolean {
    if (this.#currentInferenceLifecycleOpen) return false;
    this.#providerInferenceStarted = true;
    this.#currentInferenceLifecycleOpen = true;
    this.#currentInferencePreparedRequestRecorded = false;
    this.#preparedRequestAggregateBeforeCurrentInference = this.#preparedRequestAggregate;
    this.#preparedRequestAggregate = appendUnknownPreparedRequestAggregate(this.#preparedRequestAggregate);
    runSpanOperation(() => this.#modelSpan?.update({ attributes: this.#preparedRequestAggregate }));
    return true;
  }

  #recordProviderOutcome(outcome: ProviderInferenceOutcome): void {
    if (!this.#currentInferenceLifecycleOpen) return;
    this.#currentInferenceLifecycleOpen = false;
    this.#currentInferencePreparedRequestRecorded = false;
    this.#preparedRequestAggregateBeforeCurrentInference = undefined;
    if (outcome === 'success') this.#providerOutcomeCounts.providerSucceededInferenceCount++;
    else if (outcome === 'error') this.#providerOutcomeCounts.providerErrorInferenceCount++;
    else this.#providerOutcomeCounts.providerAbortedInferenceCount++;
    runSpanOperation(() => this.#modelSpan?.update({ attributes: this.#providerOutcomeCounts }));
  }

  /**
   * Capture the completion start time (time to first token) when the first content chunk arrives.
   */
  #captureCompletionStartTime(): void {
    // Older callers may omit the explicit pre-dispatch startInference() call.
    // Open its bounded fallback before taking the timestamp so TTFT never
    // predates the inference span that owns it.
    if (!this.#currentInferenceSpan) this.#ensureStepAndInference();
    const completionStartedAt = new Date();
    this.#completionStartTime ??= completionStartedAt;
    this.#currentInferenceCompletionStartTime ??= completionStartedAt;
  }

  /**
   * Get the tracing context for creating child spans.
   * Returns the current step span if active, otherwise the model span.
   */
  getTracingContext(): TracingContext {
    return {
      currentSpan: this.#currentStepSpan ?? this.#modelSpan,
    };
  }

  /**
   * Report an error on the generation span, also closing any still-open
   * MODEL_INFERENCE / MODEL_STEP children so a fatal error before step-finish
   * doesn't leave them dangling. No-op if they were already closed.
   */
  reportGenerationError(options: ErrorSpanOptions<SpanType.MODEL_GENERATION>): void {
    if (!this.#flushPendingInferenceFinish()) this.#endChunkSpan();
    const inferenceSpan = this.#currentInferenceSpan;
    const stepSpan = this.#currentStepSpan;
    const completionStartTime = this.#currentInferenceCompletionStartTime;
    const inferenceResponse = this.#currentInferenceResponse;
    const providerInferenceEndedWithoutUsage = inferenceSpan !== undefined || this.#currentInferenceLifecycleOpen;
    const providerOutcome: Extract<ProviderInferenceOutcome, 'error' | 'abort'> = isDescriptorSafeAbortError(
      options.error,
    )
      ? 'abort'
      : 'error';
    const failureFinishReason = providerOutcome === 'abort' ? 'abort' : 'error';
    this.#currentInferenceSpan = undefined;
    this.#currentInferenceCompletionStartTime = undefined;
    this.#currentInferenceResponse = undefined;
    this.#hasNormalizedInferenceResponse = false;
    this.#pendingInferenceEndTime = undefined;
    if (stepSpan) this.#resetCurrentStep();
    this.#recordProviderOutcome(providerOutcome);
    if (providerOutcome === 'abort') this.#terminalGenerationFinishReason = 'abort';

    runSpanOperation(() =>
      inferenceSpan?.error({
        error: options.error,
        endSpan: true,
        attributes: {
          ...providerUsageStates(undefined, {}),
          providerOutcome,
          finishReason: failureFinishReason,
          ...(completionStartTime === undefined ? {} : { completionStartTime }),
          ...inferenceResponse,
        },
      }),
    );
    runSpanOperation(() =>
      stepSpan?.error({ error: options.error, endSpan: true, attributes: { finishReason: failureFinishReason } }),
    );
    if (providerInferenceEndedWithoutUsage) {
      this.#providerUsageState = 'provider_not_reported';
    } else if (this.#providerUsageState === undefined) {
      this.#providerUsageState = this.#providerInferenceStarted ? 'provider_not_reported' : 'not_applicable';
    }
    runSpanOperation(() =>
      this.#modelSpan?.error({
        ...options,
        attributes: {
          ...options.attributes,
          finishReason: failureFinishReason,
          ...this.#preparedRequestAggregate,
          ...this.#providerOutcomeCounts,
          providerUsageState: this.#providerUsageState,
        },
      }),
    );
  }

  /**
   * End the generation span with optional raw usage data.
   * If usage is provided, it will be converted to UsageStats with cache token details.
   */
  endGeneration(options?: EndGenerationOptions): void {
    if (this.#currentStepSpan) {
      const finishReason = runSpanOperation(() => options?.attributes?.finishReason);
      const pendingInferenceFinish = this.#pendingInferenceFinishPayload;
      if (pendingInferenceFinish) this.#endInferenceSpan(pendingInferenceFinish);
      this.endStep({
        attributes: {
          finishReason: typeof finishReason === 'string' ? finishReason : 'unknown',
          isContinued: false,
        },
      });
    }
    runSpanOperation(() => {
      const { usage, providerMetadata, stepProviderMetadata, ...spanOptions } = options ?? {};
      const normalizedUsage = extractUsageMetrics(usage, providerMetadata);
      const costContext = spanOptions.attributes?.costContext ?? getGatewayCostContext({ stepProviderMetadata });
      const reportedUsageState = providerUsageStates(usage, normalizedUsage).providerUsageState;
      const providerUsageState =
        this.#providerUsageState ??
        (!this.#providerInferenceStarted
          ? 'not_applicable'
          : reportedUsageState === 'reported'
            ? 'reported'
            : 'provider_not_reported');

      this.#modelSpan?.end({
        ...spanOptions,
        attributes: {
          ...spanOptions.attributes,
          ...(spanOptions.attributes?.finishReason === undefined && this.#terminalGenerationFinishReason !== undefined
            ? { finishReason: this.#terminalGenerationFinishReason }
            : {}),
          ...this.#preparedRequestAggregate,
          ...this.#providerOutcomeCounts,
          completionStartTime: this.#completionStartTime,
          usage: normalizedUsage,
          providerUsageState,
          ...(costContext === undefined ? {} : { costContext }),
        },
      });
    });
  }

  /**
   * Update the generation span
   */
  updateGeneration(options: UpdateSpanOptions<SpanType.MODEL_GENERATION>): void {
    runSpanOperation(() => this.#modelSpan?.update(options));
  }

  /**
   * Enable or disable deferred step closing for durable execution.
   * When enabled, step-finish chunks won't automatically close the step span.
   * Use exportCurrentStep() to get the span data, then close it manually later.
   */
  setDeferStepClose(defer: boolean): void {
    this.#deferStepClose = defer;
  }

  /** Close a deferred step without racing the asynchronously consumed stream. */
  endDeferredStep(payload?: StepFinishPayload<any, any>): void {
    if (!this.#currentStepSpan) return;
    if (payload) {
      this.#endStepSpan(payload);
      return;
    }
    const pendingPayload = this.#pendingStepFinishPayload;
    if (pendingPayload) {
      this.#endStepSpan(pendingPayload);
      return;
    }
    this.#deferredStepCloseRequested = true;
  }

  /**
   * Export the current step span for later rebuilding (durable execution).
   * Returns undefined if no step span is active.
   */
  exportCurrentStep(): ReturnType<Span<SpanType.MODEL_STEP>['exportSpan']> | undefined {
    return runSpanOperation(() => this.#currentStepSpan?.exportSpan());
  }

  /**
   * Get the pending step finish payload (captured when defer mode is enabled).
   * This contains usage, finishReason, etc. for closing the step later.
   */
  getPendingStepFinishPayload(): StepFinishPayload<any, any> | undefined {
    return this.#pendingStepFinishPayload;
  }

  /**
   * Set the starting step index for durable execution.
   * Used when resuming across agentic loop iterations to maintain step continuity.
   */
  setStepIndex(index: number): void {
    this.#stepIndex = index;
  }

  /**
   * Get the current step index.
   */
  getStepIndex(): number {
    return this.#stepIndex;
  }

  /**
   * Start a new Model execution step.
   * This should be called at the beginning of LLM execution to capture accurate startTime.
   * The step-start chunk payload can be passed later via updateStep() if needed.
   *
   * Note: this only opens MODEL_STEP. The MODEL_INFERENCE child span is opened
   * separately via startInference() so its duration excludes input processor work.
   * Callers that don't call startInference() explicitly will get one auto-created
   * when the first model chunk arrives.
   */
  startStep(payload?: StepStartPayload): void {
    // Don't create duplicate step spans
    if (this.#currentStepSpan) {
      return;
    }

    this.#currentStepSpan = runSpanOperation(() => {
      const input = extractStepInput(payload);
      return this.#modelSpan?.createChildSpan({
        name: `step: ${this.#stepIndex}`,
        type: SpanType.MODEL_STEP,
        attributes: {
          stepIndex: this.#stepIndex,
          ...(payload?.messageId ? { messageId: payload.messageId } : {}),
          ...(payload?.warnings?.length ? { warnings: payload.warnings } : {}),
        },
        input,
        requestContext: this.#requestContext,
        tracingPolicy: this.#modelSpan?.tracingPolicy,
      });
    });
    this.#currentStepInputIsFinal = runSpanOperation(() => Array.isArray(payload?.inputMessages)) ?? false;
    this.#currentStepInferenceApplicable = true;
    this.#deferredStepCloseRequested = false;
    this.#currentInferenceResponse = undefined;
    this.#hasNormalizedInferenceResponse = false;
    // Reset chunk sequence for new step
    this.#chunkSequence = 0;
  }

  /** End the active step without closing its generation. */
  endStep(options?: EndSpanOptions<SpanType.MODEL_STEP>): void {
    if (!this.#flushPendingInferenceFinish()) this.#endChunkSpan();
    const inferenceSpan = this.#currentInferenceSpan;
    const completionStartTime = this.#currentInferenceCompletionStartTime;
    const inferenceResponse = this.#currentInferenceResponse;
    const inferenceEndTime = this.#pendingInferenceEndTime;
    const finishReason = runSpanOperation(() => options?.attributes?.finishReason);
    this.#currentInferenceSpan = undefined;
    this.#currentInferenceCompletionStartTime = undefined;
    this.#currentInferenceResponse = undefined;
    this.#hasNormalizedInferenceResponse = false;
    this.#pendingInferenceEndTime = undefined;
    if (inferenceSpan || this.#currentInferenceLifecycleOpen) {
      const providerOutcome: Extract<ProviderInferenceOutcome, 'error' | 'abort'> =
        finishReason === 'abort' || finishReason === 'tripwire' ? 'abort' : 'error';
      this.#recordProviderOutcome(providerOutcome);
      this.#providerUsageState = 'provider_not_reported';
      runSpanOperation(() =>
        inferenceSpan?.error({
          error: boundedProviderFinishError(providerOutcome),
          endSpan: true,
          attributes: {
            ...providerUsageStates(undefined, {}),
            providerOutcome,
            ...(typeof finishReason === 'string' ? { finishReason } : {}),
            ...(completionStartTime === undefined ? {} : { completionStartTime }),
            ...inferenceResponse,
          },
          endTime: inferenceEndTime,
        }),
      );
      runSpanOperation(() => this.#modelSpan?.update({ attributes: { providerUsageState: this.#providerUsageState } }));
    }
    if (finishReason === 'abort') {
      this.#terminalGenerationFinishReason = finishReason;
      runSpanOperation(() => this.#modelSpan?.update({ attributes: { finishReason } }));
    }
    const stepSpan = this.#currentStepSpan;
    if (!stepSpan) return;
    this.#resetCurrentStep();
    runSpanOperation(() => stepSpan.end(options));
  }

  /** Error the active step without closing its generation. */
  reportStepError(options: ErrorSpanOptions<SpanType.MODEL_STEP>): void {
    if (!this.#flushPendingInferenceFinish()) this.#endChunkSpan();
    if (this.#currentInferenceSpan || this.#currentInferenceLifecycleOpen) {
      this.reportInferenceError({ error: options.error });
    }
    const stepSpan = this.#currentStepSpan;
    if (!stepSpan) return;
    this.#resetCurrentStep();
    runSpanOperation(() => stepSpan.error({ ...options, endSpan: true }));
  }

  /**
   * End the current MODEL_INFERENCE span when the provider stream finishes.
   * Fields are duplicated onto MODEL_STEP (in #endStepSpan) so existing
   * integrations that read usage/finishReason from the step span continue
   * to work unchanged.
   *
   * Safe to call multiple times - no-ops if the span is already closed.
   */
  #endInferenceSpan(payload: ModelInferenceFinishPayload | StepFinishPayload<any, any>): void {
    const inferenceSpan = this.#currentInferenceSpan;
    if (!inferenceSpan && !this.#currentInferenceLifecycleOpen) return;
    const inferenceEndTime = this.#pendingInferenceEndTime;
    this.#endChunkSpan(undefined, inferenceEndTime);
    const completionStartTime = this.#currentInferenceCompletionStartTime;
    const inferenceResponse = this.#currentInferenceResponse;
    this.#currentInferenceSpan = undefined;
    this.#currentInferenceCompletionStartTime = undefined;
    this.#currentInferenceResponse = undefined;
    this.#hasNormalizedInferenceResponse = false;
    this.#pendingInferenceFinishPayload = undefined;
    this.#pendingInferenceEndTime = undefined;
    const providerOutcome: ProviderInferenceOutcome =
      payload.stepResult.reason === 'abort' ? 'abort' : payload.stepResult.reason === 'error' ? 'error' : 'success';
    this.#recordProviderOutcome(providerOutcome);
    if (providerOutcome === 'abort') this.#terminalGenerationFinishReason = 'abort';
    this.#currentStepInferenceApplicable = false;

    let endedWithPayload = false;
    if (inferenceSpan)
      runSpanOperation(() => {
        const { usage: rawUsage, ...otherOutput } = payload.output;
        const usage = extractUsageMetrics(rawUsage, payload.metadata?.providerMetadata);
        const usageStates = providerUsageStates(rawUsage, usage);
        this.#providerUsageState =
          this.#providerUsageState === 'provider_not_reported' ||
          usageStates.providerUsageState === 'provider_not_reported'
            ? 'provider_not_reported'
            : 'reported';
        runSpanOperation(() =>
          this.#modelSpan?.update({ attributes: { providerUsageState: this.#providerUsageState } }),
        );
        const attributes = {
          usage,
          finishReason: payload.stepResult.reason,
          warnings: payload.stepResult.warnings,
          completionStartTime,
          ...inferenceResponse,
          ...usageStates,
          providerOutcome,
        };
        if (providerOutcome === 'success') {
          inferenceSpan.end({ output: otherOutput, attributes, endTime: inferenceEndTime });
        } else {
          inferenceSpan.error({
            error: boundedProviderFinishError(providerOutcome),
            endSpan: true,
            attributes,
            endTime: inferenceEndTime,
          });
        }
        endedWithPayload = true;
      });
    if (inferenceSpan && !endedWithPayload) {
      this.#providerUsageState = 'provider_not_reported';
      runSpanOperation(() => this.#modelSpan?.update({ attributes: { providerUsageState: this.#providerUsageState } }));
      runSpanOperation(() => {
        const attributes = {
          ...providerUsageStates(undefined, {}),
          ...(completionStartTime === undefined ? {} : { completionStartTime }),
          ...inferenceResponse,
          providerOutcome,
        };
        if (providerOutcome === 'success') {
          inferenceSpan.end({ attributes, endTime: inferenceEndTime });
        } else {
          inferenceSpan.error({
            error: boundedProviderFinishError(providerOutcome),
            endSpan: true,
            attributes,
            endTime: inferenceEndTime,
          });
        }
      });
    }
  }

  /**
   * Open the MODEL_INFERENCE span for the current step. Chunks (including tool-call
   * chunks emitted by the model) parent under this span so its duration reflects
   * pure model latency.
   *
   * Should be called immediately before invoking the model — after any input
   * processors / `prepareStep` work has completed — so the span's startTime
   * does not include processor time. The latest `#inferenceContext` (set via
   * setInferenceContext) is snapshotted onto the span at creation.
   *
   * No-ops when the installed @mastra/core lacks the `model-inference-span`
   * feature flag, or when called without an active step span. Auto-invoked from
   * chunk handlers as a safety net; explicit callers get the most accurate
   * start time.
   */
  startInference(payload?: StepStartPayload, providerAttempt?: number): void {
    if (!this.#currentStepSpan || !this.#currentStepInferenceApplicable) {
      return;
    }
    if (!this.#startProviderInferenceLifecycle()) {
      if (providerAttempt !== undefined) {
        runSpanOperation(() => this.#currentInferenceSpan?.update({ attributes: { providerAttempt } }));
      }
      return;
    }
    if (runSpanOperation(supportsModelInference) !== true) return;

    this.#currentInferenceCompletionStartTime = undefined;
    this.#pendingInferenceFinishPayload = undefined;
    this.#pendingInferenceEndTime = undefined;
    const stepSpan = this.#currentStepSpan;
    this.#currentInferenceSpan = runSpanOperation(() => {
      const input = extractStepInput(payload);
      const generationAttrs = this.#modelSpan?.attributes;
      const ctx = this.#inferenceContext;
      return stepSpan.createChildSpan({
        name: `inference: ${this.#stepIndex}`,
        type: SpanType.MODEL_INFERENCE,
        attributes: {
          stepIndex: this.#stepIndex,
          model: generationAttrs?.model,
          provider: generationAttrs?.provider,
          streaming: generationAttrs?.streaming,
          ...(providerAttempt === undefined ? {} : { providerAttempt }),
          ...(ctx?.parameters !== undefined ? { parameters: ctx.parameters } : {}),
          ...(ctx?.providerOptions !== undefined ? { providerOptions: ctx.providerOptions } : {}),
          ...(ctx?.availableTools !== undefined ? { availableTools: ctx.availableTools } : {}),
          ...(ctx?.toolChoice !== undefined ? { toolChoice: ctx.toolChoice } : {}),
          ...(ctx?.responseFormat !== undefined ? { responseFormat: ctx.responseFormat } : {}),
        },
        input,
        requestContext: this.#requestContext,
        tracingPolicy: this.#modelSpan?.tracingPolicy,
      });
    });
  }

  /** Record first semantic content before adapter buffering can hide a later stream error. */
  recordInferenceContentStart(): void {
    this.#captureCompletionStartTime();
  }

  #reportInferenceFailure(
    options: ErrorSpanOptions<SpanType.MODEL_INFERENCE>,
    providerOutcome: Extract<ProviderInferenceOutcome, 'error' | 'abort'>,
  ): void {
    if (this.#flushPendingInferenceFinish()) return;
    const inferenceSpan = this.#currentInferenceSpan;
    if (!inferenceSpan && !this.#currentInferenceLifecycleOpen) return;
    this.#endChunkSpan();
    const completionStartTime = this.#currentInferenceCompletionStartTime;
    const inferenceResponse = this.#currentInferenceResponse;
    this.#currentInferenceSpan = undefined;
    this.#currentInferenceCompletionStartTime = undefined;
    this.#currentInferenceResponse = undefined;
    this.#hasNormalizedInferenceResponse = false;
    this.#pendingInferenceFinishPayload = undefined;
    this.#pendingInferenceEndTime = undefined;
    this.#recordProviderOutcome(providerOutcome);
    this.#providerUsageState = 'provider_not_reported';
    if (providerOutcome === 'abort') {
      this.#terminalGenerationFinishReason = 'abort';
    }
    runSpanOperation(() =>
      inferenceSpan?.error({
        ...options,
        endSpan: true,
        attributes: {
          ...options.attributes,
          ...providerUsageStates(undefined, {}),
          providerOutcome,
          ...(providerOutcome === 'abort' ? { finishReason: 'abort' } : {}),
          ...(completionStartTime === undefined ? {} : { completionStartTime }),
          ...inferenceResponse,
        },
      }),
    );
    runSpanOperation(() =>
      this.#modelSpan?.update({
        attributes: {
          providerUsageState: this.#providerUsageState,
          ...(providerOutcome === 'abort' ? { finishReason: 'abort' } : {}),
        },
      }),
    );
  }

  /** Close a failed provider attempt while retaining its parent step for retry. */
  reportInferenceError(options: ErrorSpanOptions<SpanType.MODEL_INFERENCE>): void {
    this.#reportInferenceFailure(options, isDescriptorSafeAbortError(options.error) ? 'abort' : 'error');
  }

  /** Close a cancelled provider attempt while retaining its parent step. */
  reportInferenceAbort(options: ErrorSpanOptions<SpanType.MODEL_INFERENCE>): void {
    this.#reportInferenceFailure(options, 'abort');
  }

  /** Close at the inner provider finish boundary after core normalization. */
  endInference(payload?: ModelInferenceFinishPayload): void {
    const pendingPayload = this.#pendingInferenceFinishPayload ?? payload;
    if (pendingPayload) this.#endInferenceSpan(pendingPayload);
  }

  /** Retain an eager provider timestamp without observing chunk content. */
  recordInferenceChunkTimestamp(chunk: unknown, observedAt: Date = new Date()): void {
    if ((typeof chunk !== 'object' && typeof chunk !== 'function') || chunk === null) return;
    const timestamp = runSpanOperation(() => observedAt.getTime());
    if (typeof timestamp !== 'number' || !Number.isFinite(timestamp)) return;
    this.#providerChunkTimestamps.set(chunk, new Date(timestamp));
  }

  /** Capture provider finish before downstream stream backpressure. */
  recordInferenceFinish(
    payload: ModelInferenceFinishPayload,
    endTime: Date = new Date(),
    response?: { responseId?: string; responseModel?: string },
  ): void {
    if (!this.#currentInferenceLifecycleOpen || this.#pendingInferenceFinishPayload) return;
    if (response) {
      const responseId = typeof response.responseId === 'string' ? response.responseId : undefined;
      const responseModel = typeof response.responseModel === 'string' ? response.responseModel : undefined;
      const attributes = {
        ...(responseId === undefined ? {} : { responseId }),
        ...(responseModel === undefined ? {} : { responseModel }),
      };
      this.#currentInferenceResponse = responseId === undefined && responseModel === undefined ? undefined : attributes;
      this.#hasNormalizedInferenceResponse = true;
      if (responseId !== undefined || responseModel !== undefined) {
        runSpanOperation(() => this.#currentInferenceSpan?.update({ attributes }));
      }
    }
    this.#pendingInferenceFinishPayload = payload;
    const timestamp = runSpanOperation(() => endTime.getTime());
    this.#pendingInferenceEndTime =
      typeof timestamp === 'number' && Number.isFinite(timestamp) ? new Date(timestamp) : undefined;
  }

  #flushPendingInferenceFinish(): boolean {
    const pendingPayload = this.#pendingInferenceFinishPayload;
    if (!pendingPayload) return false;
    this.#endInferenceSpan(pendingPayload);
    return true;
  }

  /** Prevent replayed cache chunks from fabricating a provider inference. */
  markInferenceNotApplicable(): void {
    this.#currentStepInferenceApplicable = false;
  }

  /**
   * Update the current step span with additional payload data.
   * Called when step-start chunk arrives with request/warnings info.
   */
  updateStep(payload?: StepStartPayload): void {
    if (!this.#currentStepSpan || !payload) {
      return;
    }

    const stepSpan = this.#currentStepSpan;
    runSpanOperation(() => {
      const hasFinalInput = Array.isArray(payload.inputMessages);
      const input = hasFinalInput || !this.#currentStepInputIsFinal ? extractStepInput(payload) : undefined;

      // Update span with request/warnings from the step-start chunk
      stepSpan.update({
        ...(input !== undefined ? { input } : {}),
        attributes: {
          ...(payload.messageId ? { messageId: payload.messageId } : {}),
          ...(payload.warnings?.length ? { warnings: payload.warnings } : {}),
        },
      });
      if (hasFinalInput) {
        this.#currentStepInputIsFinal = true;
      }
    });
  }

  /** Capture provider-returned response identity without invoking payload hooks. */
  #recordInferenceResponse(payload: unknown): void {
    if (this.#hasNormalizedInferenceResponse) return;
    const responseId = ownStringDataProperty(payload, 'id');
    const responseModel = ownStringDataProperty(payload, 'modelId');
    if (responseId === undefined && responseModel === undefined) return;

    const attributes = {
      ...(responseId === undefined ? {} : { responseId }),
      ...(responseModel === undefined ? {} : { responseModel }),
    };
    this.#currentInferenceResponse = { ...this.#currentInferenceResponse, ...attributes };
    runSpanOperation(() => this.#currentInferenceSpan?.update({ attributes }));
  }

  /**
   * End the current Model execution step with token usage, finish reason, output, and metadata
   */
  #endStepSpan<OUTPUT>(payload: StepFinishPayload<any, OUTPUT>) {
    const stepSpan = this.#currentStepSpan;
    if (!stepSpan) return;

    // Inference may already be closed (closed eagerly on step-finish in defer
    // mode so its duration reflects pure model latency, not subsequent tool
    // execution). Close it here for the non-deferred path.
    if (!this.#flushPendingInferenceFinish()) this.#endInferenceSpan(payload);
    // Safety net for callers that created a chunk without an inference span.
    this.#endChunkSpan();
    this.#resetCurrentStep();

    let endedWithOutput = false;
    runSpanOperation(() => {
      // Extract all data from step-finish chunk
      const output = payload.output;
      const { usage: rawUsage, ...otherOutput } = output;
      const stepResult = payload.stepResult;
      const metadata = payload.metadata;
      const usage = extractUsageMetrics(rawUsage, metadata?.providerMetadata);

      // Remove verbose/redundant fields from metadata.
      const cleanMetadata = metadata ? { ...metadata } : undefined;
      if (cleanMetadata) {
        for (const key of ['request', 'id', 'timestamp', 'modelId', 'modelVersion', 'modelProvider']) {
          delete cleanMetadata[key];
        }
      }

      stepSpan.end({
        output: otherOutput,
        attributes: {
          usage,
          isContinued: stepResult.isContinued,
          finishReason: stepResult.reason,
          warnings: stepResult.warnings,
        },
        metadata: {
          ...cleanMetadata,
        },
      });
      endedWithOutput = true;
    });
    if (!endedWithOutput) runSpanOperation(() => stepSpan.end());
  }

  #resetCurrentStep(): void {
    this.#currentStepSpan = undefined;
    this.#currentStepInputIsFinal = false;
    this.#currentStepInferenceApplicable = true;
    this.#pendingStepFinishPayload = undefined;
    this.#pendingInferenceFinishPayload = undefined;
    this.#pendingInferenceEndTime = undefined;
    this.#deferredStepCloseRequested = false;
    this.#currentInferenceResponse = undefined;
    this.#hasNormalizedInferenceResponse = false;
    this.#stepIndex++;
  }

  /**
   * Returns the parent span for chunks. Chunks parent under MODEL_INFERENCE
   * (the provider call) when available, falling back to MODEL_STEP only if
   * startStep() was bypassed.
   */
  #chunkParent(): Span<SpanType.MODEL_INFERENCE> | Span<SpanType.MODEL_STEP> | undefined {
    return this.#currentInferenceSpan ?? this.#currentStepSpan;
  }

  /**
   * Safety-net invoked from chunk handlers: auto-create MODEL_STEP and
   * MODEL_INFERENCE if a chunk arrives before the loop has explicitly opened
   * them, so chunks parent under MODEL_INFERENCE rather than falling through
   * to MODEL_STEP. Idempotent — each public start* method is itself a no-op
   * when its span is already live.
   */
  #ensureStepAndInference(): void {
    if (!this.#currentStepSpan) {
      this.startStep();
    }
    if (!this.#currentInferenceSpan) {
      this.startInference();
    }
  }

  #takeProviderChunkTimestamp(chunk: unknown): Date | undefined {
    if ((typeof chunk !== 'object' && typeof chunk !== 'function') || chunk === null) return undefined;
    const timestamp = this.#providerChunkTimestamps.get(chunk);
    this.#providerChunkTimestamps.delete(chunk);
    return timestamp;
  }

  /**
   * Create a new chunk span (for multi-part chunks like text-start/delta/end)
   */
  #startChunkSpan(chunkType: string, initialData?: Record<string, any>, startTime?: Date) {
    // End any existing chunk span before starting a new one
    // (handles transitions like text-delta → tool-call without text-end)
    this.#endChunkSpan(undefined, startTime);

    this.#ensureStepAndInference();

    this.#currentChunkSpan = runSpanOperation(() =>
      this.#chunkParent()?.createChildSpan({
        name: `chunk: '${chunkType}'`,
        type: SpanType.MODEL_CHUNK,
        attributes: {
          chunkType,
          sequenceNumber: this.#chunkSequence,
        },
        startTime,
        tracingPolicy: this.#modelSpan?.tracingPolicy,
      }),
    );
    this.#currentChunkType = chunkType;
    this.#accumulator = initialData || {};
  }

  /**
   * Append string content to a specific field in the accumulator
   */
  #appendToAccumulator(field: string, text: string) {
    if (this.#accumulator[field] === undefined) {
      this.#accumulator[field] = text;
    } else {
      this.#accumulator[field] += text;
    }
  }

  /**
   * End the current chunk span.
   * Safe to call multiple times - will no-op if span already ended.
   */
  #endChunkSpan(output?: any, endTime?: Date) {
    const chunkSpan = this.#currentChunkSpan;
    if (!chunkSpan) return;
    const spanOutput = output !== undefined ? output : this.#accumulator;
    this.#currentChunkSpan = undefined;
    this.#currentChunkType = undefined;
    this.#accumulator = {};
    this.#chunkSequence++;
    runSpanOperation(() => chunkSpan.end({ output: spanOutput, endTime }));
  }

  /**
   * Create an event span (for single chunks like tool-call)
   */
  #createEventSpan(
    chunkType: string,
    output: any,
    options?: { attributes?: Record<string, any>; metadata?: Record<string, any>; startTime?: Date },
  ) {
    this.#ensureStepAndInference();

    const span = runSpanOperation(() =>
      this.#chunkParent()?.createEventSpan({
        name: `chunk: '${chunkType}'`,
        type: SpanType.MODEL_CHUNK,
        attributes: {
          chunkType,
          sequenceNumber: this.#chunkSequence,
          ...options?.attributes,
        },
        metadata: options?.metadata,
        output,
        startTime: options?.startTime,
        tracingPolicy: this.#modelSpan?.tracingPolicy,
      }),
    );

    if (span) {
      this.#chunkSequence++;
    }
  }

  /**
   * Check if there is currently an active chunk span
   */
  #hasActiveChunkSpan(): boolean {
    return !!this.#currentChunkSpan;
  }

  /**
   * Get the current accumulator value
   */
  #getAccumulator(): Record<string, any> {
    return this.#accumulator;
  }

  /**
   * Handle text chunk spans (text-start/delta/end)
   */
  #handleTextChunk<OUTPUT>(chunk: ChunkType<OUTPUT>, observedAt?: Date) {
    switch (chunk.type) {
      case 'text-start':
        this.#startChunkSpan('text', undefined, observedAt);
        break;

      case 'text-delta':
        // Auto-create span if we receive text-delta without text-start
        // (AI SDK streaming doesn't always emit wrapper events)
        // Allow transition from any other chunk type
        if (this.#currentChunkType !== 'text') {
          this.#startChunkSpan('text', undefined, observedAt);
        }
        this.#appendToAccumulator('text', chunk.payload.text);
        break;

      case 'text-end': {
        this.#endChunkSpan(undefined, observedAt);
        break;
      }
    }
  }

  /**
   * Handle reasoning chunk spans (reasoning-start/delta/end)
   */
  #handleReasoningChunk<OUTPUT>(chunk: ChunkType<OUTPUT>, observedAt?: Date) {
    switch (chunk.type) {
      case 'reasoning-start':
        this.#startChunkSpan('reasoning', undefined, observedAt);
        break;

      case 'reasoning-delta':
        // Auto-create span if we receive reasoning-delta without reasoning-start
        // (AI SDK streaming doesn't always emit wrapper events)
        // Allow transition from any other chunk type
        if (this.#currentChunkType !== 'reasoning') {
          this.#startChunkSpan('reasoning', undefined, observedAt);
        }
        this.#appendToAccumulator('text', chunk.payload.text);
        break;

      case 'reasoning-end': {
        this.#endChunkSpan(undefined, observedAt);
        break;
      }
    }
  }

  /**
   * Handle tool call chunk spans (tool-call-input-streaming-start/delta/end, tool-call)
   */
  #handleToolCallChunk<OUTPUT>(chunk: ChunkType<OUTPUT>, observedAt?: Date) {
    switch (chunk.type) {
      case 'tool-call-input-streaming-start':
        this.#startChunkSpan(
          'tool-call',
          {
            toolName: chunk.payload.toolName,
            toolCallId: chunk.payload.toolCallId,
          },
          observedAt,
        );
        break;

      case 'tool-call-delta':
        this.#appendToAccumulator('toolInput', chunk.payload.argsTextDelta);
        break;

      case 'tool-call-input-streaming-end':
      case 'tool-call': {
        // Build output with toolName, toolCallId, and parsed toolInput
        const acc = this.#getAccumulator();
        let toolInput;
        try {
          toolInput = acc.toolInput ? JSON.parse(acc.toolInput) : {};
        } catch {
          toolInput = acc.toolInput; // Keep as string if parsing fails
        }
        this.#endChunkSpan(
          {
            toolName: acc.toolName,
            toolCallId: acc.toolCallId,
            toolInput,
          },
          observedAt,
        );
        break;
      }
    }
  }

  /**
   * Handle object chunk spans (object, object-result)
   */
  #handleObjectChunk<OUTPUT>(chunk: ChunkType<OUTPUT>, observedAt?: Date) {
    switch (chunk.type) {
      case 'object':
        // Start span on first partial object chunk (only if not already started)
        // Multiple object chunks may arrive as the object is being generated
        // Check specifically for object chunk type to allow transitioning from other types
        if (this.#currentChunkType !== 'object') {
          this.#startChunkSpan('object', undefined, observedAt);
        }
        break;

      case 'object-result':
        // End the span with the final complete object as output
        this.#endChunkSpan(chunk.object, observedAt);
        break;
    }
  }

  /**
   * Handle tool-call-approval chunks.
   * Creates a span for approval requests so they can be seen in traces for debugging.
   */
  #handleToolApprovalChunk<OUTPUT>(chunk: ChunkType<OUTPUT>, observedAt?: Date) {
    if (chunk.type !== 'tool-call-approval') return;
    const payload = chunk.payload;

    this.#ensureStepAndInference();

    // Create an event span for the approval request
    // Using createEventSpan since approvals are point-in-time events (not time ranges)
    const span = runSpanOperation(() =>
      this.#chunkParent()?.createEventSpan({
        name: `chunk: 'tool-call-approval'`,
        type: SpanType.MODEL_CHUNK,
        attributes: {
          chunkType: 'tool-call-approval',
          sequenceNumber: this.#chunkSequence,
        },
        output: payload,
        startTime: observedAt,
        tracingPolicy: this.#modelSpan?.tracingPolicy,
      }),
    );

    if (span) {
      this.#chunkSequence++;
    }
  }
  /**
   * Observe the outer workflow boundary without reprocessing provider chunks.
   * Regular agents use this wrapper so MODEL_STEP remains live through output
   * processors and client tools while the inner provider wrapper owns precise
   * MODEL_INFERENCE / MODEL_CHUNK timing.
   */
  wrapStepStream<T extends { pipeThrough: Function }>(stream: T): T {
    return stream.pipeThrough(
      new TransformStream({
        transform: (chunk, controller) => {
          runSpanOperation(() => {
            switch (chunk.type) {
              case 'step-finish':
                this.#endStepSpan(chunk.payload);
                break;
              case 'abort':
                this.endStep({ attributes: { finishReason: 'abort', isContinued: false } });
                break;
              case 'tripwire':
                this.endStep({ attributes: { finishReason: 'tripwire', isContinued: false } });
                break;
            }
          });
          controller.enqueue(chunk);
        },
      }),
    ) as T;
  }

  /**
   * Wraps a stream with model tracing transform to track MODEL_STEP and MODEL_CHUNK spans.
   *
   * This should be added to the stream pipeline to automatically
   * create MODEL_STEP and MODEL_CHUNK spans for each semantic unit in the stream.
   */
  wrapStream<T extends { pipeThrough: Function }>(stream: T): T {
    return stream.pipeThrough(
      new TransformStream({
        transform: (chunk, controller) => {
          const providerObservedAt = this.#takeProviderChunkTimestamp(chunk);
          // Capture completion start time on first actual content (for time-to-first-token)
          runSpanOperation(() => {
            switch (chunk.type) {
              case 'text-delta':
              case 'tool-call-delta':
              case 'tool-call':
              case 'reasoning-delta':
              case 'object':
              case 'object-result':
                this.#captureCompletionStartTime();
                break;
            }
          });

          // Handle chunk span tracking based on chunk type
          runSpanOperation(() => {
            switch (chunk.type) {
              case 'text-start':
              case 'text-delta':
              case 'text-end':
                this.#handleTextChunk(chunk, providerObservedAt);
                break;

              case 'tool-call-input-streaming-start':
              case 'tool-call-delta':
              case 'tool-call-input-streaming-end':
              case 'tool-call':
                this.#handleToolCallChunk(chunk, providerObservedAt);
                break;

              case 'reasoning-start':
              case 'reasoning-delta':
              case 'reasoning-end':
                this.#handleReasoningChunk(chunk, providerObservedAt);
                break;

              case 'object':
              case 'object-result':
                this.#handleObjectChunk(chunk, providerObservedAt);
                break;

              case 'step-start':
                // If step already started (via startStep()), just update with payload data
                // Otherwise start a new step (for backwards compatibility)
                if (this.#currentStepSpan) {
                  this.updateStep(chunk.payload);
                } else {
                  this.startStep(chunk.payload);
                }
                // step-start fires when the provider stream has begun. Open the
                // inference span here as a safety net for callers that don't
                // explicitly call startInference() before invoking the model —
                // chunks that follow will parent under MODEL_INFERENCE.
                if (!this.#currentInferenceSpan) {
                  this.startInference(chunk.payload);
                }
                break;

              case 'step-finish':
                if (this.#deferStepClose) {
                  // Durable mode: save payload for later, don't close the step.
                  // Close MODEL_INFERENCE eagerly though - the provider stream is
                  // done, and any subsequent tool execution under the step should
                  // not inflate inference duration.
                  this.#pendingStepFinishPayload = chunk.payload;
                  this.#endChunkSpan();
                  this.#endInferenceSpan(chunk.payload);
                  if (this.#deferredStepCloseRequested) {
                    this.#endStepSpan(chunk.payload);
                  }
                } else {
                  // Normal mode: close the step immediately
                  this.#endStepSpan(chunk.payload);
                }
                break;

              case 'finish':
                if (this.#currentInferenceLifecycleOpen) {
                  this.recordInferenceFinish(chunk.payload, providerObservedAt);
                  this.#endChunkSpan(undefined, providerObservedAt);
                }
                break;

              case 'response-metadata': // Response metadata (not semantic content)
                this.#recordInferenceResponse(chunk.payload);
                break;
              case 'abort': // Terminal abort may arrive without step-finish
                this.endStep({ attributes: { finishReason: 'abort', isContinued: false } });
                break;

              case 'tripwire': // Terminal processor bail may arrive without step-finish
                this.endStep({ attributes: { finishReason: 'tripwire', isContinued: false } });
                break;

              // Infrastructure chunks - skip creating spans for these
              // They are either redundant, metadata-only, or error/control flow
              case 'raw': // Redundant raw data
              case 'start': // Stream start marker
              case 'source': // Source references (metadata)
              case 'file': // Binary file data (too large/not semantic)
              case 'error': // Error handling
              case 'watch': // Internal watch event
              case 'tool-error': // Tool error handling
              case 'tool-call-suspended': // Suspension (not content)
              case 'reasoning-signature': // Signature metadata
              case 'redacted-reasoning': // Redacted content metadata
              case 'step-output': // Step output wrapper (content is nested)
                // Don't create spans for these chunks
                break;

              case 'tool-call-approval': // Approval request - create span for debugging
                this.#handleToolApprovalChunk(chunk, providerObservedAt);
                break;

              case 'tool-output':
                // tool-output chunks are streaming progress from tools (e.g., sub-agents)
                // No span created - the final tool-result event captures the result
                break;

              case 'tool-result': {
                // tool-result is always a point-in-time event span
                // (tool execution duration is captured by the parent tool_call span)
                const {
                  // Metadata - tool call context (unique to tool-result chunks)
                  toolCallId,
                  toolName,
                  isError,
                  dynamic,
                  providerExecuted,
                  providerMetadata,
                  // Keep provider-executed results on MODEL_CHUNK because they come
                  // from the model/provider stream and may not have a sibling TOOL_CALL span.
                  // For locally executed tools, the canonical payload lives on TOOL_CALL.
                  result,
                  // Stripped - redundant (already on TOOL_CALL span input)
                  args: _args,
                } = (chunk.payload as Record<string, any>) || {};

                // All tool-result specific fields go in metadata
                const metadata: Record<string, any> = { toolCallId, toolName };
                if (isError !== undefined) metadata.isError = isError;
                if (dynamic !== undefined) metadata.dynamic = dynamic;
                if (providerExecuted !== undefined) metadata.providerExecuted = providerExecuted;
                if (providerMetadata !== undefined) metadata.providerMetadata = providerMetadata;

                const mastraMeta = providerMetadata?.mastra as Record<string, unknown> | undefined;
                const spanOutput = providerExecuted
                  ? result
                  : mastraMeta?.modelOutput !== undefined
                    ? mastraMeta.modelOutput
                    : undefined;

                this.#createEventSpan(chunk.type, spanOutput, { metadata, startTime: providerObservedAt });
                break;
              }

              // Default: skip creating spans for unrecognized chunk types
              // All semantic content chunks should be explicitly handled above
              // Unknown chunks are likely infrastructure or custom chunks that don't need tracing
              default:
                // No span created - reduces trace noise
                break;
            }
          });

          // Observe the provider boundary before releasing the chunk to the
          // workflow queue. A fast provider and slow consumer must not let a
          // later finish close MODEL_INFERENCE before its semantic chunks (or
          // terminal abort) have been attributed.
          controller.enqueue(chunk);
        },
      }),
    ) as T;
  }
}
