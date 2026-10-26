import { EventEmitter } from 'node:events';
import { ReadableStream, TransformStream } from 'node:stream/web';
import type { TransformStreamDefaultController } from 'node:stream/web';
import {
  convertMessages,
  coreContentToString,
  countResponseTerminalToolResultParts,
  createTerminalToolResultPartId,
  findRetainedTerminalToolResultPart,
  materializeTerminalToolResult,
} from '../../agent/message-list';
import type { MessageList, MastraDBMessage } from '../../agent/message-list';
import { TripWire } from '../../agent/trip-wire';
import { MastraBase } from '../../base';
import { ErrorCategory, ErrorDomain, MastraError } from '../../error';
import { getErrorFromUnknown } from '../../error/utils.js';
import type { ScorerRunInputForAgent, ScorerRunOutputForAgent } from '../../evals';
import { stepWasRolledBack } from '../../loop/shared/step-content';
import type { ObservabilityContext } from '../../observability';
import { getRootExportSpan, resolveObservabilityContext } from '../../observability';
import type { OutputResult, ProcessorStreamWriter } from '../../processors';
// Inlined to avoid importing structured-output.ts which pulls in the agent
// barrel and creates an ESM init-time cycle.
const STRUCTURED_OUTPUT_PROCESSOR_NAME = 'structured-output';
import {
  ProcessorState,
  ProcessorRunner,
  outputProcessorsSupportResult,
  outputProcessorsSupportStream,
} from '../../processors/runner';
import { isFailedStreamChunk } from '../../processors/stream-reprocess';
import type { TerminalToolResult } from '../../tools';
import type { WorkflowRunStatus } from '../../workflows';
import { DelayedPromise, consumeStream } from '../aisdk/v5/compat';
import type { ConsumeStreamOptions } from '../aisdk/v5/compat';
import { addPendingStepUsage, toPendingStepUsage } from '../pending-step-usage';
import type { PendingStepUsage } from '../pending-step-usage';
import { isSignalChunkExcluded } from '../signal-exclusions';
import type {
  ChunkType,
  LanguageModelUsage,
  LLMStepResult,
  MastraModelOutputOptions,
  MastraOnFinishCallbackArgs,
  ProviderMetadata,
  StreamTransport,
  StepTripwireData,
  ToolCallChunk,
} from '../types';
import { isDataChunk } from '../types';
import { safeClose, safeEnqueue } from './input';
import { createJsonTextStreamTransformer, createObjectStreamTransformer } from './output-format-handlers';
import { isChunkOutputProcessed } from './output-processed';
import { getChunkProducedAt, stampChunkProducedAt } from './produced-at';
import { getTransformedSchema } from './schema';
import { packStepMessageMirrors, unpackStepMessageMirrors } from './step-message-mirrors';
import { dedupeStepRequests, rehydrateStepRequests } from './step-request-dedupe';

const primaryUsageCountKeys = ['inputTokens', 'outputTokens', 'totalTokens'] as const satisfies ReadonlyArray<
  keyof LanguageModelUsage
>;
const detailUsageCountKeys = [
  'reasoningTokens',
  'cachedInputTokens',
  'cacheCreationInputTokens',
  'cacheCreationInputTokens5m',
  'cacheCreationInputTokens1h',
] as const satisfies ReadonlyArray<keyof LanguageModelUsage>;
const usageCountKeys = [...primaryUsageCountKeys, ...detailUsageCountKeys] as const;

function accumulateUsage(
  target: Partial<LanguageModelUsage>,
  missing: Set<(typeof primaryUsageCountKeys)[number]>,
  usage: Partial<LanguageModelUsage>,
) {
  // Primary totals describe the whole request, so any omitted contribution
  // makes that aggregate incomplete. Explicit zeroes remain valid values.
  for (const key of primaryUsageCountKeys) {
    const value = usage[key];
    if (value === undefined) {
      missing.add(key);
      target[key] = undefined;
    } else if (!missing.has(key)) {
      target[key] = (target[key] ?? 0) + value;
    }
  }

  // Detail counters are present-when-reported and remain additive across
  // providers that omit unsupported cache or reasoning measurements.
  for (const key of detailUsageCountKeys) {
    const value = usage[key];
    if (value !== undefined) {
      target[key] = (target[key] ?? 0) + value;
    }
  }

  // raw is provider-specific and not summable; keep the latest step's raw
  if (usage.raw !== undefined) {
    target.raw = usage.raw;
  }
}

/**
 * Helper function to create a destructurable version of MastraModelOutput.
 * This wraps the output to ensure properties maintain their context when destructured.
 */
export function createDestructurableOutput<OUTPUT = undefined>(
  output: MastraModelOutput<OUTPUT>,
): MastraModelOutput<OUTPUT> {
  return new Proxy(output, {
    get(target, prop, _receiver) {
      // Use target as receiver to preserve private member access
      const originalValue = Reflect.get(target, prop, target);

      // For methods, return bound version
      if (typeof originalValue === 'function') {
        return originalValue.bind(target);
      }

      // For everything else (including getters), return as-is
      return originalValue;
    },
  }) as MastraModelOutput<OUTPUT>;
}

/**
 * Persist a non-transient `data-*` chunk emitted by an output processor's
 * `writer.custom()` onto the assistant message, so the chunk survives in
 * thread history instead of being stream-only (#19375). Transient chunks
 * and non-data chunks are left untouched.
 *
 * Exported for the durable engine, whose producer-side processor writers
 * persist into the workflow-side MessageList (the one serialized into
 * `messageListState` and flushed to memory at finalize).
 */
export function persistProcessorDataChunk(
  messageList: MessageList,
  messageId: string,
  chunk: { type: string; data?: unknown; transient?: boolean },
): void {
  if (!isDataChunk(chunk) || chunk.transient) return;

  const message: MastraDBMessage = {
    id: messageId,
    role: 'assistant',
    content: {
      format: 2,
      parts: [{ type: chunk.type as `data-${string}`, data: chunk.data }],
    },
    createdAt: new Date(),
  };
  messageList.add(message, 'response');
}

type ToolCallStreamingMeta = {
  toolName: string;
  providerExecuted?: boolean;
  providerMetadata?: ProviderMetadata;
  dynamic?: boolean;
  title?: string;
  observability?: ToolCallChunk['payload']['observability'];
};

type PromiseResults<OUTPUT = undefined> = Pick<
  LLMStepResult<OUTPUT>,
  | 'text'
  | 'reasoning'
  | 'sources'
  | 'files'
  | 'toolCalls'
  | 'toolResults'
  | 'content'
  | 'usage'
  | 'warnings'
  | 'providerMetadata'
  | 'response'
  | 'request'
> & {
  suspendPayload: any;
  resumeSchema: any;
  object: OUTPUT;
  reasoningText: string | undefined;
  totalUsage: LLMStepResult<OUTPUT>['usage'];
  steps: LLMStepResult<OUTPUT>[];
  finishReason: LLMStepResult<OUTPUT>['finishReason'];
};

type DelayedPromises<OUTPUT = undefined> = {
  [K in keyof PromiseResults<OUTPUT>]: DelayedPromise<PromiseResults<OUTPUT>[K]>;
};

/**
 * @internal Rejection a provider-result listener uses when the run's turn was
 * cancelled while the listener ran: the run then ends cancelled, not failed.
 * Any other rejection, including a driver's own AbortError, fails the run.
 */
export class ProviderResultCancelledError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = 'ProviderResultCancelledError';
  }
}

/**
 * The complete output returned by `getFullOutput()`.
 */
export type FullOutput<OUTPUT = undefined> = {
  /** The text output from all steps, excluding rejected responses */
  text: string;
  /** Token usage for the last step */
  usage: PromiseResults<OUTPUT>['usage'];
  /** All LLM steps executed during the stream */
  steps: LLMStepResult<OUTPUT>[];
  /** The reason the stream finished */
  finishReason: PromiseResults<OUTPUT>['finishReason'];
  /** Any warnings from the model */
  warnings: PromiseResults<OUTPUT>['warnings'];
  /** Provider-specific metadata */
  providerMetadata: PromiseResults<OUTPUT>['providerMetadata'];
  /** The request that was sent to model */
  request: PromiseResults<OUTPUT>['request'];
  /** Reasoning details from the model */
  reasoning: PromiseResults<OUTPUT>['reasoning'];
  /** Combined reasoning text */
  reasoningText: string | undefined;
  /** Tool calls made during execution */
  toolCalls: PromiseResults<OUTPUT>['toolCalls'];
  /** Results from tool executions */
  toolResults: PromiseResults<OUTPUT>['toolResults'];
  /** Sources referenced by model */
  sources: PromiseResults<OUTPUT>['sources'];
  /** Files generated by the model */
  files: PromiseResults<OUTPUT>['files'];
  /** Response metadata from the model */
  response: PromiseResults<OUTPUT>['response'];
  /** Total token usage across all steps */
  totalUsage: PromiseResults<OUTPUT>['totalUsage'];
  /** The structured object output (when using structured output) */
  object: OUTPUT;
  /**
   * True when `object` is the configured `fallbackValue`, substituted because the model
   * output failed schema validation — or the separate structuring model failed — under
   * `errorStrategy: 'fallback'`.
   */
  usedFallbackValue: boolean;
  /** Error if the stream failed */
  error: Error | undefined;
  /** Tripwire data if content was blocked */
  tripwire: StepTripwireData | undefined;
  /** Scoring data for evals (when returnScorerData is enabled) */
  scoringData?: {
    input: Omit<ScorerRunInputForAgent, 'runId'>;
    output: ScorerRunOutputForAgent;
  };
  /** Trace ID for this execution. */
  traceId: string | undefined;
  /** Root span ID for this execution, identifying the top-level span in the trace. */
  spanId: string | undefined;
  /** Run ID for this execution */
  runId: string | undefined;
  /** Payload for resuming suspended tool calls */
  suspendPayload: any;
  /** Resume schema of suspended step if available */
  resumeSchema?: any;
  /** All messages from this execution (input + memory history + response) */
  messages: MastraDBMessage[];
  /** Only messages loaded from memory (conversation history) */
  rememberedMessages: MastraDBMessage[];
  /** Bounded tool result that intentionally ended the run without another model call. */
  terminalToolResult?: TerminalToolResult;
};

/**
 * The public content of the step a `step-finish` chunk closes, taken from the loop's StepResult
 * for it (the last entry of the chunk's `output.steps`).
 *
 * A retry attempt a processor rejected and rolled back keeps its parts in the loop's StepResult
 * so later processors can see what was rejected, but its parts are gone from the response, so
 * the public step reports no content (its `text` is already empty).
 *
 * @returns the content, or `undefined` when the chunk carries no StepResult with content.
 */
function stepContentFromLoop(
  step: { readonly content?: LLMStepResult<unknown>['content']; readonly tripwire?: StepTripwireData } | undefined,
): LLMStepResult<unknown>['content'] | undefined {
  if (!step?.content) return undefined;
  return stepWasRolledBack(step) ? [] : [...step.content];
}

/**
 * How many loop StepResults restored public steps came from, for a state saved before
 * `loopStepCount` was kept. Every public step comes from one loop step, except an attempt retried
 * after an API error: its step is `retry` and has a tripwire only when it inherits the previous
 * loop step's. A processor retry is also `retry` but always has its own tripwire, so a `retry`
 * step with a tripwire is counted. Only an API-error retry right after a processor retry is
 * overcounted, and an overcount sends a later step to the marker count, never to an earlier
 * step's content.
 */
function countLoopSteps(steps: ReadonlyArray<{ finishReason?: unknown; tripwire?: unknown }>): number {
  return steps.filter(step => step.finishReason !== 'retry' || step.tripwire !== undefined).length;
}

/**
 * Resolve the output text from the latest response message, skipping internal
 * completion-check feedback so it can't become the final text.
 * The completionResult metadata only exists on DB-format messages, and the
 * message is converted alone so adjacent assistant messages aren't merged.
 *
 * Converting to model messages splits an assistant message at every tool
 * result, so when the current loop iteration called a tool the last converted
 * message holds only the text after the call, or nothing if the step ended on
 * it. In that case the text is read from the DB message's parts after the
 * iteration's boundary instead. The last `step-start` is not that boundary:
 * one is also inserted inside a single response whenever text follows a tool call.
 *
 * Returns `undefined` only when there is no response message to read text from,
 * so callers can distinguish "no processed output exists" from an output
 * processor deliberately clearing the text to `''`. Never collapse the two with
 * a truthiness check: a redacting processor must be able to produce empty text.
 */
function resolveOutputMessageSkippingCompletionChecks(messageList: MessageList): MastraDBMessage | undefined {
  const responseDbMessages = messageList.get.response.db();
  return responseDbMessages.some(m => m.content?.metadata?.completionResult)
    ? responseDbMessages.findLast(m => !m.content?.metadata?.completionResult)
    : responseDbMessages[responseDbMessages.length - 1];
}

function resolveOutputTextSkippingCompletionChecks(messageList: MessageList): string | undefined {
  const lastRealMessage = resolveOutputMessageSkippingCompletionChecks(messageList);
  if (!lastRealMessage) return undefined;
  if (lastRealMessage.role === 'assistant' && lastRealMessage.content?.parts) {
    const stepParts = messageList.partsSinceStepBoundary(lastRealMessage);
    if (stepParts.some(p => p.type === 'tool-invocation')) {
      return stepParts.map(p => (p.type === 'text' ? p.text : '')).join('');
    }
  }
  const hasCompletionCheckMessages = messageList.get.response.db().some(m => m.content?.metadata?.completionResult);
  const converted = hasCompletionCheckMessages
    ? convertMessages([lastRealMessage]).to('AIV4.Core')
    : messageList.get.response.aiV4.core();
  const lastConverted = converted[converted.length - 1];
  return lastConverted ? coreContentToString(lastConverted.content) : undefined;
}

/**
 * A single entry of a stored message's part array.
 */
type StepMessagePart = MastraDBMessage['content']['parts'][number];

/**
 * Step text for the current iteration of the message that supplies the resolved output text.
 *
 * The iteration boundary is a property of a message's own part array, so the offset measured before
 * processing must not be transplanted onto a message a processor produced. Resolve the boundary in
 * this order:
 *   1. the message's own `step-start` marker, when it has one;
 *   2. for the same message, the pre-processing step text where it survives (an unchanged in-place
 *      rewrite, or a split of the current iteration's text), the pre-processing offset while the
 *      parts before it are unchanged, then the offset while the part count and part kinds are
 *      unchanged (redaction or other rewrites that keep positions), then the unchanged prefix;
 *   3. for a different message, the trailing text matching the pre-processing step text, which finds
 *      the boundary when a processor replaced the id of the accumulated response;
 *   4. otherwise the whole message, which then holds only this iteration.
 *
 * The rules below are heuristics over the processed parts, so a processor can defeat them. When it
 * rewrites the current iteration's text (so the pre-processing step text no longer appears) *and*
 * moves the boundary while keeping the part count and kinds (for example by merging an earlier
 * iteration's parts and splitting the current one), the measured offset still matches by count and
 * kind and the step text is taken from the wrong position. That information is gone from the parts —
 * only a processor that preserves the boundary (a `step-start` part, or positions it does not move)
 * can be resolved exactly.
 */
function resolveProcessedStepText({
  processedStepMessage,
  processedStepParts,
  originalMessageId,
  originalIterationPartOffset,
  originalPartCount,
  originalParts,
  originalStepText,
  outputText,
}: {
  processedStepMessage: MastraDBMessage | undefined;
  processedStepParts: StepMessagePart[] | undefined;
  originalMessageId: string | undefined;
  originalIterationPartOffset: number | undefined;
  originalPartCount: number | undefined;
  originalParts: StepMessagePart[] | undefined;
  originalStepText: string | undefined;
  outputText: string | undefined;
}): string {
  if (!processedStepMessage || !processedStepParts) return outputText ?? '';

  const textWithin = (parts: StepMessagePart[]) => parts.map(part => (part.type === 'text' ? part.text : '')).join('');
  const boundaryIndex = processedStepParts.findLastIndex(part => part.type === 'step-start');
  if (boundaryIndex !== -1) return textWithin(processedStepParts.slice(boundaryIndex + 1));

  if (processedStepMessage.id === originalMessageId && originalParts) {
    const sliceFrom = (index: number) => textWithin(processedStepParts.slice(index));
    const offset = originalIterationPartOffset;
    const textOffset = findIterationOffsetInParts(processedStepParts, originalStepText);
    if (textOffset !== undefined) return sliceFrom(textOffset);

    if (
      offset !== undefined &&
      offset > 0 &&
      processedStepParts.length >= offset &&
      originalParts.slice(0, offset).every((part, index) => partMatches(part, processedStepParts[index]))
    ) {
      return sliceFrom(offset);
    }

    if (
      offset !== undefined &&
      originalPartCount !== undefined &&
      processedStepParts.length === originalPartCount &&
      originalParts.slice(0, offset).every((part, index) => part?.type === processedStepParts[index]?.type)
    ) {
      return sliceFrom(offset);
    }

    let common = 0;
    while (
      common < originalParts.length &&
      common < processedStepParts.length &&
      partMatches(originalParts[common], processedStepParts[common])
    ) {
      common++;
    }
    if (common > 0) return sliceFrom(common);

    return textWithin(processedStepParts);
  }

  const offset = findIterationOffsetInParts(processedStepParts, originalStepText);
  return offset !== undefined ? textWithin(processedStepParts.slice(offset)) : textWithin(processedStepParts);
}

function partMatches(a: StepMessagePart | undefined, b: StepMessagePart | undefined): boolean {
  if (!a || !b || a.type !== b.type) return false;
  if (a.type === 'text') return a.text === (b as typeof a).text;
  if (a.type === 'reasoning') return a.reasoning === (b as typeof a).reasoning;
  if (a.type === 'tool-invocation') {
    return a.toolInvocation?.toolCallId === (b as typeof a).toolInvocation?.toolCallId;
  }
  if (a.type === 'step-start') return Number(a.createdAt) === Number((b as typeof a).createdAt);
  return true;
}

function findIterationOffsetInParts(parts: StepMessagePart[], stepText: string | undefined): number | undefined {
  if (!stepText) return undefined;

  let suffixText = '';
  for (let index = parts.length - 1; index >= 0; index--) {
    const part = parts[index];
    if (part?.type === 'text') suffixText = part.text + suffixText;
    if (suffixText === stepText) return index;
    if (!stepText.endsWith(suffixText)) return undefined;
  }
  return undefined;
}

function findIterationPartOffset(
  messageList: MessageList,
  message: MastraDBMessage,
  stepText: string,
): number | undefined {
  const parts = message.content?.parts;
  if (!parts) return undefined;

  const stepParts = messageList.partsSinceStepBoundary(message);
  if (stepParts.length < parts.length) return parts.length - stepParts.length;
  if (!stepText) return undefined;

  let suffixText = '';
  for (let index = parts.length - 1; index >= 0; index--) {
    const part = parts[index];
    if (part?.type === 'text') suffixText = part.text + suffixText;
    if (suffixText === stepText) return index;
    if (!stepText.endsWith(suffixText)) return undefined;
  }

  return undefined;
}

function resolveMessageText(message: MastraDBMessage): string {
  const converted = convertMessages([message]).to('AIV4.Core');
  const convertedMessage = converted[converted.length - 1];
  return convertedMessage ? coreContentToString(convertedMessage.content) : '';
}

// Enumerable so processor object spreads preserve it; removed before the split response is coalesced.
const stepPartIndex = Symbol('stepPartIndex');

type StepIndexedPart = {
  [stepPartIndex]?: number;
};

type StepPartBoundaries = {
  offsets: number[];
};

function findStepPartBoundaries<OUTPUT>(
  message: MastraDBMessage | undefined,
  steps: LLMStepResult<OUTPUT>[],
): StepPartBoundaries | undefined {
  const parts = message?.content.parts;
  if (!parts) return undefined;

  const offsets = Array<number>(steps.length + 1);
  offsets[steps.length] = parts.length;
  let partIndex = parts.length;

  for (let stepIndex = steps.length - 1; stepIndex >= 0; stepIndex--) {
    const stepText = steps[stepIndex]!.text;
    let text = '';

    while (partIndex > 0 && text !== stepText) {
      const part = parts[--partIndex]!;
      if (part.type === 'text') text = `${part.text}${text}`;
    }

    if (text !== stepText) return undefined;
    offsets[stepIndex] = partIndex;
  }

  for (const [stepIndex] of steps.entries()) {
    for (const part of parts.slice(offsets[stepIndex], offsets[stepIndex + 1])) {
      if (part.type === 'text') (part as typeof part & StepIndexedPart)[stepPartIndex] = stepIndex;
    }
  }

  return { offsets };
}

function clearStepPartIndexes(messages: MastraDBMessage[]): void {
  for (const message of messages) {
    for (const part of message.content.parts) delete (part as typeof part & StepIndexedPart)[stepPartIndex];
  }
}

type EarlierStepMessageGroup<OUTPUT> = {
  messageId: string;
  steps: LLMStepResult<OUTPUT>[];
  originalText: string;
  boundaries?: StepPartBoundaries;
};

/**
 * Fork: every feedback continuation writes its own response message (the finished iteration's
 * message is sealed, issue #19445), so upstream's single-message split never applies. Pair each
 * earlier assistant message with the buffered steps whose text it holds so the result
 * processors' rewrite of that message reconciles those steps (upstream #26346/#26543 intent).
 * Returns undefined when the steps cannot be attributed unambiguously.
 */
function groupEarlierStepsByMessage<OUTPUT>(
  messages: MastraDBMessage[],
  steps: LLMStepResult<OUTPUT>[],
): EarlierStepMessageGroup<OUTPUT>[] | undefined {
  const groups: EarlierStepMessageGroup<OUTPUT>[] = [];
  let stepIndex = 0;
  for (const message of messages) {
    const originalText = resolveMessageText(message);
    const groupSteps: LLMStepResult<OUTPUT>[] = [];
    let text = '';
    while (stepIndex < steps.length && text !== originalText) {
      const step = steps[stepIndex++]!;
      groupSteps.push(step);
      text += step.text;
      if (!originalText.startsWith(text)) return undefined;
    }
    if (text !== originalText) return undefined;
    groups.push({ messageId: message.id, steps: groupSteps, originalText });
  }
  // Text-less steps after the last earlier message (tool-only iterations) keep their text.
  if (steps.slice(stepIndex).some(step => step.text)) return undefined;
  for (const group of groups) {
    group.boundaries = findStepPartBoundaries(
      messages.find(message => message.id === group.messageId),
      group.steps,
    );
  }
  return groups;
}

function reconcileBufferedStepTexts<OUTPUT>(
  steps: LLMStepResult<OUTPUT>[],
  processedText: string,
  processedMessage?: MastraDBMessage,
  stepPartBoundaries?: StepPartBoundaries,
  originalText?: string,
): void {
  if (processedText === originalText) return;

  const processedParts = processedMessage?.content.parts;
  let processedOffsets: number[] | undefined;

  if (processedParts && stepPartBoundaries) {
    const processedStepTexts = steps.map(() => '');
    let lastStepIndex = 0;
    let hasUnknownPart = false;

    for (const part of processedParts) {
      if (part.type !== 'text') continue;
      const stepIndex = (part as typeof part & StepIndexedPart)[stepPartIndex];
      if (stepIndex === undefined || stepIndex < lastStepIndex) {
        hasUnknownPart = true;
        break;
      }
      processedStepTexts[stepIndex] += part.text;
      lastStepIndex = stepIndex;
    }

    if (!hasUnknownPart) {
      for (const [index, step] of steps.entries()) step.text = processedStepTexts[index]!;
      return;
    }

    if (stepPartBoundaries.offsets.at(-1) === processedParts.length) {
      processedOffsets = stepPartBoundaries.offsets;
    }
  }

  if (processedParts && processedOffsets) {
    for (const [index, step] of steps.entries()) {
      step.text = processedParts
        .slice(processedOffsets[index], processedOffsets[index + 1])
        .map(part => (part.type === 'text' ? part.text : ''))
        .join('');
    }
    return;
  }

  let offset = 0;
  let index = 0;

  while (index < steps.length && processedText.startsWith(steps[index]!.text, offset)) {
    offset += steps[index]!.text.length;
    index++;
  }

  if (index === steps.length) return;

  steps[index]!.text = processedText.slice(offset);
  for (const step of steps.slice(index + 1)) step.text = '';
}

export class MastraModelOutput<OUTPUT = undefined> extends MastraBase {
  #status: WorkflowRunStatus = 'running';
  #error: Error | undefined;
  #baseStream: ReadableStream<ChunkType<OUTPUT>>;
  #bufferedChunks: ChunkType<OUTPUT>[] = [];
  /**
   * Set when a continuing goal evaluation arrives while a step is still in
   * flight (in-process engines emit the goal chunk before the judged turn's
   * step-finish): the next step-finish belongs to the judged turn and its
   * buffers are dropped once its stepResult and usage are recorded.
   */
  #truncateAtNextStepFinish = false;
  #streamFinished = false;
  #finishCallbackSent = false;
  #emitter = new EventEmitter();
  #bufferedSteps: LLMStepResult<OUTPUT>[] = [];
  #bufferedReasoningDetails: Record<string, LLMStepResult<OUTPUT>['reasoning'][number]> = {};
  /**
   * Per-step counterpart of `#bufferedReasoningDetails`. Reset on `step-finish`
   * alongside `#bufferedByStep` so a step result reports only its own reasoning.
   */
  #bufferedByStepReasoningDetails: Record<string, LLMStepResult<OUTPUT>['reasoning'][number]> = {};
  #bufferedByStep: LLMStepResult<OUTPUT> = {
    text: '',
    reasoning: [],
    sources: [],
    files: [],
    toolCalls: [],
    toolResults: [],
    dynamicToolCalls: [],
    dynamicToolResults: [],
    staticToolCalls: [],
    staticToolResults: [],
    content: [],
    usage: { inputTokens: undefined, outputTokens: undefined, totalTokens: undefined },
    warnings: [],
    request: {},
    response: {
      id: '',
      timestamp: new Date(),
      modelId: '',
      messages: [],
      uiMessages: [],
    },
    reasoningText: '',
    providerMetadata: undefined,
    finishReason: undefined,
  };
  #bufferedText: LLMStepResult<OUTPUT>['text'][] = [];
  /** Length of `#bufferedText` when the current step began; rejected or retried steps are cut back to it. */
  #bufferedTextAtStepStart = 0;
  #bufferedObject: OUTPUT | undefined;
  #usedFallbackValue = false;
  #bufferedTextChunks: Record<string, LLMStepResult<OUTPUT>['text'][]> = {};
  #bufferedSources: LLMStepResult<OUTPUT>['sources'] = [];
  #bufferedReasoning: LLMStepResult<OUTPUT>['reasoning'] = [];
  /** Run-level reasoning as of the last `step-start` that has no `step-finish` yet. */
  #reasoningAtStepStart?: {
    length: number;
    details: Record<string, LLMStepResult<OUTPUT>['reasoning'][number]>;
  };
  #bufferedFiles: LLMStepResult<OUTPUT>['files'] = [];
  // Records keyed by provider-supplied tool call IDs have no prototype, so IDs like "__proto__"
  // or "constructor" are plain keys.
  #toolCallArgsDeltas: Record<string, LLMStepResult<OUTPUT>['text'][]> = Object.create(null);
  #toolCallDeltaIdNameMap: Record<string, string> = Object.create(null);
  #toolCallStreamingMeta: Record<string, ToolCallStreamingMeta> = Object.create(null);
  /** Recorded calls whose input streams again in a retry; the retry's tool-call replaces them. */
  #replaceableToolCallIds = new Set<string>();
  /** Calls recorded since the last accepted step; only these can be replaced by a retry. */
  #attemptToolCallIds = new Set<string>();
  /**
   * Tool calls whose input stream ended without any deltas. Their args only arrive with the
   * final `tool-call` chunk, so they wait here instead of being emitted as a synthetic `{}` call.
   */
  #pendingToolCalls: Record<
    string,
    {
      meta: ToolCallStreamingMeta;
      args: Record<string, unknown>;
      streamed: boolean;
      runId: string;
      from: ToolCallChunk['from'];
    }
  > = Object.create(null);
  #toolCalls: LLMStepResult<OUTPUT>['toolCalls'] = [];
  #toolResults: LLMStepResult<OUTPUT>['toolResults'] = [];
  #warnings: LLMStepResult<OUTPUT>['warnings'] = [];
  #finishReason: LLMStepResult<OUTPUT>['finishReason'] = undefined;
  /**
   * Provider-specific metadata captured from the most recent step `finish`
   * chunk (e.g. AWS Bedrock guardrail trace under
   * `providerMetadata.bedrock.trace`). Exposed via {@link _getImmediateProviderMetadata}
   * so output-step processors can attribute content-filter blocks, for which
   * the completed-steps array is empty.
   */
  #stepProviderMetadata: ProviderMetadata | undefined = undefined;
  #request: LLMStepResult<OUTPUT>['request'] = {};
  #usageCount: LLMStepResult<OUTPUT>['usage'] = {
    inputTokens: undefined,
    outputTokens: undefined,
    totalTokens: undefined,
  };
  /**
   * Usage of the model step whose tool calls suspended the run. That step's
   * `step-finish` is emitted only after its tools settle, i.e. on resume, so a
   * suspended snapshot would otherwise omit tokens the provider already billed
   * (PF-5349). The tool-call step records it out of band (never on a chunk), it
   * is serialized with the suspension state so a cold resume that suspends again
   * still reports it, and the step's own `step-finish` clears it. It is reported
   * on the suspended result only and never enters `#usageCount`.
   */
  #pendingSuspendedStepUsage: PendingStepUsage | undefined = undefined;
  /**
   * How many loop StepResults the `step-finish` chunks seen so far carried. The loop adds one per
   * step it completes; an attempt retried after an API error adds none, so its `step-finish`
   * carries the previous step last (PF-5399). Saved with the stream state, so a restored output
   * does not take that previous step for a new one.
   */
  #loopStepCount = 0;
  #usageCountMissing = new Set<(typeof primaryUsageCountKeys)[number]>();
  /**
   * Usage of the most recently completed step. Kept apart from `#bufferedSteps`
   * because goal truncation clears that buffer while usage keeps accumulating.
   */
  #lastStepUsage: LLMStepResult<OUTPUT>['usage'] | undefined = undefined;
  /**
   * Set when a suspension settled usage, totalUsage and finishReason early on a stream that
   * stays open across it (durable, upstream #27882); the next finish replaces them. The
   * suspended step's usage itself is the out-of-band `#pendingSuspendedStepUsage` (PF-5349).
   */
  #suspendedUsageIsProvisional = false;
  /**
   * Cumulative usage the MODEL_GENERATION spans of earlier suspended legs of this run already
   * recorded. Resumed legs report cumulative totals, so their generation span subtracts this to
   * keep token metrics counted once. Serialized with the run state.
   */
  #generationReportedUsage: LanguageModelUsage | undefined = undefined;
  #finishUsageIsTotal = false;
  #tripwire: StepTripwireData | undefined = undefined;
  #wasSuspended = false;
  /** Awaited at the run's final `finish`, before output result processors and `onFinish` (see `_onProviderResult`). */
  #providerResultListeners: Array<(result: FullOutput<OUTPUT>) => Promise<void>> = [];
  #providerResultObserved = false;
  #terminalToolResult: TerminalToolResult | undefined;
  #resumedPriorText = '';
  #transportRef: MastraModelOutputOptions<OUTPUT>['transportRef'] | undefined;
  #transportClosed = false;

  #delayedPromises: DelayedPromises<OUTPUT> = {
    suspendPayload: new DelayedPromise<PromiseResults<OUTPUT>['suspendPayload']>(),
    resumeSchema: new DelayedPromise<PromiseResults<OUTPUT>['resumeSchema']>(),
    object: new DelayedPromise<PromiseResults<OUTPUT>['object']>(),
    finishReason: new DelayedPromise<PromiseResults<OUTPUT>['finishReason']>(),
    usage: new DelayedPromise<PromiseResults<OUTPUT>['usage']>(),
    warnings: new DelayedPromise<PromiseResults<OUTPUT>['warnings']>(),
    providerMetadata: new DelayedPromise<PromiseResults<OUTPUT>['providerMetadata']>(),
    response: new DelayedPromise<PromiseResults<OUTPUT>['response']>(),
    request: new DelayedPromise<PromiseResults<OUTPUT>['request']>(),
    text: new DelayedPromise<PromiseResults<OUTPUT>['text']>(),
    reasoning: new DelayedPromise<PromiseResults<OUTPUT>['reasoning']>(),
    reasoningText: new DelayedPromise<string | undefined>(),
    sources: new DelayedPromise<PromiseResults<OUTPUT>['sources']>(),
    files: new DelayedPromise<PromiseResults<OUTPUT>['files']>(),
    toolCalls: new DelayedPromise<PromiseResults<OUTPUT>['toolCalls']>(),
    toolResults: new DelayedPromise<PromiseResults<OUTPUT>['toolResults']>(),
    steps: new DelayedPromise<PromiseResults<OUTPUT>['steps']>(),
    totalUsage: new DelayedPromise<PromiseResults<OUTPUT>['usage']>(),
    content: new DelayedPromise<PromiseResults<OUTPUT>['content']>(),
  };

  #consumptionStarted = false;
  #consumeStreamPromise: Promise<void> | undefined;
  #consumeStreamErrored = false;
  #consumeStreamError: unknown;
  #returnScorerData = false;
  #structuredOutputMode: 'direct' | 'processor' | undefined = undefined;

  #model: {
    modelId: string | undefined;
    provider: string | undefined;
    version: 'v2' | 'v3' | 'v4';
  };

  /**
   * Unique identifier for this execution run.
   */
  public runId: string;
  #options: MastraModelOutputOptions<OUTPUT>;
  /**
   * The processor runner for this stream.
   */
  public processorRunner?: ProcessorRunner;
  /**
   * The message list for this stream.
   */
  public messageList: MessageList;
  /**
   * Trace ID for this execution.
   */
  public traceId?: string;
  /**
   * Root span ID for this execution, identifying the top-level span in the trace.
   */
  public spanId?: string;
  public messageId: string;

  constructor({
    model: _model,
    stream,
    messageList,
    options,
    messageId,
    initialState,
    initialToolCalls,
    resumedText,
    finishUsageIsTotal,
  }: {
    model: {
      modelId: string | undefined;
      provider: string | undefined;
      version: 'v2' | 'v3' | 'v4';
    };
    stream: ReadableStream<ChunkType<OUTPUT>>;
    messageList: MessageList;
    options: MastraModelOutputOptions<OUTPUT>;
    messageId: string;
    initialState?: any;
    initialToolCalls?: ToolCallChunk[];
    /**
     * Text a resumed stream segment inherits from before its suspension: the run's text so far
     * and the suspended step's part of it. Used when no serialized `initialState` is available.
     */
    resumedText?: { priorText: string; stepText: string };
    finishUsageIsTotal?: boolean;
  }) {
    super({ component: 'LLM', name: 'MastraModelOutput' });
    if (options.logger) {
      this.__setLogger(options.logger);
    }
    this.#options = options;
    this.#finishUsageIsTotal = finishUsageIsTotal ?? false;
    this.#transportRef = options.transportRef;
    this.#returnScorerData = !!options.returnScorerData;
    this.runId = options.runId;
    const resultSpan = getRootExportSpan(options.tracingContext?.currentSpan);
    this.traceId = resultSpan?.externalTraceId;
    this.spanId = resultSpan?.id;

    this.#model = _model;

    this.messageId = messageId;

    // Determine structured output mode:
    // - 'direct': LLM generates JSON directly (no model provided), object transformers run in this stream
    // - 'processor': StructuredOutputProcessor uses internal agent with provided model
    // - undefined: No structured output
    if (options.structuredOutput?.schema) {
      this.#structuredOutputMode = options.structuredOutput.model ? 'processor' : 'direct';
    }

    // Create processor runner if outputProcessors are provided
    if (
      options.outputProcessors?.length &&
      (options.isLLMExecutionStep
        ? outputProcessorsSupportStream(options.outputProcessors)
        : outputProcessorsSupportResult(options.outputProcessors))
    ) {
      this.processorRunner = new ProcessorRunner({
        inputProcessors: [],
        outputProcessors: options.outputProcessors,
        logger: this.logger,
        agentName: 'MastraModelOutput',
        processorStates: options.processorStates,
      });
    }

    this.messageList = messageList;

    const self = this;

    // Apply output processors if they exist
    let processedStream = stream;
    const processorRunner = this.processorRunner;
    if (processorRunner?.hasOutputStreamProcessors && options.isLLMExecutionStep) {
      // Use shared processor states if provided, otherwise create new ones
      const processorStates = (options.processorStates || new Map<string, ProcessorState>()) as Map<
        string,
        ProcessorState<OUTPUT>
      >;
      let streamWriter: ProcessorStreamWriter | undefined;

      // `options` never changes for the lifetime of this stream, so the
      // resolved context is the same for every chunk. Deriving it eagerly
      // per chunk clones the span metadata for logger + metrics contexts
      // on every part, even when no stream processor consumes it.
      let observabilityContext: ObservabilityContext | undefined;
      const getObservabilityContext = () => (observabilityContext ??= resolveObservabilityContext(options));
      // Set once the model stream errors or aborts; held parts are then dropped
      // rather than released, since they belong to a failed (maybe retried) call.
      let streamFailed = false;

      processedStream = stream.pipeThrough(
        new TransformStream<ChunkType<OUTPUT>, ChunkType<OUTPUT>>({
          async transform(chunk, controller) {
            if (isFailedStreamChunk(chunk)) streamFailed = true;
            // Filter out intermediate finish chunks with 'tool-calls' reason
            // These are internal signals that shouldn't reach output processors
            //
            // Error-shaped chunks are filtered out the same way when the caller
            // opted into `deferErrorChunks`: they describe one model call that
            // may still be retried or served by a fallback model, so processors
            // must not react to them here. The caller runs processors on the
            // error once it has ruled out recovery.
            //
            // Chunks marked output-processed already ran through the
            // processors upstream, so they pass through too.
            const isDeferredErrorChunk =
              options.deferErrorChunks &&
              (chunk.type === 'error' || (chunk.type === 'finish' && chunk.payload?.stepResult?.reason === 'error'));

            if (
              (chunk.type === 'finish' && chunk.payload?.stepResult?.reason === 'tool-calls') ||
              isDeferredErrorChunk ||
              isChunkOutputProcessed(chunk)
            ) {
              controller.enqueue(chunk);
              return;
            } else {
              /**
               * Add/update base stream controller to structured output processor state
               * so it can be used to enqueue chunks into the main stream from the structuring agent stream.
               * Need to update controller on each new LLM execution step since each step has its own TransformStream.
               */
              if (!processorStates.has(STRUCTURED_OUTPUT_PROCESSOR_NAME)) {
                const processorIndex = processorRunner.outputProcessors.findIndex(
                  p => p.id === STRUCTURED_OUTPUT_PROCESSOR_NAME,
                );
                // Only create the state if the processor actually exists in the list
                if (processorIndex !== -1) {
                  const structuredOutputProcessor = processorRunner.outputProcessors[processorIndex];
                  const structuredOutputProcessorState = new ProcessorState<OUTPUT>({
                    processorName: structuredOutputProcessor?.name ?? STRUCTURED_OUTPUT_PROCESSOR_NAME,
                    tracingContext: options.tracingContext,
                    processorIndex,
                    createSpan: true,
                    processor: structuredOutputProcessor,
                  });
                  structuredOutputProcessorState.customState = { controller };
                  processorStates.set(STRUCTURED_OUTPUT_PROCESSOR_NAME, structuredOutputProcessorState);
                }
              } else {
                // Update controller for new LLM execution step
                const structuredOutputProcessorState = processorStates.get(STRUCTURED_OUTPUT_PROCESSOR_NAME);
                if (structuredOutputProcessorState) {
                  structuredOutputProcessorState.customState.controller = controller;
                }
              }

              // Create a ProcessorStreamWriter from the controller so processOutputStream can emit custom chunks
              streamWriter ??= {
                custom: async (
                  data: { type: string; data?: unknown; transient?: boolean },
                  writerOptions?: { messageId?: string },
                ) => {
                  persistProcessorDataChunk(self.messageList, writerOptions?.messageId ?? self.messageId, data);
                  controller.enqueue(data as ChunkType<OUTPUT>);
                },
              };

              const {
                part: processed,
                blocked,
                reason,
                tripwireOptions,
                processorId,
              } = await processorRunner.processPart(
                chunk,
                processorStates,
                getObservabilityContext(),
                options.requestContext,
                self.messageList,
                0,
                streamWriter,
                options.abortSignal,
              );
              const enqueueTripwire = (r?: string, opts?: { retry?: boolean; metadata?: unknown }, pid?: string) => {
                controller.enqueue({
                  type: 'tripwire',
                  payload: {
                    reason: r || 'Output processor blocked content',
                    retry: opts?.retry,
                    metadata: opts?.metadata,
                    processorId: pid,
                  },
                } as ChunkType<OUTPUT>);
              };

              if (blocked) {
                // Emit a tripwire chunk so downstream knows about the abort
                enqueueTripwire(reason, tripwireOptions, processorId);
                return;
              }
              if (processed) {
                controller.enqueue(processed as ChunkType<OUTPUT>);
              }

              // Emit any parts a processor stashed for reprocessing (e.g. the
              // non-text part that triggered a BatchPartsProcessor flush),
              // pushing each back through the whole chain for downstream
              // processing.
              const reprocessed = await processorRunner.drainReprocessParts(
                processorStates,
                getObservabilityContext(),
                options.requestContext,
                self.messageList,
                0,
                streamWriter,
                options.abortSignal,
              );
              for (const r of reprocessed) {
                if (r.blocked) {
                  enqueueTripwire(r.reason, r.tripwireOptions, r.processorId);
                  return;
                }
                if (r.part != null) {
                  controller.enqueue(r.part as ChunkType<OUTPUT>);
                }
              }
            }
          },
          async flush(controller) {
            // Release parts processors still hold (e.g. a partial BatchPartsProcessor
            // batch) so a model stream that ends mid-batch doesn't drop its tail.
            // A failed or aborted stream drops them instead: a retry would duplicate
            // them, and an abort must not emit content after cancellation.
            if (streamFailed || options.abortSignal?.aborted) {
              for (const part of await processorRunner.discardHeldParts(processorStates)) {
                controller.enqueue(part as ChunkType<OUTPUT>);
              }
              processorRunner.endStreamProcessorSpans(processorStates, { keepStepScopedSpans: true });
              return;
            }
            const released = await processorRunner.flushHeldParts(
              processorStates,
              getObservabilityContext(),
              options.requestContext,
              self.messageList,
              0,
              {
                custom: async (data, writerOptions) => {
                  persistProcessorDataChunk(self.messageList, writerOptions?.messageId ?? self.messageId, data);
                  controller.enqueue(data as ChunkType<OUTPUT>);
                },
              },
              options.abortSignal,
            );
            for (const r of released) {
              if (r.blocked) {
                controller.enqueue({
                  type: 'tripwire',
                  payload: {
                    reason: r.reason || 'Output processor blocked content',
                    retry: r.tripwireOptions?.retry,
                    metadata: r.tripwireOptions?.metadata,
                    processorId: r.processorId,
                  },
                } as ChunkType<OUTPUT>);
                break;
              }
              if (r.part != null) {
                controller.enqueue(r.part as ChunkType<OUTPUT>);
              }
            }
            // Only this step's model stream ended; the step's tool results are still to come.
            processorRunner.endStreamProcessorSpans(processorStates, { keepStepScopedSpans: true });
          },
          async cancel() {
            // The model call was torn down (e.g. a mid-stream disconnect being
            // retried). What processors hold belongs to it and must not be
            // released with the next call's parts.
            await processorRunner.discardHeldParts(processorStates);
            processorRunner.endStreamProcessorSpans(processorStates);
          },
        }),
      );
    }

    // Only apply object transformer in 'direct' mode (LLM generates JSON directly)
    // In 'processor' mode, the StructuredOutputProcessor handles object transformation
    if (self.#structuredOutputMode === 'direct' && self.#options.isLLMExecutionStep) {
      processedStream = processedStream.pipeThrough(
        createObjectStreamTransformer({
          structuredOutput: self.#options.structuredOutput,
          logger: self.logger,
          model: {
            provider: self.#model.provider ?? '',
            modelId: self.#model.modelId ?? '',
            supportsStructuredOutputs: true,
          },
        }),
      );
    }

    type StagedTerminalChunk = ChunkType<OUTPUT> & {
      type: 'data-terminal-tool-result';
      id: string;
      data: TerminalToolResult;
    };
    let stagedTerminalChunk: StagedTerminalChunk | undefined;
    let stagedTerminalStepFinishChunk: Extract<ChunkType<OUTPUT>, { type: 'step-finish' }> | undefined;
    let stagedTerminalStepResult: LLMStepResult<OUTPUT> | undefined;
    const terminalSignature = (value: unknown): { data: TerminalToolResult; signature: string } => {
      const data = materializeTerminalToolResult(value);
      return { data, signature: JSON.stringify(data) };
    };

    const emitSyntheticToolCall = (
      controller: TransformStreamDefaultController<ChunkType<OUTPUT>>,
      toolCallId: string,
      meta: ToolCallStreamingMeta,
      args: Record<string, unknown>,
      runId: string,
      from: ToolCallChunk['from'],
    ) => {
      const synthetic: ToolCallChunk = {
        type: 'tool-call',
        runId,
        from,
        payload: {
          toolCallId,
          toolName: meta.toolName,
          args,
          providerExecuted: meta.providerExecuted,
          providerMetadata: meta.providerMetadata,
          dynamic: meta.dynamic,
          ...(meta.title ? { title: meta.title } : {}),
          ...(meta.observability ? { observability: meta.observability } : {}),
        },
      };
      self.#toolCalls.push(synthetic);
      self.#bufferedByStep.toolCalls.push(synthetic);
      self.#attemptToolCallIds.add(synthetic.payload.toolCallId);
      self.#emitChunk(synthetic);
      controller.enqueue(synthetic);
    };

    // A provider that ended a tool input stream without deltas and never sent the final
    // tool-call still gets a (no-args) tool call recorded before the step closes.
    const flushPendingToolCall = (
      controller: TransformStreamDefaultController<ChunkType<OUTPUT>>,
      toolCallId: string,
    ) => {
      const pending = self.#pendingToolCalls[toolCallId];
      if (!pending) return;
      delete self.#pendingToolCalls[toolCallId];
      emitSyntheticToolCall(controller, toolCallId, pending.meta, pending.args, pending.runId, pending.from);
    };
    const flushPendingToolCalls = (
      controller: TransformStreamDefaultController<ChunkType<OUTPUT>>,
      { onlyStreamed = false }: { onlyStreamed?: boolean } = {},
    ) => {
      for (const [toolCallId, pending] of Object.entries(self.#pendingToolCalls)) {
        if (!onlyStreamed || pending.streamed) flushPendingToolCall(controller, toolCallId);
      }
    };

    this.#baseStream = processedStream.pipeThrough(
      new TransformStream<ChunkType<OUTPUT>, ChunkType<OUTPUT>>({
        transform: async (chunk, controller) => {
          let outboundChunk = chunk;
          let suppressChunkEmission = false;
          // A stream that fails after a tool input finished but before its final tool-call
          // still records the call built from the streamed args. Calls with no streamed args
          // stay pending: the error may be retried, and the retry delivers the real tool-call.
          if (chunk.type === 'error') {
            flushPendingToolCalls(controller, { onlyStreamed: true });
          }
          // A call's outcome must not precede the call itself.
          if (chunk.type === 'tool-result' || chunk.type === 'tool-error') {
            flushPendingToolCall(controller, chunk.payload.toolCallId);
          }
          switch (chunk.type) {
            case 'tool-call-suspended':
            case 'tool-call-approval': {
              self.#status = 'suspended';
              self.#wasSuspended = true;
              self.#delayedPromises.suspendPayload.resolve(chunk.payload);
              self.#delayedPromises.resumeSchema.resolve(chunk.payload.resumeSchema);
              if (!self.#finishCallbackSent) {
                self.#finishCallbackSent = true;
                await options?.onFinish?.(self.#createSuspendedOnFinishPayload(chunk));
              }
              // A plain stream ends here, so flush settles usage. A durable stream stays open
              // across the suspension, so settle the same numbers now. The next finish replaces them.
              self.#settleSuspendedUsage();
              break;
            }
            case 'data-terminal-tool-result': {
              if (typeof chunk.id !== 'string' || chunk.id.length === 0) {
                throw new TypeError('Terminal tool-result data part is missing its stable id');
              }
              const normalized = terminalSignature(chunk.data);
              if (stagedTerminalChunk) {
                const staged = terminalSignature(stagedTerminalChunk.data);
                if (stagedTerminalChunk.id !== chunk.id || staged.signature !== normalized.signature) {
                  throw new TypeError('Conflicting terminal tool-result data parts were received');
                }
                return;
              }
              stagedTerminalChunk = {
                ...chunk,
                data: normalized.data,
              } as StagedTerminalChunk;
              // A public custom data part is not authoritative. Keep it
              // hidden until the framework-owned step and FINISH repeat the
              // exact terminal result after final persistence succeeds.
              return;
            }
            case 'abort':
              self.#status = 'canceled';
              self.#finishReason = 'aborted';
              if (!self.#finishCallbackSent) {
                self.#finishCallbackSent = true;
                await options?.onFinish?.(self.#createAbortedOnFinishPayload());
              }
              self.#closeTransportIfNeeded();
              break;
            case 'step-start':
              // A step reports the reasoning of the model request that completed it. A request
              // cancelled by a queued signal ends without step-finish, so drop what it streamed.
              self.#bufferedByStep.reasoning = [];
              self.#bufferedByStepReasoningDetails = {};
              // Text works the same way: a request that streamed text and then failed into an
              // API-error retry (the durable agent starts each attempt with a new step-start)
              // must not leave its partial text in front of the next request's answer (#26081).
              if (self.#bufferedByStep.text) {
                self.#bufferedByStep.text = '';
                self.#bufferedText.length = Math.min(self.#bufferedText.length, self.#bufferedTextAtStepStart);
              }
              if (self.#reasoningAtStepStart) {
                self.#bufferedReasoning.length = self.#reasoningAtStepStart.length;
                self.#bufferedReasoningDetails = self.#reasoningAtStepStart.details;
              }
              self.#reasoningAtStepStart = {
                length: self.#bufferedReasoning.length,
                details: { ...self.#bufferedReasoningDetails },
              };
              break;
            case 'raw':
              if (!self.#options.includeRawChunks) {
                return;
              }
              break;
            case 'object-result':
              self.#bufferedObject = chunk.object;
              self.#usedFallbackValue = chunk.metadata?.fallback === true;
              // An output processor can still reject this attempt and ask for a retry,
              // which would make this object stale. A settled promise cannot be
              // un-settled, so when processors are in play the object is only buffered
              // here and settled once the attempt survives `step-finish` (or at
              // stream end, whichever comes first). Without processors no retry is
              // possible, so resolve immediately and keep the existing timing.
              // Runner allocation is phase-specific; output-step processors can
              // reject the attempt even when this wrapper has no runner.
              // Only resolve if not already rejected by validation error
              if (!self.#options.outputProcessors?.length && self.#delayedPromises.object.status.type === 'pending') {
                self.#delayedPromises.object.resolve(chunk.object);
              }
              break;
            case 'source':
              self.#bufferedSources.push(chunk);
              self.#bufferedByStep.sources.push(chunk);
              break;
            case 'text-delta':
              self.#bufferedText.push(chunk.payload.text);
              self.#bufferedByStep.text += chunk.payload.text;
              if (chunk.payload.id) {
                const ary = self.#bufferedTextChunks[chunk.payload.id] ?? [];
                ary.push(chunk.payload.text);
                self.#bufferedTextChunks[chunk.payload.id] = ary;
              }
              break;
            case 'tool-call-input-streaming-start':
              // A call recorded in this step whose input streams again comes from a failed attempt being
              // retried (the failure itself may never reach this stream), so the retry's call replaces it.
              // Ids reused by a later step are separate calls.
              if (self.#attemptToolCallIds.has(chunk.payload.toolCallId)) {
                self.#replaceableToolCallIds.add(chunk.payload.toolCallId);
              }
              self.#toolCallDeltaIdNameMap[chunk.payload.toolCallId] = chunk.payload.toolName;
              self.#toolCallStreamingMeta[chunk.payload.toolCallId] = {
                toolName: chunk.payload.toolName,
                providerExecuted: chunk.payload.providerExecuted,
                providerMetadata: chunk.payload.providerMetadata,
                dynamic: chunk.payload.dynamic,
                ...(chunk.payload.title ? { title: chunk.payload.title } : {}),
                ...(chunk.payload.observability ? { observability: chunk.payload.observability } : {}),
              };
              break;
            case 'tool-call-input-streaming-end': {
              const toolCallId = chunk.payload.toolCallId;
              const meta = self.#toolCallStreamingMeta[toolCallId];
              const deltaParts = self.#toolCallArgsDeltas[toolCallId];
              let args: Record<string, unknown> = {};
              let streamedArgsParseFailed = false;
              if (deltaParts?.length) {
                try {
                  const merged = deltaParts.join('');
                  args = typeof merged === 'string' && merged.length > 0 ? JSON.parse(merged) : {};
                } catch {
                  // Truncated JSON is not an executable empty object. Leave
                  // synthesis to a later authoritative tool-call chunk.
                  streamedArgsParseFailed = true;
                }
              }
              delete self.#toolCallStreamingMeta[toolCallId];
              delete self.#toolCallArgsDeltas[toolCallId];
              delete self.#toolCallDeltaIdNameMap[toolCallId];
              // PF-2935: truncated streamed JSON is not an executable empty object, so it is never
              // kept as a pending call that a later flush could synthesize with `{}` args. Only the
              // provider's authoritative tool-call may follow.
              if (meta && streamedArgsParseFailed) {
                self.#emitChunk(chunk);
                controller.enqueue(chunk);
                return;
              }
              if (meta) {
                // Let the real tool-call drive emission: it carries the final args (OpenAI Responses
                // and Anthropic PTC stream none) and the tool payload transform metadata stamped
                // upstream. A synthetic call from the deltas is only emitted if it never arrives.
                self.#pendingToolCalls[toolCallId] = {
                  meta,
                  args,
                  streamed: Boolean(deltaParts?.length),
                  runId: chunk.runId,
                  from: chunk.from,
                };
              }
              break;
            }
            case 'tool-call-delta':
              if (!self.#toolCallArgsDeltas[chunk.payload.toolCallId]) {
                self.#toolCallArgsDeltas[chunk.payload.toolCallId] = [];
              }
              self.#toolCallArgsDeltas?.[chunk.payload.toolCallId]?.push(chunk.payload.argsTextDelta);
              // mutate chunk to add toolname, we need it later to look up tools by their name
              chunk.payload.toolName ||= self.#toolCallDeltaIdNameMap[chunk.payload.toolCallId];
              break;
            case 'file':
              self.#bufferedFiles.push(chunk);
              self.#bufferedByStep.files.push(chunk);
              break;
            case 'reasoning-start': {
              const makeReasoningDetail = () => ({
                type: 'reasoning' as const,
                runId: chunk.runId,
                from: chunk.from,
                payload: {
                  id: chunk.payload.id,
                  providerMetadata: chunk.payload.providerMetadata,
                  text: '',
                },
              });
              // Two independent objects: the run-level entry keeps accumulating
              // across steps while the per-step entry is dropped at step-finish.
              self.#bufferedReasoningDetails[chunk.payload.id] = makeReasoningDetail();
              self.#bufferedByStepReasoningDetails[chunk.payload.id] = makeReasoningDetail();
              break;
            }
            case 'reasoning-delta': {
              self.#bufferedReasoning.push({
                type: 'reasoning',
                runId: chunk.runId,
                from: chunk.from,
                payload: chunk.payload,
              });
              self.#bufferedByStep.reasoning.push({
                type: 'reasoning',
                runId: chunk.runId,
                from: chunk.from,
                payload: chunk.payload,
              });

              for (const bufferedReasoning of [
                self.#bufferedReasoningDetails[chunk.payload.id],
                self.#bufferedByStepReasoningDetails[chunk.payload.id],
              ]) {
                if (!bufferedReasoning) continue;
                bufferedReasoning.payload.text += chunk.payload.text;
                if (chunk.payload.providerMetadata) {
                  bufferedReasoning.payload.providerMetadata = chunk.payload.providerMetadata;
                }
              }
              break;
            }

            case 'reasoning-end': {
              if (chunk.payload.providerMetadata) {
                for (const bufferedReasoning of [
                  self.#bufferedReasoningDetails[chunk.payload.id],
                  self.#bufferedByStepReasoningDetails[chunk.payload.id],
                ]) {
                  if (!bufferedReasoning) continue;
                  bufferedReasoning.payload.providerMetadata = chunk.payload.providerMetadata;
                }
              }
              break;
            }
            case 'tool-call': {
              const pending = self.#pendingToolCalls[chunk.payload.toolCallId];
              if (pending) {
                delete self.#pendingToolCalls[chunk.payload.toolCallId];
                const { meta } = pending;
                chunk.payload.providerExecuted ??= meta.providerExecuted;
                chunk.payload.providerMetadata ??= meta.providerMetadata;
                chunk.payload.dynamic ??= meta.dynamic;
                if (meta.title && !chunk.payload.title) chunk.payload.title = meta.title;
                if (meta.observability && !chunk.payload.observability) {
                  chunk.payload.observability = meta.observability;
                }
              }
              // The retry's real call replaces the one recorded from the failed attempt.
              if (self.#replaceableToolCallIds.delete(chunk.payload.toolCallId)) {
                self.#attemptToolCallIds.delete(chunk.payload.toolCallId);
                for (const calls of [self.#toolCalls, self.#bufferedByStep.toolCalls]) {
                  const index = calls.findLastIndex(tc => tc.payload.toolCallId === chunk.payload.toolCallId);
                  if (index !== -1) calls.splice(index, 1);
                }
              }
              // A call already recorded under this id in this step (a repeated tool-call) is not emitted twice.
              if (self.#attemptToolCallIds.has(chunk.payload.toolCallId)) {
                return;
              }
              self.#toolCalls.push(chunk);
              self.#bufferedByStep.toolCalls.push(chunk);
              self.#attemptToolCallIds.add(chunk.payload.toolCallId);
              const toolCallPayload = chunk.payload;
              // @ts-expect-error TODO: What does this mean??? Why is there a nested output, what is the type supposed to be
              if (toolCallPayload?.output?.from === 'AGENT' && toolCallPayload?.output?.type === 'finish') {
                // @ts-expect-error TODO: What does this mean??? Why is there a nested output, what is the type supposed to be
                const finishPayload = toolCallPayload.output.payload;
                if (finishPayload?.usage) {
                  self.updateUsageCount(finishPayload.usage);
                }
              }
              break;
            }
            case 'tool-result':
              self.#toolResults.push(chunk);
              self.#bufferedByStep.toolResults.push(chunk);
              break;
            case 'step-finish': {
              const stepTerminalValue = (chunk.payload as { terminalToolResult?: unknown }).terminalToolResult;
              const isTerminalStep = stagedTerminalChunk !== undefined || stepTerminalValue !== undefined;
              if (isTerminalStep) {
                if (!stagedTerminalChunk || stepTerminalValue === undefined) {
                  throw new TypeError(
                    'Terminal tool-result data and its authoritative step-finish must be emitted together',
                  );
                }
                const staged = terminalSignature(stagedTerminalChunk.data);
                const authoritative = terminalSignature(stepTerminalValue);
                if (staged.signature !== authoritative.signature) {
                  throw new TypeError('Terminal tool-result step-finish does not match its staged data part');
                }
                if (stagedTerminalStepFinishChunk) {
                  throw new TypeError('Terminal tool-result has more than one authoritative step-finish');
                }
                stagedTerminalStepFinishChunk = chunk;
                suppressChunkEmission = true;
              }

              // A failed or retried step keeps its no-args calls pending, like an error chunk does.
              const stepReason = chunk.payload.stepResult.reason as string;
              const attemptFailed = stepReason === 'retry' || stepReason === 'error';
              flushPendingToolCalls(controller, { onlyStreamed: attemptFailed });
              if (!attemptFailed) {
                self.#attemptToolCallIds.clear();
                self.#replaceableToolCallIds.clear();
              }
              self.updateUsageCount(chunk.payload.output.usage);
              // The first step-finish after a suspension is the suspending step's:
              // its usage is now counted, so it is no longer pending.
              self.#pendingSuspendedStepUsage = undefined;
              // chunk.payload.totalUsage = self.totalUsage;
              self.#warnings = chunk.payload.stepResult.warnings || [];

              if (chunk.payload.metadata.request) {
                self.#request = chunk.payload.metadata.request;
              }

              const { providerMetadata, request, ...otherMetadata } = chunk.payload.metadata;

              // Check if this step has tripwire data (from DefaultStepResult in llm-execution-step)
              // The current step is the last one in the steps array (MastraStepResult with tripwire field)
              const payloadSteps = chunk.payload.output?.steps || [];
              const currentPayloadStep = payloadSteps[payloadSteps.length - 1];
              const stepTripwire = currentPayloadStep?.tripwire;
              // The loop's StepResult for this step, only when the chunk added one.
              const loopStep = payloadSteps.length > self.#loopStepCount ? currentPayloadStep : undefined;
              self.#loopStepCount = Math.max(self.#loopStepCount, payloadSteps.length);

              // Rejected and retried attempts contribute no text to the step or run,
              // including onFinish. Preserve text from earlier accepted steps (#26081, COR-1464).
              const isRetriedStep = (chunk.payload.stepResult.reason as string) === 'retry';
              if (stepTripwire || isRetriedStep) {
                self.#bufferedText.length = Math.min(self.#bufferedText.length, self.#bufferedTextAtStepStart);
              }

              // If step has tripwire, text should be empty (rejected response)
              const stepText = stepTripwire || isRetriedStep ? '' : self.#bufferedByStep.text;

              const stepResult: LLMStepResult<OUTPUT> = {
                stepType: self.#bufferedSteps.length === 0 ? 'initial' : 'tool-result',
                sources: self.#bufferedByStep.sources,
                files: self.#bufferedByStep.files,
                toolCalls: self.#bufferedByStep.toolCalls,
                toolResults: self.#bufferedByStep.toolResults,

                // Durable agents attach pre-computed step content on the
                // step-finish chunk because the stream adapter's messageList
                // may be a stale reference (each workflow step deserializes
                // a fresh instance). Otherwise the loop's own StepResult for
                // this step holds what the step wrote, read from where its
                // iteration wrote it. Counting `step-start` markers back from
                // the end of the response instead picks up an earlier step's
                // parts when the step has no marker of its own (the answer
                // after a resumed tool approval, a rolled-back retry attempt)
                // and a feedback note added after the step (PF-5399). The
                // count remains the fallback for chunks that add no step (AI
                // SDK v4 streams, durable error chunks). A retried attempt
                // (upstream #26081) reports only what the loop kept for it:
                // an API-error retry adds no loop step, so it has no content.
                content: isRetriedStep
                  ? (stepContentFromLoop(loopStep) ?? [])
                  : ((chunk.payload as any)?._durableStepContent ??
                    stepContentFromLoop(loopStep) ??
                    messageList.get.response.aiV5.modelContent(-1)),
                text: stepText,
                // Include tripwire data if present
                tripwire: stepTripwire,
                // Scoped to this step (like `text` above); the run-level
                // reasoning still spans every step.
                reasoningText: self.#bufferedByStep.reasoning.map(reasoningPart => reasoningPart.payload.text).join(''),
                reasoning: Object.values(self.#bufferedByStepReasoningDetails),
                get staticToolCalls() {
                  return self.#bufferedByStep.toolCalls.filter(
                    part => part.type === 'tool-call' && part.payload?.dynamic === false,
                  );
                },
                get dynamicToolCalls() {
                  return self.#bufferedByStep.toolCalls.filter(
                    part => part.type === 'tool-call' && part.payload?.dynamic === true,
                  );
                },
                get staticToolResults() {
                  return self.#bufferedByStep.toolResults.filter(
                    part => part.type === 'tool-result' && part.payload?.dynamic === false,
                  );
                },
                get dynamicToolResults() {
                  return self.#bufferedByStep.toolResults.filter(
                    part => part.type === 'tool-result' && part.payload?.dynamic === true,
                  );
                },
                finishReason: chunk.payload.stepResult.reason,
                usage: chunk.payload.output.usage,
                warnings: self.#warnings,
                request: request || {},
                response: {
                  id: chunk.payload.id || '',
                  timestamp: (chunk.payload.metadata?.timestamp as Date) || new Date(),
                  ...otherMetadata,
                  modelId:
                    (chunk.payload.metadata?.modelId as string) || (chunk.payload.metadata?.model as string) || '',
                  messages: (chunk.payload as any)?._durableResponseMessages ?? chunk.payload.messages?.nonUser ?? [],
                  dbMessages: self.messageList.get.response.db(),
                  // We have to cast this until messageList can take generics also and type metadata, it was too
                  // complicated to do this in this PR, it will require a much bigger change.
                  uiMessages: messageList.get.response.aiV5.ui() as LLMStepResult<OUTPUT>['response']['uiMessages'],
                },
                providerMetadata: providerMetadata ?? chunk.payload.providerMetadata,
              };

              if (isTerminalStep) {
                stagedTerminalStepResult = stepResult;
              } else {
                await options?.onStepFinish?.({
                  ...(self.#model.modelId && self.#model.provider && self.#model.version ? { model: self.#model } : {}),
                  ...stepResult,
                });
              }

              self.#bufferedSteps.push(stepResult);
              self.#lastStepUsage = stepResult.usage;

              // `text` is excluded from a rejected attempt via the empty `stepText`
              // above; the structured object needs the same treatment. Drop the
              // buffered object when this attempt is being retried, otherwise settle
              // the object promise now that the attempt has been accepted.
              if (stepTripwire?.retry) {
                self.#bufferedObject = undefined;
                self.#usedFallbackValue = false;
              } else if (self.#bufferedObject !== undefined && self.#delayedPromises.object.status.type === 'pending') {
                self.#delayedPromises.object.resolve(self.#bufferedObject);
              }

              self.#bufferedByStep = {
                text: '',
                reasoning: [],
                sources: [],
                files: [],
                toolCalls: [],
                toolResults: [],
                dynamicToolCalls: [],
                dynamicToolResults: [],
                staticToolCalls: [],
                staticToolResults: [],
                content: [],
                usage: { inputTokens: undefined, outputTokens: undefined, totalTokens: undefined },
                warnings: [],
                request: {},
                response: {
                  id: '',
                  timestamp: new Date(),
                  modelId: '',
                  messages: [],
                  uiMessages: [],
                },
                reasoningText: '',
                providerMetadata: undefined,
                finishReason: undefined,
              };
              self.#bufferedByStepReasoningDetails = {};
              self.#reasoningAtStepStart = undefined;
              self.#bufferedTextAtStepStart = self.#bufferedText.length;

              // A continuing goal evaluation arrived while this step was still
              // in flight (in-process chunk ordering): this step-finish belongs
              // to the judged turn, so drop its buffers now that its stepResult
              // and usage have been recorded.
              if (self.#truncateAtNextStepFinish) {
                self.#truncateAtNextStepFinish = false;
                self.#truncateRunBuffers();
              }

              break;
            }
            case 'tripwire':
              // Handle tripwire chunks from processors
              self.#tripwire = {
                reason: chunk.payload?.reason || 'Content blocked',
                retry: chunk.payload?.retry,
                metadata: chunk.payload?.metadata,
                processorId: chunk.payload?.processorId,
              };
              self.#finishReason = 'other';
              // The tripwire terminates the stream without a `finish` chunk, so settle the
              // status here; otherwise the run stays 'running' forever.
              if (self.#status !== 'failed' && self.#status !== 'canceled') {
                self.#status = 'tripwire';
              }
              // Mark stream as finished for EventEmitter
              self.#streamFinished = true;

              // Resolve all delayed promises before terminating
              self.resolvePromises({
                text: self.#bufferedText.join(''),
                finishReason: 'other',
                object: undefined,
                usage: self.#usageCount,
                warnings: self.#warnings,
                providerMetadata: undefined,
                response: {
                  // Durable runs end on this chunk when processOutputResult trips, so no finish
                  // chunk builds the response messages; keep the rejected draft visible (#27562).
                  ...(self.#options.resolveFinalPromises && {
                    messages: self.messageList.get.response.aiV5.model(),
                    uiMessages:
                      self.messageList.get.response.aiV5.ui() as LLMStepResult<OUTPUT>['response']['uiMessages'],
                  }),
                  dbMessages: self.messageList.get.response.db(),
                },
                request: {},
                reasoning: [],
                reasoningText: undefined,
                sources: [],
                files: [],
                toolCalls: [],
                toolResults: [],
                steps: self.#bufferedSteps,
                totalUsage: self.#usageCount,
                content: [],
                suspendPayload: undefined, // Tripwire doesn't suspend, so resolve to undefined
                resumeSchema: undefined,
              });

              self.#closeTransportIfNeeded();

              // Emit the tripwire chunk for listeners
              self.#emitChunk(chunk);
              // Pass the tripwire chunk through
              controller.enqueue(chunk);
              // Emit finish event for EventEmitter streams (since flush won't be called on terminate)
              self.#emitter.emit('finish');
              // Terminate the stream
              controller.terminate();
              return;
            case 'finish':
              // A tripwire/abort bail ends the run without the final tool-call; don't record a
              // placeholder no-args call for it.
              if ((chunk.payload.stepResult.reason as string) === 'tripwire') {
                self.#pendingToolCalls = Object.create(null);
              } else {
                flushPendingToolCalls(controller);
              }
              // 'suspended' is not terminal: a resume leg rehydrates the persisted 'suspended'
              // status and must be able to finish as 'success'. Only 'failed' and 'canceled'
              // block the success transition.
              if (self.#status !== 'failed' && self.#status !== 'canceled') {
                self.#status = 'success';
              }
              // An aborted provider stream may finish with a synthetic stop or tripwire.
              // The preceding abort remains authoritative: caller cancellation must not
              // become a successful stop or a processor tripwire in FullOutput.
              if (self.#status === 'canceled') {
                self.#finishReason = 'aborted';
              } else if (chunk.payload.stepResult.reason) {
                self.#finishReason = chunk.payload.stepResult.reason;
              }

              // We can preserve finish metadata from whichever shape the upstream SDK emitted,
              // but we cannot reconstruct providerMetadata if the provider omitted it entirely.
              const finalProviderMetadata = chunk.payload.metadata?.providerMetadata ?? chunk.payload.providerMetadata;

              // Retain the step providerMetadata so output-step processors can
              // read it (e.g. to attribute a Bedrock guardrail content-filter
              // block to the responsible policy). The completed-steps array is
              // empty on such blocks, so this is the only carrier of the trace.
              if (finalProviderMetadata) {
                self.#stepProviderMetadata = finalProviderMetadata;
              }

              // Check if this is a tripwire case - set tripwire data
              // This can happen when max retries is exceeded or a processor triggers a tripwire
              if ((chunk.payload.stepResult.reason as string) === 'tripwire') {
                // Try to get the tripwire data from the last step (MastraStepResult)
                const outputSteps = chunk.payload.output?.steps;
                const lastStep = outputSteps?.[outputSteps?.length - 1];
                const stepTripwire = lastStep?.tripwire;
                // Don't synthesize a tripwire for a caller cancellation: aborts bail through this
                // same 'tripwire' reason but carry no real step tripwire. Only surface a tripwire
                // when an actual processor produced one (or when the run wasn't aborted).
                if (self.#status !== 'canceled' || stepTripwire) {
                  self.#tripwire = {
                    reason: stepTripwire?.reason || 'Processor tripwire triggered',
                    retry: stepTripwire?.retry,
                    metadata: stepTripwire?.metadata,
                    processorId: stepTripwire?.processorId,
                  };
                }
              }

              // Add structured output to the latest assistant message metadata
              if (self.#bufferedObject !== undefined) {
                const responseMessages = messageList.get.response.db();
                const lastAssistantMessage = [...responseMessages].reverse().find(m => m.role === 'assistant');
                if (lastAssistantMessage) {
                  if (!lastAssistantMessage.content.metadata) {
                    lastAssistantMessage.content.metadata = {};
                  }
                  lastAssistantMessage.content.metadata.structuredOutput = self.#bufferedObject;
                }
              }

              let response: LLMStepResult<OUTPUT>['response'] = {};
              if (chunk.payload.metadata) {
                const { providerMetadata, request, ...otherMetadata } = chunk.payload.metadata;

                response = {
                  ...otherMetadata,
                  modelId:
                    (chunk.payload.metadata?.modelId as string) || (chunk.payload.metadata?.model as string) || '',
                  messages: messageList.get.response.aiV5.model(),
                  uiMessages: messageList.get.response.aiV5.ui() as LLMStepResult<OUTPUT>['response']['uiMessages'],
                };
              } else if (self.#bufferedSteps.length > 0) {
                // Durable finish chunks carry no model metadata; the last step's response
                // already holds the run's response messages and model id (#26780).
                response = { ...self.#bufferedSteps[self.#bufferedSteps.length - 1]!.response };
              }

              if (self.#finishUsageIsTotal) {
                self.#usageCount.inputTokens = undefined;
                self.#usageCount.outputTokens = undefined;
                self.#usageCount.totalTokens = undefined;
                delete self.#usageCount.reasoningTokens;
                delete self.#usageCount.cachedInputTokens;
                delete self.#usageCount.cacheCreationInputTokens;
                delete self.#usageCount.cacheCreationInputTokens5m;
                delete self.#usageCount.cacheCreationInputTokens1h;
                self.#usageCountMissing.clear();
              }
              this.populateUsageCount(chunk.payload.output.usage as Partial<LanguageModelUsage>);

              chunk.payload.output.usage = {
                inputTokens: self.#usageCount.inputTokens,
                outputTokens: self.#usageCount.outputTokens,
                totalTokens: self.#getTotalUsage().totalTokens,
                ...(self.#usageCount.reasoningTokens !== undefined && {
                  reasoningTokens: self.#usageCount.reasoningTokens,
                }),
                ...(self.#usageCount.cachedInputTokens !== undefined && {
                  cachedInputTokens: self.#usageCount.cachedInputTokens,
                }),
                ...(self.#usageCount.cacheCreationInputTokens !== undefined && {
                  cacheCreationInputTokens: self.#usageCount.cacheCreationInputTokens,
                }),
                ...(self.#usageCount.cacheCreationInputTokens5m !== undefined && {
                  cacheCreationInputTokens5m: self.#usageCount.cacheCreationInputTokens5m,
                }),
                ...(self.#usageCount.cacheCreationInputTokens1h !== undefined && {
                  cacheCreationInputTokens1h: self.#usageCount.cacheCreationInputTokens1h,
                }),
                ...(self.#usageCount.raw !== undefined && {
                  raw: self.#usageCount.raw,
                }),
              };

              let terminalMessageListSnapshot: ReturnType<MessageList['serialize']> | undefined;
              let terminalStepSnapshots:
                | Array<{
                    step: LLMStepResult<OUTPUT>;
                    content: LLMStepResult<OUTPUT>['content'];
                    response: LLMStepResult<OUTPUT>['response'];
                    text: string;
                  }>
                | undefined;
              let terminalMessageListReference: MessageList | undefined;
              let terminalProcessorStateSnapshots: ReturnType<ProcessorRunner['snapshotProcessorStates']> | undefined;
              let terminalPartsBeforeProcessors: Map<string, number> | undefined;
              let terminalResponseSnapshot: LLMStepResult<OUTPUT>['response'] | undefined;
              let terminalChunkResponseSnapshot: LLMStepResult<OUTPUT>['response'] | undefined;
              let terminalChunkHadResponse = false;
              let terminalPromiseResolution: Pick<PromiseResults<OUTPUT>, 'text' | 'finishReason'> | undefined;
              const terminalProcessorChunks: ChunkType<OUTPUT>[] = [];
              let terminalDeliveryFailed = false;
              let providerResultCancelled = false;
              let terminalFailureReason: 'error' | 'other' = 'error';
              // The onFinish writer runs after terminal commit, before the final finish chunk.
              // Unlike the processor writer below, it must not buffer chunks for that commit.
              // Emit to both fullStream/EventEmitter consumers and raw stream consumers.
              const outputResultWriter = {
                custom: async (
                  data: { type: string; data?: unknown; transient?: boolean },
                  writerOptions?: { messageId?: string },
                ) => {
                  if (data.type === 'data-terminal-tool-result') {
                    throw new TypeError('data-terminal-tool-result is reserved for framework terminal delivery');
                  }
                  persistProcessorDataChunk(self.messageList, writerOptions?.messageId ?? self.messageId, data);
                  self.#emitChunk(data as ChunkType<OUTPUT>);
                  controller.enqueue(data as ChunkType<OUTPUT>);
                },
              };

              let outputResultTripwire: StepTripwireData | undefined;
              try {
                const finishTerminalValue = (chunk.payload as { terminalToolResult?: unknown }).terminalToolResult;
                const hasTerminalProtocol =
                  stagedTerminalChunk !== undefined ||
                  stagedTerminalStepFinishChunk !== undefined ||
                  finishTerminalValue !== undefined;
                let terminalCommit:
                  | {
                      data: TerminalToolResult;
                      dataChunk: NonNullable<typeof stagedTerminalChunk>;
                      stepFinishChunk: NonNullable<typeof stagedTerminalStepFinishChunk>;
                      stepResult: LLMStepResult<OUTPUT>;
                    }
                  | undefined;

                if (hasTerminalProtocol) {
                  terminalResponseSnapshot = structuredClone(response);
                  terminalChunkHadResponse = Object.prototype.hasOwnProperty.call(chunk.payload, 'response');
                  if (terminalChunkHadResponse) {
                    terminalChunkResponseSnapshot = structuredClone(
                      (chunk.payload as { response?: LLMStepResult<OUTPUT>['response'] }).response,
                    );
                  }
                  if (
                    !stagedTerminalChunk ||
                    !stagedTerminalStepFinishChunk ||
                    !stagedTerminalStepResult ||
                    finishTerminalValue === undefined
                  ) {
                    throw new TypeError(
                      'Terminal tool-result delivery requires matching data, step-finish, and FINISH authority',
                    );
                  }
                  const staged = terminalSignature(stagedTerminalChunk.data);
                  const step = terminalSignature(
                    (stagedTerminalStepFinishChunk.payload as { terminalToolResult?: unknown }).terminalToolResult,
                  );
                  const finish = terminalSignature(finishTerminalValue);
                  if (staged.signature !== step.signature || staged.signature !== finish.signature) {
                    throw new TypeError('Terminal tool-result authority does not match the staged data part');
                  }
                  const stepCount = chunk.payload.output.steps?.length;
                  if (typeof stepCount !== 'number' || !Number.isSafeInteger(stepCount)) {
                    throw new TypeError('Terminal tool-result FINISH is missing its authoritative step count');
                  }
                  const expectedId = createTerminalToolResultPartId(self.runId, stepCount);
                  if (stagedTerminalChunk.id !== expectedId) {
                    throw new TypeError('Terminal tool-result data part id does not match its run and step');
                  }
                  terminalCommit = {
                    data: finish.data,
                    dataChunk: stagedTerminalChunk,
                    stepFinishChunk: stagedTerminalStepFinishChunk,
                    stepResult: stagedTerminalStepResult,
                  };

                  const terminalMessageId = stagedTerminalStepFinishChunk.payload.messageId;
                  if (typeof terminalMessageId !== 'string' || terminalMessageId.length === 0) {
                    throw new TypeError('Terminal tool-result step is missing its response message id');
                  }
                  terminalMessageListReference = self.messageList;
                  terminalMessageListSnapshot = structuredClone(self.messageList.serialize());
                  // MessageList getters may return message objects that are also referenced by
                  // buffered step responses. Adding the staged terminal part can therefore mutate
                  // those historical views in place. Keep a detached snapshot so a persistence
                  // failure cannot expose an uncommitted terminal value through `steps` or
                  // callback payloads after the canonical MessageList has been restored.
                  terminalStepSnapshots = self.#bufferedSteps.map(step => ({
                    step,
                    content: structuredClone(step.content),
                    response: structuredClone(step.response),
                    text: step.text,
                  }));
                  const existingTerminalMessage = self.messageList.get.all
                    .db()
                    .find(message => message.id === terminalMessageId);
                  const memoryInfo = terminalMessageListSnapshot.memoryInfo;
                  self.messageList.add(
                    {
                      id: terminalMessageId,
                      role: 'assistant',
                      content: {
                        format: 2,
                        parts: [
                          {
                            type: 'data-terminal-tool-result',
                            id: stagedTerminalChunk.id,
                            data: terminalCommit.data,
                          },
                        ],
                      },
                      createdAt: existingTerminalMessage?.createdAt ?? new Date(),
                      threadId: existingTerminalMessage?.threadId ?? memoryInfo?.threadId,
                      resourceId: existingTerminalMessage?.resourceId ?? memoryInfo?.resourceId,
                    },
                    'response',
                  );
                }

                // The provider's final output is known here. Its listeners finish
                // before output result processors run: those persist the turn
                // (memory), and nothing may hold the reply durably before the
                // listeners have staged it for recovery.
                try {
                  await self.#publishProviderResult({
                    response,
                    providerMetadata: finalProviderMetadata,
                    terminalToolResult: terminalCommit?.data,
                  });
                } catch (error) {
                  // Only a listener's explicit cancellation ends the run cancelled.
                  if (error instanceof ProviderResultCancelledError) providerResultCancelled = true;
                  throw error;
                }

                if (self.processorRunner && !self.#options.isLLMExecutionStep) {
                  // Run output processors when NOT in LLM execution step context
                  // (i.e., when this is the final MastraModelOutput for the agent)

                  const lastStep = self.#bufferedSteps[self.#bufferedSteps.length - 1];
                  const lastStepText = lastStep?.text || '';

                  if (terminalCommit) {
                    terminalProcessorStateSnapshots = self.processorRunner.snapshotProcessorStates();
                    terminalPartsBeforeProcessors = countResponseTerminalToolResultParts(self.messageList);
                  }

                  // Create a writer from the controller so processOutputResult can emit custom chunks.
                  // Must use both #emitChunk (for fullStream/EventEmitter consumers) and
                  // controller.enqueue (for raw stream consumers) to ensure visibility.
                  const outputResultWriter = {
                    custom: async (
                      data: { type: string; data?: unknown; transient?: boolean },
                      writerOptions?: { messageId?: string },
                    ) => {
                      if (data.type === 'data-terminal-tool-result') {
                        throw new TypeError('data-terminal-tool-result is reserved for framework terminal delivery');
                      }
                      persistProcessorDataChunk(self.messageList, writerOptions?.messageId ?? self.messageId, data);
                      const customChunk = data as ChunkType<OUTPUT>;
                      if (terminalCommit) {
                        // A pass-through processor may derive custom output from the staged
                        // terminal answer before the final persistence owner runs. Keep those
                        // chunks private until the entire terminal transaction validates.
                        terminalProcessorChunks.push(structuredClone(customChunk));
                        return;
                      }
                      self.#emitChunk(customChunk);
                      controller.enqueue(customChunk);
                    },
                  };

                  const outputResult: OutputResult = {
                    text: self.#bufferedText.join(''),
                    usage: chunk.payload.output.usage as LanguageModelUsage,
                    finishReason: self.#status === 'failed' ? 'error' : self.#finishReason || 'unknown',
                    steps: [...self.#bufferedSteps] as LLMStepResult[],
                  };

                  // Canceled output bypasses the normal LLM-step response assembly. Add its
                  // nonblank in-flight text to MessageList so every output processor receives
                  // the same terminal transcript shape; persistence remains processor-owned.
                  // A late abort can still emit step-finish, which moves this
                  // step's text into lastStep before clearing the live buffer.
                  // Never fall back to aggregate text or a completed earlier step.
                  const canceledStepText =
                    self.#bufferedByStep.text || (lastStep?.finishReason === 'abort' ? lastStep.text : '');
                  if (self.#status === 'canceled' && canceledStepText.trim().length > 0) {
                    self.messageList.add(
                      {
                        role: 'assistant',
                        content: [{ type: 'text', text: canceledStepText }],
                      },
                      'response',
                      { merge: false },
                    );
                  }

                  const outputTextBeforeProcessing = resolveOutputTextSkippingCompletionChecks(self.messageList);
                  const stepMessage = resolveOutputMessageSkippingCompletionChecks(self.messageList);
                  const iterationPartOffset = stepMessage
                    ? findIterationPartOffset(self.messageList, stepMessage, lastStepText)
                    : undefined;
                  const separatedIteration =
                    stepMessage && iterationPartOffset !== undefined
                      ? self.messageList.splitResponseMessageAtPartOffset(stepMessage.id, iterationPartOffset)
                      : undefined;
                  const earlierStepMessage = separatedIteration
                    ? self.messageList.get.response
                        .db()
                        .find(message => message.id === separatedIteration.earlierMessageId)
                    : undefined;
                  const earlierStepPartBoundaries = separatedIteration
                    ? findStepPartBoundaries(earlierStepMessage, self.#bufferedSteps.slice(0, -1))
                    : undefined;
                  const earlierTextBeforeProcessing = earlierStepMessage
                    ? resolveMessageText(earlierStepMessage)
                    : undefined;
                  const earlierMessageGroups =
                    !separatedIteration && stepMessage && self.#bufferedSteps.length > 1
                      ? groupEarlierStepsByMessage(
                          self.messageList.get.response
                            .db()
                            .slice(
                              0,
                              self.messageList.get.response.db().findIndex(message => message.id === stepMessage.id),
                            )
                            .filter(
                              message => message.role === 'assistant' && !message.content?.metadata?.completionResult,
                            ),
                          self.#bufferedSteps.slice(0, -1),
                        )
                      : undefined;
                  const processorStepMessageId = separatedIteration?.currentMessageId ?? stepMessage?.id;
                  const processorStepMessage = processorStepMessageId
                    ? self.messageList.get.response.db().find(message => message.id === processorStepMessageId)
                    : undefined;
                  const processorStepMessageParts = processorStepMessage?.content?.parts;
                  const originalProcessorStepMessageParts = processorStepMessageParts
                    ? [...processorStepMessageParts]
                    : undefined;

                  let stepText: string | undefined;
                  let processedEarlierText: string | undefined;
                  let processedEarlierMessage: MastraDBMessage | undefined;
                  try {
                    self.messageList = await self.processorRunner.runOutputProcessors(
                      self.messageList,
                      resolveObservabilityContext(options),
                      self.#options.requestContext,
                      0,
                      outputResultWriter,
                      outputResult,
                    );

                    const responseMessages = self.messageList.get.response.db();
                    const resolvedProcessedStepMessage = resolveOutputMessageSkippingCompletionChecks(self.messageList);
                    const idMatchedProcessedStepMessage = processorStepMessageId
                      ? responseMessages.find(message => message.id === processorStepMessageId)
                      : undefined;
                    const processedStepMessage =
                      resolvedProcessedStepMessage?.id === separatedIteration?.earlierMessageId
                        ? idMatchedProcessedStepMessage
                        : (resolvedProcessedStepMessage ?? idMatchedProcessedStepMessage);
                    processedEarlierMessage = separatedIteration
                      ? responseMessages.find(message => message.id === separatedIteration.earlierMessageId)
                      : undefined;
                    processedEarlierText = separatedIteration
                      ? processedEarlierMessage
                        ? resolveMessageText(processedEarlierMessage)
                        : ''
                      : undefined;
                    const processedOutputText = resolveOutputTextSkippingCompletionChecks(self.messageList);
                    const processedStepParts = processedStepMessage?.content?.parts;
                    stepText =
                      separatedIteration || processedOutputText !== outputTextBeforeProcessing
                        ? resolveProcessedStepText({
                            processedStepMessage,
                            processedStepParts,
                            originalMessageId: processorStepMessage?.id,
                            originalIterationPartOffset: separatedIteration ? 0 : iterationPartOffset,
                            originalPartCount: processorStepMessageParts?.length,
                            originalParts: originalProcessorStepMessageParts,
                            originalStepText: lastStepText,
                            outputText: processedOutputText,
                          })
                        : undefined;
                    const stepMessageWasReplaced =
                      separatedIteration &&
                      processedStepMessage &&
                      processedStepMessage.id !== processorStepMessage?.id;
                    if (stepMessageWasReplaced && resolveMessageText(processedStepMessage) === processedEarlierText) {
                      stepText = '';
                    }

                    let processedEarlierGroupText: string | undefined;
                    for (const group of earlierMessageGroups ?? []) {
                      const processedMessage = responseMessages.find(message => message.id === group.messageId);
                      processedEarlierGroupText = processedMessage ? resolveMessageText(processedMessage) : '';
                      reconcileBufferedStepTexts(
                        group.steps,
                        processedEarlierGroupText,
                        processedMessage,
                        group.boundaries,
                        group.originalText,
                      );
                    }
                    // Same rule as the split path above: a processor that replaced the current
                    // step's response with a copy of the earlier iteration removed this step's text.
                    if (
                      processedEarlierGroupText &&
                      processedStepMessage &&
                      processedStepMessage.id !== processorStepMessage?.id &&
                      resolveMessageText(processedStepMessage) === processedEarlierGroupText
                    ) {
                      stepText = '';
                    }
                    if (processedEarlierText !== undefined) {
                      reconcileBufferedStepTexts(
                        self.#bufferedSteps.slice(0, -1),
                        processedEarlierText,
                        processedEarlierMessage,
                        earlierStepPartBoundaries,
                        earlierTextBeforeProcessing,
                      );
                    }
                  } finally {
                    clearStepPartIndexes(self.messageList.get.response.db());
                    if (separatedIteration) {
                      self.messageList.coalesceSplitResponseMessages(
                        separatedIteration.earlierMessageId,
                        separatedIteration.currentMessageId,
                      );
                    }
                  }

                  // When multiple iterations share one response message, separate the current iteration before result
                  // processing so both the earlier buffered steps and the current step are reconciled from processor
                  // output. Compare against undefined, not truthiness, so clearing to '' applies when the processor
                  // removes an iteration's response.
                  if (self.#status !== 'canceled' && lastStep && stepText !== undefined && stepText !== lastStepText) {
                    lastStep.text = stepText;
                  }

                  // Fork PF-2246: the resolved text is the final caller-visible assistant turn,
                  // which can span more than one response message when the loop forced a
                  // continuation (issue #14134); suppressed completion-check feedback is
                  // filtered inside getFinalAssistantTurnText (#19951). Use the processed text
                  // when a response message exists, even if the processor intentionally emptied
                  // it. Only fall back to the raw model text when there is no processed message.
                  const finalTextAndReason = {
                    text: stepMessage ? self.messageList.getFinalAssistantTurnText() : lastStepText,
                    finishReason: self.#finishReason,
                  };
                  if (terminalCommit) {
                    terminalPromiseResolution = finalTextAndReason;
                  } else {
                    this.resolvePromises(finalTextAndReason);
                  }

                  // Update response with processed messages after output processors have run
                  if (chunk.payload.metadata) {
                    const { providerMetadata, request, ...otherMetadata } = chunk.payload.metadata;
                    response = {
                      ...otherMetadata,
                      modelId:
                        (chunk.payload.metadata?.modelId as string) || (chunk.payload.metadata?.model as string) || '',
                      messages: self.messageList.get.response.aiV5.model(),
                      uiMessages:
                        self.messageList.get.response.aiV5.ui() as LLMStepResult<OUTPUT>['response']['uiMessages'],
                    };
                  }

                  // Cast needed because chunk.payload.response is typed with default OUTPUT=undefined
                  (chunk.payload as { response?: LLMStepResult<OUTPUT>['response'] }).response = response;
                } else if (!self.#options.isLLMExecutionStep || self.#options.resolveFinalPromises) {
                  // No processor runner, not in LLM execution step - resolve ordinary
                  // tool-driven multi-step runs with the last step's text so narration before
                  // tool calls is excluded. Suspended/resumed tool approval flows keep the
                  // aggregate stream text because pre-approval text is part of the resumed run.
                  // Durable agents set resolveFinalPromises to force resolution even when
                  // isLLMExecutionStep is true (single MastraModelOutput for the entire run).
                  // A step rejected by a tripwire keeps its empty text, so durable runs don't
                  // return a rejected reply (#26048).
                  const finalTextAndReason = {
                    text: self.#producedText(),
                    finishReason: self.#finishReason,
                  };
                  if (terminalCommit) {
                    terminalPromiseResolution = finalTextAndReason;
                  } else {
                    this.resolvePromises(finalTextAndReason);
                  }
                }
                // If isLLMExecutionStep is true (without resolveFinalPromises), don't resolve
                // text here - let the outer MastraModelOutput handle it

                if (terminalCommit) {
                  // Matched by the framework part id across the response, not by the
                  // id of the message carrying it (see findRetainedTerminalToolResultPart).
                  const retainedPart = findRetainedTerminalToolResultPart(
                    self.messageList,
                    terminalCommit.dataChunk.id,
                    terminalPartsBeforeProcessors ?? countResponseTerminalToolResultParts(self.messageList),
                  );
                  if (retainedPart === undefined) {
                    throw new TypeError('Final output processors did not retain exactly one terminal tool result');
                  }
                  const persistedTerminal = terminalSignature(retainedPart.data);
                  if (persistedTerminal.signature !== terminalSignature(terminalCommit.data).signature) {
                    throw new TypeError('Final output processors changed the terminal tool result');
                  }

                  terminalCommit.stepResult.content = self.messageList.get.response.aiV5.modelContent(-1);
                  terminalCommit.stepResult.response.dbMessages = self.messageList.get.response.db();
                  terminalCommit.stepResult.response.uiMessages =
                    self.messageList.get.response.aiV5.ui() as LLMStepResult<OUTPUT>['response']['uiMessages'];
                  if (chunk.payload.metadata) {
                    const { providerMetadata, request, ...otherMetadata } = chunk.payload.metadata;
                    response = {
                      ...otherMetadata,
                      modelId:
                        (chunk.payload.metadata?.modelId as string) || (chunk.payload.metadata?.model as string) || '',
                      messages: self.messageList.get.response.aiV5.model(),
                      uiMessages:
                        self.messageList.get.response.aiV5.ui() as LLMStepResult<OUTPUT>['response']['uiMessages'],
                    };
                    (chunk.payload as { response?: LLMStepResult<OUTPUT>['response'] }).response = response;
                  }
                  self.#terminalToolResult = terminalCommit.data;
                  if (terminalPromiseResolution) {
                    this.resolvePromises(terminalPromiseResolution);
                  }
                  for (const processorChunk of terminalProcessorChunks) {
                    self.#emitChunk(processorChunk);
                    controller.enqueue(processorChunk);
                  }
                  for (const terminalChunk of [terminalCommit.dataChunk, terminalCommit.stepFinishChunk]) {
                    self.#emitChunk(terminalChunk);
                    controller.enqueue(terminalChunk);
                  }
                  try {
                    await options?.onStepFinish?.({
                      ...(self.#model.modelId && self.#model.provider && self.#model.version
                        ? { model: self.#model }
                        : {}),
                      ...terminalCommit.stepResult,
                    });
                  } catch (callbackError) {
                    self.logger?.error?.('Terminal onStepFinish callback failed after commit', {
                      error: callbackError,
                      runId: self.runId,
                    });
                  }
                  stagedTerminalChunk = undefined;
                  stagedTerminalStepFinishChunk = undefined;
                  stagedTerminalStepResult = undefined;
                }
              } catch (error) {
                terminalDeliveryFailed = true;
                if (terminalMessageListReference && terminalMessageListSnapshot) {
                  self.messageList = terminalMessageListReference;
                  self.messageList.deserialize(terminalMessageListSnapshot);
                }
                if (self.processorRunner && terminalProcessorStateSnapshots) {
                  self.processorRunner.restoreProcessorStates(terminalProcessorStateSnapshots);
                }
                for (const snapshot of terminalStepSnapshots ?? []) {
                  snapshot.step.content = snapshot.content;
                  snapshot.step.response = snapshot.response;
                  snapshot.step.text = snapshot.text;
                }
                if (terminalResponseSnapshot) {
                  response = terminalResponseSnapshot;
                }
                if (terminalChunkHadResponse) {
                  (chunk.payload as { response?: LLMStepResult<OUTPUT>['response'] }).response =
                    terminalChunkResponseSnapshot;
                } else {
                  delete (chunk.payload as { response?: LLMStepResult<OUTPUT>['response'] }).response;
                }
                stagedTerminalChunk = undefined;
                stagedTerminalStepFinishChunk = undefined;
                stagedTerminalStepResult = undefined;
                self.#terminalToolResult = undefined;
                if (providerResultCancelled) {
                  terminalFailureReason = 'other';
                  self.#status = 'canceled';
                  self.#finishReason = 'aborted';
                  self.resolvePromises({
                    finishReason: 'aborted',
                    text: '',
                  });
                } else if (error instanceof TripWire) {
                  terminalFailureReason = 'other';
                  self.#tripwire = {
                    reason: error.message,
                    retry: error.options?.retry,
                    metadata: error.options?.metadata,
                    processorId: error.processorId,
                  };
                  self.#finishReason = 'other';
                  outputResultTripwire = self.#tripwire;
                  // A tripwire rejects the output without erasing it; keep `text` in sync with
                  // steps/response messages and report the rejection via `tripwire`.
                  self.resolvePromises({
                    finishReason: 'other',
                    text: self.#producedText(),
                  });
                } else {
                  self.#status = 'failed';
                  self.#finishReason = 'error';
                  self.#error = getErrorFromUnknown(error, {
                    fallbackMessage: 'Unknown error in stream',
                  });
                  self.resolvePromises({
                    finishReason: 'error',
                    text: '',
                  });
                }
                if (self.#delayedPromises.object.status.type !== 'resolved') {
                  self.#delayedPromises.object.resolve(self.#bufferedObject as OUTPUT);
                }
              }

              if (terminalDeliveryFailed) {
                const { terminalToolResult: _terminalToolResult, ...safePayload } =
                  chunk.payload as typeof chunk.payload & {
                    terminalToolResult?: unknown;
                  };
                outboundChunk = {
                  ...chunk,
                  payload: {
                    ...safePayload,
                    stepResult: {
                      ...safePayload.stepResult,
                      reason: terminalFailureReason,
                    },
                  },
                } as ChunkType<OUTPUT>;
              }

              const reasoningText =
                self.#bufferedReasoning.length > 0
                  ? self.#bufferedReasoning.map(reasoningPart => reasoningPart.payload.text).join('')
                  : undefined;

              const baseFinishStep = self.#bufferedSteps[self.#bufferedSteps.length - 1];
              if (
                baseFinishStep &&
                baseFinishStep.providerMetadata === undefined &&
                finalProviderMetadata !== undefined
              ) {
                baseFinishStep.providerMetadata = finalProviderMetadata;
              }

              // Resolve all delayed promises with final values
              this.resolvePromises({
                usage: self.#usageCount,
                warnings: self.#warnings,
                providerMetadata: finalProviderMetadata,
                response: { ...response, dbMessages: self.messageList.get.response.db() },
                request: self.#request || {},
                reasoningText,
                reasoning: Object.values(self.#bufferedReasoningDetails || {}),
                sources: self.#bufferedSources,
                files: self.#bufferedFiles,
                toolCalls: self.#toolCalls,
                toolResults: self.#toolResults,
                steps: self.#bufferedSteps,
                totalUsage: self.#getTotalUsage(),
                content: self.messageList.get.response.aiV5.stepContent(),
                suspendPayload: undefined,
                resumeSchema: undefined,
              });

              if (baseFinishStep) {
                const onFinishPayload: MastraOnFinishCallbackArgs<OUTPUT> = {
                  // StepResult properties from baseFinishStep
                  providerMetadata: baseFinishStep.providerMetadata ?? finalProviderMetadata,
                  text: self.#bufferedText.join(''),
                  warnings: baseFinishStep.warnings ?? [],
                  finishReason: self.#finishReason ?? chunk.payload.stepResult.reason,
                  content: self.messageList.get.response.aiV5.stepContent(),
                  request: await self.request,
                  error: self.error,
                  reasoning: await self.reasoning,
                  reasoningText: await self.reasoningText,
                  sources: await self.sources,
                  files: await self.files,
                  steps: self.#bufferedSteps,
                  response: {
                    ...(await self.response),
                    ...baseFinishStep.response,
                    messages: self.messageList.get.response.aiV5.model(),
                    dbMessages: self.messageList.get.response.db(),
                  },
                  usage: chunk.payload.output.usage,
                  totalUsage: self.#getTotalUsage(),
                  ...(self.#generationReportedUsage && { generationReportedUsage: self.#generationReportedUsage }),
                  toolCalls: await self.toolCalls,
                  toolResults: await self.toolResults,
                  staticToolCalls: (await self.toolCalls).filter(toolCall => toolCall?.payload?.dynamic === false),
                  staticToolResults: (await self.toolResults).filter(
                    toolResult => toolResult?.payload?.dynamic === false,
                  ),
                  dynamicToolCalls: (await self.toolCalls).filter(toolCall => toolCall?.payload?.dynamic === true),
                  dynamicToolResults: (await self.toolResults).filter(
                    toolResult => toolResult?.payload?.dynamic === true,
                  ),
                  ...(self.#terminalToolResult ? { terminalToolResult: self.#terminalToolResult } : {}),
                  // Custom properties (not part of standard callback)
                  ...(self.#model.modelId && self.#model.provider && self.#model.version ? { model: self.#model } : {}),
                  usedFallbackValue: self.#usedFallbackValue,
                  object:
                    self.#delayedPromises.object.status.type === 'rejected'
                      ? undefined
                      : self.#delayedPromises.object.status.type === 'resolved'
                        ? self.#delayedPromises.object.status.value
                        : self.#bufferedObject !== undefined
                          ? self.#bufferedObject
                          : self.#structuredOutputMode === 'direct' && baseFinishStep.text
                            ? (() => {
                                try {
                                  return JSON.parse(baseFinishStep.text);
                                } catch {
                                  return undefined;
                                }
                              })()
                            : undefined,
                };

                if (!self.#finishCallbackSent) {
                  self.#finishCallbackSent = true;
                  const onFinishOptions = {
                    writer: outputResultWriter,
                    ...(outputResultTripwire && { outputResultTripwire }),
                  };
                  if (self.#terminalToolResult) {
                    try {
                      await options?.onFinish?.(onFinishPayload, onFinishOptions);
                    } catch (callbackError) {
                      self.logger?.error?.('Terminal onFinish callback failed after commit', {
                        error: callbackError,
                        runId: self.runId,
                      });
                    }
                  } else {
                    await options?.onFinish?.(onFinishPayload, onFinishOptions);
                  }
                }
              }

              self.#closeTransportIfNeeded();

              // A `processOutputResult` tripwire rejects the run: end the stream with a
              // `tripwire` chunk instead of the `finish` chunk, matching every other tripwire.
              if (outputResultTripwire) {
                if (self.#status !== 'failed' && self.#status !== 'canceled') {
                  self.#status = 'tripwire';
                }
                const tripwireChunk = {
                  type: 'tripwire',
                  runId: chunk.runId,
                  from: chunk.from,
                  payload: { ...outputResultTripwire },
                } as ChunkType<OUTPUT>;
                self.#emitChunk(tripwireChunk);
                controller.enqueue(tripwireChunk);
                return;
              }
              break;

            case 'goal':
              // A continuing goal evaluation marks a safe truncation point for
              // run-lifetime buffers: the turn's messages are already persisted
              // to the MessageList by this point, and goal runs chain many agent
              // turns inside one stream — retaining every chunk, step, and tool
              // result grows memory unboundedly over long goal runs (and bloats
              // suspend snapshots via `serializeState`). `shouldContinue` is the
              // goal gate's explicit continuation decision: only truncate when
              // another judged iteration follows, so terminal evaluations
              // (completion, waiting, judge failure, budget exhaustion) keep the
              // final turn intact for run-end results like `getFullOutput()`.
              if (chunk.payload.shouldContinue === true) {
                self.#truncateRunBuffers();
                // In-process engines emit the goal chunk BEFORE the judged
                // turn's step-finish (the gate decides `isContinued` first);
                // durable engines emit it after. If the judged step is still in
                // flight, its step-finish lands after this boundary — drop that
                // step's buffers too when it arrives.
                const byStep = self.#bufferedByStep;
                if (
                  byStep.text.length > 0 ||
                  byStep.toolCalls.length > 0 ||
                  byStep.reasoning.length > 0 ||
                  byStep.sources.length > 0 ||
                  byStep.files.length > 0
                ) {
                  self.#truncateAtNextStepFinish = true;
                }
              }
              break;

            case 'error':
              const error = getErrorFromUnknown(chunk.payload.error, {
                fallbackMessage: 'Unknown error chunk in stream',
              });
              self.#error = error;
              self.#status = 'failed';
              self.#streamFinished = true; // Mark stream as finished for EventEmitter

              Object.values(self.#delayedPromises).forEach(promise => {
                if (promise.status.type === 'pending') {
                  promise.reject(self.#error);
                }
              });

              self.#closeTransportIfNeeded();
              // Deliver the error chunk downstream first, then settle waiters.
              // An errored stream never reaches flush(), so without this,
              // _waitUntilFinished() callers that subscribed before the error
              // hang forever while later callers resolve immediately via the
              // #streamFinished flag. Deliberately NOT 'finish': observer
              // streams close on 'finish', and durable fallback/error-processor
              // recovery keeps consuming chunks after an error chunk.
              self.#emitChunk(chunk);
              controller.enqueue(chunk);
              self.#emitter.emit('settled');
              return;
          }
          if (suppressChunkEmission) return;
          self.#emitChunk(outboundChunk);
          controller.enqueue(outboundChunk);
        },
        flush: () => {
          if (self.#delayedPromises.object.status.type === 'pending') {
            // always resolve a pending object promise in flush (with the buffered object
            // if the stream produced one, otherwise undefined) if it hasn't been rejected
            // by a validation error
            self.#delayedPromises.object.resolve(self.#bufferedObject as OUTPUT);
          }

          // If stream ends in suspended state (e.g., tool-call-approval), resolve promises with partial results
          // This allows consumers to access data that was produced before the suspension
          if (self.#status === 'suspended') {
            self.#resolveSuspendedPromises();
          }

          // If stream ends without proper finish/error chunks, reject unresolved promises
          // This must be in the final transformer flush to ensure
          // all of the delayed promises had a chance to resolve or reject already
          // Avoids promises hanging forever
          Object.entries(self.#delayedPromises).forEach(([key, promise]) => {
            if (promise.status.type === 'pending') {
              promise.reject(new Error(`promise '${key}' was not resolved or rejected when stream finished`));
            }
          });

          self.#closeTransportIfNeeded();

          // Emit finish event for EventEmitter streams
          self.#streamFinished = true;
          self.#emitter.emit('finish');
        },
      }),
    );

    if (initialState) {
      this.deserializeState(initialState);
    } else {
      if (initialToolCalls) this.#toolCalls = initialToolCalls;
      if (resumedText) {
        const { priorText, stepText } = resumedText;
        this.#resumedPriorText = priorText;
        this.#bufferedText = [priorText, stepText].filter(Boolean);
        this.#bufferedTextAtStepStart = priorText ? 1 : 0;
        this.#bufferedByStep.text = stepText;
        this.#wasSuspended = true;
      }
    }
  }

  private resolvePromise<KEY extends keyof PromiseResults<OUTPUT>>(key: KEY, value: PromiseResults<OUTPUT>[KEY]) {
    if (this.#suspendedUsageIsProvisional && (key === 'usage' || key === 'totalUsage' || key === 'finishReason')) {
      this.#suspendedUsageIsProvisional = false;
    }
    if (!(key in this.#delayedPromises)) {
      throw new MastraError({
        id: 'MASTRA_MODEL_OUTPUT_INVALID_PROMISE_KEY',
        domain: ErrorDomain.LLM,
        category: ErrorCategory.SYSTEM,
        text: `Attempted to resolve invalid promise key '${key}' with value '${typeof value === 'object' ? JSON.stringify(value, null, 2) : value}'`,
      });
    }
    this.#delayedPromises[key].resolve(value);
  }

  private resolvePromises(data: Partial<PromiseResults<OUTPUT>>) {
    for (const keyString in data) {
      const key = keyString as keyof PromiseResults<OUTPUT>;
      this.resolvePromise(key, data[key]);
    }
  }

  // Same numbers flush would settle when a plain stream ends on suspension. A durable
  // stream stays open, so callers waiting on usage would otherwise hang until resume.
  #settleSuspendedUsage() {
    const usage = this.#suspendedUsage();
    this.#delayedPromises.usage.resolve(usage);
    this.#delayedPromises.totalUsage.resolve(this.#getTotalUsage(usage));
    this.#delayedPromises.finishReason.resolve('suspended');
    this.#suspendedUsageIsProvisional = true;
  }

  #closeTransportIfNeeded() {
    const transport = this.#transportRef?.current as StreamTransport | undefined;
    if (!transport || !transport.closeOnFinish || this.#transportClosed) {
      return;
    }

    this.#transportClosed = true;
    try {
      transport.close();
    } catch {
      // best-effort close
    }
  }

  #getDelayedPromise<T>(promise: DelayedPromise<T>): Promise<T> {
    if (!this.#consumptionStarted) {
      this.consumeStream().catch(error => {
        this.logger?.error('Error consuming stream', error);
      });
    }
    return promise.promise;
  }

  /**
   * Resolves to the complete text response after streaming completes.
   */
  get text() {
    return this.#getDelayedPromise(this.#delayedPromises.text);
  }

  /**
   * Resolves to reasoning parts array for models that support reasoning.
   */
  get reasoning() {
    return this.#getDelayedPromise(this.#delayedPromises.reasoning);
  }

  /**
   * Resolves to complete reasoning text for models that support reasoning.
   */
  get reasoningText() {
    return this.#getDelayedPromise(this.#delayedPromises.reasoningText);
  }

  get sources() {
    return this.#getDelayedPromise(this.#delayedPromises.sources);
  }

  get files() {
    return this.#getDelayedPromise(this.#delayedPromises.files);
  }

  get steps() {
    return this.#getDelayedPromise(this.#delayedPromises.steps);
  }

  get suspendPayload() {
    return this.#getDelayedPromise(this.#delayedPromises.suspendPayload);
  }

  get resumeSchema() {
    return this.#getDelayedPromise(this.#delayedPromises.resumeSchema);
  }

  /**
   * Stream of all chunks. Provides complete control over stream processing.
   */
  get fullStream() {
    const stream = this.__getUnfilteredFullStream();
    const hideSignals = this.#options.hideSignals;
    if (!hideSignals || (Array.isArray(hideSignals) && hideSignals.length === 0)) return stream;
    return stream.pipeThrough(
      new TransformStream<ChunkType<OUTPUT>, ChunkType<OUTPUT>>({
        transform(chunk, controller) {
          if (!isSignalChunkExcluded(chunk, hideSignals)) controller.enqueue(chunk);
        },
      }),
    );
  }

  /** @internal Shared fanout must retain signal chunks, with the existing transforms applied. */
  __getUnfilteredFullStream() {
    const configuredTransforms = this.#options.experimentalTransform;
    if (!configuredTransforms) {
      return this.#createEventedStream();
    }

    const transforms = typeof configuredTransforms === 'function' ? [configuredTransforms] : configuredTransforms;
    let stream = this.#createEventedStream();
    for (const transform of transforms) {
      stream = stream.pipeThrough(transform());
    }
    return stream;
  }

  /**
   * Resolves to the reason generation finished.
   */
  get finishReason() {
    return this.#getDelayedPromise(this.#delayedPromises.finishReason);
  }

  /**
   * Resolves to array of all tool calls made during execution.
   */
  get toolCalls() {
    return this.#getDelayedPromise(this.#delayedPromises.toolCalls);
  }

  /**
   * Resolves to array of all tool execution results.
   */
  get toolResults() {
    return this.#getDelayedPromise(this.#delayedPromises.toolResults);
  }

  /**
   * Resolves to token usage statistics including inputTokens, outputTokens, and totalTokens.
   */
  get usage() {
    return this.#getDelayedPromise(this.#delayedPromises.usage);
  }

  /**
   * Resolves to array of all warnings generated during execution.
   */
  get warnings() {
    return this.#getDelayedPromise(this.#delayedPromises.warnings);
  }

  /**
   * Resolves to provider metadata generated during execution.
   */
  get providerMetadata() {
    return this.#getDelayedPromise(this.#delayedPromises.providerMetadata);
  }

  /**
   * Resolves to the complete response from the model.
   */
  get response() {
    return this.#getDelayedPromise(this.#delayedPromises.response);
  }

  /**
   * Resolves to the complete request sent to the model.
   */
  get request() {
    return this.#getDelayedPromise(this.#delayedPromises.request);
  }

  /**
   * Transport handle for the current stream (when available).
   */
  get transport(): StreamTransport | undefined {
    return this.#transportRef?.current as StreamTransport | undefined;
  }

  /**
   * Resolves to an error if an error occurred during streaming.
   */
  get error(): Error | undefined {
    return this.#error;
  }

  updateUsageCount(usage: Partial<LanguageModelUsage>) {
    if (!usage) {
      return;
    }
    accumulateUsage(this.#usageCount, this.#usageCountMissing, usage);
  }

  populateUsageCount(usage: Partial<LanguageModelUsage>) {
    if (!usage) {
      return;
    }

    // Finish metadata can fill untouched counters, but cannot repair a primary
    // counter already known to be incomplete from an earlier contributing step.
    for (const key of primaryUsageCountKeys) {
      const value = usage[key];
      if (value !== undefined && this.#usageCount[key] === undefined && !this.#usageCountMissing.has(key)) {
        this.#usageCount[key] = value;
      }
    }
    for (const key of detailUsageCountKeys) {
      const value = usage[key];
      if (value !== undefined && this.#usageCount[key] === undefined) {
        this.#usageCount[key] = value;
      }
    }
    if (usage.raw !== undefined && this.#usageCount.raw === undefined) {
      this.#usageCount.raw = usage.raw;
    }
  }

  async consumeStream(options?: ConsumeStreamOptions): Promise<void> {
    // Drain #baseStream exactly once but let every caller await full consumption,
    // so `await consumeStream()` holds regardless of who started it. Capture any
    // drain error once (don't bind a caller's onError to the drain), then fire each
    // caller's own onError after the shared promise settles.
    if (!this.#consumeStreamPromise) {
      this.#consumptionStarted = true;
      this.#consumeStreamPromise = consumeStream({
        stream: this.#baseStream as globalThis.ReadableStream<any>,
        onError: error => {
          const streamError = getErrorFromUnknown(error, { fallbackMessage: 'Unknown error consuming stream' });
          this.#consumeStreamErrored = true;
          this.#consumeStreamError = streamError;
          this.#error = streamError;
          this.#status = 'failed';
          this.#streamFinished = true;
          Object.values(this.#delayedPromises).forEach(promise => {
            if (promise.status.type === 'pending') {
              promise.reject(streamError);
            }
          });
          this.#closeTransportIfNeeded();
          this.#emitter.emit('stream-error', streamError);
          this.#emitter.emit('settled');
        },
        logger: this.logger,
      });
    }

    await this.#consumeStreamPromise;

    if (this.#consumeStreamErrored) {
      options?.onError?.(this.#consumeStreamError);
    }
  }

  /**
   * Returns complete output including text, usage, tool calls, and all metadata.
   */
  async getFullOutput(): Promise<FullOutput<OUTPUT>> {
    await this.consumeStream({
      onError: (error: unknown) => {
        this.logger.error('Error consuming stream', error);
        throw error;
      },
    });

    let scoringData:
      | {
          input: Omit<ScorerRunInputForAgent, 'runId'>;
          output: ScorerRunOutputForAgent;
        }
      | undefined;

    if (this.#returnScorerData) {
      try {
        scoringData = {
          input: {
            inputMessages: this.messageList.getPersisted.input.db(),
            rememberedMessages: this.messageList.getPersisted.remembered.db(),
            systemMessages: this.messageList.getSystemMessages(),
            taggedSystemMessages: this.messageList.getPersisted.taggedSystemMessages,
          },
          output: await this.messageList.getRunResponseMessages(this.#options.loadStoredMessages),
        };
      } catch (error) {
        this.logger.warn('Failed to load run response messages for scoring data', { error });
      }
    }

    const steps = await this.steps;

    // Calculate text from steps, which respects tripwire (rejected steps return empty text)
    // This ensures rejected responses are excluded from the final text output
    // A run that suspends mid-step (e.g. awaiting tool approval) has streamed text the step results
    // don't hold yet. The per-step buffer is reset on every step-finish, so this text is never in `steps`.
    const textFromSteps =
      steps.map((step: any) => step.text || '').join('') +
      (this.#status === 'suspended' ? this.#bufferedByStep.text : '');

    const fullOutput: FullOutput<OUTPUT> = {
      // After a tripwire, `text` already holds the resolved output (and a processOutputStream
      // tripwire can end the run before any step completes), so reuse it to stay in sync.
      text: this.tripwire || steps.length === 0 ? await this.text : this.#resumedPriorText + textFromSteps,
      usage: await this.usage,
      steps,
      finishReason: await this.finishReason,
      warnings: await this.warnings,
      providerMetadata: await this.providerMetadata,
      request: await this.request,
      reasoning: await this.reasoning,
      reasoningText: await this.reasoningText,
      toolCalls: await this.toolCalls,
      toolResults: await this.toolResults,
      sources: await this.sources,
      files: await this.files,
      response: await this.response,
      totalUsage: await this.totalUsage,
      object: await this.object,
      usedFallbackValue: this.#usedFallbackValue,
      error: this.error,
      tripwire: this.#tripwire,
      ...(scoringData ? { scoringData } : {}),
      traceId: this.traceId,
      spanId: this.spanId,
      runId: this.runId,
      suspendPayload: await this.suspendPayload,
      resumeSchema: await this.resumeSchema,
      // All messages from this execution (input + memory history + response)
      messages: this.messageList.get.all.db(),
      // Only messages loaded from memory (conversation history)
      rememberedMessages: this.messageList.get.remembered.db(),
      ...(this.#terminalToolResult ? { terminalToolResult: this.#terminalToolResult } : {}),
    };

    return fullOutput;
  }

  /**
   * Tripwire data if the stream was aborted due to an output processor blocking the content.
   * Returns undefined if no tripwire was triggered.
   */
  get tripwire(): StepTripwireData | undefined {
    return this.#tripwire;
  }

  /** The terminal tool result after its data part has been consumed, if present. */
  get terminalToolResult(): TerminalToolResult | undefined {
    return this.#terminalToolResult;
  }

  /**
   * The total usage of the stream.
   */
  get totalUsage() {
    return this.#getDelayedPromise(this.#delayedPromises.totalUsage);
  }

  get content(): Promise<LLMStepResult['content']> {
    return this.#getDelayedPromise(this.#delayedPromises.content);
  }

  /**
   * Stream of valid JSON chunks. The final JSON result is validated against the output schema when the stream ends.
   *
   * @example
   * ```typescript
   * const stream = await agent.stream("Extract data", {
   *   structuredOutput: {
   *     schema: z.object({ name: z.string(), age: z.number() }),
   *     model: 'gpt-4o-mini' // optional to use a model for structuring json output
   *   }
   * });
   * // partial json chunks
   * for await (const data of stream.objectStream) {
   *   console.log(data); // { name: 'John' }, { name: 'John', age: 30 }
   * }
   * ```
   */
  get objectStream() {
    return this.#createEventedStream().pipeThrough(
      new TransformStream<ChunkType<OUTPUT>, Partial<OUTPUT>>({
        transform(chunk, controller) {
          if (chunk.type === 'object') {
            controller.enqueue(chunk.object);
          }
        },
      }),
    );
  }

  /**
   * Stream of individual array elements when output schema is an array type.
   */
  get elementStream(): ReadableStream<OUTPUT extends Array<infer T> ? T : never> {
    let publishedElements = 0;
    let latestArray: unknown[] = [];

    return this.#createEventedStream().pipeThrough(
      new TransformStream<ChunkType<OUTPUT>, OUTPUT extends Array<infer T> ? T : never>({
        transform(chunk, controller) {
          if (chunk.type === 'object') {
            if (Array.isArray(chunk.object)) {
              latestArray = chunk.object;
              // The trailing element may still be partial (a later chunk can
              // complete it in place), so only publish elements followed by another.
              for (; publishedElements < chunk.object.length - 1; publishedElements++) {
                controller.enqueue(chunk.object[publishedElements]);
              }
            }
          }
        },
        flush(controller) {
          for (; publishedElements < latestArray.length; publishedElements++) {
            controller.enqueue(latestArray[publishedElements] as OUTPUT extends Array<infer T> ? T : never);
          }
        },
      }),
    );
  }

  /**
   * Stream of only text content, filtering out metadata and other chunk types.
   */
  get textStream() {
    if (this.#structuredOutputMode === 'direct') {
      const outputSchema = getTransformedSchema(this.#options.structuredOutput?.schema);
      if (outputSchema?.outputFormat === 'array') {
        return this.#createEventedStream().pipeThrough(
          createJsonTextStreamTransformer(this.#options.structuredOutput?.schema),
        );
      }
    }

    return this.#createEventedStream().pipeThrough(
      new TransformStream<ChunkType<OUTPUT>, string>({
        transform(chunk, controller) {
          if (chunk.type === 'text-delta') {
            controller.enqueue(chunk.payload.text);
          }
        },
      }),
    );
  }

  /**
   * Resolves to the complete object response from the model. Validated against the 'output' schema when the stream ends.
   *
   * @example
   * ```typescript
   * const stream = await agent.stream("Extract data", {
   *   structuredOutput: {
   *     schema: z.object({ name: z.string(), age: z.number() }),
   *     model: 'gpt-4o-mini' // optionally use a model for structuring json output
   *   }
   * });
   * // final validated json
   * const data = await stream.object // { name: 'John', age: 30 }
   * ```
   */
  get object() {
    if (
      !this.processorRunner &&
      !this.#options.structuredOutput?.schema &&
      this.#delayedPromises.object.status.type === 'pending'
    ) {
      this.#delayedPromises.object.resolve(undefined as OUTPUT);
    }

    return this.#getDelayedPromise(this.#delayedPromises.object);
  }

  // Internal methods for immediate values - used internally by Mastra (llm-execution.ts bailing on errors/abort signals with current state)
  // These are not part of the public API
  /** @internal */
  _getImmediateToolCalls() {
    return this.#toolCalls;
  }
  /** @internal */
  _getImmediateToolResults() {
    return this.#toolResults;
  }
  /** @internal */
  _getImmediateText() {
    return this.#bufferedText.join('');
  }

  /** @internal */
  _getImmediateObject() {
    return this.#bufferedObject;
  }

  /**
   * Whether the structured object is the configured `fallbackValue`, substituted because
   * the model output failed schema validation — or the separate structuring model failed —
   * under `errorStrategy: 'fallback'`.
   *
   * Starts `false` and reflects the most recently processed object. On a live stream, await
   * `stream.object` or `stream.getFullOutput()` before reading it.
   */
  get usedFallbackValue(): boolean {
    return this.#usedFallbackValue;
  }
  /** @internal */
  _getImmediateUsage() {
    return this.#usageCount;
  }
  /** @internal */
  _getImmediateWarnings() {
    return this.#warnings;
  }
  /** @internal */
  _getImmediateFinishReason() {
    return this.#finishReason;
  }
  /** @internal */
  _getImmediateProviderMetadata() {
    return this.#stepProviderMetadata;
  }
  /** @internal  */
  _getBaseStream() {
    // The caller now owns the base stream's reader; delayed-promise getters must
    // not try to drain it a second time (that would throw "ReadableStream is locked").
    this.#consumptionStarted = true;
    return this.#baseStream;
  }

  /**
   * @internal Register a listener for the run's provider result: a
   * `FullOutput`-shaped snapshot of the final output as the provider finished
   * it, awaited at the final `finish` chunk before output result processors
   * (memory persistence), `onFinish`, and the finish chunk's delivery. It fires
   * at most once, only for a run that is still successful at that point. A
   * listener that rejects fails the run before any output result processor
   * runs, so nothing persists a reply the listener could not secure; one that
   * rejects with a `ProviderResultCancelledError` ends the run cancelled. The snapshot has no
   * structured `object` and carries the text before output result processors.
   * Returns `false` when the run's finish already started, so the caller must
   * rely on the completed output instead.
   */
  _onProviderResult(listener: (result: FullOutput<OUTPUT>) => Promise<void>): boolean {
    if (this.#providerResultObserved || this.#streamFinished) return false;
    this.#providerResultListeners.push(listener);
    return true;
  }

  async #publishProviderResult(input: {
    response: LLMStepResult<OUTPUT>['response'];
    providerMetadata: ProviderMetadata | undefined;
    terminalToolResult: TerminalToolResult | undefined;
  }): Promise<void> {
    if (this.#providerResultObserved) return;
    this.#providerResultObserved = true;
    const listeners = this.#providerResultListeners;
    this.#providerResultListeners = [];
    if (listeners.length === 0 || this.#status !== 'success') return;
    const steps = [...this.#bufferedSteps];
    const snapshot = {
      // Same derivation as getFullOutput(): rejected steps carry no text.
      text: steps.map(step => step.text || '').join(''),
      usage: { ...this.#usageCount },
      steps,
      finishReason: this.#finishReason,
      warnings: [...this.#warnings],
      providerMetadata: input.providerMetadata,
      request: this.#request || {},
      reasoning: Object.values(this.#bufferedReasoningDetails || {}),
      reasoningText:
        this.#bufferedReasoning.length > 0
          ? this.#bufferedReasoning.map(reasoningPart => reasoningPart.payload.text).join('')
          : undefined,
      toolCalls: [...this.#toolCalls],
      toolResults: [...this.#toolResults],
      sources: [...this.#bufferedSources],
      files: [...this.#bufferedFiles],
      response: input.response,
      totalUsage: this.#getTotalUsage(),
      object: undefined as OUTPUT,
      usedFallbackValue: this.#usedFallbackValue,
      error: undefined,
      tripwire: this.#tripwire,
      traceId: this.traceId,
      spanId: this.spanId,
      runId: this.runId,
      suspendPayload: undefined,
      resumeSchema: undefined,
      messages: [],
      rememberedMessages: [],
      ...(input.terminalToolResult !== undefined ? { terminalToolResult: input.terminalToolResult } : {}),
    } satisfies FullOutput<OUTPUT>;
    // Output result processors mutate the step objects only after every
    // listener settled; storage serializes what it persists.
    const result: FullOutput<OUTPUT> = snapshot;
    // The first rejection fails the run.
    const settled = await Promise.allSettled(listeners.map(async listener => listener(result)));
    const rejected = settled.find((outcome): outcome is PromiseRejectedResult => outcome.status === 'rejected');
    if (rejected !== undefined) {
      this.logger?.error?.('Provider result listener failed; failing the run', {
        error: rejected.reason,
        runId: this.runId,
      });
      throw rejected.reason;
    }
  }

  /** @internal */
  _waitUntilFinished() {
    if (this.#streamFinished) {
      return Promise.resolve();
    }

    return new Promise<void>(resolve => {
      const done = () => {
        this.#emitter.off('finish', done);
        this.#emitter.off('settled', done);
        resolve();
      };
      // 'finish' fires when the stream flushes cleanly; 'settled' fires when an
      // error chunk marks the stream finished without closing it (observer
      // streams must survive an error chunk for fallback recovery, so the
      // error path cannot emit 'finish').
      this.#emitter.once('finish', done);
      this.#emitter.once('settled', done);
    });
  }

  #getTotalUsage(usage: LanguageModelUsage = this.#usageCount): LanguageModelUsage {
    let total = usage.totalTokens;

    if (total === undefined && usage.inputTokens !== undefined && usage.outputTokens !== undefined) {
      total = usage.inputTokens + usage.outputTokens;
    }

    return {
      inputTokens: usage.inputTokens,
      outputTokens: usage.outputTokens,
      totalTokens: total,
      reasoningTokens: usage.reasoningTokens,
      cachedInputTokens: usage.cachedInputTokens,
      cacheCreationInputTokens: usage.cacheCreationInputTokens,
      cacheCreationInputTokens5m: usage.cacheCreationInputTokens5m,
      cacheCreationInputTokens1h: usage.cacheCreationInputTokens1h,
      ...(usage.raw !== undefined && { raw: usage.raw }),
    };
  }

  /** Completed-step usage plus the billed step that suspended (see `#pendingSuspendedStepUsage`). */
  #suspendedUsage(
    pendingSuspendedStepUsage: PendingStepUsage | undefined = this.#pendingSuspendedStepUsage,
  ): LanguageModelUsage {
    return addPendingStepUsage(this.#usageCount, pendingSuspendedStepUsage);
  }

  /**
   * @internal Record the usage of the model step whose tool call is suspending
   * the run. The first record for a pending step wins: parallel suspensions of
   * one step, and a cold resume that suspends again, report it once.
   */
  _recordPendingSuspendedStepUsage(usage: PendingStepUsage | undefined): void {
    if (usage !== undefined) this.#pendingSuspendedStepUsage ??= usage;
  }

  #createAbortedOnFinishPayload(): MastraOnFinishCallbackArgs<OUTPUT> {
    // Capture only text buffered before the abort event. Providers may continue producing
    // chunks after cancellation, but those chunks cannot extend this terminal snapshot.
    const emptyUsage = { inputTokens: undefined, outputTokens: undefined, totalTokens: undefined };
    // Steps that completed before the abort were already billed by the provider, so keep their usage.
    return {
      finishReason: 'aborted',
      text: this.#bufferedText.join(''),
      reasoning: [],
      reasoningText: undefined,
      sources: [],
      files: [],
      toolCalls: [],
      toolResults: [],
      staticToolCalls: [],
      staticToolResults: [],
      dynamicToolCalls: [],
      dynamicToolResults: [],
      content: [],
      usage: this.#lastStepUsage ?? emptyUsage,
      warnings: [],
      providerMetadata: undefined,
      request: {},
      response: {},
      steps: [],
      totalUsage: this.#lastStepUsage ? this.#getTotalUsage() : emptyUsage,
      ...(this.#generationReportedUsage && { generationReportedUsage: this.#generationReportedUsage }),
      object: undefined as OUTPUT,
    };
  }

  #createSuspendedOnFinishPayload(
    chunk: Extract<ChunkType<OUTPUT>, { type: 'tool-call-approval' | 'tool-call-suspended' }>,
  ): MastraOnFinishCallbackArgs<OUTPUT> & {
    suspendReason: 'tool-call-approval' | 'tool-call-suspended';
    toolName: string;
    toolCallId: string;
  } {
    // Suspend flow invokes options?.onFinish so map-results-step.ts can close the AGENT_RUN span
    // and the MODEL_GENERATION span (which records the usage below). The remaining
    // LLMStepResult fields are empty defaults to satisfy the MastraOnFinishCallback shape without
    // reconstructing partial buffered text/tool/message state from a half-finished stream.
    const totalUsage = this.#getTotalUsage(this.#suspendedUsage());
    const generationReportedUsage = this.#generationReportedUsage;
    this.#generationReportedUsage = totalUsage;
    return {
      finishReason: 'suspended',
      suspendReason: chunk.type,
      toolName: chunk.payload.toolName,
      toolCallId: chunk.payload.toolCallId,
      // Empty defaults for the LLMStepResult/MastraOnFinishCallback shape.
      text: '',
      reasoning: [],
      reasoningText: undefined,
      sources: [],
      files: [],
      toolCalls: [],
      toolResults: [],
      staticToolCalls: [],
      staticToolResults: [],
      dynamicToolCalls: [],
      dynamicToolResults: [],
      content: [],
      usage: this.#suspendedUsage(),
      warnings: [],
      providerMetadata: undefined,
      request: {},
      response: {},
      steps: [],
      totalUsage,
      ...(generationReportedUsage && { generationReportedUsage }),
      object: undefined as OUTPUT,
    };
  }

  /** Resolves the result promises with what the run produced before it suspended. */
  #resolveSuspendedPromises() {
    const reasoningText =
      this.#bufferedReasoning.length > 0
        ? this.#bufferedReasoning.map(reasoningPart => reasoningPart.payload.text).join('')
        : undefined;

    this.resolvePromises({
      toolResults: this.#toolResults,
      toolCalls: this.#toolCalls,
      text: this.#bufferedText.join(''),
      reasoning: Object.values(this.#bufferedReasoningDetails || {}),
      reasoningText,
      sources: this.#bufferedSources,
      files: this.#bufferedFiles,
      steps: this.#bufferedSteps,
      usage: this.#suspendedUsage(),
      totalUsage: this.#getTotalUsage(this.#suspendedUsage()),
      warnings: this.#warnings,
      finishReason: 'suspended',
      content: this.messageList.get.response.aiV5.stepContent(),
      object: undefined,
      request: this.#request,
      response: {
        dbMessages: this.messageList.get.response.db(),
      },
      providerMetadata: undefined,
    });
  }

  #emitChunk(chunk: ChunkType<OUTPUT>) {
    if (getChunkProducedAt(chunk) === undefined) stampChunkProducedAt(chunk, Date.now());
    // Carry the traceId on every chunk Mastra produces so stream consumers can link chunks to their
    // trace. Custom data-* chunks are left as written: their shape belongs to the user, and AI SDK
    // UI streams forward them as-is.
    if (this.traceId && !isDataChunk(chunk) && !chunk.traceId) {
      chunk.traceId = this.traceId;
    }
    this.#bufferedChunks.push(chunk); // add to bufferedChunks for replay in new streams
    this.#emitter.emit('chunk', chunk); // emit chunk for existing listener streams
  }

  /**
   * Text produced by the run: the last step's text for tool-driven multi-step runs (excluding
   * pre-tool narration), otherwise the aggregate stream text. Suspended/resumed runs keep the
   * aggregate because pre-approval text is part of the resumed run.
   */
  #producedText(): string {
    const lastStep = this.#bufferedSteps[this.#bufferedSteps.length - 1];
    const hasToolStep = this.#bufferedSteps.some(step => step.toolCalls.length > 0 || step.toolResults.length > 0);
    // Durable reads its final text from the steps, where a retried attempt's text is empty,
    // plus the text of a step that never finished (an aborted run).
    if (!hasToolStep && !this.#wasSuspended && this.#options.resolveFinalPromises) {
      return this.#bufferedSteps.map(step => step.text).join('') + this.#bufferedByStep.text;
    }
    // A step a processor tripwire rejected keeps its empty step text, and the run's
    // text must not disclose the rejected reply either (#26048, #24443): read the
    // text from the steps, as durable does, once any step was rejected.
    if (!hasToolStep && !this.#wasSuspended && this.#bufferedSteps.some(step => step.tripwire !== undefined)) {
      return this.#bufferedSteps.map(step => step.text).join('');
    }
    return hasToolStep && !this.#wasSuspended && lastStep ? lastStep.text : this.#bufferedText.join('');
  }

  /**
   * Drops all run-lifetime accumulators. Called at goal-evaluation boundaries,
   * where every completed step has already been persisted to the MessageList.
   * After truncation, run-end results (`text`, `steps`, `toolCalls`, …,
   * `getFullOutput()`) cover only the segment after the last evaluation —
   * for goal runs that segment is the completion answer. Token usage
   * (`#usageCount`), in-flight per-step state (`#bufferedByStep`), structured
   * output, and the MessageList are intentionally untouched.
   */
  #truncateRunBuffers() {
    this.#bufferedChunks.length = 0;
    this.#bufferedSteps.length = 0;
    this.#bufferedText.length = 0;
    this.#resumedPriorText = '';
    this.#bufferedTextAtStepStart = 0;
    this.#bufferedTextChunks = {};
    this.#bufferedSources.length = 0;
    this.#bufferedReasoning.length = 0;
    this.#bufferedReasoningDetails = {};
    this.#reasoningAtStepStart = undefined;
    this.#bufferedFiles.length = 0;
    this.#toolCalls.length = 0;
    this.#toolResults.length = 0;
    this.#toolCallArgsDeltas = Object.create(null);
    this.#toolCallDeltaIdNameMap = Object.create(null);
    this.#toolCallStreamingMeta = Object.create(null);
    this.#pendingToolCalls = Object.create(null);
    this.#replaceableToolCallIds.clear();
    this.#attemptToolCallIds.clear();
  }

  #createEventedStream() {
    const self = this;
    // Holds this subscriber's own detach function once start() registers its
    // listeners. cancel() must only remove this subscriber's handlers — the
    // emitter is shared across every concurrent consumer of this output, so
    // removeAllListeners() here would silently kill every other subscriber
    // (see #19743).
    let detach: (() => void) | undefined;
    // Erroring a ReadableStream discards the chunks still queued for its reader, so a failure that
    // arrives while this subscriber is behind waits in `pendingError` until `pull` sees the queue empty.
    let pendingError: { error: unknown } | undefined;
    const fail = (controller: ReadableStreamDefaultController<ChunkType<OUTPUT>>, error: unknown) => {
      if ((controller.desiredSize ?? 0) > 0) {
        controller.error(error);
      } else {
        pendingError = { error };
      }
    };

    return new ReadableStream<ChunkType<OUTPUT>>({
      start(controller) {
        // Replay existing buffered chunks
        self.#bufferedChunks.forEach(chunk => {
          controller.enqueue(chunk);
        });

        // If stream already finished, close immediately
        if (self.#streamFinished) {
          if (self.#consumeStreamErrored) {
            fail(controller, self.#consumeStreamError);
          } else {
            controller.close();
          }
          return;
        }

        // Listen for new chunks and stream finish
        const chunkHandler = (chunk: ChunkType<OUTPUT>) => {
          safeEnqueue(controller, chunk);
        };

        const detachListeners = () => {
          self.#emitter.off('chunk', chunkHandler);
          self.#emitter.off('finish', finishHandler);
          self.#emitter.off('stream-error', errorHandler);
        };
        const finishHandler = () => {
          detachListeners();
          safeClose(controller);
        };
        const errorHandler = (error: unknown) => {
          detachListeners();
          fail(controller, error);
        };

        self.#emitter.on('chunk', chunkHandler);
        self.#emitter.on('finish', finishHandler);
        self.#emitter.on('stream-error', errorHandler);

        detach = detachListeners;
      },

      pull(controller) {
        if (pendingError) {
          controller.error(pendingError.error);
          return;
        }
        // Only start consumption when someone is actively reading the stream
        if (!self.#consumptionStarted) {
          self.consumeStream().catch(error => {
            self.logger?.error('Error consuming stream', error);
          });
        }
      },

      cancel() {
        // Only detach this subscriber's own listeners — never the whole emitter.
        detach?.();
      },
    });
  }

  get status() {
    return this.#status;
  }

  /**
   * @param pendingSuspension A suspension chunk the step has emitted but this output may not have
   * processed yet. Tool suspension snapshots the stream state right after emitting the chunk, so
   * the state is serialized as if the chunk was processed; otherwise a resumed run would not know
   * which usage its suspended leg already reported.
   *
   * Fork (PF-5349, kept by the PF-5928 sync decision): the suspending step's usage reaches this
   * output out of band through `_recordPendingSuspendedStepUsage` before the snapshot, never on
   * the chunk; a `usage` passed here only fills that slot when nothing was recorded.
   */
  serializeState(pendingSuspension?: { usage?: LanguageModelUsage }) {
    const { steps, requests } = dedupeStepRequests(packStepMessageMirrors(this.#bufferedSteps));
    const recordSuspension = pendingSuspension !== undefined;
    const pendingSuspendedStepUsage = this.#pendingSuspendedStepUsage ?? toPendingStepUsage(pendingSuspension?.usage);
    // Processing the suspension chunk records exactly this generation usage (see
    // `#createSuspendedOnFinishPayload`), so the snapshot is the same whether or not the output
    // has processed the chunk yet (upstream #27634).
    const generationReportedUsage = recordSuspension
      ? this.#getTotalUsage(this.#suspendedUsage(pendingSuspendedStepUsage))
      : this.#generationReportedUsage;
    return {
      status: recordSuspension ? 'suspended' : this.#status,
      bufferedSteps: steps,
      bufferedStepRequests: requests,
      bufferedReasoningDetails: this.#bufferedReasoningDetails,
      bufferedByStepReasoningDetails: this.#bufferedByStepReasoningDetails,
      bufferedByStep: this.#bufferedByStep,
      bufferedText: this.#bufferedText,
      bufferedTextAtStepStart: this.#bufferedTextAtStepStart,
      bufferedTextChunks: this.#bufferedTextChunks,
      bufferedSources: this.#bufferedSources,
      bufferedReasoning: this.#bufferedReasoning,
      bufferedFiles: this.#bufferedFiles,
      toolCallArgsDeltas: this.#toolCallArgsDeltas,
      toolCallDeltaIdNameMap: this.#toolCallDeltaIdNameMap,
      toolCallStreamingMeta: this.#toolCallStreamingMeta,
      pendingToolCalls: this.#pendingToolCalls,
      replaceableToolCallIds: [...this.#replaceableToolCallIds],
      attemptToolCallIds: [...this.#attemptToolCallIds],
      toolCalls: this.#toolCalls,
      toolResults: this.#toolResults,
      warnings: this.#warnings,
      finishReason: this.#finishReason,
      request: this.#request,
      usageCount: this.#usageCount,
      usageCountMissing: [...this.#usageCountMissing],
      lastStepUsage: this.#lastStepUsage,
      generationReportedUsage,
      tripwire: this.#tripwire,
      wasSuspended: this.#wasSuspended || recordSuspension,
      terminalToolResult: this.#terminalToolResult,
      loopStepCount: this.#loopStepCount,
      ...(pendingSuspendedStepUsage ? { pendingSuspendedStepUsage } : {}),
      messageList: this.messageList.serialize(),
    };
  }

  deserializeState(state: any) {
    this.#status = state.status;
    this.#bufferedReasoningDetails = state.bufferedReasoningDetails;
    this.#bufferedByStepReasoningDetails = state.bufferedByStepReasoningDetails ?? {};
    this.#bufferedByStep = state.bufferedByStep;
    this.#bufferedText = state.bufferedText;
    this.#bufferedTextAtStepStart = state.bufferedTextAtStepStart ?? state.bufferedText.length;
    this.#bufferedTextChunks = state.bufferedTextChunks;
    this.#bufferedSources = state.bufferedSources;
    this.#bufferedReasoning = state.bufferedReasoning;
    this.#bufferedFiles = state.bufferedFiles;
    this.#toolCallArgsDeltas = Object.assign(Object.create(null), state.toolCallArgsDeltas);
    this.#toolCallDeltaIdNameMap = Object.assign(Object.create(null), state.toolCallDeltaIdNameMap);
    this.#toolCallStreamingMeta = Object.assign(Object.create(null), state.toolCallStreamingMeta);
    this.#pendingToolCalls = Object.assign(Object.create(null), state.pendingToolCalls);
    this.#replaceableToolCallIds = new Set(state.replaceableToolCallIds ?? []);
    this.#attemptToolCallIds = new Set(state.attemptToolCallIds ?? []);
    this.#toolCalls = state.toolCalls;
    this.#toolResults = state.toolResults;
    this.#warnings = state.warnings;
    this.#finishReason = state.finishReason;
    this.#request = state.request;
    this.#usageCount = state.usageCount;
    this.#pendingSuspendedStepUsage = toPendingStepUsage(state.pendingSuspendedStepUsage);

    if (state.usageCountMissing === undefined) {
      const hasPriorUsage =
        (Array.isArray(state.bufferedSteps) && state.bufferedSteps.length > 0) ||
        primaryUsageCountKeys.some(key => state.usageCount?.[key] !== undefined);

      // Legacy snapshots do not record which completed steps omitted primary
      // usage counters, so existing primary aggregates must fail closed. An
      // empty snapshot taken before the first step can still accumulate them.
      this.#usageCountMissing = hasPriorUsage ? new Set(primaryUsageCountKeys) : new Set();
      if (hasPriorUsage) {
        for (const key of primaryUsageCountKeys) {
          this.#usageCount[key] = undefined;
        }
      }
    } else {
      this.#usageCountMissing = new Set(state.usageCountMissing);
    }

    this.#tripwire = state.tripwire;
    this.#lastStepUsage = state.lastStepUsage ?? state.bufferedSteps?.at(-1)?.usage;
    this.#generationReportedUsage = state.generationReportedUsage;
    this.#wasSuspended = state.wasSuspended ?? state.status === 'suspended';
    this.#terminalToolResult = state.terminalToolResult
      ? materializeTerminalToolResult(state.terminalToolResult)
      : undefined;
    this.messageList = this.messageList.deserialize(state.messageList);
    // Buffered steps hold only the IDs of the response messages they had seen;
    // rebuilding their lazy mirrors needs the restored message list, so this has
    // to come last.
    this.#bufferedSteps = unpackStepMessageMirrors(
      rehydrateStepRequests(state.bufferedSteps, state.bufferedStepRequests),
      this.messageList,
    );
    this.#loopStepCount =
      typeof state.loopStepCount === 'number' ? state.loopStepCount : countLoopSteps(this.#bufferedSteps);
  }
}
