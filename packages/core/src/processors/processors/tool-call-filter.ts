import type { LanguageModelV2Prompt } from '@ai-sdk/provider-v5';

import type { ProcessLLMRequestArgs, ProcessLLMRequestResult, Processor } from '../index';
import { normalizeToolCallFilterExclude, normalizeToolCallFilterMaxModelOutputBytes } from '../tool-call-filter-utils';
import type { ToolCallFilteringOptions } from '../tool-call-filter-utils';

type PromptMessage = LanguageModelV2Prompt[number];
type PromptPart = Extract<PromptMessage, { role: 'assistant' }>['content'][number];
type ToolCallPart = Extract<PromptPart, { type: 'tool-call' }>;
type ToolResultPart = Extract<PromptPart, { type: 'tool-result' }>;

/** Per-request state key holding the tool call ids present before the loop started. */
const HISTORY_TOOL_CALL_IDS = '__toolCallFilterHistoryToolCallIds';

const MODEL_OUTPUT_TRUNCATION_SUFFIX = '\n[truncated]';

/**
 * Resolve which tools may keep compact model output when their raw payload is
 * filtered. Mirrors the shared persistence-side normalization in
 * `../tool-call-filter-utils`: an explicit allowlist always wins over the
 * legacy boolean, and an empty allowlist preserves nothing.
 */
function normalizePreservedModelOutputTools(
  preserveModelOutput: boolean | undefined,
  preserveModelOutputFor: unknown,
): string[] | 'all' {
  if (preserveModelOutputFor === undefined) return preserveModelOutput ? 'all' : [];
  if (!Array.isArray(preserveModelOutputFor) || preserveModelOutputFor.some(toolName => typeof toolName !== 'string')) {
    throw new TypeError('Tool call filter options.preserveModelOutputFor must be an array of strings when provided');
  }
  return preserveModelOutputFor;
}

/** Bound preserved model output to a UTF-8 byte budget, marking any truncation. */
function truncateUtf8(text: string, maxBytes: number | undefined): string | null {
  if (maxBytes === undefined) return text;

  const encoder = new TextEncoder();
  const encoded = encoder.encode(text);
  if (encoded.byteLength <= maxBytes) return text;

  const suffix = encoder.encode(MODEL_OUTPUT_TRUNCATION_SUFFIX);
  if (suffix.byteLength > maxBytes) return null;

  let prefixEnd = maxBytes - suffix.byteLength;
  while (prefixEnd > 0 && prefixEnd < encoded.byteLength && (encoded[prefixEnd]! & 0xc0) === 0x80) {
    prefixEnd -= 1;
  }

  const prefix = new TextDecoder().decode(encoded.subarray(0, prefixEnd));
  return `${prefix}${MODEL_OUTPUT_TRUNCATION_SUFFIX}`;
}

/**
 * Traversal bounds mirroring the fork's persistence-side converter in
 * `../tool-call-filter-utils`, so a cyclic or pathological payload can never hang
 * a request or blow the stack while it is being turned into prompt text.
 */
const MAX_MODEL_OUTPUT_TRAVERSAL_DEPTH = 64;
const MAX_MODEL_OUTPUT_TRAVERSAL_NODES = 10_000;

type ModelOutputTraversal = {
  seen: WeakSet<object>;
  depth: number;
  nodes: number;
  exhausted: boolean;
};

function boundText(text: string, maxCodeUnits: number | undefined): string {
  return maxCodeUnits === undefined ? text : text.slice(0, maxCodeUnits);
}

/** Accumulates text without ever materializing more than `maxCodeUnits` characters. */
class BoundedText {
  private readonly chunks: string[] = [];
  private written = 0;

  constructor(private readonly maxCodeUnits: number | undefined) {}

  get remaining(): number | undefined {
    return this.maxCodeUnits === undefined ? undefined : Math.max(0, this.maxCodeUnits - this.written);
  }

  get isFull(): boolean {
    return this.maxCodeUnits !== undefined && this.written >= this.maxCodeUnits;
  }

  push(text: string): void {
    const bounded = boundText(text, this.remaining);
    if (bounded.length === 0) return;
    this.chunks.push(bounded);
    this.written += bounded.length;
  }

  toString(): string {
    return this.chunks.join('');
  }
}

function createModelOutputTraversal(): ModelOutputTraversal {
  return { seen: new WeakSet<object>(), depth: 0, nodes: 0, exhausted: false };
}

/** Charge one node against the traversal budget. Returns false once the budget is spent. */
function visitModelOutputNode(traversal: ModelOutputTraversal): boolean {
  if (traversal.nodes >= MAX_MODEL_OUTPUT_TRAVERSAL_NODES) {
    traversal.exhausted = true;
    return false;
  }
  traversal.nodes += 1;
  return true;
}

/**
 * True when `value` is a plain, cycle-free JSON value inside the traversal bounds.
 * Class instances, `toJSON` hooks, cycles and oversized graphs are rejected so they
 * are never serialized into the prompt.
 */
function isBoundedJsonValue(value: unknown, traversal: ModelOutputTraversal): boolean {
  if (!visitModelOutputNode(traversal)) return false;
  if (value === null) return true;

  const valueType = typeof value;
  if (valueType === 'string' || valueType === 'number' || valueType === 'boolean') return true;
  if (valueType !== 'object') return false;

  const object = value as object;
  if (traversal.seen.has(object) || traversal.depth >= MAX_MODEL_OUTPUT_TRAVERSAL_DEPTH) return false;

  const prototype = Object.getPrototypeOf(object);
  if (!Array.isArray(object) && prototype !== Object.prototype && prototype !== null) return false;
  // A `toJSON` hook runs arbitrary code during serialization and may return anything.
  if ('toJSON' in object) return false;

  traversal.seen.add(object);
  traversal.depth += 1;
  try {
    const entries = Array.isArray(object) ? object : Object.values(object as Record<string, unknown>);
    return entries.every(entry => isBoundedJsonValue(entry, traversal));
  } finally {
    traversal.depth -= 1;
    traversal.seen.delete(object);
  }
}

/** Serialize an already-validated JSON value, stopping as soon as the text budget is spent. */
function appendBoundedJson(builder: BoundedText, value: unknown): void {
  if (value === null || typeof value !== 'object') {
    builder.push(JSON.stringify(value) ?? 'null');
    return;
  }

  if (Array.isArray(value)) {
    builder.push('[');
    for (let index = 0; index < value.length; index += 1) {
      if (index > 0) builder.push(',');
      if (builder.isFull) break;
      appendBoundedJson(builder, value[index]);
    }
    builder.push(']');
    return;
  }

  builder.push('{');
  let first = true;
  for (const [key, entry] of Object.entries(value as Record<string, unknown>)) {
    if (!first) builder.push(',');
    first = false;
    if (builder.isFull) break;
    builder.push(`${JSON.stringify(key)}:`);
    appendBoundedJson(builder, entry);
  }
  builder.push('}');
}

function boundedJsonStringify(
  value: unknown,
  maxCodeUnits: number | undefined,
  traversal: ModelOutputTraversal,
): string | null {
  if (!isBoundedJsonValue(value, traversal)) return null;

  const builder = new BoundedText(maxCodeUnits);
  appendBoundedJson(builder, value);
  const text = builder.toString();
  return text.length > 0 ? text : null;
}

/** Join converted parts with newlines, honouring the shared text budget and failing closed on exhaustion. */
function joinModelOutputParts(
  entries: readonly unknown[],
  maxTextCodeUnits: number | undefined,
  traversal: ModelOutputTraversal,
  convert: (entry: unknown, remaining: number | undefined) => string | null,
): string | null {
  const builder = new BoundedText(maxTextCodeUnits);
  let wrote = false;

  for (const entry of entries) {
    if (builder.isFull) break;
    const converted = convert(entry, builder.remaining);
    if (traversal.exhausted) return null;
    if (!converted) continue;
    if (wrote) builder.push('\n');
    builder.push(converted);
    wrote = true;
  }

  const text = builder.toString();
  return wrote && text.length > 0 ? text : null;
}

function contentOutputToText(
  value: unknown,
  maxTextCodeUnits: number | undefined,
  traversal: ModelOutputTraversal,
): string | null {
  if (!Array.isArray(value)) return null;
  if (!visitModelOutputNode(traversal)) return null;

  return joinModelOutputParts(value, maxTextCodeUnits, traversal, (part, remaining) => {
    if (!visitModelOutputNode(traversal)) return null;
    if (!part || typeof part !== 'object') return null;

    const contentPart = part as Record<string, unknown>;
    // Media parts carry base-64 payloads the model already received as media, so they
    // are dropped instead of being replayed into the prompt as text.
    if (contentPart.type !== 'text') return null;

    const text = typeof contentPart.text === 'string' ? contentPart.text : contentPart.value;
    return typeof text === 'string' ? boundText(text, remaining) : null;
  });
}

/**
 * Convert model-facing tool output into text without serializing media or unknown shapes.
 *
 * Prompt-level port of the fork's fail-closed converter in `../tool-call-filter-utils`
 * (PF-1682). Recognized shapes mirror `LanguageModelV2ToolResultOutput`; primitive and
 * array handling remain for the legacy `providerMetadata.mastra.modelOutput` payloads
 * that `MessageList.get.llmPrompt` substitutes into `output`. Media parts, `toJSON`
 * hooks, cycles and unknown provider-specific shapes are omitted rather than
 * stringified, and the text is bounded while it is built rather than afterwards.
 */
function modelOutputToText(
  modelOutput: unknown,
  maxTextCodeUnits: number | undefined,
  traversal: ModelOutputTraversal = createModelOutputTraversal(),
): string | null {
  if (!visitModelOutputNode(traversal)) return null;

  if (typeof modelOutput === 'string') return boundText(modelOutput, maxTextCodeUnits);
  if (typeof modelOutput === 'number' || typeof modelOutput === 'boolean' || typeof modelOutput === 'bigint') {
    return boundText(String(modelOutput), maxTextCodeUnits);
  }
  if (!modelOutput || typeof modelOutput !== 'object') return null;
  if (traversal.seen.has(modelOutput) || traversal.depth >= MAX_MODEL_OUTPUT_TRAVERSAL_DEPTH) return null;

  traversal.seen.add(modelOutput);
  traversal.depth += 1;
  try {
    if (Array.isArray(modelOutput)) {
      return joinModelOutputParts(modelOutput, maxTextCodeUnits, traversal, (entry, remaining) =>
        modelOutputToText(entry, remaining, traversal),
      );
    }

    const output = modelOutput as Record<string, unknown>;
    switch (output.type) {
      case 'text':
      case 'error-text': {
        if (typeof output.value === 'string') return boundText(output.value, maxTextCodeUnits);
        if (typeof output.text === 'string') return boundText(output.text, maxTextCodeUnits);
        return null;
      }
      case 'json':
      case 'error-json':
        return boundedJsonStringify(output.value, maxTextCodeUnits, traversal);
      case 'content':
        return contentOutputToText(output.value, maxTextCodeUnits, traversal);
      default: {
        // Keep the two legacy wrapper shapes ToolCallFilter already accepted, but never
        // stringify arbitrary objects. This fails closed for media and unknown
        // provider-specific output.
        if (typeof output.text === 'string') return boundText(output.text, maxTextCodeUnits);
        if (output.type === undefined && 'value' in output) {
          return modelOutputToText(output.value, maxTextCodeUnits, traversal);
        }
        return null;
      }
    }
  } finally {
    traversal.depth -= 1;
    traversal.seen.delete(modelOutput);
  }
}

export type ToolCallFilterOptions = ToolCallFilteringOptions & {
  filterAfterToolSteps?: number;
};

/**
 * Filters out tool calls and results from the prompt sent to the model.
 * By default (with no arguments), excludes all tool calls and their results.
 * Can be configured to exclude only specific tools by name.
 *
 * Filtering happens in `processLLMRequest`, which runs after the message list
 * has been converted to a provider prompt. Changes are transient: they only
 * affect what this model call receives and are never written back to the
 * message list, memory, or UI history. Stored messages keep their full
 * tool-invocation parts.
 */
export class ToolCallFilter implements Processor {
  readonly id = 'tool-call-filter';
  name = 'ToolCallFilter';
  private exclude: string[] | 'all';
  private filterAfterToolSteps: number | undefined;
  /** Tools whose compact model output may survive filtering; `'all'` or an allowlist. */
  private preserveModelOutputFor: string[] | 'all';
  private maxModelOutputBytes: number | undefined;

  /**
   * Create a filter for tool calls and results.
   * @param options Configuration options
   * @param options.exclude List of specific tool names to exclude. If not provided, all tool calls are excluded.
   * @param options.filterAfterToolSteps Preserve tool calls/results from this many recent tool-producing steps.
   * @param options.preserveModelOutput Replace filtered tool results with their compact model-facing output as text.
   * @param options.preserveModelOutputFor Preserve compact model output only for these tool names. An empty list preserves none.
   * @param options.maxModelOutputBytes Maximum UTF-8 bytes retained from each preserved model output. Oversized output is deterministically truncated.
   */
  constructor(options: ToolCallFilterOptions = {}) {
    const resolvedOptions = options ?? {};
    this.exclude = normalizeToolCallFilterExclude((resolvedOptions as { exclude?: unknown }).exclude);
    this.filterAfterToolSteps = resolvedOptions.filterAfterToolSteps;
    this.preserveModelOutputFor = normalizePreservedModelOutputTools(
      resolvedOptions.preserveModelOutput,
      (resolvedOptions as { preserveModelOutputFor?: unknown }).preserveModelOutputFor,
    );
    this.maxModelOutputBytes = normalizeToolCallFilterMaxModelOutputBytes(
      (resolvedOptions as { maxModelOutputBytes?: unknown }).maxModelOutputBytes,
    );
  }

  async processLLMRequest({ prompt, state }: ProcessLLMRequestArgs): Promise<ProcessLLMRequestResult> {
    if (this.exclude !== 'all' && this.exclude.length === 0) {
      return undefined;
    }

    const preservedToolCallIds = this.getPreservedToolCallIds(prompt, state);
    const excludedToolCallIds = this.getExcludedToolCallIds(prompt, preservedToolCallIds);
    if (excludedToolCallIds.size === 0) {
      return undefined;
    }

    const replacementTexts = this.getReplacementTexts(prompt, excludedToolCallIds);
    const idsWithToolCallPart = this.getToolCallPartIds(prompt);

    const filtered: LanguageModelV2Prompt = [];
    let changed = false;

    for (const message of prompt) {
      if (message.role === 'system' || message.role === 'user') {
        filtered.push(message);
        continue;
      }

      const content: PromptPart[] = [];
      let messageChanged = false;

      for (const part of message.content as PromptPart[]) {
        const toolCallId = this.getPartToolCallId(part);
        if (!toolCallId || !excludedToolCallIds.has(toolCallId)) {
          content.push(part);
          continue;
        }

        changed = true;
        messageChanged = true;

        // A call/result pair collapses into a single text part, emitted where the
        // tool call was so message roles stay valid. Provider-executed results that
        // have no matching tool call emit in place instead.
        const emitsText =
          part.type === 'tool-call' || (message.role === 'assistant' && !idsWithToolCallPart.has(toolCallId));
        if (emitsText) {
          const text = replacementTexts.get(toolCallId);
          if (text) {
            content.push({ type: 'text', text });
          }
        }
      }

      if (content.length === 0) {
        // Dropping the message entirely avoids sending an empty assistant or
        // tool message, which providers reject.
        continue;
      }

      filtered.push(messageChanged ? ({ ...message, content } as PromptMessage) : message);
    }

    if (!changed) {
      return undefined;
    }

    return { prompt: filtered };
  }

  /**
   * Tool call ids that must survive filtering.
   *
   * When `filterAfterToolSteps` is not configured, only prior history is filtered:
   * every tool call produced by the current run stays in the prompt so the loop can
   * still act on the results it just produced.
   *
   * When it is configured, the most recent `filterAfterToolSteps` tool-producing steps
   * are preserved instead. Each assistant message containing tool calls counts as one
   * step, in prompt order, so history and the current run are treated the same way.
   */
  private getPreservedToolCallIds(prompt: LanguageModelV2Prompt, state: Record<string, unknown>): Set<string> {
    if (this.filterAfterToolSteps === undefined) {
      // The first prompt of a request is the pre-loop history. Every tool call id seen
      // after that belongs to the current run and is preserved.
      const historyToolCallIds = (state[HISTORY_TOOL_CALL_IDS] ??= this.getToolCallIds(prompt)) as Set<string>;
      return new Set([...this.getToolCallIds(prompt)].filter(id => !historyToolCallIds.has(id)));
    }

    const preserveStepCount = Math.max(0, this.filterAfterToolSteps);
    if (preserveStepCount === 0) {
      return new Set();
    }

    const promptToolSteps: string[][] = [];
    for (const message of prompt) {
      if (message.role !== 'assistant') continue;
      const stepToolCallIds = message.content
        .filter((part): part is ToolCallPart => part.type === 'tool-call')
        .map(part => part.toolCallId);
      if (stepToolCallIds.length > 0) {
        promptToolSteps.push(stepToolCallIds);
      }
    }

    return new Set(promptToolSteps.slice(-preserveStepCount).flat());
  }

  /**
   * Resolves which tool call ids to remove. Ids are always excluded as a
   * call/result pair so no dangling tool call or orphaned tool result is sent.
   */
  private getExcludedToolCallIds(prompt: LanguageModelV2Prompt, preservedToolCallIds: Set<string>): Set<string> {
    const excluded = new Set<string>();

    for (const message of prompt) {
      if (message.role === 'system' || message.role === 'user') continue;

      for (const part of message.content as PromptPart[]) {
        if (part.type !== 'tool-call' && part.type !== 'tool-result') continue;
        if (preservedToolCallIds.has(part.toolCallId)) continue;
        if (this.exclude === 'all' || this.exclude.includes(part.toolName)) {
          excluded.add(part.toolCallId);
        }
      }
    }

    return excluded;
  }

  /**
   * Compact model-facing text for each excluded tool result, used when model
   * output preservation is enabled. The prompt's `output` already reflects
   * `providerMetadata.mastra.modelOutput` when the tool defines `toModelOutput`;
   * otherwise it is the tool result the provider was already going to receive.
   *
   * Preservation is fenced three ways: only tools on the resolved allowlist may
   * re-emit their output, conversion is fail-closed (media parts, `toJSON` hooks
   * and unknown provider-specific shapes are omitted instead of stringified), and
   * each preserved text is bounded by `maxModelOutputBytes` while it is built, so
   * a filtered payload cannot be replayed unbounded into the prompt.
   */
  private getReplacementTexts(prompt: LanguageModelV2Prompt, excludedToolCallIds: Set<string>): Map<string, string> {
    const texts = new Map<string, string>();
    const preserveFor = this.preserveModelOutputFor;
    if (preserveFor !== 'all' && preserveFor.length === 0) {
      return texts;
    }

    // Cap conversion one code unit past the byte budget: a code unit is never
    // fewer bytes than one byte, so nothing that would have fit is cut, and the
    // extra unit lets `truncateUtf8` detect that truncation happened.
    const maxTextCodeUnits =
      this.maxModelOutputBytes === undefined
        ? undefined
        : Math.min(Number.MAX_SAFE_INTEGER, this.maxModelOutputBytes + 1);

    for (const message of prompt) {
      if (message.role === 'system' || message.role === 'user') continue;

      for (const part of message.content as PromptPart[]) {
        if (part.type !== 'tool-result') continue;
        if (!excludedToolCallIds.has(part.toolCallId)) continue;
        if (preserveFor !== 'all' && !preserveFor.includes(part.toolName)) continue;

        const text = this.toPreservedText((part as ToolResultPart).output, maxTextCodeUnits);
        if (!text) continue;

        texts.set(part.toolCallId, `${part.toolName} result:\n${text}`);
      }
    }

    return texts;
  }

  /** Fail-closed conversion of one tool result output into bounded prompt text. */
  private toPreservedText(output: unknown, maxTextCodeUnits: number | undefined): string | null {
    try {
      const text = modelOutputToText(output, maxTextCodeUnits);
      return text ? truncateUtf8(text, this.maxModelOutputBytes) : null;
    } catch {
      // A throwing accessor on a stored payload is an unknown shape, not an empty one.
      return null;
    }
  }

  private getToolCallIds(prompt: LanguageModelV2Prompt): Set<string> {
    const ids = new Set<string>();
    for (const message of prompt) {
      if (message.role === 'system' || message.role === 'user') continue;
      for (const part of message.content as PromptPart[]) {
        const toolCallId = this.getPartToolCallId(part);
        if (toolCallId) ids.add(toolCallId);
      }
    }
    return ids;
  }

  private getToolCallPartIds(prompt: LanguageModelV2Prompt): Set<string> {
    const ids = new Set<string>();
    for (const message of prompt) {
      if (message.role !== 'assistant') continue;
      for (const part of message.content) {
        if (part.type === 'tool-call') {
          ids.add(part.toolCallId);
        }
      }
    }
    return ids;
  }

  private getPartToolCallId(part: PromptPart): string | undefined {
    return part.type === 'tool-call' || part.type === 'tool-result' ? part.toolCallId : undefined;
  }
}
