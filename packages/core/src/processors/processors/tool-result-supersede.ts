import type { LanguageModelV2Prompt } from '@ai-sdk/provider-v5';

import type { MessageList } from '../../agent/message-list';
import type { ProcessLLMRequestArgs, ProcessLLMRequestResult, Processor } from '../index';

type PromptMessage = LanguageModelV2Prompt[number];
type ToolMessage = Extract<PromptMessage, { role: 'tool' }>;
type ToolResultPart = ToolMessage['content'][number];

/** Longest resource key accepted from a key function; longer keys fail closed. */
const MAX_SUPERSEDE_KEY_LENGTH = 1024;

/** A completed tool call offered to a {@link ToolResultSupersedeToolConfig.key} function. */
export interface ToolResultSupersedeCall {
  toolName: string;
  toolCallId: string;
  /** The recorded tool-call input (arguments) of this occurrence. */
  input: unknown;
  /** The model-facing result in the prompt (after `toModelOutput`). */
  output: unknown;
  /**
   * The recorded tool result (after `processToolResult`) of this occurrence in
   * the request's message list.
   */
  result: unknown;
}

export interface ToolResultSupersedeToolConfig {
  /**
   * Identify the resource and coverage this result shows, for example a project,
   * file path and read span, or a paper id. A successful result with the same key
   * from a later step replaces this one in model context, so two results may
   * share a key only when the later one fully covers the earlier one. Return
   * `undefined` to keep the result. Must be pure and cheap: it runs on every
   * provider call. A throwing function, an empty key, or a key over 1024
   * characters keeps the result.
   */
  key: (call: ToolResultSupersedeCall) => string | undefined;
  /**
   * Tools that name the same group share one key space, so a result of one tool
   * can supersede a result of another. Only group tools whose equal keys mean
   * equal coverage. Defaults to the tool name.
   */
  group?: string;
  /**
   * Allow results with media content (for example an image) to be keyed. Off by
   * default: such results are kept.
   */
  includeMedia?: boolean;
}

export interface ToolResultSupersedeOptions {
  /** Opt-in per tool name. Tools that are not listed are never touched. */
  tools: Record<string, ToolResultSupersedeToolConfig>;
}

type KeyedResult = { part: ToolResultPart; step: StepPosition; scopedKey: string };

/**
 * Replaces stale tool results in model context when a later result shows the
 * same resource.
 *
 * Each step re-sends every earlier tool result. When a tool reads the same
 * resource again, the earlier copy is dead weight: this processor keeps the
 * newest successful result for each key verbatim and replaces the output of
 * every result with the same key from an earlier model step by a fixed one-line
 * pointer. Each prompt result is paired with its recorded occurrence in the
 * message list (by occurrence, since tool-call ids can repeat across turns);
 * a result without an unambiguous recorded occurrence is never keyed. The tool-call part and its input stay, so the call/result pairing and
 * provenance survive. Results of the latest step are never replaced, and
 * results of the same step never replace each other. Error results (including
 * results the tool classified as failures), provider-executed results and
 * results that passed through an approval are never keyed, and media results
 * only when the tool sets `includeMedia`.
 *
 * Supersession is opt-in per tool and keyed by the caller, so evidence the
 * model still needs (a different version, span or resource) keeps a different
 * key and is never removed. The rewrite runs in `processLLMRequest`: it changes
 * only the prompt of one provider call and is never written to the message list,
 * memory or UI history. It depends only on the prompt and the message list, so
 * once a result is superseded every later step sends the same bytes for it and
 * the provider prompt cache stays valid after that point. Place it before any
 * response-cache processor so the cache key reflects the compacted prompt. It is
 * independent of `ToolCallFilter`: a pair that filter removes is simply absent.
 */
export class ToolResultSupersedeProcessor implements Processor {
  readonly id = 'tool-result-supersede';
  name = 'ToolResultSupersede';
  private readonly tools: ReadonlyMap<string, ToolResultSupersedeToolConfig>;

  constructor(options: ToolResultSupersedeOptions) {
    const tools = options?.tools;
    if (tools === null || typeof tools !== 'object') {
      throw new TypeError('ToolResultSupersedeProcessor options.tools must be an object keyed by tool name');
    }
    const entries = new Map<string, ToolResultSupersedeToolConfig>();
    for (const [toolName, config] of Object.entries(tools)) {
      if (!config || typeof config.key !== 'function') {
        throw new TypeError(`ToolResultSupersedeProcessor options.tools["${toolName}"].key must be a function`);
      }
      if (config.group !== undefined && (typeof config.group !== 'string' || config.group.length === 0)) {
        throw new TypeError(
          `ToolResultSupersedeProcessor options.tools["${toolName}"].group must be a non-empty string`,
        );
      }
      entries.set(toolName, {
        key: config.key,
        ...(config.group !== undefined ? { group: config.group } : {}),
        ...(config.includeMedia === true ? { includeMedia: true } : {}),
      });
    }
    this.tools = entries;
  }

  processLLMRequest({ prompt, messageList, steps }: ProcessLLMRequestArgs): ProcessLLMRequestResult {
    if (this.tools.size === 0) return undefined;

    // Candidates in prompt order. Provider-executed results live in assistant
    // messages and are never keyed; only client tool results in tool messages.
    const candidates: ToolResultPart[] = [];
    for (const message of prompt) {
      if (message.role !== 'tool') continue;
      for (const part of message.content) {
        if (part.type === 'tool-result' && this.tools.has(part.toolName)) candidates.push(part);
      }
    }
    if (candidates.length < 2) return undefined;

    const occurrences = pairWithRecordedOccurrences(candidates, messageList, steps);
    const keyed: KeyedResult[] = [];
    for (const part of candidates) {
      const occurrence = occurrences.get(part);
      // No unambiguous recorded occurrence means no provenance: keep it.
      if (!occurrence || occurrence.preserve) continue;
      if (!isSuccessfulOutput(part.output, this.tools.get(part.toolName)!.includeMedia === true)) continue;
      if (isClassifiedFailure(part.providerOptions)) continue;
      const config = this.tools.get(part.toolName)!;
      const key = resolveKey(config, {
        toolName: part.toolName,
        toolCallId: part.toolCallId,
        input: occurrence.input,
        output: part.output,
        result: occurrence.result,
      });
      if (key === undefined) continue;
      keyed.push({ part, step: occurrence.step, scopedKey: scopedSupersedeKey(config.group ?? part.toolName, key) });
    }

    // The newest result per key is the one from the latest step.
    const newest = new Map<string, KeyedResult>();
    for (const entry of keyed) {
      const current = newest.get(entry.scopedKey);
      if (!current || compareSteps(entry.step, current.step) >= 0) newest.set(entry.scopedKey, entry);
    }

    const replacements = new Map<ToolResultPart, ToolResultPart>();
    for (const entry of keyed) {
      // Only a result from a strictly later model step supersedes: results of
      // one step were produced together, and the latest step's results are what
      // the model has not seen yet.
      if (compareSteps(newest.get(entry.scopedKey)!.step, entry.step) <= 0) continue;
      replacements.set(entry.part, {
        ...entry.part,
        // The pointer names no later call id, so it stays byte-identical when a
        // third read of the same resource arrives (prompt-cache stability).
        output: { type: 'text', value: supersededText(entry.part.toolName) },
      } as ToolResultPart);
    }
    if (replacements.size === 0) return undefined;

    return {
      prompt: prompt.map(message => {
        if (message.role !== 'tool') return message;
        if (!message.content.some(part => replacements.has(part))) return message;
        return { ...message, content: message.content.map(part => replacements.get(part) ?? part) } as PromptMessage;
      }),
    };
  }
}

/**
 * Model-step position of a recorded tool call: `run` is its index among this
 * run's completed steps; history calls (earlier runs) have no `run` index and
 * are ordered by their position in the recorded history.
 */
type StepPosition = { run?: number; history: number };

/**
 * Key space plus key, JSON-encoded so no group/key pair can collide with
 * another, whatever characters either contains.
 */
export function scopedSupersedeKey(group: string, key: string): string {
  return JSON.stringify([group, key]);
}

function compareSteps(left: StepPosition, right: StepPosition): number {
  if (left.run !== undefined && right.run !== undefined) return left.run - right.run;
  if (left.run !== undefined) return 1;
  if (right.run !== undefined) return -1;
  return left.history - right.history;
}

function supersededText(toolName: string): string {
  return `[superseded] A later ${toolName} result for the same resource replaced this one; use the latest result.`;
}

function isClassifiedFailure(metadata: unknown): boolean {
  const mastra = (metadata as { mastra?: { resultStatus?: unknown } } | undefined)?.mastra;
  return mastra?.resultStatus === 'error';
}

/**
 * Only a result the model could use as evidence may supersede or be superseded.
 * Media (non-text content parts) is preserved unless the tool opted in.
 */
function isSuccessfulOutput(output: unknown, includeMedia: boolean): boolean {
  if (output === null || typeof output !== 'object') return false;
  const { type, value } = output as { type?: unknown; value?: unknown };
  if (type === 'text' || type === 'json') return true;
  if (type !== 'content' || !Array.isArray(value)) return false;
  return includeMedia || value.every(part => (part as { type?: unknown })?.type === 'text');
}

function resolveKey(config: ToolResultSupersedeToolConfig, call: ToolResultSupersedeCall): string | undefined {
  let key: unknown;
  try {
    key = config.key(call);
  } catch {
    return undefined;
  }
  if (typeof key !== 'string' || key.length === 0 || key.length > MAX_SUPERSEDE_KEY_LENGTH) return undefined;
  return key;
}

type RecordedOccurrence = {
  toolName: string;
  input: unknown;
  result: unknown;
  step: StepPosition;
  /** Passed through an approval, or the tool classified the result as a failure. */
  preserve: boolean;
};

/**
 * Pair each prompt tool result with its recorded occurrence in the message list.
 *
 * Tool-call ids are not unique across turns (providers reuse ids such as
 * `call_0`), so results are paired by occurrence: the n-th prompt result with an
 * id matches the n-th recorded result with that id. When the counts or tool
 * names differ (for example, history trimmed by memory), every result with that
 * id is left unpaired and therefore kept. A call's step comes from this run's
 * completed steps when it belongs to them, otherwise from its position in the
 * recorded history, where each assistant message and each `step-start` part
 * begins a new step.
 */
function pairWithRecordedOccurrences(
  candidates: readonly ToolResultPart[],
  messageList: MessageList | undefined,
  steps: ProcessLLMRequestArgs['steps'],
): Map<ToolResultPart, RecordedOccurrence> {
  const paired = new Map<ToolResultPart, RecordedOccurrence>();
  if (!messageList) return paired;

  const recorded = new Map<string, RecordedOccurrence[]>();
  let historyStep = 0;
  for (const message of messageList.get.all.db()) {
    if (message.role !== 'assistant') continue;
    historyStep += 1;
    const parts = message.content?.parts;
    if (!Array.isArray(parts)) continue;
    for (const part of parts) {
      if (part.type === 'step-start') {
        historyStep += 1;
        continue;
      }
      if (part.type !== 'tool-invocation') continue;
      const invocation = part.toolInvocation as typeof part.toolInvocation & { approval?: unknown };
      if (invocation.state !== 'result') continue;
      const list = recorded.get(invocation.toolCallId) ?? [];
      list.push({
        toolName: invocation.toolName,
        input: invocation.args,
        result: invocation.result,
        step: { history: historyStep },
        preserve:
          invocation.approval !== undefined ||
          isClassifiedFailure((part as { providerMetadata?: unknown }).providerMetadata),
      });
      recorded.set(invocation.toolCallId, list);
    }
  }

  // This run's calls are the latest recorded occurrences of their ids.
  const runStepsById = new Map<string, number[]>();
  (steps ?? []).forEach((step, index) => {
    for (const call of (step as { toolCalls?: Array<{ toolCallId?: string; payload?: { toolCallId?: string } }> })
      .toolCalls ?? []) {
      const id = call.toolCallId ?? call.payload?.toolCallId;
      if (typeof id !== 'string') continue;
      runStepsById.set(id, [...(runStepsById.get(id) ?? []), index]);
    }
  });
  for (const [id, runSteps] of runStepsById) {
    const list = recorded.get(id);
    if (!list) continue;
    const offset = list.length - runSteps.length;
    runSteps.forEach((run, index) => {
      const occurrence = list[offset + index];
      if (occurrence) occurrence.step = { ...occurrence.step, run };
    });
  }

  const promptById = new Map<string, ToolResultPart[]>();
  for (const part of candidates) {
    promptById.set(part.toolCallId, [...(promptById.get(part.toolCallId) ?? []), part]);
  }
  for (const [id, parts] of promptById) {
    const list = recorded.get(id);
    if (!list || list.length !== parts.length) continue;
    if (parts.some((part, index) => part.toolName !== list[index]!.toolName)) continue;
    parts.forEach((part, index) => paired.set(part, list[index]!));
  }
  return paired;
}
