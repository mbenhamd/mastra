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
  /** The tool-call input the model sent. */
  input: unknown;
  /** The model-facing result in the prompt (after `toModelOutput`). */
  output: unknown;
  /**
   * The recorded tool result (after `processToolResult`) from the request's
   * message list, when it carries one. `undefined` otherwise.
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
}

export interface ToolResultSupersedeOptions {
  /** Opt-in per tool name. Tools that are not listed are never touched. */
  tools: Record<string, ToolResultSupersedeToolConfig>;
}

type KeyedResult = { messageIndex: number; partIndex: number; part: ToolResultPart; scopedKey: string };

/**
 * Replaces stale tool results in model context when a later result shows the
 * same resource.
 *
 * Each step re-sends every earlier tool result. When a tool reads the same
 * resource again, the earlier copy is dead weight: this processor keeps the
 * newest successful result for each key verbatim and replaces the output of
 * every result with the same key from an earlier step by a fixed one-line
 * pointer. The tool-call part and its input stay, so the call/result pairing and
 * provenance survive. Results of the latest step are never replaced, and
 * results of the same step never replace each other. Error results (including
 * results the tool classified as failures), provider-executed results and
 * results that passed through an approval are never keyed.
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
      entries.set(toolName, { key: config.key, ...(config.group !== undefined ? { group: config.group } : {}) });
    }
    this.tools = entries;
  }

  processLLMRequest({ prompt, messageList }: ProcessLLMRequestArgs): ProcessLLMRequestResult {
    if (this.tools.size === 0) return undefined;

    const inputs = new Map<string, unknown>();
    const candidates: Array<{ messageIndex: number; partIndex: number; part: ToolResultPart }> = [];
    prompt.forEach((message, messageIndex) => {
      if (message.role === 'assistant') {
        for (const part of message.content) {
          if (part.type === 'tool-call') inputs.set(part.toolCallId, part.input);
        }
        return;
      }
      // Provider-executed results live in assistant messages and are never
      // keyed; only client tool results in tool messages are candidates.
      if (message.role !== 'tool') return;
      message.content.forEach((part, partIndex) => {
        if (part.type === 'tool-result' && this.tools.has(part.toolName) && isSuccessfulOutput(part.output)) {
          candidates.push({ messageIndex, partIndex, part });
        }
      });
    });
    if (candidates.length < 2) return undefined;

    const recorded = collectRecordedToolResults(messageList);
    const keyed: KeyedResult[] = [];
    for (const candidate of candidates) {
      const record = recorded.get(candidate.part.toolCallId);
      // A failure the tool classified, or a result that passed through an
      // approval, is evidence of its own and is never compacted.
      if (record?.preserve || isClassifiedFailure(candidate.part.providerOptions)) continue;
      const config = this.tools.get(candidate.part.toolName)!;
      const key = resolveKey(config, {
        toolName: candidate.part.toolName,
        toolCallId: candidate.part.toolCallId,
        input: inputs.get(candidate.part.toolCallId),
        output: candidate.part.output,
        result: record?.result,
      });
      if (key === undefined) continue;
      keyed.push({ ...candidate, scopedKey: `${config.group ?? candidate.part.toolName}\u0000${key}` });
    }

    // Prompt order is chronological, so the last result per key is the newest.
    const newest = new Map<string, KeyedResult>();
    for (const entry of keyed) newest.set(entry.scopedKey, entry);

    const replacements = new Map<ToolResultPart, ToolResultPart>();
    for (const entry of keyed) {
      // Only a result from a later step supersedes: same-step duplicates were
      // produced together, and the newest step's results are what the model has
      // not seen yet.
      if (newest.get(entry.scopedKey)!.messageIndex <= entry.messageIndex) continue;
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

function supersededText(toolName: string): string {
  return `[superseded] A later ${toolName} result for the same resource replaced this one; use the latest result.`;
}

function isClassifiedFailure(metadata: unknown): boolean {
  const mastra = (metadata as { mastra?: { resultStatus?: unknown } } | undefined)?.mastra;
  return mastra?.resultStatus === 'error';
}

/** Only a result the model could use as evidence may supersede or be superseded. */
function isSuccessfulOutput(output: unknown): boolean {
  if (output === null || typeof output !== 'object') return false;
  const type = (output as { type?: unknown }).type;
  return type === 'text' || type === 'json' || type === 'content';
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

type RecordedToolResult = { result: unknown; preserve: boolean };

/**
 * The recorded (post-processor) result for each tool call in the message list,
 * and whether the call must be preserved: one that passed through an approval or
 * whose tool classified the recorded result as a failure.
 */
function collectRecordedToolResults(messageList: MessageList | undefined): Map<string, RecordedToolResult> {
  const results = new Map<string, RecordedToolResult>();
  if (!messageList) return results;
  for (const message of messageList.get.all.db()) {
    const parts = message.content?.parts;
    if (!Array.isArray(parts)) continue;
    for (const part of parts) {
      if (part.type !== 'tool-invocation') continue;
      const invocation = part.toolInvocation as typeof part.toolInvocation & { approval?: unknown };
      if (invocation.state !== 'result') continue;
      results.set(invocation.toolCallId, {
        result: invocation.result,
        preserve:
          invocation.approval !== undefined ||
          isClassifiedFailure((part as { providerMetadata?: unknown }).providerMetadata),
      });
    }
  }
  return results;
}
