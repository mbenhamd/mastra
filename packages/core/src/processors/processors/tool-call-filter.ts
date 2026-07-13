import type { MastraDBMessage, MessageList } from '../../agent/message-list';
import type { RequestContext } from '../../request-context';

import type { ProcessInputStepArgs, ProcessInputStepResult, Processor } from '../index';
import { filterToolCallMessages, normalizeToolCallFilterExclude } from '../tool-call-filter-utils';
import type { ToolCallFilteringOptions } from '../tool-call-filter-utils';

export type ToolCallFilterOptions = ToolCallFilteringOptions & {
  filterAfterToolSteps?: number;
};

/**
 * Filters out tool calls and results from messages.
 * By default (with no arguments), excludes all tool calls and their results.
 * Can be configured to exclude only specific tools by name.
 *
 * Runs on initial input (processInput). Step filtering is opt-in via filterAfterToolSteps.
 */
export class ToolCallFilter implements Processor {
  readonly id = 'tool-call-filter';
  name = 'ToolCallFilter';
  private exclude: string[] | 'all';
  private filterAfterToolSteps: number | undefined;
  private preserveModelOutput: boolean;
  private maxModelOutputBytes: number | undefined;

  /**
   * Create a filter for tool calls and results.
   * @param options Configuration options
   * @param options.exclude List of specific tool names to exclude. If not provided, all tool calls are excluded.
   * @param options.filterAfterToolSteps Enable agentic loop step filtering and preserve tool calls/results from this many recent tool-producing steps.
   * @param options.preserveModelOutput Preserve sanitized model-facing output from completed filtered tool results with providerMetadata.mastra.modelOutput.
   * @param options.maxModelOutputBytes Maximum UTF-8 bytes retained from each preserved model output. Oversized output is deterministically truncated.
   */
  constructor(options: ToolCallFilterOptions = {}) {
    const resolvedOptions = options ?? {};
    const exclude = (resolvedOptions as { exclude?: unknown }).exclude;
    this.exclude = normalizeToolCallFilterExclude(exclude);
    this.filterAfterToolSteps = resolvedOptions.filterAfterToolSteps;
    this.preserveModelOutput = resolvedOptions.preserveModelOutput ?? false;
    this.maxModelOutputBytes = resolvedOptions.maxModelOutputBytes;
  }

  async processInput(args: {
    messages: MastraDBMessage[];
    messageList: MessageList;
    abort: (reason?: string) => never;
    requestContext?: RequestContext;
  }): Promise<MessageList | MastraDBMessage[]> {
    return this.filterMessages(args.messageList.get.all.db());
  }

  async processInputStep(args: ProcessInputStepArgs): Promise<ProcessInputStepResult> {
    if (this.filterAfterToolSteps === undefined) {
      return {};
    }

    const messages = args.messageList.get.all.db();
    return { messages: this.filterMessages(messages, this.getRecentToolStepToolCallIds(args)) };
  }

  private getRecentToolStepToolCallIds(args: ProcessInputStepArgs): Set<string> {
    const state = args.state as {
      toolCallFilterSeenToolCallIds?: string[];
      toolCallFilterStepToolCallIds?: string[][];
    };
    const seenToolCallIds = new Set(state.toolCallFilterSeenToolCallIds ?? []);
    const responseToolCallIds = this.getMessageToolCallIds(args.messageList.get.response.db());
    const newToolCallIds = [...responseToolCallIds].filter(toolCallId => !seenToolCallIds.has(toolCallId));

    state.toolCallFilterSeenToolCallIds = [...new Set([...seenToolCallIds, ...newToolCallIds])];
    state.toolCallFilterStepToolCallIds = [...(state.toolCallFilterStepToolCallIds ?? []), newToolCallIds];

    const preserveStepCount = Math.max(0, this.filterAfterToolSteps ?? 0);
    const recentStepToolCallIds =
      preserveStepCount === 0 ? [] : state.toolCallFilterStepToolCallIds.slice(-preserveStepCount).flat();

    return new Set(recentStepToolCallIds);
  }

  private getMessageToolCallIds(messages: MastraDBMessage[]): Set<string> {
    const toolCallIds = new Set<string>();

    for (const message of messages) {
      const content = message.content as unknown;
      if (!content || typeof content !== 'object' || Array.isArray(content)) continue;

      const parts = (content as { parts?: unknown }).parts;
      if (Array.isArray(parts)) {
        for (const part of parts) {
          if (!part || typeof part !== 'object' || (part as { type?: unknown }).type !== 'tool-invocation') continue;
          const invocation = (part as { toolInvocation?: unknown }).toolInvocation;
          const toolCallId = this.getToolCallId(invocation);
          if (toolCallId) toolCallIds.add(toolCallId);
        }
      }

      const toolInvocations = (content as { toolInvocations?: unknown }).toolInvocations;
      if (Array.isArray(toolInvocations)) {
        for (const invocation of toolInvocations) {
          const toolCallId = this.getToolCallId(invocation);
          if (toolCallId) toolCallIds.add(toolCallId);
        }
      }
    }

    return toolCallIds;
  }

  private getToolCallId(invocation: unknown): string | undefined {
    if (!invocation || typeof invocation !== 'object') return undefined;
    const candidate = invocation as { toolCallId?: unknown; toolCall?: { id?: unknown } };
    if (typeof candidate.toolCallId === 'string') return candidate.toolCallId;
    if (typeof candidate.toolCall?.id === 'string') return candidate.toolCall.id;
    return undefined;
  }

  private filterMessages(messages: MastraDBMessage[], preserveToolCallIds = new Set<string>()): MastraDBMessage[] {
    return filterToolCallMessages(
      messages,
      {
        exclude: this.exclude === 'all' ? undefined : this.exclude,
        preserveModelOutput: this.preserveModelOutput,
        ...(this.maxModelOutputBytes === undefined ? {} : { maxModelOutputBytes: this.maxModelOutputBytes }),
      },
      preserveToolCallIds,
    );
  }
}
