import type { MastraDBMessage, MessageList } from '../../agent/message-list';
import type { RequestContext } from '../../request-context';

import type { Processor } from '../index';

type MastraMessagePart = NonNullable<Extract<MastraDBMessage['content'], { parts?: unknown }>['parts']>[number];
type V2ToolInvocationPart = Extract<MastraMessagePart, { type: 'tool-invocation' }>;

type ToolCallFilterOptions = {
  exclude?: string[];
  preserveModelOutput?: boolean;
};

/**
 * Filters out tool calls and results from messages.
 * By default (with no arguments), excludes all tool calls and their results.
 * Can be configured to exclude only specific tools by name.
 */
export class ToolCallFilter implements Processor {
  readonly id = 'tool-call-filter';
  name = 'ToolCallFilter';
  private exclude: string[] | 'all';
  private preserveModelOutput: boolean;

  /**
   * Create a filter for tool calls and results.
   * @param options Configuration options
   * @param options.exclude List of specific tool names to exclude. If not provided, all tool calls are excluded.
   * @param options.preserveModelOutput Keep completed tool results that have providerMetadata.mastra.modelOutput,
   * replacing the raw result with that compact model-facing projection.
   */
  constructor(options: ToolCallFilterOptions = {}) {
    this.preserveModelOutput = options.preserveModelOutput ?? false;

    // If no options or exclude is provided, exclude all tools
    if (!options || !options.exclude) {
      this.exclude = 'all'; // Exclude all tools
    } else {
      // Exclude specific tools
      this.exclude = Array.isArray(options.exclude) ? options.exclude : [];
    }
  }

  private getModelOutput(part: V2ToolInvocationPart): unknown {
    const mastraMetadata = part.providerMetadata?.mastra;
    if (!mastraMetadata || typeof mastraMetadata !== 'object') {
      return undefined;
    }

    return (mastraMetadata as Record<string, unknown>).modelOutput;
  }

  private compactToolResultPart(part: V2ToolInvocationPart): V2ToolInvocationPart | null {
    if (!this.preserveModelOutput || part.toolInvocation.state !== 'result') {
      return null;
    }

    const modelOutput = this.getModelOutput(part);
    if (modelOutput == null) {
      return null;
    }

    return {
      ...part,
      toolInvocation: {
        ...part.toolInvocation,
        result: modelOutput,
      },
    };
  }

  private getToolInvocationsFromParts(parts: MastraDBMessage['content']['parts']) {
    return parts
      .filter((part): part is V2ToolInvocationPart => part.type === 'tool-invocation')
      .map(part => part.toolInvocation);
  }

  async processInput(args: {
    messages: MastraDBMessage[];
    messageList: MessageList;
    abort: (reason?: string) => never;
    requestContext?: RequestContext;
  }): Promise<MessageList | MastraDBMessage[]> {
    const { messageList } = args;
    // Use messages from messageList to respect consolidation
    const messages = messageList.get.all.db();

    // Helper to check if a message has tool invocations
    const hasToolInvocations = (message: MastraDBMessage): boolean => {
      if (typeof message.content === 'string') return false;
      if (!message.content?.parts) return false;
      return message.content.parts.some(part => part.type === 'tool-invocation');
    };

    // Helper to get tool invocations from a message
    const getToolInvocations = (message: MastraDBMessage) => {
      if (typeof message.content === 'string') return [];
      if (!message.content?.parts) return [];
      return message.content.parts.filter((part: any) => part.type === 'tool-invocation');
    };

    // Case 1: Exclude all tool calls and tool results
    if (this.exclude === 'all') {
      const result = messages
        .map(message => {
          // Skip messages with tool invocations - they'll be filtered by sanitizeAIV4UIMessages
          if (!hasToolInvocations(message)) {
            return message;
          }

          // For messages with tool invocations, strip the tool invocation parts
          // but keep other content (like text)
          if (typeof message.content === 'string') {
            return message;
          }

          if (!message.content?.parts) {
            return message;
          }

          const filteredParts = message.content.parts.flatMap((part: any) => {
            if (part.type !== 'tool-invocation') {
              return [part];
            }

            const compactPart = this.compactToolResultPart(part as V2ToolInvocationPart);
            return compactPart ? [compactPart] : [];
          });

          // If no parts remain after filtering, remove the message
          if (filteredParts.length === 0) {
            return null;
          }

          // Return message with filtered parts
          // Also filter toolInvocations if present
          const { toolInvocations: originalToolInvocations, ...contentWithoutToolInvocations } = message.content as any;
          const updatedContent: any = {
            ...contentWithoutToolInvocations,
            parts: filteredParts,
          };

          const compactToolInvocations = this.getToolInvocationsFromParts(filteredParts);
          if (compactToolInvocations.length > 0) {
            updatedContent.toolInvocations = compactToolInvocations;
          }

          return {
            ...message,
            content: updatedContent,
          };
        })
        .filter((message): message is MastraDBMessage => message !== null);
      return result;
    }

    // Case 2: Exclude specific tools by name
    if (this.exclude.length > 0) {
      // Track excluded tool call IDs to also filter their results
      const excludedToolCallIds = new Set<string>();

      // First pass: identify excluded tool call IDs
      for (const message of messages) {
        const toolInvocations = getToolInvocations(message);
        for (const part of toolInvocations) {
          const invocationPart = part as unknown as V2ToolInvocationPart;
          const invocation = invocationPart.toolInvocation;

          // Track tool call IDs from both calls and results for excluded tools
          // This handles cases where only results exist (e.g., in test data)
          if (this.exclude.includes(invocation.toolName)) {
            excludedToolCallIds.add(invocation.toolCallId);
          }
        }
      }

      // Second pass: filter out excluded tool invocation parts
      const filteredMessages = messages
        .map(message => {
          if (!hasToolInvocations(message)) {
            return message;
          }

          if (typeof message.content === 'string') {
            return message;
          }

          if (!message.content?.parts) {
            return message;
          }

          // Filter out excluded tool invocation parts
          const filteredParts = message.content.parts.flatMap((part: any) => {
            if (part.type !== 'tool-invocation') {
              return [part]; // Keep non-tool parts
            }

            const invocationPart = part as unknown as V2ToolInvocationPart;
            const invocation = invocationPart.toolInvocation;
            const shouldExclude =
              (invocation.state === 'call' && this.exclude.includes(invocation.toolName)) ||
              (invocation.state === 'result' && excludedToolCallIds.has(invocation.toolCallId)) ||
              (invocation.state === 'result' && this.exclude.includes(invocation.toolName));

            if (!shouldExclude) {
              return [part];
            }

            const compactPart = this.compactToolResultPart(invocationPart);
            if (compactPart) {
              return [compactPart];
            }

            return [];
          });

          // If no parts remain, remove the message entirely
          if (filteredParts.length === 0) {
            return null;
          }

          // Return message with filtered parts
          // Also filter toolInvocations if present
          const { toolInvocations: originalToolInvocations, ...contentWithoutToolInvocations } = message.content as any;
          const updatedContent: any = {
            ...contentWithoutToolInvocations,
            parts: filteredParts,
          };

          const filteredToolInvocations = this.getToolInvocationsFromParts(filteredParts);
          if (filteredToolInvocations.length > 0) {
            updatedContent.toolInvocations = filteredToolInvocations;
          }

          // Check if message has no parts and no text content
          // Note: For V2 messages, parts is the source of truth, not toolInvocations
          const hasNoToolParts = filteredParts.length === 0;
          const hasNoTextContent = !updatedContent.content || updatedContent.content.trim() === '';

          // Only remove the message if it has no parts at all and no text content
          if (hasNoToolParts && hasNoTextContent) {
            return null;
          }

          return {
            ...message,
            content: updatedContent,
          };
        })
        .filter((message): message is MastraDBMessage => message !== null);

      return filteredMessages;
    }

    // Case 3: Empty exclude array, return original messages
    return messages;
  }
}
