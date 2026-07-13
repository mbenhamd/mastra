import type { MastraDBMessage, MastraMessagePart, MastraToolInvocationPart } from '../agent/message-list';

const MODEL_OUTPUT_TRUNCATION_SUFFIX = '\n[truncated]';
const MAX_MODEL_OUTPUT_TRAVERSAL_DEPTH = 64;
const MAX_MODEL_OUTPUT_TRAVERSAL_NODES = 10_000;

export type ToolCallFilteringOptions = {
  exclude?: string[];
  preserveModelOutput?: boolean;
  maxModelOutputBytes?: number;
};

type NormalizedToolCallFilteringOptions = {
  exclude: string[] | 'all';
  preserveModelOutput: boolean;
  maxModelOutputBytes?: number;
};

function normalizeOptions(options: ToolCallFilteringOptions | null): NormalizedToolCallFilteringOptions {
  const resolvedOptions = options ?? {};
  const exclude = (resolvedOptions as { exclude?: unknown }).exclude;
  const maxModelOutputBytes = resolvedOptions.maxModelOutputBytes;
  return {
    exclude: exclude == null ? 'all' : Array.isArray(exclude) ? exclude : [],
    preserveModelOutput: resolvedOptions.preserveModelOutput ?? false,
    ...(maxModelOutputBytes === undefined
      ? {}
      : {
          maxModelOutputBytes: Number.isFinite(maxModelOutputBytes) ? Math.max(0, Math.floor(maxModelOutputBytes)) : 0,
        }),
  };
}

function getMessageParts(message: MastraDBMessage): MastraMessagePart[] {
  const content = message.content as unknown;
  if (!content || typeof content !== 'object' || Array.isArray(content)) return [];
  const parts = (content as { parts?: unknown }).parts;
  return Array.isArray(parts) ? (parts as MastraMessagePart[]) : [];
}

function getTopLevelToolInvocations(
  message: MastraDBMessage,
): NonNullable<MastraDBMessage['content']['toolInvocations']> {
  const content = message.content as unknown;
  if (!content || typeof content !== 'object' || Array.isArray(content)) return [];
  const toolInvocations = (content as { toolInvocations?: unknown }).toolInvocations;
  return Array.isArray(toolInvocations)
    ? (toolInvocations as NonNullable<MastraDBMessage['content']['toolInvocations']>)
    : [];
}

function getToolInvocations(message: MastraDBMessage): MastraToolInvocationPart[] {
  return getMessageParts(message).filter((part): part is MastraToolInvocationPart => part.type === 'tool-invocation');
}

function hasToolInvocations(message: MastraDBMessage): boolean {
  return (
    getMessageParts(message).some(part => part.type === 'tool-invocation') ||
    getTopLevelToolInvocations(message).length > 0
  );
}

function hasTopLevelTextContent(message: MastraDBMessage): boolean {
  const content = message.content as unknown;
  if (typeof content === 'string') return content.trim().length > 0;
  if (!content || typeof content !== 'object' || Array.isArray(content)) return false;
  const topLevelContent = (content as { content?: unknown }).content;
  return typeof topLevelContent === 'string' && topLevelContent.trim().length > 0;
}

function getToolCallId(invocation: MastraToolInvocationPart['toolInvocation']): string | undefined {
  return invocation.toolCallId ?? (invocation as { toolCall?: { id?: string } }).toolCall?.id;
}

function safeJsonStringify(value: unknown): string | null {
  try {
    const serialized = JSON.stringify(value);
    return typeof serialized === 'string' ? serialized : null;
  } catch {
    return null;
  }
}

/**
 * Convert supported model-facing tool output into text without serializing media or unknown objects.
 *
 * The recognized object shapes mirror LanguageModelV2ToolResultOutput. Primitive and array handling
 * remain for backwards compatibility with existing ToolCallFilter callers that stored legacy output.
 */
function modelOutputToText(
  modelOutput: unknown,
  traversal: { seen: WeakSet<object>; depth: number; nodes: number } = {
    seen: new WeakSet<object>(),
    depth: 0,
    nodes: 0,
  },
): string | null {
  if (typeof modelOutput === 'string') {
    return modelOutput;
  }

  if (typeof modelOutput === 'number' || typeof modelOutput === 'boolean' || typeof modelOutput === 'bigint') {
    return String(modelOutput);
  }

  if (!modelOutput || typeof modelOutput !== 'object') {
    return null;
  }

  if (
    traversal.seen.has(modelOutput) ||
    traversal.depth >= MAX_MODEL_OUTPUT_TRAVERSAL_DEPTH ||
    traversal.nodes >= MAX_MODEL_OUTPUT_TRAVERSAL_NODES
  ) {
    return null;
  }

  traversal.seen.add(modelOutput);
  traversal.depth += 1;
  traversal.nodes += 1;

  try {
    if (Array.isArray(modelOutput)) {
      const text = modelOutput
        .map(part => modelOutputToText(part, traversal))
        .filter((part): part is string => Boolean(part))
        .join('\n');
      return text || null;
    }

    const output = modelOutput as Record<string, unknown>;
    switch (output.type) {
      case 'text':
      case 'error-text':
        return typeof output.value === 'string' ? output.value : typeof output.text === 'string' ? output.text : null;
      case 'json':
      case 'error-json':
        return Object.hasOwn(output, 'value') ? safeJsonStringify(output.value) : null;
      case 'content': {
        if (!Array.isArray(output.value)) return null;
        const text = output.value
          .flatMap(part => {
            if (!part || typeof part !== 'object') return [];
            const contentPart = part as Record<string, unknown>;
            if (contentPart.type !== 'text') return [];
            const value = typeof contentPart.text === 'string' ? contentPart.text : contentPart.value;
            return typeof value === 'string' ? [value] : [];
          })
          .join('\n');
        return text || null;
      }
      default:
        // Preserve the two legacy wrapper shapes ToolCallFilter already accepted, but never stringify
        // arbitrary objects. This fails closed for media and unknown provider-specific output.
        if (typeof output.text === 'string') return output.text;
        if (!Object.hasOwn(output, 'type') && Object.hasOwn(output, 'value')) {
          return modelOutputToText(output.value, traversal);
        }
        return null;
    }
  } finally {
    traversal.depth -= 1;
  }
}

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

function getPreservedModelOutputPart(
  part: MastraToolInvocationPart,
  options: NormalizedToolCallFilteringOptions,
): { type: 'text'; text: string } | null {
  if (!options.preserveModelOutput || part.toolInvocation.state !== 'result') {
    return null;
  }

  const mastraMetadata = part.providerMetadata?.mastra;
  if (!mastraMetadata || !Object.hasOwn(mastraMetadata, 'modelOutput')) {
    return null;
  }

  const text = modelOutputToText(mastraMetadata.modelOutput);
  if (!text) return null;

  const boundedText = truncateUtf8(text, options.maxModelOutputBytes);
  if (!boundedText) return null;

  return {
    type: 'text',
    text: `${part.toolInvocation.toolName} result:\n${boundedText}`,
  };
}

function buildContent(
  message: MastraDBMessage,
  parts: MastraDBMessage['content']['parts'],
  toolInvocations: MastraDBMessage['content']['toolInvocations'],
): MastraDBMessage['content'] {
  const {
    toolInvocations: _originalToolInvocations,
    providerMetadata: _providerMetadata,
    ...contentWithoutToolInvocations
  } = message.content;
  const updatedContent: MastraDBMessage['content'] = {
    ...contentWithoutToolInvocations,
    parts,
  };

  if (toolInvocations && toolInvocations.length > 0) {
    updatedContent.toolInvocations = toolInvocations;
  }

  return updatedContent;
}

function filterAllToolCalls(
  messages: MastraDBMessage[],
  options: NormalizedToolCallFilteringOptions,
  preserveToolCallIds: Set<string>,
): MastraDBMessage[] {
  return messages
    .map(message => {
      if (!hasToolInvocations(message)) return message;

      const nonToolParts: MastraMessagePart[] = [];
      for (const part of getMessageParts(message)) {
        if (part.type !== 'tool-invocation') {
          nonToolParts.push(part);
          continue;
        }

        const toolCallId = getToolCallId(part.toolInvocation);
        if (toolCallId && preserveToolCallIds.has(toolCallId)) {
          nonToolParts.push(part);
          continue;
        }

        const modelOutputPart = getPreservedModelOutputPart(part, options);
        if (modelOutputPart) nonToolParts.push(modelOutputPart);
      }

      const filteredToolInvocations = getTopLevelToolInvocations(message).filter(invocation => {
        const toolCallId = getToolCallId(invocation);
        return toolCallId !== undefined && preserveToolCallIds.has(toolCallId);
      });

      if (
        nonToolParts.length === 0 &&
        (filteredToolInvocations?.length ?? 0) === 0 &&
        !hasTopLevelTextContent(message)
      ) {
        return null;
      }

      return {
        ...message,
        content: buildContent(message, nonToolParts, filteredToolInvocations),
      };
    })
    .filter((message): message is MastraDBMessage => message !== null);
}

function filterSpecificToolCalls(
  messages: MastraDBMessage[],
  options: NormalizedToolCallFilteringOptions & { exclude: string[] },
  preserveToolCallIds: Set<string>,
): MastraDBMessage[] {
  const excludedToolCallIds = new Set<string>();

  for (const message of messages) {
    for (const part of getToolInvocations(message)) {
      if (!options.exclude.includes(part.toolInvocation.toolName)) continue;
      const toolCallId = getToolCallId(part.toolInvocation);
      if (toolCallId) excludedToolCallIds.add(toolCallId);
    }
    for (const invocation of getTopLevelToolInvocations(message)) {
      if (!options.exclude.includes(invocation.toolName)) continue;
      const toolCallId = getToolCallId(invocation);
      if (toolCallId) excludedToolCallIds.add(toolCallId);
    }
  }

  return messages
    .map(message => {
      if (!hasToolInvocations(message)) return message;

      const filteredParts: MastraMessagePart[] = [];
      for (const part of getMessageParts(message)) {
        if (part.type !== 'tool-invocation') {
          filteredParts.push(part);
          continue;
        }

        const invocation = part.toolInvocation;
        const toolCallId = getToolCallId(invocation);
        if (toolCallId && preserveToolCallIds.has(toolCallId)) {
          filteredParts.push(part);
          continue;
        }

        const shouldExclude =
          options.exclude.includes(invocation.toolName) ||
          (toolCallId !== undefined && excludedToolCallIds.has(toolCallId));
        if (!shouldExclude) {
          filteredParts.push(part);
          continue;
        }

        const modelOutputPart = getPreservedModelOutputPart(part, options);
        if (modelOutputPart) filteredParts.push(modelOutputPart);
      }

      const filteredToolInvocations = getTopLevelToolInvocations(message).filter(invocation => {
        const toolCallId = getToolCallId(invocation);
        return (
          (toolCallId !== undefined && preserveToolCallIds.has(toolCallId)) ||
          (!options.exclude.includes(invocation.toolName) &&
            (toolCallId === undefined || !excludedToolCallIds.has(toolCallId)))
        );
      });

      if (
        filteredParts.length === 0 &&
        (filteredToolInvocations?.length ?? 0) === 0 &&
        !hasTopLevelTextContent(message)
      ) {
        return null;
      }

      return {
        ...message,
        content: buildContent(message, filteredParts, filteredToolInvocations),
      };
    })
    .filter((message): message is MastraDBMessage => message !== null);
}

export function filterToolCallMessages(
  messages: MastraDBMessage[],
  options: ToolCallFilteringOptions | null = {},
  preserveToolCallIds = new Set<string>(),
): MastraDBMessage[] {
  const normalizedOptions = normalizeOptions(options);
  if (normalizedOptions.exclude === 'all') {
    return filterAllToolCalls(messages, normalizedOptions, preserveToolCallIds);
  }
  if (normalizedOptions.exclude.length === 0) return messages;
  return filterSpecificToolCalls(
    messages,
    { ...normalizedOptions, exclude: normalizedOptions.exclude },
    preserveToolCallIds,
  );
}
