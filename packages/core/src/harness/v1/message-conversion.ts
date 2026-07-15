/**
 * Harness v1 message-conversion helper used by `Session.listMessages`.
 * The public message contract aliases AgentController's canonical message
 * types, while this mapper preserves v1's durable signal metadata semantics.
 *
 * No `this`-coupling, no harness state — pure function over an input row.
 */
import { mastraDBMessageToSignal, signalContentsToParts, signalContentsToText } from '../../agent/signals';
import type { MastraDBMessage } from '../../agent/types';
import type {
  AgentControllerMessage as HarnessMessage,
  AgentControllerMessageContent as HarnessMessageContent,
} from '../../agent-controller/types';

/**
 * Memory-storage row shape that both runtimes feed in. We type the parts
 * loosely because storage backends serialise these as JSON; the converter
 * is responsible for narrowing each part by `type`.
 */
export interface StoredMessageRow {
  id: string;
  role: 'user' | 'assistant' | 'system' | 'signal';
  createdAt: Date;
  content: {
    content?: string;
    parts: Array<{
      type: string;
      text?: string;
      reasoning?: string;
      toolCallId?: string;
      toolName?: string;
      args?: unknown;
      result?: unknown;
      isError?: boolean;
      data?: unknown;
      providerMetadata?: Record<string, unknown>;
      toolInvocation?: {
        state: string;
        toolCallId: string;
        toolName: string;
        args?: unknown;
        result?: unknown;
        isError?: boolean;
      };
      [key: string]: unknown;
    }>;
    metadata?: Record<string, unknown>;
  };
}

function getStringValue(value: unknown): string | undefined {
  return typeof value === 'string' ? value : undefined;
}

function getRecordValue(value: unknown): Record<string, unknown> | undefined {
  return value && typeof value === 'object' && !Array.isArray(value) ? (value as Record<string, unknown>) : undefined;
}

function signalContentsToHarnessContent(contents: unknown): HarnessMessageContent[] {
  return signalContentsToParts(contents).map((part): HarnessMessageContent => {
    if (part.type === 'text') return { type: 'text', text: part.text };
    if (part.mediaType.startsWith('image/')) {
      return { type: 'image', data: part.data, mimeType: part.mediaType };
    }
    return {
      type: 'file',
      data: part.data,
      mediaType: part.mediaType,
      filename: part.filename,
    };
  });
}

function toSystemReminderContent(
  payload: Record<string, unknown>,
): Extract<HarnessMessageContent, { type: 'system_reminder' }> | undefined {
  const attributes = getRecordValue(payload.attributes);
  const metadata = getRecordValue(payload.metadata);
  const message = signalContentsToText(payload.contents) || getStringValue(payload.message);
  if (!message) return undefined;

  return {
    type: 'system_reminder',
    message,
    reminderType:
      getStringValue(payload.reminderType) ??
      getStringValue(attributes?.reminderType) ??
      getStringValue(attributes?.type) ??
      getStringValue(metadata?.reminderType) ??
      getStringValue(payload.type),
    path: getStringValue(payload.path) ?? getStringValue(attributes?.path),
    precedesMessageId: getStringValue(payload.precedesMessageId) ?? getStringValue(attributes?.precedesMessageId),
    gapText: getStringValue(payload.gapText) ?? getStringValue(attributes?.gapText),
    gapMs:
      typeof payload.gapMs === 'number'
        ? payload.gapMs
        : typeof attributes?.gapMs === 'number'
          ? attributes.gapMs
          : undefined,
    timestamp: getStringValue(payload.timestamp) ?? getStringValue(attributes?.timestamp),
    goalMaxTurns:
      typeof payload.goalMaxTurns === 'number'
        ? payload.goalMaxTurns
        : typeof metadata?.goalMaxTurns === 'number'
          ? metadata.goalMaxTurns
          : undefined,
    judgeModelId: getStringValue(payload.judgeModelId) ?? getStringValue(metadata?.judgeModelId),
  };
}

function toUserSignalMessage(payload: Record<string, unknown>): HarnessMessage | undefined {
  const id = getStringValue(payload.id);
  const contents = payload.contents ?? payload.message;
  if (!id || contents === undefined) return undefined;

  const content = signalContentsToHarnessContent(contents);
  if (content.length === 0) return undefined;

  return {
    id,
    role: 'user',
    content,
    createdAt: new Date(getStringValue(payload.createdAt) ?? Date.now()),
  };
}

function toStateSignalContent(
  payload: Record<string, unknown>,
): Extract<HarnessMessageContent, { type: 'state_signal' }> {
  const stateMetadata = getRecordValue(getRecordValue(payload.metadata)?.state);
  const stateId = getStringValue(stateMetadata?.id) ?? getStringValue(payload.tagName) ?? 'state';

  return {
    type: 'state_signal',
    id: getStringValue(payload.id),
    stateId,
    mode: stateMetadata?.mode === 'delta' ? 'delta' : 'snapshot',
    cacheKey: getStringValue(stateMetadata?.cacheKey),
    version: typeof stateMetadata?.version === 'number' ? stateMetadata.version : undefined,
    message: signalContentsToText(payload.contents),
  };
}

function toNotificationSummaryContent(
  payload: Record<string, unknown>,
): Extract<HarnessMessageContent, { type: 'notification_summary' }> {
  const metadataSummary = getRecordValue(getRecordValue(payload.metadata)?.notificationSummary);
  const bySource = getRecordValue(metadataSummary?.bySource) ?? {};
  const byPriority = getRecordValue(metadataSummary?.byPriority) ?? {};
  const notificationIds = Array.isArray(metadataSummary?.notificationIds)
    ? metadataSummary.notificationIds.filter((id): id is string => typeof id === 'string')
    : [];
  const pending = typeof metadataSummary?.pending === 'number' ? metadataSummary.pending : undefined;

  return {
    type: 'notification_summary',
    id: getStringValue(payload.id),
    message: signalContentsToText(payload.contents),
    pending: pending ?? notificationIds.length,
    bySource: Object.fromEntries(
      Object.entries(bySource).filter((entry): entry is [string, number] => typeof entry[1] === 'number'),
    ),
    byPriority: Object.fromEntries(
      Object.entries(byPriority).filter((entry): entry is [string, number] => typeof entry[1] === 'number'),
    ),
    notificationIds,
  };
}

function toReactiveSignalContent(
  payload: Record<string, unknown>,
): Extract<HarnessMessageContent, { type: 'reactive_signal' }> | undefined {
  const tagName = getStringValue(payload.tagName);
  if (!tagName) return undefined;

  return {
    type: 'reactive_signal',
    id: getStringValue(payload.id),
    tagName,
    message: signalContentsToText(payload.contents),
    attributes: getRecordValue(payload.attributes),
    metadata: getRecordValue(payload.metadata),
  };
}

function toNotificationContent(
  payload: Record<string, unknown>,
): Extract<HarnessMessageContent, { type: 'notification' }> | undefined {
  const attributes = getRecordValue(payload.attributes) ?? {};
  const metadata = getRecordValue(payload.metadata) ?? {};
  const notificationMetadata = getRecordValue(metadata.notification);
  const message = signalContentsToText(payload.contents);
  if (!message) return undefined;

  return {
    type: 'notification',
    id: getStringValue(payload.id),
    notificationId: getStringValue(attributes.id) ?? getStringValue(notificationMetadata?.recordId),
    message,
    source: getStringValue(attributes.source) ?? getStringValue(notificationMetadata?.source),
    kind:
      getStringValue(attributes.kind) ?? getStringValue(attributes.type) ?? getStringValue(notificationMetadata?.kind),
    priority: getStringValue(attributes.priority) ?? getStringValue(notificationMetadata?.priority),
    status: getStringValue(attributes.status) ?? getStringValue(notificationMetadata?.status),
    attributes,
    metadata,
  };
}

/**
 * Convert a stored message row into the public `HarnessMessage` partition.
 *
 * - System-reminder messages (carried on `content.metadata.systemReminder`)
 *   collapse into a single `system_reminder` content part — these are
 *   harness-injected reminders, not actual model output, so the rest of the
 *   parts array is dropped.
 * - Tool invocations split into `tool_call` + (optional) `tool_result`
 *   pairs so the renderer can interleave them with text/thinking content.
 * - OM (`om_*`) and harness-injected data parts pass through with their
 *   payloads narrowed.
 * - Unknown part types are dropped silently — this keeps forward-compat
 *   with new storage shapes.
 */
export function convertStoredMessageToHarnessMessage(msg: StoredMessageRow): HarnessMessage {
  const content: HarnessMessageContent[] = [];
  const systemReminder = getRecordValue(msg.content.metadata?.systemReminder);

  if (systemReminder && typeof systemReminder.type === 'string') {
    const reminder = toSystemReminderContent({
      ...systemReminder,
      contents: typeof systemReminder.message === 'string' ? systemReminder.message : '',
      reminderType: systemReminder.type,
    });
    if (reminder) {
      content.push(reminder);
    }

    return { id: msg.id, role: msg.role === 'signal' ? 'user' : msg.role, content, createdAt: msg.createdAt };
  }

  const signalMetadata = getRecordValue(msg.content.metadata?.signal);
  if (signalMetadata?.type === 'user-message') {
    const signalContent = signalContentsToHarnessContent(signalMetadata.contents ?? msg.content.content);
    if (signalContent.length > 0) {
      return { id: msg.id, role: 'user', content: signalContent, createdAt: msg.createdAt };
    }
  }

  if (signalMetadata?.type === 'system-reminder') {
    const reminder = toSystemReminderContent({
      type: signalMetadata.type,
      contents: signalMetadata.contents ?? msg.content.content,
      attributes: getRecordValue(signalMetadata.attributes) ?? msg.content.metadata,
      metadata: getRecordValue(signalMetadata.metadata),
    });
    if (reminder) {
      content.push(reminder);
      return { id: msg.id, role: 'user', content, createdAt: msg.createdAt };
    }
  }

  if (msg.role === 'signal') {
    const signal = mastraDBMessageToSignal(msg as MastraDBMessage);

    if (signal.type === 'user') {
      const signalContent = signalContentsToHarnessContent(signal.contents);
      if (signalContent.length > 0) {
        return { id: msg.id, role: 'user', content: signalContent, createdAt: msg.createdAt };
      }
    }

    if (signal.type === 'state') {
      content.push(
        toStateSignalContent({
          id: signal.id,
          tagName: signal.tagName,
          contents: signal.contents,
          metadata: signal.metadata,
        }),
      );
      return { id: msg.id, role: 'user', content, createdAt: msg.createdAt };
    }

    if (signal.type === 'reactive' && signal.tagName === 'system-reminder') {
      const reminder = toSystemReminderContent({
        type: signal.type,
        contents: signal.contents,
        attributes: signal.attributes ?? msg.content.metadata,
        metadata: signal.metadata,
      });
      if (reminder) {
        content.push(reminder);
      }

      return { id: msg.id, role: 'user', content, createdAt: msg.createdAt };
    }

    if (signal.type === 'notification' && signal.tagName === 'notification-summary') {
      content.push(
        toNotificationSummaryContent({
          id: signal.id,
          contents: signal.contents,
          attributes: signal.attributes,
          metadata: signal.metadata,
        }),
      );
      return { id: msg.id, role: 'user', content, createdAt: msg.createdAt };
    }

    if (signal.type === 'notification' && signal.tagName === 'notification') {
      const notification = toNotificationContent({
        id: signal.id,
        contents: signal.contents,
        attributes: signal.attributes,
        metadata: signal.metadata,
      });
      if (notification) content.push(notification);
      return { id: msg.id, role: 'user', content, createdAt: msg.createdAt };
    }

    if (signal.type === 'reactive') {
      const reactive = toReactiveSignalContent({
        id: signal.id,
        tagName: signal.tagName,
        contents: signal.contents,
        attributes: signal.attributes,
        metadata: signal.metadata,
      });
      if (reactive) content.push(reactive);
      return { id: msg.id, role: 'user', content, createdAt: msg.createdAt };
    }
  }

  for (const part of msg.content.parts) {
    switch (part.type) {
      case 'text':
        if (part.text) {
          content.push({ type: 'text', text: part.text });
        }
        break;
      case 'reasoning':
        if (part.reasoning) {
          content.push({ type: 'thinking', thinking: part.reasoning });
        }
        break;
      case 'tool-invocation':
        if (part.toolInvocation) {
          const inv = part.toolInvocation;
          content.push({ type: 'tool_call', id: inv.toolCallId, name: inv.toolName, args: inv.args });
          if (inv.state === 'result') {
            content.push({
              type: 'tool_result',
              id: inv.toolCallId,
              name: inv.toolName,
              result: inv.result,
              isError: inv.isError ?? false,
              ...(part.providerMetadata ? { providerMetadata: part.providerMetadata } : {}),
            });
          }
        } else if (part.toolCallId && part.toolName) {
          content.push({ type: 'tool_call', id: part.toolCallId, name: part.toolName, args: part.args });
        }
        break;
      case 'tool-call':
        if (part.toolCallId && part.toolName) {
          content.push({ type: 'tool_call', id: part.toolCallId, name: part.toolName, args: part.args });
        }
        break;
      case 'tool-result':
        if (part.toolCallId && part.toolName) {
          content.push({
            type: 'tool_result',
            id: part.toolCallId,
            name: part.toolName,
            result: part.result,
            isError: part.isError ?? false,
            ...(part.providerMetadata ? { providerMetadata: part.providerMetadata } : {}),
          });
        }
        break;
      case 'data-om-observation-start': {
        const data = (part as { data?: Record<string, unknown> }).data ?? {};
        content.push({
          type: 'om_observation_start',
          tokensToObserve: (data.tokensToObserve as number) ?? 0,
          operationType: (data.operationType as 'observation' | 'reflection') ?? 'observation',
        });
        break;
      }
      case 'data-om-observation-end': {
        const data = (part as { data?: Record<string, unknown> }).data ?? {};
        content.push({
          type: 'om_observation_end',
          tokensObserved: (data.tokensObserved as number) ?? 0,
          observationTokens: (data.observationTokens as number) ?? 0,
          durationMs: (data.durationMs as number) ?? 0,
          operationType: (data.operationType as 'observation' | 'reflection') ?? 'observation',
          observations: (data.observations as string) ?? undefined,
          currentTask: (data.currentTask as string) ?? undefined,
          suggestedResponse: (data.suggestedResponse as string) ?? undefined,
        });
        break;
      }
      case 'data-om-observation-failed': {
        const data = (part as { data?: Record<string, unknown> }).data ?? {};
        content.push({
          type: 'om_observation_failed',
          error: (data.error as string) ?? 'Unknown error',
          tokensAttempted: (data.tokensAttempted as number) ?? 0,
          operationType: (data.operationType as 'observation' | 'reflection') ?? 'observation',
        });
        break;
      }
      case 'data-signal': {
        const data = (part as { data?: Record<string, unknown> }).data ?? {};
        if (data.type === 'state') {
          content.push(toStateSignalContent(data));
        } else if (data.type === 'reactive' && data.tagName === 'system-reminder') {
          const reminder = toSystemReminderContent(data);
          if (reminder) content.push(reminder);
        } else if (data.type === 'notification' && data.tagName === 'notification-summary') {
          content.push(toNotificationSummaryContent(data));
        } else if (data.type === 'notification' && data.tagName === 'notification') {
          const notification = toNotificationContent(data);
          if (notification) content.push(notification);
        } else if (data.type === 'reactive') {
          const reactive = toReactiveSignalContent(data);
          if (reactive) content.push(reactive);
        }
        break;
      }
      case 'data-user-message': {
        const data = (part as { data?: Record<string, unknown> }).data ?? {};
        const message = toUserSignalMessage(data);
        if (message) {
          content.push(...message.content);
        }
        break;
      }
      case 'data-system-reminder': {
        const data = (part as { data?: Record<string, unknown> }).data ?? {};
        const reminder = toSystemReminderContent(data);
        if (reminder) {
          content.push(reminder);
        }
        break;
      }
      case 'file':
        if (typeof part.data !== 'string') {
          console.warn('[Harness] Skipping file part with non-string data:', typeof part.data);
          break;
        }
        content.push({
          type: 'file',
          data: part.data,
          mediaType:
            (part as { mediaType?: string }).mediaType ??
            (part as { mimeType?: string }).mimeType ??
            'application/octet-stream',
          ...((part as { filename?: string }).filename ? { filename: (part as { filename?: string }).filename } : {}),
        });
        break;
      case 'image': {
        const imgData =
          typeof part.data === 'string'
            ? part.data
            : typeof (part as { image?: string }).image === 'string'
              ? (part as { image?: string }).image!
              : '';
        content.push({
          type: 'image',
          data: imgData,
          mimeType:
            (part as { mimeType?: string }).mimeType ?? (part as { mediaType?: string }).mediaType ?? 'image/png',
        });
        break;
      }
      case 'data-om-thread-update': {
        const data = (part as { data?: Record<string, unknown> }).data ?? {};
        if (data.newTitle) {
          content.push({
            type: 'om_thread_title_updated',
            threadId: (data.threadId as string) ?? '',
            oldTitle: (data.oldTitle as string) ?? undefined,
            newTitle: data.newTitle as string,
          });
        }
        break;
      }
      // Skip other part types (step-start, data-om-status, etc.)
    }
  }

  return { id: msg.id, role: msg.role === 'signal' ? 'user' : msg.role, content, createdAt: msg.createdAt };
}
