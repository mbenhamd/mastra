import { createHash } from 'node:crypto';
import { PubSub } from '@mastra/core/events';
import type { Event, EventCallback, PublishEvent, SubscribeOptions } from '@mastra/core/events';
import type { IMastraLogger } from '@mastra/core/logger';
import type { Inngest } from 'inngest';
import { subscribe } from 'inngest/realtime';

/**
 * Build a TopicRef compatible with Inngest SDK v4's `inngest.realtime.publish()`.
 * The runtime only requires `channel` and `topic`; `config.schema` is optional and
 * we leave it absent so no validation runs.
 */
function buildTopicRef(channel: string, topic: string) {
  return { channel, topic, config: {} as any };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function eventCreatedAt(value: unknown): Date | undefined {
  if (value instanceof Date) return value;
  if (typeof value === 'string' || typeof value === 'number') {
    const parsed = new Date(value);
    if (!Number.isNaN(parsed.getTime())) return parsed;
  }
  return undefined;
}

function optionalSafeInteger(value: unknown): number | undefined {
  return typeof value === 'number' && Number.isSafeInteger(value) ? value : undefined;
}

function decodeEvent(value: unknown, requireReplayIdentity: boolean): Event {
  if (!isRecord(value)) throw new TypeError('Inngest event payload must be an object');
  if (
    typeof value.type !== 'string' ||
    value.type.length === 0 ||
    typeof value.id !== 'string' ||
    value.id.length === 0 ||
    typeof value.runId !== 'string' ||
    value.runId.length === 0 ||
    !('data' in value)
  ) {
    throw new TypeError('Inngest event payload is missing canonical identity fields');
  }
  const createdAt = eventCreatedAt(value.createdAt);
  if (!createdAt) throw new TypeError('Inngest event payload has an invalid createdAt value');
  const index = optionalSafeInteger(value.index);
  if (value.index !== undefined && (index === undefined || index < 0)) {
    throw new TypeError('Inngest event payload has an invalid cursor');
  }
  const logGeneration = value.logGeneration;
  if (logGeneration !== undefined && (typeof logGeneration !== 'string' || logGeneration.length === 0)) {
    throw new TypeError('Inngest event payload has an invalid log generation');
  }
  if (requireReplayIdentity && (index === undefined || typeof logGeneration !== 'string')) {
    throw new TypeError('Workflow lifecycle event payload is missing replay identity');
  }
  const deliveryAttempt = optionalSafeInteger(value.deliveryAttempt);
  if (value.deliveryAttempt !== undefined && (deliveryAttempt === undefined || deliveryAttempt < 1)) {
    throw new TypeError('Inngest event payload has an invalid delivery attempt');
  }
  return {
    type: value.type,
    id: value.id,
    runId: value.runId,
    data: value.data,
    createdAt,
    ...(typeof index === 'number' ? { index } : {}),
    ...(typeof logGeneration === 'string' ? { logGeneration } : {}),
    ...(typeof deliveryAttempt === 'number' && deliveryAttempt >= 1 ? { deliveryAttempt } : {}),
  };
}

type TopicRoute = {
  topicType: 'workflow' | 'lifecycle' | 'agent' | 'control';
  channel: string;
  inngestTopic: string;
  runId: string;
  requireReplayIdentity: boolean;
};

function lifecycleChannel(topic: string): string {
  return `workflow-lifecycle:${createHash('sha256').update(topic).digest('hex').slice(0, 32)}`;
}

/**
 * Parse a topic string and extract the runId and topic type.
 *
 * Supported formats:
 * - "workflow.lifecycle.v1.{workflowId}.{runId}.{executionGeneration}" - replayable lifecycle events
 * - "workflow.events.v2.{runId}" - legacy workflow watch events
 * - "agent.stream.{runId}" - agent stream events
 * - "agent.control.{runId}.{runtimeBindingId}" - binding-scoped durable-agent control events
 *
 * @returns { runId, topicType } or null if not a recognized format
 */
function parseTopic(topic: string, workflowId: string): TopicRoute | null {
  const lifecyclePrefix = 'workflow.lifecycle.v1.';
  if (topic.startsWith(lifecyclePrefix)) {
    const segments = topic.slice(lifecyclePrefix.length).split('.');
    if (segments.length === 3 && segments.every(Boolean)) {
      try {
        const [encodedWorkflowId, encodedRunId, encodedExecutionGeneration] = segments as [string, string, string];
        const decodedWorkflowId = decodeURIComponent(encodedWorkflowId);
        const runId = decodeURIComponent(encodedRunId);
        const executionGeneration = decodeURIComponent(encodedExecutionGeneration);
        if (decodedWorkflowId === workflowId && runId.length > 0 && executionGeneration.length > 0) {
          return {
            runId,
            topicType: 'lifecycle',
            channel: lifecycleChannel(topic),
            inngestTopic: 'lifecycle',
            requireReplayIdentity: true,
          };
        }
      } catch {
        // Malformed URI encoding is not a canonical lifecycle topic.
      }
    }
  }
  // Try workflow format first
  const workflowMatch = topic.match(/^workflow\.events\.v2\.(.+)$/);
  if (workflowMatch && workflowMatch[1]) {
    return {
      runId: workflowMatch[1],
      topicType: 'workflow',
      channel: `workflow:${workflowId}:${workflowMatch[1]}`,
      inngestTopic: 'watch',
      requireReplayIdentity: false,
    };
  }

  // Try agent stream format
  const agentMatch = topic.match(/^agent\.stream\.(.+)$/);
  if (agentMatch && agentMatch[1]) {
    return {
      runId: agentMatch[1],
      topicType: 'agent',
      channel: `agent:${agentMatch[1]}`,
      inngestTopic: 'agent-stream',
      requireReplayIdentity: false,
    };
  }

  // Durable control events are scoped to one immutable runtime binding so a
  // retained abort cannot affect a later execution that reuses the run ID.
  const controlPrefix = 'agent.control.';
  if (topic.startsWith(controlPrefix)) {
    const encodedParts = topic.slice(controlPrefix.length).split('.');
    if (encodedParts.length === 2 && encodedParts.every(Boolean)) {
      try {
        const [encodedRunId, encodedRuntimeBindingId] = encodedParts as [string, string];
        const runId = decodeURIComponent(encodedRunId);
        const runtimeBindingId = decodeURIComponent(encodedRuntimeBindingId);
        if (runId.length > 0 && runtimeBindingId.length > 0) {
          return {
            runId,
            topicType: 'control',
            channel: `agent:${encodedRunId}.${encodedRuntimeBindingId}`,
            inngestTopic: 'agent-control',
            requireReplayIdentity: false,
          };
        }
      } catch {
        // Malformed URI encoding is not a canonical control topic.
      }
    }
  }

  return null;
}

/**
 * Warn once per unrecognized topic family so a missing topic mapping never fails
 * silently again (dropped `agent.control.*` aborts shipped invisibly — see #22543).
 * Deduped on the topic's leading two segments so run-scoped topics neither spam
 * the logs nor grow the set unboundedly.
 */
const warnedUnrecognizedTopics = new Set<string>();
function warnUnrecognizedTopic(topic: string): void {
  const family = topic.split('.').slice(0, 2).join('.');
  if (warnedUnrecognizedTopics.has(family)) return;
  warnedUnrecognizedTopics.add(family);
  console.warn(`InngestPubSub: ignoring unrecognized topic format "${topic}"`);
}

/**
 * Inngest Realtime delivers messages over ~512 KB truncated, as an unparseable
 * string (#20671). Stay safely under that cap.
 */
const REALTIME_MAX_BYTES = 480 * 1024;
const TERMINAL_AGENT_EVENTS = new Set(['finish', 'error', 'abort']);
const OVERSIZED_ERROR_MESSAGE = 'Agent error event exceeded the Inngest Realtime size limit';

/** Stream consumers read `data.error.name` and `data.error.message`; keep those, shortened to fit. */
function fitErrorEvent(event: Omit<Event, 'id' | 'createdAt'>): Omit<Event, 'id' | 'createdAt'> {
  const error = (event.data as { error?: { name?: unknown; message?: unknown } } | undefined)?.error;
  const name = typeof error?.name === 'string' ? error.name.slice(0, 200) : 'Error';
  const message = typeof error?.message === 'string' ? error.message : OVERSIZED_ERROR_MESSAGE;
  const reduced = { type: event.type, runId: event.runId, data: { error: { name, message } } };
  const excess = byteSize(reduced) - REALTIME_MAX_BYTES;
  if (excess > 0)
    reduced.data.error.message = message.slice(0, Math.max(0, message.length - excess - 64)) + '… [truncated]';
  return byteSize(reduced) <= REALTIME_MAX_BYTES
    ? reduced
    : { ...reduced, data: { error: { name, message: OVERSIZED_ERROR_MESSAGE } } };
}

/** Chunk type streamed in place of an agent event that exceeded the Realtime cap. */
export const OVERSIZED_EVENT_CHUNK_TYPE = 'data-oversized-event';

function oversizedPlaceholder(
  runId: string,
  eventType: string,
  chunkType: string | undefined,
  bytes: number | undefined,
) {
  return {
    type: 'chunk',
    runId,
    data: {
      type: OVERSIZED_EVENT_CHUNK_TYPE,
      transient: true,
      data: { eventType, chunkType, bytes, limit: REALTIME_MAX_BYTES },
    },
  };
}

function byteSize(value: unknown): number {
  try {
    return Buffer.byteLength(JSON.stringify(value) ?? '', 'utf8');
  } catch {
    return 0;
  }
}

/**
 * Reduce an agent event that would exceed the Realtime cap. `finish` keeps the
 * fields stream consumers read (text, usage, stepResult) and drops the
 * per-step record; other oversized events are replaced by a small
 * `data-oversized-event` chunk rather than delivered truncated.
 */
function fitAgentEvent(event: Omit<Event, 'id' | 'createdAt'>): Omit<Event, 'id' | 'createdAt'> {
  const bytes = byteSize(event);
  if (bytes <= REALTIME_MAX_BYTES) return event;
  if (event.type !== 'finish') {
    if (event.type === 'error') return fitErrorEvent(event);
    if (TERMINAL_AGENT_EVENTS.has(event.type)) return { type: event.type, runId: event.runId, data: {} };
    return oversizedPlaceholder(event.runId, event.type, (event.data as { type?: string } | undefined)?.type, bytes);
  }
  const data = (event.data ?? {}) as { output?: Record<string, unknown>; stepResult?: unknown };
  const output = data.output ?? {};
  const reduced = {
    ...event,
    data: {
      stepResult: data.stepResult,
      output: { text: output.text, usage: output.usage, steps: [] },
    },
  };
  if (byteSize(reduced) <= REALTIME_MAX_BYTES) return reduced;
  // stepResult is too large: keep only its reason, then add back each output field that still fits.
  const reason = (data.stepResult as { reason?: unknown } | undefined)?.reason;
  const fallback = {
    ...event,
    data: {
      stepResult: typeof reason === 'string' ? { reason } : undefined,
      output: { steps: [] } as Record<string, unknown>,
    },
  };
  for (const key of ['text', 'usage'] as const) {
    if (output[key] === undefined) continue;
    fallback.data.output[key] = output[key];
    if (byteSize(fallback) > REALTIME_MAX_BYTES) delete fallback.data.output[key];
  }
  return fallback;
}

/**
 * Recover an agent event Realtime truncated into a raw string. Terminal events are
 * salvaged into a minimal envelope so attached streams close; anything else becomes
 * a `data-oversized-event` placeholder chunk.
 */
function salvageTruncatedAgentEvent(raw: string, runId: string): { type: string; runId: string; data: unknown } {
  try {
    const parsed = JSON.parse(raw);
    if (parsed?.type && parsed?.runId) return parsed;
  } catch {
    // truncated
  }
  const type = raw.slice(0, 256).match(/"type"\s*:\s*"([^"]+)"/)?.[1];
  if (!type || !TERMINAL_AGENT_EVENTS.has(type)) {
    const chunkType = raw.match(/"data"\s*:\s*\{\s*"type"\s*:\s*"([^"]+)"/)?.[1];
    return oversizedPlaceholder(runId, type ?? 'unknown', chunkType, undefined);
  }
  const reason = raw.match(/"stepResult"\s*:\s*\{[^{}]*"reason"\s*:\s*"([^"]+)"/)?.[1];
  return {
    type,
    runId,
    data:
      type === 'finish'
        ? { output: { steps: [] }, stepResult: reason ? { reason } : undefined }
        : type === 'error'
          ? { error: { name: 'Error', message: OVERSIZED_ERROR_MESSAGE } }
          : {},
  };
}

/**
 * PubSub implementation for Inngest workflows.
 *
 * This bridges the PubSub abstract class interface with Inngest's realtime system:
 * - publish() uses `inngest.realtime.publish()` (Inngest SDK v4 client API).
 *   This is non-durable: it executes immediately and is not memoized as a step.
 *   When called inside an Inngest function it auto-includes the current runId.
 * - subscribe() uses `inngest/realtime` subscribe for real-time streaming.
 *
 * Supported topic formats:
 * - "workflow.lifecycle.v1.{workflowId}.{runId}.{executionGeneration}"
 *   -> hashed run-generation channel, topic: "lifecycle"
 * - "workflow.events.v2.{runId}" - workflow events
 *   -> Inngest channel: "workflow:{workflowId}:{runId}", topic: "watch"
 * - "agent.stream.{runId}" - agent stream events (for InngestAgent)
 *   -> Inngest channel: "agent:{runId}", topic: "agent-stream"
 * - "agent.control.{runId}" - agent control events (cross-process abort)
 *   -> Inngest channel: "agent:{runId}", topic: "agent-control"
 */
export class InngestPubSub extends PubSub {
  private inngest: Inngest;
  private workflowId: string;
  private realtimeSubscribe: typeof subscribe;
  private logger?: IMastraLogger;
  private subscriptions: Map<
    string,
    {
      unsubscribe?: () => void;
      callbacks: Set<EventCallback>;
      ready: Promise<void>;
    }
  > = new Map();

  constructor(
    inngest: Inngest,
    workflowId: string,
    logger?: IMastraLogger,
    realtimeSubscribe: typeof subscribe = subscribe,
  ) {
    super();
    this.inngest = inngest;
    this.workflowId = workflowId;
    this.logger = logger;
    this.realtimeSubscribe = realtimeSubscribe;
  }

  async publishWorkflowWatchTo(workflowId: string, runId: string, data: unknown): Promise<void> {
    const payload = decodeEvent(
      {
        type: 'watch',
        id: crypto.randomUUID(),
        runId,
        data,
        createdAt: new Date(),
        deliveryAttempt: 1,
      },
      false,
    );
    await this.inngest.realtime.publish(buildTopicRef(`workflow:${workflowId}:${runId}`, 'watch'), payload);
  }

  /**
   * Publish an event to Inngest's realtime system.
   *
   * Supported topic formats:
   * - "workflow.events.v2.{runId}" - workflow events
   *   -> channel: "workflow:{workflowId}:{runId}", topic: "watch"
   * - "agent.stream.{runId}" - agent stream events
   *   -> channel: "agent:{runId}", topic: "agent-stream"
   * - "agent.control.{runId}.{runtimeBindingId}" - durable-agent control events
   *   -> channel: "agent:{runId}.{runtimeBindingId}", topic: "agent-control"
   *   (Binding-scoped channels isolate retained aborts across run-ID reuse.)
   */
  async publish(topic: string, event: PublishEvent, options?: { localOnly?: boolean }): Promise<void> {
    const parsed = parseTopic(topic, this.workflowId);
    if (!parsed) {
      warnUnrecognizedTopic(topic);
      return; // Ignore unrecognized topic formats
    }

    const { runId, topicType, inngestTopic, channel, requireReplayIdentity } = parsed;
    const payload = decodeEvent(
      {
        ...event,
        id: event.id ?? crypto.randomUUID(),
        createdAt: event.createdAt ?? new Date(),
        deliveryAttempt: event.deliveryAttempt ?? 1,
      },
      requireReplayIdentity,
    );
    if (payload.runId !== runId) {
      throw new TypeError(`Inngest event runId ${payload.runId} does not match topic runId ${runId}`);
    }

    if (options?.localOnly) {
      await this.deliverLocal(topic, payload);
      return;
    }

    try {
      // Always send the PubSub envelope. CachingPubSub assigns stable identity
      // and an indexed cursor before delegating here; stripping the envelope
      // would make live Inngest delivery disagree with replayed cache history.
      // Agent events over the Realtime size cap are reduced (see fitAgentEvent),
      // keeping the envelope's identity and cursor so replay and live delivery agree.
      let dataToSend: Event = payload;
      if (topicType === 'agent' || topicType === 'control') {
        const fitted = fitAgentEvent(payload);
        if (fitted !== payload) {
          const message = `InngestPubSub: "${payload.type}" event for run ${runId} exceeds the Inngest Realtime size limit (${REALTIME_MAX_BYTES} bytes); sending a reduced event instead`;
          if (this.logger) this.logger.warn(message);
          else console.warn(message);
          dataToSend = { ...payload, type: fitted.type, data: fitted.data };
        }
      }
      await this.inngest.realtime.publish(buildTopicRef(channel, inngestTopic), dataToSend);
    } catch (err: any) {
      const isCriticalTerminalChunk =
        topicType === 'agent' &&
        event.type === 'chunk' &&
        (event.data as { type?: unknown } | undefined)?.type === 'data-terminal-tool-result';
      // For agent stream terminal events, rethrow. FINISH repeats the terminal
      // envelope as a replay fallback, while retry-safe chunk ids de-duplicate
      // a terminal chunk if this publish committed before the worker crashed.
      if (
        topicType === 'lifecycle' ||
        topicType === 'control' ||
        isCriticalTerminalChunk ||
        (topicType === 'agent' && (event.type === 'finish' || event.type === 'error'))
      ) {
        throw err;
      }
      // Non-terminal events: log but don't throw
      console.error('InngestPubSub publish error:', err?.message ?? err);
    }
  }

  /**
   * Subscribe to events from Inngest's realtime system.
   *
   * Supported topic formats:
   * - "workflow.lifecycle.v1.{workflowId}.{runId}.{executionGeneration}"
   *   -> hashed run-generation channel, topic: "lifecycle"
   * - "workflow.events.v2.{runId}" - workflow events
   *   -> channel: "workflow:{workflowId}:{runId}", topic: "watch"
   * - "agent.stream.{runId}" - agent stream events
   *   -> channel: "agent:{runId}", topic: "agent-stream"
   * - "agent.control.{runId}.{runtimeBindingId}" - durable-agent control events
   *   -> channel: "agent:{runId}.{runtimeBindingId}", topic: "agent-control"
   *   (Binding-scoped channels isolate retained aborts across run-ID reuse.)
   */
  async subscribe(topic: string, cb: EventCallback, _options?: SubscribeOptions): Promise<void> {
    const parsed = parseTopic(topic, this.workflowId);
    if (!parsed) {
      warnUnrecognizedTopic(topic);
      return; // Ignore unrecognized topic formats
    }

    const { runId, topicType, inngestTopic, channel, requireReplayIdentity } = parsed;
    const salvageAgentEvent = topicType === 'agent' || topicType === 'control';

    // Register the topic before awaiting the websocket. Concurrent subscribers
    // then share one initialization promise instead of opening two streams and
    // leaking whichever stream loses the final Map write.
    const existing = this.subscriptions.get(topic);
    if (existing) {
      existing.callbacks.add(cb);
      await existing.ready;
      return;
    }

    const callbacks = new Set<EventCallback>([cb]);
    const subscription = {
      callbacks,
      ready: Promise.resolve(),
    } as {
      unsubscribe?: () => void;
      callbacks: Set<EventCallback>;
      ready: Promise<void>;
    };
    this.subscriptions.set(topic, subscription);
    // Await the subscribe call to ensure the WebSocket connection is established
    // before we consider the subscription "ready". This prevents race conditions
    // where the workflow triggers before the subscription can receive events.
    subscription.ready = (async () => {
      const realtimeSubscription = await this.realtimeSubscribe({
        channel,
        topics: [inngestTopic],
        app: this.inngest,
        onMessage: async (message: any) => {
          const event = this.toEvent(message, requireReplayIdentity, runId, salvageAgentEvent);
          if (event.runId !== runId) {
            throw new TypeError(`Inngest event runId ${event.runId} does not match topic runId ${runId}`);
          }
          // Inngest Realtime does not expose per-message ack/nack handles. Await
          // every callback and contain rejection here so async subscriber errors
          // never become unhandled rejections in the websocket listener.
          const results = await Promise.allSettled([...callbacks].map(callback => callback(event)));
          for (const result of results) {
            if (result.status === 'rejected') {
              console.error('InngestPubSub subscriber error:', result.reason);
            }
          }
        },
      });
      subscription.unsubscribe = () => {
        try {
          void realtimeSubscription.close();
        } catch (err) {
          console.error('InngestPubSub unsubscribe error:', err);
        }
      };
    })();

    try {
      await subscription.ready;
    } catch (error) {
      if (this.subscriptions.get(topic) === subscription) {
        this.subscriptions.delete(topic);
      }
      throw error;
    }
  }

  /**
   * Unsubscribe a callback from a topic.
   * If no callbacks remain, the underlying Inngest subscription is cancelled.
   */
  async unsubscribe(topic: string, cb: EventCallback): Promise<void> {
    const sub = this.subscriptions.get(topic);
    if (!sub) {
      return;
    }

    sub.callbacks.delete(cb);

    // If no more callbacks, cancel the subscription
    if (sub.callbacks.size === 0) {
      this.subscriptions.delete(topic);
      try {
        await sub.ready;
      } finally {
        sub.unsubscribe?.();
      }
    }
  }

  /**
   * Flush any pending operations. No-op for Inngest.
   */
  async flush(): Promise<void> {
    // No-op for Inngest
  }

  /**
   * Clean up all subscriptions during graceful shutdown.
   */
  async close(): Promise<void> {
    const subscriptions = [...this.subscriptions.values()];
    this.subscriptions.clear();
    await Promise.allSettled(subscriptions.map(sub => sub.ready));
    for (const sub of subscriptions) {
      sub.unsubscribe?.();
    }
  }

  private toEvent(message: unknown, requireReplayIdentity: boolean, runId: string, salvageAgentEvent: boolean): Event {
    const data = isRecord(message) ? message.data : undefined;
    if (salvageAgentEvent && typeof data === 'string') {
      // Realtime truncated an oversized agent event into a raw string (#20671).
      // Terminal events are salvaged so attached streams still close; anything
      // else becomes a placeholder chunk. The truncated cursor is unrecoverable.
      return decodeEvent(
        { id: crypto.randomUUID(), createdAt: new Date(), ...salvageTruncatedAgentEvent(data, runId) },
        requireReplayIdentity,
      );
    }
    return decodeEvent(data, requireReplayIdentity);
  }

  private async deliverLocal(topic: string, event: Event): Promise<void> {
    const callbacks = this.subscriptions.get(topic)?.callbacks;
    if (!callbacks) return;
    const results = await Promise.allSettled(
      [...callbacks].map(callback =>
        callback(
          event,
          async () => {},
          async () => {},
        ),
      ),
    );
    for (const result of results) {
      if (result.status === 'rejected') {
        console.error('InngestPubSub local subscriber error:', result.reason);
      }
    }
  }
}
