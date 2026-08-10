import { createHash } from 'node:crypto';
import { PubSub } from '@mastra/core/events';
import type { Event, EventCallback, PublishEvent, SubscribeOptions } from '@mastra/core/events';
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
 */
export class InngestPubSub extends PubSub {
  private inngest: Inngest;
  private workflowId: string;
  private realtimeSubscribe: typeof subscribe;
  private subscriptions: Map<
    string,
    {
      unsubscribe?: () => void;
      callbacks: Set<EventCallback>;
      ready: Promise<void>;
    }
  > = new Map();

  constructor(inngest: Inngest, workflowId: string, realtimeSubscribe: typeof subscribe = subscribe) {
    super();
    this.inngest = inngest;
    this.workflowId = workflowId;
    this.realtimeSubscribe = realtimeSubscribe;
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
      await this.inngest.realtime.publish(buildTopicRef(channel, inngestTopic), payload);
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
      return; // Ignore unrecognized topic formats
    }

    const { runId, inngestTopic, channel, requireReplayIdentity } = parsed;

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
      const stream = await this.realtimeSubscribe(
        {
          channel,
          topics: [inngestTopic],
          app: this.inngest,
        },
        async (message: any) => {
          const event = this.toEvent(message, requireReplayIdentity);
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
      );
      subscription.unsubscribe = () => {
        try {
          void stream.cancel();
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

  private toEvent(message: unknown, requireReplayIdentity: boolean): Event {
    const data = isRecord(message) ? message.data : undefined;
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
