import { createHash } from 'node:crypto';
import { PubSub as PubSubClient } from '@google-cloud/pubsub';
import type { ClientConfig, Message, Subscription } from '@google-cloud/pubsub';
import { PubSub } from '@mastra/core/events';
import type { Event, EventCallback, PublishEvent, SubscribeOptions } from '@mastra/core/events';

const LOGICAL_TOPIC_ATTRIBUTE = 'mastraTopic';
const LOGICAL_TOPIC_HASH_ATTRIBUTE = 'mastraTopicHash';
const MAX_ROUTING_VALUE_BYTES = 1024;

function brokerTopicName(topic: string): string {
  const workflowEventsTopic = /^(workflow\.events\.v[12])\..+$/.exec(topic);
  if (workflowEventsTopic) return workflowEventsTopic[1]!;
  if (/^workflow\.lifecycle\.v1\..+$/.test(topic)) return 'workflow.lifecycle.v1';
  return topic;
}

function isLifecycleTopic(topic: string): boolean {
  return /^workflow\.lifecycle\.v1\..+$/.test(topic);
}

function logicalTopicHash(topic: string): string {
  return createHash('sha256').update(topic).digest('hex').slice(0, 16);
}

function logicalTopicRoutingHash(topic: string): string {
  return createHash('sha256').update(topic).digest('hex');
}

function assertBrokerRoutingTopic(topic: string): void {
  if (Buffer.byteLength(topic, 'utf8') > MAX_ROUTING_VALUE_BYTES) {
    throw new RangeError(`Logical Pub/Sub topic exceeds the ${MAX_ROUTING_VALUE_BYTES}-byte routing limit`);
  }
}

function decodedDate(value: unknown): Date | undefined {
  if (value instanceof Date) return value;
  if (typeof value === 'string' || typeof value === 'number') {
    const parsed = new Date(value);
    if (!Number.isNaN(parsed.getTime())) return parsed;
  }
  return undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function optionalSafeInteger(value: unknown): number | undefined {
  return typeof value === 'number' && Number.isSafeInteger(value) ? value : undefined;
}

function subscriptionResourceName(value: string): string {
  const sanitized = value.replace(/[^A-Za-z0-9\-._~+%]/g, '-');
  const withUniqueSuffix = sanitized === value ? sanitized : `${sanitized}-${logicalTopicHash(value)}`;
  const withValidStart = /^[A-Za-z]/.test(withUniqueSuffix) ? withUniqueSuffix : `mastra-${withUniqueSuffix}`;
  if (withValidStart.length <= 255) return withValidStart;
  return `${withValidStart.slice(0, 238)}-${logicalTopicHash(value)}`;
}

function decodeEvent(value: unknown, requireReplayIdentity: boolean): Event {
  if (!isRecord(value)) throw new TypeError('Pub/Sub event payload must be an object');
  if (
    typeof value.type !== 'string' ||
    value.type.length === 0 ||
    typeof value.id !== 'string' ||
    value.id.length === 0 ||
    typeof value.runId !== 'string' ||
    value.runId.length === 0 ||
    !('data' in value)
  ) {
    throw new TypeError('Pub/Sub event payload is missing canonical identity fields');
  }
  const createdAt = decodedDate(value.createdAt);
  if (!createdAt) throw new TypeError('Pub/Sub event payload has an invalid createdAt value');
  const index = optionalSafeInteger(value.index);
  if (value.index !== undefined && (index === undefined || index < 0)) {
    throw new TypeError('Pub/Sub event payload has an invalid cursor');
  }
  const logGeneration = value.logGeneration;
  if (logGeneration !== undefined && (typeof logGeneration !== 'string' || logGeneration.length === 0)) {
    throw new TypeError('Pub/Sub event payload has an invalid log generation');
  }
  if (requireReplayIdentity && (index === undefined || typeof logGeneration !== 'string')) {
    throw new TypeError('Workflow lifecycle event payload is missing replay identity');
  }
  const deliveryAttempt = optionalSafeInteger(value.deliveryAttempt);
  if (value.deliveryAttempt !== undefined && (deliveryAttempt === undefined || deliveryAttempt < 1)) {
    throw new TypeError('Pub/Sub event payload has an invalid delivery attempt');
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

export class GoogleCloudPubSub extends PubSub {
  private instanceId: string;
  private pubsub: PubSubClient;
  private ackBuffer: Record<string, Promise<any>> = {};
  private activeSubscriptions: Record<string, Subscription> = {};
  private activeCbs: Record<string, Map<string, Set<EventCallback>>> = {};
  private deliveryAttempts: Record<string, Map<string, number>> = {};
  // `localOnly` publishes never touch Google Cloud — they are delivered to
  // same-process subscribers only. Tracks live callbacks per logical topic so
  // normalized workflow broker topics never leak one run into another.
  private localCallbacks: Map<string, Set<EventCallback>> = new Map();
  // Coalesces concurrent init() calls for the same subscription so racing
  // subscribers (e.g. a producer stream and a consumer observe on the same
  // run topic) share a single createTopic/createSubscription attempt.
  private inFlightInit: Record<string, Promise<Subscription | undefined>> = {};
  // Tracks the actual anonymous message listener registered on each subscription,
  // so we can remove it cleanly on the final unsubscribe.
  private messageListeners: Record<string, (message: Message) => void> = {};

  constructor(config: ClientConfig) {
    super();
    this.pubsub = new PubSubClient(config);
    this.instanceId = crypto.randomUUID();
  }

  getSubscriptionName(topic: string, group?: string, logicalTopic: string = topic) {
    const topicIdentity = logicalTopic === topic ? topic : `${topic}-${logicalTopicHash(logicalTopic)}`;
    if (group) {
      return subscriptionResourceName(`${topicIdentity}-${group}`);
    }
    return subscriptionResourceName(`${topicIdentity}-${this.instanceId}`);
  }

  async ackMessage(topic: string, message: Message) {
    try {
      const ackResponse = Promise.race([message.ackWithResponse(), new Promise(resolve => setTimeout(resolve, 5000))]);
      this.ackBuffer[topic + '-' + message.id] = ackResponse.catch(() => {});
      await ackResponse;
      delete this.ackBuffer[topic + '-' + message.id];
    } catch (e) {
      console.error('Error acking message', e);
    }
  }

  async init(topicName: string, group?: string, logicalTopic: string = topicName): Promise<Subscription | undefined> {
    const subscriptionKey = group ? `${logicalTopic}:${group}` : logicalTopic;

    // Reuse an in-flight init so concurrent subscribers don't race to create the
    // same subscription. The promise is registered synchronously below (before any
    // await), so a second caller arriving during the create window reuses it.
    if (this.inFlightInit[subscriptionKey]) {
      return this.inFlightInit[subscriptionKey];
    }

    const subscriptionName = this.getSubscriptionName(topicName, group, logicalTopic);
    const initPromise = (async (): Promise<Subscription | undefined> => {
      try {
        await this.pubsub.createTopic(topicName);
      } catch {
        // no-op
      }
      try {
        const [sub] = await this.pubsub.topic(topicName).createSubscription(subscriptionName, {
          enableMessageOrdering: true,
          enableExactlyOnceDelivery: topicName === 'workflows' || !!group,
          ...(logicalTopic !== topicName
            ? {
                // Pub/Sub filter expressions are limited to 256 bytes. Hash
                // the complete logical topic for broker routing, then verify
                // the unhashed attribute again before delivery.
                filter: `attributes.${LOGICAL_TOPIC_HASH_ATTRIBUTE} = "${logicalTopicRoutingHash(logicalTopic)}"`,
              }
            : {}),
        });
        this.activeSubscriptions[subscriptionKey] = sub;
        return sub;
      } catch (error) {
        // The subscription may already exist: created concurrently by a racing
        // subscriber (ALREADY_EXISTS / gRPC code 6), shared by another process via
        // a group, or surviving a previous process. In all of these cases attach to
        // the existing subscription instead of failing. Ungrouped subscriptions hit
        // this on the concurrent-create race, so we must not gate it on `group`.
        const alreadyExists = (error as { code?: number } | undefined)?.code === 6;
        if (alreadyExists || group) {
          try {
            const sub = this.pubsub.subscription(subscriptionName);
            this.activeSubscriptions[subscriptionKey] = sub;
            return sub;
          } catch {
            // no-op
          }
        }
      }
      return undefined;
    })().finally(() => {
      delete this.inFlightInit[subscriptionKey];
    });

    this.inFlightInit[subscriptionKey] = initPromise;
    return initPromise;
  }

  async destroy(topicName: string) {
    const subName = this.getSubscriptionName(topicName);
    delete this.activeSubscriptions[topicName];
    this.pubsub.subscription(subName).removeAllListeners();
    await this.pubsub.subscription(subName).close();
    await this.pubsub.subscription(subName).delete();
    await this.pubsub.topic(topicName).delete();
  }

  async publish(topicName: string, event: PublishEvent, options?: { localOnly?: boolean }): Promise<void> {
    const logicalTopic = topicName;
    const physicalTopic = brokerTopicName(logicalTopic);
    assertBrokerRoutingTopic(logicalTopic);
    const payload = decodeEvent(
      {
        ...event,
        id: event.id ?? crypto.randomUUID(),
        createdAt: event.createdAt ?? new Date(),
        deliveryAttempt: event.deliveryAttempt ?? 1,
      },
      isLifecycleTopic(logicalTopic),
    );

    // `localOnly` events stay entirely within the publishing process. They are
    // never serialized through Google Cloud, so live methods on payload values
    // (e.g. `MastraModelOutput.getFullOutput`) survive intact. The agent's
    // execution-workflow relies on this: the run result is delivered via
    // `workflows-finish` and includes the `MastraModelOutput` instance —
    // round-tripping it through Pub/Sub would strip its methods.
    if (options?.localOnly) {
      await this.deliverLocal(logicalTopic, payload);
      return;
    }

    const topic = this.pubsub.topic(physicalTopic);

    try {
      await topic.publishMessage({
        data: Buffer.from(JSON.stringify(payload)),
        attributes: {
          [LOGICAL_TOPIC_ATTRIBUTE]: logicalTopic,
          [LOGICAL_TOPIC_HASH_ATTRIBUTE]: logicalTopicRoutingHash(logicalTopic),
        },
        orderingKey: logicalTopic,
      });
    } catch (e: any) {
      if (e.code === 5) {
        await this.pubsub.createTopic(physicalTopic);
        await this.publish(logicalTopic, payload, options);
      } else {
        throw e;
      }
    }
  }

  async subscribe(topic: string, cb: EventCallback, options?: SubscribeOptions): Promise<void> {
    const logicalTopic = topic;
    assertBrokerRoutingTopic(logicalTopic);
    const physicalTopic = brokerTopicName(logicalTopic);

    const group = options?.group;
    // Use a composite key when group is set so grouped and non-grouped subscriptions
    // on the same topic don't collide
    const subscriptionKey = group ? `${logicalTopic}:${group}` : logicalTopic;

    // Register callback for `localOnly` delivery. Local delivery bypasses Google
    // Cloud entirely so live class instances on the payload (e.g. Date, Map,
    // Error, MastraModelOutput) keep their prototypes.
    let localSet = this.localCallbacks.get(logicalTopic);
    if (!localSet) {
      localSet = new Set();
      this.localCallbacks.set(logicalTopic, localSet);
    }
    localSet.add(cb);

    // Update tracked callbacks
    const subscription =
      this.activeSubscriptions[subscriptionKey] ?? (await this.init(physicalTopic, group, logicalTopic));
    if (!subscription) {
      throw new Error(`Failed to subscribe to topic: ${logicalTopic}`);
    }

    this.activeSubscriptions[subscriptionKey] = subscription;

    const callbacksByTopic = this.activeCbs[subscriptionKey] ?? new Map();
    const topicCallbacks = callbacksByTopic.get(logicalTopic) ?? new Set();
    topicCallbacks.add(cb);
    callbacksByTopic.set(logicalTopic, topicCallbacks);
    this.activeCbs[subscriptionKey] = callbacksByTopic;

    if (subscription.isOpen) {
      return;
    }

    const messageListener = (message: Message) => {
      void this.deliverMessage(subscriptionKey, logicalTopic, physicalTopic, message).catch(error => {
        console.error('Error processing event', error);
        try {
          message.nack();
        } catch (nackError) {
          console.error('Error nacking message', nackError);
        }
      });
    };

    this.messageListeners[subscriptionKey] = messageListener;
    subscription.on('message', messageListener);

    subscription.on('error', async error => {
      console.error('subscription error', error);
    });
  }

  async unsubscribe(topic: string, cb: EventCallback): Promise<void> {
    const logicalTopic = topic;

    // Drop from the local-delivery set; if nobody is left, tear down the bucket.
    const localSet = this.localCallbacks.get(logicalTopic);
    if (localSet?.delete(cb) && localSet.size === 0) {
      this.localCallbacks.delete(logicalTopic);
    }

    // Check both grouped and non-grouped subscription keys for this callback
    const keysToCheck = [logicalTopic];
    for (const key of Object.keys(this.activeCbs)) {
      if (key.startsWith(`${logicalTopic}:`) && !keysToCheck.includes(key)) {
        keysToCheck.push(key);
      }
    }

    for (const subscriptionKey of keysToCheck) {
      const callbacksByTopic = this.activeCbs[subscriptionKey];
      const topicCallbacks = callbacksByTopic?.get(logicalTopic);
      if (topicCallbacks?.has(cb)) {
        topicCallbacks.delete(cb);
        if (topicCallbacks.size === 0) {
          callbacksByTopic!.delete(logicalTopic);
        }

        if (callbacksByTopic!.size === 0) {
          const subscription = this.activeSubscriptions[subscriptionKey];
          const listener = this.messageListeners[subscriptionKey];
          if (subscription) {
            if (listener) subscription.removeListener('message', listener);
            await subscription.close();
          }
          delete this.activeSubscriptions[subscriptionKey];
          delete this.activeCbs[subscriptionKey];
          delete this.deliveryAttempts[subscriptionKey];
          delete this.messageListeners[subscriptionKey];
        }
        return;
      }
    }
  }

  async flush(): Promise<void> {
    await Promise.all(Object.values(this.ackBuffer));
  }

  /**
   * Fan a `localOnly` event out to in-process subscribers without going through
   * Google Cloud. The payload is delivered by reference, so live class instances
   * and functions on the event survive intact.
   */
  private async deliverLocal(topicName: string, event: Event): Promise<void> {
    const callbacks = this.localCallbacks.get(topicName);
    if (!callbacks || callbacks.size === 0) return;

    for (const cb of [...callbacks]) {
      try {
        await cb(
          event,
          async () => {},
          async () => {},
        );
      } catch (error) {
        console.error('Error delivering local event', error);
      }
    }
  }

  private async deliverMessage(
    subscriptionKey: string,
    logicalTopic: string,
    physicalTopic: string,
    message: Message,
  ): Promise<void> {
    const callbacksByTopic = this.activeCbs[subscriptionKey];
    const encodedLogicalTopic = message.attributes?.[LOGICAL_TOPIC_ATTRIBUTE];
    const matchesLogicalTopic =
      logicalTopic === physicalTopic
        ? encodedLogicalTopic === undefined || encodedLogicalTopic === logicalTopic
        : encodedLogicalTopic === logicalTopic;
    if (!matchesLogicalTopic) {
      // A filtered subscription should never receive another logical run. Do
      // not acknowledge a misrouted grouped message: that could permanently
      // steal it from the process subscribed to the encoded run.
      message.nack();
      return;
    }

    const decoded: unknown = JSON.parse(message.data.toString());
    const event = decodeEvent(decoded, isLifecycleTopic(logicalTopic));
    const attempts = this.deliveryAttempts[subscriptionKey] ?? new Map<string, number>();
    const observedAttempt = (attempts.get(event.id) ?? 0) + 1;
    attempts.set(event.id, observedAttempt);
    this.deliveryAttempts[subscriptionKey] = attempts;
    const brokerAttempt =
      Number.isSafeInteger(message.deliveryAttempt) && message.deliveryAttempt >= 1 ? message.deliveryAttempt : 0;
    event.deliveryAttempt = Math.max(observedAttempt, brokerAttempt, event.deliveryAttempt ?? 1);

    const callbacks = [...(callbacksByTopic?.get(logicalTopic) ?? [])];
    if (callbacks.length === 0) {
      await this.ackMessage(subscriptionKey, message);
      attempts.delete(event.id);
      return;
    }

    const outcomes = await Promise.all(
      callbacks.map(async callback => {
        let outcome: 'ack' | 'nack' | undefined;
        const ack = async () => {
          if (outcome === undefined) outcome = 'ack';
        };
        const nack = async () => {
          outcome = 'nack';
        };
        try {
          await callback(event, ack, nack);
        } catch {
          outcome = 'nack';
        }
        return outcome;
      }),
    );

    if (outcomes.some(outcome => outcome === 'nack')) {
      message.nack();
    } else if (outcomes.every(outcome => outcome === 'ack')) {
      await this.ackMessage(subscriptionKey, message);
      attempts.delete(event.id);
    }
  }
}
