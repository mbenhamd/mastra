import { PubSub as PubSubClient } from '@google-cloud/pubsub';
import type { Event } from '@mastra/core/events';
import { afterAll, afterEach, describe, expect, it } from 'vitest';
import { GoogleCloudPubSub } from '.';

/**
 * These tests require the Google Cloud PubSub emulator running on localhost:8085.
 *
 * Start it via:
 *   docker compose -f .dev/docker-compose.yaml up -d pubsub-emulator
 *
 * Or directly:
 *   docker run -p 8085:8085 gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators \
 *     gcloud beta emulators pubsub start --host-port=0.0.0.0:8085
 */

const EMULATOR_HOST = process.env.PUBSUB_EMULATOR_HOST ?? 'localhost:8085';

function makeEvent(overrides: Partial<Omit<Event, 'id' | 'createdAt'>> = {}): Omit<Event, 'id' | 'createdAt'> {
  return {
    type: 'test',
    data: {},
    runId: `run-${Date.now()}-${Math.random().toString(36).slice(2)}`,
    ...overrides,
  };
}

function waitForMessages(count: number, collected: Event[], timeoutMs = 10_000): Promise<Event[]> {
  return new Promise((resolve, _reject) => {
    const timeout = setTimeout(() => {
      resolve(collected);
    }, timeoutMs);

    const interval = setInterval(() => {
      if (collected.length >= count) {
        clearTimeout(timeout);
        clearInterval(interval);
        resolve(collected);
      }
    }, 100);
  });
}

// Each test gets unique topic names to avoid cross-test interference
let topicCounter = 0;
function uniqueTopic() {
  return `test-group-${Date.now()}-${topicCounter++}`;
}

describe.sequential('GoogleCloudPubSub group support', () => {
  // All instances created during tests, for cleanup
  const instances: GoogleCloudPubSub[] = [];

  function createPubSub(): GoogleCloudPubSub {
    const ps = new GoogleCloudPubSub({
      projectId: 'pubsub-test',
      apiEndpoint: EMULATOR_HOST,
    });
    instances.push(ps);
    return ps;
  }

  afterEach(async () => {
    // Flush all instances
    for (const ps of instances) {
      await ps.flush().catch(() => {});
    }
  });

  afterAll(async () => {
    instances.length = 0;
  });

  describe('fan-out (existing behavior, no group)', () => {
    it('delivers messages to all subscribers on the same instance', async () => {
      const pubsub = createPubSub();
      const topic = uniqueTopic();

      const collected1: Event[] = [];
      const collected2: Event[] = [];

      await pubsub.subscribe(topic, (event, ack) => {
        collected1.push(event);
        ack?.();
      });
      await pubsub.subscribe(topic, (event, ack) => {
        collected2.push(event);
        ack?.();
      });

      await pubsub.publish(topic, makeEvent({ type: 'hello' }));

      const [msgs1, msgs2] = await Promise.all([
        waitForMessages(1, collected1, 5000),
        waitForMessages(1, collected2, 5000),
      ]);

      expect(msgs1.length).toBe(1);
      expect(msgs2.length).toBe(1);
      expect(msgs1[0]!.type).toBe('hello');
      expect(msgs2[0]!.type).toBe('hello');
    });

    it('two subscribers racing on a fresh topic both attach and receive (issue #18203)', async () => {
      // A producer (agent.stream) and a consumer (agent.observe) can subscribe to the
      // same fresh topic within the subscription-creation window, so both reach init()
      // and race createSubscription for the same name. The loser must attach to the
      // existing subscription instead of failing, and both callbacks must receive events.
      const pubsub = createPubSub();
      const topic = uniqueTopic();

      const collected1: Event[] = [];
      const collected2: Event[] = [];

      // Subscribe concurrently (no await between) so the two init() calls overlap.
      await Promise.all([
        pubsub.subscribe(topic, (event, ack) => {
          collected1.push(event);
          ack?.();
        }),
        pubsub.subscribe(topic, (event, ack) => {
          collected2.push(event);
          ack?.();
        }),
      ]);

      await new Promise(resolve => setTimeout(resolve, 1000));
      await pubsub.publish(topic, makeEvent({ type: 'raced' }));

      const [msgs1, msgs2] = await Promise.all([
        waitForMessages(1, collected1, 5000),
        waitForMessages(1, collected2, 5000),
      ]);

      expect(msgs1.length).toBe(1);
      expect(msgs2.length).toBe(1);
      expect(msgs1[0]!.type).toBe('raced');
      expect(msgs2[0]!.type).toBe('raced');
    });

    it('re-attaches to an existing subscription when createSubscription returns ALREADY_EXISTS (issue #18203)', async () => {
      // Drive the ungrouped recovery branch directly: pre-create the exact subscription
      // the adapter will use (out-of-band) so its own createSubscription loses with
      // ALREADY_EXISTS (gRPC code 6). For an ungrouped topic the adapter must re-attach
      // to the existing subscription instead of returning undefined and throwing. This
      // also mirrors the restart case where a subscription survives a previous process.
      const pubsub = createPubSub();
      const topic = uniqueTopic();
      const subscriptionName = pubsub.getSubscriptionName(topic);

      const raw = new PubSubClient({ projectId: 'pubsub-test', apiEndpoint: EMULATOR_HOST });
      try {
        await raw.createTopic(topic);
        await raw.topic(topic).createSubscription(subscriptionName);

        const collected: Event[] = [];
        await pubsub.subscribe(topic, (event, ack) => {
          collected.push(event);
          ack?.();
        });

        await pubsub.publish(topic, makeEvent({ type: 'recovered' }));

        await waitForMessages(1, collected, 5000);

        expect(collected.length).toBe(1);
        expect(collected[0]!.type).toBe('recovered');
      } finally {
        await raw.close();
      }
    });

    it('preserves caller-assigned identity, timestamp, and cursor across the broker', async () => {
      const pubsub = createPubSub();
      const topic = uniqueTopic();
      const collected: Event[] = [];
      const createdAt = new Date('2026-07-15T10:00:00.000Z');

      await pubsub.subscribe(topic, (event, ack) => {
        collected.push(event);
        void ack?.();
      });
      await pubsub.publish(topic, {
        ...makeEvent({ type: 'identified' }),
        id: 'stable-event-id',
        createdAt,
        index: 12,
      });

      await waitForMessages(1, collected, 5000);
      expect(collected[0]).toMatchObject({ id: 'stable-event-id', createdAt, index: 12 });
    });

    it('rejects lifecycle publishes without complete replay identity', async () => {
      const pubsub = createPubSub();
      const topic = 'workflow.lifecycle.v1.research-workflow.run-1.execution-1';

      await expect(pubsub.publish(topic, makeEvent({ type: 'workflow-start', runId: 'run-1' }))).rejects.toThrow(
        'missing replay identity',
      );
    });

    it('isolates logical workflow run topics that share one normalized broker topic', async () => {
      const pubsub = createPubSub();
      const runA = `run-a-${Date.now()}-${topicCounter++}`;
      const runB = `run-b-${Date.now()}-${topicCounter++}`;
      const topicA = `workflow.events.v2.${runA}`;
      const topicB = `workflow.events.v2.${runB}`;
      const collectedA: Event[] = [];
      const collectedB: Event[] = [];

      await pubsub.subscribe(topicA, (event, ack) => {
        collectedA.push(event);
        void ack?.();
      });
      await pubsub.subscribe(topicB, (event, ack) => {
        collectedB.push(event);
        void ack?.();
      });

      await pubsub.publish(topicA, makeEvent({ type: 'run-a', runId: runA }));
      await pubsub.publish(topicB, makeEvent({ type: 'run-b', runId: runB }));

      await Promise.all([waitForMessages(1, collectedA, 5000), waitForMessages(1, collectedB, 5000)]);
      await new Promise(resolve => setTimeout(resolve, 300));
      expect(collectedA.map(event => event.type)).toEqual(['run-a']);
      expect(collectedB.map(event => event.type)).toEqual(['run-b']);
    });

    it('nacks an async callback rejection and redelivers the same logical event', async () => {
      const pubsub = createPubSub();
      const runId = `run-reject-${Date.now()}-${topicCounter++}`;
      const topic = `workflow.events.v2.${runId}`;
      const collected: Event[] = [];

      await pubsub.subscribe(topic, async (event, ack) => {
        collected.push(event);
        if (collected.length === 1) {
          throw new Error('retry this delivery');
        }
        await ack?.();
      });
      await pubsub.publish(topic, {
        ...makeEvent({ type: 'async-retry', runId }),
        id: 'stable-retry-id',
        createdAt: new Date('2026-07-15T10:00:00.000Z'),
        index: 18,
      });

      await waitForMessages(2, collected, 10_000);
      expect(collected).toHaveLength(2);
      expect(collected.map(event => event.id)).toEqual(['stable-retry-id', 'stable-retry-id']);
      expect(collected.map(event => event.index)).toEqual([18, 18]);
      expect(collected[1]!.deliveryAttempt).toBeGreaterThanOrEqual(2);
    });
  });

  describe('group (competing consumers)', () => {
    it('keeps hashed lifecycle subscription resources within Google naming limits', () => {
      const pubsub = createPubSub();
      const logicalTopic = `workflow.lifecycle.v1.${'workflow-'.repeat(20)}run.execution`;
      const subscriptionName = pubsub.getSubscriptionName(
        'workflow.lifecycle.v1',
        `workers/${'long-group-'.repeat(40)}`,
        logicalTopic,
      );

      expect(subscriptionName.length).toBeLessThanOrEqual(255);
      expect(subscriptionName).toMatch(/^[A-Za-z][A-Za-z0-9\-._~+%]*$/);
    });

    it('delivers each message exactly once via shared group subscription', async () => {
      // In a single process, two PubSub instances sharing a group subscription
      // means both register callbacks on the same underlying subscription.
      // Messages are delivered once per subscription (not duplicated), which is
      // the key difference from fan-out where each instance gets its own subscription.
      //
      // True multi-process competing consumer distribution requires separate processes,
      // but we can verify the core property: messages are NOT duplicated.
      const pubsub1 = createPubSub();
      const publisher = createPubSub();
      const topic = uniqueTopic();

      const collected: Event[] = [];

      await pubsub1.subscribe(
        topic,
        (event, ack) => {
          collected.push(event);
          ack?.();
        },
        { group: 'workers' },
      );

      // Give subscription time to establish
      await new Promise(resolve => setTimeout(resolve, 1000));

      // Publish multiple messages
      const messageCount = 4;
      for (let i = 0; i < messageCount; i++) {
        await publisher.publish(topic, makeEvent({ type: `task-${i}` }));
      }

      await waitForMessages(messageCount, collected, 10000);

      // Each message received exactly once (no duplicates)
      expect(collected.length).toBe(messageCount);

      // Verify different types received (not the same message repeated)
      const types = collected.map(e => e.type);
      expect(types).toContain('task-0');
      expect(types).toContain('task-3');
    });

    it('group and fan-out subscriptions on same topic use different subscription names', async () => {
      // Verify that a group subscription and a fan-out subscription on the same
      // topic create separate underlying subscriptions (different names), so
      // both independently receive all messages.
      const pubsub = createPubSub();
      const publisher = createPubSub();
      const topic = uniqueTopic();

      const groupCollected: Event[] = [];
      const fanoutCollected: Event[] = [];

      // Group subscription: uses name `${topic}-workers`
      await pubsub.subscribe(
        topic,
        (event, ack) => {
          groupCollected.push(event);
          ack?.();
        },
        { group: 'workers' },
      );

      // Fan-out subscription: uses name `${topic}-${instanceId}`
      await pubsub.subscribe(topic, (event, ack) => {
        fanoutCollected.push(event);
        ack?.();
      });

      await new Promise(resolve => setTimeout(resolve, 1000));

      const messageCount = 3;
      for (let i = 0; i < messageCount; i++) {
        await publisher.publish(topic, makeEvent({ type: `task-${i}` }));
      }

      // Both should receive all messages since they are separate subscriptions
      await Promise.all([
        waitForMessages(messageCount, groupCollected, 10000),
        waitForMessages(messageCount, fanoutCollected, 10000),
      ]);

      expect(fanoutCollected.length).toBe(messageCount);
      expect(groupCollected.length).toBe(messageCount);
    });

    it('exactly-once delivery is enabled for group subscriptions', async () => {
      const pubsub = createPubSub();
      const topic = uniqueTopic();

      const collected: Event[] = [];

      await pubsub.subscribe(
        topic,
        (event, ack) => {
          collected.push(event);
          ack?.();
        },
        { group: 'exactly-once-test' },
      );

      await new Promise(resolve => setTimeout(resolve, 1000));

      await pubsub.publish(topic, makeEvent({ type: 'unique-msg' }));

      await waitForMessages(1, collected, 5000);

      expect(collected.length).toBe(1);
      expect(collected[0]!.type).toBe('unique-msg');
    });

    it('keeps two logical runs isolated across instances in the same group', async () => {
      const runAConsumer = createPubSub();
      const runBConsumer = createPubSub();
      const publisher = createPubSub();
      const runA = `group-run-a-${Date.now()}-${topicCounter++}`;
      const runB = `group-run-b-${Date.now()}-${topicCounter++}`;
      const topicA = `workflow.lifecycle.v1.research-workflow.${runA}.execution-a`;
      const topicB = `workflow.lifecycle.v1.research-workflow.${runB}.execution-b`;
      const collectedA: Event[] = [];
      const collectedB: Event[] = [];
      const createdAt = new Date('2026-07-15T10:00:00.000Z');

      await runAConsumer.subscribe(
        topicA,
        (event, ack) => {
          collectedA.push(event);
          void ack?.();
        },
        { group: 'logical-run-workers' },
      );
      await runBConsumer.subscribe(
        topicB,
        (event, ack) => {
          collectedB.push(event);
          void ack?.();
        },
        { group: 'logical-run-workers' },
      );

      await publisher.publish(topicA, {
        ...makeEvent({ type: 'run-a-only', runId: runA }),
        id: 'lifecycle-a',
        createdAt,
        index: 4,
        logGeneration: 'log-a',
      });
      await publisher.publish(topicB, {
        ...makeEvent({ type: 'run-b-only', runId: runB }),
        id: 'lifecycle-b',
        createdAt,
        index: 9,
        logGeneration: 'log-b',
      });

      await Promise.all([waitForMessages(1, collectedA, 10_000), waitForMessages(1, collectedB, 10_000)]);
      await new Promise(resolve => setTimeout(resolve, 300));
      expect(collectedA.map(event => event.type)).toEqual(['run-a-only']);
      expect(collectedB.map(event => event.type)).toEqual(['run-b-only']);
      expect(collectedA[0]).toMatchObject({ id: 'lifecycle-a', createdAt, index: 4, logGeneration: 'log-a' });
      expect(collectedB[0]).toMatchObject({ id: 'lifecycle-b', createdAt, index: 9, logGeneration: 'log-b' });
      expect(runAConsumer.getSubscriptionName('workflow.lifecycle.v1', 'logical-run-workers', topicA)).not.toBe(
        runBConsumer.getSubscriptionName('workflow.lifecycle.v1', 'logical-run-workers', topicB),
      );
    });
  });

  describe('localOnly publish', () => {
    it('delivers to subscribers in the same process without round-tripping through Pub/Sub', async () => {
      const pubsub = createPubSub();
      const topic = uniqueTopic();
      const collected: Event[] = [];

      await pubsub.subscribe(topic, (event, ack) => {
        collected.push(event);
        ack?.();
      });

      // Payload carries a live class instance — if this hit Pub/Sub it would be
      // JSON-serialized and lose its prototype on the way back.
      class Holder {
        constructor(public readonly value: string) {}
        describe() {
          return `holder(${this.value})`;
        }
      }
      const holder = new Holder('alpha');
      await pubsub.publish(topic, makeEvent({ type: 'local', data: { holder } }), { localOnly: true });

      await waitForMessages(1, collected, 1000);
      expect(collected.length).toBe(1);
      const delivered = collected[0]!.data.holder as Holder;
      expect(delivered).toBe(holder);
      expect(delivered.describe()).toBe('holder(alpha)');
    });

    it('preserves caller-assigned identity for local-only delivery', async () => {
      const pubsub = createPubSub();
      const topic = uniqueTopic();
      const collected: Event[] = [];
      const createdAt = new Date('2026-07-15T10:00:00.000Z');

      await pubsub.subscribe(topic, event => {
        collected.push(event);
      });
      await pubsub.publish(
        topic,
        {
          ...makeEvent({ type: 'local-identified' }),
          id: 'stable-local-id',
          createdAt,
          index: 4,
        },
        { localOnly: true },
      );

      await waitForMessages(1, collected, 1000);
      expect(collected[0]).toMatchObject({ id: 'stable-local-id', createdAt, index: 4 });
    });

    it('does not leak local-only events to other instances sharing the same topic', async () => {
      const publisher = createPubSub();
      const remote = createPubSub();
      const topic = uniqueTopic();
      const remoteCollected: Event[] = [];

      // Remote instance subscribes via real Pub/Sub.
      await remote.subscribe(topic, (event, ack) => {
        remoteCollected.push(event);
        ack?.();
      });

      // Give the remote subscription a moment to attach to the emulator.
      await new Promise(resolve => setTimeout(resolve, 500));

      await publisher.publish(topic, makeEvent({ type: 'private' }), { localOnly: true });

      // The remote subscriber should never see this — confirm by waiting.
      await new Promise(resolve => setTimeout(resolve, 1500));
      expect(remoteCollected.length).toBe(0);
    });

    it('unsubscribe stops local delivery too', async () => {
      const pubsub = createPubSub();
      const topic = uniqueTopic();
      const collected: Event[] = [];

      const cb = (event: Event, ack?: () => Promise<void>) => {
        collected.push(event);
        ack?.();
      };

      await pubsub.subscribe(topic, cb);
      await pubsub.unsubscribe(topic, cb);

      await pubsub.publish(topic, makeEvent(), { localOnly: true });

      await new Promise(resolve => setTimeout(resolve, 300));
      expect(collected.length).toBe(0);
    });
  });
});
