import { describe, it, expect, beforeEach, vi } from 'vitest';
import { InMemoryServerCache } from '../cache/inmemory';
import { CachingPubSub, withCaching } from './caching-pubsub';
import { EventEmitterPubSub } from './event-emitter';
import type { IndexedReplayCursorError, IndexedReplayIntegrityError } from './pubsub';
import { isLeaseProvider, PubSub } from './pubsub';
import type { Event, EventCallback, PublishEvent } from './types';

describe('CachingPubSub', () => {
  let cache: InMemoryServerCache;
  let innerPubsub: EventEmitterPubSub;
  let cachingPubsub: CachingPubSub;

  beforeEach(() => {
    cache = new InMemoryServerCache();
    innerPubsub = new EventEmitterPubSub();
    cachingPubsub = new CachingPubSub(innerPubsub, cache);
  });

  describe('publish', () => {
    it('should cache events when publishing', async () => {
      const topic = 'test-topic';
      const event = { type: 'test', runId: 'run-1', data: { foo: 'bar' } };

      await cachingPubsub.publish(topic, event);

      // Wait a tick for async cache write
      await new Promise(resolve => setTimeout(resolve, 10));

      const history = await cachingPubsub.getHistory(topic);
      expect(history).toHaveLength(1);
      expect(history[0]).toMatchObject({
        type: 'test',
        runId: 'run-1',
        data: { foo: 'bar' },
      });
      expect(history[0].id).toBeDefined();
      expect(history[0].createdAt).toBeInstanceOf(Date);
    });

    it('should publish to inner pubsub', async () => {
      const topic = 'test-topic';
      const event = { type: 'test', runId: 'run-1', data: {} };
      const callback = vi.fn();

      await innerPubsub.subscribe(topic, callback);
      await cachingPubsub.publish(topic, event);

      expect(callback).toHaveBeenCalledTimes(1);
      expect(callback).toHaveBeenCalledWith(
        expect.objectContaining({
          type: 'test',
          runId: 'run-1',
        }),
      );
    });

    it('does not persist localOnly events in the shared cache', async () => {
      const callback = vi.fn();
      await innerPubsub.subscribe('internal-topic', callback);

      await cachingPubsub.publish(
        'internal-topic',
        { type: 'internal', runId: 'run-local', data: { liveCallback: () => 'local' } },
        { localOnly: true },
      );

      expect(callback).toHaveBeenCalledTimes(1);
      expect(await cachingPubsub.getHistory('internal-topic')).toEqual([]);
    });

    it('should cache multiple events in order', async () => {
      const topic = 'test-topic';

      await cachingPubsub.publish(topic, { type: 'first', runId: 'run-1', data: {} });
      await cachingPubsub.publish(topic, { type: 'second', runId: 'run-1', data: {} });
      await cachingPubsub.publish(topic, { type: 'third', runId: 'run-1', data: {} });

      // Wait for async cache writes
      await new Promise(resolve => setTimeout(resolve, 10));

      const history = await cachingPubsub.getHistory(topic);
      expect(history).toHaveLength(3);
      expect(history[0].type).toBe('first');
      expect(history[1].type).toBe('second');
      expect(history[2].type).toBe('third');
    });

    it('should assign sequential indices to events', async () => {
      const topic = 'index-topic';

      await cachingPubsub.publish(topic, { type: 'first', runId: 'run-1', data: {} });
      await cachingPubsub.publish(topic, { type: 'second', runId: 'run-1', data: {} });
      await cachingPubsub.publish(topic, { type: 'third', runId: 'run-1', data: {} });

      // Wait for async cache writes
      await new Promise(resolve => setTimeout(resolve, 10));

      const history = await cachingPubsub.getHistory(topic);
      expect(history).toHaveLength(3);
      expect(history[0].index).toBe(0);
      expect(history[1].index).toBe(1);
      expect(history[2].index).toBe(2);
    });

    it('should include index in live events', async () => {
      const topic = 'live-index-topic';
      const receivedEvents: Event[] = [];

      await cachingPubsub.subscribe(topic, event => {
        receivedEvents.push(event);
      });

      await cachingPubsub.publish(topic, { type: 'first', runId: 'run-1', data: {} });
      await cachingPubsub.publish(topic, { type: 'second', runId: 'run-1', data: {} });

      expect(receivedEvents).toHaveLength(2);
      expect(receivedEvents[0].index).toBe(0);
      expect(receivedEvents[1].index).toBe(1);
    });

    it('preserves the same event identity in live delivery and retained history', async () => {
      const topic = 'stable-identity-topic';
      const liveEvents: Event[] = [];

      await cachingPubsub.subscribe(topic, event => {
        liveEvents.push(event);
      });
      await cachingPubsub.publish(topic, { type: 'first', runId: 'run-1', data: {} });

      const history = await cachingPubsub.getHistory(topic);
      expect(liveEvents).toHaveLength(1);
      expect(history).toHaveLength(1);
      expect(liveEvents[0]!.id).toBe(history[0]!.id);
      expect(liveEvents[0]!.createdAt).toEqual(history[0]!.createdAt);
    });

    it('preserves upstream identity while assigning its own topic cursor', async () => {
      const topic = 'upstream-identity-topic';
      const createdAt = new Date('2026-07-15T10:00:00.000Z');

      await cachingPubsub.publish(topic, {
        type: 'first',
        runId: 'run-1',
        data: {},
        id: 'upstream-event-id',
        createdAt,
        index: 999,
      });

      const history = await cachingPubsub.getHistory(topic);
      expect(history[0]).toMatchObject({ id: 'upstream-event-id', createdAt, index: 0 });
    });

    it('restores Date identity from JSON-backed retained history', async () => {
      const createdAt = '2026-07-15T10:00:00.000Z';
      await cache.listPush('pubsub:serialized-history-topic', {
        type: 'first',
        runId: 'run-1',
        data: {},
        id: 'serialized-event-id',
        createdAt,
        index: 0,
      });

      const history = await cachingPubsub.getHistory('serialized-history-topic');
      expect(history[0]).toMatchObject({ id: 'serialized-event-id', index: 0 });
      expect(history[0]!.createdAt).toEqual(new Date(createdAt));
    });

    it('rejects malformed retained event shape and createdAt values', async () => {
      await cache.listPush('pubsub:malformed-shape-topic', { createdAt: new Date().toISOString() });
      await cache.listPush('pubsub:malformed-date-topic', {
        type: 'invalid-date',
        id: 'invalid-date-id',
        runId: 'run-1',
        data: {},
        createdAt: 'not-a-date',
      });

      await expect(cachingPubsub.getHistory('malformed-shape-topic')).rejects.toMatchObject<
        Partial<IndexedReplayIntegrityError>
      >({ reason: 'malformed-retained-event' });
      await expect(cachingPubsub.getHistory('malformed-date-topic')).rejects.toMatchObject<
        Partial<IndexedReplayIntegrityError>
      >({ reason: 'malformed-retained-event' });
    });

    it('should recover index from cache after restart', async () => {
      const topic = 'recovery-topic';

      // Simulate first session - publish some events
      await cachingPubsub.publish(topic, { type: 'first', runId: 'run-1', data: {} });
      await cachingPubsub.publish(topic, { type: 'second', runId: 'run-1', data: {} });
      await new Promise(resolve => setTimeout(resolve, 10));

      // Simulate restart - create new CachingPubSub with same cache
      const newPubsub = new CachingPubSub(new EventEmitterPubSub(), cache);

      // Publish more events - should continue from index 2
      await newPubsub.publish(topic, { type: 'third', runId: 'run-1', data: {} });
      await new Promise(resolve => setTimeout(resolve, 10));

      const history = await newPubsub.getHistory(topic);
      expect(history).toHaveLength(3);
      expect(history[0].index).toBe(0);
      expect(history[1].index).toBe(1);
      expect(history[2].index).toBe(2);
    });

    it('should reset index when topic is cleared', async () => {
      const topic = 'clear-topic';

      await cachingPubsub.publish(topic, { type: 'first', runId: 'run-1', data: {} });
      await cachingPubsub.publish(topic, { type: 'second', runId: 'run-1', data: {} });
      await new Promise(resolve => setTimeout(resolve, 10));

      await cachingPubsub.clearTopic(topic);

      // Publish after clear - should start from index 0
      await cachingPubsub.publish(topic, { type: 'new-first', runId: 'run-2', data: {} });
      await new Promise(resolve => setTimeout(resolve, 10));

      const history = await cachingPubsub.getHistory(topic);
      expect(history).toHaveLength(1);
      expect(history[0].index).toBe(0);
      expect(history[0].type).toBe('new-first');
    });

    describe('localOnly', () => {
      // `localOnly` events are never relayed to other instances, so nothing can
      // replay them from the shared cache. Caching them grows the store without
      // bound — `workflow.events.v2.*` watch events carry cumulative step results
      // and run to megabytes each.
      it('should not cache localOnly events', async () => {
        const topic = 'workflow.events.v2.run-1';

        await cachingPubsub.publish(
          topic,
          { type: 'watch', runId: 'run-1', data: { big: 'payload' } },
          {
            localOnly: true,
          },
        );
        await new Promise(resolve => setTimeout(resolve, 10));

        expect(await cachingPubsub.getHistory(topic)).toHaveLength(0);
      });

      it('should not allocate an index counter for localOnly events', async () => {
        const topic = 'workflow.events.v2.run-2';

        await cachingPubsub.publish(topic, { type: 'watch', runId: 'run-2', data: {} }, { localOnly: true });
        await new Promise(resolve => setTimeout(resolve, 10));

        // A counter key would survive the run and leak forever, since nothing
        // clears a topic that was never cached.
        expect(await cache.get(`pubsub:${topic}:counter`)).toBeUndefined();
      });

      it('should still deliver localOnly events live to subscribers', async () => {
        const topic = 'local-live-topic';
        const receivedEvents: Event[] = [];

        await cachingPubsub.subscribe(topic, event => {
          receivedEvents.push(event);
        });

        await cachingPubsub.publish(
          topic,
          { type: 'watch', runId: 'run-1', data: { foo: 'bar' } },
          {
            localOnly: true,
          },
        );

        expect(receivedEvents).toHaveLength(1);
        expect(receivedEvents[0]).toMatchObject({ type: 'watch', runId: 'run-1', data: { foo: 'bar' } });
        expect(receivedEvents[0].id).toBeDefined();
        expect(receivedEvents[0].createdAt).toBeInstanceOf(Date);
        // No index: the event was never assigned one from the counter.
        expect(receivedEvents[0].index).toBeUndefined();
      });

      it('should forward the localOnly option to the inner pubsub', async () => {
        const topic = 'local-forward-topic';
        const publishSpy = vi.spyOn(innerPubsub, 'publish');

        await cachingPubsub.publish(topic, { type: 'watch', runId: 'run-1', data: {} }, { localOnly: true });

        expect(publishSpy).toHaveBeenCalledWith(topic, expect.objectContaining({ type: 'watch' }), {
          localOnly: true,
        });
      });

      it('should keep cached indices gap-free across interleaved localOnly publishes', async () => {
        const topic = 'mixed-topic';

        await cachingPubsub.publish(topic, { type: 'cached-first', runId: 'run-1', data: {} });
        await cachingPubsub.publish(topic, { type: 'local', runId: 'run-1', data: {} }, { localOnly: true });
        await cachingPubsub.publish(topic, { type: 'cached-second', runId: 'run-1', data: {} });
        await new Promise(resolve => setTimeout(resolve, 10));

        const history = await cachingPubsub.getHistory(topic);
        expect(history.map(event => [event.type, event.index])).toEqual([
          ['cached-first', 0],
          ['cached-second', 1],
        ]);
      });

      it('should deliver live localOnly events to a replay subscriber without caching them', async () => {
        const topic = 'replay-mixed-topic';
        const receivedEvents: Event[] = [];

        await cachingPubsub.publish(topic, { type: 'cached-first', runId: 'run-1', data: {} });
        await new Promise(resolve => setTimeout(resolve, 10));

        await cachingPubsub.subscribeWithReplay(topic, event => {
          receivedEvents.push(event);
        });

        await cachingPubsub.publish(topic, { type: 'local', runId: 'run-1', data: {} }, { localOnly: true });
        await cachingPubsub.publish(topic, { type: 'cached-second', runId: 'run-1', data: {} });
        await new Promise(resolve => setTimeout(resolve, 10));

        expect(receivedEvents.map(event => event.type)).toEqual(['cached-first', 'local', 'cached-second']);
        expect(await cachingPubsub.getHistory(topic)).toHaveLength(2);
      });
    });
  });

  describe('subscribe', () => {
    it('should subscribe to live events without replay', async () => {
      const topic = 'test-topic';
      const callback = vi.fn();

      // Publish some events first
      await cachingPubsub.publish(topic, { type: 'cached', runId: 'run-1', data: {} });
      await new Promise(resolve => setTimeout(resolve, 10));

      // Subscribe with regular subscribe (no replay)
      await cachingPubsub.subscribe(topic, callback);

      // Publish a new event
      await cachingPubsub.publish(topic, { type: 'live', runId: 'run-1', data: {} });

      // Should only receive the live event, not the cached one
      expect(callback).toHaveBeenCalledTimes(1);
      expect(callback).toHaveBeenCalledWith(expect.objectContaining({ type: 'live' }));
    });

    it('forwards options (including batch) verbatim to the inner PubSub', async () => {
      const subscribeSpy = vi.fn(async () => {});
      class StubInner extends PubSub {
        get supportsNativeBatching() {
          return true;
        }
        async publish() {}
        subscribe = subscribeSpy;
        async unsubscribe() {}
        async flush() {}
      }
      const wrapped = new CachingPubSub(new StubInner(), cache);
      const cb = () => {};
      const options = { batch: { maxSize: 2, maxWaitMs: 50 } };
      await wrapped.subscribe('t', cb, options);
      expect(subscribeSpy).toHaveBeenCalledWith('t', cb, options);
    });

    it('reports supportsNativeBatching by delegating to the inner PubSub', () => {
      class NativeInner extends PubSub {
        get supportsNativeBatching() {
          return true;
        }
        async publish() {}
        async subscribe() {}
        async unsubscribe() {}
        async flush() {}
      }
      class NonNativeInner extends PubSub {
        async publish() {}
        async subscribe() {}
        async unsubscribe() {}
        async flush() {}
      }
      expect(new CachingPubSub(new NativeInner(), cache).supportsNativeBatching).toBe(true);
      expect(new CachingPubSub(new NonNativeInner(), cache).supportsNativeBatching).toBe(false);
    });
  });

  describe('subscribeWithReplay', () => {
    it('should replay cached events then receive live events', async () => {
      const topic = 'test-topic';
      const receivedEvents: Event[] = [];
      const callback = vi.fn((event: Event) => {
        receivedEvents.push(event);
      });

      // Publish some events first
      await cachingPubsub.publish(topic, { type: 'cached-1', runId: 'run-1', data: {} });
      await cachingPubsub.publish(topic, { type: 'cached-2', runId: 'run-1', data: {} });
      await new Promise(resolve => setTimeout(resolve, 10));

      // Subscribe with replay
      await cachingPubsub.subscribeWithReplay(topic, callback);

      // Should have received cached events
      expect(receivedEvents).toHaveLength(2);
      expect(receivedEvents[0].type).toBe('cached-1');
      expect(receivedEvents[1].type).toBe('cached-2');

      // Publish a live event
      await cachingPubsub.publish(topic, { type: 'live', runId: 'run-1', data: {} });

      // Should also receive the live event
      expect(receivedEvents).toHaveLength(3);
      expect(receivedEvents[2].type).toBe('live');
    });

    it('should deduplicate events at the replay/live boundary', async () => {
      const topic = 'test-topic';
      const receivedEvents: Event[] = [];
      const callback = vi.fn((event: Event) => {
        receivedEvents.push(event);
      });

      const racyInnerPubsub = new EventEmitterPubSub();
      const racyCachingPubsub = new CachingPubSub(racyInnerPubsub, cache);

      await racyCachingPubsub.publish(topic, { type: 'boundary-event', runId: 'run-1', data: {} });
      await new Promise(resolve => setTimeout(resolve, 10));

      await racyCachingPubsub.subscribeWithReplay(topic, callback);

      const boundaryEvents = receivedEvents.filter(e => e.type === 'boundary-event');
      expect(boundaryEvents).toHaveLength(1);
    });

    it('should not duplicate an event published while the subscription is being established', async () => {
      // Regression for #18148: a subscriber attaches with replay while the
      // producer is still publishing. The event published in the window between
      // inner.subscribe() and getHistory() is delivered BOTH live (via the inner
      // pubsub) AND via cache replay. Dedup must collapse it to a single
      // delivery even though the inner pubsub regenerates event.id.
      const topic = 'replay-race-topic';
      const received: string[] = [];

      // Pre-fill history before any subscriber exists.
      await cachingPubsub.publish(topic, { type: 'chunk', runId: 'r1', data: { c: '0' } });
      await cachingPubsub.publish(topic, { type: 'chunk', runId: 'r1', data: { c: '1' } });

      // Force the race: publish "2" exactly between inner.subscribe() and
      // getHistory(), so it lands in both the live stream and the replayed history.
      const realGetHistory = cachingPubsub.getHistory.bind(cachingPubsub);
      let raced = false;
      vi.spyOn(cachingPubsub, 'getHistory').mockImplementation(async (t: string, offset?: number) => {
        if (!raced) {
          raced = true;
          await cachingPubsub.publish(topic, { type: 'chunk', runId: 'r1', data: { c: '2' } });
        }
        return realGetHistory(t, offset);
      });

      await cachingPubsub.subscribeWithReplay(topic, (event: Event) => {
        received.push((event.data as { c: string }).c);
      });

      const counts = received.reduce<Record<string, number>>((acc, c) => {
        acc[c] = (acc[c] ?? 0) + 1;
        return acc;
      }, {});
      expect(counts).toEqual({ '0': 1, '1': 1, '2': 1 });
    });

    it('should handle empty cache gracefully', async () => {
      const topic = 'empty-topic';
      const callback = vi.fn();

      await cachingPubsub.subscribeWithReplay(topic, callback);

      // No cached events, so callback shouldn't be called yet
      expect(callback).not.toHaveBeenCalled();

      // Publish a live event
      await cachingPubsub.publish(topic, { type: 'first-event', runId: 'run-1', data: {} });

      expect(callback).toHaveBeenCalledTimes(1);
    });
  });

  describe('subscribeFromOffset', () => {
    it('should not duplicate an event published while the subscription is being established', async () => {
      // Regression for #18148, offset variant: same replay/live race as
      // subscribeWithReplay, but attaching from a specific index.
      const topic = 'offset-race-topic';
      const received: string[] = [];

      await cachingPubsub.publish(topic, { type: 'chunk', runId: 'r1', data: { c: '0' } });
      await cachingPubsub.publish(topic, { type: 'chunk', runId: 'r1', data: { c: '1' } });

      const realGetHistory = cachingPubsub.getHistory.bind(cachingPubsub);
      let raced = false;
      vi.spyOn(cachingPubsub, 'getHistory').mockImplementation(async (t: string, offset?: number) => {
        if (!raced) {
          raced = true;
          await cachingPubsub.publish(topic, { type: 'chunk', runId: 'r1', data: { c: '2' } });
        }
        return realGetHistory(t, offset);
      });

      await cachingPubsub.subscribeFromOffset(topic, 0, (event: Event) => {
        received.push((event.data as { c: string }).c);
      });

      const counts = received.reduce<Record<string, number>>((acc, c) => {
        acc[c] = (acc[c] ?? 0) + 1;
        return acc;
      }, {});
      expect(counts).toEqual({ '0': 1, '1': 1, '2': 1 });
    });

    it('skips events before the requested offset', async () => {
      const topic = 'offset-skip-topic';

      await cachingPubsub.publish(topic, { type: 'e0', runId: 'r1', data: {} });
      await cachingPubsub.publish(topic, { type: 'e1', runId: 'r1', data: {} });
      await cachingPubsub.publish(topic, { type: 'e2', runId: 'r1', data: {} });
      await cachingPubsub.publish(topic, { type: 'e3', runId: 'r1', data: {} });

      const received: number[] = [];
      await cachingPubsub.subscribeFromOffset(topic, 2, (event: Event) => {
        received.push(event.index!);
      });

      expect(received).toEqual([2, 3]);
    });
  });

  describe('getHistory', () => {
    it('should return cached events for a topic', async () => {
      const topic = 'history-topic';

      await cachingPubsub.publish(topic, { type: 'event-1', runId: 'run-1', data: { a: 1 } });
      await cachingPubsub.publish(topic, { type: 'event-2', runId: 'run-1', data: { b: 2 } });
      await new Promise(resolve => setTimeout(resolve, 10));

      const history = await cachingPubsub.getHistory(topic);

      expect(history).toHaveLength(2);
      expect(history[0].data).toEqual({ a: 1 });
      expect(history[1].data).toEqual({ b: 2 });
    });

    it('should return events from specified index', async () => {
      const topic = 'history-topic';

      await cachingPubsub.publish(topic, { type: 'event-0', runId: 'run-1', data: {} });
      await cachingPubsub.publish(topic, { type: 'event-1', runId: 'run-1', data: {} });
      await cachingPubsub.publish(topic, { type: 'event-2', runId: 'run-1', data: {} });
      await new Promise(resolve => setTimeout(resolve, 10));

      const history = await cachingPubsub.getHistory(topic, 1);

      expect(history).toHaveLength(2);
      expect(history[0].type).toBe('event-1');
      expect(history[1].type).toBe('event-2');
    });

    it('should return empty array for non-existent topic', async () => {
      const history = await cachingPubsub.getHistory('non-existent-topic');
      expect(history).toEqual([]);
    });
  });

  describe('unsubscribe', () => {
    it('should unsubscribe from topic', async () => {
      const topic = 'unsub-topic';
      const callback = vi.fn();

      await cachingPubsub.subscribe(topic, callback);
      await cachingPubsub.publish(topic, { type: 'before-unsub', runId: 'run-1', data: {} });

      expect(callback).toHaveBeenCalledTimes(1);

      await cachingPubsub.unsubscribe(topic, callback);
      await cachingPubsub.publish(topic, { type: 'after-unsub', runId: 'run-1', data: {} });

      // Should still only have been called once (before unsubscribe)
      expect(callback).toHaveBeenCalledTimes(1);
    });
  });

  describe('clearTopic', () => {
    it('should clear cached events for a topic', async () => {
      const topic = 'clear-topic';

      await cachingPubsub.publish(topic, { type: 'event-1', runId: 'run-1', data: {} });
      await cachingPubsub.publish(topic, { type: 'event-2', runId: 'run-1', data: {} });
      await new Promise(resolve => setTimeout(resolve, 10));

      let history = await cachingPubsub.getHistory(topic);
      expect(history).toHaveLength(2);

      await cachingPubsub.clearTopic(topic);

      history = await cachingPubsub.getHistory(topic);
      expect(history).toHaveLength(0);
    });

    it('should not affect other topics', async () => {
      const topic1 = 'topic-1';
      const topic2 = 'topic-2';

      await cachingPubsub.publish(topic1, { type: 'event-1', runId: 'run-1', data: {} });
      await cachingPubsub.publish(topic2, { type: 'event-2', runId: 'run-1', data: {} });
      await new Promise(resolve => setTimeout(resolve, 10));

      await cachingPubsub.clearTopic(topic1);

      const history1 = await cachingPubsub.getHistory(topic1);
      const history2 = await cachingPubsub.getHistory(topic2);

      expect(history1).toHaveLength(0);
      expect(history2).toHaveLength(1);
    });

    it('forwards clearTopic to an inner transport that implements it', async () => {
      // A persistent inner (e.g. Redis Streams) must be told to delete its
      // underlying stream — otherwise wrapping it in CachingPubSub turns
      // clearTopic into a cache-only no-op and the inner storage leaks.
      class ClearableInner extends PubSub {
        clearTopic = vi.fn(async (_topic: string) => {});
        async publish(): Promise<void> {}
        async subscribe(): Promise<void> {}
        async unsubscribe(): Promise<void> {}
        async flush(): Promise<void> {}
      }
      const inner = new ClearableInner();
      const wrapped = new CachingPubSub(inner, cache);

      await wrapped.clearTopic('some-topic');

      expect(inner.clearTopic).toHaveBeenCalledWith('some-topic');
    });

    it('does not throw when the inner transport does not override clearTopic', async () => {
      // EventEmitterPubSub retains nothing per topic and relies on the
      // PubSub base class's no-op clearTopic; forwarding must resolve cleanly.
      await expect(cachingPubsub.clearTopic('no-inner-hook')).resolves.toBeUndefined();
    });

    it('does not reject when the cache or inner transport fails', async () => {
      // The base-class contract says clearTopic is best-effort and
      // non-throwing: callers (DurableAgent, WorkflowEventProcessor) invoke
      // it fire-and-forget, so a rejection here would surface as an
      // unhandledRejection. Failures must be logged, not thrown.
      class FailingInner extends PubSub {
        override clearTopic = vi.fn(async (_topic: string) => {
          throw new Error('inner delete failed');
        });
        async publish(): Promise<void> {}
        async subscribe(): Promise<void> {}
        async unsubscribe(): Promise<void> {}
        async flush(): Promise<void> {}
      }
      const logger = { error: vi.fn() };
      const wrapped = new CachingPubSub(new FailingInner(), cache, { logger: logger as any });

      await expect(wrapped.clearTopic('failing-topic')).resolves.toBeUndefined();
      expect(logger.error).toHaveBeenCalledWith(expect.stringContaining('failing-topic'), expect.any(Error));
    });

    it('exposes cleanup failures to retryable durable operations', async () => {
      class FailingInner extends PubSub {
        override clearTopic = vi.fn(async (_topic: string) => {
          throw new Error('inner delete failed');
        });
        async publish(): Promise<void> {}
        async subscribe(): Promise<void> {}
        async unsubscribe(): Promise<void> {}
        async flush(): Promise<void> {}
      }
      const wrapped = new CachingPubSub(new FailingInner(), cache);

      await expect(wrapped.clearTopicOrThrow('retryable-cleanup-topic')).rejects.toThrow('inner delete failed');
    });
  });

  describe('flush', () => {
    it('flushes the inner transport twice to confirm a stable drain boundary', async () => {
      const flushSpy = vi.spyOn(innerPubsub, 'flush');

      await cachingPubsub.flush();

      expect(flushSpy).toHaveBeenCalledTimes(2);
    });
  });

  describe('getInner', () => {
    it('should return the inner pubsub instance', () => {
      expect(cachingPubsub.getInner()).toBe(innerPubsub);
    });
  });

  describe('lease provider', () => {
    it('exposes the inner pubsub as the lease provider when the inner can lease', () => {
      const leaseProvider = cachingPubsub.getLeaseProvider();
      expect(leaseProvider).toBe(innerPubsub);
      expect(isLeaseProvider(leaseProvider)).toBe(true);
    });

    it('returns undefined when the inner pubsub cannot lease', () => {
      class NonLeaseInner extends PubSub {
        async publish() {}
        async subscribe() {}
        async unsubscribe() {}
        async flush() {}
      }
      const wrapped = new CachingPubSub(new NonLeaseInner(), cache);
      expect(wrapped.getLeaseProvider()).toBeUndefined();
    });

    it('preserves real lease semantics through the inner lease provider', async () => {
      // Caching is transparent to leasing: callers resolve the inner's
      // provider and coordinate through it, so wrapping with caching must
      // not fake or weaken the lock. This guards against a regression where
      // a second owner could "acquire" an already-held lease.
      const leaseProvider = cachingPubsub.getLeaseProvider();
      expect(leaseProvider).toBeDefined();

      const first = await leaseProvider!.acquireLease('thread-1', 'owner-a', 5000);
      expect(first).toEqual({ acquired: true, owner: 'owner-a' });

      const second = await leaseProvider!.acquireLease('thread-1', 'owner-b', 5000);
      expect(second.acquired).toBe(false);
      expect(second.owner).toBe('owner-a');

      expect(await leaseProvider!.getLeaseOwner('thread-1')).toBe('owner-a');

      await leaseProvider!.releaseLease('thread-1', 'owner-a');
      expect(await leaseProvider!.getLeaseOwner('thread-1')).toBeUndefined();
    });
  });

  describe('withCaching factory', () => {
    it('should create a CachingPubSub instance', () => {
      const result = withCaching(innerPubsub, cache);
      expect(result).toBeInstanceOf(CachingPubSub);
    });

    it('should work with custom options', async () => {
      const customPubsub = withCaching(innerPubsub, cache, { keyPrefix: 'custom:' });

      await customPubsub.publish('test', { type: 'test', runId: 'run-1', data: {} });
      await new Promise(resolve => setTimeout(resolve, 10));

      // Events should be cached under custom prefix
      const rawCacheValue = await cache.get('custom:test');
      expect(Array.isArray(rawCacheValue)).toBe(true);
    });
  });

  describe('key prefix', () => {
    it('should use custom key prefix for cache', async () => {
      const prefixedPubsub = new CachingPubSub(innerPubsub, cache, { keyPrefix: 'myapp:' });
      const topic = 'events';

      await prefixedPubsub.publish(topic, { type: 'test', runId: 'run-1', data: {} });
      await new Promise(resolve => setTimeout(resolve, 10));

      // Check cache directly
      const rawCacheValue = await cache.get('myapp:events');
      expect(Array.isArray(rawCacheValue)).toBe(true);
      expect(rawCacheValue).toHaveLength(1);
    });

    it('should use default prefix when not specified', async () => {
      const topic = 'events';

      await cachingPubsub.publish(topic, { type: 'test', runId: 'run-1', data: {} });
      await new Promise(resolve => setTimeout(resolve, 10));

      // Check cache directly with default prefix
      const rawCacheValue = await cache.get('pubsub:events');
      expect(Array.isArray(rawCacheValue)).toBe(true);
    });
  });

  describe('topic isolation', () => {
    it('should keep events separate per topic', async () => {
      const topic1 = 'agent.stream.run-1';
      const topic2 = 'agent.stream.run-2';

      await cachingPubsub.publish(topic1, { type: 'run1-event', runId: 'run-1', data: {} });
      await cachingPubsub.publish(topic2, { type: 'run2-event', runId: 'run-2', data: {} });
      await cachingPubsub.publish(topic1, { type: 'run1-event-2', runId: 'run-1', data: {} });
      await new Promise(resolve => setTimeout(resolve, 10));

      const history1 = await cachingPubsub.getHistory(topic1);
      const history2 = await cachingPubsub.getHistory(topic2);

      expect(history1).toHaveLength(2);
      expect(history1[0].type).toBe('run1-event');
      expect(history1[1].type).toBe('run1-event-2');

      expect(history2).toHaveLength(1);
      expect(history2[0].type).toBe('run2-event');
    });
  });

  describe('publish resilience', () => {
    it('should still deliver to live subscribers when cache.listPush fails', async () => {
      const topic = 'cache-fail-topic';
      const callback = vi.fn();

      // Create a cache that throws on listPush
      const failingCache = new InMemoryServerCache();
      failingCache.listPush = async (_key: string, _value: unknown) => {
        throw new Error('Cache write failed');
      };

      const failingCachingPubsub = new CachingPubSub(innerPubsub, failingCache);

      await failingCachingPubsub.subscribe(topic, callback);
      await failingCachingPubsub.publish(topic, { type: 'test', runId: 'run-1', data: { hello: 'world' } });

      // Live subscriber should still receive the event even though cache failed
      expect(callback).toHaveBeenCalledTimes(1);
      expect(callback).toHaveBeenCalledWith(expect.objectContaining({ type: 'test', data: { hello: 'world' } }));
    });

    it('should still deliver to live subscribers when cache.increment fails', async () => {
      const topic = 'increment-fail-topic';
      const callback = vi.fn();

      const failingCache = new InMemoryServerCache();
      failingCache.increment = async (_key: string) => {
        throw new Error('Increment failed');
      };
      const listPushSpy = vi.spyOn(failingCache, 'listPush');

      const failingCachingPubsub = new CachingPubSub(innerPubsub, failingCache);

      await failingCachingPubsub.subscribe(topic, callback);
      await failingCachingPubsub.publish(topic, { type: 'test', runId: 'run-1', data: {} });

      // Live subscriber should still receive the event
      expect(callback).toHaveBeenCalledTimes(1);

      // listPush should NOT be called when increment failed (avoids duplicate index-0 entries)
      expect(listPushSpy).not.toHaveBeenCalled();
    });
  });

  describe('steady-state dedup after replay', () => {
    it('uses bounded watermark instead of unbounded seen set after replay', async () => {
      const topic = 'seen-set-topic';

      // Publish a cached event before subscribing
      await cachingPubsub.publish(topic, { type: 'cached', runId: 'run-1', data: {} });
      await new Promise(resolve => setTimeout(resolve, 10));

      const callback = vi.fn();
      await cachingPubsub.subscribeWithReplay(topic, callback);

      expect(callback).toHaveBeenCalledTimes(1);

      // Send 50 live events
      for (let i = 0; i < 50; i++) {
        await cachingPubsub.publish(topic, { type: `live-${i}`, runId: 'run-1', data: {} });
      }
      expect(callback).toHaveBeenCalledTimes(51); // 1 cached + 50 live

      // Get the wrappedCb from the callbackMap
      const callbackMap = (cachingPubsub as any).callbackMap as Map<any, any>;
      const wrappedCb = callbackMap.get(callback);
      expect(wrappedCb).toBeDefined();

      // After replay, the wrapper uses a lastDelivered watermark. A genuinely
      // new event (index higher than anything seen) should still be delivered.
      callback.mockClear();
      const newEvent = {
        id: 'brand-new',
        type: 'test',
        runId: 'run-1',
        data: {},
        createdAt: new Date(),
        index: 999,
      };
      wrappedCb(newEvent);
      expect(callback).toHaveBeenCalledTimes(1);

      // The same index again should be suppressed (watermark dedup)
      wrappedCb(newEvent);
      expect(callback).toHaveBeenCalledTimes(1);
    });
  });

  describe('concurrent operations', () => {
    it('should handle concurrent publishes', async () => {
      const topic = 'concurrent-topic';
      const promises: Promise<void>[] = [];

      for (let i = 0; i < 10; i++) {
        promises.push(cachingPubsub.publish(topic, { type: `event-${i}`, runId: 'run-1', data: { index: i } }));
      }

      await Promise.all(promises);
      await new Promise(resolve => setTimeout(resolve, 50));

      const history = await cachingPubsub.getHistory(topic);
      expect(history).toHaveLength(10);
    });

    it('should handle concurrent subscribe with replay', async () => {
      const topic = 'concurrent-sub-topic';

      // Publish some events
      for (let i = 0; i < 5; i++) {
        await cachingPubsub.publish(topic, { type: `event-${i}`, runId: 'run-1', data: {} });
      }
      await new Promise(resolve => setTimeout(resolve, 10));

      // Multiple concurrent subscriptions with replay
      const callbacks = [vi.fn(), vi.fn(), vi.fn()];
      await Promise.all(callbacks.map(cb => cachingPubsub.subscribeWithReplay(topic, cb)));

      // Each callback should receive all cached events
      for (const callback of callbacks) {
        expect(callback).toHaveBeenCalledTimes(5);
      }
    });
  });

  describe('pull-mode transport correctness', () => {
    // A mock PubSub that behaves like Redis Streams: when subscribe() is
    // called, it immediately re-delivers the full backlog to the callback
    // (simulating XREADGROUP from id '0'). This exercises the buffering
    // and dedup paths that EventEmitterPubSub never triggers.
    class PullModePubSub extends PubSub {
      private published: Event[] = [];
      private listeners: Map<string, Set<EventCallback>> = new Map();
      private acked: Event[] = [];

      async publish(_topic: string, event: Omit<Event, 'id' | 'createdAt'>): Promise<void> {
        const full: Event = {
          ...event,
          id: crypto.randomUUID(),
          createdAt: new Date(),
        } as Event;
        this.published.push(full);
        // Deliver to existing listeners
        const cbs = this.listeners.get(_topic);
        if (cbs) {
          for (const cb of cbs)
            cb(full, async () => {
              this.acked.push(full);
            });
        }
      }

      async subscribe(_topic: string, cb: EventCallback): Promise<void> {
        let cbs = this.listeners.get(_topic);
        if (!cbs) {
          cbs = new Set();
          this.listeners.set(_topic, cbs);
        }
        cbs.add(cb);
        // Re-deliver full backlog immediately (pull-mode behavior)
        for (const event of this.published) {
          cb(event, async () => {
            this.acked.push(event);
          });
        }
      }

      async unsubscribe(_topic: string, cb: EventCallback): Promise<void> {
        this.listeners.get(_topic)?.delete(cb);
      }

      async flush(): Promise<void> {}

      getAckedIndices(): number[] {
        return this.acked.map(event => event.index!);
      }

      emitLiveOnly(
        topic: string,
        event: Event,
        ack?: Parameters<EventCallback>[1],
        nack?: Parameters<EventCallback>[2],
      ): void {
        const cbs = this.listeners.get(topic);
        if (cbs) {
          for (const cb of cbs) cb(event, ack, nack);
        }
      }
    }

    it('delivers history before live events even on pull-mode transports', async () => {
      const pullInner = new PullModePubSub();
      const pullCaching = new CachingPubSub(pullInner, cache);
      const topic = 'pull-order';

      // Publish 3 events that will be in the backlog
      await pullCaching.publish(topic, { type: 'e0', runId: 'r', data: {} });
      await pullCaching.publish(topic, { type: 'e1', runId: 'r', data: {} });
      await pullCaching.publish(topic, { type: 'e2', runId: 'r', data: {} });

      const received: number[] = [];
      await pullCaching.subscribeWithReplay(topic, (event: Event) => {
        received.push(event.index!);
      });

      // Events must arrive in index order, no duplicates
      expect(received).toEqual([0, 1, 2]);
      // The live backlog copies were suppressed at the replay boundary and
      // must still be acknowledged so the broker does not redeliver them.
      expect(pullInner.getAckedIndices()).toEqual([0, 1, 2]);
    });

    it('honors offset on live path for pull-mode transports', async () => {
      const pullInner = new PullModePubSub();
      const pullCaching = new CachingPubSub(pullInner, cache);
      const topic = 'pull-offset';

      // Publish 5 events
      for (let i = 0; i < 5; i++) {
        await pullCaching.publish(topic, { type: `e${i}`, runId: 'r', data: {} });
      }

      const received: number[] = [];
      await pullCaching.subscribeFromOffset(topic, 3, (event: Event) => {
        received.push(event.index!);
      });

      // Only events with index >= 3 should be delivered
      expect(received).toEqual([3, 4]);
      // Both skipped pre-offset deliveries and the replay/live duplicates are
      // acknowledged even though only the requested suffix reaches the user.
      expect(pullInner.getAckedIndices()).toEqual([0, 1, 2, 3, 4]);
    });

    it('delivers events published during getHistory bootstrap without duplication', async () => {
      const pullInner = new PullModePubSub();
      const pullCaching = new CachingPubSub(pullInner, cache);
      const topic = 'pull-bootstrap-race';

      // Pre-fill 2 events
      await pullCaching.publish(topic, { type: 'e0', runId: 'r', data: {} });
      await pullCaching.publish(topic, { type: 'e1', runId: 'r', data: {} });

      // Publish during getHistory to simulate the race
      const realGetHistory = pullCaching.getHistory.bind(pullCaching);
      let raced = false;
      vi.spyOn(pullCaching, 'getHistory').mockImplementation(async (t: string, offset?: number) => {
        if (!raced) {
          raced = true;
          await pullCaching.publish(topic, { type: 'e2', runId: 'r', data: {} });
        }
        return realGetHistory(t, offset);
      });

      const received: number[] = [];
      await pullCaching.subscribeWithReplay(topic, (event: Event) => {
        received.push(event.index!);
      });

      // All 3 events, each exactly once, in order
      expect(received).toEqual([0, 1, 2]);
    });

    it('live events after bootstrap are delivered in steady state', async () => {
      const pullInner = new PullModePubSub();
      const pullCaching = new CachingPubSub(pullInner, cache);
      const topic = 'pull-steady-state';

      await pullCaching.publish(topic, { type: 'e0', runId: 'r', data: {} });

      const received: number[] = [];
      await pullCaching.subscribeWithReplay(topic, (event: Event) => {
        received.push(event.index!);
      });

      expect(received).toEqual([0]);

      // Publish after bootstrap — should be delivered normally
      await pullCaching.publish(topic, { type: 'e1', runId: 'r', data: {} });
      await pullCaching.publish(topic, { type: 'e2', runId: 'r', data: {} });

      expect(received).toEqual([0, 1, 2]);
    });

    it('allows nack-redelivered events through even when index <= lastDelivered', async () => {
      const pullInner = new PullModePubSub();
      const pullCaching = new CachingPubSub(pullInner, cache);
      const topic = 'pull-nack-retry';

      await pullCaching.publish(topic, { type: 'e0', runId: 'r', data: {} });
      await pullCaching.publish(topic, { type: 'e1', runId: 'r', data: {} });

      const received: Array<{ index: number; attempt: number | undefined }> = [];
      await pullCaching.subscribeWithReplay(topic, (event: Event) => {
        received.push({ index: event.index!, attempt: event.deliveryAttempt });
      });

      expect(received).toEqual([
        { index: 0, attempt: undefined },
        { index: 1, attempt: undefined },
      ]);

      // Simulate a nack redelivery: same index, deliveryAttempt > 1
      pullInner.emitLiveOnly(topic, {
        id: 'retry-id',
        type: 'e1',
        runId: 'r',
        data: {},
        createdAt: new Date(),
        index: 1,
        deliveryAttempt: 2,
      });

      expect(received).toHaveLength(3);
      expect(received[2]).toEqual({ index: 1, attempt: 2 });
    });

    it('cleans up wrappedCb when replay bootstrap fails', async () => {
      const pullInner = new PullModePubSub();
      const pullCaching = new CachingPubSub(pullInner, cache);
      const topic = 'pull-bootstrap-fail';

      await pullCaching.publish(topic, { type: 'e0', runId: 'r', data: {} });

      vi.spyOn(pullCaching, 'getHistory').mockRejectedValueOnce(new Error('cache down'));

      const cb = vi.fn();
      await expect(pullCaching.subscribeWithReplay(topic, cb)).rejects.toThrow('cache down');

      // wrappedCb should have been unsubscribed — new events must not reach cb
      pullInner.emitLiveOnly(topic, {
        id: 'after-fail',
        type: 'e1',
        runId: 'r',
        data: {},
        createdAt: new Date(),
        index: 1,
      });

      expect(cb).not.toHaveBeenCalled();
    });

    it('preserves ack and nack handles for buffered live events that are delivered after history', async () => {
      const pullInner = new PullModePubSub();
      const pullCaching = new CachingPubSub(pullInner, cache);
      const topic = 'pull-buffer-handles';

      await pullCaching.publish(topic, { type: 'e0', runId: 'r', data: {} });

      const realGetHistory = pullCaching.getHistory.bind(pullCaching);
      let raced = false;
      const ackedByConsumer: number[] = [];
      vi.spyOn(pullCaching, 'getHistory').mockImplementation(async (t: string, offset?: number) => {
        if (!raced) {
          raced = true;
          pullInner.emitLiveOnly(
            topic,
            {
              id: 'live-only',
              type: 'e1',
              runId: 'r',
              data: {},
              createdAt: new Date(),
              index: 1,
            },
            async () => {
              ackedByConsumer.push(1);
            },
            async () => {},
          );
        }
        return realGetHistory(t, offset);
      });

      const received: number[] = [];
      await pullCaching.subscribeWithReplay(topic, (event: Event, ack, nack) => {
        received.push(event.index!);
        if (event.index === 1) {
          expect(nack).toBeDefined();
          void ack?.().then(() => ackedByConsumer.push(event.index!));
        }
      });

      await new Promise(resolve => setTimeout(resolve, 0));
      expect(received).toEqual([0, 1]);
      expect(ackedByConsumer).toEqual([1, 1]);
    });
  });
});

describe('CachingPubSub exact indexed replay', () => {
  const indexedReplay = { retentionMs: 60_000, maxEvents: 100 };

  function durableIndexedCache(): InMemoryServerCache {
    const cache = new InMemoryServerCache();
    Object.defineProperty(cache, 'indexedLogScope', { value: 'durable' });
    return cache;
  }

  class ManualPubSub extends PubSub {
    callback?: EventCallback;
    published: Event[] = [];

    async publish(_topic: string, event: PublishEvent): Promise<void> {
      this.published.push(event as Event);
    }

    async subscribe(_topic: string, callback: EventCallback): Promise<void> {
      this.callback = callback;
    }

    async unsubscribe(_topic: string, callback: EventCallback): Promise<void> {
      if (this.callback === callback) this.callback = undefined;
    }

    async flush(): Promise<void> {}

    async deliver(delivered: Event, ack?: () => Promise<void>, nack?: () => Promise<void>): Promise<void> {
      await this.callback?.(delivered, ack, nack);
    }
  }

  it('does not advertise exact replay for the legacy increment-plus-list cache path', async () => {
    const legacy = new CachingPubSub(new EventEmitterPubSub(), new InMemoryServerCache());

    expect(legacy.supportsIndexedReplay).toBe(false);
    expect(legacy.indexedReplay).toBeUndefined();

    await legacy.publish('legacy-topic', { type: 'legacy', runId: 'run', data: {} });
    expect(await legacy.getHistory('legacy-topic')).toHaveLength(1);
  });

  it('assigns and retains concurrent cursors in one atomic order', async () => {
    const cache = new InMemoryServerCache();
    const listPush = vi.spyOn(cache, 'listPush');
    const exact = new CachingPubSub(new EventEmitterPubSub(), cache, { indexedReplay });

    await Promise.all(
      Array.from({ length: 50 }, (_, index) =>
        exact.publish('concurrent-topic', { type: `event-${index}`, runId: 'run', data: { index } }),
      ),
    );

    const history = await exact.getHistory('concurrent-topic');
    expect(history.map(event => event.index)).toEqual(Array.from({ length: 50 }, (_, index) => index));
    expect(new Set(history.map(event => event.id))).toHaveLength(50);
    expect(listPush).not.toHaveBeenCalled();
  });

  it('retains localOnly events only when exact replay is process-scoped', async () => {
    const exact = new CachingPubSub(new EventEmitterPubSub(), new InMemoryServerCache(), { indexedReplay });

    await exact.publish(
      'local-exact-topic',
      { type: 'local', runId: 'run', data: { nonSerializable: () => 'same-process' } },
      { localOnly: true },
    );

    await expect(exact.getHistory('local-exact-topic')).resolves.toMatchObject([
      { type: 'local', index: 0, data: { nonSerializable: expect.any(Function) } },
    ]);
  });

  it('rejects localOnly events for durable exact replay by default', async () => {
    const cache = durableIndexedCache();
    const inner = new EventEmitterPubSub();
    const innerPublish = vi.spyOn(inner, 'publish');
    const append = vi.spyOn(cache, 'appendIndexedLogEntry');
    const exact = new CachingPubSub(inner, cache, { indexedReplay });

    await expect(
      exact.publish(
        'local-durable-topic',
        { type: 'local', runId: 'run', data: { nonSerializable: () => 'same-process' } },
        { localOnly: true },
      ),
    ).rejects.toThrow('cannot provide durable exact replay for a localOnly event');

    expect(append).not.toHaveBeenCalled();
    expect(innerPublish).not.toHaveBeenCalled();
  });

  it('explicitly passes durable localOnly events through without retaining them', async () => {
    const cache = durableIndexedCache();
    const inner = new EventEmitterPubSub();
    const innerPublish = vi.spyOn(inner, 'publish');
    const append = vi.spyOn(cache, 'appendIndexedLogEntry');
    const exact = new CachingPubSub(inner, cache, {
      indexedReplay,
      durableLocalOnly: 'passthrough',
    });
    const received = vi.fn();
    const data = { nonSerializable: () => 'same-process' };
    await inner.subscribe('local-durable-topic', received);

    await exact.publish('local-durable-topic', { type: 'local', runId: 'run', data }, { localOnly: true });

    expect(innerPublish).toHaveBeenCalledWith(
      'local-durable-topic',
      { type: 'local', runId: 'run', data },
      { localOnly: true },
    );
    expect(received).toHaveBeenCalledTimes(1);
    expect(received.mock.calls[0]![0].data).toBe(data);
    expect(append).not.toHaveBeenCalled();
    await expect(exact.getHistory('local-durable-topic')).resolves.toEqual([]);

    await exact.publish('portable-durable-topic', { type: 'portable', runId: 'run', data: { value: 1 } });

    expect(append).toHaveBeenCalledTimes(1);
    await expect(exact.getHistory('portable-durable-topic')).resolves.toMatchObject([
      { type: 'portable', index: 0, data: { value: 1 } },
    ]);
  });

  it('re-reads retention after a live append races an in-flight callback', async () => {
    const exact = new CachingPubSub(new EventEmitterPubSub(), new InMemoryServerCache(), { indexedReplay });
    const received: number[] = [];
    const activeError = vi.fn();
    let releaseFirst!: () => void;
    const firstBlocked = new Promise<void>(resolve => {
      releaseFirst = resolve;
    });

    await exact.subscribeFromOffset(
      'append-during-callback-topic',
      0,
      async event => {
        received.push(event.index!);
        if (event.index === 0) await firstBlocked;
      },
      { onError: activeError },
    );

    await exact.publish('append-during-callback-topic', { type: 'zero', runId: 'run', data: {} });
    await vi.waitFor(() => expect(received).toEqual([0]));
    await exact.publish('append-during-callback-topic', { type: 'one', runId: 'run', data: {} });
    releaseFirst();

    await vi.waitFor(() => expect(received).toEqual([0, 1]));
    expect(activeError).not.toHaveBeenCalled();
  });

  it('drains bounded backend pages through the retained log head', async () => {
    const cache = new InMemoryServerCache();
    const readPage = cache.readIndexedLogEntries.bind(cache);
    vi.spyOn(cache, 'readIndexedLogEntries').mockImplementation(async (...args: any[]) => {
      const result = await (readPage as any)(...args);
      return { ...result, entries: result.entries.slice(0, 3) };
    });
    const exact = new CachingPubSub(new EventEmitterPubSub(), cache, { indexedReplay });
    for (let index = 0; index < 11; index++) {
      await exact.publish('paged-retention-topic', { type: `event-${index}`, runId: 'run', data: {} });
    }

    const history = await exact.getHistory('paged-retention-topic');
    expect(history.map(event => event.index)).toEqual(Array.from({ length: 11 }, (_, index) => index));
    expect(cache.readIndexedLogEntries).toHaveBeenCalledTimes(4);
    vi.mocked(cache.readIndexedLogEntries).mockClear();

    const cursors: number[] = [];
    await exact.subscribeFromOffset('paged-retention-topic', 0, event => {
      cursors.push(event.index!);
    });

    expect(cursors).toEqual(Array.from({ length: 11 }, (_, index) => index));
    expect(cache.readIndexedLogEntries).toHaveBeenCalledTimes(4);
  });

  it('does not chase a moving retained head while reading paged history', async () => {
    const cache = new InMemoryServerCache();
    const exact = new CachingPubSub(new EventEmitterPubSub(), cache, { indexedReplay });
    const topic = 'moving-history-head-topic';
    for (let index = 0; index < 5; index++) {
      await exact.publish(topic, { type: `event-${index}`, runId: 'run', data: {} });
    }
    const readPage = cache.readIndexedLogEntries.bind(cache);
    let appended = 0;
    vi.spyOn(cache, 'readIndexedLogEntries').mockImplementation(async (...args: any[]) => {
      if (appended >= 10) throw new Error('history chased a continuously moving head');
      const result = await (readPage as any)(...args);
      await cache.appendIndexedLogEntry(
        args[0],
        {
          type: `late-${appended}`,
          id: `late-history-${appended}`,
          runId: 'run',
          createdAt: new Date('2026-07-15T00:00:00.000Z'),
          data: {},
        },
        args[2],
      );
      appended += 1;
      return { ...result, entries: result.entries.slice(0, 2) };
    });

    const history = await exact.getHistory(topic);

    expect(history.map(event => event.index)).toEqual([0, 1, 2, 3, 4]);
    expect(appended).toBe(3);
  });

  it('does not chase a moving retained head during paged subscription bootstrap', async () => {
    const cache = new InMemoryServerCache();
    const exact = new CachingPubSub(new EventEmitterPubSub(), cache, { indexedReplay });
    const topic = 'moving-bootstrap-head-topic';
    for (let index = 0; index < 5; index++) {
      await exact.publish(topic, { type: `event-${index}`, runId: 'run', data: {} });
    }
    const readPage = cache.readIndexedLogEntries.bind(cache);
    let appended = 0;
    vi.spyOn(cache, 'readIndexedLogEntries').mockImplementation(async (...args: any[]) => {
      if (appended >= 10) throw new Error('bootstrap chased a continuously moving head');
      const result = await (readPage as any)(...args);
      await cache.appendIndexedLogEntry(
        args[0],
        {
          type: `late-${appended}`,
          id: `late-bootstrap-${appended}`,
          runId: 'run',
          createdAt: new Date('2026-07-15T00:00:00.000Z'),
          data: {},
        },
        args[2],
      );
      appended += 1;
      return { ...result, entries: result.entries.slice(0, 2) };
    });
    const received: number[] = [];

    await exact.subscribeFromOffset(topic, 0, event => {
      received.push(event.index!);
    });

    expect(received).toEqual([0, 1, 2, 3, 4]);
    expect(appended).toBe(3);
  });

  it('fills a live reorder gap from retained history without prematurely acknowledging the lower cursor', async () => {
    const inner = new ManualPubSub();
    const exact = new CachingPubSub(inner, new InMemoryServerCache(), { indexedReplay });
    const received: number[] = [];
    let releaseZero!: () => void;
    const holdZero = new Promise<void>(resolve => {
      releaseZero = resolve;
    });

    await exact.subscribeFromOffset('reordered-topic', 0, async (event, ack) => {
      received.push(event.index!);
      if (event.index === 0) await holdZero;
      await ack?.();
    });
    await exact.publish('reordered-topic', { type: 'zero', runId: 'run', data: {} });
    await exact.publish('reordered-topic', { type: 'one', runId: 'run', data: {} });

    const [zero, one] = inner.published;
    const ackZero = vi.fn(async () => {});
    const ackOne = vi.fn(async () => {});
    const deliverOne = inner.deliver(
      one!,
      ackOne,
      vi.fn(async () => {}),
    );
    await vi.waitFor(() => expect(received).toEqual([0]));

    const deliverZero = inner.deliver(
      zero!,
      ackZero,
      vi.fn(async () => {}),
    );
    expect(ackZero).not.toHaveBeenCalled();
    expect(ackOne).not.toHaveBeenCalled();

    releaseZero();
    await Promise.all([deliverOne, deliverZero]);

    expect(received).toEqual([0, 1]);
    expect(ackZero).toHaveBeenCalledTimes(1);
    expect(ackOne).toHaveBeenCalledTimes(1);
  });

  it('uses callback resolution as the commit boundary for cache-only replay and explicit ack for live delivery', async () => {
    const inner = new ManualPubSub();
    const exact = new CachingPubSub(inner, new InMemoryServerCache(), { indexedReplay });
    await exact.publish('ack-contract-topic', { type: 'replayed', runId: 'run', data: {} });

    const deliveries: Array<{ type: string; hasAck: boolean; hasNack: boolean }> = [];
    await exact.subscribeFromOffset('ack-contract-topic', 0, async (event, ack, nack) => {
      deliveries.push({ type: event.type, hasAck: Boolean(ack), hasNack: Boolean(nack) });
      await ack?.();
    });

    expect(deliveries).toEqual([{ type: 'replayed', hasAck: false, hasNack: false }]);

    await exact.publish('ack-contract-topic', { type: 'live', runId: 'run', data: {} });
    const acknowledgeLive = vi.fn(async () => {});
    const negativeAcknowledgeLive = vi.fn(async () => {});
    await inner.deliver(inner.published[1]!, acknowledgeLive, negativeAcknowledgeLive);

    expect(deliveries).toEqual([
      { type: 'replayed', hasAck: false, hasNack: false },
      { type: 'live', hasAck: true, hasNack: true },
    ]);
    expect(acknowledgeLive).toHaveBeenCalledTimes(1);
    expect(negativeAcknowledgeLive).not.toHaveBeenCalled();
  });

  it('does not flush while an exact lifecycle callback is still in flight', async () => {
    const inner = new ManualPubSub();
    const exact = new CachingPubSub(inner, new InMemoryServerCache(), { indexedReplay });
    let release!: () => void;
    let markStarted!: () => void;
    const blocked = new Promise<void>(resolve => {
      release = resolve;
    });
    const started = new Promise<void>(resolve => {
      markStarted = resolve;
    });
    await exact.subscribeFromOffset('flush-boundary-topic', 0, async (_event, ack) => {
      markStarted();
      await blocked;
      await ack?.();
    });
    await exact.publish('flush-boundary-topic', { type: 'blocked', runId: 'run', data: {} });
    const delivery = inner.deliver(
      inner.published[0]!,
      vi.fn(async () => {}),
    );
    await started;

    let flushed = false;
    const flush = exact.flush().then(() => {
      flushed = true;
    });
    await Promise.resolve();
    expect(flushed).toBe(false);

    release();
    await Promise.all([delivery, flush]);
    expect(flushed).toBe(true);
  });

  it('flushes a nack redelivery scheduled after the first inner flush', async () => {
    class RedeliveringPubSub extends ManualPubSub {
      private pendingRedeliveries: Array<() => Promise<void>> = [];

      scheduleRedelivery(event: Event, ack: () => Promise<void>): () => Promise<void> {
        return async () => {
          this.pendingRedeliveries.push(() =>
            this.deliver({ ...event, deliveryAttempt: (event.deliveryAttempt ?? 1) + 1 }, ack, async () => {}),
          );
        };
      }

      override async flush(): Promise<void> {
        while (this.pendingRedeliveries.length > 0) {
          const pending = this.pendingRedeliveries.splice(0);
          await Promise.all(pending.map(redeliver => redeliver()));
        }
      }
    }

    const inner = new RedeliveringPubSub();
    const exact = new CachingPubSub(inner, new InMemoryServerCache(), { indexedReplay });
    let releaseFirst!: () => void;
    let markFirstEntered!: () => void;
    const firstBlocked = new Promise<void>(resolve => {
      releaseFirst = resolve;
    });
    const firstEntered = new Promise<void>(resolve => {
      markFirstEntered = resolve;
    });
    const attempts: number[] = [];
    const redeliveryAck = vi.fn(async () => {});
    await exact.subscribeFromOffset('flush-redelivery-topic', 0, async (event, ack, nack) => {
      attempts.push(event.deliveryAttempt ?? 1);
      if ((event.deliveryAttempt ?? 1) === 1) {
        markFirstEntered();
        await firstBlocked;
        await nack?.();
        return;
      }
      await ack?.();
    });
    await exact.publish('flush-redelivery-topic', { type: 'retry-me', runId: 'run', data: {} });
    const retained = inner.published[0]!;
    const firstDelivery = inner
      .deliver(retained, async () => {}, inner.scheduleRedelivery(retained, redeliveryAck))
      .catch(() => {});
    await firstEntered;

    const flush = exact.flush();
    releaseFirst();
    await Promise.all([firstDelivery, flush]);

    expect(attempts).toEqual([1, 2]);
    expect(redeliveryAck).toHaveBeenCalledTimes(1);
  });

  it('fails closed before live publish when atomic retention fails', async () => {
    const cache = new InMemoryServerCache();
    vi.spyOn(cache, 'appendIndexedLogEntry').mockRejectedValueOnce(new Error('indexed store unavailable'));
    const inner = new ManualPubSub();
    const exact = new CachingPubSub(inner, cache, { indexedReplay });

    await expect(
      exact.publish('failed-append-topic', { type: 'not-live-only', runId: 'run', data: {} }),
    ).rejects.toThrow('indexed store unavailable');

    expect(inner.published).toEqual([]);
    expect(await exact.getHistory('failed-append-topic')).toEqual([]);
  });

  it('reports the retained floor and rejects a cursor older than bounded history', async () => {
    const exact = new CachingPubSub(new EventEmitterPubSub(), new InMemoryServerCache(), {
      indexedReplay: { retentionMs: 60_000, maxEvents: 2 },
    });
    await exact.publish('bounded-topic', { type: 'zero', runId: 'run', data: {} });
    await exact.publish('bounded-topic', { type: 'one', runId: 'run', data: {} });
    await exact.publish('bounded-topic', { type: 'two', runId: 'run', data: {} });

    await expect(exact.getIndexedReplayRange('bounded-topic')).resolves.toMatchObject({
      scope: 'process',
      retentionMs: 60_000,
      maxEvents: 2,
      firstCursor: 1,
      nextCursor: 3,
    });
    await expect(exact.subscribeFromOffset('bounded-topic', 0, () => {})).rejects.toMatchObject<
      Partial<IndexedReplayCursorError>
    >({
      reason: 'cursor-too-old',
      requestedCursor: 0,
    });

    const retained: number[] = [];
    await exact.subscribeFromOffset('bounded-topic', 1, event => {
      retained.push(event.index!);
    });
    expect(retained).toEqual([1, 2]);
  });

  it('changes log generation after full retention expiry and rejects the old generation', async () => {
    vi.useFakeTimers();
    try {
      const exact = new CachingPubSub(new EventEmitterPubSub(), new InMemoryServerCache(), {
        indexedReplay: { retentionMs: 10, maxEvents: 10 },
      });
      await exact.publish('expired-topic', { type: 'zero', runId: 'run', data: {} });
      const beforeExpiry = (await exact.getIndexedReplayRange('expired-topic'))!;

      await vi.advanceTimersByTimeAsync(11);
      const afterExpiry = (await exact.getIndexedReplayRange('expired-topic'))!;

      expect(afterExpiry.logGeneration).not.toBe(beforeExpiry.logGeneration);
      expect(afterExpiry).toMatchObject({ firstCursor: 0, nextCursor: 0 });
      await expect(
        exact.subscribeFromOffset('expired-topic', 1, () => {}, {
          logGeneration: beforeExpiry.logGeneration,
        }),
      ).rejects.toMatchObject<Partial<IndexedReplayCursorError>>({
        reason: 'generation-mismatch',
        requestedLogGeneration: beforeExpiry.logGeneration,
      });
    } finally {
      vi.useRealTimers();
    }
  });

  it('keeps an empty generation stable while waiting beyond event retention', async () => {
    vi.useFakeTimers();
    try {
      const exact = new CachingPubSub(new EventEmitterPubSub(), new InMemoryServerCache(), {
        indexedReplay: { retentionMs: 10, maxEvents: 10 },
      });
      const beforeWaiting = (await exact.getIndexedReplayRange('future-topic'))!;

      await vi.advanceTimersByTimeAsync(100);
      const afterWaiting = (await exact.getIndexedReplayRange('future-topic'))!;
      await exact.publish('future-topic', { type: 'first', runId: 'run', data: {} });
      const afterFirstAppend = (await exact.getIndexedReplayRange('future-topic'))!;

      expect(afterWaiting.logGeneration).toBe(beforeWaiting.logGeneration);
      expect(afterFirstAppend.logGeneration).toBe(beforeWaiting.logGeneration);
      expect(afterFirstAppend).toMatchObject({ firstCursor: 0, nextCursor: 1 });
    } finally {
      vi.useRealTimers();
    }
  });

  it('does not mistake a recreated log cursor for an old live duplicate', async () => {
    const inner = new ManualPubSub();
    const exact = new CachingPubSub(inner, new InMemoryServerCache(), { indexedReplay });
    const received: string[] = [];
    const activeError = vi.fn(async () => {});

    await exact.subscribeFromOffset(
      'recreated-topic',
      0,
      async (event, ack) => {
        received.push(event.type);
        await ack?.();
      },
      { onError: activeError },
    );
    await exact.publish('recreated-topic', { type: 'old-generation', runId: 'run', data: {} });

    const acknowledgeOld = vi.fn(async () => {});
    await inner.deliver(
      inner.published[0]!,
      acknowledgeOld,
      vi.fn(async () => {}),
    );
    expect(received).toEqual(['old-generation']);
    expect(acknowledgeOld).toHaveBeenCalledTimes(1);

    await exact.clearTopic('recreated-topic');
    await exact.publish('recreated-topic', { type: 'new-generation', runId: 'run', data: {} });

    const acknowledgeNew = vi.fn(async () => {});
    await expect(
      inner.deliver(
        inner.published[1]!,
        acknowledgeNew,
        vi.fn(async () => {}),
      ),
    ).rejects.toMatchObject<Partial<IndexedReplayCursorError>>({ reason: 'generation-mismatch' });
    expect(received).toEqual(['old-generation']);
    expect(acknowledgeNew).not.toHaveBeenCalled();
    expect(activeError).toHaveBeenCalledTimes(1);
    expect(activeError).toHaveBeenCalledWith(expect.objectContaining({ reason: 'generation-mismatch' }));

    // Terminal replay errors unsubscribe the stale watcher, so it cannot
    // fail/log forever on every event in the recreated generation.
    await exact.publish('recreated-topic', { type: 'new-generation-second', runId: 'run', data: {} });
    await inner.deliver(inner.published[2]!);
    expect(activeError).toHaveBeenCalledTimes(1);
  });
});
