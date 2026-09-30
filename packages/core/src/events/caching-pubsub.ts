import { isAtomicIndexedLogCache } from '../cache/base';
import type { AtomicIndexedLogCache, IndexedLogRetention, MastraServerCache } from '../cache/base';
import type { IMastraLogger } from '../logger';
import { IndexedReplayCursorError, IndexedReplayIntegrityError, isLeaseProvider, PubSub } from './pubsub';
import type {
  IndexedReplayCapability,
  IndexedReplayRange,
  IndexedReplaySubscribeOptions,
  LeaseProvider,
} from './pubsub';
import type { Event, EventCallback, PublishEvent, SubscribeOptions } from './types';

export interface CachingPubSubIndexedReplayOptions {
  /** Maximum age of exact retained history. */
  retentionMs: number;
  /** Maximum exact retained events per topic. */
  maxEvents: number;
}

/**
 * Options for CachingPubSub
 */
export interface CachingPubSubOptions {
  /**
   * Optional prefix for cache keys to namespace events.
   * Defaults to 'pubsub:'.
   */
  keyPrefix?: string;
  /**
   * Optional logger for structured logging.
   * Falls back to console.error if not provided.
   */
  logger?: IMastraLogger;
  /**
   * Opt into exact indexed replay when the cache implements atomic indexed
   * append. Without this option, caching and replay keep their historical
   * best-effort behavior and are not advertised as restart-safe.
   */
  indexedReplay?: CachingPubSubIndexedReplayOptions;
  /**
   * Policy for `localOnly` publishes when exact indexed replay uses a durable
   * cache.
   *
   * Durable exact replay rejects these publishes by default because an event
   * that stays in one process cannot also be part of shared retained history.
   * Set this to `passthrough` only when local-only topics are intentionally
   * excluded from replay. The event bypasses the cache and is forwarded to the
   * inner PubSub with its locality option intact.
   */
  durableLocalOnly?: 'reject' | 'passthrough';
  /**
   * Optional per-topic caching policy. Defaults to caching every topic.
   *
   * Returning `false` takes the same path as a `localOnly` publish: no list
   * entry, no index, no counter allocation — the event is handed straight to
   * the inner PubSub and still delivered live.
   *
   * This exists because `localOnly` is not always visible here. When a caller
   * wraps a PubSub that itself decides `localOnly` (e.g. the `mastra.pubsub`
   * proxy), the cache runs *above* that decision and never sees the flag, so
   * the policy has to be supplied at construction time instead.
   */
  shouldCache?: (topic: string) => boolean;
  /**
   * Optional bus to follow for topics this cache serves.
   *
   * Publishers that don't go through this `CachingPubSub` (e.g. the evented
   * workflow engine, which publishes agent-stream events on `mastra.pubsub`
   * from whichever worker executes a step) never hit `publish()` here, so
   * their events are neither cached nor — when `source` is a different
   * transport than `inner` — delivered to local subscribers at all. Setting
   * `source` makes this cache a follower: whenever a topic has local
   * subscribers, the cache also subscribes to `source`, assigns indices to and
   * caches events that haven't passed through a caching layer yet, and (when
   * `source` and `inner` are genuinely different transports) republishes them
   * into `inner` for live delivery. See {@link CachingPubSub.__setSource}.
   */
  source?: PubSub;
}

/**
 * A PubSub decorator that adds event caching and replay capabilities.
 *
 * Wraps any PubSub implementation and uses MastraServerCache to:
 * - Cache all published events per topic
 * - Enable replay of cached events for late subscribers
 *
 * This enables resumable streams - clients can disconnect and reconnect
 * without missing events.
 *
 * ## Batching
 *
 * `CachingPubSub` is transparent to `options.batch`: `subscribe()` forwards
 * the option to the inner PubSub, and `supportsNativeBatching` mirrors the
 * inner's value. Wrapping a non-native inner with `{ batch: {...} }` results
 * in unbatched delivery — use an inner that returns
 * `supportsNativeBatching === true` (e.g. `EventEmitterPubSub`) if you need
 * batched delivery.
 *
 * @example
 * ```typescript
 * import { EventEmitterPubSub, CachingPubSub } from '@mastra/core/events';
 * import { InMemoryServerCache } from '@mastra/core/cache';
 *
 * const cache = new InMemoryServerCache();
 * const pubsub = new CachingPubSub(new EventEmitterPubSub(), cache);
 *
 * // Subscribe with replay - receives cached events first, then live
 * await pubsub.subscribeWithReplay('my-topic', (event) => {
 *   console.log(event);
 * });
 * ```
 */
export class CachingPubSub extends PubSub {
  private readonly keyPrefix: string;
  private readonly logger?: IMastraLogger;
  private readonly indexedLog?: AtomicIndexedLogCache;
  private readonly indexedLogRetention?: IndexedLogRetention;
  private readonly durableLocalOnly: 'reject' | 'passthrough';
  private readonly shouldCache?: (topic: string) => boolean;
  /** Maps original callbacks to their wrapped versions for proper unsubscribe */
  private callbackMap = new Map<EventCallback, EventCallback>();
  private subscriptionDrains = new Map<EventCallback, () => Promise<void>>();
  /** Bus this cache follows for externally-published events. See {@link __setSource}. */
  private source?: PubSub;
  /** Per-topic follower subscriptions on {@link source}, refcounted by local subscribers. */
  private followers = new Map<string, { source: PubSub; cb: EventCallback; refs: number }>();

  constructor(
    private readonly inner: PubSub,
    /** Cache backing retained history. Exposed so sibling transport decorators can share one exact log. */
    readonly cache: MastraServerCache,
    options: CachingPubSubOptions = {},
  ) {
    super();
    this.keyPrefix = options.keyPrefix ?? 'pubsub:';
    this.logger = options.logger;
    this.durableLocalOnly = options.durableLocalOnly ?? 'reject';
    this.shouldCache = options.shouldCache;

    if (options.indexedReplay) {
      if (!Number.isSafeInteger(options.indexedReplay.retentionMs) || options.indexedReplay.retentionMs <= 0) {
        throw new TypeError('CachingPubSub indexedReplay.retentionMs must be a positive safe integer');
      }
      if (!Number.isSafeInteger(options.indexedReplay.maxEvents) || options.indexedReplay.maxEvents <= 0) {
        throw new TypeError('CachingPubSub indexedReplay.maxEvents must be a positive safe integer');
      }
      if (!isAtomicIndexedLogCache(cache)) {
        throw new TypeError('CachingPubSub exact indexed replay requires a cache with atomic indexed-log support');
      }
      this.indexedLog = cache;
      this.indexedLogRetention = {
        maxAgeMs: options.indexedReplay.retentionMs,
        maxEntries: options.indexedReplay.maxEvents,
      };
    }
    if (options.source) this.__setSource(options.source);
  }

  /**
   * Set (or replace) the source bus this cache follows.
   *
   * Exists separately from the constructor because the natural source is often
   * not known at construction time: a durable agent builds its `CachingPubSub`
   * lazily, but `mastra.pubsub` only becomes available at registration.
   * Replacing the source only affects topics that start being followed
   * afterwards — existing follower subscriptions stay on the bus they were
   * created against until released.
   * @internal
   */
  __setSource(source: PubSub | undefined): void {
    // Following ourselves is meaningless and hazardous: every publish on such
    // a source already flows through this.publish(), so any un-indexed event
    // the follower would observe is one publish() *deliberately* declined to
    // cache (the `localOnly` bypass, or `shouldCache`) — re-caching it would
    // resurrect the run-local cache leak (#21668). The `__self()` probe sees
    // through the `mastra.pubsub` proxy, which breaks plain reference
    // equality while still targeting this very instance (the reuse-not-
    // double-wrap configuration, #18148).
    if (source === this || source?.__self?.() === this) return;
    this.source = source;
  }

  /** Unwrap to the inner transport so aliasing checks see through this decorator. */
  override __rawBus(): PubSub {
    return this.inner.__rawBus();
  }

  get supportsNativeBatching(): boolean {
    return this.inner.supportsNativeBatching;
  }

  override get indexedReplay(): IndexedReplayCapability | undefined {
    if (!this.indexedLog || !this.indexedLogRetention) return undefined;
    return {
      scope: this.indexedLog.indexedLogScope,
      retentionMs: this.indexedLogRetention.maxAgeMs,
      maxEvents: this.indexedLogRetention.maxEntries,
    };
  }

  override get supportsOffsets(): boolean {
    return true;
  }

  /**
   * Log an error message using the configured logger or console.error.
   */
  private logError(message: string, error: unknown): void {
    if (this.logger) {
      this.logger.error(message, error);
    } else {
      console.error(message, error);
    }
  }

  /**
   * Stable key used to deduplicate an event across the cache-replay and
   * live-delivery paths.
   *
   * The sequential `index` is the authoritative ordering key. Inner transports
   * also preserve the identity assigned by `CachingPubSub.publish`, but the
   * index remains the stronger deduplication key because it represents the
   * event's exact position in retained history.
   */
  private dedupKey(event: Event): string {
    return event.index !== undefined ? `i:${event.index}` : `id:${event.id}`;
  }

  /** Restore values that JSON-backed cache adapters cannot retain by prototype. */
  private retainedEvent(value: unknown): Event {
    if (typeof value !== 'object' || value === null) {
      throw new IndexedReplayIntegrityError('malformed-retained-event', 'Retained event must be an object');
    }
    const event = value as Partial<Event> & { createdAt?: Date | string | number };
    if (
      typeof event.type !== 'string' ||
      event.type.length === 0 ||
      typeof event.id !== 'string' ||
      event.id.length === 0 ||
      typeof event.runId !== 'string' ||
      event.runId.length === 0 ||
      !('data' in event)
    ) {
      throw new IndexedReplayIntegrityError(
        'malformed-retained-event',
        'Retained event is missing a valid type, id, runId, or data field',
      );
    }
    if (event.index !== undefined && (!Number.isSafeInteger(event.index) || event.index < 0)) {
      throw new IndexedReplayIntegrityError('malformed-retained-event', 'Retained event has an invalid cursor');
    }
    if (event.logGeneration !== undefined && (typeof event.logGeneration !== 'string' || !event.logGeneration)) {
      throw new IndexedReplayIntegrityError('malformed-retained-event', 'Retained event has an invalid log generation');
    }
    if (
      event.deliveryAttempt !== undefined &&
      (!Number.isSafeInteger(event.deliveryAttempt) || event.deliveryAttempt < 1)
    ) {
      throw new IndexedReplayIntegrityError(
        'malformed-retained-event',
        'Retained event has an invalid delivery attempt',
      );
    }

    const createdAt =
      event.createdAt instanceof Date
        ? event.createdAt
        : typeof event.createdAt === 'string' || typeof event.createdAt === 'number'
          ? new Date(event.createdAt)
          : undefined;
    if (!createdAt || Number.isNaN(createdAt.getTime())) {
      throw new IndexedReplayIntegrityError('malformed-retained-event', 'Retained event has an invalid createdAt');
    }

    return event.createdAt === createdAt ? (event as Event) : ({ ...event, createdAt } as Event);
  }

  /**
   * Get the cache key for a topic's event list
   */
  private getCacheKey(topic: string): string {
    return `${this.keyPrefix}${topic}`;
  }

  /**
   * Get the cache key for a topic's index counter
   */
  private getCounterKey(topic: string): string {
    return `${this.keyPrefix}${topic}:counter`;
  }

  private getIndexedLogKey(topic: string): string {
    return `${this.keyPrefix}${topic}:indexed-log:v1`;
  }

  /**
   * Whether {@link source} is backed by the same transport as {@link inner}.
   *
   * When they alias (e.g. the test harness passes one `EventEmitterPubSub`
   * both to the agent and to `Mastra`, or the agent adopted `mastra.pubsub`
   * as its inner bus), local subscribers already receive source-published
   * events through their inner subscription — the follower must then only
   * cache, never republish, or every event would be delivered twice.
   */
  private isSourceAliased(source: PubSub): boolean {
    return source.__rawBus() === this.inner.__rawBus();
  }

  /**
   * Start following {@link source} for a topic (idempotent, refcounted).
   * Called on every local subscription entry point; released on unsubscribe
   * and torn down on {@link clearTopic}.
   *
   * The follower's contract:
   * - On an aliased source, events that carry an `index` are this cache's own
   *   publish echo (or its own republish) — ack and ignore them. This is both
   *   the dedup and the republish loop guard.
   * - On a non-aliased source, an `index` means the event passed through a
   *   *foreign* caching tier (e.g. a user-supplied `CachingPubSub` handed to
   *   `new Mastra({ pubsub })` while this agent runs its own cache tier).
   *   That tier's cache is not the one this instance replays from, so the
   *   event is treated like an un-indexed one: cached here under a fresh
   *   index (replacing the foreign index) and republished into `inner`.
   *   Dropping it instead would lose the event for local subscribers entirely
   *   — the exact stream-loss failure `source` following exists to prevent
   *   (#20646).
   * - Un-indexed events were published directly on the source bus (e.g. by
   *   the evented workflow engine, possibly from another process). Assign an
   *   index, cache them (subject to `shouldCache`), and — only when source and
   *   inner are different transports — republish the indexed copy into `inner`
   *   so local subscribers receive it live. On an aliased bus the original
   *   delivery already reached them.
   *
   * Subscribes with `startFrom: 'latest'`: the follower's job is to mirror
   * live traffic into the cache, not to re-ingest a persistent backend's
   * retained history — re-reading from 'earliest' after a process restart
   * would re-cache already-cached events under fresh indices.
   */
  private async retainFollower(topic: string): Promise<void> {
    const source = this.source;
    if (!source) return;
    const existing = this.followers.get(topic);
    if (existing) {
      existing.refs++;
      return;
    }

    const followerCb: EventCallback = async (event, ack) => {
      try {
        const aliased = this.isSourceAliased(source);
        if (event.index !== undefined && aliased) {
          return await ack?.();
        }
        let outbound: Event = event;
        if (this.shouldCache?.(topic) !== false) {
          // Same single-round-trip op as publish() (#22477): the follower sits
          // on the identical per-chunk hot path for engine-originated events.
          // The stored copy carries the allocated index, preserving the
          // "indexed ⇒ already cached" dedup contract.
          try {
            const index = await this.cache.listPushIndexed(this.getCacheKey(topic), this.getCounterKey(topic), event);
            outbound = { ...event, index };
          } catch (error) {
            this.logError(`[CachingPubSub] Failed to cache followed event for ${topic}`, error);
          }
        }
        if (!aliased) {
          // Inner implementations regenerate `id` but preserve `index`, same
          // as the publish() path — live and cached copies dedup by index.
          await this.inner.publish(topic, outbound);
        }
        await ack?.();
      } catch (error) {
        this.logError(`[CachingPubSub] Follower failed for ${topic}`, error);
        await ack?.();
      }
    };

    // Record synchronously so concurrent retains don't double-subscribe.
    const entry = { source, cb: followerCb, refs: 1 };
    this.followers.set(topic, entry);
    try {
      await source.subscribe(topic, followerCb, { startFrom: 'latest' });
    } catch (error) {
      this.followers.delete(topic);
      this.logError(`[CachingPubSub] Failed to follow source for ${topic}`, error);
    }
  }

  /** Drop one follower reference for a topic; unsubscribe from source at zero. */
  private async releaseFollower(topic: string): Promise<void> {
    const entry = this.followers.get(topic);
    if (!entry) return;
    entry.refs--;
    if (entry.refs > 0) return;
    this.followers.delete(topic);
    await entry.source.unsubscribe(topic, entry.cb).catch?.(() => {});
  }

  /** Tear down a topic's follower unconditionally (stream completion). */
  private async stopFollowing(topic: string): Promise<void> {
    const entry = this.followers.get(topic);
    if (!entry) return;
    this.followers.delete(topic);
    await entry.source.unsubscribe(topic, entry.cb).catch?.(() => {});
  }

  /**
   * Publish an event to a topic.
   * The event is cached with a sequential index before being published to the inner PubSub.
   *
   * Uses atomic increment for index assignment to prevent race conditions
   * when multiple events are published concurrently.
   *
   * `localOnly` events bypass the cache entirely — no list entry, no index, no
   * counter allocation — and are handed straight to the inner PubSub. They are
   * never relayed to other instances, so a shared replay cache can have no
   * reader for them; caching them only grows the store without bound (some
   * payloads, e.g. `workflow.events.v2.*` watch events carrying cumulative step
   * results, are multiple megabytes each). Consumers of `localOnly` topics
   * subscribe live and never replay, and every downstream `index` check is
   * guarded for absence, so dropping the index is safe.
   *
   * Topics rejected by the `shouldCache` option take the exact same path, for
   * callers whose `localOnly` decision is made below this layer.
   */
  async publish(topic: string, event: PublishEvent, options?: { localOnly?: boolean }): Promise<void> {
    // Topics the construction-time policy excludes from caching take the same
    // path as a `localOnly` publish: no retention, no cursor, straight to the
    // inner transport, still delivered live. This runs before the indexed-log
    // branches because such topics are explicitly declared non-replayable.
    if (this.shouldCache?.(topic) === false) {
      await this.inner.publish(topic, event, options);
      return;
    }

    // `localOnly` events are scoped to the publishing instance. Do not cache
    // them: a cached copy would be replayed to later subscribers (including
    // ones on other processes for shared caches), violating the locality
    // contract. Forward straight to the inner transport instead.
    if (options?.localOnly && !this.indexedLog) {
      await this.inner.publish(topic, event, options);
      return;
    }

    if (
      options?.localOnly &&
      this.indexedLog?.indexedLogScope === 'durable' &&
      this.durableLocalOnly === 'passthrough'
    ) {
      await this.inner.publish(topic, event, options);
      return;
    }

    if (options?.localOnly && this.indexedLog?.indexedLogScope === 'durable') {
      throw new Error(
        'CachingPubSub cannot provide durable exact replay for a localOnly event; use a process-scoped indexed cache, a separate topic, or explicitly configure durableLocalOnly passthrough for non-replayable topics',
      );
    }

    if (this.indexedLog && this.indexedLogRetention) {
      const { index: _callerIndex, logGeneration: _callerLogGeneration, ...eventWithoutTransportPosition } = event;
      const identifiedEvent = this.retainedEvent({
        ...eventWithoutTransportPosition,
        id: event.id ?? crypto.randomUUID(),
        createdAt: event.createdAt ?? new Date(),
      });
      // Cursor allocation and retention are one cache operation. Failure is
      // terminal for this publish: emitting live without retaining would make
      // an exact subscriber silently skip an event after restart.
      const retained = await this.indexedLog.appendIndexedLogEntry(
        this.getIndexedLogKey(topic),
        identifiedEvent,
        this.indexedLogRetention,
      );
      const fullEvent = this.retainedEvent({
        ...retained.value,
        index: retained.cursor,
        logGeneration: retained.logGeneration,
      });
      await this.inner.publish(topic, fullEvent, options);
      return;
    }

    const cacheKey = this.getCacheKey(topic);
    const counterKey = this.getCounterKey(topic);

    // The cache owns the topic cursor. Preserve upstream identity, but never a
    // caller-supplied index that could collide with the atomic counter.
    const { index: _callerIndex, ...eventWithoutIndex } = event;
    const { index: _retainedIndex, ...baseEvent } = this.retainedEvent({
      ...eventWithoutIndex,
      id: event.id ?? crypto.randomUUID(),
      createdAt: event.createdAt ?? new Date(),
    });

    // Cache BEFORE live publish so late-joining observers never miss events.
    // Index allocation and the list append happen in one cache operation so
    // network-backed caches can do it in a single round-trip (issue #22477).
    let index: number | undefined;
    try {
      index = await this.cache.listPushIndexed(cacheKey, counterKey, baseEvent);
    } catch (error) {
      this.logError(`[CachingPubSub] Failed to cache event for ${topic}`, error);
    }

    // On cache failure leave `index` undefined rather than defaulting to 0:
    // downstream consumers that key off `index` (e.g. replay-from-offset)
    // would otherwise see colliding indices across failed publishes.
    const fullEvent = this.retainedEvent({
      ...baseEvent,
      ...(index !== undefined ? { index } : {}),
    });

    // Always publish to inner PubSub — cache failure must not block live delivery
    await this.inner.publish(topic, fullEvent, options);
  }

  /**
   * Subscribe to live events on a topic (no replay).
   */
  async subscribe(topic: string, cb: EventCallback, options?: SubscribeOptions): Promise<void> {
    await this.retainFollower(topic);
    await this.inner.subscribe(topic, cb, options);
  }

  /**
   * Subscribe to a topic with automatic replay of cached events.
   * Delegates to {@link subscribeFromOffset} with offset 0.
   */
  async subscribeWithReplay(topic: string, cb: EventCallback): Promise<void> {
    return this.subscribeFromOffset(topic, 0, cb);
  }

  /**
   * Subscribe to a topic with replay starting from a specific index.
   * More efficient than full replay when the client knows their last position.
   *
   * Order of operations:
   * 1. Subscribe to live events FIRST — buffer deliveries during bootstrap
   * 2. Fetch and deliver cached history in order
   * 3. Drain the buffer, skipping events already delivered via history
   * 4. Switch to passthrough with an index watermark for steady-state dedup
   *
   * @param topic - The topic to subscribe to
   * @param offset - Start replaying from this index (0-based)
   * @param cb - Callback invoked for each event
   */
  async subscribeFromOffset(
    topic: string,
    offset: number,
    cb: EventCallback,
    options?: IndexedReplaySubscribeOptions,
  ): Promise<void> {
    // Follow the source (if any) before attaching locally: events the source
    // delivers between now and the live subscription below land in the cache
    // and are picked up by the history fetch.
    await this.retainFollower(topic);
    if (this.indexedLog && this.indexedLogRetention) {
      return this.subscribeFromExactOffset(topic, offset, cb, options);
    }
    return this.subscribeFromBestEffortOffset(topic, offset, cb);
  }

  private async subscribeFromBestEffortOffset(topic: string, offset: number, cb: EventCallback): Promise<void> {
    // --- Phase 1: subscribe live, buffer everything during bootstrap ---
    let bootstrapping = true;
    const buffer: Array<{
      event: Event;
      ack?: Parameters<EventCallback>[1];
      nack?: Parameters<EventCallback>[2];
    }> = [];
    let lastDelivered = -1;

    const wrappedCb: EventCallback = async (event, ack, nack) => {
      // Drop events strictly before the requested offset on the live path.
      // Dropped deliveries are still acknowledged: the consumer will never see
      // them, so leaving them unacknowledged would strand them on the backend.
      if (typeof event.index === 'number' && event.index < offset) {
        await ack?.();
        return;
      }

      if (bootstrapping) {
        buffer.push({ event, ack, nack });
        return;
      }

      // Steady-state: skip events we already delivered via history or buffer drain.
      // Allow nack-redelivered messages through — they carry the same index but
      // deliveryAttempt > 1, and the consumer must see them to retry processing.
      const isRetry = typeof event.deliveryAttempt === 'number' && event.deliveryAttempt > 1;
      if (typeof event.index === 'number' && event.index <= lastDelivered && !isRetry) {
        await ack?.();
        return;
      }

      if (typeof event.index === 'number' && event.index > lastDelivered) {
        lastDelivered = event.index;
      }
      await cb(event, ack, nack);
    };

    this.callbackMap.set(cb, wrappedCb);
    await this.inner.subscribe(topic, wrappedCb);

    try {
      // --- Phase 2: fetch and deliver cached history ---
      // `seen` records the index-based dedup key AND the event id. The id
      // matters on an aliased source bus: the follower caches an indexed COPY
      // of an event whose original live delivery carries no index, so the two
      // only correlate by id (same emitter delivery → same id). Distinct
      // events never share ids, so the extra key can't over-suppress.
      const seen = new Set<string>();
      const history = await this.getHistory(topic, offset);
      for (const event of history) {
        seen.add(this.dedupKey(event));
        if (event.id) seen.add(`id:${event.id}`);
        if (typeof event.index === 'number') {
          lastDelivered = event.index;
        }
        await cb(event);
      }

      // --- Phase 3: drain buffer, suppressing duplicates history already covered ---
      for (const { event, ack, nack } of buffer) {
        if (seen.has(this.dedupKey(event)) || (event.id && seen.has(`id:${event.id}`))) {
          // Acknowledge the suppressed duplicate so it doesn't stay pending.
          await ack?.();
          continue;
        }
        seen.add(this.dedupKey(event));
        if (event.id) seen.add(`id:${event.id}`);
        if (typeof event.index === 'number') {
          lastDelivered = event.index;
        }
        await cb(event, ack, nack);
      }

      // --- Phase 4: flip to passthrough ---
      bootstrapping = false;
      buffer.length = 0;
    } catch (error) {
      // Rollback: unsubscribe wrappedCb so it doesn't strand in bootstrap mode
      this.callbackMap.delete(cb);
      await this.inner.unsubscribe(topic, wrappedCb).catch(() => {});
      await this.releaseFollower(topic);
      throw error;
    }
  }

  private async subscribeFromExactOffset(
    topic: string,
    offset: number,
    cb: EventCallback,
    options?: IndexedReplaySubscribeOptions,
  ): Promise<void> {
    type PendingDelivery = {
      event: Event;
      ack?: Parameters<EventCallback>[1];
      nack?: Parameters<EventCallback>[2];
    };

    let bootstrapping = true;
    let nextCursor = offset;
    let expectedLogGeneration = options?.logGeneration;
    let terminalError: Error | undefined;
    let terminalQueued = false;
    let deliveryChain = Promise.resolve();
    const pending = new Map<number, PendingDelivery[]>();

    const rangeFor = async (afterCursor: number) => {
      const result = await this.indexedLog!.readIndexedLogEntries<Event>(
        this.getIndexedLogKey(topic),
        afterCursor,
        this.indexedLogRetention!,
      );
      const capability = this.indexedReplay!;
      const range: IndexedReplayRange = {
        ...capability,
        logGeneration: result.logGeneration,
        firstCursor: result.firstCursor,
        nextCursor: result.nextCursor,
      };
      return { result, range };
    };

    const rejectUnavailableCursor = (range: IndexedReplayRange) => {
      if (expectedLogGeneration === undefined) {
        // Even a caller that starts from the current head must remain bound to
        // that retained-log generation. Otherwise a quiet topic could expire,
        // restart at cursor zero, and have its new events mistaken for old
        // duplicates by this still-live subscription.
        expectedLogGeneration = range.logGeneration;
      } else if (expectedLogGeneration !== range.logGeneration) {
        throw new IndexedReplayCursorError('generation-mismatch', nextCursor, range, expectedLogGeneration);
      }
      if (nextCursor < range.firstCursor) {
        throw new IndexedReplayCursorError('cursor-too-old', nextCursor, range);
      }
      if (nextCursor > range.nextCursor) {
        throw new IndexedReplayCursorError('cursor-ahead', nextCursor, range);
      }
    };

    const acknowledgeCommitted = async () => {
      for (const [cursor, deliveries] of pending) {
        if (cursor >= nextCursor) continue;
        pending.delete(cursor);
        await Promise.all(deliveries.map(delivery => delivery.ack?.()));
      }
    };

    const deliverCanonical = async (canonical: Event, deliveries: PendingDelivery[]) => {
      const mismatched = deliveries.find(delivery => delivery.event.id !== canonical.id);
      if (mismatched) {
        await mismatched.nack?.();
        throw new IndexedReplayIntegrityError(
          'identity-mismatch',
          `Indexed replay identity mismatch at cursor ${canonical.index}: retained ${canonical.id}, live ${mismatched.event.id}`,
        );
      }

      const selected = deliveries.shift();
      let outcome: 'ack' | 'nack' | undefined;
      const ack = selected?.ack
        ? async () => {
            await selected.ack!();
            outcome = 'ack';
          }
        : undefined;
      const nack = selected?.nack
        ? async () => {
            await selected.nack!();
            outcome = 'nack';
          }
        : undefined;
      const deliveredEvent = selected?.event.deliveryAttempt
        ? { ...canonical, deliveryAttempt: selected.event.deliveryAttempt }
        : canonical;

      await cb(deliveredEvent, ack, nack);
      if (outcome === 'nack') {
        throw new Error(`Indexed replay delivery at cursor ${canonical.index} was negatively acknowledged`);
      }
      if (selected && (selected.ack || selected.nack) && outcome !== 'ack') {
        throw new Error(`Indexed replay delivery at cursor ${canonical.index} completed without acknowledgement`);
      }

      await Promise.all(deliveries.map(delivery => delivery.ack?.()));
    };

    const drainRetained = async () => {
      if (terminalError) throw terminalError;
      // One drain catches up to one fixed retained head. Newer publishes are
      // buffered by the live subscription and schedule a later drain, so a
      // continuously written topic cannot keep bootstrap open forever.
      let targetNextCursor: number | undefined;
      while (true) {
        const { result, range } = await rangeFor(nextCursor - 1);
        rejectUnavailableCursor(range);
        targetNextCursor ??= range.nextCursor;

        const entries = [...result.entries]
          .filter(entry => entry.cursor < targetNextCursor!)
          .sort((left, right) => left.cursor - right.cursor);
        const cursorBeforePage = nextCursor;
        for (const entry of entries) {
          if (entry.cursor < nextCursor) continue;
          if (entry.cursor !== nextCursor) {
            throw new IndexedReplayIntegrityError(
              'cursor-gap',
              `Indexed replay log has a cursor gap: expected ${nextCursor}, received ${entry.cursor}`,
            );
          }

          const canonical = this.retainedEvent({
            ...entry.value,
            index: entry.cursor,
            logGeneration: range.logGeneration,
          });
          const deliveries = pending.get(entry.cursor) ?? [];
          pending.delete(entry.cursor);
          try {
            await deliverCanonical(canonical, deliveries);
          } catch (error) {
            if (deliveries.length > 0) pending.set(entry.cursor, deliveries);
            throw error;
          }
          nextCursor += 1;
          await acknowledgeCommitted();
        }

        // Live duplicates can arrive after their retained counterpart commits.
        // Validate the generation above before acknowledging them: a lower
        // cursor from a recreated log is not an old duplicate.
        await acknowledgeCommitted();

        // A backend may page retained reads to stay under transaction limits.
        // The fixed target is the first page's full log head, not merely the
        // end of that page.
        if (nextCursor < targetNextCursor) {
          if (nextCursor === cursorBeforePage) {
            throw new IndexedReplayIntegrityError(
              'cursor-gap',
              `Indexed replay log did not return retained cursor ${nextCursor} before head ${targetNextCursor}`,
            );
          }
          continue;
        }

        const lowestPending = Math.min(...pending.keys());
        if (!Number.isFinite(lowestPending) || lowestPending < nextCursor) return;

        // During initial bootstrap, the mandatory post-bootstrap drain below
        // owns live events that arrived beyond the captured head.
        if (bootstrapping) return;

        // A live publish can append while this drain is awaiting the previous
        // callback. Re-read before declaring the live cursor absent: the range
        // above is a valid but stale snapshot from before that append.
        const { range: refreshedRange } = await rangeFor(nextCursor - 1);
        rejectUnavailableCursor(refreshedRange);
        if (refreshedRange.nextCursor > nextCursor) {
          // Catch up only far enough to reconcile the lowest live delivery;
          // do not chase an independently moving retained head.
          targetNextCursor = Math.min(refreshedRange.nextCursor, lowestPending + 1);
          if (targetNextCursor > nextCursor) continue;
        }

        throw new IndexedReplayIntegrityError(
          'live-event-not-retained',
          `Live indexed event ${lowestPending} is absent from retained history at cursor ${nextCursor}`,
        );
      }
    };

    const isTerminalReplayError = (error: unknown): error is Error =>
      error instanceof IndexedReplayCursorError || error instanceof IndexedReplayIntegrityError;

    let wrappedCb: EventCallback;
    const reportTerminalError = async (error: Error) => {
      if (terminalError) return;
      terminalError = error;
      try {
        // A generation or integrity fence cannot recover within this
        // subscription. Stop broker delivery before notifying the observer so
        // a slow onError handler cannot leave a poison record redelivering.
        await this.inner.unsubscribe(topic, wrappedCb);
      } catch (unsubscribeError) {
        this.logError(
          `[CachingPubSub] Failed to stop terminal indexed replay subscription for ${topic}`,
          unsubscribeError,
        );
      }
      // The subscription is stopped. Remove its public drain handles before
      // invoking onError so an observer may safely call flush()/unwatch()
      // without waiting on the delivery chain that is currently awaiting it.
      this.callbackMap.delete(cb);
      this.subscriptionDrains.delete(cb);
      try {
        await options?.onError?.(error);
      } catch (observerError) {
        this.logError(`[CachingPubSub] Indexed replay error observer failed for ${topic}`, observerError);
      }
    };

    const enqueueDrain = (reportActiveFailure = false) => {
      const delivery = deliveryChain.then(drainRetained);
      const reportedDelivery = reportActiveFailure
        ? delivery.catch(async error => {
            if (isTerminalReplayError(error)) {
              await reportTerminalError(error);
            }
            throw error;
          })
        : delivery;
      // Flush/unsubscribe must wait through terminal broker teardown and the
      // onError observer, not merely the raw retained read.
      deliveryChain = reportedDelivery.catch(() => {});
      return reportedDelivery;
    };

    wrappedCb = (event, ack, nack) => {
      if (terminalError || terminalQueued) {
        const terminalDrain = deliveryChain;
        const failure = async () => {
          await nack?.();
          await terminalDrain;
          throw terminalError ?? new Error(`Indexed replay subscription for ${topic} is stopping`);
        };
        return failure();
      }
      if (!Number.isSafeInteger(event.index) || event.index! < 0) {
        terminalQueued = true;
        const error = new IndexedReplayIntegrityError(
          'invalid-live-cursor',
          `Exact indexed replay received an event without a valid cursor: ${event.id}`,
        );
        const negativeAck = Promise.resolve().then(() => nack?.());
        const failure = deliveryChain.then(async () => {
          await negativeAck;
          await reportTerminalError(error);
          throw error;
        });
        deliveryChain = failure.catch(() => {});
        return failure;
      }

      const deliveries = pending.get(event.index!) ?? [];
      deliveries.push({ event, ack, nack });
      pending.set(event.index!, deliveries);
      if (bootstrapping) return;
      return enqueueDrain(true);
    };

    this.callbackMap.set(cb, wrappedCb);
    this.subscriptionDrains.set(cb, () => deliveryChain);

    try {
      await this.inner.subscribe(topic, wrappedCb);
      await drainRetained();
      bootstrapping = false;
      if (pending.size > 0) await enqueueDrain();
    } catch (error) {
      this.callbackMap.delete(cb);
      this.subscriptionDrains.delete(cb);
      await this.inner.unsubscribe(topic, wrappedCb).catch(() => {});
      // The shared subscribeFromOffset wrapper retained a follower before
      // dispatching here; a failed exact-offset bootstrap must release that
      // ref, mirroring the best-effort path's rollback.
      await this.releaseFollower(topic);
      throw error;
    }
  }

  /**
   * Unsubscribe from a topic.
   */
  async unsubscribe(topic: string, cb: EventCallback): Promise<void> {
    const wrappedCb = this.callbackMap.get(cb) ?? cb;
    const drain = this.subscriptionDrains.get(cb);
    this.callbackMap.delete(cb);
    this.subscriptionDrains.delete(cb);
    await this.inner.unsubscribe(topic, wrappedCb);
    await drain?.();
    await this.releaseFollower(topic);
  }

  /**
   * Get historical events for a topic from cache.
   */
  async getHistory(topic: string, offset: number = 0): Promise<Event[]> {
    if (this.indexedLog && this.indexedLogRetention) {
      const history: Event[] = [];
      let afterCursor = offset - 1;
      let logGeneration: string | undefined;
      let firstPage = true;
      let targetNextCursor: number | undefined;

      while (true) {
        const result = await this.indexedLog.readIndexedLogEntries<Event>(
          this.getIndexedLogKey(topic),
          afterCursor,
          this.indexedLogRetention,
        );
        const range: IndexedReplayRange = {
          ...this.indexedReplay!,
          logGeneration: result.logGeneration,
          firstCursor: result.firstCursor,
          nextCursor: result.nextCursor,
        };
        if (logGeneration !== undefined && logGeneration !== result.logGeneration) {
          throw new IndexedReplayCursorError('generation-mismatch', afterCursor + 1, range, logGeneration);
        }
        logGeneration ??= result.logGeneration;
        targetNextCursor ??= result.nextCursor;

        // Historical reads are best effort at their initial offset, matching
        // the legacy behavior of returning the retained suffix. Once paging
        // starts, however, retention must not skip a page under this reader.
        if (afterCursor < result.firstCursor - 1) {
          if (!firstPage) throw new IndexedReplayCursorError('cursor-too-old', afterCursor + 1, range);
          afterCursor = result.firstCursor - 1;
        }
        firstPage = false;

        const entries = [...result.entries]
          .filter(entry => entry.cursor > afterCursor && entry.cursor < targetNextCursor!)
          .sort((left, right) => left.cursor - right.cursor);
        for (const entry of entries) {
          if (entry.cursor !== afterCursor + 1) {
            throw new IndexedReplayIntegrityError(
              'cursor-gap',
              `Indexed replay log has a cursor gap: expected ${afterCursor + 1}, received ${entry.cursor}`,
            );
          }
          history.push(
            this.retainedEvent({
              ...entry.value,
              index: entry.cursor,
              logGeneration: result.logGeneration,
            }),
          );
          afterCursor = entry.cursor;
        }

        if (afterCursor + 1 >= targetNextCursor) return history;
        if (entries.length === 0) {
          throw new IndexedReplayIntegrityError(
            'cursor-gap',
            `Indexed replay log did not return retained cursor ${afterCursor + 1} before head ${targetNextCursor}`,
          );
        }
      }
    }
    const cacheKey = this.getCacheKey(topic);
    const events = await this.cache.listFromTo(cacheKey, offset);
    return events.map(event => this.retainedEvent(event));
  }

  override async getIndexedReplayRange(topic: string): Promise<IndexedReplayRange | undefined> {
    if (!this.indexedLog || !this.indexedLogRetention || !this.indexedReplay) return undefined;
    const result = await this.indexedLog.readIndexedLogEntries<Event>(
      this.getIndexedLogKey(topic),
      Number.MAX_SAFE_INTEGER,
      this.indexedLogRetention,
    );
    return {
      ...this.indexedReplay,
      logGeneration: result.logGeneration,
      firstCursor: result.firstCursor,
      nextCursor: result.nextCursor,
    };
  }

  /**
   * Flush any pending operations on the inner PubSub.
   */
  async flush(): Promise<void> {
    while (true) {
      await this.inner.flush();
      const before = new Map([...this.subscriptionDrains].map(([callback, drain]) => [callback, drain()]));
      await Promise.all(before.values());

      // A callback can nack while the first inner flush is already complete.
      // Give the broker a second chance to materialize that redelivery, then
      // compare the exact subscription-chain promises to find a stable point.
      await this.inner.flush();
      const after = new Map([...this.subscriptionDrains].map(([callback, drain]) => [callback, drain()]));
      if (before.size === after.size && [...before].every(([callback, delivery]) => after.get(callback) === delivery)) {
        return;
      }
    }
  }

  /**
   * Expose the inner's {@link LeaseProvider} when it has one, otherwise
   * `undefined`. Leasing is a capability of the underlying backend
   * (e.g. Redis), not of the caching decorator itself — so rather than
   * unconditionally declaring lease methods (which would make
   * {@link isLeaseProvider} report `true` even when the inner can't
   * coordinate a lock), we surface the inner's capability directly. The
   * signals runtime unwraps this so wrapping with caching preserves real
   * distributed lease semantics without faking them.
   */
  getLeaseProvider(): LeaseProvider | undefined {
    return isLeaseProvider(this.inner) ? this.inner : undefined;
  }

  /**
   * Clear cached events for a specific topic (and the index counter), and
   * forward the clear to the inner transport.
   *
   * Call this when a stream completes to free memory. The forward matters for
   * persistent inner transports (e.g. Redis Streams): without it, wrapping a
   * pubsub in `CachingPubSub` silently turns `clearTopic` into a cache-only
   * no-op and the inner stream leaks forever.
   */
  override async clearTopicOrThrow(topic: string): Promise<void> {
    const cacheKey = this.getCacheKey(topic);
    const counterKey = this.getCounterKey(topic);
    await Promise.all([
      this.cache.delete(cacheKey),
      this.cache.delete(counterKey),
      this.indexedLog?.deleteIndexedLog(this.getIndexedLogKey(topic)),
      this.inner.clearTopicOrThrow(topic),
    ]);
  }

  override async clearTopic(topic: string): Promise<void> {
    try {
      // Stop mirroring the source first — a follower running past the clear
      // would repopulate the cache the caller is trying to free.
      await this.stopFollowing(topic);
      await this.clearTopicOrThrow(topic);
    } catch (error) {
      // Honor the base-class contract: clearTopic is best-effort and callers
      // invoke it fire-and-forget, so a cache failure must not become an
      // unhandled rejection. A failed delete means retained state may leak
      // until the transport-level TTL backstop, so make it visible.
      this.logError(`[CachingPubSub] Failed to clear topic ${topic}`, error);
    }
  }

  /** Forward run trims to the inner transport; the per-process cache is left to its own bounds. */
  override async trimTopic(topic: string, options: { runId: string; producedBefore?: number }): Promise<void> {
    try {
      await this.inner.trimTopic(topic, options);
    } catch (error) {
      this.logError(`[CachingPubSub] Failed to trim topic ${topic}`, error);
    }
  }

  /**
   * Get the inner PubSub instance.
   * Useful for accessing implementation-specific methods like close().
   */
  getInner(): PubSub {
    return this.inner;
  }
}

/**
 * Factory function to wrap a PubSub with caching capabilities.
 *
 * @example
 * ```typescript
 * import { withCaching, EventEmitterPubSub } from '@mastra/core/events';
 * import { InMemoryServerCache } from '@mastra/core/cache';
 *
 * const cache = new InMemoryServerCache();
 * const pubsub = withCaching(new EventEmitterPubSub(), cache);
 * ```
 */
export function withCaching(pubsub: PubSub, cache: MastraServerCache, options?: CachingPubSubOptions): CachingPubSub {
  return new CachingPubSub(pubsub, cache, options);
}
