import { fork } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { MockLanguageModelV1 } from '@internal/ai-sdk-v4/test';
import { MockLanguageModelV2, convertArrayToReadableStream } from '@internal/ai-sdk-v5/test';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';

import { EventEmitterPubSub } from '../../events/event-emitter';
import { PubSub } from '../../events/pubsub';
import type { LeaseProvider } from '../../events/pubsub';
import type { EventCallback, SubscribeOptions } from '../../events/types';
import { UnixSocketPubSub } from '../../events/unix-socket-pubsub';
import { buildFakeOutput } from '../../harness/v1/__test-utils__/fake-output';
import { Mastra } from '../../mastra';
import { MockMemory } from '../../memory/mock';
import { MAX_NOTIFICATION_DELIVERY_ATTEMPTS } from '../../notifications/delivery-policy';
import { dispatchDueNotifications } from '../../notifications/dispatcher';
import { InMemoryNotificationsStorage } from '../../notifications/storage';
import { createNotificationInboxTool } from '../../notifications/tool';
import { MASTRA_RESOURCE_ID_KEY, MASTRA_THREAD_ID_KEY, RequestContext } from '../../request-context';
import { MastraCompositeStore } from '../../storage/base';
import { Agent } from '../agent';
import {
  createMessageSignal,
  createSignal,
  dataPartToSignal,
  mastraDBMessageToSignal,
  resolveDeliveryAttributes,
  signalToDataPartFormat,
  signalToMastraDBMessage,
} from '../signals';
import { AgentThreadStreamRuntime, agentThreadStreamRuntime } from '../thread-stream-runtime';

// Runtime source IDs are allocated once at module load while per-test UUIDs use
// a reset deterministic counter, so use native UUIDs to keep multiple runtimes distinct.
vi.unmock('crypto');
vi.unmock('node:crypto');

function decodeLeaseOwnerRunId(owner: string | undefined): string | undefined {
  if (owner === undefined || !owner.startsWith('mastra-thread-owner:')) return owner;
  try {
    const decoded = JSON.parse(owner.slice('mastra-thread-owner:'.length));
    return Array.isArray(decoded) && typeof decoded[0] === 'string' ? decoded[0] : owner;
  } catch {
    return owner;
  }
}

function createTextStreamModel(responseText: string) {
  return new MockLanguageModelV2({
    doStream: async () => ({
      rawCall: { rawPrompt: null, rawSettings: {} },
      warnings: [],
      stream: convertArrayToReadableStream([
        { type: 'stream-start', warnings: [] },
        { type: 'response-metadata', id: 'id-0', modelId: 'mock-model-id', timestamp: new Date(0) },
        { type: 'text-start', id: 'text-1' },
        { type: 'text-delta', id: 'text-1', delta: responseText },
        { type: 'text-end', id: 'text-1' },
        {
          type: 'finish',
          finishReason: 'stop',
          usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
        },
      ]),
    }),
  });
}

function createBlockingFirstTextStreamModel(firstResponseText: string, laterResponseText: string) {
  let releaseFirst!: () => void;
  const firstFinished = new Promise<void>(resolve => {
    releaseFirst = resolve;
  });
  let streamCount = 0;
  const model = new MockLanguageModelV2({
    doStream: async () => {
      streamCount += 1;
      const currentStreamCount = streamCount;
      const responseText = currentStreamCount === 1 ? firstResponseText : laterResponseText;
      return {
        rawCall: { rawPrompt: null, rawSettings: {} },
        warnings: [],
        stream: new ReadableStream({
          async start(controller) {
            controller.enqueue({ type: 'stream-start', warnings: [] });
            controller.enqueue({
              type: 'response-metadata',
              id: `blocking-stream-${currentStreamCount}`,
              modelId: 'mock-model-id',
              timestamp: new Date(0),
            });
            controller.enqueue({ type: 'text-start', id: 'text-1' });
            controller.enqueue({ type: 'text-delta', id: 'text-1', delta: responseText });
            controller.enqueue({ type: 'text-end', id: 'text-1' });
            if (currentStreamCount === 1) {
              await firstFinished;
            }
            controller.enqueue({
              type: 'finish',
              finishReason: 'stop',
              usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
            });
            controller.close();
          },
        }),
      };
    },
  });

  return { model, releaseFirst, getStreamCount: () => streamCount };
}

function nextTick() {
  return new Promise(resolve => setTimeout(resolve, 0));
}

class AsyncFanoutPubSub extends PubSub {
  #inner = new EventEmitterPubSub();

  async publish(topic: string, event: Parameters<PubSub['publish']>[1]): Promise<void> {
    await nextTick();
    await this.#inner.publish(topic, event);
  }

  async subscribe(topic: string, cb: EventCallback, options?: SubscribeOptions): Promise<void> {
    await this.#inner.subscribe(topic, cb, options);
  }

  async unsubscribe(topic: string, cb: EventCallback): Promise<void> {
    await this.#inner.unsubscribe(topic, cb);
  }

  async flush(): Promise<void> {
    await this.#inner.flush();
  }
}

class AsyncCallbackPubSub extends PubSub {
  #subscribers = new Map<string, Set<EventCallback>>();
  #index = 0;
  #pending = new Set<Promise<void>>();
  /** Callback rejections, which a real backend turns into a nack and redelivery. */
  subscriptionFailures: unknown[] = [];

  async publish(topic: string, event: any, _options?: { localOnly?: boolean }): Promise<void> {
    const subscribers = [...(this.#subscribers.get(topic) ?? [])];
    const envelope = {
      ...event,
      id: `event-${this.#index}`,
      createdAt: new Date(),
      index: this.#index++,
    };
    const pending = new Promise<void>(resolve => {
      setTimeout(() => {
        try {
          for (const subscriber of subscribers) {
            void Promise.resolve(subscriber(envelope)).catch(error => this.subscriptionFailures.push(error));
          }
        } finally {
          resolve();
        }
      }, 0);
    });
    this.#pending.add(pending);
    pending.finally(() => this.#pending.delete(pending));
  }

  async subscribe(topic: string, cb: EventCallback): Promise<void> {
    const subscribers = this.#subscribers.get(topic) ?? new Set<EventCallback>();
    subscribers.add(cb);
    this.#subscribers.set(topic, subscribers);
  }

  async unsubscribe(topic: string, cb: EventCallback): Promise<void> {
    this.#subscribers.get(topic)?.delete(cb);
  }

  async flush(): Promise<void> {
    await Promise.all([...this.#pending]);
  }
}

class RetainedAsyncCallbackPubSub extends PubSub {
  #subscribers = new Map<string, Set<EventCallback>>();

  subscriberCount(topic: string) {
    return this.#subscribers.get(topic)?.size ?? 0;
  }
  #history = new Map<string, any[]>();
  #pending = new Set<Promise<void>>();
  #index = 0;
  /** Callback rejections, which a real backend turns into a nack and redelivery. */
  subscriptionFailures: unknown[] = [];

  async publish(topic: string, event: any): Promise<void> {
    const envelope = { ...event, id: `retained-${this.#index}`, createdAt: new Date(), index: this.#index++ };
    const history = this.#history.get(topic) ?? [];
    history.push(envelope);
    this.#history.set(topic, history);
    const subscribers = [...(this.#subscribers.get(topic) ?? [])];
    const pending = new Promise<void>(resolve => {
      setTimeout(() => {
        for (const subscriber of subscribers) {
          void Promise.resolve(subscriber(envelope)).catch(error => this.subscriptionFailures.push(error));
        }
        resolve();
      }, 0);
    });
    this.#pending.add(pending);
    pending.finally(() => this.#pending.delete(pending));
  }

  async subscribe(topic: string, cb: EventCallback): Promise<void> {
    const subscribers = this.#subscribers.get(topic) ?? new Set<EventCallback>();
    subscribers.add(cb);
    this.#subscribers.set(topic, subscribers);
    for (const event of this.#history.get(topic) ?? []) {
      void Promise.resolve(cb(event)).catch(error => this.subscriptionFailures.push(error));
    }
  }

  async unsubscribe(topic: string, cb: EventCallback): Promise<void> {
    this.#subscribers.get(topic)?.delete(cb);
  }

  async flush(): Promise<void> {
    await Promise.all([...this.#pending]);
  }
}

class DelayedRegistrationPubSub extends RetainedAsyncCallbackPubSub {
  readonly published: Array<{ topic: string; event: any }> = [];
  #activeSubscriptions = new Map<string, Set<EventCallback>>();
  #nextDelayedSubscription:
    | {
        matches: (topic: string) => boolean;
        gate: Promise<void>;
        markStarted: () => void;
        markRegistered: () => void;
        barrier: { started: Promise<void>; registered: Promise<void>; release: () => void; topic?: string };
      }
    | undefined;

  delayNextSubscription(matches: (topic: string) => boolean) {
    let release!: () => void;
    let markStarted!: () => void;
    let markRegistered!: () => void;
    const barrier = {
      started: new Promise<void>(resolve => {
        markStarted = resolve;
      }),
      registered: new Promise<void>(resolve => {
        markRegistered = resolve;
      }),
      release: () => release(),
      topic: undefined as string | undefined,
    };
    const gate = new Promise<void>(resolve => {
      release = resolve;
    });
    this.#nextDelayedSubscription = { matches, gate, markStarted, markRegistered, barrier };
    return barrier;
  }

  activeSubscriptionCount(topic: string): number {
    return this.#activeSubscriptions.get(topic)?.size ?? 0;
  }

  override async publish(topic: string, event: any): Promise<void> {
    this.published.push({ topic, event });
    await super.publish(topic, event);
  }

  override async subscribe(topic: string, cb: EventCallback, options?: SubscribeOptions): Promise<void> {
    const delayed = this.#nextDelayedSubscription;
    if (delayed?.matches(topic)) {
      this.#nextDelayedSubscription = undefined;
      delayed.barrier.topic = topic;
      delayed.markStarted();
      await delayed.gate;
      try {
        await super.subscribe(topic, cb, options);
        const callbacks = this.#activeSubscriptions.get(topic) ?? new Set<EventCallback>();
        callbacks.add(cb);
        this.#activeSubscriptions.set(topic, callbacks);
      } finally {
        delayed.markRegistered();
      }
      return;
    }

    await super.subscribe(topic, cb, options);
    const callbacks = this.#activeSubscriptions.get(topic) ?? new Set<EventCallback>();
    callbacks.add(cb);
    this.#activeSubscriptions.set(topic, callbacks);
  }

  override async unsubscribe(topic: string, cb: EventCallback): Promise<void> {
    this.#activeSubscriptions.get(topic)?.delete(cb);
    await super.unsubscribe(topic, cb);
  }
}

/** Delivers each publication to subscribers in REVERSE registration order. */
class ReverseDeliveryPubSub extends RetainedAsyncCallbackPubSub {
  #subscribers = new Map<string, EventCallback[]>();
  #history = new Map<string, any[]>();
  #pending = new Set<Promise<void>>();
  #index = 0;

  async publish(topic: string, event: any): Promise<void> {
    const envelope = { ...event, id: `reverse-${this.#index}`, createdAt: new Date(), index: this.#index++ };
    const history = this.#history.get(topic) ?? [];
    history.push(envelope);
    this.#history.set(topic, history);
    const subscribers = [...(this.#subscribers.get(topic) ?? [])].reverse();
    const pending = new Promise<void>(resolve => {
      setTimeout(() => {
        for (const subscriber of subscribers) {
          void Promise.resolve(subscriber(envelope)).catch(() => {});
        }
        this.#pending.delete(pending);
        resolve();
      }, 0);
    });
    this.#pending.add(pending);
  }

  async subscribe(topic: string, cb: EventCallback): Promise<void> {
    const subscribers = this.#subscribers.get(topic) ?? [];
    subscribers.push(cb);
    this.#subscribers.set(topic, subscribers);
    for (const event of this.#history.get(topic) ?? []) {
      await Promise.resolve(cb(event)).catch(() => {});
    }
  }

  async unsubscribe(topic: string, cb: EventCallback): Promise<void> {
    const subscribers = this.#subscribers.get(topic) ?? [];
    const index = subscribers.indexOf(cb);
    if (index !== -1) subscribers.splice(index, 1);
  }

  async flush(): Promise<void> {
    await Promise.all([...this.#pending]);
  }
}

/** Holds publication of specific event data types behind a caller-owned gate. */
class GatedTypePubSub extends RetainedAsyncCallbackPubSub implements LeaseProvider {
  owners = new Map<string, string>();
  publishedData: any[] = [];
  #gates = new Map<string, Promise<void>>();

  gateType(type: string): { release: () => void } {
    let release!: () => void;
    const gate = new Promise<void>(resolve => {
      release = resolve;
    });
    this.#gates.set(type, gate);
    return { release };
  }

  override async publish(topic: string, event: any): Promise<void> {
    this.publishedData.push(event.data);
    const gate = this.#gates.get(event.data?.type);
    if (gate) await gate;
    await super.publish(topic, event);
  }

  async acquireLease(key: string, owner: string): Promise<{ acquired: boolean; owner?: string }> {
    const current = this.owners.get(key);
    if (current && current !== owner) return { acquired: false, owner: current };
    this.owners.set(key, owner);
    return { acquired: true, owner };
  }

  async getLeaseOwner(key: string): Promise<string | undefined> {
    return this.owners.get(key);
  }

  async releaseLease(key: string, owner: string): Promise<void> {
    if (this.owners.get(key) === owner) this.owners.delete(key);
  }

  async renewLease(key: string, owner: string): Promise<boolean> {
    return this.owners.get(key) === owner;
  }

  async transferLease(key: string, fromOwner: string, toOwner: string): Promise<boolean> {
    if (this.owners.get(key) !== fromOwner) return false;
    this.owners.set(key, toOwner);
    return true;
  }
}

/**
 * A gated pubsub whose held publications REJECT after delivery once released:
 * the delivery itself lands (observers see it) and only then does the bounded
 * terminal publication fail, exercising the failure disposition of a fenced
 * in-flight terminal.
 */
class GatedRejectingTypePubSub extends GatedTypePubSub {
  rejectOnRelease = new Set<string>();

  override async publish(topic: string, event: any): Promise<void> {
    await super.publish(topic, event);
    if (this.rejectOnRelease.has(event.data?.type)) {
      throw new Error(`publish rejected after delivery: ${event.data.type}`);
    }
  }
}

/**
 * A gated pubsub whose held publications REJECT before any delivery once
 * released: the event never reaches subscribers, exercising the failure
 * disposition of a fenced in-flight terminal that never landed on the wire.
 * `deliveredData` records only what was actually handed to subscribers.
 */
class GatedRejectingBeforeDeliveryTypePubSub extends GatedTypePubSub {
  rejectOnRelease = new Set<string>();
  deliveredData: any[] = [];
  #heldGates = new Map<string, Promise<void>>();

  override gateType(type: string): { release: () => void } {
    let release!: () => void;
    const gate = new Promise<void>(resolve => {
      release = resolve;
    });
    this.#heldGates.set(type, gate);
    return { release };
  }

  override async publish(topic: string, event: any): Promise<void> {
    const gate = this.#heldGates.get(event.data?.type);
    if (gate) {
      // Hold the publication behind the caller-owned gate; once released,
      // fail WITHOUT delivering: observers never see the event.
      await gate;
      if (this.rejectOnRelease.has(event.data?.type)) {
        throw new Error(`publish rejected before delivery: ${event.data.type}`);
      }
    }
    this.deliveredData.push(event.data);
    await super.publish(topic, event);
  }
}

class ControlledLeasePubSub extends RetainedAsyncCallbackPubSub implements LeaseProvider {
  owners = new Map<string, string>();
  publishedData: any[] = [];
  ownerReadDelayMs = 0;
  ownerReadFailures = 0;
  acquireLeaseWait: Promise<void> | undefined;
  onAcquireLease: (() => void) | undefined;
  transferLeaseWait: Promise<void> | undefined;
  onTransferLease: (() => void) | undefined;
  denyLeaseAcquisition = false;
  denyLeaseTransfer = false;
  rejectPublishedTypes = new Set<string>();
  unsubscribeCount = 0;

  override async publish(topic: string, event: any): Promise<void> {
    this.publishedData.push(event.data);
    await super.publish(topic, event);
    if (this.rejectPublishedTypes.has(event.data?.type)) {
      throw new Error(`publish rejected after delivery: ${event.data.type}`);
    }
  }

  async acquireLease(key: string, owner: string): Promise<{ acquired: boolean; owner?: string }> {
    this.onAcquireLease?.();
    await this.acquireLeaseWait;
    const current = this.owners.get(key);
    if (this.denyLeaseAcquisition) return { acquired: false, owner: current ?? 'competing-run' };
    if (current && current !== owner) return { acquired: false, owner: current };
    this.owners.set(key, owner);
    return { acquired: true, owner };
  }

  async getLeaseOwner(key: string): Promise<string | undefined> {
    if (this.ownerReadDelayMs) await new Promise(resolve => setTimeout(resolve, this.ownerReadDelayMs));
    if (this.ownerReadFailures > 0) {
      this.ownerReadFailures -= 1;
      throw new Error('transient owner read failure');
    }
    return this.owners.get(key);
  }

  async releaseLease(key: string, owner: string): Promise<void> {
    if (this.owners.get(key) === owner) this.owners.delete(key);
  }

  async renewLease(key: string, owner: string): Promise<boolean> {
    return this.owners.get(key) === owner;
  }

  async transferLease(key: string, fromOwner: string, toOwner: string): Promise<boolean> {
    this.onTransferLease?.();
    await this.transferLeaseWait;
    if (this.denyLeaseTransfer) return false;
    if (this.owners.get(key) !== fromOwner) return false;
    this.owners.set(key, toOwner);
    return true;
  }

  override async unsubscribe(topic: string, cb: EventCallback): Promise<void> {
    this.unsubscribeCount += 1;
    await super.unsubscribe(topic, cb);
  }
}

class ClaimedOwnerAckPubSub extends PubSub implements LeaseProvider {
  owners = new Map<string, string>();
  acked: string[] = [];
  nacked: string[] = [];
  published: Array<{ topic: string; event: any }> = [];
  rejectDataTypes = new Set<string>();
  #subscribers = new Map<string, Set<EventCallback>>();
  #index = 0;

  async publish(topic: string, event: any): Promise<void> {
    const envelope = {
      ...event,
      id: event.id ?? `claimed-owner-ack-${this.#index++}`,
      createdAt: event.createdAt ?? new Date(),
    };
    this.published.push({ topic, event: envelope });
    if (this.rejectDataTypes.has(event.data?.type)) {
      throw new Error(`injected publication failure for ${event.data.type}`);
    }

    for (const subscriber of [...(this.#subscribers.get(topic) ?? [])]) {
      let settled = false;
      const ack = async () => {
        if (settled) return;
        settled = true;
        this.acked.push(event.data?.type ?? event.type);
      };
      const nack = async () => {
        if (settled) return;
        settled = true;
        this.nacked.push(event.data?.type ?? event.type);
      };
      try {
        await subscriber(envelope, ack, nack);
      } catch (error) {
        await nack();
        throw error;
      }
    }
  }

  async subscribe(topic: string, cb: EventCallback): Promise<void> {
    const subscribers = this.#subscribers.get(topic) ?? new Set<EventCallback>();
    subscribers.add(cb);
    this.#subscribers.set(topic, subscribers);
  }

  async unsubscribe(topic: string, cb: EventCallback): Promise<void> {
    this.#subscribers.get(topic)?.delete(cb);
  }

  async flush(): Promise<void> {}

  async acquireLease(key: string, owner: string): Promise<{ acquired: boolean; owner?: string }> {
    const current = this.owners.get(key);
    if (current && current !== owner) return { acquired: false, owner: current };
    this.owners.set(key, owner);
    return { acquired: true, owner };
  }

  async getLeaseOwner(key: string): Promise<string | undefined> {
    return this.owners.get(key);
  }

  async releaseLease(key: string, owner: string): Promise<void> {
    if (this.owners.get(key) === owner) this.owners.delete(key);
  }

  async renewLease(key: string, owner: string): Promise<boolean> {
    return this.owners.get(key) === owner;
  }

  async transferLease(key: string, fromOwner: string, toOwner: string): Promise<boolean> {
    if (this.owners.get(key) !== fromOwner) return false;
    this.owners.set(key, toOwner);
    return true;
  }
}

class HangingUnsubscribePubSub extends ControlledLeasePubSub {
  override async unsubscribe(topic: string, cb: EventCallback): Promise<void> {
    await super.unsubscribe(topic, cb);
    return new Promise<void>(() => {});
  }
}

async function readNextRun(iterator: AsyncIterator<any>) {
  const nextRun = await readNextRunWithParts(iterator);
  if (nextRun.done) return nextRun;
  return { value: { runId: nextRun.value.runId, text: nextRun.value.text, part: nextRun.value.part }, done: false };
}
async function readNextRunWithParts(iterator: AsyncIterator<any>) {
  let runId: string | undefined;
  let text = '';
  const parts: any[] = [];

  while (true) {
    const next = await iterator.next();
    if (next.done) return next;

    const part = next.value;
    parts.push(part);
    runId ??= part.runId;
    if (part.type === 'text-delta') {
      text += part.payload.text;
    }
    if (part.type === 'finish' || part.type === 'error' || part.type === 'abort') {
      return { value: { runId, text, part, parts }, done: false };
    }
  }
}

class BlockingRunCompletedPubSub extends EventEmitterPubSub {
  #unblockRunCompleted!: () => void;
  readonly blockedRunCompleted = new Promise<void>(resolve => {
    this.#unblockRunCompleted = resolve;
  });
  sawRunCompleted = false;

  override async publish(topic: string, event: Parameters<PubSub['publish']>[1]): Promise<void> {
    if ((event as { data?: { type?: string } }).data?.type === 'run-completed') {
      this.sawRunCompleted = true;
      await this.blockedRunCompleted;
    }
    await super.publish(topic, event);
  }

  unblockRunCompleted() {
    this.#unblockRunCompleted();
  }
}

class DeliverThenRejectRegistrationPubSub extends EventEmitterPubSub {
  #rejected = false;

  override async publish(topic: string, event: Parameters<PubSub['publish']>[1]): Promise<void> {
    await super.publish(topic, event);
    if (!this.#rejected && (event as { data?: { type?: string } }).data?.type === 'run-registered') {
      this.#rejected = true;
      throw new Error('injected registration failure after subscriber delivery');
    }
  }
}

class RejectFirstRunCompletedPubSub extends EventEmitterPubSub {
  #rejected = false;

  override async publish(topic: string, event: Parameters<PubSub['publish']>[1]): Promise<void> {
    if (!this.#rejected && (event as { data?: { type?: string } }).data?.type === 'run-completed') {
      this.#rejected = true;
      throw new Error('injected first terminal publication failure');
    }
    await super.publish(topic, event);
  }
}

class RejectSignalEnqueuedPubSub extends EventEmitterPubSub {
  override async publish(topic: string, event: Parameters<PubSub['publish']>[1]): Promise<void> {
    if ((event as { data?: { type?: string } }).data?.type === 'signal-enqueued') {
      throw new Error('injected signal enqueue publication failure');
    }
    await super.publish(topic, event);
  }
}

async function waitForActiveRun(subscription: { activeRunId: () => string | null }, timeoutMs = 500) {
  const startedAt = Date.now();
  let runId = subscription.activeRunId();
  while (!runId) {
    if (Date.now() - startedAt > timeoutMs) {
      throw new Error('Timed out waiting for active run');
    }
    await nextTick();
    runId = subscription.activeRunId();
  }
  return runId;
}

async function waitForCondition(predicate: () => boolean, timeoutMs = 500) {
  const startedAt = Date.now();
  while (!predicate()) {
    if (Date.now() - startedAt > timeoutMs) {
      throw new Error('Timed out waiting for condition');
    }
    await nextTick();
  }
}

async function withTimeout<T>(promise: Promise<T>, message: string, timeoutMs = 500): Promise<T> {
  let timeout: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<T>((_, reject) => {
        timeout = setTimeout(() => reject(new Error(message)), timeoutMs);
      }),
    ]);
  } finally {
    if (timeout) clearTimeout(timeout);
  }
}

describe('Agent signals', () => {
  beforeEach(() => {
    agentThreadStreamRuntime.resetForTests();
  });

  it('converts signals between DB, LLM, and data part formats', () => {
    const signal = createSignal({
      id: 'signal-1',
      type: 'user-message',
      contents: 'Signal contents',
      createdAt: new Date('2026-01-01T00:00:00.000Z'),
      attributes: { priority: 'high' },
      metadata: { source: 'test', signal: { userProvided: true } },
    });

    expect(signal.toLLMMessage()).toEqual({
      role: 'user',
      content: '<user priority="high">Signal contents</user>',
    });
    expect(signal.toDataPart()).toEqual({
      type: 'data-user-message',
      data: {
        id: 'signal-1',
        type: 'user',
        tagName: 'user',
        contents: 'Signal contents',
        createdAt: '2026-01-01T00:00:00.000Z',
        attributes: { priority: 'high' },
        metadata: { source: 'test', signal: { userProvided: true } },
      },
      transient: true,
    });

    const dbMessage = signal.toDBMessage({ threadId: 'thread-1', resourceId: 'resource-1' });
    expect(dbMessage.role).toBe('signal');
    expect(dbMessage.content.metadata).toEqual({
      signal: {
        id: 'signal-1',
        type: 'user',
        tagName: 'user',
        createdAt: '2026-01-01T00:00:00.000Z',
        attributes: { priority: 'high' },
        metadata: { source: 'test', signal: { userProvided: true } },
      },
    });
    expect(signalToMastraDBMessage(signal).role).toBe('signal');
    expect(mastraDBMessageToSignal(dbMessage).contents).toBe('Signal contents');
    expect(mastraDBMessageToSignal(dbMessage).attributes).toEqual({ priority: 'high' });
    expect(mastraDBMessageToSignal(dbMessage).metadata).toEqual({ source: 'test', signal: { userProvided: true } });
    expect(dataPartToSignal(signalToDataPartFormat(signal)).contents).toBe('Signal contents');

    const reminderSignal = createSignal({
      id: 'signal-2',
      type: 'system-reminder',
      contents: 'Use <safe> content & continue',
      createdAt: new Date('2026-01-01T00:00:00.000Z'),
      attributes: { type: 'dynamic-agents-md', path: '/tmp/AGENTS.md', enabled: true, ignored: null },
    });

    expect(reminderSignal.toLLMMessage()).toEqual({
      role: 'user',
      content:
        '<system-reminder type="dynamic-agents-md" path="/tmp/AGENTS.md" enabled="true">Use &lt;safe&gt; content &amp; continue</system-reminder>',
    });
    expect(reminderSignal.toDataPart().data.attributes).toEqual({
      type: 'dynamic-agents-md',
      path: '/tmp/AGENTS.md',
      enabled: true,
      ignored: null,
    });
    expect(mastraDBMessageToSignal(reminderSignal.toDBMessage()).attributes).toEqual({
      type: 'dynamic-agents-md',
      path: '/tmp/AGENTS.md',
      enabled: true,
      ignored: null,
    });

    const fileContents = [
      { type: 'text' as const, text: 'Review this file' },
      {
        type: 'file' as const,
        data: 'data:text/plain;base64,aGVsbG8=',
        mediaType: 'text/plain',
        filename: 'note.txt',
      },
    ];
    const fileSignal = createSignal({
      id: 'signal-3',
      type: 'user-message',
      contents: fileContents,
      createdAt: new Date('2026-01-01T00:00:00.000Z'),
    });

    // toLLMMessage emits the v5 UserModelMessage shape (uses mediaType for FilePart).
    expect(fileSignal.toLLMMessage()).toEqual({
      role: 'user',
      content: [
        { type: 'text', text: 'Review this file' },
        {
          type: 'file',
          data: 'data:text/plain;base64,aGVsbG8=',
          mediaType: 'text/plain',
          filename: 'note.txt',
        },
      ],
    });
    expect(fileSignal.toDataPart().data.contents).toEqual(fileContents);
    expect(mastraDBMessageToSignal(fileSignal.toDBMessage()).contents).toEqual(fileContents);
  });

  it('normalizes message signals and legacy signal types', () => {
    const messageSignal = createMessageSignal({
      contents: 'Hello',
      attributes: { sentFrom: 'test' },
    });
    expect(messageSignal.type).toBe('user');
    expect(messageSignal.tagName).toBe('user');
    expect(messageSignal.toLLMMessage()).toEqual({ role: 'user', content: '<user sentFrom="test">Hello</user>' });

    const legacyMessage = createSignal({ type: 'user-message', contents: 'Legacy message' });
    expect(legacyMessage.type).toBe('user');
    expect(legacyMessage.tagName).toBe('user');
    expect(legacyMessage.toLLMMessage()).toEqual({ role: 'user', content: 'Legacy message' });

    const legacyReminder = createSignal({ type: 'system-reminder', contents: 'Remember this' });
    expect(legacyReminder.type).toBe('reactive');
    expect(legacyReminder.tagName).toBe('system-reminder');
    expect(legacyReminder.toLLMMessage()).toEqual({
      role: 'user',
      content: '<system-reminder>Remember this</system-reminder>',
    });

    const reactiveReminder = createSignal({ type: 'reactive', contents: 'Default reminder tag' });
    expect(reactiveReminder.type).toBe('reactive');
    expect(reactiveReminder.tagName).toBe('system-reminder');
    expect(reactiveReminder.toLLMMessage()).toEqual({
      role: 'user',
      content: '<system-reminder>Default reminder tag</system-reminder>',
    });

    const customTaggedReminder = createSignal({
      type: 'reactive',
      tagName: 'custom-reminder',
      contents: 'Custom tag',
    });
    expect(customTaggedReminder.type).toBe('reactive');
    expect(customTaggedReminder.tagName).toBe('custom-reminder');
    expect(() => createSignal({ type: 'custom-reminder' as any, contents: 'Legacy custom' })).toThrow(
      'Invalid signal type: custom-reminder',
    );
  });

  it('renders user-message attributes inline-wrapped for text and multimodal contents', () => {
    const stringSignal = createSignal({
      type: 'user-message',
      contents: 'Hello',
      attributes: { messageId: 'm-1', userId: 'u-1' },
    });
    expect(stringSignal.toLLMMessage()).toEqual({
      role: 'user',
      content: '<user messageId="m-1" userId="u-1">Hello</user>',
    });

    const partsTextSignal = createSignal({
      type: 'user-message',
      contents: [{ type: 'text', text: 'Hello again' }],
      attributes: { messageId: 'm-1b' },
    });
    expect(partsTextSignal.toLLMMessage()).toEqual({
      role: 'user',
      content: '<user messageId="m-1b">Hello again</user>',
    });

    const fileContents = [
      { type: 'text' as const, text: 'Look at this' },
      {
        type: 'file' as const,
        data: 'data:image/png;base64,aGVsbG8=',
        mediaType: 'image/png',
      },
    ];
    const multimodalSignal = createSignal({
      type: 'user-message',
      contents: fileContents,
      attributes: { messageId: 'm-2' },
    });
    // Multimodal: text part is inline-wrapped, file part is preserved.
    const multimodalResult = multimodalSignal.toLLMMessage();
    expect(multimodalResult.role).toBe('user');
    expect(multimodalResult.content).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          type: 'text',
          text: '<user messageId="m-2">Look at this</user>',
        }),
        expect.objectContaining({
          type: 'file',
          data: 'data:image/png;base64,aGVsbG8=',
        }),
      ]),
    );

    // file-only: no text part exists, so the marker is prepended as a synthetic text part on
    // the same message so the attributes still surface alongside the file payload.
    const fileOnlyContents = [
      { type: 'file' as const, data: 'data:image/png;base64,aGVsbG8=', mediaType: 'image/png' },
    ];
    const fileOnlySignal = createSignal({
      type: 'user-message',
      contents: fileOnlyContents,
      attributes: { messageId: 'm-2d' },
    });
    const fileOnlyResult = fileOnlySignal.toLLMMessage();
    expect(fileOnlyResult.role).toBe('user');
    expect(fileOnlyResult.content).toEqual([
      expect.objectContaining({ type: 'text', text: '<user messageId="m-2d" />' }),
      expect.objectContaining({ type: 'file', data: 'data:image/png;base64,aGVsbG8=' }),
    ]);

    const noAttributeSignal = createSignal({
      type: 'user-message',
      contents: 'Plain message',
    });
    expect(noAttributeSignal.toLLMMessage()).toEqual({ role: 'user', content: 'Plain message' });

    const onlyNullAttributesSignal = createSignal({
      type: 'user-message',
      contents: 'Plain message',
      attributes: { ignored: null, alsoIgnored: undefined },
    });
    expect(onlyNullAttributesSignal.toLLMMessage()).toEqual({ role: 'user', content: 'Plain message' });
  });

  it('renders system-reminder signals with multimodal contents the same way as user-message attributes', () => {
    // Text-only system-reminder still wraps even without attributes (the wrapper is the signal).
    const plainReminder = createSignal({
      type: 'system-reminder',
      contents: 'Be concise.',
    });
    expect(plainReminder.toLLMMessage()).toEqual({
      role: 'user',
      content: '<system-reminder>Be concise.</system-reminder>',
    });

    // System-reminder with multimodal contents: text part is inline-wrapped with the marker,
    // file part is preserved alongside it on the same logical turn.
    const screenshotContents = [
      { type: 'text' as const, text: 'The user is looking at this screen.' },
      {
        type: 'file' as const,
        data: 'data:image/png;base64,aGVsbG8=',
        mediaType: 'image/png',
      },
    ];
    const screenshotReminder = createSignal({
      type: 'system-reminder',
      contents: screenshotContents,
      attributes: { kind: 'screenshot' },
    });
    const screenshotResult = screenshotReminder.toLLMMessage();
    expect(screenshotResult.role).toBe('user');
    expect(screenshotResult.content).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          type: 'text',
          text: '<system-reminder kind="screenshot">The user is looking at this screen.</system-reminder>',
        }),
        expect.objectContaining({
          type: 'file',
          data: 'data:image/png;base64,aGVsbG8=',
        }),
      ]),
    );

    // System-reminder with only file parts has no text to inline-wrap, so the marker is
    // prepended as a synthetic text part on the same message.
    const fileOnlyReminderContents = [
      { type: 'file' as const, data: 'data:image/png;base64,aGVsbG8=', mediaType: 'image/png' },
    ];
    const fileOnlyReminder = createSignal({
      type: 'system-reminder',
      contents: fileOnlyReminderContents,
      attributes: { kind: 'reference-image' },
    });
    const fileOnlyResult = fileOnlyReminder.toLLMMessage();
    expect(fileOnlyResult.role).toBe('user');
    expect(fileOnlyResult.content).toEqual([
      expect.objectContaining({ type: 'text', text: '<system-reminder kind="reference-image" />' }),
      expect.objectContaining({ type: 'file', data: 'data:image/png;base64,aGVsbG8=' }),
    ]);

    // System-reminder with mixed text + file parts: the marker is inlined into the very first
    // text part, subsequent parts pass through untouched on the same logical turn.
    const mixedReminderContents = [
      { type: 'text' as const, text: 'Step one of the screen.' },
      { type: 'text' as const, text: 'Step two has this attachment.' },
      { type: 'file' as const, data: 'data:image/png;base64,aGVsbG8=', mediaType: 'image/png' },
    ];
    const mixedReminder = createSignal({
      type: 'system-reminder',
      contents: mixedReminderContents,
      attributes: { kind: 'walkthrough' },
    });
    const mixedResult = mixedReminder.toLLMMessage();
    expect(mixedResult.content).toEqual([
      expect.objectContaining({
        type: 'text',
        text: '<system-reminder kind="walkthrough">Step one of the screen.</system-reminder>',
      }),
      expect.objectContaining({ type: 'text', text: 'Step two has this attachment.' }),
      expect.objectContaining({ type: 'file', data: 'data:image/png;base64,aGVsbG8=' }),
    ]);
  });

  it('persists multimodal signal contents as faithful DB parts so UIs can render them', () => {
    const fileContents = [
      { type: 'text' as const, text: 'Look at this' },
      { type: 'file' as const, data: 'data:image/png;base64,aGVsbG8=', mediaType: 'image/png' },
    ];

    const userMessage = createSignal({
      type: 'user-message',
      contents: fileContents,
      attributes: { messageId: 'm-1' },
    });
    const userDb = userMessage.toDBMessage();
    expect(userDb.content.parts).toEqual([
      expect.objectContaining({ type: 'text', text: 'Look at this' }),
      expect.objectContaining({ type: 'file', data: 'data:image/png;base64,aGVsbG8=' }),
    ]);
    // Stash is dropped — metadata.signal carries only envelope fields (id/type/attributes/createdAt).
    const signalMeta = (userDb.content.metadata as { signal: Record<string, unknown> }).signal;
    expect(signalMeta).not.toHaveProperty('contents');
    expect(signalMeta).toMatchObject({ type: 'user', tagName: 'user', attributes: { messageId: 'm-1' } });

    const reminder = createSignal({
      type: 'system-reminder',
      contents: fileContents,
      attributes: { kind: 'screenshot' },
    });
    const reminderDb = reminder.toDBMessage();
    expect(reminderDb.content.parts).toEqual([
      expect.objectContaining({ type: 'text', text: 'Look at this' }),
      expect.objectContaining({ type: 'file', data: 'data:image/png;base64,aGVsbG8=' }),
    ]);

    // Empty contents still produce a single empty text part so consumers that assume non-empty parts stay happy.
    const emptyReminder = createSignal({ type: 'system-reminder', contents: '' });
    expect(emptyReminder.toDBMessage().content.parts).toEqual([{ type: 'text', text: '' }]);
  });

  it('round-trips multimodal non-user-message signals through DB without dropping file parts', () => {
    const screenshotContents = [
      { type: 'text' as const, text: 'The user is looking at this screen.' },
      { type: 'file' as const, data: 'data:image/png;base64,aGVsbG8=', mediaType: 'image/png' },
    ];
    const reminder = createSignal({
      type: 'system-reminder',
      contents: screenshotContents,
      attributes: { kind: 'screenshot' },
    });
    const rehydrated = mastraDBMessageToSignal(reminder.toDBMessage());
    expect(rehydrated.type).toBe('reactive');
    expect(rehydrated.tagName).toBe('system-reminder');
    expect(rehydrated.contents).toEqual(screenshotContents);
    expect(rehydrated.attributes).toEqual({ kind: 'screenshot' });

    // dataPart round-trip preserves the multimodal shape too.
    const fromDataPart = dataPartToSignal(reminder.toDataPart());
    expect(fromDataPart.contents).toEqual(screenshotContents);
  });

  it('threads providerOptions through LLM message, DB storage, and rehydration', () => {
    const providerOptions = {
      openai: { reasoningEffort: 'high' },
      anthropic: { cacheControl: { type: 'ephemeral' } },
    };
    const signal = createSignal({
      type: 'user-message',
      contents: 'hello',
      providerOptions,
    });

    // LLM message: providerOptions on the CoreMessage so it flows to the model.
    const llmMessage = signal.toLLMMessage();
    expect(llmMessage).toMatchObject({ role: 'user', content: 'hello', providerOptions });

    // DB storage: content.providerMetadata (canonical location, also surfaces to useChat).
    const db = signal.toDBMessage();
    expect(db.content.providerMetadata).toEqual(providerOptions);

    // Round-trip: rehydrated signal carries providerOptions and re-emits it.
    const rehydrated = mastraDBMessageToSignal(db);
    expect(rehydrated.providerOptions).toEqual(providerOptions);
    expect(rehydrated.toLLMMessage()).toMatchObject({ providerOptions });
  });

  it('omits providerOptions on LLM / DB output when not provided', () => {
    const signal = createSignal({ type: 'user-message', contents: 'hi' });
    const llmMessage = signal.toLLMMessage();
    expect((llmMessage as { providerOptions?: unknown }).providerOptions).toBeUndefined();
    expect(signal.toDBMessage().content.providerMetadata).toBeUndefined();
  });

  it('threads per-part providerOptions through LLM, DB, and rehydration', () => {
    const partProviderOptions = { anthropic: { cacheControl: { type: 'ephemeral' } } };
    const signal = createSignal({
      type: 'user-message',
      contents: [
        { type: 'text', text: 'hello', providerOptions: partProviderOptions },
        { type: 'file', data: 'AAA=', mediaType: 'image/png' },
      ],
    });

    // LLM: parts array carries per-part providerOptions (not collapsed to bare string).
    const llmMessage = signal.toLLMMessage();
    expect(llmMessage.role).toBe('user');
    expect(Array.isArray(llmMessage.content)).toBe(true);
    const llmParts = llmMessage.content as Array<{ type: string; providerOptions?: unknown }>;
    expect(llmParts[0]).toMatchObject({ type: 'text', text: 'hello', providerOptions: partProviderOptions });
    expect(llmParts[1]).toMatchObject({ type: 'file', data: 'AAA=', mediaType: 'image/png' });

    // DB: per-part providerMetadata persisted alongside the storage part.
    const db = signal.toDBMessage();
    const textPart = db.content.parts[0] as { type: string; providerMetadata?: unknown };
    expect(textPart).toMatchObject({ type: 'text', text: 'hello', providerMetadata: partProviderOptions });

    // Round-trip: rehydrated signal restores per-part providerOptions.
    const rehydrated = mastraDBMessageToSignal(db);
    const rehydratedContents = rehydrated.contents as Array<{ type: string; providerOptions?: unknown }>;
    expect(rehydratedContents[0]).toMatchObject({ type: 'text', text: 'hello', providerOptions: partProviderOptions });
  });

  it('preserves per-part providerOptions on a single-text user-message (no bare-string collapse)', () => {
    const partProviderOptions = { anthropic: { cacheControl: { type: 'ephemeral' } } };
    const signal = createSignal({
      type: 'user-message',
      contents: [{ type: 'text', text: 'hello', providerOptions: partProviderOptions }],
    });

    const llmMessage = signal.toLLMMessage();
    // Must keep parts array — collapsing to a bare string would drop providerOptions.
    expect(Array.isArray(llmMessage.content)).toBe(true);
    const llmParts = llmMessage.content as Array<{ type: string; providerOptions?: unknown }>;
    expect(llmParts[0]).toMatchObject({ type: 'text', text: 'hello', providerOptions: partProviderOptions });
  });

  describe('legacy metadata.signal.contents rehydration', () => {
    function buildLegacyDBRow(legacyContents: unknown) {
      const row = createSignal({
        id: 'signal-legacy',
        createdAt: '2026-01-01T00:00:00.000Z',
        type: 'user-message',
        contents: 'placeholder',
      }).toDBMessage();
      row.content.metadata = {
        ...row.content.metadata,
        signal: {
          ...(row.content.metadata?.signal as Record<string, unknown>),
          contents: legacyContents,
        },
      };
      return row;
    }

    it('recovers a bare string stash', () => {
      const rehydrated = mastraDBMessageToSignal(buildLegacyDBRow('hello world'));
      expect(rehydrated.contents).toBe('hello world');
    });

    it('recovers an Array<TextPart | FilePart> stash with mediaType', () => {
      const rehydrated = mastraDBMessageToSignal(
        buildLegacyDBRow([
          { type: 'text', text: 'caption' },
          { type: 'file', data: 'BASE64', mediaType: 'image/png', filename: 'photo.png' },
        ]),
      );
      expect(rehydrated.contents).toEqual([
        { type: 'text', text: 'caption' },
        { type: 'file', data: 'BASE64', mediaType: 'image/png', filename: 'photo.png' },
      ]);
    });

    it('recovers a CoreUserMessage wrapper with text-only content', () => {
      const rehydrated = mastraDBMessageToSignal(buildLegacyDBRow({ role: 'user', content: 'hello world' }));
      expect(rehydrated.contents).toBe('hello world');
    });

    it('recovers a CoreUserMessage wrapper with mixed text + image parts', () => {
      const rehydrated = mastraDBMessageToSignal(
        buildLegacyDBRow({
          role: 'user',
          content: [
            { type: 'text', text: 'what is this?' },
            { type: 'image', image: 'BASE64', mediaType: 'image/png' },
          ],
        }),
      );
      expect(rehydrated.contents).toEqual([
        { type: 'text', text: 'what is this?' },
        { type: 'file', data: 'BASE64', mediaType: 'image/png' },
      ]);
    });

    it('recovers a CoreUserMessage[] stash from the React hook', () => {
      const rehydrated = mastraDBMessageToSignal(
        buildLegacyDBRow([
          { role: 'user', content: 'first' },
          { role: 'user', content: [{ type: 'text', text: 'second' }] },
        ]),
      );
      expect(rehydrated.contents).toEqual([
        { type: 'text', text: 'first' },
        { type: 'text', text: 'second' },
      ]);
    });

    it('falls back to canonical content.parts when the stash is unrecognisable', () => {
      const row = buildLegacyDBRow({ totally: 'unrelated' });
      row.content.parts = [{ type: 'text', text: 'from canonical parts' }];
      const rehydrated = mastraDBMessageToSignal(row);
      expect(rehydrated.contents).toBe('from canonical parts');
    });

    it('prefers a valid multimodal stash over flattened-text content.parts (main-era rows)', () => {
      // Main wrote the full original input to metadata.signal.contents and a flattened text
      // projection to content.parts. If we preferred parts here we'd silently drop the file
      // payload on rehydrate.
      const row = buildLegacyDBRow([
        { type: 'text', text: 'caption' },
        { type: 'file', data: 'BASE64', mediaType: 'image/png', filename: 'photo.png' },
      ]);
      row.content.parts = [{ type: 'text', text: 'caption' }];
      const rehydrated = mastraDBMessageToSignal(row);
      expect(rehydrated.contents).toEqual([
        { type: 'text', text: 'caption' },
        { type: 'file', data: 'BASE64', mediaType: 'image/png', filename: 'photo.png' },
      ]);
    });
  });

  it('rejects invalid XML names for contextual signal markup', () => {
    expect(() =>
      createSignal({
        type: 'reactive',
        tagName: 'system reminder',
        contents: 'invalid tag name',
      }).toLLMMessage(),
    ).toThrow('Invalid signal XML tag name: system reminder');

    expect(() =>
      createSignal({
        type: 'system-reminder',
        contents: 'invalid attribute name',
        attributes: { 'bad attr': 'value' },
      }).toLLMMessage(),
    ).toThrow('Invalid signal XML attribute name: bad attr');
  });

  it('subscribes to a future thread run', async () => {
    const agent = new Agent({
      id: 'future-thread-agent',
      name: 'Future Thread Agent',
      instructions: 'Test',
      model: createTextStreamModel('future response'),
    });

    const subscription = await agent.subscribeToThread({
      threadId: 'future-thread',
      resourceId: 'future-user',
    });
    const nextRun = readNextRun(subscription.stream[Symbol.asyncIterator]());

    const stream = await agent.stream('Hello', {
      memory: { thread: 'future-thread', resource: 'future-user' },
    });

    const subscribedRun = await nextRun;
    expect(subscribedRun.value.runId).toBe(stream.runId);
    expect(subscribedRun.value.text).toBe('future response');

    subscription.unsubscribe();
  });

  it('does not reject a partially delivered registration until its enqueued stream is drained', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new DeliverThenRejectRegistrationPubSub();
    const agent = { id: 'partial-registration-agent' } as Agent<any, any, any, any>;
    const threadId = 'partial-registration-thread';
    const resourceId = 'partial-registration-user';
    const runId = 'partial-registration-run';
    const parts = [
      { type: 'start', runId },
      { type: 'tool-call', runId, payload: { toolCallId: 'late-tool', toolName: 'lookup', args: {} } },
      {
        type: 'finish',
        runId,
        payload: { usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 }, finishReason: 'stop' },
      },
    ];
    const output = {
      runId,
      status: 'running',
      fullStream: new ReadableStream({
        start(controller) {
          for (const part of parts) controller.enqueue(part);
          controller.close();
        },
      }),
      _waitUntilFinished: () => Promise.resolve(),
    } as any;
    const subscription = await runtime.subscribeToThread(agent, { threadId, resourceId }, pubsub);

    const completion = runtime.registerRun(
      agent,
      output,
      { memory: { thread: threadId, resource: resourceId } } as any,
      pubsub,
    );
    void completion?.catch(() => {});
    const outputDrain = subscription._waitForOutputDrain!(output)!;
    let settled = false;
    void outputDrain.finally(() => (settled = true)).catch(() => {});

    await nextTick();
    expect(settled).toBe(false);

    const iterator = subscription.stream[Symbol.asyncIterator]();
    const received = [];
    for (let index = 0; index < parts.length; index++) {
      received.push((await iterator.next()).value);
    }
    // Advance once more so the per-output generator observes the source close
    // and acknowledges that no buffered chunk can surface after rejection.
    const waitingForNextRun = iterator.next();

    await expect(outputDrain).rejects.toMatchObject({
      name: 'AgentThreadOutputDrainError',
      reason: 'registration-publish-failed',
    });
    expect(received).toEqual(parts);

    subscription.unsubscribe();
    await waitingForNextRun;
  });

  it('aborts a local provider without broadcasting after losing the exact thread lease', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new ControlledLeasePubSub();
    const agent = { id: 'lost-registration-lease-agent' } as Agent<any, any, any, any>;
    const resourceId = 'lost-registration-lease-user';
    const threadId = 'lost-registration-lease-thread';
    const runId = 'lost-registration-lease-run';
    const key = `${resourceId}\u0000${threadId}`;
    const competingOwner = `mastra-thread-owner:${JSON.stringify(['competing-run', 'remote-source', 'attempt'])}`;
    pubsub.owners.set(key, competingOwner);
    const options = runtime.prepareRunOptions(
      { runId, memory: { resource: resourceId, thread: threadId } } as any,
      pubsub,
    );
    let providerAborted = false;
    const providerFinished = new Promise<void>((_resolve, reject) => {
      options.abortSignal?.addEventListener(
        'abort',
        () => {
          providerAborted = true;
          reject(new Error('provider stopped after losing lease'));
        },
        { once: true },
      );
    });
    let sourceRead = false;
    const output = {
      runId,
      status: 'running',
      fullStream: {
        [Symbol.asyncIterator]() {
          sourceRead = true;
          return {
            next: () => new Promise<IteratorResult<unknown>>(() => {}),
          };
        },
      },
      _waitUntilFinished: () => providerFinished,
    } as any;

    const completion = runtime.registerRun(agent, output, options, pubsub)!;
    await expect(withTimeout(completion, 'Lost-lease provider run did not finalize', 2_000)).rejects.toMatchObject({
      name: 'AgentThreadOutputDrainError',
      reason: 'registration-publish-failed',
    });

    expect(providerAborted).toBe(true);
    expect(sourceRead).toBe(false);
    expect(pubsub.owners.get(key)).toBe(competingOwner);
    expect(
      pubsub.publishedData.filter(data =>
        ['run-registered', 'stream-part', 'run-aborting', 'run-aborted'].includes(data?.type),
      ),
    ).toEqual([]);
    expect(runtime.getRunOutput(runId, pubsub)).toBeUndefined();
    expect(runtime.getActiveThreadRunId({ resourceId, threadId }, pubsub)).toBeUndefined();
  });

  it('drains an authoritative tool error before the bounded abort fallback', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    const agent = { id: 'abort-drain-agent' } as Agent<any, any, any, any>;
    const threadId = 'abort-drain-thread';
    const resourceId = 'abort-drain-user';
    const runId = 'abort-drain-run';
    let finish!: () => void;
    const finished = new Promise<void>(resolve => {
      finish = resolve;
    });
    let streamController!: ReadableStreamDefaultController<unknown>;
    const output = {
      runId,
      status: 'running',
      fullStream: new ReadableStream({
        start(controller) {
          streamController = controller;
          controller.enqueue({ type: 'start', runId });
          controller.enqueue({
            type: 'tool-call',
            runId,
            payload: { toolCallId: 'abort-tool', toolName: 'lookup', args: {} },
          });
        },
      }),
      _waitUntilFinished: () => finished,
    } as any;
    const options = runtime.prepareRunOptions(
      { runId, memory: { thread: threadId, resource: resourceId } } as any,
      pubsub,
    );
    const subscription = await runtime.subscribeToThread(agent, { threadId, resourceId }, pubsub);
    const iterator = subscription.stream[Symbol.asyncIterator]();

    try {
      const completion = runtime.registerRun(agent, output, options, pubsub)!;
      void completion.catch(() => {});
      const start = await withTimeout(iterator.next(), 'Timed out waiting for abort-drain start');
      const toolCall = await withTimeout(iterator.next(), 'Timed out waiting for abort-drain tool call');
      expect([start.value.type, toolCall.value.type]).toEqual(['start', 'tool-call']);

      expect(runtime.abortRun(runId, pubsub)).toBe(true);
      queueMicrotask(() => {
        // AI SDK providers may surface the source abort before an in-flight
        // tool observes cancellation. The subscription must retain that source
        // terminal until the authoritative tool settlement crosses the drain.
        streamController.enqueue({ type: 'abort', runId });
        streamController.enqueue({
          type: 'tool-error',
          runId,
          payload: {
            toolCallId: 'abort-tool',
            toolName: 'lookup',
            error: { message: 'local_project.operation_cancelled' },
          },
        });
        streamController.close();
        finish();
      });

      const authoritative = await withTimeout(iterator.next(), 'Timed out waiting for authoritative tool error');
      expect(authoritative.value).toMatchObject({
        type: 'tool-error',
        runId,
        payload: {
          toolCallId: 'abort-tool',
          toolName: 'lookup',
          error: { message: 'local_project.operation_cancelled' },
        },
      });
      const terminal = await withTimeout(iterator.next(), 'Timed out waiting for abort terminal');
      expect(terminal.value).toEqual({ type: 'abort', runId });
      await expect(subscription._waitForOutputDrain!(output)).resolves.toBeUndefined();
    } finally {
      subscription.unsubscribe();
      await iterator.return?.();
    }
  });

  it('finalizes abort when provider completion rejects synchronously during cancellation', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new ControlledLeasePubSub();
    const agent = { id: 'abort-provider-rejection-agent' } as Agent<any, any, any, any>;
    const threadId = 'abort-provider-rejection-thread';
    const resourceId = 'abort-provider-rejection-user';
    const runId = 'abort-provider-rejection-run';
    let rejectProvider!: (error: Error) => void;
    const providerFinished = new Promise<void>((_resolve, reject) => {
      rejectProvider = reject;
    });
    const output = {
      runId,
      status: 'running',
      fullStream: new ReadableStream({
        start(controller) {
          controller.enqueue({ type: 'start', runId });
        },
      }),
      _waitUntilFinished: () => providerFinished,
    } as any;
    const options = runtime.prepareRunOptions(
      { runId, memory: { thread: threadId, resource: resourceId } } as any,
      pubsub,
    );
    const subscription = await runtime.subscribeToThread(agent, { threadId, resourceId }, pubsub);
    const iterator = subscription.stream[Symbol.asyncIterator]();

    try {
      const completion = runtime.registerRun(agent, output, options, pubsub)!;
      await iterator.next();
      options.abortSignal?.addEventListener(
        'abort',
        () => rejectProvider(new Error('provider cancellation rejection')),
        { once: true },
      );
      expect(runtime.abortRun(runId, pubsub)).toBe(true);
      await expect(withTimeout(iterator.next(), 'Timed out waiting for provider rejection abort')).resolves.toEqual({
        value: { type: 'abort', runId },
        done: false,
      });
      await expect(withTimeout(completion, 'Provider rejection abort did not finalize')).resolves.toBeUndefined();
      expect(runtime.getRunOutput(runId, pubsub)).toBeUndefined();
    } finally {
      subscription.unsubscribe();
      await iterator.return?.();
    }
  });

  it('coordinates abort after prepared state cleanup without releasing the live lease early', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new ControlledLeasePubSub();
    let sawRunCompleted = false;
    let releaseRunCompleted!: () => void;
    const runCompletedBlocked = new Promise<void>(resolve => {
      releaseRunCompleted = resolve;
    });
    const originalPublish = pubsub.publish.bind(pubsub);
    pubsub.publish = async (topic, event) => {
      if ((event as { data?: { type?: string } }).data?.type === 'run-completed') {
        sawRunCompleted = true;
        await runCompletedBlocked;
      }
      return originalPublish(topic, event);
    };
    const agent = { id: 'late-abort-agent' } as Agent<any, any, any, any>;
    const resourceId = 'late-abort-user';
    const threadId = 'late-abort-thread';
    const runId = 'late-abort-run';
    let finish!: () => void;
    const finished = new Promise<void>(resolve => {
      finish = resolve;
    });
    const options = runtime.prepareRunOptions(
      { runId, memory: { resource: resourceId, thread: threadId } } as any,
      pubsub,
    );
    const output = {
      runId,
      status: 'running',
      fullStream: new ReadableStream({
        start(controller) {
          controller.enqueue({ type: 'start', runId });
          controller.close();
        },
      }),
      _waitUntilFinished: () => finished,
    } as any;
    const completion = runtime.registerRun(agent, output, options, pubsub)!;
    finish();
    await waitForCondition(() => sawRunCompleted);
    const key = `${resourceId}\u0000${threadId}`;
    expect(pubsub.owners.has(key)).toBe(true);
    expect(runtime.abortRun(runId, pubsub)).toBe(true);
    expect(pubsub.owners.has(key)).toBe(true);
    expect(pubsub.publishedData.some(data => data?.type === 'run-aborting' && data.runId === runId)).toBe(false);
    expect(pubsub.publishedData.some(data => data?.type === 'run-aborted' && data.runId === runId)).toBe(false);
    releaseRunCompleted();
    await completion;
    await waitForCondition(() => !pubsub.owners.has(key));
  });

  it('converts an abort-time source rejection into the abort boundary', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new ControlledLeasePubSub();
    const agent = { id: 'abort-source-error-agent' } as Agent<any, any, any, any>;
    const threadId = 'abort-source-error-thread';
    const resourceId = 'abort-source-error-user';
    const runId = 'abort-source-error-run';
    let rejectSource!: (error: Error) => void;
    const source = {
      [Symbol.asyncIterator]() {
        let emitted = false;
        return {
          async next() {
            if (!emitted) {
              emitted = true;
              return { value: { type: 'start', runId }, done: false };
            }
            return new Promise<IteratorResult<unknown>>((_resolve, reject) => {
              rejectSource = reject;
            });
          },
        };
      },
    };
    const output = {
      runId,
      status: 'running',
      fullStream: source,
      _waitUntilFinished: () => new Promise<void>(() => {}),
    } as any;
    const options = runtime.prepareRunOptions(
      { runId, memory: { thread: threadId, resource: resourceId } } as any,
      pubsub,
    );
    const subscription = await runtime.subscribeToThread(agent, { threadId, resourceId }, pubsub);
    const iterator = subscription.stream[Symbol.asyncIterator]();

    try {
      const completion = runtime.registerRun(agent, output, options, pubsub)!;
      void completion.catch(() => {});
      await expect(withTimeout(iterator.next(), 'Timed out waiting for rejection start')).resolves.toMatchObject({
        value: { type: 'start', runId },
      });
      expect(runtime.abortRun(runId, pubsub)).toBe(true);
      rejectSource(new Error('provider cancellation rejection'));
      await expect(withTimeout(iterator.next(), 'Timed out waiting for rejection abort')).resolves.toEqual({
        value: { type: 'abort', runId },
        done: false,
      });
      await expect(subscription._waitForOutputDrain!(output)).resolves.toBeUndefined();
    } finally {
      subscription.unsubscribe();
      await iterator.return?.();
    }
  });

  it('fails closed when a visible abort-time tool terminal cannot publish', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new ControlledLeasePubSub();
    const publish = pubsub.publish.bind(pubsub);
    pubsub.publish = async (topic, event) => {
      const data = (event as { data?: { type?: string; part?: { type?: string } } }).data;
      if (data?.type === 'stream-part' && data.part?.type === 'tool-error') {
        throw new Error('injected visible tool terminal publication failure');
      }
      await publish(topic, event);
    };
    const agent = { id: 'abort-tool-terminal-failure-agent' } as Agent<any, any, any, any>;
    const threadId = 'abort-tool-terminal-failure-thread';
    const resourceId = 'abort-tool-terminal-failure-user';
    const runId = 'abort-tool-terminal-failure-run';
    let streamController!: ReadableStreamDefaultController<unknown>;
    const output = {
      runId,
      status: 'running',
      fullStream: new ReadableStream({
        start(controller) {
          streamController = controller;
          controller.enqueue({ type: 'start', runId });
          controller.enqueue({
            type: 'tool-call',
            runId,
            payload: { toolCallId: 'abort-tool-terminal-failure-call', toolName: 'lookup', args: {} },
          });
        },
      }),
      _waitUntilFinished: () => new Promise<void>(() => {}),
    } as any;
    const options = runtime.prepareRunOptions(
      { runId, memory: { thread: threadId, resource: resourceId } } as any,
      pubsub,
    );
    const subscription = await runtime.subscribeToThread(agent, { threadId, resourceId }, pubsub);
    const iterator = subscription.stream[Symbol.asyncIterator]();

    try {
      const completion = runtime.registerRun(agent, output, options, pubsub)!;
      void completion.catch(() => {});
      await expect(iterator.next()).resolves.toMatchObject({ value: { type: 'start', runId } });
      await expect(iterator.next()).resolves.toMatchObject({
        value: { type: 'tool-call', payload: { toolCallId: 'abort-tool-terminal-failure-call' } },
      });

      expect(runtime.abortRun(runId, pubsub)).toBe(true);
      const outputDrain = subscription._waitForOutputDrain!(output)!;
      streamController.enqueue({
        type: 'tool-error',
        runId,
        payload: {
          toolCallId: 'abort-tool-terminal-failure-call',
          toolName: 'lookup',
          error: new Error('authoritative cancellation terminal'),
        },
      });

      await expect(
        withTimeout(outputDrain, 'Timed out waiting for tool-terminal publication failure'),
      ).rejects.toMatchObject({
        name: 'AgentThreadOutputDrainError',
        reason: 'terminal-publish-failed',
      });
      expect(pubsub.publishedData.filter(data => data?.type === 'run-aborted' && data.runId === runId)).toEqual([]);
      await expect(withTimeout(completion, 'Tool-terminal publication failure did not finalize')).rejects.toMatchObject(
        {
          name: 'AgentThreadOutputDrainError',
          reason: 'terminal-publish-failed',
        },
      );
      expect(runtime.getRunOutput(runId, pubsub)).toBeUndefined();
      await waitForCondition(() => !pubsub.owners.has(`${resourceId}\u0000${threadId}`));
    } finally {
      subscription.unsubscribe();
      await iterator.return?.();
    }
  });

  it('fails closed when the distributed abort fence cannot publish', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new ControlledLeasePubSub();
    const publish = pubsub.publish.bind(pubsub);
    pubsub.publish = async (topic, event) => {
      if ((event as { data?: { type?: string } }).data?.type === 'run-aborting') {
        throw new Error('injected abort fence failure');
      }
      await publish(topic, event);
    };
    const agent = { id: 'abort-fence-agent' } as Agent<any, any, any, any>;
    const threadId = 'abort-fence-thread';
    const resourceId = 'abort-fence-user';
    const runId = 'abort-fence-run';
    const output = {
      runId,
      status: 'running',
      fullStream: new ReadableStream({
        start(controller) {
          controller.enqueue({ type: 'start', runId });
        },
      }),
      _waitUntilFinished: () => new Promise<void>(() => {}),
    } as any;
    const options = runtime.prepareRunOptions(
      { runId, memory: { thread: threadId, resource: resourceId } } as any,
      pubsub,
    );
    const subscription = await runtime.subscribeToThread(agent, { threadId, resourceId }, pubsub);
    const iterator = subscription.stream[Symbol.asyncIterator]();

    try {
      const completion = runtime.registerRun(agent, output, options, pubsub)!;
      void completion.catch(() => {});
      await iterator.next();
      expect(runtime.abortRun(runId, pubsub)).toBe(true);
      const outputDrain = subscription._waitForOutputDrain!(output)!;
      await expect(withTimeout(outputDrain, 'Timed out waiting for abort-fence failure', 1_000)).rejects.toMatchObject({
        name: 'AgentThreadOutputDrainError',
        reason: 'terminal-publish-failed',
      });
      expect(pubsub.publishedData.filter(data => data?.type === 'run-aborted' && data.runId === runId)).toEqual([]);
      await expect(withTimeout(completion, 'Abort-fence failure did not finalize', 1_000)).rejects.toMatchObject({
        name: 'AgentThreadOutputDrainError',
        reason: 'terminal-publish-failed',
      });
      expect(runtime.getRunOutput(runId, pubsub)).toBeUndefined();
      const key = `${resourceId}\u0000${threadId}`;
      await waitForCondition(() => !pubsub.owners.has(key));
    } finally {
      subscription.unsubscribe();
    }
  });

  it('publishes the abort terminal only after delayed registration succeeds', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new ControlledLeasePubSub();
    let releaseRegistration!: () => void;
    const registrationBlocked = new Promise<void>(resolve => {
      releaseRegistration = resolve;
    });
    const publish = pubsub.publish.bind(pubsub);
    pubsub.publish = async (topic, event) => {
      if ((event as { data?: { type?: string } }).data?.type === 'run-registered') {
        await registrationBlocked;
      }
      await publish(topic, event);
    };
    const agent = { id: 'abort-registration-order-agent' } as Agent<any, any, any, any>;
    const threadId = 'abort-registration-order-thread';
    const resourceId = 'abort-registration-order-user';
    const runId = 'abort-registration-order-run';
    const output = {
      runId,
      status: 'running',
      fullStream: new ReadableStream({
        start(controller) {
          controller.enqueue({ type: 'start', runId });
        },
      }),
      _waitUntilFinished: () => new Promise<void>(() => {}),
    } as any;
    const options = runtime.prepareRunOptions(
      { runId, memory: { thread: threadId, resource: resourceId } } as any,
      pubsub,
    );
    const subscription = await runtime.subscribeToThread(agent, { threadId, resourceId }, pubsub);
    const iterator = subscription.stream[Symbol.asyncIterator]();

    try {
      const completion = runtime.registerRun(agent, output, options, pubsub)!;
      void completion.catch(() => {});
      expect(runtime.abortRun(runId, pubsub)).toBe(true);
      await new Promise(resolve => setTimeout(resolve, 300));
      expect(pubsub.publishedData.some(data => data?.type === 'run-aborted' && data.runId === runId)).toBe(false);
      releaseRegistration();
      await expect(withTimeout(iterator.next(), 'Timed out waiting for delayed abort')).resolves.toEqual({
        value: { type: 'abort', runId },
        done: false,
      });
      const registrationIndex = pubsub.publishedData.findIndex(
        data => data?.type === 'run-registered' && data.runId === runId,
      );
      const terminalIndex = pubsub.publishedData.findIndex(
        data => data?.type === 'run-aborted' && data.runId === runId,
      );
      expect(registrationIndex).toBeGreaterThanOrEqual(0);
      expect(terminalIndex).toBeGreaterThan(registrationIndex);
    } finally {
      subscription.unsubscribe();
      await iterator.return?.();
    }
  });

  it('returns an abort-ignoring async iterator after the bounded grace', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new ControlledLeasePubSub();
    const agent = { id: 'abort-iterator-agent' } as Agent<any, any, any, any>;
    const threadId = 'abort-iterator-thread';
    const resourceId = 'abort-iterator-user';
    const runId = 'abort-iterator-run';
    let returned = false;
    const source = {
      [Symbol.asyncIterator]() {
        let emitted = false;
        return {
          async next() {
            if (!emitted) {
              emitted = true;
              return { value: { type: 'start', runId }, done: false };
            }
            return new Promise<IteratorResult<unknown>>(() => {});
          },
          async return() {
            returned = true;
            return { value: undefined, done: true };
          },
        };
      },
    };
    const output = {
      runId,
      status: 'running',
      fullStream: source,
      _waitUntilFinished: () => new Promise<void>(() => {}),
    } as any;
    const options = runtime.prepareRunOptions(
      { runId, memory: { thread: threadId, resource: resourceId } } as any,
      pubsub,
    );
    const subscription = await runtime.subscribeToThread(agent, { threadId, resourceId }, pubsub);
    const iterator = subscription.stream[Symbol.asyncIterator]();

    try {
      const completion = runtime.registerRun(agent, output, options, pubsub)!;
      void completion.catch(() => {});
      await iterator.next();
      expect(runtime.abortRun(runId, pubsub)).toBe(true);
      await expect(withTimeout(iterator.next(), 'Timed out waiting for iterator abort')).resolves.toEqual({
        value: { type: 'abort', runId },
        done: false,
      });
      await waitForCondition(() => returned);
      await expect(subscription._waitForOutputDrain!(output)).resolves.toBeUndefined();
    } finally {
      subscription.unsubscribe();
      await iterator.return?.();
    }
  });

  it('drains a remote authoritative tool error that follows its abort terminal', async () => {
    const pubsub = new EventEmitterPubSub();
    const sourceRuntime = new AgentThreadStreamRuntime();
    const subscriberRuntime = new AgentThreadStreamRuntime();
    const agent = { id: 'remote-abort-drain-agent' } as Agent<any, any, any, any>;
    const threadId = 'remote-abort-drain-thread';
    const resourceId = 'remote-abort-drain-user';
    const runId = 'remote-abort-drain-run';
    let finish!: () => void;
    const finished = new Promise<void>(resolve => {
      finish = resolve;
    });
    let streamController!: ReadableStreamDefaultController<unknown>;
    const output = {
      runId,
      status: 'running',
      fullStream: new ReadableStream({
        start(controller) {
          streamController = controller;
          controller.enqueue({ type: 'start', runId });
          controller.enqueue({
            type: 'tool-call',
            runId,
            payload: { toolCallId: 'remote-abort-tool', toolName: 'lookup', args: {} },
          });
        },
      }),
      _waitUntilFinished: () => finished,
    } as any;
    const options = sourceRuntime.prepareRunOptions(
      { runId, memory: { thread: threadId, resource: resourceId } } as any,
      pubsub,
    );
    const sourceSubscription = await sourceRuntime.subscribeToThread(agent, { threadId, resourceId }, pubsub);
    const remoteSubscription = await subscriberRuntime.subscribeToThread(agent, { threadId, resourceId }, pubsub);
    const remoteIterator = remoteSubscription.stream[Symbol.asyncIterator]();

    try {
      const completion = sourceRuntime.registerRun(agent, output, options, pubsub)!;
      void completion.catch(() => {});
      const start = await withTimeout(remoteIterator.next(), 'Timed out waiting for remote abort-drain start');
      const toolCall = await withTimeout(remoteIterator.next(), 'Timed out waiting for remote abort-drain tool call');
      expect([start.value.type, toolCall.value.type]).toEqual(['start', 'tool-call']);

      expect(sourceRuntime.abortRun(runId, pubsub)).toBe(true);
      queueMicrotask(() => {
        streamController.enqueue({
          type: 'tool-error',
          runId,
          payload: {
            toolCallId: 'remote-abort-tool',
            toolName: 'lookup',
            error: { message: 'local_project.operation_cancelled' },
          },
        });
        streamController.close();
        finish();
      });

      const authoritative = await withTimeout(remoteIterator.next(), 'Timed out waiting for remote tool error');
      expect(authoritative.value).toMatchObject({
        type: 'tool-error',
        runId,
        payload: {
          toolCallId: 'remote-abort-tool',
          toolName: 'lookup',
          error: { message: 'local_project.operation_cancelled' },
        },
      });
      const terminal = await withTimeout(remoteIterator.next(), 'Timed out waiting for remote abort terminal');
      expect(terminal.value).toEqual({ type: 'abort', runId });
    } finally {
      sourceSubscription.unsubscribe();
      remoteSubscription.unsubscribe();
      await remoteIterator.return?.();
    }
  });

  it('filters post-abort text and new tools while settling an already-visible tool', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    const agent = { id: 'abort-drain-filter-agent' } as Agent<any, any, any, any>;
    const threadId = 'abort-drain-filter-thread';
    const resourceId = 'abort-drain-filter-user';
    const runId = 'abort-drain-filter-run';
    let finish!: () => void;
    const finished = new Promise<void>(resolve => {
      finish = resolve;
    });
    let streamController!: ReadableStreamDefaultController<unknown>;
    const output = {
      runId,
      status: 'running',
      fullStream: new ReadableStream({
        start(controller) {
          streamController = controller;
          controller.enqueue({ type: 'start', runId });
          controller.enqueue({
            type: 'tool-call',
            runId,
            payload: { toolCallId: 'visible-tool', toolName: 'lookup', args: {} },
          });
        },
      }),
      _waitUntilFinished: () => finished,
    } as any;
    const options = runtime.prepareRunOptions(
      { runId, memory: { thread: threadId, resource: resourceId } } as any,
      pubsub,
    );
    const subscription = await runtime.subscribeToThread(agent, { threadId, resourceId }, pubsub);
    const iterator = subscription.stream[Symbol.asyncIterator]();

    try {
      const completion = runtime.registerRun(agent, output, options, pubsub)!;
      void completion.catch(() => {});
      await iterator.next();
      await iterator.next();

      expect(runtime.abortRun(runId, pubsub)).toBe(true);
      queueMicrotask(() => {
        streamController.enqueue({ type: 'text-delta', runId, payload: { id: 'late-text', text: 'must drop' } });
        streamController.enqueue({
          type: 'tool-call',
          runId,
          payload: { toolCallId: 'late-tool', toolName: 'write', args: {} },
        });
        streamController.enqueue({
          type: 'finish',
          runId,
          payload: {
            usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
            finishReason: 'stop',
          },
        });
        streamController.enqueue({
          type: 'tool-error',
          runId,
          payload: {
            toolCallId: 'visible-tool',
            toolName: 'lookup',
            error: { message: 'local_project.operation_cancelled' },
          },
        });
        streamController.close();
        finish();
      });

      const authoritative = await withTimeout(iterator.next(), 'Timed out waiting for filtered tool terminal');
      expect(authoritative.value).toMatchObject({
        type: 'tool-error',
        payload: { toolCallId: 'visible-tool', error: { message: 'local_project.operation_cancelled' } },
      });
      const terminal = await withTimeout(iterator.next(), 'Timed out waiting for filtered abort terminal');
      expect(terminal.value).toEqual({ type: 'abort', runId });
      expect([authoritative.value.type, terminal.value.type]).toEqual(['tool-error', 'abort']);
      expect(authoritative.value.payload?.toolCallId).not.toBe('late-tool');
    } finally {
      subscription.unsubscribe();
      await iterator.return?.();
    }
  });

  it('accepts a resumed remote stream with a process-local sequence reset', async () => {
    const pubsub = new EventEmitterPubSub();
    const runtime = new AgentThreadStreamRuntime();
    const agent = { id: 'remote-resume-reset-agent' } as Agent<any, any, any, any>;
    const target = { threadId: 'remote-resume-reset-thread', resourceId: 'remote-resume-reset-user' };
    const key = `${target.resourceId}\u0000${target.threadId}`;
    const topic = `agent.thread-stream.${encodeURIComponent(key)}`;
    const runId = 'remote-resume-reset-run';
    const leaseOwner = `mastra-thread-owner:${JSON.stringify([runId, 'remote-source', 'attempt'])}`;
    await pubsub.acquireLease(key, leaseOwner, 15_000);
    const subscription = await runtime.subscribeToThread(agent, target, pubsub);
    const iterator = subscription.stream[Symbol.asyncIterator]();

    try {
      await pubsub.publish(topic, {
        type: 'run-registered',
        runId,
        data: { type: 'run-registered', runId, streamId: 'remote-resume-stream-1', streamSeq: 1, leaseOwner },
      });
      await pubsub.publish(topic, {
        type: 'stream-part',
        runId,
        data: {
          type: 'stream-part',
          runId,
          streamId: 'remote-resume-stream-1',
          sourceId: 'remote-source-1',
          leaseOwner,
          part: { type: 'start' },
        },
      });
      await iterator.next();
      await pubsub.publish(topic, {
        type: 'run-suspended',
        runId,
        data: { type: 'run-suspended', runId, streamId: 'remote-resume-stream-1', leaseOwner },
      });

      await pubsub.publish(topic, {
        type: 'run-registered',
        runId,
        data: { type: 'run-registered', runId, streamId: 'remote-resume-stream-2', streamSeq: 1, leaseOwner },
      });
      await pubsub.publish(topic, {
        type: 'stream-part',
        runId,
        data: {
          type: 'stream-part',
          runId,
          streamId: 'remote-resume-stream-2',
          sourceId: 'remote-source-2',
          leaseOwner,
          part: { type: 'start' },
        },
      });
      await expect(
        withTimeout(iterator.next(), 'Timed out waiting for cross-runtime resumed stream'),
      ).resolves.toMatchObject({
        value: { type: 'start', runId },
      });
      await pubsub.publish(topic, {
        type: 'run-registered',
        runId,
        data: { type: 'run-registered', runId, streamId: 'unordered-stream', streamSeq: 0, leaseOwner },
      });
      await pubsub.publish(topic, {
        type: 'stream-part',
        runId,
        data: {
          type: 'stream-part',
          runId,
          streamId: 'unordered-stream',
          sourceId: 'delayed-source',
          leaseOwner,
          part: { type: 'start' },
        },
      });
      await pubsub.publish(topic, {
        type: 'stream-part',
        runId,
        data: {
          type: 'stream-part',
          runId,
          streamId: 'remote-resume-stream-2',
          sourceId: 'remote-source-2',
          leaseOwner,
          part: { type: 'finish' },
        },
      });
      await expect(
        withTimeout(iterator.next(), 'Delayed registration replaced the active resume'),
      ).resolves.toMatchObject({
        value: { type: 'finish', runId },
      });
      await pubsub.publish(topic, {
        type: 'run-completed',
        runId,
        data: { type: 'run-completed', runId, streamId: 'remote-resume-stream-2', leaseOwner },
      });
    } finally {
      subscription.unsubscribe();
      await iterator.return?.();
    }
  });

  it('does not resurrect a terminal remote stream from delayed registration or parts', async () => {
    const pubsub = new EventEmitterPubSub();
    const runtime = new AgentThreadStreamRuntime();
    const agent = { id: 'remote-abort-tombstone-agent' } as Agent<any, any, any, any>;
    const target = { threadId: 'remote-abort-tombstone-thread', resourceId: 'remote-abort-tombstone-user' };
    const topic = `agent.thread-stream.${encodeURIComponent(`${target.resourceId}\u0000${target.threadId}`)}`;
    const runId = 'remote-abort-tombstone-run';
    const streamId = 'remote-abort-tombstone-stream';
    const leaseOwner = `mastra-thread-owner:${JSON.stringify([runId, 'remote-source', 'attempt'])}`;
    await pubsub.acquireLease(`${target.resourceId}\u0000${target.threadId}`, leaseOwner, 15_000);
    const subscription = await runtime.subscribeToThread(agent, target, pubsub);
    const iterator = subscription.stream[Symbol.asyncIterator]();

    try {
      await pubsub.publish(topic, {
        type: 'run-registered',
        runId,
        data: { type: 'run-registered', runId, streamId, streamSeq: 1, leaseOwner },
      });
      await pubsub.publish(topic, {
        type: 'stream-part',
        runId,
        data: { type: 'stream-part', runId, streamId, sourceId: 'remote-source', leaseOwner, part: { type: 'start' } },
      });
      await expect(withTimeout(iterator.next(), 'Timed out waiting for remote tombstone start')).resolves.toMatchObject(
        {
          value: { type: 'start', runId },
        },
      );
      await pubsub.publish(topic, {
        type: 'run-aborted',
        runId,
        data: { type: 'run-aborted', runId, streamId, leaseOwner },
      });
      await expect(
        withTimeout(iterator.next(), 'Timed out waiting for remote tombstone abort', 1_000),
      ).resolves.toEqual({
        value: { type: 'abort', runId },
        done: false,
      });

      await pubsub.publish(topic, {
        type: 'run-registered',
        runId,
        data: { type: 'run-registered', runId, streamId, streamSeq: 1 },
      });
      await pubsub.publish(topic, {
        type: 'stream-part',
        runId,
        data: {
          type: 'stream-part',
          runId,
          streamId,
          sourceId: 'remote-source',
          leaseOwner,
          part: { type: 'text-delta', payload: { text: 'must not resurrect' } },
        },
      });
      await new Promise(resolve => setTimeout(resolve, 25));
      expect(subscription.activeRunId()).toBeNull();
    } finally {
      subscription.unsubscribe();
      await iterator.return?.();
    }
  });

  it('rejects spoofed-source lifecycle terminals and stale-owner established parts', async () => {
    const pubsub = new ControlledLeasePubSub();
    const runtime = new AgentThreadStreamRuntime();
    const agent = { id: 'forged-owner-agent' } as Agent<any, any, any, any>;
    const identityRunId = 'forged-owner-identity-run';
    const identityOptions = runtime.prepareRunOptions(
      {
        runId: identityRunId,
        memory: { resource: 'forged-owner-identity-user', thread: 'forged-owner-identity-thread' },
      } as any,
      pubsub,
    );
    await runtime.registerRun(
      agent,
      {
        runId: identityRunId,
        status: 'running',
        fullStream: (async function* () {
          yield { type: 'start', runId: identityRunId };
          yield { type: 'finish', runId: identityRunId };
        })(),
        _waitUntilFinished: async () => {},
      } as any,
      identityOptions,
      pubsub,
    );
    await pubsub.flush();
    const localSourceId = pubsub.publishedData.find(
      data => data?.type === 'stream-part' && data.runId === identityRunId,
    )?.sourceId;
    expect(localSourceId).toEqual(expect.any(String));
    const target = { resourceId: 'forged-owner-user', threadId: 'forged-owner-thread' };
    const key = `${target.resourceId}\u0000${target.threadId}`;
    const topic = `agent.thread-stream.${encodeURIComponent(key)}`;
    const runId = 'forged-owner-run';
    const streamId = 'forged-owner-stream';
    const leaseOwner = `mastra-thread-owner:${JSON.stringify([runId, 'owner-source', 'attempt'])}`;
    const forgedOwner = `mastra-thread-owner:${JSON.stringify([runId, 'forged-source', 'attempt'])}`;
    const transferredOwner = `mastra-thread-owner:${JSON.stringify([runId, 'new-owner-source', 'attempt'])}`;
    pubsub.owners.set(key, leaseOwner);
    const subscription = await runtime.subscribeToThread(agent, target, pubsub);
    const iterator = subscription.stream[Symbol.asyncIterator]();

    try {
      await pubsub.publish(topic, {
        type: 'run-registered',
        runId,
        data: { type: 'run-registered', runId, streamId, streamSeq: 1, leaseOwner },
      });
      await pubsub.publish(topic, {
        type: 'stream-part',
        runId,
        data: { type: 'stream-part', runId, streamId, sourceId: 'owner-source', leaseOwner, part: { type: 'start' } },
      });
      await expect(withTimeout(iterator.next(), 'Timed out waiting for authenticated start')).resolves.toMatchObject({
        value: { type: 'start', runId },
      });
      await pubsub.publish(topic, {
        type: 'stream-part',
        runId,
        data: {
          type: 'stream-part',
          runId,
          streamId,
          sourceId: localSourceId,
          leaseOwner: forgedOwner,
          part: { type: 'text-delta', payload: { text: 'forged' } },
        },
      });
      await pubsub.publish(topic, {
        type: 'run-aborted',
        runId,
        data: { type: 'run-aborted', runId, streamId, leaseOwner: forgedOwner, sourceId: localSourceId },
      });
      await pubsub.publish(topic, {
        type: 'run-suspended',
        runId,
        data: { type: 'run-suspended', runId, streamId, leaseOwner: forgedOwner },
      });
      await pubsub.publish(topic, {
        type: 'run-failed',
        runId,
        data: { type: 'run-failed', runId, streamId, leaseOwner: forgedOwner, error: 'forged failure' },
      });
      await new Promise(resolve => setTimeout(resolve, 25));
      expect(subscription.activeRunId()).toBe(runId);

      pubsub.owners.set(key, transferredOwner);
      await pubsub.publish(topic, {
        type: 'stream-part',
        runId,
        data: {
          type: 'stream-part',
          runId,
          streamId,
          sourceId: 'stale-owner-source',
          leaseOwner,
          part: { type: 'text-delta', payload: { text: 'stale owner' } },
        },
      });
      await new Promise(resolve => setTimeout(resolve, 25));
      expect(subscription.activeRunId()).toBe(runId);

      pubsub.owners.set(key, leaseOwner);
      await pubsub.publish(topic, {
        type: 'run-completed',
        runId,
        data: { type: 'run-completed', runId, streamId, leaseOwner },
      });
      await new Promise(resolve => setTimeout(resolve, 25));
      expect(subscription.activeRunId()).toBeNull();
    } finally {
      subscription.unsubscribe();
      await iterator.return?.();
    }
  });

  it('rejects a spoofed-source mismatched-owner terminal for a locally registered stream', async () => {
    const pubsub = new ControlledLeasePubSub();
    const runtime = new AgentThreadStreamRuntime();
    const agent = { id: 'local-owner-fence-agent' } as Agent<any, any, any, any>;
    const target = { resourceId: 'local-owner-fence-user', threadId: 'local-owner-fence-thread' };
    const key = `${target.resourceId}\u0000${target.threadId}`;
    const topic = `agent.thread-stream.${encodeURIComponent(key)}`;
    const runId = 'local-owner-fence-run';
    let finishRun!: () => void;
    const finished = new Promise<void>(resolve => (finishRun = resolve));
    const options = runtime.prepareRunOptions(
      { runId, memory: { resource: target.resourceId, thread: target.threadId } } as any,
      pubsub,
    );
    const subscription = await runtime.subscribeToThread(agent, target, pubsub);
    const iterator = subscription.stream[Symbol.asyncIterator]();

    try {
      const completion = runtime.registerRun(
        agent,
        {
          runId,
          status: 'running',
          fullStream: (async function* () {
            yield { type: 'start', runId };
            await finished;
            yield { type: 'finish', runId };
          })(),
          _waitUntilFinished: () => finished,
        } as any,
        options,
        pubsub,
      )!;
      await expect(withTimeout(iterator.next(), 'Timed out waiting for local owner start')).resolves.toMatchObject({
        value: { type: 'start', runId },
      });
      await pubsub.flush();
      const registration = pubsub.publishedData.find(data => data?.type === 'run-registered' && data.runId === runId)!;
      const localSourceId = pubsub.publishedData.find(
        data => data?.type === 'stream-part' && data.runId === runId,
      )?.sourceId;
      expect(localSourceId).toEqual(expect.any(String));
      await pubsub.publish(topic, {
        type: 'run-aborted',
        runId,
        data: {
          type: 'run-aborted',
          runId,
          streamId: registration.streamId,
          leaseOwner: `mastra-thread-owner:${JSON.stringify([runId, 'forged-local-source', 'attempt'])}`,
          sourceId: localSourceId,
        },
      });
      await pubsub.flush();
      await nextTick();
      expect(subscription.activeRunId()).toBe(runId);
      expect(options.abortSignal?.aborted).toBe(false);

      finishRun();
      await completion;
      await pubsub.flush();
      await waitForCondition(() => subscription.activeRunId() === null);
    } finally {
      subscription.unsubscribe();
      await iterator.return?.();
    }
  });

  it('bounds retained suspended-stream identities per run and across runs', async () => {
    const { rememberBoundedResumableTerminalStream } = await import('../thread-stream-runtime');
    const retained = new Map<string, Set<string>>();

    rememberBoundedResumableTerminalStream(retained, 'run-a', 'stream-a1', 2);
    rememberBoundedResumableTerminalStream(retained, 'run-a', 'stream-a2', 2);
    expect(rememberBoundedResumableTerminalStream(retained, 'run-a', 'stream-a3', 2)).toEqual(['stream-a1']);
    expect([...retained.get('run-a')!]).toEqual(['stream-a2', 'stream-a3']);

    rememberBoundedResumableTerminalStream(retained, 'run-b', 'stream-b1', 2);
    expect(rememberBoundedResumableTerminalStream(retained, 'run-c', 'stream-c1', 2)).toEqual([
      'stream-a2',
      'stream-a3',
    ]);
    expect([...retained.keys()]).toEqual(['run-b', 'run-c']);
    expect(retained.has('run-a')).toBe(false);
  });

  it('cancels an abort-ignoring subscriber stream after the bounded drain grace', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new ControlledLeasePubSub();
    const agent = { id: 'abort-drain-timeout-agent' } as Agent<any, any, any, any>;
    const threadId = 'abort-drain-timeout-thread';
    const resourceId = 'abort-drain-timeout-user';
    const runId = 'abort-drain-timeout-run';
    const finished = new Promise<void>(() => {});
    const output = {
      runId,
      status: 'running',
      fullStream: new ReadableStream({
        start(controller) {
          controller.enqueue({ type: 'start', runId });
          controller.enqueue({
            type: 'tool-call',
            runId,
            payload: { toolCallId: 'stuck-tool', toolName: 'lookup', args: {} },
          });
        },
      }),
      _waitUntilFinished: () => finished,
    } as any;
    const options = runtime.prepareRunOptions(
      { runId, memory: { thread: threadId, resource: resourceId } } as any,
      pubsub,
    );
    const subscription = await runtime.subscribeToThread(agent, { threadId, resourceId }, pubsub);
    const iterator = subscription.stream[Symbol.asyncIterator]();

    try {
      const completion = runtime.registerRun(agent, output, options, pubsub)!;
      void completion.catch(() => {});
      await iterator.next();
      await iterator.next();

      const outputDrain = subscription._waitForOutputDrain!(output)!;
      expect(runtime.abortRun(runId, pubsub)).toBe(true);
      const terminal = iterator.next();
      let settled = false;
      void terminal.finally(() => (settled = true));
      await new Promise(resolve => setTimeout(resolve, 25));
      expect(settled).toBe(false);
      await expect(withTimeout(terminal, 'Bounded abort drain did not settle', 1_000)).resolves.toEqual({
        value: { type: 'abort', runId },
        done: false,
      });
      // The producer never closes or settles. The runtime must still terminalize
      // the registered output after the bounded drain grace.
      await expect(withTimeout(outputDrain, 'Abort output drain did not settle', 1_000)).resolves.toBeUndefined();
      expect(runtime.getRunOutput(runId, pubsub)).toBeUndefined();
      expect(pubsub.publishedData.filter(data => data?.type === 'run-completed' && data.runId === runId)).toEqual([]);
      subscription.unsubscribe();
    } finally {
      subscription.unsubscribe();
    }
  });

  it('removes a failed terminal without a drain waiter before the same run id is reused', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new RejectFirstRunCompletedPubSub();
    const agent = { id: 'failed-terminal-reuse-agent' } as Agent<any, any, any, any>;
    const threadId = 'failed-terminal-reuse-thread';
    const resourceId = 'failed-terminal-reuse-user';
    const runId = 'failed-terminal-reuse-run';
    const target = { memory: { thread: threadId, resource: resourceId } } as any;
    const createOutput = (text: string) =>
      ({
        runId,
        status: 'running',
        fullStream: new ReadableStream({
          start(controller) {
            controller.enqueue({ type: 'start', runId });
            controller.enqueue({ type: 'text-delta', runId, payload: { id: 'text-1', text } });
            controller.enqueue({
              type: 'finish',
              runId,
              payload: { usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 }, finishReason: 'stop' },
            });
            controller.close();
          },
        }),
        _waitUntilFinished: () => Promise.resolve(),
      }) as any;
    const subscription = await runtime.subscribeToThread(agent, { threadId, resourceId }, pubsub);
    const iterator = subscription.stream[Symbol.asyncIterator]();

    try {
      const firstOutput = createOutput('first');
      const firstCompletion = runtime.registerRun(agent, firstOutput, target, pubsub)!;
      // Deliberately do not call `_waitForOutputDrain(firstOutput)`: terminal
      // rejection cleanup must be owned by the subscription itself.
      await expect(firstCompletion).rejects.toMatchObject({
        name: 'AgentThreadOutputDrainError',
        reason: 'terminal-publish-failed',
      });
      await expect(
        withTimeout(readNextRun(iterator), 'Timed out draining the failed first output'),
      ).resolves.toMatchObject({ value: { runId, text: 'first' } });

      runtime.reserveRun({ ...target, runId }, pubsub, agent.id);
      const secondOutput = createOutput('second');
      const secondCompletion = runtime.registerRun(agent, secondOutput, target, pubsub)!;
      const secondDrain = subscription._waitForOutputDrain!(secondOutput)!;
      const secondRun = readNextRun(iterator);

      await expect(secondCompletion).resolves.toBeUndefined();
      await expect(withTimeout(secondRun, 'Timed out draining the reused run id')).resolves.toMatchObject({
        value: { runId, text: 'second' },
      });
      // Advance the generator through the stream close so the second output's
      // own terminal can satisfy its drain barrier. A stale first-output entry
      // would leave this promise unresolved.
      const waitingForNextRun = iterator.next();
      await expect(withTimeout(secondDrain, 'Reused run id terminal was correlated to the wrong output')).resolves.toBe(
        undefined,
      );
      subscription.unsubscribe();
      await waitingForNextRun;
    } finally {
      subscription.unsubscribe();
    }
  });

  it('delivers each thread run to multiple same-runtime subscribers', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const agent = { id: 'multi-subscriber-thread-agent' } as Agent<any, any, any, any>;
    const threadId = 'multi-subscriber-thread';
    const resourceId = 'multi-subscriber-user';

    const registerRun = (runNumber: number) => {
      const runId = `multi-subscriber-run-${runNumber}`;
      let finish!: () => void;
      const finished = new Promise<void>(resolve => {
        finish = resolve;
      });
      const parts = [
        { type: 'start', runId },
        { type: 'text-start', runId, payload: { id: `text-${runNumber}` } },
        { type: 'text-delta', runId, payload: { id: `text-${runNumber}`, text: `response ${runNumber}` } },
        { type: 'text-end', runId, payload: { id: `text-${runNumber}` } },
        {
          type: 'finish',
          runId,
          payload: { usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 }, finishReason: 'stop' },
        },
      ];
      const fullStream = new ReadableStream({
        start(controller) {
          setTimeout(() => {
            for (const part of parts) controller.enqueue(part);
            controller.close();
            finish();
          }, 25);
        },
      });

      runtime.registerRun(
        agent,
        {
          runId,
          status: 'running',
          fullStream,
          _waitUntilFinished: () => finished,
        } as any,
        { memory: { thread: threadId, resource: resourceId } } as any,
      );
      return runId;
    };

    const firstSubscription = await runtime.subscribeToThread(agent, { threadId, resourceId });
    const secondSubscription = await runtime.subscribeToThread(agent, { threadId, resourceId });
    const firstIterator = firstSubscription.stream[Symbol.asyncIterator]();
    const secondIterator = secondSubscription.stream[Symbol.asyncIterator]();

    try {
      const firstSubscriberRun1 = readNextRun(firstIterator);
      const secondSubscriberRun1 = readNextRun(secondIterator);
      const runId1 = registerRun(1);

      const [run1a, run1b] = await Promise.all([
        withTimeout(firstSubscriberRun1, 'Timed out waiting for first subscriber to receive run 1'),
        withTimeout(secondSubscriberRun1, 'Timed out waiting for second subscriber to receive run 1'),
      ]);
      expect(run1a.value).toMatchObject({ runId: runId1, text: 'response 1' });
      expect(run1b.value).toMatchObject({ runId: runId1, text: 'response 1' });
      await waitForCondition(
        () => firstSubscription.activeRunId() === null && secondSubscription.activeRunId() === null,
      );

      const firstSubscriberRun2 = readNextRun(firstIterator);
      const secondSubscriberRun2 = readNextRun(secondIterator);
      const runId2 = registerRun(2);

      const [run2a, run2b] = await Promise.all([
        withTimeout(firstSubscriberRun2, 'Timed out waiting for first subscriber to receive run 2'),
        withTimeout(secondSubscriberRun2, 'Timed out waiting for second subscriber to receive run 2'),
      ]);
      expect(run2a.value).toMatchObject({ runId: runId2, text: 'response 2' });
      expect(run2b.value).toMatchObject({ runId: runId2, text: 'response 2' });
    } finally {
      firstSubscription.unsubscribe();
      secondSubscription.unsubscribe();
    }
  });

  it('keeps request context associated with the exact queued stream record', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const agent = { id: 'request-context-stream-agent' } as Agent<any, any, any, any>;
    const threadId = 'request-context-stream-thread';
    const resourceId = 'request-context-stream-user';
    const runId = 'shared-run-id';
    const firstContext = new RequestContext();
    firstContext.set('name', 'first-context');
    const secondContext = new RequestContext();
    secondContext.set('name', 'second-context');

    const subscription = await runtime.subscribeToThread(agent, { threadId, resourceId });
    const iterator = subscription.stream[Symbol.asyncIterator]();

    const registerCompletedRun = async (requestContext: RequestContext) => {
      let finish!: () => void;
      const finished = new Promise<void>(resolve => {
        finish = resolve;
      });
      const parts = [
        { type: 'start', runId },
        { type: 'finish', runId, payload: { finishReason: 'stop' } },
      ];
      const output = {
        runId,
        status: 'running',
        fullStream: new ReadableStream({
          pull(controller) {
            const part = parts.shift();
            if (part) {
              controller.enqueue(part);
            } else {
              controller.close();
              finish();
            }
          },
        }),
        _waitUntilFinished: () => finished,
      } as any;
      await runtime.registerRun(agent, output, {
        memory: { thread: threadId, resource: resourceId },
        requestContext,
      } as any);
      await nextTick();
    };

    try {
      await registerCompletedRun(firstContext);
      await registerCompletedRun(secondContext);

      const firstStart = await withTimeout(iterator.next(), 'Timed out waiting for first queued stream');
      expect(firstStart.value).toMatchObject({ type: 'start', runId });
      expect(subscription.__getCurrentRunRequestContext()).toBe(firstContext);
      await withTimeout(iterator.next(), 'Timed out waiting for first queued stream finish');

      const secondStart = await withTimeout(iterator.next(), 'Timed out waiting for second queued stream');
      expect(secondStart.value).toMatchObject({ type: 'start', runId });
      expect(subscription.__getCurrentRunRequestContext()).toBe(secondContext);
      await withTimeout(iterator.next(), 'Timed out waiting for second queued stream finish');
    } finally {
      subscription.unsubscribe();
    }
  });

  it('replays completed same-runtime runs without duplicating live local parts', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new RetainedAsyncCallbackPubSub();
    const agent = { id: 'retained-replay-agent' } as Agent<any, any, any, any>;
    const threadId = 'retained-replay-thread';
    const resourceId = 'retained-replay-user';

    const registerRun = (runId: string, text: string) => {
      let finish!: () => void;
      const finished = new Promise<void>(resolve => {
        finish = resolve;
      });
      const parts = [
        { type: 'start', runId },
        { type: 'text-delta', runId, payload: { id: 'text-1', text } },
        { type: 'finish', runId, payload: { finishReason: 'stop' } },
      ];
      const fullStream = new ReadableStream({
        start(controller) {
          setTimeout(() => {
            for (const part of parts) controller.enqueue(part);
            controller.close();
            finish();
          }, 10);
        },
      });
      runtime.registerRun(
        agent,
        { runId, status: 'running', fullStream, _waitUntilFinished: () => finished } as any,
        { memory: { thread: threadId, resource: resourceId } } as any,
        pubsub,
      );
      return parts;
    };

    const liveSubscription = await runtime.subscribeToThread(agent, { threadId, resourceId }, pubsub);
    const expected = registerRun('retained-run-1', 'first');
    const liveRun = await withTimeout(
      readNextRunWithParts(liveSubscription.stream[Symbol.asyncIterator]()),
      'Timed out waiting for live local run',
    );
    expect(liveRun.value.parts).toEqual(expected);
    liveSubscription.unsubscribe();

    await pubsub.flush();
    await nextTick();
    await pubsub.flush();

    const replaySubscription = await runtime.subscribeToThread(agent, { threadId, resourceId }, pubsub);
    const replayIterator = replaySubscription.stream[Symbol.asyncIterator]();
    try {
      const replayedRun = await withTimeout(
        readNextRunWithParts(replayIterator),
        'Timed out waiting for completed same-runtime replay',
      );
      expect(replayedRun.value.parts).toEqual(expected);

      const nextRunPromise = readNextRunWithParts(replayIterator);
      const nextExpected = registerRun('retained-run-2', 'second');
      const nextRun = await withTimeout(nextRunPromise, 'Timed out waiting for run after replay');
      expect(nextRun.value.parts).toEqual(nextExpected);
    } finally {
      replaySubscription.unsubscribe();
      await pubsub.flush();
      await nextTick();
      await pubsub.flush();
    }
  });

  it.each([
    [false, false],
    [false, true],
    [true, false],
    [true, true],
  ])(
    'keeps exclusion policies independent for live and replayed runs (remote: %s, booleans: %s)',
    async (remote, booleans) => {
      const owner = new AgentThreadStreamRuntime();
      const follower = remote ? new AgentThreadStreamRuntime() : owner;
      const pubsub = new RetainedAsyncCallbackPubSub();
      const agent = { id: 'excluded-replay-agent' } as Agent<any, any, any, any>;
      const identity = { threadId: 'excluded-replay-thread', resourceId: 'excluded-replay-user' };
      const runId = 'excluded-replay-run';
      const parts = [
        { type: 'start', runId },
        ...(['reactive', 'user', 'state', 'notification'] as const).map(type => ({
          ...createSignal({ type, contents: `${type} context` }).toDataPart(),
          runId,
        })),
        { type: 'data-signal', runId, data: { type: 'unknown', contents: 'keep unknown' } },
        { type: 'text-delta', runId, payload: { id: 'text', text: 'visible text' } },
        { type: 'finish', runId, payload: { finishReason: 'stop' } },
      ];
      const expectedFiltered = [parts[0], ...parts.slice(5)];
      const subscribe = (excluded: boolean) =>
        follower.subscribeToThread(
          agent,
          {
            ...identity,
            hideSignals: booleans
              ? excluded
              : excluded
                ? ['system-reminder', 'user-message', 'state', 'notification']
                : [],
          },
          pubsub,
        );
      const subscriptions = await Promise.all([subscribe(false), subscribe(true)]);
      try {
        const pendingRuns = subscriptions.map(subscription =>
          readNextRunWithParts(subscription.stream[Symbol.asyncIterator]()),
        );
        let finish!: () => void;
        const finished = new Promise<void>(resolve => {
          finish = resolve;
        });
        await owner.registerRun(
          agent,
          {
            runId,
            status: 'running',
            fullStream: new ReadableStream({
              start(controller) {
                for (const part of parts) controller.enqueue(part);
                controller.close();
                finish();
              },
            }),
            _waitUntilFinished: () => finished,
          } as any,
          { memory: { thread: identity.threadId, resource: identity.resourceId } },
          pubsub,
        );
        const [visible, filtered] = await withTimeout(Promise.all(pendingRuns), 'live filtered fanout stalled');
        expect(visible.value.parts).toEqual(parts);
        expect(filtered.value.parts).toEqual(expectedFiltered);
        subscriptions.forEach(subscription => subscription.unsubscribe());
        await pubsub.flush();
        await nextTick();
        await pubsub.flush();
        const replays = await Promise.all([subscribe(true), subscribe(false)]);
        subscriptions.push(...replays);
        const [filteredReplay, visibleReplay] = await withTimeout(
          Promise.all(replays.map(subscription => readNextRunWithParts(subscription.stream[Symbol.asyncIterator]()))),
          'filtered replay stalled',
        );
        expect(filteredReplay.value.parts).toEqual(expectedFiltered);
        expect(visibleReplay.value.parts).toEqual(parts);
      } finally {
        subscriptions.forEach(subscription => subscription.unsubscribe());
        await pubsub.flush();
        await nextTick();
        await pubsub.flush();
        owner.resetForTests();
        follower.resetForTests();
      }
    },
  );

  it('delivers resumed runs with the same run id to thread subscribers', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const agent = { id: 'resumed-thread-agent' } as Agent<any, any, any, any>;
    const threadId = 'resumed-thread';
    const resourceId = 'resumed-user';
    const runId = 'resumed-run';

    const createRun = (parts: any[]) => {
      let finish!: () => void;
      const finished = new Promise<void>(resolve => {
        finish = resolve;
      });
      const fullStream = new ReadableStream({
        start(controller) {
          setTimeout(() => {
            for (const part of parts) controller.enqueue(part);
            controller.close();
            finish();
          }, 5);
        },
      });

      runtime.registerRun(
        agent,
        {
          runId,
          status: 'running',
          fullStream,
          _waitUntilFinished: () => finished,
        } as any,
        { memory: { thread: threadId, resource: resourceId } } as any,
      );
    };

    const subscription = await runtime.subscribeToThread(agent, { threadId, resourceId });
    const iterator = subscription.stream[Symbol.asyncIterator]();

    try {
      createRun([
        { type: 'start', runId },
        {
          type: 'tool-call-suspended',
          runId,
          payload: { toolCallId: 'tool-call-1', toolName: 'testTool' },
        },
      ]);

      await withTimeout(iterator.next(), 'Timed out waiting for initial resumed-run start');
      const suspended = await withTimeout(iterator.next(), 'Timed out waiting for suspended chunk');
      expect(suspended.value).toMatchObject({ type: 'tool-call-suspended', runId });
      await waitForCondition(() => subscription.activeRunId() === null);

      const resumedRun = readNextRun(iterator);
      createRun([
        { type: 'start', runId },
        { type: 'text-start', runId, payload: { id: 'text-1' } },
        { type: 'text-delta', runId, payload: { id: 'text-1', text: 'approved response' } },
        { type: 'text-end', runId, payload: { id: 'text-1' } },
        {
          type: 'finish',
          runId,
          payload: { usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 }, finishReason: 'stop' },
        },
      ]);

      await expect(withTimeout(resumedRun, 'Timed out waiting for resumed run')).resolves.toMatchObject({
        value: { runId, text: 'approved response' },
      });
    } finally {
      subscription.unsubscribe();
    }
  });

  it('keeps subscriber streams open across tool-call finish boundaries until tool results arrive', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const agent = { id: 'tool-call-boundary-agent' } as Agent<any, any, any, any>;
    const threadId = 'tool-call-boundary-thread';
    const resourceId = 'tool-call-boundary-user';
    const runId = 'tool-call-boundary-run';
    let finish!: () => void;
    const finished = new Promise<void>(resolve => {
      finish = resolve;
    });
    const fullStream = new ReadableStream({
      start(controller) {
        controller.enqueue({ type: 'start', runId });
        controller.enqueue({ type: 'tool-call', runId, payload: { toolCallId: 'tool-1', toolName: 'testTool' } });
        controller.enqueue({
          type: 'finish',
          runId,
          payload: { finishReason: 'tool-calls', usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 } },
        });
        controller.enqueue({ type: 'tool-result', runId, payload: { toolCallId: 'tool-1', result: 'tool output' } });
        controller.enqueue({
          type: 'finish',
          runId,
          payload: { finishReason: 'stop', usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 } },
        });
        controller.close();
        finish();
      },
    });

    const subscription = await runtime.subscribeToThread(agent, { threadId, resourceId });
    const iterator = subscription.stream[Symbol.asyncIterator]();

    try {
      runtime.registerRun(
        agent,
        {
          runId,
          status: 'running',
          fullStream,
          _waitUntilFinished: () => finished,
        } as any,
        { memory: { thread: threadId, resource: resourceId } } as any,
      );

      await expect(withTimeout(iterator.next(), 'Timed out waiting for boundary start')).resolves.toMatchObject({
        value: { type: 'start', runId },
      });
      await expect(withTimeout(iterator.next(), 'Timed out waiting for boundary tool call')).resolves.toMatchObject({
        value: { type: 'tool-call', runId },
      });
      await expect(withTimeout(iterator.next(), 'Timed out waiting for tool-call finish')).resolves.toMatchObject({
        value: { type: 'finish', runId, payload: expect.objectContaining({ finishReason: 'tool-calls' }) },
      });
      await expect(withTimeout(iterator.next(), 'Timed out waiting for live tool result')).resolves.toMatchObject({
        value: { type: 'tool-result', runId, payload: expect.objectContaining({ toolCallId: 'tool-1' }) },
      });
      await expect(withTimeout(iterator.next(), 'Timed out waiting for final finish')).resolves.toMatchObject({
        value: { type: 'finish', runId, payload: expect.objectContaining({ finishReason: 'stop' }) },
      });
    } finally {
      subscription.unsubscribe();
    }
  });

  it('assigns a new stream identity to same-run registrations without stale cleanup clearing the active stream', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new BlockingRunCompletedPubSub();
    const agent = { id: 'stream-identity-agent' } as Agent<any, any, any, any>;
    const threadId = 'stream-identity-thread';
    const resourceId = 'stream-identity-resource';
    const runId = 'stream-identity-run';
    const topic = `agent.thread-stream.${encodeURIComponent(`${resourceId}\u0000${threadId}`)}`;
    const publishedEvents: any[] = [];
    await pubsub.subscribe(topic, async event => {
      publishedEvents.push(event.data);
    });

    const createRun = (text: string, finished: Promise<void>) => {
      const fullStream = new ReadableStream({
        start(controller) {
          controller.enqueue({ type: 'start', runId });
          controller.enqueue({ type: 'text-delta', runId, payload: { text } });
          controller.enqueue({ type: 'finish', runId, payload: { finishReason: 'stop' } });
          controller.close();
        },
      });

      const output = {
        runId,
        status: 'running',
        fullStream,
        _waitUntilFinished: () => finished,
      } as any;
      return {
        output,
        completion: runtime.registerRun(
          agent,
          output,
          { memory: { thread: threadId, resource: resourceId } } as any,
          pubsub,
        )!,
      };
    };

    let finishInitial!: () => void;
    const initialFinished = new Promise<void>(resolve => {
      finishInitial = resolve;
    });
    let finishResumed!: () => void;
    const resumedFinished = new Promise<void>(resolve => {
      finishResumed = resolve;
    });

    const subscription = await runtime.subscribeToThread(agent, { threadId, resourceId }, pubsub);
    const iterator = subscription.stream[Symbol.asyncIterator]();

    try {
      const initialRun = readNextRun(iterator);
      const initial = createRun('initial response', initialFinished);
      await expect(withTimeout(initialRun, 'Timed out waiting for initial stream identity run')).resolves.toMatchObject(
        {
          value: { runId, text: 'initial response' },
        },
      );
      expect(subscription.activeRunId()).toBe(runId);

      finishInitial();
      await waitForCondition(() => pubsub.sawRunCompleted);

      const resumedRun = readNextRun(iterator);
      const resumed = createRun('resumed response', resumedFinished);
      await expect(withTimeout(resumedRun, 'Timed out waiting for resumed stream identity run')).resolves.toMatchObject(
        {
          value: { runId, text: 'resumed response' },
        },
      );

      const registeredEvents = publishedEvents.filter(event => event?.type === 'run-registered');
      expect(registeredEvents).toHaveLength(2);
      expect(registeredEvents.map(event => event.runId)).toEqual([runId, runId]);
      expect(registeredEvents.map(event => event.streamSeq)).toEqual([1, 2]);
      expect(registeredEvents[0].streamId).toEqual(expect.any(String));
      expect(registeredEvents[1].streamId).toEqual(expect.any(String));
      expect(registeredEvents[1].streamId).not.toBe(registeredEvents[0].streamId);

      const initialOutputDrain = subscription._waitForOutputDrain!(initial.output);
      const resumedOutputDrain = subscription._waitForOutputDrain!(resumed.output);
      // The current output handle belongs to the resumed segment; its barrier
      // must not be satisfied by the delayed terminal for the initial stream.
      pubsub.unblockRunCompleted();
      await initial.completion;
      await nextTick();
      expect(subscription.activeRunId()).toBe(runId);
      let resumedDrainSettled = false;
      await expect(
        withTimeout(initialOutputDrain!, 'Timed out waiting for initial output drain'),
      ).resolves.toBeUndefined();
      void resumedOutputDrain?.then(() => {
        resumedDrainSettled = true;
      });
      await nextTick();
      expect(resumedDrainSettled).toBe(false);

      finishResumed();
      await resumed.completion;
      await expect(
        withTimeout(resumedOutputDrain!, 'Timed out waiting for resumed output drain'),
      ).resolves.toBeUndefined();
      expect(subscription.activeRunId()).toBeNull();
    } finally {
      subscription.unsubscribe();
    }
  });

  it('keeps multicast thread streams alive when one subscriber unsubscribes mid-run', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const agent = { id: 'subscriber-cancel-agent' } as Agent<any, any, any, any>;
    const threadId = 'subscriber-cancel-thread';
    const resourceId = 'subscriber-cancel-user';
    const runId = 'subscriber-cancel-run';
    let finish!: () => void;
    const finished = new Promise<void>(resolve => {
      finish = resolve;
    });
    const parts = [
      { type: 'start', runId },
      { type: 'text-start', runId, payload: { id: 'text-1' } },
      { type: 'text-delta', runId, payload: { id: 'text-1', text: 'still running' } },
      { type: 'text-end', runId, payload: { id: 'text-1' } },
      {
        type: 'finish',
        runId,
        payload: { usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 }, finishReason: 'stop' },
      },
    ];
    const fullStream = new ReadableStream({
      async start(controller) {
        for (const part of parts) {
          await new Promise(resolve => setTimeout(resolve, 5));
          controller.enqueue(part);
        }
        controller.close();
        finish();
      },
    });

    const firstSubscription = await runtime.subscribeToThread(agent, { threadId, resourceId });
    const secondSubscription = await runtime.subscribeToThread(agent, { threadId, resourceId });
    const firstIterator = firstSubscription.stream[Symbol.asyncIterator]();
    const secondIterator = secondSubscription.stream[Symbol.asyncIterator]();

    try {
      const secondRun = readNextRun(secondIterator);
      runtime.registerRun(
        agent,
        {
          runId,
          status: 'running',
          fullStream,
          _waitUntilFinished: () => finished,
        } as any,
        { memory: { thread: threadId, resource: resourceId } } as any,
      );

      const firstPart = await withTimeout(firstIterator.next(), 'Timed out waiting for first subscriber part');
      expect(firstPart.value).toMatchObject({ type: 'start', runId });
      await firstIterator.return?.();
      firstSubscription.unsubscribe();

      await expect(
        withTimeout(secondRun, 'Timed out waiting for second subscriber to finish run'),
      ).resolves.toMatchObject({
        value: { runId, text: 'still running' },
        done: false,
      });
    } finally {
      firstSubscription.unsubscribe();
      secondSubscription.unsubscribe();
    }
  });

  it('starts an idle thread run without cross-agent owner discovery when a user-message signal is sent', async () => {
    const pubsub = new ControlledLeasePubSub();
    const agent = new Agent({
      id: 'idle-signal-agent',
      name: 'Idle Signal Agent',
      instructions: 'Test',
      model: createTextStreamModel('signal response'),
      pubsub,
    });

    const subscription = await agent.subscribeToThread({
      threadId: 'idle-thread',
      resourceId: 'idle-user',
    });
    const nextRun = readNextRun(subscription.stream[Symbol.asyncIterator]());

    const signalResult = await agent.sendSignal(
      { type: 'user-message', contents: 'Hello from signal' },
      {
        resourceId: 'idle-user',
        threadId: 'idle-thread',
        ifIdle: { streamOptions: { memory: { resource: 'idle-user', thread: 'idle-thread' } } },
      },
    );

    const subscribedRun = await nextRun;
    await expect(signalResult.accepted).resolves.toMatchObject({ action: 'wake', runId: subscribedRun.value.runId });
    expect(pubsub.publishedData.some(data => data?.type === 'thread-owner-discovery')).toBe(false);
    expect(signalResult.signal.id).toBeDefined();
    expect(subscribedRun.value.text).toBe('signal response');

    subscription.unsubscribe();
  });

  it('wakes idle threads through the registered thread-runtime agent instead of the wrapped agent', async () => {
    // Durable wrappers that are not Agent subclasses (the Inngest Proxy) forward
    // sendSignal() to the wrapped agent. The runtime must still start the idle
    // run on the wrapper so the woken turn takes the durable path.
    const pubsub = new EventEmitterPubSub();
    const agent = new Agent({
      id: 'runtime-agent-wrapper',
      name: 'Runtime Agent Wrapper',
      instructions: 'Test',
      model: createTextStreamModel('wrapped response'),
      pubsub,
    });
    const wrapper = {
      id: agent.id,
      stream: vi.fn((...args: Parameters<Agent['stream']>) => agent.stream(...args)),
    };
    agent.__setThreadRuntimeAgent(wrapper as unknown as Agent<any, any, any, any>);

    const signalResult = await agent.sendSignal(
      { type: 'user-message', contents: 'Hello through the wrapper' },
      {
        resourceId: 'wrapper-user',
        threadId: 'wrapper-thread',
        ifIdle: { streamOptions: { memory: { resource: 'wrapper-user', thread: 'wrapper-thread' } } },
      },
    );

    const accepted = await signalResult.accepted;
    expect(accepted).toMatchObject({ action: 'wake' });
    if (accepted.action !== 'wake') throw new Error('Expected signal wake');
    expect(await accepted.output.text).toBe('wrapped response');
    expect(wrapper.stream).toHaveBeenCalledTimes(1);
    expect(wrapper.stream.mock.calls[0]?.[0]).toBe(signalResult.signal);
    expect(wrapper.stream.mock.calls[0]?.[1]).toMatchObject({ untilIdle: true, runId: accepted.runId });
  });

  it('wakes the claimed owner when the current runtime owns the thread claim', async () => {
    const pubsub = new EventEmitterPubSub();
    const agent = new Agent({
      id: 'local-owner-agent',
      name: 'Local Owner Agent',
      instructions: 'Test',
      model: createTextStreamModel('local owner response'),
      pubsub,
    });

    const subscription = await agent.subscribeToThread({
      resourceId: 'local-owner-user',
      threadId: 'local-owner-thread',
    });
    const nextRun = readNextRunWithParts(subscription.stream[Symbol.asyncIterator]());
    const claim = await agent.claimThreadOwnership({
      resourceId: 'local-owner-user',
      threadId: 'local-owner-thread',
      streamOptions: { memory: { resource: 'local-owner-user', thread: 'local-owner-thread' } },
    });
    expect(claim.claimed).toBe(true);

    const signalResult = await agent.sendSignal(
      { type: 'user-message', contents: 'wake local owner' },
      {
        resourceId: 'local-owner-user',
        threadId: 'local-owner-thread',
        ifIdle: {
          behavior: 'wake',
          streamOptions: { memory: { resource: 'local-owner-user', thread: 'local-owner-thread' } },
        },
      },
    );

    const subscribedRun = await withTimeout(nextRun, 'Timed out waiting for local owner run');
    // The claimed owner ran the turn in this process, so this is a `wake`, not a
    // `deliver`: `deliver` means no run started locally and the signal joined a
    // run that was already in flight.
    await expect(signalResult.accepted).resolves.toMatchObject({ action: 'wake', runId: subscribedRun.value.runId });
    expect(subscribedRun.value.text).toBe('local owner response');

    claim.unsubscribe();
    subscription.unsubscribe();
  });

  it('rejects a full logical message before local claimed-owner execution', async () => {
    const pubsub = new EventEmitterPubSub();
    const runtime = new AgentThreadStreamRuntime();
    const target = { resourceId: 'local-lineage-owner-user', threadId: 'local-lineage-owner-thread' };
    const stream = vi.fn(async () => ({
      runId: 'should-not-start',
      status: 'running',
      fullStream: (async function* () {})(),
      _waitUntilFinished: () => new Promise<void>(() => {}),
    }));
    const agent = { id: 'local-lineage-owner-agent', stream } as any;
    let idleSignalDiscarded = false;
    const claim = await runtime.claimThreadOwnership(
      agent,
      {
        ...target,
        streamOptions: { logicalMessageIdentity: { input: 'owner-input', response: 'owner-response' } },
      } as any,
      pubsub,
    );

    try {
      const result = runtime.sendSignal(
        agent,
        {
          id: 'local-lineage-owner-signal',
          type: 'user-message',
          contents: 'preserve response identity',
          metadata: { logicalMessageId: 'request-input' },
        } as any,
        {
          ...target,
          ifIdle: {
            behavior: 'wake',
            streamOptions: { logicalMessageIdentity: { input: 'request-input', response: 'request-response' } },
            _onThreadStreamSignalDiscarded: () => {
              idleSignalDiscarded = true;
            },
          },
        } as any,
        pubsub,
      );

      await expect(result.accepted).resolves.toEqual({ action: 'discard' });
      expect(idleSignalDiscarded).toBe(true);
      expect(stream).not.toHaveBeenCalled();
    } finally {
      claim.unsubscribe();
    }
  });

  it('does not cache a discarded full logical message during claimed-owner discovery', async () => {
    const pubsub = new EventEmitterPubSub();
    const ownerRuntime = new AgentThreadStreamRuntime();
    const senderRuntime = new AgentThreadStreamRuntime();
    const target = { resourceId: 'remote-lineage-owner-user', threadId: 'remote-lineage-owner-thread' };
    const ownerAgent = { id: 'remote-lineage-owner-agent', stream: vi.fn() } as any;
    const senderAgent = { id: 'remote-lineage-sender-agent' } as any;
    const claim = await ownerRuntime.claimThreadOwnership(ownerAgent, target, pubsub);
    const signal = {
      id: 'remote-lineage-owner-signal',
      type: 'user-message' as const,
      contents: 'preserve response identity',
    };
    const options = {
      ...target,
      ifIdle: {
        behavior: 'wake' as const,
        requireClaimedOwner: true,
        streamOptions: { logicalMessageIdentity: { input: 'remote-input', response: 'remote-response' } },
      },
    } as any;

    try {
      const first = senderRuntime.sendSignal(senderAgent, signal, options, pubsub);
      await expect(first.accepted).resolves.toEqual({ action: 'discard' });

      const retry = senderRuntime.sendSignal(senderAgent, signal, options, pubsub);
      expect(retry.runId).not.toBe(first.runId);
      expect(retry.accepted).not.toBe(first.accepted);
      await expect(retry.accepted).resolves.toEqual({ action: 'discard' });
    } finally {
      claim.unsubscribe();
      ownerRuntime.resetForTests();
      senderRuntime.resetForTests();
    }
  });

  it('runs a local claimed-owner wake through public request-context preflight', async () => {
    const pubsub = new EventEmitterPubSub();
    const requestContext = new RequestContext();
    requestContext.set('allowed', true);
    const agent = new Agent({
      id: 'local-owner-request-context-agent',
      name: 'Local Owner Request Context Agent',
      instructions: 'Test',
      requestContextSchema: z.object({ allowed: z.literal(true) }),
      model: createTextStreamModel('local owner request context response'),
      pubsub,
    });

    const target = {
      resourceId: 'local-owner-request-context-user',
      threadId: 'local-owner-request-context-thread',
    };
    const subscription = await agent.subscribeToThread(target);
    const nextRun = readNextRunWithParts(subscription.stream[Symbol.asyncIterator]());
    const claim = await agent.claimThreadOwnership({
      ...target,
      streamOptions: { memory: target, requestContext },
    });

    try {
      const signalResult = agent.sendSignal(
        { type: 'user-message', contents: 'wake local owner with request context' },
        { ...target, ifIdle: { behavior: 'wake' } },
      );

      const subscribedRun = await withTimeout(nextRun, 'Timed out waiting for request-context owner run');
      // Upstream #24347 (PF-4402 sync): a local claimed owner that ran the turn reports `wake`.
      await expect(signalResult.accepted).resolves.toMatchObject({
        action: 'wake',
        runId: subscribedRun.value.runId,
      });
      expect(subscribedRun.value.text).toBe('local owner request context response');
    } finally {
      claim.unsubscribe();
      subscription.unsubscribe();
    }
  });

  it('preserves a foreign prepared run when a claimed-owner wake loses a run-id collision', async () => {
    const pubsub = new EventEmitterPubSub();
    const ownerResourceId = 'foreign-collision-user';
    const ownerThreadId = 'foreign-collision-claimed-thread';
    const foreignThreadId = 'foreign-collision-thread';
    const runId = 'foreign-collision-run';
    let releaseOwnerOptions!: () => void;
    let markOwnerOptionsStarted!: () => void;
    const ownerOptionsGate = new Promise<void>(resolve => {
      releaseOwnerOptions = resolve;
    });
    const ownerOptionsStarted = new Promise<void>(resolve => {
      markOwnerOptionsStarted = resolve;
    });
    let releaseForeignModel!: () => void;
    let markForeignModelStarted!: () => void;
    const foreignModelGate = new Promise<void>(resolve => {
      releaseForeignModel = resolve;
    });
    const foreignModelStarted = new Promise<void>(resolve => {
      markForeignModelStarted = resolve;
    });
    let foreignAbortSignal: AbortSignal | undefined;
    const ownerAgent = new Agent({
      id: 'foreign-collision-owner',
      name: 'Foreign Collision Owner',
      instructions: 'Test',
      model: createTextStreamModel('owner response'),
      pubsub,
    });
    const foreignAgent = new Agent({
      id: 'foreign-collision-agent',
      name: 'Foreign Collision Agent',
      instructions: 'Test',
      model: new MockLanguageModelV2({
        doStream: async ({ abortSignal }) => {
          foreignAbortSignal = abortSignal;
          markForeignModelStarted();
          await foreignModelGate;
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: convertArrayToReadableStream([
              { type: 'stream-start', warnings: [] },
              { type: 'response-metadata', id: 'foreign-collision', modelId: 'mock-model-id', timestamp: new Date(0) },
              { type: 'text-start', id: 'text-1' },
              { type: 'text-delta', id: 'text-1', delta: 'foreign response' },
              { type: 'text-end', id: 'text-1' },
              {
                type: 'finish',
                finishReason: 'stop',
                usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
              },
            ]),
          };
        },
      }),
      pubsub,
    });
    const claim = await ownerAgent.claimThreadOwnership({
      resourceId: ownerResourceId,
      threadId: ownerThreadId,
      streamOptions: async () => {
        markOwnerOptionsStarted();
        await ownerOptionsGate;
        return { memory: { resource: ownerResourceId, thread: ownerThreadId } };
      },
    });
    let foreignStream: Promise<any> | undefined;

    try {
      const signalResult = ownerAgent.sendSignal(
        { id: 'foreign-collision-signal', type: 'user-message', contents: 'claimed wake' },
        {
          runId,
          resourceId: ownerResourceId,
          threadId: ownerThreadId,
          ifIdle: { behavior: 'wake' },
        },
      );
      await ownerOptionsStarted;

      foreignStream = foreignAgent.stream('foreign run', {
        runId,
        memory: { resource: ownerResourceId, thread: foreignThreadId },
      });
      await foreignModelStarted;

      releaseOwnerOptions();
      await expect(signalResult.accepted).rejects.toThrow('already reserved for another thread');
      expect(foreignAgent.getActiveThreadRunId({ resourceId: ownerResourceId, threadId: foreignThreadId })).toBe(runId);

      expect(agentThreadStreamRuntime.abortRun(runId, pubsub)).toBe(true);
      expect(foreignAbortSignal?.aborted).toBe(true);
    } finally {
      releaseOwnerOptions();
      releaseForeignModel();
      await foreignStream?.catch(() => {});
      claim.unsubscribe();
      agentThreadStreamRuntime.resetForTests();
    }
  });

  it('clears a failed claimed-owner discovery admission so the signal can be retried', async () => {
    const pubsub = new EventEmitterPubSub();
    const agent = new Agent({
      id: 'required-owner-retry-agent',
      name: 'Required Owner Retry Agent',
      instructions: 'Test',
      model: createTextStreamModel('must not run'),
      pubsub,
    });
    const signal = {
      id: 'required-owner-retry-signal',
      type: 'user-message' as const,
      contents: 'deliver remotely',
    };
    const target = {
      resourceId: 'required-owner-retry-resource',
      threadId: 'required-owner-retry-thread',
      ifIdle: { behavior: 'wake' as const, requireClaimedOwner: true },
    };

    const first = agent.sendSignal(signal, target);
    await expect(first.accepted).rejects.toThrow('No claimed thread owner responded');

    const retry = agent.sendSignal(signal, target);
    await expect(retry.accepted).rejects.toThrow('No claimed thread owner responded');
    expect(retry.runId).not.toBe(first.runId);
    expect(agent.getActiveThreadRunId(target)).toBe(undefined);
  });

  it('transfers a finishing lease before starting a local claimed-owner wake', async () => {
    const pubsub = new ControlledLeasePubSub();
    const ownerRuntime = agentThreadStreamRuntime;
    const ownerAgent = new Agent({
      id: 'finishing-local-owner',
      name: 'Finishing Local Owner',
      instructions: 'Test',
      model: createTextStreamModel('finishing handoff response'),
      pubsub,
    });
    const target = {
      resourceId: 'finishing-local-user',
      threadId: 'finishing-local-thread',
    };
    const key = `${target.resourceId}\u0000${target.threadId}`;
    const finishingRunId = 'finishing-local-run';
    let finishRun!: () => void;
    const finishing = new Promise<void>(resolve => {
      finishRun = resolve;
    });
    const finishingOutput = {
      runId: finishingRunId,
      status: 'success',
      fullStream: new ReadableStream({
        start(controller) {
          controller.close();
        },
      }),
      _waitUntilFinished: () => finishing,
    } as any;
    const subscription = await ownerRuntime.subscribeToThread(ownerAgent, target, pubsub);
    const nextRun = readNextRunWithParts(subscription.stream[Symbol.asyncIterator]());

    try {
      const completion = ownerRuntime.registerRun(
        ownerAgent,
        finishingOutput,
        { memory: { resource: target.resourceId, thread: target.threadId } },
        pubsub,
      )!;
      await waitForCondition(() => ownerRuntime.getRunOutput(finishingRunId, pubsub) !== undefined);
      await waitForCondition(() => pubsub.owners.has(key));

      const claim = await ownerRuntime.claimThreadOwnership(ownerAgent, target, pubsub);
      try {
        const signalResult = ownerAgent.sendSignal(
          { type: 'user-message', contents: 'wake after finishing' },
          { ...target, ifIdle: { behavior: 'wake', requireClaimedOwner: true } },
        );

        // Upstream #24347 (PF-4402 sync): a local claimed owner that ran the turn reports `wake`.
        await expect(signalResult.accepted).resolves.toMatchObject({ action: 'wake' });
        const subscribedRun = await withTimeout(nextRun, 'Timed out waiting for finishing lease handoff');
        expect(subscribedRun.value.text).toBe('finishing handoff response');
        expect(subscribedRun.value.runId).toBe(signalResult.runId);
      } finally {
        claim.unsubscribe();
      }

      finishRun();
      await withTimeout(completion, 'Timed out finishing the held run');
    } finally {
      finishRun();
      subscription.unsubscribe();
    }
  });

  it('acknowledges filtered and handled claimed-owner callbacks and leaves failed discovery publication unacked', async () => {
    const pubsub = new ClaimedOwnerAckPubSub();
    const runtime = agentThreadStreamRuntime;
    const ownerAgent = new Agent({
      id: 'callback-ack-owner',
      name: 'Callback Ack Owner',
      instructions: 'Test',
      model: createTextStreamModel('callback ack response'),
      pubsub,
    });
    const resourceId = 'callback-ack-user';
    const threadId = 'callback-ack-thread';
    const key = `${resourceId}\u0000${threadId}`;
    const threadTopic = `agent.thread-stream.${encodeURIComponent(key)}`;
    const ownerDiscoveryTopic = 'agent.thread-owner-discovery';
    const peerDiscoveryTopic = 'agent.thread-peer-discovery';
    const target = { resourceId, threadId };
    const claim = await runtime.claimThreadOwnership(ownerAgent, target, pubsub);

    try {
      const ownerReplyTopic = 'callback-ack-owner-reply';
      await pubsub.publish(ownerDiscoveryTopic, {
        type: ownerDiscoveryTopic,
        runId: 'callback-ack-owner-discovery',
        data: {
          type: 'thread-owner-request',
          key,
          requestId: 'callback-ack-owner-discovery',
          replyTopic: ownerReplyTopic,
          sourceId: 'remote-source',
        },
      });
      const ownerResponse = pubsub.published.find(record => record.topic === ownerReplyTopic)?.event.data;
      const ownerSourceId = ownerResponse?.sourceId;
      expect(ownerSourceId).toBeDefined();

      await pubsub.publish(threadTopic, {
        type: threadTopic,
        runId: 'callback-ack-filtered-thread',
        data: { type: 'stream-part', runId: 'callback-ack-filtered-thread' },
      });
      await pubsub.publish(ownerDiscoveryTopic, {
        type: ownerDiscoveryTopic,
        runId: 'callback-ack-filtered-owner',
        data: {
          type: 'thread-owner-request',
          key,
          requestId: 'callback-ack-filtered-owner',
          replyTopic: 'callback-ack-filtered-owner-reply',
          sourceId: ownerSourceId,
        },
      });
      await pubsub.publish(peerDiscoveryTopic, {
        type: peerDiscoveryTopic,
        runId: 'callback-ack-filtered-peer',
        data: {
          type: 'thread-peer-request',
          requestId: 'callback-ack-filtered-peer',
          replyTopic: 'callback-ack-filtered-peer-reply',
          sourceId: ownerSourceId,
        },
      });

      await pubsub.publish(threadTopic, {
        type: threadTopic,
        runId: 'callback-ack-handled-thread',
        data: {
          type: 'idle-signal-enqueued',
          runId: 'callback-ack-handled-thread',
          signal: { id: 'callback-ack-signal', type: 'user-message', contents: 'wake owner' },
          sourceId: 'remote-source',
          requestId: 'callback-ack-handled-thread',
          replyTopic: 'callback-ack-handled-thread-reply',
          targetSourceId: ownerSourceId,
          timeoutMs: 5_000,
        },
      });
      // The claim itself holds a separate `thread-claim:` lease (upstream #24898);
      // only the execution lease must be released once the run finishes.
      await waitForCondition(() => !pubsub.owners.has(key), 2_000);

      await pubsub.publish(peerDiscoveryTopic, {
        type: peerDiscoveryTopic,
        runId: 'callback-ack-handled-peer',
        data: {
          type: 'thread-peer-request',
          requestId: 'callback-ack-handled-peer',
          replyTopic: 'callback-ack-handled-peer-reply',
          sourceId: 'remote-source',
        },
      });

      expect(pubsub.acked).toEqual(
        expect.arrayContaining(['thread-owner-request', 'thread-peer-request', 'stream-part', 'idle-signal-enqueued']),
      );

      const ackedBeforeFailure = pubsub.acked.length;
      pubsub.rejectDataTypes.add('thread-owner-response');
      await expect(
        pubsub.publish(ownerDiscoveryTopic, {
          type: ownerDiscoveryTopic,
          runId: 'callback-ack-failed-owner',
          data: {
            type: 'thread-owner-request',
            key,
            requestId: 'callback-ack-failed-owner',
            replyTopic: 'callback-ack-failed-owner-reply',
            sourceId: 'remote-source',
          },
        }),
      ).rejects.toThrow('injected publication failure');
      expect(pubsub.acked).toHaveLength(ackedBeforeFailure);
      expect(pubsub.nacked).toContain('thread-owner-request');
    } finally {
      claim.unsubscribe();
    }
  });

  it('honors the incoming request context when waking a locally claimed thread owner', async () => {
    // A claimed owner's stream options belong to whichever run claimed the
    // thread, so they do not carry the context of every later wake. A wake that
    // brings its own request context — a dispatcher starting a turn on behalf of
    // an authenticated caller — has to have it applied to the woken run, or the
    // run starts anonymously and downstream resolution rejects the caller.
    const pubsub = new EventEmitterPubSub();
    const runtime = new AgentThreadStreamRuntime();
    const ownerAgent = {
      id: 'context-owner',
      stream: vi.fn(async () => ({})),
    } as unknown as Agent;
    const senderAgent = new Agent({
      id: 'context-sender',
      name: 'Context Sender',
      instructions: 'Test',
      model: createTextStreamModel('sender response'),
      pubsub,
    });
    const claim = await runtime.claimThreadOwnership(
      ownerAgent,
      {
        resourceId: 'context-user',
        threadId: 'context-thread',
        streamOptions: { memory: { resource: 'context-user', thread: 'context-thread' } },
      },
      pubsub,
    );
    expect(claim.claimed).toBe(true);
    const requestContext = new RequestContext();
    requestContext.set('caller', { organizationId: 'context-org' });

    const signalResult = runtime.sendSignal(
      senderAgent,
      { type: 'user-message', contents: 'wake with context' },
      {
        resourceId: 'context-user',
        threadId: 'context-thread',
        ifIdle: {
          behavior: 'wake',
          requireClaimedOwner: true,
          streamOptions: { requestContext },
        },
      },
      pubsub,
    );

    const accepted = await signalResult.accepted;
    expect(accepted).toMatchObject({ action: 'wake' });
    if (accepted.action !== 'wake') throw new Error('Expected signal wake');
    expect(accepted.output).toBeDefined();
    expect(ownerAgent.stream).toHaveBeenCalledTimes(1);
    expect(ownerAgent.stream.mock.calls[0]?.[1]).toMatchObject({ requestContext });
    // Options the claim itself contributed must survive the merge.
    expect(ownerAgent.stream.mock.calls[0]?.[1]?.memory).toEqual({
      resource: 'context-user',
      thread: 'context-thread',
    });

    claim.unsubscribe();
  });

  it('routes idle signals to the claimed thread owner runtime', async () => {
    const pubsub = new EventEmitterPubSub();
    const ownerRuntime = agentThreadStreamRuntime;
    const senderRuntime = new AgentThreadStreamRuntime();
    const ownerAgent = new Agent({
      id: 'owner-agent',
      name: 'Owner Agent',
      instructions: 'Test',
      model: createTextStreamModel('owner response'),
      pubsub,
    });
    const senderAgent = new Agent({
      id: 'owner-sender',
      name: 'Owner Sender',
      instructions: 'Test',
      model: createTextStreamModel('sender response'),
      pubsub,
    });

    const subscription = await ownerRuntime.subscribeToThread(
      ownerAgent,
      {
        resourceId: 'owner-user',
        threadId: 'owner-thread',
      },
      pubsub,
    );
    const nextRun = readNextRunWithParts(subscription.stream[Symbol.asyncIterator]());
    const claim = await ownerRuntime.claimThreadOwnership(
      ownerAgent,
      {
        resourceId: 'owner-user',
        threadId: 'owner-thread',
        streamOptions: { memory: { resource: 'owner-user', thread: 'owner-thread' } },
      },
      pubsub,
    );
    expect(claim.claimed).toBe(true);

    const signalResult = senderRuntime.sendSignal(
      senderAgent,
      { type: 'user-message', contents: 'wake the owner' },
      {
        resourceId: 'owner-user',
        threadId: 'owner-thread',
        ifIdle: { behavior: 'wake', requireClaimedOwner: true },
      },
      pubsub,
    );

    await expect(signalResult.accepted).resolves.toMatchObject({ action: 'deliver' });
    const subscribedRun = await nextRun;
    expect(subscribedRun.value.text).toBe('owner response');

    claim.unsubscribe();
    subscription.unsubscribe();
  });

  it('routes concurrent idle signals that arrive during claimed-owner discovery', async () => {
    const pubsub = new ControlledLeasePubSub();
    const ownerRuntime = agentThreadStreamRuntime;
    const senderRuntime = new AgentThreadStreamRuntime();
    const ownerAgent = new Agent({
      id: 'concurrent-discovery-owner',
      name: 'Concurrent Discovery Owner',
      instructions: 'Test',
      model: createTextStreamModel('concurrent owner response'),
      pubsub,
    });
    const senderAgent = new Agent({
      id: 'concurrent-discovery-sender',
      name: 'Concurrent Discovery Sender',
      instructions: 'Test',
      model: createTextStreamModel('sender response'),
      pubsub,
    });
    const subscription = await ownerRuntime.subscribeToThread(
      ownerAgent,
      { resourceId: 'concurrent-discovery-user', threadId: 'concurrent-discovery-thread' },
      pubsub,
    );
    const iterator = subscription.stream[Symbol.asyncIterator]();
    const firstRun = readNextRunWithParts(iterator);
    const claim = await ownerRuntime.claimThreadOwnership(
      ownerAgent,
      { resourceId: 'concurrent-discovery-user', threadId: 'concurrent-discovery-thread' },
      pubsub,
    );

    try {
      const firstSignal = senderRuntime.sendSignal(
        senderAgent,
        { type: 'user-message', contents: 'first concurrent signal' },
        {
          resourceId: 'concurrent-discovery-user',
          threadId: 'concurrent-discovery-thread',
          ifIdle: { behavior: 'wake', requireClaimedOwner: true },
        },
        pubsub,
      );
      const secondSignal = senderRuntime.sendSignal(
        senderAgent,
        { id: 'second-concurrent-signal', type: 'user-message', contents: 'second concurrent signal' },
        {
          resourceId: 'concurrent-discovery-user',
          threadId: 'concurrent-discovery-thread',
          ifIdle: { behavior: 'wake' },
        },
        pubsub,
      );
      const duplicateSignal = senderRuntime.sendSignal(
        senderAgent,
        { id: 'second-concurrent-signal', type: 'user-message', contents: 'second concurrent signal' },
        {
          resourceId: 'concurrent-discovery-user',
          threadId: 'concurrent-discovery-thread',
          ifIdle: { behavior: 'wake' },
        },
        pubsub,
      );

      const [firstAccepted, secondAccepted, duplicateAccepted] = await Promise.all([
        firstSignal.accepted,
        secondSignal.accepted,
        duplicateSignal.accepted,
      ]);
      expect(firstAccepted).toMatchObject({ action: 'deliver', runId: firstSignal.runId });
      expect(secondAccepted).toMatchObject({ action: 'deliver', runId: secondSignal.runId });
      expect(duplicateAccepted).toEqual(secondAccepted);
      expect(duplicateSignal.runId).toBe(secondSignal.runId);
      expect(firstSignal.runId).not.toBe(secondSignal.runId);
      const deliveredRuns = [
        await firstRun,
        await withTimeout(readNextRunWithParts(iterator), 'Timed out waiting for concurrent owner run'),
      ];
      expect(deliveredRuns.map(run => run.value.text)).toEqual([
        'concurrent owner response',
        'concurrent owner response',
      ]);
      expect(
        deliveredRuns.map(run => run.value.parts.find((part: any) => part.type === 'data-user-message')?.data.contents),
      ).toEqual(expect.arrayContaining(['first concurrent signal', 'second concurrent signal']));
    } finally {
      claim.unsubscribe();
      subscription.unsubscribe();
    }
  });

  it('acknowledges remote claimed-owner delivery only after stream admission', async () => {
    const pubsub = new ControlledLeasePubSub();
    const ownerRuntime = new AgentThreadStreamRuntime();
    const senderRuntime = new AgentThreadStreamRuntime();
    let releaseAdmission!: () => void;
    let markAdmissionStarted!: () => void;
    const admissionGate = new Promise<void>(resolve => {
      releaseAdmission = resolve;
    });
    const admissionStarted = new Promise<void>(resolve => {
      markAdmissionStarted = resolve;
    });
    const ownerAgent = {
      id: 'admission-owner',
      stream: vi.fn(async () => {
        markAdmissionStarted();
        await admissionGate;
        return {};
      }),
    } as unknown as Agent;
    const senderAgent = new Agent({
      id: 'admission-sender',
      name: 'Admission Sender',
      instructions: 'Test',
      model: createTextStreamModel('sender response'),
      pubsub,
    });
    const claim = await ownerRuntime.claimThreadOwnership(
      ownerAgent,
      { resourceId: 'admission-user', threadId: 'admission-thread' },
      pubsub,
    );

    const signalResult = senderRuntime.sendSignal(
      senderAgent,
      { type: 'user-message', contents: 'wait for admission' },
      {
        resourceId: 'admission-user',
        threadId: 'admission-thread',
        ifIdle: { behavior: 'wake', requireClaimedOwner: true },
      },
      pubsub,
    );
    let settled = false;
    void signalResult.accepted.finally(() => {
      settled = true;
    });

    await admissionStarted;
    await Promise.resolve();
    expect(settled).toBe(false);
    releaseAdmission();
    await expect(signalResult.accepted).resolves.toMatchObject({ action: 'deliver' });

    claim.unsubscribe();
  });

  it('does not enqueue a signal after the acceptance subscription times out before registration', async () => {
    vi.useFakeTimers();
    const pubsub = new DelayedRegistrationPubSub();
    const ownerRuntime = new AgentThreadStreamRuntime();
    const senderRuntime = new AgentThreadStreamRuntime();
    const ownerStream = vi.fn(async () => ({}));
    const ownerAgent = { id: 'late-acceptance-owner', stream: ownerStream } as unknown as Agent;
    const senderAgent = { id: 'late-acceptance-sender' } as unknown as Agent;
    const ownerClaimPromise = ownerRuntime.claimThreadOwnership(
      ownerAgent,
      { resourceId: 'late-acceptance-user', threadId: 'late-acceptance-thread', peer: false },
      pubsub,
    );
    let claim: { claimed: boolean; unsubscribe: () => void } | undefined;
    const barrier = pubsub.delayNextSubscription(topic => topic.includes('.idle-acceptance.'));

    try {
      await vi.advanceTimersByTimeAsync(100);
      claim = await ownerClaimPromise;
      expect(claim.claimed).toBe(true);

      const signalResult = senderRuntime.sendSignal(
        senderAgent,
        { type: 'user-message', contents: 'late acceptance' },
        {
          resourceId: 'late-acceptance-user',
          threadId: 'late-acceptance-thread',
          ifIdle: { behavior: 'wake', requireClaimedOwner: true },
        },
        pubsub,
      );

      await vi.advanceTimersByTimeAsync(100);
      await barrier.started;
      await vi.advanceTimersByTimeAsync(5_000);
      await expect(signalResult.accepted).rejects.toThrow('Claimed thread owner did not accept signal');

      barrier.release();
      await barrier.registered;
      await Promise.resolve();
      await Promise.resolve();

      expect(pubsub.published.some(({ event }) => event.data?.type === 'idle-signal-enqueued')).toBe(false);
      expect(pubsub.activeSubscriptionCount(barrier.topic!)).toBe(0);
      expect(ownerStream).not.toHaveBeenCalled();
    } finally {
      barrier.release();
      if (barrier.topic) await barrier.registered;
      claim?.unsubscribe();
      vi.useRealTimers();
    }
  });

  it.each([true, false])('clears a %s claimed-owner pre-stream failure for retry', async local => {
    const pubsub = new ControlledLeasePubSub();
    const ownerRuntime = local ? agentThreadStreamRuntime : new AgentThreadStreamRuntime();
    const senderRuntime = local ? ownerRuntime : new AgentThreadStreamRuntime();
    let streamOptionsCalls = 0;
    const ownerAgent = {
      id: local ? 'local-rejected-owner' : 'remote-rejected-owner',
      stream: vi.fn(async () => ({})),
    } as unknown as Agent;
    const senderAgent = { id: local ? 'local-rejected-sender' : 'remote-rejected-sender' } as unknown as Agent;
    const resourceId = local ? 'local-rejected-user' : 'remote-rejected-user';
    const threadId = local ? 'local-rejected-thread' : 'remote-rejected-thread';
    const claim = await ownerRuntime.claimThreadOwnership(
      ownerAgent,
      {
        resourceId,
        threadId,
        streamOptions: async () => {
          streamOptionsCalls += 1;
          if (streamOptionsCalls === 1) throw new Error('stream options failed');
          return {};
        },
      },
      pubsub,
    );

    const signal = {
      id: `${local ? 'local' : 'remote'}-rejected-signal`,
      type: 'user-message' as const,
      contents: 'retry',
    };
    const target = {
      resourceId,
      threadId,
      ifIdle: { behavior: 'wake' as const, requireClaimedOwner: true },
    };
    const signalResult = senderRuntime.sendSignal(senderAgent, signal, target, pubsub);

    await expect(signalResult.accepted).rejects.toThrow('stream options failed');
    expect(ownerAgent.stream).not.toHaveBeenCalled();

    const retry = senderRuntime.sendSignal(senderAgent, signal, target, pubsub);
    // Upstream #24347 (PF-4402 sync): a local claimed owner that ran the turn
    // reports `wake`; a remote owner still answers `deliver`.
    await expect(retry.accepted).resolves.toMatchObject({ action: local ? 'wake' : 'deliver', runId: retry.runId });
    expect(retry.runId).not.toBe(signalResult.runId);
    expect(streamOptionsCalls).toBe(2);
    expect(ownerAgent.stream).toHaveBeenCalledTimes(1);

    claim.unsubscribe();
  });

  it('retains an admitted signal receipt when its broker event is redelivered before acceptance', async () => {
    const pubsub = new ControlledLeasePubSub();
    const ownerRuntime = new AgentThreadStreamRuntime();
    const senderRuntime = new AgentThreadStreamRuntime();
    const resourceId = 'redelivered-claimed-owner-user';
    const threadId = 'redelivered-claimed-owner-thread';
    const target = { resourceId, threadId };
    const topic = `agent.thread-stream.${encodeURIComponent(`${resourceId}\u0000${threadId}`)}`;
    let releaseStream!: () => void;
    const streamGate = new Promise<void>(resolve => {
      releaseStream = resolve;
    });
    let markRegistered!: () => void;
    const registered = new Promise<void>(resolve => {
      markRegistered = resolve;
    });
    let finishRun!: () => void;
    const finished = new Promise<void>(resolve => {
      finishRun = resolve;
    });
    let completion: Promise<void> | undefined;
    const ownerAgent = {
      id: 'redelivered-claimed-owner',
      stream: vi.fn(async (_signal: unknown, options: any) => {
        const output = { ...createFakeThreadRun(options.runId, finished), status: 'success' };
        completion = ownerRuntime.registerRun(ownerAgent as any, output, options, pubsub) as Promise<void> | undefined;
        markRegistered();
        await streamGate;
        return output;
      }),
    } as unknown as Agent;
    const senderAgent = { id: 'redelivered-claimed-sender' } as unknown as Agent;
    const claim = await ownerRuntime.claimThreadOwnership(ownerAgent, { ...target, streamOptions: {} }, pubsub);

    try {
      const signal = {
        id: 'redelivered-claimed-signal',
        type: 'user-message' as const,
        contents: 'execute once despite redelivery',
      };
      const first = senderRuntime.sendSignal(
        senderAgent,
        signal,
        { ...target, ifIdle: { behavior: 'wake', requireClaimedOwner: true } },
        pubsub,
      );

      await registered;
      await waitForCondition(() => pubsub.publishedData.some(data => data?.type === 'idle-signal-enqueued'));
      const idleSignal = pubsub.publishedData.find(data => data?.type === 'idle-signal-enqueued');
      expect(idleSignal).toBeDefined();
      await pubsub.publish(topic, {
        type: 'idle-signal-enqueued',
        runId: idleSignal.runId,
        data: idleSignal,
      });
      // Upstream #24467 (PF-4402 sync): the owner remembers handled idle
      // requests by requestId, so a redelivery that lands before acceptance is
      // acknowledged without acting on the signal again or answering twice.
      await new Promise(resolve => setTimeout(resolve, 20));
      expect(
        pubsub.publishedData.some(
          data => data?.type === 'idle-signal-rejected' && data.requestId === idleSignal.requestId,
        ),
      ).toBe(false);

      releaseStream();
      await expect(first.accepted).resolves.toMatchObject({ runId: first.runId });
      const retry = senderRuntime.sendSignal(
        senderAgent,
        signal,
        { ...target, ifIdle: { behavior: 'wake', requireClaimedOwner: true } },
        pubsub,
      );
      await expect(retry.accepted).resolves.toMatchObject({ action: 'deliver', runId: first.runId });
      expect(retry.runId).toBe(first.runId);
      expect(ownerAgent.stream).toHaveBeenCalledTimes(1);
    } finally {
      releaseStream();
      finishRun();
      await completion;
      claim.unsubscribe();
    }
  });

  it('refreshes claimed-owner identity after asynchronous stream options resolve', async () => {
    const pubsub = new ControlledLeasePubSub();
    const ownerRuntime = new AgentThreadStreamRuntime();
    const senderRuntime = new AgentThreadStreamRuntime();
    const secondSenderRuntime = new AgentThreadStreamRuntime();
    const resourceId = 'async-options-claimed-owner-user';
    const threadId = 'async-options-claimed-owner-thread';
    const target = { resourceId, threadId };
    let releaseOptions!: () => void;
    const optionsGate = new Promise<void>(resolve => {
      releaseOptions = resolve;
    });
    let markOptionsStarted!: () => void;
    const optionsStarted = new Promise<void>(resolve => {
      markOptionsStarted = resolve;
    });
    let releaseStream!: () => void;
    const streamGate = new Promise<void>(resolve => {
      releaseStream = resolve;
    });
    let markStreamStarted!: () => void;
    const streamStarted = new Promise<void>(resolve => {
      markStreamStarted = resolve;
    });
    const finished = new Promise<void>(() => {});
    const ownerAgent = {
      id: 'async-options-claimed-owner',
      stream: vi.fn(async (_signal: unknown, options: any) => {
        const output = { ...createFakeThreadRun(options.runId, finished), status: 'running' };
        void ownerRuntime.registerRun(ownerAgent as any, output, options, pubsub)?.catch(() => {});
        markStreamStarted();
        await streamGate;
        return output;
      }),
    } as unknown as Agent;
    let optionsCalls = 0;
    const claim = await ownerRuntime.claimThreadOwnership(
      ownerAgent,
      {
        ...target,
        streamOptions: async () => {
          optionsCalls += 1;
          if (optionsCalls === 1) {
            markOptionsStarted();
            await optionsGate;
          }
          return {};
        },
      },
      pubsub,
    );

    try {
      const first = senderRuntime.sendSignal(
        { id: 'async-options-first-sender' } as unknown as Agent,
        { id: 'async-options-first-signal', type: 'user-message', contents: 'first' },
        { ...target, ifIdle: { behavior: 'wake', requireClaimedOwner: true } },
        pubsub,
      );
      await optionsStarted;

      const second = secondSenderRuntime.sendSignal(
        { id: 'async-options-second-sender' } as unknown as Agent,
        { id: 'async-options-second-signal', type: 'user-message', contents: 'second' },
        { ...target, ifIdle: { behavior: 'wake', requireClaimedOwner: true } },
        pubsub,
      );
      await streamStarted;

      releaseOptions();
      await expect(first.accepted).resolves.toMatchObject({ action: 'deliver', runId: first.runId });
      releaseStream();
      await expect(second.accepted).resolves.toMatchObject({ action: 'deliver', runId: second.runId });
      expect(optionsCalls).toBe(2);
      expect(ownerAgent.stream).toHaveBeenCalledTimes(1);
      expect(ownerRuntime.getActiveThreadRunId(target, pubsub)).toBe(second.runId);
    } finally {
      releaseOptions();
      releaseStream();
      claim.unsubscribe();
    }
  });

  it('keeps an admitted claimed-owner run when acknowledgement publication rejects after delivery', async () => {
    const pubsub = new ControlledLeasePubSub();
    const ownerRuntime = agentThreadStreamRuntime;
    const senderRuntime = new AgentThreadStreamRuntime();
    const ownerAgent = new Agent({
      id: 'delivered-ack-owner',
      name: 'Delivered Ack Owner',
      instructions: 'Test',
      model: createTextStreamModel('admitted owner response'),
      pubsub,
    });
    const senderAgent = new Agent({
      id: 'delivered-ack-sender',
      name: 'Delivered Ack Sender',
      instructions: 'Test',
      model: createTextStreamModel('sender response'),
      pubsub,
    });
    const subscription = await ownerRuntime.subscribeToThread(
      ownerAgent,
      { resourceId: 'delivered-ack-user', threadId: 'delivered-ack-thread' },
      pubsub,
    );
    const nextRun = readNextRunWithParts(subscription.stream[Symbol.asyncIterator]());
    const claim = await ownerRuntime.claimThreadOwnership(
      ownerAgent,
      { resourceId: 'delivered-ack-user', threadId: 'delivered-ack-thread' },
      pubsub,
    );
    pubsub.rejectPublishedTypes.add('idle-signal-accepted');

    const signalResult = senderRuntime.sendSignal(
      senderAgent,
      { type: 'user-message', contents: 'admit despite post-delivery rejection' },
      {
        resourceId: 'delivered-ack-user',
        threadId: 'delivered-ack-thread',
        ifIdle: { behavior: 'wake', requireClaimedOwner: true },
      },
      pubsub,
    );

    await expect(signalResult.accepted).resolves.toMatchObject({ action: 'deliver' });
    await expect(nextRun).resolves.toMatchObject({ value: { text: 'admitted owner response' } });
    expect(pubsub.publishedData.filter(data => data?.type === 'idle-signal-rejected')).toHaveLength(0);

    claim.unsubscribe();
    subscription.unsubscribe();
  });

  it.each(['released', 'expired', 'rejected'] as const)(
    'does not retain controls or execute a claim whose options are %s',
    async outcome => {
      const scope = { resourceId: 'claim-options', threadId: 'claim-options' };
      const topic = `agent.thread-stream.${encodeURIComponent(`${scope.resourceId}\u0000${scope.threadId}`)}`;
      const pubsub = new ControlledLeasePubSub();
      const model = createTextStreamModel('unused');
      const agent = new Agent({ id: 'claim-options', name: 'Options', instructions: 'Test', model, pubsub });
      let release!: () => void;
      const gate = new Promise<void>(resolve => {
        release = resolve;
      });
      let entered!: () => void;
      const entering = new Promise<void>(resolve => {
        entered = resolve;
      });
      const now = vi.spyOn(Date, 'now').mockReturnValue(1000);
      const claim = await agent.claimThreadOwnership({
        ...scope,
        streamOptions: async () => {
          entered();
          await gate;
          if (outcome === 'rejected') throw new Error('Options rejected');
          return {};
        },
      });
      const pending = agent.sendSignal({ type: 'user-message', contents: 'invalid claim' }, scope);
      void pending.accepted.catch(() => {});
      try {
        await entering;
        expect(pubsub.subscriberCount(topic)).toBe(1);
        if (outcome === 'released') claim.unsubscribe();
        if (outcome === 'expired') now.mockReturnValue(10000);
        release();
        await expect(pending.accepted).rejects.toThrow(
          outcome === 'released'
            ? 'owner was released'
            : outcome === 'expired'
              ? 'acceptance expired'
              : 'Options rejected',
        );
        expect(model.doStreamCalls).toHaveLength(0);
        expect(agentThreadStreamRuntime.getActiveThreadRunId(scope, pubsub)).toBeUndefined();
        claim.unsubscribe();
        await vi.waitFor(() => expect(pubsub.subscriberCount(topic)).toBe(0));
      } finally {
        release();
        claim.unsubscribe();
        now.mockRestore();
      }
    },
  );

  it.each(['released', 'failed'] as const)('cleans up claimed startup when control readiness is %s', async outcome => {
    const scope = { resourceId: 'claim-ready', threadId: 'claim-ready' };
    const topic = `agent.thread-stream.${encodeURIComponent(`${scope.resourceId}\u0000${scope.threadId}`)}`;
    const pubsub = new ControlledLeasePubSub();
    const model = createTextStreamModel('unused');
    const agent = new Agent({ id: 'claim-ready', name: 'Ready', instructions: 'Test', model, pubsub });
    const claim = await agent.claimThreadOwnership(scope);
    let release!: () => void;
    const gate = new Promise<void>(resolve => {
      release = resolve;
    });
    let entered!: () => void;
    const entering = new Promise<void>(resolve => {
      entered = resolve;
    });
    const subscribe = pubsub.subscribe.bind(pubsub);
    vi.spyOn(pubsub, 'subscribe').mockImplementationOnce(async (topic, callback) => {
      await subscribe(topic, callback);
      entered();
      await gate;
      if (outcome === 'failed') throw new Error('Controls unavailable');
    });
    const pending = agent.sendSignal({ type: 'user-message', contents: 'claim input' }, scope);
    void pending.accepted.catch(() => {});
    try {
      await entering;
      expect(agentThreadStreamRuntime.getActiveThreadRunId(scope, pubsub)).toBeUndefined();
      if (outcome === 'released') claim.unsubscribe();
      release();
      await expect(pending.accepted).rejects.toThrow(
        outcome === 'released' ? 'owner was released' : 'Controls unavailable',
      );
      claim.unsubscribe();
      await vi.waitFor(() => expect(pubsub.subscriberCount(topic)).toBe(0));
      expect(model.doStreamCalls).toHaveLength(0);
    } finally {
      release();
      claim.unsubscribe();
    }
  });

  it('cancels claimant-held idle input after claim release without any thread observers', async () => {
    const scope = { resourceId: 'claim-control-user', threadId: 'claim-control-thread' };
    const key = `${scope.resourceId}\u0000${scope.threadId}`;
    const topic = `agent.thread-stream.${encodeURIComponent(key)}`;
    const pubsub = new ControlledLeasePubSub();
    const model = createTextStreamModel('should not execute');
    const agent = new Agent({ id: 'claim-control', name: 'Claim control', instructions: 'Test', model, pubsub });
    const sender = new AgentThreadStreamRuntime();
    const claim = await agent.claimThreadOwnership(scope);
    let release!: () => void;
    pubsub.acquireLeaseWait = new Promise<void>(resolve => {
      release = resolve;
    });
    let entered!: () => void;
    const acquiring = new Promise<void>(resolve => {
      entered = resolve;
    });
    pubsub.onAcquireLease = entered;
    const first = agent.sendSignal({ type: 'user-message', contents: 'reserved claim startup' }, scope);
    void first.accepted.catch(() => {});
    try {
      await acquiring;
      const queued = sender.sendSignal(
        agent,
        { type: 'user-message', contents: 'claimant queued input' },
        {
          ...scope,
          ifIdle: { behavior: 'wake', requireClaimedOwner: true },
        },
        pubsub,
      );
      await queued.accepted;
      expect(model.doStreamCalls).toHaveLength(0);
      claim.unsubscribe();
      await pubsub.publish(topic, {
        type: 'signals-cancelled',
        data: { type: 'signals-cancelled', signalIds: [queued.signal.id] },
      });
      await pubsub.flush();
      await nextTick();
      expect(agent.cancelQueuedMessages({ ...scope, signalIds: [queued.signal.id] })).toEqual({
        cancelledSignalIds: [],
      });
      release();
      await expect(first.accepted).rejects.toThrow('Claimed thread owner was released');
      await vi.waitFor(() => expect(pubsub.subscriberCount(topic)).toBe(0));
      expect(pubsub.owners.get(key)).toBeUndefined();
      expect(model.doStreamCalls).toHaveLength(0);
    } finally {
      release();
      claim.unsubscribe();
      await first.accepted.catch(() => {});
    }
  });

  it('cancels idle input behind a background claimed run after releasing its claim', async () => {
    const scope = { resourceId: 'background-claim', threadId: 'background-claim' };
    const topic = `agent.thread-stream.${encodeURIComponent(`${scope.resourceId}\u0000${scope.threadId}`)}`;
    const pubsub = new ControlledLeasePubSub();
    const { model, releaseFirst, getStreamCount } = createBlockingFirstTextStreamModel('first', 'cancelled');
    const agent = new Agent({ id: 'background-claim', name: 'Background', instructions: 'Test', model, pubsub });
    const sender = new AgentThreadStreamRuntime();
    const claim = await agent.claimThreadOwnership(scope);
    try {
      await agent.sendSignal({ type: 'user-message', contents: 'first claim input' }, scope).accepted;
      await vi.waitFor(() => expect(getStreamCount()).toBe(1));
      const queued = sender.sendSignal(
        agent,
        { type: 'user-message', contents: 'cancel queued claim input' },
        {
          ...scope,
          ifIdle: { behavior: 'wake', requireClaimedOwner: true },
        },
        pubsub,
      );
      await queued.accepted;
      claim.unsubscribe();
      await pubsub.publish(topic, {
        type: 'signals-cancelled',
        data: { type: 'signals-cancelled', signalIds: [queued.signal.id] },
      });
      await pubsub.flush();
      await nextTick();
      expect(agent.cancelQueuedMessages({ ...scope, signalIds: [queued.signal.id] })).toEqual({
        cancelledSignalIds: [],
      });
      releaseFirst();
      await vi.waitFor(() => expect(pubsub.subscriberCount(topic)).toBe(0));
      expect(getStreamCount()).toBe(1);
      expect(agentThreadStreamRuntime.getActiveThreadRunId(scope, pubsub)).toBeUndefined();
      expect(pubsub.owners.get(`${scope.resourceId}\u0000${scope.threadId}`)).toBeUndefined();
    } finally {
      releaseFirst();
      claim.unsubscribe();
    }
  });

  it('preserves an acknowledged remote wake when the busy owner releases its claim', async () => {
    const now = vi.spyOn(Date, 'now').mockReturnValue(1_000);
    const pubsub = new ControlledLeasePubSub();
    const ownerRuntime = agentThreadStreamRuntime;
    const senderRuntime = new AgentThreadStreamRuntime();
    const { model, releaseFirst, getStreamCount } = createBlockingFirstTextStreamModel(
      'first owner response',
      'queued owner response',
    );
    const ownerAgent = new Agent({
      id: 'queued-admission-owner',
      name: 'Queued Admission Owner',
      instructions: 'Test',
      model,
      pubsub,
    });
    const senderAgent = new Agent({
      id: 'queued-admission-sender',
      name: 'Queued Admission Sender',
      instructions: 'Test',
      model: createTextStreamModel('sender response'),
      pubsub,
    });
    const subscription = await ownerRuntime.subscribeToThread(
      ownerAgent,
      { resourceId: 'queued-admission-user', threadId: 'queued-admission-thread' },
      pubsub,
    );
    const iterator = subscription.stream[Symbol.asyncIterator]();
    const firstRun = readNextRunWithParts(iterator);
    const claim = await ownerRuntime.claimThreadOwnership(
      ownerAgent,
      { resourceId: 'queued-admission-user', threadId: 'queued-admission-thread' },
      pubsub,
    );

    try {
      const firstSignal = senderRuntime.sendSignal(
        senderAgent,
        { type: 'user-message', contents: 'start the first owner run' },
        {
          resourceId: 'queued-admission-user',
          threadId: 'queued-admission-thread',
          ifIdle: { behavior: 'wake', requireClaimedOwner: true },
        },
        pubsub,
      );
      await expect(firstSignal.accepted).resolves.toMatchObject({ action: 'deliver' });
      await waitForCondition(() => getStreamCount() === 1);

      const queuedSignal = senderRuntime.sendSignal(
        senderAgent,
        { type: 'user-message', contents: 'queue the second owner run' },
        {
          resourceId: 'queued-admission-user',
          threadId: 'queued-admission-thread',
          ifIdle: { behavior: 'wake', requireClaimedOwner: true },
        },
        pubsub,
      );
      await expect(queuedSignal.accepted).resolves.toMatchObject({ action: 'deliver' });
      expect(getStreamCount()).toBe(1);

      // Once accepted, queued work remains in flight even if the claim is released.
      claim.unsubscribe();
      now.mockReturnValue(10_000);
      releaseFirst();
      await firstRun;
      const queuedRun = await readNextRunWithParts(iterator);
      expect(queuedRun.value.text).toBe('queued owner response');
      expect(getStreamCount()).toBe(2);
    } finally {
      releaseFirst();
      now.mockRestore();
      claim.unsubscribe();
      subscription.unsubscribe();
    }
  });

  it('acknowledges a queued remote wake before a later lease handoff failure', async () => {
    const pubsub = new ControlledLeasePubSub();
    const ownerRuntime = agentThreadStreamRuntime;
    const senderRuntime = new AgentThreadStreamRuntime();
    const { model, releaseFirst, getStreamCount } = createBlockingFirstTextStreamModel(
      'first owner response',
      'queued owner response',
    );
    const ownerAgent = new Agent({
      id: 'queued-lease-owner',
      name: 'Queued Lease Owner',
      instructions: 'Test',
      model,
      pubsub,
    });
    const senderAgent = new Agent({
      id: 'queued-lease-sender',
      name: 'Queued Lease Sender',
      instructions: 'Test',
      model: createTextStreamModel('sender response'),
      pubsub,
    });
    const subscription = await ownerRuntime.subscribeToThread(
      ownerAgent,
      { resourceId: 'queued-lease-user', threadId: 'queued-lease-thread' },
      pubsub,
    );
    const firstRun = readNextRunWithParts(subscription.stream[Symbol.asyncIterator]());
    const claim = await ownerRuntime.claimThreadOwnership(
      ownerAgent,
      { resourceId: 'queued-lease-user', threadId: 'queued-lease-thread' },
      pubsub,
    );

    const firstSignal = senderRuntime.sendSignal(
      senderAgent,
      { type: 'user-message', contents: 'start the lease owner run' },
      {
        resourceId: 'queued-lease-user',
        threadId: 'queued-lease-thread',
        ifIdle: { behavior: 'wake', requireClaimedOwner: true },
      },
      pubsub,
    );
    await expect(firstSignal.accepted).resolves.toMatchObject({ action: 'deliver' });
    await waitForCondition(() => getStreamCount() === 1);

    const queuedSignal = senderRuntime.sendSignal(
      senderAgent,
      { type: 'user-message', contents: 'lose the lease before this run starts' },
      {
        resourceId: 'queued-lease-user',
        threadId: 'queued-lease-thread',
        ifIdle: { behavior: 'wake', requireClaimedOwner: true },
      },
      pubsub,
    );
    await expect(queuedSignal.accepted).resolves.toMatchObject({ action: 'deliver' });
    pubsub.denyLeaseTransfer = true;
    pubsub.denyLeaseAcquisition = true;
    releaseFirst();
    await firstRun;
    await waitForCondition(() =>
      pubsub.publishedData.some(
        data => data?.type === 'signal-enqueued' && data.signal?.contents === 'lose the lease before this run starts',
      ),
    );

    expect(getStreamCount()).toBe(1);

    claim.unsubscribe();
    subscription.unsubscribe();
  });

  it('forwards concurrent remote wakes when the claimed owner loses the execution lease', async () => {
    const pubsub = new ControlledLeasePubSub();
    const ownerRuntime = new AgentThreadStreamRuntime();
    const firstSenderRuntime = new AgentThreadStreamRuntime();
    const secondSenderRuntime = new AgentThreadStreamRuntime();
    let releaseAcquire!: () => void;
    let markAcquireStarted!: () => void;
    const ownerAgent = {
      id: 'initial-lease-loss-owner',
      stream: vi.fn(),
    } as unknown as Agent;
    const firstSenderAgent = new Agent({
      id: 'initial-lease-loss-sender-1',
      name: 'Initial Lease Loss Sender 1',
      instructions: 'Test',
      model: createTextStreamModel('sender response'),
      pubsub,
    });
    const secondSenderAgent = new Agent({
      id: 'initial-lease-loss-sender-2',
      name: 'Initial Lease Loss Sender 2',
      instructions: 'Test',
      model: createTextStreamModel('sender response'),
      pubsub,
    });
    const claim = await ownerRuntime.claimThreadOwnership(
      ownerAgent,
      { resourceId: 'initial-lease-loss-user', threadId: 'initial-lease-loss-thread' },
      pubsub,
    );
    pubsub.acquireLeaseWait = new Promise<void>(resolve => {
      releaseAcquire = resolve;
    });
    const acquireStarted = new Promise<void>(resolve => {
      markAcquireStarted = resolve;
    });
    pubsub.onAcquireLease = markAcquireStarted;
    pubsub.denyLeaseAcquisition = true;

    const firstSignal = firstSenderRuntime.sendSignal(
      firstSenderAgent,
      { type: 'user-message', contents: 'lose the initial lease' },
      {
        resourceId: 'initial-lease-loss-user',
        threadId: 'initial-lease-loss-thread',
        ifIdle: { behavior: 'wake', requireClaimedOwner: true },
      },
      pubsub,
    );
    const firstOutcome = firstSignal.accepted.then(
      value => ({ value }),
      error => ({ error: error instanceof Error ? error : new Error(String(error)) }),
    );
    await acquireStarted;

    const secondSignal = secondSenderRuntime.sendSignal(
      secondSenderAgent,
      { type: 'user-message', contents: 'queue behind the losing acquisition' },
      {
        resourceId: 'initial-lease-loss-user',
        threadId: 'initial-lease-loss-thread',
        ifIdle: { behavior: 'wake', requireClaimedOwner: true },
      },
      pubsub,
    );
    const secondOutcome = secondSignal.accepted.then(
      value => ({ value }),
      error => ({ error: error instanceof Error ? error : new Error(String(error)) }),
    );
    await new Promise(resolve => setTimeout(resolve, 25));
    releaseAcquire();

    const [firstResult, secondResult] = await Promise.all([firstOutcome, secondOutcome]);
    expect(firstResult).toMatchObject({ value: { action: 'deliver', runId: 'competing-run' } });
    expect(secondResult).toMatchObject({ value: { action: 'deliver' } });
    expect(ownerAgent.stream).not.toHaveBeenCalled();

    claim.unsubscribe();
  });

  it('does not start a claimed-owner run when lease acquisition completes after the admission deadline', async () => {
    const now = vi.spyOn(Date, 'now').mockReturnValue(1_000);
    try {
      const pubsub = new ControlledLeasePubSub();
      const ownerRuntime = new AgentThreadStreamRuntime();
      const senderRuntime = new AgentThreadStreamRuntime();
      let releaseAcquire!: () => void;
      let markAcquireStarted!: () => void;
      const ownerAgent = {
        id: 'expired-admission-owner',
        stream: vi.fn(),
      } as unknown as Agent;
      const senderAgent = new Agent({
        id: 'expired-admission-sender',
        name: 'Expired Admission Sender',
        instructions: 'Test',
        model: createTextStreamModel('sender response'),
        pubsub,
      });
      const claim = await ownerRuntime.claimThreadOwnership(
        ownerAgent,
        { resourceId: 'expired-admission-user', threadId: 'expired-admission-thread' },
        pubsub,
      );
      pubsub.acquireLeaseWait = new Promise<void>(resolve => {
        releaseAcquire = resolve;
      });
      const acquireStarted = new Promise<void>(resolve => {
        markAcquireStarted = resolve;
      });
      pubsub.onAcquireLease = markAcquireStarted;

      const signal = senderRuntime.sendSignal(
        senderAgent,
        { type: 'user-message', contents: 'expire while acquiring the lease' },
        {
          resourceId: 'expired-admission-user',
          threadId: 'expired-admission-thread',
          ifIdle: { behavior: 'wake', requireClaimedOwner: true },
        },
        pubsub,
      );
      await acquireStarted;
      now.mockReturnValue(6_001);
      releaseAcquire();

      await expect(signal.accepted).rejects.toThrow('acceptance expired');
      expect(ownerAgent.stream).not.toHaveBeenCalled();
      expect(pubsub.owners.get('expired-admission-user\u0000expired-admission-thread')).toBeUndefined();

      claim.unsubscribe();
    } finally {
      now.mockRestore();
    }
  });

  it('transfers a finishing lease before starting a remote claimed-owner wake', async () => {
    const pubsub = new ControlledLeasePubSub();
    const ownerRuntime = new AgentThreadStreamRuntime();
    const senderRuntime = new AgentThreadStreamRuntime();
    const ownerAgent = {
      id: 'remote-finishing-owner',
      stream: vi.fn(async () => ({})),
    } as unknown as Agent;
    const senderAgent = { id: 'remote-finishing-sender' } as unknown as Agent;
    const resourceId = 'remote-finishing-user';
    const threadId = 'remote-finishing-thread';
    const target = { resourceId, threadId };
    let finishRun!: () => void;
    const finishing = new Promise<void>(resolve => {
      finishRun = resolve;
    });
    const finishingOutput = {
      runId: 'remote-finishing-run',
      status: 'success',
      fullStream: new ReadableStream({
        start(controller) {
          controller.close();
        },
      }),
      _waitUntilFinished: () => finishing,
    } as any;
    const completion = ownerRuntime.registerRun(
      ownerAgent,
      finishingOutput,
      { memory: { resource: resourceId, thread: threadId } },
      pubsub,
    )!;
    const transfer = vi.spyOn(pubsub, 'transferLease');
    const claim = await ownerRuntime.claimThreadOwnership(ownerAgent, target, pubsub);

    try {
      await waitForCondition(() => ownerRuntime.getRunOutput(finishingOutput.runId, pubsub) !== undefined);
      const signalResult = senderRuntime.sendSignal(
        senderAgent,
        { id: 'remote-finishing-signal', type: 'user-message', contents: 'wake after remote finish' },
        { ...target, ifIdle: { behavior: 'wake', requireClaimedOwner: true } },
        pubsub,
      );

      await expect(signalResult.accepted).resolves.toMatchObject({ action: 'deliver' });
      expect(ownerAgent.stream).toHaveBeenCalledTimes(1);
      expect(transfer).toHaveBeenCalledTimes(1);
    } finally {
      claim.unsubscribe();
      finishRun();
      await completion;
    }
  });

  it('does not overwrite a foreign reservation when a claimed wake reuses its run id', async () => {
    const pubsub = new ControlledLeasePubSub();
    const ownerRuntime = new AgentThreadStreamRuntime();
    const senderRuntime = new AgentThreadStreamRuntime();
    const ownerAgent = {
      id: 'foreign-reservation-owner',
      stream: vi.fn(async () => ({})),
    } as unknown as Agent;
    const senderAgent = { id: 'foreign-reservation-sender' } as unknown as Agent;
    const foreignRunId = 'foreign-reservation-run';
    const foreignTarget = { resourceId: 'foreign-resource', threadId: 'foreign-thread' };
    const claimedTarget = { resourceId: 'claimed-resource', threadId: 'claimed-thread' };
    const release = ownerRuntime.reserveRun(
      {
        runId: foreignRunId,
        memory: { resource: foreignTarget.resourceId, thread: foreignTarget.threadId },
      } as any,
      pubsub,
      ownerAgent.id,
    );
    expect(release).toBeDefined();
    const claim = await ownerRuntime.claimThreadOwnership(ownerAgent, claimedTarget, pubsub);

    try {
      const signalResult = senderRuntime.sendSignal(
        senderAgent,
        { id: 'foreign-reservation-signal', type: 'user-message', contents: 'must not overwrite' },
        {
          ...claimedTarget,
          runId: foreignRunId,
          ifIdle: { behavior: 'wake', requireClaimedOwner: true },
        },
        pubsub,
      );

      await expect(signalResult.accepted).rejects.toThrow('already reserved for another thread');
      expect(ownerRuntime.getActiveThreadRunId(foreignTarget, pubsub)).toBe(foreignRunId);
      expect(ownerRuntime.getActiveThreadRunId(claimedTarget, pubsub)).toBeUndefined();
      expect(ownerAgent.stream).not.toHaveBeenCalled();
    } finally {
      claim.unsubscribe();
      release?.();
    }
  });

  it('uses the thread lease to fence simultaneous claimed owners before acknowledging delivery', async () => {
    const pubsub = new ControlledLeasePubSub();
    const firstRuntime = new AgentThreadStreamRuntime();
    const secondRuntime = new AgentThreadStreamRuntime();
    const senderRuntime = new AgentThreadStreamRuntime();
    let streamCount = 0;
    const createOwnerAgent = (runtime: AgentThreadStreamRuntime, id: string, responseText: string) => {
      const agent = new Agent({
        id,
        name: id,
        instructions: 'Test',
        model: createTextStreamModel(responseText),
        pubsub,
      });
      vi.spyOn(agent, 'stream').mockImplementation((async (_signal: unknown, options: any) => {
        streamCount += 1;
        const output = createFakeThreadRun(options.runId, Promise.resolve());
        void runtime.registerRun(agent, output, options, pubsub)?.catch(() => {});
        return output;
      }) as any);
      return agent;
    };
    const firstAgent = createOwnerAgent(firstRuntime, 'simultaneous-owner-1', 'first owner response');
    const secondAgent = createOwnerAgent(secondRuntime, 'simultaneous-owner-2', 'second owner response');
    const senderAgent = new Agent({
      id: 'simultaneous-owner-sender',
      name: 'Simultaneous Owner Sender',
      instructions: 'Test',
      model: createTextStreamModel('sender response'),
      pubsub,
    });

    const firstSubscription = await firstRuntime.subscribeToThread(
      firstAgent,
      { resourceId: 'simultaneous-user', threadId: 'simultaneous-thread' },
      pubsub,
    );
    const secondSubscription = await secondRuntime.subscribeToThread(
      secondAgent,
      { resourceId: 'simultaneous-user', threadId: 'simultaneous-thread' },
      pubsub,
    );
    const [firstClaim, secondClaim] = await Promise.all([
      firstRuntime.claimThreadOwnership(
        firstAgent,
        { resourceId: 'simultaneous-user', threadId: 'simultaneous-thread' },
        pubsub,
      ),
      secondRuntime.claimThreadOwnership(
        secondAgent,
        { resourceId: 'simultaneous-user', threadId: 'simultaneous-thread' },
        pubsub,
      ),
    ]);
    expect([firstClaim.claimed, secondClaim.claimed].filter(Boolean)).toHaveLength(1);
    const ownerSubscription = firstClaim.claimed ? firstSubscription : secondSubscription;
    const nextRun = readNextRunWithParts(ownerSubscription.stream[Symbol.asyncIterator]());

    const signalResult = senderRuntime.sendSignal(
      senderAgent,
      { type: 'user-message', contents: 'wake exactly one claimed owner' },
      {
        resourceId: 'simultaneous-user',
        threadId: 'simultaneous-thread',
        ifIdle: { behavior: 'wake', requireClaimedOwner: true },
      },
      pubsub,
    );

    await expect(signalResult.accepted).resolves.toMatchObject({ action: 'deliver' });
    await nextRun;
    expect(streamCount).toBe(1);

    firstClaim.unsubscribe();
    secondClaim.unsubscribe();
    firstSubscription.unsubscribe();
    secondSubscription.unsubscribe();
  });

  it('encodes reserved characters in advertised thread peer IDs', async () => {
    const pubsub = new EventEmitterPubSub();
    const ownerAgent = new Agent({
      id: 'discoverable-agent',
      name: 'Discoverable Agent',
      instructions: 'Test',
      model: createTextStreamModel('discoverable response'),
      pubsub,
    });
    const discoveryAgent = new Agent({
      id: 'discovery-agent',
      name: 'Discovery Agent',
      instructions: 'Test',
      model: createTextStreamModel('discovery response'),
      pubsub,
    });

    const claim = await ownerAgent.claimThreadOwnership({
      resourceId: 'discoverable:resource',
      threadId: 'discoverable/thread',
      peer: {
        label: 'Discoverable peer',
        metadata: { mode: 'build' },
      },
    });

    const peers = await discoveryAgent.discoverThreadPeers();

    expect(peers).toEqual([
      expect.objectContaining({
        id: 'discoverable-agent:discoverable%3Aresource:discoverable%2Fthread',
        agentId: 'discoverable-agent',
        resourceId: 'discoverable:resource',
        threadId: 'discoverable/thread',
        label: 'Discoverable peer',
        metadata: { mode: 'build' },
      }),
    ]);
    expect(peers[0]?.discoveredAt).toBeInstanceOf(Date);

    claim.unsubscribe();
  });

  it('clears the per-request discovery reply topic once discovery settles', async () => {
    const cleared: string[] = [];
    class RecordingPubSub extends EventEmitterPubSub {
      override async clearTopic(topic: string): Promise<void> {
        cleared.push(topic);
      }
    }
    const pubsub = new RecordingPubSub();
    const discoveryAgent = new Agent({
      id: 'reply-topic-discovery-agent',
      name: 'Reply Topic Discovery Agent',
      instructions: 'Test',
      model: createTextStreamModel('discovery response'),
      pubsub,
    });

    await discoveryAgent.discoverThreadPeers({ timeoutMs: 10 });
    // releaseReplyTopic is fire-and-forget; let its unsubscribe → clearTopic chain flush.
    await new Promise(resolve => setTimeout(resolve, 0));

    // On persistent brokers the reply stream would otherwise outlive the
    // request forever — every discovery must drop its own reply topic.
    expect(cleared).toEqual([expect.stringMatching(/^agent\.thread-peer-discovery\./)]);
  });

  it('clears the owner-discovery and idle-acceptance reply topics after a claimed-owner wake', async () => {
    const cleared: string[] = [];
    class RecordingPubSub extends EventEmitterPubSub {
      override async clearTopic(topic: string): Promise<void> {
        cleared.push(topic);
      }
    }
    const pubsub = new RecordingPubSub();
    const ownerRuntime = agentThreadStreamRuntime;
    const senderRuntime = new AgentThreadStreamRuntime();
    const ownerAgent = new Agent({
      id: 'reply-topic-owner-agent',
      name: 'Reply Topic Owner Agent',
      instructions: 'Test',
      model: createTextStreamModel('owner response'),
      pubsub,
    });
    const senderAgent = new Agent({
      id: 'reply-topic-sender-agent',
      name: 'Reply Topic Sender Agent',
      instructions: 'Test',
      model: createTextStreamModel('sender response'),
      pubsub,
    });
    const target = { resourceId: 'reply-topic-user', threadId: 'reply-topic-thread' };

    const subscription = await ownerRuntime.subscribeToThread(ownerAgent, target, pubsub);
    const nextRun = readNextRunWithParts(subscription.stream[Symbol.asyncIterator]());
    const claim = await ownerRuntime.claimThreadOwnership(ownerAgent, target, pubsub);
    expect(claim.claimed).toBe(true);

    const signalResult = senderRuntime.sendSignal(
      senderAgent,
      { type: 'user-message', contents: 'wake the owner' },
      { ...target, ifIdle: { behavior: 'wake', requireClaimedOwner: true } },
      pubsub,
    );
    await expect(signalResult.accepted).resolves.toMatchObject({ action: 'deliver' });
    await nextRun;

    // Both per-request reply topics must be dropped once their round trips
    // settle — on persistent brokers each would otherwise leak a stream.
    await waitForCondition(
      () =>
        cleared.some(topic => topic.startsWith('agent.thread-owner-discovery.')) &&
        cleared.some(topic => topic.includes('.idle-acceptance.')),
    );

    claim.unsubscribe();
    subscription.unsubscribe();
  });

  it('clears the owner-discovery reply topic when discovery times out without an owner', async () => {
    const cleared: string[] = [];
    class RecordingPubSub extends EventEmitterPubSub {
      override async clearTopic(topic: string): Promise<void> {
        cleared.push(topic);
      }
    }
    const pubsub = new RecordingPubSub();
    const agent = new Agent({
      id: 'reply-topic-timeout-agent',
      name: 'Reply Topic Timeout Agent',
      instructions: 'Test',
      model: createTextStreamModel('must not run'),
      pubsub,
    });

    const result = agent.sendSignal(
      { type: 'user-message', contents: 'nobody home' },
      {
        resourceId: 'reply-topic-timeout-resource',
        threadId: 'reply-topic-timeout-thread',
        ifIdle: { behavior: 'wake', requireClaimedOwner: true },
      },
    );

    await expect(result.accepted).rejects.toThrow('No claimed thread owner responded');
    await waitForCondition(() => cleared.some(topic => topic.startsWith('agent.thread-owner-discovery.')));
  });

  it('skips the discovery request and re-releases the reply topic when subscribe finishes after the timeout', async () => {
    const cleared: string[] = [];
    const publishedTypes: string[] = [];
    let releaseSubscribe!: () => void;
    const subscribeGate = new Promise<void>(resolve => {
      releaseSubscribe = resolve;
    });
    class GatedSubscribePubSub extends EventEmitterPubSub {
      override async subscribe(topic: string, cb: EventCallback): Promise<void> {
        // Hold the per-request reply-topic subscribe past the discovery timeout.
        if (topic.startsWith('agent.thread-peer-discovery.')) await subscribeGate;
        return super.subscribe(topic, cb);
      }
      override async publish(topic: string, event: any): Promise<void> {
        publishedTypes.push(event.data?.type);
        return super.publish(topic, event);
      }
      override async clearTopic(topic: string): Promise<void> {
        cleared.push(topic);
      }
    }
    const pubsub = new GatedSubscribePubSub();
    const agent = new Agent({
      id: 'late-subscribe-discovery-agent',
      name: 'Late Subscribe Discovery Agent',
      instructions: 'Test',
      model: createTextStreamModel('discovery response'),
      pubsub,
    });

    await agent.discoverThreadPeers({ timeoutMs: 10 });
    expect(cleared).toEqual([expect.stringMatching(/^agent\.thread-peer-discovery\./)]);

    releaseSubscribe();
    await new Promise(resolve => setTimeout(resolve, 0));

    // The late subscribe must not publish the request (replies would recreate
    // the released reply stream) and must release the reply topic again, since
    // its callback attached after the first release.
    expect(publishedTypes).not.toContain('thread-peer-request');
    expect(cleared).toEqual([
      expect.stringMatching(/^agent\.thread-peer-discovery\./),
      expect.stringMatching(/^agent\.thread-peer-discovery\./),
    ]);
    expect(cleared[0]).toBe(cleared[1]);
  });

  it('updates advertised peer metadata without replacing thread ownership', async () => {
    const pubsub = new EventEmitterPubSub();
    const ownerAgent = new Agent({
      id: 'updatable-peer-agent',
      name: 'Updatable Peer Agent',
      instructions: 'Test',
      model: createTextStreamModel('owner response'),
      pubsub,
    });
    const discoveryAgent = new Agent({
      id: 'peer-discovery-agent',
      name: 'Peer Discovery Agent',
      instructions: 'Test',
      model: createTextStreamModel('discovery response'),
      pubsub,
    });
    const target = { resourceId: 'updatable-resource', threadId: 'updatable-thread' };
    const claim = await ownerAgent.claimThreadOwnership({
      ...target,
      peer: { label: 'Mastra', title: 'Initial title', metadata: { mode: 'build' } },
    });

    expect(
      ownerAgent.updateThreadPeerAdvertisement({
        ...target,
        peer: { title: 'Renamed thread', metadata: { mode: 'review' } },
      }),
    ).toBe(true);
    expect(discoveryAgent.updateThreadPeerAdvertisement({ ...target, peer: { title: 'Unauthorized rename' } })).toBe(
      false,
    );

    await expect(discoveryAgent.discoverThreadPeers()).resolves.toEqual([
      expect.objectContaining({
        id: 'updatable-peer-agent:updatable-resource:updatable-thread',
        label: 'Mastra',
        title: 'Renamed thread',
        metadata: { mode: 'review' },
      }),
    ]);

    expect(ownerAgent.updateThreadPeerAdvertisement({ ...target, peer: { metadata: undefined } })).toBe(true);
    const peersAfterClearingMetadata = await discoveryAgent.discoverThreadPeers();
    expect(peersAfterClearingMetadata).toEqual([
      expect.objectContaining({
        id: 'updatable-peer-agent:updatable-resource:updatable-thread',
        label: 'Mastra',
        title: 'Renamed thread',
      }),
    ]);
    expect(peersAfterClearingMetadata[0]?.metadata).toBeUndefined();

    claim.unsubscribe();
    await expect(discoveryAgent.discoverThreadPeers({ timeoutMs: 10 })).resolves.toEqual([]);
  });

  it('settles peer discovery without waiting for pubsub unsubscribe', async () => {
    const pubsub = new HangingUnsubscribePubSub();
    const agent = new Agent({
      id: 'hanging-unsubscribe-agent',
      name: 'Hanging Unsubscribe Agent',
      instructions: 'Test',
      model: createTextStreamModel('unused'),
      pubsub,
    });

    await expect(
      withTimeout(agent.discoverThreadPeers({ timeoutMs: 10 }), 'Peer discovery waited for unsubscribe', 100),
    ).resolves.toEqual([]);
  });

  it('does not publish peer discovery after a timed-out subscription registers late', async () => {
    vi.useFakeTimers();
    const pubsub = new DelayedRegistrationPubSub();
    const barrier = pubsub.delayNextSubscription(topic => topic.startsWith('agent.thread-peer-discovery.'));
    const discovery = new AgentThreadStreamRuntime().discoverThreadPeers({ timeoutMs: 10 }, pubsub);

    try {
      await barrier.started;
      await vi.advanceTimersByTimeAsync(10);
      await expect(discovery).resolves.toEqual([]);

      barrier.release();
      await barrier.registered;
      await Promise.resolve();
      await Promise.resolve();

      expect(pubsub.published.some(({ event }) => event.data?.type === 'thread-peer-request')).toBe(false);
      expect(pubsub.activeSubscriptionCount(barrier.topic!)).toBe(0);
    } finally {
      barrier.release();
      if (barrier.topic) await barrier.registered;
      vi.useRealTimers();
    }
  });

  it('rejects an idle wake that requires an unavailable claimed owner', async () => {
    const pubsub = new EventEmitterPubSub();
    const agent = new Agent({
      id: 'required-owner-agent',
      name: 'Required Owner Agent',
      instructions: 'Test',
      model: createTextStreamModel('must not run'),
      pubsub,
    });

    const result = agent.sendSignal(
      { type: 'user-message', contents: 'deliver remotely' },
      {
        resourceId: 'required-owner-resource',
        threadId: 'required-owner-thread',
        ifIdle: { behavior: 'wake', requireClaimedOwner: true },
      },
    );

    await expect(result.accepted).rejects.toThrow('No claimed thread owner responded');
    expect(
      agent.getActiveThreadRunId({ resourceId: 'required-owner-resource', threadId: 'required-owner-thread' }),
    ).toBe(undefined);
  });

  it('does not publish owner discovery after a timed-out subscription registers late', async () => {
    vi.useFakeTimers();
    const pubsub = new DelayedRegistrationPubSub();
    const barrier = pubsub.delayNextSubscription(topic => topic.startsWith('agent.thread-owner-discovery.'));
    const runtime = new AgentThreadStreamRuntime();
    const agent = { id: 'late-owner-discovery-agent' } as unknown as Agent;
    let claim: { claimed: boolean; unsubscribe: () => void } | undefined;
    const claimPromise = runtime.claimThreadOwnership(
      agent,
      {
        resourceId: 'late-owner-discovery-user',
        threadId: 'late-owner-discovery-thread',
        peer: false,
      },
      pubsub,
    );

    try {
      await barrier.started;
      await vi.advanceTimersByTimeAsync(100);
      claim = await claimPromise;
      expect(claim.claimed).toBe(true);

      barrier.release();
      await barrier.registered;
      await Promise.resolve();
      await Promise.resolve();

      expect(pubsub.published.some(({ event }) => event.data?.type === 'thread-owner-request')).toBe(false);
      expect(pubsub.activeSubscriptionCount(barrier.topic!)).toBe(0);
    } finally {
      barrier.release();
      if (barrier.topic) await barrier.registered;
      claim?.unsubscribe();
      vi.useRealTimers();
    }
  });

  it('rejects every concurrent idle wake when claimed-owner discovery times out', async () => {
    const pubsub = new EventEmitterPubSub();
    const agent = new Agent({
      id: 'concurrent-required-owner-agent',
      name: 'Concurrent Required Owner Agent',
      instructions: 'Test',
      model: createTextStreamModel('must not run'),
      pubsub,
    });

    const firstSignal = agent.sendSignal(
      { type: 'user-message', contents: 'first remote delivery' },
      {
        resourceId: 'concurrent-required-owner-resource',
        threadId: 'concurrent-required-owner-thread',
        ifIdle: { behavior: 'wake', requireClaimedOwner: true },
      },
    );
    const secondSignal = agent.sendSignal(
      { type: 'user-message', contents: 'second remote delivery' },
      {
        resourceId: 'concurrent-required-owner-resource',
        threadId: 'concurrent-required-owner-thread',
        ifIdle: { behavior: 'wake' },
      },
    );

    const outcomes = await Promise.allSettled([firstSignal.accepted, secondSignal.accepted]);
    expect(outcomes).toHaveLength(2);
    for (const outcome of outcomes) {
      expect(outcome.status).toBe('rejected');
      if (outcome.status === 'rejected') {
        expect(outcome.reason).toEqual(
          expect.objectContaining({ message: expect.stringContaining('No claimed thread owner responded') }),
        );
      }
    }
  });

  it('does not answer ownership discovery after a claim is synchronously released', async () => {
    const pubsub = new RetainedAsyncCallbackPubSub();
    const firstRuntime = new AgentThreadStreamRuntime();
    const secondRuntime = new AgentThreadStreamRuntime();
    const firstAgent = new Agent({
      id: 'released-owner-agent',
      name: 'Released Owner Agent',
      instructions: 'Test',
      model: createTextStreamModel('first owner response'),
      pubsub,
    });
    const secondAgent = new Agent({
      id: 'replacement-owner-agent',
      name: 'Replacement Owner Agent',
      instructions: 'Test',
      model: createTextStreamModel('second owner response'),
      pubsub,
    });

    const firstClaim = await firstRuntime.claimThreadOwnership(
      firstAgent,
      { resourceId: 'released-owner-user', threadId: 'released-owner-thread' },
      pubsub,
    );
    const secondClaimPromise = secondRuntime.claimThreadOwnership(
      secondAgent,
      { resourceId: 'released-owner-user', threadId: 'released-owner-thread' },
      pubsub,
    );
    firstClaim.unsubscribe();

    const secondClaim = await secondClaimPromise;
    expect(secondClaim.claimed).toBe(true);
    secondClaim.unsubscribe();
  });

  it('keeps only one active same-runtime claim across concurrent replacements with distinct peer IDs', async () => {
    const pubsub = new RetainedAsyncCallbackPubSub();
    const runtime = new AgentThreadStreamRuntime();
    const agent = new Agent({
      id: 'concurrent-peer-owner-agent',
      name: 'Concurrent Peer Owner Agent',
      instructions: 'Test',
      model: createTextStreamModel('owner response'),
      pubsub,
    });
    const target = { resourceId: 'concurrent-peer-resource', threadId: 'concurrent-peer-thread' };

    const [firstClaim, secondClaim] = await Promise.all([
      runtime.claimThreadOwnership(agent, { ...target, peer: { id: 'first-custom-peer' } }, pubsub),
      runtime.claimThreadOwnership(agent, { ...target, peer: { id: 'second-custom-peer' } }, pubsub),
    ]);

    expect(firstClaim.claimed).toBe(true);
    expect(secondClaim.claimed).toBe(true);
    const peers = await runtime.discoverThreadPeers({ timeoutMs: 10 }, pubsub);
    expect(peers).toHaveLength(1);

    const displacedClaim = peers[0]?.id === 'first-custom-peer' ? secondClaim : firstClaim;
    const activeClaim = peers[0]?.id === 'first-custom-peer' ? firstClaim : secondClaim;
    displacedClaim.unsubscribe();
    await expect(runtime.discoverThreadPeers({ timeoutMs: 10 }, pubsub)).resolves.toHaveLength(1);
    activeClaim.unsubscribe();
  });

  it('keeps only one active same-runtime owner callback across concurrent peerless replacements', async () => {
    const pubsub = new RetainedAsyncCallbackPubSub();
    const runtime = agentThreadStreamRuntime;
    const senderRuntime = new AgentThreadStreamRuntime();
    const firstAgent = new Agent({
      id: 'first-concurrent-owner-agent',
      name: 'First Concurrent Owner Agent',
      instructions: 'Test',
      model: createTextStreamModel('first owner response'),
      pubsub,
    });
    const secondAgent = new Agent({
      id: 'second-concurrent-owner-agent',
      name: 'Second Concurrent Owner Agent',
      instructions: 'Test',
      model: createTextStreamModel('second owner response'),
      pubsub,
    });
    const firstStream = vi.spyOn(firstAgent, 'stream');
    const secondStream = vi.spyOn(secondAgent, 'stream');
    const target = { resourceId: 'concurrent-owner-resource', threadId: 'concurrent-owner-thread' };

    const claims = await Promise.all([
      runtime.claimThreadOwnership(firstAgent, { ...target, peer: false }, pubsub),
      runtime.claimThreadOwnership(secondAgent, { ...target, peer: false }, pubsub),
    ]);
    const sender = new Agent({
      id: 'concurrent-owner-sender',
      name: 'Concurrent Owner Sender',
      instructions: 'Test',
      model: createTextStreamModel('unused'),
      pubsub,
    });

    await expect(
      senderRuntime.sendSignal(
        sender,
        { type: 'user-message', contents: 'wake active owner' },
        {
          ...target,
          ifIdle: { behavior: 'wake', requireClaimedOwner: true },
        },
        pubsub,
      ).accepted,
    ).resolves.toMatchObject({ action: 'deliver' });
    await pubsub.flush();
    await waitForCondition(() => firstStream.mock.calls.length + secondStream.mock.calls.length > 0);
    expect(firstStream.mock.calls.length + secondStream.mock.calls.length).toBe(1);

    claims.forEach(claim => claim.unsubscribe());
  });

  it('does not claim thread ownership when another runtime already owns the thread', async () => {
    const pubsub = new EventEmitterPubSub();
    const firstRuntime = new AgentThreadStreamRuntime();
    const secondRuntime = new AgentThreadStreamRuntime();
    const firstAgent = new Agent({
      id: 'first-owner-agent',
      name: 'First Owner Agent',
      instructions: 'Test',
      model: createTextStreamModel('first owner response'),
      pubsub,
    });
    const secondAgent = new Agent({
      id: 'second-owner-agent',
      name: 'Second Owner Agent',
      instructions: 'Test',
      model: createTextStreamModel('second owner response'),
      pubsub,
    });

    const firstClaim = await firstRuntime.claimThreadOwnership(
      firstAgent,
      {
        resourceId: 'claimed-user',
        threadId: 'claimed-thread',
      },
      pubsub,
    );
    const secondClaim = await secondRuntime.claimThreadOwnership(
      secondAgent,
      {
        resourceId: 'claimed-user',
        threadId: 'claimed-thread',
      },
      pubsub,
    );

    expect(firstClaim.claimed).toBe(true);
    expect(secondClaim.claimed).toBe(false);

    firstClaim.unsubscribe();
  });

  it('discovers advertised thread peers over pubsub', async () => {
    const pubsub = new EventEmitterPubSub();
    const ownerAgent = new Agent({
      id: 'peer-owner-agent',
      name: 'Peer Owner Agent',
      instructions: 'Test',
      model: createTextStreamModel('owner response'),
      pubsub,
    });
    const discovererAgent = new Agent({
      id: 'peer-discoverer-agent',
      name: 'Peer Discoverer Agent',
      instructions: 'Test',
      model: createTextStreamModel('discoverer response'),
      pubsub,
    });

    const claim = await ownerAgent.claimThreadOwnership({
      resourceId: 'peer-resource',
      threadId: 'peer-thread',
      peer: {
        label: 'Peer Owner',
        title: 'Owner Thread',
        metadata: { mode: 'build' },
      },
    });

    const peers = await discovererAgent.discoverThreadPeers({ timeoutMs: 10 });

    expect(peers).toHaveLength(1);
    expect(peers[0]).toMatchObject({
      id: 'peer-owner-agent:peer-resource:peer-thread',
      agentId: 'peer-owner-agent',
      resourceId: 'peer-resource',
      threadId: 'peer-thread',
      label: 'Peer Owner',
      title: 'Owner Thread',
      metadata: { mode: 'build' },
    });
    expect(peers[0].sourceId).toBeDefined();
    expect(peers[0].discoveredAt).toBeInstanceOf(Date);

    claim.unsubscribe();
  });

  it('marks the discovering agent own advertisements and leaves sibling agents discoverable', async () => {
    const pubsub = new EventEmitterPubSub();
    const sessionAgent = new Agent({
      id: 'session-peer-agent',
      name: 'Session Peer Agent',
      instructions: 'Test',
      model: createTextStreamModel('session response'),
      pubsub,
    });
    const siblingAgent = new Agent({
      id: 'sibling-peer-agent',
      name: 'Sibling Peer Agent',
      instructions: 'Test',
      model: createTextStreamModel('sibling response'),
      pubsub,
    });

    const sessionClaim = await sessionAgent.claimThreadOwnership({
      resourceId: 'shared-peer-resource',
      threadId: 'session-peer-thread',
      peer: { label: 'Session' },
    });
    const siblingClaim = await siblingAgent.claimThreadOwnership({
      resourceId: 'shared-peer-resource',
      threadId: 'sibling-peer-thread',
      peer: { label: 'Sibling' },
    });

    const peers = await sessionAgent.discoverThreadPeers({ timeoutMs: 10 });
    const byId = new Map(peers.map(peer => [peer.id, peer]));

    // Both advertisements live in one process-wide runtime, but only the caller's
    // own thread is the caller's own — a sibling agent is a peer it can address.
    expect(byId.get('session-peer-agent:shared-peer-resource:session-peer-thread')?.selfAdvertised).toBe(true);
    expect(byId.get('sibling-peer-agent:shared-peer-resource:sibling-peer-thread')?.selfAdvertised).toBeUndefined();

    sessionClaim.unsubscribe();
    siblingClaim.unsubscribe();
  });

  it('keeps the own-advertisement mark when another instance answers discovery for the same thread', async () => {
    const pubsub = new RetainedAsyncCallbackPubSub();
    const agent = new Agent({
      id: 'session-peer-agent',
      name: 'Session Peer Agent',
      instructions: 'Test',
      model: createTextStreamModel('session response'),
      pubsub,
    });

    const claim = await agent.claimThreadOwnership({
      resourceId: 'shared-peer-resource',
      threadId: 'session-peer-thread',
      peer: { label: 'Session' },
    });

    // A second live instance with the same thread loaded answers discovery too, and
    // its reply replaces the local entry with one rebuilt from the wire payload — so
    // a mark computed only from the local advertisements is lost on that path.
    const responder: EventCallback = async event => {
      const data = event.data as any;
      if (data?.type !== 'thread-peer-request') return;
      await pubsub.publish(data.replyTopic, {
        type: 'thread-peer-response',
        runId: data.requestId,
        data: {
          type: 'thread-peer-response',
          requestId: data.requestId,
          sourceId: 'other-instance-source',
          peer: {
            id: 'session-peer-agent:shared-peer-resource:session-peer-thread',
            agentId: 'session-peer-agent',
            resourceId: 'shared-peer-resource',
            threadId: 'session-peer-thread',
            label: 'Session',
          },
        },
      });
    };
    await pubsub.subscribe('agent.thread-peer-discovery', responder);

    const peers = await agent.discoverThreadPeers({ timeoutMs: 10 });
    const byId = new Map(peers.map(peer => [peer.id, peer]));

    expect(byId.get('session-peer-agent:shared-peer-resource:session-peer-thread')?.selfAdvertised).toBe(true);

    await pubsub.unsubscribe('agent.thread-peer-discovery', responder);
    claim.unsubscribe();
  });

  it('releases claimed ownership and peer advertisements when reset for tests', async () => {
    const ownerAgent = new Agent({
      id: 'reset-owner-agent',
      name: 'Reset Owner Agent',
      instructions: 'Test',
      model: createTextStreamModel('owner response'),
    });
    const discovererAgent = new Agent({
      id: 'reset-discoverer-agent',
      name: 'Reset Discoverer Agent',
      instructions: 'Test',
      model: createTextStreamModel('discoverer response'),
    });

    const claim = await ownerAgent.claimThreadOwnership({
      resourceId: 'reset-resource',
      threadId: 'reset-thread',
      peer: { label: 'Reset peer' },
    });
    expect(claim.claimed).toBe(true);
    await expect(discovererAgent.discoverThreadPeers({ timeoutMs: 10 })).resolves.toHaveLength(1);

    agentThreadStreamRuntime.resetForTests();

    await expect(discovererAgent.discoverThreadPeers({ timeoutMs: 10 })).resolves.toEqual([]);
  });

  it('starts an idle thread run when sendMessage is called', async () => {
    const agent = new Agent({
      id: 'idle-message-agent',
      name: 'Idle Message Agent',
      instructions: 'Test',
      model: createTextStreamModel('message response'),
    });

    const subscription = await agent.subscribeToThread({
      threadId: 'idle-message-thread',
      resourceId: 'idle-message-user',
    });
    const nextRun = readNextRunWithParts(subscription.stream[Symbol.asyncIterator]());

    const result = await agent.sendMessage(
      { contents: 'Hello from sendMessage', attributes: { sentFrom: 'test' } },
      {
        resourceId: 'idle-message-user',
        threadId: 'idle-message-thread',
        ifIdle: { streamOptions: { memory: { resource: 'idle-message-user', thread: 'idle-message-thread' } } },
      },
    );

    const subscribedRun = await nextRun;
    await expect(result.accepted).resolves.toMatchObject({ action: 'wake', runId: subscribedRun.value.runId });
    expect(result.signal).toMatchObject({ type: 'user', tagName: 'user', contents: 'Hello from sendMessage' });
    const signalPart = subscribedRun.value.parts.find((part: any) => part.type === 'data-user-message');
    expect(signalPart?.data).toMatchObject({
      id: result.signal.id,
      type: 'user',
      tagName: 'user',
      contents: 'Hello from sendMessage',
      attributes: { sentFrom: 'test' },
    });
    expect(subscribedRun.value.text).toBe('message response');

    subscription.unsubscribe();
  });

  it('uses the configured message ID generator for persisted sendMessage signal rows', async () => {
    const memory = new MockMemory();
    const threadId = 'configured-send-message-thread';
    const resourceId = 'configured-send-message-user';
    await memory.createThread({ threadId, resourceId });

    let sequence = 0;
    const idGenerator = vi.fn((context?: { idType?: string; source?: string; entityId?: string }) => {
      sequence += 1;
      return `${context?.idType ?? 'id'}_custom_${sequence}`;
    });

    const agent = new Agent({
      id: 'configured-send-message-agent',
      name: 'Configured Send Message Agent',
      instructions: 'Test',
      model: createTextStreamModel('unused'),
      memory,
    });

    new Mastra({
      agents: { configuredSendMessageAgent: agent },
      idGenerator,
      logger: false,
    });

    const result = agent.sendMessage(
      { contents: 'persist with configured id' },
      {
        resourceId,
        threadId,
        ifActive: { behavior: 'persist' },
        ifIdle: { behavior: 'persist' },
      },
    );

    await expect(result.persisted).resolves.toBeUndefined();

    expect(result.signal.id).toMatch(/^message_custom_\d+$/);
    const recalled = await memory.recall({ threadId, resourceId });
    const persistedSignal = recalled.messages.find(message => message.role === 'signal');

    expect(persistedSignal?.id).toBe(result.signal.id);
    expect(persistedSignal?.id).toMatch(/^message_custom_\d+$/);
    expect(idGenerator).toHaveBeenCalledWith(
      expect.objectContaining({
        idType: 'message',
        source: 'agent',
        entityId: 'configured-send-message-agent',
        threadId,
        resourceId,
      }),
    );
  });

  it('preserves explicit sendSignal IDs', async () => {
    const memory = new MockMemory();
    const threadId = 'explicit-signal-id-thread';
    const resourceId = 'explicit-signal-id-user';
    await memory.createThread({ threadId, resourceId });

    const agent = new Agent({
      id: 'explicit-signal-id-agent',
      name: 'Explicit Signal ID Agent',
      instructions: 'Test',
      model: createTextStreamModel('unused'),
      memory,
    });

    new Mastra({
      agents: { explicitSignalIdAgent: agent },
      idGenerator: () => 'message_custom_generated',
      logger: false,
    });

    const result = agent.sendSignal(
      { id: 'caller-signal-id', type: 'system-reminder', contents: 'remember this' },
      {
        resourceId,
        threadId,
        ifIdle: { behavior: 'persist' },
      },
    );

    await expect(result.persisted).resolves.toBeUndefined();
    expect(result.signal.id).toBe('caller-signal-id');
  });

  it('uses the configured message ID generator for queueMessage signals', async () => {
    const memory = new MockMemory();
    const threadId = 'configured-queue-message-thread';
    const resourceId = 'configured-queue-message-user';
    await memory.createThread({ threadId, resourceId });

    let sequence = 0;
    const idGenerator = vi.fn((context?: { idType?: string; source?: string; entityId?: string }) => {
      sequence += 1;
      return `${context?.idType ?? 'id'}_custom_${sequence}`;
    });

    const agent = new Agent({
      id: 'configured-queue-message-agent',
      name: 'Configured Queue Message Agent',
      instructions: 'Test',
      model: createTextStreamModel('queued response'),
      memory,
    });

    new Mastra({
      agents: { configuredQueueMessageAgent: agent },
      idGenerator,
      logger: false,
    });

    const subscription = await agent.subscribeToThread({ threadId, resourceId });
    const nextRun = readNextRunWithParts(subscription.stream[Symbol.asyncIterator]());

    const result = agent.queueMessage('queue with configured id', { resourceId, threadId });

    expect(result.signal.id).toMatch(/^message_custom_\d+$/);
    expect(idGenerator).toHaveBeenCalledWith(
      expect.objectContaining({
        idType: 'message',
        source: 'agent',
        entityId: 'configured-queue-message-agent',
        threadId,
        resourceId,
      }),
    );

    const queuedRun = await nextRun;
    expect(queuedRun.value.text).toBe('queued response');
    subscription.unsubscribe();
  });

  it('resolves run id context before generating sendMessage signal IDs', async () => {
    const threadId = 'run-id-send-message-id-thread';
    const resourceId = 'run-id-send-message-id-user';
    let sequence = 0;
    const idGenerator = vi.fn(context => {
      sequence += 1;
      if (context?.idType === 'message') return `message_custom_${context.threadId}_${context.resourceId}`;
      return `${context?.idType ?? 'id'}_custom_${sequence}`;
    });
    const { model, releaseFirst } = createBlockingFirstTextStreamModel('first response', 'message response');
    const agent = new Agent({
      id: 'run-id-send-message-id-agent',
      name: 'Run Id Send Message Id Agent',
      instructions: 'Test',
      model,
    });

    new Mastra({
      agents: { runIdSendMessageIdAgent: agent },
      idGenerator,
      logger: false,
    });

    const subscription = await agent.subscribeToThread({ threadId, resourceId });
    const stream = await agent.stream('Hello', { memory: { thread: threadId, resource: resourceId } });

    try {
      await expect(waitForActiveRun(subscription)).resolves.toBe(stream.runId);
      const result = agent.sendMessage('message by run id', { runId: stream.runId });

      expect(result.signal.id).toBe(`message_custom_${threadId}_${resourceId}`);
      expect(idGenerator).toHaveBeenCalledWith(
        expect.objectContaining({
          idType: 'message',
          source: 'agent',
          entityId: 'run-id-send-message-id-agent',
          threadId,
          resourceId,
        }),
      );
    } finally {
      releaseFirst();
      subscription.unsubscribe();
    }

    await expect(stream.text).resolves.toBe('first responsemessage response');
  });

  it('resolves run id context before generating queueMessage signal IDs', async () => {
    const threadId = 'run-id-queue-message-id-thread';
    const resourceId = 'run-id-queue-message-id-user';
    let sequence = 0;
    const idGenerator = vi.fn(context => {
      sequence += 1;
      if (context?.idType === 'message') return `message_custom_${context.threadId}_${context.resourceId}`;
      return `${context?.idType ?? 'id'}_custom_${sequence}`;
    });
    const { model, releaseFirst, getStreamCount } = createBlockingFirstTextStreamModel(
      'first response',
      'queued response',
    );
    const agent = new Agent({
      id: 'run-id-queue-message-id-agent',
      name: 'Run Id Queue Message Id Agent',
      instructions: 'Test',
      model,
    });

    new Mastra({
      agents: { runIdQueueMessageIdAgent: agent },
      idGenerator,
      logger: false,
    });

    const subscription = await agent.subscribeToThread({ threadId, resourceId });
    const stream = await agent.stream('Hello', { memory: { thread: threadId, resource: resourceId } });

    try {
      await expect(waitForActiveRun(subscription)).resolves.toBe(stream.runId);
      const result = agent.queueMessage('queue by run id', { runId: stream.runId });

      expect(result.signal.id).toBe(`message_custom_${threadId}_${resourceId}`);
      expect(result.runId).not.toBe(stream.runId);
      expect(idGenerator).toHaveBeenCalledWith(
        expect.objectContaining({
          idType: 'message',
          source: 'agent',
          entityId: 'run-id-queue-message-id-agent',
          threadId,
          resourceId,
        }),
      );
      await nextTick();
      expect(getStreamCount()).toBe(1);
    } finally {
      releaseFirst();
      subscription.unsubscribe();
    }

    await expect(stream.text).resolves.toBe('first response');
  });

  describe('thread-scoped pending signal cancellation', () => {
    it.each(['live', 'retained'] as const)('does not report completed %s observer input as cancelled', async mode => {
      const scope = { resourceId: 'completed-observer', threadId: 'completed-observer' };
      const pubsub = new ControlledLeasePubSub();
      const { model, releaseFirst, getStreamCount } = createBlockingFirstTextStreamModel('first', 'done');
      const agent = new Agent({ id: 'completed-observer', name: 'Observer', model, instructions: 'Test', pubsub });
      const follower = new AgentThreadStreamRuntime();
      let observer = mode === 'live' ? await follower.subscribeToThread(agent, scope, pubsub) : undefined;
      try {
        const first = await agent.stream('initial', { memory: { resource: scope.resourceId, thread: scope.threadId } });
        await vi.waitFor(() => expect(getStreamCount()).toBe(1));
        const signal = agent.sendSignal({ type: 'user-message', contents: 'already answered' }, scope);
        await signal.accepted;
        releaseFirst();
        await first.text;
        await vi.waitFor(() => expect(getStreamCount()).toBe(2));
        await vi.waitFor(() => expect(agentThreadStreamRuntime.getActiveThreadRunId(scope, pubsub)).toBeUndefined());
        observer ??= await follower.subscribeToThread(agent, scope, pubsub);
        await pubsub.flush();
        await nextTick();
        expect(follower.cancelQueuedMessages(agent, { ...scope, signalIds: [signal.signal.id] }, pubsub)).toEqual({
          cancelledSignalIds: [],
        });
        expect(pubsub.publishedData.filter(data => data.type === 'signals-cancelled')).toEqual([
          { type: 'signals-cancelled', signalIds: [signal.signal.id] },
        ]);
      } finally {
        observer?.unsubscribe();
        releaseFirst();
      }
    });

    it.each([false, true])(
      'keeps cancellation ahead of delayed enqueue retries with initial delivery=%s',
      async delivered => {
        const scope = { resourceId: 'retry-cancel', threadId: 'retry-cancel' };
        const pubsub = new ControlledLeasePubSub();
        const topic = `agent.thread-stream.${encodeURIComponent(`${scope.resourceId}\u0000${scope.threadId}`)}`;
        const { model, releaseFirst, getStreamCount } = createBlockingFirstTextStreamModel('first', 'survivor');
        const agent = new Agent({ id: 'retry-cancel', name: 'Retry', instructions: 'Test', model, pubsub });
        let release!: () => void;
        const gate = new Promise<void>(resolve => {
          release = resolve;
        });
        const first = await agent.stream('initial', { memory: { resource: scope.resourceId, thread: scope.threadId } });
        await vi.waitFor(() => expect(getStreamCount()).toBe(1));
        const signal = createSignal({ id: 'delayed-retry', type: 'user-message', contents: 'cancelled retry input' });
        const enqueue = {
          type: 'signal-enqueued',
          data: { type: 'signal-enqueued', runId: first.runId, sourceId: 'remote', signal: signal.toDataPart().data },
        };
        const registration = pubsub.publishedData.find(
          data => data.type === 'run-registered' && data.runId === first.runId,
        );
        const readOwner = vi.spyOn(pubsub, 'getLeaseOwner').mockImplementationOnce(async () => {
          await gate;
          return undefined;
        });
        try {
          if (delivered) {
            await pubsub.publish(topic, enqueue);
            await pubsub.flush();
            await nextTick();
          }
          // Hold a real control handler while cancellation and a Redis-style republished retry arrive.
          // Fork (PF-4402): only an abort request carrying the run's exact lease
          // owner reaches the live-lease read.
          await pubsub.publish(topic, {
            type: 'run-abort-requested',
            data: {
              type: 'run-abort-requested',
              runId: first.runId,
              streamId: registration.streamId,
              leaseOwner: registration.leaseOwner,
            },
          });
          await vi.waitFor(() => expect(readOwner).toHaveBeenCalled());
          await pubsub.publish(topic, {
            type: 'signals-cancelled',
            data: { type: 'signals-cancelled', signalIds: [signal.id] },
          });
          await pubsub.publish(topic, { ...enqueue, deliveryAttempt: 2 });
          await pubsub.flush();
          release();
          await nextTick();
          expect(agent.cancelQueuedMessages({ ...scope, signalIds: [signal.id] })).toEqual({ cancelledSignalIds: [] });
          await agent.queueMessage('surviving retry input', scope).accepted;
          releaseFirst();
          await first.text;
          await vi.waitFor(() => expect(getStreamCount()).toBe(2));
          await vi.waitFor(() => expect(agentThreadStreamRuntime.getActiveThreadRunId(scope, pubsub)).toBeUndefined());
          expect(JSON.stringify(model.doStreamCalls[1]?.prompt)).toContain('surviving retry input');
          expect(JSON.stringify(model.doStreamCalls[1]?.prompt)).not.toContain('cancelled retry input');
          await vi.waitFor(() => expect(pubsub.subscriberCount(topic)).toBe(0));
        } finally {
          release();
          releaseFirst();
          readOwner.mockRestore();
        }
      },
    );

    it.each([false, true])('preserves in-flight remote input during handoff with observer=%s', async observed => {
      const scope = { resourceId: 'transfer-ingress', threadId: 'transfer-ingress' };
      const pubsub = new ControlledLeasePubSub();
      const { model, releaseFirst, getStreamCount } = createBlockingFirstTextStreamModel('first', 'next');
      const agent = new Agent({ id: 'transfer-ingress', name: 'Transfer', model, pubsub, instructions: 'Test' });
      const subscription = observed ? await agent.subscribeToThread(scope) : undefined;
      let entered!: () => void;
      const entering = new Promise<void>(resolve => {
        entered = resolve;
      });
      let release!: () => void;
      const gate = new Promise<void>(resolve => {
        release = resolve;
      });
      let deliver!: () => void;
      const delivery = new Promise<void>(resolve => {
        deliver = resolve;
      });
      let sending!: () => void;
      const sent = new Promise<void>(resolve => {
        sending = resolve;
      });
      const publish = pubsub.publish.bind(pubsub);
      vi.spyOn(pubsub, 'publish').mockImplementation(async (topic, event) => {
        if (event.data?.type === 'signal-enqueued' && event.data.signal.contents === 'late forwarded input') {
          sending();
          await delivery;
        }
        await publish(topic, event);
      });
      const remote = new AgentThreadStreamRuntime();
      try {
        const first = await agent.stream('initial', { memory: { resource: scope.resourceId, thread: scope.threadId } });
        await vi.waitFor(() => expect(getStreamCount()).toBe(1));
        await agent.sendSignal({ type: 'user-message', contents: 'followup' }, scope).accepted;
        const forwarded = remote.sendSignal(
          agent,
          { type: 'user-message', contents: 'late forwarded input' },
          scope,
          pubsub,
        );
        await sent;
        pubsub.transferLeaseWait = gate;
        pubsub.onTransferLease = entered;
        agent.abortThreadStream(scope);
        releaseFirst();
        await first.text;
        await entering;
        deliver();
        await forwarded.accepted;
        await pubsub.flush();
        await nextTick();
        release();
        await vi.waitFor(() => expect(agentThreadStreamRuntime.getActiveThreadRunId(scope, pubsub)).toBeUndefined());
        expect(JSON.stringify(model.doStreamCalls.slice(1))).toContain('late forwarded input');
      } finally {
        deliver();
        release();
        releaseFirst();
        subscription?.unsubscribe();
      }
    });

    it('releases follower control after observed work finishes and the last observer disconnects', async () => {
      const scope = { resourceId: 'observed-copy', threadId: 'observed-copy' };
      const topic = `agent.thread-stream.${encodeURIComponent(`${scope.resourceId}\u0000${scope.threadId}`)}`;
      const pubsub = new ControlledLeasePubSub();
      const { model, releaseFirst, getStreamCount } = createBlockingFirstTextStreamModel('first', 'next');
      const agent = new Agent({ id: 'observed-copy', name: 'Copy', model, pubsub, instructions: 'Test' });
      const followerRuntime = new AgentThreadStreamRuntime();
      const follower = await followerRuntime.subscribeToThread(agent, scope, pubsub);
      try {
        const first = await agent.stream('initial', { memory: { resource: scope.resourceId, thread: scope.threadId } });
        await vi.waitFor(() => expect(getStreamCount()).toBe(1));
        await agent.sendSignal({ type: 'user-message', contents: 'already consumed input' }, scope).accepted;
        await pubsub.flush();
        await nextTick();
        releaseFirst();
        await first.text;
        await vi.waitFor(() => expect(getStreamCount()).toBe(2));
        await vi.waitFor(() => expect(pubsub.subscriberCount(topic)).toBe(2));
        follower.unsubscribe();
        await vi.waitFor(() => expect(pubsub.subscriberCount(topic)).toBe(0));
      } finally {
        releaseFirst();
        follower.unsubscribe();
      }
    });
    it.each([false, true])(
      'does not execute retained observer history after listener recreation with new runtime=%s',
      async newRuntime => {
        const scope = { resourceId: 'retained-lifetime', threadId: 'retained-lifetime' };
        const topic = `agent.thread-stream.${encodeURIComponent(`${scope.resourceId}\u0000${scope.threadId}`)}`;
        const pubsub = new ControlledLeasePubSub();
        const { model, releaseFirst, getStreamCount } = createBlockingFirstTextStreamModel('first', 'next');
        const agent = new Agent({ id: 'retained-lifetime', name: 'Retained', model, pubsub, instructions: 'Test' });
        const options = { memory: { resource: scope.resourceId, thread: scope.threadId } };
        const first = await agent.stream('initial', options);
        await vi.waitFor(() => expect(getStreamCount()).toBe(1));
        await agent.sendSignal({ type: 'user-message', contents: 'already executed' }, scope).accepted;
        const cancelled = createSignal({ type: 'user-message', contents: 'already cancelled' });
        await pubsub.publish(topic, {
          type: 'signal-enqueued',
          data: {
            type: 'signal-enqueued',
            runId: first.runId,
            signal: cancelled.toDataPart().data,
            sourceId: 'foreign',
          },
        });
        await pubsub.flush();
        await nextTick();
        expect(agent.cancelQueuedMessages({ ...scope, signalIds: [cancelled.id] }).cancelledSignalIds).toEqual([
          cancelled.id,
        ]);
        releaseFirst();
        await first.text;
        await vi.waitFor(() => expect(pubsub.subscriberCount(topic)).toBe(0));
        const runtime = newRuntime ? new AgentThreadStreamRuntime() : agentThreadStreamRuntime;
        for (let lifetime = 0; lifetime < 2; lifetime++) {
          const subscription = await runtime.subscribeToThread(agent, scope, pubsub);
          const runId = `fresh-run-${lifetime}`;
          try {
            await runtime.waitForCrossAgentThreadRun(agent, { ...options, runId }, pubsub);
            await pubsub.flush();
            await nextTick();
            expect(runtime.drainPendingSignals(runId, pubsub, 'pre-run')).toEqual([]);
            expect(runtime.drainPendingSignals(runId, pubsub)).toEqual([]);
          } finally {
            runtime.releaseThreadRunReservation(runId, pubsub);
            subscription.unsubscribe();
          }
          await vi.waitFor(() => expect(pubsub.subscriberCount(topic)).toBe(0));
        }
        expect(getStreamCount()).toBe(2);
      },
    );

    it('does not readmit consumed or cancelled input on duplicate control delivery', async () => {
      const scope = { resourceId: 'duplicate-control', threadId: 'duplicate-control' };
      const topic = `agent.thread-stream.${encodeURIComponent(`${scope.resourceId}\u0000${scope.threadId}`)}`;
      const pubsub = new ControlledLeasePubSub();
      const runtime = new AgentThreadStreamRuntime();
      const agent = new Agent({
        id: 'duplicate-control',
        name: 'Duplicate',
        instructions: 'Test',
        model: createTextStreamModel('unused'),
        pubsub,
      });
      const runId = 'duplicate-run';
      // Real reserve+prepare fixture: `waitForCrossAgentThreadRun` neither
      // prepares the run nor establishes control readiness, so the duplicate
      // deliveries below must run against an explicitly prepared and reserved
      // run whose control subscription is verifiably live.
      const prepared = runtime.prepareRunOptions(
        { runId, memory: { resource: scope.resourceId, thread: scope.threadId } } as any,
        pubsub,
      );
      const releaseReservation = runtime.reserveRun(prepared, pubsub, agent.id);
      expect(releaseReservation).toBeDefined();
      await vi.waitFor(() => expect(pubsub.subscriberCount(topic)).toBe(1));
      try {
        for (const cancel of [false, true]) {
          const signal = createSignal({ type: 'user-message', contents: 'once' });
          const event = {
            type: 'signal-enqueued',
            data: { type: 'signal-enqueued', runId, sourceId: 'foreign', signal: signal.toDataPart().data },
          };
          await pubsub.publish(topic, event);
          await pubsub.flush();
          await nextTick();
          if (cancel)
            expect(
              runtime.cancelQueuedMessages(agent, { ...scope, signalIds: [signal.id] }, pubsub).cancelledSignalIds,
            ).toEqual([signal.id]);
          else expect(runtime.drainPendingSignals(runId, pubsub).map(entry => entry.id)).toEqual([signal.id]);
          await pubsub.publish(topic, event);
          await pubsub.flush();
          await nextTick();
          expect(runtime.drainPendingSignals(runId, pubsub)).toEqual([]);
        }
      } finally {
        // The public release path also tears down the prepared run and its
        // control subscription; the reserveRun callback alone would leave the
        // subscriber attached.
        runtime.releaseThreadRunReservation(runId, pubsub);
      }
      await vi.waitFor(() => expect(pubsub.subscriberCount(topic)).toBe(0));
    });

    it('receives remote input without observers and cleans up without replaying it into later runs', async () => {
      const scope = { resourceId: 'control-user', threadId: 'control-thread' };
      const topic = `agent.thread-stream.${encodeURIComponent(`${scope.resourceId}\u0000${scope.threadId}`)}`;
      const pubsub = new ControlledLeasePubSub();
      const { model, releaseFirst, getStreamCount } = createBlockingFirstTextStreamModel('first', 'next');
      const agent = new Agent({ id: 'control-owner', name: 'Control', instructions: 'Test', model, pubsub });
      const options = { memory: { resource: scope.resourceId, thread: scope.threadId } };
      try {
        const first = await agent.stream('initial', options);
        await vi.waitFor(() => expect(getStreamCount()).toBe(1));
        expect(pubsub.subscriberCount(topic)).toBe(1);
        const signal = createSignal({ type: 'user-message', contents: 'remote input exactly once' });
        await pubsub.publish(topic, {
          type: 'signal-enqueued',
          data: { type: 'signal-enqueued', runId: first.runId, sourceId: 'remote', signal: signal.toDataPart().data },
        });
        await pubsub.flush();
        await nextTick();
        releaseFirst();
        await first.text;
        await vi.waitFor(() => expect(pubsub.subscriberCount(topic)).toBe(0));
        expect(getStreamCount()).toBe(2);
        expect(JSON.stringify(model.doStreamCalls[1]?.prompt)).toContain('remote input exactly once');
        const next = await agent.stream('new request', options);
        await next.text;
        await vi.waitFor(() => expect(pubsub.subscriberCount(topic)).toBe(0));
        expect(getStreamCount()).toBe(3);
        expect(JSON.stringify(model.doStreamCalls[2]?.prompt)).not.toContain('remote input exactly once');
      } finally {
        releaseFirst();
      }
    });

    it.each(['success', 'failure'] as const)(
      'cancels remote pre-run input without observers and cleans up on preparation %s',
      async outcome => {
        const scope = { resourceId: 'prepare-control-user', threadId: 'prepare-control-thread' };
        const topic = `agent.thread-stream.${encodeURIComponent(`${scope.resourceId}\u0000${scope.threadId}`)}`;
        const pubsub = new ControlledLeasePubSub();
        const model = createTextStreamModel('answer');
        let entered!: () => void;
        let release!: () => void;
        const preparing = new Promise<void>(resolve => {
          entered = resolve;
        });
        const gate = new Promise<void>(resolve => {
          release = resolve;
        });
        const agent = new Agent({
          id: 'prepare-control',
          name: 'Prepare',
          model,
          pubsub,
          instructions: async () => {
            entered();
            await gate;
            if (outcome === 'failure') throw new Error('Preparation failed');
            return 'Test';
          },
        });
        const starting = agent.stream('initial', { memory: { resource: scope.resourceId, thread: scope.threadId } });
        void starting.catch(() => {});
        try {
          await preparing;
          expect(pubsub.subscriberCount(topic)).toBe(1);
          const pending = agent.sendSignal({ type: 'user-message', contents: 'cancel before registration' }, scope);
          await pending.accepted;
          await pubsub.publish(topic, {
            type: 'signals-cancelled',
            data: { type: 'signals-cancelled', signalIds: [pending.signal.id] },
          });
          await pubsub.flush();
          await nextTick();
          expect(agent.cancelQueuedMessages({ ...scope, signalIds: [pending.signal.id] })).toEqual({
            cancelledSignalIds: [],
          });
          release();
          if (outcome === 'failure') await expect(starting).rejects.toThrow('Preparation failed');
          else {
            await (
              await starting
            ).text;
            expect(JSON.stringify(model.doStreamCalls[0]?.prompt)).not.toContain('cancel before registration');
          }
          await vi.waitFor(() => expect(pubsub.subscriberCount(topic)).toBe(0));
        } finally {
          release();
        }
      },
    );

    it('keeps control listening while suspended without observers and releases it after resume', async () => {
      const scope = { resourceId: 'suspended-control-user', threadId: 'suspended-control-thread' };
      const topic = `agent.thread-stream.${encodeURIComponent(`${scope.resourceId}\u0000${scope.threadId}`)}`;
      const pubsub = new ControlledLeasePubSub();
      const runtime = new AgentThreadStreamRuntime();
      const agent = new Agent({
        id: 'suspended-control',
        name: 'Suspended',
        instructions: 'Test',
        model: createTextStreamModel('unused'),
        pubsub,
      });
      const runId = 'suspended-control-run';
      let finish!: () => void;
      const finished = new Promise<void>(resolve => {
        finish = resolve;
      });
      const output = createFakeThreadRun(runId, finished);
      output.status = 'suspended';
      output.fullStream = new ReadableStream({
        start(controller) {
          controller.enqueue({
            type: 'tool-call-approval',
            runId,
            payload: { toolCallId: 'approval', toolName: 'test' },
          });
          controller.close();
        },
      });
      const options = { memory: { resource: scope.resourceId, thread: scope.threadId } };
      // Fork (PF-4402): registerRun resolves at terminal delivery, which a
      // suspended segment does not reach until it is resumed.
      void runtime.registerRun(agent, output, options, pubsub);
      await vi.waitFor(() => expect(runtime.getResumableThreadRun({ ...scope, runId }, pubsub)).toBeDefined());
      finish();
      await vi.waitFor(() => expect(pubsub.publishedData.some(data => data.type === 'run-suspended')).toBe(true));
      expect(pubsub.subscriberCount(topic)).toBe(1);
      const pending = runtime.queueMessage(agent, 'cancel behind approval', scope, pubsub);
      await pending.accepted;
      await pubsub.publish(topic, {
        type: 'signals-cancelled',
        data: { type: 'signals-cancelled', signalIds: [pending.signal.id] },
      });
      await pubsub.flush();
      await nextTick();
      expect(runtime.cancelQueuedMessages(agent, { ...scope, signalIds: [pending.signal.id] }, pubsub)).toEqual({
        cancelledSignalIds: [],
      });
      expect(pubsub.subscriberCount(topic)).toBe(1);
      await runtime.registerRun(agent, createFakeThreadRun(runId, Promise.resolve()), options, pubsub);
      await vi.waitFor(() => expect(pubsub.subscriberCount(topic)).toBe(0));
    });

    it('waits for the control subscription before preparing a run and recovers from subscription failure', async () => {
      const scope = { resourceId: 'ready-user', threadId: 'ready-thread' };
      const pubsub = new ControlledLeasePubSub();
      let release!: () => void;
      const gate = new Promise<void>(resolve => {
        release = resolve;
      });
      const subscribeNormally = pubsub.subscribe.bind(pubsub);
      const subscribe = vi.spyOn(pubsub, 'subscribe').mockImplementationOnce(async (topic, callback) => {
        await subscribeNormally(topic, callback);
        await gate;
        throw new Error('Subscription unavailable');
      });
      const instructions = vi.fn(() => 'Test');
      const agent = new Agent({
        id: 'ready-control',
        name: 'Ready',
        instructions,
        model: createTextStreamModel('answer'),
        pubsub,
      });
      const options = { memory: { resource: scope.resourceId, thread: scope.threadId } };
      const starting = agent.stream('initial', options);
      void starting.catch(() => {});
      try {
        await vi.waitFor(() => expect(subscribe).toHaveBeenCalledTimes(1));
        expect(instructions).not.toHaveBeenCalled();
        release();
        await expect(starting).rejects.toThrow('Subscription unavailable');
        expect(
          pubsub.subscriberCount(
            `agent.thread-stream.${encodeURIComponent(`${scope.resourceId}\u0000${scope.threadId}`)}`,
          ),
        ).toBe(0);
        subscribe.mockRestore();
        await expect((await agent.stream('retry', options)).text).resolves.toBe('answer');
      } finally {
        release();
        subscribe.mockRestore();
      }
    });

    it.each(
      (['connected', 'absent', 'disconnected'] as const).flatMap(observer =>
        [false, true].map(localCopy => ({ observer, localCopy })),
      ),
    )(
      'propagates requested IDs with owner observer $observer and local copy $localCopy without cancelling survivors',
      async ({ observer, localCopy }) => {
        const scope = { resourceId: 'propagate-user', threadId: 'propagate-thread' };
        const pubsub = new ControlledLeasePubSub();
        const followerRuntime = new AgentThreadStreamRuntime();
        const { model, releaseFirst, getStreamCount } = createBlockingFirstTextStreamModel('first', 'survivor answer');
        const agent = new Agent({
          id: 'propagate-agent',
          name: 'Propagation',
          instructions: 'Test',
          model,
          memory: new MockMemory(),
          pubsub,
        });
        const owner = observer === 'absent' ? undefined : await agent.subscribeToThread(scope);
        const follower = await followerRuntime.subscribeToThread(agent, scope, pubsub);
        try {
          const first = await agent.stream('initial', {
            memory: { resource: scope.resourceId, thread: scope.threadId },
          });
          await vi.waitFor(() => expect(getStreamCount()).toBe(1));
          await pubsub.flush();
          await vi.waitFor(() => expect(follower.activeRunId()).toBe(first.runId));
          const removed = localCopy
            ? followerRuntime.queueMessage(agent, 'cancelled remote input', scope, pubsub)
            : agent.sendSignal({ type: 'user-message', contents: 'cancelled remote input' }, scope);
          await removed.accepted;
          // Model a pending copy routed to the execution owner, not an observer-only history entry.
          if (localCopy)
            await pubsub.publish(
              `agent.thread-stream.${encodeURIComponent(`${scope.resourceId}\u0000${scope.threadId}`)}`,
              {
                type: 'signal-enqueued',
                data: {
                  type: 'signal-enqueued',
                  runId: first.runId,
                  signal: removed.signal.toDataPart().data,
                  sourceId: 'follower',
                },
              },
            );
          const ownerOnly = agent.queueMessage('cancelled owner-only input', scope);
          await ownerOnly.accepted;
          const survivor = agent.queueMessage('surviving input', scope);
          await survivor.accepted;
          await pubsub.flush();
          await nextTick();
          if (observer === 'disconnected') owner?.unsubscribe();
          // Requested remote-only IDs are published but never included in the local result.
          expect(
            followerRuntime.cancelQueuedMessages(
              agent,
              { ...scope, signalIds: [removed.signal.id, removed.signal.id, ownerOnly.signal.id, 'missing'] },
              pubsub,
            ),
          ).toEqual({ cancelledSignalIds: localCopy ? [removed.signal.id] : [] });
          await pubsub.flush();
          await nextTick();
          expect(pubsub.publishedData.filter(data => data.type === 'signals-cancelled')).toEqual([
            { type: 'signals-cancelled', signalIds: [removed.signal.id, ownerOnly.signal.id, 'missing'] },
          ]);
          expect(agent.cancelQueuedMessages({ ...scope, signalIds: [removed.signal.id, ownerOnly.signal.id] })).toEqual(
            {
              cancelledSignalIds: [],
            },
          );
          releaseFirst();
          await first.text;
          await vi.waitFor(() => expect(getStreamCount()).toBe(2));
          await vi.waitFor(() => expect(agentThreadStreamRuntime.getActiveThreadRunId(scope, pubsub)).toBeUndefined());
          const prompt = JSON.stringify(model.doStreamCalls[1]?.prompt);
          expect(prompt).toContain('surviving input');
          expect(prompt).not.toContain('cancelled remote input');
          expect(prompt).not.toContain('cancelled owner-only input');
          expect(getStreamCount()).toBe(2);
        } finally {
          releaseFirst();
          owner?.unsubscribe();
          follower.unsubscribe();
        }
      },
    );

    it.each([false, true])(
      'applies ordered cancellation events with retained replay=%s and duplicate subscribers',
      async replay => {
        const scope = { resourceId: 'replay-cancel-user', threadId: 'replay-cancel-thread' };
        const pubsub = new ControlledLeasePubSub();
        const runtime = new AgentThreadStreamRuntime();
        const agent = new Agent({
          id: 'replay-cancel-agent',
          name: 'Replay cancellation',
          instructions: 'Test',
          model: createTextStreamModel('unused'),
          pubsub,
        });
        const topic = `agent.thread-stream.${encodeURIComponent(`${scope.resourceId}\u0000${scope.threadId}`)}`;
        const subscriptions: Array<Awaited<ReturnType<typeof runtime.subscribeToThread>>> = [];
        const subscribe = async () => {
          // Fork (PF-4402): waitForCrossAgentThreadRun never reserves; hold the
          // thread through reserveRun and wait for its control listener.
          const reservation = { runId: 'queued-run', memory: { resource: scope.resourceId, thread: scope.threadId } };
          runtime.reserveRun(reservation, pubsub, agent.id);
          await runtime.waitForThreadRunReservation(reservation, pubsub, agent.id);
          subscriptions.push(await runtime.subscribeToThread(agent, scope, pubsub));
          subscriptions.push(await runtime.subscribeToThread(agent, scope, pubsub));
        };
        try {
          if (!replay) await subscribe();
          for (const preRun of [true, false]) {
            const signal = createSignal({ id: `cancel-replay-${preRun}`, type: 'user-message', contents: 'cancel' });
            await pubsub.publish(topic, {
              type: 'signal-enqueued',
              data: {
                type: 'signal-enqueued',
                runId: 'queued-run',
                signal: signal.toDataPart().data,
                sourceId: 'remote',
                preRun,
              },
            });
          }
          const event = {
            type: 'signals-cancelled',
            data: { type: 'signals-cancelled', signalIds: ['cancel-replay-true', 'cancel-replay-false', 'missing'] },
          };
          await pubsub.publish(topic, event);
          await pubsub.publish(topic, event);
          const marker = createSignal({ id: 'after-cancellation', type: 'user-message', contents: 'keep' });
          await pubsub.publish(topic, {
            type: 'signal-enqueued',
            data: {
              type: 'signal-enqueued',
              runId: 'queued-run',
              signal: marker.toDataPart().data,
              sourceId: 'remote',
            },
          });
          await pubsub.flush();
          if (replay) await subscribe();
          await nextTick();
          expect(
            runtime.cancelQueuedMessages(
              agent,
              { ...scope, signalIds: ['cancel-replay-true', 'cancel-replay-false'] },
              pubsub,
            ),
          ).toEqual({ cancelledSignalIds: [] });
          expect(pubsub.publishedData.filter(data => data.type === 'signals-cancelled')).toHaveLength(3);
          expect(runtime.cancelQueuedMessages(agent, { ...scope, signalIds: [marker.id] }, pubsub)).toEqual({
            cancelledSignalIds: [marker.id],
          });
        } finally {
          runtime.releaseThreadRunReservation('queued-run', pubsub);
          subscriptions.forEach(subscription => subscription.unsubscribe());
        }
      },
    );

    it('notifies idle queue observers once when remote cancellation is delivered twice', async () => {
      const scope = { resourceId: 'remote-count-user', threadId: 'remote-count-thread' };
      const pubsub = new ControlledLeasePubSub();
      const runtime = new AgentThreadStreamRuntime();
      const agent = new Agent({
        id: 'remote-count-agent',
        name: 'Remote count',
        instructions: 'Test',
        model: createTextStreamModel('unused'),
        pubsub,
      });
      // Ordinary registration returns the completion watcher, which is backed
      // by the never-settling finish gate below: observe it without awaiting so
      // setup can proceed to the registration-readiness barrier instead of
      // hanging on run completion, and settle the gate during cleanup.
      let finishRun!: () => void;
      const finishGate = new Promise<void>(resolve => {
        finishRun = resolve;
      });
      const registration = runtime.registerRun(
        agent,
        createFakeThreadRun('remote-count-run', finishGate),
        { memory: { resource: scope.resourceId, thread: scope.threadId } },
        pubsub,
      );
      void registration?.catch(() => {});
      await vi.waitFor(() =>
        expect(
          pubsub.publishedData.some(data => data.type === 'run-registered' && data.runId === 'remote-count-run'),
        ).toBe(true),
      );
      const subscription = await runtime.subscribeToThread(agent, scope, pubsub);
      const counts: number[] = [];
      const unsubscribe = runtime.subscribeThreadEvents(agent, scope, event => counts.push(event.count), pubsub);
      try {
        const queued = runtime.queueMessage(agent, 'cancel remote idle input', scope, pubsub);
        await queued.accepted;
        expect(counts).toEqual([0, 1]);
        const topic = `agent.thread-stream.${encodeURIComponent(`${scope.resourceId}\u0000${scope.threadId}`)}`;
        const event = { type: 'signals-cancelled', data: { type: 'signals-cancelled', signalIds: [queued.signal.id] } };
        await pubsub.publish(topic, event);
        await pubsub.publish(topic, event);
        await pubsub.flush();
        await nextTick();
        expect(counts).toEqual([0, 1, 0]);
        expect(pubsub.publishedData.filter(data => data.type === 'signals-cancelled')).toHaveLength(2);
        expect(runtime.cancelQueuedMessages(agent, { ...scope, signalIds: [queued.signal.id] }, pubsub)).toEqual({
          cancelledSignalIds: [],
        });
      } finally {
        unsubscribe();
        subscription.unsubscribe();
        finishRun();
        await registration?.catch(() => {});
      }
    });

    it('preserves a queued continuation when clear-on-abort removes pending and idle signals', async () => {
      const scope = { resourceId: 'clear-continuation-user', threadId: 'clear-continuation-thread' };
      const pubsub = new ControlledLeasePubSub();
      const { model, releaseFirst, getStreamCount } = createBlockingFirstTextStreamModel(
        'first',
        'continuation answer',
      );
      const memory = new MockMemory();
      const agent = new Agent({
        id: 'clear-continuation',
        name: 'Clear continuation',
        instructions: 'Test',
        model,
        memory,
        pubsub,
      });
      const subscription = await agent.subscribeToThread(scope);
      try {
        const first = await agent.stream('initial', { memory: { resource: scope.resourceId, thread: scope.threadId } });
        await vi.waitFor(() => expect(getStreamCount()).toBe(1));
        await agent.sendSignal({ type: 'user-message', contents: 'cleared pending input' }, scope).accepted;
        await agent.queueMessage('cleared idle input', scope).accepted;
        agentThreadStreamRuntime.continueWithMessages(agent, 'surviving continuation', scope, pubsub);
        expect(subscription.abort({ clearPendingSignals: true })).toBe(true);
        releaseFirst();
        await first.text;
        await vi.waitFor(() => expect(getStreamCount()).toBe(2));
        await vi.waitFor(() => expect(pubsub.owners.get(`${scope.resourceId}\u0000${scope.threadId}`)).toBeUndefined());
        const prompt = JSON.stringify(model.doStreamCalls[1]?.prompt);
        expect(prompt).toContain('surviving continuation');
        expect(prompt).not.toContain('cleared pending input');
        expect(prompt).not.toContain('cleared idle input');
        const { messages } = await memory.recall(scope);
        expect(messages.at(-1)).toMatchObject({
          role: 'assistant',
          content: {
            parts: expect.arrayContaining([expect.objectContaining({ type: 'text', text: 'continuation answer' })]),
          },
        });
        expect(getStreamCount()).toBe(2);
      } finally {
        releaseFirst();
        subscription.unsubscribe();
      }
    });

    it.each(['local', 'remote'] as const)(
      'clears requeued input via %s cancellation without observers or an active run',
      async origin => {
        const scope = { resourceId: 'inactive-clear-user', threadId: 'inactive-clear-thread' };
        const pubsub = new ControlledLeasePubSub();
        const { model, releaseFirst, getStreamCount } = createBlockingFirstTextStreamModel('first', 'new answer');
        let preparations = 0;
        const agent = new Agent({
          id: 'inactive-clear',
          name: 'Inactive clear',
          model,
          memory: new MockMemory(),
          pubsub,
          instructions: () => {
            if (++preparations === 2) throw new Error('Queued preparation failed');
            return 'Test';
          },
        });
        const topic = `agent.thread-stream.${encodeURIComponent(`${scope.resourceId}\u0000${scope.threadId}`)}`;
        try {
          const first = await agent.stream('initial', {
            memory: { resource: scope.resourceId, thread: scope.threadId },
          });
          await vi.waitFor(() => expect(getStreamCount()).toBe(1));
          const pending = agent.sendSignal({ type: 'user-message', contents: 'requeued input to clear' }, scope);
          await pending.accepted;
          expect(agent.abortThreadStream(scope)).toBe(true);
          releaseFirst();
          await first.text;
          await vi.waitFor(() =>
            expect(
              pubsub.publishedData.some(data => data.type === 'run-failed' && data.error.includes('requeued')),
            ).toBe(true),
          );
          await vi.waitFor(() => expect(agentThreadStreamRuntime.getActiveThreadRunId(scope, pubsub)).toBeUndefined());
          expect(pubsub.subscriberCount(topic)).toBe(1);
          if (origin === 'remote') {
            await pubsub.publish(topic, {
              type: 'signals-cancelled',
              data: { type: 'signals-cancelled', signalIds: [pending.signal.id] },
            });
            await pubsub.flush();
            await nextTick();
            expect(agent.cancelQueuedMessages({ ...scope, signalIds: [pending.signal.id] })).toEqual({
              cancelledSignalIds: [],
            });
          }
          expect(agent.abortThreadStream({ ...scope, clearPendingSignals: true })).toBe(false);
          await vi.waitFor(() => expect(pubsub.subscriberCount(topic)).toBe(0));
          const next = await agent.stream('new input', {
            memory: { resource: scope.resourceId, thread: scope.threadId },
          });
          await next.text;
          expect(getStreamCount()).toBe(2);
          expect(JSON.stringify(model.doStreamCalls[1]?.prompt)).toContain('new input');
          expect(JSON.stringify(model.doStreamCalls[1]?.prompt)).not.toContain('requeued input to clear');
          await vi.waitFor(() =>
            expect(pubsub.owners.get(`${scope.resourceId}\u0000${scope.threadId}`)).toBeUndefined(),
          );
          await vi.waitFor(() => expect(pubsub.subscriberCount(topic)).toBe(0));
        } finally {
          releaseFirst();
        }
      },
    );

    it('ignores remote clear after ownership changes during lease verification', async () => {
      const scope = { resourceId: 'stale-clear-user', threadId: 'stale-clear-thread' };
      const key = `${scope.resourceId}\u0000${scope.threadId}`;
      const pubsub = new ControlledLeasePubSub();
      const owner = new AgentThreadStreamRuntime();
      const follower = new AgentThreadStreamRuntime();
      const agent = new Agent({
        id: 'stale-clear',
        name: 'Stale clear',
        instructions: 'Test',
        model: createTextStreamModel('unused'),
        memory: new MockMemory(),
        pubsub,
      });
      const local = await owner.subscribeToThread(agent, scope, pubsub);
      const remote = await follower.subscribeToThread(agent, scope, pubsub);
      let finish!: () => void;
      const finished = new Promise<void>(resolve => {
        finish = resolve;
      });
      let checking!: () => void;
      const checked = new Promise<void>(resolve => {
        checking = resolve;
      });
      let release!: () => void;
      const gate = new Promise<void>(resolve => {
        release = resolve;
      });
      try {
        const oldOptions = owner.prepareRunOptions(
          { runId: 'old-clear-run', memory: { resource: scope.resourceId, thread: scope.threadId } },
          pubsub,
        );
        // Fork (PF-4402): registerRun resolves at terminal delivery, and leases
        // are held under process-attempt owner tokens rather than raw run ids.
        let finishOld!: () => void;
        const oldFinished = new Promise<void>(resolve => {
          finishOld = resolve;
        });
        void owner.registerRun(
          agent,
          {
            runId: 'old-clear-run',
            status: 'running',
            fullStream: (async function* () {})(),
            _waitUntilFinished: () => oldFinished,
          } as any,
          oldOptions,
          pubsub,
        );
        await vi.waitFor(() => expect(remote.activeRunId()).toBe('old-clear-run'));
        await vi.waitFor(() => expect(pubsub.owners.get(key)).toBeDefined());
        const oldOwner = pubsub.owners.get(key)!;
        // A lease read can have observed the old owner before its response arrives.
        vi.spyOn(pubsub, 'getLeaseOwner').mockImplementationOnce(async () => {
          checking();
          await gate;
          return oldOwner;
        });
        expect(remote.abort({ clearPendingSignals: true })).toBe(true);
        await checked;
        // Fork (PF-4402): a successor cannot replace a live registration on the
        // same thread, so ownership changes by the old run finishing first.
        finishOld();
        await vi.waitFor(() => expect(owner.getActiveThreadRunId(scope, pubsub)).toBeUndefined());
        await vi.waitFor(() => expect(pubsub.owners.get(key)).toBeUndefined());
        const newOptions = owner.prepareRunOptions(
          { runId: 'new-clear-run', memory: { resource: scope.resourceId, thread: scope.threadId } },
          pubsub,
        );
        const registration = owner.registerRun(
          agent,
          {
            runId: 'new-clear-run',
            status: 'running',
            fullStream: (async function* () {})(),
            _waitUntilFinished: () => finished,
          } as any,
          newOptions,
          pubsub,
        );
        await vi.waitFor(() => expect(owner.getActiveThreadRunId(scope, pubsub)).toBe('new-clear-run'));
        const survivor = owner.queueMessage(agent, 'successor pending input', scope, pubsub);
        await survivor.accepted;
        release();
        void registration;
        await vi.waitFor(() => expect(decodeLeaseOwnerRunId(pubsub.owners.get(key))).toBe('new-clear-run'));
        // This marker is handled after the stale request on the owner's serialized event tail.
        const marker = createSignal({ id: 'stale-clear-marker', type: 'user-message', contents: 'marker' });
        await pubsub.publish(`agent.thread-stream.${encodeURIComponent(key)}`, {
          type: 'agent.thread-stream',
          data: {
            type: 'signal-enqueued',
            runId: 'new-clear-run',
            signal: marker.toDataPart().data,
            sourceId: 'marker-source',
          },
        });
        await pubsub.flush();
        await nextTick();
        expect(owner.cancelQueuedMessages(agent, { ...scope, signalIds: [marker.id] }, pubsub)).toEqual({
          cancelledSignalIds: [marker.id],
        });
        expect(owner.isRunAborted('new-clear-run', pubsub)).toBe(false);
        expect(owner.isRunAborted('old-clear-run', pubsub)).toBe(false);
        expect(owner.cancelQueuedMessages(agent, { ...scope, signalIds: [survivor.signal.id] }, pubsub)).toEqual({
          cancelledSignalIds: [survivor.signal.id],
        });
        expect(decodeLeaseOwnerRunId(pubsub.owners.get(key))).toBe('new-clear-run');
      } finally {
        release();
        finish();
        local.unsubscribe();
        remote.unsubscribe();
      }
    });
    it.each(['success', 'failure'] as const)(
      'notifies reentrant listeners after abort before preparation %s',
      async outcome => {
        const scope = { resourceId: 'reentrant-clear-user', threadId: 'reentrant-clear-thread' };
        const pubsub = new ControlledLeasePubSub();
        const { model, releaseFirst, getStreamCount } = createBlockingFirstTextStreamModel('first', 'next');
        let preparing!: () => void;
        const prepared = new Promise<void>(resolve => {
          preparing = resolve;
        });
        let release!: () => void;
        const gate = new Promise<void>(resolve => {
          release = resolve;
        });
        let preparations = 0;
        const agent = new Agent({
          id: 'reentrant-clear',
          name: 'Reentrant clear',
          model,
          memory: new MockMemory(),
          pubsub,
          instructions: async () => {
            if (++preparations === 2) {
              preparing();
              await gate;
              if (outcome === 'failure') throw new Error('Preparation failed');
            }
            return 'Test';
          },
        });
        const subscription = await agent.subscribeToThread(scope);
        let armed = false;
        let abortedBeforeNotification: boolean | undefined;
        let startupRunId: string | undefined;
        let survivor: ReturnType<typeof agent.queueMessage> | undefined;
        const unsubscribe = agent.subscribeThreadEvents(scope, event => {
          if (!armed || event.count !== 0) return;
          armed = false;
          abortedBeforeNotification = !!startupRunId && agentThreadStreamRuntime.isRunAborted(startupRunId, pubsub);
          subscription.abort({ clearPendingSignals: true });
          survivor = agent.queueMessage('listener survivor', scope);
          throw new Error('Listener errors must not interrupt abort cleanup');
        });
        try {
          const first = await agent.stream('initial', {
            memory: { resource: scope.resourceId, thread: scope.threadId },
          });
          await vi.waitFor(() => expect(getStreamCount()).toBe(1));
          await agent.sendSignal({ type: 'user-message', contents: 'aborted startup' }, scope).accepted;
          subscription.abort();
          releaseFirst();
          await first.text;
          await prepared;
          startupRunId = agentThreadStreamRuntime.getActiveThreadRunId(scope, pubsub);
          await agent.queueMessage('cleared idle', scope).accepted;
          armed = true;
          expect(subscription.abort({ clearPendingSignals: true })).toBe(true);
          expect(abortedBeforeNotification).toBe(true);
          expect(survivor).toBeDefined();
          await survivor?.accepted;
          release();
          await vi.waitFor(() => expect(getStreamCount()).toBe(2));
          await vi.waitFor(() =>
            expect(pubsub.owners.get(`${scope.resourceId}\u0000${scope.threadId}`)).toBeUndefined(),
          );
          const prompts = JSON.stringify(model.doStreamCalls.slice(1));
          expect(prompts).toContain('listener survivor');
          // Successful preparation can persist aborted input as history; it must not execute it again.
          if (outcome === 'failure') expect(prompts).not.toContain('aborted startup');
          expect(JSON.stringify(model.doStreamCalls[1]?.prompt.at(-1))).toContain('listener survivor');
          expect(JSON.stringify(model.doStreamCalls[1]?.prompt.at(-1))).not.toContain('aborted startup');
          expect(prompts).not.toContain('cleared idle');
          const next = await agent.stream('after clear', {
            memory: { resource: scope.resourceId, thread: scope.threadId },
          });
          await next.text;
          expect(getStreamCount()).toBe(3);
        } finally {
          armed = false;
          release();
          releaseFirst();
          unsubscribe();
          subscription.unsubscribe();
        }
      },
    );
    it.each(
      (['pending', 'idle'] as const).flatMap(queue =>
        [false, true].flatMap(clearPendingSignals =>
          [false, true].map(cancelById => ({ queue, clearPendingSignals, cancelById })),
        ),
      ),
    )(
      'handles failed $queue preparation after abort with clearPendingSignals=$clearPendingSignals and cancelById=$cancelById',
      async ({ queue, clearPendingSignals, cancelById }) => {
        const scope = { resourceId: 'failed-clear-user', threadId: 'failed-clear-thread' };
        const pubsub = new ControlledLeasePubSub();
        const { model, releaseFirst, getStreamCount } = createBlockingFirstTextStreamModel('first', 'next');
        let preparing!: () => void;
        const prepared = new Promise<void>(resolve => {
          preparing = resolve;
        });
        let release!: () => void;
        const gate = new Promise<void>(resolve => {
          release = resolve;
        });
        let preparations = 0;
        const agent = new Agent({
          id: 'failed-clear',
          name: 'Failed clear',
          model,
          memory: new MockMemory(),
          pubsub,
          instructions: async () => {
            if (++preparations === 2) {
              preparing();
              await gate;
              throw new Error('Queued preparation failed');
            }
            return 'Test';
          },
        });
        const subscription = await agent.subscribeToThread(scope);
        try {
          const first = await agent.stream('initial', {
            memory: { resource: scope.resourceId, thread: scope.threadId },
          });
          await vi.waitFor(() => expect(getStreamCount()).toBe(1));
          const queued =
            queue === 'pending'
              ? agent.sendSignal({ type: 'user-message', contents: 'failed queued input' }, scope)
              : agent.queueMessage('failed queued input', scope);
          await queued.accepted;
          subscription.abort();
          releaseFirst();
          await first.text;
          await prepared;
          if (cancelById) {
            // Handoff prevents local success; deliver the request before testing preparation cleanup.
            expect(agent.cancelQueuedMessages({ ...scope, signalIds: [queued.signal.id] })).toEqual({
              cancelledSignalIds: [],
            });
            await pubsub.flush();
            await nextTick();
          }
          expect(subscription.abort({ clearPendingSignals })).toBe(true);
          release();
          await vi.waitFor(() => expect(pubsub.publishedData.some(data => data.type === 'run-failed')).toBe(true));
          await vi.waitFor(() => expect(agentThreadStreamRuntime.getActiveThreadRunId(scope, pubsub)).toBeUndefined());
          const next = await agent.stream('new input', {
            memory: { resource: scope.resourceId, thread: scope.threadId },
          });
          await next.text;
          await vi.waitFor(() => expect(agentThreadStreamRuntime.getActiveThreadRunId(scope, pubsub)).toBeUndefined());
          const prompts = JSON.stringify(model.doStreamCalls.slice(1));
          if (queue === 'pending' && !clearPendingSignals) expect(prompts).toContain('failed queued input');
          else expect(prompts).not.toContain('failed queued input');
          await vi.waitFor(() =>
            expect(pubsub.owners.get(`${scope.resourceId}\u0000${scope.threadId}`)).toBeUndefined(),
          );
        } finally {
          release();
          releaseFirst();
          subscription.unsubscribe();
        }
      },
    );
    it.each(['local', 'remote'] as const)(
      'clears pending and cross-agent idle work on %s abort only when requested',
      async origin => {
        for (const clearPendingSignals of [false, true]) {
          const scope = { resourceId: 'clear-user', threadId: `clear-${origin}-${clearPendingSignals}` };
          const pubsub = new ControlledLeasePubSub();
          const memory = new MockMemory();
          const { model, releaseFirst, getStreamCount } = createBlockingFirstTextStreamModel('first', 'next');
          const agent = new Agent({ id: 'clear-owner', name: 'Owner', instructions: 'Test', model, memory, pubsub });
          const other = new Agent({ id: 'clear-other', name: 'Other', instructions: 'Test', model, memory, pubsub });
          const subscription = origin === 'local' ? await agent.subscribeToThread(scope) : undefined;
          const remote = new AgentThreadStreamRuntime();
          const follower = await remote.subscribeToThread(agent, scope, pubsub);
          try {
            const output = await agent.stream('initial', {
              memory: { resource: scope.resourceId, thread: scope.threadId },
            });
            await vi.waitFor(() => expect(getStreamCount()).toBe(1));
            const pending = agent.sendSignal({ type: 'user-message', contents: 'pending to clear' }, scope);
            await pending.accepted;
            const idle = other.queueMessage('other agent idle', scope);
            await idle.accepted;
            await vi.waitFor(() => expect(follower.activeRunId()).toBe(output.runId));
            expect(
              origin === 'local'
                ? agent.abortThreadStream({ ...scope, clearPendingSignals })
                : follower.abort({ clearPendingSignals }),
            ).toBe(true);
            await vi.waitFor(() =>
              expect(
                pubsub.publishedData.some(data => data.type === 'run-aborted' && data.runId === output.runId),
              ).toBe(true),
            );
            releaseFirst();
            await output.text;
            await vi.waitFor(() =>
              expect(agentThreadStreamRuntime.getActiveThreadRunId(scope, pubsub)).toBeUndefined(),
            );
            await vi.waitFor(() => expect(getStreamCount()).toBe(clearPendingSignals ? 1 : 3));
            await vi.waitFor(() =>
              expect(pubsub.owners.get(`${scope.resourceId}\u0000${scope.threadId}`)).toBeUndefined(),
            );
            const { messages } = await memory.recall(scope);
            const texts = JSON.stringify(messages);
            if (clearPendingSignals) {
              expect(texts).not.toContain('pending to clear');
              expect(texts).not.toContain('other agent idle');
            } else {
              expect(texts).toContain('pending to clear');
              expect(texts).toContain('other agent idle');
            }
            await vi.waitFor(() =>
              expect(pubsub.owners.get(`${scope.resourceId}\u0000${scope.threadId}`)).toBeUndefined(),
            );
          } finally {
            releaseFirst();
            subscription?.unsubscribe();
            follower.unsubscribe();
          }
        }
      },
    );

    it('cancels selected pre-run and idle signals across Agents without changing owner-group cancellation', async () => {
      const scope = { resourceId: 'selected-user', threadId: 'selected-thread' };
      const pubsub = new ControlledLeasePubSub();
      const memory = new MockMemory();
      const model = createTextStreamModel('answer');
      let preparing!: () => void;
      const prepared = new Promise<void>(resolve => {
        preparing = resolve;
      });
      let release!: () => void;
      const gate = new Promise<void>(resolve => {
        release = resolve;
      });
      const agent = new Agent({
        id: 'selected-owner',
        name: 'Owner',
        model,
        memory,
        pubsub,
        instructions: async () => {
          preparing();
          await gate;
          return 'Test';
        },
      });
      const other = new Agent({ id: 'selected-other', name: 'Other', model, memory, pubsub, instructions: 'Test' });
      const counts: number[] = [];
      const unsubscribe = agent.subscribeThreadEvents(scope, event => counts.push(event.count));
      const subscription = await agent.subscribeToThread(scope);
      const started = agent.stream('initial', { memory: { resource: scope.resourceId, thread: scope.threadId } });
      try {
        await prepared;
        const preRun = agent.sendSignal({ type: 'user-message', contents: 'remove pre-run' }, scope);
        await preRun.accepted;
        const idle = other.queueMessage('remove other idle', { ...scope, queueOwnerId: 'group' });
        await idle.accepted;
        await other.queueMessage('keep other idle', { ...scope, queueOwnerId: 'keep' }).accepted;
        expect(agent.cancelQueuedMessages({ ...scope, queueOwnerId: 'group' })).toEqual({ cancelledSignalIds: [] });
        expect(
          agent.cancelQueuedMessages({
            ...scope,
            threadId: 'wrong-thread',
            signalIds: [preRun.signal.id, idle.signal.id],
          }),
        ).toEqual({ cancelledSignalIds: [] });
        expect(
          agent.cancelQueuedMessages({ ...scope, resourceId: 'wrong-user', signalIds: [preRun.signal.id] }),
        ).toEqual({ cancelledSignalIds: [] });
        expect(
          other.cancelQueuedMessages({
            ...scope,
            signalIds: [preRun.signal.id, idle.signal.id, preRun.signal.id, 'missing'],
          }),
        ).toEqual({ cancelledSignalIds: [preRun.signal.id, idle.signal.id] });
        expect(other.cancelQueuedMessages({ ...scope, signalIds: [preRun.signal.id] })).toEqual({
          cancelledSignalIds: [],
        });
        expect(counts.at(-1)).toBe(1);
        release();
        const output = await started;
        await output.text;
        await vi.waitFor(() => expect(model.doStreamCalls).toHaveLength(2));
        await vi.waitFor(() => expect(agentThreadStreamRuntime.getActiveThreadRunId(scope, pubsub)).toBeUndefined());
        const { messages } = await memory.recall(scope);
        expect(JSON.stringify(messages)).not.toContain('remove pre-run');
        expect(JSON.stringify(messages)).not.toContain('remove other idle');
        expect(JSON.stringify(messages)).toContain('keep other idle');
        expect(model.doStreamCalls).toHaveLength(2);
        expect(counts.at(-1)).toBe(0);
      } finally {
        release();
        subscription.unsubscribe();
        unsubscribe();
      }
    });

    it('does not report a signal as cancelled after forwarding to another owner starts', async () => {
      const scope = { resourceId: 'forward-user', threadId: 'forward-thread' };
      const key = `${scope.resourceId}\u0000${scope.threadId}`;
      const pubsub = new ControlledLeasePubSub();
      const { model, releaseFirst, getStreamCount } = createBlockingFirstTextStreamModel('first', 'next');
      const agent = new Agent({
        id: 'forward-agent',
        name: 'Forward',
        instructions: 'Test',
        model,
        memory: new MockMemory(),
        pubsub,
      });
      const subscription = await agent.subscribeToThread(scope);
      let forwarding!: () => void;
      const forwarded = new Promise<void>(resolve => {
        forwarding = resolve;
      });
      let release!: () => void;
      const gate = new Promise<void>(resolve => {
        release = resolve;
      });
      const publish = pubsub.publish.bind(pubsub);
      vi.spyOn(pubsub, 'publish').mockImplementation(async (topic, event) => {
        if (event.data?.type === 'signal-enqueued' && event.data.runId === 'new-owner') {
          forwarding();
          await gate;
        }
        await publish(topic, event);
      });
      try {
        const output = await agent.stream('initial', {
          memory: { resource: scope.resourceId, thread: scope.threadId },
        });
        await vi.waitFor(() => expect(getStreamCount()).toBe(1));
        const queued = agent.sendSignal({ type: 'user-message', contents: 'forwarded input' }, scope);
        await queued.accepted;
        pubsub.onTransferLease = () => {
          pubsub.owners.set(key, 'new-owner');
        };
        expect(subscription.abort()).toBe(true);
        releaseFirst();
        await output.text;
        await forwarded;
        expect(agent.cancelQueuedMessages({ ...scope, signalIds: [queued.signal.id] })).toEqual({
          cancelledSignalIds: [],
        });
        release();
        await vi.waitFor(() =>
          expect(pubsub.publishedData.some(data => data.type === 'signal-enqueued' && data.runId === 'new-owner')).toBe(
            true,
          ),
        );
        expect(getStreamCount()).toBe(1);
      } finally {
        release();
        releaseFirst();
        subscription.unsubscribe();
      }
    });

    it.each(
      (['pending', 'idle'] as const).flatMap(queue =>
        (['success', 'lost', 'transfer-rejection'] as const).flatMap(outcome =>
          (['local', 'remote'] as const).map(source => [queue, outcome, source] as const),
        ),
      ),
    )(
      'does not restore or forward a cancelled %s signal after lease %s via %s cancellation',
      async (queue, outcome, source) => {
        const scope = { resourceId: 'cancel-transfer-user', threadId: `cancel-transfer-${queue}-${outcome}-${source}` };
        const key = `${scope.resourceId}\u0000${scope.threadId}`;
        const pubsub = new ControlledLeasePubSub();
        const memory = new MockMemory();
        const { model, releaseFirst, getStreamCount } = createBlockingFirstTextStreamModel('first', 'after');
        const agent = new Agent({
          id: 'cancel-transfer',
          name: 'Transfer',
          instructions: 'Test',
          model,
          memory,
          pubsub,
        });
        const subscription = source === 'local' ? await agent.subscribeToThread(scope) : undefined;
        let transferStarted!: () => void;
        const transferring = new Promise<void>(resolve => {
          transferStarted = resolve;
        });
        let release!: () => void;
        const gate = new Promise<void>(resolve => {
          release = resolve;
        });
        try {
          const output = await agent.stream('initial', {
            memory: { resource: scope.resourceId, thread: scope.threadId },
          });
          await vi.waitFor(() => expect(getStreamCount()).toBe(1));
          const queued =
            queue === 'pending'
              ? agent.sendSignal({ type: 'user-message', contents: 'cancel during lease' }, scope)
              : agent.queueMessage('cancel during lease', scope);
          await queued.accepted;
          pubsub.transferLeaseWait = gate;
          pubsub.onTransferLease = transferStarted;
          const acquireLease = vi.spyOn(pubsub, 'acquireLease');
          // Provider rejection falls back to acquisition, rather than entering failed-start recovery.
          if (outcome === 'transfer-rejection')
            vi.spyOn(pubsub, 'transferLease').mockImplementationOnce(async () => {
              transferStarted();
              await gate;
              throw new Error('Lease unavailable');
            });
          expect(agent.abortThreadStream(scope)).toBe(true);
          releaseFirst();
          await output.text;
          await transferring;
          if (source === 'remote') {
            await pubsub.publish(`agent.thread-stream.${encodeURIComponent(key)}`, {
              type: 'signals-cancelled',
              data: { type: 'signals-cancelled', signalIds: [queued.signal.id] },
            });
            await pubsub.flush();
            await nextTick();
          } else {
            expect(agent.cancelQueuedMessages({ ...scope, signalIds: [queued.signal.id, queued.signal.id] })).toEqual({
              cancelledSignalIds: [queued.signal.id],
            });
          }
          expect(agent.cancelQueuedMessages({ ...scope, signalIds: [queued.signal.id] })).toEqual({
            cancelledSignalIds: [],
          });
          if (outcome === 'lost') pubsub.owners.set(key, 'new-owner');
          release();
          await vi.waitFor(() => expect(agentThreadStreamRuntime.getActiveThreadRunId(scope, pubsub)).toBeUndefined());
          expect(getStreamCount()).toBe(1);
          expect(
            pubsub.publishedData.filter(
              data =>
                data.type === 'signal-enqueued' && data.signal.id === queued.signal.id && data.runId !== output.runId,
            ),
          ).toEqual([]);
          if (outcome === 'transfer-rejection') {
            // Fail-closed contract: a rejected handoff leaves the lease
            // outcome unknown, so nothing may fall back to acquisition — the
            // cancelled input is neither restored nor forwarded above, and no
            // competing run may start on the strength of the rejection.
            expect(acquireLease).not.toHaveBeenCalled();
            if (queue === 'pending') {
              // The pending-drain catch publishes its authenticated failure
              // terminal with the truthful cause before reconciling ownership.
              expect(
                pubsub.publishedData.some(
                  data =>
                    data.type === 'run-failed' &&
                    data.error?.includes('Lease unavailable') &&
                    data.error?.includes('cancelled'),
                ),
              ).toBe(true);
            } else {
              // The cancelled idle settlement reports no run-failed terminal;
              // the truthful cause is retained as the item's rejected-run
              // receipt and surfaced to its output waiters.
              expect(pubsub.publishedData.some(data => data.type === 'run-failed')).toBe(false);
              await expect(agentThreadStreamRuntime.waitForRunOutput(queued.runId, pubsub)).rejects.toThrow(
                'Lease unavailable',
              );
              // Ownership stayed unreadable, so the predecessor's lease was
              // never released by the settlement — its stopped renewal lets
              // the TTL lapse (simulated here) instead of an unsafe release.
              expect(pubsub.owners.get(key)).toBeTruthy();
              await pubsub.releaseLease(key, pubsub.owners.get(key) as string);
            }
          }
          if (outcome === 'lost') {
            expect(pubsub.owners.get(key)).toBe('new-owner');
            await pubsub.releaseLease(key, 'new-owner');
          } else await vi.waitFor(() => expect(pubsub.owners.get(key)).toBeUndefined());
          const after = await agent.stream('after cancellation', {
            memory: { resource: scope.resourceId, thread: scope.threadId },
          });
          await after.text;
          expect(model.doStreamCalls).toHaveLength(2);
          expect(JSON.stringify(model.doStreamCalls[1]?.prompt)).not.toContain('cancel during lease');
          if (source === 'remote')
            await vi.waitFor(() =>
              expect(pubsub.subscriberCount(`agent.thread-stream.${encodeURIComponent(key)}`)).toBe(0),
            );
        } finally {
          release();
          releaseFirst();
          subscription?.unsubscribe();
        }
      },
    );
  });

  it.each(['pre-run', 'pending'] as const)(
    'admits a forwarded signal once when first queued as %s',
    async firstQueue => {
      const pubsub = new ControlledLeasePubSub();
      const scope = { resourceId: 'forwarded-pre-run-user', threadId: 'forwarded-pre-run-thread' };
      const memory = new MockMemory();
      const model = createTextStreamModel('winner answer');
      const agent = new Agent({
        id: 'forwarded-pre-run',
        name: 'Forwarded pre-run',
        instructions: 'Test',
        model,
        memory,
        pubsub,
      });
      const subscription = await agent.subscribeToThread(scope);
      const topic = `agent.thread-stream.${encodeURIComponent(`${scope.resourceId}\u0000${scope.threadId}`)}`;
      const signal = createSignal({
        id: 'same-forwarded-signal',
        type: 'user-message',
        contents: 'Only handle A once',
      });
      try {
        // Authentic reserve+prepare fixture: only a genuine reservation
        // (never a bare idle wait) admits run-addressed deliveries into the
        // local queues. The winner's stream below adopts this reservation
        // through the same ownership marker the runtime's own follow-up
        // handoff uses.
        const prepared = agentThreadStreamRuntime.prepareRunOptions(
          { runId: 'winner-run', memory: { resource: scope.resourceId, thread: scope.threadId } } as any,
          pubsub,
        );
        const releaseReservation = agentThreadStreamRuntime.reserveRun(prepared, pubsub, agent.id);
        expect(releaseReservation).toBeDefined();
        await vi.waitFor(() => expect(pubsub.subscriberCount(topic)).toBe(2));
        for (const preRun of [firstQueue === 'pre-run', firstQueue !== 'pre-run']) {
          const runId = 'winner-run';
          await pubsub.publish(
            `agent.thread-stream.${encodeURIComponent(`${scope.resourceId}\u0000${scope.threadId}`)}`,
            {
              type: 'signal-enqueued',
              runId,
              data: { type: 'signal-enqueued', runId, signal: signal.toDataPart().data, preRun, sourceId: 'old-owner' },
            },
          );
        }
        await pubsub.flush();
        const output = await agent.stream('winner input', {
          runId: 'winner-run',
          memory: { resource: scope.resourceId, thread: scope.threadId },
          _threadRunReservationOwner: true,
        } as any);
        await output.text;
        await vi.waitFor(() => expect(agentThreadStreamRuntime.getActiveThreadRunId(scope, pubsub)).toBeUndefined());
        expect(model.doStreamCalls).toHaveLength(firstQueue === 'pre-run' ? 1 : 2);
        const prompt = model.doStreamCalls.at(-1)?.prompt;
        expect(JSON.stringify(prompt).match(/Only handle A once/g)).toHaveLength(1);
        const { messages } = await memory.recall(scope);
        expect(
          messages.filter(message =>
            message.content.parts.some(part => part.type === 'text' && part.text === 'Only handle A once'),
          ),
        ).toHaveLength(1);
      } finally {
        agentThreadStreamRuntime.releaseThreadRunReservation('winner-run', pubsub);
        subscription.unsubscribe();
      }
    },
  );

  it.each(['thread', 'upstream', 'none'] as const)(
    'preserves pending order after %s cancellation and idle preparation failure',
    async cancellation => {
      const scope = { resourceId: 'idle-failure-user', threadId: `idle-failure-${cancellation}` };
      const pubsub = new ControlledLeasePubSub();
      const memory = new MockMemory();
      const { model, releaseFirst, getStreamCount } = createBlockingFirstTextStreamModel('first response', 'answer');
      let preparing!: () => void;
      const prepared = new Promise<void>(resolve => {
        preparing = resolve;
      });
      let release!: () => void;
      const gate = new Promise<void>(resolve => {
        release = resolve;
      });
      const abort = new AbortController();
      let calls = 0;
      const agent = new Agent({
        id: 'idle-failure',
        name: 'Idle failure',
        model,
        memory,
        pubsub,
        instructions: async () => {
          if (++calls === 2) {
            preparing();
            await gate;
            throw new Error('Idle preparation failed');
          }
          return 'Test';
        },
      });
      const subscription = await agent.subscribeToThread(scope);
      try {
        const initial = await agent.stream('initial', {
          memory: { resource: scope.resourceId, thread: scope.threadId },
        });
        await vi.waitFor(() => expect(getStreamCount()).toBe(1));
        await agent.queueMessage('failed startup', {
          ...scope,
          ifIdle: { streamOptions: { abortSignal: abort.signal } },
        }).accepted;
        releaseFirst();
        await initial.text;
        await prepared;
        await agent.sendSignal({ type: 'user-message', contents: 'pre-run A' }, scope).accepted;
        await agent.queueMessage('idle C', scope).accepted;
        if (cancellation === 'thread') expect(subscription.abort()).toBe(true);
        if (cancellation === 'upstream') abort.abort();
        release();
        await vi.waitFor(() => expect(pubsub.publishedData.some(data => data.type === 'run-failed')).toBe(true));
        // Fork (PF-4402): the completion handoff to queued idle work is
        // asynchronous (verified-owner forwarding runs first), so the thread
        // can be briefly idle between the recovered run and idle C.
        await vi.waitFor(() => expect(model.doStreamCalls).toHaveLength(3));
        await vi.waitFor(() => expect(agentThreadStreamRuntime.getActiveThreadRunId(scope, pubsub)).toBeUndefined());
        const { messages } = await memory.recall(scope);
        const order = messages.flatMap(message =>
          message.content.parts.flatMap(part =>
            part.type === 'text' && ['pre-run A', 'idle C'].includes(part.text) ? [part.text] : [],
          ),
        );
        expect(order).toEqual(['pre-run A', 'idle C']);
        expect(model.doStreamCalls).toHaveLength(3);
        expect(JSON.stringify(model.doStreamCalls[1]?.prompt)).toContain('pre-run A');
        expect(JSON.stringify(model.doStreamCalls[1]?.prompt)).not.toContain('idle C');
        expect(JSON.stringify(model.doStreamCalls[2]?.prompt)).toContain('idle C');
        expect(calls).toBe(4);
        await vi.waitFor(() => expect(pubsub.owners.get(`${scope.resourceId}\u0000${scope.threadId}`)).toBeUndefined());
      } finally {
        releaseFirst();
        release();
        subscription.unsubscribe();
      }
    },
  );

  it.each(['success', 'failure'] as const)(
    'delivers pre-run and idle signals after aborting before preparation %s',
    async outcome => {
      const scope = { resourceId: 'preparation-abort-user', threadId: 'preparation-abort-thread' };
      const pubsub = new ControlledLeasePubSub();
      const memory = new MockMemory();
      let reachedPreparation!: () => void;
      const preparing = new Promise<void>(resolve => {
        reachedPreparation = resolve;
      });
      let releasePreparation!: () => void;
      const preparationGate = new Promise<void>(resolve => {
        releasePreparation = resolve;
      });
      const model = createTextStreamModel('queued answer');
      let firstPreparation = true;
      const agent = new Agent({
        id: 'preparation-abort',
        name: 'Preparation abort',
        model,
        memory,
        pubsub,
        instructions: async () => {
          if (!firstPreparation) return 'Test';
          firstPreparation = false;
          reachedPreparation();
          await preparationGate;
          if (outcome === 'failure') throw new Error('Preparation failed after abort');
          return 'Test';
        },
      });
      const subscription = await agent.subscribeToThread(scope);
      const runId = 'aborted-preparation';
      const streamPromise = agent.stream('first', {
        runId,
        memory: { resource: scope.resourceId, thread: scope.threadId },
      });
      void streamPromise.catch(() => {});
      try {
        await preparing;
        expect(agentThreadStreamRuntime.getActiveThreadRunId(scope, pubsub)).toBe(runId);
        expect(agentThreadStreamRuntime.hasThreadRun(runId, pubsub)).toBe(false);
        await agent.sendSignal({ type: 'user-message', contents: 'pre-run A' }, scope).accepted;
        await agent.queueMessage('idle B', scope).accepted;
        expect(subscription.abort()).toBe(true);
        expect(agentThreadStreamRuntime.drainPendingSignals(runId, pubsub, 'pre-run')).toEqual([]);
        releasePreparation();
        if (outcome === 'failure') {
          await expect(streamPromise).rejects.toThrow('Preparation failed after abort');
        } else {
          const stream = await streamPromise;
          await stream.text;
        }
        await vi.waitFor(() => expect(agentThreadStreamRuntime.getActiveThreadRunId(scope, pubsub)).toBeUndefined());
        await vi.waitFor(() => expect(model.doStreamCalls).toHaveLength(2));
        const { messages } = await memory.recall(scope);
        const order = messages.flatMap(message =>
          message.content.parts.flatMap(part =>
            part.type === 'text' && ['pre-run A', 'idle B'].includes(part.text) ? [part.text] : [],
          ),
        );
        expect(order).toEqual(['pre-run A', 'idle B']);
        expect(JSON.stringify(model.doStreamCalls[0]?.prompt)).toContain('pre-run A');
        expect(JSON.stringify(model.doStreamCalls[0]?.prompt)).not.toContain('idle B');
        expect(JSON.stringify(model.doStreamCalls[1]?.prompt)).toContain('idle B');
        await vi.waitFor(() => expect(pubsub.owners.get(`${scope.resourceId}\u0000${scope.threadId}`)).toBeUndefined());
      } finally {
        releasePreparation();
        subscription.unsubscribe();
      }
    },
  );

  // The follower side of the mixed-queue fixture needs a real Agent wired to
  // its own module singleton: `Agent.stream` reserves and registers through
  // the module singleton its own module graph imported, so a bare
  // `new AgentThreadStreamRuntime()` holding the wake lease can never start
  // the queued follow-up itself — the singleton mints its own lease attempt
  // token and loses `acquireLease` against the follower's. Vitest 4 has no
  // `vi.isolateModules*`; the supported equivalent is `vi.resetModules()`
  // followed by dynamic imports, which re-evaluate the invalidated modules
  // into a fresh graph. The static imports above stay bound to the original
  // graph, `vi.unmock('node:crypto')` still applies to the re-evaluated graph
  // (its runtime source id stays a native UUID), and no test in this file
  // dynamically imports these modules afterwards, so no post-test reset is
  // needed. Each follower cell re-evaluates so its runtime starts with empty
  // state maps, exactly like the per-cell `new AgentThreadStreamRuntime()`
  // observer used for owner-queued cells.
  async function loadIsolatedFollowerGraph(): Promise<{
    Agent: typeof Agent;
    runtime: typeof agentThreadStreamRuntime;
  }> {
    await vi.dynamicImportSettled();
    vi.resetModules();
    const [{ Agent: FollowerAgent }, { agentThreadStreamRuntime: followerRuntime }] = await Promise.all([
      import('../agent'),
      import('../thread-stream-runtime'),
    ]);
    return { Agent: FollowerAgent, runtime: followerRuntime };
  }

  it.each(
    (['none', 'thread', 'upstream', 'remote'] as const).flatMap(cancellation =>
      (['owner', 'follower'] as const).map(queueLocation => [cancellation, queueLocation] as const),
    ),
  )(
    'preserves pending-before-idle order after %s cancellation with idle work on the %s',
    async (cancellation, queueLocation) => {
      const scope = { resourceId: 'mixed-queue-user', threadId: `mixed-${cancellation}` };
      const pubsub = new ControlledLeasePubSub();
      const memory = new MockMemory();
      const upstream = new AbortController();
      const { model, releaseFirst, getStreamCount } = createBlockingFirstTextStreamModel(
        'first response',
        'later response',
      );
      const agent = new Agent({ id: 'mixed-queue', name: 'Mixed queue', instructions: 'Test', model, memory, pubsub });
      const subscription = await agent.subscribeToThread(scope);
      // A separate runtime has no owner records and must forward abort over PubSub.
      // For follower-queued cells it must also be able to run the queued follow-up
      // itself, so it gets an isolated Agent + module-singleton pair that shares
      // only the test pubsub, memory, and model with the owner — the ordering
      // assertions below keep covering real runs on both sides.
      const followerPair = queueLocation === 'follower' ? await loadIsolatedFollowerGraph() : undefined;
      const followerAgent = followerPair
        ? new followerPair.Agent({
            id: 'mixed-queue-follower',
            name: 'Mixed queue follower',
            instructions: 'Test',
            model,
            memory,
            pubsub,
          })
        : undefined;
      const follower = followerPair?.runtime ?? new AgentThreadStreamRuntime();
      const remoteSubscription = await follower.subscribeToThread(followerAgent ?? agent, scope, pubsub);
      const stream = await agent.stream('first', {
        memory: { resource: scope.resourceId, thread: scope.threadId },
        abortSignal: upstream.signal,
      });
      try {
        await vi.waitFor(() => expect(getStreamCount()).toBe(1));
        await agent.sendSignal({ type: 'user-message', contents: 'pending A' }, scope).accepted;
        await vi.waitFor(() => expect(remoteSubscription.activeRunId()).toBe(stream.runId));
        await (
          queueLocation === 'owner'
            ? agent.queueMessage('idle B', scope)
            : follower.queueMessage(followerAgent!, 'idle B', scope, pubsub)
        ).accepted;
        if (cancellation === 'thread') expect(subscription.abort()).toBe(true);
        if (cancellation === 'upstream') upstream.abort();
        if (cancellation === 'remote') {
          await vi.waitFor(() => expect(remoteSubscription.activeRunId()).toBe(stream.runId));
          expect(remoteSubscription.abort()).toBe(true);
          await vi.waitFor(() =>
            expect(pubsub.publishedData.some(data => data.type === 'run-aborted' && data.runId === stream.runId)).toBe(
              true,
            ),
          );
          expect(
            pubsub.publishedData.some(data => data.type === 'run-abort-requested' && data.runId === stream.runId),
          ).toBe(true);
        }
        releaseFirst();
        await stream.text;
        await vi.waitFor(() => expect(getStreamCount()).toBe(3));
        await vi.waitFor(() => expect(agentThreadStreamRuntime.getActiveThreadRunId(scope, pubsub)).toBeUndefined());
        const { messages } = await memory.recall(scope);
        const order = messages.flatMap(message =>
          message.content.parts.flatMap(part =>
            part.type === 'text' && ['pending A', 'idle B'].includes(part.text) ? [part.text] : [],
          ),
        );
        expect(order).toEqual(['pending A', 'idle B']);
        expect(JSON.stringify(model.doStreamCalls[1]?.prompt)).toContain('pending A');
        expect(JSON.stringify(model.doStreamCalls[1]?.prompt)).not.toContain('idle B');
        expect(JSON.stringify(model.doStreamCalls[2]?.prompt)).toContain('idle B');
        await vi.waitFor(() => expect(pubsub.owners.get(`${scope.resourceId}\u0000${scope.threadId}`)).toBeUndefined());
      } finally {
        releaseFirst();
        subscription.unsubscribe();
        remoteSubscription.unsubscribe();
      }
    },
  );

  it('does not recursively retry a cancelled follow-up whose preparation also fails', async () => {
    const scope = { resourceId: 'failed-recovery-user', threadId: 'failed-recovery-thread' };
    const pubsub = new ControlledLeasePubSub();
    const memory = new MockMemory();
    const model = createTextStreamModel('recovered answer');
    let preparing!: () => void;
    const started = new Promise<void>(resolve => {
      preparing = resolve;
    });
    let release!: () => void;
    const gate = new Promise<void>(resolve => {
      release = resolve;
    });
    let preparations = 0;
    const agent = new Agent({
      id: 'failed-recovery',
      name: 'Failed recovery',
      memory,
      model,
      pubsub,
      instructions: async () => {
        preparations++;
        if (preparations === 1) {
          preparing();
          await gate;
          throw new Error('Initial preparation failed');
        }
        if (preparations === 2) {
          expect(subscription.abort()).toBe(true);
          throw new Error('Follow-up preparation failed');
        }
        return 'Test';
      },
    });
    const subscription = await agent.subscribeToThread(scope);
    const first = agent.stream('first', { memory: { resource: scope.resourceId, thread: scope.threadId } });
    void first.catch(() => {});
    try {
      await started;
      await agent.sendSignal({ type: 'user-message', contents: 'preserved A' }, scope).accepted;
      await agent.sendSignal({ type: 'user-message', contents: 'preserved B' }, scope).accepted;
      expect(subscription.abort()).toBe(true);
      release();
      await expect(first).rejects.toThrow('Initial preparation failed');
      await vi.waitFor(() =>
        expect(
          pubsub.publishedData.some(
            data => data.type === 'run-failed' && data.error.includes('Follow-up preparation failed'),
          ),
        ).toBe(true),
      );
      await vi.waitFor(() => expect(pubsub.owners.get(`${scope.resourceId}\u0000${scope.threadId}`)).toBeUndefined());
      expect(preparations).toBe(2);
      expect(model.doStreamCalls).toHaveLength(0);
      const recovered = await agent.stream('natural next turn', {
        memory: { resource: scope.resourceId, thread: scope.threadId },
      });
      await recovered.text;
      await vi.waitFor(() => expect(agentThreadStreamRuntime.getActiveThreadRunId(scope, pubsub)).toBeUndefined());
      expect(preparations).toBe(3);
      const { messages } = await memory.recall(scope);
      const preserved = messages.flatMap(message =>
        message.content.parts.flatMap(part =>
          part.type === 'text' && ['preserved A', 'preserved B'].includes(part.text) ? [part.text] : [],
        ),
      );
      expect(preserved).toEqual(['preserved A', 'preserved B']);
      expect(model.doStreamCalls).toHaveLength(2);
      expect(JSON.stringify(model.doStreamCalls[1]?.prompt)).toContain('preserved A');
      expect(JSON.stringify(model.doStreamCalls[1]?.prompt)).toContain('preserved B');
    } finally {
      release();
      subscription.unsubscribe();
    }
  });

  it('executes forwarded messages once on a real winner after an aborted owner loses its lease', async () => {
    const scope = { resourceId: 'real-winner-user', threadId: 'real-winner-thread' };
    const key = `${scope.resourceId}\u0000${scope.threadId}`;
    const pubsub = new ControlledLeasePubSub();
    const owner = new AgentThreadStreamRuntime();
    const memory = new MockMemory();
    // The winner's first model call blocks until released: registration +
    // lease acquisition complete before the run produces output, so the
    // owner's parked transfer resumes only against the winner's genuine
    // attempt token (readiness), while the run itself stays unfinished
    // (completion) until the forwarded work has arrived.
    const { model, releaseFirst, getStreamCount } = createBlockingFirstTextStreamModel(
      'winner answer',
      'winner answer',
    );
    const agent = new Agent({
      id: 'real-winner',
      name: 'Real winner',
      model,
      memory,
      pubsub,
      instructions: 'Test',
    });
    const ownerSubscription = await owner.subscribeToThread(agent, scope, pubsub);
    const winnerSubscription = await agent.subscribeToThread(scope);
    let finishOwner!: () => void;
    const ownerFinished = new Promise<void>(resolve => {
      finishOwner = resolve;
    });
    const oldRunId = 'aborted-losing-owner';
    const winnerRunId = 'real-winning-run';
    const options = owner.prepareRunOptions(
      { runId: oldRunId, memory: { resource: scope.resourceId, thread: scope.threadId } },
      pubsub,
    );
    const registration = owner.registerRun(
      agent,
      {
        runId: oldRunId,
        status: 'running',
        fullStream: (async function* () {})(),
        _waitUntilFinished: () => ownerFinished,
      } as any,
      options,
      pubsub,
    );
    void registration?.catch(() => {});
    let releaseTransfer!: () => void;
    const transferGate = new Promise<void>(resolve => {
      releaseTransfer = resolve;
    });
    let transferring!: () => void;
    const transferStarted = new Promise<void>(resolve => {
      transferring = resolve;
    });
    try {
      const pending = owner.sendSignal(agent, { type: 'user-message', contents: 'pending A' }, scope, pubsub);
      await pending.accepted;
      const idle = owner.queueMessage(agent, 'idle B', scope, pubsub);
      await idle.accepted;
      pubsub.transferLeaseWait = transferGate;
      pubsub.onTransferLease = transferring;
      expect(ownerSubscription.abort()).toBe(true);
      finishOwner();
      await transferStarted;
      // Simulate the predecessor lease lapsing while its transfer is parked:
      // the winner below acquires the key under its own genuine attempt
      // token instead of a seeded raw run id (which can never own a lease
      // and would make the winner lose to itself). Registration readiness
      // (run-registered) is separate from completion: the winner's first
      // model call stays blocked until releaseFirst() below.
      pubsub.owners.delete(key);
      const winning = agent.stream('winner input', {
        runId: winnerRunId,
        memory: { resource: scope.resourceId, thread: scope.threadId },
      });
      // The winner's first model call has started: it registered and
      // acquired the lease under its genuine token before blocking.
      await vi.waitFor(() => expect(getStreamCount()).toBe(1));
      // The winner's exact attempt lease token, captured from its
      // registration publication — never the public run id. Readiness is
      // awaited separately from completion (the run stays gated below).
      await vi.waitFor(() =>
        expect(pubsub.publishedData.some(data => data.type === 'run-registered' && data.runId === winnerRunId)).toBe(
          true,
        ),
      );
      const winnerLeaseOwner = pubsub.publishedData.find(
        data => data.type === 'run-registered' && data.runId === winnerRunId,
      )?.leaseOwner;
      expect(winnerLeaseOwner).toBeTruthy();
      expect(pubsub.owners.get(key)).toBe(winnerLeaseOwner);
      // Keep the parked owner transfer gated until the winner is established:
      // its transfer then fails against the winner's token and its acquire
      // reports the verified foreign winner, so the loss handoff forwards.
      releaseTransfer();
      await vi.waitFor(() =>
        expect(
          pubsub.publishedData.filter(
            data =>
              data.type === 'signal-enqueued' &&
              data.runId === winnerRunId &&
              [pending.signal.id, idle.signal.id].includes(data.signal.id),
          ),
        ).toHaveLength(2),
      );
      await pubsub.flush();
      // The winner is still blocked in its first model call, so no follow-up
      // has started yet.
      expect(getStreamCount()).toBe(1);
      releaseFirst();
      const output = await winning;
      await output.text;
      await vi.waitFor(() => expect(agentThreadStreamRuntime.getActiveThreadRunId(scope, pubsub)).toBeUndefined());
      expect(model.doStreamCalls).toHaveLength(2);
      const prompt = JSON.stringify(model.doStreamCalls[1]?.prompt);
      expect(prompt.match(/pending A/g)).toHaveLength(1);
      expect(prompt.match(/idle B/g)).toHaveLength(1);
      const { messages } = await memory.recall(scope);
      const delivered = messages.flatMap(message =>
        message.content.parts.flatMap(part =>
          part.type === 'text' && ['pending A', 'idle B'].includes(part.text) ? [part.text] : [],
        ),
      );
      expect(delivered).toEqual(['pending A', 'idle B']);
      expect(messages.filter(message => message.role === 'assistant')).toHaveLength(2);
      await vi.waitFor(() => expect(pubsub.owners.get(key)).toBeUndefined());
      const next = await agent.stream('after handoff', {
        memory: { resource: scope.resourceId, thread: scope.threadId },
      });
      await next.text;
      expect(model.doStreamCalls).toHaveLength(3);
      await vi.waitFor(() => expect(pubsub.owners.get(key)).toBeUndefined());
    } finally {
      finishOwner();
      releaseTransfer();
      releaseFirst();
      ownerSubscription.unsubscribe();
      winnerSubscription.unsubscribe();
    }
  });

  async function setupForeignWinnerHandoff(suffix: string) {
    // Two-runtime handoff fixture: `owner` observes while the winner runs
    // on the agent's own runtime. The owner stub holds the lease until the
    // test hands it to the winner, mirroring the real-winner ordering test.
    const scope = { resourceId: `${suffix}-user`, threadId: `${suffix}-thread` };
    const key = `${scope.resourceId}\u0000${scope.threadId}`;
    const topic = `agent.thread-stream.${encodeURIComponent(key)}`;
    const pubsub = new ControlledLeasePubSub();
    const owner = new AgentThreadStreamRuntime();
    const memory = new MockMemory();
    const first = createBlockingFirstTextStreamModel('winner answer', 'winner answer');
    const agent = new Agent({
      id: `${suffix}-winner`,
      name: 'Handoff winner',
      instructions: 'Test',
      model: first.model,
      memory,
      pubsub,
    });
    const ownerSubscription = await owner.subscribeToThread(agent, scope, pubsub);
    const winnerSubscription = await agent.subscribeToThread(scope);
    let finishOwner!: () => void;
    const ownerFinished = new Promise<void>(resolve => {
      finishOwner = resolve;
    });
    const oldRunId = `${suffix}-owner`;
    const winnerRunId = `${suffix}-winner-run`;
    const options = owner.prepareRunOptions(
      { runId: oldRunId, memory: { resource: scope.resourceId, thread: scope.threadId } } as any,
      pubsub,
    );
    const registration = owner.registerRun(
      agent,
      {
        runId: oldRunId,
        // `running` keeps the stub thread-blocking so follow-up input
        // pending-queues (the completed path still runs on finish).
        status: 'running',
        fullStream: (async function* () {})(),
        _waitUntilFinished: () => ownerFinished,
      } as any,
      options,
      pubsub,
    );
    void registration?.catch(() => {});
    return {
      scope,
      key,
      topic,
      pubsub,
      owner,
      memory,
      agent,
      model: first.model,
      releaseFirst: first.releaseFirst,
      getStreamCount: first.getStreamCount,
      ownerSubscription,
      winnerSubscription,
      oldRunId,
      winnerRunId,
      finishOwner,
    };
  }

  async function seizeLeaseForWinner(fixture: Awaited<ReturnType<typeof setupForeignWinnerHandoff>>) {
    // The predecessor lease lapses and the winner acquires the key under
    // its genuine attempt token, then the owner runtime projects it.
    fixture.pubsub.owners.delete(fixture.key);
    const winning = fixture.agent.stream('winner input', {
      runId: fixture.winnerRunId,
      memory: { resource: fixture.scope.resourceId, thread: fixture.scope.threadId },
    });
    await vi.waitFor(() => expect(fixture.getStreamCount()).toBe(1));
    await vi.waitFor(() =>
      expect(fixture.owner.getActiveThreadRunId(fixture.scope, fixture.pubsub)).toBe(fixture.winnerRunId),
    );
    return winning;
  }

  it('forwards pending, pre-run and idle input to an already-projected foreign winner at completion', async () => {
    // The winner registers while the owner is still finishing, so the
    // completion handoff (not the drain) must forward the ordered tail —
    // including pre-run input that arrives during settlement.
    const fixture = await setupForeignWinnerHandoff('handoff-completion');
    const { pubsub, owner, memory, model } = fixture;
    let releaseRead!: () => void;
    try {
      const pendingA = owner.sendSignal(
        fixture.agent,
        { type: 'user-message', contents: 'pending A' },
        fixture.scope,
        pubsub,
      );
      const pendingB = owner.sendSignal(
        fixture.agent,
        { type: 'user-message', contents: 'pending B' },
        fixture.scope,
        pubsub,
      );
      await pendingA.accepted;
      await pendingB.accepted;
      const idleC = owner.queueMessage(fixture.agent, 'idle C', fixture.scope, pubsub);
      const idleD = owner.queueMessage(fixture.agent, 'idle D', fixture.scope, pubsub);
      await idleC.accepted;
      await idleD.accepted;
      const winning = await seizeLeaseForWinner(fixture);
      // Gate the handoff's verification read: the pre-run injection below
      // must land during settlement, after the capture.
      await pubsub.flush();
      await nextTick();
      await nextTick();
      const readGate = new Promise<void>(resolve => {
        releaseRead = resolve;
      });
      const realGetLeaseOwner = pubsub.getLeaseOwner.bind(pubsub);
      vi.spyOn(pubsub, 'getLeaseOwner').mockImplementation(async (k: string) => {
        await readGate;
        return realGetLeaseOwner(k);
      });
      fixture.finishOwner();
      const preSignal = createSignal({
        id: 'settlement-pre-run',
        type: 'user-message',
        contents: 'pre-run during settlement',
      });
      await pubsub.publish(fixture.topic, {
        type: 'signal-enqueued',
        runId: fixture.oldRunId,
        data: {
          type: 'signal-enqueued',
          runId: fixture.oldRunId,
          signal: preSignal.toDataPart().data,
          preRun: true,
          sourceId: 'settlement-injector',
        },
      });
      await pubsub.flush();
      await nextTick();
      await nextTick();
      releaseRead();
      // Ordered forwarding tail: captured pending head, surviving pre-run
      // input, surviving pending tail, then eligible idle work.
      await vi.waitFor(() =>
        expect(
          pubsub.publishedData
            .filter(data => data.type === 'signal-enqueued' && data.runId === fixture.winnerRunId)
            .map(data => data.signal.id),
        ).toEqual([pendingA.signal.id, preSignal.id, pendingB.signal.id, idleC.signal.id, idleD.signal.id]),
      );
      fixture.releaseFirst();
      const output = await winning;
      await output.text;
      // Follow-up runs may batch several queued signals into one run (as
      // the during-transfer ordering test proves); the contract is
      // exactly-once ordered delivery, not one run per signal.
      const texts = ['pending A', 'pre-run during settlement', 'pending B', 'idle C', 'idle D'];
      await vi.waitFor(async () => {
        const { messages } = await memory.recall(fixture.scope);
        expect(
          texts.filter(text =>
            messages.some(message => message.content.parts.some(part => part.type === 'text' && part.text === text)),
          ),
        ).toEqual(texts);
      });
      const promptHaystack = model.doStreamCalls.map(call => JSON.stringify(call?.prompt)).join('\n');
      for (const text of texts) {
        expect(promptHaystack.match(new RegExp(text.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'g'))).toHaveLength(1);
      }
      const { messages } = await memory.recall(fixture.scope);
      for (const text of texts) {
        expect(
          messages.filter(message => message.content.parts.some(part => part.type === 'text' && part.text === text)),
        ).toHaveLength(1);
      }
      await vi.waitFor(() => expect(pubsub.owners.get(fixture.key)).toBeUndefined());
    } finally {
      try {
        releaseRead();
      } catch {
        // Already released on the success path.
      }
      fixture.finishOwner();
      fixture.releaseFirst();
      fixture.ownerSubscription.unsubscribe();
      fixture.winnerSubscription.unsubscribe();
    }
  });

  it('forwards queued input to an already-projected foreign winner from the aborted-terminal handoff', async () => {
    // Abort-before-entry variant of the completion handoff: the genuine
    // winner is projected BEFORE the old run aborts, so the
    // aborted-terminal handoff must share the forwarding-only disposition
    // instead of stranding the queue behind the ordinary drain's
    // active-run early return. Asserts ordered recipient execution on the
    // real winner, not merely retained queues.
    const fixture = await setupForeignWinnerHandoff('handoff-abort-entry');
    const { pubsub, owner, memory, model } = fixture;
    try {
      const pendingA = owner.sendSignal(
        fixture.agent,
        { type: 'user-message', contents: 'abort pending A' },
        fixture.scope,
        pubsub,
      );
      const pendingB = owner.sendSignal(
        fixture.agent,
        { type: 'user-message', contents: 'abort pending B' },
        fixture.scope,
        pubsub,
      );
      await pendingA.accepted;
      await pendingB.accepted;
      const idleC = owner.queueMessage(fixture.agent, 'abort idle C', fixture.scope, pubsub);
      await idleC.accepted;
      const winning = await seizeLeaseForWinner(fixture);
      await pubsub.flush();
      // The winner is already projected here: abort the OLD run directly
      // (a thread abort would target the currently-active winner and ask
      // it to stop). Aborting now must forward through the shared
      // disposition, never the ordinary drain.
      expect(owner.abortRun(fixture.oldRunId, pubsub)).toBe(true);
      fixture.finishOwner();
      await vi.waitFor(() =>
        expect(
          pubsub.publishedData
            .filter(data => data.type === 'signal-enqueued' && data.runId === fixture.winnerRunId)
            .map(data => data.signal.id),
        ).toEqual([pendingA.signal.id, pendingB.signal.id, idleC.signal.id]),
      );
      fixture.releaseFirst();
      const output = await winning;
      await output.text;
      // Exactly-once ordered recipient execution: the winner may fold the
      // admitted tail into its running turn or a follow-up run, so the
      // contract is one execution per signal in order, not a run count.
      const texts = ['abort pending A', 'abort pending B', 'abort idle C'];
      await vi.waitFor(async () => {
        const { messages } = await memory.recall(fixture.scope);
        expect(
          texts.filter(text =>
            messages.some(message => message.content.parts.some(part => part.type === 'text' && part.text === text)),
          ),
        ).toEqual(texts);
      });
      const promptHaystack = model.doStreamCalls.map(call => JSON.stringify(call?.prompt)).join('\n');
      for (const text of texts) {
        expect(promptHaystack.match(new RegExp(text.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'g'))).toHaveLength(1);
      }
      await vi.waitFor(() => expect(pubsub.owners.get(fixture.key)).toBeUndefined());
    } finally {
      fixture.finishOwner();
      fixture.releaseFirst();
      fixture.ownerSubscription.unsubscribe();
      fixture.winnerSubscription.unsubscribe();
    }
  });

  it('forwards already-present pre-run input ahead of pending input at completion entry', async () => {
    // Existing pre-run X plus pending A/B must forward as X/A/B, mirroring
    // the drain-entry fold — the pending queue supplies the capture only
    // when no pre-run input is already present.
    const fixture = await setupForeignWinnerHandoff('handoff-completion-prerun');
    const { pubsub, owner, memory, model } = fixture;
    try {
      const pendingA = owner.sendSignal(
        fixture.agent,
        { type: 'user-message', contents: 'entry pending A' },
        fixture.scope,
        pubsub,
      );
      const pendingB = owner.sendSignal(
        fixture.agent,
        { type: 'user-message', contents: 'entry pending B' },
        fixture.scope,
        pubsub,
      );
      await pendingA.accepted;
      await pendingB.accepted;
      const winning = await seizeLeaseForWinner(fixture);
      // Seed pre-run X BEFORE entry (but after the winner is established,
      // so no idle wake can claim the thread) through the same authentic
      // pre-run publication the settlement test uses; the flush parks it in
      // the pre-run queue ahead of the completion handoff.
      const preSignal = createSignal({
        id: 'entry-pre-run-x',
        type: 'user-message',
        contents: 'entry pre-run X',
      });
      await pubsub.publish(fixture.topic, {
        type: 'signal-enqueued',
        runId: fixture.oldRunId,
        data: {
          type: 'signal-enqueued',
          runId: fixture.oldRunId,
          signal: preSignal.toDataPart().data,
          preRun: true,
          sourceId: 'entry-injector',
        },
      });
      await pubsub.flush();
      await nextTick();
      await nextTick();
      fixture.finishOwner();
      await vi.waitFor(() =>
        expect(
          pubsub.publishedData
            .filter(data => data.type === 'signal-enqueued' && data.runId === fixture.winnerRunId)
            .map(data => data.signal.id),
        ).toEqual([preSignal.id, pendingA.signal.id, pendingB.signal.id]),
      );
      fixture.releaseFirst();
      const output = await winning;
      await output.text;
      const texts = ['entry pre-run X', 'entry pending A', 'entry pending B'];
      await vi.waitFor(async () => {
        const { messages } = await memory.recall(fixture.scope);
        expect(
          texts.filter(text =>
            messages.some(message => message.content.parts.some(part => part.type === 'text' && part.text === text)),
          ),
        ).toEqual(texts);
      });
      const promptHaystack = model.doStreamCalls.map(call => JSON.stringify(call?.prompt)).join('\n');
      for (const text of texts) {
        expect(promptHaystack.match(new RegExp(text.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'g'))).toHaveLength(1);
      }
      await vi.waitFor(() => expect(pubsub.owners.get(fixture.key)).toBeUndefined());
    } finally {
      fixture.finishOwner();
      fixture.releaseFirst();
      fixture.ownerSubscription.unsubscribe();
      fixture.winnerSubscription.unsubscribe();
    }
  });

  it('never resurrects a cancelled capture when pre-publication verification fails', async () => {
    // Cancellation during verification combined with a failed verification
    // result: the cancelled capture stays dropped (never restored), the
    // surviving tail stays retained, and repeated cancellation is once-only.
    const fixture = await setupForeignWinnerHandoff('handoff-cancel-unreadable');
    const { pubsub, owner, memory } = fixture;
    let releaseRead!: () => void;
    try {
      const pendingA = owner.sendSignal(
        fixture.agent,
        { type: 'user-message', contents: 'cancelled A' },
        fixture.scope,
        pubsub,
      );
      const pendingB = owner.sendSignal(
        fixture.agent,
        { type: 'user-message', contents: 'retained B' },
        fixture.scope,
        pubsub,
      );
      await pendingA.accepted;
      await pendingB.accepted;
      const winning = await seizeLeaseForWinner(fixture);
      await pubsub.flush();
      await nextTick();
      await nextTick();
      const readGate = new Promise<void>(resolve => {
        releaseRead = resolve;
      });
      const realGetLeaseOwner = pubsub.getLeaseOwner.bind(pubsub);
      vi.spyOn(pubsub, 'getLeaseOwner').mockImplementation(async (k: string) => {
        await readGate;
        return realGetLeaseOwner(k);
      });
      fixture.finishOwner();
      await new Promise(resolve => setTimeout(resolve, 30));
      expect(
        owner.cancelQueuedMessages(fixture.agent, { ...fixture.scope, signalIds: [pendingA.signal.id] }, pubsub)
          .cancelledSignalIds,
      ).toEqual([pendingA.signal.id]);
      // Fail the parked verification read: the entry check must drop the
      // cancelled capture instead of restoring it.
      pubsub.ownerReadFailures = 10;
      releaseRead();
      await new Promise(resolve => setTimeout(resolve, 50));
      // Repeated cancellation is once-only and A was never published.
      expect(
        owner.cancelQueuedMessages(fixture.agent, { ...fixture.scope, signalIds: [pendingA.signal.id] }, pubsub)
          .cancelledSignalIds,
      ).toEqual([]);
      expect(
        pubsub.publishedData.some(
          data =>
            data.type === 'signal-enqueued' &&
            data.runId === fixture.winnerRunId &&
            data.signal.id === pendingA.signal.id,
        ),
      ).toBe(false);
      // The surviving tail stays retained for the next natural trigger.
      expect(
        owner.cancelQueuedMessages(fixture.agent, { ...fixture.scope, signalIds: [pendingB.signal.id] }, pubsub)
          .cancelledSignalIds,
      ).toEqual([pendingB.signal.id]);
      fixture.releaseFirst();
      const output = await winning;
      await output.text;
      const { messages } = await memory.recall(fixture.scope);
      expect(
        messages.filter(message =>
          message.content.parts.some(part => part.type === 'text' && part.text === 'cancelled A'),
        ),
      ).toHaveLength(0);
      await vi.waitFor(() => expect(pubsub.owners.get(fixture.key)).toBeUndefined());
    } finally {
      pubsub.ownerReadFailures = 0;
      try {
        releaseRead();
      } catch {
        // Already released on the success path.
      }
      fixture.finishOwner();
      fixture.releaseFirst();
      fixture.ownerSubscription.unsubscribe();
      fixture.winnerSubscription.unsubscribe();
    }
  });

  it('drops a selectively cancelled capture during verification instead of publishing it', async () => {
    // The capture stays exactly cancellable through owner verification: a
    // selective cancellation that wins during the read must drop the item
    // without publishing it, while the surviving tail still forwards.
    const fixture = await setupForeignWinnerHandoff('handoff-cancel-verify');
    const { pubsub, owner, memory, model } = fixture;
    let releaseRead!: () => void;
    try {
      const pendingA = owner.sendSignal(
        fixture.agent,
        { type: 'user-message', contents: 'cancelled A' },
        fixture.scope,
        pubsub,
      );
      const pendingB = owner.sendSignal(
        fixture.agent,
        { type: 'user-message', contents: 'surviving B' },
        fixture.scope,
        pubsub,
      );
      await pendingA.accepted;
      await pendingB.accepted;
      const winning = await seizeLeaseForWinner(fixture);
      // Gate every owner read: the handoff parks its verification on the
      // gate while the capture is still queue-resident (commit happens only
      // at publication initiation), so cancelling after a short settle lands
      // deterministically inside the verification window.
      await pubsub.flush();
      await nextTick();
      await nextTick();
      const readGate = new Promise<void>(resolve => {
        releaseRead = resolve;
      });
      const realGetLeaseOwner = pubsub.getLeaseOwner.bind(pubsub);
      vi.spyOn(pubsub, 'getLeaseOwner').mockImplementation(async (k: string) => {
        await readGate;
        return realGetLeaseOwner(k);
      });
      fixture.finishOwner();
      await new Promise(resolve => setTimeout(resolve, 30));
      expect(
        owner.cancelQueuedMessages(fixture.agent, { ...fixture.scope, signalIds: [pendingA.signal.id] }, pubsub)
          .cancelledSignalIds,
      ).toEqual([pendingA.signal.id]);
      releaseRead();
      await vi.waitFor(() =>
        expect(
          pubsub.publishedData
            .filter(data => data.type === 'signal-enqueued' && data.runId === fixture.winnerRunId)
            .map(data => data.signal.id),
        ).toEqual([pendingB.signal.id]),
      );
      // Repeated cancellation is once-only: the dropped item settles once
      // and is never published or executed.
      expect(
        owner.cancelQueuedMessages(fixture.agent, { ...fixture.scope, signalIds: [pendingA.signal.id] }, pubsub)
          .cancelledSignalIds,
      ).toEqual([]);
      expect(
        pubsub.publishedData.some(
          data =>
            data.type === 'signal-enqueued' &&
            data.runId === fixture.winnerRunId &&
            data.signal.id === pendingA.signal.id,
        ),
      ).toBe(false);
      fixture.releaseFirst();
      const output = await winning;
      await output.text;
      await vi.waitFor(() => expect(fixture.getStreamCount()).toBe(2));
      expect(JSON.stringify(model.doStreamCalls[1]?.prompt)).toContain('surviving B');
      expect(JSON.stringify(model.doStreamCalls[1]?.prompt)).not.toContain('cancelled A');
      const { messages } = await memory.recall(fixture.scope);
      expect(
        messages.filter(message =>
          message.content.parts.some(part => part.type === 'text' && part.text === 'cancelled A'),
        ),
      ).toHaveLength(0);
      await vi.waitFor(() => expect(pubsub.owners.get(fixture.key)).toBeUndefined());
    } finally {
      try {
        releaseRead();
      } catch {
        // Already released on the success path.
      }
      fixture.finishOwner();
      fixture.releaseFirst();
      fixture.ownerSubscription.unsubscribe();
      fixture.winnerSubscription.unsubscribe();
    }
  });

  async function setupDrainLossHandoff(suffix: string) {
    // 7791-style drain-loss fixture: the owner stub aborts while its
    // lease transfer is gated, so the drain observes a verified foreign
    // winner and enters the forwarding handoff with a draining record.
    const scope = { resourceId: `${suffix}-user`, threadId: `${suffix}-thread` };
    const key = `${scope.resourceId}\u0000${scope.threadId}`;
    const pubsub = new ControlledLeasePubSub();
    const owner = new AgentThreadStreamRuntime();
    const memory = new MockMemory();
    const first = createBlockingFirstTextStreamModel('winner answer', 'winner answer');
    const agent = new Agent({
      id: `${suffix}-winner`,
      name: 'Drain winner',
      instructions: 'Test',
      model: first.model,
      memory,
      pubsub,
    });
    const ownerSubscription = await owner.subscribeToThread(agent, scope, pubsub);
    const winnerSubscription = await agent.subscribeToThread(scope);
    let finishOwner!: () => void;
    const ownerFinished = new Promise<void>(resolve => {
      finishOwner = resolve;
    });
    const oldRunId = `${suffix}-owner`;
    const winnerRunId = `${suffix}-winner-run`;
    const options = owner.prepareRunOptions(
      { runId: oldRunId, memory: { resource: scope.resourceId, thread: scope.threadId } } as any,
      pubsub,
    );
    const registration = owner.registerRun(
      agent,
      {
        runId: oldRunId,
        status: 'running',
        fullStream: (async function* () {})(),
        _waitUntilFinished: () => ownerFinished,
      } as any,
      options,
      pubsub,
    );
    void registration?.catch(() => {});
    return {
      scope,
      key,
      pubsub,
      owner,
      memory,
      agent,
      model: first.model,
      releaseFirst: first.releaseFirst,
      getStreamCount: first.getStreamCount,
      ownerSubscription,
      winnerSubscription,
      oldRunId,
      winnerRunId,
      finishOwner,
    };
  }

  async function startDrainLossHandoff(fixture: Awaited<ReturnType<typeof setupDrainLossHandoff>>) {
    const pendingA = fixture.owner.sendSignal(
      fixture.agent,
      { type: 'user-message', contents: 'drain pending A' },
      fixture.scope,
      fixture.pubsub,
    );
    const pendingB = fixture.owner.sendSignal(
      fixture.agent,
      { type: 'user-message', contents: 'drain pending B' },
      fixture.scope,
      fixture.pubsub,
    );
    await pendingA.accepted;
    await pendingB.accepted;
    const idleC = fixture.owner.queueMessage(fixture.agent, 'drain idle C', fixture.scope, fixture.pubsub);
    await idleC.accepted;
    let releaseTransfer!: () => void;
    const transferGate = new Promise<void>(resolve => {
      releaseTransfer = resolve;
    });
    let transferring!: () => void;
    const transferStarted = new Promise<void>(resolve => {
      transferring = resolve;
    });
    fixture.pubsub.transferLeaseWait = transferGate;
    fixture.pubsub.onTransferLease = transferring;
    expect(fixture.ownerSubscription.abort()).toBe(true);
    fixture.finishOwner();
    await transferStarted;
    fixture.pubsub.owners.delete(fixture.key);
    const winning = fixture.agent.stream('winner input', {
      runId: fixture.winnerRunId,
      memory: { resource: fixture.scope.resourceId, thread: fixture.scope.threadId },
    });
    await vi.waitFor(() => expect(fixture.getStreamCount()).toBe(1));
    await vi.waitFor(() =>
      expect(
        fixture.pubsub.publishedData.some(data => data.type === 'run-registered' && data.runId === fixture.winnerRunId),
      ).toBe(true),
    );
    return { pendingA, pendingB, idleC, winning, releaseTransfer };
  }

  it('drops a selectively cancelled drain capture during verification and forwards the tail', async () => {
    // Drain-loss variant: the capture is already shifted with a draining
    // record, and the handoff flag is still unset through verification, so
    // selective cancellation still owns the exact item.
    const fixture = await setupDrainLossHandoff('drain-cancel-verify');
    const { pubsub, owner, memory, model } = fixture;
    let releaseRead!: () => void;
    try {
      const handoff = await startDrainLossHandoff(fixture);
      await pubsub.flush();
      await nextTick();
      await nextTick();
      const readGate = new Promise<void>(resolve => {
        releaseRead = resolve;
      });
      const realGetLeaseOwner = pubsub.getLeaseOwner.bind(pubsub);
      vi.spyOn(pubsub, 'getLeaseOwner').mockImplementation(async (k: string) => {
        await readGate;
        return realGetLeaseOwner(k);
      });
      handoff.releaseTransfer();
      await new Promise(resolve => setTimeout(resolve, 30));
      expect(
        owner.cancelQueuedMessages(fixture.agent, { ...fixture.scope, signalIds: [handoff.pendingA.signal.id] }, pubsub)
          .cancelledSignalIds,
      ).toEqual([handoff.pendingA.signal.id]);
      releaseRead();
      await vi.waitFor(() =>
        expect(
          pubsub.publishedData
            .filter(data => data.type === 'signal-enqueued' && data.runId === fixture.winnerRunId)
            .map(data => data.signal.id),
        ).toEqual([handoff.pendingB.signal.id, handoff.idleC.signal.id]),
      );
      expect(
        owner.cancelQueuedMessages(fixture.agent, { ...fixture.scope, signalIds: [handoff.pendingA.signal.id] }, pubsub)
          .cancelledSignalIds,
      ).toEqual([]);
      expect(
        pubsub.publishedData.some(
          data =>
            data.type === 'signal-enqueued' &&
            data.runId === fixture.winnerRunId &&
            data.signal.id === handoff.pendingA.signal.id,
        ),
      ).toBe(false);
      fixture.releaseFirst();
      const output = await handoff.winning;
      await output.text;
      const promptHaystack = model.doStreamCalls.map(call => JSON.stringify(call?.prompt)).join('\n');
      expect(promptHaystack).toContain('drain pending B');
      expect(promptHaystack).toContain('drain idle C');
      expect(promptHaystack).not.toContain('drain pending A');
      const { messages } = await memory.recall(fixture.scope);
      expect(
        messages.filter(message =>
          message.content.parts.some(part => part.type === 'text' && part.text === 'drain pending A'),
        ),
      ).toHaveLength(0);
      await vi.waitFor(() => expect(pubsub.owners.get(fixture.key)).toBeUndefined());
    } finally {
      try {
        releaseRead();
      } catch {
        // Already released on the success path.
      }
      fixture.finishOwner();
      fixture.releaseFirst();
      fixture.ownerSubscription.unsubscribe();
      fixture.winnerSubscription.unsubscribe();
    }
  });

  it('completes an in-flight publication across clear-pending without forwarding the cleared tail', async () => {
    // Clear-pending during A's publication: the committed item completes
    // exactly once while the cleared tail never forwards.
    const fixture = await setupDrainLossHandoff('drain-clear-publish');
    const { pubsub, owner, memory, model } = fixture;
    let releasePublish!: () => void;
    try {
      const handoff = await startDrainLossHandoff(fixture);
      let publishAttempts = 0;
      const publishGate = new Promise<void>(resolve => {
        releasePublish = resolve;
      });
      const realPublish = pubsub.publish.bind(pubsub);
      vi.spyOn(pubsub, 'publish').mockImplementation(async (topic, event) => {
        if (event.data?.type === 'signal-enqueued' && event.data?.runId === fixture.winnerRunId) {
          publishAttempts += 1;
          await publishGate;
        }
        return realPublish(topic, event);
      });
      handoff.releaseTransfer();
      await vi.waitFor(() => expect(publishAttempts).toBe(1));
      // Local-only clear: queues drain without publishing an abort to the
      // winner, so the in-flight item is unaffected while B and C die.
      owner.abortThread({ ...fixture.scope, localOnly: true, clearPendingSignals: true }, pubsub);
      releasePublish();
      await vi.waitFor(() =>
        expect(
          pubsub.publishedData.filter(data => data.type === 'signal-enqueued' && data.runId === fixture.winnerRunId),
        ).toHaveLength(1),
      );
      await new Promise(resolve => setTimeout(resolve, 30));
      expect(
        pubsub.publishedData
          .filter(data => data.type === 'signal-enqueued' && data.runId === fixture.winnerRunId)
          .map(data => data.signal.id),
      ).toEqual([handoff.pendingA.signal.id]);
      fixture.releaseFirst();
      const output = await handoff.winning;
      await output.text;
      await vi.waitFor(() => expect(pubsub.owners.get(fixture.key)).toBeUndefined());
      const promptHaystack = model.doStreamCalls.map(call => JSON.stringify(call?.prompt)).join('\n');
      expect(promptHaystack.match(/drain pending A/g)).toHaveLength(1);
      expect(promptHaystack).not.toContain('drain pending B');
      expect(promptHaystack).not.toContain('drain idle C');
      const { messages } = await memory.recall(fixture.scope);
      expect(
        messages.filter(message =>
          message.content.parts.some(part => part.type === 'text' && part.text === 'drain pending A'),
        ),
      ).toHaveLength(1);
    } finally {
      try {
        releasePublish();
      } catch {
        // Already released on the success path.
      }
      fixture.finishOwner();
      fixture.releaseFirst();
      fixture.ownerSubscription.unsubscribe();
      fixture.winnerSubscription.unsubscribe();
    }
  });

  it('rejects a same-run replacement token projected during the verification read', async () => {
    // A delayed owner read returns the captured token after an authenticated
    // replacement (same public run, different stream/token) was projected:
    // the handoff must fence the projected identity, not forward under
    // stale provenance. Work stays retained for the actual winner.
    const fixture = await setupForeignWinnerHandoff('handoff-replacement');
    const { pubsub, owner, memory } = fixture;
    let releaseRead!: () => void;
    try {
      const pendingA = owner.sendSignal(
        fixture.agent,
        { type: 'user-message', contents: 'replacement A' },
        fixture.scope,
        pubsub,
      );
      await pendingA.accepted;
      const winning = await seizeLeaseForWinner(fixture);
      const capturedToken = pubsub.owners.get(fixture.key);
      expect(capturedToken).toBeTruthy();
      // Drain any in-flight deliveries so the handoff verification is the
      // next lease read; then park ONLY that first read (one-shot) while
      // every later read — including the replacement registration's own
      // liveness check — passes through with live values.
      await pubsub.flush();
      await nextTick();
      await nextTick();
      const readGate = new Promise<void>(resolve => {
        releaseRead = resolve;
      });
      const realGetLeaseOwner = pubsub.getLeaseOwner.bind(pubsub);
      let parkedReads = 0;
      let heldDuringPublish = false;
      vi.spyOn(pubsub, 'getLeaseOwner').mockImplementation(async (k: string) => {
        if (parkedReads > 0) return realGetLeaseOwner(k);
        parkedReads += 1;
        await readGate;
        return capturedToken;
      });
      fixture.finishOwner();
      await new Promise(resolve => setTimeout(resolve, 30));
      const replacementToken = `mastra-thread-owner:${JSON.stringify([fixture.winnerRunId, 'replacement-source', '22222222-2222-4222-8222-222222222222'])}`;
      pubsub.owners.set(fixture.key, replacementToken);
      await pubsub.publish(fixture.topic, {
        type: 'run-registered',
        runId: fixture.winnerRunId,
        data: {
          type: 'run-registered',
          runId: fixture.winnerRunId,
          streamId: 'replacement-stream',
          streamSeq: 2,
          leaseOwner: replacementToken,
          sourceId: 'replacement-injector',
        },
      });
      await pubsub.flush();
      await nextTick();
      await nextTick();
      // The replacement must project while the stale read is still held:
      // the handler's own liveness read passes the one-shot gate through.
      heldDuringPublish = parkedReads === 1;
      releaseRead();
      expect(heldDuringPublish).toBe(true);
      await new Promise(resolve => setTimeout(resolve, 30));
      expect(
        pubsub.publishedData.filter(data => data.type === 'signal-enqueued' && data.runId === fixture.winnerRunId),
      ).toHaveLength(0);
      // The unhanded-off work is retained, not lost: still cancellable.
      expect(
        owner.cancelQueuedMessages(fixture.agent, { ...fixture.scope, signalIds: [pendingA.signal.id] }, pubsub)
          .cancelledSignalIds,
      ).toEqual([pendingA.signal.id]);
      fixture.releaseFirst();
      const output = await winning;
      await output.text;
      const { messages } = await memory.recall(fixture.scope);
      expect(
        messages.filter(message =>
          message.content.parts.some(part => part.type === 'text' && part.text === 'replacement A'),
        ),
      ).toHaveLength(0);
    } finally {
      try {
        releaseRead();
      } catch {
        // Already released on the success path.
      }
      fixture.finishOwner();
      fixture.releaseFirst();
      fixture.ownerSubscription.unsubscribe();
      fixture.winnerSubscription.unsubscribe();
    }
  });

  it('retains forwarded work when the verification owner read is unreadable', async () => {
    // Ownership unreadable means fail closed: no forward, no release, no
    // local execution — the input stays queued with truthful receipts.
    const fixture = await setupForeignWinnerHandoff('handoff-unreadable');
    const { pubsub, owner } = fixture;
    try {
      const pendingA = owner.sendSignal(
        fixture.agent,
        { type: 'user-message', contents: 'unreadable A' },
        fixture.scope,
        pubsub,
      );
      await pendingA.accepted;
      const winning = await seizeLeaseForWinner(fixture);
      pubsub.ownerReadFailures = 10;
      fixture.finishOwner();
      await new Promise(resolve => setTimeout(resolve, 50));
      expect(
        pubsub.publishedData.filter(data => data.type === 'signal-enqueued' && data.runId === fixture.winnerRunId),
      ).toHaveLength(0);
      expect(
        owner.cancelQueuedMessages(fixture.agent, { ...fixture.scope, signalIds: [pendingA.signal.id] }, pubsub)
          .cancelledSignalIds,
      ).toEqual([pendingA.signal.id]);
      pubsub.ownerReadFailures = 0;
      fixture.releaseFirst();
      const output = await winning;
      await output.text;
      await vi.waitFor(() => expect(pubsub.owners.get(fixture.key)).toBeUndefined());
    } finally {
      fixture.pubsub.ownerReadFailures = 0;
      fixture.finishOwner();
      fixture.releaseFirst();
      fixture.ownerSubscription.unsubscribe();
      fixture.winnerSubscription.unsubscribe();
    }
  });

  it('rejects a successor installed while the verification read is outstanding', async () => {
    // A different run acquires the lease mid-read: the post-await recheck
    // must observe the new owner and retain the work instead of forwarding
    // under the superseded winner.
    const fixture = await setupForeignWinnerHandoff('handoff-successor');
    const { pubsub, owner } = fixture;
    let releaseRead!: () => void;
    try {
      const pendingA = owner.sendSignal(
        fixture.agent,
        { type: 'user-message', contents: 'successor A' },
        fixture.scope,
        pubsub,
      );
      await pendingA.accepted;
      const winning = await seizeLeaseForWinner(fixture);
      // Drain stragglers, then park only the handoff verification (one-shot)
      // while the successor registration's own liveness read passes through.
      await pubsub.flush();
      await nextTick();
      await nextTick();
      const readGate = new Promise<void>(resolve => {
        releaseRead = resolve;
      });
      const realGetLeaseOwner = pubsub.getLeaseOwner.bind(pubsub);
      let parkedReads = 0;
      let heldDuringPublish = false;
      vi.spyOn(pubsub, 'getLeaseOwner').mockImplementation(async (k: string) => {
        if (parkedReads > 0) return realGetLeaseOwner(k);
        parkedReads += 1;
        await readGate;
        return realGetLeaseOwner(k);
      });
      fixture.finishOwner();
      await new Promise(resolve => setTimeout(resolve, 30));
      const successorRunId = 'handoff-successor-elsewhere';
      const successorToken = `mastra-thread-owner:${JSON.stringify([successorRunId, 'successor-source', '33333333-3333-4333-8333-333333333333'])}`;
      pubsub.owners.set(fixture.key, successorToken);
      await pubsub.publish(fixture.topic, {
        type: 'run-registered',
        runId: successorRunId,
        data: {
          type: 'run-registered',
          runId: successorRunId,
          streamId: 'successor-stream',
          streamSeq: 1,
          leaseOwner: successorToken,
          sourceId: 'successor-injector',
        },
      });
      await pubsub.flush();
      await nextTick();
      await nextTick();
      // The successor must project while the verification is still held.
      heldDuringPublish = parkedReads === 1;
      releaseRead();
      expect(heldDuringPublish).toBe(true);
      await new Promise(resolve => setTimeout(resolve, 30));
      expect(
        pubsub.publishedData.filter(data => data.type === 'signal-enqueued' && data.runId === fixture.winnerRunId),
      ).toHaveLength(0);
      expect(
        owner.cancelQueuedMessages(fixture.agent, { ...fixture.scope, signalIds: [pendingA.signal.id] }, pubsub)
          .cancelledSignalIds,
      ).toEqual([pendingA.signal.id]);
      fixture.releaseFirst();
      const output = await winning;
      await output.text;
    } finally {
      try {
        releaseRead();
      } catch {
        // Already released on the success path.
      }
      fixture.finishOwner();
      fixture.releaseFirst();
      fixture.ownerSubscription.unsubscribe();
      fixture.winnerSubscription.unsubscribe();
    }
  });

  it.each([
    { item: 'A', mode: 'before' },
    { item: 'A', mode: 'after' },
    { item: 'B', mode: 'before' },
    { item: 'B', mode: 'after' },
  ] as const)(
    'withholds a failed $item forward ($mode delivery) instead of redispatching it',
    async ({ item, mode }) => {
      // Fail-closed safety stop for a failed verified-loss forward: once
      // the forwarding publication was invoked, a generic rejection is
      // unknown admission — not proof the item never arrived — in BOTH the
      // before and after modes (the runtime cannot distinguish them). The
      // faulted head is fenced as unresolved provenance under its stable
      // id, nothing past it is ever attempted (no overtaking), the first
      // winner admits and executes exactly the delivered prefix, and — once
      // that winner finishes normally — a genuine subsequent lifecycle
      // transition still dispatches nothing: no second `agent.stream` call
      // and no re-publication of the ambiguous item. No manual republish,
      // no retry-injector, no fabricated trigger. Withholding trades
      // liveness for safety pending an authoritative disposition or explicit
      // reconciliation, which need a product decision first.
      const fixture = await setupDrainLossHandoff(`drain-retry-${item}-${mode}`);
      const { pubsub, owner, memory, model } = fixture;
      try {
        const handoff = await startDrainLossHandoff(fixture);
        const target = item === 'A' ? handoff.pendingA : handoff.pendingB;
        const targetId = target.signal.id;
        // Cycle 1 attempts nothing past the faulted head; the faulted item
        // reaches the wire only when the fault lands after delivery.
        const expectedWire =
          item === 'A'
            ? mode === 'after'
              ? [handoff.pendingA.signal.id]
              : []
            : mode === 'after'
              ? [handoff.pendingA.signal.id, handoff.pendingB.signal.id]
              : [handoff.pendingA.signal.id];
        let faultFired = false;
        const realPublish = pubsub.publish.bind(pubsub);
        const publishSpy = vi.spyOn(pubsub, 'publish').mockImplementation(async (topic, event) => {
          if (
            !faultFired &&
            event.data?.type === 'signal-enqueued' &&
            event.data?.runId === fixture.winnerRunId &&
            event.data?.signal.id === targetId
          ) {
            faultFired = true;
            if (mode === 'after') await realPublish(topic, event);
            throw new Error(`injected ${mode}-delivery forward failure for ${item}`);
          }
          return realPublish(topic, event);
        });
        handoff.releaseTransfer();
        await new Promise(resolve => setTimeout(resolve, 50));
        publishSpy.mockRestore();
        expect(faultFired).toBe(true);
        expect(
          pubsub.publishedData
            .filter(data => data.type === 'signal-enqueued' && data.runId === fixture.winnerRunId)
            .map(data => data.signal.id),
        ).toEqual(expectedWire);
        // Settle the idle tail while the winner is still blocked: it must
        // never forward or execute afterwards. Left queued, the winner's
        // normal completion would pull it into the unrelated
        // remote-completion idle drain and race every later assertion.
        expect(
          owner.cancelQueuedMessages(fixture.agent, { ...fixture.scope, signalIds: [handoff.idleC.signal.id] }, pubsub)
            .cancelledSignalIds,
        ).toEqual([handoff.idleC.signal.id]);
        expect(
          owner.cancelQueuedMessages(fixture.agent, { ...fixture.scope, signalIds: [handoff.idleC.signal.id] }, pubsub)
            .cancelledSignalIds,
        ).toEqual([]);
        // The first winner executes exactly the delivered prefix, proving
        // actual recipient admission for the delivered copies, then finishes
        // normally and releases the thread.
        const deliveredTexts =
          item === 'A'
            ? mode === 'after'
              ? ['drain pending A']
              : []
            : mode === 'after'
              ? ['drain pending A', 'drain pending B']
              : ['drain pending A'];
        const allTexts = ['drain pending A', 'drain pending B', 'drain idle C'];
        fixture.releaseFirst();
        const firstOutput = await handoff.winning;
        await firstOutput.text;
        await vi.waitFor(async () => {
          const { messages } = await memory.recall(fixture.scope);
          expect(
            deliveredTexts.filter(text =>
              messages.some(message => message.content.parts.some(part => part.type === 'text' && part.text === text)),
            ),
          ).toEqual(deliveredTexts);
        });
        const winnerHaystack = model.doStreamCalls.map(call => JSON.stringify(call?.prompt)).join('\n');
        {
          const { messages } = await memory.recall(fixture.scope);
          for (const text of deliveredTexts) {
            expect(
              messages.filter(message =>
                message.content.parts.some(part => part.type === 'text' && part.text === text),
              ),
            ).toHaveLength(1);
            expect(winnerHaystack.match(new RegExp(text.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'g'))).toHaveLength(1);
          }
          for (const text of allTexts.filter(text => !deliveredTexts.includes(text))) {
            expect(
              messages.filter(message =>
                message.content.parts.some(part => part.type === 'text' && part.text === text),
              ),
            ).toHaveLength(0);
          }
        }
        await vi.waitFor(() => expect(pubsub.owners.get(fixture.key)).toBeUndefined());
        await vi.waitFor(() =>
          expect(fixture.owner.getActiveThreadRunId(fixture.scope, fixture.pubsub)).toBeUndefined(),
        );
        // Genuine subsequent trigger: a fresh owner run through the
        // authentic reserve/register lifecycle finishes, and its completion
        // drain must withhold the ambiguous head — no recovery execution
        // starts, and the handoff never retries in a loop.
        const recoveryRunId = `drain-retry-${item}-${mode}-recovery`;
        const streamMock = vi.fn().mockResolvedValue({} as any);
        const recoveryAgent = {
          id: `drain-retry-${item}-${mode}-agent`,
          stream: streamMock,
        } as unknown as Agent<any, any, any, any>;
        const prepared = owner.prepareRunOptions(
          {
            runId: recoveryRunId,
            memory: { resource: fixture.scope.resourceId, thread: fixture.scope.threadId },
          } as any,
          pubsub,
        );
        const releaseReservation = owner.reserveRun(prepared, pubsub, recoveryAgent.id);
        expect(releaseReservation).toBeDefined();
        let finishRecovery!: () => void;
        const recoveryFinished = new Promise<void>(resolve => {
          finishRecovery = resolve;
        });
        const registration = owner.registerRun(
          recoveryAgent,
          createFakeThreadRun(recoveryRunId, recoveryFinished),
          prepared,
          pubsub,
        );
        // registerRun settles on run completion, so observe the
        // registration publish (not the returned promise) before finishing.
        void registration?.catch(() => {});
        await vi.waitFor(() =>
          expect(
            pubsub.publishedData.some(data => data.type === 'run-registered' && data.runId === recoveryRunId),
          ).toBe(true),
        );
        finishRecovery();
        await registration;
        await vi.waitFor(() =>
          expect(fixture.owner.getActiveThreadRunId(fixture.scope, fixture.pubsub)).toBeUndefined(),
        );
        // Let the fenced completion drain settle, then prove nothing
        // dispatched: the ambiguous head is excluded from the ordinary
        // drain, the projected handoff, in-loop/pre-run consumption, and
        // sibling advancement alike.
        await new Promise(resolve => setTimeout(resolve, 100));
        await pubsub.flush();
        expect(streamMock.mock.calls).toHaveLength(0);
        // No re-publication either: the winner-addressed wire still shows
        // only the first cycle's publications.
        expect(
          pubsub.publishedData
            .filter(data => data.type === 'signal-enqueued' && data.runId === fixture.winnerRunId)
            .map(data => data.signal.id),
        ).toEqual(expectedWire);
        // The surviving tail stays retained behind the fenced head with
        // stable ids: the selective probe lists queue order, not request
        // order, so equality proves order as well as retention. (The idle
        // tail was settled above; only pending B can remain.)
        const expectedTail = item === 'A' ? [handoff.pendingB.signal.id] : [];
        expect(
          owner.cancelQueuedMessages(fixture.agent, { ...fixture.scope, signalIds: expectedTail }, pubsub)
            .cancelledSignalIds,
        ).toEqual(expectedTail);
        // Once-only: the settled tail never cancels twice.
        if (expectedTail.length > 0) {
          expect(
            owner.cancelQueuedMessages(fixture.agent, { ...fixture.scope, signalIds: expectedTail }, pubsub)
              .cancelledSignalIds,
          ).toEqual([]);
        }
        // Explicit cancellation of the ambiguous head must not falsely
        // report it as unsent: delivery is unknown, so the id is neither
        // cancelled nor resurrected — the probe returns [] while the
        // provenance stays fenced with its original error receipt.
        expect(
          owner.cancelQueuedMessages(fixture.agent, { ...fixture.scope, signalIds: [targetId] }, pubsub)
            .cancelledSignalIds,
        ).toEqual([]);
        if (mode === 'before') {
          // The faulted head never reached the wire, so the winner never
          // saw it — and the owner withholds its ambiguous copy too, so
          // the head executes nowhere until explicit reconciliation.
          expect(winnerHaystack).not.toContain(item === 'A' ? 'drain pending A' : 'drain pending B');
        } else {
          // The after-delivery copy was genuinely admitted and executed
          // exactly once by the first winner; the owner's ambiguous copy
          // is withheld, so the same stable id never executes a second
          // time. Pinning before/after evidence here instead of claiming
          // global exactly-once across runtime ownership.
          expect(winnerHaystack).toContain(item === 'A' ? 'drain pending A' : 'drain pending B');
        }
        // Steady state is honest fail-closed retention, not forced
        // cleanup: the ambiguous item stays fenced with its original
        // payload, stable id, destination, attempt, and error pending
        // reconciliation, and no trigger redispatches it. Retention is
        // in-memory only — liveness and crash durability need a product
        // decision first.
      } finally {
        fixture.finishOwner();
        fixture.releaseFirst();
        fixture.ownerSubscription.unsubscribe();
        fixture.winnerSubscription.unsubscribe();
      }
    },
  );

  it.each(['retain', 'lose'] as const)(
    'preserves queued input when abort %s ownership during a delayed handoff',
    async ownership => {
      const scope = { resourceId: 'abort-handoff-user', threadId: `abort-handoff-${ownership}` };
      const key = `${scope.resourceId}\u0000${scope.threadId}`;
      const pubsub = new ControlledLeasePubSub();
      const memory = new MockMemory();
      const { model, releaseFirst, getStreamCount } = createBlockingFirstTextStreamModel(
        'first response',
        'queued response',
      );
      const agent = new Agent({
        id: 'abort-handoff',
        name: 'Abort handoff',
        instructions: 'Test',
        model,
        memory,
        pubsub,
      });
      const subscription = await agent.subscribeToThread(scope);
      const winner = new AgentThreadStreamRuntime();
      const winnerSubscription = await winner.subscribeToThread(agent, scope, pubsub);
      const stream = await agent.stream('first', { memory: { resource: scope.resourceId, thread: scope.threadId } });
      let releaseTransfer!: () => void;
      const transferGate = new Promise<void>(resolve => {
        releaseTransfer = resolve;
      });
      let transferStarted!: () => void;
      const transferring = new Promise<void>(resolve => {
        transferStarted = resolve;
      });
      let finishWinner!: () => void;
      const winnerFinished = new Promise<void>(resolve => {
        finishWinner = resolve;
      });
      const winnerRunId = 'competing-abort-winner';
      // The winner's exact attempt lease token, captured from its registration
      // publication — never the public run id, which cannot own a lease.
      let winnerLeaseOwner: string | undefined;
      try {
        await vi.waitFor(() => expect(getStreamCount()).toBe(1));
        const pending = agent.sendSignal({ type: 'user-message', contents: 'pending A' }, scope);
        await pending.accepted;
        const idle = agent.queueMessage('idle B', scope);
        await idle.accepted;
        pubsub.transferLeaseWait = transferGate;
        pubsub.onTransferLease = transferStarted;
        expect(subscription.abort()).toBe(true);
        releaseFirst();
        await transferring;
        expect(getStreamCount()).toBe(1);
        if (ownership === 'lose') {
          // Owner transfer having STARTED is not proof the winner processed
          // the predecessor terminal: this transport dispatches callbacks on
          // setTimeout, independently of publication completion. Wait for the
          // winner's own projection of the predecessor finishing first, so its
          // registration cannot trip the active-run guard on a stale remote run.
          await vi.waitFor(() => expect(winnerSubscription.activeRunId()).toBeNull());
          // Simulate the predecessor lease lapsing while its transfer is parked,
          // then let the winner register and acquire the key under its own
          // genuine attempt token instead of a seeded raw run id.
          pubsub.owners.delete(key);
          const winnerRegistration = winner.registerRun(
            agent,
            {
              runId: winnerRunId,
              status: 'running',
              fullStream: (async function* () {})(),
              _waitUntilFinished: () => winnerFinished,
            } as any,
            { runId: winnerRunId, memory: { resource: scope.resourceId, thread: scope.threadId } },
            pubsub,
          );
          // Registration readiness is separate from completion: the winner run
          // stays unfinished until finishWinner() below.
          void winnerRegistration?.catch(() => {});
          await vi.waitFor(() =>
            expect(
              pubsub.publishedData.some(data => data.type === 'run-registered' && data.runId === winnerRunId),
            ).toBe(true),
          );
          winnerLeaseOwner = pubsub.publishedData.find(
            data => data.type === 'run-registered' && data.runId === winnerRunId,
          )?.leaseOwner;
          expect(winnerLeaseOwner).toBeTruthy();
          expect(pubsub.owners.get(key)).toBe(winnerLeaseOwner);
        }
        // Keep the parked owner transfer gated until the winner is established.
        releaseTransfer();
        if (ownership === 'lose') {
          await vi.waitFor(() =>
            expect(
              pubsub.publishedData.filter(
                data =>
                  data.type === 'signal-enqueued' && data.runId === winnerRunId && data.signal.id === pending.signal.id,
              ),
            ).toHaveLength(1),
          );
          await vi.waitFor(() =>
            expect(
              pubsub.publishedData.filter(
                data =>
                  data.type === 'signal-enqueued' && data.runId === winnerRunId && data.signal.id === idle.signal.id,
              ),
            ).toHaveLength(1),
          );
          await pubsub.flush();
          expect(winner.drainPendingSignals(winnerRunId, pubsub).map(signal => signal.id)).toEqual([
            pending.signal.id,
            idle.signal.id,
          ]);
          expect(winner.drainPendingSignals(winnerRunId, pubsub)).toEqual([]);
          expect(getStreamCount()).toBe(1);
          expect(pubsub.owners.get(key)).toBe(winnerLeaseOwner);
          finishWinner();
        }
        await stream.text;
        await vi.waitFor(() => expect(getStreamCount()).toBe(ownership === 'lose' ? 1 : 3));
        await vi.waitFor(() => expect(pubsub.owners.get(key)).toBeUndefined());
        const prompts = model.doStreamCalls.slice(1).map(call => JSON.stringify(call.prompt));
        if (ownership === 'retain') {
          expect(prompts[0]).toContain('pending A');
          expect(prompts[0]).not.toContain('idle B');
          expect(prompts.at(-1)).toContain('idle B');
        }
        await vi.waitFor(() => expect(agentThreadStreamRuntime.getActiveThreadRunId(scope, pubsub)).toBeUndefined());
      } finally {
        releaseFirst();
        releaseTransfer();
        finishWinner();
        subscription.unsubscribe();
        winnerSubscription.unsubscribe();
      }
    },
  );

  describe.each(['sendSignal', 'queueMessage'] as const)('pending %s cancellation', enqueue => {
    it.each(['none', 'thread', 'upstream'] as const)(
      'answers the follow-up after %s cancellation',
      async cancellation => {
        const scope = { resourceId: 'abort-queue-user', threadId: `abort-${enqueue}-${cancellation}` };
        const pubsub = new EventEmitterPubSub();
        const memory = new MockMemory();
        const upstream = new AbortController();
        const { model, releaseFirst, getStreamCount } = createBlockingFirstTextStreamModel(
          'first response',
          'follow-up response',
        );
        const agent = new Agent({
          id: 'abort-queue',
          name: 'Abort queue',
          instructions: 'Test',
          model,
          memory,
          pubsub,
        });
        const subscription = await agent.subscribeToThread(scope);
        const stream = await agent.stream('first', {
          memory: { thread: scope.threadId, resource: scope.resourceId },
          abortSignal: upstream.signal,
        });

        try {
          await vi.waitFor(() => expect(getStreamCount()).toBe(1));
          const queued =
            enqueue === 'queueMessage'
              ? agent.queueMessage('follow-up', scope)
              : agent.sendSignal({ type: 'user-message', contents: 'follow-up' }, scope);
          await queued.accepted;
          if (cancellation === 'thread') expect(subscription.abort()).toBe(true);
          if (cancellation === 'upstream') upstream.abort();
          releaseFirst();
          await stream.text;

          await vi.waitFor(async () => {
            const { messages } = await memory.recall(scope);
            const followUps = messages.filter(message =>
              message.content.parts.some(part => part.type === 'text' && part.text === 'follow-up'),
            );
            expect(followUps).toHaveLength(1);
            expect(messages.at(-1)).toMatchObject({
              role: 'assistant',
              content: {
                parts: expect.arrayContaining([expect.objectContaining({ type: 'text', text: 'follow-up response' })]),
              },
            });
          });
          expect(getStreamCount()).toBe(2);
          await vi.waitFor(() => expect(agentThreadStreamRuntime.getActiveThreadRunId(scope, pubsub)).toBeUndefined());
        } finally {
          releaseFirst();
          subscription.unsubscribe();
        }
      },
    );
  });

  it('preserves an explicitly supplied queued-message abort signal while allowing later queued work', async () => {
    const scope = { resourceId: 'explicit-abort-user', threadId: 'explicit-abort-thread' };
    const pubsub = new EventEmitterPubSub();
    const memory = new MockMemory();
    const { model, releaseFirst, getStreamCount } = createBlockingFirstTextStreamModel(
      'first response',
      'surviving response',
    );
    const agent = new Agent({
      id: 'explicit-abort',
      name: 'Explicit abort',
      instructions: 'Test',
      model,
      memory,
      pubsub,
    });
    const subscription = await agent.subscribeToThread(scope);
    const stream = await agent.stream('first', { memory: { thread: scope.threadId, resource: scope.resourceId } });
    const queuedAbort = new AbortController();

    try {
      await vi.waitFor(() => expect(getStreamCount()).toBe(1));
      await agent.queueMessage('cancelled follow-up', {
        ...scope,
        ifIdle: { streamOptions: { abortSignal: queuedAbort.signal } },
      }).accepted;
      await agent.queueMessage('surviving follow-up', scope).accepted;
      queuedAbort.abort();
      expect(subscription.abort()).toBe(true);
      releaseFirst();
      await stream.text;

      await vi.waitFor(async () => {
        const { messages } = await memory.recall(scope);
        expect(messages.at(-1)).toMatchObject({
          role: 'assistant',
          content: { parts: [expect.objectContaining({ type: 'text', text: 'surviving response' })] },
        });
      });
      expect(getStreamCount()).toBe(2);
      const prompt = model.doStreamCalls[1]?.prompt;
      expect(prompt?.at(-1)).toMatchObject({
        role: 'user',
        content: expect.arrayContaining([
          expect.objectContaining({ type: 'text', text: expect.stringContaining('surviving follow-up') }),
        ]),
      });
      await vi.waitFor(() => expect(agentThreadStreamRuntime.getActiveThreadRunId(scope, pubsub)).toBeUndefined());
    } finally {
      releaseFirst();
      subscription.unsubscribe();
    }
  });

  it('persists external state signals with cache-key tracking', async () => {
    const memory = new MockMemory();
    await memory.createThread({ threadId: 'state-thread', resourceId: 'state-user' });
    const agent = new Agent({
      id: 'state-agent',
      name: 'State Agent',
      instructions: 'Test',
      model: createTextStreamModel('state response'),
      memory,
    });

    const result = await agent.sendStateSignal(
      {
        id: 'browser',
        cacheKey: 'browser:v1',
        mode: 'snapshot',
        contents: 'Browser is open on https://example.com',
        value: { activeUrl: 'https://example.com' },
      },
      { resourceId: 'state-user', threadId: 'state-thread', ifIdle: { behavior: 'persist' } },
    );
    if (result.skipped) throw new Error('expected state signal to be persisted, not skipped');
    await expect(result.accepted).resolves.toMatchObject({ action: 'persist' });
    expect(result.signal).toBeDefined();

    expect(result.signal).toMatchObject({
      type: 'state',
      tagName: 'state',
      metadata: expect.objectContaining({
        state: expect.objectContaining({ id: 'browser', cacheKey: 'browser:v1', mode: 'snapshot', version: 1 }),
        value: { activeUrl: 'https://example.com' },
      }),
    });
    await expect(
      agent.sendStateSignal(
        { id: 'browser', cacheKey: 'browser:v1', contents: 'unchanged' },
        { resourceId: 'state-user', threadId: 'state-thread', ifIdle: { behavior: 'persist' } },
      ),
    ).resolves.toEqual({ skipped: true, reason: 'unchanged' });
    const thread = await memory.getThreadById({ threadId: 'state-thread' });
    expect(thread?.metadata?.mastra).toEqual(
      expect.objectContaining({
        stateSignals: expect.objectContaining({
          browser: expect.objectContaining({
            currentCacheKey: 'browser:v1',
            version: 1,
            lastSnapshotSignalId: result.signal!.id,
          }),
        }),
      }),
    );
  });

  it('delivers medium-priority notification records while idle', async () => {
    const notifications = new InMemoryNotificationsStorage();
    const storage = new MastraCompositeStore({ id: 'notification-storage', domains: { notifications } });
    const agent = new Agent({
      id: 'notification-agent',
      name: 'Notification Agent',
      instructions: 'Test',
      model: createTextStreamModel('notification response'),
    });
    new Mastra({ agents: { notificationAgent: agent }, storage, logger: false });

    const subscription = await agent.subscribeToThread({
      threadId: 'notification-thread',
      resourceId: 'notification-user',
    });
    const nextRun = readNextRunWithParts(subscription.stream[Symbol.asyncIterator]());

    const result = await agent.sendNotificationSignal(
      {
        source: 'github',
        kind: 'ci-status',
        priority: 'medium',
        summary: 'CI failed on main',
        dedupeKey: 'main-ci',
      },
      {
        resourceId: 'notification-user',
        threadId: 'notification-thread',
        ifIdle: { streamOptions: { memory: { resource: 'notification-user', thread: 'notification-thread' } } },
      },
    );

    const subscribedRun = await nextRun;
    expect(result).toEqual(expect.objectContaining({ runId: subscribedRun.value.runId }));
    await expect(result.accepted).resolves.toMatchObject({ action: 'wake', runId: subscribedRun.value.runId });
    expect(result.decision).toMatchObject({ action: 'deliver' });
    expect(result.record).toMatchObject({
      agentId: 'notification-agent',
      resourceId: 'notification-user',
      threadId: 'notification-thread',
      status: 'delivered',
      deliveredSignalId: result.signal?.id,
    });
    const signalPart = subscribedRun.value.parts.find((part: any) => part.type === 'data-signal');
    expect(signalPart?.data).toMatchObject({
      id: result.signal?.id,
      type: 'notification',
      tagName: 'notification',
      contents: 'CI failed on main',
      attributes: { source: 'github', kind: 'ci-status', priority: 'medium', status: 'delivered' },
    });
    await expect(
      notifications.getNotification({ threadId: 'notification-thread', id: result.record.id }),
    ).resolves.toMatchObject({ status: 'delivered', deliveredSignalId: result.signal?.id });

    subscription.unsubscribe();
  });

  it('attaches delivery-policy stream options to immediate idle deliveries', async () => {
    const notifications = new InMemoryNotificationsStorage();
    const storage = new MastraCompositeStore({ id: 'notification-storage', domains: { notifications } });
    const streamOptions = { memory: { resource: 'notification-user', thread: 'notification-thread' } };
    const agent = new Agent({
      id: 'notification-agent',
      name: 'Notification Agent',
      instructions: 'Test',
      model: createTextStreamModel('notification response'),
      notifications: { deliveryPolicy: { decide: () => ({ action: 'deliver', streamOptions }) } },
    });
    new Mastra({ agents: { notificationAgent: agent }, storage, logger: false });
    const sendSignalSpy = vi.spyOn(agentThreadStreamRuntime, 'sendSignal');

    const result = await agent.sendNotificationSignal(
      { source: 'github', kind: 'ci-status', priority: 'medium', summary: 'CI failed on main' },
      { resourceId: 'notification-user', threadId: 'notification-thread' },
    );

    await result.accepted;
    expect(sendSignalSpy).toHaveBeenCalledWith(
      expect.anything(),
      expect.anything(),
      expect.objectContaining({ ifIdle: expect.objectContaining({ streamOptions }) }),
      expect.anything(),
    );
    sendSignalSpy.mockRestore();
  });

  it('keeps caller-supplied stream options over the delivery policy on immediate deliveries', async () => {
    const notifications = new InMemoryNotificationsStorage();
    const storage = new MastraCompositeStore({ id: 'notification-storage', domains: { notifications } });
    const policyOptions = { memory: { resource: 'policy-user', thread: 'policy-thread' } };
    const callerOptions = { memory: { resource: 'notification-user', thread: 'notification-thread' } };
    const agent = new Agent({
      id: 'notification-agent',
      name: 'Notification Agent',
      instructions: 'Test',
      model: createTextStreamModel('notification response'),
      notifications: { deliveryPolicy: { decide: () => ({ action: 'deliver', streamOptions: policyOptions }) } },
    });
    new Mastra({ agents: { notificationAgent: agent }, storage, logger: false });
    const sendSignalSpy = vi.spyOn(agentThreadStreamRuntime, 'sendSignal');

    const result = await agent.sendNotificationSignal(
      { source: 'github', kind: 'ci-status', priority: 'medium', summary: 'CI failed on main' },
      {
        resourceId: 'notification-user',
        threadId: 'notification-thread',
        ifIdle: { streamOptions: callerOptions },
      },
    );

    await result.accepted;
    expect(sendSignalSpy).toHaveBeenCalledWith(
      expect.anything(),
      expect.anything(),
      expect.objectContaining({ ifIdle: expect.objectContaining({ streamOptions: callerOptions }) }),
      expect.anything(),
    );
    sendSignalSpy.mockRestore();
  });

  it('resolves delivery-policy stream options through a real agent when dispatching deferred notifications', async () => {
    const notifications = new InMemoryNotificationsStorage();
    const storage = new MastraCompositeStore({ id: 'notification-storage', domains: { notifications } });
    const streamOptions = { memory: { resource: 'notification-user', thread: 'notification-thread' } };
    const agent = new Agent({
      id: 'notification-agent',
      name: 'Notification Agent',
      instructions: 'Test',
      model: createTextStreamModel('notification response'),
      notifications: { deliveryPolicy: { decide: () => ({ action: 'deliver', streamOptions }) } },
    });
    const mastra = new Mastra({ agents: { notificationAgent: agent }, storage, logger: false });
    const now = new Date();
    await notifications.createNotification({
      id: 'deferred-1',
      agentId: 'notification-agent',
      resourceId: 'notification-user',
      threadId: 'notification-thread',
      source: 'github',
      kind: 'ci-status',
      priority: 'high',
      summary: 'CI failed on main',
      deliverAt: now,
    });
    const sendSignalSpy = vi.spyOn(agent, 'sendSignal');

    const result = await dispatchDueNotifications({ mastra, storage: notifications, now });

    expect(result.failed).toEqual([]);
    expect(result.delivered).toHaveLength(1);
    expect(sendSignalSpy).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({ ifIdle: { streamOptions } }),
    );
    sendSignalSpy.mockRestore();
  });

  it('delivers batched idle notifications using one initial thread-state decision', async () => {
    const notifications = new InMemoryNotificationsStorage();
    const storage = new MastraCompositeStore({ id: 'notification-batch-storage', domains: { notifications } });
    const agent = new Agent({
      id: 'notification-batch-agent',
      name: 'Notification Batch Agent',
      instructions: 'Test',
      model: createTextStreamModel('notification batch response'),
    });
    new Mastra({ agents: { notificationBatchAgent: agent }, storage, logger: false });

    const results = await agent.sendNotificationSignal(
      [
        { source: 'github', kind: 'pull-request-ci-failure', priority: 'high', summary: 'CI failed' },
        { source: 'github', kind: 'pull-request-activity', priority: 'high', summary: 'Devin commented' },
      ],
      { resourceId: 'notification-batch-user', threadId: 'notification-batch-thread' },
    );

    expect(results).toHaveLength(2);
    expect(results[0]).toMatchObject({ decision: { action: 'deliver', reason: 'idle-high' } });
    await expect(results[0]?.accepted).resolves.toMatchObject({ action: expect.stringMatching(/wake|deliver/) });
    expect(results[0]?.signal).toMatchObject({ type: 'notification', tagName: 'notification' });
    expect(results[0]?.record).toMatchObject({ status: 'delivered', deliveredSignalId: results[0]?.signal?.id });
    expect(results[1]).toMatchObject({ decision: { action: 'deliver', reason: 'idle-high' } });
    await expect(results[1]?.accepted).resolves.toMatchObject({ action: expect.stringMatching(/wake|deliver/) });
    expect(results[1]?.signal).toMatchObject({ type: 'notification', tagName: 'notification' });
    expect(results[1]?.record).toMatchObject({ status: 'delivered', deliveredSignalId: results[1]?.signal?.id });
  });

  it('wakes idle threads for immediate medium-priority notification summaries', async () => {
    let streamCount = 0;
    const notifications = new InMemoryNotificationsStorage();
    const storage = new MastraCompositeStore({ id: 'medium-summary-wake-storage', domains: { notifications } });
    const agent = new Agent({
      id: 'medium-summary-wake-agent',
      name: 'Medium Summary Wake Agent',
      instructions: 'Test',
      model: new MockLanguageModelV2({
        doStream: async () => {
          streamCount += 1;
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: convertArrayToReadableStream([
              { type: 'stream-start', warnings: [] },
              { type: 'finish', finishReason: 'stop', usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 } },
            ]),
          };
        },
      }),
      notifications: {
        deliveryPolicy: {
          decide: ({ now }) => ({ action: 'summarize', summaryAt: now, reason: 'test-medium-summary-now' }),
        },
      },
    });
    new Mastra({ agents: { mediumSummaryWakeAgent: agent }, storage, logger: false });
    const subscription = await agent.subscribeToThread({
      threadId: 'medium-summary-wake-thread',
      resourceId: 'medium-summary-wake-user',
    });
    const nextRun = readNextRunWithParts(subscription.stream[Symbol.asyncIterator]());

    const result = await agent.sendNotificationSignal(
      { source: 'github', kind: 'pull-request-activity', priority: 'medium', summary: 'Devin commented' },
      { resourceId: 'medium-summary-wake-user', threadId: 'medium-summary-wake-thread' },
    );
    const subscribedRun = await withTimeout(nextRun, 'Timed out waiting for medium notification summary wake');
    const signalPart = subscribedRun.value.parts.find((part: any) => part.type === 'data-signal');

    expect(result.signal).toMatchObject({ type: 'notification', tagName: 'notification-summary' });
    expect(result.decision).toMatchObject({ action: 'summarize', reason: 'test-medium-summary-now' });
    expect(result.record).toMatchObject({
      status: 'pending',
      summaryAt: undefined,
      summarySignalId: result.signal?.id,
    });
    expect(signalPart?.data).toMatchObject({
      id: result.signal?.id,
      type: 'notification',
      tagName: 'notification-summary',
      contents: 'github: 1',
      attributes: { pending: 1 },
    });
    expect(streamCount).toBe(1);

    subscription.unsubscribe();
  });

  it('keeps immediate notification records pending when runtime rejects delivery', async () => {
    const notifications = new InMemoryNotificationsStorage();
    const storage = new MastraCompositeStore({ id: 'rejected-notification-storage', domains: { notifications } });
    const agent = new Agent({
      id: 'rejected-notification-agent',
      name: 'Rejected Notification Agent',
      instructions: 'Test',
      model: createTextStreamModel('unused'),
    });
    new Mastra({ agents: { rejectedNotificationAgent: agent }, storage, logger: false });
    const rejectedAccepted = Promise.reject(new Error('signal rejected'));
    // Attach a no-op catch so the rejection is considered handled and never surfaces as an
    // unhandled rejection; the dispatcher attaches its own awaiting handler in try/catch.
    rejectedAccepted.catch(() => {});
    const sendSignal = vi.spyOn(agentThreadStreamRuntime, 'sendSignal').mockReturnValue({
      accepted: rejectedAccepted,
      signal: createSignal({ type: 'notification', tagName: 'notification', contents: 'Rejected' }),
    } as any);

    try {
      const result = await agent.sendNotificationSignal(
        { source: 'github', kind: 'ci-status', priority: 'medium', summary: 'Rejected notification' },
        { resourceId: 'notification-user', threadId: 'notification-thread' },
      );

      expect(result.accepted).toBeUndefined();
      expect(result.record).toMatchObject({
        status: 'pending',
        deliveryAttempts: 1,
        lastDeliveryError: 'signal rejected',
      });
      expect(result.record.deliveredSignalId).toBeUndefined();
      const stored = await notifications.getNotification({ threadId: 'notification-thread', id: result.record.id });
      expect(stored).toMatchObject({ status: 'pending', deliveryAttempts: 1 });
      expect(stored?.deliveredSignalId).toBeUndefined();
    } finally {
      sendSignal.mockRestore();
    }
  });

  it('keeps a rejected notification summary due for retry', async () => {
    const notifications = new InMemoryNotificationsStorage();
    const storage = new MastraCompositeStore({ id: 'rejected-summary-storage', domains: { notifications } });
    const agent = new Agent({
      id: 'rejected-summary-agent',
      name: 'Rejected Summary Agent',
      instructions: 'Test',
      model: createTextStreamModel('unused'),
      notifications: {
        deliveryPolicy: {
          decide: ({ now }) => ({ action: 'summarize', summaryAt: now, reason: 'test-summary-now' }),
        },
      },
    });
    const mastra = new Mastra({ agents: { rejectedSummaryAgent: agent }, storage, logger: false });
    const rejectedAccepted = Promise.reject(new Error('summary rejected'));
    rejectedAccepted.catch(() => {});
    const sendSignal = vi.spyOn(agentThreadStreamRuntime, 'sendSignal').mockReturnValue({
      accepted: rejectedAccepted,
      signal: createSignal({ type: 'notification', tagName: 'notification-summary', contents: 'Rejected' }),
    } as any);

    try {
      const result = await agent.sendNotificationSignal(
        { source: 'github', kind: 'ci-status', priority: 'medium', summary: 'Rejected summary' },
        { resourceId: 'summary-user', threadId: 'summary-thread' },
      );

      expect(result.record).toMatchObject({
        status: 'pending',
        deliveryAttempts: 1,
        lastDeliveryError: 'summary rejected',
      });
      const stored = await notifications.getNotification({ threadId: 'summary-thread', id: result.record.id });
      expect(stored?.summaryAt).toBeInstanceOf(Date);
      await expect(notifications.listDueNotifications({ now: new Date() })).resolves.toMatchObject([
        { id: result.record.id },
      ]);

      for (let attempt = 2; attempt <= MAX_NOTIFICATION_DELIVERY_ATTEMPTS; attempt++) {
        await dispatchDueNotifications({ mastra, storage: notifications, now: new Date() });
        await expect(
          notifications.getNotification({ threadId: 'summary-thread', id: result.record.id }),
        ).resolves.toMatchObject({ deliveryAttempts: attempt });
      }

      await expect(
        notifications.getNotification({ threadId: 'summary-thread', id: result.record.id }),
      ).resolves.toMatchObject({ status: 'failed', deliveryAttempts: MAX_NOTIFICATION_DELIVERY_ATTEMPTS });
      await expect(notifications.listDueNotifications({ now: new Date() })).resolves.toEqual([]);

      await dispatchDueNotifications({ mastra, storage: notifications, now: new Date() });
      expect(sendSignal).toHaveBeenCalledTimes(MAX_NOTIFICATION_DELIVERY_ATTEMPTS);
    } finally {
      sendSignal.mockRestore();
    }
  });

  it('batches active high notifications for full delivery and active medium or low notifications for summaries', async () => {
    let releaseFirst!: () => void;
    const firstFinished = new Promise<void>(resolve => {
      releaseFirst = resolve;
    });
    let streamCount = 0;
    const notifications = new InMemoryNotificationsStorage();
    const storage = new MastraCompositeStore({
      id: 'active-priority-notification-storage',
      domains: { notifications },
    });
    const responseText = 'active response';
    const model = new MockLanguageModelV2({
      doStream: async () => {
        streamCount += 1;
        const currentStream = streamCount;
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: new ReadableStream({
            async start(controller) {
              controller.enqueue({ type: 'stream-start', warnings: [] });
              controller.enqueue({ type: 'text-start', id: `text-${currentStream}` });
              controller.enqueue({ type: 'text-delta', id: `text-${currentStream}`, delta: responseText });
              controller.enqueue({ type: 'text-end', id: `text-${currentStream}` });
              if (currentStream === 1) await firstFinished;
              controller.enqueue({
                type: 'finish',
                finishReason: 'stop',
                usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
              });
              controller.close();
            },
          }),
        };
      },
    });
    const agent = new Agent({
      id: 'active-priority-notification-agent',
      name: 'Active Priority Notification Agent',
      instructions: 'Test',
      model,
    });
    new Mastra({ agents: { activePriorityNotificationAgent: agent }, storage, logger: false });
    const subscription = await agent.subscribeToThread({
      threadId: 'active-priority-notification-thread',
      resourceId: 'active-priority-notification-user',
    });

    const stream = await agent.stream('Hello', {
      memory: { thread: 'active-priority-notification-thread', resource: 'active-priority-notification-user' },
    });
    await expect(waitForActiveRun(subscription)).resolves.toBe(stream.runId);

    const high = await agent.sendNotificationSignal(
      { source: 'github', kind: 'ci-status', priority: 'high', summary: 'CI failed' },
      { resourceId: 'active-priority-notification-user', threadId: 'active-priority-notification-thread' },
    );
    const medium = await agent.sendNotificationSignal(
      { source: 'slack', kind: 'mention', priority: 'medium', summary: 'Jane mentioned you' },
      { resourceId: 'active-priority-notification-user', threadId: 'active-priority-notification-thread' },
    );
    const low = await agent.sendNotificationSignal(
      { source: 'calendar', kind: 'event-reminder', priority: 'low', summary: 'Standup starts soon' },
      { resourceId: 'active-priority-notification-user', threadId: 'active-priority-notification-thread' },
    );

    expect(high.signal).toMatchObject({ type: 'notification', tagName: 'notification-summary' });
    expect(high.decision).toMatchObject({ action: 'summarize', reason: 'active-high-summary-then-full' });
    expect(high.record).toMatchObject({
      status: 'pending',
      deliveryReason: 'active-high-summary-then-full',
      summarySignalId: high.signal?.id,
    });
    expect(high.record.summaryAt).toBeUndefined();
    expect(high.record.deliverAt).toBeInstanceOf(Date);
    expect(medium.signal).toMatchObject({ type: 'notification', tagName: 'notification-summary' });
    expect(medium.decision).toMatchObject({ action: 'summarize', reason: 'active-batch-summary' });
    expect(medium.record).toMatchObject({
      status: 'pending',
      deliveryReason: 'active-batch-summary',
      summarySignalId: medium.signal?.id,
    });
    expect(medium.record.summaryAt).toBeUndefined();
    expect(low.signal).toBeUndefined();
    expect(low.decision).toMatchObject({ action: 'summarize', reason: 'active-batch-summary' });
    expect(low.record).toMatchObject({ status: 'pending', deliveryReason: 'active-batch-summary' });
    expect(low.record.summaryAt).toBeInstanceOf(Date);

    releaseFirst();
    // The high-priority summary signal is delivered to the active run, which the
    // agentic loop picks up and processes with an additional model iteration.
    await expect(stream.text).resolves.toBe('active responseactive response');
    expect(streamCount).toBe(2);
    subscription.unsubscribe();
  });

  it('summarizes active high-priority notifications immediately, then delivers full notifications when idle', async () => {
    let releaseFirst!: () => void;
    const firstFinished = new Promise<void>(resolve => {
      releaseFirst = resolve;
    });
    let streamCount = 0;
    const notifications = new InMemoryNotificationsStorage();
    const storage = new MastraCompositeStore({ id: 'high-active-integration-storage', domains: { notifications } });
    const agent = new Agent({
      id: 'high-active-integration-agent',
      name: 'High Active Integration Agent',
      instructions: 'Test',
      model: new MockLanguageModelV2({
        doStream: async () => {
          streamCount += 1;
          const responseText = `response ${streamCount}`;
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: new ReadableStream({
              async start(controller) {
                controller.enqueue({ type: 'stream-start', warnings: [] });
                controller.enqueue({ type: 'text-start', id: 'text-1' });
                controller.enqueue({ type: 'text-delta', id: 'text-1', delta: responseText });
                controller.enqueue({ type: 'text-end', id: 'text-1' });
                if (streamCount === 1) {
                  await firstFinished;
                }
                controller.enqueue({
                  type: 'finish',
                  finishReason: 'stop',
                  usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
                });
                controller.close();
              },
            }),
          };
        },
      }),
    });
    const mastra = new Mastra({ agents: { highActiveIntegrationAgent: agent }, storage, logger: false });
    const subscription = await agent.subscribeToThread({
      threadId: 'high-active-thread',
      resourceId: 'high-active-user',
    });
    const iterator = subscription.stream[Symbol.asyncIterator]();
    const firstRun = readNextRunWithParts(iterator);

    const stream = await agent.stream('Hello', {
      memory: { thread: 'high-active-thread', resource: 'high-active-user' },
    });
    const streamText = stream.text;
    await expect(waitForActiveRun(subscription)).resolves.toBe(stream.runId);

    const result = await agent.sendNotificationSignal(
      { source: 'github', kind: 'ci-status', priority: 'high', summary: 'CI failed on main' },
      { resourceId: 'high-active-user', threadId: 'high-active-thread' },
    );

    expect(result.signal).toMatchObject({ type: 'notification', tagName: 'notification-summary' });
    expect(result.decision).toMatchObject({ action: 'summarize', reason: 'active-high-summary-then-full' });
    expect(result.record).toMatchObject({
      status: 'pending',
      summarySignalId: result.signal?.id,
      deliveryReason: 'active-high-summary-then-full',
    });
    expect(result.record.summaryAt).toBeUndefined();
    expect(result.record.deliverAt).toBeInstanceOf(Date);

    releaseFirst();
    const subscribedSummary = await withTimeout(firstRun, 'Timed out waiting for high-priority summary signal');
    const summaryPart = subscribedSummary.value.parts.find((part: any) => part.type === 'data-signal');
    expect(summaryPart?.data).toMatchObject({
      id: result.signal?.id,
      type: 'notification',
      tagName: 'notification-summary',
      contents: 'github: 1',
      attributes: { pending: 1 },
    });
    // The summary signal is delivered to the active run, triggering an additional model iteration.
    expect(streamCount).toBe(2);
    await expect(
      notifications.getNotification({ threadId: 'high-active-thread', id: result.record.id }),
    ).resolves.toMatchObject({
      status: 'pending',
      summarySignalId: result.signal?.id,
      summaryAt: undefined,
      deliverAt: result.record.deliverAt,
    });

    const deliveryRun = readNextRunWithParts(iterator);
    const dispatchResult = await dispatchDueNotifications({ mastra, storage: notifications, now: new Date() });
    const subscribedDelivery = await withTimeout(deliveryRun, 'Timed out waiting for full high-priority delivery');
    const deliveryPart = subscribedDelivery.value.parts.find((part: any) => part.type === 'data-signal');

    expect(dispatchResult.failed).toEqual([]);
    expect(dispatchResult.signals[0]).toMatchObject({ type: 'notification', tagName: 'notification' });
    expect(deliveryPart?.data).toMatchObject({
      id: dispatchResult.signals[0]?.id,
      type: 'notification',
      tagName: 'notification',
      contents: 'CI failed on main',
      attributes: { source: 'github', kind: 'ci-status', priority: 'high', status: 'delivered' },
    });
    await expect(
      notifications.getNotification({ threadId: 'high-active-thread', id: result.record.id }),
    ).resolves.toMatchObject({
      status: 'delivered',
      deliveredSignalId: dispatchResult.signals[0]?.id,
    });
    await streamText;

    subscription.unsubscribe();
  });

  it('plans due notifications by thread so medium summaries cannot starve high full delivery', async () => {
    let releaseRun!: () => void;
    const runFinished = new Promise<void>(resolve => {
      releaseRun = resolve;
    });
    const notifications = new InMemoryNotificationsStorage();
    const storage = new MastraCompositeStore({ id: 'priority-dispatch-storage', domains: { notifications } });
    const agent = new Agent({
      id: 'priority-dispatch-agent',
      name: 'Priority Dispatch Agent',
      instructions: 'Test',
      model: new MockLanguageModelV2({
        doStream: async () => ({
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: new ReadableStream({
            async start(controller) {
              controller.enqueue({ type: 'stream-start', warnings: [] });
              controller.enqueue({ type: 'text-start', id: 'text-1' });
              controller.enqueue({ type: 'text-delta', id: 'text-1', delta: 'notification response' });
              controller.enqueue({ type: 'text-end', id: 'text-1' });
              await runFinished;
              controller.enqueue({
                type: 'finish',
                finishReason: 'stop',
                usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
              });
              controller.close();
            },
          }),
        }),
      }),
    });
    const mastra = new Mastra({ agents: { priorityDispatchAgent: agent }, storage, logger: false });
    const dueAt = new Date('2026-06-05T22:56:00Z');
    await notifications.createNotification({
      id: 'medium-ci-pending',
      agentId: 'priority-dispatch-agent',
      resourceId: 'priority-dispatch-user',
      threadId: 'priority-dispatch-thread',
      source: 'github',
      kind: 'pull-request-ci-pending',
      priority: 'medium',
      summary: 'CI is still pending',
      summaryAt: dueAt,
      createdAt: new Date('2026-06-05T22:55:00Z'),
    });
    const high = await notifications.createNotification({
      id: 'high-comment',
      agentId: 'priority-dispatch-agent',
      resourceId: 'priority-dispatch-user',
      threadId: 'priority-dispatch-thread',
      source: 'github',
      kind: 'pull-request-activity',
      priority: 'high',
      summary: 'Devin commented',
      deliverAt: dueAt,
      deliveryReason: 'active-high-summary-then-full',
      createdAt: new Date('2026-06-05T22:55:01Z'),
    });
    await notifications.updateNotification({
      id: high.id,
      threadId: high.threadId,
      summarySignalId: 'previous-summary-signal',
    });

    const dispatchResult = await dispatchDueNotifications({ mastra, storage: notifications, now: dueAt });

    expect(dispatchResult.failed).toEqual([]);
    expect(dispatchResult.signals.map(signal => signal.contents)).toEqual(['Devin commented', 'github: 1']);
    await expect(
      notifications.getNotification({ threadId: 'priority-dispatch-thread', id: 'high-comment' }),
    ).resolves.toMatchObject({
      status: 'delivered',
      deliveredSignalId: dispatchResult.signals[0]?.id,
    });
    await expect(
      notifications.getNotification({ threadId: 'priority-dispatch-thread', id: 'medium-ci-pending' }),
    ).resolves.toMatchObject({
      status: 'pending',
      summaryAt: undefined,
      summarySignalId: dispatchResult.signals[1]?.id,
    });

    releaseRun();
    await nextTick();
  });

  it('dispatches medium-priority active summaries through agent subscriptions without marking records delivered', async () => {
    let releaseFirst!: () => void;
    const firstFinished = new Promise<void>(resolve => {
      releaseFirst = resolve;
    });
    let streamCount = 0;
    const notifications = new InMemoryNotificationsStorage();
    const storage = new MastraCompositeStore({ id: 'medium-active-dispatch-storage', domains: { notifications } });
    const agent = new Agent({
      id: 'medium-active-dispatch-agent',
      name: 'Medium Active Dispatch Agent',
      instructions: 'Test',
      model: new MockLanguageModelV2({
        doStream: async () => {
          streamCount += 1;
          const responseText = `medium response ${streamCount}`;
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: new ReadableStream({
              async start(controller) {
                controller.enqueue({ type: 'stream-start', warnings: [] });
                controller.enqueue({ type: 'text-start', id: 'text-1' });
                controller.enqueue({ type: 'text-delta', id: 'text-1', delta: responseText });
                controller.enqueue({ type: 'text-end', id: 'text-1' });
                if (streamCount === 1) await firstFinished;
                controller.enqueue({
                  type: 'finish',
                  finishReason: 'stop',
                  usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
                });
                controller.close();
              },
            }),
          };
        },
      }),
    });
    const mastra = new Mastra({ agents: { mediumActiveDispatchAgent: agent }, storage, logger: false });
    const subscription = await agent.subscribeToThread({
      threadId: 'medium-active-thread',
      resourceId: 'medium-active-user',
    });
    const iterator = subscription.stream[Symbol.asyncIterator]();
    const firstRun = readNextRunWithParts(iterator);

    await notifications.createNotification({
      id: 'medium-active-notification',
      agentId: 'medium-active-dispatch-agent',
      resourceId: 'medium-active-user',
      threadId: 'medium-active-thread',
      source: 'slack',
      kind: 'mention',
      priority: 'medium',
      summary: 'Jane mentioned you',
      summaryAt: new Date('2026-05-30T12:00:00Z'),
    });
    const stream = await agent.stream('Hello', {
      memory: { thread: 'medium-active-thread', resource: 'medium-active-user' },
    });
    const streamText = stream.text;
    await expect(waitForActiveRun(subscription)).resolves.toBe(stream.runId);

    const dispatchResult = await dispatchDueNotifications({
      mastra,
      storage: notifications,
      now: new Date('2026-05-30T12:00:01Z'),
    });
    expect(dispatchResult.failed).toEqual([]);
    expect(dispatchResult.signals[0]).toMatchObject({ type: 'notification', tagName: 'notification-summary' });

    releaseFirst();
    const subscribedSummary = await withTimeout(firstRun, 'Timed out waiting for medium active summary signal');
    const summaryPart = subscribedSummary.value.parts.find((part: any) => part.type === 'data-signal');
    expect(summaryPart?.data).toMatchObject({
      id: dispatchResult.signals[0]?.id,
      type: 'notification',
      tagName: 'notification-summary',
      contents: 'slack: 1',
      attributes: { pending: 1 },
    });
    // The summary signal is delivered to the active run, triggering an additional model iteration.
    expect(streamCount).toBe(2);
    await expect(
      notifications.getNotification({ threadId: 'medium-active-thread', id: 'medium-active-notification' }),
    ).resolves.toMatchObject({
      status: 'pending',
      summaryAt: undefined,
      summarySignalId: dispatchResult.signals[0]?.id,
    });
    await streamText;

    subscription.unsubscribe();
  });

  it('saves and dispatches low-priority idle notification summaries through agent subscriptions without starting a run', async () => {
    let streamCount = 0;
    const pubsub = new AsyncCallbackPubSub();
    const notifications = new InMemoryNotificationsStorage();
    const storage = new MastraCompositeStore({ id: 'low-priority-notification-storage', domains: { notifications } });
    const agent = new Agent({
      id: 'low-priority-notification-agent',
      name: 'Low Priority Notification Agent',
      instructions: 'Test',
      model: new MockLanguageModelV2({
        doStream: async () => {
          streamCount += 1;
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: convertArrayToReadableStream([{ type: 'stream-start', warnings: [] }]),
          };
        },
      }),
    });
    const mastra = new Mastra({ agents: { lowPriorityNotificationAgent: agent }, storage, logger: false, pubsub });
    const subscription = await agent.subscribeToThread({
      threadId: 'notification-thread',
      resourceId: 'notification-user',
    });

    const result = await agent.sendNotificationSignal(
      { source: 'mastracode', kind: 'manual', priority: 'low', summary: 'Read when you have time' },
      { resourceId: 'notification-user', threadId: 'notification-thread' },
    );

    await nextTick();
    expect(streamCount).toBe(0);
    expect(result.signal).toBeUndefined();
    expect(result.accepted).toBeUndefined();
    expect(result).toMatchObject({
      decision: { action: 'summarize', reason: 'idle-low-summary' },
      record: { status: 'pending', deliveryReason: 'idle-low-summary' },
    });
    expect(result.record.deliverAt).toBeUndefined();
    expect(result.record.summaryAt).toBeInstanceOf(Date);

    const dispatchNow = new Date((result.record.summaryAt?.getTime() ?? Date.now()) + 1);
    const nextRun = readNextRunWithParts(subscription.stream[Symbol.asyncIterator]());
    const dispatchResult = await dispatchDueNotifications({ mastra, storage: notifications, now: dispatchNow });
    const subscribedRun = await withTimeout(
      nextRun,
      'Timed out waiting for low-priority notification summary broadcast',
    );

    expect(dispatchResult.failed).toEqual([]);
    expect(dispatchResult.signals[0]).toMatchObject({ type: 'notification', tagName: 'notification-summary' });
    expect(streamCount).toBe(0);
    const signalPart = subscribedRun.value.parts.find((part: any) => part.type === 'data-signal');
    expect(signalPart?.data).toMatchObject({
      id: dispatchResult.signals[0]?.id,
      type: 'notification',
      tagName: 'notification-summary',
      contents: 'mastracode: 1',
      attributes: { pending: 1 },
    });
    await expect(
      notifications.getNotification({ threadId: 'notification-thread', id: result.record.id }),
    ).resolves.toMatchObject({
      status: 'pending',
      summaryAt: undefined,
      summarySignalId: dispatchResult.signals[0]?.id,
    });

    subscription.unsubscribe();
  });

  it('notification inbox read injects a real notification signal through agent subscriptions', async () => {
    let streamCount = 0;
    const notifications = new InMemoryNotificationsStorage();
    const storage = new MastraCompositeStore({ id: 'inbox-read-delivery-storage', domains: { notifications } });
    const agent = new Agent({
      id: 'inbox-read-delivery-agent',
      name: 'Inbox Read Delivery Agent',
      instructions: 'Test',
      model: new MockLanguageModelV2({
        doStream: async () => {
          streamCount += 1;
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: convertArrayToReadableStream([
              { type: 'stream-start', warnings: [] },
              { type: 'finish', finishReason: 'stop', usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 } },
            ]),
          };
        },
      }),
    });
    const mastra = new Mastra({ agents: { inboxReadDeliveryAgent: agent }, storage, logger: false });
    const tool = createNotificationInboxTool({ storage: notifications });
    await notifications.createNotification({
      id: 'inbox-read-notification',
      agentId: 'inbox-read-delivery-agent',
      resourceId: 'inbox-read-user',
      threadId: 'inbox-read-thread',
      source: 'github',
      kind: 'ci-status',
      priority: 'medium',
      summary: 'CI failed on main',
    });
    const subscription = await agent.subscribeToThread({
      threadId: 'inbox-read-thread',
      resourceId: 'inbox-read-user',
    });
    const nextRun = readNextRunWithParts(subscription.stream[Symbol.asyncIterator]());

    const result = await tool.execute?.({ action: 'read', id: 'inbox-read-notification' }, {
      agent: { agentId: 'inbox-read-delivery-agent', threadId: 'inbox-read-thread', resourceId: 'inbox-read-user' },
      mastra,
    } as any);
    const subscribedRun = await withTimeout(nextRun, 'Timed out waiting for inbox read notification delivery');
    const signalPart = subscribedRun.value.parts.find((part: any) => part.type === 'data-signal');

    expect(result).toMatchObject({ message: '1 notification will now be delivered.', delivered: 1 });
    expect(signalPart?.data).toMatchObject({
      type: 'notification',
      tagName: 'notification',
      contents: 'CI failed on main',
      attributes: { source: 'github', kind: 'ci-status', priority: 'medium', status: 'delivered' },
    });
    expect(streamCount).toBe(1);
    await expect(
      notifications.getNotification({ threadId: 'inbox-read-thread', id: 'inbox-read-notification' }),
    ).resolves.toMatchObject({
      status: 'seen',
      deliveredSignalId: signalPart?.data.id,
    });

    subscription.unsubscribe();
  });

  it('notification inbox read marks already-delivered notifications seen without injecting another signal', async () => {
    let streamCount = 0;
    const notifications = new InMemoryNotificationsStorage();
    const storage = new MastraCompositeStore({
      id: 'inbox-read-already-delivered-storage',
      domains: { notifications },
    });
    const agent = new Agent({
      id: 'inbox-read-already-delivered-agent',
      name: 'Inbox Read Already Delivered Agent',
      instructions: 'Test',
      model: new MockLanguageModelV2({
        doStream: async () => {
          streamCount += 1;
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: convertArrayToReadableStream([{ type: 'stream-start', warnings: [] }]),
          };
        },
      }),
    });
    const mastra = new Mastra({ agents: { inboxReadAlreadyDeliveredAgent: agent }, storage, logger: false });
    const tool = createNotificationInboxTool({ storage: notifications });
    await notifications.createNotification({
      id: 'already-delivered-notification',
      agentId: 'inbox-read-already-delivered-agent',
      resourceId: 'already-delivered-user',
      threadId: 'already-delivered-thread',
      source: 'github',
      kind: 'ci-status',
      priority: 'high',
      summary: 'CI failed earlier',
    });
    await notifications.updateNotification({
      threadId: 'already-delivered-thread',
      id: 'already-delivered-notification',
      status: 'delivered',
      deliveredSignalId: 'existing-signal-id',
    });

    const result = await tool.execute?.({ action: 'read', id: 'already-delivered-notification' }, {
      agent: {
        agentId: 'inbox-read-already-delivered-agent',
        threadId: 'already-delivered-thread',
        resourceId: 'already-delivered-user',
      },
      mastra,
    } as any);

    expect(result).toMatchObject({ delivered: 0, markedSeen: 1, message: 'No unread notifications needed delivery.' });
    expect(streamCount).toBe(0);
    await expect(
      notifications.getNotification({ threadId: 'already-delivered-thread', id: 'already-delivered-notification' }),
    ).resolves.toMatchObject({
      status: 'seen',
      deliveredSignalId: 'existing-signal-id',
    });
  });

  it('dispatches low-priority idle notification summaries without subscribers', async () => {
    let streamCount = 0;
    const pubsub = new AsyncCallbackPubSub();
    const notifications = new InMemoryNotificationsStorage();
    const storage = new MastraCompositeStore({ id: 'no-subscriber-notification-storage', domains: { notifications } });
    const agent = new Agent({
      id: 'no-subscriber-notification-agent',
      name: 'No Subscriber Notification Agent',
      instructions: 'Test',
      model: new MockLanguageModelV2({
        doStream: async () => {
          streamCount += 1;
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: convertArrayToReadableStream([{ type: 'stream-start', warnings: [] }]),
          };
        },
      }),
    });
    const mastra = new Mastra({ agents: { noSubscriberNotificationAgent: agent }, storage, logger: false, pubsub });

    const result = await agent.sendNotificationSignal(
      { source: 'mastracode', kind: 'manual', priority: 'low', summary: 'No one is watching' },
      { resourceId: 'notification-user', threadId: 'notification-thread' },
    );
    const dispatchNow = new Date((result.record.summaryAt?.getTime() ?? Date.now()) + 1);
    const dispatchResult = await withTimeout(
      dispatchDueNotifications({ mastra, storage: notifications, now: dispatchNow }),
      'Timed out dispatching low-priority notification summary without subscribers',
    );

    expect(dispatchResult.failed).toEqual([]);
    expect(dispatchResult.signals[0]).toMatchObject({ type: 'notification', tagName: 'notification-summary' });
    expect(streamCount).toBe(0);
    await expect(
      notifications.getNotification({ threadId: 'notification-thread', id: result.record.id }),
    ).resolves.toMatchObject({
      status: 'pending',
      summaryAt: undefined,
      summarySignalId: dispatchResult.signals[0]?.id,
    });
  });

  it('defers notification records without starting an idle run', async () => {
    let streamCount = 0;
    const notifications = new InMemoryNotificationsStorage();
    const storage = new MastraCompositeStore({ id: 'deferred-notification-storage', domains: { notifications } });
    const deliverAt = new Date('2026-05-30T12:00:00Z');
    const agent = new Agent({
      id: 'deferred-notification-agent',
      name: 'Deferred Notification Agent',
      instructions: 'Test',
      model: new MockLanguageModelV2({
        doStream: async () => {
          streamCount += 1;
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: convertArrayToReadableStream([{ type: 'stream-start', warnings: [] }]),
          };
        },
      }),
      notifications: {
        deliveryPolicy: {
          decide: () => ({ action: 'defer', deliverAt, reason: 'after-hours' }),
        },
      },
    });
    new Mastra({ agents: { deferredNotificationAgent: agent }, storage, logger: false });

    const result = await agent.sendNotificationSignal(
      {
        source: 'calendar',
        kind: 'event-reminder',
        summary: 'Planning starts tomorrow',
      },
      { resourceId: 'notification-user', threadId: 'notification-thread' },
    );

    await nextTick();
    expect(streamCount).toBe(0);
    expect(result).toMatchObject({
      decision: { action: 'defer', reason: 'after-hours' },
      record: { status: 'pending', deliveryReason: 'after-hours' },
    });
    expect(result.signal).toBeUndefined();
    expect(result.accepted).toBeUndefined();
    expect(result.record.deliverAt?.toISOString()).toBe(deliverAt.toISOString());
  });

  it('coalesces pending notification records through sendNotificationSignal', async () => {
    const notifications = new InMemoryNotificationsStorage();
    const storage = new MastraCompositeStore({ id: 'coalesced-notification-storage', domains: { notifications } });
    const agent = new Agent({
      id: 'coalesced-notification-agent',
      name: 'Coalesced Notification Agent',
      instructions: 'Test',
      model: createTextStreamModel('notification response'),
      notifications: {
        deliveryPolicy: {
          default: { action: 'summarize', summaryAt: new Date('2026-05-30T12:00:00Z') },
        },
      },
    });
    new Mastra({ agents: { coalescedNotificationAgent: agent }, storage, logger: false });

    const first = await agent.sendNotificationSignal(
      {
        source: 'github',
        kind: 'ci-status',
        summary: 'CI failed: one test',
        dedupeKey: 'main-ci',
      },
      { resourceId: 'notification-user', threadId: 'notification-thread' },
    );
    const second = await agent.sendNotificationSignal(
      {
        source: 'github',
        kind: 'ci-status',
        summary: 'CI failed: three tests',
        dedupeKey: 'main-ci',
      },
      { resourceId: 'notification-user', threadId: 'notification-thread' },
    );

    expect(second.record.id).toBe(first.record.id);
    expect(second.record).toMatchObject({ status: 'pending', summary: 'CI failed: three tests', coalescedCount: 2 });
    await expect(notifications.listNotifications({ threadId: 'notification-thread' })).resolves.toHaveLength(1);
  });

  it('throws a clear error when notification storage is missing', async () => {
    const agent = new Agent({
      id: 'missing-notification-storage-agent',
      name: 'Missing Notification Storage Agent',
      instructions: 'Test',
      model: createTextStreamModel('notification response'),
    });

    await expect(
      agent.sendNotificationSignal(
        { source: 'github', kind: 'ci-status', summary: 'CI failed' },
        { resourceId: 'notification-user', threadId: 'notification-thread' },
      ),
    ).rejects.toThrow('sendNotificationSignal requires a notifications storage domain');
  });

  it('delivers sendMessage into an active same-agent run', async () => {
    let releaseFirst!: () => void;
    const firstFinished = new Promise<void>(resolve => {
      releaseFirst = resolve;
    });
    let streamCount = 0;
    const prompts: any[][] = [];
    const handledSignalMetadata: unknown[] = [];
    const model = new MockLanguageModelV2({
      doStream: async ({ prompt }) => {
        streamCount += 1;
        prompts.push(prompt);
        const responseText = streamCount === 1 ? 'first response' : 'message response';
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: new ReadableStream({
            async start(controller) {
              controller.enqueue({ type: 'stream-start', warnings: [] });
              controller.enqueue({
                type: 'response-metadata',
                id: `send-message-${streamCount}`,
                modelId: 'mock-model-id',
                timestamp: new Date(0),
              });
              controller.enqueue({ type: 'text-start', id: 'text-1' });
              controller.enqueue({ type: 'text-delta', id: 'text-1', delta: responseText });
              controller.enqueue({ type: 'text-end', id: 'text-1' });
              if (streamCount === 1) {
                await firstFinished;
              }
              controller.enqueue({
                type: 'finish',
                finishReason: 'stop',
                usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
              });
              controller.close();
            },
          }),
        };
      },
    });
    const agent = new Agent({
      id: 'active-message-agent',
      name: 'Active Message Agent',
      instructions: 'Test',
      model,
      inputProcessors: [
        {
          id: 'capture-active-message-metadata',
          processInputStep: ({ messageList }) => {
            for (const message of messageList.get.input.db()) {
              if (message.role !== 'signal') continue;
              const signal = message.content.metadata?.signal as Record<string, unknown> | undefined;
              if (signal?.metadata !== undefined) handledSignalMetadata.push(signal.metadata);
            }
          },
        },
      ],
    });
    const subscription = await agent.subscribeToThread({
      threadId: 'active-message-thread',
      resourceId: 'active-message-user',
    });

    const stream = await agent.stream('Hello', {
      memory: { thread: 'active-message-thread', resource: 'active-message-user' },
    });
    await expect(waitForActiveRun(subscription)).resolves.toBe(stream.runId);
    const result = agent.sendMessage(
      {
        contents: 'Hello while active',
        metadata: { channel: { attachmentId: 'file-1' } },
      },
      {
        resourceId: 'active-message-user',
        threadId: 'active-message-thread',
      },
    );

    await expect(result.accepted).resolves.toMatchObject({ action: 'deliver', runId: stream.runId });
    releaseFirst();
    await expect(stream.text).resolves.toBe('first responsemessage response');
    expect(streamCount).toBe(2);
    expect(JSON.stringify(prompts[1])).toContain('Hello while active');
    expect(handledSignalMetadata).toContainEqual({ channel: { attachmentId: 'file-1' } });

    subscription.unsubscribe();
  });

  it('queues sendMessage behind a suspended same-agent approval run', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const agent = {
      id: 'suspended-message-agent',
      stream: vi.fn(),
    } as unknown as Agent<any, any, any, any>;
    const runId = 'suspended-message-run';
    const threadId = 'suspended-message-thread';
    const resourceId = 'suspended-message-user';
    let finishRun!: () => void;
    const finished = new Promise<void>(resolve => {
      finishRun = resolve;
    });
    const subscription = await runtime.subscribeToThread(agent, { threadId, resourceId });
    const iterator = subscription.stream[Symbol.asyncIterator]();

    try {
      runtime.registerRun(
        agent,
        {
          runId,
          status: 'suspended',
          fullStream: new ReadableStream({
            start(controller) {
              controller.enqueue({ type: 'start', runId });
              controller.enqueue({
                type: 'tool-call-approval',
                runId,
                payload: { toolCallId: 'tool-call-1', toolName: 'testTool' },
              });
            },
          }),
          _waitUntilFinished: () => finished,
        } as any,
        { memory: { thread: threadId, resource: resourceId } } as any,
      );

      await withTimeout(iterator.next(), 'Timed out waiting for approval run start');
      await withTimeout(iterator.next(), 'Timed out waiting for approval chunk');
      expect(runtime.getThreadState({ resourceId, threadId })).toBe('active');

      const result = runtime.sendMessage(agent, 'Queued behind approval', { resourceId, threadId });

      await expect(result.accepted).resolves.toMatchObject({ action: 'deliver', runId });
      expect((agent as any).stream).not.toHaveBeenCalled();
      const [signal] = runtime.drainPendingSignals(runId);
      expect(signal).toMatchObject({ type: 'user', contents: 'Queued behind approval' });
    } finally {
      finishRun();
      subscription.unsubscribe();
    }
  });

  it('re-registers an approval-suspended run on resume without rejecting the retained record', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const agent = { id: 'approval-resume-agent' } as Agent<any, any, any, any>;
    const threadId = 'approval-resume-thread';
    const resourceId = 'approval-resume-user';
    const runId = 'approval-resume-run';

    const subscription = await runtime.subscribeToThread(agent, { threadId, resourceId });
    const iterator = subscription.stream[Symbol.asyncIterator]();

    try {
      // Register the suspended run that emits a tool-call-approval part. The
      // completion finalizer surfaces run-suspended and retains its records so
      // the thread stays blocked awaiting approval.
      let finishSuspended!: () => void;
      const suspendedFinished = new Promise<void>(resolve => {
        finishSuspended = resolve;
      });
      const suspendedCompletion = runtime.registerRun(
        agent,
        {
          runId,
          status: 'suspended',
          fullStream: new ReadableStream({
            start(controller) {
              controller.enqueue({ type: 'start', runId });
              controller.enqueue({
                type: 'tool-call-approval',
                runId,
                payload: { toolCallId: 'tool-call-1', toolName: 'testTool' },
              });
              controller.close();
            },
          }),
          _waitUntilFinished: () => suspendedFinished,
        } as any,
        { memory: { thread: threadId, resource: resourceId } } as any,
      );

      await withTimeout(iterator.next(), 'Timed out waiting for approval run start');
      await withTimeout(iterator.next(), 'Timed out waiting for approval chunk');
      // Drive and await the finalizer so run-suspended has propagated to the
      // subscriber; the approval marker must keep the thread blocked (active).
      finishSuspended();
      await withTimeout(suspendedCompletion ?? Promise.resolve(), 'Timed out waiting for approval-suspended finalizer');
      await nextTick();
      expect(runtime.getThreadState({ resourceId, threadId })).toBe('active');

      // The resume reuses the same runId. Without the fix this throws
      // "already registered" / "already reserved" and the resume wedges.
      const resumedRun = readNextRun(iterator);
      let finishResumed!: () => void;
      const resumedFinished = new Promise<void>(resolve => {
        finishResumed = resolve;
      });
      expect(() =>
        runtime.registerRun(
          agent,
          {
            runId,
            status: 'running',
            fullStream: new ReadableStream({
              start(controller) {
                setTimeout(() => {
                  controller.enqueue({ type: 'start', runId });
                  controller.enqueue({ type: 'text-start', runId, payload: { id: 'text-1' } });
                  controller.enqueue({
                    type: 'text-delta',
                    runId,
                    payload: { id: 'text-1', text: 'approved response' },
                  });
                  controller.enqueue({ type: 'text-end', runId, payload: { id: 'text-1' } });
                  controller.enqueue({
                    type: 'finish',
                    runId,
                    payload: { usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 }, finishReason: 'stop' },
                  });
                  controller.close();
                  finishResumed();
                }, 5);
              },
            }),
            _waitUntilFinished: () => resumedFinished,
          } as any,
          { memory: { thread: threadId, resource: resourceId } } as any,
        ),
      ).not.toThrow();

      await expect(withTimeout(resumedRun, 'Timed out waiting for resumed run')).resolves.toMatchObject({
        value: { runId, text: 'approved response' },
      });
    } finally {
      subscription.unsubscribe();
    }
  });

  it('lets the same agent reserve a fresh turn while retaining a suspended run for explicit resume', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    const agent = { id: 'suspended-fresh-turn-agent' } as Agent<any, any, any, any>;
    const threadId = 'suspended-fresh-turn-thread';
    const resourceId = 'suspended-fresh-turn-user';
    const suspendedRunId = 'suspended-fresh-turn-old-run';

    const suspendedCompletion = runtime.registerRun(
      agent,
      {
        runId: suspendedRunId,
        status: 'suspended',
        fullStream: new ReadableStream({
          start(controller) {
            controller.enqueue({ type: 'start', runId: suspendedRunId });
            controller.enqueue({
              type: 'tool-call-approval',
              runId: suspendedRunId,
              payload: { toolCallId: 'fresh-turn-tool-call', toolName: 'freshTurnTool' },
            });
            controller.close();
          },
        }),
        _waitUntilFinished: () => Promise.resolve(),
      } as any,
      {
        runId: suspendedRunId,
        memory: { thread: threadId, resource: resourceId },
      } as any,
      pubsub,
    );
    await withTimeout(suspendedCompletion ?? Promise.resolve(), 'Timed out finalizing retained suspended run');

    expect(runtime.getActiveThreadRunId({ resourceId, threadId }, pubsub)).toBe(suspendedRunId);

    const freshOptions = {
      runId: 'suspended-fresh-turn-new-run',
      memory: { thread: threadId, resource: resourceId },
    } as any;
    await runtime.waitForThreadRunReservation(freshOptions, pubsub, agent.id);
    const releaseFreshReservation = runtime.reserveRun(freshOptions, pubsub, agent.id);

    expect(releaseFreshReservation).toBeDefined();
    expect(runtime.getActiveThreadRunId({ resourceId, threadId }, pubsub)).toBe(freshOptions.runId);
    expect(runtime.getRunOutput(suspendedRunId, pubsub)).toBeDefined();
    expect(
      runtime.getResumableThreadRun(
        { resourceId, threadId, runId: suspendedRunId, toolCallId: 'fresh-turn-tool-call' },
        pubsub,
      ),
    ).toEqual({ runId: suspendedRunId, toolCallId: 'fresh-turn-tool-call' });

    releaseFreshReservation?.();
  });

  it('restores a queued signal when the drain follow-up stream fails', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const streamMock = vi.fn().mockRejectedValue(new Error('connection error: ECONNRESET'));
    const agent = {
      id: 'drain-failure-agent',
      stream: streamMock,
    } as unknown as Agent<any, any, any, any>;
    const runId = 'drain-failure-run';
    const threadId = 'drain-failure-thread';
    const resourceId = 'drain-failure-user';
    let finishRun!: () => void;
    const finished = new Promise<void>(resolve => {
      finishRun = resolve;
    });
    const subscription = await runtime.subscribeToThread(agent, { threadId, resourceId });
    const iterator = subscription.stream[Symbol.asyncIterator]();

    try {
      runtime.registerRun(
        agent,
        {
          runId,
          status: 'running',
          fullStream: new ReadableStream({
            start(controller) {
              controller.enqueue({ type: 'start', runId });
              controller.enqueue({ type: 'finish', runId, payload: {} });
              controller.close();
            },
          }),
          _waitUntilFinished: () => finished,
        } as any,
        { memory: { thread: threadId, resource: resourceId } } as any,
      );

      await withTimeout(readNextRunWithParts(iterator), 'Timed out waiting for the first run to stream');
      const result = runtime.sendMessage(agent, 'steer follow-up', { resourceId, threadId });
      await expect(result.accepted).resolves.toMatchObject({ action: 'deliver', runId });
      expect(streamMock).not.toHaveBeenCalled();

      finishRun();
      await waitForCondition(() => streamMock.mock.calls.length === 1);
      await nextTick();
      await nextTick();

      // Probe: register a fresh run on the same thread so the public
      // drainPendingSignals can resolve the thread key, then inspect the queue.
      // The failed signal must have been restored to the queue head.
      runtime.registerRun(
        agent,
        {
          runId: 'drain-failure-probe',
          status: 'running',
          fullStream: new ReadableStream({
            start(controller) {
              controller.enqueue({ type: 'start', runId: 'drain-failure-probe' });
            },
          }),
          _waitUntilFinished: () => new Promise<void>(() => {}),
        } as any,
        { memory: { thread: threadId, resource: resourceId } } as any,
      );
      const restored = runtime.drainPendingSignals('drain-failure-probe');
      expect(restored).toHaveLength(1);
      expect(restored[0]).toMatchObject({ type: 'user', contents: 'steer follow-up' });
    } finally {
      subscription.unsubscribe();
    }
  });

  it('publishes run-failed when the drain follow-up stream fails', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const streamMock = vi.fn().mockRejectedValue(new Error('connection error: ECONNRESET'));
    const agent = {
      id: 'drain-failure-event-agent',
      stream: streamMock,
    } as unknown as Agent<any, any, any, any>;
    const runId = 'drain-failure-event-run';
    const threadId = 'drain-failure-event-thread';
    const resourceId = 'drain-failure-event-user';
    let finishRun!: () => void;
    const finished = new Promise<void>(resolve => {
      finishRun = resolve;
    });
    const subscription = await runtime.subscribeToThread(agent, { threadId, resourceId });
    const iterator = subscription.stream[Symbol.asyncIterator]();

    try {
      runtime.registerRun(
        agent,
        {
          runId,
          status: 'running',
          fullStream: new ReadableStream({
            start(controller) {
              controller.enqueue({ type: 'start', runId });
              controller.enqueue({ type: 'finish', runId, payload: {} });
              controller.close();
            },
          }),
          _waitUntilFinished: () => finished,
        } as any,
        { memory: { thread: threadId, resource: resourceId } } as any,
      );

      await withTimeout(readNextRunWithParts(iterator), 'Timed out waiting for the first run to stream');
      const result = runtime.sendMessage(agent, 'steer follow-up', { resourceId, threadId });
      await expect(result.accepted).resolves.toMatchObject({ action: 'deliver', runId });

      finishRun();
      await waitForCondition(() => streamMock.mock.calls.length === 1);

      const errorRun = await withTimeout(
        readNextRunWithParts(iterator),
        'Timed out waiting for the run-failed error run',
        1000,
      );
      expect(errorRun.done).toBe(false);
      expect(errorRun.value?.part?.type).toBe('error');
      const errorPayload = errorRun.value?.part?.payload?.error;
      const errorMessage = errorPayload instanceof Error ? errorPayload.message : String(errorPayload);
      expect(errorMessage).toContain('failed to start follow-up run for queued message');
    } finally {
      subscription.unsubscribe();
    }
  });

  function createFakeThreadRun(runId: string, finished: Promise<void>) {
    return {
      runId,
      status: 'running',
      fullStream: new ReadableStream({
        start(controller) {
          controller.enqueue({ type: 'start', runId });
          controller.enqueue({ type: 'finish', runId, payload: {} });
          controller.close();
        },
      }),
      _waitUntilFinished: () => finished,
    } as any;
  }

  it('restores the failed signal at the queue head ahead of later queued signals', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const streamMock = vi.fn().mockRejectedValue(new Error('connection error: ECONNRESET'));
    const agent = {
      id: 'drain-order-agent',
      stream: streamMock,
    } as unknown as Agent<any, any, any, any>;
    const runId = 'drain-order-run';
    const threadId = 'drain-order-thread';
    const resourceId = 'drain-order-user';
    let finishRun!: () => void;
    const finished = new Promise<void>(resolve => {
      finishRun = resolve;
    });

    runtime.registerRun(agent, createFakeThreadRun(runId, finished), {
      memory: { thread: threadId, resource: resourceId },
    } as any);

    const first = runtime.sendMessage(agent, 'first steer', { resourceId, threadId });
    const second = runtime.sendMessage(agent, 'second steer', { resourceId, threadId });
    await expect(first.accepted).resolves.toMatchObject({ action: 'deliver', runId });
    await expect(second.accepted).resolves.toMatchObject({ action: 'deliver', runId });

    finishRun();
    await waitForCondition(() => streamMock.mock.calls.length === 1);
    await nextTick();
    await nextTick();

    runtime.registerRun(agent, createFakeThreadRun('drain-order-probe', new Promise<void>(() => {})), {
      memory: { thread: threadId, resource: resourceId },
    } as any);
    const restored = runtime.drainPendingSignals('drain-order-probe');
    expect(restored).toHaveLength(2);
    expect(restored[0]).toMatchObject({ contents: 'first steer' });
    expect(restored[1]).toMatchObject({ contents: 'second steer' });
  });

  it('releases the thread lease with the failed run id when the handoff starts nothing', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new ControlledLeasePubSub();
    const releaseSpy = vi.spyOn(pubsub, 'releaseLease');
    const transferSpy = vi.spyOn(pubsub, 'transferLease');
    const acquireSpy = vi.spyOn(pubsub, 'acquireLease');
    const streamMock = vi.fn().mockRejectedValue(new Error('connection error: ECONNRESET'));
    const agent = {
      id: 'drain-release-agent',
      stream: streamMock,
    } as unknown as Agent<any, any, any, any>;
    const runId = 'drain-release-run';
    const threadId = 'drain-release-thread';
    const resourceId = 'drain-release-user';
    let finishRun!: () => void;
    const finished = new Promise<void>(resolve => {
      finishRun = resolve;
    });

    runtime.registerRun(
      agent,
      createFakeThreadRun(runId, finished),
      { memory: { thread: threadId, resource: resourceId } } as any,
      pubsub,
    );

    const result = runtime.sendMessage(agent, 'steer follow-up', { resourceId, threadId }, pubsub);
    await expect(result.accepted).resolves.toMatchObject({ action: 'deliver', runId });

    finishRun();
    await waitForCondition(() => streamMock.mock.calls.length === 1);
    const nextRunId = streamMock.mock.calls[0]?.[1]?.runId;
    expect(nextRunId).toBeTruthy();
    expect(nextRunId).not.toBe(runId);

    // Follow-up handoff ownership is a process-attempt token. Capture the
    // transfer target, or the fallback acquire owner if the old lease was gone,
    // so this assertion catches cleanup that releases by public run id.
    const transferCalls = transferSpy.mock.calls;
    const acquireCalls = acquireSpy.mock.calls;
    const attemptedLeaseOwner =
      transferCalls[transferCalls.length - 1]?.[2] ?? acquireCalls[acquireCalls.length - 1]?.[1];
    expect(attemptedLeaseOwner).toBeTruthy();
    await waitForCondition(() => releaseSpy.mock.calls.some(call => call[1] === attemptedLeaseOwner));
    expect(releaseSpy).toHaveBeenCalledWith(expect.stringContaining(threadId), attemptedLeaseOwner);
  });

  it('restores the signal when the lease transfer step throws', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new ControlledLeasePubSub();
    const acquireSpy = vi.spyOn(pubsub, 'acquireLease');
    const releaseSpy = vi.spyOn(pubsub, 'releaseLease');
    // Only a SYNCHRONOUS throw reaches the drain's catch: async provider
    // rejections are swallowed inside the lease helpers and take the
    // lease-lost branch instead. Throw once so the follow-up drain below can
    // prove the restored signal still delivers afterwards.
    const transferSpy = vi.spyOn(pubsub, 'transferLease').mockImplementationOnce(() => {
      throw new Error('lease backend down');
    });
    const streamMock = vi.fn().mockResolvedValue({} as any);
    const agent = {
      id: 'drain-lease-throw-agent',
      stream: streamMock,
    } as unknown as Agent<any, any, any, any>;
    const runId = 'drain-lease-throw-run';
    const threadId = 'drain-lease-throw-thread';
    const resourceId = 'drain-lease-throw-user';
    let finishRun!: () => void;
    const finished = new Promise<void>(resolve => {
      finishRun = resolve;
    });

    runtime.registerRun(
      agent,
      createFakeThreadRun(runId, finished),
      { memory: { thread: threadId, resource: resourceId } } as any,
      pubsub,
    );

    const result = runtime.sendMessage(agent, 'steer follow-up', { resourceId, threadId }, pubsub);
    await expect(result.accepted).resolves.toMatchObject({ action: 'deliver', runId });

    finishRun();
    await waitForCondition(() =>
      pubsub.publishedData.some(
        data => data?.type === 'run-failed' && String(data?.error).includes('failed to start follow-up run'),
      ),
    );
    expect(streamMock).not.toHaveBeenCalled();

    // Exact-token release proof: raw run ids never match owner tokens, so
    // asserting against them proves nothing. The failed attempt's candidate
    // token never committed (the throw preceded the owner swap); the bounded
    // owner reconciliation verifies the finished predecessor still holds the
    // key, so the catch must release THAT captured token — after awaiting the
    // handoff settlement, not from a detached chain.
    const key = `${resourceId}\u0000${threadId}`;
    const predecessorToken = acquireSpy.mock.calls[0]?.[1];
    const failedAttemptToken = transferSpy.mock.calls[0]?.[2];
    expect(predecessorToken).toBeTruthy();
    expect(failedAttemptToken).toBeTruthy();
    expect(failedAttemptToken).not.toBe(predecessorToken);
    // No eager reacquisition of the failed attempt's token while the lease
    // operation's outcome was still unknown.
    expect(acquireSpy.mock.calls.some(call => call[1] === failedAttemptToken)).toBe(false);
    await waitForCondition(() => releaseSpy.mock.calls.some(call => call[1] === predecessorToken));
    await waitForCondition(() => pubsub.owners.get(key) === undefined);

    // Prove a subsequent NATURAL drain actually delivers the restored signal,
    // not merely that it sits in the queue.
    let finishSecondRun!: () => void;
    const secondFinished = new Promise<void>(resolve => {
      finishSecondRun = resolve;
    });
    runtime.registerRun(
      agent,
      createFakeThreadRun('drain-lease-throw-second', secondFinished),
      { memory: { thread: threadId, resource: resourceId } } as any,
      pubsub,
    );
    finishSecondRun();
    await waitForCondition(() => streamMock.mock.calls.length === 1);
    expect(JSON.stringify(streamMock.mock.calls[0]?.[0])).toContain('steer follow-up');
  });

  it('releases the stale previous-run lease on a transfer throw even when an idle signal is queued', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new ControlledLeasePubSub();
    const acquireSpy = vi.spyOn(pubsub, 'acquireLease');
    const releaseSpy = vi.spyOn(pubsub, 'releaseLease');
    const transferSpy = vi.spyOn(pubsub, 'transferLease').mockImplementationOnce(() => {
      throw new Error('lease backend down');
    });
    const streamMock = vi.fn().mockResolvedValue({} as any);
    const agent = {
      id: 'drain-lease-throw-idle-agent',
      stream: streamMock,
    } as unknown as Agent<any, any, any, any>;
    const runId = 'drain-lease-throw-idle-run';
    const threadId = 'drain-lease-throw-idle-thread';
    const resourceId = 'drain-lease-throw-idle-user';
    let finishRun!: () => void;
    const finished = new Promise<void>(resolve => {
      finishRun = resolve;
    });

    runtime.registerRun(
      agent,
      createFakeThreadRun(runId, finished),
      { memory: { thread: threadId, resource: resourceId } } as any,
      pubsub,
    );

    const result = runtime.sendMessage(agent, 'steer follow-up', { resourceId, threadId }, pubsub);
    await expect(result.accepted).resolves.toMatchObject({ action: 'deliver', runId });
    // Queue an idle message while the run is active so the failure path's
    // handoff has idle work to consider. The idle drain reports work on its
    // lease-lost branch without starting a local run, so a release gated on
    // the handoff outcome would be skipped here and the finished run would
    // hold the lease forever.
    const queued = runtime.queueMessage(agent, 'idle follow-up', { resourceId, threadId }, pubsub);
    await expect(queued.accepted).resolves.toMatchObject({ action: 'deliver' });

    finishRun();
    await waitForCondition(() =>
      pubsub.publishedData.some(
        data => data?.type === 'run-failed' && String(data?.error).includes('failed to start follow-up run'),
      ),
    );

    // Exact-token handoff proof: the reconciliation hands the verified
    // predecessor holder to the queued idle successor token-to-token (the
    // second, unmocked transfer), which releases its own token when it
    // finishes. The never-committed attempt token is neither released nor
    // reacquired, and no raw run id is ever used as an owner.
    const key = `${resourceId}\u0000${threadId}`;
    const predecessorToken = acquireSpy.mock.calls[0]?.[1];
    const failedAttemptToken = transferSpy.mock.calls[0]?.[2];
    expect(predecessorToken).toBeTruthy();
    expect(failedAttemptToken).toBeTruthy();
    expect(failedAttemptToken).not.toBe(predecessorToken);
    expect(acquireSpy.mock.calls.some(call => call[1] === failedAttemptToken)).toBe(false);
    expect(releaseSpy.mock.calls.some(call => call[1] === failedAttemptToken)).toBe(false);
    await waitForCondition(() => streamMock.mock.calls.length === 1);
    expect(JSON.stringify(streamMock.mock.calls[0]?.[0])).toContain('idle follow-up');
    // The finished run must not own the lease, no matter what the handoff did.
    await waitForCondition(() => pubsub.owners.get(key) === undefined);
  });

  it('reconciles the committed owner when a transfer rejects after committing', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new ControlledLeasePubSub();
    const acquireSpy = vi.spyOn(pubsub, 'acquireLease');
    const releaseSpy = vi.spyOn(pubsub, 'releaseLease');
    // Commit-then-reject: the provider moves the key to the failed attempt's
    // token and only then rejects. A failed operation is not proof that
    // nothing committed, so the catch must reconcile the captured tokens
    // against one bounded owner read instead of assuming a rollback.
    const transferSpy = vi
      .spyOn(pubsub, 'transferLease')
      .mockImplementationOnce(async (key: string, _fromOwner: string, toOwner: string) => {
        pubsub.owners.set(key, toOwner);
        throw new Error('lease backend down after commit');
      });
    const streamMock = vi.fn().mockResolvedValue({} as any);
    const agent = {
      id: 'drain-commit-reject-agent',
      stream: streamMock,
    } as unknown as Agent<any, any, any, any>;
    const runId = 'drain-commit-reject-run';
    const threadId = 'drain-commit-reject-thread';
    const resourceId = 'drain-commit-reject-user';
    const key = `${resourceId}\u0000${threadId}`;
    let finishRun!: () => void;
    const finished = new Promise<void>(resolve => {
      finishRun = resolve;
    });

    runtime.registerRun(
      agent,
      createFakeThreadRun(runId, finished),
      { memory: { thread: threadId, resource: resourceId } } as any,
      pubsub,
    );

    const result = runtime.sendMessage(agent, 'steer follow-up', { resourceId, threadId }, pubsub);
    await expect(result.accepted).resolves.toMatchObject({ action: 'deliver', runId });

    finishRun();
    await waitForCondition(() =>
      pubsub.publishedData.some(
        data => data?.type === 'run-failed' && String(data?.error).includes('failed to start follow-up run'),
      ),
    );
    expect(streamMock).not.toHaveBeenCalled();

    // The committed attempt token is the proven holder after reconciliation:
    // the catch releases exactly that token — never the predecessor's, which
    // no longer owns the key — after awaiting the handoff settlement.
    const predecessorToken = acquireSpy.mock.calls[0]?.[1];
    const committedToken = transferSpy.mock.calls[0]?.[2];
    expect(predecessorToken).toBeTruthy();
    expect(committedToken).toBeTruthy();
    expect(committedToken).not.toBe(predecessorToken);
    expect(acquireSpy.mock.calls.some(call => call[1] === committedToken)).toBe(false);
    expect(releaseSpy.mock.calls.some(call => call[1] === predecessorToken)).toBe(false);
    await waitForCondition(() => releaseSpy.mock.calls.some(call => call[1] === committedToken));
    await waitForCondition(() => pubsub.owners.get(key) === undefined);

    // The restored signal stays queued and still delivers on the next
    // natural drain trigger.
    let finishSecondRun!: () => void;
    const secondFinished = new Promise<void>(resolve => {
      finishSecondRun = resolve;
    });
    runtime.registerRun(
      agent,
      createFakeThreadRun('drain-commit-reject-second', secondFinished),
      { memory: { thread: threadId, resource: resourceId } } as any,
      pubsub,
    );
    finishSecondRun();
    await waitForCondition(() => streamMock.mock.calls.length === 1);
    expect(JSON.stringify(streamMock.mock.calls[0]?.[0])).toContain('steer follow-up');
  });

  it('releases the reconciled previous-run token when a rejected transfer did not commit', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new ControlledLeasePubSub();
    const acquireSpy = vi.spyOn(pubsub, 'acquireLease');
    const releaseSpy = vi.spyOn(pubsub, 'releaseLease');
    // Rejection-before-commit with a readable follow-up owner read: the
    // reconciliation proves nothing committed, so the predecessor's captured
    // token remains the proven holder.
    const transferSpy = vi.spyOn(pubsub, 'transferLease').mockImplementationOnce(async () => {
      throw new Error('lease backend down');
    });
    const streamMock = vi.fn().mockResolvedValue({} as any);
    const agent = {
      id: 'drain-reject-predecessor-agent',
      stream: streamMock,
    } as unknown as Agent<any, any, any, any>;
    const runId = 'drain-reject-predecessor-run';
    const threadId = 'drain-reject-predecessor-thread';
    const resourceId = 'drain-reject-predecessor-user';
    const key = `${resourceId}\u0000${threadId}`;
    let finishRun!: () => void;
    const finished = new Promise<void>(resolve => {
      finishRun = resolve;
    });

    runtime.registerRun(
      agent,
      createFakeThreadRun(runId, finished),
      { memory: { thread: threadId, resource: resourceId } } as any,
      pubsub,
    );

    const result = runtime.sendMessage(agent, 'steer follow-up', { resourceId, threadId }, pubsub);
    await expect(result.accepted).resolves.toMatchObject({ action: 'deliver', runId });

    finishRun();
    await waitForCondition(() =>
      pubsub.publishedData.some(
        data => data?.type === 'run-failed' && String(data?.error).includes('failed to start follow-up run'),
      ),
    );
    expect(streamMock).not.toHaveBeenCalled();

    // Verified-holder proof by exact token: the reconciled predecessor token
    // is released exactly once, and the never-committed attempt token is
    // neither released nor reacquired.
    const predecessorToken = acquireSpy.mock.calls[0]?.[1];
    const failedAttemptToken = transferSpy.mock.calls[0]?.[2];
    expect(predecessorToken).toBeTruthy();
    expect(failedAttemptToken).toBeTruthy();
    expect(failedAttemptToken).not.toBe(predecessorToken);
    expect(acquireSpy.mock.calls.some(call => call[1] === failedAttemptToken)).toBe(false);
    expect(releaseSpy.mock.calls.some(call => call[1] === failedAttemptToken)).toBe(false);
    await waitForCondition(() => releaseSpy.mock.calls.some(call => call[1] === predecessorToken));
    await waitForCondition(() => pubsub.owners.get(key) === undefined);

    // The restored signal still delivers on the next natural drain trigger.
    let finishSecondRun!: () => void;
    const secondFinished = new Promise<void>(resolve => {
      finishSecondRun = resolve;
    });
    runtime.registerRun(
      agent,
      createFakeThreadRun('drain-reject-predecessor-second', secondFinished),
      { memory: { thread: threadId, resource: resourceId } } as any,
      pubsub,
    );
    finishSecondRun();
    await waitForCondition(() => streamMock.mock.calls.length === 1);
    expect(JSON.stringify(streamMock.mock.calls[0]?.[0])).toContain('steer follow-up');
  });

  it('fails closed without releasing when a rejected transfer leaves ownership unreadable', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new ControlledLeasePubSub();
    const acquireSpy = vi.spyOn(pubsub, 'acquireLease');
    const releaseSpy = vi.spyOn(pubsub, 'releaseLease');
    // Rejection-before-commit with an unreadable follow-up owner read: the
    // provider neither confirms nor denies who owns the key, so no boolean
    // result may drive a release or a reacquisition.
    vi.spyOn(pubsub, 'transferLease').mockImplementationOnce(async () => {
      throw new Error('lease backend down');
    });
    pubsub.ownerReadFailures = 1;
    const streamMock = vi.fn().mockResolvedValue({} as any);
    const agent = {
      id: 'drain-unreadable-agent',
      stream: streamMock,
    } as unknown as Agent<any, any, any, any>;
    const runId = 'drain-unreadable-run';
    const threadId = 'drain-unreadable-thread';
    const resourceId = 'drain-unreadable-user';
    const key = `${resourceId}\u0000${threadId}`;
    let finishRun!: () => void;
    const finished = new Promise<void>(resolve => {
      finishRun = resolve;
    });

    runtime.registerRun(
      agent,
      createFakeThreadRun(runId, finished),
      { memory: { thread: threadId, resource: resourceId } } as any,
      pubsub,
    );

    const result = runtime.sendMessage(agent, 'steer follow-up', { resourceId, threadId }, pubsub);
    await expect(result.accepted).resolves.toMatchObject({ action: 'deliver', runId });

    finishRun();
    await waitForCondition(() =>
      pubsub.publishedData.some(
        data => data?.type === 'run-failed' && String(data?.error).includes('failed to start follow-up run'),
      ),
    );
    const failedRunId = pubsub.publishedData.find(
      data => data?.type === 'run-failed' && String(data?.error).includes('failed to start follow-up run'),
    )?.runId;
    expect(failedRunId).toBeTruthy();
    expect(failedRunId).not.toBe(runId);
    await nextTick();
    await nextTick();

    // Fail-closed invariants while ownership is unreadable: no release of
    // either captured token, no eager reacquisition, no forwarding, no
    // sibling dispatch, and no stream start. The predecessor keeps the key.
    const predecessorToken = acquireSpy.mock.calls[0]?.[1];
    expect(predecessorToken).toBeTruthy();
    expect(pubsub.owners.get(key)).toBe(predecessorToken);
    expect(releaseSpy).not.toHaveBeenCalled();
    expect(acquireSpy.mock.calls.length).toBe(1);
    // No forwarding beyond the original enqueue broadcast: exactly one
    // signal-enqueued (the sender's own), never one addressed to a foreign
    // owner the drain could not verify.
    expect(pubsub.publishedData.filter(data => data?.type === 'signal-enqueued').length).toBe(1);
    expect(streamMock).not.toHaveBeenCalled();
    // The infrastructure receipt is retained truthfully for the failed attempt.
    const receiptError = await runtime.waitForRunOutput(failedRunId as string, pubsub).then(
      () => undefined,
      (error: Error) => error,
    );
    expect(receiptError).toBeInstanceOf(Error);
    expect(receiptError?.message).toContain('lease backend down');

    // Recovery on a later natural trigger: once the unreachable owner's key
    // is freed (TTL expiry in production), the retained signal still delivers.
    pubsub.owners.delete(key);
    let finishSecondRun!: () => void;
    const secondFinished = new Promise<void>(resolve => {
      finishSecondRun = resolve;
    });
    runtime.registerRun(
      agent,
      createFakeThreadRun('drain-unreadable-second', secondFinished),
      { memory: { thread: threadId, resource: resourceId } } as any,
      pubsub,
    );
    finishSecondRun();
    await waitForCondition(() => streamMock.mock.calls.length === 1);
    expect(JSON.stringify(streamMock.mock.calls[0]?.[0])).toContain('steer follow-up');
  });

  it('retains a release failure without clobbering a successor installed during the release', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new ControlledLeasePubSub();
    const acquireSpy = vi.spyOn(pubsub, 'acquireLease');
    // The drain's verified release frees the key and then fails, with a
    // same-thread successor installed while the release is still in flight.
    let releaseGate!: () => void;
    const releaseGatePromise = new Promise<void>(resolve => {
      releaseGate = resolve;
    });
    let successorInstalled!: () => void;
    const successorInstalledPromise = new Promise<void>(resolve => {
      successorInstalled = resolve;
    });
    const releaseSpy = vi.spyOn(pubsub, 'releaseLease').mockImplementationOnce(async (key: string, _owner: string) => {
      await releaseGatePromise;
      pubsub.owners.delete(key);
      await successorInstalledPromise;
      throw new Error('release failed');
    });
    vi.spyOn(pubsub, 'transferLease').mockImplementationOnce(() => {
      throw new Error('lease backend down');
    });
    const streamMock = vi.fn().mockResolvedValue({} as any);
    const agent = {
      id: 'drain-release-fence-agent',
      stream: streamMock,
    } as unknown as Agent<any, any, any, any>;
    const runId = 'drain-release-fence-run';
    const threadId = 'drain-release-fence-thread';
    const resourceId = 'drain-release-fence-user';
    const key = `${resourceId}\u0000${threadId}`;
    let finishRun!: () => void;
    const finished = new Promise<void>(resolve => {
      finishRun = resolve;
    });

    runtime.registerRun(
      agent,
      createFakeThreadRun(runId, finished),
      { memory: { thread: threadId, resource: resourceId } } as any,
      pubsub,
    );

    const result = runtime.sendMessage(agent, 'steer follow-up', { resourceId, threadId }, pubsub);
    await expect(result.accepted).resolves.toMatchObject({ action: 'deliver', runId });

    finishRun();
    // The catch's verified release of the reconciled predecessor token is
    // now in flight (gated).
    await waitForCondition(() => releaseSpy.mock.calls.length === 1);
    releaseGate();
    // Install a same-thread successor while the release is still in flight:
    // it acquires the freed key under its own exact token.
    const successorFinished = new Promise<void>(() => {});
    runtime.registerRun(
      agent,
      createFakeThreadRun('drain-release-fence-successor', successorFinished),
      { memory: { thread: threadId, resource: resourceId } } as any,
      pubsub,
    );
    await waitForCondition(() => pubsub.owners.get(key) !== undefined);
    successorInstalled();

    // The release now fails: the failure is retained as the failed attempt's
    // infrastructure receipt instead of being swallowed — it supersedes the
    // failed attempt's earlier failed-startup receipt once the release itself
    // rejects.
    const failedRunId = pubsub.publishedData.find(
      data => data?.type === 'run-failed' && String(data?.error).includes('failed to start follow-up run'),
    )?.runId;
    expect(failedRunId).toBeTruthy();
    await vi.waitFor(async () => {
      await expect(runtime.waitForRunOutput(failedRunId as string, pubsub)).rejects.toThrow('release failed');
    });
    // ...and the successor's exact token, record, and active-thread identity
    // are untouched by the retired release: no raw run id, no map clobbering,
    // no timer removal.
    expect(decodeLeaseOwnerRunId(pubsub.owners.get(key))).toBe('drain-release-fence-successor');
    expect(runtime.hasThreadRun('drain-release-fence-successor', pubsub)).toBe(true);
    expect(runtime.getActiveThreadRunId({ resourceId, threadId }, pubsub)).toBe('drain-release-fence-successor');
    expect(acquireSpy.mock.calls.some(call => call[1] === runId)).toBe(false);
  });

  it('redelivers the restored signal exactly once on the next natural drain trigger', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const streamMock = vi
      .fn()
      .mockRejectedValueOnce(new Error('connection error: ECONNRESET'))
      .mockResolvedValue({} as any);
    const agent = {
      id: 'drain-redeliver-agent',
      stream: streamMock,
    } as unknown as Agent<any, any, any, any>;
    const threadId = 'drain-redeliver-thread';
    const resourceId = 'drain-redeliver-user';
    let finishFirst!: () => void;
    const firstFinished = new Promise<void>(resolve => {
      finishFirst = resolve;
    });

    runtime.registerRun(agent, createFakeThreadRun('drain-redeliver-run-1', firstFinished), {
      memory: { thread: threadId, resource: resourceId },
    } as any);

    const result = runtime.sendMessage(agent, 'steer follow-up', { resourceId, threadId });
    await expect(result.accepted).resolves.toMatchObject({ action: 'deliver', runId: 'drain-redeliver-run-1' });

    finishFirst();
    await waitForCondition(() => streamMock.mock.calls.length === 1);
    await nextTick();
    await nextTick();

    // The next natural trigger: another run on the same thread completing.
    let finishSecond!: () => void;
    const secondFinished = new Promise<void>(resolve => {
      finishSecond = resolve;
    });
    runtime.registerRun(agent, createFakeThreadRun('drain-redeliver-run-2', secondFinished), {
      memory: { thread: threadId, resource: resourceId },
    } as any);
    finishSecond();

    await waitForCondition(() => streamMock.mock.calls.length === 2);
    expect(streamMock.mock.calls[1]?.[0]).toMatchObject({ type: 'user', contents: 'steer follow-up' });

    await nextTick();
    await nextTick();
    expect(streamMock.mock.calls).toHaveLength(2);

    const secondRunId = streamMock.mock.calls[1]?.[1]?.runId;
    expect(secondRunId).toBeTruthy();
    expect(runtime.drainPendingSignals(secondRunId!)).toHaveLength(0);
  });

  it('hands the lease to a pending continuation instead of releasing it when the signal drain fails', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new ControlledLeasePubSub();
    const releaseSpy = vi.spyOn(pubsub, 'releaseLease');
    const transferSpy = vi.spyOn(pubsub, 'transferLease');
    const streamMock = vi
      .fn()
      .mockRejectedValueOnce(new Error('connection error: ECONNRESET'))
      .mockResolvedValue({} as any);
    const agent = {
      id: 'drain-continuation-agent',
      stream: streamMock,
    } as unknown as Agent<any, any, any, any>;
    const runId = 'drain-continuation-run';
    const threadId = 'drain-continuation-thread';
    const resourceId = 'drain-continuation-user';
    let finishRun!: () => void;
    const finished = new Promise<void>(resolve => {
      finishRun = resolve;
    });

    runtime.registerRun(
      agent,
      createFakeThreadRun(runId, finished),
      { memory: { thread: threadId, resource: resourceId } } as any,
      pubsub,
    );

    const result = runtime.sendMessage(agent, 'steer follow-up', { resourceId, threadId }, pubsub);
    await expect(result.accepted).resolves.toMatchObject({ action: 'deliver', runId });
    const continuation = runtime.continueWithMessages(agent, 'continuation work', { resourceId, threadId }, pubsub);
    expect(continuation.accepted).toBe(true);

    finishRun();
    // Call 1: the failed signal drain. Call 2: the continuation started by the
    // failure path's handoff.
    await waitForCondition(() => streamMock.mock.calls.length === 2);
    expect(streamMock.mock.calls[1]?.[0]).toBe('continuation work');
    expect(streamMock.mock.calls[1]?.[1]?.runId).toBe(continuation.runId);
    const transferCalls = transferSpy.mock.calls;
    const continuationLeaseOwner = transferCalls[transferCalls.length - 1]?.[2];
    expect(continuationLeaseOwner).toBeTruthy();
    await nextTick();
    await nextTick();

    // The lease was handed to the continuation, not released. The failure path
    // does release the finished previous run's token unconditionally (an
    // owner-guarded no-op here), so assert ownership rather than call count:
    // the continuation's process-attempt token must survive.
    expect(releaseSpy.mock.calls.some(call => call[1] === continuationLeaseOwner)).toBe(false);
    expect([...pubsub.owners.values()]).toContain(continuationLeaseOwner);

    // The failed steer signal is still queued for a later drain, untouched by
    // the continuation handoff.
    const restored = runtime.drainPendingSignals(continuation.runId, pubsub);
    expect(restored).toHaveLength(1);
    expect(restored[0]).toMatchObject({ contents: 'steer follow-up' });
  });

  describe('thread lease unknown-ownership settlement', () => {
    function createUnsettledRunFinished() {
      let finishRun!: () => void;
      const finished = new Promise<void>(resolve => {
        finishRun = resolve;
      });
      return { finishRun, finished };
    }

    it('does not dispatch an idle sibling or release when a continuation handoff transfer rejects', async () => {
      const runtime = new AgentThreadStreamRuntime();
      const pubsub = new ControlledLeasePubSub();
      const streamMock = vi.fn().mockResolvedValue({} as any);
      const agent = {
        id: 'unresolved-continuation-agent',
        stream: streamMock,
      } as unknown as Agent<any, any, any, any>;
      const resourceId = 'unresolved-continuation-user';
      const threadId = 'unresolved-continuation-thread';
      const { finishRun, finished } = createUnsettledRunFinished();
      runtime.registerRun(
        agent,
        createFakeThreadRun('unresolved-continuation-run', finished),
        { memory: { resource: resourceId, thread: threadId } } as any,
        pubsub,
      );
      const continuation = runtime.continueWithMessages(agent, 'continuation work', { resourceId, threadId }, pubsub);
      expect(continuation.accepted).toBe(true);
      const idle = runtime.queueMessage(agent, 'idle sibling', { resourceId, threadId }, pubsub);
      await expect(idle.accepted).resolves.toMatchObject({ action: 'deliver' });
      const acquireSpy = vi.spyOn(pubsub, 'acquireLease');
      const releaseSpy = vi.spyOn(pubsub, 'releaseLease');
      let releaseGate!: () => void;
      const gate = new Promise<void>(resolve => {
        releaseGate = resolve;
      });
      let transferring!: () => void;
      const transferStarted = new Promise<void>(resolve => {
        transferring = resolve;
      });
      const transferSpy = vi.spyOn(pubsub, 'transferLease').mockImplementationOnce(async () => {
        transferring();
        await gate;
        throw new Error('Lease unavailable');
      });
      try {
        finishRun();
        await transferStarted;
        releaseGate();
        await nextTick();
        await nextTick();
        await nextTick();
        // The continuation's lease operation failed with an unknown outcome:
        // no run started, no eager reacquisition, no idle sibling dispatch and
        // no release — the re-queued continuation and the retained lease wait
        // for the next natural trigger.
        expect(streamMock).not.toHaveBeenCalled();
        expect(acquireSpy).not.toHaveBeenCalled();
        expect(releaseSpy).not.toHaveBeenCalled();
        expect(transferSpy).toHaveBeenCalledTimes(1);
        expect(pubsub.owners.get(`${resourceId}\u0000${threadId}`)).toBeTruthy();
        expect(runtime.getActiveThreadRunId({ resourceId, threadId }, pubsub)).toBeUndefined();
      } finally {
        releaseGate();
      }
    });

    it('does not release from the continuation failure finally when its follow-up handoff transfer rejects', async () => {
      const runtime = new AgentThreadStreamRuntime();
      const pubsub = new ControlledLeasePubSub();
      const streamMock = vi
        .fn()
        .mockRejectedValueOnce(new Error('connection error: ECONNRESET'))
        .mockResolvedValue({} as any);
      const agent = {
        id: 'continuation-finally-agent',
        stream: streamMock,
      } as unknown as Agent<any, any, any, any>;
      const resourceId = 'continuation-finally-user';
      const threadId = 'continuation-finally-thread';
      const acquireSpy = vi.spyOn(pubsub, 'acquireLease');
      const releaseSpy = vi.spyOn(pubsub, 'releaseLease');
      let releaseGate!: () => void;
      const gate = new Promise<void>(resolve => {
        releaseGate = resolve;
      });
      let transferring!: () => void;
      const transferStarted = new Promise<void>(resolve => {
        transferring = resolve;
      });
      const transferSpy = vi.spyOn(pubsub, 'transferLease').mockImplementationOnce(async () => {
        transferring();
        await gate;
        throw new Error('Lease unavailable');
      });
      try {
        // The first continuation starts immediately; the second queues behind
        // its synchronous active-run reservation.
        runtime.continueWithMessages(agent, 'failing continuation', { resourceId, threadId }, pubsub);
        const queued = runtime.continueWithMessages(agent, 'queued continuation', { resourceId, threadId }, pubsub);
        await waitForCondition(() => streamMock.mock.calls.length === 1);
        // The failing continuation's recovery hands off to the queued one,
        // whose transfer rejects with an unknown outcome.
        await transferStarted;
        releaseGate();
        await nextTick();
        await nextTick();
        await nextTick();
        expect(streamMock).toHaveBeenCalledTimes(1);
        expect(streamMock.mock.calls[0]?.[0]).toBe('failing continuation');
        // The unresolved outcome reaches the failure path's finally: neither a
        // release nor an eager reacquisition may follow it.
        expect(releaseSpy).not.toHaveBeenCalled();
        expect(acquireSpy).not.toHaveBeenCalled();
        expect(transferSpy).toHaveBeenCalledTimes(1);
        expect(pubsub.owners.get(`${resourceId}\u0000${threadId}`)).toBeUndefined();
      } finally {
        releaseGate();
      }
    });

    it('does not recursively dispatch when a cancelled pending-signal transfer rejection leaves ownership unreadable', async () => {
      const runtime = new AgentThreadStreamRuntime();
      const pubsub = new ControlledLeasePubSub();
      const streamMock = vi.fn().mockResolvedValue({} as any);
      const agent = {
        id: 'cancelled-unreadable-agent',
        stream: streamMock,
      } as unknown as Agent<any, any, any, any>;
      const resourceId = 'cancelled-unreadable-user';
      const threadId = 'cancelled-unreadable-thread';
      const { finishRun, finished } = createUnsettledRunFinished();
      runtime.registerRun(
        agent,
        createFakeThreadRun('cancelled-unreadable-run', finished),
        { memory: { resource: resourceId, thread: threadId } } as any,
        pubsub,
      );
      const pending = runtime.sendMessage(agent, 'cancelled during lease', { resourceId, threadId }, pubsub);
      await expect(pending.accepted).resolves.toMatchObject({ action: 'deliver' });
      const idle = runtime.queueMessage(agent, 'idle sibling', { resourceId, threadId }, pubsub);
      await expect(idle.accepted).resolves.toMatchObject({ action: 'deliver' });
      const acquireSpy = vi.spyOn(pubsub, 'acquireLease');
      const releaseSpy = vi.spyOn(pubsub, 'releaseLease');
      let releaseGate!: () => void;
      const gate = new Promise<void>(resolve => {
        releaseGate = resolve;
      });
      let transferring!: () => void;
      const transferStarted = new Promise<void>(resolve => {
        transferring = resolve;
      });
      vi.spyOn(pubsub, 'transferLease').mockImplementationOnce(async () => {
        transferring();
        await gate;
        throw new Error('Lease unavailable');
      });
      try {
        finishRun();
        await transferStarted;
        // Cancel the in-flight drained signal while its transfer is gated.
        expect(
          runtime.cancelQueuedMessages(agent, { resourceId, threadId, signalIds: [pending.signal.id] }, pubsub),
        ).toEqual({ cancelledSignalIds: [pending.signal.id] });
        // The post-failure reconciliation read fails: ownership is unreadable.
        pubsub.ownerReadFailures = 1;
        releaseGate();
        await nextTick();
        await nextTick();
        await nextTick();
        // Fail closed: the cancelled item's terminal settled, but no recursive
        // drain dispatched the surviving idle sibling, forwarded anything, or
        // released/acquired the lease on the unresolved operation's strength.
        expect(streamMock).not.toHaveBeenCalled();
        expect(acquireSpy).not.toHaveBeenCalled();
        expect(releaseSpy).not.toHaveBeenCalled();
        expect(
          pubsub.publishedData.filter(data => data.type === 'signal-enqueued' && data.signal.id === idle.signal.id),
        ).toEqual([]);
        expect(
          pubsub.publishedData.some(
            data =>
              data.type === 'run-failed' &&
              data.runId !== 'cancelled-unreadable-run' &&
              data.error?.includes('cancelled'),
          ),
        ).toBe(true);
        expect(pubsub.owners.get(`${resourceId}\u0000${threadId}`)).toBeTruthy();
      } finally {
        releaseGate();
      }
    });

    it('settles the original idle item once when its lease handoff rejects with an unknown outcome', async () => {
      const runtime = new AgentThreadStreamRuntime();
      const pubsub = new ControlledLeasePubSub();
      const streamMock = vi.fn().mockResolvedValue({} as any);
      const agent = {
        id: 'idle-settle-agent',
        stream: streamMock,
      } as unknown as Agent<any, any, any, any>;
      const resourceId = 'idle-settle-user';
      const threadId = 'idle-settle-thread';
      const { finishRun, finished } = createUnsettledRunFinished();
      runtime.registerRun(
        agent,
        createFakeThreadRun('idle-settle-run', finished),
        { memory: { resource: resourceId, thread: threadId } } as any,
        pubsub,
      );
      let rejections = 0;
      const idleRunId = 'idle-settle-item';
      // A different agent's idle wake cannot join the active run, so it queues
      // as an idle item carrying its own rejection callback and run id.
      const idleAgent = {
        id: 'idle-settle-wake-agent',
        stream: streamMock,
      } as unknown as Agent<any, any, any, any>;
      const idle = runtime.sendSignal(
        idleAgent,
        { type: 'user-message', contents: 'idle original' },
        {
          resourceId,
          threadId,
          runId: idleRunId,
          ifIdle: {
            behavior: 'wake',
            _onThreadStreamRunRejected: () => {
              rejections += 1;
            },
          },
        },
        pubsub,
      );
      await expect(idle.accepted).resolves.toMatchObject({ action: 'deliver', runId: idleRunId });
      // The idle result's own output promise is one of the item's waiters.
      void idle.output?.catch(() => {});
      // Preinstall an output waiter for the original item BEFORE the failure.
      // Handlers attach at creation so the later rejection is never unhandled.
      const outputWait = runtime.waitForRunOutput(idleRunId, pubsub).then(
        () => 'resolved',
        () => 'rejected',
      );
      let releaseGate!: () => void;
      const gate = new Promise<void>(resolve => {
        releaseGate = resolve;
      });
      let transferring!: () => void;
      const transferStarted = new Promise<void>(resolve => {
        transferring = resolve;
      });
      vi.spyOn(pubsub, 'transferLease').mockImplementationOnce(async () => {
        transferring();
        await gate;
        throw new Error('Lease unavailable');
      });
      try {
        finishRun();
        await transferStarted;
        releaseGate();
        await nextTick();
        await nextTick();
        await nextTick();
        // The captured original item settled exactly once: its rejection
        // callback was consumed and its preinstalled output waiter rejected,
        // instead of the item silently disappearing with its waiters pending.
        expect(rejections).toBe(1);
        expect(await outputWait).toBe('rejected');
        expect(streamMock).not.toHaveBeenCalled();
        // Ownership was reconciled against the captured provenance (the
        // transfer did not commit, so the finished run still verifiably holds
        // the key): the verified holder is released once nothing is left.
        await vi.waitFor(() => expect(pubsub.owners.get(`${resourceId}\u0000${threadId}`)).toBeUndefined());
      } finally {
        releaseGate();
      }
    });

    it('does not revoke a same-run successor that adopts the retained token during failure recovery', async () => {
      class GatedRunFailedPubSub extends ControlledLeasePubSub {
        held = Promise.withResolvers<void>();
        entered = false;
        override async publish(topic: string, event: any): Promise<void> {
          // ControlledLeasePubSub.publish receives the terminal payload
          // directly, so gate on the payload's own type — and record that the
          // drain actually reached (and is blocked at) this publication.
          if (event?.type === 'run-failed') {
            this.entered = true;
            await this.held.promise;
          }
          await super.publish(topic, event);
        }
      }
      const runtime = new AgentThreadStreamRuntime();
      const pubsub = new GatedRunFailedPubSub();
      const streamMock = vi.fn().mockResolvedValue({} as any);
      const agent = {
        id: 'adopt-fence-agent',
        stream: streamMock,
      } as unknown as Agent<any, any, any, any>;
      const resourceId = 'adopt-fence-user';
      const threadId = 'adopt-fence-thread';
      const predecessorRunId = 'adopt-fence-run';
      const { finishRun, finished } = createUnsettledRunFinished();
      runtime.registerRun(
        agent,
        createFakeThreadRun(predecessorRunId, finished),
        { memory: { resource: resourceId, thread: threadId } } as any,
        pubsub,
      );
      const pending = runtime.sendMessage(agent, 'steer follow-up', { resourceId, threadId }, pubsub);
      await expect(pending.accepted).resolves.toMatchObject({ action: 'deliver' });
      const releaseSpy = vi.spyOn(pubsub, 'releaseLease');
      const renewSpy = vi.spyOn(pubsub, 'renewLease');
      vi.spyOn(pubsub, 'transferLease').mockImplementationOnce(async () => {
        throw new Error('Lease unavailable');
      });
      try {
        finishRun();
        // The drain must be observed BLOCKED at its run-failed publication
        // before the successor adopts: the barrier is the proof that adoption
        // happens while the failed attempt's cleanup is still outstanding.
        await vi.waitFor(() => expect(pubsub.entered).toBe(true));
        expect(pubsub.publishedData.some(data => data?.type === 'run-failed')).toBe(false);
        // A same-run successor registers for the predecessor run id while the
        // terminal is gated, re-adopting its retained token.
        await runtime.registerRun(
          agent,
          createFakeThreadRun(predecessorRunId, new Promise<void>(() => {})),
          { memory: { resource: resourceId, thread: threadId } } as any,
          pubsub,
          { strict: true },
        );
        const retainedOwner = pubsub.owners.get(`${resourceId}\u0000${threadId}`);
        expect(retainedOwner).toBeTruthy();
        pubsub.held.resolve();
        await nextTick();
        await nextTick();
        await nextTick();
        // The reconciled predecessor token must not be released: the successor
        // adopted the same run id (reusing the retained token), so the release
        // is fenced on the replaced record and renewal-timer identities even
        // though the token itself is unchanged.
        expect(releaseSpy.mock.calls.some(call => call[1] === retainedOwner)).toBe(false);
        expect(pubsub.owners.get(`${resourceId}\u0000${threadId}`)).toBe(retainedOwner);
        expect(runtime.getActiveThreadRunId({ resourceId, threadId }, pubsub)).toBe(predecessorRunId);
        // Renewal preservation: the successor's renewal interval survived the
        // fenced release decision and keeps renewing under the exact retained
        // token (default interval: lease TTL / 3 = 5s).
        await vi.waitFor(
          () =>
            expect(
              renewSpy.mock.calls.some(
                call => call[0] === `${resourceId}\u0000${threadId}` && call[1] === retainedOwner,
              ),
            ).toBe(true),
          { timeout: 7_000 },
        );
      } finally {
        pubsub.held.resolve();
      }
    });

    it.each(
      (['acquired', 'lost', 'error'] as const).flatMap(outcome =>
        (['reserving', 'non-reserving'] as const).map(reservation => [outcome, reservation] as const),
      ),
    )(
      'settles a cancelled idle wake exactly once when its lease handoff %s (%s admission)',
      async (outcome, reservation) => {
        const runtime = new AgentThreadStreamRuntime();
        const pubsub = new ControlledLeasePubSub();
        const streamMock = vi.fn().mockResolvedValue({} as any);
        const agent = {
          id: 'cancel-idle-matrix-agent',
          stream: streamMock,
        } as unknown as Agent<any, any, any, any>;
        // A different agent's idle wake cannot join the active run, so it
        // queues as an idle item carrying its own rejection callback and id.
        const idleAgent = {
          id: 'cancel-idle-matrix-wake-agent',
          stream: streamMock,
        } as unknown as Agent<any, any, any, any>;
        const resourceId = 'cancel-idle-matrix-user';
        const threadId = `cancel-idle-matrix-${outcome}-${reservation}`;
        const key = `${resourceId}\u0000${threadId}`;
        const { finishRun, finished } = createUnsettledRunFinished();
        runtime.registerRun(
          agent,
          createFakeThreadRun('cancel-idle-matrix-run', finished),
          { memory: { resource: resourceId, thread: threadId } } as any,
          pubsub,
        );
        let rejections = 0;
        const cancelledRunId = 'cancel-idle-matrix-item';
        const cancelled = runtime.sendSignal(
          idleAgent,
          { type: 'user-message', contents: 'cancelled idle input' },
          {
            resourceId,
            threadId,
            runId: cancelledRunId,
            ifIdle: {
              behavior: 'wake',
              _onThreadStreamRunRejected: () => {
                rejections += 1;
              },
              ...(reservation === 'non-reserving'
                ? {
                    _skipThreadRunReservationBeforePreflight: true,
                    streamOptions: { memory: { resource: resourceId, thread: threadId } },
                  }
                : {}),
            },
          },
          pubsub,
        );
        await expect(cancelled.accepted).resolves.toMatchObject({ action: 'deliver', runId: cancelledRunId });
        // The idle result's own output promise is one of the item's waiters.
        void cancelled.output?.catch(() => {});
        // A surviving sibling stays queued behind the cancelled item.
        const sibling = runtime.queueMessage(agent, 'sibling idle input', { resourceId, threadId }, pubsub);
        await expect(sibling.accepted).resolves.toMatchObject({ action: 'deliver' });
        const queueCounts: number[] = [];
        const unsubscribeEvents = runtime.subscribeThreadEvents(
          agent,
          { resourceId, threadId },
          event => {
            if (event.type === 'queue-count-changed') queueCounts.push(event.count);
          },
          pubsub,
        );
        const acquireSpy = vi.spyOn(pubsub, 'acquireLease');
        let releaseGate!: () => void;
        const gate = new Promise<void>(resolve => {
          releaseGate = resolve;
        });
        let transferring!: () => void;
        const transferStarted = new Promise<void>(resolve => {
          transferring = resolve;
        });
        try {
          if (outcome === 'error') {
            vi.spyOn(pubsub, 'transferLease').mockImplementationOnce(async () => {
              transferring();
              await gate;
              throw new Error('Lease unavailable');
            });
          } else {
            pubsub.transferLeaseWait = gate;
            pubsub.onTransferLease = transferring;
          }
          finishRun();
          await transferStarted;
          // Preinstall waiters while the captured item's lease operation is
          // still outstanding. An output waiter parks in both admission
          // modes; the cross-agent reservation API can only park on a
          // reserving run id — a non-reserving inflight idle id is invisible
          // to it (asserted per mode below).
          const outputWait = runtime.waitForRunOutput(cancelledRunId, pubsub).then(
            () => 'resolved',
            (error: Error) => error.message,
          );
          let reservationSettled = false;
          const reservationWait = runtime
            .waitForCrossAgentThreadRun(
              { id: 'cancel-idle-matrix-other-agent' } as unknown as Agent<any, any, any, any>,
              { memory: { resource: resourceId, thread: threadId } },
              pubsub,
            )
            .then(() => {
              reservationSettled = true;
            });
          // Cancellation arrives after the original item was captured: the
          // drain that captured it owns the once-only settlement.
          expect(
            runtime.cancelQueuedMessages(agent, { resourceId, threadId, signalIds: [cancelled.signal.id] }, pubsub),
          ).toEqual({ cancelledSignalIds: [cancelled.signal.id] });
          if (outcome === 'lost') pubsub.owners.set(key, 'new-owner');
          releaseGate();
          await vi.waitFor(() => expect(rejections).toBe(1));
          // The preinstalled output waiter settled through the settlement,
          // not by a competing drain or a dropped item.
          expect(await outputWait).toContain('was rejected');
          if (reservation === 'reserving') {
            // The parked reservation waiter was released by the settlement.
            await vi.waitFor(() => expect(reservationSettled).toBe(true));
          } else {
            // Documented accurately: a non-reserving inflight idle id never
            // holds the thread's active-run id, so the reservation API
            // resolves immediately — this mode's settlement proof is the
            // output waiter plus the once-only callback above.
            await reservationWait;
            expect(reservationSettled).toBe(true);
          }
          // The cancelled input was never run and never forwarded.
          expect(streamMock.mock.calls.some(call => (call[0] as any)?.contents === 'cancelled idle input')).toBe(false);
          expect(
            pubsub.publishedData.filter(
              data => data.type === 'signal-enqueued' && data.signal.id === cancelled.signal.id,
            ),
          ).toEqual([]);
          await vi.waitFor(() =>
            expect(runtime.getActiveThreadRunId({ resourceId, threadId }, pubsub)).toBeUndefined(),
          );
          if (outcome === 'acquired') {
            // The verified holder (the cancelled attempt's own token) handed
            // the lease to the surviving sibling, which delivered its input.
            await vi.waitFor(() =>
              expect(streamMock.mock.calls.some(call => (call[0] as any)?.contents === 'sibling idle input')).toBe(
                true,
              ),
            );
            expect(
              pubsub.publishedData.filter(
                data => data.type === 'signal-enqueued' && data.signal.id === sibling.signal.id,
              ),
            ).toEqual([]);
            await vi.waitFor(() => expect(pubsub.owners.get(key)).toBeUndefined());
          } else if (outcome === 'lost') {
            // The verified foreign owner kept the lease; the surviving sibling
            // was handed to it instead of being dropped or run locally.
            expect(streamMock).not.toHaveBeenCalled();
            await vi.waitFor(() =>
              expect(
                pubsub.publishedData.some(
                  data =>
                    data.type === 'signal-enqueued' &&
                    data.signal.id === sibling.signal.id &&
                    data.runId === 'new-owner',
                ),
              ).toBe(true),
            );
            expect(pubsub.owners.get(key)).toBe('new-owner');
          } else {
            // Unknown outcome: fail closed. No eager acquisition, no release,
            // no forwarding and no sibling dispatch — the sibling input stays
            // queued for the next natural trigger, and the truthful cause is
            // retained as the cancelled attempt's rejected-run receipt.
            expect(acquireSpy).not.toHaveBeenCalled();
            expect(streamMock).not.toHaveBeenCalled();
            expect(
              pubsub.publishedData.filter(
                data => data.type === 'signal-enqueued' && data.signal.id === sibling.signal.id,
              ),
            ).toEqual([]);
            await expect(runtime.waitForRunOutput(cancelledRunId, pubsub)).rejects.toThrow('Lease unavailable');
            expect(queueCounts[queueCounts.length - 1]).toBe(1);
            // The abandoned captured renewal was stopped; the predecessor
            // keeps the lease until its TTL lapses instead of an unsafe
            // release.
            expect(pubsub.owners.get(key)).toBeTruthy();
          }
          await reservationWait;
        } finally {
          releaseGate();
          unsubscribeEvents();
        }
      },
    );

    it('treats a never-settling reconciliation owner read as unreadable after the internal deadline', async () => {
      let unsubscribeEvents: (() => void) | undefined;
      vi.useFakeTimers();
      try {
        const runtime = new AgentThreadStreamRuntime();
        const pubsub = new ControlledLeasePubSub();
        const streamMock = vi.fn().mockResolvedValue({} as any);
        const agent = {
          id: 'unreadable-deadline-agent',
          stream: streamMock,
        } as unknown as Agent<any, any, any, any>;
        const resourceId = 'unreadable-deadline-user';
        const threadId = 'unreadable-deadline-thread';
        const key = `${resourceId}\u0000${threadId}`;
        const { finishRun, finished } = createUnsettledRunFinished();
        runtime.registerRun(
          agent,
          createFakeThreadRun('unreadable-deadline-run', finished),
          { memory: { resource: resourceId, thread: threadId } } as any,
          pubsub,
        );
        const pending = runtime.sendMessage(agent, 'unreadable deadline input', { resourceId, threadId }, pubsub);
        await expect(pending.accepted).resolves.toMatchObject({ action: 'deliver' });
        const sibling = runtime.queueMessage(agent, 'unreadable deadline sibling', { resourceId, threadId }, pubsub);
        await expect(sibling.accepted).resolves.toMatchObject({ action: 'deliver' });
        const queueCounts: number[] = [];
        unsubscribeEvents = runtime.subscribeThreadEvents(
          agent,
          { resourceId, threadId },
          event => {
            if (event.type === 'queue-count-changed') queueCounts.push(event.count);
          },
          pubsub,
        );
        const acquireSpy = vi.spyOn(pubsub, 'acquireLease');
        const releaseSpy = vi.spyOn(pubsub, 'releaseLease');
        const renewSpy = vi.spyOn(pubsub, 'renewLease');
        vi.spyOn(pubsub, 'transferLease').mockImplementationOnce(async () => {
          throw new Error('Lease unavailable');
        });
        let ownerReads = 0;
        // The single exact-owner reconciliation read never settles.
        vi.spyOn(pubsub, 'getLeaseOwner').mockImplementation(() => {
          ownerReads += 1;
          return new Promise<string | undefined>(() => {});
        });
        finishRun();
        // Pump the fake clock through the terminal-publication cascade
        // (publish delivery is timer-driven here) until the failed drain's
        // reconciliation read starts and hangs.
        for (let i = 0; i < 100 && ownerReads === 0; i += 1) {
          await vi.advanceTimersByTimeAsync(1);
        }
        // Before the internal deadline the disposition stays pending: the
        // truthful failure terminal is already published, but nothing has
        // been acquired, released, forwarded or dispatched.
        expect(ownerReads).toBe(1);
        expect(
          pubsub.publishedData.some(
            data =>
              data.type === 'run-failed' &&
              data.error?.includes('Lease unavailable') &&
              data.error?.includes('requeued'),
          ),
        ).toBe(true);
        expect(acquireSpy).not.toHaveBeenCalled();
        expect(releaseSpy).not.toHaveBeenCalled();
        expect(streamMock).not.toHaveBeenCalled();
        expect(
          pubsub.publishedData.filter(
            data =>
              data.type === 'signal-enqueued' &&
              [pending.signal.id, sibling.signal.id].includes(data.signal.id) &&
              data.runId !== 'unreadable-deadline-run',
          ),
        ).toEqual([]);
        expect(pubsub.owners.get(key)).toBeTruthy();
        // The internal 5s reconciliation deadline fires (5s, the same
        // internal-deadline pattern as the owner-acceptance timeout): the
        // read settles as unreadable and the disposition completes
        // fail-closed — still no eager acquisition, release, forwarding or
        // sibling dispatch, and the retained lease outlives the stopped
        // renewal so the TTL can lapse instead.
        await vi.advanceTimersByTimeAsync(5_100);
        expect(acquireSpy).not.toHaveBeenCalled();
        expect(releaseSpy).not.toHaveBeenCalled();
        expect(streamMock).not.toHaveBeenCalled();
        expect(
          pubsub.publishedData.filter(
            data =>
              data.type === 'signal-enqueued' &&
              [pending.signal.id, sibling.signal.id].includes(data.signal.id) &&
              data.runId !== 'unreadable-deadline-run',
          ),
        ).toEqual([]);
        expect(pubsub.owners.get(key)).toBeTruthy();
        expect(runtime.getActiveThreadRunId({ resourceId, threadId }, pubsub)).toBeUndefined();
        expect(queueCounts).toEqual([1]);
        // Only the abandoned captured renewal was stopped: after the deadline
        // the predecessor's renewal interval no longer fires.
        const renewalsAtDeadline = renewSpy.mock.calls.length;
        await vi.advanceTimersByTimeAsync(20_000);
        expect(renewSpy.mock.calls.length).toBe(renewalsAtDeadline);
        // The failed attempt's infrastructure receipt retains the truthful
        // cause for the run id reported in its failure terminal.
        const failedRunId = pubsub.publishedData.find(data => data?.type === 'run-failed')?.runId as string;
        await expect(runtime.waitForRunOutput(failedRunId, pubsub)).rejects.toThrow('Lease unavailable');
      } finally {
        vi.useRealTimers();
        unsubscribeEvents?.();
      }
    });
  });

  it.each(['request_access', 'ask_user'])('keeps %s suspensions discoverable and blocks idle wake', async toolName => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    const agent = {
      id: `generic-suspended-${toolName}`,
      stream: vi.fn(),
    } as unknown as Agent<any, any, any, any>;
    const idleAgent = {
      id: `idle-agent-${toolName}`,
      stream: vi.fn(),
    } as unknown as Agent<any, any, any, any>;
    const runId = `generic-suspended-run-${toolName}`;
    const threadId = `generic-suspended-thread-${toolName}`;
    const resourceId = `generic-suspended-user-${toolName}`;
    const topic = `agent.thread-stream.${encodeURIComponent(`${resourceId}\u0000${threadId}`)}`;
    const events: any[] = [];
    let finishRun!: () => void;
    const finished = new Promise<void>(resolve => {
      finishRun = resolve;
    });
    await pubsub.subscribe(topic, event => events.push(event.data));
    const subscription = await runtime.subscribeToThread(agent, { threadId, resourceId }, pubsub);
    const iterator = subscription.stream[Symbol.asyncIterator]();

    try {
      runtime.registerRun(
        agent,
        {
          runId,
          status: 'suspended',
          fullStream: new ReadableStream({
            start(controller) {
              controller.enqueue({ type: 'start', runId });
              controller.enqueue({
                type: 'tool-call-suspended',
                runId,
                payload: { toolCallId: `tool-call-${toolName}`, toolName },
              });
              controller.close();
            },
          }),
          _waitUntilFinished: () => finished,
        } as any,
        { memory: { thread: threadId, resource: resourceId } } as any,
        pubsub,
      );

      await withTimeout(iterator.next(), 'Timed out waiting for generic suspended run start');
      await withTimeout(iterator.next(), 'Timed out waiting for generic suspended chunk');
      expect(runtime.getThreadState({ resourceId, threadId }, pubsub)).toBe('active');
      expect(runtime.getActiveThreadRunId({ resourceId, threadId }, pubsub)).toBe(runId);

      const queuedForSuspendedRun = runtime.sendMessage(
        agent,
        'Resume-adjacent input',
        { resourceId, threadId },
        pubsub,
      );
      await expect(queuedForSuspendedRun.accepted).resolves.toMatchObject({ action: 'deliver', runId });
      expect((agent as any).stream).not.toHaveBeenCalled();
      expect(runtime.drainPendingSignals(runId, pubsub)[0]).toMatchObject({
        type: 'user',
        contents: 'Resume-adjacent input',
      });

      finishRun();
      await waitForCondition(() => events.some(event => event?.type === 'run-suspended' && event.runId === runId));
      expect(runtime.getThreadState({ resourceId, threadId }, pubsub)).toBe('active');
      expect(runtime.getActiveThreadRunId({ resourceId, threadId }, pubsub)).toBe(runId);

      const idleWake = runtime.sendSignal(
        idleAgent,
        createSignal({ type: 'user-message', contents: 'Unrelated idle wake' }),
        { resourceId, threadId, ifIdle: { streamOptions: { memory: { resource: resourceId, thread: threadId } } } },
        pubsub,
      );
      await expect(idleWake.accepted).resolves.toMatchObject({ action: 'blocked', reason: 'thread-blocked', runId });
      expect((idleAgent as any).stream).not.toHaveBeenCalled();
      expect(runtime.getThreadState({ resourceId, threadId }, pubsub)).toBe('active');

      expect(runtime.abortThread({ resourceId, threadId }, pubsub)).toBe(true);
      await waitForCondition(() => events.some(event => event?.type === 'run-aborted' && event.runId === runId));
      // Fork (PF-4402): a parked run publishes its authenticated run-aborted
      // terminal before its generation-fenced local teardown completes.
      await waitForCondition(() => !runtime.hasThreadRun(runId, pubsub));
      expect(runtime.getActiveThreadRunId({ resourceId, threadId }, pubsub)).toBeUndefined();
      expect(runtime.getThreadState({ resourceId, threadId }, pubsub)).toBe('idle');
      expect(runtime.hasThreadRun(runId, pubsub)).toBe(false);
      expect(runtime.abortThread({ resourceId, threadId }, pubsub)).toBe(false);
    } finally {
      finishRun();
      subscription.unsubscribe();
    }
  });

  it('marks a caller-only approval-suspended run without any thread subscribers', async () => {
    // Regression: the local multicast drain detects the approval marker. With no
    // thread subscribers nothing pulls a subscriber stream, so the drain must be
    // kicked eagerly at registration — otherwise #markApprovalSuspendedFromPart
    // never runs and the finalizer wrongly treats the suspended run as completed,
    // clearing its record. Here there is NO subscribeToThread caller at all.
    const runtime = new AgentThreadStreamRuntime();
    const agent = { id: 'caller-only-approval-agent' } as Agent<any, any, any, any>;
    const threadId = 'caller-only-approval-thread';
    const resourceId = 'caller-only-approval-user';
    const runId = 'caller-only-approval-run';

    let finishSuspended!: () => void;
    const suspendedFinished = new Promise<void>(resolve => {
      finishSuspended = resolve;
    });
    const suspendedCompletion = runtime.registerRun(
      agent,
      {
        runId,
        status: 'suspended',
        fullStream: new ReadableStream({
          start(controller) {
            controller.enqueue({ type: 'start', runId });
            controller.enqueue({
              type: 'tool-call-approval',
              runId,
              payload: { toolCallId: 'tool-call-1', toolName: 'testTool' },
            });
            controller.close();
          },
        }),
        _waitUntilFinished: () => suspendedFinished,
      } as any,
      { memory: { thread: threadId, resource: resourceId } } as any,
    );

    // Drive and await the finalizer with no subscriber ever attached.
    finishSuspended();
    await withTimeout(
      suspendedCompletion ?? Promise.resolve(),
      'Timed out waiting for caller-only approval-suspended finalizer',
    );
    await nextTick();

    // The approval marker must have been set by the eager drain: the thread stays
    // blocked (active) and the run record is retained rather than finalized-as-
    // completed and deleted.
    expect(runtime.getThreadState({ resourceId, threadId })).toBe('active');
    expect(runtime.getRunOutput(runId)).toBeDefined();

    // A subsequent resume reuses the same runId and must re-register cleanly
    // (the retained record is dropped via #clearApprovalSuspendedRunForResume).
    let finishResumed!: () => void;
    const resumedFinished = new Promise<void>(resolve => {
      finishResumed = resolve;
    });
    const resumedCompletion = runtime.registerRun(
      agent,
      {
        runId,
        status: 'running',
        fullStream: new ReadableStream({
          start(controller) {
            controller.enqueue({ type: 'start', runId });
            controller.enqueue({ type: 'text-start', runId, payload: { id: 'text-1' } });
            controller.enqueue({
              type: 'text-delta',
              runId,
              payload: { id: 'text-1', text: 'approved response' },
            });
            controller.enqueue({ type: 'text-end', runId, payload: { id: 'text-1' } });
            controller.enqueue({
              type: 'finish',
              runId,
              payload: { usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 }, finishReason: 'stop' },
            });
            controller.close();
            finishResumed();
          },
        }),
        _waitUntilFinished: () => resumedFinished,
      } as any,
      { memory: { thread: threadId, resource: resourceId } } as any,
    );

    await withTimeout(resumedCompletion ?? Promise.resolve(), 'Timed out waiting for resumed run completion');
    await nextTick();

    // The resumed run completed normally: the thread is idle again and the record
    // is cleared.
    expect(runtime.getThreadState({ resourceId, threadId })).toBe('idle');
    expect(runtime.getRunOutput(runId)).toBeUndefined();
  });

  it('queues queueMessage until the active run completes', async () => {
    let releaseFirst!: () => void;
    const firstFinished = new Promise<void>(resolve => {
      releaseFirst = resolve;
    });
    let streamCount = 0;
    const prompts: any[][] = [];
    const model = new MockLanguageModelV2({
      doStream: async ({ prompt }) => {
        streamCount += 1;
        prompts.push(prompt);
        const responseText = streamCount === 1 ? 'first response' : 'queued response';
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: new ReadableStream({
            async start(controller) {
              controller.enqueue({ type: 'stream-start', warnings: [] });
              controller.enqueue({
                type: 'response-metadata',
                id: `queue-message-${streamCount}`,
                modelId: 'mock-model-id',
                timestamp: new Date(0),
              });
              controller.enqueue({ type: 'text-start', id: 'text-1' });
              controller.enqueue({ type: 'text-delta', id: 'text-1', delta: responseText });
              controller.enqueue({ type: 'text-end', id: 'text-1' });
              if (streamCount === 1) {
                await firstFinished;
              }
              controller.enqueue({
                type: 'finish',
                finishReason: 'stop',
                usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
              });
              controller.close();
            },
          }),
        };
      },
    });
    const agent = new Agent({ id: 'queue-message-agent', name: 'Queue Message Agent', instructions: 'Test', model });
    const subscription = await agent.subscribeToThread({
      threadId: 'queue-message-thread',
      resourceId: 'queue-message-user',
    });
    const iterator = subscription.stream[Symbol.asyncIterator]();
    const firstRun = readNextRunWithParts(iterator);

    const stream = await agent.stream('Hello', {
      memory: { thread: 'queue-message-thread', resource: 'queue-message-user' },
    });
    await expect(waitForActiveRun(subscription)).resolves.toBe(stream.runId);
    const result = agent.queueMessage('Queued follow-up', {
      resourceId: 'queue-message-user',
      threadId: 'queue-message-thread',
    });

    const settled = await result.accepted;
    const queuedRunId = 'runId' in settled ? settled.runId : undefined;
    expect(settled.action).toBe('deliver');
    expect(queuedRunId).not.toBe(stream.runId);
    await nextTick();
    expect(streamCount).toBe(1);

    releaseFirst();
    await expect(stream.text).resolves.toBe('first response');
    await firstRun;
    const secondRun = await readNextRunWithParts(iterator);
    expect(secondRun.value.runId).toBe(queuedRunId);
    expect(secondRun.value.text).toBe('queued response');
    expect(
      agent.cancelQueuedMessages({
        resourceId: 'queue-message-user',
        threadId: 'queue-message-thread',
        signalIds: [result.signal.id],
      }),
    ).toEqual({ cancelledSignalIds: [] });
    expect(streamCount).toBe(2);
    expect(JSON.stringify(prompts[1])).toContain('Queued follow-up');

    subscription.unsubscribe();
  });

  it('observes only relevant owner-scoped queue changes and validates cancellation selectors', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new ControlledLeasePubSub();
    const resourceId = 'observed-queue-resource';
    const threadId = 'observed-queue-thread';
    const ownerId = 'observed-owner';
    const activeRunId = 'observed-active-run';
    const neverFinished = new Promise<void>(() => {});
    const agent = { id: 'observed-agent', stream: vi.fn() } as unknown as Agent<any, any, any, any>;
    const otherAgent = { id: 'other-observed-agent', stream: vi.fn() } as unknown as Agent<any, any, any, any>;

    runtime.registerRun(
      agent,
      createFakeThreadRun(activeRunId, neverFinished),
      { memory: { resource: resourceId, thread: threadId } } as any,
      pubsub,
    );

    const events: Array<{ type: 'queue-count-changed'; count: number }> = [];
    const counts: number[] = [];
    const unsubscribe = runtime.subscribeThreadEvents(
      agent,
      { resourceId, threadId, queueOwnerId: ownerId },
      event => {
        if (event.type === 'queue-count-changed') {
          events.push(event);
          counts.push(event.count);
        }
      },
      pubsub,
    );
    const sharedCounts: number[] = [];
    const unsubscribeShared = runtime.subscribeThreadEvents(
      otherAgent,
      { resourceId, threadId },
      event => {
        if (event.type === 'queue-count-changed') sharedCounts.push(event.count);
      },
      pubsub,
    );
    const owned = runtime.queueMessage(
      agent,
      'owned queued message',
      { resourceId, threadId, queueOwnerId: ownerId },
      pubsub,
    );
    runtime.registerRun(
      agent,
      createFakeThreadRun('other-resource-run', neverFinished),
      { memory: { resource: 'other-resource', thread: threadId } } as any,
      pubsub,
    );
    runtime.registerRun(
      agent,
      createFakeThreadRun('other-thread-run', neverFinished),
      { memory: { resource: resourceId, thread: 'other-thread' } } as any,
      pubsub,
    );
    runtime.queueMessage(
      agent,
      'other resource queued message',
      { resourceId: 'other-resource', threadId, queueOwnerId: ownerId },
      pubsub,
    );
    runtime.queueMessage(
      agent,
      'other thread queued message',
      { resourceId, threadId: 'other-thread', queueOwnerId: ownerId },
      pubsub,
    );
    const untagged = runtime.queueMessage(agent, 'untagged supervisor message', { resourceId, threadId }, pubsub);
    const other = runtime.queueMessage(
      otherAgent,
      'other agent queued message',
      { resourceId, threadId, queueOwnerId: ownerId },
      pubsub,
    );

    expect(counts).toEqual([0, 1]);
    expect(sharedCounts).toEqual([0, 1, 2, 3]);
    expect(runtime.cancelQueuedMessages(otherAgent, { resourceId, threadId, queueOwnerId: ownerId }, pubsub)).toEqual({
      cancelledSignalIds: [other.signal.id],
    });
    expect(counts).toEqual([0, 1]);
    expect(runtime.cancelQueuedMessages(agent, { resourceId, threadId, queueOwnerId: ownerId }, pubsub)).toEqual({
      cancelledSignalIds: [owned.signal.id],
    });
    expect(
      runtime.cancelQueuedMessages(otherAgent, { resourceId, threadId, signalIds: [untagged.signal.id] }, pubsub),
    ).toEqual({
      cancelledSignalIds: [untagged.signal.id],
    });
    expect(
      runtime.cancelQueuedMessages(agent, { resourceId, threadId, signalIds: [untagged.signal.id] }, pubsub),
    ).toEqual({
      cancelledSignalIds: [],
    });
    expect(counts).toEqual([0, 1, 0]);
    expect(events).toEqual([
      { type: 'queue-count-changed', count: 0 },
      { type: 'queue-count-changed', count: 1 },
      { type: 'queue-count-changed', count: 0 },
    ]);
    expect(() => runtime.cancelQueuedMessages(agent, { resourceId, threadId } as any, pubsub)).toThrow(
      'exactly one of signalIds or queueOwnerId',
    );

    expect(sharedCounts).toEqual([0, 1, 2, 3, 2, 1, 0]);
    unsubscribeShared();
    unsubscribeShared();
    unsubscribe();
    unsubscribe();
    runtime.queueMessage(agent, 'unobserved queued message', { resourceId, threadId, queueOwnerId: ownerId }, pubsub);
    expect(counts).toEqual([0, 1, 0]);
    expect(sharedCounts).toEqual([0, 1, 2, 3, 2, 1, 0]);
  });

  it('isolates listener errors and permits reentrant owner cancellation', () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new ControlledLeasePubSub();
    const resourceId = 'reentrant-queue-resource';
    const threadId = 'reentrant-queue-thread';
    const queueOwnerId = 'reentrant-owner';
    const activeRunId = 'reentrant-active-run';
    const neverFinished = new Promise<void>(() => {});
    const agent = { id: 'reentrant-agent', stream: vi.fn() } as unknown as Agent<any, any, any, any>;

    runtime.registerRun(
      agent,
      createFakeThreadRun(activeRunId, neverFinished),
      { memory: { resource: resourceId, thread: threadId } } as any,
      pubsub,
    );

    const counts: number[] = [];
    runtime.subscribeThreadEvents(
      agent,
      { resourceId, threadId, queueOwnerId },
      event => {
        if (event.type === 'queue-count-changed') {
          counts.push(event.count);
          if (event.count > 0) {
            runtime.cancelQueuedMessages(agent, { resourceId, threadId, queueOwnerId }, pubsub);
          }
        }
      },
      pubsub,
    );
    runtime.subscribeThreadEvents(
      agent,
      { resourceId, threadId, queueOwnerId },
      () => {
        throw new Error('listener failure must not interrupt the queue');
      },
      pubsub,
    );

    const queued = runtime.queueMessage(agent, 'cancel from listener', { resourceId, threadId, queueOwnerId }, pubsub);
    expect(counts).toEqual([0, 1, 0]);
    expect(
      runtime.cancelQueuedMessages(agent, { resourceId, threadId, signalIds: [queued.signal.id] }, pubsub),
    ).toEqual({
      cancelledSignalIds: [],
    });
  });

  it('keeps observed messages pending through lease handoff and drops them before stream registration', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new ControlledLeasePubSub();
    const handoff = Promise.withResolvers<void>();
    const resourceId = 'handoff-observation-resource';
    const threadId = 'handoff-observation-thread';
    const queueOwnerId = 'handoff-observation-owner';
    const activeRunId = 'handoff-observation-active-run';
    let finishActiveRun!: () => void;
    let handoffStarted = false;
    const activeRunFinished = new Promise<void>(resolve => {
      finishActiveRun = resolve;
    });
    const counts: number[] = [];
    const agent = {
      id: 'handoff-observation-agent',
      stream: vi.fn(async () => {
        expect(counts).toEqual([0, 1, 0]);
        return { runId: 'handoff-observation-next-run' };
      }),
    } as unknown as Agent<any, any, any, any>;
    pubsub.transferLeaseWait = handoff.promise;
    pubsub.onTransferLease = () => {
      handoffStarted = true;
    };
    pubsub.owners.set(`${resourceId}\u0000${threadId}`, activeRunId);

    runtime.registerRun(
      agent,
      createFakeThreadRun(activeRunId, activeRunFinished),
      { memory: { resource: resourceId, thread: threadId } } as any,
      pubsub,
    );
    runtime.subscribeThreadEvents(
      agent,
      { resourceId, threadId, queueOwnerId },
      event => {
        if (event.type === 'queue-count-changed') counts.push(event.count);
      },
      pubsub,
    );
    const queued = runtime.queueMessage(
      agent,
      'handoff observed message',
      { resourceId, threadId, queueOwnerId },
      pubsub,
    );

    finishActiveRun();
    await waitForCondition(() => handoffStarted);
    expect(counts).toEqual([0, 1]);
    handoff.resolve();
    await waitForCondition(() => (agent.stream as any).mock.calls.length === 1);
    expect(counts).toEqual([0, 1, 0]);
    expect(runtime.cancelQueuedMessages(agent, { resourceId, threadId, queueOwnerId }, pubsub)).toEqual({
      cancelledSignalIds: [],
    });
    expect(queued.signal.id).toBeDefined();
  });

  it('removes observed messages when a lease loss forwards them or execution fails', async () => {
    const resourceId = 'queue-terminal-resource';
    const threadId = 'queue-terminal-thread';
    const queueOwnerId = 'queue-terminal-owner';

    for (const outcome of ['forward', 'fail'] as const) {
      const runtime = new AgentThreadStreamRuntime();
      const pubsub = new ControlledLeasePubSub();
      const activeRunId = `queue-terminal-active-${outcome}`;
      let finishActiveRun!: () => void;
      const activeRunFinished = new Promise<void>(resolve => {
        finishActiveRun = resolve;
      });
      const stream = vi.fn().mockRejectedValue(new Error('queued stream failure'));
      const agent = { id: `queue-terminal-agent-${outcome}`, stream } as unknown as Agent<any, any, any, any>;
      const counts: number[] = [];
      pubsub.owners.set(`${resourceId}\u0000${threadId}`, activeRunId);
      if (outcome === 'forward') {
        pubsub.transferLeaseWait = new Promise<void>(resolve => {
          pubsub.onTransferLease = () => {
            pubsub.owners.set(`${resourceId}\u0000${threadId}`, 'remote-winner');
            resolve();
          };
        });
      }

      runtime.registerRun(
        agent,
        createFakeThreadRun(activeRunId, activeRunFinished),
        { memory: { resource: resourceId, thread: threadId } } as any,
        pubsub,
      );
      runtime.subscribeThreadEvents(
        agent,
        { resourceId, threadId, queueOwnerId },
        event => {
          if (event.type === 'queue-count-changed') counts.push(event.count);
        },
        pubsub,
      );
      const queued = runtime.queueMessage(agent, `queued ${outcome}`, { resourceId, threadId, queueOwnerId }, pubsub);
      finishActiveRun();

      await waitForCondition(() => counts.at(-1) === 0);
      expect(counts).toEqual([0, 1, 0]);
      if (outcome === 'forward') {
        expect(stream).not.toHaveBeenCalled();
        expect(pubsub.publishedData).toContainEqual(
          expect.objectContaining({
            type: 'signal-enqueued',
            signal: expect.objectContaining({ id: queued.signal.id }),
          }),
        );
      } else {
        expect(stream).toHaveBeenCalledTimes(1);
      }
    }
  });

  it('removes observed messages after lease acquisition failure', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new ControlledLeasePubSub();
    const resourceId = 'lease-failure-observation-resource';
    const threadId = 'lease-failure-observation-thread';
    const queueOwnerId = 'lease-failure-observation-owner';
    const activeRunId = 'lease-failure-observation-active-run';
    let finishActiveRun!: () => void;
    const activeRunFinished = new Promise<void>(resolve => {
      finishActiveRun = resolve;
    });
    const stream = vi.fn();
    const agent = { id: 'lease-failure-observation-agent', stream } as unknown as Agent<any, any, any, any>;
    const counts: number[] = [];
    pubsub.owners.set(`${resourceId}\u0000${threadId}`, activeRunId);
    vi.spyOn(pubsub, 'transferLease').mockRejectedValue(new Error('lease backend unavailable'));

    runtime.registerRun(
      agent,
      createFakeThreadRun(activeRunId, activeRunFinished),
      { memory: { resource: resourceId, thread: threadId } } as any,
      pubsub,
    );
    runtime.subscribeThreadEvents(
      agent,
      { resourceId, threadId, queueOwnerId },
      event => {
        if (event.type === 'queue-count-changed') counts.push(event.count);
      },
      pubsub,
    );
    runtime.queueMessage(agent, 'lease failure', { resourceId, threadId, queueOwnerId }, pubsub);
    finishActiveRun();

    await waitForCondition(() => counts.at(-1) === 0);
    expect(counts).toEqual([0, 1, 0]);
    expect(stream).not.toHaveBeenCalled();
  });

  it('does not forward a cancelled observed message after losing the lease', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new ControlledLeasePubSub();
    const resourceId = 'lease-loss-cancel-resource';
    const threadId = 'lease-loss-cancel-thread';
    const queueOwnerId = 'lease-loss-cancel-owner';
    const activeRunId = 'lease-loss-cancel-active-run';
    let finishActiveRun!: () => void;
    let transferStarted!: () => void;
    const activeRunFinished = new Promise<void>(resolve => {
      finishActiveRun = resolve;
    });
    const transferStartedPromise = new Promise<void>(resolve => {
      transferStarted = resolve;
    });
    let releaseTransfer!: () => void;
    const stream = vi.fn();
    const agent = { id: 'lease-loss-cancel-agent', stream } as unknown as Agent<any, any, any, any>;
    const counts: number[] = [];
    pubsub.owners.set(`${resourceId}\u0000${threadId}`, activeRunId);
    pubsub.transferLeaseWait = new Promise<void>(resolve => {
      releaseTransfer = resolve;
    });
    pubsub.onTransferLease = transferStarted;

    runtime.registerRun(
      agent,
      createFakeThreadRun(activeRunId, activeRunFinished),
      { memory: { resource: resourceId, thread: threadId } } as any,
      pubsub,
    );
    runtime.subscribeThreadEvents(
      agent,
      { resourceId, threadId, queueOwnerId },
      event => {
        if (event.type === 'queue-count-changed') counts.push(event.count);
      },
      pubsub,
    );
    const queued = runtime.queueMessage(
      agent,
      'cancel before lease loss',
      { resourceId, threadId, queueOwnerId },
      pubsub,
    );
    finishActiveRun();
    await transferStartedPromise;

    expect(runtime.cancelQueuedMessages(agent, { resourceId, threadId, queueOwnerId }, pubsub)).toEqual({
      cancelledSignalIds: [queued.signal.id],
    });
    pubsub.owners.set(`${resourceId}\u0000${threadId}`, 'remote-winner');
    releaseTransfer();
    await waitForCondition(() => runtime.getActiveThreadRunId({ resourceId, threadId }, pubsub) === undefined);
    expect(counts).toEqual([0, 1, 0]);
    expect(stream).not.toHaveBeenCalled();
    expect(pubsub.publishedData).not.toContainEqual(expect.objectContaining({ type: 'signal-enqueued' }));
  });

  it('cancels a queueMessage after dequeue but before the lease handoff starts execution', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new ControlledLeasePubSub();
    const handoff = Promise.withResolvers<void>();
    const stream = vi.fn().mockResolvedValue({ runId: 'should-not-start' });
    const agent = { id: 'cancel-prestart-agent', stream } as unknown as Agent<any, any, any, any>;
    const resourceId = 'cancel-prestart-resource';
    const threadId = 'cancel-prestart-thread';
    const activeRunId = 'cancel-prestart-active';
    let finishActiveRun!: () => void;
    let handoffStarted = false;
    const activeRunFinished = new Promise<void>(resolve => {
      finishActiveRun = resolve;
    });
    pubsub.transferLeaseWait = handoff.promise;
    pubsub.onTransferLease = () => {
      handoffStarted = true;
    };
    // A raw run id can never own a lease, so it is never seeded here.
    runtime.registerRun(
      agent,
      createFakeThreadRun(activeRunId, activeRunFinished),
      { memory: { resource: resourceId, thread: threadId } } as any,
      pubsub,
    );
    // Authentic lease ownership: the active run acquires the key under its
    // registered attempt token. A seeded raw run id can never own a lease,
    // so seeding it here made the registration lose to itself and the
    // handoff below never genuinely started. Wait for the registration's
    // own acquisition and prove the holder is the attempt token — with no
    // relaxed register semantics — before driving the cancellation.
    await waitForCondition(() => pubsub.owners.get(`${resourceId}\u0000${threadId}`) !== undefined);
    const heldOwner = pubsub.owners.get(`${resourceId}\u0000${threadId}`);
    expect(heldOwner).toMatch(/^mastra-thread-owner:/);
    expect(heldOwner).toContain(activeRunId);
    expect(heldOwner).not.toBe(activeRunId);
    const queued = runtime.queueMessage(agent, 'cancel before execution', { resourceId, threadId }, pubsub);
    await expect(queued.accepted).resolves.toMatchObject({ action: 'deliver' });

    finishActiveRun();
    await waitForCondition(() => handoffStarted);
    expect(
      runtime.cancelQueuedMessages(agent, { resourceId, threadId, signalIds: [queued.signal.id] }, pubsub),
    ).toEqual({
      cancelledSignalIds: [queued.signal.id],
    });
    handoff.resolve();
    await nextTick();
    await nextTick();

    expect(stream).not.toHaveBeenCalled();
  });

  it('fans out sequential idle signal runs to many same-thread subscribers', async () => {
    const resourceId = 'share-resource';
    const threadId = 'share-thread';
    const subscriberCount = 100;
    const runCount = 5;
    let streamCount = 0;
    const agent = new Agent({
      id: 'share-signal-agent',
      name: 'Share Signal Agent',
      instructions: 'Test',
      model: new MockLanguageModelV2({
        doStream: async () => {
          streamCount += 1;
          const text = `signal response ${streamCount}`;
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: new ReadableStream({
              async start(controller) {
                const parts = [
                  { type: 'stream-start', warnings: [] },
                  {
                    type: 'response-metadata',
                    id: `id-${streamCount}`,
                    modelId: 'mock-model-id',
                    timestamp: new Date(0),
                  },
                  { type: 'text-start', id: 'text-1' },
                  { type: 'text-delta', id: 'text-1', delta: text },
                  { type: 'text-end', id: 'text-1' },
                  {
                    type: 'finish',
                    finishReason: 'stop',
                    usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
                  },
                ] as any[];
                for (const part of parts) {
                  await nextTick();
                  controller.enqueue(part);
                }
                controller.close();
              },
            }),
          };
        },
      }),
    });

    const subscriptions = await Promise.all(
      Array.from({ length: subscriberCount }, () => agent.subscribeToThread({ threadId, resourceId })),
    );
    const iterators = subscriptions.map(subscription => subscription.stream[Symbol.asyncIterator]());

    try {
      for (let runIndex = 1; runIndex <= runCount; runIndex += 1) {
        const nextRuns = iterators.map(iterator => readNextRunWithParts(iterator));
        const contents = `Hello from signal ${runIndex}`;

        const signalResult = await agent.sendSignal(
          { type: 'user-message', contents },
          { resourceId, threadId, ifIdle: { streamOptions: { memory: { resource: resourceId, thread: threadId } } } },
        );

        const runs = await withTimeout(
          Promise.all(nextRuns),
          `Timed out waiting for ${subscriberCount} subscribers to receive idle signal run ${runIndex}`,
          5_000,
        );
        const [firstRun] = runs;

        await expect(signalResult.accepted).resolves.toMatchObject({ action: 'wake', runId: firstRun.value.runId });
        expect(firstRun.value.text).toBe(`signal response ${runIndex}`);

        for (const run of runs) {
          expect(run.value.runId).toBe(firstRun.value.runId);
          expect(run.value.text).toBe(`signal response ${runIndex}`);
          const signalPart = run.value.parts.find((part: any) => part.type === 'data-user-message');
          expect(signalPart?.data).toMatchObject({
            id: signalResult.signal.id,
            contents,
            acceptedAt: signalResult.signal.acceptedAt?.toISOString(),
          });
          expect(signalPart?.data.createdAt).toBeDefined();
          expect(signalPart?.transient).toBe(true);
        }
        await waitForCondition(() => subscriptions.every(subscription => subscription.activeRunId() === null));
      }

      expect(streamCount).toBe(runCount);
    } finally {
      for (const subscription of subscriptions) {
        subscription.unsubscribe();
      }
    }
  });

  it('starts an idle thread run by default when a thread-targeted signal is sent', async () => {
    const agent = new Agent({
      id: 'idle-signal-without-options-agent',
      name: 'Idle Signal Without Options Agent',
      instructions: 'Test',
      model: createTextStreamModel('signal response'),
    });

    const result = await agent.sendSignal(
      { type: 'user-message', contents: 'Hello from signal' },
      { resourceId: 'idle-user', threadId: 'idle-thread' },
    );

    await expect(result.accepted).resolves.toMatchObject({ action: 'wake' });
  });

  it('reports the reserved runId as active before registerRun populates the stream record', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const threadId = 'reservation-gap-thread';
    const resourceId = 'reservation-gap-user';

    // agent.stream is awaited inside the idle-wake path before registerRun fires. Returning
    // a never-resolving promise pins the runtime in the gap where sendSignal has reserved
    // activeThreadRunIds + threadKeysByRunId but threadRunsById is still empty.
    const agent = {
      id: 'reservation-gap-agent',
      stream: () => new Promise(() => {}),
    } as unknown as Agent<any, any, any, any>;

    const subscription = await runtime.subscribeToThread(agent, { threadId, resourceId });
    expect(subscription.activeRunId()).toBeNull();

    const result = runtime.sendSignal(agent, createSignal({ type: 'user-message', contents: 'hello' }), {
      resourceId,
      threadId,
      ifIdle: { streamOptions: { memory: { resource: resourceId, thread: threadId } } as any },
    });

    // accepted never settles here because agent.stream is pinned; the reserved runId is
    // observable via the subscription's active run id before registerRun populates the stream.
    expect(result.accepted).toBeInstanceOf(Promise);
    expect(subscription.activeRunId()).not.toBeNull();

    const queued = runtime.queueMessage(agent, 'separate queued message', { resourceId, threadId });
    await expect(queued.accepted).resolves.toMatchObject({ action: 'deliver' });
    expect(runtime.cancelQueuedMessages(agent, { resourceId, threadId, signalIds: [queued.signal.id] })).toEqual({
      cancelledSignalIds: [queued.signal.id],
    });

    subscription.unsubscribe();
  });

  it('lets a run start immediately after a persisted idle signal without starving the event loop', async () => {
    // Runs in a child process on purpose: the regression starves the macrotask queue, so an
    // in-process timeout would never fire. The parent bounds it with SIGKILL instead.
    const fixture = new URL('./fixtures/persisted-signal-immediate-run.ts', import.meta.url);
    const child = fork(fixture, { execArgv: ['--import', import.meta.resolve('tsx')], silent: true });
    const stderr: Buffer[] = [];
    child.stderr?.on('data', chunk => stderr.push(chunk));
    let reachedWait = false;
    let completed = false;
    child.on('message', message => {
      if (message === 'waiting') reachedWait = true;
      if (message === 'ok') completed = true;
    });

    const result = await new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve, reject) => {
      const timeout = setTimeout(() => child.kill('SIGKILL'), 10_000);
      child.once('error', error => {
        clearTimeout(timeout);
        reject(error);
      });
      child.once('close', (code, signal) => {
        clearTimeout(timeout);
        resolve({ code, signal });
      });
    });

    const diagnostics = `reached wait: ${reachedWait}\n${Buffer.concat(stderr).toString() || 'child produced no stderr'}`;
    expect({ ...result, completed }, diagnostics).toEqual({
      code: 0,
      signal: null,
      completed: true,
    });
  }, 20_000);

  it('persists an idle signal without waking the agent when idle behavior is persist', async () => {
    let streamCount = 0;
    const memory = new MockMemory();
    await memory.createThread({ threadId: 'idle-persist-thread', resourceId: 'idle-persist-user' });
    const agent = new Agent({
      id: 'idle-persist-agent',
      name: 'Idle Persist Agent',
      instructions: 'Test',
      model: new MockLanguageModelV2({
        doStream: async () => {
          streamCount += 1;
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: convertArrayToReadableStream([{ type: 'stream-start', warnings: [] }]),
          };
        },
      }),
      memory,
    });

    const subscription = await agent.subscribeToThread({
      resourceId: 'idle-persist-user',
      threadId: 'idle-persist-thread',
    });
    const nextRun = readNextRunWithParts(subscription.stream[Symbol.asyncIterator]());

    try {
      const result = agent.sendSignal(
        { type: 'user-message', contents: 'persist without waking' },
        { resourceId: 'idle-persist-user', threadId: 'idle-persist-thread', ifIdle: { behavior: 'persist' } },
      );
      await expect(result.persisted).resolves.toBeUndefined();

      const subscribedRun = await withTimeout(nextRun, 'Timed out waiting for persisted signal broadcast');
      expect(subscribedRun.value.parts).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            type: 'data-user-message',
            data: expect.objectContaining({ contents: 'persist without waking' }),
          }),
        ]),
      );
      expect(subscribedRun.value.part).toMatchObject({
        type: 'finish',
        payload: {
          stepResult: { reason: 'stop' },
          output: { usage: { inputTokens: 0, outputTokens: 0, totalTokens: 0 } },
        },
      });
      expect(subscribedRun.value.part.payload).not.toHaveProperty('usage');

      const recalled = await memory.recall({ threadId: 'idle-persist-thread', resourceId: 'idle-persist-user' });
      expect(streamCount).toBe(0);
      expect(recalled.messages).toHaveLength(1);
      // The synthesized `start` chunk must match the shape of every real start emitter
      // (from + payload.id/messageId) so chunk consumers don't crash on `payload.messageId`.
      expect(subscribedRun.value.parts[0]).toEqual({
        type: 'start',
        runId: expect.any(String),
        from: 'AGENT',
        payload: { id: 'idle-persist-agent', messageId: `persisted-signal:${recalled.messages[0]?.id}` },
      });
      expect(new Set(subscribedRun.value.parts.map(part => part.runId))).toEqual(
        new Set([subscribedRun.value.part.runId]),
      );
      expect((subscribedRun.value.parts[0] as any).payload.messageId).not.toBe(recalled.messages[0]?.id);
      // Stash dropped; payload lives in content.parts now.
      expect(recalled.messages[0]?.content.metadata?.signal).toMatchObject({ type: 'user', tagName: 'user' });
      expect(recalled.messages[0]?.content.parts).toEqual(
        expect.arrayContaining([expect.objectContaining({ type: 'text', text: 'persist without waking' })]),
      );
    } finally {
      subscription.unsubscribe();
    }
  });

  it('publishes an authenticated persisted-signal terminal before releasing its cross-runtime lease', async () => {
    const pubsub = new ControlledLeasePubSub();
    const ownerRuntime = new AgentThreadStreamRuntime();
    const followerRuntime = new AgentThreadStreamRuntime();
    const memory = new MockMemory();
    const target = { resourceId: 'remote-persist-user', threadId: 'remote-persist-thread' };
    const key = `${target.resourceId}\u0000${target.threadId}`;
    await memory.createThread({ threadId: target.threadId, resourceId: target.resourceId });
    const agent = new Agent({
      id: 'remote-persist-agent',
      name: 'Remote Persist Agent',
      instructions: 'Test',
      model: createTextStreamModel('must not run'),
      memory,
    });
    const subscription = await followerRuntime.subscribeToThread(agent, target, pubsub);
    const nextRun = readNextRunWithParts(subscription.stream[Symbol.asyncIterator]());

    try {
      const result = ownerRuntime.sendSignal(
        agent,
        createSignal({ type: 'user-message', contents: 'persist across runtimes' }),
        { ...target, ifIdle: { behavior: 'persist' as const } },
        pubsub,
      );
      await expect(result.persisted).resolves.toBeUndefined();
      const subscribedRun = await withTimeout(nextRun, 'Timed out waiting for remote persisted-signal stream');
      expect(subscribedRun).toMatchObject({ value: { part: { type: 'finish' } } });
      const runId = subscribedRun.value.runId;
      await waitForCondition(
        () => pubsub.publishedData.some(data => data?.type === 'run-completed' && data.runId === runId),
        1_000,
      );
      await pubsub.flush();
      await waitForCondition(() => subscription.activeRunId() === null, 1_000);
      const registration = pubsub.publishedData.find(data => data?.type === 'run-registered' && data.runId === runId);
      const terminal = pubsub.publishedData.find(data => data?.type === 'run-completed' && data.runId === runId);
      expect(terminal?.streamId).toBe(registration?.streamId);
      expect(terminal?.leaseOwner).toBe(registration?.leaseOwner);
      await waitForCondition(() => pubsub.owners.get(key) === undefined, 1_000);
    } finally {
      subscription.unsubscribe();
    }
  });

  it.each(['reactive', 'system-reminder'] as const)(
    'persists and broadcasts an idle %s signal with independent subscriber exclusions',
    async type => {
      const memory = new MockMemory();
      const target = { threadId: 'hidden-persist-thread', resourceId: 'hidden-persist-user' };
      await memory.createThread(target);
      const agent = new Agent({
        id: 'hidden-persist-agent',
        name: 'Hidden Persist Agent',
        instructions: 'Test',
        model: createTextStreamModel('unused'),
        memory,
      });
      const subscription = await agent.subscribeToThread(target);
      const excluding = await agent.subscribeToThread({ ...target, hideSignals: ['system-reminder'] });
      const includedIterator = subscription.stream[Symbol.asyncIterator]();
      const excludedIterator = excluding.stream[Symbol.asyncIterator]();
      const includedRun = readNextRunWithParts(includedIterator);
      const excludedRun = readNextRunWithParts(excludedIterator);
      try {
        const result = agent.sendSignal(
          { type, contents: 'internal context' },
          {
            ...target,
            ifIdle: { behavior: 'persist' },
          },
        );
        await expect(result.accepted).resolves.toMatchObject({ action: 'persist' });
        await result.persisted;
        expect((await memory.recall(target)).messages).toHaveLength(0);
        const stored = await memory.recall({ ...target, includeSystemReminders: true });
        expect(stored.messages).toHaveLength(1);
        expect(stored.messages[0]?.content.parts).toContainEqual({ type: 'text', text: 'internal context' });
        const [included, excluded] = await withTimeout(
          Promise.all([includedRun, excludedRun]),
          'Idle signal broadcast',
        );
        expect(included.value.parts).toContainEqual(
          expect.objectContaining({
            type: 'data-signal',
            data: expect.objectContaining({ type: 'reactive', contents: 'internal context' }),
          }),
        );
        expect(excluded.value.parts.some(part => part.type === 'data-signal')).toBe(false);
        expect(included.value.part.type).toBe('finish');
        expect(excluded.value.part.type).toBe('finish');
        const idleReaders = Promise.all([includedIterator.next(), excludedIterator.next()]);
        await waitForCondition(() => subscription.activeRunId() === null && excluding.activeRunId() === null);
        expect(subscription.activeRunId()).toBeNull();
        subscription.unsubscribe();
        excluding.unsubscribe();
        await idleReaders;
      } finally {
        subscription.unsubscribe();
        excluding.unsubscribe();
      }
    },
  );

  it.each([undefined, false, true, [], ['reactive'], ['system-reminder']] as const)(
    'keeps stream exclusions %j local while transforms, subscribers and model retain signals',
    async exclusions => {
      const memory = new MockMemory();
      const target = { threadId: crypto.randomUUID(), resourceId: 'exclusions-user' };
      const model = createTextStreamModel('ordinary response');
      const transformed: unknown[] = [];
      const onChunk = vi.fn();
      const agent = new Agent({
        id: 'caller-exclusions-agent',
        name: 'Caller Exclusions',
        instructions: 'Test',
        model,
        memory,
        inputProcessors: [
          {
            id: 'emit-signals',
            processInputStep: async ({ stepNumber, sendSignal }) => {
              if (stepNumber === 0) {
                await sendSignal({ type: 'reactive', id: 'reminder-id', contents: 'retained reminder' });
                await sendSignal({ type: 'state', id: 'state-id', contents: 'retained state' });
              }
            },
          },
        ],
      });
      const subscription = await agent.subscribeToThread({ ...target, hideSignals: false });
      const allExcluded = await agent.subscribeToThread({
        ...target,
        hideSignals: true,
      });
      const includedRun = readNextRunWithParts(subscription.stream[Symbol.asyncIterator]());
      const excludedRun = readNextRunWithParts(allExcluded.stream[Symbol.asyncIterator]());
      try {
        const output = await agent.stream('hello', {
          memory: { thread: target.threadId, resource: target.resourceId },
          hideSignals: typeof exclusions === 'boolean' ? exclusions : exclusions ? [...exclusions] : undefined,
          onChunk,
          experimentalTransform: () =>
            new TransformStream({
              transform(chunk, controller) {
                transformed.push(chunk);
                controller.enqueue(chunk);
              },
            }),
        });
        const direct: unknown[] = [];
        for await (const chunk of output.fullStream) direct.push(chunk);
        const [included, excluded] = await withTimeout(
          Promise.all([includedRun, excludedRun]),
          'Independent signal consumers',
        );
        const reminder = expect.objectContaining({
          type: 'data-signal',
          data: expect.objectContaining({ type: 'reactive', contents: 'retained reminder' }),
        });
        if (exclusions === true || (Array.isArray(exclusions) && exclusions.length))
          expect(direct).not.toContainEqual(reminder);
        else expect(direct).toContainEqual(reminder);
        const state = expect.objectContaining({
          type: 'data-signal',
          data: expect.objectContaining({ type: 'state' }),
        });
        if (exclusions === true) expect(direct).not.toContainEqual(state);
        else expect(direct).toContainEqual(state);
        expect(direct).toContainEqual(expect.objectContaining({ type: 'text-delta' }));
        expect(transformed).toContainEqual(reminder);
        expect(onChunk).toHaveBeenCalledExactlyOnceWith(
          expect.objectContaining({
            type: 'text-delta',
            payload: expect.objectContaining({ text: 'ordinary response' }),
          }),
        );
        expect(included.value.parts).toContainEqual(reminder);
        expect(excluded.value.parts.some(part => part.type === 'data-signal')).toBe(false);
        expect(excluded.value.part.type).toBe('finish');
        expect(JSON.stringify(model.doStreamCalls[0]?.prompt)).toContain('retained reminder');
        expect(await output.text).toBe('ordinary response');
        const stored = await memory.recall({ ...target, includeSystemReminders: true });
        expect(stored.messages.map(message => message.id)).toEqual(expect.arrayContaining(['reminder-id', 'state-id']));
      } finally {
        subscription.unsubscribe();
        allExcluded.unsubscribe();
      }
    },
  );

  it('does not persist or broadcast a transient idle signal when idle behavior is persist', async () => {
    const memory = new MockMemory();
    await memory.createThread({ threadId: 'transient-idle-persist-thread', resourceId: 'transient-idle-persist-user' });
    const agent = new Agent({
      id: 'transient-idle-persist-agent',
      name: 'Transient Idle Persist Agent',
      instructions: 'Test',
      model: createTextStreamModel('unused response'),
      memory,
    });
    const subscription = await agent.subscribeToThread({
      resourceId: 'transient-idle-persist-user',
      threadId: 'transient-idle-persist-thread',
    });
    const iterator = subscription.stream[Symbol.asyncIterator]();

    try {
      const result = agent.sendSignal(
        { type: 'user-message', contents: 'do not retain', transient: true },
        {
          resourceId: 'transient-idle-persist-user',
          threadId: 'transient-idle-persist-thread',
          ifIdle: { behavior: 'persist' },
        },
      );
      await expect(result.accepted).resolves.toEqual({ action: 'discard' });
      expect(result.persisted).toBeUndefined();

      const recalled = await memory.recall({
        threadId: 'transient-idle-persist-thread',
        resourceId: 'transient-idle-persist-user',
      });
      expect(recalled.messages).toHaveLength(0);
      await expect(
        Promise.race([
          iterator.next().then(() => 'broadcast'),
          new Promise(resolve => setTimeout(resolve, 25, 'none')),
        ]),
      ).resolves.toBe('none');
    } finally {
      subscription.unsubscribe();
    }
  });

  it('reports discard and skips storage for a transient active signal when active behavior is persist', async () => {
    const { model, releaseFirst } = createBlockingFirstTextStreamModel('first response', 'unused');
    const memory = new MockMemory();
    await memory.createThread({
      threadId: 'transient-active-persist-thread',
      resourceId: 'transient-active-persist-user',
    });
    const agent = new Agent({
      id: 'transient-active-persist-agent',
      name: 'Transient Active Persist Agent',
      instructions: 'Test',
      model,
      memory,
    });

    const stream = await agent.stream('Hello', {
      memory: { thread: 'transient-active-persist-thread', resource: 'transient-active-persist-user' },
    });
    try {
      const result = agent.sendSignal(
        { type: 'user-message', contents: 'do not retain', transient: true },
        {
          resourceId: 'transient-active-persist-user',
          threadId: 'transient-active-persist-thread',
          ifActive: { behavior: 'persist' },
        },
      );
      await expect(result.accepted).resolves.toEqual({ action: 'discard' });
      expect(result.persisted).toBeUndefined();

      const recalled = await memory.recall({
        threadId: 'transient-active-persist-thread',
        resourceId: 'transient-active-persist-user',
      });
      expect(recalled.messages.filter(message => message.role === 'signal')).toHaveLength(0);
    } finally {
      releaseFirst();
    }
    await expect(stream.text).resolves.toBe('first response');
  });

  it('discards an active signal when active behavior is discard', async () => {
    let releaseFirst!: () => void;
    const firstFinished = new Promise<void>(resolve => {
      releaseFirst = resolve;
    });
    let streamCount = 0;
    const prompts: any[][] = [];

    const agent = new Agent({
      id: 'active-discard-agent',
      name: 'Active Discard Agent',
      instructions: 'Test',
      model: new MockLanguageModelV2({
        doStream: async ({ prompt }) => {
          streamCount += 1;
          prompts.push(prompt);
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: new ReadableStream({
              async start(controller) {
                controller.enqueue({ type: 'stream-start', warnings: [] });
                controller.enqueue({
                  type: 'response-metadata',
                  id: `discard-${streamCount}`,
                  modelId: 'mock-model-id',
                  timestamp: new Date(0),
                });
                controller.enqueue({ type: 'text-start', id: 'text-1' });
                controller.enqueue({ type: 'text-delta', id: 'text-1', delta: 'first response' });
                controller.enqueue({ type: 'text-end', id: 'text-1' });
                if (streamCount === 1) {
                  await firstFinished;
                }
                controller.enqueue({
                  type: 'finish',
                  finishReason: 'stop',
                  usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
                });
                controller.close();
              },
            }),
          };
        },
      }),
    });

    const stream = await agent.stream('Hello', {
      memory: { thread: 'active-discard-thread', resource: 'active-discard-user' },
    });
    await agent.sendSignal(
      { type: 'user-message', contents: 'discard while running' },
      { resourceId: 'active-discard-user', threadId: 'active-discard-thread', ifActive: { behavior: 'discard' } },
    );

    releaseFirst();
    await expect(stream.text).resolves.toBe('first response');
    expect(streamCount).toBe(1);
    expect(JSON.stringify(prompts)).not.toContain('discard while running');
  });

  it('uses lease ownership as the authority for remote active thread state', async () => {
    const agent = { id: 'lease-authority-agent' } as Agent<any, any, any, any>;
    const key = 'lease-authority-resource\u0000lease-authority-thread';
    const topic = `agent.thread-stream.${encodeURIComponent(key)}`;

    const fallbackPubSub = new RetainedAsyncCallbackPubSub();
    const fallbackRuntime = new AgentThreadStreamRuntime();
    const fallbackSubscription = await fallbackRuntime.subscribeToThread(
      agent,
      { resourceId: 'lease-authority-resource', threadId: 'lease-authority-thread' },
      fallbackPubSub,
    );
    await fallbackPubSub.publish(topic, {
      type: 'run-registered',
      runId: 'fallback-run',
      data: { type: 'run-registered', runId: 'fallback-run', streamId: 'fallback-stream', streamSeq: 1 },
    });
    await fallbackPubSub.flush();
    await waitForCondition(() => fallbackSubscription.activeRunId() === 'fallback-run');
    fallbackSubscription.unsubscribe();

    const stalePubSub = new ControlledLeasePubSub();
    const staleRuntime = new AgentThreadStreamRuntime();
    const staleSubscription = await staleRuntime.subscribeToThread(
      agent,
      { resourceId: 'lease-authority-resource', threadId: 'lease-authority-thread' },
      stalePubSub,
    );
    await stalePubSub.publish(topic, {
      type: 'run-registered',
      runId: 'stale-run',
      data: { type: 'run-registered', runId: 'stale-run', streamId: 'stale-stream', streamSeq: 1 },
    });
    await stalePubSub.flush();
    await nextTick();
    expect(staleSubscription.activeRunId()).toBeNull();
    staleSubscription.unsubscribe();

    const livePubSub = new ControlledLeasePubSub();
    const liveOwner = 'mastra-thread-owner:["live-run","peer-runtime","attempt"]';
    livePubSub.owners.set(key, liveOwner);
    livePubSub.ownerReadFailures = 1;
    const liveRuntime = new AgentThreadStreamRuntime();
    const liveSubscription = await liveRuntime.subscribeToThread(
      agent,
      { resourceId: 'lease-authority-resource', threadId: 'lease-authority-thread' },
      livePubSub,
    );
    await livePubSub.publish(topic, {
      type: 'run-registered',
      runId: 'live-run',
      data: {
        type: 'run-registered',
        runId: 'live-run',
        streamId: 'failed-owner-read-stream',
        streamSeq: 1,
        leaseOwner: liveOwner,
      },
    });
    await livePubSub.flush();
    expect(liveSubscription.activeRunId()).toBeNull();
    await livePubSub.publish(topic, {
      type: 'run-registered',
      runId: 'live-run',
      data: {
        type: 'run-registered',
        runId: 'live-run',
        streamId: 'forged-owner-stream',
        streamSeq: 1,
        leaseOwner: 'mastra-thread-owner:["live-run","forged-runtime","attempt"]',
      },
    });
    await livePubSub.flush();
    expect(liveSubscription.activeRunId()).toBeNull();
    await livePubSub.publish(topic, {
      type: 'run-registered',
      runId: 'live-run',
      data: {
        type: 'run-registered',
        runId: 'live-run',
        streamId: 'live-stream',
        streamSeq: 1,
        leaseOwner: liveOwner,
      },
    });
    await livePubSub.publish(topic, {
      type: 'stream-part',
      runId: 'live-run',
      data: {
        type: 'stream-part',
        runId: 'live-run',
        streamId: 'live-stream',
        sourceId: 'peer-runtime',
        leaseOwner: liveOwner,
        part: { type: 'start', runId: 'live-run' },
      },
    });
    await livePubSub.flush();
    await waitForCondition(() => liveSubscription.activeRunId() === 'live-run');
    liveSubscription.unsubscribe();
  });

  it('discards local pre-run copies when a drained run loses its reserved lease', async () => {
    const pubsub = new ControlledLeasePubSub();
    const runtime = new AgentThreadStreamRuntime();
    const resourceId = 'drained-reservation-resource';
    const threadId = 'drained-reservation-thread';
    const key = `${resourceId}\u0000${threadId}`;
    const oldRunId = 'drained-reservation-old-run';
    let finishOldRun!: () => void;
    const oldRunFinished = new Promise<void>(resolve => {
      finishOldRun = resolve;
    });
    let signalTransferStarted!: () => void;
    const transferStarted = new Promise<void>(resolve => {
      signalTransferStarted = resolve;
    });
    let releaseTransfer!: () => void;
    pubsub.transferLeaseWait = new Promise<void>(resolve => {
      releaseTransfer = resolve;
    });
    pubsub.onTransferLease = signalTransferStarted;

    const agent = { id: 'drained-reservation-agent' } as Agent<any, any, any, any>;
    agent.stream = vi.fn(async (_signal, options) => ({ runId: options.runId })) as any;
    runtime.registerRun(
      agent,
      {
        runId: oldRunId,
        status: 'running',
        fullStream: (async function* () {})(),
        _waitUntilFinished: () => oldRunFinished,
      } as any,
      { runId: oldRunId, memory: { resource: resourceId, thread: threadId } } as any,
      pubsub,
    );
    runtime.sendSignal(
      agent,
      { type: 'user-message', contents: 'start drained run' },
      { resourceId, threadId },
      pubsub,
    );

    finishOldRun();
    await transferStarted;
    const reservedRunId = runtime.getActiveThreadRunId({ resourceId, threadId }, pubsub)!;
    const followUp = runtime.sendSignal(
      agent,
      { type: 'user-message', contents: 'attach during transfer' },
      { resourceId, threadId },
      pubsub,
    );
    await expect(followUp.accepted).resolves.toMatchObject({ action: 'deliver', runId: reservedRunId });

    const winnerRunId = 'drained-reservation-winner';
    pubsub.owners.set(key, winnerRunId);
    releaseTransfer();
    await waitForCondition(() => runtime.getActiveThreadRunId({ resourceId, threadId }, pubsub) === undefined);

    const recoveryRunId = 'drained-reservation-recovery';
    let finishRecovery!: () => void;
    const recoveryFinished = new Promise<void>(resolve => {
      finishRecovery = resolve;
    });
    runtime.registerRun(
      agent,
      {
        runId: recoveryRunId,
        status: 'running',
        fullStream: (async function* () {})(),
        _waitUntilFinished: () => recoveryFinished,
      } as any,
      { runId: recoveryRunId, memory: { resource: resourceId, thread: threadId } } as any,
      pubsub,
    );
    expect(runtime.drainPendingSignals(recoveryRunId, pubsub, 'pre-run')).toEqual([]);
    expect(agent.stream).not.toHaveBeenCalled();
    finishRecovery();
    await pubsub.releaseLease(key, winnerRunId);
  });

  it('drains queued messages when a pre-registration reservation is released', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const resourceId = 'released-reservation-resource';
    const threadId = 'released-reservation-thread';
    const agent = {
      id: 'released-reservation-agent',
      stream: vi.fn(async (_signal, options) => ({ runId: options.runId })),
    } as any;

    // Fork (PF-4402): waitForCrossAgentThreadRun never reserves; a same-agent
    // pre-registration run holds the thread through reserveRun.
    const reservation = { runId: 'reservation-run', memory: { resource: resourceId, thread: threadId } };
    runtime.reserveRun(reservation, undefined, agent.id);
    await runtime.waitForThreadRunReservation(reservation, undefined, agent.id);
    const queued = runtime.queueMessage(agent, { contents: 'queued after reservation' }, { resourceId, threadId });
    await expect(queued.accepted).resolves.toMatchObject({ action: 'deliver' });

    runtime.releaseThreadRunReservation('reservation-run');
    await waitForCondition(() => agent.stream.mock.calls.length === 1);

    expect(agent.stream).toHaveBeenCalledWith(
      expect.objectContaining({ contents: 'queued after reservation' }),
      expect.objectContaining({ memory: expect.objectContaining({ resource: resourceId, thread: threadId }) }),
    );
  });

  it('updates the controller request context with the prepared run abort signal', () => {
    const runtime = new AgentThreadStreamRuntime();
    const requestContext = new RequestContext();
    const upstreamAbortController = new AbortController();
    requestContext.set('controller', { abortSignal: upstreamAbortController.signal });

    const prepared = runtime.prepareRunOptions({
      runId: 'controller-abort-run',
      memory: { resource: 'controller-abort-resource', thread: 'controller-abort-thread' },
      abortSignal: upstreamAbortController.signal,
      requestContext,
    } as any);
    const controller = prepared.requestContext?.get('controller') as { abortSignal: AbortSignal };

    expect(controller.abortSignal).toBe(prepared.abortSignal);
    expect(controller.abortSignal).not.toBe(upstreamAbortController.signal);
    expect((requestContext.get('controller') as { abortSignal: AbortSignal }).abortSignal).toBe(
      upstreamAbortController.signal,
    );

    expect(runtime.abortRun('controller-abort-run')).toBe(true);
    expect(controller.abortSignal.aborted).toBe(true);
  });

  it('preserves abort intent for a thread reserved by a signal wake before its run is prepared', async () => {
    const pubsub = new ControlledLeasePubSub();
    const runtime = new AgentThreadStreamRuntime();
    const resourceId = 'reservation-abort-resource';
    const threadId = 'reservation-abort-thread';
    const preparedAborted: boolean[] = [];
    const agent = { id: 'reservation-abort-agent' } as Agent<any, any, any, any>;
    agent.stream = vi.fn(async (_signal, options) => {
      const prepared = runtime.prepareRunOptions(options as any, pubsub);
      preparedAborted.push(prepared.abortSignal?.aborted ?? false);
      if (prepared.abortSignal?.aborted) {
        throw new Error('aborted before start');
      }
      return { runId: (options as any).runId } as any;
    }) as any;

    const result = runtime.sendSignal(
      agent,
      { type: 'user-message', contents: 'wake' },
      { resourceId, threadId },
      pubsub,
    );
    // The thread reservation is taken synchronously inside sendSignal; the lease
    // acquire has not resolved yet, so the run is not in preparedRunsById.
    expect(runtime.abortThread({ resourceId, threadId }, pubsub)).toBe(true);

    await expect(result.accepted).rejects.toThrow('aborted before start');
    expect(preparedAborted).toEqual([true]);
    expect(runtime.getActiveThreadRunId({ resourceId, threadId }, pubsub)).toBeUndefined();
  });

  it('keeps follow-ups attached while a continuation reserves its lease', async () => {
    const pubsub = new ControlledLeasePubSub();
    const runtime = new AgentThreadStreamRuntime();
    const resourceId = 'continuation-reservation-resource';
    const threadId = 'continuation-reservation-thread';
    const key = `${resourceId}\u0000${threadId}`;
    const oldRunId = 'continuation-reservation-old-run';
    let finishOldRun!: () => void;
    const oldRunFinished = new Promise<void>(resolve => {
      finishOldRun = resolve;
    });
    let signalTransferStarted!: () => void;
    const transferStarted = new Promise<void>(resolve => {
      signalTransferStarted = resolve;
    });
    let releaseTransfer!: () => void;
    pubsub.transferLeaseWait = new Promise<void>(resolve => {
      releaseTransfer = resolve;
    });
    pubsub.onTransferLease = signalTransferStarted;

    const agent = {
      id: 'continuation-reservation-agent',
      stream: vi.fn(async (_messages, options) => ({ runId: options.runId })),
    } as unknown as Agent<any, any, any, any>;
    runtime.registerRun(
      agent,
      {
        runId: oldRunId,
        status: 'running',
        fullStream: (async function* () {})(),
        _waitUntilFinished: () => oldRunFinished,
      } as any,
      { runId: oldRunId, memory: { resource: resourceId, thread: threadId } } as any,
      pubsub,
    );
    const continuation = runtime.continueWithMessages(
      agent,
      [] as any,
      { resourceId, threadId, streamOptions: { memory: { resource: resourceId, thread: threadId } } as any },
      pubsub,
    );

    finishOldRun();
    await transferStarted;
    const followUp = runtime.sendSignal(
      agent,
      { type: 'user-message', contents: 'attach to continuation' },
      { resourceId, threadId },
      pubsub,
    );
    await expect(followUp.accepted).resolves.toMatchObject({ action: 'deliver', runId: continuation.runId });
    expect(runtime.drainPendingSignals(continuation.runId, pubsub, 'pre-run')).toEqual([
      expect.objectContaining({ contents: 'attach to continuation' }),
    ]);

    releaseTransfer();
    await waitForCondition(() => vi.mocked(agent.stream).mock.calls.length === 1);
    const continuationOwner = pubsub.owners.get(key);
    expect(continuationOwner).toContain(`"${continuation.runId}"`);
    if (continuationOwner) await pubsub.releaseLease(key, continuationOwner);
  });

  it('orders delayed lease validation and ignores stale stream terminal events', async () => {
    const pubsub = new ControlledLeasePubSub();
    const runtime = new AgentThreadStreamRuntime();
    const key = 'ordered-resource\u0000ordered-thread';
    const topic = `agent.thread-stream.${encodeURIComponent(key)}`;
    const runId = 'ordered-run';
    const leaseOwner = `mastra-thread-owner:${JSON.stringify([runId, 'ordered-source', 'attempt'])}`;
    pubsub.owners.set(key, leaseOwner);
    pubsub.ownerReadDelayMs = 10;
    const subscription = await runtime.subscribeToThread(
      { id: 'ordered-agent' } as Agent<any, any, any, any>,
      { resourceId: 'ordered-resource', threadId: 'ordered-thread' },
      pubsub,
    );

    for (const data of [
      { type: 'run-registered', runId, streamId: 'ordered-stream-1', streamSeq: 1, leaseOwner },
      { type: 'run-registered', runId, streamId: 'ordered-stream-2', streamSeq: 2, leaseOwner },
      { type: 'run-aborted', runId, streamId: 'ordered-stream-1', leaseOwner },
    ]) {
      await pubsub.publish(topic, { type: data.type, runId, data });
    }
    await pubsub.flush();
    await waitForCondition(() => subscription.activeRunId() === runId);
    await new Promise(resolve => setTimeout(resolve, 50));
    expect(subscription.abort()).toBe(true);
    await pubsub.flush();
    await waitForCondition(() =>
      pubsub.publishedData.some(data => data?.type === 'run-abort-requested' && data.streamId === 'ordered-stream-2'),
    );
    subscription.unsubscribe();
  });

  it('bounds remote waits by renewed lease ownership and unsubscribes on exit', async () => {
    const pubsub = new ControlledLeasePubSub();
    const runtime = new AgentThreadStreamRuntime();
    const key = 'bounded-resource\u0000bounded-thread';
    const topic = `agent.thread-stream.${encodeURIComponent(key)}`;
    const runId = 'bounded-run';
    const leaseOwner = `mastra-thread-owner:${JSON.stringify([runId, 'remote-owner-source', 'attempt'])}`;
    pubsub.owners.set(key, leaseOwner);
    const subscription = await runtime.subscribeToThread(
      { id: 'bounded-owner-agent' } as Agent<any, any, any, any>,
      { resourceId: 'bounded-resource', threadId: 'bounded-thread' },
      pubsub,
    );
    await pubsub.publish(topic, {
      type: 'run-registered',
      runId,
      data: { type: 'run-registered', runId, streamId: 'bounded-stream', streamSeq: 1, leaseOwner },
    });
    await pubsub.flush();
    await waitForCondition(() => subscription.activeRunId() === runId);

    vi.useFakeTimers();
    try {
      let resolved = false;
      const wait = runtime
        .waitForCrossAgentThreadRun(
          { id: 'bounded-other-agent' } as Agent<any, any, any, any>,
          { memory: { resource: 'bounded-resource', thread: 'bounded-thread' } },
          pubsub,
        )
        .then(() => {
          resolved = true;
        });
      await vi.advanceTimersByTimeAsync(15_000);
      expect(resolved).toBe(false);
      pubsub.owners.delete(key);
      await vi.advanceTimersByTimeAsync(15_000);
      await wait;
      expect(resolved).toBe(true);
      expect(pubsub.unsubscribeCount).toBeGreaterThanOrEqual(1);
      expect(subscription.activeRunId()).toBeNull();
    } finally {
      vi.useRealTimers();
      subscription.unsubscribe();
    }
  });

  it('ends a remote wait on a terminal event before the lease deadline', async () => {
    const pubsub = new ControlledLeasePubSub();
    const runtime = new AgentThreadStreamRuntime();
    const key = 'terminal-wait-resource\u0000terminal-wait-thread';
    const topic = `agent.thread-stream.${encodeURIComponent(key)}`;
    const runId = 'terminal-wait-run';
    const leaseOwner = `mastra-thread-owner:${JSON.stringify([runId, 'terminal-source', 'attempt'])}`;
    pubsub.owners.set(key, leaseOwner);
    const subscription = await runtime.subscribeToThread(
      { id: 'terminal-wait-owner' } as Agent<any, any, any, any>,
      { resourceId: 'terminal-wait-resource', threadId: 'terminal-wait-thread' },
      pubsub,
    );
    await pubsub.publish(topic, {
      type: 'run-registered',
      runId,
      data: { type: 'run-registered', runId, streamId: 'terminal-wait-stream', streamSeq: 1, leaseOwner },
    });
    await pubsub.flush();
    await waitForCondition(() => subscription.activeRunId() === runId);

    const wait = runtime.waitForCrossAgentThreadRun(
      { id: 'terminal-wait-other' } as Agent<any, any, any, any>,
      { memory: { resource: 'terminal-wait-resource', thread: 'terminal-wait-thread' } },
      pubsub,
    );
    let waitResolved = false;
    void wait.then(() => (waitResolved = true));
    await pubsub.publish(topic, {
      type: 'run-completed',
      runId,
      data: {
        type: 'run-completed',
        runId,
        streamId: 'terminal-wait-stream',
        leaseOwner: `mastra-thread-owner:${JSON.stringify([runId, 'forged-source', 'attempt'])}`,
      },
    });
    await pubsub.flush();
    await nextTick();
    expect(waitResolved).toBe(false);
    expect(subscription.activeRunId()).toBe(runId);
    pubsub.owners.delete(key);
    await pubsub.publish(topic, {
      type: 'run-completed',
      runId,
      data: { type: 'run-completed', runId, streamId: 'terminal-wait-stream', leaseOwner },
    });
    await pubsub.flush();
    await expect(wait).resolves.toBeUndefined();
    expect(pubsub.unsubscribeCount).toBeGreaterThanOrEqual(1);
    subscription.unsubscribe();
  });

  it('ends a remote wait on a discarded registration before the lease deadline', async () => {
    const pubsub = new ControlledLeasePubSub();
    const runtime = new AgentThreadStreamRuntime();
    const key = 'discard-wait-resource\u0000discard-wait-thread';
    const topic = `agent.thread-stream.${encodeURIComponent(key)}`;
    const runId = 'discard-wait-run';
    const leaseOwner = `mastra-thread-owner:${JSON.stringify([runId, 'discard-source', 'attempt'])}`;
    pubsub.owners.set(key, leaseOwner);
    const subscription = await runtime.subscribeToThread(
      { id: 'discard-wait-owner' } as Agent<any, any, any, any>,
      { resourceId: 'discard-wait-resource', threadId: 'discard-wait-thread' },
      pubsub,
    );
    await pubsub.publish(topic, {
      type: 'run-registered',
      runId,
      data: { type: 'run-registered', runId, streamId: 'discard-wait-stream', streamSeq: 1, leaseOwner },
    });
    await pubsub.flush();
    await waitForCondition(() => subscription.activeRunId() === runId);

    const wait = runtime.waitForCrossAgentThreadRun(
      { id: 'discard-wait-other' } as Agent<any, any, any, any>,
      { memory: { resource: 'discard-wait-resource', thread: 'discard-wait-thread' } },
      pubsub,
    );
    // A rolled-back strict registration retracts that exact stream. Its owner
    // never publishes a lifecycle terminal for the stream, so stream identity
    // alone authenticates the discard and the waiter must not block until the
    // lease deadline.
    await pubsub.publish(topic, {
      type: 'run-discarded',
      runId,
      data: { type: 'run-discarded', runId, streamId: 'discard-wait-stream' },
    });
    await pubsub.flush();
    await expect(wait).resolves.toBeUndefined();
    expect(pubsub.unsubscribeCount).toBeGreaterThanOrEqual(1);
    subscription.unsubscribe();
  });

  describe('same-agent thread serialization', () => {
    const threadId = 'same-agent-wait-thread';
    const resourceId = 'same-agent-wait-user';

    const registerRunningRun = async (
      runtime: AgentThreadStreamRuntime,
      agent: Agent<any, any, any, any>,
      runId: string,
    ) => {
      let finish!: () => void;
      const finished = new Promise<void>(resolve => {
        finish = resolve;
      });
      const output = {
        runId,
        status: 'running',
        fullStream: new ReadableStream({
          start(controller) {
            void finished.then(() => controller.close());
          },
        }),
        _waitUntilFinished: () => finished,
      } as any;
      const completion = runtime.registerRun(agent, output, {
        memory: { thread: threadId, resource: resourceId },
      } as any);
      void completion?.catch(() => {});
      return {
        output,
        finish: () => {
          output.status = 'success';
          finish();
        },
      };
    };

    it('parks a same-agent reservation retry behind an actively running record', async () => {
      const runtime = new AgentThreadStreamRuntime();
      const agent = { id: 'same-agent-wait-agent' } as Agent<any, any, any, any>;
      const run = await registerRunningRun(runtime, agent, 'same-agent-wait-run-1');

      let resolved = false;
      const wait = runtime
        .waitForThreadRunReservation(
          {
            runId: 'same-agent-wait-run-2',
            memory: { thread: threadId, resource: resourceId },
          },
          undefined,
          agent.id,
        )
        .then(() => {
          resolved = true;
        });
      await new Promise(resolve => setTimeout(resolve, 25));
      expect(resolved).toBe(false);

      run.finish();
      await withTimeout(wait, 'Timed out waiting for the same-agent wait to release');
      expect(resolved).toBe(true);
    });

    it('does not wait when the caller targets the active run (continuation)', async () => {
      const runtime = new AgentThreadStreamRuntime();
      const agent = { id: 'same-agent-continuation-agent' } as Agent<any, any, any, any>;
      const run = await registerRunningRun(runtime, agent, 'same-agent-continuation-run');

      await withTimeout(
        runtime.waitForCrossAgentThreadRun(agent, {
          memory: { thread: threadId, resource: resourceId },
          runId: 'same-agent-continuation-run',
        }),
        'Continuation wait should resolve immediately',
      );

      run.finish();
    });

    it('does not wait on a same-agent suspended record', async () => {
      const runtime = new AgentThreadStreamRuntime();
      const agent = { id: 'same-agent-suspended-agent' } as Agent<any, any, any, any>;
      const run = await registerRunningRun(runtime, agent, 'same-agent-suspended-run');
      run.output.status = 'suspended';

      await withTimeout(
        runtime.waitForCrossAgentThreadRun(agent, { memory: { thread: threadId, resource: resourceId } }),
        'Suspended-record wait should resolve immediately',
      );

      run.finish();
    });

    // PF-4402 decision C: skipped — asserts the opposite of the fork contract.
    // In this fork same-agent turns serialize on the thread reservation
    // (waitForCrossAgentThreadRun only orders cross-agent work), and a run that
    // still holds a sibling suspension is treated as suspended, so a fresh
    // same-agent turn may rotate the thread owner instead of waiting on it
    // (waitForThreadRunReservation's canRotateSuspendedOwner path).
    it.skip('keeps blocking a same-agent contender during a partial resume with sibling suspensions', async () => {
      const runtime = new AgentThreadStreamRuntime();
      const pubsub = new EventEmitterPubSub();
      const publish = vi.spyOn(pubsub, 'publish');
      const agent = { id: 'same-agent-partial-resume-agent' } as Agent<any, any, any, any>;
      const runId = 'same-agent-partial-resume-run';
      const options = { memory: { thread: threadId, resource: resourceId } } as any;

      let finishRun!: () => void;
      const finished = new Promise<void>(resolve => {
        finishRun = resolve;
      });
      let parts!: ReadableStreamDefaultController<unknown>;
      const output = {
        runId,
        status: 'running',
        fullStream: new ReadableStream({
          start(controller) {
            parts = controller;
          },
        }),
        _waitUntilFinished: () => finished,
      } as any;
      await runtime.registerRun(agent, output, options, pubsub, { continuation: 'across-suspension' });

      // Two sibling tool calls suspend within the same segment.
      parts.enqueue({ type: 'tool-call-approval', runId, payload: { toolCallId: 'call-1', toolName: 'one' } });
      parts.enqueue({ type: 'tool-call-approval', runId, payload: { toolCallId: 'call-2', toolName: 'two' } });
      await vi.waitFor(() =>
        expect(
          publish.mock.calls.filter(([, event]) => (event as any).data?.part?.type === 'tool-call-approval'),
        ).toHaveLength(2),
      );
      output.status = 'suspended';

      // Fully suspended: a same-agent contender must not wait on human input.
      await withTimeout(
        runtime.waitForCrossAgentThreadRun(agent, options, pubsub),
        'Fully suspended wait should resolve immediately',
      );

      // Resume only call-1. call-2 stays suspended, but the resumed segment is
      // actively executing — a new same-agent run must wait for it.
      const resumed = {
        runId,
        status: 'running',
        consumeStream: async () => {},
      } as any;
      expect(runtime.continueRun(agent, resumed, { ...options, toolCallId: 'call-1' }, pubsub)).toBe(true);

      let resolved = false;
      const wait = runtime.waitForCrossAgentThreadRun(agent, options, pubsub).then(() => {
        resolved = true;
      });
      await new Promise(resolve => setTimeout(resolve, 25));
      expect(resolved).toBe(false);

      // The resumed segment settles; the contender is released.
      resumed.status = 'success';
      output.status = 'success';
      finishRun();
      await withTimeout(wait, 'Timed out waiting for the partial-resume wait to release');
      expect(resolved).toBe(true);
    });

    it('still waits on a different-agent running record', async () => {
      const runtime = new AgentThreadStreamRuntime();
      const owner = { id: 'other-agent-owner' } as Agent<any, any, any, any>;
      const run = await registerRunningRun(runtime, owner, 'other-agent-run');

      let resolved = false;
      const wait = runtime
        .waitForCrossAgentThreadRun({ id: 'other-agent-contender' } as Agent<any, any, any, any>, {
          memory: { thread: threadId, resource: resourceId },
        })
        .then(() => {
          resolved = true;
        });
      await new Promise(resolve => setTimeout(resolve, 25));
      expect(resolved).toBe(false);

      run.finish();
      await withTimeout(wait, 'Timed out waiting for the cross-agent wait to release');
      expect(resolved).toBe(true);
    });

    it('serializes two concurrent agent.stream() calls on the same thread', async () => {
      let concurrent = 0;
      let maxConcurrent = 0;
      const model = new MockLanguageModelV2({
        doStream: async () => {
          concurrent += 1;
          maxConcurrent = Math.max(maxConcurrent, concurrent);
          await new Promise(resolve => setTimeout(resolve, 25));
          concurrent -= 1;
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: convertArrayToReadableStream([
              { type: 'stream-start', warnings: [] },
              { type: 'response-metadata', id: 'id-0', modelId: 'mock-model-id', timestamp: new Date(0) },
              { type: 'text-start', id: 'text-1' },
              { type: 'text-delta', id: 'text-1', delta: 'serialized response' },
              { type: 'text-end', id: 'text-1' },
              {
                type: 'finish',
                finishReason: 'stop',
                usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
              },
            ]),
          };
        },
      });
      const agent = new Agent({
        id: 'concurrent-stream-agent',
        name: 'Concurrent Stream Agent',
        instructions: 'Test',
        model,
      });
      const memory = { thread: 'concurrent-stream-thread', resource: 'concurrent-stream-user' };

      const [first, second] = await Promise.all([
        agent.stream('first message', { memory }),
        agent.stream('second message', { memory }),
      ]);
      await Promise.all([first.consumeStream(), second.consumeStream()]);

      expect(maxConcurrent).toBe(1);
    });

    it('does not reserve or wait for a read-only same-thread stream while a parent run is active', async () => {
      const { model, releaseFirst, getStreamCount } = createBlockingFirstTextStreamModel(
        'parent response',
        'read-only response',
      );
      const agent = new Agent({
        id: 'read-only-reentrant-stream-agent',
        name: 'Read-only Reentrant Stream Agent',
        instructions: 'Test',
        model,
      });
      const memory = { thread: 'read-only-reentrant-thread', resource: 'read-only-reentrant-user' };
      const target = { threadId: memory.thread, resourceId: memory.resource };

      const parent = await agent.stream('parent message', { memory });
      const parentConsumption = parent.consumeStream();
      try {
        await waitForCondition(() => getStreamCount() === 1);
        expect(agentThreadStreamRuntime.getActiveThreadRunId(target)).toBe(parent.runId);

        const readOnly = await withTimeout(
          agent.stream('structure the parent response', {
            memory: { ...memory, options: { readOnly: true } },
          }),
          'Read-only same-thread stream waited for its active parent',
        );
        await withTimeout(readOnly.consumeStream(), 'Read-only same-thread stream did not complete');

        expect(getStreamCount()).toBe(2);
        expect(agentThreadStreamRuntime.getActiveThreadRunId(target)).toBe(parent.runId);
      } finally {
        releaseFirst();
        await parentConsumption;
      }
    });
  });

  it('does not abort a successor run when the expected run has completed', async () => {
    const pubsub = new ControlledLeasePubSub();
    const runtime = new AgentThreadStreamRuntime();
    const resourceId = 'conditional-abort-resource';
    const threadId = 'conditional-abort-thread';
    const successorRunId = 'run-b';
    const options = runtime.prepareRunOptions(
      { runId: successorRunId, memory: { resource: resourceId, thread: threadId } } as any,
      pubsub,
    );

    runtime.registerRun(
      { id: 'conditional-abort-agent' } as Agent<any, any, any, any>,
      {
        runId: successorRunId,
        status: 'running',
        fullStream: (async function* () {})(),
        _waitUntilFinished: () => new Promise<void>(() => {}),
      } as any,
      options,
      pubsub,
    );

    const agent = { id: 'conditional-abort-agent' } as Agent<any, any, any, any>;
    const pending = runtime.sendSignal(
      agent,
      { type: 'user-message', contents: 'queued for successor' },
      { resourceId, threadId },
      pubsub,
    );
    await pending.accepted;

    // A stale clear-on-abort must neither stop the successor nor drop its queued input.
    expect(
      runtime.abortThread({ resourceId, threadId, expectedRunId: 'run-a', clearPendingSignals: true }, pubsub),
    ).toBe(false);
    expect(options.abortSignal?.aborted).toBe(false);
    expect(runtime.getActiveThreadRunId({ resourceId, threadId }, pubsub)).toBe(successorRunId);
    expect(
      runtime.cancelQueuedMessages(agent, { resourceId, threadId, signalIds: [pending.signal.id] }, pubsub),
    ).toEqual({ cancelledSignalIds: [pending.signal.id] });

    expect(runtime.abortThread({ resourceId, threadId, expectedRunId: successorRunId }, pubsub)).toBe(true);
    expect(options.abortSignal?.aborted).toBe(true);
  });

  it('routes remote abort requests to only the live lease owner', async () => {
    const pubsub = new ControlledLeasePubSub();
    const ownerRuntime = new AgentThreadStreamRuntime();
    const followerRuntime = new AgentThreadStreamRuntime();
    const key = 'remote-abort-resource\u0000remote-abort-thread';
    const runId = 'remote-abort-run';
    const ownerSubscription = await ownerRuntime.subscribeToThread(
      { id: 'remote-abort-agent' } as Agent<any, any, any, any>,
      { resourceId: 'remote-abort-resource', threadId: 'remote-abort-thread' },
      pubsub,
    );
    const followerSubscription = await followerRuntime.subscribeToThread(
      { id: 'remote-abort-agent' } as Agent<any, any, any, any>,
      { resourceId: 'remote-abort-resource', threadId: 'remote-abort-thread' },
      pubsub,
    );
    expect(followerSubscription.abort()).toBe(false);

    const options = ownerRuntime.prepareRunOptions(
      { runId, memory: { resource: 'remote-abort-resource', thread: 'remote-abort-thread' } } as any,
      pubsub,
    );
    ownerRuntime.registerRun(
      { id: 'remote-abort-agent' } as Agent<any, any, any, any>,
      {
        runId,
        status: 'running',
        fullStream: (async function* () {})(),
        _waitUntilFinished: () => new Promise<void>(() => {}),
      } as any,
      options,
      pubsub,
    );
    await pubsub.flush();
    await waitForCondition(() => followerSubscription.activeRunId() === runId);
    const publishedBeforeMismatch = pubsub.publishedData.length;
    expect(
      followerRuntime.abortThread(
        {
          resourceId: 'remote-abort-resource',
          threadId: 'remote-abort-thread',
          expectedRunId: 'completed-run',
        },
        pubsub,
      ),
    ).toBe(false);
    expect(pubsub.publishedData).toHaveLength(publishedBeforeMismatch);

    const leaseOwner = pubsub.owners.get(key)!;
    const streamId = pubsub.publishedData.find(
      data => data?.type === 'run-registered' && data.runId === runId,
    )!.streamId;
    await pubsub.publish(`agent.thread-stream.${encodeURIComponent(key)}`, {
      type: 'run-abort-requested',
      runId,
      data: {
        type: 'run-abort-requested',
        runId,
        streamId,
        leaseOwner: `mastra-thread-owner:${JSON.stringify([runId, 'forged-runtime', 'attempt'])}`,
      },
    });
    await pubsub.flush();
    expect(options.abortSignal?.aborted).toBe(false);
    expect(pubsub.owners.get(key)).toBe(leaseOwner);
    const transferredOwner = `mastra-thread-owner:${JSON.stringify([runId, 'new-owner-runtime', 'attempt'])}`;
    pubsub.owners.set(key, transferredOwner);
    await pubsub.publish(`agent.thread-stream.${encodeURIComponent(key)}`, {
      type: 'run-abort-requested',
      runId,
      data: { type: 'run-abort-requested', runId, streamId, leaseOwner },
    });
    await pubsub.flush();
    expect(options.abortSignal?.aborted).toBe(false);
    pubsub.owners.set(key, leaseOwner);
    // The real remote abort, addressed to the expected run (upstream), reaches
    // the owner only through the authenticated lease-owner path (fork).
    expect(
      followerRuntime.abortThread(
        { resourceId: 'remote-abort-resource', threadId: 'remote-abort-thread', expectedRunId: runId },
        pubsub,
      ),
    ).toBe(true);
    expect(options.abortSignal?.aborted).toBe(false);
    await pubsub.flush();
    await waitForCondition(() => options.abortSignal?.aborted === true);
    const requestIndex = pubsub.publishedData.findIndex(data => data?.type === 'run-abort-requested');
    const terminalIndex = pubsub.publishedData.findIndex(data => data?.type === 'run-aborted');
    expect(requestIndex).toBeGreaterThanOrEqual(0);
    expect(terminalIndex).toBeGreaterThan(requestIndex);
    expect(pubsub.owners.get(key)).toBeUndefined();

    const terminalCount = pubsub.publishedData.filter(data => data?.type === 'run-aborted').length;
    await pubsub.publish(`agent.thread-stream.${encodeURIComponent(key)}`, {
      type: 'run-abort-requested',
      runId,
      data: { type: 'run-abort-requested', runId, streamId: 'stale-stream' },
    });
    await pubsub.flush();
    await nextTick();
    expect(pubsub.publishedData.filter(data => data?.type === 'run-aborted')).toHaveLength(terminalCount);
    ownerSubscription.unsubscribe();
    followerSubscription.unsubscribe();
  });

  it('keeps a remote owner run alive when a thread teardown aborts locally', async () => {
    const pubsub = new ControlledLeasePubSub();
    const ownerRuntime = new AgentThreadStreamRuntime();
    const followerRuntime = new AgentThreadStreamRuntime();
    const resourceId = 'local-abort-resource';
    const threadId = 'local-abort-thread';
    const key = `${resourceId}\u0000${threadId}`;
    const runId = 'local-abort-run';
    // Fork (PF-4402): registerRun acquires the lease under a process-attempt
    // owner token; pre-seeding a raw run-id owner would make it fail closed.
    const ownerSubscription = await ownerRuntime.subscribeToThread(
      { id: 'local-abort-agent' } as Agent<any, any, any, any>,
      { resourceId, threadId },
      pubsub,
    );
    const followerSubscription = await followerRuntime.subscribeToThread(
      { id: 'local-abort-agent' } as Agent<any, any, any, any>,
      { resourceId, threadId },
      pubsub,
    );

    const options = ownerRuntime.prepareRunOptions(
      { runId, memory: { resource: resourceId, thread: threadId } } as any,
      pubsub,
    );
    ownerRuntime.registerRun(
      { id: 'local-abort-agent' } as Agent<any, any, any, any>,
      {
        runId,
        status: 'running',
        fullStream: (async function* () {})(),
        _waitUntilFinished: () => new Promise<void>(() => {}),
      } as any,
      options,
      pubsub,
    );
    await pubsub.flush();
    await waitForCondition(() => followerSubscription.activeRunId() === runId);

    // Unbinding a thread (a follower running `/new`, or a session teardown) stops
    // this process's own run and must not reach the owner's run over PubSub.
    const publishedBeforeLocalAbort = pubsub.publishedData.length;
    expect(followerSubscription.abort({ localOnly: true })).toBe(false);
    await pubsub.flush();
    await nextTick();
    expect(options.abortSignal?.aborted).toBe(false);
    expect(pubsub.publishedData).toHaveLength(publishedBeforeLocalAbort);
    expect(pubsub.publishedData.some(data => data?.type === 'run-abort-requested')).toBe(false);
    // The owner still holds the live lease, so a real abort from this follower
    // would have been forwarded — the suppression above is what kept it alive.
    expect(decodeLeaseOwnerRunId(pubsub.owners.get(key))).toBe(runId);

    ownerSubscription.unsubscribe();
    followerSubscription.unsubscribe();
  });

  it('routes remote abort requests to a parked suspended run on the lease owner', async () => {
    const pubsub = new ControlledLeasePubSub();
    const ownerRuntime = new AgentThreadStreamRuntime();
    const followerRuntime = new AgentThreadStreamRuntime();
    const resourceId = 'parked-remote-abort-resource';
    const threadId = 'parked-remote-abort-thread';
    const key = `${resourceId}\u0000${threadId}`;
    const runId = 'parked-remote-abort-run';
    const agent = { id: 'parked-remote-abort-agent' } as Agent<any, any, any, any>;
    // The lease is acquired by registerRun under the fork's process-attempt
    // owner token; pre-seeding a raw run-id owner would make that acquisition
    // fail closed before the run ever registers.
    const ownerSubscription = await ownerRuntime.subscribeToThread(agent, { resourceId, threadId }, pubsub);
    const followerSubscription = await followerRuntime.subscribeToThread(agent, { resourceId, threadId }, pubsub);
    const iterator = ownerSubscription.stream[Symbol.asyncIterator]();
    let finishRun!: () => void;
    const finished = new Promise<void>(resolve => {
      finishRun = resolve;
    });

    try {
      ownerRuntime.registerRun(
        agent,
        {
          runId,
          status: 'suspended',
          fullStream: new ReadableStream({
            start(controller) {
              controller.enqueue({ type: 'start', runId });
              controller.enqueue({
                type: 'tool-call-suspended',
                runId,
                payload: { toolCallId: 'parked-remote-abort-call', toolName: 'ask_user' },
              });
              controller.close();
            },
          }),
          _waitUntilFinished: () => finished,
        } as any,
        { memory: { thread: threadId, resource: resourceId } } as any,
        pubsub,
      );
      await withTimeout(iterator.next(), 'Timed out waiting for parked remote run start');
      await withTimeout(iterator.next(), 'Timed out waiting for parked remote suspension chunk');
      await pubsub.flush();
      await waitForCondition(() => followerSubscription.activeRunId() === runId);

      // Park the run for real: the completion watcher evicts it from
      // preparedRunsById and marks its record lifecycle 'suspended'.
      finishRun();
      await pubsub.flush();
      await waitForCondition(() =>
        pubsub.publishedData.some(data => data?.type === 'run-suspended' && data.runId === runId),
      );
      expect(ownerRuntime.hasThreadRun(runId, pubsub)).toBe(true);
      expect(ownerRuntime.getActiveThreadRunId({ resourceId, threadId }, pubsub)).toBe(runId);

      // A forged request with a stale streamId is still dropped: the parked-run
      // guard relaxation must not weaken the ownership checks.
      await pubsub.publish(`agent.thread-stream.${encodeURIComponent(key)}`, {
        type: 'run-abort-requested',
        runId,
        data: { type: 'run-abort-requested', runId, streamId: 'stale-stream' },
      });
      await pubsub.flush();
      await nextTick();
      expect(pubsub.publishedData.some(data => data?.type === 'run-aborted')).toBe(false);
      expect(ownerRuntime.hasThreadRun(runId, pubsub)).toBe(true);
      // The fork's lease owner is a process-attempt token wrapping the run id;
      // decode it to assert the parked run still holds the lease.
      expect(decodeLeaseOwnerRunId(pubsub.owners.get(key))).toBe(runId);

      // The real remote abort releases the parked run on the owner.
      expect(followerSubscription.abort()).toBe(true);
      await pubsub.flush();
      await waitForCondition(() =>
        pubsub.publishedData.some(data => data?.type === 'run-aborted' && data.runId === runId),
      );
      const requestIndex = pubsub.publishedData.findIndex(
        data => data?.type === 'run-abort-requested' && data.streamId !== 'stale-stream',
      );
      const terminalIndex = pubsub.publishedData.findIndex(data => data?.type === 'run-aborted');
      expect(requestIndex).toBeGreaterThanOrEqual(0);
      expect(terminalIndex).toBeGreaterThan(requestIndex);
      expect(ownerRuntime.hasThreadRun(runId, pubsub)).toBe(false);
      expect(ownerRuntime.getActiveThreadRunId({ resourceId, threadId }, pubsub)).toBeUndefined();
      expect(ownerRuntime.getThreadState({ resourceId, threadId }, pubsub)).toBe('idle');
      expect(pubsub.owners.get(key)).toBeUndefined();
      expect(ownerRuntime.abortThread({ resourceId, threadId }, pubsub)).toBe(false);
    } finally {
      finishRun();
      ownerSubscription.unsubscribe();
      followerSubscription.unsubscribe();
    }
  });

  it.each(['control-first', 'observer-first'] as const)(
    'admits a remote signal exactly once regardless of %s callback delivery',
    async order => {
      const pubsub = order === 'observer-first' ? new ReverseDeliveryPubSub() : new RetainedAsyncCallbackPubSub();
      const runtime = new AgentThreadStreamRuntime();
      const target = {
        resourceId: 'admission-order-resource',
        threadId: `admission-order-${order}`,
      };
      const runId = 'admission-order-run';
      const signal = {
        id: 'admission-order-signal',
        type: 'user-message' as const,
        contents: 'once regardless of callback order',
      };
      let finishRun!: () => void;
      const finished = new Promise<void>(resolve => {
        finishRun = resolve;
      });
      const agent = { id: 'admission-order-agent' } as Agent<any, any, any, any>;
      const subscription = await runtime.subscribeToThread(agent, target, pubsub);
      try {
        runtime.registerRun(
          agent,
          {
            runId,
            status: 'running',
            fullStream: (async function* () {})(),
            _waitUntilFinished: () => finished,
          } as any,
          { runId, memory: { resource: target.resourceId, thread: target.threadId } } as any,
          pubsub,
        );
        const topic = `agent.thread-stream.${encodeURIComponent(`${target.resourceId}\u0000${target.threadId}`)}`;
        const remoteSignal = {
          type: 'signal-enqueued',
          runId,
          signal,
          sourceId: 'remote-admission-source',
          preRun: false,
        };
        await pubsub.publish(topic, { type: 'signal-enqueued', runId, data: remoteSignal });
        await pubsub.flush();
        // Exactly one execution-queue admission, whichever subscription the
        // pubsub handed the delivery to first.
        expect(runtime.drainPendingSignals(runId, pubsub)).toMatchObject([
          { id: signal.id, contents: signal.contents },
        ]);
        // At-least-once redelivery after the queue drained must not re-admit.
        await pubsub.publish(topic, { type: 'signal-enqueued', runId, data: remoteSignal });
        await pubsub.flush();
        expect(runtime.drainPendingSignals(runId, pubsub)).toEqual([]);
        // A conflicting payload under the same stable id is rejected fail-closed.
        await pubsub.publish(topic, {
          type: 'signal-enqueued',
          runId,
          data: { ...remoteSignal, signal: { ...signal, contents: 'conflicting replay' } },
        });
        await pubsub.flush();
        expect(runtime.drainPendingSignals(runId, pubsub)).toEqual([]);
      } finally {
        finishRun();
        subscription.unsubscribe();
      }
    },
  );

  it('preserves queued input when the run-aborted observer event is processed before the handoff drain', async () => {
    const scope = { resourceId: 'observer-preserve-user', threadId: 'observer-preserve-thread' };
    const pubsub = new ControlledLeasePubSub();
    const memory = new MockMemory();
    const { model, releaseFirst, getStreamCount } = createBlockingFirstTextStreamModel(
      'first response',
      'follow-up response',
    );
    const agent = new Agent({
      id: 'observer-preserve',
      name: 'Observer preserve',
      instructions: 'Test',
      model,
      memory,
      pubsub,
    });
    const subscription = await agent.subscribeToThread(scope);
    let releaseTransfer!: () => void;
    const transferGate = new Promise<void>(resolve => {
      releaseTransfer = resolve;
    });
    try {
      const first = await agent.stream('initial', { memory: { resource: scope.resourceId, thread: scope.threadId } });
      await vi.waitFor(() => expect(getStreamCount()).toBe(1));
      await agent.sendSignal({ type: 'user-message', contents: 'preserved follow-up' }, scope).accepted;
      // Gate the abort finalizer's queued-work handoff so the authenticated
      // run-aborted terminal is published and fully processed by the thread
      // observer BEFORE any follow-up input is drained.
      pubsub.transferLeaseWait = transferGate;
      expect(subscription.abort()).toBe(true);
      await vi.waitFor(() => expect(pubsub.publishedData.some(data => data?.type === 'run-aborted')).toBe(true), {
        timeout: 5_000,
      });
      await pubsub.flush();
      // The observer has processed the abort terminal; the preserved input
      // must still be queued for the gated handoff rather than deleted.
      expect(getStreamCount()).toBe(1);
      releaseFirst();
      await first.text;
      releaseTransfer();
      await vi.waitFor(() => expect(getStreamCount()).toBe(2));
      const prompt = JSON.stringify(model.doStreamCalls[1]?.prompt);
      expect(prompt).toContain('preserved follow-up');
      await vi.waitFor(() => expect(pubsub.owners.get(`${scope.resourceId}\u0000${scope.threadId}`)).toBeUndefined());
    } finally {
      releaseFirst();
      releaseTransfer();
      subscription.unsubscribe();
    }
  });

  it('retains a truthful delivery failure receipt when a parked abort terminal cannot publish', async () => {
    const pubsub = new ControlledLeasePubSub();
    pubsub.rejectPublishedTypes.add('run-aborted');
    const runtime = new AgentThreadStreamRuntime();
    const resourceId = 'parked-delivery-failure-resource';
    const threadId = 'parked-delivery-failure-thread';
    const runId = 'parked-delivery-failure-run';
    const agent = { id: 'parked-delivery-failure-agent' } as Agent<any, any, any, any>;
    let finishRun!: () => void;
    const finished = new Promise<void>(resolve => {
      finishRun = resolve;
    });
    const subscription = await runtime.subscribeToThread(agent, { resourceId, threadId }, pubsub);
    try {
      runtime.registerRun(
        agent,
        {
          runId,
          status: 'suspended',
          fullStream: new ReadableStream({
            start(controller) {
              controller.enqueue({ type: 'start', runId });
              controller.enqueue({
                type: 'tool-call-suspended',
                runId,
                payload: { toolCallId: 'parked-delivery-failure-call', toolName: 'ask_user' },
              });
              controller.close();
            },
          }),
          _waitUntilFinished: () => finished,
        } as any,
        { memory: { thread: threadId, resource: resourceId } } as any,
        pubsub,
      );
      finishRun();
      await pubsub.flush();
      await waitForCondition(() =>
        pubsub.publishedData.some(data => data?.type === 'run-suspended' && data.runId === runId),
      );
      expect(runtime.hasThreadRun(runId, pubsub)).toBe(true);

      expect(runtime.abortRun(runId, pubsub)).toBe(true);
      // Local teardown still completes after the bounded authenticated
      // terminal publication fails.
      await vi.waitFor(() => expect(runtime.hasThreadRun(runId, pubsub)).toBe(false), { timeout: 5_000 });
      // The delivery failure is retained truthfully: the same aborted attempt
      // keeps its abort message with the original failure as `cause`.
      const waitError = await runtime.waitForRunOutput(runId, pubsub).catch(error => error as Error);
      expect(waitError).toBeInstanceOf(Error);
      expect(waitError.message).toContain('has been aborted');
      expect(waitError.cause).toBeInstanceOf(Error);
      expect((waitError.cause as Error).message).toContain('run-aborted');
    } finally {
      finishRun();
      subscription.unsubscribe();
    }
  });

  it('preserves a same-run successor registered while a parked abort terminal is in flight', async () => {
    const pubsub = new GatedTypePubSub();
    const runtime = new AgentThreadStreamRuntime();
    const resourceId = 'parked-successor-resource';
    const threadId = 'parked-successor-thread';
    const key = `${resourceId}\u0000${threadId}`;
    const runId = 'parked-successor-run';
    const agent = { id: 'parked-successor-agent' } as Agent<any, any, any, any>;
    let finishRun!: () => void;
    const finished = new Promise<void>(resolve => {
      finishRun = resolve;
    });
    const subscription = await runtime.subscribeToThread(agent, { resourceId, threadId }, pubsub);
    const gate = pubsub.gateType('run-aborted');
    try {
      runtime.registerRun(
        agent,
        {
          runId,
          status: 'suspended',
          fullStream: new ReadableStream({
            start(controller) {
              controller.enqueue({ type: 'start', runId });
              controller.enqueue({
                type: 'tool-call-suspended',
                runId,
                payload: { toolCallId: 'parked-successor-call', toolName: 'ask_user' },
              });
              controller.close();
            },
          }),
          _waitUntilFinished: () => finished,
        } as any,
        { memory: { thread: threadId, resource: resourceId } } as any,
        pubsub,
      );
      finishRun();
      await pubsub.flush();
      await waitForCondition(() =>
        pubsub.publishedData.some(data => data?.type === 'run-suspended' && data.runId === runId),
      );
      expect(runtime.hasThreadRun(runId, pubsub)).toBe(true);

      // The parked abort starts; its authenticated terminal publication is held.
      expect(runtime.abortRun(runId, pubsub)).toBe(true);
      // A same-runId successor (strict recovery re-registration) registers
      // while the terminal is in flight.
      const successorFinished = new Promise<void>(() => {});
      await runtime.registerRun(
        agent,
        {
          runId,
          status: 'running',
          fullStream: (async function* () {})(),
          _waitUntilFinished: () => successorFinished,
        } as any,
        { runId, memory: { thread: threadId, resource: resourceId } } as any,
        pubsub,
        { strict: true },
      );
      expect(runtime.hasThreadRun(runId, pubsub)).toBe(true);
      expect(runtime.getActiveThreadRunId({ resourceId, threadId }, pubsub)).toBe(runId);
      const successorOwner = pubsub.owners.get(key);
      expect(decodeLeaseOwnerRunId(successorOwner)).toBe(runId);

      // Release the terminal: the retired record's generation-fenced cleanup
      // must not remove the successor's records or release its lease.
      gate.release();
      await pubsub.flush();
      await waitForCondition(() =>
        pubsub.publishedData.some(data => data?.type === 'run-aborted' && data.runId === runId),
      );
      expect(runtime.hasThreadRun(runId, pubsub)).toBe(true);
      expect(runtime.getActiveThreadRunId({ resourceId, threadId }, pubsub)).toBe(runId);
      expect(decodeLeaseOwnerRunId(pubsub.owners.get(key))).toBe(runId);
    } finally {
      gate.release();
      finishRun();
      subscription.unsubscribe();
    }
  });

  it('keeps a same-run successor intact when a parked abort terminal fails after its registration', async () => {
    const pubsub = new GatedRejectingTypePubSub();
    pubsub.rejectOnRelease.add('run-aborted');
    const runtime = new AgentThreadStreamRuntime();
    const resourceId = 'parked-failure-successor-resource';
    const threadId = 'parked-failure-successor-thread';
    const key = `${resourceId}\u0000${threadId}`;
    const runId = 'parked-failure-successor-run';
    const agent = { id: 'parked-failure-successor-agent' } as Agent<any, any, any, any>;
    let finishRun!: () => void;
    const finished = new Promise<void>(resolve => {
      finishRun = resolve;
    });
    const subscription = await runtime.subscribeToThread(agent, { resourceId, threadId }, pubsub);
    const gate = pubsub.gateType('run-aborted');
    try {
      runtime.registerRun(
        agent,
        {
          runId,
          status: 'suspended',
          fullStream: new ReadableStream({
            start(controller) {
              controller.enqueue({ type: 'start', runId });
              controller.enqueue({
                type: 'tool-call-suspended',
                runId,
                payload: { toolCallId: 'parked-failure-successor-call', toolName: 'ask_user' },
              });
              controller.close();
            },
          }),
          _waitUntilFinished: () => finished,
        } as any,
        { memory: { thread: threadId, resource: resourceId } } as any,
        pubsub,
      );
      finishRun();
      await pubsub.flush();
      await waitForCondition(() =>
        pubsub.publishedData.some(data => data?.type === 'run-suspended' && data.runId === runId),
      );
      expect(runtime.hasThreadRun(runId, pubsub)).toBe(true);

      // The parked abort starts; its authenticated terminal publication is
      // held and will REJECT after delivery once released.
      expect(runtime.abortRun(runId, pubsub)).toBe(true);
      // A same-runId successor (strict recovery re-registration) registers
      // while the terminal is in flight.
      const successorFinished = new Promise<void>(() => {});
      await runtime.registerRun(
        agent,
        {
          runId,
          status: 'running',
          fullStream: (async function* () {})(),
          _waitUntilFinished: () => successorFinished,
        } as any,
        { runId, memory: { thread: threadId, resource: resourceId } } as any,
        pubsub,
        { strict: true },
      );
      expect(runtime.hasThreadRun(runId, pubsub)).toBe(true);
      expect(runtime.getActiveThreadRunId({ resourceId, threadId }, pubsub)).toBe(runId);
      const successorOwner = pubsub.owners.get(key);
      expect(decodeLeaseOwnerRunId(successorOwner)).toBe(runId);

      // Release the terminal: it delivers, then its publication fails. The
      // retired attempt may settle only its own output-scoped receipt — the
      // run-id receipt and the successor's records, token, active-thread
      // identity, and prepared state must stay untouched by the stale failure.
      gate.release();
      await pubsub.flush();
      await waitForCondition(() =>
        pubsub.publishedData.some(data => data?.type === 'run-aborted' && data.runId === runId),
      );
      expect(runtime.hasThreadRun(runId, pubsub)).toBe(true);
      expect(runtime.getActiveThreadRunId({ resourceId, threadId }, pubsub)).toBe(runId);
      expect(decodeLeaseOwnerRunId(pubsub.owners.get(key))).toBe(runId);
      // A run-output lookup resolves through the successor's record instead
      // of being rejected by the retired attempt's delivery failure.
      expect(runtime.getRunOutput(runId, pubsub)).toBeDefined();
    } finally {
      gate.release();
      finishRun();
      subscription.unsubscribe();
    }
  });

  it('keeps a prepared same-run successor registrable when a parked abort terminal fails in its preparation window', async () => {
    const pubsub = new GatedRejectingTypePubSub();
    pubsub.rejectOnRelease.add('run-aborted');
    const runtime = new AgentThreadStreamRuntime();
    const resourceId = 'parked-failure-prepared-resource';
    const threadId = 'parked-failure-prepared-thread';
    const key = `${resourceId}\u0000${threadId}`;
    const runId = 'parked-failure-prepared-run';
    const agent = { id: 'parked-failure-prepared-agent' } as Agent<any, any, any, any>;
    let finishRun!: () => void;
    const finished = new Promise<void>(resolve => {
      finishRun = resolve;
    });
    const subscription = await runtime.subscribeToThread(agent, { resourceId, threadId }, pubsub);
    const gate = pubsub.gateType('run-aborted');
    try {
      runtime.registerRun(
        agent,
        {
          runId,
          status: 'suspended',
          fullStream: new ReadableStream({
            start(controller) {
              controller.enqueue({ type: 'start', runId });
              controller.enqueue({
                type: 'tool-call-suspended',
                runId,
                payload: { toolCallId: 'parked-failure-prepared-call', toolName: 'ask_user' },
              });
              controller.close();
            },
          }),
          _waitUntilFinished: () => finished,
        } as any,
        { memory: { thread: threadId, resource: resourceId } } as any,
        pubsub,
      );
      finishRun();
      await pubsub.flush();
      await waitForCondition(() =>
        pubsub.publishedData.some(data => data?.type === 'run-suspended' && data.runId === runId),
      );
      expect(runtime.hasThreadRun(runId, pubsub)).toBe(true);

      // The parked abort starts; its authenticated terminal publication is
      // held and will REJECT after delivery once released.
      expect(runtime.abortRun(runId, pubsub)).toBe(true);
      // A same-runId successor PREPARES (not yet registers) while the terminal
      // is in flight: its prepared state owns the run id's next attempt.
      const prepared = runtime.prepareRunOptions(
        { runId, memory: { thread: threadId, resource: resourceId } } as any,
        pubsub,
      );
      expect(prepared.abortSignal).toBeDefined();

      // Release the terminal: it delivers, then its publication fails. The
      // retired attempt must not write the run-id receipt for the stale
      // failure — the abort message stays clean of the delivery error.
      gate.release();
      await pubsub.flush();
      await waitForCondition(() =>
        pubsub.publishedData.some(data => data?.type === 'run-aborted' && data.runId === runId),
      );
      await vi.waitFor(() => expect(runtime.hasThreadRun(runId, pubsub)).toBe(false), { timeout: 5_000 });
      const waitError = await runtime.waitForRunOutput(runId, pubsub).catch(error => error as Error);
      expect(waitError).toBeInstanceOf(Error);
      expect(waitError.message).toContain('has been aborted');
      expect(waitError.cause).toBeUndefined();

      // The prepared successor still owns the next attempt: it registers
      // cleanly after the retired attempt's fenced cleanup.
      const successorFinished = new Promise<void>(() => {});
      await runtime.registerRun(
        agent,
        {
          runId,
          status: 'running',
          fullStream: (async function* () {})(),
          _waitUntilFinished: () => successorFinished,
        } as any,
        { runId, memory: { thread: threadId, resource: resourceId } } as any,
        pubsub,
        { strict: true },
      );
      expect(runtime.hasThreadRun(runId, pubsub)).toBe(true);
      expect(runtime.getActiveThreadRunId({ resourceId, threadId }, pubsub)).toBe(runId);
      expect(decodeLeaseOwnerRunId(pubsub.owners.get(key))).toBe(runId);
    } finally {
      gate.release();
      finishRun();
      subscription.unsubscribe();
    }
  });

  it('rejects only the retired output receipt when a parked abort terminal fails after a successor reservation', async () => {
    const pubsub = new GatedRejectingTypePubSub();
    pubsub.rejectOnRelease.add('run-aborted');
    const runtime = new AgentThreadStreamRuntime();
    const resourceId = 'parked-receipt-reservation-resource';
    const threadId = 'parked-receipt-reservation-thread';
    const key = `${resourceId}\u0000${threadId}`;
    const runId = 'parked-receipt-reservation-run';
    const agent = { id: 'parked-receipt-reservation-agent' } as Agent<any, any, any>;
    let finishRun!: () => void;
    const finished = new Promise<void>(resolve => {
      finishRun = resolve;
    });
    const subscription = await runtime.subscribeToThread(agent, { resourceId, threadId }, pubsub);
    const gate = pubsub.gateType('run-aborted');
    const iterator = subscription.stream[Symbol.asyncIterator]();
    const oldOutput = {
      runId,
      status: 'suspended',
      fullStream: new ReadableStream({
        start(controller) {
          controller.enqueue({ type: 'start', runId });
          controller.enqueue({
            type: 'tool-call-suspended',
            runId,
            payload: { toolCallId: 'parked-receipt-reservation-call', toolName: 'ask_user' },
          });
          controller.close();
        },
      }),
      _waitUntilFinished: () => finished,
    } as any;
    try {
      runtime.registerRun(agent, oldOutput, { memory: { thread: threadId, resource: resourceId } } as any, pubsub);
      finishRun();
      await pubsub.flush();
      await waitForCondition(() =>
        pubsub.publishedData.some(data => data?.type === 'run-suspended' && data.runId === runId),
      );
      expect(runtime.hasThreadRun(runId, pubsub)).toBe(true);

      // Drain the retired output through the subscription: its suspension
      // settled, so a later output-drain wait on it would take the
      // drained-output shortcut rather than waiting on a terminal promise.
      await expect(iterator.next()).resolves.toMatchObject({ value: { type: 'start', runId } });
      await expect(iterator.next()).resolves.toMatchObject({
        value: { type: 'tool-call-suspended', payload: { toolCallId: 'parked-receipt-reservation-call' } },
      });
      // Resume the generator once more so the closed source marks the segment
      // fully drained; it then parks awaiting the next enqueued run.
      void iterator.next();
      await vi.waitFor(async () => {
        await subscription._waitForOutputDrain!(oldOutput);
      });

      // The parked abort starts; its authenticated terminal publication is
      // held and will REJECT after delivery once released.
      expect(runtime.abortRun(runId, pubsub)).toBe(true);
      // A real same-run reservation removes the completed predecessor record
      // while the terminal is in flight: the reservation owns the run id now.
      const releaseReservation = runtime.reserveRun(
        { runId, memory: { thread: threadId, resource: resourceId } } as any,
        pubsub,
        agent.id,
      );
      expect(releaseReservation).toBeTypeOf('function');
      expect(runtime.hasThreadRun(runId, pubsub)).toBe(false);
      // A successor reservation waiter parks on the reserved run id's
      // lifecycle; the retired attempt must not settle it.
      let reservationSettled = false;
      const reservationWait = runtime
        .waitForThreadRunReservation(
          {
            runId: 'parked-receipt-successor-waiter-run',
            memory: { thread: threadId, resource: resourceId },
          } as any,
          pubsub,
          'parked-receipt-successor-waiter-agent',
        )
        .then(
          () => {
            reservationSettled = true;
          },
          () => {
            reservationSettled = true;
          },
        );
      // The retired output's drain wait must observe the output-scoped parked
      // abort receipt instead of succeeding through the drained shortcut.
      const retiredDrain = subscription._waitForOutputDrain!(oldOutput)!;
      expect(retiredDrain).toBeDefined();

      // Release the terminal: it delivers, then its publication fails. The
      // retired attempt settles only its own output-scoped receipt.
      gate.release();
      await pubsub.flush();
      await waitForCondition(() =>
        pubsub.publishedData.some(data => data?.type === 'run-aborted' && data.runId === runId),
      );
      await expect(
        withTimeout(retiredDrain, 'Timed out waiting for retired output receipt rejection'),
      ).rejects.toMatchObject({
        name: 'AgentThreadOutputDrainError',
        reason: 'terminal-publish-failed',
      });
      // The successor reservation waiter is untouched by the stale failure.
      await nextTick();
      expect(reservationSettled).toBe(false);

      // The successor registers cleanly and owns the run id after the retired
      // attempt's fenced cleanup: its records, lease and output lookup work.
      const successorFinished = new Promise<void>(() => {});
      await runtime.registerRun(
        agent,
        {
          runId,
          status: 'running',
          fullStream: (async function* () {})(),
          _waitUntilFinished: () => successorFinished,
        } as any,
        { runId, memory: { thread: threadId, resource: resourceId } } as any,
        pubsub,
        { strict: true },
      );
      expect(runtime.hasThreadRun(runId, pubsub)).toBe(true);
      expect(runtime.getActiveThreadRunId({ resourceId, threadId }, pubsub)).toBe(runId);
      expect(decodeLeaseOwnerRunId(pubsub.owners.get(key))).toBe(runId);
      await expect(runtime.waitForRunOutput(runId, pubsub)).resolves.toBeDefined();
      await nextTick();
      expect(reservationSettled).toBe(false);
      void reservationWait;
    } finally {
      gate.release();
      finishRun();
      subscription.unsubscribe();
      await iterator.return?.();
    }
  });

  it('rejects the retired output receipt when a parked abort terminal fails before delivery', async () => {
    const pubsub = new GatedRejectingBeforeDeliveryTypePubSub();
    pubsub.rejectOnRelease.add('run-aborted');
    const runtime = new AgentThreadStreamRuntime();
    const resourceId = 'parked-receipt-undelivered-resource';
    const threadId = 'parked-receipt-undelivered-thread';
    const key = `${resourceId}\u0000${threadId}`;
    const runId = 'parked-receipt-undelivered-run';
    const agent = { id: 'parked-receipt-undelivered-agent' } as Agent<any, any, any>;
    let finishRun!: () => void;
    const finished = new Promise<void>(resolve => {
      finishRun = resolve;
    });
    const subscription = await runtime.subscribeToThread(agent, { resourceId, threadId }, pubsub);
    const gate = pubsub.gateType('run-aborted');
    const iterator = subscription.stream[Symbol.asyncIterator]();
    const oldOutput = {
      runId,
      status: 'suspended',
      fullStream: new ReadableStream({
        start(controller) {
          controller.enqueue({ type: 'start', runId });
          controller.enqueue({
            type: 'tool-call-suspended',
            runId,
            payload: { toolCallId: 'parked-receipt-undelivered-call', toolName: 'ask_user' },
          });
          controller.close();
        },
      }),
      _waitUntilFinished: () => finished,
    } as any;
    try {
      runtime.registerRun(agent, oldOutput, { memory: { thread: threadId, resource: resourceId } } as any, pubsub);
      finishRun();
      await pubsub.flush();
      await waitForCondition(() =>
        pubsub.publishedData.some(data => data?.type === 'run-suspended' && data.runId === runId),
      );
      expect(runtime.hasThreadRun(runId, pubsub)).toBe(true);

      await expect(iterator.next()).resolves.toMatchObject({ value: { type: 'start', runId } });
      await expect(iterator.next()).resolves.toMatchObject({
        value: { type: 'tool-call-suspended', payload: { toolCallId: 'parked-receipt-undelivered-call' } },
      });
      void iterator.next();
      await vi.waitFor(async () => {
        await subscription._waitForOutputDrain!(oldOutput);
      });

      // The parked abort starts; its authenticated terminal publication is
      // held and will REJECT before any delivery once released.
      expect(runtime.abortRun(runId, pubsub)).toBe(true);
      // A real same-run reservation owns the run id while the terminal is in
      // flight.
      const releaseReservation = runtime.reserveRun(
        { runId, memory: { thread: threadId, resource: resourceId } } as any,
        pubsub,
        agent.id,
      );
      expect(releaseReservation).toBeTypeOf('function');
      const retiredDrain = subscription._waitForOutputDrain!(oldOutput)!;
      expect(retiredDrain).toBeDefined();

      // Release the terminal: it fails WITHOUT delivering, so no observer ever
      // sees the run-aborted event. The retired output receipt still rejects.
      gate.release();
      await pubsub.flush();
      await expect(
        withTimeout(retiredDrain, 'Timed out waiting for retired output receipt rejection'),
      ).rejects.toMatchObject({
        name: 'AgentThreadOutputDrainError',
        reason: 'terminal-publish-failed',
      });
      expect(pubsub.deliveredData.some(data => data?.type === 'run-aborted' && data.runId === runId)).toBe(false);
      expect(runtime.hasThreadRun(runId, pubsub)).toBe(false);

      // The successor registers cleanly after the retired attempt's fenced
      // cleanup.
      const successorFinished = new Promise<void>(() => {});
      await runtime.registerRun(
        agent,
        {
          runId,
          status: 'running',
          fullStream: (async function* () {})(),
          _waitUntilFinished: () => successorFinished,
        } as any,
        { runId, memory: { thread: threadId, resource: resourceId } } as any,
        pubsub,
        { strict: true },
      );
      expect(runtime.getActiveThreadRunId({ resourceId, threadId }, pubsub)).toBe(runId);
      expect(decodeLeaseOwnerRunId(pubsub.owners.get(key))).toBe(runId);
    } finally {
      gate.release();
      finishRun();
      subscription.unsubscribe();
      await iterator.return?.();
    }
  });

  it('routes active-run signals across runtime instances through PubSub', async () => {
    const pubsub = new EventEmitterPubSub();
    const ownerRuntime = new AgentThreadStreamRuntime();
    const senderRuntime = new AgentThreadStreamRuntime();
    const owner = new Agent({
      id: 'remote-signal-agent',
      name: 'Remote Signal Owner Agent',
      instructions: 'Test',
      model: createTextStreamModel('owner response'),
    });
    const sender = new Agent({
      id: 'remote-signal-agent',
      name: 'Remote Signal Sender Agent',
      instructions: 'Test',
      model: createTextStreamModel('sender response'),
    });
    let finishRun!: () => void;
    const output = {
      runId: 'remote-run-1',
      status: 'running',
      fullStream: (async function* () {})(),
      _waitUntilFinished: () => new Promise<void>(resolve => (finishRun = resolve)),
    } as any;

    const ownerSubscription = await ownerRuntime.subscribeToThread(
      owner,
      {
        resourceId: 'remote-resource',
        threadId: 'remote-thread',
      },
      pubsub,
    );
    const senderSubscription = await senderRuntime.subscribeToThread(
      sender,
      {
        resourceId: 'remote-resource',
        threadId: 'remote-thread',
      },
      pubsub,
    );

    ownerRuntime.registerRun(
      owner,
      output,
      { runId: 'remote-run-1', memory: { resource: 'remote-resource', thread: 'remote-thread' } } as any,
      pubsub,
    );
    await waitForCondition(() => senderSubscription.activeRunId() === 'remote-run-1');

    let waitResolved = false;
    const waitForRemoteRun = senderRuntime
      .waitForCrossAgentThreadRun(
        new Agent({
          id: 'remote-other-agent',
          name: 'Remote Other Agent',
          instructions: 'Test',
          model: createTextStreamModel('other response'),
        }),
        { memory: { resource: 'remote-resource', thread: 'remote-thread' } },
        pubsub,
      )
      .then(() => {
        waitResolved = true;
      });
    await nextTick();
    expect(waitResolved).toBe(false);

    const result = senderRuntime.sendSignal(
      sender,
      {
        type: 'user-message',
        contents: 'remote follow-up',
        metadata: { channel: { attachmentId: 'file-remote' } },
      },
      { resourceId: 'remote-resource', threadId: 'remote-thread' },
      pubsub,
    );

    await expect(result.accepted).resolves.toMatchObject({ action: 'deliver' });
    let deliveredSignals: ReturnType<typeof ownerRuntime.drainPendingSignals> = [];
    await waitForCondition(() => {
      deliveredSignals = ownerRuntime.drainPendingSignals('remote-run-1', pubsub);
      return deliveredSignals.length === 1;
    });
    expect(deliveredSignals[0]?.metadata).toEqual({ channel: { attachmentId: 'file-remote' } });

    finishRun();
    await waitForRemoteRun;
    ownerSubscription.unsubscribe();
    senderSubscription.unsubscribe();
  });

  it('deduplicates stable signal ids delivered by multiple runtime instances', async () => {
    const pubsub = new EventEmitterPubSub();
    const ownerRuntime = new AgentThreadStreamRuntime();
    const senderRuntimeA = new AgentThreadStreamRuntime();
    const senderRuntimeB = new AgentThreadStreamRuntime();
    const owner = new Agent({
      id: 'remote-idempotent-agent',
      name: 'Remote Idempotent Owner Agent',
      instructions: 'Test',
      model: createTextStreamModel('owner response'),
    });
    const senderA = new Agent({
      id: 'remote-idempotent-agent',
      name: 'Remote Idempotent Sender A',
      instructions: 'Test',
      model: createTextStreamModel('sender response'),
    });
    const senderB = new Agent({
      id: 'remote-idempotent-agent',
      name: 'Remote Idempotent Sender B',
      instructions: 'Test',
      model: createTextStreamModel('sender response'),
    });
    let finishRun!: () => void;
    const output = {
      runId: 'remote-idempotent-run',
      status: 'running',
      fullStream: (async function* () {})(),
      _waitUntilFinished: () => new Promise<void>(resolve => (finishRun = resolve)),
    } as any;
    const target = { resourceId: 'remote-idempotent-resource', threadId: 'remote-idempotent-thread' };
    const threadTopic = `agent.thread-stream.${encodeURIComponent(`${target.resourceId}\u0000${target.threadId}`)}`;
    const observedSignals: any[] = [];
    await pubsub.subscribe(threadTopic, event => {
      if (event.data?.type === 'signal-enqueued') observedSignals.push(event.data);
    });

    const ownerSubscription = await ownerRuntime.subscribeToThread(owner, target, pubsub);
    const senderSubscriptionA = await senderRuntimeA.subscribeToThread(senderA, target, pubsub);
    const senderSubscriptionB = await senderRuntimeB.subscribeToThread(senderB, target, pubsub);
    ownerRuntime.registerRun(
      owner,
      output,
      { runId: output.runId, memory: { resource: target.resourceId, thread: target.threadId } } as any,
      pubsub,
    );
    await waitForCondition(
      () => senderSubscriptionA.activeRunId() === output.runId && senderSubscriptionB.activeRunId() === output.runId,
    );

    const stableSignal = { id: 'stable-remote-signal', type: 'user-message' as const, contents: 'once only' };
    const acceptedA = await senderRuntimeA.sendSignal(senderA, stableSignal, target, pubsub).accepted;
    expect(acceptedA).toMatchObject({ action: 'deliver', runId: output.runId });
    const firstDrain = ownerRuntime.drainPendingSignals(output.runId, pubsub);
    const acceptedB = await senderRuntimeB.sendSignal(senderB, stableSignal, target, pubsub).accepted;
    expect(acceptedB).toMatchObject({ action: 'deliver', runId: output.runId });
    // PF-4402 merge: observer subscriptions no longer consume stable-id
    // admissions (the owner's control subscription is the sole execution-queue
    // ledger, so a forwarded handoff can still be admitted by its winner).
    // Sender B therefore republishes its exact retry, and the owner's ledger
    // drops it: the signal still executes exactly once.
    expect(observedSignals).toHaveLength(2);
    expect(firstDrain).toMatchObject([{ id: stableSignal.id, contents: stableSignal.contents }]);
    expect(ownerRuntime.drainPendingSignals(output.runId, pubsub)).toEqual([]);

    expect(() =>
      senderRuntimeB.sendSignal(senderB, { ...stableSignal, contents: 'conflicting replay' }, target, pubsub),
    ).toThrow('already accepted with a different payload');
    expect(ownerRuntime.drainPendingSignals(output.runId, pubsub)).toEqual([]);

    finishRun();
    await nextTick();
    ownerSubscription.unsubscribe();
    senderSubscriptionA.unsubscribe();
    senderSubscriptionB.unsubscribe();
  });

  it('uses process-attempt lease owners when two runtimes share a stable public wake run id', async () => {
    const pubsub = new EventEmitterPubSub();
    const runtimeA = new AgentThreadStreamRuntime();
    const runtimeB = new AgentThreadStreamRuntime();
    const target = { resourceId: 'stable-wake-resource', threadId: 'stable-wake-thread' };
    const runId = 'stable-public-wake-run';
    const signal = { id: 'stable-public-wake-signal', type: 'user-message' as const, contents: 'execute once' };
    const makeOutput = () =>
      ({
        runId,
        status: 'running',
        fullStream: (async function* () {})(),
        _waitUntilFinished: () => new Promise<void>(() => {}),
      }) as any;
    const agentA = { id: 'stable-wake-agent', stream: vi.fn(async () => makeOutput()) } as any;
    const agentB = { id: 'stable-wake-agent', stream: vi.fn(async () => makeOutput()) } as any;
    const subscriptionA = await runtimeA.subscribeToThread(agentA, target, pubsub);
    const subscriptionB = await runtimeB.subscribeToThread(agentB, target, pubsub);

    const dispatchedA = runtimeA.sendSignal(agentA, signal, { ...target, runId, ifIdle: { behavior: 'wake' } }, pubsub);
    const dispatchedB = runtimeB.sendSignal(agentB, signal, { ...target, runId, ifIdle: { behavior: 'wake' } }, pubsub);
    const accepted = await Promise.all([dispatchedA.accepted, dispatchedB.accepted]);

    expect(accepted.map(result => result.action).sort()).toEqual(['deliver', 'wake']);
    expect(accepted).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ action: 'deliver', runId }),
        expect.objectContaining({ action: 'wake', runId }),
      ]),
    );
    expect(agentA.stream.mock.calls.length + agentB.stream.mock.calls.length).toBe(1);
    const winnerRuntime = accepted[0]!.action === 'wake' ? runtimeA : runtimeB;
    expect(winnerRuntime.getThreadState(target, pubsub)).toBe('active');
    expect(winnerRuntime.drainPendingSignals(runId, pubsub)).toEqual([]);

    runtimeA.abortRun(runId, pubsub);
    runtimeB.abortRun(runId, pubsub);
    subscriptionA.unsubscribe();
    subscriptionB.unsubscribe();
  });

  it('rejects a lost-lease delivery when broker publication fails and permits a retry', async () => {
    const pubsub = new RejectSignalEnqueuedPubSub();
    const runtime = new AgentThreadStreamRuntime();
    const target = { resourceId: 'failed-publish-resource', threadId: 'failed-publish-thread' };
    const key = `${target.resourceId}\u0000${target.threadId}`;
    const runId = 'failed-publish-stable-run';
    const signal = { id: 'failed-publish-stable-signal', type: 'user-message' as const, contents: 'do not drop' };
    const agent = {
      id: 'failed-publish-agent',
      stream: vi.fn(async () => ({
        runId,
        status: 'running',
        fullStream: (async function* () {})(),
        _waitUntilFinished: () => new Promise<void>(() => {}),
      })),
    } as any;
    const subscription = await runtime.subscribeToThread(agent, target, pubsub);
    await pubsub.acquireLease(key, 'winning-run', 15_000);

    const first = runtime.sendSignal(agent, signal, { ...target, runId, ifIdle: { behavior: 'wake' } }, pubsub);
    await expect(first.accepted).rejects.toThrow('injected signal enqueue publication failure');
    expect(agent.stream).not.toHaveBeenCalled();

    await pubsub.releaseLease(key, 'winning-run');
    const retry = runtime.sendSignal(agent, signal, { ...target, runId, ifIdle: { behavior: 'wake' } }, pubsub);
    await expect(retry.accepted).resolves.toMatchObject({ action: 'wake', runId });
    expect(agent.stream).toHaveBeenCalledTimes(1);

    runtime.abortRun(runId, pubsub);
    subscription.unsubscribe();
  });

  it('discards a full logical message when distributed idle wake loses to an active owner', async () => {
    const pubsub = new ControlledLeasePubSub();
    const runtime = new AgentThreadStreamRuntime();
    const target = { resourceId: 'lineage-lease-resource', threadId: 'lineage-lease-thread' };
    const key = `${target.resourceId}\u0000${target.threadId}`;
    const runId = 'lineage-lease-loser-run';
    const signal = { id: 'lineage-lease-signal', type: 'user-message' as const, contents: 'preserve response owner' };
    let idleSignalDiscarded = false;
    const agent = {
      id: 'lineage-lease-agent',
      stream: vi.fn(async (_signal: unknown, options: { runId: string }) => ({
        runId: options.runId,
        status: 'running',
        fullStream: (async function* () {})(),
        _waitUntilFinished: () => new Promise<void>(() => {}),
      })),
    } as any;
    await pubsub.acquireLease(key, 'lineage-lease-winning-run');

    const result = runtime.sendSignal(
      agent,
      signal,
      {
        ...target,
        runId,
        ifIdle: {
          behavior: 'wake',
          streamOptions: { logicalMessageIdentity: { input: 'lease-input', response: 'lease-response' } },
          _onThreadStreamSignalDiscarded: () => {
            idleSignalDiscarded = true;
          },
        },
      },
      pubsub,
    );

    await expect(result.accepted).resolves.toEqual({ action: 'discard' });
    expect(idleSignalDiscarded).toBe(true);
    expect(pubsub.publishedData.filter(event => event?.type === 'signal-enqueued')).toEqual([]);
    expect(agent.stream).not.toHaveBeenCalled();
  });

  it('discards a full logical message behind a foreign reservation instead of queueing it', async () => {
    const pubsub = new EventEmitterPubSub();
    const runtime = new AgentThreadStreamRuntime();
    const target = { resourceId: 'foreign-reservation-lineage-user', threadId: 'foreign-reservation-lineage-thread' };
    const release = runtime.reserveRun(
      {
        runId: 'foreign-reservation-lineage-run',
        memory: { resource: target.resourceId, thread: target.threadId },
      } as any,
      pubsub,
      'foreign-owner-agent',
    );
    const stream = vi.fn();
    let idleSignalDiscarded = false;
    const sender = { id: 'lineage-sender-agent', stream } as any;

    try {
      const result = runtime.sendSignal(
        sender,
        { id: 'foreign-reservation-lineage-signal', type: 'user-message', contents: 'preserve response identity' },
        {
          ...target,
          ifActive: { behavior: 'discard' },
          ifIdle: {
            behavior: 'wake',
            streamOptions: { logicalMessageIdentity: { input: 'foreign-input', response: 'foreign-response' } },
            _onThreadStreamSignalDiscarded: () => {
              idleSignalDiscarded = true;
            },
          },
        } as any,
        pubsub,
      );

      await expect(result.accepted).resolves.toEqual({ action: 'discard' });
      expect(idleSignalDiscarded).toBe(true);
      expect(stream).not.toHaveBeenCalled();
      expect(runtime.drainPendingSignals('foreign-reservation-lineage-run', pubsub)).toEqual([]);
    } finally {
      release?.();
    }
  });

  it('reuses an exact full logical signal on its pending run after a newer admission attempt', async () => {
    const pubsub = new EventEmitterPubSub();
    const runtime = new AgentThreadStreamRuntime();
    const target = { resourceId: 'attempt-retry-resource', threadId: 'attempt-retry-thread' };
    const runId = 'attempt-retry-run';
    const signal = { id: 'attempt-retry-signal', type: 'user-message' as const, contents: 'execute once' };
    const agent = {
      id: 'attempt-retry-agent',
      stream: vi.fn(() => new Promise<never>(() => {})),
    } as any;

    const first = runtime.sendSignal(
      agent,
      signal,
      {
        ...target,
        runId,
        ifActive: { behavior: 'discard' },
        ifIdle: {
          behavior: 'wake',
          streamOptions: { logicalMessageIdentity: { input: 'attempt-input', response: 'attempt-response' } },
        },
        _signalAdmissionAttemptId: 'attempt-a',
      } as any,
      pubsub,
    );
    let firstSettled = false;
    void first.accepted.finally(() => {
      firstSettled = true;
    });
    await nextTick();
    expect(firstSettled).toBe(false);
    expect(agent.stream).toHaveBeenCalledTimes(1);

    const retry = runtime.sendSignal(
      agent,
      signal,
      {
        ...target,
        runId,
        ifActive: { behavior: 'discard' },
        ifIdle: {
          behavior: 'wake',
          streamOptions: { logicalMessageIdentity: { input: 'attempt-input', response: 'attempt-response' } },
        },
        _signalAdmissionAttemptId: 'attempt-b',
      } as any,
      pubsub,
    );
    await expect(retry.accepted).resolves.toMatchObject({ action: 'deliver', runId });
    expect(agent.stream).toHaveBeenCalledTimes(1);
    expect(() =>
      runtime.sendSignal(
        agent,
        { ...signal, contents: 'conflicting retry' },
        {
          ...target,
          runId,
          ifIdle: { behavior: 'wake' },
          _signalAdmissionAttemptId: 'attempt-c',
        } as any,
        pubsub,
      ),
    ).toThrow('already accepted with a different payload');
    runtime.resetForTests();
  });

  it('keeps a retry attached while the original idle admission is still provisional', async () => {
    const pubsub = new ControlledLeasePubSub();
    const runtime = new AgentThreadStreamRuntime();
    const target = { resourceId: 'provisional-retry-resource', threadId: 'provisional-retry-thread' };
    const runId = 'provisional-retry-run';
    const signal = { id: 'provisional-retry-signal', type: 'user-message' as const, contents: 'wait for admission' };
    const agent = {
      id: 'provisional-retry-agent',
      stream: vi.fn(async (_signal: unknown, options: { runId: string }) => ({
        runId: options.runId,
        status: 'running',
        fullStream: (async function* () {})(),
        _waitUntilFinished: () => new Promise<void>(() => {}),
      })),
    } as any;
    let releaseLease!: () => void;
    pubsub.acquireLeaseWait = new Promise<void>(resolve => {
      releaseLease = resolve;
    });
    let markAcquireStarted!: () => void;
    const acquireStarted = new Promise<void>(resolve => {
      markAcquireStarted = resolve;
    });
    pubsub.onAcquireLease = markAcquireStarted;

    const first = runtime.sendSignal(
      agent,
      signal,
      {
        ...target,
        runId,
        ifActive: { behavior: 'discard' },
        ifIdle: {
          behavior: 'wake',
          streamOptions: { logicalMessageIdentity: { input: 'provisional-input', response: 'provisional-response' } },
        },
        _signalAdmissionAttemptId: 'attempt-a',
      } as any,
      pubsub,
    );
    await acquireStarted;

    const retry = runtime.sendSignal(
      agent,
      signal,
      {
        ...target,
        runId,
        ifActive: { behavior: 'discard' },
        ifIdle: {
          behavior: 'wake',
          streamOptions: { logicalMessageIdentity: { input: 'provisional-input', response: 'provisional-response' } },
        },
        _signalAdmissionAttemptId: 'attempt-b',
      } as any,
      pubsub,
    );
    expect(retry.accepted).toBe(first.accepted);
    expect(agent.stream).not.toHaveBeenCalled();

    releaseLease();
    await expect(retry.accepted).resolves.toMatchObject({ action: 'wake', runId });
    expect(agent.stream).toHaveBeenCalledTimes(1);
    runtime.abortRun(runId, pubsub);
  });

  it('allows a newer attempt to redispatch after a lost lease rolls back before forwarding', async () => {
    const pubsub = new ControlledLeasePubSub();
    const runtime = new AgentThreadStreamRuntime();
    const target = { resourceId: 'retry-after-forward-resource', threadId: 'retry-after-forward-thread' };
    const key = `${target.resourceId}\u0000${target.threadId}`;
    const signal = { id: 'retry-after-forward-signal', type: 'user-message' as const, contents: 'retry this signal' };
    const agent = {
      id: 'retry-after-forward-agent',
      stream: vi.fn(async (_signal: unknown, options: { runId: string }) => ({
        runId: options.runId,
        status: 'running',
        fullStream: (async function* () {})(),
        _waitUntilFinished: () => new Promise<void>(() => {}),
      })),
    } as any;
    let releaseForward!: () => void;
    let markForwardStarted!: () => void;
    const forwardGate = new Promise<void>(resolve => {
      releaseForward = resolve;
    });
    const forwardStarted = new Promise<void>(resolve => {
      markForwardStarted = resolve;
    });
    const realPublish = pubsub.publish.bind(pubsub);
    let blockedForward = true;
    pubsub.publish = async (topic, event) => {
      if (blockedForward && event.data?.type === 'signal-enqueued') {
        blockedForward = false;
        markForwardStarted();
        await forwardGate;
      }
      await realPublish(topic, event);
    };
    pubsub.owners.set(key, 'winner-run');

    const first = runtime.sendSignal(
      agent,
      signal,
      {
        ...target,
        ifIdle: { behavior: 'wake' },
        _signalAdmissionAttemptId: 'attempt-a',
      } as any,
      pubsub,
    );
    await forwardStarted;

    pubsub.owners.delete(key);
    const retry = runtime.sendSignal(
      agent,
      signal,
      {
        ...target,
        ifIdle: { behavior: 'wake' },
        _signalAdmissionAttemptId: 'attempt-b',
      } as any,
      pubsub,
    );

    await expect(retry.accepted).resolves.toMatchObject({ action: 'wake', runId: retry.runId });
    expect(retry.accepted).not.toBe(first.accepted);
    expect(agent.stream).toHaveBeenCalledTimes(1);

    releaseForward();
    await expect(first.accepted).resolves.toMatchObject({ action: 'deliver', runId: 'winner-run' });
    runtime.abortRun(retry.runId, pubsub);
  });

  it('retains stable signal admission through the run-completed publication window', async () => {
    const pubsub = new BlockingRunCompletedPubSub();
    const ownerRuntime = new AgentThreadStreamRuntime();
    const senderRuntimeA = new AgentThreadStreamRuntime();
    const senderRuntimeB = new AgentThreadStreamRuntime();
    const target = { resourceId: 'terminal-window-resource', threadId: 'terminal-window-thread' };
    const owner = new Agent({
      id: 'terminal-window-agent',
      name: 'Terminal Window Owner',
      instructions: 'Test',
      model: createTextStreamModel('owner terminal-window response'),
    });
    const senderA = new Agent({
      id: 'terminal-window-agent',
      name: 'Terminal Window Sender A',
      instructions: 'Test',
      model: createTextStreamModel('sender terminal-window response'),
    });
    const senderB = new Agent({
      id: 'terminal-window-agent',
      name: 'Terminal Window Sender B',
      instructions: 'Test',
      model: createTextStreamModel('sender terminal-window response'),
    });
    let finishRun!: () => void;
    const finished = new Promise<void>(resolve => {
      finishRun = resolve;
    });
    const output = {
      runId: 'terminal-window-run',
      status: 'running',
      fullStream: (async function* () {})(),
      _waitUntilFinished: () => finished,
    } as any;
    const signal = { id: 'terminal-window-signal', type: 'user-message' as const, contents: 'once at terminal' };
    const ownerSubscription = await ownerRuntime.subscribeToThread(owner, target, pubsub);
    const senderSubscriptionA = await senderRuntimeA.subscribeToThread(senderA, target, pubsub);
    const senderSubscriptionB = await senderRuntimeB.subscribeToThread(senderB, target, pubsub);
    const completion = ownerRuntime.registerRun(
      owner,
      output,
      { runId: output.runId, memory: { resource: target.resourceId, thread: target.threadId } } as any,
      pubsub,
    );
    await waitForCondition(
      () => senderSubscriptionA.activeRunId() === output.runId && senderSubscriptionB.activeRunId() === output.runId,
    );
    // Retain B's observed run projection but stop it from observing A's first
    // signal admission, forcing the terminal-window retry through the broker.
    senderSubscriptionB.unsubscribe();

    const first = senderRuntimeA.sendSignal(senderA, signal, target, pubsub);
    await expect(first.accepted).resolves.toMatchObject({ action: 'deliver', runId: output.runId });
    expect(ownerRuntime.drainPendingSignals(output.runId, pubsub)).toMatchObject([
      { id: signal.id, contents: signal.contents },
    ]);

    finishRun();
    await waitForCondition(() => pubsub.sawRunCompleted);
    const retry = senderRuntimeB.sendSignal(senderB, signal, target, pubsub);
    await expect(retry.accepted).resolves.toMatchObject({ action: 'deliver', runId: output.runId });
    expect(ownerRuntime.drainPendingSignals(output.runId, pubsub)).toEqual([]);

    pubsub.unblockRunCompleted();
    await completion;
    ownerSubscription.unsubscribe();
    senderSubscriptionA.unsubscribe();
  });

  it('evicts retained stable signal admissions after their terminal-window TTL', async () => {
    vi.useFakeTimers();
    try {
      const pubsub = new EventEmitterPubSub();
      const runtime = new AgentThreadStreamRuntime();
      const target = { resourceId: 'admission-ttl-resource', threadId: 'admission-ttl-thread' };
      let finishRun!: () => void;
      const finished = new Promise<void>(resolve => {
        finishRun = resolve;
      });
      const stream = vi.fn(async (_signal: unknown, options: { runId: string }) => ({
        runId: options.runId,
        status: 'running',
        fullStream: (async function* () {})(),
        _waitUntilFinished: () => new Promise<void>(() => {}),
      }));
      const agent = { id: 'admission-ttl-agent', stream } as any;
      const activeRunId = 'admission-ttl-active-run';
      const completion = runtime.registerRun(
        agent,
        {
          runId: activeRunId,
          status: 'running',
          fullStream: (async function* () {})(),
          _waitUntilFinished: () => finished,
        } as any,
        { runId: activeRunId, memory: { resource: target.resourceId, thread: target.threadId } } as any,
        pubsub,
      );
      const signal = { id: 'admission-ttl-signal', type: 'user-message' as const, contents: 'expires later' };
      await expect(runtime.sendSignal(agent, signal, target, pubsub).accepted).resolves.toMatchObject({
        action: 'deliver',
        runId: activeRunId,
      });
      expect(runtime.drainPendingSignals(activeRunId, pubsub)).toHaveLength(1);
      finishRun();
      await completion;

      const retained = runtime.sendSignal(agent, signal, target, pubsub);
      await expect(retained.accepted).resolves.toMatchObject({ action: 'deliver', runId: activeRunId });
      expect(stream).not.toHaveBeenCalled();

      await vi.advanceTimersByTimeAsync(5 * 60 * 1000 + 1);
      const expired = runtime.sendSignal(agent, signal, target, pubsub);
      await expect(expired.accepted).resolves.toMatchObject({ action: 'wake', runId: expired.runId });
      expect(expired.runId).not.toBe(activeRunId);
      expect(stream).toHaveBeenCalledTimes(1);
      runtime.abortRun(expired.runId, pubsub);
    } finally {
      vi.useRealTimers();
    }
  });

  it('does not enqueue the owned wake signal again when PubSub redelivers it', async () => {
    const pubsub = new EventEmitterPubSub();
    const runtime = new AgentThreadStreamRuntime();
    const runId = 'stable-owned-wake-run';
    const target = { resourceId: 'stable-owned-resource', threadId: 'stable-owned-thread' };
    let releaseStream!: () => void;
    const streamGate = new Promise<void>(resolve => {
      releaseStream = resolve;
    });
    const agent = {
      id: 'stable-owned-agent',
      stream: vi.fn(async () => {
        await streamGate;
        return {
          runId,
          status: 'running',
          fullStream: (async function* () {})(),
          _waitUntilFinished: () => new Promise<void>(() => {}),
        } as any;
      }),
    } as any;
    const subscription = await runtime.subscribeToThread(agent, target, pubsub);
    const signal = {
      id: 'stable-owned-signal',
      type: 'user-message' as const,
      contents: 'initial wake input',
      createdAt: new Date(),
    };

    const dispatched = runtime.sendSignal(
      agent,
      signal,
      {
        ...target,
        runId,
        ifIdle: { behavior: 'wake', streamOptions: { memory: target } as any },
      },
      pubsub,
    );
    await pubsub.publish(`agent.thread-stream.${encodeURIComponent(`${target.resourceId}\u0000${target.threadId}`)}`, {
      type: 'signal-enqueued',
      runId,
      data: {
        type: 'signal-enqueued',
        runId,
        signal,
        sourceId: 'redelivering-runtime',
      },
    });
    expect(runtime.drainPendingSignals(runId, pubsub)).toEqual([]);

    releaseStream();
    await expect(dispatched.accepted).resolves.toMatchObject({ action: 'wake', runId });
    expect(agent.stream).toHaveBeenCalledTimes(1);
    expect(runtime.abortRun(runId, pubsub)).toBe(true);
    subscription.unsubscribe();
  });

  it('wakes a new run instead of delivering to a stale remote active run id', async () => {
    const pubsub = new EventEmitterPubSub();
    const ownerRuntime = new AgentThreadStreamRuntime();
    const senderRuntime = new AgentThreadStreamRuntime();
    const owner = new Agent({
      id: 'stale-remote-signal-agent',
      name: 'Stale Remote Signal Owner Agent',
      instructions: 'Test',
      model: createTextStreamModel('owner response'),
    });
    const sender = new Agent({
      id: 'stale-remote-signal-agent',
      name: 'Stale Remote Signal Sender Agent',
      instructions: 'Test',
      model: createTextStreamModel('sender response'),
    });
    let finishRun!: () => void;
    const output = {
      runId: 'stale-remote-run-1',
      status: 'running',
      fullStream: (async function* () {})(),
      _waitUntilFinished: () => new Promise<void>(resolve => (finishRun = resolve)),
    } as any;

    const senderSubscription = await senderRuntime.subscribeToThread(
      sender,
      {
        resourceId: 'stale-remote-resource',
        threadId: 'stale-remote-thread',
      },
      pubsub,
    );
    ownerRuntime.registerRun(
      owner,
      output,
      {
        runId: 'stale-remote-run-1',
        memory: { resource: 'stale-remote-resource', thread: 'stale-remote-thread' },
      } as any,
      pubsub,
    );
    await waitForCondition(() => senderSubscription.activeRunId() === 'stale-remote-run-1');

    senderSubscription.unsubscribe();
    finishRun();
    await nextTick();

    const result = senderRuntime.sendSignal(
      sender,
      { type: 'user-message', contents: 'stale remote follow-up' },
      { resourceId: 'stale-remote-resource', threadId: 'stale-remote-thread' },
      pubsub,
    );

    await expect(result.accepted).resolves.toMatchObject({ action: 'wake' });
    await expect(result.accepted).resolves.not.toMatchObject({ runId: 'stale-remote-run-1' });
  });

  it('grants the wake output to exactly one runtime when two race to wake an idle thread', async () => {
    const pubsub = new EventEmitterPubSub();
    const runtimeA = new AgentThreadStreamRuntime();
    const runtimeB = new AgentThreadStreamRuntime();

    // Track which agents had their .stream invoked. Only the lease winner
    // should actually call .stream(); the loser must short-circuit.
    const streamCallsA: number[] = [];
    const streamCallsB: number[] = [];

    const makeStubAgent = (id: string, calls: number[]) => {
      let nextRunId = 0;
      return {
        id,
        stream: async () => {
          const runId = `${id}-run-${++nextRunId}`;
          calls.push(nextRunId);
          return {
            runId,
            status: 'running',
            fullStream: (async function* () {})(),
            _waitUntilFinished: () => new Promise<void>(() => {}),
          } as any;
        },
      } as any;
    };

    const agentA = makeStubAgent('race-agent-a', streamCallsA);
    const agentB = makeStubAgent('race-agent-b', streamCallsB);

    const target = {
      resourceId: 'race-resource',
      threadId: 'race-thread',
      ifIdle: {
        behavior: 'wake' as const,
        streamOptions: { memory: { resource: 'race-resource', thread: 'race-thread' } },
      },
    };

    // Fire both signals in the same microtask burst so the lease race is real.
    const resultA = runtimeA.sendSignal(agentA, { type: 'user-message', contents: 'from A' }, target, pubsub);
    const resultB = runtimeB.sendSignal(agentB, { type: 'user-message', contents: 'from B' }, target, pubsub);

    expect(resultA.accepted).toBeInstanceOf(Promise);
    expect(resultB.accepted).toBeInstanceOf(Promise);

    const [settledA, settledB] = await Promise.all([resultA.accepted, resultB.accepted]);

    // Exactly one runtime won the lease and ran the stream (`wake` + owned output); the
    // loser forwarded its signal to the winner and resolves to `deliver`.
    const ownerA = settledA.action === 'wake' ? settledA.output : undefined;
    const ownerB = settledB.action === 'wake' ? settledB.output : undefined;
    const winners = [ownerA, ownerB].filter(s => s !== undefined);
    expect(winners).toHaveLength(1);

    const actions = [settledA.action, settledB.action].sort();
    expect(actions).toEqual(['deliver', 'wake']);

    // Only the winner's agent.stream was invoked.
    const totalStreamCalls = streamCallsA.length + streamCallsB.length;
    expect(totalStreamCalls).toBe(1);
  });

  it.runIf(process.platform !== 'win32')(
    'queues a signal on the real UnixSocketPubSub execution-lease owner',
    async () => {
      const tempDir = await mkdtemp(join(tmpdir(), 'mastra-agent-execution-lease-'));
      const socketPath = join(tempDir, 'signals.sock');
      const ownerPubSub = new UnixSocketPubSub(socketPath);
      const senderPubSub = new UnixSocketPubSub(socketPath);
      const ownerRuntime = new AgentThreadStreamRuntime();
      const senderRuntime = new AgentThreadStreamRuntime();
      const owner = new Agent({
        id: 'unix-execution-lease-agent',
        name: 'Unix Execution Lease Owner',
        instructions: 'Test',
        model: createTextStreamModel('owner response'),
      });
      const sender = new Agent({
        id: 'unix-execution-lease-agent',
        name: 'Unix Execution Lease Sender',
        instructions: 'Test',
        model: createTextStreamModel('sender response'),
      });
      const scope = { resourceId: 'unix-execution-resource', threadId: 'unix-execution-thread' };
      const key = `${scope.resourceId}\u0000${scope.threadId}`;
      const runId = 'unix-execution-run';
      let finishRun!: () => void;
      const output = {
        runId,
        status: 'running',
        fullStream: (async function* () {})(),
        _waitUntilFinished: () => new Promise<void>(resolve => (finishRun = resolve)),
      } as any;

      try {
        const ownerSubscription = await ownerRuntime.subscribeToThread(owner, scope, ownerPubSub);
        const senderSubscription = await senderRuntime.subscribeToThread(sender, scope, senderPubSub);
        const ownerClaim = await ownerRuntime.claimThreadOwnership(
          owner,
          { ...scope, yieldOwnership: () => true },
          ownerPubSub,
        );
        expect(ownerClaim.claimed).toBe(true);
        // Fork (PF-4402): registerRun acquires the execution lease under its
        // process-attempt owner token; a pre-seeded raw run-id owner would make
        // that acquisition fail closed.
        void ownerRuntime.registerRun(
          owner,
          output,
          { runId, memory: { resource: scope.resourceId, thread: scope.threadId } } as any,
          ownerPubSub,
        );
        await waitForCondition(() => senderSubscription.activeRunId() === runId);

        const senderClaim = await senderRuntime.claimThreadOwnership(sender, scope, senderPubSub);
        expect(senderClaim.claimed).toBe(true);

        const send = async (contents: string) => {
          const result = senderRuntime.sendSignal(sender, { type: 'user-message', contents }, scope, senderPubSub);
          await expect(result.accepted).resolves.toMatchObject({ action: 'deliver' });

          let deliveredSignals: ReturnType<typeof ownerRuntime.drainPendingSignals> = [];
          await waitForCondition(() => {
            deliveredSignals = ownerRuntime.drainPendingSignals(runId, ownerPubSub);
            return deliveredSignals.length === 1;
          });
          expect(deliveredSignals[0]?.contents).toBe(contents);
        };

        await send('queued while remote run is active');
        await send('queued after claim ownership yielded');

        finishRun();
        senderClaim.unsubscribe();
        ownerClaim.unsubscribe();
        ownerSubscription.unsubscribe();
        senderSubscription.unsubscribe();
      } finally {
        await Promise.allSettled([ownerPubSub.close(), senderPubSub.close()]);
        await rm(tempDir, { recursive: true, force: true });
      }
    },
  );

  it.runIf(process.platform !== 'win32')(
    'broadcasts subscribed thread stream parts across UnixSocketPubSub runtime instances',
    async () => {
      const tempDir = await mkdtemp(join(tmpdir(), 'mastra-agent-signals-'));
      const ownerPubSub = new UnixSocketPubSub(join(tempDir, 'signals.sock'));
      const followerPubSub = new UnixSocketPubSub(join(tempDir, 'signals.sock'));
      const ownerRuntime = new AgentThreadStreamRuntime();
      const followerRuntime = new AgentThreadStreamRuntime();
      const owner = new Agent({
        id: 'unix-stream-agent',
        name: 'Unix Stream Owner Agent',
        instructions: 'Test',
        model: createTextStreamModel('owner response'),
      });
      const follower = new Agent({
        id: 'unix-stream-agent',
        name: 'Unix Stream Follower Agent',
        instructions: 'Test',
        model: createTextStreamModel('follower response'),
      });
      let finishRun!: () => void;
      const output = {
        runId: 'unix-run-1',
        status: 'running',
        fullStream: (async function* () {
          yield { type: 'text-delta', runId: 'unix-run-1', payload: { text: 'hello over uds' } };
          yield { type: 'finish', runId: 'unix-run-1', payload: {} };
        })(),
        _waitUntilFinished: () => new Promise<void>(resolve => (finishRun = resolve)),
      } as any;

      try {
        const ownerSubscription = await ownerRuntime.subscribeToThread(
          owner,
          { resourceId: 'unix-resource', threadId: 'unix-thread' },
          ownerPubSub,
        );
        const followerSubscription = await followerRuntime.subscribeToThread(
          follower,
          { resourceId: 'unix-resource', threadId: 'unix-thread' },
          followerPubSub,
        );
        const ownerRun = readNextRunWithParts(ownerSubscription.stream[Symbol.asyncIterator]());
        const followerRun = readNextRunWithParts(followerSubscription.stream[Symbol.asyncIterator]());

        ownerRuntime.registerRun(
          owner,
          output,
          { runId: 'unix-run-1', memory: { resource: 'unix-resource', thread: 'unix-thread' } } as any,
          ownerPubSub,
        );

        await expect(ownerRun).resolves.toMatchObject({ value: { text: 'hello over uds' }, done: false });
        await expect(followerRun).resolves.toMatchObject({ value: { text: 'hello over uds' }, done: false });
        finishRun();
        ownerSubscription.unsubscribe();
        followerSubscription.unsubscribe();
      } finally {
        await Promise.allSettled([ownerPubSub.close(), followerPubSub.close()]);
        await rm(tempDir, { recursive: true, force: true });
      }
    },
  );

  it.runIf(process.platform !== 'win32')(
    'lets a remote subscriber join an already-active UnixSocketPubSub run',
    async () => {
      const tempDir = await mkdtemp(join(tmpdir(), 'mastra-agent-late-subscriber-'));
      const ownerPubSub = new UnixSocketPubSub(join(tempDir, 'signals.sock'));
      const followerPubSub = new UnixSocketPubSub(join(tempDir, 'signals.sock'));
      const ownerRuntime = new AgentThreadStreamRuntime();
      const followerRuntime = new AgentThreadStreamRuntime();
      const owner = { id: 'late-subscriber-agent' } as Agent<any, any, any, any>;
      const follower = { id: 'late-subscriber-agent' } as Agent<any, any, any, any>;
      const runId = 'late-subscriber-run';
      const threadKey = 'late-subscriber-resource\u0000late-subscriber-thread';
      let firstPartBroadcasted!: () => void;
      let continueRun!: () => void;
      let finishLateRun!: () => void;
      let finishRun!: () => void;
      const firstPart = new Promise<void>(resolve => (firstPartBroadcasted = resolve));
      const continuePromise = new Promise<void>(resolve => (continueRun = resolve));
      const finishLateRunPromise = new Promise<void>(resolve => (finishLateRun = resolve));
      const finished = new Promise<void>(resolve => (finishRun = resolve));
      const output = {
        runId,
        status: 'running',
        fullStream: (async function* () {
          yield { type: 'text-delta', runId, payload: { text: 'before subscriber' } };
          firstPartBroadcasted();
          await continuePromise;
          yield { type: 'text-delta', runId, payload: { text: 'after subscriber' } };
          await finishLateRunPromise;
          yield { type: 'finish', runId, payload: {} };
          finishRun();
        })(),
        _waitUntilFinished: () => finished,
      } as any;

      try {
        // Fork (PF-4402): registerRun acquires the lease under its
        // process-attempt owner token; pre-seeding a raw run-id owner would make
        // that acquisition fail closed.
        void ownerRuntime.registerRun(
          owner,
          output,
          { runId, memory: { resource: 'late-subscriber-resource', thread: 'late-subscriber-thread' } } as any,
          ownerPubSub,
        );
        await withTimeout(firstPart, 'Timed out waiting for owner run to start');
        expect(decodeLeaseOwnerRunId(await followerPubSub.getLeaseOwner(threadKey))).toBe(runId);

        const followerSubscription = await followerRuntime.subscribeToThread(
          follower,
          { resourceId: 'late-subscriber-resource', threadId: 'late-subscriber-thread' },
          followerPubSub,
        );
        const followerPart = followerSubscription.stream[Symbol.asyncIterator]().next();

        continueRun();
        await expect(withTimeout(followerPart, 'Timed out waiting for late subscriber', 2_000)).resolves.toMatchObject({
          value: { runId, type: 'text-delta', payload: { text: 'after subscriber' } },
          done: false,
        });
        expect(followerSubscription.activeRunId()).toBe(runId);
        finishLateRun();
        followerSubscription.unsubscribe();
      } finally {
        finishLateRun();
        await Promise.allSettled([ownerPubSub.close(), followerPubSub.close()]);
        await rm(tempDir, { recursive: true, force: true });
      }
    },
  );

  it.runIf(process.platform !== 'win32')(
    'broadcasts to a remote subscriber without a same-runtime subscriber',
    async () => {
      const tempDir = await mkdtemp(join(tmpdir(), 'mastra-agent-remote-only-'));
      const ownerPubSub = new UnixSocketPubSub(join(tempDir, 'signals.sock'));
      const followerPubSub = new UnixSocketPubSub(join(tempDir, 'signals.sock'));
      const ownerRuntime = new AgentThreadStreamRuntime();
      const followerRuntime = new AgentThreadStreamRuntime();
      const owner = { id: 'remote-only-agent' } as Agent<any, any, any, any>;
      const follower = { id: 'remote-only-agent' } as Agent<any, any, any, any>;
      const runId = 'remote-only-run';
      let finishRun!: () => void;
      const output = {
        runId,
        status: 'running',
        fullStream: (async function* () {
          yield { type: 'text-delta', runId, payload: { text: 'remote only response' } };
          yield { type: 'finish', runId, payload: {} };
        })(),
        _waitUntilFinished: () => new Promise<void>(resolve => (finishRun = resolve)),
      } as any;

      try {
        const followerSubscription = await followerRuntime.subscribeToThread(
          follower,
          { resourceId: 'remote-only-resource', threadId: 'remote-only-thread' },
          followerPubSub,
        );
        const followerRun = readNextRun(followerSubscription.stream[Symbol.asyncIterator]());

        ownerRuntime.registerRun(
          owner,
          output,
          { runId, memory: { resource: 'remote-only-resource', thread: 'remote-only-thread' } } as any,
          ownerPubSub,
        );

        await expect(withTimeout(followerRun, 'Timed out waiting for remote-only subscriber')).resolves.toMatchObject({
          value: { runId, text: 'remote only response' },
          done: false,
        });
        finishRun();
        followerSubscription.unsubscribe();
      } finally {
        await Promise.allSettled([ownerPubSub.close(), followerPubSub.close()]);
        await rm(tempDir, { recursive: true, force: true });
      }
    },
  );

  it('supports cross-instance thread subscriptions through an injected PubSub without Mastra', async () => {
    const pubsub = new EventEmitterPubSub();
    const runner = new Agent({
      id: 'standalone-shared-agent',
      name: 'Standalone Shared Runner Agent',
      instructions: 'Test',
      model: createTextStreamModel('standalone shared response'),
      pubsub,
    });
    const observer = new Agent({
      id: 'standalone-shared-agent',
      name: 'Standalone Shared Observer Agent',
      instructions: 'Test',
      model: createTextStreamModel('standalone observer response'),
      pubsub,
    });

    const subscription = await observer.subscribeToThread({
      threadId: 'standalone-shared-thread',
      resourceId: 'standalone-shared-user',
    });
    const iterator = subscription.stream[Symbol.asyncIterator]();
    const firstRunPromise = readNextRun(iterator);

    const stream = await runner.stream('Hello', {
      memory: { thread: 'standalone-shared-thread', resource: 'standalone-shared-user' },
    });

    const subscribedRun = await firstRunPromise;
    expect(subscribedRun.value.runId).toBe(stream.runId);
    expect(subscribedRun.value.text).toBe('standalone shared response');

    const secondRunPromise = readNextRun(iterator);
    const signalResult = await runner.sendSignal(
      { type: 'user-message', contents: 'Hello from standalone shared signal' },
      {
        resourceId: 'standalone-shared-user',
        threadId: 'standalone-shared-thread',
        ifIdle: {
          streamOptions: { memory: { resource: 'standalone-shared-user', thread: 'standalone-shared-thread' } },
        },
      },
    );
    const signalRun = await secondRunPromise;
    await expect(signalResult.accepted).resolves.toMatchObject({ action: 'wake', runId: signalRun.value.runId });
    expect(signalResult.signal.id).toBeDefined();
    expect(signalRun.value.text).toBe('standalone shared response');

    subscription.unsubscribe();
  });

  it('broadcasts through async PubSub without consuming the caller fullStream', async () => {
    const pubsub = new AsyncFanoutPubSub();
    const runner = new Agent({
      id: 'async-shared-agent',
      name: 'Async Shared Runner Agent',
      instructions: 'Test',
      model: createTextStreamModel('async shared response'),
      pubsub,
    });
    const observer = new Agent({
      id: 'async-shared-agent',
      name: 'Async Shared Observer Agent',
      instructions: 'Test',
      model: createTextStreamModel('async observer response'),
      pubsub,
    });
    const subscription = await observer.subscribeToThread({
      resourceId: 'async-user',
      threadId: 'async-thread',
    });

    const stream = await runner.stream('Hello', {
      memory: { resource: 'async-user', thread: 'async-thread' },
    });

    await expect(readNextRun(stream.fullStream[Symbol.asyncIterator]())).resolves.toMatchObject({
      value: { runId: stream.runId, text: 'async shared response' },
      done: false,
    });
    await expect(readNextRun(subscription.stream[Symbol.asyncIterator]())).resolves.toMatchObject({
      value: { runId: stream.runId, text: 'async shared response' },
      done: false,
    });

    subscription.unsubscribe();
  });

  it('broadcasts async PubSub stream parts across runtime instances in order', async () => {
    const pubsub = new AsyncFanoutPubSub();
    const ownerRuntime = new AgentThreadStreamRuntime();
    const observerRuntime = new AgentThreadStreamRuntime();
    const runId = 'async-remote-run';
    const chunks = [
      { type: 'stream-start', runId, from: 'AGENT', payload: { warnings: [] } },
      { type: 'text-start', runId, from: 'AGENT', payload: { id: 'text-1' } },
      { type: 'text-delta', runId, from: 'AGENT', payload: { id: 'text-1', text: 'remote async response' } },
      { type: 'text-end', runId, from: 'AGENT', payload: { id: 'text-1' } },
      { type: 'finish', runId, from: 'AGENT', payload: {} },
    ];
    let finish!: () => void;
    const finished = new Promise<void>(resolve => {
      finish = resolve;
    });

    const subscription = await observerRuntime.subscribeToThread(
      { id: 'async-remote-observer' } as any,
      {
        resourceId: 'async-remote-user',
        threadId: 'async-remote-thread',
      },
      pubsub,
    );
    const nextRun = readNextRun(subscription.stream[Symbol.asyncIterator]());
    const output = {
      runId,
      status: 'running',
      fullStream: new ReadableStream({
        start(controller) {
          for (const chunk of chunks) controller.enqueue(chunk);
          finish();
          controller.close();
        },
      }),
      _waitUntilFinished: () => finished,
    };

    const completion = ownerRuntime.registerRun(
      { id: 'async-remote-owner' } as any,
      output as any,
      {
        runId,
        memory: { resource: 'async-remote-user', thread: 'async-remote-thread' },
      } as any,
      pubsub,
    )!;

    await expect(nextRun).resolves.toMatchObject({
      value: { runId, text: 'remote async response' },
      done: false,
    });
    await expect(completion).resolves.toBeUndefined();

    subscription.unsubscribe();
  });

  it('isolates standalone agents that use different injected pubsubs', async () => {
    const runner = new Agent({
      id: 'standalone-isolated-agent',
      name: 'Standalone Isolated Runner Agent',
      instructions: 'Test',
      model: createTextStreamModel('isolated response'),
      pubsub: new EventEmitterPubSub(),
    });
    const observer = new Agent({
      id: 'standalone-isolated-agent',
      name: 'Standalone Isolated Observer Agent',
      instructions: 'Test',
      model: createTextStreamModel('isolated observer response'),
      pubsub: new EventEmitterPubSub(),
    });

    const subscription = await observer.subscribeToThread({
      threadId: 'standalone-isolated-thread',
      resourceId: 'standalone-isolated-user',
    });
    const iterator = subscription.stream[Symbol.asyncIterator]();
    const nextRunPromise = readNextRun(iterator);

    await runner.stream('Hello', {
      memory: { thread: 'standalone-isolated-thread', resource: 'standalone-isolated-user' },
    });

    const result = await Promise.race([
      nextRunPromise.then(() => 'delivered'),
      new Promise<'timeout'>(resolve => setTimeout(() => resolve('timeout'), 20)),
    ]);
    expect(result).toBe('timeout');

    subscription.unsubscribe();
    await nextRunPromise;
  });

  it('passes parent PubSub to child agent execution without mutating shared child agents', async () => {
    const pubsub = new EventEmitterPubSub();
    const childCalls: Array<{ _pubsub?: PubSub }> = [];
    const createDelegatingModel = () => {
      let callCount = 0;
      return new MockLanguageModelV2({
        doGenerate: async () => {
          callCount += 1;
          if (callCount === 1) {
            return {
              rawCall: { rawPrompt: null, rawSettings: {} },
              finishReason: 'tool-calls' as const,
              usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
              text: '',
              content: [
                {
                  type: 'tool-call' as const,
                  toolCallId: `call-${callCount}`,
                  toolName: 'agent-child',
                  input: JSON.stringify({ prompt: 'ask child' }),
                },
              ],
              warnings: [],
            };
          }
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            finishReason: 'stop' as const,
            usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
            text: 'parent response',
            content: [{ type: 'text' as const, text: 'parent response' }],
            warnings: [],
          };
        },
      });
    };

    class CapturingChildAgent extends Agent {
      override async generate(_messages: any, options?: any) {
        childCalls.push(options ?? {});
        return {
          text: 'child response',
          finishReason: 'stop',
          runId: 'child-run',
          response: { dbMessages: [] },
        } as any;
      }

      override async stream(_messages: any, options?: any) {
        childCalls.push(options ?? {});
        const output = buildFakeOutput({
          runId: options?.runId ?? 'child-stream-run',
          fullOutput: {
            text: 'child response',
            finishReason: 'stop',
            response: { dbMessages: [] },
          },
        }) as any;
        return {
          ...output,
          messageList: {
            get: {
              response: {
                db: () => [],
              },
            },
          },
          toolResults: Promise.resolve([]),
        } as any;
      }
    }

    const child = new CapturingChildAgent({
      id: 'standalone-child-agent',
      name: 'Standalone Child Agent',
      instructions: 'Test',
      model: createTextStreamModel('child response'),
    });
    const parent = new Agent({
      id: 'standalone-parent-agent',
      name: 'Standalone Parent Agent',
      instructions: 'Test',
      model: createDelegatingModel(),
      pubsub,
      agents: { child },
    });

    await parent.generate('delegate to child', {
      runId: 'parent-run',
      maxSteps: 3,
    });

    expect(childCalls.at(-1)?._pubsub).toBe(pubsub);
    expect(child.getPubSub()).toBeUndefined();

    const secondPubSub = new EventEmitterPubSub();
    const secondParent = new Agent({
      id: 'second-standalone-parent-agent',
      name: 'Second Standalone Parent Agent',
      instructions: 'Test',
      model: createDelegatingModel(),
      pubsub: secondPubSub,
      agents: { child },
    });

    await secondParent.generate('delegate to child again', {
      runId: 'second-parent-run',
      maxSteps: 3,
    });

    expect(childCalls.at(-1)?._pubsub).toBe(secondPubSub);
    expect(child.getPubSub()).toBeUndefined();
    expect(child.hasOwnPubSub()).toBe(false);
  });

  it('preserves an injected PubSub when forking an agent', () => {
    const pubsub = new AsyncFanoutPubSub();
    const agent = new Agent({
      id: 'forked-agent-pubsub',
      name: 'Forked Agent PubSub',
      instructions: 'Test',
      model: createTextStreamModel('forked response'),
    });

    agent.__setPubSub(pubsub);
    const fork = agent.__fork();

    expect(fork.getPubSub()).toBe(pubsub);
    expect(fork.hasOwnPubSub()).toBe(false);
  });

  it('keeps one PubSub for a stream run when the agent gets a PubSub during execution setup', async () => {
    const swappedPubSub = new EventEmitterPubSub();
    let markDefaultOptionsStarted!: () => void;
    let releaseDefaultOptions!: () => void;
    const defaultOptionsStarted = new Promise<void>(resolve => {
      markDefaultOptionsStarted = resolve;
    });
    const defaultOptionsReleased = new Promise<void>(resolve => {
      releaseDefaultOptions = resolve;
    });
    let releaseFirst!: () => void;
    const firstFinished = new Promise<void>(resolve => {
      releaseFirst = resolve;
    });
    let streamCount = 0;
    const prompts: any[][] = [];

    const runner = new Agent({
      id: 'pubsub-snapshot-agent',
      name: 'PubSub Snapshot Runner',
      instructions: 'Test',
      defaultOptions: async () => {
        markDefaultOptionsStarted();
        await defaultOptionsReleased;
        return {};
      },
      model: new MockLanguageModelV2({
        doStream: async ({ prompt }) => {
          streamCount += 1;
          const callIndex = streamCount;
          prompts.push(prompt);
          const responseText = callIndex === 1 ? 'first response' : 'signal response';
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: new ReadableStream({
              async start(controller) {
                controller.enqueue({ type: 'stream-start', warnings: [] });
                controller.enqueue({
                  type: 'response-metadata',
                  id: `id-${callIndex}`,
                  modelId: 'mock-model-id',
                  timestamp: new Date(0),
                });
                controller.enqueue({ type: 'text-start', id: 'text-1' });
                controller.enqueue({ type: 'text-delta', id: 'text-1', delta: responseText });
                controller.enqueue({ type: 'text-end', id: 'text-1' });
                if (callIndex === 1) {
                  await firstFinished;
                }
                controller.enqueue({
                  type: 'finish',
                  finishReason: 'stop',
                  usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
                });
                controller.close();
              },
            }),
          };
        },
      }),
    });
    const initialObserver = new Agent({
      id: 'pubsub-snapshot-agent',
      name: 'Initial PubSub Observer',
      instructions: 'Test',
      model: createTextStreamModel('initial observer response'),
    });
    const initialSubscription = await initialObserver.subscribeToThread({
      threadId: 'pubsub-snapshot-thread',
      resourceId: 'pubsub-snapshot-user',
    });
    const initialIterator = initialSubscription.stream[Symbol.asyncIterator]();
    const initialNextRun = readNextRun(initialIterator);

    const streamPromise = runner.stream('Hello', {
      memory: { thread: 'pubsub-snapshot-thread', resource: 'pubsub-snapshot-user' },
    });
    await defaultOptionsStarted;
    runner.__setPubSub(swappedPubSub);
    const signalResult = await runner.sendSignal(
      { type: 'user-message', contents: 'Hello while running' },
      { resourceId: 'pubsub-snapshot-user', threadId: 'pubsub-snapshot-thread' },
    );
    expect(signalResult.runId).toEqual(expect.any(String));
    releaseDefaultOptions();

    const stream = await streamPromise;
    await expect(waitForActiveRun(initialSubscription)).resolves.toBe(stream.runId);
    expect(runner.getRunOutput(stream.runId)).toBe(stream);
    expect(signalResult.runId).toBe(stream.runId);
    await expect(signalResult.accepted).resolves.toMatchObject({ action: 'deliver', runId: stream.runId });

    releaseFirst();
    const initialRun = await Promise.race([
      initialNextRun,
      new Promise<'timeout'>(resolve => setTimeout(() => resolve('timeout'), 500)),
    ]);
    expect(initialRun).not.toBe('timeout');
    expect(initialRun).toMatchObject({
      value: { runId: stream.runId, text: 'first response' },
      done: false,
    });
    await expect(stream.text).resolves.toBe('first response');
    expect(JSON.stringify(prompts)).toContain('Hello while running');

    initialSubscription.unsubscribe();
  });

  it('keeps one PubSub for a default idle wake signal when ifIdle options are omitted', async () => {
    const swappedPubSub = new EventEmitterPubSub();
    let markDefaultOptionsStarted!: () => void;
    let releaseDefaultOptions!: () => void;
    const defaultOptionsStarted = new Promise<void>(resolve => {
      markDefaultOptionsStarted = resolve;
    });
    const defaultOptionsReleased = new Promise<void>(resolve => {
      releaseDefaultOptions = resolve;
    });

    const runner = new Agent({
      id: 'default-idle-wake-pubsub-agent',
      name: 'Default Idle Wake PubSub Agent',
      instructions: 'Test',
      defaultOptions: async () => {
        markDefaultOptionsStarted();
        await defaultOptionsReleased;
        return {};
      },
      model: createTextStreamModel('default idle wake response'),
    });
    const initialObserver = new Agent({
      id: 'default-idle-wake-pubsub-agent',
      name: 'Default Idle Wake Initial Observer',
      instructions: 'Test',
      model: createTextStreamModel('observer response'),
    });
    const initialSubscription = await initialObserver.subscribeToThread({
      threadId: 'default-idle-wake-thread',
      resourceId: 'default-idle-wake-user',
    });
    const initialNextRun = readNextRun(initialSubscription.stream[Symbol.asyncIterator]());

    const signalResult = runner.sendSignal(
      { type: 'user-message', contents: 'Wake without explicit ifIdle' },
      { resourceId: 'default-idle-wake-user', threadId: 'default-idle-wake-thread' },
    );
    expect(signalResult.output).toBeDefined();
    await defaultOptionsStarted;
    runner.__setPubSub(swappedPubSub);
    releaseDefaultOptions();

    await expect(signalResult.output).resolves.toMatchObject({ runId: signalResult.runId });
    const initialRun = await Promise.race([
      initialNextRun,
      new Promise<'timeout'>(resolve => setTimeout(() => resolve('timeout'), 500)),
    ]);
    expect(initialRun).not.toBe('timeout');
    expect(initialRun).toMatchObject({
      value: { runId: signalResult.runId, text: 'default idle wake response' },
      done: false,
    });

    initialSubscription.unsubscribe();
  });

  it('tracks idle wake PubSub from ifIdle streamOptions memory when top-level thread target is omitted', async () => {
    const swappedPubSub = new EventEmitterPubSub();
    let markDefaultOptionsStarted!: () => void;
    let releaseDefaultOptions!: () => void;
    const defaultOptionsStarted = new Promise<void>(resolve => {
      markDefaultOptionsStarted = resolve;
    });
    const defaultOptionsReleased = new Promise<void>(resolve => {
      releaseDefaultOptions = resolve;
    });

    const runner = new Agent({
      id: 'stream-options-idle-wake-pubsub-agent',
      name: 'Stream Options Idle Wake PubSub Agent',
      instructions: 'Test',
      defaultOptions: async () => {
        markDefaultOptionsStarted();
        await defaultOptionsReleased;
        return {};
      },
      model: createTextStreamModel('stream options idle wake response'),
    });

    const signalResult = runner.sendSignal({ type: 'user-message', contents: 'Wake from streamOptions target' }, {
      ifIdle: {
        streamOptions: {
          memory: { resource: 'stream-options-idle-wake-user', thread: 'stream-options-idle-wake-thread' },
        },
      },
    } as any);
    expect(signalResult.output).toBeDefined();
    await defaultOptionsStarted;
    runner.__setPubSub(swappedPubSub);
    releaseDefaultOptions();

    const output = await signalResult.output;
    expect(output).toMatchObject({ runId: signalResult.runId });
    expect(runner.getRunOutput(signalResult.runId!)).toBe(output);
  });

  it('honors an injected PubSub for streamUntilIdle when the agent PubSub changes', async () => {
    const initialPubSub = new EventEmitterPubSub();
    const swappedPubSub = new EventEmitterPubSub();
    const runner = new Agent({
      id: 'stream-until-idle-pubsub-agent',
      name: 'Stream Until Idle PubSub Agent',
      instructions: 'Test',
      model: createTextStreamModel('stream until idle response'),
    });
    runner.__setPubSub(swappedPubSub);

    const observer = new Agent({
      id: 'stream-until-idle-pubsub-agent',
      name: 'Stream Until Idle PubSub Observer',
      instructions: 'Test',
      model: createTextStreamModel('observer response'),
    });
    observer.__setPubSub(initialPubSub);
    const subscription = await observer.subscribeToThread({
      threadId: 'stream-until-idle-thread',
      resourceId: 'stream-until-idle-user',
    });
    const nextRun = readNextRun(subscription.stream[Symbol.asyncIterator]());

    const stream = await runner.streamUntilIdle('Hello', {
      memory: { thread: 'stream-until-idle-thread', resource: 'stream-until-idle-user' },
      _pubsub: initialPubSub,
    } as any);

    await expect(stream.text).resolves.toBe('stream until idle response');
    await expect(nextRun).resolves.toMatchObject({
      value: { runId: stream.runId, text: 'stream until idle response' },
      done: false,
    });

    subscription.unsubscribe();
  });

  it('honors an injected PubSub when test agents register streams through the internal hook', async () => {
    const initialPubSub = new EventEmitterPubSub();
    const swappedPubSub = new EventEmitterPubSub();
    const agent = new Agent({
      id: 'internal-register-pubsub-agent',
      name: 'Internal Register PubSub Agent',
      instructions: 'Test',
      model: createTextStreamModel('unused'),
    });
    agent.__setPubSub(swappedPubSub);
    const observer = new Agent({
      id: 'internal-register-pubsub-agent',
      name: 'Internal Register Observer',
      instructions: 'Test',
      model: createTextStreamModel('observer response'),
    });
    observer.__setPubSub(initialPubSub);
    const subscription = await observer.subscribeToThread({
      threadId: 'internal-register-thread',
      resourceId: 'internal-register-user',
    });
    const nextRun = readNextRun(subscription.stream[Symbol.asyncIterator]());
    const output = buildFakeOutput({
      runId: 'internal-register-run',
      fullOutput: { text: 'internal response', finishReason: 'stop', usage: {} },
      chunks: [
        { runId: 'internal-register-run', type: 'text-delta', payload: { text: 'internal response' } },
        { runId: 'internal-register-run', type: 'finish', payload: {} },
      ],
    });

    agent._internalRegisterStreamRun(output, {
      runId: 'internal-register-run',
      memory: { resource: 'internal-register-user', thread: 'internal-register-thread' },
      _pubsub: initialPubSub,
    } as any);
    expect(agent.getRunOutput('internal-register-run')).toBe(output);

    await expect(nextRun).resolves.toMatchObject({
      value: { runId: 'internal-register-run', text: 'internal response' },
      done: false,
    });

    subscription.unsubscribe();
  });

  it('re-reserves a pre-default stream when default options change the request-context thread target', async () => {
    let markDefaultOptionsStarted!: () => void;
    let releaseDefaultOptions!: () => void;
    const defaultOptionsStarted = new Promise<void>(resolve => {
      markDefaultOptionsStarted = resolve;
    });
    const defaultOptionsReleased = new Promise<void>(resolve => {
      releaseDefaultOptions = resolve;
    });
    const requestContext = new RequestContext();
    requestContext.set(MASTRA_RESOURCE_ID_KEY, 'default-context-user');
    requestContext.set(MASTRA_THREAD_ID_KEY, 'default-context-thread');

    const runner = new Agent({
      id: 'default-context-reservation-agent',
      name: 'Default Context Reservation Agent',
      instructions: 'Test',
      defaultOptions: async () => {
        markDefaultOptionsStarted();
        await defaultOptionsReleased;
        return { requestContext };
      },
      model: createTextStreamModel('default context response'),
    });
    const observer = new Agent({
      id: 'default-context-reservation-agent',
      name: 'Default Context Reservation Observer',
      instructions: 'Test',
      model: createTextStreamModel('observer response'),
    });
    const subscription = await observer.subscribeToThread({
      threadId: 'default-context-thread',
      resourceId: 'default-context-user',
    });
    const nextRun = readNextRun(subscription.stream[Symbol.asyncIterator]());

    const streamPromise = runner.stream('Hello', {
      memory: { thread: 'explicit-before-default-thread', resource: 'explicit-before-default-user' },
    });
    await defaultOptionsStarted;
    releaseDefaultOptions();

    const stream = await streamPromise;
    await expect(nextRun).resolves.toMatchObject({
      value: { runId: stream.runId, text: 'default context response' },
      done: false,
    });

    subscription.unsubscribe();
  });

  it('preserves accepted setup signals when default options retarget a reserved stream', async () => {
    let markDefaultOptionsStarted!: () => void;
    let releaseDefaultOptions!: () => void;
    const defaultOptionsStarted = new Promise<void>(resolve => {
      markDefaultOptionsStarted = resolve;
    });
    const defaultOptionsReleased = new Promise<void>(resolve => {
      releaseDefaultOptions = resolve;
    });
    const requestContext = new RequestContext();
    requestContext.set(MASTRA_RESOURCE_ID_KEY, 'retarget-signal-context-user');
    requestContext.set(MASTRA_THREAD_ID_KEY, 'retarget-signal-context-thread');
    const prompts: any[][] = [];

    const runner = new Agent({
      id: 'retarget-preserve-signal-agent',
      name: 'Retarget Preserve Signal Agent',
      instructions: 'Test',
      defaultOptions: async () => {
        markDefaultOptionsStarted();
        await defaultOptionsReleased;
        return { requestContext };
      },
      model: new MockLanguageModelV2({
        doStream: async ({ prompt }) => {
          prompts.push(prompt);
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: convertArrayToReadableStream([
              { type: 'stream-start', warnings: [] },
              { type: 'response-metadata', id: 'id-0', modelId: 'mock-model-id', timestamp: new Date(0) },
              { type: 'text-start', id: 'text-1' },
              { type: 'text-delta', id: 'text-1', delta: 'retarget response' },
              { type: 'text-end', id: 'text-1' },
              { type: 'finish', finishReason: 'stop', usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 } },
            ]),
          };
        },
      }),
    });

    const streamPromise = runner.stream('Hello', {
      memory: { thread: 'retarget-signal-original-thread', resource: 'retarget-signal-original-user' },
    });
    await defaultOptionsStarted;
    const signalResult = runner.sendSignal(
      { type: 'user-message', contents: 'accepted before retarget' },
      { resourceId: 'retarget-signal-original-user', threadId: 'retarget-signal-original-thread' },
    );
    releaseDefaultOptions();

    const stream = await streamPromise;
    expect(signalResult.runId).toBe(stream.runId);
    await expect(stream.text).resolves.toBe('retarget response');
    expect(JSON.stringify(prompts)).toContain('accepted before retarget');
  });

  it('forgets a re-reserved PubSub mapping when stream setup fails before preparation', async () => {
    const swappedPubSub = new EventEmitterPubSub();
    let useUnsupportedModel = true;
    const requestContext = new RequestContext();
    requestContext.set(MASTRA_RESOURCE_ID_KEY, 'failed-rereserve-context-user');
    requestContext.set(MASTRA_THREAD_ID_KEY, 'failed-rereserve-context-thread');
    const unsupportedModel = new MockLanguageModelV1({
      doGenerate: async () => ({
        rawCall: { rawPrompt: null, rawSettings: {} },
        finishReason: 'stop',
        usage: { promptTokens: 1, completionTokens: 1 },
        text: 'unsupported',
      }),
      doStream: async () => ({
        rawCall: { rawPrompt: null, rawSettings: {} },
        stream: convertArrayToReadableStream([]),
      }),
    });
    const supportedModel = createTextStreamModel('post failure response');

    const runner = new Agent({
      id: 'failed-rereserve-pubsub-agent',
      name: 'Failed Rereserve PubSub Agent',
      instructions: 'Test',
      defaultOptions: async () => ({ requestContext }),
      model: () => (useUnsupportedModel ? unsupportedModel : supportedModel),
    });

    await expect(
      runner.stream('Hello', {
        runId: 'failed-rereserve-explicit-run',
        memory: { thread: 'failed-rereserve-original-thread', resource: 'failed-rereserve-original-user' },
      }),
    ).rejects.toThrow('not compatible with stream()');

    useUnsupportedModel = false;
    runner.__setPubSub(swappedPubSub);
    const subscription = await runner.subscribeToThread({
      threadId: 'failed-rereserve-context-thread',
      resourceId: 'failed-rereserve-context-user',
    });
    const nextRun = readNextRun(subscription.stream[Symbol.asyncIterator]());
    const stream = await runner.stream('Hello again', {
      runId: 'failed-rereserve-explicit-run',
      memory: { thread: 'failed-rereserve-context-thread', resource: 'failed-rereserve-context-user' },
    });

    expect(stream.runId).toBe('failed-rereserve-explicit-run');
    await expect(stream.text).resolves.toBe('post failure response');
    const observedRun = await Promise.race([
      nextRun,
      new Promise<'timeout'>(resolve => setTimeout(() => resolve('timeout'), 500)),
    ]);
    expect(observedRun).not.toBe('timeout');
    expect(observedRun).toMatchObject({
      value: { runId: stream.runId, text: 'post failure response' },
      done: false,
    });

    subscription.unsubscribe();
  });

  it('reserves request-context scoped streams while default options are pending', async () => {
    let markDefaultOptionsStarted!: () => void;
    let releaseDefaultOptions!: () => void;
    const defaultOptionsStarted = new Promise<void>(resolve => {
      markDefaultOptionsStarted = resolve;
    });
    const defaultOptionsReleased = new Promise<void>(resolve => {
      releaseDefaultOptions = resolve;
    });
    const requestContext = new RequestContext();
    requestContext.set(MASTRA_RESOURCE_ID_KEY, 'request-context-reserved-user');
    requestContext.set(MASTRA_THREAD_ID_KEY, 'request-context-reserved-thread');

    const runner = new Agent({
      id: 'request-context-reserved-agent',
      name: 'Request Context Reserved Agent',
      instructions: 'Test',
      defaultOptions: async () => {
        markDefaultOptionsStarted();
        await defaultOptionsReleased;
        return {};
      },
      model: createTextStreamModel('request context reserved response'),
    });

    const streamPromise = runner.stream('Hello', { requestContext });
    await defaultOptionsStarted;
    const signalResult = runner.sendSignal(
      { type: 'user-message', contents: 'Hello while request-context stream is starting' },
      { resourceId: 'request-context-reserved-user', threadId: 'request-context-reserved-thread' },
    );
    releaseDefaultOptions();

    const stream = await streamPromise;
    expect(signalResult.runId).toBe(stream.runId);
    await expect(signalResult.accepted).resolves.toMatchObject({ action: 'deliver', runId: stream.runId });
  });

  it('does not expose explicit thread streams when request context preflight fails', async () => {
    const requestContext = new RequestContext();
    requestContext.set('allowed', false);

    const runner = new Agent({
      id: 'preflight-denied-reservation-agent',
      name: 'Preflight Denied Reservation Agent',
      instructions: 'Test',
      requestContextSchema: z.object({ allowed: z.literal(true) }),
      model: createTextStreamModel('unused denied response'),
    });

    await expect(
      runner.stream('Hello', {
        runId: 'preflight-denied-run',
        memory: { resource: 'preflight-denied-user', thread: 'preflight-denied-thread' },
        requestContext,
      }),
    ).rejects.toThrow('Request context validation failed');

    const signalResult = runner.sendSignal(
      { type: 'user-message', contents: 'Should not attach to denied setup run' },
      {
        resourceId: 'preflight-denied-user',
        threadId: 'preflight-denied-thread',
        ifIdle: { behavior: 'discard' },
      },
    );

    expect(signalResult.runId).not.toBe('preflight-denied-run');
  });

  it('does not attach idle signals to explicit-context streams before preflight passes', async () => {
    const requestContext = new RequestContext();
    requestContext.set('allowed', false);

    const runner = new Agent({
      id: 'explicit-preflight-denied-reservation-agent',
      name: 'Explicit Preflight Denied Reservation Agent',
      instructions: 'Test',
      requestContextSchema: z.object({ allowed: z.literal(true) }),
      model: createTextStreamModel('unused denied explicit idle response'),
    });

    const wake = runner.sendSignal(
      { type: 'user-message', contents: 'Start denied explicit-context idle stream' },
      {
        resourceId: 'explicit-preflight-denied-user',
        threadId: 'explicit-preflight-denied-thread',
        ifIdle: { behavior: 'wake', streamOptions: { requestContext } },
      },
    );
    void wake.output?.catch(() => {});

    const followUp = runner.sendSignal(
      { type: 'user-message', contents: 'Should not attach before explicit preflight passes' },
      {
        resourceId: 'explicit-preflight-denied-user',
        threadId: 'explicit-preflight-denied-thread',
        ifIdle: { behavior: 'discard' },
      },
    );

    expect(followUp.runId).not.toBe(wake.runId);
    await expect(wake.output).rejects.toThrow('Request context validation failed');
  });

  it('does not reserve explicit-context streams before preflight passes', async () => {
    let markPreflightStarted!: () => void;
    let releasePreflight!: () => void;
    const preflightStarted = new Promise<void>(resolve => {
      markPreflightStarted = resolve;
    });
    const preflightReleased = new Promise<void>(resolve => {
      releasePreflight = resolve;
    });
    const requestContext = new RequestContext();
    requestContext.set('allowed', true);

    const runner = new Agent({
      id: 'explicit-preflight-allowed-reservation-agent',
      name: 'Explicit Preflight Allowed Reservation Agent',
      instructions: 'Test',
      requestContextSchema: z.object({ allowed: z.literal(true) }).superRefine(async () => {
        markPreflightStarted();
        await preflightReleased;
      }),
      model: createTextStreamModel('allowed explicit preflight response'),
    });

    const streamPromise = runner.stream('Hello', {
      memory: { resource: 'explicit-preflight-allowed-user', thread: 'explicit-preflight-allowed-thread' },
      requestContext,
    });
    await preflightStarted;

    const followUp = runner.sendSignal(
      { type: 'user-message', contents: 'Should not attach before explicit preflight passes' },
      {
        resourceId: 'explicit-preflight-allowed-user',
        threadId: 'explicit-preflight-allowed-thread',
        ifIdle: { behavior: 'discard' },
      },
    );
    expect(() =>
      runner.sendSignal(
        { type: 'user-message', contents: 'Thread-only signal should not see a pending reservation' },
        {
          threadId: 'explicit-preflight-allowed-thread',
        },
      ),
    ).toThrow('No active agent run found for signal target');
    const explicitActivePolicyFollowUp = runner.sendSignal(
      { type: 'user-message', contents: 'Explicit active-deliver signal should not attach before preflight passes' },
      {
        resourceId: 'explicit-preflight-allowed-user',
        threadId: 'explicit-preflight-allowed-thread',
        ifActive: { behavior: 'deliver' },
        ifIdle: { behavior: 'discard' },
      },
    );
    releasePreflight();

    const stream = await streamPromise;
    expect(followUp.runId).not.toBe(stream.runId);
    expect(explicitActivePolicyFollowUp.runId).not.toBe(stream.runId);
    await expect(stream.text).resolves.toBe('allowed explicit preflight response');
  });

  it('reserves explicit-context streams after preflight passes before defaults finish', async () => {
    let markPreflightStarted!: () => void;
    let releasePreflight!: () => void;
    let markDefaultOptionsStarted!: () => void;
    let releaseDefaultOptions!: () => void;
    const preflightStarted = new Promise<void>(resolve => {
      markPreflightStarted = resolve;
    });
    const preflightReleased = new Promise<void>(resolve => {
      releasePreflight = resolve;
    });
    const defaultOptionsStarted = new Promise<void>(resolve => {
      markDefaultOptionsStarted = resolve;
    });
    const defaultOptionsReleased = new Promise<void>(resolve => {
      releaseDefaultOptions = resolve;
    });
    const requestContext = new RequestContext();
    requestContext.set('allowed', true);

    const runner = new Agent({
      id: 'explicit-preflight-defaults-pending-agent',
      name: 'Explicit Preflight Defaults Pending Agent',
      instructions: 'Test',
      requestContextSchema: z.object({ allowed: z.literal(true) }).superRefine(async () => {
        markPreflightStarted();
        await preflightReleased;
      }),
      defaultOptions: async () => {
        markDefaultOptionsStarted();
        await defaultOptionsReleased;
        return {};
      },
      model: createTextStreamModel('allowed explicit preflight defaults response'),
    });

    const streamPromise = runner.stream('Hello', {
      runId: 'explicit-preflight-defaults-run',
      memory: { resource: 'explicit-preflight-defaults-user', thread: 'explicit-preflight-defaults-thread' },
      requestContext,
    });
    await preflightStarted;

    const beforePreflightFollowUp = runner.sendSignal(
      { type: 'user-message', contents: 'Should not attach before preflight passes' },
      {
        resourceId: 'explicit-preflight-defaults-user',
        threadId: 'explicit-preflight-defaults-thread',
        ifIdle: { behavior: 'discard' },
      },
    );
    expect(beforePreflightFollowUp.runId).not.toBe('explicit-preflight-defaults-run');

    releasePreflight();
    await defaultOptionsStarted;

    const afterPreflightFollowUp = runner.sendSignal(
      { type: 'user-message', contents: 'Should attach after preflight passes while defaults are pending' },
      {
        resourceId: 'explicit-preflight-defaults-user',
        threadId: 'explicit-preflight-defaults-thread',
        ifIdle: { behavior: 'discard' },
      },
    );
    releaseDefaultOptions();

    expect(afterPreflightFollowUp.runId).toBe('explicit-preflight-defaults-run');
    const stream = await streamPromise;
    expect(stream.runId).toBe('explicit-preflight-defaults-run');
    await expect(stream.text).resolves.toBe('allowed explicit preflight defaults response');
  });

  it('keeps direct preflight stream output waiters on the captured PubSub', async () => {
    const initialPubSub = new EventEmitterPubSub();
    const swappedPubSub = new EventEmitterPubSub();
    let markPreflightStarted!: () => void;
    let releasePreflight!: () => void;
    const preflightStarted = new Promise<void>(resolve => {
      markPreflightStarted = resolve;
    });
    const preflightReleased = new Promise<void>(resolve => {
      releasePreflight = resolve;
    });
    const requestContext = new RequestContext();
    requestContext.set('allowed', true);

    const runner = new Agent({
      id: 'direct-preflight-pubsub-agent',
      name: 'Direct Preflight PubSub Agent',
      instructions: 'Test',
      requestContextSchema: z.object({ allowed: z.literal(true) }).superRefine(async () => {
        markPreflightStarted();
        await preflightReleased;
      }),
      model: createTextStreamModel('direct preflight pubsub response'),
    });
    runner.__setPubSub(initialPubSub);

    const streamPromise = runner.stream('Hello', {
      runId: 'direct-preflight-pubsub-run',
      memory: { resource: 'direct-preflight-pubsub-user', thread: 'direct-preflight-pubsub-thread' },
      requestContext,
    });
    await preflightStarted;

    runner.__setPubSub(swappedPubSub);
    const outputPromise = runner.waitForRunOutput('direct-preflight-pubsub-run');
    releasePreflight();

    const output = await Promise.race([
      outputPromise,
      new Promise<'timeout'>(resolve => setTimeout(() => resolve('timeout'), 500)),
    ]);
    expect(output).not.toBe('timeout');
    if (output === 'timeout') return;
    expect(output.runId).toBe('direct-preflight-pubsub-run');
    await expect(output.text).resolves.toBe('direct preflight pubsub response');
    await expect(streamPromise).resolves.toBe(output);
  });

  it('does not tombstone an admitted stream when a duplicate explicit run id is rejected', async () => {
    let markModelStarted!: () => void;
    let releaseModel!: () => void;
    const modelStarted = new Promise<void>(resolve => {
      markModelStarted = resolve;
    });
    const modelReleased = new Promise<void>(resolve => {
      releaseModel = resolve;
    });
    const requestContext = new RequestContext();
    requestContext.set('allowed', true);

    const runner = new Agent({
      id: 'duplicate-preflight-run-id-agent',
      name: 'Duplicate Preflight Run Id Agent',
      instructions: 'Test',
      requestContextSchema: z.object({ allowed: z.literal(true) }),
      model: new MockLanguageModelV2({
        doStream: async () => {
          markModelStarted();
          await modelReleased;
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: convertArrayToReadableStream([
              { type: 'stream-start', warnings: [] },
              {
                type: 'response-metadata',
                id: 'duplicate-preflight',
                modelId: 'mock-model-id',
                timestamp: new Date(0),
              },
              { type: 'text-start', id: 'text-1' },
              { type: 'text-delta', id: 'text-1', delta: 'first admitted response' },
              { type: 'text-end', id: 'text-1' },
              {
                type: 'finish',
                finishReason: 'stop',
                usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
              },
            ]),
          };
        },
      }),
    });

    const firstStreamPromise = runner.stream('First', {
      runId: 'duplicate-preflight-run',
      memory: { resource: 'duplicate-preflight-user', thread: 'duplicate-preflight-thread' },
      requestContext,
    });
    await modelStarted;

    await expect(
      runner.stream('Duplicate', {
        runId: 'duplicate-preflight-run',
        memory: { resource: 'duplicate-preflight-user', thread: 'duplicate-preflight-thread' },
        requestContext,
      }),
    ).rejects.toThrow('already reserved');

    releaseModel();
    const firstStream = await firstStreamPromise;
    expect(firstStream.runId).toBe('duplicate-preflight-run');
    await expect(firstStream.text).resolves.toBe('first admitted response');
  });

  it('keeps explicit-context idle reservations when no preflight boundary is configured', async () => {
    let markDefaultOptionsStarted!: () => void;
    let releaseDefaultOptions!: () => void;
    const defaultOptionsStarted = new Promise<void>(resolve => {
      markDefaultOptionsStarted = resolve;
    });
    const defaultOptionsReleased = new Promise<void>(resolve => {
      releaseDefaultOptions = resolve;
    });
    const requestContext = new RequestContext();

    const runner = new Agent({
      id: 'explicit-context-no-preflight-agent',
      name: 'Explicit Context No Preflight Agent',
      instructions: 'Test',
      defaultOptions: async () => {
        markDefaultOptionsStarted();
        await defaultOptionsReleased;
        return {};
      },
      model: createTextStreamModel('explicit context no preflight response'),
    });

    const wake = runner.sendSignal(
      { type: 'user-message', contents: 'Start explicit-context idle stream' },
      {
        resourceId: 'explicit-context-no-preflight-user',
        threadId: 'explicit-context-no-preflight-thread',
        ifIdle: { behavior: 'wake', streamOptions: { requestContext } },
      },
    );
    await defaultOptionsStarted;

    const followUp = runner.sendSignal(
      { type: 'user-message', contents: 'Should attach when preflight cannot reject' },
      {
        resourceId: 'explicit-context-no-preflight-user',
        threadId: 'explicit-context-no-preflight-thread',
        ifIdle: { behavior: 'discard' },
      },
    );
    releaseDefaultOptions();

    expect(followUp.runId).toBe(wake.runId);
    await expect(wake.output).resolves.toMatchObject({ runId: wake.runId });
  });

  it('does not attach idle signals to default-context streams before preflight passes', async () => {
    let markDefaultOptionsStarted!: () => void;
    let releaseDefaultOptions!: () => void;
    const defaultOptionsStarted = new Promise<void>(resolve => {
      markDefaultOptionsStarted = resolve;
    });
    const defaultOptionsReleased = new Promise<void>(resolve => {
      releaseDefaultOptions = resolve;
    });
    const requestContext = new RequestContext();
    requestContext.set('allowed', false);

    const runner = new Agent({
      id: 'default-preflight-denied-reservation-agent',
      name: 'Default Preflight Denied Reservation Agent',
      instructions: 'Test',
      requestContextSchema: z.object({ allowed: z.literal(true) }),
      defaultOptions: async () => {
        markDefaultOptionsStarted();
        await defaultOptionsReleased;
        return { requestContext };
      },
      model: createTextStreamModel('unused denied idle response'),
    });

    const wake = runner.sendSignal(
      { type: 'user-message', contents: 'Start denied idle stream' },
      {
        resourceId: 'default-preflight-denied-user',
        threadId: 'default-preflight-denied-thread',
      },
    );
    await defaultOptionsStarted;
    const outputPromise = runner.waitForRunOutput(wake.runId);
    void wake.output?.catch(() => {});

    const followUp = runner.sendSignal(
      { type: 'user-message', contents: 'Should not attach before preflight passes' },
      {
        resourceId: 'default-preflight-denied-user',
        threadId: 'default-preflight-denied-thread',
        ifIdle: { behavior: 'discard' },
      },
    );
    const activePolicyResult = runner.sendSignal(
      { type: 'user-message', contents: 'Should not treat preflight-pending run as active' },
      {
        resourceId: 'default-preflight-denied-user',
        threadId: 'default-preflight-denied-thread',
        ifActive: { behavior: 'discard' },
        ifIdle: { behavior: 'discard' },
      },
    );
    const plainFollowUp = runner.sendSignal(
      { type: 'user-message', contents: 'Plain signal should not attach before preflight passes' },
      {
        resourceId: 'default-preflight-denied-user',
        threadId: 'default-preflight-denied-thread',
      },
    );
    void plainFollowUp.output?.catch(() => {});
    releaseDefaultOptions();

    expect(followUp.runId).not.toBe(wake.runId);
    expect(activePolicyResult.runId).not.toBe(wake.runId);
    expect(plainFollowUp.runId).not.toBe(wake.runId);
    await expect(outputPromise).rejects.toThrow(`Agent thread run id "${wake.runId}" was rejected`);
    await expect(wake.output).rejects.toThrow('Request context validation failed');
  });

  it('keeps idle wake output waiters pending while default-context preflight is pending', async () => {
    const initialPubSub = new EventEmitterPubSub();
    const swappedPubSub = new EventEmitterPubSub();
    let markDefaultOptionsStarted!: () => void;
    let releaseDefaultOptions!: () => void;
    const defaultOptionsStarted = new Promise<void>(resolve => {
      markDefaultOptionsStarted = resolve;
    });
    const defaultOptionsReleased = new Promise<void>(resolve => {
      releaseDefaultOptions = resolve;
    });
    const requestContext = new RequestContext();
    requestContext.set('allowed', true);

    const runner = new Agent({
      id: 'default-preflight-valid-waiter-agent',
      name: 'Default Preflight Valid Waiter Agent',
      instructions: 'Test',
      requestContextSchema: z.object({ allowed: z.literal(true) }),
      defaultOptions: async () => {
        markDefaultOptionsStarted();
        await defaultOptionsReleased;
        return { requestContext };
      },
      model: createTextStreamModel('valid default preflight response'),
    });
    runner.__setPubSub(initialPubSub);

    const wake = runner.sendSignal(
      { type: 'user-message', contents: 'Start valid idle stream' },
      {
        resourceId: 'default-preflight-valid-user',
        threadId: 'default-preflight-valid-thread',
      },
    );
    await defaultOptionsStarted;

    runner.__setPubSub(swappedPubSub);
    const outputPromise = runner.waitForRunOutput(wake.runId);
    releaseDefaultOptions();

    const output = await Promise.race([
      outputPromise,
      new Promise<'timeout'>(resolve => setTimeout(() => resolve('timeout'), 500)),
    ]);
    expect(output).not.toBe('timeout');
    if (output === 'timeout') return;
    expect(output.runId).toBe(wake.runId);
    await expect(output.text).resolves.toBe('valid default preflight response');
  });

  it('reserves thread-only streams while default options are pending', async () => {
    let markDefaultOptionsStarted!: () => void;
    let releaseDefaultOptions!: () => void;
    const defaultOptionsStarted = new Promise<void>(resolve => {
      markDefaultOptionsStarted = resolve;
    });
    const defaultOptionsReleased = new Promise<void>(resolve => {
      releaseDefaultOptions = resolve;
    });

    const runner = new Agent({
      id: 'thread-only-reserved-agent',
      name: 'Thread Only Reserved Agent',
      instructions: 'Test',
      defaultOptions: async () => {
        markDefaultOptionsStarted();
        await defaultOptionsReleased;
        return {};
      },
      model: createTextStreamModel('thread only reserved response'),
    });

    const streamPromise = runner.stream('Hello', { memory: { thread: 'thread-only-reserved-thread' } });
    await defaultOptionsStarted;
    const signalResult = runner.sendSignal(
      { type: 'user-message', contents: 'Hello while thread-only stream is starting' },
      { threadId: 'thread-only-reserved-thread' },
    );
    releaseDefaultOptions();

    const stream = await streamPromise;
    expect(signalResult.runId).toBe(stream.runId);
    await expect(signalResult.accepted).resolves.toMatchObject({ action: 'deliver', runId: stream.runId });
  });

  it('routes thread-only signals after default options add a resource target', async () => {
    let markDefaultOptionsStarted!: () => void;
    let releaseDefaultOptions!: () => void;
    let releaseStream!: () => void;
    const defaultOptionsStarted = new Promise<void>(resolve => {
      markDefaultOptionsStarted = resolve;
    });
    const defaultOptionsReleased = new Promise<void>(resolve => {
      releaseDefaultOptions = resolve;
    });
    const streamReleased = new Promise<void>(resolve => {
      releaseStream = resolve;
    });

    const runner = new Agent({
      id: 'thread-only-retargeted-agent',
      name: 'Thread Only Retargeted Agent',
      instructions: 'Test',
      defaultOptions: async () => {
        markDefaultOptionsStarted();
        await defaultOptionsReleased;
        return { memory: { resource: 'thread-only-retargeted-user' } };
      },
      model: new MockLanguageModelV2({
        doStream: async () => ({
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: new ReadableStream({
            async start(controller) {
              controller.enqueue({ type: 'stream-start', warnings: [] });
              controller.enqueue({
                type: 'response-metadata',
                id: 'thread-only-retargeted-id',
                modelId: 'mock-model-id',
                timestamp: new Date(0),
              });
              controller.enqueue({ type: 'text-start', id: 'text-1' });
              controller.enqueue({ type: 'text-delta', id: 'text-1', delta: 'thread only retargeted response' });
              controller.enqueue({ type: 'text-end', id: 'text-1' });
              await streamReleased;
              controller.enqueue({
                type: 'finish',
                finishReason: 'stop',
                usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
              });
              controller.close();
            },
          }),
        }),
      }),
    });

    const streamPromise = runner.stream('Hello', { memory: { thread: 'thread-only-retargeted-thread' } });
    await defaultOptionsStarted;
    const earlySignal = runner.sendSignal(
      { type: 'user-message', contents: 'Hello before retarget' },
      { threadId: 'thread-only-retargeted-thread' },
    );
    releaseDefaultOptions();

    const stream = await streamPromise;
    const lateSignal = runner.sendSignal(
      { type: 'user-message', contents: 'Hello after retarget' },
      { threadId: 'thread-only-retargeted-thread' },
    );

    expect(earlySignal.runId).toBe(stream.runId);
    expect(lateSignal.runId).toBe(stream.runId);
    await expect(earlySignal.accepted).resolves.toMatchObject({ action: 'deliver', runId: stream.runId });
    await expect(lateSignal.accepted).resolves.toMatchObject({ action: 'deliver', runId: stream.runId });
    releaseStream();
    await expect(stream.text).resolves.toBe('thread only retargeted responsethread only retargeted response');
  });

  it('supports cross-instance thread subscriptions through the Mastra runtime', async () => {
    const pubsub = new EventEmitterPubSub();
    const runner = new Agent({
      id: 'shared-agent',
      name: 'Shared Runner Agent',
      instructions: 'Test',
      model: createTextStreamModel('shared response'),
    });
    const observer = new Agent({
      id: 'shared-agent',
      name: 'Shared Observer Agent',
      instructions: 'Test',
      model: createTextStreamModel('observer response'),
    });
    const mastra = new Mastra({ agents: { runner, observer }, logger: false, pubsub });
    // Mastra wraps the raw pubsub in a Proxy (for localOnly tagging), so
    // reference equality against the raw instance won't hold. Verify both
    // agents share the same (proxy-wrapped) `mastra.pubsub` instead.
    expect(runner.getPubSub()).toBe(mastra.pubsub);
    expect(observer.getPubSub()).toBe(mastra.pubsub);

    const subscription = await observer.subscribeToThread({
      threadId: 'shared-thread',
      resourceId: 'shared-user',
    });
    const iterator = subscription.stream[Symbol.asyncIterator]();
    const firstRunPromise = readNextRun(iterator);

    const stream = await runner.stream('Hello', {
      memory: { thread: 'shared-thread', resource: 'shared-user' },
    });

    const subscribedRun = await firstRunPromise;
    expect(subscribedRun.value.runId).toBe(stream.runId);
    expect(subscribedRun.value.text).toBe('shared response');

    const secondRunPromise = readNextRun(iterator);
    const signalResult = await runner.sendSignal(
      { type: 'user-message', contents: 'Hello from shared signal' },
      {
        resourceId: 'shared-user',
        threadId: 'shared-thread',
        ifIdle: { streamOptions: { memory: { resource: 'shared-user', thread: 'shared-thread' } } },
      },
    );
    const signalRun = await secondRunPromise;
    await expect(signalResult.accepted).resolves.toMatchObject({ action: 'wake', runId: signalRun.value.runId });
    expect(signalResult.signal.id).toBeDefined();
    expect(signalRun.value.text).toBe('shared response');

    subscription.unsubscribe();
  });

  it('drains multiple user-message signals into an active same-agent thread run without merging them into users', async () => {
    let releaseFirst!: () => void;
    const firstFinished = new Promise<void>(resolve => {
      releaseFirst = resolve;
    });
    let releaseSecond!: () => void;
    const secondFinished = new Promise<void>(resolve => {
      releaseSecond = resolve;
    });
    let streamCount = 0;
    const prompts: any[][] = [];

    const model = new MockLanguageModelV2({
      doStream: async ({ prompt }) => {
        streamCount += 1;
        const callIndex = streamCount;
        prompts.push(prompt);
        const responseText =
          callIndex === 1 ? 'first response' : callIndex === 2 ? 'first signal response' : 'second signal response';

        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: new ReadableStream({
            async start(controller) {
              controller.enqueue({ type: 'stream-start', warnings: [] });
              controller.enqueue({
                type: 'response-metadata',
                id: `id-${callIndex}`,
                modelId: 'mock-model-id',
                timestamp: new Date(0),
              });
              controller.enqueue({ type: 'text-start', id: `text-${callIndex}` });
              controller.enqueue({ type: 'text-delta', id: `text-${callIndex}`, delta: responseText });
              controller.enqueue({ type: 'text-end', id: `text-${callIndex}` });
              if (callIndex === 1) {
                await firstFinished;
              }
              if (callIndex === 2) {
                await secondFinished;
              }
              controller.enqueue({
                type: 'finish',
                finishReason: 'stop',
                usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
              });
              controller.close();
            },
          }),
        };
      },
    });

    const memory = new MockMemory();
    const agent = new Agent({
      id: 'active-signal-agent',
      name: 'Active Signal Agent',
      instructions: 'Test',
      model,
      memory,
    });

    const subscription = await agent.subscribeToThread({
      threadId: 'active-thread',
      resourceId: 'active-user',
    });
    const iterator = subscription.stream[Symbol.asyncIterator]();
    const firstRunPromise = readNextRun(iterator);

    const stream = await agent.stream('Hello', {
      memory: { thread: 'active-thread', resource: 'active-user' },
    });
    await expect(waitForActiveRun(subscription)).resolves.toBe(stream.runId);

    const firstSignalResult = await agent.sendSignal(
      { type: 'user-message', contents: 'First signal while running' },
      { resourceId: 'active-user', threadId: 'active-thread' },
    );
    await expect(firstSignalResult.accepted).resolves.toMatchObject({ action: 'deliver', runId: stream.runId });
    expect(firstSignalResult.signal.id).toBeDefined();

    releaseFirst();
    await waitForCondition(() => streamCount === 2);

    const secondSignalResult = await agent.sendSignal(
      { type: 'user-message', contents: 'Second signal while running' },
      { resourceId: 'active-user', threadId: 'active-thread' },
    );
    await expect(secondSignalResult.accepted).resolves.toMatchObject({ action: 'deliver', runId: stream.runId });
    expect(secondSignalResult.signal.id).toBeDefined();
    expect(secondSignalResult.signal.id).not.toBe(firstSignalResult.signal.id);

    releaseSecond();
    const firstRun = await firstRunPromise;
    expect(firstRun.value.text).toBe('first responsefirst signal responsesecond signal response');
    expect(streamCount).toBe(3);
    expect(JSON.stringify(prompts[1])).toContain('First signal while running');
    expect(JSON.stringify(prompts[1])).not.toContain('Second signal while running');
    expect(JSON.stringify(prompts[2])).toContain('First signal while running');
    expect(JSON.stringify(prompts[2])).toContain('Second signal while running');

    await stream.consumeStream();
    const recalled = await memory.recall({ threadId: 'active-thread', resourceId: 'active-user' });
    expect(recalled.messages.map(message => message.role)).toEqual([
      'user',
      'assistant',
      'signal',
      'assistant',
      'signal',
      'assistant',
    ]);
    expect(recalled.messages.map(message => message.content.parts.map(part => part.type))).toEqual([
      ['text'],
      ['text'],
      ['text'],
      ['text'],
      ['text'],
      ['text'],
    ]);
    expect(
      recalled.messages.map(message =>
        message.content.parts.map(part => (part.type === 'text' ? part.text : '')).join(''),
      ),
    ).toEqual([
      'Hello',
      'first response',
      'First signal while running',
      'first signal response',
      'Second signal while running',
      'second signal response',
    ]);

    const [userMessage, firstAssistant, firstSignal, secondAssistant, secondSignal, thirdAssistant] = recalled.messages;
    expect(firstSignal.id).toBe(firstSignalResult.signal.id);
    expect(secondSignal.id).toBe(secondSignalResult.signal.id);
    expect(firstSignal.id).not.toBe(userMessage.id);
    expect(secondSignal.id).not.toBe(userMessage.id);
    expect(firstSignal.createdAt.getTime()).toBeGreaterThan(firstAssistant.createdAt.getTime());
    expect(firstSignal.createdAt.getTime()).toBeLessThanOrEqual(secondAssistant.createdAt.getTime());
    expect(secondSignal.createdAt.getTime()).toBeGreaterThan(secondAssistant.createdAt.getTime());
    expect(secondSignal.createdAt.getTime()).toBeLessThanOrEqual(thirdAssistant.createdAt.getTime());

    const firstRecalledSignal = mastraDBMessageToSignal(firstSignal);
    const secondRecalledSignal = mastraDBMessageToSignal(secondSignal);
    expect(firstRecalledSignal.createdAt).toEqual(firstSignal.createdAt);
    expect(secondRecalledSignal.createdAt).toEqual(secondSignal.createdAt);
    expect(firstRecalledSignal.acceptedAt).toEqual(firstSignalResult.signal.acceptedAt);
    expect(secondRecalledSignal.acceptedAt).toEqual(secondSignalResult.signal.acceptedAt);

    const firstSignalMetadata = firstSignal.content.metadata?.signal as { createdAt?: string; acceptedAt?: string };
    const secondSignalMetadata = secondSignal.content.metadata?.signal as { createdAt?: string; acceptedAt?: string };
    expect(firstSignalMetadata).toMatchObject({
      createdAt: firstSignal.createdAt.toISOString(),
      acceptedAt: firstSignalResult.signal.acceptedAt?.toISOString(),
    });
    expect(secondSignalMetadata).toMatchObject({
      createdAt: secondSignal.createdAt.toISOString(),
      acceptedAt: secondSignalResult.signal.acceptedAt?.toISOString(),
    });
    expect(firstAssistant.content.metadata?.mastra).toMatchObject({ responseBoundary: true });
    expect(secondAssistant.content.metadata?.mastra).toMatchObject({ responseBoundary: true });

    subscription.unsubscribe();
  });

  it('characterizes PF-802 active signal output as one aggregate run without segment markers', async () => {
    let releaseFirst!: () => void;
    const firstFinished = new Promise<void>(resolve => {
      releaseFirst = resolve;
    });
    let streamCount = 0;

    const model = new MockLanguageModelV2({
      doStream: async () => {
        const streamIndex = ++streamCount;
        const responseText = streamIndex === 1 ? 'first response' : 'signal response';

        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: new ReadableStream({
            async start(controller) {
              controller.enqueue({ type: 'stream-start', warnings: [] });
              controller.enqueue({
                type: 'response-metadata',
                id: `id-${streamIndex}`,
                modelId: 'mock-model-id',
                timestamp: new Date(0),
              });
              controller.enqueue({ type: 'text-start', id: `text-${streamIndex}` });
              controller.enqueue({ type: 'text-delta', id: `text-${streamIndex}`, delta: responseText });
              controller.enqueue({ type: 'text-end', id: `text-${streamIndex}` });
              if (streamIndex === 1) {
                await firstFinished;
              }
              controller.enqueue({
                type: 'finish',
                finishReason: 'stop',
                usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
              });
              controller.close();
            },
          }),
        };
      },
    });

    const agent = new Agent({
      id: 'pf-802-aggregate-signal-agent',
      name: 'PF-802 Aggregate Signal Agent',
      instructions: 'Test',
      model,
    });

    const subscription = await agent.subscribeToThread({
      threadId: 'pf-802-aggregate-thread',
      resourceId: 'pf-802-aggregate-user',
    });
    try {
      const iterator = subscription.stream[Symbol.asyncIterator]();
      const runPromise = readNextRunWithParts(iterator);

      const stream = await agent.stream('Hello', {
        memory: { thread: 'pf-802-aggregate-thread', resource: 'pf-802-aggregate-user' },
      });
      await expect(waitForActiveRun(subscription)).resolves.toBe(stream.runId);

      const signalResult = await agent.sendSignal(
        { type: 'user-message', contents: 'Hello while running' },
        { resourceId: 'pf-802-aggregate-user', threadId: 'pf-802-aggregate-thread' },
      );
      expect(signalResult.runId).toBe(stream.runId);
      await expect(signalResult.accepted).resolves.toMatchObject({ action: 'deliver', runId: stream.runId });

      releaseFirst();
      const run = await runPromise;
      const textDeltas = run.value.parts.filter(part => part.type === 'text-delta');

      expect(run.value.runId).toBe(stream.runId);
      expect(run.value.text).toBe('first responsesignal response');
      expect(textDeltas.map(part => part.payload.text)).toEqual(['first response', 'signal response']);
      expect(new Set(textDeltas.map(part => part.runId))).toEqual(new Set([stream.runId]));
      const textDeltaMessageIds = textDeltas.map(part => part.messageId ?? part.payload?.messageId);
      expect(textDeltaMessageIds).toEqual([undefined, undefined]);
      expect(
        run.value.parts.map(part => ({
          segmentId: part.segmentId ?? part.payload?.segmentId,
          segmentIndex: part.segmentIndex ?? part.payload?.segmentIndex,
        })),
      ).toEqual(run.value.parts.map(() => ({ segmentId: undefined, segmentIndex: undefined })));

      await stream.consumeStream();
    } finally {
      subscription.unsubscribe();
    }
  });

  it('drops a not-yet-visible current-step tool call when draining a follow-up signal', async () => {
    const prompts: any[][] = [];
    let callCount = 0;
    let continueToToolCall!: () => void;
    const waitBeforeToolCall = new Promise<void>(resolve => {
      continueToToolCall = resolve;
    });

    const model = new MockLanguageModelV2({
      doStream: async ({ prompt }) => {
        callCount += 1;
        const callIndex = callCount;
        prompts.push(prompt);

        if (callIndex === 1) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: new ReadableStream({
              async start(controller) {
                controller.enqueue({ type: 'stream-start', warnings: [] });
                controller.enqueue({
                  type: 'response-metadata',
                  id: 'id-1',
                  modelId: 'mock-model-id',
                  timestamp: new Date(0),
                });
                controller.enqueue({ type: 'text-start', id: 'text-1' });
                controller.enqueue({ type: 'text-delta', id: 'text-1', delta: 'I will check' });
                await waitBeforeToolCall;
                controller.enqueue({
                  type: 'tool-call',
                  toolCallId: 'stale-tool-call',
                  toolName: 'staleTool',
                  input: '{}',
                });
                controller.enqueue({ type: 'text-end', id: 'text-1' });
                controller.enqueue({
                  type: 'finish',
                  finishReason: 'stop',
                  usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
                });
                controller.close();
              },
            }),
          };
        }

        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: convertArrayToReadableStream([
            { type: 'stream-start', warnings: [] },
            { type: 'response-metadata', id: 'id-2', modelId: 'mock-model-id', timestamp: new Date(0) },
            { type: 'text-start', id: 'text-2' },
            { type: 'text-delta', id: 'text-2', delta: 'signal response' },
            { type: 'text-end', id: 'text-2' },
            {
              type: 'finish',
              finishReason: 'stop',
              usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
            },
          ]),
        };
      },
    });

    const agent = new Agent({
      id: 'tool-interjection-signal-agent',
      name: 'Tool Interjection Signal Agent',
      instructions: 'Test',
      model,
    });

    const subscription = await agent.subscribeToThread({
      threadId: 'tool-interjection-thread',
      resourceId: 'tool-interjection-user',
    });
    const iterator = subscription.stream[Symbol.asyncIterator]();
    const chunks: any[] = [];
    const runPromise = (async () => {
      while (true) {
        const next = await iterator.next();
        if (next.done) return;
        chunks.push(next.value);
        if (next.value.type === 'finish' || next.value.type === 'error' || next.value.type === 'abort') return;
      }
    })();

    const stream = await agent.stream('Hello', {
      memory: { thread: 'tool-interjection-thread', resource: 'tool-interjection-user' },
    });
    await expect(waitForActiveRun(subscription)).resolves.toBe(stream.runId);

    const signalResult = await agent.sendSignal(
      { type: 'user-message', contents: 'Actually stop and answer this instead' },
      { resourceId: 'tool-interjection-user', threadId: 'tool-interjection-thread' },
    );
    await expect(signalResult.accepted).resolves.toMatchObject({ action: 'deliver', runId: stream.runId });

    continueToToolCall();
    await waitForCondition(() => callCount === 2);
    await runPromise;
    await stream._waitUntilFinished();

    expect(chunks.map(chunk => chunk.type)).not.toContain('tool-call');
    expect(JSON.stringify(prompts[1])).toContain('Actually stop and answer this instead');
    expect(JSON.stringify(prompts[1])).not.toContain('stale-tool-call');

    subscription.unsubscribe();
  });

  it('interrupts an active reasoning stream to drain thread-targeted follow-up signals', async () => {
    const prompts: any[][] = [];
    let callCount = 0;
    let releaseReasoningChunk: (() => void) | undefined;
    let finishFirstCall: (() => void) | undefined;

    const model = new MockLanguageModelV2({
      doStream: async ({ prompt }) => {
        callCount += 1;
        const callIndex = callCount;
        prompts.push(prompt);

        if (callIndex === 1) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: new ReadableStream({
              async start(controller) {
                controller.enqueue({ type: 'stream-start', warnings: [] });
                controller.enqueue({
                  type: 'response-metadata',
                  id: 'id-1',
                  modelId: 'mock-model-id',
                  timestamp: new Date(0),
                });
                controller.enqueue({ type: 'reasoning-start', id: 'reasoning-1' });
                controller.enqueue({ type: 'reasoning-delta', id: 'reasoning-1', delta: 'thinking' });
                await new Promise<void>(resolve => (releaseReasoningChunk = resolve));
                controller.enqueue({ type: 'reasoning-delta', id: 'reasoning-1', delta: ' still thinking' });
                await new Promise<void>(resolve => (finishFirstCall = resolve));
                controller.enqueue({ type: 'reasoning-end', id: 'reasoning-1' });
                controller.enqueue({
                  type: 'finish',
                  finishReason: 'stop',
                  usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
                });
                controller.close();
              },
            }),
          };
        }

        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: convertArrayToReadableStream([
            { type: 'stream-start', warnings: [] },
            { type: 'response-metadata', id: 'id-2', modelId: 'mock-model-id', timestamp: new Date(0) },
            { type: 'text-start', id: 'text-1' },
            { type: 'text-delta', id: 'text-1', delta: 'signal response' },
            { type: 'text-end', id: 'text-1' },
            {
              type: 'finish',
              finishReason: 'stop',
              usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
            },
          ]),
        };
      },
    });

    const agent = new Agent({
      id: 'interleaved-reasoning-signal-agent',
      name: 'Interleaved Reasoning Signal Agent',
      instructions: 'Test',
      model,
    });

    const subscription = await agent.subscribeToThread({
      threadId: 'interleaved-reasoning-thread',
      resourceId: 'interleaved-reasoning-user',
    });
    const iterator = subscription.stream[Symbol.asyncIterator]();
    const runPromise = readNextRun(iterator);

    const stream = await agent.stream('Hello', {
      memory: { thread: 'interleaved-reasoning-thread', resource: 'interleaved-reasoning-user' },
    });
    await expect(waitForActiveRun(subscription)).resolves.toBe(stream.runId);
    await waitForCondition(() => !!releaseReasoningChunk);

    const signalResult = await agent.sendSignal(
      { type: 'user-message', contents: 'Stop reasoning and answer this' },
      { resourceId: 'interleaved-reasoning-user', threadId: 'interleaved-reasoning-thread' },
    );
    await expect(signalResult.accepted).resolves.toMatchObject({ action: 'deliver', runId: stream.runId });

    releaseReasoningChunk?.();
    await waitForCondition(() => !!finishFirstCall);
    finishFirstCall?.();
    await waitForCondition(() => callCount === 2);

    const run = await runPromise;
    expect(run.value.text).toContain('signal response');
    expect(JSON.stringify(prompts[1])).toContain('Stop reasoning and answer this');

    subscription.unsubscribe();
  });

  it.each(['user-message', 'reactive', 'system-reminder'] as const)(
    'drains pre-run %s signals with matching visibility',
    async type => {
      const prompts: any[][] = [];

      const model = new MockLanguageModelV2({
        doStream: async ({ prompt }) => {
          prompts.push(prompt);

          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: convertArrayToReadableStream([
              { type: 'stream-start', warnings: [] },
              { type: 'response-metadata', id: 'id-0', modelId: 'mock-model-id', timestamp: new Date(0) },
              { type: 'text-start', id: 'text-1' },
              { type: 'text-delta', id: 'text-1', delta: 'response' },
              { type: 'text-end', id: 'text-1' },
              {
                type: 'finish',
                finishReason: 'stop',
                usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
              },
            ]),
          };
        },
      });

      const agent = new Agent({
        id: 'idle-start-thread-target-agent',
        name: 'Idle Start Thread Target Agent',
        instructions: 'Test',
        model,
      });

      const subscription = await agent.subscribeToThread({
        threadId: 'idle-start-thread',
        resourceId: 'idle-start-user',
      });
      const iterator = subscription.stream[Symbol.asyncIterator]();
      const runPromise = readNextRunWithParts(iterator);

      const firstSignal = await agent.sendSignal(
        { type: 'user-message', contents: 'start idle stream' },
        {
          resourceId: 'idle-start-user',
          threadId: 'idle-start-thread',
          ifIdle: { streamOptions: { memory: { resource: 'idle-start-user', thread: 'idle-start-thread' } } },
        },
      );

      const followUp = await agent.sendSignal(
        { type, contents: 'thread targeted follow up' },
        {
          resourceId: 'idle-start-user',
          threadId: 'idle-start-thread',
          ifIdle: { streamOptions: { memory: { resource: 'idle-start-user', thread: 'idle-start-thread' } } },
        },
      );

      const firstAccepted = await firstSignal.accepted;
      const followUpAccepted = await followUp.accepted;
      const firstRunId = 'runId' in firstAccepted ? firstAccepted.runId : undefined;
      const followUpRunId = 'runId' in followUpAccepted ? followUpAccepted.runId : undefined;
      expect(firstAccepted.action).toBe('wake');
      expect(followUpRunId).toBe(firstRunId);

      const run = await runPromise;
      expect(run.value.runId).toBe(firstRunId);
      expect(run.value.text).toBe('response');
      expect(run.value.parts.filter((part: any) => part.data?.contents === 'thread targeted follow up')).toHaveLength(
        1,
      );
      expect(prompts).toHaveLength(1);
      expect(JSON.stringify(prompts[0])).toContain('thread targeted follow up');

      subscription.unsubscribe();
    },
  );

  it('completes a signal-started run that no caller subscribes to or consumes', async () => {
    // Regression: a fire-and-forget wake (e.g. an agent schedule) starts a thread run
    // but never subscribes to or consumes the returned stream. The runtime must
    // still drive the stream to completion on its own so the run reaches a
    // terminal state and its active-run record releases. If it does not, the
    // thread stays wedged and every later signal coalesces into the stuck run.
    const agent = new Agent({
      id: 'unconsumed-wake-agent',
      name: 'Unconsumed Wake Agent',
      instructions: 'Test',
      model: createTextStreamModel('unconsumed response'),
    });

    const resourceId = 'unconsumed-wake-user';
    const threadId = 'unconsumed-wake-thread';

    // Wake the thread without subscribing or consuming the resulting stream.
    const accepted = await agent.sendSignal(
      { type: 'user-message', contents: 'wake without a consumer' },
      {
        resourceId,
        threadId,
        ifIdle: { streamOptions: { memory: { resource: resourceId, thread: threadId } } },
      },
    ).accepted;
    expect(accepted.action).toBe('wake');
    const runId = 'runId' in accepted ? accepted.runId : undefined;
    expect(runId).toBeTruthy();
    expect(agent.getActiveThreadRunId({ resourceId, threadId })).toBe(runId);

    // With no consumer, the run must still finish and release the active-run record.
    await waitForCondition(() => agent.getActiveThreadRunId({ resourceId, threadId }) === undefined, 2000);
    expect(agent.getActiveThreadRunId({ resourceId, threadId })).toBeUndefined();

    // A follow-up wake now starts a fresh run rather than coalescing into a stuck one.
    const followUp = await agent.sendSignal(
      { type: 'user-message', contents: 'second wake after first completed' },
      {
        resourceId,
        threadId,
        ifIdle: { streamOptions: { memory: { resource: resourceId, thread: threadId } } },
      },
    ).accepted;
    expect(followUp.action).toBe('wake');
    const followUpRunId = 'runId' in followUp ? followUp.runId : undefined;
    expect(followUpRunId).not.toBe(runId);
  });

  it('preserves active interjections sent immediately after repeated idle signal-started runs', async () => {
    const releaseInitialCalls: Array<() => void> = [];
    const prompts: any[][] = [];
    let callCount = 0;

    const model = new MockLanguageModelV2({
      doStream: async ({ prompt }) => {
        callCount += 1;
        const callIndex = callCount;
        prompts.push(prompt);

        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: new ReadableStream({
            async start(controller) {
              controller.enqueue({ type: 'stream-start', warnings: [] });
              controller.enqueue({
                type: 'response-metadata',
                id: `id-${callIndex}`,
                modelId: 'mock-model-id',
                timestamp: new Date(0),
              });
              controller.enqueue({ type: 'text-start', id: 'text-1' });
              controller.enqueue({ type: 'text-delta', id: 'text-1', delta: `response ${callIndex}` });
              controller.enqueue({ type: 'text-end', id: 'text-1' });
              if (callIndex === 1 || callIndex === 2) {
                await new Promise<void>(resolve => releaseInitialCalls.push(resolve));
              }
              controller.enqueue({
                type: 'finish',
                finishReason: 'stop',
                usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
              });
              controller.close();
            },
          }),
        };
      },
    });

    const agent = new Agent({
      id: 'repeated-idle-signal-agent',
      name: 'Repeated Idle Signal Agent',
      instructions: 'Test',
      model,
    });

    const subscription = await agent.subscribeToThread({
      threadId: 'repeated-idle-thread',
      resourceId: 'repeated-idle-user',
    });
    const iterator = subscription.stream[Symbol.asyncIterator]();

    const firstRunPromise = readNextRun(iterator);
    const firstIdle = await agent.sendSignal(
      { type: 'user-message', contents: 'start first idle stream' },
      {
        resourceId: 'repeated-idle-user',
        threadId: 'repeated-idle-thread',
        ifIdle: { streamOptions: { memory: { resource: 'repeated-idle-user', thread: 'repeated-idle-thread' } } },
      },
    );
    await agent.sendSignal(
      { type: 'user-message', contents: 'first active interjection' },
      { runId: firstIdle.runId, resourceId: 'repeated-idle-user', threadId: 'repeated-idle-thread' },
    );
    while (releaseInitialCalls.length < 1) await nextTick();
    releaseInitialCalls.shift()?.();
    const firstRun = await firstRunPromise;
    expect(firstRun.value.text).toBe('response 1');
    expect(JSON.stringify(prompts[0])).toContain('first active interjection');

    const secondRunPromise = readNextRun(iterator);
    const secondIdle = await agent.sendSignal(
      { type: 'user-message', contents: 'start second idle stream' },
      {
        resourceId: 'repeated-idle-user',
        threadId: 'repeated-idle-thread',
        ifIdle: { streamOptions: { memory: { resource: 'repeated-idle-user', thread: 'repeated-idle-thread' } } },
      },
    );
    await agent.sendSignal(
      { type: 'user-message', contents: 'second active interjection' },
      { runId: secondIdle.runId, resourceId: 'repeated-idle-user', threadId: 'repeated-idle-thread' },
    );
    while (releaseInitialCalls.length < 1) await nextTick();
    releaseInitialCalls.shift()?.();
    const secondRun = await secondRunPromise;
    expect(secondRun.value.text).toBe('response 2');
    expect(JSON.stringify(prompts[1])).toContain('second active interjection');

    subscription.unsubscribe();
  });

  it('queues a signal from another agent until the active thread run finishes', async () => {
    let releaseFirst!: () => void;
    const firstFinished = new Promise<void>(resolve => {
      releaseFirst = resolve;
    });
    let firstStarted = false;
    let secondStarted = false;

    const firstAgent = new Agent({
      id: 'cross-agent-a',
      name: 'Cross Agent A',
      instructions: 'Test',
      model: new MockLanguageModelV2({
        doStream: async () => {
          firstStarted = true;
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: new ReadableStream({
              async start(controller) {
                controller.enqueue({ type: 'stream-start', warnings: [] });
                controller.enqueue({
                  type: 'response-metadata',
                  id: 'cross-a',
                  modelId: 'mock-model-id',
                  timestamp: new Date(0),
                });
                controller.enqueue({ type: 'text-start', id: 'text-1' });
                controller.enqueue({ type: 'text-delta', id: 'text-1', delta: 'first response' });
                controller.enqueue({ type: 'text-end', id: 'text-1' });
                await firstFinished;
                controller.enqueue({
                  type: 'finish',
                  finishReason: 'stop',
                  usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
                });
                controller.close();
              },
            }),
          };
        },
      }),
    });
    const secondAgent = new Agent({
      id: 'cross-agent-b',
      name: 'Cross Agent B',
      instructions: 'Test',
      model: new MockLanguageModelV2({
        doStream: async () => {
          secondStarted = true;
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: convertArrayToReadableStream([
              { type: 'stream-start', warnings: [] },
              { type: 'response-metadata', id: 'cross-b', modelId: 'mock-model-id', timestamp: new Date(0) },
              { type: 'text-start', id: 'text-1' },
              { type: 'text-delta', id: 'text-1', delta: 'second response' },
              { type: 'text-end', id: 'text-1' },
              {
                type: 'finish',
                finishReason: 'stop',
                usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
              },
            ]),
          };
        },
      }),
    });
    new Mastra({ agents: { firstAgent, secondAgent }, logger: false });

    const subscription = await firstAgent.subscribeToThread({
      threadId: 'cross-agent-thread',
      resourceId: 'cross-agent-user',
    });
    const iterator = subscription.stream[Symbol.asyncIterator]();
    const firstRunPromise = readNextRun(iterator);

    const firstStream = await firstAgent.stream('Hello', {
      memory: { thread: 'cross-agent-thread', resource: 'cross-agent-user' },
    });
    const firstText = firstStream.text;
    await waitForCondition(() => firstStarted);
    expect(firstStarted).toBe(true);

    const signalResult = await secondAgent.sendSignal(
      { type: 'user-message', contents: 'Hello from another agent' },
      {
        resourceId: 'cross-agent-user',
        threadId: 'cross-agent-thread',
        ifIdle: { streamOptions: { memory: { resource: 'cross-agent-user', thread: 'cross-agent-thread' } } },
      },
    );
    await nextTick();
    expect(secondStarted).toBe(false);

    releaseFirst();
    await expect(firstText).resolves.toBe('first response');
    await expect(firstRunPromise).resolves.toMatchObject({ value: { runId: firstStream.runId }, done: false });

    const signalAccepted = await signalResult.accepted;
    const signalRunId = 'runId' in signalAccepted ? signalAccepted.runId : undefined;
    const secondRun = await readNextRun(iterator);
    expect(secondRun.value.runId).toBe(signalRunId);
    expect(secondRun.value.text).toBe('second response');
    expect(secondStarted).toBe(true);

    subscription.unsubscribe();
  });

  it('preserves caller-provided runId for idle wake signals', async () => {
    const agent = new Agent({
      id: 'caller-run-id-agent',
      name: 'Caller Run Id Agent',
      instructions: 'Test',
      model: createTextStreamModel('caller run response'),
    });
    const subscription = await agent.subscribeToThread({
      resourceId: 'caller-run-user',
      threadId: 'caller-run-thread',
    });

    const signalResult = await agent.sendSignal(
      { type: 'user-message', contents: 'wake with caller id' },
      {
        runId: 'caller-provided-run',
        resourceId: 'caller-run-user',
        threadId: 'caller-run-thread',
        ifIdle: { streamOptions: { memory: { resource: 'caller-run-user', thread: 'caller-run-thread' } } },
      },
    );

    expect(signalResult.runId).toBe('caller-provided-run');
    await expect(signalResult.accepted).resolves.toMatchObject({ action: 'wake', runId: 'caller-provided-run' });
    await expect(readNextRun(subscription.stream[Symbol.asyncIterator]())).resolves.toMatchObject({
      value: { runId: 'caller-provided-run', text: 'caller run response' },
      done: false,
    });

    subscription.unsubscribe();
  });

  it('runs idle wake rejection cleanup when a queued idle stream fails', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new ControlledLeasePubSub();
    let runFailedLeaseOwner: string | undefined;
    let runFailedPublishedWithLiveOwner = false;
    const publish = pubsub.publish.bind(pubsub);
    pubsub.publish = async (topic, event) => {
      const data = (event as { data?: { type?: string; leaseOwner?: string } }).data;
      if (data?.type === 'run-failed') {
        runFailedLeaseOwner = data.leaseOwner;
        runFailedPublishedWithLiveOwner =
          data.leaseOwner !== undefined && [...pubsub.owners.values()].includes(data.leaseOwner);
      }
      await publish(topic, event);
    };
    let finishActive!: () => void;
    const activeFinished = new Promise<void>(resolve => {
      finishActive = resolve;
    });

    const completion = runtime.registerRun(
      { id: 'active-agent' } as any,
      {
        runId: 'active-run',
        status: 'running',
        _waitUntilFinished: () => activeFinished,
      } as any,
      {
        runId: 'active-run',
        memory: { resource: 'queued-failure-user', thread: 'queued-failure-thread' },
      } as any,
      pubsub,
    );
    const cleanup = vi.fn();
    const stream = vi.fn(async () => {
      throw new Error('queued idle stream failed');
    });

    const result = runtime.sendSignal(
      { id: 'queued-idle-agent', stream } as any,
      { type: 'user-message', contents: 'queued wake' },
      {
        resourceId: 'queued-failure-user',
        threadId: 'queued-failure-thread',
        ifIdle: {
          streamOptions: { memory: { resource: 'queued-failure-user', thread: 'queued-failure-thread' } },
          _onThreadStreamRunRejected: cleanup,
        } as any,
      },
      pubsub,
    );

    expect(result.output).toBeUndefined();
    expect(cleanup).not.toHaveBeenCalled();

    finishActive();
    await completion;

    expect(cleanup).toHaveBeenCalledTimes(1);
    expect(stream).toHaveBeenCalledWith(
      expect.objectContaining({ contents: 'queued wake' }),
      expect.objectContaining({
        runId: result.runId,
        memory: { resource: 'queued-failure-user', thread: 'queued-failure-thread' },
      }),
    );
    expect(runFailedLeaseOwner).toBeDefined();
    expect(runFailedPublishedWithLiveOwner).toBe(true);
    expect(pubsub.publishedData.some(data => data?.type === 'run-aborted' && data.runId === result.runId)).toBe(false);
  });

  it('wakes reservation waiters when a queued idle stream fails', async () => {
    const runtime = new AgentThreadStreamRuntime();
    let finishActive!: () => void;
    const activeFinished = new Promise<void>(resolve => {
      finishActive = resolve;
    });
    let rejectStream!: (error: Error) => void;

    const completion = runtime.registerRun(
      { id: 'active-agent' } as any,
      {
        runId: 'active-run',
        status: 'running',
        _waitUntilFinished: () => activeFinished,
      } as any,
      {
        runId: 'active-run',
        memory: { resource: 'queued-waiter-user', thread: 'queued-waiter-thread' },
      } as any,
    );
    const stream = vi.fn(
      () =>
        new Promise((_resolve, reject) => {
          rejectStream = reject;
        }),
    );

    runtime.sendSignal(
      { id: 'queued-idle-agent', stream } as any,
      { type: 'user-message', contents: 'queued wake' },
      {
        resourceId: 'queued-waiter-user',
        threadId: 'queued-waiter-thread',
        ifIdle: {
          streamOptions: { memory: { resource: 'queued-waiter-user', thread: 'queued-waiter-thread' } },
        } as any,
      },
    );

    finishActive();
    await nextTick();
    expect(stream).toHaveBeenCalledTimes(1);

    let waiterResolved = false;
    const waiter = runtime
      .waitForCrossAgentThreadRun(
        { id: 'other-agent' } as any,
        {
          runId: 'other-run',
          memory: { resource: 'queued-waiter-user', thread: 'queued-waiter-thread' },
        } as any,
      )
      .then(() => {
        waiterResolved = true;
      });
    await nextTick();
    expect(waiterResolved).toBe(false);

    rejectStream(new Error('queued idle stream failed'));
    await completion;
    await waiter;
    expect(waiterResolved).toBe(true);
  });

  it('does not reserve queued idle streams before preflight when reservation is deferred', async () => {
    const runtime = new AgentThreadStreamRuntime();
    let finishActive!: () => void;
    const activeFinished = new Promise<void>(resolve => {
      finishActive = resolve;
    });
    let rejectFirstStream!: (error: Error) => void;

    const completion = runtime.registerRun(
      { id: 'active-agent' } as any,
      {
        runId: 'active-run',
        status: 'running',
        _waitUntilFinished: () => activeFinished,
      } as any,
      {
        runId: 'active-run',
        memory: { resource: 'queued-deferred-user', thread: 'queued-deferred-thread' },
      } as any,
    );
    const stream = vi
      .fn()
      .mockImplementationOnce(
        () =>
          new Promise((_resolve, reject) => {
            rejectFirstStream = reject;
          }),
      )
      .mockResolvedValueOnce({ runId: 'queued-deferred-second-run' })
      .mockResolvedValueOnce({ runId: 'queued-deferred-retry-run' });

    const firstResult = runtime.sendSignal(
      { id: 'queued-deferred-agent', stream } as any,
      { id: 'queued-deferred-signal', type: 'user-message', contents: 'queued deferred wake' },
      {
        resourceId: 'queued-deferred-user',
        threadId: 'queued-deferred-thread',
        ifIdle: {
          _skipThreadRunReservationBeforePreflight: true,
          streamOptions: { memory: { resource: 'queued-deferred-user', thread: 'queued-deferred-thread' } },
        } as any,
      },
    );
    runtime.sendSignal(
      { id: 'queued-deferred-agent', stream } as any,
      { type: 'user-message', contents: 'queued second deferred wake' },
      {
        resourceId: 'queued-deferred-user',
        threadId: 'queued-deferred-thread',
        ifIdle: {
          _skipThreadRunReservationBeforePreflight: true,
          streamOptions: { memory: { resource: 'queued-deferred-user', thread: 'queued-deferred-thread' } },
        } as any,
      },
    );

    finishActive();
    await nextTick();
    expect(stream).toHaveBeenCalledWith(
      expect.objectContaining({ contents: 'queued deferred wake' }),
      expect.not.objectContaining({ _threadRunReservationOwner: true }),
    );

    let waiterResolved = false;
    await runtime
      .waitForCrossAgentThreadRun(
        { id: 'other-agent' } as any,
        {
          runId: 'other-run',
          memory: { resource: 'queued-deferred-user', thread: 'queued-deferred-thread' },
        } as any,
      )
      .then(() => {
        waiterResolved = true;
      });
    expect(waiterResolved).toBe(true);

    rejectFirstStream(new Error('queued deferred idle stream failed'));
    await completion;
    expect(stream).toHaveBeenCalledWith(
      expect.objectContaining({ contents: 'queued second deferred wake' }),
      expect.not.objectContaining({ _threadRunReservationOwner: true }),
    );
    expect(stream).toHaveBeenCalledTimes(2);

    const retryResult = runtime.sendSignal(
      { id: 'queued-deferred-agent', stream } as any,
      { id: 'queued-deferred-signal', type: 'user-message', contents: 'queued deferred wake' },
      {
        resourceId: 'queued-deferred-user',
        threadId: 'queued-deferred-thread',
        ifIdle: {
          _skipThreadRunReservationBeforePreflight: true,
          streamOptions: { memory: { resource: 'queued-deferred-user', thread: 'queued-deferred-thread' } },
        } as any,
      },
    );
    expect(retryResult.runId).not.toBe(firstResult.runId);
    await expect(retryResult.accepted).resolves.toMatchObject({ action: 'wake', runId: retryResult.runId });
    expect(stream).toHaveBeenCalledTimes(3);
  });

  it('aborts queued deferred idle streams after they start preflight without reservation', async () => {
    const runtime = new AgentThreadStreamRuntime();
    let finishActive!: () => void;
    const activeFinished = new Promise<void>(resolve => {
      finishActive = resolve;
    });
    let rejectStream!: (error: Error) => void;

    const completion = runtime.registerRun(
      { id: 'active-agent' } as any,
      {
        runId: 'active-run',
        status: 'running',
        _waitUntilFinished: () => activeFinished,
      } as any,
      {
        runId: 'active-run',
        memory: { resource: 'queued-deferred-abort-user', thread: 'queued-deferred-abort-thread' },
      } as any,
    );
    const stream = vi.fn(
      () =>
        new Promise((_resolve, reject) => {
          rejectStream = reject;
        }),
    );

    const result = runtime.sendSignal(
      { id: 'queued-deferred-abort-agent', stream } as any,
      { type: 'user-message', contents: 'queued deferred abort wake' },
      {
        resourceId: 'queued-deferred-abort-user',
        threadId: 'queued-deferred-abort-thread',
        ifIdle: {
          _skipThreadRunReservationBeforePreflight: true,
          streamOptions: { memory: { resource: 'queued-deferred-abort-user', thread: 'queued-deferred-abort-thread' } },
        } as any,
      },
    );

    finishActive();
    await nextTick();
    expect(stream).toHaveBeenCalledTimes(1);

    const waiter = runtime.waitForRunOutput(result.runId);
    expect(runtime.abortRun(result.runId)).toBe(true);
    await expect(waiter).rejects.toThrow('has been aborted');

    rejectStream(new Error('queued deferred abort stream stopped'));
    await completion;
  });

  it('aborts immediate deferred idle streams while preflight is pending', async () => {
    const runtime = new AgentThreadStreamRuntime();
    let rejectStream!: (error: Error) => void;
    const stream = vi.fn(
      () =>
        new Promise((_resolve, reject) => {
          rejectStream = reject;
        }),
    );

    const result = runtime.sendSignal(
      { id: 'immediate-deferred-abort-agent', stream } as any,
      { type: 'user-message', contents: 'immediate deferred abort wake' },
      {
        resourceId: 'immediate-deferred-abort-user',
        threadId: 'immediate-deferred-abort-thread',
        ifIdle: {
          _skipThreadRunReservationBeforePreflight: true,
          streamOptions: {
            memory: { resource: 'immediate-deferred-abort-user', thread: 'immediate-deferred-abort-thread' },
          },
        } as any,
      },
    );
    void result.output?.catch(() => {});
    await waitForCondition(() => stream.mock.calls.length === 1);
    expect(stream).toHaveBeenCalledTimes(1);

    const waiter = runtime.waitForRunOutput(result.runId);
    expect(runtime.abortRun(result.runId)).toBe(true);
    await expect(waiter).rejects.toThrow('has been aborted');

    rejectStream(new Error('immediate deferred abort stream stopped'));
    await expect(result.output).rejects.toThrow('immediate deferred abort stream stopped');
  });

  it('blocks direct reservations while a deferred idle run id is inflight', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    let rejectStream!: (error: Error) => void;
    const stream = vi.fn(
      () =>
        new Promise((_resolve, reject) => {
          rejectStream = reject;
        }),
    );

    const result = runtime.sendSignal(
      { id: 'inflight-deferred-owner-agent', stream } as any,
      { type: 'user-message', contents: 'inflight deferred wake' },
      {
        runId: 'inflight-deferred-run',
        resourceId: 'inflight-deferred-user',
        threadId: 'inflight-deferred-thread',
        ifIdle: {
          _skipThreadRunReservationBeforePreflight: true,
          streamOptions: { memory: { resource: 'inflight-deferred-user', thread: 'inflight-deferred-thread' } },
        } as any,
      },
      pubsub,
    );
    await waitForCondition(() => stream.mock.calls.length === 1);
    expect(stream).toHaveBeenCalledTimes(1);

    expect(() =>
      runtime.reserveRun(
        {
          runId: 'inflight-deferred-run',
          memory: { resource: 'inflight-deferred-user', thread: 'inflight-deferred-thread' },
        } as any,
        pubsub,
      ),
    ).toThrow('already reserved');

    const duplicateOutput = buildFakeOutput({
      runId: 'inflight-deferred-run',
      fullOutput: { text: 'duplicate response', finishReason: 'stop', usage: {} },
      chunks: [{ runId: 'inflight-deferred-run', type: 'finish', payload: {} }],
    });
    expect(() =>
      runtime.registerRun(
        { id: 'duplicate-inflight-agent' } as any,
        duplicateOutput,
        {
          runId: 'inflight-deferred-run',
          memory: { resource: 'inflight-deferred-user', thread: 'inflight-deferred-thread' },
        } as any,
        pubsub,
      ),
    ).toThrow('already reserved');

    rejectStream(new Error('inflight deferred stream stopped'));
    await expect(result.output).rejects.toThrow('inflight deferred stream stopped');
  });

  it('keeps deferred idle run ids inflight when owner reservation waits for an active thread', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    let rejectStream!: (error: Error) => void;
    const stream = vi.fn((_signal, options) => {
      runtime.registerRun(
        { id: 'inflight-owner-blocking-agent' } as any,
        {
          runId: 'inflight-owner-blocking-active-run',
          status: 'running',
          fullStream: (async function* () {})(),
          _waitUntilFinished: () => new Promise<void>(() => {}),
        } as any,
        {
          runId: 'inflight-owner-blocking-active-run',
          memory: { resource: 'inflight-owner-blocked-user', thread: 'inflight-owner-blocked-thread' },
        } as any,
        pubsub,
      );
      expect(runtime.reserveRun(options as any, pubsub, 'inflight-owner-blocked-agent')).toBeUndefined();
      return new Promise((_resolve, reject) => {
        rejectStream = reject;
      });
    });

    const result = runtime.sendSignal(
      { id: 'inflight-owner-blocked-agent', stream } as any,
      { type: 'user-message', contents: 'inflight owner blocked wake' },
      {
        runId: 'inflight-owner-blocked-run',
        resourceId: 'inflight-owner-blocked-user',
        threadId: 'inflight-owner-blocked-thread',
        ifIdle: {
          _skipThreadRunReservationBeforePreflight: true,
          streamOptions: {
            memory: { resource: 'inflight-owner-blocked-user', thread: 'inflight-owner-blocked-thread' },
          },
        } as any,
      },
      pubsub,
    );
    await waitForCondition(() => stream.mock.calls.length === 1);
    expect(stream).toHaveBeenCalledTimes(1);

    expect(() =>
      runtime.reserveRun(
        {
          runId: 'inflight-owner-blocked-run',
          memory: { resource: 'inflight-owner-blocked-user', thread: 'inflight-owner-blocked-thread' },
        } as any,
        pubsub,
      ),
    ).toThrow('already reserved');
    expect(runtime.abortRun(result.runId, pubsub)).toBe(true);

    rejectStream(new Error('inflight owner blocked stream stopped'));
    await expect(result.output).rejects.toThrow('inflight owner blocked stream stopped');
  });

  it('wakes reservation waiters when an immediate idle stream fails', async () => {
    const runtime = new AgentThreadStreamRuntime();
    let rejectStream!: (error: Error) => void;
    const stream = vi
      .fn()
      .mockImplementationOnce(
        () =>
          new Promise((_resolve, reject) => {
            rejectStream = reject;
          }),
      )
      .mockResolvedValueOnce({ runId: 'immediate-retry-run' });

    const result = runtime.sendSignal(
      { id: 'immediate-idle-agent', stream } as any,
      { id: 'immediate-wake-signal', type: 'user-message', contents: 'immediate wake' },
      {
        resourceId: 'immediate-waiter-user',
        threadId: 'immediate-waiter-thread',
        ifIdle: {
          streamOptions: { memory: { resource: 'immediate-waiter-user', thread: 'immediate-waiter-thread' } },
        } as any,
      },
    );
    await waitForCondition(() => stream.mock.calls.length === 1);
    expect(stream).toHaveBeenCalledTimes(1);

    let waiterResolved = false;
    const waiter = runtime
      .waitForCrossAgentThreadRun(
        { id: 'other-agent' } as any,
        {
          runId: 'other-run',
          memory: { resource: 'immediate-waiter-user', thread: 'immediate-waiter-thread' },
        } as any,
      )
      .then(() => {
        waiterResolved = true;
      });
    await nextTick();
    expect(waiterResolved).toBe(false);

    rejectStream(new Error('immediate idle stream failed'));
    await expect(result.output).rejects.toThrow('immediate idle stream failed');
    await expect(runtime.waitForRunOutput(result.runId)).rejects.toThrow('was rejected');
    await waiter;
    expect(waiterResolved).toBe(true);

    const retryResult = runtime.sendSignal(
      { id: 'immediate-idle-agent', stream } as any,
      { id: 'immediate-wake-signal', type: 'user-message', contents: 'immediate wake' },
      {
        resourceId: 'immediate-waiter-user',
        threadId: 'immediate-waiter-thread',
        ifIdle: {
          streamOptions: { memory: { resource: 'immediate-waiter-user', thread: 'immediate-waiter-thread' } },
        } as any,
      },
    );
    expect(retryResult.runId).not.toBe(result.runId);
    await expect(retryResult.accepted).resolves.toMatchObject({ action: 'wake', runId: retryResult.runId });
    expect(stream).toHaveBeenCalledTimes(2);
  });

  it('publishes an authenticated setup failure before releasing its exact lease owner', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new ControlledLeasePubSub();
    let runFailedLeaseOwner: string | undefined;
    let runFailedPublishedWithLiveOwner = false;
    const publish = pubsub.publish.bind(pubsub);
    pubsub.publish = async (topic, event) => {
      const data = (event as { data?: { type?: string; leaseOwner?: string } }).data;
      if (data?.type === 'run-failed') {
        runFailedLeaseOwner = data.leaseOwner;
        runFailedPublishedWithLiveOwner =
          data.leaseOwner !== undefined && [...pubsub.owners.values()].includes(data.leaseOwner);
      }
      await publish(topic, event);
    };
    const resourceId = 'authenticated-setup-failure-user';
    const threadId = 'authenticated-setup-failure-thread';
    const result = runtime.sendSignal(
      {
        id: 'authenticated-setup-failure-agent',
        stream: vi.fn(async () => {
          throw new Error('authenticated setup failure');
        }),
      } as any,
      { id: 'authenticated-setup-failure-signal', type: 'user-message', contents: 'wake and fail' },
      {
        resourceId,
        threadId,
        ifIdle: { streamOptions: { memory: { resource: resourceId, thread: threadId } } } as any,
      },
      pubsub,
    );

    await expect(result.accepted).rejects.toThrow('authenticated setup failure');
    expect(runFailedLeaseOwner).toBeDefined();
    expect(runFailedPublishedWithLiveOwner).toBe(true);
    expect(
      pubsub.publishedData.filter(data => data?.type === 'run-failed' && data.runId === result.runId),
    ).toHaveLength(1);
    expect(pubsub.publishedData.some(data => data?.type === 'run-aborted' && data.runId === result.runId)).toBe(false);
    await waitForCondition(() => pubsub.owners.size === 0);
  });

  it('wakes waiters and drops queued signals when a reserved setup run is released', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    const streamOptions = {
      runId: 'reserved-setup-run',
      memory: { resource: 'reserved-setup-user', thread: 'reserved-setup-thread' },
    } as any;
    const release = runtime.reserveRun(streamOptions, pubsub);
    expect(release).toBeDefined();

    let waiterResolved = false;
    const waiter = runtime
      .waitForCrossAgentThreadRun(
        { id: 'other-agent' } as any,
        {
          runId: 'reserved-setup-run',
          memory: { resource: 'reserved-setup-user', thread: 'reserved-setup-thread' },
        } as any,
        pubsub,
      )
      .then(() => {
        waiterResolved = true;
      });
    await nextTick();
    expect(waiterResolved).toBe(false);

    const signalResult = runtime.sendSignal(
      { id: 'reserved-agent' } as any,
      { type: 'user-message', contents: 'stale setup signal' },
      {
        resourceId: 'reserved-setup-user',
        threadId: 'reserved-setup-thread',
      },
      pubsub,
    );
    expect(signalResult).toEqual(expect.objectContaining({ runId: 'reserved-setup-run' }));

    release!();
    await waiter;
    expect(waiterResolved).toBe(true);

    const laterRelease = runtime.reserveRun(
      {
        runId: 'later-run',
        memory: { resource: 'reserved-setup-user', thread: 'reserved-setup-thread' },
      } as any,
      pubsub,
    );
    expect(laterRelease).toBeDefined();
    expect(runtime.drainPendingSignals('later-run', pubsub)).toEqual([]);
    const laterSignal = runtime.sendSignal(
      { id: 'reserved-agent' } as any,
      { type: 'user-message', contents: 'successor signal' },
      {
        resourceId: 'reserved-setup-user',
        threadId: 'reserved-setup-thread',
      },
      pubsub,
    );
    expect(laterSignal).toEqual(expect.objectContaining({ runId: 'later-run' }));
    expect(runtime.drainPendingSignals('later-run', pubsub, 'pre-run')).toHaveLength(1);
    laterRelease!();
  });

  it('does not overwrite an existing run reservation with a reused run id', () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    const release = runtime.reserveRun(
      {
        runId: 'reused-reserved-run',
        memory: { resource: 'first-reservation-user', thread: 'first-reservation-thread' },
      } as any,
      pubsub,
    );

    expect(release).toBeDefined();
    expect(() =>
      runtime.reserveRun(
        {
          runId: 'reused-reserved-run',
          memory: { resource: 'first-reservation-user', thread: 'first-reservation-thread' },
        } as any,
        pubsub,
      ),
    ).toThrow('already reserved');
    expect(() =>
      runtime.reserveRun(
        {
          runId: 'reused-reserved-run',
          memory: { resource: 'second-reservation-user', thread: 'second-reservation-thread' },
        } as any,
        pubsub,
      ),
    ).toThrow('already reserved for another thread');
    expect(
      runtime.abortThread({ resourceId: 'second-reservation-user', threadId: 'second-reservation-thread' }, pubsub),
    ).toBe(false);
    expect(
      runtime.abortThread({ resourceId: 'first-reservation-user', threadId: 'first-reservation-thread' }, pubsub),
    ).toBe(true);
  });

  it('rejects duplicate queued idle run ids before either idle wake starts', () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    const stream = vi.fn(async () => ({
      runId: 'queued-duplicate-idle-run',
      status: 'running',
      fullStream: (async function* () {})(),
      _waitUntilFinished: async () => {},
    }));

    runtime.registerRun(
      { id: 'active-agent' } as any,
      {
        runId: 'active-before-queued-duplicate',
        status: 'running',
        _waitUntilFinished: () => new Promise<any>(() => {}),
      } as any,
      {
        runId: 'active-before-queued-duplicate',
        memory: { resource: 'queued-duplicate-user', thread: 'queued-duplicate-thread' },
      } as any,
      pubsub,
    );

    const target = {
      runId: 'queued-duplicate-idle-run',
      resourceId: 'queued-duplicate-user',
      threadId: 'queued-duplicate-thread',
      ifIdle: {
        streamOptions: { memory: { resource: 'queued-duplicate-user', thread: 'queued-duplicate-thread' } },
      },
    } as any;

    const firstQueued = runtime.sendSignal(
      { id: 'queued-duplicate-agent', stream } as any,
      { type: 'user-message', contents: 'first queued idle' },
      target,
      pubsub,
    );
    expect(firstQueued.runId).toBe('queued-duplicate-idle-run');
    expect(firstQueued.accepted).toBeInstanceOf(Promise);
    expect(() =>
      runtime.sendSignal(
        { id: 'queued-duplicate-agent', stream } as any,
        { type: 'user-message', contents: 'second queued idle' },
        target,
        pubsub,
      ),
    ).toThrow('already reserved');
    expect(() =>
      runtime.reserveRun(
        {
          runId: 'queued-duplicate-idle-run',
          memory: { resource: 'queued-duplicate-user', thread: 'queued-duplicate-thread' },
        } as any,
        pubsub,
      ),
    ).toThrow('already reserved');
    expect(stream).not.toHaveBeenCalled();
  });

  it('clears caller-signal idempotency when a queued idle run is aborted', () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    const stream = vi.fn(async () => ({
      runId: 'unused-queued-idempotency-run',
      status: 'running',
      fullStream: (async function* () {})(),
      _waitUntilFinished: async () => {},
    }));

    runtime.registerRun(
      { id: 'active-agent' } as any,
      {
        runId: 'active-before-queued-idempotency',
        status: 'running',
        _waitUntilFinished: () => new Promise<any>(() => {}),
      } as any,
      {
        runId: 'active-before-queued-idempotency',
        memory: { resource: 'queued-idempotency-user', thread: 'queued-idempotency-thread' },
      } as any,
      pubsub,
    );

    const target = {
      resourceId: 'queued-idempotency-user',
      threadId: 'queued-idempotency-thread',
      ifIdle: {
        streamOptions: { memory: { resource: 'queued-idempotency-user', thread: 'queued-idempotency-thread' } },
      },
    } as any;
    const signal = { id: 'caller-signal-id', type: 'user-message', contents: 'retry queued idle' } as any;

    const first = runtime.sendSignal({ id: 'queued-idempotency-agent', stream } as any, signal, target, pubsub);
    expect(runtime.abortRun(first.runId, pubsub)).toBe(true);
    const second = runtime.sendSignal({ id: 'queued-idempotency-agent', stream } as any, signal, target, pubsub);

    expect(second.runId).not.toBe(first.runId);
    expect(stream).not.toHaveBeenCalled();
  });

  it('rejects run-output waiters when a queued idle run is aborted before it starts', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();

    runtime.registerRun(
      { id: 'active-agent' } as any,
      {
        runId: 'active-before-waiter-abort',
        status: 'running',
        fullStream: (async function* () {})(),
        _waitUntilFinished: () => new Promise<any>(() => {}),
      } as any,
      {
        runId: 'active-before-waiter-abort',
        memory: { resource: 'queued-waiter-abort-user', thread: 'queued-waiter-abort-thread' },
      } as any,
      pubsub,
    );

    const result = runtime.sendSignal(
      { id: 'queued-waiter-abort-agent', stream: vi.fn() } as any,
      { type: 'user-message', contents: 'queued waiter abort' },
      {
        resourceId: 'queued-waiter-abort-user',
        threadId: 'queued-waiter-abort-thread',
        ifIdle: {
          streamOptions: { memory: { resource: 'queued-waiter-abort-user', thread: 'queued-waiter-abort-thread' } },
        } as any,
      },
      pubsub,
    );
    const waiter = runtime.waitForRunOutput(result.runId, pubsub);

    expect(runtime.abortRun(result.runId, pubsub)).toBe(true);
    await expect(waiter).rejects.toThrow('has been aborted');
  });

  it('does not tombstone unknown run ids when abort returns false', () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();

    expect(runtime.abortRun('unknown-abort-run', pubsub)).toBe(false);
    const output = buildFakeOutput({
      runId: 'unknown-abort-run',
      fullOutput: { text: 'not aborted', finishReason: 'stop', usage: {} },
      chunks: [{ runId: 'unknown-abort-run', type: 'finish', payload: {} }],
    });
    expect(() =>
      runtime.registerRun(
        { id: 'unknown-abort-agent' } as any,
        output,
        {
          runId: 'unknown-abort-run',
          memory: { resource: 'unknown-abort-user', thread: 'unknown-abort-thread' },
        } as any,
        pubsub,
      ),
    ).not.toThrow();
  });

  it('aborts a reserved setup run before stream preparation', () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    runtime.reserveRun(
      {
        runId: 'reserved-abort-run',
        memory: { resource: 'reserved-abort-user', thread: 'reserved-abort-thread' },
      } as any,
      pubsub,
    );

    expect(runtime.abortRun('reserved-abort-run', pubsub)).toBe(true);
    const prepared = runtime.prepareRunOptions(
      {
        runId: 'reserved-abort-run',
        memory: { resource: 'reserved-abort-user', thread: 'reserved-abort-thread' },
      } as any,
      pubsub,
    );
    expect(prepared.abortSignal?.aborted).toBe(true);
  });

  it('rejects run-output waiters and releases a prepared setup run on abort before registration', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    runtime.reserveRun(
      {
        runId: 'prepared-setup-abort-run',
        memory: { resource: 'prepared-setup-abort-user', thread: 'prepared-setup-abort-thread' },
      } as any,
      pubsub,
    );
    runtime.prepareRunOptions(
      {
        runId: 'prepared-setup-abort-run',
        memory: { resource: 'prepared-setup-abort-user', thread: 'prepared-setup-abort-thread' },
      } as any,
      pubsub,
    );
    const waiter = runtime.waitForRunOutput('prepared-setup-abort-run', pubsub);

    expect(runtime.abortRun('prepared-setup-abort-run', pubsub)).toBe(true);
    await expect(waiter).rejects.toThrow('has been aborted');
    const successorRelease = runtime.reserveRun(
      {
        runId: 'prepared-setup-successor-run',
        memory: { resource: 'prepared-setup-abort-user', thread: 'prepared-setup-abort-thread' },
      } as any,
      pubsub,
    );
    expect(successorRelease).toBeDefined();
    const lateOutput = buildFakeOutput({
      runId: 'prepared-setup-abort-run',
      fullOutput: { text: 'late aborted response', finishReason: 'stop', usage: {} },
      chunks: [{ runId: 'prepared-setup-abort-run', type: 'finish', payload: {} }],
    });
    expect(() =>
      runtime.registerRun(
        { id: 'prepared-setup-abort-agent' } as any,
        lateOutput,
        {
          runId: 'prepared-setup-abort-run',
          memory: { resource: 'prepared-setup-abort-user', thread: 'prepared-setup-abort-thread' },
        } as any,
        pubsub,
      ),
    ).toThrow('has been aborted');
  });

  it('keeps run-output waiters when a reservation is released for non-terminal retargeting', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    runtime.reserveRun(
      {
        runId: 'retarget-waiter-run',
        memory: { resource: 'retarget-waiter-old-user', thread: 'retarget-waiter-old-thread' },
      } as any,
      pubsub,
    );
    const waiter = runtime.waitForRunOutput('retarget-waiter-run', pubsub);
    let waiterRejected = false;
    void waiter.catch(() => {
      waiterRejected = true;
    });

    expect(
      runtime.releaseRunReservation('retarget-waiter-run', pubsub, { cleanupPrepared: true, clearAbort: true }),
    ).toBe(true);
    await nextTick();
    expect(waiterRejected).toBe(false);

    runtime.reserveRun(
      {
        runId: 'retarget-waiter-run',
        memory: { resource: 'retarget-waiter-new-user', thread: 'retarget-waiter-new-thread' },
      } as any,
      pubsub,
    );
    const output = buildFakeOutput({
      runId: 'retarget-waiter-run',
      fullOutput: { text: 'retarget waiter response', finishReason: 'stop', usage: {} },
      chunks: [{ runId: 'retarget-waiter-run', type: 'finish', payload: {} }],
    });
    runtime.registerRun(
      { id: 'retarget-waiter-agent' } as any,
      output,
      {
        runId: 'retarget-waiter-run',
        memory: { resource: 'retarget-waiter-new-user', thread: 'retarget-waiter-new-thread' },
      } as any,
      pubsub,
    );

    await expect(waiter).resolves.toBe(output);
  });

  it('cancels run-output waiters without poisoning a later registration', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    runtime.reserveRun(
      {
        runId: 'abortable-waiter-run',
        memory: { resource: 'abortable-waiter-user', thread: 'abortable-waiter-thread' },
      } as any,
      pubsub,
    );
    const waitAbortController = new AbortController();
    const waiter = runtime.waitForRunOutput('abortable-waiter-run', pubsub, waitAbortController.signal);

    waitAbortController.abort(new Error('stop waiting'));
    await expect(waiter).rejects.toThrow('stop waiting');

    const output = buildFakeOutput({
      runId: 'abortable-waiter-run',
      fullOutput: { text: 'abortable waiter response', finishReason: 'stop', usage: {} },
      chunks: [{ runId: 'abortable-waiter-run', type: 'finish', payload: {} }],
    });
    runtime.registerRun(
      { id: 'abortable-waiter-agent' } as any,
      output,
      {
        runId: 'abortable-waiter-run',
        memory: { resource: 'abortable-waiter-user', thread: 'abortable-waiter-thread' },
      } as any,
      pubsub,
    );

    await expect(runtime.waitForRunOutput('abortable-waiter-run', pubsub)).resolves.toBe(output);
  });

  it('keeps rejected run ids tombstoned when retry cannot reserve an active thread', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    const releaseRejected = runtime.reserveRun(
      {
        runId: 'rejected-retry-run',
        memory: { resource: 'rejected-retry-user', thread: 'rejected-retry-thread' },
      } as any,
      pubsub,
    );
    const rejectedWaiter = runtime.waitForRunOutput('rejected-retry-run', pubsub);
    releaseRejected!();
    await expect(rejectedWaiter).rejects.toThrow('was rejected');
    runtime.registerRun(
      { id: 'active-retry-agent' } as any,
      {
        runId: 'active-retry-run',
        status: 'running',
        fullStream: (async function* () {})(),
        _waitUntilFinished: () => new Promise<void>(() => {}),
      } as any,
      {
        runId: 'active-retry-run',
        memory: { resource: 'rejected-retry-user', thread: 'rejected-retry-thread' },
      } as any,
      pubsub,
    );

    expect(
      runtime.reserveRun(
        {
          runId: 'rejected-retry-run',
          memory: { resource: 'rejected-retry-user', thread: 'rejected-retry-thread' },
        } as any,
        pubsub,
      ),
    ).toBeUndefined();

    const staleOutput = buildFakeOutput({
      runId: 'rejected-retry-run',
      fullOutput: { text: 'stale rejected response', finishReason: 'stop', usage: {} },
      chunks: [{ runId: 'rejected-retry-run', type: 'finish', payload: {} }],
    });
    expect(() =>
      runtime.registerRun(
        { id: 'stale-retry-agent' } as any,
        staleOutput,
        {
          runId: 'rejected-retry-run',
          memory: { resource: 'rejected-retry-user', thread: 'rejected-retry-thread' },
        } as any,
        pubsub,
      ),
    ).toThrow('was rejected');
  });

  it('starts queued idle wakes left behind when a reservation is retargeted', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    runtime.reserveRun(
      {
        runId: 'retarget-with-idle-run',
        memory: { resource: 'retarget-idle-old-user', thread: 'retarget-idle-old-thread' },
      } as any,
      pubsub,
      'retarget-owner-agent',
    );
    const stream = vi.fn(async () => ({
      runId: 'retarget-queued-idle-run',
      status: 'running',
      fullStream: (async function* () {})(),
      _waitUntilFinished: async () => {},
    }));

    const result = runtime.sendSignal(
      { id: 'retarget-queued-idle-agent', stream } as any,
      { type: 'user-message', contents: 'wake after retarget' },
      {
        resourceId: 'retarget-idle-old-user',
        threadId: 'retarget-idle-old-thread',
        ifIdle: {
          streamOptions: { memory: { resource: 'retarget-idle-old-user', thread: 'retarget-idle-old-thread' } },
        } as any,
      },
      pubsub,
    );
    expect(stream).not.toHaveBeenCalled();

    expect(
      runtime.retargetReservedRun(
        'retarget-with-idle-run',
        { resourceId: 'retarget-idle-old-user', threadId: 'retarget-idle-old-thread' },
        { resourceId: 'retarget-idle-new-user', threadId: 'retarget-idle-new-thread' },
        pubsub,
        'retarget-owner-agent',
      ),
    ).toBe(true);
    await waitForCondition(() => stream.mock.calls.length > 0);
    expect(stream).toHaveBeenCalledWith(
      expect.objectContaining({ contents: 'wake after retarget' }),
      expect.objectContaining({
        runId: result.runId,
        memory: { resource: 'retarget-idle-old-user', thread: 'retarget-idle-old-thread' },
      }),
    );
  });

  it('wakes waiters parked on the old thread when a reservation is retargeted', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    runtime.reserveRun(
      {
        runId: 'retarget-wakes-old-thread-run',
        memory: { resource: 'retarget-wakes-old-user', thread: 'retarget-wakes-old-thread' },
      } as any,
      pubsub,
      'retarget-wakes-owner',
    );

    let waiterResolved = false;
    const waiter = runtime
      .waitForCrossAgentThreadRun(
        { id: 'retarget-wakes-waiter' } as any,
        {
          runId: 'retarget-wakes-waiter-run',
          memory: { resource: 'retarget-wakes-old-user', thread: 'retarget-wakes-old-thread' },
        } as any,
        pubsub,
      )
      .then(() => {
        waiterResolved = true;
      });
    await nextTick();
    expect(waiterResolved).toBe(false);

    expect(
      runtime.retargetReservedRun(
        'retarget-wakes-old-thread-run',
        { resourceId: 'retarget-wakes-old-user', threadId: 'retarget-wakes-old-thread' },
        { resourceId: 'retarget-wakes-new-user', threadId: 'retarget-wakes-new-thread' },
        pubsub,
        'retarget-wakes-owner',
      ),
    ).toBe(true);

    await waiter;
    expect(waiterResolved).toBe(true);
  });

  it('starts a queued idle wake when a reserved setup run is aborted', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    runtime.reserveRun(
      {
        runId: 'reserved-abort-with-idle-run',
        memory: { resource: 'reserved-abort-idle-user', thread: 'reserved-abort-idle-thread' },
      } as any,
      pubsub,
      'reserved-agent',
    );
    const stream = vi.fn(async () => ({
      runId: 'queued-idle-after-abort-run',
      status: 'running',
      fullStream: (async function* () {})(),
      _waitUntilFinished: async () => {},
    }));

    const result = runtime.sendSignal(
      { id: 'queued-idle-after-abort-agent', stream } as any,
      { type: 'user-message', contents: 'wake after abort' },
      {
        resourceId: 'reserved-abort-idle-user',
        threadId: 'reserved-abort-idle-thread',
        ifIdle: {
          streamOptions: { memory: { resource: 'reserved-abort-idle-user', thread: 'reserved-abort-idle-thread' } },
        } as any,
      },
      pubsub,
    );
    expect(result).toEqual(expect.objectContaining({ runId: expect.any(String) }));
    expect(stream).not.toHaveBeenCalled();

    expect(runtime.abortRun('reserved-abort-with-idle-run', pubsub)).toBe(true);
    await nextTick();
    expect(stream).toHaveBeenCalledWith(
      expect.objectContaining({ contents: 'wake after abort' }),
      expect.objectContaining({
        runId: result.runId,
        memory: { resource: 'reserved-abort-idle-user', thread: 'reserved-abort-idle-thread' },
      }),
    );
  });

  it('cleans the run and lease when provider completion rejects without abort', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new ControlledLeasePubSub();
    const resourceId = 'provider-failure-user';
    const threadId = 'provider-failure-thread';
    const runId = 'provider-failure-run';
    const failure = new Error('provider failed');
    const completion = runtime.registerRun(
      { id: 'provider-failure-agent' } as any,
      {
        runId,
        status: 'running',
        fullStream: (async function* () {})(),
        _waitUntilFinished: () => Promise.reject(failure),
      } as any,
      { runId, memory: { resource: resourceId, thread: threadId } } as any,
      pubsub,
    )!;

    await expect(completion).rejects.toBe(failure);
    expect(runtime.getRunOutput(runId, pubsub)).toBeUndefined();
    expect(runtime.getActiveThreadRunId({ resourceId, threadId }, pubsub)).toBeUndefined();
    await waitForCondition(() => !pubsub.owners.has(`${resourceId}\u0000${threadId}`));
  });

  it('releases waiters when draining a queued active signal fails', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new ControlledLeasePubSub();
    let runFailedLeaseOwner: string | undefined;
    let runFailedPublishedWithLiveOwner = false;
    const publish = pubsub.publish.bind(pubsub);
    pubsub.publish = async (topic, event) => {
      const data = (event as { data?: { type?: string; leaseOwner?: string } }).data;
      if (data?.type === 'run-failed') {
        runFailedLeaseOwner = data.leaseOwner;
        runFailedPublishedWithLiveOwner =
          data.leaseOwner !== undefined && [...pubsub.owners.values()].includes(data.leaseOwner);
      }
      await publish(topic, event);
    };
    let finishActive!: () => void;
    const activeFinished = new Promise<void>(resolve => {
      finishActive = resolve;
    });
    const stream = vi.fn(async () => {
      throw new Error('queued active setup failed');
    });
    const owner = { id: 'queued-active-failure-agent', stream };
    const completion = runtime.registerRun(
      owner as any,
      {
        runId: 'queued-active-failure-run',
        status: 'running',
        fullStream: (async function* () {})(),
        _waitUntilFinished: () => activeFinished,
      } as any,
      {
        runId: 'queued-active-failure-run',
        memory: { resource: 'queued-active-failure-user', thread: 'queued-active-failure-thread' },
      } as any,
      pubsub,
    );
    const signalResult = runtime.sendSignal(
      owner as any,
      { type: 'user-message', contents: 'queued active failure' },
      { resourceId: 'queued-active-failure-user', threadId: 'queued-active-failure-thread' },
      pubsub,
    );
    await expect(signalResult.accepted).resolves.toMatchObject({
      action: 'deliver',
      runId: 'queued-active-failure-run',
    });

    let waiterResolved = false;
    const waiter = runtime
      .waitForCrossAgentThreadRun(
        { id: 'queued-active-failure-waiter' } as any,
        {
          runId: 'queued-active-failure-next-run',
          memory: { resource: 'queued-active-failure-user', thread: 'queued-active-failure-thread' },
        } as any,
        pubsub,
      )
      .then(() => {
        waiterResolved = true;
      });
    await nextTick();
    expect(waiterResolved).toBe(false);

    finishActive();
    await expect(completion).rejects.toThrow('queued active setup failed');
    await waiter;
    expect(waiterResolved).toBe(true);
    expect(
      runtime.reserveRun(
        {
          runId: 'queued-active-failure-next-run',
          memory: { resource: 'queued-active-failure-user', thread: 'queued-active-failure-thread' },
        } as any,
        pubsub,
      ),
    ).toEqual(expect.any(Function));
    const failureTerminal = pubsub.publishedData.find(data => data?.type === 'run-failed');
    expect(failureTerminal?.leaseOwner).toBe(runFailedLeaseOwner);
    expect(runFailedPublishedWithLiveOwner).toBe(true);
    expect(
      pubsub.publishedData.some(data => data?.type === 'run-aborted' && data.runId === failureTerminal?.runId),
    ).toBe(false);
  });

  it('preserves queued signals for the successor when a registered run is aborted', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    let finishActive!: () => void;
    const activeFinished = new Promise<void>(resolve => {
      finishActive = resolve;
    });
    const streamedFollowUps: any[] = [];
    const agent = {
      id: 'active-agent',
      stream: async (signal: any) => {
        streamedFollowUps.push(signal);
        return {
          runId: 'prepared-abort-follow-up',
          status: 'running',
          fullStream: (async function* () {})(),
          _waitUntilFinished: () => Promise.resolve(),
        } as any;
      },
    } as any;

    const completion = runtime.registerRun(
      agent,
      {
        runId: 'prepared-abort-run',
        status: 'running',
        fullStream: (async function* () {})(),
        _waitUntilFinished: () => activeFinished,
      } as any,
      {
        runId: 'prepared-abort-run',
        memory: { resource: 'prepared-abort-user', thread: 'prepared-abort-thread' },
      } as any,
      pubsub,
    );

    const signalResult = runtime.sendSignal(
      agent,
      { type: 'user-message', contents: 'queued follow-up signal' },
      {
        runId: 'prepared-abort-run',
      },
      pubsub,
    );
    expect(signalResult).toEqual(expect.objectContaining({ runId: 'prepared-abort-run' }));

    expect(runtime.abortRun('prepared-abort-run', pubsub)).toBe(true);
    expect(() =>
      runtime.sendSignal(
        agent,
        { type: 'user-message', contents: 'post-abort stale signal' },
        {
          runId: 'prepared-abort-run',
        },
        pubsub,
      ),
    ).toThrow('has been aborted');
    finishActive();
    await completion;
    // Preserve-by-default abort: the queued follow-up input survives the
    // abort and the bounded abort finalizer hands it to the next run instead
    // of deleting it.
    await vi.waitFor(() =>
      expect(streamedFollowUps.map(signal => signal.contents)).toContain('queued follow-up signal'),
    );

    runtime.reserveRun(
      {
        runId: 'prepared-abort-successor-run',
        memory: { resource: 'prepared-abort-user', thread: 'prepared-abort-thread' },
      } as any,
      pubsub,
    );
    expect(runtime.drainPendingSignals('prepared-abort-successor-run', pubsub)).toEqual([]);
  });

  it('rejects duplicate registered run ids on the same thread', () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    const streamOptions = {
      runId: 'duplicate-registered-run',
      memory: { resource: 'duplicate-registered-user', thread: 'duplicate-registered-thread' },
    } as any;

    runtime.registerRun(
      { id: 'active-agent' } as any,
      {
        runId: 'duplicate-registered-run',
        status: 'running',
        fullStream: (async function* () {})(),
        _waitUntilFinished: () => new Promise<void>(() => {}),
      } as any,
      streamOptions,
      pubsub,
    );

    expect(() =>
      runtime.registerRun(
        { id: 'active-agent' } as any,
        {
          runId: 'duplicate-registered-run',
          status: 'running',
          fullStream: (async function* () {})(),
          _waitUntilFinished: () => new Promise<void>(() => {}),
        } as any,
        streamOptions,
        pubsub,
      ),
    ).toThrow('already registered');
  });

  it('rejects same-agent registration when another run is active on the thread', () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    const streamOptions = {
      memory: { resource: 'same-agent-active-user', thread: 'same-agent-active-thread' },
    } as any;

    runtime.registerRun(
      { id: 'same-active-agent' } as any,
      {
        runId: 'same-active-first-run',
        status: 'running',
        fullStream: (async function* () {})(),
        _waitUntilFinished: () => new Promise<void>(() => {}),
      } as any,
      { ...streamOptions, runId: 'same-active-first-run' },
      pubsub,
    );

    expect(() =>
      runtime.registerRun(
        { id: 'same-active-agent' } as any,
        {
          runId: 'same-active-second-run',
          status: 'running',
          fullStream: (async function* () {})(),
          _waitUntilFinished: () => new Promise<void>(() => {}),
        } as any,
        { ...streamOptions, runId: 'same-active-second-run' },
        pubsub,
      ),
    ).toThrow('already active for this thread');
  });

  it('does not treat a same-agent active run as a cross-agent blocker', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    let finishActive!: () => void;
    const activeFinished = new Promise<void>(resolve => {
      finishActive = resolve;
    });
    const completion = runtime.registerRun(
      { id: 'same-wait-agent' } as any,
      {
        runId: 'same-wait-active-run',
        status: 'running',
        fullStream: (async function* () {})(),
        _waitUntilFinished: () => activeFinished,
      } as any,
      {
        runId: 'same-wait-active-run',
        memory: { resource: 'same-wait-user', thread: 'same-wait-thread' },
      } as any,
      pubsub,
    );

    let waiterResolved = false;
    const waiter = runtime
      .waitForCrossAgentThreadRun(
        { id: 'same-wait-agent' } as any,
        {
          runId: 'same-wait-next-run',
          memory: { resource: 'same-wait-user', thread: 'same-wait-thread' },
        } as any,
        pubsub,
      )
      .then(() => {
        waiterResolved = true;
      });
    await nextTick();
    expect(waiterResolved).toBe(true);

    finishActive();
    await completion;
    await waiter;
    expect(waiterResolved).toBe(true);
  });

  it('does not block on a completed active record awaiting cleanup', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    let finishActive!: () => void;
    const activeFinished = new Promise<void>(resolve => {
      finishActive = resolve;
    });
    const completion = runtime.registerRun(
      { id: 'completed-window-agent' } as any,
      {
        runId: 'completed-window-active-run',
        status: 'success',
        fullStream: (async function* () {})(),
        _waitUntilFinished: () => activeFinished,
      } as any,
      {
        runId: 'completed-window-active-run',
        memory: { resource: 'completed-window-user', thread: 'completed-window-thread' },
      } as any,
      pubsub,
    );

    let waiterResolved = false;
    const waiter = runtime
      .waitForCrossAgentThreadRun(
        { id: 'completed-window-next-agent' } as any,
        {
          runId: 'completed-window-next-run',
          memory: { resource: 'completed-window-user', thread: 'completed-window-thread' },
        } as any,
        pubsub,
      )
      .then(() => {
        waiterResolved = true;
      });
    await nextTick();
    expect(waiterResolved).toBe(true);

    finishActive();
    await completion;
    await waiter;
    expect(waiterResolved).toBe(true);
  });

  it('reserves the thread after waiting so concurrent stream callers do not overlap execution', async () => {
    let finishActive!: () => void;
    const activeFinished = new Promise<void>(resolve => {
      finishActive = resolve;
    });
    let finishFirstWaiter!: () => void;
    const firstWaiterFinished = new Promise<void>(resolve => {
      finishFirstWaiter = resolve;
    });
    let streamCalls = 0;
    const runner = new Agent({
      id: 'post-wait-reservation-agent',
      name: 'Post Wait Reservation Agent',
      instructions: 'Test',
      model: new MockLanguageModelV2({
        doStream: async () => {
          streamCalls += 1;
          const call = streamCalls;
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: new ReadableStream({
              async start(controller) {
                controller.enqueue({ type: 'stream-start', warnings: [] });
                controller.enqueue({
                  type: 'response-metadata',
                  id: `id-${call}`,
                  modelId: 'mock-model-id',
                  timestamp: new Date(0),
                });
                controller.enqueue({ type: 'text-start', id: `text-${call}` });
                controller.enqueue({ type: 'text-delta', id: `text-${call}`, delta: `response ${call}` });
                if (call === 1) await firstWaiterFinished;
                controller.enqueue({ type: 'text-end', id: `text-${call}` });
                controller.enqueue({
                  type: 'finish',
                  finishReason: 'stop',
                  usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
                });
                controller.close();
              },
            }),
          };
        },
      }),
    });
    const activeCompletion = agentThreadStreamRuntime.registerRun(
      runner as any,
      {
        runId: 'post-wait-active-run',
        status: 'running',
        fullStream: (async function* () {})(),
        _waitUntilFinished: () => activeFinished,
      } as any,
      {
        runId: 'post-wait-active-run',
        memory: { resource: 'post-wait-user', thread: 'post-wait-thread' },
      } as any,
    );

    const first = runner.stream('first', {
      memory: { resource: 'post-wait-user', thread: 'post-wait-thread' },
    });
    const second = runner.stream('second', {
      memory: { resource: 'post-wait-user', thread: 'post-wait-thread' },
    });
    await nextTick();
    expect(streamCalls).toBe(0);

    finishActive();
    await activeCompletion;
    await waitForCondition(() => streamCalls === 1);
    await nextTick();
    expect(streamCalls).toBe(1);

    const firstOutput = await first;
    const firstText = firstOutput.text;
    finishFirstWaiter();
    await expect(firstText).resolves.toBe('response 1');
    await waitForCondition(() => streamCalls === 2);

    const secondOutput = await second;
    await expect(secondOutput.text).resolves.toBe('response 2');
  });

  it('drains accepted queued signals before releasing waiters after async completion publish', async () => {
    const runtime = agentThreadStreamRuntime;
    const pubsub = new BlockingRunCompletedPubSub();
    let finishActive!: () => void;
    const activeFinished = new Promise<void>(resolve => {
      finishActive = resolve;
    });
    let finishQueued!: () => void;
    const queuedFinished = new Promise<void>(resolve => {
      finishQueued = resolve;
    });
    const ownerCalls: string[] = [];
    const competitorCalls: string[] = [];
    const callOrder: string[] = [];
    const owner = new Agent({
      id: 'completion-drain-agent',
      name: 'Completion Drain Owner',
      instructions: 'Test',
      model: new MockLanguageModelV2({
        doStream: async ({ prompt }) => {
          callOrder.push('queued');
          ownerCalls.push(JSON.stringify(prompt));
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: new ReadableStream({
              async start(controller) {
                controller.enqueue({ type: 'stream-start', warnings: [] });
                controller.enqueue({
                  type: 'response-metadata',
                  id: 'queued-id',
                  modelId: 'mock-model-id',
                  timestamp: new Date(0),
                });
                controller.enqueue({ type: 'text-start', id: 'queued-text' });
                controller.enqueue({ type: 'text-delta', id: 'queued-text', delta: 'queued response' });
                await queuedFinished;
                controller.enqueue({ type: 'text-end', id: 'queued-text' });
                controller.enqueue({
                  type: 'finish',
                  finishReason: 'stop',
                  usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
                });
                controller.close();
              },
            }),
          };
        },
      }),
    });
    const competitor = new Agent({
      id: 'completion-drain-agent',
      name: 'Completion Drain Competitor',
      instructions: 'Test',
      model: new MockLanguageModelV2({
        doStream: async ({ prompt }) => {
          callOrder.push('competitor');
          competitorCalls.push(JSON.stringify(prompt));
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: convertArrayToReadableStream([
              { type: 'stream-start', warnings: [] },
              { type: 'response-metadata', id: 'competitor-id', modelId: 'mock-model-id', timestamp: new Date(0) },
              { type: 'text-start', id: 'competitor-text' },
              { type: 'text-delta', id: 'competitor-text', delta: 'competitor response' },
              { type: 'text-end', id: 'competitor-text' },
              {
                type: 'finish',
                finishReason: 'stop',
                usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
              },
            ]),
          };
        },
      }),
    });

    const completion = runtime.registerRun(
      owner as any,
      {
        runId: 'completion-drain-active-run',
        status: 'running',
        fullStream: (async function* () {})(),
        _waitUntilFinished: () => activeFinished,
      } as any,
      {
        runId: 'completion-drain-active-run',
        memory: { resource: 'completion-drain-user', thread: 'completion-drain-thread' },
      } as any,
      pubsub,
    );
    const queuedSignal = runtime.sendSignal(
      owner as any,
      { type: 'user-message', contents: 'queued signal' },
      { resourceId: 'completion-drain-user', threadId: 'completion-drain-thread' },
      pubsub,
    );
    await expect(queuedSignal.accepted).resolves.toMatchObject({
      action: 'deliver',
      runId: 'completion-drain-active-run',
    });

    finishActive();
    await waitForCondition(() => pubsub.sawRunCompleted);
    const competitorStream = competitor.stream('competing stream', {
      memory: { resource: 'completion-drain-user', thread: 'completion-drain-thread' },
      _pubsub: pubsub,
    } as any);
    await nextTick();
    expect(ownerCalls).toHaveLength(0);
    expect(competitorCalls).toHaveLength(0);

    pubsub.unblockRunCompleted();
    await waitForCondition(() => ownerCalls.length === 1);
    expect(JSON.stringify(ownerCalls)).toContain('queued signal');
    expect(callOrder[0]).toBe('queued');

    finishQueued();
    await completion;
    await expect(competitorStream.then(stream => stream.text)).resolves.toBe('competitor response');
    expect(competitorCalls).toHaveLength(1);
    expect(callOrder).toEqual(['queued', 'competitor']);
  });

  it('moves reservation waiters onto the registered run on setup success', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    runtime.reserveRun(
      {
        runId: 'reserved-success-run',
        memory: { resource: 'reserved-success-user', thread: 'reserved-success-thread' },
      } as any,
      pubsub,
      'owner-agent',
    );

    let waiterResolved = false;
    const waiter = runtime
      .waitForCrossAgentThreadRun(
        { id: 'other-agent' } as any,
        {
          runId: 'other-run',
          memory: { resource: 'reserved-success-user', thread: 'reserved-success-thread' },
        } as any,
        pubsub,
      )
      .then(() => {
        waiterResolved = true;
      });
    await nextTick();
    expect(waiterResolved).toBe(false);

    let finishActive!: () => void;
    const activeFinished = new Promise<void>(resolve => {
      finishActive = resolve;
    });
    const completion = runtime.registerRun(
      { id: 'owner-agent' } as any,
      {
        runId: 'reserved-success-run',
        status: 'running',
        fullStream: (async function* () {})(),
        _waitUntilFinished: () => activeFinished,
      } as any,
      {
        runId: 'reserved-success-run',
        memory: { resource: 'reserved-success-user', thread: 'reserved-success-thread' },
      } as any,
      pubsub,
    );
    await nextTick();
    expect(waiterResolved).toBe(false);

    finishActive();
    await completion;
    await waiter;
    expect(waiterResolved).toBe(true);
  });

  it('does not treat matching run ids from another agent as its own reservation', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    let finishActive!: () => void;
    const activeFinished = new Promise<void>(resolve => {
      finishActive = resolve;
    });

    const completion = runtime.registerRun(
      { id: 'owner-agent' } as any,
      {
        runId: 'shared-run-id',
        status: 'running',
        fullStream: (async function* () {})(),
        _waitUntilFinished: () => activeFinished,
      } as any,
      {
        runId: 'shared-run-id',
        memory: { resource: 'run-id-collision-user', thread: 'run-id-collision-thread' },
      } as any,
      pubsub,
    );

    let waiterResolved = false;
    const waiter = runtime
      .waitForCrossAgentThreadRun(
        { id: 'different-agent' } as any,
        {
          runId: 'shared-run-id',
          memory: { resource: 'run-id-collision-user', thread: 'run-id-collision-thread' },
        } as any,
        pubsub,
      )
      .then(() => {
        waiterResolved = true;
      });
    await nextTick();
    expect(waiterResolved).toBe(false);

    finishActive();
    await completion;
    await waiter;
    expect(waiterResolved).toBe(true);
  });

  it('does not treat matching run ids from the same agent as its own reservation without ownership', async () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    const release = runtime.reserveRun(
      {
        runId: 'same-agent-shared-run-id',
        memory: { resource: 'same-agent-collision-user', thread: 'same-agent-collision-thread' },
      } as any,
      pubsub,
      'owner-agent',
    );

    let waiterResolved = false;
    const waiter = runtime
      .waitForCrossAgentThreadRun(
        { id: 'owner-agent' } as any,
        {
          runId: 'same-agent-shared-run-id',
          memory: { resource: 'same-agent-collision-user', thread: 'same-agent-collision-thread' },
        } as any,
        pubsub,
      )
      .then(() => {
        waiterResolved = true;
      });
    await nextTick();
    expect(waiterResolved).toBe(false);

    release?.();
    await waiter;
    expect(waiterResolved).toBe(true);
  });

  it('rejects registration when another agent owns the reservation', () => {
    const runtime = new AgentThreadStreamRuntime();
    const pubsub = new EventEmitterPubSub();
    runtime.reserveRun(
      {
        runId: 'reserved-owner-register-run',
        memory: { resource: 'reserved-owner-register-user', thread: 'reserved-owner-register-thread' },
      } as any,
      pubsub,
      'owner-agent',
    );

    expect(() =>
      runtime.registerRun(
        { id: 'different-agent' } as any,
        {
          runId: 'reserved-owner-register-run',
          status: 'running',
          fullStream: (async function* () {})(),
          _waitUntilFinished: () => new Promise<void>(() => {}),
        } as any,
        {
          runId: 'reserved-owner-register-run',
          memory: { resource: 'reserved-owner-register-user', thread: 'reserved-owner-register-thread' },
        } as any,
        pubsub,
      ),
    ).toThrow('reserved by another agent');
  });

  it('cleans up a thread subscription and completes the iterator', async () => {
    const agent = new Agent({
      id: 'cleanup-signal-agent',
      name: 'Cleanup Signal Agent',
      instructions: 'Test',
      model: createTextStreamModel('cleanup response'),
    });

    const subscription = await agent.subscribeToThread({
      threadId: 'cleanup-thread',
      resourceId: 'cleanup-user',
    });
    const iterator = subscription.stream[Symbol.asyncIterator]();

    subscription.unsubscribe();
    await expect(iterator.next()).resolves.toEqual({ value: undefined, done: true });
  });

  it('allows a thread follower to abort the active run controller', () => {
    const runtime = new AgentThreadStreamRuntime();
    const options = runtime.prepareRunOptions({
      runId: 'abort-run',
      memory: { thread: 'abort-thread', resource: 'abort-user' },
    } as any);
    const neverFinishes = new Promise<any>(() => {});

    runtime.registerRun(
      { id: 'abortable-agent' } as any,
      {
        runId: 'abort-run',
        status: 'running',
        _waitUntilFinished: () => neverFinishes,
      } as any,
      options,
    );

    expect(runtime.abortThread({ threadId: 'abort-thread', resourceId: 'abort-user' })).toBe(true);
    expect(options.abortSignal?.aborted).toBe(true);
  });

  it('does not consume active run output while watching for completion', () => {
    const runtime = new AgentThreadStreamRuntime();
    const getFullOutput = vi.fn();

    runtime.registerRun(
      { id: 'watch-agent' } as any,
      {
        runId: 'watch-run',
        status: 'running',
        getFullOutput,
        _waitUntilFinished: () => new Promise<any>(() => {}),
      } as any,
      {
        runId: 'watch-run',
        memory: { thread: 'watch-thread', resource: 'watch-user' },
      } as any,
    );

    expect(getFullOutput).not.toHaveBeenCalled();
  });

  it('delivers a future thread run to multiple subscribers', async () => {
    const agent = new Agent({
      id: 'multiple-subscriber-agent',
      name: 'Multiple Subscriber Agent',
      instructions: 'Test',
      model: createTextStreamModel('multi response'),
    });

    const firstSubscription = await agent.subscribeToThread({
      threadId: 'multi-thread',
      resourceId: 'multi-user',
    });
    const secondSubscription = await agent.subscribeToThread({
      threadId: 'multi-thread',
      resourceId: 'multi-user',
    });
    const firstRunPromise = readNextRun(firstSubscription.stream[Symbol.asyncIterator]());
    const secondRunPromise = readNextRun(secondSubscription.stream[Symbol.asyncIterator]());

    const stream = await agent.stream('Hello', {
      memory: { thread: 'multi-thread', resource: 'multi-user' },
    });

    await expect(firstRunPromise).resolves.toMatchObject({ value: { runId: stream.runId }, done: false });
    await expect(secondRunPromise).resolves.toMatchObject({ value: { runId: stream.runId }, done: false });

    firstSubscription.unsubscribe();
    secondSubscription.unsubscribe();
  });

  it('isolates subscriptions by resource and thread id', async () => {
    const agent = new Agent({
      id: 'isolated-signal-agent',
      name: 'Isolated Signal Agent',
      instructions: 'Test',
      model: createTextStreamModel('isolated response'),
    });

    const targetSubscription = await agent.subscribeToThread({
      threadId: 'isolated-thread',
      resourceId: 'isolated-user',
    });
    const otherResourceSubscription = await agent.subscribeToThread({
      threadId: 'isolated-thread',
      resourceId: 'other-user',
    });
    const otherThreadSubscription = await agent.subscribeToThread({
      threadId: 'other-thread',
      resourceId: 'isolated-user',
    });

    const targetNext = readNextRun(targetSubscription.stream[Symbol.asyncIterator]());
    const otherResourceNext = readNextRun(otherResourceSubscription.stream[Symbol.asyncIterator]());
    const otherThreadNext = readNextRun(otherThreadSubscription.stream[Symbol.asyncIterator]());

    const stream = await agent.stream('Hello', {
      memory: { thread: 'isolated-thread', resource: 'isolated-user' },
    });

    await expect(targetNext).resolves.toMatchObject({ value: { runId: stream.runId }, done: false });
    await nextTick();

    otherResourceSubscription.unsubscribe();
    otherThreadSubscription.unsubscribe();
    await expect(otherResourceNext).resolves.toEqual({ value: undefined, done: true });
    await expect(otherThreadNext).resolves.toEqual({ value: undefined, done: true });

    targetSubscription.unsubscribe();
  });

  it('does not replay completed thread runs to late subscribers', async () => {
    const agent = new Agent({
      id: 'late-subscription-agent',
      name: 'Late Subscription Agent',
      instructions: 'Test',
      model: createTextStreamModel('late response'),
    });

    const stream = await agent.stream('Hello', {
      memory: { thread: 'late-thread', resource: 'late-user' },
    });
    await stream.text;
    const subscription = await agent.subscribeToThread({
      threadId: 'late-thread',
      resourceId: 'late-user',
    });
    const iterator = subscription.stream[Symbol.asyncIterator]();

    const nextRun = readNextRun(iterator);
    await nextTick();
    subscription.unsubscribe();
    await expect(nextRun).resolves.toEqual({ value: undefined, done: true });
  });

  it.each(['user-message', 'reactive', 'system-reminder'] as const)(
    'drains active %s signals with matching visibility',
    async type => {
      let releaseFirst!: () => void;
      const firstFinished = new Promise<void>(resolve => {
        releaseFirst = resolve;
      });
      let streamCount = 0;
      const prompts: any[][] = [];

      const model = new MockLanguageModelV2({
        doStream: async ({ prompt }) => {
          streamCount += 1;
          prompts.push(prompt);
          const responseText = streamCount === 1 ? 'run id first response' : 'run id signal response';

          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: new ReadableStream({
              async start(controller) {
                controller.enqueue({ type: 'stream-start', warnings: [] });
                controller.enqueue({
                  type: 'response-metadata',
                  id: `run-id-${streamCount}`,
                  modelId: 'mock-model-id',
                  timestamp: new Date(0),
                });
                controller.enqueue({ type: 'text-start', id: 'text-1' });
                controller.enqueue({ type: 'text-delta', id: 'text-1', delta: responseText });
                controller.enqueue({ type: 'text-end', id: 'text-1' });
                if (streamCount === 1) {
                  await firstFinished;
                }
                controller.enqueue({
                  type: 'finish',
                  finishReason: 'stop',
                  usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
                });
                controller.close();
              },
            }),
          };
        },
      });

      const agent = new Agent({
        id: 'run-id-signal-agent',
        name: 'Run Id Signal Agent',
        instructions: 'Test',
        model,
      });
      const subscription = await agent.subscribeToThread({
        threadId: 'run-id-thread',
        resourceId: 'run-id-user',
      });
      const iterator = subscription.stream[Symbol.asyncIterator]();
      const firstRunPromise = readNextRunWithParts(iterator);

      const stream = await agent.stream('Hello', {
        memory: { thread: 'run-id-thread', resource: 'run-id-user' },
      });
      await expect(waitForActiveRun(subscription)).resolves.toBe(stream.runId);

      const runIdSignalResult = agent.sendSignal({ type, contents: 'Hello by run id' }, { runId: stream.runId });
      await expect(runIdSignalResult.accepted).resolves.toMatchObject({ action: 'deliver', runId: stream.runId });

      releaseFirst();
      const run = await firstRunPromise;
      expect(run.value.parts.filter((part: any) => part.data?.contents === 'Hello by run id')).toHaveLength(1);
      await expect(stream.text).resolves.toBe('run id first responserun id signal response');
      expect(streamCount).toBe(2);
      expect(JSON.stringify(prompts[1])).toContain('Hello by run id');

      subscription.unsubscribe();
    },
  );

  it('throws when sending a signal to an unknown run id without a thread target', () => {
    const agent = new Agent({
      id: 'missing-run-signal-agent',
      name: 'Missing Run Signal Agent',
      instructions: 'Test',
      model: createTextStreamModel('missing run response'),
    });

    expect(() => agent.sendSignal({ type: 'user-message', contents: 'Hello' }, { runId: 'missing-run-id' })).toThrow(
      'No active agent run found for signal target',
    );
  });

  it.each(['reactive', 'system-reminder'] as const)(
    'delivers idle %s context to the model and echoes it',
    async type => {
      let capturedPrompt: any[] | undefined;
      const model = new MockLanguageModelV2({
        doStream: async ({ prompt }) => {
          capturedPrompt = prompt;
          return {
            rawCall: { rawPrompt: prompt, rawSettings: {} },
            warnings: [],
            stream: convertArrayToReadableStream([
              { type: 'stream-start', warnings: [] },
              { type: 'response-metadata', id: 'system-signal-id', modelId: 'mock-model-id', timestamp: new Date(0) },
              { type: 'text-start', id: 'text-1' },
              { type: 'text-delta', id: 'text-1', delta: 'system signal response' },
              { type: 'text-end', id: 'text-1' },
              {
                type: 'finish',
                finishReason: 'stop',
                usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
              },
            ]),
          };
        },
      });

      const agent = new Agent({
        id: 'system-signal-agent',
        name: 'System Signal Agent',
        instructions: 'Test',
        model,
      });

      const subscription = await agent.subscribeToThread({
        resourceId: 'system-signal-user',
        threadId: 'system-signal-thread',
      });
      const runPromise = readNextRunWithParts(subscription.stream[Symbol.asyncIterator]());
      const stream = await agent.sendSignal(
        { type, contents: 'continue', attributes: { reminderType: 'test-reminder' } },
        {
          resourceId: 'system-signal-user',
          threadId: 'system-signal-thread',
          ifIdle: { streamOptions: { memory: { resource: 'system-signal-user', thread: 'system-signal-thread' } } },
        },
      );

      await expect(stream.accepted).resolves.toMatchObject({ action: 'wake' });
      const run = await runPromise;
      subscription.unsubscribe();
      expect(run.value.parts.filter((part: any) => part.type === 'data-signal')).toEqual([
        expect.objectContaining({ data: expect.objectContaining({ type: 'reactive', contents: 'continue' }) }),
      ]);
      expect(
        capturedPrompt?.some(
          message =>
            message.role === 'user' &&
            Array.isArray(message.content) &&
            message.content.some(
              (part: any) => part.text === '<system-reminder reminderType="test-reminder">continue</system-reminder>',
            ),
        ),
      ).toBe(true);
    },
  );

  describe('delivery option attributes', () => {
    it('resolveDeliveryAttributes merges option attributes into signal attributes', () => {
      const signal = createSignal({
        type: 'user-message',
        contents: 'hello',
        attributes: { existing: 'yes' },
      });

      const resolved = resolveDeliveryAttributes(signal, { delivery: 'while-active' });
      expect(resolved.attributes).toEqual({ existing: 'yes', delivery: 'while-active' });
    });

    it('resolveDeliveryAttributes returns same signal when no option attributes are selected', () => {
      const signal = createSignal({
        type: 'user-message',
        contents: 'hello',
      });

      const resolved = resolveDeliveryAttributes(signal, undefined);
      expect(resolved).toBe(signal);
    });

    it('resolved delivery attributes appear in toLLMMessage XML', () => {
      const signal = createSignal({
        type: 'user-message',
        contents: 'fix the bug',
      });

      const resolved = resolveDeliveryAttributes(signal, { delivery: 'while-active' });
      expect(resolved.toLLMMessage()).toEqual({
        role: 'user',
        content: '<user delivery="while-active">fix the bug</user>',
      });
    });

    it('resolved delivery attributes appear in toDBMessage and toDataPart', () => {
      const signal = createSignal({
        type: 'user-message',
        contents: 'fix the bug',
      });

      const resolved = resolveDeliveryAttributes(signal, { delivery: 'while-active' });
      const db = resolved.toDBMessage({ threadId: 't', resourceId: 'r' });
      expect((db.content.metadata!.signal as Record<string, unknown>).attributes).toEqual({
        delivery: 'while-active',
      });

      const dataPart = resolved.toDataPart();
      expect(dataPart.data.attributes).toEqual({ delivery: 'while-active' });
    });

    it('thread-stream-runtime resolves ifActive.attributes as while-active on active signal delivery', () => {
      const runtime = new AgentThreadStreamRuntime();
      const pubsub = new EventEmitterPubSub();
      const agent = { id: 'delivery-active-agent' } as any;

      // Prepare and register a run that is still "running" so the thread is active.
      const options = runtime.prepareRunOptions(
        {
          runId: 'active-run',
          memory: { thread: 'delivery-thread', resource: 'delivery-resource' },
        } as any,
        pubsub,
      );
      runtime.registerRun(
        agent,
        {
          runId: 'active-run',
          status: 'running',
          _waitUntilFinished: () => new Promise<any>(() => {}),
        } as any,
        options,
        pubsub,
      );

      // Send a signal while the run is still active.
      const result = runtime.sendSignal(
        agent,
        {
          type: 'user-message',
          contents: 'while-active test',
        },
        {
          resourceId: 'delivery-resource',
          threadId: 'delivery-thread',
          ifActive: { attributes: { delivery: 'while-active' } },
          ifIdle: {
            attributes: { delivery: 'message' },
            streamOptions: {
              memory: { thread: 'delivery-thread', resource: 'delivery-resource' },
            },
          },
        },
        pubsub,
      );

      // Active run → ifActive.attributes → delivery: 'while-active'
      expect(result.signal.attributes).toEqual({ delivery: 'while-active' });
    });

    it('thread-stream-runtime resolves ifIdle.attributes as message on idle signal delivery', () => {
      const runtime = new AgentThreadStreamRuntime();
      const pubsub = new EventEmitterPubSub();
      const agent = { id: 'delivery-idle-agent', stream: () => new Promise(() => {}) } as any;

      // No run registered → thread is idle.
      const result = runtime.sendSignal(
        agent,
        {
          type: 'user-message',
          contents: 'idle test',
        },
        {
          resourceId: 'idle-resource',
          threadId: 'idle-thread',
          ifActive: { attributes: { delivery: 'while-active' } },
          ifIdle: {
            attributes: { delivery: 'message' },
            streamOptions: {
              memory: { thread: 'idle-thread', resource: 'idle-resource' },
            },
          },
        },
        pubsub,
      );

      // No active run → ifIdle.attributes → delivery: 'message'
      expect(result.signal.attributes).toEqual({ delivery: 'message' });
    });
  });
});
