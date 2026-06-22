/**
 * Harness v1 — resolver + lifecycle tests.
 *
 * Covers the M1 slice: `new Harness(config)`, `harness.session(...)` for
 * every §5.3 branch, lease acquisition, close, list, shutdown.
 *
 * Storage is the real `InMemoryHarness` adapter — not a mock — so the lease
 * + CAS contract is exercised end-to-end. Agents are minimal stubs because
 * the resolver/lifecycle paths don't dispatch model calls.
 */

import { beforeEach, describe, expect, it, vi } from 'vitest';
import { z } from 'zod';

import { Agent } from '../../agent';
import { AgentChannels } from '../../channels';
import type { ChannelProvider } from '../../channels';
import { PubSub } from '../../events';
import type { Event, EventCallback, SubscribeOptions } from '../../events';
import { Mastra } from '../../mastra';
import type {
  ChannelActionReceipt,
  ChannelActionToken,
  ChannelInboxItem,
  ChannelOutboxEnqueueOptions,
  ChannelOutboxItem,
} from '../../storage/domains/harness';
import {
  HarnessStorage,
  HarnessStorageChannelOutboxClaimConflictError,
  HarnessStorageDeleteGuardConflictError,
  HarnessStorageVersionConflictError,
} from '../../storage/domains/harness';
import { InMemoryHarness } from '../../storage/domains/harness/inmemory';
import { InMemoryDB } from '../../storage/domains/inmemory-db';
import { InMemoryStore } from '../../storage/mock';
import { MastraWorker } from '../../worker';

import { extractSignalContents, MockAgent } from './__test-utils__';
import {
  HarnessAbortedError,
  HarnessConfigError,
  HarnessLiveSessionLimitError,
  HarnessOverrideConflictError,
  HarnessQueueFullError,
  HarnessSessionClosedError,
  HarnessSessionClosingError,
  HarnessSessionConflictError,
  HarnessSessionCorruptError,
  HarnessSessionDeletedError,
  HarnessSessionLockedError,
  HarnessSessionNotFoundError,
  HarnessStorageError,
  HarnessValidationError,
} from './errors';
import type { HarnessSessionDeleteBlockedError } from './errors';
import { Harness, createHarnessOperatorThreadController } from './harness';
import type { HarnessChannelConfig } from './types';

const TEST_MODEL = 'test/model' as any;

function makeAgent(name = 'test-agent') {
  return new Agent({
    id: name,
    name,
    instructions: 'test',
    model: TEST_MODEL,
  });
}

function makeStorage() {
  const db = new InMemoryDB();
  const storage = new InMemoryHarness({ db });
  return storage;
}

class CloseAfterLeaseStorage extends InMemoryHarness {
  readonly closeAfterLeaseSessionIds = new Set<string>();

  override async acquireSessionLease(
    input: Parameters<InMemoryHarness['acquireSessionLease']>[0],
  ): ReturnType<InMemoryHarness['acquireSessionLease']> {
    const lease = await super.acquireSessionLease(input);
    if (!this.closeAfterLeaseSessionIds.delete(input.sessionId)) return lease;

    const latest = await super.loadSession({ harnessName: input.harnessName, sessionId: input.sessionId });
    if (latest && latest.closedAt === undefined) {
      const closedAt = Date.now();
      await super.saveSession(
        {
          ...latest,
          closedAt,
          lastActivityAt: closedAt,
        },
        { harnessName: latest.harnessName, ownerId: input.ownerId, ifVersion: lease.version },
      );
    }

    return lease;
  }
}

function makeChannelProvider(id = 'slack'): ChannelProvider {
  return {
    id,
    getRoutes: () => [],
  };
}

function makeHarnessChannelConfig(overrides: Partial<HarnessChannelConfig> = {}): HarnessChannelConfig {
  return {
    providerId: 'slack',
    platform: 'slack',
    adapter: {
      deliver: async () => ({}),
    },
    ingress: {
      resolveResource: async () => ({
        resourceId: 'resource-1',
        mode: 'shared-resource',
      }),
    },
    ...overrides,
  };
}

function channelOutboxInput(overrides: Partial<ChannelOutboxEnqueueOptions> = {}): ChannelOutboxEnqueueOptions {
  return {
    channelId: 'support',
    idempotencyKey: 'outbox-key-1',
    resourceId: 'resource-1',
    threadId: 'thread-1',
    sessionId: 'session-1',
    owningSessionId: 'session-1',
    target: {
      platform: 'slack',
      externalTenantId: 'tenant-1',
      externalChannelId: 'channel-1',
      externalThreadId: 'thread-ext-1',
    },
    kind: 'assistant-message',
    operationKind: 'message-create',
    payload: { text: 'hello' },
    ...overrides,
  };
}

function channelInboxRow(overrides: Partial<ChannelInboxItem> = {}): ChannelInboxItem {
  return {
    id: 'inbox-1',
    harnessName: 'primary',
    channelId: 'support',
    providerId: 'slack',
    idempotencyKey: 'provider-event-1',
    payloadHash: 'payload-hash-1',
    admissionId: 'admission-1',
    bindingId: 'support-binding',
    resourceId: 'resource-1',
    threadId: 'thread-1',
    sessionId: 'session-1',
    externalMessageId: 'message-1',
    receivedAt: 1000,
    updatedAt: 1000,
    status: 'received',
    attempts: 0,
    claimId: 'claim-secret',
    claimExpiresAt: 2000,
    requestContext: { metadata: { secret: 'request-context-secret' } },
    content: 'content-secret',
    attachments: [],
    lastError: { code: 'worker_unavailable', message: 'message-secret', retryable: true },
    ...overrides,
  };
}

function channelActionTokenRow(overrides: Partial<ChannelActionToken> = {}): ChannelActionToken {
  return {
    actionTokenId: 'action-token-1',
    harnessName: 'primary',
    channelId: 'support',
    providerId: 'slack',
    resourceId: 'resource-1',
    owningSessionId: 'session-1',
    itemId: 'question-1',
    kind: 'question',
    bindingId: 'support-binding',
    bindingGeneration: 1,
    runId: 'run-1',
    pendingRequestedAt: 1000,
    audience: { secret: 'audience-secret' },
    metadataHash: 'metadata-secret',
    transportHash: 'transport-secret',
    keyId: 'key-secret',
    expiresAt: 10_000,
    createdAt: 1000,
    updatedAt: 1000,
    ...overrides,
  };
}

function channelActionReceiptRow(overrides: Partial<ChannelActionReceipt> = {}): ChannelActionReceipt {
  return {
    id: 'receipt-1',
    harnessName: 'primary',
    channelId: 'support',
    providerId: 'slack',
    actionTokenId: 'action-token-1',
    actionId: 'provider-action-1',
    bindingId: 'support-binding',
    bindingGeneration: 1,
    resourceId: 'resource-1',
    owningSessionId: 'session-1',
    itemId: 'question-1',
    kind: 'question',
    runId: 'run-1',
    pendingRequestedAt: 1000,
    audience: { secret: 'audience-secret' },
    verifiedActor: { platformUserId: 'user-secret' },
    responseHash: 'response-secret',
    response: { secret: 'response-secret' },
    status: 'received',
    attempts: 1,
    claimId: 'receipt-claim-secret',
    claimExpiresAt: 3000,
    lastError: { code: 'delivery_operation_unavailable', message: 'receipt-error-secret', retryable: true },
    createdAt: 1000,
    updatedAt: 1000,
    ...overrides,
  };
}

function channelOutboxRow(overrides: Partial<ChannelOutboxItem> = {}): ChannelOutboxItem {
  return {
    id: 'outbox-1',
    harnessName: 'primary',
    channelId: 'support',
    providerId: 'slack',
    bindingId: 'support-binding',
    bindingGeneration: 1,
    idempotencyKey: 'outbox-key-1',
    payloadHash: 'payload-secret',
    resourceId: 'resource-1',
    threadId: 'thread-1',
    sessionId: 'session-1',
    owningSessionId: 'session-1',
    source: { kind: 'session-event', id: 'event-1', metadata: { secret: 'source-secret' } },
    target: {
      platform: 'slack',
      externalTenantId: 'tenant-secret',
      externalChannelId: 'channel-secret',
      externalThreadId: 'thread-secret',
    },
    kind: 'assistant-message',
    operationKind: 'message-create',
    payload: { secret: 'payload-secret' },
    deliverySemantics: 'native-idempotency',
    status: 'pending',
    attempts: 0,
    createdAt: 1000,
    updatedAt: 1000,
    ...overrides,
  };
}

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>(res => {
    resolve = res;
  });
  return { promise, resolve };
}

async function waitFor(predicate: () => boolean, label: string, timeoutMs = 500): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) {
      throw new Error(`timed out waiting for ${label}`);
    }
    await new Promise(resolve => setImmediate(resolve));
  }
}

class AbortIgnoringMockAgent extends MockAgent {
  override async stream(messages: any, options?: any): Promise<any> {
    return super.stream(messages, { ...options, abortSignal: undefined });
  }
}

class BlockingGenerateMockAgent extends MockAgent {
  override async generate(messages: any, options?: any): Promise<any> {
    this.streamCalls.push({ type: 'generate', messages, options });
    return new Promise(() => {});
  }
}

class RecordingStopWorker extends MastraWorker {
  readonly name: string;
  registerCalls = 0;
  stopCalls = 0;

  constructor(
    name: string,
    private readonly failure?: Error,
  ) {
    super();
    this.name = name;
  }

  override __registerMastra(mastra: Mastra): void {
    this.registerCalls += 1;
    super.__registerMastra(mastra);
  }

  async start(): Promise<void> {}

  async stop(): Promise<void> {
    this.stopCalls += 1;
    if (this.failure) throw this.failure;
  }

  get isRunning(): boolean {
    return true;
  }
}

class FailingCleanupPubSub extends PubSub {
  readonly unsubscribeCalls: string[] = [];
  flushCalls = 0;

  constructor(
    private readonly unsubscribeFailures: Record<string, unknown>,
    private readonly flushFailure?: unknown,
  ) {
    super();
  }

  override get supportedModes(): ReadonlyArray<'push'> {
    return ['push'];
  }

  async publish(_topic: string, _event: Omit<Event, 'id' | 'createdAt'>): Promise<void> {}

  async subscribe(_topic: string, _cb: EventCallback, _options?: SubscribeOptions): Promise<void> {}

  async unsubscribe(topic: string, _cb: EventCallback): Promise<void> {
    this.unsubscribeCalls.push(topic);
    if (Object.prototype.hasOwnProperty.call(this.unsubscribeFailures, topic)) {
      throw this.unsubscribeFailures[topic];
    }
  }

  async flush(): Promise<void> {
    this.flushCalls += 1;
    if (this.flushFailure) throw this.flushFailure;
  }
}

class DeferredSubscribePubSub extends PubSub {
  readonly publishCalls: string[] = [];
  readonly subscribeCalls: string[] = [];
  readonly unsubscribeCalls: string[] = [];

  private resolveFirstSubscribeStarted!: () => void;
  private resolveSubscribeGate!: () => void;
  private readonly firstSubscribeStarted = new Promise<void>(resolve => {
    this.resolveFirstSubscribeStarted = resolve;
  });
  private readonly subscribeGate = new Promise<void>(resolve => {
    this.resolveSubscribeGate = resolve;
  });

  override get supportedModes(): ReadonlyArray<'pull' | 'push'> {
    return ['pull', 'push'];
  }

  async publish(topic: string, _event: Omit<Event, 'id' | 'createdAt'>): Promise<void> {
    this.publishCalls.push(topic);
  }

  async subscribe(topic: string, _cb: EventCallback, _options?: SubscribeOptions): Promise<void> {
    this.subscribeCalls.push(topic);
    this.resolveFirstSubscribeStarted();
    await this.subscribeGate;
  }

  async unsubscribe(topic: string, _cb: EventCallback): Promise<void> {
    this.unsubscribeCalls.push(topic);
  }

  async flush(): Promise<void> {}

  waitForFirstSubscribe(): Promise<void> {
    return this.firstSubscribeStarted;
  }

  releaseSubscribes(): void {
    this.resolveSubscribeGate();
  }
}

function makeHarness(overrides?: Partial<ConstructorParameters<typeof Harness>[0]>) {
  const { sessions: overrideSessions, ...restOverrides } = overrides ?? {};
  const storage = overrideSessions?.storage ?? makeStorage();
  const compositeStorage = new InMemoryStore();
  compositeStorage.stores.harness = storage;
  const { storage: _sessionStorage, ...sessionOverrides } = overrideSessions ?? {};
  return new Harness({
    agents: { default: makeAgent() },
    storage: compositeStorage,
    modes: [{ id: 'default', agentId: 'default' }],
    defaultModeId: 'default',
    ...(Object.keys(sessionOverrides).length > 0 ? { sessions: sessionOverrides } : {}),
    ...restOverrides,
  });
}

describe('Harness v1 — construction', () => {
  it('accepts a valid config', () => {
    expect(() => makeHarness()).not.toThrow();
  });

  it('registers single-harness Mastra sugar under the default key', async () => {
    const storage = new InMemoryStore();
    const harness = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });

    const mastra = new Mastra({
      agents: { default: makeAgent() },
      storage,
      harness,
    });

    expect(mastra.getHarness()).toBe(harness);
    expect(mastra.getHarness('default')).toBe(harness);
    expect(mastra.getHarnesses()).toEqual({ default: harness });

    const session = await harness.session({ threadId: 'default-thread', resourceId: 'r1' });
    expect(session.getRecord().harnessName).toBe('default');
  });

  it('rejects duplicate default harness registration between sugar and explicit map', () => {
    const harness = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });
    const worker = new RecordingStopWorker('side-effect-check');
    const logger = {
      debug: vi.fn(),
      error: vi.fn(),
      info: vi.fn(),
      trackException: vi.fn(),
      warn: vi.fn(),
    };

    expect(
      () =>
        new Mastra({
          agents: { default: makeAgent() },
          storage: new InMemoryStore(),
          logger: logger as any,
          workers: [worker],
          harness,
          harnesses: { default: harness },
        }),
    ).toThrow(/config\.harnesses\.default/);
    expect(worker.registerCalls).toBe(0);
    expect(logger.trackException).toHaveBeenCalledTimes(1);
  });

  it('ignores nullish explicit default harness entries when using single-harness sugar', () => {
    const harness = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });

    const mastra = new Mastra({
      agents: { default: makeAgent() },
      storage: new InMemoryStore(),
      harness,
      harnesses: { default: undefined },
    });

    expect(mastra.getHarness()).toBe(harness);
  });

  it('shuts down registered harnesses during Mastra shutdown', async () => {
    const alpha = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });
    const beta = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });
    const alphaShutdown = vi.spyOn(alpha, 'shutdown');
    const betaShutdown = vi.spyOn(beta, 'shutdown');

    const mastra = new Mastra({
      agents: { default: makeAgent() },
      storage: new InMemoryStore(),
      harnesses: { alpha, beta },
    });

    await mastra.shutdown();

    expect(alphaShutdown).toHaveBeenCalledTimes(1);
    expect(betaShutdown).toHaveBeenCalledTimes(1);
  });

  it('logs harness shutdown failures, continues shutting down other harnesses, and throws the first failure', async () => {
    const failure = new Error('shutdown failed');
    const logger = {
      debug: vi.fn(),
      error: vi.fn(),
      info: vi.fn(),
      trackException: vi.fn(),
      warn: vi.fn(),
    };
    const alpha = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });
    const beta = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });
    const alphaShutdown = vi.spyOn(alpha, 'shutdown').mockRejectedValueOnce(failure);
    const betaShutdown = vi.spyOn(beta, 'shutdown');

    const mastra = new Mastra({
      agents: { default: makeAgent() },
      storage: new InMemoryStore(),
      logger: logger as any,
      harnesses: { alpha, beta },
    });

    await expect(mastra.shutdown()).rejects.toBe(failure);

    expect(alphaShutdown).toHaveBeenCalledTimes(1);
    expect(betaShutdown).toHaveBeenCalledTimes(1);
    expect(logger.error).toHaveBeenCalledWith('Failed to shutdown harness "alpha"', failure);
  });

  it('propagates falsy cleanup rejection reasons after best-effort shutdown', async () => {
    const logger = {
      debug: vi.fn(),
      error: vi.fn(),
      info: vi.fn(),
      trackException: vi.fn(),
      warn: vi.fn(),
    };
    const pubsub = new FailingCleanupPubSub({
      workflows: undefined,
    });

    const mastra = new Mastra({
      agents: { default: makeAgent() },
      storage: new InMemoryStore(),
      logger: logger as any,
      pubsub,
      workers: false,
    });

    await mastra.startWorkers();
    await expect(mastra.stopWorkers()).rejects.toBeUndefined();

    expect(pubsub.unsubscribeCalls).toEqual(['workflows']);
    expect(logger.error).toHaveBeenCalledWith('Failed to unsubscribe workflow push subscription', undefined);
  });

  it('keeps stopping workers and shuts down harnesses when one worker fails', async () => {
    const failure = new Error('worker stop failed');
    const logger = {
      debug: vi.fn(),
      error: vi.fn(),
      info: vi.fn(),
      trackException: vi.fn(),
      warn: vi.fn(),
    };
    const harness = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });
    const harnessShutdown = vi.spyOn(harness, 'shutdown');
    const first = new RecordingStopWorker('first');
    const failing = new RecordingStopWorker('failing', failure);

    const mastra = new Mastra({
      agents: { default: makeAgent() },
      storage: new InMemoryStore(),
      logger: logger as any,
      workers: [first, failing],
      harness,
    });

    await expect(mastra.shutdown()).rejects.toBe(failure);

    expect(failing.stopCalls).toBe(1);
    expect(first.stopCalls).toBe(1);
    expect(harnessShutdown).toHaveBeenCalledTimes(1);
    expect(logger.error).toHaveBeenCalledWith('Failed to stop worker "failing"', failure);
    expect(logger.error).toHaveBeenCalledWith('Failed to stop workers', failure);
  });

  it('does not mask worker stop failures when observability shutdown also fails', async () => {
    const workerFailure = new Error('worker stop failed');
    const observabilityFailure = new Error('observability shutdown failed');
    const logger = {
      debug: vi.fn(),
      error: vi.fn(),
      info: vi.fn(),
      trackException: vi.fn(),
      warn: vi.fn(),
    };
    const worker = new RecordingStopWorker('failing', workerFailure);
    const observability = {
      shutdown: vi.fn().mockRejectedValue(observabilityFailure),
      setMastraContext: vi.fn(),
      setLogger: vi.fn(),
      getSelectedInstance: vi.fn(),
      registerInstance: vi.fn(),
      getInstance: vi.fn(),
      getDefaultInstance: vi.fn(),
      listInstances: vi.fn(() => new Map()),
      unregisterInstance: vi.fn(),
      hasInstance: vi.fn(),
      setConfigSelector: vi.fn(),
      clear: vi.fn(),
    };

    const mastra = new Mastra({
      agents: { default: makeAgent() },
      storage: new InMemoryStore(),
      logger: logger as any,
      workers: [worker],
      observability: observability as any,
    });

    await expect(mastra.shutdown()).rejects.toBe(workerFailure);

    expect(worker.stopCalls).toBe(1);
    expect(observability.shutdown).toHaveBeenCalledTimes(1);
    expect(logger.error).toHaveBeenCalledWith('Failed to stop workers', workerFailure);
    expect(logger.error).toHaveBeenCalledWith('Failed to shutdown observability', observabilityFailure);
  });

  it('shuts down Mastra-owned background task manager and still shuts down harnesses after failure', async () => {
    const backgroundFailure = new Error('background task shutdown failed');
    const logger = {
      debug: vi.fn(),
      error: vi.fn(),
      info: vi.fn(),
      trackException: vi.fn(),
      warn: vi.fn(),
    };
    const harness = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });
    const harnessShutdown = vi.spyOn(harness, 'shutdown');

    const mastra = new Mastra({
      agents: { default: makeAgent() },
      storage: new InMemoryStore(),
      logger: logger as any,
      backgroundTasks: { enabled: true },
      workers: [],
      harness,
    });
    const backgroundTaskManager = mastra.backgroundTaskManager!;
    const backgroundShutdown = vi.spyOn(backgroundTaskManager, 'shutdown').mockRejectedValueOnce(backgroundFailure);

    await expect(mastra.shutdown()).rejects.toBe(backgroundFailure);

    expect(backgroundShutdown).toHaveBeenCalledTimes(1);
    expect(harnessShutdown).toHaveBeenCalledTimes(1);
    expect(logger.error).toHaveBeenCalledWith('Failed to shutdown background task manager', backgroundFailure);

    backgroundShutdown.mockRestore();
    await backgroundTaskManager.shutdown();
  });

  it('waits for Mastra-owned background task init before shutdown cleanup', async () => {
    const pubsub = new DeferredSubscribePubSub();
    const storage = new InMemoryStore();
    const bgStore = await storage.getStore('backgroundTasks');
    await bgStore!.createTask({
      id: 'pending-during-shutdown',
      status: 'pending',
      toolName: 'test-tool',
      toolCallId: 'tool-call-1',
      args: {},
      agentId: 'default',
      runId: 'run-1',
      retryCount: 0,
      maxRetries: 0,
      timeoutMs: 5000,
      createdAt: new Date(),
    });
    const mastra = new Mastra({
      agents: { default: makeAgent() },
      storage,
      logger: false,
      pubsub,
      backgroundTasks: { enabled: true },
      workers: [],
    });

    await pubsub.waitForFirstSubscribe();
    const shutdown = mastra.shutdown();
    await Promise.resolve();

    expect(pubsub.unsubscribeCalls).toEqual([]);

    pubsub.releaseSubscribes();
    await shutdown;

    expect(pubsub.subscribeCalls).toEqual(['background-tasks', 'background-tasks-result']);
    expect(pubsub.unsubscribeCalls).toEqual(['background-tasks', 'background-tasks-result']);
    expect(pubsub.publishCalls).toEqual([]);
  });

  it('keeps cleanup subscriptions that fail during worker stop', async () => {
    const pushFailure = new Error('push unsubscribe failed');
    const eventFailure = new Error('event unsubscribe failed');
    const flushFailure = new Error('flush failed');
    const logger = {
      debug: vi.fn(),
      error: vi.fn(),
      info: vi.fn(),
      trackException: vi.fn(),
      warn: vi.fn(),
    };
    const pubsub = new FailingCleanupPubSub(
      {
        workflows: pushFailure,
        updates: eventFailure,
      },
      flushFailure,
    );

    const mastra = new Mastra({
      agents: { default: makeAgent() },
      storage: new InMemoryStore(),
      logger: logger as any,
      pubsub,
      events: {
        updates: async () => {},
      },
      workers: false,
    });

    await mastra.startWorkers();
    await expect(mastra.stopWorkers()).rejects.toBe(pushFailure);

    expect(pubsub.unsubscribeCalls).toEqual(['workflows', 'updates']);
    expect(pubsub.flushCalls).toBe(1);
    expect(logger.error).toHaveBeenCalledWith('Failed to unsubscribe workflow push subscription', pushFailure);
    expect(logger.error).toHaveBeenCalledWith('Failed to unsubscribe event listener for topic "updates"', eventFailure);
    expect(logger.error).toHaveBeenCalledWith('Failed to flush pubsub during worker shutdown', flushFailure);

    await expect(mastra.stopWorkers()).rejects.toBe(pushFailure);

    expect(pubsub.unsubscribeCalls).toEqual(['workflows', 'updates', 'workflows', 'updates']);
    expect(pubsub.flushCalls).toBe(2);
  });

  it('registers harness channel bindings with stable durable identity', () => {
    const harness = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      channels: {
        support: makeHarnessChannelConfig({
          bindingId: 'support-binding',
          callbackTarget: 'slack-support-webhook',
        }),
      },
    });
    new Mastra({
      agents: { default: makeAgent() },
      storage: new InMemoryStore(),
      channels: { slack: makeChannelProvider('slack') },
      harnesses: { primary: harness },
    });

    expect(harness.listChannelBindings()).toEqual([
      {
        harnessName: 'primary',
        channelId: 'support',
        bindingId: 'support-binding',
        providerId: 'slack',
        platform: 'slack',
        callbackTarget: 'slack-support-webhook',
        durableId: 'primary:support:support-binding',
      },
    ]);
    expect(harness.getChannelBinding('support')).toMatchObject({
      harnessName: 'primary',
      channelId: 'support',
      providerId: 'slack',
    });
    expect(harness.getChannelBinding('missing')).toBeUndefined();
  });

  it('does not validate harness names for durable channel ids when no channels are configured', () => {
    expect(
      () =>
        new Mastra({
          agents: { default: makeAgent() },
          storage: new InMemoryStore(),
          harnesses: {
            'legacy.name:ok-without-channels': new Harness({
              modes: [{ id: 'default', agentId: 'default' }],
              defaultModeId: 'default',
            }),
          },
        }),
    ).not.toThrow();
  });

  it('rejects harness channel bindings that reference a missing provider', () => {
    const harness = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      channels: {
        support: makeHarnessChannelConfig({ providerId: 'missing', platform: 'missing' }),
      },
    });

    expect(
      () =>
        new Mastra({
          agents: { default: makeAgent() },
          storage: new InMemoryStore(),
          channels: { slack: makeChannelProvider('slack') },
          harnesses: { primary: harness },
        }),
    ).toThrow(HarnessConfigError);
  });

  it('rolls back Mastra binding when harness channel provider validation fails', () => {
    const harness = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      channels: {
        support: makeHarnessChannelConfig({ providerId: 'missing', platform: 'missing' }),
      },
    });

    expect(
      () =>
        new Mastra({
          agents: { default: makeAgent() },
          storage: new InMemoryStore(),
          channels: { slack: makeChannelProvider('slack') },
          harnesses: { primary: harness },
        }),
    ).toThrow(HarnessConfigError);

    expect(
      () =>
        new Mastra({
          agents: { default: makeAgent() },
          storage: new InMemoryStore(),
          channels: { missing: makeChannelProvider('missing') },
          harnesses: { primary: harness },
        }),
    ).not.toThrow();
    expect(harness.getChannelBinding('support')).toMatchObject({
      providerId: 'missing',
      durableId: 'primary:support:support',
    });
  });

  it('rejects duplicate harness channel binding ids', () => {
    expect(
      () =>
        new Harness({
          agents: { default: makeAgent() },
          storage: new InMemoryStore(),
          modes: [{ id: 'default', agentId: 'default' }],
          defaultModeId: 'default',
          channels: {
            support: makeHarnessChannelConfig({ bindingId: 'shared-binding' }),
            alerts: makeHarnessChannelConfig({ bindingId: 'shared-binding' }),
          },
        }),
    ).toThrow(HarnessConfigError);
  });

  it('rejects harness channel ids or binding ids that cannot form stable durable ids', () => {
    expect(
      () =>
        new Harness({
          agents: { default: makeAgent() },
          storage: new InMemoryStore(),
          modes: [{ id: 'default', agentId: 'default' }],
          defaultModeId: 'default',
          channels: {
            'support:slack': makeHarnessChannelConfig(),
          },
        }),
    ).toThrow(HarnessConfigError);

    expect(
      () =>
        new Harness({
          agents: { default: makeAgent() },
          storage: new InMemoryStore(),
          modes: [{ id: 'default', agentId: 'default' }],
          defaultModeId: 'default',
          channels: {
            support: makeHarnessChannelConfig({ bindingId: 'support:binding' }),
          },
        }),
    ).toThrow(HarnessConfigError);

    const harness = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      channels: {
        support: makeHarnessChannelConfig(),
      },
    });
    expect(
      () =>
        new Mastra({
          agents: { default: makeAgent() },
          storage: new InMemoryStore(),
          channels: { slack: makeChannelProvider('slack') },
          harnesses: { 'primary:bad': harness },
        }),
    ).toThrow(HarnessConfigError);
  });

  it('rejects harness channel adapters without a delivery function', () => {
    expect(
      () =>
        new Harness({
          agents: { default: makeAgent() },
          storage: new InMemoryStore(),
          modes: [{ id: 'default', agentId: 'default' }],
          defaultModeId: 'default',
          channels: {
            support: makeHarnessChannelConfig({ adapter: {} as any }),
          },
        }),
    ).toThrow(HarnessConfigError);
  });

  it('rejects harness channel callback targets that cannot be routed', () => {
    expect(
      () =>
        new Harness({
          agents: { default: makeAgent() },
          storage: new InMemoryStore(),
          modes: [{ id: 'default', agentId: 'default' }],
          defaultModeId: 'default',
          channels: {
            support: makeHarnessChannelConfig({ callbackTarget: '' }),
          },
        }),
    ).toThrow(HarnessConfigError);
  });

  it('rejects inline agents with harness channels because no channel providers are available', () => {
    expect(
      () =>
        new Harness({
          agents: { default: makeAgent() },
          storage: new InMemoryStore(),
          modes: [{ id: 'default', agentId: 'default' }],
          defaultModeId: 'default',
          channels: {
            support: makeHarnessChannelConfig(),
          },
        }),
    ).toThrow(/channel bindings require a Mastra with channel providers/);
  });

  it('rejects harness channel platform mismatches with the registered provider', () => {
    const harness = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      channels: {
        support: makeHarnessChannelConfig({ platform: 'discord' }),
      },
    });

    expect(
      () =>
        new Mastra({
          agents: { default: makeAgent() },
          storage: new InMemoryStore(),
          channels: { slack: makeChannelProvider('slack') },
          harnesses: { primary: harness },
        }),
    ).toThrow(HarnessConfigError);
  });

  it('coexists with existing AgentChannels route registration', () => {
    const agent = makeAgent();
    const harness = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      channels: {
        support: makeHarnessChannelConfig(),
      },
    });
    const mastra = new Mastra({
      agents: { default: agent },
      storage: new InMemoryStore(),
      channels: { slack: makeChannelProvider('slack') },
      harnesses: { primary: harness },
    });

    expect(mastra.getChannels()).toEqual({});
    expect(mastra.getChannelProvider('slack')).toBeDefined();
    expect(harness.listChannelBindings()).toHaveLength(1);
  });

  it('rejects AgentChannels tools on a harness-bound provider platform', () => {
    const harness = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      channels: {
        support: makeHarnessChannelConfig(),
      },
    });

    expect(
      () =>
        new Mastra({
          agents: {
            default: new Agent({
              id: 'default',
              name: 'default',
              instructions: 'test',
              model: TEST_MODEL,
              channels: { adapters: { slack: {} as any } },
            }),
          },
          storage: new InMemoryStore(),
          channels: { slack: makeChannelProvider('slack') },
          harnesses: { primary: harness },
        }),
    ).toThrow(/AgentChannels tools enabled/);

    expect(
      () =>
        new Mastra({
          agents: {
            default: new Agent({
              id: 'default',
              name: 'default',
              instructions: 'test',
              model: TEST_MODEL,
              channels: { adapters: { slack: {} as any }, tools: false },
            }),
          },
          storage: new InMemoryStore(),
          channels: { slack: makeChannelProvider('slack') },
          harnesses: { primary: harness },
        }),
    ).not.toThrow();
  });

  it('rejects durable-agent AgentChannels tools on a harness-bound provider platform', () => {
    const underlyingAgent = new Agent({
      id: 'default',
      name: 'default',
      instructions: 'test',
      model: TEST_MODEL,
      channels: { adapters: { slack: {} as any } },
    });
    const durableAgent = {
      id: 'default',
      name: 'default',
      agent: underlyingAgent,
      stream: async () => ({}) as any,
    };
    const harness = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      channels: {
        support: makeHarnessChannelConfig(),
      },
    });

    expect(
      () =>
        new Mastra({
          agents: { default: durableAgent as any },
          storage: new InMemoryStore(),
          channels: { slack: makeChannelProvider('slack') },
          harnesses: { primary: harness },
        }),
    ).toThrow(/AgentChannels tools enabled/);
  });

  it('rejects rebuilt durable-agent AgentChannels using the harness registry key', () => {
    const underlyingAgent = makeAgent('underlying');
    const durableAgent = {
      id: 'durable-default',
      name: 'durable-default',
      agent: underlyingAgent,
      stream: async () => ({}) as any,
      __setLogger: () => {},
    };
    const harness = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      channels: {
        support: makeHarnessChannelConfig(),
      },
    });
    new Mastra({
      agents: { default: durableAgent as any },
      storage: new InMemoryStore(),
      channels: { slack: makeChannelProvider('slack') },
      harnesses: { primary: harness },
    });

    expect(() => underlyingAgent.setChannels(new AgentChannels({ adapters: { slack: {} as any } }))).toThrow(
      /AgentChannels tools enabled/,
    );
    expect(underlyingAgent.getChannels()).toBeNull();
  });

  it('allows AgentChannels tools on live-only platforms outside harness bindings', () => {
    const harness = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      channels: {
        support: makeHarnessChannelConfig(),
      },
    });

    expect(
      () =>
        new Mastra({
          agents: {
            default: new Agent({
              id: 'default',
              name: 'default',
              instructions: 'test',
              model: TEST_MODEL,
              channels: { adapters: { discord: {} as any } },
            }),
          },
          storage: new InMemoryStore(),
          channels: { slack: makeChannelProvider('slack') },
          harnesses: { primary: harness },
        }),
    ).not.toThrow();
  });

  it('allows AgentChannels tools on same-platform agents not used by the harness', () => {
    const harness = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      channels: {
        support: makeHarnessChannelConfig(),
      },
    });

    expect(
      () =>
        new Mastra({
          agents: {
            default: makeAgent('default'),
            live: new Agent({
              id: 'live',
              name: 'live',
              instructions: 'test',
              model: TEST_MODEL,
              channels: { adapters: { slack: {} as any } },
            }),
          },
          storage: new InMemoryStore(),
          channels: { slack: makeChannelProvider('slack') },
          harnesses: { primary: harness },
        }),
    ).not.toThrow();
  });

  it('allows AgentChannels tools when only another harness binds that platform', () => {
    const liveAgent = new Agent({
      id: 'live',
      name: 'live',
      instructions: 'test',
      model: TEST_MODEL,
      channels: { adapters: { slack: {} as any } },
    });
    const liveHarness = new Harness({
      modes: [{ id: 'live', agentId: 'live' }],
      defaultModeId: 'live',
    });
    const channelHarness = new Harness({
      modes: [{ id: 'worker', agentId: 'worker' }],
      defaultModeId: 'worker',
      channels: {
        support: makeHarnessChannelConfig(),
      },
    });

    expect(
      () =>
        new Mastra({
          agents: {
            live: liveAgent,
            worker: makeAgent('worker'),
          },
          storage: new InMemoryStore(),
          channels: { slack: makeChannelProvider('slack') },
          harnesses: {
            live: liveHarness,
            channel: channelHarness,
          },
        }),
    ).not.toThrow();
  });

  it('allows harness-bound AgentChannels when generic channel tools are disabled', () => {
    const harness = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      channels: {
        support: makeHarnessChannelConfig(),
      },
    });

    expect(
      () =>
        new Mastra({
          agents: {
            default: new Agent({
              id: 'default',
              name: 'default',
              instructions: 'test',
              model: TEST_MODEL,
              channels: new AgentChannels({ adapters: { slack: {} as any }, tools: false }),
            }),
          },
          storage: new InMemoryStore(),
          channels: { slack: makeChannelProvider('slack') },
          harnesses: { primary: harness },
        }),
    ).not.toThrow();
  });

  it('allows dynamically added AgentChannels tools for agents not used by the harness', () => {
    const harness = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      channels: {
        support: makeHarnessChannelConfig(),
      },
    });
    const mastra = new Mastra({
      agents: { default: makeAgent() },
      storage: new InMemoryStore(),
      channels: { slack: makeChannelProvider('slack') },
      harnesses: { primary: harness },
    });

    expect(() =>
      mastra.addAgent(
        new Agent({
          id: 'live',
          name: 'live',
          instructions: 'test',
          model: TEST_MODEL,
          channels: { adapters: { slack: {} as any } },
        }),
      ),
    ).not.toThrow();
    expect(mastra.getAgent('live' as never)).toBeDefined();
  });

  it('rejects provider-added AgentChannels adapters that would expose tools on a harness-bound platform', () => {
    const agent = new Agent({
      id: 'default',
      name: 'default',
      instructions: 'test',
      model: TEST_MODEL,
      channels: new AgentChannels({ adapters: { discord: {} as any } }),
    });
    const harness = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      channels: {
        support: makeHarnessChannelConfig(),
      },
    });
    new Mastra({
      agents: { default: agent },
      storage: new InMemoryStore(),
      channels: { slack: makeChannelProvider('slack') },
      harnesses: { primary: harness },
    });
    const agentChannels = agent.getChannels();

    expect(agentChannels).toBeInstanceOf(AgentChannels);
    expect(() => agentChannels?.__registerAdapter('slack', {} as any)).toThrow(/AgentChannels tools enabled/);
    expect(agentChannels?.hasAdapter('slack')).toBe(false);
  });

  it('rejects replacing a harness-bound agent with tool-enabled AgentChannels for that platform', () => {
    const agent = makeAgent('default');
    const harness = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      channels: {
        support: makeHarnessChannelConfig(),
      },
    });
    new Mastra({
      agents: { default: agent },
      storage: new InMemoryStore(),
      channels: { slack: makeChannelProvider('slack') },
      harnesses: { primary: harness },
    });

    expect(() => agent.setChannels(new AgentChannels({ adapters: { slack: {} as any } }))).toThrow(
      /AgentChannels tools enabled/,
    );
    expect(agent.getChannels()).toBeNull();
  });

  it('rejects direct-bound Harness AgentChannels tools on a harness-bound provider platform', () => {
    const mastra = new Mastra({
      agents: {
        default: new Agent({
          id: 'default',
          name: 'default',
          instructions: 'test',
          model: TEST_MODEL,
          channels: { adapters: { slack: {} as any } },
        }),
      },
      storage: new InMemoryStore(),
      channels: { slack: makeChannelProvider('slack') },
    });

    expect(
      () =>
        new Harness({
          mastra,
          modes: [{ id: 'default', agentId: 'default' }],
          defaultModeId: 'default',
          channels: {
            support: makeHarnessChannelConfig(),
          },
        }),
    ).toThrow(/AgentChannels tools enabled/);
  });

  it('rejects replacing AgentChannels after a direct-bound Harness claims the platform', () => {
    const agent = makeAgent('default');
    const mastra = new Mastra({
      agents: { default: agent },
      storage: new InMemoryStore(),
      channels: { slack: makeChannelProvider('slack') },
    });
    new Harness({
      mastra,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      channels: {
        support: makeHarnessChannelConfig(),
      },
    });

    expect(() => agent.setChannels(new AgentChannels({ adapters: { slack: {} as any } }))).toThrow(
      /AgentChannels tools enabled/,
    );
    expect(agent.getChannels()).toBeNull();
  });

  it('rejects a direct-bound default Harness when Mastra already has one', () => {
    const existingHarness = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });
    const mastra = new Mastra({
      agents: { default: makeAgent('default') },
      storage: new InMemoryStore(),
      harnesses: { default: existingHarness },
    });

    expect(
      () =>
        new Harness({
          mastra,
          modes: [{ id: 'other', agentId: 'default' }],
          defaultModeId: 'other',
        }),
    ).toThrow(/already registered/);
    expect(mastra.getHarness()).toBe(existingHarness);
  });

  it('adds the wakeup worker when a direct-bound Harness registers after Mastra construction', () => {
    const mastra = new Mastra({
      agents: { default: makeAgent('default') },
      storage: new InMemoryStore(),
    });
    expect(mastra.workers.some(worker => worker.name === 'harnessWakeups')).toBe(false);

    new Harness({
      mastra,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });

    expect(mastra.workers.some(worker => worker.name === 'harnessWakeups')).toBe(true);
  });

  it('rejects AgentChannels registration after an agent is removed', () => {
    const agent = makeAgent('default');
    const mastra = new Mastra({
      agents: { default: agent },
      storage: new InMemoryStore(),
    });

    expect(mastra.removeAgent('default')).toBe(true);
    expect(() => agent.setChannels(new AgentChannels({ adapters: { slack: {} as any } }))).toThrow(/removed/);
    expect(
      mastra.getServer()?.apiRoutes?.some(route => route.path === '/api/agents/default/channels/slack/webhook') ??
        false,
    ).toBe(false);
  });

  it('replaces stale webhook routes when AgentChannels are rebuilt', () => {
    const agent = new Agent({
      id: 'default',
      name: 'default',
      instructions: 'test',
      model: TEST_MODEL,
      channels: { adapters: { slack: {} as any }, tools: false },
    });
    const mastra = new Mastra({
      agents: { default: agent },
      storage: new InMemoryStore(),
      channels: { slack: makeChannelProvider('slack') },
    });
    const routePath = '/api/agents/default/channels/slack/webhook';
    const initialRoutes = mastra.getServer()?.apiRoutes?.filter(route => route.path === routePath) ?? [];
    const replacement = new AgentChannels({ adapters: { slack: {} as any }, tools: false });

    agent.setChannels(replacement);

    const routes = mastra.getServer()?.apiRoutes?.filter(route => route.path === routePath) ?? [];
    expect(initialRoutes).toHaveLength(1);
    expect(routes).toHaveLength(1);
    expect(routes[0]).not.toBe(initialRoutes[0]);
  });

  it('enqueues durable channel outbox rows before dispatching through the adapter', async () => {
    const storage = makeStorage();
    const delivered: string[] = [];
    const harness = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage },
      channels: {
        support: makeHarnessChannelConfig({
          bindingId: 'support-binding',
          adapter: {
            deliverySemantics: 'native-idempotency',
            deliver: async item => {
              delivered.push(item.idempotencyKey);
              return { providerMessageId: 'provider-message-1', providerReceipt: { deliveryId: 'delivery-1' } };
            },
          },
        }),
      },
    });
    new Mastra({
      agents: { default: makeAgent() },
      storage: new InMemoryStore(),
      channels: { slack: makeChannelProvider('slack') },
      harnesses: { primary: harness },
    });

    // §10.2 channel_outbox_* events.
    const channelEvents: Array<{
      type: string;
      outboxItemId?: string;
      bindingId?: string;
      kind?: string;
      providerMessageId?: string;
    }> = [];
    harness.subscribe(e => {
      if (e.type.startsWith('channel_')) channelEvents.push(e as never);
    });

    await expect(harness.channels.enqueueOutbox(channelOutboxInput())).resolves.toMatchObject({
      duplicate: false,
      conflict: false,
    });
    await expect(harness.channels.enqueueOutbox(channelOutboxInput())).resolves.toMatchObject({
      duplicate: true,
      conflict: false,
    });
    await expect(
      harness.channels.enqueueOutbox(channelOutboxInput({ payload: { text: 'different' } })),
    ).resolves.toMatchObject({
      duplicate: true,
      conflict: true,
    });
    await expect(
      harness.channels.enqueueOutbox(
        channelOutboxInput({
          idempotencyKey: 'outbox-key-wrong-platform',
          target: {
            platform: 'discord',
            externalTenantId: 'tenant-1',
            externalChannelId: 'channel-1',
            externalThreadId: 'thread-ext-1',
          },
        }),
      ),
    ).rejects.toBeInstanceOf(HarnessValidationError);

    await expect(harness.channels.dispatchOutbox({ channelId: 'support', claimId: 'claim-1' })).resolves.toEqual({
      claimed: 1,
      sent: 1,
      failed: 0,
      dead: 0,
      items: [{ outboxItemId: expect.any(String), status: 'sent', providerMessageId: 'provider-message-1' }],
    });
    expect(delivered).toEqual(['outbox-key-1']);

    // Only the fresh enqueue emits channel_outbox_enqueued (duplicate/conflict do
    // not); the successful dispatch emits channel_outbox_sent after the durable
    // sent transition commits.
    expect(channelEvents.map(e => e.type)).toEqual(['channel_outbox_enqueued', 'channel_outbox_sent']);
    expect(channelEvents[0]).toMatchObject({ bindingId: 'support-binding', outboxItemId: expect.any(String) });
    expect(channelEvents[1]).toMatchObject({ bindingId: 'support-binding', providerMessageId: 'provider-message-1' });
  });

  it('uses canonical payload hashes for channel outbox idempotency', async () => {
    const storage = makeStorage();
    const harness = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage },
      channels: {
        support: makeHarnessChannelConfig({ bindingId: 'support-binding' }),
      },
    });
    new Mastra({
      agents: { default: makeAgent() },
      storage: new InMemoryStore(),
      channels: { slack: makeChannelProvider('slack') },
      harnesses: { primary: harness },
    });

    await expect(
      harness.channels.enqueueOutbox(
        channelOutboxInput({
          payload: { b: 2, a: 1 },
        }),
      ),
    ).resolves.toMatchObject({ duplicate: false, conflict: false });
    await expect(
      harness.channels.enqueueOutbox(
        channelOutboxInput({
          idempotencyKey: 'outbox-key-1',
          payload: { a: 1, b: 2 },
        }),
      ),
    ).resolves.toMatchObject({ duplicate: true, conflict: false });
    await expect(
      harness.channels.enqueueOutbox(
        channelOutboxInput({
          idempotencyKey: 'outbox-key-1',
          payload: { a: 1, b: 2, c: null },
        }),
      ),
    ).resolves.toMatchObject({ duplicate: true, conflict: true });
  });

  it('returns session-scoped channel diagnostics without leaking provider payloads', async () => {
    const storage = makeStorage();
    const harness = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage },
      channels: {
        support: makeHarnessChannelConfig({ bindingId: 'support-binding' }),
      },
    });
    new Mastra({
      agents: { default: makeAgent() },
      storage: new InMemoryStore(),
      channels: { slack: makeChannelProvider('slack') },
      harnesses: { primary: harness },
    });
    // This test persists RAW inbox rows that are claimable at wall-clock time, so
    // stop the §14.2 recovery worker the bind auto-started — otherwise a background
    // tick crossing the poll interval could mutate a row before the assertions.
    // (Channel tests that save raw claimable inbox rows must stop the loop; tests
    // that only go through admitChannelInbound get live real-time claims and the
    // main C4 setup() already stops it.)
    (harness as unknown as { _stopChannelInboxRecoveryLoop: () => void })._stopChannelInboxRecoveryLoop();
    const root = await harness.session({ sessionId: 'session-1', resourceId: 'resource-1', threadId: { fresh: true } });
    const child = await harness.session({
      sessionId: 'child-1',
      resourceId: 'resource-1',
      parentSessionId: root.id,
      threadId: { fresh: true },
    });

    await storage.saveChannelInboxItem(channelInboxRow());
    await storage.createOrLoadChannelActionToken(
      channelActionTokenRow({ owningSessionId: child.id, expiresAt: Date.now() + 60_000 }),
    );
    await storage.createOrLoadChannelActionReceipt(channelActionReceiptRow({ owningSessionId: child.id }));
    await storage.enqueueChannelOutbox(channelOutboxRow());
    await storage.saveChannelInboxItem(
      channelInboxRow({
        id: 'foreign-inbox',
        idempotencyKey: 'foreign-event',
        admissionId: 'foreign-admission',
        resourceId: 'resource-2',
        sessionId: 'foreign-session',
      }),
    );

    const listSpy = vi.spyOn(storage, 'listChannelDiagnosticsRows');
    const diagnostics = await harness.getChannelDiagnostics({
      sessionId: root.id,
      resourceId: 'resource-1',
      limit: 10,
    });

    expect(diagnostics).toMatchObject({
      sessionId: root.id,
      resourceId: 'resource-1',
      visibleSessionIds: expect.arrayContaining([root.id, child.id]),
      bindings: [{ channelId: 'support', providerId: 'slack', bindingId: 'support-binding' }],
      // §13.3f.1: bare row codes are projected to namespaced wire codes.
      inbox: [
        { id: 'inbox-1', status: 'received', lastError: { code: 'harness.worker_unavailable', retryable: true } },
      ],
      actionTokens: [{ actionTokenId: 'action-token-1', owningSessionId: child.id, status: 'active' }],
      actionReceipts: [
        {
          id: 'receipt-1',
          owningSessionId: child.id,
          lastError: { code: 'harness.channel_delivery_unavailable', reason: 'delivery_operation_unavailable' },
        },
      ],
      outbox: [{ id: 'outbox-1', source: { kind: 'session-event', id: 'event-1' } }],
      redacted: true,
      truncated: false,
    });
    expect(listSpy).toHaveBeenCalledWith(
      expect.objectContaining({ harnessName: 'primary', resourceId: 'resource-1', sessionIds: [root.id, child.id] }),
    );
    listSpy.mockClear();
    await harness.getChannelDiagnostics({ sessionId: root.id, resourceId: 'resource-1', limit: 1000 });
    expect(listSpy).toHaveBeenCalledWith(expect.objectContaining({ limit: 51 }));
    const wire = JSON.stringify(diagnostics);
    expect(wire).not.toContain('secret');
    expect(wire).not.toContain('foreign-inbox');
    expect(wire).not.toContain('claim-secret');
    await expect(
      storage.claimChannelOutbox({
        harnessName: 'primary',
        channelId: 'support',
        claimId: 'post-diagnostics',
        limit: 10,
        now: Date.now(),
        claimTtlMs: 1000,
      }),
    ).resolves.toEqual([expect.objectContaining({ id: 'outbox-1', claimId: 'post-diagnostics' })]);
  });

  it('marks channel diagnostics truncated only when visible sessions exceed the cap', async () => {
    const storage = makeStorage();
    const harness = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage },
    });
    new Mastra({
      agents: { default: makeAgent() },
      storage: new InMemoryStore(),
      harnesses: { primary: harness },
    });
    const root = await harness.session({ sessionId: 'session-1', resourceId: 'resource-1', threadId: { fresh: true } });
    for (let index = 0; index < 255; index += 1) {
      await harness.session({
        sessionId: `child-${index}`,
        resourceId: 'resource-1',
        parentSessionId: root.id,
        threadId: { fresh: true },
      });
    }

    await expect(
      harness.getChannelDiagnostics({ sessionId: root.id, resourceId: 'resource-1' }),
    ).resolves.toMatchObject({
      visibleSessionIds: expect.arrayContaining([root.id, 'child-254']),
      truncated: false,
    });

    await harness.session({
      sessionId: 'child-over-cap',
      resourceId: 'resource-1',
      parentSessionId: root.id,
      threadId: { fresh: true },
    });

    const diagnostics = await harness.getChannelDiagnostics({ sessionId: root.id, resourceId: 'resource-1' });
    expect(diagnostics?.visibleSessionIds).toHaveLength(256);
    expect(diagnostics?.truncated).toBe(true);
  });

  it('uses lookup reconciliation before retrying lookup-reconcile outbox rows', async () => {
    const storage = makeStorage();
    let deliverCalls = 0;
    const harness = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage },
      channels: {
        support: makeHarnessChannelConfig({
          outbox: { retryBackoffMs: () => 0, maxAttempts: 2 },
          adapter: {
            deliverySemantics: 'lookup-reconcile',
            reconcileDelivery: async () => ({
              delivered: true,
              providerMessageId: 'provider-message-recovered',
              providerReceipt: { deliveryId: 'delivery-recovered' },
            }),
            deliver: async () => {
              deliverCalls += 1;
              throw new Error('delivery outcome unknown');
            },
          },
        }),
      },
    });
    new Mastra({
      agents: { default: makeAgent() },
      storage: new InMemoryStore(),
      channels: { slack: makeChannelProvider('slack') },
      harnesses: { primary: harness },
    });

    await harness.channels.enqueueOutbox(channelOutboxInput({ deliverySemantics: 'lookup-reconcile' }));

    await expect(harness.channels.dispatchOutbox({ channelId: 'support', claimId: 'claim-lookup-1' })).resolves.toEqual(
      {
        claimed: 1,
        sent: 0,
        failed: 1,
        dead: 0,
        items: [
          {
            outboxItemId: expect.any(String),
            status: 'failed',
            error: { code: 'unknown', message: 'delivery outcome unknown' },
          },
        ],
      },
    );
    await expect(harness.channels.dispatchOutbox({ channelId: 'support', claimId: 'claim-lookup-2' })).resolves.toEqual(
      {
        claimed: 1,
        sent: 1,
        failed: 0,
        dead: 0,
        items: [{ outboxItemId: expect.any(String), status: 'sent', providerMessageId: 'provider-message-recovered' }],
      },
    );
    expect(deliverCalls).toBe(1);
  });

  it('renews the outbox claim while provider delivery is in flight', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(10_000);
    try {
      const storage = makeStorage();
      let deliverStarted = false;
      let resolveDelivery!: () => void;
      const delivery = new Promise<{ providerMessageId: string }>(resolve => {
        resolveDelivery = () => resolve({ providerMessageId: 'provider-slow' });
      });
      const harness = new Harness({
        modes: [{ id: 'default', agentId: 'default' }],
        defaultModeId: 'default',
        sessions: { storage },
        channels: {
          support: makeHarnessChannelConfig({
            outbox: { claimRenewMs: 25 },
            adapter: {
              deliver: async () => {
                deliverStarted = true;
                return delivery;
              },
            },
          }),
        },
      });
      new Mastra({
        agents: { default: makeAgent() },
        storage: new InMemoryStore(),
        channels: { slack: makeChannelProvider('slack') },
        harnesses: { primary: harness },
      });

      await harness.channels.enqueueOutbox(channelOutboxInput());
      const firstDispatch = harness.channels.dispatchOutbox({
        channelId: 'support',
        claimId: 'claim-slow-1',
        claimTtlMs: 100,
      });
      for (let i = 0; i < 5 && !deliverStarted; i += 1) {
        await Promise.resolve();
      }
      expect(deliverStarted).toBe(true);
      await vi.advanceTimersByTimeAsync(250);

      await expect(
        harness.channels.dispatchOutbox({ channelId: 'support', claimId: 'claim-slow-2', claimTtlMs: 100 }),
      ).resolves.toMatchObject({ claimed: 0, sent: 0, failed: 0, dead: 0 });

      resolveDelivery();
      await expect(firstDispatch).resolves.toMatchObject({ claimed: 1, sent: 1, failed: 0, dead: 0 });
    } finally {
      vi.useRealTimers();
    }
  });

  it('does not call the outbox provider or mutate after pre-delivery claim ownership is lost', async () => {
    const db = new InMemoryDB();
    const storage = new InMemoryHarness({ db });
    const deliver = vi.fn(async () => ({ providerMessageId: 'provider-message-1' }));
    const harness = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage },
      channels: {
        support: makeHarnessChannelConfig({
          adapter: { deliver },
        }),
      },
    });
    new Mastra({
      agents: { default: makeAgent() },
      storage: new InMemoryStore(),
      channels: { slack: makeChannelProvider('slack') },
      harnesses: { primary: harness },
    });
    const enqueued = await harness.channels.enqueueOutbox(channelOutboxInput());
    const outboxItemId = enqueued.outboxItemId;
    const dispatchNow = Date.now();
    const claimTtlMs = 1_000;
    const competingClaimId = 'claim-competing';
    const competingClaimExpiresAt = dispatchNow + claimTtlMs * 5;
    const renewSpy = vi.spyOn(storage, 'renewChannelOutboxClaim').mockImplementationOnce(async input => {
      const claimed = db.harnessChannelOutbox.get(outboxItemId);
      expect(claimed).toMatchObject({ status: 'claimed', claimId: 'claim-lost' });
      db.harnessChannelOutbox.set(outboxItemId, {
        ...claimed!,
        claimId: competingClaimId,
        claimExpiresAt: competingClaimExpiresAt,
        updatedAt: dispatchNow + 1,
      });
      throw new HarnessStorageChannelOutboxClaimConflictError(outboxItemId, input.claimId);
    });

    await expect(
      harness.channels.dispatchOutbox({ channelId: 'support', claimId: 'claim-lost', now: dispatchNow, claimTtlMs }),
    ).resolves.toEqual({
      claimed: 1,
      sent: 0,
      failed: 1,
      dead: 0,
      items: [
        {
          outboxItemId: expect.any(String),
          status: 'failed',
          error: {
            code: 'unknown',
            message: `Channel outbox item "${outboxItemId}" is not held by claim "claim-lost"; claim was lost before failure could be recorded`,
          },
        },
      ],
    });
    expect(renewSpy).toHaveBeenCalledOnce();
    expect(deliver).not.toHaveBeenCalled();
    const retainedByCompetingClaim = db.harnessChannelOutbox.get(outboxItemId);
    expect(retainedByCompetingClaim).toMatchObject({
      status: 'claimed',
      claimId: competingClaimId,
      claimExpiresAt: competingClaimExpiresAt,
    });
    expect(retainedByCompetingClaim?.sentAt).toBeUndefined();
    expect(retainedByCompetingClaim?.failedAt).toBeUndefined();
    expect(retainedByCompetingClaim?.lastError).toBeUndefined();
    await expect(
      storage.claimChannelOutbox({
        harnessName: 'primary',
        channelId: 'support',
        claimId: 'early-recovery',
        limit: 10,
        now: dispatchNow + claimTtlMs + 1,
        claimTtlMs: 1000,
      }),
    ).resolves.toEqual([]);
    await expect(
      storage.claimChannelOutbox({
        harnessName: 'primary',
        channelId: 'support',
        claimId: 'recovery',
        limit: 10,
        now: competingClaimExpiresAt + 1,
        claimTtlMs: 1000,
      }),
    ).resolves.toEqual([expect.objectContaining({ id: outboxItemId, status: 'claimed', claimId: 'recovery' })]);
  });

  it('records terminal provider failures on outbox dispatch', async () => {
    const storage = makeStorage();
    const harness = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage },
      channels: {
        support: makeHarnessChannelConfig({
          outbox: { maxAttempts: 1 },
          adapter: {
            deliver: async () => {
              throw new Error('provider rejected payload');
            },
          },
        }),
      },
    });
    new Mastra({
      agents: { default: makeAgent() },
      storage: new InMemoryStore(),
      channels: { slack: makeChannelProvider('slack') },
      harnesses: { primary: harness },
    });

    const failedEvents: Array<{ type: string; dead?: boolean; attempts?: number; error?: { code: string } }> = [];
    harness.subscribe(e => {
      if (e.type === 'channel_outbox_failed') failedEvents.push(e as never);
    });

    await harness.channels.enqueueOutbox(channelOutboxInput());

    await expect(harness.channels.dispatchOutbox({ channelId: 'support', claimId: 'claim-dead' })).resolves.toEqual({
      claimed: 1,
      sent: 0,
      failed: 0,
      dead: 1,
      items: [
        {
          outboxItemId: expect.any(String),
          status: 'dead',
          error: { code: 'unknown', message: 'provider rejected payload' },
        },
      ],
    });

    // §10.2 channel_outbox_failed: terminal (dead) and the bare row code is
    // projected to its namespaced wire code (`unknown` -> `harness.internal`),
    // never leaked bare onto the public event boundary (§13.3f.1).
    expect(failedEvents).toHaveLength(1);
    expect(failedEvents[0]).toMatchObject({ dead: true, error: { code: 'harness.internal' } });
  });

  it('uses the dispatch clock for retryable outbox failure backoff', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    try {
      const storage = makeStorage();
      const harness = new Harness({
        modes: [{ id: 'default', agentId: 'default' }],
        defaultModeId: 'default',
        sessions: { storage },
        channels: {
          support: makeHarnessChannelConfig({
            outbox: { maxAttempts: 3, retryBackoffMs: () => 5_000 },
            adapter: {
              deliver: async () => {
                throw new Error('provider temporarily unavailable');
              },
            },
          }),
        },
      });
      new Mastra({
        agents: { default: makeAgent() },
        storage: new InMemoryStore(),
        channels: { slack: makeChannelProvider('slack') },
        harnesses: { primary: harness },
      });

      await harness.channels.enqueueOutbox(channelOutboxInput());

      await expect(
        harness.channels.dispatchOutbox({ channelId: 'support', claimId: 'claim-clock-1', now: 100_000 }),
      ).resolves.toMatchObject({
        claimed: 1,
        sent: 0,
        failed: 1,
        dead: 0,
      });
      await expect(
        harness.channels.dispatchOutbox({ channelId: 'support', claimId: 'claim-clock-early', now: 104_999 }),
      ).resolves.toMatchObject({
        claimed: 0,
        sent: 0,
        failed: 0,
        dead: 0,
      });
      await expect(
        harness.channels.dispatchOutbox({ channelId: 'support', claimId: 'claim-clock-2', now: 105_000 }),
      ).resolves.toMatchObject({
        claimed: 1,
        sent: 0,
        failed: 1,
        dead: 0,
      });
    } finally {
      vi.useRealTimers();
    }
  });

  it('backs off from failure time for delayed outbox delivery failures', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    try {
      const storage = makeStorage();
      let deliverCalls = 0;
      const harness = new Harness({
        modes: [{ id: 'default', agentId: 'default' }],
        defaultModeId: 'default',
        sessions: { storage },
        channels: {
          support: makeHarnessChannelConfig({
            outbox: { maxAttempts: 3, retryBackoffMs: () => 5_000 },
            adapter: {
              deliver: async () => {
                deliverCalls += 1;
                if (deliverCalls === 1) {
                  await new Promise(resolve => setTimeout(resolve, 10_000));
                  throw new Error('delayed provider failure');
                }
                throw new Error('provider still unavailable');
              },
            },
          }),
        },
      });
      new Mastra({
        agents: { default: makeAgent() },
        storage: new InMemoryStore(),
        channels: { slack: makeChannelProvider('slack') },
        harnesses: { primary: harness },
      });

      await harness.channels.enqueueOutbox(channelOutboxInput());
      const firstDispatch = harness.channels.dispatchOutbox({ channelId: 'support', claimId: 'claim-delayed-1' });
      await vi.advanceTimersByTimeAsync(10_000);
      await expect(firstDispatch).resolves.toMatchObject({
        claimed: 1,
        sent: 0,
        failed: 1,
        dead: 0,
      });
      expect(deliverCalls).toBe(1);

      await expect(
        harness.channels.dispatchOutbox({ channelId: 'support', claimId: 'claim-delayed-early', now: 14_999 }),
      ).resolves.toMatchObject({
        claimed: 0,
        sent: 0,
        failed: 0,
        dead: 0,
      });
      await expect(
        harness.channels.dispatchOutbox({ channelId: 'support', claimId: 'claim-delayed-2', now: 15_000 }),
      ).resolves.toMatchObject({
        claimed: 1,
        sent: 0,
        failed: 1,
        dead: 0,
      });
      expect(deliverCalls).toBe(2);
    } finally {
      vi.useRealTimers();
    }
  });

  it('throws HarnessConfigError for unknown agentId on a mode', () => {
    expect(
      () =>
        new Harness({
          agents: { default: makeAgent() },
          modes: [{ id: 'default', agentId: 'missing' }],
          defaultModeId: 'default',
          sessions: { storage: makeStorage() },
        }),
    ).toThrow(HarnessConfigError);
  });

  it('preserves the unbound Mastra config error when resolving mode agents', () => {
    const harness = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });

    expect(() => harness.getAgentForMode('default')).toThrow(
      expect.objectContaining({ field: 'mastra', name: 'HarnessConfigError' }),
    );
    expect(() =>
      harness._resolveAgentForRuntimeDependencies({ modeId: 'default', agentId: 'default' }, 'queued item recovery'),
    ).toThrow(expect.objectContaining({ field: 'mastra', name: 'HarnessConfigError' }));
  });

  it('captures and validates runtimeCompatibilityGeneration for recoverable runtime deps', () => {
    expect(
      () =>
        new Harness({
          agents: { default: makeAgent() },
          modes: [{ id: 'default', agentId: 'default' }],
          defaultModeId: 'default',
          runtimeCompatibilityGeneration: '   ',
          sessions: { storage: makeStorage() },
        }),
    ).toThrow(expect.objectContaining({ field: 'runtimeCompatibilityGeneration', name: 'HarnessConfigError' }));

    const harness = new Harness({
      agents: { default: makeAgent() },
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      runtimeCompatibilityGeneration: ' generation-a ',
      sessions: { storage: makeStorage() },
    });

    expect(harness._runtimeDependenciesForMode('default')).toMatchObject({
      modeId: 'default',
      agentId: 'default',
      runtimeCompatibilityGeneration: 'generation-a',
    });

    expect(() =>
      harness._resolveAgentForRuntimeDependencies(
        {
          modeId: 'default',
          agentId: 'default',
          runtimeCompatibilityGeneration: 'generation-b',
        },
        'queued item recovery',
      ),
    ).toThrow(
      expect.objectContaining({
        code: 'harness.runtime_drift',
        driftedRefs: expect.arrayContaining([
          expect.objectContaining({
            kind: 'mode',
            ref: 'default',
            expectedGeneration: 'generation-b',
            actualGeneration: 'generation-a',
          }),
        ]),
      }),
    );

    expect(
      harness._resolveAgentForRuntimeDependencies({ modeId: 'default', agentId: 'default' }, 'legacy recovery'),
    ).toMatchObject({
      mode: { id: 'default', agentId: 'default' },
    });

    const unconfiguredHarness = new Harness({
      agents: { default: makeAgent() },
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage: makeStorage() },
    });

    expect(() =>
      unconfiguredHarness._resolveAgentForRuntimeDependencies(
        {
          modeId: 'default',
          agentId: 'default',
          runtimeCompatibilityGeneration: 'generation-a',
        },
        'unset generation recovery',
      ),
    ).toThrow(
      expect.objectContaining({
        code: 'harness.runtime_drift',
        driftedRefs: expect.arrayContaining([
          expect.objectContaining({ kind: 'mode', ref: 'default', expectedGeneration: 'generation-a' }),
        ]),
      }),
    );
  });

  it('throws HarnessConfigError for duplicate mode ids', () => {
    expect(
      () =>
        new Harness({
          agents: { default: makeAgent() },
          modes: [
            { id: 'default', agentId: 'default' },
            { id: 'default', agentId: 'default' },
          ],
          defaultModeId: 'default',
          sessions: { storage: makeStorage() },
        }),
    ).toThrow(HarnessConfigError);
  });

  it('throws HarnessConfigError when defaultModeId references an unknown mode', () => {
    expect(
      () =>
        new Harness({
          agents: { default: makeAgent() },
          modes: [{ id: 'default', agentId: 'default' }],
          defaultModeId: 'missing',
          sessions: { storage: makeStorage() },
        }),
    ).toThrow(HarnessConfigError);
  });

  it('throws HarnessConfigError when modes is non-empty but defaultModeId is omitted', () => {
    expect(
      () =>
        new Harness({
          agents: { default: makeAgent() },
          modes: [{ id: 'default', agentId: 'default' }],
          sessions: { storage: makeStorage() },
        }),
    ).toThrow(HarnessConfigError);
  });

  it('throws HarnessConfigError for invalid close timeout', () => {
    for (const closeTimeoutMs of [0, 1.5, Number.NaN, Number.POSITIVE_INFINITY, 2_147_483_648]) {
      expect(
        () =>
          new Harness({
            agents: { default: makeAgent() },
            modes: [{ id: 'default', agentId: 'default' }],
            defaultModeId: 'default',
            sessions: { storage: makeStorage(), closeTimeoutMs },
          }),
      ).toThrow(HarnessConfigError);
    }
  });

  it('throws HarnessConfigError when transitionsTo references an unknown mode', () => {
    expect(
      () =>
        new Harness({
          agents: { default: makeAgent() },
          modes: [{ id: 'default', agentId: 'default', transitionsTo: 'nope' }],
          defaultModeId: 'default',
          sessions: { storage: makeStorage() },
        }),
    ).toThrow(HarnessConfigError);
  });

  it('throws HarnessConfigError when a mode declares both tools and additionalTools', () => {
    expect(
      () =>
        new Harness({
          agents: { default: makeAgent() },
          modes: [{ id: 'default', agentId: 'default', tools: {}, additionalTools: {} }],
          defaultModeId: 'default',
          sessions: { storage: makeStorage() },
        }),
    ).toThrow(HarnessConfigError);
  });

  it('mints a unique ownerId per Harness instance', () => {
    const a = makeHarness();
    const b = makeHarness();
    expect(a.ownerId).not.toBe(b.ownerId);
    expect(a.ownerId).toMatch(/^harness-/);
  });
});

describe('Harness v1 — session(...) by thread', () => {
  let storage: InMemoryHarness;
  let harness: Harness;

  beforeEach(() => {
    storage = makeStorage();
    harness = makeHarness({ sessions: { storage } });
  });

  it('creates a fresh record when no session exists for the thread', async () => {
    const session = await harness.session({ threadId: 't1', resourceId: 'r1' });
    expect(session.threadId).toBe('t1');
    expect(session.resourceId).toBe('r1');
    expect(session.id).toMatch(/^sess-/);
    expect(session.lifecycleState).toBe('live');
  });

  it('returns the same live instance on a repeat lookup', async () => {
    const a = await harness.session({ threadId: 't1', resourceId: 'r1' });
    const b = await harness.session({ threadId: 't1', resourceId: 'r1' });
    expect(b).toBe(a);
  });

  it('hydrates from storage when the session is no longer live', async () => {
    const original = await harness.session({ threadId: 't1', resourceId: 'r1' });
    const id = original.id;

    // Simulate a process restart by spinning up a new Harness against the
    // same storage. The old harness still holds the lease, so we shutdown
    // first to release it.
    await harness.shutdown();
    const harness2 = makeHarness({ sessions: { storage } });

    const rehydrated = await harness2.session({ threadId: 't1', resourceId: 'r1' });
    expect(rehydrated.id).toBe(id);
    expect(rehydrated).not.toBe(original);
  });

  it('emits session_hydrated on rehydrate and harness_shutdown on shutdown (§10.2)', async () => {
    const first = await harness.session({ threadId: 't-hyd', resourceId: 'r1' });
    const id = first.id;
    await harness.shutdown();

    const harness2 = makeHarness({ sessions: { storage } });
    const harnessEvents: string[] = [];
    harness2.subscribe(e => harnessEvents.push((e as { type: string }).type));

    const rehydrated = await harness2.session({ threadId: 't-hyd', resourceId: 'r1' });
    expect(rehydrated.id).toBe(id);
    // §10.2: a session re-loaded from storage emits session_hydrated (fanned to
    // harness.subscribe as a session-scoped observer event).
    expect(harnessEvents).toContain('session_hydrated');

    await harness2.shutdown();
    // §10.2: harness-scoped process-shutdown marker on harness.subscribe.
    expect(harnessEvents).toContain('harness_shutdown');
  });

  it('uses the registered Mastra harness key as the storage namespace', async () => {
    const storage = new InMemoryStore();
    const alpha = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });
    const beta = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });
    new Mastra({
      agents: { default: makeAgent() },
      storage,
      harnesses: { alpha, beta },
    });

    const a = await alpha.session({ threadId: 'shared-thread', resourceId: 'r1' });
    const b = await beta.session({ threadId: 'shared-thread', resourceId: 'r1' });

    expect(a.id).not.toBe(b.id);
    expect(a.getRecord().harnessName).toBe('alpha');
    expect(b.getRecord().harnessName).toBe('beta');
    const harnessStore = await storage.getStore('harness');
    expect(harnessStore).toBeDefined();
    await expect(harnessStore!.loadSession({ harnessName: 'alpha', sessionId: a.id })).resolves.toMatchObject({
      harnessName: 'alpha',
    });
    await expect(harnessStore!.loadSession({ harnessName: 'beta', sessionId: b.id })).resolves.toMatchObject({
      harnessName: 'beta',
    });
  });

  it('uses the first registered key for a harness pre-bound to the same Mastra', async () => {
    const storage = new InMemoryStore();
    const mastra = new Mastra({
      agents: { default: makeAgent() },
      storage,
    });
    const alpha = new Harness({
      mastra,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });

    expect(() => alpha.__registerMastra(mastra, 'alpha')).not.toThrow();

    const session = await alpha.session({ threadId: 'shared-thread', resourceId: 'r1' });
    expect(session.getRecord().harnessName).toBe('alpha');

    const harnessStore = await storage.getStore('harness');
    expect(harnessStore).toBeDefined();
    await expect(harnessStore!.loadSession({ harnessName: 'alpha', sessionId: session.id })).resolves.toMatchObject({
      harnessName: 'alpha',
    });
    await expect(harnessStore!.loadSession({ harnessName: 'default', sessionId: session.id })).resolves.toBeNull();
  });

  it('rejects registering a pre-bound harness on a different Mastra', () => {
    const mastra = new Mastra({
      agents: { default: makeAgent() },
      storage: new InMemoryStore(),
    });
    const other = new Mastra({
      agents: { default: makeAgent() },
      storage: new InMemoryStore(),
    });
    const harness = new Harness({
      mastra,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });

    expect(() => harness.__registerMastra(other, 'alpha')).toThrow(HarnessConfigError);
  });

  it('rejects first non-default registration after a pre-bound harness created sessions', async () => {
    const storage = new InMemoryStore();
    const mastra = new Mastra({
      agents: { default: makeAgent() },
      storage,
    });
    const harness = new Harness({
      mastra,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });
    const session = await harness.session({ threadId: 'shared-thread', resourceId: 'r1' });
    await harness.shutdown();

    expect(() => harness.__registerMastra(mastra, 'alpha')).toThrow(HarnessConfigError);

    const harnessStore = await storage.getStore('harness');
    await expect(harnessStore!.loadSession({ harnessName: 'default', sessionId: session.id })).resolves.toMatchObject({
      harnessName: 'default',
    });
  });

  it('fails closed instead of shadowing active default sessions after restart', async () => {
    const storage = new InMemoryStore();
    const mastra = new Mastra({
      agents: { default: makeAgent() },
      storage,
    });
    const defaultHarness = new Harness({
      mastra,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });
    const defaultSession = await defaultHarness.session({ threadId: 'shared-thread', resourceId: 'r1' });
    await defaultHarness.shutdown();

    const alpha = new Harness({
      mastra,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });
    expect(() => alpha.__registerMastra(mastra, 'alpha')).not.toThrow();

    await expect(alpha.session({ threadId: 'shared-thread', resourceId: 'r1' })).rejects.toThrow(HarnessConfigError);

    const harnessStore = await storage.getStore('harness');
    await expect(
      harnessStore!.loadSession({ harnessName: 'default', sessionId: defaultSession.id }),
    ).resolves.toMatchObject({
      harnessName: 'default',
    });

    const alphaSession = await alpha.session({ threadId: 'alpha-thread', resourceId: 'r1' });
    expect(alphaSession.getRecord().harnessName).toBe('alpha');
  });

  it('rolls back the first registered key if Mastra binding validation fails', () => {
    const invalid = new Mastra({
      agents: { other: makeAgent() },
      storage: new InMemoryStore(),
    });
    const valid = new Mastra({
      agents: { default: makeAgent() },
      storage: new InMemoryStore(),
    });
    const harness = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });

    expect(() => harness.__registerMastra(invalid, 'bad')).toThrow(HarnessConfigError);
    expect(() => harness.__registerMastra(valid, 'alpha')).not.toThrow();
  });

  it('rejects re-registering the same Mastra under a different harness key', async () => {
    const storage = new InMemoryStore();
    const alpha = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });
    const mastra = new Mastra({
      agents: { default: makeAgent() },
      storage,
      harnesses: { alpha },
    });

    expect(() => alpha.__registerMastra(mastra, 'renamed')).toThrow(HarnessConfigError);
  });

  it('forces a brand-new thread when threadId is { fresh: true }', async () => {
    const a = await harness.session({ threadId: { fresh: true }, resourceId: 'r1' });
    const b = await harness.session({ threadId: { fresh: true }, resourceId: 'r1' });
    expect(a.threadId).not.toBe(b.threadId);
    expect(a.id).not.toBe(b.id);
  });

  it('marks ownsThread=true when minting a fresh thread', async () => {
    const session = await harness.session({ threadId: { fresh: true }, resourceId: 'r1' });
    expect(session.getRecord().ownsThread).toBe(true);
  });

  it('marks ownsThread=false when binding to a caller-supplied thread', async () => {
    const session = await harness.session({ threadId: 't1', resourceId: 'r1' });
    expect(session.getRecord().ownsThread).toBe(false);
  });

  it('treats a foreign-resource thread as not-existing (creates fresh under caller resource)', async () => {
    await harness.session({ threadId: 't1', resourceId: 'r1' });
    const stranger = await harness.session({ threadId: 't1', resourceId: 'r2' });
    expect(stranger.resourceId).toBe('r2');
  });

  it('reopens the same closed session on thread lookup instead of forking a fresh one (§5.3)', async () => {
    const first = await harness.session({ threadId: 't1', resourceId: 'r1' });
    await first.close();

    const second = await harness.session({ threadId: 't1', resourceId: 'r1' });
    expect(second.id).toBe(first.id);
    expect(second.threadId).toBe('t1');
    expect(second.isClosed).toBe(false);
  });
});

describe('Harness v1 — session(...) by sessionId', () => {
  let storage: InMemoryHarness;
  let harness: Harness;

  beforeEach(() => {
    storage = makeStorage();
    harness = makeHarness({ sessions: { storage } });
  });

  it('returns the live instance', async () => {
    const created = await harness.session({ threadId: 't1', resourceId: 'r1' });
    const fetched = await harness.session({ sessionId: created.id });
    expect(fetched).toBe(created);
  });

  it('throws HarnessSessionNotFoundError for an unknown sessionId', async () => {
    await expect(harness.session({ sessionId: 'nope' })).rejects.toThrow(HarnessSessionNotFoundError);
  });

  it('reopens a closed session on direct-ID lookup instead of failing (§5.3)', async () => {
    const created = await harness.session({ threadId: 't1', resourceId: 'r1' });
    await created.close();
    const reopened = await harness.session({ sessionId: created.id });
    expect(reopened.id).toBe(created.id);
    expect(reopened.isClosed).toBe(false);
  });

  it('does not leak existence across resources (foreign resourceId surfaces as not-found)', async () => {
    const created = await harness.session({ threadId: 't1', resourceId: 'r1' });
    await expect(harness.session({ sessionId: created.id, resourceId: 'r2' })).rejects.toThrow(
      HarnessSessionNotFoundError,
    );
  });

  it('returns the live instance when resourceId matches', async () => {
    const created = await harness.session({ threadId: 't1', resourceId: 'r1' });
    const fetched = await harness.session({ sessionId: created.id, resourceId: 'r1' });
    expect(fetched).toBe(created);
  });

  // §5.2a/§4.5: a closed/closing record requested by id, while a DIFFERENT session
  // currently owns the thread, is an expected CONFLICT — reopening it would create a
  // second active owner. Fail with HarnessSessionConflictError naming the active owner
  // (distinct from duplicate-owner corruption).
  function makeOwnershipBaseRecord() {
    return {
      harnessName: 'default',
      resourceId: 'r1',
      origin: 'top-level' as const,
      ownsThread: false,
      modeId: 'default',
      modelId: 'default',
      subagentModelOverrides: {},
      permissionRules: { categories: {}, tools: {} },
      sessionGrants: { categories: [], tools: [] },
      tokenUsage: { promptTokens: 0, completionTokens: 0, totalTokens: 0 },
      pendingQueue: [],
      state: undefined,
      version: 0,
    };
  }

  it('throws HarnessSessionConflictError when a closed direct-ID record is no longer the thread owner (§5.2a/§4.5)', async () => {
    const base = makeOwnershipBaseRecord();
    const t0 = Date.now();
    // The superseded (closed) record the caller asks for by id.
    await storage.saveSession(
      { ...base, id: 'sess-stale', threadId: 't-conflict', createdAt: t0, lastActivityAt: t0, closedAt: t0 },
      { harnessName: 'default', ifVersion: 0 },
    );
    // A different, currently-active owner of the same thread (later activity → current owner).
    await storage.saveSession(
      { ...base, id: 'sess-active', threadId: 't-conflict', createdAt: t0 + 1, lastActivityAt: t0 + 1000 },
      { harnessName: 'default', ifVersion: 0 },
    );

    await expect(harness.session({ sessionId: 'sess-stale' })).rejects.toBeInstanceOf(HarnessSessionConflictError);
    await expect(harness.session({ sessionId: 'sess-stale' })).rejects.toMatchObject({
      code: 'harness.session_conflict',
      resourceId: 'r1',
      threadId: 't-conflict',
      requestedSessionId: 'sess-stale',
      activeSessionId: 'sess-active',
    });
  });

  it('still throws HarnessSessionCorruptError when an ACTIVE direct-ID record collides with a different active owner (§5.2a)', async () => {
    const base = makeOwnershipBaseRecord();
    const t0 = Date.now();
    // Two genuinely-active records on one thread — only reachable via out-of-band
    // mutation, since saveSession refuses a second active insert on an owned thread.
    await storage.saveSession(
      { ...base, id: 'sess-dup-a', threadId: 't-corrupt', createdAt: t0, lastActivityAt: t0 },
      { harnessName: 'default', ifVersion: 0 },
    );
    await storage.saveSession(
      { ...base, id: 'sess-dup-b', threadId: 't-corrupt-staging', createdAt: t0, lastActivityAt: t0 },
      { harnessName: 'default', ifVersion: 0 },
    );
    // Move B onto A's thread out-of-band (update path skips the active-owner guard).
    await storage.saveSession(
      { ...base, id: 'sess-dup-b', threadId: 't-corrupt', createdAt: t0, lastActivityAt: t0 + 1000 },
      { harnessName: 'default', ifVersion: 1 },
    );

    await expect(harness.session({ sessionId: 'sess-dup-a' })).rejects.toBeInstanceOf(HarnessSessionCorruptError);
    await expect(harness.session({ sessionId: 'sess-dup-a' })).rejects.toMatchObject({
      code: 'harness.session_corrupt',
      reason: 'duplicate_session_owner',
      sessionId: 'sess-dup-a',
      threadId: 't-corrupt',
      ownerSessionIds: ['sess-dup-b'],
    });
  });
});

describe('Harness v1 — session(...) by resource (removed, §5.3)', () => {
  it('rejects resource-only resolution — there is no "continue latest" resolver', async () => {
    const harness = makeHarness();
    // §5.3: resource-only resolution is intentionally not a resolver overload —
    // the type forbids it and the runtime rejects it. "Continue latest" is
    // product policy: read listSessions(...) then resolve a concrete
    // sessionId/threadId.
    await expect(harness.session({ resourceId: 'r1' } as never)).rejects.toBeInstanceOf(HarnessConfigError);
  });
});

describe('Harness v1 — lifecycle', () => {
  it('drops sessions from the live map on close', async () => {
    const harness = makeHarness();
    const s = await harness.session({ threadId: 't1', resourceId: 'r1' });
    expect(harness._internalLiveSessionCount()).toBe(1);
    await s.close();
    expect(harness._internalLiveSessionCount()).toBe(0);
    expect(s.isClosed).toBe(true);
  });

  it('close is idempotent', async () => {
    const harness = makeHarness();
    const s = await harness.session({ threadId: 't1', resourceId: 'r1' });
    await s.close();
    await expect(s.close()).resolves.toBeUndefined();
  });

  it('Session.delete() removes a closed session via the harness cascade (§4.1)', async () => {
    const storage = makeStorage();
    const harness = makeHarness({ sessions: { storage } });
    const s = await harness.session({ threadId: 't1', resourceId: 'r1' });
    await s.close();
    await s.delete();
    expect(harness._internalLiveSessionCount()).toBe(0);
    await expect(storage.loadSession({ sessionId: s.id, harnessName: 'default' })).resolves.toBeNull();
  });

  it('operator force-delete removes an active session (Session.delete is guarded-only, §4.1)', async () => {
    const storage = makeStorage();
    const harness = makeHarness({ sessions: { storage } });
    const s = await harness.session({ threadId: 't1', resourceId: 'r1' });
    // Force-delete is the operator path, not exposed on the Session handle.
    await harness.deleteSession({ sessionId: s.id, resourceId: 'r1', force: true });
    expect(harness._internalLiveSessionCount()).toBe(0);
    await expect(storage.loadSession({ sessionId: s.id, harnessName: 'default' })).resolves.toBeNull();
  });

  it('Session.rename() updates the backing thread title (§4.1)', async () => {
    const harness = makeHarness();
    await createHarnessOperatorThreadController(harness).create({
      resourceId: 'r1',
      threadId: 't1',
      title: 'Original',
    });
    const s = await harness.session({ threadId: 't1', resourceId: 'r1' });
    await s.rename({ title: 'Renamed conversation' });
    const thread = await createHarnessOperatorThreadController(harness).get({ resourceId: 'r1', threadId: 't1' });
    expect(thread?.title).toBe('Renamed conversation');
  });

  it('Session.clone() creates a new owning session over a copied thread (§4.1)', async () => {
    const harness = makeHarness();
    await createHarnessOperatorThreadController(harness).create({ resourceId: 'r1', threadId: 't1', title: 'Source' });
    const s = await harness.session({ threadId: 't1', resourceId: 'r1' });

    const cloned = await s.clone({ title: 'Clone' });
    expect(cloned.id).not.toBe(s.id);
    expect(cloned.threadId).not.toBe(s.threadId);
    expect(cloned.resourceId).toBe('r1');
    expect(cloned.isClosed).toBe(false);

    const clonedThread = await createHarnessOperatorThreadController(harness).get({
      resourceId: 'r1',
      threadId: cloned.threadId,
    });
    expect(clonedThread?.title).toBe('Clone');
    // Cloning never tears down or mutates the source session.
    expect(s.isClosed).toBe(false);
  });

  it('pressure-evicts the least-recently-active unpinned session at maxLive (§5.4)', async () => {
    const storage = makeStorage();
    const harness = makeHarness({ sessions: { storage, maxLive: 2 } });
    const a = await harness.session({ threadId: 'ta', resourceId: 'r1' });
    await harness.session({ threadId: 'tb', resourceId: 'r1' });
    expect(harness._internalLiveSessionCount()).toBe(2);

    // A third live session exceeds the cap → evict the LRU (a, the oldest).
    const c = await harness.session({ threadId: 'tc', resourceId: 'r1' });
    expect(harness._internalLiveSessionCount()).toBe(2);
    expect(c.threadId).toBe('tc');

    // The evicted session's record persists (reopenable), and re-resolving it
    // hydrates transparently.
    await expect(storage.loadSession({ sessionId: a.id, harnessName: 'default' })).resolves.not.toBeNull();
    const reA = await harness.session({ sessionId: a.id });
    expect(reA.id).toBe(a.id);
  });

  it('throws HarnessLiveSessionLimitError when every live session at maxLive is pinned (§5.4)', async () => {
    const storage = makeStorage();
    const harness = makeHarness({ sessions: { storage, maxLive: 1 } });
    const now = Date.now();
    await storage.saveSession(
      {
        harnessName: 'default',
        id: 'sess-pinned',
        resourceId: 'r1',
        threadId: 'tp',
        origin: 'top-level',
        ownsThread: false,
        modeId: 'default',
        modelId: 'default',
        subagentModelOverrides: {},
        permissionRules: { categories: {}, tools: {} },
        sessionGrants: { categories: [], tools: [] },
        tokenUsage: { promptTokens: 0, completionTokens: 0, totalTokens: 0 },
        pendingQueue: [],
        pendingResume: {
          kind: 'question',
          itemId: 'q1',
          runId: 'run-q',
          toolCallId: 'tc-q',
          source: 'parent',
          requestedAt: now,
          payload: { question: 'pick' },
        },
        state: undefined,
        createdAt: now,
        lastActivityAt: now,
        version: 0,
      } as any,
      { harnessName: 'default', ifVersion: 0 },
    );

    const pinned = await harness.session({ sessionId: 'sess-pinned' });
    expect(pinned.isPinned()).toBe(true);
    expect(harness._internalLiveSessionCount()).toBe(1);

    // Only live session is pinned → a new session cannot evict it.
    await expect(harness.session({ threadId: 'tnew', resourceId: 'r1' })).rejects.toBeInstanceOf(
      HarnessLiveSessionLimitError,
    );
  });

  it('idle-evicts an unpinned session after idleTimeoutMs (§5.4)', async () => {
    const storage = makeStorage();
    const harness = makeHarness({ sessions: { storage, idleTimeoutMs: 1 } });
    const s = await harness.session({ threadId: 'ti', resourceId: 'r1' });
    expect(harness._internalLiveSessionCount()).toBe(1);

    await new Promise(resolve => setTimeout(resolve, 5));
    await (harness as unknown as { _evictIdleSessions(): Promise<void> })._evictIdleSessions();

    expect(harness._internalLiveSessionCount()).toBe(0);
    // The evicted record persists and rehydrates transparently.
    await expect(storage.loadSession({ sessionId: s.id, harnessName: 'default' })).resolves.not.toBeNull();
    const re = await harness.session({ sessionId: s.id });
    expect(re.id).toBe(s.id);
  });

  it('persists closing markers before terminal close', async () => {
    const storage = makeStorage();
    const harness = makeHarness({ sessions: { storage, closeTimeoutMs: 250 } });
    const events: Array<{ type: string; sessionId?: string; closingAt?: number; closeDeadlineAt?: number }> = [];
    harness.subscribe(event => {
      if (event.type === 'session_closing' || event.type === 'session_closed') {
        events.push(event);
      }
    });

    const s = await harness.session({ threadId: 't1', resourceId: 'r1' });
    await s.close();

    const stored = await harness.loadSession({ sessionId: s.id, includeClosed: true });
    expect(stored?.closingAt).toBeDefined();
    expect(stored?.closeDeadlineAt).toBe((stored?.closingAt ?? 0) + 250);
    expect(stored?.closedAt).toBeDefined();

    const closing = events.find(event => event.type === 'session_closing');
    expect(closing).toMatchObject({
      type: 'session_closing',
      sessionId: s.id,
      closingAt: stored?.closingAt,
      closeDeadlineAt: stored?.closeDeadlineAt,
    });
  });

  it('rejects live work after the closing marker commits', async () => {
    const harness = makeHarness();
    const s = await harness.session({ threadId: 't1', resourceId: 'r1' });
    const rejection = new Promise<unknown>(resolve => {
      harness.subscribe(event => {
        if (event.type === 'session_closing' && event.sessionId === s.id) {
          resolve(s.setState({ closing: true }).catch(err => err));
        }
      });
    });

    await s.close();

    await expect(rejection).resolves.toBeInstanceOf(HarnessSessionClosingError);
  });

  it('lets a concurrent close terminalize before shutdown releases the lease', async () => {
    const storage = makeStorage();
    const harness = makeHarness({ sessions: { storage } });
    const s = await harness.session({ threadId: 't1', resourceId: 'r1' });
    const originalSaveSession = storage.saveSession.bind(storage);
    let releaseTerminalSave!: () => void;
    let terminalSaveStarted!: () => void;
    const terminalSaveGate = new Promise<void>(resolve => {
      releaseTerminalSave = resolve;
    });
    const terminalSaveSeen = new Promise<void>(resolve => {
      terminalSaveStarted = resolve;
    });
    storage.saveSession = (async (...args: Parameters<typeof storage.saveSession>) => {
      const [record] = args;
      if (record.id === s.id && record.closingAt !== undefined && record.closedAt !== undefined) {
        terminalSaveStarted();
        await terminalSaveGate;
      }
      return originalSaveSession(...args);
    }) as typeof storage.saveSession;

    const close = s.close();
    await terminalSaveSeen;
    const shutdown = harness.shutdown();

    releaseTerminalSave();
    await Promise.all([close, shutdown]);

    const stored = await storage.loadSession({ sessionId: s.id, harnessName: 'default' });
    expect(stored?.closedAt).toBeDefined();
  });

  it('does not start a new close after shutdown begins', async () => {
    const storage = makeStorage();
    const harness = makeHarness({ sessions: { storage } });
    const s = await harness.session({ threadId: 't1', resourceId: 'r1' });
    const originalRelease = storage.releaseSessionLease.bind(storage);
    let releaseShutdown!: () => void;
    let releaseStarted!: () => void;
    const releaseGate = new Promise<void>(resolve => {
      releaseShutdown = resolve;
    });
    const releaseSeen = new Promise<void>(resolve => {
      releaseStarted = resolve;
    });
    storage.releaseSessionLease = (async (...args: Parameters<typeof storage.releaseSessionLease>) => {
      releaseStarted();
      await releaseGate;
      return originalRelease(...args);
    }) as typeof storage.releaseSessionLease;

    const shutdown = harness.shutdown();
    await releaseSeen;
    await s.close();
    releaseShutdown();
    await shutdown;

    const stored = await storage.loadSession({ sessionId: s.id, harnessName: 'default' });
    expect(stored?.closedAt).toBeUndefined();
  });

  it('does not cascade thread delete after shutdown begins', async () => {
    const storage = makeStorage();
    const harness = makeHarness({ sessions: { storage } });
    const thread = await createHarnessOperatorThreadController(harness).create({
      resourceId: 'r1',
      threadId: 'delete-during-shutdown',
    });
    const s = await harness.session({ threadId: thread.id, resourceId: 'r1' });
    const originalRelease = storage.releaseSessionLease.bind(storage);
    let releaseShutdown!: () => void;
    let releaseStarted!: () => void;
    const releaseGate = new Promise<void>(resolve => {
      releaseShutdown = resolve;
    });
    const releaseSeen = new Promise<void>(resolve => {
      releaseStarted = resolve;
    });
    storage.releaseSessionLease = (async (...args: Parameters<typeof storage.releaseSessionLease>) => {
      releaseStarted();
      await releaseGate;
      return originalRelease(...args);
    }) as typeof storage.releaseSessionLease;

    const shutdown = harness.shutdown();
    await releaseSeen;
    await createHarnessOperatorThreadController(harness).delete({ resourceId: 'r1', threadId: thread.id });
    releaseShutdown();
    await shutdown;

    const stored = await storage.loadSession({ sessionId: s.id, harnessName: 'default' });
    expect(stored?.closedAt).toBeUndefined();
  });

  it('does not delete thread data when shutdown starts during a delete cascade', async () => {
    const storage = makeStorage();
    const harness = makeHarness({ sessions: { storage } });
    const thread = await createHarnessOperatorThreadController(harness).create({
      resourceId: 'r1',
      threadId: 'delete-cascade-during-shutdown',
    });
    const s = await harness.session({ threadId: thread.id, resourceId: 'r1' });
    const originalSaveSession = storage.saveSession.bind(storage);
    let releaseTerminalSave!: () => void;
    let terminalSaveStarted!: () => void;
    const terminalSaveGate = new Promise<void>(resolve => {
      releaseTerminalSave = resolve;
    });
    const terminalSaveSeen = new Promise<void>(resolve => {
      terminalSaveStarted = resolve;
    });
    storage.saveSession = (async (...args: Parameters<typeof storage.saveSession>) => {
      const [record] = args;
      if (record.id === s.id && record.closingAt !== undefined && record.closedAt !== undefined) {
        terminalSaveStarted();
        await terminalSaveGate;
      }
      return originalSaveSession(...args);
    }) as typeof storage.saveSession;

    const deleting = createHarnessOperatorThreadController(harness).delete({ resourceId: 'r1', threadId: thread.id });
    await terminalSaveSeen;
    const shutdown = harness.shutdown();
    releaseTerminalSave();
    await Promise.all([deleting, shutdown]);

    await expect(
      createHarnessOperatorThreadController(harness).get({ resourceId: 'r1', threadId: thread.id }),
    ).resolves.toMatchObject({
      id: thread.id,
    });
    const stored = await storage.loadSession({ sessionId: s.id, harnessName: 'default' });
    expect(stored?.closedAt).toBeDefined();
  });

  it('does not hard-delete session rows when shutdown starts during force delete', async () => {
    const storage = makeStorage();
    const harness = makeHarness({ sessions: { storage } });
    const s = await harness.session({ threadId: 'delete-during-shutdown', resourceId: 'r1' });
    const originalSaveSession = storage.saveSession.bind(storage);
    let releaseTerminalSave!: () => void;
    let terminalSaveStarted!: () => void;
    const terminalSaveGate = new Promise<void>(resolve => {
      releaseTerminalSave = resolve;
    });
    const terminalSaveSeen = new Promise<void>(resolve => {
      terminalSaveStarted = resolve;
    });
    storage.saveSession = (async (...args: Parameters<typeof storage.saveSession>) => {
      const [record] = args;
      if (record.id === s.id && record.closingAt !== undefined && record.closedAt !== undefined) {
        terminalSaveStarted();
        await terminalSaveGate;
      }
      return originalSaveSession(...args);
    }) as typeof storage.saveSession;

    const deleting = harness.deleteSession({ sessionId: s.id, resourceId: 'r1', force: true });
    await terminalSaveSeen;
    const shutdown = harness.shutdown();
    releaseTerminalSave();
    await Promise.all([deleting, shutdown]);

    const stored = await storage.loadSession({ sessionId: s.id, harnessName: 'default' });
    expect(stored?.closedAt).toBeDefined();
  });

  it('aborts an active turn at the close deadline before terminalizing', async () => {
    const storage = makeStorage();
    const agent = new MockAgent({ id: 'default' });
    const hold = deferred();
    const abortSeen = deferred();
    agent.enqueueRun({
      holdUntil: hold.promise,
      onAbort: reason => {
        // §6.2: close-drain timeout aborts the live turn with a typed
        // HarnessAbortedError carrying the close-lifecycle reason.
        expect(reason).toBeInstanceOf(HarnessAbortedError);
        expect(reason).toMatchObject({ reason: 'session_closed', sessionId: s.id });
        abortSeen.resolve();
      },
    });
    const harness = new Harness({
      agents: { default: agent } as any,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage, closeTimeoutMs: 1 },
    });
    const s = await harness.session({ threadId: { fresh: true }, resourceId: 'r1' });

    const message = s.message({ content: 'slow' });
    await new Promise(resolve => setImmediate(resolve));
    expect(s.isRunning()).toBe(true);

    await s.close();
    await abortSeen.promise;
    await expect(message).resolves.toMatchObject({ finishReason: 'aborted' });
    expect(s.isRunning()).toBe(false);

    const stored = await storage.loadSession({ sessionId: s.id, harnessName: 'default' });
    expect(stored?.closedAt).toBeDefined();
  });

  it('aborts an active turn at the shutdown drain deadline with process_restart (§6.2)', async () => {
    const storage = makeStorage();
    const agent = new MockAgent({ id: 'default' });
    const hold = deferred();
    const abortSeen = deferred();
    let abortReason: unknown;
    agent.enqueueRun({
      holdUntil: hold.promise,
      onAbort: reason => {
        abortReason = reason;
        abortSeen.resolve();
      },
    });
    const harness = new Harness({
      agents: { default: agent } as any,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage },
    });
    const s = await harness.session({ threadId: { fresh: true }, resourceId: 'r1' });
    const message = s.message({ content: 'slow' });
    void message.catch(() => {});
    await new Promise(resolve => setImmediate(resolve));
    expect(s.isRunning()).toBe(true);

    // Shutdown rejects on drain-timeout (the turn did not finish in time); that is
    // expected here — we are asserting the tool-visible abort reason it produced.
    const shutdownDone = harness.shutdown({ drainTimeoutMs: 1 }).catch(() => undefined);
    await abortSeen.promise;
    hold.resolve();
    await shutdownDone;

    // §6.2: shutdown releases live process ownership without a durable close, so
    // the turn aborts with the typed process_restart reason.
    expect(abortReason).toBeInstanceOf(HarnessAbortedError);
    expect(abortReason).toMatchObject({ reason: 'process_restart', sessionId: s.id });
  });

  it('bounds close when an active turn ignores the abort signal', async () => {
    const storage = makeStorage();
    const agent = new AbortIgnoringMockAgent({ id: 'default' });
    const hold = deferred();
    agent.enqueueRun({ holdUntil: hold.promise });
    const harness = new Harness({
      agents: { default: agent } as any,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage, closeTimeoutMs: 1 },
    });
    const s = await harness.session({ threadId: { fresh: true }, resourceId: 'r1' });
    const message = s.message({ content: 'slow' });
    void message.catch(() => {});
    await new Promise(resolve => setImmediate(resolve));
    expect(s.isRunning()).toBe(true);

    await s.close();

    const stored = await storage.loadSession({ sessionId: s.id, harnessName: 'default' });
    expect(stored?.closedAt).toBeDefined();
  });

  it('drains queued work admitted before close starts', async () => {
    const storage = makeStorage();
    const agent = new MockAgent({ id: 'default' });
    const hold = deferred();
    agent.enqueueRun({ holdUntil: hold.promise, text: 'manual' });
    agent.enqueueRun({ text: 'queued-1' });
    agent.enqueueRun({ text: 'queued-2' });
    const harness = new Harness({
      agents: { default: agent } as any,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage, closeTimeoutMs: 1000 },
    });
    const s = await harness.session({ threadId: { fresh: true }, resourceId: 'r1' });

    const manual = s.message({ content: 'manual' });
    await new Promise(resolve => setImmediate(resolve));
    const q1 = s.queue({ content: 'q1' });
    const q2 = s.queue({ content: 'q2' });
    await new Promise(resolve => setTimeout(resolve, 0));
    expect(s.getQueueDepth()).toBe(2);

    const close = s.close();
    await new Promise(resolve => setImmediate(resolve));
    await expect(s.queue({ content: 'late' })).rejects.toBeInstanceOf(HarnessSessionClosingError);

    hold.resolve();
    await Promise.all([manual, q1, q2, close]);

    const stored = await storage.loadSession({ sessionId: s.id, harnessName: 'default' });
    expect(stored?.pendingQueue).toEqual([]);
    expect(stored?.closedAt).toBeDefined();
    expect(agent.streamCalls.map(call => extractSignalContents(call.messages))).toEqual(['manual', 'q1', 'q2']);
  });

  it('rejects delayed queue admission once close starts', async () => {
    const storage = makeStorage();
    const agent = new MockAgent({ id: 'default' });
    const hold = deferred();
    agent.enqueueRun({ holdUntil: hold.promise, text: 'manual' });
    const harness = new Harness({
      agents: { default: agent } as any,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage, closeTimeoutMs: 1000 },
    });
    const s = await harness.session({ threadId: { fresh: true }, resourceId: 'r1' });
    const attachment = await harness.attachments.upload({
      sessionId: s.id,
      data: Buffer.from('queued attachment'),
      filename: 'queued.txt',
      contentType: 'text/plain',
    });

    const originalGetAttachmentRecord = storage.getAttachmentRecord.bind(storage);
    let releaseLookup!: () => void;
    let lookupStarted!: () => void;
    const lookupGate = new Promise<void>(resolve => {
      releaseLookup = resolve;
    });
    const lookupSeen = new Promise<void>(resolve => {
      lookupStarted = resolve;
    });
    let lookupGated = false;
    storage.getAttachmentRecord = (async (...args: Parameters<typeof storage.getAttachmentRecord>) => {
      const [opts] = args;
      if (!lookupGated && opts.attachmentId === attachment.attachmentId) {
        lookupGated = true;
        lookupStarted();
        await lookupGate;
      }
      return originalGetAttachmentRecord(...args);
    }) as typeof storage.getAttachmentRecord;

    const manual = s.message({ content: 'manual' });
    await new Promise(resolve => setImmediate(resolve));
    const late = s.queue({ content: 'late', attachments: [attachment] });
    await lookupSeen;

    const close = s.close();
    await waitFor(() => s.getRecord().closingAt !== undefined, 'session closing marker');
    releaseLookup();

    await expect(late).rejects.toBeInstanceOf(HarnessSessionClosingError);
    hold.resolve();
    await Promise.all([manual, close]);

    const stored = await storage.loadSession({ sessionId: s.id, harnessName: 'default' });
    expect(stored?.pendingQueue).toEqual([]);
    expect(agent.streamCalls.map(call => extractSignalContents(call.messages))).toEqual(['manual']);
  });

  it('fails queued waiters instead of hanging when close drain times out', async () => {
    const storage = makeStorage();
    const agent = new AbortIgnoringMockAgent({ id: 'default' });
    const hold = deferred();
    agent.enqueueRun({ holdUntil: hold.promise, text: 'slow' });
    const harness = new Harness({
      agents: { default: agent } as any,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage, closeTimeoutMs: 20 },
    });
    const s = await harness.session({ threadId: { fresh: true }, resourceId: 'r1' });

    const queued = s.queue({ content: 'slow' });
    const queuedSecond = s.queue({ content: 'second' });
    await new Promise(resolve => setImmediate(resolve));
    const close = s.close();

    await expect(queued).rejects.toBeInstanceOf(HarnessSessionClosingError);
    await expect(queuedSecond).rejects.toBeInstanceOf(HarnessSessionClosingError);
    await close;
    hold.resolve();
    await new Promise(resolve => setImmediate(resolve));

    const stored = await storage.loadSession({ sessionId: s.id, harnessName: 'default' });
    expect(stored?.pendingQueue).toEqual([]);
    expect(stored?.closedAt).toBeDefined();
    expect(stored?.queueAdmissionReceipts?.[Object.keys(stored.queueAdmissionReceipts)[0]!]!.status).toBe('failed');
    expect(agent.streamCalls.map(call => extractSignalContents(call.messages))).toEqual(['slow']);
  });

  it('allows an admitted turn to park a question while close is draining', async () => {
    const storage = makeStorage();
    const agent = new MockAgent({ id: 'default' });
    const hold = deferred();
    agent.enqueueRun({ holdUntil: hold.promise, text: 'manual' });
    const harness = new Harness({
      agents: { default: agent } as any,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage, closeTimeoutMs: 20 },
    });
    const s = await harness.session({ threadId: { fresh: true }, resourceId: 'r1' });

    const manual = s.message({ content: 'manual' });
    await new Promise(resolve => setImmediate(resolve));
    const close = s.close();
    await waitFor(() => s.getRecord().closingAt !== undefined, 'session closing marker');

    await expect(
      (s as any)._registerQuestion({
        questionId: 'q1',
        question: 'continue?',
        runId: 'run-1',
        toolCallId: 'tool-1',
      }),
    ).resolves.toBeUndefined();
    expect(s.getRecord().pendingResume).toMatchObject({ kind: 'question', itemId: 'q1' });

    hold.resolve();
    await Promise.all([manual, close]);
    const stored = await storage.loadSession({ sessionId: s.id, harnessName: 'default' });
    expect(stored?.closedAt).toBeDefined();
  });

  it('serializes admitted writes before close and rejects late child creation', async () => {
    const storage = makeStorage();
    const harness = makeHarness({ sessions: { storage } });
    const s = await harness.session({ threadId: 't1', resourceId: 'r1' });
    const originalSaveSession = storage.saveSession.bind(storage);
    let releaseAdmittedSave!: () => void;
    let admittedSaveStarted!: () => void;
    const admittedSaveGate = new Promise<void>(resolve => {
      releaseAdmittedSave = resolve;
    });
    const admittedSaveSeen = new Promise<void>(resolve => {
      admittedSaveStarted = resolve;
    });
    let admittedSaveGated = false;
    storage.saveSession = (async (...args: Parameters<typeof storage.saveSession>) => {
      const [record] = args;
      const state = record.state as { admitted?: unknown } | undefined;
      if (!admittedSaveGated && record.id === s.id && state?.admitted === true && record.closingAt === undefined) {
        admittedSaveGated = true;
        admittedSaveStarted();
        await admittedSaveGate;
      }
      return originalSaveSession(...args);
    }) as typeof storage.saveSession;

    const admitted = s.setState({ admitted: true });
    await admittedSaveSeen;
    const closing = s.close();
    const closingAgain = s.close();

    await expect(
      harness.session({
        resourceId: 'r1',
        threadId: { fresh: true },
        parentSessionId: s.id,
      }),
    ).rejects.toBeInstanceOf(HarnessSessionClosingError);

    releaseAdmittedSave();
    await admitted;
    await Promise.all([closing, closingAgain]);

    const stored = await harness.loadSession({ sessionId: s.id, includeClosed: true });
    expect(stored?.state).toMatchObject({ admitted: true });
    expect(stored?.closingAt).toBeDefined();
    expect(stored?.closedAt).toBeDefined();
  });

  it('terminalizes descendant sessions before the close target', async () => {
    const storage = makeStorage();
    const harness = makeHarness({ sessions: { storage } });
    const closedSessionIds: string[] = [];
    harness.subscribe(event => {
      if (event.type === 'session_closed') closedSessionIds.push(event.sessionId!);
    });

    const parent = await harness.session({ threadId: 't1', resourceId: 'r1' });
    const child = await harness.session({
      threadId: { fresh: true },
      resourceId: 'r1',
      parentSessionId: parent.id,
    });
    const grandchild = await harness.session({
      threadId: { fresh: true },
      resourceId: 'r1',
      parentSessionId: child.id,
    });

    await parent.close();

    expect(closedSessionIds).toEqual([grandchild.id, child.id, parent.id]);
  });

  it('§5.5: stamps ONE fixed close deadline across a 3-level subtree (not depth-multiplied)', async () => {
    const storage = makeStorage();
    const closeTimeoutMs = 250;
    const harness = makeHarness({ sessions: { storage, closeTimeoutMs } });
    const closingEvents: Array<{ sessionId?: string; closingAt?: number; closeDeadlineAt?: number }> = [];
    harness.subscribe(event => {
      if (event.type === 'session_closing') {
        closingEvents.push({
          sessionId: event.sessionId,
          closingAt: event.closingAt,
          closeDeadlineAt: event.closeDeadlineAt,
        });
      }
    });

    const parent = await harness.session({ threadId: 't1', resourceId: 'r1' });
    const child = await harness.session({
      threadId: { fresh: true },
      resourceId: 'r1',
      parentSessionId: parent.id,
    });
    const grandchild = await harness.session({
      threadId: { fresh: true },
      resourceId: 'r1',
      parentSessionId: child.id,
    });

    await parent.close();

    // Every node in the subtree shares the SINGLE root deadline. Under the
    // pre-fix per-node computation each descendant stamped its own (later)
    // `Date.now() + closeTimeoutMs` during the BFS marking walk, so the child's
    // and grandchild's deadlines would be strictly greater than the root's.
    expect(closingEvents).toHaveLength(3);
    const root = closingEvents.find(e => e.sessionId === parent.id)!;
    const childEvent = closingEvents.find(e => e.sessionId === child.id)!;
    const grandchildEvent = closingEvents.find(e => e.sessionId === grandchild.id)!;
    expect(root.closeDeadlineAt).toBe((root.closingAt ?? 0) + closeTimeoutMs);
    expect(childEvent.closeDeadlineAt).toBe(root.closeDeadlineAt);
    expect(grandchildEvent.closeDeadlineAt).toBe(root.closeDeadlineAt);

    // And the durable records carry the same single deadline.
    for (const id of [parent.id, child.id, grandchild.id]) {
      const stored = await harness.loadSession({ sessionId: id, includeClosed: true });
      expect(stored?.closeDeadlineAt).toBe(root.closeDeadlineAt);
    }
  });

  it('§5.5: a busy 3-level subtree closes within ~closeTimeoutMs, not depth-multiplied', async () => {
    const storage = makeStorage();
    const closeTimeoutMs = 60;
    const agent = new MockAgent({ id: 'default' });
    // One held run per subtree level: each session is busy when close drains, so
    // every node's `_waitForCloseDrain` actually blocks until the deadline. Under
    // the pre-fix per-node deadline the waits stack (≈ depth × closeTimeoutMs);
    // with one shared deadline the whole drain is bounded by ~closeTimeoutMs.
    const hold = deferred();
    agent.enqueueRun({ holdUntil: hold.promise, text: 'p' });
    agent.enqueueRun({ holdUntil: hold.promise, text: 'c' });
    agent.enqueueRun({ holdUntil: hold.promise, text: 'g' });
    const harness = new Harness({
      agents: { default: agent } as any,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage, closeTimeoutMs },
    });

    const parent = await harness.session({ threadId: { fresh: true }, resourceId: 'r1' });
    const child = await harness.session({
      threadId: { fresh: true },
      resourceId: 'r1',
      parentSessionId: parent.id,
    });
    const grandchild = await harness.session({
      threadId: { fresh: true },
      resourceId: 'r1',
      parentSessionId: child.id,
    });

    const pMsg = parent.message({ content: 'p' });
    const cMsg = child.message({ content: 'c' });
    const gMsg = grandchild.message({ content: 'g' });
    await waitFor(() => agent.streamCalls.length === 3, 'all three turns in flight');

    const startedAt = Date.now();
    const close = parent.close();
    await close;
    const elapsed = Date.now() - startedAt;

    // Bounded by a single deadline (+ scheduling slack), NOT 3× the timeout.
    expect(elapsed).toBeLessThan(closeTimeoutMs * 2);

    hold.resolve();
    await Promise.allSettled([pMsg, cMsg, gMsg]);

    for (const id of [parent.id, child.id, grandchild.id]) {
      const stored = await storage.loadSession({ sessionId: id, harnessName: 'default' });
      expect(stored?.closedAt).toBeDefined();
    }
  });

  it('§5.5: a descendant marked during the close walk inherits the root deadline', async () => {
    // Build the subtree first, then drive close on the root. Children are
    // discovered + marked DURING the BFS close walk (after the root deadline is
    // fixed), so a child/grandchild marked later must still carry the root's
    // single deadline rather than a fresh `now + closeTimeoutMs`.
    const storage = makeStorage();
    const closeTimeoutMs = 1000;
    const harness = makeHarness({ sessions: { storage, closeTimeoutMs } });
    const deadlineBySession = new Map<string, number>();
    harness.subscribe(event => {
      if (event.type === 'session_closing' && event.sessionId && event.closeDeadlineAt !== undefined) {
        deadlineBySession.set(event.sessionId, event.closeDeadlineAt);
      }
    });

    const parent = await harness.session({ threadId: 't1', resourceId: 'r1' });
    const child = await harness.session({
      threadId: { fresh: true },
      resourceId: 'r1',
      parentSessionId: parent.id,
    });
    const grandchild = await harness.session({
      threadId: { fresh: true },
      resourceId: 'r1',
      parentSessionId: child.id,
    });

    await parent.close();

    const rootDeadline = deadlineBySession.get(parent.id);
    expect(rootDeadline).toBeDefined();
    expect(deadlineBySession.get(child.id)).toBe(rootDeadline);
    expect(deadlineBySession.get(grandchild.id)).toBe(rootDeadline);
  });

  it('repairs a stored closing marker without resetting the deadline', async () => {
    const storage = makeStorage();
    const harness = makeHarness({ sessions: { storage, closeTimeoutMs: 250 } });
    const s = await harness.session({ threadId: 't1', resourceId: 'r1' });
    const record = s.getRecord();
    await storage.saveSession(
      {
        ...record,
        closingAt: 1234,
        closeDeadlineAt: 5678,
        lastActivityAt: 1234,
      },
      { harnessName: record.harnessName, ownerId: harness.ownerId, ifVersion: record.version },
    );
    await harness.shutdown();

    const harness2 = makeHarness({ sessions: { storage, closeTimeoutMs: 999 } });
    await expect(harness2.session({ sessionId: s.id })).rejects.toBeInstanceOf(HarnessSessionClosingError);
    await harness2.closeSession({ sessionId: s.id });

    const stored = await harness2.loadSession({ sessionId: s.id, includeClosed: true });
    expect(stored?.closingAt).toBe(1234);
    expect(stored?.closeDeadlineAt).toBe(5678);
    expect(stored?.closedAt).toBeDefined();
  });

  it('drains a stored pending queue before closeSession terminalizes a non-live session', async () => {
    const storage = makeStorage();
    const harness = makeHarness({ sessions: { storage, closeTimeoutMs: 1000 } });
    const s = await harness.session({ threadId: 't1', resourceId: 'r1' });
    const record = s.getRecord();

    await harness.shutdown();
    const now = Date.now();
    const queuedItemId = 'stored-close-queue';
    await storage.saveSession(
      {
        ...record,
        pendingQueue: [
          {
            id: queuedItemId,
            admissionId: 'stored-close-admission',
            admissionHash: 'stored-close-hash',
            enqueuedAt: now,
            content: 'stored queued',
            attachments: [],
          },
        ],
        queueAdmissionReceipts: {
          [queuedItemId]: {
            admissionId: 'stored-close-admission',
            admissionHash: 'stored-close-hash',
            queuedItemId,
            status: 'queued',
            attempts: 0,
            enqueuedAt: now,
            updatedAt: now,
          },
        },
        lastActivityAt: now,
      },
      { harnessName: record.harnessName, ownerId: harness.ownerId, ifVersion: record.version },
    );

    const replayAgent = new MockAgent({ id: 'default' });
    replayAgent.enqueueRun({ text: 'stored queued result' });
    const harness2 = new Harness({
      agents: { default: replayAgent } as any,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage, closeTimeoutMs: 1000 },
    });
    await harness2.closeSession({ sessionId: s.id });

    const stored = await harness2.loadSession({ sessionId: s.id, includeClosed: true });
    expect(stored?.pendingQueue).toEqual([]);
    expect(stored?.closedAt).toBeDefined();
    expect(replayAgent.streamCalls.map(call => extractSignalContents(call.messages))).toEqual(['stored queued']);
  });

  it('closeSession by id without holding a live instance still cascades', async () => {
    const storage = makeStorage();
    const harness = makeHarness({ sessions: { storage } });
    const s = await harness.session({ threadId: 't1', resourceId: 'r1' });
    const id = s.id;

    // Drop instance from live map without closing — simulates restart
    await harness.shutdown();

    const harness2 = makeHarness({ sessions: { storage } });
    await harness2.closeSession({ sessionId: id });

    const stored = await harness2.loadSession({ sessionId: id, includeClosed: true });
    expect(stored?.closedAt).toBeDefined();
  });

  it('treats a stored session closed after lease acquisition as an idempotent root close', async () => {
    const db = new InMemoryDB();
    const storage = new CloseAfterLeaseStorage({ db });
    const harness = makeHarness({ sessions: { storage } });
    const s = await harness.session({ threadId: 't1', resourceId: 'r1' });
    await harness.shutdown();

    storage.closeAfterLeaseSessionIds.add(s.id);
    const harness2 = makeHarness({ sessions: { storage } });
    const events: Array<{ type: string; sessionId?: string }> = [];
    harness2.subscribe((event, context) => {
      events.push({ type: event.type, sessionId: context.sessionId });
    });

    await harness2.closeSession({ sessionId: s.id });

    const stored = await harness2.loadSession({ sessionId: s.id, includeClosed: true });
    expect(stored?.closedAt).toBeDefined();
    expect(stored?.closingAt).toBeUndefined();
    expect(events.filter(event => event.sessionId === s.id)).toEqual([]);
  });

  it('skips a cascade child that closes after lease acquisition', async () => {
    const db = new InMemoryDB();
    const storage = new CloseAfterLeaseStorage({ db });
    const harness = makeHarness({ sessions: { storage } });
    const parent = await harness.session({ threadId: 't1', resourceId: 'r1' });
    const child = await harness.session({
      threadId: { fresh: true },
      resourceId: 'r1',
      parentSessionId: parent.id,
    });
    await harness.shutdown();

    storage.closeAfterLeaseSessionIds.add(child.id);
    const harness2 = makeHarness({ sessions: { storage } });
    const events: Array<{ type: string; sessionId?: string }> = [];
    harness2.subscribe((event, context) => {
      events.push({ type: event.type, sessionId: context.sessionId });
    });

    await harness2.closeSession({ sessionId: parent.id });

    const storedParent = await harness2.loadSession({ sessionId: parent.id, includeClosed: true });
    const storedChild = await harness2.loadSession({ sessionId: child.id, includeClosed: true });
    expect(storedParent?.closedAt).toBeDefined();
    expect(storedChild?.closedAt).toBeDefined();
    expect(storedChild?.closingAt).toBeUndefined();
    expect(events.filter(event => event.sessionId === child.id)).toEqual([]);
  });

  it('rejects resource-scoped close for a stored session owned by another resource', async () => {
    const storage = makeStorage();
    const harness = makeHarness({ sessions: { storage } });
    const s = await harness.session({ threadId: 't1', resourceId: 'r1' });
    const id = s.id;
    await harness.shutdown();

    const harness2 = makeHarness({ sessions: { storage } });
    await expect(harness2.closeSession({ sessionId: id, resourceId: 'r2' })).rejects.toBeInstanceOf(
      HarnessSessionNotFoundError,
    );

    const stillOpen = await harness2.loadSession({ sessionId: id, includeClosed: true });
    expect(stillOpen?.closedAt).toBeUndefined();

    await expect(harness2.closeSession({ sessionId: id, resourceId: 'r1' })).resolves.toBeUndefined();
    const closed = await harness2.loadSession({ sessionId: id, includeClosed: true });
    expect(closed?.closedAt).toBeDefined();
  });

  it('cascades close to direct child sessions', async () => {
    const storage = makeStorage();
    const harness = makeHarness({ sessions: { storage } });

    const parent = await harness.session({ threadId: 't1', resourceId: 'r1' });
    const child = await harness.session({
      threadId: { fresh: true },
      resourceId: 'r1',
      parentSessionId: parent.id,
    });

    await parent.close();

    expect(child.isClosed || harness._internalLiveSessionCount() === 0).toBe(true);

    const stored = await harness.loadSession({ sessionId: child.id, includeClosed: true });
    expect(stored?.closedAt).toBeDefined();
  });

  it('listSessions hides closed records by default and surfaces them with includeClosed', async () => {
    const harness = makeHarness();
    const s1 = await harness.session({ threadId: 't1', resourceId: 'r1' });
    const s2 = await harness.session({ threadId: 't2', resourceId: 'r1' });
    await s1.close();

    const active = await harness.listSessions({ resourceId: 'r1' });
    expect(active.map(r => r.id)).toEqual([s2.id]);

    const all = await harness.listSessions({ resourceId: 'r1', includeClosed: true });
    expect(all.map(r => r.id).sort()).toEqual([s1.id, s2.id].sort());
  });
});

describe('Harness v1 — lease + write concurrency', () => {
  it('rejects a second harness trying to acquire a session held by the first', async () => {
    const storage = makeStorage();
    const a = makeHarness({ sessions: { storage } });
    const b = makeHarness({ sessions: { storage } });

    const session = await a.session({ threadId: 't1', resourceId: 'r1' });

    await expect(b.session({ sessionId: session.id })).rejects.toThrow(HarnessSessionLockedError);
  });

  it('lets a second harness take over after the first releases via shutdown', async () => {
    const storage = makeStorage();
    const a = makeHarness({ sessions: { storage } });
    const b = makeHarness({ sessions: { storage } });

    const session = await a.session({ threadId: 't1', resourceId: 'r1' });
    const id = session.id;
    await a.shutdown();

    const taken = await b.session({ sessionId: id });
    expect(taken.id).toBe(id);
  });

  it('shutdown is idempotent', async () => {
    const harness = makeHarness();
    await harness.session({ threadId: 't1', resourceId: 'r1' });
    await harness.shutdown();
    await expect(harness.shutdown()).resolves.toBeUndefined();
  });

  it('rejects new session() calls after shutdown', async () => {
    const harness = makeHarness();
    await harness.shutdown();
    await expect(harness.session({ threadId: 't1', resourceId: 'r1' })).rejects.toThrow();
  });
});

describe('Harness v1 — config validation surfaces in resolver', () => {
  it('throws when no agents/storage/mastra are supplied and a session is requested', async () => {
    // Three-shape constructor: passing nothing keeps the harness deferred-bound,
    // expecting a parent Mastra to call __registerMastra. If neither happens,
    // session() must surface the misconfiguration with a clear error.
    const harness = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });
    await expect(harness.session({ threadId: 't1', resourceId: 'r1' })).rejects.toThrow(HarnessConfigError);
  });

  it('defaults to InMemoryStore when agents are supplied without storage', async () => {
    // Standalone construction with agents-only must still work end-to-end —
    // both the harness storage domain and the memory storage domain (used by
    // thread CRUD) are provided by the default InMemoryStore.
    const harness = new Harness({
      agents: { default: makeAgent() },
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });
    const session = await harness.session({ threadId: 't1', resourceId: 'r1' });
    expect(session.id).toMatch(/^sess-/);
  });
});

describe('Harness v1 — deterministic-id branch (§5.3)', () => {
  it('mints a record using the caller-supplied sessionId when none exists', async () => {
    const harness = makeHarness();
    const s = await harness.session({ threadId: 't1', resourceId: 'r1', sessionId: 'sess-explicit-1' });
    expect(s.id).toBe('sess-explicit-1');
    expect(s.threadId).toBe('t1');
  });

  it('returns the existing record when sessionId matches the live instance', async () => {
    const harness = makeHarness();
    const a = await harness.session({ threadId: 't1', resourceId: 'r1', sessionId: 'sess-explicit-2' });
    const b = await harness.session({ threadId: 't1', resourceId: 'r1', sessionId: 'sess-explicit-2' });
    expect(b).toBe(a);
  });

  it('returns the active record when caller-supplied sessionId disagrees with the active one', async () => {
    const harness = makeHarness();
    const first = await harness.session({ threadId: 't1', resourceId: 'r1' });
    const second = await harness.session({ threadId: 't1', resourceId: 'r1', sessionId: 'sess-different' });
    expect(second.id).toBe(first.id);
    expect(second.threadId).toBe('t1');
  });

  it('rehydrates the active thread record when caller-supplied sessionId is stale', async () => {
    const storage = makeStorage();
    const harness = makeHarness({ sessions: { storage } });
    const first = await harness.session({ threadId: 't1', resourceId: 'r1' });
    await harness.shutdown();

    const restarted = makeHarness({ sessions: { storage } });
    const resolved = await restarted.session({ threadId: 't1', resourceId: 'r1', sessionId: 'sess-stale' });

    expect(resolved.id).toBe(first.id);
    expect(resolved.threadId).toBe('t1');
  });

  it('returns an existing active child when the parent closes after thread lookup misses', async () => {
    const storage = makeStorage();
    const harness = makeHarness({ sessions: { storage } });
    const parent = await harness.session({ threadId: 'parent-thread', resourceId: 'r1', sessionId: 'parent' });
    const now = Date.now();
    await storage.createOrLoadActiveSession(
      {
        ...parent.getRecord(),
        id: 'existing-child',
        threadId: 'child-thread',
        parentSessionId: parent.id,
        origin: 'subagent-tool',
        subagentDepth: 1,
        createdAt: now,
        lastActivityAt: now,
        version: 0,
        ownerId: harness.ownerId,
        leaseExpiresAt: now + 30_000,
      },
      { initialLease: { ownerId: harness.ownerId, ttlMs: 30_000 } },
    );

    const originalLoadSessionByThread = storage.loadSessionByThread.bind(storage);
    let forcedThreadMiss = false;
    storage.loadSessionByThread = (async (...args: Parameters<typeof storage.loadSessionByThread>) => {
      const [opts] = args;
      if (opts.threadId === 'child-thread' && opts.resourceId === 'r1' && !forcedThreadMiss) {
        forcedThreadMiss = true;
        return null;
      }
      return originalLoadSessionByThread(...args);
    }) as typeof storage.loadSessionByThread;

    const originalSaveSession = storage.saveSession.bind(storage);
    let releaseClosingMarker!: () => void;
    let closingMarkerStarted!: () => void;
    const closingMarkerGate = new Promise<void>(resolve => {
      releaseClosingMarker = resolve;
    });
    const closingMarkerSeen = new Promise<void>(resolve => {
      closingMarkerStarted = resolve;
    });
    storage.saveSession = (async (...args: Parameters<typeof storage.saveSession>) => {
      const [record] = args;
      if (record.id === parent.id && record.closingAt !== undefined && record.closedAt === undefined) {
        closingMarkerStarted();
        await closingMarkerGate;
      }
      return originalSaveSession(...args);
    }) as typeof storage.saveSession;

    const closing = parent.close();
    await closingMarkerSeen;

    const child = await harness.session({
      threadId: 'child-thread',
      resourceId: 'r1',
      parentSessionId: parent.id,
      sessionId: 'retry-child',
      origin: 'subagent-tool',
    });

    expect(child.id).toBe('existing-child');
    expect(forcedThreadMiss).toBe(true);
    await expect(storage.loadSession({ sessionId: 'retry-child', harnessName: 'default' })).resolves.toBeNull();
    releaseClosingMarker();
    await closing;
  });
});

describe('Harness v1 — deep cascade', () => {
  it('cascades close through a parent → child → grandchild chain', async () => {
    const storage = makeStorage();
    const harness = makeHarness({ sessions: { storage } });

    const parent = await harness.session({ threadId: 't1', resourceId: 'r1' });
    const child = await harness.session({
      threadId: { fresh: true },
      resourceId: 'r1',
      parentSessionId: parent.id,
    });
    const grandchild = await harness.session({
      threadId: { fresh: true },
      resourceId: 'r1',
      parentSessionId: child.id,
    });

    await parent.close();

    const storedChild = await harness.loadSession({ sessionId: child.id, includeClosed: true });
    const storedGrand = await harness.loadSession({ sessionId: grandchild.id, includeClosed: true });
    expect(storedChild?.closedAt).toBeDefined();
    expect(storedGrand?.closedAt).toBeDefined();
    expect(harness._internalLiveSessionCount()).toBe(0);
  });
});

describe('Harness v1 — delete lifecycle', () => {
  it('blocks non-force delete while the target subtree is still active', async () => {
    const storage = makeStorage();
    const harness = makeHarness({ sessions: { storage } });
    const parent = await harness.session({ threadId: 'parent-thread', resourceId: 'r1' });
    const child = await harness.session({
      threadId: { fresh: true },
      resourceId: 'r1',
      parentSessionId: parent.id,
    });

    await expect(harness.deleteSession({ sessionId: parent.id, resourceId: 'r1' })).rejects.toMatchObject({
      name: 'HarnessSessionDeleteBlockedError',
      sessionId: parent.id,
      blockers: expect.arrayContaining([
        expect.objectContaining({ source: 'session', id: parent.id, status: 'not_closed' }),
        expect.objectContaining({ source: 'child_session', id: child.id, status: 'not_closed' }),
      ]),
    } satisfies Partial<HarnessSessionDeleteBlockedError>);
    await expect(storage.loadSession({ sessionId: parent.id, harnessName: 'default' })).resolves.not.toBeNull();
    await expect(storage.loadSession({ sessionId: child.id, harnessName: 'default' })).resolves.not.toBeNull();
  });

  it('blocks non-force delete for completed queue receipts until post-run finalization is marked', async () => {
    const storage = makeStorage();
    const harness = makeHarness({ sessions: { storage } });
    const session = await harness.session({ threadId: { fresh: true }, resourceId: 'r1' });
    await session.close();
    const stored = (await storage.loadSession({ sessionId: session.id, harnessName: 'default' }))!;
    const now = Date.now();

    await storage.saveSession(
      {
        ...stored,
        queueAdmissionReceipts: {
          queued: {
            admissionId: 'admission-1',
            admissionHash: 'hash-1',
            queuedItemId: 'queued',
            status: 'completed',
            result: { text: 'done' },
            attempts: 1,
            enqueuedAt: now,
            completedAt: now,
            updatedAt: now,
          },
        },
      },
      { harnessName: 'default', ownerId: stored.ownerId ?? 'h', ifVersion: stored.version },
    );

    await expect(harness.deleteSession({ sessionId: session.id, resourceId: 'r1' })).rejects.toMatchObject({
      blockers: expect.arrayContaining([
        expect.objectContaining({ source: 'queue', id: 'queued', status: 'completed' }),
      ]),
    } satisfies Partial<HarnessSessionDeleteBlockedError>);
  });

  it('non-force deletes an already closed subtree and owned attachments bottom-up', async () => {
    const storage = makeStorage();
    const harness = makeHarness({ sessions: { storage } });
    const parent = await harness.session({ threadId: 'parent-thread', resourceId: 'r1' });
    const child = await harness.session({
      threadId: { fresh: true },
      resourceId: 'r1',
      parentSessionId: parent.id,
    });
    const attachment = await harness.attachments.upload({
      sessionId: child.id,
      resourceId: 'r1',
      data: Buffer.from('delete me'),
      filename: 'delete.txt',
      contentType: 'text/plain',
    });

    await parent.close();
    await harness.deleteSession({ sessionId: parent.id, resourceId: 'r1' });

    await expect(storage.loadSession({ sessionId: parent.id, harnessName: 'default' })).resolves.toBeNull();
    await expect(storage.loadSession({ sessionId: child.id, harnessName: 'default' })).resolves.toBeNull();
    await expect(
      storage.getAttachmentRecord({
        harnessName: 'default',
        sessionId: child.id,
        attachmentId: attachment.attachmentId,
      }),
    ).resolves.toBeNull();
  });

  it('does not hard-delete when storage observes a stale closed-tree version', async () => {
    const storage = makeStorage();
    const harness = makeHarness({ sessions: { storage } });
    const parent = await harness.session({ threadId: 'parent-thread', resourceId: 'r1' });
    const child = await harness.session({
      threadId: { fresh: true },
      resourceId: 'r1',
      parentSessionId: parent.id,
    });
    await parent.close();
    const originalDeleteSessions = storage.deleteSessions.bind(storage);
    let mutated = false;
    storage.deleteSessions = (async (...args: Parameters<typeof storage.deleteSessions>) => {
      if (!mutated) {
        mutated = true;
        const current = await storage.loadSession({ harnessName: 'default', sessionId: parent.id });
        if (!current) throw new Error('expected session before guarded delete');
        await storage.saveSession(
          {
            ...current,
            state: { raced: true },
          },
          { harnessName: 'default', ownerId: current.ownerId ?? 'racer', ifVersion: current.version },
        );
      }
      return originalDeleteSessions(...args);
    }) as typeof storage.deleteSessions;

    await expect(harness.deleteSession({ sessionId: parent.id, resourceId: 'r1' })).rejects.toBeInstanceOf(
      HarnessStorageVersionConflictError,
    );
    await expect(storage.loadSession({ sessionId: parent.id, harnessName: 'default' })).resolves.toMatchObject({
      id: parent.id,
      state: { raced: true },
    });
    await expect(storage.loadSession({ sessionId: child.id, harnessName: 'default' })).resolves.toMatchObject({
      id: child.id,
    });
  });

  it('keeps legacy deleteSession fallback behavior for adapters without batch delete override', async () => {
    class LegacyDeleteSessionStorage extends InMemoryHarness {
      override get supportsAtomicDeleteSessions(): boolean {
        return false;
      }

      override async deleteSession(opts: Parameters<InMemoryHarness['deleteSession']>[0]): Promise<void> {
        await InMemoryHarness.prototype.deleteSessions.call(this, { sessions: [opts] });
      }

      override async deleteSessions(opts: Parameters<HarnessStorage['deleteSessions']>[0]): Promise<void> {
        await HarnessStorage.prototype.deleteSessions.call(this, opts);
      }
    }
    const storage = new LegacyDeleteSessionStorage({ db: new InMemoryDB() });
    const harness = makeHarness({ sessions: { storage } });
    const parent = await harness.session({ threadId: 'parent-thread', resourceId: 'r1' });
    const child = await harness.session({
      threadId: { fresh: true },
      resourceId: 'r1',
      parentSessionId: parent.id,
    });

    await parent.close();
    await expect(
      storage.deleteSessions({
        sessions: [
          { sessionId: parent.id, requireClosed: true },
          { sessionId: child.id, requireClosed: true },
        ],
      }),
    ).rejects.toThrow('HarnessStorage.deleteSessions must be overridden');
    await harness.deleteSession({ sessionId: parent.id, resourceId: 'r1' });

    await expect(storage.loadSession({ sessionId: child.id, harnessName: 'default' })).resolves.toBeNull();
    await expect(storage.loadSession({ sessionId: parent.id, harnessName: 'default' })).resolves.toBeNull();
  });

  it('reconciles live sessions after a custom batch delete partially fails', async () => {
    class PartiallyFailingBatchStorage extends InMemoryHarness {
      override async deleteSessions(opts: Parameters<InMemoryHarness['deleteSessions']>[0]): Promise<void> {
        await InMemoryHarness.prototype.deleteSessions.call(this, { sessions: [opts.sessions[0]!] });
        throw new HarnessStorageDeleteGuardConflictError(opts.sessions[1]!.sessionId, 'ifVersion', 1, 2);
      }
    }
    const storage = new PartiallyFailingBatchStorage({ db: new InMemoryDB() });
    const harness = makeHarness({ sessions: { storage } });
    const parent = await harness.session({ threadId: 'parent-thread', resourceId: 'r1' });
    const child = await harness.session({
      threadId: { fresh: true },
      resourceId: 'r1',
      parentSessionId: parent.id,
    });

    await expect(harness.deleteSession({ sessionId: parent.id, resourceId: 'r1', force: true })).rejects.toBeInstanceOf(
      HarnessStorageDeleteGuardConflictError,
    );

    await expect(storage.loadSession({ sessionId: child.id, harnessName: 'default' })).resolves.toBeNull();
    await expect(storage.loadSession({ sessionId: parent.id, harnessName: 'default' })).resolves.toMatchObject({
      id: parent.id,
    });
    await expect(child.setState({ afterPartialDelete: true })).rejects.toBeInstanceOf(HarnessSessionDeletedError);
  });

  it('preserves the original batch delete error when reconciliation reads fail', async () => {
    const deleteError = new HarnessStorageDeleteGuardConflictError('parent', 'ifVersion', 1, 2);
    class ReconciliationReadFailingStorage extends InMemoryHarness {
      private failLoads = false;

      override async deleteSessions(opts: Parameters<InMemoryHarness['deleteSessions']>[0]): Promise<void> {
        await InMemoryHarness.prototype.deleteSessions.call(this, { sessions: [opts.sessions[0]!] });
        this.failLoads = true;
        throw deleteError;
      }

      override async loadSession(opts: Parameters<InMemoryHarness['loadSession']>[0]) {
        if (this.failLoads) throw new Error('load failed during reconciliation');
        return InMemoryHarness.prototype.loadSession.call(this, opts);
      }
    }
    const storage = new ReconciliationReadFailingStorage({ db: new InMemoryDB() });
    const harness = makeHarness({ sessions: { storage } });
    const parent = await harness.session({ threadId: 'parent-thread', resourceId: 'r1' });
    await harness.session({
      threadId: { fresh: true },
      resourceId: 'r1',
      parentSessionId: parent.id,
    });

    await expect(harness.deleteSession({ sessionId: parent.id, resourceId: 'r1', force: true })).rejects.toBe(
      deleteError,
    );
  });

  it('force deletes an active subtree after terminalizing it through close', async () => {
    const storage = makeStorage();
    const harness = makeHarness({ sessions: { storage } });
    const parent = await harness.session({ threadId: 'parent-thread', resourceId: 'r1' });
    const child = await harness.session({
      threadId: { fresh: true },
      resourceId: 'r1',
      parentSessionId: parent.id,
    });

    await harness.deleteSession({ sessionId: parent.id, resourceId: 'r1', force: true });

    await expect(storage.loadSession({ sessionId: parent.id, harnessName: 'default' })).resolves.toBeNull();
    await expect(storage.loadSession({ sessionId: child.id, harnessName: 'default' })).resolves.toBeNull();
    await expect(parent.setState({ afterDelete: true })).rejects.toBeInstanceOf(HarnessSessionDeletedError);
    expect(harness._internalLiveSessionCount()).toBe(0);
  });

  it('aborts an active turn when hard-delete marks a live session deleted', async () => {
    class GatedEvidenceStorage extends InMemoryHarness {
      gatedSignalId?: string;
      private gated = false;
      private releasedDuringDeleteCleanup = false;
      private release!: () => void;
      private start!: () => void;
      private finish!: () => void;
      readonly started = new Promise<void>(resolve => {
        this.start = resolve;
      });
      readonly finished = new Promise<void>(resolve => {
        this.finish = resolve;
      });
      private readonly gate = new Promise<void>(resolve => {
        this.release = resolve;
      });

      override async writeMessageResultEvidence(
        record: Parameters<InMemoryHarness['writeMessageResultEvidence']>[0],
      ): Promise<{ created: boolean }> {
        if (!this.gated && record.status === 'pending') {
          this.gated = true;
          this.gatedSignalId = record.signalId;
          this.start();
          await this.gate;
        }
        try {
          return await super.writeMessageResultEvidence(record);
        } finally {
          if (record.signalId === this.gatedSignalId) this.finish();
        }
      }

      override async deleteOperationAdmissionTombstonesForSession(
        opts: Parameters<InMemoryHarness['deleteOperationAdmissionTombstonesForSession']>[0],
      ): Promise<void> {
        await super.deleteOperationAdmissionTombstonesForSession(opts);
        if (!this.releasedDuringDeleteCleanup && this.gatedSignalId !== undefined) {
          this.releasedDuringDeleteCleanup = true;
          this.release();
          await this.finished;
        }
      }
    }
    const storage = new GatedEvidenceStorage({ db: new InMemoryDB() });
    const agent = new MockAgent({ id: 'default' });
    const hold = deferred();
    agent.enqueueRun({
      holdUntil: hold.promise,
    });
    const harness = new Harness({
      agents: { default: agent } as any,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage },
    });
    const session = await harness.session({ threadId: { fresh: true }, resourceId: 'r1' });
    const message = session.message({ content: 'slow', admissionId: 'delete-active-turn' });
    const messageSettled = message.then(
      value => ({ ok: true as const, value }),
      err => ({ ok: false as const, err }),
    );
    await waitFor(() => session.isRunning(), 'active turn start');
    await storage.started;
    const idleSettled = session.waitForIdle().then(
      () => ({ ok: true as const }),
      err => ({ ok: false as const, err }),
    );

    const stored = (await storage.loadSession({ sessionId: session.id, harnessName: 'default' }))!;
    // Simulate an out-of-band close marker so this live instance reaches hard-delete while still running.
    await storage.saveSession(
      {
        ...stored,
        closedAt: Date.now(),
      },
      { harnessName: 'default', ownerId: harness.ownerId, ifVersion: stored.version },
    );

    await harness.deleteSession({ sessionId: session.id, resourceId: 'r1' });

    expect(session.isRunning()).toBe(false);
    await expect(idleSettled).resolves.toMatchObject({ ok: false, err: expect.any(HarnessSessionDeletedError) });
    await expect(messageSettled).resolves.toMatchObject({ ok: false, err: expect.any(HarnessSessionDeletedError) });
    await storage.finished;
    await expect(storage.loadSession({ sessionId: session.id, harnessName: 'default' })).resolves.toBeNull();
    await expect(
      storage.loadMessageResultEvidence({
        sessionId: session.id,
        resourceId: session.resourceId,
        threadId: session.threadId,
        signalId: storage.gatedSignalId!,
      }),
    ).resolves.toBeNull();
    await expect(session.setState({ afterDelete: true })).rejects.toBeInstanceOf(HarnessSessionDeletedError);
  });

  it('rejects an active structured sync turn when hard-delete marks a live session deleted', async () => {
    const storage = makeStorage();
    const agent = new BlockingGenerateMockAgent({ id: 'default' });
    const harness = new Harness({
      agents: { default: agent } as any,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage },
    });
    const session = await harness.session({ threadId: { fresh: true }, resourceId: 'r1' });
    const messageRejected = expect(
      session.message({ content: 'slow', output: z.object({ ok: z.boolean() }), sync: true }),
    ).rejects.toBeInstanceOf(HarnessSessionDeletedError);
    await waitFor(() => session.isRunning(), 'structured turn start');
    const idleRejected = expect(session.waitForIdle()).rejects.toBeInstanceOf(HarnessSessionDeletedError);

    const stored = (await storage.loadSession({ sessionId: session.id, harnessName: 'default' }))!;
    await storage.saveSession(
      {
        ...stored,
        closedAt: Date.now(),
      },
      { harnessName: 'default', ownerId: harness.ownerId, ifVersion: stored.version },
    );

    await harness.deleteSession({ sessionId: session.id, resourceId: 'r1' });

    expect(session.isRunning()).toBe(false);
    await messageRejected;
    await idleRejected;
  });

  it('aborts an active streamed turn when hard-delete marks a live session deleted', async () => {
    const storage = makeStorage();
    const writeMessageResultEvidence = storage.writeMessageResultEvidence.bind(storage);
    let signalId: string | undefined;
    storage.writeMessageResultEvidence = async record => {
      if (record.admissionId === 'delete-active-stream') {
        signalId = record.signalId;
      }
      return writeMessageResultEvidence(record);
    };
    const agent = new MockAgent({ id: 'default' });
    const hold = deferred();
    agent.enqueueRun({ holdUntil: hold.promise, text: 'streamed after delete' });
    const harness = new Harness({
      agents: { default: agent } as any,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage },
    });
    const session = await harness.session({ threadId: { fresh: true }, resourceId: 'r1' });

    await session.message({ content: 'slow', admissionId: 'delete-active-stream', stream: true });
    await waitFor(() => session.isRunning() && signalId !== undefined, 'stream turn start');
    const idleSettled = session.waitForIdle().then(
      () => ({ ok: true as const }),
      err => ({ ok: false as const, err }),
    );

    const stored = (await storage.loadSession({ sessionId: session.id, harnessName: 'default' }))!;
    await storage.saveSession(
      {
        ...stored,
        closedAt: Date.now(),
      },
      { harnessName: 'default', ownerId: harness.ownerId, ifVersion: stored.version },
    );

    await harness.deleteSession({ sessionId: session.id, resourceId: 'r1' });

    expect(session.isRunning()).toBe(false);
    await expect(idleSettled).resolves.toMatchObject({ ok: false, err: expect.any(HarnessSessionDeletedError) });
    await expect(
      storage.loadMessageResultEvidence({
        sessionId: session.id,
        resourceId: session.resourceId,
        threadId: session.threadId,
        signalId: signalId!,
      }),
    ).resolves.toBeNull();
    hold.resolve();
    await new Promise(resolve => setImmediate(resolve));
  });

  it('scopes late deleted-session evidence cleanup to the deleted turn identity', async () => {
    const storage = makeStorage();
    const harness = makeHarness({ sessions: { storage } });
    const oldSession = await harness.session({ threadId: 'old-thread', resourceId: 'r1', sessionId: 'sess-reused' });
    const now = Date.now();
    await storage.writeMessageResultEvidence({
      harnessName: 'default',
      sessionId: oldSession.id,
      resourceId: oldSession.resourceId,
      threadId: oldSession.threadId,
      signalId: 'old-signal',
      status: 'pending',
      createdAt: now,
      updatedAt: now,
    });
    await storage.writeMessageResultEvidence({
      harnessName: 'default',
      sessionId: oldSession.id,
      resourceId: oldSession.resourceId,
      threadId: oldSession.threadId,
      signalId: 'same-thread-new-signal',
      status: 'pending',
      createdAt: now,
      updatedAt: now,
    });
    await storage.writeMessageResultEvidence({
      harnessName: 'default',
      sessionId: oldSession.id,
      resourceId: oldSession.resourceId,
      threadId: 'new-thread',
      signalId: 'new-signal',
      status: 'pending',
      createdAt: now,
      updatedAt: now,
    });

    (oldSession as any)._markDeleted();
    await (oldSession as any)._cleanupOperationEvidenceIfDeleted({ signalId: 'old-signal' });

    await expect(
      storage.loadMessageResultEvidence({
        harnessName: 'default',
        sessionId: oldSession.id,
        resourceId: oldSession.resourceId,
        threadId: oldSession.threadId,
        signalId: 'old-signal',
      }),
    ).resolves.toBeNull();
    await expect(
      storage.loadMessageResultEvidence({
        harnessName: 'default',
        sessionId: oldSession.id,
        resourceId: oldSession.resourceId,
        threadId: oldSession.threadId,
        signalId: 'same-thread-new-signal',
      }),
    ).resolves.toMatchObject({ status: 'pending' });
    await expect(
      storage.loadMessageResultEvidence({
        harnessName: 'default',
        sessionId: oldSession.id,
        resourceId: oldSession.resourceId,
        threadId: 'new-thread',
        signalId: 'new-signal',
      }),
    ).resolves.toMatchObject({ status: 'pending' });
  });

  it('marks live descendants created during force-delete discovery as deleted', async () => {
    const storage = makeStorage();
    const harness = makeHarness({ sessions: { storage } });
    const parent = await harness.session({ threadId: 'parent-thread', resourceId: 'r1' });
    const originalListSessions = storage.listSessions.bind(storage);
    let child: Awaited<ReturnType<typeof harness.session>> | undefined;
    let injected = false;
    storage.listSessions = (async (...args: Parameters<typeof storage.listSessions>) => {
      const [opts] = args;
      const result = await originalListSessions(...args);
      if (!injected && opts.parentSessionId === parent.id && opts.includeClosed === true) {
        injected = true;
        child = await harness.session({
          threadId: { fresh: true },
          resourceId: 'r1',
          parentSessionId: parent.id,
        });
      }
      return result;
    }) as typeof storage.listSessions;

    await harness.deleteSession({ sessionId: parent.id, resourceId: 'r1', force: true });

    expect(child).toBeDefined();
    await expect(storage.loadSession({ sessionId: child!.id, harnessName: 'default' })).resolves.toBeNull();
    await expect(child!.setState({ afterDelete: true })).rejects.toBeInstanceOf(HarnessSessionDeletedError);
    expect(harness._internalLiveSessionCount()).toBe(0);
  });

  it('does not flush a queued turn after force delete removes the row', async () => {
    const storage = makeStorage();
    const agent = new AbortIgnoringMockAgent({ id: 'default' });
    const hold = deferred();
    agent.enqueueRun({ holdUntil: hold.promise, text: 'queued after delete' });
    const harness = new Harness({
      agents: { default: agent } as any,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage, closeTimeoutMs: 1 },
    });
    const session = await harness.session({ threadId: { fresh: true }, resourceId: 'r1' });
    const queued = session.queue({ content: 'queued' });
    void queued.catch(() => {});
    await waitFor(() => agent.streamCalls.length === 1, 'queued run start');

    await harness.deleteSession({ sessionId: session.id, resourceId: 'r1', force: true });
    expect(session.isRunning()).toBe(false);
    await expect(storage.loadSession({ sessionId: session.id, harnessName: 'default' })).resolves.toBeNull();

    hold.resolve();
    await expect(queued).rejects.toBeInstanceOf(HarnessSessionClosingError);
    await new Promise(resolve => setImmediate(resolve));
  });

  it('rejects an active queued turn when hard-delete marks the live session deleted', async () => {
    const storage = makeStorage();
    const agent = new AbortIgnoringMockAgent({ id: 'default' });
    const hold = deferred();
    agent.enqueueRun({ holdUntil: hold.promise, text: 'queued after delete' });
    const harness = new Harness({
      agents: { default: agent } as any,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage },
    });
    const session = await harness.session({ threadId: { fresh: true }, resourceId: 'r1' });
    const queued = session.queue({ content: 'queued' });
    const queuedSettled = queued.then(
      value => ({ ok: true as const, value }),
      err => ({ ok: false as const, err }),
    );
    await waitFor(() => agent.streamCalls.length === 1 && session.isRunning(), 'queued run start');
    const idleSettled = session.waitForIdle().then(
      () => ({ ok: true as const }),
      err => ({ ok: false as const, err }),
    );

    (session as any)._markDeleted();
    expect(session.isRunning()).toBe(false);

    hold.resolve();
    await expect(queuedSettled).resolves.toMatchObject({ ok: false, err: expect.any(HarnessSessionDeletedError) });
    await expect(idleSettled).resolves.toMatchObject({ ok: false, err: expect.any(HarnessSessionDeletedError) });
    await new Promise(resolve => setImmediate(resolve));
  });

  it('does not leak existence across resources during delete', async () => {
    const storage = makeStorage();
    const harness = makeHarness({ sessions: { storage } });
    const session = await harness.session({ threadId: 't1', resourceId: 'r1' });

    await expect(
      harness.deleteSession({ sessionId: session.id, resourceId: 'r2', force: true }),
    ).rejects.toBeInstanceOf(HarnessSessionNotFoundError);
    await expect(storage.loadSession({ sessionId: session.id, harnessName: 'default' })).resolves.not.toBeNull();
  });

  it('rechecks resource scope before committing a force delete', async () => {
    const storage = makeStorage();
    const harness = makeHarness({ sessions: { storage } });
    const session = await harness.session({ threadId: 't1', resourceId: 'r1' });
    const originalLoadSession = storage.loadSession.bind(storage);
    let loads = 0;
    storage.loadSession = (async (...args: Parameters<typeof storage.loadSession>) => {
      const record = await originalLoadSession(...args);
      if (args[0].sessionId === session.id) {
        loads += 1;
        if (loads === 2 && record) {
          return { ...record, resourceId: 'r2' };
        }
      }
      return record;
    }) as typeof storage.loadSession;

    await expect(
      harness.deleteSession({ sessionId: session.id, resourceId: 'r1', force: true }),
    ).rejects.toBeInstanceOf(HarnessSessionNotFoundError);
    const stored = await originalLoadSession({ sessionId: session.id, harnessName: 'default' });
    expect(stored).not.toBeNull();
    expect(stored?.closedAt).toBeUndefined();
  });
});

describe('Harness v1 — crash recovery (lease TTL)', () => {
  it('lets a fresh harness take over once the prior owner lease has expired', async () => {
    // Build storage with a db handle we can poke directly to age out the lease.
    const db = new InMemoryDB();
    const storage = new InMemoryHarness({ db });

    const a = makeHarness({ sessions: { storage } });
    const session = await a.session({ threadId: 't1', resourceId: 'r1' });
    const id = session.id;

    // Simulate process crash: lease still held, no graceful shutdown. Force
    // expiry by rewriting the lease window into the past directly in the
    // backing db (saveSession preserves lease metadata so we cannot do this
    // through the public storage API).
    const stored = db.harnessSessions.get(`default\u0000${id}`);
    if (!stored) throw new Error('precondition: session must exist after crash');
    db.harnessSessions.set(`default\u0000${id}`, { ...stored, leaseExpiresAt: Date.now() - 60_000 });

    const b = makeHarness({ sessions: { storage } });
    const taken = await b.session({ sessionId: id });
    expect(taken.id).toBe(id);
    expect(taken._internalOwnerId).toBe(b.ownerId);
  });
});

describe('Harness v1 — concurrent resolver race', () => {
  // §5.3 cold-resolution singleflight. Two CONCURRENT session() calls for the
  // SAME not-yet-live target (same harness instance) must collapse onto one
  // hydrate/adopt and receive the SAME Session. Without the singleflight both
  // calls miss _liveSessions, both cold-hydrate, both acquire the lease under
  // the same ownerId (CAS does not conflict for the same owner), and the second
  // _adoptSession overwrites the first's event bridge — leaking a subscription
  // and leaving two live Session objects for one row with racing flush chains.

  it('collapses two parallel session({ resourceId, threadId }) cold creates to one Session + one bridge', async () => {
    const harness = makeHarness();
    const [a, b] = await Promise.all([
      harness.session({ threadId: 't-race', resourceId: 'r1' }),
      harness.session({ threadId: 't-race', resourceId: 'r1' }),
    ]);
    // Same instance — not just same id (would double-adopt without the fix).
    expect(a).toBe(b);
    // Exactly one live session + exactly one event bridge for the row.
    expect(harness._internalLiveSessionCount()).toBe(1);
    expect(harness._internalSessionEventBridgeCount(a.id)).toBe(1);
  });

  it('collapses three parallel session({ resourceId, threadId }) cold creates to one Session', async () => {
    const harness = makeHarness();
    const [a, b, c] = await Promise.all([
      harness.session({ threadId: 't-race3', resourceId: 'r1' }),
      harness.session({ threadId: 't-race3', resourceId: 'r1' }),
      harness.session({ threadId: 't-race3', resourceId: 'r1' }),
    ]);
    expect(a).toBe(b);
    expect(b).toBe(c);
    expect(harness._internalLiveSessionCount()).toBe(1);
    expect(harness._internalSessionEventBridgeCount(a.id)).toBe(1);
  });

  it('collapses two parallel session({ sessionId }) cold hydrates of a stored-but-not-live row to one Session + one bridge', async () => {
    const storage = makeStorage();
    const harness = makeHarness({ sessions: { storage } });
    const t0 = Date.now();
    // A row owned by THIS harness with a live lease, but never adopted into
    // _liveSessions — exactly the "not-yet-live" state two cold resolves race on.
    await storage.saveSession(
      {
        id: 'sess-cold',
        harnessName: 'default',
        resourceId: 'r1',
        threadId: 't-cold',
        origin: 'top-level',
        ownsThread: false,
        modeId: 'default',
        modelId: 'default',
        subagentModelOverrides: {},
        permissionRules: { categories: {}, tools: {} },
        sessionGrants: { categories: [], tools: [] },
        tokenUsage: { promptTokens: 0, completionTokens: 0, totalTokens: 0 },
        pendingQueue: [],
        state: undefined,
        version: 0,
        createdAt: t0,
        lastActivityAt: t0,
        ownerId: harness.ownerId,
        leaseExpiresAt: t0 + 60_000,
      },
      { harnessName: 'default', ifVersion: 0 },
    );

    const [a, b] = await Promise.all([
      harness.session({ sessionId: 'sess-cold' }),
      harness.session({ sessionId: 'sess-cold' }),
    ]);
    expect(a).toBe(b);
    expect(a.id).toBe('sess-cold');
    expect(harness._internalLiveSessionCount()).toBe(1);
    expect(harness._internalSessionEventBridgeCount('sess-cold')).toBe(1);
  });

  it('does not falsely dedupe DISTINCT sessions resolved in parallel', async () => {
    const harness = makeHarness();
    const [a, b] = await Promise.all([
      harness.session({ threadId: 't-distinct-a', resourceId: 'r1' }),
      harness.session({ threadId: 't-distinct-b', resourceId: 'r1' }),
    ]);
    expect(a).not.toBe(b);
    expect(a.id).not.toBe(b.id);
    expect(harness._internalLiveSessionCount()).toBe(2);
  });

  it('does not dedupe { fresh: true } — each fresh call mints its own session', async () => {
    const harness = makeHarness();
    const [a, b] = await Promise.all([
      harness.session({ threadId: { fresh: true }, resourceId: 'r1' }),
      harness.session({ threadId: { fresh: true }, resourceId: 'r1' }),
    ]);
    expect(a).not.toBe(b);
    expect(a.id).not.toBe(b.id);
    expect(a.threadId).not.toBe(b.threadId);
    expect(harness._internalLiveSessionCount()).toBe(2);
  });

  it('clears the in-flight entry on failure so a retry can proceed', async () => {
    const harness = makeHarness();
    // First cold resolve fails (unknown sessionId). The singleflight entry must
    // be cleared in finally so a subsequent resolve is not served the cached
    // rejected promise.
    await expect(harness.session({ sessionId: 'ghost' })).rejects.toThrow(HarnessSessionNotFoundError);
    await expect(harness.session({ sessionId: 'ghost' })).rejects.toThrow(HarnessSessionNotFoundError);
  });

  it('serialises lease acquisition per session — distinct sessions resolve in parallel', async () => {
    // A single harness owns its leases; parallel resolves for *different*
    // threads must not block each other behind a shared mutex. This catches
    // accidental global locking around _initSession.
    const harness = makeHarness();
    const [a, b, c] = await Promise.all([
      harness.session({ threadId: 't1', resourceId: 'r1' }),
      harness.session({ threadId: 't2', resourceId: 'r1' }),
      harness.session({ threadId: 't3', resourceId: 'r1' }),
    ]);
    expect(new Set([a.id, b.id, c.id]).size).toBe(3);
  });

  it('rejects a deterministic-id collision between two harnesses with HarnessSessionLockedError', async () => {
    // Two harnesses race to insert the same caller-supplied sessionId. The
    // loser's saveSession() sees a version mismatch and translates it into
    // HarnessSessionLockedError — the resolver does not silently downgrade
    // to "load the existing record" because that would steal the lease.
    const storage = makeStorage();
    const a = makeHarness({ sessions: { storage } });
    const b = makeHarness({ sessions: { storage } });

    await a.session({ threadId: 't1', resourceId: 'r1', sessionId: 'sess-shared' });
    await expect(b.session({ threadId: 't1', resourceId: 'r1', sessionId: 'sess-shared' })).rejects.toThrow(
      HarnessSessionLockedError,
    );
  });

  it('exposes holder + expiry fields on HarnessSessionLockedError', async () => {
    // Holders need to know who owns the lease and when it expires so they
    // can decide whether to wait, retry, or steal. Assert the error carries
    // the contract we promise in §4.5.
    const storage = makeStorage();
    const a = makeHarness({ sessions: { storage } });
    const b = makeHarness({ sessions: { storage } });

    const owner = await a.session({ threadId: 't1', resourceId: 'r1' });

    let captured: HarnessSessionLockedError | undefined;
    try {
      await b.session({ sessionId: owner.id });
    } catch (err) {
      captured = err as HarnessSessionLockedError;
    }
    expect(captured).toBeInstanceOf(HarnessSessionLockedError);
    expect(captured!.sessionId).toBe(owner.id);
    expect(captured!.currentOwnerId).toBe(a.ownerId);
    expect(typeof captured!.expiresAt).toBe('number');
    expect(captured!.expiresAt).toBeGreaterThan(Date.now());
  });
});

describe('Session — identity + lifecycle', () => {
  it('exposes id, threadId, resourceId, createdAt as readonly identity', async () => {
    const harness = makeHarness();
    const s = await harness.session({ threadId: 't1', resourceId: 'r1' });
    expect(typeof s.id).toBe('string');
    expect(s.threadId).toBe('t1');
    expect(s.resourceId).toBe('r1');
    expect(typeof s.createdAt).toBe('number');
    expect(s.parentSessionId).toBeUndefined();
  });

  it('records parentSessionId when spawned as a child', async () => {
    const harness = makeHarness();
    const parent = await harness.session({ threadId: 't1', resourceId: 'r1' });
    const child = await harness.session({
      threadId: { fresh: true },
      resourceId: 'r1',
      parentSessionId: parent.id,
    });
    expect(child.parentSessionId).toBe(parent.id);
  });

  it('flips lifecycleState from "live" to "closed" on close()', async () => {
    const harness = makeHarness();
    const s = await harness.session({ threadId: 't1', resourceId: 'r1' });
    expect(s.lifecycleState).toBe('live');
    expect(s.isClosed).toBe(false);
    await s.close();
    expect(s.lifecycleState).toBe('closed');
    expect(s.isClosed).toBe(true);
  });

  it('getRecord() returns a snapshot reflecting the persisted record', async () => {
    const harness = makeHarness();
    const s = await harness.session({ threadId: 't1', resourceId: 'r1' });
    const rec = s.getRecord();
    expect(rec.id).toBe(s.id);
    expect(rec.threadId).toBe('t1');
    expect(rec.resourceId).toBe('r1');
    expect(rec.closedAt).toBeUndefined();
  });
});

describe('Harness._resolveChannelBindingForIngress (§14.1 / C2b)', () => {
  function makeIngressCtx(overrides: Record<string, unknown> = {}) {
    return {
      harnessName: 'primary',
      channelId: 'support',
      providerId: 'slack',
      platform: 'slack',
      conversationKind: 'channel',
      trigger: 'message',
      externalTenantId: 'T1',
      externalChannelId: 'C1',
      externalThreadId: 'TS1',
      externalMessageId: 'M1',
      content: 'hello',
      receivedAt: 1000,
      ...overrides,
    } as never;
  }

  function setup(channelOverrides: Record<string, unknown> = {}) {
    const storage = makeStorage();
    const harness = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage },
      channels: { support: makeHarnessChannelConfig(channelOverrides) },
    });
    new Mastra({
      agents: { default: makeAgent() },
      storage: new InMemoryStore(),
      channels: { slack: makeChannelProvider('slack') },
      harnesses: { primary: harness },
    });
    return { harness, storage };
  }

  type ResolveResult = {
    binding: { id: string; status: string; generation: number; mode: string; lastInboundAt?: number };
    resolved: { resourceId: string; threadId: string; sessionId: string; mode: string };
  };

  it('resolves a new active binding, deriving stable namespaced thread/session ids', async () => {
    const { harness } = setup();
    const r = (await (
      harness as never as {
        _resolveChannelBindingForIngress: (c: never) => Promise<ResolveResult>;
      }
    )._resolveChannelBindingForIngress(makeIngressCtx())) as ResolveResult;
    expect(r.resolved.resourceId).toBe('resource-1');
    expect(r.resolved.threadId).toMatch(/^ch:/);
    expect(r.resolved.sessionId).toMatch(/^chs:/);
    expect(r.resolved.mode).toBe('shared-resource');
    expect(r.binding).toMatchObject({ status: 'active', generation: 1, mode: 'shared-resource' });

    // Stability: a fresh harness with the same inputs derives the SAME ids.
    const { harness: h2 } = setup();
    const r2 = (await (
      h2 as never as {
        _resolveChannelBindingForIngress: (c: never) => Promise<ResolveResult>;
      }
    )._resolveChannelBindingForIngress(makeIngressCtx())) as ResolveResult;
    expect(r2.resolved.threadId).toBe(r.resolved.threadId);
    expect(r2.resolved.sessionId).toBe(r.resolved.sessionId);

    // Separation: a different resolved resourceId derives a DIFFERENT threadId.
    const { harness: h3 } = setup({
      ingress: { resolveResource: async () => ({ resourceId: 'other', mode: 'shared-resource' }) },
    });
    const r3 = (await (
      h3 as never as {
        _resolveChannelBindingForIngress: (c: never) => Promise<ResolveResult>;
      }
    )._resolveChannelBindingForIngress(makeIngressCtx())) as ResolveResult;
    expect(r3.resolved.threadId).not.toBe(r.resolved.threadId);
  });

  it('§14.1: a missing optional external id does NOT collide with a literal single space', async () => {
    // Residual #3: `normChannelExternalId` must use the same out-of-band sentinel
    // as the storage tuple key. A provider may legitimately send a single-space
    // externalChannelId; if `undefined` normalised to `' '` (the old behaviour),
    // an absent id and a literal space would derive the SAME thread/session id and
    // collide at the channel level even though storage keeps them distinct.
    const call = (h: Harness) =>
      (
        h as never as {
          _resolveChannelBindingForIngress: (c: never) => Promise<ResolveResult>;
        }
      )._resolveChannelBindingForIngress.bind(h) as (c: never) => Promise<ResolveResult>;

    const { harness: hMissing } = setup();
    const missing = await call(hMissing)(makeIngressCtx({ externalChannelId: undefined }));

    const { harness: hSpace } = setup();
    const space = await call(hSpace)(makeIngressCtx({ externalChannelId: ' ' }));

    // Absent external id and a literal single space derive DISTINCT ids.
    expect(missing.resolved.threadId).not.toBe(space.resolved.threadId);
    expect(missing.resolved.sessionId).not.toBe(space.resolved.sessionId);

    // …and each remains stable on its own inputs (no accidental cross-talk).
    const { harness: hMissing2 } = setup();
    const missing2 = await call(hMissing2)(makeIngressCtx({ externalChannelId: undefined }));
    expect(missing2.resolved.threadId).toBe(missing.resolved.threadId);
  });

  it('idempotently reuses the active binding and advances lastInboundAt only forward', async () => {
    const { harness } = setup();
    const call = (
      harness as never as {
        _resolveChannelBindingForIngress: (c: never) => Promise<ResolveResult>;
      }
    )._resolveChannelBindingForIngress.bind(harness) as (c: never) => Promise<ResolveResult>;
    const first = await call(makeIngressCtx({ receivedAt: 1000 }));
    const second = await call(makeIngressCtx({ receivedAt: 2000 }));
    expect(second.binding.id).toBe(first.binding.id); // same active binding, not a duplicate
    expect(second.binding.lastInboundAt).toBe(2000);
    // A delayed/out-of-order OLDER ingress must NOT regress the activity marker.
    const stale = await call(makeIngressCtx({ receivedAt: 500 }));
    expect(stale.binding.lastInboundAt).toBe(2000);
  });

  it("maps a 'custom' policy mode to the durable 'shared-resource' mode (§5.1h)", async () => {
    const { harness } = setup({
      ingress: { resolveResource: async () => ({ resourceId: 'r', mode: 'custom' }) },
    });
    const r = (await (
      harness as never as {
        _resolveChannelBindingForIngress: (c: never) => Promise<ResolveResult>;
      }
    )._resolveChannelBindingForIngress(makeIngressCtx())) as ResolveResult;
    expect(r.binding.mode).toBe('shared-resource');
  });

  it('rejects a policy that returns sessionId without threadId (§14.1 no mispairing)', async () => {
    const { harness } = setup({
      ingress: { resolveResource: async () => ({ resourceId: 'r', sessionId: 's1', mode: 'shared-resource' }) },
    });
    await expect(
      (
        harness as never as { _resolveChannelBindingForIngress: (c: never) => Promise<unknown> }
      )._resolveChannelBindingForIngress(makeIngressCtx()),
    ).rejects.toBeInstanceOf(HarnessValidationError);
  });
});

describe('Harness.admitChannelInbound (§14.2 ingress admission core / C4)', () => {
  function makeIngressCtx(overrides: Record<string, unknown> = {}) {
    return {
      harnessName: 'primary',
      channelId: 'support',
      providerId: 'slack',
      platform: 'slack',
      conversationKind: 'channel',
      trigger: 'message',
      externalTenantId: 'T1',
      externalChannelId: 'C1',
      externalThreadId: 'TS1',
      externalMessageId: 'M1',
      content: 'hello from channel',
      receivedAt: 1000,
      ...overrides,
    } as never;
  }

  function setup(
    channelOverrides: Partial<HarnessChannelConfig> = {},
    sessionsOverrides: Record<string, unknown> = {},
  ) {
    // Shared storage: the harness session storage IS the Mastra's harness store
    // instance, so `_usesSeparateSessionStorage()` is false and session creation
    // for a fresh channel conversation is not blocked by the separate-storage
    // thread-must-exist guard. (Channel ingress under SEPARATE session storage
    // needs a resolver create-with-explicit-id capability — C4 follow-up.)
    const composite = new InMemoryStore();
    const storage = composite.stores.harness as ReturnType<typeof makeStorage>;
    const harness = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage, ...sessionsOverrides },
      channels: { support: makeHarnessChannelConfig(channelOverrides) },
    });
    new Mastra({
      agents: { default: makeAgent() },
      storage: composite,
      channels: { slack: makeChannelProvider('slack') },
      harnesses: { primary: harness },
    });
    // Binding auto-starts the §14.2 recovery worker loop; stop it so these tests
    // drive recovery explicitly (via recoverChannelInboxOnce) without a background
    // tick racing their assertions. The loop itself is covered by dedicated
    // scheduler tests below.
    (harness as unknown as { _stopChannelInboxRecoveryLoop: () => void })._stopChannelInboxRecoveryLoop();
    return { harness, storage };
  }

  type AdmitResult = {
    inboxItemId: string;
    status: string;
    binding?: { id: string };
    sessionId?: string;
    queuedItemId?: string;
    duplicate: boolean;
  };

  it('records the inbox row, resolves a binding+session, and admits the turn via queue', async () => {
    const { harness } = setup();
    const events: Array<{ type: string; inboxItemId?: string; delivery?: string }> = [];
    harness.subscribe(e => {
      if (e.type.startsWith('channel_ingress_')) events.push(e as never);
    });

    const r = (await (
      harness as never as {
        admitChannelInbound: (c: never) => Promise<AdmitResult>;
      }
    ).admitChannelInbound(makeIngressCtx())) as AdmitResult;

    expect(r.status).toBe('queued');
    expect(r.duplicate).toBe(false);
    expect(r.binding?.id).toMatch(/^binding-/);
    expect(r.sessionId).toMatch(/^chs:/);
    expect(typeof r.queuedItemId).toBe('string');
    expect(events.map(e => e.type)).toEqual(['channel_ingress_received', 'channel_ingress_admitted']);
    expect(events[1]).toMatchObject({ delivery: 'queue', inboxItemId: r.inboxItemId });
  });

  it('is idempotent for a provider retry (same external message) — no double admission', async () => {
    const { harness } = setup();
    const events: string[] = [];
    harness.subscribe(e => {
      if (e.type.startsWith('channel_ingress_')) events.push(e.type);
    });
    const call = (
      harness as never as { admitChannelInbound: (c: never) => Promise<AdmitResult> }
    ).admitChannelInbound.bind(harness) as (c: never) => Promise<AdmitResult>;
    const first = await call(makeIngressCtx());
    const second = await call(makeIngressCtx()); // same externalMessageId → same inbox row
    expect(second.duplicate).toBe(true);
    expect(second.status).toBe('queued');
    expect(second.inboxItemId).toBe(first.inboxItemId);
    // The retry does not re-emit received/admitted (already admitted).
    expect(events).toEqual(['channel_ingress_received', 'channel_ingress_admitted']);
  });

  it('threads policy-selected admission mode/model through the persisted admissionHash without conflict', async () => {
    // §14.2 step 7: the bridge computes the admissionHash over the EXACT payload
    // (content + persisted attachments + policy mode/model + requestContext) and
    // persists it BEFORE admitting, then replays it as expectedAdmissionHash. If
    // the pre-computed hash and the queue-boundary recompute disagree (e.g. a field
    // is dropped on one side), _admitQueue throws HarnessAdmissionConflictError.
    // A clean 'queued' result proves the payload threads consistently.
    const { harness } = setup({
      ingress: {
        resolveResource: async () => ({
          resourceId: 'resource-1',
          mode: 'shared-resource',
          admission: { mode: 'default', model: 'model-x' },
        }),
      },
    });
    const r = (await (
      harness as never as {
        admitChannelInbound: (c: never) => Promise<AdmitResult>;
      }
    ).admitChannelInbound(makeIngressCtx())) as AdmitResult;
    expect(r.status).toBe('queued');
    expect(typeof r.queuedItemId).toBe('string');
  });

  it('preserves the recovery-complete admission on a failure AFTER the admitted write', async () => {
    // Model a crash AFTER the queue append but BEFORE the 'queued' status commits:
    // make the 'queued' inbox write throw. The failure lands after the 'admitted'
    // write committed admissionHash/bindingId/resolved ids, so the 'failed' row
    // must RETAIN that recovery-complete admission (catch spreads the latest
    // committed row, not the stale pre-admission row) — recovery then replays the
    // persisted payload and never re-runs policy.
    const { harness, storage } = setup();
    const realUpdate = storage.updateChannelInboxItem.bind(storage);
    (storage as { updateChannelInboxItem: unknown }).updateChannelInboxItem = async (
      row: ChannelInboxItem,
      opts: { claimId: string },
    ) => {
      if (row.status === 'queued') throw new Error('crash before queued commit');
      return realUpdate(row, opts);
    };
    const events: Array<{ type: string; inboxItemId?: string }> = [];
    harness.subscribe(e => {
      if (e.type.startsWith('channel_ingress_')) events.push(e as never);
    });
    const call = (
      harness as never as { admitChannelInbound: (c: never) => Promise<AdmitResult> }
    ).admitChannelInbound.bind(harness) as (c: never) => Promise<AdmitResult>;

    await expect(call(makeIngressCtx())).rejects.toThrow();
    expect(events.map(e => e.type)).toEqual(['channel_ingress_received', 'channel_ingress_failed']);

    const failed = await storage.claimChannelInboxItems({
      harnessName: 'primary',
      channelId: 'support',
      statuses: ['failed'],
      claimId: 'recovery-claim',
      limit: 10,
      now: 4_000_000_000_000,
      claimTtlMs: 30_000,
    });
    expect(failed).toHaveLength(1);
    const row = failed[0]!;
    expect(row.status).toBe('failed');
    // Recovery-complete admission survived the failure (NOT reset to the
    // pre-admission shape): admissionHash + binding + resolved ids + bound
    // requestContext are all still present.
    expect(typeof row.admissionHash).toBe('string');
    expect(row.bindingId).toMatch(/^binding-/);
    expect(row.resourceId).toBe('resource-1');
    expect(typeof row.threadId).toBe('string');
    expect(row.sessionId).toMatch(/^chs:/);
    expect(row.delivery).toBe('queue');
    expect(row.requestContext.channel?.bindingId).toBe(row.bindingId);
    // It also carries the failure evidence the worker needs.
    expect(row.lastError).toMatchObject({ code: 'unknown', retryable: true });
  });

  it('persists a durable failed inbox row (with lastError) and emits channel_ingress_failed when admission errors', async () => {
    // Force the admission body to throw after the row is created (resolveResource
    // rejects). The catch must record status:'failed' + lastError on the row, NOT
    // leave it stranded at 'received'. (Regression guard: a failed update without
    // lastError throws inside the in-memory store and is swallowed.)
    const { harness, storage } = setup({
      ingress: {
        resolveResource: async () => {
          throw new Error('policy boom');
        },
      },
    });
    const events: Array<{ type: string; inboxItemId?: string; error?: { code: string; message: string } }> = [];
    harness.subscribe(e => {
      if (e.type.startsWith('channel_ingress_')) events.push(e as never);
    });

    const call = (
      harness as never as { admitChannelInbound: (c: never) => Promise<AdmitResult> }
    ).admitChannelInbound.bind(harness) as (c: never) => Promise<AdmitResult>;

    // The local thrown error still carries the raw resolver text (it stays a
    // local-only cause); only the PUBLIC projection is redacted.
    await expect(call(makeIngressCtx())).rejects.toThrow(/policy boom/);

    // received emitted, then failed (not admitted).
    expect(events.map(e => e.type)).toEqual(['channel_ingress_received', 'channel_ingress_failed']);
    // §13.3f.1: a raw (non-namespaced) resolver Error must NOT leak its message
    // or its `err.name` code onto the public channel_ingress_failed event. It is
    // redacted to the reserved `harness.internal` catch-all with a generic message.
    expect(events[1]?.error).toEqual({ code: 'harness.internal', message: 'An internal harness error occurred' });
    expect(events[1]?.error?.message).not.toMatch(/policy boom/);
    const failedItemId = events[1]?.inboxItemId;
    expect(typeof failedItemId).toBe('string');

    // The durable row transitioned to 'failed' and carries diagnostic lastError.
    // The recovery worker can reclaim it once the original admission claim lapses
    // (the admission claim was stamped with wall-clock Date.now(), so the reclaim
    // `now` must sit past that expiry — 'failed' is non-terminal, so it is eligible).
    const claimed = await storage.claimChannelInboxItems({
      harnessName: 'primary',
      channelId: 'support',
      statuses: ['failed'],
      claimId: 'recovery-claim',
      limit: 10,
      now: 4_000_000_000_000,
      claimTtlMs: 30_000,
    });
    expect(claimed).toHaveLength(1);
    expect(claimed[0]?.id).toBe(failedItemId);
    expect(claimed[0]?.status).toBe('failed');
    // The durable row keeps the bare `code: 'unknown'` (row-side readability,
    // §13.3f.1), but its stored message is the redacted public projection — the
    // raw resolver text never reaches the durable failure receipt either.
    expect(claimed[0]?.lastError).toMatchObject({ code: 'unknown', retryable: true });
    expect(claimed[0]?.lastError?.message).toBe('An internal harness error occurred');
    expect(claimed[0]?.lastError?.message).not.toMatch(/policy boom/);
  });

  it('backs off (in-progress duplicate) when the failed row still holds a live claim — does not steal it', async () => {
    // After a failed admission the durable row is 'failed' but its admission claim
    // is still live (claimExpiresAt = wall-clock now + TTL). A synchronous
    // re-delivery of the same provider event within that window must NOT steal the
    // claim and re-run admission (that would claim-conflict and emit a false
    // failure). It reports the existing non-terminal row as an in-progress
    // duplicate, leaving recovery to the worker after the claim lapses.
    // (Exercises the `!created.claimed` branch with real calls — no internal
    // idempotency-key reconstruction needed.)
    let boom = true;
    const { harness } = setup({
      ingress: {
        resolveResource: async () => {
          if (boom) throw new Error('transient resolver outage');
          return { resourceId: 'resource-1', mode: 'shared-resource' };
        },
      },
    });
    const call = (
      harness as never as { admitChannelInbound: (c: never) => Promise<AdmitResult> }
    ).admitChannelInbound.bind(harness) as (c: never) => Promise<AdmitResult>;

    await expect(call(makeIngressCtx())).rejects.toThrow(/transient resolver outage/);

    // Heal the resolver and re-deliver the SAME provider event immediately. The
    // failed row's claim is still live, so admission backs off rather than retrying
    // synchronously (within-TTL recovery is the worker's job, not the ingress call).
    boom = false;
    const events: string[] = [];
    harness.subscribe(e => {
      if (e.type.startsWith('channel_ingress_')) events.push(e.type);
    });
    const replay = await call(makeIngressCtx());
    expect(replay.duplicate).toBe(true);
    expect(replay.status).toBe('failed'); // unchanged — not re-admitted under the live claim
    // No received/admitted/failed re-emitted — we did not steal the live claim.
    expect(events).toEqual([]);
  });

  // ——— §14.2 recovery worker (recoverChannelInboxOnce) ———

  type RecoverResult = { claimed: number; queued: number; failed: number; dead: number };
  const WORKER_NOW = 4_000_000_000_000; // past any wall-clock admission-claim expiry

  it('worker resumes a row that failed AFTER admit by replaying the persisted admission (admissionHash present)', async () => {
    // Crash the original admission after the queue append but before the 'queued'
    // write, leaving a 'failed' row that is recovery-complete (admissionHash +
    // resolved ids). The worker takes the replay-only path: re-admit with the
    // stored payload + expectedAdmissionHash → queue de-dupes on admissionId → row
    // reaches 'queued' without re-running policy or double-queuing.
    const { harness, storage } = setup();
    const realUpdate = storage.updateChannelInboxItem.bind(storage);
    (storage as { updateChannelInboxItem: unknown }).updateChannelInboxItem = async (
      row: ChannelInboxItem,
      opts: { claimId: string },
    ) => {
      if (row.status === 'queued') throw new Error('crash before queued commit');
      return realUpdate(row, opts);
    };
    const call = (
      harness as never as { admitChannelInbound: (c: never) => Promise<AdmitResult> }
    ).admitChannelInbound.bind(harness) as (c: never) => Promise<AdmitResult>;
    await expect(call(makeIngressCtx())).rejects.toThrow();
    // Heal storage so recovery can commit.
    (storage as { updateChannelInboxItem: unknown }).updateChannelInboxItem = realUpdate;

    const res = (await harness.recoverChannelInboxOnce({ channelId: 'support', now: WORKER_NOW })) as RecoverResult;
    expect(res).toMatchObject({ claimed: 1, queued: 1, failed: 0, dead: 0 });
  });

  it('worker resumes a row that failed BEFORE binding resolution by re-running resolution (no admissionHash)', async () => {
    // First admission throws in resolveResource → 'failed' row with NO admissionHash
    // (policy never completed). The worker reconstructs the ChannelIngressContext
    // from the persisted envelope (conversationKind/trigger) and resumes from
    // binding resolution → admitted → queued.
    let boom = true;
    const { harness } = setup({
      ingress: {
        resolveResource: async () => {
          if (boom) throw new Error('resolver outage');
          return { resourceId: 'resource-1', mode: 'shared-resource' };
        },
      },
    });
    const call = (
      harness as never as { admitChannelInbound: (c: never) => Promise<AdmitResult> }
    ).admitChannelInbound.bind(harness) as (c: never) => Promise<AdmitResult>;
    await expect(call(makeIngressCtx())).rejects.toThrow(/resolver outage/);

    boom = false;
    const res = (await harness.recoverChannelInboxOnce({ channelId: 'support', now: WORKER_NOW })) as RecoverResult;
    expect(res).toMatchObject({ claimed: 1, queued: 1, failed: 0, dead: 0 });
  });

  it('worker dead-letters a row already at maxAttempts (no further admission attempt)', async () => {
    const { harness, storage } = setup();
    await storage.saveChannelInboxItem(
      channelInboxRow({
        id: 'inbox-dead-1',
        status: 'failed',
        attempts: 10, // == default maxAttempts
        claimId: undefined,
        claimExpiresAt: undefined,
        admissionHash: 'persisted-hash',
        externalMessageId: 'm-dead',
      }),
    );
    const failedEvents: Array<{ type: string }> = [];
    harness.subscribe(e => {
      if (e.type === 'channel_ingress_failed') failedEvents.push(e as never);
    });
    const res = (await harness.recoverChannelInboxOnce({ channelId: 'support', now: WORKER_NOW })) as RecoverResult;
    expect(res).toMatchObject({ claimed: 1, queued: 0, failed: 0, dead: 1 });
    expect(failedEvents).toHaveLength(1);
  });

  it('worker is a no-op when there are no claimable rows', async () => {
    const { harness } = setup();
    const res = (await harness.recoverChannelInboxOnce({ channelId: 'support', now: WORKER_NOW })) as RecoverResult;
    expect(res).toEqual({ claimed: 0, queued: 0, accepted: 0, failed: 0, dead: 0 });
  });

  // ——— §14.2 error taxonomy ———
  // Drive a 'received'/'failed' (no-admissionHash) row, then recover with a
  // resolveResource that throws a specific error so the worker's classifier maps
  // it to the durable failure shape (status / bare lastError.code / nextAttemptAt).

  async function recoverAfterThrow(err: unknown): Promise<{
    res: RecoverResult;
    storage: ReturnType<typeof makeStorage>;
    failedEvents: Array<{ error?: { code: string; message: string } }>;
  }> {
    const { harness, storage } = setup({
      ingress: {
        resolveResource: async () => {
          throw err;
        },
      },
    });
    const call = (
      harness as never as { admitChannelInbound: (c: never) => Promise<AdmitResult> }
    ).admitChannelInbound.bind(harness) as (c: never) => Promise<AdmitResult>;
    // First admission fails → durable 'failed' row (no admissionHash).
    await expect(call(makeIngressCtx())).rejects.toThrow();
    const failedEvents: Array<{ error?: { code: string; message: string } }> = [];
    harness.subscribe(e => {
      if (e.type === 'channel_ingress_failed') failedEvents.push(e as never);
    });
    const res = (await harness.recoverChannelInboxOnce({ channelId: 'support', now: WORKER_NOW })) as RecoverResult;
    return { res, storage, failedEvents };
  }

  async function claimFailedRow(storage: ReturnType<typeof makeStorage>) {
    // Inspect the recovered row past its backoff `nextAttemptAt` + worker claim.
    const rows = await storage.claimChannelInboxItems({
      harnessName: 'primary',
      channelId: 'support',
      statuses: ['failed'],
      claimId: 'inspect',
      limit: 10,
      now: WORKER_NOW + 1_000_000,
      claimTtlMs: 1_000,
    });
    return rows[0];
  }

  it.each([
    [
      'HarnessSessionLockedError',
      () => new HarnessSessionLockedError('chs:x', 'owner-y', WORKER_NOW + 1),
      'session_locked',
    ],
    ['HarnessLiveSessionLimitError', () => new HarnessLiveSessionLimitError(2, 2), 'live_session_limit'],
    ['HarnessQueueFullError', () => new HarnessQueueFullError('chs:x', 0, 0), 'queue_full'],
  ])('classifies %s as retryable failed with its bare code + backoff', async (_name, makeErr, code) => {
    const { res, storage } = await recoverAfterThrow(makeErr());
    expect(res).toMatchObject({ failed: 1, dead: 0, queued: 0 });
    const row = await claimFailedRow(storage);
    expect(row?.status).toBe('failed');
    expect(row?.lastError).toMatchObject({ code, retryable: true });
    expect(row?.nextAttemptAt).toBeGreaterThan(WORKER_NOW); // backoff scheduled
  });

  it.each([
    ['HarnessSessionClosedError', () => new HarnessSessionClosedError('chs:x'), 'session_closed'],
    [
      'HarnessSessionDeletedError',
      () => new HarnessSessionDeletedError('chs:x', 'resource-1', 'thread-1'),
      'session_deleted',
    ],
    [
      'HarnessOverrideConflictError',
      () => new HarnessOverrideConflictError('chs:x', 'run-1', ['mode']),
      'override_conflict',
    ],
    // Unrecoverable row-shape / invalid-policy resolution → terminal, not retried.
    ['HarnessValidationError', () => new HarnessValidationError('field', 'bad'), 'unknown'],
  ])('classifies %s as terminal dead (operator repair), ignoring maxAttempts', async (_name, makeErr) => {
    const { res, failedEvents } = await recoverAfterThrow(makeErr());
    expect(res).toMatchObject({ dead: 1, failed: 0, queued: 0 });
    expect(failedEvents).toHaveLength(1); // the worker still surfaces a failure event
    expect(failedEvents[0]?.error?.code).toBeTruthy(); // carries the projected error identity
  });

  it('clamps a session_closing failed row’s nextAttemptAt to closeDeadlineAt (deadline in the future)', async () => {
    const deadline = WORKER_NOW + 1_000; // sooner than the ~2s+ exponential backoff for attempt 1
    const { res, storage } = await recoverAfterThrow(
      new HarnessSessionClosingError('chs:x', WORKER_NOW - 1_000, deadline),
    );
    expect(res).toMatchObject({ failed: 1, dead: 0 });
    const row = await claimFailedRow(storage);
    expect(row?.lastError).toMatchObject({ code: 'session_closing', retryable: true });
    expect(row?.nextAttemptAt).toBe(deadline); // clamped to the deadline, not now+backoff
  });

  it('dead-letters a session_closing row once the close deadline has passed', async () => {
    const { res } = await recoverAfterThrow(
      new HarnessSessionClosingError('chs:x', WORKER_NOW - 10_000, WORKER_NOW - 1_000), // deadline already past
    );
    expect(res).toMatchObject({ dead: 1, failed: 0 });
  });

  // ——— §14.2 recovery worker scheduler (start-on-bind / tick / stop) ———

  type RecoveryTimerProbe = { _channelInboxRecoveryTimer?: unknown; _tickChannelInboxRecovery: () => Promise<void> };
  const probe = (h: unknown) => h as unknown as RecoveryTimerProbe;

  // A harness bound WITHOUT stopping the recovery loop (the C4 setup stops it).
  function makeBoundHarness(channels?: Record<string, HarnessChannelConfig>) {
    const composite = new InMemoryStore();
    const harness = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage: composite.stores.harness },
      ...(channels ? { channels } : {}),
    });
    new Mastra({
      agents: { default: makeAgent() },
      storage: composite,
      channels: { slack: makeChannelProvider('slack') },
      harnesses: { primary: harness },
    });
    return harness;
  }

  it('starts the recovery loop on bind when channels are configured, and stops it on shutdown', async () => {
    const harness = makeBoundHarness({ support: makeHarnessChannelConfig() });
    expect(probe(harness)._channelInboxRecoveryTimer).toBeDefined();
    await harness.shutdown();
    expect(probe(harness)._channelInboxRecoveryTimer).toBeUndefined();
  });

  it('does NOT start the recovery loop when no channels are configured', async () => {
    const harness = makeBoundHarness(); // no channels
    expect(probe(harness)._channelInboxRecoveryTimer).toBeUndefined();
    await harness.shutdown();
  });

  it('restarts the recovery loop when shutdown rolls back on a drain failure (PF-812)', async () => {
    const harness = makeBoundHarness({ support: makeHarnessChannelConfig() });
    const session = await harness.session({ threadId: 't-rollback', resourceId: 'u' });
    expect(probe(harness)._channelInboxRecoveryTimer).toBeDefined();

    // Force the shutdown drain to fail once so shutdown() rolls back: it restores
    // live sessions + lease renewal, and (PF-812) must also resume the channel
    // recovery worker rather than leaving channel recovery permanently dead.
    vi.spyOn(
      session as unknown as { _flushEventPersistence: () => Promise<void> },
      '_flushEventPersistence',
    ).mockRejectedValueOnce(new Error('event persistence failed'));
    await expect(harness.shutdown()).rejects.toBeInstanceOf(HarnessStorageError);

    expect(probe(harness)._channelInboxRecoveryTimer).toBeDefined();
    expect(harness.channelWorkerReadiness({ channelId: 'support' })).toEqual({ ready: true });

    await harness.shutdown(); // clean shutdown now succeeds (spy exhausted)
    expect(probe(harness)._channelInboxRecoveryTimer).toBeUndefined();
  });

  it('tick runs one recovery pass per configured channel', async () => {
    const harness = makeBoundHarness({ a: makeHarnessChannelConfig(), b: makeHarnessChannelConfig() });
    probe(harness)._channelInboxRecoveryTimer &&
      (harness as unknown as { _stopChannelInboxRecoveryLoop: () => void })._stopChannelInboxRecoveryLoop();
    const spy = vi
      .spyOn(
        harness as unknown as { recoverChannelInboxOnce: (o: unknown) => Promise<unknown> },
        'recoverChannelInboxOnce',
      )
      .mockResolvedValue({ claimed: 0, queued: 0, failed: 0, dead: 0 });
    await probe(harness)._tickChannelInboxRecovery();
    const channelArgs = spy.mock.calls.map(c => (c[0] as { channelId: string }).channelId).sort();
    expect(channelArgs).toEqual(['a', 'b']);
    await harness.shutdown();
  });

  it('tick is reentrancy-guarded (a concurrent pass is a no-op)', async () => {
    const { harness } = setup(); // loop already stopped by setup
    (harness as unknown as { _channelInboxRecoveryRunning: boolean })._channelInboxRecoveryRunning = true;
    const spy = vi.spyOn(
      harness as unknown as { recoverChannelInboxOnce: () => Promise<unknown> },
      'recoverChannelInboxOnce',
    );
    await probe(harness)._tickChannelInboxRecovery();
    expect(spy).not.toHaveBeenCalled();
  });

  // ——— §14.2 claim-renewal heartbeat ———

  type HeartbeatProbe = {
    _withChannelInboxClaimHeartbeat: <T>(
      storage: unknown,
      item: ChannelInboxItem,
      claimId: string,
      claimTtlMs: number,
      claimRenewMs: number,
      operation: () => Promise<T>,
    ) => Promise<T>;
  };

  it('renews the inbox claim on the heartbeat interval while a slow operation runs', async () => {
    const { harness, storage } = setup();
    const renewSpy = vi
      .spyOn(storage, 'renewChannelInboxClaim')
      .mockResolvedValue({ claimExpiresAt: 99_999, storageNow: 1 });
    const result = await (harness as unknown as HeartbeatProbe)._withChannelInboxClaimHeartbeat(
      storage,
      channelInboxRow({ id: 'hb-1' }),
      'claim-x',
      30_000,
      5, // renew every 5ms
      async () => {
        await new Promise(r => setTimeout(r, 40)); // slow enough for the 5ms heartbeat to fire
        return 'done';
      },
    );
    expect(result).toBe('done');
    expect(renewSpy.mock.calls.length).toBeGreaterThanOrEqual(1);
    expect(renewSpy).toHaveBeenCalledWith(
      expect.objectContaining({ inboxItemId: 'hb-1', claimId: 'claim-x', claimTtlMs: 30_000 }),
    );
  });

  it('treats a heartbeat renewal failure as benign (does not fail the operation)', async () => {
    // A renewal that fails is NOT surfaced as an operation error — a genuine claim
    // loss is detected by the operation's own claim-checked writes (which throw),
    // and a renewal that fails because the row already committed terminal must not
    // produce a false failure. The heartbeat just stops renewing.
    const { harness, storage } = setup();
    const renewSpy = vi
      .spyOn(storage, 'renewChannelInboxClaim')
      .mockRejectedValue(new Error('claim lost to another worker'));
    const result = await (harness as unknown as HeartbeatProbe)._withChannelInboxClaimHeartbeat(
      storage,
      channelInboxRow({ id: 'hb-2' }),
      'claim-x',
      30_000,
      5,
      async () => {
        await new Promise(r => setTimeout(r, 40));
        return 'done';
      },
    );
    expect(result).toBe('done'); // benign — no throw
    expect(renewSpy).toHaveBeenCalled(); // it tried, then stopped after the failure
    expect(renewSpy.mock.calls.length).toBeLessThan(8); // stopped renewing (not every 5ms for 40ms)
  });

  // ——— §14.2 record-only admission (continueAdmission:false / ACK-after-received) ———

  it('records + ACKs without admitting (continueAdmission:false); the recovery worker completes it', async () => {
    const { harness } = setup();
    const events: string[] = [];
    harness.subscribe(e => {
      if (e.type.startsWith('channel_ingress_')) events.push(e.type);
    });
    const admit = (
      harness as never as {
        admitChannelInbound: (c: never, o: { continueAdmission: boolean }) => Promise<AdmitResult>;
      }
    ).admitChannelInbound.bind(harness) as (c: never, o: { continueAdmission: boolean }) => Promise<AdmitResult>;

    const r = await admit(makeIngressCtx(), { continueAdmission: false });
    expect(r.status).toBe('received');
    expect(r.duplicate).toBe(false);
    expect(r.queuedItemId).toBeUndefined();
    expect(r.binding).toBeUndefined();
    expect(events).toEqual(['channel_ingress_received']); // recorded, NOT admitted

    // The 'received' row (claim released) is completed by the recovery worker.
    const res = (await harness.recoverChannelInboxOnce({ channelId: 'support', now: WORKER_NOW })) as RecoverResult;
    expect(res).toMatchObject({ claimed: 1, queued: 1 });
  });

  it('record-only is idempotent: a duplicate provider retry reports the same received row, still un-admitted', async () => {
    const { harness } = setup();
    const admit = (
      harness as never as {
        admitChannelInbound: (c: never, o: { continueAdmission: boolean }) => Promise<AdmitResult>;
      }
    ).admitChannelInbound.bind(harness) as (c: never, o: { continueAdmission: boolean }) => Promise<AdmitResult>;

    const first = await admit(makeIngressCtx(), { continueAdmission: false }); // records + releases claim
    const events: string[] = [];
    harness.subscribe(e => {
      if (e.type.startsWith('channel_ingress_')) events.push(e.type);
    });
    // A duplicate provider retry (same external message) re-claims the released
    // 'received' row, reports it as a duplicate, and re-releases — no admission.
    const dup = await admit(makeIngressCtx(), { continueAdmission: false });
    expect(dup.inboxItemId).toBe(first.inboxItemId);
    expect(dup.status).toBe('received');
    expect(dup.duplicate).toBe(true);
    expect(events).toEqual([]); // no new received/admitted events on the duplicate
  });

  // ——— §13.6 worker-readiness gate ———

  it('reports ready when the recovery worker loop is running', () => {
    const harness = makeBoundHarness({ support: makeHarnessChannelConfig() });
    expect(harness.channelWorkerReadiness({ channelId: 'support' })).toEqual({ ready: true });
    return harness.shutdown();
  });

  it('reports worker_not_started when the loop is stopped, draining on shutdown, and not_configured for an unknown channel', async () => {
    const harness = makeBoundHarness({ support: makeHarnessChannelConfig() });
    (harness as unknown as { _stopChannelInboxRecoveryLoop: () => void })._stopChannelInboxRecoveryLoop();
    expect(harness.channelWorkerReadiness({ channelId: 'support' })).toEqual({
      ready: false,
      reason: 'worker_not_started',
    });
    expect(harness.channelWorkerReadiness({ channelId: 'nope' })).toEqual({
      ready: false,
      reason: 'channel_not_configured',
    });
    await harness.shutdown();
    expect(harness.channelWorkerReadiness({ channelId: 'support' })).toEqual({
      ready: false,
      reason: 'server_draining',
    });
  });
});

describe('Harness.admitChannelInbound — signal delivery + attachments (§14.2 / §21 / §13.7)', () => {
  function makeIngressCtx(overrides: Record<string, unknown> = {}) {
    return {
      harnessName: 'primary',
      channelId: 'support',
      providerId: 'slack',
      platform: 'slack',
      conversationKind: 'channel',
      trigger: 'message',
      externalTenantId: 'T1',
      externalChannelId: 'C1',
      externalThreadId: 'TS1',
      externalMessageId: 'M1',
      content: 'hello from channel',
      receivedAt: 1000,
      ...overrides,
    } as never;
  }

  function setup(channelOverrides: Partial<HarnessChannelConfig> = {}) {
    const agent = new MockAgent({ id: 'default' });
    const composite = new InMemoryStore();
    const storage = composite.stores.harness as ReturnType<typeof makeStorage>;
    // Capture the latest durable inbox row per id (storage exposes no get-by-id),
    // so tests can assert on persisted delivery/runId/signalId/attachments.
    const rows = new Map<string, ChannelInboxItem>();
    const realUpdate = storage.updateChannelInboxItem.bind(storage);
    (storage as { updateChannelInboxItem: unknown }).updateChannelInboxItem = async (
      record: ChannelInboxItem,
      opts: { claimId: string },
    ) => {
      await realUpdate(record, opts);
      rows.set(record.id, { ...record });
    };
    const harness = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage },
      channels: { support: makeHarnessChannelConfig(channelOverrides) },
    });
    new Mastra({
      agents: { default: agent },
      storage: composite,
      channels: { slack: makeChannelProvider('slack') },
      harnesses: { primary: harness },
    });
    (harness as unknown as { _stopChannelInboxRecoveryLoop: () => void })._stopChannelInboxRecoveryLoop();
    return { harness, storage, agent, rows };
  }

  type AdmitResult = {
    inboxItemId: string;
    status: string;
    binding?: { id: string };
    sessionId?: string;
    queuedItemId?: string;
    duplicate: boolean;
  };

  const admit = (harness: Harness) =>
    (harness as never as { admitChannelInbound: (c: never) => Promise<AdmitResult> }).admitChannelInbound.bind(
      harness,
    ) as (c: never) => Promise<AdmitResult>;

  it('queue delivery is the default — unchanged when the policy selects no delivery', async () => {
    const { harness, rows } = setup();
    const r = await admit(harness)(makeIngressCtx());
    expect(r.status).toBe('queued');
    expect(typeof r.queuedItemId).toBe('string');
    const row = rows.get(r.inboxItemId);
    expect(row?.delivery).toBe('queue');
    expect(row?.runId).toBeUndefined();
    expect(row?.signalId).toBeUndefined();
  });

  it("signal delivery admits as a SIGNAL: idle-wake → status accepted, runId/signalId persisted, event delivery: 'signal'", async () => {
    const { harness, rows, agent } = setup({
      ingress: {
        resolveResource: async () => ({
          resourceId: 'resource-1',
          mode: 'shared-resource',
          admission: { delivery: 'signal' },
        }),
      },
    });
    const events: Array<{ type: string; delivery?: string; runId?: string; signalId?: string }> = [];
    harness.subscribe(e => {
      if (e.type.startsWith('channel_ingress_')) events.push(e as never);
    });

    const r = await admit(harness)(makeIngressCtx());

    expect(r.status).toBe('accepted');
    // queue-only field is absent for a signal admission
    expect(r.queuedItemId).toBeUndefined();
    const row = rows.get(r.inboxItemId);
    expect(row?.delivery).toBe('signal');
    expect(typeof row?.runId).toBe('string');
    expect(typeof row?.signalId).toBe('string');
    expect(row?.acceptedAt).toBeDefined();
    const admitted = events.find(e => e.type === 'channel_ingress_admitted');
    expect(admitted).toMatchObject({ delivery: 'signal', runId: row?.runId, signalId: row?.signalId });
    // the idle-wake run actually dispatched to the agent
    expect(agent.streamCalls.length).toBeGreaterThanOrEqual(1);
  });

  it('worker resumes a SIGNAL row that crashed AFTER dispatch but before the accepted commit WITHOUT firing a second run (post-dispatch idempotency)', async () => {
    // §14.2 POST-DISPATCH idempotency case: crash the original admission on the
    // 'accepted' write (after `_admitChannelSignalTurn` already dispatched a run
    // AND recorded its runId on the reservation), leaving an 'admitted' signal
    // row that carries the admissionHash + resolved ids. The recovery worker
    // reclaims it and replays `_admitChannelSignalTurn`, but the durable
    // per-`admissionId` reservation now carries a real runId, so it
    // short-circuits to the ORIGINAL run instead of dispatching a SECOND
    // interleave/wake (no double model turn / double spend). This does NOT cover
    // the pre-dispatch lost-signal window — see the next test for that.
    const { harness, storage, agent, rows } = setup({
      ingress: {
        resolveResource: async () => ({
          resourceId: 'resource-1',
          mode: 'shared-resource',
          admission: { delivery: 'signal' },
        }),
      },
    });
    const wrappedUpdate = storage.updateChannelInboxItem.bind(storage);
    (storage as { updateChannelInboxItem: unknown }).updateChannelInboxItem = async (
      record: ChannelInboxItem,
      opts: { claimId: string },
    ) => {
      if (record.status === 'accepted') throw new Error('crash before accepted commit');
      return wrappedUpdate(record, opts);
    };
    await expect(admit(harness)(makeIngressCtx())).rejects.toThrow(/crash before accepted commit/);
    // The first admission already dispatched exactly one run before the crash.
    expect(agent.streamCalls.length).toBe(1);
    const firstRunId = agent.streamCalls[0]?.options?.runId as string | undefined;

    // Heal storage so recovery can commit, then run one recovery pass past the
    // wall-clock admission-claim expiry.
    (storage as { updateChannelInboxItem: unknown }).updateChannelInboxItem = wrappedUpdate;
    const res = (await harness.recoverChannelInboxOnce({ channelId: 'support', now: 4_000_000_000_000 })) as {
      claimed: number;
      accepted: number;
    };
    expect(res).toMatchObject({ claimed: 1, accepted: 1 });
    // The recovery replay did NOT dispatch a second run — still exactly one.
    // (Before the fix this was 2: the worker re-ran `_admitChannelSignalTurn`,
    // minting a fresh signal/run for one inbound.)
    expect(agent.streamCalls.length).toBe(1);
    const recoveredRow = [...rows.values()].find(r => r.status === 'accepted');
    expect(recoveredRow?.status).toBe('accepted');
    // The recovered row reuses the original run, not a freshly minted second one.
    if (firstRunId !== undefined) expect(recoveredRow?.runId).toBe(firstRunId);
  });

  it('worker re-delivers a SIGNAL row that crashed AFTER admit but BEFORE dispatch — no lost signal (C2/C7 pre-dispatch window)', async () => {
    // §14.2 PRE-DISPATCH lost-signal window (C2/C7): the durable 'pending'
    // reservation is written BEFORE `signal()` dispatches a run. Crash the very
    // first stream() so the reservation commits with NO runId and NO run is ever
    // created (streamCalls === 0). Before the fix, recovery saw the still-pending
    // reservation and short-circuited to a fabricated `runId === signalId`,
    // permanently losing the signal while marking the row 'accepted'. After the
    // fix, a 'pending' reservation with no recorded runId is treated as
    // never-dispatched and the worker RE-DISPATCHES under the SAME deterministic
    // signalId, delivering exactly one run.
    const { harness, agent, rows } = setup({
      ingress: {
        resolveResource: async () => ({
          resourceId: 'resource-1',
          mode: 'shared-resource',
          admission: { delivery: 'signal' },
        }),
      },
    });
    // Crash the FIRST dispatch before any run is created: `signal()` calls
    // `agent.sendSignal(...)` to wake/interleave a run, so throwing there leaves
    // the durable reservation committed (written before dispatch) but no run and
    // no streamCall. Subsequent dispatches (recovery) succeed.
    const realSendSignal = agent.sendSignal.bind(agent);
    let crashed = false;
    (agent as { sendSignal: unknown }).sendSignal = (signal: unknown, target: unknown) => {
      if (!crashed) {
        crashed = true;
        throw new Error('crash before dispatch');
      }
      return realSendSignal(signal as never, target as never);
    };

    await expect(admit(harness)(makeIngressCtx())).rejects.toThrow(/crash before dispatch/);
    // The pre-dispatch crash means NO run was ever created.
    expect(agent.streamCalls.length).toBe(0);
    // The live catch path records the row as 'failed' (spreading the already-
    // committed 'admitted' state, so admissionHash + resolved ids survive). The
    // pending signal reservation was written before dispatch but carries no runId.
    const failedRow = [...rows.values()].find(r => r.status === 'failed');
    expect(failedRow?.status).toBe('failed');
    expect(failedRow?.admissionHash).toBeDefined();
    expect(failedRow?.runId).toBeUndefined();

    // Recovery replays `_admitChannelSignalTurn`; the pending reservation has no
    // runId, so the worker RE-DISPATCHES instead of fabricating an identity.
    const res = (await harness.recoverChannelInboxOnce({ channelId: 'support', now: 4_000_000_000_000 })) as {
      claimed: number;
      accepted: number;
    };
    expect(res).toMatchObject({ claimed: 1, accepted: 1 });
    // Exactly one run was dispatched on recovery — the signal was NOT lost.
    expect(agent.streamCalls.length).toBe(1);
    expect(extractSignalContents(agent.streamCalls[0]?.messages)).toBe('hello from channel');
    const recoveredRow = [...rows.values()].find(r => r.status === 'accepted');
    expect(recoveredRow?.status).toBe('accepted');
    // The recovered row carries a REAL dispatched runId, never the synthesized
    // `runId === signalId` fabrication (C7).
    expect(typeof recoveredRow?.runId).toBe('string');
    const dispatchedRunId = agent.streamCalls[0]?.options?.runId as string | undefined;
    if (dispatchedRunId !== undefined) expect(recoveredRow?.runId).toBe(dispatchedRunId);
    expect(recoveredRow?.runId).not.toBe(recoveredRow?.signalId);
  });

  it('signal delivery interleaves into an active run and shares its run terminal (§21)', async () => {
    let releaseFirst!: () => void;
    const hold = new Promise<void>(resolve => {
      releaseFirst = resolve;
    });
    const { harness, rows, agent } = setup({
      ingress: {
        resolveResource: async () => ({
          resourceId: 'resource-1',
          mode: 'shared-resource',
          admission: { delivery: 'signal' },
        }),
      },
    });
    // First inbound wakes a run that holds mid-flight, so the second inbound
    // arrives while a run is active on the same resolved channel session.
    agent.enqueueRun({ holdUntil: hold, text: 'first' });

    const first = await admit(harness)(makeIngressCtx({ externalMessageId: 'M-first' }));
    expect(first.status).toBe('accepted');
    const firstRow = rows.get(first.inboxItemId);

    // Wait until the held run is the active run on the thread.
    for (let i = 0; i < 100 && agent.streamCalls.length < 1; i++) {
      await new Promise<void>(resolve => setImmediate(resolve));
    }

    const second = await admit(harness)(makeIngressCtx({ externalMessageId: 'M-second', content: 'second' }));
    expect(second.status).toBe('accepted');
    const secondRow = rows.get(second.inboxItemId);

    // §21: the interleaved signal shares the active run's terminal — same runId.
    expect(secondRow?.runId).toBe(firstRow?.runId);
    expect(secondRow?.signalId).not.toBe(firstRow?.signalId);
    // The first inbound woke the run (its content went to the agent); the second
    // interleaved into that same run rather than starting a fresh stream.
    expect(extractSignalContents(agent.streamCalls[0]?.messages)).toBe('hello from channel');
    expect(agent.streamCalls.length).toBe(1);

    releaseFirst();
  });

  it("rejects delivery: 'signal' carrying a policy model override (signal has no per-turn model)", async () => {
    const { harness } = setup({
      ingress: {
        resolveResource: async () => ({
          resourceId: 'resource-1',
          mode: 'shared-resource',
          admission: { delivery: 'signal', model: 'model-x' },
        }),
      },
    });
    await expect(admit(harness)(makeIngressCtx())).rejects.toBeInstanceOf(HarnessValidationError);
  });

  it('record-only recovery replays inbound attachments (C4): raw files survive an ACK-before-admission crash', async () => {
    // §14.2 record-only durability (C4): a route that records the 'received' row
    // and ACKs BEFORE admission cannot scope the bytes to a session yet, so the
    // durable row's `attachments` is []. The RAW provider refs are persisted on
    // the row (`rawFiles`) so FROM-SCRATCH recovery re-populates the ingress
    // context and normalizes the SAME attachments the live path would have.
    // Before the fix, `reconstructChannelIngressContext` dropped the files and
    // the recovered turn carried ZERO attachments.
    const { harness, rows, agent } = setup();
    agent.enqueueRun({ text: 'ok' });
    const { resolved } = await (
      harness as never as {
        _resolveChannelBindingForIngress: (
          c: never,
        ) => Promise<{ resolved: { sessionId: string; resourceId: string; threadId: string } }>;
      }
    )._resolveChannelBindingForIngress(makeIngressCtx());
    const sessionId = resolved.sessionId;
    const resourceId = resolved.resourceId;
    const session = await harness.session({
      resourceId,
      threadId: resolved.threadId,
      sessionId,
    } as never);
    const { attachmentId } = await session.uploadAttachment({
      name: 'note.txt',
      mimeType: 'text/plain',
      data: new TextEncoder().encode('attachment-bytes'),
    });

    const recordOnly = (
      harness as never as {
        admitChannelInbound: (c: never, o: { continueAdmission: boolean }) => Promise<AdmitResult>;
      }
    ).admitChannelInbound.bind(harness) as (c: never, o: { continueAdmission: boolean }) => Promise<AdmitResult>;

    // Record + ACK before admission, carrying the inbound provider file refs.
    const r = await recordOnly(
      makeIngressCtx({
        externalMessageId: 'M-record-only-file',
        content: 'see attached',
        files: [{ attachmentId, resourceId, ownerSessionId: sessionId }],
      }),
      { continueAdmission: false },
    );
    expect(r.status).toBe('received');
    const receivedRow = rows.get(r.inboxItemId) ?? [...rows.values()].find(row => row.status === 'received');
    // The received row is not session-scoped yet → no normalized attachments, but
    // the RAW provider refs are persisted for recovery.
    expect(receivedRow?.attachments).toEqual([]);
    expect(receivedRow?.rawFiles?.length).toBe(1);
    expect(receivedRow?.rawFiles?.[0]).toMatchObject({ attachmentId, resourceId });

    // FROM-SCRATCH recovery normalizes the raw refs and dispatches the turn.
    const res = (await harness.recoverChannelInboxOnce({ channelId: 'support', now: 4_000_000_000_000 })) as {
      claimed: number;
      queued: number;
    };
    expect(res).toMatchObject({ claimed: 1, queued: 1 });

    // The recovered row now carries the normalized attachment (not []).
    const recoveredRow = [...rows.values()].find(row => row.status === 'queued');
    expect(recoveredRow?.attachments.length).toBe(1);
    expect(recoveredRow?.attachments[0]).toMatchObject({ kind: 'ref', attachmentId, name: 'note.txt' });

    // The dispatched turn carries the file part — the SAME shape the non-channel
    // queue/live path forwards (role:'user' with a file content part).
    for (let i = 0; i < 200 && agent.streamCalls.length < 1; i++) {
      await new Promise<void>(resolve => setImmediate(resolve));
    }
    const dispatched = extractSignalContents(agent.streamCalls[agent.streamCalls.length - 1]?.messages) as {
      role?: string;
      content?: Array<{ type: string; filename?: string }>;
    };
    expect(dispatched.role).toBe('user');
    expect(dispatched.content?.some(p => p.type === 'file' && p.filename === 'note.txt')).toBe(true);
  });

  it('normalizes inbound files into durable row attachments and forwards them to the queued turn', async () => {
    const { harness, rows, agent } = setup();
    agent.enqueueRun({ text: 'ok' });
    // Resolve the binding WITHOUT admitting a turn (no competing queue drain),
    // materialize that exact channel session via its threadId, then upload an
    // attachment owned by it and reference it on the single inbound under test.
    const { resolved } = await (
      harness as never as {
        _resolveChannelBindingForIngress: (
          c: never,
        ) => Promise<{ resolved: { sessionId: string; resourceId: string; threadId: string } }>;
      }
    )._resolveChannelBindingForIngress(makeIngressCtx());
    const sessionId = resolved.sessionId;
    const resourceId = resolved.resourceId;
    const session = await harness.session({
      resourceId,
      threadId: resolved.threadId,
      sessionId,
    } as never);
    const { attachmentId } = await session.uploadAttachment({
      name: 'note.txt',
      mimeType: 'text/plain',
      data: new TextEncoder().encode('attachment-bytes'),
    });

    const r = await admit(harness)(
      makeIngressCtx({
        externalMessageId: 'M-with-file',
        content: 'see attached',
        files: [{ attachmentId, resourceId, ownerSessionId: sessionId }],
      }),
    );
    expect(r.status).toBe('queued');
    const row = rows.get(r.inboxItemId);
    // Durable row carries the normalized ref (metadata only), not [].
    expect(row?.attachments.length).toBe(1);
    expect(row?.attachments[0]).toMatchObject({ kind: 'ref', attachmentId, name: 'note.txt', mimeType: 'text/plain' });

    // The queued turn dispatched a structured user message carrying the file part.
    for (let i = 0; i < 200 && agent.streamCalls.length < 1; i++) {
      await new Promise<void>(resolve => setImmediate(resolve));
    }
    const dispatched = extractSignalContents(agent.streamCalls[agent.streamCalls.length - 1]?.messages) as {
      role?: string;
      content?: Array<{ type: string; mediaType?: string; filename?: string }>;
    };
    expect(dispatched.role).toBe('user');
    expect(dispatched.content?.some(p => p.type === 'text')).toBe(true);
    expect(dispatched.content?.some(p => p.type === 'file' && p.filename === 'note.txt')).toBe(true);
  });

  it('the idempotency key distinguishes two inbound messages that differ only by their files', async () => {
    const { harness, rows } = setup();
    const { resolved } = await (
      harness as never as {
        _resolveChannelBindingForIngress: (
          c: never,
        ) => Promise<{ resolved: { sessionId: string; resourceId: string; threadId: string } }>;
      }
    )._resolveChannelBindingForIngress(makeIngressCtx());
    const sessionId = resolved.sessionId;
    const resourceId = resolved.resourceId;
    const session = await harness.session({
      resourceId,
      threadId: resolved.threadId,
      sessionId,
    } as never);
    const a = await session.uploadAttachment({
      name: 'a.txt',
      mimeType: 'text/plain',
      data: new TextEncoder().encode('A'),
    });
    const b = await session.uploadAttachment({
      name: 'b.txt',
      mimeType: 'text/plain',
      data: new TextEncoder().encode('B'),
    });

    // Same idempotency identity (same external ids/content) but different files →
    // a DIFFERENT payloadHash → a payload conflict, not a false duplicate.
    const withA = await admit(harness)(
      makeIngressCtx({
        externalMessageId: 'M-files',
        content: 'x',
        files: [{ attachmentId: a.attachmentId, resourceId, ownerSessionId: sessionId }],
      }),
    );
    const withB = await admit(harness)(
      makeIngressCtx({
        externalMessageId: 'M-files',
        content: 'x',
        files: [{ attachmentId: b.attachmentId, resourceId, ownerSessionId: sessionId }],
      }),
    );
    expect(withA.inboxItemId).toBe(withB.inboxItemId);
    expect((withB as { conflict?: boolean }).conflict).toBe(true);
    const row = rows.get(withA.inboxItemId);
    // The first (file A) admission stands; the conflicting retry did not overwrite it.
    expect(row?.attachments[0]).toMatchObject({ attachmentId: a.attachmentId });
  });

  it('an inbound with no files leaves the dispatched contents a bare string (unchanged)', async () => {
    const { harness, agent } = setup();
    agent.enqueueRun({ text: 'ok' });
    const before = agent.streamCalls.length;
    const r = await admit(harness)(makeIngressCtx({ externalMessageId: 'M-nofiles' }));
    expect(r.status).toBe('queued');
    for (let i = 0; i < 100 && agent.streamCalls.length <= before; i++) {
      await new Promise<void>(resolve => setImmediate(resolve));
    }
    expect(extractSignalContents(agent.streamCalls[agent.streamCalls.length - 1]?.messages)).toBe('hello from channel');
  });
});

describe('Harness.handleChannelInboundRequest (§13.2/§14.2 ingress bridge / C4c)', () => {
  // Adapter with a verifyInbound that projects the transport body into an ingress
  // envelope; `bad: true` simulates a failed provider signature verification.
  function makeInboundConfig(overrides: Partial<HarnessChannelConfig> = {}): HarnessChannelConfig {
    return makeHarnessChannelConfig({
      adapter: {
        deliver: async () => ({}),
        verifyInbound: async (request: { body?: unknown }) => {
          const body = (request.body ?? {}) as {
            content?: string;
            externalMessageId?: string;
            bad?: boolean;
            files?: { attachmentId: string; resourceId: string; sha256?: string }[];
          };
          if (body.bad) throw new Error('provider signature verification failed');
          return {
            platform: 'slack',
            conversationKind: 'channel',
            trigger: 'message',
            externalTenantId: 'T1',
            externalChannelId: 'C1',
            externalThreadId: 'TS1',
            externalMessageId: body.externalMessageId ?? 'M1',
            content: body.content ?? 'hello from channel',
            ...(body.files !== undefined ? { files: body.files } : {}),
            receivedAt: 1000,
          };
        },
      } as never,
      ...overrides,
    });
  }

  function setup(channelOverrides: Partial<HarnessChannelConfig> = {}) {
    const composite = new InMemoryStore();
    const storage = composite.stores.harness as ReturnType<typeof makeStorage>;
    const harness = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage },
      channels: { support: makeInboundConfig(channelOverrides) },
    });
    new Mastra({
      agents: { default: makeAgent() },
      storage: composite,
      channels: { slack: makeChannelProvider('slack') },
      harnesses: { primary: harness },
    });
    // Recovery loop stays running so `channelWorkerReadiness` reports ready (the
    // §13.6 gate requires a started worker). Its poll interval is floored at 1s, far
    // longer than these synchronous tests, so no background tick races the asserts.
    return { harness, storage };
  }

  function inboundRequest(body: unknown): never {
    return {
      method: 'POST',
      path: '/harness/primary/channels/support/inbound',
      headers: { 'content-type': 'application/json' },
      body,
    } as never;
  }

  it('404 not_found for an unregistered channel (registry boundary, no verification)', async () => {
    const { harness } = setup();
    const result = await harness.handleChannelInboundRequest('nope', inboundRequest({ content: 'hi' }));
    expect(result).toMatchObject({ kind: 'not_found', httpStatus: 404, error: { code: 'harness.not_found' } });
  });

  it('401 verify_failed when the adapter rejects the provider signature', async () => {
    const { harness } = setup();
    const result = await harness.handleChannelInboundRequest('support', inboundRequest({ bad: true }));
    expect(result).toMatchObject({
      kind: 'verify_failed',
      httpStatus: 401,
      error: { code: 'harness.permission_denied' },
    });
  });

  it('redacts the raw verifyInbound error message on the public verify_failed envelope (§13.3f.1)', async () => {
    // The adapter throws a message that would leak provider-auth internals
    // (signing-secret/config hints) to an anonymous webhook caller.
    const { harness } = setup({
      adapter: {
        deliver: async () => ({}),
        verifyInbound: async () => {
          throw new Error('SLACK_SIGNING_SECRET mismatch: expected v0=deadbeef but got v0=cafe');
        },
      } as never,
    });
    const result = await harness.handleChannelInboundRequest('support', inboundRequest({ content: 'hi' }));
    expect(result).toMatchObject({
      kind: 'verify_failed',
      httpStatus: 401,
      error: { code: 'harness.permission_denied', message: 'channel inbound verification failed' },
    });
    // The raw adapter cause must NOT cross the public envelope.
    const message = (result as { error: { message: string } }).error.message;
    expect(message).not.toContain('SLACK_SIGNING_SECRET');
    expect(message).not.toContain('v0=deadbeef');
  });

  it('400 verify_failed when the channel adapter has no inbound verification', async () => {
    // Adapter without verifyInbound — cannot project provider transport into an envelope.
    const { harness } = setup({ adapter: { deliver: async () => ({}) } as never });
    const result = await harness.handleChannelInboundRequest('support', inboundRequest({ content: 'hi' }));
    expect(result).toMatchObject({
      kind: 'verify_failed',
      httpStatus: 400,
      error: { code: 'harness.bad_request' },
    });
  });

  it('503 worker_unavailable when the readiness gate is not ready, before creating a row', async () => {
    const { harness } = setup();
    (harness as unknown as { _stopChannelInboxRecoveryLoop: () => void })._stopChannelInboxRecoveryLoop();
    const result = await harness.handleChannelInboundRequest('support', inboundRequest({ content: 'hi' }));
    expect(result).toMatchObject({
      kind: 'not_ready',
      httpStatus: 503,
      error: { code: 'harness.worker_unavailable', retryable: true, details: { reason: 'worker_not_started' } },
    });
    // §14.2: the gate returns BEFORE a durable inbox row is created. Restarting the
    // worker and retrying the same provider event admits a FRESH row (duplicate:false),
    // which proves the 503 path persisted nothing to de-dupe against.
    (harness as unknown as { _ensureChannelInboxRecoveryLoop: () => void })._ensureChannelInboxRecoveryLoop();
    const retry = await harness.handleChannelInboundRequest('support', inboundRequest({ content: 'hi' }), {
      continueAdmission: true,
    });
    expect(retry).toMatchObject({ kind: 'ok', duplicate: false });
  });

  it('202 record-only ACK by default (continueAdmission omitted → received)', async () => {
    const { harness } = setup();
    const result = await harness.handleChannelInboundRequest('support', inboundRequest({ content: 'hi' }));
    expect(result).toMatchObject({ kind: 'ok', ackStatus: 202, status: 'received', duplicate: false });
  });

  it('200 and full admission when continueAdmission is true', async () => {
    const { harness } = setup();
    const result = await harness.handleChannelInboundRequest('support', inboundRequest({ content: 'hi' }), {
      continueAdmission: true,
    });
    expect(result).toMatchObject({ kind: 'ok', ackStatus: 200, status: 'queued', duplicate: false });
    expect((result as { sessionId?: string }).sessionId).toMatch(/^chs:/);
  });

  it('200 duplicate:true for an exact provider retry', async () => {
    const { harness } = setup();
    const first = await harness.handleChannelInboundRequest('support', inboundRequest({ content: 'hi' }), {
      continueAdmission: true,
    });
    const second = await harness.handleChannelInboundRequest('support', inboundRequest({ content: 'hi' }), {
      continueAdmission: true,
    });
    expect(first).toMatchObject({ kind: 'ok', duplicate: false });
    expect(second).toMatchObject({ kind: 'ok', ackStatus: 200, status: 'queued', duplicate: true });
  });

  it('409 conflict for the same idempotency key with a different payload', async () => {
    const { harness } = setup();
    await harness.handleChannelInboundRequest(
      'support',
      inboundRequest({ content: 'first', externalMessageId: 'M-conflict' }),
      { continueAdmission: true },
    );
    const conflicting = await harness.handleChannelInboundRequest(
      'support',
      inboundRequest({ content: 'SECOND DIFFERENT', externalMessageId: 'M-conflict' }),
      { continueAdmission: true },
    );
    expect(conflicting).toMatchObject({
      kind: 'conflict',
      httpStatus: 409,
      error: { code: 'harness.channel_action_conflict' },
    });
  });

  it('409 conflict when only the attachments differ (payload hash covers files)', async () => {
    const { harness } = setup();
    // Record-only ACK: the payloadHash (which covers `ctx.files`) is computed and
    // the durable row created at the idempotency-key boundary, BEFORE binding
    // resolution + attachment normalization. That is the layer this test pins —
    // a different attachment set under the same key is a conflict, not a false
    // duplicate. (continueAdmission:true would additionally require the refs to be
    // uploaded Harness records, which §13.7 normalization now resolves; the
    // hash-covers-files contract is independent of that.)
    await harness.handleChannelInboundRequest(
      'support',
      inboundRequest({
        content: 'same',
        externalMessageId: 'M-files',
        files: [{ attachmentId: 'a1', resourceId: 'r1' }],
      }),
      { continueAdmission: false },
    );
    const differentFiles = await harness.handleChannelInboundRequest(
      'support',
      inboundRequest({
        content: 'same',
        externalMessageId: 'M-files',
        files: [{ attachmentId: 'a2', resourceId: 'r1' }],
      }),
      { continueAdmission: false },
    );
    // Same route + externalMessageId (same idempotency key) but a different attachment set must be a
    // conflict, not a false duplicate — otherwise the second message is silently dropped.
    expect(differentFiles).toMatchObject({ kind: 'conflict', httpStatus: 409 });
  });

  it('200 duplicate:true when both content and attachments match', async () => {
    const { harness } = setup();
    const files = [{ attachmentId: 'a1', resourceId: 'r1' }];
    // Record-only ACK pins the payloadHash idempotency layer (see the conflict
    // test above) independent of attachment normalization.
    const first = await harness.handleChannelInboundRequest(
      'support',
      inboundRequest({ content: 'x', externalMessageId: 'M-dup-files', files }),
      { continueAdmission: false },
    );
    const second = await harness.handleChannelInboundRequest(
      'support',
      inboundRequest({ content: 'x', externalMessageId: 'M-dup-files', files }),
      { continueAdmission: false },
    );
    expect(first).toMatchObject({ kind: 'ok', duplicate: false });
    expect(second).toMatchObject({ kind: 'ok', duplicate: true });
  });
});
