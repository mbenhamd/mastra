import { randomUUID } from 'node:crypto';

import { getErrorFromUnknown } from '../error';
import { EventEmitterPubSub } from '../events/event-emitter';
import { isLeaseProvider, NoopLeaseProvider } from '../events/pubsub';
import type { LeaseProvider, PubSub } from '../events/pubsub';
import type { EventCallback } from '../events/types';
import { parseMemoryRequestContext } from '../memory/types';
import type { RequestContext } from '../request-context';
import { MASTRA_RESOURCE_ID_KEY, MASTRA_THREAD_ID_KEY } from '../request-context';
import type { MastraModelOutput } from '../stream/base/output';
import type { Agent } from './agent';
import type { AgentExecutionOptions } from './agent.types';
import type { MessageListInput } from './message-list';
import { createMessageSignal, createSignal, resolveDeliveryAttributes } from './signals';
import type { AgentMessageInput, AgentStateSignalInput, CreatedAgentSignal } from './signals';
import { applyStateSignal } from './state-signals';
import type {
  AgentSignal,
  AgentSubscribeToThreadOptions,
  AgentThreadSubscription,
  QueueAgentMessageOptions,
  QueueAgentMessageResult,
  SendAgentMessageOptions,
  SendAgentMessageResult,
  SendAgentSignalOptions,
  SendAgentSignalAccepted,
  SendAgentSignalResult,
  SendAgentStateSignalOptions,
  SendAgentStateSignalResult,
} from './types';

const AGENT_THREAD_KEY_SEPARATOR = '\u0000';
const AGENT_THREAD_STREAM_TOPIC_PREFIX = 'agent.thread-stream';
const REJECTED_RUN_TOMBSTONE_TTL_MS = 5 * 60 * 1000;
const MAX_REJECTED_RUN_TOMBSTONES = 1000;
const ABORTED_RUN_TOMBSTONE_TTL_MS = 5 * 60 * 1000;
const MAX_ABORTED_RUN_TOMBSTONES = 1000;
const TERMINAL_PUBLISH_TIMEOUT_MS = 10_000;
const TERMINAL_DELIVERY_TIMEOUT_MS = 30_000;

export type AgentThreadOutputDrainErrorReason =
  | 'subscription-closed'
  | 'stream-stopped'
  | 'registration-publish-failed'
  | 'terminal-publish-failed'
  | 'terminal-delivery-timeout';

/** Internal failure for the subscription barrier that makes terminal delivery observable. */
export class AgentThreadOutputDrainError extends Error {
  readonly name = 'AgentThreadOutputDrainError';

  constructor(
    readonly reason: AgentThreadOutputDrainErrorReason,
    message: string,
    readonly cause?: unknown,
  ) {
    super(message);
  }
}

/** Teardown may wake a terminal model result without replacing that already-known result. */
export function isAgentThreadOutputDrainTeardownError(error: unknown): boolean {
  return (
    error instanceof AgentThreadOutputDrainError &&
    (error.reason === 'subscription-closed' || error.reason === 'stream-stopped')
  );
}

async function waitWithTimeout<T>(work: Promise<T>, timeoutMs: number, timeoutError: () => Error): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => reject(timeoutError()), timeoutMs);
    (timer as ReturnType<typeof setTimeout> & { unref?: () => void }).unref?.();
  });
  try {
    return await Promise.race([work, timeout]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}

/**
 * TTL for the cross-process thread lease acquired in the idle-wake path.
 * Kept short so a crashed owner process frees the thread quickly. A
 * background timer renews the lease while the run is still running.
 */
function readPositiveIntEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  if (!raw) return fallback;
  const parsed = Number(raw);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

/**
 * Lease TTL. Overridable via `MASTRA_AGENT_THREAD_LEASE_TTL_MS` so cross-process
 * tests can shrink the takeover window (production keeps the 15s default).
 */
const AGENT_THREAD_LEASE_TTL_MS = readPositiveIntEnv('MASTRA_AGENT_THREAD_LEASE_TTL_MS', 15_000);
/**
 * Interval at which the owner process renews its lease. Defaults to TTL/3,
 * leaving room for two missed renewals (network blip, GC pause) before the
 * lease expires. Overridable via `MASTRA_AGENT_THREAD_LEASE_RENEW_INTERVAL_MS`.
 */
const AGENT_THREAD_LEASE_RENEW_INTERVAL_MS = readPositiveIntEnv(
  'MASTRA_AGENT_THREAD_LEASE_RENEW_INTERVAL_MS',
  Math.floor(AGENT_THREAD_LEASE_TTL_MS / 3),
);

export let defaultAgentThreadPubSub: PubSub = new EventEmitterPubSub();

function callerSignalPayloadKey(signal: AgentSignal): string | undefined {
  try {
    return JSON.stringify({
      type: signal.type,
      contents: signal.contents,
      attributes: signal.attributes,
      metadata: signal.metadata,
    });
  } catch {
    return undefined;
  }
}

function withThreadMemory(memory: unknown, resourceId: string, threadId: string) {
  return {
    ...((memory && typeof memory === 'object' ? memory : {}) as Record<string, unknown>),
    resource: (memory as { resource?: string } | undefined)?.resource ?? resourceId,
    thread: (memory as { thread?: string } | undefined)?.thread ?? threadId,
  };
}

type AgentThreadRunLifecycle = 'running' | 'suspending' | 'suspended' | 'completed' | 'failed' | 'aborted';

type AgentThreadRunSuspension = {
  toolCallId?: string;
  toolName?: string;
  kind: 'approval' | 'generic-tool';
};

type AgentThreadRunRecord<OUTPUT = unknown> = {
  agent: Agent<any, any, any, any>;
  output: MastraModelOutput<OUTPUT>;
  runId: string;
  streamId: string;
  streamSeq: number;
  lifecycle: AgentThreadRunLifecycle;
  suspension?: AgentThreadRunSuspension;
  threadId: string;
  resourceId?: string;
  streamOptions: AgentExecutionOptions<OUTPUT>;
  // For local (same-runtime / EventEmitterPubSub) runs, a multicast factory that
  // returns an independent ReadableStream per thread subscriber. The fork does not
  // republish local runs through pubsub, so without this every subscriber (and the
  // caller) would compete over the single `output.fullStream`, starving all but the
  // first reader. Absent for remote runs, whose subscribers already get a dedicated
  // per-subscription stream fed by `stream-part` pubsub events.
  createSubscriberStream?: () => ReadableStream<unknown>;
};

type PreparedThreadRun = {
  abortController: AbortController;
  cleanup: () => void;
};

type RejectedRunErrorRecord = {
  error: Error;
  cleanupTimer: ReturnType<typeof setTimeout>;
};

type PendingIdleSignal<OUTPUT = unknown> = {
  agent: Agent<any, any, any, any>;
  signal: CreatedAgentSignal;
  runId: string;
  resourceId: string;
  threadId: string;
  streamOptions?: AgentExecutionOptions<OUTPUT>;
  onRunRejected?: () => void;
  reserveBeforePreflight?: boolean;
};

type PendingContinuation<OUTPUT = unknown> = {
  agent: Agent<any, any, any, any>;
  messages: MessageListInput;
  runId: string;
  resourceId: string;
  threadId: string;
  streamOptions?: AgentExecutionOptions<OUTPUT>;
};

type AgentThreadRuntimeState = {
  threadRunsById: Map<string, AgentThreadRunRecord<any>>;
  threadRunsByStreamId: Map<string, AgentThreadRunRecord<any>>;
  threadKeysByRunId: Map<string, string>;
  activeThreadRunIds: Map<string, string>;
  activeThreadStreamIds: Map<string, string>;
  streamSeqByRunId: Map<string, number>;
  approvalSuspendedRunIds: Set<string>;
  suspendedRunIds: Set<string>;
  suspensionMetadataByRunId: Map<string, AgentThreadRunSuspension>;
  pendingSignalsByThread: Map<string, CreatedAgentSignal[]>;
  // Signals queued for a run that is starting but has not made its first model
  // request yet. The first LLM step drains these and folds them into that
  // request; `pendingSignalsByThread` follow-ups instead become their own turn.
  preRunSignalsByThread: Map<string, CreatedAgentSignal[]>;
  pendingIdleSignalsByThread: Map<string, PendingIdleSignal<any>[]>;
  pendingIdleThreadKeysByRunId: Map<string, string>;
  inflightIdleThreadKeysByRunId: Map<string, string>;
  inflightIdleAgentIdsByRunId: Map<string, string>;
  pendingContinuationsByThread: Map<string, PendingContinuation<any>[]>;
  watchedThreadStreamIds: Set<string>;
  preparedRunsById: Map<string, PreparedThreadRun>;
  reservedAgentIdsByRunId: Map<string, string>;
  reservationWaitersByRunId: Map<string, Array<() => void>>;
  abortedRunIds: Set<string>;
  abortedRunCleanupTimersByRunId: Map<string, ReturnType<typeof setTimeout>>;
  rejectedRunErrorsByRunId: Map<string, RejectedRunErrorRecord>;
  acceptedCallerSignals: Map<string, SendAgentSignalResult>;
  callerSignalIdsByRunId: Map<string, Set<string>>;
  pendingOutputWaiters: Map<
    string,
    Array<{ resolve: (out: MastraModelOutput<any>) => void; reject: (error: Error) => void }>
  >;
  registrationPublishesByRunId: Map<string, Promise<void>>;
  broadcastsByRunId: Map<string, Promise<void>>;
  /**
   * Active lease-renewal timers keyed by runId. Set when the owner
   * process wins the cross-process lease, cleared on release. Stored
   * here (not on a Map<key,timer>) so a run's renewal timer survives even
   * if `activeThreadRunIds` is rotated by a follow-up signal.
   */
  leaseRenewalTimers: Map<string, ReturnType<typeof setInterval>>;
};

export type AgentThreadState = 'active' | 'idle';

type SerializableAgentSignal = AgentSignal & Pick<CreatedAgentSignal, 'id' | 'createdAt'>;

type AgentThreadStreamRuntimeEvent =
  | { type: 'run-registered'; runId: string; streamId: string; streamSeq: number }
  | { type: 'stream-part'; runId: string; streamId: string; part: unknown; sourceId: string }
  | { type: 'run-completed'; runId: string; streamId?: string }
  | { type: 'run-suspended'; runId: string; streamId?: string }
  | { type: 'run-aborted'; runId: string; streamId?: string }
  | { type: 'run-failed'; runId: string; streamId?: string; error: string }
  | { type: 'signal-enqueued'; runId: string; signal: SerializableAgentSignal; sourceId: string; preRun?: boolean };

function getIdleRunRejectedHandler(ifIdle: unknown): (() => void) | undefined {
  const handler = (ifIdle as { _onThreadStreamRunRejected?: unknown } | undefined)?._onThreadStreamRunRejected;
  return typeof handler === 'function' ? () => handler() : undefined;
}

function createRuntimeState(): AgentThreadRuntimeState {
  return {
    threadRunsById: new Map(),
    threadRunsByStreamId: new Map(),
    threadKeysByRunId: new Map(),
    activeThreadRunIds: new Map(),
    activeThreadStreamIds: new Map(),
    streamSeqByRunId: new Map(),
    approvalSuspendedRunIds: new Set(),
    suspendedRunIds: new Set(),
    suspensionMetadataByRunId: new Map(),
    pendingSignalsByThread: new Map(),
    preRunSignalsByThread: new Map(),
    pendingIdleSignalsByThread: new Map(),
    pendingIdleThreadKeysByRunId: new Map(),
    inflightIdleThreadKeysByRunId: new Map(),
    inflightIdleAgentIdsByRunId: new Map(),
    pendingContinuationsByThread: new Map(),
    watchedThreadStreamIds: new Set(),
    preparedRunsById: new Map(),
    reservedAgentIdsByRunId: new Map(),
    reservationWaitersByRunId: new Map(),
    abortedRunIds: new Set(),
    abortedRunCleanupTimersByRunId: new Map(),
    rejectedRunErrorsByRunId: new Map(),
    acceptedCallerSignals: new Map(),
    callerSignalIdsByRunId: new Map(),
    pendingOutputWaiters: new Map(),
    registrationPublishesByRunId: new Map(),
    broadcastsByRunId: new Map(),
    leaseRenewalTimers: new Map(),
  };
}

export class AgentThreadStreamRuntime {
  #id = randomUUID();
  #statesByPubSub = new WeakMap<PubSub, AgentThreadRuntimeState>();
  #threadOutputRegistrations = new WeakMap<object, Promise<void>>();
  #threadOutputTerminals = new WeakMap<object, Promise<void>>();

  #getPubSub(pubsub?: PubSub): PubSub {
    return pubsub ?? defaultAgentThreadPubSub;
  }

  /**
   * Resolve the {@link LeaseProvider} for the configured pubsub. Leasing is
   * a separate capability from event delivery: a backend only implements it
   * when it can genuinely coordinate a distributed lock (Redis via SET-NX,
   * in-memory for single-process). We feature-detect once here so all lease
   * call sites can use the resolved provider unconditionally.
   *
   * `CachingPubSub` exposes its inner's lease provider via `getLeaseProvider`
   * (caching is transparent to leasing). Otherwise we duck-type the pubsub
   * directly. Backends that cannot lease fall back to {@link NoopLeaseProvider}
   * (always-win / no-op), preserving single-process behavior.
   */
  #getLeaseProvider(pubsub?: PubSub): LeaseProvider {
    const resolved = this.#getPubSub(pubsub);
    const unwrap = (resolved as { getLeaseProvider?: () => LeaseProvider | undefined }).getLeaseProvider;
    if (typeof unwrap === 'function') {
      const inner = unwrap.call(resolved);
      return inner ?? NoopLeaseProvider;
    }
    return isLeaseProvider(resolved) ? resolved : NoopLeaseProvider;
  }

  #getSourceId(): string {
    this.#id ??= randomUUID();
    return this.#id;
  }

  /**
   * Fire-and-forget release of the cross-process thread lease held by
   * this owner. Safe to call when no lease was ever acquired — the
   * pubsub's `releaseLease` is a no-op for non-owners (Lua-guarded
   * GET+DEL on Redis), and the default in-memory implementation is
   * identical. Also stops the renewal timer if one is running for
   * this run.
   */
  #releaseThreadLease(pubsub: PubSub | undefined, key: string, runId: string): void {
    const resolved = this.#getPubSub(pubsub);
    this.#stopLeaseRenewal(resolved, runId);
    void this.#getLeaseProvider(resolved)
      .releaseLease(key, runId)
      .catch(() => {});
  }

  /**
   * Start a background timer that renews the cross-process lease at
   * TTL/3 intervals while the run is still going. If the lease is lost
   * (e.g. expired due to clock skew or pubsub outage) the renewal
   * stops itself — there's nothing useful we can do from the runner
   * side beyond log; the original owner will keep running until the run
   * itself errors or completes.
   */
  #startLeaseRenewal(pubsub: PubSub, key: string, runId: string): void {
    const state = this.#getState(pubsub);
    if (state.leaseRenewalTimers.has(runId)) return;
    const leaseProvider = this.#getLeaseProvider(pubsub);
    const timer = setInterval(() => {
      void leaseProvider
        .renewLease(key, runId, AGENT_THREAD_LEASE_TTL_MS)
        .then(renewed => {
          if (!renewed) {
            // If renewLease reports the lease is gone, stop renewing; the current stream may still finish,
            // but another process can now claim the thread until this run completes or errors.
            this.#stopLeaseRenewal(pubsub, runId);
          }
        })
        .catch(() => {});
    }, AGENT_THREAD_LEASE_RENEW_INTERVAL_MS);
    // Don't keep the process alive solely to renew a lease.
    if (typeof timer === 'object' && timer && typeof (timer as any).unref === 'function') {
      (timer as any).unref();
    }
    state.leaseRenewalTimers.set(runId, timer);
  }

  #stopLeaseRenewal(pubsub: PubSub, runId: string): void {
    const state = this.#getState(pubsub);
    const timer = state.leaseRenewalTimers.get(runId);
    if (!timer) return;
    clearInterval(timer);
    state.leaseRenewalTimers.delete(runId);
  }

  /**
   * Hand the cross-process thread lease from a finishing run (`fromRunId`)
   * to the run that will drain queued follow-up work next (`toRunId`),
   * without the lease key ever going empty.
   *
   * The previous owner releases its renewal timer and the new owner starts
   * its own; the lease key is re-stamped by `transferLease` (with a full fresh
   * TTL). On atomic backends (Redis, in-memory) a racing process cannot win a
   * freed key between a release and a re-acquire. Backends that can't transfer
   * atomically implement `transferLease` as release+acquire internally and own
   * that race cost. Returns `true` if the new owner now holds the lease.
   */
  async #transferThreadLease(
    pubsub: PubSub | undefined,
    key: string,
    fromRunId: string,
    toRunId: string,
  ): Promise<boolean> {
    const resolved = this.#getPubSub(pubsub);
    const leaseProvider = this.#getLeaseProvider(resolved);
    // `transferLease` is a required `LeaseProvider` method. Atomic backends
    // (Redis, in-memory) swap the key gap-free; backends that can't be atomic
    // implement it as release+acquire internally and own that race cost.
    const held = await leaseProvider
      .transferLease(key, fromRunId, toRunId, AGENT_THREAD_LEASE_TTL_MS)
      .catch(() => false);
    // Move the renewal timer to the new owner regardless: the old timer is
    // owner-guarded and would only no-op now, and the new owner needs its
    // own keep-alive for long drains.
    this.#stopLeaseRenewal(resolved, fromRunId);
    if (held) {
      this.#startLeaseRenewal(resolved, key, toRunId);
    }
    return held;
  }

  /**
   * Ensure this process owns the cross-process lease for `toRunId` before it
   * starts a run, regardless of whether it already held the lease.
   *
   * - When `fromRunId` is provided (draining after a run this process owned),
   *   atomically transfer the held lease to `toRunId` — gap-free, no empty key.
   * - When `fromRunId` is absent, or the transfer reports the old owner no
   *   longer holds the lease, fall back to a fresh `acquireLease`. This covers
   *   a *different* process that observed the owner finish via pub/sub and now
   *   wants to wake the thread: it never held the lease, so it must win one.
   *
   * On success the renewal timer is started for `toRunId`. On failure the
   * returned `owner` is the current holder so the caller can forward work to it.
   */
  async #acquireOrTransferThreadLease(
    pubsub: PubSub | undefined,
    key: string,
    toRunId: string,
    fromRunId?: string,
  ): Promise<{ acquired: boolean; owner?: string }> {
    const resolved = this.#getPubSub(pubsub);
    if (fromRunId) {
      const transferred = await this.#transferThreadLease(pubsub, key, fromRunId, toRunId);
      if (transferred) return { acquired: true, owner: toRunId };
      // Old owner lost the lease before the handoff — fall through to acquire.
    }
    const leaseProvider = this.#getLeaseProvider(resolved);
    const result = await leaseProvider
      .acquireLease(key, toRunId, AGENT_THREAD_LEASE_TTL_MS)
      .catch(() => ({ acquired: false as boolean, owner: undefined as string | undefined }));
    if (result.acquired) {
      this.#startLeaseRenewal(resolved, key, toRunId);
      return { acquired: true, owner: toRunId };
    }
    return { acquired: false, owner: result.owner };
  }

  /**
   * Whether the thread has any queued follow-up work that a finishing run's
   * completion handler would drain next: pending follow-up signals (including
   * any pre-run leftover that will be folded in), queued continuations, or
   * queued idle signals.
   */
  #hasPendingThreadWork(state: AgentThreadRuntimeState, key: string): boolean {
    return (
      (state.pendingSignalsByThread.get(key)?.length ?? 0) > 0 ||
      (state.preRunSignalsByThread.get(key)?.length ?? 0) > 0 ||
      (state.pendingContinuationsByThread.get(key)?.length ?? 0) > 0 ||
      (state.pendingIdleSignalsByThread.get(key)?.length ?? 0) > 0
    );
  }

  #getState(pubsub?: PubSub): AgentThreadRuntimeState {
    const resolvedPubSub = this.#getPubSub(pubsub);
    let state = this.#statesByPubSub.get(resolvedPubSub);
    if (!state) {
      state = createRuntimeState();
      this.#statesByPubSub.set(resolvedPubSub, state);
    }
    return state;
  }

  #threadKey(resourceId: string | undefined, threadId: string): string {
    return [resourceId ?? '', threadId].join(AGENT_THREAD_KEY_SEPARATOR);
  }

  #threadIdFromKey(key: string): string {
    return key.slice(key.indexOf(AGENT_THREAD_KEY_SEPARATOR) + AGENT_THREAD_KEY_SEPARATOR.length);
  }

  #findUniqueActiveThreadRunByThreadId(
    state: AgentThreadRuntimeState,
    threadId: string,
  ): { key: string; runId: string } | undefined {
    let match: { key: string; runId: string } | undefined;
    for (const [candidateKey, candidateRunId] of state.activeThreadRunIds.entries()) {
      if (this.#threadIdFromKey(candidateKey) !== threadId || state.abortedRunIds.has(candidateRunId)) continue;
      if (match && match.runId !== candidateRunId) {
        throw new Error('resourceId is required when multiple active agent runs match signal target');
      }
      match = { key: candidateKey, runId: candidateRunId };
    }
    return match;
  }

  #threadTopic(key: string): string {
    return `${AGENT_THREAD_STREAM_TOPIC_PREFIX}.${encodeURIComponent(key)}`;
  }

  #isApprovalSuspendedRun(state: AgentThreadRuntimeState, runId: string) {
    return state.approvalSuspendedRunIds.has(runId);
  }

  #isSuspendedRun(state: AgentThreadRuntimeState, runId: string) {
    return state.suspendedRunIds.has(runId) || this.#isApprovalSuspendedRun(state, runId);
  }

  #isThreadBlockingRun(state: AgentThreadRuntimeState, record: AgentThreadRunRecord<any>) {
    return (
      record.output.status === 'running' ||
      record.output.status === 'suspended' ||
      record.lifecycle === 'suspending' ||
      record.lifecycle === 'suspended' ||
      !!record.suspension ||
      this.#isSuspendedRun(state, record.runId)
    );
  }

  #serializeSignal(signal: CreatedAgentSignal): SerializableAgentSignal {
    return signal;
  }

  #nextStreamIdentity(state: AgentThreadRuntimeState, runId: string) {
    const streamSeq = (state.streamSeqByRunId.get(runId) ?? 0) + 1;
    state.streamSeqByRunId.set(runId, streamSeq);
    return { streamId: randomUUID(), streamSeq };
  }

  #markRunSuspending(
    state: AgentThreadRuntimeState,
    runId: string,
    streamId: string,
    suspension: AgentThreadRunSuspension,
  ) {
    state.suspendedRunIds.add(runId);
    state.suspensionMetadataByRunId.set(runId, suspension);
    const record = state.threadRunsByStreamId.get(streamId) ?? state.threadRunsById.get(runId);
    if (record) {
      record.lifecycle = 'suspending';
      record.suspension = suspension;
    }
    if (suspension.kind === 'approval') {
      state.approvalSuspendedRunIds.add(runId);
    }
  }

  #clearSuspendedRun(state: AgentThreadRuntimeState, runId: string) {
    state.suspendedRunIds.delete(runId);
    state.suspensionMetadataByRunId.delete(runId);
    state.approvalSuspendedRunIds.delete(runId);
  }

  getThreadState(options: { resourceId?: string; threadId: string }, pubsub?: PubSub): AgentThreadState {
    const state = this.#getState(pubsub);
    const key = this.#threadKey(options.resourceId, options.threadId);
    const activeRunId = state.activeThreadRunIds.get(key);
    if (!activeRunId) return 'idle';

    const activeRecord = state.threadRunsById.get(activeRunId);
    if (activeRecord && !this.#isThreadBlockingRun(state, activeRecord)) {
      state.activeThreadRunIds.delete(key);
      return 'idle';
    }

    return 'active';
  }

  #publish(pubsub: PubSub | undefined, key: string, event: AgentThreadStreamRuntimeEvent) {
    void this.#publishAndWait(pubsub, key, event).catch(() => {});
  }

  async #publishAndWait(pubsub: PubSub | undefined, key: string, event: AgentThreadStreamRuntimeEvent) {
    await this.#getPubSub(pubsub).publish(this.#threadTopic(key), {
      type: event.type,
      runId: event.runId,
      data: event,
    });
  }

  async #publishRegistrationAndWait(
    pubsub: PubSub | undefined,
    key: string,
    event: Extract<AgentThreadStreamRuntimeEvent, { type: 'run-registered' }>,
  ): Promise<void> {
    try {
      await this.#publishAndWait(pubsub, key, event);
    } catch (error) {
      if (error instanceof AgentThreadOutputDrainError) throw error;
      throw new AgentThreadOutputDrainError(
        'registration-publish-failed',
        `Failed to publish run-registered for agent thread run ${event.runId}`,
        error,
      );
    }
  }

  async #publishTerminalAndWait(
    pubsub: PubSub | undefined,
    key: string,
    event: Extract<AgentThreadStreamRuntimeEvent, { type: 'run-completed' | 'run-suspended' }>,
  ): Promise<void> {
    try {
      await waitWithTimeout(
        this.#publishAndWait(pubsub, key, event),
        TERMINAL_PUBLISH_TIMEOUT_MS,
        () =>
          new AgentThreadOutputDrainError(
            'terminal-publish-failed',
            `Timed out publishing ${event.type} for agent thread run ${event.runId}`,
          ),
      );
    } catch (error) {
      if (error instanceof AgentThreadOutputDrainError) throw error;
      throw new AgentThreadOutputDrainError(
        'terminal-publish-failed',
        `Failed to publish ${event.type} for agent thread run ${event.runId}`,
        error,
      );
    }
  }

  #withBroadcastStream<OUTPUT>(
    output: MastraModelOutput<OUTPUT>,
    pubsub: PubSub | undefined,
    key: string,
    streamId: string,
  ) {
    const runtime = this;

    const parts: unknown[] = [];
    const waiters = new Set<() => void>();
    let started = false;
    let done = false;
    let error: unknown;
    // Presence-tracked separately: a source that throws a FALSY value
    // (undefined/null/0/'') must still fail the subscriber stream instead of
    // silently closing it (PF-802 / PR #204 review).
    let hasError = false;

    const wake = () => {
      const pending = [...waiters];
      waiters.clear();
      for (const waiter of pending) waiter();
    };

    // A real `MastraModelOutput` exposes `fullStream` as an evented getter that
    // yields a fresh independent stream on every access, so the drain loop can
    // read it lazily without competing with the caller. The synthetic
    // persisted-signal output (#broadcastPersistedSignal) instead defines
    // `fullStream` as a one-shot OWN-PROPERTY ReadableStream that only one
    // consumer can drain. Capture that single stream up front so the drain owns
    // it and subscribers replay the shared buffer instead of racing the drain.
    const ownFullStream = Object.prototype.hasOwnProperty.call(output, 'fullStream');
    const capturedSource = ownFullStream ? (output.fullStream as any) : undefined;

    const emitPart = async (part: unknown) => {
      if (part && typeof part === 'object' && 'type' in part) {
        const typedPart = part as { type?: string; payload?: { toolCallId?: string; toolName?: string } };
        if (typedPart.type === 'tool-call-approval' || typedPart.type === 'tool-call-suspended') {
          runtime.#markRunSuspending(runtime.#getState(pubsub), output.runId, streamId, {
            toolCallId: typedPart.payload?.toolCallId,
            toolName: typedPart.payload?.toolName,
            kind: typedPart.type === 'tool-call-approval' ? 'approval' : 'generic-tool',
          });
        }
      }
      parts.push(part);
      await runtime.#publishAndWait(pubsub, key, {
        type: 'stream-part',
        runId: output.runId,
        streamId,
        part,
        sourceId: runtime.#getSourceId(),
      });
      wake();
    };

    // `start` is idempotent and returns the drain-completion promise so callers
    // (registerRun / #broadcastPersistedSignal) can gate `run-completed` on the
    // stream-part broadcast settling (PR #202/#204: publishing completion off
    // `_waitUntilFinished()` alone let remote subscribers observe completion
    // FIRST and ignore the late parts).
    let drain: Promise<void> | undefined;
    const start = (): Promise<void> => {
      if (started) return drain!;
      started = true;
      drain = (async () => {
        try {
          // For getter-based outputs read `output.fullStream` lazily so the
          // evented getter yields a fresh stream for the drain loop, independent
          // of the caller's own access; for own-property one-shot outputs drain
          // the single captured stream.
          const source = ownFullStream ? capturedSource : (output.fullStream as ReadableStream<unknown> | undefined);
          if (!source) return;

          if (typeof source.getReader === 'function') {
            const reader = source.getReader();
            try {
              while (true) {
                const { value: part, done: streamDone } = await reader.read();
                if (streamDone) break;
                await emitPart(part);
              }
            } finally {
              reader.releaseLock();
            }
          } else {
            for await (const part of source as any) {
              await emitPart(part);
            }
          }
        } catch (caught) {
          error = caught;
          hasError = true;
        } finally {
          done = true;
          wake();
        }
      })();
      return drain;
    };

    const createStream = () => {
      let index = 0;
      let closed = false;
      let waiter: (() => void) | undefined;
      return new ReadableStream({
        async pull(controller) {
          void start();
          while (!closed) {
            if (index < parts.length) {
              controller.enqueue(parts[index++]);
              return;
            }
            if (hasError) {
              controller.error(error);
              return;
            }
            if (done) {
              controller.close();
              return;
            }
            await new Promise<void>(resolve => {
              waiter = resolve;
              waiters.add(resolve);
            });
            if (waiter) {
              waiters.delete(waiter);
              waiter = undefined;
            }
          }
        },
        cancel() {
          closed = true;
          if (waiter) {
            waiters.delete(waiter);
            waiter();
            waiter = undefined;
          }
        },
      });
    };

    return { output, createSubscriberStream: createStream, startBroadcast: start };
  }

  #getThreadTarget(options?: { memory?: AgentExecutionOptions<any>['memory']; requestContext?: RequestContext }) {
    const thread = options?.memory?.thread;
    const threadId =
      (options?.requestContext?.get(MASTRA_THREAD_ID_KEY) as string | undefined) ||
      (typeof thread === 'string' ? thread : thread?.id);
    const resourceId =
      (options?.requestContext?.get(MASTRA_RESOURCE_ID_KEY) as string | undefined) || options?.memory?.resource;

    return { threadId, resourceId };
  }

  prepareRunOptions<OUTPUT>(options: AgentExecutionOptions<OUTPUT>, pubsub?: PubSub): AgentExecutionOptions<OUTPUT> {
    const { threadId } = this.#getThreadTarget(options);
    if (!threadId || !options.runId) return options;

    const state = this.#getState(pubsub);
    const abortController = new AbortController();
    const upstreamAbortSignal = options.abortSignal;
    const abort = () => abortController.abort();
    if (upstreamAbortSignal?.aborted) {
      abort();
    } else {
      upstreamAbortSignal?.addEventListener('abort', abort, { once: true });
    }

    state.preparedRunsById.set(options.runId, {
      abortController,
      cleanup: () => upstreamAbortSignal?.removeEventListener('abort', abort),
    });

    if (state.abortedRunIds.has(options.runId)) {
      abort();
    }

    return {
      ...options,
      abortSignal: abortController.signal,
    };
  }

  reserveRun<OUTPUT>(
    options: AgentExecutionOptions<OUTPUT>,
    pubsub?: PubSub,
    agentId?: string,
  ): (() => void) | undefined {
    const { threadId, resourceId } = this.#getThreadTarget(options);
    const runId = options.runId;
    if (!threadId || !runId) return;

    const state = this.#getState(pubsub);
    const key = this.#threadKey(resourceId, threadId);
    // An approval resume reuses the suspended run's id; drop its retained record
    // so the reservation can re-establish it on the same thread (upstream parity).
    this.#clearApprovalSuspendedRunForResume(state, runId, key);
    const existingKey = state.threadKeysByRunId.get(runId) ?? state.pendingIdleThreadKeysByRunId.get(runId);
    if (existingKey) {
      const reservedAgentId = state.reservedAgentIdsByRunId.get(runId);
      const ownsExistingReservation =
        existingKey === key &&
        Boolean(agentId) &&
        reservedAgentId === agentId &&
        Boolean((options as { _threadRunReservationOwner?: unknown })._threadRunReservationOwner);
      if (!ownsExistingReservation) {
        throw new Error(
          existingKey === key
            ? `Agent thread run id "${runId}" is already reserved`
            : `Agent thread run id "${runId}" is already reserved for another thread`,
        );
      }
      return () => {
        this.#releaseReservedRun(state, pubsub, key, runId, {
          cleanupPrepared: true,
          clearAbort: true,
          rejectOutputWaiters: true,
        });
      };
    }
    const inflightIdleKey = state.inflightIdleThreadKeysByRunId.get(runId);
    let ownsInflightIdle = false;
    if (inflightIdleKey) {
      ownsInflightIdle =
        inflightIdleKey === key &&
        Boolean(agentId) &&
        state.inflightIdleAgentIdsByRunId.get(runId) === agentId &&
        Boolean((options as { _threadRunInflightIdleOwner?: unknown })._threadRunInflightIdleOwner);
      if (!ownsInflightIdle) {
        throw new Error(
          inflightIdleKey === key
            ? `Agent thread run id "${runId}" is already reserved`
            : `Agent thread run id "${runId}" is already reserved for another thread`,
        );
      }
    }
    if (state.activeThreadRunIds.has(key)) return;

    if (ownsInflightIdle) {
      state.inflightIdleThreadKeysByRunId.delete(runId);
      state.inflightIdleAgentIdsByRunId.delete(runId);
    }
    this.#forgetRejectedRunError(state, runId);
    state.activeThreadRunIds.set(key, runId);
    state.threadKeysByRunId.set(runId, key);
    if (agentId) {
      state.reservedAgentIdsByRunId.set(runId, agentId);
    }
    return () => {
      this.#releaseReservedRun(state, pubsub, key, runId, {
        cleanupPrepared: true,
        clearAbort: true,
        rejectOutputWaiters: true,
      });
    };
  }

  retargetReservedRun(
    runId: string | undefined,
    fromTarget: { resourceId?: string; threadId?: string },
    toTarget: { resourceId?: string; threadId?: string },
    pubsub?: PubSub,
    agentId?: string,
  ): boolean {
    if (!runId || !fromTarget.threadId || !toTarget.threadId) return false;

    const state = this.#getState(pubsub);
    const fromKey = this.#threadKey(fromTarget.resourceId, fromTarget.threadId);
    const toKey = this.#threadKey(toTarget.resourceId, toTarget.threadId);
    if (fromKey === toKey) return true;
    if (state.threadRunsById.has(runId) || state.threadKeysByRunId.get(runId) !== fromKey) return false;

    const reservedAgentId = state.reservedAgentIdsByRunId.get(runId);
    if (agentId && reservedAgentId && reservedAgentId !== agentId) {
      throw new Error(`Agent thread run id "${runId}" is reserved by another agent`);
    }

    const activeRunId = state.activeThreadRunIds.get(toKey);
    if (activeRunId && activeRunId !== runId) return false;

    state.activeThreadRunIds.delete(fromKey);
    state.activeThreadRunIds.set(toKey, runId);
    state.threadKeysByRunId.set(runId, toKey);
    this.#resolveReservationWaiters(state, runId);

    const pendingSignals = state.pendingSignalsByThread.get(fromKey);
    if (pendingSignals?.length) {
      state.pendingSignalsByThread.delete(fromKey);
      const existingSignals = state.pendingSignalsByThread.get(toKey) ?? [];
      existingSignals.push(...pendingSignals);
      state.pendingSignalsByThread.set(toKey, existingSignals);
    }
    const preRunSignals = state.preRunSignalsByThread.get(fromKey);
    if (preRunSignals?.length) {
      state.preRunSignalsByThread.delete(fromKey);
      const existingSignals = state.preRunSignalsByThread.get(toKey) ?? [];
      existingSignals.push(...preRunSignals);
      state.preRunSignalsByThread.set(toKey, existingSignals);
    }
    if (state.pendingIdleSignalsByThread.has(fromKey)) {
      void this.#drainPendingIdleSignals(state, pubsub, fromKey).catch(() => {});
    }

    return true;
  }

  releaseRunReservation(
    runId: string | undefined,
    pubsub?: PubSub,
    options: { cleanupPrepared?: boolean; clearAbort?: boolean; rejectOutputWaiters?: boolean } = {},
  ): boolean {
    if (!runId) return false;

    const state = this.#getState(pubsub);
    const key = state.threadKeysByRunId.get(runId) ?? state.pendingIdleThreadKeysByRunId.get(runId);
    if (!key) return false;

    this.#releaseReservedRun(state, pubsub, key, runId, options);
    return true;
  }

  rejectUnregisteredRun(runId: string | undefined, pubsub?: PubSub) {
    if (!runId) return;

    const state = this.#getState(pubsub);
    if (
      state.threadRunsById.has(runId) ||
      state.threadKeysByRunId.has(runId) ||
      state.pendingIdleThreadKeysByRunId.has(runId) ||
      state.inflightIdleThreadKeysByRunId.has(runId) ||
      state.preparedRunsById.has(runId)
    ) {
      return;
    }
    this.#forgetCallerSignalsForRun(state, runId);
    this.#rejectPendingOutputWaiters(state, runId, new Error(`Agent thread run id "${runId}" was rejected`));
  }

  abortRun(runId: string, pubsub?: PubSub): boolean {
    const state = this.#getState(pubsub);
    const preparedRun = state.preparedRunsById.get(runId);
    if (!preparedRun) {
      const key = state.threadKeysByRunId.get(runId);
      if (key) {
        this.#rememberAbortedRun(state, runId);
        this.#releaseReservedRun(state, pubsub, key, runId, { rejectOutputWaiters: true });
        return true;
      }
      const pendingIdleKey = state.pendingIdleThreadKeysByRunId.get(runId);
      if (pendingIdleKey) {
        this.#rememberAbortedRun(state, runId);
        this.#removePendingIdleRun(state, pendingIdleKey, runId, true);
        this.#publish(pubsub, pendingIdleKey, { type: 'run-aborted', runId });
        return true;
      }
      const inflightIdleKey = state.inflightIdleThreadKeysByRunId.get(runId);
      if (inflightIdleKey) {
        this.#rememberAbortedRun(state, runId);
        state.inflightIdleThreadKeysByRunId.delete(runId);
        state.inflightIdleAgentIdsByRunId.delete(runId);
        this.#forgetCallerSignalsForRun(state, runId);
        this.#rejectPendingOutputWaiters(state, runId, new Error(`Agent thread run id "${runId}" has been aborted`));
        this.#publish(pubsub, inflightIdleKey, { type: 'run-aborted', runId });
        return true;
      }
      return false;
    }

    const key = state.threadKeysByRunId.get(runId);
    if (key && !state.threadRunsById.has(runId)) {
      preparedRun.abortController.abort();
      this.#rememberAbortedRun(state, runId);
      preparedRun.cleanup();
      state.preparedRunsById.delete(runId);
      this.#releaseReservedRun(state, pubsub, key, runId, { rejectOutputWaiters: true });
      return true;
    }

    preparedRun.abortController.abort();
    this.#rememberAbortedRun(state, runId);

    if (key) {
      state.pendingSignalsByThread.delete(key);
      this.#releaseThreadLease(pubsub, key, runId);
      this.#publish(pubsub, key, { type: 'run-aborted', runId });
    }

    return true;
  }

  getActiveThreadRunId(options: AgentSubscribeToThreadOptions, pubsub?: PubSub): string | undefined {
    const state = this.#getState(pubsub);
    const key = this.#threadKey(options.resourceId, options.threadId);
    const activeRunId = state.activeThreadRunIds.get(key);
    if (!activeRunId) return undefined;

    const record = state.threadRunsById.get(activeRunId);
    if (record && !this.#isThreadBlockingRun(state, record)) return undefined;

    return activeRunId;
  }

  getResumableThreadRun(
    options: AgentSubscribeToThreadOptions & { runId: string; toolCallId?: string },
    pubsub?: PubSub,
  ): { runId: string; toolCallId?: string } | undefined {
    const state = this.#getState(pubsub);
    const key = this.#threadKey(options.resourceId, options.threadId);
    const record = state.threadRunsById.get(options.runId);
    const isSuspended = this.#isSuspendedRun(state, options.runId);
    if (!record || state.threadKeysByRunId.get(options.runId) !== key || !isSuspended) {
      return undefined;
    }

    const suspension = record.suspension ?? state.suspensionMetadataByRunId.get(options.runId);
    if (options.toolCallId && suspension?.toolCallId && suspension.toolCallId !== options.toolCallId) {
      return undefined;
    }

    return { runId: options.runId, toolCallId: options.toolCallId ?? suspension?.toolCallId };
  }

  abortThread(options: AgentSubscribeToThreadOptions, pubsub?: PubSub): boolean {
    const activeRunId = this.getActiveThreadRunId(options, pubsub);
    if (!activeRunId) return false;
    return this.abortRun(activeRunId, pubsub);
  }

  /** @internal */
  resetForTests() {
    for (const pubsub of [defaultAgentThreadPubSub]) {
      this.#resetState(pubsub);
      void (pubsub as { close?: () => Promise<void> }).close?.();
    }
    defaultAgentThreadPubSub = new EventEmitterPubSub();
  }

  #resetState(pubsub: PubSub) {
    const state = this.#statesByPubSub.get(pubsub);
    if (!state) return;

    state.preparedRunsById.forEach(preparedRun => {
      preparedRun.abortController.abort();
      preparedRun.cleanup();
    });
    state.leaseRenewalTimers.forEach(timer => clearInterval(timer));
    state.leaseRenewalTimers.clear();
    state.threadRunsById.clear();
    state.threadRunsByStreamId.clear();
    state.threadKeysByRunId.clear();
    state.activeThreadRunIds.clear();
    state.approvalSuspendedRunIds.clear();
    state.suspendedRunIds.clear();
    state.suspensionMetadataByRunId.clear();
    state.pendingSignalsByThread.clear();
    state.preRunSignalsByThread.clear();
    state.pendingIdleSignalsByThread.clear();
    state.pendingIdleThreadKeysByRunId.clear();
    state.inflightIdleThreadKeysByRunId.clear();
    state.inflightIdleAgentIdsByRunId.clear();
    state.pendingContinuationsByThread.clear();
    state.activeThreadStreamIds.clear();
    state.streamSeqByRunId.clear();
    state.watchedThreadStreamIds.clear();
    state.preparedRunsById.clear();
    state.reservedAgentIdsByRunId.clear();
    state.reservationWaitersByRunId.clear();
    for (const runId of state.abortedRunIds) {
      this.#forgetAbortedRun(state, runId);
    }
    for (const runId of state.rejectedRunErrorsByRunId.keys()) {
      this.#forgetRejectedRunError(state, runId);
    }
    state.acceptedCallerSignals.clear();
    state.callerSignalIdsByRunId.clear();
    for (const runId of state.pendingOutputWaiters.keys()) {
      this.#rejectPendingOutputWaiters(state, runId, new Error(`Agent thread run id "${runId}" was reset`));
    }
    for (const runId of state.rejectedRunErrorsByRunId.keys()) {
      this.#forgetRejectedRunError(state, runId);
    }
    state.registrationPublishesByRunId.clear();
    state.broadcastsByRunId.clear();
  }

  #cleanupPreparedRun(state: AgentThreadRuntimeState, runId: string, preserveAbort = false) {
    state.preparedRunsById.get(runId)?.cleanup();
    state.preparedRunsById.delete(runId);
    if (!preserveAbort) this.#forgetAbortedRun(state, runId);
  }

  #forgetAbortedRun(state: AgentThreadRuntimeState, runId: string) {
    const cleanupTimer = state.abortedRunCleanupTimersByRunId.get(runId);
    if (cleanupTimer) {
      clearTimeout(cleanupTimer);
      state.abortedRunCleanupTimersByRunId.delete(runId);
    }
    state.abortedRunIds.delete(runId);
  }

  #rememberAbortedRun(state: AgentThreadRuntimeState, runId: string) {
    this.#forgetAbortedRun(state, runId);

    const cleanupTimer = setTimeout(() => {
      state.abortedRunIds.delete(runId);
      state.abortedRunCleanupTimersByRunId.delete(runId);
    }, ABORTED_RUN_TOMBSTONE_TTL_MS);
    (cleanupTimer as { unref?: () => void }).unref?.();
    state.abortedRunIds.add(runId);
    state.abortedRunCleanupTimersByRunId.set(runId, cleanupTimer);

    if (state.abortedRunIds.size <= MAX_ABORTED_RUN_TOMBSTONES) return;

    const oldestRunId = state.abortedRunIds.values().next().value;
    if (oldestRunId) {
      this.#forgetAbortedRun(state, oldestRunId);
    }
  }

  #forgetRejectedRunError(state: AgentThreadRuntimeState, runId: string) {
    const rejectedRunError = state.rejectedRunErrorsByRunId.get(runId);
    if (!rejectedRunError) return;

    clearTimeout(rejectedRunError.cleanupTimer);
    state.rejectedRunErrorsByRunId.delete(runId);
  }

  #rememberRejectedRunError(state: AgentThreadRuntimeState, runId: string, error: Error) {
    this.#forgetRejectedRunError(state, runId);

    const cleanupTimer = setTimeout(() => {
      state.rejectedRunErrorsByRunId.delete(runId);
    }, REJECTED_RUN_TOMBSTONE_TTL_MS);
    (cleanupTimer as { unref?: () => void }).unref?.();
    state.rejectedRunErrorsByRunId.set(runId, { error, cleanupTimer });

    if (state.rejectedRunErrorsByRunId.size <= MAX_REJECTED_RUN_TOMBSTONES) return;

    const oldestRunId = state.rejectedRunErrorsByRunId.keys().next().value;
    if (oldestRunId) {
      this.#forgetRejectedRunError(state, oldestRunId);
    }
  }

  #forgetCallerSignalsForRun(state: AgentThreadRuntimeState, runId: string) {
    const callerSignalIds = state.callerSignalIdsByRunId.get(runId);
    if (!callerSignalIds) return;
    state.callerSignalIdsByRunId.delete(runId);
    for (const callerSignalId of callerSignalIds) state.acceptedCallerSignals.delete(callerSignalId);
  }

  #resolveReservationWaiters(state: AgentThreadRuntimeState, runId: string) {
    const waiters = state.reservationWaitersByRunId.get(runId);
    if (!waiters) return;

    state.reservationWaitersByRunId.delete(runId);
    for (const resolve of waiters) resolve();
  }

  #rejectPendingOutputWaiters(state: AgentThreadRuntimeState, runId: string, error: Error) {
    this.#rememberRejectedRunError(state, runId, error);
    const waiters = state.pendingOutputWaiters.get(runId);
    if (!waiters) return;

    state.pendingOutputWaiters.delete(runId);
    for (const waiter of waiters) waiter.reject(error);
  }

  #removePendingIdleRun(state: AgentThreadRuntimeState, key: string, runId: string, reject = false) {
    state.pendingIdleThreadKeysByRunId.delete(runId);
    const queue = state.pendingIdleSignalsByThread.get(key);
    if (!queue) return false;

    const index = queue.findIndex(pendingIdle => pendingIdle.runId === runId);
    if (index === -1) return false;

    const [pendingIdle] = queue.splice(index, 1);
    if (queue.length === 0) {
      state.pendingIdleSignalsByThread.delete(key);
    }
    this.#forgetCallerSignalsForRun(state, runId);
    if (reject) {
      const error = state.abortedRunIds.has(runId)
        ? new Error(`Agent thread run id "${runId}" has been aborted`)
        : new Error(`Agent thread run id "${runId}" was rejected`);
      this.#rejectPendingOutputWaiters(state, runId, error);
      pendingIdle?.onRunRejected?.();
    }
    return true;
  }

  #releaseReservedRun(
    state: AgentThreadRuntimeState,
    pubsub: PubSub | undefined,
    key: string,
    runId: string,
    options: { cleanupPrepared?: boolean; clearAbort?: boolean; rejectOutputWaiters?: boolean } = {},
  ) {
    const ownsThread = state.activeThreadRunIds.get(key) === runId || state.threadKeysByRunId.get(runId) === key;
    const wasAborted = state.abortedRunIds.has(runId);
    if (state.activeThreadRunIds.get(key) === runId) {
      state.activeThreadRunIds.delete(key);
    }
    if (state.threadKeysByRunId.get(runId) === key) {
      state.threadKeysByRunId.delete(runId);
    }
    if (state.pendingIdleThreadKeysByRunId.get(runId) === key) {
      this.#removePendingIdleRun(state, key, runId, Boolean(options.rejectOutputWaiters));
    }
    state.reservedAgentIdsByRunId.delete(runId);
    if (ownsThread) {
      state.pendingSignalsByThread.delete(key);
      state.preRunSignalsByThread.delete(key);
    }
    if (options.cleanupPrepared) {
      this.#cleanupPreparedRun(state, runId, Boolean(options.rejectOutputWaiters && wasAborted));
    } else if (options.clearAbort) {
      this.#forgetAbortedRun(state, runId);
    }
    this.#forgetCallerSignalsForRun(state, runId);
    this.#resolveReservationWaiters(state, runId);
    if (options.rejectOutputWaiters) {
      const error = wasAborted
        ? new Error(`Agent thread run id "${runId}" has been aborted`)
        : new Error(`Agent thread run id "${runId}" was rejected`);
      this.#rejectPendingOutputWaiters(state, runId, error);
    }
    // A suspended owner can hand its distributed lease to a fresh-turn
    // reservation before that successor has registered an output. If setup
    // then fails, release the transferred lease here; otherwise its renewal
    // timer would keep the thread locked indefinitely. Registered runs own
    // their lease through the normal completion finalizer instead.
    if (ownsThread && !state.threadRunsById.has(runId) && state.leaseRenewalTimers.has(runId)) {
      this.#releaseThreadLease(pubsub, key, runId);
    }
    if (ownsThread) {
      this.#publish(pubsub, key, { type: 'run-aborted', runId });
      void this.#drainPendingIdleSignals(state, pubsub, key).catch(() => {});
    }
  }

  /**
   * Clears the retained record for an approval-suspended run so it can be
   * re-reserved/re-registered by a resume on the same thread.
   *
   * The completion finalizer leaves `threadRunsById`/`threadKeysByRunId`/
   * `activeThreadRunIds`/`approvalSuspendedRunIds` in place for an
   * approval-suspended run so the thread stays blocked awaiting approval. The
   * fork's reservation guards (`reserveRun`/`registerRun`) reject a runId that
   * is still registered, which would wedge an approval resume that reuses the
   * same runId. Upstream resumes by simply re-`registerRun`ing the same id
   * (overwriting the record); we reproduce that within the fork's machinery by
   * quietly dropping the stale record here — without aborting subscribers,
   * rejecting waiters, or dropping pending signals queued behind the approval
   * (those must still flow to the resumed run). Returns true if a stale
   * approval-suspended record was cleared for this thread key.
   */
  #clearApprovalSuspendedRunForResume(state: AgentThreadRuntimeState, runId: string, key: string): boolean {
    // A retained record is released to a same-runId re-registration when the
    // prior segment can no longer produce visible work: any suspension kind
    // (suspended records are retained precisely so a later resume can
    // re-attach) or a segment already marked completed whose terminal delivery
    // is still being finalized by its completion watcher (the watcher's later
    // cleanup is identity/streamId-guarded, so it cannot clobber the new
    // registration). A live 'running' record still rejects duplicates.
    const retainedRecord = state.threadRunsById.get(runId);
    const suspended = this.#isSuspendedRun(state, runId) || retainedRecord?.lifecycle === 'suspended';
    const terminalInFlight = retainedRecord?.lifecycle === 'completed';
    if (!suspended && !terminalInFlight) return false;
    const reservedKey = state.threadKeysByRunId.get(runId);
    if (reservedKey !== undefined && reservedKey !== key) return false;

    this.#clearSuspendedRun(state, runId);
    state.threadRunsById.delete(runId);
    if (retainedRecord) {
      state.threadRunsByStreamId.delete(retainedRecord.streamId);
      // The suspended run's watcher entry was already removed by its finalizer;
      // the resume registers a fresh streamId, so no stale watch entry remains.
    }
    if (state.threadKeysByRunId.get(runId) === key) {
      state.threadKeysByRunId.delete(runId);
    }
    if (state.activeThreadRunIds.get(key) === runId) {
      state.activeThreadRunIds.delete(key);
      state.activeThreadStreamIds.delete(key);
    }
    state.reservedAgentIdsByRunId.delete(runId);
    return true;
  }

  async #persistSignal(
    agent: Agent<any, any, any, any>,
    signal: CreatedAgentSignal,
    resourceId: string,
    threadId: string,
    requestContext?: RequestContext,
  ) {
    const memory = await agent.getMemory({ requestContext });
    if (!memory) return;
    await memory.saveMessages({
      messages: [signal.toDBMessage({ resourceId, threadId })],
    });
  }

  #broadcastPersistedSignal(
    state: AgentThreadRuntimeState,
    pubsub: PubSub | undefined,
    key: string,
    runId: string,
    signal: CreatedAgentSignal,
    resourceId: string,
    threadId: string,
  ) {
    let finish!: () => void;
    const finished = new Promise<void>(resolve => {
      finish = resolve;
    });
    const parts: any[] = [
      { type: 'start', runId },
      { ...signal.toDataPart(), runId },
      {
        type: 'finish',
        runId,
        payload: {
          stepResult: { reason: 'stop' },
          usage: { inputTokens: 0, outputTokens: 0, totalTokens: 0 },
        },
      },
    ];
    const output = {
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
    } as MastraModelOutput<any>;
    const { streamId, streamSeq } = this.#nextStreamIdentity(state, runId);
    const {
      output: outputForSubscribers,
      createSubscriberStream,
      startBroadcast,
    } = this.#withBroadcastStream(output, pubsub, key, streamId);
    const record: AgentThreadRunRecord<any> = {
      agent: { id: `persisted-signal:${signal.id}` } as Agent<any, any, any, any>,
      output,
      runId,
      streamId,
      streamSeq,
      lifecycle: 'running',
      threadId,
      resourceId,
      streamOptions: {},
      createSubscriberStream,
    };

    state.threadRunsById.set(runId, record);
    state.threadRunsByStreamId.set(streamId, record);
    state.threadKeysByRunId.set(runId, key);
    state.activeThreadStreamIds.set(key, streamId);
    const registered = this.#publishAndWait(pubsub, key, { type: 'run-registered', runId, streamId, streamSeq });
    const broadcast = registered.then(startBroadcast, startBroadcast).catch(() => {});
    // PR #202 review (P2): the synthetic output's `finish()` fires at stream
    // CONSTRUCTION, long before an async pubsub has published the stream-parts.
    // Publishing `run-completed` off `_waitUntilFinished()` alone let remote
    // subscribers observe completion FIRST, delete the remote run, and ignore
    // the late persisted-signal parts. Gate completion on registration + the
    // stream-part broadcast settling.
    void Promise.allSettled([outputForSubscribers._waitUntilFinished(), broadcast]).then(() => {
      setTimeout(() => {
        state.threadRunsByStreamId.delete(streamId);
        if (state.threadRunsById.get(runId) === record) {
          state.threadRunsById.delete(runId);
          state.threadKeysByRunId.delete(runId);
        }
        if (state.activeThreadRunIds.get(key) === runId && state.activeThreadStreamIds.get(key) === streamId) {
          state.activeThreadRunIds.delete(key);
          state.activeThreadStreamIds.delete(key);
        }
        this.#releaseThreadLease(pubsub, key, runId);
        this.#publish(pubsub, key, { type: 'run-completed', runId, streamId });
      }, 0);
    });
  }

  async #persistAndBroadcastIdleSignal(
    state: AgentThreadRuntimeState,
    pubsub: PubSub | undefined,
    key: string,
    runId: string,
    agent: Agent<any, any, any, any>,
    signal: CreatedAgentSignal,
    resourceId: string,
    threadId: string,
    requestContext?: RequestContext,
  ) {
    await this.#persistSignal(agent, signal, resourceId, threadId, requestContext);
    this.#broadcastPersistedSignal(state, pubsub, key, runId, signal, resourceId, threadId);
  }

  registerRun<OUTPUT>(
    agent: Agent<any, any, any, any>,
    output: MastraModelOutput<OUTPUT>,
    streamOptions: AgentExecutionOptions<OUTPUT>,
    pubsub?: PubSub,
  ): Promise<void> | undefined {
    const { threadId, resourceId } = this.#getThreadTarget(streamOptions);
    if (!threadId) return;

    const state = this.#getState(pubsub);
    const key = this.#threadKey(resourceId, threadId);
    // An approval resume re-registers the suspended run's id; drop its retained
    // record so registration overwrites it on the same thread (upstream parity).
    this.#clearApprovalSuspendedRunForResume(state, output.runId, key);
    const existingKey =
      state.threadKeysByRunId.get(output.runId) ?? state.pendingIdleThreadKeysByRunId.get(output.runId);
    const inflightIdleKey = state.inflightIdleThreadKeysByRunId.get(output.runId);
    const activeRunId = state.activeThreadRunIds.get(key);
    const reservedAgentId = state.reservedAgentIdsByRunId.get(output.runId);
    const rejectedRunError = state.rejectedRunErrorsByRunId.get(output.runId);
    if (state.abortedRunIds.has(output.runId)) {
      throw new Error(`Agent thread run id "${output.runId}" has been aborted`);
    }
    if (rejectedRunError) {
      throw rejectedRunError.error;
    }
    if (state.threadRunsById.has(output.runId)) {
      throw new Error(`Agent thread run id "${output.runId}" is already registered`);
    }
    if (inflightIdleKey) {
      const ownsInflightIdle =
        inflightIdleKey === key &&
        state.inflightIdleAgentIdsByRunId.get(output.runId) === agent.id &&
        Boolean((streamOptions as { _threadRunInflightIdleOwner?: unknown })._threadRunInflightIdleOwner);
      if (!ownsInflightIdle) {
        throw new Error(
          inflightIdleKey === key
            ? `Agent thread run id "${output.runId}" is already reserved`
            : `Agent thread run id "${output.runId}" is already reserved for another thread`,
        );
      }
    }
    if (activeRunId && activeRunId !== output.runId) {
      throw new Error(`Agent thread run id "${activeRunId}" is already active for this thread`);
    }
    if (existingKey && existingKey !== key) {
      throw new Error(`Agent thread run id "${output.runId}" is already reserved for another thread`);
    }
    if (reservedAgentId && reservedAgentId !== agent.id) {
      throw new Error(`Agent thread run id "${output.runId}" is reserved by another agent`);
    }
    if (inflightIdleKey) {
      state.inflightIdleThreadKeysByRunId.delete(output.runId);
      state.inflightIdleAgentIdsByRunId.delete(output.runId);
    }
    const { streamId, streamSeq } = this.#nextStreamIdentity(state, output.runId);
    const { createSubscriberStream, startBroadcast } = this.#withBroadcastStream(output, pubsub, key, streamId);
    const record: AgentThreadRunRecord<OUTPUT> = {
      agent,
      output,
      runId: output.runId,
      streamId,
      streamSeq,
      lifecycle: 'running',
      threadId,
      resourceId,
      streamOptions: streamOptions as AgentThreadRunRecord<OUTPUT>['streamOptions'],
      createSubscriberStream,
    };

    this.#clearSuspendedRun(state, output.runId);
    state.threadRunsById.set(output.runId, record);
    state.threadRunsByStreamId.set(streamId, record);
    state.threadKeysByRunId.set(output.runId, key);
    state.activeThreadRunIds.set(key, output.runId);
    state.activeThreadStreamIds.set(key, streamId);
    this.#forgetRejectedRunError(state, output.runId);
    state.reservedAgentIdsByRunId.delete(output.runId);
    this.#resolveReservationWaiters(state, output.runId);
    const waiters = state.pendingOutputWaiters.get(output.runId);
    if (waiters) {
      state.pendingOutputWaiters.delete(output.runId);
      for (const waiter of waiters) waiter.resolve(output);
    }
    // Registration is part of the subscriber's delivery barrier just like the
    // terminal event. Normalize its rejection so a provider run that already
    // executed cannot become retryable merely because PubSub rejected the
    // segment's run-registered publication.
    const registrationPublish = this.#publishRegistrationAndWait(pubsub, key, {
      type: 'run-registered',
      runId: output.runId,
      streamId,
      streamSeq,
    });
    // The Harness output-drain waiter may not attach until the model has already
    // produced FullOutput. Mark the promise observed immediately while retaining
    // the original rejecting promise below for that later waiter.
    void registrationPublish.catch(() => {});
    this.#threadOutputRegistrations.set(output, registrationPublish);
    state.registrationPublishesByRunId.set(output.runId, registrationPublish);
    // Always drive the run's stream to completion, even when no caller consumes
    // the returned output (e.g. a fire-and-forget schedule wake). The broadcast
    // tee buffers every part, so a later/external subscriber still replays the
    // full stream; without this pump the run never reaches a terminal state and
    // its active-run record + thread lease would never release, permanently
    // wedging the thread. The broadcast promise settles when the parts drain
    // finishes; the completion watcher gates `run-completed` on it (PR #202/#204).
    const broadcast = registrationPublish.then(startBroadcast, startBroadcast);
    state.broadcastsByRunId.set(output.runId, broadcast);
    void broadcast.catch(() => {});
    return this.#watchThreadRunCompletion(state, pubsub, key, record);
  }

  /**
   * Returns the `MastraModelOutput` for a registered run, or `undefined` if the
   * run has finished and been cleared. Used by signal-routed callers that send
   * a signal, receive a `runId`, and then need the matching output handle.
   */
  getRunOutput<OUTPUT = unknown>(runId: string, pubsub?: PubSub): MastraModelOutput<OUTPUT> | undefined {
    const state = this.#getState(pubsub);
    const record = state.threadRunsById.get(runId);
    return record?.output as MastraModelOutput<OUTPUT> | undefined;
  }

  /**
   * Resolves with the `MastraModelOutput` for `runId` as soon as `registerRun`
   * registers it, or immediately if it is already registered and retained.
   */
  waitForRunOutput<OUTPUT = unknown>(
    runId: string,
    pubsub?: PubSub,
    abortSignal?: AbortSignal,
  ): Promise<MastraModelOutput<OUTPUT>> {
    const state = this.#getState(pubsub);
    const existing = state.threadRunsById.get(runId);
    if (existing) return Promise.resolve(existing.output as MastraModelOutput<OUTPUT>);
    if (abortSignal?.aborted) {
      return Promise.reject(abortSignal.reason ?? new Error(`Agent thread run id "${runId}" wait was aborted`));
    }
    if (state.abortedRunIds.has(runId)) {
      return Promise.reject(new Error(`Agent thread run id "${runId}" has been aborted`));
    }
    const rejectedRunError = state.rejectedRunErrorsByRunId.get(runId);
    if (rejectedRunError) {
      return Promise.reject(rejectedRunError.error);
    }
    return new Promise<MastraModelOutput<OUTPUT>>((resolve, reject) => {
      const waiters = state.pendingOutputWaiters.get(runId) ?? [];
      let waiter: { resolve: (out: MastraModelOutput<any>) => void; reject: (error: Error) => void };
      const cleanup = () => abortSignal?.removeEventListener('abort', onAbort);
      const onAbort = () => {
        const currentWaiters = state.pendingOutputWaiters.get(runId);
        const index = currentWaiters?.indexOf(waiter) ?? -1;
        if (index !== -1) {
          currentWaiters!.splice(index, 1);
          if (currentWaiters!.length === 0) state.pendingOutputWaiters.delete(runId);
        }
        cleanup();
        reject(abortSignal?.reason ?? new Error(`Agent thread run id "${runId}" wait was aborted`));
      };
      waiter = {
        resolve: out => {
          cleanup();
          resolve(out);
        },
        reject: error => {
          cleanup();
          reject(error);
        },
      };
      abortSignal?.addEventListener('abort', onAbort, { once: true });
      waiters.push(waiter);
      state.pendingOutputWaiters.set(runId, waiters);
    });
  }

  #watchThreadRunCompletion(
    state: AgentThreadRuntimeState,
    pubsub: PubSub | undefined,
    key: string,
    record: AgentThreadRunRecord<any>,
  ): Promise<void> | undefined {
    if (state.watchedThreadStreamIds.has(record.streamId)) return;
    state.watchedThreadStreamIds.add(record.streamId);

    let terminalSettled = false;
    let resolveTerminal!: () => void;
    let rejectTerminal!: (error: AgentThreadOutputDrainError) => void;
    const terminal = new Promise<void>((resolve, reject) => {
      resolveTerminal = () => {
        if (terminalSettled) return;
        terminalSettled = true;
        resolve();
      };
      rejectTerminal = error => {
        if (terminalSettled) return;
        terminalSettled = true;
        reject(error);
      };
    });
    this.#threadOutputTerminals.set(record.output, terminal);
    void terminal.catch(() => {});

    const completion = record.output._waitUntilFinished().finally(async () => {
      try {
        // Gate finalization on the registration publish + the stream-part
        // broadcast settling (PR #202/#204): publishing `run-completed` off
        // `_waitUntilFinished()` alone let remote subscribers observe completion
        // FIRST, delete the remote run, and ignore the late parts.
        await state.registrationPublishesByRunId.get(record.runId)?.catch(() => {});
        state.registrationPublishesByRunId.delete(record.runId);
        await state.broadcastsByRunId.get(record.runId)?.catch(() => {});
        state.broadcastsByRunId.delete(record.runId);
        state.watchedThreadStreamIds.delete(record.streamId);
        this.#cleanupPreparedRun(state, record.runId);

        // A suspended run (approval or generic tool suspension) is paused, not
        // finished: surface run-suspended and leave its records in place so a
        // later resume can re-attach to the same thread.
        if (record.output.status === 'suspended' && this.#isSuspendedRun(state, record.runId)) {
          record.lifecycle = 'suspended';
          await this.#publishTerminalAndWait(pubsub, key, {
            type: 'run-suspended',
            runId: record.runId,
            streamId: record.streamId,
          });
          resolveTerminal();
          return;
        }

        record.lifecycle = 'completed';
        this.#clearSuspendedRun(state, record.runId);
        this.#forgetCallerSignalsForRun(state, record.runId);
        await this.#publishTerminalAndWait(pubsub, key, {
          type: 'run-completed',
          runId: record.runId,
          streamId: record.streamId,
        });
        resolveTerminal();
        state.threadRunsByStreamId.delete(record.streamId);
        if (
          state.activeThreadRunIds.get(key) === record.runId &&
          state.activeThreadStreamIds.get(key) === record.streamId
        ) {
          state.activeThreadRunIds.delete(key);
          state.activeThreadStreamIds.delete(key);
        }
        if (state.threadKeysByRunId.get(record.runId) === key) {
          state.threadKeysByRunId.delete(record.runId);
        }
        // If queued follow-up work exists, keep the cross-process lease held by
        // handing it to the next run instead of releasing it: releasing here
        // would briefly empty the lease key, letting a racing process win it and
        // start a competing run on this thread. The drain runs under the
        // transferred lease and releases it only once every queue is empty. If
        // there's no pending work, release as usual so other processes can wake
        // the thread.
        try {
          if (this.#hasPendingThreadWork(state, key)) {
            await this.#drainPendingSignals(state, pubsub, key, record);
          } else {
            this.#releaseThreadLease(pubsub, key, record.runId);
          }
        } finally {
          if (state.threadRunsById.get(record.runId) === record) {
            state.threadRunsById.delete(record.runId);
          }
          this.#resolveReservationWaiters(state, record.runId);
        }
      } catch (error) {
        if (terminalSettled) throw error;
        const terminalError =
          error instanceof AgentThreadOutputDrainError
            ? error
            : new AgentThreadOutputDrainError(
                'terminal-publish-failed',
                `Failed to finalize terminal delivery for agent thread run ${record.runId}`,
                error,
              );
        rejectTerminal(terminalError);
        this.#clearSuspendedRun(state, record.runId);
        state.pendingSignalsByThread.delete(key);
        state.threadRunsByStreamId.delete(record.streamId);
        if (
          state.activeThreadRunIds.get(key) === record.runId &&
          state.activeThreadStreamIds.get(key) === record.streamId
        ) {
          state.activeThreadRunIds.delete(key);
          state.activeThreadStreamIds.delete(key);
        }
        if (state.threadKeysByRunId.get(record.runId) === key) {
          state.threadKeysByRunId.delete(record.runId);
        }
        if (state.threadRunsById.get(record.runId) === record) {
          state.threadRunsById.delete(record.runId);
        }
        this.#forgetCallerSignalsForRun(state, record.runId);
        this.#releaseThreadLease(pubsub, key, record.runId);
        this.#resolveReservationWaiters(state, record.runId);
        this.#rememberRejectedRunError(state, record.runId, terminalError);
        void this.#drainPendingIdleSignals(state, pubsub, key).catch(() => {});
        throw terminalError;
      }
    });
    void completion.catch(() => {});
    return completion;
  }

  async #drainPendingSignals(
    state: AgentThreadRuntimeState,
    pubsub: PubSub | undefined,
    key: string,
    previousRun: AgentThreadRunRecord<any>,
  ) {
    if (state.activeThreadRunIds.has(key)) {
      return;
    }

    // A run can finish before its first model request drained its pre-run
    // signals (e.g. it errored early). Don't strand them — fold them into the
    // follow-up queue so the next run still picks them up.
    const preRunLeftover = state.preRunSignalsByThread.get(key);
    if (preRunLeftover?.length) {
      state.preRunSignalsByThread.delete(key);
      state.pendingSignalsByThread.set(key, [...preRunLeftover, ...(state.pendingSignalsByThread.get(key) ?? [])]);
    }

    const queue = state.pendingSignalsByThread.get(key);
    const signal = queue?.shift();
    if (signal && queue) {
      if (queue.length === 0) {
        state.pendingSignalsByThread.delete(key);
      }

      // Hand the lease from the finished run to this drained run before
      // streaming, so the lease key never goes empty during the handoff. If the
      // old owner already lost the lease (e.g. a pubsub blip let the TTL lapse
      // and another process took over), forward the signal to the new winner
      // instead of starting a competing run here.
      const nextRunId = randomUUID();
      state.activeThreadRunIds.set(key, nextRunId);
      const owns = await this.#acquireOrTransferThreadLease(pubsub, key, nextRunId, previousRun.runId);
      if (!owns.acquired) {
        if (state.activeThreadRunIds.get(key) === nextRunId) {
          state.activeThreadRunIds.delete(key);
        }
        // Put the signal back at the head so a later drain (or the winner) runs
        // it, and forward it to the current lease owner.
        const restored = state.pendingSignalsByThread.get(key) ?? [];
        state.pendingSignalsByThread.set(key, [signal, ...restored]);
        if (owns.owner) {
          await this.#publishAndWait(pubsub, key, {
            type: 'signal-enqueued',
            runId: owns.owner,
            signal: this.#serializeSignal(signal),
            sourceId: this.#getSourceId(),
          }).catch(() => {});
          state.pendingSignalsByThread.get(key)?.shift();
          if ((state.pendingSignalsByThread.get(key)?.length ?? 0) === 0) {
            state.pendingSignalsByThread.delete(key);
          }
        }
        return;
      }

      // The lease now belongs to nextRunId, so mirror that ownership in the
      // local reservation maps before Agent.stream performs its admission
      // checks. Passing the native owner marker lets Agent.stream adopt this
      // reservation instead of waiting on the run it is itself responsible for
      // starting.
      state.threadKeysByRunId.set(nextRunId, key);
      state.reservedAgentIdsByRunId.set(nextRunId, previousRun.agent.id);

      try {
        const output = await previousRun.agent.stream(signal, {
          ...(previousRun.streamOptions as any),
          _pubsub: this.#getPubSub(pubsub),
          _threadRunReservationOwner: true,
          runId: nextRunId,
          memory: withThreadMemory(
            previousRun.streamOptions.memory,
            previousRun.resourceId ?? '',
            previousRun.threadId ?? '',
          ),
        });

        if (queue.length > 0) {
          const nextRecord = state.threadRunsById.get(output.runId);
          if (nextRecord) {
            void this.#watchThreadRunCompletion(state, pubsub, key, nextRecord);
          }
        }
        return;
      } catch (error) {
        if (state.activeThreadRunIds.get(key) === nextRunId) {
          state.activeThreadRunIds.delete(key);
        }
        if (state.threadKeysByRunId.get(nextRunId) === key) {
          state.threadKeysByRunId.delete(nextRunId);
        }
        state.reservedAgentIdsByRunId.delete(nextRunId);
        this.#cleanupPreparedRun(state, nextRunId);
        this.#forgetCallerSignalsForRun(state, nextRunId);
        this.#resolveReservationWaiters(state, nextRunId);
        const runError = getErrorFromUnknown(error);
        this.#rejectPendingOutputWaiters(state, nextRunId, runError);
        this.#publish(pubsub, key, {
          type: 'run-failed',
          runId: nextRunId,
          error: runError.message,
        });

        // Preserve FIFO progress for any work queued behind the failed setup.
        // The recursive drain transfers the lease from this failed run; with no
        // remaining work, release it directly.
        if (this.#hasPendingThreadWork(state, key)) {
          try {
            await this.#drainPendingSignals(state, pubsub, key, { ...previousRun, runId: nextRunId });
          } catch {}
        } else {
          this.#releaseThreadLease(pubsub, key, nextRunId);
        }
        throw error;
      }
    }

    if (await this.#drainPendingContinuations(state, pubsub, key, previousRun.runId)) {
      return;
    }

    if (await this.#drainPendingIdleSignals(state, pubsub, key, previousRun.runId)) {
      return;
    }

    // Nothing left to drain: release the lease we kept held for the drain.
    this.#releaseThreadLease(pubsub, key, previousRun.runId);
  }

  async #drainPendingContinuations(
    state: AgentThreadRuntimeState,
    pubsub: PubSub | undefined,
    key: string,
    fromRunId?: string,
  ) {
    if (state.activeThreadRunIds.has(key)) {
      return false;
    }

    const queue = state.pendingContinuationsByThread.get(key);
    const pending = queue?.shift();
    if (!pending || !queue) {
      return false;
    }
    if (queue.length === 0) {
      state.pendingContinuationsByThread.delete(key);
    }

    // A continuation only ever drains in the process that owned the finished
    // run, so it always carries a `fromRunId` to hand the held lease to. If the
    // old owner already lost the lease, re-queue the continuation and let the
    // new lease owner take over rather than starting a competing run here.
    if (fromRunId) {
      state.activeThreadRunIds.set(key, pending.runId);
      const owns = await this.#acquireOrTransferThreadLease(pubsub, key, pending.runId, fromRunId);
      if (!owns.acquired) {
        if (state.activeThreadRunIds.get(key) === pending.runId) {
          state.activeThreadRunIds.delete(key);
        }
        const restored = state.pendingContinuationsByThread.get(key) ?? [];
        state.pendingContinuationsByThread.set(key, [pending, ...restored]);
        return false;
      }
    }

    this.#startContinuation(state, pubsub, key, pending);
    return true;
  }

  #startContinuation(
    state: AgentThreadRuntimeState,
    pubsub: PubSub | undefined,
    key: string,
    pending: PendingContinuation<any>,
  ) {
    state.activeThreadRunIds.set(key, pending.runId);
    state.threadKeysByRunId.set(pending.runId, key);
    void pending.agent
      .stream(pending.messages, {
        ...(pending.streamOptions as any),
        runId: pending.runId,
        memory: withThreadMemory(pending.streamOptions?.memory, pending.resourceId, pending.threadId),
      })
      .then(output => {
        if ((state.pendingContinuationsByThread.get(key)?.length ?? 0) > 0) {
          const nextRecord = state.threadRunsById.get(output.runId);
          if (nextRecord) {
            void this.#watchThreadRunCompletion(state, pubsub, key, nextRecord);
          }
        }
      })
      .catch(err => {
        state.threadKeysByRunId.delete(pending.runId);
        this.#cleanupPreparedRun(state, pending.runId);
        if (state.activeThreadRunIds.get(key) === pending.runId) {
          state.activeThreadRunIds.delete(key);
        }
        this.#publish(pubsub, key, {
          type: 'run-failed',
          runId: pending.runId,
          error: getErrorFromUnknown(err).message,
        });
        // Hand the lease to remaining queued work (transfer keeps the key from
        // going empty); only release once nothing is left to drain.
        void this.#drainPendingContinuations(state, pubsub, key, pending.runId).then(async started => {
          if (started) return;
          if (await this.#drainPendingIdleSignals(state, pubsub, key, pending.runId)) return;
          this.#releaseThreadLease(pubsub, key, pending.runId);
        });
      });
  }

  continueWithMessages<OUTPUT = unknown>(
    agent: Agent<any, any, any, any>,
    messages: MessageListInput,
    target: { resourceId: string; threadId: string; streamOptions?: AgentExecutionOptions<OUTPUT>; runId?: string },
    pubsub?: PubSub,
  ): { accepted: true; runId: string } {
    const state = this.#getState(pubsub);
    const key = this.#threadKey(target.resourceId, target.threadId);
    const runId = target.runId ?? randomUUID();
    const pending: PendingContinuation<OUTPUT> = {
      agent,
      messages,
      runId,
      resourceId: target.resourceId,
      threadId: target.threadId,
      streamOptions: target.streamOptions,
    };

    const activeRunId = state.activeThreadRunIds.get(key);
    const activeRecord = activeRunId ? state.threadRunsById.get(activeRunId) : undefined;
    if (state.activeThreadRunIds.has(key)) {
      const queue = state.pendingContinuationsByThread.get(key) ?? [];
      queue.push(pending);
      state.pendingContinuationsByThread.set(key, queue);
      if (activeRecord) {
        void this.#watchThreadRunCompletion(state, pubsub, key, activeRecord);
      }
      return { accepted: true, runId };
    }

    this.#startContinuation(state, pubsub, key, pending);
    return { accepted: true, runId };
  }

  async #drainPendingIdleSignals(
    state: AgentThreadRuntimeState,
    pubsub: PubSub | undefined,
    key: string,
    fromRunId?: string,
  ): Promise<boolean> {
    if (state.activeThreadRunIds.has(key)) {
      return false;
    }

    const idleQueue = state.pendingIdleSignalsByThread.get(key);
    const pendingIdle = idleQueue?.shift();
    if (!pendingIdle || !idleQueue) {
      return false;
    }
    if (idleQueue.length === 0) {
      state.pendingIdleSignalsByThread.delete(key);
    }
    state.pendingIdleThreadKeysByRunId.delete(pendingIdle.runId);

    const existingRunKey = state.threadKeysByRunId.get(pendingIdle.runId);
    if (existingRunKey && existingRunKey !== key) {
      pendingIdle.onRunRejected?.();
      this.#releaseReservedRun(state, pubsub, existingRunKey, pendingIdle.runId, {
        cleanupPrepared: true,
        clearAbort: true,
        rejectOutputWaiters: true,
      });
      return this.#drainPendingIdleSignals(state, pubsub, key, fromRunId);
    }
    if (state.threadRunsById.has(pendingIdle.runId)) {
      pendingIdle.onRunRejected?.();
      this.#releaseReservedRun(state, pubsub, key, pendingIdle.runId, {
        cleanupPrepared: true,
        clearAbort: true,
        rejectOutputWaiters: true,
      });
      return this.#drainPendingIdleSignals(state, pubsub, key, fromRunId);
    }
    const reserveBeforePreflight = pendingIdle.reserveBeforePreflight ?? true;
    if (reserveBeforePreflight) {
      state.activeThreadRunIds.set(key, pendingIdle.runId);
      state.threadKeysByRunId.set(pendingIdle.runId, key);
      state.reservedAgentIdsByRunId.set(pendingIdle.runId, pendingIdle.agent.id);
    } else {
      state.inflightIdleThreadKeysByRunId.set(pendingIdle.runId, key);
      state.inflightIdleAgentIdsByRunId.set(pendingIdle.runId, pendingIdle.agent.id);
    }

    // A queued idle signal may be draining either in the process that just
    // finished a run (it still holds the lease — hand it over) or in a
    // *different* process that observed the owner's run finish via pub/sub and
    // now wants to wake the thread (it holds no lease — it must win one). Either
    // way the run must only start if this process owns the cross-process lease,
    // otherwise two processes could each start a competing idle run.
    const owns = await this.#acquireOrTransferThreadLease(pubsub, key, pendingIdle.runId, fromRunId);
    if (!owns.acquired) {
      // Lost the wake race. Roll back the optimistic local reservation and
      // forward the signal to the winner so it is not dropped, then try the
      // next queued idle signal (which may belong to a different run we can win).
      if (reserveBeforePreflight) {
        this.#releaseReservedRun(state, pubsub, key, pendingIdle.runId, {
          cleanupPrepared: true,
          clearAbort: true,
          rejectOutputWaiters: true,
        });
      } else {
        state.inflightIdleThreadKeysByRunId.delete(pendingIdle.runId);
        state.inflightIdleAgentIdsByRunId.delete(pendingIdle.runId);
      }
      if (owns.owner) {
        await this.#publishAndWait(pubsub, key, {
          type: 'signal-enqueued',
          runId: owns.owner,
          signal: this.#serializeSignal(pendingIdle.signal),
          sourceId: this.#getSourceId(),
        }).catch(() => {});
      }
      await this.#drainPendingIdleSignals(state, pubsub, key, fromRunId);
      return true;
    }

    try {
      const output = await pendingIdle.agent.stream(pendingIdle.signal, {
        ...(pendingIdle.streamOptions as any),
        ...(reserveBeforePreflight ? { _threadRunReservationOwner: true } : { _threadRunInflightIdleOwner: true }),
        runId: pendingIdle.runId,
        memory: withThreadMemory(pendingIdle.streamOptions?.memory, pendingIdle.resourceId, pendingIdle.threadId),
      });
      state.inflightIdleThreadKeysByRunId.delete(pendingIdle.runId);
      state.inflightIdleAgentIdsByRunId.delete(pendingIdle.runId);

      const registeredRecord = state.threadRunsById.get(pendingIdle.runId) ?? state.threadRunsById.get(output.runId);
      if (!registeredRecord) {
        // Duck-typed Agent overrides can return a valid output without calling
        // the native registerRun hook. There is no completion watcher in that
        // case, so clear the optimistic reservation and explicitly hand off or
        // release the lease instead of wedging every later wake on this thread.
        if (state.activeThreadRunIds.get(key) === pendingIdle.runId) {
          state.activeThreadRunIds.delete(key);
        }
        if (state.threadKeysByRunId.get(pendingIdle.runId) === key) {
          state.threadKeysByRunId.delete(pendingIdle.runId);
        }
        state.reservedAgentIdsByRunId.delete(pendingIdle.runId);
        this.#resolveReservationWaiters(state, pendingIdle.runId);
        const outputWaiters = state.pendingOutputWaiters.get(pendingIdle.runId);
        if (outputWaiters) {
          state.pendingOutputWaiters.delete(pendingIdle.runId);
          for (const waiter of outputWaiters) waiter.resolve(output);
        }
        if (!(await this.#drainPendingIdleSignals(state, pubsub, key, pendingIdle.runId))) {
          this.#releaseThreadLease(pubsub, key, pendingIdle.runId);
        }
        return true;
      }

      if ((idleQueue?.length ?? 0) > 0) {
        void this.#watchThreadRunCompletion(state, pubsub, key, registeredRecord);
      }
    } catch (err) {
      pendingIdle.onRunRejected?.();
      if (reserveBeforePreflight) {
        this.#releaseReservedRun(state, pubsub, key, pendingIdle.runId, {
          cleanupPrepared: true,
          clearAbort: true,
          rejectOutputWaiters: true,
        });
      } else {
        state.inflightIdleThreadKeysByRunId.delete(pendingIdle.runId);
        state.inflightIdleAgentIdsByRunId.delete(pendingIdle.runId);
        this.rejectUnregisteredRun(pendingIdle.runId, pubsub);
      }
      this.#publish(pubsub, key, {
        type: 'run-failed',
        runId: pendingIdle.runId,
        error: getErrorFromUnknown(err).message,
      });
      // Hand the lease to remaining idle work; release only when none remains.
      if (!(await this.#drainPendingIdleSignals(state, pubsub, key, pendingIdle.runId))) {
        this.#releaseThreadLease(pubsub, key, pendingIdle.runId);
      }
    }
    return true;
  }

  /**
   * Drains queued signals for a run.
   *
   * - `scope: 'pending'` (default) returns active-run follow-up signals — each
   *   becomes its own model turn via `signalDrainStep`.
   * - `scope: 'pre-run'` returns signals queued before the run's first model
   *   request — the first LLM step folds these into that request.
   */
  drainPendingSignals(runId: string, pubsub?: PubSub, scope: 'pending' | 'pre-run' = 'pending'): CreatedAgentSignal[] {
    const state = this.#getState(pubsub);
    const record = state.threadRunsById.get(runId);
    const key = record ? this.#threadKey(record.resourceId, record.threadId) : state.threadKeysByRunId.get(runId);
    if (!key) return [];

    const signalsByThread = scope === 'pre-run' ? state.preRunSignalsByThread : state.pendingSignalsByThread;
    const queue = signalsByThread.get(key);
    if (!queue || queue.length === 0) {
      return [];
    }

    signalsByThread.delete(key);
    return queue;
  }

  async waitForCrossAgentThreadRun(
    agent: Agent<any, any, any, any>,
    options: { memory?: AgentExecutionOptions<any>['memory']; requestContext?: RequestContext },
    pubsub?: PubSub,
    ownsReservation = false,
  ) {
    const { threadId, resourceId } = this.#getThreadTarget(options);
    if (!threadId) return;

    const state = this.#getState(pubsub);
    const key = this.#threadKey(resourceId, threadId);
    while (true) {
      const activeRunId = state.activeThreadRunIds.get(key);
      const activeRecord = activeRunId ? state.threadRunsById.get(activeRunId) : undefined;
      const reservedAgentId = activeRunId ? state.reservedAgentIdsByRunId.get(activeRunId) : undefined;
      if (
        activeRunId &&
        activeRunId === (options as { runId?: string }).runId &&
        ownsReservation &&
        ((activeRecord && activeRecord.agent.id === agent.id) || (!activeRecord && reservedAgentId === agent.id))
      ) {
        return;
      }
      if (!activeRunId) return;
      if (activeRecord) {
        if (activeRecord.agent.id === agent.id || !this.#isThreadBlockingRun(state, activeRecord)) {
          return;
        }
        await activeRecord.output._waitUntilFinished().catch(() => {});
        if (
          state.activeThreadRunIds.get(key) === activeRunId &&
          state.threadRunsById.get(activeRunId) === activeRecord
        ) {
          await new Promise<void>(resolve => {
            const waiters = state.reservationWaitersByRunId.get(activeRunId) ?? [];
            waiters.push(resolve);
            state.reservationWaitersByRunId.set(activeRunId, waiters);
          });
        } else {
          await new Promise<void>(resolve => setTimeout(resolve, 0));
        }
        continue;
      }
      if (state.threadKeysByRunId.get(activeRunId) === key) {
        await new Promise<void>(resolve => {
          const waiters = state.reservationWaitersByRunId.get(activeRunId) ?? [];
          waiters.push(resolve);
          state.reservationWaitersByRunId.set(activeRunId, waiters);
        });
        continue;
      }
      await this.#waitForRemoteRunToFinish(pubsub, key, activeRunId);
    }
  }

  /**
   * Wait until a failed thread reservation attempt can be retried.
   *
   * `waitForCrossAgentThreadRun()` deliberately returns immediately for a
   * same-agent run, because that run is not a cross-agent execution blocker.
   * A caller that still needs an exclusive reservation must nevertheless wait
   * for the current reservation to be released. Retrying in a resolved-promise
   * loop can starve the terminal publish that performs that release, so park on
   * the reservation lifecycle instead.
   */
  async waitForThreadRunReservation(
    options: {
      runId?: string;
      memory?: AgentExecutionOptions<any>['memory'];
      requestContext?: RequestContext;
      _threadRunReservationOwner?: boolean;
    },
    pubsub?: PubSub,
    agentId?: string,
  ) {
    const { threadId, resourceId } = this.#getThreadTarget(options);
    if (!threadId) return;

    const state = this.#getState(pubsub);
    const key = this.#threadKey(resourceId, threadId);
    while (true) {
      const activeRunId = state.activeThreadRunIds.get(key);
      if (!activeRunId) return;

      const activeRecord = state.threadRunsById.get(activeRunId);
      const requestedRunId = options.runId;
      const canRotateSuspendedOwner =
        requestedRunId !== undefined &&
        requestedRunId !== activeRunId &&
        agentId !== undefined &&
        activeRecord?.agent.id === agentId &&
        this.#isSuspendedRun(state, activeRunId) &&
        (activeRecord.lifecycle === 'suspending' ||
          activeRecord.lifecycle === 'suspended' ||
          activeRecord.output.status === 'suspended');

      if (canRotateSuspendedOwner) {
        // A suspended run remains addressable by run id for explicit resume,
        // approval, and subscribers, but it must not deadlock a fresh direct
        // Agent.stream() turn on the same agent/thread. Rotate only the active
        // thread owner; retain the old run record and thread-key binding.
        // Signal/idle admission still sees the suspended run as active until a
        // direct caller explicitly reaches this reservation path.
        const heldLease = state.leaseRenewalTimers.has(activeRunId);
        if (heldLease) {
          const transferred = await this.#transferThreadLease(pubsub, key, activeRunId, requestedRunId);
          if (!transferred) {
            await new Promise<void>(resolve => setTimeout(resolve, 0));
            continue;
          }
        }

        // The lease transfer is asynchronous. Re-check ownership before
        // committing the local handoff so a concurrent resume/new run cannot
        // be overwritten by this waiter.
        if (
          state.activeThreadRunIds.get(key) !== activeRunId ||
          state.threadRunsById.get(activeRunId) !== activeRecord ||
          !this.#isSuspendedRun(state, activeRunId)
        ) {
          if (heldLease) this.#releaseThreadLease(pubsub, key, requestedRunId);
          continue;
        }

        state.activeThreadRunIds.set(key, requestedRunId);
        if (state.activeThreadStreamIds.get(key) === activeRecord.streamId) {
          state.activeThreadStreamIds.delete(key);
        }
        state.threadKeysByRunId.set(requestedRunId, key);
        state.reservedAgentIdsByRunId.set(requestedRunId, agentId);
        options._threadRunReservationOwner = true;
        this.#resolveReservationWaiters(state, activeRunId);
        return;
      }

      const isLocalRun = state.threadRunsById.has(activeRunId) || state.threadKeysByRunId.get(activeRunId) === key;
      if (!isLocalRun) {
        await this.#waitForRemoteRunToFinish(pubsub, key, activeRunId);
        continue;
      }

      await new Promise<void>(resolve => {
        // Avoid losing a release between reading activeRunId above and
        // installing the waiter.
        if (state.activeThreadRunIds.get(key) !== activeRunId) {
          resolve();
          return;
        }
        const waiters = state.reservationWaitersByRunId.get(activeRunId) ?? [];
        waiters.push(resolve);
        state.reservationWaitersByRunId.set(activeRunId, waiters);
      });
    }
  }

  async #waitForRemoteRunToFinish(pubsub: PubSub | undefined, key: string, runId: string) {
    const resolvedPubSub = this.#getPubSub(pubsub);
    const topic = this.#threadTopic(key);
    await new Promise<void>(resolve => {
      const onEvent: EventCallback = event => {
        const data = event.data as AgentThreadStreamRuntimeEvent | undefined;
        if (
          (data?.type === 'run-completed' || data?.type === 'run-aborted' || data?.type === 'run-failed') &&
          data.runId === runId
        ) {
          void resolvedPubSub.unsubscribe(topic, onEvent).catch(() => {});
          resolve();
        }
      };
      void resolvedPubSub.subscribe(topic, onEvent).catch(() => resolve());
    });
  }

  async subscribeToThread<OUTPUT = unknown>(
    agent: Agent<any, any, any, any>,
    options: AgentSubscribeToThreadOptions,
    pubsub?: PubSub,
  ): Promise<AgentThreadSubscription<OUTPUT>> {
    void agent;
    const resolvedPubSub = this.#getPubSub(pubsub);
    const state = this.#getState(resolvedPubSub);
    const key = this.#threadKey(options.resourceId, options.threadId);
    const topic = this.#threadTopic(key);
    const seenStreamIds = new Set<string>();
    const pendingRuns: AgentThreadRunRecord<any>[] = [];
    const waiters: Array<() => void> = [];
    const drainedOutputs = new WeakSet<object>();
    const enqueuedOutputs = new WeakSet<object>();
    const streamDrainedOutputs = new WeakSet<object>();
    const terminalOutputs = new WeakSet<object>();
    const outputsByRunId = new Map<string, MastraModelOutput<unknown>[]>();
    const outputDrainWaiters = new Map<object, Set<{ resolve: () => void; reject: (error: Error) => void }>>();
    const streamDrainWaiters = new Map<object, Set<{ resolve: () => void; reject: (error: Error) => void }>>();
    const remoteRuns = new Map<
      string,
      {
        parts: unknown[];
        waiters: Array<() => void>;
        finishWaiters: Array<() => void>;
        done: boolean;
        stream: ReadableStream<unknown>;
        closed: boolean;
      }
    >();
    let done = false;

    const wake = () => {
      while (waiters.length) waiters.shift()?.();
    };

    const settleOutputDrainIfReady = (output: MastraModelOutput<unknown>) => {
      if (!streamDrainedOutputs.has(output) || !terminalOutputs.has(output)) return;
      drainedOutputs.add(output);
      const pending = outputDrainWaiters.get(output);
      outputDrainWaiters.delete(output);
      for (const waiter of pending ?? []) waiter.resolve();
    };

    const markOutputStreamDrained = (output: MastraModelOutput<unknown>) => {
      streamDrainedOutputs.add(output);
      const pending = streamDrainWaiters.get(output);
      streamDrainWaiters.delete(output);
      for (const waiter of pending ?? []) waiter.resolve();
      settleOutputDrainIfReady(output);
    };

    const removeTrackedOutput = (runId: string, output: MastraModelOutput<unknown>) => {
      const outputs = outputsByRunId.get(runId);
      if (!outputs) return;
      const index = outputs.indexOf(output);
      if (index === -1) return;
      outputs.splice(index, 1);
      if (outputs.length === 0) {
        // Admission is guarded per streamId (a later approval-resume/retry
        // segment mints a fresh streamId), so dropping the correlation queue is
        // all that is needed to admit a legitimate later segment.
        outputsByRunId.delete(runId);
      }
    };

    const markRunTerminalDelivered = (runId: string) => {
      const outputs = outputsByRunId.get(runId);
      const output = outputs?.shift();
      if (outputs?.length === 0) outputsByRunId.delete(runId);
      if (!output) return;
      terminalOutputs.add(output);
      settleOutputDrainIfReady(output);
    };

    const rejectOutputDrain = (output: MastraModelOutput<unknown>, error: Error) => {
      const pending = outputDrainWaiters.get(output);
      outputDrainWaiters.delete(output);
      for (const waiter of pending ?? []) waiter.reject(error);
    };

    const rejectStreamDrain = (output: MastraModelOutput<unknown>, error: Error) => {
      const pending = streamDrainWaiters.get(output);
      streamDrainWaiters.delete(output);
      for (const waiter of pending ?? []) waiter.reject(error);
    };

    const waitForStreamDrain = (output: MastraModelOutput<unknown>): Promise<void> => {
      if (streamDrainedOutputs.has(output)) return Promise.resolve();
      if (done) {
        return Promise.reject(
          new AgentThreadOutputDrainError(
            'subscription-closed',
            'Thread subscription closed before output stream drain completed',
          ),
        );
      }
      const pending = streamDrainWaiters.get(output) ?? new Set();
      let waiter!: { resolve: () => void; reject: (error: Error) => void };
      const drained = new Promise<void>((resolve, reject) => {
        waiter = { resolve, reject };
      });
      pending.add(waiter);
      streamDrainWaiters.set(output, pending);
      return drained.finally(() => {
        pending.delete(waiter);
        if (pending.size === 0) streamDrainWaiters.delete(output);
      });
    };

    const rejectAfterEnqueuedStreamDrain = async (
      output: MastraModelOutput<unknown>,
      error: unknown,
    ): Promise<never> => {
      // A PubSub can invoke this subscription and then reject later in the same
      // publish. Once enqueued, the segment is observable regardless of the
      // publication promise's outcome. Do not let its failure reach Session
      // until every buffered part has crossed the subscription generator; turn
      // teardown can then close all tools that actually started, with no later
      // tool_start appearing after the failure terminal.
      if (enqueuedOutputs.has(output) && !streamDrainedOutputs.has(output)) {
        try {
          await waitForStreamDrain(output);
        } catch {
          // Explicit subscription teardown stops the generator and is therefore
          // also a safe boundary after which this segment cannot emit more parts.
        }
      }
      removeTrackedOutput(output.runId, output);
      throw error;
    };

    const waitForOutputDrain = (output: MastraModelOutput<unknown>): Promise<void> | undefined => {
      const registration = this.#threadOutputRegistrations.get(output);
      const terminal = this.#threadOutputTerminals.get(output);
      // Duck-typed Agent overrides may return an output without registering it
      // with the thread runtime. There is no subscription segment to drain in
      // that case, so preserve their existing direct completion contract.
      if (!registration) return undefined;
      if (drainedOutputs.has(output)) return Promise.resolve();
      if (done) {
        return Promise.reject(
          new AgentThreadOutputDrainError(
            'subscription-closed',
            'Thread subscription closed before output drain completed',
          ),
        );
      }
      const pending = outputDrainWaiters.get(output) ?? new Set();
      let waiter!: { resolve: () => void; reject: (error: Error) => void };
      const drained = new Promise<void>((resolve, reject) => {
        waiter = { resolve, reject };
      });
      pending.add(waiter);
      outputDrainWaiters.set(output, pending);
      return registration
        .then(
          async () => {
            if (terminal) {
              try {
                await terminal;
              } catch (error) {
                await rejectAfterEnqueuedStreamDrain(output, error);
              }
            }
            try {
              await waitWithTimeout(
                drained,
                TERMINAL_DELIVERY_TIMEOUT_MS,
                () =>
                  new AgentThreadOutputDrainError(
                    'terminal-delivery-timeout',
                    `Thread subscription did not observe terminal delivery for agent run ${output.runId}`,
                  ),
              );
            } catch (error) {
              await rejectAfterEnqueuedStreamDrain(output, error);
            }
          },
          error => rejectAfterEnqueuedStreamDrain(output, error),
        )
        .catch(error => {
          pending.delete(waiter);
          if (pending.size === 0) outputDrainWaiters.delete(output);
          throw error;
        });
    };

    const activeRunId = () => {
      const runId = state.activeThreadRunIds.get(key);
      if (!runId) return null;
      const record = state.threadRunsById.get(runId);
      // No record yet means either a remote run (record never lives locally) or a local run
      // that sendSignal has reserved but has not yet registered via registerRun. Both are
      // in flight from the subscriber's perspective; treat them as active.
      if (!record) return runId;
      return this.#isThreadBlockingRun(state, record) ? runId : null;
    };

    const enqueueRun = (record: AgentThreadRunRecord<any>) => {
      if (done || seenStreamIds.has(record.streamId)) return;
      seenStreamIds.add(record.streamId);
      enqueuedOutputs.add(record.output);
      const outputs = outputsByRunId.get(record.runId) ?? [];
      outputs.push(record.output);
      outputsByRunId.set(record.runId, outputs);
      // The per-run correlation queue exists even when no caller uses the
      // internal output-drain waiter. A rejected terminal has no delivery event
      // that could shift this output, so observe the runtime's terminal promise
      // and remove this exact object on failure.
      queueMicrotask(() => {
        const terminal = this.#threadOutputTerminals.get(record.output);
        void terminal?.catch(() => removeTrackedOutput(record.runId, record.output));
      });
      pendingRuns.push(record);
      wake();
    };

    const createRemoteRun = (runId: string, streamId: string, streamSeq: number): AgentThreadRunRecord<any> => {
      const remoteRun = {
        parts: [] as unknown[],
        waiters: [] as Array<() => void>,
        finishWaiters: [] as Array<() => void>,
        done: false,
        stream: undefined as unknown as ReadableStream<unknown>,
        closed: false,
      };
      remoteRun.stream = new ReadableStream({
        pull(controller) {
          const drain = () => {
            if (remoteRun.closed) return;
            while (remoteRun.parts.length > 0) {
              controller.enqueue(remoteRun.parts.shift());
            }
            if (remoteRun.done) {
              remoteRun.closed = true;
              controller.close();
            }
          };
          drain();
          if (!remoteRun.done && !remoteRun.closed) {
            remoteRun.waiters.push(drain);
          }
        },
        cancel() {
          remoteRun.done = true;
          remoteRun.closed = true;
          remoteRun.waiters.length = 0;
          while (remoteRun.finishWaiters.length) remoteRun.finishWaiters.shift()?.();
        },
      });
      remoteRuns.set(streamId, remoteRun);
      return {
        agent,
        output: {
          runId,
          status: 'running',
          fullStream: remoteRun.stream,
          _waitUntilFinished: async () => {
            if (remoteRun.done) return;
            await new Promise<void>(resolve => remoteRun.finishWaiters.push(resolve));
          },
        } as MastraModelOutput<any>,
        runId,
        streamId,
        streamSeq,
        lifecycle: 'running',
        threadId: options.threadId,
        resourceId: options.resourceId,
        streamOptions: {},
      };
    };

    const onEvent: EventCallback = event => {
      const data = event.data as AgentThreadStreamRuntimeEvent | undefined;
      if (!data) return;
      if (data.type === 'run-registered') {
        state.activeThreadRunIds.set(key, data.runId);
        state.activeThreadStreamIds.set(key, data.streamId);
        const record =
          state.threadRunsByStreamId.get(data.streamId) ?? createRemoteRun(data.runId, data.streamId, data.streamSeq);
        enqueueRun(record);
        wake();
        return;
      }
      if (data.type === 'stream-part') {
        if (data.sourceId === this.#id) return;
        let remoteRun = remoteRuns.get(data.streamId);
        if (!remoteRun) {
          // A subscriber can attach after another runtime already broadcast run-registered.
          // Treat the first stream-part on this thread topic as proof of the remote run and
          // create the local proxy stream from that point forward.
          state.activeThreadRunIds.set(key, data.runId);
          state.activeThreadStreamIds.set(key, data.streamId);
          enqueueRun(createRemoteRun(data.runId, data.streamId, state.streamSeqByRunId.get(data.runId) ?? 1));
          remoteRun = remoteRuns.get(data.streamId);
          if (!remoteRun) return;
        }
        remoteRun.parts.push(data.part);
        while (remoteRun.waiters.length) remoteRun.waiters.shift()?.();
        return;
      }
      if (data.type === 'signal-enqueued') {
        if (data.sourceId === this.#id) return;
        const signalsByThread = data.preRun ? state.preRunSignalsByThread : state.pendingSignalsByThread;
        const queue = signalsByThread.get(key) ?? [];
        queue.push(createSignal(data.signal));
        signalsByThread.set(key, queue);
        return;
      }
      if (data.type === 'run-failed') {
        const eventStreamId = data.streamId ?? data.runId;
        if (
          state.activeThreadRunIds.get(key) === data.runId &&
          (!data.streamId || state.activeThreadStreamIds.get(key) === data.streamId)
        ) {
          state.activeThreadRunIds.delete(key);
          state.activeThreadStreamIds.delete(key);
        }
        let errorRun: AgentThreadRunRecord<any> | undefined;
        let remoteRun = remoteRuns.get(eventStreamId);
        if (!remoteRun) {
          errorRun = createRemoteRun(data.runId, eventStreamId, state.streamSeqByRunId.get(data.runId) ?? 1);
          remoteRun = remoteRuns.get(eventStreamId);
        }
        if (remoteRun) {
          remoteRun.parts.push({ type: 'error', payload: { error: new Error(data.error) } });
          remoteRun.done = true;
          while (remoteRun.waiters.length) remoteRun.waiters.shift()?.();
          while (remoteRun.finishWaiters.length) remoteRun.finishWaiters.shift()?.();
          remoteRuns.delete(eventStreamId);
          seenStreamIds.delete(eventStreamId);
        }
        if (errorRun) enqueueRun(errorRun);
        void this.#drainPendingIdleSignals(state, resolvedPubSub, key);
        wake();
        return;
      }
      if (data.type === 'run-completed' || data.type === 'run-aborted' || data.type === 'run-suspended') {
        markRunTerminalDelivered(data.runId);
        const eventStreamId = data.streamId ?? data.runId;
        if (data.type === 'run-suspended') {
          state.suspendedRunIds.add(data.runId);
          const record = state.threadRunsByStreamId.get(eventStreamId) ?? state.threadRunsById.get(data.runId);
          if (record) record.lifecycle = 'suspended';
        } else if (
          state.activeThreadRunIds.get(key) === data.runId &&
          (!data.streamId || state.activeThreadStreamIds.get(key) === data.streamId)
        ) {
          state.activeThreadRunIds.delete(key);
          state.activeThreadStreamIds.delete(key);
        }
        if (data.type === 'run-aborted') {
          state.pendingSignalsByThread.delete(key);
        }
        if (data.type !== 'run-suspended') {
          this.#clearSuspendedRun(state, data.runId);
        }
        const remoteRun = remoteRuns.get(eventStreamId);
        if (remoteRun) {
          remoteRun.done = true;
          while (remoteRun.waiters.length) remoteRun.waiters.shift()?.();
          while (remoteRun.finishWaiters.length) remoteRun.finishWaiters.shift()?.();
          remoteRuns.delete(eventStreamId);
          seenStreamIds.delete(eventStreamId);
        }
        // When a run is aborted, cancel the current subscriber stream reader so
        // the generator's inner loop unblocks and can yield the synthetic abort.
        if (data.type === 'run-aborted' && activeReaderRunId === data.runId && currentReader) {
          cancelledByAbort = true;
          try {
            void currentReader.cancel();
          } catch {}
        }
        if (data.type !== 'run-suspended') {
          void this.#drainPendingIdleSignals(state, resolvedPubSub, key);
        }
        wake();
      }
    };

    await resolvedPubSub.subscribe(topic, onEvent);

    const currentRunId = activeRunId();
    const currentRecord = currentRunId ? state.threadRunsById.get(currentRunId) : undefined;
    if (currentRecord) {
      enqueueRun(currentRecord);
    }

    // Mutable ref to the subscriber stream reader currently being consumed by
    // the generator. When a run-aborted event fires, we cancel this reader so
    // the blocked `reader.read()` resolves immediately with {done: true}.
    let currentReader: ReadableStreamDefaultReader<any> | null = null;
    let activeReaderRunId: string | null = null;
    // Set to true when the reader is cancelled explicitly due to a run-aborted
    // event, so the generator can yield a synthetic abort chunk.
    let cancelledByAbort = false;

    const unsubscribe = () => {
      if (done) return;
      done = true;
      void resolvedPubSub.unsubscribe(topic, onEvent).catch(() => {});
      // Cancel current reader so the generator's inner loop breaks.
      if (currentReader) {
        try {
          void currentReader.cancel();
        } catch {}
      }
      const error = new AgentThreadOutputDrainError(
        'subscription-closed',
        'Thread subscription closed before output drain completed',
      );
      for (const [output] of outputDrainWaiters) rejectOutputDrain(output as MastraModelOutput<unknown>, error);
      for (const [output] of streamDrainWaiters) rejectStreamDrain(output as MastraModelOutput<unknown>, error);
      wake();
    };

    return {
      activeRunId,
      abort: () => this.abortThread(options, resolvedPubSub),
      unsubscribe,
      _waitForOutputDrain: waitForOutputDrain,
      stream: (async function* () {
        try {
          while (!done || pendingRuns.length > 0) {
            if (pendingRuns.length === 0) {
              await new Promise<void>(resolve => waiters.push(resolve));
              continue;
            }
            const run = pendingRuns.shift()!;
            // Local registered runs expose a multicast `createSubscriberStream`
            // giving this subscriber an independent fan-out view; remote runs are
            // already per-subscription streams fed by pubsub `stream-part` events.
            // Reading `output.fullStream` directly would let one subscriber lock
            // and drain the shared stream, starving every other subscriber.
            const subscriberStream = run.createSubscriberStream?.() ?? run.output.fullStream;
            const reader = subscriberStream.getReader();
            currentReader = reader as ReadableStreamDefaultReader<any>;
            activeReaderRunId = run.runId;
            let readerReleased = false;
            let fullyDrained = false;
            try {
              while (true) {
                const { value: part, done: streamDone } = await reader.read();
                if (streamDone) {
                  // A natural close (including a reader cancelled by a
                  // run-aborted event) means every visible part crossed this
                  // generator; only an explicit subscription teardown leaves
                  // the segment un-drained.
                  fullyDrained = !done;
                  break;
                }
                const typedPart = part as any;
                const partWithRunId =
                  typedPart && typeof typedPart === 'object' && !('runId' in typedPart)
                    ? { ...typedPart, runId: run.runId }
                    : typedPart;
                yield partWithRunId;
                if (done) break;
                const finishReason = typedPart.finishReason ?? typedPart.payload?.finishReason;
                const terminalBoundary =
                  typedPart.type === 'error' ||
                  typedPart.type === 'abort' ||
                  (typedPart.type === 'finish' && finishReason !== 'tool-calls');
                if (terminalBoundary) {
                  // After a final terminal chunk, drain any non-visible trailing
                  // data in the background to prevent upstream backpressure while
                  // allowing the generator to immediately serve subsequent runs.
                  readerReleased = true;
                  void (async () => {
                    try {
                      while (true) {
                        const { done: d } = await reader.read();
                        if (d) break;
                      }
                    } catch {}
                    reader.releaseLock();
                  })();
                  // Every model-visible part has crossed the generator; the
                  // background read above only discards non-visible trailers.
                  fullyDrained = true;
                  break;
                }
              }
              // If the stream closed because we cancelled the reader after a
              // run-aborted event, yield a synthetic abort so subscribers
              // finalize the run.
              if (!readerReleased && !done && cancelledByAbort) {
                yield { type: 'abort', runId: run.runId } as any;
                cancelledByAbort = false;
              }
            } finally {
              currentReader = null;
              activeReaderRunId = null;
              if (!readerReleased) {
                reader.releaseLock();
              }
              if (fullyDrained) {
                markOutputStreamDrained(run.output);
              } else {
                rejectOutputDrain(
                  run.output,
                  new AgentThreadOutputDrainError(
                    'stream-stopped',
                    'Thread subscription stopped before output drain completed',
                  ),
                );
              }
            }
          }
        } finally {
          unsubscribe();
        }
      })(),
    };
  }

  sendMessage<OUTPUT = unknown>(
    agent: Agent<any, any, any, any>,
    message: AgentMessageInput,
    target: SendAgentMessageOptions<OUTPUT>,
    pubsub?: PubSub,
  ): SendAgentMessageResult<OUTPUT> {
    return this.sendSignal<OUTPUT>(agent, createMessageSignal(message, { acceptedAt: new Date() }), target, pubsub);
  }

  queueMessage<OUTPUT = unknown>(
    agent: Agent<any, any, any, any>,
    message: AgentMessageInput,
    target: QueueAgentMessageOptions<OUTPUT>,
    pubsub?: PubSub,
  ): QueueAgentMessageResult<OUTPUT> {
    const state = this.#getState(pubsub);
    const signal = createMessageSignal(message, { acceptedAt: new Date() });
    let key: string | undefined;
    let runId = target.runId;
    let activeRecord: AgentThreadRunRecord<any> | undefined;

    if (target.resourceId && target.threadId) {
      key = this.#threadKey(target.resourceId, target.threadId);
      const activeRunId = state.activeThreadRunIds.get(key);
      activeRecord = activeRunId ? state.threadRunsById.get(activeRunId) : undefined;
      if (activeRecord && !this.#isThreadBlockingRun(state, activeRecord)) {
        state.activeThreadRunIds.delete(key);
        activeRecord = undefined;
      }
      runId ??= activeRunId;
    }

    if (runId) {
      activeRecord ??= state.threadRunsById.get(runId);
      if (activeRecord) {
        key ??= this.#threadKey(activeRecord.resourceId, activeRecord.threadId);
      }
    }

    const resourceId = target.resourceId ?? activeRecord?.resourceId;
    const threadId = target.threadId ?? activeRecord?.threadId;
    if (!resourceId || !threadId) {
      throw new Error('resourceId and threadId are required to queue a message');
    }

    key ??= this.#threadKey(resourceId, threadId);
    const queuedRunId = randomUUID();
    const queuedStreamOptions = target.ifIdle?.streamOptions ?? activeRecord?.streamOptions;

    if (activeRecord) {
      const idleQueue = state.pendingIdleSignalsByThread.get(key) ?? [];
      idleQueue.push({ agent, signal, runId: queuedRunId, resourceId, threadId, streamOptions: queuedStreamOptions });
      state.pendingIdleSignalsByThread.set(key, idleQueue);
      void this.#watchThreadRunCompletion(state, pubsub, key, activeRecord);
      return {
        signal,
        runId: queuedRunId,
        accepted: Promise.resolve({ action: 'deliver' as const, runId: queuedRunId }),
      };
    }

    return this.sendSignal<OUTPUT>(
      agent,
      signal,
      { ...target, runId, resourceId, threadId, ifIdle: { ...target.ifIdle, behavior: 'wake' } },
      pubsub,
    );
  }

  async sendStateSignal<OUTPUT = unknown>(
    agent: Agent<any, any, any, any>,
    stateInput: AgentStateSignalInput,
    target: SendAgentStateSignalOptions<OUTPUT>,
    pubsub?: PubSub,
  ): Promise<SendAgentStateSignalResult<OUTPUT>> {
    if (!target.resourceId || !target.threadId) {
      throw new Error('resourceId and threadId are required to send a state signal');
    }
    const resourceId = target.resourceId;
    const threadId = target.threadId;

    const requestContext = target.ifIdle?.streamOptions?.requestContext;
    const memoryContext = parseMemoryRequestContext(requestContext);
    const memory = await agent.getMemory({ requestContext });
    if (!memory) {
      throw new Error('sendStateSignal requires Mastra memory');
    }

    const loadedThread = (await memory.getThreadById({ threadId })) ?? memoryContext?.thread;
    if (!loadedThread) {
      throw new Error(`sendStateSignal could not load thread ${threadId}`);
    }

    const thread = {
      ...loadedThread,
      id: threadId,
      resourceId: loadedThread.resourceId ?? resourceId,
      createdAt: loadedThread.createdAt ?? new Date(),
      updatedAt: loadedThread.updatedAt ?? new Date(),
      metadata: loadedThread.metadata,
    };

    const applied = await applyStateSignal({
      input: stateInput,
      memory,
      thread,
      resourceId,
      threadId,
      memoryConfig: memoryContext?.memoryConfig,
      acceptedAt: new Date(),
    });

    if (applied.skipped) {
      return { skipped: true, reason: 'unchanged' };
    }

    return this.sendSignal<OUTPUT>(agent, applied.signal, target, pubsub);
  }

  /**
   * Routes a signal to an agent thread.
   *
   * Signals can land in three places:
   * - an active same-agent run, where they are queued for the execution loop to drain;
   * - a reserved thread run that has not registered its stream record yet;
   * - a new idle-started run, when idle behavior allows a wakeup.
   *
   * Cross-agent active runs are intentionally not interrupted here. They either finish first
   * through `waitForCrossAgentThreadRun()` on the stream path, or this method falls through to
   * the idle-start path when the caller provided a resource/thread target and idle behavior allows a wakeup.
   */
  sendSignal<OUTPUT = unknown>(
    agent: Agent<any, any, any, any>,
    signalInput: AgentSignal,
    target: SendAgentSignalOptions<OUTPUT>,
    pubsub?: PubSub,
  ): SendAgentSignalResult<OUTPUT> {
    const state = this.#getState(pubsub);
    let signal = createSignal({ ...signalInput, acceptedAt: new Date() });
    const callerSignalId = signalInput.id;
    let key: string | undefined;
    let runId = target.runId;
    const activeBehavior = target.ifActive?.behavior ?? 'deliver';
    const idleBehavior = target.ifIdle?.behavior ?? 'wake';

    let activeRecord: AgentThreadRunRecord<any> | undefined;
    let finishingLeaseOwnerRunId: string | undefined;
    if (target.threadId) {
      key = this.#threadKey(target.resourceId, target.threadId);
      let activeRunId = state.activeThreadRunIds.get(key);
      if (!activeRunId && !target.resourceId) {
        const activeThreadMatch = this.#findUniqueActiveThreadRunByThreadId(state, target.threadId);
        if (activeThreadMatch) {
          key = activeThreadMatch.key;
          activeRunId = activeThreadMatch.runId;
        }
      }
      activeRecord = activeRunId ? state.threadRunsById.get(activeRunId) : undefined;
      const activeRunAborted = activeRunId ? state.abortedRunIds.has(activeRunId) : false;
      const reservedAgentId = activeRunId ? state.reservedAgentIdsByRunId.get(activeRunId) : undefined;
      if (activeRunAborted) {
        activeRecord = undefined;
      } else if (activeRecord && !this.#isThreadBlockingRun(state, activeRecord)) {
        // A subscriber can observe the final stream part before the completion
        // watcher releases this local run's thread lease. Preserve that owner
        // for a gap-free handoff to an immediately following idle wake instead
        // of racing a fresh acquire against the finishing run (PF-558).
        if (state.threadKeysByRunId.get(activeRecord.runId) === key) {
          finishingLeaseOwnerRunId = activeRecord.runId;
        }
        state.activeThreadRunIds.delete(key);
        activeRunId = undefined;
        activeRecord = undefined;
      }

      // Prefer the active same-agent run for thread-targeted signals. This is the normal
      // follow-up path used by clients that know the thread/resource but not the run id.
      if (activeRecord && activeRecord.agent.id === agent.id) {
        runId = activeRecord.runId;
      } else if (activeRunId && !activeRecord && !activeRunAborted) {
        if (state.threadKeysByRunId.get(activeRunId) === key) {
          // A run can be reserved before its stream record is registered. Keep the reserved
          // id so early follow-ups still attach to the run that is starting — but only when
          // the reservation belongs to this agent or the caller explicitly opted in
          // (an unrelated agent's idle wake must not silently join a foreign reservation).
          if (
            !target.ifIdle ||
            reservedAgentId === agent.id ||
            Boolean((target.ifIdle as { _attachToReservedRun?: unknown })._attachToReservedRun)
          ) {
            runId = activeRunId;
          }
        } else {
          // Stale cross-pod entry. Clean it up from the local map, then let the lease decide
          state.activeThreadRunIds.delete(key);
          state.activeThreadStreamIds.delete(key);
        }
      }
    }

    if (target.runId && state.abortedRunIds.has(target.runId)) {
      throw new Error(`Agent thread run id "${target.runId}" has been aborted`);
    }
    if (runId && activeRecord?.runId !== runId) {
      activeRecord = state.threadRunsById.get(runId);
    }
    if (!key && activeRecord) {
      key = this.#threadKey(activeRecord.resourceId, activeRecord.threadId);
    }
    const isActiveTarget = Boolean(
      runId && (activeRecord?.output.status === 'running' || (key && state.activeThreadRunIds.get(key) === runId)),
    );
    const resourceId = target.resourceId ?? activeRecord?.resourceId;
    const threadId = target.threadId ?? activeRecord?.threadId;

    // Resolve the selected branch only after admission determined whether the
    // signal is targeting active work or taking the idle path.
    signal = resolveDeliveryAttributes(
      signal,
      isActiveTarget ? target.ifActive?.attributes : target.ifIdle?.attributes,
    );

    const scopedRunId = target.runId;
    const signalPayloadKey = callerSignalPayloadKey(signalInput);
    const callerSignalKey =
      callerSignalId !== undefined && signalPayloadKey !== undefined
        ? [agent.id, resourceId ?? '', threadId ?? '', scopedRunId ?? '', callerSignalId, signalPayloadKey].join(
            '\u0000',
          )
        : undefined;
    if (callerSignalKey) {
      const accepted = state.acceptedCallerSignals.get(callerSignalKey);
      if (accepted) return accepted as SendAgentSignalResult<OUTPUT>;
    }
    const acceptSignal = <T extends SendAgentSignalResult<OUTPUT>>(
      result: T,
      acceptedRunId: string | undefined,
      cache = true,
    ): T => {
      if (callerSignalKey && cache) {
        state.acceptedCallerSignals.set(callerSignalKey, result as SendAgentSignalResult);
        if (acceptedRunId) {
          const signalIds = state.callerSignalIdsByRunId.get(acceptedRunId) ?? new Set<string>();
          signalIds.add(callerSignalKey);
          state.callerSignalIdsByRunId.set(acceptedRunId, signalIds);
        }
      }
      return result;
    };

    if (isActiveTarget && activeBehavior !== 'deliver') {
      runId ??= randomUUID();
      if (activeBehavior === 'persist') {
        if (!resourceId || !threadId) {
          throw new Error('resourceId and threadId are required to persist an active signal');
        }
        const persisted = this.#persistSignal(
          agent,
          signal,
          resourceId,
          threadId,
          target.ifIdle?.streamOptions?.requestContext,
        );
        void persisted.catch(() => {});
        return acceptSignal(
          {
            signal,
            runId,
            persisted,
            accepted: Promise.resolve({ action: 'persist' as const }),
          },
          runId,
        );
      }
      return acceptSignal(
        {
          signal,
          runId,
          accepted: Promise.resolve({ action: 'discard' as const }),
        },
        runId,
      );
    }

    if (runId) {
      activeRecord ??= state.threadRunsById.get(runId);
      // A run is "blocking" while it is running or suspended awaiting tool approval. Both
      // states mean the run has already made model requests, so a follow-up signal must be
      // queued as a pending (next-turn) signal rather than folded into a not-yet-started
      // first request via the pre-run path below.
      if (activeRecord && this.#isThreadBlockingRun(state, activeRecord)) {
        key ??= this.#threadKey(activeRecord.resourceId, activeRecord.threadId);
        if (activeRecord.agent.id === agent.id) {
          // Same-agent active run: queue the signal for in-loop draining so it becomes
          // the next model input instead of waiting for the run to finish.
          const queue = state.pendingSignalsByThread.get(key) ?? [];
          queue.push(signal);
          state.pendingSignalsByThread.set(key, queue);
          this.#publish(pubsub, key, {
            type: 'signal-enqueued',
            runId,
            signal: this.#serializeSignal(signal),
            sourceId: this.#id,
          });
          void this.#watchThreadRunCompletion(state, pubsub, key, activeRecord);
          return acceptSignal(
            {
              signal,
              runId,
              accepted: Promise.resolve({ action: 'deliver' as const, runId }),
            },
            runId,
          );
        }

        return {
          signal,
          runId: activeRecord.runId,
          accepted: Promise.resolve({
            action: 'blocked' as const,
            reason: 'thread-blocked' as const,
            runId: activeRecord.runId,
          }),
        };
      }

      if (key && state.activeThreadRunIds.get(key) === runId) {
        // A local reserved run has not registered its stream record yet, so it
        // has not made its first model request — queue the signal as a pre-run
        // signal so the first LLM step folds it into that request. A run owned
        // by another runtime instance is reached only via PubSub; treat it as a
        // follow-up, since the sender cannot see the owner's request state.
        const isLocalReservedRun = state.threadKeysByRunId.get(runId) === key;
        if (isLocalReservedRun) {
          const queue = state.preRunSignalsByThread.get(key) ?? [];
          queue.push(signal);
          state.preRunSignalsByThread.set(key, queue);
        }
        this.#publish(pubsub, key, {
          type: 'signal-enqueued',
          runId,
          signal: this.#serializeSignal(signal),
          sourceId: this.#getSourceId(),
          preRun: isLocalReservedRun,
        });
        return acceptSignal(
          {
            signal,
            runId,
            accepted: Promise.resolve({ action: 'deliver' as const, runId }),
          },
          runId,
        );
      }
    }

    if (!resourceId || !threadId) {
      throw new Error('No active agent run found for signal target');
    }

    runId ??= randomUUID();
    key ??= this.#threadKey(resourceId, threadId);
    if (idleBehavior === 'persist') {
      // Persist the signal AND broadcast it to thread subscribers.
      // #persistAndBroadcastIdleSignal persists first, then emits a synthetic
      // start/data/finish run through the (multicast) broadcast machinery
      // without waking the agent.
      const persisted = this.#persistAndBroadcastIdleSignal(
        state,
        pubsub,
        key,
        runId,
        agent,
        signal,
        resourceId,
        threadId,
        target.ifIdle?.streamOptions?.requestContext,
      );
      void persisted.catch(() => {});
      return acceptSignal(
        {
          signal,
          runId,
          persisted,
          accepted: Promise.resolve({ action: 'persist' as const }),
        },
        runId,
        false,
      );
    }
    if (idleBehavior !== 'wake') {
      return acceptSignal(
        {
          signal,
          runId,
          accepted: Promise.resolve({ action: 'discard' as const }),
        },
        runId,
        false,
      );
    }

    key ??= this.#threadKey(resourceId, threadId);
    const onRunRejected = getIdleRunRejectedHandler(target.ifIdle);
    const reserveBeforeIdleWake = !Boolean(
      (target.ifIdle as { _skipThreadRunReservationBeforePreflight?: unknown } | undefined)
        ?._skipThreadRunReservationBeforePreflight,
    );
    const existingRunKey =
      state.threadKeysByRunId.get(runId) ??
      state.pendingIdleThreadKeysByRunId.get(runId) ??
      state.inflightIdleThreadKeysByRunId.get(runId);
    if (existingRunKey) {
      throw new Error(
        existingRunKey === key
          ? `Agent thread run id "${runId}" is already reserved`
          : `Agent thread run id "${runId}" is already reserved for another thread`,
      );
    }
    if (state.activeThreadRunIds.has(key)) {
      const blockingRunId = state.activeThreadRunIds.get(key)!;
      const blockingRecord = activeRecord ?? state.threadRunsById.get(blockingRunId);
      if (
        this.#isSuspendedRun(state, blockingRunId) ||
        blockingRecord?.output.status === 'suspended' ||
        blockingRecord?.lifecycle === 'suspended'
      ) {
        return {
          signal,
          runId: blockingRunId,
          accepted: Promise.resolve({
            action: 'blocked' as const,
            reason: 'thread-blocked' as const,
            runId: blockingRunId,
          }),
        };
      }

      // Another run owns the thread. Queue this idle-start request and let the watcher
      // launch it only after the active run clears the thread reservation.
      const idleQueue = state.pendingIdleSignalsByThread.get(key) ?? [];
      idleQueue.push({
        agent,
        signal,
        runId,
        resourceId,
        threadId,
        streamOptions: target.ifIdle?.streamOptions,
        onRunRejected,
        reserveBeforePreflight: reserveBeforeIdleWake,
      });
      state.pendingIdleSignalsByThread.set(key, idleQueue);
      state.pendingIdleThreadKeysByRunId.set(runId, key);
      if (activeRecord) {
        void this.#watchThreadRunCompletion(state, pubsub, key, activeRecord);
      }
      return acceptSignal(
        {
          signal,
          runId,
          accepted: Promise.resolve({ action: 'deliver' as const, runId }),
        },
        runId,
      );
    }

    // No active same-agent run accepted the signal. Reserve early when the runtime owns
    // admission; deferred starts let Agent.stream() claim the run under its own preflight rules.
    if (reserveBeforeIdleWake) {
      state.activeThreadRunIds.set(key, runId);
      state.threadKeysByRunId.set(runId, key);
      state.reservedAgentIdsByRunId.set(runId, agent.id);
    } else {
      state.inflightIdleThreadKeysByRunId.set(runId, key);
      state.inflightIdleAgentIdsByRunId.set(runId, agent.id);
    }
    const reservedKey = key;
    const reservedRunId = runId;
    const resolvedPubSub = this.#getPubSub(pubsub);
    const leaseProvider = this.#getLeaseProvider(resolvedPubSub);
    const rollbackLocalReservation = (rejectRun: boolean) => {
      onRunRejected?.();
      if (reserveBeforeIdleWake) {
        this.#releaseReservedRun(state, pubsub, reservedKey, reservedRunId, {
          cleanupPrepared: true,
          clearAbort: true,
          rejectOutputWaiters: true,
        });
      } else {
        state.inflightIdleThreadKeysByRunId.delete(reservedRunId);
        state.inflightIdleAgentIdsByRunId.delete(reservedRunId);
        if (rejectRun) this.rejectUnregisteredRun(reservedRunId, pubsub);
      }
    };
    // First acquire the cross-process lease via pubsub; on win, kick off the stream and
    // resolve a `wake` accepted result carrying the owned stream. On loss, hand the user
    // signal off to the winning process via signal-enqueued and resolve a `deliver` result
    // (the signal was queued onto the winning run, not run locally).
    const accepted: Promise<SendAgentSignalAccepted<OUTPUT>> = (async () => {
      // Fail-open on pubsub errors: if the lease backend is unreachable we treat the
      // call as "acquired" so the caller still gets a response. The tradeoff is that
      // if multiple processes hit the same pubsub failure simultaneously they can each
      // start a stream for the same thread (the bug this lease is supposed to prevent),
      // but failing closed would silently drop user messages on any Redis blip which
      // is the worse failure mode. Lease TTL + renewal still bound the duplicate
      // window to a single run, and the next clean acquireLease re-serializes callers.
      const lease = finishingLeaseOwnerRunId
        ? await this.#acquireOrTransferThreadLease(resolvedPubSub, reservedKey, reservedRunId, finishingLeaseOwnerRunId)
        : await leaseProvider
            .acquireLease(reservedKey, reservedRunId, AGENT_THREAD_LEASE_TTL_MS)
            .catch(() => ({ acquired: true as boolean, owner: reservedRunId as string | undefined }));

      if (!lease.acquired) {
        // Lost the wake race to another process. Roll back our optimistic local reservation
        // so we don't trip our own activeThreadRunIds check on a follow-up.
        rollbackLocalReservation(false);

        // Forward the user signal to the winning runId so the message is not dropped.
        // Await the publish so that callers using `accepted` resolution as their
        // "safe to exit" boundary (e.g. a serverless Lambda holding the request open
        // via waitUntil) don't tear down before the enqueue lands on the broker.
        const winnerRunId = lease.owner;
        if (winnerRunId) {
          await this.#publishAndWait(pubsub, reservedKey, {
            type: 'signal-enqueued',
            runId: winnerRunId,
            signal: this.#serializeSignal(signal),
            sourceId: this.#getSourceId(),
          }).catch(() => {});
        }
        return { action: 'deliver' as const, runId: winnerRunId ?? reservedRunId };
      }

      // We own the lease. Start the renewal timer so it survives runs
      // that outlive the TTL, then kick off the stream.
      this.#startLeaseRenewal(resolvedPubSub, reservedKey, reservedRunId);
      try {
        const output = await agent.stream(signal, {
          ...(target.ifIdle?.streamOptions as any),
          ...(reserveBeforeIdleWake ? { _threadRunReservationOwner: true } : { _threadRunInflightIdleOwner: true }),
          untilIdle: true,
          runId: reservedRunId,
          memory: withThreadMemory(target.ifIdle?.streamOptions?.memory, resourceId, threadId),
        });
        state.inflightIdleThreadKeysByRunId.delete(reservedRunId);
        state.inflightIdleAgentIdsByRunId.delete(reservedRunId);
        return { action: 'wake' as const, runId: reservedRunId, output };
      } catch (error) {
        rollbackLocalReservation(true);
        this.#releaseThreadLease(pubsub, reservedKey, reservedRunId);
        this.#publish(pubsub, reservedKey, {
          type: 'run-failed',
          runId: reservedRunId,
          error: getErrorFromUnknown(error).message,
        });
        void this.#drainPendingIdleSignals(state, pubsub, reservedKey);
        throw error;
      }
    })();
    // Attach a detached no-op catch so that if stream setup throws (a misconfigured
    // agent: no/unsupported model, FGA denial) and the caller never awaits
    // `result.accepted`, the rejection does not surface as an unhandled rejection.
    // Callers that opt in to `accepted` still see the rejection via their own
    // await/catch — `accepted` itself remains rejectable; only this detached branch is
    // swallowed.
    void accepted.catch(() => {});

    const output: Promise<MastraModelOutput<unknown>> = accepted.then(result => {
      if (result.action !== 'wake') {
        const destinationRunId = 'runId' in result ? result.runId : runId;
        throw new Error(`Agent thread idle wake was delivered to run "${destinationRunId}" in another process`);
      }
      return result.output as unknown as MastraModelOutput<unknown>;
    });
    void output.catch(() => {});

    return acceptSignal({ signal, runId, accepted, output }, runId);
  }
}

export const agentThreadStreamRuntime = new AgentThreadStreamRuntime();
