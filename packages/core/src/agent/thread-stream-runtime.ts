import { randomUUID } from 'node:crypto';

import { EventEmitterPubSub } from '../events/event-emitter';
import type { PubSub } from '../events/pubsub';
import type { EventCallback } from '../events/types';
import { parseMemoryRequestContext } from '../memory/types';
import type { RequestContext } from '../request-context';
import { MASTRA_RESOURCE_ID_KEY, MASTRA_THREAD_ID_KEY } from '../request-context';
import type { MastraModelOutput } from '../stream/base/output';
import type { Agent } from './agent';
import type { AgentExecutionOptions } from './agent.types';
import type { MessageListInput } from './message-list';
import { createMessageSignal, createSignal } from './signals';
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

type AgentThreadRunRecord<OUTPUT = unknown> = {
  agent: Agent<any, any, any, any>;
  output: MastraModelOutput<OUTPUT>;
  runId: string;
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
  threadKeysByRunId: Map<string, string>;
  activeThreadRunIds: Map<string, string>;
  approvalSuspendedRunIds: Set<string>;
  pendingSignalsByThread: Map<string, CreatedAgentSignal[]>;
  pendingIdleSignalsByThread: Map<string, PendingIdleSignal<any>[]>;
  pendingIdleThreadKeysByRunId: Map<string, string>;
  inflightIdleThreadKeysByRunId: Map<string, string>;
  inflightIdleAgentIdsByRunId: Map<string, string>;
  pendingContinuationsByThread: Map<string, PendingContinuation<any>[]>;
  watchedThreadRunIds: Set<string>;
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
};

export type AgentThreadState = 'active' | 'idle';

type SerializableAgentSignal = AgentSignal & Pick<CreatedAgentSignal, 'id' | 'createdAt'>;

type AgentThreadStreamRuntimeEvent =
  | { type: 'run-registered'; runId: string }
  | { type: 'stream-part'; runId: string; part: unknown; sourceId: string }
  | { type: 'run-completed'; runId: string }
  | { type: 'run-suspended'; runId: string }
  | { type: 'run-aborted'; runId: string }
  | { type: 'signal-enqueued'; runId: string; signal: SerializableAgentSignal; sourceId: string };

function getIdleRunRejectedHandler(ifIdle: unknown): (() => void) | undefined {
  const handler = (ifIdle as { _onThreadStreamRunRejected?: unknown } | undefined)?._onThreadStreamRunRejected;
  return typeof handler === 'function' ? () => handler() : undefined;
}

function createRuntimeState(): AgentThreadRuntimeState {
  return {
    threadRunsById: new Map(),
    threadKeysByRunId: new Map(),
    activeThreadRunIds: new Map(),
    approvalSuspendedRunIds: new Set(),
    pendingSignalsByThread: new Map(),
    pendingIdleSignalsByThread: new Map(),
    pendingIdleThreadKeysByRunId: new Map(),
    inflightIdleThreadKeysByRunId: new Map(),
    inflightIdleAgentIdsByRunId: new Map(),
    pendingContinuationsByThread: new Map(),
    watchedThreadRunIds: new Set(),
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

  #isThreadBlockingRun(state: AgentThreadRuntimeState, record: AgentThreadRunRecord<any>) {
    return record.output.status === 'running' || this.#isApprovalSuspendedRun(state, record.runId);
  }

  #serializeSignal(signal: CreatedAgentSignal): SerializableAgentSignal {
    return signal;
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

  async #publishRegistrationAndWait(pubsub: PubSub | undefined, key: string, runId: string): Promise<void> {
    try {
      await this.#publishAndWait(pubsub, key, { type: 'run-registered', runId });
    } catch (error) {
      if (error instanceof AgentThreadOutputDrainError) throw error;
      throw new AgentThreadOutputDrainError(
        'registration-publish-failed',
        `Failed to publish run-registered for agent thread run ${runId}`,
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

  #markApprovalSuspendedFromPart(pubsub: PubSub | undefined, runId: string, part: unknown) {
    // A tool-call-approval part marks the run as approval-suspended so thread
    // subscribers don't treat the resulting run-suspended as a terminal idle.
    if (part && typeof part === 'object' && 'type' in part && part.type === 'tool-call-approval') {
      this.#getState(pubsub).approvalSuspendedRunIds.add(runId);
    }
  }

  // For local (EventEmitterPubSub) runs the fork does not republish the stream
  // through pubsub, so thread subscribers cannot be fed by `stream-part` events
  // like remote runs are. Historically the subscriber loop then read the run's
  // single `output.fullStream` directly, which only one consumer can drain — every
  // other subscriber (and the caller) starved or hung. This builds a multicast
  // buffer that drains the source exactly once into a shared `parts[]` array and
  // hands each subscriber an independent ReadableStream replaying that buffer,
  // matching upstream's `#withBroadcastStream` fan-out. The single drain loop also
  // records the approval-suspension marker before any subscriber observes the
  // `tool-call-approval` part — folding in the former `#attachLocalApprovalDetection`
  // so the marker is set strictly ahead of the part (keeping the thread blocked
  // when `getThreadState` is queried) while the subscriber's reader stays unlocked.
  #withLocalBroadcastStream<OUTPUT>(output: MastraModelOutput<OUTPUT>, pubsub: PubSub | undefined) {
    const runtime = this;
    const runId = output.runId;

    const parts: unknown[] = [];
    const waiters = new Set<() => void>();
    let started = false;
    let done = false;
    let error: unknown;
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
    // it, then (below) redefine the own property to a multicast replay so the
    // caller reads through the same buffer instead of racing the drain.
    const ownFullStream = Object.prototype.hasOwnProperty.call(output, 'fullStream');
    const capturedSource = ownFullStream ? (output.fullStream as any) : undefined;

    const start = () => {
      if (started) return;
      started = true;
      void (async () => {
        try {
          // For getter-based outputs read `output.fullStream` lazily so the
          // evented getter yields a fresh stream for the drain loop, independent
          // of the caller's own access; for own-property one-shot outputs drain
          // the single captured stream.
          const source = ownFullStream ? capturedSource : (output.fullStream as any);
          if (!source) return;
          for await (const part of source) {
            runtime.#markApprovalSuspendedFromPart(pubsub, runId, part);
            parts.push(part);
            wake();
          }
        } catch (caught) {
          error = caught;
          hasError = true;
        } finally {
          done = true;
          wake();
        }
      })();
    };

    const createSubscriberStream = (): ReadableStream<unknown> => {
      let index = 0;
      let closed = false;
      let waiter: (() => void) | undefined;
      return new ReadableStream({
        async pull(controller) {
          start();
          while (!closed) {
            if (index < parts.length) {
              controller.enqueue(parts[index++]);
              return;
            }
            if (hasError) {
              // Presence-tracked separately: a source that throws a FALSY value
              // (undefined/null/0/'') must still fail the subscriber stream
              // rather than close it cleanly (PR #202 review).
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

    // For the own-property one-shot path, redefine `fullStream` to a multicast
    // replay stream so the caller reads through the shared buffer (single drain,
    // no starvation) instead of racing the eager drain for the single stream.
    if (ownFullStream) {
      Object.defineProperty(output, 'fullStream', {
        configurable: true,
        enumerable: true,
        get: () => createSubscriberStream(),
      });
    }

    // Kick the drain eagerly (matching upstream's `#withBroadcastStream`, whose
    // buffer fills regardless of subscribers). A LOCAL run consumed only by the
    // original caller — with no thread subscribers — must still run the drain so
    // `#markApprovalSuspendedFromPart` observes a `tool-call-approval` part and
    // marks the run approval-suspended; otherwise the finalizer treats a
    // suspended run as completed and clears its record. The buffer-fills-
    // regardless memory profile was explicitly accepted by review.
    start();

    return createSubscriberStream;
  }

  #prepareBroadcastSource<OUTPUT>(
    output: MastraModelOutput<OUTPUT>,
    pubsub: PubSub | undefined,
    key: string,
  ): {
    source?: AsyncIterable<unknown> | ReadableStream<unknown>;
    createSubscriberStream?: () => ReadableStream<unknown>;
  } {
    if (this.#getPubSub(pubsub) instanceof EventEmitterPubSub) {
      return { createSubscriberStream: this.#withLocalBroadcastStream(output, pubsub) };
    }

    let source = output.fullStream as any;
    if (!source) return {};

    if (Object.prototype.hasOwnProperty.call(output, 'fullStream')) {
      if (typeof source.tee === 'function') {
        const [broadcastSource, callerSource] = source.tee();
        source = broadcastSource;
        Object.defineProperty(output, 'fullStream', {
          configurable: true,
          enumerable: true,
          value: callerSource,
        });
      } else {
        const runtime = this;
        const fullStream = (async function* () {
          for await (const part of source) {
            await runtime.#publishAndWait(pubsub, key, {
              type: 'stream-part',
              runId: output.runId,
              part,
              sourceId: runtime.#id,
            });
            yield part;
          }
        })();
        Object.defineProperty(output, 'fullStream', {
          configurable: true,
          enumerable: true,
          value: fullStream,
        });
        return {};
      }
    }

    return { source };
  }

  async #broadcastStream<OUTPUT>(
    output: MastraModelOutput<OUTPUT>,
    source: AsyncIterable<unknown> | ReadableStream<unknown> | undefined,
    pubsub: PubSub | undefined,
    key: string,
  ) {
    if (!source) return;

    for await (const part of source) {
      this.#markApprovalSuspendedFromPart(pubsub, output.runId, part);
      await this.#publishAndWait(pubsub, key, {
        type: 'stream-part',
        runId: output.runId,
        part,
        sourceId: this.#id,
      });
    }
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
    state.threadRunsById.clear();
    state.threadKeysByRunId.clear();
    state.activeThreadRunIds.clear();
    state.approvalSuspendedRunIds.clear();
    state.pendingSignalsByThread.clear();
    state.pendingIdleSignalsByThread.clear();
    state.pendingIdleThreadKeysByRunId.clear();
    state.inflightIdleThreadKeysByRunId.clear();
    state.inflightIdleAgentIdsByRunId.clear();
    state.pendingContinuationsByThread.clear();
    state.watchedThreadRunIds.clear();
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
    if (!state.approvalSuspendedRunIds.has(runId)) return false;
    const reservedKey = state.threadKeysByRunId.get(runId);
    if (reservedKey !== undefined && reservedKey !== key) return false;

    state.approvalSuspendedRunIds.delete(runId);
    state.threadRunsById.delete(runId);
    if (state.threadKeysByRunId.get(runId) === key) {
      state.threadKeysByRunId.delete(runId);
    }
    if (state.activeThreadRunIds.get(key) === runId) {
      state.activeThreadRunIds.delete(key);
    }
    state.reservedAgentIdsByRunId.delete(runId);
    state.watchedThreadRunIds.delete(runId);
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
    // Fork broadcast pattern: #prepareBroadcastSource either tees `output.fullStream`
    // in place for remote pubsub (subscribers read the teed caller side via the
    // record's `output`, #broadcastStream publishes the broadcast side once
    // registration lands) or, for the local EventEmitterPubSub, returns a
    // multicast `createSubscriberStream` factory so every same-runtime subscriber
    // gets an independent fan-out view instead of competing over `output.fullStream`.
    const { source: broadcastSource, createSubscriberStream } = this.#prepareBroadcastSource(output, pubsub, key);
    const startBroadcast = () => this.#broadcastStream(output, broadcastSource, pubsub, key);
    const record: AgentThreadRunRecord<any> = {
      agent: { id: `persisted-signal:${signal.id}` } as Agent<any, any, any, any>,
      output,
      runId,
      threadId,
      resourceId,
      streamOptions: {},
      createSubscriberStream,
    };

    state.threadRunsById.set(runId, record);
    state.threadKeysByRunId.set(runId, key);
    const registered = this.#publishAndWait(pubsub, key, { type: 'run-registered', runId });
    const broadcast = registered.then(startBroadcast, startBroadcast).catch(() => {});
    // PR #202 review (P2): the synthetic output's `finish()` fires at stream
    // CONSTRUCTION, long before an async pubsub has published the stream-parts.
    // Publishing `run-completed` off `_waitUntilFinished()` alone let remote
    // subscribers observe completion FIRST, delete the remote run, and ignore
    // the late persisted-signal parts. Gate completion on registration + the
    // stream-part broadcast settling (local multicast resolves immediately —
    // `#broadcastStream` is a fast no-op without a remote source).
    void Promise.allSettled([output._waitUntilFinished(), broadcast]).then(() => {
      setTimeout(() => {
        state.threadRunsById.delete(runId);
        state.threadKeysByRunId.delete(runId);
        if (state.activeThreadRunIds.get(key) === runId) {
          state.activeThreadRunIds.delete(key);
        }
        this.#publish(pubsub, key, { type: 'run-completed', runId });
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
    const { source: broadcastSource, createSubscriberStream } = this.#prepareBroadcastSource(output, pubsub, key);
    const record: AgentThreadRunRecord<OUTPUT> = {
      agent,
      output,
      runId: output.runId,
      threadId,
      resourceId,
      streamOptions: streamOptions as AgentThreadRunRecord<OUTPUT>['streamOptions'],
      createSubscriberStream,
    };

    state.threadRunsById.set(output.runId, record);
    state.threadKeysByRunId.set(output.runId, key);
    state.activeThreadRunIds.set(key, output.runId);
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
    const registrationPublish = this.#publishRegistrationAndWait(pubsub, key, output.runId);
    // The Harness output-drain waiter may not attach until the model has already
    // produced FullOutput. Mark the promise observed immediately while retaining
    // the original rejecting promise below for that later waiter.
    void registrationPublish.catch(() => {});
    this.#threadOutputRegistrations.set(output, registrationPublish);
    state.registrationPublishesByRunId.set(output.runId, registrationPublish);
    const broadcast = registrationPublish.then(() => this.#broadcastStream(output, broadcastSource, pubsub, key));
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
    if (state.watchedThreadRunIds.has(record.runId)) return;
    state.watchedThreadRunIds.add(record.runId);

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
        await state.registrationPublishesByRunId.get(record.runId)?.catch(() => {});
        state.registrationPublishesByRunId.delete(record.runId);
        await state.broadcastsByRunId.get(record.runId)?.catch(() => {});
        state.broadcastsByRunId.delete(record.runId);
        state.watchedThreadRunIds.delete(record.runId);
        this.#cleanupPreparedRun(state, record.runId);

        // An approval-suspended run is paused, not finished: surface run-suspended and
        // leave its records in place so a later resume can re-attach to the same thread.
        if (record.output.status === 'suspended' && this.#isApprovalSuspendedRun(state, record.runId)) {
          await this.#publishTerminalAndWait(pubsub, key, { type: 'run-suspended', runId: record.runId });
          resolveTerminal();
          return;
        }

        state.approvalSuspendedRunIds.delete(record.runId);
        this.#forgetCallerSignalsForRun(state, record.runId);
        await this.#publishTerminalAndWait(pubsub, key, { type: 'run-completed', runId: record.runId });
        resolveTerminal();
        if (state.activeThreadRunIds.get(key) === record.runId) {
          state.activeThreadRunIds.delete(key);
        }
        if (state.threadKeysByRunId.get(record.runId) === key) {
          state.threadKeysByRunId.delete(record.runId);
        }
        try {
          await this.#drainPendingSignals(state, pubsub, key, record);
        } finally {
          state.threadRunsById.delete(record.runId);
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
        state.approvalSuspendedRunIds.delete(record.runId);
        state.pendingSignalsByThread.delete(key);
        if (state.activeThreadRunIds.get(key) === record.runId) {
          state.activeThreadRunIds.delete(key);
        }
        if (state.threadKeysByRunId.get(record.runId) === key) {
          state.threadKeysByRunId.delete(record.runId);
        }
        state.threadRunsById.delete(record.runId);
        this.#forgetCallerSignalsForRun(state, record.runId);
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

    const queue = state.pendingSignalsByThread.get(key);
    const signal = queue?.shift();
    if (signal && queue) {
      if (queue.length === 0) {
        state.pendingSignalsByThread.delete(key);
      }

      const output = await previousRun.agent.stream(signal, {
        ...(previousRun.streamOptions as any),
        runId: randomUUID(),
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
    }

    if (await this.#drainPendingContinuations(state, pubsub, key)) {
      return;
    }

    await this.#drainPendingIdleSignals(state, pubsub, key);
  }

  async #drainPendingContinuations(state: AgentThreadRuntimeState, pubsub: PubSub | undefined, key: string) {
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
      .catch(() => {
        state.threadKeysByRunId.delete(pending.runId);
        this.#cleanupPreparedRun(state, pending.runId);
        if (state.activeThreadRunIds.get(key) === pending.runId) {
          state.activeThreadRunIds.delete(key);
        }
        void this.#drainPendingContinuations(state, pubsub, key).then(started => {
          if (!started) {
            void this.#drainPendingIdleSignals(state, pubsub, key);
          }
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

  async #drainPendingIdleSignals(state: AgentThreadRuntimeState, pubsub: PubSub | undefined, key: string) {
    if (state.activeThreadRunIds.has(key)) {
      return;
    }

    const idleQueue = state.pendingIdleSignalsByThread.get(key);
    const pendingIdle = idleQueue?.shift();
    if (!pendingIdle || !idleQueue) {
      return;
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
      return;
    }
    if (state.threadRunsById.has(pendingIdle.runId)) {
      pendingIdle.onRunRejected?.();
      this.#releaseReservedRun(state, pubsub, key, pendingIdle.runId, {
        cleanupPrepared: true,
        clearAbort: true,
        rejectOutputWaiters: true,
      });
      return;
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
    try {
      const output = await pendingIdle.agent.stream(pendingIdle.signal, {
        ...(pendingIdle.streamOptions as any),
        ...(reserveBeforePreflight ? { _threadRunReservationOwner: true } : { _threadRunInflightIdleOwner: true }),
        runId: pendingIdle.runId,
        memory: withThreadMemory(pendingIdle.streamOptions?.memory, pendingIdle.resourceId, pendingIdle.threadId),
      });
      state.inflightIdleThreadKeysByRunId.delete(pendingIdle.runId);
      state.inflightIdleAgentIdsByRunId.delete(pendingIdle.runId);

      if ((idleQueue?.length ?? 0) > 0) {
        const nextRecord = state.threadRunsById.get(output.runId);
        if (nextRecord) {
          void this.#watchThreadRunCompletion(state, pubsub, key, nextRecord);
        }
      }
    } catch {
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
        await this.#drainPendingIdleSignals(state, pubsub, key);
      }
    }
  }

  drainPendingSignals(runId: string, pubsub?: PubSub) {
    const state = this.#getState(pubsub);
    const record = state.threadRunsById.get(runId);
    const key = record ? this.#threadKey(record.resourceId, record.threadId) : state.threadKeysByRunId.get(runId);
    if (!key) return [];

    const queue = state.pendingSignalsByThread.get(key);
    if (!queue || queue.length === 0) {
      return [];
    }

    state.pendingSignalsByThread.delete(key);
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

  async #waitForRemoteRunToFinish(pubsub: PubSub | undefined, key: string, runId: string) {
    const resolvedPubSub = this.#getPubSub(pubsub);
    const topic = this.#threadTopic(key);
    await new Promise<void>(resolve => {
      const onEvent: EventCallback = event => {
        const data = event.data as AgentThreadStreamRuntimeEvent | undefined;
        if ((data?.type === 'run-completed' || data?.type === 'run-aborted') && data.runId === runId) {
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
    const seenRunIds = new Set<string>();
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
      { parts: unknown[]; waiters: Array<() => void>; done: boolean; stream: ReadableStream<unknown> }
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
        outputsByRunId.delete(runId);
        // A rejected terminal has no delivery event to release the run-id
        // admission guard. Once its exact output is gone, permit a legitimate
        // later segment (approval resume/retry) to reuse the same run id.
        seenRunIds.delete(runId);
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
      if (done || seenRunIds.has(record.runId)) return;
      seenRunIds.add(record.runId);
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

    const createRemoteRun = (runId: string): AgentThreadRunRecord<any> => {
      const remoteRun = {
        parts: [] as unknown[],
        waiters: [] as Array<() => void>,
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
        },
      });
      remoteRuns.set(runId, remoteRun);
      return {
        agent,
        output: {
          runId,
          status: 'running',
          fullStream: remoteRun.stream,
          _waitUntilFinished: async () => {},
        } as MastraModelOutput<any>,
        runId,
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
        const record = state.threadRunsById.get(data.runId) ?? createRemoteRun(data.runId);
        enqueueRun(record);
        wake();
        return;
      }
      if (data.type === 'stream-part') {
        if (data.sourceId === this.#id) return;
        const remoteRun = remoteRuns.get(data.runId);
        if (!remoteRun) return;
        remoteRun.parts.push(data.part);
        while (remoteRun.waiters.length) remoteRun.waiters.shift()?.();
        return;
      }
      if (data.type === 'signal-enqueued') {
        if (data.sourceId === this.#id) return;
        const queue = state.pendingSignalsByThread.get(key) ?? [];
        queue.push(createSignal(data.signal));
        state.pendingSignalsByThread.set(key, queue);
        return;
      }
      if (data.type === 'run-completed' || data.type === 'run-aborted' || data.type === 'run-suspended') {
        markRunTerminalDelivered(data.runId);
        if (
          (data.type !== 'run-suspended' || !state.approvalSuspendedRunIds.has(data.runId)) &&
          state.activeThreadRunIds.get(key) === data.runId
        ) {
          state.activeThreadRunIds.delete(key);
        }
        if (data.type === 'run-aborted') {
          state.pendingSignalsByThread.delete(key);
        }
        if (data.type !== 'run-suspended') {
          state.approvalSuspendedRunIds.delete(data.runId);
        }
        const remoteRun = remoteRuns.get(data.runId);
        if (remoteRun) {
          remoteRun.done = true;
          while (remoteRun.waiters.length) remoteRun.waiters.shift()?.();
          remoteRuns.delete(data.runId);
        }
        // Allow the same runId to be re-enqueued when it resumes (e.g. after tool approval).
        seenRunIds.delete(data.runId);
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

    const unsubscribe = () => {
      if (done) return;
      done = true;
      void resolvedPubSub.unsubscribe(topic, onEvent).catch(() => {});
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
            let fullyDrained = false;
            try {
              for await (const part of subscriberStream) {
                yield part as any;
                if (done) break;
              }
              fullyDrained = !done;
            } finally {
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
  ): SendAgentMessageResult {
    return this.sendSignal(agent, createMessageSignal(message, { acceptedAt: new Date() }), target, pubsub);
  }

  queueMessage<OUTPUT = unknown>(
    agent: Agent<any, any, any, any>,
    message: AgentMessageInput,
    target: QueueAgentMessageOptions<OUTPUT>,
    pubsub?: PubSub,
  ): QueueAgentMessageResult {
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
      return { accepted: true, runId: queuedRunId, signal };
    }

    return this.sendSignal(
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
  ): Promise<SendAgentStateSignalResult> {
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
      return { accepted: true, skipped: true, reason: 'unchanged' };
    }

    return this.sendSignal(agent, applied.signal, target, pubsub);
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
  ): SendAgentSignalResult {
    const state = this.#getState(pubsub);
    const signal = createSignal({ ...signalInput, acceptedAt: new Date() });
    const callerSignalId = signalInput.id;
    let key: string | undefined;
    let runId = target.runId;
    const activeBehavior = target.ifActive?.behavior ?? 'deliver';
    const idleBehavior = target.ifIdle?.behavior ?? 'wake';

    let activeRecord: AgentThreadRunRecord<any> | undefined;
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
        state.activeThreadRunIds.delete(key);
        activeRunId = undefined;
        activeRecord = undefined;
      }

      // Prefer the active same-agent run for thread-targeted signals. This is the normal
      // follow-up path used by clients that know the thread/resource but not the run id.
      if (activeRecord && activeRecord.agent.id === agent.id) {
        runId = activeRecord.runId;
      } else if (
        activeRunId &&
        !activeRecord &&
        !activeRunAborted &&
        (!target.ifIdle ||
          reservedAgentId === agent.id ||
          Boolean((target.ifIdle as { _attachToReservedRun?: unknown })._attachToReservedRun))
      ) {
        // A run can be reserved before its stream record is registered. Keep the reserved
        // id so early follow-ups still attach to the run that is starting.
        runId = activeRunId;
      }
    }

    if (target.runId && state.abortedRunIds.has(target.runId)) {
      throw new Error(`Agent thread run id "${target.runId}" has been aborted`);
    }
    if (runId && !activeRecord) {
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
      if (accepted) return accepted;
    }
    const acceptSignal = (result: SendAgentSignalResult, cache = true): SendAgentSignalResult => {
      if (callerSignalKey && cache) {
        state.acceptedCallerSignals.set(callerSignalKey, result);
        const signalIds = state.callerSignalIdsByRunId.get(result.runId) ?? new Set<string>();
        signalIds.add(callerSignalKey);
        state.callerSignalIdsByRunId.set(result.runId, signalIds);
      }
      return result;
    };

    if (isActiveTarget && activeBehavior !== 'deliver') {
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
        return acceptSignal({ accepted: true, runId: runId!, signal, persisted });
      }
      return acceptSignal({ accepted: true, runId: runId!, signal });
    }

    if (runId) {
      activeRecord ??= state.threadRunsById.get(runId);
      if (activeRecord?.output.status === 'running') {
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
          return acceptSignal({ accepted: true, runId, signal });
        }
      }

      if (key && state.activeThreadRunIds.get(key) === runId) {
        // Reserved local runs need a local queue until registerRun() attaches the stream record.
        // Remote active runs only need the PubSub event; the owning process queues it locally.
        if (state.threadKeysByRunId.get(runId) === key) {
          const queue = state.pendingSignalsByThread.get(key) ?? [];
          queue.push(signal);
          state.pendingSignalsByThread.set(key, queue);
        }
        this.#publish(pubsub, key, {
          type: 'signal-enqueued',
          runId,
          signal: this.#serializeSignal(signal),
          sourceId: this.#id,
        });
        return acceptSignal({ accepted: true, runId, signal });
      }
    }

    if (!resourceId || !threadId) {
      throw new Error('No active agent run found for signal target');
    }

    runId ??= randomUUID();
    if (idleBehavior !== 'wake') {
      if (idleBehavior === 'persist') {
        // Persist the signal AND broadcast it to thread subscribers (upstream
        // parity). The fork previously only persisted, so idle-persist signals —
        // e.g. low-priority notification summaries dispatched via
        // `ifIdle: { behavior: 'persist' }` — never reached same-runtime
        // subscribers. #persistAndBroadcastIdleSignal persists first, then emits
        // a synthetic start/data/finish run through the (multicast) broadcast
        // machinery without waking the agent.
        key ??= this.#threadKey(resourceId, threadId);
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
        return acceptSignal({ accepted: true, runId, signal, persisted }, false);
      }
      return acceptSignal({ accepted: true, runId, signal }, false);
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
      return acceptSignal({ accepted: true, runId, signal });
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
    const output = agent
      .stream(signal, {
        ...(target.ifIdle?.streamOptions as any),
        ...(reserveBeforeIdleWake ? { _threadRunReservationOwner: true } : { _threadRunInflightIdleOwner: true }),
        runId,
        memory: withThreadMemory(target.ifIdle?.streamOptions?.memory, resourceId, threadId),
      })
      .then(output => {
        state.inflightIdleThreadKeysByRunId.delete(runId);
        state.inflightIdleAgentIdsByRunId.delete(runId);
        return output;
      })
      .catch(err => {
        onRunRejected?.();
        if (reserveBeforeIdleWake) {
          this.#releaseReservedRun(state, pubsub, key, runId, {
            cleanupPrepared: true,
            clearAbort: true,
            rejectOutputWaiters: true,
          });
        } else {
          state.inflightIdleThreadKeysByRunId.delete(runId);
          state.inflightIdleAgentIdsByRunId.delete(runId);
          this.rejectUnregisteredRun(runId, pubsub);
        }
        throw err;
      }) as Promise<MastraModelOutput<unknown>>;
    void output.catch(() => {});

    return acceptSignal({ accepted: true, runId, signal, output });
  }
}

export const agentThreadStreamRuntime = new AgentThreadStreamRuntime();
