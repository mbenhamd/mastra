import { randomUUID } from 'node:crypto';
import { getErrorFromUnknown } from '../error';
import { withAck } from '../events/acking-callback';
import { EventEmitterPubSub } from '../events/event-emitter';
import { isLeaseProvider, NoopLeaseProvider } from '../events/pubsub';
import type { LeaseProvider, PubSub } from '../events/pubsub';
import type { EventCallback } from '../events/types';
import { parseMemoryRequestContext } from '../memory/types';
import { MASTRA_RESOURCE_ID_KEY, MASTRA_THREAD_ID_KEY, RequestContext } from '../request-context';
import type { MastraModelOutput } from '../stream/base/output';
import { getChunkProducedAt } from '../stream/base/produced-at';
import { isSignalChunkExcluded } from '../stream/signal-exclusions';
import { ChunkFrom } from '../stream/types';
import type { ChunkType, ThreadHistoryChunk } from '../stream/types';
import { readPositiveIntEnv } from '../utils';
import type { Agent } from './agent';
import type { AgentExecutionOptions } from './agent.types';
import type { MessageListInput } from './message-list';
import type { MastraDBMessage } from './message-list/types';
import { createRecentRequests } from './recent-requests';
import { createMessageSignal, createSignal, resolveDeliveryAttributes } from './signals';
import type { AgentMessageInput, AgentStateSignalInput, CreatedAgentSignal } from './signals';
import { applyStateSignal } from './state-signals';
import {
  collectStoredPendingToolApprovals,
  createThreadHistoryFilter,
  getPartProducedAt,
  stampPartProducedAt,
  toolCallApprovalChunkFromStored,
} from './thread-history';
import { onThreadMessagesSaved } from './thread-saves';
import type {
  AgentAbortThreadOptions,
  AgentClaimThreadPeerOptions,
  AgentSignal,
  AgentSubscribeToThreadOptions,
  AgentThreadIdentityOptions,
  AgentThreadPeerAdvertisement,
  AgentThreadPeerInfo,
  AgentThreadSubscription,
  AgentUpdateThreadPeerOptions,
  DiscoverAgentThreadPeersOptions,
  CancelQueuedAgentMessagesOptions,
  CancelQueuedAgentMessagesResult,
  AgentThreadEventListener,
  SubscribeAgentThreadEventsOptions,
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
const AGENT_THREAD_LEASE_OWNER_PREFIX = 'mastra-thread-owner:';
const REJECTED_RUN_TOMBSTONE_TTL_MS = 5 * 60 * 1000;
const MAX_REJECTED_RUN_TOMBSTONES = 1000;
const ABORTED_RUN_TOMBSTONE_TTL_MS = 5 * 60 * 1000;
const MAX_ABORTED_RUN_TOMBSTONES = 1000;
const SIGNAL_ADMISSION_TOMBSTONE_TTL_MS = 5 * 60 * 1000;
const MAX_SIGNAL_ADMISSION_TOMBSTONES_PER_THREAD = 1000;
const MAX_SIGNAL_ADMISSION_THREADS = 1000;
const TERMINAL_PUBLISH_TIMEOUT_MS = 10_000;
const TERMINAL_DELIVERY_TIMEOUT_MS = 30_000;
// `run-aborted` can arrive just before an already-running tool publishes its
// authoritative `tool-error`. Give that terminal a short, bounded chance to
// cross the subscriber before cancelling its view and falling back to a
// synthetic abort. The bound preserves prompt teardown for abort-ignoring
// streams while keeping real tool errors observable.
const ABORT_OUTPUT_DRAIN_GRACE_MS = 250;

/** @internal Bounded LRU retention for suspended/resumed stream identities. */
export function rememberBoundedResumableTerminalStream(
  retainedByRunId: Map<string, Set<string>>,
  runId: string,
  streamId: string,
  maxRetained = MAX_ABORTED_RUN_TOMBSTONES,
): string[] {
  const evictedStreamIds: string[] = [];
  const retained = retainedByRunId.get(runId) ?? new Set<string>();
  retained.delete(streamId);
  retained.add(streamId);
  while (retained.size > maxRetained) {
    const oldest = retained.values().next().value;
    if (oldest === undefined) break;
    retained.delete(oldest);
    evictedStreamIds.push(oldest);
  }
  retainedByRunId.delete(runId);
  retainedByRunId.set(runId, retained);
  while (retainedByRunId.size > maxRetained) {
    const oldestRunId = retainedByRunId.keys().next().value;
    if (oldestRunId === undefined) break;
    const evictedRun = retainedByRunId.get(oldestRunId);
    retainedByRunId.delete(oldestRunId);
    for (const evictedStreamId of evictedRun ?? []) evictedStreamIds.push(evictedStreamId);
  }
  return evictedStreamIds;
}

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

class AgentThreadLeaseOwnershipLostError extends AgentThreadOutputDrainError {}

/** Pre-dispatch lease failure for callers that require strict signal admission. */
export class AgentThreadSignalAdmissionError extends Error {
  readonly name = 'AgentThreadSignalAdmissionError';

  constructor(
    readonly reason: 'lease-unavailable',
    message: string,
    readonly cause?: unknown,
  ) {
    super(message);
  }
}

/** Internal marker for a claimed-owner request that failed before stream admission. */
class ClaimedOwnerPreAdmissionError extends Error {
  readonly name = 'ClaimedOwnerPreAdmissionError';

  constructor(
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
const AGENT_THREAD_OWNER_DISCOVERY_TOPIC = 'agent.thread-owner-discovery';
/** Safety margin when trimming up to a retained run, covering clock skew between us and the pubsub backend. */
const AGENT_THREAD_OWNER_DISCOVERY_TIMEOUT_MS = 100;
const AGENT_THREAD_CLAIM_LEASE_PREFIX = 'thread-claim:';
const AGENT_THREAD_OWNER_ACCEPTANCE_TIMEOUT_MS = 5_000;
// Long enough for live subscribers (including cross-process readers polling the
// topic) to read a failed run's outcome before its entries are deleted.
const FAILED_RUN_TRIM_DELAY_MS = 30_000;
const AGENT_THREAD_PEER_DISCOVERY_TOPIC = 'agent.thread-peer-discovery';
const AGENT_THREAD_PEER_DISCOVERY_TIMEOUT_MS = 100;

/**
 * Slack added to a request's carried `expiresAt` before a responder drops it
 * as stale. Requests cross processes, so the check compares two machines'
 * clocks; without slack, skew larger than the 100ms discovery window would
 * make responders drop every live request and silently break cross-process
 * discovery. The grace only needs to be small next to the delays it exists to
 * reject — reclaim redelivery (tens of seconds) and backlog replay on a fresh
 * fan-out group (arbitrarily old) — where a late reply would recreate the
 * caller's already-released reply stream on persistent backends.
 */
const AGENT_REQUEST_EXPIRY_SKEW_GRACE_MS = 5_000;

/**
 * True when a request's caller-side deadline has passed by more than the skew
 * grace. Tolerates events without a deadline (older senders): comparisons
 * against `undefined`/`NaN` are false, which degrades to the previous
 * always-respond behavior.
 */
function isStaleRequest(expiresAt: number | undefined): boolean {
  return (
    typeof expiresAt === 'number' &&
    Number.isFinite(expiresAt) &&
    Date.now() - expiresAt > AGENT_REQUEST_EXPIRY_SKEW_GRACE_MS
  );
}

/**
 * Lease TTL for the cross-process thread lease acquired in the idle-wake
 * path. Kept short so a crashed owner process frees the thread quickly; a
 * background timer renews it while the run is still running. Overridable via
 * `MASTRA_AGENT_THREAD_LEASE_TTL_MS` (production keeps the 15s default).
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
/**
 * Bound for the single exact-owner reconciliation read that follows a failed
 * lease operation, following the same internal-deadline pattern as
 * TERMINAL_PUBLISH_TIMEOUT_MS. A read that cannot settle in time is treated
 * as unreadable ownership (fail closed) instead of blocking the failed
 * operation's disposition indefinitely. The agent-signals suite exercises
 * the deadline deterministically with fake timers.
 */
const AGENT_THREAD_LEASE_RECONCILE_TIMEOUT_MS = 5_000;
/**
 * TTL for a suspended run's warm in-memory state — the parked thread-run record
 * (swept by #sweepStaleSuspendedRecords). The Mastra internal-workflow registry
 * reads the same `MASTRA_SUSPENDED_RUN_TTL_MS` so both expire on one bound. A
 * suspended run is kept warm so a same-instance resume can reattach and the thread
 * stays blocked; once it lapses the state is evicted and resume falls back to the
 * durable snapshot. Multi-instance deployments (resume rarely lands on the origin)
 * can shed it sooner; 30 minute default.
 */
const AGENT_SUSPENDED_RUN_TTL_MS = readPositiveIntEnv('MASTRA_SUSPENDED_RUN_TTL_MS', 30 * 60 * 1000);

export const AGENT_THREAD_LEASE_CONFLICT_CODE = 'AGENT_THREAD_LEASE_CONFLICT';

export class AgentThreadLeaseConflictError extends Error {
  readonly code = AGENT_THREAD_LEASE_CONFLICT_CODE;

  constructor(runId: string, owner: string) {
    super(`Cannot register run ${runId}: thread lease is held by ${owner}`);
    this.name = 'AgentThreadLeaseConflictError';
  }
}

export let defaultAgentThreadPubSub: PubSub = new EventEmitterPubSub();

function hasReadOnlyMemory(options?: { memory?: AgentExecutionOptions<any>['memory'] }): boolean {
  const memory = options?.memory;
  return Boolean(memory && typeof memory === 'object' && 'options' in memory && memory.options?.readOnly);
}

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

/**
 * Strip raw model-request bookkeeping from a chunk before it is broadcast to
 * the thread-stream topic (and retained for subscriber replay). `step-start`
 * embeds the full serialized provider request (including any base64 media in
 * context), and `step-finish`/`finish` repeat it via `metadata.request`,
 * `output.steps[]` (each step re-embeds its cumulative request/response) and
 * `messages`. No thread subscriber needs those — they are available to the
 * local caller via `output.request`/`output.steps` — and broadcasting them
 * multiplies the largest object in the system ≥5× per step, persists it in
 * durable pubsub backends, and exposes prompt contents to every subscriber.
 * Delegated agent chunks are wrapped in `tool-output.payload.output`, so known
 * agent-stream wrappers are sanitized recursively. Only broadcast copies are
 * rewritten; the caller's MastraModelOutput is untouched.
 */
const DEFAULT_INITIAL_HISTORY_PER_PAGE = 40;

/**
 * Boundary type for broadcast stream parts after sanitization: parts are
 * opaque chunks, so unrecognized values pass through untouched and only the
 * rewritten `{ type, payload }` object form is reconstructed.
 */
type SanitizedBroadcastPart = { type?: string; payload?: Record<string, unknown> };

function sanitizeBroadcastPart(part: unknown): SanitizedBroadcastPart {
  if (!part || typeof part !== 'object' || !('type' in part)) return part as SanitizedBroadcastPart;
  const typed = part as SanitizedBroadcastPart;
  const payload = typed.payload;
  if (!payload || typeof payload !== 'object') return part as SanitizedBroadcastPart;

  if (typed.type === 'tool-output') {
    const output = payload.output;
    if (output && typeof output === 'object' && 'type' in output && 'from' in output && output.from === 'AGENT') {
      const sanitizedOutput = sanitizeBroadcastPart(output);
      if (sanitizedOutput !== output) {
        return { ...typed, payload: { ...payload, output: sanitizedOutput } };
      }
    }
    return part as SanitizedBroadcastPart;
  }

  if (typed.type === 'step-start') {
    if (!('request' in payload) && !('inputMessages' in payload)) return part as SanitizedBroadcastPart;
    const { request: _request, inputMessages: _inputMessages, ...rest } = payload;
    return { ...typed, payload: rest };
  }

  if (typed.type === 'step-finish' || typed.type === 'finish') {
    let changed = false;
    const next: Record<string, unknown> = { ...payload };
    const metadata = payload.metadata;
    if (metadata && typeof metadata === 'object' && 'request' in metadata) {
      const { request: _request, ...restMetadata } = metadata as Record<string, unknown>;
      next.metadata = restMetadata;
      changed = true;
    }
    const output = payload.output;
    if (output && typeof output === 'object' && 'steps' in output) {
      const { steps: _steps, ...restOutput } = output as Record<string, unknown>;
      next.output = restOutput;
      changed = true;
    }
    if ('messages' in payload) {
      delete next.messages;
      changed = true;
    }
    return changed ? { ...typed, payload: next } : (part as SanitizedBroadcastPart);
  }

  return part as SanitizedBroadcastPart;
}

/**
 * Tear down a per-request reply topic once its request has settled. The
 * requester mints the topic name (it embeds a fresh UUID), is its only
 * subscriber, and nothing will publish to it again — so beyond unsubscribing,
 * ask the broker to drop the topic entirely. On persistent backends (e.g.
 * Redis Streams) merely subscribing creates a real key; without the
 * `clearTopic` every discovery/acceptance round trip would leak one stream
 * forever. Best-effort and fire-and-forget: `clearTopic` is a no-op on
 * in-memory brokers and failures here must never affect the request outcome.
 */
function releaseReplyTopic(pubsub: PubSub, replyTopic: string, cb: EventCallback): void {
  void pubsub
    .unsubscribe(replyTopic, cb)
    .catch(() => {})
    .then(() => pubsub.clearTopic(replyTopic))
    .catch(() => {});
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

type AgentThreadRunContinuation<OUTPUT = unknown> = {
  sourceOutput: MastraModelOutput<OUTPUT>;
  canContinue: () => boolean;
};

type AgentThreadRunRecord<OUTPUT = unknown> = {
  agent: Agent<any, any, any, any>;
  /** The source output that owns the subscriber-facing broadcast. */
  output: MastraModelOutput<OUTPUT>;
  /** The latest execution segment attached to a suspension-spanning broadcast. */
  currentSegmentOutput?: MastraModelOutput<OUTPUT>;
  runId: string;
  streamId: string;
  streamSeq: number;
  lifecycle: AgentThreadRunLifecycle;
  suspensions?: Map<string | undefined, AgentThreadRunSuspension>;
  /** When the record was parked as suspended (ms epoch); drives the TTL sweep. */
  suspendedAt?: number;
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
  /** Mark provider cancellation before its source can reject. */
  markAbortRequested?: () => void;
  /** Force the broadcast/replay view closed after the bounded abort drain grace. */
  abortBroadcast?: () => Promise<void>;
  /** Reliable registration/fence/broadcast/final-terminal delivery for abort. */
  abortDelivery?: Promise<void>;
  /** Exact process-attempt owner authenticated by the thread lease. */
  leaseOwner: string;
  /** Terminalize this exact registered stream independently of provider settlement. */
  finalizeAbort?: () => boolean;
  /** Settles once every stream-part broadcast publish for this run completed. */
  broadcastFinished?: Promise<void>;
  /** Present only while the broadcast may accept resumed execution segments. */
  continuation?: AgentThreadRunContinuation<OUTPUT>;
  /**
   * Outstanding `run-suspended` terminal publication for this record. Installed
   * synchronously before the publication is awaited so abort re-entry can see
   * it and order its own fencing behind the publication.
   */
  suspensionPublication?: Promise<void>;
  /**
   * Set only after `run-suspended` publication settled, the record generation
   * was revalidated, and the completion watcher performed its last
   * run/thread-shared effects. Until this marker is set the watcher remains
   * the sole finalizer; afterwards `#releaseParkedRun` owns finalization.
   */
  parked?: boolean;
  /**
   * Set by the completion watcher once this segment's provider output has
   * settled (its source can produce no more parts). Only then may a same-run
   * resume wait for the segment's own terminal delivery.
   */
  providerSettled?: boolean;
};

type ThreadControlSubscription = {
  ready: Promise<void>;
  references: number;
  observers: number;
  ownedRunIds: Set<string>;
  admittedSignalIds: Set<string>;
  unsubscribe: () => void;
};

type PreparedThreadRun = {
  threadKey: string;
  abortController: AbortController;
  cleanup: () => void;
  /**
   * Set for a first-party `Agent.stream()` preparation. An abort before the
   * preparation settles keeps the reservation until `Agent.stream()` releases
   * it; that release then hands the thread's queued input to a fresh
   * follow-up run (upstream parity) instead of dropping it.
   */
  abortHandoff?: Pick<AgentThreadRunRecord<any>, 'agent' | 'streamOptions'>;
};

type RejectedRunErrorRecord = {
  error: Error;
  cleanupTimer: ReturnType<typeof setTimeout>;
  /**
   * True when the retained error is an infrastructure failure recorded while
   * settling a cancelled attempt: later same-attempt lookups keep the abort
   * message and attach this error as `Error.cause`. Ordinary abort/reject
   * receipts never set it, so their rejection shape is unchanged.
   */
  infrastructure?: boolean;
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
  queueOwnerId?: string;
  cancelled?: boolean;
  /** Guards once-only settlement of a cancelled drain item. */
  cancelSettled?: boolean;
};

type PendingContinuation<OUTPUT = unknown> = {
  agent: Agent<any, any, any, any>;
  messages: MessageListInput;
  runId: string;
  resourceId: string;
  threadId: string;
  streamOptions?: AgentExecutionOptions<OUTPUT>;
};

type CachedCallerSignal = {
  result: SendAgentSignalResult;
  /** Harness dispatch attempt that owns a still-pending native acknowledgement. */
  admissionAttemptId?: string;
  status: 'pending' | 'accepted' | 'rejected';
};

type ClaimedThreadOwnerStreamOptions =
  | AgentExecutionOptions<any>
  | (() => AgentExecutionOptions<any> | Promise<AgentExecutionOptions<any>>);

type ClaimedThreadOwner<OUTPUT = unknown> = {
  agent: Agent<any, any, any, any>;
  resourceId: string;
  threadId: string;
  streamOptions?: ClaimedThreadOwnerStreamOptions;
  peer?: AdvertisedThreadPeer;
  unsubscribe: () => void;
};

type ClaimedThreadOwnerStartResult<OUTPUT = unknown> = {
  runId: string;
  error?: string;
  /** Only set when this helper proves that Agent.stream was never called. */
  preAdmission?: true;
  /** Set when a run started here; lets the caller join its output (wake action). */
  output?: MastraModelOutput<OUTPUT>;
};

type AdvertisedThreadPeer = AgentThreadPeerInfo & {
  sourceId: string;
  unsubscribe: () => void;
};

type ThreadEventListenerRegistration = SubscribeAgentThreadEventsOptions & {
  agent: Agent<any, any, any, any>;
  listener: AgentThreadEventListener;
  lastCount: number;
};

/**
 * What a claimed owner remembers about an idle signal once it has acted on it.
 * A redelivery must not act on the signal again, but it may still have to
 * deliver the caller's reply if the first attempt never reached the backend.
 */
type HandledIdleSignal = {
  /** Where the caller is waiting for its reply. */
  replyTopic: string;
  /** The acceptance reply, kept so a repeat can re-send it verbatim. */
  reply?: AgentThreadIdleSignalAcceptanceEvent;
  /** Whether that reply reached the backend. Set once its publish resolves. */
  replyPublished: boolean;
};

type UnresolvedForwarding = {
  /**
   * The committed item whose forwarding publication rejected: its admission
   * is unknown. Retained with its original payload and stable id — never
   * restored into an ordinary queue, so no completion drain, in-loop or
   * pre-run consumption, sibling advancement, or later forwarding handoff
   * can consume or overtake it automatically.
   */
  signal: CreatedAgentSignal;
  /** Which ordinary queue the item headed when it was committed. */
  queueKind: 'pending' | 'pre-run' | 'idle';
  /** Exact winner the rejected publication targeted. */
  destinationRunId: string;
  /** Exact winner owner token the rejected publication targeted. */
  destinationOwner: string;
  /** Local attempt whose infrastructure receipt carries the original error. */
  receiptRunId: string;
  /** The original publication rejection: unknown admission, not proof unsent. */
  error: Error;
  /**
   * Idle-only settlement identity, preserved for explicit reconciliation.
   * It is never invoked automatically: a later cancellation or drain must
   * not claim the ambiguously published item as locally cancelled/unsent.
   */
  onRunRejected?: () => void;
};

type AgentThreadRuntimeState = {
  threadRunsById: Map<string, AgentThreadRunRecord<any>>;
  threadRunsByStreamId: Map<string, AgentThreadRunRecord<any>>;
  threadKeysByRunId: Map<string, string>;
  remoteThreadKeysByRunId: Map<string, string>;
  activeThreadRunIds: Map<string, string>;
  activeThreadStreamIds: Map<string, string>;
  streamSeqByRunId: Map<string, number>;
  approvalSuspendedRunIds: Set<string>;
  suspendedRunIds: Set<string>;
  suspensionMetadataByRunId: Map<string, Map<string | undefined, AgentThreadRunSuspension>>;
  pendingSignalsByThread: Map<string, CreatedAgentSignal[]>;
  // Signals queued for a run that is starting but has not made its first model
  // request yet. The first LLM step drains these and folds them into that
  // request; `pendingSignalsByThread` follow-ups instead become their own turn.
  preRunSignalsByThread: Map<string, CreatedAgentSignal[]>;
  pendingIdleSignalsByThread: Map<string, PendingIdleSignal<any>[]>;
  pendingIdleThreadKeysByRunId: Map<string, string>;
  inflightIdleThreadKeysByRunId: Map<string, string>;
  inflightIdleAgentIdsByRunId: Map<string, string>;
  /** A dequeued idle message retains its cancellation identity until execution begins. */
  drainingIdleSignalsByThread: Map<string, PendingIdleSignal<any>>;
  drainingPendingSignalsByThread: Map<string, { signal: CreatedAgentSignal; cancelled: boolean; handedOff: boolean }>;
  /**
   * An outstanding verified-loss forwarding handoff per thread: the original
   * owner positively lost the lease to a foreign winner and is transferring
   * its ordered queued tail to that winner by publication only. Presence is
   * an exclusive marker — a competing observer/drain must not capture the
   * same queued work while the handoff's publications are outstanding.
   */
  foreignWinnerHandoffsByThread: Map<string, { owner: string; runId: string }>;
  /**
   * Fail-closed retention for an ambiguously published forward per thread.
   * Once a committed forwarding publication rejects, the runtime cannot
   * tell whether the winner admitted the item (a generic rejection is not
   * proof it never arrived), so the item is fenced here pending an
   * authoritative disposition or explicit reconciliation — neither of which
   * is implemented (no shared ledger/service or public reconciliation API
   * exists absent a product decision). Presence fences every automatic
   * execution path on the thread; retention is in-memory only, with no
   * guaranteed liveness or crash durability. Receipt/admission TTL expiry
   * never converts this entry back into executable work.
   */
  unresolvedForwardingsByThread: Map<string, UnresolvedForwarding>;
  pendingContinuationsByThread: Map<string, PendingContinuation<any>[]>;
  claimedThreadOwnerDiscoveries: Map<string, Promise<string | undefined>>;
  /**
   * Idle signals this process has already acted on, keyed by request id, with the
   * reply it sent. Backends deliver at least once, so a redelivery of a signal
   * that already started or joined a run must not queue it again or start a
   * second run — the wake path is not idempotent — but it may still have to
   * re-send a reply that never reached the caller.
   */
  handledIdleSignals: ReturnType<typeof createRecentRequests<HandledIdleSignal>>;
  claimedThreadOwners: Map<string, ClaimedThreadOwner<any>>;
  advertisedThreadPeers: Map<string, AdvertisedThreadPeer>;
  watchedThreadStreamIds: Set<string>;
  preparedRunsById: Map<string, PreparedThreadRun>;
  reservedAgentIdsByRunId: Map<string, string>;
  reservationWaitersByRunId: Map<string, Array<() => void>>;
  resumeTailsByRunId: Map<string, Promise<void>>;
  abortedRunIds: Set<string>;
  abortedRunCleanupTimersByRunId: Map<string, ReturnType<typeof setTimeout>>;
  rejectedRunErrorsByRunId: Map<string, RejectedRunErrorRecord>;
  acceptedCallerSignals: Map<string, CachedCallerSignal>;
  callerSignalIdsByRunId: Map<string, Set<string>>;
  /** Bounded stable signal-id admissions retained beyond run termination. */
  signalAdmissionsByThread: Map<
    string,
    Map<string, { payloadKey: string; runId: string; expiresAt: number; admissionAttemptId?: string }>
  >;
  /** One unref'd sweep timer evicts expired admissions across otherwise-idle threads. */
  signalAdmissionCleanupTimer?: ReturnType<typeof setTimeout>;
  /** Process-attempt lease owner tokens keyed by the stable public run id. */
  leaseOwnerTokensByRunId: Map<string, string>;
  /** Exact authenticated identity for the one current remote segment per thread. */
  remoteStreamIdentityByThread: Map<string, { runId: string; streamId: string; leaseOwner: string; streamSeq: number }>;
  pendingOutputWaiters: Map<
    string,
    Array<{ resolve: (out: MastraModelOutput<any>) => void; reject: (error: Error) => void }>
  >;
  registrationPublishesByStreamId: Map<string, Promise<void>>;
  broadcastsByStreamId: Map<string, Promise<void>>;
  startingQueuedRunIds: Set<string>;
  /**
   * Active lease-renewal timers keyed by runId. Set when the owner
   * process wins the cross-process lease, cleared on release. Stored
   * here (not on a Map<key,timer>) so a run's renewal timer survives even
   * if `activeThreadRunIds` is rotated by a follow-up signal.
   */
  leaseRenewalTimers: Map<string, ReturnType<typeof setInterval>>;
  threadEventListeners: Set<ThreadEventListenerRegistration>;
  threadControlSubscriptions: Map<string, ThreadControlSubscription>;
};

export type AgentThreadState = 'active' | 'idle';

export type ActiveThreadRun = { runId: string; resourceId?: string; threadId: string };

type AgentThreadRunContinuationMode = 'across-suspension';

export type AgentThreadStrictRegistrationOptions = {
  strict: true;
  continuation?: AgentThreadRunContinuationMode;
  /**
   * Revalidates ownership held outside this runtime (for example, a durable
   * recovery lease). A validation failure rolls back local registration but
   * deliberately leaves the same-run thread lease intact for the new owner.
   */
  validate?: () => void | Promise<void>;
};

type AgentThreadStreamRegistrationOptions = {
  strict?: false;
  continuation?: AgentThreadRunContinuationMode;
};

export type AgentThreadRunRegistration = {
  /**
   * Remove this exact registration. Callers that lost an external ownership
   * claim can preserve the same-run thread lease for its successor.
   */
  rollback(options?: { releaseLease?: boolean }): Promise<void>;
};

type SerializableAgentSignal = AgentSignal & Pick<CreatedAgentSignal, 'id' | 'createdAt'>;

type AgentThreadStreamRuntimeEvent =
  | {
      type: 'run-registered';
      runId: string;
      streamId: string;
      streamSeq: number;
      sourceId?: string;
      leaseOwner: string;
    }
  | { type: 'run-aborting'; runId: string; streamId: string; leaseOwner: string }
  | {
      type: 'stream-part';
      runId: string;
      streamId: string;
      part: unknown;
      sourceId: string;
      leaseOwner: string;
      /** Epoch ms the part was produced; publishing can lag behind it. */
      producedAt?: number;
      /** Kept by save-time trims; removed only when the whole run is trimmed. */
      pinned?: boolean;
    }
  | {
      type: 'run-completed';
      runId: string;
      streamId?: string;
      persisted?: boolean;
      /** The run's final status; `success` means storage holds all of it and nothing is left to act on. */
      status?: string;
      leaseOwner?: string;
    }
  | { type: 'run-suspended'; runId: string; streamId?: string; leaseOwner?: string }
  | { type: 'run-discarded'; runId: string; streamId: string; leaseOwner?: string }
  | {
      type: 'run-abort-requested';
      runId: string;
      streamId: string;
      leaseOwner?: string;
      clearPendingSignals?: boolean;
    }
  | { type: 'signals-cancelled'; signalIds: string[] }
  | { type: 'run-aborted'; runId: string; streamId?: string; leaseOwner?: string }
  | { type: 'run-failed'; runId: string; streamId?: string; error: string; leaseOwner?: string }
  | { type: 'signal-enqueued'; runId: string; signal: SerializableAgentSignal; sourceId: string; preRun?: boolean }
  | {
      type: 'idle-signal-enqueued';
      runId: string;
      signal: SerializableAgentSignal;
      sourceId: string;
      requestId: string;
      replyTopic: string;
      targetSourceId: string;
      /**
       * Caller's absolute deadline. Optional only on the wire: current senders
       * always set it, but during a rolling deploy an older process publishes
       * `timeoutMs` instead. The receiving handler normalizes.
       */
      expiresAt?: number;
      /** Legacy relative window from pre-`expiresAt` senders. */
      timeoutMs?: number;
    };

type AgentThreadIdleSignalAcceptanceEvent =
  | { type: 'idle-signal-accepted'; requestId: string; runId: string; sourceId: string }
  | {
      type: 'idle-signal-rejected';
      requestId: string;
      runId: string;
      sourceId: string;
      error: string;
      /** Internal wire marker; sender may safely release this attempt's admission. */
      preAdmission?: boolean;
    };

type AgentThreadOwnerDiscoveryEvent =
  | {
      type: 'thread-owner-request';
      key: string;
      requestId: string;
      replyTopic: string;
      sourceId: string;
      /** Caller's absolute deadline; optional on the wire (legacy senders omit it). */
      expiresAt?: number;
      /**
       * Set when the requester wants to own the thread itself (as opposed to
       * looking the owner up for signal delivery). Owners may yield to it.
       */
      intent?: 'claim';
      /** Lease owner the request is addressed to. Older senders omit it. */
      targetSourceId?: string;
    }
  | { type: 'thread-owner-response'; key: string; requestId: string; sourceId: string };

type AgentThreadPeerDiscoveryEvent =
  // expiresAt is optional on the wire: legacy senders omit it.
  | { type: 'thread-peer-request'; requestId: string; replyTopic: string; sourceId: string; expiresAt?: number }
  | { type: 'thread-peer-response'; requestId: string; peer: AgentThreadPeerInfo; sourceId: string };

function toPublicThreadPeer(peer: AdvertisedThreadPeer): Omit<AdvertisedThreadPeer, 'unsubscribe'> {
  const { unsubscribe: _unsubscribe, ...publicPeer } = peer;
  return publicPeer;
}

function createThreadPeerId(agentId: string, resourceId: string, threadId: string): string {
  return [agentId, resourceId, threadId].map(part => encodeURIComponent(part)).join(':');
}

function getIdleRunRejectedHandler(ifIdle: unknown): (() => void) | undefined {
  const handler = (ifIdle as { _onThreadStreamRunRejected?: unknown } | undefined)?._onThreadStreamRunRejected;
  return typeof handler === 'function' ? () => handler() : undefined;
}

function getIdleSignalDiscardHandler(ifIdle: unknown): (() => void) | undefined {
  const handler = (ifIdle as { _onThreadStreamSignalDiscarded?: unknown } | undefined)?._onThreadStreamSignalDiscarded;
  return typeof handler === 'function' ? () => handler() : undefined;
}

function hasFullLogicalMessageIdentity(ifIdle: unknown): boolean {
  const identity = (ifIdle as { streamOptions?: { logicalMessageIdentity?: unknown } } | undefined)?.streamOptions
    ?.logicalMessageIdentity;
  return typeof identity === 'object' && identity !== null && 'response' in identity;
}

function createRuntimeState(): AgentThreadRuntimeState {
  return {
    threadRunsById: new Map(),
    threadRunsByStreamId: new Map(),
    threadKeysByRunId: new Map(),
    remoteThreadKeysByRunId: new Map(),
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
    drainingIdleSignalsByThread: new Map(),
    drainingPendingSignalsByThread: new Map(),
    foreignWinnerHandoffsByThread: new Map(),
    unresolvedForwardingsByThread: new Map(),
    pendingContinuationsByThread: new Map(),
    claimedThreadOwnerDiscoveries: new Map(),
    handledIdleSignals: createRecentRequests<HandledIdleSignal>(),
    claimedThreadOwners: new Map(),
    advertisedThreadPeers: new Map(),
    watchedThreadStreamIds: new Set(),
    preparedRunsById: new Map(),
    reservedAgentIdsByRunId: new Map(),
    reservationWaitersByRunId: new Map(),
    resumeTailsByRunId: new Map(),
    abortedRunIds: new Set(),
    abortedRunCleanupTimersByRunId: new Map(),
    rejectedRunErrorsByRunId: new Map(),
    acceptedCallerSignals: new Map(),
    callerSignalIdsByRunId: new Map(),
    signalAdmissionsByThread: new Map(),
    leaseOwnerTokensByRunId: new Map(),
    remoteStreamIdentityByThread: new Map(),
    pendingOutputWaiters: new Map(),
    registrationPublishesByStreamId: new Map(),
    broadcastsByStreamId: new Map(),
    startingQueuedRunIds: new Set(),
    leaseRenewalTimers: new Map(),
    threadEventListeners: new Set(),
    threadControlSubscriptions: new Map(),
  };
}

type ThreadRegistrationListener = (
  event: Extract<AgentThreadStreamRuntimeEvent, { type: 'run-registered' }>,
  trustedRecord: AgentThreadRunRecord<any>,
) => void;

/**
 * Immutable lease-ownership provenance for one thread-lease operation.
 *
 * Captured synchronously BEFORE the operation's provider await (and refreshed
 * for the renewal timer / run record the settled operation itself installs),
 * then passed into transfer/release cleanup instead of rereading the mutable
 * maps by run id. A successor installed for the same run id while the
 * operation is outstanding — including one that re-adopts the retained token,
 * where token equality cannot distinguish the attempts — replaces the run
 * record and/or the renewal timer, so cleanup fences on all three captured
 * identities. A record that is merely absent was trimmed, not adopted.
 */
type ThreadLeaseOperationProvenance = {
  runId: string;
  token: string | undefined;
  timer: ReturnType<typeof setInterval> | undefined;
  record: AgentThreadRunRecord<any> | undefined;
};

export class AgentThreadStreamRuntime {
  #id = randomUUID();
  #statesByPubSub = new WeakMap<PubSub, AgentThreadRuntimeState>();
  #threadRegistrationListenersByPubSub = new WeakMap<PubSub, Map<string, Set<ThreadRegistrationListener>>>();
  #threadOutputRegistrations = new WeakMap<object, Promise<void>>();
  #threadOutputTerminals = new WeakMap<object, Promise<void>>();
  /**
   * Parked-abort delivery receipts keyed by the exact retired output object.
   * `#finalizeParkedRunRelease` installs the retired attempt's authenticated
   * `run-aborted` publication here: a same-run successor replaces the run-id
   * record, and the retired output's terminal entry already settled at its
   * suspension, so this output-scoped entry is the only lookup that still
   * resolves to THIS attempt's publication after replacement.
   */
  #threadOutputParkedAbortDeliveries = new WeakMap<object, Promise<void>>();
  #eagerAbortListenersByStreamId = new Map<string, Set<() => void>>();

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

  #resolveLeaseProvider(pubsub?: PubSub): { provider: LeaseProvider; isFallback: boolean } {
    const provider = this.#getLeaseProvider(pubsub);
    return { provider, isFallback: provider === NoopLeaseProvider };
  }

  async #hasLiveThreadLease(pubsub: PubSub, key: string, runId: string, expectedOwner?: string): Promise<boolean> {
    const { provider, isFallback } = this.#resolveLeaseProvider(pubsub);
    if (isFallback) return true;
    return provider
      .getLeaseOwner(key)
      .then(owner =>
        expectedOwner !== undefined
          ? owner === expectedOwner && this.#runIdFromLeaseOwner(expectedOwner) === runId
          : owner !== undefined && this.#runIdFromLeaseOwner(owner) === runId,
      )
      .catch(() => false);
  }

  #getSourceId(): string {
    this.#id ??= globalThis.crypto.randomUUID();
    return this.#id;
  }

  /**
   * One bounded exact-owner reconciliation after a failed lease operation.
   *
   * A failed transfer/acquisition is not proof that nothing committed: the
   * provider may have moved the key before rejecting. This performs a single
   * `getLeaseOwner` read and matches it against the captured provenance
   * (candidate tokens and their run ids):
   *
   * - `'holder'` — the current owner verifiably matches one of the captured
   *   candidates; `holder` carries that exact run id and token.
   * - `'foreign'` — the current owner is a verified owner this attempt never
   *   held; `owner` is the raw current owner value.
   * - `'absent'` — the key is verifiably empty.
   * - `'unreadable'` — the read itself failed. Ownership stays unknown.
   */
  async #reconcileThreadLeaseHolder(
    pubsub: PubSub | undefined,
    key: string,
    candidates: Array<{ runId: string; token: string | undefined }>,
  ): Promise<
    | { status: 'holder'; holder: { runId: string; token: string | undefined } }
    | { status: 'foreign'; owner: string }
    | { status: 'absent' }
    | { status: 'unreadable' }
  > {
    let owner: string | undefined;
    let timedOut = false;
    try {
      // Bounded exact-owner read: the reconciliation must settle as
      // unreadable once the deadline passes — an unbounded provider await
      // would leave the failed operation's disposition pending forever, and a
      // read slower than the lease TTL can no longer authenticate any owner
      // worth releasing. The timeout is not an owner value: `timedOut`
      // distinguishes it from a verified empty key.
      let deadline: ReturnType<typeof setTimeout> | undefined;
      try {
        owner = await Promise.race([
          this.#getLeaseProvider(this.#getPubSub(pubsub)).getLeaseOwner(key),
          new Promise<undefined>(resolve => {
            deadline = setTimeout(() => {
              timedOut = true;
              resolve(undefined);
            }, AGENT_THREAD_LEASE_RECONCILE_TIMEOUT_MS);
          }),
        ]);
      } finally {
        if (deadline !== undefined) clearTimeout(deadline);
      }
    } catch {
      return { status: 'unreadable' };
    }
    if (timedOut) return { status: 'unreadable' };
    if (owner === undefined) return { status: 'absent' };
    for (const candidate of candidates) {
      if (candidate.token !== undefined && owner === candidate.token) {
        return { status: 'holder', holder: { runId: candidate.runId, token: candidate.token } };
      }
    }
    return { status: 'foreign', owner };
  }

  /**
   * Capture the immutable lease-ownership provenance for `runId` — its exact
   * retained token, renewal-timer identity and run record — synchronously,
   * before a lease operation's provider await. Callers refresh it with
   * {@link #refreshLeaseProvenance} once the operation settles so only
   * successors adopted DURING later awaits trip the fence.
   */
  #captureLeaseProvenance(
    state: AgentThreadRuntimeState,
    runId: string | undefined,
    token?: string,
  ): ThreadLeaseOperationProvenance | undefined {
    if (runId === undefined) return undefined;
    return {
      runId,
      token: token ?? state.leaseOwnerTokensByRunId.get(runId),
      timer: state.leaseRenewalTimers.get(runId),
      record: state.threadRunsById.get(runId),
    };
  }

  /**
   * Refresh the renewal-timer and run-record identities a settled lease
   * operation may itself have installed (its acquisition starts a renewal;
   * its own failed registration leaves its record). The captured token stays
   * the operation's candidate token.
   */
  #refreshLeaseProvenance(
    state: AgentThreadRuntimeState,
    provenance: ThreadLeaseOperationProvenance | undefined,
  ): void {
    if (!provenance) return;
    provenance.timer = state.leaseRenewalTimers.get(provenance.runId);
    provenance.record = state.threadRunsById.get(provenance.runId);
  }

  /**
   * Whether a successor adopted `provenance.runId` while its operation was
   * outstanding. A successor that adopted this run id replaced the token
   * and/or started its own renewal; one that re-adopted the RETAINED token
   * keeps token equality but still replaces the run record — so token
   * equality alone is insufficient and all three captured identities fence.
   * A record that is merely absent was trimmed, not adopted: known
   * foreign/absent state is never treated as a stale predecessor.
   */
  #leaseProvenanceSuperseded(state: AgentThreadRuntimeState, provenance: ThreadLeaseOperationProvenance): boolean {
    if (state.leaseOwnerTokensByRunId.get(provenance.runId) !== provenance.token) return true;
    if (state.leaseRenewalTimers.get(provenance.runId) !== provenance.timer) return true;
    const record = state.threadRunsById.get(provenance.runId);
    return record !== undefined && record !== provenance.record;
  }

  #leaseOwnerForRun(state: AgentThreadRuntimeState, runId: string): string {
    const retained = state.leaseOwnerTokensByRunId.get(runId);
    if (retained) return retained;
    // Lease providers intentionally treat reacquisition by the same owner as
    // idempotent. The public run id can itself be retry-stable, so it cannot be
    // the lease owner: two processes retrying that run id would both "win".
    // Keep the public correlation id inside a process-attempt-unique token.
    const owner = `${AGENT_THREAD_LEASE_OWNER_PREFIX}${JSON.stringify([runId, this.#getSourceId(), randomUUID()])}`;
    state.leaseOwnerTokensByRunId.set(runId, owner);
    return owner;
  }

  #runIdFromLeaseOwner(owner: string): string {
    if (!owner.startsWith(AGENT_THREAD_LEASE_OWNER_PREFIX)) return owner;
    try {
      const decoded = JSON.parse(owner.slice(AGENT_THREAD_LEASE_OWNER_PREFIX.length));
      if (Array.isArray(decoded) && typeof decoded[0] === 'string') return decoded[0];
    } catch {
      // Treat malformed/legacy owner values as opaque run ids.
    }
    return owner;
  }

  /**
   * Fire-and-forget release of the cross-process thread lease held by
   * this owner. Safe to call when no lease was ever acquired — the
   * pubsub's `releaseLease` is a no-op for non-owners (Lua-guarded
   * GET+DEL on Redis), and the default in-memory implementation is
   * identical. Also stops the renewal timer if one is running for
   * this run.
   */
  #releaseThreadLease(
    pubsub: PubSub | undefined,
    key: string,
    runId: string,
    provenance?: ThreadLeaseOperationProvenance,
  ): void {
    const resolved = this.#getPubSub(pubsub);
    const state = this.#getState(resolved);
    const leaseOwner = provenance?.token ?? state.leaseOwnerTokensByRunId.get(runId) ?? runId;
    this.#releaseThreadLeaseOwner(resolved, key, runId, leaseOwner, provenance).catch(error => {
      // Truthful failure evidence for the release: retained through the
      // existing error-record mechanism so later lookups for the same
      // cancelled attempt keep the abort message with this error as cause.
      this.#rememberRejectedRunError(state, runId, getErrorFromUnknown(error), { infrastructure: true });
    });
  }

  async #releaseThreadLeaseOwner(
    pubsub: PubSub,
    key: string,
    runId: string,
    leaseOwner: string,
    provenance?: ThreadLeaseOperationProvenance,
  ): Promise<void> {
    const state = this.#getState(pubsub);
    try {
      if (provenance && this.#leaseProvenanceSuperseded(state, provenance)) {
        // A successor adopted this run id while the operation was outstanding
        // — it may even have re-adopted the retained token, where token
        // equality cannot distinguish the attempts. Never revoke its token,
        // renewal timer or lease; the release is abandoned, not swallowed into
        // a success.
        return;
      }
      // Retain the captured token as provenance even when invalidating the
      // reusable map entry at committed release: post-await cleanup must never
      // remove a replacement token/timer a successor started meanwhile, so all
      // cleanup happens before the bounded exact-token release is awaited.
      if (state.leaseOwnerTokensByRunId.get(runId) === leaseOwner) {
        state.leaseOwnerTokensByRunId.delete(runId);
      }
      this.#stopLeaseRenewal(pubsub, runId, provenance?.timer);
      // Await the bounded exact-token result and preserve its rejection: a
      // swallowed provider failure must not masquerade as a successful
      // release. Callers decide how to retain the evidence.
      await this.#getLeaseProvider(pubsub).releaseLease(key, leaseOwner);
    } finally {
      // Relocated from the unreachable post-return tail; uses the actual
      // pubsub argument at this guarded boundary.
      this.#releaseUnusedThreadControlSubscription(state, key);
    }
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
    const leaseOwner = this.#leaseOwnerForRun(state, runId);
    const timer = setInterval(() => {
      void leaseProvider
        .renewLease(key, leaseOwner, AGENT_THREAD_LEASE_TTL_MS)
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

  #stopLeaseRenewal(
    pubsub: PubSub,
    runId: string,
    /**
     * Renewal-timer identity captured before the operation's provider await.
     * When provided, only that exact timer is stopped: a successor that
     * adopted the run id meanwhile owns the current timer, and stopping it
     * would silently kill the adopted lease's keep-alive.
     */
    expectedTimer?: ReturnType<typeof setInterval>,
  ): void {
    const state = this.#getState(pubsub);
    const timer = state.leaseRenewalTimers.get(runId);
    if (!timer) return;
    if (expectedTimer !== undefined && timer !== expectedTimer) return;
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
    failClosed = false,
  ): Promise<boolean> {
    const resolved = this.#getPubSub(pubsub);
    const state = this.#getState(resolved);
    const leaseProvider = this.#getLeaseProvider(resolved);
    // Operation provenance, read BEFORE the provider await: the predecessor's
    // exact token and renewal-timer identity. Post-await cleanup uses these
    // instead of rereading the mutable maps, so a successor installed for
    // either run id during the transfer — including one that re-adopts the
    // retained token — is never mistaken for this operation's attempt.
    const fromOwner = state.leaseOwnerTokensByRunId.get(fromRunId) ?? fromRunId;
    const fromTimer = state.leaseRenewalTimers.get(fromRunId);
    const toOwner = this.#leaseOwnerForRun(state, toRunId);
    // `transferLease` is a required `LeaseProvider` method. Atomic backends
    // (Redis, in-memory) swap the key gap-free; backends that can't be atomic
    // implement it as release+acquire internally and own that race cost.
    const transfer = leaseProvider.transferLease(key, fromOwner, toOwner, AGENT_THREAD_LEASE_TTL_MS);
    const held = failClosed ? await transfer : await transfer.catch(() => false);
    // Move the renewal timer to the new owner, fenced on the captured timer
    // identity: the old timer is owner-guarded and would only no-op now, the
    // new owner needs its own keep-alive for long drains, and a successor's
    // timer must survive this cleanup untouched.
    this.#stopLeaseRenewal(resolved, fromRunId, fromTimer);
    if (held) {
      if (state.leaseOwnerTokensByRunId.get(fromRunId) === fromOwner) {
        state.leaseOwnerTokensByRunId.delete(fromRunId);
      }
      this.#startLeaseRenewal(resolved, key, toRunId);
    } else {
      if (state.leaseOwnerTokensByRunId.get(toRunId) === toOwner) {
        state.leaseOwnerTokensByRunId.delete(toRunId);
      }
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
    options: { failClosed?: boolean } = {},
  ): Promise<{ acquired: boolean; owner?: string; ownerToken?: string; error?: unknown }> {
    const resolved = this.#getPubSub(pubsub);
    if (fromRunId) {
      const transferred = await this.#transferThreadLease(pubsub, key, fromRunId, toRunId, options.failClosed);
      if (transferred) return { acquired: true, owner: toRunId };
      // Old owner lost the lease before the handoff — fall through to acquire.
    }
    const leaseProvider = this.#getLeaseProvider(resolved);
    const state = this.#getState(resolved);
    const toOwner = this.#leaseOwnerForRun(state, toRunId);
    const acquisition = leaseProvider.acquireLease(key, toOwner, AGENT_THREAD_LEASE_TTL_MS);
    const result = options.failClosed
      ? await acquisition
      : await acquisition.catch((error: unknown) => ({
          acquired: false as boolean,
          owner: undefined as string | undefined,
          error,
        }));
    if (result.acquired) {
      this.#startLeaseRenewal(resolved, key, toRunId);
      return { acquired: true, owner: toRunId };
    }
    state.leaseOwnerTokensByRunId.delete(toRunId);
    return {
      acquired: false,
      owner: result.owner ? this.#runIdFromLeaseOwner(result.owner) : undefined,
      // The raw exact owner the provider reported, retained alongside the
      // decoded routing run id: the public run id alone is not lease
      // authority (a different attempt can reuse it), so verified-loss
      // forwarding must verify the exact token, never the decoded id.
      ownerToken: result.owner,
      error: (result as { error?: unknown }).error,
    };
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

  #ensureThreadControlSubscription(
    state: AgentThreadRuntimeState,
    pubsub: PubSub | undefined,
    key: string,
  ): ThreadControlSubscription {
    const ownedRunIds = new Set(
      [...state.threadKeysByRunId].filter(([, threadKey]) => threadKey === key).map(([runId]) => runId),
    );
    const existing = state.threadControlSubscriptions.get(key);
    if (existing) {
      for (const runId of ownedRunIds) existing.ownedRunIds.add(runId);
      return existing;
    }
    const resolvedPubSub = this.#getPubSub(pubsub);
    const topic = this.#threadTopic(key);
    let active = true;
    let tail = Promise.resolve();
    const handleEvent = async (event: Parameters<EventCallback>[0]) => {
      if (!active) return;
      const data = event.data as AgentThreadStreamRuntimeEvent | undefined;
      if (data?.type === 'signal-enqueued') {
        if (data.sourceId === this.#id || subscription.admittedSignalIds.has(data.signal.id)) return;
        const signal = createSignal(data.signal);
        // Keep predecessor routing through a handoff, but never promote
        // observer copies into execution.
        if (state.threadKeysByRunId.get(data.runId) !== key && !subscription.ownedRunIds.has(data.runId)) {
          return;
        }
        // This control subscription is the sole execution-queue admission
        // owner: the canonical stable-id/payload-conflict ledger decides once
        // for every delivery that reaches an execution queue this runtime
        // owns, so the same remote signal executes at most once no matter
        // which subscription (control or observer) a pubsub hands it to
        // first, and an id redelivered with a different payload is rejected
        // fail-closed rather than executing ambiguous input. A delivery
        // addressed to a run this runtime does not own is routing-rejected
        // above BEFORE the ledger: it never reached an execution queue, so it
        // must not consume the signal's stable-id admission — a verified-loss
        // forwarding handoff re-addresses that same signal to the run which
        // positively won the lease, and the winner admits it exactly once
        // through this same ledger.
        const disposition = this.#rememberSignalPayloadForRun(state, key, data.runId, signal);
        if (disposition.disposition !== 'accepted') return;
        const queues = data.preRun ? state.preRunSignalsByThread : state.pendingSignalsByThread;
        if (
          [state.preRunSignalsByThread.get(key), state.pendingSignalsByThread.get(key)].some(queue =>
            queue?.some(queued => queued.id === signal.id),
          )
        )
          return;
        const queue = queues.get(key) ?? [];
        queue.push(signal);
        queues.set(key, queue);
      } else if (data?.type === 'signals-cancelled') {
        // A backend retry can deliver the original enqueue after its cancellation.
        for (const id of data.signalIds) subscription.admittedSignalIds.add(id);
        this.#cancelPendingSignals(state, key, new Set(data.signalIds));
        this.#notifyThreadEvents(state);
      } else if (data?.type === 'run-abort-requested') {
        const ownsRun = () =>
          active &&
          (state.preparedRunsById.has(data.runId) || state.threadRunsById.has(data.runId)) &&
          state.threadKeysByRunId.get(data.runId) === key &&
          state.activeThreadRunIds.get(key) === data.runId &&
          state.activeThreadStreamIds.get(key) === data.streamId;
        // Fork contract: authenticate the request against the run's exact
        // owner token — a forged or transferred owner must not stop a run it
        // does not hold, and an unauthenticated request is rejected. The
        // remote abort path publishes with the provider's current owner.
        const runRecord = state.threadRunsByStreamId.get(data.streamId) ?? state.threadRunsById.get(data.runId);
        const authenticated =
          runRecord?.runId === data.runId &&
          data.leaseOwner === runRecord.leaseOwner &&
          (await this.#hasLiveThreadLease(resolvedPubSub, key, data.runId, runRecord.leaseOwner));
        if (ownsRun() && authenticated && ownsRun()) {
          if (data.clearPendingSignals) this.#cancelPendingSignals(state, key);
          if (state.preparedRunsById.has(data.runId) || this.#isParkedRun(state, data.runId)) {
            // A parked (suspended) run has no prepared run left, but abortRun's
            // parked branch releases it so the thread stops blocking on it.
            this.abortRun(data.runId, resolvedPubSub);
          } else {
            // Durable runs own their controller outside the thread runtime.
            state.threadRunsById.get(data.runId)?.agent.abortRunStream(data.runId);
          }
          if (data.clearPendingSignals) this.#notifyThreadEvents(state);
        }
      }
    };
    const onEvent: EventCallback = (event, ack) => {
      const type = (event.data as AgentThreadStreamRuntimeEvent | undefined)?.type;
      if (type !== 'signal-enqueued' && type !== 'signals-cancelled' && type !== 'run-abort-requested') return ack?.();
      // Finish acknowledging the delivery before an empty queue can release its listener.
      subscription.references++;
      const handled = tail.then(() => handleEvent(event));
      tail = handled.catch(() => {});
      return handled
        .then(() => ack?.())
        .finally(() => {
          subscription.references--;
          this.#releaseUnusedThreadControlSubscription(state, key);
        });
    };
    const subscription: ThreadControlSubscription = {
      ready: Promise.resolve(),
      references: 0,
      observers: 0,
      ownedRunIds,
      admittedSignalIds: new Set(),
      unsubscribe: () => {
        active = false;
        void subscription.ready.then(() => resolvedPubSub.unsubscribe(topic, onEvent)).catch(() => {});
      },
    };
    state.threadControlSubscriptions.set(key, subscription);
    subscription.ready = resolvedPubSub.subscribe(topic, onEvent).catch(error => {
      if (state.threadControlSubscriptions.get(key) === subscription) state.threadControlSubscriptions.delete(key);
      active = false;
      void resolvedPubSub.unsubscribe(topic, onEvent).catch(() => {});
      throw error;
    });
    // Synchronous queue APIs install immediately; their async execution/acceptance paths await readiness.
    void subscription.ready.catch(() => {});
    return subscription;
  }

  #releaseUnusedThreadControlSubscription(state: AgentThreadRuntimeState, key: string) {
    const subscription = state.threadControlSubscriptions.get(key);
    if (
      !subscription ||
      subscription.references > subscription.observers ||
      this.#hasPendingThreadWork(state, key) ||
      state.drainingPendingSignalsByThread.has(key) ||
      state.drainingIdleSignalsByThread.has(key) ||
      state.foreignWinnerHandoffsByThread.has(key) ||
      [...state.threadKeysByRunId.values()].includes(key) ||
      [...state.preparedRunsById.values()].some(run => run.threadKey === key)
    )
      return;
    subscription.ownedRunIds.clear();
    subscription.admittedSignalIds.clear();
    if (subscription.observers) return;
    state.threadControlSubscriptions.delete(key);
    subscription.unsubscribe();
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

  #registerThreadRegistrationListener(pubsub: PubSub, key: string, listener: ThreadRegistrationListener): () => void {
    let listenersByKey = this.#threadRegistrationListenersByPubSub.get(pubsub);
    if (!listenersByKey) {
      listenersByKey = new Map();
      this.#threadRegistrationListenersByPubSub.set(pubsub, listenersByKey);
    }
    let listeners = listenersByKey.get(key);
    if (!listeners) {
      listeners = new Set();
      listenersByKey.set(key, listeners);
    }
    listeners.add(listener);

    return () => {
      const currentByKey = this.#threadRegistrationListenersByPubSub.get(pubsub);
      const current = currentByKey?.get(key);
      if (!current?.delete(listener)) return;
      if (current.size === 0) currentByKey?.delete(key);
      if (currentByKey?.size === 0) this.#threadRegistrationListenersByPubSub.delete(pubsub);
    };
  }

  #queuedMessageCount(
    state: AgentThreadRuntimeState,
    scope: SubscribeAgentThreadEventsOptions & { agent: Agent<any, any, any, any> },
  ): number {
    const key = this.#threadKey(scope.resourceId, scope.threadId);
    const matches = (pending: PendingIdleSignal<any> | undefined) =>
      pending !== undefined &&
      !pending.cancelled &&
      (scope.queueOwnerId === undefined ||
        (pending.agent === scope.agent && pending.queueOwnerId === scope.queueOwnerId));
    return (
      (state.pendingIdleSignalsByThread.get(key)?.filter(matches).length ?? 0) +
      (matches(state.drainingIdleSignalsByThread.get(key)) ? 1 : 0)
    );
  }

  #notifyThreadEvents(state: AgentThreadRuntimeState): void {
    for (const registration of [...state.threadEventListeners]) {
      const count = this.#queuedMessageCount(state, registration);
      if (count === registration.lastCount) continue;
      registration.lastCount = count;
      try {
        registration.listener({ type: 'queue-count-changed', count });
      } catch {
        // One listener that throws must not break notification for the rest.
      }
    }
    for (const key of state.threadControlSubscriptions.keys()) this.#releaseUnusedThreadControlSubscription(state, key);
  }

  #threadKey(resourceId: string | undefined, threadId: string): string {
    return [resourceId ?? '', threadId].join(AGENT_THREAD_KEY_SEPARATOR);
  }

  #parseThreadKey(key: string): { resourceId?: string; threadId: string } {
    const separator = key.indexOf(AGENT_THREAD_KEY_SEPARATOR);
    const resourceId = key.slice(0, separator);
    return { resourceId: resourceId || undefined, threadId: key.slice(separator + AGENT_THREAD_KEY_SEPARATOR.length) };
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
      !!record.suspensions?.size ||
      this.#isSuspendedRun(state, record.runId)
    );
  }

  #isActivelyRunning(record: AgentThreadRunRecord<any>) {
    // Activity is derived from the current execution segment and the record lifecycle only.
    // Pending sibling suspensions legitimately coexist with an actively running resumed
    // segment (partial resume), so suspension bookkeeping must not make the run look idle.
    return (record.currentSegmentOutput ?? record.output).status === 'running' && record.lifecycle === 'running';
  }

  #serializeSignal(signal: CreatedAgentSignal): SerializableAgentSignal {
    return signal;
  }

  #nextStreamIdentity(state: AgentThreadRuntimeState, runId: string) {
    const streamSeq = (state.streamSeqByRunId.get(runId) ?? 0) + 1;
    state.streamSeqByRunId.set(runId, streamSeq);
    return { streamId: globalThis.crypto.randomUUID(), streamSeq };
  }

  #markRunSuspending(
    state: AgentThreadRuntimeState,
    runId: string,
    streamId: string,
    suspension: AgentThreadRunSuspension,
  ) {
    state.suspendedRunIds.add(runId);
    const suspensions = state.suspensionMetadataByRunId.get(runId) ?? new Map();
    suspensions.set(suspension.toolCallId, suspension);
    state.suspensionMetadataByRunId.set(runId, suspensions);
    const record = state.threadRunsByStreamId.get(streamId) ?? state.threadRunsById.get(runId);
    if (record) {
      record.lifecycle = 'suspending';
      record.suspensions = suspensions;
    }
    if (suspension.kind === 'approval') {
      state.approvalSuspendedRunIds.add(runId);
    }
  }

  #clearSuspendedRun(state: AgentThreadRuntimeState, runId: string) {
    state.suspendedRunIds.delete(runId);
    state.suspensionMetadataByRunId.delete(runId);
    state.approvalSuspendedRunIds.delete(runId);
    const record = state.threadRunsById.get(runId);
    if (record) {
      record.suspensions = undefined;
    }
  }

  #clearSuspendedToolCall(state: AgentThreadRuntimeState, runId: string, toolCallId: string) {
    const suspensions = state.suspensionMetadataByRunId.get(runId);
    suspensions?.delete(toolCallId);

    if (suspensions?.size) {
      if (![...suspensions.values()].some(suspension => suspension.kind === 'approval')) {
        state.approvalSuspendedRunIds.delete(runId);
      }
      return;
    }

    this.#clearSuspendedRun(state, runId);
  }

  #generateSignalMessageId(
    agent: Agent<any, any, any, any>,
    target: { threadId?: string; resourceId?: string },
  ): string {
    return (
      agent.getMastraInstance?.()?.generateId({
        idType: 'message',
        source: 'agent',
        entityId: agent.id,
        threadId: target.threadId,
        resourceId: target.resourceId,
      }) ?? globalThis.crypto.randomUUID()
    );
  }

  #createMessageSignalInput(message: AgentMessageInput): AgentSignal {
    const normalizedMessage = typeof message === 'string' || Array.isArray(message) ? { contents: message } : message;
    return {
      ...normalizedMessage,
      type: 'user',
      tagName: 'user',
    };
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

  async claimThreadOwnership<OUTPUT = unknown>(
    agent: Agent<any, any, any, any>,
    options: {
      resourceId: string;
      threadId: string;
      streamOptions?: ClaimedThreadOwnerStreamOptions;
      peer?: false | AgentClaimThreadPeerOptions;
      yieldOwnership?: () => boolean;
      onOwnershipYielded?: () => void;
      onOwnershipLost?: () => void;
    },
    pubsub?: PubSub,
  ): Promise<{ claimed: boolean; unsubscribe: () => void }> {
    const resolvedPubSub = this.#getPubSub(pubsub);
    const state = this.#getState(resolvedPubSub);
    const key = this.#threadKey(options.resourceId, options.threadId);
    const topic = this.#threadTopic(key);
    const sourceId = this.#getSourceId();
    const initialLocalClaim = state.claimedThreadOwners.get(key);
    const claimLeaseKey = `${AGENT_THREAD_CLAIM_LEASE_PREFIX}${key}`;
    const { provider: leaseProvider, isFallback: isLeaseFallback } = this.#resolveLeaseProvider(resolvedPubSub);

    if (isLeaseFallback) {
      if (!initialLocalClaim) {
        const remoteOwnerSourceId = await this.#findClaimedThreadOwner(resolvedPubSub, key, {
          includeLocal: false,
          intent: 'claim',
        });
        if (remoteOwnerSourceId) {
          return { claimed: false, unsubscribe: () => {} };
        }
      }
    } else {
      let lease = await leaseProvider.acquireLease(claimLeaseKey, sourceId, AGENT_THREAD_LEASE_TTL_MS);
      if (!lease.acquired) {
        await this.#findClaimedThreadOwner(resolvedPubSub, key, {
          includeLocal: false,
          intent: 'claim',
          targetSourceId: lease.owner,
        });
        lease = await leaseProvider.acquireLease(claimLeaseKey, sourceId, AGENT_THREAD_LEASE_TTL_MS);
      }
      if (!lease.acquired) {
        return { claimed: false, unsubscribe: () => {} };
      }
    }

    const peerOptions = options.peer === false ? undefined : (options.peer ?? {});
    const peerAgentId = peerOptions?.agentId ?? agent.id;
    const peer: AdvertisedThreadPeer | undefined = peerOptions
      ? {
          id: peerOptions.id ?? createThreadPeerId(peerAgentId, options.resourceId, options.threadId),
          agentId: peerAgentId,
          resourceId: options.resourceId,
          threadId: options.threadId,
          ...(peerOptions.label ? { label: peerOptions.label } : {}),
          ...(peerOptions.title ? { title: peerOptions.title } : {}),
          ...(peerOptions.metadata ? { metadata: peerOptions.metadata } : {}),
          sourceId,
          unsubscribe: () => {},
        }
      : undefined;

    let active = false;

    const onEvent: EventCallback = async (event, ack) => {
      if (!active) {
        await ack?.();
        return;
      }
      const data = event.data as AgentThreadStreamRuntimeEvent | undefined;
      if (data?.type !== 'idle-signal-enqueued' || data.sourceId === sourceId || data.targetSourceId !== sourceId) {
        await ack?.();
        return;
      }
      // The caller's deadline, normalized for a legacy sender (a rolling-deploy
      // peer on an older core) that published a relative `timeoutMs` instead of
      // an absolute `expiresAt`. Deriving at receipt is exactly what that
      // sender expected; without a deadline at all the checks degrade to the
      // previous always-respond behavior.
      const expiresAt =
        data.expiresAt ?? (data.timeoutMs === undefined ? Number.POSITIVE_INFINITY : Date.now() + data.timeoutMs);
      // The caller already timed out on this request (it arrived via backlog
      // replay or redelivery). Starting a run now would duplicate work the
      // caller reported as failed, and the reply would recreate its released
      // reply stream. Returning still acks: a stale request must not redeliver.
      if (isStaleRequest(expiresAt)) {
        await ack?.();
        return;
      }
      const owner = state.claimedThreadOwners.get(key);
      if (!owner) {
        await ack?.();
        return;
      }

      const handled = state.handledIdleSignals.get(data.requestId);
      if (handled) {
        // Backends deliver at least once. This handler starts a run or queues the
        // signal onto one, and neither is idempotent, so the repeat must not touch
        // the signal again. What it still owes the caller is the reply: without
        // it the caller waits out its acceptance timeout and reports that no
        // owner accepted, when a run is in fact already in flight.
        if (handled.reply && !handled.replyPublished) {
          const reply = handled.reply;
          await resolvedPubSub.publish(handled.replyTopic, {
            type: reply.type,
            runId: reply.runId,
            data: reply,
          });
          handled.replyPublished = true;
        }
        await ack?.();
        return;
      }
      const handledSignal: HandledIdleSignal = { replyTopic: data.replyTopic, replyPublished: false };
      state.handledIdleSignals.set(data.requestId, handledSignal);

      let replyAttempted = false;
      const reply = async (response: AgentThreadIdleSignalAcceptanceEvent) => {
        if (replyAttempted) return;
        replyAttempted = true;
        // Record the reply before the publish settles, so a repeat can re-send the
        // same answer if this attempt never lands. A duplicate reply is harmless:
        // the caller settles on the first one it sees.
        handledSignal.reply = response;
        await resolvedPubSub.publish(handledSignal.replyTopic, {
          type: response.type,
          runId: response.runId,
          data: response,
        });
        handledSignal.replyPublished = true;
      };

      try {
        const accepted = await this.#startClaimedIdleRun(
          state,
          resolvedPubSub,
          key,
          owner,
          data.runId,
          createSignal(data.signal),
          // The caller's deadline translated onto this process's clock. The
          // deadline crossed machines, so the strict `Date.now() >= expiresAt`
          // checks inside #startClaimedIdleRun would reject live requests
          // whenever this clock runs ahead of the caller's; the same skew
          // grace `isStaleRequest` applies covers that. The cap keeps a
          // request that arrived late (but inside the grace) or a slow local
          // clock from granting more than one fresh acceptance window —
          // deriving an uncapped fresh window at receive time is exactly the
          // bug that let arbitrarily old replayed requests start runs.
          Math.min(
            expiresAt + AGENT_REQUEST_EXPIRY_SKEW_GRACE_MS,
            Date.now() + AGENT_THREAD_OWNER_ACCEPTANCE_TIMEOUT_MS,
          ),
          () => active && state.claimedThreadOwners.get(key)?.unsubscribe === unsubscribe,
        );
        if (!active || state.claimedThreadOwners.get(key)?.unsubscribe !== unsubscribe) {
          await ack?.();
          return;
        }
        if (!accepted) {
          await reply({
            type: 'idle-signal-rejected',
            requestId: data.requestId,
            runId: data.runId,
            sourceId,
            error: `Claimed thread owner could not acquire the execution lease for ${key}`,
            preAdmission: true,
          });
        } else if (accepted.error) {
          await reply({
            type: 'idle-signal-rejected',
            requestId: data.requestId,
            runId: accepted.runId,
            sourceId,
            error: accepted.error,
            ...(accepted.preAdmission ? { preAdmission: true } : {}),
          });
        } else {
          await reply({
            type: 'idle-signal-accepted',
            requestId: data.requestId,
            runId: accepted.runId,
            sourceId,
          });
        }
      } catch (error) {
        if (!active || state.claimedThreadOwners.get(key)?.unsubscribe !== unsubscribe) {
          await ack?.();
          return;
        }
        if (replyAttempted) {
          // The reply never reached the backend, so the caller cannot learn the
          // signal was accepted. Leave the delivery unacked: the backend
          // redelivers, and the repeat re-sends the remembered reply instead of
          // acting on the signal again.
          throw error;
        }
        await reply({
          type: 'idle-signal-rejected',
          requestId: data.requestId,
          runId: data.runId,
          sourceId,
          error: getErrorFromUnknown(error).message,
          ...(error instanceof ClaimedOwnerPreAdmissionError ? { preAdmission: true } : {}),
        });
      }
      await ack?.();
    };

    // Both discovery handlers await their reply before returning, so the request is only
    // acked once the reply publish has settled. Acking first would let a failed publish lose
    // the request — the backend sees it as handled and cannot redeliver, leaving the caller's
    // `#deliverAfterClaimedOwnerDiscovery` to throw with no response. Awaiting keeps the ack
    // behind the publish and lets a rejection reach the nack path, matching `onEvent`.
    const onOwnerDiscovery: EventCallback = withAck(async event => {
      if (!active) return;
      const data = event.data as AgentThreadOwnerDiscoveryEvent | undefined;
      if (
        data?.type !== 'thread-owner-request' ||
        data.key !== key ||
        data.sourceId === sourceId ||
        (data.targetSourceId !== undefined && data.targetSourceId !== sourceId)
      ) {
        return;
      }
      // A stale request's caller has timed out and released its reply topic;
      // replying would recreate the stream on persistent backends. Fan-out
      // groups anchor at the stream start, so a fresh claimant replays the
      // whole discovery backlog — without this guard it would answer every
      // request ever retained.
      if (isStaleRequest(data.expiresAt)) return;
      // Yield-on-demand: a requester that wants to own this thread asks first.
      // Lease-backed transports hand the claim over atomically before this
      // owner unsubscribes. Lease-less transports preserve the legacy silent
      // release, allowing the requester's discovery to settle without an owner.
      const requesterStillWaiting = data.expiresAt === undefined || Date.now() < data.expiresAt;
      if (data.intent === 'claim' && requesterStillWaiting && options.yieldOwnership?.()) {
        if (!isLeaseFallback) {
          const transferred = await leaseProvider.transferLease(
            claimLeaseKey,
            sourceId,
            data.sourceId,
            AGENT_THREAD_LEASE_TTL_MS,
          );
          if (!transferred) return;
          unsubscribe();
          options.onOwnershipYielded?.();
          await resolvedPubSub.publish(data.replyTopic, {
            type: 'thread-owner-response',
            runId: data.requestId,
            data: { type: 'thread-owner-response', key, requestId: data.requestId, sourceId },
          });
          return;
        }
        unsubscribe();
        options.onOwnershipYielded?.();
        return;
      }
      await resolvedPubSub.publish(data.replyTopic, {
        type: 'thread-owner-response',
        runId: data.requestId,
        data: { type: 'thread-owner-response', key, requestId: data.requestId, sourceId },
      });
    });

    const onPeerDiscovery: EventCallback = withAck(async event => {
      if (!active || !peer) return;
      const data = event.data as AgentThreadPeerDiscoveryEvent | undefined;
      if (data?.type !== 'thread-peer-request' || data.sourceId === sourceId) return;
      if (isStaleRequest(data.expiresAt)) return; // see onOwnerDiscovery
      await resolvedPubSub.publish(data.replyTopic, {
        type: 'thread-peer-response',
        runId: data.requestId,
        data: { type: 'thread-peer-response', requestId: data.requestId, peer: toPublicThreadPeer(peer), sourceId },
      });
    });

    let threadSubscribed = false;
    let ownerDiscoverySubscribed = false;
    let peerDiscoverySubscribed = false;
    try {
      await resolvedPubSub.subscribe(topic, onEvent);
      threadSubscribed = true;
      await resolvedPubSub.subscribe(AGENT_THREAD_OWNER_DISCOVERY_TOPIC, onOwnerDiscovery);
      ownerDiscoverySubscribed = true;
      if (peer) {
        await resolvedPubSub.subscribe(AGENT_THREAD_PEER_DISCOVERY_TOPIC, onPeerDiscovery);
        peerDiscoverySubscribed = true;
      }
    } catch (error) {
      await Promise.all([
        threadSubscribed ? resolvedPubSub.unsubscribe(topic, onEvent).catch(() => {}) : Promise.resolve(),
        ownerDiscoverySubscribed
          ? resolvedPubSub.unsubscribe(AGENT_THREAD_OWNER_DISCOVERY_TOPIC, onOwnerDiscovery).catch(() => {})
          : Promise.resolve(),
        peerDiscoverySubscribed
          ? resolvedPubSub.unsubscribe(AGENT_THREAD_PEER_DISCOVERY_TOPIC, onPeerDiscovery).catch(() => {})
          : Promise.resolve(),
        !isLeaseFallback && !initialLocalClaim
          ? leaseProvider.releaseLease(claimLeaseKey, sourceId).catch(() => {})
          : Promise.resolve(),
      ]);
      throw error;
    }

    let claimLeaseRenewalTimer: ReturnType<typeof setInterval> | undefined;
    const unsubscribe = () => {
      active = false;
      if (claimLeaseRenewalTimer) {
        clearInterval(claimLeaseRenewalTimer);
        claimLeaseRenewalTimer = undefined;
      }
      const currentOwner = state.claimedThreadOwners.get(key);
      if (currentOwner?.unsubscribe === unsubscribe) {
        state.claimedThreadOwners.delete(key);
        if (!isLeaseFallback) void leaseProvider.releaseLease(claimLeaseKey, sourceId).catch(() => {});
      }
      if (peer && state.advertisedThreadPeers.get(peer.id)?.unsubscribe === unsubscribe) {
        state.advertisedThreadPeers.delete(peer.id);
      }
      void resolvedPubSub.unsubscribe(topic, onEvent).catch(() => {});
      void resolvedPubSub.unsubscribe(AGENT_THREAD_OWNER_DISCOVERY_TOPIC, onOwnerDiscovery).catch(() => {});
      if (peer) {
        void resolvedPubSub.unsubscribe(AGENT_THREAD_PEER_DISCOVERY_TOPIC, onPeerDiscovery).catch(() => {});
      }
    };

    const displacedClaim = state.claimedThreadOwners.get(key);
    const displacedPeer = peer ? state.advertisedThreadPeers.get(peer.id) : undefined;
    if (peer) {
      peer.unsubscribe = unsubscribe;
      state.advertisedThreadPeers.set(peer.id, peer);
    }

    let claimLeaseRenewalInFlight = false;
    if (!isLeaseFallback) {
      claimLeaseRenewalTimer = setInterval(() => {
        if (claimLeaseRenewalInFlight) return;
        claimLeaseRenewalInFlight = true;
        void leaseProvider
          .renewLease(claimLeaseKey, sourceId, AGENT_THREAD_LEASE_TTL_MS)
          .then(async renewed => {
            if (renewed || !active || state.claimedThreadOwners.get(key)?.unsubscribe !== unsubscribe) return;
            const reacquired = await leaseProvider.acquireLease(claimLeaseKey, sourceId, AGENT_THREAD_LEASE_TTL_MS);
            if (reacquired.acquired || !active || state.claimedThreadOwners.get(key)?.unsubscribe !== unsubscribe)
              return;
            unsubscribe();
            options.onOwnershipLost?.();
          })
          .catch(() => {})
          .finally(() => {
            claimLeaseRenewalInFlight = false;
          });
      }, AGENT_THREAD_LEASE_RENEW_INTERVAL_MS);
      claimLeaseRenewalTimer.unref?.();
    }
    state.claimedThreadOwners.set(key, {
      agent,
      resourceId: options.resourceId,
      threadId: options.threadId,
      streamOptions: options.streamOptions,
      peer,
      unsubscribe,
    });
    active = true;

    displacedClaim?.unsubscribe();
    if (displacedPeer?.unsubscribe !== displacedClaim?.unsubscribe) displacedPeer?.unsubscribe();

    return { claimed: true, unsubscribe };
  }

  updateThreadPeerAdvertisement(
    agent: Agent<any, any, any, any>,
    options: { resourceId: string; threadId: string; peer: AgentUpdateThreadPeerOptions },
    pubsub?: PubSub,
  ): boolean {
    const state = this.#getState(this.#getPubSub(pubsub));
    const key = this.#threadKey(options.resourceId, options.threadId);
    const owner = state.claimedThreadOwners.get(key);
    if (!owner?.peer || owner.agent.id !== agent.id) return false;

    const peer = owner.peer;
    if (Object.hasOwn(options.peer, 'label')) peer.label = options.peer.label;
    if (Object.hasOwn(options.peer, 'title')) peer.title = options.peer.title;
    if (Object.hasOwn(options.peer, 'metadata')) peer.metadata = options.peer.metadata;
    return true;
  }

  async discoverThreadPeers(
    options: DiscoverAgentThreadPeersOptions = {},
    pubsub?: PubSub,
    callerAgent?: Agent<any, any, any, any>,
  ): Promise<AgentThreadPeerAdvertisement[]> {
    const resolvedPubSub = this.#getPubSub(pubsub);
    const state = this.#getState(resolvedPubSub);
    const requestId = globalThis.crypto.randomUUID();
    const replyTopic = `${AGENT_THREAD_PEER_DISCOVERY_TOPIC}.${requestId}`;
    const peers = new Map<string, AgentThreadPeerAdvertisement>();
    const discoveredAt = new Date();

    for (const peer of state.advertisedThreadPeers.values()) {
      peers.set(peer.id, { ...toPublicThreadPeer(peer), discoveredAt });
    }

    await new Promise<void>(resolve => {
      let settled = false;
      const finish = () => {
        if (settled) return;
        settled = true;
        clearTimeout(timeout);
        resolve();
        releaseReplyTopic(resolvedPubSub, replyTopic, onReply);
      };
      const onReply: EventCallback = withAck(event => {
        const data = event.data as AgentThreadPeerDiscoveryEvent | undefined;
        if (data?.type !== 'thread-peer-response' || data.requestId !== requestId) return;
        peers.set(data.peer.id, { ...data.peer, sourceId: data.sourceId, discoveredAt: new Date() });
      });
      const timeoutMs = options.timeoutMs ?? AGENT_THREAD_PEER_DISCOVERY_TIMEOUT_MS;
      // Absolute deadline carried on the request so a responder that receives
      // it late (backlog replay, redelivery) can drop it instead of replying
      // into the released reply topic.
      const expiresAt = Date.now() + timeoutMs;
      const timeout = setTimeout(finish, timeoutMs);

      void resolvedPubSub
        .subscribe(replyTopic, onReply)
        .then(() => {
          // If the timeout already settled the request, the reply topic has
          // been released. Publishing now would invite replies that recreate
          // it, and the subscribe that just finished may have attached the
          // callback after the release — so release again instead.
          if (settled) {
            releaseReplyTopic(resolvedPubSub, replyTopic, onReply);
            return;
          }
          return resolvedPubSub.publish(AGENT_THREAD_PEER_DISCOVERY_TOPIC, {
            type: 'thread-peer-request',
            runId: requestId,
            data: { type: 'thread-peer-request', requestId, replyTopic, sourceId: this.#getSourceId(), expiresAt },
          });
        })
        .catch(() => finish());
    });

    // The mark is applied after every pass that can produce an entry: a reply can
    // describe a thread this caller already owns — a second live instance with the
    // same thread loaded answers discovery too, and its reply replaces the local
    // entry. The runtime is shared by every agent in the process, so the mark is
    // scoped to the claiming agent: a sibling agent's claim stays a real peer.
    if (callerAgent !== undefined) {
      for (const [id, peer] of peers) {
        const owner = state.claimedThreadOwners.get(this.#threadKey(peer.resourceId, peer.threadId));
        if (owner?.agent === callerAgent) peers.set(id, { ...peer, selfAdvertised: true });
      }
    }

    return [...peers.values()].sort((a, b) => a.id.localeCompare(b.id));
  }

  async #startClaimedIdleRun<OUTPUT>(
    state: AgentThreadRuntimeState,
    pubsub: PubSub,
    key: string,
    owner: ClaimedThreadOwner<OUTPUT>,
    runId: string,
    signal: CreatedAgentSignal,
    expiresAt: number,
    isOwnerActive: () => boolean,
    finishingRunId?: string,
    incomingStreamOptions?: AgentExecutionOptions<any>,
  ): Promise<ClaimedThreadOwnerStartResult<OUTPUT> | undefined> {
    const inspectRunIdentity = (): {
      activeRunId: string | undefined;
      activeRecord: AgentThreadRunRecord<any> | undefined;
      activeRunIsBlocking: boolean;
      error?: ClaimedThreadOwnerStartResult<OUTPUT>;
    } => {
      const activeRunId = state.activeThreadRunIds.get(key);
      const activeRecord = activeRunId ? state.threadRunsById.get(activeRunId) : undefined;
      const activeRunIsBlocking = Boolean(
        activeRunId && (!activeRecord || this.#isThreadBlockingRun(state, activeRecord)),
      );
      const existingRun = state.threadRunsById.get(runId);
      const knownRunKeys = new Set<string>();
      if (existingRun) {
        knownRunKeys.add(this.#threadKey(existingRun.resourceId, existingRun.threadId));
      }
      const reservedKeys = [
        state.threadKeysByRunId.get(runId),
        state.pendingIdleThreadKeysByRunId.get(runId),
        state.inflightIdleThreadKeysByRunId.get(runId),
      ];
      for (const existingKey of reservedKeys) {
        if (existingKey) knownRunKeys.add(existingKey);
      }
      const foreignIdentity = [...knownRunKeys].some(existingKey => existingKey !== key);
      const sameKeyIdentity =
        activeRunId === runId || knownRunKeys.has(key) || reservedKeys.some(existingKey => existingKey === key);
      let error: ClaimedThreadOwnerStartResult<OUTPUT> | undefined;
      if (foreignIdentity) {
        error = {
          runId,
          error: `Agent thread run id "${runId}" is already reserved for another thread`,
          preAdmission: true,
        };
      } else if (existingRun) {
        error = { runId, error: `Agent thread run id "${runId}" is already registered` };
      } else if (sameKeyIdentity) {
        error = { runId, error: `Agent thread run id "${runId}" is already reserved` };
      }
      return { activeRunId, activeRecord, activeRunIsBlocking, error };
    };

    const initialIdentity = inspectRunIdentity();
    if (initialIdentity.error) return initialIdentity.error;
    if (!isOwnerActive()) {
      return { runId, error: `Claimed thread owner was released for ${key}`, preAdmission: true };
    }
    if (Date.now() >= expiresAt) {
      return { runId, error: `Claimed thread owner acceptance expired for ${key}`, preAdmission: true };
    }
    let ownerStreamOptions: AgentExecutionOptions<any> | undefined;
    try {
      ownerStreamOptions =
        typeof owner.streamOptions === 'function' ? await owner.streamOptions() : owner.streamOptions;
    } catch (error) {
      const identityAfterOptionsFailure = inspectRunIdentity();
      if (identityAfterOptionsFailure.error) return identityAfterOptionsFailure.error;
      throw new ClaimedOwnerPreAdmissionError(getErrorFromUnknown(error).message, error);
    }
    // The run executes inside the claiming owner's session, so the owner's
    // options stay authoritative for everything that shapes the run — memory,
    // toolsets, provider options. Only `requestContext` crosses over: it
    // identifies the caller this wake acts for rather than the shape of the run.
    // A dispatcher waking a session on behalf of an authenticated caller needs
    // that identity visible downstream, or the run starts anonymously and a
    // workspace resolver that requires a caller rejects it.
    const streamOptions: AgentExecutionOptions<any> | undefined =
      incomingStreamOptions?.requestContext === undefined
        ? ownerStreamOptions
        : { ...ownerStreamOptions, requestContext: incomingStreamOptions.requestContext };
    const identityAfterOptions = inspectRunIdentity();
    if (identityAfterOptions.error) return identityAfterOptions.error;
    const { activeRunId, activeRecord, activeRunIsBlocking } = identityAfterOptions;
    const control = this.#ensureThreadControlSubscription(state, pubsub, key);
    control.references++;
    try {
      await control.ready;
      if (!isOwnerActive()) {
        return { runId, error: `Claimed thread owner was released for ${key}`, preAdmission: true };
      }
      if (Date.now() >= expiresAt) {
        return { runId, error: `Claimed thread owner acceptance expired for ${key}`, preAdmission: true };
      }

      if (activeRunId && activeRunIsBlocking) {
        const idleQueue = state.pendingIdleSignalsByThread.get(key) ?? [];
        idleQueue.push({
          agent: owner.agent,
          signal,
          runId,
          resourceId: owner.resourceId,
          threadId: owner.threadId,
          streamOptions,
        });
        state.pendingIdleSignalsByThread.set(key, idleQueue);
        state.pendingIdleThreadKeysByRunId.set(runId, key);
        if (activeRecord) {
          void this.#watchThreadRunCompletion(state, pubsub, key, activeRecord);
        }
        // No run starts here — the signal joins the in-flight run and is drained
        // later. Returning without `output` is what tells the caller this queued
        // instead of running, so it reports `deliver` rather than `wake`.
        return { runId };
      }
      const finishingRunToTransfer =
        finishingRunId ??
        (activeRunId &&
        activeRecord &&
        !this.#isThreadBlockingRun(state, activeRecord) &&
        state.threadKeysByRunId.get(activeRunId) === key
          ? activeRunId
          : undefined);
      if (activeRunId) {
        state.activeThreadRunIds.delete(key);
      }

      if (!isOwnerActive()) {
        return { runId, error: `Claimed thread owner was released for ${key}`, preAdmission: true };
      }
      state.activeThreadRunIds.set(key, runId);
      state.threadKeysByRunId.set(runId, key);
      state.reservedAgentIdsByRunId.set(runId, owner.agent.id);
      const lease = await this.#acquireOrTransferThreadLease(pubsub, key, runId, finishingRunToTransfer);
      const ownerActive = isOwnerActive();
      const expired = Date.now() >= expiresAt;
      if (!lease.acquired || !ownerActive || expired) {
        state.activeThreadRunIds.delete(key);
        state.threadKeysByRunId.delete(runId);
        state.reservedAgentIdsByRunId.delete(runId);
        if (!ownerActive || expired) {
          const drained = await this.#drainPendingIdleSignals(state, pubsub, key, lease.acquired ? runId : undefined);
          if (lease.acquired && !drained) this.#releaseThreadLease(pubsub, key, runId);
          if (!ownerActive) return { runId, error: `Claimed thread owner was released for ${key}`, preAdmission: true };
          return { runId, error: `Claimed thread owner acceptance expired for ${key}`, preAdmission: true };
        }
        if (lease.owner) {
          this.#publish(pubsub, key, {
            type: 'signal-enqueued',
            runId: lease.owner,
            signal: this.#serializeSignal(signal),
            sourceId: this.#getSourceId(),
          });
          return { runId: lease.owner };
        }
        await this.#drainPendingIdleSignals(state, pubsub, key);
        return undefined;
      }

      try {
        const output = await owner.agent.stream(signal, {
          ...(streamOptions as any),
          _threadRunReservationOwner: true,
          runId,
          memory: withThreadMemory(streamOptions?.memory, owner.resourceId, owner.threadId),
        });
        return { runId, output };
      } catch (error) {
        const message = getErrorFromUnknown(error).message;
        state.threadKeysByRunId.delete(runId);
        state.reservedAgentIdsByRunId.delete(runId);
        this.#cleanupPreparedRun(state, runId);
        if (state.activeThreadRunIds.get(key) === runId) {
          state.activeThreadRunIds.delete(key);
        }
        this.#publish(pubsub, key, {
          type: 'run-failed',
          runId,
          error: message,
        });
        this.#trimFailedRun(pubsub, key, { agent: owner.agent, streamOptions: streamOptions ?? {}, runId });
        if (!(await this.#drainPendingIdleSignals(state, pubsub, key, runId))) {
          this.#releaseThreadLease(pubsub, key, runId);
        }
        return { runId, error: message };
      }
    } finally {
      control.references--;
      this.#releaseUnusedThreadControlSubscription(state, key);
    }
  }

  #publish(pubsub: PubSub | undefined, key: string, event: AgentThreadStreamRuntimeEvent) {
    void this.#publishAndWait(pubsub, key, event).catch(() => {});
  }

  async #publishAndWait(pubsub: PubSub | undefined, key: string, event: AgentThreadStreamRuntimeEvent) {
    const resolvedPubSub = this.#getPubSub(pubsub);
    const topic = this.#threadTopic(key);
    const registration = event.type === 'run-registered' ? event : undefined;
    let trustedRecord: AgentThreadRunRecord<any> | undefined;
    let registrationListeners: ThreadRegistrationListener[] = [];
    if (registration) {
      const state = this.#getState(resolvedPubSub);
      const candidate = state.threadRunsByStreamId.get(registration.streamId);
      if (
        candidate &&
        state.threadRunsById.get(registration.runId) === candidate &&
        state.threadKeysByRunId.get(registration.runId) === key &&
        this.#threadKey(candidate.resourceId, candidate.threadId) === key &&
        candidate.runId === registration.runId &&
        candidate.streamId === registration.streamId &&
        candidate.streamSeq === registration.streamSeq &&
        candidate.leaseOwner === registration.leaseOwner
      ) {
        trustedRecord = candidate;
        registrationListeners = [...(this.#threadRegistrationListenersByPubSub.get(resolvedPubSub)?.get(key) ?? [])];
      }
    }
    await resolvedPubSub.publish(topic, {
      type: event.type,
      // Thread-scoped control events use the thread key as their envelope correlation ID.
      runId: 'runId' in event ? event.runId : key,
      data: event,
    });
    if (registration && trustedRecord) {
      for (const listener of registrationListeners) {
        try {
          listener(registration, trustedRecord);
        } catch {
          // Private admission delivery must not turn a successful publication into a failure.
        }
      }
    }
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
    event: Extract<
      AgentThreadStreamRuntimeEvent,
      { type: 'run-aborting' | 'run-aborted' | 'run-completed' | 'run-suspended' | 'run-failed' }
    >,
  ): Promise<void> {
    const state = this.#getState(pubsub);
    const leaseOwner =
      event.leaseOwner ??
      state.threadRunsById.get(event.runId)?.leaseOwner ??
      state.leaseOwnerTokensByRunId.get(event.runId);
    const authenticatedEvent = leaseOwner ? { ...event, leaseOwner } : event;
    try {
      await waitWithTimeout(
        this.#publishAndWait(pubsub, key, authenticatedEvent),
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

  async #deliverToClaimedThreadOwner(
    pubsub: PubSub,
    key: string,
    runId: string,
    signal: CreatedAgentSignal,
    targetSourceId: string,
  ): Promise<string> {
    const requestId = globalThis.crypto.randomUUID();
    const replyTopic = `${this.#threadTopic(key)}.idle-acceptance.${requestId}`;

    return new Promise<string>((resolve, reject) => {
      let settled = false;
      const finish = (result: { runId: string } | { error: Error }) => {
        if (settled) return;
        settled = true;
        clearTimeout(timeout);
        if ('error' in result) reject(result.error);
        else resolve(result.runId);
        releaseReplyTopic(pubsub, replyTopic, onReply);
      };
      const onReply: EventCallback = withAck(event => {
        const data = event.data as AgentThreadIdleSignalAcceptanceEvent | undefined;
        if (!data || data.requestId !== requestId || data.sourceId !== targetSourceId) return;
        if (data.type === 'idle-signal-rejected') {
          finish({
            error: data.preAdmission ? new ClaimedOwnerPreAdmissionError(data.error) : new Error(data.error),
          });
          return;
        }
        if (data.type === 'idle-signal-accepted') {
          finish({ runId: data.runId });
        }
      });
      // Absolute deadline carried on the request so a responder that receives
      // it late (backlog replay, redelivery) drops it instead of starting a run
      // this caller has already reported as timed out.
      const expiresAt = Date.now() + AGENT_THREAD_OWNER_ACCEPTANCE_TIMEOUT_MS;
      const timeout = setTimeout(
        () => finish({ error: new Error(`Claimed thread owner did not accept signal for ${key}`) }),
        AGENT_THREAD_OWNER_ACCEPTANCE_TIMEOUT_MS,
      );

      // This event can reach a claimed owner in another process. It carries the
      // signal, the request/reply ids and the timeout — not `requestContext`,
      // which is an open map that may hold non-serializable values and has no
      // wire contract. A remote owner therefore starts the run with its own
      // options, so the caller-context handling in `#startClaimedIdleRun` covers
      // a locally claimed owner only.
      void pubsub
        .subscribe(replyTopic, onReply)
        .then(() => {
          // If the acceptance timeout already settled the request, the reply
          // topic has been released and the caller has its timeout error.
          // Publishing now would recreate the topic and enqueue a signal the
          // caller will never see accepted, and the subscribe that just
          // finished may have attached the callback after the release — so
          // release again instead.
          if (settled) {
            releaseReplyTopic(pubsub, replyTopic, onReply);
            return;
          }
          return this.#publishAndWait(pubsub, key, {
            type: 'idle-signal-enqueued',
            runId,
            signal: this.#serializeSignal(signal),
            sourceId: this.#getSourceId(),
            requestId,
            replyTopic,
            targetSourceId,
            expiresAt,
          });
        })
        .catch(error => finish({ error: getErrorFromUnknown(error) }));
    });
  }

  async #deliverAfterClaimedOwnerDiscovery<OUTPUT>(
    pubsub: PubSub,
    key: string,
    runId: string,
    signal: CreatedAgentSignal,
    discovery: Promise<string | undefined>,
    onNoOwner?: () => void,
    onPreAdmissionFailure?: () => void,
    rejectFullLogicalMessageIdentity = false,
    onIdleSignalDiscarded?: () => void,
  ): Promise<SendAgentSignalAccepted<OUTPUT>> {
    const claimedOwnerSourceId = await discovery;
    if (!claimedOwnerSourceId) {
      onNoOwner?.();
      throw new Error(`No claimed thread owner responded for ${key}`);
    }
    if (rejectFullLogicalMessageIdentity) {
      onIdleSignalDiscarded?.();
      onPreAdmissionFailure?.();
      return { action: 'discard' as const };
    }
    let acceptedRunId: string;
    try {
      acceptedRunId = await this.#deliverToClaimedThreadOwner(pubsub, key, runId, signal, claimedOwnerSourceId);
    } catch (error) {
      if (error instanceof ClaimedOwnerPreAdmissionError) onPreAdmissionFailure?.();
      throw error;
    }
    return { action: 'deliver', runId: acceptedRunId };
  }

  async #findClaimedThreadOwner(
    pubsub: PubSub,
    key: string,
    options?: { includeLocal?: boolean; intent?: 'claim'; targetSourceId?: string },
  ): Promise<string | undefined> {
    const hasLocalOwner = this.#getState(pubsub).claimedThreadOwners.has(key);
    if (options?.includeLocal !== false && hasLocalOwner) {
      return this.#getSourceId();
    }

    const requestId = globalThis.crypto.randomUUID();
    const replyTopic = `${AGENT_THREAD_OWNER_DISCOVERY_TOPIC}.${requestId}`;

    return new Promise<string | undefined>(resolve => {
      let settled = false;
      const finish = (sourceId?: string) => {
        if (settled) return;
        settled = true;
        clearTimeout(timeout);
        resolve(sourceId);
        releaseReplyTopic(pubsub, replyTopic, onReply);
      };
      const onReply: EventCallback = withAck(event => {
        const data = event.data as AgentThreadOwnerDiscoveryEvent | undefined;
        if (data?.type === 'thread-owner-response' && data.key === key && data.requestId === requestId) {
          finish(data.sourceId);
        }
      });
      // Absolute deadline carried on the request; see discoverThreadPeers.
      const timeoutMs = AGENT_THREAD_OWNER_DISCOVERY_TIMEOUT_MS;
      const expiresAt = Date.now() + timeoutMs;
      const timeout = setTimeout(() => finish(), timeoutMs);

      void pubsub
        .subscribe(replyTopic, onReply)
        .then(() => {
          // If the timeout already settled the request, the reply topic has
          // been released. Publishing now would invite replies that recreate
          // it, and the subscribe that just finished may have attached the
          // callback after the release — so release again instead.
          if (settled) {
            releaseReplyTopic(pubsub, replyTopic, onReply);
            return;
          }
          return pubsub.publish(AGENT_THREAD_OWNER_DISCOVERY_TOPIC, {
            type: 'thread-owner-request',
            runId: requestId,
            data: {
              type: 'thread-owner-request',
              key,
              requestId,
              replyTopic,
              sourceId: this.#getSourceId(),
              expiresAt,
              ...(options?.intent ? { intent: options.intent } : {}),
              ...(options?.targetSourceId ? { targetSourceId: options.targetSourceId } : {}),
            },
          });
        })
        .catch(() => finish());
    });
  }

  #withBroadcastStream<OUTPUT>(
    output: MastraModelOutput<OUTPUT>,
    pubsub: PubSub | undefined,
    key: string,
    streamId: string,
    resumed = false,
  ) {
    const runtime = this;

    const parts: unknown[] = [];
    // Parts already dropped from the front of `parts`; reader positions are absolute.
    let dropped = 0;
    let published = 0;
    const readers = new Set<{ index: number }>();
    let startPart: unknown;
    const waiters = new Set<() => void>();
    let started = false;
    let done = false;
    let forceStopped = false;
    let abortRequested = false;
    let cancelled = false;
    const visibleToolCallIds = new Set<string>();
    let failed = false;
    /** Cancels whichever source shape the drain loop is attached to. */
    let cancelSource: (() => Promise<void>) | undefined;
    let abortTimer: ReturnType<typeof setTimeout> | undefined;
    let resolveBroadcast!: () => void;
    let rejectBroadcast!: (error: unknown) => void;
    const broadcastCompletion = new Promise<void>((resolve, reject) => {
      resolveBroadcast = resolve;
      rejectBroadcast = reject;
    });
    // Never-rejecting twin of `broadcastCompletion`, settled at exactly the same
    // points. Terminal publishes and rollbacks gate on this so a fail-closed
    // publish error cannot turn a `void`-ed gate into an unhandled rejection;
    // callers that must observe the error keep awaiting `broadcastCompletion`.
    let resolveBroadcastFinished!: () => void;
    const broadcastFinished = new Promise<void>(resolve => {
      resolveBroadcastFinished = resolve;
    });
    let broadcastSettled = false;
    let broadcastError: unknown;
    let hasBroadcastError = false;
    const settleBroadcast = () => {
      if (broadcastSettled) return;
      broadcastSettled = true;
      if (abortTimer !== undefined) {
        clearTimeout(abortTimer);
        abortTimer = undefined;
      }
      resolveBroadcastFinished();
      if (hasBroadcastError) rejectBroadcast(broadcastError);
      else resolveBroadcast();
    };
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

    let needsStart = resumed;
    const emitPart = async (rawPart: unknown): Promise<void> => {
      if (forceStopped) return;
      if (needsStart) {
        needsStart = false;
        // A resumed half of a run doesn't emit its own `start`, and the first
        // half's is dropped once saved. Open the resumed half so subscribers
        // (and joiners) see the run running again.
        if ((rawPart as { type?: string } | null | undefined)?.type !== 'start') {
          await emitPart({
            type: 'start',
            runId: output.runId,
            from: ChunkFrom.AGENT,
            payload: { messageId: output.messageId },
          });
        }
      }
      if (rawPart && typeof rawPart === 'object' && 'type' in rawPart) {
        const typedPart = rawPart as { type?: string; payload?: { toolCallId?: string; toolName?: string } };
        const toolCallId = typedPart.payload?.toolCallId;
        const settlingVisibleTool =
          (typedPart.type === 'tool-result' || typedPart.type === 'tool-error') &&
          toolCallId !== undefined &&
          visibleToolCallIds.has(toolCallId);
        if (abortRequested && !settlingVisibleTool && typedPart.type !== 'abort') return;
        if (!abortRequested && typedPart.type === 'tool-call' && toolCallId !== undefined) {
          visibleToolCallIds.add(toolCallId);
        } else if (settlingVisibleTool) {
          visibleToolCallIds.delete(toolCallId!);
        }
        if (typedPart.type === 'tool-call-approval' || typedPart.type === 'tool-call-suspended') {
          runtime.#markRunSuspending(runtime.#getState(pubsub), output.runId, streamId, {
            toolCallId: typedPart.payload?.toolCallId,
            toolName: typedPart.payload?.toolName,
            kind: typedPart.type === 'tool-call-approval' ? 'approval' : 'generic-tool',
          });
        }
      }
      const part = sanitizeBroadcastPart(rawPart);
      const producedAt = getChunkProducedAt(rawPart) ?? Date.now();
      stampPartProducedAt(part, producedAt);
      parts.push(part);
      if ((part as { type?: string } | null | undefined)?.type === 'start') startPart = part;
      const partType = (part as { type?: string } | null | undefined)?.type;
      // Wake same-runtime replay subscribers before awaiting distributed
      // publication. A lifecycle abort may be published by a concurrently
      // settling completion watcher; local subscribers must not wait for the
      // broker round trip before observing the authoritative tool terminal.
      wake();
      await runtime.#publishAndWait(pubsub, key, {
        type: 'stream-part',
        runId: output.runId,
        streamId,
        part,
        sourceId: runtime.#getSourceId(),
        leaseOwner: runtime.#leaseOwnerForRun(runtime.#getState(pubsub), output.runId),
        producedAt,
        // `start` and prompts stay on the topic until the run is trimmed as a
        // whole, so a subscriber joining mid-run still sees the run begin.
        ...(partType === 'start' || partType === 'tool-call-approval' || partType === 'tool-call-suspended'
          ? { pinned: true }
          : {}),
      });
      published++;
      if (savedAt !== undefined) trimSaved();
      // An error chunk settles `_waitUntilFinished()` without closing
      // `fullStream` (durable error-recovery keeps consuming), so the pump can
      // stay blocked on `read()` forever. The error chunk is the last part
      // this run broadcasts, and it has now been published — settle the
      // broadcast here so the terminal publish gate (and the lease
      // release/drain behind it) is never stranded on the error path.
      if ((part as { type?: string } | null | undefined)?.type === 'error') {
        failed = true;
        resolveBroadcastFinished();
      }
    };

    const emitPublishedPart = async (part: unknown) => {
      try {
        await emitPart(part);
      } catch (cause) {
        const error =
          cause instanceof AgentThreadOutputDrainError
            ? cause
            : new AgentThreadOutputDrainError(
                'terminal-publish-failed',
                `Failed to publish a stream part for agent thread run ${output.runId}`,
                cause,
              );
        broadcastError = error;
        hasBroadcastError = true;
        throw error;
      }
    };

    // `start` is idempotent and returns the drain-completion promise so callers
    // (registerRun / #broadcastPersistedSignal) can gate `run-completed` on the
    // stream-part broadcast settling (PR #202/#204: publishing completion off
    // `_waitUntilFinished()` alone let remote subscribers observe completion
    // FIRST and ignore the late parts).
    let drain: Promise<void> | undefined;
    const start = (): Promise<void> => {
      if (started) return broadcastCompletion;
      started = true;
      if (forceStopped || cancelled) {
        done = true;
        wake();
        settleBroadcast();
        return broadcastCompletion;
      }
      drain = (async () => {
        try {
          // For getter-based outputs read `output.fullStream` lazily so the
          // evented getter yields a fresh stream for the drain loop, independent
          // of the caller's own access; for own-property one-shot outputs drain
          // the single captured stream.
          const source = ownFullStream
            ? capturedSource
            : ((output.__getUnfilteredFullStream?.() ?? output.fullStream) as ReadableStream<unknown> | undefined);
          if (!source) return;

          if (typeof source.getReader === 'function') {
            const reader = source.getReader();
            cancelSource = async () => {
              await reader.cancel().catch(() => {});
            };
            try {
              while (!cancelled) {
                const { value: part, done: streamDone } = await reader.read();
                if (streamDone) break;
                await emitPublishedPart(part);
              }
            } finally {
              reader.releaseLock();
            }
          } else {
            const iterable = source as AsyncIterable<unknown> | Iterable<unknown>;
            const iterator =
              Symbol.asyncIterator in Object(iterable)
                ? (iterable as AsyncIterable<unknown>)[Symbol.asyncIterator]()
                : (iterable as Iterable<unknown>)[Symbol.iterator]();
            let iteratorClosed = false;
            const closeIterator = async () => {
              if (iteratorClosed) return;
              iteratorClosed = true;
              await iterator.return?.();
            };
            cancelSource = async () => {
              await closeIterator().catch(() => {});
            };
            try {
              while (!cancelled) {
                const { value: part, done: streamDone } = await iterator.next();
                if (streamDone) {
                  iteratorClosed = true;
                  break;
                }
                await emitPublishedPart(part);
              }
            } finally {
              if (!iteratorClosed) await closeIterator();
            }
          }
        } catch (caught) {
          // Abort-time source rejection is the provider's cancellation boundary,
          // not a competing subscriber error. Preserve buffered tool terminals
          // and close replay views so they synthesize the authoritative abort.
          if (!abortRequested || hasBroadcastError) {
            error = caught;
            hasError = true;
            failed = true;
          }
        } finally {
          done = true;
          wake();
          settleBroadcast();
        }
      })();
      void drain.catch(() => {});
      return broadcastCompletion;
    };

    const markAbortRequested = () => {
      abortRequested = true;
    };

    const abortBroadcast = (): Promise<void> => {
      markAbortRequested();
      if (done || forceStopped) {
        settleBroadcast();
        return broadcastCompletion;
      }
      if (abortTimer === undefined) {
        abortTimer = setTimeout(() => {
          abortTimer = undefined;
          forceStopped = true;
          done = true;
          wake();
          // The producer may ignore cancellation; its detached source must not
          // keep the thread terminal/output-drain barriers pending.
          void cancelSource?.();
          settleBroadcast();
        }, ABORT_OUTPUT_DRAIN_GRACE_MS);
        abortTimer.unref?.();
      }
      return broadcastCompletion;
    };

    /**
     * Roll back a registration that lost its ownership claim: stop the pump and
     * do not return until any in-flight publish has drained, so no later part
     * can land under a stream id the successor now owns.
     */
    const cancel = async () => {
      if (cancelled) return;
      cancelled = true;
      if (!started) {
        done = true;
        wake();
        settleBroadcast();
        return;
      }
      await cancelSource?.();
      await broadcastFinished;
    };

    // Messages saved mid-run (e.g. at each step) already hold every part up to
    // the last finished step: drop those parts from the topic and, once every
    // open reader has passed them, from this buffer.
    // Latest save not yet fully trimmed; publishing can lag behind the save.
    let savedAt: number | undefined;
    let trimmedThrough = -1;
    const trimSaved = () => {
      if (savedAt === undefined) return;
      let cutoff = -1;
      let cutoffAt: number | undefined;
      for (let i = dropped; i < published; i++) {
        const part = parts[i - dropped] as { type?: string } | undefined;
        const at = getPartProducedAt(part);
        if (at === undefined || at > savedAt) {
          // Saves at a step's end (savePerStep) start before its step-finish is
          // produced: every part of the step is saved, only the marker is newer.
          if (at !== undefined && part?.type === 'step-finish' && i > cutoff + 1) {
            cutoff = i;
            cutoffAt = at;
          }
          savedAt = undefined;
          break;
        }
        if (part?.type === 'step-finish') {
          cutoff = i;
          cutoffAt = at;
        }
      }
      if (cutoffAt === undefined || cutoff <= trimmedThrough) return;
      trimmedThrough = cutoff;
      void runtime.#getPubSub(pubsub)
        .trimTopic(runtime.#threadTopic(key), { runId: output.runId, producedBefore: cutoffAt })
        .catch(() => {});
      let keep = cutoff + 1;
      for (const reader of readers) keep = Math.min(keep, reader.index);
      for (let i = dropped; i < keep; i++) {
        const type = (parts[i - dropped] as { type?: string } | undefined)?.type;
        if (type === 'tool-call-approval' || type === 'tool-call-suspended') {
          keep = i;
          break;
        }
      }
      if (keep > dropped) {
        parts.splice(0, keep - dropped);
        dropped = keep;
      }
    };
    const { threadId: savedThreadId, resourceId: savedResourceId } = runtime.#parseThreadKey(key);
    const stopSaveListener = onThreadMessagesSaved({ threadId: savedThreadId, resourceId: savedResourceId }, at => {
      savedAt = Math.max(savedAt ?? at, at);
      trimSaved();
    });
    void broadcastFinished.then(stopSaveListener);

    const createStream = () => {
      // A subscriber that joins late starts after parts already dropped as
      // saved, but still gets the run's `start`.
      const reader = { index: dropped };
      let pendingStart = dropped > 0 ? startPart : undefined;
      let closed = false;
      let waiter: (() => void) | undefined;
      readers.add(reader);
      const stream = new ReadableStream({
        async pull(controller) {
          void start();
          while (!closed) {
            if (pendingStart !== undefined) {
              controller.enqueue(pendingStart);
              pendingStart = undefined;
              return;
            }
            if (reader.index < dropped) reader.index = dropped;
            if (reader.index - dropped < parts.length) {
              controller.enqueue(parts[reader.index++ - dropped]);
              return;
            }
            if (hasError) {
              controller.error(error);
              return;
            }
            if (done) {
              readers.delete(reader);
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
          readers.delete(reader);
          if (waiter) {
            waiters.delete(waiter);
            waiter();
            waiter = undefined;
          }
        },
      });
      return stream;
    };

    return {
      output,
      createSubscriberStream: createStream,
      startBroadcast: start,
      markAbortRequested,
      abortBroadcast,
      cancelBroadcast: cancel,
      canContinueBroadcast: () => started && !done && !cancelled && !failed,
      broadcastFinished,
    };
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

  prepareRunOptions<OUTPUT>(
    options: AgentExecutionOptions<OUTPUT>,
    pubsub?: PubSub,
    agent?: Agent<any, any, any, any>,
  ): AgentExecutionOptions<OUTPUT> {
    if (hasReadOnlyMemory(options)) return options;

    const { threadId, resourceId } = this.#getThreadTarget(options);
    if (!threadId || !options.runId) return options;
    const key = this.#threadKey(resourceId, threadId);

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
      threadKey: key,
      abortController,
      cleanup: () => upstreamAbortSignal?.removeEventListener('abort', abort),
      ...(agent
        ? { abortHandoff: { agent, streamOptions: options as AgentThreadRunRecord<any>['streamOptions'] } }
        : {}),
    });
    this.#ensureThreadControlSubscription(state, pubsub, key);

    if (state.abortedRunIds.has(options.runId)) {
      abort();
    }

    const requestContext = options.requestContext;
    const controllerContext = requestContext?.get('controller');
    if (requestContext && typeof controllerContext === 'object' && controllerContext !== null) {
      const preparedRequestContext = new RequestContext(requestContext.entries());
      preparedRequestContext.set('controller', { ...controllerContext, abortSignal: abortController.signal });
      return {
        ...options,
        abortSignal: abortController.signal,
        requestContext: preparedRequestContext,
      };
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
    if (hasReadOnlyMemory(options)) return;

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
    options: {
      cleanupPrepared?: boolean;
      clearAbort?: boolean;
      rejectOutputWaiters?: boolean;
    } = {},
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

  isRunAborted(runId: string, pubsub?: PubSub): boolean {
    return this.#getState(pubsub).abortedRunIds.has(runId);
  }

  abortRun(runId: string, pubsub?: PubSub): boolean {
    const resolvedPubSub = this.#getPubSub(pubsub);
    const state = this.#getState(resolvedPubSub);
    const preparedRun = state.preparedRunsById.get(runId);
    const registeredRecord = state.threadRunsById.get(runId);
    if (!preparedRun && !registeredRecord) {
      const key = state.threadKeysByRunId.get(runId);
      if (key) {
        this.#rememberAbortedRun(state, runId);
        // The run may be captured by an idle drain that is still awaiting its
        // lease operation. Record cancellation and reject existing output
        // waiters synchronously, but defer every ownership effect — index
        // removal, reservation-waiter release, prepared/admission cleanup,
        // lease disposition and queue continuation — to that original drain's
        // awaited `#settleCancelledIdleRun`, so no competing drain can start a
        // queued sibling while the lease outcome is still unknown.
        const capturedDraining = state.drainingIdleSignalsByThread.get(key);
        if (capturedDraining?.runId === runId) {
          capturedDraining.cancelled = true;
          this.#rejectPendingOutputWaiters(state, runId, new Error(`Agent thread run id "${runId}" has been aborted`));
          return true;
        }
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
        // Non-reserving drains also capture their item before the lease
        // operation; the same deferred-settlement contract applies.
        const capturedDraining = state.drainingIdleSignalsByThread.get(inflightIdleKey);
        if (capturedDraining?.runId === runId) {
          capturedDraining.cancelled = true;
          this.#rejectPendingOutputWaiters(state, runId, new Error(`Agent thread run id "${runId}" has been aborted`));
          return true;
        }
        state.inflightIdleThreadKeysByRunId.delete(runId);
        state.inflightIdleAgentIdsByRunId.delete(runId);
        this.#forgetCallerSignalsForRun(state, runId);
        this.#rejectPendingOutputWaiters(state, runId, new Error(`Agent thread run id "${runId}" has been aborted`));
        this.#publish(pubsub, inflightIdleKey, { type: 'run-aborted', runId });
        return true;
      }
      // No execution owns this bare ID. Do not create a tombstone that could
      // abort a legitimate future execution reusing the caller-supplied ID.
      return false;
    }
    if (!preparedRun) {
      state.abortedRunIds.add(runId);
      // A run parked on a tool suspension (a question to the user, a pending
      // generation) has no prepared run left to abort, and its completion
      // watcher returned when it suspended, so nothing else frees its thread.
      // A run still inside its suspension window — terminal publication
      // outstanding or the watcher not yet parked — is NOT released here: the
      // watcher remains the sole finalizer, so fall through to the
      // registered-record lifecycle path whose fence orders `run-aborting`
      // behind the outstanding publication.
      const inSuspensionWindow =
        registeredRecord !== undefined &&
        !registeredRecord.parked &&
        !state.approvalSuspendedRunIds.has(runId) &&
        (registeredRecord.lifecycle === 'suspended' ||
          registeredRecord.lifecycle === 'suspending' ||
          this.#isSuspendedRun(state, runId));
      // Only an actually parked run is released by the parked finalizer; a
      // registered run that is neither parked nor inside its suspension window
      // (e.g. registered directly without a prepared entry) still needs its
      // ordinary registered-record lifecycle abort below.
      if (!inSuspensionWindow && this.#isParkedRun(state, runId)) {
        return this.#releaseParkedRun(state, pubsub, runId);
      }
    }

    const key = state.threadKeysByRunId.get(runId);
    if (preparedRun && key && !registeredRecord) {
      if (preparedRun.abortHandoff && !state.startingQueuedRunIds.has(runId)) {
        // Upstream parity (PF-4402): a first-party preparation keeps its
        // reservation until it settles, so queued input is not dropped and no
        // competing run starts meanwhile. Agent.stream() refuses to register
        // the aborted attempt (fail-closed) and its release hands queued input
        // to a fresh follow-up run. Queued follow-up startups keep the fork's
        // immediate release so a cancelled follow-up never recurses.
        preparedRun.abortController.abort();
        this.#rememberAbortedRun(state, runId);
        this.#rejectPendingOutputWaiters(state, runId, new Error(`Agent thread run id "${runId}" has been aborted`));
        return true;
      }
      preparedRun.abortController.abort();
      this.#rememberAbortedRun(state, runId);
      preparedRun.cleanup();
      state.preparedRunsById.delete(runId);
      this.#releaseReservedRun(state, pubsub, key, runId, {
        rejectOutputWaiters: true,
        // Upstream parity (PF-4402): aborting a queued follow-up while it
        // prepares cancels that attempt only. The draining handoff restores
        // its captured signal; input queued behind it stays for the next
        // natural turn and is not re-drained here (no recursion).
        preserveQueuedInput: state.startingQueuedRunIds.has(runId),
      });
      return true;
    }

    const ownsAbortLifecycle = registeredRecord?.finalizeAbort?.() ?? false;
    if (ownsAbortLifecycle) registeredRecord?.markAbortRequested?.();
    preparedRun?.abortController.abort();
    this.#rememberAbortedRun(state, runId);

    if (registeredRecord && ownsAbortLifecycle) {
      // Preserve a short window for the provider/tool stream to publish its
      // authoritative cancellation terminal, then detach even if it ignores
      // abort forever. Terminalize completion only after that bounded broadcast
      // barrier so remote subscribers receive all prompt tool terminals before
      // the lifecycle abort event closes their proxy.
      for (const notifyAbort of this.#eagerAbortListenersByStreamId.get(registeredRecord.streamId) ?? []) {
        notifyAbort();
      }
      const registration = this.#threadOutputRegistrations.get(registeredRecord.output) ?? Promise.resolve();
      // Order the abort fence behind an already-started `run-suspended`
      // publication so `run-aborting` cannot overtake it on the wire. Only the
      // record-local publication receipt is awaited — never the watcher's
      // terminal promise, whose completion this fence itself fulfills.
      const suspensionPublication = registeredRecord.suspensionPublication ?? Promise.resolve();
      const publishAborting = async () => {
        if (!key) return;
        await this.#publishTerminalAndWait(resolvedPubSub, key, {
          type: 'run-aborting',
          runId,
          streamId: registeredRecord.streamId,
          leaseOwner: registeredRecord.leaseOwner,
        });
      };
      const publishAbortAfterBroadcast = async () => {
        if (!key) return;
        await this.#publishTerminalAndWait(resolvedPubSub, key, {
          type: 'run-aborted',
          runId,
          streamId: registeredRecord.streamId,
          leaseOwner: registeredRecord.leaseOwner ?? this.#leaseOwnerForRun(state, runId),
        });
      };
      const fence = Promise.all([registration, suspensionPublication]).then(publishAborting);
      // Arm local bounded teardown whether the distributed fence succeeds or
      // fails. A failed fence must block later publication, but it cannot leave
      // an abort-ignoring provider/broadcast retaining the run and lease.
      const broadcastAbort = fence.then(
        () => registeredRecord.abortBroadcast?.() ?? Promise.resolve(),
        async error => {
          await registeredRecord.abortBroadcast?.();
          throw error;
        },
      );
      const abortDelivery = broadcastAbort.then(publishAbortAfterBroadcast);
      registeredRecord.abortDelivery = abortDelivery;
      // A failed registration/fence has no valid distributed segment to
      // terminate. Retain the rejection for the output-drain barrier instead
      // of publishing an orphan terminal; local provider abort still occurred.
      void abortDelivery.catch(() => {});
    }

    if (key) {
      // Preserve-by-default abort: queued follow-up input survives an
      // ordinary exact-run abort and is handed off by the bounded abort
      // finalizer below. Only the explicit `clearPendingSignals` flow —
      // owned by `abortThread` and the control subscription — cancels it.
      const streamId = state.activeThreadRunIds.get(key) === runId ? state.activeThreadStreamIds.get(key) : undefined;
      // Registered runs retain the lease through their bounded abort finalizer,
      // which either transfers it to queued work or releases it without a gap.
      // Pre-registration abort paths still release in their reservation cleanup.
      if (!registeredRecord) {
        this.#releaseThreadLease(pubsub, key, runId);
        this.#publish(pubsub, key, { type: 'run-aborted', runId, streamId });
      }
    }

    return true;
  }

  /**
   * A run parked on a tool suspension: its active segment ended (so it was
   * evicted from `preparedRunsById` by {@link #cleanupPreparedRun}), but its
   * record remains the thread's blocking run until it is resumed or released.
   */
  #isParkedRun(state: AgentThreadRuntimeState, runId: string): boolean {
    const record = state.threadRunsById.get(runId);
    return state.suspendedRunIds.has(runId) || record?.lifecycle === 'suspended' || record?.lifecycle === 'suspending';
  }

  /**
   * Release an aborted run that is parked on a tool suspension. Its completion
   * watcher returned when it suspended, so without this the thread keeps it as
   * its blocking run: every later message is queued onto a run that will never
   * resume, and nothing sent after Stop is answered. The run is released the
   * way a finished run is: an authenticated `run-aborted` terminal is delivered
   * under the captured owner token, then its records and thread reservation are
   * dropped and the thread's pending work starts or its lease is given up. A
   * run parked on a tool approval is left alone; its decline path releases it.
   */
  #releaseParkedRun(state: AgentThreadRuntimeState, pubsub: PubSub | undefined, runId: string): boolean {
    if (state.approvalSuspendedRunIds.has(runId)) return false;
    const record = state.threadRunsById.get(runId);
    if (!this.#isParkedRun(state, runId) || !record) return false;
    // Neither `lifecycle === 'suspended'` nor the stream watch proves
    // retirement: the completion watcher is the sole finalizer until it sets
    // the explicit parked marker after successful suspension publication and
    // generation revalidation. A run whose run-suspended publication is still
    // outstanding (or whose stream is still locally watched mid-run) must be
    // aborted through the record lifecycle instead — tearing it down here
    // would race the watcher's own finalization.
    if (
      !record.parked &&
      (record.suspensionPublication !== undefined || state.watchedThreadStreamIds.has(record.streamId))
    ) {
      return false;
    }
    const key = state.threadKeysByRunId.get(runId) ?? this.#threadKey(record.resourceId, record.threadId);
    // Retire the parked bookkeeping synchronously so a repeated abort while the
    // terminal publication is in flight cannot start a second settlement
    // owner. The ownership indexes and the captured owner token survive until
    // the authenticated terminal is actually delivered; cleanup and handoff
    // are generation-fenced in #finalizeParkedRunRelease.
    this.#clearSuspendedRun(state, runId);
    record.lifecycle = 'completed';
    const ownedActive = state.activeThreadRunIds.get(key) === runId;
    void this.#finalizeParkedRunRelease(state, pubsub, key, runId, record, ownedActive);
    return true;
  }

  /**
   * Bounded authenticated terminal publication for a parked run aborted through
   * {@link #releaseParkedRun}, followed by generation-fenced cleanup and
   * handoff. The captured owner token signs the `run-aborted` terminal the same
   * way the registered-run abort fence does, so receivers accept it against the
   * exact owner that signed the segment instead of rejecting an unauthenticated
   * terminal. A failed publication rejects truthfully and is retained as
   * infrastructure evidence — it is never swallowed into a successful-looking
   * release, and a same-run successor that registers while the terminal is in
   * flight keeps its records, token and prepared state.
   */
  async #finalizeParkedRunRelease(
    state: AgentThreadRuntimeState,
    pubsub: PubSub | undefined,
    key: string,
    runId: string,
    record: AgentThreadRunRecord<any>,
    ownedActive: boolean,
  ): Promise<void> {
    const streamId = record.streamId;
    const leaseOwner = record.leaseOwner ?? this.#leaseOwnerForRun(state, runId);
    // Capture the retired attempt's own output-waiter identities before the
    // terminal publication is awaited. A same-run successor that reserves,
    // prepares or registers this run id while the terminal is in flight owns
    // the run-id waiter bucket from that point on; the stale-generation branch
    // below must settle only these captured identities, never the successor's.
    const retiredOutputWaiters = [...(state.pendingOutputWaiters.get(runId) ?? [])];
    // Output-scoped delivery receipt: install the bounded terminal promise on
    // this retired record before awaiting it, so a later drain of the same run
    // id settles on this attempt's own publication outcome instead of racing
    // it detached. The receipt is ALSO bound to the exact retired output
    // object: the run-id record is replaced by a successor, and the retired
    // output's terminal entry settled at its suspension, so the output-scoped
    // entry is what keeps this publication observable through the old output.
    const delivery = this.#publishTerminalAndWait(pubsub, key, {
      type: 'run-aborted',
      runId,
      streamId,
      leaseOwner,
    });
    record.abortDelivery = delivery;
    this.#threadOutputParkedAbortDeliveries.set(record.output, delivery);
    // The rejection is retained as the receipt below; keep the record-scoped
    // promise handled so it never surfaces as an unhandled rejection.
    void delivery.catch(() => {});
    try {
      await delivery;
    } catch (error) {
      // Truthful delivery failure: no run-aborted terminal reached the wire.
      // Reject this attempt's waiters with the failure and retain it as
      // infrastructure evidence so a later lookup for the same aborted attempt
      // keeps the abort message with the original delivery error as cause.
      const deliveryError = getErrorFromUnknown(error);
      // Generation fence for the run-id receipt and waiters, checked before
      // any mutation: a same-run successor that registered — or an attempt
      // that reserved/prepared this run id — while the terminal was in flight
      // owns the run-id receipt and its own waiters now. This retired record
      // may settle only its own output-scoped receipt (already installed
      // above) plus waiters that still belong to it; it must not poison the
      // successor's state with a stale failure.
      const successorHoldsRunId =
        state.threadRunsById.get(runId) !== record ||
        state.preparedRunsById.has(runId) ||
        state.reservedAgentIdsByRunId.has(runId);
      if (successorHoldsRunId) {
        // Stale generation: the current run-id waiter bucket belongs to the
        // successor now. Settle only the waiter identities captured for this
        // retired attempt; waiters installed after the capture (the successor's
        // reservation/registration waiters) must never see this failure.
        this.#rejectCapturedOutputWaiters(state, runId, retiredOutputWaiters, deliveryError);
      } else {
        this.#rejectPendingOutputWaiters(state, runId, deliveryError);
        this.#rememberRejectedRunError(state, runId, deliveryError, { infrastructure: true });
      }
    }
    // Generation fence: a same-run successor that registered while the terminal
    // was in flight owns these indexes now. This retired record may settle only
    // its own receipt — never the successor's records, token or prepared state.
    if (state.threadRunsById.get(runId) !== record) return;
    if (state.threadRunsByStreamId.get(streamId) === record) {
      state.threadRunsByStreamId.delete(streamId);
    }
    state.threadRunsById.delete(runId);
    if (state.threadKeysByRunId.get(runId) === key) {
      state.threadKeysByRunId.delete(runId);
    }
    if (!ownedActive) return;
    if (state.activeThreadRunIds.get(key) === runId) {
      state.activeThreadRunIds.delete(key);
      if (state.activeThreadStreamIds.get(key) === streamId) {
        state.activeThreadStreamIds.delete(key);
      }
    }
    if (this.#hasPendingThreadWork(state, key)) {
      await this.#drainPendingSignals(state, pubsub, key, record);
    } else {
      this.#releaseThreadLease(pubsub, key, runId);
    }
  }

  getActiveThreadRunId(options: AgentThreadIdentityOptions, pubsub?: PubSub): string | undefined {
    const state = this.#getState(pubsub);
    const key = this.#threadKey(options.resourceId, options.threadId);
    const activeRunId = state.activeThreadRunIds.get(key);
    if (!activeRunId) return undefined;

    const record = state.threadRunsById.get(activeRunId);
    if (record && !this.#isThreadBlockingRun(state, record)) return undefined;

    return activeRunId;
  }

  /** Same predicate as {@link getActiveThreadRunId}, over every tracked thread. */
  listActiveThreadRuns(pubsub?: PubSub): ActiveThreadRun[] {
    const state = this.#getState(pubsub);
    const runs: ActiveThreadRun[] = [];
    for (const [key, runId] of state.activeThreadRunIds) {
      const record = state.threadRunsById.get(runId);
      if (record && !this.#isThreadBlockingRun(state, record)) continue;
      runs.push({ runId, ...this.#parseThreadKey(key) });
    }
    return runs;
  }

  hasThreadRun(runId: string, pubsub?: PubSub): boolean {
    return this.#getState(pubsub).threadRunsById.has(runId);
  }

  /**
   * Resolves once a retained same-run segment whose provider output already
   * finished as `suspended` has finished its own terminal delivery (published
   * `run-suspended` and parked, or been finalized otherwise).
   *
   * The completion watcher gates `run-suspended` on the stream-part broadcast,
   * which can lag the provider (a remote publish round trip per part). A resume
   * that reuses the run id and arrives inside that window would otherwise find
   * a record that still looks live and be rejected as a duplicate
   * reservation/registration. Waiting for the exact watcher's terminal keeps
   * the prior segment the sole finalizer (no forced record replacement, no
   * lease release racing the successor) and orders the resumed segment's
   * `run-registered` behind the prior `run-suspended` on the wire. A live
   * running segment, or a suspension-spanning registration, returns
   * `undefined` synchronously so the existing duplicate guards keep applying.
   */
  waitForSuspendedSegmentSettlement(runId: string, pubsub?: PubSub): Promise<void> | undefined {
    // Synchronous when there is nothing to wait for: callers must not yield
    // (and let the prior segment's lifecycle advance) on the common path.
    const record = this.#getState(pubsub).threadRunsById.get(runId);
    if (!record || record.parked || !record.providerSettled) return undefined;
    if (record.lifecycle !== 'running' && record.lifecycle !== 'suspending') return undefined;
    if ((record.currentSegmentOutput ?? record.output).status !== 'suspended') return undefined;
    // A suspension-spanning registration keeps its source output open across
    // the suspension, so its terminal only lands when the whole run ends.
    if (record.continuation) return undefined;
    // A rejected terminal is retained as the run's own failure evidence by the
    // watcher; the resume then meets the ordinary guards unchanged.
    return this.#threadOutputTerminals.get(record.output)?.catch(() => {});
  }

  getResumableThreadRun(
    options: AgentSubscribeToThreadOptions & {
      runId: string;
      toolCallId?: string;
      suspensionKind?: AgentThreadRunSuspension['kind'];
    },
    pubsub?: PubSub,
  ): { runId: string; toolCallId?: string } | undefined {
    const state = this.#getState(pubsub);
    const key = this.#threadKey(options.resourceId, options.threadId);
    const record = state.threadRunsById.get(options.runId);
    const isSuspended = this.#isSuspendedRun(state, options.runId);
    if (!record || state.threadKeysByRunId.get(options.runId) !== key || !isSuspended) {
      return undefined;
    }

    const suspensions = state.suspensionMetadataByRunId.get(options.runId);
    const suspension = options.toolCallId
      ? suspensions?.get(options.toolCallId)
      : options.suspensionKind
        ? [...(suspensions?.values() ?? [])].find(candidate => candidate.kind === options.suspensionKind)
        : suspensions?.values().next().value;
    if ((options.toolCallId || options.suspensionKind) && !suspension) {
      return undefined;
    }
    if (options.suspensionKind && suspension?.kind !== options.suspensionKind) {
      return undefined;
    }

    return { runId: options.runId, toolCallId: options.toolCallId ?? suspension?.toolCallId };
  }

  /** Whether the live run registry has already advanced to this exact suspended tool call. */
  isSuspendedToolCall(runId: string, toolCallId: string, pubsub?: PubSub): boolean {
    const state = this.#getState(pubsub);
    if (!this.#isSuspendedRun(state, runId)) return false;
    const suspensions = state.threadRunsById.get(runId)?.suspensions ?? state.suspensionMetadataByRunId.get(runId);
    return suspensions?.has(toolCallId) ?? false;
  }

  async queueStreamResume<OUTPUT>(
    runId: string,
    resume: () => Promise<MastraModelOutput<OUTPUT>>,
    pubsub?: PubSub,
  ): Promise<MastraModelOutput<OUTPUT>> {
    const state = this.#getState(pubsub);
    const previousTail = state.resumeTailsByRunId.get(runId) ?? Promise.resolve();
    let resolveStarted!: (output: MastraModelOutput<OUTPUT>) => void;
    let rejectStarted!: (error: unknown) => void;
    const started = new Promise<MastraModelOutput<OUTPUT>>((resolve, reject) => {
      resolveStarted = resolve;
      rejectStarted = reject;
    });

    const resumeTail = previousTail
      .catch(() => {})
      .then(async () => {
        try {
          const output = await resume();
          resolveStarted(output);
          await output._waitUntilFinished();
        } catch (error) {
          rejectStarted(error);
          throw error;
        }
      });
    const settledTail = resumeTail
      .catch(() => {})
      .finally(() => {
        if (state.resumeTailsByRunId.get(runId) === settledTail) {
          state.resumeTailsByRunId.delete(runId);
        }
      });
    state.resumeTailsByRunId.set(runId, settledTail);

    return started;
  }

  abortThread(options: AgentAbortThreadOptions, pubsub?: PubSub): boolean {
    const resolvedPubSub = this.#getPubSub(pubsub);
    const state = this.#getState(resolvedPubSub);
    const key = this.#threadKey(options.resourceId, options.threadId);
    const runId = this.getActiveThreadRunId(options, resolvedPubSub);
    // A stale request conditioned on a run that is no longer active must not
    // stop the successor run or clear input queued for it.
    if (options.expectedRunId !== undefined && options.expectedRunId !== runId) return false;
    if (options.clearPendingSignals) this.#cancelPendingSignals(state, key);
    try {
      if (!runId) return false;
      if (state.preparedRunsById.has(runId)) return this.abortRun(runId, resolvedPubSub);
      if (state.threadKeysByRunId.get(runId) === key) {
        // Reserved locally (a sendSignal wake that has not prepared its run yet):
        // record the abort intent so preparation aborts the run before it starts.
        this.abortRun(runId, resolvedPubSub);
        return true;
      }
      if (state.remoteThreadKeysByRunId.get(runId) !== key) return false;
      const streamId = state.activeThreadStreamIds.get(key);
      if (!streamId) return false;
      // A remote owner's run is only stopped when the abort is meant for it. Thread
      // lifecycle transitions abort locally on the way out and must not reach across
      // processes: a follower running `/new` would otherwise kill the owner's run.
      if (options.localOnly) return false;
      // Publish authenticated with the provider's current owner (fork contract):
      // every owner-side handler rejects an unauthenticated or forged request.
      void this.#getLeaseProvider(resolvedPubSub)
        .getLeaseOwner(key)
        .then(leaseOwner => {
          if (leaseOwner && this.#runIdFromLeaseOwner(leaseOwner) === runId) {
            this.#publish(resolvedPubSub, key, {
              type: 'run-abort-requested',
              runId,
              streamId,
              leaseOwner,
              ...(options.clearPendingSignals ? { clearPendingSignals: true } : {}),
            });
          }
        })
        .catch(() => {});
      return true;
    } finally {
      // Synchronous listeners must see the completed clear/abort, not an intermediate state.
      if (options.clearPendingSignals) this.#notifyThreadEvents(state);
    }
  }

  /** @internal */
  resetForTests() {
    this.#eagerAbortListenersByStreamId.clear();
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
    for (const subscription of state.threadControlSubscriptions.values()) subscription.unsubscribe();
    state.threadControlSubscriptions.clear();
    state.leaseRenewalTimers.forEach(timer => clearInterval(timer));
    state.leaseRenewalTimers.clear();
    state.threadRunsById.clear();
    state.threadRunsByStreamId.clear();
    state.threadKeysByRunId.clear();
    state.remoteThreadKeysByRunId.clear();
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
    state.drainingPendingSignalsByThread.clear();
    state.foreignWinnerHandoffsByThread.clear();
    state.unresolvedForwardingsByThread.clear();
    state.pendingContinuationsByThread.clear();
    state.claimedThreadOwnerDiscoveries.clear();
    state.handledIdleSignals.clear();
    for (const claim of [...state.claimedThreadOwners.values()]) {
      claim.unsubscribe();
    }
    state.claimedThreadOwners.clear();
    for (const peer of [...state.advertisedThreadPeers.values()]) {
      peer.unsubscribe();
    }
    state.advertisedThreadPeers.clear();
    state.activeThreadStreamIds.clear();
    state.streamSeqByRunId.clear();
    state.watchedThreadStreamIds.clear();
    state.preparedRunsById.clear();
    state.reservedAgentIdsByRunId.clear();
    state.reservationWaitersByRunId.clear();
    state.resumeTailsByRunId.clear();
    for (const runId of state.abortedRunIds) {
      this.#forgetAbortedRun(state, runId);
    }
    for (const runId of state.rejectedRunErrorsByRunId.keys()) {
      this.#forgetRejectedRunError(state, runId);
    }
    state.acceptedCallerSignals.clear();
    state.callerSignalIdsByRunId.clear();
    if (state.signalAdmissionCleanupTimer !== undefined) {
      clearTimeout(state.signalAdmissionCleanupTimer);
      state.signalAdmissionCleanupTimer = undefined;
    }
    state.signalAdmissionsByThread.clear();
    state.leaseOwnerTokensByRunId.clear();
    state.remoteStreamIdentityByThread.clear();
    for (const runId of state.pendingOutputWaiters.keys()) {
      this.#rejectPendingOutputWaiters(state, runId, new Error(`Agent thread run id "${runId}" was reset`));
    }
    for (const runId of state.rejectedRunErrorsByRunId.keys()) {
      this.#forgetRejectedRunError(state, runId);
    }
    state.registrationPublishesByStreamId.clear();
    state.broadcastsByStreamId.clear();
    this.#eagerAbortListenersByStreamId.clear();
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

  #rememberRejectedRunError(
    state: AgentThreadRuntimeState,
    runId: string,
    error: Error,
    options: { infrastructure?: boolean } = {},
  ) {
    this.#forgetRejectedRunError(state, runId);

    const cleanupTimer = setTimeout(() => {
      state.rejectedRunErrorsByRunId.delete(runId);
    }, REJECTED_RUN_TOMBSTONE_TTL_MS);
    (cleanupTimer as { unref?: () => void }).unref?.();
    state.rejectedRunErrorsByRunId.set(runId, { error, cleanupTimer, infrastructure: options.infrastructure });

    if (state.rejectedRunErrorsByRunId.size <= MAX_REJECTED_RUN_TOMBSTONES) return;

    const oldestRunId = state.rejectedRunErrorsByRunId.keys().next().value;
    if (oldestRunId) {
      this.#forgetRejectedRunError(state, oldestRunId);
    }
  }

  #forgetCallerSignalsForRun(state: AgentThreadRuntimeState, runId: string) {
    const callerSignalIds = state.callerSignalIdsByRunId.get(runId);
    if (callerSignalIds) {
      state.callerSignalIdsByRunId.delete(runId);
      for (const callerSignalId of callerSignalIds) state.acceptedCallerSignals.delete(callerSignalId);
    }
  }

  #scheduleSignalAdmissionCleanup(state: AgentThreadRuntimeState): void {
    if (state.signalAdmissionCleanupTimer !== undefined || state.signalAdmissionsByThread.size === 0) return;
    let nextExpiry = Number.POSITIVE_INFINITY;
    for (const admissions of state.signalAdmissionsByThread.values()) {
      for (const retained of admissions.values()) nextExpiry = Math.min(nextExpiry, retained.expiresAt);
    }
    if (!Number.isFinite(nextExpiry)) return;
    const timer = setTimeout(
      () => {
        state.signalAdmissionCleanupTimer = undefined;
        const now = Date.now();
        for (const [key, admissions] of state.signalAdmissionsByThread) {
          for (const [signalId, retained] of admissions) {
            if (retained.expiresAt <= now) admissions.delete(signalId);
          }
          if (admissions.size === 0) state.signalAdmissionsByThread.delete(key);
        }
        this.#scheduleSignalAdmissionCleanup(state);
      },
      Math.max(0, nextExpiry - Date.now()),
    );
    timer.unref?.();
    state.signalAdmissionCleanupTimer = timer;
  }

  #clearSignalAdmissionCleanupIfEmpty(state: AgentThreadRuntimeState): void {
    if (state.signalAdmissionsByThread.size !== 0 || state.signalAdmissionCleanupTimer === undefined) return;
    clearTimeout(state.signalAdmissionCleanupTimer);
    state.signalAdmissionCleanupTimer = undefined;
  }

  #rememberSignalPayloadForRun(
    state: AgentThreadRuntimeState,
    key: string,
    runId: string,
    signal: AgentSignal,
    options: { admissionAttemptId?: string; allowAttemptSupersede?: boolean } = {},
  ): { disposition: 'accepted' | 'duplicate' | 'conflict'; runId: string } {
    const signalId = signal.id;
    const payloadKey = callerSignalPayloadKey(signal);
    if (signalId === undefined || payloadKey === undefined) return { disposition: 'accepted', runId };
    const now = Date.now();
    const admissions = state.signalAdmissionsByThread.get(key) ?? new Map();
    for (const [retainedId, retained] of admissions) {
      if (retained.expiresAt <= now) admissions.delete(retainedId);
    }
    const retained = admissions.get(signalId);
    if (retained !== undefined) {
      if (
        retained.payloadKey === payloadKey &&
        options.allowAttemptSupersede === true &&
        options.admissionAttemptId !== undefined &&
        retained.admissionAttemptId !== undefined &&
        retained.admissionAttemptId !== options.admissionAttemptId
      ) {
        admissions.set(signalId, {
          payloadKey,
          runId,
          expiresAt: now + SIGNAL_ADMISSION_TOMBSTONE_TTL_MS,
          admissionAttemptId: options.admissionAttemptId,
        });
        return { disposition: 'accepted', runId };
      }
      return {
        disposition: retained.payloadKey === payloadKey ? 'duplicate' : 'conflict',
        runId: retained.runId,
      };
    }
    admissions.set(signalId, {
      payloadKey,
      runId,
      expiresAt: now + SIGNAL_ADMISSION_TOMBSTONE_TTL_MS,
      ...(options.admissionAttemptId !== undefined ? { admissionAttemptId: options.admissionAttemptId } : {}),
    });
    while (admissions.size > MAX_SIGNAL_ADMISSION_TOMBSTONES_PER_THREAD) {
      const oldest = admissions.keys().next().value;
      if (oldest === undefined) break;
      admissions.delete(oldest);
    }
    state.signalAdmissionsByThread.set(key, admissions);
    while (state.signalAdmissionsByThread.size > MAX_SIGNAL_ADMISSION_THREADS) {
      const oldestKey = state.signalAdmissionsByThread.keys().next().value;
      if (oldestKey === undefined) break;
      state.signalAdmissionsByThread.delete(oldestKey);
    }
    this.#scheduleSignalAdmissionCleanup(state);
    return { disposition: 'accepted', runId };
  }

  #findSignalPayloadForRun(
    state: AgentThreadRuntimeState,
    key: string,
    signal: AgentSignal,
  ): { disposition: 'duplicate' | 'conflict'; runId: string } | undefined {
    const signalId = signal.id;
    const payloadKey = callerSignalPayloadKey(signal);
    if (signalId === undefined || payloadKey === undefined) return undefined;
    const retained = state.signalAdmissionsByThread.get(key)?.get(signalId);
    if (retained === undefined || retained.expiresAt <= Date.now()) return undefined;
    return {
      disposition: retained.payloadKey === payloadKey ? 'duplicate' : 'conflict',
      runId: retained.runId,
    };
  }

  #hasConfirmedSignalAdmission(state: AgentThreadRuntimeState, key: string, runId: string): boolean {
    const record = state.threadRunsById.get(runId);
    if (record) return this.#threadKey(record.resourceId, record.threadId) === key;
    return (
      state.leaseRenewalTimers.has(runId) &&
      (state.threadKeysByRunId.get(runId) === key ||
        state.activeThreadRunIds.get(key) === runId ||
        state.inflightIdleThreadKeysByRunId.get(runId) === key)
    );
  }

  #forgetSignalAdmission(state: AgentThreadRuntimeState, key: string, runId: string, signal: AgentSignal): void {
    const signalId = signal.id;
    const payloadKey = callerSignalPayloadKey(signal);
    if (signalId === undefined || payloadKey === undefined) return;
    const admissions = state.signalAdmissionsByThread.get(key);
    if (!admissions) return;
    const retained = admissions.get(signalId);
    if (retained?.runId !== runId || retained.payloadKey !== payloadKey) return;
    admissions.delete(signalId);
    if (admissions.size === 0) state.signalAdmissionsByThread.delete(key);
    this.#clearSignalAdmissionCleanupIfEmpty(state);
  }

  #forgetSignalAdmissionsForRun(state: AgentThreadRuntimeState, key: string, runId: string): void {
    const admissions = state.signalAdmissionsByThread.get(key);
    if (!admissions) return;
    for (const [signalId, retained] of admissions) {
      if (retained.runId === runId) admissions.delete(signalId);
    }
    if (admissions.size === 0) state.signalAdmissionsByThread.delete(key);
    this.#clearSignalAdmissionCleanupIfEmpty(state);
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

  /**
   * Reject only the waiter identities captured for one retired attempt of
   * `runId`, leaving waiters installed after the capture — a same-run
   * successor's reservation or registration waiters — untouched. Used by the
   * parked-run release, whose terminal publication can fail after a successor
   * already owns the run-id waiter bucket.
   */
  #rejectCapturedOutputWaiters(
    state: AgentThreadRuntimeState,
    runId: string,
    captured: Array<{ resolve: (out: MastraModelOutput<any>) => void; reject: (error: Error) => void }>,
    error: Error,
  ) {
    if (captured.length === 0) return;
    const current = state.pendingOutputWaiters.get(runId);
    if (current) {
      for (const waiter of captured) {
        const index = current.indexOf(waiter);
        if (index !== -1) current.splice(index, 1);
      }
      if (current.length === 0) state.pendingOutputWaiters.delete(runId);
    }
    for (const waiter of captured) waiter.reject(error);
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
      this.#forgetSignalAdmissionsForRun(state, key, runId);
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
    options: {
      cleanupPrepared?: boolean;
      clearAbort?: boolean;
      rejectOutputWaiters?: boolean;
      announceAbort?: boolean;
      /**
       * Cleanup-only mode for a caller that owns the ownership handoff: the
       * helper performs index/waiter/admission cleanup but suppresses BOTH the
       * lease release and the sibling idle drain, leaving lease disposition
       * and queue continuation to the caller. `announceAbort: false` alone is
       * insufficient — it only suppresses the terminal publish.
       */
      callerOwnedHandoff?: boolean;
      /** Keep the thread's pending/pre-run queues (see the abortRun follow-up branch). */
      preserveQueuedInput?: boolean;
    } = {},
  ) {
    const ownsThread = state.activeThreadRunIds.get(key) === runId || state.threadKeysByRunId.get(runId) === key;
    const wasAborted = state.abortedRunIds.has(runId);
    // An aborted first-party preparation settling (see `abortRun`): hand its
    // queued input to a fresh follow-up run instead of dropping it.
    const abortHandoff =
      wasAborted && ownsThread && !state.startingQueuedRunIds.has(runId)
        ? state.preparedRunsById.get(runId)?.abortHandoff
        : undefined;
    // Upstream parity: a queued idle/continuation startup's own catch path
    // restores its input and drains pending-before-idle work, so releasing its
    // reservation here must not drop the queues, dispatch idle work or give up
    // its lease. (A follow-up drain's startup keeps the fork's release: its
    // draining record already owns the captured signal.)
    if (state.startingQueuedRunIds.has(runId) && !state.drainingPendingSignalsByThread.has(key)) {
      options = { ...options, callerOwnedHandoff: true };
    }
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
    if (ownsThread && !options.callerOwnedHandoff && !abortHandoff && !options.preserveQueuedInput) {
      // A caller that owns the handoff drains the surviving queue itself;
      // deleting it here would destroy the work the caller is about to
      // hand off.
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
      this.#forgetSignalAdmissionsForRun(state, key, runId);
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
    if (
      ownsThread &&
      !state.threadRunsById.has(runId) &&
      state.leaseRenewalTimers.has(runId) &&
      !options.callerOwnedHandoff
    ) {
      this.#releaseThreadLease(pubsub, key, runId);
    }
    if (ownsThread && !options.callerOwnedHandoff) {
      if (options.announceAbort !== false) {
        this.#publish(pubsub, key, { type: 'run-aborted', runId });
      }
      const target = abortHandoff ? this.#getThreadTarget(abortHandoff.streamOptions) : undefined;
      if (abortHandoff && target?.threadId) {
        void this.#drainPendingSignals(state, pubsub, key, {
          ...abortHandoff,
          runId,
          threadId: target.threadId,
          resourceId: target.resourceId,
        }).catch(() => {});
      } else {
        void this.#drainPendingIdleSignals(state, pubsub, key).catch(() => {});
      }
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
    // Transient signals are delivery-only: never write them to storage, even when the
    // active-behavior asked to persist. Honored here (not just in the memory layer) so it holds
    // for any memory implementation, including ones without a signal-aware save filter.
    if (signal.transient) return;
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
    agentId: string,
    signal: CreatedAgentSignal,
    resourceId: string,
    threadId: string,
  ) {
    let finish!: () => void;
    const finished = new Promise<void>(resolve => {
      finish = resolve;
    });
    // Mirror the shape every real `start` emitter uses (see loop/workflows/stream.ts).
    // The messageId is derived from the signal id rather than reused verbatim so
    // consumers keying messages by id don't collide with the persisted signal row.
    const startChunk: ChunkType = {
      type: 'start',
      runId,
      from: ChunkFrom.AGENT,
      payload: { id: agentId, messageId: `persisted-signal:${signal.id}` },
    };
    const parts: any[] = [
      startChunk,
      { ...signal.toDataPart(), runId },
      {
        type: 'finish',
        runId,
        from: ChunkFrom.AGENT,
        payload: {
          stepResult: { reason: 'stop' },
          output: {
            usage: { inputTokens: 0, outputTokens: 0, totalTokens: 0 },
          },
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
    const leaseOwner = this.#leaseOwnerForRun(state, runId);
    const {
      output: outputForSubscribers,
      createSubscriberStream,
      startBroadcast,
      abortBroadcast,
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
      abortBroadcast,
      leaseOwner,
    };

    state.threadRunsById.set(runId, record);
    state.threadRunsByStreamId.set(streamId, record);
    state.threadKeysByRunId.set(runId, key);
    state.activeThreadRunIds.set(key, runId);
    state.activeThreadStreamIds.set(key, streamId);
    const registered = this.#getLeaseProvider(pubsub)
      .acquireLease(key, leaseOwner, AGENT_THREAD_LEASE_TTL_MS)
      .then(lease => {
        if (!lease.acquired) throw new Error(`Agent thread run id "${runId}" lost its persisted-signal lease`);
        this.#startLeaseRenewal(this.#getPubSub(pubsub), key, runId);
        return this.#publishAndWait(pubsub, key, {
          type: 'run-registered',
          runId,
          streamId,
          streamSeq,
          sourceId: this.#getSourceId(),
          leaseOwner,
        });
      });
    const broadcast = registered.then(startBroadcast, startBroadcast).catch(() => {});
    // PR #202 review (P2): the synthetic output's `finish()` fires at stream
    // CONSTRUCTION, long before an async pubsub has published the stream-parts.
    // Publishing `run-completed` off `_waitUntilFinished()` alone let remote
    // subscribers observe completion FIRST, delete the remote run, and ignore
    // the late persisted-signal parts. Gate completion on registration + the
    // stream-part broadcast settling.
    void Promise.allSettled([outputForSubscribers._waitUntilFinished(), broadcast]).then(() => {
      setTimeout(() => {
        void (async () => {
          try {
            await this.#publishTerminalAndWait(pubsub, key, {
              type: 'run-completed',
              runId,
              streamId,
              // The signal this run rebroadcasts is persisted by definition, so
              // once run-completed lands its entries can leave the topic.
              persisted: true,
              status: 'success',
              leaseOwner,
            });
            await this.#getPubSub(pubsub)
              .trimTopic(this.#threadTopic(key), { runId })
              .catch(() => {});
          } catch {
            // A terminal broker failure cannot retain the synthetic run and
            // lease indefinitely; local teardown remains bounded and truthful.
          } finally {
            state.threadRunsByStreamId.delete(streamId);
            if (state.threadRunsById.get(runId) === record) {
              state.threadRunsById.delete(runId);
              state.threadKeysByRunId.delete(runId);
            }
            if (state.activeThreadRunIds.get(key) === runId && state.activeThreadStreamIds.get(key) === streamId) {
              state.activeThreadRunIds.delete(key);
              state.activeThreadStreamIds.delete(key);
            }
            // Contenders park on this run's reservation lifecycle (fork
            // waitForCrossAgentThreadRun); wake them now that it cleared.
            this.#resolveReservationWaiters(state, runId);
            await this.#releaseThreadLeaseOwner(this.#getPubSub(pubsub), key, runId, leaseOwner).catch(() => {});
          }
        })();
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
    if (signal.transient) return;

    await this.#persistSignal(agent, signal, resourceId, threadId, requestContext);
    this.#broadcastPersistedSignal(state, pubsub, key, runId, agent.id, signal, resourceId, threadId);
  }

  /**
   * Evict SUSPENDED records parked longer than {@link AGENT_SUSPENDED_RUN_TTL_MS}.
   * Called lazily on each registration so cleanup is proportional to activity and
   * zero-cost when idle — mirrors the internal-workflow registry sweep. Bounds the
   * records left behind by abandoned suspends and by resumes that land on a
   * different instance (which never clean the origin instance's record).
   *
   * An expiring record only proves that this runtime no longer owns warm state.
   * Another instance may have resumed the same runId and taken over its lease, so
   * cleanup must remain local: stop this runtime's renewal timer and let an
   * abandoned lease expire naturally instead of releasing or broadcasting a
   * terminal event that could disrupt the resumed run.
   */
  #sweepStaleSuspendedRecords(state: AgentThreadRuntimeState, pubsub: PubSub | undefined) {
    const now = Date.now();
    for (const [streamId, record] of state.threadRunsByStreamId) {
      if (record.lifecycle !== 'suspended' || record.suspendedAt === undefined) continue;
      if (now - record.suspendedAt <= AGENT_SUSPENDED_RUN_TTL_MS) continue;
      state.threadRunsByStreamId.delete(streamId);
      state.watchedThreadStreamIds.delete(streamId);
      // A same-instance resume re-registers the run under a newer streamId, so a
      // record that is no longer the run's current record is just the superseded
      // older stream: dropping its stream entry above is enough. Only the current
      // record (an abandoned suspend) gets the full run-level teardown below.
      if (state.threadRunsById.get(record.runId) !== record) continue;
      const staleKey = this.#threadKey(record.resourceId, record.threadId);
      state.threadRunsById.delete(record.runId);
      state.threadKeysByRunId.delete(record.runId);
      this.#clearSuspendedRun(state, record.runId);
      this.#stopLeaseRenewal(this.#getPubSub(pubsub), record.runId);
      // Remove the retired token from local reuse while leaving any distributed
      // lease untouched. Another instance may already own the resumed run, and an
      // abandoned lease will expire naturally now that renewal has stopped.
      if (state.leaseOwnerTokensByRunId.get(record.runId) === record.leaseOwner) {
        state.leaseOwnerTokensByRunId.delete(record.runId);
      }
      if (
        state.activeThreadRunIds.get(staleKey) === record.runId &&
        state.activeThreadStreamIds.get(staleKey) === streamId
      ) {
        state.activeThreadRunIds.delete(staleKey);
        state.activeThreadStreamIds.delete(staleKey);
      }
      this.#releaseUnusedThreadControlSubscription(state, staleKey);
    }
  }

  continueRun<OUTPUT>(
    agent: Agent<any, any, any, any>,
    output: MastraModelOutput<OUTPUT>,
    streamOptions: AgentExecutionOptions<OUTPUT>,
    pubsub?: PubSub,
  ): boolean {
    const { threadId, resourceId } = this.#getThreadTarget(streamOptions);
    if (!threadId) return false;

    const state = this.#getState(pubsub);
    this.#sweepStaleSuspendedRecords(state, pubsub);
    const key = this.#threadKey(resourceId, threadId);
    const existing = state.threadRunsById.get(output.runId);
    if (
      !existing?.continuation?.canContinue() ||
      existing.agent !== agent ||
      existing.threadId !== threadId ||
      existing.resourceId !== resourceId ||
      state.activeThreadRunIds.get(key) !== output.runId ||
      state.activeThreadStreamIds.get(key) !== existing.streamId ||
      (output.status !== 'running' && output.status !== 'suspended') ||
      existing.lifecycle === 'completed' ||
      existing.lifecycle === 'failed' ||
      existing.lifecycle === 'aborted'
    ) {
      return false;
    }

    const resumedToolCallId = (streamOptions as AgentExecutionOptions<OUTPUT> & { toolCallId?: string }).toolCallId;
    if (resumedToolCallId) this.#clearSuspendedToolCall(state, output.runId, resumedToolCallId);
    else this.#clearSuspendedRun(state, output.runId);
    existing.lifecycle = 'running';
    existing.streamOptions = streamOptions;
    existing.currentSegmentOutput = output;
    // Resume callers may only send an approval and never read this segment.
    // Drain its native output so completion and subsequent approvals advance.
    void output.consumeStream();
    return true;
  }

  closeRunContinuation<OUTPUT>(ownerOutput: MastraModelOutput<OUTPUT>, pubsub?: PubSub): boolean {
    const record = this.#getState(pubsub).threadRunsById.get(ownerOutput.runId);
    if (
      !record?.continuation ||
      (record.continuation.sourceOutput !== ownerOutput && record.currentSegmentOutput !== ownerOutput)
    ) {
      return false;
    }
    record.continuation = undefined;
    return true;
  }

  registerRun<OUTPUT>(
    agent: Agent<any, any, any, any>,
    output: MastraModelOutput<OUTPUT>,
    streamOptions: AgentExecutionOptions<OUTPUT>,
    pubsub: PubSub | undefined,
    registrationOptions: AgentThreadStrictRegistrationOptions,
  ): Promise<AgentThreadRunRegistration> | undefined;
  registerRun<OUTPUT>(
    agent: Agent<any, any, any, any>,
    output: MastraModelOutput<OUTPUT>,
    streamOptions: AgentExecutionOptions<OUTPUT>,
    pubsub?: PubSub,
    registrationOptions?: AgentThreadStreamRegistrationOptions,
  ): Promise<void> | undefined;
  registerRun<OUTPUT>(
    agent: Agent<any, any, any, any>,
    output: MastraModelOutput<OUTPUT>,
    streamOptions: AgentExecutionOptions<OUTPUT>,
    pubsub?: PubSub,
    registrationOptions?: AgentThreadStrictRegistrationOptions | AgentThreadStreamRegistrationOptions,
  ): Promise<void | AgentThreadRunRegistration> | undefined {
    if (hasReadOnlyMemory(streamOptions)) return;

    const { threadId, resourceId } = this.#getThreadTarget(streamOptions);
    if (!threadId) return;

    if (registrationOptions?.strict) {
      return this.#registerRunStrict(agent, output, streamOptions, pubsub, threadId, resourceId, registrationOptions);
    }

    const state = this.#getState(pubsub);
    this.#sweepStaleSuspendedRecords(state, pubsub);
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
    const leaseOwner = this.#leaseOwnerForRun(state, output.runId);
    const {
      output: outputForSubscribers,
      createSubscriberStream,
      startBroadcast,
      markAbortRequested,
      abortBroadcast,
      canContinueBroadcast,
      broadcastFinished,
    } = this.#withBroadcastStream(output, pubsub, key, streamId, streamSeq > 1);
    const resumedToolCallId = (streamOptions as AgentExecutionOptions<OUTPUT> & { toolCallId?: string }).toolCallId;
    if (resumedToolCallId) {
      this.#clearSuspendedToolCall(state, output.runId, resumedToolCallId);
    } else {
      this.#clearSuspendedRun(state, output.runId);
    }
    const record: AgentThreadRunRecord<OUTPUT> = {
      agent,
      output: outputForSubscribers,
      currentSegmentOutput: output,
      runId: output.runId,
      streamId,
      streamSeq,
      lifecycle: 'running',
      threadId,
      resourceId,
      streamOptions: streamOptions as AgentThreadRunRecord<OUTPUT>['streamOptions'],
      createSubscriberStream,
      markAbortRequested,
      abortBroadcast,
      leaseOwner,
      suspensions: state.suspensionMetadataByRunId.get(output.runId),
      broadcastFinished,
      continuation:
        registrationOptions?.continuation === 'across-suspension'
          ? { sourceOutput: output, canContinue: canContinueBroadcast }
          : undefined,
    };

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
    const resolvedPubSub = this.#getPubSub(pubsub);
    // Registration is part of the subscriber's delivery barrier just like the
    // terminal event. Normalize its rejection so a provider run that already
    // executed cannot become retryable merely because PubSub rejected the
    // segment's run-registered publication.
    const registrationPublish = (async () => {
      await this.#ensureThreadControlSubscription(state, resolvedPubSub, key).ready;
      // Every thread-bound run must hold the cross-process lease while it is
      // live: the liveness checks (markActiveIfLive / #waitForRemoteRunToFinish)
      // treat a lease-less run as a ghost, so a plain `agent.stream()` run that
      // never acquired would let contending instances start competing runs
      // instead of serializing behind it. Acquire BEFORE publishing
      // `run-registered` so an observer that checks liveness on receipt finds
      // the lease held. Acquire under the run's retained owner TOKEN
      // (#leaseOwnerForRun) — never the raw run id — so a signal-woken run that
      // already holds the lease under its token just refreshes idempotently,
      // and the completion drain's transfer/release find the matching owner.
      // Fail-open on loss or error (simultaneous-start race): proceed and never
      // roll back the local registration — matches pre-lease semantics and
      // sendSignal's documented fail-open rationale. A thrown acquire
      // (transient provider error) is treated as acquired so renewal starts: if
      // the acquire landed server-side but the response failed, skipping
      // renewal would let the lease expire mid-run; renewal self-stops when we
      // don't own the key.
      const lease = await this.#getLeaseProvider(resolvedPubSub)
        .acquireLease(key, leaseOwner, AGENT_THREAD_LEASE_TTL_MS)
        .catch(() => ({ acquired: true as boolean }));
      if (lease.acquired) {
        this.#startLeaseRenewal(resolvedPubSub, key, output.runId);
      } else if (state.leaseOwnerTokensByRunId.get(output.runId) === leaseOwner) {
        // Another owner genuinely holds the key. Never publish a registration
        // signed by the losing token: remote subscribers authenticate this
        // exact owner before projecting a live segment.
        state.leaseOwnerTokensByRunId.delete(output.runId);
        const ownershipError = new AgentThreadLeaseOwnershipLostError(
          'registration-publish-failed',
          `Agent thread run ${output.runId} did not acquire its exact lease owner`,
        );
        // The provider output may already exist, but a process that lost this
        // exact lease must not execute or publish any part of the run.
        this.abortRun(output.runId, resolvedPubSub);
        throw ownershipError;
      }
      await this.#publishRegistrationAndWait(pubsub, key, {
        type: 'run-registered',
        runId: output.runId,
        streamId,
        streamSeq,
        sourceId: this.#getSourceId(),
        leaseOwner,
      });
    })();
    // The Harness output-drain waiter may not attach until the model has already
    // produced FullOutput. Mark the promise observed immediately while retaining
    // the original rejecting promise below for that later waiter.
    void registrationPublish.catch(() => {});
    this.#threadOutputRegistrations.set(output, registrationPublish);
    state.registrationPublishesByStreamId.set(streamId, registrationPublish);
    // Always drive the run's stream to completion, even when no caller consumes
    // the returned output (e.g. a fire-and-forget schedule wake). The broadcast
    // tee buffers every part, so a later/external subscriber still replays the
    // full stream; without this pump the run never reaches a terminal state and
    // its active-run record + thread lease would never release, permanently
    // wedging the thread. The broadcast promise settles when the parts drain
    // finishes; the completion watcher gates `run-completed` on it (PR #202/#204).
    const broadcast = registrationPublish.then(startBroadcast, error => {
      if (error instanceof AgentThreadLeaseOwnershipLostError) throw error;
      return startBroadcast();
    });
    state.broadcastsByStreamId.set(streamId, broadcast);
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
      // A cancelled attempt whose settlement hit an infrastructure failure
      // keeps its abort message; the retained original error rides along as
      // `Error.cause` (same-attempt only — ordinary aborts carry no cause).
      const retained = state.rejectedRunErrorsByRunId.get(runId);
      const abortedError = new Error(`Agent thread run id "${runId}" has been aborted`);
      if (retained?.infrastructure) abortedError.cause = retained.error;
      return Promise.reject(abortedError);
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

  async #registerRunStrict<OUTPUT>(
    agent: Agent<any, any, any, any>,
    output: MastraModelOutput<OUTPUT>,
    streamOptions: AgentExecutionOptions<OUTPUT>,
    pubsub: PubSub | undefined,
    threadId: string,
    resourceId: string | undefined,
    registrationOptions: AgentThreadStrictRegistrationOptions,
  ): Promise<AgentThreadRunRegistration> {
    await registrationOptions.validate?.();

    const state = this.#getState(pubsub);
    this.#sweepStaleSuspendedRecords(state, pubsub);
    const key = this.#threadKey(resourceId, threadId);
    const activeRunId = state.activeThreadRunIds.get(key);
    const activeRecord = activeRunId ? state.threadRunsById.get(activeRunId) : undefined;
    if (activeRecord && activeRecord.runId !== output.runId && this.#isThreadBlockingRun(state, activeRecord)) {
      throw new Error(`Cannot register run ${output.runId}: thread is already active with run ${activeRunId}`);
    }
    const resolvedPubSub = this.#getPubSub(pubsub);
    const leaseProvider = this.#getLeaseProvider(resolvedPubSub);
    // Acquire under this run's retained owner TOKEN (#leaseOwnerForRun), never
    // the raw run id: renewal (#startLeaseRenewal), release
    // (#releaseThreadLease) and handoff (#transferThreadLease) all resolve the
    // owner through `leaseOwnerTokensByRunId`, and remote subscribers
    // authenticate a segment against the exact owner that signed it. Acquiring
    // under the raw run id would make every one of those lookups miss.
    // Unlike upstream's raw-run-id owner, this token is process-attempt-unique,
    // so a concurrent recovery of the same runId in another process loses the
    // acquire outright and fails closed here instead of silently sharing an
    // indistinguishable lease.
    const leaseOwner = this.#leaseOwnerForRun(state, output.runId);
    const lease = await leaseProvider.acquireLease(key, leaseOwner, AGENT_THREAD_LEASE_TTL_MS);
    if (!lease.acquired) {
      if (state.leaseOwnerTokensByRunId.get(output.runId) === leaseOwner) {
        state.leaseOwnerTokensByRunId.delete(output.runId);
      }
      throw new AgentThreadLeaseConflictError(output.runId, lease.owner ?? 'another owner');
    }

    // A failed external-ownership validation means another recovery attempt may
    // already own this same runId. Do not release this thread lease; its TTL or
    // the successor registration (which reuses the retained owner token) will
    // take over renewal.
    await registrationOptions.validate?.();

    this.#startLeaseRenewal(resolvedPubSub, key, output.runId);
    const { streamId, streamSeq } = this.#nextStreamIdentity(state, output.runId);
    const {
      output: outputForSubscribers,
      createSubscriberStream,
      startBroadcast,
      cancelBroadcast,
      canContinueBroadcast,
      broadcastFinished,
    } = this.#withBroadcastStream(output, pubsub, key, streamId, streamSeq > 1);
    const record: AgentThreadRunRecord<OUTPUT> = {
      agent,
      output: outputForSubscribers,
      currentSegmentOutput: output,
      runId: output.runId,
      streamId,
      streamSeq,
      lifecycle: 'running',
      threadId,
      resourceId,
      streamOptions: streamOptions as AgentThreadRunRecord<OUTPUT>['streamOptions'],
      createSubscriberStream,
      // Exactly the owner token the lease above was acquired with — this is the
      // signature every stream-part and lifecycle terminal for this segment
      // carries, and the one remote subscribers authenticate against.
      leaseOwner,
      suspensions: state.suspensionMetadataByRunId.get(output.runId),
      broadcastFinished,
      continuation:
        registrationOptions.continuation === 'across-suspension'
          ? { sourceOutput: output, canContinue: canContinueBroadcast }
          : undefined,
    };

    let rolledBack = false;
    let registrationPublished = false;
    let completionWatcherDisabled = false;
    const rollback = async ({ releaseLease = true }: { releaseLease?: boolean } = {}) => {
      if (rolledBack) return;
      rolledBack = true;
      completionWatcherDisabled = true;
      await cancelBroadcast();

      const ownsCurrentRecord = state.threadRunsById.get(record.runId) === record;
      if (state.threadRunsByStreamId.get(record.streamId) === record) {
        state.threadRunsByStreamId.delete(record.streamId);
      }
      state.watchedThreadStreamIds.delete(record.streamId);
      if (ownsCurrentRecord) {
        state.threadRunsById.delete(record.runId);
        if (state.threadKeysByRunId.get(record.runId) === key) {
          state.threadKeysByRunId.delete(record.runId);
        }
      }
      if (
        state.activeThreadRunIds.get(key) === record.runId &&
        state.activeThreadStreamIds.get(key) === record.streamId
      ) {
        state.activeThreadRunIds.delete(key);
        state.activeThreadStreamIds.delete(key);
      }

      // Renewal is keyed by runId, so only the current record may stop or
      // release it; an older rollback must not disturb a newer registration.
      if (ownsCurrentRecord) {
        if (releaseLease) {
          // Release under the exact owner token the lease was acquired with;
          // releasing under the raw run id is a no-op for a token-owned key.
          // This also drops the retained token so the run cannot be resurrected
          // under a stale owner.
          await this.#releaseThreadLeaseOwner(resolvedPubSub, key, record.runId, leaseOwner).catch(() => {});
        } else {
          // Keep both the lease and its retained owner token so the successor
          // registration for this same run re-acquires idempotently under the
          // same owner and resumes renewal.
          this.#stopLeaseRenewal(resolvedPubSub, record.runId);
        }
      }
      if (registrationPublished) await discardPublishedRegistration().catch(() => {});
      this.#releaseUnusedThreadControlSubscription(state, key);
    };

    const discardPublishedRegistration = async () => {
      await this.#publishAndWait(pubsub, key, {
        type: 'run-discarded',
        runId: record.runId,
        streamId: record.streamId,
        leaseOwner,
      });
    };

    state.threadRunsById.set(output.runId, record);
    state.threadRunsByStreamId.set(streamId, record);
    state.threadKeysByRunId.set(output.runId, key);
    state.activeThreadRunIds.set(key, output.runId);
    state.activeThreadStreamIds.set(key, streamId);

    try {
      await this.#ensureThreadControlSubscription(state, resolvedPubSub, key).ready;
      await this.#publishAndWait(pubsub, key, {
        type: 'run-registered',
        runId: output.runId,
        streamId,
        streamSeq,
        sourceId: this.#getSourceId(),
        leaseOwner,
      });
      registrationPublished = true;
      await registrationOptions.validate?.();
    } catch (error) {
      if (!rolledBack) {
        let releaseLease = true;
        try {
          await registrationOptions.validate?.();
        } catch {
          releaseLease = false;
        }
        await rollback({ releaseLease });
        // A durable backend may accept/deliver the registration and still fail
        // its acknowledgement. `rollback` compensates every confirmed publish;
        // an ambiguous acknowledgement also gets an idempotent discard here.
        if (!registrationPublished) await discardPublishedRegistration().catch(() => {});
      }
      throw error;
    }

    const resumedToolCallId = (streamOptions as AgentExecutionOptions<OUTPUT> & { toolCallId?: string }).toolCallId;
    if (resumedToolCallId) {
      this.#clearSuspendedToolCall(state, output.runId, resumedToolCallId);
    } else {
      this.#clearSuspendedRun(state, output.runId);
    }
    // Fire-and-forget, matching every other fork call site for this method: the fork
    // made it promise-returning, upstream calls it synchronously.
    void this.#watchThreadRunCompletion(state, pubsub, key, record, undefined, () => completionWatcherDisabled);
    // Register the drain promise the way the non-strict path does so the
    // completion watcher's terminal gate still waits for every stream-part
    // publish before `run-completed` goes on the wire.
    const broadcast = startBroadcast();
    state.broadcastsByStreamId.set(streamId, broadcast);
    void broadcast.catch(() => {});
    return { rollback };
  }

  #watchThreadRunCompletion(
    state: AgentThreadRuntimeState,
    pubsub: PubSub | undefined,
    key: string,
    record: AgentThreadRunRecord<any>,
    /**
     * Publication of this run's `run-registered`, when the caller published it
     * outside `state.registrationPublishesByStreamId` (the strict recovery
     * registration path). Terminal delivery waits on it so `run-completed`
     * cannot overtake `run-registered` on the wire.
     */
    registered?: Promise<unknown>,
    /** A rolled-back registration must not publish any lifecycle terminal. */
    isDisabled?: () => boolean,
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

    let completionSettled = false;
    let completionTerminalPublished = false;
    let abortRequested = false;
    let resolveAbortCompletion!: () => void;
    const abortCompletion = new Promise<void>(resolve => {
      resolveAbortCompletion = resolve;
    });
    record.finalizeAbort = () => {
      if (abortRequested || record.lifecycle === 'aborted') return false;
      // Once completion has won and published its terminal, its exact
      // publication owns the run: a late abort may still signal the provider,
      // but cannot schedule a competing run-aborted lifecycle terminal. The
      // narrowly scoped generic-suspension window is exempt — the completion
      // callback is finalizing a suspended run whose terminal is not on the
      // wire yet, so a genuine cancellation still wins finalization; its abort
      // fence orders behind the outstanding run-suspended publication and the
      // watcher stays the sole finalizer until the parked marker lands.
      if (completionSettled) {
        const inGenericSuspensionWindow =
          !completionTerminalPublished &&
          !record.parked &&
          record.suspensionPublication !== undefined &&
          !state.approvalSuspendedRunIds.has(record.runId);
        if (!inGenericSuspensionWindow) return false;
      }
      record.lifecycle = 'aborted';
      abortRequested = true;
      resolveAbortCompletion();
      return true;
    };

    const providerCompletion = record.output._waitUntilFinished();
    const completionSignal = new Promise<void>((resolve, reject) => {
      providerCompletion.then(resolve, error => {
        // Cancellation can reject synchronously when the abort signal fires.
        // Abort intent was marked before provider abort, so route that rejection
        // into finalization while preserving non-abort provider failures.
        if (abortRequested || record.lifecycle === 'aborted') resolve();
        else reject(error);
      });
      void abortCompletion.then(resolve);
    });
    const cleanupStreamBarriers = () => {
      state.registrationPublishesByStreamId.delete(record.streamId);
      state.broadcastsByStreamId.delete(record.streamId);
    };
    const completion = completionSignal
      .then(
        async () => {
          completionSettled = true;
          record.providerSettled = true;
          try {
            // Gate finalization on the registration publish + the stream-part
            // broadcast settling (PR #202/#204): publishing `run-completed` off
            // `_waitUntilFinished()` alone let remote subscribers observe completion
            // FIRST, delete the remote run, and ignore the late parts.
            await state.registrationPublishesByStreamId.get(record.streamId)?.catch(() => {});
            state.registrationPublishesByStreamId.delete(record.streamId);
            // Callers that published `run-registered` outside the stream-barrier
            // map (strict recovery registration) hand their publish in directly.
            if (registered) await Promise.allSettled([registered]);
            await state.broadcastsByStreamId.get(record.streamId)?.catch(() => {});
            state.broadcastsByStreamId.delete(record.streamId);
            state.watchedThreadStreamIds.delete(record.streamId);
            // A registration rolled back by its owner has already been discarded
            // on the wire; publishing a lifecycle terminal for it would resurrect
            // a segment the successor now owns.
            if (isDisabled?.()) {
              resolveTerminal();
              return;
            }
            const abortedTerminal = abortRequested || record.lifecycle === 'aborted';

            if (abortedTerminal) {
              await record.abortDelivery;
              record.lifecycle = 'aborted';
              this.#clearSuspendedRun(state, record.runId);
              this.#forgetCallerSignalsForRun(state, record.runId);
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
              try {
                // A foreign winner projected before this abort (its
                // registration set the active identity while this run was
                // still finishing) takes the same forwarding-only
                // disposition as the normal completion handoff below: the
                // ordinary drain would return immediately on the winner's
                // active identity and strand the queued input. Ordinary
                // guards stay intact: without a positively verified foreign
                // projection the original drain/release path runs unchanged.
                const forwarded = await this.#forwardProjectedWinnerAtHandoff(state, pubsub, key, record);
                if (!forwarded) {
                  if (this.#hasPendingThreadWork(state, key)) {
                    await this.#drainPendingSignals(state, pubsub, key, record);
                  } else {
                    this.#releaseThreadLease(pubsub, key, record.runId);
                  }
                }
              } finally {
                if (state.threadRunsById.get(record.runId) === record) {
                  state.threadRunsById.delete(record.runId);
                }
                this.#resolveReservationWaiters(state, record.runId);
                // A failed follow-up drain rethrows its setup failure; the
                // aborted run's prepared state must still be released, and the
                // thread's control listener re-checked once it is gone.
                this.#cleanupPreparedRun(state, record.runId, true);
                this.#releaseUnusedThreadControlSubscription(state, key);
              }
              return;
            }

            this.#cleanupPreparedRun(state, record.runId);
            // A suspended run (approval or generic tool suspension) is paused, not
            // finished: surface run-suspended and leave its records in place so a
            // later resume can re-attach to the same thread.
            if (record.output.status === 'suspended' && this.#isSuspendedRun(state, record.runId)) {
              record.lifecycle = 'suspended';
              record.suspendedAt = Date.now();
              // Install the pending-suspension-publication receipt BEFORE
              // invoking the publication so synchronous abort re-entry can see
              // it and order its fence behind the publication. The receipt is
              // record-local; the watcher's terminal promise is never awaited
              // by that fence.
              let markSuspensionPublished!: () => void;
              const suspensionPublication = new Promise<void>(resolve => {
                markSuspensionPublished = resolve;
              });
              record.suspensionPublication = suspensionPublication;
              try {
                await this.#publishTerminalAndWait(pubsub, key, {
                  type: 'run-suspended',
                  runId: record.runId,
                  streamId: record.streamId,
                  leaseOwner: record.leaseOwner,
                });
              } finally {
                if (record.suspensionPublication === suspensionPublication) {
                  record.suspensionPublication = undefined;
                }
                markSuspensionPublished();
              }
              // After the publication settles, recheck cancellation instead of
              // blindly parking. An abort that won during the window still
              // finds this watcher as the sole finalizer: release the run the
              // way the aborted-terminal path does, deferring the
              // run-aborting/run-aborted terminals to the abort chain (whose
              // fence awaited this very publication) rather than republishing.
              // SAFETY: re-widen the property read so the comparison sees the
              // full declared union. CFA still holds the pre-await 'suspended'
              // narrowing, but the abort chain may write 'aborted' while the
              // publication is in flight — the recheck is against that mutation.
              const lifecycleAfterPublication = record.lifecycle as AgentThreadRunLifecycle;
              if (abortRequested || lifecycleAfterPublication === 'aborted') {
                record.lifecycle = 'aborted';
                this.#clearSuspendedRun(state, record.runId);
                this.#forgetCallerSignalsForRun(state, record.runId);
                state.threadRunsByStreamId.delete(record.streamId);
                if (state.threadRunsById.get(record.runId) === record) {
                  state.threadRunsById.delete(record.runId);
                }
                if (state.threadKeysByRunId.get(record.runId) === key) {
                  state.threadKeysByRunId.delete(record.runId);
                }
                if (
                  state.activeThreadRunIds.get(key) === record.runId &&
                  state.activeThreadStreamIds.get(key) === record.streamId
                ) {
                  state.activeThreadRunIds.delete(key);
                  state.activeThreadStreamIds.delete(key);
                }
                try {
                  await record.abortDelivery;
                } catch {
                  // The abort chain retains its own failure evidence;
                  // this cleanup must still complete.
                }
                try {
                  if (this.#hasPendingThreadWork(state, key)) {
                    await this.#drainPendingSignals(state, pubsub, key, record);
                  } else {
                    this.#releaseThreadLease(pubsub, key, record.runId);
                  }
                } finally {
                  this.#resolveReservationWaiters(state, record.runId);
                }
                this.#cleanupPreparedRun(state, record.runId, true);
                resolveTerminal();
                return;
              }
              // Successful publication, exact-generation revalidation and the
              // watcher's last run/thread-shared effects are done: only now
              // does the parked marker transfer finalization ownership to
              // #releaseParkedRun. A record replaced by a successor
              // registration is never marked.
              if (state.threadRunsById.get(record.runId) === record) {
                record.parked = true;
              }
              resolveTerminal();
              return;
            }

            record.lifecycle = 'completed';
            completionTerminalPublished = true;
            this.#clearSuspendedRun(state, record.runId);
            this.#forgetCallerSignalsForRun(state, record.runId);
            await this.#publishTerminalAndWait(pubsub, key, {
              type: 'run-completed',
              runId: record.runId,
              streamId: record.streamId,
              // Origin-side truth for replay filtering: only a successful run
              // flushed its messages to storage, so only its retained chunks are
              // backed by a persisted message and safe to replay to fresh
              // subscribers.
              persisted: record.output.status === 'success',
              status: record.output.status,
              leaseOwner: record.leaseOwner,
            });
            resolveTerminal();
            // Saved runs can leave the topic once their terminal landed;
            // failed runs trim their unsaved tail. Suspended runs keep
            // everything for resume replay.
            if (record.output.status === 'success') {
              void this.#trimSavedRun(pubsub, key, record).catch(() => {});
            } else if (record.output.status !== 'suspended') {
              // #trimFailedRun is synchronous: it schedules the delayed
              // #trimSavedRun internally, so there is no promise to catch.
              this.#trimFailedRun(pubsub, key, record);
            }
            state.threadRunsByStreamId.delete(record.streamId);
            if (
              state.activeThreadRunIds.get(key) === record.runId &&
              state.activeThreadStreamIds.get(key) === record.streamId
            ) {
              state.activeThreadRunIds.delete(key);
              state.activeThreadStreamIds.delete(key);
            }
            // Retain threadKeysByRunId through the queued-work handoff so the
            // finishing run's exact lease-owner token remains available to the
            // atomic transfer. Cleanup it only after drain/release completes.
            // If queued follow-up work exists, keep the cross-process lease held by
            // handing it to the next run instead of releasing it: releasing here
            // would briefly empty the lease key, letting a racing process win it and
            // start a competing run on this thread. The drain runs under the
            // transferred lease and releases it only once every queue is empty. If
            // there's no pending work, release as usual so other processes can wake
            // the thread.
            try {
              // A foreign winner projected before this completion (its
              // registration set the active identity while this run was
              // still finishing) bypasses the drain's active-run guard
              // below, so route it through the explicit forwarding-only
              // disposition. Ordinary guards stay intact: without a
              // positively verified foreign projection the original
              // drain/release path runs unchanged. The aborted-terminal
              // handoff above shares this same disposition.
              const forwarded = await this.#forwardProjectedWinnerAtHandoff(state, pubsub, key, record);
              if (!forwarded) {
                if (this.#hasPendingThreadWork(state, key)) {
                  await this.#drainPendingSignals(state, pubsub, key, record);
                } else {
                  this.#releaseThreadLease(pubsub, key, record.runId);
                }
              }
            } finally {
              if (state.threadKeysByRunId.get(record.runId) === key) {
                state.threadKeysByRunId.delete(record.runId);
              }
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
        },
        error => {
          const providerError = getErrorFromUnknown(error);
          rejectTerminal(
            error instanceof AgentThreadOutputDrainError
              ? error
              : new AgentThreadOutputDrainError(
                  'terminal-publish-failed',
                  `Agent thread run ${record.runId} failed before terminal delivery`,
                  error,
                ),
          );
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
          if (state.threadKeysByRunId.get(record.runId) === key) state.threadKeysByRunId.delete(record.runId);
          if (state.threadRunsById.get(record.runId) === record) state.threadRunsById.delete(record.runId);
          this.#cleanupPreparedRun(state, record.runId);
          this.#forgetCallerSignalsForRun(state, record.runId);
          // Registration now awaits the control subscription before acquiring
          // the lease (upstream), so a provider that rejects immediately can
          // settle first; release only once that acquisition has settled, or
          // the lease it later acquires would never be released.
          const pendingRegistration = state.registrationPublishesByStreamId.get(record.streamId);
          if (pendingRegistration) {
            void pendingRegistration.catch(() => {}).then(() => this.#releaseThreadLease(pubsub, key, record.runId));
          } else {
            this.#releaseThreadLease(pubsub, key, record.runId);
          }
          this.#resolveReservationWaiters(state, record.runId);
          this.#rememberRejectedRunError(state, record.runId, providerError);
          void this.#drainPendingIdleSignals(state, pubsub, key).catch(() => {});
          throw error;
        },
      )
      .finally(cleanupStreamBarriers);
    void completion.catch(() => {});
    return completion;
  }

  /**
   * Once a run saved successfully and the agent has storage, delete every topic
   * entry published with its runId. Matching by runId (not tracked entry IDs)
   * also removes entries published before a restart, e.g. the suspended half of
   * a resumed run. Other runs' entries are never touched.
   */
  async #trimSavedRun(
    pubsub: PubSub | undefined,
    key: string,
    record: Pick<AgentThreadRunRecord<any>, 'agent' | 'streamOptions' | 'runId'>,
  ) {
    // Without storage the topic is the only copy, so it stays.
    // Fork (PF-2238/PF-4402): a real Agent answers from its configuration so a
    // dynamic memory factory is not invoked again after the execution resolved it.
    const hasEffectiveMemory = (record.agent as { __hasEffectiveMemory?: (rc?: unknown) => boolean })
      .__hasEffectiveMemory;
    const memory =
      typeof hasEffectiveMemory === 'function'
        ? hasEffectiveMemory.call(record.agent, record.streamOptions.requestContext)
        : await record.agent.getMemory?.({ requestContext: record.streamOptions.requestContext });
    if (!memory) return;
    await this.#getPubSub(pubsub).trimTopic(this.#threadTopic(key), { runId: record.runId });
  }

  /**
   * A run that failed, was canceled, or was aborted saved nothing that its
   * topic entries could be replayed against, so reconnecting subscribers ignore
   * them. They only matter to subscribers already reading the run live, so
   * delete them once those have had time to read the outcome.
   */
  #trimFailedRun(
    pubsub: PubSub | undefined,
    key: string,
    record: Pick<AgentThreadRunRecord<any>, 'agent' | 'streamOptions' | 'runId'>,
  ) {
    const timer = setTimeout(() => {
      this.#trimSavedRun(pubsub, key, record).catch(() => {});
    }, FAILED_RUN_TRIM_DELAY_MS);
    timer.unref?.();
  }

  async #drainPendingSignals(
    state: AgentThreadRuntimeState,
    pubsub: PubSub | undefined,
    key: string,
    previousRun: Pick<AgentThreadRunRecord<any>, 'agent' | 'streamOptions' | 'runId' | 'resourceId' | 'threadId'>,
  ) {
    if (state.unresolvedForwardingsByThread.has(key)) {
      // Fail-closed: a committed forward on this thread rejected with
      // unknown admission. The ambiguous item and everything behind it are
      // fenced from ordinary completion drains — an owner change or natural
      // trigger is not evidence of nondelivery, so nothing here may consume
      // past it or start a competing run.
      return;
    }
    if (state.activeThreadRunIds.has(key)) {
      return;
    }
    if (state.drainingPendingSignalsByThread.has(key)) {
      // An outstanding handoff cannot be overwritten by a competing drain:
      // the drain that captured a follow-up signal owns its settlement and
      // the queue continuation. Retained work waits for the next natural
      // trigger instead of racing the outstanding lease operation.
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
    let signal: CreatedAgentSignal | undefined;
    let nextRunId: string | undefined;
    let draining: { signal: CreatedAgentSignal; cancelled: boolean; handedOff: boolean } | undefined;
    // Ownership across the lease await: assigned only after the
    // transfer/acquisition settles, so the catch can distinguish a positively
    // acquired attempt (startup failed after acquisition) from a lease
    // operation that failed with an unknown outcome. A generated `nextRunId`
    // or a token-map entry alone does not prove acquisition.
    let leaseOutcome: { acquired: boolean; owner?: string; ownerToken?: string; error?: unknown } | undefined;
    // Immutable operation provenance, captured BEFORE the lease operation's
    // provider await: the finished predecessor's exact token, renewal timer
    // and run record, plus the failed attempt's candidate token. Post-await
    // cleanup reconciles and fences against these instead of rereading the
    // mutable maps by run id, so a successor installed for either run id
    // during the operation — including one that re-adopts the retained
    // predecessor token — can never be mistaken for this attempt.
    let attemptProvenance: ThreadLeaseOperationProvenance | undefined;
    let predecessorProvenance: ThreadLeaseOperationProvenance | undefined;
    try {
      signal = queue?.shift();
      if (signal && queue) {
        if (queue.length === 0) {
          state.pendingSignalsByThread.delete(key);
        }

        // Hand the lease from the finished run to this drained run before
        // streaming, so the lease key never goes empty during the handoff. If the
        // old owner already lost the lease (e.g. a pubsub blip let the TTL lapse
        // and another process took over), forward the signal to the new winner
        // instead of starting a competing run here.
        nextRunId = globalThis.crypto.randomUUID();
        state.activeThreadRunIds.set(key, nextRunId);
        state.threadKeysByRunId.set(nextRunId, key);
        draining = { signal, cancelled: false, handedOff: false };
        state.drainingPendingSignalsByThread.set(key, draining);
        predecessorProvenance = this.#captureLeaseProvenance(state, previousRun.runId);
        attemptProvenance = this.#captureLeaseProvenance(state, nextRunId, this.#leaseOwnerForRun(state, nextRunId));
        // Fail-closed lease operation: an asynchronous rejection propagates
        // to the catch instead of collapsing into a verified loss that would
        // permit fallback acquisition after an ambiguous transfer.
        const owns = await this.#acquireOrTransferThreadLease(pubsub, key, nextRunId, previousRun.runId, {
          failClosed: true,
        });
        leaseOutcome = owns;
        this.#refreshLeaseProvenance(state, attemptProvenance);
        if (draining.cancelled) {
          if (state.activeThreadRunIds.get(key) === nextRunId) state.activeThreadRunIds.delete(key);
          state.threadKeysByRunId.delete(nextRunId);
          this.#cleanupPreparedRun(state, nextRunId);
          if (state.drainingPendingSignalsByThread.get(key) === draining) {
            state.drainingPendingSignalsByThread.delete(key);
          }
          await this.#drainPendingSignals(state, pubsub, key, {
            ...previousRun,
            runId: owns.acquired ? nextRunId : previousRun.runId,
          });
          return;
        }
        // The exclusive handoff marker and the selective-cancellation
        // handoff flag are both installed synchronously at the forwarding
        // publication's initiation inside the helper (see
        // `#forwardVerifiedLossHandoff`): the captured item stays exactly
        // cancellable through owner verification, and a competing handoff
        // cannot interleave once verification starts.
        if (!owns.acquired) {
          if (state.activeThreadRunIds.get(key) === nextRunId) {
            state.activeThreadRunIds.delete(key);
          }
          state.threadKeysByRunId.delete(nextRunId);
          // Pre-run input that arrived for the optimistic reservation during
          // lease settlement is preserved: the forwarding handoff below
          // incorporates the survivors into its ordered tail (ahead of the
          // pending tail). The winner rejects the original attempted-run
          // address, so deleting the local copies here would strand that
          // input instead of delivering it.
          if (owns.owner && owns.ownerToken) {
            // Positively verified foreign winner: the forwarding-only
            // disposition completes this item's publication and then
            // transfers the ordered surviving pre-run/pending tail and
            // eligible idle work to that exact winner. It never reserves,
            // acquires, transfers, renews or releases anything, never calls
            // `agent.stream`, and never overwrites the winner's projected
            // active/stream identity.
            // Narrowed const capture: `signal` is a `let` whose narrowing
            // does not persist into the closures below.
            const capturedSignal = signal;
            await this.#forwardVerifiedLossHandoff(
              state,
              pubsub,
              key,
              { owner: owns.ownerToken, runId: owns.owner },
              {
                signal: capturedSignal,
                queueKind: 'pending',
                receiptRunId: nextRunId,
                // Already shifted from the pending queue above; committing
                // the handoff flag synchronously at publication initiation
                // ends exact-item selective cancellation exactly when the
                // item leaves local ownership.
                commit: () => {
                  if (draining) draining.handedOff = true;
                },
                restore: () => {
                  const restored = state.pendingSignalsByThread.get(key) ?? [];
                  state.pendingSignalsByThread.set(key, [capturedSignal, ...restored]);
                },
                // Clear-pending (or selective cancellation, while the flag
                // above is still unset) marks the draining record during
                // the verification await; the helper rechecks before
                // publishing.
                isCancelled: () => draining?.cancelled === true,
              },
            );
          } else {
            const restored = state.pendingSignalsByThread.get(key) ?? [];
            state.pendingSignalsByThread.set(key, [signal, ...restored]);
          }
          return;
        }
        // Selective cancellation ends here for the starting run, but retain
        // clear-on-abort intent through startup recovery. (The loss branch
        // above commits its handoff flag later, inside the forwarding
        // helper at the publication's initiation, so the captured item
        // stays exactly cancellable through owner verification.)
        draining.handedOff = true;

        // The lease now belongs to nextRunId, so mirror that ownership in the
        // local reservation maps before Agent.stream performs its admission
        // checks. Passing the native owner marker lets Agent.stream adopt this
        // reservation instead of waiting on the run it is itself responsible for
        // starting.
        state.threadKeysByRunId.set(nextRunId, key);
        state.reservedAgentIdsByRunId.set(nextRunId, previousRun.agent.id);
        state.startingQueuedRunIds.add(nextRunId);

        const output = await previousRun.agent.stream(signal, {
          ...(previousRun.streamOptions as any),
          _pubsub: this.#getPubSub(pubsub),
          _threadRunReservationOwner: true,
          // Cancellation belongs to the finished run, not its queued follow-up.
          abortSignal: undefined,
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
      }
    } catch (error) {
      // Starting the follow-up run failed (e.g. a transient connection error
      // from `agent.stream`, or the lease transfer itself threw). Clean up the
      // failed run's state, restore the signal so it is not lost, publish this
      // segment's authenticated failure terminal, then hand the lease to
      // remaining queued work and only release once nothing is left to drain.
      // The restored signal is deliberately NOT re-drained here (that would
      // tight-loop against a still-broken upstream); it delivers on the next
      // natural drain trigger instead.
      const failedRunId = nextRunId ?? previousRun.runId;
      const runError = getErrorFromUnknown(error);
      if (nextRunId) {
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
        this.#rejectPendingOutputWaiters(state, nextRunId, runError);
      }
      if (signal && !draining?.cancelled) {
        // Restore through the map, not the local `queue` array: the shift above
        // deletes the map entry when it empties the queue, so the local array
        // may be detached from the map by the time we get here.
        state.pendingSignalsByThread.set(key, [signal, ...(state.pendingSignalsByThread.get(key) ?? [])]);
      }
      try {
        // Published under the failed segment's still-live lease owner, before
        // any release below, so subscribers can authenticate the terminal.
        await this.#publishTerminalAndWait(pubsub, key, {
          type: 'run-failed',
          runId: failedRunId,
          error: `failed to start follow-up run for queued message: ${runError.message}${draining?.cancelled ? '; the message was cancelled' : '; the message was requeued and will deliver on the next turn'}`,
          leaseOwner: attemptProvenance?.token ?? state.leaseOwnerTokensByRunId.get(failedRunId),
        });
      } catch {
        // The original setup error remains authoritative while the failed
        // segment still proceeds through bounded lease handoff cleanup.
      }
      if (previousRun.runId !== failedRunId) {
        this.#trimFailedRun(pubsub, key, { ...previousRun, runId: failedRunId });
      }
      // Exact captured provenance (read before the lease operation's provider
      // await): the failed attempt's candidate token and the finished
      // predecessor's retained token/timer/record are what the ownership-aware
      // disposition below reconciles and fences against — never a reread of
      // the mutable maps, which a successor installed during the outstanding
      // operation may already have replaced. The attempt's record identity is
      // refreshed after its own failed registration so the release fence only
      // blocks on successors adopted while the terminal publication and
      // reconciliation below are outstanding.
      this.#refreshLeaseProvenance(state, attemptProvenance);
      // Ownership-aware lease disposition. The unconditional predecessor
      // release is gone: whether this catch may release or forward anything
      // depends on what the lease operation positively established across
      // the await, never on queue occupancy.
      let holder: ThreadLeaseOperationProvenance | undefined;
      let forwardTo: string | undefined;
      // The raw exact owner token behind `forwardTo` — retained so the
      // forwarding handoff verifies the exact token, never just the decoded
      // public run id (a different attempt can reuse the public id).
      let forwardOwner: string | undefined;
      let ownershipUnreadable = false;
      if (leaseOutcome === undefined) {
        // The lease operation itself failed. Its outcome is unknown — the
        // provider may have committed the transfer before rejecting. One
        // bounded exact-owner read reconciles the captured provenance.
        const reconciled = await this.#reconcileThreadLeaseHolder(pubsub, key, [
          ...(attemptProvenance ? [{ runId: attemptProvenance.runId, token: attemptProvenance.token }] : []),
          ...(predecessorProvenance
            ? [{ runId: predecessorProvenance.runId, token: predecessorProvenance.token }]
            : []),
        ]);
        if (reconciled.status === 'unreadable') {
          ownershipUnreadable = true;
        } else if (reconciled.status === 'holder') {
          holder =
            reconciled.holder.runId === attemptProvenance?.runId
              ? attemptProvenance
              : reconciled.holder.runId === predecessorProvenance?.runId
                ? predecessorProvenance
                : undefined;
        } else if (reconciled.status === 'foreign') {
          forwardTo = this.#runIdFromLeaseOwner(reconciled.owner);
          forwardOwner = reconciled.owner;
        }
        // Verified absence follows the known-loss disposition below: the
        // restored input stays queued for the next natural trigger under a
        // fresh lease; nothing local owns the key, so there is nothing to
        // release or forward.
      } else if (leaseOutcome.acquired) {
        // Positively acquired by this attempt, then startup failed: the
        // captured failed-attempt token is the proven holder.
        holder = attemptProvenance;
      }
      if (ownershipUnreadable) {
        // Ownership unreadable: fail closed. Inputs and token provenance are
        // preserved, the infrastructure receipt is retained, and only this
        // drain's captured renewal is stopped — no release, forwarding,
        // sibling dispatch, or eager reacquisition, for the cancelled item's
        // recursive drain exactly as for the surviving queue. Recovery
        // happens on a later natural trigger once ownership becomes
        // verifiable; a failed operation is not proof that nothing committed.
        this.#rememberRejectedRunError(state, failedRunId, runError, { infrastructure: true });
        this.#stopLeaseRenewal(this.#getPubSub(pubsub), previousRun.runId, predecessorProvenance?.timer);
        return;
      }
      if (draining?.cancelled) {
        if (state.drainingPendingSignalsByThread.get(key) === draining) {
          state.drainingPendingSignalsByThread.delete(key);
        }
        // Cancellation routes through the outcome classification above BEFORE
        // any recursive drain: the cancelled item's own disposition is already
        // settled (its terminal was published and its input retained as
        // cancelled), and the surviving queue may only continue from a
        // positively established holder — never from an unresolved operation.
        await this.#drainPendingSignals(state, pubsub, key, {
          ...previousRun,
          runId: holder?.runId ?? previousRun.runId,
        });
        return;
      }
      if (forwardTo !== undefined && signal !== undefined) {
        // Verified foreign owner: the known-loss disposition — the same
        // forwarding-only handoff as the normal lease-loss branch completes
        // the restored signal's publication and then transfers the ordered
        // surviving pending tail and eligible idle work to that exact winner
        // (a handoff: the item is no longer locally cancellable). A failed
        // forward retains the restored input and preserves the forwarding
        // failure as evidence.
        const forwardOwnerToken = forwardOwner;
        if (forwardOwnerToken !== undefined) {
          // Narrowed const capture: `signal` is a `let` whose narrowing
          // does not persist into the closures below.
          const capturedSignal = signal;
          await this.#forwardVerifiedLossHandoff(
            state,
            pubsub,
            key,
            { owner: forwardOwnerToken, runId: forwardTo },
            {
              signal: capturedSignal,
              queueKind: 'pending',
              receiptRunId: failedRunId,
              // Queue-resident capture: commit removes it synchronously at
              // the publication's initiation, and a failed publication
              // restores it to the head (it may have been cancelled out of
              // the queue during verification, in which case both are
              // no-ops and the item stays dropped).
              commit: () => {
                const restoredQueue = state.pendingSignalsByThread.get(key);
                const restoredIndex = restoredQueue?.indexOf(capturedSignal) ?? -1;
                if (restoredIndex !== -1) {
                  restoredQueue!.splice(restoredIndex, 1);
                  if (restoredQueue!.length === 0) state.pendingSignalsByThread.delete(key);
                }
              },
              restore: () => {
                const restored = state.pendingSignalsByThread.get(key) ?? [];
                if (!restored.includes(capturedSignal))
                  state.pendingSignalsByThread.set(key, [capturedSignal, ...restored]);
              },
              // The capture sits in the restored queue until the
              // publication's initiation: cancellation during verification
              // removes it (and marks the draining record), so the helper
              // must recheck instead of publishing the retained reference.
              isCancelled: () =>
                draining?.cancelled === true ||
                !(state.pendingSignalsByThread.get(key)?.includes(capturedSignal) ?? false),
            },
          );
        }
        return;
      }
      if (holder !== undefined) {
        // Acquired or reconciled holder: await the native continuation/idle
        // handoff (never a detached chain) so settlement is observed before
        // any disposition. A continuation or idle successor transfers the
        // proven holder's exact token token-to-token.
        const continuationOutcome = await this.#drainPendingContinuations(state, pubsub, key, holder.runId);
        if (continuationOutcome === 'unresolved') {
          // The continuation's lease operation failed with an unknown
          // outcome: fail closed — no idle sibling dispatch and no release.
          // The helper retained the re-queued continuation and the
          // infrastructure receipt and stopped only its captured renewal.
        } else if (
          continuationOutcome === 'none' &&
          !(await this.#drainPendingIdleSignals(state, pubsub, key, holder.runId))
        ) {
          // A `true` from the idle helper is not proof a local successor
          // started — it also reports an already-owned outstanding drain,
          // whose owner this settlement must preserve. Only a `none` reaches
          // the release decision, fenced on the captured holder provenance:
          // no successor and no outstanding original drain owns the handoff,
          // so release the captured holder token — even though the restored
          // signal remains queued, it delivers on the next natural trigger
          // under a fresh lease, and a successor that adopted the same run id
          // (even reusing the retained token) must keep its lease.
          await this.#releaseThreadLeaseOwner(
            this.#getPubSub(pubsub),
            key,
            holder.runId,
            holder.token ?? holder.runId,
            holder,
          ).catch(releaseError => {
            this.#rememberRejectedRunError(state, failedRunId, getErrorFromUnknown(releaseError), {
              infrastructure: true,
            });
          });
        }
      }
      // The caller's terminal barrier must observe a setup failure it fenced.
      throw error;
    } finally {
      if (state.drainingPendingSignalsByThread.get(key) === draining) state.drainingPendingSignalsByThread.delete(key);
      if (nextRunId) state.startingQueuedRunIds.delete(nextRunId);
      this.#releaseUnusedThreadControlSubscription(state, key);
    }

    const continuationOutcome = await this.#drainPendingContinuations(state, pubsub, key, previousRun.runId);
    if (continuationOutcome !== 'none') {
      // 'started': a continuation run owns the handoff. 'unresolved': its
      // lease operation failed with an unknown outcome — no idle dispatch and
      // no release may follow on the strength of a boolean; the thread stays
      // fenced until a later natural trigger can verify ownership.
      return;
    }

    if (await this.#drainPendingIdleSignals(state, pubsub, key, previousRun.runId)) {
      return;
    }

    // Nothing left to drain: release the lease we kept held for the drain.
    // Retained work (a re-queued continuation that lost its lease, pre-run
    // input, still-queued signals) is not an empty queue — the next natural
    // trigger drains it under whoever owns the lease then.
    if (!this.#hasPendingThreadWork(state, key)) {
      this.#releaseThreadLease(pubsub, key, previousRun.runId);
    }
  }

  /**
   * Bounded reverify and projected-identity fence for a verified-loss
   * forwarding handoff, checked synchronously before entry and again before
   * every advance (each item's publication).
   *
   * The handoff may only continue while BOTH hold:
   *
   * - the provider positively still reports the captured exact winner token
   *   (`#reconcileThreadLeaseHolder` bounds the read; a changed owner, an
   *   absent key, or an unreadable/deadline-expired read retains the
   *   unhanded-off work — it is never permission to acquire or execute);
   * - the thread's projected identity does not belong to a live local
   *   reservation or successor. The authenticated remote projection of the
   * verified winner may be present (it arrived before handoff entry or while
   * the lease operation was outstanding — both reach this same decision);
   *   the same PUBLIC run id held by a local run/reservation is a different
   *   attempt with a different token and is not authority.
   */
  /**
   * Shared forwarding-only disposition for a foreign winner projected before
   * this run's terminal handoff, used by both the normal completion handoff
   * and the original aborted-terminal handoff. Returns true when the handoff
   * owned the disposition (the forwarding helper was invoked), false when the
   * ordinary drain/release path must run instead.
   *
   * At handoff entry, already-present pre-run input leads the capture
   * (mirroring the drain-entry fold, so existing pre-run X plus pending A/B
   * forwards as X/A/B); the pending queue supplies the capture only when no
   * pre-run input is present. Input arriving after the capture still lands
   * behind it through the handoff's ordered tail (captured A, later pre-run
   * X, pending tail B).
   *
   * Forwarding-only: never reserves a local execution run, never overwrites
   * the winner's active/stream identity, never acquires, transfers, renews or
   * releases the lease, and never calls `agent.stream`. Positive verification
   * (exact owner plus the projected stream/owner fence) happens in
   * `#verifiedForeignWinnerStillHolds` before anything forwards.
   */
  async #forwardProjectedWinnerAtHandoff(
    state: AgentThreadRuntimeState,
    pubsub: PubSub | undefined,
    key: string,
    record: Pick<AgentThreadRunRecord<any>, 'runId'>,
  ): Promise<boolean> {
    const projectedWinner = this.#projectedForeignWinner(state, key, record.runId);
    if (projectedWinner === undefined) return false;
    if (state.unresolvedForwardingsByThread.has(key)) {
      // Fail-closed: the completion handoff never forwards past an
      // ambiguously published item. The ordinary drain/release path below
      // stays fenced the same way, so returning false is safe.
      return false;
    }
    const firstSource =
      (state.preRunSignalsByThread.get(key)?.length ?? 0) > 0
        ? state.preRunSignalsByThread
        : state.pendingSignalsByThread;
    const handoffFirst = firstSource.get(key)?.[0];
    if (handoffFirst === undefined) return false;
    const winner = projectedWinner;
    const firstSignal = handoffFirst;
    const commitFirst = () => {
      for (const queue of [state.pendingSignalsByThread, state.preRunSignalsByThread]) {
        const owned = queue.get(key);
        const index = owned?.indexOf(firstSignal) ?? -1;
        if (index !== -1) {
          owned!.splice(index, 1);
          if (owned!.length === 0) queue.delete(key);
        }
      }
    };
    await this.#forwardVerifiedLossHandoff(state, pubsub, key, winner, {
      signal: firstSignal,
      queueKind: firstSource === state.preRunSignalsByThread ? 'pre-run' : 'pending',
      receiptRunId: record.runId,
      commit: commitFirst,
      restore: () => {
        if (
          !state.pendingSignalsByThread.get(key)?.includes(firstSignal) &&
          !state.preRunSignalsByThread.get(key)?.includes(firstSignal)
        ) {
          const owned = firstSource.get(key) ?? [];
          firstSource.set(key, [firstSignal, ...owned]);
        }
      },
      isCancelled: () =>
        !state.pendingSignalsByThread.get(key)?.includes(firstSignal) &&
        !state.preRunSignalsByThread.get(key)?.includes(firstSignal),
    });
    return true;
  }

  /**
   * Synchronous candidate derivation for the completion-handoff forwarding
   * route: the thread's active identity is a projected foreign run (neither
   * the just-finished run nor any locally owned run/reservation), with a
   * remote stream projection matching it. Returns undefined for ordinary
   * local/idle states so the native drain/release path runs unchanged.
   * Positive verification (exact owner plus the projected stream/owner
   * fence) happens in `#verifiedForeignWinnerStillHolds` before anything
   * forwards.
   */
  #projectedForeignWinner(
    state: AgentThreadRuntimeState,
    key: string,
    finishedRunId: string,
  ): { owner: string; runId: string } | undefined {
    const activeRunId = state.activeThreadRunIds.get(key);
    if (!activeRunId || activeRunId === finishedRunId) return undefined;
    if (state.threadRunsById.has(activeRunId)) return undefined;
    if (state.threadKeysByRunId.get(activeRunId) === key) return undefined;
    const identity = state.remoteStreamIdentityByThread.get(key);
    if (!identity || identity.runId !== activeRunId) return undefined;
    return { owner: identity.leaseOwner, runId: identity.runId };
  }

  async #verifiedForeignWinnerStillHolds(
    state: AgentThreadRuntimeState,
    pubsub: PubSub | undefined,
    key: string,
    winner: { owner: string; runId: string },
  ): Promise<boolean> {
    const winnerProjectionIntact = () => {
      const activeRunId = state.activeThreadRunIds.get(key);
      if (activeRunId === undefined) return true;
      if (activeRunId !== winner.runId) return false;
      // Same public run id: only the winner's remote projection (never a
      // local record/reservation reusing the public id) keeps the handoff.
      return !state.threadRunsById.has(winner.runId) && state.threadKeysByRunId.get(winner.runId) !== key;
    };
    // Exact projected remote stream+owner identity at entry, fenced across
    // the bounded read below: a delayed owner read must not forward under a
    // stale token after an authenticated replacement was projected, and the
    // same public run id with a different attempt token or stream is a
    // different owner. An absent projection stays permissible (registrations
    // can be missed), but only while it stays absent or a projection exactly
    // matching the winner appears.
    const projectedAtEntry = state.remoteStreamIdentityByThread.get(key);
    const projectedIdentityIntact = () => {
      const current = state.remoteStreamIdentityByThread.get(key);
      if (projectedAtEntry === undefined) {
        if (current === undefined) return true;
        return current.runId === winner.runId && current.leaseOwner === winner.owner;
      }
      if (projectedAtEntry.runId !== winner.runId || projectedAtEntry.leaseOwner !== winner.owner) return false;
      return (
        current !== undefined &&
        current.runId === projectedAtEntry.runId &&
        current.streamId === projectedAtEntry.streamId &&
        current.leaseOwner === projectedAtEntry.leaseOwner
      );
    };
    if (!winnerProjectionIntact() || !projectedIdentityIntact()) return false;
    const reconciled = await this.#reconcileThreadLeaseHolder(pubsub, key, [
      { runId: winner.runId, token: winner.owner },
    ]);
    if (reconciled.status !== 'holder') return false;
    // Recheck the projected identity after the bounded read's await: a
    // successor installed while the read was outstanding owns the queue now.
    return winnerProjectionIntact() && projectedIdentityIntact();
  }

  /**
   * Forwarding-only disposition for a positively verified foreign lease
   * winner, invoked after the drain's lease operation positively established
   * the loss (the normal lease-loss branch, or successful foreign-owner
   * reconciliation in the catch), or from the original completion handoff
   * when a foreign winner was already projected before drain entry. The
   * ordinary idle-start path keeps its active-run guard; this path never
   * reserves a local execution run, never overwrites the winner's
   * active/stream identity, never acquires, transfers, renews or releases
   * the lease, and never calls `agent.stream`.
   *
   * One settlement owner — the original drain (or the finishing run's
   * completion handoff) — transfers the ordered queue tail by publication,
   * awaiting each publication:
   *
   * 1. Complete the captured item's publication. The capture stays exactly
   *    cancellable through owner verification: cancellation is rechecked
   *    immediately before initiation, the handoff commits synchronously at
   *    initiation, and a cancelled capture is dropped without publishing.
   * 2. Forward surviving pre-run input in arrival order, ahead of the
   *    pending tail (mirroring the drain-entry fold).
   * 3. Forward the surviving pending work in order (the live queue is
   *    re-read before every item, so clear-pending cancellation during an
   *    outstanding publication cannot be overtaken).
   * 4. Only then forward eligible idle work, in order. A queued continuation
   *    cannot be forwarded by publication and its disposition in this
   *    handoff is unresolved — exactly as in the native chain, no idle work
   *    is forwarded behind it.
   *
   * A committed publication that rejects has unknown admission: the item
   * is retained as unresolved forwarding provenance (never restored as
   * executable work), the unsent tail stays fenced behind it, the failure
   * is retained as truthful infrastructure evidence, and the handoff stops
   * — no immediate retry loop and no automatic redispatch. A changed owner,
   * an unreadable read, or a live local reservation or successor retains
   * the unhanded-off work the same way, in its ordinary queues.
   * `signal-enqueued` is thread/run-addressed input delivery, not an
   * execution-lease transfer: publication confirms delivery, never atomic
   * recipient admission.
   */
  /**
   * Retain a committed-but-rejected forward as unresolved provenance.
   * First ambiguity wins and stays: later triggers must neither overwrite
   * the original attempt/error nor re-arm automatic dispatch. There is no
   * reconciling/clearing path here — authoritative disposition and explicit
   * reconciliation need a product decision first.
   */
  #retainUnresolvedForwarding(state: AgentThreadRuntimeState, key: string, entry: UnresolvedForwarding): void {
    if (!state.unresolvedForwardingsByThread.has(key)) {
      state.unresolvedForwardingsByThread.set(key, entry);
    }
  }

  async #forwardVerifiedLossHandoff(
    state: AgentThreadRuntimeState,
    pubsub: PubSub | undefined,
    key: string,
    winner: { owner: string; runId: string },
    first: {
      signal: CreatedAgentSignal;
      /** Which ordinary queue the captured item headed when committed. */
      queueKind: 'pending' | 'pre-run';
      /** Run id the failed-forward infrastructure receipt is recorded under. */
      receiptRunId: string;
      /**
       * Commits the captured item's handoff synchronously at its forwarding
       * publication's initiation: removes the item from its queue (or flips
       * the draining handoff flag for an already-shifted item) so exact-item
       * cancellation ends exactly when the item leaves local ownership.
       */
      commit: () => void;
      /** Puts the captured item back at its queue head after a failed publication. */
      restore: () => void;
      /**
       * Exact-item cancellation ownership recheck, evaluated immediately
       * before the captured item's publication is initiated: cancellation
       * that won during owner verification (clear-pending, or selective
       * cancellation while the handoff flag is still unset, or a queue
       * removal for a queue-resident capture) drops the item without
       * publishing it.
       */
      isCancelled: () => boolean;
    },
  ): Promise<void> {
    if (state.foreignWinnerHandoffsByThread.has(key)) {
      // Defensive: an outstanding handoff already owns this thread's queue
      // continuation; retain the captured item for the next natural trigger
      // instead of racing its publications — but only a surviving capture:
      // a capture cancelled while racing this guard stays dropped instead
      // of resurrecting. A committed, failed publication retains its item
      // as unresolved provenance through the failure paths below.
      if (first.isCancelled()) first.commit();
      else first.restore();
      return;
    }
    if (state.unresolvedForwardingsByThread.has(key)) {
      // Fail-closed: an earlier committed publication on this thread
      // rejected with unknown admission. The ambiguous item is fenced
      // pending reconciliation, so this handoff must not publish past it:
      // restore the new capture to its ordinary queue and publish nothing.
      // An owner change or natural trigger is not evidence of nondelivery.
      if (first.isCancelled()) first.commit();
      else first.restore();
      return;
    }
    // Install the exclusive handoff marker BEFORE the first await: the
    // check-and-set above is synchronous, so a competing handoff that
    // arrives during verification observes it instead of interleaving its
    // own publications. The finally below releases it; a failed entry
    // verification restores the captured item through the same path.
    state.foreignWinnerHandoffsByThread.set(key, winner);
    try {
      if (!(await this.#verifiedForeignWinnerStillHolds(state, pubsub, key, winner))) {
        // Pre-publication verification failure: restore only a surviving
        // capture. A capture cancelled during verification stays dropped
        // (never resurrected); a genuinely committed, failed publication
        // below instead retains its item as unresolved provenance (never as
        // executable work) because publication was invoked and admission is
        // unknown.
        if (first.isCancelled()) first.commit();
        else first.restore();
        return;
      }
      // 1. Complete the captured item's publication. The item stays exactly
      //    cancellable through verification: recheck immediately before
      //    initiating its publication, and commit synchronously at
      //    initiation. A cancelled capture is dropped — never published,
      //    never restored — while the surviving tail still forwards below.
      let forwardingError: unknown;
      if (!first.isCancelled()) {
        first.commit();
        await this.#publishAndWait(pubsub, key, {
          type: 'signal-enqueued',
          runId: winner.runId,
          signal: this.#serializeSignal(first.signal),
          sourceId: this.#getSourceId(),
        }).catch(error => {
          forwardingError = error;
        });
        if (forwardingError !== undefined) {
          // Post-commit rejection: admission is unknown — a generic
          // rejection is not proof the item never arrived. Retain the
          // committed item as unresolved provenance instead of restoring it
          // as executable work; the surviving tail stays fenced behind it.
          this.#retainUnresolvedForwarding(state, key, {
            signal: first.signal,
            queueKind: first.queueKind,
            destinationRunId: winner.runId,
            destinationOwner: winner.owner,
            receiptRunId: first.receiptRunId,
            error: getErrorFromUnknown(forwardingError),
          });
          this.#rememberRejectedRunError(state, first.receiptRunId, getErrorFromUnknown(forwardingError), {
            infrastructure: true,
          });
          return;
        }
      } else {
        first.commit();
      }

      // 2. Forward surviving pre-run input in arrival order, ahead of the
      //    pending tail (mirroring the drain-entry fold): these signals were
      //    addressed to the optimistic reservation during lease settlement
      //    and the winner rejects that address, so only this handoff
      //    delivers them. The live queue is re-read before every advance so
      //    cancellation during an outstanding publication is observed; a
      //    capture removed before its commitment point is skipped, never
      //    published.
      for (;;) {
        const preRunQueue = state.preRunSignalsByThread.get(key);
        if (!preRunQueue || preRunQueue.length === 0) break;
        if (!(await this.#verifiedForeignWinnerStillHolds(state, pubsub, key, winner))) return;
        const livePreRunQueue = state.preRunSignalsByThread.get(key);
        const preRunSignal = livePreRunQueue?.[0];
        if (!preRunSignal) break;
        livePreRunQueue!.shift();
        if (livePreRunQueue!.length === 0) state.preRunSignalsByThread.delete(key);
        await this.#publishAndWait(pubsub, key, {
          type: 'signal-enqueued',
          runId: winner.runId,
          signal: this.#serializeSignal(preRunSignal),
          sourceId: this.#getSourceId(),
        }).catch(error => {
          forwardingError = error;
        });
        if (forwardingError !== undefined) {
          // Post-commit rejection with unknown admission: fence the item
          // as unresolved provenance (already shifted out above, so it is
          // not restored) instead of letting a later trigger redispatch it.
          this.#retainUnresolvedForwarding(state, key, {
            signal: preRunSignal,
            queueKind: 'pre-run',
            destinationRunId: winner.runId,
            destinationOwner: winner.owner,
            receiptRunId: first.receiptRunId,
            error: getErrorFromUnknown(forwardingError),
          });
          this.#rememberRejectedRunError(state, first.receiptRunId, getErrorFromUnknown(forwardingError), {
            infrastructure: true,
          });
          return;
        }
      }

      // 3. Forward the surviving pending tail in order. The live queue is
      //    re-read before every advance, so cancellation that removed tail
      //    items (including clear-pending during the outstanding publication
      //    above) is always observed before the next item is taken.
      for (;;) {
        if (!(await this.#verifiedForeignWinnerStillHolds(state, pubsub, key, winner))) return;
        const queue = state.pendingSignalsByThread.get(key);
        const signal = queue?.shift();
        if (!signal || !queue) break;
        if (queue.length === 0) state.pendingSignalsByThread.delete(key);
        await this.#publishAndWait(pubsub, key, {
          type: 'signal-enqueued',
          runId: winner.runId,
          signal: this.#serializeSignal(signal),
          sourceId: this.#getSourceId(),
        }).catch(error => {
          forwardingError = error;
        });
        if (forwardingError !== undefined) {
          // Post-commit rejection with unknown admission: fence the item
          // as unresolved provenance (already shifted out above, so it is
          // not restored) instead of letting a later trigger redispatch it.
          this.#retainUnresolvedForwarding(state, key, {
            signal,
            queueKind: 'pending',
            destinationRunId: winner.runId,
            destinationOwner: winner.owner,
            receiptRunId: first.receiptRunId,
            error: getErrorFromUnknown(forwardingError),
          });
          this.#rememberRejectedRunError(state, first.receiptRunId, getErrorFromUnknown(forwardingError), {
            infrastructure: true,
          });
          return;
        }
      }

      // A queued continuation stays retained (its disposition is unresolved
      // in this handoff): stop before idle work, never behind it.
      if ((state.pendingContinuationsByThread.get(key)?.length ?? 0) > 0) return;

      // 4. Forward eligible idle work, in order. Each dequeued item keeps its
      //    cancellation identity until its forwarding commitment point.
      for (;;) {
        if (!(await this.#verifiedForeignWinnerStillHolds(state, pubsub, key, winner))) return;
        const idleQueue = state.pendingIdleSignalsByThread.get(key);
        const pendingIdle = idleQueue?.shift();
        if (!pendingIdle || !idleQueue) break;
        if (idleQueue.length === 0) state.pendingIdleSignalsByThread.delete(key);
        state.pendingIdleThreadKeysByRunId.delete(pendingIdle.runId);
        state.drainingIdleSignalsByThread.set(key, pendingIdle);
        if (pendingIdle.cancelled) {
          // Cancellation before the commitment point wins and prevents
          // publication. The native once-only settlement is exact-item and
          // cleanup-only here — this handoff never held a local lease — and
          // the surviving queue continues through this same settlement owner.
          await this.#settleCancelledIdleRun(state, pubsub, key, pendingIdle, {});
          continue;
        }
        // Commitment point: publication is initiated, so the item leaves its
        // cancellation identity — a later cancellation can no longer claim
        // the item as locally cancelled and unsent.
        const rejection = pendingIdle.onRunRejected;
        pendingIdle.onRunRejected = undefined;
        if (state.drainingIdleSignalsByThread.get(key) === pendingIdle) {
          state.drainingIdleSignalsByThread.delete(key);
        }
        await this.#publishAndWait(pubsub, key, {
          type: 'signal-enqueued',
          runId: winner.runId,
          signal: this.#serializeSignal(pendingIdle.signal),
          sourceId: this.#getSourceId(),
        }).catch(error => {
          forwardingError = error;
        });
        if (forwardingError !== undefined) {
          // Post-commit rejection with unknown admission: fence the item
          // as unresolved provenance (already dequeued above, so it is not
          // re-queued) instead of letting a later drain settle it as if
          // unsent. The rejection identity is preserved for explicit
          // reconciliation and never invoked automatically.
          this.#retainUnresolvedForwarding(state, key, {
            signal: pendingIdle.signal,
            queueKind: 'idle',
            destinationRunId: winner.runId,
            destinationOwner: winner.owner,
            receiptRunId: pendingIdle.runId,
            error: getErrorFromUnknown(forwardingError),
            onRunRejected: rejection,
          });
          this.#rememberRejectedRunError(state, pendingIdle.runId, getErrorFromUnknown(forwardingError), {
            infrastructure: true,
          });
          return;
        }
        // The local run id never executes: settle its receipt exactly once,
        // cleanup-only — never the default release's surviving-queue
        // deletion, abort publication, or sibling dispatch.
        this.#releaseReservedRun(state, pubsub, key, pendingIdle.runId, {
          rejectOutputWaiters: true,
          callerOwnedHandoff: true,
        });
        this.#notifyThreadEvents(state);
        rejection?.();
      }
    } finally {
      if (state.foreignWinnerHandoffsByThread.get(key) === winner) {
        state.foreignWinnerHandoffsByThread.delete(key);
      }
    }
  }

  /**
   * Drain one queued continuation, handing the finished run's lease to it.
   *
   * - `'started'` — a continuation run was started and owns the handoff.
   * - `'none'` — nothing to drain, the thread is already active, or the
   *   transfer/acquisition reported a VERIFIED loss (the continuation is
   *   re-queued for the next natural trigger under a fresh lease).
   * - `'unresolved'` — the lease operation failed with an UNKNOWN outcome.
   *   Callers must not dispatch an idle sibling, release, forward, or
   *   reacquire on this result: ownership is unresolved, so the thread stays
   *   fenced until a later natural trigger can verify it. Neither `false`
   *   nor a cosmetic `true` establishes a safe disposition.
   */
  async #drainPendingContinuations(
    state: AgentThreadRuntimeState,
    pubsub: PubSub | undefined,
    key: string,
    fromRunId?: string,
  ): Promise<'started' | 'none' | 'unresolved'> {
    if (state.unresolvedForwardingsByThread.has(key)) {
      // Fail-closed: no continuation sibling may advance past an
      // ambiguously published forward. Report unresolved (not none) so
      // callers stay fenced: no idle dispatch and no release follow.
      return 'unresolved';
    }
    if (state.activeThreadRunIds.has(key)) {
      return 'none';
    }

    const queue = state.pendingContinuationsByThread.get(key);
    const pending = queue?.shift();
    if (!pending || !queue) {
      return 'none';
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
      state.threadKeysByRunId.set(pending.runId, key);
      // Operation provenance, captured BEFORE the lease operation's provider
      // await: the predecessor's exact renewal-timer identity, used to fence
      // the fail-closed stop below against a successor adopted meanwhile.
      const fromTimer = state.leaseRenewalTimers.get(fromRunId);
      let owns: { acquired: boolean; owner?: string; ownerToken?: string; error?: unknown };
      try {
        // Fail-closed lease operation: an asynchronous rejection propagates
        // here instead of collapsing into a verified loss that would permit
        // fallback acquisition after an ambiguous transfer.
        owns = await this.#acquireOrTransferThreadLease(pubsub, key, pending.runId, fromRunId, { failClosed: true });
      } catch (error) {
        // The lease operation failed with an unknown outcome: no eager
        // reacquisition and no sibling start. Roll the optimistic reservation
        // back, requeue the continuation for the next natural trigger, retain
        // the failure as infrastructure evidence for this attempt, and stop
        // only the captured predecessor renewal so the TTL can lapse — never
        // a successor's timer.
        if (state.activeThreadRunIds.get(key) === pending.runId) {
          state.activeThreadRunIds.delete(key);
        }
        state.threadKeysByRunId.delete(pending.runId);
        const restored = state.pendingContinuationsByThread.get(key) ?? [];
        state.pendingContinuationsByThread.set(key, [pending, ...restored]);
        this.#rememberRejectedRunError(state, pending.runId, getErrorFromUnknown(error), {
          infrastructure: true,
        });
        this.#stopLeaseRenewal(this.#getPubSub(pubsub), fromRunId, fromTimer);
        return 'unresolved';
      }
      if (!owns.acquired) {
        if (state.activeThreadRunIds.get(key) === pending.runId) {
          state.activeThreadRunIds.delete(key);
        }
        state.threadKeysByRunId.delete(pending.runId);
        // Retained work is not an empty queue: the re-queued continuation
        // still needs the thread's pre-run input, so it is not deleted here.
        const restored = state.pendingContinuationsByThread.get(key) ?? [];
        state.pendingContinuationsByThread.set(key, [pending, ...restored]);
        return 'none';
      }
    }

    this.#startContinuation(state, pubsub, key, pending);
    return 'started';
  }

  #startContinuation(
    state: AgentThreadRuntimeState,
    pubsub: PubSub | undefined,
    key: string,
    pending: PendingContinuation<any>,
  ) {
    state.activeThreadRunIds.set(key, pending.runId);
    state.threadKeysByRunId.set(pending.runId, key);
    // Mirror the pending-signal startup contract (see #drainPendingSignals):
    // the runtime already owns this run's optimistic reservation and lease
    // handoff, so establish the reserved agent identity and pass the internal
    // reservation-owner marker plus the captured PubSub. A real Agent.stream
    // then adopts the existing reservation instead of attempting a fresh one
    // whose run-id collision cannot authenticate adoption. Duck-typed agents
    // without the reserveRun hook keep working: they ignore the marker.
    state.reservedAgentIdsByRunId.set(pending.runId, pending.agent.id);
    // Queued startups own their failure path; keep an external
    // releaseThreadRunReservation from tearing down this optimistic
    // reservation while the stream call is still in flight.
    state.startingQueuedRunIds.add(pending.runId);
    void pending.agent
      .stream(pending.messages, {
        ...(pending.streamOptions as any),
        _pubsub: this.#getPubSub(pubsub),
        _threadRunReservationOwner: true,
        runId: pending.runId,
        memory: withThreadMemory(pending.streamOptions?.memory, pending.resourceId, pending.threadId),
      })
      .then(output => {
        state.startingQueuedRunIds.delete(pending.runId);
        if ((state.pendingContinuationsByThread.get(key)?.length ?? 0) > 0) {
          const nextRecord = state.threadRunsById.get(output.runId);
          if (nextRecord) {
            void this.#watchThreadRunCompletion(state, pubsub, key, nextRecord);
          }
        }
      })
      .catch(async err => {
        state.startingQueuedRunIds.delete(pending.runId);
        if (state.threadKeysByRunId.get(pending.runId) === key) {
          state.threadKeysByRunId.delete(pending.runId);
        }
        state.reservedAgentIdsByRunId.delete(pending.runId);
        this.#cleanupPreparedRun(state, pending.runId);
        if (state.activeThreadRunIds.get(key) === pending.runId) {
          state.activeThreadRunIds.delete(key);
        }
        try {
          await this.#publishTerminalAndWait(pubsub, key, {
            type: 'run-failed',
            runId: pending.runId,
            error: getErrorFromUnknown(err).message,
            leaseOwner: state.leaseOwnerTokensByRunId.get(pending.runId),
          });
        } catch {
          // Continue bounded queue/lease cleanup without replacing the original
          // continuation setup failure.
        }
        this.#trimFailedRun(pubsub, key, { ...pending, streamOptions: pending.streamOptions ?? {} });
        // Hand the lease to remaining queued work (transfer keeps the key from
        // going empty); only release once nothing is left to drain. An
        // unresolved continuation outcome must reach this finally too: the
        // release is gated on it so an unknown lease outcome can never be
        // released around on the strength of a boolean.
        let leaseDispositionUnresolved = false;
        try {
          const continuationOutcome = await this.#drainPendingContinuations(state, pubsub, key, pending.runId);
          if (continuationOutcome === 'started') return;
          if (continuationOutcome === 'unresolved') {
            leaseDispositionUnresolved = true;
            return;
          }
          if (await this.#drainPendingIdleSignals(state, pubsub, key, pending.runId)) return;
        } finally {
          if (!leaseDispositionUnresolved && !state.activeThreadRunIds.has(key)) {
            this.#releaseThreadLease(pubsub, key, pending.runId);
          }
        }
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
    const runId = target.runId ?? globalThis.crypto.randomUUID();
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

  /**
   * Settles a cancelled idle-drain item exactly once, from its original drain.
   *
   * `abortRun` records cancellation on the item captured by a drain whose
   * lease operation is still outstanding — it marks `cancelled`, retains the
   * abort tombstone and synchronously rejects already-registered output
   * waiters — but defers every ownership effect to here: exact-item index
   * cleanup, reservation-waiter release, the rejection callback (consumed
   * once), lease disposition and the queue continuation. A repeated
   * cancellation can therefore never create a second settlement owner, and no
   * competing drain can start a queued sibling while the lease outcome is
   * unknown.
   *
   * `leaseHolderRunId` is the run whose owner token this process verifiably
   * holds once the lease operation settled (the cancelled attempt after a
   * successful transfer/acquisition). `fromRunId` is the predecessor the drain
   * started from; it is used as the holder only when the attempt verifiably
   * still owns it. With no verified holder the settlement stays fail-closed:
   * inputs are retained and only the abandoned captured renewal is stopped.
   */
  async #settleCancelledIdleRun(
    state: AgentThreadRuntimeState,
    pubsub: PubSub | undefined,
    key: string,
    pendingIdle: PendingIdleSignal<any>,
    options: {
      fromRunId?: string;
      leaseHolderRunId?: string;
      infrastructureError?: unknown;
      /**
       * Provenance of the verified holder, captured before the cancelled
       * drain's lease operation: the final release is fenced on it so a
       * same-run successor adopted while the settlement was outstanding —
       * even one re-adopting the retained token — keeps its lease.
       */
      leaseHolderProvenance?: ThreadLeaseOperationProvenance;
    } = {},
  ): Promise<void> {
    if (pendingIdle.cancelSettled) return;
    pendingIdle.cancelSettled = true;

    const runId = pendingIdle.runId;
    // Waiter sets are removed before the rejection callback is invoked, and
    // the callback is consumed exactly once.
    const rejection = pendingIdle.onRunRejected;
    pendingIdle.onRunRejected = undefined;
    if (state.drainingIdleSignalsByThread.get(key) === pendingIdle) {
      state.drainingIdleSignalsByThread.delete(key);
    }
    // Caller-owned handoff: exact-item reservation rollback without the lease
    // release, the sibling drain, or the surviving-queue deletion — this
    // settlement owns all three dispositions below.
    this.#releaseReservedRun(state, pubsub, key, runId, {
      rejectOutputWaiters: true,
      callerOwnedHandoff: true,
    });
    state.inflightIdleThreadKeysByRunId.delete(runId);
    state.inflightIdleAgentIdsByRunId.delete(runId);
    this.#notifyThreadEvents(state);
    rejection?.();
    this.#publish(pubsub, key, {
      type: 'run-aborted',
      runId,
      leaseOwner: options.leaseHolderRunId !== undefined ? state.leaseOwnerTokensByRunId.get(runId) : undefined,
    });

    const holder = options.leaseHolderRunId ?? options.fromRunId;
    if (holder === undefined || options.infrastructureError !== undefined) {
      // No verified holder, or ownership unreadable after a provider failure:
      // reject the drain without a progress claim. Unhanded-off inputs and
      // token provenance are retained; only the abandoned captured renewal is
      // stopped. TTL/next natural trigger may recover — no retry loop.
      if (options.infrastructureError !== undefined) {
        this.#rememberRejectedRunError(state, runId, getErrorFromUnknown(options.infrastructureError), {
          infrastructure: true,
        });
        if (options.fromRunId !== undefined) {
          this.#stopLeaseRenewal(this.#getPubSub(pubsub), options.fromRunId);
        }
      }
      return;
    }
    if (this.#hasPendingThreadWork(state, key)) {
      // One awaited native continuation with the verified actual holder:
      // pre-run folding, follow-up delivery, continuation priority, idle
      // work and the final release all stay inside the native chain, which
      // transfers the actual retained lease token-to-token — never a
      // release-and-reacquire gap.
      await this.#drainPendingSignals(state, pubsub, key, {
        agent: pendingIdle.agent,
        streamOptions: (pendingIdle.streamOptions ?? {}) as AgentExecutionOptions<any>,
        runId: holder,
        resourceId: pendingIdle.resourceId,
        threadId: pendingIdle.threadId,
      });
      return;
    }
    // Positively empty queue: release the exact captured token and preserve
    // the rejection as truthful failure evidence instead of treating a
    // swallowed provider failure as a successful release.
    const resolvedPubSub = this.#getPubSub(pubsub);
    if (options.leaseHolderProvenance && this.#leaseProvenanceSuperseded(state, options.leaseHolderProvenance)) {
      // A successor adopted the holder run id while this settlement was
      // outstanding (possibly re-adopting the retained token, where token
      // equality cannot distinguish the attempts): never revoke its token,
      // timer or lease. The cancellation settlement itself is already
      // complete; only the release is abandoned.
      return;
    }
    const capturedToken = options.leaseHolderProvenance?.token ?? state.leaseOwnerTokensByRunId.get(holder);
    // Committed release: invalidate the reusable map entry now, retaining the
    // captured token as provenance; post-await cleanup must never remove a
    // replacement token or timer a successor started meanwhile.
    if (capturedToken !== undefined && state.leaseOwnerTokensByRunId.get(holder) === capturedToken) {
      state.leaseOwnerTokensByRunId.delete(holder);
    }
    this.#stopLeaseRenewal(resolvedPubSub, holder, options.leaseHolderProvenance?.timer);
    try {
      await this.#getLeaseProvider(resolvedPubSub).releaseLease(key, capturedToken ?? holder);
    } catch (error) {
      this.#rememberRejectedRunError(state, runId, getErrorFromUnknown(error), { infrastructure: true });
    }
  }

  async #drainPendingIdleSignals(
    state: AgentThreadRuntimeState,
    pubsub: PubSub | undefined,
    key: string,
    fromRunId?: string,
  ): Promise<boolean> {
    if (state.unresolvedForwardingsByThread.has(key)) {
      // Fail-closed: an ambiguously published forward fences the thread's
      // queue continuation exactly like an outstanding handoff. Report the
      // thread as claimed rather than starting an idle sibling past it;
      // the ordinary active-run guard below is unchanged for every other
      // drain.
      return true;
    }
    if (state.foreignWinnerHandoffsByThread.has(key)) {
      // An outstanding verified-loss forwarding handoff owns this thread's
      // queue continuation — its publications are transferring the ordered
      // tail to the foreign winner. Report the thread as claimed rather than
      // racing the handoff for its idle items; the ordinary active-run guard
      // below is unchanged for every other drain.
      return true;
    }
    if (state.activeThreadRunIds.has(key)) {
      return false;
    }
    if (state.drainingIdleSignalsByThread.has(key)) {
      // An outstanding handoff — a captured item whose lease operation has
      // not settled — cannot be overwritten by a competing drain, including
      // the non-reserving mode (which never blocks on activeThreadRunIds).
      // The original drain owns the once-only settlement and the queue
      // continuation, so report the thread as claimed rather than racing it.
      return true;
    }

    const idleQueue = state.pendingIdleSignalsByThread.get(key);
    const pendingIdle = idleQueue?.shift();
    if (!pendingIdle || !idleQueue) {
      return false;
    }
    if (pendingIdle.cancelled) {
      // A cancelled item must not be captured by a competing drain: its
      // original drain owns the once-only settlement. This drain continues
      // the surviving queue through the settlement's native chain.
      if (idleQueue.length === 0) state.pendingIdleSignalsByThread.delete(key);
      await this.#settleCancelledIdleRun(state, pubsub, key, pendingIdle, { fromRunId });
      return true;
    }
    if (idleQueue.length === 0) {
      state.pendingIdleSignalsByThread.delete(key);
    }
    state.pendingIdleThreadKeysByRunId.delete(pendingIdle.runId);
    // A dequeued idle message retains its cancellation identity until execution
    // begins, so a concurrent abort can find and settle the exact item.
    state.drainingIdleSignalsByThread.set(key, pendingIdle);

    const existingRunKey = state.threadKeysByRunId.get(pendingIdle.runId);
    if (existingRunKey && existingRunKey !== key) {
      pendingIdle.onRunRejected?.();
      this.#releaseReservedRun(state, pubsub, existingRunKey, pendingIdle.runId, {
        cleanupPrepared: true,
        clearAbort: true,
        rejectOutputWaiters: true,
      });
      state.drainingIdleSignalsByThread.delete(key);
      return this.#drainPendingIdleSignals(state, pubsub, key, fromRunId);
    }
    if (state.threadRunsById.has(pendingIdle.runId)) {
      pendingIdle.onRunRejected?.();
      this.#releaseReservedRun(state, pubsub, key, pendingIdle.runId, {
        cleanupPrepared: true,
        clearAbort: true,
        rejectOutputWaiters: true,
      });
      state.drainingIdleSignalsByThread.delete(key);
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
    // Operation provenance, captured BEFORE the lease operation's provider
    // await: this attempt's candidate token and the predecessor's exact
    // token/timer/record. Post-await cleanup reconciles and fences against
    // these instead of rereading the mutable maps by run id.
    const attemptProvenance = this.#captureLeaseProvenance(
      state,
      pendingIdle.runId,
      this.#leaseOwnerForRun(state, pendingIdle.runId),
    );
    const predecessorProvenance = this.#captureLeaseProvenance(state, fromRunId);
    let owns: { acquired: boolean; owner?: string; ownerToken?: string; error?: unknown };
    try {
      // Fail-closed lease operation: an asynchronous rejection propagates to
      // the catch instead of collapsing into a verified loss that would
      // permit fallback acquisition after an ambiguous transfer.
      owns = await this.#acquireOrTransferThreadLease(pubsub, key, pendingIdle.runId, fromRunId, { failClosed: true });
      this.#refreshLeaseProvenance(state, attemptProvenance);
    } catch (err) {
      if (pendingIdle.cancelled) {
        // The lease operation itself threw while the item was cancelled: the
        // abort already owns this item's settlement. Ownership is unreadable,
        // so the settlement stays fail-closed and the original infrastructure
        // error is retained as the cancelled attempt's Error.cause.
        await this.#settleCancelledIdleRun(state, pubsub, key, pendingIdle, {
          fromRunId,
          infrastructureError: err,
        });
        return true;
      }
      // Settle the captured original item exactly once, BEFORE disposing of
      // its ownership: the shifted item's rejection callback is consumed, its
      // already-installed output/reservation waiters settle, and its
      // reserved/in-flight identities are removed. Cleanup-only
      // (callerOwnedHandoff) so this settlement cannot independently release
      // the lease or dispatch a sibling — the ownership disposition below
      // owns both. The original idle item stays owned until this settlement;
      // its input is not silently dropped alongside the failed lease op.
      const rejection = pendingIdle.onRunRejected;
      pendingIdle.onRunRejected = undefined;
      state.drainingIdleSignalsByThread.delete(key);
      if (reserveBeforePreflight) {
        this.#releaseReservedRun(state, pubsub, key, pendingIdle.runId, {
          cleanupPrepared: true,
          clearAbort: true,
          rejectOutputWaiters: true,
          announceAbort: false,
          callerOwnedHandoff: true,
        });
      } else {
        state.inflightIdleThreadKeysByRunId.delete(pendingIdle.runId);
        state.inflightIdleAgentIdsByRunId.delete(pendingIdle.runId);
        this.#rejectPendingOutputWaiters(state, pendingIdle.runId, getErrorFromUnknown(err));
        this.#resolveReservationWaiters(state, pendingIdle.runId);
      }
      rejection?.();
      this.#notifyThreadEvents(state);
      this.#publish(pubsub, key, {
        type: 'run-failed',
        runId: pendingIdle.runId,
        error: getErrorFromUnknown(err).message,
      });
      this.#trimFailedRun(pubsub, key, { ...pendingIdle, streamOptions: pendingIdle.streamOptions ?? {} });
      // Ownership-aware disposition: the lease operation failed with an
      // unknown outcome — the provider may have committed the transfer before
      // rejecting — so nothing may be released or reacquired on the strength
      // of a boolean alone. One bounded exact-owner read reconciles the
      // captured provenance (this attempt's candidate token and the
      // predecessor's retained token).
      const reconciled = await this.#reconcileThreadLeaseHolder(pubsub, key, [
        ...(attemptProvenance ? [{ runId: attemptProvenance.runId, token: attemptProvenance.token }] : []),
        ...(predecessorProvenance ? [{ runId: predecessorProvenance.runId, token: predecessorProvenance.token }] : []),
      ]);
      if (reconciled.status === 'unreadable') {
        // Ownership unreadable: fail closed. The failed attempt already
        // published its run-failed terminal and settled its original item;
        // retain the infrastructure receipt, stop only this drain's captured
        // renewal, and skip sibling dispatch, release, and reacquisition.
        // Recovery happens on a later natural trigger once ownership becomes
        // verifiable.
        this.#rememberRejectedRunError(state, pendingIdle.runId, getErrorFromUnknown(err), {
          infrastructure: true,
        });
        if (fromRunId !== undefined) {
          this.#stopLeaseRenewal(this.#getPubSub(pubsub), fromRunId, predecessorProvenance?.timer);
        }
        return true;
      }
      if (reconciled.status === 'holder') {
        // The provider verifiably still holds one of the captured tokens:
        // continue the native handoff from that proven holder, releasing its
        // exact captured token — fenced on the captured provenance — only
        // when no sibling or successor claims it.
        const holder =
          reconciled.holder.runId === attemptProvenance?.runId
            ? attemptProvenance
            : reconciled.holder.runId === predecessorProvenance?.runId
              ? predecessorProvenance
              : undefined;
        if (holder !== undefined && !(await this.#drainPendingIdleSignals(state, pubsub, key, holder.runId))) {
          await this.#releaseThreadLeaseOwner(
            this.#getPubSub(pubsub),
            key,
            holder.runId,
            holder.token ?? holder.runId,
            holder,
          ).catch(releaseError => {
            this.#rememberRejectedRunError(state, pendingIdle.runId, getErrorFromUnknown(releaseError), {
              infrastructure: true,
            });
          });
        }
        return true;
      }
      // Verified foreign owner or verified absence: nothing local owns the
      // key, so there is nothing to release. Continue draining siblings from
      // the predecessor the drain started from — the native chain re-verifies
      // ownership at every step.
      await this.#drainPendingIdleSignals(state, pubsub, key, fromRunId);
      return true;
    }
    if (!owns.acquired) {
      // A cancellation during lease acquisition must not forward the signal.
      if (pendingIdle.cancelled) {
        // A provider failure (not a verified loss) leaves ownership
        // unreadable: settle fail-closed with the retained infrastructure
        // error. A verified different live owner or a positively absent
        // owner continues from the predecessor the drain started from — the
        // native chain re-verifies at every step.
        const ownershipUnreadable = owns.owner === undefined && owns.error !== undefined;
        await this.#settleCancelledIdleRun(
          state,
          pubsub,
          key,
          pendingIdle,
          ownershipUnreadable ? { fromRunId, infrastructureError: owns.error } : { fromRunId },
        );
        return true;
      }
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
      // If another process owns the lease, hand the already-accepted signal to
      // that run's owner; drop the draining marker either way.
      if (state.activeThreadRunIds.get(key) === pendingIdle.runId) {
        state.activeThreadRunIds.delete(key);
      }
      state.drainingIdleSignalsByThread.delete(key);
      state.threadKeysByRunId.delete(pendingIdle.runId);
      state.preRunSignalsByThread.delete(key);
      this.#notifyThreadEvents(state);
      if (owns.owner) {
        await this.#publishAndWait(pubsub, key, {
          type: 'signal-enqueued',
          runId: owns.owner,
          signal: this.#serializeSignal(pendingIdle.signal),
          sourceId: this.#getSourceId(),
        }).catch(() => {});
      }
      const drained = await this.#drainPendingIdleSignals(
        state,
        pubsub,
        key,
        owns.acquired ? pendingIdle.runId : fromRunId,
      );
      if (owns.acquired && !drained) this.#releaseThreadLease(pubsub, key, pendingIdle.runId);
      return drained || owns.acquired;
    }

    if (pendingIdle.cancelled) {
      // The transfer/acquisition succeeded, so the cancelled attempt is the
      // verified lease holder; the settlement hands its exact token onward,
      // fenced on the captured attempt provenance so a successor adopted
      // meanwhile — even one re-adopting the retained token — keeps its lease.
      await this.#settleCancelledIdleRun(state, pubsub, key, pendingIdle, {
        fromRunId,
        leaseHolderRunId: pendingIdle.runId,
        leaseHolderProvenance: attemptProvenance,
      });
      return true;
    }

    try {
      state.drainingIdleSignalsByThread.delete(key);
      this.#notifyThreadEvents(state);
      state.startingQueuedRunIds.add(pendingIdle.runId);
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
      const leaseOwner = state.leaseOwnerTokensByRunId.get(pendingIdle.runId);
      try {
        await this.#publishTerminalAndWait(pubsub, key, {
          type: 'run-failed',
          runId: pendingIdle.runId,
          error: getErrorFromUnknown(err).message,
          leaseOwner,
        });
      } catch {
        // The queued setup failure stays authoritative; cleanup must proceed
        // even when its bounded distributed terminal cannot be delivered.
      }
      pendingIdle.onRunRejected?.();
      if (reserveBeforePreflight) {
        this.#releaseReservedRun(state, pubsub, key, pendingIdle.runId, {
          cleanupPrepared: true,
          clearAbort: true,
          rejectOutputWaiters: true,
          announceAbort: false,
        });
      } else {
        state.inflightIdleThreadKeysByRunId.delete(pendingIdle.runId);
        state.inflightIdleAgentIdsByRunId.delete(pendingIdle.runId);
        this.#forgetSignalAdmissionsForRun(state, key, pendingIdle.runId);
        this.rejectUnregisteredRun(pendingIdle.runId, pubsub);
      }
      // The authenticated terminal above is the only run-failed publish.
      this.#trimFailedRun(pubsub, key, { ...pendingIdle, streamOptions: pendingIdle.streamOptions ?? {} });
      // No completion watcher exists for a failed startup. Preserve pending-before-idle recovery here too.
      await this.#drainPendingSignals(state, pubsub, key, {
        agent: pendingIdle.agent,
        runId: pendingIdle.runId,
        resourceId: pendingIdle.resourceId,
        threadId: pendingIdle.threadId,
        streamOptions: pendingIdle.streamOptions ?? {},
      });
      // Hand the lease to remaining idle work; release only when none remains.
      if (!(await this.#drainPendingIdleSignals(state, pubsub, key, pendingIdle.runId))) {
        this.#releaseThreadLease(pubsub, key, pendingIdle.runId);
      }
    } finally {
      state.startingQueuedRunIds.delete(pendingIdle.runId);
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
    // Leave queued input for the completion handler to deliver in a fresh run.
    if (state.abortedRunIds.has(runId) || state.preparedRunsById.get(runId)?.abortController.signal.aborted) {
      return [];
    }
    const record = state.threadRunsById.get(runId);
    const key = record ? this.#threadKey(record.resourceId, record.threadId) : state.threadKeysByRunId.get(runId);
    if (!key) return [];
    if (state.unresolvedForwardingsByThread.has(key)) {
      // Fail-closed: in-loop and pre-run consumption never take past an
      // ambiguously published forward. The retained input stays queued for
      // explicit reconciliation instead of becoming executable input again.
      return [];
    }

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
    options: { memory?: AgentExecutionOptions<any>['memory']; requestContext?: RequestContext; runId?: string },
    pubsub?: PubSub,
    ownsReservation = false,
  ) {
    const { threadId, resourceId } = this.#getThreadTarget(options);
    if (!threadId) return;

    // Read-only runs never persist to the thread, so they cannot corrupt
    // message ordering and must not serialize (or reserve): structured-output's
    // `useAgent` path re-enters agent.stream() on the same thread mid-run.
    if (hasReadOnlyMemory(options)) return;

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
      if (!activeRunId) {
        return;
      }
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

    // Read-only runs are outside the reservation protocol; callers should not
    // park or retry them if a thread-bound run is already active.
    if (hasReadOnlyMemory(options)) return;

    const state = this.#getState(pubsub);
    const key = this.#threadKey(resourceId, threadId);
    const control = this.#ensureThreadControlSubscription(state, pubsub, key);
    control.references++;
    try {
      await control.ready;
      while (true) {
        const activeRunId = state.activeThreadRunIds.get(key);
        if (!activeRunId) {
          // The caller owns the retrying `reserveRun()` operation. Reserving here
          // would make that immediate retry collide with its own run id; multiple
          // awakened callers instead race through the single atomic local reserve,
          // and losers park again on the winner's lifecycle.
          return;
        }

        // A caller that targets the active run (resumeStream, approval continuations) is a
        // continuation of that run, not a contender — never wait on ourselves.
        if (options.runId && options.runId === activeRunId) return;

        const activeRecord = state.threadRunsById.get(activeRunId);
        if (activeRecord && !this.#isThreadBlockingRun(state, activeRecord)) {
          // PF-4402: a finished run stays the active owner until its terminal
          // publishes settle, and `reserveRun()` refuses while it does. Yield a
          // macrotask so the caller's retry cannot starve that release.
          await new Promise<void>(resolve => setTimeout(resolve, 0));
          return;
        }
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
    } finally {
      control.references--;
      this.#releaseUnusedThreadControlSubscription(state, key);
    }
  }

  /**
   * Releases a thread reservation made by `waitForCrossAgentThreadRun` when the
   * run fails before reaching `registerRun`. No-op once the run has registered
   * (registration owns cleanup from then on) or if the reservation was already
   * replaced.
   */
  releaseThreadRunReservation(
    runId: string,
    pubsub?: PubSub,
    failedRun?: Pick<AgentThreadRunRecord<any>, 'agent' | 'streamOptions'>,
  ) {
    const state = this.#getState(pubsub);
    // Queued startups have their own catch path, which must restore input before draining anything else.
    if (state.threadRunsById.has(runId) || state.startingQueuedRunIds.has(runId)) return;
    const key = state.threadKeysByRunId.get(runId) ?? state.preparedRunsById.get(runId)?.threadKey;
    if (!key) return;
    try {
      state.threadKeysByRunId.delete(runId);
      const activeRunId = state.activeThreadRunIds.get(key);
      const wasAborted =
        state.abortedRunIds.has(runId) || state.preparedRunsById.get(runId)?.abortController.signal.aborted;
      if (activeRunId !== runId && (activeRunId || !wasAborted)) return;
      state.activeThreadRunIds.delete(key);
      const target = failedRun ? this.#getThreadTarget(failedRun.streamOptions) : undefined;
      if (wasAborted && failedRun && target?.threadId) {
        // Failed preparation never registers a completion watcher. Recover pending input before idle work.
        this.#cleanupPreparedRun(state, runId);
        void this.#drainPendingSignals(state, pubsub, key, {
          ...failedRun,
          threadId: target.threadId,
          resourceId: target.resourceId,
          runId,
        });
      } else {
        void this.#drainPendingIdleSignals(state, pubsub, key);
      }
    } finally {
      this.#cleanupPreparedRun(state, runId);
      this.#releaseUnusedThreadControlSubscription(state, key);
    }
  }

  async #waitForRemoteRunToFinish(pubsub: PubSub | undefined, key: string, runId: string) {
    const resolvedPubSub = this.#getPubSub(pubsub);
    const state = this.#getState(resolvedPubSub);
    const { provider, isFallback } = this.#resolveLeaseProvider(resolvedPubSub);
    const topic = this.#threadTopic(key);
    const expectedStreamId =
      state.activeThreadRunIds.get(key) === runId ? state.activeThreadStreamIds.get(key) : undefined;
    const remoteIdentity = state.remoteStreamIdentityByThread.get(key);
    const expectedLeaseOwner =
      expectedStreamId && remoteIdentity?.streamId === expectedStreamId ? remoteIdentity.leaseOwner : undefined;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let subscribed = false;
    let settled = false;
    let resolveWait!: () => void;
    const wait = new Promise<void>(resolve => {
      resolveWait = resolve;
    });
    const clearRemoteActive = (streamId?: string) => {
      const activeStreamId = state.activeThreadStreamIds.get(key);
      if (state.activeThreadRunIds.get(key) !== runId || (streamId && activeStreamId !== streamId)) {
        return;
      }
      state.activeThreadRunIds.delete(key);
      state.activeThreadStreamIds.delete(key);
      if (state.remoteStreamIdentityByThread.get(key)?.streamId === activeStreamId) {
        state.remoteStreamIdentityByThread.delete(key);
      }
      if (state.remoteThreadKeysByRunId.get(runId) === key) state.remoteThreadKeysByRunId.delete(runId);
    };
    const finish = () => {
      if (settled) return;
      settled = true;
      if (timer) clearTimeout(timer);
      resolveWait();
    };
    const checkLease = async () => {
      if (settled) return;
      if (isFallback) return;
      const owner = await provider.getLeaseOwner(key).catch(() => undefined);
      if (settled) return;
      if (owner === undefined || this.#runIdFromLeaseOwner(owner) !== runId) {
        clearRemoteActive();
        finish();
        return;
      }
      timer = setTimeout(() => void checkLease(), AGENT_THREAD_LEASE_TTL_MS);
    };
    const onEvent: EventCallback = async (event, ack) => {
      const data = event.data as AgentThreadStreamRuntimeEvent | undefined;
      const terminalCandidate =
        data?.type === 'run-completed' ||
        data?.type === 'run-aborted' ||
        data?.type === 'run-failed' ||
        data?.type === 'run-discarded';
      const exactStream =
        terminalCandidate &&
        data.runId === runId &&
        expectedStreamId !== undefined &&
        data.streamId === expectedStreamId;
      const exactOwner = exactStream && expectedLeaseOwner !== undefined && data.leaseOwner === expectedLeaseOwner;
      // A discard retracts the exact registration this waiter follows, and the
      // rolled-back owner never publishes a lifecycle terminal for that stream,
      // so stream identity alone authenticates it.
      const isTerminal =
        terminalCandidate &&
        data.runId === runId &&
        (isFallback || exactOwner || (data.type === 'run-discarded' && exactStream));
      // Acknowledge every delivered event, not just the terminal one — this is a
      // private fan-out subscription, so anything left unacked stays pending on
      // the backend. The terminal ack completes before the waiter resolves so
      // the subsequent unsubscribe cannot race it.
      // A failing ack must never strand the waiter: the backend's ack deadline
      // will redeliver or expire the entry, but this run is still finished.
      try {
        await ack?.();
      } catch {
        // Ack expiry/redelivery is handled by the backend.
      }
      if (isTerminal) {
        clearRemoteActive(data.streamId);
        finish();
      }
    };

    try {
      await resolvedPubSub.subscribe(topic, onEvent);
      subscribed = true;
      if (!isFallback) timer = setTimeout(() => void checkLease(), AGENT_THREAD_LEASE_TTL_MS);
      await wait;
    } catch {
      finish();
      await wait;
    } finally {
      if (timer) clearTimeout(timer);
      if (subscribed) await resolvedPubSub.unsubscribe(topic, onEvent).catch(() => {});
    }
  }

  async #loadThreadHistory(agent: Agent<any, any, any, any>, options: AgentSubscribeToThreadOptions) {
    const memory = await agent.getMemory({ requestContext: options.requestContext });
    if (!memory) return { messages: [], hasMore: false };
    // The controller subscribes as soon as it creates a thread, before memory
    // may hold it; a thread that doesn't exist yet has no history.
    if (!(await memory.getThreadById({ threadId: options.threadId }))) return { messages: [], hasMore: false };
    const perPage =
      typeof options.withInitialHistory === 'object' && options.withInitialHistory.perPage !== undefined
        ? options.withInitialHistory.perPage
        : DEFAULT_INITIAL_HISTORY_PER_PAGE;
    const result = await memory.recall({
      threadId: options.threadId,
      resourceId: options.resourceId,
      perPage,
      page: 0,
      orderBy: { field: 'createdAt', direction: 'DESC' },
      hideSignals: options.hideSignals,
    });
    return { messages: [...result.messages].reverse(), hasMore: result.hasMore };
  }

  /**
   * Rebuilds the `tool-call-approval` prompts of runs that died while waiting
   * on the user (restart, lost lease). The broadcast backlog of such a run is
   * rejected because its lease owner can no longer be proven live, so the
   * prompt is rebuilt from the `pendingToolApprovals` entry the tool-call step
   * persisted, and only when the agent's own suspended-run discovery confirms
   * that exact run is suspended in storage on that approval. Runs this runtime
   * still holds replay their own prompt; any failure to verify rebuilds
   * nothing (PF-4402 user decision).
   */
  async #restoreStoredPendingApprovals(
    agent: Agent<any, any, any, any>,
    options: AgentSubscribeToThreadOptions,
    messages: MastraDBMessage[],
    state: AgentThreadRuntimeState,
  ): Promise<ReturnType<typeof toolCallApprovalChunkFromStored>[]> {
    const stored = collectStoredPendingToolApprovals(messages).filter(
      approval => !state.threadRunsById.has(approval.runId),
    );
    if (stored.length === 0 || typeof agent.listSuspendedRuns !== 'function') return [];
    let verified: Set<string>;
    try {
      const { runs } = await agent.listSuspendedRuns({
        threadId: options.threadId,
        resourceId: options.resourceId,
        requestContext: options.requestContext,
      });
      verified = new Set(
        runs.flatMap(run =>
          run.status === 'suspended' && run.threadId === options.threadId && run.resourceId === options.resourceId
            ? run.toolCalls
                .filter(toolCall => toolCall.requiresApproval)
                .map(toolCall => `${run.runId}\u0000${toolCall.toolCallId}`)
            : [],
        ),
      );
    } catch {
      return [];
    }
    const seen = new Set<string>();
    return stored
      .filter(approval => {
        const key = `${approval.runId}\u0000${approval.toolCallId}`;
        if (!verified.has(key) || seen.has(key)) return false;
        seen.add(key);
        return true;
      })
      .map(toolCallApprovalChunkFromStored);
  }

  async subscribeToThread<OUTPUT = unknown>(
    agent: Agent<any, any, any, any>,
    options: AgentSubscribeToThreadOptions,
    pubsub?: PubSub,
  ): Promise<AgentThreadSubscription<OUTPUT, boolean>> {
    const resolvedPubSub = this.#getPubSub(pubsub);
    const { provider: leaseProvider, isFallback: hasFallbackLeaseProvider } =
      this.#resolveLeaseProvider(resolvedPubSub);
    const state = this.#getState(resolvedPubSub);
    const key = this.#threadKey(options.resourceId, options.threadId);
    const topic = this.#threadTopic(key);
    const seenStreamIds = new Set<string>();
    const highestLocalStreamSeqByRunId = new Map<string, number>();
    const pendingRuns: AgentThreadRunRecord<any>[] = [];
    const waiters: Array<() => void> = [];
    const drainedOutputs = new WeakSet<object>();
    const enqueuedOutputs = new WeakSet<object>();
    const streamDrainedOutputs = new WeakSet<object>();
    const terminalOutputs = new WeakSet<object>();
    const outputsByStreamId = new Map<string, MastraModelOutput<unknown>>();
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
    // Remote terminals can be redelivered out of order. Retain bounded stream
    // tombstones so late registration/part events cannot resurrect a completed
    // segment or overwrite a newer same-run stream's active identity.
    const terminalRemoteStreamIds = new Set<string>();
    // Suspended runs legitimately reuse their public run id across resume
    // segments. Retain a bounded recent identity window; exact lease-owner and
    // active-stream checks independently reject older delayed events.
    const resumableTerminalStreamIdsByRunId = new Map<string, Set<string>>();
    let done = false;

    const rememberTerminalRemoteStream = (streamId: string) => {
      terminalRemoteStreamIds.delete(streamId);
      terminalRemoteStreamIds.add(streamId);
      while (terminalRemoteStreamIds.size > MAX_ABORTED_RUN_TOMBSTONES) {
        const oldest = terminalRemoteStreamIds.values().next().value;
        if (oldest === undefined) break;
        terminalRemoteStreamIds.delete(oldest);
      }
    };

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

    const removeTrackedOutput = (streamId: string, output: MastraModelOutput<unknown>) => {
      if (outputsByStreamId.get(streamId) === output) outputsByStreamId.delete(streamId);
    };

    const markRunTerminalDelivered = (streamId: string) => {
      const output = outputsByStreamId.get(streamId);
      outputsByStreamId.delete(streamId);
      if (!output) return;
      terminalOutputs.add(output);
      settleOutputDrainIfReady(output);
    };

    const forgetAcceptedOwner = (streamId: string) => {
      acceptedLeaseOwnersByStreamId.delete(streamId);
      acceptedStreamSeqByStreamId.delete(streamId);
      if (state.remoteStreamIdentityByThread.get(key)?.streamId === streamId) {
        state.remoteStreamIdentityByThread.delete(key);
      }
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
      waitForDrain = true,
    ): Promise<never> => {
      // A PubSub can invoke this subscription and then reject later in the same
      // publish. Once enqueued, the segment is observable regardless of the
      // publication promise's outcome. Do not let its failure reach Session
      // until every buffered part has crossed the subscription generator; turn
      // teardown can then close all tools that actually started, with no later
      // tool_start appearing after the failure terminal.
      if (waitForDrain && enqueuedOutputs.has(output) && !streamDrainedOutputs.has(output)) {
        try {
          await waitForStreamDrain(output);
        } catch {
          // Explicit subscription teardown stops the generator and is therefore
          // also a safe boundary after which this segment cannot emit more parts.
        }
      }
      for (const [streamId, trackedOutput] of outputsByStreamId) {
        if (trackedOutput === output) removeTrackedOutput(streamId, output);
      }
      throw error;
    };

    const waitForOutputDrain = (output: MastraModelOutput<unknown>): Promise<void> | undefined => {
      const registration = this.#threadOutputRegistrations.get(output);
      const record = this.#getState(resolvedPubSub).threadRunsById.get(output.runId);
      // Duck-typed Agent overrides may return an output without registering it
      // with the thread runtime. There is no subscription segment to drain in
      // that case, so preserve their existing direct completion contract.
      if (!registration) return undefined;
      // Parked-abort receipt bound to this exact output generation. Consult it
      // BEFORE the drained-output shortcut: a retired parked output can be
      // fully drained (its suspension settled) while its authenticated
      // run-aborted publication is still pending, and the shortcut would
      // otherwise hide that newly pending receipt behind a success.
      const parkedAbortDelivery = this.#threadOutputParkedAbortDeliveries.get(output);
      if (drainedOutputs.has(output)) {
        if (!parkedAbortDelivery) return Promise.resolve();
        return parkedAbortDelivery.then(
          () => undefined,
          error => rejectAfterEnqueuedStreamDrain(output, error, false),
        );
      }
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
            // Abort delivery may be installed after this waiter is created.
            // Resolve the terminal promise after registration settles instead
            // of snapshotting it before abortRun can attach the exact fence.
            // The parked-abort receipt outranks the run-id record: after a
            // same-run successor replaced the record, only the output-scoped
            // receipt still names THIS output's publication.
            const terminal = this.#threadOutputTerminals.get(output);
            const exactReceipt = this.#threadOutputParkedAbortDeliveries.get(output);
            const exactTerminal = exactReceipt ?? record?.abortDelivery ?? terminal;
            if (exactTerminal) {
              try {
                await exactTerminal;
              } catch (error) {
                // A failed abort fence is itself the fail-closed boundary: the
                // broadcaster stopped accepting post-abort chunks synchronously.
                // Waiting for a source that may ignore abort would hide this
                // delivery failure until teardown.
                await rejectAfterEnqueuedStreamDrain(
                  output,
                  error,
                  exactReceipt === undefined && record?.abortDelivery === undefined,
                );
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
      outputsByStreamId.set(record.streamId, record.output);
      // The per-run correlation queue exists even when no caller uses the
      // internal output-drain waiter. A rejected terminal has no delivery event
      // that could shift this output, so observe the runtime's terminal promise
      // and remove this exact object on failure.
      queueMicrotask(() => {
        const terminal = this.#threadOutputTerminals.get(record.output);
        void terminal?.catch(() => removeTrackedOutput(record.streamId, record.output));
      });
      pendingRuns.push(record);
      wake();
    };

    const createRemoteRun = (
      runId: string,
      streamId: string,
      streamSeq: number,
      leaseOwner: string,
    ): AgentThreadRunRecord<any> => {
      const remoteRun = {
        parts: [] as unknown[],
        waiters: [] as Array<() => void>,
        finishWaiters: [] as Array<() => void>,
        done: false,
        // SAFETY: placeholder until the ReadableStream constructed below is
        // assigned immediately after; readers only reach the stream after a
        // `parts`/`waiters` drain, so the undefined gap is never observed.
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
              try {
                controller.close();
              } catch {
                // A second close after the consumer already closed the stream
                // is the expected terminal state, not an error to surface.
              }
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
        leaseOwner,
      };
    };

    const localStreamIds = new Set<string>();
    // Authentication is established once at registration (or late-subscriber
    // bootstrap) and retained through terminal delivery. This survives local
    // record/lease cleanup without trusting forgeable event source ids.
    const acceptedLeaseOwnersByStreamId = new Map<string, string>();
    const acceptedStreamSeqByStreamId = new Map<string, number>();
    const eagerAbortListenersByStreamId = new Map<string, () => void>();
    const replayedStreamIds = new Set<string>();
    // Replayed runs whose origin no longer holds the thread lease (the run is
    // terminal or its process died). Their parts are buffered instead of being
    // yielded live: a retained backend replays every run's chunks to a fresh
    // subscriber, but a run that failed mid-stream never persisted a message,
    // so replaying it would surface a phantom partial message that hydrated
    // history can never reconcile. The run is only released to the subscriber
    // once its terminal control event proves it finished cleanly; failed,
    // aborted, or never-terminated (process crash) runs are dropped.
    //
    // PF-3375 UPSTREAM SYNC — RECORDED GAP, NOT AN OVERSIGHT. Upstream (#21223)
    // populated this map at two admission sites: a `run-registered` with no live
    // owner, and a `stream-part` arriving before any registration. This fork
    // authenticates remote admission against the segment's exact lease owner
    // instead, so both of those sites `return` (see the `run-registered` and
    // `stream-part` handlers below) and this map is currently never written —
    // the flush/discard wiring on the terminal handlers is retained but
    // unreachable. That is deliberate: `agent-signals.test.ts` ("uses lease
    // ownership as the authority for remote active thread state") pins the drop
    // for an unsigned registration AND for one signed by a non-lease-holder, and
    // relaxing either would let any topic publisher project a forged segment.
    // Re-enabling upstream's phantom-replay protection is a product decision that
    // must (a) defer ONLY when `getLeaseOwner(key)` is `undefined` — an empty key
    // grants no authority to anyone, so buffering costs nothing, whereas a key
    // held by a DIFFERENT owner must keep failing closed — and (b) come with
    // signed fixtures: `thread-stream-test-utils.ts` emits unsigned events with
    // raw-run-id owners, which this fork drops before deferral is ever reached.
    const deferredRunsByStreamId = new Map<string, AgentThreadRunRecord<any>>();
    const remoteRunLeaseTimers = new Map<string, ReturnType<typeof setTimeout>>();
    let currentReader: ReadableStreamDefaultReader<any> | null = null;
    let activeReaderRunId: string | null = null;
    let activeReaderStreamId: string | null = null;
    let currentRunRequestContext: RequestContext | undefined;
    // Abort finalization is owned by the fork's authoritative abort terminal
    // (`abortTerminalPending` / `pendingAuthoritativeAbortPart`), which carries
    // the origin's abort part instead of synthesizing one on reader cancel.
    let abortTerminalPending = false;
    const activeToolCallIdsByRunId = new Map<string, Set<string>>();
    let abortDrainTimer: ReturnType<typeof setTimeout> | undefined;

    const clearAbortDrainTimer = () => {
      if (abortDrainTimer === undefined) return;
      clearTimeout(abortDrainTimer);
      abortDrainTimer = undefined;
    };

    const markAbortPending = (streamId: string) => {
      if (activeReaderStreamId === streamId && currentReader) abortTerminalPending = true;
    };

    const armAbortDrain = (runId: string, streamId: string) => {
      if (activeReaderStreamId !== streamId || !currentReader) return;
      abortTerminalPending = true;
      clearAbortDrainTimer();
      const readerAtAbort = currentReader;
      const remoteRunAtAbort = remoteRuns.get(streamId);
      abortDrainTimer = setTimeout(() => {
        abortDrainTimer = undefined;
        if (remoteRunAtAbort && remoteRuns.get(streamId) === remoteRunAtAbort) {
          remoteRunAtAbort.done = true;
          while (remoteRunAtAbort.waiters.length) remoteRunAtAbort.waiters.shift()?.();
          while (remoteRunAtAbort.finishWaiters.length) remoteRunAtAbort.finishWaiters.shift()?.();
          remoteRuns.delete(streamId);
          return;
        }
        if (currentReader !== readerAtAbort || activeReaderStreamId !== streamId) return;
        void readerAtAbort.cancel().catch(() => {
          // Cancellation is best-effort after the run has already aborted.
        });
      }, ABORT_OUTPUT_DRAIN_GRACE_MS);
      abortDrainTimer.unref?.();
    };

    const registerEagerAbortListener = (runId: string, streamId: string) => {
      if (eagerAbortListenersByStreamId.has(streamId)) return;
      const listener = () => markAbortPending(streamId);
      eagerAbortListenersByStreamId.set(streamId, listener);
      const listeners = this.#eagerAbortListenersByStreamId.get(streamId) ?? new Set<() => void>();
      listeners.add(listener);
      this.#eagerAbortListenersByStreamId.set(streamId, listeners);
    };

    const removeEagerAbortListener = (streamId: string) => {
      const listener = eagerAbortListenersByStreamId.get(streamId);
      if (!listener) return;
      eagerAbortListenersByStreamId.delete(streamId);
      const listeners = this.#eagerAbortListenersByStreamId.get(streamId);
      listeners?.delete(listener);
      if (listeners?.size === 0) this.#eagerAbortListenersByStreamId.delete(streamId);
    };

    const markActiveIfLive = async (
      runId: string,
      streamId: string,
      local: boolean,
      leaseOwner?: string,
    ): Promise<boolean> => {
      if (!local && !(await this.#hasLiveThreadLease(resolvedPubSub, key, runId, leaseOwner))) return false;
      state.activeThreadRunIds.set(key, runId);
      state.activeThreadStreamIds.set(key, streamId);
      if (!local) state.remoteThreadKeysByRunId.set(runId, key);
      return true;
    };

    // A deferred run may be flushed only when its replayed chunks ended with a
    // clean `finish` — the origin publishes `run-completed` after the run's
    // messages were flushed to storage, so a clean finish implies a persisted
    // message backs the replay. An in-band `error` chunk, a finish with
    // `stepResult.reason: 'error'`, or a missing finish all mean nothing was
    // persisted for the run.
    const deferredRunEndedCleanly = (parts: unknown[]) => {
      let sawFinish = false;
      for (const part of parts) {
        const typed = part as { type?: string; payload?: { stepResult?: { reason?: string } } } | undefined;
        if (typed?.type === 'error' || typed?.type === 'abort') return false;
        if (typed?.type === 'finish') {
          if (typed.payload?.stepResult?.reason === 'error') return false;
          sawFinish = true;
        }
      }
      return sawFinish;
    };

    const discardDeferredRun = (streamId: string) => {
      deferredRunsByStreamId.delete(streamId);
      const timer = remoteRunLeaseTimers.get(streamId);
      if (timer) clearTimeout(timer);
      remoteRunLeaseTimers.delete(streamId);
      const remoteRun = remoteRuns.get(streamId);
      if (!remoteRun) return;
      remoteRun.parts.length = 0;
      remoteRun.done = true;
      while (remoteRun.waiters.length) remoteRun.waiters.shift()?.();
      while (remoteRun.finishWaiters.length) remoteRun.finishWaiters.shift()?.();
      remoteRuns.delete(streamId);
    };

    const clearActiveIfCurrent = (runId: string, streamId?: string) => {
      if (
        state.activeThreadRunIds.get(key) !== runId ||
        (streamId && state.activeThreadStreamIds.get(key) !== streamId)
      ) {
        return;
      }
      state.activeThreadRunIds.delete(key);
      state.activeThreadStreamIds.delete(key);
      if (!streamId || state.remoteStreamIdentityByThread.get(key)?.streamId === streamId) {
        state.remoteStreamIdentityByThread.delete(key);
      }
      if (state.remoteThreadKeysByRunId.get(runId) === key) state.remoteThreadKeysByRunId.delete(runId);
    };

    const stopRemoteRunLeaseWatch = (streamId: string) => {
      const timer = remoteRunLeaseTimers.get(streamId);
      if (timer) clearTimeout(timer);
      remoteRunLeaseTimers.delete(streamId);
    };

    const startRemoteRunLeaseWatch = (runId: string, streamId: string) => {
      if (hasFallbackLeaseProvider || remoteRunLeaseTimers.has(streamId)) return;

      const checkLease = async () => {
        remoteRunLeaseTimers.delete(streamId);
        const remoteRun = remoteRuns.get(streamId);
        if (done || !remoteRun || remoteRun.done) return;

        let owner: string | undefined;
        try {
          owner = await leaseProvider.getLeaseOwner(key);
        } catch {
          if (done || remoteRuns.get(streamId) !== remoteRun || remoteRun.done) return;
          remoteRunLeaseTimers.set(
            streamId,
            setTimeout(() => void checkLease(), AGENT_THREAD_LEASE_TTL_MS),
          );
          return;
        }
        if (done || remoteRuns.get(streamId) !== remoteRun || remoteRun.done) return;
        // Liveness is keyed on the exact process-attempt lease token this proxy
        // accepted, never on the public run id: `#leaseOwnerForRun` deliberately
        // makes the owner unique per attempt because a retry-stable run id would
        // let two processes both "win" the lease. Comparing against `runId` here
        // can never match a token-owned run and would falsely terminate it.
        // Mirrors `#hasLiveThreadLease`, including its legacy opaque-owner path.
        const expectedLeaseOwner = acceptedLeaseOwnersByStreamId.get(streamId);
        const stillOwned =
          expectedLeaseOwner !== undefined
            ? owner === expectedLeaseOwner && this.#runIdFromLeaseOwner(expectedLeaseOwner) === runId
            : owner !== undefined && this.#runIdFromLeaseOwner(owner) === runId;
        if (stillOwned) {
          remoteRunLeaseTimers.set(
            streamId,
            setTimeout(() => void checkLease(), AGENT_THREAD_LEASE_TTL_MS),
          );
          return;
        }

        clearActiveIfCurrent(runId, streamId);
        remoteRun.parts.push({
          type: 'error',
          payload: { error: new Error(`Thread run ${runId} lost its lease before publishing a terminal event`) },
        });
        remoteRun.done = true;
        while (remoteRun.waiters.length) remoteRun.waiters.shift()?.();
        while (remoteRun.finishWaiters.length) remoteRun.finishWaiters.shift()?.();
        remoteRuns.delete(streamId);
        seenStreamIds.delete(streamId);
        await this.#drainPendingIdleSignals(state, resolvedPubSub, key, runId);
        wake();
      };

      remoteRunLeaseTimers.set(
        streamId,
        setTimeout(() => void checkLease(), AGENT_THREAD_LEASE_TTL_MS),
      );
    };

    const handleEvent = async (
      event: Parameters<EventCallback>[0],
      trustedRegistration?: AgentThreadRunRecord<any>,
    ) => {
      if (done) return;
      const data = event.data as AgentThreadStreamRuntimeEvent | undefined;
      if (!data) return;
      if (data.type === 'run-registered') {
        // At-least-once delivery can replay a registration after its aborted
        // segment was terminalized. Never recreate a proxy that is intentionally
        // tombstoned and can no longer receive a terminal.
        const trustedLocalRecord =
          trustedRegistration &&
          trustedRegistration.runId === data.runId &&
          trustedRegistration.streamId === data.streamId &&
          trustedRegistration.streamSeq === data.streamSeq &&
          trustedRegistration.leaseOwner === data.leaseOwner &&
          this.#threadKey(trustedRegistration.resourceId, trustedRegistration.threadId) === key
            ? trustedRegistration
            : undefined;
        if (trustedRegistration && !trustedLocalRecord) return;
        noteRunHalf(data.runId, { streamId: data.streamId, streamSeq: data.streamSeq });
        const localRecord = trustedLocalRecord ?? state.threadRunsByStreamId.get(data.streamId);
        if (
          terminalRemoteStreamIds.has(data.streamId) ||
          resumableTerminalStreamIdsByRunId.get(data.runId)?.has(data.streamId) ||
          (!localRecord && seenStreamIds.has(data.streamId))
        ) {
          return;
        }
        const activeRunId = state.activeThreadRunIds.get(key);
        const activeStreamId = state.activeThreadStreamIds.get(key);
        if (
          !localRecord &&
          activeRunId === data.runId &&
          activeStreamId !== undefined &&
          activeStreamId !== data.streamId &&
          !resumableTerminalStreamIdsByRunId.get(data.runId)?.has(activeStreamId)
        ) {
          const activeIdentity = state.remoteStreamIdentityByThread.get(key);
          const activeOwner =
            acceptedLeaseOwnersByStreamId.get(activeStreamId) ??
            (activeIdentity?.streamId === activeStreamId ? activeIdentity.leaseOwner : undefined);
          const activeStreamSeq =
            acceptedStreamSeqByStreamId.get(activeStreamId) ??
            (activeIdentity?.streamId === activeStreamId ? activeIdentity.streamSeq : undefined);
          if (
            activeOwner !== undefined &&
            data.leaseOwner === activeOwner &&
            (activeStreamSeq === undefined || data.streamSeq <= activeStreamSeq)
          ) {
            return;
          }
        }
        if (localRecord) {
          if (data.leaseOwner !== localRecord.leaseOwner) return;
          if (
            data.streamSeq < (highestLocalStreamSeqByRunId.get(data.runId) ?? 0) ||
            localStreamIds.has(data.streamId)
          ) {
            return;
          }
          registerEagerAbortListener(data.runId, data.streamId);
          acceptedLeaseOwnersByStreamId.set(data.streamId, localRecord.leaseOwner);
          acceptedStreamSeqByStreamId.set(data.streamId, data.streamSeq);
          highestLocalStreamSeqByRunId.set(data.runId, data.streamSeq);
          localStreamIds.add(data.streamId);
        } else {
          const { isFallback } = this.#resolveLeaseProvider(resolvedPubSub);
          if (!isFallback && data.leaseOwner === undefined) return;
          if (!(await markActiveIfLive(data.runId, data.streamId, false, data.leaseOwner))) return;
          if (data.leaseOwner !== undefined) {
            acceptedLeaseOwnersByStreamId.set(data.streamId, data.leaseOwner);
            state.remoteStreamIdentityByThread.set(key, {
              runId: data.runId,
              streamId: data.streamId,
              leaseOwner: data.leaseOwner,
              streamSeq: data.streamSeq,
            });
          }
          acceptedStreamSeqByStreamId.set(data.streamId, data.streamSeq);
          replayedStreamIds.add(data.streamId);
        }
        if (localRecord) await markActiveIfLive(data.runId, data.streamId, true);
        // Liveness is already authenticated above: a non-local registration only
        // reaches here once its exact lease owner was proven live, so the run is
        // released rather than deferred. Reuse a proxy that a stream-part-first
        // delivery already created for this stream — creating a fresh record
        // here would orphan its buffered parts.
        const record =
          localRecord ??
          deferredRunsByStreamId.get(data.streamId) ??
          createRemoteRun(data.runId, data.streamId, data.streamSeq, data.leaseOwner ?? data.runId);
        deferredRunsByStreamId.delete(data.streamId);
        enqueueRun(record);
        wake();
        return;
      }
      if (data.type === 'run-aborting') {
        const activeStreamId =
          state.activeThreadRunIds.get(key) === data.runId ? state.activeThreadStreamIds.get(key) : undefined;
        if (activeStreamId !== data.streamId) return;
        const remoteIdentity = state.remoteStreamIdentityByThread.get(key);
        const expectedOwner =
          acceptedLeaseOwnersByStreamId.get(data.streamId) ??
          (remoteIdentity?.streamId === data.streamId ? remoteIdentity.leaseOwner : undefined);
        const { isFallback } = this.#resolveLeaseProvider(resolvedPubSub);
        if (!isFallback && (expectedOwner === undefined || data.leaseOwner !== expectedOwner)) return;
        markAbortPending(data.streamId);
        return;
      }
      if (data.type === 'stream-part') {
        const localRecord = state.threadRunsByStreamId.get(data.streamId);
        if (
          data.sourceId === this.#id &&
          (localStreamIds.has(data.streamId) || !replayedStreamIds.has(data.streamId))
        ) {
          return;
        }
        const activeStreamId =
          state.activeThreadRunIds.get(key) === data.runId ? state.activeThreadStreamIds.get(key) : undefined;
        // Once a newer stream for this stable run id is active, a delayed part
        // from an older segment cannot reactivate it. `run-registered` is the
        // sole authority that advances stream identity.
        if (activeStreamId !== undefined && activeStreamId !== data.streamId) return;
        const remoteIdentity = state.remoteStreamIdentityByThread.get(key);
        const expectedOwner =
          acceptedLeaseOwnersByStreamId.get(data.streamId) ??
          (remoteIdentity?.streamId === data.streamId ? remoteIdentity.leaseOwner : undefined);
        const { isFallback } = this.#resolveLeaseProvider(resolvedPubSub);
        if (expectedOwner !== undefined) {
          if (data.leaseOwner !== expectedOwner) return;
          if (!isFallback && !(await this.#hasLiveThreadLease(resolvedPubSub, key, data.runId, expectedOwner))) {
            return;
          }
        } else if (!localRecord && !isFallback) {
          if (!(await this.#hasLiveThreadLease(resolvedPubSub, key, data.runId, data.leaseOwner))) return;
          acceptedLeaseOwnersByStreamId.set(data.streamId, data.leaseOwner);
          state.remoteStreamIdentityByThread.set(key, {
            runId: data.runId,
            streamId: data.streamId,
            leaseOwner: data.leaseOwner,
            streamSeq: state.streamSeqByRunId.get(data.runId) ?? 1,
          });
        }
        stampPartProducedAt(data.part, data.producedAt ?? new Date(event.createdAt ?? Date.now()).getTime());
        if (
          terminalRemoteStreamIds.has(data.streamId) ||
          resumableTerminalStreamIdsByRunId.get(data.runId)?.has(data.streamId)
        ) {
          return;
        }
        if (activeStreamId === undefined) {
          if (!(await markActiveIfLive(data.runId, data.streamId, false, data.leaseOwner))) return;
          replayedStreamIds.add(data.streamId);
          enqueueRun(
            createRemoteRun(data.runId, data.streamId, state.streamSeqByRunId.get(data.runId) ?? 1, data.leaseOwner),
          );
        }
        const remoteRun = remoteRuns.get(data.streamId);
        // A part whose stream never produced an authenticated proxy above is not
        // proof of a remote run: this runtime only projects a segment whose exact
        // lease owner it verified, so an unauthenticated part is dropped rather
        // than used to synthesize a proxy stream.
        if (!remoteRun) return;
        remoteRun.parts.push(data.part);
        while (remoteRun.waiters.length) remoteRun.waiters.shift()?.();
        return;
      }
      if (data.type === 'signal-enqueued') {
        // Observer-only view: execution-queue admission is owned solely by the
        // control subscription (`#ensureThreadControlSubscription`), which
        // admits remote signals through the canonical stable-id/payload-conflict
        // ledger. Enqueueing here as well would double-execute a signal whenever
        // both subscriptions receive the same delivery, so this subscription
        // only projects run state and never mutates the execution queues.
        return;
      }
      if (data.type === 'run-abort-requested') {
        const record = state.threadRunsByStreamId.get(data.streamId);
        if (
          record?.runId === data.runId &&
          state.preparedRunsById.has(data.runId) &&
          state.threadKeysByRunId.get(data.runId) === key &&
          state.activeThreadRunIds.get(key) === data.runId &&
          state.activeThreadStreamIds.get(key) === data.streamId &&
          data.leaseOwner === record.leaseOwner &&
          (await this.#hasLiveThreadLease(resolvedPubSub, key, data.runId, record.leaseOwner))
        ) {
          this.abortRun(data.runId, resolvedPubSub);
        }
        return;
      }
      if (data.type === 'run-failed') {
        const eventStreamId = data.streamId ?? data.runId;
        const localRecord = data.streamId
          ? state.threadRunsByStreamId.get(data.streamId)
          : state.threadRunsById.get(data.runId);
        const remoteIdentity = state.remoteStreamIdentityByThread.get(key);
        const expectedOwner =
          acceptedLeaseOwnersByStreamId.get(eventStreamId) ??
          (remoteIdentity?.streamId === eventStreamId ? remoteIdentity.leaseOwner : undefined) ??
          localRecord?.leaseOwner;
        const { isFallback } = this.#resolveLeaseProvider(resolvedPubSub);
        if (expectedOwner !== undefined) {
          if (data.leaseOwner !== expectedOwner) return;
        } else if (
          !isFallback &&
          (data.leaseOwner === undefined ||
            !(await this.#hasLiveThreadLease(resolvedPubSub, key, data.runId, data.leaseOwner)))
        ) {
          return;
        }
        forgetAcceptedOwner(eventStreamId);
        const activeRunId = state.activeThreadRunIds.get(key);
        const activeStreamId = state.activeThreadStreamIds.get(key);
        if (
          (activeRunId !== undefined && activeRunId !== data.runId) ||
          (data.streamId !== undefined && activeStreamId !== undefined && activeStreamId !== data.streamId)
        ) {
          rememberTerminalRemoteStream(eventStreamId);
          removeEagerAbortListener(eventStreamId);
          return;
        }
        this.#forgetCallerSignalsForRun(state, data.runId);
        this.#forgetSignalAdmissionsForRun(state, key, data.runId);
        stopRemoteRunLeaseWatch(eventStreamId);
        clearActiveIfCurrent(data.runId, data.streamId);
        if (deferredRunsByStreamId.has(eventStreamId)) {
          // Replayed failure of a run that never persisted anything — drop it.
          discardDeferredRun(eventStreamId);
          seenStreamIds.delete(eventStreamId);
          await this.#drainPendingIdleSignals(state, resolvedPubSub, key, data.runId);
          wake();
          return;
        }
        let errorRun: AgentThreadRunRecord<any> | undefined;
        let remoteRun = remoteRuns.get(eventStreamId);
        if (!remoteRun) {
          errorRun = createRemoteRun(
            data.runId,
            eventStreamId,
            state.streamSeqByRunId.get(data.runId) ?? 1,
            data.leaseOwner ?? data.runId,
          );
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
        removeEagerAbortListener(eventStreamId);
        await this.#drainPendingIdleSignals(state, resolvedPubSub, key, data.runId);
        wake();
        return;
      }
      if (data.type === 'run-discarded') {
        // A discard tears down the active reader, so it is authenticated exactly
        // like the lifecycle terminals below rather than trusted on stream
        // identity alone: its sole publisher (#registerRunStrict's rollback)
        // signs it with the same owner token that signed the registration.
        // `#waitForRemoteRunToFinish` accepts a discard on stream identity alone
        // for its own documented reason (it only ends a wait); this path mutates
        // subscriber state, so it fails closed instead.
        const discardLocalRecord = state.threadRunsByStreamId.get(data.streamId);
        const discardIdentity = state.remoteStreamIdentityByThread.get(key);
        const discardExpectedOwner =
          acceptedLeaseOwnersByStreamId.get(data.streamId) ??
          (discardIdentity?.streamId === data.streamId ? discardIdentity.leaseOwner : undefined) ??
          discardLocalRecord?.leaseOwner;
        const { isFallback } = this.#resolveLeaseProvider(resolvedPubSub);
        if (discardExpectedOwner !== undefined) {
          if (data.leaseOwner !== discardExpectedOwner) return;
        } else if (!isFallback && data.leaseOwner === undefined) {
          return;
        }
        // The accepted owner for this stream id is deliberately retained: a
        // later replay of its `run-registered` must still be fenced against the
        // exact owner that was authenticated, not re-admitted under a new one.
        stopRemoteRunLeaseWatch(data.streamId);
        clearActiveIfCurrent(data.runId, data.streamId);
        localStreamIds.delete(data.streamId);
        replayedStreamIds.delete(data.streamId);
        for (let index = pendingRuns.length - 1; index >= 0; index--) {
          if (pendingRuns[index]?.streamId === data.streamId) pendingRuns.splice(index, 1);
        }
        discardDeferredRun(data.streamId);
        const remoteRun = remoteRuns.get(data.streamId);
        if (remoteRun) {
          remoteRun.done = true;
          while (remoteRun.waiters.length) remoteRun.waiters.shift()?.();
          while (remoteRun.finishWaiters.length) remoteRun.finishWaiters.shift()?.();
          remoteRuns.delete(data.streamId);
        }
        seenStreamIds.delete(data.streamId);
        if (activeReaderRunId === data.runId && activeReaderStreamId === data.streamId && currentReader) {
          try {
            void currentReader.cancel();
          } catch {
            // The reader may already be closed or errored; cancellation is
            // best-effort teardown of a consumer that is going away anyway.
          }
        }
        wake();
        return;
      }
      if (data.type === 'run-completed' || data.type === 'run-aborted' || data.type === 'run-suspended') {
        const eventStreamId = data.streamId ?? data.runId;
        const localRecord =
          state.threadRunsByStreamId.get(eventStreamId) ??
          (data.streamId === undefined ? state.threadRunsById.get(data.runId) : undefined);
        const remoteIdentity = state.remoteStreamIdentityByThread.get(key);
        const expectedOwner =
          acceptedLeaseOwnersByStreamId.get(eventStreamId) ??
          (remoteIdentity?.streamId === eventStreamId ? remoteIdentity.leaseOwner : undefined) ??
          localRecord?.leaseOwner;
        const { isFallback } = this.#resolveLeaseProvider(resolvedPubSub);
        if (expectedOwner !== undefined) {
          if (data.leaseOwner !== expectedOwner) return;
        } else if (
          !isFallback &&
          (data.leaseOwner === undefined ||
            !(await this.#hasLiveThreadLease(resolvedPubSub, key, data.runId, data.leaseOwner)))
        ) {
          return;
        }
        const activeRunId = state.activeThreadRunIds.get(key);
        const activeStreamId = state.activeThreadStreamIds.get(key);
        const currentStreamId = activeRunId === data.runId ? activeStreamId : undefined;
        // Delivery belongs to the exact stream even if a resume already made a
        // newer same-run segment active. Mark that segment's barrier first, then
        // refuse every thread-state mutation from the stale terminal.
        markRunTerminalDelivered(eventStreamId);
        if (activeRunId !== undefined && activeRunId !== data.runId) {
          forgetAcceptedOwner(eventStreamId);
          rememberTerminalRemoteStream(eventStreamId);
          removeEagerAbortListener(eventStreamId);
          return;
        }
        if (data.type === 'run-suspended') {
          const evictedStreamIds = rememberBoundedResumableTerminalStream(
            resumableTerminalStreamIdsByRunId,
            data.runId,
            eventStreamId,
          );
          for (const evictedStreamId of evictedStreamIds) forgetAcceptedOwner(evictedStreamId);
        } else {
          forgetAcceptedOwner(eventStreamId);
        }
        if (data.streamId !== undefined && currentStreamId !== undefined && data.streamId !== currentStreamId) {
          rememberTerminalRemoteStream(eventStreamId);
          removeEagerAbortListener(eventStreamId);
          return;
        }
        // Keep retired identities for a live same-run resume chain. Once the
        // run's lease is gone, stale registration/part events fail liveness
        // admission independently; clearing this set is therefore unnecessary
        // and would reopen older suspended segments during finalization races.
        stopRemoteRunLeaseWatch(eventStreamId);
        const deferredRecord = deferredRunsByStreamId.get(eventStreamId);
        if (options.withInitialHistory && data.type === 'run-completed' && data.status === 'success') {
          // Judge by publish time, not delivery time: backends such as Redis
          // Streams deliver the backlog after subscribe() returns.
          const completedAt = event.createdAt === undefined ? undefined : new Date(event.createdAt).getTime();
          if (historyReadAt === undefined || (completedAt !== undefined && completedAt <= historyReadAt)) {
            // Finished successfully before history was read, so storage holds all of it.
            storedStreamIds.add(eventStreamId);
          }
        }
        if (deferredRecord) {
          deferredRunsByStreamId.delete(eventStreamId);
          const bufferedRun = remoteRuns.get(eventStreamId);
          // Prefer the origin's explicit `persisted` verdict; fall back to the
          // clean-finish heuristic for events published by older origins.
          const flush =
            data.type === 'run-suspended' ||
            (data.type === 'run-completed' &&
              (data.persisted ?? (bufferedRun !== undefined && deferredRunEndedCleanly(bufferedRun.parts))));
          if (storedStreamIds.has(eventStreamId)) {
            discardDeferredRun(eventStreamId);
          } else if (flush) {
            enqueueRun(deferredRecord);
          } else {
            // Unpersisted terminal run (mid-stream failure surfaces as
            // `run-completed` on the wire, aborts as `run-aborted`): no stored
            // message backs its partial content, so it must not replay.
            discardDeferredRun(eventStreamId);
          }
        }
        if (data.type === 'run-suspended') {
          suspendedStreamIdsByRunId.set(data.runId, eventStreamId);
          noteRunHalf(data.runId);
          state.suspendedRunIds.add(data.runId);
          const record = state.threadRunsByStreamId.get(eventStreamId) ?? state.threadRunsById.get(data.runId);
          if (record) record.lifecycle = 'suspended';
        } else {
          clearActiveIfCurrent(data.runId, data.streamId);
        }
        if (data.type === 'run-aborted') {
          // Preserve-by-default abort: the queued follow-up input survives an
          // ordinary exact-run abort and is drained by the abort finalizer's
          // handoff. Cancelling it here would race that handoff and delete
          // preserved work; only the explicit `clearPendingSignals` flow —
          // owned by the control subscription and `abortThread` — cancels
          // queued input, and the admission tombstones must be retained so an
          // at-least-once redelivery of a preserved or already-consumed signal
          // cannot be re-admitted and executed twice.
        }
        if (data.type !== 'run-suspended') {
          this.#clearSuspendedRun(state, data.runId);
          this.#forgetCallerSignalsForRun(state, data.runId);
          highestLocalStreamSeqByRunId.delete(data.runId);
          for (const retiredStreamId of resumableTerminalStreamIdsByRunId.get(data.runId) ?? []) {
            forgetAcceptedOwner(retiredStreamId);
          }
          resumableTerminalStreamIdsByRunId.delete(data.runId);
        }
        const remoteRun = remoteRuns.get(eventStreamId);
        const abortingActiveReader =
          data.type === 'run-aborted' && activeReaderStreamId === eventStreamId && currentReader !== null;
        rememberTerminalRemoteStream(eventStreamId);
        localStreamIds.delete(eventStreamId);
        replayedStreamIds.delete(eventStreamId);
        removeEagerAbortListener(eventStreamId);
        if (data.type === 'run-aborted') {
          // Only an actively consumed segment gets the grace period. A terminal
          // for a queued/unconsumed remote proxy cannot be waiting on a visible
          // tool_start in this subscriber, so retain the old prompt close.
          if (remoteRun && !abortingActiveReader) {
            remoteRun.done = true;
            while (remoteRun.waiters.length) remoteRun.waiters.shift()?.();
            while (remoteRun.finishWaiters.length) remoteRun.finishWaiters.shift()?.();
            remoteRuns.delete(eventStreamId);
            seenStreamIds.delete(eventStreamId);
          }
        } else if (remoteRun) {
          remoteRun.done = true;
          while (remoteRun.waiters.length) remoteRun.waiters.shift()?.();
          while (remoteRun.finishWaiters.length) remoteRun.finishWaiters.shift()?.();
          remoteRuns.delete(eventStreamId);
          seenStreamIds.delete(eventStreamId);
        }
        // A run terminal can race the active tool's own abort rejection. Keep
        // reading briefly so an authoritative `tool-error` can cross this
        // subscriber; only then cancel the view and synthesize the abort. This
        // never delays the abort signal itself, and the bound prevents an
        // abort-ignoring stream from hanging the subscription.
        if (data.type === 'run-aborted' && abortingActiveReader && currentReader) {
          abortTerminalPending = true;
          clearAbortDrainTimer();
          armAbortDrain(data.runId, eventStreamId);
        }
        // An abort is not completion: the owner still needs to finish and drain
        // pending signals before idle messages can start.
        if (data.type === 'run-completed') {
          await this.#drainPendingIdleSignals(state, resolvedPubSub, key, data.runId);
        }
        wake();
      }
    };

    let historyReadAt: number | undefined;
    const storedStreamIds = new Set<string>();
    const suspendedStreamIdsByRunId = new Map<string, string>();
    /** streamSeq of each registered stream, per run. */
    const registeredSeqsByRunId = new Map<string, Map<string, number>>();
    /** Suspended halves whose run has since resumed: their prompts are already answered. */
    const answeredStreamIds = new Set<string>();
    // A run registering a later stream means its suspension was answered. A
    // lagging publisher can deliver that registration before the suspended
    // half's `run-suspended`, so check whichever arrives second. Only a later
    // stream answers: a resumed half that suspends again is still pending.
    function noteRunHalf(runId: string, registration?: { streamId: string; streamSeq: number }) {
      const registered = registeredSeqsByRunId.get(runId) ?? new Map<string, number>();
      if (registration) registered.set(registration.streamId, registration.streamSeq);
      registeredSeqsByRunId.set(runId, registered);
      const suspendedStreamId = suspendedStreamIdsByRunId.get(runId);
      if (suspendedStreamId === undefined) return;
      const suspendedSeq = registered.get(suspendedStreamId);
      for (const [streamId, streamSeq] of registered) {
        if (streamId === suspendedStreamId) continue;
        if (suspendedSeq === undefined || streamSeq > suspendedSeq) answeredStreamIds.add(suspendedStreamId);
      }
    }

    // The resumed half's registration can still be queued behind the suspended
    // half's parts; a local run already records which stream it moved to.
    const isAnsweredHalf = (runId: string, streamId: string) => {
      if (answeredStreamIds.has(streamId)) return true;
      // A run registers a later stream only after this one ended, so its
      // prompts were answered even if `run-suspended` hasn't arrived yet.
      const registered = registeredSeqsByRunId.get(runId);
      const seq = registered?.get(streamId);
      if (seq !== undefined && [...registered!.values()].some(other => other > seq)) return true;
      const current = state.threadRunsById.get(runId);
      return current !== undefined && current.streamId !== streamId && current.lifecycle !== 'suspended';
    };

    let eventTail = Promise.resolve();
    const queueEvent = (
      event: Parameters<EventCallback>[0],
      trustedRegistration?: AgentThreadRunRecord<any>,
      ack?: () => Promise<void>,
    ): Promise<void> => {
      if (done) return Promise.resolve();
      // Events are processed strictly in publish order, but each delivery is
      // acknowledged on its own outcome. Every delivered event is acked once it
      // has been inspected — including events this subscriber filters out —
      // because a persistent backend (Redis consumer groups) keeps unacked
      // deliveries pending for the lifetime of the subscription.
      const processed = eventTail.then(() => {
        if (done) return;
        return handleEvent(event, trustedRegistration);
      });
      // The tail must survive a failed event so later events still run.
      eventTail = processed.then(
        () => {},
        () => {},
      );
      // Returned rejection lets the backend nack and redeliver.
      return processed.then(() => ack?.());
    };

    const onEvent: EventCallback = (event, ack) => {
      if (done) return;
      return queueEvent(event, undefined, ack);
    };

    const registrationListener: ThreadRegistrationListener = (registration, trustedRecord) => {
      if (done) return;
      void queueEvent(
        {
          type: registration.type,
          id: `local-registration:${registration.streamId}`,
          createdAt: new Date(),
          runId: registration.runId,
          data: registration,
        },
        trustedRecord,
      ).catch(() => {});
    };

    const control = this.#ensureThreadControlSubscription(state, resolvedPubSub, key);
    control.references++;
    control.observers++;
    try {
      await control.ready;
      await resolvedPubSub.subscribe(topic, onEvent);
    } catch (error) {
      control.references--;
      control.observers--;
      this.#releaseUnusedThreadControlSubscription(state, key);
      throw error;
    }

    // Subscribe first, then load: parts published while history loads are held
    // by the subscription and filtered against it, so nothing falls in the gap.
    let historyChunk: ThreadHistoryChunk | undefined;
    let historyFilter: ((part: unknown, runId: string) => boolean) | undefined;
    let restoredApprovalChunks: ReturnType<typeof toolCallApprovalChunkFromStored>[] = [];
    if (options.withInitialHistory) {
      // Events already processed were published before this read, so any run
      // they completed is in storage.
      await eventTail;
      historyReadAt = Date.now();
      try {
        const history = await this.#loadThreadHistory(agent, options);
        historyChunk = {
          type: 'thread-history',
          runId: '',
          from: ChunkFrom.AGENT,
          payload: history,
        };
        const storedHistoryFilter = createThreadHistoryFilter(history.messages);
        historyFilter = storedHistoryFilter;
        restoredApprovalChunks = await this.#restoreStoredPendingApprovals(agent, options, history.messages, state);
        if (restoredApprovalChunks.length > 0) {
          const restoredToolCallIds = new Set(restoredApprovalChunks.map(chunk => chunk.payload.toolCallId));
          // A still-live remote run may replay the same prompt; the rebuilt
          // card already covers it.
          historyFilter = (part, runId) => {
            const allowed = storedHistoryFilter(part, runId);
            const typed = part as { type?: unknown; payload?: { toolCallId?: unknown } } | undefined;
            return (
              allowed && !(typed?.type === 'tool-call-approval' && restoredToolCallIds.has(typed.payload?.toolCallId))
            );
          };
        }
      } catch (error) {
        control.references--;
        control.observers--;
        this.#releaseUnusedThreadControlSubscription(state, key);
        await resolvedPubSub.unsubscribe(topic, onEvent).catch(() => {});
        throw error;
      }
    }

    const removeRegistrationListener = this.#registerThreadRegistrationListener(
      resolvedPubSub,
      key,
      registrationListener,
    );

    const currentRunId = activeRunId();
    const currentRecord = currentRunId ? state.threadRunsById.get(currentRunId) : undefined;
    if (currentRecord) {
      localStreamIds.add(currentRecord.streamId);
      acceptedLeaseOwnersByStreamId.set(currentRecord.streamId, currentRecord.leaseOwner);
      acceptedStreamSeqByStreamId.set(currentRecord.streamId, currentRecord.streamSeq);
      registerEagerAbortListener(currentRecord.runId, currentRecord.streamId);
      enqueueRun(currentRecord);
    }

    const unsubscribe = () => {
      if (done) return;
      done = true;
      removeRegistrationListener();
      for (const timer of remoteRunLeaseTimers.values()) clearTimeout(timer);
      remoteRunLeaseTimers.clear();
      control.references--;
      control.observers--;
      this.#releaseUnusedThreadControlSubscription(state, key);
      void resolvedPubSub.unsubscribe(topic, onEvent).catch(() => {});
      clearAbortDrainTimer();
      for (const streamId of [...eagerAbortListenersByStreamId.keys()]) removeEagerAbortListener(streamId);
      // Cancel current reader so the generator's inner loop breaks.
      if (currentReader) {
        try {
          void currentReader.cancel().catch(() => {
            // Cancellation is best-effort during unsubscribe.
          });
        } catch {
          // Cancellation is best-effort during unsubscribe.
        }
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
      __getCurrentRunRequestContext: () => {
        const record = activeReaderStreamId ? state.threadRunsByStreamId.get(activeReaderStreamId) : undefined;
        return record ? record.streamOptions.requestContext : currentRunRequestContext;
      },
      abort: abortOptions => this.abortThread({ ...options, ...abortOptions }, resolvedPubSub),
      unsubscribe,
      _waitForOutputDrain: waitForOutputDrain,
      stream: (async function* () {
        try {
          if (historyChunk) yield historyChunk;
          for (const chunk of restoredApprovalChunks) yield chunk;
          while (!done || pendingRuns.length > 0) {
            if (pendingRuns.length === 0) {
              await new Promise<void>(resolve => waiters.push(resolve));
              continue;
            }
            const run = pendingRuns.shift()!;
            // A local run can be read before its `run-registered` event arrives.
            noteRunHalf(run.runId, { streamId: run.streamId, streamSeq: run.streamSeq });
            // Local registered runs expose a multicast `createSubscriberStream`
            // giving this subscriber an independent fan-out view; remote runs are
            // already per-subscription streams fed by pubsub `stream-part` events.
            // Reading `output.fullStream` directly would let one subscriber lock
            // and drain the shared stream, starving every other subscriber.
            // Do not silently skip locked streams here: a locked fallback stream
            // means a caller is sharing a non-multicast stream.
            const subscriberStream = run.createSubscriberStream?.() ?? run.output.fullStream;
            const reader = subscriberStream.getReader();
            currentReader = reader as ReadableStreamDefaultReader<any>;
            activeReaderRunId = run.runId;
            activeReaderStreamId = run.streamId;
            currentRunRequestContext = run.streamOptions.requestContext;
            if (remoteRuns.has(run.streamId)) startRemoteRunLeaseWatch(run.runId, run.streamId);
            let readerReleased = false;
            let fullyDrained = false;
            let pendingAuthoritativeAbortPart: any;
            const partWithRunId = (part: any) =>
              part && typeof part === 'object' && !('runId' in part) ? { ...part, runId: run.runId } : part;
            const acceptSourceTerminal = () => {
              abortTerminalPending = false;
              clearAbortDrainTimer();
              // Every model-visible part has crossed this generator once the
              // terminal below is delivered. Mark the barrier before pausing
              // at `yield`; otherwise a caller awaiting `_waitForOutputDrain()`
              // immediately after consuming it would deadlock.
              fullyDrained = true;
              markOutputStreamDrained(run.output);
              // Drain non-visible trailers in the background to prevent
              // upstream backpressure while serving subsequent runs.
              readerReleased = true;
              void (async () => {
                try {
                  while (true) {
                    const { done: d } = await reader.read();
                    if (d) break;
                  }
                } catch {
                  // Background trailer draining is best-effort.
                }
                reader.releaseLock();
              })();
            };
            // A `start` covered by stored history is held back and sent before the
            // first live part of this stream, so a mid-run joiner sees the run begin.
            let heldStart: unknown;
            try {
              while (true) {
                const { value: part, done: streamDone } = await reader.read();
                if (streamDone) {
                  // A natural close, or the deliberate bounded abort fallback,
                  // means no later part can cross this subscriber view. Only an
                  // explicit subscription teardown leaves the segment un-drained.
                  fullyDrained = !done;
                  break;
                }
                const typedPart = part as any;
                const toolCallId =
                  typedPart?.payload && typeof typedPart.payload.toolCallId === 'string'
                    ? typedPart.payload.toolCallId
                    : undefined;
                const activeToolCallIds = activeToolCallIdsByRunId.get(run.runId) ?? new Set<string>();
                if (typedPart.type === 'tool-call' && toolCallId !== undefined && !abortTerminalPending) {
                  activeToolCallIds.add(toolCallId);
                  activeToolCallIdsByRunId.set(run.runId, activeToolCallIds);
                }
                const isSettlingActiveTool =
                  (typedPart.type === 'tool-result' || typedPart.type === 'tool-error') &&
                  toolCallId !== undefined &&
                  activeToolCallIds.has(toolCallId);
                if (isSettlingActiveTool) {
                  activeToolCallIds.delete(toolCallId!);
                  if (activeToolCallIds.size === 0) activeToolCallIdsByRunId.delete(run.runId);
                }
                const finishReason = typedPart.finishReason ?? typedPart.payload?.finishReason;
                const sourceTerminal =
                  typedPart.type === 'error' ||
                  typedPart.type === 'abort' ||
                  (typedPart.type === 'finish' && finishReason !== 'tool-calls');
                const authoritativeAbortTerminal = typedPart.type === 'abort';
                // `run-aborted` is a hard stop. During its bounded drain grace,
                // surface only terminals for tools that were already visible
                // before the abort, plus the source's matching abort boundary.
                // Some providers emit their source `abort` before an in-flight
                // tool observes cancellation. Hold that boundary until the
                // already-visible tool settles (or the bounded reader cancel
                // closes the source) so the authoritative tool payload is not
                // discarded as a trailer.
                const deferAuthoritativeAbort =
                  abortTerminalPending && authoritativeAbortTerminal && activeToolCallIds.size > 0;
                if (deferAuthoritativeAbort && pendingAuthoritativeAbortPart === undefined) {
                  pendingAuthoritativeAbortPart = typedPart;
                }
                const visibleAfterAbort = isSettlingActiveTool || authoritativeAbortTerminal;
                const shouldYieldPart = (!abortTerminalPending || visibleAfterAbort) && !deferAuthoritativeAbort;
                const acceptedSourceTerminal =
                  sourceTerminal && (!abortTerminalPending || (authoritativeAbortTerminal && !deferAuthoritativeAbort));
                if (acceptedSourceTerminal) {
                  // A matching source abort supersedes the synthetic fallback
                  // once every visible tool has settled. Competing terminals
                  // after `run-aborted` remain filtered until close/cancellation.
                  acceptSourceTerminal();
                }
                if (shouldYieldPart) {
                  const visiblePart = partWithRunId(typedPart);
                  if (
                    !isSignalChunkExcluded(visiblePart, options.hideSignals) &&
                    !storedStreamIds.has(run.streamId) &&
                    !(
                      (typedPart?.type === 'tool-call-approval' || typedPart?.type === 'tool-call-suspended') &&
                      isAnsweredHalf(run.runId, run.streamId)
                    ) &&
                    (!historyFilter || historyFilter(typedPart, run.runId))
                  ) {
                    // A `start` covered by stored history is held back and sent
                    // before the first live part, so a mid-run joiner sees the
                    // run begin.
                    const isPrompt =
                      typedPart?.type === 'tool-call-approval' || typedPart?.type === 'tool-call-suspended';
                    if (heldStart !== undefined && typedPart?.type !== 'start' && !isPrompt) {
                      yield heldStart;
                    }
                    if (!isPrompt) heldStart = undefined;
                    yield visiblePart;
                  } else if (typedPart?.type === 'start' && historyFilter && !storedStreamIds.has(run.streamId)) {
                    heldStart = visiblePart;
                  }
                }
                if (
                  abortTerminalPending &&
                  pendingAuthoritativeAbortPart !== undefined &&
                  activeToolCallIds.size === 0
                ) {
                  const abortPart = pendingAuthoritativeAbortPart;
                  pendingAuthoritativeAbortPart = undefined;
                  acceptSourceTerminal();
                  yield partWithRunId(abortPart);
                  break;
                }
                if (done || acceptedSourceTerminal) break;
              }
              // A source that closed without its own terminal still needs an
              // abort boundary. This covers both a prompt authoritative tool
              // error followed by close and the bounded cancellation fallback.
              if (!readerReleased && !done && abortTerminalPending) {
                // No later part can cross this subscriber once the source has
                // closed/cancelled, so certify stream drain before yielding.
                // An async generator pauses at `yield`; delaying the mark until
                // `finally` would deadlock callers that await the drain barrier
                // immediately after consuming this terminal.
                fullyDrained = true;
                markOutputStreamDrained(run.output);
                const abortPart = pendingAuthoritativeAbortPart ?? { type: 'abort', runId: run.runId };
                pendingAuthoritativeAbortPart = undefined;
                yield partWithRunId(abortPart);
                abortTerminalPending = false;
              }
            } finally {
              clearAbortDrainTimer();
              abortTerminalPending = false;
              activeToolCallIdsByRunId.delete(run.runId);
              stopRemoteRunLeaseWatch(run.streamId);
              currentReader = null;
              activeReaderRunId = null;
              activeReaderStreamId = null;
              currentRunRequestContext = undefined;
              if (!readerReleased) {
                reader.releaseLock();
              }
              if (fullyDrained) {
                if (!streamDrainedOutputs.has(run.output)) markOutputStreamDrained(run.output);
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
    return this.sendSignal<OUTPUT>(agent, this.#createMessageSignalInput(message), target, pubsub);
  }

  subscribeThreadEvents(
    agent: Agent<any, any, any, any>,
    scope: SubscribeAgentThreadEventsOptions,
    listener: AgentThreadEventListener,
    pubsub?: PubSub,
  ): () => void {
    const state = this.#getState(pubsub);
    const registration: ThreadEventListenerRegistration = { ...scope, agent, listener, lastCount: 0 };
    const count = this.#queuedMessageCount(state, registration);
    registration.lastCount = count;
    state.threadEventListeners.add(registration);
    try {
      listener({ type: 'queue-count-changed', count });
    } catch {
      // One listener that throws must not break its own registration flow.
    }
    let subscribed = true;
    return () => {
      if (!subscribed) return;
      subscribed = false;
      state.threadEventListeners.delete(registration);
    };
  }

  cancelQueuedMessages(
    agent: Agent<any, any, any, any>,
    target: CancelQueuedAgentMessagesOptions,
    pubsub?: PubSub,
  ): CancelQueuedAgentMessagesResult {
    const state = this.#getState(pubsub);
    const key = this.#threadKey(target.resourceId, target.threadId);
    const hasSignalIds = Array.isArray((target as { signalIds?: unknown }).signalIds);
    const hasQueueOwnerId = typeof (target as { queueOwnerId?: unknown }).queueOwnerId === 'string';
    if (hasSignalIds === hasQueueOwnerId) {
      throw new Error('cancelQueuedMessages requires exactly one of signalIds or queueOwnerId');
    }
    if (hasSignalIds) {
      const signalIds = new Set(target.signalIds);
      const result = this.#cancelPendingSignals(state, key, signalIds);
      this.#publish(pubsub, key, { type: 'signals-cancelled', signalIds: [...signalIds] });
      if (result.cancelledSignalIds.length) {
        this.#notifyThreadEvents(state);
      }
      return result;
    }
    const matches = (pending: PendingIdleSignal<any>) =>
      pending.agent === agent && pending.queueOwnerId === target.queueOwnerId;
    const cancelledSignalIds = this.#cancelIdleSignals(state, key, matches);
    if (cancelledSignalIds.length > 0) this.#notifyThreadEvents(state);
    return { cancelledSignalIds };
  }

  #cancelIdleSignals(
    state: AgentThreadRuntimeState,
    key: string,
    matches: (pending: PendingIdleSignal<any>) => boolean,
  ): string[] {
    const cancelledSignalIds: string[] = [];
    const queue = state.pendingIdleSignalsByThread.get(key);
    if (queue) {
      const remaining = queue.filter(pending => {
        if (!matches(pending)) return true;
        cancelledSignalIds.push(pending.signal.id);
        return false;
      });
      if (remaining.length === 0) state.pendingIdleSignalsByThread.delete(key);
      else state.pendingIdleSignalsByThread.set(key, remaining);
    }

    const draining = state.drainingIdleSignalsByThread.get(key);
    if (draining && matches(draining) && !draining.cancelled) {
      draining.cancelled = true;
      cancelledSignalIds.push(draining.signal.id);
    }
    return cancelledSignalIds;
  }

  #cancelPendingSignals(
    state: AgentThreadRuntimeState,
    key: string,
    signalIds?: ReadonlySet<string>,
  ): CancelQueuedAgentMessagesResult {
    // Ambiguous (unresolved) ids live outside the ordinary queues, so they
    // are never reported as cancelled here: unknown delivery must not be
    // claimed as cancelled-before-send. The `signals-cancelled` broadcast
    // below still informs subscribers, which drop the id only if it arrives
    // unadmitted.
    const matches = (signal: CreatedAgentSignal) => signalIds === undefined || signalIds.has(signal.id);
    const cancelled = new Set<string>();
    for (const queues of [state.preRunSignalsByThread, state.pendingSignalsByThread]) {
      const queue = queues.get(key);
      if (!queue) continue;
      const remaining = queue.filter(signal => {
        if (!matches(signal)) return true;
        cancelled.add(signal.id);
        return false;
      });
      if (remaining.length) queues.set(key, remaining);
      else queues.delete(key);
    }
    const draining = state.drainingPendingSignalsByThread.get(key);
    if (
      draining &&
      !draining.cancelled &&
      matches(draining.signal) &&
      (!draining.handedOff || signalIds === undefined)
    ) {
      draining.cancelled = true;
      if (!draining.handedOff) cancelled.add(draining.signal.id);
    }
    for (const id of this.#cancelIdleSignals(state, key, pending => matches(pending.signal))) cancelled.add(id);
    const control = state.threadControlSubscriptions.get(key);
    for (const id of cancelled) control?.admittedSignalIds.add(id);
    return { cancelledSignalIds: [...cancelled] };
  }

  queueMessage<OUTPUT = unknown>(
    agent: Agent<any, any, any, any>,
    message: AgentMessageInput,
    target: QueueAgentMessageOptions<OUTPUT>,
    pubsub?: PubSub,
  ): QueueAgentMessageResult<OUTPUT> {
    const state = this.#getState(pubsub);
    const acceptedAt = new Date();
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
    const signal = createMessageSignal(message, {
      id: this.#generateSignalMessageId(agent, { resourceId, threadId }),
      acceptedAt,
    });
    const queuedRunId = globalThis.crypto.randomUUID();
    // Preserve explicit cancellation, but don't inherit the active run's signal.
    const queuedStreamOptions = target.ifIdle?.streamOptions ?? {
      ...activeRecord?.streamOptions,
      abortSignal: undefined,
    };

    if (activeRecord || state.activeThreadRunIds.has(key)) {
      const idleQueue = state.pendingIdleSignalsByThread.get(key) ?? [];
      idleQueue.push({
        agent,
        signal,
        runId: queuedRunId,
        resourceId,
        threadId,
        streamOptions: queuedStreamOptions,
        queueOwnerId: target.queueOwnerId,
      });
      state.pendingIdleSignalsByThread.set(key, idleQueue);
      const control = this.#ensureThreadControlSubscription(state, pubsub, key);
      this.#notifyThreadEvents(state);
      if (activeRecord) {
        void this.#watchThreadRunCompletion(state, pubsub, key, activeRecord);
      }
      return {
        signal,
        runId: queuedRunId,
        accepted: control.ready.then(() => ({ action: 'deliver' as const, runId: queuedRunId })),
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
    const callerSignalId = signalInput.id;
    let key: string | undefined;
    let runId = target.runId;
    const activeBehavior = target.ifActive?.behavior ?? 'deliver';
    const idleBehavior = target.ifIdle?.behavior ?? 'wake';
    const onIdleSignalDiscarded = getIdleSignalDiscardHandler(target.ifIdle);
    const fullLogicalMessageIdentity = hasFullLogicalMessageIdentity(target.ifIdle);

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
        // of racing a fresh acquire against the finishing run.
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
          // Stale cross-pod entry. Clean it up from the local map, then let the lease decide.
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

    const resourceId = target.resourceId ?? activeRecord?.resourceId;
    const threadId = target.threadId ?? activeRecord?.threadId;
    const isActiveTarget = Boolean(
      runId && (activeRecord?.output.status === 'running' || (key && state.activeThreadRunIds.get(key) === runId)),
    );
    let signal = createSignal({
      ...signalInput,
      id: signalInput.id ?? this.#generateSignalMessageId(agent, { resourceId, threadId }),
      acceptedAt: new Date(),
    });

    // Resolve the selected branch only after admission determined whether the
    // signal is targeting active work or taking the idle path.
    signal = resolveDeliveryAttributes(
      signal,
      isActiveTarget ? target.ifActive?.attributes : target.ifIdle?.attributes,
    );

    const scopedRunId = target.runId;
    // Harness durable admission retries retain the public signal/run identity
    // but acquire a new dispatch attempt after the previous claim expires. A
    // permanently-pending native acknowledgement from the old attempt must not
    // pin that retry to the same cached Promise forever. This is intentionally
    // an AgentThreadStreamRuntime concern: the runtime owns this cache and the
    // stable signal-id tombstone that preserves same-payload idempotence.
    const admissionAttemptId = (target as SendAgentSignalOptions<OUTPUT> & { _signalAdmissionAttemptId?: string })
      ._signalAdmissionAttemptId;
    const signalPayloadKey = callerSignalPayloadKey(signalInput);
    const callerSignalKey =
      callerSignalId !== undefined && signalPayloadKey !== undefined
        ? [agent.id, resourceId ?? '', threadId ?? '', scopedRunId ?? '', callerSignalId, signalPayloadKey].join(
            '\u0000',
          )
        : undefined;
    if (callerSignalKey) {
      const cached = state.acceptedCallerSignals.get(callerSignalKey);
      if (cached) {
        const supersedesPendingAttempt =
          cached.status === 'pending' &&
          admissionAttemptId !== undefined &&
          cached.admissionAttemptId !== undefined &&
          cached.admissionAttemptId !== admissionAttemptId;
        if (
          supersedesPendingAttempt &&
          cached.result.runId !== undefined &&
          key !== undefined &&
          !this.#hasConfirmedSignalAdmission(state, key, cached.result.runId)
        ) {
          // The original attempt has reserved the thread but has not crossed
          // native admission yet. Keep the retry attached to its acknowledgement
          // rather than synthesizing a successful duplicate from its provisional
          // signal tombstone; that reservation may still lose the lease.
          return cached.result as SendAgentSignalResult<OUTPUT>;
        }
        if (!supersedesPendingAttempt && cached.status !== 'rejected') {
          return cached.result as SendAgentSignalResult<OUTPUT>;
        }
        state.acceptedCallerSignals.delete(callerSignalKey);
      }
    }
    const acceptSignal = <T extends SendAgentSignalResult<OUTPUT>>(
      result: T,
      acceptedRunId: string | undefined,
      cache = true,
    ): T => {
      if (callerSignalKey && cache) {
        const cached: CachedCallerSignal = {
          result: result as SendAgentSignalResult,
          admissionAttemptId,
          status: 'pending',
        };
        state.acceptedCallerSignals.set(callerSignalKey, cached);
        void result.accepted.then(
          () => {
            if (state.acceptedCallerSignals.get(callerSignalKey) === cached) cached.status = 'accepted';
          },
          () => {
            if (state.acceptedCallerSignals.get(callerSignalKey) === cached) cached.status = 'rejected';
          },
        );
        if (acceptedRunId) {
          const signalIds = state.callerSignalIdsByRunId.get(acceptedRunId) ?? new Set<string>();
          signalIds.add(callerSignalKey);
          state.callerSignalIdsByRunId.set(acceptedRunId, signalIds);
        }
      }
      return result;
    };

    const activeSignalDisposition =
      isActiveTarget && activeBehavior !== 'deliver' && key !== undefined
        ? this.#findSignalPayloadForRun(state, key, signal)
        : undefined;
    if (activeSignalDisposition?.disposition === 'conflict') {
      throw new Error(`Agent signal id "${signal.id}" was already accepted with a different payload`);
    }
    if (activeSignalDisposition?.disposition === 'duplicate') {
      return acceptSignal(
        {
          signal,
          runId: activeSignalDisposition.runId,
          accepted: Promise.resolve({ action: 'deliver' as const, runId: activeSignalDisposition.runId }),
        },
        activeSignalDisposition.runId,
        false,
      );
    }

    if (isActiveTarget && activeBehavior !== 'deliver') {
      runId ??= randomUUID();
      if (activeBehavior === 'persist') {
        if (!resourceId || !threadId) {
          throw new Error('resourceId and threadId are required to persist an active signal');
        }
        // Transient signals are never written to storage, so a `persist` behavior has nothing
        // to do with them — report the drop honestly as `discard` instead of `persist`.
        if (signal.transient) {
          return {
            signal,
            runId,
            accepted: Promise.resolve({ action: 'discard' as const }),
          };
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
      // A run is "blocking" while it is running or suspended awaiting tool approval. Both
      // states mean the run has already made model requests, so a follow-up signal must be
      // queued as a pending (next-turn) signal rather than folded into a not-yet-started
      // first request via the pre-run path below.
      if (activeRecord && this.#isThreadBlockingRun(state, activeRecord)) {
        key ??= this.#threadKey(activeRecord.resourceId, activeRecord.threadId);
        if (activeRecord.agent.id === agent.id) {
          // Same-agent active run: queue the signal for in-loop draining so it becomes
          // the next model input instead of waiting for the run to finish.
          const disposition = this.#rememberSignalPayloadForRun(state, key, runId, signal);
          if (disposition.disposition === 'conflict') {
            throw new Error(`Agent signal id "${signal.id}" was already accepted with a different payload`);
          }
          if (disposition.disposition === 'duplicate') {
            return acceptSignal(
              {
                signal,
                runId: disposition.runId,
                accepted: Promise.resolve({ action: 'deliver' as const, runId: disposition.runId }),
              },
              disposition.runId,
              false,
            );
          }
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
        const claimedOwnerDiscovery = isLocalReservedRun ? state.claimedThreadOwnerDiscoveries.get(key) : undefined;
        if (claimedOwnerDiscovery) {
          const discoveryKey = key;
          const discoveredRunId = randomUUID();
          const disposition = this.#rememberSignalPayloadForRun(state, key, discoveredRunId, signal, {
            admissionAttemptId,
            allowAttemptSupersede: true,
          });
          if (disposition.disposition === 'conflict') {
            throw new Error(`Agent signal id "${signal.id}" was already accepted with a different payload`);
          }
          if (disposition.disposition === 'duplicate') {
            return acceptSignal(
              {
                signal,
                runId: disposition.runId,
                accepted: Promise.resolve({ action: 'deliver' as const, runId: disposition.runId }),
              },
              disposition.runId,
              false,
            );
          }
          const accepted = this.#deliverAfterClaimedOwnerDiscovery<OUTPUT>(
            this.#getPubSub(pubsub),
            discoveryKey,
            discoveredRunId,
            signal,
            claimedOwnerDiscovery,
            () => this.#forgetSignalAdmission(state, discoveryKey, discoveredRunId, signal),
            () => this.#forgetSignalAdmission(state, discoveryKey, discoveredRunId, signal),
            fullLogicalMessageIdentity,
            onIdleSignalDiscarded,
          );
          void accepted.catch(() => {});
          return acceptSignal(
            { signal, accepted, runId: discoveredRunId },
            discoveredRunId,
            !fullLogicalMessageIdentity,
          );
        }
        if (isLocalReservedRun) {
          const disposition = this.#rememberSignalPayloadForRun(state, key, runId, signal);
          if (disposition.disposition === 'conflict') {
            throw new Error(`Agent signal id "${signal.id}" was already accepted with a different payload`);
          }
          if (disposition.disposition === 'duplicate') {
            return acceptSignal(
              {
                signal,
                runId: disposition.runId,
                accepted: Promise.resolve({ action: 'deliver' as const, runId: disposition.runId }),
              },
              disposition.runId,
              false,
            );
          }
          const queue = state.preRunSignalsByThread.get(key) ?? [];
          queue.push(signal);
          state.preRunSignalsByThread.set(key, queue);
        }
        const deliveredRunId = runId;
        const publication = this.#publishAndWait(pubsub, key, {
          type: 'signal-enqueued',
          runId: deliveredRunId,
          signal: this.#serializeSignal(signal),
          sourceId: this.#getSourceId(),
          preRun: isLocalReservedRun,
        });
        void publication.catch(() => {});
        const accepted = isLocalReservedRun
          ? Promise.resolve({ action: 'deliver' as const, runId: deliveredRunId })
          : publication.then(() => ({ action: 'deliver' as const, runId: deliveredRunId }));
        return acceptSignal(
          {
            signal,
            runId: deliveredRunId,
            accepted,
          },
          deliveredRunId,
        );
      }
    }

    if (!resourceId || !threadId) {
      throw new Error('No active agent run found for signal target');
    }

    runId ??= randomUUID();
    key ??= this.#threadKey(resourceId, threadId);
    if (idleBehavior === 'persist') {
      if (signal.transient) {
        return { signal, runId, accepted: Promise.resolve({ action: 'discard' as const }) };
      }
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
      if (idleBehavior === 'discard') onIdleSignalDiscarded?.();
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
    const failClosedOnLeaseError = Boolean(
      (target.ifIdle as { _failClosedOnLeaseError?: unknown } | undefined)?._failClosedOnLeaseError,
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

      // A full logical message owns a response stream and cannot wait behind
      // a foreign reservation: this branch would otherwise report `deliver`
      // while the queued idle signal later starts with its original response
      // identity after the active owner finishes.
      if (fullLogicalMessageIdentity) {
        onIdleSignalDiscarded?.();
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

      const disposition = this.#rememberSignalPayloadForRun(state, key, runId, signal);
      if (disposition.disposition === 'conflict') {
        throw new Error(`Agent signal id "${signal.id}" was already accepted with a different payload`);
      }
      if (disposition.disposition === 'duplicate') {
        return acceptSignal(
          {
            signal,
            runId: disposition.runId,
            accepted: Promise.resolve({ action: 'deliver' as const, runId: disposition.runId }),
          },
          disposition.runId,
          false,
        );
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

    const idleSignalDisposition = this.#rememberSignalPayloadForRun(state, key, runId, signal, {
      admissionAttemptId,
      allowAttemptSupersede: true,
    });
    if (idleSignalDisposition.disposition === 'conflict') {
      throw new Error(`Agent signal id "${signal.id}" was already accepted with a different payload`);
    }
    if (idleSignalDisposition.disposition === 'duplicate') {
      return acceptSignal(
        {
          signal,
          runId: idleSignalDisposition.runId,
          accepted: Promise.resolve({ action: 'deliver' as const, runId: idleSignalDisposition.runId }),
        },
        idleSignalDisposition.runId,
        false,
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
    const reservedLeaseOwner = this.#leaseOwnerForRun(state, reservedRunId);
    const hasForeignRunIdentity = () => {
      const registeredRun = state.threadRunsById.get(reservedRunId);
      if (registeredRun && this.#threadKey(registeredRun.resourceId, registeredRun.threadId) !== reservedKey) {
        return true;
      }
      return [
        state.threadKeysByRunId.get(reservedRunId),
        state.pendingIdleThreadKeysByRunId.get(reservedRunId),
        state.inflightIdleThreadKeysByRunId.get(reservedRunId),
      ].some(existingKey => existingKey !== undefined && existingKey !== reservedKey);
    };
    const rollbackLocalReservation = (rejectRun: boolean, announceAbort = rejectRun, preserveOtherRunState = false) => {
      if (preserveOtherRunState) return;
      onRunRejected?.();
      if (reserveBeforeIdleWake) {
        this.#releaseReservedRun(state, pubsub, reservedKey, reservedRunId, {
          cleanupPrepared: true,
          clearAbort: true,
          rejectOutputWaiters: rejectRun,
          announceAbort,
        });
      } else {
        state.inflightIdleThreadKeysByRunId.delete(reservedRunId);
        state.inflightIdleAgentIdsByRunId.delete(reservedRunId);
        this.#forgetCallerSignalsForRun(state, reservedRunId);
        if (rejectRun) {
          this.#forgetSignalAdmissionsForRun(state, reservedKey, reservedRunId);
          this.rejectUnregisteredRun(reservedRunId, pubsub);
        }
      }
    };
    const cleanupDefiniteClaimedOwnerFailure = () => {
      const preserveOtherRunState =
        hasForeignRunIdentity() || state.leaseOwnerTokensByRunId.get(reservedRunId) !== reservedLeaseOwner;
      rollbackLocalReservation(false, false, preserveOtherRunState);
      this.#forgetSignalAdmission(state, reservedKey, reservedRunId, signal);
      if (!preserveOtherRunState && state.leaseOwnerTokensByRunId.get(reservedRunId) === reservedLeaseOwner) {
        state.leaseOwnerTokensByRunId.delete(reservedRunId);
        this.#stopLeaseRenewal(resolvedPubSub, reservedRunId);
      }
    };
    // First acquire the cross-process lease via pubsub; on win, kick off the stream and
    // resolve a `wake` accepted result carrying the owned stream. On loss, hand the user
    // signal off to the winning process via signal-enqueued and resolve a `deliver` result
    // (the signal was queued onto the winning run, not run locally).
    const accepted: Promise<SendAgentSignalAccepted<OUTPUT>> = (async () => {
      const localClaimedOwner = state.claimedThreadOwners.get(reservedKey);
      const rejectClaimedOwnerLineage = fullLogicalMessageIdentity;
      if (localClaimedOwner && rejectClaimedOwnerLineage) {
        onIdleSignalDiscarded?.();
        cleanupDefiniteClaimedOwnerFailure();
        return { action: 'discard' as const };
      }
      if (localClaimedOwner) {
        if (
          state.inflightIdleThreadKeysByRunId.get(reservedRunId) === reservedKey &&
          state.inflightIdleAgentIdsByRunId.get(reservedRunId) === agent.id
        ) {
          state.inflightIdleThreadKeysByRunId.delete(reservedRunId);
          state.inflightIdleAgentIdsByRunId.delete(reservedRunId);
        }
        if (state.activeThreadRunIds.get(reservedKey) === reservedRunId) {
          state.activeThreadRunIds.delete(reservedKey);
        }
        state.threadKeysByRunId.delete(reservedRunId);
        let localAcceptance: ClaimedThreadOwnerStartResult<OUTPUT> | undefined;
        try {
          localAcceptance = await this.#startClaimedIdleRun(
            state,
            resolvedPubSub,
            reservedKey,
            localClaimedOwner,
            reservedRunId,
            signal,
            Date.now() + AGENT_THREAD_OWNER_ACCEPTANCE_TIMEOUT_MS,
            () => state.claimedThreadOwners.get(reservedKey)?.unsubscribe === localClaimedOwner.unsubscribe,
            finishingLeaseOwnerRunId,
            target.ifIdle?.streamOptions,
          );
        } catch (error) {
          if (error instanceof ClaimedOwnerPreAdmissionError) cleanupDefiniteClaimedOwnerFailure();
          throw error;
        }
        if (!localAcceptance) {
          cleanupDefiniteClaimedOwnerFailure();
          throw new Error(`Claimed thread owner could not acquire the execution lease for ${reservedKey}`);
        }
        if (localAcceptance.error) {
          if (localAcceptance.preAdmission) cleanupDefiniteClaimedOwnerFailure();
          throw new Error(localAcceptance.error);
        }
        if (!localAcceptance.output) {
          // The signal joined a run that was still in flight, so nothing ran here.
          return { action: 'deliver' as const, runId: localAcceptance.runId };
        }
        // This owner ran the turn. Reporting `deliver` would claim no run started,
        // leaving the caller waiting on a delivery that already happened and
        // re-sending into a run it believes is still in flight.
        return { action: 'wake' as const, runId: localAcceptance.runId, output: localAcceptance.output };
      }

      if (target.ifIdle?.requireClaimedOwner) {
        const discovery = this.#findClaimedThreadOwner(resolvedPubSub, reservedKey, { includeLocal: false });
        state.claimedThreadOwnerDiscoveries.set(reservedKey, discovery);
        try {
          return await this.#deliverAfterClaimedOwnerDiscovery<OUTPUT>(
            resolvedPubSub,
            reservedKey,
            reservedRunId,
            signal,
            discovery,
            cleanupDefiniteClaimedOwnerFailure,
            cleanupDefiniteClaimedOwnerFailure,
            rejectClaimedOwnerLineage,
            onIdleSignalDiscarded,
          );
        } finally {
          if (state.claimedThreadOwnerDiscoveries.get(reservedKey) === discovery) {
            state.claimedThreadOwnerDiscoveries.delete(reservedKey);
          }
          if (state.activeThreadRunIds.get(reservedKey) === reservedRunId) {
            state.activeThreadRunIds.delete(reservedKey);
          }
          state.threadKeysByRunId.delete(reservedRunId);
        }
      }

      try {
        await this.#ensureThreadControlSubscription(state, resolvedPubSub, reservedKey).ready;
      } catch (error) {
        this.releaseThreadRunReservation(reservedRunId, resolvedPubSub);
        throw error;
      }

      // Fail-open on pubsub errors: if the lease backend is unreachable we treat the
      // call as "acquired" so the caller still gets a response. The tradeoff is that
      // if multiple processes hit the same pubsub failure simultaneously they can each
      // start a stream for the same thread (the bug this lease is supposed to prevent),
      // but failing closed would silently drop user messages on any Redis blip which
      // is the worse failure mode. Lease TTL + renewal still bound the duplicate
      // window to a single run, and the next clean acquireLease re-serializes callers.
      let lease: { acquired: boolean; owner?: string };
      try {
        lease = finishingLeaseOwnerRunId
          ? await this.#acquireOrTransferThreadLease(
              resolvedPubSub,
              reservedKey,
              reservedRunId,
              finishingLeaseOwnerRunId,
              { failClosed: failClosedOnLeaseError },
            )
          : failClosedOnLeaseError
            ? await leaseProvider.acquireLease(reservedKey, reservedLeaseOwner, AGENT_THREAD_LEASE_TTL_MS)
            : await leaseProvider
                .acquireLease(reservedKey, reservedLeaseOwner, AGENT_THREAD_LEASE_TTL_MS)
                .catch(() => ({ acquired: true as boolean, owner: reservedLeaseOwner as string | undefined }));
      } catch (error) {
        rollbackLocalReservation(false);
        state.leaseOwnerTokensByRunId.delete(reservedRunId);
        this.#forgetSignalAdmission(state, reservedKey, reservedRunId, signal);
        throw new AgentThreadSignalAdmissionError(
          'lease-unavailable',
          'Agent signal admission could not acquire the thread lease',
          error,
        );
      }

      if (!lease.acquired) {
        // Lost the wake race to another process. Roll back our optimistic local reservation
        // so we don't trip our own activeThreadRunIds check on a follow-up.
        rollbackLocalReservation(false);
        state.leaseOwnerTokensByRunId.delete(reservedRunId);

        // A full logical message must never be forwarded into the active run that won
        // this distributed wake race. The caller can retry through its owned-turn path;
        // publishing here would lose the response identity at the winning process.
        if (activeBehavior === 'discard' || fullLogicalMessageIdentity) {
          onIdleSignalDiscarded?.();
          this.#forgetSignalAdmission(state, reservedKey, reservedRunId, signal);
          return { action: 'discard' as const };
        }

        // Forward the user signal to the winning runId so the message is not dropped.
        // Await the publish so that callers using `accepted` resolution as their
        // "safe to exit" boundary (e.g. a serverless Lambda holding the request open
        // via waitUntil) don't tear down before the enqueue lands on the broker.
        try {
          const winnerRunId = lease.owner ? this.#runIdFromLeaseOwner(lease.owner) : undefined;
          if (!winnerRunId) {
            throw new Error('Agent thread idle wake lost its lease without an owning run');
          }
          await this.#publishAndWait(pubsub, reservedKey, {
            type: 'signal-enqueued',
            runId: winnerRunId,
            signal: this.#serializeSignal(signal),
            sourceId: this.#getSourceId(),
          });
          return { action: 'deliver' as const, runId: winnerRunId };
        } catch (error) {
          // No owner accepted this signal. Remove only this attempt's matching
          // tombstone so a retry can make progress instead of falsely replaying
          // a delivery that never reached the broker.
          this.#forgetSignalAdmission(state, reservedKey, reservedRunId, signal);
          throw error;
        }
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
        const leaseOwner = state.leaseOwnerTokensByRunId.get(reservedRunId) ?? reservedLeaseOwner;
        try {
          await this.#publishTerminalAndWait(pubsub, reservedKey, {
            type: 'run-failed',
            runId: reservedRunId,
            error: getErrorFromUnknown(error).message,
            leaseOwner,
          });
        } catch {
          // Stream setup failure remains authoritative even when its best-effort
          // distributed terminal cannot be delivered within the bounded fence.
        }
        rollbackLocalReservation(true, false);
        if (!reserveBeforeIdleWake) {
          this.#releaseThreadLease(pubsub, reservedKey, reservedRunId);
          void this.#drainPendingIdleSignals(state, pubsub, reservedKey);
        }
        // The authenticated terminal above is this wake's only run-failed
        // publish; delete it from the topic once it has been observed.
        this.#trimFailedRun(pubsub, reservedKey, {
          agent,
          streamOptions: target.ifIdle?.streamOptions ?? {},
          runId: reservedRunId,
        });
        throw error;
      }
    })();
    // Attach a detached no-op catch so that if stream setup throws (a misconfigured
    // agent: no/unsupported model, FGA denial) and the caller never awaits
    // `result.accepted`, the rejection does not surface as an unhandled rejection.
    // Callers that opt in to `accepted` still see the rejection via their own
    // await/catch — `accepted` itself remains rejectable; only this detached branch is
    // swallowed.
    void accepted.finally(() => this.#releaseUnusedThreadControlSubscription(state, reservedKey)).catch(() => {});

    const output: Promise<MastraModelOutput<unknown>> = accepted.then(result => {
      if (result.action !== 'wake') {
        const destinationRunId = 'runId' in result ? result.runId : runId;
        throw new Error(`Agent thread idle wake was delivered to run "${destinationRunId}" in another process`);
      }
      // SAFETY: `accepted` resolves with the idle run's model output; the
      // union's non-wake members are ruled out by the action check above.
      return result.output as unknown as MastraModelOutput<unknown>;
    });
    void output.catch(() => {});

    return acceptSignal({ signal, runId, accepted, output }, runId);
  }
}

export const agentThreadStreamRuntime = new AgentThreadStreamRuntime();
