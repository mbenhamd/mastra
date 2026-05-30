/**
 * Harness v1 — event surface (§10).
 *
 * `HarnessEvent` is a discriminated union of every event the harness can
 * emit. Every event flows through `EventEmitter.emit()`; subscribers see
 * a fully-stamped event with `id`, `timestamp`, and (where relevant)
 * `sessionId`.
 *
 * IDs are scoped to an emitter: `harness-v1:<epoch>:<seq>`. The epoch is regenerated
 * whenever the emitter is constructed (i.e. process start, eviction +
 * rehydration), so SSE clients can detect a regenerated emitter and reset
 * their replay cursor (§10.5).
 *
 * Subscribers see only events emitted after `subscribe()` returns. Remote
 * callers that need history replay query the durable session event ledger.
 */

import { randomUUID } from 'node:crypto';

import type {
  ChannelActionReceipt,
  ChannelOutboxItem,
  GoalJudgeDecision,
  GoalState,
  JsonValue,
  SessionRecord,
} from '../../storage/domains/harness';

import { HarnessEventSerializationError, HarnessValidationError } from './errors';
import type { EventSerializationReason, HarnessStorageOperation, HarnessStorageSubject } from './errors';
import type { SessionLifecycleState, TokenUsage } from './session';
import type { PermissionPolicy, ToolCategory } from './types';

// ---------------------------------------------------------------------------
// Event base.
// ---------------------------------------------------------------------------

/**
 * Common fields stamped on every event. `sessionId` is set when the event
 * originated on a Session emitter; harness-level events (registry, lifecycle
 * across all sessions, intervals) leave it unset.
 *
 * `signalId` correlates an event back to the `message()` call that produced
 * it. `queuedItemId` correlates events back to a `queue()` item. Subagent
 * events also carry `subagentSessionId` so a parent subscriber can route by
 * origin (§10.6).
 */
export interface HarnessEventBase {
  /** Monotonic-within-emitter id formatted as `harness-v1:<epoch>:<seq>`. */
  id: string;
  timestamp: number;
  sessionId?: string;
  subagentSessionId?: string;
  runId?: string;
  signalId?: string;
  queuedItemId?: string;
}

// ---------------------------------------------------------------------------
// Lifecycle / state events (§10.2).
// ---------------------------------------------------------------------------

export interface SessionCreatedEvent extends HarnessEventBase {
  type: 'session_created';
  resourceId: string;
  threadId: string;
  parentSessionId?: string;
  modeId: string;
  modelId: string;
}

export interface SessionClosingEvent extends HarnessEventBase {
  type: 'session_closing';
  reason: 'requested' | 'shutdown';
  closingAt: number;
  closeDeadlineAt: number;
}

export interface SessionClosedEvent extends HarnessEventBase {
  type: 'session_closed';
  // §5.5: `closedAt` is written only on an explicit close request, so the
  // terminal `session_closed` event always carries `reason: 'requested'`.
  // Shutdown does NOT close live sessions — it evicts them (`session_evicted`
  // reason 'shutdown'), which is non-terminal and leaves `closedAt` unset.
  reason: 'requested';
}

export interface SessionEvictedEvent extends HarnessEventBase {
  type: 'session_evicted';
  reason: 'idle' | 'pressure' | 'pinned_timeout' | 'shutdown' | 'lease_lost';
}

/**
 * Session re-loaded from storage into the live cache on next access (§10.2).
 * Non-terminal observer notification; the durable session is unchanged.
 */
export interface SessionHydratedEvent extends HarnessEventBase {
  type: 'session_hydrated';
}

/**
 * Process shutdown (§10.2). Harness-scoped only — delivered to
 * `harness.subscribe(...)` and NOT part of per-session SSE replay. Sessions
 * persist; this is a process-lifecycle notification, not a session close.
 */
export interface HarnessShutdownEvent extends HarnessEventBase {
  type: 'harness_shutdown';
}

export interface ModeChangedEvent extends HarnessEventBase {
  type: 'mode_changed';
  modeId: string;
  previousModeId: string;
}

export interface ModelChangedEvent extends HarnessEventBase {
  type: 'model_changed';
  modelId: string;
  previousModelId: string;
}

/**
 * Cumulative session token usage changed after a turn committed its delta
 * (§10.2). `usage` is the new running total for the session.
 */
export interface TokenUsageChangedEvent extends HarnessEventBase {
  type: 'token_usage_changed';
  usage: TokenUsage;
}

export interface StateChangedEvent extends HarnessEventBase {
  type: 'state_changed';
  // §10.2: the full post-commit `session.state` so subscribers can sync durable
  // state from the event alone, plus the top-level keys whose value changed.
  // This is durable session state, NOT the debounced render snapshot — see
  // §10.2's note that `display_state_changed` is not a v1 built-in event.
  //
  // `session.state` is `unknown` and may be a scalar, array, or plain-object
  // root, so this is the full `JsonValue` (not narrowed to an object). For a
  // non-object root change, `changedKeys` carries the `'$'` root sentinel; the
  // new root value is read from this `state` field.
  state: JsonValue;
  changedKeys: string[];
}

// ---------------------------------------------------------------------------
// Permission events (§4.2e).
//
// Emitted whenever the session's permission rules or session-scoped grants
// change. Exactly one of `category` / `toolName` is set on each event so
// subscribers can route to per-category vs per-tool views without
// inspecting payload shape.
// ---------------------------------------------------------------------------

export interface PermissionGrantedEvent extends HarnessEventBase {
  type: 'permission_granted';
  category?: ToolCategory;
  toolName?: string;
}

export interface PermissionRevokedEvent extends HarnessEventBase {
  type: 'permission_revoked';
  category?: ToolCategory;
  toolName?: string;
}

export interface PermissionPolicyChangedEvent extends HarnessEventBase {
  type: 'permission_policy_changed';
  category?: ToolCategory;
  toolName?: string;
  oldPolicy: PermissionPolicy | undefined;
  newPolicy: PermissionPolicy;
}

// ---------------------------------------------------------------------------
// Turn events (§10.2).
// ---------------------------------------------------------------------------

export interface AgentStartEvent extends HarnessEventBase {
  type: 'agent_start';
  runId: string;
  /** Run-start overrides committed for this turn, when any were applied. */
  overrides?: { model?: string; mode?: string; yolo?: boolean };
}

/**
 * Streaming assistant text (§10.2). One `text_delta` per text chunk the model
 * streams within a turn. `runId` identifies the run; `signalId` attributes the
 * delta to a specific accepted signal when known. (§10.2 defines no
 * message-boundary events — `text_delta` is the only streaming-text event.)
 */
export interface TextDeltaEvent extends HarnessEventBase {
  type: 'text_delta';
  runId: string;
  signalId?: string;
  delta: string;
}

/**
 * Tool-call lifecycle (§10.2). `tool_start` when the (complete) tool call is
 * issued; `tool_end` when it resolves. Public payloads are JSON-safe
 * projections (raw non-JSON tool objects stay inside the runtime). §10.2 has no
 * incremental tool-input-streaming or tool-progress events — tools surface
 * progress via §10.3 custom events instead.
 */
export interface ToolStartEvent extends HarnessEventBase {
  type: 'tool_start';
  runId: string;
  toolCallId: string;
  toolName: string;
  input: unknown;
}

export interface ToolEndEvent extends HarnessEventBase {
  type: 'tool_end';
  runId: string;
  toolCallId: string;
  toolName: string;
  output: unknown;
  isError: boolean;
}

export interface AgentEndEvent extends HarnessEventBase {
  type: 'agent_end';
  runId: string;
  finishReason: string;
  usage: TokenUsage;
}

/**
 * Diagnostic/run-surface error (§10.2). `signalId` may attribute where the
 * runtime noticed the error, but promise settlement uses the OperationEvents
 * (`signal_failed` / `queue_failed`), not this event.
 */
export interface TurnErrorEvent extends HarnessEventBase {
  type: 'error';
  runId?: string;
  signalId?: string;
  error: { code: string; message: string };
}

// ---------------------------------------------------------------------------
// Operation settlement events (§10.2). These — not `agent_end` — are the
// promise/SDK settlement boundary for admitted work. One run can answer
// several signals, so `agent_end` alone never identifies which `signal(...)`
// or `queue(...)` call completed. `result` is scoped to the operation
// identified by `signalId` / `queuedItemId`, projected from the durable
// per-signal result evidence (§5.1d), never from the run aggregate.
// ---------------------------------------------------------------------------

export interface SignalCompletedEvent extends HarnessEventBase {
  type: 'signal_completed';
  runId: string;
  signalId: string;
  admissionId?: string;
  result: unknown;
}

export interface SignalFailedEvent extends HarnessEventBase {
  type: 'signal_failed';
  signalId: string;
  runId?: string;
  admissionId?: string;
  error: { code: string; message: string };
}

export interface QueueCompletedEvent extends HarnessEventBase {
  type: 'queue_completed';
  runId: string;
  queuedItemId: string;
  signalId: string;
  admissionId?: string;
  result: unknown;
}

export interface QueueFailedEvent extends HarnessEventBase {
  type: 'queue_failed';
  queuedItemId: string;
  signalId?: string;
  runId?: string;
  admissionId?: string;
  error: { code: string; message: string };
}

// ---------------------------------------------------------------------------
// Suspension events (§10.2). Emitted after the durable-parking barrier so
// any subscriber observing the event can reconstruct the pending state from
// storage (§5.4).
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Suspension — tool / question / plan needs user input (§10.2). The closed
// union has FOUR pending shapes (no generic `suspension_required` and no
// `suspension_resolved` — resolution is observed via the inbox response
// transition + display snapshot). `source: 'subagent'` carries the parent-side
// `subagentToolCallId` + child `subagentSessionId`; clients post the response
// to the child session's inbox. Sandbox/path-access prompts project to
// `question_pending` (§10.2: no dedicated sandbox event).
// ---------------------------------------------------------------------------

type SuspensionSource = { source: 'parent' } | { source: 'subagent'; subagentToolCallId: string; subagentSessionId: string };

export type ToolApprovalRequiredEvent = HarnessEventBase & {
  type: 'tool_approval_required';
  runId: string;
  itemId: string;
  requestedAt: number;
  toolCallId: string;
  toolName: string;
  toolCategory?: string;
  /** Reasons the tool requires approval (§10.2). Empty when none are recorded. */
  approvalReasons: string[];
  input: unknown;
} & SuspensionSource;

export type ToolSuspensionRequiredEvent = HarnessEventBase & {
  type: 'tool_suspension_required';
  runId: string;
  itemId: string;
  requestedAt: number;
  toolCallId: string;
  toolName: string;
  suspendData: unknown;
} & SuspensionSource;

export type QuestionPendingEvent = HarnessEventBase & {
  type: 'question_pending';
  runId: string;
  itemId: string;
  requestedAt: number;
  toolCallId: string;
  question: string;
  options?: { label: string; description?: string }[];
  selectionMode?: 'single_select' | 'multi_select';
} & SuspensionSource;

export type PlanApprovalRequiredEvent = HarnessEventBase & {
  type: 'plan_approval_required';
  runId: string;
  itemId: string;
  requestedAt: number;
  toolCallId: string;
  title: string;
  plan: string;
} & SuspensionSource;

// §10.2: session-wide cancellation is not a public HarnessEventV1 event. The
// observable effect is the rejected operation promises + cleared queue +
// updated display snapshot; there is no task_cancellation_requested event.

// ---------------------------------------------------------------------------
// Queue lifecycle events (§10.2). These are *lifecycle* notifications, not the
// settlement boundary: the queued operation settles through the OperationEvents
// above (`queue_completed` / `queue_failed`, keyed by `queuedItemId` /
// `signalId`), NOT through `agent_end`. One run can answer several queued or
// signalled operations, so `agent_end` alone never identifies which `queue()`
// call finished.
// §10.2: there is no queue-lifecycle event family (no queue_item_started /
// replayed / expired / queue_full_dropped). Queued work settles through the
// OperationEvents above (`queue_completed` / `queue_failed`); the drained
// turn's `agent_start` marks the run boundary; backpressure / expiry / drop
// surface as rejected operation promises, not events.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Attachment events (§10.2, session-scoped).
// ---------------------------------------------------------------------------

export interface AttachmentUploadedEvent extends HarnessEventBase {
  type: 'attachment_uploaded';
  attachmentId: string;
  name: string;
  mimeType: string;
  bytes: number;
}

export interface AttachmentDeletedEvent extends HarnessEventBase {
  type: 'attachment_deleted';
  attachmentId: string;
}

// ---------------------------------------------------------------------------
// Channel events (§14 / §10.2). Best-effort projections of the durable Channel*
// ledger rows (ingress inbox, action token/receipt, outbox) — NOT the durable
// recovery/dispatch substrate itself. Harness-scoped (carry harnessName +
// channelId); session-scoped when a binding/session is known.
//
// EMISSION READINESS: the three `channel_outbox_*` events are emitted today by
// the outbox enqueue/dispatch path. The `channel_ingress_*` and
// `channel_action_*` events are defined here as part of the closed v1 event
// union so consumers can switch exhaustively, but are EMITTED only once the §14
// ingress (C4) and action/approval bridge (C5) orchestration lands. Until then
// they are a reserved, type-level contract — not a runtime guarantee.
// ---------------------------------------------------------------------------
export interface ChannelIngressReceivedEvent extends HarnessEventBase {
  type: 'channel_ingress_received';
  harnessName: string;
  channelId: string;
  inboxItemId: string;
  externalMessageId: string;
  bindingId?: string;
}

export interface ChannelIngressAdmittedEvent extends HarnessEventBase {
  type: 'channel_ingress_admitted';
  harnessName: string;
  channelId: string;
  inboxItemId: string;
  bindingId: string;
  delivery: 'signal' | 'queue';
  runId?: string;
  signalId?: string;
  queuedItemId?: string;
}

export interface ChannelIngressFailedEvent extends HarnessEventBase {
  type: 'channel_ingress_failed';
  harnessName: string;
  channelId: string;
  inboxItemId?: string;
  externalMessageId?: string;
  error: { code: string; message: string };
}

export interface ChannelOutboxEnqueuedEvent extends HarnessEventBase {
  type: 'channel_outbox_enqueued';
  harnessName: string;
  channelId: string;
  outboxItemId: string;
  bindingId: string;
  kind: ChannelOutboxItem['kind'];
}

export interface ChannelOutboxSentEvent extends HarnessEventBase {
  type: 'channel_outbox_sent';
  harnessName: string;
  channelId: string;
  outboxItemId: string;
  bindingId: string;
  providerMessageId?: string;
}

export interface ChannelOutboxFailedEvent extends HarnessEventBase {
  type: 'channel_outbox_failed';
  harnessName: string;
  channelId: string;
  outboxItemId: string;
  bindingId: string;
  attempts: number;
  dead: boolean;
  // §13.3f.1: `code` is the namespaced harness.* wire code. `reason` carries the
  // optional discriminator that distinguishes the bare row code collapsed onto a
  // shared envelope (e.g. `platform_unlinked` vs `operator_closed` under
  // `harness.channel_binding_closed`). Optional and additive — absent when the
  // projection has no discriminator.
  error: { code: string; reason?: string; message: string };
}

export interface ChannelActionReceivedEvent extends HarnessEventBase {
  type: 'channel_action_received';
  harnessName: string;
  channelId: string;
  actionReceiptId: string;
  actionTokenId: string;
  actionId: string;
  itemId: string;
}

export interface ChannelActionAcceptedEvent extends HarnessEventBase {
  type: 'channel_action_accepted';
  harnessName: string;
  channelId: string;
  actionReceiptId: string;
  actionTokenId: string;
  actionId: string;
  itemId: string;
  responseId: string;
}

export interface ChannelActionAppliedEvent extends HarnessEventBase {
  type: 'channel_action_applied';
  harnessName: string;
  channelId: string;
  actionReceiptId: string;
  actionTokenId: string;
  actionId: string;
  itemId: string;
}

export interface ChannelActionConflictEvent extends HarnessEventBase {
  type: 'channel_action_conflict';
  harnessName: string;
  channelId: string;
  actionReceiptId: string;
  actionTokenId: string;
  actionId: string;
  itemId: string;
  conflictReason?: ChannelActionReceipt['conflictReason'];
}

export interface ChannelActionFailedEvent extends HarnessEventBase {
  type: 'channel_action_failed';
  harnessName: string;
  channelId: string;
  actionReceiptId?: string;
  actionTokenId?: string;
  actionId?: string;
  itemId?: string;
  error: { code: string; message: string };
}

// ---------------------------------------------------------------------------
// Storage failure events (§10.2). Session-scoped or harness-scoped depending on
// origin. Surfaces a storage failure that occurred on a BACKGROUND / best-effort
// path (lease renewal, event persistence, idle eviction flush) where the error
// is not propagated to a caller's awaited promise — foreground storage failures
// already reach the caller as a thrown `HarnessStorageError`. `operation` /
// `subject` use the same taxonomy as `HarnessStorageError`.
// ---------------------------------------------------------------------------

export interface StorageErrorEvent extends HarnessEventBase {
  type: 'storage_error';
  operation: HarnessStorageOperation;
  retryable: boolean;
  error: { code: 'harness.storage'; message: string };
  resourceId?: string;
  threadId?: string;
  harnessName?: string;
  channelId?: string;
  subject?: HarnessStorageSubject;
}

// ---------------------------------------------------------------------------
// Custom events (§10.3) — escape hatch for callers that want to attach their
// own typed events to the same subscription channel. Type must be dotted
// and not start with the reserved harness prefix; payload must be JSON-
// serializable.
// ---------------------------------------------------------------------------

export interface CustomEvent extends HarnessEventBase {
  type: `${string}.${string}`;
  payload?: unknown;
}

// §4.1/§10.2: there are NO thread_* lifecycle events. Thread CRUD is an
// internal/operator op (no public `harness.threads.*` surface — see
// `createHarnessOperatorThreadController`), and the closed HarnessEventV1 union
// has no thread family. Sidebar/product surfaces react to `Session` lifecycle
// events (session_created/closing/closed) + display snapshots instead.

// ---------------------------------------------------------------------------
// Subagent events (§10.2 / §10.6 — parent-session attribution).
//
// Emitted on the *parent* session's subscriber when a subagent session
// makes progress. `toolCallId` is the parent's `spawn_subagent` tool-call
// handle (stable for the subagent's lifetime). `subagentSessionId` is the
// child session id, addressable for response routing. `agentType` is the
// child's registered subagent type from `HarnessConfig.subagents.types`.
// `depth` is the child's depth in the subagent tree (`>= 1` for any
// subagent event; parent session itself is depth 0).
//
// `parentId` is the parent's session id, repeated on every subagent event
// to make routing trivial in flat consumers that see events from many
// sessions.
// ---------------------------------------------------------------------------

export interface SubagentStartEvent extends HarnessEventBase {
  type: 'subagent_start';
  toolCallId: string;
  subagentSessionId: string;
  agentType: string;
  task: string;
  modelId: string;
  parentId?: string;
  depth: number;
}

export interface SubagentTextDeltaEvent extends HarnessEventBase {
  type: 'subagent_text_delta';
  toolCallId: string;
  subagentSessionId: string;
  agentType: string;
  delta: string;
  parentId?: string;
  depth: number;
}

export interface SubagentToolStartEvent extends HarnessEventBase {
  type: 'subagent_tool_start';
  toolCallId: string;
  subagentSessionId: string;
  agentType: string;
  innerToolCallId: string;
  toolName: string;
  parentId?: string;
  depth: number;
}

export interface SubagentToolEndEvent extends HarnessEventBase {
  type: 'subagent_tool_end';
  toolCallId: string;
  subagentSessionId: string;
  agentType: string;
  innerToolCallId: string;
  toolName: string;
  output: unknown;
  isError: boolean;
  parentId?: string;
  depth: number;
}

export interface SubagentEndEvent extends HarnessEventBase {
  type: 'subagent_end';
  toolCallId: string;
  subagentSessionId: string;
  agentType: string;
  output: unknown;
  isError: boolean;
  durationMs: number;
  parentId?: string;
  depth: number;
}

// ---------------------------------------------------------------------------
// Goal events (§4.7 / §10.2).
//
// Goals are a standing objective attached to a session that survives across
// turns. While a goal is `active`, the harness invokes a separate judge
// model after every assistant turn and dispatches its verdict
// (`done` / `continue` / `waiting`). See §4.7 for the full lifecycle.
// ---------------------------------------------------------------------------

export interface GoalSetEvent extends HarnessEventBase {
  type: 'goal_set';
  goal: GoalState;
}

export interface GoalJudgedEvent extends HarnessEventBase {
  type: 'goal_judged';
  goalId: string;
  decision: GoalJudgeDecision;
  turnsUsed: number;
  maxTurns: number;
}

export interface GoalDoneEvent extends HarnessEventBase {
  type: 'goal_done';
  goalId: string;
  reason: string;
  turnsUsed: number;
}

// §10.2 / §4.7: the goal judge returned `waiting` — the goal requires an
// external checkpoint (user feedback, human verification, another out-of-loop
// event) before the assistant continues. The auto-continuation loop stops
// without advancing `turnsUsed`; `reason` carries the judge's guidance about
// the outstanding checkpoint.
export interface GoalWaitingEvent extends HarnessEventBase {
  type: 'goal_waiting';
  goalId: string;
  reason: string;
  turnsUsed: number;
}

export interface GoalPausedEvent extends HarnessEventBase {
  type: 'goal_paused';
  goalId: string;
  reason: 'requested' | 'budget_exhausted' | 'judge_failed';
}

export interface GoalResumedEvent extends HarnessEventBase {
  type: 'goal_resumed';
  goalId: string;
}

export interface GoalClearedEvent extends HarnessEventBase {
  type: 'goal_cleared';
  goalId: string;
}

// §2.7 / §10.2: Harness v1 does NOT define workspace lifecycle/error events.
// Provider filesystem audit, if present, is provider-owned inspection data and
// is not part of the closed HarnessEventV1 union. Workspace provisioning
// failures surface as thrown `HarnessWorkspaceProvisioningError` (and friends);
// status transitions are an optional internal notice (see
// `WorkspaceRegistry`), never a public `session.subscribe`/SSE event.

export type HarnessEvent =
  | SessionCreatedEvent
  | SessionClosingEvent
  | SessionClosedEvent
  | SessionEvictedEvent
  | SessionHydratedEvent
  | HarnessShutdownEvent
  | ModeChangedEvent
  | ModelChangedEvent
  | TokenUsageChangedEvent
  | StateChangedEvent
  | PermissionGrantedEvent
  | PermissionRevokedEvent
  | PermissionPolicyChangedEvent
  | AgentStartEvent
  | TextDeltaEvent
  | ToolStartEvent
  | ToolEndEvent
  | AgentEndEvent
  | TurnErrorEvent
  | SignalCompletedEvent
  | SignalFailedEvent
  | QueueCompletedEvent
  | QueueFailedEvent
  | ToolApprovalRequiredEvent
  | ToolSuspensionRequiredEvent
  | QuestionPendingEvent
  | PlanApprovalRequiredEvent
  | SubagentStartEvent
  | SubagentTextDeltaEvent
  | SubagentToolStartEvent
  | SubagentToolEndEvent
  | SubagentEndEvent
  | GoalSetEvent
  | GoalJudgedEvent
  | GoalDoneEvent
  | GoalWaitingEvent
  | GoalPausedEvent
  | GoalResumedEvent
  | GoalClearedEvent
  | AttachmentUploadedEvent
  | AttachmentDeletedEvent
  | ChannelIngressReceivedEvent
  | ChannelIngressAdmittedEvent
  | ChannelIngressFailedEvent
  | ChannelOutboxEnqueuedEvent
  | ChannelOutboxSentEvent
  | ChannelOutboxFailedEvent
  | ChannelActionReceivedEvent
  | ChannelActionAcceptedEvent
  | ChannelActionAppliedEvent
  | ChannelActionConflictEvent
  | ChannelActionFailedEvent
  | StorageErrorEvent
  | CustomEvent;

export type HarnessEventListener = (event: HarnessEvent) => void | Promise<void>;
export type HarnessEventUnsubscribe = () => void;

export const HARNESS_EVENT_ID_PREFIX = 'harness-v1';

export interface ParsedHarnessEventId {
  epoch: string;
  sequence: number;
}

export function formatHarnessEventId(epoch: string, sequence: number): string {
  if (epoch.length === 0 || epoch.includes(':')) {
    throw new HarnessValidationError('eventId.epoch', 'epoch must be non-empty and must not contain ":"');
  }
  if (!Number.isSafeInteger(sequence) || sequence < 0) {
    throw new HarnessValidationError('eventId.sequence', 'sequence must be a non-negative safe integer');
  }
  return `${HARNESS_EVENT_ID_PREFIX}:${epoch}:${sequence}`;
}

export function parseHarnessEventId(eventId: string): ParsedHarnessEventId {
  const parts = eventId.split(':');
  if (parts.length !== 3 || parts[0] !== HARNESS_EVENT_ID_PREFIX || parts[1] === '' || parts[2] === '') {
    throw new HarnessValidationError('lastEventId', 'expected event id grammar harness-v1:<epoch>:<seq>');
  }
  const sequenceText = parts[2]!;
  if (!/^(0|[1-9][0-9]*)$/.test(sequenceText)) {
    throw new HarnessValidationError('lastEventId', 'event id sequence must be an unsigned decimal integer');
  }
  const sequence = Number(sequenceText);
  if (!Number.isSafeInteger(sequence)) {
    throw new HarnessValidationError('lastEventId', 'event id sequence must be within JavaScript safe integer range');
  }
  return { epoch: parts[1]!, sequence };
}

export function snapshotHarnessEventForJson(value: unknown, path = 'event'): JsonValue {
  try {
    const encoded = JSON.stringify(value, harnessEventJsonReplacer);
    if (encoded === undefined) {
      throw new HarnessValidationError(path, 'must be JSON-serializable for event replay');
    }
    return JSON.parse(encoded) as JsonValue;
  } catch (err) {
    if (err instanceof HarnessValidationError) throw err;
    throw new HarnessValidationError(path, 'must be JSON-serializable for event replay');
  }
}

export function projectHarnessPublicError(err: unknown): { code: string; message: string } {
  if (err instanceof Error) {
    return { code: (err as { code?: string }).code ?? err.name ?? 'harness.message_failed', message: err.message };
  }
  return { code: 'harness.message_failed', message: String(err) };
}

function harnessEventJsonReplacer(_key: string, value: unknown): unknown {
  if (value instanceof Error) {
    return {
      name: value.name,
      ...projectHarnessPublicError(value),
    };
  }
  return value;
}

// ---------------------------------------------------------------------------
// Emitter.
// ---------------------------------------------------------------------------

/**
 * Shape of an event before `emit()` stamps the framework fields. Callers
 * provide the type-discriminated payload; the emitter fills in `id`,
 * `timestamp`, `sessionId` (when configured), and (optionally)
 * `subagentSessionId` / `runId` / `signalId` / `queuedItemId`.
 *
 * Distributes Omit over the union so the discriminator is preserved.
 */
type DistributiveOmit<T, K extends keyof never> = T extends unknown ? Omit<T, K> : never;

export type EmitInput = DistributiveOmit<HarnessEvent, 'id' | 'timestamp' | 'sessionId'>;

/**
 * Per-emitter scope applied to every event the emitter publishes. Used so
 * the Session emitter automatically stamps `sessionId`; harness-level
 * emitters leave it unset.
 */
export interface EmitterScope {
  sessionId?: string;
}

/**
 * Tiny pub/sub primitive used by `Session` and `Harness`. Listeners are
 * dispatched in registration order. A throwing or rejecting listener is
 * isolated (logged to console) so a buggy subscriber cannot disrupt the
 * producer or other listeners.
 *
 * Event IDs are formatted `harness-v1:<epoch>:<seq>`; the epoch is a
 * per-emitter UUID regenerated on every construction. Clients that have
 * buffered an `id` from a previous epoch and rejoin can detect mismatch and
 * reset.
 */
export class EventEmitter {
  private readonly listeners: HarnessEventListener[] = [];
  private readonly epoch: string;
  private seq: number;
  private readonly scope: EmitterScope;
  private readonly onEvent?: HarnessEventListener;

  constructor(
    scope: EmitterScope = {},
    opts: { onEvent?: HarnessEventListener; epoch?: string; nextSequence?: number } = {},
  ) {
    this.scope = scope;
    this.onEvent = opts.onEvent;
    this.epoch = opts.epoch ?? randomUUID();
    this.seq = opts.nextSequence ?? 0;
    formatHarnessEventId(this.epoch, this.seq);
  }

  subscribe(listener: HarnessEventListener): HarnessEventUnsubscribe {
    this.listeners.push(listener);
    return () => {
      const index = this.listeners.indexOf(listener);
      if (index !== -1) this.listeners.splice(index, 1);
    };
  }

  emit(event: EmitInput, overrides?: { sessionId?: string }): HarnessEvent {
    const sessionId = overrides?.sessionId ?? this.scope.sessionId;
    const stamped = {
      ...event,
      id: formatHarnessEventId(this.epoch, this.seq++),
      timestamp: Date.now(),
      ...(sessionId !== undefined && { sessionId }),
    } as HarnessEvent;
    this.dispatch(stamped);
    return stamped;
  }

  /**
   * Re-emit an already-stamped event (e.g. when a Harness bridges a Session
   * event into its own subscriber pool). The original `id` / `timestamp` /
   * `sessionId` are preserved; the bridging emitter does NOT re-stamp.
   */
  forward(event: HarnessEvent): void {
    this.dispatch(event);
  }

  /** Number of currently registered listeners — for tests. */
  get listenerCount(): number {
    return this.listeners.length;
  }

  /** Current epoch id — for tests and for diagnostics on rehydrate. */
  get epochId(): string {
    return this.epoch;
  }

  private dispatch(event: HarnessEvent): void {
    if (this.onEvent) {
      try {
        const result = this.onEvent(event);
        if (result && typeof (result as Promise<void>).catch === 'function') {
          (result as Promise<void>).catch(err => console.error('[harness/v1] event persistence rejected:', err));
        }
      } catch (err) {
        console.error('[harness/v1] event persistence threw:', err);
      }
    }
    for (const listener of this.listeners) {
      try {
        const result = listener(event);
        if (result && typeof (result as Promise<void>).catch === 'function') {
          (result as Promise<void>).catch(err => console.error('[harness/v1] event listener rejected:', err));
        }
      } catch (err) {
        console.error('[harness/v1] event listener threw:', err);
      }
    }
  }
}

// ---------------------------------------------------------------------------
// Helpers consumed by Session/Harness.
// ---------------------------------------------------------------------------

/**
 * Map a `SessionRecord` to the payload of `session_created`. Centralized so
 * the Session and Harness emit identical fields.
 */
export function sessionCreatedPayload(
  record: SessionRecord,
): Omit<SessionCreatedEvent, keyof HarnessEventBase | 'type'> {
  return {
    resourceId: record.resourceId,
    threadId: record.threadId,
    parentSessionId: record.parentSessionId,
    modeId: record.modeId,
    modelId: record.modelId,
  };
}

// ---------------------------------------------------------------------------
// Reserved-event metadata (§6.2, §10.3).
//
// Tools emit data via `ctx.writer?.custom({ type: 'data-*', data })` and
// the harness whitelists known `data-*` chunk types in `_drainStreamToEvents`
// to bridge them into typed events. These reserved sets capture the names
// the harness owns so future custom-event surfaces can validate against
// them as a single source of truth.
// ---------------------------------------------------------------------------

/** Harness-owned event types — exhaustive list per spec §6.2 / §10.2. */
const RESERVED_EVENT_TYPES: ReadonlySet<string> = new Set([
  'session_created',
  'session_closing',
  'session_closed',
  'session_evicted',
  'session_pin_overflow',
  'mode_changed',
  'model_changed',
  'model_override_set',
  'state_changed',
  'agent_start',
  'agent_end',
  'message_start',
  'message_update',
  'message_end',
  'tool_input_start',
  'tool_input_delta',
  'tool_input_end',
  'tool_start',
  'tool_update',
  'shell_output',
  'task_updated',
  'tool_end',
  'suspension_required',
  'suspension_resolved',
  'sandbox_access_requested',
  'sandbox_access_resolved',
  'task_cancellation_requested',
  'queue_item_started',
  'queue_item_replayed',
  'queue_item_cancelled',
  'queue_item_expired',
  'queue_full_dropped',
  'queue_item_failed',
  'queue_item_completed',
  'thread_created',
  'thread_renamed',
  'thread_deleted',
  'thread_cloned',
  'thread_settings_changed',
  'goal_set',
  'goal_judged',
  'goal_done',
  'goal_waiting',
  'goal_paused',
  'goal_resumed',
  'goal_cleared',
  'workspace_status_changed',
  'workspace_error',
  'permission_granted',
  'permission_revoked',
  'permission_policy_changed',
  // §6.3: the exact type `error` is reserved (not a prefix family).
  'error',
]);

/** Prefixes reserved for built-in event families (subagent_*, goal_*, etc.). */
const RESERVED_EVENT_PREFIXES: readonly string[] = [
  // §6.3 reserved internal-prefix families — a custom event type must not START
  // with any of these (even dotted, e.g. `tool_start.progress` is rejected).
  'agent_',
  'text_',
  'message_',
  'queue_',
  'tool_',
  'subagent_',
  'state_',
  'mode_',
  'model_',
  'session_',
  'token_',
  'channel_',
  'goal_',
  'attachment_',
  'display_',
  'storage_',
  // Additional built-in families this impl owns (stricter than §6.3 — safe).
  'workspace_',
  'thread_',
  'permission_',
];

/**
 * Throws `HarnessValidationError` if `type` collides with a harness-owned
 * event type or omits the required dotted prefix. Custom events must follow
 * `<namespace>.<rest>` per spec §10.3.
 */
export function assertCustomEventType(type: string): void {
  if (RESERVED_EVENT_TYPES.has(type)) {
    throw new HarnessValidationError('event.type', `"${type}" is a reserved harness event type`);
  }
  for (const prefix of RESERVED_EVENT_PREFIXES) {
    if (type.startsWith(prefix)) {
      throw new HarnessValidationError(
        'event.type',
        `"${type}" uses reserved prefix "${prefix}*" — custom events need a different namespace`,
      );
    }
  }
  if (!type.includes('.')) {
    throw new HarnessValidationError(
      'event.type',
      `custom event "${type}" must be dotted (e.g. "myorg.tool.progress")`,
    );
  }
}

/**
 * Walks an event payload and throws `HarnessEventSerializationError` on the
 * first non-JSON-serializable value. Catches functions, Symbols, BigInts,
 * Dates, Map/Set, typed arrays, class instances with a non-plain prototype,
 * `undefined`, and cyclic refs.
 *
 * `sessionId` is threaded through purely for the error payload.
 */
export function assertJsonSerializable(eventType: string, sessionId: string | undefined, value: unknown): void {
  const seen = new WeakSet<object>();
  walk(value, 'event');

  function fail(path: string, reason: EventSerializationReason): never {
    throw new HarnessEventSerializationError(sessionId, eventType, path, reason);
  }

  function walk(node: unknown, path: string): void {
    if (node === null) return;
    const t = typeof node;
    if (t === 'string' || t === 'number' || t === 'boolean') return;
    if (t === 'undefined') return fail(path, 'undefined');
    if (t === 'function') return fail(path, 'function');
    if (t === 'symbol') return fail(path, 'symbol');
    if (t === 'bigint') return fail(path, 'bigint');

    if (Array.isArray(node)) {
      if (seen.has(node)) return fail(path, 'cyclic');
      seen.add(node);
      for (let i = 0; i < node.length; i++) walk(node[i], `${path}[${i}]`);
      return;
    }

    if (node instanceof Date) return fail(path, 'date');
    if (node instanceof Map) return fail(path, 'map');
    if (node instanceof Set) return fail(path, 'set');
    if (ArrayBuffer.isView(node) || node instanceof ArrayBuffer) return fail(path, 'typed-array');

    if (t === 'object') {
      const proto = Object.getPrototypeOf(node);
      if (proto !== null && proto !== Object.prototype) {
        return fail(path, 'class-instance');
      }
      if (seen.has(node as object)) return fail(path, 'cyclic');
      seen.add(node as object);
      for (const key of Object.keys(node as object)) {
        walk((node as Record<string, unknown>)[key], `${path}.${key}`);
      }
      return;
    }

    fail(path, 'unknown');
  }
}

// Re-export so consumers that import HarnessEvent get the lifecycle import for free.
export type { SessionLifecycleState };
