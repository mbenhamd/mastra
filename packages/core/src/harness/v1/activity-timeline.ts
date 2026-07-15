/**
 * §5.1b.4 / §5.6 / §10.6 — pure activity-timeline assembler.
 *
 * The owning `Session` gathers the durable sources (thread messages, goal state,
 * session-owned pending inbox, and — under `includeDescendants` — descendant
 * subagent inputs) and hands them to {@link buildActivityTimeline}, which is a
 * pure, storage-free projection: it builds redacted {@link ActivityTimelineEntry}
 * rows, sorts them by `(occurredAt ASC, sessionId ASC, entryId ASC)`, and applies
 * the forward-seek cursor. It NEVER reads storage, settles promises, proves
 * delivery, claims rows, or mutates state.
 *
 * Scope of THIS build: the message log (message + tool_call + tool_result parts),
 * goal, pending inbox, and subagent entries. The lower-durability / heavier
 * source kinds in the §5.1b.4 union — `operation-result`, `durable-work`,
 * `channel`, and `file-reference` — are intentionally NOT emitted here yet and
 * are tracked as a follow-up (they overlap the not-yet-built `DurableWorkSummary`
 * projection); the entry/source kind enums already reserve them.
 */

import type { AgentControllerMessage as HarnessMessage } from '../../agent-controller/types';

import { HarnessValidationError } from './errors';
import type { ActivityTimelineEntry, ActivityTimelineOptions, SessionActivityTimeline } from './types';

/** Default + hard cap on entries returned in one page (server-bounded). */
export const ACTIVITY_TIMELINE_DEFAULT_LIMIT = 100;
export const ACTIVITY_TIMELINE_MAX_LIMIT = 500;

/** Max chars of free text surfaced in a redacted `summary`. */
const SUMMARY_MAX_CHARS = 280;

/** A single goal's durable state, normalized for projection. */
export interface ActivityTimelineGoalInput {
  id: string;
  objective: string;
  status: string;
  createdAt: number;
  lastDecision?: { decision: string; reason: string; judgedAt: number };
}

/** A session-owned pending inbox item, normalized for projection. */
export interface ActivityTimelinePendingInput {
  itemId: string;
  kind: string;
  requestedAt: number;
  toolName?: string;
  runId?: string;
}

/** One session's durable sources (the addressed session or a descendant). */
export interface ActivityTimelineSessionInput {
  sessionId: string;
  threadId: string;
  parentSessionId?: string;
  /** 0 for the addressed session; +1 per descendant level. */
  depth: number;
  messages: HarnessMessage[];
  goal?: ActivityTimelineGoalInput;
  pendingInbox?: ActivityTimelinePendingInput[];
  /** Present on a descendant: the `subagent` entry attributed to the parent. */
  subagent?: { parentSessionId: string; childSessionId: string; createdAt: number; label?: string };
}

export interface BuildActivityTimelineInput {
  addressedSessionId: string;
  addressedThreadId: string;
  generatedAt: number;
  /** Addressed session first (depth 0), then descendants when includeDescendants. */
  sessions: ActivityTimelineSessionInput[];
}

interface CursorKey {
  o: number; // occurredAt
  s: string; // sessionId
  e: string; // entryId
}

interface DecodedCursor extends CursorKey {
  v: 1;
  sid: string; // addressed sessionId scope
  d: boolean; // includeDescendants scope
}

// ---------------------------------------------------------------------------
// Redaction helpers
// ---------------------------------------------------------------------------

function redactText(value: string): string {
  const collapsed = value.replace(/\s+/g, ' ').trim();
  return collapsed.length > SUMMARY_MAX_CHARS ? `${collapsed.slice(0, SUMMARY_MAX_CHARS)}…` : collapsed;
}

function firstText(message: HarnessMessage): string | undefined {
  for (const part of message.content) {
    if (part.type === 'text' && typeof part.text === 'string' && part.text.length > 0) return part.text;
  }
  return undefined;
}

// ---------------------------------------------------------------------------
// Cursor codec — opaque base64url JSON. Validation rejects BEFORE any scan.
// ---------------------------------------------------------------------------

function encodeCursor(key: CursorKey, addressedSessionId: string, includeDescendants: boolean): string {
  const payload: DecodedCursor = { v: 1, sid: addressedSessionId, d: includeDescendants, ...key };
  return Buffer.from(JSON.stringify(payload), 'utf8').toString('base64url');
}

function decodeCursor(cursor: string, addressedSessionId: string, includeDescendants: boolean): CursorKey {
  let parsed: unknown;
  try {
    parsed = JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8'));
  } catch {
    throw new HarnessValidationError('cursor', 'malformed activity timeline cursor');
  }
  if (
    !parsed ||
    typeof parsed !== 'object' ||
    (parsed as DecodedCursor).v !== 1 ||
    typeof (parsed as DecodedCursor).o !== 'number' ||
    typeof (parsed as DecodedCursor).s !== 'string' ||
    typeof (parsed as DecodedCursor).e !== 'string' ||
    typeof (parsed as DecodedCursor).sid !== 'string' ||
    typeof (parsed as DecodedCursor).d !== 'boolean'
  ) {
    throw new HarnessValidationError('cursor', 'malformed activity timeline cursor');
  }
  const decoded = parsed as DecodedCursor;
  // Wrong-scope: a cursor is only valid for the session + includeDescendants flag
  // it was issued under (§9 read-time-model rules).
  if (decoded.sid !== addressedSessionId) {
    throw new HarnessValidationError('cursor', 'activity timeline cursor was issued for a different session');
  }
  if (decoded.d !== includeDescendants) {
    throw new HarnessValidationError(
      'cursor',
      'activity timeline cursor was issued for a different includeDescendants scope',
    );
  }
  return { o: decoded.o, s: decoded.s, e: decoded.e };
}

/**
 * §6.1 / P6.3 — compute the bounded per-thread message-read window for
 * `getActivityTimeline` BEFORE any source scan. Returns the same `limit`
 * {@link buildActivityTimeline} will apply, plus the inclusive lower-bound
 * `occurredAt` implied by an optional forward cursor. A caller reads at most
 * `limit + 1` oldest-at/after-cursor messages PER THREAD instead of the whole
 * log: the timeline paginates forward (oldest→newest, `slice(0, limit)` after an
 * ascending sort), so the oldest `limit` global entries can span at most
 * `limit + 1` messages from any one thread (entries within a message share the
 * message's `occurredAt`), and the `+1` keeps truncation detection exact when a
 * single thread alone overflows the page. The precise `> cursor` tiebreak and the
 * final slice stay in {@link buildActivityTimeline}; the lower bound is
 * intentionally INCLUSIVE on `occurredAt` so a same-timestamp boundary is not
 * over-excluded by the coarse date filter. Decoding the cursor here also makes a
 * malformed/wrong-scope cursor reject BEFORE the read rather than after.
 */
export function activityTimelineMessageReadBound(
  opts: ActivityTimelineOptions,
  addressedSessionId: string,
  includeDescendants: boolean,
): { limit: number; sinceOccurredAt?: number } {
  if (opts.limit !== undefined && (typeof opts.limit !== 'number' || !Number.isInteger(opts.limit) || opts.limit < 1)) {
    throw new HarnessValidationError('limit', 'must be a positive integer when provided');
  }
  const limit = Math.min(opts.limit ?? ACTIVITY_TIMELINE_DEFAULT_LIMIT, ACTIVITY_TIMELINE_MAX_LIMIT);
  const sinceOccurredAt =
    opts.cursor !== undefined ? decodeCursor(opts.cursor, addressedSessionId, includeDescendants).o : undefined;
  return sinceOccurredAt !== undefined ? { limit, sinceOccurredAt } : { limit };
}

/** `(occurredAt, sessionId, entryId)` total order. */
function compareKeys(a: CursorKey, b: CursorKey): number {
  if (a.o !== b.o) return a.o - b.o;
  if (a.s !== b.s) return a.s < b.s ? -1 : 1;
  if (a.e !== b.e) return a.e < b.e ? -1 : 1;
  return 0;
}

// ---------------------------------------------------------------------------
// Entry builders
// ---------------------------------------------------------------------------

function messageEntries(src: ActivityTimelineSessionInput): ActivityTimelineEntry[] {
  const out: ActivityTimelineEntry[] = [];
  for (const message of src.messages) {
    const occurredAt = message.createdAt instanceof Date ? message.createdAt.getTime() : Number(message.createdAt);
    if (!Number.isFinite(occurredAt)) continue;
    const base = { sessionId: src.sessionId, threadId: src.threadId, occurredAt, depth: src.depth } as const;

    // Message-level entry only when the turn carries displayable text/thinking;
    // a pure tool-call/result message is represented by its part entries below.
    const text = firstText(message);
    const hasThinking = message.content.some(p => p.type === 'thinking');
    if (text !== undefined || hasThinking) {
      out.push({
        ...base,
        entryId: `message:${src.sessionId}:${message.id}`,
        kind: 'message',
        actor: { kind: message.role },
        sourceDurability: 'durable',
        sourceRefs: [{ kind: 'thread-message', id: message.id, route: 'thread-messages' }],
        title: `${message.role} message`,
        ...(text !== undefined ? { summary: redactText(text) } : {}),
      });
    }

    message.content.forEach((part, i) => {
      if (part.type === 'tool_call') {
        out.push({
          ...base,
          entryId: `message-tool-call:${src.sessionId}:${message.id}:${i}`,
          kind: 'message-tool-call',
          toolCallId: part.id,
          actor: { kind: 'assistant' },
          sourceDurability: 'durable',
          sourceRefs: [{ kind: 'message-part', id: `${message.id}:${i}` }],
          title: `Tool call: ${part.name}`,
        });
      } else if (part.type === 'tool_result') {
        out.push({
          ...base,
          entryId: `message-tool-result:${src.sessionId}:${message.id}:${i}`,
          kind: 'message-tool-result',
          toolCallId: part.id,
          actor: { kind: 'tool', label: part.name },
          sourceDurability: 'durable',
          sourceRefs: [{ kind: 'message-part', id: `${message.id}:${i}` }],
          title: `Tool result: ${part.name}`,
          summary: part.isError ? 'error' : 'ok',
        });
      }
    });
  }
  return out;
}

function goalEntry(src: ActivityTimelineSessionInput): ActivityTimelineEntry | undefined {
  const goal = src.goal;
  if (!goal) return undefined;
  const occurredAt = goal.lastDecision?.judgedAt ?? goal.createdAt;
  if (!Number.isFinite(occurredAt)) return undefined;
  return {
    sessionId: src.sessionId,
    threadId: src.threadId,
    occurredAt,
    depth: src.depth,
    entryId: `goal:${src.sessionId}:${goal.id}`,
    kind: 'goal',
    ...(goal.lastDecision ? { updatedAt: goal.lastDecision.judgedAt } : {}),
    actor: { kind: 'goal' },
    sourceDurability: 'durable',
    sourceRefs: [{ kind: 'session-snapshot', id: goal.id }],
    title: `Goal: ${goal.status}`,
    summary: redactText(goal.objective),
    ...(goal.lastDecision
      ? { payload: { decision: goal.lastDecision.decision, reason: redactText(goal.lastDecision.reason) } }
      : {}),
  };
}

function pendingEntries(src: ActivityTimelineSessionInput): ActivityTimelineEntry[] {
  return (src.pendingInbox ?? [])
    .filter(item => Number.isFinite(item.requestedAt))
    .map(item => ({
      sessionId: src.sessionId,
      threadId: src.threadId,
      occurredAt: item.requestedAt,
      depth: src.depth,
      entryId: `pending-inbox:${src.sessionId}:${item.itemId}`,
      kind: 'pending-inbox' as const,
      ...(item.runId !== undefined ? { runId: item.runId } : {}),
      actor: { kind: 'harness' as const },
      sourceDurability: 'durable' as const,
      sourceRefs: [{ kind: 'pending-inbox' as const, id: item.itemId, route: 'subagent-inbox' as const }],
      title: `Pending: ${item.kind}`,
      ...(item.toolName !== undefined ? { summary: item.toolName } : {}),
    }));
}

function subagentEntry(src: ActivityTimelineSessionInput): ActivityTimelineEntry | undefined {
  const sub = src.subagent;
  if (!sub || !Number.isFinite(sub.createdAt)) return undefined;
  return {
    // The subagent entry is attributed to the PARENT session/thread but names the child.
    sessionId: sub.parentSessionId,
    threadId: src.threadId,
    occurredAt: sub.createdAt,
    depth: Math.max(0, src.depth - 1),
    entryId: `subagent:${sub.parentSessionId}:${sub.childSessionId}`,
    kind: 'subagent',
    subagentSessionId: sub.childSessionId,
    parentSessionId: sub.parentSessionId,
    actor: { kind: 'subagent', ...(sub.label !== undefined ? { label: sub.label } : {}) },
    sourceDurability: 'durable',
    sourceRefs: [{ kind: 'subagent-session', id: sub.childSessionId, route: 'subagent-inbox' }],
    title: 'Subagent session',
  };
}

// ---------------------------------------------------------------------------
// Assemble
// ---------------------------------------------------------------------------

export function buildActivityTimeline(
  input: BuildActivityTimelineInput,
  opts: ActivityTimelineOptions = {},
): SessionActivityTimeline {
  const includeDescendants = opts.includeDescendants === true;

  // Validate limit + decode/validate cursor BEFORE assembling any entries, so a
  // malformed/wrong-scope cursor rejects without a source scan.
  if (opts.limit !== undefined && (typeof opts.limit !== 'number' || !Number.isInteger(opts.limit) || opts.limit < 1)) {
    throw new HarnessValidationError('limit', 'must be a positive integer when provided');
  }
  const limit = Math.min(opts.limit ?? ACTIVITY_TIMELINE_DEFAULT_LIMIT, ACTIVITY_TIMELINE_MAX_LIMIT);
  const after =
    opts.cursor !== undefined ? decodeCursor(opts.cursor, input.addressedSessionId, includeDescendants) : undefined;

  // Collect entries. Descendant sessions only contribute when includeDescendants.
  const all: ActivityTimelineEntry[] = [];
  const seen = new Set<string>(); // entryId+sessionId scope dedupe within one response
  const push = (entry: ActivityTimelineEntry | undefined): void => {
    if (!entry) return;
    const dedupeKey = `${entry.sessionId} ${entry.entryId}`;
    if (seen.has(dedupeKey)) return;
    seen.add(dedupeKey);
    all.push(entry);
  };
  for (const src of input.sessions) {
    if (src.depth > 0 && !includeDescendants) continue;
    for (const e of messageEntries(src)) push(e);
    push(goalEntry(src));
    for (const e of pendingEntries(src)) push(e);
    push(subagentEntry(src));
  }

  all.sort((a, b) =>
    compareKeys({ o: a.occurredAt, s: a.sessionId, e: a.entryId }, { o: b.occurredAt, s: b.sessionId, e: b.entryId }),
  );

  const visible = after
    ? all.filter(e => compareKeys({ o: e.occurredAt, s: e.sessionId, e: e.entryId }, after) > 0)
    : all;

  const page = visible.slice(0, limit);
  const truncated = visible.length > page.length;
  const last = page[page.length - 1];

  return {
    sessionId: input.addressedSessionId,
    threadId: input.addressedThreadId,
    generatedAt: input.generatedAt,
    includeDescendants,
    entries: page,
    truncated,
    ...(truncated && last
      ? {
          nextCursor: encodeCursor(
            { o: last.occurredAt, s: last.sessionId, e: last.entryId },
            input.addressedSessionId,
            includeDescendants,
          ),
        }
      : {}),
  };
}
