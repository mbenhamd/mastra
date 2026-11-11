import type { QueueAdmissionReceipt, SessionRecord } from '../../storage/domains/harness';

export function queueReceiptRecency(receipt: QueueAdmissionReceipt): number {
  return (
    receipt.completedAt ?? receipt.failedAt ?? receipt.acceptedAt ?? receipt.admittingAt ?? receipt.enqueuedAt ?? 0
  );
}

// Just under Convex's ~1MiB document limit, leaving headroom for envelope
// fields the adapter adds around the record.
export const SESSION_RECORD_SIZE_BUDGET_BYTES = 900_000;

export function safeSerializedSize(value: unknown): number {
  try {
    const serialized = JSON.stringify(value);
    return serialized === undefined ? 0 : Buffer.byteLength(serialized, 'utf8');
  } catch {
    return Number.MAX_SAFE_INTEGER;
  }
}

export function topRecordFieldSizes(record: SessionRecord): Array<[string, number]> {
  return Object.entries(record as unknown as Record<string, unknown>)
    .map(([key, value]) => [key, safeSerializedSize(value)] as [string, number])
    .sort((left, right) => right[1] - left[1])
    .slice(0, 5);
}

/**
 * §S4.1 — max transiently-failed event-ledger appends retained for retry before
 * fail-stop. A transient `appendSessionEvent` failure no longer permanently latches
 * the durable log: the failed slot is backlogged and retried (in order) ahead of any
 * later sequence on the next append, preserving ledger contiguity. Past this bound we
 * can no longer guarantee retry without unbounded memory, so we hard-latch and let a
 * reconnect recover via the §10.5 snapshot path.
 */
export const MAX_PENDING_FAILED_EVENT_APPENDS = 256;

export function isPlainStateObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/**
 * §10.2 state_changed sentinel for a whole-root change. `session.state` is
 * `unknown` — it may legitimately be a scalar, array, or plain object root (see
 * `firstNonJsonStatePath`, which explicitly allows scalar/array roots). When
 * the root itself is not a plain object on both sides, there are no top-level
 * keys to diff, so a genuine root change is reported under this sentinel key,
 * matching the `$`-for-root path convention used elsewhere in this module.
 */
export const STATE_ROOT_SENTINEL = '$';

/**
 * Drafts share the parent SessionRecord with queues, receipts, policy and run
 * state. Bound both each streamed field and the whole draft projection in
 * UTF-8 bytes so multibyte output cannot silently exceed a storage adapter's
 * document limit (Convex, for example, caps a document at 1 MiB).
 */
export const MAX_ASSISTANT_DRAFT_FIELD_BYTES = 128_000;

export const MAX_PERSISTED_ASSISTANT_DRAFT_BYTES = 384 * 1024;

export const MAX_PERSISTED_ASSISTANT_DRAFTS = 8;

export const ASSISTANT_DRAFT_FLUSH_DELAY_MS = 250;

export const ASSISTANT_DRAFT_TEXT_ENCODER = new TextEncoder();

export const ASSISTANT_DRAFT_TEXT_DECODER = new TextDecoder();

/**
 * [PF-7749] `agentic-loop` row statuses that mean the run is not parked on a
 * suspension: it was still executing when its owner died (an executing run's
 * row reads `pending` or `running`), or it finished.
 */
export const UNSUSPENDED_AGENTIC_LOOP_STATUSES: ReadonlySet<string> = new Set([
  'pending',
  'running',
  'success',
  'failed',
  'tripwire',
  'canceled',
  'bailed',
  'skipped',
]);
