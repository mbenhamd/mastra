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

/**
 * §5.1 state-shape validation. Returns the dotted path (`$` for the root) of the
 * FIRST value in a candidate `session.state` that cannot round-trip through
 * `JSON.stringify`/`JSON.parse` as plain JSON, or `undefined` when the whole
 * value is valid. Valid = `null`, string, boolean, FINITE number, plain array,
 * or PLAIN object whose values are all valid.
 *
 * The check is descriptor-STRICT — it rejects everything `JSON.stringify` would
 * silently DROP or TRANSFORM, not just obviously-bad scalar values:
 *   - scalar: `undefined`, `function`, `symbol`, `bigint`, `NaN`/`Infinity`
 *   - circular references
 *   - non-plain objects (`Date`/`Map`/`Set`/`RegExp`/class instances) — these
 *     either throw, serialize as `{}`, or transform via `toJSON`
 *   - symbol-keyed properties (dropped by JSON)
 *   - non-enumerable own properties (dropped by JSON)
 *   - accessor properties (getters/setters) — not data, and a getter would run
 *     arbitrary/throwing code; we reject WITHOUT invoking it
 *   - arrays with non-index own properties or sparse holes (dropped / become null)
 * Traversal is deterministic (`Reflect.ownKeys` order) so the reported path is
 * stable, and a data value is only READ after its descriptor is confirmed safe.
 */
export function firstNonJsonStatePath(value: unknown, path: string, seen: Set<object>): string | undefined {
  if (value === null) return undefined;
  const t = typeof value;
  if (t === 'string' || t === 'boolean') return undefined;
  if (t === 'number') return Number.isFinite(value as number) ? undefined : path;
  if (t === 'undefined' || t === 'function' || t === 'symbol' || t === 'bigint') return path;
  if (t !== 'object') return path;
  const obj = value as object;
  if (seen.has(obj)) return path; // circular reference

  if (Array.isArray(obj)) {
    seen.add(obj);
    const len = obj.length;
    // Reject any own key that isn't a dense [0, len) index (extra props +
    // accessors are dropped/transformed by JSON; `length` is intrinsic).
    for (const key of Reflect.ownKeys(obj)) {
      if (key === 'length') continue;
      if (typeof key === 'symbol') return done(seen, obj, path);
      // Only CANONICAL dense indices are real array elements; `"01"`/`"1.0"`/
      // `""`/`" "` etc. coerce into range but are non-index props JSON drops.
      const idx = Number(key);
      if (!Number.isInteger(idx) || idx < 0 || idx >= len || String(idx) !== key) return done(seen, obj, path);
      const desc = Object.getOwnPropertyDescriptor(obj, key);
      if (desc === undefined || desc.get !== undefined || desc.set !== undefined) {
        return done(seen, obj, path === '$' ? String(idx) : `${path}.${idx}`);
      }
    }
    for (let i = 0; i < len; i++) {
      const childPath = path === '$' ? String(i) : `${path}.${i}`;
      if (!(i in obj)) return done(seen, obj, childPath); // sparse hole (JSON → null)
      const r = firstNonJsonStatePath((obj as unknown[])[i], childPath, seen);
      if (r !== undefined) return done(seen, obj, r);
    }
    seen.delete(obj);
    return undefined;
  }

  // Only plain objects (Object.prototype or null prototype) are valid JSON
  // containers; Date/Map/Set/class instances etc. are not round-trippable.
  const proto = Object.getPrototypeOf(obj);
  if (proto !== Object.prototype && proto !== null) return path;
  seen.add(obj);
  for (const key of Reflect.ownKeys(obj)) {
    if (typeof key === 'symbol') return done(seen, obj, path); // symbol-keyed prop (dropped)
    const childPath = path === '$' ? key : `${path}.${key}`;
    const desc = Object.getOwnPropertyDescriptor(obj, key);
    if (desc === undefined) return done(seen, obj, childPath);
    if (!desc.enumerable) return done(seen, obj, childPath); // dropped by JSON
    if (desc.get !== undefined || desc.set !== undefined) return done(seen, obj, childPath); // accessor — never invoked
    const r = firstNonJsonStatePath((obj as Record<string, unknown>)[key], childPath, seen);
    if (r !== undefined) return done(seen, obj, r);
  }
  seen.delete(obj);
  return undefined;
}

/** Pop the circular-tracking frame and return the offending path. */
function done(seen: Set<object>, obj: object, path: string): string {
  seen.delete(obj);
  return path;
}
