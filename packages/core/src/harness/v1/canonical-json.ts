import { createHash } from 'node:crypto';

import type { JsonValue } from '../../storage/domains/harness';

import { HarnessValidationError } from './errors';

/**
 * §5.1 — centralized stable-hash canonicalization for the harness v1 runtime.
 *
 * Admission hashing (message/queue/signal/respond), channel-binding id derivation, and
 * attachment-semantic comparison all need ONE canonical-JSON algorithm so that equal
 * payloads produce equal hashes regardless of key insertion order or representation noise.
 * Historically this logic was duplicated verbatim in three places (session.ts, harness.ts,
 * and the attachment family in harness.ts); centralizing here removes the drift surface.
 *
 * The canonical form is deterministic:
 * - object keys are sorted ascending (lexicographic over UTF-16 code units, via `Array.sort`);
 * - scalars are emitted via `JSON.stringify` (so `null` -> `"null"`, strings are escaped);
 * - arrays preserve element order;
 * - object properties whose value is `undefined` are dropped during validation (`assertJsonValue`),
 *   matching `JSON.stringify` semantics, while explicit `null` is preserved (so `{a:null}` and `{}`
 *   hash differently).
 *
 * Two hashing entry points exist by design:
 * - {@link sha256CanonicalJson} takes an already-typed {@link JsonValue} and does NOT re-validate.
 *   Use it for values the runtime constructs itself (channel-binding tuples, attachment payloads).
 * - {@link sha256CanonicalJsonChecked} validates untrusted input via {@link assertJsonValue} first,
 *   rejecting non-finite numbers, `-0`, sparse arrays, and non-plain objects. Use it on admission
 *   inputs that may originate from callers.
 */

/** True when `value`'s prototype is `Object.prototype` or `null` (a plain data object). */
export function isPlainJsonObject(value: object): value is Record<string, unknown> {
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

/**
 * Validate that `value` is a JSON value suitable for stable hashing, returning a normalized
 * {@link JsonValue}. Throws {@link HarnessValidationError} (with a dotted `path`) for non-finite
 * numbers, `-0`, sparse arrays, and any non-plain / non-JSON value. Object properties whose value
 * is `undefined` are dropped (matching `JSON.stringify`).
 */
export function assertJsonValue(value: unknown, path = 'value'): JsonValue {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return value;
  if (typeof value === 'number') {
    if (!Number.isFinite(value) || Object.is(value, -0)) {
      throw new HarnessValidationError(path, 'must be a finite JSON number');
    }
    return value;
  }
  if (Array.isArray(value)) {
    const out: JsonValue[] = [];
    for (let index = 0; index < value.length; index += 1) {
      if (!(index in value)) throw new HarnessValidationError(`${path}[${index}]`, 'sparse arrays are not allowed');
      out.push(assertJsonValue(value[index], `${path}[${index}]`));
    }
    return out;
  }
  if (typeof value === 'object' && value !== null && isPlainJsonObject(value)) {
    const out: Record<string, JsonValue> = {};
    for (const [key, entry] of Object.entries(value)) {
      if (entry !== undefined) out[key] = assertJsonValue(entry, `${path}.${key}`);
    }
    return out;
  }
  throw new HarnessValidationError(path, 'must be JSON-serializable for admission hashing');
}

/** Deterministic canonical-JSON string: sorted object keys, ordered arrays, `JSON.stringify` scalars. */
export function canonicalJson(value: JsonValue): string {
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  return `{${Object.keys(value)
    .sort()
    .map(key => `${JSON.stringify(key)}:${canonicalJson(value[key]!)}`)
    .join(',')}}`;
}

/** Structural equality of two optional {@link JsonValue}s via canonical form (`undefined` only equals `undefined`). */
export function jsonValuesEqual(left: JsonValue | undefined, right: JsonValue | undefined): boolean {
  if (left === undefined || right === undefined) return left === right;
  return canonicalJson(left) === canonicalJson(right);
}

/**
 * SHA-256 (hex) of the canonical form of an already-typed {@link JsonValue}. Does NOT validate;
 * callers must pass a real JsonValue. For untrusted input use {@link sha256CanonicalJsonChecked}.
 */
export function sha256CanonicalJson(value: JsonValue): string {
  return createHash('sha256').update(canonicalJson(value), 'utf8').digest('hex');
}

/**
 * Validate untrusted input as a {@link JsonValue} (see {@link assertJsonValue}), then return the
 * SHA-256 (hex) of its canonical form. Use on admission inputs that may carry caller-supplied data.
 */
export function sha256CanonicalJsonChecked(value: unknown): string {
  return sha256CanonicalJson(assertJsonValue(value));
}
