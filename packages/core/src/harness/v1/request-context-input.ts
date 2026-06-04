import type { JsonValue, PersistedRequestContextInput } from '../../storage/domains/harness';

import { assertJsonValue, isPlainJsonObject } from './canonical-json';
import { HarnessValidationError } from './errors';

/**
 * §4.4c — caller-supplied request-context input.
 *
 * Callers may provide ONLY `app`: a canonical-JSON application metadata bag merged into the
 * tool-visible `HarnessRequestContext`. Every other top-level request-context slot is
 * infrastructure-owned (harness identity, channel bridge, Mastra/server runtime). Caller input
 * carrying any other top-level key — or a non-JSON `app` value — is rejected with
 * {@link HarnessValidationError} BEFORE session admission/hashing.
 *
 * The trusted `channel` slot ({@link TrustedRequestContextInput}) is attached only by
 * harness-owned integration paths (channel bridge, scheduled/proactive channel work) after
 * provider verification; direct SDK/HTTP callers must not set it and are rejected here.
 */
export interface RequestContextInput {
  app?: Record<string, JsonValue>;
}

/**
 * Explicit reserved top-level request-context keys (§4.4c). `mastra__*` and `__mastra*` prefixes
 * are additionally rejected by {@link rejectNonAppKey}. The set is not exhaustive: ANY key other
 * than `app` is rejected, so future Harness/Mastra/server-owned slots are covered without edits.
 */
const RESERVED_REQUEST_CONTEXT_KEYS: ReadonlySet<string> = new Set([
  'harness',
  'channel',
  'MastraMemory',
  'browser',
  'user',
  'userPermissions',
  'userRoles',
]);

/**
 * §4.4c — the canonical reserved-key predicate. A key is infrastructure-owned (never a
 * caller `app` key) when it is an explicit reserved top-level slot or carries the
 * `mastra__*` / `__mastra*` namespace prefix. Single source of truth for both
 * {@link rejectNonAppKey} (admission-time rejection) and {@link stripReservedAppKeys}
 * (resume-time hygiene), so the two paths can never drift.
 */
export function isReservedRequestContextKey(key: string): boolean {
  return RESERVED_REQUEST_CONTEXT_KEYS.has(key) || key.startsWith('mastra__') || key.startsWith('__mastra');
}

/**
 * Drop reserved (infrastructure-owned) keys from a persisted caller `app` metadata bag.
 * Used on the resume restore path, where `pendingResume.requestContext.metadata` is read
 * back VERBATIM from durable storage and was never re-run through
 * {@link validateCallerRequestContext}. Admission already rejects reserved keys, so a
 * faithfully-persisted bag is unaffected; this strips any that a tampered/legacy durable
 * record smuggled in before the bag becomes the tool-visible `app` slot. Returns the same
 * reference when nothing was stripped (no allocation in the common case).
 */
export function stripReservedAppKeys<T>(metadata: Record<string, T>): Record<string, T> {
  // Persisted `app` metadata is canonical JSON (string keys only), so `Object.keys`
  // covers every key a durable record can carry.
  let cleaned: Record<string, T> | undefined;
  for (const key of Object.keys(metadata)) {
    if (isReservedRequestContextKey(key)) {
      if (cleaned === undefined) cleaned = { ...metadata };
      delete cleaned[key];
    }
  }
  return cleaned ?? metadata;
}

function rejectNonAppKey(key: string, path: string): never {
  if (isReservedRequestContextKey(key)) {
    throw new HarnessValidationError(
      `${path}.${key}`,
      `request-context key "${key}" is infrastructure-owned and cannot be supplied by callers`,
    );
  }
  throw new HarnessValidationError(
    `${path}.${key}`,
    `unexpected request-context key "${key}" — callers may only supply "app"`,
  );
}

/**
 * Validate caller-supplied request-context input against the §4.4c allowlist and the §5.1
 * JSON/canonicalization profile. Returns the normalized input — `{ app }` when an `app` bag is
 * present (even an empty `{}`), or `undefined` when no application metadata was supplied. Throws
 * {@link HarnessValidationError} (with a dotted `path` rooted at `${method}.requestContext`) for any
 * non-`app` top-level key, a non-object `app`, or a non-JSON `app` value.
 *
 * The returned bag is the normalized DTO used for admission/response hashing and for the
 * tool-visible `app`; it is replaced (never deep-merged) per entry point.
 */
export function validateCallerRequestContext(
  input: unknown,
  method: string,
): { app: Record<string, JsonValue> } | undefined {
  if (input === undefined) return undefined;
  const path = `${method}.requestContext`;
  if (input === null || typeof input !== 'object' || Array.isArray(input) || !isPlainJsonObject(input)) {
    throw new HarnessValidationError(path, 'must be a plain object whose only allowed property is "app"');
  }
  // Reflect.ownKeys (not Object.keys) so non-enumerable and symbol top-level keys
  // are rejected too — the contract is "only `app` may be supplied".
  for (const key of Reflect.ownKeys(input)) {
    if (key !== 'app') rejectNonAppKey(typeof key === 'symbol' ? key.toString() : key, path);
  }
  const rawApp = (input as { app?: unknown }).app;
  if (rawApp === undefined) return undefined;
  if (rawApp === null || typeof rawApp !== 'object' || Array.isArray(rawApp) || !isPlainJsonObject(rawApp)) {
    throw new HarnessValidationError(`${path}.app`, 'must be a JSON object');
  }
  const app = assertJsonValue(rawApp, `${path}.app`) as Record<string, JsonValue>;
  return { app };
}

/**
 * Map a normalized caller request context (the result of {@link validateCallerRequestContext}) onto
 * the durable {@link PersistedRequestContextInput} shape used by admission hashing and
 * `_buildRequestContext`. The caller `app` bag becomes the persisted `metadata` slot; the trusted
 * `channel` slot is never set from caller input. Returns `undefined` when no `app` was supplied, so
 * absent caller context leaves admission hashes and the tool context unchanged.
 */
export function callerRequestContextToPersisted(
  normalized: { app: Record<string, JsonValue> } | undefined,
): PersistedRequestContextInput | undefined {
  if (normalized === undefined) return undefined;
  return { metadata: normalized.app };
}
