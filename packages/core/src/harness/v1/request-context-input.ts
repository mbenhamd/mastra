import { isInfrastructureRequestContextKey } from '../../request-context';
import type { RequestContext } from '../../request-context';
import type { JsonValue, PersistedRequestContextInput } from '../../storage/domains/harness';

import { assertJsonValue, canonicalJson, isPlainJsonObject } from './canonical-json';
import { HarnessValidationError } from './errors';

/** Canonical JSON budget for the observability-only app subset inherited by subagents. */
export const MAX_INHERITED_REQUEST_CONTEXT_APP_BYTES = 256;
const REQUEST_CONTEXT_APP_ENCODER = new TextEncoder();

/** Enforce the bounded persistence/hash contract after the selected app bag is JSON-normalized. */
export function assertInheritedRequestContextAppByteLimit(
  app: Record<string, JsonValue>,
  path = 'requestContext.app',
): void {
  const bytes = REQUEST_CONTEXT_APP_ENCODER.encode(canonicalJson(app)).byteLength;
  if (bytes > MAX_INHERITED_REQUEST_CONTEXT_APP_BYTES) {
    throw new HarnessValidationError(
      path,
      `must be at most ${MAX_INHERITED_REQUEST_CONTEXT_APP_BYTES} canonical JSON UTF-8 bytes; received ${bytes}`,
    );
  }
}

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

function rejectNonAppKey(key: string, path: string): never {
  if (isInfrastructureRequestContextKey(key)) {
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

/**
 * Select an exact, operator-approved subset of an application metadata bag for
 * a descendant subagent turn. Keys are literal top-level app keys, not dotted
 * paths. The selected values are JSON-normalized again so the child never
 * shares mutable object identity with the parent turn.
 */
export function projectInheritedRequestContextAppBag(
  rawApp: unknown,
  appKeys: readonly string[],
): { app: Record<string, JsonValue> } | undefined {
  if (rawApp === undefined || appKeys.length === 0) return undefined;
  if (rawApp === null || typeof rawApp !== 'object' || Array.isArray(rawApp) || !isPlainJsonObject(rawApp)) {
    throw new HarnessValidationError('requestContext.app', 'must remain a JSON object during subagent delegation');
  }

  const app: Record<string, JsonValue> = {};
  let selected = 0;
  for (const key of appKeys) {
    const descriptor = Object.getOwnPropertyDescriptor(rawApp, key);
    if (descriptor === undefined || ('value' in descriptor && descriptor.value === undefined)) continue;
    if (!('value' in descriptor)) {
      throw new HarnessValidationError(
        `requestContext.app[${JSON.stringify(key)}]`,
        'must be an own JSON data property during subagent delegation',
      );
    }
    Object.defineProperty(app, key, {
      value: assertJsonValue(descriptor.value, `requestContext.app[${JSON.stringify(key)}]`),
      enumerable: true,
      configurable: true,
      writable: true,
    });
    selected += 1;
  }
  if (selected === 0) return undefined;
  assertInheritedRequestContextAppByteLimit(app);
  return { app };
}

/** Project the approved app-key subset from a live tool/agent RequestContext. */
export function projectInheritedRequestContextApp(
  requestContext: RequestContext | undefined,
  appKeys: readonly string[],
): { app: Record<string, JsonValue> } | undefined {
  return projectInheritedRequestContextAppBag(requestContext?.get('app'), appKeys);
}
