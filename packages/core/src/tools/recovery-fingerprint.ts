import { createHash } from 'node:crypto';
import { stableStringify } from '../agent/message-list/cache/stable-stringify';
import { standardSchemaToJSONSchema, toStandardSchema } from '../schema';

function normalizeRecoveryValue(value: unknown, seen: Map<object, string>, path: string): unknown {
  if (value === undefined) return { $undefined: true };
  if (typeof value === 'function') return { $function: Function.prototype.toString.call(value) };
  if (typeof value === 'bigint') return { $bigint: value.toString() };
  if (typeof value === 'symbol') return { $symbol: Symbol.keyFor(value) ?? value.description ?? '' };
  if (typeof value === 'number' && !Number.isFinite(value)) return { $number: String(value) };
  if (value === null || typeof value !== 'object') return value;

  const previousPath = seen.get(value);
  if (previousPath !== undefined) return { $ref: previousPath };
  seen.set(value, path);

  if (value instanceof Date) return { $date: value.toISOString() };
  if (value instanceof RegExp && Object.getPrototypeOf(value) !== RegExp.prototype) {
    throw new TypeError(`Cannot create a durable recovery fingerprint for RegExp subclass at "${path}"`);
  }
  if (value instanceof RegExp) {
    const properties: Record<string, unknown> = {};
    for (const key of Object.keys(value).sort()) {
      properties[key] = normalizeRecoveryValue(
        (value as unknown as Record<string, unknown>)[key],
        seen,
        `${path}.${key}`,
      );
    }
    return {
      $regexp: value.source,
      $flags: value.flags,
      $lastIndex: normalizeRecoveryValue(value.lastIndex, seen, `${path}.lastIndex`),
      $properties: properties,
    };
  }
  if (Array.isArray(value)) {
    return value.map((item, index) => normalizeRecoveryValue(item, seen, `${path}[${index}]`));
  }
  if (value instanceof Map) {
    const entries = [...value.entries()].map(([key, entryValue], index) => [
      normalizeRecoveryValue(key, seen, `${path}.mapKey[${index}]`),
      normalizeRecoveryValue(entryValue, seen, `${path}.mapValue[${index}]`),
    ]);
    return { $map: entries.sort((left, right) => stableStringify(left[0]).localeCompare(stableStringify(right[0]))) };
  }
  if (value instanceof Set) {
    const entries = [...value].map((entry, index) => normalizeRecoveryValue(entry, seen, `${path}.set[${index}]`));
    return { $set: entries.sort((left, right) => stableStringify(left).localeCompare(stableStringify(right))) };
  }

  const normalized: Record<string, unknown> = {};
  for (const key of Object.keys(value).sort()) {
    normalized[key] = normalizeRecoveryValue((value as Record<string, unknown>)[key], seen, `${path}.${key}`);
  }
  return { $object: normalized };
}

function canonicalizeRecoverySchema(value: unknown, key?: string): unknown {
  if (Array.isArray(value)) {
    const items = value.map(item => canonicalizeRecoverySchema(item));
    if (key === 'required' || key === 'type') {
      return items.sort((left, right) => stableStringify(left).localeCompare(stableStringify(right)));
    }
    return items;
  }
  if (value === null || typeof value !== 'object') return value;

  return Object.fromEntries(
    Object.entries(value as Record<string, unknown>)
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([entryKey, entryValue]) => [entryKey, canonicalizeRecoverySchema(entryValue, entryKey)]),
  );
}

/** Convert a public schema into deterministic JSON for durable runtime binding. */
export function normalizeToolRecoverySchema(schema: unknown): unknown {
  if (schema === undefined) return undefined;
  try {
    return canonicalizeRecoverySchema(standardSchemaToJSONSchema(toStandardSchema(schema as any)));
  } catch {
    return normalizeRecoveryValue(schema, new Map(), '$schema');
  }
}

/** Preserve both portable JSON shape and runtime-only validation behavior for binding checks. */
export function normalizeToolRecoverySchemaIdentity(schema: unknown): unknown {
  if (schema === undefined) return undefined;
  return {
    jsonSchema: normalizeToolRecoverySchema(schema),
    runtimeSchema: normalizeRecoveryValue(schema, new Map(), '$runtimeSchema'),
  };
}

/** Hash every execution-affecting tool capability using deterministic object ordering. */
export function createToolRecoveryFingerprint(value: unknown): string {
  return createHash('sha256')
    .update(stableStringify(normalizeRecoveryValue(value, new Map(), '$tool')))
    .digest('hex');
}
