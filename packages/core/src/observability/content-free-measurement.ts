import { isAnyArrayBuffer, isArrayBufferView, isProxy } from 'node:util/types';

const encoder = new TextEncoder();

// Exact JSON measurement duplicates serialization already performed by the
// provider adapter. Keep that diagnostic work below a bounded input shape so
// large attachments cannot turn observability into a material dispatch delay.
export const MAX_EXACT_JSON_MEASUREMENT_CODE_UNITS = 256 * 1024;
export const MAX_EXACT_JSON_MEASUREMENT_NODES = 10_000;
export const MAX_EXACT_JSON_MEASUREMENT_DEPTH = 64;

export type ExactJsonMeasurementValue =
  | null
  | boolean
  | number
  | string
  | readonly ExactJsonMeasurementValue[]
  | { readonly [key: string]: ExactJsonMeasurementValue };

export type ExactJsonMeasurementSnapshot =
  | {
      readonly state: 'measured';
      /** A deeply frozen, data-only value detached from the provider request. */
      readonly snapshot: ExactJsonMeasurementValue;
      readonly utf8ByteLength: number;
    }
  | { readonly state: 'size_limited' }
  | { readonly state: 'unavailable' };

const OMIT_PROPERTY = Symbol('exact-json-measurement-omit-property');
const SIZE_LIMITED = Symbol('exact-json-measurement-size-limited');
const UNAVAILABLE = Symbol('exact-json-measurement-unavailable');

const SIZE_LIMITED_RESULT = Object.freeze({ state: 'size_limited' as const });
const UNAVAILABLE_RESULT = Object.freeze({ state: 'unavailable' as const });

type DetachedValue = ExactJsonMeasurementValue | typeof OMIT_PROPERTY;
type MaterializeResult = DetachedValue | typeof SIZE_LIMITED | typeof UNAVAILABLE;

type MaterializeState = {
  ancestors: WeakSet<object>;
  codeUnits: number;
  nodes: number;
};

function consumeCodeUnits(state: MaterializeState, count: number): boolean {
  if (count > MAX_EXACT_JSON_MEASUREMENT_CODE_UNITS - state.codeUnits) return false;
  state.codeUnits += count;
  return true;
}

function consumeNode(state: MaterializeState): boolean {
  if (state.nodes >= MAX_EXACT_JSON_MEASUREMENT_NODES) return false;
  state.nodes += 1;
  return true;
}

/** Exact code-unit length of the JSON-escaped string, including its quotes. */
function consumeJsonStringCodeUnits(state: MaterializeState, value: string): boolean {
  if (!consumeCodeUnits(state, 2)) return false;
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    if (
      code === 0x22 ||
      code === 0x5c ||
      code === 0x08 ||
      code === 0x09 ||
      code === 0x0a ||
      code === 0x0c ||
      code === 0x0d
    ) {
      if (!consumeCodeUnits(state, 2)) return false;
    } else if (code < 0x20 || (code >= 0xd800 && code <= 0xdfff)) {
      if (code >= 0xd800 && code <= 0xdbff) {
        const next = value.charCodeAt(index + 1);
        if (next >= 0xdc00 && next <= 0xdfff) {
          if (!consumeCodeUnits(state, 2)) return false;
          index += 1;
          continue;
        }
      }
      if (!consumeCodeUnits(state, 6)) return false;
    } else if (!consumeCodeUnits(state, 1)) {
      return false;
    }
  }
  return true;
}

function dataDescriptor(value: object, key: PropertyKey): PropertyDescriptor | undefined {
  return Object.getOwnPropertyDescriptor(value, key);
}

function materializeArray(value: unknown[], depth: number, state: MaterializeState): MaterializeResult {
  if (Object.getPrototypeOf(value) !== Array.prototype) return UNAVAILABLE;
  if (Object.getOwnPropertyDescriptor(Array.prototype, 'toJSON') !== undefined) return UNAVAILABLE;

  const lengthDescriptor = dataDescriptor(value, 'length');
  const length = lengthDescriptor && 'value' in lengthDescriptor ? lengthDescriptor.value : undefined;
  if (!lengthDescriptor || lengthDescriptor.enumerable || !Number.isSafeInteger(length) || (length as number) < 0) {
    return UNAVAILABLE;
  }
  if ((length as number) > MAX_EXACT_JSON_MEASUREMENT_NODES - state.nodes) return SIZE_LIMITED;

  const ownKeys = Reflect.ownKeys(value);
  if (ownKeys.length !== (length as number) + 1) return UNAVAILABLE;
  for (let index = 0; index < ownKeys.length; index += 1) {
    const key = ownKeys[index];
    if (key === 'length') continue;
    if (typeof key !== 'string' || !/^(0|[1-9][0-9]*)$/.test(key) || Number(key) >= (length as number)) {
      return UNAVAILABLE;
    }
  }

  if (!consumeCodeUnits(state, 2 + Math.max(0, (length as number) - 1))) return SIZE_LIMITED;
  const snapshot: ExactJsonMeasurementValue[] = new Array(length as number);
  Object.setPrototypeOf(snapshot, null);
  for (let index = 0; index < (length as number); index += 1) {
    const descriptor = dataDescriptor(value, String(index));
    if (!descriptor || !descriptor.enumerable || !('value' in descriptor)) return UNAVAILABLE;
    const item = materializeValue(descriptor.value, depth + 1, state, true);
    if (item === SIZE_LIMITED || item === UNAVAILABLE) return item;
    const normalized = item === OMIT_PROPERTY ? null : item;
    Object.defineProperty(snapshot, String(index), {
      configurable: true,
      enumerable: true,
      value: normalized,
      writable: true,
    });
  }
  return Object.freeze(snapshot);
}

function materializeObject(value: object, depth: number, state: MaterializeState): MaterializeResult {
  const prototype = Object.getPrototypeOf(value);
  if (prototype !== Object.prototype && prototype !== null) return UNAVAILABLE;
  if (prototype === Object.prototype && Object.getOwnPropertyDescriptor(Object.prototype, 'toJSON') !== undefined) {
    return UNAVAILABLE;
  }

  const ownKeys = Reflect.ownKeys(value);
  if (ownKeys.length > MAX_EXACT_JSON_MEASUREMENT_NODES - state.nodes) return SIZE_LIMITED;
  const keys: string[] = [];
  for (let index = 0; index < ownKeys.length; index += 1) {
    const key = ownKeys[index];
    if (typeof key !== 'string' || key === 'toJSON') return UNAVAILABLE;
    const descriptor = dataDescriptor(value, key);
    if (!descriptor || !descriptor.enumerable || !('value' in descriptor)) return UNAVAILABLE;
    keys.push(key);
  }
  keys.sort();

  if (!consumeCodeUnits(state, 2)) return SIZE_LIMITED;
  const snapshot: Record<string, ExactJsonMeasurementValue> = Object.create(null);
  let emitted = 0;
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index]!;
    const descriptor = dataDescriptor(value, key)!;
    const item = materializeValue(descriptor.value, depth + 1, state, false);
    if (item === SIZE_LIMITED || item === UNAVAILABLE) return item;
    if (item === OMIT_PROPERTY) continue;
    if (emitted > 0 && !consumeCodeUnits(state, 1)) return SIZE_LIMITED;
    if (!consumeJsonStringCodeUnits(state, key) || !consumeCodeUnits(state, 1)) {
      return SIZE_LIMITED;
    }
    Object.defineProperty(snapshot, key, {
      configurable: true,
      enumerable: true,
      value: item,
      writable: true,
    });
    emitted += 1;
  }
  return Object.freeze(snapshot);
}

function materializeValue(
  value: unknown,
  depth: number,
  state: MaterializeState,
  arrayEntry: boolean,
): MaterializeResult {
  if (depth > MAX_EXACT_JSON_MEASUREMENT_DEPTH) return SIZE_LIMITED;
  if (!consumeNode(state)) return SIZE_LIMITED;
  if (value === undefined) {
    if (!arrayEntry) return OMIT_PROPERTY;
    return consumeCodeUnits(state, 4) ? null : SIZE_LIMITED;
  }
  if (value === null) return consumeCodeUnits(state, 4) ? null : SIZE_LIMITED;
  if (typeof value === 'boolean') {
    return consumeCodeUnits(state, value ? 4 : 5) ? value : SIZE_LIMITED;
  }
  if (typeof value === 'string') {
    return consumeJsonStringCodeUnits(state, value) ? value : SIZE_LIMITED;
  }
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) return UNAVAILABLE;
    const normalized = value === 0 ? 0 : value;
    return consumeCodeUnits(state, String(normalized).length) ? normalized : SIZE_LIMITED;
  }
  if (typeof value !== 'object') return UNAVAILABLE;
  if (isProxy(value)) return UNAVAILABLE;
  if (isAnyArrayBuffer(value) || isArrayBufferView(value)) return UNAVAILABLE;
  if (state.ancestors.has(value)) return UNAVAILABLE;

  state.ancestors.add(value);
  try {
    return Array.isArray(value) ? materializeArray(value, depth, state) : materializeObject(value, depth, state);
  } finally {
    state.ancestors.delete(value);
  }
}

/**
 * Build a bounded immutable JSON snapshot without executing content-owned code.
 * Unsupported values fail closed as `unavailable`; budget exhaustion is
 * distinguishable as `size_limited`. This function never throws.
 */
export function createExactJsonMeasurementSnapshot(value: unknown): ExactJsonMeasurementSnapshot {
  try {
    const state: MaterializeState = {
      ancestors: new WeakSet<object>(),
      codeUnits: 0,
      nodes: 0,
    };
    const snapshot = materializeValue(value, 0, state, false);
    if (snapshot === SIZE_LIMITED) return SIZE_LIMITED_RESULT;
    if (snapshot === UNAVAILABLE || snapshot === OMIT_PROPERTY) return UNAVAILABLE_RESULT;

    // All containers in `snapshot` are detached null-prototype data containers,
    // so JSON serialization cannot reach input getters, iterators, or toJSON.
    const serialized = JSON.stringify(snapshot);
    if (serialized === undefined || serialized.length > MAX_EXACT_JSON_MEASUREMENT_CODE_UNITS) {
      return SIZE_LIMITED_RESULT;
    }
    return Object.freeze({
      state: 'measured' as const,
      snapshot,
      utf8ByteLength: encoder.encode(serialized).byteLength,
    });
  } catch {
    return UNAVAILABLE_RESULT;
  }
}
