import { describe, expect, it } from 'vitest';

import { HarnessValidationError } from './errors';
import {
  assertJsonValue,
  canonicalJson,
  isPlainJsonObject,
  jsonValuesEqual,
  sha256CanonicalJson,
  sha256CanonicalJsonChecked,
} from './canonical-json';

const SHA256_HEX = /^[0-9a-f]{64}$/;

/**
 * Characterization tests for §5.1 stable-hash canonicalization. These lock the EXACT canonical
 * form and the deliberate behavioral split between the validating and raw hash entry points so the
 * upcoming centralization (migrating session.ts / harness.ts call sites onto this module) is
 * provably behavior-preserving.
 */
describe('canonicalJson', () => {
  it('emits sorted object keys regardless of insertion order', () => {
    expect(canonicalJson({ b: 2, a: 1 })).toBe('{"a":1,"b":2}');
    expect(canonicalJson({ a: 1, b: 2 })).toBe('{"a":1,"b":2}');
  });

  it('preserves array element order', () => {
    expect(canonicalJson([1, 2])).toBe('[1,2]');
    expect(canonicalJson([2, 1])).toBe('[2,1]');
  });

  it('emits scalars via JSON.stringify (null, escaped strings)', () => {
    expect(canonicalJson(null)).toBe('null');
    expect(canonicalJson('a"b')).toBe('"a\\"b"');
    expect(canonicalJson(true)).toBe('true');
    expect(canonicalJson(0)).toBe('0');
    expect(canonicalJson('')).toBe('""');
  });

  it('preserves explicit null and distinguishes it from an absent key', () => {
    expect(canonicalJson({ a: null })).toBe('{"a":null}');
    expect(canonicalJson({})).toBe('{}');
    expect(canonicalJson({ a: null })).not.toBe(canonicalJson({}));
  });

  it('canonicalizes nested structures deterministically under key shuffling', () => {
    const left = canonicalJson({ z: { b: 1, a: 2 }, a: [3, 1] });
    const right = canonicalJson({ a: [3, 1], z: { a: 2, b: 1 } });
    expect(left).toBe('{"a":[3,1],"z":{"a":2,"b":1}}');
    expect(left).toBe(right);
  });
});

describe('assertJsonValue', () => {
  it('returns the value unchanged for valid JSON scalars and structures', () => {
    expect(assertJsonValue(null)).toBeNull();
    expect(assertJsonValue('x')).toBe('x');
    expect(assertJsonValue(true)).toBe(true);
    expect(assertJsonValue(42)).toBe(42);
    expect(assertJsonValue([1, 'a', null])).toEqual([1, 'a', null]);
  });

  it('drops object properties whose value is undefined (JSON.stringify semantics)', () => {
    expect(assertJsonValue({ a: 1, b: undefined })).toEqual({ a: 1 });
    // The dropped-undefined object hashes identically to the object without the key...
    expect(sha256CanonicalJsonChecked({ a: 1, b: undefined })).toBe(sha256CanonicalJsonChecked({ a: 1 }));
    // ...but explicit null is NOT dropped, so it differs from absence.
    expect(sha256CanonicalJsonChecked({ a: null })).not.toBe(sha256CanonicalJsonChecked({}));
    expect(sha256CanonicalJsonChecked({ a: null })).not.toBe(sha256CanonicalJsonChecked({ a: undefined }));
  });

  it('rejects non-finite numbers and negative zero with a dotted path', () => {
    expect(() => assertJsonValue(Number.NaN)).toThrow(HarnessValidationError);
    expect(() => assertJsonValue(Number.POSITIVE_INFINITY)).toThrow(HarnessValidationError);
    expect(() => assertJsonValue(Number.NEGATIVE_INFINITY)).toThrow(HarnessValidationError);
    expect(() => assertJsonValue(-0)).toThrow(HarnessValidationError);
    expect(() => assertJsonValue({ a: Number.NaN }, 'payload')).toThrow(/payload\.a/);
    // Positive zero is a valid finite number.
    expect(assertJsonValue(0)).toBe(0);
  });

  it('rejects sparse arrays at the missing index', () => {
    const sparse: number[] = [];
    sparse[2] = 3; // indices 0 and 1 are holes
    expect(() => assertJsonValue(sparse, 'arr')).toThrow(/arr\[0\]/);
  });

  it('rejects non-plain and non-JSON values', () => {
    expect(() => assertJsonValue(() => undefined)).toThrow(HarnessValidationError);
    expect(() => assertJsonValue(Symbol('x') as unknown)).toThrow(HarnessValidationError);
    expect(() => assertJsonValue(10n as unknown)).toThrow(HarnessValidationError);
    expect(() => assertJsonValue(new Date())).toThrow(HarnessValidationError);
    expect(() => assertJsonValue(new Map())).toThrow(HarnessValidationError);
  });
});

describe('isPlainJsonObject', () => {
  it('accepts plain and null-prototype objects, rejects class instances', () => {
    expect(isPlainJsonObject({})).toBe(true);
    expect(isPlainJsonObject(Object.create(null) as object)).toBe(true);
    expect(isPlainJsonObject(new Date())).toBe(false);
    expect(isPlainJsonObject([])).toBe(false);
  });
});

describe('jsonValuesEqual', () => {
  it('treats key-shuffled objects as equal and only undefined equals undefined', () => {
    expect(jsonValuesEqual({ a: 1, b: 2 }, { b: 2, a: 1 })).toBe(true);
    expect(jsonValuesEqual({ a: 1 }, { a: 2 })).toBe(false);
    expect(jsonValuesEqual(undefined, undefined)).toBe(true);
    expect(jsonValuesEqual(undefined, null)).toBe(false);
    expect(jsonValuesEqual(null, null)).toBe(true);
  });
});

describe('sha256CanonicalJson / sha256CanonicalJsonChecked', () => {
  it('produces a 64-char lowercase hex digest', () => {
    expect(sha256CanonicalJson({ a: 1 })).toMatch(SHA256_HEX);
    expect(sha256CanonicalJsonChecked({ a: 1 })).toMatch(SHA256_HEX);
  });

  it('is order-independent and idempotent for objects', () => {
    expect(sha256CanonicalJson({ a: 1, b: 2 })).toBe(sha256CanonicalJson({ b: 2, a: 1 }));
    expect(sha256CanonicalJsonChecked({ a: 1, b: 2 })).toBe(sha256CanonicalJsonChecked({ b: 2, a: 1 }));
  });

  it('agrees with the checked variant on already-valid JsonValue input', () => {
    const value = { z: 1, a: [true, null, 'x'], m: { q: 2 } };
    expect(sha256CanonicalJson(value)).toBe(sha256CanonicalJsonChecked(value));
  });

  it('differs by array order', () => {
    expect(sha256CanonicalJson([1, 2])).not.toBe(sha256CanonicalJson([2, 1]));
  });

  it('locks the deliberate raw-vs-checked split on -0', () => {
    // Raw does NOT validate: -0 canonicalizes via JSON.stringify to "0", colliding with 0.
    expect(sha256CanonicalJson(-0)).toBe(sha256CanonicalJson(0));
    // Checked validates first and rejects -0.
    expect(() => sha256CanonicalJsonChecked(-0)).toThrow(HarnessValidationError);
  });
});
