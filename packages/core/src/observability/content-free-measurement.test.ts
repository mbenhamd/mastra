import { describe, expect, it, vi } from 'vitest';

import {
  MAX_EXACT_JSON_MEASUREMENT_CODE_UNITS,
  MAX_EXACT_JSON_MEASUREMENT_DEPTH,
  MAX_EXACT_JSON_MEASUREMENT_NODES,
  createExactJsonMeasurementSnapshot,
  isExactJsonMeasurementCandidate,
  jsonUtf8ByteLength,
} from './content-free-measurement';

describe('createExactJsonMeasurementSnapshot', () => {
  it('returns an exact immutable canonical snapshot detached from the input', () => {
    const shared = { text: 'private café' };
    const input = {
      z: undefined,
      b: [shared, undefined],
      a: shared,
    };

    const measurement = createExactJsonMeasurementSnapshot(input);

    expect(measurement.state).toBe('measured');
    if (measurement.state !== 'measured') throw new Error('expected a measured snapshot');
    expect(Object.isFrozen(measurement)).toBe(true);
    expect(Object.isFrozen(measurement.snapshot)).toBe(true);
    expect(Object.getPrototypeOf(measurement.snapshot)).toBeNull();

    const snapshot = measurement.snapshot as Record<string, unknown>;
    const snapshotArray = snapshot.b as readonly unknown[];
    expect(Object.isFrozen(snapshotArray)).toBe(true);
    expect(Object.getPrototypeOf(snapshotArray)).toBeNull();
    expect(snapshot.a).not.toBe(shared);
    expect(snapshotArray[0]).not.toBe(shared);

    const expected = '{"a":{"text":"private café"},"b":[{"text":"private café"},null]}';
    expect(JSON.stringify(measurement.snapshot)).toBe(expected);
    expect(measurement.utf8ByteLength).toBe(new TextEncoder().encode(expected).byteLength);
    expect(jsonUtf8ByteLength(input)).toBe(measurement.utf8ByteLength);
    expect(isExactJsonMeasurementCandidate(input)).toBe(true);

    shared.text = 'changed after measurement';
    expect(JSON.stringify(measurement.snapshot)).toBe(expected);
  });

  it('rejects active objects without invoking getters, toJSON, iterators, or proxy traps', () => {
    const getter = vi.fn(() => {
      throw new Error('getter must not run');
    });
    const withGetter = Object.defineProperty({}, 'secret', {
      enumerable: true,
      get: getter,
    });
    expect(createExactJsonMeasurementSnapshot(withGetter)).toEqual({ state: 'unavailable' });
    expect(getter).not.toHaveBeenCalled();

    const toJSON = vi.fn(() => ({ expanded: 'private' }));
    expect(createExactJsonMeasurementSnapshot({ safe: true, toJSON })).toEqual({
      state: 'unavailable',
    });
    expect(toJSON).not.toHaveBeenCalled();

    const indexGetter = vi.fn(() => {
      throw new Error('array index getter must not run');
    });
    const accessorArray = Object.defineProperty([undefined], '0', {
      enumerable: true,
      get: indexGetter,
    });
    expect(createExactJsonMeasurementSnapshot(accessorArray)).toEqual({ state: 'unavailable' });
    expect(indexGetter).not.toHaveBeenCalled();

    const iterator = vi.fn(() => {
      throw new Error('iterator must not run');
    });
    const iterableArray = [1];
    Object.defineProperty(iterableArray, Symbol.iterator, { value: iterator });
    expect(createExactJsonMeasurementSnapshot(iterableArray)).toEqual({ state: 'unavailable' });
    expect(iterator).not.toHaveBeenCalled();

    const proxyTraps = vi.fn(() => {
      throw new Error('proxy trap must not run');
    });
    const proxy = new Proxy(
      { safe: true },
      {
        get: proxyTraps,
        getOwnPropertyDescriptor: proxyTraps,
        getPrototypeOf: proxyTraps,
        ownKeys: proxyTraps,
      },
    );
    expect(createExactJsonMeasurementSnapshot(proxy)).toEqual({ state: 'unavailable' });
    expect(proxyTraps).not.toHaveBeenCalled();
  });

  it('fails closed for non-canonical, cyclic, symbolic, and binary values', () => {
    const sparse = new Array(1);
    const extraArrayProperty = [1] as number[] & { extra?: number };
    extraArrayProperty.extra = 2;
    const symbolProperty = { safe: true } as Record<PropertyKey, unknown>;
    symbolProperty[Symbol('secret')] = 'private';
    const cyclic: { self?: unknown } = {};
    cyclic.self = cyclic;
    class ArraySubclass<T> extends Array<T> {}

    for (const value of [
      sparse,
      extraArrayProperty,
      symbolProperty,
      cyclic,
      new ArraySubclass(1),
      new Uint8Array([1, 2, 3]),
      Buffer.from('private bytes'),
      new Date(0),
      new Map([['private', true]]),
      Number.NaN,
      1n,
      Symbol('private'),
    ]) {
      expect(createExactJsonMeasurementSnapshot(value)).toEqual({ state: 'unavailable' });
      expect(isExactJsonMeasurementCandidate(value)).toBe(false);
      expect(jsonUtf8ByteLength(value)).toBeUndefined();
    }

    const shared = { value: 'safe' };
    expect(createExactJsonMeasurementSnapshot({ first: shared, second: shared }).state).toBe('measured');
  });

  it('distinguishes code-unit, node, and depth budget exhaustion from unsupported input', () => {
    const oversizedText = 'x'.repeat(MAX_EXACT_JSON_MEASUREMENT_CODE_UNITS + 1);
    const escapedExpansion = '\0'.repeat(Math.floor(MAX_EXACT_JSON_MEASUREMENT_CODE_UNITS / 2));
    const oversizedArray = new Array(MAX_EXACT_JSON_MEASUREMENT_NODES).fill(null);
    let tooDeep: Record<string, unknown> = {};
    for (let depth = 0; depth <= MAX_EXACT_JSON_MEASUREMENT_DEPTH; depth += 1) {
      tooDeep = { child: tooDeep };
    }

    for (const value of [oversizedText, escapedExpansion, oversizedArray, tooDeep]) {
      expect(createExactJsonMeasurementSnapshot(value)).toEqual({ state: 'size_limited' });
      expect(() => createExactJsonMeasurementSnapshot(value)).not.toThrow();
    }
  });
});
