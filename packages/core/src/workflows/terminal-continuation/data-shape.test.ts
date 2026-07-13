import { describe, expect, it, vi } from 'vitest';
import { getDenseDataArray, getPlainDataDescriptors } from './data-shape';

function captureError(run: () => void): unknown {
  try {
    run();
  } catch (error) {
    return error;
  }
}

describe('workflow terminal data-shape guards', () => {
  it('enforces object bounds before retrieving per-property descriptors', () => {
    const getDescriptor = vi.spyOn(Object, 'getOwnPropertyDescriptor');
    getDescriptor.mockClear();
    let error: unknown;
    let descriptorCalls = 0;
    try {
      error = captureError(() =>
        getPlainDataDescriptors(
          { first: 1, second: 2 },
          {
            allowNullPrototype: true,
            typeError: 'invalid object',
            fieldsError: 'invalid field',
            maxKeys: 1,
            maxKeysError: 'too many keys',
          },
        ),
      );
      descriptorCalls = getDescriptor.mock.calls.length;
    } finally {
      getDescriptor.mockRestore();
    }
    expect(error).toEqual(new TypeError('too many keys'));
    expect(descriptorCalls).toBe(0);
  });

  it('enforces array bounds before enumerating entries', () => {
    const ownKeys = vi.spyOn(Reflect, 'ownKeys');
    ownKeys.mockClear();
    let error: unknown;
    let ownKeyCalls = 0;
    try {
      error = captureError(() =>
        getDenseDataArray([1, 2], {
          typeError: 'invalid array',
          lengthError: 'too many entries',
          dataError: 'invalid entry',
          maxLength: 1,
        }),
      );
      ownKeyCalls = ownKeys.mock.calls.length;
    } finally {
      ownKeys.mockRestore();
    }
    expect(error).toEqual(new TypeError('too many entries'));
    expect(ownKeyCalls).toBe(0);
  });

  it('preserves descriptor-only validation after bounds pass', () => {
    const object = { value: 1 };
    const descriptors = getPlainDataDescriptors(object, {
      allowNullPrototype: true,
      typeError: 'invalid object',
      fieldsError: 'invalid field',
      maxKeys: 1,
    });
    expect(Object.getPrototypeOf(descriptors)).toBeNull();
    expect(descriptors.value?.value).toBe(1);

    const accessorArray = [1];
    Object.defineProperty(accessorArray, '0', { enumerable: true, get: () => 1 });
    expect(() =>
      getDenseDataArray(accessorArray, {
        typeError: 'invalid array',
        lengthError: 'too many entries',
        dataError: 'invalid entry',
        maxLength: 1,
      }),
    ).toThrow('invalid entry');
  });
});
