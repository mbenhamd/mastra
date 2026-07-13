import { describe, expect, it, vi } from 'vitest';
import { getDenseDataArray, getPlainDataDescriptors } from './data-shape';

describe('terminal continuation data-shape bounds', () => {
  it('rejects oversized objects before retrieving their descriptors', () => {
    const getDescriptor = vi.spyOn(Object, 'getOwnPropertyDescriptor');

    expect(() =>
      getPlainDataDescriptors(
        { first: 1, second: 2 },
        {
          allowNullPrototype: true,
          typeError: 'type',
          fieldsError: 'fields',
          maxKeys: 1,
          maxKeysError: 'too many keys',
        },
      ),
    ).toThrow('too many keys');
    expect(getDescriptor).not.toHaveBeenCalled();

    getDescriptor.mockRestore();
  });

  it('rejects oversized arrays before enumerating their entries', () => {
    const ownKeys = vi.spyOn(Reflect, 'ownKeys');

    expect(() =>
      getDenseDataArray([1, 2], {
        typeError: 'type',
        lengthError: 'too long',
        dataError: 'data',
        maxLength: 1,
      }),
    ).toThrow('too long');
    expect(ownKeys).not.toHaveBeenCalled();

    ownKeys.mockRestore();
  });
});
