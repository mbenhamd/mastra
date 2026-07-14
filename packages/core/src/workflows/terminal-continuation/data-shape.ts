import { isProxy } from 'node:util/types';

interface PlainDataDescriptorOptions {
  allowNullPrototype: boolean;
  typeError: string;
  proxyError?: string;
  prototypeError?: string;
  fieldsError: string | ((key: PropertyKey) => string);
  selectedKeys?: ReadonlySet<string>;
  maxKeys?: number;
  maxKeysError?: string;
}

/** @internal Reads own data descriptors without invoking caller-controlled accessors. */
export function getPlainDataDescriptors(
  value: unknown,
  options: PlainDataDescriptorOptions,
): Record<string, PropertyDescriptor> {
  const fieldsError = (key: PropertyKey) =>
    typeof options.fieldsError === 'function' ? options.fieldsError(key) : options.fieldsError;
  if (isProxy(value)) throw new TypeError(options.proxyError ?? options.typeError);
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    throw new TypeError(options.typeError);
  }
  const prototype = Object.getPrototypeOf(value);
  if (prototype !== Object.prototype && !(options.allowNullPrototype && prototype === null)) {
    throw new TypeError(options.prototypeError ?? options.typeError);
  }
  if (options.selectedKeys) {
    const selected = Object.create(null) as Record<string, PropertyDescriptor>;
    for (const key of options.selectedKeys) {
      const descriptor = Object.getOwnPropertyDescriptor(value, key);
      if (!descriptor) continue;
      if (!('value' in descriptor) || descriptor.enumerable !== true) throw new TypeError(fieldsError(key));
      Object.defineProperty(selected, key, {
        configurable: true,
        enumerable: true,
        value: descriptor,
        writable: true,
      });
    }
    return selected;
  }
  const keys = Reflect.ownKeys(value);
  if (options.maxKeys !== undefined && keys.length > options.maxKeys) {
    throw new TypeError(options.maxKeysError ?? fieldsError(''));
  }
  const descriptors = Object.create(null) as Record<string, PropertyDescriptor>;
  for (const key of keys) {
    if (typeof key !== 'string') throw new TypeError(fieldsError(key));
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (!descriptor || !('value' in descriptor) || descriptor.enumerable !== true) {
      throw new TypeError(fieldsError(key));
    }
    Object.defineProperty(descriptors, key, {
      configurable: true,
      enumerable: true,
      value: descriptor,
      writable: true,
    });
  }
  return descriptors;
}

interface DenseDataArrayOptions {
  typeError: string;
  proxyError?: string;
  lengthError: string;
  dataError: string;
  minLength?: number;
  maxLength: number;
}

/** @internal Copies a bounded dense array without reading through accessors or proxies. */
export function getDenseDataArray(value: unknown, options: DenseDataArrayOptions): unknown[] {
  if (isProxy(value)) throw new TypeError(options.proxyError ?? options.typeError);
  if (!Array.isArray(value)) throw new TypeError(options.typeError);
  const length = Object.getOwnPropertyDescriptor(value, 'length')?.value;
  if (!Number.isSafeInteger(length) || length < (options.minLength ?? 0) || length > options.maxLength) {
    throw new TypeError(options.lengthError);
  }
  const keys = Reflect.ownKeys(value);
  if (
    keys.length !== length + 1 ||
    keys.some(
      key => key !== 'length' && (typeof key !== 'string' || !/^(0|[1-9][0-9]*)$/.test(key) || Number(key) >= length),
    )
  ) {
    throw new TypeError(options.dataError);
  }
  const result = Array.from({ length }, (_, index) => {
    const descriptor = Object.getOwnPropertyDescriptor(value, String(index));
    if (!descriptor || !('value' in descriptor)) throw new TypeError(options.dataError);
    return descriptor.value;
  });
  return result;
}
