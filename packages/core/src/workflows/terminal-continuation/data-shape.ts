import { isProxy } from 'node:util/types';

interface PlainDataDescriptorOptions {
  allowNullPrototype: boolean;
  typeError: string;
  proxyError?: string;
  prototypeError?: string;
  fieldsError: string | ((key: PropertyKey) => string);
  requireEnumerable?: boolean;
  nullPrototypeResult?: boolean;
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
    const selected: Record<string, PropertyDescriptor> = options.nullPrototypeResult ? Object.create(null) : {};
    for (const key of options.selectedKeys) {
      const descriptor = Object.getOwnPropertyDescriptor(value, key);
      if (!descriptor) continue;
      if (!('value' in descriptor) || (options.requireEnumerable && descriptor.enumerable !== true)) {
        throw new TypeError(fieldsError(key));
      }
      Object.defineProperty(selected, key, {
        configurable: true,
        enumerable: true,
        value: descriptor,
        writable: true,
      });
    }
    return selected;
  }
  const descriptors = Object.getOwnPropertyDescriptors(value);
  if (options.nullPrototypeResult) Object.setPrototypeOf(descriptors, null);
  if (options.maxKeys !== undefined && Reflect.ownKeys(descriptors).length > options.maxKeys) {
    throw new TypeError(options.maxKeysError ?? fieldsError(''));
  }
  for (const key of Reflect.ownKeys(descriptors)) {
    if (
      typeof key !== 'string' ||
      !('value' in descriptors[key]!) ||
      (options.requireEnumerable && descriptors[key]!.enumerable !== true)
    ) {
      throw new TypeError(fieldsError(key));
    }
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
  const descriptors = Object.getOwnPropertyDescriptors(value) as unknown as Record<PropertyKey, PropertyDescriptor>;
  const length = descriptors.length?.value;
  if (!Number.isSafeInteger(length) || length < (options.minLength ?? 0) || length > options.maxLength) {
    throw new TypeError(options.lengthError);
  }
  const result = Array.from({ length }, (_, index) => {
    const descriptor = descriptors[String(index)];
    if (!descriptor || !('value' in descriptor)) throw new TypeError(options.dataError);
    return descriptor.value;
  });
  if (
    Reflect.ownKeys(descriptors).some(
      key => key !== 'length' && (typeof key !== 'string' || !/^(0|[1-9][0-9]*)$/.test(key) || Number(key) >= length),
    )
  ) {
    throw new TypeError(options.dataError);
  }
  return result;
}
