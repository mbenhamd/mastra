import { RequestContext } from '../request-context';

/**
 * Snapshot caller-owned execution input before crossing an asynchronous trust boundary.
 *
 * Plain containers and RequestContext values are copied recursively while functions and
 * framework-owned class instances retain their identity. Data property descriptors,
 * cycles, sparse arrays, Maps, Sets, and Dates are preserved. Accessors are materialized
 * exactly once so they cannot reintroduce a caller-owned live value after the snapshot.
 */
function snapshotAgentExecutionValueInternal<T>(
  value: T,
  seen: WeakMap<object, unknown>,
  cloneOpaqueRoot = false,
  requestContextSnapshots?: Map<RequestContext, RequestContext>,
): T {
  if (value === null || (typeof value !== 'object' && typeof value !== 'function')) return value;
  if (typeof value === 'function') return value;

  const existing = seen.get(value);
  if (existing !== undefined) return existing as T;
  const copyOwnProperties = (target: object, excludedKeys = new Set<PropertyKey>()) => {
    for (const key of Reflect.ownKeys(value)) {
      if (excludedKeys.has(key)) continue;
      const descriptor = Object.getOwnPropertyDescriptor(value, key);
      if (!descriptor) continue;
      const snapshotDescriptor: PropertyDescriptor =
        'value' in descriptor
          ? {
              ...descriptor,
              value: snapshotAgentExecutionValueInternal(descriptor.value, seen, false, requestContextSnapshots),
            }
          : {
              configurable: descriptor.configurable,
              enumerable: descriptor.enumerable,
              writable: descriptor.set !== undefined,
              value: snapshotAgentExecutionValueInternal(Reflect.get(value, key), seen, false, requestContextSnapshots),
            };
      Object.defineProperty(target, key, snapshotDescriptor);
    }
  };
  if (value instanceof RequestContext) {
    const clone = new RequestContext();
    seen.set(value, clone);
    requestContextSnapshots?.set(value, clone);
    for (const [key, entryValue] of value.entries()) {
      clone.set(key, snapshotAgentExecutionValueInternal(entryValue, seen, false, requestContextSnapshots));
    }
    copyOwnProperties(clone, new Set(['registry']));
    return clone as T;
  }
  if (value instanceof Date) {
    const clone = new Date(value.getTime());
    seen.set(value, clone);
    copyOwnProperties(clone);
    return clone as T;
  }
  if (value instanceof Map) {
    const clone = new Map();
    seen.set(value, clone);
    for (const [key, entryValue] of value) {
      clone.set(
        snapshotAgentExecutionValueInternal(key, seen, false, requestContextSnapshots),
        snapshotAgentExecutionValueInternal(entryValue, seen, false, requestContextSnapshots),
      );
    }
    copyOwnProperties(clone);
    return clone as T;
  }
  if (value instanceof Set) {
    const clone = new Set();
    seen.set(value, clone);
    for (const entryValue of value) {
      clone.add(snapshotAgentExecutionValueInternal(entryValue, seen, false, requestContextSnapshots));
    }
    copyOwnProperties(clone);
    return clone as T;
  }
  if (Array.isArray(value)) {
    const clone: unknown[] = [];
    seen.set(value, clone);
    copyOwnProperties(clone);
    return clone as T;
  }

  const prototype = Object.getPrototypeOf(value);
  if (!cloneOpaqueRoot && prototype !== Object.prototype && prototype !== null) return value;

  const clone: Record<PropertyKey, unknown> = Object.create(prototype);
  seen.set(value, clone);
  copyOwnProperties(clone);
  return clone as T;
}

export function snapshotAgentExecutionValue<T>(value: T): T {
  return snapshotAgentExecutionValueInternal(value, new WeakMap());
}

/**
 * Execution-option fields that are live capability handles rather than caller-owned
 * data, so they cross the boundary by reference like the AbortSignal and Workspace
 * instances above. `mcp` is the calling MCP server's transport-bound context: its
 * `extra` carries the request signal plus `sendNotification`/`sendRequest`, and
 * `elicitation`/`log`/`progress` are transport closures. A durable run parks this
 * object on the run registry for the tool-call step (`preparation.ts` ->
 * `RunRegistryEntry.mcp`), so deep-copying the plain container would hand tools a
 * detached copy of a handle whose whole purpose is to talk to the live client.
 */
const LIVE_HANDLE_OPTION_FIELDS: readonly PropertyKey[] = ['mcp'];

function snapshotAgentExecutionOptionsInternal<T>(
  value: T,
  inheritedFields: readonly PropertyKey[],
  requestContextSnapshots?: Map<RequestContext, RequestContext>,
): T {
  const seen = new WeakMap<object, unknown>();
  const clone = snapshotAgentExecutionValueInternal(value, seen, true, requestContextSnapshots);
  if (clone === value || value === null || (typeof value !== 'object' && typeof value !== 'function')) return clone;

  for (const key of inheritedFields) {
    if (Object.prototype.hasOwnProperty.call(value, key) || !Reflect.has(value as object, key)) continue;
    Object.defineProperty(clone as object, key, {
      configurable: true,
      enumerable: true,
      writable: true,
      value: snapshotAgentExecutionValueInternal(
        Reflect.get(value as object, key),
        seen,
        false,
        requestContextSnapshots,
      ),
    });
  }

  for (const key of LIVE_HANDLE_OPTION_FIELDS) {
    if (!Object.prototype.hasOwnProperty.call(value as object, key)) continue;
    const handle = (value as Record<PropertyKey, unknown>)[key];
    if (handle === undefined) continue;
    Object.defineProperty(clone as object, key, {
      configurable: true,
      enumerable: true,
      writable: true,
      value: handle,
    });
  }

  return clone;
}

/**
 * Snapshot a public execution-options bag even when it is a structurally valid class instance.
 * Named inherited fields are materialized as own data properties so prototype accessors cannot
 * return live security-bearing values after the boundary snapshot.
 */
export function snapshotAgentExecutionOptions<T>(value: T, inheritedFields: readonly PropertyKey[] = []): T {
  return snapshotAgentExecutionOptionsInternal(value, inheritedFields);
}

/**
 * Snapshot public execution options and retain only the source-to-snapshot
 * RequestContext identity mapping needed to move private, in-process run state.
 */
export function snapshotAgentExecutionOptionsWithRequestContexts<T>(
  value: T,
  inheritedFields: readonly PropertyKey[] = [],
): {
  value: T;
  requestContextSnapshots: ReadonlyMap<RequestContext, RequestContext>;
} {
  const requestContextSnapshots = new Map<RequestContext, RequestContext>();
  return {
    value: snapshotAgentExecutionOptionsInternal(value, inheritedFields, requestContextSnapshots),
    requestContextSnapshots,
  };
}
