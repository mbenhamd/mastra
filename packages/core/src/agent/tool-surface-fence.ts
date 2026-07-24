import type { RequestContext } from '../request-context';
import { getToolHookSnapshotTarget } from '../tools/tool-hooks';

const DEFAULT_RUN_KEY = '__default__';
const MAX_RETAINED_FENCES_PER_CONTEXT = 64;
const MAX_SNAPSHOTTED_TOOL_OBJECTS = 10_000;
const REGEXP_COMPILE = Object.getOwnPropertyDescriptor(RegExp.prototype, 'compile')?.value as
  | ((pattern: string, flags?: string) => RegExp)
  | undefined;
const DESCRIPTOR_ONLY_STATE_TAGS = new Set([
  '[object Object]',
  '[object Array]',
  '[object Function]',
  '[object AsyncFunction]',
  '[object GeneratorFunction]',
  '[object AsyncGeneratorFunction]',
  '[object Error]',
]);
const OPAQUE_INTRINSIC_PROTOTYPES = new Set<object>(
  [
    'WeakRef',
    'FinalizationRegistry',
    'URL',
    'URLSearchParams',
    'Headers',
    'Request',
    'Response',
    'Blob',
    'File',
    'FormData',
    'AbortController',
    'AbortSignal',
    'ReadableStream',
    'WritableStream',
    'TransformStream',
  ].flatMap(name => {
    const constructor = (globalThis as Record<string, unknown>)[name];
    if (typeof constructor !== 'function') return [];
    const prototype = (constructor as { prototype?: unknown }).prototype;
    return isObjectLike(prototype) ? [prototype] : [];
  }),
);
const PROTECTED_TOOL_DEFINITION_KEYS = Object.freeze([
  'id',
  'name',
  'description',
  'type',
  'inputSchema',
  'outputSchema',
  'suspendSchema',
  'resumeSchema',
  'requestContextSchema',
  'parameters',
  'execute',
  'requireApproval',
  'needsApproval',
  'needsApprovalFn',
  'strict',
  'hasSuspendSchema',
  'providerOptions',
  'toModelOutput',
  'transform',
  'onInputStart',
  'onInputDelta',
  'onInputAvailable',
  'onOutput',
  'inputExamples',
  'args',
  'configuration',
  'config',
  'options',
  'background',
  'backgroundConfig',
  'terminalResult',
  'mcp',
  'mcpMetadata',
  'annotations',
  '_meta',
  'mastra',
] as const);
const DEEP_PROTECTED_TOOL_DEFINITION_KEYS = new Set<PropertyKey>([
  'inputSchema',
  'outputSchema',
  'suspendSchema',
  'resumeSchema',
  'requestContextSchema',
  'parameters',
  'providerOptions',
  'inputExamples',
  'args',
  'configuration',
  'config',
  'options',
  'background',
  'backgroundConfig',
  'terminalResult',
  'mcp',
  'mcpMetadata',
  'annotations',
  '_meta',
]);
const SCHEMA_DEFINITION_KEYS = new Set<PropertyKey>([
  'def',
  '_def',
  'shape',
  'properties',
  'items',
  'options',
  'innerType',
  'schema',
]);
const NO_DEEP_DEFINITION_KEYS = new Set<PropertyKey>();
const INTRINSIC_PROTOTYPES = new Set<object>([
  Object.prototype,
  Function.prototype,
  Array.prototype,
  ArrayBuffer.prototype,
  DataView.prototype,
  Map.prototype,
  Set.prototype,
  WeakMap.prototype,
  WeakSet.prototype,
  Date.prototype,
  RegExp.prototype,
  Promise.prototype,
  Error.prototype,
  Object.getPrototypeOf(Uint8Array.prototype),
  Uint8Array.prototype,
  Uint8ClampedArray.prototype,
  Uint16Array.prototype,
  Uint32Array.prototype,
  Int8Array.prototype,
  Int16Array.prototype,
  Int32Array.prototype,
  Float32Array.prototype,
  Float64Array.prototype,
  BigInt64Array.prototype,
  BigUint64Array.prototype,
  ...(typeof SharedArrayBuffer === 'undefined' ? [] : [SharedArrayBuffer.prototype]),
]);

interface OwnDescriptorSnapshot {
  readonly key: PropertyKey;
  readonly descriptor: PropertyDescriptor;
}

interface AccessorValueSnapshot {
  readonly key: PropertyKey;
  readonly value: unknown;
  readonly child?: ToolObjectSnapshot;
}

interface PrototypeSnapshot {
  readonly target: object;
  readonly prototype: object | null;
  readonly descriptors: readonly OwnDescriptorSnapshot[];
  readonly trackedKeys?: readonly PropertyKey[];
}

interface ToolObjectSnapshot {
  readonly target: object;
  readonly prototype: object | null;
  readonly descriptors: readonly OwnDescriptorSnapshot[];
  readonly accessorValues: readonly AccessorValueSnapshot[];
  readonly children: readonly ToolObjectSnapshot[];
  readonly builtInState?: BuiltInStateSnapshot;
  readonly trackedKeys?: readonly PropertyKey[];
  readonly prototypeSnapshots?: readonly PrototypeSnapshot[];
}

type BuiltInStateSnapshot =
  | { readonly kind: 'map'; readonly entries: readonly (readonly [unknown, unknown])[] }
  | { readonly kind: 'set'; readonly values: readonly unknown[] }
  | { readonly kind: 'date'; readonly time: number }
  | { readonly kind: 'regexp'; readonly source: string; readonly flags: string }
  | {
      readonly kind: 'bytes';
      readonly buffer: ArrayBufferLike;
      readonly byteLength: number;
      readonly bytes: Uint8Array;
    };

export interface ToolSurfaceFence {
  readonly allowedNames: readonly string[];
  readonly originalTools: Readonly<Record<string, unknown>>;
  readonly originalToolDescriptors: Readonly<Record<string, PropertyDescriptorMap>>;
  readonly originalToolSnapshots?: Readonly<Record<string, ToolObjectSnapshot>>;
}

export interface ProcessorToolSurfaceView {
  readonly tools: Record<string, unknown>;
  readonly fence: ToolSurfaceFence;
}

interface RegisteredToolSurfaceFence {
  readonly fence: ToolSurfaceFence;
  readonly ownerId?: string;
  readonly state: 'active' | 'suspended';
}

declare const suspendedToolSurfaceFenceLease: unique symbol;
export type SuspendedToolSurfaceFenceLease = {
  readonly [suspendedToolSurfaceFenceLease]: true;
};

// Keep enforcement state outside RequestContext's public key/value bag. A
// processor can read and mutate that bag, so storing the registry there would
// let it widen its own replacement ceiling.
const toolSurfaceFenceRegistries = new WeakMap<RequestContext, Map<string, RegisteredToolSurfaceFence>>();
const toolSurfaceRestoreRegistries = new WeakMap<RequestContext, Map<string, readonly string[]>>();

function runKey(runId?: string): string {
  return runId ?? DEFAULT_RUN_KEY;
}

function getOrCreateRegistry<T>(
  registries: WeakMap<RequestContext, Map<string, T>>,
  requestContext: RequestContext,
): Map<string, T> {
  const existing = registries.get(requestContext);
  if (existing) return existing;
  const registry = new Map<string, T>();
  registries.set(requestContext, registry);
  return registry;
}

function isObjectLike(value: unknown): value is object {
  return (typeof value === 'object' && value !== null) || typeof value === 'function';
}

function isPlainDefinitionContainer(value: object): boolean {
  const prototype = Object.getPrototypeOf(value);
  return Array.isArray(value) || prototype === Object.prototype || prototype === null;
}

function shouldSnapshotNestedDefinition(
  target: object,
  key: PropertyKey,
  deepKeys?: ReadonlySet<PropertyKey>,
): boolean {
  if (deepKeys) return deepKeys.has(key);
  return isPlainDefinitionContainer(target) || SCHEMA_DEFINITION_KEYS.has(key);
}

interface ObjectGraphSnapshotState {
  readonly seen: WeakSet<object>;
  readonly retained: { count: number };
  readonly prototypeSnapshots: Map<object, { trackedKeys?: Set<PropertyKey> }>;
}

function retainSnapshotObject(state: ObjectGraphSnapshotState): void {
  state.retained.count++;
  if (state.retained.count > MAX_SNAPSHOTTED_TOOL_OBJECTS) {
    throw new Error(
      `Replacement tool implementation exceeds the ${MAX_SNAPSHOTTED_TOOL_OBJECTS}-object executable-state snapshot limit.`,
    );
  }
}

function snapshotPrototypeChain(
  target: object,
  state: ObjectGraphSnapshotState,
  trackedKeys?: readonly PropertyKey[],
): void {
  let prototype = Object.getPrototypeOf(target);
  while (prototype !== null && !INTRINSIC_PROTOTYPES.has(prototype)) {
    const existing = state.prototypeSnapshots.get(prototype);
    if (!existing) {
      retainSnapshotObject(state);
      state.prototypeSnapshots.set(prototype, {
        ...(trackedKeys ? { trackedKeys: new Set(trackedKeys) } : {}),
      });
    } else if (existing.trackedKeys && !trackedKeys) {
      delete existing.trackedKeys;
    } else if (existing.trackedKeys && trackedKeys) {
      for (const key of trackedKeys) existing.trackedKeys.add(key);
    }
    prototype = Object.getPrototypeOf(prototype);
  }
}

function freezePrototypeSnapshots(state: ObjectGraphSnapshotState): readonly PrototypeSnapshot[] {
  return Object.freeze(
    [...state.prototypeSnapshots].map(([target, selection]) => {
      const trackedKeys = selection.trackedKeys ? [...selection.trackedKeys] : undefined;
      const trackedKeySet = trackedKeys ? new Set(trackedKeys) : undefined;
      const descriptors = Reflect.ownKeys(target)
        .filter(key => typeof key === 'symbol' || !trackedKeySet || trackedKeySet.has(key))
        .map(key => {
          const descriptor = Object.getOwnPropertyDescriptor(target, key);
          if (!descriptor) {
            throw new Error(`Replacement tool prototype property ${String(key)} disappeared while being snapshotted.`);
          }
          return Object.freeze({ key, descriptor: Object.freeze({ ...descriptor }) });
        });
      return Object.freeze({
        target,
        prototype: Object.getPrototypeOf(target),
        descriptors: Object.freeze(descriptors),
        ...(trackedKeys ? { trackedKeys: Object.freeze(trackedKeys) } : {}),
      });
    }),
  );
}

function isSharedArrayBuffer(value: object): value is SharedArrayBuffer {
  return typeof SharedArrayBuffer !== 'undefined' && value instanceof SharedArrayBuffer;
}

function readArrayBufferByteLength(buffer: ArrayBufferLike): number {
  if (buffer instanceof ArrayBuffer) {
    return Object.getOwnPropertyDescriptor(ArrayBuffer.prototype, 'byteLength')!.get!.call(buffer);
  }
  if (isSharedArrayBuffer(buffer)) {
    return Object.getOwnPropertyDescriptor(SharedArrayBuffer.prototype, 'byteLength')!.get!.call(buffer);
  }
  throw new Error('unsupported backing buffer');
}

function readViewBuffer(view: ArrayBufferView): ArrayBufferLike {
  if (view instanceof DataView) {
    return Object.getOwnPropertyDescriptor(DataView.prototype, 'buffer')!.get!.call(view);
  }
  const typedArrayPrototype = Object.getPrototypeOf(Uint8Array.prototype);
  return Object.getOwnPropertyDescriptor(typedArrayPrototype, 'buffer')!.get!.call(view);
}

function assertFixedLengthBuffer(buffer: ArrayBufferLike): void {
  if (buffer instanceof ArrayBuffer) {
    const resizable = Object.getOwnPropertyDescriptor(ArrayBuffer.prototype, 'resizable')?.get?.call(buffer);
    if (resizable) throw new Error('resizable ArrayBuffer state cannot be retained safely');
    return;
  }
  if (isSharedArrayBuffer(buffer)) {
    throw new Error('SharedArrayBuffer executable state cannot be retained safely');
  }
}

function snapshotBuiltInState(
  target: object,
): { snapshot: BuiltInStateSnapshot; childValues: readonly unknown[] } | undefined {
  if (target instanceof WeakMap || target instanceof WeakSet || target instanceof Promise) {
    throw new Error('WeakMap, WeakSet, and Promise executable state cannot be snapshotted safely');
  }
  if (target instanceof Map) {
    const entries = Array.from(
      Map.prototype.entries.call(target) as IterableIterator<readonly [unknown, unknown]>,
      ([key, value]) => Object.freeze([key, value] as const),
    );
    return {
      snapshot: { kind: 'map', entries: Object.freeze(entries) },
      childValues: entries.flatMap(([key, value]) => [key, value]),
    };
  }
  if (target instanceof Set) {
    const values = Array.from(Set.prototype.values.call(target) as IterableIterator<unknown>);
    return {
      snapshot: { kind: 'set', values: Object.freeze(values) },
      childValues: values,
    };
  }
  if (target instanceof Date) {
    return {
      snapshot: { kind: 'date', time: Date.prototype.getTime.call(target) },
      childValues: [],
    };
  }
  if (target instanceof RegExp) {
    if (!REGEXP_COMPILE) throw new Error('RegExp executable state cannot be restored safely');
    const source = Object.getOwnPropertyDescriptor(RegExp.prototype, 'source')!.get!.call(target);
    const flags = Object.getOwnPropertyDescriptor(RegExp.prototype, 'flags')!.get!.call(target);
    return { snapshot: { kind: 'regexp', source, flags }, childValues: [] };
  }
  if (target instanceof ArrayBuffer || isSharedArrayBuffer(target) || ArrayBuffer.isView(target)) {
    const buffer = target instanceof ArrayBuffer || isSharedArrayBuffer(target) ? target : readViewBuffer(target);
    assertFixedLengthBuffer(buffer);
    const byteLength = readArrayBufferByteLength(buffer);
    const bytes = Uint8Array.from(new Uint8Array(buffer));
    return {
      snapshot: { kind: 'bytes', buffer, byteLength, bytes },
      childValues: [],
    };
  }
  // Ordinary objects, arrays, functions, errors, and user-defined class
  // instances have descriptor/prototype state that the graph snapshot below
  // can observe. Other branded objects may hide mutable state in internal
  // slots. Reject those unless they have an explicit snapshot/restore policy
  // above; treating their own descriptors as complete would fail open.
  const stateTag = Object.prototype.toString.call(target);
  let prototype: object | null = target;
  let hasOpaqueIntrinsicPrototype = false;
  while ((prototype = Object.getPrototypeOf(prototype)) !== null) {
    if (OPAQUE_INTRINSIC_PROTOTYPES.has(prototype)) {
      hasOpaqueIntrinsicPrototype = true;
      break;
    }
  }
  // Object.prototype.toString() is not a trustworthy brand check by itself:
  // an ordinary class may define Symbol.toStringTag, while an opaque object
  // may spoof "Object". Known opaque platform prototypes remain fail-closed;
  // otherwise a descriptor-visible user class is handled like an ordinary
  // object regardless of its cosmetic tag.
  let hasDescriptorVisibleToStringTag = false;
  prototype = target;
  while (prototype !== null && !INTRINSIC_PROTOTYPES.has(prototype)) {
    if (Object.hasOwn(prototype, Symbol.toStringTag)) {
      hasDescriptorVisibleToStringTag = true;
      break;
    }
    prototype = Object.getPrototypeOf(prototype);
  }
  if (hasOpaqueIntrinsicPrototype || (!DESCRIPTOR_ONLY_STATE_TAGS.has(stateTag) && !hasDescriptorVisibleToStringTag)) {
    throw new Error(`${stateTag} internal-slot executable state cannot be snapshotted safely`);
  }
  return undefined;
}

function isTypedArrayIndex(key: PropertyKey): boolean {
  if (typeof key !== 'string' || key === '') return false;
  const index = Number(key);
  return Number.isInteger(index) && index >= 0 && String(index) === key;
}

function snapshotObjectGraph(
  target: object,
  state: ObjectGraphSnapshotState,
  trackedKeys?: readonly PropertyKey[],
  deepKeys?: ReadonlySet<PropertyKey>,
): ToolObjectSnapshot | undefined {
  if (state.seen.has(target)) return undefined;
  state.seen.add(target);
  retainSnapshotObject(state);

  const prototype = Object.getPrototypeOf(target);
  const builtIn = snapshotBuiltInState(target);
  const functionDefinitionKeys = typeof target === 'function' && !trackedKeys ? Reflect.ownKeys(target) : undefined;
  const effectiveTrackedKeys = trackedKeys ?? functionDefinitionKeys;
  snapshotPrototypeChain(target, state, effectiveTrackedKeys);
  const trackedKeySet = effectiveTrackedKeys ? new Set(effectiveTrackedKeys) : undefined;
  const descriptors = Reflect.ownKeys(target)
    .filter(
      key =>
        (!trackedKeySet || trackedKeySet.has(key)) && !(builtIn?.snapshot.kind === 'bytes' && isTypedArrayIndex(key)),
    )
    .map(key => {
      const descriptor = Object.getOwnPropertyDescriptor(target, key);
      if (!descriptor) throw new Error(`Replacement tool property ${String(key)} disappeared while being snapshotted.`);
      return Object.freeze({ key, descriptor: Object.freeze({ ...descriptor }) });
    });
  const children = descriptors.flatMap(({ key, descriptor }) => {
    if (!('value' in descriptor) || !isObjectLike(descriptor.value)) return [];
    // A function's own descriptors are executable-definition state, but
    // object-valued properties are also commonly used for last-invocation
    // metadata (request context, AbortSignal, provider response, and so on).
    // Capture the descriptor identity without recursively treating that live
    // runtime graph as tool configuration. Function-valued properties still
    // receive their own descriptor snapshot.
    if (typeof target === 'function' && typeof descriptor.value !== 'function') return [];
    if (!shouldSnapshotNestedDefinition(target, key, deepKeys) && typeof descriptor.value !== 'function') {
      return [];
    }
    const child = snapshotObjectGraph(descriptor.value, state);
    return child ? [child] : [];
  });
  for (const value of builtIn?.childValues ?? []) {
    if (!isObjectLike(value)) continue;
    const child = snapshotObjectGraph(value, state);
    if (child) children.push(child);
  }
  const accessorValues = descriptors.flatMap(({ key, descriptor }) => {
    // Do not invoke function-owned accessors while fencing. Mock functions and
    // user functions may expose throwing or live runtime accessors (`mock`,
    // `arguments`, `lastContext`) that aren't executable definitions.
    if (typeof target === 'function' || !shouldSnapshotNestedDefinition(target, key, deepKeys)) return [];
    if (
      'value' in descriptor ||
      (!descriptor.enumerable && !SCHEMA_DEFINITION_KEYS.has(key)) ||
      descriptor.get === undefined
    ) {
      return [];
    }
    let value: unknown;
    try {
      value = Reflect.get(target, key);
    } catch (error) {
      throw new Error(`Cannot snapshot accessor-backed replacement tool property "${String(key)}".`, { cause: error });
    }
    const child = isObjectLike(value) ? snapshotObjectGraph(value, state) : undefined;
    return [Object.freeze({ key, value, ...(child ? { child } : {}) })];
  });
  return Object.freeze({
    target,
    prototype,
    descriptors: Object.freeze(descriptors),
    accessorValues: Object.freeze(accessorValues),
    children: Object.freeze(children),
    ...(builtIn ? { builtInState: Object.freeze(builtIn.snapshot) } : {}),
    ...(effectiveTrackedKeys ? { trackedKeys: Object.freeze([...effectiveTrackedKeys]) } : {}),
  });
}

function snapshotToolImplementation(tool: object): ToolObjectSnapshot {
  const symbolKeys = Reflect.ownKeys(tool).filter((key): key is symbol => typeof key === 'symbol');
  const state: ObjectGraphSnapshotState = {
    seen: new WeakSet<object>(),
    retained: { count: 0 },
    prototypeSnapshots: new Map(),
  };
  const snapshot = snapshotObjectGraph(
    tool,
    state,
    [...PROTECTED_TOOL_DEFINITION_KEYS, ...symbolKeys],
    DEEP_PROTECTED_TOOL_DEFINITION_KEYS,
  )!;
  // Hook wrapping deliberately clones a tool while retaining the true method
  // receiver in the execute closure. Snapshot that receiver as part of the
  // same graph so a later processor cannot mutate its prototype/configuration
  // outside the replacement fence. Keeping the original receiver also
  // preserves class private-field semantics when the hook invokes execute.
  const hookSnapshotTarget = getToolHookSnapshotTarget(tool);
  const hookTargetSnapshot =
    hookSnapshotTarget && hookSnapshotTarget !== tool
      ? snapshotObjectGraph(
          hookSnapshotTarget,
          state,
          [
            ...PROTECTED_TOOL_DEFINITION_KEYS,
            ...Reflect.ownKeys(hookSnapshotTarget).filter((key): key is symbol => typeof key === 'symbol'),
          ],
          DEEP_PROTECTED_TOOL_DEFINITION_KEYS,
        )
      : undefined;
  return Object.freeze({
    ...snapshot,
    children: hookTargetSnapshot ? Object.freeze([...snapshot.children, hookTargetSnapshot]) : snapshot.children,
    prototypeSnapshots: freezePrototypeSnapshots(state),
  });
}

interface ProcessorCloneState {
  readonly seen: Map<object, object>;
  readonly retained: { count: number };
}

function retainProcessorClone(state: ProcessorCloneState): void {
  state.retained.count++;
  if (state.retained.count > MAX_SNAPSHOTTED_TOOL_OBJECTS) {
    throw new Error(
      `Replacement tool implementation exceeds the ${MAX_SNAPSHOTTED_TOOL_OBJECTS}-object processor-view limit.`,
    );
  }
}

function cloneProcessorPrototype(prototype: object | null, state: ProcessorCloneState): object | null {
  if (prototype === null || INTRINSIC_PROTOTYPES.has(prototype) || OPAQUE_INTRINSIC_PROTOTYPES.has(prototype)) {
    return prototype;
  }
  return cloneProcessorObject(prototype, state);
}

function cloneProcessorBuiltIn(target: object, state: ProcessorCloneState): object | undefined {
  if (target instanceof Map) {
    const clone = new Map();
    state.seen.set(target, clone);
    for (const [key, value] of Map.prototype.entries.call(target) as IterableIterator<[unknown, unknown]>) {
      clone.set(cloneProcessorValue(key, state), cloneProcessorValue(value, state));
    }
    return clone;
  }
  if (target instanceof Set) {
    const clone = new Set();
    state.seen.set(target, clone);
    for (const value of Set.prototype.values.call(target) as IterableIterator<unknown>) {
      clone.add(cloneProcessorValue(value, state));
    }
    return clone;
  }
  if (target instanceof Date) {
    const clone = new Date(Date.prototype.getTime.call(target));
    state.seen.set(target, clone);
    return clone;
  }
  if (target instanceof RegExp) {
    const source = Object.getOwnPropertyDescriptor(RegExp.prototype, 'source')!.get!.call(target);
    const flags = Object.getOwnPropertyDescriptor(RegExp.prototype, 'flags')!.get!.call(target);
    const clone = new RegExp(source, flags);
    clone.lastIndex = (target as RegExp).lastIndex;
    state.seen.set(target, clone);
    return clone;
  }
  if (target instanceof ArrayBuffer) {
    const clone = target.slice(0);
    state.seen.set(target, clone);
    return clone;
  }
  if (ArrayBuffer.isView(target)) {
    const sourceBuffer = readViewBuffer(target);
    assertFixedLengthBuffer(sourceBuffer);
    const clonedBuffer = sourceBuffer instanceof ArrayBuffer ? sourceBuffer.slice(0) : undefined;
    if (!clonedBuffer) throw new Error('SharedArrayBuffer executable state cannot be retained safely');
    const clone =
      target instanceof DataView
        ? new DataView(clonedBuffer, target.byteOffset, target.byteLength)
        : new (target.constructor as new (buffer: ArrayBuffer, byteOffset: number, length: number) => object)(
            clonedBuffer,
            target.byteOffset,
            (target as unknown as { length: number }).length,
          );
    state.seen.set(target, clone);
    return clone;
  }
  return undefined;
}

function cloneProcessorValue(value: unknown, state: ProcessorCloneState): unknown {
  return isObjectLike(value) ? cloneProcessorObject(value, state) : value;
}

function cloneProcessorObject(
  target: object,
  state: ProcessorCloneState,
  trackedKeys?: readonly PropertyKey[],
  deepKeys?: ReadonlySet<PropertyKey>,
): object {
  const existing = state.seen.get(target);
  if (existing) return existing;
  retainProcessorClone(state);

  const builtInClone = cloneProcessorBuiltIn(target, state);
  let clone: object;
  if (builtInClone) {
    clone = builtInClone;
  } else if (typeof target === 'function') {
    const original = target as (...args: unknown[]) => unknown;
    clone = function (this: unknown, ...args: unknown[]) {
      return Reflect.apply(original, this, args);
    };
    state.seen.set(target, clone);
    Reflect.setPrototypeOf(clone, cloneProcessorPrototype(Object.getPrototypeOf(target), state));
  } else {
    clone = Object.create(cloneProcessorPrototype(Object.getPrototypeOf(target), state));
    state.seen.set(target, clone);
  }

  const trackedKeySet = trackedKeys ? new Set(trackedKeys) : undefined;
  for (const key of Reflect.ownKeys(target)) {
    if (trackedKeySet && !trackedKeySet.has(key)) continue;
    if (ArrayBuffer.isView(target) && isTypedArrayIndex(key)) continue;
    const descriptor = Object.getOwnPropertyDescriptor(target, key);
    if (!descriptor) continue;

    let processorDescriptor: PropertyDescriptor;
    if ('value' in descriptor) {
      let processorValue = descriptor.value;
      if (isObjectLike(descriptor.value)) {
        if (typeof descriptor.value === 'function' || shouldSnapshotNestedDefinition(target, key, deepKeys)) {
          processorValue = cloneProcessorValue(descriptor.value, state);
        } else if (typeof target === 'function' && isPlainDefinitionContainer(descriptor.value)) {
          // Give processors a shallow copy of object-valued function metadata
          // so direct edits don't alter the retained function. Nested runtime
          // handles remain runtime state and aren't recursively traversed.
          processorValue = cloneProcessorObject(
            descriptor.value,
            state,
            Reflect.ownKeys(descriptor.value),
            NO_DEEP_DEFINITION_KEYS,
          );
        }
      }
      processorDescriptor = {
        ...descriptor,
        value: processorValue,
      };
    } else if (
      descriptor.get &&
      (descriptor.enumerable || SCHEMA_DEFINITION_KEYS.has(key)) &&
      (typeof target === 'function' || shouldSnapshotNestedDefinition(target, key, deepKeys))
    ) {
      let value: unknown;
      try {
        value = Reflect.get(target, key);
      } catch (error) {
        throw new Error(`Cannot materialize accessor-backed replacement tool property "${String(key)}".`, {
          cause: error,
        });
      }
      processorDescriptor = {
        value: cloneProcessorValue(value, state),
        writable: false,
        enumerable: descriptor.enumerable,
        configurable: descriptor.configurable,
      };
    } else {
      processorDescriptor = descriptor;
    }

    const current = Object.getOwnPropertyDescriptor(clone, key);
    if (current && !current.configurable && !sameDescriptor(current, processorDescriptor)) continue;
    Reflect.defineProperty(clone, key, processorDescriptor);
  }
  return clone;
}

/**
 * Give durable input processors an isolated executable-definition view. Any
 * irreversible descriptor or private-class mutation is confined to this view;
 * successful validation materializes the registered implementations instead.
 */
export function createProcessorToolSurfaceView(fence: ToolSurfaceFence): ProcessorToolSurfaceView {
  const state: ProcessorCloneState = { seen: new Map(), retained: { count: 0 } };
  const tools: Record<string, unknown> = {};
  for (const toolName of fence.allowedNames) {
    const originalTool = fence.originalTools[toolName];
    defineRecordValue(
      tools,
      toolName,
      isObjectLike(originalTool)
        ? cloneProcessorObject(originalTool, state, undefined, DEEP_PROTECTED_TOOL_DEFINITION_KEYS)
        : originalTool,
    );
  }
  return { tools, fence: immutableFence(tools, fence.allowedNames) };
}

function sameDescriptor(current: PropertyDescriptor | undefined, original: PropertyDescriptor): boolean {
  return (
    current?.value === original.value &&
    current?.get === original.get &&
    current?.set === original.set &&
    current?.writable === original.writable &&
    current?.enumerable === original.enumerable &&
    current?.configurable === original.configurable
  );
}

function equivalentViewValue(current: unknown, original: unknown, seen = new WeakMap<object, object>()): boolean {
  if (Object.is(current, original)) return true;
  if (!isObjectLike(current) || !isObjectLike(original)) return false;
  if (Object.getPrototypeOf(current) !== Object.getPrototypeOf(original)) return false;
  if (!isPlainDefinitionContainer(current) || !isPlainDefinitionContainer(original)) {
    for (const key of ['type', 'id', 'name'] as const) {
      const currentDescriptor = Object.getOwnPropertyDescriptor(current, key);
      const originalDescriptor = Object.getOwnPropertyDescriptor(original, key);
      if (currentDescriptor || originalDescriptor) {
        return currentDescriptor?.value === originalDescriptor?.value;
      }
    }
    return false;
  }
  if (seen.get(current) === original) return true;
  seen.set(current, original);
  const currentKeys = Reflect.ownKeys(current);
  const originalKeys = Reflect.ownKeys(original);
  if (currentKeys.length !== originalKeys.length || currentKeys.some(key => !originalKeys.includes(key))) return false;
  return originalKeys.every(key => {
    const currentDescriptor = Object.getOwnPropertyDescriptor(current, key);
    const originalDescriptor = Object.getOwnPropertyDescriptor(original, key);
    if (!currentDescriptor || !originalDescriptor) return false;
    if (
      currentDescriptor.enumerable !== originalDescriptor.enumerable ||
      currentDescriptor.configurable !== originalDescriptor.configurable ||
      'value' in currentDescriptor !== 'value' in originalDescriptor
    ) {
      return false;
    }
    if ('value' in originalDescriptor) {
      return (
        currentDescriptor.writable === originalDescriptor.writable &&
        equivalentViewValue(currentDescriptor.value, originalDescriptor.value, seen)
      );
    }
    return (
      Boolean(currentDescriptor.get) === Boolean(originalDescriptor.get) &&
      Boolean(currentDescriptor.set) === Boolean(originalDescriptor.set)
    );
  });
}

function sameViewDescriptor(current: PropertyDescriptor | undefined, original: PropertyDescriptor): boolean {
  if (!current) return false;
  if (
    current.enumerable !== original.enumerable ||
    current.configurable !== original.configurable ||
    'value' in current !== 'value' in original
  ) {
    return false;
  }
  if ('value' in original) {
    return current.writable === original.writable && equivalentViewValue(current.value, original.value);
  }
  return Boolean(current.get) === Boolean(original.get) && Boolean(current.set) === Boolean(original.set);
}

function matchesSnapshotView(current: unknown, snapshot: ToolObjectSnapshot): boolean {
  if (!isObjectLike(current) || Object.getPrototypeOf(current) !== snapshot.prototype) return false;
  const expectedKeys = new Set(snapshot.descriptors.map(({ key }) => key));
  const trackedKeys = snapshot.trackedKeys ? new Set(snapshot.trackedKeys) : undefined;
  for (const key of Reflect.ownKeys(current)) {
    if (trackedKeys && !trackedKeys.has(key)) continue;
    if (snapshot.builtInState?.kind === 'bytes' && isTypedArrayIndex(key)) continue;
    if (!expectedKeys.has(key)) return false;
  }
  if (
    !snapshot.descriptors.every(({ key, descriptor }) =>
      sameViewDescriptor(Object.getOwnPropertyDescriptor(current, key), descriptor),
    )
  ) {
    return false;
  }
  return snapshot.builtInState ? matchesBuiltInState(current, snapshot.builtInState) : true;
}

function matchesBuiltInState(target: object, snapshot: BuiltInStateSnapshot): boolean {
  try {
    if (snapshot.kind === 'map') {
      const entries = Array.from(Map.prototype.entries.call(target) as IterableIterator<readonly [unknown, unknown]>);
      return (
        entries.length === snapshot.entries.length &&
        entries.every(
          ([key, value], index) =>
            Object.is(key, snapshot.entries[index]![0]) && Object.is(value, snapshot.entries[index]![1]),
        )
      );
    }
    if (snapshot.kind === 'set') {
      const values = Array.from(Set.prototype.values.call(target) as IterableIterator<unknown>);
      return (
        values.length === snapshot.values.length &&
        values.every((value, index) => Object.is(value, snapshot.values[index]))
      );
    }
    if (snapshot.kind === 'date') return Object.is(Date.prototype.getTime.call(target), snapshot.time);
    if (snapshot.kind === 'regexp') {
      return (
        Object.getOwnPropertyDescriptor(RegExp.prototype, 'source')!.get!.call(target) === snapshot.source &&
        Object.getOwnPropertyDescriptor(RegExp.prototype, 'flags')!.get!.call(target) === snapshot.flags
      );
    }

    const currentBuffer =
      target instanceof ArrayBuffer || isSharedArrayBuffer(target) ? target : readViewBuffer(target as ArrayBufferView);
    if (currentBuffer !== snapshot.buffer || readArrayBufferByteLength(currentBuffer) !== snapshot.byteLength) {
      return false;
    }
    const currentBytes = new Uint8Array(currentBuffer);
    return currentBytes.every((value, index) => value === snapshot.bytes[index]);
  } catch {
    return false;
  }
}

function restoreBuiltInState(target: object, snapshot: BuiltInStateSnapshot): boolean {
  if (matchesBuiltInState(target, snapshot)) return false;

  if (snapshot.kind === 'map') {
    Map.prototype.clear.call(target);
    for (const [key, value] of snapshot.entries) Map.prototype.set.call(target, key, value);
    return true;
  }
  if (snapshot.kind === 'set') {
    Set.prototype.clear.call(target);
    for (const value of snapshot.values) Set.prototype.add.call(target, value);
    return true;
  }
  if (snapshot.kind === 'date') {
    Date.prototype.setTime.call(target, snapshot.time);
    return true;
  }
  if (snapshot.kind === 'regexp') {
    if (!REGEXP_COMPILE) throw new Error('RegExp source or flags changed and cannot be restored');
    Reflect.apply(REGEXP_COMPILE, target, [snapshot.source, snapshot.flags]);
    return true;
  }

  const currentBuffer =
    target instanceof ArrayBuffer || isSharedArrayBuffer(target) ? target : readViewBuffer(target as ArrayBufferView);
  if (currentBuffer !== snapshot.buffer || readArrayBufferByteLength(currentBuffer) !== snapshot.byteLength) {
    throw new Error('binary backing buffer identity or length changed and cannot be restored');
  }
  new Uint8Array(currentBuffer).set(snapshot.bytes);
  return true;
}

function restoreObjectGraph(snapshot: ToolObjectSnapshot): boolean {
  let changed = false;
  // Restore internal slots first. In particular, RegExp.prototype.compile()
  // also resets lastIndex; restoring own descriptors afterwards returns the
  // complete RegExp state to its exact captured baseline. Doing this before
  // any fallible descriptor cleanup also prevents a failed restore from
  // leaving the registered object's internal state poisoned.
  if (snapshot.builtInState) changed = restoreBuiltInState(snapshot.target, snapshot.builtInState) || changed;
  if (Object.getPrototypeOf(snapshot.target) !== snapshot.prototype) {
    changed = true;
    if (!Reflect.setPrototypeOf(snapshot.target, snapshot.prototype)) {
      throw new Error('object prototype cannot be restored');
    }
  }
  const originalKeys = new Set(snapshot.descriptors.map(({ key }) => key));
  const trackedKeys = snapshot.trackedKeys ? new Set(snapshot.trackedKeys) : undefined;
  for (const key of Reflect.ownKeys(snapshot.target)) {
    if (trackedKeys && !trackedKeys.has(key)) continue;
    if (snapshot.builtInState?.kind === 'bytes' && isTypedArrayIndex(key)) continue;
    if (!originalKeys.has(key)) {
      changed = true;
      if (!Reflect.deleteProperty(snapshot.target, key)) {
        throw new Error(`added property "${String(key)}" is not configurable`);
      }
    }
  }
  for (const { key, descriptor } of snapshot.descriptors) {
    if (!sameDescriptor(Object.getOwnPropertyDescriptor(snapshot.target, key), descriptor)) changed = true;
    if (!Reflect.defineProperty(snapshot.target, key, descriptor)) {
      throw new Error(`property "${String(key)}" cannot be restored`);
    }
  }
  for (const child of snapshot.children) changed = restoreObjectGraph(child) || changed;
  for (const { key, value, child } of snapshot.accessorValues) {
    if (child) changed = restoreObjectGraph(child) || changed;
    let current: unknown;
    try {
      current = Reflect.get(snapshot.target, key);
    } catch (error) {
      throw new Error(`accessor-backed property "${String(key)}" cannot be read`, { cause: error });
    }
    if (current !== value && (!child || !matchesSnapshotView(current, child))) {
      throw new Error(`accessor-backed property "${String(key)}" returned a different value`);
    }
  }
  for (const prototypeSnapshot of snapshot.prototypeSnapshots ?? []) {
    changed = restorePrototypeSnapshot(prototypeSnapshot) || changed;
  }
  return changed;
}

function restorePrototypeSnapshot(snapshot: PrototypeSnapshot): boolean {
  let changed = false;
  if (Object.getPrototypeOf(snapshot.target) !== snapshot.prototype) {
    changed = true;
    if (!Reflect.setPrototypeOf(snapshot.target, snapshot.prototype)) {
      throw new Error('prototype chain cannot be restored');
    }
  }
  const originalKeys = new Set(snapshot.descriptors.map(({ key }) => key));
  const trackedKeys = snapshot.trackedKeys ? new Set(snapshot.trackedKeys) : undefined;
  for (const key of Reflect.ownKeys(snapshot.target)) {
    if (typeof key !== 'symbol' && trackedKeys && !trackedKeys.has(key)) continue;
    if (!originalKeys.has(key)) {
      changed = true;
      if (!Reflect.deleteProperty(snapshot.target, key)) {
        throw new Error(`added prototype property "${String(key)}" is not configurable`);
      }
    }
  }
  for (const { key, descriptor } of snapshot.descriptors) {
    if (!sameDescriptor(Object.getOwnPropertyDescriptor(snapshot.target, key), descriptor)) changed = true;
    if (!Reflect.defineProperty(snapshot.target, key, descriptor)) {
      throw new Error(`prototype property "${String(key)}" cannot be restored`);
    }
  }
  return changed;
}

function defineRecordValue(record: Record<string, unknown>, name: string, value: unknown): void {
  Object.defineProperty(record, name, {
    value,
    writable: true,
    enumerable: true,
    configurable: true,
  });
}

function immutableFence(tools: Record<string, unknown>, allowedNames: Iterable<string>): ToolSurfaceFence {
  const allowed = new Set(allowedNames);
  const originalTools: Record<string, unknown> = {};
  const originalToolDescriptors: Record<string, PropertyDescriptorMap> = {};
  const originalToolSnapshots: Record<string, ToolObjectSnapshot> = {};
  for (const name of allowed) {
    const descriptor = Object.getOwnPropertyDescriptor(tools, name);
    if (!descriptor || !('value' in descriptor) || descriptor.value === undefined) {
      throw new Error(
        `Cannot create replacement tool surface: allowed tool "${name}" has no own concrete implementation.`,
      );
    }
    const tool = descriptor.value;
    defineRecordValue(originalTools, name, tool);
    if (isObjectLike(tool)) {
      defineRecordValue(originalToolDescriptors, name, Object.freeze(Object.getOwnPropertyDescriptors(tool)));
      defineRecordValue(originalToolSnapshots, name, snapshotToolImplementation(tool));
    }
  }
  return Object.freeze({
    allowedNames: Object.freeze([...allowed]),
    originalTools: Object.freeze(originalTools),
    originalToolDescriptors: Object.freeze(originalToolDescriptors),
    originalToolSnapshots: Object.freeze(originalToolSnapshots),
  });
}

function restoreOriginalToolDescriptors(toolName: string, tool: unknown, fence: ToolSurfaceFence): boolean {
  const snapshot =
    fence.originalToolSnapshots && Object.hasOwn(fence.originalToolSnapshots, toolName)
      ? fence.originalToolSnapshots[toolName]
      : undefined;
  const descriptors = Object.hasOwn(fence.originalToolDescriptors, toolName)
    ? fence.originalToolDescriptors[toolName]
    : undefined;
  if ((!snapshot && !descriptors) || !isObjectLike(tool)) return false;

  let changed = false;
  try {
    if (snapshot) return restoreObjectGraph(snapshot);
    if (!descriptors) return false;
    for (const key of Object.getOwnPropertyNames(tool)) {
      if (descriptors[key] === undefined) {
        changed = true;
        if (!Reflect.deleteProperty(tool, key)) {
          throw new Error(`added property "${key}" is not configurable`);
        }
      }
    }
    for (const [key, descriptor] of Object.entries(descriptors)) {
      const current = Object.getOwnPropertyDescriptor(tool, key);
      if (!sameDescriptor(current, descriptor)) changed = true;
    }
    Object.defineProperties(tool, descriptors);
  } catch (error) {
    throw new Error(
      `Cannot restore replacement tool "${toolName}" after a processor mutated its executable descriptor. Refusing to execute the modified tool.`,
      { cause: error },
    );
  }
  return changed;
}

/** Restore retained replacement tool objects before a later same-process resume reassembles them. */
export function restoreToolSurfaceFenceImplementations(fence: ToolSurfaceFence): void {
  for (const [toolName, tool] of Object.entries(fence.originalTools)) {
    restoreOriginalToolDescriptors(toolName, tool, fence);
  }
}

/** Freeze the allowed names and implementations for one execution run. */
export function stampToolSurfaceFence(
  requestContext: RequestContext,
  runId: string | undefined,
  tools: Record<string, unknown>,
  ownerId?: string,
): ToolSurfaceFence {
  const registry = getOrCreateRegistry(toolSurfaceFenceRegistries, requestContext);
  const key = runKey(runId);
  if (registry.has(key)) {
    throw new Error(
      `Cannot replace the retained tool surface for active run ${runId ?? '<default>'}; refusing a concurrent execution until terminal cleanup or explicit resume reconstruction.`,
    );
  }
  if (registry.size >= MAX_RETAINED_FENCES_PER_CONTEXT) {
    throw new Error(
      `Cannot retain another replacement tool surface on this RequestContext: ${MAX_RETAINED_FENCES_PER_CONTEXT} active or suspended runs are already awaiting terminal cleanup.`,
    );
  }
  const fence = immutableFence(tools, Object.keys(tools));
  registry.set(key, { fence, ownerId, state: 'active' });
  return fence;
}

/** Transfer a suspended run's fence to exactly one resume execution. */
export function claimToolSurfaceFence(
  requestContext: RequestContext,
  runId: string | undefined,
  ownerId: string,
): ToolSurfaceFence | undefined {
  const registry = toolSurfaceFenceRegistries.get(requestContext);
  const key = runKey(runId);
  const registered = registry?.get(key);
  if (!registered) return undefined;
  if (registered.state !== 'suspended') {
    throw new Error(
      `Cannot resume replacement tool surface for run ${runId ?? '<unknown>'}: another execution still owns the active fence.`,
    );
  }
  registry!.set(key, { fence: registered.fence, ownerId, state: 'active' });
  return registered.fence;
}

/** Mark an owned fence as resumable only after the execution has durably suspended. */
export function suspendToolSurfaceFence(
  requestContext: RequestContext,
  runId: string | undefined,
  ownerId: string,
): void {
  const registry = toolSurfaceFenceRegistries.get(requestContext);
  const key = runKey(runId);
  const registered = registry?.get(key);
  if (!registered || registered.ownerId !== ownerId) return;
  registry!.set(key, { ...registered, state: 'suspended' });
}

/** Read the immutable tool ceiling for one execution run. */
export function readToolSurfaceFence(
  requestContext: RequestContext | undefined,
  runId?: string,
): ToolSurfaceFence | undefined {
  return requestContext ? toolSurfaceFenceRegistries.get(requestContext)?.get(runKey(runId))?.fence : undefined;
}

/** Clear one run's ceiling, optionally only while the caller still owns it. */
export function clearToolSurfaceFence(requestContext: RequestContext, runId?: string, ownerId?: string): boolean {
  const registry = toolSurfaceFenceRegistries.get(requestContext);
  const key = runKey(runId);
  const registered = registry?.get(key);
  if (!registered || (ownerId !== undefined && registered.ownerId !== ownerId)) return false;
  registry!.delete(key);
  if (registry?.size === 0) toolSurfaceFenceRegistries.delete(requestContext);
  return true;
}

/** Capture one exact suspended generation without exposing its execution owner. */
export function captureSuspendedToolSurfaceFenceLease(
  requestContext: RequestContext,
  runId?: string,
): SuspendedToolSurfaceFenceLease | undefined {
  const registered = toolSurfaceFenceRegistries.get(requestContext)?.get(runKey(runId));
  return registered?.state === 'suspended' ? (registered as unknown as SuspendedToolSurfaceFenceLease) : undefined;
}

/** Move one exact suspended ceiling when a public RequestContext is defensively snapshotted. */
export function transferSuspendedToolSurfaceFence(
  sourceContext: RequestContext,
  targetContext: RequestContext,
  runId: string | undefined,
  lease: SuspendedToolSurfaceFenceLease,
): boolean {
  const sourceRegistry = toolSurfaceFenceRegistries.get(sourceContext);
  const key = runKey(runId);
  const registered = sourceRegistry?.get(key);
  if (registered !== (lease as unknown as RegisteredToolSurfaceFence) || registered.state !== 'suspended') {
    return false;
  }
  if (sourceContext === targetContext) return true;

  const targetRegistry = getOrCreateRegistry(toolSurfaceFenceRegistries, targetContext);
  if (targetRegistry.has(key)) {
    throw new Error(
      `Cannot transfer replacement tool surface for run ${runId ?? '<unknown>'}: the snapshotted RequestContext already retains that run.`,
    );
  }
  if (targetRegistry.size >= MAX_RETAINED_FENCES_PER_CONTEXT) {
    throw new Error(
      `Cannot transfer replacement tool surface to the snapshotted RequestContext: ${MAX_RETAINED_FENCES_PER_CONTEXT} active or suspended runs are already retained.`,
    );
  }

  targetRegistry.set(key, registered);
  sourceRegistry!.delete(key);
  if (sourceRegistry!.size === 0) toolSurfaceFenceRegistries.delete(sourceContext);
  return true;
}

/** Clear a parked ceiling only if it is still the exact captured suspension. */
export function clearSuspendedToolSurfaceFence(
  requestContext: RequestContext,
  runId: string | undefined,
  lease: SuspendedToolSurfaceFenceLease,
): boolean {
  const registry = toolSurfaceFenceRegistries.get(requestContext);
  const key = runKey(runId);
  const registered = registry?.get(key);
  if (registered !== (lease as unknown as RegisteredToolSurfaceFence) || registered.state !== 'suspended') {
    return false;
  }
  registry!.delete(key);
  if (registry!.size === 0) toolSurfaceFenceRegistries.delete(requestContext);
  return true;
}

/** Stage a persisted name ceiling for the next replacement assembly of one run. */
export function stageToolSurfaceFenceRestore(
  requestContext: RequestContext,
  runId: string | undefined,
  allowedNames: Iterable<string>,
): void {
  getOrCreateRegistry(toolSurfaceRestoreRegistries, requestContext).set(
    runKey(runId),
    Object.freeze([...allowedNames]),
  );
}

/** Consume a staged persisted name ceiling exactly once for one run. */
export function consumeToolSurfaceFenceRestore(
  requestContext: RequestContext,
  runId?: string,
): readonly string[] | undefined {
  const registry = toolSurfaceRestoreRegistries.get(requestContext);
  const key = runKey(runId);
  const restored = registry?.get(key);
  registry?.delete(key);
  if (registry?.size === 0) toolSurfaceRestoreRegistries.delete(requestContext);
  return restored;
}

/** Build an identity-preserving fence, optionally capped to persisted names. */
export function createToolSurfaceFence(
  tools: Record<string, unknown>,
  allowedNames: Iterable<string> = Object.keys(tools),
): ToolSurfaceFence {
  return immutableFence(tools, allowedNames);
}

/**
 * Materialize a plain provider-facing record containing the original
 * implementation for every allowed name a processor kept. Processors may still
 * narrow the surface by removing an allowed name.
 */
export function enforceToolSurfaceFence(
  tools: Record<string, unknown>,
  fence: ToolSurfaceFence,
  logger?: { warn: (message: string) => void },
): Record<string, unknown> {
  const allowedNames = new Set(fence.allowedNames);
  const providerTools: Record<string, unknown> = {};
  const selectedTools: Array<{ toolName: string; currentTool: unknown }> = [];
  for (const toolName of Object.keys(tools)) {
    if (!allowedNames.has(toolName)) {
      logger?.warn(
        `[agent tool surface] Stripped tool "${toolName}": the execution uses a replacement toolset and processors cannot expand its model-visible tool surface.`,
      );
      continue;
    }
    let currentTool: unknown;
    try {
      currentTool = Reflect.get(tools, toolName);
    } catch (error) {
      throw new Error(
        `Cannot read processor-provided replacement tool "${toolName}" while materializing the provider tool surface.`,
        { cause: error },
      );
    }
    selectedTools.push({ toolName, currentTool });
  }
  for (const { toolName, currentTool } of selectedTools) {
    const originalTool = fence.originalTools[toolName];
    if (currentTool !== originalTool) {
      logger?.warn(
        `[agent tool surface] Restored tool "${toolName}": processors cannot replace implementations inside a replacement toolset.`,
      );
    }
    if (restoreOriginalToolDescriptors(toolName, originalTool, fence)) {
      logger?.warn(
        `[agent tool surface] Restored tool "${toolName}": processors cannot mutate implementations inside a replacement toolset.`,
      );
    }
    defineRecordValue(providerTools, toolName, originalTool);
  }
  return providerTools;
}

/**
 * Materialize only processor-selected tools when every selected implementation
 * is still the exact implementation captured by the fence.
 *
 * Durable execution uses this stricter variant because its tool-call step may
 * run on another worker and rebuild tools from the registered Agent. Arbitrary
 * processor-created implementations and closure decorators cannot be rebuilt
 * there. The check also restores any in-place mutation before it fails so a
 * retained registry tool can't poison a later step or retry.
 */
export function enforceReconstructibleToolSurface(
  tools: Record<string, unknown>,
  fence: ToolSurfaceFence,
  processorViewFence: ToolSurfaceFence = fence,
): Record<string, unknown> {
  let violation: { toolName: string; action: 'add' | 'replace' | 'mutate' } | undefined;
  const restoreFenceAndDetectMutation = (candidateFence: ToolSurfaceFence) => {
    for (const toolName of candidateFence.allowedNames) {
      const processorTool = candidateFence.originalTools[toolName];
      try {
        if (restoreOriginalToolDescriptors(toolName, processorTool, candidateFence) && !violation) {
          violation = { toolName, action: 'mutate' };
        }
      } catch {
        // The processor may have made its isolated view non-configurable or
        // non-extensible. That view is disposable; the registered executable
        // remains pristine and the durable attempt still fails closed.
        violation ??= { toolName, action: 'mutate' };
      }
    }
  };
  const restoreAndDetectMutation = () => {
    restoreFenceAndDetectMutation(processorViewFence);
    // A processor can retain a closure over the registered function instead of
    // mutating the isolated view it receives. Validate both graphs at every
    // untrusted trap boundary; shallow function-owned object identity catches
    // replacement without traversing invocation context internals.
    if (processorViewFence !== fence) restoreFenceAndDetectMutation(fence);
  };

  restoreAndDetectMutation();
  let surfaceKeys: readonly PropertyKey[];
  try {
    surfaceKeys = Reflect.ownKeys(tools);
  } catch {
    restoreAndDetectMutation();
    violation ??= { toolName: '<tool surface>', action: 'replace' };
    surfaceKeys = [];
  }
  // A Proxy ownKeys trap can mutate a retained executable before returning.
  restoreAndDetectMutation();

  const providerTools: Record<string, unknown> = {};
  for (const toolName of surfaceKeys) {
    if (violation) break;
    if (typeof toolName !== 'string') continue;

    let descriptor: PropertyDescriptor | undefined;
    try {
      descriptor = Reflect.getOwnPropertyDescriptor(tools, toolName);
    } catch {
      violation = { toolName, action: 'replace' };
    }
    // A stateful descriptor trap gets exactly one read and cannot leave a
    // mutation behind for this or a later tool.
    restoreAndDetectMutation();
    if (violation) break;
    if (!descriptor?.enumerable) continue;

    if (!Object.hasOwn(processorViewFence.originalTools, toolName)) {
      violation = { toolName, action: 'add' };
      break;
    }
    if (!('value' in descriptor) || descriptor.value !== processorViewFence.originalTools[toolName]) {
      violation = { toolName, action: 'replace' };
      break;
    }
    defineRecordValue(providerTools, toolName, fence.originalTools[toolName]);
  }

  // Catch a mutation performed by the final descriptor trap after its return.
  restoreAndDetectMutation();

  if (violation) {
    throw new Error(
      `Durable input processors cannot ${violation.action} executable tool "${violation.toolName}". Durable tool surfaces must remain reconstructible from the registered agent; remove tools to narrow the surface, or use beforeToolCall/afterToolCall hooks for around-call behavior.`,
    );
  }
  return providerTools;
}

/** Recreate the complete trusted tool map for a same-process resume. */
export function materializeToolSurfaceFence(fence: ToolSurfaceFence): Record<string, unknown> {
  return enforceToolSurfaceFence(fence.originalTools as Record<string, unknown>, fence);
}

/** Restrict active names to the immutable ceiling and the tools still visible after processing. */
export function enforceActiveToolsFence(
  activeTools: readonly string[] | undefined,
  fence: ToolSurfaceFence,
  visibleToolNames: Iterable<string> = fence.allowedNames,
): string[] {
  const allowedNames = new Set(fence.allowedNames);
  const visibleNames = new Set(visibleToolNames);
  // Always return a plain array. A processor may return an Array Proxy or
  // subclass whose iterator/species runs user code while the selection is
  // read; callers must perform this materialization before their final
  // executable-surface restore/check.
  return Array.from(activeTools ?? fence.allowedNames).filter(
    toolName => allowedNames.has(toolName) && visibleNames.has(toolName),
  );
}

/** Materialize a stable choice and fail when a forced tool is outside the visible ceiling. */
export function enforceToolChoiceFence(
  toolChoice: string | { type?: string; toolName?: string } | undefined,
  fence: ToolSurfaceFence,
  visibleToolNames: Iterable<string> = fence.allowedNames,
): string | { type?: string; toolName?: string } | undefined {
  if (!toolChoice || typeof toolChoice !== 'object') return toolChoice;

  let type: unknown;
  let toolName: unknown;
  try {
    type = Reflect.get(toolChoice, 'type');
    toolName = Reflect.get(toolChoice, 'toolName');
  } catch (error) {
    throw new Error('Cannot read processor-provided toolChoice while materializing the provider request.', {
      cause: error,
    });
  }
  const visibleNames = new Set(visibleToolNames);
  if (typeof toolName === 'string' && (!fence.allowedNames.includes(toolName) || !visibleNames.has(toolName))) {
    throw new Error(`Forced toolChoice names tool "${toolName}" outside the execution's replacement tool surface.`);
  }
  return {
    ...(type !== undefined ? { type: type as string } : {}),
    ...(toolName !== undefined ? { toolName: toolName as string } : {}),
  };
}
