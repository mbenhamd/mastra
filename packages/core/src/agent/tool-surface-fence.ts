import type { RequestContext } from '../request-context';

const DEFAULT_RUN_KEY = '__default__';
const MAX_RETAINED_FENCES_PER_CONTEXT = 64;
const MAX_SNAPSHOTTED_TOOL_OBJECTS = 10_000;
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
const INTRINSIC_PROTOTYPES = new Set<object>([
  Object.prototype,
  Function.prototype,
  Array.prototype,
  Map.prototype,
  Set.prototype,
  WeakMap.prototype,
  WeakSet.prototype,
  Date.prototype,
  RegExp.prototype,
  Promise.prototype,
  Error.prototype,
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
  readonly trackedKeys?: readonly PropertyKey[];
  readonly prototypeSnapshots?: readonly PrototypeSnapshot[];
}

export interface ToolSurfaceFence {
  readonly allowedNames: readonly string[];
  readonly originalTools: Readonly<Record<string, unknown>>;
  readonly originalToolDescriptors: Readonly<Record<string, PropertyDescriptorMap>>;
  readonly originalToolSnapshots?: Readonly<Record<string, ToolObjectSnapshot>>;
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
  snapshotPrototypeChain(target, state, trackedKeys);
  const trackedKeySet = trackedKeys ? new Set(trackedKeys) : undefined;
  const descriptors = Reflect.ownKeys(target)
    .filter(key => !trackedKeySet || trackedKeySet.has(key))
    .map(key => {
      const descriptor = Object.getOwnPropertyDescriptor(target, key);
      if (!descriptor) throw new Error(`Replacement tool property ${String(key)} disappeared while being snapshotted.`);
      return Object.freeze({ key, descriptor: Object.freeze({ ...descriptor }) });
    });
  const children = descriptors.flatMap(({ key, descriptor }) => {
    if (!shouldSnapshotNestedDefinition(target, key, deepKeys)) return [];
    if (!('value' in descriptor) || !isObjectLike(descriptor.value)) return [];
    if (typeof descriptor.value === 'function') return [];
    const child = snapshotObjectGraph(descriptor.value, state);
    return child ? [child] : [];
  });
  const accessorValues = descriptors.flatMap(({ key, descriptor }) => {
    if (!shouldSnapshotNestedDefinition(target, key, deepKeys)) return [];
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
    ...(trackedKeys ? { trackedKeys: Object.freeze([...trackedKeys]) } : {}),
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
  return Object.freeze({ ...snapshot, prototypeSnapshots: freezePrototypeSnapshots(state) });
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
    if (!expectedKeys.has(key)) return false;
  }
  return snapshot.descriptors.every(({ key, descriptor }) =>
    sameViewDescriptor(Object.getOwnPropertyDescriptor(current, key), descriptor),
  );
}

function restoreObjectGraph(snapshot: ToolObjectSnapshot): boolean {
  let changed = false;
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
  return (activeTools ?? fence.allowedNames).filter(
    toolName => allowedNames.has(toolName) && visibleNames.has(toolName),
  );
}

/** Fail before provider invocation when a forced tool choice is outside the ceiling. */
export function enforceToolChoiceFence(
  toolChoice: string | { type?: string; toolName?: string } | undefined,
  fence: ToolSurfaceFence,
): void {
  if (
    toolChoice &&
    typeof toolChoice === 'object' &&
    typeof toolChoice.toolName === 'string' &&
    !fence.allowedNames.includes(toolChoice.toolName)
  ) {
    throw new Error(
      `Forced toolChoice names tool "${toolChoice.toolName}" outside the execution's replacement tool surface.`,
    );
  }
}
