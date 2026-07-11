import type { RequestContext } from '../request-context';

const DEFAULT_RUN_KEY = '__default__';
const MAX_RETAINED_FENCES_PER_CONTEXT = 64;

export interface ToolSurfaceFence {
  readonly allowedNames: readonly string[];
  readonly originalTools: Readonly<Record<string, unknown>>;
  readonly originalToolDescriptors: Readonly<Record<string, PropertyDescriptorMap>>;
}

// Keep enforcement state outside RequestContext's public key/value bag. A
// processor can read and mutate that bag, so storing the registry there would
// let it widen its own replacement ceiling.
const toolSurfaceFenceRegistries = new WeakMap<RequestContext, Map<string, ToolSurfaceFence>>();
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

function immutableFence(tools: Record<string, unknown>, allowedNames: Iterable<string>): ToolSurfaceFence {
  const allowed = new Set(allowedNames);
  const originalTools = Object.fromEntries(Object.entries(tools).filter(([name]) => allowed.has(name)));
  return Object.freeze({
    allowedNames: Object.freeze([...allowed]),
    originalTools: Object.freeze(originalTools),
    originalToolDescriptors: Object.freeze(
      Object.fromEntries(
        Object.entries(originalTools).flatMap(([name, tool]) =>
          (typeof tool === 'object' && tool !== null) || typeof tool === 'function'
            ? [[name, Object.freeze(Object.getOwnPropertyDescriptors(tool))]]
            : [],
        ),
      ),
    ),
  });
}

function restoreOriginalToolDescriptors(toolName: string, tool: unknown, fence: ToolSurfaceFence): boolean {
  const descriptors = fence.originalToolDescriptors[toolName];
  if (!descriptors || ((typeof tool !== 'object' || tool === null) && typeof tool !== 'function')) return false;

  let changed = false;
  try {
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
      if (
        current?.value !== descriptor.value ||
        current?.get !== descriptor.get ||
        current?.set !== descriptor.set ||
        current?.writable !== descriptor.writable ||
        current?.enumerable !== descriptor.enumerable ||
        current?.configurable !== descriptor.configurable
      ) {
        changed = true;
      }
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
): ToolSurfaceFence {
  const fence = immutableFence(tools, Object.keys(tools));
  const registry = getOrCreateRegistry(toolSurfaceFenceRegistries, requestContext);
  const key = runKey(runId);
  if (!registry.has(key) && registry.size >= MAX_RETAINED_FENCES_PER_CONTEXT) {
    throw new Error(
      `Cannot retain another replacement tool surface on this RequestContext: ${MAX_RETAINED_FENCES_PER_CONTEXT} active or suspended runs are already awaiting terminal cleanup.`,
    );
  }
  registry.set(key, fence);
  return fence;
}

/** Read the immutable tool ceiling for one execution run. */
export function readToolSurfaceFence(
  requestContext: RequestContext | undefined,
  runId?: string,
): ToolSurfaceFence | undefined {
  return requestContext ? toolSurfaceFenceRegistries.get(requestContext)?.get(runKey(runId)) : undefined;
}

/** Clear only one execution run's prior ceiling when a RequestContext is reused. */
export function clearToolSurfaceFence(requestContext: RequestContext, runId?: string): void {
  const registry = toolSurfaceFenceRegistries.get(requestContext);
  registry?.delete(runKey(runId));
  if (registry?.size === 0) toolSurfaceFenceRegistries.delete(requestContext);
}

/** Stage a persisted name ceiling for the next replacement assembly of one run. */
export function stageToolSurfaceFenceRestore(
  requestContext: RequestContext,
  runId: string | undefined,
  allowedNames: Iterable<string>,
): void {
  getOrCreateRegistry(toolSurfaceRestoreRegistries, requestContext).set(runKey(runId), Object.freeze([...allowedNames]));
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
 * Remove names outside the ceiling and restore the original implementation for
 * every allowed name a processor kept. Processors may still narrow the surface
 * by removing an allowed name.
 */
export function enforceToolSurfaceFence(
  tools: Record<string, unknown>,
  fence: ToolSurfaceFence,
  logger?: { warn: (message: string) => void },
): void {
  const allowedNames = new Set(fence.allowedNames);
  for (const toolName of Object.keys(tools)) {
    if (!allowedNames.has(toolName)) {
      logger?.warn(
        `[agent tool surface] Stripped tool "${toolName}": the execution uses a replacement toolset and processors cannot expand its model-visible tool surface.`,
      );
      delete tools[toolName];
      continue;
    }
    const originalTool = fence.originalTools[toolName];
    if (originalTool !== undefined && tools[toolName] === originalTool) {
      if (restoreOriginalToolDescriptors(toolName, originalTool, fence)) {
        logger?.warn(
          `[agent tool surface] Restored tool "${toolName}": processors cannot mutate implementations inside a replacement toolset.`,
        );
      }
    } else if (originalTool !== undefined) {
      logger?.warn(
        `[agent tool surface] Restored tool "${toolName}": processors cannot replace implementations inside a replacement toolset.`,
      );
      tools[toolName] = originalTool;
    }
  }
}

/** Restrict active tool names to the same immutable execution ceiling. */
export function enforceActiveToolsFence(
  activeTools: readonly string[] | undefined,
  fence: ToolSurfaceFence,
): string[] {
  if (activeTools === undefined) return [...fence.allowedNames];
  const allowedNames = new Set(fence.allowedNames);
  return activeTools.filter(toolName => allowedNames.has(toolName));
}

/** Fail before provider invocation when a forced tool choice is outside the ceiling. */
export function enforceToolChoiceFence(
  toolChoice: string | { type?: string; toolName?: string } | undefined,
  fence: ToolSurfaceFence,
): void {
  if (
    toolChoice &&
    typeof toolChoice === 'object' &&
    toolChoice.type === 'tool' &&
    typeof toolChoice.toolName === 'string' &&
    !fence.allowedNames.includes(toolChoice.toolName)
  ) {
    throw new Error(
      `Forced toolChoice names tool "${toolChoice.toolName}" outside the execution's replacement tool surface.`,
    );
  }
}
