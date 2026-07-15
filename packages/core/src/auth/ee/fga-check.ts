import './telemetry';

import type { IFGAProvider } from '@internal/auth/ee';

export * from '@internal/auth/ee';

// Conversion provenance is deliberately tied to the final in-process tool
// object. Keeping it outside the AI SDK-compatible object shape prevents raw
// tools from forging an agent or MCP authorization identity. These helpers
// are fork-original (PF-545/546/1874) and stay core-side: they bind the tool
// objects produced by core's tool builder, not the shared auth internals.
interface BuiltToolFGAProvenance {
  resourceId: string;
  getFGAProvider: () => IFGAProvider | undefined;
}

const builtToolFGAProvenance = new WeakMap<object, BuiltToolFGAProvenance>();

/** @internal Bind the canonical FGA identity selected during tool conversion. */
export function bindBuiltToolFGAResourceId<T extends object>(
  tool: T,
  resourceId: string,
  getFGAProvider: () => IFGAProvider | undefined,
): T {
  builtToolFGAProvenance.set(tool, { resourceId, getFGAProvider });
  return tool;
}

/** @internal Read conversion provenance; unregistered tools fail closed to their standalone identity. */
export function getBuiltToolFGAResourceId(tool: unknown): string | undefined {
  return typeof tool === 'object' && tool !== null ? builtToolFGAProvenance.get(tool)?.resourceId : undefined;
}

/** @internal Whether this converted tool will enforce the same provider during execution. */
export function builtToolEnforcesFGAProvider(tool: unknown, fgaProvider: IFGAProvider): boolean {
  if (typeof tool !== 'object' || tool === null) return false;
  return builtToolFGAProvenance.get(tool)?.getFGAProvider() === fgaProvider;
}
