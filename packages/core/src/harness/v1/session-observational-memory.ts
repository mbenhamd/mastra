/**
 * Session Observational Memory surface (HARNESS_V1_SPEC.md §4.2e / §9.2).
 *
 * OM is advisory memory context, not a recovery proof boundary. Native per-turn
 * engine selection is not available yet, so enabled Harness OM configuration is
 * rejected at Harness construction and model switches reject before persisting
 * anything. `clearOverride()` remains as a recovery surface for sessions written
 * by an older build that recorded now-unsupported intent.
 *
 * It reaches the session only through `ObservationalMemorySessionPort`, which
 * `Session` builds in `_observationalMemoryPort()`.
 */

import type { SessionRecord } from '../../storage/domains/harness';
import { HarnessConfigError, HarnessValidationError } from './errors';
import type { Harness } from './harness';

/**
 * The slice of `Session` the Observational Memory operations need. `record` is
 * a getter so the operations always see the current record.
 */
export interface ObservationalMemorySessionPort {
  readonly record: SessionRecord;
  readonly harness: Harness;
  assertLive(method: string): void;
  flushUpdate(update: (prev: SessionRecord) => SessionRecord): Promise<void>;
  maybeDrainQueue(): Promise<void>;
}

function omDefaults(port: ObservationalMemorySessionPort) {
  return port.harness._getObservationalMemoryDefaults();
}

export function omObserverModelId(port: ObservationalMemorySessionPort): string | null {
  port.assertLive('om.getObserverModelId()');
  return port.record.observationalMemory?.observerModelId ?? omDefaults(port).observerModelId ?? null;
}

export function omReflectorModelId(port: ObservationalMemorySessionPort): string | null {
  port.assertLive('om.getReflectorModelId()');
  return port.record.observationalMemory?.reflectorModelId ?? omDefaults(port).reflectorModelId ?? null;
}

/** Effective observation trigger threshold. `0` means "use the memory adapter default". */
export function omObservationThreshold(port: ObservationalMemorySessionPort): number {
  port.assertLive('om.getObservationThreshold()');
  return port.record.observationalMemory?.observationThreshold ?? omDefaults(port).observationThreshold ?? 0;
}

/** Effective reflection trigger threshold. `0` means "use the memory adapter default". */
export function omReflectionThreshold(port: ObservationalMemorySessionPort): number {
  port.assertLive('om.getReflectionThreshold()');
  return port.record.observationalMemory?.reflectionThreshold ?? omDefaults(port).reflectionThreshold ?? 0;
}

export async function omSwitchObserverModel(
  port: ObservationalMemorySessionPort,
  opts: { model: string },
): Promise<void> {
  port.assertLive('om.switchObserverModel()');
  if (typeof opts?.model !== 'string' || opts.model.length === 0) {
    throw new HarnessValidationError('om.switchObserverModel.model', 'must be a non-empty string');
  }
  throw new HarnessConfigError(
    'observationalMemory',
    'per-session model switches are not supported until @mastra/memory can select an OM engine from the effective per-turn config',
  );
}

export async function omSwitchReflectorModel(
  port: ObservationalMemorySessionPort,
  opts: { model: string },
): Promise<void> {
  port.assertLive('om.switchReflectorModel()');
  if (typeof opts?.model !== 'string' || opts.model.length === 0) {
    throw new HarnessValidationError('om.switchReflectorModel.model', 'must be a non-empty string');
  }
  throw new HarnessConfigError(
    'observationalMemory',
    'per-session model switches are not supported until @mastra/memory can select an OM engine from the effective per-turn config',
  );
}

/** Remove unsupported intent persisted by an older Harness build. */
export async function omClearOverride(port: ObservationalMemorySessionPort): Promise<void> {
  port.assertLive('om.clearOverride()');
  await port.flushUpdate(prev => {
    if (prev.observationalMemory === undefined) return prev;
    const next = { ...prev };
    delete next.observationalMemory;
    return next;
  });
  // Recovered queue work parks while a legacy override is present. Wake it
  // only after the clearing CAS commits; a failed write leaves the durable
  // poison authoritative. This mirrors the other fire-and-forget queue kicks
  // and is idempotent when another drain already owns the session.
  void port.maybeDrainQueue();
}

export function hasUnsupportedObservationalMemoryIntent(port: ObservationalMemorySessionPort): boolean {
  return port.harness._getObservationalMemoryDefaults().enabled || port.record.observationalMemory !== undefined;
}
