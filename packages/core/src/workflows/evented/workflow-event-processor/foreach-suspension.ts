import { assertValidEventedForeachSuspensionResults, isEventedForeachSuspensionResult } from '../foreach-suspension';
import { createEventedResumeLabels, mergeEventedResumeLabels } from '../resume-label';
import type { EventedResumeLabels } from '../resume-label';

export { assertValidEventedForeachSuspensionResults, isEventedForeachSuspensionResult } from '../foreach-suspension';

export type EventedForeachSuspension = {
  firstSuspendedIndex: number;
  firstSuspendPayload: Record<string, any> | undefined;
  foreachOutput: Record<string, unknown>;
  resumeLabels: EventedResumeLabels;
};

function isRecord(value: unknown): value is Record<string, any> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

/**
 * Materializes the durable suspension state for an evented foreach step.
 *
 * The step-level payload intentionally mirrors the first suspended iteration
 * for backwards-compatible user suspend data. Resume routing, however, needs
 * every iteration's own payload so parallel suspensions cannot alias the first
 * sibling's nested run id or stream state.
 */
export function aggregateEventedForeachSuspensions(
  iterationResults: readonly unknown[],
): EventedForeachSuspension | undefined {
  assertValidEventedForeachSuspensionResults(iterationResults);

  let firstSuspendedIndex: number | undefined;
  let firstSuspendPayload: Record<string, any> | undefined;
  let resumeLabels = createEventedResumeLabels();
  const foreachOutput: Record<string, unknown> = {};

  for (let index = 0; index < iterationResults.length; index++) {
    const iterationResult = iterationResults[index];
    if (!isEventedForeachSuspensionResult(iterationResult)) continue;

    foreachOutput[String(index)] = iterationResult;
    const suspendPayload = iterationResult.suspendPayload;
    resumeLabels = mergeEventedResumeLabels(resumeLabels, suspendPayload?.__workflow_meta?.resumeLabels, target => ({
      ...target,
      foreachIndex: index,
    }));
    firstSuspendedIndex ??= index;
    firstSuspendPayload ??= suspendPayload;
  }

  if (firstSuspendedIndex === undefined) return undefined;
  return { firstSuspendedIndex, firstSuspendPayload, foreachOutput, resumeLabels };
}

/**
 * Rehydrates pruned output mirrors from the authoritative per-index resume map.
 * Terminal output and explicitly fresh re-suspensions win, so the old map can
 * neither revive a completed iteration nor replace a new payload or label.
 */
export function restoreEventedForeachSuspensionPayloads(
  iterationResults: readonly unknown[],
  foreachOutput: unknown,
  freshSuspensionIndices: readonly number[] = [],
): unknown[] {
  if (!isRecord(foreachOutput)) return [...iterationResults];
  assertValidEventedForeachSuspensionResults(Object.values(foreachOutput));
  const freshIndices = new Set(freshSuspensionIndices);

  return iterationResults.map((iterationResult, index) => {
    if (freshIndices.has(index)) return iterationResult;
    if (!isEventedForeachSuspensionResult(iterationResult)) return iterationResult;
    const authoritativeResult = foreachOutput[index];
    if (!isEventedForeachSuspensionResult(authoritativeResult)) return iterationResult;
    return { ...iterationResult, suspendPayload: authoritativeResult.suspendPayload };
  });
}

/** Removes duplicated stream state when a child suspension is copied to its parent snapshot. */
export function stripEventedForeachStreamStateForPropagation(
  foreachOutput: unknown,
): Record<string, unknown> | undefined {
  if (!isRecord(foreachOutput)) return undefined;

  const propagated: Record<string, unknown> = {};
  for (const [index, entry] of Object.entries(foreachOutput)) {
    if (!isEventedForeachSuspensionResult(entry)) continue;

    const { __streamState: _streamState, ...suspendPayload } = entry.suspendPayload;
    propagated[index] = { ...entry, suspendPayload };
  }
  return propagated;
}
