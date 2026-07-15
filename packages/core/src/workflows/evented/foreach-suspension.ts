function isRecord(value: unknown): value is Record<string, any> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function isEventedForeachSuspensionCandidate(value: unknown): value is Record<string, any> {
  return (
    isRecord(value) &&
    value.status === 'suspended' &&
    (Object.hasOwn(value, 'suspendedAt') || Object.hasOwn(value, 'suspendPayload'))
  );
}

/** Distinguishes engine StepResults from successful user outputs with a status field. */
export function isEventedForeachSuspensionResult(
  value: unknown,
): value is { status: 'suspended'; suspendedAt: number; suspendPayload: Record<string, any> } {
  if (!isRecord(value) || value.status !== 'suspended' || !Number.isFinite(value.suspendedAt)) return false;
  if (!isRecord(value.suspendPayload) || !isRecord(value.suspendPayload.__workflow_meta)) return false;
  const path = value.suspendPayload.__workflow_meta.path;
  return (
    Array.isArray(path) && path.length > 0 && path.every(segment => typeof segment === 'string' && segment.length > 0)
  );
}

/** Fails closed for corrupted engine envelopes without reserving `status` in user outputs. */
export function assertValidEventedForeachSuspensionResults(iterationResults: readonly unknown[]): void {
  for (const [index, iterationResult] of iterationResults.entries()) {
    if (isEventedForeachSuspensionCandidate(iterationResult) && !isEventedForeachSuspensionResult(iterationResult)) {
      throw new Error(`Invalid evented foreach suspension state at index ${index}`);
    }
  }
}
