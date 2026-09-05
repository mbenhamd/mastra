// NOTE: This mirrors packages/core/src/storage/workflow-snapshot.ts. The Convex
// server runtime can't import @mastra/core, so keep both copies in sync.
const PENDING_MARKER_KEY = '__mastra_pending__';
const FOREACH_QUEUED_MARKER_KEY = '__mastra_foreach_queued__';

function isPendingMarker(val: unknown): boolean {
  return (
    val !== null &&
    typeof val === 'object' &&
    Object.prototype.hasOwnProperty.call(val, PENDING_MARKER_KEY) &&
    (val as Record<string, unknown>)[PENDING_MARKER_KEY] === true &&
    Object.keys(val).length === 1
  );
}

// Suspended forEach iteration results may come from multiple engines. Treat
// StepResult-shaped suspended entries as resettable without relying only on
// evented __workflow_meta, but avoid matching plain user outputs with only
// status/payload fields.
function isSuspendedStepResult(val: unknown): boolean {
  const result = val as Record<string, unknown> | null;

  return (
    val !== null &&
    typeof val === 'object' &&
    'status' in val &&
    result?.status === 'suspended' &&
    ('suspendPayload' in val || 'suspendedAt' in val)
  );
}

function canResetWithPendingMarker(val: unknown, iterationResult: unknown): boolean {
  if (val == null || isPendingMarker(val)) {
    return true;
  }

  if (
    isRecord(val) &&
    Object.hasOwn(val, FOREACH_QUEUED_MARKER_KEY) &&
    val[FOREACH_QUEUED_MARKER_KEY] === true &&
    Object.keys(val).length === 1
  ) {
    return true;
  }

  // Public outputs may themselves contain status: 'failed'. Only engine-owned
  // iteration progress can authorize resetting a failed coordinate.
  return isSuspendedStepResult(val) || (isRecord(iterationResult) && iterationResult.status === 'failed');
}

function isRecord(value: unknown): value is Record<string, any> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function mergeForeachProgress(
  existingResult: Record<string, any>,
  incomingResult: Record<string, any>,
  stepOutput: unknown[],
): Record<string, any> | undefined {
  const existingMeta = existingResult.suspendPayload?.__workflow_meta;
  const incomingMeta = incomingResult.suspendPayload?.__workflow_meta;
  const existingOutput = existingMeta?.foreachOutput;
  const incomingOutput = incomingMeta?.foreachOutput;
  if (
    (!Array.isArray(existingOutput) && !isRecord(existingOutput)) ||
    (!Array.isArray(incomingOutput) && !isRecord(incomingOutput))
  ) {
    return undefined;
  }

  const mergedOutput: Record<string, any> = Array.isArray(existingOutput)
    ? existingOutput.slice()
    : { ...existingOutput };
  if (Array.isArray(existingOutput) && Array.isArray(incomingOutput)) {
    mergedOutput.length = Math.max(mergedOutput.length, incomingOutput.length);
  }
  for (const [index, incoming] of Object.entries(incomingOutput)) {
    // Sparse arrays acquire null placeholders when serialized by a store.
    if (incoming == null) continue;
    const existing = mergedOutput[index];
    const isRetrySuspension =
      existing?.status === 'failed' &&
      existingResult.output[index] === null &&
      isSuspendedStepResult(incomingResult.output[index]);
    // A copied suspension from a stale sibling write must not replace a
    // terminal coordinate. An admitted failed retry can explicitly suspend again.
    if (isSuspendedStepResult(incoming) && existing && !isSuspendedStepResult(existing) && !isRetrySuspension) continue;
    mergedOutput[index] = incoming;
  }
  for (const [index, iteration] of Object.entries(mergedOutput)) {
    // Propagated maps contain only the remaining suspensions. A completed
    // public output retires its old suspension; pending/queued outputs do not.
    if (isSuspendedStepResult(iteration) && !canResetWithPendingMarker(stepOutput[Number(index)], undefined)) {
      delete mergedOutput[index];
    }
  }
  return {
    ...existingResult.suspendPayload,
    ...incomingResult.suspendPayload,
    __workflow_meta: {
      ...existingMeta,
      ...incomingMeta,
      foreachOutput: mergedOutput,
    },
  };
}

export function createEmptyWorkflowSnapshot(runId: string): Record<string, any> {
  return {
    context: {},
    activePaths: [],
    activeStepsPath: {},
    timestamp: Date.now(),
    suspendedPaths: {},
    resumeLabels: {},
    serializedStepGraph: [],
    value: {},
    waitingPaths: {},
    status: 'pending',
    runId,
  };
}

export function mergeWorkflowStepResult({
  snapshot,
  stepId,
  result,
  requestContext,
}: {
  snapshot: Record<string, any>;
  stepId: string;
  result: Record<string, any>;
  requestContext: Record<string, any>;
}): Record<string, any> {
  if (!snapshot?.context) {
    throw new Error(`Snapshot context not found for runId ${snapshot?.runId}`);
  }

  const existingDescriptor = Object.getOwnPropertyDescriptor(snapshot.context, stepId);
  const existingResult =
    existingDescriptor?.enumerable && 'value' in existingDescriptor ? existingDescriptor.value : undefined;
  let nextResult: Record<string, any>;
  if (
    existingResult &&
    typeof existingResult === 'object' &&
    'output' in existingResult &&
    Array.isArray(existingResult.output) &&
    result &&
    typeof result === 'object' &&
    'output' in result &&
    Array.isArray(result.output)
  ) {
    const existingOutput = existingResult.output as unknown[];
    const newOutput = result.output as unknown[];
    const mergedOutput = [...existingOutput];
    const hasPendingMarker = newOutput.some(isPendingMarker);
    for (let i = 0; i < Math.max(existingOutput.length, newOutput.length); i++) {
      if (i < newOutput.length) {
        const newVal = newOutput[i];
        if (isPendingMarker(newVal)) {
          if (
            i >= existingOutput.length ||
            canResetWithPendingMarker(
              existingOutput[i],
              existingResult.suspendPayload?.__workflow_meta?.foreachOutput?.[i],
            )
          ) {
            mergedOutput[i] = null;
          }
        } else if (newVal !== null && newVal !== undefined && !hasPendingMarker) {
          mergedOutput[i] = newVal;
        } else if (i >= existingOutput.length) {
          mergedOutput[i] = null;
        }
      }
    }
    nextResult = {
      ...existingResult,
      // Pending-marker writes are reset commands built from an earlier snapshot,
      // so keep existing step-level fields and ignore sibling values they carry.
      ...(hasPendingMarker ? {} : result),
      output: mergedOutput,
    };
    if (!hasPendingMarker) {
      const mergedSuspendPayload = mergeForeachProgress(existingResult, result, mergedOutput);
      if (mergedSuspendPayload) nextResult.suspendPayload = mergedSuspendPayload;
    }
  } else {
    nextResult = result;
  }
  Object.defineProperty(snapshot.context, stepId, {
    configurable: true,
    enumerable: true,
    writable: true,
    value: nextResult,
  });

  snapshot.requestContext = { ...snapshot.requestContext, ...requestContext };
  return JSON.parse(JSON.stringify(snapshot.context));
}
