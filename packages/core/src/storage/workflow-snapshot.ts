import type { StepResult, WorkflowRunState } from '../workflows';

// NOTE: This merge logic is duplicated in stores/convex/src/server/workflow-snapshot.ts
// for the Convex server runtime. Keep both copies in sync.
const PENDING_MARKER_KEY = '__mastra_pending__';

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

function canResetWithPendingMarker(val: unknown): boolean {
  if (val == null || isPendingMarker(val)) {
    return true;
  }

  return isSuspendedStepResult(val);
}

export function createEmptyWorkflowSnapshot(runId: string): WorkflowRunState {
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
  } as WorkflowRunState;
}

/** @internal Preserves a valid logical workflow clock across a storage-clock final-state write. */
export function validateWorkflowSnapshotTimestampForFinalState(timestamp: unknown, now: number): number {
  if (!Number.isSafeInteger(timestamp) || (timestamp as number) < 0 || !Number.isSafeInteger(now) || now < 0) {
    throw new TypeError('Workflow snapshot and storage timestamps must be non-negative safe integers');
  }
  return Math.max(timestamp as number, now);
}

export function mergeWorkflowStepResult({
  snapshot,
  stepId,
  result,
  requestContext,
}: {
  snapshot: WorkflowRunState;
  stepId: string;
  result: StepResult<any, any, any, any>;
  requestContext: Record<string, any>;
}): Record<string, StepResult<any, any, any, any>> {
  if (!snapshot?.context) {
    throw new Error(`Snapshot context not found for runId ${snapshot?.runId}`);
  }

  const existingDescriptor = Object.getOwnPropertyDescriptor(snapshot.context, stepId);
  const existingResult =
    existingDescriptor?.enumerable && 'value' in existingDescriptor ? existingDescriptor.value : undefined;
  let nextResult: StepResult<any, any, any, any>;
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
          if (i >= existingOutput.length || canResetWithPendingMarker(existingOutput[i])) {
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
      ...(hasPendingMarker ? {} : (result as any)),
      output: mergedOutput,
    };
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
  try {
    return JSON.parse(JSON.stringify(snapshot.context));
  } catch {
    // Step results may contain non-serializable values (circular refs, functions, etc.)
    // when the workflow opts out of full persistence. Return a shallow copy so the
    // caller still gets a usable context without crashing.
    return { ...snapshot.context };
  }
}
