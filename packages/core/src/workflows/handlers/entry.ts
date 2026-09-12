import type { ActorSignal } from '../../auth/ee';
import type { RequestContext } from '../../di';
import { getErrorFromUnknown } from '../../error';
import type { SerializedError } from '../../error';
import type { PubSub } from '../../events/pubsub';
import { resolveObservabilityContext } from '../../observability';
import type { ObservabilityContext } from '../../observability';
import { WORKFLOW_LIFECYCLE_OUTBOX_LIMIT } from '../../storage/domains/workflows/resume';
import type { PersistWorkflowStepUpdateResult } from '../../storage/types';
import type { DefaultExecutionEngine } from '../default';
import { requireWorkflowExecutionGeneration, workflowLifecycleEventsAreSuppressed } from '../lifecycle-events';
import type { WorkflowLifecycleEvent } from '../lifecycle-events';
import type {
  EntryExecutionResult,
  ExecutionContext,
  OutputWriter,
  RestartExecutionParams,
  SerializedStepFlowEntry,
  StepFailure,
  StepFlowEntry,
  StepResult,
  TimeTravelExecutionParams,
  WorkflowRunStatus,
  WorkflowRunState,
} from '../types';
import { getSingleStepEntryId, isSingleStepEntry } from '../utils';

function publishStepEvent(
  engine: DefaultExecutionEngine,
  pubsub: PubSub,
  ...args: Parameters<PubSub['publish']>
): Promise<void> {
  return engine.options.emitStepEvents === false ? Promise.resolve() : pubsub.publish(...args);
}

type LifecyclePayloadKey = 'output' | 'suspendPayload' | 'error';

type LifecyclePayloadBaseline = {
  type: WorkflowLifecycleEvent['type'];
  stepId: string;
  stepCallId: string;
  stepAttempt: number;
  payloadKey: LifecyclePayloadKey;
  eventHasPayload: boolean;
  eventPayload: unknown;
  contextHasPayload: boolean;
  contextPayload: unknown;
};

function cloneLifecyclePayload(value: unknown, seen: WeakMap<object, object> = new WeakMap()): unknown {
  if (value === null || typeof value !== 'object') return value;

  const prior = seen.get(value);
  if (prior) return prior;

  const retainClone = (clone: object) => {
    seen.set(value, clone);
    return clone;
  };
  if (value instanceof Date) return retainClone(new Date(value.getTime()));
  if (value instanceof RegExp) return retainClone(new RegExp(value.source, value.flags));
  if (value instanceof URL) return retainClone(new URL(value.href));
  if (value instanceof ArrayBuffer) return retainClone(value.slice(0));
  if (ArrayBuffer.isView(value)) {
    const buffer = new ArrayBuffer(value.byteLength);
    new Uint8Array(buffer).set(new Uint8Array(value.buffer, value.byteOffset, value.byteLength));
    if (value instanceof DataView) return retainClone(new DataView(buffer));
    const View = value.constructor as new (buffer: ArrayBuffer) => ArrayBufferView;
    return retainClone(new View(buffer));
  }
  if (value instanceof Map) {
    const clone = new Map();
    seen.set(value, clone);
    for (const [key, entryValue] of value) {
      clone.set(cloneLifecyclePayload(key, seen), cloneLifecyclePayload(entryValue, seen));
    }
    return clone;
  }
  if (value instanceof Set) {
    const clone = new Set();
    seen.set(value, clone);
    for (const entryValue of value) clone.add(cloneLifecyclePayload(entryValue, seen));
    return clone;
  }
  if (value instanceof Error) {
    const clone = new Error() as Error & Record<string, unknown>;
    delete clone.stack;
    Object.setPrototypeOf(clone, Object.getPrototypeOf(value));
    seen.set(value, clone);
    for (const key of Object.getOwnPropertyNames(value)) {
      const descriptor = Object.getOwnPropertyDescriptor(value, key)!;
      Object.defineProperty(clone, key, {
        configurable: descriptor.configurable,
        enumerable: descriptor.enumerable,
        writable: true,
        value: cloneLifecyclePayload(Reflect.get(value, key), seen),
      });
    }
    return clone;
  }
  if (Array.isArray(value)) {
    const clone: unknown[] = [];
    seen.set(value, clone);
    for (const entryValue of value) clone.push(cloneLifecyclePayload(entryValue, seen));
    return clone;
  }

  const clone = Object.create(Object.getPrototypeOf(value)) as Record<string, unknown>;
  seen.set(value, clone);
  for (const key of Object.keys(value)) {
    clone[key] = cloneLifecyclePayload((value as Record<string, unknown>)[key], seen);
  }
  return clone;
}

function lifecyclePayloadKey(event: WorkflowLifecycleEvent): LifecyclePayloadKey | undefined {
  if (event.type === 'step.completed') return 'output';
  if (event.type === 'step.suspended') return 'suspendPayload';
  if (event.type === 'step.failed') return 'error';
  return undefined;
}

function captureLifecyclePayloadBaseline(snapshot: WorkflowRunState): LifecyclePayloadBaseline[] {
  return (snapshot.lifecycleOutbox ?? []).flatMap(event => {
    if (!('stepId' in event)) return [];
    const payloadKey = lifecyclePayloadKey(event);
    if (!payloadKey) return [];
    const eventRecord = event as unknown as Record<string, unknown>;
    const stepResult = snapshot.context?.[event.stepId] as Record<string, unknown> | undefined;
    return [
      {
        type: event.type,
        stepId: event.stepId,
        stepCallId: event.stepCallId,
        stepAttempt: event.stepAttempt,
        payloadKey,
        eventHasPayload: Object.hasOwn(event, payloadKey),
        eventPayload: cloneLifecyclePayload(eventRecord[payloadKey]),
        contextHasPayload: stepResult ? Object.hasOwn(stepResult, payloadKey) : false,
        contextPayload: cloneLifecyclePayload(stepResult?.[payloadKey]),
      },
    ];
  });
}

function lifecyclePayloadEquals(left: unknown, right: unknown, seen: WeakMap<object, object> = new WeakMap()): boolean {
  if (Object.is(left, right)) return true;
  if (left === null || right === null || typeof left !== 'object' || typeof right !== 'object') return false;

  const prior = seen.get(left);
  if (prior) return prior === right;
  seen.set(left, right);

  if (left instanceof Date || right instanceof Date) {
    return left instanceof Date && right instanceof Date && left.getTime() === right.getTime();
  }
  if (left instanceof RegExp || right instanceof RegExp) {
    return (
      left instanceof RegExp && right instanceof RegExp && left.source === right.source && left.flags === right.flags
    );
  }
  if (left instanceof URL || right instanceof URL) {
    return left instanceof URL && right instanceof URL && left.href === right.href;
  }
  if (left instanceof Map || right instanceof Map) {
    if (!(left instanceof Map) || !(right instanceof Map) || left.size !== right.size) return false;
    const leftEntries = [...left.entries()];
    const rightEntries = [...right.entries()];
    return leftEntries.every(
      ([key, value], index) =>
        lifecyclePayloadEquals(key, rightEntries[index]?.[0], seen) &&
        lifecyclePayloadEquals(value, rightEntries[index]?.[1], seen),
    );
  }
  if (left instanceof Set || right instanceof Set) {
    if (!(left instanceof Set) || !(right instanceof Set) || left.size !== right.size) return false;
    const rightValues = [...right.values()];
    return [...left.values()].every((value, index) => lifecyclePayloadEquals(value, rightValues[index], seen));
  }
  if (left instanceof ArrayBuffer || right instanceof ArrayBuffer) {
    if (!(left instanceof ArrayBuffer) || !(right instanceof ArrayBuffer) || left.byteLength !== right.byteLength) {
      return false;
    }
    return new Uint8Array(left).every((value, index) => value === new Uint8Array(right)[index]);
  }
  if (ArrayBuffer.isView(left) || ArrayBuffer.isView(right)) {
    if (!ArrayBuffer.isView(left) || !ArrayBuffer.isView(right) || left.byteLength !== right.byteLength) return false;
    const leftBytes = new Uint8Array(left.buffer, left.byteOffset, left.byteLength);
    const rightBytes = new Uint8Array(right.buffer, right.byteOffset, right.byteLength);
    return leftBytes.every((value, index) => value === rightBytes[index]);
  }
  if (left instanceof Error || right instanceof Error) {
    if (!(left instanceof Error) || !(right instanceof Error)) return false;
    if (left.name !== right.name || left.message !== right.message || left.stack !== right.stack) return false;
    if (Object.hasOwn(left, 'cause') !== Object.hasOwn(right, 'cause')) return false;
    if (!lifecyclePayloadEquals(left.cause, right.cause, seen)) return false;
  }
  if (Array.isArray(left) || Array.isArray(right)) {
    return (
      Array.isArray(left) &&
      Array.isArray(right) &&
      left.length === right.length &&
      left.every((value, index) => lifecyclePayloadEquals(value, right[index], seen))
    );
  }

  const leftRecord = left as Record<string, unknown>;
  const rightRecord = right as Record<string, unknown>;
  const leftPrototype = Object.getPrototypeOf(left);
  const rightPrototype = Object.getPrototypeOf(right);
  const supportedRecordPrototype = (prototype: object | null) => prototype === null || prototype === Object.prototype;
  if (
    leftPrototype !== rightPrototype &&
    (!supportedRecordPrototype(leftPrototype) || !supportedRecordPrototype(rightPrototype))
  ) {
    return false;
  }
  const leftKeys = Object.keys(leftRecord);
  const rightKeys = Object.keys(rightRecord);
  return (
    leftKeys.length === rightKeys.length &&
    leftKeys.every(
      key => Object.hasOwn(rightRecord, key) && lifecyclePayloadEquals(leftRecord[key], rightRecord[key], seen),
    )
  );
}

function getPrunedLifecycleEvents(
  snapshot: WorkflowRunState,
  baseline: LifecyclePayloadBaseline[],
): WorkflowLifecycleEvent[] | undefined {
  if (!snapshot.lifecycleOutbox?.length) return undefined;
  return snapshot.lifecycleOutbox.map(event => {
    if (!('stepId' in event)) return event;
    const stepResult = snapshot.context?.[event.stepId] as Record<string, unknown> | undefined;
    const prunedPayload = (key: LifecyclePayloadKey) => {
      const original = baseline.find(
        candidate =>
          candidate.type === event.type &&
          candidate.stepId === event.stepId &&
          candidate.stepCallId === event.stepCallId &&
          candidate.stepAttempt === event.stepAttempt &&
          candidate.payloadKey === key,
      );
      const eventRecord = event as unknown as Record<string, unknown>;
      const eventHasPayload = Object.hasOwn(event, key);
      const contextHasPayload = stepResult ? Object.hasOwn(stepResult, key) : false;
      if (!original) return eventHasPayload ? eventRecord[key] : undefined;
      const eventWasExplicitlyPruned =
        eventHasPayload !== original.eventHasPayload ||
        !lifecyclePayloadEquals(eventRecord[key], original.eventPayload);
      const contextWasExplicitlyPruned =
        contextHasPayload !== original.contextHasPayload ||
        !lifecyclePayloadEquals(stepResult?.[key], original.contextPayload);
      if (contextWasExplicitlyPruned) {
        // A copied/serialized outbox can change representation without being
        // redacted. It must never restore data removed from the step context.
        // If both projections were changed differently, omit the payload:
        // neither projection is evidence that the other's removed data is safe.
        if (eventWasExplicitlyPruned && !lifecyclePayloadEquals(eventRecord[key], stepResult?.[key])) return undefined;
        return contextHasPayload ? stepResult?.[key] : undefined;
      }
      if (eventWasExplicitlyPruned) return eventHasPayload ? eventRecord[key] : undefined;
      return eventHasPayload ? eventRecord[key] : undefined;
    };
    if (event.type === 'step.completed') {
      const { output: _output, ...identity } = event;
      return { ...identity, output: prunedPayload('output') };
    }
    if (event.type === 'step.suspended') {
      const { suspendPayload: _suspendPayload, ...identity } = event;
      return { ...identity, suspendPayload: prunedPayload('suspendPayload') };
    }
    if (event.type === 'step.failed') {
      const { error: _error, ...identity } = event;
      const prunedError = prunedPayload('error');
      const error =
        prunedError instanceof Error
          ? getErrorFromUnknown(prunedError, { serializeStack: false }).toJSON()
          : prunedError;
      return { ...identity, error };
    }
    return event;
  });
}

/**
 * After resuming a single step within a parallel or conditional block, check whether
 * all relevant branch steps are now complete and build the appropriate block-level result.
 *
 * For parallel blocks every step must complete; for conditional blocks only the steps
 * that were actually executed (have entries in stepResults) are considered.
 */
function buildResumedBlockResult(
  entrySteps: StepFlowEntry[],
  stepResults: Record<string, StepResult<any, any, any, any>>,
  executionContext: ExecutionContext,
  opts?: { onlyExecutedSteps?: boolean },
): any {
  const stepsToCheck = opts?.onlyExecutedSteps
    ? entrySteps.filter(s => isSingleStepEntry(s) && stepResults[getSingleStepEntryId(s)] !== undefined)
    : entrySteps;

  const allComplete = stepsToCheck.every(s => {
    if (isSingleStepEntry(s)) {
      const r = stepResults[getSingleStepEntryId(s)];
      return r && r.status === 'success';
    }
    return true;
  });

  let result: any;
  if (allComplete) {
    result = {
      status: 'success',
      output: entrySteps.reduce((acc: Record<string, any>, s) => {
        if (isSingleStepEntry(s)) {
          const id = getSingleStepEntryId(s);
          const r = stepResults[id];
          if (r && r.status === 'success') {
            acc[id] = r.output;
          }
        }
        return acc;
      }, {}),
    };
  } else {
    // Check for failed steps before assuming suspended
    const failedStep = stepsToCheck.find(
      s => isSingleStepEntry(s) && stepResults[getSingleStepEntryId(s)]?.status === 'failed',
    );
    if (failedStep && isSingleStepEntry(failedStep)) {
      const failedResult = stepResults[getSingleStepEntryId(failedStep)] as StepFailure<any, any, any, any> | undefined;
      result = {
        status: 'failed',
        error: failedResult?.error ?? new Error('Workflow step failed after resume'),
        tripwire: failedResult?.tripwire,
      };
    } else {
      const stillSuspended = entrySteps.find(
        s => isSingleStepEntry(s) && stepResults[getSingleStepEntryId(s)]?.status === 'suspended',
      );
      const suspendData =
        stillSuspended && isSingleStepEntry(stillSuspended)
          ? stepResults[getSingleStepEntryId(stillSuspended)]?.suspendPayload
          : {};
      result = {
        status: 'suspended',
        payload: suspendData,
        suspendPayload: suspendData,
        suspendedAt: Date.now(),
      };
    }
  }

  if (result.status === 'suspended') {
    entrySteps.forEach((s, stepIndex) => {
      if (isSingleStepEntry(s) && stepResults[getSingleStepEntryId(s)]?.status === 'suspended') {
        executionContext.suspendedPaths[getSingleStepEntryId(s)] = [...executionContext.executionPath, stepIndex];
      }
    });
  }

  return result;
}

function getResumeStepPrevOutput({
  isResumedStep,
  stepId,
  stepResults,
  prevOutput,
}: {
  isResumedStep: boolean;
  stepId: string;
  stepResults: Record<string, StepResult<any, any, any, any>>;
  prevOutput: any;
}) {
  if (!isResumedStep) {
    return prevOutput;
  }

  const stepResult = stepResults[stepId];
  return stepResult && Object.prototype.hasOwnProperty.call(stepResult, 'payload') ? stepResult.payload : prevOutput;
}

export interface PersistStepUpdateParams {
  workflowId: string;
  runId: string;
  resourceId?: string;
  stepResults: Record<string, StepResult<any, any, any, any>>;
  serializedStepGraph: SerializedStepFlowEntry[];
  executionContext: ExecutionContext;
  workflowStatus: WorkflowRunStatus;
  result?: Record<string, any>;
  error?: SerializedError;
  requestContext: RequestContext;
  /**
   * Tracing context for span continuity during suspend/resume.
   * When provided, this will be persisted to the snapshot for use on resume.
   */
  tracingContext?: {
    traceId?: string;
    spanId?: string;
    parentSpanId?: string;
  };
  /**
   * Phase suffix appended to the durable operation ID to prevent duplicate
   * step IDs when persistStepUpdate is called multiple times for the same
   * execution path (e.g. 'start' before execution, 'entry-end' after).
   *
   * Every call site must pass a phase that is unique among the persists that
   * can run for one execution path in a single execution; otherwise replay
   * engines (Inngest) see duplicate step IDs. Optional only for backward
   * compatibility with external callers.
   */
  phase?: string;
  /** Canonical lifecycle events that must commit with this snapshot or not at all. */
  lifecycleEvents?: WorkflowLifecycleEvent[];
}

export function prepareStepSnapshot(engine: DefaultExecutionEngine, params: PersistStepUpdateParams) {
  const {
    runId,
    executionContext,
    workflowStatus,
    stepResults,
    serializedStepGraph,
    result,
    error,
    requestContext,
    tracingContext,
    lifecycleEvents,
  } = params;
  const requestContextObj = engine.serializeRequestContext(requestContext);

  const snapshot: WorkflowRunState = {
    runId,
    executionGeneration: executionContext.executionGeneration,
    lifecycleResumeAttempt: executionContext.lifecycleResumeAttempt,
    lifecycleStepStates: executionContext.lifecycleStepStates,
    status: workflowStatus,
    value: executionContext.state,
    context: stepResults as any,
    activePaths: executionContext.executionPath,
    stepExecutionPath: executionContext.stepExecutionPath,
    activeStepsPath: executionContext.activeStepsPath,
    serializedStepGraph,
    suspendedPaths: executionContext.suspendedPaths,
    waitingPaths: {},
    resumeLabels: executionContext.resumeLabels,
    result,
    error,
    requestContext: requestContextObj,
    timestamp: Date.now(),
    // Persist tracing context for span continuity on resume
    tracingContext,
    ...(lifecycleEvents && lifecycleEvents.length > 0 ? { lifecycleOutbox: lifecycleEvents } : {}),
  };

  const lifecyclePayloadBaseline = engine.options?.pruneSnapshot ? captureLifecyclePayloadBaseline(snapshot) : [];
  const snapshotToPersist = engine.options?.pruneSnapshot
    ? engine.options.pruneSnapshot({ snapshot: cloneLifecyclePayload(snapshot) as WorkflowRunState, workflowStatus })
    : snapshot;
  const prunedLifecycleEvents = engine.options?.pruneSnapshot
    ? getPrunedLifecycleEvents(snapshotToPersist, lifecyclePayloadBaseline)
    : lifecycleEvents;
  const snapshotForPersistence = (() => {
    if (!lifecycleEvents?.length) return snapshotToPersist;
    const { lifecycleOutbox: _lifecycleOutbox, ...withoutUnprunedOutbox } = snapshotToPersist;
    return prunedLifecycleEvents?.length
      ? { ...withoutUnprunedOutbox, lifecycleOutbox: prunedLifecycleEvents }
      : withoutUnprunedOutbox;
  })();

  return { snapshot, snapshotForPersistence, prunedLifecycleEvents };
}

export async function persistStepUpdate(
  engine: DefaultExecutionEngine,
  params: PersistStepUpdateParams,
): Promise<PersistWorkflowStepUpdateResult | void> {
  const {
    workflowId,
    runId,
    resourceId,
    stepResults,
    serializedStepGraph,
    executionContext,
    workflowStatus,
    result,
    error,
    requestContext,
    tracingContext,
    phase,
    lifecycleEvents,
  } = params;

  // A transient run is a per-execution decision. The workflow-level callback
  // may still describe the durable fallback used by explicit/custom run IDs,
  // so do not consult it for this execution.
  if (executionContext.transientExecution) {
    return;
  }

  const operationId = `workflow.${workflowId}.run.${runId}.path.${JSON.stringify(executionContext.executionPath)}.stepUpdate${phase ? `.${phase}` : ''}`;

  return engine.wrapDurableOperation(operationId, async () => {
    // A run-scoped override (e.g. the transient per-chunk runs of a workflow used as an
    // agent output processor, #19605) wins over the workflow-wide option.
    const persistencePredicate = engine.getRunPersistenceOverride(runId) ?? engine.options?.shouldPersistSnapshot;
    const shouldPersistSnapshot = persistencePredicate?.({ stepResults, workflowStatus });

    if (!shouldPersistSnapshot) {
      return;
    }

    // Guard: never overwrite a `suspended` / `paused` snapshot with a later
    // `running` update from the same run. During resume the loop transitions
    // suspended → running mid-execution, and any step-update write would
    // otherwise clobber the suspend record before the resume actually
    // completes. The engine tracks its own last-persisted status for this
    // run (process-local) so we don't need an extra storage read per step.
    if (workflowStatus === 'running') {
      const lastPersisted = engine.getLastPersistedStatus(runId);
      if (lastPersisted === 'suspended' || lastPersisted === 'paused') {
        return;
      }
    }

    const { snapshot, snapshotForPersistence, prunedLifecycleEvents } = prepareStepSnapshot(engine, params);
    const workflowsStore = await engine.mastra?.getStorage()?.getStore('workflows');
    const resumeCapabilities = workflowsStore?.getWorkflowResumeCapabilities();
    if (
      workflowsStore &&
      executionContext.resumeOperationHash !== undefined &&
      (resumeCapabilities?.atomicResumeVersion !== 1 || resumeCapabilities.fencedStepUpdateVersion !== 1)
    ) {
      throw new Error(
        `Workflow storage for ${workflowId}/${runId} does not support atomic resume admission and fenced resumed step updates`,
      );
    }
    if (workflowsStore && resumeCapabilities?.fencedStepUpdateVersion !== 1) {
      // Compatibility path for custom adapters which do not implement the
      // storage-locked step mutation. Capability-enabled adapters keep every
      // write, including ordinary default resumes, behind that mutation lock.
      const authoritativeSnapshot = await workflowsStore.loadWorkflowSnapshot({ workflowName: workflowId, runId });
      if (
        authoritativeSnapshot &&
        authoritativeSnapshot.executionGeneration !== executionContext.executionGeneration &&
        authoritativeSnapshot.executionGeneration !== undefined &&
        executionContext.executionGeneration !== undefined
      ) {
        return { status: 'stale_execution', disposition: 'superseded' };
      }
      if (
        authoritativeSnapshot &&
        (authoritativeSnapshot.status === 'success' ||
          authoritativeSnapshot.status === 'failed' ||
          authoritativeSnapshot.status === 'canceled' ||
          authoritativeSnapshot.status === 'tripwire' ||
          authoritativeSnapshot.status === 'bailed' ||
          authoritativeSnapshot.status === 'skipped')
      ) {
        return { status: 'finalized', disposition: authoritativeSnapshot.status };
      }
      const authoritativeMetadata = authoritativeSnapshot
        ? Object.fromEntries(
            Object.entries(authoritativeSnapshot).filter(
              ([key]) =>
                !(key in snapshotForPersistence) && (!engine.options?.pruneSnapshot || key !== 'lifecycleOutbox'),
            ),
          )
        : {};
      const legacyLifecycleOutbox = engine.options?.pruneSnapshot
        ? prunedLifecycleEvents
        : prunedLifecycleEvents?.length
          ? [...(authoritativeSnapshot?.lifecycleOutbox ?? []), ...prunedLifecycleEvents].slice(
              -WORKFLOW_LIFECYCLE_OUTBOX_LIMIT,
            )
          : authoritativeSnapshot?.lifecycleOutbox;
      await workflowsStore.persistWorkflowSnapshot({
        workflowName: workflowId,
        runId,
        resourceId,
        snapshot: {
          ...snapshotForPersistence,
          ...authoritativeMetadata,
          resourceId: authoritativeSnapshot?.resourceId ?? resourceId,
          resumeCheckpoint: authoritativeSnapshot?.resumeCheckpoint,
          resumeResultReceipt: authoritativeSnapshot?.resumeResultReceipt,
          resumeRollbackReceipt: authoritativeSnapshot?.resumeRollbackReceipt,
          executionGeneration: snapshot.executionGeneration,
          lifecycleResumeAttempt: snapshot.lifecycleResumeAttempt,
          lifecycleStepStates: snapshot.lifecycleStepStates,
          ...(lifecycleEvents?.length ? { lifecycleOutbox: legacyLifecycleOutbox } : {}),
        },
      });
      engine.setLastPersistedStatus(runId, workflowStatus);
      return {
        status: 'persisted',
        ...(lifecycleEvents?.length ? { acceptedEvents: prunedLifecycleEvents ?? [] } : {}),
      };
    }

    const persisted = await workflowsStore?.persistWorkflowStepUpdate({
      workflowName: workflowId,
      runId,
      resourceId,
      expectedResumeOperationHash: executionContext.resumeOperationHash,
      expectedExecutionGeneration: executionContext.executionGeneration,
      expectedLifecycleResumeAttempt: executionContext.lifecycleResumeAttempt,
      snapshot: snapshotForPersistence,
      lifecycleEvents: prunedLifecycleEvents,
      retainExistingLifecycleOutbox: engine.options?.pruneSnapshot === undefined,
    });
    if (persisted?.status === 'unsupported') {
      throw new Error(`Workflow storage for ${workflowId}/${runId} does not support fenced workflow step updates`);
    }
    if (persisted?.status === 'invalid_snapshot') {
      throw new Error(`Workflow storage rejected an invalid step snapshot for ${workflowId}/${runId}`);
    }
    if (persisted?.status === 'persisted') {
      engine.setLastPersistedStatus(runId, workflowStatus);
      if (lifecycleEvents?.length && persisted.acceptedEvents === undefined) {
        return { ...persisted, acceptedEvents: prunedLifecycleEvents ?? [] };
      }
    }
    return persisted;
  });
}

export interface ExecuteEntryParams extends ObservabilityContext {
  workflowId: string;
  runId: string;
  resourceId?: string;
  entry: StepFlowEntry;
  prevStep: StepFlowEntry;
  serializedStepGraph: SerializedStepFlowEntry[];
  stepResults: Record<string, StepResult<any, any, any, any>>;
  restart?: RestartExecutionParams;
  timeTravel?: TimeTravelExecutionParams;
  resume?: {
    steps: string[];
    stepResults: Record<string, StepResult<any, any, any, any>>;
    resumePayload: any;
    resumePath: number[];
    forEachIndex?: number;
  };
  executionContext: ExecutionContext;
  pubsub: PubSub;
  abortController: AbortController;
  requestContext: RequestContext;
  actor?: ActorSignal;
  outputWriter?: OutputWriter;
  disableScorers?: boolean;
  perStep?: boolean;
}

export async function executeEntry(
  engine: DefaultExecutionEngine,
  params: ExecuteEntryParams,
): Promise<EntryExecutionResult> {
  const {
    workflowId,
    runId,
    resourceId,
    entry: rawEntry,
    prevStep,
    serializedStepGraph,
    stepResults,
    restart,
    timeTravel,
    resume,
    executionContext,
    pubsub,
    abortController,
    requestContext,
    actor,
    outputWriter,
    disableScorers,
    perStep,
    ...rest
  } = params;
  const observabilityContext = resolveObservabilityContext(rest);
  const suppressLifecycleEvents = workflowLifecycleEventsAreSuppressed(pubsub);

  const entry = rawEntry;

  const prevOutput = engine.getStepOutput(stepResults, prevStep);
  let execResults: any;
  let entryRequestContext: Record<string, any> | undefined;

  if (isSingleStepEntry(entry)) {
    // The engine dispatches by step type: a plain `step` runs as-is, while the
    // declarative `agent` / `tool` / `mapping` variants each have their own
    // execute method that resolves and runs the step. Resume bookkeeping keys
    // off the entry id and is shared across all single-step kinds.
    const stepId = getSingleStepEntryId(entry);
    const isResumedStep = resume?.steps?.includes(stepId) ?? false;
    if (!isResumedStep) {
      executionContext.stepExecutionPath?.push(stepId);
    }
    const stepPrevOutput = getResumeStepPrevOutput({
      isResumedStep,
      stepId,
      stepResults,
      prevOutput,
    });
    const singleStepParams = {
      workflowId,
      runId,
      resourceId,
      stepResults,
      executionContext,
      timeTravel,
      restart,
      resume,
      prevOutput: stepPrevOutput,
      ...observabilityContext,
      pubsub,
      abortController,
      requestContext,
      actor,
      outputWriter,
      disableScorers,
      serializedStepGraph,
      perStep,
    };
    const stepExecResult =
      entry.type === 'step'
        ? await engine.executeStep({ ...singleStepParams, step: entry.step })
        : entry.type === 'agent'
          ? await engine.executeAgent({ ...singleStepParams, entry })
          : entry.type === 'tool'
            ? await engine.executeTool({ ...singleStepParams, entry })
            : await engine.executeMapping({ ...singleStepParams, entry });

    // Extract result and apply context changes
    execResults = stepExecResult.result;
    engine.applyMutableContext(executionContext, stepExecResult.mutableContext);
    Object.assign(stepResults, stepExecResult.stepResults);
    entryRequestContext = stepExecResult.requestContext;
  } else if (resume?.resumePath?.length && entry.type === 'parallel') {
    const idx = resume.resumePath.shift();
    const resumedStepResult = await executeEntry(engine, {
      workflowId,
      runId,
      resourceId,
      entry: entry.steps[idx!]!,
      prevStep,
      serializedStepGraph,
      stepResults,
      resume,
      executionContext: {
        transientExecution: executionContext.transientExecution,
        executionGeneration: executionContext.executionGeneration,
        lifecycleResumeAttempt: executionContext.lifecycleResumeAttempt,
        lifecycleStepStates: executionContext.lifecycleStepStates,
        workflowId,
        runId,
        executionPath: [...executionContext.executionPath, idx!],
        stepExecutionPath: executionContext.stepExecutionPath ? [...executionContext.stepExecutionPath] : undefined,
        suspendedPaths: executionContext.suspendedPaths,
        resumeLabels: executionContext.resumeLabels,
        retryConfig: executionContext.retryConfig,
        activeStepsPath: executionContext.activeStepsPath,
        state: executionContext.state,
      },
      ...observabilityContext,
      pubsub,
      abortController,
      requestContext,
      actor,
      outputWriter,
      disableScorers,
      perStep,
    });

    // Apply context changes from resumed step
    engine.applyMutableContext(executionContext, resumedStepResult.mutableContext);
    Object.assign(stepResults, resumedStepResult.stepResults);

    execResults = buildResumedBlockResult(entry.steps, stepResults, executionContext);

    return {
      result: execResults,
      stepResults,
      mutableContext: engine.buildMutableContext(executionContext),
      requestContext: resumedStepResult.requestContext,
    };
  } else if (entry.type === 'parallel') {
    execResults = await engine.executeParallel({
      workflowId,
      runId,
      resourceId,
      entry,
      prevStep,
      stepResults,
      serializedStepGraph,
      timeTravel,
      restart,
      resume,
      executionContext,
      ...observabilityContext,
      pubsub,
      abortController,
      requestContext,
      outputWriter,
      disableScorers,
      perStep,
    });
  } else if (resume?.resumePath?.length && entry.type === 'conditional') {
    // Resume-aware handling for conditional entries: skip condition re-evaluation
    // and go directly to the branch step identified by the resume path.
    // This mirrors the parallel resume handling above.
    const idx = resume.resumePath.shift();
    const branchStep = entry.steps[idx!]!;

    let branchResult: EntryExecutionResult;

    if (branchStep.type !== 'step') {
      // Recurse through executeEntry for nested block types (parallel, conditional, etc.)
      branchResult = await executeEntry(engine, {
        workflowId,
        runId,
        resourceId,
        entry: branchStep,
        prevStep,
        serializedStepGraph,
        stepResults,
        resume,
        executionContext: {
          transientExecution: executionContext.transientExecution,
          executionGeneration: executionContext.executionGeneration,
          lifecycleResumeAttempt: executionContext.lifecycleResumeAttempt,
          lifecycleStepStates: executionContext.lifecycleStepStates,
          workflowId,
          runId,
          executionPath: [...executionContext.executionPath, idx!],
          stepExecutionPath: executionContext.stepExecutionPath ? [...executionContext.stepExecutionPath] : undefined,
          suspendedPaths: executionContext.suspendedPaths,
          resumeLabels: executionContext.resumeLabels,
          retryConfig: executionContext.retryConfig,
          activeStepsPath: executionContext.activeStepsPath,
          state: executionContext.state,
        },
        ...observabilityContext,
        pubsub,
        abortController,
        requestContext,
        actor,
        outputWriter,
        disableScorers,
        perStep,
      });
    } else {
      const resumePrevOutput = getResumeStepPrevOutput({
        isResumedStep: true,
        stepId: branchStep.step.id,
        stepResults,
        prevOutput,
      });

      branchResult = await engine.executeStep({
        workflowId,
        runId,
        resourceId,
        step: branchStep.step,
        prevOutput: resumePrevOutput,
        stepResults,
        serializedStepGraph,
        resume,
        restart,
        timeTravel,
        executionContext: {
          transientExecution: executionContext.transientExecution,
          executionGeneration: executionContext.executionGeneration,
          lifecycleResumeAttempt: executionContext.lifecycleResumeAttempt,
          lifecycleStepStates: executionContext.lifecycleStepStates,
          workflowId,
          runId,
          executionPath: [...executionContext.executionPath, idx!],
          stepExecutionPath: executionContext.stepExecutionPath ? [...executionContext.stepExecutionPath] : undefined,
          suspendedPaths: executionContext.suspendedPaths,
          resumeLabels: executionContext.resumeLabels,
          retryConfig: executionContext.retryConfig,
          activeStepsPath: executionContext.activeStepsPath,
          state: executionContext.state,
        },
        ...observabilityContext,
        pubsub,
        abortController,
        requestContext,
        actor,
        outputWriter,
        disableScorers,
        perStep,
      });
    }

    // Apply context changes from resumed step
    engine.applyMutableContext(executionContext, branchResult.mutableContext);
    Object.assign(stepResults, branchResult.stepResults);

    // For conditionals, only check steps that were actually executed (have results).
    // Branches whose conditions were false during initial execution should be ignored.
    execResults = buildResumedBlockResult(entry.steps, stepResults, executionContext, { onlyExecutedSteps: true });

    return {
      result: execResults,
      stepResults,
      mutableContext: engine.buildMutableContext(executionContext),
      requestContext: branchResult.requestContext,
    };
  } else if (entry.type === 'conditional') {
    execResults = await engine.executeConditional({
      workflowId,
      runId,
      resourceId,
      entry,
      prevOutput,
      stepResults,
      serializedStepGraph,
      timeTravel,
      restart,
      resume,
      executionContext,
      ...observabilityContext,
      pubsub,
      abortController,
      requestContext,
      actor,
      outputWriter,
      disableScorers,
      perStep,
    });
  } else if (entry.type === 'loop') {
    execResults = await engine.executeLoop({
      workflowId,
      runId,
      resourceId,
      entry,
      prevStep,
      prevOutput,
      stepResults,
      timeTravel,
      restart,
      resume,
      executionContext,
      ...observabilityContext,
      pubsub,
      abortController,
      requestContext,
      actor,
      outputWriter,
      disableScorers,
      serializedStepGraph,
      perStep,
    });
  } else if (entry.type === 'foreach') {
    const foreachStepId = getSingleStepEntryId(entry.step);
    const foreachPrevOutput = getResumeStepPrevOutput({
      isResumedStep: resume?.steps?.includes(foreachStepId) ?? false,
      stepId: foreachStepId,
      stepResults,
      prevOutput,
    });

    execResults = await engine.executeForeach({
      workflowId,
      runId,
      resourceId,
      entry,
      prevStep,
      prevOutput: foreachPrevOutput,
      stepResults,
      timeTravel,
      restart,
      resume,
      executionContext,
      ...observabilityContext,
      pubsub,
      abortController,
      requestContext,
      actor,
      outputWriter,
      disableScorers,
      serializedStepGraph,
      perStep,
    });
  } else if (entry.type === 'sleep') {
    executionContext.stepExecutionPath?.push(entry.id);
    const startedAt = Date.now();
    if (!suppressLifecycleEvents) {
      const sleepWaitingOperationId = `workflow.${workflowId}.run.${runId}.sleep.${entry.id}.waiting_ev`;
      await engine.wrapDurableOperation(sleepWaitingOperationId, async () => {
        await publishStepEvent(engine, pubsub, `workflow.events.v2.${runId}`, {
          type: 'watch',
          runId,
          data: {
            type: 'workflow-step-waiting',
            payload: {
              id: entry.id,
              payload: prevOutput,
              startedAt,
              status: 'waiting',
            },
          },
        });
      });
    }

    stepResults[entry.id] = {
      status: 'waiting',
      payload: prevOutput,
      startedAt,
    };
    executionContext.activeStepsPath[entry.id] = executionContext.executionPath;

    if (!executionContext.transientExecution) {
      await engine.persistStepUpdate({
        workflowId,
        runId,
        resourceId,
        serializedStepGraph,
        stepResults,
        executionContext,
        workflowStatus: 'waiting',
        requestContext,
        phase: 'sleep-waiting',
      });
    }

    await engine.executeSleep({
      workflowId,
      runId,
      entry,
      prevStep,
      prevOutput,
      stepResults,
      serializedStepGraph,
      resume,
      executionContext,
      ...observabilityContext,
      pubsub,
      abortController,
      requestContext,
      outputWriter,
    });

    delete executionContext.activeStepsPath[entry.id];

    // An abort during the sleep must not be overwritten by a success terminal
    // for the sleep entry; upstream fix, kept behind the fork's transient and
    // lifecycle-suppression guards.
    if (abortController?.signal?.aborted) {
      execResults = { status: 'canceled' };
    } else {
      if (!executionContext.transientExecution) {
        await engine.persistStepUpdate({
          workflowId,
          runId,
          resourceId,
          serializedStepGraph,
          stepResults,
          executionContext,
          workflowStatus: 'running',
          requestContext,
          phase: 'sleep-resumed',
        });
      }

      const endedAt = Date.now();
      const stepInfo = {
        payload: prevOutput,
        startedAt,
        endedAt,
      };

      execResults = { ...stepInfo, status: 'success', output: prevOutput };
      stepResults[entry.id] = { ...stepInfo, status: 'success', output: prevOutput };

      if (!suppressLifecycleEvents) {
        const sleepResultOperationId = `workflow.${workflowId}.run.${runId}.sleep.${entry.id}.result_ev`;
        await engine.wrapDurableOperation(sleepResultOperationId, async () => {
          await publishStepEvent(engine, pubsub, `workflow.events.v2.${runId}`, {
            type: 'watch',
            runId,
            data: {
              type: 'workflow-step-result',
              payload: {
                id: entry.id,
                endedAt,
                status: 'success',
                output: prevOutput,
              },
            },
          });

          await publishStepEvent(engine, pubsub, `workflow.events.v2.${runId}`, {
            type: 'watch',
            runId,
            data: {
              type: 'workflow-step-finish',
              payload: {
                id: entry.id,
                metadata: {},
              },
            },
          });
        });
      }
    }
  } else if (entry.type === 'sleepUntil') {
    executionContext.stepExecutionPath?.push(entry.id);
    const startedAt = Date.now();
    if (!suppressLifecycleEvents) {
      const sleepUntilWaitingOperationId = `workflow.${workflowId}.run.${runId}.sleepUntil.${entry.id}.waiting_ev`;
      await engine.wrapDurableOperation(sleepUntilWaitingOperationId, async () => {
        await publishStepEvent(engine, pubsub, `workflow.events.v2.${runId}`, {
          type: 'watch',
          runId,
          data: {
            type: 'workflow-step-waiting',
            payload: {
              id: entry.id,
              payload: prevOutput,
              startedAt,
              status: 'waiting',
            },
          },
        });
      });
    }

    stepResults[entry.id] = {
      status: 'waiting',
      payload: prevOutput,
      startedAt,
    };
    executionContext.activeStepsPath[entry.id] = executionContext.executionPath;

    if (!executionContext.transientExecution) {
      await engine.persistStepUpdate({
        workflowId,
        runId,
        resourceId,
        serializedStepGraph,
        stepResults,
        executionContext,
        workflowStatus: 'waiting',
        requestContext,
        phase: 'sleep-until-waiting',
      });
    }

    await engine.executeSleepUntil({
      workflowId,
      runId,
      entry,
      prevStep,
      prevOutput,
      stepResults,
      serializedStepGraph,
      resume,
      executionContext,
      ...observabilityContext,
      pubsub,
      abortController,
      requestContext,
      outputWriter,
    });

    delete executionContext.activeStepsPath[entry.id];

    // An abort during the sleep must not be overwritten by a success terminal
    // for the sleep entry; upstream fix, kept behind the fork's transient and
    // lifecycle-suppression guards.
    if (abortController?.signal?.aborted) {
      execResults = { status: 'canceled' };
    } else {
      if (!executionContext.transientExecution) {
        await engine.persistStepUpdate({
          workflowId,
          runId,
          resourceId,
          serializedStepGraph,
          stepResults,
          executionContext,
          workflowStatus: 'running',
          requestContext,
          phase: 'sleep-until-resumed',
        });
      }

      const endedAt = Date.now();
      const stepInfo = {
        payload: prevOutput,
        startedAt,
        endedAt,
      };

      execResults = { ...stepInfo, status: 'success', output: prevOutput };
      stepResults[entry.id] = { ...stepInfo, status: 'success', output: prevOutput };

      if (!suppressLifecycleEvents) {
        const sleepUntilResultOperationId = `workflow.${workflowId}.run.${runId}.sleepUntil.${entry.id}.result_ev`;
        await engine.wrapDurableOperation(sleepUntilResultOperationId, async () => {
          await publishStepEvent(engine, pubsub, `workflow.events.v2.${runId}`, {
            type: 'watch',
            runId,
            data: {
              type: 'workflow-step-result',
              payload: {
                id: entry.id,
                endedAt,
                status: 'success',
                output: prevOutput,
              },
            },
          });

          await publishStepEvent(engine, pubsub, `workflow.events.v2.${runId}`, {
            type: 'watch',
            runId,
            data: {
              type: 'workflow-step-finish',
              payload: {
                id: entry.id,
                metadata: {},
              },
            },
          });
        });
      }
    }
  }

  if (isSingleStepEntry(entry)) {
    stepResults[getSingleStepEntryId(entry)] = execResults;
  } else if (entry.type === 'loop' || entry.type === 'foreach') {
    stepResults[getSingleStepEntryId(entry.step)] = execResults;
  }

  if (abortController?.signal?.aborted) {
    execResults = { ...execResults, status: 'canceled' };
  }

  let persistOutcome: PersistWorkflowStepUpdateResult | void = undefined;
  if (!executionContext.transientExecution) {
    const isCompositeChild = executionContext.foreachIndex !== undefined || executionContext.executionPath.length > 1;
    const workflowStatus =
      !isCompositeChild && (execResults.status === 'suspended' || execResults.status === 'paused')
        ? execResults.status
        : 'running';
    persistOutcome = await engine.persistStepUpdate({
      workflowId,
      runId,
      resourceId,
      serializedStepGraph,
      stepResults,
      executionContext,
      workflowStatus,
      requestContext,
      phase: 'entry-result',
    });
    if (persistOutcome?.status === 'protected_state' && workflowStatus === 'running') {
      const executionGeneration = requireWorkflowExecutionGeneration(
        executionContext.executionGeneration,
        `Workflow entry ${workflowId}/${runId}`,
      );
      const disposition = await engine.getAuthoritativeExecutionDisposition({
        workflowId,
        runId,
        executionGeneration,
      });
      if (!disposition) persistOutcome = undefined;
    }
    const matchesLocallyPersistedResult = engine.getLastPersistedStatus(runId) === workflowStatus;
    const repeatsLocalTerminal =
      persistOutcome?.status === 'finalized' &&
      (persistOutcome.disposition === undefined || persistOutcome.disposition === workflowStatus);
    const repeatsLocalResumeResult =
      persistOutcome?.status === 'stale_execution' &&
      persistOutcome.disposition === undefined &&
      (workflowStatus === 'suspended' || workflowStatus === 'paused');
    if (matchesLocallyPersistedResult && (repeatsLocalTerminal || repeatsLocalResumeResult)) {
      // A single-step entry persists once in executeStep and again here. The
      // fence rejects that second write after the first one has already saved
      // this engine's terminal or resumed-suspend result. It is not a remote
      // disposition and must not turn the local result into cancellation.
      persistOutcome = undefined;
    } else if (persistOutcome && persistOutcome.status !== 'persisted') {
      execResults = { ...execResults, status: 'canceled', endedAt: Date.now() };
    }
  }

  if (
    !suppressLifecycleEvents &&
    execResults.status === 'canceled' &&
    !(persistOutcome && persistOutcome.status !== 'persisted')
  ) {
    await publishStepEvent(engine, pubsub, `workflow.events.v2.${runId}`, {
      type: 'watch',
      runId,
      data: { type: 'workflow-canceled', payload: {} },
    });
  }

  return {
    result: execResults,
    stepResults,
    persistOutcome,
    mutableContext: engine.buildMutableContext(executionContext),
    // Serialize requestContext only for engines that restore it from serialized
    // results (Inngest memoization). The default engine keeps the original
    // reference and never reads this field, so serializing here would be pure
    // per-entry overhead — RequestContext.toJSON() probes every stored value
    // with JSON.stringify.
    requestContext:
      entryRequestContext ??
      (engine.requiresDurableContextSerialization() ? engine.serializeRequestContext(requestContext) : undefined),
  };
}
