import { createHash } from 'node:crypto';
import type {
  WorkflowLifecycleFenceV1,
  WorkflowResumeCheckpointV1,
  WorkflowResumeOperationReplayContextV1,
  WorkflowResumeRollbackReceiptV1,
  WorkflowResumeResultReceiptV1,
  WorkflowRunState,
} from '../../../workflows';
import type {
  AdmitWorkflowResumeInput,
  AdmitWorkflowResumeResult,
  ConsumeWorkflowResumeResult,
  ConsumeWorkflowResumeResultInput,
  FinalizeWorkflowResumeInput,
  FinalizeWorkflowResumeResult,
  PersistWorkflowStepUpdateInput,
  PersistWorkflowStepUpdateResult,
  RollbackWorkflowResumeInput,
  RollbackWorkflowResumeResult,
} from '../../types';

export const WORKFLOW_RESUME_RESULT_RECEIPT_MAX_BYTES = 8 * 1024 * 1024;

type MaterializeSnapshot = (snapshot: WorkflowRunState) => WorkflowRunState;

type InternalResumeMutationResult<T> = T & { snapshot?: WorkflowRunState };

const TERMINAL_STATUSES = new Set(['success', 'failed', 'canceled', 'tripwire', 'bailed', 'skipped']);

const WORKFLOW_STATE_RUNTIME_KEYS = new Set([
  'runId',
  'resourceId',
  'status',
  'result',
  'error',
  'requestContext',
  'value',
  'context',
  'serializedStepGraph',
  'activePaths',
  'activeStepsPath',
  'suspendedPaths',
  'resumeLabels',
  'waitingPaths',
  'timestamp',
  'executionGeneration',
  'lifecycleResumeAttempt',
  'lifecycleStepStates',
  'resumeCheckpoint',
  'resumeResultReceipt',
  'resumeRollbackReceipt',
  'tripwire',
  'stepExecutionPath',
  'tracingContext',
]);

function fail(reason: string): never {
  throw new TypeError(`Invalid workflow resume data: ${reason}`);
}

function normalizedStepStates(value: unknown): Record<string, { stepCallId: string; stepAttempt: number }> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) fail('lifecycleStepStates');
  const normalized: Record<string, { stepCallId: string; stepAttempt: number }> = {};
  for (const coordinate of Object.keys(value).sort()) {
    const state = (value as Record<string, unknown>)[coordinate];
    if (
      !state ||
      typeof state !== 'object' ||
      Array.isArray(state) ||
      typeof (state as { stepCallId?: unknown }).stepCallId !== 'string' ||
      !(state as { stepCallId: string }).stepCallId ||
      !Number.isSafeInteger((state as { stepAttempt?: unknown }).stepAttempt) ||
      (state as { stepAttempt: number }).stepAttempt < 1
    ) {
      fail(`lifecycleStepStates.${coordinate}`);
    }
    normalized[coordinate] = {
      stepCallId: (state as { stepCallId: string }).stepCallId,
      stepAttempt: (state as { stepAttempt: number }).stepAttempt,
    };
  }
  return normalized;
}

function stableJsonValue(value: unknown, seen = new WeakSet<object>(), key = ''): unknown {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return value;
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : null;
  }
  if (value === undefined || typeof value === 'function' || typeof value === 'symbol') return undefined;
  if (typeof value !== 'object') fail('non-JSON value');
  if (seen.has(value)) fail('cyclic value');
  seen.add(value);

  const toJSON = (value as { toJSON?: unknown }).toJSON;
  if (typeof toJSON === 'function') {
    const serialized = toJSON.call(value, key);
    if (serialized !== value) {
      const result = stableJsonValue(serialized, seen, key);
      seen.delete(value);
      return result;
    }
  }

  if (Array.isArray(value)) {
    const result = value.map((item, index) => stableJsonValue(item, seen, String(index)) ?? null);
    seen.delete(value);
    return result;
  }
  const prototype = Object.getPrototypeOf(value);
  if (prototype !== Object.prototype && prototype !== null) fail('unsupported object prototype');
  const result: Record<string, unknown> = {};
  for (const key of Object.keys(value).sort()) {
    const normalized = stableJsonValue((value as Record<string, unknown>)[key], seen, key);
    if (normalized !== undefined) result[key] = normalized;
  }
  seen.delete(value);
  return result;
}

function canonicalJson(value: unknown): string {
  return JSON.stringify(stableJsonValue(value));
}

function hash(value: unknown): `sha256:${string}` {
  return `sha256:${createHash('sha256').update(canonicalJson(value), 'utf8').digest('hex')}`;
}

function checkpointIntegrityValue(value: unknown, seen = new Map<object, number>()): unknown {
  if (value === null) return ['null'];
  if (value === undefined) return ['undefined'];
  if (typeof value === 'string' || typeof value === 'boolean') return [typeof value, value];
  if (typeof value === 'number') {
    if (Number.isNaN(value)) return ['number', 'NaN'];
    if (value === Infinity) return ['number', 'Infinity'];
    if (value === -Infinity) return ['number', '-Infinity'];
    if (Object.is(value, -0)) return ['number', '-0'];
    return ['number', value];
  }
  if (typeof value === 'bigint') return ['bigint', value.toString()];
  if (typeof value === 'symbol') return ['symbol', Symbol.keyFor(value) ?? null, value.description ?? null];
  if (typeof value === 'function') return ['function', value.name, Function.prototype.toString.call(value)];

  const retainedReference = seen.get(value);
  if (retainedReference !== undefined) return ['ref', retainedReference];
  const reference = seen.size;
  seen.set(value, reference);

  if (value instanceof Date) return ['date', reference, value.getTime()];
  if (value instanceof RegExp) return ['regexp', reference, value.source, value.flags, value.lastIndex];
  if (value instanceof URL) return ['url', reference, value.href];
  if (value instanceof Map) {
    return [
      'map',
      reference,
      [...value].map(([key, entryValue]) => [
        checkpointIntegrityValue(key, seen),
        checkpointIntegrityValue(entryValue, seen),
      ]),
    ];
  }
  if (value instanceof Set) {
    return ['set', reference, [...value].map(entry => checkpointIntegrityValue(entry, seen))];
  }
  if (value instanceof ArrayBuffer) {
    return ['array-buffer', reference, Buffer.from(value).toString('base64')];
  }
  if (ArrayBuffer.isView(value)) {
    return [
      value instanceof DataView ? 'data-view' : 'typed-array',
      reference,
      value.constructor.name,
      Buffer.from(value.buffer, value.byteOffset, value.byteLength).toString('base64'),
    ];
  }
  if (value instanceof Error) {
    const own = Object.keys(value)
      .filter(key => key !== 'cause')
      .sort()
      .map(key => [key, checkpointIntegrityValue((value as unknown as Record<string, unknown>)[key], seen)]);
    return [
      'error',
      reference,
      Object.getPrototypeOf(value)?.constructor?.name ?? 'Error',
      value.name,
      value.message,
      value.stack,
      checkpointIntegrityValue(value.cause, seen),
      own,
    ];
  }
  if (Array.isArray(value)) {
    return ['array', reference, value.map(entry => checkpointIntegrityValue(entry, seen))];
  }

  const prototype = Object.getPrototypeOf(value);
  const prototypeName = prototype === null ? null : (prototype?.constructor?.name ?? 'Object');
  return [
    'object',
    reference,
    prototypeName,
    Object.keys(value)
      .sort()
      .map(key => [key, checkpointIntegrityValue((value as Record<string, unknown>)[key], seen)]),
  ];
}

function checkpointIntegrityHash(value: unknown): `sha256:${string}` {
  return `sha256:${createHash('sha256')
    .update(JSON.stringify(checkpointIntegrityValue(value)), 'utf8')
    .digest('hex')}`;
}

function isHash(value: unknown): value is `sha256:${string}` {
  return typeof value === 'string' && /^sha256:[a-f0-9]{64}$/.test(value);
}

/**
 * Canonical identity for the complete serialized resume operation. Callers
 * deliberately exclude ephemeral authorization signals such as `actor`.
 */
export function materializeWorkflowResumeOperationHash(value: unknown): `sha256:${string}` {
  return hash({ version: 1, operation: value });
}

function captureFence(input: {
  executionGeneration?: unknown;
  lifecycleResumeAttempt?: unknown;
  lifecycleStepStates?: unknown;
}): Omit<WorkflowLifecycleFenceV1, 'fenceHash'> {
  if (typeof input.executionGeneration !== 'string' || !input.executionGeneration) fail('executionGeneration');
  if (!Number.isSafeInteger(input.lifecycleResumeAttempt) || (input.lifecycleResumeAttempt as number) < 0) {
    fail('lifecycleResumeAttempt');
  }
  return {
    executionGeneration: input.executionGeneration,
    lifecycleResumeAttempt: input.lifecycleResumeAttempt as number,
    lifecycleStepStates: normalizedStepStates(input.lifecycleStepStates),
  };
}

export function materializeWorkflowLifecycleFence(input: {
  executionGeneration?: unknown;
  lifecycleResumeAttempt?: unknown;
  lifecycleStepStates?: unknown;
}): WorkflowLifecycleFenceV1 {
  const fence = captureFence(input);
  return { ...fence, fenceHash: hash(fence) };
}

function fenceMatchesSnapshot(snapshot: WorkflowRunState, fence: WorkflowLifecycleFenceV1): boolean {
  try {
    return materializeWorkflowLifecycleFence(snapshot).fenceHash === fence.fenceHash;
  } catch {
    return false;
  }
}

function stripResumeEvidence(snapshot: WorkflowRunState, materialize: MaterializeSnapshot): WorkflowRunState {
  const owned = materialize(snapshot);
  delete owned.resumeCheckpoint;
  delete owned.resumeResultReceipt;
  delete owned.resumeRollbackReceipt;
  return owned;
}

function checkpointBody(checkpoint: Omit<WorkflowResumeCheckpointV1, 'checkpointHash'>) {
  return {
    version: checkpoint.version,
    runId: checkpoint.runId,
    resumeOperationHash: checkpoint.resumeOperationHash,
    operationReplayContext: checkpoint.operationReplayContext,
    executionGeneration: checkpoint.executionGeneration,
    lifecycleResumeAttempt: checkpoint.lifecycleResumeAttempt,
    lifecycleStepStates: checkpoint.lifecycleStepStates,
    fenceHash: checkpoint.fenceHash,
    snapshot: checkpoint.snapshot,
  };
}

function materializeOperationReplayContext(value: unknown): WorkflowResumeOperationReplayContextV1 | undefined {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return undefined;
  const context = value as Partial<WorkflowResumeOperationReplayContextV1>;
  if (
    context.version !== 1 ||
    !Array.isArray(context.steps) ||
    context.steps.some(step => typeof step !== 'string' || !step) ||
    (context.label !== undefined && (typeof context.label !== 'string' || !context.label)) ||
    (context.resumePath !== undefined &&
      (!Array.isArray(context.resumePath) ||
        context.resumePath.some(index => !Number.isSafeInteger(index) || index < 0)))
  ) {
    return undefined;
  }
  return {
    version: 1 as const,
    steps: [...context.steps],
    ...(context.resumePath === undefined ? {} : { resumePath: [...context.resumePath] }),
    ...(context.label === undefined ? {} : { label: context.label }),
  };
}

function materializeCheckpoint(
  checkpoint: WorkflowResumeCheckpointV1,
  materialize: MaterializeSnapshot,
): WorkflowResumeCheckpointV1 | undefined {
  try {
    const fence = materializeWorkflowLifecycleFence(checkpoint);
    if (
      fence.fenceHash !== checkpoint.fenceHash ||
      checkpoint.version !== 1 ||
      !isHash(checkpoint.resumeOperationHash)
    ) {
      return undefined;
    }
    const snapshot = stripResumeEvidence(checkpoint.snapshot, materialize);
    const operationReplayContext = materializeOperationReplayContext(checkpoint.operationReplayContext);
    const snapshotFence = materializeWorkflowLifecycleFence(snapshot);
    if (
      typeof checkpoint.runId !== 'string' ||
      !checkpoint.runId ||
      snapshot.runId !== checkpoint.runId ||
      snapshot.status !== 'suspended' ||
      snapshotFence.executionGeneration !== fence.executionGeneration ||
      snapshotFence.lifecycleResumeAttempt + 1 !== fence.lifecycleResumeAttempt ||
      canonicalJson(snapshotFence.lifecycleStepStates) !== canonicalJson(fence.lifecycleStepStates) ||
      !operationReplayContext
    ) {
      return undefined;
    }
    const candidate = {
      version: 1 as const,
      runId: checkpoint.runId,
      resumeOperationHash: checkpoint.resumeOperationHash,
      operationReplayContext,
      ...fence,
      snapshot,
    };
    const checkpointHash = checkpointIntegrityHash(checkpointBody(candidate));
    if (checkpointHash !== checkpoint.checkpointHash) return undefined;
    return { ...candidate, checkpointHash };
  } catch {
    return undefined;
  }
}

function createCheckpoint(
  runId: string,
  admittedFence: WorkflowLifecycleFenceV1,
  resumeOperationHash: `sha256:${string}`,
  operationReplayContext: WorkflowResumeOperationReplayContextV1,
  snapshot: WorkflowRunState,
  materialize: MaterializeSnapshot,
): WorkflowResumeCheckpointV1 {
  const checkpoint = {
    version: 1 as const,
    runId,
    resumeOperationHash,
    operationReplayContext,
    ...admittedFence,
    snapshot: stripResumeEvidence(snapshot, materialize),
  };
  return { ...checkpoint, checkpointHash: checkpointIntegrityHash(checkpointBody(checkpoint)) };
}

function receiptBody(receipt: Omit<WorkflowResumeResultReceiptV1, 'receiptHash' | 'consumedBy' | 'consumedAt'>) {
  return {
    version: receipt.version,
    runId: receipt.runId,
    receiptKey: receipt.receiptKey,
    resumeOperationHash: receipt.resumeOperationHash,
    operationReplayContext: receipt.operationReplayContext,
    executionGeneration: receipt.executionGeneration,
    lifecycleResumeAttempt: receipt.lifecycleResumeAttempt,
    lifecycleStepStates: receipt.lifecycleStepStates,
    fenceHash: receipt.fenceHash,
    result: receipt.result,
    createdAt: receipt.createdAt,
  };
}

function materializeReceipt(receipt: WorkflowResumeResultReceiptV1): WorkflowResumeResultReceiptV1 | undefined {
  try {
    if (
      receipt.version !== 1 ||
      typeof receipt.receiptKey !== 'string' ||
      !receipt.receiptKey ||
      !isHash(receipt.resumeOperationHash)
    ) {
      return undefined;
    }
    const fence = materializeWorkflowLifecycleFence(receipt);
    if (fence.fenceHash !== receipt.fenceHash) return undefined;
    const operationReplayContext = materializeOperationReplayContext(receipt.operationReplayContext);
    if (!operationReplayContext) return undefined;
    const body = stableJsonValue(receiptBody(receipt)) as Omit<
      WorkflowResumeResultReceiptV1,
      'receiptHash' | 'consumedBy' | 'consumedAt'
    >;
    const receiptHash = hash(body);
    if (receiptHash !== receipt.receiptHash) return undefined;
    if (
      (receipt.consumedBy !== undefined && (typeof receipt.consumedBy !== 'string' || !receipt.consumedBy)) ||
      (receipt.consumedAt !== undefined &&
        (typeof receipt.consumedAt !== 'number' || !Number.isFinite(receipt.consumedAt)))
    ) {
      return undefined;
    }
    return {
      ...body,
      operationReplayContext,
      receiptHash,
      ...(receipt.consumedBy === undefined ? {} : { consumedBy: receipt.consumedBy }),
      ...(receipt.consumedAt === undefined ? {} : { consumedAt: receipt.consumedAt }),
    };
  } catch {
    return undefined;
  }
}

function extensionMetadata(snapshot: WorkflowRunState): Record<string, unknown> {
  return Object.fromEntries(Object.entries(snapshot).filter(([key]) => !WORKFLOW_STATE_RUNTIME_KEYS.has(key)));
}

/**
 * Applies an ordinary workflow step snapshot behind the same storage lock used
 * by resume finalization. This is deliberately a storage mutation primitive:
 * a read-then-persist guard in the workflow handler leaves a window where a
 * stale step can overwrite a terminal receipt.
 */
export function persistWorkflowStepUpdateRecord(
  existing: WorkflowRunState | undefined,
  input: PersistWorkflowStepUpdateInput,
  materialize: MaterializeSnapshot,
): InternalResumeMutationResult<PersistWorkflowStepUpdateResult> {
  if (input.snapshot.runId !== input.runId) return { status: 'invalid_snapshot' };

  let proposed: WorkflowRunState;
  try {
    proposed = materialize(input.snapshot);
  } catch {
    return { status: 'invalid_snapshot' };
  }

  if (!existing && input.expectedResumeOperationHash !== undefined) {
    return { status: 'missing_run' };
  }
  if (!existing) {
    return { status: 'persisted', snapshot: proposed };
  }

  if (existing.resumeResultReceipt || TERMINAL_STATUSES.has(existing.status)) {
    return { status: 'finalized' };
  }
  if (input.expectedResumeOperationHash !== undefined) {
    const checkpoint = existing.resumeCheckpoint
      ? materializeCheckpoint(existing.resumeCheckpoint, materialize)
      : undefined;
    if (
      !isHash(input.expectedResumeOperationHash) ||
      !checkpoint ||
      checkpoint.runId !== input.runId ||
      checkpoint.resumeOperationHash !== input.expectedResumeOperationHash ||
      checkpoint.executionGeneration !== input.expectedExecutionGeneration ||
      checkpoint.lifecycleResumeAttempt !== input.expectedLifecycleResumeAttempt
    ) {
      return { status: 'stale_execution' };
    }
  }
  if (
    input.expectedExecutionGeneration !== undefined &&
    existing.executionGeneration !== undefined &&
    existing.executionGeneration !== input.expectedExecutionGeneration
  ) {
    return { status: 'stale_execution' };
  }
  const isUnadmittedOrdinaryResume =
    input.expectedResumeOperationHash === undefined &&
    input.expectedLifecycleResumeAttempt !== undefined &&
    input.expectedLifecycleResumeAttempt > 0;
  const isOrdinaryResumeResultStatus =
    proposed.status === 'suspended' || proposed.status === 'paused' || TERMINAL_STATUSES.has(proposed.status);
  if (
    isUnadmittedOrdinaryResume &&
    (proposed.lifecycleResumeAttempt !== input.expectedLifecycleResumeAttempt ||
      typeof input.expectedExecutionGeneration !== 'string' ||
      input.expectedExecutionGeneration.length === 0 ||
      existing.executionGeneration !== input.expectedExecutionGeneration ||
      proposed.executionGeneration !== input.expectedExecutionGeneration)
  ) {
    return { status: 'stale_execution' };
  }
  if (isUnadmittedOrdinaryResume && !isOrdinaryResumeResultStatus) {
    return { status: 'protected_state' };
  }
  if (isUnadmittedOrdinaryResume) {
    const existingResumeAttempt = existing.lifecycleResumeAttempt ?? 0;
    if (existing.status !== 'suspended' && existing.status !== 'paused') {
      return { status: 'stale_execution' };
    }
    if (existing.resumeCheckpoint !== undefined || input.expectedLifecycleResumeAttempt !== existingResumeAttempt + 1) {
      return { status: 'stale_execution' };
    }
  } else if (
    input.expectedLifecycleResumeAttempt !== undefined &&
    existing.lifecycleResumeAttempt !== undefined &&
    existing.lifecycleResumeAttempt !== input.expectedLifecycleResumeAttempt
  ) {
    return { status: 'stale_execution' };
  }
  if ((existing.status === 'suspended' || existing.status === 'paused') && proposed.status === 'running') {
    return { status: 'protected_state' };
  }

  const retainedMetadata = Object.fromEntries(
    Object.entries(existing).filter(([key]) => !(key in proposed) && !WORKFLOW_STATE_RUNTIME_KEYS.has(key)),
  );
  const snapshot = materialize({
    ...proposed,
    ...retainedMetadata,
    resourceId: existing.resourceId ?? proposed.resourceId ?? input.resourceId,
    resumeCheckpoint: existing.resumeCheckpoint,
    resumeResultReceipt: existing.resumeResultReceipt,
    resumeRollbackReceipt: existing.resumeRollbackReceipt,
  });
  return { status: 'persisted', snapshot };
}

function materializeRollbackReceipt(value: unknown): WorkflowResumeRollbackReceiptV1 | undefined {
  try {
    if (!value || typeof value !== 'object' || Array.isArray(value)) return undefined;
    const receipt = value as WorkflowResumeRollbackReceiptV1;
    if (
      receipt.version !== 1 ||
      typeof receipt.runId !== 'string' ||
      !receipt.runId ||
      !isHash(receipt.resumeOperationHash) ||
      !isHash(receipt.rollbackHash) ||
      typeof receipt.rolledBackAt !== 'number' ||
      !Number.isFinite(receipt.rolledBackAt)
    ) {
      return undefined;
    }
    const fence = materializeWorkflowLifecycleFence(receipt);
    if (fence.fenceHash !== receipt.fenceHash) return undefined;
    if (
      receipt.rollbackHash !==
      hash({
        version: receipt.version,
        runId: receipt.runId,
        resumeOperationHash: receipt.resumeOperationHash,
        ...fence,
        rolledBackAt: receipt.rolledBackAt,
      })
    ) {
      return undefined;
    }
    return { ...receipt, ...fence };
  } catch {
    return undefined;
  }
}

export function admitWorkflowResumeRecord(
  existing: WorkflowRunState | undefined,
  input: AdmitWorkflowResumeInput,
  now: number,
  materialize: MaterializeSnapshot,
): InternalResumeMutationResult<AdmitWorkflowResumeResult> {
  if (!existing) return { status: 'missing_run' };
  if (!isHash(input.resumeOperationHash)) return { status: 'operation_conflict' };
  if (existing.runId !== input.runId) return { status: 'fence_conflict' };
  const expectedFence = materializeWorkflowLifecycleFence(input);
  const operationReplayContext = materializeOperationReplayContext(input.operationReplayContext);
  if (!operationReplayContext) return { status: 'operation_conflict' };
  if (input.nextLifecycleResumeAttempt !== expectedFence.lifecycleResumeAttempt + 1) {
    return { status: 'fence_conflict' };
  }
  const admittedFence = materializeWorkflowLifecycleFence({
    ...expectedFence,
    lifecycleResumeAttempt: input.nextLifecycleResumeAttempt,
  });
  const retainedCheckpoint = existing.resumeCheckpoint
    ? materializeCheckpoint(existing.resumeCheckpoint, materialize)
    : undefined;
  if (
    existing.status === 'running' &&
    retainedCheckpoint?.runId === input.runId &&
    retainedCheckpoint.fenceHash === admittedFence.fenceHash &&
    fenceMatchesSnapshot(existing, admittedFence)
  ) {
    if (retainedCheckpoint.resumeOperationHash !== input.resumeOperationHash) {
      return { status: 'operation_conflict' };
    }
    if (canonicalJson(retainedCheckpoint.operationReplayContext) !== canonicalJson(operationReplayContext)) {
      return { status: 'operation_conflict' };
    }
    return { status: 'already_admitted', fenceHash: admittedFence.fenceHash, snapshot: materialize(existing) };
  }
  if (existing.status !== 'suspended' || !fenceMatchesSnapshot(existing, expectedFence)) {
    return { status: 'fence_conflict' };
  }
  if (existing.resumeCheckpoint) return { status: 'checkpoint_conflict' };
  const checkpoint = createCheckpoint(
    input.runId,
    admittedFence,
    input.resumeOperationHash,
    operationReplayContext,
    existing,
    materialize,
  );
  const snapshot = materialize({
    ...existing,
    resourceId: existing.resourceId ?? input.resourceId,
    requestContext: input.requestContext ?? existing.requestContext,
    status: 'running',
    result: undefined,
    error: undefined,
    executionGeneration: admittedFence.executionGeneration,
    lifecycleResumeAttempt: admittedFence.lifecycleResumeAttempt,
    lifecycleStepStates: admittedFence.lifecycleStepStates,
    resumeCheckpoint: checkpoint,
    resumeResultReceipt: undefined,
    resumeRollbackReceipt: undefined,
    timestamp: now,
  });
  return { status: 'admitted', fenceHash: admittedFence.fenceHash, snapshot };
}

export function rollbackWorkflowResumeRecord(
  existing: WorkflowRunState | undefined,
  input: RollbackWorkflowResumeInput,
  now: number,
  materialize: MaterializeSnapshot,
): InternalResumeMutationResult<RollbackWorkflowResumeResult> {
  if (!existing) return { status: 'missing_run' };
  if (!isHash(input.resumeOperationHash)) return { status: 'checkpoint_conflict' };
  const currentFence = materializeWorkflowLifecycleFence(input);
  const retainedRollback = materializeRollbackReceipt(existing.resumeRollbackReceipt);
  if (
    retainedRollback?.runId === input.runId &&
    retainedRollback.resumeOperationHash === input.resumeOperationHash &&
    retainedRollback.fenceHash === currentFence.fenceHash
  ) {
    return { status: 'already_rolled_back', snapshot: materialize(existing) };
  }
  const checkpoint = existing.resumeCheckpoint
    ? materializeCheckpoint(existing.resumeCheckpoint, materialize)
    : undefined;
  if (
    !checkpoint ||
    checkpoint.runId !== input.runId ||
    checkpoint.fenceHash !== currentFence.fenceHash ||
    checkpoint.resumeOperationHash !== input.resumeOperationHash
  ) {
    return { status: 'checkpoint_conflict' };
  }
  if (existing.status !== 'running' || !fenceMatchesSnapshot(existing, currentFence)) {
    return { status: 'fence_conflict' };
  }
  const restored = materialize(checkpoint.snapshot);
  const resourceId = existing.resourceId ?? restored.resourceId ?? input.resourceId;
  if (resourceId !== undefined) restored.resourceId = resourceId;
  delete restored.resumeCheckpoint;
  delete restored.resumeResultReceipt;
  const rollbackReceipt = {
    version: 1 as const,
    runId: input.runId,
    resumeOperationHash: input.resumeOperationHash,
    ...currentFence,
    rolledBackAt: now,
  };
  restored.resumeRollbackReceipt = { ...rollbackReceipt, rollbackHash: hash(rollbackReceipt) };
  return { status: 'rolled_back', snapshot: restored };
}

export function finalizeWorkflowResumeRecord(
  existing: WorkflowRunState | undefined,
  input: FinalizeWorkflowResumeInput,
  now: number,
  materialize: MaterializeSnapshot,
): InternalResumeMutationResult<FinalizeWorkflowResumeResult> {
  if (!existing) return { status: 'missing_run' };
  if (!isHash(input.resumeOperationHash)) return { status: 'receipt_conflict' };
  const finalFence = materializeWorkflowLifecycleFence(input);
  const retainedReceipt = existing.resumeResultReceipt ? materializeReceipt(existing.resumeResultReceipt) : undefined;
  if (retainedReceipt) {
    if (
      retainedReceipt.runId === input.runId &&
      retainedReceipt.receiptKey === input.receiptKey &&
      retainedReceipt.resumeOperationHash === input.resumeOperationHash &&
      retainedReceipt.fenceHash === finalFence.fenceHash
    ) {
      return { status: 'already_finalized', receipt: retainedReceipt, snapshot: materialize(existing) };
    }
    return { status: 'receipt_conflict' };
  }
  const checkpoint = existing.resumeCheckpoint
    ? materializeCheckpoint(existing.resumeCheckpoint, materialize)
    : undefined;
  if (!checkpoint || checkpoint.runId !== input.runId || checkpoint.resumeOperationHash !== input.resumeOperationHash) {
    return { status: 'checkpoint_conflict' };
  }
  if (
    checkpoint.executionGeneration !== finalFence.executionGeneration ||
    checkpoint.lifecycleResumeAttempt !== finalFence.lifecycleResumeAttempt
  ) {
    return { status: 'checkpoint_conflict' };
  }
  if (!fenceMatchesSnapshot(existing, finalFence)) return { status: 'fence_conflict' };
  if (typeof input.receiptKey !== 'string' || !input.receiptKey) return { status: 'receipt_conflict' };
  let receiptResult: FinalizeWorkflowResumeInput['result'];
  try {
    receiptResult = stableJsonValue(input.result) as FinalizeWorkflowResumeInput['result'];
  } catch {
    return { status: 'receipt_conflict' };
  }
  let receiptBase = {
    version: 1 as const,
    runId: input.runId,
    receiptKey: input.receiptKey,
    resumeOperationHash: input.resumeOperationHash,
    operationReplayContext: checkpoint.operationReplayContext,
    ...finalFence,
    result: receiptResult,
    createdAt: now,
  };
  let receiptBodyValue = receiptBody(receiptBase);
  const oversizedResult =
    Buffer.byteLength(canonicalJson(receiptBodyValue), 'utf8') > WORKFLOW_RESUME_RESULT_RECEIPT_MAX_BYTES;
  if (oversizedResult) {
    receiptBase = {
      ...receiptBase,
      result: {
        status: 'failed',
        steps: {},
        error: {
          name: 'WorkflowResumeResultTooLargeError',
          message: `Workflow resume result exceeded the ${WORKFLOW_RESUME_RESULT_RECEIPT_MAX_BYTES} byte receipt limit`,
        },
      },
    };
    receiptBodyValue = receiptBody(receiptBase);
  }
  const receipt: WorkflowResumeResultReceiptV1 = {
    ...receiptBase,
    receiptHash: hash(receiptBodyValue),
  };
  const suppliedSnapshot = stripResumeEvidence(input.snapshot, materialize);
  if (suppliedSnapshot.runId !== input.runId || !fenceMatchesSnapshot(suppliedSnapshot, finalFence)) {
    return { status: 'fence_conflict' };
  }
  const resourceId = existing.resourceId ?? checkpoint.snapshot.resourceId ?? input.resourceId;
  const extensions = extensionMetadata(checkpoint.snapshot);
  let snapshot: WorkflowRunState;
  if (input.shouldPersistSnapshot) {
    snapshot = {
      ...suppliedSnapshot,
      ...extensions,
      resourceId,
      resumeResultReceipt: receipt,
    };
    if (oversizedResult) {
      snapshot.status = 'failed';
      snapshot.result = undefined;
      snapshot.error = receipt.result.error;
    }
  } else {
    snapshot = {
      ...checkpoint.snapshot,
      ...extensions,
      resourceId,
      requestContext: suppliedSnapshot.requestContext ?? existing.requestContext ?? checkpoint.snapshot.requestContext,
      executionGeneration: finalFence.executionGeneration,
      lifecycleResumeAttempt: finalFence.lifecycleResumeAttempt,
      lifecycleStepStates: finalFence.lifecycleStepStates,
      resumeResultReceipt: receipt,
      timestamp: now,
    };
  }
  delete snapshot.resumeCheckpoint;
  delete snapshot.resumeRollbackReceipt;
  if (TERMINAL_STATUSES.has(snapshot.status)) {
    snapshot.suspendedPaths = {};
    snapshot.resumeLabels = {};
    delete snapshot.tracingContext;
  }
  return { status: 'finalized', receipt, snapshot: materialize(snapshot) };
}

export function consumeWorkflowResumeResultRecord(
  existing: WorkflowRunState | undefined,
  input: ConsumeWorkflowResumeResultInput,
  now: number,
  materialize: MaterializeSnapshot,
): InternalResumeMutationResult<ConsumeWorkflowResumeResult> {
  if (!existing) return { status: 'missing_run' };
  if (!isHash(input.resumeOperationHash)) return { status: 'receipt_conflict' };
  const receipt = existing.resumeResultReceipt ? materializeReceipt(existing.resumeResultReceipt) : undefined;
  if (!receipt) return { status: 'missing_receipt' };
  if (
    receipt.runId !== input.runId ||
    receipt.receiptKey !== input.receiptKey ||
    receipt.resumeOperationHash !== input.resumeOperationHash ||
    receipt.executionGeneration !== input.executionGeneration ||
    receipt.lifecycleResumeAttempt !== input.lifecycleResumeAttempt ||
    existing.executionGeneration !== receipt.executionGeneration ||
    existing.lifecycleResumeAttempt !== receipt.lifecycleResumeAttempt ||
    !fenceMatchesSnapshot(existing, receipt)
  ) {
    return { status: 'receipt_conflict' };
  }
  if (receipt.consumedBy !== undefined) {
    return receipt.consumedBy === input.consumerId
      ? { status: 'already_consumed', receipt, snapshot: materialize(existing) }
      : { status: 'receipt_conflict' };
  }
  const consumed = { ...receipt, consumedBy: input.consumerId, consumedAt: now };
  return {
    status: 'consumed',
    receipt: consumed,
    snapshot: materialize({ ...existing, resumeResultReceipt: consumed }),
  };
}
