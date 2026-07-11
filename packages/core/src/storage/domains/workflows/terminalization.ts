import { Buffer } from 'node:buffer';
import { createHash } from 'node:crypto';
import type {
  WorkflowTerminalEffectRecord,
  WorkflowTerminalizationClaimedRecord,
  WorkflowTerminalSnapshotRecord,
  WorkflowTerminalizationPhase,
  WorkflowTerminalizationRecord,
} from '../../../workflows';
import type {
  AdvanceWorkflowTerminalizationInput,
  ClaimWorkflowTerminalizationInput,
  GetWorkflowTerminalEffectForDispatchInput,
  PersistWorkflowTerminalStateInput,
  PrepareWorkflowTerminalEffectInput,
  WorkflowTerminalEffectDescriptor,
  ReleaseWorkflowTerminalizationInput,
  WorkflowTerminalEffectObservation,
  WorkflowTerminalizationObservation,
} from '../../types';

type InternalClaimWorkflowTerminalizationResult =
  | {
      status: 'acquired' | 'renewed';
      record: WorkflowTerminalizationClaimedRecord;
    }
  | {
      status: 'leased' | 'lease_expired' | 'fence_conflict' | 'terminal_conflict' | 'complete';
      record: WorkflowTerminalizationRecord;
    }
  | { status: 'missing_record' };

type InternalAdvanceWorkflowTerminalizationResult =
  | {
      status: 'advanced' | 'phase_conflict' | 'not_owner' | 'fence_conflict' | 'lease_expired' | 'complete';
      record: WorkflowTerminalizationRecord;
    }
  | { status: 'invalid_transition' | 'missing_record' };

type InternalReleaseWorkflowTerminalizationResult =
  | {
      status: 'released' | 'not_owner' | 'fence_conflict' | 'lease_expired' | 'complete';
      record: WorkflowTerminalizationRecord;
    }
  | { status: 'missing_record' };

type InternalPersistWorkflowTerminalStateResult =
  | {
      status: 'phase_conflict' | 'not_owner' | 'fence_conflict' | 'lease_expired' | 'complete';
      record: WorkflowTerminalizationRecord;
    }
  | {
      status: 'advanced';
      record: WorkflowTerminalizationRecord;
      snapshot: PersistWorkflowTerminalStateInput['snapshot'];
    }
  | { status: 'invalid_snapshot' | 'missing_record' };

type InternalPrepareWorkflowTerminalEffectResult =
  | {
      status: 'prepared' | 'already_prepared';
      record: WorkflowTerminalizationRecord;
      effect: WorkflowTerminalEffectRecord;
    }
  | { status: 'effect_conflict'; record: WorkflowTerminalizationRecord; effect: WorkflowTerminalEffectRecord }
  | {
      status: 'phase_conflict' | 'not_owner' | 'fence_conflict' | 'lease_expired' | 'complete';
      record: WorkflowTerminalizationRecord;
    }
  | { status: 'invalid_transition' | 'missing_effect' | 'missing_record' };

type InternalGetWorkflowTerminalEffectForDispatchResult =
  | { status: 'found'; effect: WorkflowTerminalEffectRecord }
  | {
      status: 'not_owner' | 'fence_conflict' | 'lease_expired' | 'complete';
      record: WorkflowTerminalizationRecord;
    }
  | { status: 'missing_effect' | 'missing_record' };

const NEXT_PHASES: Record<WorkflowTerminalizationPhase, readonly WorkflowTerminalizationPhase[]> = {
  terminalization_pending: [],
  run_state_persisted: [],
  parent_outbox_pending: [],
  parent_effect_recorded: [],
  finish_outbox_pending: [],
  finish_effect_recorded: ['complete'],
  complete: [],
};

export const MAX_WORKFLOW_TERMINALIZATION_LEASE_MS = 86_400_000;
export const MAX_WORKFLOW_TERMINAL_PARENT_EXECUTION_PATH_LENGTH = 256;

function isWellFormedUnicode(value: string): boolean {
  for (let index = 0; index < value.length; index += 1) {
    const codeUnit = value.charCodeAt(index);
    if (codeUnit >= 0xd800 && codeUnit <= 0xdbff) {
      const next = value.charCodeAt(index + 1);
      if (Number.isNaN(next) || next < 0xdc00 || next > 0xdfff) return false;
      index += 1;
    } else if (codeUnit >= 0xdc00 && codeUnit <= 0xdfff) {
      return false;
    }
  }
  return true;
}

export function validateWorkflowTerminalizationIdentity(value: string, field: string, maxLength: number): void {
  if (
    typeof value !== 'string' ||
    value.length === 0 ||
    value.length > maxLength ||
    value.includes('\0') ||
    !isWellFormedUnicode(value)
  ) {
    throw new TypeError(
      `${field} must be a well-formed non-empty string no longer than ${maxLength} characters without null characters`,
    );
  }
}

export function validateWorkflowTerminalizationLeaseMs(leaseMs: number): void {
  if (!Number.isSafeInteger(leaseMs) || leaseMs <= 0 || leaseMs > MAX_WORKFLOW_TERMINALIZATION_LEASE_MS) {
    throw new TypeError('leaseMs must be a positive safe integer no greater than 86400000');
  }
}

export function validateWorkflowTerminalizationClaim(input: ClaimWorkflowTerminalizationInput): void {
  validateWorkflowTerminalizationIdentity(input.eventKey, 'eventKey', 1024);
  validateWorkflowTerminalizationIdentity(input.ownerId, 'ownerId', 256);
  if (!['success', 'failed', 'canceled'].includes(input.terminalStatus)) {
    throw new TypeError('terminalStatus must be success, failed, or canceled');
  }
  validateWorkflowTerminalizationLeaseMs(input.leaseMs);
  if ((input.claimToken === undefined) !== (input.claimGeneration === undefined)) {
    throw new TypeError('claimToken and claimGeneration must be provided together');
  }
  if (input.claimToken !== undefined) {
    validateWorkflowTerminalizationIdentity(input.claimToken, 'claimToken', 256);
    validateWorkflowTerminalizationGeneration(input.claimGeneration!);
  }
}

export function validateWorkflowTerminalizationFence(input: {
  ownerId: string;
  claimToken: string;
  claimGeneration: number;
}): void {
  validateWorkflowTerminalizationIdentity(input.ownerId, 'ownerId', 256);
  validateWorkflowTerminalizationIdentity(input.claimToken, 'claimToken', 256);
  validateWorkflowTerminalizationGeneration(input.claimGeneration);
}

function validateWorkflowTerminalizationGeneration(generation: number): void {
  if (!Number.isSafeInteger(generation) || generation <= 0) {
    throw new TypeError('claimGeneration must be a positive safe integer');
  }
}

function validateWorkflowTerminalizationClock(now: number): void {
  if (!Number.isSafeInteger(now) || now < 0) {
    throw new TypeError('terminalization clock must be a non-negative safe integer');
  }
}

function getWorkflowTerminalizationLeaseExpiry(now: number, leaseMs: number): number {
  validateWorkflowTerminalizationClock(now);
  const expiresAt = now + leaseMs;
  if (!Number.isSafeInteger(expiresAt)) {
    throw new RangeError('terminalization lease expiry exceeds the safe integer range');
  }
  return expiresAt;
}

export function copyWorkflowTerminalizationRecord<TRecord extends WorkflowTerminalizationRecord>(
  record: TRecord,
): TRecord {
  return { ...record };
}

export function copyWorkflowTerminalEffectRecord(effect: WorkflowTerminalEffectRecord): WorkflowTerminalEffectRecord {
  return effect.kind === 'parent-workflow-step-end'
    ? { ...effect, parentExecutionPath: [...effect.parentExecutionPath] }
    : { ...effect };
}

export function observeWorkflowTerminalEffectRecord(
  effect: WorkflowTerminalEffectRecord,
): WorkflowTerminalEffectObservation {
  return {
    version: effect.version,
    effectKey: effect.effectKey,
    kind: effect.kind,
    payloadHash: effect.payloadHash,
    createdAt: effect.createdAt,
  };
}

/** Removes credentials that would let an observer impersonate the live claim owner. */
export function observeWorkflowTerminalizationRecord(
  record: WorkflowTerminalizationRecord,
): WorkflowTerminalizationObservation {
  return {
    version: record.version,
    eventKey: record.eventKey,
    terminalStatus: record.terminalStatus,
    phase: record.phase,
    leaseExpiresAt: record.leaseExpiresAt,
    createdAt: record.createdAt,
    updatedAt: record.updatedAt,
    completedAt: record.completedAt,
  };
}

/** @internal Applies claim semantics inside an adapter-provided atomic section. */
export function claimWorkflowTerminalizationRecord(
  existing: WorkflowTerminalizationRecord | undefined,
  input: ClaimWorkflowTerminalizationInput,
  now: number,
  newClaimToken: string,
): InternalClaimWorkflowTerminalizationResult {
  validateWorkflowTerminalizationClaim(input);
  validateWorkflowTerminalizationIdentity(newClaimToken, 'claimToken', 256);
  const leaseExpiresAt = getWorkflowTerminalizationLeaseExpiry(now, input.leaseMs);

  if (existing && (existing.eventKey !== input.eventKey || existing.terminalStatus !== input.terminalStatus)) {
    return { status: 'terminal_conflict', record: copyWorkflowTerminalizationRecord(existing) };
  }

  if (existing?.phase === 'complete') {
    return { status: 'complete', record: copyWorkflowTerminalizationRecord(existing) };
  }

  if (input.claimToken !== undefined) {
    if (!existing) return { status: 'missing_record' };
    if (
      existing.ownerId !== input.ownerId ||
      existing.claimToken !== input.claimToken ||
      existing.claimGeneration !== input.claimGeneration
    ) {
      return { status: 'fence_conflict', record: copyWorkflowTerminalizationRecord(existing) };
    }
    if ((existing.leaseExpiresAt ?? 0) <= now) {
      return { status: 'lease_expired', record: copyWorkflowTerminalizationRecord(existing) };
    }
    const renewed: WorkflowTerminalizationClaimedRecord = {
      ...existing,
      ownerId: existing.ownerId,
      claimToken: existing.claimToken,
      leaseExpiresAt,
      updatedAt: now,
    };
    return { status: 'renewed', record: copyWorkflowTerminalizationRecord(renewed) };
  }

  if (existing && (existing.leaseExpiresAt ?? 0) > now) {
    return { status: 'leased', record: copyWorkflowTerminalizationRecord(existing) };
  }

  if (existing && existing.claimGeneration >= Number.MAX_SAFE_INTEGER) {
    throw new RangeError('claimGeneration cannot be incremented safely');
  }

  const record: WorkflowTerminalizationClaimedRecord = existing
    ? {
        ...existing,
        ownerId: input.ownerId,
        claimToken: newClaimToken,
        claimGeneration: existing.claimGeneration + 1,
        leaseExpiresAt,
        updatedAt: now,
      }
    : {
        version: 1,
        eventKey: input.eventKey,
        terminalStatus: input.terminalStatus,
        phase: 'terminalization_pending',
        ownerId: input.ownerId,
        claimToken: newClaimToken,
        claimGeneration: 1,
        leaseExpiresAt,
        createdAt: now,
        updatedAt: now,
      };
  return { status: 'acquired', record: copyWorkflowTerminalizationRecord(record) };
}

/** @internal Applies phase CAS inside an adapter-provided atomic section. */
export function advanceWorkflowTerminalizationRecord(
  existing: WorkflowTerminalizationRecord | undefined,
  input: AdvanceWorkflowTerminalizationInput,
  now: number,
): InternalAdvanceWorkflowTerminalizationResult {
  if (input.leaseMs !== undefined) validateWorkflowTerminalizationLeaseMs(input.leaseMs);
  const leaseExpiresAt =
    input.leaseMs === undefined ? undefined : getWorkflowTerminalizationLeaseExpiry(now, input.leaseMs);
  const allowedPhases = Object.hasOwn(NEXT_PHASES, input.expectedPhase) ? NEXT_PHASES[input.expectedPhase] : undefined;
  if (!allowedPhases || !allowedPhases.includes(input.nextPhase)) return { status: 'invalid_transition' };
  const fence = checkLiveWorkflowTerminalizationFence(existing, input, now);
  if (fence.status !== 'ok') return fence;
  if (fence.record.phase !== input.expectedPhase) {
    return { status: 'phase_conflict', record: copyWorkflowTerminalizationRecord(fence.record) };
  }

  const complete = input.nextPhase === 'complete';
  const record: WorkflowTerminalizationRecord = {
    ...fence.record,
    phase: input.nextPhase,
    updatedAt: now,
    ...(leaseExpiresAt === undefined || complete ? {} : { leaseExpiresAt }),
    ...(complete ? { ownerId: undefined, claimToken: undefined, leaseExpiresAt: undefined, completedAt: now } : {}),
  };
  return { status: 'advanced', record: copyWorkflowTerminalizationRecord(record) };
}

function hashFramedParts(domain: string, parts: readonly string[]): string {
  const hash = createHash('sha256');
  for (const part of [domain, ...parts]) {
    const bytes = Buffer.from(part, 'utf8');
    const length = Buffer.allocUnsafe(4);
    length.writeUInt32BE(bytes.length);
    hash.update(length);
    hash.update(bytes);
  }
  return hash.digest('hex');
}

function getWorkflowTerminalEffectIntegrity(input: {
  version: 1;
  workflowName: string;
  runId: string;
  sourceEventKey: string;
  kind: WorkflowTerminalEffectRecord['kind'];
  terminalStatus: WorkflowTerminalEffectRecord['terminalStatus'];
  parentWorkflowName?: string;
  parentRunId?: string;
  parentStepId?: string;
  parentExecutionPath?: readonly number[];
}): { effectKey: string; payloadHash: string } {
  const destinationParts =
    input.kind === 'parent-workflow-step-end'
      ? [
          input.parentWorkflowName!,
          input.parentRunId!,
          input.parentStepId!,
          String(input.parentExecutionPath!.length),
          ...input.parentExecutionPath!.map(String),
        ]
      : [input.workflowName, input.runId];
  const identityParts = [
    String(input.version),
    input.workflowName,
    input.runId,
    input.sourceEventKey,
    input.kind,
    ...destinationParts,
  ];
  const payloadParts = [...identityParts, input.terminalStatus];
  return {
    effectKey: `wte:v1:${hashFramedParts('mastra.workflow-terminal-effect.identity.v1', identityParts)}`,
    payloadHash: `sha256:${hashFramedParts('mastra.workflow-terminal-effect.payload.v1', payloadParts)}`,
  };
}

/** @internal Fails closed when a persisted intent does not match its canonical framed identity. */
export function validateWorkflowTerminalEffectIntegrity(effect: WorkflowTerminalEffectRecord): void {
  const expected = getWorkflowTerminalEffectIntegrity(effect);
  if (effect.effectKey !== expected.effectKey || effect.payloadHash !== expected.payloadHash) {
    throw new TypeError('Invalid workflow terminal effect integrity');
  }
}

/** @internal Fails closed when a persisted intent is not evidence for its owning journal. */
export function validateWorkflowTerminalEffectJournalLink(
  effect: WorkflowTerminalEffectRecord,
  journal: WorkflowTerminalizationRecord,
  workflowName: string,
  runId: string,
): void {
  if (
    effect.workflowName !== workflowName ||
    effect.runId !== runId ||
    effect.sourceEventKey !== journal.eventKey ||
    effect.terminalStatus !== journal.terminalStatus ||
    effect.createdAt < journal.createdAt ||
    effect.createdAt > journal.updatedAt
  ) {
    throw new TypeError('Invalid workflow terminal effect journal link');
  }
}

/** @internal Fails closed when retained terminal state is not evidence for its owning journal. */
export function validateWorkflowTerminalSnapshotJournalLink(
  retained: WorkflowTerminalSnapshotRecord,
  journal: WorkflowTerminalizationRecord,
  workflowName: string,
  runId: string,
): void {
  if (
    retained.workflowName !== workflowName ||
    retained.runId !== runId ||
    retained.terminalStatus !== journal.terminalStatus ||
    retained.snapshot.runId !== runId ||
    retained.snapshot.status !== journal.terminalStatus ||
    retained.createdAt < journal.createdAt ||
    retained.createdAt > journal.updatedAt
  ) {
    throw new TypeError('Invalid workflow terminal snapshot journal link');
  }
}

function getValidatedWorkflowTerminalParentExecutionPath(value: unknown): number[] {
  if (!Array.isArray(value)) {
    throw new TypeError(
      `parentExecutionPath must contain 1-${MAX_WORKFLOW_TERMINAL_PARENT_EXECUTION_PATH_LENGTH} non-negative safe integers`,
    );
  }
  const descriptors = Object.getOwnPropertyDescriptors(value) as unknown as Record<PropertyKey, PropertyDescriptor>;
  const lengthDescriptor = descriptors.length;
  const length = lengthDescriptor && 'value' in lengthDescriptor ? lengthDescriptor.value : undefined;
  if (
    typeof length !== 'number' ||
    !Number.isSafeInteger(length) ||
    length === 0 ||
    length > MAX_WORKFLOW_TERMINAL_PARENT_EXECUTION_PATH_LENGTH
  ) {
    throw new TypeError(
      `parentExecutionPath must contain 1-${MAX_WORKFLOW_TERMINAL_PARENT_EXECUTION_PATH_LENGTH} non-negative safe integers`,
    );
  }
  const validKeys = Reflect.ownKeys(descriptors).every(key => {
    if (key === 'length') return true;
    if (typeof key !== 'string' || !/^(0|[1-9][0-9]*)$/.test(key)) return false;
    const index = Number(key);
    return index < length && 'value' in descriptors[key]!;
  });
  const path = Array.from({ length }, (_, index) => {
    const descriptor = descriptors[String(index)];
    return descriptor && 'value' in descriptor ? descriptor.value : undefined;
  });
  if (!validKeys || !path.every(item => Number.isSafeInteger(item) && item >= 0)) {
    throw new TypeError(
      `parentExecutionPath must contain 1-${MAX_WORKFLOW_TERMINAL_PARENT_EXECUTION_PATH_LENGTH} non-negative safe integers`,
    );
  }
  return path as number[];
}

export function materializeWorkflowTerminalEffectKind(value: unknown): WorkflowTerminalEffectRecord['kind'] {
  if (value !== 'parent-workflow-step-end' && value !== 'workflow-finish') {
    throw new TypeError('kind must be parent-workflow-step-end or workflow-finish');
  }
  return value;
}

/** @internal Copies an untrusted descriptor into bounded, data-only structural identity. */
export function materializeWorkflowTerminalEffectDescriptor(value: unknown): WorkflowTerminalEffectDescriptor {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new TypeError('effect must be a plain object');
  }
  const prototype = Object.getPrototypeOf(value);
  if (prototype !== Object.prototype && prototype !== null) {
    throw new TypeError('effect must be a plain object');
  }
  const descriptors = Object.getOwnPropertyDescriptors(value);
  const kindDescriptor = descriptors.kind;
  if (!kindDescriptor || !('value' in kindDescriptor)) {
    throw new TypeError('effect contains unknown or accessor fields');
  }
  const kind = materializeWorkflowTerminalEffectKind(kindDescriptor.value);
  const allowedKeys =
    kind === 'parent-workflow-step-end'
      ? new Set(['kind', 'parentWorkflowName', 'parentRunId', 'parentStepId', 'parentExecutionPath'])
      : new Set(['kind']);
  for (const key of Reflect.ownKeys(descriptors)) {
    if (typeof key !== 'string' || !allowedKeys.has(key) || !('value' in descriptors[key]!)) {
      throw new TypeError('effect contains unknown or accessor fields');
    }
  }
  if (kind === 'parent-workflow-step-end') {
    validateWorkflowTerminalizationIdentity(descriptors.parentWorkflowName?.value, 'parentWorkflowName', 512);
    validateWorkflowTerminalizationIdentity(descriptors.parentRunId?.value, 'parentRunId', 512);
    validateWorkflowTerminalizationIdentity(descriptors.parentStepId?.value, 'parentStepId', 512);
    return {
      kind,
      parentWorkflowName: descriptors.parentWorkflowName!.value,
      parentRunId: descriptors.parentRunId!.value,
      parentStepId: descriptors.parentStepId!.value,
      parentExecutionPath: getValidatedWorkflowTerminalParentExecutionPath(descriptors.parentExecutionPath?.value),
    };
  }
  return { kind };
}

function validateWorkflowTerminalEffectInput(
  input: PrepareWorkflowTerminalEffectInput,
): WorkflowTerminalEffectDescriptor {
  validateWorkflowTerminalizationFence(input);
  validateWorkflowTerminalizationIdentity(input.workflowName, 'workflowName', 512);
  validateWorkflowTerminalizationIdentity(input.runId, 'runId', 512);
  if (input.leaseMs !== undefined) validateWorkflowTerminalizationLeaseMs(input.leaseMs);
  return materializeWorkflowTerminalEffectDescriptor(input.effect);
}

export function createWorkflowTerminalEffectRecord(
  journal: WorkflowTerminalizationRecord,
  input: PrepareWorkflowTerminalEffectInput,
  now: number,
): WorkflowTerminalEffectRecord {
  const descriptor = validateWorkflowTerminalEffectInput(input);
  validateWorkflowTerminalizationClock(now);
  const common = {
    version: 1 as const,
    kind: descriptor.kind,
    workflowName: input.workflowName,
    runId: input.runId,
    sourceEventKey: journal.eventKey,
    terminalStatus: journal.terminalStatus,
    createdAt: now,
  };
  const effect =
    descriptor.kind === 'parent-workflow-step-end'
      ? {
          ...common,
          kind: descriptor.kind,
          parentWorkflowName: descriptor.parentWorkflowName,
          parentRunId: descriptor.parentRunId,
          parentStepId: descriptor.parentStepId,
          parentExecutionPath: descriptor.parentExecutionPath,
        }
      : { ...common, kind: descriptor.kind };
  return { ...effect, ...getWorkflowTerminalEffectIntegrity(effect) };
}

function sameWorkflowTerminalEffect(left: WorkflowTerminalEffectRecord, right: WorkflowTerminalEffectRecord): boolean {
  if (
    left.version !== right.version ||
    left.effectKey !== right.effectKey ||
    left.kind !== right.kind ||
    left.workflowName !== right.workflowName ||
    left.runId !== right.runId ||
    left.sourceEventKey !== right.sourceEventKey ||
    left.terminalStatus !== right.terminalStatus ||
    left.payloadHash !== right.payloadHash
  ) {
    return false;
  }
  if (left.kind === 'parent-workflow-step-end' && right.kind === 'parent-workflow-step-end') {
    return (
      left.parentWorkflowName === right.parentWorkflowName &&
      left.parentRunId === right.parentRunId &&
      left.parentStepId === right.parentStepId &&
      left.parentExecutionPath.length === right.parentExecutionPath.length &&
      left.parentExecutionPath.every((value, index) => value === right.parentExecutionPath[index])
    );
  }
  return left.kind === 'workflow-finish' && right.kind === 'workflow-finish';
}

function checkLiveWorkflowTerminalizationFence(
  existing: WorkflowTerminalizationRecord | undefined,
  input: { ownerId: string; claimToken: string; claimGeneration: number },
  now: number,
):
  | { status: 'ok'; record: WorkflowTerminalizationRecord }
  | {
      status: 'not_owner' | 'fence_conflict' | 'lease_expired' | 'complete';
      record: WorkflowTerminalizationRecord;
    }
  | { status: 'missing_record' } {
  validateWorkflowTerminalizationFence(input);
  validateWorkflowTerminalizationClock(now);
  if (!existing) return { status: 'missing_record' };
  if (existing.phase === 'complete') return { status: 'complete', record: copyWorkflowTerminalizationRecord(existing) };
  if (existing.ownerId !== input.ownerId) {
    return { status: 'not_owner', record: copyWorkflowTerminalizationRecord(existing) };
  }
  if (existing.claimToken !== input.claimToken || existing.claimGeneration !== input.claimGeneration) {
    return { status: 'fence_conflict', record: copyWorkflowTerminalizationRecord(existing) };
  }
  if ((existing.leaseExpiresAt ?? 0) <= now) {
    return { status: 'lease_expired', record: copyWorkflowTerminalizationRecord(existing) };
  }
  return { status: 'ok', record: copyWorkflowTerminalizationRecord(existing) };
}

export function prepareWorkflowTerminalEffectRecord(
  existingJournal: WorkflowTerminalizationRecord | undefined,
  existingEffect: WorkflowTerminalEffectRecord | undefined,
  input: PrepareWorkflowTerminalEffectInput,
  now: number,
): InternalPrepareWorkflowTerminalEffectResult {
  validateWorkflowTerminalEffectInput(input);
  const fence = checkLiveWorkflowTerminalizationFence(existingJournal, input, now);
  if (fence.status !== 'ok') return fence;

  const targetPhase =
    input.effect.kind === 'parent-workflow-step-end' ? 'parent_outbox_pending' : 'finish_outbox_pending';
  const validTransition =
    (input.effect.kind === 'parent-workflow-step-end' && input.expectedPhase === 'run_state_persisted') ||
    (input.effect.kind === 'workflow-finish' &&
      (input.expectedPhase === 'run_state_persisted' || input.expectedPhase === 'parent_effect_recorded'));
  if (!validTransition) return { status: 'invalid_transition' };

  const desired = createWorkflowTerminalEffectRecord(fence.record, input, now);
  if (fence.record.phase === targetPhase) {
    if (!existingEffect) return { status: 'missing_effect' };
    return sameWorkflowTerminalEffect(existingEffect, desired)
      ? {
          status: 'already_prepared',
          record: copyWorkflowTerminalizationRecord(fence.record),
          effect: copyWorkflowTerminalEffectRecord(existingEffect),
        }
      : {
          status: 'effect_conflict',
          record: copyWorkflowTerminalizationRecord(fence.record),
          effect: copyWorkflowTerminalEffectRecord(existingEffect),
        };
  }
  if (fence.record.phase !== input.expectedPhase) {
    return { status: 'phase_conflict', record: copyWorkflowTerminalizationRecord(fence.record) };
  }
  if (existingEffect) {
    return {
      status: 'effect_conflict',
      record: copyWorkflowTerminalizationRecord(fence.record),
      effect: copyWorkflowTerminalEffectRecord(existingEffect),
    };
  }

  const leaseExpiresAt =
    input.leaseMs === undefined ? undefined : getWorkflowTerminalizationLeaseExpiry(now, input.leaseMs);
  const record: WorkflowTerminalizationRecord = {
    ...fence.record,
    phase: targetPhase,
    updatedAt: now,
    ...(leaseExpiresAt === undefined ? {} : { leaseExpiresAt }),
  };
  return {
    status: 'prepared',
    record: copyWorkflowTerminalizationRecord(record),
    effect: copyWorkflowTerminalEffectRecord(desired),
  };
}

export function getWorkflowTerminalEffectForDispatchRecord(
  existingJournal: WorkflowTerminalizationRecord | undefined,
  existingEffect: WorkflowTerminalEffectRecord | undefined,
  input: GetWorkflowTerminalEffectForDispatchInput,
  now: number,
): InternalGetWorkflowTerminalEffectForDispatchResult {
  materializeWorkflowTerminalEffectKind(input.kind);
  const fence = checkLiveWorkflowTerminalizationFence(existingJournal, input, now);
  if (fence.status !== 'ok') return fence;
  if (!existingEffect) return { status: 'missing_effect' };
  return { status: 'found', effect: copyWorkflowTerminalEffectRecord(existingEffect) };
}

/** @internal Atomically certifies that the canonical run snapshot is terminal. */
export function persistWorkflowTerminalStateRecord(
  existing: WorkflowTerminalizationRecord | undefined,
  input: PersistWorkflowTerminalStateInput,
  now: number,
  materializeSnapshot: (
    snapshot: PersistWorkflowTerminalStateInput['snapshot'],
  ) => PersistWorkflowTerminalStateInput['snapshot'],
): InternalPersistWorkflowTerminalStateResult {
  validateWorkflowTerminalizationFence(input);
  validateWorkflowTerminalizationClock(now);
  if (input.leaseMs !== undefined) validateWorkflowTerminalizationLeaseMs(input.leaseMs);
  if (!existing) return { status: 'missing_record' };
  if (existing.phase === 'complete') return { status: 'complete', record: copyWorkflowTerminalizationRecord(existing) };
  if (existing.ownerId !== input.ownerId) {
    return { status: 'not_owner', record: copyWorkflowTerminalizationRecord(existing) };
  }
  if (existing.claimToken !== input.claimToken || existing.claimGeneration !== input.claimGeneration) {
    return { status: 'fence_conflict', record: copyWorkflowTerminalizationRecord(existing) };
  }
  if ((existing.leaseExpiresAt ?? 0) <= now) {
    return { status: 'lease_expired', record: copyWorkflowTerminalizationRecord(existing) };
  }
  if (existing.phase !== 'terminalization_pending') {
    return { status: 'phase_conflict', record: copyWorkflowTerminalizationRecord(existing) };
  }
  if (!input.snapshot || typeof input.snapshot !== 'object' || Array.isArray(input.snapshot)) {
    return { status: 'invalid_snapshot' };
  }
  const snapshot = materializeSnapshot(input.snapshot);
  if (!snapshot || snapshot.runId !== input.runId || snapshot.status !== existing.terminalStatus) {
    return { status: 'invalid_snapshot' };
  }
  const leaseExpiresAt =
    input.leaseMs === undefined ? undefined : getWorkflowTerminalizationLeaseExpiry(now, input.leaseMs);
  return {
    status: 'advanced',
    snapshot,
    record: {
      ...existing,
      phase: 'run_state_persisted',
      updatedAt: now,
      ...(leaseExpiresAt === undefined ? {} : { leaseExpiresAt }),
    },
  };
}

/** @internal Releases a live fenced claim inside an adapter-provided atomic section. */
export function releaseWorkflowTerminalizationRecord(
  existing: WorkflowTerminalizationRecord | undefined,
  input: ReleaseWorkflowTerminalizationInput,
  now: number,
): InternalReleaseWorkflowTerminalizationResult {
  validateWorkflowTerminalizationFence(input);
  validateWorkflowTerminalizationClock(now);
  if (!existing) return { status: 'missing_record' };
  if (existing.phase === 'complete') return { status: 'complete', record: copyWorkflowTerminalizationRecord(existing) };
  if (existing.ownerId !== input.ownerId) {
    return { status: 'not_owner', record: copyWorkflowTerminalizationRecord(existing) };
  }
  if (existing.claimToken !== input.claimToken || existing.claimGeneration !== input.claimGeneration) {
    return { status: 'fence_conflict', record: copyWorkflowTerminalizationRecord(existing) };
  }
  if ((existing.leaseExpiresAt ?? 0) <= now) {
    return { status: 'lease_expired', record: copyWorkflowTerminalizationRecord(existing) };
  }

  const record: WorkflowTerminalizationRecord = {
    ...existing,
    ownerId: undefined,
    claimToken: undefined,
    leaseExpiresAt: undefined,
    updatedAt: now,
  };
  return { status: 'released', record: copyWorkflowTerminalizationRecord(record) };
}
