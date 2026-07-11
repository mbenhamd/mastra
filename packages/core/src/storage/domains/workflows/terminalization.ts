import { Buffer } from 'node:buffer';
import { createHash } from 'node:crypto';
import type {
  WorkflowTerminalDestinationReceiptRecord,
  WorkflowTerminalEffectRecord,
  WorkflowTerminalizationClaimedRecord,
  WorkflowTerminalSnapshotRecord,
  WorkflowTerminalizationPhase,
  WorkflowTerminalizationRecord,
} from '../../../workflows';
import {
  copyWorkflowTerminalParentContinuationContract,
  getWorkflowTerminalEffectIntegrity,
  validateWorkflowTerminalEffectIntegrity,
  validateWorkflowTerminalParentContinuationIntegrity,
} from '../../../workflows/terminal-continuation';
export {
  applyWorkflowTerminalParentContinuationPatch,
  copyWorkflowTerminalParentContinuationContract,
  createWorkflowTerminalGraphFingerprint,
  createWorkflowTerminalParentContinuationContract,
  validateWorkflowTerminalEffectIntegrity,
} from '../../../workflows/terminal-continuation';
import type {
  AdvanceWorkflowTerminalizationInput,
  ApplyWorkflowTerminalParentEffectInput,
  ClaimWorkflowTerminalizationInput,
  GetWorkflowTerminalEffectForDispatchInput,
  GetWorkflowTerminalDestinationReceiptInput,
  GetWorkflowTerminalContinuationPlanInput,
  PersistWorkflowTerminalStateInput,
  PrepareWorkflowTerminalEffectInput,
  WorkflowTerminalEffectDescriptor,
  ReleaseWorkflowTerminalizationInput,
  ReserveWorkflowTerminalDestinationReceiptInput,
  WorkflowTerminalContinuationPlanObservation,
  WorkflowTerminalContinuationPlanRecord,
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

type InternalReserveWorkflowTerminalDestinationReceiptResult =
  | {
      status: 'reserved' | 'already_exists';
      receipt: WorkflowTerminalDestinationReceiptRecord;
    }
  | {
      status: 'not_owner' | 'fence_conflict' | 'lease_expired' | 'complete';
      record: WorkflowTerminalizationRecord;
    }
  | { status: 'consumer_limit_reached' | 'missing_effect' | 'missing_record' };

type InternalGetWorkflowTerminalDestinationReceiptResult =
  | { status: 'found'; receipt: WorkflowTerminalDestinationReceiptRecord }
  | {
      status: 'not_owner' | 'fence_conflict' | 'lease_expired' | 'complete';
      record: WorkflowTerminalizationRecord;
    }
  | { status: 'missing_receipt' | 'missing_effect' | 'missing_record' };

export type InternalPrepareWorkflowTerminalParentApplicationResult =
  | {
      status: 'ready';
      journal: WorkflowTerminalizationRecord;
      receipt: WorkflowTerminalDestinationReceiptRecord;
      plan: WorkflowTerminalContinuationPlanRecord;
    }
  | {
      status: 'already_applied' | 'already_quarantined';
      journal: WorkflowTerminalizationRecord;
      receipt: WorkflowTerminalDestinationReceiptRecord;
      plan: WorkflowTerminalContinuationPlanRecord;
    }
  | {
      status: 'contract_conflict';
      plan: WorkflowTerminalContinuationPlanRecord;
    }
  | {
      status: 'phase_conflict' | 'not_owner' | 'fence_conflict' | 'lease_expired' | 'complete';
      record: WorkflowTerminalizationRecord;
    }
  | { status: 'invalid_contract' | 'missing_effect' | 'missing_record' };

export interface WorkflowTerminalParentApplicationFinalRecords {
  journal: WorkflowTerminalizationRecord;
  receipt: WorkflowTerminalDestinationReceiptRecord;
  plan: WorkflowTerminalContinuationPlanRecord;
  status: 'applied' | 'quarantined';
}

export type InternalGetWorkflowTerminalContinuationPlanResult =
  | {
      status: 'found';
      plan: WorkflowTerminalContinuationPlanRecord;
      applicationState: 'applied' | 'quarantined';
      dispatchState: 'none' | 'pending' | 'destination_applied';
    }
  | {
      status: 'not_owner' | 'fence_conflict' | 'lease_expired' | 'complete';
      record: WorkflowTerminalizationRecord;
    }
  | { status: 'missing_effect' | 'missing_receipt' | 'missing_plan' | 'missing_record' };

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
export const MAX_WORKFLOW_TERMINAL_DESTINATION_RECEIPTS_PER_EFFECT = 8;

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

export function validateWorkflowTerminalizationRunIdentity(input: { workflowName: string; runId: string }): void {
  validateWorkflowTerminalizationIdentity(input.workflowName, 'workflowName', 512);
  validateWorkflowTerminalizationIdentity(input.runId, 'runId', 512);
}

export function validateWorkflowTerminalizationClaim(input: ClaimWorkflowTerminalizationInput): void {
  validateWorkflowTerminalizationRunIdentity(input);
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

export function copyWorkflowTerminalDestinationReceiptRecord(
  receipt: WorkflowTerminalDestinationReceiptRecord,
): WorkflowTerminalDestinationReceiptRecord {
  return { ...receipt };
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

/** @internal Fails closed when a persisted intent is not evidence for its owning journal. */
export function validateWorkflowTerminalEffectJournalLink(
  effect: WorkflowTerminalEffectRecord,
  journal: WorkflowTerminalizationRecord,
  workflowName: string,
  runId: string,
): void {
  if (
    ![effect.createdAt, journal.createdAt, journal.updatedAt].every(
      value => Number.isSafeInteger(value) && value >= 0,
    ) ||
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
    ![retained.createdAt, journal.createdAt, journal.updatedAt].every(
      value => Number.isSafeInteger(value) && value >= 0,
    ) ||
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
  return path.map(item => (item === 0 ? 0 : item)) as number[];
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
  validateWorkflowTerminalizationRunIdentity(input);
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
    const leaseExpiresAt =
      input.leaseMs === undefined ? undefined : getWorkflowTerminalizationLeaseExpiry(now, input.leaseMs);
    const record = leaseExpiresAt === undefined ? fence.record : { ...fence.record, leaseExpiresAt, updatedAt: now };
    return sameWorkflowTerminalEffect(existingEffect, desired)
      ? {
          status: 'already_prepared',
          record: copyWorkflowTerminalizationRecord(record),
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

function getWorkflowTerminalEffectDestinationHash(effect: WorkflowTerminalEffectRecord): string {
  const destination =
    effect.kind === 'parent-workflow-step-end'
      ? [
          effect.parentWorkflowName,
          effect.parentRunId,
          effect.parentStepId,
          String(effect.parentExecutionPath.length),
          ...effect.parentExecutionPath.map(String),
        ]
      : [effect.workflowName, effect.runId];
  return `sha256:${hashFramedParts('mastra.workflow-terminal-destination.v1', [effect.kind, ...destination])}`;
}

export function createWorkflowTerminalDestinationReceiptRecord(
  effect: WorkflowTerminalEffectRecord,
  consumerId: string,
  now: number,
): WorkflowTerminalDestinationReceiptRecord {
  validateWorkflowTerminalEffectIntegrity(effect);
  validateWorkflowTerminalizationIdentity(consumerId, 'consumerId', 256);
  validateWorkflowTerminalizationClock(now);
  if (!Number.isSafeInteger(effect.createdAt) || effect.createdAt < 0) {
    throw new TypeError('Invalid workflow terminal effect createdAt');
  }
  if (now < effect.createdAt) {
    throw new TypeError('workflow terminal destination receipt cannot predate its producer effect');
  }
  return {
    version: 1,
    receiptKey: `wtr:v1:${hashFramedParts('mastra.workflow-terminal-receipt.identity.v1', [
      '1',
      effect.effectKey,
      consumerId,
    ])}`,
    workflowName: effect.workflowName,
    runId: effect.runId,
    effectKey: effect.effectKey,
    consumerId,
    effectKind: effect.kind,
    producerPayloadHash: effect.payloadHash,
    destinationHash: getWorkflowTerminalEffectDestinationHash(effect),
    applicationState: 'reserved',
    dispatchState: 'none',
    createdAt: now,
    updatedAt: now,
  };
}

export function validateWorkflowTerminalDestinationReceiptIntegrity(
  receipt: WorkflowTerminalDestinationReceiptRecord,
  effect: WorkflowTerminalEffectRecord,
  now: number,
): void {
  validateWorkflowTerminalizationClock(now);
  const expected = createWorkflowTerminalDestinationReceiptRecord(effect, receipt.consumerId, effect.createdAt);
  const validApplication = ['reserved', 'applied', 'quarantined'].includes(receipt.applicationState);
  const validDispatch = ['none', 'pending', 'destination_applied'].includes(receipt.dispatchState);
  const validState =
    (receipt.applicationState === 'reserved' &&
      receipt.dispatchState === 'none' &&
      receipt.updatedAt === receipt.createdAt &&
      receipt.appliedAt === undefined &&
      receipt.dispatchPendingAt === undefined &&
      receipt.destinationAppliedAt === undefined &&
      receipt.quarantinedAt === undefined) ||
    (receipt.applicationState === 'applied' &&
      receipt.appliedAt !== undefined &&
      receipt.quarantinedAt === undefined &&
      ((receipt.dispatchState === 'none' &&
        receipt.updatedAt === receipt.appliedAt &&
        receipt.dispatchPendingAt === undefined &&
        receipt.destinationAppliedAt === undefined) ||
        (receipt.dispatchState === 'pending' &&
          receipt.dispatchPendingAt !== undefined &&
          receipt.appliedAt <= receipt.dispatchPendingAt &&
          receipt.updatedAt === receipt.dispatchPendingAt &&
          receipt.destinationAppliedAt === undefined) ||
        (receipt.dispatchState === 'destination_applied' &&
          receipt.dispatchPendingAt !== undefined &&
          receipt.destinationAppliedAt !== undefined &&
          receipt.appliedAt <= receipt.dispatchPendingAt &&
          receipt.dispatchPendingAt <= receipt.destinationAppliedAt &&
          receipt.updatedAt === receipt.destinationAppliedAt))) ||
    (receipt.applicationState === 'quarantined' &&
      receipt.dispatchState === 'none' &&
      receipt.appliedAt === undefined &&
      receipt.dispatchPendingAt === undefined &&
      receipt.destinationAppliedAt === undefined &&
      receipt.quarantinedAt !== undefined &&
      receipt.updatedAt === receipt.quarantinedAt);
  const timestamps = [
    receipt.appliedAt,
    receipt.dispatchPendingAt,
    receipt.destinationAppliedAt,
    receipt.quarantinedAt,
  ].filter((value): value is number => value !== undefined);
  if (
    receipt.version !== 1 ||
    receipt.receiptKey !== expected.receiptKey ||
    receipt.workflowName !== effect.workflowName ||
    receipt.runId !== effect.runId ||
    receipt.effectKey !== effect.effectKey ||
    receipt.effectKind !== effect.kind ||
    receipt.producerPayloadHash !== effect.payloadHash ||
    receipt.destinationHash !== expected.destinationHash ||
    !Number.isSafeInteger(receipt.createdAt) ||
    receipt.createdAt < effect.createdAt ||
    receipt.createdAt > now ||
    !validApplication ||
    !validDispatch ||
    !validState ||
    !Number.isSafeInteger(receipt.updatedAt) ||
    receipt.updatedAt < receipt.createdAt ||
    receipt.updatedAt > now ||
    timestamps.some(value => !Number.isSafeInteger(value) || value < receipt.createdAt || value > receipt.updatedAt)
  ) {
    throw new TypeError('Invalid workflow terminal destination receipt integrity');
  }
}

function sameWorkflowTerminalDestinationReceipt(
  left: WorkflowTerminalDestinationReceiptRecord,
  right: WorkflowTerminalDestinationReceiptRecord,
): boolean {
  return (
    left.version === right.version &&
    left.receiptKey === right.receiptKey &&
    left.workflowName === right.workflowName &&
    left.runId === right.runId &&
    left.effectKey === right.effectKey &&
    left.consumerId === right.consumerId &&
    left.effectKind === right.effectKind &&
    left.producerPayloadHash === right.producerPayloadHash &&
    left.destinationHash === right.destinationHash
  );
}

export function reserveWorkflowTerminalDestinationReceiptRecord(
  journal: WorkflowTerminalizationRecord | undefined,
  effect: WorkflowTerminalEffectRecord | undefined,
  existingReceipt: WorkflowTerminalDestinationReceiptRecord | undefined,
  existingReceiptCount: number,
  input: ReserveWorkflowTerminalDestinationReceiptInput,
  now: number,
): InternalReserveWorkflowTerminalDestinationReceiptResult {
  validateWorkflowTerminalizationIdentity(input.consumerId, 'consumerId', 256);
  materializeWorkflowTerminalEffectKind(input.effectKind);
  const fence = checkLiveWorkflowTerminalizationFence(journal, input, now);
  if (fence.status !== 'ok') return fence;
  if (!effect || effect.kind !== input.effectKind) return { status: 'missing_effect' };
  const desired = createWorkflowTerminalDestinationReceiptRecord(effect, input.consumerId, now);
  if (!existingReceipt) {
    if (
      !Number.isSafeInteger(existingReceiptCount) ||
      existingReceiptCount < 0 ||
      existingReceiptCount >= MAX_WORKFLOW_TERMINAL_DESTINATION_RECEIPTS_PER_EFFECT
    ) {
      return { status: 'consumer_limit_reached' };
    }
    return { status: 'reserved', receipt: desired };
  }
  validateWorkflowTerminalDestinationReceiptIntegrity(existingReceipt, effect, now);
  if (!sameWorkflowTerminalDestinationReceipt(existingReceipt, desired)) {
    throw new TypeError('Conflicting workflow terminal destination receipt identity');
  }
  return { status: 'already_exists', receipt: copyWorkflowTerminalDestinationReceiptRecord(existingReceipt) };
}

export function getWorkflowTerminalDestinationReceiptRecord(
  journal: WorkflowTerminalizationRecord | undefined,
  effect: WorkflowTerminalEffectRecord | undefined,
  receipt: WorkflowTerminalDestinationReceiptRecord | undefined,
  input: GetWorkflowTerminalDestinationReceiptInput,
  now: number,
): InternalGetWorkflowTerminalDestinationReceiptResult {
  validateWorkflowTerminalizationIdentity(input.consumerId, 'consumerId', 256);
  materializeWorkflowTerminalEffectKind(input.effectKind);
  const fence = checkLiveWorkflowTerminalizationFence(journal, input, now);
  if (fence.status !== 'ok') return fence;
  if (!effect || effect.kind !== input.effectKind) return { status: 'missing_effect' };
  if (!receipt) return { status: 'missing_receipt' };
  validateWorkflowTerminalDestinationReceiptIntegrity(receipt, effect, now);
  if (receipt.consumerId !== input.consumerId || receipt.effectKind !== input.effectKind) {
    throw new TypeError('Conflicting workflow terminal destination receipt identity');
  }
  return { status: 'found', receipt: copyWorkflowTerminalDestinationReceiptRecord(receipt) };
}

/** @internal The only receipt consumer allowed to mutate a parent workflow. */
export const WORKFLOW_TERMINAL_PARENT_APPLICATION_CONSUMER_ID = 'mastra.parent-application.v1' as const;

function workflowTerminalActionNeedsDispatch(
  action: ApplyWorkflowTerminalParentEffectInput['contract']['action'],
): boolean {
  return action.kind !== 'wait' && action.kind !== 'noop' && action.kind !== 'quarantine';
}

export function createWorkflowTerminalContinuationPlanRecord(
  effect: WorkflowTerminalEffectRecord,
  receipt: WorkflowTerminalDestinationReceiptRecord,
  parentRevision: string,
  contractInput: ApplyWorkflowTerminalParentEffectInput['contract'],
  now: number,
): WorkflowTerminalContinuationPlanRecord {
  validateWorkflowTerminalEffectIntegrity(effect);
  if (effect.kind !== 'parent-workflow-step-end') {
    throw new TypeError('parent continuation plans require a parent-workflow-step-end effect');
  }
  validateWorkflowTerminalDestinationReceiptIntegrity(receipt, effect, now);
  validateWorkflowTerminalizationIdentity(parentRevision, 'parentRevision', 256);
  validateWorkflowTerminalizationClock(now);
  const contract = copyWorkflowTerminalParentContinuationContract(contractInput);
  validateWorkflowTerminalParentContinuationIntegrity(contract);
  if (
    receipt.consumerId !== WORKFLOW_TERMINAL_PARENT_APPLICATION_CONSUMER_ID ||
    receipt.effectKey !== effect.effectKey ||
    contract.terminalEffectKey !== effect.effectKey ||
    contract.terminalEffectPayloadHash !== effect.payloadHash ||
    contract.childTerminalStatus !== effect.terminalStatus ||
    contract.expectedParentRevision !== parentRevision ||
    contract.executionMode !== 'continuous' ||
    now < receipt.createdAt ||
    now < effect.createdAt
  ) {
    throw new TypeError('Invalid parent continuation contract binding');
  }

  const parts = [
    '1',
    effect.effectKey,
    receipt.receiptKey,
    WORKFLOW_TERMINAL_PARENT_APPLICATION_CONSUMER_ID,
    effect.parentWorkflowName,
    effect.parentRunId,
    parentRevision,
    contract.contractHash,
  ];
  const planKey = `wtp:v1:${hashFramedParts('mastra.workflow-terminal-continuation-plan.identity.v1', parts)}`;
  const planHash = `sha256:${hashFramedParts('mastra.workflow-terminal-continuation-plan.payload.v1', [
    ...parts,
    String(now),
  ])}`;
  const frameworkActionKey = workflowTerminalActionNeedsDispatch(contract.action)
    ? `wta:v1:${hashFramedParts('mastra.workflow-terminal-framework-action.identity.v1', [
        planKey,
        contract.contractHash,
        contract.action.kind,
        contract.action.reason,
      ])}`
    : undefined;

  return {
    version: 1,
    planKey,
    planHash,
    receiptKey: receipt.receiptKey,
    effectKey: effect.effectKey,
    consumerId: WORKFLOW_TERMINAL_PARENT_APPLICATION_CONSUMER_ID,
    workflowName: effect.workflowName,
    runId: effect.runId,
    parentWorkflowName: effect.parentWorkflowName,
    parentRunId: effect.parentRunId,
    parentRevision,
    contract,
    ...(frameworkActionKey === undefined ? {} : { frameworkActionKey }),
    createdAt: now,
  };
}

export function copyWorkflowTerminalContinuationPlanRecord(
  plan: WorkflowTerminalContinuationPlanRecord,
): WorkflowTerminalContinuationPlanRecord {
  return {
    ...plan,
    contract: copyWorkflowTerminalParentContinuationContract(plan.contract),
  };
}

export function observeWorkflowTerminalContinuationPlanRecord(
  plan: WorkflowTerminalContinuationPlanRecord,
): WorkflowTerminalContinuationPlanObservation {
  return {
    version: 1,
    planKey: plan.planKey,
    planHash: plan.planHash,
    contractHash: plan.contract.contractHash,
    actionKind: plan.contract.action.kind,
    actionReason: plan.contract.action.reason,
    createdAt: plan.createdAt,
  };
}

export function validateWorkflowTerminalContinuationPlanIntegrity(
  plan: WorkflowTerminalContinuationPlanRecord,
  effect: WorkflowTerminalEffectRecord,
  receipt: WorkflowTerminalDestinationReceiptRecord,
  now: number,
): void {
  const expected = createWorkflowTerminalContinuationPlanRecord(
    effect,
    receipt,
    plan.parentRevision,
    plan.contract,
    plan.createdAt,
  );
  if (
    plan.version !== expected.version ||
    plan.planKey !== expected.planKey ||
    plan.planHash !== expected.planHash ||
    plan.receiptKey !== expected.receiptKey ||
    plan.effectKey !== expected.effectKey ||
    plan.consumerId !== expected.consumerId ||
    plan.workflowName !== expected.workflowName ||
    plan.runId !== expected.runId ||
    plan.parentWorkflowName !== expected.parentWorkflowName ||
    plan.parentRunId !== expected.parentRunId ||
    plan.parentRevision !== expected.parentRevision ||
    plan.contract.contractHash !== expected.contract.contractHash ||
    plan.frameworkActionKey !== expected.frameworkActionKey ||
    !Number.isSafeInteger(plan.createdAt) ||
    plan.createdAt < effect.createdAt ||
    plan.createdAt < receipt.createdAt ||
    plan.createdAt > now
  ) {
    throw new TypeError('Invalid workflow terminal continuation plan integrity');
  }
}

function sameWorkflowTerminalContinuationPlan(
  left: WorkflowTerminalContinuationPlanRecord,
  right: WorkflowTerminalContinuationPlanRecord,
): boolean {
  return (
    left.planKey === right.planKey &&
    left.planHash === right.planHash &&
    left.contract.contractHash === right.contract.contractHash
  );
}

export function getWorkflowTerminalContinuationPlanRecord(
  journal: WorkflowTerminalizationRecord | undefined,
  effect: WorkflowTerminalEffectRecord | undefined,
  receipt: WorkflowTerminalDestinationReceiptRecord | undefined,
  plan: WorkflowTerminalContinuationPlanRecord | undefined,
  input: GetWorkflowTerminalContinuationPlanInput,
  now: number,
): InternalGetWorkflowTerminalContinuationPlanResult {
  if (effect) {
    validateWorkflowTerminalEffectIntegrity(effect);
    if (!journal) throw new TypeError('Workflow terminal effect exists without its journal');
    validateWorkflowTerminalEffectJournalLink(effect, journal, input.workflowName, input.runId);
  }
  if (receipt) {
    if (!effect) throw new TypeError('Workflow terminal receipt exists without its effect');
    validateWorkflowTerminalDestinationReceiptIntegrity(receipt, effect, now);
  }
  if (plan) {
    if (!effect || !receipt) throw new TypeError('Workflow terminal continuation exists without receipt evidence');
    validateWorkflowTerminalContinuationPlanIntegrity(plan, effect, receipt, now);
  }
  const fence = checkLiveWorkflowTerminalizationFence(journal, input, now);
  if (fence.status !== 'ok') return fence;
  if (!effect || effect.kind !== 'parent-workflow-step-end') return { status: 'missing_effect' };
  if (!receipt) return { status: 'missing_receipt' };
  if (receipt.consumerId !== WORKFLOW_TERMINAL_PARENT_APPLICATION_CONSUMER_ID) {
    throw new TypeError('Invalid canonical parent-application receipt');
  }
  if (!plan) return { status: 'missing_plan' };

  const actionable = workflowTerminalActionNeedsDispatch(plan.contract.action);
  const applied =
    receipt.applicationState === 'applied' &&
    plan.contract.action.kind !== 'quarantine' &&
    (actionable
      ? (receipt.dispatchState === 'pending' && fence.record.phase === 'parent_outbox_pending') ||
        (receipt.dispatchState === 'destination_applied' && fence.record.phase === 'parent_effect_recorded')
      : receipt.dispatchState === 'none' && fence.record.phase === 'parent_effect_recorded');
  const quarantined =
    receipt.applicationState === 'quarantined' &&
    receipt.dispatchState === 'none' &&
    fence.record.phase === 'parent_outbox_pending' &&
    plan.contract.action.kind === 'quarantine';
  if (!applied && !quarantined) {
    throw new TypeError('Contradictory workflow terminal parent application evidence');
  }
  return {
    status: 'found',
    plan: copyWorkflowTerminalContinuationPlanRecord(plan),
    applicationState: receipt.applicationState,
    dispatchState: receipt.dispatchState,
  };
}

export function prepareWorkflowTerminalParentApplicationRecords(
  journal: WorkflowTerminalizationRecord | undefined,
  effect: WorkflowTerminalEffectRecord | undefined,
  existingReceipt: WorkflowTerminalDestinationReceiptRecord | undefined,
  existingPlan: WorkflowTerminalContinuationPlanRecord | undefined,
  input: ApplyWorkflowTerminalParentEffectInput,
  now: number,
): InternalPrepareWorkflowTerminalParentApplicationResult {
  if (effect) {
    validateWorkflowTerminalEffectIntegrity(effect);
    if (!journal) throw new TypeError('Workflow terminal effect exists without its journal');
    validateWorkflowTerminalEffectJournalLink(effect, journal, input.workflowName, input.runId);
  }
  if (existingReceipt) {
    if (!effect) throw new TypeError('Workflow terminal receipt exists without its effect');
    validateWorkflowTerminalDestinationReceiptIntegrity(existingReceipt, effect, now);
  }
  if (existingPlan) {
    if (!effect || !existingReceipt) {
      throw new TypeError('Workflow terminal continuation exists without receipt evidence');
    }
    validateWorkflowTerminalContinuationPlanIntegrity(existingPlan, effect, existingReceipt, now);
  }
  const fence = checkLiveWorkflowTerminalizationFence(journal, input, now);
  if (fence.status !== 'ok') return fence;
  try {
    validateWorkflowTerminalParentContinuationIntegrity(input.contract);
  } catch {
    return { status: 'invalid_contract' };
  }
  if (!effect || effect.kind !== 'parent-workflow-step-end') return { status: 'missing_effect' };
  if (
    input.contract.terminalEffectKey !== effect.effectKey ||
    input.contract.terminalEffectPayloadHash !== effect.payloadHash ||
    input.contract.childTerminalStatus !== effect.terminalStatus ||
    input.contract.executionMode !== 'continuous'
  ) {
    return { status: 'invalid_contract' };
  }
  if (fence.record.phase !== 'parent_outbox_pending' && fence.record.phase !== 'parent_effect_recorded') {
    return { status: 'phase_conflict', record: copyWorkflowTerminalizationRecord(fence.record) };
  }
  const desiredReceipt = createWorkflowTerminalDestinationReceiptRecord(
    effect,
    WORKFLOW_TERMINAL_PARENT_APPLICATION_CONSUMER_ID,
    now,
  );
  const receipt = existingReceipt ?? desiredReceipt;
  if (existingReceipt) {
    if (
      existingReceipt.receiptKey !== desiredReceipt.receiptKey ||
      existingReceipt.consumerId !== WORKFLOW_TERMINAL_PARENT_APPLICATION_CONSUMER_ID
    ) {
      throw new TypeError('Conflicting canonical parent-application receipt');
    }
  }
  let desiredPlan: WorkflowTerminalContinuationPlanRecord;
  try {
    desiredPlan = createWorkflowTerminalContinuationPlanRecord(
      effect,
      receipt,
      input.contract.expectedParentRevision,
      input.contract,
      existingPlan?.createdAt ?? now,
    );
  } catch {
    return { status: 'invalid_contract' };
  }
  if (existingPlan) {
    if (!sameWorkflowTerminalContinuationPlan(existingPlan, desiredPlan)) {
      return { status: 'contract_conflict', plan: copyWorkflowTerminalContinuationPlanRecord(existingPlan) };
    }
    const actionable = workflowTerminalActionNeedsDispatch(existingPlan.contract.action);
    const exactAppliedEvidence =
      receipt.applicationState === 'applied' &&
      existingPlan.contract.action.kind !== 'quarantine' &&
      (actionable
        ? (receipt.dispatchState === 'pending' && fence.record.phase === 'parent_outbox_pending') ||
          (receipt.dispatchState === 'destination_applied' && fence.record.phase === 'parent_effect_recorded')
        : receipt.dispatchState === 'none' && fence.record.phase === 'parent_effect_recorded');
    if (exactAppliedEvidence) {
      return {
        status: 'already_applied',
        journal: copyWorkflowTerminalizationRecord(fence.record),
        receipt: copyWorkflowTerminalDestinationReceiptRecord(receipt),
        plan: copyWorkflowTerminalContinuationPlanRecord(existingPlan),
      };
    }
    if (
      receipt.applicationState === 'quarantined' &&
      receipt.dispatchState === 'none' &&
      existingPlan.contract.action.kind === 'quarantine' &&
      fence.record.phase === 'parent_outbox_pending'
    ) {
      return {
        status: 'already_quarantined',
        journal: copyWorkflowTerminalizationRecord(fence.record),
        receipt: copyWorkflowTerminalDestinationReceiptRecord(receipt),
        plan: copyWorkflowTerminalContinuationPlanRecord(existingPlan),
      };
    }
    throw new TypeError('Contradictory workflow terminal parent application evidence');
  }
  if (receipt.applicationState !== 'reserved' || receipt.dispatchState !== 'none') {
    throw new TypeError('Workflow terminal parent receipt has state without a plan');
  }
  if (fence.record.phase !== 'parent_outbox_pending') {
    throw new TypeError('Workflow terminal parent journal has applied phase without evidence');
  }
  return {
    status: 'ready',
    journal: copyWorkflowTerminalizationRecord(fence.record),
    receipt: copyWorkflowTerminalDestinationReceiptRecord(receipt),
    plan: copyWorkflowTerminalContinuationPlanRecord(desiredPlan),
  };
}

export function finalizeWorkflowTerminalParentApplicationRecords(
  journal: WorkflowTerminalizationRecord,
  receipt: WorkflowTerminalDestinationReceiptRecord,
  plan: WorkflowTerminalContinuationPlanRecord,
  now: number,
): WorkflowTerminalParentApplicationFinalRecords {
  validateWorkflowTerminalizationClock(now);
  if (now < receipt.createdAt || now < plan.createdAt || journal.phase !== 'parent_outbox_pending') {
    throw new TypeError('Invalid workflow terminal parent application finalization');
  }
  const {
    applicationState: _applicationState,
    dispatchState: _dispatchState,
    appliedAt: _appliedAt,
    dispatchPendingAt: _dispatchPendingAt,
    destinationAppliedAt: _destinationAppliedAt,
    quarantinedAt: _quarantinedAt,
    ...receiptBase
  } = receipt;
  if (plan.contract.action.kind === 'quarantine') {
    return {
      status: 'quarantined',
      journal: copyWorkflowTerminalizationRecord(journal),
      receipt: {
        ...receiptBase,
        applicationState: 'quarantined',
        dispatchState: 'none',
        updatedAt: now,
        quarantinedAt: now,
      },
      plan: copyWorkflowTerminalContinuationPlanRecord(plan),
    };
  }
  const dispatchPending = workflowTerminalActionNeedsDispatch(plan.contract.action);
  return {
    status: 'applied',
    journal: dispatchPending
      ? copyWorkflowTerminalizationRecord(journal)
      : { ...journal, phase: 'parent_effect_recorded', updatedAt: now },
    receipt: dispatchPending
      ? {
          ...receiptBase,
          applicationState: 'applied',
          dispatchState: 'pending',
          updatedAt: now,
          appliedAt: now,
          dispatchPendingAt: now,
        }
      : {
          ...receiptBase,
          applicationState: 'applied',
          dispatchState: 'none',
          updatedAt: now,
          appliedAt: now,
        },
    plan: copyWorkflowTerminalContinuationPlanRecord(plan),
  };
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
