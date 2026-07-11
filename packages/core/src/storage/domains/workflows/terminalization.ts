import type { WorkflowTerminalizationPhase, WorkflowTerminalizationRecord } from '../../../workflows';
import type {
  AdvanceWorkflowTerminalizationInput,
  ClaimWorkflowTerminalizationInput,
  ReleaseWorkflowTerminalizationInput,
  WorkflowTerminalizationObservation,
} from '../../types';

type InternalClaimWorkflowTerminalizationResult =
  | {
      status: 'acquired' | 'renewed' | 'leased' | 'lease_expired' | 'fence_conflict' | 'terminal_conflict' | 'complete';
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

const NEXT_PHASES: Record<WorkflowTerminalizationPhase, readonly WorkflowTerminalizationPhase[]> = {
  terminalization_pending: ['run_state_persisted'],
  run_state_persisted: ['parent_outbox_pending', 'finish_outbox_pending'],
  parent_outbox_pending: ['parent_effect_recorded'],
  parent_effect_recorded: ['finish_outbox_pending'],
  finish_outbox_pending: ['finish_effect_recorded'],
  finish_effect_recorded: ['complete'],
  complete: [],
};

export const MAX_WORKFLOW_TERMINALIZATION_LEASE_MS = 86_400_000;

export function validateWorkflowTerminalizationIdentity(
  value: string,
  field: 'eventKey' | 'ownerId' | 'claimToken',
  maxLength: number,
): void {
  if (typeof value !== 'string' || value.length === 0 || value.length > maxLength) {
    throw new TypeError(`${field} must be a non-empty string no longer than ${maxLength} characters`);
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

export function copyWorkflowTerminalizationRecord(
  record: WorkflowTerminalizationRecord,
): WorkflowTerminalizationRecord {
  return { ...record };
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
    const renewed = {
      ...existing,
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

  const record: WorkflowTerminalizationRecord = existing
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
  validateWorkflowTerminalizationFence(input);
  validateWorkflowTerminalizationClock(now);
  if (input.leaseMs !== undefined) validateWorkflowTerminalizationLeaseMs(input.leaseMs);
  const leaseExpiresAt =
    input.leaseMs === undefined ? undefined : getWorkflowTerminalizationLeaseExpiry(now, input.leaseMs);
  const allowedPhases = NEXT_PHASES[input.expectedPhase];
  if (!allowedPhases || !allowedPhases.includes(input.nextPhase)) return { status: 'invalid_transition' };
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
  if (existing.phase !== input.expectedPhase) {
    return { status: 'phase_conflict', record: copyWorkflowTerminalizationRecord(existing) };
  }

  const complete = input.nextPhase === 'complete';
  const record: WorkflowTerminalizationRecord = {
    ...existing,
    phase: input.nextPhase,
    updatedAt: now,
    ...(leaseExpiresAt === undefined || complete ? {} : { leaseExpiresAt }),
    ...(complete ? { ownerId: undefined, claimToken: undefined, leaseExpiresAt: undefined, completedAt: now } : {}),
  };
  return { status: 'advanced', record: copyWorkflowTerminalizationRecord(record) };
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
