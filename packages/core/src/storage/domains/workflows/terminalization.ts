import { Buffer } from 'node:buffer';
import { createHash } from 'node:crypto';
import type {
  StepResult,
  WorkflowRunState,
  WorkflowTerminalDestinationReceiptRecord,
  WorkflowTerminalEffectRecord,
  WorkflowTerminalizationClaimedRecord,
  WorkflowTerminalSnapshotRecord,
  WorkflowTerminalizationPhase,
  WorkflowTerminalizationRecord,
} from '../../../workflows';
import {
  copyWorkflowTerminalParentContinuationContract,
  createWorkflowTerminalGraphFingerprint,
  getWorkflowTerminalEffectIntegrity,
  materializeWorkflowTerminalParentContinuationChildProjection,
  validateWorkflowTerminalEffectIntegrity,
  validateWorkflowTerminalParentContinuationIntegrity,
  WORKFLOW_TERMINAL_FOREACH_RUN_KEY,
} from '../../../workflows/terminal-continuation';
import { getDenseDataArray, getPlainDataDescriptors } from '../../../workflows/terminal-continuation/data-shape';
import {
  copyWorkflowTerminalRecoveryAncestry,
  getWorkflowTerminalRecoveryAncestryHash,
  materializeWorkflowTerminalCanonicalJsonObject,
  materializeWorkflowTerminalRecoveryEnvelope,
  validateWorkflowTerminalRecoveryEnvelopeIntegrity,
} from '../../../workflows/terminal-recovery';
import type {
  WorkflowTerminalRecoveryAncestryV1,
  WorkflowTerminalRecoveryEnvelopeRecordV1,
  WorkflowTerminalRecoveryEnvelopeV1,
} from '../../../workflows/terminal-recovery';
import {
  getMaterializedWorkflowTerminalRecoveryEnvelopeHash,
  validateMaterializedWorkflowTerminalRecoveryEnvelope,
  validateMaterializedWorkflowTerminalRecoveryGraphBinding,
} from '../../../workflows/terminal-recovery/envelope';
export {
  applyWorkflowTerminalParentContinuationPatch,
  copyWorkflowTerminalParentContinuationContract,
  createWorkflowTerminalGraphFingerprint,
  createWorkflowTerminalParentContinuationContract,
  validateWorkflowTerminalEffectIntegrity,
  WorkflowTerminalContinuationStoredStateError,
  WORKFLOW_TERMINAL_FOREACH_RUN_KEY,
} from '../../../workflows/terminal-continuation';
import type {
  AdvanceWorkflowTerminalizationInput,
  AdmitWorkflowNestedRunInput,
  ApplyWorkflowTerminalParentEffectInput,
  BindWorkflowNestedRunOwnershipInput,
  ClaimWorkflowTerminalizationInput,
  GetWorkflowTerminalEffectForDispatchInput,
  GetWorkflowRunTerminalStatusInput,
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
  WorkflowTerminalRecoveryAncestryRecord,
} from '../../types';
import { mergeWorkflowStepResult } from '../../workflow-snapshot';

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
      recovery: WorkflowTerminalRecoveryEnvelopeRecordV1;
    }
  | {
      status: 'invalid_snapshot' | 'invalid_recovery_envelope' | 'missing_recovery_ancestry' | 'missing_record';
    };

type InternalPersistWorkflowTerminalStateAuthorizationResult =
  | { status: 'authorized'; record: WorkflowTerminalizationRecord }
  | {
      status: 'phase_conflict' | 'not_owner' | 'fence_conflict' | 'lease_expired' | 'complete';
      record: WorkflowTerminalizationRecord;
    }
  | { status: 'missing_record' };

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
  | { status: 'invalid_transition' | 'missing_effect' | 'missing_terminal_state' | 'missing_record' };

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

type WorkflowNestedRunOwnershipOperation = Pick<
  BindWorkflowNestedRunOwnershipInput,
  'workflowName' | 'runId' | 'stepId' | 'nestedRunId' | 'forEachIndex' | 'result' | 'requestContext'
>;

export type BindWorkflowNestedRunOwnershipRecordResult =
  | { status: 'bound' | 'already_bound'; snapshot: WorkflowRunState }
  | { status: 'ownership_conflict' };

function ownDataValue(value: unknown, key: PropertyKey): unknown {
  if (value === null || typeof value !== 'object') return undefined;
  const descriptor = Object.getOwnPropertyDescriptor(value, key);
  return descriptor?.enumerable && 'value' in descriptor ? descriptor.value : undefined;
}

/** @internal Captures a workflow identity without executing inherited or accessor state. */
export function captureWorkflowRunIdentity(
  input: GetWorkflowRunTerminalStatusInput,
): GetWorkflowRunTerminalStatusInput {
  if (input === null || typeof input !== 'object' || Array.isArray(input)) {
    throw new TypeError('Workflow identity must be an own-data payload');
  }
  const workflowName = Object.getOwnPropertyDescriptor(input, 'workflowName');
  const runId = Object.getOwnPropertyDescriptor(input, 'runId');
  if (!workflowName?.enumerable || !('value' in workflowName)) {
    throw new TypeError('Workflow identity workflowName must be own data');
  }
  if (!runId?.enumerable || !('value' in runId)) {
    throw new TypeError('Workflow identity runId must be own data');
  }
  validateWorkflowTerminalizationIdentity(workflowName.value, 'workflowName', 512);
  validateWorkflowTerminalizationIdentity(runId.value, 'runId', 512);
  return { workflowName: workflowName.value as string, runId: runId.value as string };
}

function requiredOwnAdmissionData(input: unknown, key: keyof AdmitWorkflowNestedRunInput): unknown {
  if (input === null || typeof input !== 'object' || Array.isArray(input)) {
    throw new TypeError('Nested workflow admission must be an own-data payload');
  }
  const descriptor = Object.getOwnPropertyDescriptor(input, key);
  if (!descriptor?.enumerable || !('value' in descriptor)) {
    throw new TypeError(`Nested workflow admission ${key} must be own data`);
  }
  return descriptor.value;
}

function optionalOwnAdmissionData(input: object, key: keyof AdmitWorkflowNestedRunInput): unknown {
  const descriptor = Object.getOwnPropertyDescriptor(input, key);
  if (!descriptor) return undefined;
  if (!descriptor.enumerable || !('value' in descriptor)) {
    throw new TypeError(`Nested workflow admission ${key} must be own data`);
  }
  return descriptor.value;
}

/** @internal Captures the admission envelope once without executing inherited or accessor state. */
export function captureWorkflowNestedRunAdmissionInput(
  input: AdmitWorkflowNestedRunInput,
): AdmitWorkflowNestedRunInput {
  const workflowName = requiredOwnAdmissionData(input, 'workflowName') as string;
  const runId = requiredOwnAdmissionData(input, 'runId') as string;
  const stepId = requiredOwnAdmissionData(input, 'stepId') as string;
  const nestedWorkflowName = requiredOwnAdmissionData(input, 'nestedWorkflowName') as string;
  const nestedRunId = requiredOwnAdmissionData(input, 'nestedRunId') as string;
  const result = requiredOwnAdmissionData(input, 'result') as AdmitWorkflowNestedRunInput['result'];
  const requestContext = requiredOwnAdmissionData(
    input,
    'requestContext',
  ) as AdmitWorkflowNestedRunInput['requestContext'];
  const recoveryAncestry = requiredOwnAdmissionData(
    input,
    'recoveryAncestry',
  ) as AdmitWorkflowNestedRunInput['recoveryAncestry'];
  const expectedChildGraphFingerprint = requiredOwnAdmissionData(input, 'expectedChildGraphFingerprint') as string;
  const forEachIndex = optionalOwnAdmissionData(input, 'forEachIndex') as number | undefined;
  const initialChildSnapshot = optionalOwnAdmissionData(
    input,
    'initialChildSnapshot',
  ) as AdmitWorkflowNestedRunInput['initialChildSnapshot'];
  return {
    workflowName,
    runId,
    stepId,
    nestedWorkflowName,
    nestedRunId,
    result,
    requestContext,
    recoveryAncestry,
    expectedChildGraphFingerprint,
    ...(forEachIndex === undefined ? {} : { forEachIndex }),
    ...(initialChildSnapshot === undefined ? {} : { initialChildSnapshot }),
  };
}

const WORKFLOW_RUN_STATUSES = [
  'running',
  'success',
  'failed',
  'tripwire',
  'suspended',
  'waiting',
  'pending',
  'canceled',
  'bailed',
  'paused',
] as const;
const WORKFLOW_RUN_TERMINAL_STATUSES = ['success', 'failed', 'tripwire', 'canceled', 'bailed'] as const;
type WorkflowNestedRunTerminalStatus = (typeof WORKFLOW_RUN_TERMINAL_STATUSES)[number];

const WORKFLOW_RUN_REQUIRED_SNAPSHOT_FIELDS = new Set([
  'runId',
  'status',
  'value',
  'context',
  'serializedStepGraph',
  'activePaths',
  'activeStepsPath',
  'suspendedPaths',
  'resumeLabels',
  'waitingPaths',
  'timestamp',
]);
const MAX_WORKFLOW_RUN_SNAPSHOT_MAP_KEYS = 100_000;

function workflowRunDataDescriptors(value: unknown, field: string): Record<string, PropertyDescriptor> {
  return getPlainDataDescriptors(value, {
    allowNullPrototype: true,
    typeError: `${field} must be a plain data object`,
    fieldsError: `${field} must contain only own enumerable data fields`,
    maxKeys: MAX_WORKFLOW_RUN_SNAPSHOT_MAP_KEYS,
    maxKeysError: `${field} exceeds the workflow snapshot map-key limit`,
  });
}

function requiredWorkflowRunSnapshotField(
  descriptors: Record<string, PropertyDescriptor>,
  key: string,
  field: string,
): unknown {
  const descriptor = descriptors[key];
  if (!descriptor) throw new TypeError(`${field} is missing ${key}`);
  return descriptor.value;
}

function validateWorkflowRunSnapshotPath(value: unknown, field: string, minLength = 0): void {
  const entries = getDenseDataArray(value, {
    typeError: `${field} must be a dense path`,
    lengthError: `${field} exceeds the workflow path limit`,
    dataError: `${field} must be dense and data-only`,
    minLength,
    maxLength: MAX_WORKFLOW_TERMINAL_PARENT_EXECUTION_PATH_LENGTH,
  });
  for (const entry of entries) {
    if (!Number.isSafeInteger(entry) || (entry as number) < 0) {
      throw new TypeError(`${field} must contain non-negative safe integers`);
    }
  }
}

function validateWorkflowRunSnapshotPathMap(value: unknown, field: string): void {
  const descriptors = workflowRunDataDescriptors(value, field);
  for (const [key, descriptor] of Object.entries(descriptors)) {
    validateWorkflowRunSnapshotPath(descriptor.value, `${field}.${key}`, 1);
  }
}

function validateWorkflowRunResumeLabels(value: unknown, field: string): void {
  const labels = workflowRunDataDescriptors(value, field);
  for (const [label, descriptor] of Object.entries(labels)) {
    const fields = workflowRunDataDescriptors(descriptor.value, `${field}.${label}`);
    const keys = Object.keys(fields);
    if (keys.some(key => key !== 'stepId' && key !== 'foreachIndex') || !fields.stepId) {
      throw new TypeError(`${field}.${label} is invalid`);
    }
    validateWorkflowTerminalizationIdentity(fields.stepId.value, `${field}.${label}.stepId`, 512);
    if (
      fields.foreachIndex &&
      (!Number.isSafeInteger(fields.foreachIndex.value) || (fields.foreachIndex.value as number) < 0)
    ) {
      throw new TypeError(`${field}.${label}.foreachIndex must be a non-negative safe integer`);
    }
  }
}

/** @internal Validates the complete required, own-data WorkflowRunState shape. */
export function validateWorkflowRunSnapshotShape(
  snapshot: unknown,
  runId: string,
  field = 'Workflow run snapshot',
): {
  status: (typeof WORKFLOW_RUN_STATUSES)[number];
  graphFingerprint: string;
} {
  const descriptors = workflowRunDataDescriptors(snapshot, field);
  for (const key of WORKFLOW_RUN_REQUIRED_SNAPSHOT_FIELDS) requiredWorkflowRunSnapshotField(descriptors, key, field);
  const snapshotRunId = requiredWorkflowRunSnapshotField(descriptors, 'runId', field);
  const status = requiredWorkflowRunSnapshotField(descriptors, 'status', field);
  if (snapshotRunId !== runId) throw new TypeError(`${field} run identity is invalid`);
  if (typeof status !== 'string' || !WORKFLOW_RUN_STATUSES.includes(status as (typeof WORKFLOW_RUN_STATUSES)[number])) {
    throw new TypeError(`${field} status is invalid`);
  }
  const timestamp = requiredWorkflowRunSnapshotField(descriptors, 'timestamp', field);
  if (!Number.isSafeInteger(timestamp) || (timestamp as number) < 0) {
    throw new TypeError(`${field} timestamp is invalid`);
  }
  workflowRunDataDescriptors(requiredWorkflowRunSnapshotField(descriptors, 'value', field), `${field}.value`);
  workflowRunDataDescriptors(requiredWorkflowRunSnapshotField(descriptors, 'context', field), `${field}.context`);
  const requestContext = descriptors.requestContext?.value;
  if (requestContext !== undefined) workflowRunDataDescriptors(requestContext, `${field}.requestContext`);
  validateWorkflowRunSnapshotPath(
    requiredWorkflowRunSnapshotField(descriptors, 'activePaths', field),
    `${field}.activePaths`,
  );
  validateWorkflowRunSnapshotPathMap(
    requiredWorkflowRunSnapshotField(descriptors, 'activeStepsPath', field),
    `${field}.activeStepsPath`,
  );
  validateWorkflowRunSnapshotPathMap(
    requiredWorkflowRunSnapshotField(descriptors, 'suspendedPaths', field),
    `${field}.suspendedPaths`,
  );
  validateWorkflowRunResumeLabels(
    requiredWorkflowRunSnapshotField(descriptors, 'resumeLabels', field),
    `${field}.resumeLabels`,
  );
  validateWorkflowRunSnapshotPathMap(
    requiredWorkflowRunSnapshotField(descriptors, 'waitingPaths', field),
    `${field}.waitingPaths`,
  );
  const graphFingerprint = createWorkflowTerminalGraphFingerprint(
    requiredWorkflowRunSnapshotField(
      descriptors,
      'serializedStepGraph',
      field,
    ) as WorkflowRunState['serializedStepGraph'],
  );
  return {
    status: status as (typeof WORKFLOW_RUN_STATUSES)[number],
    graphFingerprint,
  };
}

export type WorkflowNestedRunRetainedSnapshotInspection =
  | { status: 'retained'; graphFingerprint: string }
  | { status: 'terminal'; terminalStatus: WorkflowNestedRunTerminalStatus; graphFingerprint: string }
  | { status: 'conflict' };

/** @internal Inspects retained child state without trusting prototypes, accessors, or stale graph identity. */
export function inspectWorkflowNestedRunRetainedSnapshot(
  snapshot: unknown,
  nestedRunId: string,
  expectedGraphFingerprint?: string,
): WorkflowNestedRunRetainedSnapshotInspection {
  try {
    const { status, graphFingerprint } = validateWorkflowRunSnapshotShape(
      snapshot,
      nestedRunId,
      'Retained nested workflow snapshot',
    );
    if (WORKFLOW_RUN_TERMINAL_STATUSES.includes(status as (typeof WORKFLOW_RUN_TERMINAL_STATUSES)[number])) {
      return { status: 'terminal', terminalStatus: status as WorkflowNestedRunTerminalStatus, graphFingerprint };
    }
    if (expectedGraphFingerprint !== undefined && graphFingerprint !== expectedGraphFingerprint) {
      return { status: 'conflict' };
    }
    return { status: 'retained', graphFingerprint };
  } catch {
    return { status: 'conflict' };
  }
}

/** @internal Validates one scalar or foreach child-ownership coordinate. */
export function validateWorkflowNestedRunOwnershipInput(input: WorkflowNestedRunOwnershipOperation): void {
  validateWorkflowTerminalizationIdentity(input.workflowName, 'workflowName', 512);
  validateWorkflowTerminalizationIdentity(input.runId, 'runId', 512);
  validateWorkflowTerminalizationIdentity(input.stepId, 'stepId', 512);
  validateWorkflowTerminalizationIdentity(input.nestedRunId, 'nestedRunId', 512);
  if (input.forEachIndex !== undefined && (!Number.isSafeInteger(input.forEachIndex) || input.forEachIndex < 0)) {
    throw new TypeError('forEachIndex must be a non-negative safe integer');
  }
}

/** @internal Validates an atomic nested-child initialization payload. */
export function validateWorkflowNestedRunInitialSnapshot(
  input: AdmitWorkflowNestedRunInput['initialChildSnapshot'],
  nestedRunId: string,
  expectedChildGraphFingerprint: string,
): AdmitWorkflowNestedRunInput['initialChildSnapshot'] {
  if (!/^sha256:[a-f0-9]{64}$/.test(expectedChildGraphFingerprint)) {
    throw new TypeError('expectedChildGraphFingerprint must be a canonical SHA-256 fingerprint');
  }
  if (input === undefined) return undefined;
  if (input === null || typeof input !== 'object' || Array.isArray(input)) {
    throw new TypeError('Initial nested workflow snapshot must be an own-data payload');
  }
  const snapshotDescriptor = Object.getOwnPropertyDescriptor(input, 'snapshot');
  const resourceIdDescriptor = Object.getOwnPropertyDescriptor(input, 'resourceId');
  if (!snapshotDescriptor?.enumerable || !('value' in snapshotDescriptor)) {
    throw new TypeError('Initial nested workflow snapshot must be an own-data payload');
  }
  if (resourceIdDescriptor && (!resourceIdDescriptor.enumerable || !('value' in resourceIdDescriptor))) {
    throw new TypeError('Initial nested workflow resourceId must be own data');
  }
  const resourceId = resourceIdDescriptor?.value;
  if (resourceId !== undefined) {
    validateWorkflowTerminalizationIdentity(resourceId, 'resourceId', 512);
  }
  const snapshot = materializeWorkflowTerminalCanonicalJsonObject(
    snapshotDescriptor.value,
    'initialChildSnapshot.snapshot',
  ) as unknown as WorkflowRunState;
  const inspection = validateWorkflowRunSnapshotShape(snapshot, nestedRunId, 'Initial nested workflow snapshot');
  if (inspection.status !== 'running') {
    throw new TypeError('Initial nested workflow snapshot must be a valid running child snapshot');
  }
  if (inspection.graphFingerprint !== expectedChildGraphFingerprint) {
    throw new TypeError('Initial nested workflow snapshot graph does not match the expected child graph');
  }
  return {
    snapshot,
    ...(resourceId === undefined ? {} : { resourceId }),
  };
}

/** @internal Pure canonical nested-child ownership transition shared by storage adapters. */
export function bindWorkflowNestedRunOwnershipRecord(
  snapshot: WorkflowRunState,
  input: WorkflowNestedRunOwnershipOperation,
): BindWorkflowNestedRunOwnershipRecordResult {
  validateWorkflowNestedRunOwnershipInput(input);
  const current = ownDataValue(snapshot.context, input.stepId);
  const currentMetadataValue = ownDataValue(current, 'metadata');
  const currentMetadata =
    currentMetadataValue !== null && typeof currentMetadataValue === 'object' ? currentMetadataValue : {};
  const incomingMetadataValue = ownDataValue(input.result, 'metadata');
  const incomingMetadata =
    incomingMetadataValue !== null && typeof incomingMetadataValue === 'object' ? incomingMetadataValue : {};
  const incomingWorkflowMetadataValue = ownDataValue(incomingMetadata, '__workflow_meta');
  const incomingWorkflowMetadata =
    incomingWorkflowMetadataValue !== null && typeof incomingWorkflowMetadataValue === 'object'
      ? incomingWorkflowMetadataValue
      : {};
  const workflowMetadataValue = ownDataValue(currentMetadata, '__workflow_meta');
  const workflowMetadata =
    workflowMetadataValue !== null && typeof workflowMetadataValue === 'object' ? workflowMetadataValue : {};
  const iterationRunsValue = ownDataValue(workflowMetadata, WORKFLOW_TERMINAL_FOREACH_RUN_KEY);
  const iterationRuns = iterationRunsValue !== null && typeof iterationRunsValue === 'object' ? iterationRunsValue : {};
  const existing =
    input.forEachIndex === undefined
      ? ownDataValue(currentMetadata, 'nestedRunId')
      : ownDataValue(iterationRuns, String(input.forEachIndex));
  if (existing !== undefined && existing !== input.nestedRunId) return { status: 'ownership_conflict' };

  const updatedSnapshot: WorkflowRunState = {
    ...snapshot,
    context: { ...snapshot.context },
    requestContext: { ...snapshot.requestContext },
  };
  if (existing === input.nestedRunId) return { status: 'already_bound', snapshot: updatedSnapshot };

  const metadata =
    input.forEachIndex === undefined
      ? { ...currentMetadata, ...incomingMetadata, nestedRunId: input.nestedRunId }
      : {
          ...currentMetadata,
          ...incomingMetadata,
          __workflow_meta: {
            ...workflowMetadata,
            ...incomingWorkflowMetadata,
            [WORKFLOW_TERMINAL_FOREACH_RUN_KEY]: {
              ...iterationRuns,
              [String(input.forEachIndex)]: input.nestedRunId,
            },
          },
        };
  mergeWorkflowStepResult({
    snapshot: updatedSnapshot,
    stepId: input.stepId,
    result: { ...input.result, metadata } as StepResult<any, any, any, any>,
    requestContext: input.requestContext,
  });
  return { status: 'bound', snapshot: updatedSnapshot };
}

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

export function createWorkflowTerminalRecoveryAncestryRecord(
  workflowName: string,
  runId: string,
  ancestry: WorkflowTerminalRecoveryAncestryV1,
  now: number,
): WorkflowTerminalRecoveryAncestryRecord {
  validateWorkflowTerminalizationIdentity(workflowName, 'workflowName', 512);
  validateWorkflowTerminalizationIdentity(runId, 'runId', 512);
  validateWorkflowTerminalizationClock(now);
  const materialized = copyWorkflowTerminalRecoveryAncestry(ancestry);
  if (materialized[0] && (materialized[0].childWorkflowName !== workflowName || materialized[0].childRunId !== runId)) {
    throw new TypeError('Workflow terminal recovery ancestry does not start at the child run');
  }
  return {
    version: 1,
    workflowName,
    runId,
    ancestryHash: getWorkflowTerminalRecoveryAncestryHash(materialized),
    ancestry: materialized,
    createdAt: now,
  };
}

export function copyWorkflowTerminalRecoveryAncestryRecord(
  record: WorkflowTerminalRecoveryAncestryRecord,
): WorkflowTerminalRecoveryAncestryRecord {
  return { ...record, ancestry: copyWorkflowTerminalRecoveryAncestry(record.ancestry) };
}

export function validateWorkflowTerminalRecoveryAncestryRecord(
  record: WorkflowTerminalRecoveryAncestryRecord,
  expected: { workflowName: string; runId: string; now: number },
): void {
  const materialized = createWorkflowTerminalRecoveryAncestryRecord(
    record.workflowName,
    record.runId,
    record.ancestry,
    record.createdAt,
  );
  if (
    record.version !== 1 ||
    record.workflowName !== expected.workflowName ||
    record.runId !== expected.runId ||
    record.createdAt > expected.now ||
    record.ancestryHash !== materialized.ancestryHash
  ) {
    throw new TypeError('Invalid workflow terminal recovery ancestry record');
  }
}

export function sameWorkflowTerminalRecoveryAncestry(
  left: WorkflowTerminalRecoveryAncestryRecord,
  right: WorkflowTerminalRecoveryAncestryRecord,
): boolean {
  return left.ancestryHash === right.ancestryHash;
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

/** @internal Authenticates a retained terminal record, including explicit resource presence. */
export function getWorkflowTerminalSnapshotRecordHash(
  retained: Pick<
    WorkflowTerminalSnapshotRecord,
    'version' | 'workflowName' | 'runId' | 'resourceId' | 'terminalStatus' | 'envelopeHash' | 'createdAt'
  >,
): `sha256:${string}` {
  if (retained.version !== 1) throw new TypeError('Invalid workflow terminal snapshot version');
  validateWorkflowTerminalizationIdentity(retained.workflowName, 'workflowName', 512);
  validateWorkflowTerminalizationIdentity(retained.runId, 'runId', 512);
  if (retained.resourceId !== undefined) {
    validateWorkflowTerminalizationIdentity(retained.resourceId, 'resourceId', 512);
  }
  if (!['success', 'failed', 'canceled'].includes(retained.terminalStatus)) {
    throw new TypeError('Invalid workflow terminal snapshot status');
  }
  if (!/^sha256:[a-f0-9]{64}$/.test(retained.envelopeHash)) {
    throw new TypeError('Invalid workflow terminal recovery envelope hash');
  }
  if (!Number.isSafeInteger(retained.createdAt) || retained.createdAt < 0) {
    throw new TypeError('Invalid workflow terminal snapshot createdAt');
  }
  const resourceParts =
    retained.resourceId === undefined ? ['resource-id-absent'] : ['resource-id-present', retained.resourceId];
  return `sha256:${hashFramedParts('mastra.workflow-terminal-snapshot-record.v1', [
    String(retained.version),
    retained.workflowName,
    retained.runId,
    retained.terminalStatus,
    retained.envelopeHash,
    String(retained.createdAt),
    ...resourceParts,
  ])}`;
}

/** @internal Fails closed when any authenticated retained-record field was altered. */
export function validateWorkflowTerminalSnapshotRecordIntegrity(retained: WorkflowTerminalSnapshotRecord): void {
  if (retained.recordHash !== getWorkflowTerminalSnapshotRecordHash(retained)) {
    throw new TypeError('Invalid workflow terminal snapshot record integrity');
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
  validateWorkflowTerminalSnapshotRecordIntegrity(retained);
  validateWorkflowTerminalRecoveryEnvelopeIntegrity(
    { version: retained.version, envelopeHash: retained.envelopeHash, envelope: retained.envelope },
    { workflowName, runId, terminalStatus: journal.terminalStatus },
  );
  if (
    ![retained.createdAt, journal.createdAt, journal.updatedAt].every(
      value => Number.isSafeInteger(value) && value >= 0,
    ) ||
    retained.workflowName !== workflowName ||
    retained.runId !== runId ||
    retained.terminalStatus !== journal.terminalStatus ||
    retained.createdAt < journal.createdAt ||
    retained.createdAt > journal.updatedAt
  ) {
    throw new TypeError('Invalid workflow terminal snapshot journal link');
  }
}

/** @internal Binds the structural producer intent to the authenticated retained payload. */
export function validateWorkflowTerminalEffectRecoveryLink(
  effect: WorkflowTerminalEffectRecord,
  retained: WorkflowTerminalSnapshotRecord,
): void {
  if (
    effect.workflowName !== retained.workflowName ||
    effect.runId !== retained.runId ||
    effect.terminalStatus !== retained.terminalStatus ||
    effect.recoveryEnvelopeHash !== retained.envelopeHash ||
    effect.retainedRecordHash !== retained.recordHash ||
    effect.resourceId !== retained.resourceId
  ) {
    throw new TypeError('Invalid workflow terminal effect recovery link');
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
  retained: WorkflowTerminalSnapshotRecord,
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
    recoveryEnvelopeHash: retained.envelopeHash,
    retainedRecordHash: retained.recordHash,
    ...(retained.resourceId === undefined ? {} : { resourceId: retained.resourceId }),
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
    left.recoveryEnvelopeHash !== right.recoveryEnvelopeHash ||
    left.retainedRecordHash !== right.retainedRecordHash ||
    left.resourceId !== right.resourceId ||
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
  retained: WorkflowTerminalSnapshotRecord | undefined,
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
  if (!retained) return { status: 'missing_terminal_state' };
  validateWorkflowTerminalSnapshotJournalLink(retained, fence.record, input.workflowName, input.runId);

  const descriptor = materializeWorkflowTerminalEffectDescriptor(input.effect);
  const immediate = retained.envelope.ancestry[0];
  if (descriptor.kind === 'workflow-finish') {
    if (immediate) throw new TypeError('Nested workflow recovery envelope requires a parent terminal effect');
  } else {
    if (!immediate) throw new TypeError('Root workflow recovery envelope cannot prepare a parent terminal effect');
    const sourcePath =
      immediate.source.kind === 'step'
        ? immediate.source.executionPath
        : [...immediate.source.containerPath, immediate.source.iterationIndex];
    if (
      immediate.parentWorkflowName !== descriptor.parentWorkflowName ||
      immediate.parentRunId !== descriptor.parentRunId ||
      immediate.source.stepId !== descriptor.parentStepId ||
      sourcePath.length !== descriptor.parentExecutionPath.length ||
      sourcePath.some((entry, index) => entry !== descriptor.parentExecutionPath[index])
    ) {
      throw new TypeError('Workflow terminal parent effect does not match retained recovery ancestry');
    }
  }

  const desired = createWorkflowTerminalEffectRecord(fence.record, retained, input, now);
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

/** @internal Authorizes terminal-state persistence before an adapter locks canonical run state. */
export function authorizeWorkflowTerminalStateRecord(
  existing: WorkflowTerminalizationRecord | undefined,
  input: Pick<
    PersistWorkflowTerminalStateInput,
    'workflowName' | 'runId' | 'ownerId' | 'claimToken' | 'claimGeneration' | 'leaseMs'
  >,
  now: number,
): InternalPersistWorkflowTerminalStateAuthorizationResult {
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
  return { status: 'authorized', record: copyWorkflowTerminalizationRecord(existing) };
}

/** @internal Atomically certifies that the canonical run snapshot is terminal. */
export function persistWorkflowTerminalStateRecord(
  existing: WorkflowTerminalizationRecord | undefined,
  existingAncestry: WorkflowTerminalRecoveryAncestryRecord | undefined,
  input: PersistWorkflowTerminalStateInput,
  now: number,
  materializeSnapshot: (
    snapshot: PersistWorkflowTerminalStateInput['snapshot'],
  ) => PersistWorkflowTerminalStateInput['snapshot'],
  materializeRecoveryEnvelope: (
    envelope: PersistWorkflowTerminalStateInput['recoveryEnvelope'],
  ) => WorkflowTerminalRecoveryEnvelopeV1 = materializeWorkflowTerminalRecoveryEnvelope,
): InternalPersistWorkflowTerminalStateResult {
  const authorization = authorizeWorkflowTerminalStateRecord(existing, input, now);
  if (authorization.status !== 'authorized') return authorization;
  if (!input.snapshot || typeof input.snapshot !== 'object' || Array.isArray(input.snapshot)) {
    return { status: 'invalid_snapshot' };
  }
  let inputSnapshot: PersistWorkflowTerminalStateInput['snapshot'];
  try {
    inputSnapshot = materializeSnapshot(input.snapshot);
  } catch {
    return { status: 'invalid_snapshot' };
  }
  try {
    const inspection = validateWorkflowRunSnapshotShape(inputSnapshot, input.runId, 'Workflow terminal snapshot');
    if (inspection.status !== authorization.record.terminalStatus) {
      return { status: 'invalid_snapshot' };
    }
  } catch {
    return { status: 'invalid_snapshot' };
  }
  let recovery: WorkflowTerminalRecoveryEnvelopeRecordV1;
  try {
    const envelope = materializeRecoveryEnvelope(input.recoveryEnvelope);
    validateMaterializedWorkflowTerminalRecoveryGraphBinding(envelope, {
      childSerializedStepGraph: inputSnapshot.serializedStepGraph,
    });
    const envelopeHash = getMaterializedWorkflowTerminalRecoveryEnvelopeHash(envelope);
    validateMaterializedWorkflowTerminalRecoveryEnvelope(
      envelope,
      {
        workflowName: input.workflowName,
        runId: input.runId,
        terminalStatus: authorization.record.terminalStatus,
      },
      envelopeHash,
    );
    recovery = {
      version: 1 as const,
      envelopeHash,
      // materializeWorkflowTerminalRecoveryEnvelope already returned an owned,
      // detached canonical value. Re-entering public validation/copy helpers
      // here would repeatedly traverse an envelope that may be up to 8 MiB.
      envelope,
    };
    if (envelope.ancestry.length > 0) {
      // Nested envelopes must fit the exact aggregate budget that native
      // parent continuation consumes. Root envelopes have no parent consumer.
      materializeWorkflowTerminalParentContinuationChildProjection(envelope, now);
      if (!existingAncestry) return { status: 'missing_recovery_ancestry' };
      validateWorkflowTerminalRecoveryAncestryRecord(existingAncestry, {
        workflowName: input.workflowName,
        runId: input.runId,
        now,
      });
      if (existingAncestry.ancestryHash !== getWorkflowTerminalRecoveryAncestryHash(envelope.ancestry)) {
        return { status: 'invalid_recovery_envelope' };
      }
    } else if (existingAncestry && existingAncestry.ancestry.length > 0) {
      return { status: 'invalid_recovery_envelope' };
    }
  } catch {
    return { status: 'invalid_recovery_envelope' };
  }
  const projectedSnapshot = { ...inputSnapshot };
  delete projectedSnapshot.error;
  if (authorization.record.terminalStatus === 'failed') {
    projectedSnapshot.error = recovery.envelope.terminalResult
      .error as PersistWorkflowTerminalStateInput['snapshot']['error'];
  }
  let snapshot: PersistWorkflowTerminalStateInput['snapshot'];
  try {
    snapshot = materializeSnapshot({
      ...projectedSnapshot,
      result: recovery.envelope.terminalResult,
      context: {
        ...inputSnapshot.context,
        __state: recovery.envelope.finalState as never,
      } as unknown as PersistWorkflowTerminalStateInput['snapshot']['context'],
      value: recovery.envelope.finalState as PersistWorkflowTerminalStateInput['snapshot']['value'],
      requestContext: recovery.envelope.requestContextPatch,
    });
    const inspection = validateWorkflowRunSnapshotShape(snapshot, input.runId, 'Projected workflow terminal snapshot');
    if (inspection.status !== authorization.record.terminalStatus) return { status: 'invalid_snapshot' };
  } catch {
    return { status: 'invalid_snapshot' };
  }
  const leaseExpiresAt =
    input.leaseMs === undefined ? undefined : getWorkflowTerminalizationLeaseExpiry(now, input.leaseMs);
  return {
    status: 'advanced',
    snapshot,
    recovery,
    record: {
      ...authorization.record,
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
