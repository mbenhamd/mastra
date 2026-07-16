import { createHash } from 'node:crypto';
import { isProxy } from 'node:util/types';
import type { WorkflowTerminalEffectRecord, WorkflowRunState, WorkflowRunStatus } from '../types';
import {
  MAX_TERMINAL_LOOP_ITERATIONS,
  MAX_TERMINAL_PATH_LENGTH,
  MAX_TERMINAL_REVISION_LENGTH,
  createWorkflowTerminalGraphFingerprint,
  resolveWorkflowTerminalGraphCoordinate,
  validateWorkflowTerminalStructuralString,
} from './graph-fingerprint';
import { WORKFLOW_TERMINAL_FOREACH_RUN_KEY, WORKFLOW_TERMINAL_FOREACH_STATE_KEY } from './types';
import type {
  WorkflowTerminalContinuationAction,
  WorkflowTerminalLoopDecision,
  WorkflowTerminalParentContinuationContract,
  WorkflowTerminalParentContinuationSpec,
  WorkflowTerminalParentPatch,
  WorkflowTerminalResultCoordinate,
  WorkflowTerminalRunTarget,
} from './types';

const ACTIVE_PARENT_STATUSES = new Set<WorkflowRunStatus>(['running', 'waiting', 'suspended']);
const FINAL_PARENT_STATUSES = new Set<WorkflowRunStatus>([
  'success',
  'failed',
  'canceled',
  'tripwire',
  'bailed',
  'skipped',
]);
const ALL_PARENT_STATUSES = new Set<WorkflowRunStatus>([
  'pending',
  'running',
  'waiting',
  'suspended',
  'paused',
  'success',
  'failed',
  'canceled',
  'tripwire',
  'bailed',
  'skipped',
]);
const CHILD_STATUSES = new Set(['success', 'failed', 'canceled']);
const QUARANTINE_REASONS = new Set(['graph-conflict', 'parent-conflict-exhausted', 'plan-conflict']);
const SHA256 = /^sha256:[a-f0-9]{64}$/;
const MAX_TERMINAL_FOREACH_ENTRIES = 16_384;

function getRecord(value: unknown, field: string): Record<string, PropertyDescriptor> {
  if (isProxy(value)) throw new TypeError(`${field} must not be a proxy`);
  if (value === null || typeof value !== 'object') throw new TypeError(`${field} must be a data object`);
  const prototype = Object.getPrototypeOf(value);
  if (prototype !== Object.prototype && prototype !== null) throw new TypeError(`${field} must be a plain object`);
  const descriptors = Object.getOwnPropertyDescriptors(value);
  Object.setPrototypeOf(descriptors, null);
  for (const key of Reflect.ownKeys(descriptors)) {
    if (typeof key !== 'string' || !('value' in descriptors[key]!) || descriptors[key]!.enumerable !== true) {
      throw new TypeError(`${field} contains symbol, accessor, or non-enumerable fields`);
    }
  }
  return descriptors as Record<string, PropertyDescriptor>;
}

function assertKeys(
  descriptors: Record<string, PropertyDescriptor>,
  allowed: readonly string[],
  required: readonly string[],
  field: string,
): void {
  const allowedSet = new Set(allowed);
  for (const key of Object.keys(descriptors)) {
    if (!allowedSet.has(key)) throw new TypeError(`${field} contains unknown field ${key}`);
  }
  for (const key of required) {
    if (!Object.prototype.hasOwnProperty.call(descriptors, key)) throw new TypeError(`${field} is missing ${key}`);
  }
}

function value(descriptors: Record<string, PropertyDescriptor>, key: string): unknown {
  return Object.prototype.hasOwnProperty.call(descriptors, key) ? descriptors[key]!.value : undefined;
}

function materializeDenseDataArray(input: unknown, field: string, maxLength: number): unknown[] {
  if (isProxy(input)) throw new TypeError(`${field} must not be a proxy`);
  if (!Array.isArray(input)) throw new TypeError(`${field} must be a dense data-only array`);
  const length = Object.getOwnPropertyDescriptor(input, 'length')?.value;
  if (!Number.isSafeInteger(length) || length < 0 || length > maxLength) {
    throw new TypeError(`${field} has an invalid length`);
  }
  const descriptors = Object.getOwnPropertyDescriptors(input) as unknown as Record<PropertyKey, PropertyDescriptor>;
  const descriptorKeys = Reflect.ownKeys(descriptors);
  if (
    descriptorKeys.length !== length + 1 ||
    descriptorKeys.some(
      key =>
        key !== 'length' &&
        (typeof key !== 'string' ||
          !/^(0|[1-9][0-9]*)$/.test(key) ||
          Number(key) >= length ||
          descriptors[key]!.enumerable !== true),
    ) ||
    descriptorKeys.some(key => !('value' in descriptors[key]!))
  ) {
    throw new TypeError(`${field} must be dense and data-only`);
  }
  return Array.from({ length }, (_, index) => descriptors[String(index)]!.value);
}

function canonicalPath(input: unknown, field: string, allowEmpty = false): number[] {
  const entries = materializeDenseDataArray(input, field, MAX_TERMINAL_PATH_LENGTH);
  if (!allowEmpty && entries.length === 0) throw new TypeError(`${field} has an invalid length`);
  return entries.map(entry => {
    if (!Number.isSafeInteger(entry) || (entry as number) < 0) {
      throw new TypeError(`${field} contains an invalid index`);
    }
    return entry === 0 ? 0 : (entry as number);
  });
}

function canonicalSafeInteger(value: unknown, field: string, max = Number.MAX_SAFE_INTEGER): number {
  if (!Number.isSafeInteger(value) || (value as number) < 0 || (value as number) > max) {
    throw new TypeError(`${field} must be a non-negative safe integer`);
  }
  return value === 0 ? 0 : (value as number);
}

function canonicalSource(input: unknown): WorkflowTerminalResultCoordinate {
  const descriptors = getRecord(input, 'source');
  const kind = value(descriptors, 'kind');
  if (kind === 'step') {
    assertKeys(descriptors, ['kind', 'stepId', 'executionPath'], ['kind', 'stepId', 'executionPath'], 'source');
    return {
      kind,
      stepId: validateWorkflowTerminalStructuralString(value(descriptors, 'stepId'), 'source.stepId'),
      executionPath: canonicalPath(value(descriptors, 'executionPath'), 'source.executionPath'),
    };
  }
  if (kind === 'foreach-iteration') {
    assertKeys(
      descriptors,
      ['kind', 'stepId', 'containerPath', 'iterationIndex'],
      ['kind', 'stepId', 'containerPath', 'iterationIndex'],
      'source',
    );
    return {
      kind,
      stepId: validateWorkflowTerminalStructuralString(value(descriptors, 'stepId'), 'source.stepId'),
      containerPath: canonicalPath(value(descriptors, 'containerPath'), 'source.containerPath'),
      iterationIndex: canonicalSafeInteger(value(descriptors, 'iterationIndex'), 'source.iterationIndex'),
    };
  }
  throw new TypeError('source.kind is invalid');
}

function canonicalRunTarget(input: unknown, field: string): WorkflowTerminalRunTarget {
  const descriptors = getRecord(input, field);
  const kind = value(descriptors, 'kind');
  if (kind === 'step') {
    assertKeys(descriptors, ['kind', 'stepId', 'executionPath'], ['kind', 'stepId', 'executionPath'], field);
    return {
      kind,
      stepId: validateWorkflowTerminalStructuralString(value(descriptors, 'stepId'), `${field}.stepId`),
      executionPath: canonicalPath(value(descriptors, 'executionPath'), `${field}.executionPath`),
    };
  }
  if (kind === 'entry') {
    assertKeys(
      descriptors,
      ['kind', 'entryType', 'entryId', 'executionPath'],
      ['kind', 'entryType', 'entryId', 'executionPath'],
      field,
    );
    const entryType = value(descriptors, 'entryType');
    if (entryType !== 'sleep' && entryType !== 'sleepUntil') {
      throw new TypeError(`${field}.entryType is invalid`);
    }
    return {
      kind,
      entryType,
      entryId: validateWorkflowTerminalStructuralString(value(descriptors, 'entryId'), `${field}.entryId`),
      executionPath: canonicalPath(value(descriptors, 'executionPath'), `${field}.executionPath`),
    };
  }
  if (kind === 'container') {
    assertKeys(
      descriptors,
      ['kind', 'containerType', 'executionPath'],
      ['kind', 'containerType', 'executionPath'],
      field,
    );
    const containerType = value(descriptors, 'containerType');
    if (!['parallel', 'conditional', 'loop', 'foreach'].includes(String(containerType))) {
      throw new TypeError(`${field}.containerType is invalid`);
    }
    return {
      kind,
      containerType: containerType as 'parallel' | 'conditional' | 'loop' | 'foreach',
      executionPath: canonicalPath(value(descriptors, 'executionPath'), `${field}.executionPath`),
    };
  }
  throw new TypeError(`${field}.kind is invalid`);
}

function canonicalLoopDecision(input: unknown): WorkflowTerminalLoopDecision {
  const descriptors = getRecord(input, 'action.loopDecision');
  assertKeys(
    descriptors,
    ['loopType', 'conditionResult', 'previousIterationCount', 'nextIterationCount'],
    ['loopType', 'conditionResult', 'previousIterationCount', 'nextIterationCount'],
    'action.loopDecision',
  );
  const loopType = value(descriptors, 'loopType');
  const conditionResult = value(descriptors, 'conditionResult');
  if (loopType !== 'dowhile' && loopType !== 'dountil') throw new TypeError('action.loopDecision.loopType is invalid');
  if (typeof conditionResult !== 'boolean') throw new TypeError('action.loopDecision.conditionResult must be boolean');
  const previousIterationCount = canonicalSafeInteger(
    value(descriptors, 'previousIterationCount'),
    'action.loopDecision.previousIterationCount',
    MAX_TERMINAL_LOOP_ITERATIONS,
  );
  const nextIterationCount = canonicalSafeInteger(
    value(descriptors, 'nextIterationCount'),
    'action.loopDecision.nextIterationCount',
  );
  if (nextIterationCount !== previousIterationCount + 1) {
    throw new TypeError('action.loopDecision iteration counts are not consecutive');
  }
  return { loopType, conditionResult, previousIterationCount, nextIterationCount };
}

function canonicalAction(input: unknown): WorkflowTerminalContinuationAction {
  const descriptors = getRecord(input, 'action');
  const kind = value(descriptors, 'kind');
  const reason = value(descriptors, 'reason');
  if (kind === 'run-entry' && reason === 'next-step') {
    assertKeys(descriptors, ['kind', 'reason', 'target'], ['kind', 'reason', 'target'], 'action');
    return { kind, reason, target: canonicalRunTarget(value(descriptors, 'target'), 'action.target') };
  }
  if (kind === 'run-entry' && reason === 'loop-continue') {
    assertKeys(
      descriptors,
      ['kind', 'reason', 'target', 'loopDecision'],
      ['kind', 'reason', 'target', 'loopDecision'],
      'action',
    );
    const target = canonicalRunTarget(value(descriptors, 'target'), 'action.target');
    if (target.kind !== 'container' || target.containerType !== 'loop') {
      throw new TypeError('loop-continue requires a loop container target');
    }
    const loopDecision = canonicalLoopDecision(value(descriptors, 'loopDecision'));
    const continues =
      loopDecision.loopType === 'dowhile' ? loopDecision.conditionResult : !loopDecision.conditionResult;
    if (!continues) throw new TypeError('loop-continue contradicts the evaluated loop decision');
    return { kind, reason, target, loopDecision } as WorkflowTerminalContinuationAction;
  }
  if (kind === 'run-entry' && reason === 'foreach-continue') {
    assertKeys(descriptors, ['kind', 'reason', 'target'], ['kind', 'reason', 'target'], 'action');
    const target = getRecord(value(descriptors, 'target'), 'action.target');
    assertKeys(
      target,
      ['kind', 'stepId', 'containerPath', 'iterationIndex'],
      ['kind', 'stepId', 'containerPath', 'iterationIndex'],
      'action.target',
    );
    if (value(target, 'kind') !== 'foreach-iteration') throw new TypeError('foreach-continue requires an iteration');
    return {
      kind,
      reason,
      target: {
        kind: 'foreach-iteration',
        stepId: validateWorkflowTerminalStructuralString(value(target, 'stepId'), 'action.target.stepId'),
        containerPath: canonicalPath(value(target, 'containerPath'), 'action.target.containerPath'),
        iterationIndex: canonicalSafeInteger(value(target, 'iterationIndex'), 'action.target.iterationIndex'),
      },
    };
  }
  if (kind === 'complete-entry' && reason === 'loop-exit') {
    assertKeys(
      descriptors,
      ['kind', 'reason', 'target', 'loopDecision'],
      ['kind', 'reason', 'target', 'loopDecision'],
      'action',
    );
    const target = canonicalRunTarget(value(descriptors, 'target'), 'action.target');
    if (target.kind !== 'container' || target.containerType !== 'loop') {
      throw new TypeError('loop-exit requires a loop container target');
    }
    const loopDecision = canonicalLoopDecision(value(descriptors, 'loopDecision'));
    const exits = loopDecision.loopType === 'dowhile' ? !loopDecision.conditionResult : loopDecision.conditionResult;
    if (!exits) throw new TypeError('loop-exit contradicts the evaluated loop decision');
    return { kind, reason, target, loopDecision } as WorkflowTerminalContinuationAction;
  }
  if (kind === 'complete-entry' && (reason === 'parallel-continue' || reason === 'conditional-continue')) {
    assertKeys(descriptors, ['kind', 'reason', 'target'], ['kind', 'reason', 'target'], 'action');
    const target = canonicalRunTarget(value(descriptors, 'target'), 'action.target');
    const expected = reason === 'parallel-continue' ? 'parallel' : 'conditional';
    if (target.kind !== 'container' || target.containerType !== expected) {
      throw new TypeError(`${reason} requires a matching container target`);
    }
    return { kind, reason, target } as WorkflowTerminalContinuationAction;
  }
  if (kind === 'complete-entry' && reason === 'foreach-complete') {
    assertKeys(descriptors, ['kind', 'reason', 'target'], ['kind', 'reason', 'target'], 'action');
    const target = canonicalRunTarget(value(descriptors, 'target'), 'action.target');
    if (target.kind !== 'container' || target.containerType !== 'foreach') {
      throw new TypeError('foreach-complete requires a foreach container target');
    }
    return { kind, reason, target } as WorkflowTerminalContinuationAction;
  }
  if (kind === 'fail-parent' && reason === 'parent-fail') {
    assertKeys(descriptors, ['kind', 'reason'], ['kind', 'reason'], 'action');
    return { kind, reason };
  }
  if (kind === 'finish-parent' && reason === 'parent-end') {
    assertKeys(descriptors, ['kind', 'reason'], ['kind', 'reason'], 'action');
    return { kind, reason };
  }
  if (kind === 'cancel-parent' && reason === 'child-canceled') {
    assertKeys(descriptors, ['kind', 'reason'], ['kind', 'reason'], 'action');
    return { kind, reason };
  }
  if (kind === 'suspend-parent' && (reason === 'branch-suspended' || reason === 'foreach-suspended')) {
    assertKeys(descriptors, ['kind', 'reason', 'target'], ['kind', 'reason', 'target'], 'action');
    const target = canonicalRunTarget(value(descriptors, 'target'), 'action.target');
    if (target.kind !== 'container') throw new TypeError(`${reason} requires a container target`);
    if (
      (reason === 'branch-suspended' &&
        target.containerType !== 'parallel' &&
        target.containerType !== 'conditional') ||
      (reason === 'foreach-suspended' && target.containerType !== 'foreach')
    ) {
      throw new TypeError(`${reason} requires a matching container target`);
    }
    return { kind, reason, target } as WorkflowTerminalContinuationAction;
  }
  if (
    kind === 'wait' &&
    ['parallel-aggregation', 'conditional-aggregation', 'foreach-aggregation'].includes(String(reason))
  ) {
    assertKeys(descriptors, ['kind', 'reason', 'coordinate'], ['kind', 'reason', 'coordinate'], 'action');
    const coordinate = canonicalRunTarget(value(descriptors, 'coordinate'), 'action.coordinate');
    if (coordinate.kind !== 'container') throw new TypeError('wait requires a container coordinate');
    const expected = String(reason).split('-')[0];
    if (coordinate.containerType !== expected) throw new TypeError('wait reason does not match its container');
    return { kind, reason, coordinate } as WorkflowTerminalContinuationAction;
  }
  if (kind === 'noop' && reason === 'already-terminal') {
    assertKeys(descriptors, ['kind', 'reason'], ['kind', 'reason'], 'action');
    return { kind, reason };
  }
  if (kind === 'quarantine' && typeof reason === 'string' && QUARANTINE_REASONS.has(reason)) {
    assertKeys(descriptors, ['kind', 'reason', 'conflictDigest'], ['kind', 'reason', 'conflictDigest'], 'action');
    const conflictDigest = value(descriptors, 'conflictDigest');
    if (typeof conflictDigest !== 'string' || !SHA256.test(conflictDigest)) {
      throw new TypeError('action.conflictDigest must be a lowercase SHA-256 digest');
    }
    return { kind, reason, conflictDigest } as WorkflowTerminalContinuationAction;
  }
  throw new TypeError('action kind/reason combination is invalid');
}

const MERGE_PATCH_CONSTANTS = {
  resultWrite: 'source-coordinate',
  resultSource: 'retained-child-terminal-envelope',
  payloadWrite: 'preserve-parent-step-payload',
  metadataWrite: 'merge-child-and-bind-nested-run-id',
  stateWrite: 'replace-context-__state-from-retained-child',
  requestContextWrite: 'merge-from-retained-child',
  activeStepsWrite: 'derive-from-source-coordinate',
  snapshotTimestampWrite: 'storage-clock',
} as const;

function canonicalPatch(input: unknown): WorkflowTerminalParentPatch {
  const descriptors = getRecord(input, 'patch');
  const kind = value(descriptors, 'kind');
  if (kind === 'none') {
    assertKeys(descriptors, ['kind'], ['kind'], 'patch');
    return { kind };
  }
  if (kind !== 'merge-child-terminal') throw new TypeError('patch.kind is invalid');
  const commonKeys = ['kind', ...Object.keys(MERGE_PATCH_CONSTANTS), 'parentRunWrite', 'loopWrite'];
  assertKeys(descriptors, commonKeys, commonKeys, 'patch');
  for (const [key, expected] of Object.entries(MERGE_PATCH_CONSTANTS)) {
    if (value(descriptors, key) !== expected) throw new TypeError(`patch.${key} is invalid`);
  }
  const parent = getRecord(value(descriptors, 'parentRunWrite'), 'patch.parentRunWrite');
  const loop = getRecord(value(descriptors, 'loopWrite'), 'patch.loopWrite');
  let parentRunWrite: Exclude<WorkflowTerminalParentPatch, { kind: 'none' }>['parentRunWrite'];
  if (value(parent, 'kind') === 'preserve') {
    assertKeys(parent, ['kind'], ['kind'], 'patch.parentRunWrite');
    parentRunWrite = { kind: 'preserve' };
  } else if (value(parent, 'kind') === 'set') {
    assertKeys(
      parent,
      ['kind', 'status', 'resultSource', 'activePathSource'],
      ['kind', 'status', 'resultSource', 'activePathSource'],
      'patch.parentRunWrite',
    );
    const status = value(parent, 'status');
    if (!['success', 'failed', 'canceled'].includes(String(status))) {
      throw new TypeError('patch.parentRunWrite.status is invalid');
    }
    if (
      value(parent, 'resultSource') !== 'source-coordinate' ||
      value(parent, 'activePathSource') !== 'source-coordinate'
    ) {
      throw new TypeError('patch.parentRunWrite source is invalid');
    }
    parentRunWrite = {
      kind: 'set',
      status: status as 'success' | 'failed' | 'canceled',
      resultSource: 'source-coordinate',
      activePathSource: 'source-coordinate',
    };
  } else if (value(parent, 'kind') === 'set-suspended') {
    assertKeys(
      parent,
      ['kind', 'resultSource', 'activePathSource', 'suspendedPathsSource', 'resumeLabelsSource'],
      ['kind', 'resultSource', 'activePathSource', 'suspendedPathsSource', 'resumeLabelsSource'],
      'patch.parentRunWrite',
    );
    if (
      value(parent, 'resultSource') !== 'aggregate-container' ||
      value(parent, 'activePathSource') !== 'source-coordinate' ||
      value(parent, 'suspendedPathsSource') !== 'aggregate-container' ||
      value(parent, 'resumeLabelsSource') !== 'aggregate-container'
    ) {
      throw new TypeError('patch.parentRunWrite suspended source is invalid');
    }
    parentRunWrite = {
      kind: 'set-suspended',
      resultSource: 'aggregate-container',
      activePathSource: 'source-coordinate',
      suspendedPathsSource: 'aggregate-container',
      resumeLabelsSource: 'aggregate-container',
    };
  } else {
    throw new TypeError('patch.parentRunWrite.kind is invalid');
  }
  let loopWrite: Exclude<WorkflowTerminalParentPatch, { kind: 'none' }>['loopWrite'];
  if (value(loop, 'kind') === 'preserve') {
    assertKeys(loop, ['kind'], ['kind'], 'patch.loopWrite');
    loopWrite = { kind: 'preserve' };
  } else if (value(loop, 'kind') === 'set-iteration') {
    assertKeys(loop, ['kind', 'stepId', 'iterationCount'], ['kind', 'stepId', 'iterationCount'], 'patch.loopWrite');
    loopWrite = {
      kind: 'set-iteration',
      stepId: validateWorkflowTerminalStructuralString(value(loop, 'stepId'), 'patch.loopWrite.stepId'),
      iterationCount: canonicalSafeInteger(
        value(loop, 'iterationCount'),
        'patch.loopWrite.iterationCount',
        MAX_TERMINAL_LOOP_ITERATIONS + 1,
      ),
    };
  } else {
    throw new TypeError('patch.loopWrite.kind is invalid');
  }
  return { kind, ...MERGE_PATCH_CONSTANTS, parentRunWrite, loopWrite } as WorkflowTerminalParentPatch;
}

function validateMatrix(spec: WorkflowTerminalParentContinuationSpec): void {
  const { action, patch, childTerminalStatus, observedParentStatus } = spec;
  if (action.kind === 'noop') {
    if (!FINAL_PARENT_STATUSES.has(observedParentStatus) || patch.kind !== 'none') {
      throw new TypeError('already-terminal noop requires a terminal parent and no patch');
    }
    return;
  }
  if (action.kind === 'quarantine') {
    if (patch.kind !== 'none') throw new TypeError('quarantine cannot mutate the parent');
    return;
  }
  if (!ACTIVE_PARENT_STATUSES.has(observedParentStatus) || patch.kind !== 'merge-child-terminal') {
    throw new TypeError('parent application requires an active parent and merge patch');
  }
  if (
    action.kind !== 'wait' &&
    action.kind !== 'fail-parent' &&
    action.kind !== 'cancel-parent' &&
    observedParentStatus !== 'running'
  ) {
    throw new TypeError('dispatching parent action requires a running parent');
  }
  if (childTerminalStatus === 'canceled') {
    if (
      action.kind !== 'cancel-parent' ||
      patch.parentRunWrite.kind !== 'set' ||
      patch.parentRunWrite.status !== 'canceled'
    ) {
      throw new TypeError('canceled children require the child-canceled parent patch');
    }
  }
  const actionKind: string = action.kind;
  if (actionKind === 'fail-parent' && childTerminalStatus !== 'failed') {
    throw new TypeError('successful or canceled children cannot use parent-fail');
  }
  if (childTerminalStatus === 'failed') {
    if (
      action.kind !== 'fail-parent' ||
      patch.parentRunWrite.kind !== 'set' ||
      patch.parentRunWrite.status !== 'failed'
    ) {
      throw new TypeError('failed children require the parent-fail patch');
    }
  }
  if (action.kind === 'finish-parent') {
    if (
      childTerminalStatus !== 'success' ||
      patch.parentRunWrite.kind !== 'set' ||
      patch.parentRunWrite.status !== 'success'
    ) {
      throw new TypeError('parent-end requires a successful child and parent patch');
    }
  } else if (action.kind === 'cancel-parent') {
    if (
      childTerminalStatus !== 'canceled' ||
      patch.parentRunWrite.kind !== 'set' ||
      patch.parentRunWrite.status !== 'canceled'
    ) {
      throw new TypeError('child-canceled action requires a canceled child and parent patch');
    }
  } else if (action.kind === 'suspend-parent') {
    if (childTerminalStatus !== 'success' || patch.parentRunWrite.kind !== 'set-suspended') {
      throw new TypeError('aggregate suspension requires a successful child and suspended parent patch');
    }
  } else if (action.kind !== 'fail-parent' && patch.parentRunWrite.kind !== 'preserve') {
    throw new TypeError('non-terminal actions must preserve parent run state');
  }
  const loopAction = action.reason === 'loop-continue' || action.reason === 'loop-exit';
  if (loopAction) {
    if (
      patch.loopWrite.kind !== 'set-iteration' ||
      patch.loopWrite.iterationCount !== action.loopDecision.nextIterationCount
    ) {
      throw new TypeError('loop action and loop patch do not agree');
    }
  } else if (patch.loopWrite.kind !== 'preserve') {
    throw new TypeError('non-loop actions cannot update loop iteration state');
  }
}

/** @internal Materializes a strict isolated structural specification exactly once. */
export function canonicalizeWorkflowTerminalParentContinuationSpec(
  input: unknown,
): WorkflowTerminalParentContinuationSpec {
  const descriptors = getRecord(input, 'parent continuation');
  const keys = [
    'version',
    'terminalEffectKey',
    'terminalEffectPayloadHash',
    'executionMode',
    'expectedParentRevision',
    'graphFingerprint',
    'childTerminalStatus',
    'observedParentStatus',
    'source',
    'action',
    'patch',
  ];
  assertKeys(descriptors, keys, keys, 'parent continuation');
  if (value(descriptors, 'version') !== 1) throw new TypeError('parent continuation version must be 1');
  const terminalEffectKey = validateWorkflowTerminalStructuralString(
    value(descriptors, 'terminalEffectKey'),
    'terminalEffectKey',
    2_048,
  );
  const terminalEffectPayloadHash = value(descriptors, 'terminalEffectPayloadHash');
  if (typeof terminalEffectPayloadHash !== 'string' || !SHA256.test(terminalEffectPayloadHash)) {
    throw new TypeError('terminalEffectPayloadHash must be a lowercase SHA-256 digest');
  }
  const executionMode = value(descriptors, 'executionMode');
  if (executionMode !== 'continuous') throw new TypeError('executionMode must be continuous');
  const expectedParentRevision = validateWorkflowTerminalStructuralString(
    value(descriptors, 'expectedParentRevision'),
    'expectedParentRevision',
    MAX_TERMINAL_REVISION_LENGTH,
  );
  const graphFingerprint = value(descriptors, 'graphFingerprint');
  if (typeof graphFingerprint !== 'string' || !SHA256.test(graphFingerprint)) {
    throw new TypeError('graphFingerprint must be a lowercase SHA-256 digest');
  }
  const childTerminalStatus = value(descriptors, 'childTerminalStatus');
  if (typeof childTerminalStatus !== 'string' || !CHILD_STATUSES.has(childTerminalStatus)) {
    throw new TypeError('childTerminalStatus is invalid');
  }
  const observedParentStatus = value(descriptors, 'observedParentStatus');
  if (!ALL_PARENT_STATUSES.has(observedParentStatus as WorkflowRunStatus)) {
    throw new TypeError('observedParentStatus is invalid');
  }
  const spec = {
    version: 1,
    terminalEffectKey,
    terminalEffectPayloadHash,
    executionMode,
    expectedParentRevision,
    graphFingerprint,
    childTerminalStatus,
    observedParentStatus,
    source: canonicalSource(value(descriptors, 'source')),
    action: canonicalAction(value(descriptors, 'action')),
    patch: canonicalPatch(value(descriptors, 'patch')),
  } as WorkflowTerminalParentContinuationSpec;
  validateMatrix(spec);
  return spec;
}

function hashFramedParts(domain: string, parts: readonly string[]): string {
  const hash = createHash('sha256');
  const write = (part: string) => {
    const bytes = Buffer.from(part, 'utf8');
    hash.update(String(bytes.length));
    hash.update(':');
    hash.update(bytes);
  };
  write(domain);
  parts.forEach(write);
  return hash.digest('hex');
}

function contractHash(spec: WorkflowTerminalParentContinuationSpec): `sha256:${string}` {
  return `sha256:${hashFramedParts('mastra.workflow-terminal-parent-continuation.contract.v1', [JSON.stringify(spec)])}`;
}

/** @internal Creates an immutable structural contract without runtime payloads. */
export function createWorkflowTerminalParentContinuationContract(
  input: unknown,
): WorkflowTerminalParentContinuationContract {
  const spec = canonicalizeWorkflowTerminalParentContinuationSpec(input);
  return { ...spec, contractHash: contractHash(spec) } as WorkflowTerminalParentContinuationContract;
}

function contractSpec(value: WorkflowTerminalParentContinuationContract): WorkflowTerminalParentContinuationSpec {
  const descriptors = getRecord(value, 'parent continuation contract');
  const keys = [
    'version',
    'terminalEffectKey',
    'terminalEffectPayloadHash',
    'executionMode',
    'expectedParentRevision',
    'graphFingerprint',
    'childTerminalStatus',
    'observedParentStatus',
    'source',
    'action',
    'patch',
    'contractHash',
  ];
  assertKeys(descriptors, keys, keys, 'parent continuation contract');
  const hash = valueFrom(descriptors, 'contractHash');
  if (typeof hash !== 'string' || !SHA256.test(hash)) throw new TypeError('contractHash is invalid');
  return canonicalizeWorkflowTerminalParentContinuationSpec({
    version: valueFrom(descriptors, 'version'),
    terminalEffectKey: valueFrom(descriptors, 'terminalEffectKey'),
    terminalEffectPayloadHash: valueFrom(descriptors, 'terminalEffectPayloadHash'),
    executionMode: valueFrom(descriptors, 'executionMode'),
    expectedParentRevision: valueFrom(descriptors, 'expectedParentRevision'),
    graphFingerprint: valueFrom(descriptors, 'graphFingerprint'),
    childTerminalStatus: valueFrom(descriptors, 'childTerminalStatus'),
    observedParentStatus: valueFrom(descriptors, 'observedParentStatus'),
    source: valueFrom(descriptors, 'source'),
    action: valueFrom(descriptors, 'action'),
    patch: valueFrom(descriptors, 'patch'),
  });
}

function valueFrom(descriptors: Record<string, PropertyDescriptor>, key: string): unknown {
  return descriptors[key]!.value;
}

/** @internal Recomputes the canonical structural hash and rejects any forged record. */
export function validateWorkflowTerminalParentContinuationIntegrity(
  input: WorkflowTerminalParentContinuationContract,
): void {
  const descriptors = getRecord(input, 'parent continuation contract');
  const spec = contractSpec(input);
  if (value(descriptors, 'contractHash') !== contractHash(spec)) {
    throw new TypeError('Invalid workflow terminal parent continuation integrity');
  }
}

function samePath(left: readonly number[], right: readonly number[]): boolean {
  return left.length === right.length && left.every((entry, index) => entry === right[index]);
}

function materializeWorkflowTerminalForeachSidecar<T extends string>(
  value: unknown,
  upperBound: number,
  field: string,
  parseValue: (value: unknown, key: string) => T,
): Record<string, T> {
  if (value === undefined) return {};
  if (isProxy(value)) {
    throw new TypeError(`${field} is invalid`);
  }
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    throw new TypeError(`${field} is invalid`);
  }
  const prototype = Object.getPrototypeOf(value);
  if (prototype !== Object.prototype && prototype !== null) {
    throw new TypeError(`${field} is invalid`);
  }
  const descriptors = Object.getOwnPropertyDescriptors(value);
  const output: Record<string, T> = {};
  for (const key of Reflect.ownKeys(descriptors)) {
    const descriptor = descriptors[key as string];
    if (
      typeof key !== 'string' ||
      !/^(0|[1-9][0-9]*)$/.test(key) ||
      Number(key) >= upperBound ||
      !descriptor ||
      !('value' in descriptor) ||
      descriptor.enumerable !== true
    ) {
      throw new TypeError(`${field} is invalid`);
    }
    Object.defineProperty(output, key, {
      configurable: true,
      enumerable: true,
      value: parseValue(descriptor.value, key),
      writable: true,
    });
  }
  return output;
}

/** @internal Shared closed parser for persisted foreach terminal-state sidecars. */
export function materializeWorkflowTerminalForeachStates(value: unknown, upperBound: number): Record<string, string> {
  return materializeWorkflowTerminalForeachSidecar(
    value,
    upperBound,
    'Workflow terminal foreach iteration state sidecar',
    state => {
      if (state !== 'success' && state !== 'failed' && state !== 'canceled') {
        throw new TypeError('Workflow terminal foreach iteration state sidecar is invalid');
      }
      return state;
    },
  );
}

function materializeWorkflowTerminalForeachOwnership(value: unknown, upperBound: number): Record<string, string> {
  return materializeWorkflowTerminalForeachSidecar(
    value,
    upperBound,
    'Workflow terminal foreach iteration ownership sidecar',
    (runId, key) => {
      if (typeof runId !== 'string') {
        throw new TypeError('Workflow terminal foreach iteration ownership sidecar is invalid');
      }
      return validateWorkflowTerminalStructuralString(runId, `Workflow terminal foreach iteration ownership ${key}`);
    },
  );
}

function sourceExecutionPath(source: WorkflowTerminalResultCoordinate): readonly number[] {
  return source.kind === 'step' ? source.executionPath : source.containerPath;
}

function validateTargetBinding(
  target: WorkflowTerminalRunTarget,
  graph: WorkflowRunState['serializedStepGraph'],
): void {
  const resolved = resolveWorkflowTerminalGraphCoordinate(graph, target.executionPath);
  if (target.kind === 'step') {
    if (!['step', 'branch'].includes(resolved.kind) || !('stepId' in resolved) || resolved.stepId !== target.stepId) {
      throw new TypeError('continuation target does not match a graph step');
    }
  } else if (target.kind === 'entry') {
    if (resolved.kind !== target.entryType || resolved.entryId !== target.entryId) {
      throw new TypeError('continuation target does not match a graph entry');
    }
  } else {
    const matches =
      (resolved.kind === 'container' && resolved.containerType === target.containerType) ||
      (resolved.kind === 'loop' && target.containerType === 'loop') ||
      (resolved.kind === 'foreach' && resolved.iterationIndex === undefined && target.containerType === 'foreach');
    if (!matches) throw new TypeError('continuation target does not match a graph container');
  }
}

/** @internal Binds the structural contract to the exact effect, parent revision, status, and graph. */
export function validateWorkflowTerminalParentContinuationBinding(
  input: WorkflowTerminalParentContinuationContract,
  context: {
    effect: Extract<WorkflowTerminalEffectRecord, { kind: 'parent-workflow-step-end' }>;
    parentRevision: string;
    parentWorkflowName: string;
    parentSnapshot: WorkflowRunState;
    executionMode: 'continuous';
  },
): void {
  validateWorkflowTerminalParentContinuationIntegrity(input);
  const contract = input;
  const bindingContext = getRecord(context, 'parent continuation binding context');
  assertKeys(
    bindingContext,
    ['effect', 'parentRevision', 'parentWorkflowName', 'parentSnapshot', 'executionMode'],
    ['effect', 'parentRevision', 'parentWorkflowName', 'parentSnapshot', 'executionMode'],
    'parent continuation binding context',
  );
  const effect = value(bindingContext, 'effect') as Extract<
    WorkflowTerminalEffectRecord,
    { kind: 'parent-workflow-step-end' }
  >;
  const parentRevision = value(bindingContext, 'parentRevision');
  const parentWorkflowName = value(bindingContext, 'parentWorkflowName');
  const parentSnapshot = value(bindingContext, 'parentSnapshot') as WorkflowRunState;
  const executionMode = value(bindingContext, 'executionMode');
  const effectRecord = getRecord(effect, 'parent continuation effect');
  const parentSnapshotRecord = getRecord(parentSnapshot, 'parent continuation snapshot');
  const parentContext = getRecord(value(parentSnapshotRecord, 'context'), 'parent continuation snapshot context');
  const activeStepsPath = getRecord(
    value(parentSnapshotRecord, 'activeStepsPath'),
    'parent continuation active step paths',
  );
  const serializedStepGraph = value(
    parentSnapshotRecord,
    'serializedStepGraph',
  ) as WorkflowRunState['serializedStepGraph'];
  const effectRunId = value(effectRecord, 'runId');
  const effectParentExecutionPath = canonicalPath(
    value(effectRecord, 'parentExecutionPath'),
    'parent continuation effect parentExecutionPath',
  );
  if (
    contract.expectedParentRevision !== parentRevision ||
    contract.terminalEffectKey !== value(effectRecord, 'effectKey') ||
    contract.terminalEffectPayloadHash !== value(effectRecord, 'payloadHash') ||
    contract.executionMode !== executionMode ||
    parentWorkflowName !== value(effectRecord, 'parentWorkflowName') ||
    contract.graphFingerprint !== createWorkflowTerminalGraphFingerprint(serializedStepGraph) ||
    contract.observedParentStatus !== value(parentSnapshotRecord, 'status') ||
    contract.childTerminalStatus !== value(effectRecord, 'terminalStatus') ||
    value(parentSnapshotRecord, 'runId') !== value(effectRecord, 'parentRunId')
  ) {
    throw new TypeError('Workflow terminal parent continuation binding conflict');
  }
  if (contract.source.stepId !== value(effectRecord, 'parentStepId')) {
    throw new TypeError('Workflow terminal parent continuation source step conflict');
  }
  let boundForeachPayload: unknown[] | undefined;
  let boundForeachOutput: unknown[] | undefined;
  let boundForeachStates: Record<string, string> | undefined;
  const contractSourcePath =
    contract.source.kind === 'step'
      ? contract.source.executionPath
      : [...contract.source.containerPath, contract.source.iterationIndex];
  if (!samePath(contractSourcePath, effectParentExecutionPath)) {
    throw new TypeError('Workflow terminal parent continuation source path conflict');
  }
  if (contract.action.kind === 'quarantine') {
    if (contract.action.reason === 'graph-conflict') {
      let graphConflict = false;
      try {
        const resolved = resolveWorkflowTerminalGraphCoordinate(serializedStepGraph, contractSourcePath);
        graphConflict =
          contract.source.kind === 'step'
            ? !(
                ['step', 'branch', 'loop'].includes(resolved.kind) &&
                'stepId' in resolved &&
                resolved.stepId === contract.source.stepId
              )
            : !(resolved.kind === 'foreach' && resolved.stepId === contract.source.stepId);
      } catch {
        graphConflict = true;
      }
      if (!graphConflict) throw new TypeError('Workflow terminal graph-conflict quarantine has no graph conflict');
    }
    return;
  }
  if (contract.source.kind === 'step') {
    const resolved = resolveWorkflowTerminalGraphCoordinate(serializedStepGraph, contract.source.executionPath);
    if (
      !['step', 'branch', 'loop'].includes(resolved.kind) ||
      !('stepId' in resolved) ||
      resolved.stepId !== contract.source.stepId
    ) {
      throw new TypeError('Workflow terminal parent continuation scalar source is invalid');
    }
  } else {
    const fullPath = [...contract.source.containerPath, contract.source.iterationIndex];
    const resolved = resolveWorkflowTerminalGraphCoordinate(serializedStepGraph, fullPath);
    if (resolved.kind !== 'foreach' || resolved.stepId !== contract.source.stepId) {
      throw new TypeError('Workflow terminal parent continuation foreach source is invalid');
    }
    if (contract.patch.kind === 'merge-child-terminal') {
      const current = getRecord(
        value(parentContext, contract.source.stepId),
        'Workflow terminal parent continuation foreach source state',
      );
      const payload = materializeDenseDataArray(
        value(current, 'payload'),
        'parent foreach payload',
        MAX_TERMINAL_FOREACH_ENTRIES,
      );
      if (contract.source.iterationIndex >= payload.length) {
        throw new TypeError('Workflow terminal parent continuation foreach index is out of bounds');
      }
      const output = materializeDenseDataArray(
        value(current, 'output'),
        'parent foreach output',
        MAX_TERMINAL_FOREACH_ENTRIES,
      );
      if (contract.source.iterationIndex >= output.length) {
        throw new TypeError('Workflow terminal parent continuation foreach source iteration was not started');
      }
      if (output.length > payload.length) {
        throw new TypeError('Workflow terminal parent continuation foreach output exceeds its input payload');
      }
      boundForeachPayload = payload;
      boundForeachOutput = output;
      const metadataValue = value(current, 'metadata');
      const metadata = metadataValue === undefined ? undefined : getRecord(metadataValue, 'parent foreach metadata');
      const workflowMetadataValue = metadata && value(metadata, '__workflow_meta');
      const workflowMetadata =
        workflowMetadataValue === undefined
          ? undefined
          : getRecord(workflowMetadataValue, 'parent foreach workflow metadata');
      const iterationRunsValue = workflowMetadata && value(workflowMetadata, WORKFLOW_TERMINAL_FOREACH_RUN_KEY);
      const iterationRuns = materializeWorkflowTerminalForeachOwnership(iterationRunsValue, output.length);
      if (iterationRuns[String(contract.source.iterationIndex)] !== effectRunId) {
        throw new TypeError('Workflow terminal foreach source is not owned by the terminal child run');
      }
      const states = materializeWorkflowTerminalForeachStates(
        workflowMetadata && value(workflowMetadata, WORKFLOW_TERMINAL_FOREACH_STATE_KEY),
        output.length,
      );
      boundForeachStates = states;
      const state = states[String(contract.source.iterationIndex)];
      if (state === 'success' || state === 'failed' || state === 'canceled') {
        throw new TypeError('Workflow terminal foreach source iteration is already terminal');
      }
    }
  }
  const { action } = contract;
  if (contract.patch.kind === 'merge-child-terminal' && contract.source.kind === 'step') {
    const existing = getRecord(
      value(parentContext, contract.source.stepId),
      'Workflow terminal parent continuation scalar source state',
    );
    const metadata = getRecord(value(existing, 'metadata'), 'Workflow terminal parent continuation scalar metadata');
    const activePath = canonicalPath(
      value(activeStepsPath, contract.source.stepId),
      'Workflow terminal parent continuation active source path',
    );
    if (
      value(existing, 'status') !== 'running' ||
      value(metadata, 'nestedRunId') !== effectRunId ||
      !samePath(activePath, contract.source.executionPath)
    ) {
      throw new TypeError('Workflow terminal parent continuation source is not the active child run');
    }
  }
  if ('target' in action) {
    if (action.target.kind === 'foreach-iteration') {
      const path = [...action.target.containerPath, action.target.iterationIndex];
      const resolved = resolveWorkflowTerminalGraphCoordinate(serializedStepGraph, path);
      if (resolved.kind !== 'foreach' || resolved.stepId !== action.target.stepId) {
        throw new TypeError('Workflow terminal parent continuation foreach target is invalid');
      }
    } else {
      validateTargetBinding(action.target, serializedStepGraph);
    }
  }
  if ('coordinate' in action) validateTargetBinding(action.coordinate, serializedStepGraph);
  const sourcePath = sourceExecutionPath(contract.source);
  const sourceEntry = resolveWorkflowTerminalGraphCoordinate(
    serializedStepGraph,
    contract.source.kind === 'step'
      ? contract.source.executionPath
      : [...contract.source.containerPath, contract.source.iterationIndex],
  );
  if (action.reason === 'next-step') {
    if (
      contract.source.kind !== 'step' ||
      sourceEntry.kind !== 'step' ||
      !samePath(action.target.executionPath, [sourcePath[0]! + 1])
    ) {
      throw new TypeError('Workflow terminal next-step target is not the immediate sequential successor');
    }
  }
  if (action.reason === 'parent-end') {
    if (
      contract.source.kind !== 'step' ||
      sourceEntry.kind !== 'step' ||
      contract.source.executionPath.length !== 1 ||
      contract.source.executionPath[0] !== serializedStepGraph.length - 1
    ) {
      throw new TypeError('Workflow terminal parent-end source is not the final sequential entry');
    }
    if (Object.keys(activeStepsPath).some(stepId => stepId !== contract.source.stepId)) {
      throw new TypeError('Workflow terminal parent-end has unrelated active steps');
    }
  }
  if (
    action.reason === 'parallel-continue' ||
    action.reason === 'conditional-continue' ||
    action.reason === 'parallel-aggregation' ||
    action.reason === 'conditional-aggregation' ||
    action.reason === 'branch-suspended'
  ) {
    const containerType =
      action.reason === 'branch-suspended'
        ? action.target.containerType
        : action.reason.startsWith('parallel')
          ? 'parallel'
          : 'conditional';
    const target = 'target' in action ? action.target : action.coordinate;
    if (
      contract.source.kind !== 'step' ||
      sourceEntry.kind !== 'branch' ||
      sourceEntry.containerType !== containerType ||
      target.kind !== 'container' ||
      !samePath(target.executionPath, [sourcePath[0]!])
    ) {
      throw new TypeError('Workflow terminal branch action is not bound to its source container');
    }
    const branch = serializedStepGraph[sourcePath[0]!];
    if (!branch || (branch.type !== 'parallel' && branch.type !== 'conditional')) {
      throw new TypeError('Workflow terminal branch container is missing');
    }
    const siblingStatuses = branch.steps.map(entry => {
      if (entry.step.id === contract.source.stepId) return contract.childTerminalStatus;
      const sibling = getRecord(
        value(parentContext, entry.step.id),
        `Workflow terminal parent continuation sibling ${entry.step.id}`,
      );
      return value(sibling, 'status');
    });
    const hasInvalidTerminal = siblingStatuses.some(
      status => status === 'failed' || status === 'canceled' || (containerType === 'parallel' && status === 'skipped'),
    );
    const allComplete = siblingStatuses.every(
      status => status === 'success' || (containerType === 'conditional' && status === 'skipped'),
    );
    const hasSuspended = siblingStatuses.some(status => status === 'suspended');
    const allAccounted = siblingStatuses.every(
      status =>
        status === 'success' || status === 'suspended' || (containerType === 'conditional' && status === 'skipped'),
    );
    const isCompleteAction = action.reason === 'parallel-continue' || action.reason === 'conditional-continue';
    const actionMatchesState =
      action.reason === 'branch-suspended'
        ? allAccounted && hasSuspended
        : isCompleteAction
          ? allComplete
          : !allAccounted;
    if (hasInvalidTerminal || !actionMatchesState) {
      throw new TypeError('Workflow terminal branch aggregation action conflicts with sibling state');
    }
  }
  if (
    action.reason === 'foreach-continue' ||
    action.reason === 'foreach-complete' ||
    action.reason === 'foreach-aggregation' ||
    action.reason === 'foreach-suspended'
  ) {
    if (contract.source.kind !== 'foreach-iteration' || sourceEntry.kind !== 'foreach') {
      throw new TypeError('Workflow terminal foreach action requires a foreach iteration source');
    }
    const foreachSource = contract.source;
    const targetPath =
      action.reason === 'foreach-continue'
        ? action.target.containerPath
        : action.reason === 'foreach-complete' || action.reason === 'foreach-suspended'
          ? action.target.executionPath
          : action.coordinate.executionPath;
    if (!samePath(targetPath, foreachSource.containerPath)) {
      throw new TypeError('Workflow terminal foreach action is not bound to its source container');
    }
    if (!boundForeachPayload || !boundForeachOutput || !boundForeachStates) {
      throw new TypeError('Workflow terminal foreach source state is invalid');
    }
    const foreachPayload = boundForeachPayload;
    const foreachOutput = boundForeachOutput;
    const states = boundForeachStates;
    const isTerminal = (index: number) =>
      index === foreachSource.iterationIndex ||
      ['success', 'failed', 'canceled'].includes(String(states[String(index)]));
    if (
      foreachOutput.some(
        (_, index) =>
          index !== foreachSource.iterationIndex &&
          (states[String(index)] === 'failed' || states[String(index)] === 'canceled'),
      )
    ) {
      throw new TypeError('Workflow terminal foreach state contains a failed or canceled sibling');
    }
    const isSuspended = (index: number) => {
      if (index === foreachSource.iterationIndex) return false;
      if (['success', 'failed', 'canceled'].includes(String(states[String(index)]))) return false;
      const output = foreachOutput[index];
      return output !== null && typeof output === 'object' && (output as { status?: unknown }).status === 'suspended';
    };
    const hasSuspended = foreachOutput.some((_, index) => isSuspended(index));
    const hasPendingStarted = foreachOutput.some((_, index) => !isTerminal(index) && !isSuspended(index));
    const nextIndex = foreachOutput.length;
    if (action.reason === 'foreach-continue') {
      if (
        hasPendingStarted ||
        nextIndex >= foreachPayload.length ||
        action.target.iterationIndex !== nextIndex ||
        action.target.stepId !== foreachSource.stepId
      ) {
        throw new TypeError('Workflow terminal foreach continuation is not the next eligible iteration');
      }
    } else if (action.reason === 'foreach-complete') {
      if (hasPendingStarted || hasSuspended || nextIndex < foreachPayload.length) {
        throw new TypeError('Workflow terminal foreach container is not complete');
      }
    } else if (action.reason === 'foreach-suspended') {
      if (hasPendingStarted || !hasSuspended || nextIndex < foreachPayload.length) {
        throw new TypeError('Workflow terminal foreach container is not ready to suspend');
      }
    } else if (!hasPendingStarted) {
      throw new TypeError('Workflow terminal foreach aggregation wait has no pending iteration');
    }
  }
  if (action.reason === 'loop-continue' || action.reason === 'loop-exit') {
    const loopAction = action as Extract<WorkflowTerminalContinuationAction, { reason: 'loop-continue' | 'loop-exit' }>;
    if (!samePath(loopAction.target.executionPath, [sourcePath[0]!])) {
      throw new TypeError('Workflow terminal parent continuation loop target is not the source loop');
    }
    if (sourceEntry.kind !== 'loop' || sourceEntry.loopType !== loopAction.loopDecision.loopType) {
      throw new TypeError('Workflow terminal parent continuation loop decision does not match the source loop');
    }
    const currentLoop = getRecord(
      value(parentContext, contract.source.stepId),
      'Workflow terminal parent continuation loop source state',
    );
    const loopMetadataValue = value(currentLoop, 'metadata');
    const loopMetadata =
      loopMetadataValue === undefined
        ? undefined
        : getRecord(loopMetadataValue, 'Workflow terminal parent continuation loop metadata');
    const previousIterationCount = (loopMetadata && value(loopMetadata, 'iterationCount')) ?? 0;
    if (previousIterationCount !== loopAction.loopDecision.previousIterationCount) {
      throw new TypeError('Workflow terminal parent continuation loop count conflicts with the parent snapshot');
    }
    if (contract.patch.kind !== 'merge-child-terminal' || contract.patch.loopWrite.kind !== 'set-iteration') {
      throw new TypeError('Workflow terminal parent continuation loop patch is missing');
    }
    if (contract.patch.loopWrite.stepId !== contract.source.stepId) {
      throw new TypeError('Workflow terminal parent continuation loop step conflict');
    }
  }
}

/** @internal Produces a deeply isolated canonical copy. */
export function copyWorkflowTerminalParentContinuationContract(
  input: WorkflowTerminalParentContinuationContract,
): WorkflowTerminalParentContinuationContract {
  validateWorkflowTerminalParentContinuationIntegrity(input);
  const descriptors = getRecord(input, 'parent continuation contract');
  return {
    ...canonicalizeWorkflowTerminalParentContinuationSpec({
      version: value(descriptors, 'version'),
      terminalEffectKey: value(descriptors, 'terminalEffectKey'),
      terminalEffectPayloadHash: value(descriptors, 'terminalEffectPayloadHash'),
      executionMode: value(descriptors, 'executionMode'),
      expectedParentRevision: value(descriptors, 'expectedParentRevision'),
      graphFingerprint: value(descriptors, 'graphFingerprint'),
      childTerminalStatus: value(descriptors, 'childTerminalStatus'),
      observedParentStatus: value(descriptors, 'observedParentStatus'),
      source: value(descriptors, 'source'),
      action: value(descriptors, 'action'),
      patch: value(descriptors, 'patch'),
    }),
    contractHash: value(descriptors, 'contractHash'),
  } as WorkflowTerminalParentContinuationContract;
}
