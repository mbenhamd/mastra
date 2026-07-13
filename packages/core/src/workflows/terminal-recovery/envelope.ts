import { Buffer } from 'node:buffer';
import { createHash } from 'node:crypto';
import { isProxy } from 'node:util/types';
import { isInfrastructureRequestContextKey } from '../../request-context';
import {
  MAX_TERMINAL_PATH_LENGTH,
  createWorkflowTerminalGraphFingerprint,
  resolveWorkflowTerminalGraphCoordinate,
  validateWorkflowTerminalStructuralString,
} from '../terminal-continuation/graph-fingerprint';
import type { WorkflowTerminalSha256 } from '../terminal-continuation/types';
import type { SerializedStepFlowEntry } from '../types';
import {
  MAX_WORKFLOW_TERMINAL_RECOVERY_ANCESTRY_DEPTH,
  MAX_WORKFLOW_TERMINAL_RECOVERY_ENVELOPE_BYTES,
  getWorkflowTerminalCanonicalJson,
  materializeWorkflowTerminalCanonicalJson,
  materializeWorkflowTerminalCanonicalJsonObject,
  materializeWorkflowTerminalCanonicalJsonObjectPatch,
} from './canonical-json';
import type {
  WorkflowTerminalCanonicalJsonObject,
  WorkflowTerminalRecoveryAncestryV1,
  WorkflowTerminalRecoveryEnvelopeExpectedBinding,
  WorkflowTerminalRecoveryEnvelopeInputV1,
  WorkflowTerminalRecoveryEnvelopeRecordV1,
  WorkflowTerminalRecoveryEnvelopeV1,
  WorkflowTerminalRecoveryGraphBinding,
  WorkflowTerminalRecoveryParentFrameV1,
  WorkflowTerminalRecoverySource,
  WorkflowTerminalRecoveryStatus,
} from './types';

const SHA256 = /^sha256:[a-f0-9]{64}$/;

function fail(field: string, reason: string): never {
  throw new TypeError(`Invalid workflow terminal recovery envelope at ${field}: ${reason}`);
}

function descriptors(value: unknown, field: string): Record<string, PropertyDescriptor> {
  if (value === null || typeof value !== 'object' || Array.isArray(value) || isProxy(value)) {
    fail(field, 'must be a plain data object');
  }
  const prototype = Object.getPrototypeOf(value);
  if (prototype !== Object.prototype && prototype !== null) fail(field, 'must be a plain data object');
  const result = Object.getOwnPropertyDescriptors(value);
  for (const key of Reflect.ownKeys(result)) {
    const descriptor = result[key as string];
    if (typeof key !== 'string' || !descriptor || !('value' in descriptor) || !descriptor.enumerable) {
      fail(field, 'must contain only enumerable string data fields');
    }
  }
  return result;
}

function exactKeys(
  input: Record<string, PropertyDescriptor>,
  allowed: readonly string[],
  required: readonly string[],
  field: string,
): void {
  const allowedSet = new Set(allowed);
  if (Object.keys(input).some(key => !allowedSet.has(key))) fail(field, 'contains unknown fields');
  if (required.some(key => !Object.hasOwn(input, key))) fail(field, 'is missing required fields');
}

function read(input: Record<string, PropertyDescriptor>, key: string): unknown {
  return input[key]?.value;
}

function sha256(value: unknown, field: string): WorkflowTerminalSha256 {
  if (typeof value !== 'string' || !SHA256.test(value)) fail(field, 'must be a lowercase SHA-256 digest');
  return value as WorkflowTerminalSha256;
}

function status(value: unknown, field: string): WorkflowTerminalRecoveryStatus {
  if (value !== 'success' && value !== 'failed' && value !== 'canceled') fail(field, 'terminal status is invalid');
  return value;
}

function safeInteger(value: unknown, field: string): number {
  if (!Number.isSafeInteger(value) || (value as number) < 0) fail(field, 'must be a non-negative safe integer');
  return value === 0 ? 0 : (value as number);
}

function path(value: unknown, field: string): number[] {
  if (!Array.isArray(value) || isProxy(value)) fail(field, 'must be a dense data-only path');
  const input = Object.getOwnPropertyDescriptors(value) as unknown as Record<PropertyKey, PropertyDescriptor>;
  const length = input.length?.value;
  if (!Number.isSafeInteger(length) || length < 1 || length > MAX_TERMINAL_PATH_LENGTH) {
    fail(field, 'path length is invalid');
  }
  const output = Array.from({ length }, (_, index) => {
    const descriptor = input[String(index)];
    if (!descriptor || !('value' in descriptor) || !descriptor.enumerable)
      fail(field, 'path must be dense and data-only');
    return safeInteger(descriptor.value, `${field}[${index}]`);
  });
  if (
    Reflect.ownKeys(input).some(
      key => key !== 'length' && (typeof key !== 'string' || !/^(0|[1-9][0-9]*)$/.test(key) || Number(key) >= length),
    )
  ) {
    fail(field, 'path must be dense and data-only');
  }
  return output;
}

function denseArray(value: unknown, field: string, maxLength: number): unknown[] {
  if (!Array.isArray(value) || isProxy(value)) fail(field, 'must be a dense data-only array');
  const input = Object.getOwnPropertyDescriptors(value) as unknown as Record<PropertyKey, PropertyDescriptor>;
  const length = input.length?.value;
  if (!Number.isSafeInteger(length) || length < 0 || length > maxLength) fail(field, 'array length is invalid');
  const output = Array.from({ length }, (_, index) => {
    const descriptor = input[String(index)];
    if (!descriptor || !('value' in descriptor) || !descriptor.enumerable) fail(field, 'must be dense and data-only');
    return descriptor.value;
  });
  if (
    Reflect.ownKeys(input).some(
      key => key !== 'length' && (typeof key !== 'string' || !/^(0|[1-9][0-9]*)$/.test(key) || Number(key) >= length),
    )
  ) {
    fail(field, 'must be dense and data-only');
  }
  return output;
}

function source(value: unknown, field: string): WorkflowTerminalRecoverySource {
  const input = descriptors(value, field);
  const kind = read(input, 'kind');
  if (kind === 'step') {
    exactKeys(input, ['kind', 'stepId', 'executionPath'], ['kind', 'stepId', 'executionPath'], field);
    return {
      kind,
      stepId: validateWorkflowTerminalStructuralString(read(input, 'stepId'), `${field}.stepId`),
      executionPath: path(read(input, 'executionPath'), `${field}.executionPath`),
    };
  }
  if (kind === 'foreach-iteration') {
    exactKeys(
      input,
      ['kind', 'stepId', 'containerPath', 'iterationIndex'],
      ['kind', 'stepId', 'containerPath', 'iterationIndex'],
      field,
    );
    return {
      kind,
      stepId: validateWorkflowTerminalStructuralString(read(input, 'stepId'), `${field}.stepId`),
      containerPath: path(read(input, 'containerPath'), `${field}.containerPath`),
      iterationIndex: safeInteger(read(input, 'iterationIndex'), `${field}.iterationIndex`),
    };
  }
  return fail(`${field}.kind`, 'source kind is invalid');
}

function frame(value: unknown, field: string): WorkflowTerminalRecoveryParentFrameV1 {
  const input = descriptors(value, field);
  const keys = [
    'version',
    'childWorkflowName',
    'childRunId',
    'parentWorkflowName',
    'parentRunId',
    'parentGraphFingerprint',
    'source',
    'inputPointer',
    'resultPointer',
    'resumeMetadata',
  ];
  exactKeys(input, keys, keys, field);
  if (read(input, 'version') !== 1) fail(`${field}.version`, 'must be 1');
  const childWorkflowName = validateWorkflowTerminalStructuralString(
    read(input, 'childWorkflowName'),
    `${field}.childWorkflowName`,
  );
  const childRunId = validateWorkflowTerminalStructuralString(read(input, 'childRunId'), `${field}.childRunId`);
  const parentWorkflowName = validateWorkflowTerminalStructuralString(
    read(input, 'parentWorkflowName'),
    `${field}.parentWorkflowName`,
  );
  const parentRunId = validateWorkflowTerminalStructuralString(read(input, 'parentRunId'), `${field}.parentRunId`);
  const materializedSource = source(read(input, 'source'), `${field}.source`);

  const inputPointer = descriptors(read(input, 'inputPointer'), `${field}.inputPointer`);
  exactKeys(inputPointer, ['kind', 'stepId'], ['kind', 'stepId'], `${field}.inputPointer`);
  if (read(inputPointer, 'kind') !== 'parent-source-payload') fail(`${field}.inputPointer.kind`, 'is invalid');
  const inputStepId = validateWorkflowTerminalStructuralString(
    read(inputPointer, 'stepId'),
    `${field}.inputPointer.stepId`,
  );
  if (inputStepId !== materializedSource.stepId) fail(`${field}.inputPointer.stepId`, 'does not match source');

  const resultPointer = descriptors(read(input, 'resultPointer'), `${field}.resultPointer`);
  exactKeys(
    resultPointer,
    ['kind', 'workflowName', 'runId'],
    ['kind', 'workflowName', 'runId'],
    `${field}.resultPointer`,
  );
  if (read(resultPointer, 'kind') !== 'retained-terminal-result') fail(`${field}.resultPointer.kind`, 'is invalid');
  const resultWorkflowName = validateWorkflowTerminalStructuralString(
    read(resultPointer, 'workflowName'),
    `${field}.resultPointer.workflowName`,
  );
  const resultRunId = validateWorkflowTerminalStructuralString(
    read(resultPointer, 'runId'),
    `${field}.resultPointer.runId`,
  );
  if (resultWorkflowName !== childWorkflowName || resultRunId !== childRunId) {
    fail(`${field}.resultPointer`, 'does not match child identity');
  }

  const resume = descriptors(read(input, 'resumeMetadata'), `${field}.resumeMetadata`);
  exactKeys(resume, ['wasResume', 'resumeSteps'], ['wasResume', 'resumeSteps'], `${field}.resumeMetadata`);
  if (typeof read(resume, 'wasResume') !== 'boolean') fail(`${field}.resumeMetadata.wasResume`, 'must be boolean');
  const resumeSteps = denseArray(
    read(resume, 'resumeSteps'),
    `${field}.resumeMetadata.resumeSteps`,
    MAX_TERMINAL_PATH_LENGTH,
  ).map((entry, index) =>
    validateWorkflowTerminalStructuralString(entry, `${field}.resumeMetadata.resumeSteps[${index}]`),
  );
  const wasResume = read(resume, 'wasResume') as boolean;

  return {
    version: 1,
    childWorkflowName,
    childRunId,
    parentWorkflowName,
    parentRunId,
    parentGraphFingerprint: sha256(read(input, 'parentGraphFingerprint'), `${field}.parentGraphFingerprint`),
    source: materializedSource,
    inputPointer: { kind: 'parent-source-payload', stepId: inputStepId },
    resultPointer: { kind: 'retained-terminal-result', workflowName: resultWorkflowName, runId: resultRunId },
    resumeMetadata: { wasResume, resumeSteps },
  };
}

export function materializeWorkflowTerminalRecoveryAncestry(input: unknown): WorkflowTerminalRecoveryAncestryV1 {
  const values = denseArray(input, 'ancestry', MAX_WORKFLOW_TERMINAL_RECOVERY_ANCESTRY_DEPTH);
  const ancestry = values.map((value, index) => frame(value, `ancestry[${index}]`));
  const seen = new Set<string>();
  for (let index = 0; index < ancestry.length; index++) {
    const current = ancestry[index]!;
    const childKey = JSON.stringify([current.childWorkflowName, current.childRunId]);
    const parentKey = JSON.stringify([current.parentWorkflowName, current.parentRunId]);
    if (seen.has(childKey) || childKey === parentKey) fail(`ancestry[${index}]`, 'contains an identity cycle');
    seen.add(childKey);
    if (index + 1 < ancestry.length) {
      const next = ancestry[index + 1]!;
      if (next.childWorkflowName !== current.parentWorkflowName || next.childRunId !== current.parentRunId) {
        fail(`ancestry[${index + 1}]`, 'is not continuous with its child frame');
      }
    } else if (seen.has(parentKey)) {
      fail(`ancestry[${index}]`, 'contains an identity cycle');
    }
  }
  return ancestry;
}

export function copyWorkflowTerminalRecoveryAncestry(
  ancestry: WorkflowTerminalRecoveryAncestryV1,
): WorkflowTerminalRecoveryAncestryV1 {
  return materializeWorkflowTerminalRecoveryAncestry(ancestry);
}

function hashFramed(domain: string, payload: string): WorkflowTerminalSha256 {
  const hash = createHash('sha256');
  for (const value of [domain, payload]) {
    const bytes = Buffer.from(value, 'utf8');
    const length = Buffer.allocUnsafe(4);
    length.writeUInt32BE(bytes.length);
    hash.update(length);
    hash.update(bytes);
  }
  return `sha256:${hash.digest('hex')}`;
}

export function getWorkflowTerminalRecoveryAncestryHash(
  input: WorkflowTerminalRecoveryAncestryV1,
): WorkflowTerminalSha256 {
  const ancestry = materializeWorkflowTerminalRecoveryAncestry(input);
  return hashFramed(
    'mastra.workflow-terminal-recovery-ancestry.v1',
    getWorkflowTerminalCanonicalJson(materializeWorkflowTerminalCanonicalJson(ancestry, 'ancestry')),
  );
}

function validateTerminalResult(
  result: WorkflowTerminalCanonicalJsonObject,
  expected: WorkflowTerminalRecoveryStatus,
): void {
  if (result.status !== expected) fail('terminalResult.status', 'does not match terminalStatus');
  const hasError = Object.hasOwn(result, 'error') && result.error !== null;
  if (expected === 'failed' && !hasError) fail('terminalResult.error', 'failed result requires an error');
  if (expected !== 'failed' && Object.hasOwn(result, 'error'))
    fail('terminalResult.error', 'non-failed result cannot contain error');
  if (expected === 'failed') {
    const error = result.error;
    if (error === null || typeof error !== 'object' || Array.isArray(error)) {
      fail('terminalResult.error', 'must be a serialized error');
    }
    if (typeof error.name !== 'string' || typeof error.message !== 'string') {
      fail('terminalResult.error', 'must contain string name and message fields');
    }
    if (Object.hasOwn(error, 'stack') && typeof error.stack !== 'string') {
      fail('terminalResult.error.stack', 'must be a string');
    }
  }
  const startedAt = result.startedAt;
  const endedAt = result.endedAt;
  if (startedAt !== undefined) safeInteger(startedAt, 'terminalResult.startedAt');
  if (endedAt !== undefined) safeInteger(endedAt, 'terminalResult.endedAt');
  if (typeof startedAt === 'number' && typeof endedAt === 'number' && startedAt > endedAt) {
    fail('terminalResult', 'timestamps are not monotonic');
  }
}

export function materializeWorkflowTerminalRecoveryEnvelope(
  value: WorkflowTerminalRecoveryEnvelopeInputV1 | unknown,
): WorkflowTerminalRecoveryEnvelopeV1 {
  const input = descriptors(value, 'envelope');
  const allowed = [
    'version',
    'workflowName',
    'runId',
    'terminalStatus',
    'executionMode',
    'terminalResult',
    'finalState',
    'requestContextPatch',
    'childGraphFingerprint',
    'ancestry',
  ];
  exactKeys(
    input,
    allowed,
    allowed.filter(key => key !== 'requestContextPatch'),
    'envelope',
  );
  if (read(input, 'version') !== 1) fail('version', 'must be 1');
  if (read(input, 'executionMode') !== 'continuous') fail('executionMode', 'must be continuous');
  const workflowName = validateWorkflowTerminalStructuralString(read(input, 'workflowName'), 'workflowName');
  const runId = validateWorkflowTerminalStructuralString(read(input, 'runId'), 'runId');
  const terminalStatus = status(read(input, 'terminalStatus'), 'terminalStatus');
  const terminalResult = materializeWorkflowTerminalCanonicalJsonObject(
    read(input, 'terminalResult'),
    'terminalResult',
  );
  validateTerminalResult(terminalResult, terminalStatus);
  const finalState = materializeWorkflowTerminalCanonicalJsonObject(read(input, 'finalState'), 'finalState');
  const requestContextPatch = materializeWorkflowTerminalCanonicalJsonObjectPatch(
    read(input, 'requestContextPatch'),
    'requestContextPatch',
  );
  if (Object.keys(requestContextPatch).some(isInfrastructureRequestContextKey)) {
    fail('requestContextPatch', 'contains an infrastructure-owned key');
  }
  const ancestry = materializeWorkflowTerminalRecoveryAncestry(read(input, 'ancestry'));
  if (ancestry[0] && (ancestry[0].childWorkflowName !== workflowName || ancestry[0].childRunId !== runId)) {
    fail('ancestry[0]', 'does not start at the terminal child');
  }
  const envelope: WorkflowTerminalRecoveryEnvelopeV1 = {
    version: 1,
    workflowName,
    runId,
    terminalStatus,
    executionMode: 'continuous',
    terminalResult,
    finalState,
    requestContextPatch,
    childGraphFingerprint: sha256(read(input, 'childGraphFingerprint'), 'childGraphFingerprint'),
    ancestry,
  };
  const bytes = Buffer.byteLength(JSON.stringify(envelope), 'utf8');
  if (bytes > MAX_WORKFLOW_TERMINAL_RECOVERY_ENVELOPE_BYTES) fail('envelope', 'byte limit exceeded');
  // Enforce the aggregate node/entry/depth limits across all envelope fields,
  // not only inside each independently materialized payload.
  return materializeWorkflowTerminalCanonicalJson(
    envelope,
    'envelope',
  ) as unknown as WorkflowTerminalRecoveryEnvelopeV1;
}

export function getWorkflowTerminalRecoveryEnvelopeHash(
  input: WorkflowTerminalRecoveryEnvelopeV1,
): WorkflowTerminalSha256 {
  const envelope = materializeWorkflowTerminalRecoveryEnvelope(input);
  return getMaterializedWorkflowTerminalRecoveryEnvelopeHash(envelope);
}

/** @internal Call only with an envelope returned by materializeWorkflowTerminalRecoveryEnvelope. */
export function getMaterializedWorkflowTerminalRecoveryEnvelopeHash(
  envelope: WorkflowTerminalRecoveryEnvelopeV1,
): WorkflowTerminalSha256 {
  return hashFramed('mastra.workflow-terminal-recovery-envelope.v1', JSON.stringify(envelope));
}

/** @internal Call only with an envelope returned by materializeWorkflowTerminalRecoveryEnvelope. */
export function validateMaterializedWorkflowTerminalRecoveryEnvelope(
  envelope: WorkflowTerminalRecoveryEnvelopeV1,
  expected: WorkflowTerminalRecoveryEnvelopeExpectedBinding,
  computedEnvelopeHash?: WorkflowTerminalSha256,
): void {
  const expectedInput = descriptors(expected, 'expected');
  exactKeys(
    expectedInput,
    ['workflowName', 'runId', 'terminalStatus', 'childGraphFingerprint', 'envelopeHash'],
    [],
    'expected',
  );
  const expectedWorkflowName = read(expectedInput, 'workflowName');
  const expectedRunId = read(expectedInput, 'runId');
  const expectedTerminalStatus = read(expectedInput, 'terminalStatus');
  const expectedChildGraphFingerprint = read(expectedInput, 'childGraphFingerprint');
  const expectedEnvelopeHash = read(expectedInput, 'envelopeHash');
  if (expectedWorkflowName !== undefined && envelope.workflowName !== expectedWorkflowName)
    fail('workflowName', 'binding mismatch');
  if (expectedRunId !== undefined && envelope.runId !== expectedRunId) fail('runId', 'binding mismatch');
  if (expectedTerminalStatus !== undefined && envelope.terminalStatus !== expectedTerminalStatus) {
    fail('terminalStatus', 'binding mismatch');
  }
  if (expectedChildGraphFingerprint !== undefined && envelope.childGraphFingerprint !== expectedChildGraphFingerprint) {
    fail('childGraphFingerprint', 'binding mismatch');
  }
  if (
    expectedEnvelopeHash !== undefined &&
    (computedEnvelopeHash ?? getMaterializedWorkflowTerminalRecoveryEnvelopeHash(envelope)) !== expectedEnvelopeHash
  ) {
    fail('envelopeHash', 'binding mismatch');
  }
}

export function validateWorkflowTerminalRecoveryEnvelope(
  input: WorkflowTerminalRecoveryEnvelopeV1,
  expected: WorkflowTerminalRecoveryEnvelopeExpectedBinding = {},
): void {
  validateMaterializedWorkflowTerminalRecoveryEnvelope(materializeWorkflowTerminalRecoveryEnvelope(input), expected);
}

export function validateWorkflowTerminalRecoveryEnvelopeIntegrity(
  record: WorkflowTerminalRecoveryEnvelopeRecordV1,
  expected: WorkflowTerminalRecoveryEnvelopeExpectedBinding = {},
): void {
  const input = descriptors(record, 'record');
  exactKeys(input, ['version', 'envelopeHash', 'envelope'], ['version', 'envelopeHash', 'envelope'], 'record');
  if (read(input, 'version') !== 1) fail('record.version', 'must be 1');
  const envelopeHash = sha256(read(input, 'envelopeHash'), 'record.envelopeHash');
  const envelope = materializeWorkflowTerminalRecoveryEnvelope(read(input, 'envelope'));
  const computedEnvelopeHash = getMaterializedWorkflowTerminalRecoveryEnvelopeHash(envelope);
  if (computedEnvelopeHash !== envelopeHash) fail('record.envelopeHash', 'integrity mismatch');
  validateMaterializedWorkflowTerminalRecoveryEnvelope(envelope, expected, computedEnvelopeHash);
}

export function copyWorkflowTerminalRecoveryEnvelope(
  input: WorkflowTerminalRecoveryEnvelopeV1,
): WorkflowTerminalRecoveryEnvelopeV1 {
  return materializeWorkflowTerminalRecoveryEnvelope(input);
}

/** Validates one retained parent frame against the exact locked parent graph. */
export function validateWorkflowTerminalRecoveryParentFrameGraphBinding(
  input: WorkflowTerminalRecoveryParentFrameV1,
  parentSerializedStepGraph: SerializedStepFlowEntry[],
): void {
  const materialized = materializeWorkflowTerminalRecoveryAncestry([input])[0]!;
  if (createWorkflowTerminalGraphFingerprint(parentSerializedStepGraph) !== materialized.parentGraphFingerprint) {
    fail('parentGraphFingerprint', 'does not match serialized parent graph');
  }
  const executionPath =
    materialized.source.kind === 'step'
      ? materialized.source.executionPath
      : [...materialized.source.containerPath, materialized.source.iterationIndex];
  const resolved = resolveWorkflowTerminalGraphCoordinate(parentSerializedStepGraph, executionPath);
  const sourceMatches =
    materialized.source.kind === 'step'
      ? ['step', 'branch', 'loop'].includes(resolved.kind) &&
        'stepId' in resolved &&
        resolved.stepId === materialized.source.stepId
      : resolved.kind === 'foreach' && resolved.stepId === materialized.source.stepId;
  if (!sourceMatches) fail('source', 'does not resolve in serialized parent graph');
}

export function validateWorkflowTerminalRecoveryGraphBinding(
  input: WorkflowTerminalRecoveryEnvelopeV1,
  binding: WorkflowTerminalRecoveryGraphBinding,
): void {
  validateMaterializedWorkflowTerminalRecoveryGraphBinding(materializeWorkflowTerminalRecoveryEnvelope(input), binding);
}

/** @internal Call only with an envelope returned by materializeWorkflowTerminalRecoveryEnvelope. */
export function validateMaterializedWorkflowTerminalRecoveryGraphBinding(
  envelope: WorkflowTerminalRecoveryEnvelopeV1,
  binding: WorkflowTerminalRecoveryGraphBinding,
): void {
  const bindingInput = descriptors(binding, 'graphBinding');
  exactKeys(
    bindingInput,
    ['childSerializedStepGraph', 'parentSerializedStepGraphs'],
    ['childSerializedStepGraph'],
    'graphBinding',
  );
  const childSerializedStepGraph = read(bindingInput, 'childSerializedStepGraph') as SerializedStepFlowEntry[];
  if (createWorkflowTerminalGraphFingerprint(childSerializedStepGraph) !== envelope.childGraphFingerprint) {
    fail('childGraphFingerprint', 'does not match serialized child graph');
  }
  const rawParentGraphs = read(bindingInput, 'parentSerializedStepGraphs');
  // Producer persistence always has the child graph. Parent graphs may be
  // bound later under the parent-row lock; absence means "child-only check",
  // while an explicitly supplied list must cover the complete ancestry.
  if (rawParentGraphs === undefined) return;
  const parentValues = denseArray(
    rawParentGraphs,
    'graphBinding.parentSerializedStepGraphs',
    MAX_WORKFLOW_TERMINAL_RECOVERY_ANCESTRY_DEPTH,
  );
  const materializedParents = parentValues.map((value, index) => {
    const field = `graphBinding.parentSerializedStepGraphs[${index}]`;
    const entry = descriptors(value, field);
    exactKeys(
      entry,
      ['workflowName', 'runId', 'serializedStepGraph'],
      ['workflowName', 'runId', 'serializedStepGraph'],
      field,
    );
    return {
      workflowName: validateWorkflowTerminalStructuralString(read(entry, 'workflowName'), `${field}.workflowName`),
      runId: validateWorkflowTerminalStructuralString(read(entry, 'runId'), `${field}.runId`),
      serializedStepGraph: read(entry, 'serializedStepGraph') as SerializedStepFlowEntry[],
    };
  });
  const parents = new Map(materializedParents.map(entry => [JSON.stringify([entry.workflowName, entry.runId]), entry]));
  if (parents.size !== materializedParents.length) {
    fail('graphBinding.parentSerializedStepGraphs', 'contains duplicates');
  }
  for (let index = 0; index < envelope.ancestry.length; index++) {
    const frame = envelope.ancestry[index]!;
    const parent = parents.get(JSON.stringify([frame.parentWorkflowName, frame.parentRunId]));
    if (!parent) fail(`ancestry[${index}].parentGraphFingerprint`, 'missing parent graph binding');
    try {
      validateWorkflowTerminalRecoveryParentFrameGraphBinding(frame, parent.serializedStepGraph);
    } catch (error) {
      fail(`ancestry[${index}]`, error instanceof Error ? error.message : 'does not match serialized parent graph');
    }
  }
}
