import { isProxy } from 'node:util/types';
import { isInfrastructureRequestContextKey } from '../../request-context';
import type { WorkflowTerminalRecoveryEnvelopeV1 } from '../terminal-recovery/types';
import type { WorkflowRunState, WorkflowTerminalEffectRecord, WorkflowTerminalSnapshotRecord } from '../types';
import {
  materializeWorkflowTerminalForeachStates,
  validateWorkflowTerminalParentContinuationBinding,
  validateWorkflowTerminalParentContinuationIntegrity,
} from './contract';
import { createWorkflowTerminalGraphFingerprint } from './graph-fingerprint';
import {
  WORKFLOW_TERMINAL_FOREACH_RUN_KEY,
  WORKFLOW_TERMINAL_FOREACH_STATE_KEY,
  WORKFLOW_TERMINAL_FOREACH_SUSPEND_PAYLOAD_KEY,
} from './types';
import type {
  WorkflowTerminalChildStatus,
  WorkflowTerminalParentContinuationContract,
  WorkflowTerminalResultCoordinate,
} from './types';

export { WORKFLOW_TERMINAL_FOREACH_STATE_KEY } from './types';

type ParentStepResult = Record<string, unknown>;
const OMIT_JSON_PROPERTY = Symbol('omit-json-property');
const CONTINUATION_TEXT_ENCODER = new TextEncoder();

export const MAX_WORKFLOW_TERMINAL_CONTINUATION_DATA_DEPTH = 64;
export const MAX_WORKFLOW_TERMINAL_CONTINUATION_DATA_NODES = 4_096;
export const MAX_WORKFLOW_TERMINAL_CONTINUATION_DATA_ENTRIES = 16_384;
export const MAX_WORKFLOW_TERMINAL_CONTINUATION_DATA_BYTES = 1_048_576;

interface ContinuationDataBudget {
  bytes: number;
  entries: number;
  nodes: number;
}

function createContinuationDataBudget(): ContinuationDataBudget {
  return { bytes: 0, entries: 0, nodes: 0 };
}

function chargeContinuationDataNode(budget: ContinuationDataBudget, depth: number): void {
  if (depth > MAX_WORKFLOW_TERMINAL_CONTINUATION_DATA_DEPTH) {
    throw new TypeError('parent continuation data exceeds depth limit');
  }
  budget.nodes++;
  if (budget.nodes > MAX_WORKFLOW_TERMINAL_CONTINUATION_DATA_NODES) {
    throw new TypeError('parent continuation data exceeds node limit');
  }
}

function chargeContinuationDataEntries(budget: ContinuationDataBudget, count: number): void {
  budget.entries += count;
  if (budget.entries > MAX_WORKFLOW_TERMINAL_CONTINUATION_DATA_ENTRIES) {
    throw new TypeError('parent continuation data exceeds entry limit');
  }
}

function chargeContinuationDataString(budget: ContinuationDataBudget, value: string): void {
  const remaining = MAX_WORKFLOW_TERMINAL_CONTINUATION_DATA_BYTES - budget.bytes;
  if (value.length > remaining) {
    throw new TypeError('parent continuation data exceeds byte limit');
  }
  assertJsonString(value, 'parent continuation data');
  const bytes = CONTINUATION_TEXT_ENCODER.encode(value).byteLength;
  if (bytes > remaining) throw new TypeError('parent continuation data exceeds byte limit');
  budget.bytes += bytes;
}

function clone<T>(value: T): T {
  return structuredClone(value);
}

function defineDataProperty(target: Record<string, unknown>, key: string, value: unknown): void {
  Object.defineProperty(target, key, {
    configurable: true,
    enumerable: true,
    value,
    writable: true,
  });
}

function assertBoundedDataOnly(
  value: unknown,
  field: string,
  budget: ContinuationDataBudget,
  allowErrorStack = false,
): void {
  type Visit = { depth: number; field: string; kind: 'enter'; value: unknown } | { kind: 'exit'; value: object };
  const stack: Visit[] = [{ depth: 0, field, kind: 'enter', value }];
  const ancestors = new WeakSet<object>();

  while (stack.length > 0) {
    const visit = stack.pop()!;
    if (visit.kind === 'exit') {
      ancestors.delete(visit.value);
      continue;
    }
    chargeContinuationDataNode(budget, visit.depth);
    if (typeof visit.value === 'string') chargeContinuationDataString(budget, visit.value);
    if (visit.value === null || typeof visit.value !== 'object') {
      if (typeof visit.value === 'function' || typeof visit.value === 'symbol' || typeof visit.value === 'bigint') {
        throw new TypeError(`${visit.field} contains non-data values`);
      }
      continue;
    }
    if (isProxy(visit.value)) throw new TypeError(`${visit.field} contains a proxy`);
    if (ancestors.has(visit.value)) throw new TypeError(`${visit.field} contains a cycle`);
    const prototype = Object.getPrototypeOf(visit.value);
    if (
      !Array.isArray(visit.value) &&
      !(visit.value instanceof Date) &&
      !(visit.value instanceof Error) &&
      !(visit.value instanceof Map) &&
      !(visit.value instanceof Set) &&
      prototype !== Object.prototype &&
      prototype !== null
    ) {
      throw new TypeError(`${visit.field} contains a non-data object`);
    }
    if (visit.value instanceof Date && !Number.isFinite(Date.prototype.getTime.call(visit.value))) {
      throw new TypeError(`${visit.field} contains an invalid Date`);
    }
    ancestors.add(visit.value);
    stack.push({ kind: 'exit', value: visit.value });

    let arrayLength: number | undefined;
    if (Array.isArray(visit.value)) {
      const lengthDescriptor = Object.getOwnPropertyDescriptor(visit.value, 'length');
      const length = lengthDescriptor?.value;
      if (!Number.isSafeInteger(length) || length < 0) throw new TypeError(`${visit.field} has an invalid length`);
      chargeContinuationDataEntries(budget, length);
      arrayLength = length;
    } else if (visit.value instanceof Map) {
      const size = Object.getOwnPropertyDescriptor(Map.prototype, 'size')!.get!.call(visit.value) as number;
      chargeContinuationDataEntries(budget, size * 2);
    } else if (visit.value instanceof Set) {
      const size = Object.getOwnPropertyDescriptor(Set.prototype, 'size')!.get!.call(visit.value) as number;
      chargeContinuationDataEntries(budget, size);
    }

    const descriptorKeys = Reflect.ownKeys(visit.value);
    if (arrayLength !== undefined) {
      if (
        descriptorKeys.length !== arrayLength + 1 ||
        descriptorKeys.some(
          key =>
            key !== 'length' &&
            (typeof key !== 'string' || !/^(0|[1-9][0-9]*)$/.test(key) || Number(key) >= arrayLength),
        )
      ) {
        throw new TypeError(`${visit.field} must be a dense data-only array`);
      }
    } else {
      chargeContinuationDataEntries(budget, descriptorKeys.length);
    }
    for (let index = descriptorKeys.length - 1; index >= 0; index--) {
      const key = descriptorKeys[index]!;
      if (typeof key !== 'string') {
        throw new TypeError(`${visit.field} contains symbol or accessor fields`);
      }
      chargeContinuationDataString(budget, key);
      const descriptor = Object.getOwnPropertyDescriptor(visit.value, key);
      if (
        allowErrorStack &&
        visit.value instanceof Error &&
        key === 'stack' &&
        descriptor &&
        !('value' in descriptor)
      ) {
        continue;
      }
      if (!descriptor || !('value' in descriptor)) {
        throw new TypeError(`${visit.field} contains symbol or accessor fields`);
      }
      stack.push({ depth: visit.depth + 1, field: `${visit.field}.${key}`, kind: 'enter', value: descriptor.value });
    }

    if (visit.value instanceof Map) {
      const entries = [...(Map.prototype.entries.call(visit.value) as MapIterator<[unknown, unknown]>)];
      for (let index = entries.length - 1; index >= 0; index--) {
        const [key, entry] = entries[index]!;
        stack.push({ depth: visit.depth + 1, field: `${visit.field} map value`, kind: 'enter', value: entry });
        stack.push({ depth: visit.depth + 1, field: `${visit.field} map key`, kind: 'enter', value: key });
      }
    } else if (visit.value instanceof Set) {
      const entries = [...(Set.prototype.values.call(visit.value) as SetIterator<unknown>)];
      for (let index = entries.length - 1; index >= 0; index--) {
        stack.push({
          depth: visit.depth + 1,
          field: `${visit.field} set value`,
          kind: 'enter',
          value: entries[index],
        });
      }
    }
  }
}

function errorDataString(value: Error, key: 'name' | 'message', fallback: string): string {
  let current: object | null = value;
  while (current) {
    const descriptor = Object.getOwnPropertyDescriptor(current, key);
    if (descriptor) return 'value' in descriptor && typeof descriptor.value === 'string' ? descriptor.value : fallback;
    current = Object.getPrototypeOf(current);
  }
  return fallback;
}

function assertJsonString(value: string, field: string): string {
  if (value.includes('\0')) throw new TypeError(`${field} contains a null character`);
  for (let index = 0; index < value.length; index++) {
    const code = value.charCodeAt(index);
    if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(index + 1);
      if (!(next >= 0xdc00 && next <= 0xdfff)) throw new TypeError(`${field} contains malformed Unicode`);
      index++;
    } else if (code >= 0xdc00 && code <= 0xdfff) {
      throw new TypeError(`${field} contains malformed Unicode`);
    }
  }
  return value;
}

function canonicalJsonValue(
  value: unknown,
  field: string,
  budget = createContinuationDataBudget(),
  ancestors = new Set<object>(),
  depth = 0,
  arrayEntry = false,
): unknown | typeof OMIT_JSON_PROPERTY {
  chargeContinuationDataNode(budget, depth);
  if (value === undefined) return arrayEntry ? null : OMIT_JSON_PROPERTY;
  if (value === null || typeof value === 'boolean') return value;
  if (typeof value === 'string') {
    chargeContinuationDataString(budget, value);
    return assertJsonString(value, field);
  }
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) throw new TypeError(`${field} contains a non-finite number`);
    return value === 0 ? 0 : value;
  }
  if (typeof value !== 'object') throw new TypeError(`${field} contains non-JSON data`);
  if (ancestors.has(value)) throw new TypeError(`${field} contains a cycle`);
  if (value instanceof Date) {
    const epoch = Date.prototype.getTime.call(value);
    if (!Number.isFinite(epoch)) throw new TypeError(`${field} contains an invalid Date`);
    const normalized = new Date(epoch).toISOString();
    chargeContinuationDataString(budget, normalized);
    return normalized;
  }

  ancestors.add(value);
  try {
    if (value instanceof Error) {
      const own = Object.getOwnPropertyDescriptors(value) as Record<PropertyKey, PropertyDescriptor>;
      const name = errorDataString(value, 'name', 'Error');
      const message = errorDataString(value, 'message', '');
      chargeContinuationDataEntries(budget, 2);
      chargeContinuationDataString(budget, 'name');
      chargeContinuationDataString(budget, name);
      chargeContinuationDataString(budget, 'message');
      chargeContinuationDataString(budget, message);
      const errorRecord: Record<string, unknown> = {
        name: assertJsonString(name, `${field}.name`),
        message: assertJsonString(message, `${field}.message`),
      };
      const stack = own.stack;
      if (stack && 'value' in stack && typeof stack.value === 'string') {
        chargeContinuationDataEntries(budget, 1);
        chargeContinuationDataString(budget, 'stack');
        chargeContinuationDataString(budget, stack.value);
        errorRecord.stack = assertJsonString(stack.value, `${field}.stack`);
      }
      for (const key of Reflect.ownKeys(own)) {
        if (typeof key !== 'string') {
          throw new TypeError(`${field} contains symbol or accessor fields`);
        }
        if (key === 'name' || key === 'message' || key === 'stack') continue;
        if (!('value' in own[key]!)) throw new TypeError(`${field} contains symbol or accessor fields`);
        chargeContinuationDataEntries(budget, 1);
        chargeContinuationDataString(budget, key);
        assertJsonString(key, `${field} key`);
        const normalized = canonicalJsonValue(own[key]!.value, `${field}.${key}`, budget, ancestors, depth + 1);
        if (normalized !== OMIT_JSON_PROPERTY) defineDataProperty(errorRecord, key, normalized);
      }
      return errorRecord;
    }
    if (Array.isArray(value)) {
      const descriptors = Object.getOwnPropertyDescriptors(value) as unknown as Record<PropertyKey, PropertyDescriptor>;
      const length = descriptors.length?.value;
      if (!Number.isSafeInteger(length) || length < 0) throw new TypeError(`${field} has an invalid length`);
      if (
        Reflect.ownKeys(descriptors).some(
          key =>
            key !== 'length' && (typeof key !== 'string' || !/^(0|[1-9][0-9]*)$/.test(key) || Number(key) >= length),
        )
      ) {
        throw new TypeError(`${field} contains symbol or non-index fields`);
      }
      chargeContinuationDataEntries(budget, length);
      return Array.from({ length }, (_, index) => {
        const descriptor = descriptors[String(index)];
        if (!descriptor || !('value' in descriptor)) throw new TypeError(`${field} must be a dense data-only array`);
        return canonicalJsonValue(descriptor.value, `${field}[${index}]`, budget, ancestors, depth + 1, true);
      });
    }
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) {
      throw new TypeError(`${field} contains a non-JSON object`);
    }
    const descriptors = Object.getOwnPropertyDescriptors(value) as Record<PropertyKey, PropertyDescriptor>;
    const output: Record<string, unknown> = {};
    const keys = Reflect.ownKeys(descriptors);
    chargeContinuationDataEntries(budget, keys.length);
    for (const key of keys) {
      if (typeof key !== 'string' || !('value' in descriptors[key]!)) {
        throw new TypeError(`${field} contains symbol or accessor fields`);
      }
      chargeContinuationDataString(budget, key);
      assertJsonString(key, `${field} key`);
      const normalized = canonicalJsonValue(descriptors[key]!.value, `${field}.${key}`, budget, ancestors, depth + 1);
      if (normalized !== OMIT_JSON_PROPERTY) defineDataProperty(output, key, normalized);
    }
    return output;
  } finally {
    ancestors.delete(value);
  }
}

function dataRecord(value: unknown, field: string, budget = createContinuationDataBudget()): Record<string, unknown> {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    throw new TypeError(`${field} must be a data object`);
  }
  const prototype = Object.getPrototypeOf(value);
  if (prototype !== Object.prototype && prototype !== null) {
    throw new TypeError(`${field} must be a plain object`);
  }
  const descriptors = Object.getOwnPropertyDescriptors(value);
  for (const key of Reflect.ownKeys(descriptors)) {
    if (typeof key !== 'string' || !('value' in descriptors[key]!)) {
      throw new TypeError(`${field} contains symbol or accessor fields`);
    }
    assertJsonString(key, `${field} key`);
  }
  const output: Record<string, unknown> = {};
  chargeContinuationDataEntries(budget, Object.keys(descriptors).length);
  for (const [key, descriptor] of Object.entries(descriptors)) {
    chargeContinuationDataString(budget, key);
    const normalized = canonicalJsonValue(descriptor.value, `${field}.${key}`, budget, new Set<object>(), 1);
    if (normalized !== OMIT_JSON_PROPERTY) defineDataProperty(output, key, normalized);
  }
  return output;
}

function optionalDataRecord(
  value: unknown,
  field: string,
  budget = createContinuationDataBudget(),
): Record<string, unknown> {
  return value === undefined ? {} : dataRecord(value, field, budget);
}

export interface WorkflowTerminalParentContinuationChildProjection {
  finalState: Record<string, unknown>;
  requestContextPatch: Record<string, unknown>;
  terminalResult: Record<string, unknown>;
}

/**
 * @internal Applies the exact aggregate child-data budget consumed by parent
 * continuation. Callers may use this before committing a nested envelope so a
 * durable outbox can never contain child data that the native patch rejects.
 */
export function materializeWorkflowTerminalParentContinuationChildProjection(
  envelope: Pick<WorkflowTerminalRecoveryEnvelopeV1, 'finalState' | 'requestContextPatch' | 'terminalResult'>,
  retainedCreatedAt: number,
): WorkflowTerminalParentContinuationChildProjection {
  if (!Number.isSafeInteger(retainedCreatedAt) || retainedCreatedAt < 0) {
    throw new TypeError('retained child createdAt must be a non-negative safe integer');
  }
  const budget = createContinuationDataBudget();
  const projection = {
    finalState: dataRecord(envelope.finalState, 'retained child final context.__state', budget),
    requestContextPatch: optionalDataRecord(envelope.requestContextPatch, 'retained child requestContext', budget),
    terminalResult: dataRecord(envelope.terminalResult, 'retained child terminal result', budget),
  };
  if (projection.terminalResult.metadata !== undefined) {
    normalizedDataRecord(projection.terminalResult.metadata, 'retained child result metadata');
  }
  const startedAt = projection.terminalResult.startedAt;
  const endedAt = projection.terminalResult.endedAt;
  if (startedAt !== undefined && (!Number.isSafeInteger(startedAt) || (startedAt as number) < 0)) {
    throw new TypeError('retained child startedAt must be a non-negative safe integer');
  }
  if (endedAt !== undefined && (!Number.isSafeInteger(endedAt) || (endedAt as number) < 0)) {
    throw new TypeError('retained child endedAt must be a non-negative safe integer');
  }
  if (startedAt !== undefined && endedAt !== undefined && (startedAt as number) > (endedAt as number)) {
    throw new TypeError('retained child result timestamps must be monotonic');
  }
  return projection;
}

function storedDataRecord(value: unknown, field: string): Record<string, unknown> {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    throw new TypeError(`${field} must be a data object`);
  }
  const prototype = Object.getPrototypeOf(value);
  if (prototype !== Object.prototype && prototype !== null) {
    throw new TypeError(`${field} must be a plain object`);
  }
  for (const [key, descriptor] of Object.entries(Object.getOwnPropertyDescriptors(value))) {
    assertJsonString(key, `${field} key`);
    if (!('value' in descriptor)) throw new TypeError(`${field} contains accessor fields`);
  }
  if (Object.getOwnPropertySymbols(value).length > 0) {
    throw new TypeError(`${field} contains symbol fields`);
  }
  return value as Record<string, unknown>;
}

/** Use only for records produced by this module's canonical normalization in the current patch call. */
function normalizedDataRecord(value: unknown, field: string): Record<string, unknown> {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    throw new TypeError(`${field} must be a normalized data object`);
  }
  return value as Record<string, unknown>;
}

function optionalNormalizedDataRecord(value: unknown, field: string): Record<string, unknown> {
  return value === undefined ? {} : normalizedDataRecord(value, field);
}

function terminalResultFromRetained(
  retained: WorkflowTerminalSnapshotRecord,
  retainedTerminalResult: Record<string, unknown>,
  parentResult: ParentStepResult,
  nestedRunId: string,
  storageTimestamp: number,
): ParentStepResult {
  const terminalResult = retainedTerminalResult;
  const status = retained.terminalStatus;
  if (terminalResult.status !== undefined && terminalResult.status !== status) {
    throw new TypeError('retained child result status conflicts with its terminal snapshot');
  }
  if (status === 'success' && terminalResult.status !== 'success') {
    throw new TypeError('successful retained child snapshot is missing a successful result');
  }
  const error = terminalResult.error;
  if (status === 'failed' && error === undefined) {
    throw new TypeError('failed retained child snapshot is missing an error');
  }
  const existingMetadata = readContinuationStoredState('parent', () =>
    optionalNormalizedDataRecord(parentResult.metadata, 'parent step metadata'),
  );
  const childMetadata = readContinuationStoredState('child', () =>
    optionalNormalizedDataRecord(terminalResult.metadata, 'retained child result metadata'),
  );
  const startedAt = parentResult.startedAt ?? terminalResult.startedAt ?? retained.createdAt;
  const endedAt = terminalResult.endedAt ?? retained.createdAt;
  if (!Number.isSafeInteger(startedAt) || (startedAt as number) < 0) {
    throw new WorkflowTerminalContinuationStoredStateError(
      parentResult.startedAt === undefined ? 'child' : 'parent',
      'parent step startedAt must be a non-negative safe integer',
    );
  }
  if (!Number.isSafeInteger(endedAt) || (endedAt as number) < 0) {
    throw new WorkflowTerminalContinuationStoredStateError(
      'child',
      'retained child endedAt must be a non-negative safe integer',
    );
  }
  if ((startedAt as number) > (endedAt as number)) {
    throw new WorkflowTerminalContinuationStoredStateError(
      parentResult.startedAt === undefined ? 'child' : 'parent',
      'retained child result timestamps are not monotonic',
    );
  }
  if (retained.createdAt > storageTimestamp) {
    throw new WorkflowTerminalContinuationStoredStateError(
      'child',
      'retained child result timestamps are not monotonic',
    );
  }
  delete terminalResult.__state;
  if (status !== 'failed') delete terminalResult.error;
  return {
    ...terminalResult,
    status,
    ...(status === 'failed' ? { error: readContinuationStoredState('child', () => clone(error)) } : {}),
    payload: readContinuationStoredState('parent', () => clone(parentResult.payload)),
    startedAt,
    endedAt,
    metadata: { ...existingMetadata, ...childMetadata, nestedRunId },
  };
}

function sourcePath(source: WorkflowTerminalResultCoordinate): number[] {
  return source.kind === 'step' ? [...source.executionPath] : [...source.containerPath, source.iterationIndex];
}

function mergeForeachIteration(
  existing: ParentStepResult,
  terminalResult: ParentStepResult,
  source: Extract<WorkflowTerminalResultCoordinate, { kind: 'foreach-iteration' }>,
): ParentStepResult {
  if (!Array.isArray(existing.payload)) {
    throw new TypeError('parent foreach step payload must be an array');
  }
  if (source.iterationIndex >= existing.payload.length) {
    throw new TypeError('parent foreach iteration is out of bounds');
  }
  const output = Array.isArray(existing.output) ? clone(existing.output) : [];
  while (output.length <= source.iterationIndex) output.push(null);
  output[source.iterationIndex] =
    terminalResult.status === 'success' && terminalResult.output !== undefined ? terminalResult.output : null;

  const metadata = optionalNormalizedDataRecord(existing.metadata, 'parent foreach metadata');
  const workflowMetadata = optionalNormalizedDataRecord(metadata.__workflow_meta, 'parent foreach workflow metadata');
  const iterationStates = optionalNormalizedDataRecord(
    workflowMetadata[WORKFLOW_TERMINAL_FOREACH_STATE_KEY],
    'parent foreach terminal iteration states',
  );
  defineDataProperty(iterationStates, String(source.iterationIndex), terminalResult.status);

  return {
    ...existing,
    output,
    metadata: {
      ...metadata,
      __workflow_meta: {
        ...workflowMetadata,
        [WORKFLOW_TERMINAL_FOREACH_STATE_KEY]: iterationStates,
      },
    },
  };
}

function validateRetainedBinding(
  retained: WorkflowTerminalSnapshotRecord,
  effect: Extract<WorkflowTerminalEffectRecord, { kind: 'parent-workflow-step-end' }>,
  childStatus: WorkflowTerminalChildStatus,
): void {
  if (
    retained.version !== 1 ||
    retained.workflowName !== effect.workflowName ||
    retained.runId !== effect.runId ||
    retained.terminalStatus !== childStatus ||
    retained.terminalStatus !== effect.terminalStatus ||
    retained.envelope.workflowName !== effect.workflowName ||
    retained.envelope.runId !== effect.runId ||
    retained.envelope.terminalStatus !== retained.terminalStatus ||
    retained.envelopeHash !== effect.recoveryEnvelopeHash ||
    retained.recordHash !== effect.retainedRecordHash ||
    retained.resourceId !== effect.resourceId ||
    !Number.isSafeInteger(retained.createdAt) ||
    retained.createdAt < 0
  ) {
    throw new TypeError('retained child snapshot does not match the terminal parent effect');
  }
}

function materializeResumeLabel(value: unknown, field: string): { stepId: string; foreachIndex?: number } {
  const label = normalizedDataRecord(value, field);
  const keys = Object.keys(label);
  if (!keys.includes('stepId') || keys.some(key => key !== 'stepId' && key !== 'foreachIndex')) {
    throw new TypeError(`${field} contains unknown or missing fields`);
  }
  if (typeof label.stepId !== 'string' || label.stepId.length === 0 || label.stepId.length > 512) {
    throw new TypeError(`${field}.stepId must be a bounded string`);
  }
  const foreachIndex = label.foreachIndex;
  if (foreachIndex !== undefined && (!Number.isSafeInteger(foreachIndex) || (foreachIndex as number) < 0)) {
    throw new TypeError(`${field}.foreachIndex must be a non-negative safe integer`);
  }
  return {
    stepId: label.stepId,
    ...(foreachIndex === undefined ? {} : { foreachIndex: foreachIndex === 0 ? 0 : (foreachIndex as number) }),
  };
}

function collectResumeLabels(
  result: ParentStepResult,
  output: Record<string, unknown>,
  field: string,
  expectedForeachIndex?: number,
): void {
  const suspendPayload = optionalNormalizedDataRecord(result.suspendPayload, `${field}.suspendPayload`);
  const workflowMetadata = optionalNormalizedDataRecord(suspendPayload.__workflow_meta, `${field} workflow metadata`);
  const resumeLabels = optionalNormalizedDataRecord(workflowMetadata.resumeLabels, `${field} resume labels`);
  for (const [label, value] of Object.entries(resumeLabels)) {
    if (label.length === 0 || label.length > 512) throw new TypeError(`${field} contains an invalid resume label`);
    const materialized = materializeResumeLabel(value, `${field} resume label ${label}`);
    if (expectedForeachIndex !== undefined && materialized.foreachIndex !== expectedForeachIndex) {
      throw new TypeError(`${field} resume label ${label} is not bound to its foreach iteration`);
    }
    if (Object.hasOwn(output, label)) {
      const existing = output[label] as { stepId: string; foreachIndex?: number };
      if (existing.stepId !== materialized.stepId || existing.foreachIndex !== materialized.foreachIndex) {
        throw new TypeError(`${field} contains a conflicting duplicate resume label ${label}`);
      }
      continue;
    }
    defineDataProperty(output, label, materialized);
  }
}

function applyAggregateSuspension(
  snapshot: WorkflowRunState,
  contract: WorkflowTerminalParentContinuationContract,
  storageTimestamp: number,
  budget: ContinuationDataBudget,
): void {
  const action = contract.action;
  const path = sourcePath(contract.source);
  const rootIndex = path[0]!;
  const resumeLabels: Record<string, unknown> = {};
  if (action.reason === 'branch-suspended') {
    const container = snapshot.serializedStepGraph[rootIndex];
    if (!container || (container.type !== 'parallel' && container.type !== 'conditional')) {
      throw new TypeError('parent branch suspension container is missing');
    }
    const suspendedPaths: Record<string, number[]> = {};
    container.steps.forEach((entry, branchIndex) => {
      const result =
        entry.step.id === contract.source.stepId
          ? normalizedDataRecord(snapshot.context[entry.step.id], `parent branch ${entry.step.id}`)
          : dataRecord(snapshot.context[entry.step.id], `parent branch ${entry.step.id}`, budget);
      if (result.status === 'suspended') {
        defineDataProperty(suspendedPaths, entry.step.id, [rootIndex, branchIndex]);
        collectResumeLabels(result, resumeLabels, `parent branch ${entry.step.id}`);
      }
    });
    if (Object.keys(suspendedPaths).length === 0) throw new TypeError('parent branch suspension has no suspended path');
    snapshot.status = 'suspended';
    snapshot.result = { status: 'suspended' };
    snapshot.suspendedPaths = suspendedPaths;
    snapshot.resumeLabels = resumeLabels as WorkflowRunState['resumeLabels'];
    snapshot.activePaths = path;
    return;
  }
  if (action.reason !== 'foreach-suspended') {
    throw new TypeError('aggregate suspension action is invalid');
  }
  if (contract.source.kind !== 'foreach-iteration') {
    throw new TypeError('foreach aggregate suspension requires an iteration source');
  }
  const foreachSource = contract.source;
  const current = normalizedDataRecord(snapshot.context[contract.source.stepId], 'parent foreach suspension result');
  if (!Array.isArray(current.output)) throw new TypeError('parent foreach suspension output is missing');
  const metadata = optionalNormalizedDataRecord(current.metadata, 'parent foreach suspension metadata');
  const workflowMetadata = optionalNormalizedDataRecord(
    metadata.__workflow_meta,
    'parent foreach suspension workflow metadata',
  );
  const iterationStates = materializeWorkflowTerminalForeachStates(
    workflowMetadata[WORKFLOW_TERMINAL_FOREACH_STATE_KEY],
    current.output.length,
  );
  let suspendedCount = 0;
  const iterationSuspendPayloads: Record<string, unknown> = {};
  let onlySuspendPayload: Record<string, unknown> | undefined;
  for (let index = 0; index < current.output.length; index++) {
    if (
      index === foreachSource.iterationIndex ||
      iterationStates[String(index)] === 'success' ||
      iterationStates[String(index)] === 'failed' ||
      iterationStates[String(index)] === 'canceled'
    ) {
      continue;
    }
    const iteration = current.output[index];
    if (iteration !== null && typeof iteration === 'object' && !Array.isArray(iteration)) {
      const result = normalizedDataRecord(iteration, `parent foreach suspension iteration ${index}`);
      if (result.status === 'suspended') {
        suspendedCount++;
        if (result.suspendPayload !== undefined) {
          const suspendPayload = optionalNormalizedDataRecord(
            result.suspendPayload,
            `parent foreach suspension iteration ${index}.suspendPayload`,
          );
          const { __workflow_meta: _frameworkMetadata, ...payload } = suspendPayload;
          defineDataProperty(iterationSuspendPayloads, String(index), payload);
          onlySuspendPayload = payload;
        }
        collectResumeLabels(result, resumeLabels, `parent foreach suspension iteration ${index}`, index);
      }
    }
  }
  if (suspendedCount === 0) throw new TypeError('parent foreach suspension has no suspended iteration');
  const aggregate = {
    ...current,
    status: 'suspended',
    suspendedAt: storageTimestamp,
    suspendPayload: {
      ...(suspendedCount === 1 ? onlySuspendPayload : undefined),
      __workflow_meta: {
        path,
        resumeLabels,
        [WORKFLOW_TERMINAL_FOREACH_SUSPEND_PAYLOAD_KEY]: iterationSuspendPayloads,
      },
    },
  };
  defineDataProperty(
    snapshot.context,
    contract.source.stepId,
    aggregate as unknown as WorkflowRunState['context'][string],
  );
  snapshot.status = 'suspended';
  snapshot.result = aggregate;
  snapshot.suspendedPaths = { [contract.source.stepId]: [rootIndex] };
  snapshot.resumeLabels = resumeLabels as WorkflowRunState['resumeLabels'];
  snapshot.activePaths = path;
}

function validateStoredParentSnapshotForContinuation(parentSnapshot: WorkflowRunState): void {
  if (
    typeof parentSnapshot.runId !== 'string' ||
    ![
      'pending',
      'running',
      'suspended',
      'success',
      'failed',
      'tripwire',
      'canceled',
      'bailed',
      'paused',
      'waiting',
    ].includes(String(parentSnapshot.status)) ||
    !Number.isSafeInteger(parentSnapshot.timestamp) ||
    parentSnapshot.timestamp < 0
  ) {
    throw new TypeError('parent continuation snapshot identity or lifecycle is invalid');
  }
  createWorkflowTerminalGraphFingerprint(parentSnapshot.serializedStepGraph);
  if (
    parentSnapshot.context === null ||
    typeof parentSnapshot.context !== 'object' ||
    Array.isArray(parentSnapshot.context) ||
    (Object.getPrototypeOf(parentSnapshot.context) !== Object.prototype &&
      Object.getPrototypeOf(parentSnapshot.context) !== null)
  ) {
    throw new TypeError('parent continuation context is invalid');
  }
  if (
    parentSnapshot.activeStepsPath === null ||
    typeof parentSnapshot.activeStepsPath !== 'object' ||
    Array.isArray(parentSnapshot.activeStepsPath) ||
    (Object.getPrototypeOf(parentSnapshot.activeStepsPath) !== Object.prototype &&
      Object.getPrototypeOf(parentSnapshot.activeStepsPath) !== null)
  ) {
    throw new TypeError('parent continuation active step paths are invalid');
  }
  const activeStepsPath = parentSnapshot.activeStepsPath as Record<string, unknown>;
  for (const sourcePathValue of Object.values(activeStepsPath)) {
    if (
      !Array.isArray(sourcePathValue) ||
      sourcePathValue.length === 0 ||
      sourcePathValue.some(value => !Number.isSafeInteger(value) || value < 0)
    ) {
      throw new TypeError('parent continuation active source path is invalid');
    }
  }
  for (const [stepId, value] of Object.entries(parentSnapshot.context)) {
    if (stepId === '__state' || stepId === 'input') continue;
    const result = storedDataRecord(value, `parent continuation context ${stepId}`);
    if (
      result.status !== undefined &&
      ![
        'pending',
        'running',
        'suspended',
        'success',
        'failed',
        'canceled',
        'waiting',
        'paused',
        'tripwire',
        'bailed',
        'skipped',
      ].includes(String(result.status))
    ) {
      throw new TypeError(`parent continuation context ${stepId} status is invalid`);
    }
    const metadata =
      result.metadata === undefined
        ? {}
        : storedDataRecord(result.metadata, `parent continuation context ${stepId} metadata`);
    if (metadata.__workflow_meta === undefined) continue;
    const workflowMetadata = storedDataRecord(
      metadata.__workflow_meta,
      `parent continuation context ${stepId} workflow metadata`,
    );
    if (workflowMetadata[WORKFLOW_TERMINAL_FOREACH_RUN_KEY] !== undefined) {
      const iterationRuns = storedDataRecord(
        workflowMetadata[WORKFLOW_TERMINAL_FOREACH_RUN_KEY],
        `parent continuation context ${stepId} foreach ownership`,
      );
      for (const [index, runId] of Object.entries(iterationRuns)) {
        if (!/^(0|[1-9][0-9]*)$/.test(index) || typeof runId !== 'string') {
          throw new TypeError(`parent continuation context ${stepId} foreach ownership is invalid`);
        }
      }
    }
    if (workflowMetadata[WORKFLOW_TERMINAL_FOREACH_STATE_KEY] !== undefined) {
      if (!Array.isArray(result.output)) {
        throw new TypeError(`parent continuation context ${stepId} foreach output is invalid`);
      }
      materializeWorkflowTerminalForeachStates(
        workflowMetadata[WORKFLOW_TERMINAL_FOREACH_STATE_KEY],
        result.output.length,
      );
    }
  }
}

function validateParentContinuationIdentityBinding(
  contract: WorkflowTerminalParentContinuationContract,
  context: {
    effect: Extract<WorkflowTerminalEffectRecord, { kind: 'parent-workflow-step-end' }>;
    parentRevision: string;
    parentWorkflowName: string;
    parentSnapshot: WorkflowRunState;
    executionMode: 'continuous';
  },
): void {
  validateWorkflowTerminalParentContinuationIntegrity(contract);
  const { effect, parentRevision, parentWorkflowName, parentSnapshot, executionMode } = context;
  const contractSourcePath =
    contract.source.kind === 'step'
      ? contract.source.executionPath
      : [...contract.source.containerPath, contract.source.iterationIndex];
  if (
    contract.expectedParentRevision !== parentRevision ||
    contract.terminalEffectKey !== effect.effectKey ||
    contract.terminalEffectPayloadHash !== effect.payloadHash ||
    contract.executionMode !== executionMode ||
    parentWorkflowName !== effect.parentWorkflowName ||
    contract.graphFingerprint !== createWorkflowTerminalGraphFingerprint(parentSnapshot.serializedStepGraph) ||
    contract.observedParentStatus !== parentSnapshot.status ||
    contract.childTerminalStatus !== effect.terminalStatus ||
    parentSnapshot.runId !== effect.parentRunId ||
    contract.source.stepId !== effect.parentStepId ||
    contractSourcePath.length !== effect.parentExecutionPath.length ||
    contractSourcePath.some((entry, index) => entry !== effect.parentExecutionPath[index])
  ) {
    throw new TypeError('Workflow terminal parent continuation identity binding conflict');
  }
  if (
    contract.patch.kind === 'merge-child-terminal' &&
    contract.patch.loopWrite.kind === 'set-iteration' &&
    contract.patch.loopWrite.stepId !== contract.source.stepId
  ) {
    throw new TypeError('Workflow terminal parent continuation loop step conflict');
  }
}

function validateStoredParentSourceForContinuation(
  contract: WorkflowTerminalParentContinuationContract,
  effect: Extract<WorkflowTerminalEffectRecord, { kind: 'parent-workflow-step-end' }>,
  parentSnapshot: WorkflowRunState,
): void {
  if (contract.patch.kind === 'none') return;

  const context = parentSnapshot.context as Record<string, unknown>;
  const source = dataRecord(context[contract.source.stepId], 'parent continuation source result');
  if (source.status !== 'running') {
    throw new TypeError('parent continuation source status is invalid');
  }
  const metadata = optionalDataRecord(source.metadata, 'parent continuation source metadata');
  if (contract.source.kind === 'step') {
    const executionPath = contract.source.executionPath;
    if (
      metadata.nestedRunId !== effect.runId ||
      !Array.isArray(parentSnapshot.activeStepsPath[contract.source.stepId]) ||
      parentSnapshot.activeStepsPath[contract.source.stepId]!.length !== executionPath.length ||
      parentSnapshot.activeStepsPath[contract.source.stepId]!.some((entry, index) => entry !== executionPath[index])
    ) {
      throw new TypeError('parent continuation nested run ownership is invalid');
    }
  } else {
    if (!Array.isArray(source.payload) || !Array.isArray(source.output)) {
      throw new TypeError('parent continuation foreach source state is invalid');
    }
    const workflowMetadata = optionalDataRecord(
      metadata.__workflow_meta,
      'parent continuation foreach workflow metadata',
    );
    const iterationRuns = dataRecord(
      workflowMetadata.iterationRunIds,
      'parent continuation foreach iteration ownership',
    );
    if (
      Object.values(iterationRuns).some(runId => typeof runId !== 'string') ||
      iterationRuns[String(contract.source.iterationIndex)] !== effect.runId
    ) {
      throw new TypeError('parent continuation foreach iteration ownership is invalid');
    }
    const states = materializeWorkflowTerminalForeachStates(
      workflowMetadata[WORKFLOW_TERMINAL_FOREACH_STATE_KEY],
      source.output.length,
    );
    const state = states[String(contract.source.iterationIndex)];
    if (state === 'success' || state === 'failed' || state === 'canceled') {
      throw new TypeError('parent continuation foreach iteration is already terminal');
    }
  }

  if (contract.patch.loopWrite.kind === 'set-iteration') {
    const loopResult = dataRecord(context[contract.patch.loopWrite.stepId], 'parent continuation loop source result');
    optionalDataRecord(loopResult.metadata, 'parent continuation loop metadata');
  }
}

/**
 * @internal Pure reference semantics for the later atomic storage implementations.
 * It applies no framework action and publishes no event; it only materializes the
 * exact parent snapshot a successful PF-1771 storage transaction must persist.
 */
export function applyWorkflowTerminalParentContinuationPatch(input: {
  contract: WorkflowTerminalParentContinuationContract;
  effect: Extract<WorkflowTerminalEffectRecord, { kind: 'parent-workflow-step-end' }>;
  parentRevision: string;
  parentWorkflowName: string;
  parentSnapshot: WorkflowRunState;
  retainedChild: WorkflowTerminalSnapshotRecord;
  storageTimestamp: number;
  executionMode: 'continuous';
}): WorkflowRunState {
  if (isProxy(input)) throw new TypeError('parent continuation patch input must not be a proxy');
  const expectedInputKeys = new Set([
    'contract',
    'effect',
    'parentRevision',
    'parentWorkflowName',
    'parentSnapshot',
    'retainedChild',
    'storageTimestamp',
    'executionMode',
  ]);
  const inputKeys = Reflect.ownKeys(input);
  if (
    inputKeys.length !== expectedInputKeys.size ||
    inputKeys.some(key => typeof key !== 'string' || !expectedInputKeys.has(key))
  ) {
    throw new TypeError('parent continuation patch input has unexpected fields');
  }
  for (const key of inputKeys) {
    if (typeof key !== 'string') {
      throw new TypeError('parent continuation patch input contains symbol or accessor fields');
    }
    const descriptor = Object.getOwnPropertyDescriptor(input, key);
    if (!descriptor || !('value' in descriptor)) {
      throw new TypeError('parent continuation patch input contains symbol or accessor fields');
    }
  }
  const {
    contract,
    effect,
    parentRevision,
    parentWorkflowName,
    parentSnapshot,
    retainedChild,
    storageTimestamp,
    executionMode,
  } = input;
  const contractBudget = createContinuationDataBudget();
  assertBoundedDataOnly(contract, 'parent continuation contract', contractBudget);
  assertBoundedDataOnly(effect, 'parent continuation effect', contractBudget);
  readContinuationStoredState('parent', () =>
    assertBoundedDataOnly(parentSnapshot, 'parent continuation snapshot', createContinuationDataBudget()),
  );
  readContinuationStoredState('parent', () => validateStoredParentSnapshotForContinuation(parentSnapshot));
  validateParentContinuationIdentityBinding(contract, {
    effect,
    parentRevision,
    parentWorkflowName,
    parentSnapshot,
    executionMode,
  });
  validateWorkflowTerminalParentContinuationBinding(contract, {
    effect,
    parentRevision,
    parentWorkflowName,
    parentSnapshot,
    executionMode,
  });
  readContinuationStoredState('parent', () =>
    validateStoredParentSourceForContinuation(contract, effect, parentSnapshot),
  );
  if (!Number.isSafeInteger(storageTimestamp) || storageTimestamp < 0) {
    throw new TypeError('storageTimestamp must be a monotonic non-negative safe integer');
  }
  if (storageTimestamp < parentSnapshot.timestamp) {
    throw new WorkflowTerminalContinuationStoredStateError(
      'parent',
      'storageTimestamp is not monotonic: it precedes the parent snapshot timestamp',
    );
  }
  if (storageTimestamp < retainedChild.createdAt) {
    throw new WorkflowTerminalContinuationStoredStateError(
      'child',
      'storageTimestamp is not monotonic: it precedes retained child createdAt',
    );
  }

  const next = readContinuationStoredState('parent', () => clone(parentSnapshot));
  if (contract.patch.kind === 'none') return next;

  readContinuationStoredState('child', () =>
    validateRetainedBinding(retainedChild, effect, contract.childTerminalStatus),
  );
  const parentNormalizationBudget = createContinuationDataBudget();
  const childProjection = readContinuationStoredState('child', () =>
    materializeWorkflowTerminalParentContinuationChildProjection(retainedChild.envelope, retainedChild.createdAt),
  );
  const parentRequestContext = readContinuationStoredState('parent', () =>
    optionalDataRecord(parentSnapshot.requestContext, 'parent requestContext', parentNormalizationBudget),
  );
  const existing = readContinuationStoredState('parent', () =>
    dataRecord(next.context?.[contract.source.stepId], 'parent source step result', parentNormalizationBudget),
  );
  const terminalResult = readContinuationStoredState('child', () =>
    terminalResultFromRetained(retainedChild, childProjection.terminalResult, existing, effect.runId, storageTimestamp),
  );

  let sourceResult: ParentStepResult;
  if (contract.source.kind === 'step') {
    sourceResult = terminalResult;
    defineDataProperty(next.context, contract.source.stepId, sourceResult as WorkflowRunState['context'][string]);
    delete next.activeStepsPath[contract.source.stepId];
  } else {
    sourceResult = terminalResult;
    const source = contract.source;
    defineDataProperty(
      next.context,
      contract.source.stepId,
      readContinuationStoredState('parent', () =>
        mergeForeachIteration(existing, terminalResult, source),
      ) as WorkflowRunState['context'][string],
    );
  }

  next.context.__state = readContinuationStoredState('child', () =>
    clone(childProjection.finalState),
  ) as WorkflowRunState['context'][string];
  next.value = readContinuationStoredState('child', () =>
    clone(childProjection.finalState),
  ) as WorkflowRunState['value'];
  const applicationChildRequestContext: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(childProjection.requestContextPatch)) {
    if (!isInfrastructureRequestContextKey(key)) defineDataProperty(applicationChildRequestContext, key, value);
  }
  next.requestContext = { ...parentRequestContext, ...applicationChildRequestContext };

  if (contract.patch.loopWrite.kind === 'set-iteration') {
    const loopWrite = contract.patch.loopWrite;
    readContinuationStoredState('parent', () => {
      const loopResult = normalizedDataRecord(next.context[loopWrite.stepId], 'parent loop step result');
      const metadata = optionalNormalizedDataRecord(loopResult.metadata, 'parent loop metadata');
      const nextIterationMetadata = { ...metadata };
      if (contract.action.reason === 'loop-continue') {
        delete nextIterationMetadata.nestedRunId;
      }
      defineDataProperty(next.context, loopWrite.stepId, {
        ...loopResult,
        // Continuing the loop atomically retires the completed nested child
        // owner so the next iteration derives and binds its own run id. An
        // exit retains the completed owner, matching the live callback/exit
        // path and preserving the authenticated terminal-source evidence.
        metadata: { ...nextIterationMetadata, iterationCount: loopWrite.iterationCount },
      } as unknown as WorkflowRunState['context'][string]);
      if (contract.action.reason === 'loop-continue') {
        defineDataProperty(next.activeStepsPath, loopWrite.stepId, sourcePath(contract.source));
      }
    });
  }

  if (contract.patch.parentRunWrite.kind === 'set') {
    next.status = contract.patch.parentRunWrite.status;
    next.result = readContinuationStoredState('child', () => clone(sourceResult));
    next.activePaths = sourcePath(contract.source);
    if (contract.patch.parentRunWrite.status === 'failed') {
      const error = sourceResult.error;
      if (error === undefined) {
        throw new WorkflowTerminalContinuationStoredStateError(
          'child',
          'failed parent continuation source is missing an error',
        );
      }
      next.error = readContinuationStoredState('child', () => clone(error)) as WorkflowRunState['error'];
    } else {
      delete next.error;
    }
    if (next.status === 'success' || next.status === 'failed' || next.status === 'canceled') {
      next.activeStepsPath = {};
      next.suspendedPaths = {};
      next.waitingPaths = {};
      next.resumeLabels = {};
      delete next.tripwire;
      delete next.stepExecutionPath;
    }
  } else if (contract.patch.parentRunWrite.kind === 'set-suspended') {
    readContinuationStoredState('parent', () =>
      applyAggregateSuspension(next, contract, storageTimestamp, parentNormalizationBudget),
    );
  }

  next.timestamp = storageTimestamp;
  return next;
}

export class WorkflowTerminalContinuationStoredStateError extends TypeError {
  readonly state: 'parent' | 'child';

  constructor(state: 'parent' | 'child', message = `Invalid durable ${state} workflow continuation state`) {
    super(message);
    this.name = 'WorkflowTerminalContinuationStoredStateError';
    this.state = state;
  }
}

function readContinuationStoredState<T>(state: 'parent' | 'child', read: () => T): T {
  try {
    return read();
  } catch (error) {
    if (error instanceof WorkflowTerminalContinuationStoredStateError) throw error;
    throw new WorkflowTerminalContinuationStoredStateError(state, error instanceof Error ? error.message : undefined);
  }
}
