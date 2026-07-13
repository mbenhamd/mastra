import { isProxy } from 'node:util/types';
import type { WorkflowRunState, WorkflowTerminalEffectRecord, WorkflowTerminalSnapshotRecord } from '../types';
import {
  materializeWorkflowTerminalForeachStates,
  validateWorkflowTerminalParentContinuationBinding,
} from './contract';
import { WORKFLOW_TERMINAL_FOREACH_STATE_KEY, WORKFLOW_TERMINAL_FOREACH_SUSPEND_PAYLOAD_KEY } from './types';
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
  parentResult: ParentStepResult,
  nestedRunId: string,
  storageTimestamp: number,
  budget: ContinuationDataBudget,
): ParentStepResult {
  const childSnapshot = retained.snapshot;
  const terminalResult =
    childSnapshot.result === undefined
      ? {}
      : dataRecord(childSnapshot.result, 'retained child snapshot result', budget);
  const status = retained.terminalStatus;
  if (terminalResult.status !== undefined && terminalResult.status !== status) {
    throw new TypeError('retained child result status conflicts with its terminal snapshot');
  }
  if (status === 'success' && terminalResult.status !== 'success') {
    throw new TypeError('successful retained child snapshot is missing a successful result');
  }
  const rawError = terminalResult.error ?? childSnapshot.error;
  const error =
    rawError === undefined
      ? undefined
      : terminalResult.error !== undefined
        ? terminalResult.error
        : canonicalJsonValue(rawError, 'retained child error', budget);
  if (status === 'failed' && error === undefined) {
    throw new TypeError('failed retained child snapshot is missing an error');
  }
  const existingMetadata = optionalNormalizedDataRecord(parentResult.metadata, 'parent step metadata');
  const childMetadata = optionalNormalizedDataRecord(terminalResult.metadata, 'retained child result metadata');
  const startedAt = parentResult.startedAt ?? terminalResult.startedAt ?? retained.createdAt;
  const endedAt = terminalResult.endedAt ?? retained.createdAt;
  if (!Number.isSafeInteger(startedAt) || (startedAt as number) < 0) {
    throw new TypeError('parent step startedAt must be a non-negative safe integer');
  }
  if (!Number.isSafeInteger(endedAt) || (endedAt as number) < 0) {
    throw new TypeError('retained child endedAt must be a non-negative safe integer');
  }
  if (
    (startedAt as number) > (endedAt as number) ||
    (endedAt as number) > retained.createdAt ||
    retained.createdAt > storageTimestamp
  ) {
    throw new TypeError('retained child result timestamps are not monotonic');
  }
  delete terminalResult.__state;
  if (status !== 'failed') delete terminalResult.error;
  return {
    ...terminalResult,
    status,
    ...(status === 'failed' ? { error: clone(error) } : {}),
    payload: clone(parentResult.payload),
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
    retained.snapshot.runId !== effect.runId ||
    retained.snapshot.status !== retained.terminalStatus ||
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
  const inputBudget = createContinuationDataBudget();
  assertBoundedDataOnly(contract, 'parent continuation contract', inputBudget);
  assertBoundedDataOnly(effect, 'parent continuation effect', inputBudget);
  assertBoundedDataOnly(parentSnapshot, 'parent continuation snapshot', inputBudget);
  assertBoundedDataOnly(retainedChild, 'retained child terminal snapshot', inputBudget, true);
  validateWorkflowTerminalParentContinuationBinding(contract, {
    effect,
    parentRevision,
    parentWorkflowName,
    parentSnapshot,
    executionMode,
  });
  if (
    !Number.isSafeInteger(storageTimestamp) ||
    storageTimestamp < parentSnapshot.timestamp ||
    storageTimestamp < retainedChild.createdAt
  ) {
    throw new TypeError('storageTimestamp must be a monotonic non-negative safe integer');
  }

  const next = clone(parentSnapshot);
  if (contract.patch.kind === 'none') return next;

  validateRetainedBinding(retainedChild, effect, contract.childTerminalStatus);
  const retainedState = retainedChild.snapshot.context?.__state;
  if (retainedState === undefined) {
    throw new TypeError('retained child snapshot is missing final context.__state');
  }
  const normalizationBudget = createContinuationDataBudget();
  const finalState = dataRecord(retainedState, 'retained child final context.__state', normalizationBudget);
  const childRequestContext = optionalDataRecord(
    retainedChild.snapshot.requestContext,
    'retained child requestContext',
    normalizationBudget,
  );
  const parentRequestContext = optionalDataRecord(
    parentSnapshot.requestContext,
    'parent requestContext',
    normalizationBudget,
  );
  const existing = dataRecord(next.context?.[contract.source.stepId], 'parent source step result', normalizationBudget);
  const terminalResult = terminalResultFromRetained(
    retainedChild,
    existing,
    effect.runId,
    storageTimestamp,
    normalizationBudget,
  );

  let sourceResult: ParentStepResult;
  if (contract.source.kind === 'step') {
    sourceResult = terminalResult;
    defineDataProperty(next.context, contract.source.stepId, sourceResult as WorkflowRunState['context'][string]);
    delete next.activeStepsPath[contract.source.stepId];
  } else {
    sourceResult = terminalResult;
    defineDataProperty(
      next.context,
      contract.source.stepId,
      mergeForeachIteration(existing, terminalResult, contract.source) as WorkflowRunState['context'][string],
    );
  }

  next.context.__state = clone(finalState) as WorkflowRunState['context'][string];
  next.value = clone(finalState) as WorkflowRunState['value'];
  next.requestContext = { ...parentRequestContext, ...childRequestContext };

  if (contract.patch.loopWrite.kind === 'set-iteration') {
    const loopResult = normalizedDataRecord(next.context[contract.patch.loopWrite.stepId], 'parent loop step result');
    const metadata = optionalNormalizedDataRecord(loopResult.metadata, 'parent loop metadata');
    defineDataProperty(next.context, contract.patch.loopWrite.stepId, {
      ...loopResult,
      metadata: { ...metadata, iterationCount: contract.patch.loopWrite.iterationCount },
    } as unknown as WorkflowRunState['context'][string]);
    if (contract.action.reason === 'loop-continue') {
      defineDataProperty(next.activeStepsPath, contract.patch.loopWrite.stepId, sourcePath(contract.source));
    }
  }

  if (contract.patch.parentRunWrite.kind === 'set') {
    next.status = contract.patch.parentRunWrite.status;
    next.result = clone(sourceResult);
    next.activePaths = sourcePath(contract.source);
    if (contract.patch.parentRunWrite.status === 'failed') {
      const error = sourceResult.error;
      if (error === undefined) throw new TypeError('failed parent continuation source is missing an error');
      next.error = clone(error) as WorkflowRunState['error'];
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
    applyAggregateSuspension(next, contract, storageTimestamp, normalizationBudget);
  }

  next.timestamp = storageTimestamp;
  return next;
}
