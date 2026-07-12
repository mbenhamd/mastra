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

function assertNoAccessors(value: unknown, field: string, seen = new WeakSet<object>(), allowErrorStack = false): void {
  if (value === null || typeof value !== 'object') {
    if (typeof value === 'function') throw new TypeError(`${field} contains non-data values`);
    return;
  }
  if (seen.has(value)) return;
  seen.add(value);
  const descriptors = Object.getOwnPropertyDescriptors(value);
  for (const key of Reflect.ownKeys(descriptors)) {
    if (typeof key !== 'string') {
      throw new TypeError(`${field} contains symbol or accessor fields`);
    }
    const descriptor = descriptors[key];
    if (allowErrorStack && value instanceof Error && key === 'stack' && descriptor && !('value' in descriptor)) {
      continue;
    }
    if (!descriptor || !('value' in descriptor)) throw new TypeError(`${field} contains symbol or accessor fields`);
    assertNoAccessors(descriptor.value, `${field}.${key}`, seen, allowErrorStack);
  }
  if (value instanceof Map) {
    for (const [key, entry] of Map.prototype.entries.call(value) as MapIterator<[unknown, unknown]>) {
      assertNoAccessors(key, `${field} map key`, seen, allowErrorStack);
      assertNoAccessors(entry, `${field} map value`, seen, allowErrorStack);
    }
  } else if (value instanceof Set) {
    for (const entry of Set.prototype.values.call(value) as SetIterator<unknown>) {
      assertNoAccessors(entry, `${field} set value`, seen, allowErrorStack);
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
  ancestors = new Set<object>(),
  arrayEntry = false,
): unknown | typeof OMIT_JSON_PROPERTY {
  if (value === undefined) return arrayEntry ? null : OMIT_JSON_PROPERTY;
  if (value === null || typeof value === 'boolean') return value;
  if (typeof value === 'string') return assertJsonString(value, field);
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) throw new TypeError(`${field} contains a non-finite number`);
    return value === 0 ? 0 : value;
  }
  if (typeof value !== 'object') throw new TypeError(`${field} contains non-JSON data`);
  if (ancestors.has(value)) throw new TypeError(`${field} contains a cycle`);
  if (value instanceof Date) {
    const epoch = Date.prototype.getTime.call(value);
    if (!Number.isFinite(epoch)) throw new TypeError(`${field} contains an invalid Date`);
    return new Date(epoch).toISOString();
  }

  ancestors.add(value);
  try {
    if (value instanceof Error) {
      const own = Object.getOwnPropertyDescriptors(value) as Record<PropertyKey, PropertyDescriptor>;
      const errorRecord: Record<string, unknown> = {
        name: assertJsonString(errorDataString(value, 'name', 'Error'), `${field}.name`),
        message: assertJsonString(errorDataString(value, 'message', ''), `${field}.message`),
      };
      const stack = own.stack;
      if (stack && 'value' in stack && typeof stack.value === 'string') {
        errorRecord.stack = assertJsonString(stack.value, `${field}.stack`);
      }
      for (const key of Reflect.ownKeys(own)) {
        if (typeof key !== 'string') {
          throw new TypeError(`${field} contains symbol or accessor fields`);
        }
        if (key === 'name' || key === 'message' || key === 'stack') continue;
        if (!('value' in own[key]!)) throw new TypeError(`${field} contains symbol or accessor fields`);
        assertJsonString(key, `${field} key`);
        const normalized = canonicalJsonValue(own[key]!.value, `${field}.${key}`, ancestors);
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
      return Array.from({ length }, (_, index) => {
        const descriptor = descriptors[String(index)];
        if (!descriptor || !('value' in descriptor)) throw new TypeError(`${field} must be a dense data-only array`);
        return canonicalJsonValue(descriptor.value, `${field}[${index}]`, ancestors, true);
      });
    }
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) {
      throw new TypeError(`${field} contains a non-JSON object`);
    }
    const descriptors = Object.getOwnPropertyDescriptors(value) as Record<PropertyKey, PropertyDescriptor>;
    const output: Record<string, unknown> = {};
    for (const key of Reflect.ownKeys(descriptors)) {
      if (typeof key !== 'string' || !('value' in descriptors[key]!)) {
        throw new TypeError(`${field} contains symbol or accessor fields`);
      }
      assertJsonString(key, `${field} key`);
      const normalized = canonicalJsonValue(descriptors[key]!.value, `${field}.${key}`, ancestors);
      if (normalized !== OMIT_JSON_PROPERTY) defineDataProperty(output, key, normalized);
    }
    return output;
  } finally {
    ancestors.delete(value);
  }
}

function dataRecord(value: unknown, field: string): Record<string, unknown> {
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
  for (const [key, descriptor] of Object.entries(descriptors)) {
    const normalized = canonicalJsonValue(descriptor.value, `${field}.${key}`);
    if (normalized !== OMIT_JSON_PROPERTY) defineDataProperty(output, key, normalized);
  }
  return output;
}

function optionalDataRecord(value: unknown, field: string): Record<string, unknown> {
  return value === undefined ? {} : dataRecord(value, field);
}

function terminalResultFromRetained(
  retained: WorkflowTerminalSnapshotRecord,
  parentResult: ParentStepResult,
  nestedRunId: string,
  storageTimestamp: number,
): ParentStepResult {
  const childSnapshot = retained.snapshot;
  const terminalResult =
    childSnapshot.result === undefined ? {} : dataRecord(childSnapshot.result, 'retained child snapshot result');
  const status = retained.terminalStatus;
  if (terminalResult.status !== undefined && terminalResult.status !== status) {
    throw new TypeError('retained child result status conflicts with its terminal snapshot');
  }
  if (status === 'success' && terminalResult.status !== 'success') {
    throw new TypeError('successful retained child snapshot is missing a successful result');
  }
  const rawError = terminalResult.error ?? childSnapshot.error;
  const error = rawError === undefined ? undefined : canonicalJsonValue(rawError, 'retained child error');
  if (status === 'failed' && error === undefined) {
    throw new TypeError('failed retained child snapshot is missing an error');
  }
  const existingMetadata = optionalDataRecord(parentResult.metadata, 'parent step metadata');
  const childMetadata = optionalDataRecord(terminalResult.metadata, 'retained child result metadata');
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

  const metadata = optionalDataRecord(existing.metadata, 'parent foreach metadata');
  const workflowMetadata = optionalDataRecord(metadata.__workflow_meta, 'parent foreach workflow metadata');
  const iterationStates = optionalDataRecord(
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
  const label = dataRecord(value, field);
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
  const suspendPayload = optionalDataRecord(result.suspendPayload, `${field}.suspendPayload`);
  const workflowMetadata = optionalDataRecord(suspendPayload.__workflow_meta, `${field} workflow metadata`);
  const resumeLabels = optionalDataRecord(workflowMetadata.resumeLabels, `${field} resume labels`);
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
      const result = dataRecord(snapshot.context[entry.step.id], `parent branch ${entry.step.id}`);
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
  const current = dataRecord(snapshot.context[contract.source.stepId], 'parent foreach suspension result');
  if (!Array.isArray(current.output)) throw new TypeError('parent foreach suspension output is missing');
  const metadata = optionalDataRecord(current.metadata, 'parent foreach suspension metadata');
  const workflowMetadata = optionalDataRecord(metadata.__workflow_meta, 'parent foreach suspension workflow metadata');
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
      const result = dataRecord(iteration, `parent foreach suspension iteration ${index}`);
      if (result.status === 'suspended') {
        suspendedCount++;
        if (result.suspendPayload !== undefined) {
          const suspendPayload = optionalDataRecord(
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
  const inputDescriptors = Object.getOwnPropertyDescriptors(input);
  for (const key of Reflect.ownKeys(inputDescriptors)) {
    if (typeof key !== 'string') {
      throw new TypeError('parent continuation patch input contains symbol or accessor fields');
    }
    const descriptor = inputDescriptors[key];
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
  assertNoAccessors(contract, 'parent continuation contract');
  assertNoAccessors(effect, 'parent continuation effect');
  assertNoAccessors(parentSnapshot, 'parent continuation snapshot');
  assertNoAccessors(retainedChild, 'retained child terminal snapshot', new WeakSet<object>(), true);
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
  const finalState = dataRecord(retainedState, 'retained child final context.__state');
  const childRequestContext = optionalDataRecord(
    retainedChild.snapshot.requestContext,
    'retained child requestContext',
  );
  const existing = dataRecord(next.context?.[contract.source.stepId], 'parent source step result');
  const terminalResult = terminalResultFromRetained(retainedChild, existing, effect.runId, storageTimestamp);

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
  next.requestContext = { ...next.requestContext, ...childRequestContext };

  if (contract.patch.loopWrite.kind === 'set-iteration') {
    const loopResult = dataRecord(next.context[contract.patch.loopWrite.stepId], 'parent loop step result');
    const metadata = optionalDataRecord(loopResult.metadata, 'parent loop metadata');
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
    applyAggregateSuspension(next, contract, storageTimestamp);
  }

  next.timestamp = storageTimestamp;
  return next;
}
