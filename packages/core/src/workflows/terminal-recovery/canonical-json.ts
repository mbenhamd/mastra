import { Buffer } from 'node:buffer';
import { isDate, isNativeError, isProxy } from 'node:util/types';
import type { WorkflowTerminalCanonicalJsonObject, WorkflowTerminalCanonicalJsonValue } from './types';

export const MAX_WORKFLOW_TERMINAL_RECOVERY_VALUE_DEPTH = 64;
export const MAX_WORKFLOW_TERMINAL_RECOVERY_CONTAINER_ENTRIES = 100_000;
export const MAX_WORKFLOW_TERMINAL_RECOVERY_TOTAL_NODES = 100_000;
export const MAX_WORKFLOW_TERMINAL_RECOVERY_ENVELOPE_BYTES = 8 * 1024 * 1024;
export const MAX_WORKFLOW_TERMINAL_RECOVERY_ERROR_STACK_BYTES = 256 * 1024;
export const MAX_WORKFLOW_TERMINAL_RECOVERY_ANCESTRY_DEPTH = 64;

const OMIT_PROPERTY = Symbol('workflow-terminal-recovery-omit-property');

type Normalized = WorkflowTerminalCanonicalJsonValue | typeof OMIT_PROPERTY;

interface CanonicalState {
  ancestors: Set<object>;
  nodes: number;
  entries: number;
  estimatedBytes: number;
}

function createState(): CanonicalState {
  return { ancestors: new Set(), nodes: 0, entries: 0, estimatedBytes: 0 };
}

function fail(field: string, reason: string): never {
  throw new TypeError(`Invalid workflow terminal recovery data at ${field}: ${reason}`);
}

function consumeNode(state: CanonicalState, field: string): void {
  state.nodes++;
  if (state.nodes > MAX_WORKFLOW_TERMINAL_RECOVERY_TOTAL_NODES) fail(field, 'node limit exceeded');
}

function consumeEntries(state: CanonicalState, count: number, field: string): void {
  state.entries += count;
  if (
    count > MAX_WORKFLOW_TERMINAL_RECOVERY_CONTAINER_ENTRIES ||
    state.entries > MAX_WORKFLOW_TERMINAL_RECOVERY_CONTAINER_ENTRIES
  ) {
    fail(field, 'container-entry limit exceeded');
  }
  // Conservatively reserve comma/colon plus container-delimiter bytes before
  // JSON serialization allocates its output buffer.
  state.estimatedBytes += count * 2 + 2;
  if (state.estimatedBytes > MAX_WORKFLOW_TERMINAL_RECOVERY_ENVELOPE_BYTES) {
    fail(field, 'byte limit exceeded');
  }
}

function consumeBytes(state: CanonicalState, value: string, field: string): void {
  let escapedBytes = Buffer.byteLength(value, 'utf8') + 2;
  for (let index = 0; index < value.length; index++) {
    const code = value.charCodeAt(index);
    if (
      code === 0x22 ||
      code === 0x5c ||
      code === 0x08 ||
      code === 0x09 ||
      code === 0x0a ||
      code === 0x0c ||
      code === 0x0d
    ) {
      escapedBytes += 1;
    } else if (code < 0x20) {
      escapedBytes += 5;
    } else if (code >= 0xd800 && code <= 0xdbff) {
      index++;
    }
  }
  state.estimatedBytes += escapedBytes;
  if (state.estimatedBytes > MAX_WORKFLOW_TERMINAL_RECOVERY_ENVELOPE_BYTES) fail(field, 'byte limit exceeded');
}

export function validateWorkflowTerminalRecoveryString(value: unknown, field: string): string {
  if (typeof value !== 'string') fail(field, 'must be a string');
  if (value.includes('\0')) fail(field, 'contains a null character');
  for (let index = 0; index < value.length; index++) {
    const code = value.charCodeAt(index);
    if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(index + 1);
      if (!(next >= 0xdc00 && next <= 0xdfff)) fail(field, 'contains malformed Unicode');
      index++;
    } else if (code >= 0xdc00 && code <= 0xdfff) {
      fail(field, 'contains malformed Unicode');
    }
  }
  return value;
}

function defineData(target: Record<string, WorkflowTerminalCanonicalJsonValue>, key: string, value: Normalized): void {
  if (value === OMIT_PROPERTY) return;
  Object.defineProperty(target, key, {
    configurable: true,
    enumerable: true,
    value,
    writable: true,
  });
}

function dataDescriptors(value: object, field: string): Record<PropertyKey, PropertyDescriptor> {
  if (isProxy(value)) fail(field, 'proxies are not allowed');
  return Object.getOwnPropertyDescriptors(value) as Record<PropertyKey, PropertyDescriptor>;
}

function inheritedErrorString(error: Error, key: 'name' | 'message', fallback: string, field: string): string {
  let current: object | null = error;
  while (current) {
    if (isProxy(current)) fail(field, 'proxy prototypes are not allowed');
    const descriptor = Object.getOwnPropertyDescriptor(current, key);
    if (descriptor) {
      if (!('value' in descriptor)) fail(field, `${key} must be a data property`);
      if (descriptor.value === undefined && key === 'message') return fallback;
      if (typeof descriptor.value !== 'string') fail(field, `${key} must be a string`);
      return validateWorkflowTerminalRecoveryString(descriptor.value, `${field}.${key}`);
    }
    current = Object.getPrototypeOf(current);
  }
  return fallback;
}

function normalizeError(
  error: Error,
  field: string,
  depth: number,
  state: CanonicalState,
): WorkflowTerminalCanonicalJsonObject {
  if (depth > MAX_WORKFLOW_TERMINAL_RECOVERY_VALUE_DEPTH) fail(field, 'depth limit exceeded');
  if (state.ancestors.has(error)) fail(field, 'contains a cycle');
  state.ancestors.add(error);
  try {
    const descriptors = dataDescriptors(error, field);
    const ownKeys = Reflect.ownKeys(descriptors);
    consumeEntries(state, ownKeys.length + 2, field);
    const output: WorkflowTerminalCanonicalJsonObject = {};
    const message = inheritedErrorString(error, 'message', '', field);
    const name = inheritedErrorString(error, 'name', 'Error', field);
    consumeBytes(state, 'message', `${field} key`);
    consumeBytes(state, message, `${field}.message`);
    consumeBytes(state, 'name', `${field} key`);
    consumeBytes(state, name, `${field}.name`);
    defineData(output, 'message', message);
    defineData(output, 'name', name);

    for (const key of ownKeys) {
      if (typeof key !== 'string') fail(field, 'symbol keys are not allowed');
      const descriptor = descriptors[key];
      if (!descriptor) fail(`${field}.${key}`, 'descriptor is missing');
      // Some runtimes expose Error.stack lazily. It is safe to omit that one
      // diagnostic without invoking the getter; every other accessor fails.
      if (key === 'stack' && !('value' in descriptor)) continue;
      if (!('value' in descriptor)) fail(`${field}.${key}`, 'accessors are not allowed');
      validateWorkflowTerminalRecoveryString(key, `${field} key`);
      if (key === 'name' || key === 'message') continue;
      consumeBytes(state, key, `${field} key`);
      if (key === 'stack') {
        if (descriptor.value === undefined) continue;
        if (typeof descriptor.value !== 'string') fail(`${field}.stack`, 'must be a string');
        const stack = validateWorkflowTerminalRecoveryString(descriptor.value, `${field}.stack`);
        if (Buffer.byteLength(stack, 'utf8') > MAX_WORKFLOW_TERMINAL_RECOVERY_ERROR_STACK_BYTES) {
          fail(`${field}.stack`, 'byte limit exceeded');
        }
        consumeBytes(state, stack, `${field}.stack`);
        defineData(output, 'stack', stack);
        continue;
      }
      if (key === 'cause') {
        if (descriptor.value === undefined) continue;
        const cause = normalizeValue(descriptor.value, `${field}.cause`, depth + 1, state, false);
        defineData(output, 'cause', cause);
        continue;
      }
      if (!descriptor.enumerable) fail(`${field}.${key}`, 'custom Error fields must be enumerable');
      defineData(output, key, normalizeValue(descriptor.value, `${field}.${key}`, depth + 1, state, false));
    }
    return sortObject(output);
  } finally {
    state.ancestors.delete(error);
  }
}

function sortObject(input: WorkflowTerminalCanonicalJsonObject): WorkflowTerminalCanonicalJsonObject {
  const output: WorkflowTerminalCanonicalJsonObject = {};
  for (const key of Object.keys(input).sort()) defineData(output, key, input[key]!);
  return output;
}

function normalizeArray(
  value: unknown[],
  field: string,
  depth: number,
  state: CanonicalState,
): WorkflowTerminalCanonicalJsonValue[] {
  const descriptors = dataDescriptors(value, field);
  const length = descriptors.length?.value;
  if (!Number.isSafeInteger(length) || (length as number) < 0) fail(field, 'array length is invalid');
  consumeEntries(state, length as number, field);
  for (const key of Reflect.ownKeys(descriptors)) {
    if (key === 'length') continue;
    if (typeof key !== 'string' || !/^(0|[1-9][0-9]*)$/.test(key) || Number(key) >= (length as number)) {
      fail(field, 'arrays may contain only indexed data properties');
    }
  }
  return Array.from({ length: length as number }, (_, index) => {
    const descriptor = descriptors[String(index)];
    if (!descriptor || !('value' in descriptor) || !descriptor.enumerable) {
      fail(`${field}[${index}]`, 'array must be dense and data-only');
    }
    const normalized = normalizeValue(descriptor.value, `${field}[${index}]`, depth + 1, state, true);
    return normalized === OMIT_PROPERTY ? null : normalized;
  });
}

function normalizeObject(
  value: object,
  field: string,
  depth: number,
  state: CanonicalState,
): WorkflowTerminalCanonicalJsonObject {
  const prototype = Object.getPrototypeOf(value);
  if (prototype !== Object.prototype && prototype !== null) fail(field, 'custom prototypes are not allowed');
  const descriptors = dataDescriptors(value, field);
  const keys = Reflect.ownKeys(descriptors);
  consumeEntries(state, keys.length, field);
  const output: WorkflowTerminalCanonicalJsonObject = {};
  const stringKeys: string[] = [];
  for (const key of keys) {
    if (typeof key !== 'string') fail(field, 'symbol keys are not allowed');
    const descriptor = descriptors[key];
    if (!descriptor || !('value' in descriptor)) fail(`${field}.${key}`, 'accessors are not allowed');
    if (!descriptor.enumerable) fail(`${field}.${key}`, 'non-enumerable fields are not allowed');
    validateWorkflowTerminalRecoveryString(key, `${field} key`);
    consumeBytes(state, key, `${field} key`);
    stringKeys.push(key);
  }
  for (const key of stringKeys.sort()) {
    defineData(output, key, normalizeValue(descriptors[key]!.value, `${field}.${key}`, depth + 1, state, false));
  }
  return output;
}

function normalizeValue(
  value: unknown,
  field: string,
  depth: number,
  state: CanonicalState,
  arrayEntry: boolean,
): Normalized {
  if (depth > MAX_WORKFLOW_TERMINAL_RECOVERY_VALUE_DEPTH) fail(field, 'depth limit exceeded');
  consumeNode(state, field);
  if (value === undefined) return arrayEntry ? null : OMIT_PROPERTY;
  if (value === null || typeof value === 'boolean') return value;
  if (typeof value === 'string') {
    const string = validateWorkflowTerminalRecoveryString(value, field);
    consumeBytes(state, string, field);
    return string;
  }
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) fail(field, 'non-finite numbers are not allowed');
    return value === 0 ? 0 : value;
  }
  if (typeof value !== 'object') fail(field, 'non-JSON values are not allowed');
  if (isProxy(value)) fail(field, 'proxies are not allowed');
  if (state.ancestors.has(value)) fail(field, 'contains a cycle');
  if (isDate(value)) {
    if (Reflect.ownKeys(dataDescriptors(value, field)).length !== 0) fail(field, 'Date must not have custom fields');
    const epoch = Date.prototype.getTime.call(value);
    if (!Number.isFinite(epoch)) fail(field, 'invalid Date');
    const iso = new Date(epoch).toISOString();
    consumeBytes(state, iso, field);
    return iso;
  }
  if (isNativeError(value)) return normalizeError(value, field, depth, state);
  state.ancestors.add(value);
  try {
    if (Array.isArray(value)) return normalizeArray(value, field, depth, state);
    return normalizeObject(value, field, depth, state);
  } finally {
    state.ancestors.delete(value);
  }
}

function assertFinalBytes(value: WorkflowTerminalCanonicalJsonValue, field: string): void {
  const bytes = Buffer.byteLength(JSON.stringify(value), 'utf8');
  if (bytes > MAX_WORKFLOW_TERMINAL_RECOVERY_ENVELOPE_BYTES) fail(field, 'byte limit exceeded');
}

export function materializeWorkflowTerminalCanonicalJson(
  value: unknown,
  field = 'value',
): WorkflowTerminalCanonicalJsonValue {
  const normalized = normalizeValue(value, field, 0, createState(), false);
  if (normalized === OMIT_PROPERTY) fail(field, 'top-level undefined is not allowed');
  assertFinalBytes(normalized, field);
  return normalized;
}

export function materializeWorkflowTerminalCanonicalJsonObject(
  value: unknown,
  field = 'value',
): WorkflowTerminalCanonicalJsonObject {
  const normalized = materializeWorkflowTerminalCanonicalJson(value, field);
  if (normalized === null || Array.isArray(normalized) || typeof normalized !== 'object') {
    fail(field, 'must be an object');
  }
  return normalized;
}

/** Missing and undefined both mean an empty patch; null remains invalid. */
export function materializeWorkflowTerminalCanonicalJsonObjectPatch(
  value: unknown,
  field = 'value',
): WorkflowTerminalCanonicalJsonObject {
  return value === undefined ? {} : materializeWorkflowTerminalCanonicalJsonObject(value, field);
}

export function getWorkflowTerminalCanonicalJson(value: WorkflowTerminalCanonicalJsonValue): string {
  const normalized = materializeWorkflowTerminalCanonicalJson(value, 'canonical value');
  return JSON.stringify(normalized);
}
