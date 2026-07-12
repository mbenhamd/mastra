import { createHash } from 'node:crypto';
import type { SerializedStep, SerializedStepFlowEntry } from '../types';
import type { WorkflowTerminalSha256 } from './types';

export const MAX_TERMINAL_PATH_LENGTH = 256;
export const MAX_TERMINAL_GRAPH_NODES = 4_096;
export const MAX_TERMINAL_GRAPH_DEPTH = 64;
export const MAX_TERMINAL_GRAPH_BYTES = 1_048_576;
export const MAX_TERMINAL_ID_LENGTH = 512;
export const MAX_TERMINAL_REVISION_LENGTH = 256;
export const MAX_TERMINAL_LOOP_ITERATIONS = Number.MAX_SAFE_INTEGER - 1;

function isWellFormed(value: string): boolean {
  for (let index = 0; index < value.length; index++) {
    const code = value.charCodeAt(index);
    if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(index + 1);
      if (!(next >= 0xdc00 && next <= 0xdfff)) return false;
      index++;
    } else if (code >= 0xdc00 && code <= 0xdfff) {
      return false;
    }
  }
  return true;
}

export function validateWorkflowTerminalStructuralString(
  value: unknown,
  field: string,
  maxLength = MAX_TERMINAL_ID_LENGTH,
  allowEmpty = false,
): string {
  if (
    typeof value !== 'string' ||
    (!allowEmpty && value.length === 0) ||
    value.length > maxLength ||
    value.includes('\0') ||
    !isWellFormed(value)
  ) {
    throw new TypeError(`${field} must be a well-formed bounded string`);
  }
  return value;
}

function getDataDescriptors(value: unknown, field: string): Record<PropertyKey, PropertyDescriptor> {
  if (value === null || typeof value !== 'object') throw new TypeError(`${field} must be a data object`);
  const prototype = Object.getPrototypeOf(value);
  if (prototype !== Object.prototype && prototype !== null) throw new TypeError(`${field} must be a plain object`);
  const descriptors = Object.getOwnPropertyDescriptors(value) as Record<PropertyKey, PropertyDescriptor>;
  for (const key of Reflect.ownKeys(descriptors)) {
    if (typeof key !== 'string' || !('value' in descriptors[key]!)) {
      throw new TypeError(`${field} contains symbol or accessor fields`);
    }
  }
  return descriptors;
}

function validateKeys(
  descriptors: Record<PropertyKey, PropertyDescriptor>,
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

function getDenseArray(value: unknown, field: string, maxLength = MAX_TERMINAL_GRAPH_NODES): unknown[] {
  if (!Array.isArray(value)) throw new TypeError(`${field} must be a dense array`);
  const descriptors = Object.getOwnPropertyDescriptors(value) as unknown as Record<PropertyKey, PropertyDescriptor>;
  const length = descriptors.length?.value;
  if (!Number.isSafeInteger(length) || length < 0 || length > maxLength) {
    throw new TypeError(`${field} has an invalid length`);
  }
  const out = Array.from({ length }, (_, index) => {
    const descriptor = descriptors[String(index)];
    if (!descriptor || !('value' in descriptor)) throw new TypeError(`${field} must be a dense data-only array`);
    return descriptor.value;
  });
  if (
    Reflect.ownKeys(descriptors).some(
      key => key !== 'length' && (typeof key !== 'string' || !/^(0|[1-9][0-9]*)$/.test(key) || Number(key) >= length),
    )
  ) {
    throw new TypeError(`${field} must be a dense data-only array`);
  }
  return out;
}

function hashFramedParts(domain: string, parts: readonly string[]): string {
  const hash = createHash('sha256');
  const write = (value: string) => {
    const bytes = Buffer.from(value, 'utf8');
    hash.update(String(bytes.length));
    hash.update(':');
    hash.update(bytes);
  };
  write(domain);
  for (const part of parts) write(part);
  return hash.digest('hex');
}

function hashSource(domain: string, value: unknown, field: string, state: GraphState): string {
  const source = validateWorkflowTerminalStructuralString(value, field, MAX_TERMINAL_GRAPH_BYTES, true);
  state.bytes += Buffer.byteLength(source, 'utf8');
  if (state.bytes > MAX_TERMINAL_GRAPH_BYTES) throw new TypeError('serialized workflow graph exceeds byte limit');
  return hashFramedParts(domain, [source]);
}

interface GraphState {
  nodes: number;
  bytes: number;
}

function consumeNode(state: GraphState): void {
  state.nodes++;
  if (state.nodes > MAX_TERMINAL_GRAPH_NODES) throw new TypeError('serialized workflow graph exceeds node limit');
}

function append(state: GraphState, parts: string[], ...values: string[]): void {
  for (const value of values) {
    state.bytes += Buffer.byteLength(value, 'utf8');
    if (state.bytes > MAX_TERMINAL_GRAPH_BYTES) throw new TypeError('serialized workflow graph exceeds byte limit');
    parts.push(value);
  }
}

function normalizeStep(
  value: unknown,
  parts: string[],
  state: GraphState,
  scopeStepIds: Set<string>,
  depth: number,
  field: string,
): string {
  consumeNode(state);
  const descriptors = getDataDescriptors(value, field);
  validateKeys(
    descriptors,
    ['id', 'description', 'metadata', 'component', 'serializedStepFlow', 'mapConfig', 'canSuspend'],
    ['id'],
    field,
  );
  const id = validateWorkflowTerminalStructuralString(descriptors.id!.value, `${field}.id`);
  if (scopeStepIds.has(id)) throw new TypeError(`serialized workflow graph contains duplicate step id ${id}`);
  scopeStepIds.add(id);
  const component =
    descriptors.component === undefined
      ? ''
      : validateWorkflowTerminalStructuralString(descriptors.component.value, `${field}.component`, 512, true);
  const mapConfig =
    descriptors.mapConfig === undefined
      ? ''
      : hashSource(
          'mastra.workflow-terminal-parent-graph.map-config.v1',
          descriptors.mapConfig.value,
          `${field}.mapConfig`,
          state,
        );
  const canSuspend = descriptors.canSuspend?.value;
  if (canSuspend !== undefined && typeof canSuspend !== 'boolean') {
    throw new TypeError(`${field}.canSuspend must be boolean`);
  }
  append(state, parts, 'step', id, component, mapConfig, canSuspend === undefined ? '' : String(canSuspend));
  const nested = descriptors.serializedStepFlow?.value;
  if (nested === undefined) {
    append(state, parts, 'nested', '0');
  } else {
    const entries = getDenseArray(nested, `${field}.serializedStepFlow`);
    append(state, parts, 'nested', String(entries.length));
    normalizeGraph(entries, parts, state, depth + 1, `${field}.serializedStepFlow`);
  }
  return id;
}

function normalizeCondition(value: unknown, parts: string[], state: GraphState, field: string): string {
  consumeNode(state);
  const descriptors = getDataDescriptors(value, field);
  validateKeys(descriptors, ['id', 'fn'], ['id', 'fn'], field);
  const id = validateWorkflowTerminalStructuralString(descriptors.id!.value, `${field}.id`);
  append(
    state,
    parts,
    id,
    hashSource(
      'mastra.workflow-terminal-parent-graph.condition-source.v1',
      descriptors.fn!.value,
      `${field}.fn`,
      state,
    ),
  );
  return id;
}

function normalizeGraph(
  entries: readonly unknown[],
  parts: string[],
  state: GraphState,
  depth: number,
  field: string,
): void {
  if (depth > MAX_TERMINAL_GRAPH_DEPTH) throw new TypeError('serialized workflow graph exceeds depth limit');
  const scopeStepIds = new Set<string>();
  for (let index = 0; index < entries.length; index++) {
    consumeNode(state);
    const entryField = `${field}[${index}]`;
    const descriptors = getDataDescriptors(entries[index], entryField);
    const type = descriptors.type?.value;
    append(state, parts, 'entry', String(index), String(type));
    if (type === 'step') {
      validateKeys(descriptors, ['type', 'step'], ['type', 'step'], entryField);
      normalizeStep(descriptors.step!.value, parts, state, scopeStepIds, depth, `${entryField}.step`);
    } else if (type === 'sleep' || type === 'sleepUntil') {
      validateKeys(
        descriptors,
        type === 'sleep' ? ['type', 'id', 'duration', 'fn'] : ['type', 'id', 'date', 'fn'],
        ['type', 'id'],
        entryField,
      );
      const id = validateWorkflowTerminalStructuralString(descriptors.id!.value, `${entryField}.id`);
      if (scopeStepIds.has(id)) throw new TypeError(`serialized workflow graph contains duplicate step id ${id}`);
      scopeStepIds.add(id);
      let timing = '';
      if (type === 'sleep') {
        const duration = descriptors.duration?.value;
        if ((duration === undefined) === (descriptors.fn?.value === undefined)) {
          throw new TypeError(`${entryField} must contain exactly one of duration or fn`);
        }
        if (duration !== undefined && (!Number.isFinite(duration) || duration < 0)) {
          throw new TypeError(`${entryField}.duration must be a non-negative finite number`);
        }
        timing = duration === undefined ? '' : String(duration);
      } else {
        const date = descriptors.date?.value;
        if ((date === undefined) === (descriptors.fn?.value === undefined)) {
          throw new TypeError(`${entryField} must contain exactly one of date or fn`);
        }
        if (date !== undefined) {
          if (!(date instanceof Date) && typeof date !== 'string') {
            throw new TypeError(`${entryField}.date must be a Date or string`);
          }
          const epoch = date instanceof Date ? Date.prototype.getTime.call(date) : Date.parse(date);
          if (!Number.isFinite(epoch)) throw new TypeError(`${entryField}.date must be valid`);
          if (typeof date === 'string' && new Date(epoch).toISOString() !== date) {
            throw new TypeError(`${entryField}.date must be a canonical ISO timestamp`);
          }
          timing = String(epoch);
        }
      }
      const fn =
        descriptors.fn === undefined
          ? ''
          : hashSource(
              'mastra.workflow-terminal-parent-graph.sleep-source.v1',
              descriptors.fn.value,
              `${entryField}.fn`,
              state,
            );
      append(state, parts, id, timing, fn);
    } else if (type === 'parallel' || type === 'conditional') {
      validateKeys(
        descriptors,
        type === 'conditional' ? ['type', 'steps', 'serializedConditions'] : ['type', 'steps'],
        type === 'conditional' ? ['type', 'steps', 'serializedConditions'] : ['type', 'steps'],
        entryField,
      );
      const steps = getDenseArray(descriptors.steps!.value, `${entryField}.steps`);
      append(state, parts, 'branches', String(steps.length));
      const branchIds = steps.map((branch, branchIndex) => {
        const branchDescriptors = getDataDescriptors(branch, `${entryField}.steps[${branchIndex}]`);
        validateKeys(branchDescriptors, ['type', 'step'], ['type', 'step'], `${entryField}.steps[${branchIndex}]`);
        if (branchDescriptors.type!.value !== 'step') throw new TypeError(`${entryField} branch must be a step`);
        return normalizeStep(
          branchDescriptors.step!.value,
          parts,
          state,
          scopeStepIds,
          depth,
          `${entryField}.steps[${branchIndex}].step`,
        );
      });
      if (type === 'conditional') {
        const conditions = getDenseArray(descriptors.serializedConditions!.value, `${entryField}.serializedConditions`);
        if (conditions.length !== branchIds.length) {
          throw new TypeError(`${entryField} conditional branch and condition counts differ`);
        }
        append(state, parts, 'conditions', String(conditions.length));
        conditions.forEach((condition, conditionIndex) => {
          const conditionId = normalizeCondition(
            condition,
            parts,
            state,
            `${entryField}.serializedConditions[${conditionIndex}]`,
          );
          if (conditionId !== branchIds[conditionIndex]) {
            throw new TypeError(`${entryField} conditional branch and condition IDs differ`);
          }
        });
      }
    } else if (type === 'loop') {
      validateKeys(
        descriptors,
        ['type', 'step', 'serializedCondition', 'loopType'],
        ['type', 'step', 'serializedCondition', 'loopType'],
        entryField,
      );
      if (descriptors.loopType!.value !== 'dowhile' && descriptors.loopType!.value !== 'dountil') {
        throw new TypeError(`${entryField}.loopType is invalid`);
      }
      append(state, parts, descriptors.loopType!.value);
      const stepId = normalizeStep(descriptors.step!.value, parts, state, scopeStepIds, depth, `${entryField}.step`);
      const conditionId = normalizeCondition(
        descriptors.serializedCondition!.value,
        parts,
        state,
        `${entryField}.serializedCondition`,
      );
      if (conditionId !== stepId) throw new TypeError(`${entryField} loop step and condition IDs differ`);
    } else if (type === 'foreach') {
      validateKeys(descriptors, ['type', 'step', 'opts'], ['type', 'step', 'opts'], entryField);
      normalizeStep(descriptors.step!.value, parts, state, scopeStepIds, depth, `${entryField}.step`);
      const opts = getDataDescriptors(descriptors.opts!.value, `${entryField}.opts`);
      validateKeys(opts, ['concurrency'], ['concurrency'], `${entryField}.opts`);
      const concurrency = opts.concurrency!.value;
      if (!Number.isSafeInteger(concurrency) || concurrency < 1) {
        throw new TypeError(`${entryField}.opts.concurrency must be a positive safe integer`);
      }
      append(state, parts, String(concurrency));
    } else {
      throw new TypeError(`${entryField}.type is invalid`);
    }
  }
}

/** @internal Fingerprints execution-semantic serialized graph structure, never raw callback source. */
export function createWorkflowTerminalGraphFingerprint(
  graph: readonly SerializedStepFlowEntry[],
): WorkflowTerminalSha256 {
  const entries = getDenseArray(graph, 'serialized workflow graph');
  const parts: string[] = ['1', String(entries.length)];
  normalizeGraph(entries, parts, { nodes: 0, bytes: 0 }, 0, 'serialized workflow graph');
  return `sha256:${hashFramedParts('mastra.workflow-terminal-parent-graph.v1', parts)}`;
}

export type WorkflowTerminalResolvedGraphEntry =
  | { kind: 'step'; stepId: string }
  | { kind: 'branch'; containerType: 'parallel' | 'conditional'; stepId: string }
  | { kind: 'container'; containerType: 'parallel' | 'conditional' }
  | { kind: 'loop'; stepId: string; loopType: 'dowhile' | 'dountil' }
  | { kind: 'foreach'; stepId: string; iterationIndex?: number }
  | { kind: 'sleep' | 'sleepUntil'; entryId: string };

function readStepId(step: SerializedStep, field: string): string {
  const descriptors = getDataDescriptors(step, field);
  return validateWorkflowTerminalStructuralString(descriptors.id?.value, `${field}.id`);
}

/** @internal Resolves only the immediate parent graph coordinate used by terminal effects. */
export function resolveWorkflowTerminalGraphCoordinate(
  graph: readonly SerializedStepFlowEntry[],
  executionPath: readonly number[],
): WorkflowTerminalResolvedGraphEntry {
  const entries = getDenseArray(graph, 'serialized workflow graph') as SerializedStepFlowEntry[];
  const path = getDenseArray(executionPath, 'executionPath', MAX_TERMINAL_PATH_LENGTH);
  if (
    path.length === 0 ||
    path.length > 2 ||
    path.some(value => !Number.isSafeInteger(value) || (value as number) < 0)
  ) {
    throw new TypeError('executionPath is not an immediate parent graph coordinate');
  }
  const rootIndex = path[0] as number;
  const entry = entries[rootIndex];
  if (!entry) throw new TypeError('executionPath does not exist in serialized workflow graph');
  if (entry.type === 'parallel' || entry.type === 'conditional') {
    if (path.length === 1) return { kind: 'container', containerType: entry.type };
    const branch = entry.steps[path[1] as number];
    if (!branch) throw new TypeError('executionPath branch does not exist');
    return {
      kind: 'branch',
      containerType: entry.type,
      stepId: readStepId(branch.step, `serialized workflow graph[${rootIndex}].steps[${path[1]}].step`),
    };
  }
  if (entry.type === 'foreach') {
    if (path.length > 2) throw new TypeError('foreach executionPath is invalid');
    return {
      kind: 'foreach',
      stepId: readStepId(entry.step, `serialized workflow graph[${rootIndex}].step`),
      ...(path.length === 2 ? { iterationIndex: path[1] as number } : {}),
    };
  }
  if (path.length !== 1) throw new TypeError('executionPath contains a surplus segment');
  if (entry.type === 'step') {
    return { kind: 'step', stepId: readStepId(entry.step, `serialized workflow graph[${rootIndex}].step`) };
  }
  if (entry.type === 'loop') {
    return {
      kind: 'loop',
      stepId: readStepId(entry.step, `serialized workflow graph[${rootIndex}].step`),
      loopType: entry.loopType,
    };
  }
  return { kind: entry.type, entryId: entry.id };
}
