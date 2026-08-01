import { createHash } from 'node:crypto';
import { isDate, isProxy } from 'node:util/types';
import { SERIALIZED_AGENT_PASSTHROUGH_OPTION_KEYS } from '../types';
import type { SerializedStep, SerializedStepFlowEntry } from '../types';
import { getDenseDataArray, getPlainDataDescriptors } from './data-shape';
import type { WorkflowTerminalSha256 } from './types';

export const MAX_TERMINAL_PATH_LENGTH = 256;
export const MAX_TERMINAL_GRAPH_NODES = 4_096;
export const MAX_TERMINAL_GRAPH_DEPTH = 64;
export const MAX_TERMINAL_GRAPH_BYTES = 1_048_576;
export const MAX_TERMINAL_ID_LENGTH = 512;
export const MAX_TERMINAL_REVISION_LENGTH = 256;
export const MAX_TERMINAL_LOOP_ITERATIONS = Number.MAX_SAFE_INTEGER - 1;

export function isWorkflowTerminalNativeFunctionSource(source: string): boolean {
  return /^function(?:\s+[^()]*)?\s*\([^)]*\)\s*\{\s*\[native code\]\s*\}$/.test(source);
}

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
  return getPlainDataDescriptors(value, {
    allowNullPrototype: true,
    typeError: `${field} must be a data object`,
    proxyError: `${field} must not be a proxy`,
    prototypeError: `${field} must be a plain object`,
    fieldsError: `${field} contains symbol, accessor, or non-enumerable fields`,
    maxKeys: 8,
    maxKeysError: `${field} contains too many fields`,
  });
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
  return getDenseDataArray(value, {
    typeError: `${field} must be a dense array`,
    proxyError: `${field} must not be a proxy`,
    lengthError: `${field} has an invalid length`,
    dataError: `${field} must be a dense data-only array`,
    maxLength,
  });
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

function hashBoundedSource(
  domain: string,
  value: unknown,
  field: string,
  state: GraphState,
  validateSource?: (source: string) => void,
): string {
  const source = validateWorkflowTerminalStructuralString(value, field, MAX_TERMINAL_GRAPH_BYTES, true);
  validateSource?.(source);
  state.bytes += Buffer.byteLength(source, 'utf8');
  if (state.bytes > MAX_TERMINAL_GRAPH_BYTES) throw new TypeError('serialized workflow graph exceeds byte limit');
  return hashFramedParts(domain, [source]);
}

function hashSource(domain: string, value: unknown, field: string, state: GraphState): string {
  return hashBoundedSource(domain, value, field, state);
}

function hashExecutableSource(domain: string, value: unknown, field: string, state: GraphState): string {
  return hashBoundedSource(domain, value, field, state, source => {
    if (source.length === 0) {
      throw new TypeError(`${field} must not be empty`);
    }
    if (isWorkflowTerminalNativeFunctionSource(source)) {
      throw new TypeError(`${field} must not contain native or bound callback source`);
    }
  });
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
    ['id', 'description', 'metadata', 'component', 'serializedStepFlow', 'mapConfig', 'generatedId', 'canSuspend'],
    ['id'],
    field,
  );
  const id = validateWorkflowTerminalStructuralString(descriptors.id!.value, `${field}.id`);
  if (scopeStepIds.has(id)) throw new TypeError(`serialized workflow graph contains duplicate step id ${id}`);
  scopeStepIds.add(id);
  const componentValue = descriptors.component?.value;
  const component =
    componentValue === undefined
      ? ''
      : validateWorkflowTerminalStructuralString(componentValue, `${field}.component`, 512, true);
  const mapConfigValue = descriptors.mapConfig?.value;
  const mapConfig =
    mapConfigValue === undefined
      ? ''
      : hashSource('mastra.workflow-terminal-parent-graph.map-config.v1', mapConfigValue, `${field}.mapConfig`, state);
  const generatedId = descriptors.generatedId?.value;
  if (generatedId !== undefined && typeof generatedId !== 'boolean') {
    throw new TypeError(`${field}.generatedId must be boolean`);
  }
  if (generatedId === true && (mapConfigValue === undefined || !id.startsWith('mapping_'))) {
    throw new TypeError(`${field}.generatedId is only valid for implicit mapping steps`);
  }
  // Implicit mapping steps are addressed by graph position and executable map
  // configuration. Their UUID is process-local construction noise and changes
  // when the same workflow is rebuilt after a restart.
  const fingerprintId = generatedId === true ? '@generated-mapping-step' : id;
  const canSuspend = descriptors.canSuspend?.value;
  if (canSuspend !== undefined && typeof canSuspend !== 'boolean') {
    throw new TypeError(`${field}.canSuspend must be boolean`);
  }
  append(state, parts, 'step', fingerprintId, component, mapConfig, canSuspend === undefined ? '' : String(canSuspend));
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
    hashExecutableSource(
      'mastra.workflow-terminal-parent-graph.condition-source.v1',
      descriptors.fn!.value,
      `${field}.fn`,
      state,
    ),
  );
  return id;
}

const MAX_TERMINAL_CANONICAL_DEPTH = 32;
const MAX_TERMINAL_CANONICAL_KEYS = 64;

function consumeCanonicalBytes(state: GraphState, token: string): string {
  state.bytes += Buffer.byteLength(token, 'utf8');
  if (state.bytes > MAX_TERMINAL_GRAPH_BYTES) throw new TypeError('serialized workflow graph exceeds byte limit');
  return token;
}

/**
 * Deterministic bounded serialization of declarative data (predicates,
 * structured-output JSON Schemas). Rejects everything that is not plain,
 * dense, finite data; sorts object keys so storage round-trips and
 * reconstruction cannot change the fingerprint. Byte-accounts every emitted
 * token into the shared graph state before materializing containers.
 */
function canonicalizeBoundedData(value: unknown, field: string, state: GraphState, depth: number): string {
  if (depth > MAX_TERMINAL_CANONICAL_DEPTH) throw new TypeError(`${field} exceeds canonical data depth limit`);
  if (value === null) return consumeCanonicalBytes(state, 'null');
  if (typeof value === 'boolean') return consumeCanonicalBytes(state, value ? 'true' : 'false');
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) throw new TypeError(`${field} must be a finite number`);
    return consumeCanonicalBytes(state, String(value));
  }
  if (typeof value === 'string') {
    validateWorkflowTerminalStructuralString(value, field, MAX_TERMINAL_GRAPH_BYTES, true);
    return consumeCanonicalBytes(state, JSON.stringify(value));
  }
  if (Array.isArray(value)) {
    const items = getDenseArray(value, field, MAX_TERMINAL_GRAPH_NODES);
    const canonical = items.map((item, index) => canonicalizeBoundedData(item, `${field}[${index}]`, state, depth + 1));
    return `[${canonical.join(',')}]`;
  }
  const descriptors = getPlainDataDescriptors(value, {
    allowNullPrototype: true,
    typeError: `${field} must be plain data`,
    proxyError: `${field} must not be a proxy`,
    prototypeError: `${field} must be a plain object`,
    fieldsError: `${field} contains symbol, accessor, or non-enumerable fields`,
    maxKeys: MAX_TERMINAL_CANONICAL_KEYS,
    maxKeysError: `${field} contains too many fields`,
  });
  const keys = Object.keys(descriptors).sort();
  const canonical = keys.map(key => {
    validateWorkflowTerminalStructuralString(key, `${field} key`, MAX_TERMINAL_ID_LENGTH, false);
    const keyToken = consumeCanonicalBytes(state, JSON.stringify(key));
    return `${keyToken}:${canonicalizeBoundedData(descriptors[key]!.value, `${field}.${key}`, state, depth + 1)}`;
  });
  return `{${canonical.join(',')}}`;
}

function hashCanonicalData(domain: string, value: unknown, field: string, state: GraphState): string {
  return hashFramedParts(domain, [canonicalizeBoundedData(value, field, state, 0)]);
}

function normalizeSerializedStepOptions(value: unknown, field: string, state: GraphState): string {
  if (value === undefined) return '';
  const descriptors = getDataDescriptors(value, field);
  validateKeys(descriptors, ['retries', 'metadata', ...SERIALIZED_AGENT_PASSTHROUGH_OPTION_KEYS], [], field);
  const retries = descriptors.retries?.value;
  let normalized = '';
  if (retries !== undefined) {
    if (!Number.isSafeInteger(retries) || retries < 0) {
      throw new TypeError(`${field}.retries must be a non-negative safe integer`);
    }
    normalized = String(retries);
  }
  // Serialized agent execution passthrough options (maxSteps, toolChoice,
  // activeTools, modelSettings, …) change run behavior, so they participate in
  // the fingerprint. Entries without them emit the legacy retries-only token,
  // keeping retained pre-upgrade fingerprints stable — stored graphs could not
  // carry these keys before they entered the serialized subset.
  const passthrough: Record<string, unknown> = {};
  for (const key of SERIALIZED_AGENT_PASSTHROUGH_OPTION_KEYS) {
    const descriptor = descriptors[key];
    if (descriptor !== undefined && descriptor.value !== undefined) passthrough[key] = descriptor.value;
  }
  if (Object.keys(passthrough).length > 0) {
    normalized += `#${hashCanonicalData(
      'mastra.workflow-terminal-parent-graph.step-options.v1',
      passthrough,
      `${field}`,
      state,
    )}`;
  }
  return normalized;
}

/**
 * Normalizes one {@link SerializedSingleStepEntry} (or a legacy bare
 * {@link SerializedStep}, which pre-#20471 graphs stored for loop/foreach
 * bodies). Implicit `mapping` entries intentionally emit the exact part
 * sequence their legacy `type: 'step'` serialization produced, so retained
 * pre-upgrade fingerprints keep matching semantically unchanged graphs.
 */
function normalizeSingleStepEntry(
  value: unknown,
  parts: string[],
  state: GraphState,
  scopeStepIds: Set<string>,
  depth: number,
  field: string,
): string {
  const descriptors = getDataDescriptors(value, field);
  if (descriptors.type === undefined) {
    return normalizeStep(value, parts, state, scopeStepIds, depth, field);
  }
  const type = descriptors.type.value;
  if (type === 'step') {
    validateKeys(descriptors, ['type', 'step'], ['type', 'step'], field);
    return normalizeStep(descriptors.step!.value, parts, state, scopeStepIds, depth, `${field}.step`);
  }
  consumeNode(state);
  if (type === 'mapping') {
    validateKeys(descriptors, ['type', 'id', 'generatedId', 'mapConfig'], ['type', 'id', 'mapConfig'], field);
    const id = validateWorkflowTerminalStructuralString(descriptors.id!.value, `${field}.id`);
    if (scopeStepIds.has(id)) throw new TypeError(`serialized workflow graph contains duplicate step id ${id}`);
    scopeStepIds.add(id);
    const generatedId = descriptors.generatedId?.value;
    if (generatedId !== undefined && typeof generatedId !== 'boolean') {
      throw new TypeError(`${field}.generatedId must be boolean`);
    }
    if (generatedId === true && !id.startsWith('mapping_')) {
      throw new TypeError(`${field}.generatedId is only valid for implicit mapping steps`);
    }
    const fingerprintId = generatedId === true ? '@generated-mapping-step' : id;
    const mapConfig = hashSource(
      'mastra.workflow-terminal-parent-graph.map-config.v1',
      descriptors.mapConfig!.value,
      `${field}.mapConfig`,
      state,
    );
    append(state, parts, 'step', fingerprintId, '', mapConfig, '');
    append(state, parts, 'nested', '0');
    return id;
  }
  if (type === 'agent' || type === 'tool') {
    const refKey = type === 'agent' ? 'agentId' : 'toolId';
    validateKeys(
      descriptors,
      type === 'agent'
        ? ['type', 'id', 'agentId', 'description', 'outputSchema', 'options']
        : ['type', 'id', 'toolId', 'description', 'options'],
      ['type', 'id', refKey],
      field,
    );
    const id = validateWorkflowTerminalStructuralString(descriptors.id!.value, `${field}.id`);
    if (scopeStepIds.has(id)) throw new TypeError(`serialized workflow graph contains duplicate step id ${id}`);
    scopeStepIds.add(id);
    const ref = validateWorkflowTerminalStructuralString(descriptors[refKey]!.value, `${field}.${refKey}`);
    const options = normalizeSerializedStepOptions(descriptors.options?.value, `${field}.options`, state);
    if (type === 'agent') {
      const outputSchemaValue = descriptors.outputSchema?.value;
      const outputSchema =
        outputSchemaValue === undefined
          ? ''
          : hashCanonicalData(
              'mastra.workflow-terminal-parent-graph.agent-output-schema.v1',
              outputSchemaValue,
              `${field}.outputSchema`,
              state,
            );
      append(state, parts, 'agent', id, ref, outputSchema, options);
    } else {
      append(state, parts, 'tool', id, ref, options);
    }
    return id;
  }
  if (type === 'workflow') {
    validateKeys(
      descriptors,
      ['type', 'id', 'workflowId', 'description', 'serializedStepFlow'],
      ['type', 'id', 'workflowId'],
      field,
    );
    const id = validateWorkflowTerminalStructuralString(descriptors.id!.value, `${field}.id`);
    if (scopeStepIds.has(id)) throw new TypeError(`serialized workflow graph contains duplicate step id ${id}`);
    scopeStepIds.add(id);
    const workflowId = validateWorkflowTerminalStructuralString(descriptors.workflowId!.value, `${field}.workflowId`);
    append(state, parts, 'workflow', id, workflowId);
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
  throw new TypeError(`${field}.type is invalid`);
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
    if (
      ![
        'step',
        'agent',
        'tool',
        'mapping',
        'workflow',
        'sleep',
        'sleepUntil',
        'parallel',
        'conditional',
        'loop',
        'foreach',
      ].includes(type as string)
    ) {
      throw new TypeError(`${entryField}.type is invalid`);
    }
    // Implicit `.map()` steps used to serialize as `type: 'step'`; frame the
    // dedicated `mapping` variant identically so unchanged graphs keep their
    // pre-upgrade fingerprint.
    append(state, parts, 'entry', String(index), type === 'mapping' ? 'step' : (type as string));
    if (type === 'step' || type === 'agent' || type === 'tool' || type === 'mapping' || type === 'workflow') {
      normalizeSingleStepEntry(entries[index], parts, state, scopeStepIds, depth, entryField);
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
          if (isProxy(date)) throw new TypeError(`${entryField}.date must not be a proxy`);
          if (!isDate(date) && typeof date !== 'string') {
            throw new TypeError(`${entryField}.date must be a Date or string`);
          }
          const epoch = isDate(date) ? Date.prototype.getTime.call(date) : Date.parse(date);
          if (!Number.isFinite(epoch)) throw new TypeError(`${entryField}.date must be valid`);
          if (typeof date === 'string' && new Date(epoch).toISOString() !== date) {
            throw new TypeError(`${entryField}.date must be a canonical ISO timestamp`);
          }
          timing = String(epoch);
        }
      }
      const fn =
        descriptors.fn?.value === undefined
          ? ''
          : hashExecutableSource(
              'mastra.workflow-terminal-parent-graph.sleep-source.v1',
              descriptors.fn.value,
              `${entryField}.fn`,
              state,
            );
      append(state, parts, id, timing, fn);
    } else if (type === 'parallel' || type === 'conditional') {
      validateKeys(
        descriptors,
        type === 'conditional' ? ['type', 'steps', 'serializedConditions', 'predicates'] : ['type', 'steps'],
        type === 'conditional' ? ['type', 'steps', 'serializedConditions'] : ['type', 'steps'],
        entryField,
      );
      const steps = getDenseArray(descriptors.steps!.value, `${entryField}.steps`);
      append(state, parts, 'branches', String(steps.length));
      const branchIds = steps.map((branch, branchIndex) =>
        normalizeSingleStepEntry(branch, parts, state, scopeStepIds, depth, `${entryField}.steps[${branchIndex}]`),
      );
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
          if (conditionId !== `${branchIds[conditionIndex]}-condition`) {
            throw new TypeError(`${entryField} conditional branch and condition IDs differ`);
          }
        });
        // Declarative predicates are execution-semantic: they are rebuilt into
        // live conditions on rehydration. Absent predicates append nothing so
        // closure-based graphs keep their pre-upgrade fingerprint.
        const predicatesValue = descriptors.predicates?.value;
        if (predicatesValue !== undefined) {
          const predicates = getDenseArray(predicatesValue, `${entryField}.predicates`);
          if (predicates.length !== branchIds.length) {
            throw new TypeError(`${entryField} conditional branch and predicate counts differ`);
          }
          append(state, parts, 'predicates', String(predicates.length));
          predicates.forEach((predicate, predicateIndex) => {
            append(
              state,
              parts,
              predicate === null
                ? 'null'
                : hashCanonicalData(
                    'mastra.workflow-terminal-parent-graph.predicate.v1',
                    predicate,
                    `${entryField}.predicates[${predicateIndex}]`,
                    state,
                  ),
            );
          });
        }
      }
    } else if (type === 'loop') {
      validateKeys(
        descriptors,
        ['type', 'step', 'serializedCondition', 'loopType', 'predicate'],
        ['type', 'step', 'serializedCondition', 'loopType'],
        entryField,
      );
      if (descriptors.loopType!.value !== 'dowhile' && descriptors.loopType!.value !== 'dountil') {
        throw new TypeError(`${entryField}.loopType is invalid`);
      }
      append(state, parts, descriptors.loopType!.value);
      const stepId = normalizeSingleStepEntry(
        descriptors.step!.value,
        parts,
        state,
        scopeStepIds,
        depth,
        `${entryField}.step`,
      );
      const conditionId = normalizeCondition(
        descriptors.serializedCondition!.value,
        parts,
        state,
        `${entryField}.serializedCondition`,
      );
      if (conditionId !== `${stepId}-condition`) {
        throw new TypeError(`${entryField} loop step and condition IDs differ`);
      }
      const predicateValue = descriptors.predicate?.value;
      if (predicateValue !== undefined) {
        append(
          state,
          parts,
          'predicate',
          hashCanonicalData(
            'mastra.workflow-terminal-parent-graph.predicate.v1',
            predicateValue,
            `${entryField}.predicate`,
            state,
          ),
        );
      }
    } else if (type === 'foreach') {
      validateKeys(descriptors, ['type', 'step', 'opts'], ['type', 'step'], entryField);
      normalizeSingleStepEntry(descriptors.step!.value, parts, state, scopeStepIds, depth, `${entryField}.step`);
      const optsValue = descriptors.opts?.value;
      if (optsValue === undefined) {
        // Omitted opts execute with the engine default `concurrency: 1`; frame
        // them identically to an explicit `{ concurrency: 1 }` so unchanged
        // graphs keep their pre-upgrade fingerprint.
        append(state, parts, '1');
      } else {
        const opts = getDataDescriptors(optsValue, `${entryField}.opts`);
        validateKeys(opts, ['concurrency', 'fn'], [], `${entryField}.opts`);
        const concurrency = opts.concurrency?.value;
        const fn = opts.fn?.value;
        if (concurrency !== undefined && fn !== undefined) {
          throw new TypeError(`${entryField}.opts must not contain both concurrency and fn`);
        }
        if (fn !== undefined) {
          append(
            state,
            parts,
            `fn:${hashExecutableSource(
              'mastra.workflow-terminal-parent-graph.foreach-concurrency-source.v1',
              fn,
              `${entryField}.opts.fn`,
              state,
            )}`,
          );
        } else {
          if (concurrency !== undefined && (!Number.isSafeInteger(concurrency) || concurrency < 1)) {
            throw new TypeError(`${entryField}.opts.concurrency must be a positive safe integer`);
          }
          append(state, parts, concurrency === undefined ? '1' : String(concurrency));
        }
      }
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

/** @internal Reads the step identity of any serialized single-step entry (or a legacy bare step). */
export function readWorkflowTerminalSingleEntryStepId(value: unknown, field: string): string {
  const descriptors = getDataDescriptors(value, field);
  const type = descriptors.type?.value;
  if (type === undefined) {
    return validateWorkflowTerminalStructuralString(descriptors.id?.value, `${field}.id`);
  }
  if (type === 'step') {
    return readStepId(descriptors.step?.value as SerializedStep, `${field}.step`);
  }
  if (type === 'agent' || type === 'tool' || type === 'mapping' || type === 'workflow') {
    return validateWorkflowTerminalStructuralString(descriptors.id?.value, `${field}.id`);
  }
  throw new TypeError(`${field}.type is invalid`);
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
  const entryField = `serialized workflow graph[${rootIndex}]`;
  const entryDescriptors = getDataDescriptors(entry, entryField);
  const type = entryDescriptors.type?.value;
  if (type === 'parallel' || type === 'conditional') {
    if (path.length === 1) return { kind: 'container', containerType: type };
    const steps = getDenseArray(entryDescriptors.steps?.value, `${entryField}.steps`);
    const branch = steps[path[1] as number];
    if (!branch) throw new TypeError('executionPath branch does not exist');
    return {
      kind: 'branch',
      containerType: type,
      stepId: readWorkflowTerminalSingleEntryStepId(branch, `${entryField}.steps[${path[1]}]`),
    };
  }
  if (type === 'foreach') {
    if (path.length > 2) throw new TypeError('foreach executionPath is invalid');
    return {
      kind: 'foreach',
      stepId: readWorkflowTerminalSingleEntryStepId(entryDescriptors.step?.value, `${entryField}.step`),
      ...(path.length === 2 ? { iterationIndex: path[1] as number } : {}),
    };
  }
  if (path.length !== 1) throw new TypeError('executionPath contains a surplus segment');
  if (type === 'step') {
    return { kind: 'step', stepId: readStepId(entryDescriptors.step?.value as SerializedStep, `${entryField}.step`) };
  }
  if (type === 'agent' || type === 'tool' || type === 'mapping' || type === 'workflow') {
    return {
      kind: 'step',
      stepId: validateWorkflowTerminalStructuralString(entryDescriptors.id?.value, `${entryField}.id`),
    };
  }
  if (type === 'loop') {
    const loopType = entryDescriptors.loopType?.value;
    if (loopType !== 'dowhile' && loopType !== 'dountil') {
      throw new TypeError(`${entryField}.loopType is invalid`);
    }
    return {
      kind: 'loop',
      stepId: readWorkflowTerminalSingleEntryStepId(entryDescriptors.step?.value, `${entryField}.step`),
      loopType,
    };
  }
  if (type !== 'sleep' && type !== 'sleepUntil') throw new TypeError(`${entryField}.type is invalid`);
  return {
    kind: type,
    entryId: validateWorkflowTerminalStructuralString(entryDescriptors.id?.value, `${entryField}.id`),
  };
}
