import { isProxy } from 'node:util/types';
import type {
  SerializedStepFlowEntry,
  WorkflowRunState,
  WorkflowRunStatus,
  WorkflowTerminalEffectRecord,
} from '../types';
import { getDenseDataArray, getPlainDataDescriptors } from './data-shape';
import { validateWorkflowTerminalEffectIntegrity } from './effect-integrity';
import {
  MAX_TERMINAL_LOOP_ITERATIONS,
  MAX_TERMINAL_PATH_LENGTH,
  createWorkflowTerminalGraphFingerprint,
  resolveWorkflowTerminalGraphCoordinate,
} from './graph-fingerprint';
import { WORKFLOW_TERMINAL_FOREACH_RUN_KEY, WORKFLOW_TERMINAL_FOREACH_STATE_KEY } from './types';

const PARENT_STATUSES = new Set<WorkflowRunStatus>([
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
const CHILD_STATUSES: ReadonlySet<unknown> = new Set(['success', 'failed', 'canceled']);
const MAX_PLANNER_COLLECTION_ITEMS = 100_000;
const MAX_PLANNER_MAP_KEYS = 100_000;

function dataRecord(
  value: unknown,
  field: string,
  selectedKeys?: ReadonlySet<string>,
): Record<string, PropertyDescriptor> {
  return getPlainDataDescriptors(value, {
    allowNullPrototype: true,
    typeError: `${field} must be a plain data object`,
    fieldsError: selectedKeys
      ? key => `${field} contains an accessor field ${String(key)}`
      : `${field} contains symbol or accessor fields`,
    selectedKeys,
    maxKeys: selectedKeys ? undefined : MAX_PLANNER_MAP_KEYS,
    maxKeysError: `${field} exceeds the planner map-key limit`,
  });
}

function defineDataProperty(target: Record<string, unknown>, key: string, value: unknown): void {
  Object.defineProperty(target, key, {
    configurable: true,
    enumerable: true,
    value,
    writable: true,
  });
}

function exactKeys(
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

function read(descriptors: Record<string, PropertyDescriptor>, key: string): unknown {
  return descriptors[key]?.value;
}

export function canonicalPlannerInteger(value: unknown, field: string, max = Number.MAX_SAFE_INTEGER): number {
  if (!Number.isSafeInteger(value) || (value as number) < 0 || (value as number) > max) {
    throw new TypeError(`${field} must be a non-negative safe integer`);
  }
  return value === 0 ? 0 : (value as number);
}

export function canonicalPlannerPath(value: unknown, field: string): number[] {
  const entries = getDenseDataArray(value, {
    typeError: `${field} must be a dense path`,
    lengthError: `${field} has an invalid length`,
    dataError: `${field} must be dense and data-only`,
    minLength: 1,
    maxLength: MAX_TERMINAL_PATH_LENGTH,
  });
  return entries.map((entry, index) => canonicalPlannerInteger(entry, `${field}[${index}]`));
}

export function canonicalPlannerStructuralString(value: unknown, field: string, maxLength: number): string {
  const isWellFormed = (input: string) => {
    for (let index = 0; index < input.length; index += 1) {
      const code = input.charCodeAt(index);
      if (code >= 0xd800 && code <= 0xdbff) {
        const next = input.charCodeAt(index + 1);
        if (!(next >= 0xdc00 && next <= 0xdfff)) return false;
        index += 1;
      } else if (code >= 0xdc00 && code <= 0xdfff) {
        return false;
      }
    }
    return true;
  };
  if (
    typeof value !== 'string' ||
    value.length === 0 ||
    value.length > maxLength ||
    value.includes('\0') ||
    !isWellFormed(value)
  ) {
    throw new TypeError(`${field} must be a well-formed bounded string`);
  }
  return value;
}

function materializeEffect(
  value: unknown,
): Extract<WorkflowTerminalEffectRecord, { kind: 'parent-workflow-step-end' }> {
  const descriptors = dataRecord(value, 'effect');
  const keys = [
    'version',
    'effectKey',
    'kind',
    'workflowName',
    'runId',
    'sourceEventKey',
    'terminalStatus',
    'recoveryEnvelopeHash',
    'retainedRecordHash',
    'resourceId',
    'parentWorkflowName',
    'parentRunId',
    'parentStepId',
    'parentExecutionPath',
    'payloadHash',
    'createdAt',
  ];
  exactKeys(
    descriptors,
    keys,
    keys.filter(key => key !== 'resourceId'),
    'effect',
  );
  const terminalStatus = read(descriptors, 'terminalStatus');
  if (read(descriptors, 'version') !== 1 || read(descriptors, 'kind') !== 'parent-workflow-step-end') {
    throw new TypeError('effect must be a version 1 parent-workflow-step-end effect');
  }
  if (!CHILD_STATUSES.has(terminalStatus)) throw new TypeError('effect terminalStatus is invalid');
  const effectKey = canonicalPlannerStructuralString(read(descriptors, 'effectKey'), 'effect.effectKey', 256);
  const payloadHash = canonicalPlannerStructuralString(read(descriptors, 'payloadHash'), 'effect.payloadHash', 256);
  const recoveryEnvelopeHash = canonicalPlannerStructuralString(
    read(descriptors, 'recoveryEnvelopeHash'),
    'effect.recoveryEnvelopeHash',
    256,
  );
  const retainedRecordHash = canonicalPlannerStructuralString(
    read(descriptors, 'retainedRecordHash'),
    'effect.retainedRecordHash',
    256,
  );
  if (
    !/^wte:v1:[a-f0-9]{64}$/.test(effectKey) ||
    !/^sha256:[a-f0-9]{64}$/.test(payloadHash) ||
    !/^sha256:[a-f0-9]{64}$/.test(recoveryEnvelopeHash) ||
    !/^sha256:[a-f0-9]{64}$/.test(retainedRecordHash)
  ) {
    throw new TypeError('effect hashes are invalid');
  }
  return {
    version: 1,
    effectKey,
    kind: 'parent-workflow-step-end',
    workflowName: canonicalPlannerStructuralString(read(descriptors, 'workflowName'), 'effect.workflowName', 512),
    runId: canonicalPlannerStructuralString(read(descriptors, 'runId'), 'effect.runId', 512),
    sourceEventKey: canonicalPlannerStructuralString(
      read(descriptors, 'sourceEventKey'),
      'effect.sourceEventKey',
      1024,
    ),
    terminalStatus: terminalStatus as 'success' | 'failed' | 'canceled',
    recoveryEnvelopeHash: recoveryEnvelopeHash as `sha256:${string}`,
    retainedRecordHash: retainedRecordHash as `sha256:${string}`,
    ...(Object.prototype.hasOwnProperty.call(descriptors, 'resourceId')
      ? { resourceId: canonicalPlannerStructuralString(read(descriptors, 'resourceId'), 'effect.resourceId', 512) }
      : {}),
    parentWorkflowName: canonicalPlannerStructuralString(
      read(descriptors, 'parentWorkflowName'),
      'effect.parentWorkflowName',
      512,
    ),
    parentRunId: canonicalPlannerStructuralString(read(descriptors, 'parentRunId'), 'effect.parentRunId', 512),
    parentStepId: canonicalPlannerStructuralString(read(descriptors, 'parentStepId'), 'effect.parentStepId', 512),
    parentExecutionPath: canonicalPlannerPath(read(descriptors, 'parentExecutionPath'), 'effect.parentExecutionPath'),
    payloadHash,
    createdAt: canonicalPlannerInteger(read(descriptors, 'createdAt'), 'effect.createdAt'),
  };
}

function denseArray(value: unknown, field: string): unknown[] {
  return getDenseDataArray(value, {
    typeError: `${field} must be an array`,
    lengthError: `${field} exceeds the planner collection-item limit`,
    dataError: `${field} must be dense and data-only`,
    maxLength: MAX_PLANNER_COLLECTION_ITEMS,
  });
}

function copySidecarMap(value: unknown, field: string): Record<string, unknown> | undefined {
  if (value === undefined) return undefined;
  const descriptors = dataRecord(value, field);
  const result: Record<string, unknown> = {};
  for (const [key, descriptor] of Object.entries(descriptors)) {
    defineDataProperty(result, key, typeof descriptor.value === 'string' ? descriptor.value : null);
  }
  return result;
}

function materializeMetadata(
  value: unknown,
  field: string,
  options: { nestedRunId: boolean; iterationCount: boolean; foreach: boolean },
): Record<string, unknown> | undefined {
  if (value === undefined) return undefined;
  const selected = new Set<string>();
  if (options.nestedRunId) selected.add('nestedRunId');
  if (options.iterationCount) selected.add('iterationCount');
  if (options.foreach) selected.add('__workflow_meta');
  const descriptors = dataRecord(value, field, selected);
  const result: Record<string, unknown> = {};
  if (descriptors.nestedRunId) {
    result.nestedRunId =
      typeof descriptors.nestedRunId.value === 'string'
        ? canonicalPlannerStructuralString(descriptors.nestedRunId.value, `${field}.nestedRunId`, 512)
        : null;
  }
  if (descriptors.iterationCount) {
    result.iterationCount = canonicalPlannerInteger(
      descriptors.iterationCount.value,
      `${field}.iterationCount`,
      MAX_TERMINAL_LOOP_ITERATIONS,
    );
  }
  if (descriptors.__workflow_meta) {
    const workflowDescriptors = dataRecord(
      descriptors.__workflow_meta.value,
      `${field}.__workflow_meta`,
      new Set([WORKFLOW_TERMINAL_FOREACH_RUN_KEY, WORKFLOW_TERMINAL_FOREACH_STATE_KEY]),
    );
    const workflowMeta: Record<string, unknown> = {};
    const iterationRuns = copySidecarMap(
      read(workflowDescriptors, WORKFLOW_TERMINAL_FOREACH_RUN_KEY),
      `${field}.__workflow_meta.${WORKFLOW_TERMINAL_FOREACH_RUN_KEY}`,
    );
    const iterationStates = copySidecarMap(
      read(workflowDescriptors, WORKFLOW_TERMINAL_FOREACH_STATE_KEY),
      `${field}.__workflow_meta.${WORKFLOW_TERMINAL_FOREACH_STATE_KEY}`,
    );
    if (iterationRuns) defineDataProperty(workflowMeta, WORKFLOW_TERMINAL_FOREACH_RUN_KEY, iterationRuns);
    if (iterationStates) defineDataProperty(workflowMeta, WORKFLOW_TERMINAL_FOREACH_STATE_KEY, iterationStates);
    result.__workflow_meta = workflowMeta;
  }
  return result;
}

function materializeOutput(value: unknown, field: string): unknown[] | undefined {
  if (value === undefined) return undefined;
  return denseArray(value, field).map((item, index) => {
    if (item === null) return null;
    if (typeof item !== 'object') return false;
    if (isProxy(item)) return {};
    const prototype = Object.getPrototypeOf(item);
    if (prototype !== Object.prototype && prototype !== null) return {};
    const descriptors = dataRecord(item, `${field}[${index}]`, new Set(['status']));
    const status = read(descriptors, 'status');
    return status === undefined ? {} : { status };
  });
}

function contextProjection(
  graph: SerializedStepFlowEntry[],
  effect: Extract<WorkflowTerminalEffectRecord, { kind: 'parent-workflow-step-end' }>,
): { ids: Set<string>; sourceKind: 'step' | 'branch' | 'loop' | 'foreach' | 'unknown' } {
  const ids = new Set([effect.parentStepId]);
  try {
    const resolved = resolveWorkflowTerminalGraphCoordinate(graph, effect.parentExecutionPath);
    if (resolved.kind === 'branch') {
      const entry = graph[effect.parentExecutionPath[0]!];
      if (entry?.type === 'parallel' || entry?.type === 'conditional') {
        for (const branch of entry.steps) ids.add(branch.step.id);
      }
      return { ids, sourceKind: 'branch' };
    }
    if (resolved.kind === 'loop') return { ids, sourceKind: 'loop' };
    if (resolved.kind === 'foreach') return { ids, sourceKind: 'foreach' };
    if (resolved.kind === 'step') return { ids, sourceKind: 'step' };
  } catch {
    // A fingerprintable coordinate mismatch becomes graph-conflict in the planner.
  }
  return { ids, sourceKind: 'unknown' };
}

function materializeContext(
  value: unknown,
  graph: SerializedStepFlowEntry[],
  effect: Extract<WorkflowTerminalEffectRecord, { kind: 'parent-workflow-step-end' }>,
): WorkflowRunState['context'] {
  const projection = contextProjection(graph, effect);
  const descriptors = dataRecord(value, 'parentSnapshot.context', projection.ids);
  const context: WorkflowRunState['context'] = {};
  for (const [stepId, descriptor] of Object.entries(descriptors)) {
    const isSource = stepId === effect.parentStepId;
    const selected = new Set(['status']);
    if (isSource) selected.add('metadata');
    if (isSource && projection.sourceKind === 'foreach') {
      selected.add('payload');
      selected.add('output');
    }
    const resultDescriptors = dataRecord(descriptor.value, `parentSnapshot.context.${stepId}`, selected);
    const status = read(resultDescriptors, 'status');
    const payload = read(resultDescriptors, 'payload');
    const output = materializeOutput(read(resultDescriptors, 'output'), `parentSnapshot.context.${stepId}.output`);
    const result = {
      ...(status === undefined ? {} : { status }),
      ...(Array.isArray(payload)
        ? { payload: denseArray(payload, `parentSnapshot.context.${stepId}.payload`).map(() => null) }
        : {}),
      ...(output === undefined ? {} : { output }),
      ...(resultDescriptors.metadata
        ? {
            metadata: materializeMetadata(
              resultDescriptors.metadata.value,
              `parentSnapshot.context.${stepId}.metadata`,
              {
                nestedRunId: true,
                iterationCount: projection.sourceKind === 'loop',
                foreach: projection.sourceKind === 'foreach',
              },
            ),
          }
        : {}),
    } as WorkflowRunState['context'][string];
    defineDataProperty(context, stepId, result);
  }
  return context;
}

function materializeActiveSteps(value: unknown): Record<string, number[]> {
  const descriptors = dataRecord(value, 'parentSnapshot.activeStepsPath');
  const result: Record<string, number[]> = {};
  for (const [stepId, descriptor] of Object.entries(descriptors)) {
    defineDataProperty(
      result,
      stepId,
      canonicalPlannerPath(descriptor.value, `parentSnapshot.activeStepsPath.${stepId}`),
    );
  }
  return result;
}

export interface WorkflowTerminalParentPlanningView {
  version: 1;
  effect: Extract<WorkflowTerminalEffectRecord, { kind: 'parent-workflow-step-end' }>;
  graphFingerprint: `sha256:${string}`;
  parentRevision: string;
  parentSnapshot: WorkflowRunState;
  evaluatedDecision?: unknown;
}

/** @internal Materializes a payload-free structural view without invoking accessors or user callbacks. */
export function materializeWorkflowTerminalParentPlanningView(input: unknown): WorkflowTerminalParentPlanningView {
  const descriptors = dataRecord(input, 'planner input');
  exactKeys(
    descriptors,
    ['version', 'effect', 'parentRevision', 'parentSnapshot', 'evaluatedDecision'],
    ['version', 'effect', 'parentRevision', 'parentSnapshot'],
    'planner input',
  );
  if (read(descriptors, 'version') !== 1) throw new TypeError('planner input version must be 1');
  const rawEffect = read(descriptors, 'effect');
  validateWorkflowTerminalEffectIntegrity(rawEffect);
  const effect = materializeEffect(rawEffect);
  const snapshotDescriptors = dataRecord(
    read(descriptors, 'parentSnapshot'),
    'parentSnapshot',
    new Set(['runId', 'status', 'serializedStepGraph', 'context', 'activeStepsPath']),
  );
  const status = read(snapshotDescriptors, 'status');
  if (!PARENT_STATUSES.has(status as WorkflowRunStatus)) throw new TypeError('parentSnapshot.status is invalid');
  const graph = read(snapshotDescriptors, 'serializedStepGraph');
  if (!Array.isArray(graph)) throw new TypeError('parentSnapshot.serializedStepGraph must be an array');
  const graphFingerprint = createWorkflowTerminalGraphFingerprint(graph);
  const parentSnapshot = {
    runId: canonicalPlannerStructuralString(read(snapshotDescriptors, 'runId'), 'parentSnapshot.runId', 512),
    status: status as WorkflowRunStatus,
    serializedStepGraph: graph,
    context: materializeContext(read(snapshotDescriptors, 'context'), graph, effect),
    activeStepsPath: materializeActiveSteps(read(snapshotDescriptors, 'activeStepsPath')),
    activePaths: [],
    suspendedPaths: {},
    resumeLabels: {},
    value: {},
    waitingPaths: {},
    timestamp: 0,
  } as WorkflowRunState;
  return {
    version: 1,
    effect,
    graphFingerprint,
    parentRevision: canonicalPlannerStructuralString(read(descriptors, 'parentRevision'), 'parentRevision', 256),
    parentSnapshot,
    ...(descriptors.evaluatedDecision ? { evaluatedDecision: descriptors.evaluatedDecision.value } : {}),
  };
}
