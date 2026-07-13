import { createHash } from 'node:crypto';
import type { SerializedStepFlowEntry, WorkflowRunState } from '../types';
import {
  createWorkflowTerminalParentContinuationContract,
  validateWorkflowTerminalParentContinuationBinding,
} from './contract';
import { getPlainDataDescriptors } from './data-shape';
import {
  MAX_TERMINAL_LOOP_ITERATIONS,
  resolveWorkflowTerminalGraphCoordinate,
  validateWorkflowTerminalStructuralString,
} from './graph-fingerprint';
import type {
  WorkflowTerminalEvaluatedLoopDecisionV1,
  WorkflowTerminalLoopDecisionRequestV1,
  WorkflowTerminalParentPlannerInputV1,
  WorkflowTerminalParentPlannerResult,
} from './planner-types';
import type { WorkflowTerminalParentPlanningView } from './planning-view';
import { canonicalPlannerInteger, materializeWorkflowTerminalParentPlanningView } from './planning-view';
import type {
  WorkflowTerminalContinuationAction,
  WorkflowTerminalParentContinuationSpec,
  WorkflowTerminalParentPatch,
  WorkflowTerminalResultCoordinate,
  WorkflowTerminalRunTarget,
  WorkflowTerminalSha256,
} from './types';
import { WORKFLOW_TERMINAL_FOREACH_RUN_KEY, WORKFLOW_TERMINAL_FOREACH_STATE_KEY } from './types';

const FINAL_PARENT_STATUSES = new Set(['success', 'failed', 'canceled', 'tripwire', 'bailed']);
const ACTIVE_PARENT_STATUSES = new Set(['running', 'waiting', 'suspended']);
const BRANCH_STATUSES: ReadonlySet<unknown> = new Set([
  'running',
  'waiting',
  'success',
  'failed',
  'canceled',
  'suspended',
  'skipped',
]);
const FOREACH_TERMINAL_STATES: ReadonlySet<unknown> = new Set(['success', 'failed', 'canceled']);

const preservePatch = (): Extract<WorkflowTerminalParentPatch, { kind: 'merge-child-terminal' }> => ({
  kind: 'merge-child-terminal',
  resultWrite: 'source-coordinate',
  resultSource: 'retained-child-terminal-envelope',
  payloadWrite: 'preserve-parent-step-payload',
  metadataWrite: 'merge-child-and-bind-nested-run-id',
  stateWrite: 'replace-context-__state-from-retained-child',
  requestContextWrite: 'merge-from-retained-child',
  activeStepsWrite: 'derive-from-source-coordinate',
  snapshotTimestampWrite: 'storage-clock',
  parentRunWrite: { kind: 'preserve' },
  loopWrite: { kind: 'preserve' },
});

function terminalPatch(status: 'failed' | 'success' | 'canceled'): WorkflowTerminalParentPatch {
  return {
    ...preservePatch(),
    parentRunWrite: {
      kind: 'set',
      status,
      resultSource: 'source-coordinate',
      activePathSource: 'source-coordinate',
    },
  } as WorkflowTerminalParentPatch;
}

function suspendPatch(): WorkflowTerminalParentPatch {
  return {
    ...preservePatch(),
    parentRunWrite: {
      kind: 'set-suspended',
      resultSource: 'aggregate-container',
      activePathSource: 'source-coordinate',
      suspendedPathsSource: 'aggregate-container',
      resumeLabelsSource: 'aggregate-container',
    },
  } as WorkflowTerminalParentPatch;
}

function loopPatch(stepId: string, iterationCount: number): WorkflowTerminalParentPatch {
  return {
    ...preservePatch(),
    loopWrite: { kind: 'set-iteration', stepId, iterationCount },
  } as WorkflowTerminalParentPatch;
}

function framedHash(domain: string, parts: readonly string[]): WorkflowTerminalSha256 {
  const hash = createHash('sha256');
  for (const part of [domain, ...parts]) {
    const bytes = Buffer.from(part, 'utf8');
    hash.update(String(bytes.length));
    hash.update(':');
    hash.update(bytes);
  }
  return `sha256:${hash.digest('hex')}`;
}

function samePath(left: readonly number[], right: readonly number[]): boolean {
  return left.length === right.length && left.every((entry, index) => entry === right[index]);
}

function validIndexedSidecar(
  value: unknown,
  upperBound: number,
  validValue: (entry: unknown) => boolean,
): value is Record<string, unknown> {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) return false;
  return Object.entries(value).every(
    ([key, entry]) => /^(0|[1-9][0-9]*)$/.test(key) && Number(key) < upperBound && validValue(entry),
  );
}

function validRunId(value: unknown): boolean {
  try {
    validateWorkflowTerminalStructuralString(value, 'foreach iteration run ID');
    return true;
  } catch {
    return false;
  }
}

interface ForeachPlanningState {
  current: Record<string, any>;
  iterationRuns: Record<string, unknown>;
  states: Record<string, unknown>;
}

function foreachPlanningState(
  view: WorkflowTerminalParentPlanningView,
  source: Extract<WorkflowTerminalResultCoordinate, { kind: 'foreach-iteration' }>,
): ForeachPlanningState | undefined {
  const current = contextEntry(view.parentSnapshot, source.stepId);
  if (!Array.isArray(current?.payload) || !Array.isArray(current.output)) return undefined;
  if (current.output.length > current.payload.length) return undefined;
  const workflowMeta = current.metadata?.__workflow_meta as Record<string, unknown> | undefined;
  const iterationRuns = workflowMeta?.[WORKFLOW_TERMINAL_FOREACH_RUN_KEY] ?? {};
  const storedStates = workflowMeta?.[WORKFLOW_TERMINAL_FOREACH_STATE_KEY] ?? {};
  if (
    !validIndexedSidecar(iterationRuns, current.output.length, validRunId) ||
    !validIndexedSidecar(storedStates, current.output.length, value => FOREACH_TERMINAL_STATES.has(value))
  ) {
    return undefined;
  }
  const runValues = Object.values(iterationRuns);
  if (new Set(runValues).size !== runValues.length) return undefined;
  return {
    current,
    iterationRuns: iterationRuns as Record<string, unknown>,
    states: storedStates as Record<string, unknown>,
  };
}

function foreachTerminalPropagationIsBindable(
  view: WorkflowTerminalParentPlanningView,
  source: Extract<WorkflowTerminalResultCoordinate, { kind: 'foreach-iteration' }>,
): boolean {
  const current = contextEntry(view.parentSnapshot, source.stepId);
  if (!Array.isArray(current?.payload) || !Array.isArray(current.output)) return false;
  const workflowMeta = current.metadata?.__workflow_meta as Record<string, unknown> | undefined;
  return validIndexedSidecar(workflowMeta?.[WORKFLOW_TERMINAL_FOREACH_STATE_KEY] ?? {}, current.payload.length, value =>
    FOREACH_TERMINAL_STATES.has(value),
  );
}

function sourceForView(view: WorkflowTerminalParentPlanningView): {
  source: WorkflowTerminalResultCoordinate;
  graphConflict: boolean;
} {
  const { effect, parentSnapshot } = view;
  try {
    const resolved = resolveWorkflowTerminalGraphCoordinate(
      parentSnapshot.serializedStepGraph,
      effect.parentExecutionPath,
    );
    if (resolved.kind === 'foreach' && resolved.iterationIndex !== undefined) {
      return {
        source: {
          kind: 'foreach-iteration',
          stepId: effect.parentStepId,
          containerPath: [effect.parentExecutionPath[0]!],
          iterationIndex: effect.parentExecutionPath[1]!,
        },
        graphConflict: resolved.stepId !== effect.parentStepId,
      };
    }
    const validScalar =
      (resolved.kind === 'step' || resolved.kind === 'branch' || resolved.kind === 'loop') &&
      resolved.stepId === effect.parentStepId;
    return {
      source: { kind: 'step', stepId: effect.parentStepId, executionPath: [...effect.parentExecutionPath] },
      graphConflict: !validScalar,
    };
  } catch {
    return {
      source: { kind: 'step', stepId: effect.parentStepId, executionPath: [...effect.parentExecutionPath] },
      graphConflict: true,
    };
  }
}

function contractBase(
  view: WorkflowTerminalParentPlanningView,
  graphFingerprint: WorkflowTerminalSha256,
  source: WorkflowTerminalResultCoordinate,
) {
  return {
    version: 1 as const,
    terminalEffectKey: view.effect.effectKey,
    terminalEffectPayloadHash: view.effect.payloadHash as WorkflowTerminalSha256,
    executionMode: 'continuous' as const,
    expectedParentRevision: view.parentRevision,
    graphFingerprint,
    childTerminalStatus: view.effect.terminalStatus,
    observedParentStatus: view.parentSnapshot.status,
    source,
  };
}

function conflictDigest(
  reason: 'graph-conflict' | 'plan-conflict',
  view: WorkflowTerminalParentPlanningView,
  graphFingerprint: WorkflowTerminalSha256,
): WorkflowTerminalSha256 {
  return framedHash('mastra.workflow-terminal-parent-planner.conflict.v1', [
    reason,
    view.effect.effectKey,
    view.effect.payloadHash,
    view.parentRevision,
    graphFingerprint,
    view.parentSnapshot.status,
    view.effect.parentStepId,
    String(view.effect.parentExecutionPath.length),
    ...view.effect.parentExecutionPath.map(String),
  ]);
}

function quarantine(
  reason: 'graph-conflict' | 'plan-conflict',
  view: WorkflowTerminalParentPlanningView,
  graphFingerprint: WorkflowTerminalSha256,
  source: WorkflowTerminalResultCoordinate,
): WorkflowTerminalParentPlannerResult {
  const contract = createWorkflowTerminalParentContinuationContract({
    ...contractBase(view, graphFingerprint, source),
    action: { kind: 'quarantine', reason, conflictDigest: conflictDigest(reason, view, graphFingerprint) },
    patch: { kind: 'none' },
  });
  validateWorkflowTerminalParentContinuationBinding(contract, {
    effect: view.effect,
    parentRevision: view.parentRevision,
    parentWorkflowName: view.effect.parentWorkflowName,
    parentSnapshot: view.parentSnapshot,
    executionMode: 'continuous',
  });
  return contract;
}

function contextEntry(snapshot: WorkflowRunState, stepId: string): Record<string, any> | undefined {
  return snapshot.context?.[stepId] as Record<string, any> | undefined;
}

function sourceIsOwned(view: WorkflowTerminalParentPlanningView, source: WorkflowTerminalResultCoordinate): boolean {
  const current = contextEntry(view.parentSnapshot, source.stepId);
  if (source.kind === 'step') {
    return (
      current?.status === 'running' &&
      current.metadata?.nestedRunId === view.effect.runId &&
      samePath(view.parentSnapshot.activeStepsPath[source.stepId] ?? [], source.executionPath)
    );
  }
  if (!Array.isArray(current?.payload) || !Array.isArray(current.output)) return false;
  const workflowMeta = current.metadata?.__workflow_meta as Record<string, unknown> | undefined;
  const iterationRuns = workflowMeta?.[WORKFLOW_TERMINAL_FOREACH_RUN_KEY] as Record<string, unknown> | undefined;
  const iterationStates = workflowMeta?.[WORKFLOW_TERMINAL_FOREACH_STATE_KEY] as Record<string, unknown> | undefined;
  const state = iterationStates?.[String(source.iterationIndex)];
  return (
    source.iterationIndex < current.payload.length &&
    source.iterationIndex < current.output.length &&
    iterationRuns?.[String(source.iterationIndex)] === view.effect.runId &&
    !FOREACH_TERMINAL_STATES.has(state)
  );
}

function activeTopologyMatchesSource(
  view: WorkflowTerminalParentPlanningView,
  source: WorkflowTerminalResultCoordinate,
): boolean {
  const activeEntries = Object.entries(view.parentSnapshot.activeStepsPath);
  if (source.kind === 'step') {
    return (
      activeEntries.length === 1 &&
      activeEntries[0]![0] === source.stepId &&
      samePath(activeEntries[0]![1], source.executionPath)
    );
  }

  const current = contextEntry(view.parentSnapshot, source.stepId);
  if (!Array.isArray(current?.payload) || !Array.isArray(current.output)) return false;
  const workflowMeta = current.metadata?.__workflow_meta as Record<string, unknown> | undefined;
  const iterationRuns = workflowMeta?.[WORKFLOW_TERMINAL_FOREACH_RUN_KEY] as Record<string, unknown> | undefined;
  const iterationStates = workflowMeta?.[WORKFLOW_TERMINAL_FOREACH_STATE_KEY] as Record<string, unknown> | undefined;
  return activeEntries.every(([stepId, path]) => {
    if (stepId !== source.stepId) return false;
    if (samePath(path, source.containerPath)) return true;
    if (
      path.length !== source.containerPath.length + 1 ||
      !source.containerPath.every((entry, index) => path[index] === entry)
    ) {
      return false;
    }
    const iterationIndex = path[path.length - 1]!;
    return (
      Number.isSafeInteger(iterationIndex) &&
      iterationIndex >= 0 &&
      iterationIndex < current.output.length &&
      typeof iterationRuns?.[String(iterationIndex)] === 'string' &&
      !FOREACH_TERMINAL_STATES.has(iterationStates?.[String(iterationIndex)])
    );
  });
}

function nextTarget(graph: readonly SerializedStepFlowEntry[], index: number): WorkflowTerminalRunTarget {
  const entry = graph[index];
  if (!entry) throw new TypeError('Workflow terminal planner successor is missing');
  if (entry.type === 'step') {
    return { kind: 'step', stepId: entry.step.id, executionPath: [index] };
  }
  if (entry.type === 'sleep' || entry.type === 'sleepUntil') {
    return { kind: 'entry', entryType: entry.type, entryId: entry.id, executionPath: [index] };
  }
  return { kind: 'container', containerType: entry.type, executionPath: [index] };
}

function materializeDecision(value: unknown): WorkflowTerminalEvaluatedLoopDecisionV1 {
  const keys = ['version', 'kind', 'decisionKey', 'conditionResult'];
  const descriptors = getPlainDataDescriptors(value, {
    allowNullPrototype: true,
    typeError: 'evaluatedDecision must be a plain data object',
    fieldsError: 'evaluatedDecision contains unknown, missing, symbol, or accessor fields',
  });
  if (
    Reflect.ownKeys(descriptors).some(
      key => typeof key !== 'string' || !keys.includes(key) || !('value' in descriptors[key]!),
    ) ||
    keys.some(key => !Object.prototype.hasOwnProperty.call(descriptors, key))
  ) {
    throw new TypeError('evaluatedDecision contains unknown, missing, symbol, or accessor fields');
  }
  const decisionKey = descriptors.decisionKey!.value;
  const conditionResult = descriptors.conditionResult!.value;
  if (
    descriptors.version!.value !== 1 ||
    descriptors.kind!.value !== 'loop-condition' ||
    typeof decisionKey !== 'string' ||
    !/^sha256:[a-f0-9]{64}$/.test(decisionKey) ||
    typeof conditionResult !== 'boolean'
  ) {
    throw new TypeError('evaluatedDecision is invalid');
  }
  return {
    version: 1,
    kind: 'loop-condition',
    decisionKey: decisionKey as WorkflowTerminalSha256,
    conditionResult,
  };
}

function loopDecisionRequestForView(
  view: WorkflowTerminalParentPlanningView,
  graphFingerprint: WorkflowTerminalSha256,
): WorkflowTerminalLoopDecisionRequestV1 {
  const resolved = resolveWorkflowTerminalGraphCoordinate(
    view.parentSnapshot.serializedStepGraph,
    view.effect.parentExecutionPath,
  );
  if (resolved.kind !== 'loop' || resolved.stepId !== view.effect.parentStepId) {
    throw new TypeError('Loop decision requested for a non-loop source');
  }
  const current = contextEntry(view.parentSnapshot, resolved.stepId);
  const previousIterationCount = canonicalPlannerInteger(
    current?.metadata?.iterationCount ?? 0,
    'loop previousIterationCount',
    MAX_TERMINAL_LOOP_ITERATIONS,
  );
  const decisionKey = framedHash('mastra.workflow-terminal-loop-decision.v1', [
    view.effect.effectKey,
    view.effect.payloadHash,
    view.parentRevision,
    graphFingerprint,
    view.parentSnapshot.status,
    view.effect.parentStepId,
    String(view.effect.parentExecutionPath.length),
    ...view.effect.parentExecutionPath.map(String),
    resolved.loopType,
    String(previousIterationCount),
  ]);
  return {
    version: 1,
    kind: 'loop-condition',
    decisionKey,
    loopType: resolved.loopType,
    previousIterationCount,
  };
}

/** @internal Derives the exact callback request for one locked loop-source revision. */
export function createWorkflowTerminalLoopDecisionRequest(
  input: Omit<WorkflowTerminalParentPlannerInputV1, 'evaluatedDecision'>,
): WorkflowTerminalLoopDecisionRequestV1 {
  const view = materializeWorkflowTerminalParentPlanningView(input);
  const graphFingerprint = view.graphFingerprint;
  const { source, graphConflict } = sourceForView(view);
  if (
    graphConflict ||
    view.effect.terminalStatus !== 'success' ||
    view.parentSnapshot.status !== 'running' ||
    !sourceIsOwned(view, source) ||
    !activeTopologyMatchesSource(view, source)
  ) {
    throw new TypeError('Loop decision requested for an inactive or unowned source');
  }
  return loopDecisionRequestForView(view, graphFingerprint);
}

/** @internal Attaches a data-only callback result to its exact structural request. */
export function completeWorkflowTerminalLoopDecision(
  request: WorkflowTerminalLoopDecisionRequestV1,
  conditionResult: boolean,
): WorkflowTerminalEvaluatedLoopDecisionV1 {
  const keys = ['version', 'kind', 'decisionKey', 'loopType', 'previousIterationCount'];
  const descriptors = getPlainDataDescriptors(request, {
    allowNullPrototype: false,
    typeError: 'Loop decision request or result is invalid',
    fieldsError: 'Loop decision request or result is invalid',
  });
  if (
    Reflect.ownKeys(descriptors).some(
      key => typeof key !== 'string' || !keys.includes(key) || !('value' in descriptors[key]!),
    ) ||
    keys.some(key => !Object.prototype.hasOwnProperty.call(descriptors, key)) ||
    request.version !== 1 ||
    request.kind !== 'loop-condition' ||
    !/^sha256:[a-f0-9]{64}$/.test(request.decisionKey) ||
    !['dowhile', 'dountil'].includes(request.loopType) ||
    canonicalPlannerInteger(
      request.previousIterationCount,
      'request.previousIterationCount',
      MAX_TERMINAL_LOOP_ITERATIONS,
    ) !== request.previousIterationCount ||
    typeof conditionResult !== 'boolean'
  ) {
    throw new TypeError('Loop decision request or result is invalid');
  }
  return { version: 1, kind: 'loop-condition', decisionKey: request.decisionKey, conditionResult };
}

function planAction(
  view: WorkflowTerminalParentPlanningView,
  graphFingerprint: WorkflowTerminalSha256,
  source: WorkflowTerminalResultCoordinate,
): { action: WorkflowTerminalContinuationAction; patch: WorkflowTerminalParentPatch } | 'plan-conflict' {
  const resolved = resolveWorkflowTerminalGraphCoordinate(
    view.parentSnapshot.serializedStepGraph,
    view.effect.parentExecutionPath,
  );
  if (view.effect.terminalStatus === 'failed') {
    if (
      resolved.kind === 'foreach' &&
      (source.kind !== 'foreach-iteration' || !foreachTerminalPropagationIsBindable(view, source))
    ) {
      return 'plan-conflict';
    }
    return { action: { kind: 'fail-parent', reason: 'parent-fail' }, patch: terminalPatch('failed') };
  }
  if (view.effect.terminalStatus === 'canceled') {
    if (
      resolved.kind === 'foreach' &&
      (source.kind !== 'foreach-iteration' || !foreachTerminalPropagationIsBindable(view, source))
    ) {
      return 'plan-conflict';
    }
    return { action: { kind: 'cancel-parent', reason: 'child-canceled' }, patch: terminalPatch('canceled') };
  }
  if (
    (resolved.kind === 'loop' || (resolved.kind === 'foreach' && resolved.iterationIndex !== undefined)) &&
    !activeTopologyMatchesSource(view, source)
  ) {
    return 'plan-conflict';
  }
  const foreachState =
    resolved.kind === 'foreach' && resolved.iterationIndex !== undefined && source.kind === 'foreach-iteration'
      ? foreachPlanningState(view, source)
      : undefined;
  if (resolved.kind === 'foreach' && foreachState === undefined) return 'plan-conflict';
  if (resolved.kind === 'loop') {
    if (view.evaluatedDecision === undefined) throw new TypeError('Loop planning requires an evaluated decision');
    const expected = loopDecisionRequestForView(view, graphFingerprint);
    const evaluated = materializeDecision(view.evaluatedDecision);
    if (evaluated.decisionKey !== expected.decisionKey) throw new TypeError('Loop decision is stale or unbound');
    const nextIterationCount = expected.previousIterationCount + 1;
    const continues = expected.loopType === 'dowhile' ? evaluated.conditionResult : !evaluated.conditionResult;
    if (
      !Number.isSafeInteger(nextIterationCount) ||
      (continues && expected.previousIterationCount >= MAX_TERMINAL_LOOP_ITERATIONS)
    ) {
      throw new TypeError('Loop iteration count is exhausted');
    }
    return {
      action: {
        kind: continues ? 'run-entry' : 'complete-entry',
        reason: continues ? 'loop-continue' : 'loop-exit',
        target: { kind: 'container', containerType: 'loop', executionPath: [view.effect.parentExecutionPath[0]!] },
        loopDecision: {
          loopType: expected.loopType,
          conditionResult: evaluated.conditionResult,
          previousIterationCount: expected.previousIterationCount,
          nextIterationCount,
        },
      } as WorkflowTerminalContinuationAction,
      patch: loopPatch(source.stepId, nextIterationCount),
    };
  }
  if (view.evaluatedDecision !== undefined) throw new TypeError('Non-loop planning rejects evaluated decisions');
  if (resolved.kind === 'branch') {
    const rootIndex = view.effect.parentExecutionPath[0]!;
    const entry = view.parentSnapshot.serializedStepGraph[rootIndex];
    if (!entry || (entry.type !== 'parallel' && entry.type !== 'conditional')) throw new TypeError('Branch is missing');
    const branchCoordinates = new Map(entry.steps.map((branch, index) => [branch.step.id, [rootIndex, index]]));
    if (
      Object.entries(view.parentSnapshot.activeStepsPath).some(
        ([stepId, path]) => !branchCoordinates.has(stepId) || !samePath(path, branchCoordinates.get(stepId)!),
      )
    ) {
      return 'plan-conflict';
    }
    const statuses = entry.steps.map(branch =>
      branch.step.id === source.stepId ? 'success' : contextEntry(view.parentSnapshot, branch.step.id)?.status,
    );
    if (statuses.some(status => status !== undefined && !BRANCH_STATUSES.has(status))) {
      return 'plan-conflict';
    }
    if (
      entry.steps.some(
        (branch, index) =>
          branch.step.id !== source.stepId &&
          statuses[index] === 'running' &&
          !Object.prototype.hasOwnProperty.call(view.parentSnapshot.activeStepsPath, branch.step.id),
      )
    ) {
      return 'plan-conflict';
    }
    if (
      statuses.some(
        status => status === 'failed' || status === 'canceled' || (entry.type === 'parallel' && status === 'skipped'),
      )
    ) {
      return 'plan-conflict';
    }
    if (
      entry.steps.some(
        (branch, index) =>
          branch.step.id !== source.stepId &&
          (statuses[index] === 'success' || statuses[index] === 'skipped') &&
          Object.prototype.hasOwnProperty.call(view.parentSnapshot.activeStepsPath, branch.step.id),
      )
    ) {
      return 'plan-conflict';
    }
    const hasSuspended = statuses.includes('suspended');
    const allAccounted = statuses.every(
      status =>
        status === 'success' || status === 'suspended' || (entry.type === 'conditional' && status === 'skipped'),
    );
    const allComplete = statuses.every(
      status => status === 'success' || (entry.type === 'conditional' && status === 'skipped'),
    );
    const target = { kind: 'container' as const, containerType: entry.type, executionPath: [rootIndex] };
    if (allAccounted && hasSuspended) {
      return { action: { kind: 'suspend-parent', reason: 'branch-suspended', target }, patch: suspendPatch() };
    }
    if (allComplete) {
      return {
        action:
          entry.type === 'parallel'
            ? {
                kind: 'complete-entry',
                reason: 'parallel-continue',
                target: { ...target, containerType: 'parallel' },
              }
            : {
                kind: 'complete-entry',
                reason: 'conditional-continue',
                target: { ...target, containerType: 'conditional' },
              },
        patch: preservePatch(),
      };
    }
    return {
      action: {
        kind: 'wait',
        reason: entry.type === 'parallel' ? 'parallel-aggregation' : 'conditional-aggregation',
        coordinate: target,
      },
      patch: preservePatch(),
    };
  }
  if (resolved.kind === 'foreach' && resolved.iterationIndex !== undefined && source.kind === 'foreach-iteration') {
    if (!foreachState) return 'plan-conflict';
    const { current, iterationRuns: runs, states } = foreachState;
    const isTerminal = (index: number) =>
      index === source.iterationIndex || FOREACH_TERMINAL_STATES.has(states[String(index)]);
    if (
      current.output.some(
        (_: unknown, index: number) =>
          index !== source.iterationIndex && !isTerminal(index) && typeof runs[String(index)] !== 'string',
      )
    ) {
      return 'plan-conflict';
    }
    if (
      current.output.some(
        (_: unknown, index: number) =>
          index !== source.iterationIndex &&
          (states[String(index)] === 'failed' || states[String(index)] === 'canceled'),
      )
    ) {
      return 'plan-conflict';
    }
    const isSuspended = (index: number) =>
      index !== source.iterationIndex &&
      !isTerminal(index) &&
      current.output[index] !== null &&
      typeof current.output[index] === 'object' &&
      current.output[index]?.status === 'suspended';
    if (
      current.output.some(
        (output: unknown, index: number) =>
          !isTerminal(index) &&
          output !== null &&
          !(
            typeof output === 'object' &&
            !Array.isArray(output) &&
            (output as { status?: unknown }).status === 'suspended'
          ),
      )
    ) {
      return 'plan-conflict';
    }
    const hasPendingStarted = current.output.some(
      (_: unknown, index: number) => !isTerminal(index) && !isSuspended(index),
    );
    const hasSuspended = current.output.some((_: unknown, index: number) => isSuspended(index));
    const containerPath = [...source.containerPath];
    if (hasPendingStarted) {
      return {
        action: {
          kind: 'wait',
          reason: 'foreach-aggregation',
          coordinate: { kind: 'container', containerType: 'foreach', executionPath: containerPath },
        },
        patch: preservePatch(),
      };
    }
    if (current.output.length < current.payload.length) {
      return {
        action: {
          kind: 'run-entry',
          reason: 'foreach-continue',
          target: {
            kind: 'foreach-iteration',
            stepId: source.stepId,
            containerPath,
            iterationIndex: current.output.length,
          },
        },
        patch: preservePatch(),
      };
    }
    const target = { kind: 'container' as const, containerType: 'foreach' as const, executionPath: containerPath };
    return hasSuspended
      ? { action: { kind: 'suspend-parent', reason: 'foreach-suspended', target }, patch: suspendPatch() }
      : { action: { kind: 'complete-entry', reason: 'foreach-complete', target }, patch: preservePatch() };
  }
  if (resolved.kind !== 'step' || source.kind !== 'step' || source.executionPath.length !== 1) {
    return 'plan-conflict';
  }
  const index = source.executionPath[0]!;
  if (Object.keys(view.parentSnapshot.activeStepsPath).some(stepId => stepId !== source.stepId)) {
    return 'plan-conflict';
  }
  if (index === view.parentSnapshot.serializedStepGraph.length - 1) {
    return { action: { kind: 'finish-parent', reason: 'parent-end' }, patch: terminalPatch('success') };
  }
  return {
    action: {
      kind: 'run-entry',
      reason: 'next-step',
      target: nextTarget(view.parentSnapshot.serializedStepGraph, index + 1),
    },
    patch: preservePatch(),
  };
}

/** @internal Purely selects one graph-bound continuation; it never calls storage, callbacks, clocks, or PubSub. */
export function planWorkflowTerminalParentContinuation(input: unknown): WorkflowTerminalParentPlannerResult {
  const view = materializeWorkflowTerminalParentPlanningView(input);
  const graphFingerprint = view.graphFingerprint;
  const { source, graphConflict } = sourceForView(view);
  if (graphConflict) {
    if (view.evaluatedDecision !== undefined)
      throw new TypeError('Graph-conflict planning rejects evaluated decisions');
    return quarantine('graph-conflict', view, graphFingerprint, source);
  }
  if (FINAL_PARENT_STATUSES.has(view.parentSnapshot.status)) {
    if (view.evaluatedDecision !== undefined) throw new TypeError('No-op planning rejects evaluated decisions');
    const contract = createWorkflowTerminalParentContinuationContract({
      ...contractBase(view, graphFingerprint, source),
      action: { kind: 'noop', reason: 'already-terminal' },
      patch: { kind: 'none' },
    });
    validateWorkflowTerminalParentContinuationBinding(contract, {
      effect: view.effect,
      parentRevision: view.parentRevision,
      parentWorkflowName: view.effect.parentWorkflowName,
      parentSnapshot: view.parentSnapshot,
      executionMode: 'continuous',
    });
    return contract;
  }
  if (!ACTIVE_PARENT_STATUSES.has(view.parentSnapshot.status) || !sourceIsOwned(view, source)) {
    if (view.evaluatedDecision !== undefined) throw new TypeError('Plan-conflict planning rejects evaluated decisions');
    return quarantine('plan-conflict', view, graphFingerprint, source);
  }
  if (view.effect.terminalStatus !== 'success' && view.evaluatedDecision !== undefined) {
    throw new TypeError('Terminal child planning rejects evaluated decisions');
  }
  if (view.effect.terminalStatus === 'success' && view.parentSnapshot.status !== 'running') {
    if (view.evaluatedDecision !== undefined) {
      throw new TypeError('Inactive parent planning rejects evaluated decisions');
    }
    const resolved = resolveWorkflowTerminalGraphCoordinate(
      view.parentSnapshot.serializedStepGraph,
      view.effect.parentExecutionPath,
    );
    if (resolved.kind === 'loop') {
      return quarantine('plan-conflict', view, graphFingerprint, source);
    }
  }
  const planned = planAction(view, graphFingerprint, source);
  if (planned === 'plan-conflict') return quarantine('plan-conflict', view, graphFingerprint, source);
  if (
    view.parentSnapshot.status !== 'running' &&
    planned.action.kind !== 'wait' &&
    planned.action.kind !== 'fail-parent' &&
    planned.action.kind !== 'cancel-parent'
  ) {
    return quarantine('plan-conflict', view, graphFingerprint, source);
  }
  const contract = createWorkflowTerminalParentContinuationContract({
    ...contractBase(view, graphFingerprint, source),
    action: planned.action,
    patch: planned.patch,
  } as WorkflowTerminalParentContinuationSpec);
  validateWorkflowTerminalParentContinuationBinding(contract, {
    effect: view.effect,
    parentRevision: view.parentRevision,
    parentWorkflowName: view.effect.parentWorkflowName,
    parentSnapshot: view.parentSnapshot,
    executionMode: 'continuous',
  });
  return contract;
}
