import { createHash } from 'node:crypto';
import { isProxy } from 'node:util/types';
import {
  materializeWorkflowTerminalCanonicalJson,
  materializeWorkflowTerminalCanonicalJsonObject,
  validateWorkflowTerminalRecoveryEnvelopeIntegrity,
  validateWorkflowTerminalRecoveryParentFrameGraphBinding,
} from '../terminal-recovery';
import type { WorkflowTerminalCanonicalJsonObject, WorkflowTerminalCanonicalJsonValue } from '../terminal-recovery';
import type { WorkflowRunState, WorkflowTerminalSnapshotRecord } from '../types';
import { resolveWorkflowTerminalGraphCoordinate, validateWorkflowTerminalStructuralString } from './graph-fingerprint';
import {
  completeWorkflowTerminalLoopDecision,
  createWorkflowTerminalLoopDecisionRequest,
  planWorkflowTerminalParentContinuation,
} from './planner';
import type { WorkflowTerminalLoopDecisionRequestV1, WorkflowTerminalParentPlannerInputV1 } from './planner-types';
import { applyWorkflowTerminalParentContinuationPatch } from './semantics';
import type { WorkflowTerminalSha256 } from './types';

/**
 * Maximum number of fresh parent revisions one live PF-1780 orchestration may
 * evaluate after the initial decision. A process crash resets this in-memory
 * bound; it is not a durable callback execution counter.
 */
export const MAX_WORKFLOW_TERMINAL_LOOP_DECISION_ATTEMPTS = 3;

/** Initial attempt excluded: three total attempts permit two fresh-revision replans. */
export const MAX_WORKFLOW_TERMINAL_LOOP_DECISION_REPLANS = MAX_WORKFLOW_TERMINAL_LOOP_DECISION_ATTEMPTS - 1;

/** The default cooperative deadline for one callback attempt. */
export const DEFAULT_WORKFLOW_TERMINAL_LOOP_DECISION_TIMEOUT_MS = 30_000;

/** The largest callback deadline accepted by the internal evaluator. */
export const MAX_WORKFLOW_TERMINAL_LOOP_DECISION_TIMEOUT_MS = 5 * 60_000;
const MASTRA_AUTH_TOKEN_KEY = 'mastra__authToken';

/**
 * Restartable data supplied to one loop-condition callback attempt.
 * Framework runtime objects are intentionally not retained in this frame.
 */
export interface WorkflowTerminalLoopConditionFrameV1 {
  version: 1;
  kind: 'loop-condition-frame';
  request: WorkflowTerminalLoopDecisionRequestV1;
  parentWorkflowName: string;
  parentRunId: string;
  parentRevision: string;
  conditionId: string;
  conditionSourceHash: WorkflowTerminalSha256;
  inputData?: WorkflowTerminalCanonicalJsonValue;
  state: WorkflowTerminalCanonicalJsonObject;
  requestContext: WorkflowTerminalCanonicalJsonObject;
  stepResults: WorkflowTerminalCanonicalJsonObject;
  resumeData?: WorkflowTerminalCanonicalJsonValue;
  retryCount: number;
  iterationCount: number;
}

export interface MaterializeWorkflowTerminalLoopConditionFrameInput {
  plannerInput: Omit<WorkflowTerminalParentPlannerInputV1, 'evaluatedDecision'>;
  retainedChild: WorkflowTerminalSnapshotRecord;
}

function dataDescriptors(value: unknown, field: string): Record<string, PropertyDescriptor> {
  if (value === null || typeof value !== 'object' || Array.isArray(value) || isProxy(value)) {
    throw new TypeError(`${field} must be a plain data object`);
  }
  const prototype = Object.getPrototypeOf(value);
  if (prototype !== Object.prototype && prototype !== null) {
    throw new TypeError(`${field} must be a plain data object`);
  }
  const descriptors = Object.getOwnPropertyDescriptors(value);
  for (const key of Reflect.ownKeys(descriptors)) {
    const descriptor = descriptors[key as string];
    if (typeof key !== 'string' || !descriptor || !descriptor.enumerable || !('value' in descriptor)) {
      throw new TypeError(`${field} must contain only enumerable string data fields`);
    }
  }
  return descriptors;
}

function exactKeys(
  descriptors: Record<string, PropertyDescriptor>,
  allowed: readonly string[],
  required: readonly string[],
  field: string,
): void {
  const allowedSet = new Set(allowed);
  if (Object.keys(descriptors).some(key => !allowedSet.has(key))) {
    throw new TypeError(`${field} contains unknown fields`);
  }
  if (required.some(key => !Object.hasOwn(descriptors, key))) {
    throw new TypeError(`${field} is missing required fields`);
  }
}

function read(descriptors: Record<string, PropertyDescriptor>, key: string): unknown {
  return descriptors[key]?.value;
}

function boundedInteger(value: unknown, field: string, maximum: number): number {
  if (!Number.isSafeInteger(value) || (value as number) < 0 || (value as number) > maximum) {
    throw new TypeError(`${field} must be a non-negative safe integer no greater than ${maximum}`);
  }
  return value === 0 ? 0 : (value as number);
}

/** Hashes registered callback source without treating stored source as executable authorization. */
export function getWorkflowTerminalLoopConditionSourceHash(source: unknown): WorkflowTerminalSha256 {
  const validated = validateWorkflowTerminalStructuralString(source, 'loop condition source', 256 * 1024);
  if (validated.includes('[native code]')) {
    throw new TypeError('native or bound loop conditions are unsupported in durable mode');
  }
  const bytes = Buffer.from(validated, 'utf8');
  if (bytes.length > 256 * 1024) throw new TypeError('loop condition source must be a bounded string');
  const hash = createHash('sha256');
  hash.update('mastra.workflow-terminal-loop-condition-source.v1');
  hash.update(String(bytes.length));
  hash.update(':');
  hash.update(bytes);
  return `sha256:${hash.digest('hex')}`;
}

function copyDecisionRequest(input: unknown): WorkflowTerminalLoopDecisionRequestV1 {
  const descriptors = dataDescriptors(input, 'loop decision request');
  exactKeys(
    descriptors,
    ['version', 'kind', 'decisionKey', 'loopType', 'previousIterationCount'],
    ['version', 'kind', 'decisionKey', 'loopType', 'previousIterationCount'],
    'loop decision request',
  );
  const request = {
    version: read(descriptors, 'version'),
    kind: read(descriptors, 'kind'),
    decisionKey: read(descriptors, 'decisionKey'),
    loopType: read(descriptors, 'loopType'),
    previousIterationCount: read(descriptors, 'previousIterationCount'),
  } as WorkflowTerminalLoopDecisionRequestV1;
  // The completion helper is the canonical validator for this internal request.
  completeWorkflowTerminalLoopDecision(request, false);
  return {
    version: 1,
    kind: 'loop-condition',
    decisionKey: request.decisionKey,
    loopType: request.loopType,
    previousIterationCount: request.previousIterationCount,
  };
}

function optionalCanonicalValue(
  descriptors: Record<string, PropertyDescriptor>,
  key: string,
  field: string,
): { present: false } | { present: true; value: WorkflowTerminalCanonicalJsonValue } {
  if (!Object.hasOwn(descriptors, key)) return { present: false };
  return { present: true, value: materializeWorkflowTerminalCanonicalJson(read(descriptors, key), field) };
}

/** Copies and validates a callback frame without invoking accessors or `toJSON`. */
export function copyWorkflowTerminalLoopConditionFrame(input: unknown): WorkflowTerminalLoopConditionFrameV1 {
  const descriptors = dataDescriptors(input, 'loop condition frame');
  const required = [
    'version',
    'kind',
    'request',
    'parentWorkflowName',
    'parentRunId',
    'parentRevision',
    'conditionId',
    'conditionSourceHash',
    'state',
    'requestContext',
    'stepResults',
    'retryCount',
    'iterationCount',
  ];
  exactKeys(descriptors, [...required, 'inputData', 'resumeData'], required, 'loop condition frame');
  if (read(descriptors, 'version') !== 1 || read(descriptors, 'kind') !== 'loop-condition-frame') {
    throw new TypeError('loop condition frame version or kind is invalid');
  }
  const request = copyDecisionRequest(read(descriptors, 'request'));
  const retryCount = boundedInteger(read(descriptors, 'retryCount'), 'loop condition frame retryCount', 0);
  const iterationCount = boundedInteger(
    read(descriptors, 'iterationCount'),
    'loop condition frame iterationCount',
    Number.MAX_SAFE_INTEGER,
  );
  if (iterationCount !== request.previousIterationCount + 1) {
    throw new TypeError('loop condition frame iterationCount does not follow its decision request');
  }
  const inputData = optionalCanonicalValue(descriptors, 'inputData', 'loop condition frame inputData');
  const resumeData = optionalCanonicalValue(descriptors, 'resumeData', 'loop condition frame resumeData');
  const frame: WorkflowTerminalLoopConditionFrameV1 = {
    version: 1,
    kind: 'loop-condition-frame',
    request,
    parentWorkflowName: validateWorkflowTerminalStructuralString(
      read(descriptors, 'parentWorkflowName'),
      'loop condition frame parentWorkflowName',
    ),
    parentRunId: validateWorkflowTerminalStructuralString(
      read(descriptors, 'parentRunId'),
      'loop condition frame parentRunId',
    ),
    parentRevision: validateWorkflowTerminalStructuralString(
      read(descriptors, 'parentRevision'),
      'loop condition frame parentRevision',
    ),
    conditionId: validateWorkflowTerminalStructuralString(
      read(descriptors, 'conditionId'),
      'loop condition frame conditionId',
    ),
    conditionSourceHash: (() => {
      const value = read(descriptors, 'conditionSourceHash');
      if (typeof value !== 'string' || !/^sha256:[a-f0-9]{64}$/.test(value)) {
        throw new TypeError('loop condition frame conditionSourceHash is invalid');
      }
      return value as WorkflowTerminalSha256;
    })(),
    ...(inputData.present ? { inputData: inputData.value } : {}),
    state: materializeWorkflowTerminalCanonicalJsonObject(read(descriptors, 'state'), 'loop condition frame state'),
    requestContext: materializeWorkflowTerminalCanonicalJsonObject(
      read(descriptors, 'requestContext'),
      'loop condition frame requestContext',
    ),
    stepResults: materializeWorkflowTerminalCanonicalJsonObject(
      read(descriptors, 'stepResults'),
      'loop condition frame stepResults',
    ),
    ...(resumeData.present ? { resumeData: resumeData.value } : {}),
    retryCount,
    iterationCount,
  };
  if (Object.hasOwn(frame.requestContext, MASTRA_AUTH_TOKEN_KEY)) {
    throw new TypeError('loop condition frame requestContext contains a framework credential');
  }
  // Enforce one aggregate frame bound rather than allowing every large field
  // to independently consume the full recovery-envelope budget.
  materializeWorkflowTerminalCanonicalJson(frame, 'loop condition frame');
  return frame;
}

/**
 * Builds the corrected restartable callback subset from locked structural
 * state plus authenticated PF-1782 evidence. It intentionally does not reuse
 * process-local ParentWorkflow arguments.
 */
export function materializeWorkflowTerminalLoopConditionFrame(
  input: MaterializeWorkflowTerminalLoopConditionFrameInput,
): WorkflowTerminalLoopConditionFrameV1 {
  const inputDescriptors = dataDescriptors(input, 'loop condition materialization input');
  exactKeys(
    inputDescriptors,
    ['plannerInput', 'retainedChild'],
    ['plannerInput', 'retainedChild'],
    'loop condition materialization input',
  );
  const plannerDescriptors = dataDescriptors(read(inputDescriptors, 'plannerInput'), 'loop condition planner input');
  exactKeys(
    plannerDescriptors,
    ['version', 'effect', 'parentRevision', 'parentSnapshot'],
    ['version', 'effect', 'parentRevision', 'parentSnapshot'],
    'loop condition planner input',
  );
  if (read(plannerDescriptors, 'version') !== 1) throw new TypeError('loop condition planner input version is invalid');
  const plannerInput: Omit<WorkflowTerminalParentPlannerInputV1, 'evaluatedDecision'> = {
    version: 1,
    effect: materializeWorkflowTerminalCanonicalJsonObject(
      read(plannerDescriptors, 'effect'),
      'loop condition terminal effect',
    ) as unknown as WorkflowTerminalParentPlannerInputV1['effect'],
    parentRevision: validateWorkflowTerminalStructuralString(
      read(plannerDescriptors, 'parentRevision'),
      'loop condition parent revision',
    ),
    parentSnapshot: materializeWorkflowTerminalCanonicalJsonObject(
      read(plannerDescriptors, 'parentSnapshot'),
      'loop condition parent snapshot',
    ) as unknown as WorkflowRunState,
  };
  const retainedChild = materializeWorkflowTerminalCanonicalJsonObject(
    read(inputDescriptors, 'retainedChild'),
    'loop condition retained child',
  ) as unknown as WorkflowTerminalSnapshotRecord;
  const request = createWorkflowTerminalLoopDecisionRequest(plannerInput);
  const { effect, parentRevision, parentSnapshot } = plannerInput;
  if (effect.terminalStatus !== 'success' || retainedChild.terminalStatus !== 'success') {
    throw new TypeError('Loop condition materialization requires a successful child terminal effect');
  }
  validateWorkflowTerminalRecoveryEnvelopeIntegrity(
    { version: 1, envelopeHash: retainedChild.envelopeHash, envelope: retainedChild.envelope },
    {
      workflowName: effect.workflowName,
      runId: effect.runId,
      terminalStatus: 'success',
      envelopeHash: effect.recoveryEnvelopeHash,
    },
  );
  if (
    retainedChild.version !== 1 ||
    retainedChild.workflowName !== effect.workflowName ||
    retainedChild.runId !== effect.runId ||
    retainedChild.envelopeHash !== effect.recoveryEnvelopeHash ||
    !Number.isSafeInteger(retainedChild.createdAt) ||
    retainedChild.createdAt < 0
  ) {
    throw new TypeError('Retained child does not match the loop terminal effect');
  }
  const parentFrame = retainedChild.envelope.ancestry[0];
  if (
    !parentFrame ||
    parentFrame.childWorkflowName !== effect.workflowName ||
    parentFrame.childRunId !== effect.runId ||
    parentFrame.parentWorkflowName !== effect.parentWorkflowName ||
    parentFrame.parentRunId !== effect.parentRunId
  ) {
    throw new TypeError('Retained child ancestry does not match the loop parent effect');
  }
  validateWorkflowTerminalRecoveryParentFrameGraphBinding(parentFrame, parentSnapshot.serializedStepGraph);
  const resolved = resolveWorkflowTerminalGraphCoordinate(
    parentSnapshot.serializedStepGraph,
    effect.parentExecutionPath,
  );
  if (resolved.kind !== 'loop' || resolved.stepId !== effect.parentStepId) {
    throw new TypeError('Loop condition registration does not match the terminal source');
  }
  const loopEntry = parentSnapshot.serializedStepGraph[effect.parentExecutionPath[0]!];
  if (!loopEntry || loopEntry.type !== 'loop') {
    throw new TypeError('Loop condition registration is missing from the locked parent graph');
  }

  // The loop decision itself is not known yet. An exit decision is used only
  // to reuse the canonical child merge projection; both loop outcomes produce
  // the same context/state/request-context values and consecutive count.
  const exitConditionResult = request.loopType === 'dowhile' ? false : true;
  const previewContract = planWorkflowTerminalParentContinuation({
    ...plannerInput,
    evaluatedDecision: completeWorkflowTerminalLoopDecision(request, exitConditionResult),
  });
  const preview = applyWorkflowTerminalParentContinuationPatch({
    contract: previewContract,
    effect,
    parentRevision,
    parentWorkflowName: effect.parentWorkflowName,
    parentSnapshot,
    retainedChild,
    storageTimestamp: Math.max(parentSnapshot.timestamp, retainedChild.createdAt),
    executionMode: 'continuous',
  });
  const terminalResultDescriptors = dataDescriptors(retainedChild.envelope.terminalResult, 'retained terminal result');
  const inputData = optionalCanonicalValue(terminalResultDescriptors, 'output', 'retained terminal output');
  const sourceResult = dataDescriptors(parentSnapshot.context[effect.parentStepId], 'locked parent loop source result');
  const resumeData = optionalCanonicalValue(sourceResult, 'resumePayload', 'locked parent loop resume payload');

  return copyWorkflowTerminalLoopConditionFrame({
    version: 1,
    kind: 'loop-condition-frame',
    request,
    parentWorkflowName: effect.parentWorkflowName,
    parentRunId: effect.parentRunId,
    parentRevision,
    conditionId: loopEntry.serializedCondition.id,
    conditionSourceHash: getWorkflowTerminalLoopConditionSourceHash(loopEntry.serializedCondition.fn),
    ...(inputData.present ? { inputData: inputData.value } : {}),
    state: preview.value,
    requestContext: preview.requestContext ?? {},
    stepResults: preview.context,
    ...(resumeData.present ? { resumeData: resumeData.value } : {}),
    // Parent CAS replans are not workflow step retries and must not change a
    // user-visible condition retryCount.
    retryCount: 0,
    iterationCount: request.previousIterationCount + 1,
  });
}

/** Returns the next bounded live replan attempt, or `undefined` when exhausted. */
export function nextWorkflowTerminalLoopDecisionReplanAttempt(currentAttempt: number): number | undefined {
  const current = boundedInteger(
    currentAttempt,
    'loop decision replan attempt',
    MAX_WORKFLOW_TERMINAL_LOOP_DECISION_REPLANS,
  );
  return current >= MAX_WORKFLOW_TERMINAL_LOOP_DECISION_REPLANS ? undefined : current + 1;
}
