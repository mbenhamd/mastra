import { describe, expect, it, vi } from 'vitest';
import {
  getWorkflowTerminalRecoveryEnvelopeHash,
  materializeWorkflowTerminalRecoveryEnvelope,
} from '../terminal-recovery';
import type { SerializedStepFlowEntry, WorkflowRunState, WorkflowTerminalSnapshotRecord } from '../types';
import { getWorkflowTerminalEffectIntegrity } from './effect-integrity';
import { createWorkflowTerminalGraphFingerprint } from './graph-fingerprint';
import {
  MAX_WORKFLOW_TERMINAL_LOOP_DECISION_REPLANS,
  MAX_WORKFLOW_TERMINAL_LOOP_DECISION_ATTEMPTS,
  copyWorkflowTerminalLoopConditionFrame,
  getWorkflowTerminalLoopConditionSourceHash,
  materializeWorkflowTerminalLoopConditionFrame,
  nextWorkflowTerminalLoopDecisionReplanAttempt,
} from './loop-decision';

function fixture(
  loopType: 'dowhile' | 'dountil' = 'dowhile',
  options: { output?: boolean; resumePayload?: boolean } = {},
) {
  const parentGraph: SerializedStepFlowEntry[] = [
    {
      type: 'loop',
      step: { id: 'nested', component: 'WORKFLOW' },
      loopType,
      serializedCondition: { id: 'nested-condition', fn: 'async ({ state }) => state.keepGoing' },
    },
  ];
  const childGraph: SerializedStepFlowEntry[] = [{ type: 'step', step: { id: 'child-step' } }];
  const envelope = materializeWorkflowTerminalRecoveryEnvelope({
    version: 1,
    workflowName: 'child',
    runId: 'child-run',
    terminalStatus: 'success',
    executionMode: 'continuous',
    terminalResult: {
      status: 'success',
      ...(options.output === false ? {} : { output: { answer: 42 } }),
      metadata: { child: true },
      startedAt: 2,
      endedAt: 20,
    },
    finalState: { keepGoing: true, nested: { value: 1 } },
    requestContextPatch: { childContext: 'retained' },
    childGraphFingerprint: createWorkflowTerminalGraphFingerprint(childGraph),
    ancestry: [
      {
        version: 1,
        childWorkflowName: 'child',
        childRunId: 'child-run',
        parentWorkflowName: 'parent',
        parentRunId: 'parent-run',
        parentGraphFingerprint: createWorkflowTerminalGraphFingerprint(parentGraph),
        source: { kind: 'step', stepId: 'nested', executionPath: [0] },
        inputPointer: { kind: 'parent-source-payload', stepId: 'nested' },
        resultPointer: { kind: 'retained-terminal-result', workflowName: 'child', runId: 'child-run' },
        resumeMetadata: { wasResume: true, resumeSteps: ['approval'] },
      },
    ],
  });
  const envelopeHash = getWorkflowTerminalRecoveryEnvelopeHash(envelope);
  const identity = {
    version: 1 as const,
    kind: 'parent-workflow-step-end' as const,
    workflowName: 'child',
    runId: 'child-run',
    sourceEventKey: 'event-1',
    terminalStatus: 'success' as const,
    recoveryEnvelopeHash: envelopeHash,
    parentWorkflowName: 'parent',
    parentRunId: 'parent-run',
    parentStepId: 'nested',
    parentExecutionPath: [0],
  };
  const effect = { ...identity, ...getWorkflowTerminalEffectIntegrity(identity), createdAt: 20 };
  const parentSnapshot: WorkflowRunState = {
    runId: 'parent-run',
    status: 'running',
    requestContext: { parentContext: 'locked', childContext: 'old' },
    value: { keepGoing: false },
    context: {
      input: { initial: 'value' },
      nested: {
        status: 'running',
        payload: { original: true },
        ...(options.resumePayload === false ? {} : { resumePayload: { approval: 'yes' } }),
        startedAt: 1,
        metadata: { nestedRunId: 'child-run', iterationCount: 2, parent: true },
      },
      __state: { keepGoing: false },
    } as WorkflowRunState['context'],
    serializedStepGraph: parentGraph,
    activePaths: [0],
    activeStepsPath: { nested: [0] },
    suspendedPaths: {},
    resumeLabels: {},
    waitingPaths: {},
    timestamp: 10,
  };
  const retainedChild: WorkflowTerminalSnapshotRecord = {
    version: 1,
    workflowName: 'child',
    runId: 'child-run',
    terminalStatus: 'success',
    envelopeHash,
    envelope,
    createdAt: 20,
  };
  return {
    plannerInput: { version: 1 as const, effect, parentRevision: 'revision-1', parentSnapshot },
    retainedChild,
  };
}

describe('workflow terminal loop decision frame', () => {
  it.each(['dowhile', 'dountil'] as const)(
    'materializes the restartable %s callback subset from the canonical child merge',
    loopType => {
      const input = fixture(loopType);
      const frame = materializeWorkflowTerminalLoopConditionFrame(input);

      expect(frame).toMatchObject({
        version: 1,
        kind: 'loop-condition-frame',
        parentWorkflowName: 'parent',
        parentRunId: 'parent-run',
        parentRevision: 'revision-1',
        conditionId: 'nested-condition',
        conditionSourceHash: expect.stringMatching(/^sha256:[a-f0-9]{64}$/),
        inputData: { answer: 42 },
        state: { keepGoing: true, nested: { value: 1 } },
        requestContext: { parentContext: 'locked', childContext: 'retained' },
        resumeData: { approval: 'yes' },
        retryCount: 0,
        iterationCount: 3,
        request: { loopType, previousIterationCount: 2 },
      });
      expect(frame.stepResults.nested).toMatchObject({
        status: 'success',
        output: { answer: 42 },
        payload: { original: true },
        metadata: { child: true, parent: true, nestedRunId: 'child-run', iterationCount: 3 },
      });
      expect(frame.stepResults.input).toEqual({ initial: 'value' });

      input.plannerInput.parentSnapshot.context.nested!.payload = { changed: true } as never;
      input.retainedChild.envelope.finalState.keepGoing = false;
      expect(frame.stepResults.nested).toMatchObject({ payload: { original: true } });
      expect(frame.state.keepGoing).toBe(true);
    },
  );

  it('binds replans to a fresh request without changing the callback retry count', () => {
    const input = fixture();
    const initial = materializeWorkflowTerminalLoopConditionFrame(input);
    expect(initial.retryCount).toBe(0);

    const staleRevision = materializeWorkflowTerminalLoopConditionFrame({
      ...input,
      plannerInput: { ...input.plannerInput, parentRevision: 'revision-2' },
    });
    expect(staleRevision.retryCount).toBe(0);
    expect(staleRevision.request.decisionKey).not.toBe(initial.request.decisionKey);

    const wrong = fixture();
    wrong.retainedChild.runId = 'different-child';
    expect(() => materializeWorkflowTerminalLoopConditionFrame(wrong)).toThrow(/does not match/);
  });

  it('materializes absent terminal output and persisted resume payload as absent frame values', () => {
    const input = fixture('dowhile', { output: false, resumePayload: false });
    const frame = materializeWorkflowTerminalLoopConditionFrame(input);
    expect(Object.hasOwn(frame, 'inputData')).toBe(false);
    expect(Object.hasOwn(frame, 'resumeData')).toBe(false);
  });

  it('rejects hostile frames without invoking accessors and enforces exact fields', () => {
    const frame = materializeWorkflowTerminalLoopConditionFrame(fixture());
    const getter = () => {
      throw new Error('must not execute');
    };
    const hostile = { ...frame } as Record<string, unknown>;
    Object.defineProperty(hostile, 'state', { enumerable: true, get: getter });
    expect(() => copyWorkflowTerminalLoopConditionFrame(hostile)).toThrow(/data fields/);
    expect(() => copyWorkflowTerminalLoopConditionFrame({ ...frame, extra: true })).toThrow(/unknown fields/);
    expect(() => copyWorkflowTerminalLoopConditionFrame({ ...frame, iterationCount: 9 })).toThrow(/does not follow/);
  });

  it('rejects malformed callback source, aggregate oversize, credentials, and materialization accessors', () => {
    expect(() => getWorkflowTerminalLoopConditionSourceHash('function bad() { /* \ud800 */ }')).toThrow(/well-formed/);
    expect(() => getWorkflowTerminalLoopConditionSourceHash('function () { [native code] }')).toThrow(/unsupported/);

    const valid = materializeWorkflowTerminalLoopConditionFrame(fixture());
    expect(() =>
      copyWorkflowTerminalLoopConditionFrame({
        ...valid,
        state: { large: 'a'.repeat(4_300_000) },
        stepResults: { large: 'b'.repeat(4_300_000) },
      }),
    ).toThrow(/byte limit/);

    const credential = fixture();
    credential.plannerInput.parentSnapshot.requestContext = { mastra__authToken: 'secret' };
    expect(() => materializeWorkflowTerminalLoopConditionFrame(credential)).toThrow(/framework credential/);

    const getter = vi.fn(() => fixture().plannerInput);
    const hostile = { retainedChild: fixture().retainedChild } as Record<string, unknown>;
    Object.defineProperty(hostile, 'plannerInput', { enumerable: true, get: getter });
    expect(() => materializeWorkflowTerminalLoopConditionFrame(hostile as never)).toThrow(/data fields/);
    expect(getter).not.toHaveBeenCalled();
  });

  it('defines a bounded live replan count without claiming a durable execution bound', () => {
    let attempt: number | undefined = 0;
    const observed = [attempt];
    while (attempt !== undefined) {
      attempt = nextWorkflowTerminalLoopDecisionReplanAttempt(attempt);
      observed.push(attempt);
    }
    expect(observed).toEqual([0, 1, 2, undefined]);
    expect(MAX_WORKFLOW_TERMINAL_LOOP_DECISION_ATTEMPTS).toBe(3);
    expect(MAX_WORKFLOW_TERMINAL_LOOP_DECISION_REPLANS).toBe(2);
    expect(() => nextWorkflowTerminalLoopDecisionReplanAttempt(3)).toThrow(/no greater than/);
  });
});
