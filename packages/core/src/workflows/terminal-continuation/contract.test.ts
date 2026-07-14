import { describe, expect, it } from 'vitest';
import type { WorkflowRunState, WorkflowTerminalEffectRecord } from '../types';
import {
  copyWorkflowTerminalParentContinuationContract,
  createWorkflowTerminalParentContinuationContract,
  validateWorkflowTerminalParentContinuationBinding,
  validateWorkflowTerminalParentContinuationIntegrity,
} from './contract';
import { createWorkflowTerminalGraphFingerprint } from './graph-fingerprint';

const mergePatch = {
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
} as const;

function parentSnapshot(status: WorkflowRunState['status'] = 'running'): WorkflowRunState {
  return {
    runId: 'parent-run',
    status,
    value: {},
    context: {
      nested: {
        status: 'running',
        payload: { paperId: 'p1' },
        startedAt: 1,
        metadata: { nestedRunId: 'child-run' },
      },
      each: {
        status: 'running',
        payload: ['a', 'b'],
        output: [null, null],
        startedAt: 1,
        metadata: { __workflow_meta: { iterationRunIds: { '0': 'child-run', '1': 'child-run' } } },
      },
    } as WorkflowRunState['context'],
    serializedStepGraph: [
      { type: 'step', step: { id: 'nested', component: 'WORKFLOW' } },
      { type: 'sleep', id: 'sleep-1', duration: 10 },
      {
        type: 'loop',
        step: { id: 'loop' },
        serializedCondition: { id: 'loop-condition', fn: '() => true' },
        loopType: 'dowhile',
      },
      { type: 'foreach', step: { id: 'each' }, opts: { concurrency: 2 } },
    ],
    activePaths: [0],
    activeStepsPath: { nested: [0] },
    suspendedPaths: {},
    resumeLabels: {},
    waitingPaths: {},
    timestamp: 1,
  };
}

function effect(
  path = [0],
  stepId = 'nested',
  terminalStatus: 'success' | 'failed' | 'canceled' = 'success',
): Extract<WorkflowTerminalEffectRecord, { kind: 'parent-workflow-step-end' }> {
  return {
    version: 1,
    effectKey: 'effect',
    kind: 'parent-workflow-step-end',
    workflowName: 'child',
    runId: 'child-run',
    sourceEventKey: 'event',
    terminalStatus,
    payloadHash: `sha256:${'d'.repeat(64)}`,
    createdAt: 1,
    parentWorkflowName: 'parent',
    parentRunId: 'parent-run',
    parentStepId: stepId,
    parentExecutionPath: path,
  };
}

function nextSleepSpec(snapshot = parentSnapshot()) {
  return {
    version: 1,
    terminalEffectKey: 'effect',
    terminalEffectPayloadHash: `sha256:${'d'.repeat(64)}`,
    executionMode: 'continuous',
    expectedParentRevision: 'revision-1',
    graphFingerprint: createWorkflowTerminalGraphFingerprint(snapshot.serializedStepGraph),
    childTerminalStatus: 'success',
    observedParentStatus: snapshot.status,
    source: { kind: 'step', stepId: 'nested', executionPath: [0] },
    action: {
      kind: 'run-entry',
      reason: 'next-step',
      target: { kind: 'entry', entryType: 'sleep', entryId: 'sleep-1', executionPath: [1] },
    },
    patch: mergePatch,
  };
}

describe('workflow terminal parent continuation contract', () => {
  it('canonicalizes, hashes, binds, and deeply copies a sleep continuation', () => {
    const snapshot = parentSnapshot();
    const contract = createWorkflowTerminalParentContinuationContract(nextSleepSpec(snapshot));
    expect(contract.contractHash).toMatch(/^sha256:[a-f0-9]{64}$/);
    expect(() =>
      validateWorkflowTerminalParentContinuationBinding(contract, {
        effect: effect(),
        parentRevision: 'revision-1',
        parentWorkflowName: 'parent',
        parentSnapshot: snapshot,
        executionMode: 'continuous',
      }),
    ).not.toThrow();

    const copy = copyWorkflowTerminalParentContinuationContract(contract);
    if (copy.source.kind !== 'step' || !('target' in copy.action) || !('executionPath' in copy.action.target)) {
      throw new Error('invalid fixture');
    }
    copy.source.executionPath[0] = 99;
    copy.action.target.executionPath[0] = 99;
    expect(contract.source.kind === 'step' && contract.source.executionPath).toEqual([0]);
    expect(
      'target' in contract.action && 'executionPath' in contract.action.target && contract.action.target.executionPath,
    ).toEqual([1]);
  });

  it('normalizes negative zero before hashing and JSON replay', () => {
    const snapshot = parentSnapshot();
    const negative = createWorkflowTerminalParentContinuationContract({
      ...nextSleepSpec(snapshot),
      source: { kind: 'step', stepId: 'nested', executionPath: [-0] },
    });
    const ordinary = createWorkflowTerminalParentContinuationContract(nextSleepSpec(snapshot));
    expect(negative.contractHash).toBe(ordinary.contractHash);
    if (negative.source.kind !== 'step') throw new Error('invalid fixture');
    expect(Object.is(negative.source.executionPath[0], -0)).toBe(false);
    const replay = JSON.parse(JSON.stringify(negative));
    expect(() => validateWorkflowTerminalParentContinuationIntegrity(replay)).not.toThrow();
    expect(replay).toEqual(negative);
  });

  it('rejects record and array proxies without invoking their reflection traps', () => {
    const countingHandler = <T extends object>(increment: () => void): ProxyHandler<T> => ({
      get(target, property, receiver) {
        increment();
        return Reflect.get(target, property, receiver);
      },
      getOwnPropertyDescriptor(target, property) {
        increment();
        return Reflect.getOwnPropertyDescriptor(target, property);
      },
      getPrototypeOf(target) {
        increment();
        return Reflect.getPrototypeOf(target);
      },
      ownKeys(target) {
        increment();
        return Reflect.ownKeys(target);
      },
    });

    let recordTrapCalls = 0;
    const recordProxy = new Proxy(
      nextSleepSpec(),
      countingHandler(() => {
        recordTrapCalls++;
      }),
    );
    expect(() => createWorkflowTerminalParentContinuationContract(recordProxy)).toThrow(/must not be a proxy/);
    expect(recordTrapCalls).toBe(0);

    let arrayTrapCalls = 0;
    const pathProxy = new Proxy(
      [0],
      countingHandler(() => {
        arrayTrapCalls++;
      }),
    );
    expect(() =>
      createWorkflowTerminalParentContinuationContract({
        ...nextSleepSpec(),
        source: { kind: 'step', stepId: 'nested', executionPath: pathProxy },
      }),
    ).toThrow(/must not be a proxy/);
    expect(arrayTrapCalls).toBe(0);

    const snapshot = parentSnapshot();
    const contract = createWorkflowTerminalParentContinuationContract(nextSleepSpec(snapshot));
    let contextTrapCalls = 0;
    snapshot.context = new Proxy(
      snapshot.context,
      countingHandler(() => {
        contextTrapCalls++;
      }),
    );
    expect(() =>
      validateWorkflowTerminalParentContinuationBinding(contract, {
        effect: effect(),
        parentRevision: 'revision-1',
        parentWorkflowName: 'parent',
        parentSnapshot: snapshot,
        executionMode: 'continuous',
      }),
    ).toThrow(/must not be a proxy/);
    expect(contextTrapCalls).toBe(0);
  });

  it('rejects non-enumerable source and active-path evidence that JSON persistence would drop', () => {
    const snapshot = parentSnapshot();
    const contract = createWorkflowTerminalParentContinuationContract(nextSleepSpec(snapshot));

    const hiddenSource = structuredClone(snapshot);
    const sourceState = hiddenSource.context.nested;
    Object.defineProperty(hiddenSource.context, 'nested', {
      configurable: true,
      enumerable: false,
      value: sourceState,
      writable: true,
    });
    expect(() =>
      validateWorkflowTerminalParentContinuationBinding(contract, {
        effect: effect(),
        parentRevision: 'revision-1',
        parentWorkflowName: 'parent',
        parentSnapshot: hiddenSource,
        executionMode: 'continuous',
      }),
    ).toThrow(/non-enumerable/);

    const hiddenActivePath = structuredClone(snapshot);
    Object.defineProperty(hiddenActivePath.activeStepsPath, 'nested', {
      configurable: true,
      enumerable: false,
      value: [0],
      writable: true,
    });
    expect(() =>
      validateWorkflowTerminalParentContinuationBinding(contract, {
        effect: effect(),
        parentRevision: 'revision-1',
        parentWorkflowName: 'parent',
        parentSnapshot: hiddenActivePath,
        executionMode: 'continuous',
      }),
    ).toThrow(/non-enumerable/);
  });

  it('does not read inherited parent-context accessors as source state', () => {
    const snapshot = parentSnapshot();
    const contract = createWorkflowTerminalParentContinuationContract(nextSleepSpec(snapshot));
    delete snapshot.context.nested;
    let getterCalls = 0;
    const previous = Object.getOwnPropertyDescriptor(Object.prototype, 'nested');
    Object.defineProperty(Object.prototype, 'nested', {
      configurable: true,
      get() {
        getterCalls++;
        return { status: 'running', metadata: { nestedRunId: 'child-run' } };
      },
    });
    try {
      expect(() =>
        validateWorkflowTerminalParentContinuationBinding(contract, {
          effect: effect(),
          parentRevision: 'revision-1',
          parentWorkflowName: 'parent',
          parentSnapshot: snapshot,
          executionMode: 'continuous',
        }),
      ).toThrow(/must be a data object/);
      expect(getterCalls).toBe(0);
    } finally {
      if (previous) Object.defineProperty(Object.prototype, 'nested', previous);
      else delete (Object.prototype as Record<string, unknown>).nested;
    }
  });

  it('rejects forged hashes and revision, status, graph, or effect binding drift', () => {
    const snapshot = parentSnapshot();
    const contract = createWorkflowTerminalParentContinuationContract(nextSleepSpec(snapshot));
    expect(() =>
      validateWorkflowTerminalParentContinuationIntegrity({ ...contract, contractHash: `sha256:${'0'.repeat(64)}` }),
    ).toThrow(/integrity/);
    expect(() =>
      validateWorkflowTerminalParentContinuationBinding(contract, {
        effect: effect(),
        parentRevision: 'revision-2',
        parentWorkflowName: 'parent',
        parentSnapshot: snapshot,
        executionMode: 'continuous',
      }),
    ).toThrow(/binding conflict/);
    expect(() =>
      validateWorkflowTerminalParentContinuationBinding(contract, {
        effect: effect(),
        parentRevision: 'revision-1',
        parentWorkflowName: 'parent',
        parentSnapshot: { ...snapshot, status: 'waiting' },
        executionMode: 'continuous',
      }),
    ).toThrow(/binding conflict/);
    const changedGraph = parentSnapshot();
    changedGraph.serializedStepGraph[1] = { type: 'sleep', id: 'sleep-1', duration: 20 };
    expect(() =>
      validateWorkflowTerminalParentContinuationBinding(contract, {
        effect: effect(),
        parentRevision: 'revision-1',
        parentWorkflowName: 'parent',
        parentSnapshot: changedGraph,
        executionMode: 'continuous',
      }),
    ).toThrow(/binding conflict/);
    expect(() =>
      validateWorkflowTerminalParentContinuationBinding(contract, {
        effect: effect([0], 'wrong'),
        parentRevision: 'revision-1',
        parentWorkflowName: 'parent',
        parentSnapshot: snapshot,
        executionMode: 'continuous',
      }),
    ).toThrow(/source step conflict/);
  });

  it('enforces action, terminal status, and patch matrices', () => {
    const spec = nextSleepSpec();
    expect(() =>
      createWorkflowTerminalParentContinuationContract({
        ...spec,
        childTerminalStatus: 'failed',
      }),
    ).toThrow(/failed children/);
    expect(() =>
      createWorkflowTerminalParentContinuationContract({
        ...spec,
        action: { kind: 'finish-parent', reason: 'parent-end' },
      }),
    ).toThrow(/patch status|preserve parent run state|parent-end/);
    expect(() =>
      createWorkflowTerminalParentContinuationContract({
        ...spec,
        action: {
          kind: 'run-entry',
          reason: 'loop-continue',
          target: { kind: 'container', containerType: 'loop', executionPath: [2] },
          loopDecision: {
            loopType: 'dowhile',
            conditionResult: false,
            previousIterationCount: 0,
            nextIterationCount: 1,
          },
        },
        patch: { ...mergePatch, loopWrite: { kind: 'set-iteration', stepId: 'loop', iterationCount: 1 } },
      }),
    ).toThrow(/contradicts/);
  });

  it('allows terminal-parent noop only with no patch', () => {
    const snapshot = parentSnapshot('success');
    const contract = createWorkflowTerminalParentContinuationContract({
      version: 1,
      terminalEffectKey: 'effect',
      terminalEffectPayloadHash: `sha256:${'d'.repeat(64)}`,
      executionMode: 'continuous',
      expectedParentRevision: 'revision-1',
      graphFingerprint: createWorkflowTerminalGraphFingerprint(snapshot.serializedStepGraph),
      childTerminalStatus: 'success',
      observedParentStatus: 'success',
      source: { kind: 'step', stepId: 'nested', executionPath: [0] },
      action: { kind: 'noop', reason: 'already-terminal' },
      patch: { kind: 'none' },
    });
    expect(contract.action.kind).toBe('noop');

    const foreachSnapshot = parentSnapshot('success');
    delete foreachSnapshot.context.each;
    const foreachNoop = createWorkflowTerminalParentContinuationContract({
      ...nextSleepSpec(foreachSnapshot),
      observedParentStatus: 'success',
      source: { kind: 'foreach-iteration', stepId: 'each', containerPath: [3], iterationIndex: 1 },
      action: { kind: 'noop', reason: 'already-terminal' },
      patch: { kind: 'none' },
    });
    expect(() =>
      validateWorkflowTerminalParentContinuationBinding(foreachNoop, {
        effect: effect([3, 1], 'each'),
        parentRevision: 'revision-1',
        parentWorkflowName: 'parent',
        parentSnapshot: foreachSnapshot,
        executionMode: 'continuous',
      }),
    ).not.toThrow();
  });

  it('binds an exact foreach iteration and rejects out-of-bounds coordinates', () => {
    const snapshot = parentSnapshot();
    const contract = createWorkflowTerminalParentContinuationContract({
      ...nextSleepSpec(snapshot),
      source: { kind: 'foreach-iteration', stepId: 'each', containerPath: [3], iterationIndex: 1 },
      action: {
        kind: 'wait',
        reason: 'foreach-aggregation',
        coordinate: { kind: 'container', containerType: 'foreach', executionPath: [3] },
      },
    });
    expect(() =>
      validateWorkflowTerminalParentContinuationBinding(contract, {
        effect: effect([3, 1], 'each'),
        parentRevision: 'revision-1',
        parentWorkflowName: 'parent',
        parentSnapshot: snapshot,
        executionMode: 'continuous',
      }),
    ).not.toThrow();
    const notStarted = structuredClone(snapshot);
    (notStarted.context.each as any).output = [null];
    expect(() =>
      validateWorkflowTerminalParentContinuationBinding(contract, {
        effect: effect([3, 1], 'each'),
        parentRevision: 'revision-1',
        parentWorkflowName: 'parent',
        parentSnapshot: notStarted,
        executionMode: 'continuous',
      }),
    ).toThrow(/iteration was not started/);
    const sparse = structuredClone(snapshot);
    (sparse.context.each as any).output = new Array(2);
    expect(() =>
      validateWorkflowTerminalParentContinuationBinding(contract, {
        effect: effect([3, 1], 'each'),
        parentRevision: 'revision-1',
        parentWorkflowName: 'parent',
        parentSnapshot: sparse,
        executionMode: 'continuous',
      }),
    ).toThrow(/dense and data-only/);
    const oversized = structuredClone(snapshot);
    const oversizedPayload: unknown[] = [];
    oversizedPayload.length = 16_385;
    (oversized.context.each as any).payload = oversizedPayload;
    expect(() =>
      validateWorkflowTerminalParentContinuationBinding(contract, {
        effect: effect([3, 1], 'each'),
        parentRevision: 'revision-1',
        parentWorkflowName: 'parent',
        parentSnapshot: oversized,
        executionMode: 'continuous',
      }),
    ).toThrow(/payload has an invalid length/);
    const nonEnumerable = structuredClone(snapshot);
    Object.defineProperty((nonEnumerable.context.each as any).output, '1', {
      configurable: true,
      enumerable: false,
      value: null,
      writable: true,
    });
    expect(() =>
      validateWorkflowTerminalParentContinuationBinding(contract, {
        effect: effect([3, 1], 'each'),
        parentRevision: 'revision-1',
        parentWorkflowName: 'parent',
        parentSnapshot: nonEnumerable,
        executionMode: 'continuous',
      }),
    ).toThrow(/dense and data-only/);
    const hiddenOwnership = structuredClone(snapshot);
    Object.defineProperty((hiddenOwnership.context.each as any).metadata.__workflow_meta.iterationRunIds, '1', {
      configurable: true,
      enumerable: false,
      value: 'child-run',
      writable: true,
    });
    expect(() =>
      validateWorkflowTerminalParentContinuationBinding(contract, {
        effect: effect([3, 1], 'each'),
        parentRevision: 'revision-1',
        parentWorkflowName: 'parent',
        parentSnapshot: hiddenOwnership,
        executionMode: 'continuous',
      }),
    ).toThrow(/ownership sidecar is invalid/);
    const accessor = structuredClone(snapshot);
    let getterCalls = 0;
    Object.defineProperty((accessor.context.each as any).output, '1', {
      configurable: true,
      enumerable: true,
      get() {
        getterCalls++;
        return null;
      },
    });
    expect(() =>
      validateWorkflowTerminalParentContinuationBinding(contract, {
        effect: effect([3, 1], 'each'),
        parentRevision: 'revision-1',
        parentWorkflowName: 'parent',
        parentSnapshot: accessor,
        executionMode: 'continuous',
      }),
    ).toThrow(/dense and data-only/);
    expect(getterCalls).toBe(0);
    const missingOutput = structuredClone(snapshot);
    delete (missingOutput.context.each as any).output;
    expect(() =>
      validateWorkflowTerminalParentContinuationBinding(contract, {
        effect: effect([3, 1], 'each'),
        parentRevision: 'revision-1',
        parentWorkflowName: 'parent',
        parentSnapshot: missingOutput,
        executionMode: 'continuous',
      }),
    ).toThrow(/dense data-only array/);
    const outOfBounds = createWorkflowTerminalParentContinuationContract({
      ...nextSleepSpec(snapshot),
      source: { kind: 'foreach-iteration', stepId: 'each', containerPath: [3], iterationIndex: 2 },
      action: {
        kind: 'wait',
        reason: 'foreach-aggregation',
        coordinate: { kind: 'container', containerType: 'foreach', executionPath: [3] },
      },
    });
    expect(() =>
      validateWorkflowTerminalParentContinuationBinding(outOfBounds, {
        effect: effect([3, 2], 'each'),
        parentRevision: 'revision-1',
        parentWorkflowName: 'parent',
        parentSnapshot: snapshot,
        executionMode: 'continuous',
      }),
    ).toThrow(/out of bounds/);
  });

  it('rejects cross-effect substitution and non-adjacent sequential targets', () => {
    const snapshot = parentSnapshot();
    const contract = createWorkflowTerminalParentContinuationContract(nextSleepSpec(snapshot));
    expect(() =>
      validateWorkflowTerminalParentContinuationBinding(contract, {
        effect: { ...effect(), effectKey: 'different-effect' },
        parentRevision: 'revision-1',
        parentWorkflowName: 'parent',
        parentSnapshot: snapshot,
        executionMode: 'continuous',
      }),
    ).toThrow(/binding conflict/);

    const backEdge = createWorkflowTerminalParentContinuationContract({
      ...nextSleepSpec(snapshot),
      action: {
        kind: 'run-entry',
        reason: 'next-step',
        target: { kind: 'step', stepId: 'nested', executionPath: [0] },
      },
    });
    expect(() =>
      validateWorkflowTerminalParentContinuationBinding(backEdge, {
        effect: effect(),
        parentRevision: 'revision-1',
        parentWorkflowName: 'parent',
        parentSnapshot: snapshot,
        executionMode: 'continuous',
      }),
    ).toThrow(/immediate sequential successor/);
  });

  it('binds success finish only at the final entry and cancellation as an explicit abort', () => {
    const finalParent = parentSnapshot();
    finalParent.serializedStepGraph = [{ type: 'step', step: { id: 'nested', component: 'WORKFLOW' } }];
    finalParent.activeStepsPath = { nested: [0] };
    const successPatch = {
      ...mergePatch,
      parentRunWrite: {
        kind: 'set',
        status: 'success',
        resultSource: 'source-coordinate',
        activePathSource: 'source-coordinate',
      },
    };
    const finish = createWorkflowTerminalParentContinuationContract({
      ...nextSleepSpec(finalParent),
      action: { kind: 'finish-parent', reason: 'parent-end' },
      patch: successPatch,
    });
    expect(() =>
      validateWorkflowTerminalParentContinuationBinding(finish, {
        effect: effect(),
        parentRevision: 'revision-1',
        parentWorkflowName: 'parent',
        parentSnapshot: finalParent,
        executionMode: 'continuous',
      }),
    ).not.toThrow();

    const parent = parentSnapshot();
    const cancelPatch = {
      ...mergePatch,
      parentRunWrite: {
        kind: 'set',
        status: 'canceled',
        resultSource: 'source-coordinate',
        activePathSource: 'source-coordinate',
      },
    };
    const cancel = createWorkflowTerminalParentContinuationContract({
      ...nextSleepSpec(parent),
      childTerminalStatus: 'canceled',
      action: { kind: 'cancel-parent', reason: 'child-canceled' },
      patch: cancelPatch,
    });
    expect(() =>
      validateWorkflowTerminalParentContinuationBinding(cancel, {
        effect: effect([0], 'nested', 'canceled'),
        parentRevision: 'revision-1',
        parentWorkflowName: 'parent',
        parentSnapshot: parent,
        executionMode: 'continuous',
      }),
    ).not.toThrow();
    expect(() =>
      createWorkflowTerminalParentContinuationContract({
        ...nextSleepSpec(parent),
        executionMode: 'per-step',
      }),
    ).toThrow(/executionMode must be continuous/);
  });

  it('binds foreach continue, wait, and complete to exact locked iteration state', () => {
    const continuing = parentSnapshot();
    continuing.context.each = {
      status: 'running',
      payload: ['a', 'b', 'c'],
      output: [null],
      startedAt: 1,
      metadata: { __workflow_meta: { iterationRunIds: { '0': 'child-run' } } },
    } as WorkflowRunState['context'][string];
    const continueContract = createWorkflowTerminalParentContinuationContract({
      ...nextSleepSpec(continuing),
      source: { kind: 'foreach-iteration', stepId: 'each', containerPath: [3], iterationIndex: 0 },
      action: {
        kind: 'run-entry',
        reason: 'foreach-continue',
        target: { kind: 'foreach-iteration', stepId: 'each', containerPath: [3], iterationIndex: 1 },
      },
    });
    expect(() =>
      validateWorkflowTerminalParentContinuationBinding(continueContract, {
        effect: effect([3, 0], 'each'),
        parentRevision: 'revision-1',
        parentWorkflowName: 'parent',
        parentSnapshot: continuing,
        executionMode: 'continuous',
      }),
    ).not.toThrow();
    const futureTerminalState = structuredClone(continuing);
    (futureTerminalState.context.each as any).metadata.__workflow_meta.terminalIterationStates = { '1': 'success' };
    expect(() =>
      validateWorkflowTerminalParentContinuationBinding(continueContract, {
        effect: effect([3, 0], 'each'),
        parentRevision: 'revision-1',
        parentWorkflowName: 'parent',
        parentSnapshot: futureTerminalState,
        executionMode: 'continuous',
      }),
    ).toThrow(/state sidecar is invalid/);

    const complete = parentSnapshot();
    complete.context.each = {
      status: 'running',
      payload: ['a', 'b'],
      // Index 0 is a legitimate successful user output that happens to contain
      // `status: suspended`; index 1 is the resumed source's pre-patch value.
      // Sidecar/source truth must win over both raw shapes.
      output: [{ status: 'suspended' }, { status: 'suspended' }],
      startedAt: 1,
      metadata: {
        __workflow_meta: {
          terminalIterationStates: { '0': 'success' },
          iterationRunIds: { '1': 'child-run' },
        },
      },
    } as WorkflowRunState['context'][string];
    const completeContract = createWorkflowTerminalParentContinuationContract({
      ...nextSleepSpec(complete),
      source: { kind: 'foreach-iteration', stepId: 'each', containerPath: [3], iterationIndex: 1 },
      action: {
        kind: 'complete-entry',
        reason: 'foreach-complete',
        target: { kind: 'container', containerType: 'foreach', executionPath: [3] },
      },
    });
    expect(() =>
      validateWorkflowTerminalParentContinuationBinding(completeContract, {
        effect: effect([3, 1], 'each'),
        parentRevision: 'revision-1',
        parentWorkflowName: 'parent',
        parentSnapshot: complete,
        executionMode: 'continuous',
      }),
    ).not.toThrow();
    const falseSuspend = createWorkflowTerminalParentContinuationContract({
      ...nextSleepSpec(complete),
      source: { kind: 'foreach-iteration', stepId: 'each', containerPath: [3], iterationIndex: 1 },
      action: {
        kind: 'suspend-parent',
        reason: 'foreach-suspended',
        target: { kind: 'container', containerType: 'foreach', executionPath: [3] },
      },
      patch: {
        ...mergePatch,
        parentRunWrite: {
          kind: 'set-suspended',
          resultSource: 'aggregate-container',
          activePathSource: 'source-coordinate',
          suspendedPathsSource: 'aggregate-container',
          resumeLabelsSource: 'aggregate-container',
        },
      },
    });
    expect(() =>
      validateWorkflowTerminalParentContinuationBinding(falseSuspend, {
        effect: effect([3, 1], 'each'),
        parentRevision: 'revision-1',
        parentWorkflowName: 'parent',
        parentSnapshot: complete,
        executionMode: 'continuous',
      }),
    ).toThrow(/not ready to suspend/);

    const suspended = parentSnapshot();
    suspended.context.each = {
      status: 'running',
      payload: ['a', 'b'],
      output: [
        {
          status: 'suspended',
          suspendPayload: { __workflow_meta: { resumeLabels: { resume: { stepId: 'each', foreachIndex: 0 } } } },
        },
        null,
      ],
      startedAt: 1,
      metadata: { __workflow_meta: { iterationRunIds: { '1': 'child-run' } } },
    } as WorkflowRunState['context'][string];
    const suspendContract = createWorkflowTerminalParentContinuationContract({
      ...nextSleepSpec(suspended),
      source: { kind: 'foreach-iteration', stepId: 'each', containerPath: [3], iterationIndex: 1 },
      action: {
        kind: 'suspend-parent',
        reason: 'foreach-suspended',
        target: { kind: 'container', containerType: 'foreach', executionPath: [3] },
      },
      patch: {
        ...mergePatch,
        parentRunWrite: {
          kind: 'set-suspended',
          resultSource: 'aggregate-container',
          activePathSource: 'source-coordinate',
          suspendedPathsSource: 'aggregate-container',
          resumeLabelsSource: 'aggregate-container',
        },
      },
    });
    expect(() =>
      validateWorkflowTerminalParentContinuationBinding(suspendContract, {
        effect: effect([3, 1], 'each'),
        parentRevision: 'revision-1',
        parentWorkflowName: 'parent',
        parentSnapshot: suspended,
        executionMode: 'continuous',
      }),
    ).not.toThrow();

    const overrun = structuredClone(suspended);
    (overrun.context.each as any).output.push({
      status: 'suspended',
      suspendPayload: {
        __workflow_meta: { resumeLabels: { phantom: { stepId: 'each', foreachIndex: 2 } } },
      },
    });
    expect(() =>
      validateWorkflowTerminalParentContinuationBinding(suspendContract, {
        effect: effect([3, 1], 'each'),
        parentRevision: 'revision-1',
        parentWorkflowName: 'parent',
        parentSnapshot: overrun,
        executionMode: 'continuous',
      }),
    ).toThrow(/output exceeds its input payload/);

    const corrupt = structuredClone(suspended);
    (corrupt.context.each as any).metadata.__workflow_meta.terminalIterationStates = { '0': 'bogus' };
    expect(() =>
      validateWorkflowTerminalParentContinuationBinding(suspendContract, {
        effect: effect([3, 1], 'each'),
        parentRevision: 'revision-1',
        parentWorkflowName: 'parent',
        parentSnapshot: corrupt,
        executionMode: 'continuous',
      }),
    ).toThrow(/state sidecar is invalid/);

    const hiddenState = structuredClone(suspended);
    (hiddenState.context.each as any).metadata.__workflow_meta.terminalIterationStates = {};
    Object.defineProperty((hiddenState.context.each as any).metadata.__workflow_meta.terminalIterationStates, '0', {
      configurable: true,
      enumerable: false,
      value: 'success',
      writable: true,
    });
    expect(() =>
      validateWorkflowTerminalParentContinuationBinding(suspendContract, {
        effect: effect([3, 1], 'each'),
        parentRevision: 'revision-1',
        parentWorkflowName: 'parent',
        parentSnapshot: hiddenState,
        executionMode: 'continuous',
      }),
    ).toThrow(/state sidecar is invalid/);
  });

  it('rejects stale failed or canceled foreach child ownership before parent terminalization', () => {
    const snapshot = parentSnapshot();
    const failedPatch = {
      ...mergePatch,
      parentRunWrite: {
        kind: 'set',
        status: 'failed',
        resultSource: 'source-coordinate',
        activePathSource: 'source-coordinate',
      },
    };
    const cancelPatch = {
      ...mergePatch,
      parentRunWrite: {
        kind: 'set',
        status: 'canceled',
        resultSource: 'source-coordinate',
        activePathSource: 'source-coordinate',
      },
    };
    for (const terminalStatus of ['failed', 'canceled'] as const) {
      const contract = createWorkflowTerminalParentContinuationContract({
        ...nextSleepSpec(snapshot),
        childTerminalStatus: terminalStatus,
        source: { kind: 'foreach-iteration', stepId: 'each', containerPath: [3], iterationIndex: 0 },
        action:
          terminalStatus === 'failed'
            ? { kind: 'fail-parent', reason: 'parent-fail' }
            : { kind: 'cancel-parent', reason: 'child-canceled' },
        patch: terminalStatus === 'failed' ? failedPatch : cancelPatch,
      });
      expect(() =>
        validateWorkflowTerminalParentContinuationBinding(contract, {
          effect: { ...effect([3, 0], 'each', terminalStatus), runId: 'stale-child-run' },
          parentRevision: 'revision-1',
          parentWorkflowName: 'parent',
          parentSnapshot: snapshot,
          executionMode: 'continuous',
        }),
      ).toThrow(/not owned by the terminal child run/);
    }
  });

  it('commits bounded graph-conflict quarantine without pretending the source resolves', () => {
    const snapshot = parentSnapshot();
    const contract = createWorkflowTerminalParentContinuationContract({
      ...nextSleepSpec(snapshot),
      source: { kind: 'step', stepId: 'missing', executionPath: [9] },
      action: {
        kind: 'quarantine',
        reason: 'graph-conflict',
        conflictDigest: `sha256:${'a'.repeat(64)}`,
      },
      patch: { kind: 'none' },
    });
    expect(() =>
      validateWorkflowTerminalParentContinuationBinding(contract, {
        effect: effect([9], 'missing'),
        parentRevision: 'revision-1',
        parentWorkflowName: 'parent',
        parentSnapshot: snapshot,
        executionMode: 'continuous',
      }),
    ).not.toThrow();
  });

  it('binds branch complete versus wait to the locked sibling states', () => {
    const snapshot = parentSnapshot();
    snapshot.serializedStepGraph = [
      {
        type: 'parallel',
        steps: [
          { type: 'step', step: { id: 'left' } },
          { type: 'step', step: { id: 'right' } },
        ],
      },
      { type: 'sleep', id: 'sleep-1', duration: 10 },
    ];
    snapshot.context.left = {
      status: 'running',
      payload: {},
      startedAt: 1,
      metadata: { nestedRunId: 'child-run' },
    } as WorkflowRunState['context'][string];
    snapshot.context.right = {
      status: 'success',
      payload: {},
      output: 'right',
      startedAt: 1,
      endedAt: 2,
    } as WorkflowRunState['context'][string];
    snapshot.activeStepsPath = { left: [0, 0] };
    const complete = createWorkflowTerminalParentContinuationContract({
      ...nextSleepSpec(snapshot),
      source: { kind: 'step', stepId: 'left', executionPath: [0, 0] },
      action: {
        kind: 'complete-entry',
        reason: 'parallel-continue',
        target: { kind: 'container', containerType: 'parallel', executionPath: [0] },
      },
    });
    expect(() =>
      validateWorkflowTerminalParentContinuationBinding(complete, {
        effect: effect([0, 0], 'left'),
        parentRevision: 'revision-1',
        parentWorkflowName: 'parent',
        parentSnapshot: snapshot,
        executionMode: 'continuous',
      }),
    ).not.toThrow();

    snapshot.context.right = { status: 'running', payload: {}, startedAt: 1 } as WorkflowRunState['context'][string];
    const waiting = createWorkflowTerminalParentContinuationContract({
      ...nextSleepSpec(snapshot),
      source: { kind: 'step', stepId: 'left', executionPath: [0, 0] },
      action: {
        kind: 'wait',
        reason: 'parallel-aggregation',
        coordinate: { kind: 'container', containerType: 'parallel', executionPath: [0] },
      },
    });
    expect(() =>
      validateWorkflowTerminalParentContinuationBinding(waiting, {
        effect: effect([0, 0], 'left'),
        parentRevision: 'revision-1',
        parentWorkflowName: 'parent',
        parentSnapshot: snapshot,
        executionMode: 'continuous',
      }),
    ).not.toThrow();

    snapshot.context.right = {
      status: 'suspended',
      payload: {},
      startedAt: 1,
      suspendedAt: 2,
      suspendPayload: { __workflow_meta: { resumeLabels: { resume: { stepId: 'right' } } } },
    } as WorkflowRunState['context'][string];
    const suspend = createWorkflowTerminalParentContinuationContract({
      ...nextSleepSpec(snapshot),
      source: { kind: 'step', stepId: 'left', executionPath: [0, 0] },
      action: {
        kind: 'suspend-parent',
        reason: 'branch-suspended',
        target: { kind: 'container', containerType: 'parallel', executionPath: [0] },
      },
      patch: {
        ...mergePatch,
        parentRunWrite: {
          kind: 'set-suspended',
          resultSource: 'aggregate-container',
          activePathSource: 'source-coordinate',
          suspendedPathsSource: 'aggregate-container',
          resumeLabelsSource: 'aggregate-container',
        },
      },
    });
    expect(() =>
      validateWorkflowTerminalParentContinuationBinding(suspend, {
        effect: effect([0, 0], 'left'),
        parentRevision: 'revision-1',
        parentWorkflowName: 'parent',
        parentSnapshot: snapshot,
        executionMode: 'continuous',
      }),
    ).not.toThrow();
  });

  it('treats a skipped conditional sibling as complete-neutral', () => {
    const snapshot = parentSnapshot();
    snapshot.serializedStepGraph = [
      {
        type: 'conditional',
        steps: [
          { type: 'step', step: { id: 'selected' } },
          { type: 'step', step: { id: 'skipped' } },
        ],
        serializedConditions: [
          { id: 'selected-condition', fn: '() => true' },
          { id: 'skipped-condition', fn: '() => false' },
        ],
      },
    ];
    snapshot.context.selected = {
      status: 'running',
      payload: {},
      startedAt: 1,
      metadata: { nestedRunId: 'child-run' },
    } as WorkflowRunState['context'][string];
    snapshot.context.skipped = { status: 'skipped' } as unknown as WorkflowRunState['context'][string];
    snapshot.activeStepsPath = { selected: [0, 0] };
    const contract = createWorkflowTerminalParentContinuationContract({
      ...nextSleepSpec(snapshot),
      source: { kind: 'step', stepId: 'selected', executionPath: [0, 0] },
      action: {
        kind: 'complete-entry',
        reason: 'conditional-continue',
        target: { kind: 'container', containerType: 'conditional', executionPath: [0] },
      },
    });
    expect(() =>
      validateWorkflowTerminalParentContinuationBinding(contract, {
        effect: effect([0, 0], 'selected'),
        parentRevision: 'revision-1',
        parentWorkflowName: 'parent',
        parentSnapshot: snapshot,
        executionMode: 'continuous',
      }),
    ).not.toThrow();
  });
});
