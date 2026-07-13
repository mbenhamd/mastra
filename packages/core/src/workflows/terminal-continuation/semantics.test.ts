import { describe, expect, it } from 'vitest';
import type { WorkflowRunState, WorkflowTerminalEffectRecord, WorkflowTerminalSnapshotRecord } from '../types';
import { createWorkflowTerminalParentContinuationContract } from './contract';
import { createWorkflowTerminalGraphFingerprint } from './graph-fingerprint';
import {
  applyWorkflowTerminalParentContinuationPatch,
  MAX_WORKFLOW_TERMINAL_CONTINUATION_DATA_BYTES,
  MAX_WORKFLOW_TERMINAL_CONTINUATION_DATA_DEPTH,
  MAX_WORKFLOW_TERMINAL_CONTINUATION_DATA_ENTRIES,
  MAX_WORKFLOW_TERMINAL_CONTINUATION_DATA_NODES,
  WorkflowTerminalContinuationStoredStateError,
  WORKFLOW_TERMINAL_FOREACH_STATE_KEY,
} from './semantics';

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

function parentSnapshot(): WorkflowRunState {
  return {
    runId: 'parent-run',
    status: 'running',
    value: { original: 'state' },
    context: {
      nested: {
        status: 'running',
        payload: { parentInput: true },
        startedAt: 10,
        metadata: { parent: true, nestedRunId: 'child-run' },
      },
      loop: {
        status: 'running',
        payload: {},
        startedAt: 10,
        metadata: { iterationCount: 0, nestedRunId: 'child-run' },
      },
      each: {
        status: 'running',
        payload: ['a', 'b'],
        output: [null, null],
        startedAt: 10,
        metadata: {
          owner: 'parent',
          __workflow_meta: { iterationRunIds: { '0': 'child-run', '1': 'child-run' } },
        },
      },
      __state: { stale: true },
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
    activeStepsPath: { nested: [0], each: [3] },
    suspendedPaths: {},
    resumeLabels: {},
    waitingPaths: {},
    requestContext: { parent: true, shared: 'parent' },
    timestamp: 10,
  };
}

function effect(
  terminalStatus: 'success' | 'failed' | 'canceled' = 'success',
  path = [0],
  stepId = 'nested',
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
    createdAt: 20,
    parentWorkflowName: 'parent',
    parentRunId: 'parent-run',
    parentStepId: stepId,
    parentExecutionPath: path,
  };
}

function retained(
  terminalStatus: 'success' | 'failed' | 'canceled' = 'success',
  result: unknown = {
    status: 'success',
    output: { answer: 42 },
    startedAt: 11,
    endedAt: 20,
    metadata: { child: true },
  },
): WorkflowTerminalSnapshotRecord {
  return {
    version: 1,
    workflowName: 'child',
    runId: 'child-run',
    terminalStatus,
    createdAt: 20,
    snapshot: {
      runId: 'child-run',
      status: terminalStatus,
      result,
      ...(terminalStatus === 'failed' ? { error: { message: 'boom', name: 'Error' } } : {}),
      value: {},
      context: { __state: { final: true } } as WorkflowRunState['context'],
      serializedStepGraph: [],
      activePaths: [],
      activeStepsPath: {},
      suspendedPaths: {},
      resumeLabels: {},
      waitingPaths: {},
      requestContext: { child: true, shared: 'child' },
      timestamp: 20,
    },
  };
}

function contractFor(snapshot: WorkflowRunState, overrides: Record<string, unknown> = {}) {
  return createWorkflowTerminalParentContinuationContract({
    version: 1,
    terminalEffectKey: 'effect',
    terminalEffectPayloadHash: `sha256:${'d'.repeat(64)}`,
    executionMode: 'continuous',
    expectedParentRevision: 'revision-1',
    graphFingerprint: createWorkflowTerminalGraphFingerprint(snapshot.serializedStepGraph),
    childTerminalStatus: 'success',
    observedParentStatus: 'running',
    source: { kind: 'step', stepId: 'nested', executionPath: [0] },
    action: {
      kind: 'run-entry',
      reason: 'next-step',
      target: { kind: 'entry', entryType: 'sleep', entryId: 'sleep-1', executionPath: [1] },
    },
    patch: mergePatch,
    ...overrides,
  });
}

describe('workflow terminal parent patch semantics', () => {
  it('applies scalar success without mutating either snapshot', () => {
    const parent = parentSnapshot();
    const child = retained();
    const beforeParent = structuredClone(parent);
    const beforeChild = structuredClone(child);
    const next = applyWorkflowTerminalParentContinuationPatch({
      contract: contractFor(parent),
      effect: effect(),
      parentRevision: 'revision-1',
      parentWorkflowName: 'parent',
      parentSnapshot: parent,
      retainedChild: child,
      storageTimestamp: 30,
      executionMode: 'continuous',
    });

    expect(parent).toEqual(beforeParent);
    expect(child).toEqual(beforeChild);
    expect(next.context.nested).toMatchObject({
      status: 'success',
      output: { answer: 42 },
      payload: { parentInput: true },
      startedAt: 10,
      endedAt: 20,
      metadata: { parent: true, child: true, nestedRunId: 'child-run' },
    });
    expect(next.context.__state).toEqual({ final: true });
    expect(next.value).toEqual({ final: true });
    expect(next.requestContext).toEqual({ parent: true, child: true, shared: 'child' });
    expect(next.activeStepsPath).toEqual({ each: [3] });
    expect(next.status).toBe('running');
    expect(next.timestamp).toBe(30);
  });

  it('rejects snapshot accessors and proxies before binding or structured cloning can execute them', () => {
    const parent = parentSnapshot();
    let getterCalls = 0;
    Object.defineProperty(parent.context, 'unrelated', {
      enumerable: true,
      get() {
        getterCalls++;
        return { status: 'success' };
      },
    });
    expect(() =>
      applyWorkflowTerminalParentContinuationPatch({
        contract: contractFor(parent),
        effect: effect(),
        parentRevision: 'revision-1',
        parentWorkflowName: 'parent',
        parentSnapshot: parent,
        retainedChild: retained(),
        storageTimestamp: 30,
        executionMode: 'continuous',
      }),
    ).toThrow(/accessor/);
    expect(getterCalls).toBe(0);

    const errorParent = parentSnapshot();
    const hostileError = new Error('hostile');
    let stackGetterCalls = 0;
    Object.defineProperty(hostileError, 'stack', {
      configurable: true,
      get() {
        stackGetterCalls++;
        return 'must not execute';
      },
    });
    errorParent.context.unrelated = { status: 'failed', error: hostileError } as any;
    expect(() =>
      applyWorkflowTerminalParentContinuationPatch({
        contract: contractFor(errorParent),
        effect: effect(),
        parentRevision: 'revision-1',
        parentWorkflowName: 'parent',
        parentSnapshot: errorParent,
        retainedChild: retained(),
        storageTimestamp: 30,
        executionMode: 'continuous',
      }),
    ).toThrow(/accessor/);
    expect(stackGetterCalls).toBe(0);

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
    const proxyParent = parentSnapshot();
    const patchInput = {
      contract: contractFor(proxyParent),
      effect: effect(),
      parentRevision: 'revision-1',
      parentWorkflowName: 'parent',
      parentSnapshot: proxyParent,
      retainedChild: retained(),
      storageTimestamp: 30,
      executionMode: 'continuous' as const,
    };
    let inputTrapCalls = 0;
    const inputProxy = new Proxy(
      patchInput,
      countingHandler(() => {
        inputTrapCalls++;
      }),
    );
    expect(() => applyWorkflowTerminalParentContinuationPatch(inputProxy)).toThrow(/must not be a proxy/);
    expect(inputTrapCalls).toBe(0);

    const nestedProxyParent = parentSnapshot();
    let nestedTrapCalls = 0;
    nestedProxyParent.context.unrelated = new Proxy(
      { status: 'success' },
      countingHandler(() => {
        nestedTrapCalls++;
      }),
    ) as any;
    expect(() =>
      applyWorkflowTerminalParentContinuationPatch({
        ...patchInput,
        contract: contractFor(nestedProxyParent),
        parentSnapshot: nestedProxyParent,
      }),
    ).toThrow(/contains a proxy/);
    expect(nestedTrapCalls).toBe(0);
  });

  it('sets parent failure state, result, error, and exact active path', () => {
    const parent = parentSnapshot();
    const child = retained('failed', {
      status: 'failed',
      error: { message: 'boom', name: 'Error' },
      startedAt: 11,
      endedAt: 20,
    });
    const failedPatch = {
      ...mergePatch,
      parentRunWrite: {
        kind: 'set',
        status: 'failed',
        resultSource: 'source-coordinate',
        activePathSource: 'source-coordinate',
      },
    };
    const next = applyWorkflowTerminalParentContinuationPatch({
      contract: contractFor(parent, {
        childTerminalStatus: 'failed',
        action: { kind: 'fail-parent', reason: 'parent-fail' },
        patch: failedPatch,
      }),
      effect: effect('failed'),
      parentRevision: 'revision-1',
      parentWorkflowName: 'parent',
      parentSnapshot: parent,
      retainedChild: child,
      storageTimestamp: 30,
      executionMode: 'continuous',
    });
    expect(next.status).toBe('failed');
    expect(next.result).toMatchObject({ status: 'failed', payload: { parentInput: true } });
    expect(next.error).toMatchObject({ message: 'boom' });
    expect(next.activePaths).toEqual([0]);
  });

  it('records foreach terminal status separately from raw undefined output', () => {
    const parent = parentSnapshot();
    const child = retained('success', {
      status: 'success',
      output: undefined,
      startedAt: 11,
      endedAt: 20,
    });
    const next = applyWorkflowTerminalParentContinuationPatch({
      contract: contractFor(parent, {
        source: { kind: 'foreach-iteration', stepId: 'each', containerPath: [3], iterationIndex: 1 },
        action: {
          kind: 'wait',
          reason: 'foreach-aggregation',
          coordinate: { kind: 'container', containerType: 'foreach', executionPath: [3] },
        },
      }),
      effect: effect('success', [3, 1], 'each'),
      parentRevision: 'revision-1',
      parentWorkflowName: 'parent',
      parentSnapshot: parent,
      retainedChild: child,
      storageTimestamp: 30,
      executionMode: 'continuous',
    });
    const each = next.context.each as Record<string, any>;
    expect(each.output[1]).toBeNull();
    expect(each.metadata.__workflow_meta[WORKFLOW_TERMINAL_FOREACH_STATE_KEY]).toEqual({ '1': 'success' });
    expect(next.activeStepsPath.each).toEqual([3]);

    const stored = JSON.parse(JSON.stringify(next));
    expect(stored.context.each.output[1]).toBeNull();
    expect(stored.context.each.metadata.__workflow_meta[WORKFLOW_TERMINAL_FOREACH_STATE_KEY]['1']).toBe('success');
  });

  it('attributes invalid foreach status and malformed sidecars to stored parent corruption', () => {
    for (const corruptParent of [
      (() => {
        const parent = parentSnapshot();
        (parent.context.each as Record<string, any>).status = 'invalid';
        return parent;
      })(),
      (() => {
        const parent = parentSnapshot();
        (parent.context.each as Record<string, any>).metadata.__workflow_meta[WORKFLOW_TERMINAL_FOREACH_STATE_KEY] = {
          '1': 'invalid',
        };
        return parent;
      })(),
    ]) {
      expect(() =>
        applyWorkflowTerminalParentContinuationPatch({
          contract: contractFor(corruptParent, {
            source: { kind: 'foreach-iteration', stepId: 'each', containerPath: [3], iterationIndex: 1 },
            action: {
              kind: 'wait',
              reason: 'foreach-aggregation',
              coordinate: { kind: 'container', containerType: 'foreach', executionPath: [3] },
            },
          }),
          effect: effect('success', [3, 1], 'each'),
          parentRevision: 'revision-1',
          parentWorkflowName: 'parent',
          parentSnapshot: corruptParent,
          retainedChild: retained(),
          storageTimestamp: 30,
          executionMode: 'continuous',
        }),
      ).toThrow(WorkflowTerminalContinuationStoredStateError);
    }
  });

  it('updates loop iteration metadata from the evaluated structural decision', () => {
    const parent = parentSnapshot();
    parent.activeStepsPath = { loop: [2] };
    const loopPatch = {
      ...mergePatch,
      loopWrite: { kind: 'set-iteration', stepId: 'loop', iterationCount: 1 },
    };
    const next = applyWorkflowTerminalParentContinuationPatch({
      contract: contractFor(parent, {
        source: { kind: 'step', stepId: 'loop', executionPath: [2] },
        action: {
          kind: 'run-entry',
          reason: 'loop-continue',
          target: { kind: 'container', containerType: 'loop', executionPath: [2] },
          loopDecision: {
            loopType: 'dowhile',
            conditionResult: true,
            previousIterationCount: 0,
            nextIterationCount: 1,
          },
        },
        patch: loopPatch,
      }),
      effect: effect('success', [2], 'loop'),
      parentRevision: 'revision-1',
      parentWorkflowName: 'parent',
      parentSnapshot: parent,
      retainedChild: retained(),
      storageTimestamp: 30,
      executionMode: 'continuous',
    });
    expect((next.context.loop as Record<string, any>).metadata.iterationCount).toBe(1);
    expect(next.activeStepsPath.loop).toEqual([2]);
  });

  it('fails closed when exact retained final state is unavailable', () => {
    const parent = parentSnapshot();
    const child = retained();
    delete child.snapshot.context.__state;
    expect(() =>
      applyWorkflowTerminalParentContinuationPatch({
        contract: contractFor(parent),
        effect: effect(),
        parentRevision: 'revision-1',
        parentWorkflowName: 'parent',
        parentSnapshot: parent,
        retainedChild: child,
        storageTimestamp: 30,
        executionMode: 'continuous',
      }),
    ).toThrow(/missing final context.__state/);
  });

  it('normalizes touched data to JSON semantics before merging request context and state', () => {
    const parent = parentSnapshot();
    parent.requestContext = { tenantId: 'tenant-a', parent: true };
    const child = retained();
    child.snapshot.requestContext = {
      tenantId: undefined,
      observedAt: new Date('2026-01-01T00:00:00.000Z'),
    };
    child.snapshot.context.__state = {
      nested: { missing: undefined, observedAt: new Date('2026-01-01T00:00:00.000Z') },
    } as WorkflowRunState['context'][string];
    const next = applyWorkflowTerminalParentContinuationPatch({
      contract: contractFor(parent),
      effect: effect(),
      parentRevision: 'revision-1',
      parentWorkflowName: 'parent',
      parentSnapshot: parent,
      retainedChild: child,
      storageTimestamp: 30,
      executionMode: 'continuous',
    });
    expect(next.requestContext).toEqual({
      tenantId: 'tenant-a',
      parent: true,
      observedAt: '2026-01-01T00:00:00.000Z',
    });
    expect(next.value).toEqual({ nested: { observedAt: '2026-01-01T00:00:00.000Z' } });
  });

  it('canonicalizes existing parent request context before applying the child overlay', () => {
    const parent = parentSnapshot();
    const parentRequestContext = Object.create(null) as Record<string, unknown>;
    parentRequestContext.observedAt = new Date('2025-12-31T23:59:59.000Z');
    parentRequestContext.missing = undefined;
    parentRequestContext.nested = [{ offset: -0, missing: undefined }, ['value']];
    Object.defineProperty(parentRequestContext, '__proto__', {
      enumerable: true,
      value: { safe: true },
      writable: true,
    });
    parent.requestContext = parentRequestContext;
    const child = retained();
    child.snapshot.requestContext = { child: true };

    const next = applyWorkflowTerminalParentContinuationPatch({
      contract: contractFor(parent),
      effect: effect(),
      parentRevision: 'revision-1',
      parentWorkflowName: 'parent',
      parentSnapshot: parent,
      retainedChild: child,
      storageTimestamp: 30,
      executionMode: 'continuous',
    });

    expect(next.requestContext).toMatchObject({
      observedAt: '2025-12-31T23:59:59.000Z',
      nested: [{ offset: 0 }, ['value']],
      child: true,
    });
    expect(Object.hasOwn(next.requestContext!, '__proto__')).toBe(true);
    expect(Object.getOwnPropertyDescriptor(next.requestContext!, '__proto__')?.value).toEqual({ safe: true });
    expect(Object.is((next.requestContext!.nested as any)[0].offset, -0)).toBe(false);
    expect(JSON.parse(JSON.stringify(next.requestContext))).toEqual(next.requestContext);
    expect(Object.prototype).not.toHaveProperty('safe');

    for (const invalid of [new Map([['key', 'value']]), { malformed: '\ud800' }]) {
      const invalidParent = parentSnapshot();
      invalidParent.requestContext = { invalid };
      expect(() =>
        applyWorkflowTerminalParentContinuationPatch({
          contract: contractFor(invalidParent),
          effect: effect(),
          parentRevision: 'revision-1',
          parentWorkflowName: 'parent',
          parentSnapshot: invalidParent,
          retainedChild: retained(),
          storageTimestamp: 30,
          executionMode: 'continuous',
        }),
      ).toThrow(/non-JSON object|malformed Unicode/);
    }
  });

  it('enforces provenance-specific bounded traversals for depth, cycles, amplification, entries, and bytes', () => {
    const nestedData = (depth: number): Record<string, unknown> => {
      let value: unknown = true;
      for (let index = 0; index < depth; index++) value = { next: value };
      return value as Record<string, unknown>;
    };
    const apply = (parent: WorkflowRunState) =>
      applyWorkflowTerminalParentContinuationPatch({
        contract: contractFor(parent),
        effect: effect(),
        parentRevision: 'revision-1',
        parentWorkflowName: 'parent',
        parentSnapshot: parent,
        retainedChild: retained(),
        storageTimestamp: 30,
        executionMode: 'continuous',
      });
    const inheritedErrorWithMessage = (message: string): Error => {
      const error = new Error();
      delete error.message;
      delete error.stack;
      const errorPrototype = Object.create(Error.prototype);
      Object.defineProperty(errorPrototype, 'message', { value: message });
      Object.setPrototypeOf(error, errorPrototype);
      return error;
    };

    const exactDepth = parentSnapshot();
    exactDepth.requestContext = { payload: nestedData(MAX_WORKFLOW_TERMINAL_CONTINUATION_DATA_DEPTH - 2) };
    expect(() => apply(exactDepth)).not.toThrow();

    const overDepth = parentSnapshot();
    overDepth.requestContext = { payload: nestedData(MAX_WORKFLOW_TERMINAL_CONTINUATION_DATA_DEPTH - 1) };
    expect(() => apply(overDepth)).toThrow(/depth limit/);

    const cyclic = parentSnapshot();
    const cycle: Record<string, unknown> = {};
    cycle.self = cycle;
    cyclic.requestContext = { cycle };
    expect(() => apply(cyclic)).toThrow(/contains a cycle/);

    const amplified = parentSnapshot();
    const shared = { value: 'shared' };
    amplified.requestContext = {
      aliases: Array.from({ length: MAX_WORKFLOW_TERMINAL_CONTINUATION_DATA_NODES }, () => shared),
    };
    expect(() => apply(amplified)).toThrow(/node limit/);

    const tooManyEntries = parentSnapshot();
    tooManyEntries.requestContext = {
      entries: new Map(
        Array.from({ length: Math.floor(MAX_WORKFLOW_TERMINAL_CONTINUATION_DATA_ENTRIES / 2) + 1 }, (_, index) => [
          index,
          index,
        ]),
      ),
    };
    expect(() => apply(tooManyEntries)).toThrow(/entry limit/);

    const sparse = parentSnapshot();
    const sparseArray: unknown[] = [];
    sparseArray.length = MAX_WORKFLOW_TERMINAL_CONTINUATION_DATA_ENTRIES + 1;
    sparse.requestContext = { sparseArray };
    expect(() => apply(sparse)).toThrow(/entry limit/);

    const tooManyBytes = parentSnapshot();
    tooManyBytes.requestContext = { value: 'x'.repeat(MAX_WORKFLOW_TERMINAL_CONTINUATION_DATA_BYTES + 1) };
    expect(() => apply(tooManyBytes)).toThrow(/byte limit/);

    const inheritedError = inheritedErrorWithMessage('x'.repeat(MAX_WORKFLOW_TERMINAL_CONTINUATION_DATA_BYTES + 1));
    const failedParent = parentSnapshot();
    const failedPatch = {
      ...mergePatch,
      parentRunWrite: {
        kind: 'set',
        status: 'failed',
        resultSource: 'source-coordinate',
        activePathSource: 'source-coordinate',
      },
    } as const;
    expect(() =>
      applyWorkflowTerminalParentContinuationPatch({
        contract: contractFor(failedParent, {
          childTerminalStatus: 'failed',
          action: { kind: 'fail-parent', reason: 'parent-fail' },
          patch: failedPatch,
        }),
        effect: effect('failed'),
        parentRevision: 'revision-1',
        parentWorkflowName: 'parent',
        parentSnapshot: failedParent,
        retainedChild: retained('failed', {
          status: 'failed',
          error: inheritedError,
          startedAt: 11,
          endedAt: 20,
        }),
        storageTimestamp: 30,
        executionMode: 'continuous',
      }),
    ).toThrow(/byte limit/);

    const aggregateParent = parentSnapshot();
    aggregateParent.requestContext = {
      parentError: inheritedErrorWithMessage('p'.repeat(600_000)),
    };
    const aggregateChild = retained();
    aggregateChild.snapshot.requestContext = {
      childError: inheritedErrorWithMessage('c'.repeat(600_000)),
    };
    expect(
      applyWorkflowTerminalParentContinuationPatch({
        contract: contractFor(aggregateParent),
        effect: effect(),
        parentRevision: 'revision-1',
        parentWorkflowName: 'parent',
        parentSnapshot: aggregateParent,
        retainedChild: aggregateChild,
        storageTimestamp: 30,
        executionMode: 'continuous',
      }),
    ).toBeDefined();

    const singleChargeParent = parentSnapshot();
    (singleChargeParent.context.nested as Record<string, any>).metadata.large = 'm'.repeat(600_000);
    const singleCharge = apply(singleChargeParent);
    expect((singleCharge.context.nested as Record<string, any>).metadata.large).toHaveLength(600_000);

    const opaque = parentSnapshot();
    opaque.tracingContext = { opaque: new ArrayBuffer(8) } as never;
    expect(() => apply(opaque)).toThrow(/non-data object/);

    for (const nonJsonValue of [Symbol('not cloneable'), 1n]) {
      const nonJson = parentSnapshot();
      nonJson.requestContext = { nonJsonValue };
      expect(() => apply(nonJson)).toThrow(/non-data values/);
    }

    for (const traceId of ['\ud800', 'bad\0trace']) {
      const malformed = parentSnapshot();
      malformed.tracingContext = { traceId };
      expect(() => apply(malformed)).toThrow(/malformed Unicode|null character/);
    }
  });

  it('serializes native failed errors and rejects non-monotonic clocks', () => {
    const parent = parentSnapshot();
    const nativeError = Object.assign(new Error('boom'), { code: 'E_CHILD' });
    let stackGetterCalls = 0;
    Object.defineProperty(nativeError, 'stack', {
      configurable: true,
      get() {
        stackGetterCalls++;
        return 'must not execute';
      },
    });
    const child = retained('failed', {
      status: 'failed',
      error: nativeError,
      startedAt: 11,
      endedAt: 20,
    });
    const failedPatch = {
      ...mergePatch,
      parentRunWrite: {
        kind: 'set',
        status: 'failed',
        resultSource: 'source-coordinate',
        activePathSource: 'source-coordinate',
      },
    };
    const contract = contractFor(parent, {
      childTerminalStatus: 'failed',
      action: { kind: 'fail-parent', reason: 'parent-fail' },
      patch: failedPatch,
    });
    const next = applyWorkflowTerminalParentContinuationPatch({
      contract,
      effect: effect('failed'),
      parentRevision: 'revision-1',
      parentWorkflowName: 'parent',
      parentSnapshot: parent,
      retainedChild: child,
      storageTimestamp: 30,
      executionMode: 'continuous',
    });
    expect(next.error).toMatchObject({ name: 'Error', message: 'boom', code: 'E_CHILD' });
    expect(stackGetterCalls).toBe(0);
    expect(() =>
      applyWorkflowTerminalParentContinuationPatch({
        contract,
        effect: effect('failed'),
        parentRevision: 'revision-1',
        parentWorkflowName: 'parent',
        parentSnapshot: parent,
        retainedChild: child,
        storageTimestamp: 9,
        executionMode: 'continuous',
      }),
    ).toThrow(/monotonic/);
  });

  it('propagates cancellation explicitly and clears terminal run-control state', () => {
    const parent = parentSnapshot();
    parent.suspendedPaths = { nested: [0] };
    parent.waitingPaths = { nested: [0] };
    parent.resumeLabels = { resume: { stepId: 'nested' } };
    parent.tripwire = { reason: 'stale' };
    parent.stepExecutionPath = ['stale'];
    const cancelPatch = {
      ...mergePatch,
      parentRunWrite: {
        kind: 'set',
        status: 'canceled',
        resultSource: 'source-coordinate',
        activePathSource: 'source-coordinate',
      },
    };
    const child = retained('canceled', { status: 'canceled', startedAt: 11, endedAt: 20 });
    const next = applyWorkflowTerminalParentContinuationPatch({
      contract: contractFor(parent, {
        childTerminalStatus: 'canceled',
        action: { kind: 'cancel-parent', reason: 'child-canceled' },
        patch: cancelPatch,
      }),
      effect: effect('canceled'),
      parentRevision: 'revision-1',
      parentWorkflowName: 'parent',
      parentSnapshot: parent,
      retainedChild: child,
      storageTimestamp: 30,
      executionMode: 'continuous',
    });
    expect(next.status).toBe('canceled');
    expect(next.activeStepsPath).toEqual({});
    expect(next.suspendedPaths).toEqual({});
    expect(next.waitingPaths).toEqual({});
    expect(next.resumeLabels).toEqual({});
    expect(next.tripwire).toBeUndefined();
    expect(next.stepExecutionPath).toBeUndefined();
  });

  it('rejects a non-record retained final state', () => {
    const parent = parentSnapshot();
    const child = retained();
    child.snapshot.context.__state = [] as unknown as WorkflowRunState['context'][string];
    expect(() =>
      applyWorkflowTerminalParentContinuationPatch({
        contract: contractFor(parent),
        effect: effect(),
        parentRevision: 'revision-1',
        parentWorkflowName: 'parent',
        parentSnapshot: parent,
        retainedChild: child,
        storageTimestamp: 30,
        executionMode: 'continuous',
      }),
    ).toThrow(/data object/);
  });

  it('rejects PostgreSQL-destructive null and malformed-Unicode strings and keys', () => {
    const parent = parentSnapshot();
    for (const requestContext of [{ value: 'a\0b' }, { ['key\0collision']: true }, { value: '\ud800' }]) {
      const child = retained();
      child.snapshot.requestContext = requestContext;
      expect(() =>
        applyWorkflowTerminalParentContinuationPatch({
          contract: contractFor(parent),
          effect: effect(),
          parentRevision: 'revision-1',
          parentWorkflowName: 'parent',
          parentSnapshot: parent,
          retainedChild: child,
          storageTimestamp: 30,
          executionMode: 'continuous',
        }),
      ).toThrow(/null character|malformed Unicode/);
    }
  });

  it('materializes all-accounted branch suspension instead of conflating it with wait', () => {
    const parent = parentSnapshot();
    parent.serializedStepGraph = [
      {
        type: 'parallel',
        steps: [
          { type: 'step', step: { id: 'left' } },
          { type: 'step', step: { id: 'right' } },
        ],
      },
    ];
    parent.context.left = {
      status: 'running',
      payload: { left: true },
      startedAt: 10,
      metadata: { nestedRunId: 'child-run' },
    } as WorkflowRunState['context'][string];
    parent.context.right = {
      status: 'suspended',
      payload: { right: true },
      startedAt: 10,
      suspendedAt: 20,
      suspendPayload: { __workflow_meta: { resumeLabels: { resumeRight: { stepId: 'right' } } } },
    } as WorkflowRunState['context'][string];
    parent.activeStepsPath = { left: [0, 0], right: [0, 1] };
    const suspendPatch = {
      ...mergePatch,
      parentRunWrite: {
        kind: 'set-suspended',
        resultSource: 'aggregate-container',
        activePathSource: 'source-coordinate',
        suspendedPathsSource: 'aggregate-container',
        resumeLabelsSource: 'aggregate-container',
      },
    };
    const next = applyWorkflowTerminalParentContinuationPatch({
      contract: contractFor(parent, {
        source: { kind: 'step', stepId: 'left', executionPath: [0, 0] },
        action: {
          kind: 'suspend-parent',
          reason: 'branch-suspended',
          target: { kind: 'container', containerType: 'parallel', executionPath: [0] },
        },
        patch: suspendPatch,
      }),
      effect: effect('success', [0, 0], 'left'),
      parentRevision: 'revision-1',
      parentWorkflowName: 'parent',
      parentSnapshot: parent,
      retainedChild: retained(),
      storageTimestamp: 30,
      executionMode: 'continuous',
    });
    expect(next.status).toBe('suspended');
    expect(next.result).toEqual({ status: 'suspended' });
    expect(next.suspendedPaths).toEqual({ right: [0, 1] });
    expect(next.resumeLabels).toEqual({ resumeRight: { stepId: 'right' } });
  });

  it('materializes all-accounted foreach suspension with resume evidence', () => {
    const parent = parentSnapshot();
    const firstSuspendPayload = JSON.parse(
      '{"__proto__":{"safe":true},"offset":0,"__streamState":{"messageList":{"memoryInfo":{"resourceId":"first"}}},"__workflow_meta":{"path":[99],"resumeLabels":{"resumeEach":{"stepId":"each","foreachIndex":1}}}}',
    );
    firstSuspendPayload.offset = -0;
    parent.context.each = {
      status: 'running',
      payload: ['a', 'b', 'c', 'd'],
      output: [
        {
          status: 'suspended',
          suspendPayload: {
            __streamState: { messageList: { memoryInfo: { resourceId: 'spoof' } } },
            __workflow_meta: { resumeLabels: { spoof: { stepId: 'each', foreachIndex: 0 } } },
          },
        },
        {
          status: 'suspended',
          suspendPayload: firstSuspendPayload,
        },
        {
          status: 'suspended',
          suspendPayload: {
            __streamState: { messageList: { memoryInfo: { resourceId: 'second' } } },
            __workflow_meta: { resumeLabels: { resumeOther: { stepId: 'each', foreachIndex: 2 } } },
          },
        },
        null,
      ],
      startedAt: 10,
      metadata: {
        __workflow_meta: { iterationRunIds: { '3': 'child-run' }, terminalIterationStates: { '0': 'success' } },
      },
    } as WorkflowRunState['context'][string];
    const originalParent = structuredClone(parent);
    const suspendPatch = {
      ...mergePatch,
      parentRunWrite: {
        kind: 'set-suspended',
        resultSource: 'aggregate-container',
        activePathSource: 'source-coordinate',
        suspendedPathsSource: 'aggregate-container',
        resumeLabelsSource: 'aggregate-container',
      },
    };
    const next = applyWorkflowTerminalParentContinuationPatch({
      contract: contractFor(parent, {
        source: { kind: 'foreach-iteration', stepId: 'each', containerPath: [3], iterationIndex: 3 },
        action: {
          kind: 'suspend-parent',
          reason: 'foreach-suspended',
          target: { kind: 'container', containerType: 'foreach', executionPath: [3] },
        },
        patch: suspendPatch,
      }),
      effect: effect('success', [3, 3], 'each'),
      parentRevision: 'revision-1',
      parentWorkflowName: 'parent',
      parentSnapshot: parent,
      retainedChild: retained(),
      storageTimestamp: 30,
      executionMode: 'continuous',
    });
    expect(next.status).toBe('suspended');
    expect(next.suspendedPaths).toEqual({ each: [3] });
    expect(next.resumeLabels).toEqual({
      resumeEach: { stepId: 'each', foreachIndex: 1 },
      resumeOther: { stepId: 'each', foreachIndex: 2 },
    });
    expect((next.context.each as Record<string, unknown>).status).toBe('suspended');
    const suspendPayload = (next.context.each as any).suspendPayload;
    expect(suspendPayload).toMatchObject({
      __workflow_meta: {
        path: [3, 3],
        resumeLabels: {
          resumeEach: { stepId: 'each', foreachIndex: 1 },
          resumeOther: { stepId: 'each', foreachIndex: 2 },
        },
        iterationSuspendPayloads: {
          '1': {
            offset: 0,
            __streamState: { messageList: { memoryInfo: { resourceId: 'first' } } },
          },
          '2': {
            __streamState: { messageList: { memoryInfo: { resourceId: 'second' } } },
          },
        },
      },
    });
    expect(suspendPayload).not.toHaveProperty('__streamState');
    const firstIterationPayload = suspendPayload.__workflow_meta.iterationSuspendPayloads['1'];
    expect(Object.hasOwn(firstIterationPayload, '__proto__')).toBe(true);
    expect(firstIterationPayload.__proto__).toEqual({ safe: true });
    expect(Object.is(firstIterationPayload.offset, -0)).toBe(false);
    expect(Object.getPrototypeOf(suspendPayload)).toBe(Object.prototype);
    expect(Object.prototype).not.toHaveProperty('safe');
    expect(parent).toEqual(originalParent);
    expect(JSON.parse(JSON.stringify(next.context.each))).toEqual(next.context.each);

    const singleParent = structuredClone(parent);
    (singleParent.context.each as any).metadata.__workflow_meta.terminalIterationStates['2'] = 'success';
    const single = applyWorkflowTerminalParentContinuationPatch({
      contract: contractFor(singleParent, {
        source: { kind: 'foreach-iteration', stepId: 'each', containerPath: [3], iterationIndex: 3 },
        action: {
          kind: 'suspend-parent',
          reason: 'foreach-suspended',
          target: { kind: 'container', containerType: 'foreach', executionPath: [3] },
        },
        patch: suspendPatch,
      }),
      effect: effect('success', [3, 3], 'each'),
      parentRevision: 'revision-1',
      parentWorkflowName: 'parent',
      parentSnapshot: singleParent,
      retainedChild: retained(),
      storageTimestamp: 30,
      executionMode: 'continuous',
    });
    expect((single.context.each as any).suspendPayload.__streamState).toEqual(firstIterationPayload.__streamState);

    const conflictingParent = structuredClone(parent);
    (conflictingParent.context.each as any).output[2].suspendPayload.__workflow_meta.resumeLabels = {
      resumeEach: { stepId: 'each', foreachIndex: 2 },
    };
    expect(() =>
      applyWorkflowTerminalParentContinuationPatch({
        contract: contractFor(conflictingParent, {
          source: { kind: 'foreach-iteration', stepId: 'each', containerPath: [3], iterationIndex: 3 },
          action: {
            kind: 'suspend-parent',
            reason: 'foreach-suspended',
            target: { kind: 'container', containerType: 'foreach', executionPath: [3] },
          },
          patch: suspendPatch,
        }),
        effect: effect('success', [3, 3], 'each'),
        parentRevision: 'revision-1',
        parentWorkflowName: 'parent',
        parentSnapshot: conflictingParent,
        retainedChild: retained(),
        storageTimestamp: 30,
        executionMode: 'continuous',
      }),
    ).toThrow(/conflicting duplicate resume label/);
  });
});
