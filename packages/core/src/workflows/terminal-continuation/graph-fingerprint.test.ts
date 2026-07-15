import { describe, expect, it } from 'vitest';
import { z } from 'zod/v4';
import { createWorkflow } from '../create';
import type { SerializedStepFlowEntry } from '../types';
import { createStep } from '../workflow';
import {
  createWorkflowTerminalGraphFingerprint,
  resolveWorkflowTerminalGraphCoordinate,
  validateWorkflowTerminalStructuralString,
} from './graph-fingerprint';

function graph(): SerializedStepFlowEntry[] {
  return [
    { type: 'step', step: { id: 'nested', component: 'WORKFLOW', description: 'ignored' } },
    { type: 'sleep', id: 'sleep-1', duration: 10 },
    { type: 'sleepUntil', id: 'sleep-until-1', date: new Date('2026-01-01T00:00:00.000Z') },
    {
      type: 'conditional',
      steps: [
        { type: 'step', step: { id: 'left' } },
        { type: 'step', step: { id: 'right' } },
      ],
      serializedConditions: [
        { id: 'left-condition', fn: '({ inputData }) => Boolean(inputData)' },
        { id: 'right-condition', fn: '() => true' },
      ],
    },
    {
      type: 'loop',
      step: { id: 'loop-body' },
      serializedCondition: { id: 'loop-body-condition', fn: '({ iterationCount }) => iterationCount < 3' },
      loopType: 'dowhile',
    },
    { type: 'foreach', step: { id: 'each-body' }, opts: { concurrency: 2 } },
  ];
}

function countingProxy<T extends object>(target: T, increment: () => void): T {
  return new Proxy(target, {
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
}

describe('workflow terminal graph fingerprint', () => {
  it.each(['dowhile', 'dountil'] as const)('accepts the real Workflow.%s serialized condition identity', method => {
    const schema = z.object({ value: z.number() });
    const step = createStep({
      id: 'real-loop',
      inputSchema: schema,
      outputSchema: schema,
      execute: async ({ inputData }) => inputData,
    });
    const condition = async ({ iterationCount }: { iterationCount: number }) => iterationCount < 2;
    const workflow = createWorkflow({ id: `real-${method}`, inputSchema: schema, outputSchema: schema });
    const committed = workflow[method](step, condition as never).commit();
    const entry = committed.serializedStepGraph[0];
    expect(entry).toMatchObject({
      type: 'loop',
      step: { id: 'real-loop' },
      serializedCondition: { id: 'real-loop-condition' },
    });
    expect(() => createWorkflowTerminalGraphFingerprint(committed.serializedStepGraph)).not.toThrow();
  });

  it('rejects bound and proxied workflow callbacks at the durable graph boundary', () => {
    const schema = z.object({ value: z.number() });
    const step = createStep({
      id: 'unsafe-loop',
      inputSchema: schema,
      outputSchema: schema,
      execute: async ({ inputData }) => inputData,
    });
    const condition = async () => true;
    const build = (callback: typeof condition) =>
      createWorkflow({ id: 'unsafe-condition', inputSchema: schema, outputSchema: schema }).dowhile(
        step,
        callback as never,
      );
    expect(() =>
      createWorkflowTerminalGraphFingerprint(build(condition.bind(null)).commit().serializedStepGraph),
    ).toThrow(/native|bound/);
    expect(() =>
      createWorkflowTerminalGraphFingerprint(build(new Proxy(condition, {})).commit().serializedStepGraph),
    ).toThrow(/native|bound/);
  });

  it('accepts executable callbacks containing a native marker literal', () => {
    const fixture = graph();
    const loop = fixture[4];
    if (loop?.type !== 'loop') throw new Error('invalid fixture');
    loop.serializedCondition.fn = "({ state }) => state.label === '[native code]'";
    expect(() => createWorkflowTerminalGraphFingerprint(fixture)).not.toThrow();
  });

  it('rejects empty executable callback source before fingerprinting', () => {
    const invalid = graph();
    const loop = invalid[4];
    if (loop?.type !== 'loop') throw new Error('invalid fixture');
    loop.serializedCondition.fn = '';

    expect(() => createWorkflowTerminalGraphFingerprint(invalid)).toThrow(/must not be empty/);
  });

  it('is stable across JSON storage and ignores descriptive fields', () => {
    const original = graph();
    const stored = JSON.parse(JSON.stringify(original)) as SerializedStepFlowEntry[];
    expect(createWorkflowTerminalGraphFingerprint(stored)).toBe(createWorkflowTerminalGraphFingerprint(original));

    original[0] = { type: 'step', step: { id: 'nested', component: 'WORKFLOW', description: 'changed' } };
    expect(createWorkflowTerminalGraphFingerprint(original)).toBe(createWorkflowTerminalGraphFingerprint(stored));
  });

  it('is stable across reconstructed implicit mapping step IDs while retaining explicit IDs', () => {
    const schema = z.object({ value: z.number() });
    const mapping = async ({ inputData }: { inputData: { value: number } }) => ({ value: inputData.value + 1 });
    const build = (id?: string) =>
      createWorkflow({ id: 'mapped-workflow', inputSchema: schema, outputSchema: schema })
        .map(mapping as never, id === undefined ? undefined : { id })
        .commit();

    const first = build();
    const rebuilt = build();
    expect(first.serializedStepGraph[0]).not.toEqual(rebuilt.serializedStepGraph[0]);
    expect(createWorkflowTerminalGraphFingerprint(first.serializedStepGraph)).toBe(
      createWorkflowTerminalGraphFingerprint(rebuilt.serializedStepGraph),
    );
    expect(createWorkflowTerminalGraphFingerprint(build('explicit-map-a').serializedStepGraph)).not.toBe(
      createWorkflowTerminalGraphFingerprint(build('explicit-map-b').serializedStepGraph),
    );
  });

  it('treats explicit undefined optional step fields like JSON-omitted fields', () => {
    const original = graph();
    const conditional = original[3];
    if (conditional?.type !== 'conditional') throw new Error('invalid fixture');
    conditional.steps[0]!.step = {
      ...conditional.steps[0]!.step,
      component: undefined,
      mapConfig: undefined,
      canSuspend: undefined,
      serializedStepFlow: undefined,
    };

    expect(createWorkflowTerminalGraphFingerprint(original)).toBe(
      createWorkflowTerminalGraphFingerprint(JSON.parse(JSON.stringify(original)) as SerializedStepFlowEntry[]),
    );
  });

  it('changes for execution-semantic graph changes', () => {
    const original = graph();
    const changedConcurrency = graph();
    const foreach = changedConcurrency[5];
    if (foreach?.type !== 'foreach') throw new Error('invalid fixture');
    foreach.opts.concurrency = 3;

    const changedCondition = graph();
    const loop = changedCondition[4];
    if (loop?.type !== 'loop') throw new Error('invalid fixture');
    loop.serializedCondition.fn = '() => false';

    expect(createWorkflowTerminalGraphFingerprint(changedConcurrency)).not.toBe(
      createWorkflowTerminalGraphFingerprint(original),
    );
    expect(createWorkflowTerminalGraphFingerprint(changedCondition)).not.toBe(
      createWorkflowTerminalGraphFingerprint(original),
    );
  });

  it('resolves steps, containers, branches, loops, foreach iterations, and sleep entries', () => {
    const value = graph();
    expect(resolveWorkflowTerminalGraphCoordinate(value, [0])).toEqual({ kind: 'step', stepId: 'nested' });
    expect(resolveWorkflowTerminalGraphCoordinate(value, [1])).toEqual({ kind: 'sleep', entryId: 'sleep-1' });
    expect(resolveWorkflowTerminalGraphCoordinate(value, [3])).toEqual({
      kind: 'container',
      containerType: 'conditional',
    });
    expect(resolveWorkflowTerminalGraphCoordinate(value, [3, 1])).toEqual({
      kind: 'branch',
      containerType: 'conditional',
      stepId: 'right',
    });
    expect(resolveWorkflowTerminalGraphCoordinate(value, [4])).toEqual({
      kind: 'loop',
      stepId: 'loop-body',
      loopType: 'dowhile',
    });
    expect(resolveWorkflowTerminalGraphCoordinate(value, [5, 7])).toEqual({
      kind: 'foreach',
      stepId: 'each-body',
      iterationIndex: 7,
    });
  });

  it('rejects a malformed non-step branch during coordinate resolution', () => {
    const invalid = graph();
    const conditional = invalid[3];
    if (conditional?.type !== 'conditional') throw new Error('invalid fixture');
    conditional.steps[0] = { type: 'sleep', step: { id: 'left' } } as unknown as (typeof conditional.steps)[number];

    expect(() => resolveWorkflowTerminalGraphCoordinate(invalid, [3, 0])).toThrow(/branch must be a step/);
  });

  it('rejects sparse graphs, accessors, malformed Unicode, and duplicate IDs', () => {
    const sparse = graph();
    delete sparse[1];
    expect(() => createWorkflowTerminalGraphFingerprint(sparse)).toThrow(/dense/);

    const accessor = graph();
    Object.defineProperty(accessor[0]!.step, 'id', { get: () => 'nested' });
    expect(() => createWorkflowTerminalGraphFingerprint(accessor)).toThrow(/accessor/);

    const hidden = graph();
    Object.defineProperty(hidden[0], 'type', {
      configurable: true,
      enumerable: false,
      value: 'step',
      writable: true,
    });
    expect(() => createWorkflowTerminalGraphFingerprint(hidden)).toThrow(/non-enumerable/);

    let proxyTrapCalls = 0;
    const proxied = countingProxy(graph(), () => proxyTrapCalls++);
    expect(() => createWorkflowTerminalGraphFingerprint(proxied)).toThrow(/must not be a proxy/);
    expect(proxyTrapCalls).toBe(0);

    const proxiedEntryGraph = graph();
    let entryTrapCalls = 0;
    proxiedEntryGraph[0] = countingProxy(proxiedEntryGraph[0]!, () => entryTrapCalls++);
    expect(() => resolveWorkflowTerminalGraphCoordinate(proxiedEntryGraph, [0])).toThrow(/must not be a proxy/);
    expect(entryTrapCalls).toBe(0);

    expect(() => validateWorkflowTerminalStructuralString('\ud800', 'id')).toThrow(/well-formed/);
    expect(validateWorkflowTerminalStructuralString('emoji-😀', 'id')).toBe('emoji-😀');

    const duplicate = graph();
    duplicate.push({ type: 'step', step: { id: 'nested' } });
    expect(() => createWorkflowTerminalGraphFingerprint(duplicate)).toThrow(/duplicate step id/);
  });

  it('scopes duplicate IDs to each nested workflow graph', () => {
    const nested: SerializedStepFlowEntry[] = [
      {
        type: 'step',
        step: {
          id: 'shared',
          serializedStepFlow: [{ type: 'step', step: { id: 'shared' } }],
        },
      },
    ];
    expect(() => createWorkflowTerminalGraphFingerprint(nested)).not.toThrow();
  });

  it('rejects ambiguous sleeps, noncanonical dates, and mismatched conditional identities', () => {
    expect(() =>
      createWorkflowTerminalGraphFingerprint([
        { type: 'sleep', id: 'sleep', duration: 1, fn: '() => 1' } as SerializedStepFlowEntry,
      ]),
    ).toThrow(/exactly one/);
    expect(() =>
      createWorkflowTerminalGraphFingerprint([
        { type: 'sleepUntil', id: 'sleep', date: '2026-01-01' } as unknown as SerializedStepFlowEntry,
      ]),
    ).toThrow(/canonical ISO/);

    let prototypeTrapCalls = 0;
    const proxiedPrototype = countingProxy(Object.create(null) as object, () => prototypeTrapCalls++);
    const dateWithProxiedPrototype = Object.create(proxiedPrototype) as Date;
    expect(() =>
      createWorkflowTerminalGraphFingerprint([{ type: 'sleepUntil', id: 'sleep', date: dateWithProxiedPrototype }]),
    ).toThrow(/Date or string/);
    expect(prototypeTrapCalls).toBe(0);

    expect(() =>
      createWorkflowTerminalGraphFingerprint([
        {
          type: 'conditional',
          steps: [{ type: 'step', step: { id: 'branch' } }],
          serializedConditions: [{ id: 'other-condition', fn: '() => true' }],
        },
      ]),
    ).toThrow(/IDs differ/);
  });

  it('rejects graph arrays before materializing beyond the resource bound', () => {
    const oversized = Array.from({ length: 4_097 }, (_, index) => ({
      type: 'step' as const,
      step: { id: `step-${index}` },
    }));
    expect(() => createWorkflowTerminalGraphFingerprint(oversized)).toThrow(/invalid length/);
  });
});
