import { describe, expect, it } from 'vitest';
import { z } from 'zod/v4';
import { Mastra } from '../../mastra';
import { createStep, createWorkflow } from '../index';
import { computeScheduleDefinitionHash } from './definition-hash';

const step = (id: string) =>
  createStep({
    id,
    inputSchema: z.object({}),
    outputSchema: z.object({}),
    execute: async () => ({}),
  }) as any;

const buildWorkflow = (stepIds: string[]) => {
  const wf = createWorkflow({
    id: 'hash-wf',
    inputSchema: z.object({}),
    outputSchema: z.object({}),
  });
  let chain: any = wf;
  for (const id of stepIds) chain = chain.then(step(id));
  chain.commit();
  return wf;
};

const buildSleepWorkflow = ({
  duration = 1000,
  date = new Date('2030-01-01T00:00:00.000Z'),
  id = 'sleep_hash-sleep-wf_1',
  mastra,
}: { duration?: number; date?: Date; id?: string; mastra?: Mastra } = {}) => {
  const wf = createWorkflow({
    id: 'hash-sleep-wf',
    inputSchema: z.object({}),
    outputSchema: z.object({}),
  });
  if (mastra) wf.__registerMastra(mastra);
  // Reserve the next generated ordinal so neither fallback can overwrite it.
  return wf.sleep(1, { id }).sleep(duration).sleepUntil(date).commit();
};

describe('computeScheduleDefinitionHash', () => {
  it('is stable across separately-built instances of the same graph', () => {
    // This is the property the fence depends on: the schedule row is hashed by
    // one process at reconcile time and compared by another at fire time. If
    // two identical builds hashed differently, every fire would be refused.
    const a = computeScheduleDefinitionHash(buildWorkflow(['one', 'two']).serializedStepGraph);
    const b = computeScheduleDefinitionHash(buildWorkflow(['one', 'two']).serializedStepGraph);

    expect(a).toBeDefined();
    expect(a).toBe(b);
  });

  it('is stable for unnamed sleep entries across replicas with or without Mastra registration', () => {
    const original = buildSleepWorkflow();
    const replica = buildSleepWorkflow({ mastra: new Mastra({ logger: false }) });
    const originalHash = computeScheduleDefinitionHash(original.serializedStepGraph);

    expect(originalHash).toBeDefined();
    expect(originalHash).toBe(computeScheduleDefinitionHash(replica.serializedStepGraph));
    expect(Object.keys(original.steps)).toHaveLength(3);
    expect(Object.keys(replica.steps)).toHaveLength(3);
  });

  it('preserves wait values and caller-provided identities in the definition hash', () => {
    const originalHash = computeScheduleDefinitionHash(buildSleepWorkflow().serializedStepGraph);
    expect(originalHash).not.toBe(
      computeScheduleDefinitionHash(buildSleepWorkflow({ duration: 2000 }).serializedStepGraph),
    );
    expect(originalHash).not.toBe(
      computeScheduleDefinitionHash(
        buildSleepWorkflow({ date: new Date('2030-01-02T00:00:00.000Z') }).serializedStepGraph,
      ),
    );
    expect(
      computeScheduleDefinitionHash(
        buildSleepWorkflow({ id: 'sleep_00000000-0000-4000-8000-000000000001' }).serializedStepGraph,
      ),
    ).not.toBe(
      computeScheduleDefinitionHash(
        buildSleepWorkflow({ id: 'sleep_00000000-0000-4000-8000-000000000002' }).serializedStepGraph,
      ),
    );

    const custom = buildSleepWorkflow({
      mastra: new Mastra({ logger: false, idGenerator: context => `custom-${context?.stepType}` }),
    });
    expect(Object.keys(custom.steps)).toEqual([
      'sleep_hash-sleep-wf_1',
      'sleep_custom-sleep',
      'sleep_custom-sleep-until',
    ]);
    expect(originalHash).not.toBe(computeScheduleDefinitionHash(custom.serializedStepGraph));
  });

  it('is stable across repeated calls on the same graph', () => {
    const graph = buildWorkflow(['one']).serializedStepGraph;

    expect(computeScheduleDefinitionHash(graph)).toBe(computeScheduleDefinitionHash(graph));
  });

  it('changes when a step is added', () => {
    // The #19169 scenario: the current build inserts a gate step ahead of the
    // side effect, and the stale build must not hash the same.
    const before = computeScheduleDefinitionHash(buildWorkflow(['side-effect']).serializedStepGraph);
    const after = computeScheduleDefinitionHash(buildWorkflow(['gate', 'side-effect']).serializedStepGraph);

    expect(before).not.toBe(after);
  });

  it('changes when step order changes', () => {
    const forward = computeScheduleDefinitionHash(buildWorkflow(['one', 'two']).serializedStepGraph);
    const reversed = computeScheduleDefinitionHash(buildWorkflow(['two', 'one']).serializedStepGraph);

    expect(forward).not.toBe(reversed);
  });

  it('produces a short hex digest', () => {
    expect(computeScheduleDefinitionHash(buildWorkflow(['one']).serializedStepGraph)).toMatch(/^[0-9a-f]{16}$/);
  });

  describe('fail-open cases', () => {
    it.each([
      ['null', null],
      ['undefined', undefined],
      ['an empty array', []],
      ['an empty object', {}],
    ])('returns undefined for %s so unfenced schedules keep firing', (_label, input) => {
      expect(computeScheduleDefinitionHash(input)).toBeUndefined();
    });

    it('returns undefined for a non-serializable graph', () => {
      const circular: any = { steps: [] };
      circular.self = circular;

      expect(computeScheduleDefinitionHash(circular)).toBeUndefined();
    });
  });
});
