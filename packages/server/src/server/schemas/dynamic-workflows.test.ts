import { describe, expect, it } from 'vitest';

import { dynamicWorkflowDefinitionBodySchema } from './dynamic-workflows';

const baseDefinition = {
  id: 'wire-wf',
  inputSchema: { type: 'object' },
  outputSchema: { type: 'object' },
};

describe('dynamic workflow wire schema — control-flow entry identity fields', () => {
  it('preserves id/description/metadata on every control-flow entry instead of stripping them', () => {
    const graph = [
      {
        type: 'parallel',
        id: 'fan-out',
        description: 'Run both',
        metadata: { title: 'Fan out' },
        steps: [{ type: 'tool', id: 'echo', toolId: 'echo-tool' }],
      },
      {
        type: 'conditional',
        id: 'route',
        description: 'Route on value',
        metadata: { title: 'Route' },
        steps: [{ type: 'tool', id: 'echo-2', toolId: 'echo-tool' }],
        predicates: [{ op: 'truthy', value: { path: 'inputData.value' } }],
      },
      {
        type: 'loop',
        id: 'bump',
        description: 'Loop it',
        metadata: { title: 'Bump' },
        step: { type: 'tool', id: 'echo-3', toolId: 'echo-tool' },
        loopType: 'dountil',
        predicate: { op: 'truthy', value: { path: 'inputData.done' } },
      },
      {
        type: 'foreach',
        id: 'each',
        description: 'Per item',
        metadata: { title: 'Each' },
        step: { type: 'tool', id: 'echo-4', toolId: 'echo-tool' },
        opts: { concurrency: 2 },
      },
      { type: 'sleep', id: 'wait', description: 'Pause', metadata: { title: 'Wait' }, duration: 5 },
      { type: 'sleepUntil', id: 'hold', description: 'Hold', metadata: { title: 'Hold' }, date: '2099-01-01' },
      { type: 'mapping', id: 'shape', description: 'Reshape', metadata: { title: 'Shape' }, mapConfig: '{}' },
    ];

    const result = dynamicWorkflowDefinitionBodySchema.safeParse({ ...baseDefinition, graph });

    expect(result.success).toBe(true);
    for (const [index, entry] of graph.entries()) {
      expect(result.data!.graph[index]).toMatchObject({
        id: entry.id,
        description: entry.description,
        metadata: entry.metadata,
      });
    }
  });

  it('still accepts control-flow entries without the optional identity fields', () => {
    const result = dynamicWorkflowDefinitionBodySchema.safeParse({
      ...baseDefinition,
      graph: [
        { type: 'parallel', steps: [{ type: 'tool', id: 'echo', toolId: 'echo-tool' }] },
        { type: 'sleep', id: 'wait', duration: 5 },
      ],
    });

    expect(result.success).toBe(true);
    expect(result.data!.graph[0]).not.toHaveProperty('description');
  });

  it('preserves declarative schedules and agent passthrough options', () => {
    const options = {
      retries: 1,
      maxSteps: 3,
      disableBackgroundTasks: true,
      toolCallConcurrency: { limit: 2, strategy: 'called' },
      versions: {
        agents: { summarizer: { versionId: 'version-1' }, reviewer: { status: 'published' } },
        defaultStatus: 'draft',
      },
    };
    const result = dynamicWorkflowDefinitionBodySchema.parse({
      ...baseDefinition,
      schedule: { cron: '0 0 * * *', inputData: { tenant: 'acme' } },
      graph: [
        {
          type: 'agent',
          id: 'summarize',
          agentId: 'summarizer',
          options,
        },
      ],
    });

    expect(result.schedule).toEqual({ cron: '0 0 * * *', inputData: { tenant: 'acme' } });
    expect(result.graph[0]).toMatchObject({ options });
  });

  it('rejects unsupported version selectors and concurrency strategies', () => {
    for (const options of [
      { versions: { summarizer: 'version-1' } },
      { versions: { agents: { summarizer: { status: 'unknown' } } } },
      { toolCallConcurrency: { strategy: 'unknown' } },
    ]) {
      expect(
        dynamicWorkflowDefinitionBodySchema.safeParse({
          ...baseDefinition,
          graph: [{ type: 'agent', id: 'summarize', agentId: 'summarizer', options }],
        }).success,
      ).toBe(false);
    }
  });

  it('rejects unknown schedule fields instead of silently changing the schedule', () => {
    const result = dynamicWorkflowDefinitionBodySchema.safeParse({
      ...baseDefinition,
      graph: [{ type: 'tool', id: 'echo', toolId: 'echo-tool' }],
      schedule: { cron: '0 0 * * *', timeZone: 'Europe/Berlin' },
    });

    expect(result.success).toBe(false);
  });

  it('preserves valid schedule payloads and absent or empty schedule semantics', () => {
    const definition = {
      ...baseDefinition,
      graph: [{ type: 'tool', id: 'echo', toolId: 'echo-tool' }],
    };
    const schedule = {
      cron: '0 9 * * *',
      timezone: 'Europe/Berlin',
      inputData: { enabled: false, count: 0, optional: null },
      initialState: ['ready'],
      requestContext: { tenant: 'acme' },
      metadata: { label: 'Daily' },
    };
    for (const schedules of [
      schedule,
      [
        { ...schedule, id: 'daily' },
        { id: 'weekly', cron: '0 9 * * 1' },
      ],
      [],
    ]) {
      const result = dynamicWorkflowDefinitionBodySchema.safeParse({ ...definition, schedule: schedules });

      expect(result.success).toBe(true);
      expect(result.data?.schedule).toEqual(schedules);
    }
    const absent = dynamicWorkflowDefinitionBodySchema.safeParse(definition);
    expect(absent.success).toBe(true);
    expect(absent.data).not.toHaveProperty('schedule');
  });

  it.each([
    { schedule: { cron: 'not a cron' }, path: ['schedule', 'cron'] },
    { schedule: { cron: '0 9 * * *', timezone: 'Not/AZone' }, path: ['schedule', 'timezone'] },
    { schedule: [{ cron: '0 9 * * *' }], path: ['schedule', 0, 'id'] },
    {
      schedule: [
        { id: 'daily', cron: '0 9 * * *' },
        { id: 'daily', cron: '0 10 * * *' },
      ],
      path: ['schedule', 1, 'id'],
    },
  ])('rejects invalid schedule configuration at $path', ({ schedule, path }) => {
    const result = dynamicWorkflowDefinitionBodySchema.safeParse({
      ...baseDefinition,
      graph: [{ type: 'tool', id: 'echo', toolId: 'echo-tool' }],
      schedule,
    });

    expect(result.success).toBe(false);
    expect(result.error?.issues).toEqual(expect.arrayContaining([expect.objectContaining({ code: 'custom', path })]));
  });
});
