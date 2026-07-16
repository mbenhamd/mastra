import { EventEmitterPubSub } from '@mastra/core/events';
import { RequestContext } from '@mastra/core/request-context';
import type { Step, Workflow } from '@mastra/core/workflows';
import { Inngest } from 'inngest';
import { describe, expectTypeOf, it } from 'vitest';
import { z } from 'zod/v4';

import type { InngestEngineType } from './types';
import { init } from './index';

describe('Inngest createRun contract', () => {
  it('accepts raw workflow input while steps receive parsed input', async () => {
    const inngest = new Inngest({ id: 'create-run-types' });
    const { createStep, createWorkflow } = init(inngest);
    const inputSchema = z.object({
      name: z.string(),
      count: z.string().transform(Number),
      mode: z.enum(['safe', 'fast']).default('safe'),
    });
    const outputSchema = z.object({ count: z.number() });
    const requestContextSchema = z.object({ tenantId: z.string() });
    const step = createStep({
      id: 'parse-input',
      inputSchema,
      outputSchema,
      execute: async ({ inputData }) => {
        expectTypeOf(inputData).toEqualTypeOf<{
          name: string;
          count: number;
          mode: 'safe' | 'fast';
        }>();
        return { count: inputData.count };
      },
    });
    const workflow = createWorkflow({
      id: 'raw-input-workflow',
      inputSchema,
      outputSchema,
      requestContextSchema,
      steps: [step],
    })
      .then(step)
      .commit();

    const coreWorkflow: Workflow<
      InngestEngineType,
      Step<string, any, any, any, any, any, InngestEngineType>[],
      string,
      unknown,
      z.output<typeof inputSchema>,
      z.output<typeof outputSchema>,
      z.output<typeof inputSchema>,
      z.output<typeof requestContextSchema>,
      z.input<typeof inputSchema>
    > = workflow;
    expectTypeOf(coreWorkflow).not.toBeAny();

    const run = await workflow.createRun({
      disableScorers: true,
    });
    const requestContext = new RequestContext<z.output<typeof requestContextSchema>>();
    requestContext.set('tenantId', 'tenant-1');
    await run.start({ inputData: { name: 'Ada', count: '2' }, requestContext });
    await run.startAsync({ inputData: { name: 'Grace', count: '3', mode: 'fast' }, requestContext });

    // @ts-expect-error - callers provide pre-validation schema input, so count must be a string
    await run.start({ inputData: { name: 'Ada', count: 2 } });

    // Core exposes this option, and the Inngest override must remain substitutable.
    await coreWorkflow.createRun({ pubsub: new EventEmitterPubSub() });
  });

  it('preserves the original explicit generic parameter order', async () => {
    const inngest = new Inngest({ id: 'legacy-explicit-generics' });
    const { createStep, createWorkflow } = init(inngest);
    const step = createStep({
      id: 'legacy-step',
      inputSchema: z.object({ value: z.string() }),
      outputSchema: z.object({ value: z.string() }),
      stateSchema: z.object({ attempt: z.number() }),
      execute: async ({ inputData }) => inputData,
    });
    const workflow = createWorkflow<
      'legacy-explicit-generics',
      { attempt: number },
      { value: string },
      { value: string },
      [typeof step]
    >({
      id: 'legacy-explicit-generics',
      inputSchema: z.object({ value: z.string() }),
      outputSchema: z.object({ value: z.string() }),
      stateSchema: z.object({ attempt: z.number() }),
      steps: [step],
    })
      .then(step)
      .commit();

    const run = await workflow.createRun();
    await run.start({ inputData: { value: 'ok' }, initialState: { attempt: 1 } });
  });

  it('preserves ordinary workflow input types', async () => {
    const inngest = new Inngest({ id: 'ordinary-create-run-types' });
    const { createStep, createWorkflow } = init(inngest);
    type WorkflowParams = Parameters<typeof createWorkflow>[0];
    expectTypeOf<'schedule' extends keyof WorkflowParams ? true : false>().toEqualTypeOf<true>();
    const legacyScheduleParams = {
      id: 'legacy-schedule-shape',
      inputSchema: z.object({ value: z.string() }),
      outputSchema: z.object({ value: z.string() }),
      schedule: { cron: '0 9 * * *' },
    } satisfies WorkflowParams;
    expectTypeOf(legacyScheduleParams).toMatchTypeOf<WorkflowParams>();

    const step = createStep({
      id: 'ordinary-step',
      inputSchema: z.object({ value: z.string() }),
      outputSchema: z.object({ value: z.string() }),
      execute: async ({ inputData }) => inputData,
    });
    const workflow = createWorkflow({
      id: 'ordinary-workflow',
      inputSchema: z.object({ value: z.string() }),
      outputSchema: z.object({ value: z.string() }),
      steps: [step],
    })
      .then(step)
      .commit();

    const run = await workflow.createRun();
    await run.start({ inputData: { value: 'ok' } });
    await run.startAsync({ inputData: { value: 'ok' } });

    // @ts-expect-error - ordinary workflows stay schema checked
    await run.startAsync({ inputData: { value: 1 } });
  });
});
