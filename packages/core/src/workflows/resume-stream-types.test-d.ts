import { describe, expectTypeOf, it } from 'vitest';
import { z } from 'zod/v4';
import { createStep, createWorkflow } from './workflow';

describe('Run.resumeStream type surface', () => {
  it('accepts a resume label', async () => {
    const step = createStep({
      id: 'approval',
      inputSchema: z.object({ value: z.number() }),
      outputSchema: z.object({ value: z.number() }),
      execute: async ({ inputData }) => inputData,
    });
    const workflow = createWorkflow({
      id: 'resume-stream-label-workflow',
      inputSchema: z.object({ value: z.number() }),
      outputSchema: z.object({ value: z.number() }),
    })
      .then(step)
      .commit();

    const run = await workflow.createRun();
    const stream = run.resumeStream({ label: 'approve-order' });

    expectTypeOf(stream).not.toBeNever();
    // @ts-expect-error - labels must be strings
    run.resumeStream({ label: 42 });
  });
});
