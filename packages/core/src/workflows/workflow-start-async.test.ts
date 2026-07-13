import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';

import { Mastra } from '../mastra';
import { MockStore } from '../storage/mock';
import { createStep, createWorkflow } from './workflow';

describe('Workflow startAsync', () => {
  it('returns an observable promise for the background execution', async () => {
    const execute = vi.fn().mockResolvedValue({ result: 'success' });
    const step = createStep({
      id: 'step',
      execute,
      inputSchema: z.object({}),
      outputSchema: z.object({ result: z.string() }),
    });
    const workflow = createWorkflow({
      id: 'observable-start-async-workflow',
      inputSchema: z.object({}),
      outputSchema: z.object({ result: z.string() }),
      steps: [step],
    });
    workflow.then(step).commit();
    new Mastra({
      storage: new MockStore(),
      workflows: { 'observable-start-async-workflow': workflow },
    });

    const run = await workflow.createRun();
    const { runId, execution } = await run.startAsync({ inputData: {} });

    expect(runId).toBe(run.runId);
    await expect(execution).resolves.toMatchObject({ status: 'success' });
    expect(execute).toHaveBeenCalledOnce();
  });
});
