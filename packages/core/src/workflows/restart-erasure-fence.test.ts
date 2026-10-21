/**
 * PF-6186: a restart that finds every entry already finished completes the run
 * from its saved output. Its terminal write must carry the restart's admitted
 * execution generation, so that an erasure which deleted the run after the
 * restart's claim is never undone by that write.
 */

import { describe, expect, it } from 'vitest';
import { z } from 'zod/v4';
import { Mastra } from '../mastra';
import { InMemoryStore } from '../storage';
import { createWorkflow } from './create';
import { createStep } from './workflow';

const id = { workflowName: 'wf', runId: 'run-1', expectedResourceId: 'account-1' };

function build(storage: InMemoryStore) {
  const step = createStep({
    id: 'only',
    inputSchema: z.object({}).passthrough(),
    outputSchema: z.object({ secret: z.string() }),
    execute: async () => ({ secret: 'account-content' }),
  });
  const wf = createWorkflow({
    id: 'wf',
    inputSchema: z.object({}).passthrough(),
    outputSchema: z.object({ secret: z.string() }),
  })
    .then(step)
    .commit();
  return new Mastra({ storage, workflows: { wf }, logger: false });
}

describe('restart after the final entry (PF-6186)', () => {
  it('never recreates a run that erasure deleted after the restart claimed it', async () => {
    const storage = new InMemoryStore();
    const workflows = (await storage.getStore('workflows'))!;

    // A run whose only entry finished, stranded as running before its terminal write.
    const first = await build(storage)
      .getWorkflow('wf')
      .createRun({ runId: id.runId, resourceId: id.expectedResourceId });
    await first.start({ inputData: {} });
    const finished = (await workflows.loadWorkflowSnapshot({ workflowName: id.workflowName, runId: id.runId }))!;
    await workflows.persistWorkflowSnapshot({
      workflowName: id.workflowName,
      runId: id.runId,
      resourceId: id.expectedResourceId,
      snapshot: { ...finished, status: 'running', result: undefined },
    });

    // Account erasure (closure delete, erasure cancel, closure delete) lands
    // between the restart's claim and its terminal write.
    const persistStepUpdate = workflows.persistWorkflowStepUpdate.bind(workflows);
    let erased = false;
    workflows.persistWorkflowStepUpdate = async input => {
      if (!erased) {
        erased = true;
        expect(await workflows.deleteWorkflowRunClosure(id)).toEqual({ status: 'active' });
        await expect(workflows.cancelWorkflowRunForErasure(id)).resolves.toMatchObject({ status: 'cancelled' });
        await expect(workflows.deleteWorkflowRunClosure(id)).resolves.toMatchObject({ status: 'deleted' });
      }
      return persistStepUpdate(input);
    };

    const restarted = await build(storage)
      .getWorkflow('wf')
      .createRun({ runId: id.runId, resourceId: id.expectedResourceId });
    await restarted.restart({}).catch(() => undefined);

    expect(erased).toBe(true);
    await expect(workflows.getWorkflowRunById({ runId: id.runId, workflowName: id.workflowName })).resolves.toBeNull();
  });
});
