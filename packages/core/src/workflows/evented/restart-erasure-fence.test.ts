/**
 * PF-6182: an evented restart claims the run, then its worker writes the
 * restarted run's running snapshot. That write must be fenced by the claimed
 * execution generation, so an erasure that deletes the run after the claim is
 * never undone by it. A first start still creates the run's row.
 */

import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { EventEmitterPubSub } from '../../events/event-emitter';
import { Mastra } from '../../mastra';
import { MockStore } from '../../storage/mock';
import { createStep, createWorkflow } from '.';

const WORKFLOW_ID = 'evented-restart-erasure-wf';
const RESOURCE_ID = 'account-1';

function setup() {
  let executions = 0;
  const work = createStep({
    id: 'work',
    inputSchema: z.object({ item: z.string() }),
    outputSchema: z.object({ secret: z.string() }),
    suspendSchema: z.object({ reason: z.string() }),
    execute: async ({ suspend }) => {
      executions++;
      if (executions === 1) {
        await suspend({ reason: 'pause once' });
        return { secret: '' };
      }
      return { secret: 'account-content' };
    },
  });
  const workflow = createWorkflow({
    id: WORKFLOW_ID,
    inputSchema: z.object({ item: z.string() }),
    outputSchema: z.object({ secret: z.string() }),
    steps: [work],
    options: { validateInputs: false },
  })
    .then(work)
    .commit();
  const storage = new MockStore();
  const mastra = new Mastra({
    storage,
    pubsub: new EventEmitterPubSub(),
    workflows: { [WORKFLOW_ID]: workflow },
    logger: false,
  });
  return { workflow, storage, mastra };
}

async function waitFor(check: () => Promise<boolean>, timeoutMs = 3000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await check()) return true;
    await new Promise(resolve => setTimeout(resolve, 25));
  }
  return false;
}

describe('evented restart erasure fence (PF-6182)', () => {
  it('creates the row on a first start', async () => {
    const { workflow, storage, mastra } = setup();
    await mastra.startWorkers();
    try {
      const run = await workflow.createRun({ resourceId: RESOURCE_ID });
      await expect(run.start({ inputData: { item: 'widget' } })).resolves.toMatchObject({ status: 'suspended' });
      const workflows = (await storage.getStore('workflows'))!;
      await expect(
        workflows.getWorkflowRunById({ runId: run.runId, workflowName: WORKFLOW_ID }),
      ).resolves.not.toBeNull();
    } finally {
      await mastra.stopWorkers?.();
    }
  });

  it('never recreates a run that erasure deleted after the restart claimed it', async () => {
    const { workflow, storage, mastra } = setup();
    await mastra.startWorkers();
    try {
      const run = await workflow.createRun({ resourceId: RESOURCE_ID });
      expect((await run.start({ inputData: { item: 'widget' } })).status).toBe('suspended');
      const workflows = (await storage.getStore('workflows'))!;
      const id = { workflowName: WORKFLOW_ID, runId: run.runId, expectedResourceId: RESOURCE_ID };

      // Stranded as running, the state a restart recovers.
      const suspended = (await workflows.loadWorkflowSnapshot({ workflowName: WORKFLOW_ID, runId: run.runId }))!;
      await workflows.persistWorkflowSnapshot({
        workflowName: WORKFLOW_ID,
        runId: run.runId,
        resourceId: RESOURCE_ID,
        snapshot: {
          ...suspended,
          status: 'running',
          suspendedPaths: {},
          activePaths: [0],
          activeStepsPath: { work: [0] },
        },
      });

      // Account erasure (closure delete, erasure cancel, closure delete) lands
      // after the restart's claim committed, just before the worker writes the
      // restarted run's running snapshot.
      let restarting = false;
      let erased = false;
      const erase = async () => {
        erased = true;
        expect(await workflows.deleteWorkflowRunClosure(id)).toEqual({ status: 'active' });
        await expect(workflows.cancelWorkflowRunForErasure(id)).resolves.toMatchObject({ status: 'cancelled' });
        await expect(workflows.deleteWorkflowRunClosure(id)).resolves.toMatchObject({ status: 'deleted' });
      };
      for (const name of ['persistWorkflowSnapshot', 'persistWorkflowStepUpdate'] as const) {
        const original = (workflows[name] as (...args: any[]) => Promise<any>).bind(workflows);
        vi.spyOn(workflows, name).mockImplementation(async (input: any) => {
          if (restarting && !erased && input.runId === run.runId && input.snapshot?.status === 'running') await erase();
          return original(input);
        });
      }
      restarting = true;

      let timer: ReturnType<typeof setTimeout> | undefined;
      await Promise.race([
        run.restart().catch(() => undefined),
        new Promise(resolve => {
          timer = setTimeout(resolve, 3000);
        }),
      ]);
      clearTimeout(timer);

      expect(erased).toBe(true);
      // Give the worker time to process the restart's start event.
      const recreated = await waitFor(
        async () => (await workflows.getWorkflowRunById({ runId: run.runId, workflowName: WORKFLOW_ID })) !== null,
      );
      expect(recreated).toBe(false);
    } finally {
      await mastra.stopWorkers?.();
    }
  });
});
