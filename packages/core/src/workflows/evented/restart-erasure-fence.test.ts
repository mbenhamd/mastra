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
  const pubsub = new EventEmitterPubSub();
  const mastra = new Mastra({
    storage,
    pubsub,
    workflows: { [WORKFLOW_ID]: workflow },
    logger: false,
  });
  return { workflow, storage, mastra, pubsub };
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

      // The restart settles (the default engine's superseded outcome) rather
      // than waiting on a finish event no worker would publish.
      await expect(run.restart()).resolves.toMatchObject({ status: 'canceled' });

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

  it('settles a restart whose run erasure canceled before the start was dispatched', async () => {
    const { workflow, storage, mastra, pubsub } = setup();
    await mastra.startWorkers();
    try {
      const run = await workflow.createRun({ resourceId: RESOURCE_ID });
      expect((await run.start({ inputData: { item: 'widget' } })).status).toBe('suspended');
      const workflows = (await storage.getStore('workflows'))!;
      const id = { workflowName: WORKFLOW_ID, runId: run.runId, expectedResourceId: RESOURCE_ID };
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

      // Erasure cancels the run after the restart claimed it, while its start
      // event is still on the way to the worker.
      let canceled = false;
      const publish = pubsub.publish.bind(pubsub);
      vi.spyOn(pubsub, 'publish').mockImplementation(async (topic: string, event: any) => {
        if (!canceled && topic === 'workflows' && event.type === 'workflow.start' && event.runId === run.runId) {
          canceled = true;
          expect(await workflows.deleteWorkflowRunClosure(id)).toEqual({ status: 'active' });
          await expect(workflows.cancelWorkflowRunForErasure(id)).resolves.toMatchObject({ status: 'cancelled' });
        }
        return publish(topic, event);
      });

      await expect(run.restart()).resolves.toMatchObject({ status: 'canceled' });
      expect(canceled).toBe(true);
    } finally {
      await mastra.stopWorkers?.();
    }
  });

  it('keeps a resume waiting when a refused admission of another resume attempt settles', async () => {
    let release!: () => void;
    const gate = new Promise<void>(resolve => {
      release = resolve;
    });
    let resumedStarted = false;
    const work = createStep({
      id: 'work',
      inputSchema: z.object({ item: z.string() }),
      outputSchema: z.object({ done: z.boolean() }),
      resumeSchema: z.object({ go: z.boolean() }),
      execute: async ({ suspend, resumeData }) => {
        if (!resumeData) {
          await suspend({});
          return { done: false };
        }
        resumedStarted = true;
        await gate;
        return { done: true };
      },
    });
    const workflow = createWorkflow({
      id: 'evented-resume-attempt-wf',
      inputSchema: z.object({ item: z.string() }),
      outputSchema: z.object({ done: z.boolean() }),
      steps: [work],
      options: { validateInputs: false },
    })
      .then(work)
      .commit();
    const storage = new MockStore();
    const mastra = new Mastra({
      storage,
      pubsub: new EventEmitterPubSub(),
      workflows: { 'evented-resume-attempt-wf': workflow },
      logger: false,
    });
    await mastra.startWorkers();
    try {
      const run = await workflow.createRun({ resourceId: RESOURCE_ID });
      expect((await run.start({ inputData: { item: 'widget' } })).status).toBe('suspended');
      const resumed = run.resume({ step: 'work', resumeData: { go: true } });
      expect(await waitFor(async () => resumedStarted)).toBe(true);

      // A redelivered resume of an earlier attempt is refused under the same
      // generation; its settlement is not this resume's outcome.
      const workflows = (await storage.getStore('workflows'))!;
      const current = (await workflows.loadWorkflowSnapshot({
        workflowName: 'evented-resume-attempt-wf',
        runId: run.runId,
      }))!;
      await mastra.pubsub.publish('workflows-finish', {
        type: 'workflow.superseded',
        runId: run.runId,
        data: {
          workflowId: 'evented-resume-attempt-wf',
          runId: run.runId,
          executionGeneration: current.executionGeneration,
          lifecycleResumeAttempt: (current.lifecycleResumeAttempt ?? 0) - 1,
        },
      });
      await new Promise(resolve => setTimeout(resolve, 50));
      release();

      await expect(resumed).resolves.toMatchObject({ status: 'success', result: { done: true } });
    } finally {
      await mastra.stopWorkers?.();
    }
  });
});
