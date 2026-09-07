import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { EventEmitterPubSub } from '../events/event-emitter';
import { Mastra } from '../mastra';
import { MockStore } from '../storage/mock';
import { createStep, createWorkflow } from './index';

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  const promise = new Promise<T>(resolvePromise => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

describe('default cancellation acknowledgement across time-travel generations', () => {
  it('does not let an older cancel acknowledgement abort the newer generation', async () => {
    const storage = new MockStore();
    const pubsub = new EventEmitterPubSub();
    const aEntered = deferred<void>();
    const aRelease = deferred<void>();
    const bEntered = deferred<void>();
    const bRelease = deferred<void>();
    const aFinished = deferred<void>();
    const acknowledgement = deferred<void>();
    let invocation = 0;
    let bAbortSignal!: AbortSignal;
    const finishStatuses: string[] = [];
    const step = createStep({
      id: 'generation-step',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      execute: async ({ abortSignal }) => {
        invocation++;
        if (invocation === 1) {
          aEntered.resolve();
          await aRelease.promise;
          aFinished.resolve();
        } else {
          bAbortSignal = abortSignal;
          bEntered.resolve();
          await bRelease.promise;
        }
        return {};
      },
    });
    const workflow = createWorkflow({
      id: 'conditional-cancel-generation',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      options: {
        onFinish: async ({ status }) => {
          finishStatuses.push(status);
        },
      },
    })
      .then(step)
      .commit();
    const mastra = new Mastra({ logger: false, storage, pubsub, workflows: { [workflow.id]: workflow } });
    const run = await workflow.createRun({ runId: 'conditional-cancel-generation-run' });
    const workflowsStore = (await storage.getStore('workflows'))!;
    const originalUpdate = workflowsStore.updateWorkflowState.bind(workflowsStore);
    let cancellationCommitted = false;
    const update = vi.spyOn(workflowsStore, 'updateWorkflowState').mockImplementation(async args => {
      const result = await originalUpdate(args);
      if (args.opts.status === 'canceled' && result && !cancellationCommitted) {
        cancellationCommitted = true;
        await acknowledgement.promise;
      }
      return result;
    });

    try {
      const first = run.start({ inputData: {} });
      await aEntered.promise;
      const firstSnapshot = await workflowsStore.loadWorkflowSnapshot({ workflowName: workflow.id, runId: run.runId });
      const firstGeneration = firstSnapshot?.executionGeneration;

      const cancel = run.cancel();
      await vi.waitFor(async () => {
        await expect(
          workflowsStore.loadWorkflowSnapshot({ workflowName: workflow.id, runId: run.runId }),
        ).resolves.toMatchObject({
          status: 'canceled',
        });
      });

      aRelease.resolve();
      await aFinished.promise;

      const second = run.timeTravel({ step: 'generation-step', inputData: {} });
      await bEntered.promise;
      const secondSnapshot = await workflowsStore.loadWorkflowSnapshot({ workflowName: workflow.id, runId: run.runId });
      expect(secondSnapshot).toMatchObject({ status: 'running', executionGeneration: expect.any(String) });
      expect(secondSnapshot?.executionGeneration).not.toBe(firstGeneration);
      expect(bAbortSignal.aborted).toBe(false);
      expect(finishStatuses).not.toContain('success');

      acknowledgement.resolve();
      await cancel;
      await expect(first).resolves.toMatchObject({ status: 'canceled' });
      await Promise.resolve();
      expect(bAbortSignal.aborted).toBe(false);
      expect(run.workflowRunStatus).toBe('running');
      expect(finishStatuses).not.toContain('success');

      bRelease.resolve();
      await expect(second).resolves.toMatchObject({ status: 'success' });
      expect(bAbortSignal.aborted).toBe(false);
      expect(finishStatuses).toContain('success');
    } finally {
      aRelease.resolve();
      bRelease.resolve();
      acknowledgement.resolve();
      update.mockRestore();
      await mastra.shutdown();
    }
  });
});
