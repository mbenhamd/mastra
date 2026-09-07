import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { EventEmitterPubSub } from '../../events/event-emitter';
import { Mastra } from '../../mastra';
import { MockStore } from '../../storage/mock';
import { EventedRun, createStep, createWorkflow } from './workflow';

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>(done => {
    resolve = done;
  });
  return { promise, resolve };
}

async function fixture(allowUnclaimedResumes?: boolean) {
  const storage = new MockStore();
  const pubsub = new EventEmitterPubSub();
  const started = deferred();
  const release = deferred();
  let effects = 0;
  const makeWorkflow = () => {
    const approval = createStep({
      id: 'approval',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      resumeSchema: z.object({ approved: z.boolean() }),
      suspendSchema: z.object({}),
      execute: async ({ resumeData, suspend }) => {
        if (!resumeData) {
          await suspend({});
          return {};
        }
        effects++;
        started.resolve();
        await release.promise;
        return {};
      },
    });
    return createWorkflow({
      id: 'evented-resume-claim',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      options: { shouldPersistSnapshot: ({ workflowStatus }) => workflowStatus !== 'running', allowUnclaimedResumes },
    })
      .then(approval)
      .commit();
  };
  const workflowA = makeWorkflow();
  const workflowB = makeWorkflow();
  const mastraA = new Mastra({ storage, pubsub, workflows: { [workflowA.id]: workflowA }, logger: false });
  const mastraB = new Mastra({ storage, pubsub, workflows: { [workflowB.id]: workflowB }, logger: false });
  await mastraA.startWorkers();
  const runA = await workflowA.createRun();
  expect(runA).toBeInstanceOf(EventedRun);
  expect((await runA.start({ inputData: {} })).status).toBe('suspended');
  const runB = await workflowB.createRun({ runId: runA.runId });
  expect(runB).toBeInstanceOf(EventedRun);
  expect(runB).not.toBe(runA);
  const store = (await storage.getStore('workflows'))!;
  const address = { workflowName: workflowA.id, runId: runA.runId };
  const originalPublish = pubsub.publish.bind(pubsub);
  const publish = vi.spyOn(pubsub, 'publish');
  const cleanup = async () => {
    release.resolve();
    vi.restoreAllMocks();
    await mastraA.stopWorkers();
    await Promise.all([mastraA.shutdown(), mastraB.shutdown()]);
  };
  return {
    store,
    address,
    publish,
    originalPublish,
    runA,
    runB,
    started,
    release,
    logger: mastraA.getLogger(),
    effects: () => effects,
    cleanup,
  };
}

describe('evented public resume claim', () => {
  it.each([true, false])('honors allowUnclaimedResumes=%s on a nonconcurrent store', async allowUnclaimedResumes => {
    const f = await fixture(allowUnclaimedResumes);
    try {
      vi.spyOn(f.store, 'supportsConcurrentUpdates').mockReturnValue(false);
      const warning = vi.spyOn(f.logger, 'warn');
      f.release.resolve();
      await expect(f.runA.resume({ resumeData: { approved: true } })).resolves.toMatchObject({ status: 'success' });
      expect(warning.mock.calls.some(([message]) => String(message).includes('cannot be de-duplicated'))).toBe(
        !allowUnclaimedResumes,
      );
    } finally {
      await f.cleanup();
    }
  });
  it.each([true, false])(
    'admits one independent caller with approval=%s and running snapshots disabled',
    async approved => {
      const f = await fixture();
      try {
        const bothLoaded = deferred();
        let reads = 0;
        const load = f.store.loadWorkflowSnapshot.bind(f.store);
        vi.spyOn(f.store, 'loadWorkflowSnapshot').mockImplementation(async input => {
          const snapshot = await load(input);
          if (++reads <= 2) {
            if (reads === 2) bothLoaded.resolve();
            await bothLoaded.promise;
          }
          return snapshot;
        });
        const results = Promise.allSettled([
          f.runA.resume({ resumeData: { approved } }),
          f.runB.resume({ resumeData: { approved } }),
        ]);
        await f.started.promise;
        f.release.resolve();
        const settled = await results;
        expect(settled.filter(result => result.status === 'fulfilled')).toHaveLength(1);
        expect(settled.filter(result => result.status === 'rejected')).toMatchObject([
          { reason: { id: 'WORKFLOW_RESUME_ALREADY_CLAIMED' } },
        ]);
        expect(f.effects()).toBe(1);
        expect(
          f.publish.mock.calls.filter(([topic, event]) => topic === 'workflows' && event.type === 'workflow.resume'),
        ).toHaveLength(1);
      } finally {
        await f.cleanup();
      }
    },
  );

  it.each(['generation', 'attempt'] as const)('rejects a stale %s before dispatch', async competitor => {
    const f = await fixture();
    try {
      const update = f.store.updateWorkflowState.bind(f.store);
      vi.spyOn(f.store, 'updateWorkflowState').mockImplementation(async input => {
        if (input.opts.expectedStatus === 'suspended') {
          await update({
            ...f.address,
            opts:
              competitor === 'generation' ? { executionGeneration: 'wfeg:replacement' } : { lifecycleResumeAttempt: 1 },
          });
        }
        return update(input);
      });
      await expect(f.runA.resume({ resumeData: { approved: true } })).rejects.toMatchObject({
        id: 'WORKFLOW_RESUME_ALREADY_CLAIMED',
      });
      expect(f.effects()).toBe(0);
      expect(
        f.publish.mock.calls.filter(([topic, event]) => topic === 'workflows' && event.type === 'workflow.resume'),
      ).toHaveLength(0);
    } finally {
      await f.cleanup();
    }
  });

  it('keeps a claim consumed after resume dispatch fails', async () => {
    const f = await fixture();
    try {
      f.publish.mockImplementation(async (topic, event, options) => {
        if (topic === 'workflows' && event.type === 'workflow.resume') throw new Error('dispatch failed');
        return f.originalPublish(topic, event, options);
      });
      await expect(f.runA.resume({ resumeData: { approved: true } })).rejects.toThrow('dispatch failed');
      expect(await f.store.loadWorkflowSnapshot(f.address)).toMatchObject({
        status: 'running',
        lifecycleResumeAttempt: 1,
      });
      expect(f.effects()).toBe(0);
    } finally {
      await f.cleanup();
    }
  });

  it('settles when cancellation finishes while the claim acknowledgement is delayed', async () => {
    const f = await fixture();
    const claimCommitted = deferred();
    const acknowledgeClaim = deferred();
    const canceledPublished = deferred();
    try {
      const update = f.store.updateWorkflowState.bind(f.store);
      vi.spyOn(f.store, 'updateWorkflowState').mockImplementation(async input => {
        const result = await update(input);
        if (input.opts.expectedStatus === 'suspended' && result) {
          claimCommitted.resolve();
          await acknowledgeClaim.promise;
        }
        return result;
      });
      f.publish.mockImplementation(async (topic, event, options) => {
        const result = await f.originalPublish(topic, event, options);
        if (topic === 'workflows-finish' && event.type === 'workflow.end') canceledPublished.resolve();
        return result;
      });
      const rejected = expect(f.runA.resume({ resumeData: { approved: true } })).rejects.toMatchObject({
        id: 'WORKFLOW_RESUME_ALREADY_CLAIMED',
      });
      await claimCommitted.promise;
      await f.runB.cancel();
      await canceledPublished.promise;
      acknowledgeClaim.resolve();
      await rejected;
      expect(await f.store.loadWorkflowSnapshot(f.address)).toMatchObject({ status: 'canceled' });
      expect(f.effects()).toBe(0);
      expect(
        f.publish.mock.calls.filter(([topic, event]) => topic === 'workflows' && event.type === 'workflow.resume'),
      ).toHaveLength(0);
    } finally {
      acknowledgeClaim.resolve();
      await f.cleanup();
    }
  });
});
