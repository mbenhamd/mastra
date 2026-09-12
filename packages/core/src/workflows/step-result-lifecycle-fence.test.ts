import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { EventEmitterPubSub } from '../events/event-emitter';
import { Mastra } from '../mastra';
import { MockStore } from '../storage/mock';
import { DefaultExecutionEngine } from './default';
import { createStep, createWorkflow } from './index';

const ioSchema = z.object({ value: z.string() });

describe('step-result lifecycle fence', () => {
  it('does not poll snapshots after a fenced step persist on a 3-step run', async () => {
    const storage = new MockStore();
    const pubsub = new EventEmitterPubSub();
    const workflow = createWorkflow({
      id: 'pf-3750-three-step',
      inputSchema: ioSchema,
      outputSchema: ioSchema,
    })
      .then(
        createStep({
          id: 'one',
          inputSchema: ioSchema,
          outputSchema: ioSchema,
          execute: async ({ inputData }) => inputData,
        }),
      )
      .then(
        createStep({
          id: 'two',
          inputSchema: ioSchema,
          outputSchema: ioSchema,
          execute: async ({ inputData }) => inputData,
        }),
      )
      .then(
        createStep({
          id: 'three',
          inputSchema: ioSchema,
          outputSchema: ioSchema,
          execute: async ({ inputData }) => inputData,
        }),
      )
      .commit();

    const mastra = new Mastra({ logger: false, storage, pubsub, workflows: { [workflow.id]: workflow } });
    const authority = vi.spyOn(DefaultExecutionEngine.prototype, 'getAuthoritativeExecutionDisposition');
    const run = await workflow.createRun();

    try {
      const result = await run.start({ inputData: { value: 'ok' } });

      expect(result.status).toBe('success');
      expect(result.result).toEqual({ value: 'ok' });
      expect(authority).not.toHaveBeenCalled();
    } finally {
      await mastra.shutdown();
      authority.mockRestore();
    }
  });

  it('retains a complete parallel failure snapshot and a serialized lifecycle error', async () => {
    const storage = new MockStore();
    const pubsub = new EventEmitterPubSub();
    const failedStepPersisted = Promise.withResolvers<void>();
    const workflowsStore = await storage.getStore('workflows');
    const persistWorkflowStepUpdate = workflowsStore!.persistWorkflowStepUpdate.bind(workflowsStore);
    const persistSpy = vi.spyOn(workflowsStore!, 'persistWorkflowStepUpdate').mockImplementation(async input => {
      const outcome = await persistWorkflowStepUpdate(input);
      if (
        outcome.status === 'persisted' &&
        input.lifecycleEvents?.some(event => event.type === 'step.failed' && event.stepId === 'fails')
      ) {
        failedStepPersisted.resolve();
      }
      return outcome;
    });
    const fails = createStep({
      id: 'fails',
      inputSchema: ioSchema,
      outputSchema: ioSchema,
      execute: async () => {
        throw new Error('fenced failure');
      },
    });
    const sibling = createStep({
      id: 'sibling',
      inputSchema: ioSchema,
      outputSchema: ioSchema,
      execute: async () => {
        await failedStepPersisted.promise;
        return { value: 'sibling' };
      },
    });
    const workflow = createWorkflow({
      id: 'pf-3750-failed-parallel',
      inputSchema: ioSchema,
      outputSchema: z.any(),
      steps: [fails, sibling],
    })
      .parallel([fails, sibling])
      .commit();

    const mastra = new Mastra({ logger: false, storage, pubsub, workflows: { [workflow.id]: workflow } });
    const run = await workflow.createRun();

    try {
      const result = await run.start({ inputData: { value: 'input' } });
      expect(result.status).toBe('failed');
      expect(result.error).toMatchObject({ message: 'fenced failure' });

      const snapshot = await workflowsStore?.loadWorkflowSnapshot({ workflowName: workflow.id, runId: run.runId });
      expect(snapshot?.context).toMatchObject({
        fails: { status: 'failed' },
        sibling: { status: 'success', output: { value: 'sibling' } },
      });
      expect(snapshot?.error).toMatchObject({ message: 'fenced failure' });
      const jsonOutbox = JSON.parse(JSON.stringify(snapshot?.lifecycleOutbox));
      expect(jsonOutbox).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            type: 'step.failed',
            error: expect.objectContaining({ message: 'fenced failure' }),
          }),
        ]),
      );
    } finally {
      await mastra.shutdown();
      persistSpy.mockRestore();
    }
  });

  it('retains state and closes lifecycle events across fresh-engine ordinary resumes', async () => {
    const storage = new MockStore();
    const pubsub = new EventEmitterPubSub();
    const publish = vi.spyOn(pubsub, 'publish');
    const makeWorkflow = () => {
      const approval = createStep({
        id: 'approval',
        inputSchema: ioSchema,
        outputSchema: ioSchema,
        stateSchema: ioSchema,
        suspendSchema: z.object({}),
        resumeSchema: ioSchema,
        execute: async ({ inputData, resumeData, setState, suspend, bail }) => {
          if (!resumeData) {
            await suspend({});
            return inputData;
          }
          if (resumeData.value === 'again') {
            await setState({ value: 'updated' });
            await suspend({});
            return inputData;
          }
          await setState({ value: 'updated' });
          return bail(resumeData);
        },
      });
      return createWorkflow({
        id: 'pf-3750-fresh-resume',
        inputSchema: ioSchema,
        outputSchema: ioSchema,
        stateSchema: ioSchema,
      })
        .then(approval)
        .commit();
    };
    const firstWorkflow = makeWorkflow();
    const firstMastra = new Mastra({
      logger: false,
      storage,
      pubsub,
      workflows: { [firstWorkflow.id]: firstWorkflow },
    });
    const firstRun = await firstWorkflow.createRun();

    try {
      await expect(
        firstRun.start({ inputData: { value: 'input' }, initialState: { value: 'initial' } }),
      ).resolves.toMatchObject({ status: 'suspended' });
      await firstMastra.shutdown();

      const secondWorkflow = makeWorkflow();
      const secondMastra = new Mastra({
        logger: false,
        storage,
        pubsub,
        workflows: { [secondWorkflow.id]: secondWorkflow },
      });
      try {
        const secondRun = await secondWorkflow.createRun({ runId: firstRun.runId });
        await expect(secondRun.resume({ step: 'approval', resumeData: { value: 'again' } })).resolves.toMatchObject({
          status: 'suspended',
        });
        const suspendedAgain = await (
          await storage.getStore('workflows')
        )?.loadWorkflowSnapshot({
          workflowName: secondWorkflow.id,
          runId: secondRun.runId,
        });
        expect(suspendedAgain).toMatchObject({ status: 'suspended', value: { value: 'updated' } });
      } finally {
        await secondMastra.shutdown();
      }

      const finalWorkflow = makeWorkflow();
      const finalMastra = new Mastra({
        logger: false,
        storage,
        pubsub,
        workflows: { [finalWorkflow.id]: finalWorkflow },
      });
      try {
        const finalRun = await finalWorkflow.createRun({ runId: firstRun.runId });
        await expect(finalRun.resume({ step: 'approval', resumeData: { value: 'approved' } })).resolves.toMatchObject({
          status: 'success',
          result: { value: 'approved' },
        });
        const finalSnapshot = await (
          await storage.getStore('workflows')
        )?.loadWorkflowSnapshot({
          workflowName: finalWorkflow.id,
          runId: finalRun.runId,
        });
        expect(finalSnapshot).toMatchObject({ status: 'success', value: { value: 'updated' } });
        const finalStepEvents = publish.mock.calls
          .map(([, event]) => event.data?.event)
          .filter(event => event?.stepId === 'approval');
        expect(finalStepEvents).toEqual(
          expect.arrayContaining([
            expect.objectContaining({ type: 'step.completed', stepAttempt: 3 }),
            expect.objectContaining({ type: 'step.finished', stepAttempt: 3, status: 'success' }),
          ]),
        );
      } finally {
        await finalMastra.shutdown();
      }
    } finally {
      await firstMastra.shutdown();
    }
  });

  it('falls back to authority before publishing when running snapshots are disabled', async () => {
    const storage = new MockStore();
    const pubsub = new EventEmitterPubSub();
    const publish = vi.spyOn(pubsub, 'publish');
    const workflowsStore = (await storage.getStore('workflows'))!;
    const canceledRemotely = createStep({
      id: 'canceled-remotely',
      inputSchema: ioSchema,
      outputSchema: ioSchema,
      execute: async ({ runId }) => {
        await workflowsStore.updateWorkflowState({
          workflowName: 'pf-3750-disabled-running-snapshots',
          runId,
          opts: { status: 'canceled' },
        });
        return { value: 'late' };
      },
    });
    const workflow = createWorkflow({
      id: 'pf-3750-disabled-running-snapshots',
      inputSchema: ioSchema,
      outputSchema: ioSchema,
      options: { shouldPersistSnapshot: ({ workflowStatus }) => workflowStatus !== 'running' },
    })
      .then(canceledRemotely)
      .commit();
    const mastra = new Mastra({ logger: false, storage, pubsub, workflows: { [workflow.id]: workflow } });
    const run = await workflow.createRun();

    try {
      await expect(run.start({ inputData: { value: 'input' } })).resolves.toMatchObject({ status: 'canceled' });
      expect(publish.mock.calls.map(([, event]) => event.data?.event?.type)).not.toContain('step.completed');
    } finally {
      await mastra.shutdown();
    }
  });

  it('honors lifecycle pruning with a status-only fenced adapter', async () => {
    const storage = new MockStore();
    const pubsub = new EventEmitterPubSub();
    const publish = vi.spyOn(pubsub, 'publish');
    const workflowsStore = (await storage.getStore('workflows'))!;
    const persistWorkflowStepUpdate = workflowsStore.persistWorkflowStepUpdate.bind(workflowsStore);
    const persistSpy = vi.spyOn(workflowsStore, 'persistWorkflowStepUpdate').mockImplementation(async input => {
      const outcome = await persistWorkflowStepUpdate(input);
      return { status: outcome.status };
    });
    const fails = createStep({
      id: 'status-only-failure',
      inputSchema: ioSchema,
      outputSchema: ioSchema,
      execute: async () => {
        throw new Error('status-only failure');
      },
    });
    const workflow = createWorkflow({
      id: 'pf-3750-status-only-adapter',
      inputSchema: ioSchema,
      outputSchema: ioSchema,
      options: {
        pruneSnapshot: ({ snapshot }) => {
          const { lifecycleOutbox: _lifecycleOutbox, ...pruned } = snapshot;
          return pruned;
        },
      },
    })
      .then(fails)
      .commit();
    const mastra = new Mastra({ logger: false, storage, pubsub, workflows: { [workflow.id]: workflow } });
    const run = await workflow.createRun();

    try {
      await expect(run.start({ inputData: { value: 'input' } })).resolves.toMatchObject({
        status: 'failed',
        error: expect.objectContaining({ message: 'status-only failure' }),
      });
      const snapshot = await workflowsStore.loadWorkflowSnapshot({ workflowName: workflow.id, runId: run.runId });
      expect(snapshot).toMatchObject({ status: 'failed', error: { message: 'status-only failure' } });
      expect(snapshot?.lifecycleOutbox).toBeUndefined();
      expect(publish.mock.calls.map(([, event]) => event.data?.event?.type)).not.toContain('step.failed');
    } finally {
      await mastra.shutdown();
      persistSpy.mockRestore();
    }
  });
});
