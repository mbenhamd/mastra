import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { Mastra } from '../mastra';
import { MockStore } from '../storage/mock';
import { createWorkflow as createEventedWorkflow } from './evented';
import type { WorkflowRunState } from './types';
import { createStep, createWorkflow } from './index';

const ioSchema = z.object({ value: z.string() });

function defineCreateRunStorageReadTests(
  engine: string,
  workflowFactory: (config: Record<string, unknown>) => ReturnType<typeof createWorkflow>,
) {
  const buildWorkflow = (id: string, shouldPersistSnapshot: boolean) =>
    workflowFactory({
      id,
      inputSchema: ioSchema,
      outputSchema: ioSchema,
      options: { shouldPersistSnapshot: () => shouldPersistSnapshot },
    })
      .then(
        createStep({
          id: 'passthrough',
          inputSchema: ioSchema,
          outputSchema: ioSchema,
          execute: async ({ inputData }) => inputData,
        }),
      )
      .commit();

  describe(`${engine} createRun storage existence read`, () => {
    it('skips a guaranteed-miss read for a generated transient run', async () => {
      const storage = new MockStore();
      const workflow = buildWorkflow(`${engine}-transient-generated`, false);
      new Mastra({ logger: false, storage, workflows: { [workflow.id]: workflow } });
      const workflowsStore = (await storage.getStore('workflows'))!;
      const read = vi.spyOn(workflowsStore, 'getWorkflowRunById');
      const persist = vi.spyOn(workflowsStore, 'persistWorkflowSnapshot');

      await workflow.createRun();

      expect(read).not.toHaveBeenCalled();
      expect(persist).not.toHaveBeenCalled();
    });

    it('still reads storage for a persisting workflow', async () => {
      const storage = new MockStore();
      const workflow = buildWorkflow(`${engine}-persisting-generated`, true);
      new Mastra({ logger: false, storage, workflows: { [workflow.id]: workflow } });
      const workflowsStore = (await storage.getStore('workflows'))!;
      const read = vi.spyOn(workflowsStore, 'getWorkflowRunById');

      await workflow.createRun();

      expect(read).toHaveBeenCalledTimes(1);
    });

    it('still reads storage when a transient workflow receives an explicit runId', async () => {
      const storage = new MockStore();
      const workflow = buildWorkflow(`${engine}-transient-explicit`, false);
      new Mastra({ logger: false, storage, workflows: { [workflow.id]: workflow } });
      const workflowsStore = (await storage.getStore('workflows'))!;
      const read = vi.spyOn(workflowsStore, 'getWorkflowRunById');

      await workflow.createRun({ runId: 'caller-owned-run-id' });

      expect(read).toHaveBeenCalledTimes(1);
    });
  });
}

defineCreateRunStorageReadTests('default', createWorkflow);
defineCreateRunStorageReadTests('evented', createEventedWorkflow as never);

function buildExplicitlyTransientWorkflow(id: string) {
  return createWorkflow({
    id,
    inputSchema: ioSchema,
    outputSchema: ioSchema,
    options: { executionMode: 'transient' },
  })
    .then(
      createStep({
        id: 'passthrough',
        inputSchema: ioSchema,
        outputSchema: ioSchema,
        execute: async ({ inputData }) => inputData,
      }),
    )
    .commit();
}

describe('explicitly transient workflow lifecycle reads', () => {
  it('skips lifecycle reads for a built-in generated run ID', async () => {
    const workflow = buildExplicitlyTransientWorkflow('explicit-transient-generated');
    const storage = new MockStore();
    new Mastra({ logger: false, storage, workflows: { [workflow.id]: workflow } });
    const workflowsStore = (await storage.getStore('workflows'))!;
    const read = vi.spyOn(workflowsStore, 'loadWorkflowSnapshot');

    const run = await workflow.createRun();
    await run.start({ inputData: { value: 'generated' } });

    expect(run.transientExecution).toBe(true);
    expect(read).not.toHaveBeenCalled();
  });

  it('propagates transient execution through parallel child contexts', async () => {
    const first = createStep({
      id: 'parallel-first',
      inputSchema: ioSchema,
      outputSchema: ioSchema,
      execute: async ({ inputData }) => inputData,
    });
    const second = createStep({
      id: 'parallel-second',
      inputSchema: ioSchema,
      outputSchema: ioSchema,
      execute: async ({ inputData }) => inputData,
    });
    const workflow = createWorkflow({
      id: 'explicit-transient-parallel',
      inputSchema: ioSchema,
      outputSchema: z.any(),
      options: { executionMode: 'transient' },
    })
      .parallel([first, second])
      .commit();
    const storage = new MockStore();
    new Mastra({ logger: false, storage, workflows: { [workflow.id]: workflow } });
    const workflowsStore = (await storage.getStore('workflows'))!;
    const read = vi.spyOn(workflowsStore, 'loadWorkflowSnapshot');

    const run = await workflow.createRun();
    await run.start({ inputData: { value: 'parallel' } });

    expect(run.transientExecution).toBe(true);
    expect(read).not.toHaveBeenCalled();
  });

  it('cancels a transient run without creating durable state', async () => {
    const workflow = buildExplicitlyTransientWorkflow('explicit-transient-cancel');
    const storage = new MockStore();
    new Mastra({ logger: false, storage, workflows: { [workflow.id]: workflow } });
    const workflowsStore = (await storage.getStore('workflows'))!;
    const read = vi.spyOn(workflowsStore, 'loadWorkflowSnapshot');
    const update = vi.spyOn(workflowsStore, 'updateWorkflowState');

    const run = await workflow.createRun();
    await run.cancel();

    expect(run.workflowRunStatus).toBe('canceled');
    expect(read).not.toHaveBeenCalled();
    expect(update).not.toHaveBeenCalled();
  });

  it('rejects durable replay operations without reading storage', async () => {
    const workflow = buildExplicitlyTransientWorkflow('explicit-transient-replay');
    const storage = new MockStore();
    new Mastra({ logger: false, storage, workflows: { [workflow.id]: workflow } });
    const workflowsStore = (await storage.getStore('workflows'))!;
    const read = vi.spyOn(workflowsStore, 'loadWorkflowSnapshot');

    const run = await workflow.createRun();

    await expect(run.resume({ resumeData: { value: 'resume' } })).rejects.toThrow(
      'Transient workflow runs cannot resume',
    );
    await expect(run.restart()).rejects.toThrow('Transient workflow runs cannot restart');
    await expect(run.timeTravel({ step: 'passthrough', inputData: { value: 'travel' } })).rejects.toThrow(
      'Transient workflow runs cannot time travel',
    );
    expect(read).not.toHaveBeenCalled();
  });

  it('retains lifecycle reads for an explicit run ID', async () => {
    const workflow = buildExplicitlyTransientWorkflow('explicit-transient-caller-id');
    const storage = new MockStore();
    new Mastra({ logger: false, storage, workflows: { [workflow.id]: workflow } });
    const workflowsStore = (await storage.getStore('workflows'))!;
    const read = vi.spyOn(workflowsStore, 'loadWorkflowSnapshot');

    const run = await workflow.createRun({ runId: 'caller-owned-run-id' });
    await run.start({ inputData: { value: 'explicit' } });

    expect(run.transientExecution).toBe(false);
    expect(read).toHaveBeenCalled();
  });

  it('retains lifecycle reads for a custom-generated run ID', async () => {
    const workflow = buildExplicitlyTransientWorkflow('explicit-transient-custom-id');
    const storage = new MockStore();
    new Mastra({
      idGenerator: () => 'custom-generated-run-id',
      logger: false,
      storage,
      workflows: { [workflow.id]: workflow },
    });
    const workflowsStore = (await storage.getStore('workflows'))!;
    const read = vi.spyOn(workflowsStore, 'loadWorkflowSnapshot');

    const run = await workflow.createRun();
    await run.start({ inputData: { value: 'custom' } });

    expect(run.transientExecution).toBe(false);
    expect(read).toHaveBeenCalled();
  });
});

function defineCustomIdGeneratorCollisionTest(
  engine: string,
  workflowFactory: (config: Record<string, unknown>) => ReturnType<typeof createWorkflow>,
) {
  describe(`${engine} createRun custom ID generator collision`, () => {
    it('retains the storage read and synchronizes status for a deterministic generated ID', async () => {
      const runId = 'deterministic-run-id';
      const workflow = workflowFactory({
        id: `${engine}-transient-deterministic`,
        inputSchema: ioSchema,
        outputSchema: ioSchema,
        options: { shouldPersistSnapshot: () => false },
      })
        .then(
          createStep({
            id: 'passthrough',
            inputSchema: ioSchema,
            outputSchema: ioSchema,
            execute: async ({ inputData }) => inputData,
          }),
        )
        .commit();
      const storage = new MockStore();
      const workflowsStore = (await storage.getStore('workflows'))!;
      await workflowsStore.persistWorkflowSnapshot({
        workflowName: workflow.id,
        runId,
        snapshot: {
          runId,
          status: 'success',
          value: {},
          context: {},
          activePaths: [],
          activeStepsPath: {},
          suspendedPaths: {},
          resumeLabels: {},
          serializedStepGraph: [],
          waitingPaths: {},
          timestamp: Date.now(),
        } as WorkflowRunState,
      });
      new Mastra({
        idGenerator: () => runId,
        logger: false,
        storage,
        workflows: { [workflow.id]: workflow },
      });
      const read = vi.spyOn(workflowsStore, 'getWorkflowRunById');

      const run = await workflow.createRun();

      expect(read).toHaveBeenCalledWith({ runId, workflowName: workflow.id });
      expect(run.workflowRunStatus).toBe('success');
    });
  });
}

defineCustomIdGeneratorCollisionTest('default', createWorkflow);
defineCustomIdGeneratorCollisionTest('evented', createEventedWorkflow as never);
