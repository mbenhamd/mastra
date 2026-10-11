import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { ErrorCategory, ErrorDomain, MastraError } from '../error';
import { EventEmitterPubSub } from '../events/event-emitter';
import { Mastra } from '../mastra';
import { getStoragePersistenceFailure } from '../storage';
import type { StoragePersistenceFailure } from '../storage';
import { MockStore } from '../storage/mock';
import { createWorkflow } from './create';
import { createStep as createEventedStep, createWorkflow as createEventedWorkflow } from './evented/workflow';
import { createStep } from './workflow';

const WORKFLOW_ID = 'error-contract-wf';

function createApprovalWorkflow() {
  const approval = createStep({
    id: 'approval',
    inputSchema: z.object({ item: z.string() }),
    outputSchema: z.object({ item: z.string(), approved: z.boolean() }),
    resumeSchema: z.object({ approved: z.boolean() }),
    execute: async ({ inputData, resumeData, suspend }) => {
      if (!resumeData) {
        await suspend({});
        return { item: inputData.item, approved: false };
      }
      return { item: inputData.item, approved: resumeData.approved };
    },
  });
  const downstream = vi.fn(async ({ inputData }: { inputData: { item: string; approved: boolean } }) => ({
    done: inputData.approved,
  }));
  const finish = createStep({
    id: 'finish',
    inputSchema: z.object({ item: z.string(), approved: z.boolean() }),
    outputSchema: z.object({ done: z.boolean() }),
    execute: downstream,
  });
  const workflow = createWorkflow({
    id: WORKFLOW_ID,
    inputSchema: z.object({ item: z.string() }),
    outputSchema: z.object({ done: z.boolean() }),
    steps: [approval, finish],
  })
    .then(approval)
    .then(finish)
    .commit();
  return { workflow, downstream };
}

async function setup() {
  const { workflow, downstream } = createApprovalWorkflow();
  const storage = new MockStore();
  const mastra = new Mastra({ storage, workflows: { [WORKFLOW_ID]: workflow }, logger: false });
  const workflowsStore = (await storage.getStore('workflows'))!;
  return { workflow, downstream, mastra, workflowsStore };
}

async function rejection(promise: Promise<unknown>): Promise<unknown> {
  return promise.then(
    () => {
      throw new Error('expected the operation to reject');
    },
    (error: unknown) => error,
  );
}

function classifiedStorageFailure(kind: StoragePersistenceFailure): MastraError {
  return new MastraError(
    {
      id: 'MASTRA_STORAGE_TEST_PERSIST_WORKFLOW_STEP_UPDATE_FAILED',
      domain: ErrorDomain.STORAGE,
      category: ErrorCategory.THIRD_PARTY,
      details: { persistenceFailure: kind },
    },
    new Error('connection lost'),
  );
}

describe('workflow lifecycle error ids', () => {
  it('rejects resume of a run that is no longer suspended with WORKFLOW_RUN_NOT_SUSPENDED', async () => {
    const { workflow, mastra } = await setup();
    try {
      const run = await workflow.createRun();
      expect((await run.start({ inputData: { item: 'a' } })).status).toBe('suspended');
      expect((await run.resume({ step: 'approval', resumeData: { approved: true } })).status).toBe('success');

      const error = await rejection(run.resume({ step: 'approval', resumeData: { approved: true } }));

      expect(error).toBeInstanceOf(MastraError);
      expect(error).toMatchObject({
        id: 'WORKFLOW_RUN_NOT_SUSPENDED',
        domain: ErrorDomain.MASTRA_WORKFLOW,
        category: ErrorCategory.USER,
        message: 'This workflow run was not suspended',
        details: { workflowId: WORKFLOW_ID, runId: run.runId, actualStatus: 'success' },
      });
      // Server error handling reads details.status as the HTTP status code.
      expect((error as MastraError).details).not.toHaveProperty('status');
    } finally {
      await mastra.shutdown();
    }
  });

  it('rejects restart of a run that is not running or waiting with WORKFLOW_RUN_NOT_ACTIVE', async () => {
    const { workflow, mastra } = await setup();
    try {
      const run = await workflow.createRun();
      expect((await run.start({ inputData: { item: 'a' } })).status).toBe('suspended');

      const error = await rejection(run.restart());

      expect(error).toMatchObject({
        id: 'WORKFLOW_RUN_NOT_ACTIVE',
        category: ErrorCategory.USER,
        message: 'This workflow run was not active',
        details: { runId: run.runId, actualStatus: 'suspended' },
      });
    } finally {
      await mastra.shutdown();
    }
  });

  it('rejects an evented resume of a run that is no longer suspended with WORKFLOW_RUN_NOT_SUSPENDED', async () => {
    const approval = createEventedStep({
      id: 'approval',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      resumeSchema: z.object({ approved: z.boolean() }),
      suspendSchema: z.object({}),
      execute: async ({ resumeData, suspend }) => {
        if (!resumeData) await suspend({});
        return {};
      },
    });
    const workflow = createEventedWorkflow({
      id: 'evented-error-contract',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    })
      .then(approval)
      .commit();
    const mastra = new Mastra({
      storage: new MockStore(),
      pubsub: new EventEmitterPubSub(),
      workflows: { [workflow.id]: workflow },
      logger: false,
    });
    await mastra.startWorkers();
    try {
      const run = await workflow.createRun();
      expect((await run.start({ inputData: {} })).status).toBe('suspended');
      expect((await run.resume({ step: 'approval', resumeData: { approved: true } })).status).toBe('success');

      const error = await rejection(run.resume({ step: 'approval', resumeData: { approved: true } }));

      expect(error).toMatchObject({
        id: 'WORKFLOW_RUN_NOT_SUSPENDED',
        category: ErrorCategory.USER,
        details: { workflowId: workflow.id, runId: run.runId, actualStatus: 'success' },
      });
    } finally {
      await mastra.stopWorkers();
      await mastra.shutdown();
    }
  });

  it('rejects timeTravel of a run that is still running with WORKFLOW_RUN_STILL_RUNNING', async () => {
    const { workflow, mastra, workflowsStore } = await setup();
    try {
      const run = await workflow.createRun();
      expect((await run.start({ inputData: { item: 'a' } })).status).toBe('suspended');
      await workflowsStore.updateWorkflowState({
        workflowName: WORKFLOW_ID,
        runId: run.runId,
        opts: { status: 'running' },
      });

      const error = await rejection(run.timeTravel({ step: 'approval', inputData: { item: 'a' } }));

      expect(error).toMatchObject({
        id: 'WORKFLOW_RUN_STILL_RUNNING',
        category: ErrorCategory.USER,
        message: 'This workflow run is still running, cannot time travel',
        details: { workflowId: WORKFLOW_ID, runId: run.runId, actualStatus: 'running' },
      });
    } finally {
      await mastra.shutdown();
    }
  });

  it.each([
    ['resume', (run: any) => run.resume({ step: 'approval', resumeData: { approved: true } })],
    ['restart', (run: any) => run.restart()],
    ['timeTravel', (run: any) => run.timeTravel({ step: 'approval', inputData: { item: 'a' } })],
  ])('rejects %s of a run without a stored snapshot with WORKFLOW_SNAPSHOT_NOT_FOUND', async (_name, operate) => {
    const { workflow, mastra, workflowsStore } = await setup();
    try {
      const run = await workflow.createRun({ runId: 'never-started' });
      await workflowsStore.deleteWorkflowRunById({ workflowName: WORKFLOW_ID, runId: 'never-started' });

      const error = await rejection(operate(run));

      expect(error).toMatchObject({
        id: 'WORKFLOW_SNAPSHOT_NOT_FOUND',
        category: ErrorCategory.USER,
        details: { workflowId: WORKFLOW_ID, runId: 'never-started' },
      });
    } finally {
      await mastra.shutdown();
    }
  });
});

describe('workflow storage persistence failures', () => {
  it.each(['transient', 'commit_unknown', 'permanent'] as const)(
    'surfaces a %s terminal-write failure to the resume caller and leaves the durable run as storage has it',
    async kind => {
      const { workflow, downstream, mastra, workflowsStore } = await setup();
      try {
        const run = await workflow.createRun();
        expect((await run.start({ inputData: { item: 'a' } })).status).toBe('suspended');

        const persistStepUpdate = workflowsStore.persistWorkflowStepUpdate.bind(workflowsStore);
        const stepWrites = vi.spyOn(workflowsStore, 'persistWorkflowStepUpdate');
        const snapshotWrites = vi.spyOn(workflowsStore, 'persistWorkflowSnapshot');
        const stateWrites = vi.spyOn(workflowsStore, 'updateWorkflowState');
        stepWrites.mockImplementationOnce(async input => {
          // A commit-unknown write committed although its caller saw a
          // failure; the other outcomes did not apply.
          if (kind === 'commit_unknown') await persistStepUpdate(input);
          throw classifiedStorageFailure(kind);
        });

        const error = await rejection(run.resume({ step: 'approval', resumeData: { approved: true } }));

        expect(getStoragePersistenceFailure(error)).toBe(kind);
        expect(downstream).toHaveBeenCalledTimes(1);
        // The engine neither retries the failed write nor records another
        // outcome over whatever it left behind. Only the resume claim wrote
        // run state before it.
        expect(stepWrites).toHaveBeenCalledTimes(1);
        expect(snapshotWrites).not.toHaveBeenCalled();
        expect(stateWrites).toHaveBeenCalledTimes(1);

        const durable = await workflowsStore.loadWorkflowSnapshot({ workflowName: WORKFLOW_ID, runId: run.runId });
        expect(durable?.lifecycleResumeAttempt).toBe(1);
        expect(durable?.status).toBe(kind === 'commit_unknown' ? 'success' : 'running');
      } finally {
        await mastra.shutdown();
      }
    },
  );

  it('does not re-run a nested workflow whose terminal write is commit_unknown', async () => {
    const childEffect = vi.fn(async () => ({ ok: true }));
    const childStep = createStep({
      id: 'child-step',
      inputSchema: z.object({}),
      outputSchema: z.object({ ok: z.boolean() }),
      execute: childEffect,
    });
    const child = createWorkflow({
      id: 'nested-child-wf',
      inputSchema: z.object({}),
      outputSchema: z.object({ ok: z.boolean() }),
      steps: [childStep],
    })
      .then(childStep)
      .commit();
    const parent = createWorkflow({
      id: 'nested-parent-wf',
      inputSchema: z.object({}),
      outputSchema: z.object({ ok: z.boolean() }),
      steps: [child],
      retryConfig: { attempts: 1 },
    })
      .then(child)
      .commit();
    const storage = new MockStore();
    const mastra = new Mastra({ storage, workflows: { 'nested-parent-wf': parent }, logger: false });
    try {
      const workflowsStore = (await storage.getStore('workflows'))!;
      const persistStepUpdate = workflowsStore.persistWorkflowStepUpdate.bind(workflowsStore);
      let failed = false;
      vi.spyOn(workflowsStore, 'persistWorkflowStepUpdate').mockImplementation(async input => {
        const result = await persistStepUpdate(input);
        // The child's terminal write commits, but its caller sees a failure.
        if (!failed && input.workflowName === 'nested-child-wf' && input.snapshot.status === 'success') {
          failed = true;
          throw classifiedStorageFailure('commit_unknown');
        }
        return result;
      });
      const run = await parent.createRun();

      const error = await rejection(run.start({ inputData: {} }));

      expect(failed).toBe(true);
      expect(getStoragePersistenceFailure(error)).toBe('commit_unknown');
      // The parent's step retry would replay the child's effects over its
      // committed terminal state.
      expect(childEffect).toHaveBeenCalledTimes(1);
      const durableParent = await workflowsStore.loadWorkflowSnapshot({
        workflowName: 'nested-parent-wf',
        runId: run.runId,
      });
      expect(durableParent?.status).toBe('running');
    } finally {
      await mastra.shutdown();
    }
  });

  it('keeps an unclassified in-memory storage failure unclassified', async () => {
    const { workflowsStore } = await setup();
    await workflowsStore.persistWorkflowSnapshot({
      workflowName: WORKFLOW_ID,
      runId: 'fenced',
      snapshot: { runId: 'fenced', status: 'running', context: {}, executionGeneration: 'a' } as any,
    });

    const error = await rejection(
      workflowsStore.persistWorkflowSnapshot({
        workflowName: WORKFLOW_ID,
        runId: 'fenced',
        snapshot: { runId: 'fenced', status: 'running', context: {}, executionGeneration: 'b' } as any,
        expectedExecutionGeneration: 'b',
      }),
    );

    expect(error).toBeInstanceOf(Error);
    expect(getStoragePersistenceFailure(error)).toBeUndefined();
  });
});
