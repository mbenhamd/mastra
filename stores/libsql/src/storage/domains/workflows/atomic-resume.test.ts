import {
  expectAtomicWorkflowResumeRunFenceContract,
  expectAtomicWorkflowResumeStorageContract,
} from '@internal/storage-test-utils';
import { createClient } from '@libsql/client';
import { Mastra } from '@mastra/core/mastra';
import { InMemoryDB, TABLE_SCHEMAS, TABLE_WORKFLOW_SNAPSHOT, WorkflowsInMemory } from '@mastra/core/storage';
import { createStep, createWorkflow } from '@mastra/core/workflows';
import type { WorkflowRunState } from '@mastra/core/workflows';
import { afterEach, describe, expect, it } from 'vitest';
import { z } from 'zod/v4';
import { LibSQLStore } from '../..';
import { LibSQLDB } from '../../db';
import { WorkflowsLibSQL } from '.';

describe('atomic workflow resume storage contract', () => {
  it('retains an immediate cancellation and refuses a retargeted marker guard', async () => {
    const store = new LibSQLStore({ id: 'retained-cancel-bare-memory', url: ':memory:' });
    const schema = z.object({ topic: z.string() });
    const approval = createStep({
      id: 'approval',
      inputSchema: schema,
      outputSchema: schema,
      suspendSchema: schema,
      resumeSchema: schema,
      execute: async ({ inputData, suspend }) => suspend(inputData),
    });
    const workflow = createWorkflow({
      id: 'retained-cancel',
      inputSchema: schema,
      outputSchema: schema,
      options: { validateInputs: false },
    })
      .then(approval)
      .commit();
    new Mastra({ storage: store, workflows: { 'retained-cancel': workflow }, logger: false });
    try {
      const run = await workflow.createRun();
      expect((await run.start({ inputData: { topic: 'aspirin' } })).status).toBe('suspended');
      const workflows = (await store.getStore('workflows'))!;
      const load = () => workflows.loadWorkflowSnapshot({ workflowName: workflow.id, runId: run.runId });
      const snapshot = (await load())!;
      const lineage = {
        expectedExecutionGeneration: snapshot.executionGeneration!,
        expectedLifecycleResumeAttempt: snapshot.lifecycleResumeAttempt ?? 0,
      };
      const canceled = await run.requestCancel({
        requestId: 'first-abort',
        ...lineage,
        retainCancellationRequest: true,
      });
      expect(canceled).toMatchObject({ status: 'canceled', cancelRequest: { requestId: 'first-abort' } });
      const marker = (await load())!.cancelRequest!;
      const replay = await (
        await workflow.createRun({ runId: run.runId })
      ).requestCancel({
        requestId: 'later-abort',
        ...lineage,
        retainCancellationRequest: true,
      });
      expect(replay).toEqual({ status: 'canceled', cancelRequest: marker });

      const opts = {
        cancelRequest: { ...marker, requestId: 'retargeted-write' },
        expectedCancelRequest: { ...marker, requestId: 'mismatched-marker' },
      };
      const refused = workflows.updateWorkflowState({ workflowName: workflow.id, runId: run.runId, opts });
      opts.expectedCancelRequest = marker;
      await expect(refused).resolves.toBeUndefined();
      expect((await load())?.cancelRequest).toEqual(marker);
    } finally {
      await store.close();
    }
  });

  it('holds for the in-memory adapter under concurrent admission', async () => {
    const db = new InMemoryDB();
    await expectAtomicWorkflowResumeStorageContract({
      primary: new WorkflowsInMemory({ db }),
      concurrent: new WorkflowsInMemory({ db }),
      workflowName: 'atomic-resume-in-memory',
    });
  });

  it('holds across LibSQL clients and JSON materialization', async () => {
    const client = createClient({ url: 'file::memory:?cache=shared' });
    const concurrentClient = createClient({ url: 'file::memory:?cache=shared' });
    const primary = new WorkflowsLibSQL({ client, maxRetries: 5, initialBackoffMs: 1 });
    const concurrent = new WorkflowsLibSQL({ client: concurrentClient, maxRetries: 5, initialBackoffMs: 1 });
    try {
      const db = new LibSQLDB({ client, maxRetries: 1, initialBackoffMs: 1 });
      await db.createTable({
        tableName: TABLE_WORKFLOW_SNAPSHOT,
        schema: TABLE_SCHEMAS[TABLE_WORKFLOW_SNAPSHOT],
      });
      await expectAtomicWorkflowResumeStorageContract({
        primary,
        concurrent,
        workflowName: 'atomic-resume-libsql',
      });
    } finally {
      client.close();
      concurrentClient.close();
    }
  });

  it('holds for the standard isolated :memory: store', async () => {
    const store = new LibSQLStore({ id: 'atomic-resume-bare-memory', url: ':memory:' });
    try {
      await store.init();
      const workflows = await store.getStore('workflows');
      await expectAtomicWorkflowResumeStorageContract({
        primary: workflows!,
        workflowName: 'atomic-resume-bare-memory',
      });
    } finally {
      await store.close();
    }
  });

  it('refuses in-memory resume mutations from an execution that lost the run claim (PF-6182)', async () => {
    await expectAtomicWorkflowResumeRunFenceContract({
      primary: new WorkflowsInMemory({ db: new InMemoryDB() }),
      workflowName: 'atomic-resume-fence-in-memory',
    });
  });

  it('refuses libsql resume mutations from an execution that lost the run claim (PF-6182)', async () => {
    const store = new LibSQLStore({ id: 'atomic-resume-fence-bare-memory', url: ':memory:' });
    try {
      await store.init();
      await expectAtomicWorkflowResumeRunFenceContract({
        primary: (await store.getStore('workflows'))!,
        workflowName: 'atomic-resume-fence-libsql',
      });
    } finally {
      await store.close();
    }
  });

  it('inserts an ordinary unfenced first step update', async () => {
    const store = new LibSQLStore({ id: 'first-step-update-bare-memory', url: ':memory:' });
    try {
      await store.init();
      const workflows = (await store.getStore('workflows'))!;
      const snapshot = {
        runId: 'first-step-update-run',
        status: 'running',
        value: {},
        context: {},
        serializedStepGraph: [],
        activePaths: [],
        activeStepsPath: {},
        suspendedPaths: {},
        resumeLabels: {},
        waitingPaths: {},
        timestamp: 1,
      } as WorkflowRunState;

      await expect(
        workflows.persistWorkflowStepUpdate({
          workflowName: 'first-step-update-workflow',
          runId: snapshot.runId,
          snapshot,
        }),
      ).resolves.toEqual({ status: 'persisted' });
      await expect(
        workflows.loadWorkflowSnapshot({
          workflowName: 'first-step-update-workflow',
          runId: snapshot.runId,
        }),
      ).resolves.toMatchObject({ runId: snapshot.runId, status: 'running' });
    } finally {
      await store.close();
    }
  });
});

const WORKFLOW_ID = 'libsql-resume-restart-intent-wf';

type Behaviour = 'block' | 'run';

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>(settle => {
    resolve = settle;
  });
  return { promise, resolve };
}

function createCheckpointWorkflow(behaviour: { apply: Behaviour; publish: Behaviour }) {
  const seen = { review: [] as unknown[], apply: [] as string[], publish: [] as string[] };
  const started = { apply: deferred(), publish: deferred() };
  const never = new Promise<never>(() => {});
  const review = createStep({
    id: 'review',
    inputSchema: z.object({ draft: z.string() }),
    outputSchema: z.object({ draft: z.string() }),
    suspendSchema: z.object({ draft: z.string() }),
    resumeSchema: z.object({ decision: z.enum(['approve', 'revise']), draft: z.string().optional() }),
    execute: async ({ inputData, resumeData, suspend }) => {
      seen.review.push(resumeData);
      if (!resumeData) return suspend({ draft: inputData.draft });
      return { draft: resumeData.decision === 'revise' ? resumeData.draft! : inputData.draft };
    },
  });
  const apply = createStep({
    id: 'apply',
    inputSchema: z.object({ draft: z.string() }),
    outputSchema: z.object({ applied: z.string() }),
    execute: async ({ inputData }) => {
      seen.apply.push(inputData.draft);
      started.apply.resolve();
      if (behaviour.apply === 'block') await never;
      return { applied: inputData.draft };
    },
  });
  const publish = createStep({
    id: 'publish',
    inputSchema: z.object({ applied: z.string() }),
    outputSchema: z.object({ published: z.string() }),
    execute: async ({ inputData }) => {
      seen.publish.push(inputData.applied);
      started.publish.resolve();
      if (behaviour.publish === 'block') await never;
      return { published: inputData.applied };
    },
  });
  const workflow = createWorkflow({
    id: WORKFLOW_ID,
    inputSchema: z.object({ draft: z.string() }),
    outputSchema: z.object({ published: z.string() }),
  })
    .then(review)
    .then(apply)
    .then(publish)
    .commit();
  return { workflow, seen, started };
}

/**
 * PF-7806 on libSQL: each host is its own Mastra over one in-memory libSQL store.
 * Host A accepts a resume and dies in the next step; host B's restart continues
 * the accepted answer; host C's boot sweep does not apply it again.
 */
describe('restart() continues an accepted resume on libSQL (PF-7806)', () => {
  let storage: LibSQLStore | undefined;

  afterEach(async () => {
    await storage?.close();
    storage = undefined;
  });

  function createHost(store: LibSQLStore, behaviour: { apply: Behaviour; publish: Behaviour }) {
    const probe = createCheckpointWorkflow(behaviour);
    const mastra = new Mastra({ storage: store, workflows: { [WORKFLOW_ID]: probe.workflow }, logger: false });
    return { mastra, ...probe };
  }

  async function loadSnapshot(store: LibSQLStore, runId: string) {
    const workflows = await store.getStore('workflows');
    return workflows!.loadWorkflowSnapshot({ workflowName: WORKFLOW_ID, runId });
  }

  /** A dead host renews nothing, so its run-ownership lease lapses. */
  async function crash(store: LibSQLStore, runId: string) {
    const workflows = (await store.getStore('workflows'))!;
    const owner = await workflows.getRunOwnership({ runId });
    if (owner?.live) {
      await workflows.releaseRunOwnership({ runId, generation: owner.generation, ownerId: owner.ownerId });
    }
  }

  it.each([
    ['approve', { decision: 'approve' as const }, 'v1'],
    ['revise', { decision: 'revise' as const, draft: 'v2-edited' }, 'v2-edited'],
  ])('continues the accepted %s answer once across hosts', async (_, answer, expectedDraft) => {
    storage = new LibSQLStore({ id: 'libsql-resume-intent', url: ':memory:' });
    const a = createHost(storage, { apply: 'block', publish: 'run' });
    const runA = await a.workflow.createRun();
    expect((await runA.start({ inputData: { draft: 'v1' } })).status).toBe('suspended');
    void runA.resume({ step: 'review', resumeData: answer });
    await a.started.apply.promise;

    const stranded = await loadSnapshot(storage, runA.runId);
    expect(stranded?.status).toBe('running');
    expect(stranded?.resumeIntent?.resumePayload).toEqual(answer);
    expect(stranded?.context.review).toMatchObject({ status: 'suspended', suspendPayload: { draft: 'v1' } });
    await crash(storage, runA.runId);

    const b = createHost(storage, { apply: 'run', publish: 'block' });
    // Host B dies in publish; a restart that lost the answer settles early instead.
    const restartB = (await b.workflow.createRun({ runId: runA.runId })).restart();
    await Promise.race([b.started.publish.promise, restartB]);
    expect(b.seen.review).toEqual([answer]);
    expect(b.seen.apply).toEqual([expectedDraft]);
    await crash(storage, runA.runId);

    // Host C recovers through the boot-time sweep.
    const c = createHost(storage, { apply: 'run', publish: 'run' });
    await c.mastra.restartAllActiveWorkflowRuns();
    expect(c.seen.review).toEqual([]);
    expect(c.seen.apply).toEqual([]);
    expect(c.seen.publish).toEqual([expectedDraft]);
    const settled = await loadSnapshot(storage, runA.runId);
    expect(settled?.status).toBe('success');
    expect(settled?.result).toEqual({ published: expectedDraft });
  });
});
