import { Mastra } from '@mastra/core/mastra';
import { createStep, createWorkflow } from '@mastra/core/workflows';
import { afterEach, describe, expect, it } from 'vitest';
import { z } from 'zod/v4';
import { LibSQLStore } from '../..';

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
