import { randomUUID } from 'node:crypto';
import { Mastra } from '@mastra/core/mastra';
import { createStep, createWorkflow } from '@mastra/core/workflows';
import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { z } from 'zod/v4';
import { PostgresStore } from '../..';
import { connectionString } from '../../test-utils';

const WORKFLOW_ID = 'pg-resume-restart-intent-wf';

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
 * PF-7806 on PostgreSQL: each host is its own PostgresStore and Mastra over one
 * schema. Host A accepts a resume and dies in the next step; host B's restart
 * continues the accepted answer; host C's boot sweep does not apply it again.
 */
describe('restart() continues an accepted resume on PostgreSQL (PF-7806)', () => {
  const schemaName = `resume_intent_${randomUUID().replaceAll('-', '')}`;
  const stores: PostgresStore[] = [];

  function createHost(behaviour: { apply: Behaviour; publish: Behaviour }) {
    const storage = new PostgresStore({ id: `pg-resume-intent-${stores.length}`, connectionString, schemaName });
    stores.push(storage);
    const probe = createCheckpointWorkflow(behaviour);
    const mastra = new Mastra({ storage, workflows: { [WORKFLOW_ID]: probe.workflow }, logger: false });
    return { storage, mastra, ...probe };
  }

  async function loadSnapshot(storage: PostgresStore, runId: string) {
    const workflows = await storage.getStore('workflows');
    return workflows!.loadWorkflowSnapshot({ workflowName: WORKFLOW_ID, runId });
  }

  /** A dead host renews nothing, so its run-ownership lease lapses. */
  async function crash(storage: PostgresStore, runId: string) {
    const workflows = (await storage.getStore('workflows'))!;
    const owner = await workflows.getRunOwnership({ runId });
    if (owner?.live) {
      await workflows.releaseRunOwnership({ runId, generation: owner.generation, ownerId: owner.ownerId });
    }
  }

  beforeAll(async () => {
    const setup = new PostgresStore({ id: 'pg-resume-intent-setup', connectionString, schemaName });
    stores.push(setup);
    await setup.init();
  });

  afterAll(async () => {
    await Promise.all(stores.map(store => store.close()));
    const pool = new Pool({ connectionString });
    await pool.query(`DROP SCHEMA IF EXISTS "${schemaName}" CASCADE`).finally(() => pool.end());
  });

  it.each([
    ['approve', { decision: 'approve' as const }, 'v1'],
    ['revise', { decision: 'revise' as const, draft: 'v2-edited' }, 'v2-edited'],
  ])('continues the accepted %s answer once across hosts', async (_, answer, expectedDraft) => {
    const a = createHost({ apply: 'block', publish: 'run' });
    const runA = await a.workflow.createRun();
    expect((await runA.start({ inputData: { draft: 'v1' } })).status).toBe('suspended');
    void runA.resume({ step: 'review', resumeData: answer });
    await a.started.apply.promise;

    const stranded = await loadSnapshot(a.storage, runA.runId);
    expect(stranded?.status).toBe('running');
    expect(stranded?.context.review).toMatchObject({ status: 'suspended', suspendPayload: { draft: 'v1' } });
    await crash(a.storage, runA.runId);

    const b = createHost({ apply: 'run', publish: 'block' });
    // Host B dies in publish; a restart that lost the answer settles early instead.
    const restartB = (await b.workflow.createRun({ runId: runA.runId })).restart();
    await Promise.race([b.started.publish.promise, restartB]);
    expect(b.seen.review).toEqual([answer]);
    expect(b.seen.apply).toEqual([expectedDraft]);
    await crash(b.storage, runA.runId);

    // Host C recovers through the boot-time sweep.
    const c = createHost({ apply: 'run', publish: 'run' });
    await c.mastra.restartAllActiveWorkflowRuns();
    expect(c.seen.review).toEqual([]);
    expect(c.seen.apply).toEqual([]);
    expect(c.seen.publish).toEqual([expectedDraft]);
    const settled = await loadSnapshot(c.storage, runA.runId);
    expect(settled?.status).toBe('success');
    expect(settled?.result).toEqual({ published: expectedDraft });
  });

  it('leaves a suspended run that was never resumed to resume()', async () => {
    const a = createHost({ apply: 'block', publish: 'run' });
    const runA = await a.workflow.createRun();
    expect((await runA.start({ inputData: { draft: 'v1' } })).status).toBe('suspended');
    // No resume claim: restart() refuses the suspended run as before and never answers for it.
    const b = createHost({ apply: 'run', publish: 'run' });
    await expect((await b.workflow.createRun({ runId: runA.runId })).restart()).rejects.toThrow();
    expect(b.seen.review).toEqual([]);
    expect((await loadSnapshot(b.storage, runA.runId))?.status).toBe('suspended');
  });
});
