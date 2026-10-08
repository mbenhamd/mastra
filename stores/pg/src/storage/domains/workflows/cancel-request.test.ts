import { randomUUID } from 'node:crypto';
import { Mastra } from '@mastra/core/mastra';
import { createStep, createWorkflow } from '@mastra/core/workflows';
import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { z } from 'zod/v4';
import { PostgresStore } from '../..';
import { connectionString } from '../../test-utils';

const WORKFLOW_ID = 'pg-cancel-request-wf';
const topicSchema = z.object({ topic: z.string() });

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>(settle => {
    resolve = settle;
  });
  return { promise, resolve };
}

function createProbe() {
  return {
    stageOneStarted: deferred(),
    releaseStageOne: deferred(),
    stageOneExecutions: 0,
    stageTwoExecutions: 0,
  };
}
type Probe = ReturnType<typeof createProbe>;

function createStagedWorkflow(probe: Probe) {
  const stageOne = createStep({
    id: 'stage-one',
    inputSchema: topicSchema,
    outputSchema: topicSchema,
    execute: async ({ inputData }) => {
      probe.stageOneExecutions++;
      probe.stageOneStarted.resolve();
      await probe.releaseStageOne.promise;
      return inputData;
    },
  });
  const stageTwo = createStep({
    id: 'stage-two',
    inputSchema: topicSchema,
    outputSchema: topicSchema,
    execute: async ({ inputData }) => {
      probe.stageTwoExecutions++;
      return inputData;
    },
  });
  return createWorkflow({
    id: WORKFLOW_ID,
    inputSchema: topicSchema,
    outputSchema: topicSchema,
    steps: [stageOne, stageTwo],
    options: { validateInputs: false },
  })
    .then(stageOne)
    .then(stageTwo)
    .commit();
}

/**
 * The run lifecycle across two processes, each with its own PostgresStore over
 * one database, so every cross-handle read goes through PostgreSQL.
 */
describe('Run.requestCancel() on PostgreSQL', () => {
  const schemaName = `cancel_request_${randomUUID().replaceAll('-', '')}`;
  const stores: PostgresStore[] = [];

  function createProcess(probe: Probe) {
    const storage = new PostgresStore({ id: `pg-cancel-request-${stores.length}`, connectionString, schemaName });
    stores.push(storage);
    const workflow = createStagedWorkflow(probe);
    const mastra = new Mastra({ storage, workflows: { [WORKFLOW_ID]: workflow }, logger: false });
    return { storage, mastra, workflow };
  }

  async function loadSnapshot(storage: PostgresStore, runId: string) {
    const workflows = await storage.getStore('workflows');
    return workflows!.loadWorkflowSnapshot({ workflowName: WORKFLOW_ID, runId });
  }

  async function lineageOf(storage: PostgresStore, runId: string) {
    const snapshot = await loadSnapshot(storage, runId);
    return {
      expectedExecutionGeneration: snapshot!.executionGeneration!,
      expectedLifecycleResumeAttempt: snapshot!.lifecycleResumeAttempt ?? 0,
    };
  }

  beforeAll(async () => {
    const setup = new PostgresStore({ id: 'pg-cancel-request-setup', connectionString, schemaName });
    stores.push(setup);
    await setup.init();
  });

  afterAll(async () => {
    await Promise.all(stores.map(store => store.close()));
    const pool = new Pool({ connectionString });
    await pool.query(`DROP SCHEMA IF EXISTS "${schemaName}" CASCADE`).finally(() => pool.end());
  });

  it('stops a run owned by another process at its next step boundary', async () => {
    const probe = createProbe();
    const owner = createProcess(probe);
    const controller = createProcess(probe);

    const ownerRun = await owner.workflow.createRun();
    const execution = ownerRun.start({ inputData: { topic: 'aspirin' } });
    await probe.stageOneStarted.promise;

    // The public run state names the execution to target.
    const observed = await controller.workflow.getWorkflowRunById(ownerRun.runId, { fields: [] });
    const remoteRun = await controller.workflow.createRun({ runId: ownerRun.runId });
    const lineage = {
      expectedExecutionGeneration: observed!.executionGeneration!,
      expectedLifecycleResumeAttempt: observed!.lifecycleResumeAttempt!,
    };
    const ordinaryRun = await createProcess(probe).workflow.createRun({ runId: ownerRun.runId });
    const [outcome, concurrent] = await Promise.all([
      remoteRun.requestCancel({ requestId: 'abort-op-1', ...lineage, retainCancellationRequest: true }),
      ordinaryRun.requestCancel({ requestId: 'competing-ordinary-abort', ...lineage }),
    ]);
    expect(['requested', 'already_requested']).toContain(outcome.status);
    expect(['requested', 'already_requested']).toContain(concurrent.status);
    if (!('cancelRequest' in outcome) || !('cancelRequest' in concurrent)) throw new Error('Expected stored requests');
    expect(outcome.cancelRequest).toEqual(concurrent.cancelRequest);
    expect(await owner.workflow.getWorkflowRunById(ownerRun.runId, { fields: [] })).toMatchObject({
      status: 'running',
      cancelRequest: outcome.cancelRequest,
    });

    probe.releaseStageOne.resolve();
    const result = await execution;

    expect(result.status).toBe('canceled');
    expect(probe.stageTwoExecutions).toBe(0);
    const settled = await loadSnapshot(controller.storage, ownerRun.runId);
    expect(settled?.status).toBe('canceled');
    expect(settled?.cancelRequest).toEqual(outcome.cancelRequest);
  });

  it('rejects a request whose lineage a restart has replaced', async () => {
    const probe = createProbe();
    const deadOwner = createProcess(probe);
    const survivor = createProcess(probe);

    const ownerRun = await deadOwner.workflow.createRun();
    const abandoned = ownerRun.start({ inputData: { topic: 'aspirin' } });
    await probe.stageOneStarted.promise;
    const strandedLineage = await lineageOf(survivor.storage, ownerRun.runId);

    // The survivor adopts the stranded run under a new generation.
    const recovery = (await survivor.workflow.createRun({ runId: ownerRun.runId })).restart();
    await expect.poll(() => probe.stageOneExecutions).toBe(2);

    const lateRun = await survivor.workflow.createRun({ runId: ownerRun.runId });
    const outcome = await lateRun.requestCancel({
      requestId: 'stale-abort',
      ...strandedLineage,
      retainCancellationRequest: true,
    });

    expect(outcome).toMatchObject({ status: 'lineage_moved' });
    expect((await loadSnapshot(survivor.storage, ownerRun.runId))?.cancelRequest).toBeUndefined();
    probe.releaseStageOne.resolve();
    expect((await recovery).status).toBe('success');
    expect(probe.stageTwoExecutions).toBe(1);
    // The superseded owner stands down without settling the successor.
    expect((await abandoned).status).toBe('canceled');
    expect((await loadSnapshot(survivor.storage, ownerRun.runId))?.status).toBe('success');
  });

  it('cancels a stranded running lineage at once when the caller vouches that no engine executes it', async () => {
    const probe = createProbe();
    const deadOwner = createProcess(probe);
    const survivor = createProcess(probe);

    const ownerRun = await deadOwner.workflow.createRun();
    const abandoned = ownerRun.start({ inputData: { topic: 'aspirin' } });
    await probe.stageOneStarted.promise;
    const strandedLineage = await lineageOf(survivor.storage, ownerRun.runId);
    const remoteRun = await survivor.workflow.createRun({ runId: ownerRun.runId });

    expect(
      await remoteRun.requestCancel({
        requestId: 'abort-op-1',
        ...strandedLineage,
        noActiveExecution: true,
        retainCancellationRequest: true,
      }),
    ).toMatchObject({ status: 'canceled', cancelRequest: { requestId: 'abort-op-1' } });
    const settled = await loadSnapshot(survivor.storage, ownerRun.runId);
    expect(settled?.status).toBe('canceled');
    expect(settled?.executionGeneration).toBe(strandedLineage.expectedExecutionGeneration);
    const marker = settled!.cancelRequest!;
    const replay = await (
      await createProcess(probe).workflow.createRun({ runId: ownerRun.runId })
    ).requestCancel({
      requestId: 'later-replay',
      ...strandedLineage,
      retainCancellationRequest: true,
    });
    expect(replay).toEqual({ status: 'canceled', cancelRequest: marker });
    expect((await loadSnapshot(survivor.storage, ownerRun.runId))?.cancelRequest).toEqual(marker);

    probe.releaseStageOne.resolve();
    expect((await abandoned).status).toBe('canceled');
    expect(probe.stageTwoExecutions).toBe(0);
  });

  it('makes a recovery sweep commit canceled instead of re-executing the stranded lineage', async () => {
    const probe = createProbe();
    const deadOwner = createProcess(probe);
    const survivor = createProcess(probe);

    const ownerRun = await deadOwner.workflow.createRun();
    const abandoned = ownerRun.start({ inputData: { topic: 'aspirin' } });
    await probe.stageOneStarted.promise;
    const remoteRun = await survivor.workflow.createRun({ runId: ownerRun.runId });
    const outcome = await remoteRun.requestCancel({
      requestId: 'abort-op-1',
      ...(await lineageOf(survivor.storage, ownerRun.runId)),
    });
    expect(outcome.status).toBe('requested');

    await survivor.workflow.restartAllActiveWorkflowRuns();

    expect(probe.stageOneExecutions).toBe(1);
    expect((await loadSnapshot(survivor.storage, ownerRun.runId))?.status).toBe('canceled');

    probe.releaseStageOne.resolve();
    expect((await abandoned).status).toBe('canceled');
    expect(probe.stageTwoExecutions).toBe(0);
  });
});
