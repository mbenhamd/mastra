import { randomUUID } from 'node:crypto';
import { createEmptyWorkflowSnapshot, isEmptyWorkflowSnapshot } from '@mastra/core/storage';
import type { WorkflowRunState } from '@mastra/core/workflows';
import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { PostgresStore } from '../../index';
import type { WorkflowsPG } from '../../index';

const connectionString = process.env.DB_URL || 'postgresql://postgres:postgres@localhost:5434/mastra';

/**
 * PF-6186: `deleteWorkflowRunClosure` keeps a completed, unreleased handoff
 * under the caller's `retainHandoffFence` (with a content-free snapshot), so a
 * stale delete can never leave a run that already carries an absence fence
 * unfenced.
 */
describe('WorkflowsPG run closure delete: retained absence fence (PF-6186)', () => {
  const FENCE = 'product-erasure-absent@1';
  const schemaName = `closure_fence_${randomUUID().replaceAll('-', '')}`;
  let store: PostgresStore;
  let workflows: WorkflowsPG;
  let resource: string;

  beforeAll(async () => {
    store = new PostgresStore({ id: `pg-closure-fence-${randomUUID()}`, connectionString, schemaName });
    await store.init();
    workflows = (await store.getStore('workflows'))!;
    resource = `workspace-${randomUUID()}:user-1`;
  }, 60000);

  afterAll(async () => {
    await store?.close();
    const pool = new Pool({ connectionString });
    await pool.query(`DROP SCHEMA IF EXISTS "${schemaName}" CASCADE`).finally(() => pool.end());
  });

  const newRun = () => ({ workflowName: `research-${randomUUID()}`, runId: randomUUID() });
  type Run = ReturnType<typeof newRun>;

  /** The stored handoff row's columns, read directly. */
  const handoffRow = (run: Run) =>
    store.db.oneOrNone<{
      status: string;
      mutation_fence: string;
      claim_generation: string;
      resource_id: string | null;
      released_at: string | null;
      snapshot: WorkflowRunState | string;
    }>(
      `SELECT status, mutation_fence, claim_generation::text, resource_id, released_at, snapshot
       FROM "${schemaName}".mastra_workflow_snapshot_handoffs WHERE workflow_name = $1 AND run_id = $2`,
      [run.workflowName, run.runId],
    );
  const snapshotOf = (row: { snapshot: WorkflowRunState | string }) =>
    typeof row.snapshot === 'string' ? JSON.parse(row.snapshot) : row.snapshot;

  async function fenceAbsence(
    run: Run,
    { fence = FENCE, owner = resource, snapshot = createEmptyWorkflowSnapshot(run.runId), complete = true } = {},
  ) {
    await expect(
      workflows.claimWorkflowSnapshotHandoff({
        ...run,
        expectedCanonical: { kind: 'absent' },
        resourceId: owner,
        snapshot,
        mutationFence: fence,
      }),
    ).resolves.toMatchObject({ status: 'created' });
    if (!complete) return;
    await expect(
      workflows.completeWorkflowSnapshotHandoff({
        ...run,
        expectedResourceId: owner,
        expectedSnapshot: snapshot,
        resourceId: owner,
        snapshot,
        mutationFence: fence,
      }),
    ).resolves.toMatchObject({ status: 'completed' });
  }

  it('advertises fence retention', () => {
    expect(workflows.getWorkflowSnapshotHandoffCapabilities()).toMatchObject({ closureDeleteRetainFenceVersion: 1 });
  });

  it("keeps a newer worker's absence fence when a stale delete lands after it and its response is lost", async () => {
    const run = newRun();
    await workflows.persistWorkflowSnapshot({
      ...run,
      resourceId: resource,
      snapshot: { ...createEmptyWorkflowSnapshot(run.runId), status: 'success', result: { report: 'private' } },
    });
    // Worker B erases the closure and fences the run's absence.
    await expect(
      workflows.deleteWorkflowRunClosure({ ...run, expectedResourceId: resource, retainHandoffFence: FENCE }),
    ).resolves.toMatchObject({ status: 'deleted' });
    await fenceAbsence(run);
    // Worker A's delayed delete lands afterwards; its caller never sees the answer.
    await workflows.deleteWorkflowRunClosure({ ...run, expectedResourceId: resource, retainHandoffFence: FENCE });

    await expect(handoffRow(run)).resolves.toMatchObject({ status: 'completed', mutation_fence: FENCE });
    await expect(
      workflows.persistWorkflowSnapshot({
        ...run,
        resourceId: resource,
        snapshot: { ...createEmptyWorkflowSnapshot(run.runId), status: 'running' },
      }),
    ).rejects.toThrow();
    await expect(workflows.loadWorkflowSnapshot(run)).resolves.toBeNull();
  });

  it('empties a retained fence snapshot in place and keeps its fence, status and claim generation', async () => {
    const run = newRun();
    await fenceAbsence(run, {
      snapshot: { ...createEmptyWorkflowSnapshot(run.runId), context: { private: { output: 'content' } } } as any,
    });
    const before = (await handoffRow(run))!;

    await expect(
      workflows.deleteWorkflowRunClosure({ ...run, expectedResourceId: resource, retainHandoffFence: FENCE }),
    ).resolves.toEqual({
      status: 'deleted',
      removed: { snapshot: false, handoff: false, terminalRows: 0 },
      retainedFence: true,
    });
    const after = (await handoffRow(run))!;
    expect(after).toMatchObject({
      status: 'completed',
      mutation_fence: before.mutation_fence,
      claim_generation: before.claim_generation,
      resource_id: resource,
      released_at: null,
    });
    expect(isEmptyWorkflowSnapshot(snapshotOf(after), run.runId)).toBe(true);
    expect(JSON.stringify(snapshotOf(after))).not.toContain('content');
    await expect(
      workflows.deleteWorkflowRunClosure({ ...run, expectedResourceId: resource, retainHandoffFence: FENCE }),
    ).resolves.toEqual({ status: 'missing', retainedFence: true });
    await expect(handoffRow(run)).resolves.toMatchObject({ status: 'completed', mutation_fence: FENCE });
  });

  it('replaces a fence snapshot whose timestamp carries content', async () => {
    const run = newRun();
    await fenceAbsence(run, {
      snapshot: { ...createEmptyWorkflowSnapshot(run.runId), timestamp: { userText: 'private content' } } as any,
    });
    await expect(
      workflows.deleteWorkflowRunClosure({ ...run, expectedResourceId: resource, retainHandoffFence: FENCE }),
    ).resolves.toMatchObject({ status: 'deleted', retainedFence: true });
    const after = (await handoffRow(run))!;
    expect(typeof snapshotOf(after).timestamp).toBe('number');
    expect(JSON.stringify(snapshotOf(after))).not.toContain('private content');
    expect(after).toMatchObject({ status: 'completed', mutation_fence: FENCE });
  });

  it("clears the erased lifetime's terminal latch even when only the empty fence remains", async () => {
    const run = newRun();
    const revisionRow = () =>
      store.db.one<{ generation: string; terminal_status: string | null }>(
        `SELECT generation::text, terminal_status
         FROM "${schemaName}".mastra_workflow_parent_revisions WHERE workflow_name = $1 AND run_id = $2`,
        [run.workflowName, run.runId],
      );
    await workflows.persistWorkflowSnapshot({
      ...run,
      resourceId: resource,
      snapshot: { ...createEmptyWorkflowSnapshot(run.runId), status: 'success', result: { report: 'private' } },
    });
    await workflows.deleteWorkflowRunById(run);
    await expect(revisionRow()).resolves.toMatchObject({ terminal_status: 'success' });
    await fenceAbsence(run);
    const before = await revisionRow();

    await expect(
      workflows.deleteWorkflowRunClosure({ ...run, expectedResourceId: resource, retainHandoffFence: FENCE }),
    ).resolves.toMatchObject({ retainedFence: true });
    const revision = await revisionRow();
    expect(revision.terminal_status).toBeNull();
    expect(Number(revision.generation)).toBeGreaterThan(Number(before.generation));
    await expect(handoffRow(run)).resolves.toMatchObject({ status: 'completed', mutation_fence: FENCE });
    await expect(
      workflows.deleteWorkflowRunClosure({ ...run, expectedResourceId: resource, retainHandoffFence: FENCE }),
    ).resolves.toEqual({ status: 'missing', retainedFence: true });
    await expect(revisionRow()).resolves.toMatchObject({ generation: revision.generation });
  });

  it('still deletes a handoff under another fence, or when no fence is named', async () => {
    for (const retainHandoffFence of ['another-fence@1', undefined]) {
      const run = newRun();
      await fenceAbsence(run);
      await expect(
        workflows.deleteWorkflowRunClosure({
          ...run,
          expectedResourceId: resource,
          ...(retainHandoffFence === undefined ? {} : { retainHandoffFence }),
        }),
      ).resolves.toEqual({ status: 'deleted', removed: { snapshot: false, handoff: true, terminalRows: 0 } });
      await expect(handoffRow(run)).resolves.toBeNull();
    }
  });

  it('answers resource_mismatch for a matching fence recorded under another resource, without changes', async () => {
    const run = newRun();
    await fenceAbsence(run, { owner: `workspace-${randomUUID()}:user-2` });
    const before = await handoffRow(run);
    await expect(
      workflows.deleteWorkflowRunClosure({ ...run, expectedResourceId: resource, retainHandoffFence: FENCE }),
    ).resolves.toEqual({ status: 'resource_mismatch' });
    await expect(handoffRow(run)).resolves.toEqual(before);
  });

  it('answers fenced for a pending fence, and deletes a released one', async () => {
    const pending = newRun();
    await fenceAbsence(pending, { complete: false });
    await expect(
      workflows.deleteWorkflowRunClosure({ ...pending, expectedResourceId: resource, retainHandoffFence: FENCE }),
    ).resolves.toEqual({ status: 'fenced' });
    await expect(handoffRow(pending)).resolves.toMatchObject({ status: 'pending', mutation_fence: FENCE });

    const released = newRun();
    await fenceAbsence(released);
    await expect(
      workflows.releaseWorkflowSnapshotHandoff({ ...released, expectedClaimGeneration: 1 }),
    ).resolves.toMatchObject({ status: 'released' });
    await expect(
      workflows.deleteWorkflowRunClosure({ ...released, expectedResourceId: resource, retainHandoffFence: FENCE }),
    ).resolves.toMatchObject({ status: 'deleted', removed: { handoff: true } });
    await expect(handoffRow(released)).resolves.toBeNull();
  });
});
