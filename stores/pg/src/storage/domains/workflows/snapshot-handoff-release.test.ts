import { randomUUID } from 'node:crypto';
import { WorkflowSnapshotHandoffFenceError } from '@mastra/core/storage';
import type { WorkflowRunState } from '@mastra/core/workflows';
import { Client, Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { PostgresStore, WorkflowsPG } from '../../index';

const connectionString = process.env.DB_URL || 'postgresql://postgres:postgres@localhost:5434/mastra';

const snapshot = (runId: string, status: WorkflowRunState['status'], value: unknown = {}): WorkflowRunState =>
  ({
    runId,
    status,
    value,
    context: {},
    activePaths: [],
    activeStepsPath: {},
    suspendedPaths: {},
    resumeLabels: {},
    serializedStepGraph: [],
    waitingPaths: {},
    timestamp: Date.now(),
  }) as WorkflowRunState;

type Owner = { workflowName: string; runId: string; resourceId: string; mutationFence: string };

describe('workflow snapshot handoff release in PostgreSQL', () => {
  let store: PostgresStore;
  let workflows: WorkflowsPG;
  let adminPool: Pool;
  const storesToClose: PostgresStore[] = [];
  const schemasToDrop: string[] = [];

  beforeAll(async () => {
    adminPool = new Pool({ connectionString });
    store = new PostgresStore({ id: `workflow-handoff-release-${randomUUID()}`, connectionString });
    await store.init();
    workflows = (await store.getStore('workflows'))!;
  }, 60000);

  afterAll(async () => {
    await store?.close();
    for (const extra of storesToClose) await extra.close();
    for (const schema of schemasToDrop) await adminPool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
    await adminPool?.end();
  }, 60000);

  async function claimedRun(prefix: string) {
    const workflowName = `${prefix}-${randomUUID()}`;
    const runId = randomUUID();
    const canonical = snapshot(runId, 'success', { native: true });
    await workflows.persistWorkflowSnapshot({ workflowName, runId, snapshot: canonical });
    const pending = snapshot(runId, 'waiting', { phase: 'claimed' });
    const owner: Owner = { workflowName, runId, resourceId: 'resource-1', mutationFence: `owner-${randomUUID()}` };
    const claim = (fence: string, claimed: WorkflowRunState) =>
      workflows.claimWorkflowSnapshotHandoff({
        ...owner,
        mutationFence: fence,
        expectedCanonical: { kind: 'present', snapshot: canonical },
        snapshot: claimed,
      });
    await expect(claim(owner.mutationFence, pending)).resolves.toMatchObject({
      status: 'created',
      record: { claimGeneration: 1 },
    });
    const complete = (fence = owner.mutationFence, expectedSnapshot = pending) =>
      workflows.completeWorkflowSnapshotHandoff({
        ...owner,
        mutationFence: fence,
        expectedResourceId: owner.resourceId,
        expectedSnapshot,
        snapshot: snapshot(runId, 'success', { phase: 'delivered' }),
      });
    return { owner, target: { workflowName, runId }, pending, claim, complete };
  }

  const handoffRows = (workflowName: string, runId: string) =>
    store.db.any<{ status: string; claim_generation: string; released: boolean }>(
      `SELECT status, claim_generation, released_at IS NOT NULL AS released
       FROM mastra_workflow_snapshot_handoffs WHERE workflow_name = $1 AND run_id = $2`,
      [workflowName, runId],
    );

  const parentRevisionWaiters = async (): Promise<number> => {
    const { rows } = await adminPool.query<{ waiters: number }>(
      `SELECT count(*)::int AS waiters FROM pg_stat_activity
       WHERE datname = current_database() AND wait_event_type = 'Lock'
         AND query ILIKE '%mastra_workflow_parent_revisions%'`,
    );
    return rows[0]!.waiters;
  };

  /**
   * Runs `first` and `second` against one run in a forced order: a blocking
   * transaction holds the run's parent revision row, `first` is queued behind
   * it before `second` starts, and PostgreSQL grants the row lock to waiters
   * in queue order once the blocker commits.
   */
  async function inForcedOrder<A, B>(
    target: { workflowName: string; runId: string },
    first: () => Promise<A>,
    second: () => Promise<B>,
  ): Promise<[A, B]> {
    const blocker = new Client({ connectionString });
    await blocker.connect();
    try {
      await blocker.query('BEGIN');
      await blocker.query(
        `SELECT generation FROM mastra_workflow_parent_revisions
         WHERE workflow_name = $1 AND run_id = $2 FOR UPDATE`,
        [target.workflowName, target.runId],
      );
      const waitFor = async (count: number) => {
        const deadline = Date.now() + 10_000;
        while ((await parentRevisionWaiters()) < count) {
          if (Date.now() > deadline) throw new Error(`Timed out waiting for ${count} lock waiters`);
          await new Promise(resolve => setTimeout(resolve, 20));
        }
      };
      const firstResult = first();
      await waitFor(1);
      const secondResult = second();
      await waitFor(2);
      await blocker.query('COMMIT');
      return await Promise.all([firstResult, secondResult]);
    } finally {
      await blocker.end();
    }
  }

  it('advertises the release protocol version', () => {
    expect(workflows.getWorkflowSnapshotHandoffCapabilities()).toMatchObject({ handoffVersion: 1, releaseVersion: 1 });
  });

  it('releases a completed handoff without the fence, lifting every fence but keeping its outcome', async () => {
    const { owner, target, claim, complete } = await claimedRun('release-completed');
    await expect(complete()).resolves.toMatchObject({ status: 'completed' });
    await expect(workflows.deleteWorkflowRunById(target)).rejects.toBeInstanceOf(WorkflowSnapshotHandoffFenceError);

    const released = await workflows.releaseWorkflowSnapshotHandoff({ ...target, expectedClaimGeneration: 1 });
    expect(released).toMatchObject({
      status: 'released',
      runDeleted: false,
      record: { status: 'completed', claimGeneration: 1, snapshot: { value: { phase: 'delivered' } } },
    });
    expect(released.status === 'released' && typeof released.record.releasedAt).toBe('number');
    await expect(handoffRows(target.workflowName, target.runId)).resolves.toEqual([
      { status: 'completed', claim_generation: '1', released: true },
    ]);
    await expect(workflows.listWorkflowSnapshotHandoffs({ workflowName: target.workflowName })).resolves.toEqual({
      records: [],
      hasMore: false,
    });

    // A retry of the same delivery still finds its settled outcome; the run stays unfenced.
    await expect(
      claim(owner.mutationFence, snapshot(target.runId, 'waiting', { phase: 'retried' })),
    ).resolves.toMatchObject({ status: 'completed', record: { claimGeneration: 1 } });

    await workflows.persistWorkflowSnapshot({
      ...target,
      snapshot: snapshot(target.runId, 'success', { phase: 'released-write' }),
    });
    await expect(workflows.loadWorkflowSnapshot(target)).resolves.toMatchObject({
      value: { phase: 'released-write' },
    });
    await workflows.deleteWorkflowRunById(target);
    await expect(workflows.loadWorkflowSnapshot(target)).resolves.toBeNull();
  });

  it('refuses to release an in-flight handoff without the matching fence', async () => {
    const { owner, target, pending, complete } = await claimedRun('release-pending');

    await expect(
      workflows.releaseWorkflowSnapshotHandoff({ ...target, expectedClaimGeneration: 1 }),
    ).resolves.toMatchObject({ status: 'conflict', record: { status: 'pending', mutationFence: owner.mutationFence } });
    await expect(
      workflows.releaseWorkflowSnapshotHandoff({ ...target, expectedClaimGeneration: 1, mutationFence: 'other-fence' }),
    ).resolves.toMatchObject({ status: 'conflict', record: { status: 'pending' } });
    await expect(handoffRows(target.workflowName, target.runId)).resolves.toEqual([
      { status: 'pending', claim_generation: '1', released: false },
    ]);
    await expect(workflows.deleteWorkflowRunById(target)).rejects.toBeInstanceOf(WorkflowSnapshotHandoffFenceError);

    await expect(
      workflows.releaseWorkflowSnapshotHandoff({
        ...target,
        expectedClaimGeneration: 1,
        mutationFence: owner.mutationFence,
      }),
    ).resolves.toMatchObject({ status: 'released', record: { status: 'pending' } });
    // A released pending claim takes no further steps.
    await expect(complete()).resolves.toMatchObject({ status: 'conflict' });
    await expect(
      workflows.transitionWorkflowSnapshotHandoff({
        ...owner,
        expectedResourceId: owner.resourceId,
        expectedSnapshot: pending,
        snapshot: snapshot(target.runId, 'waiting', { phase: 'late' }),
      }),
    ).resolves.toMatchObject({ status: 'conflict' });
    await workflows.deleteWorkflowRunById(target);
    await expect(workflows.loadWorkflowSnapshot(target)).resolves.toBeNull();
  });

  it('refuses a completed release whose supplied fence belongs to another owner', async () => {
    const { target, complete } = await claimedRun('release-foreign');
    await complete();
    await expect(
      workflows.releaseWorkflowSnapshotHandoff({ ...target, expectedClaimGeneration: 1, mutationFence: 'other-fence' }),
    ).resolves.toMatchObject({ status: 'conflict', record: { status: 'completed' } });
    await expect(handoffRows(target.workflowName, target.runId)).resolves.toEqual([
      { status: 'completed', claim_generation: '1', released: false },
    ]);
  });

  it('answers concurrent and retried releases of one claim once with released, then already_released', async () => {
    const { owner, target, complete } = await claimedRun('release-twice');
    await complete();
    const release = () => workflows.releaseWorkflowSnapshotHandoff({ ...owner, expectedClaimGeneration: 1 });

    const results = await Promise.all([release(), release()]);
    expect(results.map(result => result.status).sort()).toEqual(['already_released', 'released']);
    await expect(release()).resolves.toMatchObject({ status: 'already_released', runDeleted: false });
    await expect(
      workflows.releaseWorkflowSnapshotHandoff({
        workflowName: target.workflowName,
        runId: randomUUID(),
        expectedClaimGeneration: 1,
      }),
    ).resolves.toEqual({ status: 'missing' });
  });

  it('refuses a replayed release once the run was claimed again under the same fence', async () => {
    const { owner, target, claim } = await claimedRun('release-replay');
    const release = { ...target, expectedClaimGeneration: 1, mutationFence: owner.mutationFence };
    await expect(workflows.releaseWorkflowSnapshotHandoff(release)).resolves.toMatchObject({ status: 'released' });

    await expect(
      claim(owner.mutationFence, snapshot(target.runId, 'waiting', { phase: 'reclaimed' })),
    ).resolves.toMatchObject({
      status: 'created',
      record: { status: 'pending', claimGeneration: 2, snapshot: { value: { phase: 'reclaimed' } } },
    });
    await expect(workflows.releaseWorkflowSnapshotHandoff(release)).resolves.toMatchObject({
      status: 'conflict',
      record: { status: 'pending', claimGeneration: 2 },
    });
    await expect(handoffRows(target.workflowName, target.runId)).resolves.toEqual([
      { status: 'pending', claim_generation: '2', released: false },
    ]);
    await expect(workflows.listWorkflowSnapshotHandoffs({ workflowName: target.workflowName })).resolves.toMatchObject({
      records: [{ claimGeneration: 2, status: 'pending' }],
    });
    await expect(workflows.deleteWorkflowRunById(target)).rejects.toBeInstanceOf(WorkflowSnapshotHandoffFenceError);
  });

  it('refuses a stale fenceless release of a newer completed claim', async () => {
    const { target, claim, complete } = await claimedRun('release-stale');
    await complete();
    await workflows.releaseWorkflowSnapshotHandoff({ ...target, expectedClaimGeneration: 1 });
    const second = snapshot(target.runId, 'waiting', { phase: 'second' });
    await expect(claim('second-owner', second)).resolves.toMatchObject({
      status: 'created',
      record: { claimGeneration: 2 },
    });
    await expect(complete('second-owner', second)).resolves.toMatchObject({
      status: 'completed',
      record: { claimGeneration: 2 },
    });

    await expect(
      workflows.releaseWorkflowSnapshotHandoff({ ...target, expectedClaimGeneration: 1 }),
    ).resolves.toMatchObject({ status: 'conflict', record: { status: 'completed', claimGeneration: 2 } });
    await expect(workflows.deleteWorkflowRunById(target)).rejects.toBeInstanceOf(WorkflowSnapshotHandoffFenceError);
    await expect(
      workflows.releaseWorkflowSnapshotHandoff({ ...target, expectedClaimGeneration: 2 }),
    ).resolves.toMatchObject({ status: 'released' });
  });

  it('deletes the run in the same transaction when asked, and a retry reports nothing left to delete', async () => {
    const { target, complete } = await claimedRun('release-delete');
    await complete();
    const release = { ...target, expectedClaimGeneration: 1, deleteRun: true };

    await expect(workflows.releaseWorkflowSnapshotHandoff(release)).resolves.toMatchObject({
      status: 'released',
      runDeleted: true,
    });
    await expect(workflows.loadWorkflowSnapshot(target)).resolves.toBeNull();
    await expect(workflows.releaseWorkflowSnapshotHandoff(release)).resolves.toMatchObject({
      status: 'already_released',
      runDeleted: false,
    });
  });

  it('lets retention prune a released run', async () => {
    const { target, complete } = await claimedRun('release-retention');
    await complete();
    await workflows.releaseWorkflowSnapshotHandoff({ ...target, expectedClaimGeneration: 1 });
    await new Promise(resolve => setTimeout(resolve, 10));
    await workflows.prune({ workflowSnapshot: { maxAge: '0ms' } });
    await expect(workflows.loadWorkflowSnapshot(target)).resolves.toBeNull();
  });

  describe('forced orderings', () => {
    const transitionOf = (owner: Owner, pending: WorkflowRunState) => () =>
      workflows.transitionWorkflowSnapshotHandoff({
        ...owner,
        expectedResourceId: owner.resourceId,
        expectedSnapshot: pending,
        snapshot: snapshot(owner.runId, 'waiting', { phase: 'transitioned' }),
      });

    it('transition first: the release by the fence holder releases the transitioned claim', async () => {
      const { owner, target, pending } = await claimedRun('order-transition-release');
      const [transition, release] = await inForcedOrder(target, transitionOf(owner, pending), () =>
        workflows.releaseWorkflowSnapshotHandoff({ ...owner, expectedClaimGeneration: 1 }),
      );
      expect(transition).toMatchObject({ status: 'transitioned' });
      expect(release).toMatchObject({
        status: 'released',
        record: { snapshot: { value: { phase: 'transitioned' } } },
      });
    });

    it('release first: the late transition is refused and the released claim is unchanged', async () => {
      const { owner, target, pending } = await claimedRun('order-release-transition');
      const [release, transition] = await inForcedOrder(
        target,
        () => workflows.releaseWorkflowSnapshotHandoff({ ...owner, expectedClaimGeneration: 1 }),
        transitionOf(owner, pending),
      );
      expect(release).toMatchObject({ status: 'released', record: { snapshot: { value: { phase: 'claimed' } } } });
      expect(transition).toMatchObject({ status: 'conflict', record: { snapshot: { value: { phase: 'claimed' } } } });
      await expect(handoffRows(target.workflowName, target.runId)).resolves.toEqual([
        { status: 'pending', claim_generation: '1', released: true },
      ]);
    });

    it('completion first: a fenceless release then releases the completed claim', async () => {
      const { target, complete } = await claimedRun('order-complete-release');
      const [completed, release] = await inForcedOrder(
        target,
        () => complete(),
        () => workflows.releaseWorkflowSnapshotHandoff({ ...target, expectedClaimGeneration: 1 }),
      );
      expect(completed).toMatchObject({ status: 'completed' });
      expect(release).toMatchObject({ status: 'released', record: { status: 'completed' } });
    });

    it('fenceless release first: it is refused while pending, and completion proceeds', async () => {
      const { target, complete } = await claimedRun('order-release-complete');
      const [release, completed] = await inForcedOrder(
        target,
        () => workflows.releaseWorkflowSnapshotHandoff({ ...target, expectedClaimGeneration: 1 }),
        () => complete(),
      );
      expect(release).toMatchObject({ status: 'conflict', record: { status: 'pending' } });
      expect(completed).toMatchObject({ status: 'completed' });
      await expect(handoffRows(target.workflowName, target.runId)).resolves.toEqual([
        { status: 'completed', claim_generation: '1', released: false },
      ]);
    });

    it('a re-claim queued behind a release with run deletion finds the run gone', async () => {
      const { owner, target, claim, complete } = await claimedRun('order-release-delete-claim');
      await complete();
      const [release, reclaim] = await inForcedOrder(
        target,
        () => workflows.releaseWorkflowSnapshotHandoff({ ...target, expectedClaimGeneration: 1, deleteRun: true }),
        () => claim(`next-${owner.mutationFence}`, snapshot(target.runId, 'waiting', { phase: 'late' })),
      );
      expect(release).toMatchObject({ status: 'released', runDeleted: true });
      expect(reclaim).toMatchObject({ status: 'conflict', observedCanonical: { kind: 'absent' } });
      await expect(handoffRows(target.workflowName, target.runId)).resolves.toEqual([
        { status: 'completed', claim_generation: '1', released: true },
      ]);
    });
  });

  it('adds the release columns to a handoff table created before them, once', async () => {
    const schemaName = `handoff_release_migration_${randomUUID().replace(/-/g, '')}`;
    schemasToDrop.push(schemaName);
    await adminPool.query(`CREATE SCHEMA "${schemaName}"`);
    await adminPool.query(`CREATE TABLE "${schemaName}".mastra_workflow_snapshot_handoffs (
      "workflow_name" TEXT NOT NULL,
      "run_id" TEXT NOT NULL,
      "version" INTEGER NOT NULL,
      "status" TEXT NOT NULL,
      "resource_id" TEXT,
      "snapshot" JSONB NOT NULL,
      "mutation_fence" TEXT NOT NULL,
      "created_at" BIGINT NOT NULL,
      "updated_at" BIGINT NOT NULL,
      "completed_at" BIGINT,
      PRIMARY KEY ("workflow_name", "run_id"),
      CHECK ("version" = 1),
      CHECK ("status" IN ('pending', 'completed')),
      CHECK (length("mutation_fence") BETWEEN 1 AND 4096),
      CHECK ((jsonb_typeof("snapshot") = 'object') IS TRUE),
      CHECK ("created_at" >= 0 AND "updated_at" >= "created_at"),
      CHECK (("status" = 'pending' AND "completed_at" IS NULL)
        OR ("status" = 'completed' AND "completed_at" IS NOT NULL AND "completed_at" >= "created_at"))
    )`);
    await adminPool.query(
      `INSERT INTO "${schemaName}".mastra_workflow_snapshot_handoffs
       (workflow_name, run_id, version, status, snapshot, mutation_fence, created_at, updated_at, completed_at)
       VALUES ('legacy', 'run-1', 1, 'completed', '{"status":"success"}', 'legacy-fence', 1, 2, 2)`,
    );

    const migrated = new PostgresStore({ id: `handoff-migrate-${schemaName}`, connectionString, schemaName });
    storesToClose.push(migrated);
    await migrated.init();
    const migratedWorkflows = (await migrated.getStore('workflows'))!;
    await expect(migratedWorkflows.listWorkflowSnapshotHandoffs({ workflowName: 'legacy' })).resolves.toMatchObject({
      records: [{ runId: 'run-1', status: 'completed', claimGeneration: 1 }],
    });
    await expect(
      migratedWorkflows.releaseWorkflowSnapshotHandoff({
        workflowName: 'legacy',
        runId: 'run-1',
        expectedClaimGeneration: 1,
      }),
    ).resolves.toMatchObject({ status: 'released', record: { mutationFence: 'legacy-fence' } });

    // A converged schema adds nothing on the next init.
    const warm = new PostgresStore({ id: `handoff-warm-${schemaName}`, connectionString, schemaName });
    storesToClose.push(warm);
    const statements: string[] = [];
    const original = Client.prototype.query;
    (Client.prototype as any).query = function (this: unknown, ...args: any[]) {
      const text = typeof args[0] === 'string' ? args[0] : args[0]?.text;
      if (typeof text === 'string') statements.push(text);
      return (original as any).apply(this, args);
    };
    try {
      await warm.init();
    } finally {
      (Client.prototype as any).query = original;
    }
    expect(statements.some(statement => /pg_catalog\.pg_tables/i.test(statement))).toBe(true);
    expect(
      statements.filter(statement => /ALTER TABLE[\s\S]*mastra_workflow_snapshot_handoffs/i.test(statement)),
    ).toEqual([]);
  }, 60000);
});
