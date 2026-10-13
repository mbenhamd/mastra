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

  /** Backends waiting, directly or behind another waiter, on the blocker's lock. */
  const waitersBehind = async (blockerPid: number): Promise<number> => {
    const { rows } = await adminPool.query<{ waiters: number }>(
      `WITH RECURSIVE chain(pid) AS (
         SELECT $1::int
         UNION
         SELECT activity.pid FROM pg_stat_activity AS activity
         JOIN chain ON chain.pid = ANY(pg_blocking_pids(activity.pid))
       )
       SELECT count(*)::int - 1 AS waiters FROM chain`,
      [blockerPid],
    );
    return rows[0]!.waiters;
  };

  /**
   * Runs `first` and `second` against one run in a forced order: a blocking
   * transaction holds the run's parent revision row, `first` is queued behind
   * it before `second` starts (counted through the blocker's own lock chain,
   * so other sessions on the database cannot skew it), and PostgreSQL grants
   * the row lock to waiters in queue order once the blocker commits.
   */
  async function inForcedOrder<A, B>(
    target: { workflowName: string; runId: string },
    first: () => Promise<A>,
    second: () => Promise<B>,
  ): Promise<[A, B]> {
    const blocker = new Client({ connectionString });
    await blocker.connect();
    try {
      const { rows: backend } = await blocker.query<{ pid: number }>('SELECT pg_backend_pid() AS pid');
      const blockerPid = backend[0]!.pid;
      await blocker.query('BEGIN');
      await blocker.query(
        `SELECT generation FROM mastra_workflow_parent_revisions
         WHERE workflow_name = $1 AND run_id = $2 FOR UPDATE`,
        [target.workflowName, target.runId],
      );
      const waitFor = async (count: number) => {
        const deadline = Date.now() + 10_000;
        while ((await waitersBehind(blockerPid)) < count) {
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

  it('deletes the run in the same transaction when asked, and a late retry never deletes later writes', async () => {
    const { target, complete } = await claimedRun('release-delete');
    await complete();
    const release = { ...target, expectedClaimGeneration: 1, deleteRun: true };

    await expect(workflows.releaseWorkflowSnapshotHandoff(release)).resolves.toMatchObject({
      status: 'released',
      runDeleted: true,
    });
    await expect(workflows.loadWorkflowSnapshot(target)).resolves.toBeNull();
    // The release lets native writes through; a delayed retry must not delete them.
    await workflows.persistWorkflowSnapshot({
      ...target,
      snapshot: snapshot(target.runId, 'success', { phase: 'after-release' }),
    });
    await expect(workflows.releaseWorkflowSnapshotHandoff(release)).resolves.toMatchObject({
      status: 'already_released',
      runDeleted: false,
    });
    await expect(workflows.loadWorkflowSnapshot(target)).resolves.toMatchObject({
      value: { phase: 'after-release' },
    });
  });

  it('replaces the kept record of a released completed claim when another fence claims the run', async () => {
    const { owner, target, claim, complete } = await claimedRun('release-overwrite');
    await complete();
    await workflows.releaseWorkflowSnapshotHandoff({ ...target, expectedClaimGeneration: 1 });

    await expect(claim('second-owner', snapshot(target.runId, 'waiting', { phase: 'second' }))).resolves.toMatchObject({
      status: 'created',
      record: { claimGeneration: 2, status: 'pending', mutationFence: 'second-owner' },
    });
    await expect(
      claim(owner.mutationFence, snapshot(target.runId, 'waiting', { phase: 'retried' })),
    ).resolves.toMatchObject({ status: 'conflict', record: { claimGeneration: 2, mutationFence: 'second-owner' } });
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

  it('fails closed naming the missing release columns when init is disabled, and recovers after the documented DDL', async () => {
    const schemaName = `handoff_release_external_${randomUUID().replace(/-/g, '')}`;
    schemasToDrop.push(schemaName);
    await adminPool.query(`CREATE SCHEMA "${schemaName}"`);
    const managed = new PostgresStore({ id: `handoff-managed-${schemaName}`, connectionString, schemaName });
    storesToClose.push(managed);
    await managed.init();
    const managedWorkflows = (await managed.getStore('workflows'))!;
    const runId = randomUUID();
    const canonical = snapshot(runId, 'success', { native: true });
    await managedWorkflows.persistWorkflowSnapshot({ workflowName: 'external', runId, snapshot: canonical });
    await managedWorkflows.claimWorkflowSnapshotHandoff({
      workflowName: 'external',
      runId,
      expectedCanonical: { kind: 'present', snapshot: canonical },
      snapshot: snapshot(runId, 'waiting'),
      mutationFence: 'external-owner',
    });
    // An externally managed schema that has not run the release migration yet.
    await adminPool.query(
      `ALTER TABLE "${schemaName}".mastra_workflow_snapshot_handoffs DROP COLUMN claim_generation, DROP COLUMN released_at`,
    );

    const external = new PostgresStore({
      id: `handoff-external-${schemaName}`,
      connectionString,
      schemaName,
      disableInit: true,
    });
    storesToClose.push(external);
    const externalWorkflows = (await external.getStore('workflows'))!;
    const missing = /is missing required columns: claim_generation, released_at\. Run: ALTER TABLE/;
    await expect(externalWorkflows.listWorkflowSnapshotHandoffs({ workflowName: 'external' })).rejects.toThrow(missing);
    await expect(
      externalWorkflows.persistWorkflowSnapshot({
        workflowName: 'external',
        runId,
        snapshot: snapshot(runId, 'failed'),
      }),
    ).rejects.toThrow(missing);
    await expect(
      externalWorkflows.releaseWorkflowSnapshotHandoff({
        workflowName: 'external',
        runId,
        expectedClaimGeneration: 1,
        mutationFence: 'external-owner',
      }),
    ).rejects.toThrow(missing);

    await adminPool.query(WorkflowsPG.getWorkflowSnapshotHandoffReleaseDDL(schemaName));
    await adminPool.query(WorkflowsPG.getWorkflowSnapshotHandoffReleaseDDL(schemaName));
    await expect(externalWorkflows.listWorkflowSnapshotHandoffs({ workflowName: 'external' })).resolves.toMatchObject({
      records: [{ runId, status: 'pending', claimGeneration: 1 }],
    });
    await expect(
      externalWorkflows.releaseWorkflowSnapshotHandoff({
        workflowName: 'external',
        runId,
        expectedClaimGeneration: 1,
        mutationFence: 'external-owner',
      }),
    ).resolves.toMatchObject({ status: 'released' });
  }, 60000);

  it('fails closed when an older init created the table without the columns after this init saw it absent', async () => {
    const schemaName = `handoff_release_race_${randomUUID().replace(/-/g, '')}`;
    schemasToDrop.push(schemaName);
    await adminPool.query(`CREATE SCHEMA "${schemaName}"`);
    const managed = new PostgresStore({ id: `handoff-race-${schemaName}`, connectionString, schemaName });
    storesToClose.push(managed);
    await managed.init();
    // Stands in for an older version's concurrent init winning the CREATE.
    await adminPool.query(
      `ALTER TABLE "${schemaName}".mastra_workflow_snapshot_handoffs DROP COLUMN claim_generation, DROP COLUMN released_at`,
    );
    const managedWorkflows = (await managed.getStore('workflows'))!;
    await expect(managedWorkflows.listWorkflowSnapshotHandoffs()).rejects.toThrow(
      /is missing required columns: claim_generation, released_at/,
    );
    await adminPool.query(WorkflowsPG.getWorkflowSnapshotHandoffReleaseDDL(schemaName));
    await expect(managedWorkflows.listWorkflowSnapshotHandoffs()).resolves.toEqual({ records: [], hasMore: false });
  }, 60000);

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
