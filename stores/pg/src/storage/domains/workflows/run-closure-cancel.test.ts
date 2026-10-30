import { randomUUID } from 'node:crypto';
import { createEmptyWorkflowSnapshot, createWorkflowTerminalGraphFingerprint } from '@mastra/core/storage';
import type { WorkflowRunState } from '@mastra/core/workflows';
import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { WorkflowsPG } from '../../index';
import { PostgresStore } from '../../index';

const connectionString = process.env.DB_URL || 'postgresql://postgres:postgres@localhost:5434/mastra';

type Run = { workflowName: string; runId: string };

const NESTED_STEP = 'nested';
const NESTED_GRAPH = [
  { type: 'step', step: { id: NESTED_STEP, component: 'WORKFLOW' } },
] as unknown as WorkflowRunState['serializedStepGraph'];

describe('WorkflowsPG erasure cancel', () => {
  let store: PostgresStore;
  let workflows: WorkflowsPG;
  const resource = `workspace-${randomUUID()}:user-1`;

  beforeAll(async () => {
    store = new PostgresStore({ id: `workflow-erasure-cancel-${randomUUID()}`, connectionString });
    await store.init();
    workflows = (await store.getStore('workflows'))!;
  }, 60000);

  afterAll(async () => {
    await store?.close();
  });

  const newRun = (): Run => ({ workflowName: `research-${randomUUID()}`, runId: randomUUID() });

  const persist = (run: Run, status: WorkflowRunState['status']) =>
    workflows.persistWorkflowSnapshot({
      ...run,
      resourceId: resource,
      snapshot: {
        ...createEmptyWorkflowSnapshot(run.runId),
        serializedStepGraph: NESTED_GRAPH,
        status,
        result: { draft: `${run.runId} draft` },
      },
    });

  const cancel = (run: Run, expectedResourceId = resource) =>
    workflows.cancelWorkflowRunForErasure({ ...run, expectedResourceId });

  const parentRevision = (run: Run) =>
    store.db.one<{ generation: string; terminal_status: string | null }>(
      `SELECT generation::text, terminal_status FROM mastra_workflow_parent_revisions
       WHERE workflow_name = $1 AND run_id = $2`,
      [run.workflowName, run.runId],
    );

  /** A running child whose recovery ancestry names `parent` and whose terminal journal is claimed. */
  async function pendingChild(parent: Run): Promise<Run> {
    const child = newRun();
    await persist(child, 'running');
    await expect(
      workflows.persistWorkflowTerminalRecoveryAncestry({
        ...child,
        ancestry: [
          {
            version: 1,
            childWorkflowName: child.workflowName,
            childRunId: child.runId,
            parentWorkflowName: parent.workflowName,
            parentRunId: parent.runId,
            parentGraphFingerprint: createWorkflowTerminalGraphFingerprint(NESTED_GRAPH),
            source: { kind: 'step', stepId: NESTED_STEP, executionPath: [0] },
            inputPointer: { kind: 'parent-source-payload', stepId: NESTED_STEP },
            resultPointer: { kind: 'retained-terminal-result', ...child },
            resumeMetadata: { wasResume: false, resumeSteps: [] },
          },
        ],
      }),
    ).resolves.toMatchObject({ status: 'persisted' });
    await expect(
      workflows.claimWorkflowTerminalization({
        ...child,
        eventKey: `${child.runId}-event`,
        terminalStatus: 'success',
        ownerId: `${child.runId}-owner`,
        leaseMs: 60_000,
      }),
    ).resolves.toMatchObject({ status: 'acquired' });
    return child;
  }

  it('advertises the erasure cancel', () => {
    expect(workflows.getWorkflowSnapshotHandoffCapabilities()).toMatchObject({ closureCancelVersion: 1 });
  });

  it('overwrites an unsettled run with an empty canceled snapshot, latching it, so the closure delete erases it', async () => {
    const run = newRun();
    await persist(run, 'suspended');
    const before = await parentRevision(run);

    await expect(cancel(run)).resolves.toEqual({ status: 'cancelled' });
    const stored = await workflows.loadWorkflowSnapshot(run);
    expect(stored).toMatchObject({ status: 'canceled', runId: run.runId });
    expect(stored?.result).toBeUndefined();
    const row = await store.db.one<{ resource_id: string }>(
      `SELECT "resourceId" AS resource_id FROM mastra_workflow_snapshot WHERE workflow_name = $1 AND run_id = $2`,
      [run.workflowName, run.runId],
    );
    expect(row.resource_id).toBe(resource);
    const after = await parentRevision(run);
    expect(Number(after.generation)).toBe(Number(before.generation) + 1);
    expect(after.terminal_status).toBe('canceled');

    await expect(workflows.persistWorkflowSnapshot({ ...run, snapshot: stored! })).resolves.toBeUndefined();
    await expect(workflows.updateWorkflowState({ ...run, opts: { status: 'canceled' } })).resolves.toMatchObject({
      status: 'canceled',
    });
    await expect(workflows.loadWorkflowSnapshot(run)).resolves.toMatchObject({ status: 'canceled' });
    await expect(cancel(run)).resolves.toEqual({ status: 'settled' });
    await expect(workflows.deleteWorkflowRunClosure({ ...run, expectedResourceId: resource })).resolves.toMatchObject({
      status: 'deleted',
    });
    await persist(run, 'running');
    await expect(workflows.loadWorkflowSnapshot(run)).resolves.toMatchObject({ status: 'running' });

    // Pause the cancel before its first snapshot read, then let the first
    // public persist commit. Revision evidence must come from that lifetime.
    const racing = newRun();
    const pool = new Pool({ connectionString, max: 1 });
    const cancelClients = new WeakSet<object>();
    let reachedSnapshotRead!: () => void;
    let resumeSnapshotRead!: () => void;
    const snapshotRead = new Promise<void>(resolve => (reachedSnapshotRead = resolve));
    const resume = new Promise<void>(resolve => (resumeSnapshotRead = resolve));
    let paused = false;
    pool.on('connect', client => {
      const query = client.query;
      client.query = function (this: typeof client, ...args: unknown[]) {
        const text = typeof args[0] === 'string' ? args[0] : '';
        const values = Array.isArray(args[1]) ? args[1] : [];
        if (
          text.includes('pg_advisory_xact_lock') &&
          values[0] === JSON.stringify([racing.workflowName, racing.runId])
        ) {
          cancelClients.add(client);
        }
        if (
          !paused &&
          cancelClients.has(client) &&
          text.trimStart().startsWith('SELECT') &&
          text.includes('mastra_workflow_snapshot') &&
          !text.includes('mastra_workflow_snapshot_handoff') &&
          values[0] === racing.workflowName &&
          values[1] === racing.runId
        ) {
          paused = true;
          reachedSnapshotRead();
          return resume.then(() => Reflect.apply(query, this, args));
        }
        return Reflect.apply(query, this, args);
      } as typeof client.query;
    });
    const racingStore = new PostgresStore({ id: `workflow-erasure-create-${randomUUID()}`, pool });
    let pending: Promise<unknown> | undefined;
    try {
      await racingStore.init();
      const racingWorkflows = (await racingStore.getStore('workflows'))!;
      const cancellation = racingWorkflows.cancelWorkflowRunForErasure({ ...racing, expectedResourceId: resource });
      pending = Promise.allSettled([cancellation]);
      await Promise.race([
        snapshotRead,
        cancellation.then(() => {
          throw new Error('Cancellation settled before its snapshot read');
        }),
      ]);
      await persist(racing, 'running');
      resumeSnapshotRead();
      await expect(cancellation).resolves.toEqual({ status: 'cancelled' });
      await expect(workflows.loadWorkflowSnapshot(racing)).resolves.toMatchObject({ status: 'canceled' });
      await expect(parentRevision(racing)).resolves.toMatchObject({ generation: '2', terminal_status: 'canceled' });
      await expect(
        workflows.deleteWorkflowRunClosure({ ...racing, expectedResourceId: resource }),
      ).resolves.toMatchObject({ status: 'deleted' });
    } finally {
      resumeSnapshotRead();
      await pending;
      await racingStore.close();
      await pool.end();
    }
  });

  it('rolls back late snapshot and state writes after the cancel, so the closure delete still erases the run', async () => {
    const run = newRun();
    await persist(run, 'waiting');
    await expect(cancel(run)).resolves.toEqual({ status: 'cancelled' });
    const latched = await parentRevision(run);

    // A queued engine write after the cancel: the parent revision conflict rolls it back.
    await expect(persist(run, 'running')).rejects.toThrow();
    for (const status of ['running', 'suspended'] as const) {
      await expect(workflows.updateWorkflowState({ ...run, opts: { status } })).rejects.toThrow();
    }
    await expect(workflows.loadWorkflowSnapshot(run)).resolves.toMatchObject({ status: 'canceled' });
    await expect(parentRevision(run)).resolves.toEqual(latched);
    await expect(workflows.deleteWorkflowRunClosure({ ...run, expectedResourceId: resource })).resolves.toMatchObject({
      status: 'deleted',
    });
  });

  it('refuses an ordinary reopen of a settled run, so the erasure cancel and closure delete still finish', async () => {
    const run = newRun();
    await persist(run, 'success');
    // The parent revision latched `success`: a reopening write is rolled back.
    await expect(persist(run, 'running')).rejects.toThrow();
    await expect(workflows.loadWorkflowSnapshot(run)).resolves.toMatchObject({ status: 'success' });
    await expect(cancel(run)).resolves.toEqual({ status: 'settled' });
    await expect(workflows.deleteWorkflowRunClosure({ ...run, expectedResourceId: resource })).resolves.toMatchObject({
      status: 'deleted',
    });
  });

  it('refuses a settled run whose parent revision evidence is missing instead of answering settled', async () => {
    const run = newRun();
    await persist(run, 'success');
    await store.db.none(`DELETE FROM mastra_workflow_parent_revisions WHERE workflow_name = $1 AND run_id = $2`, [
      run.workflowName,
      run.runId,
    ]);
    await expect(cancel(run)).rejects.toThrow();
    await expect(workflows.loadWorkflowSnapshot(run)).resolves.toMatchObject({ status: 'success' });
  });

  it('refuses a nested ownership bind on a cancelled run, so no transient child starts', async () => {
    const run = newRun();
    await persist(run, 'suspended');
    await expect(cancel(run)).resolves.toEqual({ status: 'cancelled' });
    await expect(
      workflows.bindWorkflowNestedRunOwnership({
        ...run,
        stepId: NESTED_STEP,
        nestedRunId: randomUUID(),
        result: { status: 'running', payload: {} },
        requestContext: {},
      } as never),
    ).resolves.toEqual({ status: 'missing_run' });
    await expect(workflows.deleteWorkflowRunClosure({ ...run, expectedResourceId: resource })).resolves.toMatchObject({
      status: 'deleted',
    });
  });

  it('refuses a parent a nested child still needs, and the child whose journal is claimed, without changes', async () => {
    const parent = newRun();
    await persist(parent, 'running');
    const child = await pendingChild(parent);
    const before = await parentRevision(parent);

    await expect(cancel(parent)).resolves.toEqual({ status: 'active' });
    await expect(cancel(child)).resolves.toEqual({ status: 'journaled' });
    await expect(workflows.loadWorkflowSnapshot(parent)).resolves.toMatchObject({
      status: 'running',
      result: { draft: `${parent.runId} draft` },
    });
    await expect(workflows.loadWorkflowSnapshot(child)).resolves.toMatchObject({ status: 'running' });
    await expect(parentRevision(parent)).resolves.toEqual(before);
  });

  it('answers missing, resource_mismatch, fenced and settled without changes', async () => {
    const missing = newRun();
    await expect(cancel(missing)).resolves.toEqual({ status: 'missing' });
    await expect(
      store.db.any(`SELECT generation FROM mastra_workflow_parent_revisions WHERE workflow_name = $1 AND run_id = $2`, [
        missing.workflowName,
        missing.runId,
      ]),
    ).resolves.toEqual([]);

    const running = newRun();
    await persist(running, 'running');
    await expect(cancel(running, `workspace-${randomUUID()}:user-1`)).resolves.toEqual({
      status: 'resource_mismatch',
    });

    const delivered = newRun();
    await persist(delivered, 'success');
    const canonical = (await workflows.loadWorkflowSnapshot(delivered))!;
    await expect(
      workflows.claimWorkflowSnapshotHandoff({
        ...delivered,
        resourceId: resource,
        mutationFence: `fence-${randomUUID()}`,
        expectedCanonical: { kind: 'present', resourceId: resource, snapshot: canonical },
        snapshot: { ...canonical, value: { phase: 'claimed' } } as WorkflowRunState,
      }),
    ).resolves.toMatchObject({ status: 'created' });
    await expect(cancel(delivered)).resolves.toEqual({ status: 'fenced' });

    const settled = newRun();
    await persist(settled, 'failed');
    await expect(cancel(settled)).resolves.toEqual({ status: 'settled' });

    await expect(workflows.loadWorkflowSnapshot(running)).resolves.toMatchObject({ status: 'running' });
    await expect(workflows.loadWorkflowSnapshot(settled)).resolves.toMatchObject({
      status: 'failed',
      result: { draft: `${settled.runId} draft` },
    });

    const orphan = newRun();
    await persist(orphan, 'running');
    await store.db.none(`DELETE FROM mastra_workflow_parent_revisions WHERE workflow_name = $1 AND run_id = $2`, [
      orphan.workflowName,
      orphan.runId,
    ]);
    try {
      await expect(cancel(orphan, `workspace-${randomUUID()}:user-1`)).resolves.toEqual({
        status: 'resource_mismatch',
      });
      await expect(cancel(orphan)).rejects.toThrow('Workflow snapshot is missing parent revision evidence');
      await expect(workflows.loadWorkflowSnapshot(orphan)).resolves.toMatchObject({ status: 'running' });
      await expect(
        store.db.any(
          `SELECT generation FROM mastra_workflow_parent_revisions WHERE workflow_name = $1 AND run_id = $2`,
          [orphan.workflowName, orphan.runId],
        ),
      ).resolves.toEqual([]);
    } finally {
      await store.db.none(`DELETE FROM mastra_workflow_snapshot WHERE workflow_name = $1 AND run_id = $2`, [
        orphan.workflowName,
        orphan.runId,
      ]);
    }
  });

  describe('under contention on the run lock', () => {
    const lockKey = (run: Run) => JSON.stringify([run.workflowName, run.runId]);
    /** A separate session that holds the run lock while the store's calls queue on it. */
    const lockPool = new Pool({ connectionString, max: 1 });
    afterAll(async () => {
      await lockPool.end();
    });

    /** Waits until `count` sessions queue on the run's advisory lock. */
    async function untilQueued(run: Run, count: number) {
      for (let attempt = 0; attempt < 400; attempt++) {
        const queued = await store.db.one<{ n: number }>(
          `SELECT count(*)::int AS n FROM pg_locks
           WHERE locktype = 'advisory' AND NOT granted
             AND ((classid::bigint << 32) | objid::bigint) = hashtextextended($1, 0)`,
          [lockKey(run)],
        );
        if (queued.n >= count) return;
        await new Promise(resolve => setTimeout(resolve, 25));
      }
      throw new Error(`fewer than ${count} sessions queued on the run lock`);
    }

    /**
     * Holds the run's advisory lock (the first lock both the cancel and its
     * competitor take) while it starts `first`, then `second`, then lets go.
     */
    async function inLockOrder<A, B>(run: Run, first: () => Promise<A>, second: () => Promise<B>) {
      const holder = await lockPool.connect();
      const pending: Promise<unknown>[] = [];
      let transactionOpen = false;
      try {
        await holder.query('BEGIN');
        transactionOpen = true;
        await holder.query(`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, [lockKey(run)]);
        const a = first();
        pending.push(Promise.allSettled([a]));
        await untilQueued(run, 1);
        const b = second();
        pending.push(Promise.allSettled([b]));
        await untilQueued(run, 2);
        await holder.query('COMMIT');
        transactionOpen = false;
        return await Promise.allSettled([a, b]);
      } finally {
        try {
          if (transactionOpen) await holder.query('ROLLBACK');
        } finally {
          // Destroy an interrupted holder even if rollback failed, releasing
          // its advisory lock before draining every call already started.
          holder.release(transactionOpen);
          await Promise.allSettled(pending);
        }
      }
    }

    const ancestryOf = (child: Run, parent: Run) => ({
      ...child,
      ancestry: [
        {
          version: 1 as const,
          childWorkflowName: child.workflowName,
          childRunId: child.runId,
          parentWorkflowName: parent.workflowName,
          parentRunId: parent.runId,
          parentGraphFingerprint: createWorkflowTerminalGraphFingerprint(NESTED_GRAPH),
          source: { kind: 'step' as const, stepId: NESTED_STEP, executionPath: [0] },
          inputPointer: { kind: 'parent-source-payload' as const, stepId: NESTED_STEP },
          resultPointer: { kind: 'retained-terminal-result' as const, ...child },
          resumeMetadata: { wasResume: false, resumeSteps: [] },
        },
      ],
    });

    it('waits behind a journal claim on the run lock, then answers journaled', async () => {
      const run = newRun();
      await persist(run, 'running');
      const [claim, cancelled] = await inLockOrder(
        run,
        () =>
          workflows.claimWorkflowTerminalization({
            ...run,
            eventKey: `${run.runId}-event`,
            terminalStatus: 'success',
            ownerId: `${run.runId}-owner`,
            leaseMs: 60_000,
          }),
        () => cancel(run),
      );
      expect(claim).toMatchObject({ status: 'fulfilled', value: { status: 'acquired' } });
      expect(cancelled).toEqual({ status: 'fulfilled', value: { status: 'journaled' } });
      await expect(workflows.loadWorkflowSnapshot(run)).resolves.toMatchObject({ status: 'running' });
    });

    it('serializes a child ancestry admission with the cancel of its parent, in either order', async () => {
      const parent = newRun();
      const child = newRun();
      await persist(parent, 'running');
      await persist(child, 'running');
      // The cancel first: the admission then finds the parent latched canceled.
      const [cancelled, admitted] = await inLockOrder(
        parent,
        () => cancel(parent),
        () => workflows.persistWorkflowTerminalRecoveryAncestry(ancestryOf(child, parent)),
      );
      expect(cancelled).toEqual({ status: 'fulfilled', value: { status: 'cancelled' } });
      expect(admitted).toMatchObject({ status: 'rejected' });
      await expect(workflows.getWorkflowTerminalRecoveryAncestry(child)).resolves.toEqual({
        status: 'missing_ancestry',
      });

      const otherParent = newRun();
      const otherChild = newRun();
      await persist(otherParent, 'running');
      await persist(otherChild, 'running');
      // The admission first: the parent now has a pending dependent.
      const [admittedFirst, refused] = await inLockOrder(
        otherParent,
        () => workflows.persistWorkflowTerminalRecoveryAncestry(ancestryOf(otherChild, otherParent)),
        () => cancel(otherParent),
      );
      expect(admittedFirst).toMatchObject({ status: 'fulfilled', value: { status: 'persisted' } });
      expect(refused).toEqual({ status: 'fulfilled', value: { status: 'active' } });
      await expect(workflows.loadWorkflowSnapshot(otherParent)).resolves.toMatchObject({ status: 'running' });
    });

    it('serializes the closure delete with the cancel without a deadlock, in either order', async () => {
      const run = newRun();
      await persist(run, 'suspended');
      const [cancelled, deleted] = await inLockOrder(
        run,
        () => cancel(run),
        () => workflows.deleteWorkflowRunClosure({ ...run, expectedResourceId: resource }),
      );
      expect(cancelled).toEqual({ status: 'fulfilled', value: { status: 'cancelled' } });
      expect(deleted).toMatchObject({ status: 'fulfilled', value: { status: 'deleted' } });

      const other = newRun();
      await persist(other, 'suspended');
      const [active, cancelledAfter] = await inLockOrder(
        other,
        () => workflows.deleteWorkflowRunClosure({ ...other, expectedResourceId: resource }),
        () => cancel(other),
      );
      expect(active).toEqual({ status: 'fulfilled', value: { status: 'active' } });
      expect(cancelledAfter).toEqual({ status: 'fulfilled', value: { status: 'cancelled' } });
    });
  });
});
