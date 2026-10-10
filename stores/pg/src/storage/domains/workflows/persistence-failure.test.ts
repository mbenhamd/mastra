import { randomUUID } from 'node:crypto';
import { MastraError } from '@mastra/core/error';
import { Mastra } from '@mastra/core/mastra';
import { createEmptyWorkflowSnapshot, getStoragePersistenceFailure } from '@mastra/core/storage';
import type { WorkflowsStorage } from '@mastra/core/storage';
import { createStep, createWorkflow } from '@mastra/core/workflows';
import { Pool } from 'pg';
import type { PoolClient, QueryResult } from 'pg';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { PostgresStore } from '../..';

const config = {
  host: process.env.POSTGRES_HOST || '127.0.0.1',
  port: Number(process.env.POSTGRES_PORT) || 5434,
  database: process.env.POSTGRES_DB || 'postgres',
  user: process.env.POSTGRES_USER || 'postgres',
  password: process.env.POSTGRES_PASSWORD || 'postgres',
};
const WRITER = 'pf4960-classified-writer';
const WORKFLOW_ID = 'pg-persistence-failure-wf';

type CommitInterceptor = (client: PoolClient, commit: () => Promise<QueryResult>) => Promise<QueryResult>;

async function rejection(promise: Promise<unknown>): Promise<unknown> {
  return promise.then(
    () => {
      throw new Error('expected the operation to reject');
    },
    (error: unknown) => error,
  );
}

/**
 * Failures are produced by the real server: backends are terminated with
 * pg_terminate_backend() and locks time out under lock_timeout. The only test
 * seam is a hook on the COMMIT statement of the writer pool's clients, which
 * decides when the termination happens relative to the COMMIT.
 */
describe('WorkflowsPG persistence failure classification on PostgreSQL', () => {
  const schemaName = `persistence_failure_${randomUUID().replaceAll('-', '')}`;
  const admin = new Pool(config);
  const writerPool = new Pool({ ...config, application_name: WRITER, options: '-c lock_timeout=8000' });
  let interceptCommit: CommitInterceptor | undefined;
  const stores: PostgresStore[] = [];
  let writer: WorkflowsStorage;
  let reader: WorkflowsStorage;

  // A terminated backend also reports on its client and on the pool; the
  // failing statement already carries the error under test.
  writerPool.on('error', () => undefined);
  writerPool.on('connect', client => {
    client.on('error', () => undefined);
    const query = client.query.bind(client) as (...args: unknown[]) => Promise<QueryResult>;
    (client as unknown as { query: (...args: unknown[]) => unknown }).query = (...args: unknown[]) => {
      const interceptor = interceptCommit;
      if (args[0] === 'COMMIT' && interceptor) {
        interceptCommit = undefined;
        return interceptor(client, () => query('COMMIT'));
      }
      return query(...args);
    };
  });

  function storeOn(pool: Pool, schema = schemaName) {
    const store = new PostgresStore({ id: `pf4960-${stores.length}`, pool, schemaName: schema });
    stores.push(store);
    return store;
  }

  async function backendPid(client: PoolClient): Promise<number> {
    const { rows } = await client.query<{ pid: number }>('SELECT pg_backend_pid() AS pid');
    return rows[0]!.pid;
  }

  /** COMMIT reaches the server and commits; the session dies before the client sees an answer. */
  const commitThenTerminate: CommitInterceptor = async (client, commit) => {
    await commit();
    return client.query('SELECT pg_terminate_backend(pg_backend_pid())');
  };

  /** The session dies while idle in its transaction; the client never sends COMMIT. */
  const terminateBeforeCommit: CommitInterceptor = async (client, commit) => {
    const pid = await backendPid(client);
    const ended = new Promise(resolve => client.once('end', resolve));
    await admin.query('SELECT pg_terminate_backend($1)', [pid]);
    await ended;
    return commit();
  };

  const terminalFence = {
    workflowName: 'terminal-lookup-wf',
    runId: 'missing-run',
    ownerId: 'owner',
    claimToken: 'token',
    claimGeneration: 1,
  };

  async function seedRun(workflowName: string, runId: string) {
    const snapshot = createEmptyWorkflowSnapshot(runId);
    snapshot.status = 'running';
    await reader.persistWorkflowSnapshot({ workflowName, runId, snapshot });
  }

  async function persistSuccess(workflowName: string, runId: string) {
    const snapshot = createEmptyWorkflowSnapshot(runId);
    snapshot.status = 'success';
    return writer.persistWorkflowSnapshot({ workflowName, runId, snapshot });
  }

  async function durableStatus(workflowName: string, runId: string) {
    return (await reader.loadWorkflowSnapshot({ workflowName, runId }))?.status;
  }

  beforeAll(async () => {
    const setup = storeOn(admin);
    await setup.init();
    reader = (await setup.getStore('workflows'))!;
    const writerStore = storeOn(writerPool);
    await writerStore.init();
    writer = (await writerStore.getStore('workflows'))!;
  });

  afterAll(async () => {
    await Promise.all(stores.map(store => store.close()));
    await admin.query(`DROP SCHEMA IF EXISTS "${schemaName}" CASCADE`);
    await Promise.all([admin.end(), writerPool.end()]);
  });

  it('classifies a write whose COMMIT committed before the session died as commit_unknown', async () => {
    const workflowName = `committed-${randomUUID()}`;
    await seedRun(workflowName, 'run');
    interceptCommit = commitThenTerminate;

    const error = await rejection(persistSuccess(workflowName, 'run'));

    expect(error).toBeInstanceOf(MastraError);
    expect(error).toMatchObject({
      id: 'MASTRA_STORAGE_PG_PERSIST_WORKFLOW_SNAPSHOT_FAILED',
      details: { workflowName, runId: 'run', persistenceFailure: 'commit_unknown' },
    });
    // The durable read, not the failure, says what happened.
    expect(await durableStatus(workflowName, 'run')).toBe('success');
  });

  it('classifies a COMMIT the client could not send as transient and leaves the run unchanged', async () => {
    const workflowName = `unsent-${randomUUID()}`;
    await seedRun(workflowName, 'run');
    interceptCommit = terminateBeforeCommit;

    const error = await rejection(persistSuccess(workflowName, 'run'));

    expect(getStoragePersistenceFailure(error)).toBe('transient');
    expect(await durableStatus(workflowName, 'run')).toBe('running');
  });

  it.each([
    ['times out on a lock', false],
    ['loses its backend', true],
  ])(
    'classifies a write that %s before COMMIT as transient and leaves the run unchanged',
    async (_name, kill) => {
      const workflowName = `blocked-${randomUUID()}`;
      await seedRun(workflowName, 'run');
      const blocker = await admin.connect();
      try {
        await blocker.query('BEGIN');
        await blocker.query(`LOCK TABLE "${schemaName}".mastra_workflow_snapshot IN ACCESS EXCLUSIVE MODE`);
        const write = rejection(persistSuccess(workflowName, 'run'));
        if (kill) {
          await vi.waitFor(
            async () => {
              const { rowCount } = await admin.query(
                `SELECT pg_terminate_backend(pid) FROM pg_stat_activity
               WHERE application_name = $1 AND wait_event_type = 'Lock'`,
                [WRITER],
              );
              expect(rowCount).toBe(1);
            },
            { timeout: 5000, interval: 50 },
          );
        }

        const error = await write;

        expect(error).toMatchObject({
          details: { persistenceFailure: 'transient' },
          cause: { code: kill ? '57P01' : '55P03' },
        });
      } finally {
        await blocker.query('ROLLBACK');
        blocker.release();
      }
      expect(await durableStatus(workflowName, 'run')).toBe('running');
    },
    20_000,
  );

  it.each([
    ['commits before the session dies', commitThenTerminate, 'commit_unknown', 'success'],
    ['cannot send its COMMIT', terminateBeforeCommit, 'transient', 'running'],
  ] as const)(
    'classifies a step update that %s by what it may have left behind',
    async (_name, interceptor, persistenceFailure, durable) => {
      const workflowName = `step-update-${randomUUID()}`;
      await seedRun(workflowName, 'run');
      const snapshot = createEmptyWorkflowSnapshot('run');
      snapshot.status = 'success';
      interceptCommit = interceptor;

      const error = await rejection(writer.persistWorkflowStepUpdate({ workflowName, runId: 'run', snapshot }));

      expect(error).toMatchObject({
        id: 'MASTRA_STORAGE_PG_PERSIST_WORKFLOW_STEP_UPDATE_FAILED',
        details: { workflowName, runId: 'run', persistenceFailure },
      });
      expect(await durableStatus(workflowName, 'run')).toBe(durable);
    },
  );

  it.each([
    [
      'getWorkflowTerminalEffectForDispatch',
      () => writer.getWorkflowTerminalEffectForDispatch({ ...terminalFence, kind: 'workflow-finish' }),
    ],
    [
      'getWorkflowTerminalDestinationReceipt',
      () =>
        writer.getWorkflowTerminalDestinationReceipt({
          ...terminalFence,
          effectKind: 'workflow-finish',
          consumerId: 'consumer',
        }),
    ],
    ['getWorkflowTerminalContinuationPlan', () => writer.getWorkflowTerminalContinuationPlan(terminalFence)],
  ])('classifies a lost COMMIT response of the %s lookup as a transient read', async (_name, lookup) => {
    interceptCommit = commitThenTerminate;

    const error = await rejection(lookup());

    expect(getStoragePersistenceFailure(error)).toBe('transient');
  });

  it('classifies writes and reads the server rejects as permanent', async () => {
    const missing = (await storeOn(writerPool, `missing_${randomUUID().replaceAll('-', '')}`).getStore('workflows'))!;
    const snapshot = createEmptyWorkflowSnapshot('run');

    const write = await rejection(missing.persistWorkflowSnapshot({ workflowName: 'wf', runId: 'run', snapshot }));
    const read = await rejection(missing.loadWorkflowSnapshot({ workflowName: 'wf', runId: 'run' }));

    expect(getStoragePersistenceFailure(write)).toBe('permanent');
    expect(getStoragePersistenceFailure(read)).toBe('permanent');
  });

  it('classifies an unreachable server as transient for writes and reads', async () => {
    const unreachable = new Pool({ ...config, port: 1, connectionTimeoutMillis: 2000 });
    try {
      const store = (await storeOn(unreachable).getStore('workflows'))!;
      const snapshot = createEmptyWorkflowSnapshot('run');

      const write = await rejection(store.persistWorkflowSnapshot({ workflowName: 'wf', runId: 'run', snapshot }));
      const read = await rejection(store.loadWorkflowSnapshot({ workflowName: 'wf', runId: 'run' }));

      expect(getStoragePersistenceFailure(write)).toBe('transient');
      expect(getStoragePersistenceFailure(read)).toBe('transient');
    } finally {
      await unreachable.end();
    }
  });

  describe('resume admission', () => {
    let downstreamRuns = 0;

    function createApprovalWorkflow() {
      const approval = createStep({
        id: 'approval',
        inputSchema: z.object({}),
        outputSchema: z.object({ approved: z.boolean() }),
        resumeSchema: z.object({ approved: z.boolean() }),
        execute: async ({ resumeData, suspend }) => {
          if (!resumeData) {
            await suspend({});
            return { approved: false };
          }
          return { approved: resumeData.approved };
        },
      });
      const downstream = createStep({
        id: 'downstream',
        inputSchema: z.object({ approved: z.boolean() }),
        outputSchema: z.object({ approved: z.boolean() }),
        execute: async ({ inputData }) => {
          downstreamRuns++;
          return inputData;
        },
      });
      return createWorkflow({
        id: WORKFLOW_ID,
        inputSchema: z.object({}),
        outputSchema: z.object({ approved: z.boolean() }),
        steps: [approval, downstream],
      })
        .then(approval)
        .then(downstream)
        .commit();
    }

    async function suspendedRun() {
      const workflow = createApprovalWorkflow();
      const storage = storeOn(writerPool);
      const mastra = new Mastra({ storage, workflows: { [WORKFLOW_ID]: workflow }, logger: false });
      const workflows = (await storage.getStore('workflows'))!;
      const run = await workflow.createRun();
      expect((await run.start({ inputData: {} })).status).toBe('suspended');
      // Arm the COMMIT hook only for the resume's admission claim.
      const updateWorkflowState = workflows.updateWorkflowState.bind(workflows);
      return {
        mastra,
        run,
        armClaim(interceptor: CommitInterceptor) {
          vi.spyOn(workflows, 'updateWorkflowState').mockImplementationOnce(args => {
            interceptCommit = interceptor;
            return updateWorkflowState(args);
          });
        },
        durable: () => reader.loadWorkflowSnapshot({ workflowName: WORKFLOW_ID, runId: run.runId }),
      };
    }

    it('does not run a resume twice when its committed claim was reported as commit_unknown', async () => {
      downstreamRuns = 0;
      const { mastra, run, armClaim, durable } = await suspendedRun();
      try {
        armClaim(commitThenTerminate);

        const error = await rejection(run.resume({ step: 'approval', resumeData: { approved: true } }));

        expect(getStoragePersistenceFailure(error)).toBe('commit_unknown');
        // The claim is durable although its caller saw a failure.
        expect(await durable()).toMatchObject({ status: 'running', lifecycleResumeAttempt: 1 });
        // A blind retry cannot claim the suspension again.
        const retry = await rejection(run.resume({ step: 'approval', resumeData: { approved: true } }));
        expect(retry).toMatchObject({ id: 'WORKFLOW_RUN_NOT_SUSPENDED' });
        expect(downstreamRuns).toBe(0);
      } finally {
        await mastra.shutdown();
      }
    });

    it('resumes once on retry after a transient claim failure that did not apply', async () => {
      downstreamRuns = 0;
      const { mastra, run, armClaim, durable } = await suspendedRun();
      try {
        armClaim(terminateBeforeCommit);

        const error = await rejection(run.resume({ step: 'approval', resumeData: { approved: true } }));

        expect(getStoragePersistenceFailure(error)).toBe('transient');
        expect(await durable()).toMatchObject({ status: 'suspended', lifecycleResumeAttempt: 0 });
        expect((await run.resume({ step: 'approval', resumeData: { approved: true } })).status).toBe('success');
        expect(downstreamRuns).toBe(1);
      } finally {
        await mastra.shutdown();
      }
    });
  });
});
