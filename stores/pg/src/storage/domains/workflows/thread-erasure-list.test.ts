import { randomUUID } from 'node:crypto';
import { TABLE_WORKFLOW_SNAPSHOT, createEmptyWorkflowSnapshot } from '@mastra/core/storage';
import type { WorkflowRunState } from '@mastra/core/workflows';
import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { PostgresStore, WorkflowsPG } from '../../index';
import { connectionString } from '../../test-utils';

const THREAD = 'thread-target';
const OTHER_THREAD = 'thread-other';

type Run = { workflowName: string; runId: string };

const suspendedLoopStep = (threadId: string, resourceId: string) => ({
  status: 'suspended',
  payload: {},
  suspendPayload: {
    __streamState: { messageList: { messages: [], memoryInfo: { threadId, resourceId } } },
  },
  startedAt: 1,
  suspendedAt: 2,
});

/** The snapshot shapes the agentic and durable loops persist; see the in-memory test in @mastra/core. */
const fixtures = (resourceId: string) => ({
  createRunPending: (runId: string): WorkflowRunState => ({
    ...createEmptyWorkflowSnapshot(runId),
    status: 'pending',
    context: {} as WorkflowRunState['context'],
  }),
  nestedCreateRunPending: (runId: string): WorkflowRunState => ({
    ...createEmptyWorkflowSnapshot(runId),
    status: 'pending',
    context: { input: { messages: { all: [{ role: 'user', content: 'hello' }] } } } as any,
  }),
  loopStart: (runId: string, status: WorkflowRunState['status'], threadId: string): WorkflowRunState => ({
    ...createEmptyWorkflowSnapshot(runId),
    status,
    context: {
      input: { messageId: 'm1', messages: { all: [], user: [], nonUser: [] }, output: { steps: [] } },
    } as any,
    requestContext: { MastraMemory: { thread: { id: threadId, resourceId }, resourceId } },
  }),
  reservedThreadKey: (runId: string, threadId: string): WorkflowRunState => ({
    ...createEmptyWorkflowSnapshot(runId),
    status: 'running',
    context: {} as WorkflowRunState['context'],
    requestContext: { mastra__threadId: threadId },
  }),
  splitSuspended: (runId: string): WorkflowRunState => ({
    ...createEmptyWorkflowSnapshot(runId),
    status: 'suspended',
    context: {
      input: {},
      'tool-a': suspendedLoopStep(OTHER_THREAD, resourceId),
      'tool-b': suspendedLoopStep(THREAD, resourceId),
    } as any,
  }),
  durableInput: (runId: string, form: 'memoryInfo' | 'state'): WorkflowRunState => ({
    ...createEmptyWorkflowSnapshot(runId),
    status: 'running',
    context: {
      input: {
        __workflowKind: 'durable-agent',
        runId,
        agentId: 'agent-1',
        messageListState:
          form === 'memoryInfo' ? { memoryInfo: { threadId: THREAD, resourceId } } : { memoryInfo: null },
        state: form === 'state' ? { threadId: THREAD, resourceId } : {},
      },
    } as any,
  }),
  otherThread: (runId: string): WorkflowRunState => ({
    ...createEmptyWorkflowSnapshot(runId),
    status: 'success',
    context: { input: {}, 'tool-a': suspendedLoopStep(OTHER_THREAD, resourceId) } as any,
  }),
});

describe('WorkflowsPG.listWorkflowRunsByThreadForErasure', () => {
  let store: PostgresStore;
  let workflows: WorkflowsPG;

  beforeAll(async () => {
    store = new PostgresStore({ id: `workflow-thread-erasure-${randomUUID()}`, connectionString });
    await store.init();
    workflows = (await store.getStore('workflows'))!;
  }, 60000);

  afterAll(async () => {
    await store?.close();
  });

  const newResource = () => `workspace-${randomUUID()}:user-1`;
  const persist = (resourceId: string, run: Run, snapshot: WorkflowRunState) =>
    workflows.persistWorkflowSnapshot({ ...run, resourceId, snapshot });

  it('advertises the capability', () => {
    expect(workflows.getWorkflowSnapshotHandoffCapabilities().threadErasureListVersion).toBe(1);
  });

  it('lists every snapshot that records the thread, in any status and any place', async () => {
    const resource = newResource();
    const otherResource = newResource();
    const snapshots = fixtures(resource);
    const runs = {
      pendingLoopStart: { workflowName: 'agentic-loop', runId: 'a-pending' },
      runningLoopStart: { workflowName: 'agentic-loop', runId: 'b-running' },
      reservedKey: { workflowName: 'agentic-loop', runId: 'c-reserved' },
      split: { workflowName: 'agentic-loop', runId: 'd-split' },
      durableMemoryInfo: { workflowName: 'durable-agentic-loop', runId: 'e-memory-info' },
      durableState: { workflowName: 'durable-agentic-loop', runId: 'f-state' },
    };
    await persist(resource, runs.pendingLoopStart, snapshots.loopStart(runs.pendingLoopStart.runId, 'pending', THREAD));
    await persist(resource, runs.runningLoopStart, snapshots.loopStart(runs.runningLoopStart.runId, 'running', THREAD));
    await persist(resource, runs.reservedKey, snapshots.reservedThreadKey(runs.reservedKey.runId, THREAD));
    await persist(resource, runs.split, snapshots.splitSuspended(runs.split.runId));
    await persist(resource, runs.durableMemoryInfo, snapshots.durableInput(runs.durableMemoryInfo.runId, 'memoryInfo'));
    await persist(resource, runs.durableState, snapshots.durableInput(runs.durableState.runId, 'state'));
    await persist(resource, { workflowName: 'agentic-loop', runId: 'g-other' }, snapshots.otherThread('g-other'));
    await persist(
      otherResource,
      { workflowName: 'agentic-loop', runId: 'h-cross' },
      snapshots.loopStart('h-cross', 'running', THREAD),
    );
    await persist(resource, { workflowName: 'agentic-loop', runId: 'i-bare' }, snapshots.createRunPending('i-bare'));

    await expect(
      workflows.listWorkflowRunsByThreadForErasure({ resourceId: resource, threadId: THREAD, limit: 100 }),
    ).resolves.toEqual({
      status: 'listed',
      runs: [
        { ...runs.pendingLoopStart, attribution: 'thread' },
        { ...runs.runningLoopStart, attribution: 'thread' },
        { ...runs.reservedKey, attribution: 'thread' },
        { ...runs.split, attribution: 'thread' },
        { ...runs.durableMemoryInfo, attribution: 'thread' },
        { ...runs.durableState, attribution: 'thread' },
      ],
      hasMore: false,
    });
    await expect(
      workflows.listWorkflowRunsByThreadForErasure({ resourceId: otherResource, threadId: THREAD, limit: 100 }),
    ).resolves.toEqual({
      status: 'listed',
      runs: [{ workflowName: 'agentic-loop', runId: 'h-cross', attribution: 'thread' }],
      hasMore: false,
    });

    // The scalar thread filter of listWorkflowRuns (unchanged) misses the loop
    // start snapshots and the split run; this listing is the erasure answer.
    const { runs: scalarRuns } = await workflows.listWorkflowRuns({ resourceId: resource, threadId: THREAD });
    expect(scalarRuns.map(run => run.runId).sort()).toEqual([runs.durableMemoryInfo.runId]);
  });

  it('reports unattributed snapshots with status, store timestamps and contentFree only on request', async () => {
    const resource = newResource();
    const snapshots = fixtures(resource);
    const createdAt = new Date('2026-10-01T00:00:00.000Z');
    const updatedAt = new Date('2026-10-01T00:05:00.000Z');
    await workflows.persistWorkflowSnapshot({
      workflowName: 'agentic-loop',
      runId: 'a-bare',
      resourceId: resource,
      snapshot: snapshots.createRunPending('a-bare'),
      createdAt,
      updatedAt,
    });
    await persist(
      resource,
      { workflowName: 'agentic-loop', runId: 'b-nested' },
      snapshots.nestedCreateRunPending('b-nested'),
    );
    await persist(
      resource,
      { workflowName: 'agentic-loop', runId: 'c-thread' },
      snapshots.loopStart('c-thread', 'running', THREAD),
    );
    await persist(resource, { workflowName: 'agentic-loop', runId: 'd-other' }, snapshots.otherThread('d-other'));
    await persist(
      resource,
      { workflowName: 'agentic-loop', runId: 'e-context' },
      {
        ...snapshots.createRunPending('e-context'),
        requestContext: { locale: 'en' },
      },
    );
    await persist(
      newResource(),
      { workflowName: 'agentic-loop', runId: 'f-cross' },
      snapshots.createRunPending('f-cross'),
    );

    const result = await workflows.listWorkflowRunsByThreadForErasure({
      resourceId: resource,
      threadId: THREAD,
      limit: 100,
      includeUnattributed: true,
    });
    expect(result.status).toBe('listed');
    if (result.status !== 'listed') return;
    expect(result.runs.map(run => [run.runId, run.attribution])).toEqual([
      ['a-bare', 'unattributed'],
      ['b-nested', 'unattributed'],
      ['c-thread', 'thread'],
      ['e-context', 'unattributed'],
    ]);
    expect(result.runs[0]).toMatchObject({ attribution: 'unattributed', status: 'pending', contentFree: true });
    const bare = result.runs[0] as { createdAt: Date; updatedAt: Date };
    expect(bare.createdAt.getTime()).toBe(createdAt.getTime());
    expect(bare.updatedAt.getTime()).toBe(updatedAt.getTime());
    expect(result.runs[1]).toMatchObject({ status: 'pending', contentFree: false });
    expect(result.runs[3]).toMatchObject({ status: 'pending', contentFree: false });
  });

  it('pages by (workflowName, runId) in code point order', async () => {
    const resource = newResource();
    const snapshots = fixtures(resource);
    for (const runId of ['b', 'a', 'Z', 'c', 'é']) {
      await persist(resource, { workflowName: 'agentic-loop', runId }, snapshots.loopStart(runId, 'running', THREAD));
    }
    await persist(resource, { workflowName: 'agentic-loop', runId: 'b2' }, snapshots.otherThread('b2'));

    const seen: string[] = [];
    let after: Run | undefined;
    let pages = 0;
    for (;;) {
      const page = await workflows.listWorkflowRunsByThreadForErasure({
        resourceId: resource,
        threadId: THREAD,
        limit: 2,
        ...(after ? { after } : {}),
      });
      if (page.status !== 'listed') throw new Error('unsupported');
      pages += 1;
      seen.push(...page.runs.map(run => run.runId));
      if (!page.hasMore) {
        expect(page.nextCursor).toBeUndefined();
        break;
      }
      after = page.nextCursor;
    }
    expect(pages).toBe(3);
    expect(seen).toEqual(['Z', 'a', 'b', 'c', 'é']);
  });

  it('binds the thread id as a jsonpath variable', async () => {
    const resource = newResource();
    const quoted = `thread "quoted" \\ $t || true`;
    await persist(
      resource,
      { workflowName: 'agentic-loop', runId: 'a' },
      fixtures(resource).loopStart('a', 'running', quoted),
    );
    await persist(
      resource,
      { workflowName: 'agentic-loop', runId: 'b' },
      fixtures(resource).loopStart('b', 'running', THREAD),
    );
    await expect(
      workflows.listWorkflowRunsByThreadForErasure({ resourceId: resource, threadId: quoted, limit: 10 }),
    ).resolves.toMatchObject({ runs: [{ runId: 'a', attribution: 'thread' }], hasMore: false });
  });
});

describe('WorkflowsPG.listWorkflowRunsByThreadForErasure (legacy text snapshot column)', () => {
  const testSchema = `thread_erasure_legacy_${Date.now()}_${Math.random().toString(16).slice(2)}`;
  const resource = 'workspace-legacy:user-1';
  let adminPool: Pool;
  let legacyStore: PostgresStore;
  let workflows: WorkflowsPG;

  beforeAll(async () => {
    adminPool = new Pool({ connectionString });
    const snapshots = fixtures(resource);
    const client = await adminPool.connect();
    try {
      await client.query(`CREATE SCHEMA ${testSchema}`);
      await client.query(`
        CREATE TABLE ${testSchema}.${TABLE_WORKFLOW_SNAPSHOT} (
          "workflow_name" TEXT NOT NULL,
          "run_id" TEXT NOT NULL,
          "resourceId" TEXT,
          "snapshot" TEXT NOT NULL,
          "createdAt" TIMESTAMP NOT NULL DEFAULT NOW(),
          "updatedAt" TIMESTAMP NOT NULL DEFAULT NOW(),
          PRIMARY KEY ("workflow_name", "run_id")
        )
      `);
      const rows: Array<[string, WorkflowRunState | string]> = [
        ['a-split', snapshots.splitSuspended('a-split')],
        ['b-bare', snapshots.createRunPending('b-bare')],
        ['c-other', snapshots.otherThread('c-other')],
        // A legacy row holding an escape jsonb rejects; the read sanitizes it.
        [
          'd-escape',
          JSON.stringify(snapshots.loopStart('d-escape', 'running', THREAD)).replace(
            '"messageId":"m1"',
            '"messageId":"m1\\u0000"',
          ),
        ],
      ];
      for (const [runId, snapshot] of rows) {
        await client.query(
          `INSERT INTO ${testSchema}.${TABLE_WORKFLOW_SNAPSHOT} ("workflow_name", "run_id", "resourceId", "snapshot")
           VALUES ($1, $2, $3, $4)`,
          ['agentic-loop', runId, resource, typeof snapshot === 'string' ? snapshot : JSON.stringify(snapshot)],
        );
      }
    } finally {
      client.release();
    }
    legacyStore = new PostgresStore({
      id: `workflow-thread-erasure-legacy-${randomUUID()}`,
      connectionString,
      schemaName: testSchema,
    });
    await legacyStore.init();
    workflows = (await legacyStore.getStore('workflows'))!;
  }, 60000);

  afterAll(async () => {
    await legacyStore?.close();
    const client = await adminPool.connect();
    try {
      await client.query(`DROP SCHEMA IF EXISTS ${testSchema} CASCADE`);
    } finally {
      client.release();
      await adminPool.end();
    }
  }, 30000);

  it('matches through the sanitized jsonb read', async () => {
    const result = await workflows.listWorkflowRunsByThreadForErasure({
      resourceId: resource,
      threadId: THREAD,
      limit: 10,
      includeUnattributed: true,
    });
    expect(result.status).toBe('listed');
    if (result.status !== 'listed') return;
    expect(result.runs.map(run => [run.runId, run.attribution])).toEqual([
      ['a-split', 'thread'],
      ['b-bare', 'unattributed'],
      ['d-escape', 'thread'],
    ]);
    expect(result.runs[1]).toMatchObject({ status: 'pending', contentFree: true });
  });
});
