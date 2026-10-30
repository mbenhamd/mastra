import { randomUUID } from 'node:crypto';
import { TABLE_WORKFLOW_SNAPSHOT, createEmptyWorkflowSnapshot } from '@mastra/core/storage';
import type { WorkflowRunState } from '@mastra/core/workflows';
import { Pool } from 'pg';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import type { WorkflowsPG } from '../../index';
import { PostgresStore } from '../../index';
import { connectionString } from '../../test-utils';

const THREAD = 'thread-target';
const SNAPSHOT_ONLY = { snapshot: 'thread', handoff: 'absent', terminalSnapshot: 'absent' };
const UNATTRIBUTED_SNAPSHOT_ONLY = { snapshot: 'unattributed', handoff: 'absent', terminalSnapshot: 'absent' };
const OTHER_THREAD = 'thread-other';

type Run = { workflowName: string; runId: string };

// Every test uses its own workflow names, so fixed run ids never collide with
// rows another test (or an earlier run against the same database) left behind.
const uniqueWorkflowNames = () => {
  const suffix = randomUUID();
  return { loop: `agentic-loop-${suffix}`, durable: `durable-agentic-loop-${suffix}` };
};
let { loop: LOOP, durable: DURABLE } = uniqueWorkflowNames();

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

  beforeEach(() => {
    ({ loop: LOOP, durable: DURABLE } = uniqueWorkflowNames());
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
      pendingLoopStart: { workflowName: LOOP, runId: 'a-pending' },
      runningLoopStart: { workflowName: LOOP, runId: 'b-running' },
      reservedKey: { workflowName: LOOP, runId: 'c-reserved' },
      split: { workflowName: LOOP, runId: 'd-split' },
      durableMemoryInfo: { workflowName: DURABLE, runId: 'e-memory-info' },
      durableState: { workflowName: DURABLE, runId: 'f-state' },
    };
    await persist(resource, runs.pendingLoopStart, snapshots.loopStart(runs.pendingLoopStart.runId, 'pending', THREAD));
    await persist(resource, runs.runningLoopStart, snapshots.loopStart(runs.runningLoopStart.runId, 'running', THREAD));
    await persist(resource, runs.reservedKey, snapshots.reservedThreadKey(runs.reservedKey.runId, THREAD));
    await persist(resource, runs.split, snapshots.splitSuspended(runs.split.runId));
    await persist(resource, runs.durableMemoryInfo, snapshots.durableInput(runs.durableMemoryInfo.runId, 'memoryInfo'));
    await persist(resource, runs.durableState, snapshots.durableInput(runs.durableState.runId, 'state'));
    await persist(resource, { workflowName: LOOP, runId: 'g-other' }, snapshots.otherThread('g-other'));
    // Application data shaped like thread metadata, in another thread's run, attributes nothing.
    await persist(resource, { workflowName: LOOP, runId: 'g2-app-data' }, {
      ...snapshots.otherThread('g2-app-data'),
      context: {
        input: {},
        'tool-a': { status: 'success', output: { memoryInfo: { threadId: THREAD }, state: { threadId: THREAD } } },
        'tool-b': snapshots.otherThread('x').context['tool-a'],
      },
    } as WorkflowRunState);
    await persist(
      otherResource,
      { workflowName: LOOP, runId: 'h-cross' },
      snapshots.loopStart('h-cross', 'running', THREAD),
    );
    await persist(resource, { workflowName: LOOP, runId: 'i-bare' }, snapshots.createRunPending('i-bare'));

    await expect(
      workflows.listWorkflowRunsByThreadForErasure({ resourceId: resource, threadId: THREAD, limit: 100 }),
    ).resolves.toMatchObject({
      status: 'listed',
      runs: [
        { ...runs.pendingLoopStart, attribution: 'thread', rows: SNAPSHOT_ONLY },
        { ...runs.runningLoopStart, attribution: 'thread', rows: SNAPSHOT_ONLY },
        { ...runs.reservedKey, attribution: 'thread', rows: SNAPSHOT_ONLY },
        // The first suspended step names another thread: the row records both.
        { ...runs.split, attribution: 'thread', rows: { ...SNAPSHOT_ONLY, snapshot: 'mixed' } },
        { ...runs.durableMemoryInfo, attribution: 'thread', rows: SNAPSHOT_ONLY },
        { ...runs.durableState, attribution: 'thread', rows: SNAPSHOT_ONLY },
      ],
      hasMore: false,
    });
    await expect(
      workflows.listWorkflowRunsByThreadForErasure({ resourceId: otherResource, threadId: THREAD, limit: 100 }),
    ).resolves.toMatchObject({
      status: 'listed',
      runs: [{ workflowName: LOOP, runId: 'h-cross', attribution: 'thread', rows: SNAPSHOT_ONLY }],
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
      workflowName: LOOP,
      runId: 'a-bare',
      resourceId: resource,
      snapshot: snapshots.createRunPending('a-bare'),
      createdAt,
      updatedAt,
    });
    await persist(resource, { workflowName: LOOP, runId: 'b-nested' }, snapshots.nestedCreateRunPending('b-nested'));
    await persist(
      resource,
      { workflowName: LOOP, runId: 'c-thread' },
      snapshots.loopStart('c-thread', 'running', THREAD),
    );
    await persist(resource, { workflowName: LOOP, runId: 'd-other' }, snapshots.otherThread('d-other'));
    await persist(
      resource,
      { workflowName: LOOP, runId: 'e-context' },
      {
        ...snapshots.createRunPending('e-context'),
        requestContext: { locale: 'en' },
      },
    );
    await persist(newResource(), { workflowName: LOOP, runId: 'f-cross' }, snapshots.createRunPending('f-cross'));

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

  it('finds the thread in a retained handoff snapshot and a terminal envelope', async () => {
    const resource = newResource();
    const snapshots = fixtures(resource);
    const released = { workflowName: LOOP, runId: 'a-released' };
    await persist(resource, released, {
      ...snapshots.loopStart(released.runId, 'running', THREAD),
      status: 'success',
    } as WorkflowRunState);
    const canonical = (await workflows.loadWorkflowSnapshot(released))!;
    const owner = { ...released, resourceId: resource, mutationFence: `fence-${randomUUID()}` };
    await expect(
      workflows.claimWorkflowSnapshotHandoff({
        ...owner,
        expectedCanonical: { kind: 'present', resourceId: resource, snapshot: canonical },
        snapshot: canonical,
      }),
    ).resolves.toMatchObject({ status: 'created' });
    await expect(
      workflows.completeWorkflowSnapshotHandoff({
        ...owner,
        expectedResourceId: resource,
        expectedSnapshot: canonical,
        snapshot: canonical,
      }),
    ).resolves.toMatchObject({ status: 'completed' });
    await expect(
      workflows.releaseWorkflowSnapshotHandoff({ ...released, expectedClaimGeneration: 1, deleteRun: true }),
    ).resolves.toMatchObject({ status: 'released' });
    await expect(workflows.loadWorkflowSnapshot(released)).resolves.toBeNull();
    // The released handoff no longer fences the run id: a newer lifetime of
    // another thread reuses it, and its canonical snapshot lists as `other`.
    await persist(resource, released, {
      ...snapshots.loopStart(released.runId, 'running', OTHER_THREAD),
      status: 'success',
    } as WorkflowRunState);

    // Terminal recovery envelopes, inserted as rows (the listing only reads them).
    const terminal = (runId: string, envelope: Record<string, unknown>) =>
      store.db.none(
        `INSERT INTO mastra_workflow_terminal_snapshots_v2
           (workflow_name, run_id, version, resource_id, terminal_status, envelope_hash, record_hash, envelope, created_at)
         VALUES ($1, $2, 1, $3, 'success', $4, $4, $5::jsonb, 1)`,
        [LOOP, runId, resource, `sha256:${'0'.repeat(64)}`, JSON.stringify(envelope)],
      );
    // An envelope's requestContextPatch cannot carry a thread (the envelope
    // materializer refuses infrastructure keys): this row attributes nothing.
    await terminal('b-envelope', {
      terminalResult: {},
      finalState: {},
      requestContextPatch: { mastra__threadId: THREAD },
    });
    // A durable run without a memory backend records its thread only in its SerializableDurableState.
    await terminal('b2-durable-state', {
      workflowName: 'durable-agentic-loop',
      terminalResult: {
        status: 'success',
        output: { messageListState: { memoryInfo: null }, state: { threadId: THREAD } },
      },
      finalState: {},
      requestContextPatch: {},
    });
    await persist(
      resource,
      { workflowName: LOOP, runId: 'c-bare-terminal' },
      snapshots.createRunPending('c-bare-terminal'),
    );
    await terminal('c-bare-terminal', { terminalResult: { text: 'result' }, finalState: {}, requestContextPatch: {} });
    await persist(resource, { workflowName: LOOP, runId: 'd-bare-other' }, snapshots.createRunPending('d-bare-other'));
    await terminal('d-bare-other', {
      workflowName: 'durable-agentic-loop',
      terminalResult: {
        status: 'success',
        output: { messageListState: { memoryInfo: { threadId: OTHER_THREAD } }, state: {} },
      },
      finalState: {},
      requestContextPatch: {},
    });

    await expect(
      workflows.listWorkflowRunsByThreadForErasure({
        resourceId: resource,
        threadId: THREAD,
        limit: 10,
        includeUnattributed: true,
      }),
    ).resolves.toMatchObject({
      status: 'listed',
      runs: [
        {
          ...released,
          attribution: 'thread',
          rows: { snapshot: 'other', handoff: 'thread', terminalSnapshot: 'absent' },
        },
        {
          workflowName: LOOP,
          runId: 'b2-durable-state',
          attribution: 'thread',
          rows: { snapshot: 'absent', handoff: 'absent', terminalSnapshot: 'thread' },
        },
        {
          workflowName: LOOP,
          runId: 'c-bare-terminal',
          attribution: 'unattributed',
          rows: { snapshot: 'unattributed', handoff: 'absent', terminalSnapshot: 'unattributed' },
          status: 'pending',
          contentFree: false,
        },
      ],
      hasMore: false,
    });
  });

  it('finds suspensions engines keep inside a step suspend payload or foreach results', async () => {
    const resource = newResource();
    const suspended = (threadId: string) => ({
      status: 'suspended',
      suspendPayload: { __streamState: { messageList: { memoryInfo: { threadId } } } },
    });
    const withStep = (runId: string, step: Record<string, unknown>) =>
      ({
        ...createEmptyWorkflowSnapshot(runId),
        status: 'suspended',
        context: { input: {}, tools: step },
      }) as WorkflowRunState;
    // The default engine keeps foreach iterations as an array.
    await persist(
      resource,
      { workflowName: LOOP, runId: 'a-foreach-array' },
      withStep('a-foreach-array', {
        status: 'suspended',
        suspendPayload: { __workflow_meta: { foreachOutput: [{ status: 'success' }, suspended(THREAD)] } },
      }),
    );
    // The evented engine keys them by index; the first suspended iteration names another thread.
    await persist(
      resource,
      { workflowName: LOOP, runId: 'b-foreach-object' },
      withStep('b-foreach-object', {
        status: 'suspended',
        suspendPayload: {
          __workflow_meta: { foreachOutput: { '0': suspended(OTHER_THREAD), '1': suspended(THREAD) } },
        },
      }),
    );
    // A foreach step's iteration results.
    await persist(
      resource,
      { workflowName: LOOP, runId: 'c-foreach-output' },
      withStep('c-foreach-output', {
        status: 'suspended',
        output: [null, suspended(THREAD)],
      }),
    );
    // The caller-owned workflow input is never read as a step.
    await persist(resource, { workflowName: LOOP, runId: 'e-input-shaped' }, {
      ...createEmptyWorkflowSnapshot('e-input-shaped'),
      status: 'suspended',
      context: { input: suspended(THREAD) },
    } as WorkflowRunState);
    // Another thread only.
    await persist(
      resource,
      { workflowName: LOOP, runId: 'd-foreach-other' },
      withStep('d-foreach-other', {
        status: 'suspended',
        suspendPayload: { __workflow_meta: { foreachOutput: [suspended(OTHER_THREAD)] } },
      }),
    );

    await expect(
      workflows.listWorkflowRunsByThreadForErasure({ resourceId: resource, threadId: THREAD, limit: 10 }),
    ).resolves.toMatchObject({
      status: 'listed',
      runs: ['a-foreach-array', 'b-foreach-object', 'c-foreach-output'].map(runId => ({
        workflowName: LOOP,
        runId,
        attribution: 'thread',
        // b-foreach-object also keeps another thread's iteration.
        rows: runId === 'b-foreach-object' ? { ...SNAPSHOT_ONLY, snapshot: 'mixed' } : SNAPSHOT_ONLY,
      })),
      hasMore: false,
    });
  });

  it('reads network inputs and durable step payloads, only at their framework locations', async () => {
    const resource = newResource();
    const suspendedWithInput = (runId: string, input: Record<string, unknown>) =>
      ({ ...createEmptyWorkflowSnapshot(runId), status: 'suspended', context: { input } }) as WorkflowRunState;
    // The agent network records the thread in its workflow input.
    await persist(
      resource,
      { workflowName: 'agent-loop-main-workflow', runId: 'a-network' },
      suspendedWithInput('a-network', {
        task: 'research',
        threadId: THREAD,
        threadResourceId: resource,
      }),
    );
    // The same input shape in any other workflow is application data.
    await persist(
      resource,
      { workflowName: LOOP, runId: 'b-not-network' },
      suspendedWithInput('b-not-network', {
        threadId: THREAD,
      }),
    );
    // A durable run's running step keeps its input as `payload`.
    await persist(resource, { workflowName: 'durable-agentic-loop', runId: 'c-durable-payload' }, {
      ...createEmptyWorkflowSnapshot('c-durable-payload'),
      status: 'running',
      context: {
        input: { __workflowKind: 'durable-agent', messageListState: { memoryInfo: null }, state: {} },
        'llm-execution': {
          status: 'running',
          payload: { messageListState: { memoryInfo: null }, state: { threadId: THREAD } },
        },
      },
    } as WorkflowRunState);

    const result = await workflows.listWorkflowRunsByThreadForErasure({
      resourceId: resource,
      threadId: THREAD,
      limit: 10,
      includeUnattributed: true,
    });
    expect(
      result.status === 'listed' && result.runs.map(run => [run.workflowName, run.runId, run.attribution]),
    ).toEqual([
      ['agent-loop-main-workflow', 'a-network', 'thread'],
      [LOOP, 'b-not-network', 'unattributed'],
      ['durable-agentic-loop', 'c-durable-payload', 'thread'],
    ]);
  });

  it('counts only the createRun shell as content-free, and only object snapshots', async () => {
    const resource = newResource();
    const shell = (runId: string) => ({ ...createEmptyWorkflowSnapshot(runId), status: 'pending' }) as WorkflowRunState;
    await persist(resource, { workflowName: LOOP, runId: 'a-shell' }, shell('a-shell'));
    await persist(resource, { workflowName: LOOP, runId: 'b-result' }, {
      ...shell('b-result'),
      result: { text: 'secret' },
    } as WorkflowRunState);
    await persist(resource, { workflowName: LOOP, runId: 'c-value' }, {
      ...shell('c-value'),
      value: { draft: 'secret' },
    } as WorkflowRunState);
    await persist(resource, { workflowName: LOOP, runId: 'd-extra-key' }, {
      ...shell('d-extra-key'),
      lifecycleOutbox: [{ kind: 'event' }],
    } as unknown as WorkflowRunState);
    // Rows stored as a JSON string and as an array, as legacy writers could leave them.
    const raw = (runId: string, snapshot: unknown) =>
      store.db.none(
        `INSERT INTO mastra_workflow_snapshot (workflow_name, run_id, "resourceId", snapshot, "createdAt", "updatedAt")
         VALUES ($1, $2, $3, $4::jsonb, NOW(), NOW())`,
        [LOOP, runId, resource, JSON.stringify(snapshot)],
      );
    await raw(
      'e-string-root',
      JSON.stringify({
        status: 'pending',
        context: { input: { messages: ['secret'] } },
        requestContext: { mastra__threadId: THREAD },
      }),
    );
    await raw('f-array-root', [{ status: 'pending' }]);

    const result = await workflows.listWorkflowRunsByThreadForErasure({
      resourceId: resource,
      threadId: THREAD,
      limit: 10,
      includeUnattributed: true,
    });
    expect(result.status).toBe('listed');
    if (result.status !== 'listed') return;
    expect(
      result.runs.map(run => [
        run.runId,
        run.attribution,
        run.attribution === 'unattributed' ? run.contentFree : null,
        run.attribution === 'unattributed' ? (run.status ?? null) : null,
      ]),
    ).toEqual([
      ['a-shell', 'unattributed', true, 'pending'],
      ['b-result', 'unattributed', false, 'pending'],
      ['c-value', 'unattributed', false, 'pending'],
      ['d-extra-key', 'unattributed', false, 'pending'],
      // A snapshot stored as a JSON string is read as the object it encodes.
      ['e-string-root', 'thread', null, null],
      // A non-object root is never content-free and has no status.
      ['f-array-root', 'unattributed', false, null],
    ]);
  });

  it('marks the framework loop rows of a crashed turn and the run ids they share', async () => {
    const resource = newResource();
    const turn = `turn-${randomUUID()}`;
    const userTurn = `turn-${randomUUID()}`;
    // A crashed non-durable turn: the agentic-loop parent's createRun shell and
    // its nested executionWorkflow child under the same run id, whose input is
    // the pruned output of the previous iteration (no thread id anywhere).
    const priorIteration = {
      messageId: 'm1',
      messages: { all: [], user: [], nonUser: [] },
      output: {
        text: 'Here is the summary you asked for.',
        toolResults: [{ toolCallId: 'call-1', toolName: 'search', result: { hits: 3 } }],
        steps: [],
        usage: { inputTokens: 10, outputTokens: 5, totalTokens: 15 },
      },
      metadata: {},
      stepResult: { reason: 'tool-calls', warnings: [], isContinued: true },
    };
    const child = (runId: string) =>
      ({
        ...createEmptyWorkflowSnapshot(runId),
        status: 'pending',
        context: { input: priorIteration },
      }) as WorkflowRunState;
    await persist(resource, { workflowName: 'agentic-loop', runId: turn }, createEmptyWorkflowSnapshot(turn));
    await persist(resource, { workflowName: 'executionWorkflow', runId: turn }, child(turn));
    // A user workflow with the same shape is not loop-internal.
    await persist(resource, { workflowName: 'research-workflow', runId: userTurn }, child(userTurn));

    const result = await workflows.listWorkflowRunsByThreadForErasure({
      resourceId: resource,
      threadId: THREAD,
      limit: 10,
      includeUnattributed: true,
    });
    expect(result.status).toBe('listed');
    if (result.status !== 'listed') return;
    expect(
      result.runs.map(run => ({
        workflowName: run.workflowName,
        attribution: run.attribution,
        loopInternal: run.loopInternal,
        sharesRunIdWith: run.sharesRunIdWith,
        contentFree: run.attribution === 'unattributed' ? run.contentFree : undefined,
      })),
    ).toEqual([
      {
        workflowName: 'agentic-loop',
        attribution: 'unattributed',
        loopInternal: true,
        sharesRunIdWith: ['executionWorkflow'],
        contentFree: true,
      },
      {
        workflowName: 'executionWorkflow',
        attribution: 'unattributed',
        loopInternal: true,
        sharesRunIdWith: ['agentic-loop'],
        contentFree: false,
      },
      {
        workflowName: 'research-workflow',
        attribution: 'unattributed',
        loopInternal: false,
        sharesRunIdWith: [],
        contentFree: false,
      },
    ]);
  });

  it('pages by (workflowName, runId) in code point order', async () => {
    const resource = newResource();
    const snapshots = fixtures(resource);
    for (const runId of ['b', 'a', 'Z', 'c', 'é']) {
      await persist(resource, { workflowName: LOOP, runId }, snapshots.loopStart(runId, 'running', THREAD));
    }
    await persist(resource, { workflowName: LOOP, runId: 'b2' }, snapshots.otherThread('b2'));

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
    await persist(resource, { workflowName: LOOP, runId: 'a' }, fixtures(resource).loopStart('a', 'running', quoted));
    await persist(resource, { workflowName: LOOP, runId: 'b' }, fixtures(resource).loopStart('b', 'running', THREAD));
    await expect(
      workflows.listWorkflowRunsByThreadForErasure({ resourceId: resource, threadId: quoted, limit: 10 }),
    ).resolves.toMatchObject({ runs: [{ runId: 'a', attribution: 'thread', rows: SNAPSHOT_ONLY }], hasMore: false });
  });
});

describe.each(['TEXT', 'JSON'])(
  'WorkflowsPG.listWorkflowRunsByThreadForErasure (legacy %s snapshot column)',
  columnType => {
    const testSchema = `thread_erasure_legacy_${columnType.toLowerCase()}_${Date.now()}_${Math.random().toString(16).slice(2)}`;
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
          "snapshot" ${columnType} NOT NULL,
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
            [LOOP, runId, resource, typeof snapshot === 'string' ? snapshot : JSON.stringify(snapshot)],
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
      // The table has no timezone mirror columns: the timestamps come from "createdAt"/"updatedAt".
      const bare = result.runs[1] as { createdAt: Date; updatedAt: Date };
      expect(Number.isNaN(bare.createdAt.getTime())).toBe(false);
      expect(Number.isNaN(bare.updatedAt.getTime())).toBe(false);
    });
  },
);
