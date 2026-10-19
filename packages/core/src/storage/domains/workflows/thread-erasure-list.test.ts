import { describe, expect, it } from 'vitest';
import { DurableStepIds } from '../../../agent/durable/constants';
import { AGENTIC_EXECUTION_WORKFLOW_ID, AGENTIC_LOOP_WORKFLOW_ID } from '../../../loop/loop-builder';
import type { WorkflowRunState } from '../../../workflows';
import { createWorkflowTerminalGraphFingerprint } from '../../../workflows/terminal-continuation';
import { createEmptyWorkflowSnapshot } from '../../workflow-snapshot';
import { WORKFLOW_SNAPSHOT_THREAD_ERASURE_LOOP_WORKFLOWS } from '../../workflow-snapshot-handoff';
import { InMemoryDB } from '../inmemory-db';
import { WorkflowsInMemory } from './inmemory';

const RESOURCE = 'workspace-1:user-1';
const OTHER_RESOURCE = 'workspace-1:user-2';
const THREAD = 'thread-target';
const SNAPSHOT_ONLY = { snapshot: 'thread', handoff: 'absent', terminalSnapshot: 'absent' };
const UNATTRIBUTED_SNAPSHOT_ONLY = { snapshot: 'unattributed', handoff: 'absent', terminalSnapshot: 'absent' };
const OTHER_THREAD = 'thread-other';

function setup() {
  const db = new InMemoryDB();
  return { db, workflows: new WorkflowsInMemory({ db }) };
}

type Run = { workflowName: string; runId: string };

const suspendedLoopStep = (threadId: string) => ({
  status: 'suspended',
  payload: {},
  suspendPayload: {
    __streamState: { messageList: { messages: [], memoryInfo: { threadId, resourceId: RESOURCE } } },
  },
  startedAt: 1,
  suspendedAt: 2,
});

/**
 * Snapshot shapes the agentic and durable loops persist (see the PR for the
 * source of each). Every fixture is a full run state; only the fields that
 * carry thread ids differ.
 */
const snapshots = {
  /** `Workflow.createRun` before the run starts: `context: {}`, no request context, no thread anywhere. */
  createRunPending: (runId: string): WorkflowRunState => ({
    ...createEmptyWorkflowSnapshot(runId),
    status: 'pending',
    context: {} as WorkflowRunState['context'],
  }),
  /** A nested run's `createRun` snapshot: `context.input` holds the parent step's payload, no thread. */
  nestedCreateRunPending: (runId: string): WorkflowRunState => ({
    ...createEmptyWorkflowSnapshot(runId),
    status: 'pending',
    context: { input: { messages: { all: [{ role: 'user', content: 'hello' }] } } } as any,
  }),
  /** The agentic loop after its first persist: request context carries the memory thread; input carries none. */
  loopStart: (runId: string, status: WorkflowRunState['status'], threadId: string): WorkflowRunState => ({
    ...createEmptyWorkflowSnapshot(runId),
    status,
    context: {
      input: { messageId: 'm1', messages: { all: [], user: [], nonUser: [] }, output: { steps: [] } },
    } as any,
    requestContext: { MastraMemory: { thread: { id: threadId, resourceId: RESOURCE }, resourceId: RESOURCE } },
  }),
  /** A run that only carries the reserved request-context thread key. */
  reservedThreadKey: (runId: string, threadId: string): WorkflowRunState => ({
    ...createEmptyWorkflowSnapshot(runId),
    status: 'running',
    context: {} as WorkflowRunState['context'],
    requestContext: { mastra__threadId: threadId },
  }),
  /** First suspended step names another thread, a later step the target. */
  splitSuspended: (runId: string): WorkflowRunState => ({
    ...createEmptyWorkflowSnapshot(runId),
    status: 'suspended',
    context: {
      input: {},
      'tool-a': suspendedLoopStep(OTHER_THREAD),
      'tool-b': suspendedLoopStep(THREAD),
    } as any,
  }),
  /** The durable loop's input: `messageListState.memoryInfo` and `state.threadId`. */
  durableInput: (runId: string, form: 'memoryInfo' | 'state'): WorkflowRunState => ({
    ...createEmptyWorkflowSnapshot(runId),
    status: 'running',
    context: {
      input: {
        __workflowKind: 'durable-agent',
        runId,
        agentId: 'agent-1',
        messageListState:
          form === 'memoryInfo' ? { memoryInfo: { threadId: THREAD, resourceId: RESOURCE } } : { memoryInfo: null },
        state: form === 'state' ? { threadId: THREAD, resourceId: RESOURCE } : {},
      },
    } as any,
  }),
  /** A completed run that records only another thread. */
  otherThread: (runId: string): WorkflowRunState => ({
    ...createEmptyWorkflowSnapshot(runId),
    status: 'success',
    context: { input: {}, 'tool-a': suspendedLoopStep(OTHER_THREAD) } as any,
  }),
};

async function persist(workflows: WorkflowsInMemory, run: Run, snapshot: WorkflowRunState, resourceId = RESOURCE) {
  await workflows.persistWorkflowSnapshot({ ...run, resourceId, snapshot });
}

describe('WorkflowsInMemory.listWorkflowRunsByThreadForErasure', () => {
  it('names the loop workflows by their source constants', () => {
    expect([...WORKFLOW_SNAPSHOT_THREAD_ERASURE_LOOP_WORKFLOWS].sort()).toEqual(
      [
        AGENTIC_LOOP_WORKFLOW_ID,
        AGENTIC_EXECUTION_WORKFLOW_ID,
        DurableStepIds.AGENTIC_LOOP,
        DurableStepIds.AGENTIC_EXECUTION,
      ].sort(),
    );
  });

  it('advertises the capability', () => {
    const { workflows } = setup();
    expect(workflows.getWorkflowSnapshotHandoffCapabilities().threadErasureListVersion).toBe(1);
  });

  it('lists every snapshot that records the thread, in any status and any place', async () => {
    const { workflows } = setup();
    const runs = {
      pendingLoopStart: { workflowName: 'agentic-loop', runId: 'a-pending' },
      runningLoopStart: { workflowName: 'agentic-loop', runId: 'b-running' },
      reservedKey: { workflowName: 'agentic-loop', runId: 'c-reserved' },
      split: { workflowName: 'agentic-loop', runId: 'd-split' },
      durableMemoryInfo: { workflowName: 'durable-agentic-loop', runId: 'e-memory-info' },
      durableState: { workflowName: 'durable-agentic-loop', runId: 'f-state' },
    };
    await persist(
      workflows,
      runs.pendingLoopStart,
      snapshots.loopStart(runs.pendingLoopStart.runId, 'pending', THREAD),
    );
    await persist(
      workflows,
      runs.runningLoopStart,
      snapshots.loopStart(runs.runningLoopStart.runId, 'running', THREAD),
    );
    await persist(workflows, runs.reservedKey, snapshots.reservedThreadKey(runs.reservedKey.runId, THREAD));
    await persist(workflows, runs.split, snapshots.splitSuspended(runs.split.runId));
    await persist(
      workflows,
      runs.durableMemoryInfo,
      snapshots.durableInput(runs.durableMemoryInfo.runId, 'memoryInfo'),
    );
    await persist(workflows, runs.durableState, snapshots.durableInput(runs.durableState.runId, 'state'));
    // Not listed: another thread, the thread under another resource, and
    // (without includeUnattributed) a snapshot with no thread at all.
    await persist(workflows, { workflowName: 'agentic-loop', runId: 'g-other' }, snapshots.otherThread('g-other'));
    // Application data shaped like thread metadata, in another thread's run, attributes nothing.
    await persist(workflows, { workflowName: 'agentic-loop', runId: 'g2-app-data' }, {
      ...snapshots.otherThread('g2-app-data'),
      context: {
        input: {},
        'tool-a': { status: 'success', output: { memoryInfo: { threadId: THREAD }, state: { threadId: THREAD } } },
        'tool-b': snapshots.otherThread('x').context['tool-a'],
      },
    } as WorkflowRunState);
    await persist(
      workflows,
      { workflowName: 'agentic-loop', runId: 'h-cross' },
      snapshots.loopStart('h-cross', 'running', THREAD),
      OTHER_RESOURCE,
    );
    await persist(workflows, { workflowName: 'agentic-loop', runId: 'i-bare' }, snapshots.createRunPending('i-bare'));

    const result = await workflows.listWorkflowRunsByThreadForErasure({
      resourceId: RESOURCE,
      threadId: THREAD,
      limit: 100,
    });
    expect(result).toMatchObject({
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
    // The other resource's run is listed only under its own resource.
    await expect(
      workflows.listWorkflowRunsByThreadForErasure({ resourceId: OTHER_RESOURCE, threadId: THREAD, limit: 100 }),
    ).resolves.toMatchObject({
      status: 'listed',
      runs: [{ workflowName: 'agentic-loop', runId: 'h-cross', attribution: 'thread', rows: SNAPSHOT_ONLY }],
      hasMore: false,
    });
  });

  it('reports unattributed snapshots with status, store timestamps and contentFree only on request', async () => {
    const { workflows } = setup();
    const createdAt = new Date('2026-10-01T00:00:00.000Z');
    const updatedAt = new Date('2026-10-01T00:05:00.000Z');
    await workflows.persistWorkflowSnapshot({
      workflowName: 'agentic-loop',
      runId: 'a-bare',
      resourceId: RESOURCE,
      snapshot: snapshots.createRunPending('a-bare'),
      createdAt,
      updatedAt,
    });
    await persist(
      workflows,
      { workflowName: 'agentic-loop', runId: 'b-nested' },
      snapshots.nestedCreateRunPending('b-nested'),
    );
    await persist(
      workflows,
      { workflowName: 'agentic-loop', runId: 'c-thread' },
      snapshots.loopStart('c-thread', 'running', THREAD),
    );
    await persist(workflows, { workflowName: 'agentic-loop', runId: 'd-other' }, snapshots.otherThread('d-other'));
    // Request context without a thread: not content-free.
    await persist(
      workflows,
      { workflowName: 'agentic-loop', runId: 'e-context' },
      {
        ...snapshots.createRunPending('e-context'),
        requestContext: { locale: 'en' },
      },
    );
    // A class instance with no own fields still serializes to content.
    await persist(
      workflows,
      { workflowName: 'agentic-loop', runId: 'e2-date' },
      {
        ...snapshots.createRunPending('e2-date'),
        context: { input: new Date('2026-10-01T00:00:00.000Z') } as any,
      },
    );
    await persist(
      workflows,
      { workflowName: 'agentic-loop', runId: 'f-cross' },
      snapshots.createRunPending('f-cross'),
      OTHER_RESOURCE,
    );

    const result = await workflows.listWorkflowRunsByThreadForErasure({
      resourceId: RESOURCE,
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
      ['e2-date', 'unattributed'],
    ]);
    expect(result.runs[0]).toMatchObject({
      workflowName: 'agentic-loop',
      runId: 'a-bare',
      attribution: 'unattributed',
      rows: UNATTRIBUTED_SNAPSHOT_ONLY,
      status: 'pending',
      createdAt,
      updatedAt,
      contentFree: true,
    });
    expect(result.runs[1]).toMatchObject({ status: 'pending', contentFree: false });
    expect(result.runs[3]).toMatchObject({ status: 'pending', contentFree: false });
    expect(result.runs[4]).toMatchObject({ status: 'pending', contentFree: false });
  });

  it('finds the thread in a retained handoff snapshot and a terminal envelope', async () => {
    const { db, workflows } = setup();
    // A delivered handoff released with deleteRun keeps its snapshot after the canonical row goes.
    const released = { workflowName: 'agentic-loop', runId: 'a-released' };
    const settled = {
      ...snapshots.loopStart(released.runId, 'running', THREAD),
      status: 'success',
    } as WorkflowRunState;
    await persist(workflows, released, settled);
    const canonical = (await workflows.loadWorkflowSnapshot(released))!;
    const owner = { ...released, resourceId: RESOURCE, mutationFence: 'fence-a' };
    await expect(
      workflows.claimWorkflowSnapshotHandoff({
        ...owner,
        expectedCanonical: { kind: 'present', resourceId: RESOURCE, snapshot: canonical },
        snapshot: canonical,
      }),
    ).resolves.toMatchObject({ status: 'created' });
    await expect(
      workflows.completeWorkflowSnapshotHandoff({
        ...owner,
        expectedResourceId: RESOURCE,
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
    await persist(workflows, released, {
      ...snapshots.loopStart(released.runId, 'running', OTHER_THREAD),
      status: 'success',
    } as WorkflowRunState);

    // Terminal recovery envelopes, stored as the terminalization journal stores them.
    const terminal = (runId: string, envelope: Record<string, unknown>) =>
      db.workflowTerminalSnapshots.set(JSON.stringify(['agentic-loop', runId]), {
        version: 1,
        workflowName: 'agentic-loop',
        runId,
        resourceId: RESOURCE,
        terminalStatus: 'success',
        envelopeHash: `sha256:${'0'.repeat(64)}`,
        recordHash: `sha256:${'0'.repeat(64)}`,
        envelope: envelope as any,
        createdAt: 1,
      });
    // An envelope's requestContextPatch cannot carry a thread (the envelope
    // materializer refuses infrastructure keys): this row attributes nothing.
    terminal('b-envelope', { terminalResult: {}, finalState: {}, requestContextPatch: { mastra__threadId: THREAD } });
    // A durable run without a memory backend records its thread only in its SerializableDurableState.
    terminal('b2-durable-state', {
      workflowName: 'durable-agentic-loop',
      terminalResult: {
        status: 'success',
        output: { messageListState: { memoryInfo: null }, state: { threadId: THREAD } },
      },
      finalState: {},
      requestContextPatch: {},
    });
    // A content-free canonical snapshot whose terminal envelope holds data (no thread): not content-free.
    await persist(
      workflows,
      { workflowName: 'agentic-loop', runId: 'c-bare-terminal' },
      snapshots.createRunPending('c-bare-terminal'),
    );
    terminal('c-bare-terminal', { terminalResult: { text: 'result' }, finalState: {}, requestContextPatch: {} });
    // A terminal envelope of another thread makes the run attributed (not unattributed, not listed).
    await persist(
      workflows,
      { workflowName: 'agentic-loop', runId: 'd-bare-other' },
      snapshots.createRunPending('d-bare-other'),
    );
    terminal('d-bare-other', {
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
        resourceId: RESOURCE,
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
          workflowName: 'agentic-loop',
          runId: 'b2-durable-state',
          attribution: 'thread',
          rows: { snapshot: 'absent', handoff: 'absent', terminalSnapshot: 'thread' },
        },
        {
          workflowName: 'agentic-loop',
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

  it('finds a durable run through the terminal envelope the terminalization journal stores', async () => {
    const { workflows } = setup();
    const run = { workflowName: 'durable-agentic-loop', runId: 'durable-terminal' };
    // A durable run without a memory backend: the thread is only in its SerializableDurableState.
    const state = { threadId: THREAD, memoryConfigured: false };
    const input = { __workflowKind: 'durable-agent', runId: run.runId, state, messageListState: { memoryInfo: null } };
    await persist(workflows, run, { ...createEmptyWorkflowSnapshot(run.runId), context: { input } as any });
    const claim = await workflows.claimWorkflowTerminalization({
      ...run,
      eventKey: 'finish',
      terminalStatus: 'success',
      ownerId: 'owner',
      leaseMs: 10_000,
    });
    if (claim.status !== 'acquired') throw new Error(claim.status);
    const snapshot = { ...createEmptyWorkflowSnapshot(run.runId), status: 'success', context: { input } as any };
    await expect(
      workflows.persistWorkflowTerminalState({
        ...run,
        ownerId: claim.record.ownerId,
        claimToken: claim.record.claimToken,
        claimGeneration: claim.record.claimGeneration,
        resourceId: RESOURCE,
        snapshot: snapshot as WorkflowRunState,
        recoveryEnvelope: {
          version: 1,
          ...run,
          terminalStatus: 'success',
          executionMode: 'continuous',
          terminalResult: { status: 'success', output: { messageListState: { memoryInfo: null }, state } },
          finalState: {},
          requestContextPatch: {},
          childGraphFingerprint: createWorkflowTerminalGraphFingerprint(snapshot.serializedStepGraph),
          ancestry: [],
        } as any,
      }),
    ).resolves.toMatchObject({ status: 'persisted' });
    // Terminal persistence stores the whole step result as the canonical `result`;
    // with an input that records no thread, the canonical row is attributed by `result.output`.
    const resultOnly = { workflowName: 'durable-agentic-loop', runId: 'durable-result-output' };
    await persist(workflows, resultOnly, {
      ...createEmptyWorkflowSnapshot(resultOnly.runId),
      status: 'success',
      context: { input: { __workflowKind: 'durable-agent', messageListState: { memoryInfo: null }, state: {} } } as any,
      result: { status: 'success', output: { messageListState: { memoryInfo: null }, state } },
    } as WorkflowRunState);
    // The canonical snapshot is deleted; the retained envelope still holds the thread's data.
    await workflows.deleteWorkflowRunById(run);

    await expect(
      workflows.listWorkflowRunsByThreadForErasure({ resourceId: RESOURCE, threadId: THREAD, limit: 10 }),
    ).resolves.toMatchObject({
      status: 'listed',
      runs: [
        { ...resultOnly, attribution: 'thread', rows: SNAPSHOT_ONLY },
        { ...run, attribution: 'thread', rows: { snapshot: 'absent', handoff: 'absent', terminalSnapshot: 'thread' } },
      ],
      hasMore: false,
    });
  });

  it('finds suspensions engines keep inside a step suspend payload or foreach results', async () => {
    const { workflows } = setup();
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
      workflows,
      { workflowName: 'agentic-loop', runId: 'a-foreach-array' },
      withStep('a-foreach-array', {
        status: 'suspended',
        suspendPayload: { __workflow_meta: { foreachOutput: [{ status: 'success' }, suspended(THREAD)] } },
      }),
    );
    // The evented engine keys them by index; the first suspended iteration names another thread.
    await persist(
      workflows,
      { workflowName: 'agentic-loop', runId: 'b-foreach-object' },
      withStep('b-foreach-object', {
        status: 'suspended',
        suspendPayload: {
          __workflow_meta: { foreachOutput: { '0': suspended(OTHER_THREAD), '1': suspended(THREAD) } },
        },
      }),
    );
    // A foreach step's iteration results.
    await persist(
      workflows,
      { workflowName: 'agentic-loop', runId: 'c-foreach-output' },
      withStep('c-foreach-output', {
        status: 'suspended',
        output: [null, suspended(THREAD)],
      }),
    );
    // The caller-owned workflow input is never read as a step.
    await persist(workflows, { workflowName: 'agentic-loop', runId: 'e-input-shaped' }, {
      ...createEmptyWorkflowSnapshot('e-input-shaped'),
      status: 'suspended',
      context: { input: suspended(THREAD) },
    } as WorkflowRunState);
    // Another thread only.
    await persist(
      workflows,
      { workflowName: 'agentic-loop', runId: 'd-foreach-other' },
      withStep('d-foreach-other', {
        status: 'suspended',
        suspendPayload: { __workflow_meta: { foreachOutput: [suspended(OTHER_THREAD)] } },
      }),
    );

    await expect(
      workflows.listWorkflowRunsByThreadForErasure({ resourceId: RESOURCE, threadId: THREAD, limit: 10 }),
    ).resolves.toMatchObject({
      status: 'listed',
      runs: ['a-foreach-array', 'b-foreach-object', 'c-foreach-output'].map(runId => ({
        workflowName: 'agentic-loop',
        runId,
        attribution: 'thread',
        // b-foreach-object also keeps another thread's iteration.
        rows: runId === 'b-foreach-object' ? { ...SNAPSHOT_ONLY, snapshot: 'mixed' } : SNAPSHOT_ONLY,
      })),
      hasMore: false,
    });
  });

  it('reads network inputs and durable step payloads, only at their framework locations', async () => {
    const { workflows } = setup();
    const suspendedWithInput = (runId: string, input: Record<string, unknown>) =>
      ({ ...createEmptyWorkflowSnapshot(runId), status: 'suspended', context: { input } }) as WorkflowRunState;
    // The agent network records the thread in its workflow input.
    await persist(
      workflows,
      { workflowName: 'agent-loop-main-workflow', runId: 'a-network' },
      suspendedWithInput('a-network', {
        task: 'research',
        threadId: THREAD,
        threadResourceId: RESOURCE,
      }),
    );
    // The same input shape in any other workflow is application data.
    await persist(
      workflows,
      { workflowName: 'agentic-loop', runId: 'b-not-network' },
      suspendedWithInput('b-not-network', {
        threadId: THREAD,
      }),
    );
    // A durable run's running step keeps its input as `payload`.
    await persist(workflows, { workflowName: 'durable-agentic-loop', runId: 'c-durable-payload' }, {
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
      resourceId: RESOURCE,
      threadId: THREAD,
      limit: 10,
      includeUnattributed: true,
    });
    expect(
      result.status === 'listed' && result.runs.map(run => [run.workflowName, run.runId, run.attribution]),
    ).toEqual([
      ['agent-loop-main-workflow', 'a-network', 'thread'],
      ['agentic-loop', 'b-not-network', 'unattributed'],
      ['durable-agentic-loop', 'c-durable-payload', 'thread'],
    ]);
  });

  it('counts only the createRun shell as content-free, and only object snapshots', async () => {
    const { db, workflows } = setup();
    const shell = (runId: string) => ({ ...createEmptyWorkflowSnapshot(runId), status: 'pending' }) as WorkflowRunState;
    await persist(workflows, { workflowName: 'agentic-loop', runId: 'a-shell' }, shell('a-shell'));
    await persist(workflows, { workflowName: 'agentic-loop', runId: 'b-result' }, {
      ...shell('b-result'),
      result: { text: 'secret' },
    } as WorkflowRunState);
    await persist(workflows, { workflowName: 'agentic-loop', runId: 'c-value' }, {
      ...shell('c-value'),
      value: { draft: 'secret' },
    } as WorkflowRunState);
    await persist(workflows, { workflowName: 'agentic-loop', runId: 'd-extra-key' }, {
      ...shell('d-extra-key'),
      lifecycleOutbox: [{ kind: 'event' }],
    } as unknown as WorkflowRunState);
    // Rows stored as a string and as an array, as a raw store could hold them.
    const raw = (runId: string, snapshot: unknown) =>
      db.workflows.set(JSON.stringify(['agentic-loop', runId]), {
        workflow_name: 'agentic-loop',
        run_id: runId,
        resourceId: RESOURCE,
        snapshot: snapshot as any,
        createdAt: new Date(),
        updatedAt: new Date(),
      });
    raw(
      'e-string-root',
      JSON.stringify({
        status: 'pending',
        context: { input: { messages: ['secret'] } },
        requestContext: { mastra__threadId: THREAD },
      }),
    );
    raw('f-array-root', [{ status: 'pending' }]);

    const result = await workflows.listWorkflowRunsByThreadForErasure({
      resourceId: RESOURCE,
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
    const { workflows } = setup();
    const turn = 'turn-1';
    const userTurn = 'turn-2';
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
    await persist(workflows, { workflowName: 'agentic-loop', runId: turn }, createEmptyWorkflowSnapshot(turn));
    await persist(workflows, { workflowName: 'executionWorkflow', runId: turn }, child(turn));
    // A user workflow with the same shape is not loop-internal.
    await persist(workflows, { workflowName: 'research-workflow', runId: userTurn }, child(userTurn));

    const result = await workflows.listWorkflowRunsByThreadForErasure({
      resourceId: RESOURCE,
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
    const { workflows } = setup();
    const ids = ['b', 'a', 'Z', 'c', 'é'];
    for (const runId of ids) {
      await persist(workflows, { workflowName: 'agentic-loop', runId }, snapshots.loopStart(runId, 'running', THREAD));
    }
    await persist(workflows, { workflowName: 'agentic-loop', runId: 'b2' }, snapshots.otherThread('b2'));

    const seen: string[] = [];
    let after: Run | undefined;
    let pages = 0;
    for (;;) {
      const page = await workflows.listWorkflowRunsByThreadForErasure({
        resourceId: RESOURCE,
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

  it('rejects invalid input', async () => {
    const { workflows } = setup();
    await expect(
      workflows.listWorkflowRunsByThreadForErasure({ resourceId: RESOURCE, threadId: '', limit: 10 }),
    ).rejects.toThrow(TypeError);
    await expect(
      workflows.listWorkflowRunsByThreadForErasure({ resourceId: '', threadId: THREAD, limit: 10 }),
    ).rejects.toThrow(TypeError);
    await expect(
      workflows.listWorkflowRunsByThreadForErasure({ resourceId: RESOURCE, threadId: THREAD, limit: 0 }),
    ).rejects.toThrow(RangeError);
  });
});
