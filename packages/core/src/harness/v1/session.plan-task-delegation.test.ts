/**
 * TM-6 — durable subtask → subagent DELEGATION orchestration (§5.1k / §5.6).
 *
 * Covers the live `Session` / `Harness` seam that the pure ops
 * (plan-task-delegation.test.ts) sit under:
 *   - `task_delegate` spawns a subagent SESSION, writes the durable
 *     `delegatedSubagentSessionId` link, drives the plan task `in_progress`, and
 *     does NOT block the parent turn;
 *   - the delegated subagent COMPLETING rolls the plan task up `completed` and
 *     cascades to ancestors; FAILING (cancel) rolls it up `failed`;
 *   - RECOVERY: a fresh Harness rehydrating a parent whose delegated subagent
 *     already terminalized rolls the plan task up; one still-live re-attaches the
 *     completion hook so a later terminalization still rolls up.
 *
 * The harness is multi-instance where recovery is under test: two `Harness`
 * instances share ONE `InMemoryDB`, so a "crash" is `harness.shutdown()` (drops
 * the lease without closing) followed by a fresh instance hydrating the row —
 * exactly the cross-process recovery path.
 */

import { describe, expect, it } from 'vitest';

import { InMemoryHarness } from '../../storage/domains/harness/inmemory';
import { InMemoryDB } from '../../storage/domains/inmemory-db';

import { MockAgent } from './__test-utils__';
import { sha256CanonicalJson } from './canonical-json';
import { Harness } from './harness';
import { createPlanTaskTools, TASK_ADD_TOOL_ID, TASK_DELEGATE_TOOL_ID } from './plan-task-tool';
import type { Session } from './session';

// ---------------------------------------------------------------------------
// Setup
// ---------------------------------------------------------------------------

function buildHarness(db: InMemoryDB): { harness: Harness; parentAgent: MockAgent; childAgent: MockAgent } {
  const parentAgent = new MockAgent({ id: 'parent-agent' });
  const childAgent = new MockAgent({ id: 'child-agent' });
  const storage = new InMemoryHarness({ db });
  const harness = new Harness({
    agents: { 'parent-agent': parentAgent, 'child-agent': childAgent } as any,
    modes: [
      { id: 'default', agentId: 'parent-agent' },
      { id: 'worker-mode', agentId: 'child-agent' },
    ],
    defaultModeId: 'default',
    sessions: { storage },
    subagents: {
      maxDepth: 2,
      types: {
        worker: {
          agentId: 'child-agent',
          modeId: 'worker-mode',
          description: 'Background worker subagent',
          defaultModelId: 'openai/gpt-4o-mini',
          workspace: 'inherit',
        },
      },
    },
  });
  return { harness, parentAgent, childAgent };
}

const toolCtx = {
  abortSignal: new AbortController().signal,
  agent: { toolCallId: 'tc-deleg', runId: 'mock-run' },
  requestContext: { get: () => undefined },
} as any;

async function poll(pred: () => boolean | Promise<boolean>, timeoutMs = 2000): Promise<void> {
  const start = Date.now();
  for (;;) {
    if (await pred()) return;
    if (Date.now() - start > timeoutMs) throw new Error('poll timeout');
    await new Promise(r => setTimeout(r, 5));
  }
}

/** Open a parent turn so plan-task tools can run (custom events are turn-gated). */
async function openParentTurn(parent: Session, agent: MockAgent): Promise<() => void> {
  let release!: () => void;
  const holdUntil = new Promise<void>(r => (release = r));
  agent.enqueueRun({ holdUntil, finishReason: 'stop', text: 'done' });
  const turn = parent.message({ content: 'go' });
  await poll(() => agent.streamCalls.length > 0);
  return () => {
    release();
    void turn.catch(() => {});
  };
}

async function planRow(parent: Session, taskId: string) {
  const page = await parent._internalStorage.listPlanTasks({ sessionId: parent.id, limit: 100 });
  return page.tasks.find(t => t.taskId === taskId);
}

// ---------------------------------------------------------------------------
// task_delegate registration
// ---------------------------------------------------------------------------

describe('task_delegate registration', () => {
  it('is registered only when subagent types exist', async () => {
    const { harness } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    expect(createPlanTaskTools(parent)[TASK_DELEGATE_TOOL_ID]).toBeDefined();
  });

  it('is absent when no subagent types are configured', async () => {
    const storage = new InMemoryHarness({ db: new InMemoryDB() });
    const agent = new MockAgent({ id: 'default' });
    const harness = new Harness({
      agents: { default: agent } as any,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage },
    });
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    expect(createPlanTaskTools(parent)[TASK_DELEGATE_TOOL_ID]).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// includeSubtree — faithful (lossless) subset transfer to the subagent
// ---------------------------------------------------------------------------

/** Extract the text the child agent received on its first stream call. */
function childTaskText(childAgent: MockAgent): string {
  const call = childAgent.streamCalls[0];
  return JSON.stringify(call?.messages ?? '');
}

describe('task_delegate — includeSubtree faithful transfer', () => {
  it('preserves taskId, status, order, blockedBy, and TRUE depth (multi-level)', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    // Hold the child so we can inspect the body it received before it settles.
    let releaseChild!: () => void;
    childAgent.enqueueRun({ holdUntil: new Promise<void>(r => (releaseChild = r)), finishReason: 'stop' });

    const close = await openParentTurn(parent, parentAgent);
    const tools = createPlanTaskTools(parent);

    // root → childA (with grandchild) + childB; give them distinct statuses/order.
    const root = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content: 'root task' } as any, toolCtx)) as any;
    const childA = (await tools[TASK_ADD_TOOL_ID]!.execute!(
      { content: 'child A', parentTaskId: root.taskId } as any,
      toolCtx,
    )) as any;
    const childB = (await tools[TASK_ADD_TOOL_ID]!.execute!(
      { content: 'child B', parentTaskId: root.taskId } as any,
      toolCtx,
    )) as any;
    const grandchild = (await tools[TASK_ADD_TOOL_ID]!.execute!(
      { content: 'grandchild', parentTaskId: childA.taskId } as any,
      toolCtx,
    )) as any;
    // Mark childB completed and add a blockedBy edge so we can assert they survive.
    await parent._internalStorage.mutatePlanTasksForSession({
      fence: {
        sessionId: parent.id,
        ownerId: (parent as any)._internalOwnerId,
        ifSessionVersion: (parent as any)._internalRecordVersion,
      },
      ops: [
        {
          kind: 'update',
          taskId: childB.taskId,
          ifVersion: (await planRow(parent, childB.taskId))!.version,
          patch: { status: 'completed', statusSource: 'explicit', blockedBy: [childA.taskId] },
        },
      ],
    });

    await tools[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: root.taskId, agentType: 'worker', task: 'do the root', includeSubtree: true } as any,
      toolCtx,
    );
    await poll(() => childAgent.streamCalls.length > 0);
    const body = childTaskText(childAgent);

    // Every node id is present (lossless), not just the flat content list.
    expect(body).toContain(`id=${root.taskId}`);
    expect(body).toContain(`id=${childA.taskId}`);
    expect(body).toContain(`id=${childB.taskId}`);
    expect(body).toContain(`id=${grandchild.taskId}`);
    // Status + blockedBy survive.
    expect(body).toContain('status=completed');
    expect(body).toContain(`blockedBy=${childA.taskId}`);
    // TRUE depth: the grandchild is rendered two levels in (4 leading spaces),
    // not flattened to one level like the old indent.
    const lines = JSON.parse(body) as unknown;
    const text = typeof lines === 'string' ? lines : JSON.stringify(lines);
    const grandLine = text.split('\\n').find(l => l.includes(`id=${grandchild.taskId}`)) ?? '';
    expect(grandLine).toMatch(/^.*\s{4}- \[/); // depth-2 indent (2 spaces per level)

    releaseChild();
    close();
    await poll(async () => (await planRow(parent, root.taskId))?.status === 'completed');
  });

  it('signals truncation when the subtree is clipped at the node cap', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    let releaseChild!: () => void;
    childAgent.enqueueRun({ holdUntil: new Promise<void>(r => (releaseChild = r)), finishReason: 'stop' });
    const close = await openParentTurn(parent, parentAgent);
    const tools = createPlanTaskTools(parent);

    const root = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content: 'root' } as any, toolCtx)) as any;
    // Add > the 100-node cap so the storage read reports truncated.
    for (let i = 0; i < 110; i++) {
      await tools[TASK_ADD_TOOL_ID]!.execute!({ content: `c${i}`, parentTaskId: root.taskId } as any, toolCtx);
    }
    await tools[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: root.taskId, agentType: 'worker', includeSubtree: true } as any,
      toolCtx,
    );
    await poll(() => childAgent.streamCalls.length > 0);
    expect(childTaskText(childAgent)).toContain('TRUNCATED');

    releaseChild();
    close();
    await poll(async () => (await planRow(parent, root.taskId))?.status === 'completed');
  });
});

// ---------------------------------------------------------------------------
// Delegate → link + in_progress (durable, parent turn does not block)
// ---------------------------------------------------------------------------

describe('task_delegate — link + in_progress', () => {
  it('writes delegatedSubagentSessionId, drives the task in_progress, and returns the child id', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    // Hold the child turn so the task stays in_progress while we inspect it.
    let releaseChild!: () => void;
    childAgent.enqueueRun({ holdUntil: new Promise<void>(r => (releaseChild = r)), finishReason: 'stop' });

    const close = await openParentTurn(parent, parentAgent);
    const tools = createPlanTaskTools(parent);
    const task = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content: 'research topic' } as any, toolCtx)) as any;
    const res = (await tools[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: task.taskId, agentType: 'worker', task: 'do the research' } as any,
      toolCtx,
    )) as any;

    expect(res.isError).toBeFalsy();
    expect(typeof res.subagentSessionId).toBe('string');
    expect(res.subagentSessionId).not.toBe(parent.id);
    expect(res.status).toBe('in_progress');
    expect(res.delegatedSubagentSessionId).toBe(res.subagentSessionId);

    // Durable link persisted, task still in_progress while the subagent runs.
    const row = await planRow(parent, task.taskId);
    expect(row?.delegatedSubagentSessionId).toBe(res.subagentSessionId);
    expect(row?.status).toBe('in_progress');

    releaseChild();
    close();
    await poll(async () => (await planRow(parent, task.taskId))?.status === 'completed');
  });

  it('returns an isError payload for an unknown agentType (does not abort the turn)', async () => {
    const { harness, parentAgent } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const close = await openParentTurn(parent, parentAgent);
    const tools = createPlanTaskTools(parent);
    const task = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content: 't' } as any, toolCtx)) as any;
    // zod enum rejects the unknown agentType before execute().
    const res = (await tools[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: task.taskId, agentType: 'bogus' } as any,
      toolCtx,
    )) as any;
    expect(res.error ?? res.isError).toBeTruthy();
    close();
  });
});

// ---------------------------------------------------------------------------
// Rollup from subagent completion / failure
// ---------------------------------------------------------------------------

describe('task_delegate — rollup from subagent outcome', () => {
  it('subagent completes → plan task completed + cascades to parent', async () => {
    const { harness, parentAgent } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const close = await openParentTurn(parent, parentAgent);
    const tools = createPlanTaskTools(parent);

    const root = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content: 'root' } as any, toolCtx)) as any;
    const child = (await tools[TASK_ADD_TOOL_ID]!.execute!(
      { content: 'delegated child', parentTaskId: root.taskId } as any,
      toolCtx,
    )) as any;

    await tools[TASK_DELEGATE_TOOL_ID]!.execute!({ taskId: child.taskId, agentType: 'worker' } as any, toolCtx);

    // Child agent completes immediately (default run); the hook rolls up.
    await poll(async () => (await planRow(parent, child.taskId))?.status === 'completed');
    expect((await planRow(parent, root.taskId))?.status).toBe('completed');
    close();
  });

  it('subagent fails (cancelled) → plan task failed + cascades failed to parent', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    // Hold the child so we can cancel it mid-flight → the delegated turn fails.
    let releaseChild!: () => void;
    childAgent.enqueueRun({ holdUntil: new Promise<void>(r => (releaseChild = r)), finishReason: 'stop' });

    const close = await openParentTurn(parent, parentAgent);
    const tools = createPlanTaskTools(parent);
    const root = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content: 'root' } as any, toolCtx)) as any;
    const child = (await tools[TASK_ADD_TOOL_ID]!.execute!(
      { content: 'delegated child', parentTaskId: root.taskId } as any,
      toolCtx,
    )) as any;
    const res = (await tools[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: child.taskId, agentType: 'worker' } as any,
      toolCtx,
    )) as any;

    // Cancel the delegated subagent session → its message() rejects → failed.
    const sub = await harness.session({ sessionId: res.subagentSessionId, resourceId: 'u1' });
    await sub.cancel({ reason: 'test cancel' });
    releaseChild();

    await poll(async () => (await planRow(parent, child.taskId))?.status === 'failed');
    expect((await planRow(parent, root.taskId))?.status).toBe('failed');
    close();
  });
});

// ---------------------------------------------------------------------------
// Recovery / reconcile-on-rehydrate (cross-instance)
// ---------------------------------------------------------------------------

describe('task_delegate — recovery on rehydrate', () => {
  it('a delegated subagent that terminalized while we were down rolls the plan task up on rehydrate', async () => {
    const db = new InMemoryDB();
    const a = buildHarness(db);
    const parentA = await a.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const close = await openParentTurn(parentA, a.parentAgent);
    const toolsA = createPlanTaskTools(parentA);
    const task = (await toolsA[TASK_ADD_TOOL_ID]!.execute!({ content: 'delegated' } as any, toolCtx)) as any;
    const res = (await toolsA[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: task.taskId, agentType: 'worker' } as any,
      toolCtx,
    )) as any;
    const childId: string = res.subagentSessionId;

    // Let the subagent finish AND close (terminal), then simulate this process
    // dying with the rollup hook lost: force the plan task BACK to in_progress in
    // storage so the rehydrate scan has real reconcile work to do, and CLOSE the
    // child so its terminal state is observable from storage.
    await poll(async () => (await planRow(parentA, task.taskId))?.status === 'completed');
    const child = await a.harness.session({ sessionId: childId, resourceId: 'u1' });
    await child.close();
    // Re-arm a non-terminal delegated link as if the hook never fired.
    const fence = {
      sessionId: parentA.id,
      ownerId: (parentA as any)._internalOwnerId,
      ifSessionVersion: (parentA as any)._internalRecordVersion,
    };
    const row = await planRow(parentA, task.taskId);
    await parentA._internalStorage.mutatePlanTasksForSession({
      fence,
      ops: [
        {
          kind: 'update',
          taskId: task.taskId,
          ifVersion: row!.version,
          patch: { status: 'in_progress', statusSource: 'explicit' },
        },
      ],
    });
    close();
    await a.harness.shutdown();

    // Fresh instance hydrates the parent → reconcile reads the closed subagent
    // and rolls the plan task up.
    const b = buildHarness(db);
    const parentB = await b.harness.session({ sessionId: parentA.id, resourceId: 'u1' });
    await poll(async () => {
      const page = await parentB._internalStorage.listPlanTasks({ sessionId: parentB.id, limit: 100 });
      return page.tasks.find(t => t.taskId === task.taskId)?.status === 'completed';
    });
    await b.harness.shutdown();
  });

  // CRASH-FAITHFUL: the original detached drive promise is LOST before rollup
  // (lease stolen at the DB level, no graceful drain). The child run was mid-flight
  // when the process died, so there is NO live run to observe and NO terminal
  // evidence — reconcile must FAIL-CLOSE the delegation rather than silently start
  // a brand-new SECOND run. (A graceful re-run is not possible: a plain message
  // turn has no run-resumption primitive, so the safe recovery is `failed`, which
  // the parent agent can re-delegate.) Critically it does NOT send a second
  // child.message: the deterministic delegation admissionId makes the re-issue a
  // duplicate that observes the orphaned reservation instead of enqueuing a run.
  it('a crash-orphaned in-flight delegation fails closed on rehydrate (no second run)', async () => {
    const db = new InMemoryDB();
    const a = buildHarness(db);
    const parentA = await a.harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    // Hold the child so it is genuinely mid-run at the moment of the crash.
    let releaseChild!: () => void;
    a.childAgent.enqueueRun({ holdUntil: new Promise<void>(r => (releaseChild = r)), finishReason: 'stop' });

    const close = await openParentTurn(parentA, a.parentAgent);
    const toolsA = createPlanTaskTools(parentA);
    const task = (await toolsA[TASK_ADD_TOOL_ID]!.execute!({ content: 'delegated' } as any, toolCtx)) as any;
    const res = (await toolsA[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: task.taskId, agentType: 'worker' } as any,
      toolCtx,
    )) as any;
    const childId: string = res.subagentSessionId;
    close();

    // Crash A WITHOUT graceful drain: steal every A-owned lease at the DB level so
    // A's live sessions (and its detached delegation driver) are abandoned and the
    // rows stay ACTIVE (not closed) for B to recover.
    for (const [key, row] of db.harnessSessions) {
      if (row.ownerId === (parentA as any)._internalOwnerId) {
        db.harnessSessions.set(key, { ...row, ownerId: 'crashed', leaseExpiresAt: Date.now() - 1 });
      }
    }

    // Fresh instance. Stage a child run that, IF the buggy path re-messaged, would
    // complete to `completed` — so a `failed` result PROVES no second run was sent.
    const b = buildHarness(db);
    let releaseChildB!: () => void;
    b.childAgent.enqueueRun({ holdUntil: new Promise<void>(r => (releaseChildB = r)), finishReason: 'stop' });
    const parentB = await b.harness.session({ sessionId: parentA.id, resourceId: 'u1' });

    // Reconcile fail-closes the orphaned delegation (no live run, no terminal
    // evidence) — the task rolls up `failed`, NOT `completed`.
    await poll(async () => {
      const page = await parentB._internalStorage.listPlanTasks({ sessionId: parentB.id, limit: 100 });
      return page.tasks.find(t => t.taskId === task.taskId)?.status === 'failed';
    });
    // B's staged child run was never consumed — no second run was enqueued.
    expect(b.childAgent.streamCalls.length).toBe(0);
    void releaseChild;
    void releaseChildB;
    void childId;
    await b.harness.shutdown();
  });

  // CRASH-FAITHFUL: the child run already COMPLETED (durable terminal evidence
  // written by the delegation admissionId) before the crash, but the rollup hook
  // was lost. Reconcile reads the durable `completed` evidence and rolls up
  // `completed` — without re-acquiring or re-messaging the child.
  it('a child that COMPLETED before the crash recovers as completed from durable evidence', async () => {
    const db = new InMemoryDB();
    const a = buildHarness(db);
    const parentA = await a.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const close = await openParentTurn(parentA, a.parentAgent);
    const toolsA = createPlanTaskTools(parentA);
    const task = (await toolsA[TASK_ADD_TOOL_ID]!.execute!({ content: 'delegated' } as any, toolCtx)) as any;
    const res = (await toolsA[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: task.taskId, agentType: 'worker' } as any,
      toolCtx,
    )) as any;
    const childId: string = res.subagentSessionId;

    // The child completes (default run); its durable `completed` evidence is now
    // persisted. Simulate the hook being lost by forcing the plan task BACK to
    // in_progress, then crash A by stealing the lease (rows stay ACTIVE).
    await poll(async () => (await planRow(parentA, task.taskId))?.status === 'completed');
    const row = await planRow(parentA, task.taskId);
    await parentA._internalStorage.mutatePlanTasksForSession({
      fence: {
        sessionId: parentA.id,
        ownerId: (parentA as any)._internalOwnerId,
        ifSessionVersion: (parentA as any)._internalRecordVersion,
      },
      ops: [
        {
          kind: 'update',
          taskId: task.taskId,
          ifVersion: row!.version,
          patch: { status: 'in_progress', statusSource: 'explicit' },
        },
      ],
    });
    close();
    for (const [key, r] of db.harnessSessions) {
      if (r.ownerId === (parentA as any)._internalOwnerId) {
        db.harnessSessions.set(key, { ...r, ownerId: 'crashed', leaseExpiresAt: Date.now() - 1 });
      }
    }

    const b = buildHarness(db);
    const parentB = await b.harness.session({ sessionId: parentA.id, resourceId: 'u1' });
    await poll(async () => {
      const page = await parentB._internalStorage.listPlanTasks({ sessionId: parentB.id, limit: 100 });
      return page.tasks.find(t => t.taskId === task.taskId)?.status === 'completed';
    });
    void childId;
    await b.harness.shutdown();
  });

  // CRASH-FAITHFUL: a CANCELLED delegated child recovers as `failed` even when its
  // held run raced to a `complete`/`completed` durable evidence row — the
  // `cancelRequest` marker overrides the evidence, mirroring the live driver. (The
  // OLD recovery did handle cancel via the close marker, but only because it never
  // consulted the durable evidence; the cancel override must survive now that
  // evidence is authoritative.)
  it('a CANCELLED child recovers as failed despite a raced completed evidence row', async () => {
    const db = new InMemoryDB();
    const a = buildHarness(db);
    const parentA = await a.harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    // Hold the child so we can cancel it → its delegated run records `failed`.
    let releaseChild!: () => void;
    a.childAgent.enqueueRun({ holdUntil: new Promise<void>(r => (releaseChild = r)), finishReason: 'stop' });

    const close = await openParentTurn(parentA, a.parentAgent);
    const toolsA = createPlanTaskTools(parentA);
    const task = (await toolsA[TASK_ADD_TOOL_ID]!.execute!({ content: 'delegated' } as any, toolCtx)) as any;
    const res = (await toolsA[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: task.taskId, agentType: 'worker' } as any,
      toolCtx,
    )) as any;
    const childId: string = res.subagentSessionId;

    const sub = await a.harness.session({ sessionId: childId, resourceId: 'u1' });
    await sub.cancel({ reason: 'test cancel' });
    releaseChild();
    // Let the delegated turn settle to a durable `failed` evidence row.
    await poll(async () => (await planRow(parentA, task.taskId))?.status === 'failed');
    // Force the plan task BACK to in_progress so the rehydrate scan has work, and
    // re-link it (cancel rolled it up + cleared the link path) so reconcile runs.
    const row = await planRow(parentA, task.taskId);
    await parentA._internalStorage.mutatePlanTasksForSession({
      fence: {
        sessionId: parentA.id,
        ownerId: (parentA as any)._internalOwnerId,
        ifSessionVersion: (parentA as any)._internalRecordVersion,
      },
      ops: [
        {
          kind: 'update',
          taskId: task.taskId,
          ifVersion: row!.version,
          patch: { status: 'in_progress', statusSource: 'explicit', delegatedSubagentSessionId: childId },
        },
      ],
    });
    close();
    await a.harness.shutdown();

    // Fresh instance: the durable `failed` evidence (NOT a close-without-cancel
    // guess) makes the recovery `failed`.
    const b = buildHarness(db);
    const parentB = await b.harness.session({ sessionId: parentA.id, resourceId: 'u1' });
    await poll(async () => {
      const page = await parentB._internalStorage.listPlanTasks({ sessionId: parentB.id, limit: 100 });
      return page.tasks.find(t => t.taskId === task.taskId)?.status === 'failed';
    });
    await b.harness.shutdown();
  });

  // CRASH-FAITHFUL: a delegated run that RESOLVED with a non-success finishReason
  // ('error'/'aborted') settles as `completed` evidence (it resolved, did not
  // reject), but the durable `result` carries finishReason, so recovery reads it
  // back and rolls the task up `failed` — matching the live driver. This closes
  // the former resolve-with-error crash window (which used to recover `completed`).
  it('a child that RESOLVED with finishReason error recovers as failed from the durable result', async () => {
    const db = new InMemoryDB();
    const a = buildHarness(db);
    const parentA = await a.harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    // The child RESOLVES with finishReason:'error' (a resolve, NOT a reject) → its
    // durable message-result evidence is `completed` with result.finishReason='error'.
    a.childAgent.enqueueRun({ finishReason: 'error' });

    const close = await openParentTurn(parentA, a.parentAgent);
    const toolsA = createPlanTaskTools(parentA);
    const task = (await toolsA[TASK_ADD_TOOL_ID]!.execute!({ content: 'delegated' } as any, toolCtx)) as any;
    const res = (await toolsA[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: task.taskId, agentType: 'worker' } as any,
      toolCtx,
    )) as any;
    const childId: string = res.subagentSessionId;

    // Live driver maps finishReason:'error' → failed.
    await poll(async () => (await planRow(parentA, task.taskId))?.status === 'failed');

    // Simulate the rollup hook being lost: force the task BACK to in_progress and
    // re-link (the failed rollup cleared the link), then crash A (steal lease).
    const row = await planRow(parentA, task.taskId);
    await parentA._internalStorage.mutatePlanTasksForSession({
      fence: {
        sessionId: parentA.id,
        ownerId: (parentA as any)._internalOwnerId,
        ifSessionVersion: (parentA as any)._internalRecordVersion,
      },
      ops: [
        {
          kind: 'update',
          taskId: task.taskId,
          ifVersion: row!.version,
          patch: { status: 'in_progress', statusSource: 'explicit', delegatedSubagentSessionId: childId },
        },
      ],
    });
    close();
    for (const [key, r] of db.harnessSessions) {
      if (r.ownerId === (parentA as any)._internalOwnerId) {
        db.harnessSessions.set(key, { ...r, ownerId: 'crashed', leaseExpiresAt: Date.now() - 1 });
      }
    }

    // Fresh instance: recovery reads the durable `completed` evidence whose
    // result.finishReason === 'error' → rolls up `failed`, NOT completed.
    const b = buildHarness(db);
    const parentB = await b.harness.session({ sessionId: parentA.id, resourceId: 'u1' });
    await poll(async () => {
      const page = await parentB._internalStorage.listPlanTasks({ sessionId: parentB.id, limit: 100 });
      return page.tasks.find(t => t.taskId === task.taskId)?.status === 'failed';
    });
    await b.harness.shutdown();
  });

  // CRASH-FAITHFUL #2 (lossy-recovery fix): a delegated run that FAILED durably
  // (a rejecting agent error writes a `failed` message-result evidence row, keyed
  // by the deterministic delegation admissionId) must recover as `failed`. The
  // OLD recovery mapped any close-without-cancel child to `completed`, so this
  // FAILED delegation would have masqueraded as completed. We seed the `failed`
  // evidence row directly (the rejecting-error path) and assert the durable-evidence
  // read wins over the close marker.
  it('a child with durable FAILED evidence recovers as failed (not completed) on rehydrate', async () => {
    const db = new InMemoryDB();
    const a = buildHarness(db);
    const parentA = await a.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const close = await openParentTurn(parentA, a.parentAgent);
    const toolsA = createPlanTaskTools(parentA);

    // Hold the child so the delegation stays in_progress while we seed evidence.
    let releaseChild!: () => void;
    a.childAgent.enqueueRun({ holdUntil: new Promise<void>(r => (releaseChild = r)), finishReason: 'stop' });

    const task = (await toolsA[TASK_ADD_TOOL_ID]!.execute!({ content: 'delegated' } as any, toolCtx)) as any;
    const res = (await toolsA[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: task.taskId, agentType: 'worker' } as any,
      toolCtx,
    )) as any;
    const childId: string = res.subagentSessionId;

    // Seed the child's durable `failed` message-result evidence under the SAME
    // deterministic signalId the delegation admissionId derives (mirrors a real
    // rejecting-error run). The child stays ACTIVE (not closed, not cancelled), so
    // the close marker can't supply the outcome — only the durable evidence can.
    const childRec = (await parentA._internalStorage.loadSession({ harnessName: 'default', sessionId: childId }))!;
    const admissionId = `harness-delegate:${parentA.id}:${task.taskId}`;
    const digest = sha256CanonicalJson({
      kind: 'message-admission',
      harnessName: childRec.harnessName,
      sessionId: childRec.id,
      resourceId: childRec.resourceId,
      threadId: childRec.threadId,
      admissionId,
    });
    const signalId = `harness-message-${digest.slice(0, 32)}`;
    await parentA._internalStorage.writeMessageResultEvidence({
      status: 'failed',
      signalId,
      runId: `${signalId}-run`,
      harnessName: childRec.harnessName,
      sessionId: childRec.id,
      resourceId: childRec.resourceId,
      threadId: childRec.threadId,
      error: { name: 'HarnessExecutionError', message: 'delegated run failed' } as any,
      createdAt: Date.now(),
      updatedAt: Date.now(),
    } as any);

    // Crash A (steal leases) so B's reconcile reads the durable evidence.
    close();
    for (const [key, r] of db.harnessSessions) {
      if (r.ownerId === (parentA as any)._internalOwnerId) {
        db.harnessSessions.set(key, { ...r, ownerId: 'crashed', leaseExpiresAt: Date.now() - 1 });
      }
    }
    void releaseChild;

    const b = buildHarness(db);
    const parentB = await b.harness.session({ sessionId: parentA.id, resourceId: 'u1' });
    await poll(async () => {
      const page = await parentB._internalStorage.listPlanTasks({ sessionId: parentB.id, limit: 100 });
      return page.tasks.find(t => t.taskId === task.taskId)?.status === 'failed';
    });
    await b.harness.shutdown();
  });
});
