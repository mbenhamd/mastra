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

import { describe, expect, it, vi } from 'vitest';

import { InMemoryHarness } from '../../storage/domains/harness/inmemory';
import { InMemoryDB } from '../../storage/domains/inmemory-db';

import { MockAgent } from './__test-utils__';
import { Harness } from './harness';
import type { Session } from './session';
import { createPlanTaskTools, TASK_ADD_TOOL_ID, TASK_DELEGATE_TOOL_ID } from './plan-task-tool';

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

  it('a still-live delegated subagent re-attaches the completion hook on rehydrate', async () => {
    const db = new InMemoryDB();
    const a = buildHarness(db);
    const parentA = await a.harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    // Hold the child so it stays live across the "crash".
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
    // A's live sessions are abandoned (its detached delegation driver is orphaned)
    // and the rows stay ACTIVE (not closed) for B to recover. A graceful
    // `shutdown()` cannot drain here because the delegated child turn is held open.
    for (const [key, row] of db.harnessSessions) {
      if (row.ownerId === (parentA as any)._internalOwnerId) {
        db.harnessSessions.set(key, { ...row, ownerId: 'crashed', leaseExpiresAt: Date.now() - 1 });
      }
    }

    // Fresh instance hydrates the parent → reconcile re-attaches the live hook.
    const b = buildHarness(db);
    // Hand B's child agent a run that completes when released.
    let releaseChildB!: () => void;
    b.childAgent.enqueueRun({ holdUntil: new Promise<void>(r => (releaseChildB = r)), finishReason: 'stop' });
    const parentB = await b.harness.session({ sessionId: parentA.id, resourceId: 'u1' });

    // The task is still in_progress (hook re-armed, subagent not yet terminal).
    await new Promise(r => setTimeout(r, 30));
    const beforeRows = await parentB._internalStorage.listPlanTasks({ sessionId: parentB.id, limit: 100 });
    expect(beforeRows.tasks.find(t => t.taskId === task.taskId)?.status).toBe('in_progress');

    // The re-attached hook drives B's child turn; releasing it terminalizes the
    // subagent and rolls the plan task up.
    releaseChildB();
    void releaseChild; // A's held run is abandoned with the dead instance.
    await poll(async () => {
      const page = await parentB._internalStorage.listPlanTasks({ sessionId: parentB.id, limit: 100 });
      return page.tasks.find(t => t.taskId === task.taskId)?.status === 'completed';
    });
    void childId;
    await b.harness.shutdown();
  });
});
