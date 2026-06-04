/**
 * TM-3 plan-task tool WIRING into the live Session (§6.4 / §5.1k).
 *
 * Covers:
 *   - the plan-task tools are injected into the `harness:builtin` toolset the
 *     agent receives every turn (the §6.4 injection point), and
 *   - calling a built-in plan-task tool mid-turn mutates the session's plan tree
 *     AND emits the `papersflow.plan_task.updated` custom event through the live
 *     session (the turn-gated `_emitCustomEvent` path).
 *
 * The op-level semantics (rollup / cycle / single-in_progress / atomicity) are
 * locked in plan-task-session.test.ts against real storage; here we assert the
 * harness seam itself.
 */

import { describe, expect, it } from 'vitest';

import { setupHarness } from './__test-utils__/setup';
import type { HarnessEvent } from './events';
import {
  createPlanTaskTools,
  TASK_ADD_TOOL_ID,
  TASK_CHECK_TOOL_ID,
  TASK_COMPLETE_TOOL_ID,
  TASK_DECOMPOSE_TOOL_ID,
  TASK_REPARENT_TOOL_ID,
  TASK_UPDATE_TOOL_ID,
  TASK_WRITE_TOOL_ID,
} from './plan-task-tool';

const PLAN_TASK_TOOL_IDS = [
  TASK_ADD_TOOL_ID,
  TASK_DECOMPOSE_TOOL_ID,
  TASK_REPARENT_TOOL_ID,
  TASK_UPDATE_TOOL_ID,
  TASK_COMPLETE_TOOL_ID,
  TASK_CHECK_TOOL_ID,
  TASK_WRITE_TOOL_ID,
];

describe('plan-task tool registration', () => {
  it('createPlanTaskTools returns every plan-task tool with the canonical ids', async () => {
    const { harness } = setupHarness();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const tools = createPlanTaskTools(session);
    expect(Object.keys(tools).sort()).toEqual([...PLAN_TASK_TOOL_IDS].sort());
    for (const id of PLAN_TASK_TOOL_IDS) {
      expect(tools[id]!.id).toBe(id);
    }
  });

  it('injects the plan-task tools into the harness:builtin toolset the agent receives', async () => {
    const { harness, agent } = setupHarness();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    await session.message({ content: 'hi' });
    const toolsets = agent.streamCalls.at(-1)!.options.toolsets;
    expect(toolsets).toBeDefined();
    const builtin = toolsets['harness:builtin'];
    expect(builtin).toBeDefined();
    for (const id of PLAN_TASK_TOOL_IDS) {
      expect(builtin[id]).toBeDefined();
    }
  });
});

describe('plan-task tool execution mid-turn', () => {
  it('task_add mutates the tree and emits papersflow.plan_task.updated through the live session', async () => {
    const { harness, agent } = setupHarness();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    const events: HarnessEvent[] = [];
    session.subscribe(e => events.push(e));

    // Hold a turn open so the tool executes while `_currentTurnAbortController`
    // is set (the active-turn gate for custom events). Resolve the hold once we
    // have driven the tool.
    let release!: () => void;
    const holdUntil = new Promise<void>(r => (release = r));
    agent.enqueueRun({ holdUntil, finishReason: 'stop', text: 'done' });

    const turn = session.message({ content: 'go' });
    // Wait until the agent's stream has been invoked so the turn is active.
    await vitestPoll(() => agent.streamCalls.length > 0);

    const tools = createPlanTaskTools(session);
    const result = (await tools[TASK_ADD_TOOL_ID]!.execute!(
      { content: 'plan thing' } as any,
      {
        abortSignal: new AbortController().signal,
        agent: { toolCallId: 'tc-1', runId: 'mock-run' },
        requestContext: { get: () => undefined },
      } as any,
    )) as any;

    expect(result.isError).toBeFalsy();
    expect(result.content).toBe('plan thing');

    release();
    await turn;

    const custom = events.find(e => e.type === 'papersflow.plan_task.updated');
    expect(custom).toBeDefined();
    expect((custom as any).payload).toMatchObject({ op: 'add' });
    expect((custom as any).payload.affectedTaskIds).toContain(result.taskId);

    // Tree persisted.
    const stored = await session._internalStorage.listPlanTasks({ sessionId: session.id, limit: 10 });
    expect(stored.tasks.map(t => t.content)).toContain('plan thing');
  });

  it('getDisplayState surfaces the bounded plan-task summary after a mutation (no full tree)', async () => {
    const { harness, agent } = setupHarness();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    // Before any plan-task activity the summary is absent.
    expect(session.getDisplayState().planTasks).toBeUndefined();

    let release!: () => void;
    const holdUntil = new Promise<void>(r => (release = r));
    agent.enqueueRun({ holdUntil, finishReason: 'stop', text: 'done' });
    const turn = session.message({ content: 'go' });
    await vitestPoll(() => agent.streamCalls.length > 0);

    const tools = createPlanTaskTools(session);
    const ctx = {
      abortSignal: new AbortController().signal,
      agent: { toolCallId: 'tc-s', runId: 'mock-run' },
      requestContext: { get: () => undefined },
    } as any;
    const root = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content: 'root' } as any, ctx)) as any;
    await tools[TASK_DECOMPOSE_TOOL_ID]!.execute!(
      { parentTaskId: root.taskId, children: [{ content: 'c1' }, { content: 'c2' }] } as any,
      ctx,
    );
    await tools[TASK_UPDATE_TOOL_ID]!.execute!({ taskId: root.taskId, status: 'in_progress' } as any, ctx);

    const summary = session.getDisplayState().planTasks!;
    expect(summary.total).toBe(3);
    expect(summary.rootCount).toBe(1);
    expect(summary.inProgressTaskIds).toEqual([root.taskId]);
    expect(summary.byStatus.in_progress).toBe(1);
    expect(summary.byStatus.pending).toBe(2);
    // Bounded — no full tree embedded on the snapshot.
    expect(Object.keys(summary).sort()).toEqual(['byStatus', 'inProgressTaskIds', 'rootCount', 'total']);

    release();
    await turn;
  });

  it('task_write back-compat: no taskId adds, taskId updates', async () => {
    const { harness, agent } = setupHarness();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    let release!: () => void;
    const holdUntil = new Promise<void>(r => (release = r));
    agent.enqueueRun({ holdUntil, finishReason: 'stop', text: 'done' });
    const turn = session.message({ content: 'go' });
    await vitestPoll(() => agent.streamCalls.length > 0);

    const tools = createPlanTaskTools(session);
    const ctx = {
      abortSignal: new AbortController().signal,
      agent: { toolCallId: 'tc-w', runId: 'mock-run' },
      requestContext: { get: () => undefined },
    } as any;
    const added = (await tools[TASK_WRITE_TOOL_ID]!.execute!({ content: 'via write' } as any, ctx)) as any;
    expect(added.taskId).toBeTruthy();
    expect(added.content).toBe('via write');
    const updated = (await tools[TASK_WRITE_TOOL_ID]!.execute!(
      { taskId: added.taskId, status: 'completed' } as any,
      ctx,
    )) as any;
    expect(updated.status).toBe('completed');

    release();
    await turn;
  });

  it('task_write back-compat: legacy {tasks:[...]} full-list shape adds + updates by content', async () => {
    const { harness, agent } = setupHarness();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    let release!: () => void;
    const holdUntil = new Promise<void>(r => (release = r));
    agent.enqueueRun({ holdUntil, finishReason: 'stop', text: 'done' });
    const turn = session.message({ content: 'go' });
    await vitestPoll(() => agent.streamCalls.length > 0);

    const tools = createPlanTaskTools(session);
    const ctx = {
      abortSignal: new AbortController().signal,
      agent: { toolCallId: 'tc-legacy', runId: 'mock-run' },
      requestContext: { get: () => undefined },
    } as any;

    // Legacy mastracode contract: pass the FULL list. First call CREATES the tasks.
    const first = (await tools[TASK_WRITE_TOOL_ID]!.execute!(
      {
        tasks: [
          { content: 'design', status: 'in_progress', activeForm: 'Designing' },
          { content: 'build', status: 'pending' },
          { content: 'ship', status: 'pending' },
        ],
      } as any,
      ctx,
    )) as any;
    expect(first.isError).toBeFalsy();
    expect(first.tasks).toHaveLength(3);
    expect(first.tasks.map((t: any) => t.content)).toEqual(['design', 'build', 'ship']);

    // Second FULL-list call RECONCILES by content: 'design' completed, 'build'
    // in_progress, 'ship' unchanged. No errors, no duplicate rows.
    const second = (await tools[TASK_WRITE_TOOL_ID]!.execute!(
      {
        tasks: [
          { content: 'design', status: 'completed' },
          { content: 'build', status: 'in_progress', activeForm: 'Building' },
          { content: 'ship', status: 'pending' },
        ],
      } as any,
      ctx,
    )) as any;
    expect(second.isError).toBeFalsy();
    const byContent = new Map(second.tasks.map((t: any) => [t.content, t]));
    expect((byContent.get('design') as any).status).toBe('completed');
    expect((byContent.get('build') as any).status).toBe('in_progress');

    // Durable: exactly 3 rows (no duplicates), matched-by-content updates applied.
    const stored = await session._internalStorage.listPlanTasks({ sessionId: session.id, limit: 50 });
    expect(stored.tasks).toHaveLength(3);
    const storedByContent = new Map(stored.tasks.map(t => [t.content, t]));
    expect(storedByContent.get('design')!.status).toBe('completed');
    expect(storedByContent.get('build')!.status).toBe('in_progress');

    release();
    await turn;
  });

  it('the lazily-seeded plan-task summary is OBSERVABLE to a display subscriber (Finding 2)', async () => {
    const { harness, agent } = setupHarness();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    // Populate durable plan tasks via a real mid-turn mutation.
    let release!: () => void;
    const holdUntil = new Promise<void>(r => (release = r));
    agent.enqueueRun({ holdUntil, finishReason: 'stop', text: 'done' });
    const turn = session.message({ content: 'go' });
    await vitestPoll(() => agent.streamCalls.length > 0);
    const tools = createPlanTaskTools(session);
    const ctx = {
      abortSignal: new AbortController().signal,
      agent: { toolCallId: 'tc-seed-obs', runId: 'mock-run' },
      requestContext: { get: () => undefined },
    } as any;
    const root = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content: 'root' } as any, ctx)) as any;
    await tools[TASK_ADD_TOOL_ID]!.execute!({ content: 'second-root' } as any, ctx);
    void root;
    release();
    await turn;

    // Simulate a freshly-hydrated session that has NOT computed the in-memory
    // summary yet (the durable tasks exist, but no mutation happened this run).
    (session as any)._planTaskSummary = undefined;
    (session as any)._planTaskSummarySeeded = false;

    // A display subscriber should see `planTasks` arrive WITHOUT any further event
    // — the async seed nudges it. Without the fix the first snapshot has planTasks
    // absent and nothing re-notifies until an unrelated event fires.
    const snapshots: Array<unknown> = [];
    const stop = session.subscribeDisplayState(s => snapshots.push((s as any).planTasks));

    await vitestPoll(() => snapshots.some(p => p !== undefined), 2000);
    stop();

    const seeded = snapshots.find(p => p !== undefined) as any;
    expect(seeded).toBeDefined();
    expect(seeded.total).toBe(2);
    expect(seeded.rootCount).toBe(2);
    expect(seeded.byStatus.pending).toBe(2);
  });

  it('a rejected mutation (cycle) returns isError, not a thrown turn abort', async () => {
    const { harness, agent } = setupHarness();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    let release!: () => void;
    const holdUntil = new Promise<void>(r => (release = r));
    agent.enqueueRun({ holdUntil, finishReason: 'stop', text: 'done' });
    const turn = session.message({ content: 'go' });
    await vitestPoll(() => agent.streamCalls.length > 0);

    const tools = createPlanTaskTools(session);
    const ctx = {
      abortSignal: new AbortController().signal,
      agent: { toolCallId: 'tc-c', runId: 'mock-run' },
      requestContext: { get: () => undefined },
    } as any;
    const a = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content: 'a' } as any, ctx)) as any;
    const b = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content: 'b', parentTaskId: a.taskId } as any, ctx)) as any;
    // Reparent a under its own child b → cycle → isError payload.
    const res = (await tools[TASK_REPARENT_TOOL_ID]!.execute!(
      { taskId: a.taskId, newParentTaskId: b.taskId } as any,
      ctx,
    )) as any;
    expect(res.isError).toBe(true);
    expect(res.errorName).toBe('HarnessPlanTaskCycleError');

    release();
    await turn;
  });
});

describe('plan-task concurrent-write serialization (§5.8 / TM-5)', () => {
  // A single model turn can emit parallel tool calls (the agent loop runs a
  // step's tool calls concurrently). Two `task_update` ops each driving a
  // DIFFERENT sibling `in_progress` under the SAME root both read the same
  // pre-image and both pass `assertSingleInProgress`. The storage fence keys on
  // the SESSION version (which plan-task writes never bump) and per-row OCC only
  // guards same-row writes, so without per-session serialization both commit and
  // break the per-root single-`in_progress` invariant. `_serializePlanTaskWrite`
  // runs each op (read + check + write) one-at-a-time on the flush chain, so the
  // second op's read sees the first's committed `in_progress` and is rejected.
  it('two concurrent task_update in_progress on sibling tasks under one root → exactly one wins', async () => {
    const { harness, agent } = setupHarness();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    let release!: () => void;
    const holdUntil = new Promise<void>(r => (release = r));
    agent.enqueueRun({ holdUntil, finishReason: 'stop', text: 'done' });
    const turn = session.message({ content: 'go' });
    await vitestPoll(() => agent.streamCalls.length > 0);

    const tools = createPlanTaskTools(session);
    const ctx = {
      abortSignal: new AbortController().signal,
      agent: { toolCallId: 'tc-seed', runId: 'mock-run' },
      requestContext: { get: () => undefined },
    } as any;
    // root with two pending sibling children.
    const root = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content: 'root' } as any, ctx)) as any;
    const [c1, c2] = await session._planTaskDecompose(root.taskId, [{ content: 'c1' }, { content: 'c2' }]);

    // Fire both in_progress writes concurrently against the SAME pre-image.
    const results = await Promise.allSettled([
      session._planTaskUpdate(c1.taskId, { status: 'in_progress' }),
      session._planTaskUpdate(c2.taskId, { status: 'in_progress' }),
    ]);

    const fulfilled = results.filter(r => r.status === 'fulfilled');
    const rejected = results.filter(r => r.status === 'rejected') as PromiseRejectedResult[];
    // Exactly one commits; the loser is rejected with the single-in_progress error.
    expect(fulfilled).toHaveLength(1);
    expect(rejected).toHaveLength(1);
    expect((rejected[0]!.reason as Error).name).toBe('HarnessPlanTaskInProgressConflictError');

    // Durable truth: exactly one child is in_progress under the root.
    const stored = await session._internalStorage.listPlanTasks({ sessionId: session.id, limit: 50 });
    const inProgress = stored.tasks.filter(t => t.status === 'in_progress' && t.statusSource === 'explicit');
    expect(inProgress).toHaveLength(1);

    release();
    await turn;
  });
});

/** Poll a predicate without a fixed sleep. */
async function vitestPoll(pred: () => boolean, timeoutMs = 2000): Promise<void> {
  const start = Date.now();
  while (!pred()) {
    if (Date.now() - start > timeoutMs) throw new Error('vitestPoll timeout');
    await new Promise(r => setTimeout(r, 5));
  }
}
