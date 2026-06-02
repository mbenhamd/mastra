/**
 * Harness v1 — PLAN-TASK real-agent E2E (NOT MockAgent, NOT direct tool.execute).
 *
 * Every other plan-task test either calls `tools[ID].execute(ctx)` with a
 * fabricated tool context, or drives `Session._planTask*` directly. That proves
 * the storage + hierarchy + surfacing seams in isolation, but it never exercises
 * the path that actually matters in production: a REAL model emitting a REAL
 * tool-call to a plan-task tool, the real ai-sdk loop resolving that tool out of
 * the harness-injected `harness:builtin` toolset, running it, feeding the result
 * back, and the resulting tree mutation surfacing as the `papersflow.plan_task.
 * updated` custom event + bounded `planTasks` display summary + durable rows.
 *
 * This file binds a REAL `Agent` (driven by a deterministic `MockLanguageModelV2`,
 * the provider-level mock the loop tests use) into a real `Harness` and asserts
 * the full round-trip end to end:
 *
 *   PT1  a real `task_add` tool-call mutates the durable tree, surfaces a
 *        `tool_start`/`tool_end` pair AND the `papersflow.plan_task.updated`
 *        custom event, and updates the bounded display summary — all through the
 *        genuine loop (the durable row + returned taskId prove the tool ran
 *        inside the real session, not a fabricated tool context).
 *   PT2  a real multi-step turn (add parent → add child → complete child) rolls
 *        the parent up to `completed` via the real loop, persisting the rollup.
 *   PT3  a real `task_delegate` tool-call spawns a REAL subagent session, links
 *        it durably (`delegatedSubagentSessionId`), and the plan task rolls up to
 *        `completed` once the subagent finishes — across the turn boundary.
 *   PT4  a plan tree built through the real loop survives a process restart: a
 *        fresh harness instance over the SAME store rehydrates the session and
 *        recovers the full tree + summary.
 *
 * Only the LANGUAGE MODEL is mocked; the agent + tools + loop are real.
 */

import { describe, expect, it } from 'vitest';

import { Agent } from '../../agent';
import { convertArrayToReadableStream, MockLanguageModelV2 } from '../../agent/__tests__/mock-model';
import { InMemoryStore } from '../../storage';
import { InMemoryHarness } from '../../storage/domains/harness/inmemory';
import { InMemoryDB } from '../../storage/domains/inmemory-db';

import type { HarnessEvent } from './events';
import { Harness } from './harness';

// ---------------------------------------------------------------------------
// Helpers (mirrors session.real-agent.e2e.test.ts)
// ---------------------------------------------------------------------------

const testUsage = { inputTokens: 10, outputTokens: 20, totalTokens: 30 };

/** A raw provider stream that emits text in N deltas then finishes with `stop`. */
function textStream(deltas: string[]) {
  return convertArrayToReadableStream([
    { type: 'stream-start', warnings: [] },
    { type: 'response-metadata', id: 'id-text', modelId: 'mock-model-id', timestamp: new Date(0) },
    { type: 'text-start', id: 'text-1' },
    ...deltas.map(delta => ({ type: 'text-delta', id: 'text-1', delta })),
    { type: 'text-end', id: 'text-1' },
    { type: 'finish', finishReason: 'stop', usage: testUsage },
  ]);
}

/** A raw provider stream that calls `toolName` with `inputJson`, finishing `tool-calls`. */
function toolCallStream(toolCallId: string, toolName: string, inputJson: string) {
  return convertArrayToReadableStream([
    { type: 'stream-start', warnings: [] },
    { type: 'response-metadata', id: `id-${toolCallId}`, modelId: 'mock-model-id', timestamp: new Date(0) },
    { type: 'tool-call', toolCallId, toolName, input: inputJson, providerExecuted: false },
    { type: 'finish', finishReason: 'tool-calls', usage: testUsage },
  ]);
}

/** Build a single-real-agent harness over a fresh InMemoryStore. */
function newHarness(agent: Agent<any, any, any>) {
  return new Harness({
    agents: { default: agent } as any,
    storage: new InMemoryStore(),
    modes: [{ id: 'default', agentId: 'default' }],
    defaultModeId: 'default',
  });
}

/** Build a single-real-agent harness whose session storage is backed by a SHARED db. */
function newSharedHarness(db: InMemoryDB, agent: Agent<any, any, any>) {
  return new Harness({
    agents: { default: agent } as any,
    modes: [{ id: 'default', agentId: 'default' }],
    defaultModeId: 'default',
    sessions: { storage: new InMemoryHarness({ db }) },
  });
}

async function poll(pred: () => boolean | Promise<boolean>, timeoutMs = 3000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (true) {
    if (await pred()) return;
    if (Date.now() > deadline) throw new Error('poll timed out');
    await new Promise(r => setTimeout(r, 10));
  }
}

type ToolEndEvent = { type: 'tool_end'; toolName: string; output: any; isError: boolean };

// ===========================================================================
// PT1 — real task_add tool-call → tree + custom event + display + durable row
// ===========================================================================

describe('Harness v1 plan-task E2E — PT1 real task_add round-trip', () => {
  it('a real task_add tool-call mutates the durable tree, emits papersflow.plan_task.updated, and updates the bounded display summary', async () => {
    let call = 0;
    const model = new MockLanguageModelV2({
      doStream: async () => {
        call++;
        if (call === 1) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream('pt-add-1', 'task_add', JSON.stringify({ content: 'Write the paper' })),
          };
        }
        return { rawCall: { rawPrompt: null, rawSettings: {} }, warnings: [], stream: textStream(['Added.']) };
      },
    });
    const agent = new Agent({ id: 'default', name: 'default', instructions: 'plan the work', model });
    const harness = newHarness(agent);
    try {
      const session = await harness.session({ resourceId: 'u-pt1', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));

      const result = (await session.message({ content: 'plan it' })) as any;

      // The real loop resolved the harness-injected task_add tool and ran it.
      expect(result.toolCalls.some((c: any) => (c.toolName ?? c.payload?.toolName) === 'task_add')).toBe(true);

      const toolEnd = events.find(e => e.type === 'tool_end' && (e as any).toolName === 'task_add') as
        | ToolEndEvent
        | undefined;
      expect(toolEnd).toBeDefined();
      expect(toolEnd!.isError).toBe(false);
      const taskId = toolEnd!.output.taskId as string;
      expect(typeof taskId).toBe('string');
      expect(taskId.length).toBeGreaterThan(0);

      // The custom plan-task event surfaced through the live turn with a real delta.
      // (The op/affectedTaskIds/deltas ride under the nested `payload`, §10.3.)
      const custom = events.find(e => e.type === ('papersflow.plan_task.updated' as any)) as any;
      expect(custom).toBeDefined();
      expect(custom.payload.op).toBe('add');
      expect(custom.payload.affectedTaskIds).toContain(taskId);
      expect(Array.isArray(custom.payload.deltas)).toBe(true);
      expect(custom.payload.deltas.length).toBeGreaterThan(0);

      // The bounded display summary reflects exactly one pending root (NOT a full tree).
      const summary = session.getDisplayState().planTasks!;
      expect(summary.total).toBe(1);
      expect(summary.rootCount).toBe(1);
      expect(summary.byStatus.pending).toBe(1);
      expect(Object.keys(summary).sort()).toEqual(['byStatus', 'inProgressTaskIds', 'rootCount', 'total']);

      // Durable: the row is persisted under the session, content intact.
      const page = await (session as any)._internalStorage.listPlanTasks({ sessionId: session.id, limit: 10 });
      expect(page.tasks).toHaveLength(1);
      expect(page.tasks[0].content).toBe('Write the paper');
      expect(page.tasks[0].taskId).toBe(taskId);
    } finally {
      await harness.shutdown();
    }
  });
});

// ===========================================================================
// PT2 — real multi-step turn rolls a parent up to completed (durable)
// ===========================================================================

describe('Harness v1 plan-task E2E — PT2 multi-step rollup through the real loop', () => {
  it('add parent → add child → complete child rolls the parent up to completed and persists it', async () => {
    const createdIds: string[] = [];
    let call = 0;
    const model = new MockLanguageModelV2({
      doStream: async () => {
        call++;
        if (call === 1) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream('pt2-parent', 'task_add', JSON.stringify({ content: 'Parent' })),
          };
        }
        if (call === 2) {
          // Child nested under the parent created in step 1 (captured from its tool_end).
          const parentId = createdIds[0];
          expect(parentId).toBeDefined();
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream(
              'pt2-child',
              'task_add',
              JSON.stringify({ content: 'Child', parentTaskId: parentId }),
            ),
          };
        }
        if (call === 3) {
          const childId = createdIds[1];
          expect(childId).toBeDefined();
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream('pt2-complete', 'task_complete', JSON.stringify({ taskId: childId })),
          };
        }
        return { rawCall: { rawPrompt: null, rawSettings: {} }, warnings: [], stream: textStream(['Done.']) };
      },
    });
    const agent = new Agent({ id: 'default', name: 'default', instructions: 'build a plan', model });
    const harness = newHarness(agent);
    try {
      const session = await harness.session({ resourceId: 'u-pt2', threadId: { fresh: true } });
      session.subscribe(e => {
        if (e.type === 'tool_end' && (e as any).toolName === 'task_add' && (e as any).output?.taskId) {
          createdIds.push((e as any).output.taskId);
        }
      });

      await session.message({ content: 'plan and finish the child' });

      expect(createdIds).toHaveLength(2);
      const [parentId, childId] = createdIds;

      // Durable rollup: single completed child → parent rolls up to completed (derived).
      const page = await (session as any)._internalStorage.listPlanTasks({ sessionId: session.id, limit: 10 });
      const byId = new Map<string, any>(page.tasks.map((t: any) => [t.taskId, t]));
      expect(byId.get(childId).status).toBe('completed');
      expect(byId.get(parentId).status).toBe('completed');
      expect(byId.get(parentId).statusSource).toBe('derived');

      const summary = session.getDisplayState().planTasks!;
      expect(summary.total).toBe(2);
      expect(summary.byStatus.completed).toBe(2);
    } finally {
      await harness.shutdown();
    }
  });
});

// ===========================================================================
// PT3 — real task_delegate → real subagent → durable rollup across the turn
// ===========================================================================

describe('Harness v1 plan-task E2E — PT3 delegation to a real subagent', () => {
  it('a real task_delegate tool-call spawns a real subagent, links it durably, and rolls the task up to completed', async () => {
    const createdIds: string[] = [];

    // REAL subagent: a model that just produces text and finishes `stop`.
    const childModel = new MockLanguageModelV2({
      doStream: async () => ({
        rawCall: { rawPrompt: null, rawSettings: {} },
        warnings: [],
        stream: textStream(['Subtask ', 'handled.']),
      }),
    });
    const childAgent = new Agent({
      id: 'child-agent',
      name: 'child-agent',
      instructions: 'do the subtask',
      model: childModel,
    });

    // REAL parent: add a task (step 1), then delegate it (step 2), then finish.
    let parentCall = 0;
    const parentModel = new MockLanguageModelV2({
      doStream: async () => {
        parentCall++;
        if (parentCall === 1) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream('pt3-add', 'task_add', JSON.stringify({ content: 'Delegate this' })),
          };
        }
        if (parentCall === 2) {
          const taskId = createdIds[0];
          expect(taskId).toBeDefined();
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream('pt3-delegate', 'task_delegate', JSON.stringify({ taskId, agentType: 'worker' })),
          };
        }
        return { rawCall: { rawPrompt: null, rawSettings: {} }, warnings: [], stream: textStream(['Delegated.']) };
      },
    });
    const parentAgent = new Agent({
      id: 'parent-agent',
      name: 'parent-agent',
      instructions: 'delegate the task',
      model: parentModel,
    });

    const harness = new Harness({
      agents: { 'parent-agent': parentAgent, 'child-agent': childAgent } as any,
      storage: new InMemoryStore(),
      modes: [
        { id: 'default', agentId: 'parent-agent' },
        { id: 'worker-mode', agentId: 'child-agent' },
      ],
      defaultModeId: 'default',
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
    try {
      const session = await harness.session({ resourceId: 'u-pt3', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => {
        events.push(e);
        if (e.type === 'tool_end' && (e as any).toolName === 'task_add' && (e as any).output?.taskId) {
          createdIds.push((e as any).output.taskId);
        }
      });

      await session.message({ content: 'delegate the work' });

      expect(createdIds).toHaveLength(1);
      const taskId = createdIds[0];

      // The real loop ran the task_delegate tool (delegation is DETACHED — unlike
      // inline spawn_subagent it does NOT block the parent turn, so its contract is
      // the durable link + cross-turn rollup below, not an inline subagent_* event).
      const delegateEnd = events.find(e => e.type === 'tool_end' && (e as any).toolName === 'task_delegate') as
        | ToolEndEvent
        | undefined;
      expect(delegateEnd).toBeDefined();
      expect(delegateEnd!.isError).toBe(false);

      // The plan task is durably linked to its delegated subagent session.
      const linked = async () => {
        const page = await (session as any)._internalStorage.listPlanTasks({ sessionId: session.id, limit: 10 });
        return page.tasks.find((t: any) => t.taskId === taskId);
      };
      await poll(async () => {
        const t = await linked();
        return typeof t?.delegatedSubagentSessionId === 'string' && t.delegatedSubagentSessionId.length > 0;
      });

      // Once the subagent finishes, the delegated task rolls up to completed (across the turn).
      await poll(async () => (await linked())?.status === 'completed');
      const final = await linked();
      expect(final.status).toBe('completed');
    } finally {
      await harness.shutdown();
    }
  });
});

// ===========================================================================
// PT4 — a real-loop plan tree survives a process restart (cross-instance)
// ===========================================================================

describe('Harness v1 plan-task E2E — PT4 recovery across a process restart', () => {
  it('a plan tree built through the real loop is recovered by a fresh harness instance over the same store', async () => {
    const db = new InMemoryDB();
    const createdIds: string[] = [];

    let call = 0;
    const buildModel = () =>
      new MockLanguageModelV2({
        doStream: async () => {
          call++;
          if (call === 1) {
            return {
              rawCall: { rawPrompt: null, rawSettings: {} },
              warnings: [],
              stream: toolCallStream('pt4-a', 'task_add', JSON.stringify({ content: 'Task A' })),
            };
          }
          if (call === 2) {
            return {
              rawCall: { rawPrompt: null, rawSettings: {} },
              warnings: [],
              stream: toolCallStream(
                'pt4-b',
                'task_add',
                JSON.stringify({ content: 'Task B', parentTaskId: createdIds[0] }),
              ),
            };
          }
          return { rawCall: { rawPrompt: null, rawSettings: {} }, warnings: [], stream: textStream(['Built.']) };
        },
      });

    const agentA = new Agent({ id: 'default', name: 'default', instructions: 'build', model: buildModel() });
    const harnessA = newSharedHarness(db, agentA);
    let sessionId: string;
    try {
      const sessionA = await harnessA.session({ resourceId: 'u-pt4', threadId: { fresh: true } });
      sessionA.subscribe(e => {
        if (e.type === 'tool_end' && (e as any).toolName === 'task_add' && (e as any).output?.taskId) {
          createdIds.push((e as any).output.taskId);
        }
      });
      await sessionA.message({ content: 'build a small plan' });
      sessionId = sessionA.id;
      expect(createdIds).toHaveLength(2);
    } finally {
      // Simulate a process exit — drop the in-memory harness (the db survives).
      await harnessA.shutdown();
    }

    // Fresh process: a brand-new harness instance over the SAME backing db.
    const agentB = new Agent({ id: 'default', name: 'default', instructions: 'build', model: buildModel() });
    const harnessB = newSharedHarness(db, agentB);
    try {
      const sessionB = await harnessB.session({ sessionId, resourceId: 'u-pt4' });
      const page = await (sessionB as any)._internalStorage.listPlanTasks({ sessionId, limit: 10 });
      expect(page.tasks).toHaveLength(2);
      const contents = page.tasks.map((t: any) => t.content).sort();
      expect(contents).toEqual(['Task A', 'Task B']);

      // Recovery must surface through the rehydrated session's DISPLAY PROJECTION,
      // not just raw storage: the bounded summary is lazy-seeded on first read, so
      // poll until it populates, then assert the recovered shape (parent + child).
      await poll(() => sessionB.getDisplayState().planTasks !== undefined);
      const summary = sessionB.getDisplayState().planTasks!;
      expect(summary.total).toBe(2);
      expect(summary.rootCount).toBe(1);
    } finally {
      await harnessB.shutdown();
    }
  });
});

// ===========================================================================
// PT5 — decompose + plan_task_check + reparent + update through the real loop
//        (rounds out real-loop coverage of the rest of the plan-task tool surface)
// ===========================================================================

describe('Harness v1 plan-task E2E — PT5 decompose / check / reparent / update', () => {
  it('drives task_decompose, plan_task_check, task_reparent and task_update through the real agent loop', async () => {
    let rootId: string | undefined;
    let childIds: string[] = [];
    let checkTaskIds: string[] = [];
    const erroredTools: string[] = [];

    let call = 0;
    const model = new MockLanguageModelV2({
      doStream: async () => {
        call++;
        if (call === 1) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream('pt5-add', 'task_add', JSON.stringify({ content: 'Root' })),
          };
        }
        if (call === 2) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream(
              'pt5-decompose',
              'task_decompose',
              JSON.stringify({ parentTaskId: rootId, children: [{ content: 'Step 1' }, { content: 'Step 2' }] }),
            ),
          };
        }
        if (call === 3) {
          // Re-orient: a real plan_task_check read under the root.
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream('pt5-check', 'plan_task_check', JSON.stringify({ rootTaskId: rootId })),
          };
        }
        if (call === 4) {
          // Promote Step 1 to a root.
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream(
              'pt5-reparent',
              'task_reparent',
              JSON.stringify({ taskId: childIds[0], newParentTaskId: null }),
            ),
          };
        }
        if (call === 5) {
          // Complete Step 2 via task_update (not task_complete).
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream(
              'pt5-update',
              'task_update',
              JSON.stringify({ taskId: childIds[1], status: 'completed' }),
            ),
          };
        }
        return { rawCall: { rawPrompt: null, rawSettings: {} }, warnings: [], stream: textStream(['Done.']) };
      },
    });
    const agent = new Agent({ id: 'default', name: 'default', instructions: 'shape the plan', model });
    const harness = newHarness(agent);
    try {
      const session = await harness.session({ resourceId: 'u-pt5', threadId: { fresh: true } });
      session.subscribe(e => {
        if (e.type !== 'tool_end') return;
        const ev = e as any;
        if (String(ev.toolName).startsWith('task_') || ev.toolName === 'plan_task_check') {
          if (ev.isError) erroredTools.push(ev.toolName);
        }
        if (ev.toolName === 'task_add' && ev.output?.taskId) rootId = ev.output.taskId;
        if (ev.toolName === 'task_decompose' && Array.isArray(ev.output?.children)) {
          childIds = ev.output.children.map((c: any) => c.taskId);
        }
        if (ev.toolName === 'plan_task_check' && Array.isArray(ev.output?.tasks)) {
          checkTaskIds = ev.output.tasks.map((t: any) => t.taskId);
        }
      });

      await session.message({ content: 'build, inspect, and reshape the plan' });

      expect(rootId).toBeDefined();
      expect(childIds).toHaveLength(2);
      // No plan-task tool errored through the real loop.
      expect(erroredTools).toEqual([]);
      // plan_task_check returned the bounded slice under the root: the root itself
      // plus both decomposed children (proves rootTaskId filtering, not just "an array").
      expect(checkTaskIds).toEqual(expect.arrayContaining([rootId, childIds[0], childIds[1]]));

      const page = await (session as any)._internalStorage.listPlanTasks({ sessionId: session.id, limit: 10 });
      const byId = new Map<string, any>(page.tasks.map((t: any) => [t.taskId, t]));
      expect(page.tasks).toHaveLength(3);
      // Step 1 was reparented to the root (parent cleared).
      expect(byId.get(childIds[0]).parentTaskId).toBeUndefined();
      // Step 2 was completed via task_update.
      expect(byId.get(childIds[1]).status).toBe('completed');

      // Two roots now (Root + the promoted Step 1).
      const summary = session.getDisplayState().planTasks!;
      expect(summary.total).toBe(3);
      expect(summary.rootCount).toBe(2);
    } finally {
      await harness.shutdown();
    }
  });
});

// ===========================================================================
// PT6 — plan-task trees are ISOLATED per session (no cross-session bleed)
// ===========================================================================

describe('Harness v1 plan-task E2E — PT6 per-session isolation', () => {
  it('two sessions in the same harness keep independent plan trees (list, count, display)', async () => {
    // One agent, driven deterministically by call order: session A runs first
    // (adds "A-task"), then session B (adds "B-task" + a second "B-extra").
    let call = 0;
    const model = new MockLanguageModelV2({
      doStream: async () => {
        call++;
        if (call === 1)
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream('a-add', 'task_add', JSON.stringify({ content: 'A-task' })),
          };
        if (call === 2)
          return { rawCall: { rawPrompt: null, rawSettings: {} }, warnings: [], stream: textStream(['A done']) };
        if (call === 3)
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream('b-add', 'task_add', JSON.stringify({ content: 'B-task' })),
          };
        if (call === 4)
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream('b-add2', 'task_add', JSON.stringify({ content: 'B-extra' })),
          };
        return { rawCall: { rawPrompt: null, rawSettings: {} }, warnings: [], stream: textStream(['B done']) };
      },
    });
    const agent = new Agent({ id: 'default', name: 'default', instructions: 'plan', model });
    const harness = newHarness(agent);
    try {
      const sessionA = await harness.session({ resourceId: 'iso-a', threadId: { fresh: true } });
      const sessionB = await harness.session({ resourceId: 'iso-b', threadId: { fresh: true } });
      const aEvents: HarnessEvent[] = [];
      const bEvents: HarnessEvent[] = [];
      sessionA.subscribe(e => aEvents.push(e));
      sessionB.subscribe(e => bEvents.push(e));

      await sessionA.message({ content: 'plan A' });
      await sessionB.message({ content: 'plan B' });

      // Storage is scoped per session: A sees only its task, B sees only its two.
      const aPage = await (sessionA as any)._internalStorage.listPlanTasks({ sessionId: sessionA.id, limit: 10 });
      const bPage = await (sessionB as any)._internalStorage.listPlanTasks({ sessionId: sessionB.id, limit: 10 });
      expect(aPage.tasks.map((t: any) => t.content)).toEqual(['A-task']);
      expect(bPage.tasks.map((t: any) => t.content).sort()).toEqual(['B-extra', 'B-task']);

      // Bounded display summaries don't bleed across sessions.
      expect(sessionA.getDisplayState().planTasks!.total).toBe(1);
      expect(sessionB.getDisplayState().planTasks!.total).toBe(2);

      // Each session's custom plan-task events reference ONLY its own task ids —
      // neither stream leaks the other session's tasks.
      const aTaskIds = new Set<string>(aPage.tasks.map((t: any) => t.taskId));
      const bTaskIds = new Set<string>(bPage.tasks.map((t: any) => t.taskId));
      const customsFor = (events: HarnessEvent[]) =>
        events.filter(e => e.type === ('papersflow.plan_task.updated' as any)) as any[];
      const aCustoms = customsFor(aEvents);
      const bCustoms = customsFor(bEvents);
      expect(aCustoms.length).toBeGreaterThan(0);
      expect(bCustoms.length).toBeGreaterThan(0);
      for (const c of aCustoms) for (const id of c.payload.affectedTaskIds) expect(aTaskIds.has(id)).toBe(true);
      for (const c of bCustoms) for (const id of c.payload.affectedTaskIds) expect(bTaskIds.has(id)).toBe(true);
    } finally {
      await harness.shutdown();
    }
  });
});

// ===========================================================================
// PT7 — a plan tree survives a GRACEFUL session.close() and reopens intact
//        (distinct from PT4's crash/shutdown path — close() marks the session
//        closed, so this guards reopen-after-close)
// ===========================================================================

describe('Harness v1 plan-task E2E — PT7 graceful close + reopen', () => {
  it('persists a plan tree across an explicit session.close(), then reopens the SAME (closed) session and stays writable', async () => {
    const db = new InMemoryDB();

    // Session A adds one task, then is closed.
    let callA = 0;
    const modelA = new MockLanguageModelV2({
      doStream: async () => {
        callA++;
        if (callA === 1)
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream('pt7-add', 'task_add', JSON.stringify({ content: 'Survive close' })),
          };
        return { rawCall: { rawPrompt: null, rawSettings: {} }, warnings: [], stream: textStream(['ok']) };
      },
    });
    const harnessA = newSharedHarness(
      db,
      new Agent({ id: 'default', name: 'default', instructions: 'plan', model: modelA }),
    );
    let sessionId: string;
    try {
      const sessionA = await harnessA.session({ resourceId: 'u-pt7', threadId: { fresh: true } });
      await sessionA.message({ content: 'add one task' });
      sessionId = sessionA.id;
      // GRACEFUL close (not a crash).
      await sessionA.close();
      // The close marker is actually persisted (so the reopen path below is genuinely
      // a reopen-AFTER-close, not a hydrate of a still-active record).
      const closedRec = await (sessionA as any)._internalStorage.loadSession({ sessionId });
      expect(closedRec).not.toBeNull();
      expect(typeof closedRec.closedAt).toBe('number');
      // close() does not delete the tree.
      const afterClose = await (sessionA as any)._internalStorage.listPlanTasks({ sessionId, limit: 10 });
      expect(afterClose.tasks.map((t: any) => t.content)).toEqual(['Survive close']);
    } finally {
      await harnessA.shutdown();
    }

    // Fresh harness; session B adds a SECOND task after reopen (proving it is writable).
    let callB = 0;
    const modelB = new MockLanguageModelV2({
      doStream: async () => {
        callB++;
        if (callB === 1)
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream('pt7-add2', 'task_add', JSON.stringify({ content: 'After reopen' })),
          };
        return { rawCall: { rawPrompt: null, rawSettings: {} }, warnings: [], stream: textStream(['ok']) };
      },
    });
    const harnessB = newSharedHarness(
      db,
      new Agent({ id: 'default', name: 'default', instructions: 'plan', model: modelB }),
    );
    try {
      const sessionB = await harnessB.session({ sessionId, resourceId: 'u-pt7' });
      // It is the SAME session, brought back to a live state (reopen clears closedAt).
      expect(sessionB.id).toBe(sessionId);
      expect(sessionB.lifecycleState).toBe('live');
      const reopenedRec = await (sessionB as any)._internalStorage.loadSession({ sessionId });
      expect(reopenedRec.closedAt).toBeUndefined();

      // The pre-close tree recovered + surfaces through the display projection.
      const recovered = await (sessionB as any)._internalStorage.listPlanTasks({ sessionId, limit: 10 });
      expect(recovered.tasks.map((t: any) => t.content)).toEqual(['Survive close']);
      await poll(() => sessionB.getDisplayState().planTasks !== undefined);
      expect(sessionB.getDisplayState().planTasks!.total).toBe(1);

      // The reopened session is writable: a new turn adds a second task.
      await sessionB.message({ content: 'add another task' });
      const both = await (sessionB as any)._internalStorage.listPlanTasks({ sessionId, limit: 10 });
      expect(both.tasks.map((t: any) => t.content).sort()).toEqual(['After reopen', 'Survive close']);
      expect(sessionB.getDisplayState().planTasks!.total).toBe(2);
    } finally {
      await harnessB.shutdown();
    }
  });
});

// ===========================================================================
// PT8 — CONCURRENT two-session isolation (the race PT6 left uncovered)
//        A BARRIER forces both sessions' first model turns to be simultaneously
//        in flight before either proceeds, so this fails (deadlocks → times out)
//        if the harness serialized the two turns — i.e. it actually proves the
//        loops interleave, not just that two awaited turns happen to end clean.
// ===========================================================================

describe('Harness v1 plan-task E2E — PT8 concurrent per-session isolation', () => {
  it('two sessions building plans CONCURRENTLY (proven overlapping) keep independent trees', async () => {
    // Barrier: the first model turn of each session blocks until BOTH have arrived,
    // guaranteeing real overlap. If the runtime serialized sessions, the second
    // turn would never start and `await barrier` would time out → the test fails.
    let arrived = 0;
    let releaseBarrier!: () => void;
    const barrier = new Promise<void>(r => (releaseBarrier = r));
    let bothInFlight = false;
    const keyOrder: string[] = [];

    const model = new MockLanguageModelV2({
      doStream: async (options: any) => {
        const promptStr = JSON.stringify(options?.prompt ?? '');
        const hasAlpha = promptStr.includes('alpha');
        const hasBeta = promptStr.includes('beta');
        // Self-validating: every call must carry EXACTLY one session key.
        if (hasAlpha === hasBeta) throw new Error(`PT8 prompt must contain exactly one key, got: ${promptStr.slice(0, 200)}`);
        const key = hasAlpha ? 'alpha' : 'beta';
        const firstTurn = !keyOrder.includes(key);

        if (firstTurn) {
          keyOrder.push(key);
          arrived += 1;
          if (arrived === 2) {
            bothInFlight = true;
            releaseBarrier();
          }
          // Block until the OTHER session's first turn is also in flight (or bail
          // loudly if the runtime serialized us and the partner never arrives).
          await Promise.race([
            barrier,
            new Promise<void>((_, reject) => setTimeout(() => reject(new Error('PT8 barrier timed out — turns did not overlap (serialized?)')), 2000)),
          ]);
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream(`tc-${key}`, 'task_add', JSON.stringify({ content: `${key}-task` })),
          };
        }
        return { rawCall: { rawPrompt: null, rawSettings: {} }, warnings: [], stream: textStream(['done']) };
      },
    });
    const agent = new Agent({ id: 'default', name: 'default', instructions: 'plan', model });
    const harness = newHarness(agent);
    try {
      const [sessionA, sessionB] = await Promise.all([
        harness.session({ resourceId: 'cc-a', threadId: { fresh: true } }),
        harness.session({ resourceId: 'cc-b', threadId: { fresh: true } }),
      ]);
      const aEvents: HarnessEvent[] = [];
      const bEvents: HarnessEvent[] = [];
      sessionA.subscribe(e => aEvents.push(e));
      sessionB.subscribe(e => bEvents.push(e));

      await Promise.all([sessionA.message({ content: 'plan alpha' }), sessionB.message({ content: 'plan beta' })]);

      // The barrier was reached by BOTH first turns → they genuinely overlapped.
      expect(bothInFlight).toBe(true);
      expect(keyOrder.sort()).toEqual(['alpha', 'beta']);

      const aPage = await (sessionA as any)._internalStorage.listPlanTasks({ sessionId: sessionA.id, limit: 10 });
      const bPage = await (sessionB as any)._internalStorage.listPlanTasks({ sessionId: sessionB.id, limit: 10 });
      // Each session got exactly its own task — no cross-contamination under interleave.
      expect(aPage.tasks.map((t: any) => t.content)).toEqual(['alpha-task']);
      expect(bPage.tasks.map((t: any) => t.content)).toEqual(['beta-task']);
      expect(sessionA.getDisplayState().planTasks!.total).toBe(1);
      expect(sessionB.getDisplayState().planTasks!.total).toBe(1);

      // Custom events are isolated too — neither stream references the other's task.
      const aTaskIds = new Set<string>(aPage.tasks.map((t: any) => t.taskId));
      const bTaskIds = new Set<string>(bPage.tasks.map((t: any) => t.taskId));
      const customs = (evs: HarnessEvent[]) => evs.filter(e => e.type === ('papersflow.plan_task.updated' as any)) as any[];
      const aCustoms = customs(aEvents);
      const bCustoms = customs(bEvents);
      expect(aCustoms.length).toBeGreaterThan(0);
      expect(bCustoms.length).toBeGreaterThan(0);
      for (const c of aCustoms) for (const id of c.payload.affectedTaskIds) expect(aTaskIds.has(id)).toBe(true);
      for (const c of bCustoms) for (const id of c.payload.affectedTaskIds) expect(bTaskIds.has(id)).toBe(true);
    } finally {
      await harness.shutdown();
    }
  });
});

// ===========================================================================
// PT9 — a REJECTED plan-task mutation returns an isError tool payload through
//        the real loop WITHOUT aborting the turn (§15.2), and leaves the tree
//        uncorrupted so the agent can recover and continue.
// ===========================================================================

describe('Harness v1 plan-task E2E — PT9 rejected mutation surfaces isError, turn survives', () => {
  it('a cycle-creating task_reparent returns isError mid-turn and the turn still completes with the tree intact', async () => {
    let rootId: string | undefined;
    let call = 0;
    const model = new MockLanguageModelV2({
      doStream: async () => {
        call++;
        if (call === 1) return { rawCall: { rawPrompt: null, rawSettings: {} }, warnings: [], stream: toolCallStream('pt9-add', 'task_add', JSON.stringify({ content: 'Root' })) };
        // Illegal: make the task its own parent → cycle → must be REJECTED (isError),
        // not throw and abort the turn.
        if (call === 2) return { rawCall: { rawPrompt: null, rawSettings: {} }, warnings: [], stream: toolCallStream('pt9-cycle', 'task_reparent', JSON.stringify({ taskId: rootId, newParentTaskId: rootId })) };
        return { rawCall: { rawPrompt: null, rawSettings: {} }, warnings: [], stream: textStream(['Recovered after rejection.']) };
      },
    });
    const agent = new Agent({ id: 'default', name: 'default', instructions: 'plan', model });
    const harness = newHarness(agent);
    try {
      const session = await harness.session({ resourceId: 'u-pt9', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => {
        events.push(e);
        if (e.type === 'tool_end' && (e as any).toolName === 'task_add' && (e as any).output?.taskId) rootId = (e as any).output.taskId;
      });

      const result = (await session.message({ content: 'add then illegally reparent' })) as any;

      // The rejection travels as an `isError` PAYLOAD (§15.2 / spawn_subagent
      // convention), NOT as a thrown tool error: the plan-task tool catches the
      // cycle and returns `{ isError: true, errorName }`, so the LOOP-level
      // tool_end.isError stays false while the OUTPUT carries the rejection.
      const reparentEnd = events.find(e => e.type === 'tool_end' && (e as any).toolName === 'task_reparent') as
        | (ToolEndEvent & { output: any })
        | undefined;
      expect(reparentEnd).toBeDefined();
      expect(reparentEnd!.isError).toBe(false); // the tool did not THROW
      expect(reparentEnd!.output.isError).toBe(true); // ...but the mutation was REJECTED
      expect(String(reparentEnd!.output.errorName)).toMatch(/Cycle/);

      // The turn was NOT aborted — it ran to a normal completion after the rejection.
      expect(result.text).toContain('Recovered');

      // The tree is uncorrupted: the root is still a root (reparent did not partially apply).
      const page = await (session as any)._internalStorage.listPlanTasks({ sessionId: session.id, limit: 10 });
      expect(page.tasks).toHaveLength(1);
      expect(page.tasks[0].taskId).toBe(rootId);
      expect(page.tasks[0].parentTaskId).toBeUndefined();
    } finally {
      await harness.shutdown();
    }
  });
});
