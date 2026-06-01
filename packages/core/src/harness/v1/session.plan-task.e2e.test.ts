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
