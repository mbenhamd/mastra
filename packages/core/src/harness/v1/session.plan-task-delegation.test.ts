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
import { z } from 'zod';

import { RequestContext } from '../../request-context';
import { InMemoryHarness } from '../../storage/domains/harness/inmemory';
import { InMemoryDB } from '../../storage/domains/inmemory-db';
import { createTool } from '../../tools';

import { MockAgent } from './__test-utils__';
import { sha256CanonicalJson } from './canonical-json';
import { Harness } from './harness';
import {
  createPlanTaskTools,
  TASK_ADD_TOOL_ID,
  TASK_CHECK_TOOL_ID,
  TASK_COMPLETE_TOOL_ID,
  TASK_DECOMPOSE_TOOL_ID,
  TASK_DELEGATE_TOOL_ID,
  TASK_UPDATE_TOOL_ID,
} from './plan-task-tool';
import { MAX_INHERITED_REQUEST_CONTEXT_APP_BYTES } from './request-context-input';
import { Session } from './session';
import {
  HARNESS_SUBAGENT_OUTCOME_REPORT_KIND,
  HARNESS_SUBAGENT_OUTCOME_REPORT_TOOL_ID,
} from './terminal-subagent-result';
import type { HarnessSubagentOutcomeReport } from './terminal-subagent-result';

// ---------------------------------------------------------------------------
// Setup
// ---------------------------------------------------------------------------

function outcomeTerminalResult(
  outcome: 'completed' | 'blocked' | 'failed' = 'completed',
  summary = 'Delegated test work completed',
  evidence: HarnessSubagentOutcomeReport['evidence'] = [
    { kind: 'analysis', description: 'Verified by the delegated test fixture' },
  ],
) {
  return {
    status: 'success' as const,
    items: [
      {
        toolName: HARNESS_SUBAGENT_OUTCOME_REPORT_TOOL_ID,
        toolCallId: 'report-call-1',
        status: 'success' as const,
        value: {
          kind: HARNESS_SUBAGENT_OUTCOME_REPORT_KIND,
          outcome,
          summary,
          evidence,
          ...(outcome === 'completed'
            ? {}
            : {
                issue: {
                  code: outcome === 'blocked' ? 'compiler.service_unavailable' : 'delegated.test_failed',
                  message: outcome === 'blocked' ? 'Compiler service unavailable' : 'Delegated fixture failed',
                  retryable: outcome === 'blocked',
                },
              }),
        },
      },
    ],
  };
}

function buildHarness(
  db: InMemoryDB,
  opts: {
    maxConcurrent?: number;
    delegationTimeoutMs?: number;
    maxLive?: number;
    allowInline?: boolean;
    inheritRequestContextAppKeys?: string[];
  } = {},
): { harness: Harness; parentAgent: MockAgent; childAgent: MockAgent } {
  const parentAgent = new MockAgent({ id: 'parent-agent' });
  const childAgent = new MockAgent({
    id: 'child-agent',
    defaultOutput: { terminalToolResult: outcomeTerminalResult() },
  });
  const storage = new InMemoryHarness({ db });
  const harness = new Harness({
    agents: { 'parent-agent': parentAgent, 'child-agent': childAgent } as any,
    modes: [
      { id: 'default', agentId: 'parent-agent' },
      { id: 'worker-mode', agentId: 'child-agent' },
    ],
    defaultModeId: 'default',
    sessions: { storage, ...(opts.maxLive !== undefined ? { maxLive: opts.maxLive } : {}) },
    subagents: {
      maxDepth: 2,
      ...(opts.maxConcurrent !== undefined ? { maxConcurrent: opts.maxConcurrent } : {}),
      ...(opts.delegationTimeoutMs !== undefined ? { delegationTimeoutMs: opts.delegationTimeoutMs } : {}),
      ...(opts.inheritRequestContextAppKeys !== undefined
        ? { inheritRequestContextAppKeys: opts.inheritRequestContextAppKeys }
        : {}),
      types: {
        worker: {
          agentId: 'child-agent',
          modeId: 'worker-mode',
          description: 'Background worker subagent',
          defaultModelId: 'openai/gpt-4o-mini',
          workspace: 'inherit',
          ...(opts.allowInline !== undefined ? { allowInline: opts.allowInline } : {}),
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

function lineageValueForCanonicalAppBytes(bytes: number): string {
  const emptyEnvelopeBytes = new TextEncoder().encode(JSON.stringify({ turnCorrelationId: '' })).byteLength;
  const value = 'x'.repeat(bytes - emptyEnvelopeBytes);
  expect(new TextEncoder().encode(JSON.stringify({ turnCorrelationId: value }))).toHaveLength(bytes);
  return value;
}

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
  it('rejects a non-positive durable delegation timeout', () => {
    expect(() => buildHarness(new InMemoryDB(), { delegationTimeoutMs: 0 })).toThrow(/delegationTimeoutMs/);
  });

  it('is registered only when subagent types exist', async () => {
    const { harness } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    expect(createPlanTaskTools(parent)[TASK_DELEGATE_TOOL_ID]).toBeDefined();
  });

  it('keeps durable-only subagent types available to task_delegate', async () => {
    const { harness } = buildHarness(new InMemoryDB(), { allowInline: false });
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

  it('fails child materialization at maxLive without pressure-evicting its active parent', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB(), { maxLive: 1 });
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const close = await openParentTurn(parent, parentAgent);
    const tools = createPlanTaskTools(parent);
    const task = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content: 'cannot fit child' } as any, toolCtx)) as any;

    const delegated = (await tools[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: task.taskId, agentType: 'worker' } as any,
      toolCtx,
    )) as any;
    expect(delegated.isError).toBe(true);
    expect(parent.lifecycleState).toBe('live');
    expect(parent.isRunning()).toBe(true);
    expect(childAgent.streamCalls).toHaveLength(0);
    expect(parent._internalSubagentExecutionsInFlight).toBe(0);
    const failed = await planRow(parent, task.taskId);
    expect(failed?.status).toBe('failed');
    expect(failed?.delegatedSubagentSessionId).toBeUndefined();
    close();
    await harness.shutdown();
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
    expect((await planRow(parent, childA.taskId))?.status).toBe('completed');
    expect((await planRow(parent, childB.taskId))?.status).toBe('completed');
    expect((await planRow(parent, grandchild.taskId))?.status).toBe('completed');
  });

  it('transfers the maximum-sized valid subtree without clipping its execution unit', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    let releaseChild!: () => void;
    childAgent.enqueueRun({ holdUntil: new Promise<void>(r => (releaseChild = r)), finishReason: 'stop' });
    const close = await openParentTurn(parent, parentAgent);
    const tools = createPlanTaskTools(parent);

    const root = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content: 'root' } as any, toolCtx)) as any;
    // Root + 99 children reaches the plan writer's 100-node cap exactly.
    for (let i = 0; i < 99; i++) {
      await tools[TASK_ADD_TOOL_ID]!.execute!({ content: `c${i}`, parentTaskId: root.taskId } as any, toolCtx);
    }
    await tools[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: root.taskId, agentType: 'worker', includeSubtree: true } as any,
      toolCtx,
    );
    await poll(() => childAgent.streamCalls.length > 0);
    expect(childTaskText(childAgent)).not.toContain('TRUNCATED');
    expect(childTaskText(childAgent)).toContain('c98');

    releaseChild();
    close();
    await poll(async () => (await planRow(parent, root.taskId))?.status === 'completed');
  });

  it('rejects a non-leaf without includeSubtree before allocating a child session', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const close = await openParentTurn(parent, parentAgent);
    const tools = createPlanTaskTools(parent);
    const root = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content: 'root' } as any, toolCtx)) as any;
    await tools[TASK_ADD_TOOL_ID]!.execute!({ content: 'child', parentTaskId: root.taskId } as any, toolCtx);

    const result = (await tools[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: root.taskId, agentType: 'worker' } as any,
      toolCtx,
    )) as any;

    expect(result.isError).toBe(true);
    expect(result.message).toMatch(/includeSubtree/);
    expect(childAgent.streamCalls).toHaveLength(0);
    expect((await planRow(parent, root.taskId))?.delegatedSubagentSessionId).toBeUndefined();
    close();
  });

  it('commits the scope link before child allocation so concurrent subtree changes are frozen', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const close = await openParentTurn(parent, parentAgent);
    const tools = createPlanTaskTools(parent);
    const root = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content: 'root' } as any, toolCtx)) as any;
    const child = (await tools[TASK_ADD_TOOL_ID]!.execute!(
      { content: 'original child', parentTaskId: root.taskId } as any,
      toolCtx,
    )) as any;

    const originalSession = harness.session.bind(harness);
    let childAllocationStarted!: () => void;
    const allocationStarted = new Promise<void>(resolve => (childAllocationStarted = resolve));
    let releaseAllocation!: () => void;
    const allocationGate = new Promise<void>(resolve => (releaseAllocation = resolve));
    const sessionSpy = vi.spyOn(harness, 'session').mockImplementation(async (options: any) => {
      if (options.parentSessionId === parent.id) {
        childAllocationStarted();
        await allocationGate;
      }
      return originalSession(options as any);
    });

    const delegation = tools[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: root.taskId, agentType: 'worker', includeSubtree: true } as any,
      toolCtx,
    ) as Promise<any>;
    await allocationStarted;
    const linked = await planRow(parent, root.taskId);
    expect(linked).toMatchObject({ status: 'in_progress' });
    expect(linked?.delegatedSubagentSessionId).toBeDefined();
    expect(await parent._internalStorage.loadSession({ sessionId: linked!.delegatedSubagentSessionId! })).toBeNull();
    // An already-armed recovery scan can observe the durable link in this exact
    // create window. It must join/skip the claimed original driver rather than
    // treating the intentionally-not-yet-created child as a crash orphan.
    await parent._reconcileDelegationsOnHydrate();
    expect((await planRow(parent, root.taskId))?.delegatedSubagentSessionId).toBe(linked!.delegatedSubagentSessionId);
    const concurrentUpdate = await tools[TASK_ADD_TOOL_ID]!.execute!(
      { parentTaskId: child.taskId, content: 'late child while allocation waited' } as any,
      toolCtx,
    );
    expect(concurrentUpdate).toMatchObject({ isError: true, field: 'parentTaskId' });
    releaseAllocation();

    await expect(delegation).resolves.toMatchObject({ subagentSessionId: linked!.delegatedSubagentSessionId });
    await poll(async () => (await planRow(parent, root.taskId))?.status === 'completed');
    expect(childAgent.streamCalls).toHaveLength(1);
    sessionSpy.mockRestore();
    close();
    await harness.shutdown();
  });
});

// ---------------------------------------------------------------------------
// Delegate → link + in_progress (durable, parent turn does not block)
// ---------------------------------------------------------------------------

describe('task_delegate — link + in_progress', () => {
  it('fails the linked task cleanly when child creation rejects', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const close = await openParentTurn(parent, parentAgent);
    const tools = createPlanTaskTools(parent);
    const task = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content: 'allocation failure' } as any, toolCtx)) as any;
    const originalSession = harness.session.bind(harness);
    const sessionSpy = vi.spyOn(harness, 'session').mockImplementation(async (options: any) => {
      if (options.parentSessionId === parent.id) throw new Error('child storage unavailable');
      return originalSession(options);
    });

    const result = (await tools[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: task.taskId, agentType: 'worker' } as any,
      toolCtx,
    )) as any;

    expect(result).toMatchObject({ isError: true });
    await poll(async () => (await planRow(parent, task.taskId))?.status === 'failed');
    expect((await planRow(parent, task.taskId))?.delegatedSubagentSessionId).toBeUndefined();
    expect(childAgent.streamCalls).toHaveLength(0);
    expect(parent._internalSubagentExecutionsInFlight).toBe(0);
    sessionSpy.mockRestore();
    close();
    await harness.shutdown();
  });

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
    // §9 — the subagent TYPE id is persisted too, so a reattach-on-rehydrate can
    // re-resolve the SubagentDefinition (tools / workspace) for the reloaded child.
    expect(row?.delegatedSubagentTypeId).toBe('worker');
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

  it('shares maxConcurrent with every durable delegation and releases the slot after settlement', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB(), { maxConcurrent: 1 });
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    let releaseFirst!: () => void;
    childAgent.enqueueRun({ holdUntil: new Promise<void>(resolve => (releaseFirst = resolve)), finishReason: 'stop' });

    const close = await openParentTurn(parent, parentAgent);
    const tools = createPlanTaskTools(parent);
    const first = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content: 'first root' } as any, toolCtx)) as any;
    const second = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content: 'second root' } as any, toolCtx)) as any;

    const accepted = (await tools[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: first.taskId, agentType: 'worker' } as any,
      toolCtx,
    )) as any;
    expect(accepted.isError).toBeFalsy();
    expect(parent._internalSubagentExecutionsInFlight).toBe(1);
    await poll(() => childAgent.streamCalls.length === 1);

    const rejected = (await tools[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: second.taskId, agentType: 'worker' } as any,
      toolCtx,
    )) as any;
    expect(rejected).toMatchObject({
      isError: true,
      errorName: 'HarnessSubagentConcurrencyLimitError',
    });
    expect((await planRow(parent, second.taskId))?.delegatedSubagentSessionId).toBeUndefined();
    expect(childAgent.streamCalls).toHaveLength(1);

    releaseFirst();
    await poll(async () => (await planRow(parent, first.taskId))?.status === 'completed');
    expect(parent._internalSubagentExecutionsInFlight).toBe(0);

    const retried = (await tools[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: second.taskId, agentType: 'worker' } as any,
      toolCtx,
    )) as any;
    expect(retried.isError).toBeFalsy();
    await poll(async () => (await planRow(parent, second.taskId))?.status === 'completed');
    expect(parent._internalSubagentExecutionsInFlight).toBe(0);
    close();
    await harness.shutdown();
  });

  it('runs sibling tasks from one decomposed root concurrently up to maxConcurrent', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB(), { maxConcurrent: 2 });
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    let releaseFirst!: () => void;
    let releaseSecond!: () => void;
    childAgent.enqueueRuns([
      { holdUntil: new Promise<void>(resolve => (releaseFirst = resolve)), finishReason: 'stop', text: 'first done' },
      {
        holdUntil: new Promise<void>(resolve => (releaseSecond = resolve)),
        finishReason: 'stop',
        text: 'second done',
      },
    ]);

    const close = await openParentTurn(parent, parentAgent);
    const tools = createPlanTaskTools(parent);
    const root = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content: 'parallel review' } as any, toolCtx)) as any;
    const decomposition = (await tools[TASK_DECOMPOSE_TOOL_ID]!.execute!(
      {
        parentTaskId: root.taskId,
        children: [{ content: 'audit source A' }, { content: 'audit source B' }],
      } as any,
      toolCtx,
    )) as any;
    const [first, second] = decomposition.children;

    const firstDelegation = (await tools[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: first.taskId, agentType: 'worker' } as any,
      toolCtx,
    )) as any;
    expect(firstDelegation.isError).toBeFalsy();
    await poll(() => childAgent.streamCalls.length === 1);

    const secondDelegation = (await tools[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: second.taskId, agentType: 'worker' } as any,
      toolCtx,
    )) as any;
    expect(secondDelegation.isError).toBeFalsy();
    await poll(() => childAgent.streamCalls.length === 2);
    expect(parent._internalSubagentExecutionsInFlight).toBe(2);
    expect((await planRow(parent, first.taskId))?.status).toBe('in_progress');
    expect((await planRow(parent, second.taskId))?.status).toBe('in_progress');
    expect((await planRow(parent, root.taskId))?.status).toBe('in_progress');

    releaseFirst();
    await poll(async () => (await planRow(parent, first.taskId))?.status === 'completed');
    expect((await planRow(parent, second.taskId))?.status).toBe('in_progress');
    releaseSecond();
    await poll(async () => (await planRow(parent, root.taskId))?.status === 'completed');
    expect(parent._internalSubagentExecutionsInFlight).toBe(0);
    close();
    await harness.shutdown();
  });

  it('rejects an unsatisfied blockedBy task before child creation, then admits it after the dependency completes', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const close = await openParentTurn(parent, parentAgent);
    const tools = createPlanTaskTools(parent);
    const dependency = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content: 'collect source' } as any, toolCtx)) as any;
    const blocked = (await tools[TASK_ADD_TOOL_ID]!.execute!(
      { content: 'write synthesis', blockedBy: [dependency.taskId] } as any,
      toolCtx,
    )) as any;

    const rejected = (await tools[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: blocked.taskId, agentType: 'worker' } as any,
      toolCtx,
    )) as any;
    expect(rejected).toMatchObject({ isError: true, field: 'taskId' });
    expect(rejected.message).toContain('unsatisfied blockedBy');
    expect(childAgent.streamCalls).toHaveLength(0);
    expect(parent._internalSubagentExecutionsInFlight).toBe(0);
    expect((await planRow(parent, blocked.taskId))?.delegatedSubagentSessionId).toBeUndefined();

    const completed = (await tools[TASK_COMPLETE_TOOL_ID]!.execute!(
      { taskId: dependency.taskId } as any,
      toolCtx,
    )) as any;
    expect(completed.isError).toBeFalsy();
    const admitted = (await tools[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: blocked.taskId, agentType: 'worker' } as any,
      toolCtx,
    )) as any;
    expect(admitted.isError).toBeFalsy();
    await poll(async () => (await planRow(parent, blocked.taskId))?.status === 'completed');
    expect(childAgent.streamCalls).toHaveLength(1);
    close();
    await harness.shutdown();
  });

  it('keeps execution state and assignment child-owned while delegated', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB());
    let releaseChild!: () => void;
    childAgent.enqueueRun({ holdUntil: new Promise<void>(resolve => (releaseChild = resolve)), finishReason: 'stop' });
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const close = await openParentTurn(parent, parentAgent);
    const tools = createPlanTaskTools(parent);
    const task = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content: 'draft methods' } as any, toolCtx)) as any;
    const delegated = (await tools[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: task.taskId, agentType: 'worker' } as any,
      toolCtx,
    )) as any;
    await poll(() => childAgent.streamCalls.length === 1);

    const completedByParent = (await tools[TASK_COMPLETE_TOOL_ID]!.execute!(
      { taskId: task.taskId } as any,
      toolCtx,
    )) as any;
    expect(completedByParent).toMatchObject({ isError: true, field: 'status' });
    const failedByParent = (await tools[TASK_UPDATE_TOOL_ID]!.execute!(
      { taskId: task.taskId, status: 'failed' } as any,
      toolCtx,
    )) as any;
    expect(failedByParent).toMatchObject({ isError: true, field: 'status' });
    const dependencyRewrite = (await tools[TASK_UPDATE_TOOL_ID]!.execute!(
      { taskId: task.taskId, blockedBy: [] } as any,
      toolCtx,
    )) as any;
    expect(dependencyRewrite).toMatchObject({ isError: true, field: 'blockedBy' });

    const renamed = (await tools[TASK_UPDATE_TOOL_ID]!.execute!(
      { taskId: task.taskId, content: 'draft reproducible methods' } as any,
      toolCtx,
    )) as any;
    expect(renamed).toMatchObject({ isError: true, field: 'content' });
    expect((await planRow(parent, task.taskId))?.delegatedSubagentSessionId).toBe(delegated.subagentSessionId);

    releaseChild();
    await poll(async () => (await planRow(parent, task.taskId))?.status === 'completed');
    close();
    await harness.shutdown();
  });

  it('clears a failed delegation for retry and ignores a late callback from the superseded child', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB());
    let releaseRetry!: () => void;
    childAgent.enqueueRuns([
      { finishReason: 'length', terminalToolResult: undefined },
      { holdUntil: new Promise<void>(resolve => (releaseRetry = resolve)), finishReason: 'stop' },
    ]);
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const close = await openParentTurn(parent, parentAgent);
    const tools = createPlanTaskTools(parent);
    const task = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content: 'retryable analysis' } as any, toolCtx)) as any;
    const first = (await tools[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: task.taskId, agentType: 'worker' } as any,
      toolCtx,
    )) as any;
    await poll(async () => (await planRow(parent, task.taskId))?.status === 'failed');
    expect((await planRow(parent, task.taskId))?.delegatedSubagentSessionId).toBeUndefined();

    const second = (await tools[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: task.taskId, agentType: 'worker' } as any,
      toolCtx,
    )) as any;
    expect(second.subagentSessionId).not.toBe(first.subagentSessionId);
    await poll(() => childAgent.streamCalls.length === 2);
    expect(await parent._reconcileDelegatedTask(task.taskId, first.subagentSessionId, 'failed')).toBe(false);
    expect(await planRow(parent, task.taskId)).toMatchObject({
      status: 'in_progress',
      delegatedSubagentSessionId: second.subagentSessionId,
    });

    releaseRetry();
    await poll(async () => (await planRow(parent, task.taskId))?.status === 'completed');
    expect((await planRow(parent, task.taskId))?.delegatedSubagentSessionId).toBeUndefined();
    close();
    await harness.shutdown();
  });

  it('lets Stop win the child-create to plan-link race without starting delegated provider work', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const close = await openParentTurn(parent, parentAgent);
    const tools = createPlanTaskTools(parent);
    const task = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content: 'race-safe work' } as any, toolCtx)) as any;

    const originalSession = harness.session.bind(harness);
    let releaseCreatedChild!: () => void;
    let childCreated!: () => void;
    let createdChildId: string | undefined;
    const childCreatedPromise = new Promise<void>(resolve => (childCreated = resolve));
    const childReturnGate = new Promise<void>(resolve => (releaseCreatedChild = resolve));
    const sessionSpy = vi.spyOn(harness, 'session').mockImplementation(async (args: any) => {
      const session = await originalSession(args);
      if (args.parentSessionId === parent.id) {
        createdChildId = session.id;
        childCreated();
        await childReturnGate;
      }
      return session;
    });

    const delegation = tools[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: task.taskId, agentType: 'worker' } as any,
      toolCtx,
    ) as Promise<any>;
    await childCreatedPromise;
    const abort = parent.abortActiveWork({ reason: 'user_requested', settleTimeoutMs: 1_000 });
    releaseCreatedChild();
    await abort;
    const rejected = await delegation;

    expect(rejected.isError).toBe(true);
    expect(childAgent.streamCalls).toHaveLength(0);
    expect(parent._internalSubagentExecutionsInFlight).toBe(0);
    await poll(async () => (await planRow(parent, task.taskId))?.delegatedSubagentSessionId === undefined);
    expect((await planRow(parent, task.taskId))?.status).toBe('failed');
    expect(createdChildId).toBeDefined();
    await poll(async () => {
      const record = await parent._internalStorage.loadSession({ sessionId: createdChildId! });
      return record?.closedAt !== undefined || record?.cancelRequest !== undefined;
    });

    sessionSpy.mockRestore();
    close();
    await harness.shutdown();
  });

  it('retains the plan ownership link when child close fails after allocation invalidation', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const closeParent = await openParentTurn(parent, parentAgent);
    const tools = createPlanTaskTools(parent);
    const task = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content: 'recover close failure' } as any, toolCtx)) as any;

    const originalSession = harness.session.bind(harness);
    let releaseCreatedChild!: () => void;
    let childCreated!: () => void;
    let childClose: ReturnType<typeof vi.spyOn> | undefined;
    const childCreatedPromise = new Promise<void>(resolve => (childCreated = resolve));
    const childReturnGate = new Promise<void>(resolve => (releaseCreatedChild = resolve));
    const sessionSpy = vi.spyOn(harness, 'session').mockImplementation(async (args: any) => {
      const session = await originalSession(args);
      if (args.parentSessionId === parent.id) {
        childClose = vi.spyOn(session, 'close');
        childClose.mockRejectedValueOnce(new Error('transient child close failure'));
        childCreated();
        await childReturnGate;
      }
      return session;
    });

    const delegation = tools[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: task.taskId, agentType: 'worker' } as any,
      toolCtx,
    ) as Promise<any>;
    await childCreatedPromise;
    const abort = parent.abortActiveWork({ reason: 'user_requested', settleTimeoutMs: 1_000 });
    releaseCreatedChild();
    await abort;
    const rejected = await delegation;

    expect(rejected.isError).toBe(true);
    expect(childAgent.streamCalls).toHaveLength(0);
    expect(await planRow(parent, task.taskId)).toMatchObject({
      status: 'in_progress',
      delegatedSubagentSessionId: expect.any(String),
    });
    expect(childClose).toHaveBeenCalledTimes(1);

    await parent._reconcileDelegationsOnHydrate();
    await poll(async () => (await planRow(parent, task.taskId))?.status === 'failed');
    expect((await planRow(parent, task.taskId))?.delegatedSubagentSessionId).toBeUndefined();
    expect(childClose).toHaveBeenCalledTimes(2);

    sessionSpy.mockRestore();
    closeParent();
    await harness.shutdown();
  });

  it('retains the plan ownership link when close fails in the post-link validation race', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const closeParent = await openParentTurn(parent, parentAgent);
    const tools = createPlanTaskTools(parent);
    const task = (await tools[TASK_ADD_TOOL_ID]!.execute!(
      { content: 'validate ownership safely' } as any,
      toolCtx,
    )) as any;

    const originalSession = harness.session.bind(harness);
    let childClose: ReturnType<typeof vi.spyOn> | undefined;
    const sessionSpy = vi.spyOn(harness, 'session').mockImplementation(async (args: any) => {
      const session = await originalSession(args);
      if (args.parentSessionId === parent.id && childClose === undefined) {
        childClose = vi.spyOn(session, 'close');
        childClose.mockRejectedValueOnce(new Error('transient child close failure'));
      }
      return session;
    });

    const originalLinkCheck = (parent as any)._delegationLinkIsCurrent.bind(parent);
    let releaseLinkCheck!: () => void;
    let linkCheckEntered!: () => void;
    const linkCheckGate = new Promise<void>(resolve => (releaseLinkCheck = resolve));
    const linkCheckEnteredPromise = new Promise<void>(resolve => (linkCheckEntered = resolve));
    const linkCheckSpy = vi
      .spyOn(parent as any, '_delegationLinkIsCurrent')
      .mockImplementation(async (...args: unknown[]) => {
        linkCheckEntered();
        await linkCheckGate;
        return originalLinkCheck(...args);
      });

    const delegation = tools[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: task.taskId, agentType: 'worker' } as any,
      toolCtx,
    ) as Promise<any>;
    await linkCheckEnteredPromise;
    const abort = parent.abortActiveWork({ reason: 'user_requested', settleTimeoutMs: 1_000 });
    releaseLinkCheck();
    await abort;
    const rejected = await delegation;

    expect(rejected.isError).toBe(true);
    expect(childAgent.streamCalls).toHaveLength(0);
    expect(await planRow(parent, task.taskId)).toMatchObject({
      status: 'in_progress',
      delegatedSubagentSessionId: expect.any(String),
    });
    expect(childClose).toHaveBeenCalledTimes(1);

    await parent._reconcileDelegationsOnHydrate();
    await poll(async () => (await planRow(parent, task.taskId))?.status === 'failed');
    expect((await planRow(parent, task.taskId))?.delegatedSubagentSessionId).toBeUndefined();
    expect(childClose).toHaveBeenCalledTimes(2);

    linkCheckSpy.mockRestore();
    sessionSpy.mockRestore();
    closeParent();
    await harness.shutdown();
  });
});

// ---------------------------------------------------------------------------
// Rollup from subagent completion / failure
// ---------------------------------------------------------------------------

describe('task_delegate — rollup from subagent outcome', () => {
  it('fails closed when a provider stop has no structured semantic outcome report', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB());
    childAgent.enqueueRun({
      finishReason: 'stop',
      text: 'Compiler unavailable, but the provider stopped normally.',
      terminalToolResult: null,
    });
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const close = await openParentTurn(parent, parentAgent);
    const tools = createPlanTaskTools(parent);
    const task = (await tools[TASK_ADD_TOOL_ID]!.execute!(
      { content: 'repair and compile LaTeX' } as any,
      toolCtx,
    )) as any;

    await tools[TASK_DELEGATE_TOOL_ID]!.execute!({ taskId: task.taskId, agentType: 'worker' } as any, toolCtx);

    await poll(async () => (await planRow(parent, task.taskId))?.status === 'failed');
    const checked = (await tools[TASK_CHECK_TOOL_ID]!.execute!({ rootTaskId: task.taskId } as any, toolCtx)) as any;
    expect(checked.tasks[0]).toMatchObject({
      status: 'failed',
      delegation: {
        status: 'failed',
        result: {
          status: 'error',
          error: { code: 'harness.subagent_outcome_missing' },
        },
      },
    });
    close();
    await harness.shutdown();
  });

  it('persists an external compiler-service failure as blocked, not completed or source-failed', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB());
    childAgent.enqueueRun({
      finishReason: 'stop',
      terminalToolResult: outcomeTerminalResult('blocked', 'Source was not edited because compilation was unavailable'),
    });
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const close = await openParentTurn(parent, parentAgent);
    const tools = createPlanTaskTools(parent);
    const task = (await tools[TASK_ADD_TOOL_ID]!.execute!(
      { content: 'repair and compile LaTeX' } as any,
      toolCtx,
    )) as any;

    await tools[TASK_DELEGATE_TOOL_ID]!.execute!({ taskId: task.taskId, agentType: 'worker' } as any, toolCtx);

    await poll(async () => (await planRow(parent, task.taskId))?.status === 'blocked');
    const checked = (await tools[TASK_CHECK_TOOL_ID]!.execute!({ rootTaskId: task.taskId } as any, toolCtx)) as any;
    expect(checked.tasks[0]).toMatchObject({
      status: 'blocked',
      delegation: {
        status: 'blocked',
        result: {
          outcome: 'blocked',
          text: 'Source was not edited because compilation was unavailable',
          issue: { code: 'compiler.service_unavailable', retryable: true },
        },
      },
    });
    const childCall = childAgent.streamCalls[0]!;
    expect(String(childCall.options?.instructions)).toContain(HARNESS_SUBAGENT_OUTCOME_REPORT_TOOL_ID);
    expect(childCall.options?.toolsets?.['harness:builtin']?.[HARNESS_SUBAGENT_OUTCOME_REPORT_TOOL_ID]).toBeDefined();
    close();
    await harness.shutdown();
  });

  it('retains the durable link when child close fails, then recovery closes before settlement', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const closeParent = await openParentTurn(parent, parentAgent);
    const tools = createPlanTaskTools(parent);
    const originalSession = harness.session.bind(harness);
    let childClose: ReturnType<typeof vi.spyOn> | undefined;
    const sessionSpy = vi.spyOn(harness, 'session').mockImplementation(async (options: any) => {
      const resolved = await originalSession(options);
      if (options.parentSessionId === parent.id && childClose === undefined) {
        childClose = vi.spyOn(resolved, 'close');
        childClose.mockRejectedValueOnce(new Error('transient child close failure'));
      }
      return resolved;
    });
    const task = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content: 'close safely' } as any, toolCtx)) as any;
    const delegated = (await tools[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: task.taskId, agentType: 'worker' } as any,
      toolCtx,
    )) as any;

    await poll(() => childAgent.streamCalls.length === 1 && parent._internalSubagentExecutionsInFlight === 0);
    expect(await planRow(parent, task.taskId)).toMatchObject({
      status: 'in_progress',
      delegatedSubagentSessionId: delegated.subagentSessionId,
    });

    await parent._reconcileDelegationsOnHydrate();
    await poll(async () => (await planRow(parent, task.taskId))?.status === 'completed');
    expect((await planRow(parent, task.taskId))?.delegatedSubagentSessionId).toBeUndefined();
    expect(childClose).toHaveBeenCalledTimes(2);
    const childRecord = await parent._internalStorage.loadSession({ sessionId: delegated.subagentSessionId });
    expect(childRecord?.closedAt).toBeDefined();
    sessionSpy.mockRestore();
    closeParent();
    await harness.shutdown();
  });

  it('bridges delegated child text/tool lifecycle and terminal summary onto the parent stream', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB());
    childAgent.enqueueRun({
      text: 'delegated analysis complete',
      finishReason: 'tool-calls',
      terminalToolResult: outcomeTerminalResult('completed', 'delegated analysis complete', [
        {
          kind: 'tool-result',
          description: 'Read the delegated source file',
          toolName: 'read_file',
          toolCallId: 'inner-read',
          status: 'success',
        },
      ]),
      chunks: [
        { type: 'text-delta', payload: { id: 'child-message', text: 'checking sources' } },
        { type: 'reasoning-delta', payload: { id: 'child-reasoning', text: 'compare both files' } },
        {
          type: 'tool-call',
          payload: { toolCallId: 'inner-read', toolName: 'read_file', args: { path: 'paper.tex' } },
        },
        {
          type: 'tool-result',
          payload: { toolCallId: 'inner-read', toolName: 'read_file', result: { lines: 42 } },
        },
      ],
    });
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const events: any[] = [];
    parent.subscribe(event => events.push(event));
    const close = await openParentTurn(parent, parentAgent);
    const tools = createPlanTaskTools(parent);
    const task = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content: 'audit paper' } as any, toolCtx)) as any;
    const delegated = (await tools[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: task.taskId, agentType: 'worker' } as any,
      toolCtx,
    )) as any;

    await poll(async () => (await planRow(parent, task.taskId))?.status === 'completed');
    const correlation = 'tc-deleg';
    expect(events.find(event => event.type === 'subagent_start')).toMatchObject({
      toolCallId: correlation,
      subagentSessionId: delegated.subagentSessionId,
      agentType: 'worker',
      task: 'audit paper',
      modelId: 'openai/gpt-4o-mini',
      depth: 1,
    });
    expect(events.find(event => event.type === 'subagent_text_delta')).toMatchObject({
      toolCallId: correlation,
      delta: 'checking sources',
    });
    expect(events.find(event => event.type === 'subagent_reasoning_delta')).toMatchObject({
      toolCallId: correlation,
      delta: 'compare both files',
    });
    expect(events.find(event => event.type === 'subagent_tool_start')).toMatchObject({
      toolCallId: correlation,
      innerToolCallId: 'inner-read',
      toolName: 'read_file',
      input: { path: 'paper.tex' },
    });
    expect(events.find(event => event.type === 'subagent_tool_end')).toMatchObject({
      toolCallId: correlation,
      innerToolCallId: 'inner-read',
      toolName: 'read_file',
      output: { lines: 42 },
      isError: false,
    });
    expect(events.find(event => event.type === 'subagent_end')).toMatchObject({
      toolCallId: correlation,
      subagentSessionId: delegated.subagentSessionId,
      isError: false,
      output: {
        status: 'success',
        text: 'delegated analysis complete',
        finishReason: 'tool-calls',
      },
    });
    const checked = (await tools[TASK_CHECK_TOOL_ID]!.execute!({ rootTaskId: task.taskId } as any, toolCtx)) as any;
    expect(checked.tasks).toHaveLength(1);
    expect(checked.tasks[0]).toMatchObject({
      taskId: task.taskId,
      status: 'completed',
      delegation: {
        subagentSessionId: delegated.subagentSessionId,
        status: 'completed',
        result: {
          status: 'success',
          text: 'delegated analysis complete',
          finishReason: 'tool-calls',
          toolCallCount: 0,
          toolResultCount: 0,
        },
      },
    });
    close();
    await harness.shutdown();
  });

  it('keeps the full bounded delegated result in task state while event output stays a preview', async () => {
    const longAnswer = `complete:${'x'.repeat(8 * 1024)}`;
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB());
    childAgent.enqueueRun({
      text: longAnswer,
      finishReason: 'stop',
      terminalToolResult: outcomeTerminalResult('completed', longAnswer),
    });
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const events: any[] = [];
    parent.subscribe(event => events.push(event));
    const close = await openParentTurn(parent, parentAgent);
    const tools = createPlanTaskTools(parent);
    const task = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content: 'retain full result' } as any, toolCtx)) as any;

    await tools[TASK_DELEGATE_TOOL_ID]!.execute!({ taskId: task.taskId, agentType: 'worker' } as any, toolCtx);
    await poll(async () => (await planRow(parent, task.taskId))?.status === 'completed');

    const terminalEvent = events.find(event => event.type === 'subagent_end');
    expect(terminalEvent.output.textTruncated).toBe(true);
    expect(new TextEncoder().encode(terminalEvent.output.text).byteLength).toBeLessThanOrEqual(4 * 1024);
    const checked = (await tools[TASK_CHECK_TOOL_ID]!.execute!({ rootTaskId: task.taskId } as any, toolCtx)) as any;
    expect(checked.tasks[0].delegation.result).toMatchObject({
      status: 'success',
      text: longAnswer,
      textTruncated: false,
    });

    close();
    await harness.shutdown();
  });

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

  it('retries a transient out-of-turn settlement write and emits the durable plan update after the parent turn ends', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB());
    let releaseChild!: () => void;
    childAgent.enqueueRun({ holdUntil: new Promise<void>(resolve => (releaseChild = resolve)), finishReason: 'stop' });
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const events: any[] = [];
    parent.subscribe(event => events.push(event));
    const close = await openParentTurn(parent, parentAgent);
    const tools = createPlanTaskTools(parent);
    const task = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content: 'settle after turn' } as any, toolCtx)) as any;
    await tools[TASK_DELEGATE_TOOL_ID]!.execute!({ taskId: task.taskId, agentType: 'worker' } as any, toolCtx);
    await poll(() => childAgent.streamCalls.length === 1);

    close();
    await poll(() => !parent.isRunning());
    events.length = 0;
    const storage = parent._internalStorage;
    const originalMutate = storage.mutatePlanTasksForSession.bind(storage);
    let failOnce = true;
    const mutationSpy = vi.spyOn(storage, 'mutatePlanTasksForSession').mockImplementation(async args => {
      if (failOnce) {
        failOnce = false;
        throw new Error('transient settlement write failure');
      }
      return originalMutate(args);
    });

    releaseChild();
    await poll(async () => (await planRow(parent, task.taskId))?.status === 'completed', 3_000);
    expect(parent.isRunning()).toBe(false);
    expect((await planRow(parent, task.taskId))?.delegatedSubagentSessionId).toBeUndefined();
    expect(events.filter(event => event.type === 'papersflow.plan_task.updated')).toHaveLength(1);
    expect(events.find(event => event.type === 'papersflow.plan_task.updated')).toMatchObject({
      payload: {
        op: 'delegate_settled',
        affectedTaskIds: [task.taskId],
      },
    });

    mutationSpy.mockRestore();
    await harness.shutdown();
  });

  it('keeps a HITL-suspended child open and in_progress until its admitted signal resumes', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB(), { maxConcurrent: 1 });
    childAgent.enqueueRuns([
      {
        finishReason: 'suspended',
        suspendPayload: { toolCallId: 'approval-1', toolName: 'write_file', args: { path: 'paper.tex' } },
      },
      { finishReason: 'stop', text: 'approved edit complete' },
    ]);

    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const parentEvents: any[] = [];
    parent.subscribe(event => parentEvents.push(event));
    const close = await openParentTurn(parent, parentAgent);
    const tools = createPlanTaskTools(parent);
    const task = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content: 'edit the paper' } as any, toolCtx)) as any;
    const delegated = (await tools[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: task.taskId, agentType: 'worker' } as any,
      toolCtx,
    )) as any;

    await poll(async () => {
      const record = await parent._internalStorage.loadSession({ sessionId: delegated.subagentSessionId });
      return record?.pendingResume?.toolCallId === 'approval-1';
    });
    expect((await planRow(parent, task.taskId))?.status).toBe('in_progress');
    expect(parent._internalSubagentExecutionsInFlight).toBe(1);
    const parked = await parent._internalStorage.loadSession({ sessionId: delegated.subagentSessionId });
    expect(parked?.closedAt).toBeUndefined();
    expect(parked?.pendingResume).toMatchObject({
      source: 'subagent',
      subagentToolCallId: 'tc-deleg',
    });
    expect(Object.values(parent.getDisplayState().activeSubagents)[0]).toMatchObject({
      subagentSessionId: delegated.subagentSessionId,
      status: 'awaiting_input',
    });
    await poll(() => parentEvents.some(event => event.type === 'tool_approval_required'));
    expect(parentEvents.find(event => event.type === 'tool_approval_required')).toMatchObject({
      source: 'subagent',
      subagentToolCallId: 'tc-deleg',
      subagentSessionId: delegated.subagentSessionId,
      toolCallId: 'approval-1',
    });

    const child = await harness.session({ sessionId: delegated.subagentSessionId, resourceId: 'u1' });
    await child.respondToToolApproval({ approved: true });
    await poll(async () => (await planRow(parent, task.taskId))?.status === 'completed');
    await poll(async () => {
      const record = await parent._internalStorage.loadSession({ sessionId: delegated.subagentSessionId });
      return record?.closedAt !== undefined;
    });
    expect(parent._internalSubagentExecutionsInFlight).toBe(0);
    expect(childAgent.resumeCalls).toHaveLength(1);
    expect(parentEvents.find(event => event.type === 'subagent_end')).toMatchObject({
      toolCallId: 'tc-deleg',
      subagentSessionId: delegated.subagentSessionId,
      isError: false,
    });
    close();
    await harness.shutdown();
  });

  it('cancels a live child at its durable deadline, persists the timeout, and releases capacity', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB(), {
      maxConcurrent: 1,
      delegationTimeoutMs: 80,
    });
    let abortReason: unknown;
    childAgent.enqueueRun({
      holdUntil: new Promise<void>(() => {}),
      finishReason: 'stop',
      onAbort: reason => {
        abortReason = reason;
      },
    });
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const close = await openParentTurn(parent, parentAgent);
    const tools = createPlanTaskTools(parent);
    const first = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content: 'bounded child' } as any, toolCtx)) as any;
    const delegated = (await tools[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: first.taskId, agentType: 'worker' } as any,
      toolCtx,
    )) as any;

    await poll(async () => (await planRow(parent, first.taskId))?.status === 'failed', 3_000);
    const childRecord = await parent._internalStorage.loadSession({ sessionId: delegated.subagentSessionId });
    expect(childRecord?.cancelRequest).toMatchObject({ reason: 'delegation_timeout', requestedBy: parent.id });
    expect(childRecord?.closedAt).toBeDefined();
    expect(abortReason).toBeDefined();
    expect(parent._internalSubagentExecutionsInFlight).toBe(0);

    const checked = (await tools[TASK_CHECK_TOOL_ID]!.execute!({ rootTaskId: first.taskId } as any, toolCtx)) as any;
    expect(checked.tasks[0].delegation).toMatchObject({
      subagentSessionId: delegated.subagentSessionId,
      status: 'failed',
      result: { status: 'error', error: { code: 'harness.delegation_timeout' } },
    });
    expect(checked.tasks[0].delegation.deadlineAt - checked.tasks[0].delegation.startedAt).toBe(80);

    // A timeout must release the shared reservation, not merely mark the task.
    childAgent.enqueueRun({ finishReason: 'stop', text: 'replacement child completed' });
    const second = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content: 'replacement child' } as any, toolCtx)) as any;
    const retried = (await tools[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: second.taskId, agentType: 'worker' } as any,
      toolCtx,
    )) as any;
    expect(retried.isError).toBeFalsy();
    await poll(async () => (await planRow(parent, second.taskId))?.status === 'completed');
    expect(parent._internalSubagentExecutionsInFlight).toBe(0);
    close();
    await harness.shutdown();
  });

  it('applies the durable deadline while child signal admission itself is stalled', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB(), {
      maxConcurrent: 1,
      delegationTimeoutMs: 60,
    });
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const originalSignal = Session.prototype.signal;
    const signalSpy = vi.spyOn(Session.prototype, 'signal').mockImplementation(function (this: Session, input: any) {
      if (this.parentSessionId === parent.id) return new Promise(() => {});
      return originalSignal.call(this, input);
    });
    const close = await openParentTurn(parent, parentAgent);
    try {
      const tools = createPlanTaskTools(parent);
      const task = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content: 'stalled admission' } as any, toolCtx)) as any;
      const delegated = (await tools[TASK_DELEGATE_TOOL_ID]!.execute!(
        { taskId: task.taskId, agentType: 'worker' } as any,
        toolCtx,
      )) as any;

      await poll(async () => (await planRow(parent, task.taskId))?.status === 'failed', 3_000);
      const child = await parent._internalStorage.loadSession({ sessionId: delegated.subagentSessionId });
      expect(child?.cancelRequest?.reason).toBe('delegation_timeout');
      expect(child?.closedAt).toBeDefined();
      expect(childAgent.streamCalls).toHaveLength(0);
      expect(parent._internalSubagentExecutionsInFlight).toBe(0);
    } finally {
      signalSpy.mockRestore();
      close();
      await harness.shutdown();
    }
  });

  it('keeps isBusy and waitForIdle attached to detached child settlement', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB());
    let releaseChild!: () => void;
    childAgent.enqueueRun({
      holdUntil: new Promise<void>(resolve => (releaseChild = resolve)),
      finishReason: 'stop',
    });
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const closeParentTurn = await openParentTurn(parent, parentAgent);
    const tools = createPlanTaskTools(parent);
    const task = (await tools[TASK_ADD_TOOL_ID]!.execute!(
      { content: 'detached idle boundary' } as any,
      toolCtx,
    )) as any;
    await tools[TASK_DELEGATE_TOOL_ID]!.execute!({ taskId: task.taskId, agentType: 'worker' } as any, toolCtx);
    await poll(() => childAgent.streamCalls.length === 1);
    closeParentTurn();
    await poll(() => !parent.isRunning());

    expect(parent.isBusy()).toBe(true);
    let idleSettled = false;
    const idle = parent.waitForIdle({ timeoutMs: 1_000 }).then(() => {
      idleSettled = true;
    });
    await new Promise(resolve => setTimeout(resolve, 20));
    expect(idleSettled).toBe(false);

    releaseChild();
    await idle;
    expect(parent.isBusy()).toBe(false);
    expect((await planRow(parent, task.taskId))?.status).toBe('completed');
    await harness.shutdown();
  });

  it('expires a HITL-pending delegated child without retaining its concurrency slot', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB(), {
      maxConcurrent: 1,
      delegationTimeoutMs: 250,
    });
    childAgent.enqueueRun({
      finishReason: 'suspended',
      suspendPayload: { toolCallId: 'timeout-approval', toolName: 'write_file', args: { path: 'main.tex' } },
    });
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const close = await openParentTurn(parent, parentAgent);
    const tools = createPlanTaskTools(parent);
    const task = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content: 'await approval' } as any, toolCtx)) as any;
    const delegated = (await tools[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: task.taskId, agentType: 'worker' } as any,
      toolCtx,
    )) as any;
    await poll(async () => {
      const child = await parent._internalStorage.loadSession({ sessionId: delegated.subagentSessionId });
      return child?.pendingResume?.toolCallId === 'timeout-approval';
    });
    expect(parent._internalSubagentExecutionsInFlight).toBe(1);

    await poll(async () => (await planRow(parent, task.taskId))?.status === 'failed', 3_000);
    const child = await parent._internalStorage.loadSession({ sessionId: delegated.subagentSessionId });
    expect(child?.cancelRequest?.reason).toBe('delegation_timeout');
    // The parked interaction remains audit evidence, while its admitted signal
    // is terminally failed and no longer owns execution capacity.
    expect(child?.pendingResume?.toolCallId).toBe('timeout-approval');
    expect(child?.closedAt).toBeDefined();
    expect(parent._internalSubagentExecutionsInFlight).toBe(0);
    close();
    await harness.shutdown();
  });

  for (const finishReason of ['length', 'content-filter', 'tool-calls', 'other', 'unknown']) {
    it(`fails closed when a delegated child resolves with finishReason ${finishReason}`, async () => {
      const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB());
      childAgent.enqueueRun({ finishReason, terminalToolResult: undefined });
      const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const close = await openParentTurn(parent, parentAgent);
      const tools = createPlanTaskTools(parent);
      const task = (await tools[TASK_ADD_TOOL_ID]!.execute!(
        { content: 'bounded specialist task' } as any,
        toolCtx,
      )) as any;
      const delegated = (await tools[TASK_DELEGATE_TOOL_ID]!.execute!(
        { taskId: task.taskId, agentType: 'worker' } as any,
        toolCtx,
      )) as any;

      await poll(async () => (await planRow(parent, task.taskId))?.status === 'failed');
      const record = await parent._internalStorage.loadSession({ sessionId: delegated.subagentSessionId });
      expect(record?.closedAt).toBeDefined();
      expect(parent._internalSubagentExecutionsInFlight).toBe(0);
      close();
      await harness.shutdown();
    });
  }
});

// ---------------------------------------------------------------------------
// Recovery / reconcile-on-rehydrate (cross-instance)
// ---------------------------------------------------------------------------

describe('task_delegate — recovery on rehydrate', () => {
  it('singleflights concurrent recovery kicks for the same durable delegation', async () => {
    const { harness, parentAgent } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const close = await openParentTurn(parent, parentAgent);
    const tools = createPlanTaskTools(parent);
    const task = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content: 'recover once' } as any, toolCtx)) as any;
    const delegated = (await tools[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: task.taskId, agentType: 'worker' } as any,
      toolCtx,
    )) as any;
    await poll(async () => (await planRow(parent, task.taskId))?.status === 'completed');

    const settled = (await planRow(parent, task.taskId))!;
    await parent._internalStorage.mutatePlanTasksForSession({
      fence: {
        sessionId: parent.id,
        ownerId: (parent as any)._internalOwnerId,
        ifSessionVersion: (parent as any)._internalRecordVersion,
      },
      ops: [
        {
          kind: 'update',
          taskId: task.taskId,
          ifVersion: settled.version,
          patch: {
            status: 'in_progress',
            statusSource: 'explicit',
            delegatedSubagentSessionId: delegated.subagentSessionId,
            delegatedSubagentTypeId: 'worker',
          },
        },
      ],
    });

    const storage = parent._internalStorage;
    const originalList = storage.listPlanTasks.bind(storage);
    let releaseFirstList!: () => void;
    let firstListEntered!: () => void;
    let listCalls = 0;
    const firstListPromise = new Promise<void>(resolve => (firstListEntered = resolve));
    const firstListGate = new Promise<void>(resolve => (releaseFirstList = resolve));
    const listSpy = vi.spyOn(storage, 'listPlanTasks').mockImplementation(async args => {
      listCalls += 1;
      if (listCalls === 1) {
        firstListEntered();
        await firstListGate;
      }
      return originalList(args);
    });

    const first = parent._reconcileDelegationsOnHydrate();
    await firstListPromise;
    const second = parent._reconcileDelegationsOnHydrate();
    await Promise.resolve();
    expect(listCalls).toBe(1);
    releaseFirstList();
    await Promise.all([first, second]);
    await poll(async () => (await planRow(parent, task.taskId))?.status === 'completed');

    listSpy.mockRestore();
    close();
    await harness.shutdown();
  });

  it('restores only approved lineage when restart occurs before the delegated signal dispatches', async () => {
    const db = new InMemoryDB();
    const lineageKey = 'turnCorrelationId';
    const lineageValue = lineageValueForCanonicalAppBytes(MAX_INHERITED_REQUEST_CONTEXT_APP_BYTES - 1);
    const privateCanary = 'PRE_DISPATCH_PRIVATE_APP_CANARY';
    const inheritedAppKeys = [lineageKey];
    const a = buildHarness(db, { maxConcurrent: 1, inheritRequestContextAppKeys: inheritedAppKeys });
    const skippedInitialDriver = vi
      .spyOn(Session.prototype as any, '_driveDelegatedSubagent')
      .mockResolvedValueOnce(undefined);
    const parentA = await a.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const closeParentTurn = await openParentTurn(parentA, a.parentAgent);
    const toolsA = createPlanTaskTools(parentA);
    const task = (await toolsA[TASK_ADD_TOOL_ID]!.execute!(
      { content: 'dispatch after restart' } as any,
      toolCtx,
    )) as any;
    let delegated: any;
    try {
      delegated = await toolsA[TASK_DELEGATE_TOOL_ID]!.execute!(
        { taskId: task.taskId, agentType: 'worker' } as any,
        {
          ...toolCtx,
          requestContext: new RequestContext<unknown>([
            [
              'app',
              {
                [lineageKey]: lineageValue,
                'papersflow.requestId': 'functional-request-must-not-cross',
                instructions: privateCanary,
              },
            ],
          ]),
        } as any,
      );
      expect(skippedInitialDriver).toHaveBeenCalledOnce();
    } finally {
      skippedInitialDriver.mockRestore();
    }
    expect(a.childAgent.streamCalls).toHaveLength(0);
    const pendingPlan = JSON.stringify(await planRow(parentA, task.taskId));
    expect(pendingPlan).toContain(lineageValue);
    expect(pendingPlan).not.toContain(privateCanary);
    expect(pendingPlan).not.toContain('functional-request-must-not-cross');
    closeParentTurn();

    for (const [key, row] of db.harnessSessions) {
      if (row.ownerId === (parentA as any)._internalOwnerId) {
        db.harnessSessions.set(key, { ...row, ownerId: 'crashed', leaseExpiresAt: Date.now() - 1 });
      }
    }

    const b = buildHarness(db, { maxConcurrent: 1, inheritRequestContextAppKeys: inheritedAppKeys });
    const parentB = await b.harness.session({ sessionId: parentA.id, resourceId: 'u1' });
    await poll(async () => (await planRow(parentB, task.taskId))?.status === 'completed');

    expect(b.childAgent.streamCalls).toHaveLength(1);
    const childRequestContext = b.childAgent.streamCalls[0]?.options.requestContext as RequestContext | undefined;
    expect(childRequestContext?.get(lineageKey)).toBe(lineageValue);
    expect(childRequestContext?.get('app')).toEqual({ [lineageKey]: lineageValue });
    expect(childRequestContext?.get('requestId')).toBeUndefined();
    expect(JSON.stringify(childRequestContext?.toJSON())).not.toContain(privateCanary);
    expect(JSON.stringify(childRequestContext?.toJSON())).not.toContain('functional-request-must-not-cross');
    const settledPlan = await planRow(parentB, task.taskId);
    expect(JSON.stringify(settledPlan)).not.toContain(lineageValue);
    expect((settledPlan?.metadata as any)?.mastraHarnessDelegationAttemptV1).not.toHaveProperty('requestContextApp');
    expect((settledPlan?.metadata as any)?.mastraHarnessDelegationAttemptV1).not.toHaveProperty(
      'requestContextAppSha256',
    );
    expect(delegated.subagentSessionId).toBeTruthy();
    await b.harness.shutdown();
  });

  it('reattaches a suspended admitted signal after a crash and rolls up only after HITL resume', async () => {
    const db = new InMemoryDB();
    const lineageKey = 'turnCorrelationId';
    const lineageValue = 'opaque-durable-turn-5049';
    const functionalRequestId = 'durable-functional-request-must-not-cross';
    const secretCanary = 'DURABLE_PRIVATE_APP_METADATA_CANARY';
    const inheritedAppKeys = [lineageKey];
    const a = buildHarness(db, { maxConcurrent: 1, inheritRequestContextAppKeys: inheritedAppKeys });
    a.childAgent.enqueueRun({
      finishReason: 'suspended',
      suspendPayload: { toolCallId: 'cold-approval', toolName: 'write_file', args: { path: 'main.tex' } },
    });
    const parentA = await a.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const closeParentTurn = await openParentTurn(parentA, a.parentAgent);
    const toolsA = createPlanTaskTools(parentA);
    const task = (await toolsA[TASK_ADD_TOOL_ID]!.execute!({ content: 'durable edit' } as any, toolCtx)) as any;
    const delegated = (await toolsA[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: task.taskId, agentType: 'worker', task: 'apply the exact admitted custom edit' } as any,
      {
        ...toolCtx,
        requestContext: new RequestContext<unknown>([
          [
            'app',
            {
              [lineageKey]: lineageValue,
              'papersflow.requestId': functionalRequestId,
              'papersflow.userId': 'durable-user-must-not-cross',
              instructions: secretCanary,
            },
          ],
        ]),
      } as any,
    )) as any;

    await poll(async () => {
      const child = await parentA._internalStorage.loadSession({ sessionId: delegated.subagentSessionId });
      return child?.pendingResume?.toolCallId === 'cold-approval';
    });
    const initialChildRequestContext = a.childAgent.streamCalls[0]?.options.requestContext as
      | RequestContext
      | undefined;
    expect(initialChildRequestContext?.get(lineageKey)).toBe(lineageValue);
    expect(initialChildRequestContext?.get('app')).toEqual({ [lineageKey]: lineageValue });
    expect((initialChildRequestContext?.get('harness') as { app?: unknown } | undefined)?.app).toEqual({
      [lineageKey]: lineageValue,
    });
    expect(initialChildRequestContext?.get('requestId')).toBeUndefined();
    expect(initialChildRequestContext?.get('userId')).toBeUndefined();
    expect((initialChildRequestContext?.get('app') as Record<string, unknown>)['papersflow.requestId']).toBeUndefined();
    expect(initialChildRequestContext?.get('papersflow.userId')).toBeUndefined();
    const persistedChildBeforeCrash = await parentA._internalStorage.loadSession({
      sessionId: delegated.subagentSessionId,
    });
    const persistedLineage = JSON.stringify({
      task: await planRow(parentA, task.taskId),
      pendingResume: persistedChildBeforeCrash?.pendingResume,
    });
    expect(persistedLineage).toContain(lineageValue);
    expect(persistedLineage).not.toContain(secretCanary);
    expect(persistedLineage).not.toContain(functionalRequestId);
    expect(persistedLineage).not.toContain('durable-user-must-not-cross');
    // The visible label remains editable while the immutable admitted body is
    // preserved separately for byte-identical recovery.
    await toolsA[TASK_UPDATE_TOOL_ID]!.execute!(
      { taskId: task.taskId, content: 'renamed while approval is pending' } as any,
      toolCtx,
    );
    closeParentTurn();

    // Simulate a process death without graceful shutdown: expire every lease
    // owned by A while leaving the durable parent/task, child pendingResume, and
    // admitted signal evidence intact.
    for (const [key, row] of db.harnessSessions) {
      if (row.ownerId === (parentA as any)._internalOwnerId) {
        db.harnessSessions.set(key, { ...row, ownerId: 'crashed', leaseExpiresAt: Date.now() - 1 });
      }
    }

    const b = buildHarness(db, { maxConcurrent: 1, inheritRequestContextAppKeys: inheritedAppKeys });
    b.childAgent.enqueueRun({ finishReason: 'stop', text: 'edit applied after reconnect' });
    const parentB = await b.harness.session({ sessionId: parentA.id, resourceId: 'u1' });
    await poll(() => parentB._internalSubagentExecutionsInFlight === 1);
    expect((await planRow(parentB, task.taskId))?.status).toBe('in_progress');
    // Reattachment observes the admitted signal; it must not start a second
    // initial specialist run.
    expect(b.childAgent.streamCalls).toHaveLength(0);

    const childB = await b.harness.session({ sessionId: delegated.subagentSessionId, resourceId: 'u1' });
    await childB.respondToToolApproval({ approved: true });
    await poll(async () => (await planRow(parentB, task.taskId))?.status === 'completed');
    expect(b.childAgent.streamCalls).toHaveLength(0);
    expect(b.childAgent.resumeCalls).toHaveLength(1);
    const resumedChildRequestContext = (b.childAgent.resumeCalls[0]?.options as { requestContext?: RequestContext })
      .requestContext;
    expect(resumedChildRequestContext?.get(lineageKey)).toBe(lineageValue);
    expect(resumedChildRequestContext?.get('app')).toEqual({ [lineageKey]: lineageValue });
    expect((resumedChildRequestContext?.get('harness') as { app?: unknown } | undefined)?.app).toEqual({
      [lineageKey]: lineageValue,
    });
    expect(resumedChildRequestContext?.get('requestId')).toBeUndefined();
    expect(resumedChildRequestContext?.get('userId')).toBeUndefined();
    expect((resumedChildRequestContext?.get('app') as Record<string, unknown>)['papersflow.requestId']).toBeUndefined();
    expect(resumedChildRequestContext?.get('papersflow.userId')).toBeUndefined();
    expect(JSON.stringify(resumedChildRequestContext?.toJSON())).not.toContain(secretCanary);
    expect(JSON.stringify(resumedChildRequestContext?.toJSON())).not.toContain(functionalRequestId);
    const settledPlan = await planRow(parentB, task.taskId);
    expect(JSON.stringify(settledPlan)).not.toContain(lineageValue);
    expect((settledPlan?.metadata as any)?.mastraHarnessDelegationAttemptV1).not.toHaveProperty('requestContextApp');
    expect((settledPlan?.metadata as any)?.mastraHarnessDelegationAttemptV1).not.toHaveProperty(
      'requestContextAppSha256',
    );
    expect(parentB._internalSubagentExecutionsInFlight).toBe(0);
    await b.harness.shutdown();
  });

  it('reattaches before the original deadline without resetting its clock or redispatching', async () => {
    const db = new InMemoryDB();
    const a = buildHarness(db, { maxConcurrent: 1, delegationTimeoutMs: 2_000 });
    a.childAgent.enqueueRun({
      finishReason: 'suspended',
      suspendPayload: { toolCallId: 'deadline-approval', toolName: 'write_file', args: { path: 'main.tex' } },
    });
    const parentA = await a.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const closeParentTurn = await openParentTurn(parentA, a.parentAgent);
    const toolsA = createPlanTaskTools(parentA);
    const task = (await toolsA[TASK_ADD_TOOL_ID]!.execute!(
      { content: 'resume before deadline' } as any,
      toolCtx,
    )) as any;
    const delegated = (await toolsA[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: task.taskId, agentType: 'worker' } as any,
      toolCtx,
    )) as any;
    await poll(async () => {
      const child = await parentA._internalStorage.loadSession({ sessionId: delegated.subagentSessionId });
      return child?.pendingResume?.toolCallId === 'deadline-approval';
    });
    const before = (await (parentA as any)._planTaskCheck({ rootTaskId: task.taskId })).tasks[0].delegation;
    expect(before.deadlineAt - before.startedAt).toBe(2_000);
    closeParentTurn();

    for (const [key, row] of db.harnessSessions) {
      if (row.ownerId === (parentA as any)._internalOwnerId) {
        db.harnessSessions.set(key, { ...row, ownerId: 'crashed', leaseExpiresAt: Date.now() - 1 });
      }
    }

    // A radically different new-process config proves recovery uses the
    // persisted absolute timestamp instead of granting a fresh duration.
    const b = buildHarness(db, { maxConcurrent: 1, delegationTimeoutMs: 1 });
    b.childAgent.enqueueRun({ finishReason: 'stop', text: 'approved before original deadline' });
    const parentB = await b.harness.session({ sessionId: parentA.id, resourceId: 'u1' });
    await poll(() => parentB._internalSubagentExecutionsInFlight === 1);
    const after = (await (parentB as any)._planTaskCheck({ rootTaskId: task.taskId })).tasks[0].delegation;
    expect(after).toMatchObject({ startedAt: before.startedAt, deadlineAt: before.deadlineAt, status: 'running' });
    expect(b.childAgent.streamCalls).toHaveLength(0);

    const childB = await b.harness.session({ sessionId: delegated.subagentSessionId, resourceId: 'u1' });
    await childB.respondToToolApproval({ approved: true });
    await poll(async () => (await planRow(parentB, task.taskId))?.status === 'completed');
    expect(b.childAgent.streamCalls).toHaveLength(0);
    expect(b.childAgent.resumeCalls).toHaveLength(1);
    expect(parentB._internalSubagentExecutionsInFlight).toBe(0);
    await b.harness.shutdown();
  });

  it('cancels an expired suspended child on restart without redispatching specialist work', async () => {
    const db = new InMemoryDB();
    const a = buildHarness(db, { maxConcurrent: 1, delegationTimeoutMs: 100 });
    a.childAgent.enqueueRun({
      finishReason: 'suspended',
      suspendPayload: { toolCallId: 'expired-approval', toolName: 'write_file', args: { path: 'main.tex' } },
    });
    const parentA = await a.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const closeParentTurn = await openParentTurn(parentA, a.parentAgent);
    const toolsA = createPlanTaskTools(parentA);
    const task = (await toolsA[TASK_ADD_TOOL_ID]!.execute!({ content: 'expire while offline' } as any, toolCtx)) as any;
    const delegated = (await toolsA[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: task.taskId, agentType: 'worker' } as any,
      toolCtx,
    )) as any;
    await poll(async () => {
      const child = await parentA._internalStorage.loadSession({ sessionId: delegated.subagentSessionId });
      return child?.pendingResume?.toolCallId === 'expired-approval';
    });
    closeParentTurn();

    for (const [key, row] of db.harnessSessions) {
      if (row.ownerId === (parentA as any)._internalOwnerId) {
        db.harnessSessions.set(key, { ...row, ownerId: 'crashed', leaseExpiresAt: Date.now() - 1 });
      }
    }
    await new Promise(resolve => setTimeout(resolve, 150));

    const b = buildHarness(db, { maxConcurrent: 1, delegationTimeoutMs: 60_000 });
    const parentB = await b.harness.session({ sessionId: parentA.id, resourceId: 'u1' });
    await poll(async () => (await planRow(parentB, task.taskId))?.status === 'failed', 3_000);
    const checked = (await (parentB as any)._planTaskCheck({ rootTaskId: task.taskId })).tasks[0];
    expect(checked.delegation).toMatchObject({
      subagentSessionId: delegated.subagentSessionId,
      status: 'failed',
      result: { error: { code: 'harness.delegation_timeout' } },
    });
    const child = await parentB._internalStorage.loadSession({ sessionId: delegated.subagentSessionId });
    expect(child?.cancelRequest?.reason).toBe('delegation_timeout');
    expect(child?.closedAt).toBeDefined();
    expect(b.childAgent.streamCalls).toHaveLength(0);
    expect(b.childAgent.resumeCalls).toHaveLength(0);
    expect(parentB._internalSubagentExecutionsInFlight).toBe(0);
    await b.harness.shutdown();
  });

  it('reattaches an includeSubtree admission byte-identically and settles every descendant after HITL', async () => {
    const db = new InMemoryDB();
    const a = buildHarness(db, { maxConcurrent: 1 });
    a.childAgent.enqueueRun({
      finishReason: 'suspended',
      suspendPayload: { toolCallId: 'subtree-approval', toolName: 'write_file', args: { path: 'paper.tex' } },
    });
    const parentA = await a.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const closeParentTurn = await openParentTurn(parentA, a.parentAgent);
    const toolsA = createPlanTaskTools(parentA);
    const root = (await toolsA[TASK_ADD_TOOL_ID]!.execute!({ content: 'repair paper' } as any, toolCtx)) as any;
    const child = (await toolsA[TASK_ADD_TOOL_ID]!.execute!(
      { content: 'compile verification', parentTaskId: root.taskId } as any,
      toolCtx,
    )) as any;
    const delegated = (await toolsA[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: root.taskId, agentType: 'worker', includeSubtree: true } as any,
      toolCtx,
    )) as any;
    await poll(async () => {
      const childRecord = await parentA._internalStorage.loadSession({ sessionId: delegated.subagentSessionId });
      return childRecord?.pendingResume?.toolCallId === 'subtree-approval';
    });
    expect(childTaskText(a.childAgent)).toContain(`id=${child.taskId}`);
    closeParentTurn();

    for (const [key, row] of db.harnessSessions) {
      if (row.ownerId === (parentA as any)._internalOwnerId) {
        db.harnessSessions.set(key, { ...row, ownerId: 'crashed', leaseExpiresAt: Date.now() - 1 });
      }
    }

    const b = buildHarness(db, { maxConcurrent: 1 });
    b.childAgent.enqueueRun({ finishReason: 'stop', text: 'subtree approved' });
    const parentB = await b.harness.session({ sessionId: parentA.id, resourceId: 'u1' });
    await poll(() => parentB._internalSubagentExecutionsInFlight === 1);
    const childB = await b.harness.session({ sessionId: delegated.subagentSessionId, resourceId: 'u1' });
    await childB.respondToToolApproval({ approved: true });

    await poll(async () => (await planRow(parentB, root.taskId))?.status === 'completed');
    expect((await planRow(parentB, child.taskId))?.status).toBe('completed');
    expect(b.childAgent.streamCalls).toHaveLength(0);
    expect(b.childAgent.resumeCalls).toHaveLength(1);
    await b.harness.shutdown();
  });

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
          patch: {
            status: 'in_progress',
            statusSource: 'explicit',
            delegatedSubagentSessionId: childId,
            delegatedSubagentTypeId: 'worker',
          },
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
          patch: {
            status: 'in_progress',
            statusSource: 'explicit',
            delegatedSubagentSessionId: childId,
            delegatedSubagentTypeId: 'worker',
          },
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
    a.childAgent.enqueueRun({ finishReason: 'error', terminalToolResult: undefined });

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

// ---------------------------------------------------------------------------
// §9 — reattach-on-rehydrate restores the SubagentDefinition tools/workspace
// override by re-resolving the persisted subagent TYPE id. (The type is also
// persisted on the child record for the direct-hydrate path; see the M4
// fail-closed test below.)
// ---------------------------------------------------------------------------

describe('task_delegate — reattach restores the subagent override from the persisted type', () => {
  function buildTooledHarness(db: InMemoryDB) {
    const parentAgent = new MockAgent({ id: 'parent-agent' });
    const childAgent = new MockAgent({
      id: 'child-agent',
      defaultOutput: { terminalToolResult: outcomeTerminalResult() },
    });
    const extraTool = createTool({
      id: 'extraTool',
      description: 'extra',
      inputSchema: z.object({}),
      execute: async () => ({}),
    });
    const harness = new Harness({
      agents: { 'parent-agent': parentAgent, 'child-agent': childAgent } as any,
      modes: [
        { id: 'default', agentId: 'parent-agent' },
        { id: 'worker-mode', agentId: 'child-agent' },
      ],
      defaultModeId: 'default',
      sessions: { storage: new InMemoryHarness({ db }) },
      subagents: {
        maxDepth: 2,
        types: {
          tooled: {
            agentId: 'child-agent',
            modeId: 'worker-mode',
            description: 'worker with an extra tool',
            tools: { extraTool },
            workspace: 'inherit',
          },
        },
      },
    });
    return { harness, parentAgent, childAgent };
  }

  it('re-resolves def.tools + workspace onto the reattached child', async () => {
    const db = new InMemoryDB();
    const a = buildTooledHarness(db);
    const parent = await a.harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    // Suspend the admitted child so the durable delegation has a genuine
    // restart join point rather than a process-local held promise.
    a.childAgent.enqueueRun({
      finishReason: 'suspended',
      suspendPayload: { toolCallId: 'reattach-approval', toolName: 'write_file', args: { path: 'main.tex' } },
    });

    const close = await openParentTurn(parent, a.parentAgent);
    const tools = createPlanTaskTools(parent);
    const task = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content: 'do it' } as any, toolCtx)) as any;
    const res = (await tools[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: task.taskId, agentType: 'tooled' } as any,
      toolCtx,
    )) as any;
    close();

    await poll(async () => {
      const child = await parent._internalStorage.loadSession({ sessionId: res.subagentSessionId });
      return child?.pendingResume?.toolCallId === 'reattach-approval';
    });

    // Crash A by expiring its leases without a graceful in-process drain.
    for (const [key, row] of db.harnessSessions) {
      if (row.ownerId === (parent as any)._internalOwnerId) {
        db.harnessSessions.set(key, { ...row, ownerId: 'crashed', leaseExpiresAt: Date.now() - 1 });
      }
    }

    const b = buildTooledHarness(db);
    b.childAgent.enqueueRun({ finishReason: 'stop', text: 'approved edit completed' });
    const parentB = await b.harness.session({ sessionId: parent.id, resourceId: 'u1' });
    const childB = await b.harness.session({ sessionId: res.subagentSessionId, resourceId: 'u1' });
    expect(Object.keys((childB as any)._subagentToolsOverride ?? {})).toContain('extraTool');
    expect((childB as any)._subagentInheritWorkspace).toBe(true);

    await childB.respondToToolApproval({ approved: true });
    await poll(async () => (await planRow(parentB, task.taskId))?.status === 'completed');
    await b.harness.shutdown();
  });
});

// ---------------------------------------------------------------------------
// M4 — a subagent child carries its `subagents.types` key on its own record, so
// a child hydrated DIRECTLY by id (on a restart / other instance) restores its
// per-subagent overrides — including the `toolAllowlist` HARD capability scope —
// WITHOUT relying on the parent's delegation-reattach. Without this the directly
// hydrated child ran fail-OPEN (allowlist lost ⇒ non-listed tools exposed).
// ---------------------------------------------------------------------------

describe('subagent child — direct hydrate restores the persisted scope (M4 fail-closed)', () => {
  function buildScopedHarness(db: InMemoryDB) {
    const parentAgent = new MockAgent({ id: 'parent-agent' });
    const childAgent = new MockAgent({
      id: 'child-agent',
      defaultOutput: { terminalToolResult: outcomeTerminalResult() },
    });
    const extraTool = createTool({
      id: 'extraTool',
      description: 'extra',
      inputSchema: z.object({}),
      execute: async () => ({}),
    });
    const harness = new Harness({
      agents: { 'parent-agent': parentAgent, 'child-agent': childAgent } as any,
      modes: [
        { id: 'default', agentId: 'parent-agent' },
        { id: 'worker-mode', agentId: 'child-agent' },
      ],
      defaultModeId: 'default',
      sessions: { storage: new InMemoryHarness({ db }) },
      subagents: {
        maxDepth: 2,
        types: {
          scoped: {
            agentId: 'child-agent',
            modeId: 'worker-mode',
            description: 'a hard-scoped worker',
            tools: { extraTool },
            toolAllowlist: ['readDoc', 'extraTool'],
            workspace: 'inherit',
          },
        },
      },
    });
    return { harness, parentAgent, childAgent };
  }

  it('task_delegate persists the subagent type id on the child record', async () => {
    const db = new InMemoryDB();
    const { harness, parentAgent, childAgent } = buildScopedHarness(db);
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    // Let the child's detached run finish on its own so shutdown can drain. The
    // child record (carrying subagentTypeId) is written synchronously at delegate.
    childAgent.enqueueRun({ finishReason: 'stop', text: 'done' });
    const close = await openParentTurn(parent, parentAgent);
    const tools = createPlanTaskTools(parent);
    const task = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content: 'scoped work' } as any, toolCtx)) as any;
    const res = (await tools[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: task.taskId, agentType: 'scoped' } as any,
      toolCtx,
    )) as any;
    const childRecord = await parent._internalStorage.loadSession({
      harnessName: 'default',
      sessionId: res.subagentSessionId,
    });
    expect(childRecord?.subagentTypeId).toBe('scoped');
    close();
    await harness.shutdown();
  });

  it('a child hydrated DIRECTLY by id on a fresh instance keeps its toolAllowlist (no parent reattach)', async () => {
    const db = new InMemoryDB();
    const a = buildScopedHarness(db);
    const parentA = await a.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    // Create a durable subagent child the way delegate/spawn do (origin +
    // parentSessionId + persisted subagentTypeId), without running a turn so the
    // record stays ACTIVE across the simulated restart.
    const childA = await a.harness.session({
      resourceId: 'u1',
      threadId: { fresh: true },
      parentSessionId: parentA.id,
      origin: 'subagent-tool',
      modeId: 'worker-mode',
      subagentDepth: 1,
      subagentTypeId: 'scoped',
    } as any);
    const childId = childA.id;
    expect((childA as any)._subagentToolAllowlist).toEqual(['readDoc', 'extraTool']);
    await a.harness.shutdown();

    // Fresh instance hydrates the CHILD directly by id — NOT via the parent's
    // delegation-reattach (the parent is never hydrated here).
    const b = buildScopedHarness(db);
    const childB = await b.harness.session({ sessionId: childId, resourceId: 'u1' });
    expect((childB as any)._subagentToolAllowlist).toEqual(['readDoc', 'extraTool']);
    expect((childB as any)._toolPermissionGateEngaged()).toBe(true);
    // A non-listed tool is hard-denied even on the directly-hydrated child.
    expect(
      (childB as any)._resolveToolPolicy(
        'writeDoc',
        { categories: {}, tools: {} },
        { categories: [], tools: [] },
        'allow',
        new Set(['readDoc', 'extraTool']),
      ),
    ).toBe('deny');
    // The persisted tools + workspace overrides are restored on the same path.
    expect(Object.keys((childB as any)._subagentToolsOverride ?? {})).toContain('extraTool');
    expect((childB as any)._subagentInheritWorkspace).toBe(true);
    await b.harness.shutdown();
  });

  it('FAILS CLOSED when a scoped child is hydrated but its subagent type was DELETED from config (M4-residual)', async () => {
    const db = new InMemoryDB();
    const a = buildScopedHarness(db);
    const parentA = await a.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const childA = await a.harness.session({
      resourceId: 'u1',
      threadId: { fresh: true },
      parentSessionId: parentA.id,
      origin: 'subagent-tool',
      modeId: 'worker-mode',
      subagentDepth: 1,
      subagentTypeId: 'scoped',
    } as any);
    const childId = childA.id;
    await a.harness.shutdown();

    // Fresh instance whose config NO LONGER defines the 'scoped' subagent type
    // (a redeploy removed it). The child's allowlist cannot be re-resolved, but
    // the persisted scoped-flag makes hydrate fail CLOSED (empty allowlist).
    const parentAgent = new MockAgent({ id: 'parent-agent' });
    const childAgent = new MockAgent({
      id: 'child-agent',
      defaultOutput: { terminalToolResult: outcomeTerminalResult() },
    });
    const bHarness = new Harness({
      agents: { 'parent-agent': parentAgent, 'child-agent': childAgent } as any,
      modes: [
        { id: 'default', agentId: 'parent-agent' },
        { id: 'worker-mode', agentId: 'child-agent' },
      ],
      defaultModeId: 'default',
      sessions: { storage: new InMemoryHarness({ db }) },
      subagents: {
        maxDepth: 2,
        types: {
          // 'scoped' is GONE; only an unrelated type remains.
          other: { agentId: 'child-agent', modeId: 'worker-mode', description: 'unrelated' },
        },
      },
    });
    const childB = await bHarness.session({ sessionId: childId, resourceId: 'u1' });
    // Fail CLOSED: empty allowlist engages the gate and denies every non-builtin.
    expect((childB as any)._subagentToolAllowlist).toEqual([]);
    expect((childB as any)._toolPermissionGateEngaged()).toBe(true);
    expect(
      (childB as any)._resolveToolPolicy(
        'readDoc',
        { categories: {}, tools: {} },
        { categories: [], tools: [] },
        'allow',
        new Set([]),
      ),
    ).toBe('deny');
    await bHarness.shutdown();
  });

  it('an UNSCOPED child whose type was deleted stays unrestricted (no false fail-closed)', async () => {
    const db = new InMemoryDB();
    const parentAgent = new MockAgent({ id: 'parent-agent' });
    const childAgent = new MockAgent({
      id: 'child-agent',
      defaultOutput: { terminalToolResult: outcomeTerminalResult() },
    });
    const a = new Harness({
      agents: { 'parent-agent': parentAgent, 'child-agent': childAgent } as any,
      modes: [
        { id: 'default', agentId: 'parent-agent' },
        { id: 'worker-mode', agentId: 'child-agent' },
      ],
      defaultModeId: 'default',
      sessions: { storage: new InMemoryHarness({ db }) },
      subagents: {
        maxDepth: 2,
        // 'plain' has NO toolAllowlist → unscoped.
        types: { plain: { agentId: 'child-agent', modeId: 'worker-mode', description: 'unscoped worker' } },
      },
    });
    const parentA = await a.session({ resourceId: 'u1', threadId: { fresh: true } });
    const childA = await a.session({
      resourceId: 'u1',
      threadId: { fresh: true },
      parentSessionId: parentA.id,
      origin: 'subagent-tool',
      modeId: 'worker-mode',
      subagentDepth: 1,
      subagentTypeId: 'plain',
    } as any);
    const childId = childA.id;
    await a.shutdown();

    // Hydrate on an instance where 'plain' is gone. An unscoped child must NOT be
    // wrongly fail-closed (it never had a capability scope).
    const b = new Harness({
      agents: {
        'parent-agent': new MockAgent({ id: 'parent-agent' }),
        'child-agent': new MockAgent({ id: 'child-agent' }),
      } as any,
      modes: [
        { id: 'default', agentId: 'parent-agent' },
        { id: 'worker-mode', agentId: 'child-agent' },
      ],
      defaultModeId: 'default',
      sessions: { storage: new InMemoryHarness({ db }) },
      subagents: { maxDepth: 2, types: { other: { agentId: 'child-agent', modeId: 'worker-mode', description: 'x' } } },
    });
    const childB = await b.session({ sessionId: childId, resourceId: 'u1' });
    expect((childB as any)._subagentToolAllowlist).toBeUndefined();
    await b.shutdown();
  });
});

describe('cancelSubagent (§SA1)', () => {
  it('cancels an in-flight delegated child and rolls its plan task up to failed', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    // Hold the child mid-run so it is genuinely in-flight when we cancel it.
    let releaseChild!: () => void;
    childAgent.enqueueRun({ holdUntil: new Promise<void>(r => (releaseChild = r)), finishReason: 'stop' });

    const close = await openParentTurn(parent, parentAgent);
    const tools = createPlanTaskTools(parent);
    const task = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content: 'cancel me' } as any, toolCtx)) as any;
    const res = (await tools[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: task.taskId, agentType: 'worker' } as any,
      toolCtx,
    )) as any;
    const childId: string = res.subagentSessionId;

    // The child is live + active. Targeted cancel of just this subagent.
    await parent.cancelSubagent({ subagentSessionId: childId });
    releaseChild();
    close();

    // The delegation driver maps the child's cancelRequest → a FAILED plan task.
    await poll(async () => (await planRow(parent, task.taskId))?.status === 'failed');
    await harness.shutdown();
  });

  it('is a no-op for an unknown / non-child subagent id', async () => {
    const { harness } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    await expect(parent.cancelSubagent({ subagentSessionId: 'not-a-child' })).resolves.toBeUndefined();
    await harness.shutdown();
  });

  it('resetPlanTasks clears a discarded revision and cancels its detached child while keeping the parent reusable', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    let releaseChild!: () => void;
    childAgent.enqueueRun({ holdUntil: new Promise<void>(resolve => (releaseChild = resolve)), finishReason: 'stop' });

    const releaseParent = await openParentTurn(parent, parentAgent);
    const tools = createPlanTaskTools(parent);
    const task = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content: 'stale branch work' } as any, toolCtx)) as any;
    const delegated = (await tools[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: task.taskId, agentType: 'worker' } as any,
      toolCtx,
    )) as any;
    await poll(() => childAgent.streamCalls.length === 1);
    releaseParent();
    await poll(() => !parent.isRunning());

    const reset = await parent.resetPlanTasks({ reason: 'message_edited' });
    expect(reset).toEqual({
      deletedCount: 1,
      delegatedSubagentSessionIds: [delegated.subagentSessionId],
      cancelledSubagentSessionIds: [delegated.subagentSessionId],
    });
    expect((await parent._internalStorage.listPlanTasks({ sessionId: parent.id, limit: 100 })).tasks).toEqual([]);
    expect(parent.getDisplayState().planTasks).toEqual({
      total: 0,
      byStatus: {},
      inProgressTaskIds: [],
      rootCount: 0,
    });
    const childRecord = await harness.loadSession({ sessionId: delegated.subagentSessionId, includeClosed: true });
    expect(childRecord?.cancelRequest).toMatchObject({ reason: 'message_edited', requestedBy: parent.id });

    // A late provider settlement cannot resurrect the deleted task, and reset
    // does not permanently cancel the parent conversation session.
    releaseChild();
    await poll(async () => {
      const page = await parent._internalStorage.listPlanTasks({ sessionId: parent.id, limit: 100 });
      return page.tasks.length === 0;
    });
    await expect(parent.message({ content: 'replacement branch' })).resolves.toBeDefined();
    await harness.shutdown();
  });

  it('resetPlanTasks fails a suspended delegation immediately and releases its concurrency slot', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB(), { maxConcurrent: 1 });
    childAgent.enqueueRun({
      finishReason: 'suspended',
      suspendPayload: {
        toolCallId: 'stale-approval',
        toolName: 'write_file',
        args: { path: 'main.tex' },
      },
    });
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const releaseParent = await openParentTurn(parent, parentAgent);
    const tools = createPlanTaskTools(parent);
    const task = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content: 'stale suspended work' } as any, toolCtx)) as any;
    const delegated = (await tools[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: task.taskId, agentType: 'worker' } as any,
      toolCtx,
    )) as any;
    await poll(async () => {
      const child = await parent._internalStorage.loadSession({ sessionId: delegated.subagentSessionId });
      return child?.pendingResume?.toolCallId === 'stale-approval';
    });
    expect(parent._internalSubagentExecutionsInFlight).toBe(1);
    releaseParent();
    await poll(() => !parent.isRunning());

    await expect(parent.resetPlanTasks({ reason: 'message_edited' })).resolves.toMatchObject({
      deletedCount: 1,
      cancelledSubagentSessionIds: [delegated.subagentSessionId],
    });
    await poll(() => parent._internalSubagentExecutionsInFlight === 0);
    expect(
      (await harness.loadSession({ sessionId: delegated.subagentSessionId, includeClosed: true }))?.cancelRequest,
    ).toMatchObject({ reason: 'message_edited', requestedBy: parent.id });

    // The maxConcurrent=1 reservation was released by durable failed signal
    // evidence, so a replacement branch can delegate immediately rather than
    // waiting for the ten-minute HITL expiry.
    childAgent.enqueueRun({ finishReason: 'stop', text: 'replacement done' });
    const releaseReplacementParent = await openParentTurn(parent, parentAgent);
    const replacementTask = (await tools[TASK_ADD_TOOL_ID]!.execute!(
      { content: 'replacement work' } as any,
      toolCtx,
    )) as any;
    await expect(
      tools[TASK_DELEGATE_TOOL_ID]!.execute!({ taskId: replacementTask.taskId, agentType: 'worker' } as any, toolCtx),
    ).resolves.toMatchObject({ status: 'in_progress', subagentSessionId: expect.any(String) });
    await poll(async () => (await planRow(parent, replacementTask.taskId))?.status === 'completed');
    releaseReplacementParent();
    await harness.shutdown();
  });

  it('preserves the plan when a cold child is locked, then cancels and clears it on retry', async () => {
    const db = new InMemoryDB();
    const a = buildHarness(db);
    const parentA = await a.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    let releaseChild!: () => void;
    a.childAgent.enqueueRun({
      holdUntil: new Promise<void>(resolve => (releaseChild = resolve)),
      finishReason: 'stop',
    });

    const releaseParent = await openParentTurn(parentA, a.parentAgent);
    const tools = createPlanTaskTools(parentA);
    const task = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content: 'cold stale work' } as any, toolCtx)) as any;
    const delegated = (await tools[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: task.taskId, agentType: 'worker' } as any,
      toolCtx,
    )) as any;
    await poll(() => a.childAgent.streamCalls.length === 1);
    releaseParent();
    await poll(() => !parentA.isRunning());

    const harnessName = parentA.getRecord().harnessName;
    const parentKey = `${harnessName}\u0000${parentA.id}`;
    const parentRecord = db.harnessSessions.get(parentKey)!;
    db.harnessSessions.set(parentKey, {
      ...parentRecord,
      ownerId: 'crashed-parent',
      leaseExpiresAt: Date.now() - 1,
    });

    // B can recover the parent, but the child remains genuinely owned by A.
    // Reset must fail without deleting the only durable ownership edge.
    const b = buildHarness(db);
    const parentB = await b.harness.session({ sessionId: parentA.id, resourceId: 'u1' });
    await expect(parentB.resetPlanTasks({ reason: 'message_edited' })).rejects.toThrow(/plan was preserved for retry/u);
    expect((await planRow(parentB, task.taskId))?.delegatedSubagentSessionId).toBe(delegated.subagentSessionId);

    // Once the crashed child lease expires, the same operation hydrates it by
    // exact id, persists the parent-originated cancel, and atomically clears the
    // now-invalid plan forest.
    const childKey = `${harnessName}\u0000${delegated.subagentSessionId}`;
    const childRecord = db.harnessSessions.get(childKey)!;
    db.harnessSessions.set(childKey, {
      ...childRecord,
      ownerId: 'crashed-child',
      leaseExpiresAt: Date.now() - 1,
    });
    const reset = await parentB.resetPlanTasks({ reason: 'message_edited' });
    expect(reset).toEqual({
      deletedCount: 1,
      delegatedSubagentSessionIds: [delegated.subagentSessionId],
      cancelledSubagentSessionIds: [delegated.subagentSessionId],
    });
    expect((await parentB._internalStorage.listPlanTasks({ sessionId: parentB.id, limit: 100 })).tasks).toEqual([]);
    expect(
      (await b.harness.loadSession({ sessionId: delegated.subagentSessionId, includeClosed: true }))?.cancelRequest,
    ).toMatchObject({ reason: 'message_edited', requestedBy: parentB.id });

    releaseChild();
    await expect(parentB.message({ content: 'replacement after recovery' })).resolves.toBeDefined();
    await b.harness.shutdown();
  });

  it('preserves the root edge when a nested grandchild is locked, then resumes the cold cascade on retry', async () => {
    const db = new InMemoryDB();
    const a = buildHarness(db);
    const parentA = await a.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    let releaseChild!: () => void;
    let releaseGrandchild!: () => void;
    a.childAgent.enqueueRun({
      holdUntil: new Promise<void>(resolve => (releaseChild = resolve)),
      finishReason: 'stop',
    });
    a.childAgent.enqueueRun({
      holdUntil: new Promise<void>(resolve => (releaseGrandchild = resolve)),
      finishReason: 'stop',
    });

    const releaseParent = await openParentTurn(parentA, a.parentAgent);
    const parentTools = createPlanTaskTools(parentA);
    const parentTask = (await parentTools[TASK_ADD_TOOL_ID]!.execute!(
      { content: 'parent stale work' } as any,
      toolCtx,
    )) as any;
    const childDelegation = (await parentTools[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: parentTask.taskId, agentType: 'worker' } as any,
      toolCtx,
    )) as any;
    await poll(() => a.childAgent.streamCalls.length === 1);

    const childA = await a.harness.session({
      sessionId: childDelegation.subagentSessionId,
      resourceId: 'u1',
    });
    const childTools = createPlanTaskTools(childA);
    const childTask = (await childTools[TASK_ADD_TOOL_ID]!.execute!(
      { content: 'grandchild stale work' } as any,
      toolCtx,
    )) as any;
    const grandchildDelegation = (await childTools[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: childTask.taskId, agentType: 'worker' } as any,
      toolCtx,
    )) as any;
    await poll(() => a.childAgent.streamCalls.length === 2);
    releaseParent();
    await poll(() => !parentA.isRunning());

    const harnessName = parentA.getRecord().harnessName;
    const childKey = `${harnessName}\u0000${childDelegation.subagentSessionId}`;
    const childRecord = db.harnessSessions.get(childKey)!;
    // Model a crash after the child's durable cancel CAS but before its
    // descendant cascade ran.
    db.harnessSessions.set(childKey, {
      ...childRecord,
      cancelRequest: {
        requestedAt: Date.now(),
        reason: 'message_edited',
        requestedBy: parentA.id,
      },
    });
    const grandchildKey = `${harnessName}\u0000${grandchildDelegation.subagentSessionId}`;
    for (const [key, record] of db.harnessSessions) {
      if (record.ownerId === (parentA as any)._internalOwnerId) {
        db.harnessSessions.set(key, {
          ...record,
          ownerId: key === grandchildKey ? 'other-live-worker' : 'crashed-tree',
          leaseExpiresAt: key === grandchildKey ? Date.now() + 60_000 : Date.now() - 1,
        });
      }
    }

    const b = buildHarness(db);
    const parentB = await b.harness.session({ sessionId: parentA.id, resourceId: 'u1' });
    // Recreate the exact crash image after B evicts A's live parent object: the
    // parent still owns C, and already-cancelled C still owns G. No live driver
    // is allowed to be the only source of these recovery edges.
    for (const [key, row] of db.harnessPlanTasks) {
      if (row.sessionId === parentA.id && row.taskId === parentTask.taskId) {
        db.harnessPlanTasks.set(key, {
          ...row,
          status: 'in_progress',
          statusSource: 'explicit',
          delegatedSubagentSessionId: childDelegation.subagentSessionId,
          delegatedSubagentTypeId: 'worker',
          version: row.version + 1,
        });
      }
      if (row.sessionId === childDelegation.subagentSessionId && row.taskId === childTask.taskId) {
        db.harnessPlanTasks.set(key, {
          ...row,
          status: 'in_progress',
          statusSource: 'explicit',
          delegatedSubagentSessionId: grandchildDelegation.subagentSessionId,
          delegatedSubagentTypeId: 'worker',
          version: row.version + 1,
        });
      }
    }
    await expect(parentB.resetPlanTasks({ reason: 'message_edited' })).rejects.toThrow(/plan was preserved for retry/u);
    expect((await planRow(parentB, parentTask.taskId))?.delegatedSubagentSessionId).toBe(
      childDelegation.subagentSessionId,
    );
    const lockedGrandchild = db.harnessSessions.get(grandchildKey)!;
    db.harnessSessions.set(grandchildKey, {
      ...lockedGrandchild,
      ownerId: 'crashed-grandchild',
      leaseExpiresAt: Date.now() - 1,
    });

    await expect(parentB.resetPlanTasks({ reason: 'message_edited' })).resolves.toMatchObject({
      deletedCount: 1,
      delegatedSubagentSessionIds: [childDelegation.subagentSessionId],
    });
    expect(
      (
        await b.harness.loadSession({
          sessionId: grandchildDelegation.subagentSessionId,
          includeClosed: true,
        })
      )?.cancelRequest,
    ).toMatchObject({
      reason: 'message_edited',
      requestedBy: childDelegation.subagentSessionId,
    });

    releaseChild();
    releaseGrandchild();
    await b.harness.shutdown();
  });

  it('abortActiveWork stops the root and detached child but leaves the plan failure visible for retry', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    let releaseChild!: () => void;
    childAgent.enqueueRun({ holdUntil: new Promise<void>(resolve => (releaseChild = resolve)), finishReason: 'stop' });

    const releaseParent = await openParentTurn(parent, parentAgent);
    const tools = createPlanTaskTools(parent);
    const task = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content: 'stop all work' } as any, toolCtx)) as any;
    const delegated = (await tools[TASK_DELEGATE_TOOL_ID]!.execute!(
      { taskId: task.taskId, agentType: 'worker' } as any,
      toolCtx,
    )) as any;
    await poll(() => childAgent.streamCalls.length === 1);

    await expect(parent.abortActiveWork({ reason: 'user_requested' })).resolves.toEqual({
      cancelledSubagentSessionIds: [delegated.subagentSessionId],
    });
    releaseParent();
    releaseChild();
    await poll(async () => (await planRow(parent, task.taskId))?.status === 'failed');
    expect((await planRow(parent, task.taskId))?.delegatedSubagentSessionId).toBeUndefined();
    expect(parent.getRecord().cancelRequest).toBeUndefined();
    await expect(parent.message({ content: 'continue after stop' })).resolves.toBeDefined();
    await harness.shutdown();
  });

  it('abortActiveWork settlement waits for the aborted provider tail before returning', async () => {
    const { harness, parentAgent } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    let releaseFlush!: () => void;
    const flushGate = new Promise<void>(resolve => (releaseFlush = resolve));
    const awaitFlush = vi.spyOn(parent, '_internalAwaitFlushChain').mockImplementation(async () => {
      await flushGate;
    });
    let settled = false;

    const abort = parent.abortActiveWork({ reason: 'message_edited', settleTimeoutMs: 1_000 }).then(result => {
      settled = true;
      return result;
    });
    await new Promise(resolve => setTimeout(resolve, 20));
    expect(settled).toBe(false);

    releaseFlush();
    await expect(abort).resolves.toEqual({ cancelledSubagentSessionIds: [] });
    expect(awaitFlush).toHaveBeenCalledTimes(1);
    expect(parent.isRunning()).toBe(false);
    parentAgent.enqueueRun({ finishReason: 'stop', text: 'replacement' });
    await expect(parent.message({ content: 'replacement after settle' })).resolves.toBeDefined();
    await harness.shutdown();
  });

  it('abortActiveWork discards a parked root interaction and keeps the session reusable', async () => {
    const { harness, parentAgent } = buildHarness(new InMemoryDB());
    parentAgent.enqueueRun({
      finishReason: 'suspended',
      suspendPayload: {
        toolCallId: 'root-approval',
        toolName: 'write_file',
        args: { path: 'main.tex' },
      },
    });
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    await expect(parent.message({ content: 'edit main.tex' })).resolves.toMatchObject({
      finishReason: 'suspended',
    });
    expect(parent.getRecord().pendingResume?.toolCallId).toBe('root-approval');

    await expect(parent.abortActiveWork({ reason: 'message_edited', settleTimeoutMs: 1_000 })).resolves.toEqual({
      cancelledSubagentSessionIds: [],
    });
    expect(parent.getRecord().pendingResume).toBeUndefined();
    expect(parent.getRecord().cancelRequest).toBeUndefined();

    parentAgent.enqueueRun({ finishReason: 'stop', text: 'replacement complete' });
    await expect(parent.message({ content: 'replacement edit' })).resolves.toMatchObject({
      finishReason: 'stop',
    });
    await harness.shutdown();
  });

  it('keeps reconciliation retryable beyond the old finite ceiling without timer fan-out', async () => {
    const { harness } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    vi.useFakeTimers();
    try {
      const reconcile = vi.spyOn(parent as any, '_reconcileDelegationsOnHydrate').mockResolvedValue(undefined);

      // Attempt 99 would have been permanently dropped by the former five-try
      // ceiling. It is now accepted with a capped 30-second delay.
      (parent as any)._scheduleDelegationReconcileRetry(99);
      (parent as any)._scheduleDelegationReconcileRetry(100);
      expect((parent as any)._delegationReconcileRetryTimers.size).toBe(1);

      await vi.advanceTimersByTimeAsync(30_000);
      expect(reconcile).toHaveBeenCalledTimes(1);
      expect(reconcile).toHaveBeenCalledWith(100);
      expect((parent as any)._delegationReconcileRetryTimers.size).toBe(0);
    } finally {
      vi.useRealTimers();
      await harness.shutdown();
    }
  });

  it('does not lose a reconciliation wakeup that arrives while a scan is in flight', async () => {
    const { harness } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    let releaseFirst!: () => void;
    let firstEntered!: () => void;
    const firstGate = new Promise<void>(resolve => (releaseFirst = resolve));
    const entered = new Promise<void>(resolve => (firstEntered = resolve));
    let passes = 0;
    const pass = vi.spyOn(parent as any, '_reconcileDelegationsOnHydratePass').mockImplementation(async () => {
      passes += 1;
      if (passes === 1) {
        firstEntered();
        await firstGate;
      }
    });

    const first = parent._reconcileDelegationsOnHydrate(4);
    await entered;
    // Models a retry timer or capacity-release kick firing while a later task is
    // still holding the current pass open. Both callers join one singleflight,
    // but the current owner must perform another complete scan before settling.
    const overlappingWakeup = parent._reconcileDelegationsOnHydrate(5);
    releaseFirst();
    await Promise.all([first, overlappingWakeup]);

    expect(pass).toHaveBeenCalledTimes(2);
    expect((parent as any)._delegationReconcileInFlight).toBeUndefined();
    expect((parent as any)._delegationReconcileRerunRequested).toBe(false);
    await harness.shutdown();
  });
});
