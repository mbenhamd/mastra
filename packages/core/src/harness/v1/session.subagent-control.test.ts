/**
 * PF-7473 — the model-facing `subagent` control tool (status / wait / send /
 * cancel) over a parent session's own children.
 *
 * Children are started the way a model starts them (`task_delegate`), and the
 * control tool is driven through its public `execute` seam. The cross-process
 * cases use several `Harness` instances over ONE `InMemoryDB`, with a foreign
 * lease left on the child row to stand in for another (crashed or live) owner.
 */

import { describe, expect, it, vi } from 'vitest';

import { Agent } from '../../agent';
import { convertArrayToReadableStream, MockLanguageModelV2 } from '../../agent/__tests__/mock-model';
import { InMemoryHarness } from '../../storage/domains/harness/inmemory';
import { InMemoryDB } from '../../storage/domains/inmemory-db';

import { MockAgent } from './__test-utils__';
import type { HarnessEvent } from './events';
import { Harness } from './harness';
import { createPlanTaskTools, TASK_ADD_TOOL_ID, TASK_DELEGATE_TOOL_ID } from './plan-task-tool';
import type { ActiveSubagentState, Session } from './session';
import { SPAWN_SUBAGENT_TOOL_ID } from './spawn-subagent-tool';
import {
  createSubagentControlTool,
  SUBAGENT_CONTROL_RESULTS_SHORTENED_HINT,
  SUBAGENT_CONTROL_TOOL_ID,
  SUBAGENT_CONTROL_WAIT_READ_STALL_MS,
} from './subagent-control-tool';
import {
  HARNESS_SUBAGENT_OUTCOME_REPORT_KIND,
  HARNESS_SUBAGENT_OUTCOME_REPORT_TOOL_ID,
} from './terminal-subagent-result';

function completedOutcome(summary: string) {
  return {
    status: 'success' as const,
    items: [
      {
        toolName: HARNESS_SUBAGENT_OUTCOME_REPORT_TOOL_ID,
        toolCallId: 'report-call-1',
        status: 'success' as const,
        value: {
          kind: HARNESS_SUBAGENT_OUTCOME_REPORT_KIND,
          outcome: 'completed',
          summary,
          evidence: [{ kind: 'analysis', description: 'Verified by the control-tool fixture' }],
        },
      },
    ],
  };
}

function subagentsConfig() {
  return {
    maxDepth: 2,
    types: {
      worker: {
        agentId: 'child-agent',
        modeId: 'worker-mode',
        description: 'Background worker subagent',
        defaultModelId: 'openai/gpt-4o-mini',
        workspace: 'inherit' as const,
      },
    },
  };
}

function buildHarness(db: InMemoryDB, subagents: Record<string, unknown> = subagentsConfig()) {
  const parentAgent = new MockAgent({ id: 'parent-agent' });
  const childAgent = new MockAgent({
    id: 'child-agent',
    defaultOutput: { terminalToolResult: completedOutcome('Worker finished the survey') },
  });
  const harness = new Harness({
    agents: { 'parent-agent': parentAgent, 'child-agent': childAgent } as any,
    modes: [
      { id: 'default', agentId: 'parent-agent' },
      { id: 'worker-mode', agentId: 'child-agent' },
    ],
    defaultModeId: 'default',
    sessions: { storage: new InMemoryHarness({ db }) },
    subagents: subagents as any,
  });
  return { harness, parentAgent, childAgent };
}

function toolCtxFor(runId: string) {
  return {
    abortSignal: new AbortController().signal,
    agent: { toolCallId: 'tc-control', runId },
    requestContext: { get: () => undefined },
  } as any;
}

const toolCtx = toolCtxFor('mock-run');

async function poll(pred: () => boolean | Promise<boolean>, timeoutMs = 2000): Promise<void> {
  const start = Date.now();
  for (;;) {
    if (await pred()) return;
    if (Date.now() - start > timeoutMs) throw new Error('poll timeout');
    await new Promise(r => setTimeout(r, 5));
  }
}

async function openParentTurn(parent: Session, agent: MockAgent): Promise<() => void> {
  let release!: () => void;
  agent.enqueueRun({ holdUntil: new Promise<void>(r => (release = r)), finishReason: 'stop', text: 'done' });
  const turn = parent.message({ content: 'go' });
  await poll(() => agent.streamCalls.length > 0);
  return () => {
    release();
    void turn.catch(() => {});
  };
}

/** Delegate one plan task; the child's run is held until the returned release fires. */
async function delegateHeldChild(
  parent: Session,
  childAgent: MockAgent,
  content: string,
  summary = `${content}: done`,
  taskBody?: string,
) {
  let releaseChild!: () => void;
  const runsBefore = childAgent.streamCalls.length;
  childAgent.enqueueRun({
    holdUntil: new Promise<void>(r => (releaseChild = r)),
    finishReason: 'stop',
    terminalToolResult: completedOutcome(summary),
  });
  const tools = createPlanTaskTools(parent);
  const task = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content } as any, toolCtx)) as any;
  const delegated = (await tools[TASK_DELEGATE_TOOL_ID]!.execute!(
    { taskId: task.taskId, agentType: 'worker', ...(taskBody !== undefined ? { task: taskBody } : {}) } as any,
    toolCtx,
  )) as any;
  await poll(() => childAgent.streamCalls.length > runsBefore);
  return { taskId: task.taskId as string, childId: delegated.subagentSessionId as string, releaseChild };
}

/** An own child suspended on a pending interaction (awaiting input, not settled). */
async function awaitingInputChild(harness: Harness, parent: Session): Promise<Session> {
  const child = await harness.session({
    resourceId: 'u1',
    threadId: { fresh: true },
    parentSessionId: parent.id,
    origin: 'subagent-tool',
    modeId: 'worker-mode',
    subagentDepth: 1,
  });
  const now = Date.now();
  await (child as any)._flushUpdate((prev: any) => ({
    ...prev,
    pendingResume: {
      kind: 'tool-approval',
      runId: 'run-suspended',
      toolCallId: 'tool-call-suspended',
      source: 'parent',
      requestedAt: now,
      expiresAt: now + 600_000,
    },
  }));
  return child;
}

/** The control tool as the model calls it from run `runId`. */
function control(parent: Session, runId = 'mock-run') {
  const tool = createSubagentControlTool(parent);
  if (!tool) throw new Error('subagent control tool not registered');
  const ctx = toolCtxFor(runId);
  return (input: Record<string, unknown>) => tool.execute!(input as any, ctx) as Promise<any>;
}

describe('subagent control tool', () => {
  it('status and wait report a delegated child running, then its settled result', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const closeTurn = await openParentTurn(parent, parentAgent);
    const { taskId, childId, releaseChild } = await delegateHeldChild(parent, childAgent, 'survey the corpus');
    const run = control(parent);

    const listed = await run({ action: 'status' });
    expect(listed.subagents).toEqual([
      expect.objectContaining({
        subagentSessionId: childId,
        source: 'delegated',
        status: 'running',
        settled: false,
        taskId,
        agentType: 'worker',
      }),
    ]);

    const waiting = run({ action: 'wait', subagentSessionId: childId, timeoutMs: 5_000 });
    releaseChild();
    const waited = await waiting;
    expect(waited.timedOut).toBe(false);
    expect(waited.runningSubagents).toEqual([]);
    expect(waited.settledSubagents).toEqual([
      expect.objectContaining({
        subagentSessionId: childId,
        status: 'completed',
        settled: true,
        result: expect.objectContaining({ text: 'survey the corpus: done' }),
      }),
    ]);

    closeTurn();
    await harness.shutdown();
  });

  it('wait returns the still-running child when the timeout passes', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const closeTurn = await openParentTurn(parent, parentAgent);
    const { childId, releaseChild } = await delegateHeldChild(parent, childAgent, 'long job');

    const started = Date.now();
    const waited = await control(parent)({ action: 'wait', timeoutMs: 150 });
    expect(Date.now() - started).toBeGreaterThanOrEqual(140);
    expect(waited.timedOut).toBe(true);
    expect(waited.settledSubagents).toEqual([]);
    expect(waited.runningSubagents).toEqual([
      expect.objectContaining({ subagentSessionId: childId, status: 'running' }),
    ]);

    releaseChild();
    closeTurn();
    await harness.shutdown();
  });

  it('send interleaves a system reminder into the running child run', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const closeTurn = await openParentTurn(parent, parentAgent);
    const { childId, releaseChild } = await delegateHeldChild(parent, childAgent, 'draft the summary');
    await poll(() => harness._internalGetLiveSession(childId)?.isRunning() === true);
    const sendSignal = vi.spyOn(childAgent, 'sendSignal');

    const sent = await control(parent)({
      action: 'send',
      subagentSessionId: childId,
      message: 'Limit the summary to trials after 2020.',
    });

    expect(sent).toMatchObject({ action: 'send', subagentSessionId: childId, delivered: true });
    expect(sendSignal).toHaveBeenCalledWith(
      expect.objectContaining({
        type: 'system-reminder',
        contents: 'Limit the summary to trials after 2020.',
        attributes: { source: 'parent_session', parentSessionId: parent.id },
      }),
      expect.anything(),
    );

    releaseChild();
    closeTurn();
    await harness.shutdown();
  });

  it('a sent message the child run ends without consuming never starts a follow-up run', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const closeTurn = await openParentTurn(parent, parentAgent);
    const { childId, releaseChild } = await delegateHeldChild(parent, childAgent, 'write the appendix');
    await poll(() => harness._internalGetLiveSession(childId)?.isRunning() === true);
    const run = control(parent);
    // The held mock run never drains queued signals, so it ends without
    // consuming the message.
    expect(await run({ action: 'send', subagentSessionId: childId, message: 'Also add a glossary.' })).toMatchObject({
      delivered: true,
    });

    releaseChild();
    const waited = await run({ action: 'wait', subagentSessionId: childId, timeoutMs: 5_000 });
    expect(waited.settledSubagents).toEqual([
      expect.objectContaining({ subagentSessionId: childId, status: 'completed' }),
    ]);
    await new Promise(r => setTimeout(r, 100));

    expect(childAgent.streamCalls).toHaveLength(1);
    closeTurn();
    await harness.shutdown();
  });

  it('cancel acknowledges a live child, then the delegation settles cancelled; a second cancel is a no-op', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const closeTurn = await openParentTurn(parent, parentAgent);
    const { childId, releaseChild } = await delegateHeldChild(parent, childAgent, 'cancel me');
    const run = control(parent);

    const cancelled = await run({ action: 'cancel', subagentSessionId: childId });
    expect(cancelled).toMatchObject({ action: 'cancel', subagentSessionId: childId, cancel: 'acknowledged' });

    releaseChild();
    const waited = await run({ action: 'wait', subagentSessionId: childId, timeoutMs: 5_000 });
    expect(waited.settledSubagents).toEqual([
      expect.objectContaining({ subagentSessionId: childId, status: 'cancelled', cancelRequested: true }),
    ]);

    const again = await run({ action: 'cancel', subagentSessionId: childId });
    expect(again).toMatchObject({ cancel: 'already_settled', settled: true, status: 'cancelled' });

    closeTurn();
    await harness.shutdown();
  });

  it('cancel after a child settled is a no-op that records no cancel request', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const closeTurn = await openParentTurn(parent, parentAgent);
    const { childId, releaseChild } = await delegateHeldChild(parent, childAgent, 'quick job');
    releaseChild();
    const run = control(parent);
    await run({ action: 'wait', subagentSessionId: childId, timeoutMs: 5_000 });

    const cancelled = await run({ action: 'cancel', subagentSessionId: childId });

    expect(cancelled).toMatchObject({ cancel: 'already_settled', settled: true, status: 'completed' });
    const stored = await parent._internalStorage.loadSession({ sessionId: childId });
    expect(stored?.cancelRequest).toBeUndefined();

    closeTurn();
    await harness.shutdown();
  });

  it('rejects a session that is not its own child and leaves it untouched', async () => {
    const { harness } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const other = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const foreignChild = await harness.session({
      resourceId: 'u1',
      threadId: { fresh: true },
      parentSessionId: other.id,
      origin: 'subagent-tool',
      subagentDepth: 1,
    });
    const run = control(parent);

    for (const input of [
      { action: 'status', subagentSessionId: foreignChild.id },
      { action: 'wait', subagentSessionId: foreignChild.id, timeoutMs: 0 },
      { action: 'send', subagentSessionId: foreignChild.id, message: 'stop' },
      { action: 'cancel', subagentSessionId: foreignChild.id },
      { action: 'cancel', subagentSessionId: 'no-such-session' },
    ]) {
      expect(await run(input)).toMatchObject({ isError: true, errorName: 'HarnessValidationError' });
    }
    expect(foreignChild.getRecord().cancelRequest).toBeUndefined();

    await harness.shutdown();
  });

  it('cancel of a child whose crashed holder lease lapsed commits durably on the child row', async () => {
    const db = new InMemoryDB();
    const a = buildHarness(db);
    const b = buildHarness(db);
    const parent = await a.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const child = await b.harness.session({
      resourceId: 'u1',
      threadId: { fresh: true },
      parentSessionId: parent.id,
      origin: 'subagent-tool',
      modeId: 'worker-mode',
      subagentDepth: 1,
      subagentTypeId: 'worker',
    });
    await b.harness.shutdown();
    // A crashed holder whose lease has already expired.
    const storage = parent._internalStorage;
    await storage.acquireSessionLease({ sessionId: child.id, ownerId: 'crashed-holder', ttlMs: 1 });
    await new Promise(r => setTimeout(r, 5));

    const cancelled = await control(parent)({ action: 'cancel', subagentSessionId: child.id });

    expect(cancelled).toMatchObject({ action: 'cancel', cancel: 'acknowledged' });
    const stored = await storage.loadSession({ sessionId: child.id });
    expect(stored?.cancelRequest).toMatchObject({ reason: 'subagent_cancelled', requestedBy: parent.id });
    // The cancel lives on the child's own row: any process that hydrates it refuses new work.
    const c = buildHarness(db);
    await a.harness.shutdown();
    const rehydrated = await c.harness.session({ sessionId: child.id, resourceId: 'u1' });
    await expect(rehydrated.message({ content: 'keep going' })).rejects.toMatchObject({
      name: 'HarnessSessionCancelledError',
    });
    await c.harness.shutdown();
  });

  it('cancel never writes under a lease another process keeps holding', async () => {
    const db = new InMemoryDB();
    const a = buildHarness(db);
    const b = buildHarness(db);
    const parent = await a.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const child = await b.harness.session({
      resourceId: 'u1',
      threadId: { fresh: true },
      parentSessionId: parent.id,
      origin: 'subagent-tool',
      subagentDepth: 1,
    });
    await b.harness.shutdown();
    const storage = parent._internalStorage;
    await storage.acquireSessionLease({ sessionId: child.id, ownerId: 'busy-holder', ttlMs: 120_000 });
    const versionBefore = (await storage.loadSession({ sessionId: child.id }))?.version;

    const cancelled = await control(parent)({ action: 'cancel', subagentSessionId: child.id });

    expect(cancelled).toMatchObject({ isError: true, code: 'harness.subagent_cancel_unavailable' });
    const stored = await storage.loadSession({ sessionId: child.id });
    expect(stored?.cancelRequest).toBeUndefined();
    expect(stored?.version).toBe(versionBefore);
    expect(stored?.ownerId).toBe('busy-holder');
    await a.harness.shutdown();
  });

  it('after a restart, a cancelled child that kept its suspension evidence reads as settled, not awaiting input', async () => {
    const db = new InMemoryDB();
    const a = buildHarness(db);
    const b = buildHarness(db);
    const parent = await a.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const child = await b.harness.session({
      resourceId: 'u1',
      threadId: { fresh: true },
      parentSessionId: parent.id,
      origin: 'subagent-tool',
      modeId: 'worker-mode',
      subagentDepth: 1,
      subagentTypeId: 'worker',
    });
    // The child suspended, then was cancelled; the cancel keeps pendingResume
    // as audit evidence and nothing closed the child before its process stopped.
    const now = Date.now();
    await (child as any)._flushUpdate((prev: any) => ({
      ...prev,
      pendingResume: {
        kind: 'tool-approval',
        runId: 'run-suspended',
        toolCallId: 'tool-call-suspended',
        source: 'parent',
        requestedAt: now,
        expiresAt: now + 600_000,
      },
    }));
    await child.cancel({ reason: 'subagent_cancelled' });
    await b.harness.shutdown();
    const stored = await parent._internalStorage.loadSession({ sessionId: child.id });
    expect(stored).toMatchObject({ pendingResume: expect.anything(), cancelRequest: expect.anything() });
    expect(stored?.closedAt).toBeUndefined();

    const run = control(parent);
    const inspected = await run({ action: 'status', subagentSessionId: child.id });
    expect(inspected.subagents).toEqual([
      expect.objectContaining({ subagentSessionId: child.id, cancelRequested: true, settled: true }),
    ]);
    expect(inspected.subagents[0].status).not.toBe('awaiting_input');
    const waited = await run({ action: 'wait', subagentSessionId: child.id, timeoutMs: 200 });
    expect(waited).toMatchObject({ action: 'wait', timedOut: false, runningSubagents: [] });
    expect(waited.settledSubagents).toEqual([expect.objectContaining({ subagentSessionId: child.id })]);
    await a.harness.shutdown();
  });

  it('wait stops at its timeout, or promptly on abort, while a later storage read stalls', async () => {
    const { harness } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const child = await awaitingInputChild(harness, parent);
    const tool = createSubagentControlTool(parent)!;
    let releaseStorage!: () => void;
    const gate = new Promise<void>(r => (releaseStorage = r));
    // The first read (ownership proof and initial views) answers; every later one stalls.
    let reads = 0;
    const original = InMemoryHarness.prototype.listPlanTasks;
    const stall = vi.spyOn(InMemoryHarness.prototype, 'listPlanTasks').mockImplementation(function (
      this: InMemoryHarness,
      ...args: Parameters<InMemoryHarness['listPlanTasks']>
    ) {
      reads += 1;
      return reads === 1 ? original.apply(this, args) : gate.then(() => original.apply(this, args));
    });
    const guard = (ms: number) => {
      let timer!: ReturnType<typeof setTimeout>;
      const stalled = new Promise<'stalled'>(r => (timer = setTimeout(() => r('stalled'), ms)));
      return { stalled, clear: () => clearTimeout(timer) };
    };
    try {
      let started = Date.now();
      let bound = guard(SUBAGENT_CONTROL_WAIT_READ_STALL_MS + 2_000);
      const timed = await Promise.race([
        tool.execute!({ action: 'wait', subagentSessionId: child.id, timeoutMs: 50 } as any, toolCtx),
        bound.stalled,
      ]);
      bound.clear();
      expect(timed).toMatchObject({
        action: 'wait',
        timedOut: true,
        settledSubagents: [],
        runningSubagents: [expect.objectContaining({ subagentSessionId: child.id, status: 'awaiting_input' })],
      });
      expect(Date.now() - started).toBeGreaterThanOrEqual(45);

      reads = 0;
      const abort = new AbortController();
      started = Date.now();
      // Past the first 100 ms poll interval, so the abort lands on a stalled read.
      setTimeout(() => abort.abort(), 150);
      bound = guard(1_000);
      const aborted = await Promise.race([
        tool.execute!({ action: 'wait', subagentSessionId: child.id, timeoutMs: 300_000 } as any, {
          ...toolCtx,
          abortSignal: abort.signal,
        }),
        bound.stalled,
      ]);
      bound.clear();
      expect(aborted).toMatchObject({ action: 'wait', timedOut: false, aborted: true });
      expect(reads).toBeGreaterThanOrEqual(2);
      expect(Date.now() - started).toBeLessThan(500);
    } finally {
      releaseStorage();
      stall.mockRestore();
    }
    await harness.shutdown();
  });

  it('wait fails with a storage error, never an empty timeout, when its first storage read stalls', async () => {
    const { harness } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const child = await awaitingInputChild(harness, parent);
    let releaseStorage!: () => void;
    const gate = new Promise<void>(r => (releaseStorage = r));
    const original = InMemoryHarness.prototype.listPlanTasks;
    const stall = vi.spyOn(InMemoryHarness.prototype, 'listPlanTasks').mockImplementation(function (
      this: InMemoryHarness,
      ...args: Parameters<InMemoryHarness['listPlanTasks']>
    ) {
      return gate.then(() => original.apply(this, args));
    });
    let guardTimer!: ReturnType<typeof setTimeout>;
    const stalled = new Promise<'stalled'>(
      r => (guardTimer = setTimeout(() => r('stalled'), SUBAGENT_CONTROL_WAIT_READ_STALL_MS + 2_000)),
    );
    try {
      const started = Date.now();
      const waited = await Promise.race([
        control(parent)({ action: 'wait', subagentSessionId: child.id, timeoutMs: 50 }),
        stalled,
      ]);
      expect(waited).toMatchObject({ isError: true, errorName: 'HarnessStorageError', code: 'harness.storage' });
      expect(waited).not.toHaveProperty('timedOut');
      expect(Date.now() - started).toBeGreaterThanOrEqual(SUBAGENT_CONTROL_WAIT_READ_STALL_MS - 5);
    } finally {
      clearTimeout(guardTimer);
      releaseStorage();
      stall.mockRestore();
    }
    await harness.shutdown();
  });

  it('wait over storage with a few ms of latency still proves ownership and reports fresh settlement', async () => {
    const { harness } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const other = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const foreignChild = await harness.session({
      resourceId: 'u1',
      threadId: { fresh: true },
      parentSessionId: other.id,
      origin: 'subagent-tool',
      subagentDepth: 1,
    });
    const idleChild = await harness.session({
      resourceId: 'u1',
      threadId: { fresh: true },
      parentSessionId: parent.id,
      origin: 'subagent-tool',
      modeId: 'worker-mode',
      subagentDepth: 1,
    });
    const suspended = await awaitingInputChild(harness, parent);
    const original = InMemoryHarness.prototype.listPlanTasks;
    // Set for the last case: the first read that answers at or after this time
    // settles the suspended child before it reads. The read that ends the wait
    // answers at or after the deadline, so it always sees the settlement.
    let settleChildFrom: number | undefined;
    const latency = vi.spyOn(InMemoryHarness.prototype, 'listPlanTasks').mockImplementation(async function (
      this: InMemoryHarness,
      ...args: Parameters<InMemoryHarness['listPlanTasks']>
    ) {
      await new Promise(r => setTimeout(r, 5));
      if (settleChildFrom !== undefined && Date.now() >= settleChildFrom) {
        settleChildFrom = undefined;
        await (suspended as any)._flushUpdate((prev: any) => ({ ...prev, pendingResume: undefined }));
      }
      return original.apply(this, args);
    });
    try {
      const run = control(parent);
      expect(await run({ action: 'wait', subagentSessionId: foreignChild.id, timeoutMs: 0 })).toMatchObject({
        isError: true,
        errorName: 'HarnessValidationError',
      });
      for (const timeoutMs of [0, 3]) {
        expect(await run({ action: 'wait', subagentSessionId: idleChild.id, timeoutMs })).toMatchObject({
          timedOut: false,
          runningSubagents: [],
          settledSubagents: [expect.objectContaining({ subagentSessionId: idleChild.id, settled: true })],
        });
      }
      // The child settles as the deadline passes: the read that ends the wait must see it.
      const timeoutMs = 1_000;
      settleChildFrom = Date.now() + timeoutMs;
      expect(await run({ action: 'wait', subagentSessionId: suspended.id, timeoutMs })).toMatchObject({
        timedOut: false,
        runningSubagents: [],
        settledSubagents: [expect.objectContaining({ subagentSessionId: suspended.id, settled: true })],
      });
    } finally {
      latency.mockRestore();
    }
    await harness.shutdown();
  });

  it('send refuses a child with no active run instead of waking it', async () => {
    const { harness, childAgent } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const idleChild = await harness.session({
      resourceId: 'u1',
      threadId: { fresh: true },
      parentSessionId: parent.id,
      origin: 'subagent-tool',
      modeId: 'worker-mode',
      subagentDepth: 1,
    });

    const sent = await control(parent)({ action: 'send', subagentSessionId: idleChild.id, message: 'start now' });

    expect(sent).toMatchObject({ isError: true, errorName: 'HarnessValidationError' });
    await new Promise(r => setTimeout(r, 20));
    expect(childAgent.streamCalls).toHaveLength(0);
    expect(idleChild.isRunning()).toBe(false);
    await harness.shutdown();
  });

  it('is absent without subagent types and for a session already at maxDepth', async () => {
    const plain = new Harness({
      agents: { default: new MockAgent({ id: 'default' }) } as any,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage: new InMemoryHarness({ db: new InMemoryDB() }) },
    });
    const root = await plain.session({ resourceId: 'u1', threadId: { fresh: true } });
    expect(createSubagentControlTool(root)).toBeUndefined();
    await plain.shutdown();

    const { harness } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const deepest = await harness.session({
      resourceId: 'u1',
      threadId: { fresh: true },
      parentSessionId: parent.id,
      origin: 'subagent-tool',
      subagentDepth: 2,
    });
    expect(createSubagentControlTool(parent)).toBeDefined();
    expect(createSubagentControlTool(deepest)).toBeUndefined();
    await harness.shutdown();
  });
});

/** A summary of `bytes` ASCII bytes that starts with `label`. */
function largeSummary(label: string, bytes: number): string {
  return `${label} `.padEnd(bytes, 'x');
}

/** A summary of about `bytes` UTF-8 bytes in 4-byte characters. */
function multibyteSummary(label: string, bytes: number): string {
  return `${label} ${'\u{1F600}'.repeat(Math.floor((bytes - label.length - 1) / 4))}`;
}

/** A summary whose JSON form is about twice its UTF-8 size (every `"` is escaped). */
function escapedSummary(chars: number): string {
  return '"'.repeat(chars);
}

/**
 * What a returning spawn_subagent call does to an inline child registered
 * under `inline:<id>`: the parent drops its spawn record (keeping the child
 * listed for the rest of the run), then the child closes.
 */
async function returnInline(parent: Session, child: Session, runId = 'mock-run'): Promise<void> {
  parent._internalReturnInlineSubagent(`inline:${child.id}`, runId);
  await child.close();
}

function utf8Length(value: unknown): number {
  return new TextEncoder().encode(JSON.stringify(value)).byteLength;
}

/** Give a settled delegation's stored result a tool trace, as a child that called tools would have. */
function addStoredToolTrace(db: InMemoryDB, childId: string): void {
  for (const [key, task] of db.harnessPlanTasks) {
    const attempt = (task.metadata as any)?.mastraHarnessDelegationAttemptV1;
    if (attempt?.subagentSessionId !== childId || attempt.settlement?.result === undefined) continue;
    attempt.settlement.result.toolTrace = [{ toolName: 'searchPapers', calls: 2, errors: 0 }];
    db.harnessPlanTasks.set(key, { ...task, version: task.version + 1 });
    return;
  }
  throw new Error(`no settled delegation for ${childId}`);
}

type ResultPageRow = {
  name: string;
  /** Summary per delegated child. */
  summaries: string[];
  /** Task content for every child (defaults to a short label). */
  taskContent?: string;
  /** Children (by index) settled before the call. */
  settledBefore: number[];
  /** Children (by index) released while the call runs. */
  settledDuring: number[];
  /** Children (by index) whose stored result gets a tool trace once settled. */
  traced?: number[];
  call: (ids: string[]) => Record<string, unknown>;
  /** Expected result per child index in the output: full, preview, or omitted. */
  expected: Record<number, 'full' | 'preview' | 'omitted'>;
  shortened: boolean;
  /** The call names one child whose full result is larger than the page budget. */
  overBudgetSingle?: boolean;
  /** The full output is within the shortening notice's size of the budget. */
  nearBudget?: boolean;
};

/** Two results of this size fill a two-child wait output to within the shortening notice's size of 64 KiB. */
const NEAR_BUDGET_SUMMARY_BYTES = 32_080;

const many = <T>(count: number, make: (index: number) => T): T[] => Array.from({ length: count }, (_, i) => make(i));

const resultPageRows: ResultPageRow[] = [
  {
    name: 'wait naming two children whose full results just fit returns both in full, with no notice',
    summaries: [largeSummary('child-0', NEAR_BUDGET_SUMMARY_BYTES), largeSummary('child-1', NEAR_BUDGET_SUMMARY_BYTES)],
    settledBefore: [0, 1],
    settledDuring: [],
    call: ids => ({ action: 'wait', subagentSessionIds: ids, timeoutMs: 1_000 }),
    expected: { 0: 'full', 1: 'full' },
    shortened: false,
    nearBudget: true,
  },
  {
    name: 'status listing: a child settled before the call gets a preview',
    summaries: [largeSummary('child-0', 20_000)],
    settledBefore: [0],
    settledDuring: [],
    call: () => ({ action: 'status' }),
    expected: { 0: 'preview' },
    shortened: true,
  },
  {
    name: 'status naming a child settled before the call gets its full result',
    summaries: [largeSummary('child-0', 20_000)],
    settledBefore: [0],
    settledDuring: [],
    call: ids => ({ action: 'status', subagentSessionId: ids[0] }),
    expected: { 0: 'full' },
    shortened: false,
  },
  {
    name: 'status listing: a small result is complete as its preview and is not marked shortened',
    summaries: [largeSummary('child-0', 200)],
    settledBefore: [0],
    settledDuring: [],
    call: () => ({ action: 'status' }),
    expected: { 0: 'full' },
    shortened: false,
  },
  {
    name: 'status listing: a tiny result with a tool trace is returned in full, not shortened',
    summaries: [largeSummary('child-0', 200)],
    settledBefore: [0],
    settledDuring: [],
    traced: [0],
    call: () => ({ action: 'status' }),
    expected: { 0: 'full' },
    shortened: false,
  },
  {
    name: 'status listing three large children stays under the budget with previews',
    summaries: many(3, i => largeSummary(`child-${i}`, 40_000)),
    settledBefore: [0, 1, 2],
    settledDuring: [],
    call: () => ({ action: 'status' }),
    expected: { 0: 'preview', 1: 'preview', 2: 'preview' },
    shortened: true,
  },
  {
    name: 'wait for every running child: a child that settles during the call gets its full result',
    summaries: [largeSummary('child-0', 20_000)],
    settledBefore: [],
    settledDuring: [0],
    call: () => ({ action: 'wait', timeoutMs: 5_000 }),
    expected: { 0: 'full' },
    shortened: false,
  },
  {
    name: 'wait naming a child settled before the call gets its full result',
    summaries: [largeSummary('child-0', 20_000)],
    settledBefore: [0],
    settledDuring: [],
    call: ids => ({ action: 'wait', subagentSessionIds: ids, timeoutMs: 1_000 }),
    expected: { 0: 'full' },
    shortened: false,
  },
  {
    name: 'wait naming three large children over the budget: the first is full, the rest are previews',
    summaries: many(3, i => largeSummary(`child-${i}`, 40_000)),
    settledBefore: [0, 1, 2],
    settledDuring: [],
    call: ids => ({ action: 'wait', subagentSessionIds: ids, timeoutMs: 1_000 }),
    expected: { 0: 'full', 1: 'preview', 2: 'preview' },
    shortened: true,
  },
  {
    name: 'wait naming three children of 4-byte characters counts UTF-8 bytes, not characters',
    summaries: many(3, i => multibyteSummary(`child-${i}`, 30_000)),
    settledBefore: [0, 1, 2],
    settledDuring: [],
    call: ids => ({ action: 'wait', subagentSessionIds: ids, timeoutMs: 1_000 }),
    expected: { 0: 'full', 1: 'preview', 2: 'preview' },
    shortened: true,
  },
  {
    name: 'wait naming sixteen children: previews fill the page and the last results are left out',
    summaries: many(16, i => largeSummary(`child-${i}`, 8_000)),
    settledBefore: many(16, i => i),
    settledDuring: [],
    call: ids => ({ action: 'wait', subagentSessionIds: ids, timeoutMs: 1_000 }),
    expected: { 15: 'omitted' },
    shortened: true,
  },
  {
    name: 'status naming one child larger than the page budget returns its full result and no hint',
    summaries: [escapedSummary(32_500)],
    taskContent: '"'.repeat(500),
    settledBefore: [0],
    settledDuring: [],
    call: ids => ({ action: 'status', subagentSessionId: ids[0] }),
    expected: { 0: 'full' },
    shortened: false,
    overBudgetSingle: true,
  },
  {
    name: 'wait naming one child larger than the page budget returns its full result and no hint',
    summaries: [escapedSummary(32_500)],
    taskContent: '"'.repeat(500),
    settledBefore: [0],
    settledDuring: [],
    call: ids => ({ action: 'wait', subagentSessionIds: ids, timeoutMs: 1_000 }),
    expected: { 0: 'full' },
    shortened: false,
    overBudgetSingle: true,
  },
];

describe('subagent control tool — bounded result page', () => {
  it.each(resultPageRows)(
    '$name',
    async row => {
      const db = new InMemoryDB();
      const { harness, parentAgent, childAgent } = buildHarness(db);
      const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const closeTurn = await openParentTurn(parent, parentAgent);
      const run = control(parent);
      const children: Array<{ childId: string; releaseChild: () => void; summary: string }> = [];
      try {
        for (const [index, summary] of row.summaries.entries()) {
          const child = await delegateHeldChild(parent, childAgent, row.taskContent ?? `job ${index}`, summary);
          children.push({ ...child, summary });
        }
        const settled = async (index: number) =>
          (await run({ action: 'status', subagentSessionId: children[index]!.childId })).subagents?.[0]?.settled ===
          true;
        for (const index of row.settledBefore) {
          children[index]!.releaseChild();
          await poll(() => settled(index));
        }
        for (const index of row.traced ?? []) addStoredToolTrace(db, children[index]!.childId);

        const pending = run(row.call(children.map(child => child.childId)));
        for (const index of row.settledDuring) children[index]!.releaseChild();
        const output = await pending;

        expect(output.isError).toBeUndefined();
        if (!row.overBudgetSingle) expect(utf8Length(output)).toBeLessThanOrEqual(64 * 1024);
        const views: any[] = output.subagents ?? output.settledSubagents;
        for (const [index, expected] of Object.entries(row.expected)) {
          const child = children[Number(index)]!;
          const view = views.find(candidate => candidate.subagentSessionId === child.childId);
          expect(view, `child ${index}`).toBeDefined();
          if (expected === 'full') {
            expect(view.result.text).toBe(child.summary);
            expect(view.result.textTruncated).toBe(false);
            expect(view.resultPreview).toBeUndefined();
            if (row.traced?.includes(Number(index))) {
              expect(view.result.toolTrace).toEqual([{ toolName: 'searchPapers', calls: 2, errors: 0 }]);
            }
          } else if (expected === 'preview') {
            expect(view.resultPreview).toBe(true);
            expect(view.result.textTruncated).toBe(true);
            expect(child.summary.startsWith(view.result.text.split('\n')[0])).toBe(true);
            // A preview never splits a character: a lone surrogate makes encodeURIComponent throw.
            expect(() => encodeURIComponent(view.result.text)).not.toThrow();
            expect(new TextEncoder().encode(view.result.text).byteLength).toBeLessThan(5 * 1024);
          } else {
            expect(view.result).toBeUndefined();
            expect(view.resultOmitted).toBe(true);
          }
        }
        // The case is real only if the full result alone is over the page budget.
        if (row.overBudgetSingle) expect(utf8Length(output)).toBeGreaterThan(64 * 1024);
        // The case is real only if the shortening notice would not also fit.
        if (row.nearBudget) {
          const notice = { resultsShortened: true, resultsHint: SUBAGENT_CONTROL_RESULTS_SHORTENED_HINT };
          expect(utf8Length({ ...output, ...notice }), `output ${utf8Length(output)} B`).toBeGreaterThan(64 * 1024);
        }
        if (row.shortened) {
          expect(output.resultsShortened).toBe(true);
          expect(output.resultsHint).toMatch(/action "status"/);
        } else {
          expect(output.resultsShortened).toBeUndefined();
          expect(output.resultsHint).toBeUndefined();
        }
      } finally {
        for (const child of children) child.releaseChild();
        closeTurn();
        await harness.shutdown();
      }
    },
    30_000,
  );
});

type WaitRow = {
  name: string;
  children: number;
  /** Builds the wait input from the child ids. */
  input: (ids: string[]) => Record<string, unknown>;
  /** Drives settlement while the wait runs; returns once its part is done. */
  drive: (ctx: WaitRowContext) => Promise<void>;
  /** Children (by index) settled before the call starts. */
  settledBefore?: number[];
  /** Hides the in-process settlement signal from the wait, as for a settlement by another process. */
  noInProcessSignal?: boolean;
  abortAfterMs?: number;
  check: (output: any, ctx: WaitRowContext & { returnedAt: number; startedAt: number; storageReads: number }) => void;
};

type WaitRowContext = {
  ids: string[];
  release: (index: number) => void;
  /** Resolves once the wait's first storage read has answered. */
  firstReadDone: () => Promise<void>;
  settledAt: (output: any, index: number) => number;
};

const settledIds = (output: any) => (output.settledSubagents as any[]).map(view => view.subagentSessionId);
const runningIds = (output: any) => (output.runningSubagents as any[]).map(view => view.subagentSessionId);

const waitRows: WaitRow[] = [
  {
    name: 'until any returns when the first of two children settles',
    children: 2,
    input: () => ({ action: 'wait', until: 'any', timeoutMs: 10_000 }),
    drive: async ({ release }) => release(0),
    check: (output, { ids }) => {
      expect(output.timedOut).toBe(false);
      expect(settledIds(output)).toEqual([ids[0]]);
      expect(runningIds(output)).toEqual([ids[1]]);
    },
  },
  {
    name: 'until all (the default) returns only when both children settle',
    children: 2,
    input: () => ({ action: 'wait', timeoutMs: 10_000 }),
    drive: async ({ release }) => {
      release(0);
      await new Promise(r => setTimeout(r, 300));
      release(1);
    },
    check: (output, { ids }) => {
      expect(output.timedOut).toBe(false);
      expect(settledIds(output).sort()).toEqual([...ids].sort());
      expect(runningIds(output)).toEqual([]);
    },
  },
  {
    name: 'an in-process settlement wakes the wait at once, with few storage reads',
    children: 1,
    input: ids => ({ action: 'wait', subagentSessionId: ids[0], timeoutMs: 10_000 }),
    drive: async ({ release }) => {
      await new Promise(r => setTimeout(r, 1_600));
      release(0);
    },
    check: (output, { returnedAt, settledAt, storageReads }) => {
      expect(output.timedOut).toBe(false);
      // One read proves ownership and the wake costs one more; a 100 ms..1 s
      // backoff poll needs about six over the same 1.6 s. The latency bound is
      // wide for a loaded host; the 5 s storage poll alone would miss it.
      expect(storageReads).toBeLessThanOrEqual(3);
      expect(returnedAt - settledAt(output, 0)).toBeLessThan(500);
    },
  },
  {
    name: 'a settlement with no in-process signal (another process) is still found by the storage poll',
    children: 1,
    noInProcessSignal: true,
    input: ids => ({ action: 'wait', subagentSessionId: ids[0], timeoutMs: 10_000 }),
    // Settle only once the wait has read the child as running, so the poll,
    // not the first read, must find the settlement.
    drive: async ({ release, firstReadDone }) => {
      await firstReadDone();
      release(0);
    },
    check: (output, { returnedAt, settledAt, ids }) => {
      expect(output.timedOut).toBe(false);
      expect(settledIds(output)).toEqual([ids[0]]);
      expect(returnedAt - settledAt(output, 0)).toBeLessThan(6_000);
    },
  },
  {
    name: 'until any still times out when no child settles',
    children: 2,
    input: () => ({ action: 'wait', until: 'any', timeoutMs: 200 }),
    drive: async () => {},
    check: (output, { ids }) => {
      expect(output.timedOut).toBe(true);
      expect(settledIds(output)).toEqual([]);
      expect(runningIds(output).sort()).toEqual([...ids].sort());
    },
  },
  {
    name: 'an abort ends the wait promptly with the children still running',
    children: 1,
    abortAfterMs: 100,
    input: () => ({ action: 'wait', timeoutMs: 10_000 }),
    drive: async () => {},
    check: (output, { ids, returnedAt, startedAt }) => {
      expect(output.aborted).toBe(true);
      expect(output.timedOut).toBe(false);
      expect(runningIds(output)).toEqual([ids[0]]);
      expect(returnedAt - startedAt).toBeLessThan(2_000);
    },
  },
  {
    name: 'until any naming a child already settled at call time returns at once',
    children: 2,
    settledBefore: [0],
    input: ids => ({ action: 'wait', until: 'any', subagentSessionIds: ids, timeoutMs: 10_000 }),
    drive: async () => {},
    check: (output, { ids, returnedAt, startedAt }) => {
      expect(output.timedOut).toBe(false);
      expect(settledIds(output)).toEqual([ids[0]]);
      expect(runningIds(output)).toEqual([ids[1]]);
      expect(returnedAt - startedAt).toBeLessThan(2_000);
    },
  },
];

describe('subagent control tool — wait until any, woken by settlement', () => {
  it.each(waitRows)(
    '$name',
    async row => {
      const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB());
      const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const closeTurn = await openParentTurn(parent, parentAgent);
      const releases: Array<() => void> = [];
      const ids: string[] = [];
      for (let index = 0; index < row.children; index += 1) {
        const child = await delegateHeldChild(parent, childAgent, `wait job ${index}`);
        releases.push(child.releaseChild);
        ids.push(child.childId);
      }
      if (row.settledBefore !== undefined) {
        const statusOf = control(parent);
        for (const index of row.settledBefore) {
          releases[index]!();
          await poll(
            async () =>
              (await statusOf({ action: 'status', subagentSessionId: ids[index]! })).subagents?.[0]?.settled === true,
          );
        }
      }
      if (row.noInProcessSignal) {
        vi.spyOn(parent, '_internalOnDelegatedSettlement').mockReturnValue(() => {});
      }
      // The tool reads storage through the handle it takes at creation; count its own reads.
      let toolReads = 0;
      let answeredReads = 0;
      const storage = parent._internalStorage;
      const counting = new Proxy(storage, {
        get(target, key) {
          const value = Reflect.get(target, key, target);
          if (typeof value !== 'function') return value;
          return (...args: unknown[]) => {
            const result = value.apply(target, args);
            if (key === 'listPlanTasks' || key === 'loadSession') {
              toolReads += 1;
              void Promise.resolve(result).then(
                () => (answeredReads += 1),
                () => (answeredReads += 1),
              );
            }
            return result;
          };
        },
      });
      Object.defineProperty(parent, '_internalStorage', { value: counting, configurable: true });
      const tool = createSubagentControlTool(parent)!;
      delete (parent as any)._internalStorage;
      const controller = new AbortController();
      if (row.abortAfterMs !== undefined) setTimeout(() => controller.abort(), row.abortAfterMs);
      const ctx: WaitRowContext = {
        ids,
        release: index => releases[index]!(),
        firstReadDone: async () => {
          await poll(() => toolReads > 0 && answeredReads === toolReads);
          // Let the wait finish the read round before the settlement lands.
          await new Promise(r => setTimeout(r, 50));
        },
        settledAt: (output, index) =>
          (output.settledSubagents as any[]).find(view => view.subagentSessionId === ids[index]).settledAt,
      };
      const startedAt = Date.now();
      try {
        const pending = tool.execute!(row.input(ids) as any, {
          ...toolCtx,
          abortSignal: controller.signal,
        }) as Promise<any>;
        await row.drive(ctx);
        const output = await pending;
        const returnedAt = Date.now();
        row.check(output, { ...ctx, returnedAt, startedAt, storageReads: toolReads });
      } finally {
        vi.restoreAllMocks();
        for (const release of releases) release();
        closeTurn();
        await harness.shutdown();
      }
    },
    15_000,
  );
});

describe('delegated settlement listeners', () => {
  it('a throwing listener neither skips the other listeners nor the settlement notice', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB(), {
      ...subagentsConfig(),
      onDelegatedSettlement: 'wake',
    });
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const closeTurn = await openParentTurn(parent, parentAgent);
    const child = await delegateHeldChild(parent, childAgent, 'listener job');
    const notice = vi.spyOn(parent as any, '_startDelegationSettlementNotice');
    let later = 0;
    parent._internalOnDelegatedSettlement(() => {
      throw new Error('listener failed');
    });
    parent._internalOnDelegatedSettlement(() => {
      later += 1;
    });
    try {
      child.releaseChild();
      await poll(() => later > 0 && notice.mock.calls.length > 0, 5_000);
      expect(later).toBe(1);
      expect(notice).toHaveBeenCalledTimes(1);
    } finally {
      vi.restoreAllMocks();
      closeTurn();
      await harness.shutdown();
    }
  }, 15_000);
});

/** A task text of `chars` 3-byte Japanese characters after a short label. */
function japaneseTask(index: number, chars: number): string {
  return `job ${index} ${'調査'.repeat(chars / 2)}`;
}

type ChildListRow = {
  name: string;
  /** Children whose 40 KB results settle while the wait runs (delegated first). */
  largeSettling: number;
  /** Delegated children that stay running, each with a 500-character Japanese task. */
  running: number;
  /**
   * Inline (`spawn_subagent`) children running in this process, each with a
   * 500-character Japanese task. Plan tasks cap delegated children at 100;
   * inline children have no such cap.
   */
  inline?: number;
  call: { action: 'wait'; timeoutMs: number };
  /** Some children are left out and named in the hint. */
  leavesOut: boolean;
  /** Read every left-out child back through the calls the hint names. */
  followHint?: boolean;
  /**
   * Between pages, some inline children return the way a finished
   * spawn_subagent call does (`returnInline`): three already read and three
   * not read yet. They keep their place for the rest of the run, so paging
   * still sees every child exactly once.
   */
  settleBetweenPages?: boolean;
};

const childListRows: ChildListRow[] = [
  {
    name: '40 running children with 500-character multibyte tasks stay within 64 KiB, shortened',
    largeSettling: 0,
    running: 40,
    call: { action: 'wait', timeoutMs: 0 },
    leavesOut: false,
  },
  {
    name: '100 running children stay within 64 KiB with every child listed',
    largeSettling: 0,
    running: 100,
    call: { action: 'wait', timeoutMs: 0 },
    leavesOut: false,
  },
  {
    name: 'three large results settling beside 40 running children stay within 64 KiB',
    largeSettling: 3,
    running: 40,
    call: { action: 'wait', timeoutMs: 1_500 },
    leavesOut: false,
  },
  {
    name: '250 running children: the last are left out, counted, and the hint gives the next offset',
    largeSettling: 0,
    running: 100,
    inline: 150,
    call: { action: 'wait', timeoutMs: 0 },
    leavesOut: true,
  },
  {
    name: 'following the hint returns every left-out child, each call within 64 KiB and complete',
    largeSettling: 0,
    running: 100,
    inline: 150,
    call: { action: 'wait', timeoutMs: 0 },
    leavesOut: true,
    followHint: true,
  },
  {
    name: '1000 inline children page through the hint offsets, each child seen exactly once',
    largeSettling: 0,
    running: 0,
    inline: 1000,
    call: { action: 'wait', timeoutMs: 0 },
    leavesOut: true,
    followHint: true,
  },
  {
    name: '1000 inline children, some returning between pages: each child seen exactly once, every page within 64 KiB',
    largeSettling: 0,
    running: 0,
    inline: 1000,
    call: { action: 'wait', timeoutMs: 0 },
    leavesOut: true,
    followHint: true,
    settleBetweenPages: true,
  },
];

describe('subagent control tool — bounded child list', () => {
  it.each(childListRows)(
    '$name',
    async row => {
      const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB());
      const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const closeTurn = await openParentTurn(parent, parentAgent);
      const run = control(parent);
      const children: Array<{ childId: string; releaseChild: () => void }> = [];
      const active = (parent as any)._activeSubagents as Map<string, ActiveSubagentState>;
      const inlineKeys: string[] = [];
      const inlineSessions: Session[] = [];
      try {
        for (let index = 0; index < row.largeSettling + row.running; index += 1) {
          const large = index < row.largeSettling;
          children.push(
            await delegateHeldChild(
              parent,
              childAgent,
              `job ${index}`,
              large ? largeSummary(`child-${index}`, 40_000) : undefined,
              japaneseTask(index, 500),
            ),
          );
        }
        // An inline child is a real child session plus the parent's in-process
        // record of the running spawn_subagent call.
        for (let index = 0; index < (row.inline ?? 0); index += 1) {
          const child = await harness.session({
            resourceId: 'u1',
            threadId: { fresh: true },
            parentSessionId: parent.id,
            origin: 'subagent-tool',
            modeId: 'worker-mode',
            subagentDepth: 1,
          });
          const key = `inline:${child.id}`;
          active.set(key, {
            subagentSessionId: child.id,
            agentType: 'worker',
            task: japaneseTask(row.running + index, 500),
            parentToolCallId: `spawn-${index}`,
            startedAt: Date.now(),
            status: 'running',
          });
          inlineKeys.push(key);
          inlineSessions.push(child);
          children.push({ childId: child.id, releaseChild: () => {} });
        }
        const pending = run(row.call);
        for (let index = 0; index < row.largeSettling; index += 1) children[index]!.releaseChild();
        const output = await pending;

        expect(output.isError).toBeUndefined();
        expect(utf8Length(output)).toBeLessThanOrEqual(64 * 1024);
        expect(output.resultsShortened).toBe(true);
        const listed: any[] = [...output.settledSubagents, ...output.runningSubagents];
        const listedIds = listed.map(view => view.subagentSessionId as string);
        if (row.largeSettling === 0) {
          // The same call lists the same children in the same order.
          const again = await run(row.call);
          expect([...again.settledSubagents, ...again.runningSubagents].map(view => view.subagentSessionId)).toEqual(
            listedIds,
          );
        }
        const allIds = children.map(child => child.childId);
        for (const view of listed) {
          expect(new TextEncoder().encode(view.task).byteLength).toBeLessThanOrEqual(160);
          expect(() => encodeURIComponent(view.task)).not.toThrow();
        }
        expect(output.settledSubagents).toHaveLength(row.largeSettling);
        const leftOut = allIds.filter(id => !listedIds.includes(id));
        if (!row.leavesOut) {
          expect(leftOut).toEqual([]);
          expect(output.omittedSubagentCount).toBeUndefined();
          return;
        }
        expect(leftOut.length).toBeGreaterThan(0);
        expect(output.omittedSubagentCount).toBe(leftOut.length);
        // The hint reads on from the first left-out child.
        expect(output.resultsHint).toContain(`action "wait", timeoutMs 0 and offset ${listed.length}`);
        if (!row.followHint) return;

        // Page through the rest with the offset each hint gives, until a page leaves nothing out.
        const seen = [...listedIds];
        let page = output;
        let returned = 0;
        while (page.omittedSubagentCount !== undefined) {
          const offset = Number(/offset (\d+)/.exec(page.resultsHint)![1]);
          expect(offset).toBe(seen.length);
          if (row.settleBetweenPages) {
            const open = inlineSessions.filter(child => child.getRecord().closedAt === undefined);
            const read = open.filter(child => seen.includes(child.id)).slice(0, 3);
            const unread = open.filter(child => !seen.includes(child.id)).slice(0, 3);
            for (const child of [...read, ...unread]) await returnInline(parent, child);
            returned += read.length + unread.length;
          }
          page = await run({ action: 'wait', timeoutMs: 0, offset });
          expect(page.isError).toBeUndefined();
          expect(utf8Length(page)).toBeLessThanOrEqual(64 * 1024);
          const ids = [...page.settledSubagents, ...page.runningSubagents].map(
            (view: any) => view.subagentSessionId as string,
          );
          // Every page moves on, so following the hint cannot loop.
          expect(ids.length).toBeGreaterThan(0);
          seen.push(...ids);
        }
        // Every child is seen exactly once.
        expect(new Set(seen).size).toBe(seen.length);
        expect([...seen].sort()).toEqual([...allIds].sort());
        if (row.settleBetweenPages) expect(returned).toBeGreaterThanOrEqual(6);
      } finally {
        for (const key of inlineKeys) active.delete(key);
        for (const child of children) child.releaseChild();
        closeTurn();
        await harness.shutdown();
      }
    },
    120_000,
  );
});

describe('subagent control tool — stable offsets', () => {
  /** `count` real child sessions; `register` adds the parent's in-process spawn records for them. */
  async function addInlineChildren(harness: Harness, parent: Session, count: number): Promise<Session[]> {
    const sessions: Session[] = [];
    for (let index = 0; index < count; index += 1) {
      sessions.push(
        await harness.session({
          resourceId: 'u1',
          threadId: { fresh: true },
          parentSessionId: parent.id,
          origin: 'subagent-tool',
          modeId: 'worker-mode',
          subagentDepth: 1,
        }),
      );
    }
    return sessions;
  }

  function register(parent: Session, sessions: readonly Session[], startedAt: (index: number) => number): string[] {
    const active = (parent as any)._activeSubagents as Map<string, ActiveSubagentState>;
    return sessions.map((child, index) => {
      const key = `inline:${child.id}`;
      active.set(key, {
        subagentSessionId: child.id,
        agentType: 'worker',
        task: japaneseTask(index, 500),
        parentToolCallId: `spawn-${index}`,
        startedAt: startedAt(index),
        status: 'running',
      });
      return key;
    });
  }

  function unregister(parent: Session, keys: readonly string[]): void {
    const active = (parent as any)._activeSubagents as Map<string, ActiveSubagentState>;
    for (const key of keys) active.delete(key);
  }

  it('wait until all over more than a page: following hints from offset 0 returns every child once, with its result', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const closeTurn = await openParentTurn(parent, parentAgent);
    const run = control(parent);
    const children: Array<{ childId: string; releaseChild: () => void; summary: string }> = [];
    let keys: string[] = [];
    try {
      // 100 delegated children (the plan-task cap) with results, plus 150 inline
      // children, so even the children without their results exceed one page.
      for (let index = 0; index < 100; index += 1) {
        const summary = largeSummary(`child-${index}`, 3_000);
        children.push({
          ...(await delegateHeldChild(parent, childAgent, `job ${index}`, summary, japaneseTask(index, 500))),
          summary,
        });
      }
      const sessions = await addInlineChildren(harness, parent, 150);
      keys = register(parent, sessions, () => Date.now());
      const pending = run({ action: 'wait', timeoutMs: 20_000, offset: 0 });
      for (const child of children) child.releaseChild();
      for (const child of sessions) await child.close();
      let page = await pending;
      expect(page.timedOut).toBe(false);
      expect(page.omittedSubagentCount).toBeGreaterThan(0);
      const seen: string[] = [];
      let pages = 0;
      for (;;) {
        pages += 1;
        expect(page.isError).toBeUndefined();
        expect(utf8Length(page)).toBeLessThanOrEqual(64 * 1024);
        expect(page.runningSubagents).toEqual([]);
        for (const view of page.settledSubagents as any[]) {
          seen.push(view.subagentSessionId);
          const child = children.find(candidate => candidate.childId === view.subagentSessionId);
          // An inline child's result is returned by its spawn_subagent call only.
          if (child === undefined) continue;
          // A result left out of a page is read by naming that child, as the hint says.
          const full = view.resultOmitted
            ? (await run({ action: 'status', subagentSessionId: view.subagentSessionId })).subagents[0]
            : view;
          expect(full.result.text).toBe(child.summary);
        }
        if (page.omittedSubagentCount === undefined) break;
        const offset = Number(/offset (\d+)/.exec(page.resultsHint)![1]);
        expect(offset).toBe(seen.length);
        page = await run({ action: 'wait', timeoutMs: 0, offset });
      }
      expect(pages).toBeGreaterThan(1);
      expect(new Set(seen).size).toBe(seen.length);
      expect([...seen].sort()).toEqual(
        [...children.map(child => child.childId), ...sessions.map(child => child.id)].sort(),
      );
    } finally {
      unregister(parent, keys);
      for (const child of children) child.releaseChild();
      closeTurn();
      await harness.shutdown();
    }
  }, 60_000);

  it('delegated children settling between pages of a multi-page wait: each child seen once, each page within 64 KiB', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const closeTurn = await openParentTurn(parent, parentAgent);
    const run = control(parent);
    const children: Array<{ childId: string; releaseChild: () => void; summary: string }> = [];
    const keys: string[] = [];
    try {
      // Each delegated child (3 KB result) is followed by two inline children,
      // so the delegated ones are spread over every page of the list.
      const sessions = await addInlineChildren(harness, parent, 200);
      for (let index = 0; index < 100; index += 1) {
        const summary = largeSummary(`child-${index}`, 3_000);
        children.push({
          ...(await delegateHeldChild(parent, childAgent, `job ${index}`, summary, japaneseTask(index, 500))),
          summary,
        });
        keys.push(...register(parent, sessions.slice(index * 2, index * 2 + 2), () => Date.now()));
      }
      const released = new Set<string>();
      const settle = async (child: { childId: string; releaseChild: () => void }) => {
        child.releaseChild();
        released.add(child.childId);
        await poll(
          async () =>
            (await run({ action: 'status', subagentSessionId: child.childId })).subagents?.[0]?.settled === true,
          10_000,
        );
      };
      const allIds = [...children.map(child => child.childId), ...sessions.map(child => child.id)];
      const seen: string[] = [];
      let page = await run({ action: 'wait', timeoutMs: 0, offset: 0 });
      let pages = 0;
      for (;;) {
        pages += 1;
        expect(page.isError).toBeUndefined();
        expect(utf8Length(page)).toBeLessThanOrEqual(64 * 1024);
        for (const view of page.settledSubagents as any[]) {
          // A settled delegated child carries its result, or says it was left out.
          expect(view.result !== undefined || view.resultOmitted === true).toBe(true);
          expect(released.has(view.subagentSessionId)).toBe(true);
        }
        const ids = [...page.settledSubagents, ...page.runningSubagents].map(
          (view: any) => view.subagentSessionId as string,
        );
        expect(ids.length).toBeGreaterThan(0);
        seen.push(...ids);
        if (page.omittedSubagentCount === undefined) break;
        const offset = Number(/offset (\d+)/.exec(page.resultsHint)![1]);
        expect(offset).toBe(seen.length);
        // Settle three delegated children already read and three not read yet.
        const open = children.filter(child => !released.has(child.childId));
        for (const child of [
          ...open.filter(child => seen.includes(child.childId)).slice(0, 3),
          ...open.filter(child => !seen.includes(child.childId)).slice(0, 3),
        ]) {
          await settle(child);
        }
        page = await run({ action: 'wait', timeoutMs: 0, offset });
      }
      expect(pages).toBeGreaterThan(1);
      expect(released.size).toBeGreaterThanOrEqual(6);
      expect(new Set(seen).size).toBe(seen.length);
      expect([...seen].sort()).toEqual([...allIds].sort());
    } finally {
      unregister(parent, keys);
      for (const child of children) child.releaseChild();
      closeTurn();
      await harness.shutdown();
    }
  }, 60_000);

  it('status over 250 children pages from offset 0 through the hints, each child once, each page within 64 KiB', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const closeTurn = await openParentTurn(parent, parentAgent);
    const run = control(parent);
    const children: Array<{ childId: string; releaseChild: () => void }> = [];
    let keys: string[] = [];
    try {
      for (let index = 0; index < 100; index += 1) {
        children.push(await delegateHeldChild(parent, childAgent, `job ${index}`, undefined, japaneseTask(index, 500)));
      }
      const sessions = await addInlineChildren(harness, parent, 150);
      keys = register(parent, sessions, () => Date.now());
      const allIds = [...children.map(child => child.childId), ...sessions.map(child => child.id)];
      const seen: string[] = [];
      let page = await run({ action: 'status', offset: 0 });
      for (;;) {
        expect(page.isError).toBeUndefined();
        expect(utf8Length(page)).toBeLessThanOrEqual(64 * 1024);
        const ids = (page.subagents as any[]).map(view => view.subagentSessionId as string);
        expect(ids.length).toBeGreaterThan(0);
        seen.push(...ids);
        if (page.omittedSubagentCount === undefined) break;
        expect(page.omittedSubagentCount).toBe(allIds.length - seen.length);
        const offset = Number(/action "status" and offset (\d+)/.exec(page.resultsHint)![1]);
        expect(offset).toBe(seen.length);
        page = await run({ action: 'status', offset });
      }
      expect(new Set(seen).size).toBe(seen.length);
      expect([...seen].sort()).toEqual([...allIds].sort());
    } finally {
      unregister(parent, keys);
      for (const child of children) child.releaseChild();
      closeTurn();
      await harness.shutdown();
    }
  }, 60_000);

  it('wait until any with an offset waits for a new settlement, and still returns the children already settled', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const closeTurn = await openParentTurn(parent, parentAgent);
    const run = control(parent);
    const children: Array<{ childId: string; releaseChild: () => void }> = [];
    try {
      for (let index = 0; index < 4; index += 1) {
        children.push(await delegateHeldChild(parent, childAgent, `job ${index}`));
      }
      // Children 0 and 1 settle before the wait starts.
      for (const child of children.slice(0, 2)) {
        child.releaseChild();
        await poll(
          async () =>
            (await run({ action: 'status', subagentSessionId: child.childId })).subagents?.[0]?.settled === true,
          10_000,
        );
      }
      const startedAt = Date.now();
      const pending = run({ action: 'wait', until: 'any', offset: 0, timeoutMs: 10_000 });
      await new Promise(resolve => setTimeout(resolve, 300));
      children[2]!.releaseChild();
      const output = await pending;
      // The wait ended on child 2's settlement, not on the children already settled.
      expect(Date.now() - startedAt).toBeGreaterThanOrEqual(290);
      expect(output.timedOut).toBe(false);
      expect(settledIds(output).sort()).toEqual(
        children
          .slice(0, 3)
          .map(child => child.childId)
          .sort(),
      );
      expect(runningIds(output)).toEqual([children[3]!.childId]);
      for (const view of output.settledSubagents as any[]) expect(view.result.text).toMatch(/^job \d: done$/);

      // With nothing new settling, it blocks until the timeout.
      const blockedAt = Date.now();
      const blocked = await run({ action: 'wait', until: 'any', offset: 0, timeoutMs: 400 });
      expect(Date.now() - blockedAt).toBeGreaterThanOrEqual(380);
      expect(blocked.timedOut).toBe(true);
      expect(runningIds(blocked)).toEqual([children[3]!.childId]);
    } finally {
      for (const child of children) child.releaseChild();
      closeTurn();
      await harness.shutdown();
    }
  }, 30_000);

  it('wait until any woken by a child far past the first page returns that child, within 64 KiB', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const closeTurn = await openParentTurn(parent, parentAgent);
    const run = control(parent);
    let waker: { childId: string; releaseChild: () => void } | undefined;
    let keys: string[] = [];
    try {
      // 500 inline children started before the delegated child and 500 after
      // it, so it sits in the middle of the list, far past the first page.
      const sessions = await addInlineChildren(harness, parent, 1000);
      const now = Date.now();
      keys = register(parent, sessions, index => (index < 500 ? now - 60_000 : now + 60_000));
      waker = await delegateHeldChild(parent, childAgent, 'job waker', undefined, japaneseTask(0, 500));
      const pending = run({ action: 'wait', until: 'any', timeoutMs: 20_000 });
      await new Promise(resolve => setTimeout(resolve, 200));
      waker.releaseChild();
      const output = await pending;
      expect(output.isError).toBeUndefined();
      expect(output.timedOut).toBe(false);
      expect(utf8Length(output)).toBeLessThanOrEqual(64 * 1024);
      // The page is cut short, yet the child that woke the wait is in it, with its result.
      expect(output.omittedSubagentCount).toBeGreaterThan(0);
      const woke = (output.settledSubagents as any[]).find(view => view.subagentSessionId === waker!.childId);
      expect(woke).toBeDefined();
      expect(woke.result.text).toBe('job waker: done');
    } finally {
      unregister(parent, keys);
      waker?.releaseChild();
      closeTurn();
      await harness.shutdown();
    }
  }, 60_000);

  /** Page an implicit list from offset 0 through the hints, running `between` before each next page. */
  async function pageAll(
    run: (input: Record<string, unknown>) => Promise<any>,
    action: 'status' | 'wait',
    between: (seen: readonly string[]) => Promise<void>,
  ): Promise<string[]> {
    const call = (offset: number) => run(action === 'wait' ? { action, timeoutMs: 0, offset } : { action, offset });
    const seen: string[] = [];
    let page = await call(0);
    for (;;) {
      expect(page.isError).toBeUndefined();
      expect(utf8Length(page)).toBeLessThanOrEqual(64 * 1024);
      const views: any[] = action === 'wait' ? [...page.settledSubagents, ...page.runningSubagents] : page.subagents;
      expect(views.length).toBeGreaterThan(0);
      seen.push(...views.map(view => view.subagentSessionId as string));
      if (page.omittedSubagentCount === undefined) return seen;
      const offset = Number(/offset (\d+)/.exec(page.resultsHint)![1]);
      expect(offset).toBe(seen.length);
      await between(seen);
      page = await call(offset);
    }
  }

  /** Return three open inline children already read and three not read yet. */
  async function returnSome(parent: Session, sessions: readonly Session[], seen: readonly string[]): Promise<number> {
    const open = sessions.filter(child => child.getRecord().closedAt === undefined);
    const picked = [
      ...open.filter(child => seen.includes(child.id)).slice(0, 3),
      ...open.filter(child => !seen.includes(child.id)).slice(0, 3),
    ];
    for (const child of picked) await returnInline(parent, child);
    return picked.length;
  }

  it('250 inline children, some returning between pages: wait and status each see every child exactly once', async () => {
    const { harness, parentAgent } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const closeTurn = await openParentTurn(parent, parentAgent);
    const run = control(parent);
    let keys: string[] = [];
    try {
      const sessions = await addInlineChildren(harness, parent, 250);
      keys = register(parent, sessions, () => Date.now());
      const allIds = sessions.map(child => child.id).sort();
      let returned = 0;
      for (const action of ['wait', 'status'] as const) {
        const seen = await pageAll(run, action, async read => {
          returned += await returnSome(parent, sessions, read);
        });
        expect(new Set(seen).size).toBe(seen.length);
        expect([...seen].sort()).toEqual(allIds);
      }
      expect(returned).toBeGreaterThan(6);
    } finally {
      unregister(parent, keys);
      closeTurn();
      await harness.shutdown();
    }
  }, 60_000);

  it('30 inline children started before 40 delegated ones, inline returning between pages: each child seen exactly once', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const closeTurn = await openParentTurn(parent, parentAgent);
    const run = control(parent);
    const children: Array<{ childId: string; releaseChild: () => void }> = [];
    let keys: string[] = [];
    try {
      const sessions = await addInlineChildren(harness, parent, 30);
      const inlineStart = Date.now() - 60_000;
      keys = register(parent, sessions, () => inlineStart);
      for (let index = 0; index < 40; index += 1) {
        children.push(await delegateHeldChild(parent, childAgent, `job ${index}`));
      }
      const allIds = [...sessions.map(child => child.id), ...children.map(child => child.childId)].sort();
      let returned = 0;
      const seen = await pageAll(run, 'status', async read => {
        returned += await returnSome(parent, sessions, read);
      });
      expect(returned).toBeGreaterThanOrEqual(6);
      expect(new Set(seen).size).toBe(seen.length);
      expect([...seen].sort()).toEqual(allIds);
    } finally {
      unregister(parent, keys);
      for (const child of children) child.releaseChild();
      closeTurn();
      await harness.shutdown();
    }
  }, 60_000);

  it('the record of returned inline children belongs to one run: a suspend and resume keeps it, a new run clears it, and it does not grow across runs', async () => {
    const { harness, parentAgent } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const recordSize = () => ((parent as any)._returnedInlineSubagents as Map<string, unknown>).size;
    const currentRunId = () => (parent as any)._currentRunId as string | undefined;
    const releases: Array<() => void> = [];
    const hold = () => new Promise<void>(r => releases.push(r));
    const pageIds = async (runId: string) => {
      const run = control(parent, runId);
      const seen: string[] = [];
      let page = await run({ action: 'status', offset: 0 });
      for (;;) {
        seen.push(...(page.subagents as any[]).map(view => view.subagentSessionId as string));
        if (page.omittedSubagentCount === undefined) return seen;
        page = await run({ action: 'status', offset: Number(/offset (\d+)/.exec(page.resultsHint)![1]) });
      }
    };
    try {
      // Run 1: inline children return, the run suspends on an approval, then resumes as the same run.
      parentAgent.enqueueRun({
        holdUntil: hold(),
        finishReason: 'suspended',
        suspendPayload: { toolCallId: 'tc-approve', toolName: 'shell' },
      });
      const first = parent.message({ content: 'run 1' });
      await poll(() => currentRunId() !== undefined);
      const firstRunId = currentRunId()!;
      const firstChildren = await addInlineChildren(harness, parent, 5);
      register(parent, firstChildren, () => Date.now());
      for (const child of firstChildren) await returnInline(parent, child, firstRunId);
      releases.shift()!();
      const suspended = (await first) as Record<string, unknown>;
      expect(suspended.runId).toBe(firstRunId);
      expect(recordSize()).toBe(5);
      parentAgent.enqueueRun({ holdUntil: hold(), finishReason: 'stop', text: 'resumed', runId: firstRunId });
      const resumeCallsBefore = parentAgent.resumeCalls.length;
      const resumed = parent.respondToToolApproval({ approved: true });
      void resumed.catch(() => {});
      await poll(() => parentAgent.resumeCalls.length > resumeCallsBefore);
      // The resume continues run 1, so its returned children are still listed, each once.
      const afterResume = await pageIds(firstRunId);
      expect(new Set(afterResume).size).toBe(afterResume.length);
      expect([...afterResume].sort()).toEqual(firstChildren.map(child => child.id).sort());
      expect(recordSize()).toBe(5);
      releases.shift()!();
      await resumed.catch(() => {});

      // Runs 2 to 4: each new run starts from an empty record and holds only its own children.
      let previousRunId = firstRunId;
      for (let index = 2; index <= 4; index += 1) {
        parentAgent.enqueueRun({ holdUntil: hold(), finishReason: 'stop', text: 'done' });
        const callsBefore = parentAgent.streamCalls.length;
        const pending = parent.message({ content: `run ${index}` });
        await poll(() => parentAgent.streamCalls.length > callsBefore && currentRunId() !== undefined);
        const runId = currentRunId()!;
        expect(runId).not.toBe(previousRunId);
        expect(await pageIds(runId)).toEqual([]);
        expect(recordSize()).toBe(0);
        const children = await addInlineChildren(harness, parent, 5);
        register(parent, children, () => Date.now());
        for (const child of children) await returnInline(parent, child, runId);
        expect(recordSize()).toBe(5);
        expect([...(await pageIds(runId))].sort()).toEqual(children.map(child => child.id).sort());
        releases.shift()!();
        await pending.catch(() => {});
        expect(recordSize()).toBe(5);
        previousRunId = runId;
      }
    } finally {
      for (const release of releases) release();
      await harness.shutdown();
    }
  }, 30_000);

  it('a new run that starts while the previous turn is still finishing holds only its own returned children', async () => {
    const { harness, parentAgent } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const recordSize = () => ((parent as any)._returnedInlineSubagents as Map<string, unknown>).size;
    const currentRunId = () => (parent as any)._currentRunId as string | undefined;
    const original = (parent as any)._endTurn as (controller: AbortController) => void;
    let finishFirstTurn: (() => void) | undefined;
    let releaseFirst!: () => void;
    let releaseSecond!: () => void;
    try {
      parentAgent.enqueueRun({
        holdUntil: new Promise<void>(r => (releaseFirst = r)),
        finishReason: 'stop',
        text: 'done',
      });
      const first = parent.message({ content: 'run 1' });
      await poll(() => currentRunId() !== undefined);
      const firstRunId = currentRunId()!;
      const firstChildren = await addInlineChildren(harness, parent, 4);
      register(parent, firstChildren, () => Date.now());
      for (const child of firstChildren) await returnInline(parent, child, firstRunId);
      // Run 1's turn stays open after its run ends, as its post-run bookkeeping would keep it.
      (parent as any)._endTurn = function (this: unknown, controller: AbortController) {
        if (finishFirstTurn === undefined) {
          finishFirstTurn = () => original.call(this, controller);
          return;
        }
        original.call(this, controller);
      };
      releaseFirst();
      await first;
      expect(finishFirstTurn).toBeDefined();

      parentAgent.enqueueRun({
        holdUntil: new Promise<void>(r => (releaseSecond = r)),
        finishReason: 'stop',
        text: 'done',
      });
      const callsBefore = parentAgent.streamCalls.length;
      const second = parent.message({ content: 'run 2' });
      void second.catch(() => {});
      await poll(() => parentAgent.streamCalls.length > callsBefore && currentRunId() !== undefined);
      const secondRunId = currentRunId()!;
      expect(secondRunId).not.toBe(firstRunId);
      const secondChildren = await addInlineChildren(harness, parent, 3);
      register(parent, secondChildren, () => Date.now());
      for (const child of secondChildren) await returnInline(parent, child, secondRunId);
      expect(recordSize()).toBe(3);
      const listed = (await control(parent, secondRunId)({ action: 'status' })).subagents.map(
        (view: any) => view.subagentSessionId,
      );
      expect([...listed].sort()).toEqual(secondChildren.map(child => child.id).sort());
    } finally {
      releaseFirst?.();
      releaseSecond?.();
      finishFirstTurn?.();
      await harness.shutdown();
    }
  }, 30_000);

  it('a message that joins the active run keeps the returned inline children listed, each once', async () => {
    const { harness, parentAgent } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const closeTurn = await openParentTurn(parent, parentAgent);
    const run = control(parent);
    let keys: string[] = [];
    try {
      const returned = await addInlineChildren(harness, parent, 5);
      register(parent, returned, () => Date.now());
      for (const child of returned) await returnInline(parent, child);
      const running = await addInlineChildren(harness, parent, 3);
      keys = register(parent, running, () => Date.now());
      const callsBefore = parentAgent.streamCalls.length;
      const sendSignal = vi.spyOn(parentAgent, 'sendSignal');
      // A second message while the run is active joins that run instead of starting a new one.
      const joined = parent.message({ content: 'one more thing' });
      void joined.catch(() => {});
      const joinedCall = () =>
        sendSignal.mock.calls.findIndex(([signal]) => (signal as { contents?: unknown }).contents === 'one more thing');
      await poll(() => joinedCall() >= 0);
      const signalResult = sendSignal.mock.results[joinedCall()]!.value as { accepted: Promise<{ action: string }> };
      await expect(signalResult.accepted).resolves.toMatchObject({ action: 'deliver' });
      expect(parentAgent.streamCalls.length).toBe(callsBefore);
      const seen: string[] = [];
      let page = await run({ action: 'status', offset: 0 });
      for (;;) {
        seen.push(...(page.subagents as any[]).map(view => view.subagentSessionId as string));
        if (page.omittedSubagentCount === undefined) break;
        page = await run({ action: 'status', offset: Number(/offset (\d+)/.exec(page.resultsHint)![1]) });
      }
      const allIds = [...returned, ...running].map(child => child.id);
      expect(new Set(seen).size).toBe(seen.length);
      expect([...seen].sort()).toEqual([...allIds].sort());
    } finally {
      unregister(parent, keys);
      closeTurn();
      await harness.shutdown();
    }
  }, 30_000);

  it('children that start at the same time are ordered by id, whatever order they were registered in', async () => {
    const { harness, parentAgent } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const closeTurn = await openParentTurn(parent, parentAgent);
    const run = control(parent);
    let keys: string[] = [];
    try {
      const sessions = await addInlineChildren(harness, parent, 6);
      const sortedIds = sessions.map(child => child.id).sort();
      // Register in reverse id order, all with one start time.
      const reversed = [...sessions].sort((left, right) => (left.id < right.id ? 1 : -1));
      const startedAt = Date.now();
      keys = register(parent, reversed, () => startedAt);
      const listed = await run({ action: 'status' });
      expect((listed.subagents as any[]).map(view => view.subagentSessionId)).toEqual(sortedIds);
      const waited = await run({ action: 'wait', timeoutMs: 0 });
      expect((waited.runningSubagents as any[]).map(view => view.subagentSessionId)).toEqual(sortedIds);
      const fromTwo = await run({ action: 'status', offset: 2 });
      expect((fromTwo.subagents as any[]).map(view => view.subagentSessionId)).toEqual(sortedIds.slice(2));
    } finally {
      unregister(parent, keys);
      closeTurn();
      await harness.shutdown();
    }
  }, 30_000);
});

describe('subagent control tool — permission gate (real agent loop)', () => {
  it('a host deny removes the tool before the model sees it, so it never runs', async () => {
    const testUsage = { inputTokens: 1, outputTokens: 1, totalTokens: 2 };
    let call = 0;
    const model = new MockLanguageModelV2({
      doStream: async () => {
        call++;
        const parts =
          call === 1
            ? [
                {
                  type: 'tool-call',
                  toolCallId: 'call-1',
                  toolName: SUBAGENT_CONTROL_TOOL_ID,
                  input: '{"action":"status"}',
                },
                { type: 'finish', finishReason: 'tool-calls', usage: testUsage },
              ]
            : [
                { type: 'text-start', id: 't1' },
                { type: 'text-delta', id: 't1', delta: 'done' },
                { type: 'text-end', id: 't1' },
                { type: 'finish', finishReason: 'stop', usage: testUsage },
              ];
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: convertArrayToReadableStream([{ type: 'stream-start', warnings: [] }, ...parts] as any),
        };
      },
    });
    const parentAgent = new Agent({ id: 'parent-agent', name: 'parent-agent', instructions: 'control', model });
    const harness = new Harness({
      agents: { 'parent-agent': parentAgent, 'child-agent': new MockAgent({ id: 'child-agent' }) } as any,
      modes: [
        {
          id: 'default',
          agentId: 'parent-agent',
          permissions: { categories: {}, tools: { [SUBAGENT_CONTROL_TOOL_ID]: 'deny' } },
        },
        { id: 'worker-mode', agentId: 'child-agent' },
      ],
      defaultModeId: 'default',
      sessions: { storage: new InMemoryHarness({ db: new InMemoryDB() }) },
      subagents: subagentsConfig(),
    });
    try {
      const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(event => events.push(event));

      const result = (await session.message({ content: 'check my subagents' })) as any;

      expect(result.text).toContain('done');
      expect(
        events.some(
          event =>
            event.type === 'tool_denied' &&
            (event as any).stage === 'pre-exposure' &&
            (event as any).toolName === SUBAGENT_CONTROL_TOOL_ID,
        ),
      ).toBe(true);
      // The loop may still report the model's call, but the control tool never
      // ran: no tool result carries its status payload.
      const toolOutputs = events.filter(event => event.type === 'tool_end').map(event => (event as any).output);
      expect(toolOutputs.some(output => (output as any)?.action === 'status')).toBe(false);
    } finally {
      await harness.shutdown();
    }
  });
});

/**
 * PF-7632 — a detached `task_delegate` child outlives its parent turn. The
 * parent must still accept a new user turn (which can stop or steer that
 * child), so `isTurnActive()` excludes in-flight subagent executions while
 * `isBusy()` keeps counting them for shutdown and `waitForIdle()`.
 */
describe('isTurnActive — user turns while a delegated child runs (real agent loop)', () => {
  type StreamPart = Record<string, unknown>;
  const usage = { inputTokens: 1, outputTokens: 1, totalTokens: 2 };
  const text = (value: string): StreamPart[] => [
    { type: 'text-start', id: 't1' },
    { type: 'text-delta', id: 't1', delta: value },
    { type: 'text-end', id: 't1' },
    { type: 'finish', finishReason: 'stop', usage },
  ];
  const toolCall = (toolCallId: string, toolName: string, input: unknown): StreamPart => ({
    type: 'tool-call',
    toolCallId,
    toolName,
    input: JSON.stringify(input),
  });

  /** A parent backed by a real Agent loop; `script` decides each model step. */
  function buildRealLoopHarness(script: () => StreamPart[] | Promise<StreamPart[]>) {
    const model = new MockLanguageModelV2({
      doStream: async () => ({
        rawCall: { rawPrompt: null, rawSettings: {} },
        warnings: [],
        stream: convertArrayToReadableStream([{ type: 'stream-start', warnings: [] }, ...(await script())] as any),
      }),
    });
    const parentAgent = new Agent({ id: 'parent-agent', name: 'parent-agent', instructions: 'coordinate', model });
    const childAgent = new MockAgent({
      id: 'child-agent',
      defaultOutput: { terminalToolResult: completedOutcome('Worker finished') },
    });
    const harness = new Harness({
      agents: { 'parent-agent': parentAgent, 'child-agent': childAgent } as any,
      modes: [
        { id: 'default', agentId: 'parent-agent' },
        { id: 'worker-mode', agentId: 'child-agent' },
      ],
      defaultModeId: 'default',
      sessions: { storage: new InMemoryHarness({ db: new InMemoryDB() }) },
      subagents: subagentsConfig(),
    });
    return { harness, childAgent };
  }

  function holdChildRun(childAgent: MockAgent, summary: string): () => void {
    let release!: () => void;
    childAgent.enqueueRun({
      holdUntil: new Promise<void>(r => (release = r)),
      finishReason: 'stop',
      terminalToolResult: completedOutcome(summary),
    });
    return release;
  }

  it('a new user turn is admitted while two delegated children run, and cancels one and steers the other', async () => {
    let phase: 'delegate' | 'steer' | 'report' | 'idle' = 'delegate';
    let openDelegationTurn!: () => void;
    const delegationTurnGate = new Promise<void>(r => (openDelegationTurn = r));
    const children: { a?: string; b?: string } = {};
    let turnActiveDuringSteerTurn: boolean | undefined;
    let parent!: Session;
    const releases: Array<() => void> = [];
    const { harness, childAgent } = buildRealLoopHarness(async () => {
      switch (phase) {
        case 'delegate':
          // Turn 1 stays open while the test delegates through the turn's tools.
          phase = 'idle';
          await delegationTurnGate;
          return text('DELEGATED');
        case 'steer':
          phase = 'report';
          return [
            toolCall('call-cancel', SUBAGENT_CONTROL_TOOL_ID, { action: 'cancel', subagentSessionId: children.a }),
            toolCall('call-send', SUBAGENT_CONTROL_TOOL_ID, {
              action: 'send',
              subagentSessionId: children.b,
              message: 'Only include trials after 2020.',
            }),
            { type: 'finish', finishReason: 'tool-calls', usage },
          ];
        case 'report':
          phase = 'idle';
          turnActiveDuringSteerTurn = parent.isTurnActive();
          return text('Stopped the first task; redirected the second.');
        default:
          return text('ok');
      }
    });
    try {
      parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      parent.subscribe(event => events.push(event));
      const sendSignal = vi.spyOn(childAgent, 'sendSignal');
      const releaseA = holdChildRun(childAgent, 'first survey: done');
      const releaseB = holdChildRun(childAgent, 'second survey: done');
      releases.push(openDelegationTurn, releaseA, releaseB);

      const delegationTurn = parent.message({ content: 'run two surveys in the background' });
      await poll(() => parent.isRunning());
      const tools = createPlanTaskTools(parent);
      for (const [key, content] of [
        ['a', 'first survey'],
        ['b', 'second survey'],
      ] as const) {
        const task = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content } as any, toolCtx)) as any;
        const delegated = (await tools[TASK_DELEGATE_TOOL_ID]!.execute!(
          { taskId: task.taskId, agentType: 'worker' } as any,
          toolCtx,
        )) as any;
        children[key] = delegated.subagentSessionId;
      }
      await poll(() =>
        [children.a!, children.b!].every(id => harness._internalGetLiveSession(id)?.isRunning() === true),
      );
      openDelegationTurn();
      await delegationTurn;
      await poll(() => !parent.isRunning());

      // The parent turn ended; both detached children still run.
      expect(parent._internalSubagentExecutionsInFlight).toBe(2);
      expect(parent.isBusy()).toBe(true);
      expect(parent.isTurnActive()).toBe(false);

      phase = 'steer';
      const steerTurn = (await parent.message({ content: 'stop the first survey' })) as any;

      expect(steerTurn.text).toContain('Stopped the first task');
      expect(turnActiveDuringSteerTurn).toBe(true);
      const controlOutputs = events
        .filter(event => event.type === 'tool_end' && (event as any).toolName === SUBAGENT_CONTROL_TOOL_ID)
        .map(event => (event as any).output);
      expect(controlOutputs).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ action: 'cancel', subagentSessionId: children.a, cancel: 'acknowledged' }),
          expect.objectContaining({ action: 'send', subagentSessionId: children.b, delivered: true }),
        ]),
      );
      expect(sendSignal).toHaveBeenCalledWith(
        expect.objectContaining({ type: 'system-reminder', contents: 'Only include trials after 2020.' }),
        expect.anything(),
      );

      releaseA();
      releaseB();
      const run = control(parent);
      const settledA = await run({ action: 'wait', subagentSessionId: children.a, timeoutMs: 5_000 });
      const settledB = await run({ action: 'wait', subagentSessionId: children.b, timeoutMs: 5_000 });
      expect(settledA.settledSubagents).toEqual([
        expect.objectContaining({ subagentSessionId: children.a, status: 'cancelled', cancelRequested: true }),
      ]);
      expect(settledB.settledSubagents).toEqual([
        expect.objectContaining({ subagentSessionId: children.b, status: 'completed' }),
      ]);
      await parent.waitForIdle({ timeoutMs: 5_000 });
      expect(parent.isBusy()).toBe(false);
      expect(parent.isTurnActive()).toBe(false);
    } finally {
      // Let held runs finish so a failed assertion surfaces instead of a drain timeout.
      for (const release of releases) release();
      await harness.shutdown();
    }
  });

  it('an inline spawn_subagent reads as an active turn while its child runs', async () => {
    let step = 0;
    let observed: { turnActive: boolean; busy: boolean; inFlight: number } | undefined;
    let parent!: Session;
    let releaseChild = () => {};
    const { harness, childAgent } = buildRealLoopHarness(() => {
      step++;
      return step === 1
        ? [
            toolCall('call-spawn', SPAWN_SUBAGENT_TOOL_ID, { agentType: 'worker', task: 'check one citation' }),
            { type: 'finish', finishReason: 'tool-calls', usage },
          ]
        : text('checked');
    });
    try {
      parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      releaseChild = holdChildRun(childAgent, 'citation checked');

      const turn = parent.message({ content: 'check the citation' });
      await poll(() => childAgent.streamCalls.length === 1);
      observed = {
        turnActive: parent.isTurnActive(),
        busy: parent.isBusy(),
        inFlight: parent._internalSubagentExecutionsInFlight,
      };
      releaseChild();
      await turn;

      expect(observed).toEqual({ turnActive: true, busy: true, inFlight: 1 });
      expect(parent.isTurnActive()).toBe(false);
      expect(parent.isBusy()).toBe(false);
    } finally {
      releaseChild();
      await harness.shutdown();
    }
  });
});
