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
import type { Session } from './session';
import { SPAWN_SUBAGENT_TOOL_ID } from './spawn-subagent-tool';
import {
  createSubagentControlTool,
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

function buildHarness(db: InMemoryDB) {
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
    subagents: subagentsConfig(),
  });
  return { harness, parentAgent, childAgent };
}

const toolCtx = {
  abortSignal: new AbortController().signal,
  agent: { toolCallId: 'tc-control', runId: 'mock-run' },
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
    { taskId: task.taskId, agentType: 'worker' } as any,
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

function control(parent: Session) {
  const tool = createSubagentControlTool(parent);
  if (!tool) throw new Error('subagent control tool not registered');
  return (input: Record<string, unknown>) => tool.execute!(input as any, toolCtx) as Promise<any>;
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

  it('cancel acknowledges a live child, then the delegation settles failed; a second cancel is a no-op', async () => {
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
      expect.objectContaining({ subagentSessionId: childId, status: 'failed', cancelRequested: true }),
    ]);

    const again = await run({ action: 'cancel', subagentSessionId: childId });
    expect(again).toMatchObject({ cancel: 'already_settled', settled: true, status: 'failed' });

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

function utf8Length(value: unknown): number {
  return new TextEncoder().encode(JSON.stringify(value)).byteLength;
}

type ResultPageRow = {
  name: string;
  /** Summary bytes per delegated child. */
  sizes: number[];
  /** Children (by index) settled before the call. */
  settledBefore: number[];
  /** Children (by index) released while the call runs. */
  settledDuring: number[];
  call: (ids: string[]) => Record<string, unknown>;
  /** Expected result per child index in the output: full, preview, or omitted. */
  expected: Record<number, 'full' | 'preview' | 'omitted'>;
  shortened: boolean;
};

const resultPageRows: ResultPageRow[] = [
  {
    name: 'status listing: a child settled before the call gets a preview',
    sizes: [20_000],
    settledBefore: [0],
    settledDuring: [],
    call: () => ({ action: 'status' }),
    expected: { 0: 'preview' },
    shortened: true,
  },
  {
    name: 'status naming a child settled before the call gets its full result',
    sizes: [20_000],
    settledBefore: [0],
    settledDuring: [],
    call: ids => ({ action: 'status', subagentSessionId: ids[0] }),
    expected: { 0: 'full' },
    shortened: false,
  },
  {
    name: 'status listing: a small result is complete as its preview and is not marked shortened',
    sizes: [200],
    settledBefore: [0],
    settledDuring: [],
    call: () => ({ action: 'status' }),
    expected: { 0: 'full' },
    shortened: false,
  },
  {
    name: 'status listing three large children stays under the budget with previews',
    sizes: [40_000, 40_000, 40_000],
    settledBefore: [0, 1, 2],
    settledDuring: [],
    call: () => ({ action: 'status' }),
    expected: { 0: 'preview', 1: 'preview', 2: 'preview' },
    shortened: true,
  },
  {
    name: 'wait for every running child: a child that settles during the call gets its full result',
    sizes: [20_000],
    settledBefore: [],
    settledDuring: [0],
    call: () => ({ action: 'wait', timeoutMs: 5_000 }),
    expected: { 0: 'full' },
    shortened: false,
  },
  {
    name: 'wait naming a child settled before the call gets its full result',
    sizes: [20_000],
    settledBefore: [0],
    settledDuring: [],
    call: ids => ({ action: 'wait', subagentSessionIds: ids, timeoutMs: 1_000 }),
    expected: { 0: 'full' },
    shortened: false,
  },
  {
    name: 'wait naming three large children over the budget: the first is full, the rest are previews',
    sizes: [40_000, 40_000, 40_000],
    settledBefore: [0, 1, 2],
    settledDuring: [],
    call: ids => ({ action: 'wait', subagentSessionIds: ids, timeoutMs: 1_000 }),
    expected: { 0: 'full', 1: 'preview', 2: 'preview' },
    shortened: true,
  },
];

describe('subagent control tool — bounded result page', () => {
  it.each(resultPageRows)('$name', async row => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const closeTurn = await openParentTurn(parent, parentAgent);
    const run = control(parent);
    const children: Array<{ childId: string; releaseChild: () => void; summary: string }> = [];
    for (const [index, size] of row.sizes.entries()) {
      const summary = largeSummary(`child-${index}`, size);
      const child = await delegateHeldChild(parent, childAgent, `job ${index}`, summary);
      children.push({ ...child, summary });
    }
    const settled = async (index: number) =>
      (await run({ action: 'status', subagentSessionId: children[index]!.childId })).subagents?.[0]?.settled === true;
    for (const index of row.settledBefore) {
      children[index]!.releaseChild();
      await poll(() => settled(index));
    }

    const pending = run(row.call(children.map(child => child.childId)));
    for (const index of row.settledDuring) children[index]!.releaseChild();
    const output = await pending;

    expect(output.isError).toBeUndefined();
    expect(utf8Length(output)).toBeLessThanOrEqual(64 * 1024);
    const views: any[] = output.subagents ?? output.settledSubagents;
    for (const [index, expected] of Object.entries(row.expected)) {
      const child = children[Number(index)]!;
      const view = views.find(candidate => candidate.subagentSessionId === child.childId);
      expect(view, `child ${index}`).toBeDefined();
      if (expected === 'full') {
        expect(view.result.text).toBe(child.summary);
        expect(view.result.textTruncated).toBe(false);
        expect(view.resultPreview).toBeUndefined();
      } else if (expected === 'preview') {
        expect(view.resultPreview).toBe(true);
        expect(view.result.textTruncated).toBe(true);
        expect(child.summary.startsWith(view.result.text.split('\n')[0])).toBe(true);
        expect(new TextEncoder().encode(view.result.text).byteLength).toBeLessThan(5 * 1024);
      } else {
        expect(view.result).toBeUndefined();
        expect(view.resultOmitted).toBe(true);
      }
    }
    if (row.shortened) {
      expect(output.resultsShortened).toBe(true);
      expect(output.resultsHint).toMatch(/action "status" and that subagentSessionId/);
    } else {
      expect(output.resultsShortened).toBeUndefined();
      expect(output.resultsHint).toBeUndefined();
    }

    for (const child of children) child.releaseChild();
    closeTurn();
    await harness.shutdown();
  });
});

type WaitRow = {
  name: string;
  children: number;
  /** Builds the wait input from the child ids. */
  input: (ids: string[]) => Record<string, unknown>;
  /** Drives settlement while the wait runs; returns once its part is done. */
  drive: (ctx: WaitRowContext) => Promise<void>;
  /** Hides the in-process settlement signal from the wait, as for a settlement by another process. */
  noInProcessSignal?: boolean;
  abortAfterMs?: number;
  check: (output: any, ctx: WaitRowContext & { returnedAt: number; startedAt: number; storageReads: number }) => void;
};

type WaitRowContext = {
  ids: string[];
  release: (index: number) => void;
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
      // backoff poll needs about six over the same 1.6 s.
      expect(storageReads).toBeLessThanOrEqual(3);
      expect(returnedAt - settledAt(output, 0)).toBeLessThan(250);
    },
  },
  {
    name: 'a settlement with no in-process signal (another process) is still found by the storage poll',
    children: 1,
    noInProcessSignal: true,
    input: ids => ({ action: 'wait', subagentSessionId: ids[0], timeoutMs: 10_000 }),
    drive: async ({ release }) => release(0),
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
      expect(returnedAt - startedAt).toBeLessThan(1_000);
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
      // Fork main has no settlement signal to hide; there the row checks the poll alone.
      if (row.noInProcessSignal && '_internalOnDelegatedSettlement' in parent) {
        vi.spyOn(parent as any, '_internalOnDelegatedSettlement').mockReturnValue(() => {});
      }
      // The tool reads storage through the handle it takes at creation; count its own reads.
      let toolReads = 0;
      const storage = parent._internalStorage;
      const counting = new Proxy(storage, {
        get(target, key) {
          const value = Reflect.get(target, key, target);
          if (typeof value !== 'function') return value;
          return (...args: unknown[]) => {
            if (key === 'listPlanTasks' || key === 'loadSession') toolReads += 1;
            return value.apply(target, args);
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
        expect.objectContaining({ subagentSessionId: children.a, status: 'failed', cancelRequested: true }),
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
