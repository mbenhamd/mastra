/**
 * PF-7472 — parent notice when a detached `task_delegate` child settles.
 *
 * `subagents.onDelegatedSettlement` decides whether the parent model hears about
 * a settlement: `'none'` (default) keeps today's poll-only behaviour, `'remind'`
 * persists a system reminder for the next parent turn, and `'wake'` starts a
 * follow-up parent turn. The owed notice is written with the fenced settlement,
 * so delivery survives a restart and happens at most once.
 */

import { describe, expect, it, onTestFinished, vi } from 'vitest';
import { z } from 'zod';

import { Agent } from '../../agent';
import { convertArrayToReadableStream, MockLanguageModelV2 } from '../../agent/__tests__/mock-model';
import { hasHistoryLoadWatches } from '../../memory/history-load-watch';
import { MockMemory } from '../../memory/mock';
import { InMemoryStore } from '../../storage';
import type { HarnessTerminalFinalizer } from '../../storage/domains/harness';
import { InMemoryHarness } from '../../storage/domains/harness/inmemory';
import { InMemoryDB } from '../../storage/domains/inmemory-db';

import { MockAgent } from './__test-utils__';
import { HarnessConfigError } from './errors';
import { Harness } from './harness';
import {
  planTaskAcknowledgeDelegationSettlementNotice,
  planTaskClaimDelegationSettlementNotice,
  planTaskClearDelegationSettlementNoticeClaim,
  planTaskDowngradeDelegationWakeNotices,
  readPendingDelegationSettlementNotice,
} from './plan-task-session';
import { createPlanTaskTools, TASK_ADD_TOOL_ID, TASK_DELEGATE_TOOL_ID } from './plan-task-tool';
import type { Session } from './session';
import {
  HARNESS_SUBAGENT_OUTCOME_REPORT_KIND,
  HARNESS_SUBAGENT_OUTCOME_REPORT_TOOL_ID,
} from './terminal-subagent-result';

type NoticeMode = 'remind' | 'wake' | 'none' | undefined;

const messageFinalizer: HarnessTerminalFinalizer = {
  id: 'doxa.chat',
  version: '2026-09-28',
  finalize: async ({ result }) => ({
    projectionKind: 'chat.summary',
    projectionId: result.runId,
    payload: { status: result.status },
  }),
};

const wakeFinalizer: HarnessTerminalFinalizer = {
  id: 'doxa.chat-wake',
  version: '2026-11-04',
  finalize: async ({ result }) => ({
    projectionKind: 'chat.wake',
    projectionId: result.runId,
    payload: { status: result.status },
  }),
};

const completedReport = {
  status: 'success' as const,
  items: [
    {
      toolName: HARNESS_SUBAGENT_OUTCOME_REPORT_TOOL_ID,
      toolCallId: 'report-call-1',
      status: 'success' as const,
      value: {
        kind: HARNESS_SUBAGENT_OUTCOME_REPORT_KIND,
        outcome: 'completed',
        summary: 'Delegated test work completed',
        evidence: [{ kind: 'analysis', description: 'Verified by the delegated test fixture' }],
      },
    },
  ],
};

function buildHarness(db: InMemoryDB, onDelegatedSettlement: NoticeMode, memory?: MockMemory) {
  const parentAgent = new MockAgent({ id: 'parent-agent' });
  const childAgent = new MockAgent({ id: 'child-agent', defaultOutput: { terminalToolResult: completedReport } });
  // A memory-less mode that cannot delegate, like a governed workflow step.
  const quietAgent = new MockAgent({ id: 'quiet-agent' });
  if (memory !== undefined) {
    // Both modes can delegate, so `remind` requires memory on both agents.
    for (const agent of [parentAgent, childAgent]) {
      vi.spyOn(agent, 'hasOwnMemory').mockReturnValue(true);
      vi.spyOn(agent, 'getMemory').mockResolvedValue(memory as never);
      vi.spyOn(agent, '__fixedMemory').mockReturnValue(memory as never);
    }
  }
  const harness = new Harness({
    agents: { 'parent-agent': parentAgent, 'child-agent': childAgent, 'quiet-agent': quietAgent } as any,
    modes: [
      { id: 'default', agentId: 'parent-agent' },
      { id: 'worker-mode', agentId: 'child-agent' },
      { id: 'quiet-mode', agentId: 'quiet-agent', harnessBuiltins: 'exclude' },
    ],
    defaultModeId: 'default',
    sessions: { storage: new InMemoryHarness({ db }) },
    subagents: {
      maxDepth: 2,
      ...(onDelegatedSettlement !== undefined ? { onDelegatedSettlement } : {}),
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

/**
 * Deterministic end of notice delivery: the owed notice on `taskId` is gone, no
 * delivery is in flight, and the parent's work graph (including a woken turn)
 * is idle.
 */
async function deliveryFinished(parent: Session, taskId: string): Promise<void> {
  // The notice is written by the settlement itself, so wait for that first.
  await poll(async () => ['completed', 'failed', 'blocked'].includes((await planRow(parent, taskId))?.status ?? ''));
  await poll(
    async () =>
      (parent as any)._delegationNoticesInFlight.size === 0 && owedNotice(await planRow(parent, taskId)) === undefined,
  );
  await parent.waitForIdle({ timeoutMs: 2000 });
}

async function planRow(parent: Session, taskId: string) {
  const page = await parent._internalStorage.listPlanTasks({ sessionId: parent.id, limit: 100 });
  return page.tasks.find(t => t.taskId === taskId);
}

function owedNotice(row: { metadata?: unknown } | undefined): unknown {
  return (row?.metadata as any)?.mastraHarnessDelegationAttemptV1?.settlement?.notice;
}

/**
 * Delegate one task from inside a held parent turn, end that turn, and return a
 * release for the held child so it settles while the parent is idle.
 */
async function delegateFromParentTurn(
  parent: Session,
  parentAgent: MockAgent,
  childAgent: MockAgent,
  opts: { keepParentTurn?: boolean } = {},
) {
  let releaseChild!: () => void;
  childAgent.enqueueRun({
    holdUntil: new Promise<void>(r => (releaseChild = r)),
    finishReason: 'tool-calls',
    terminalToolResult: completedReport,
  });
  let releaseParent!: () => void;
  parentAgent.enqueueRun({ holdUntil: new Promise<void>(r => (releaseParent = r)), finishReason: 'stop' });
  const turn = parent.message({ content: 'go' });
  await poll(() => parentAgent.streamCalls.length === 1);
  const tools = createPlanTaskTools(parent);
  const task = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content: 'background work' } as any, toolCtx)) as any;
  const delegated = (await tools[TASK_DELEGATE_TOOL_ID]!.execute!(
    { taskId: task.taskId, agentType: 'worker' } as any,
    toolCtx,
  )) as any;
  await poll(() => childAgent.streamCalls.length === 1);
  const endParentTurn = async () => {
    releaseParent();
    await turn;
  };
  if (!opts.keepParentTurn) await endParentTurn();
  return { taskId: task.taskId as string, childId: delegated.subagentSessionId as string, releaseChild, endParentTurn };
}

/** Hold reminder-row writes to the parent thread until the returned release is called. */
function gatedMemory(): { memory: MockMemory; release: () => void; entered: Promise<void>; written: () => boolean } {
  const memory = new MockMemory();
  let release!: () => void;
  const gate = new Promise<void>(r => (release = r));
  let enter!: () => void;
  const entered = new Promise<void>(r => (enter = r));
  let written = false;
  const save = memory.saveMessages.bind(memory);
  vi.spyOn(memory, 'saveMessages').mockImplementation(async (args: any) => {
    if (!args.messages.some((m: { id?: string }) => m.id?.startsWith('harness-delegation-notice-'))) return save(args);
    enter();
    await gate;
    const saved = await save(args);
    written = true;
    return saved;
  });
  return { memory, release, entered, written: () => written };
}

function crashParent(db: InMemoryDB, parent: Session): void {
  for (const [key, r] of db.harnessSessions) {
    if (r.ownerId === (parent as any)._internalOwnerId) {
      db.harnessSessions.set(key, { ...r, ownerId: 'crashed', leaseExpiresAt: Date.now() - 1 });
    }
  }
}

function reminderCalls(agent: MockAgent) {
  return agent.streamCalls.filter(call => (call.messages as { tagName?: string })?.tagName === 'system-reminder');
}

describe('task_delegate settlement notice (subagents.onDelegatedSettlement)', () => {
  it('rejects an unknown notice mode', () => {
    expect(() => buildHarness(new InMemoryDB(), 'shout' as never)).toThrow(/onDelegatedSettlement/);
  });

  it('rejects remind when a delegation-capable mode has no memory to persist the reminder into', () => {
    const build = () => buildHarness(new InMemoryDB(), 'remind');
    expect(build).toThrow(HarnessConfigError);
    expect(build).toThrow(/subagents\.onDelegatedSettlement.*mode "default".*no memory/);
  });

  it('accepts remind on a memory-less mode when no subagent type exists to delegate to', async () => {
    const harness = new Harness({
      agents: { 'parent-agent': new MockAgent({ id: 'parent-agent' }) } as any,
      modes: [{ id: 'default', agentId: 'parent-agent' }],
      defaultModeId: 'default',
      sessions: { storage: new InMemoryHarness({ db: new InMemoryDB() }) },
      subagents: { maxDepth: 2, onDelegatedSettlement: 'remind', types: {} },
    });
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    // Without a subagent type there is no task_delegate, so no reminder can be owed.
    expect(Object.keys(createPlanTaskTools(session))).not.toContain(TASK_DELEGATE_TOOL_ID);
    await harness.shutdown();
  });

  it('shutdown does not report success while a reminder write is still pending', async () => {
    const gated = gatedMemory();
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB(), 'remind', gated.memory);
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const { releaseChild } = await delegateFromParentTurn(parent, parentAgent, childAgent);

    releaseChild();
    await gated.entered;
    // At the deadline the shutdown fails and names the outstanding work.
    await expect(harness.shutdown({ drainTimeoutMs: 200 })).rejects.toMatchObject({
      name: 'HarnessStorageError',
      cause: expect.objectContaining({ message: expect.stringContaining('Session storage work did not settle') }),
    });

    let shutdownSettled = false;
    const retry = harness.shutdown({ drainTimeoutMs: 30_000 }).finally(() => {
      shutdownSettled = true;
    });
    await new Promise(r => setTimeout(r, 50));
    expect(shutdownSettled).toBe(false);
    gated.release();
    await expect(retry).resolves.toBeUndefined();
    expect(gated.written()).toBe(true);
  });

  it('X1: a public reminder sent into a running turn while a notice row is being saved starts no run after a Stop', async () => {
    const gated = gatedMemory();
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB(), 'wake', gated.memory);
    try {
      const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const { releaseChild, endParentTurn } = await delegateFromParentTurn(parent, parentAgent, childAgent, {
        keepParentTurn: true,
      });
      releaseChild();
      await gated.entered;
      const injected = parent.injectSystemReminder({ content: 'public reminder' });
      void Promise.resolve(injected).catch(() => {});
      await parent.abortActiveWork({ reason: 'user_requested' });
      await endParentTurn().catch(() => {});
      gated.release();
      await poll(() => gated.written());
      await new Promise(r => setTimeout(r, 300));

      // Neither the public reminder nor the settlement notice starts a fresh run.
      expect(parentAgent.streamCalls).toHaveLength(1);
      expect(reminderCalls(parentAgent)).toHaveLength(0);
    } finally {
      gated.release();
      await harness.shutdown({ drainTimeoutMs: 2000 }).catch(() => {});
    }
  });

  it('a notice left owed by a shutdown that then fails is delivered once the session is live again', async () => {
    const memory = new MockMemory();
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB(), 'remind', memory);
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const { taskId, releaseChild } = await delegateFromParentTurn(parent, parentAgent, childAgent);
    // The delivery parks while resolving the parent's memory.
    let releaseMemory!: () => void;
    const memoryGate = new Promise<void>(r => (releaseMemory = r));
    let memoryEntered!: () => void;
    const entered = new Promise<void>(r => (memoryEntered = r));
    vi.spyOn(parentAgent, 'getMemory').mockImplementationOnce(async () => {
      memoryEntered();
      await memoryGate;
      return memory as never;
    });
    // Other shutdown storage work stalls past the drain deadline.
    let releaseStall!: () => void;
    const stall = new Promise<void>(r => (releaseStall = r));
    const append = InMemoryHarness.prototype.appendSessionEvent;
    const appendSpy = vi
      .spyOn(InMemoryHarness.prototype, 'appendSessionEvent')
      .mockImplementation(async function (this: InMemoryHarness, record) {
        if (parent.isClosing) await stall;
        return append.call(this, record);
      });
    onTestFinished(() => appendSpy.mockRestore());

    releaseChild();
    await entered;
    const shutdown = harness.shutdown({ drainTimeoutMs: 300 });
    // Shutdown has begun, so the delivery bails out and leaves the notice owed.
    await poll(() => parent.isClosing);
    releaseMemory();
    await expect(shutdown).rejects.toMatchObject({ name: 'HarnessStorageError' });
    expect(parent.isClosing).toBe(false);
    expect(owedNotice(await planRow(parent, taskId))).toMatchObject({ mode: 'remind' });

    // No new trigger: the restored session delivers the owed notice itself.
    releaseStall();
    await deliveryFinished(parent, taskId);
    const { messages } = await memory.recall({ threadId: parent.threadId, resourceId: 'u1', hideSignals: false });
    expect(messages.filter(m => JSON.stringify(m.content).includes('settled with outcome'))).toHaveLength(1);
    await harness.shutdown();
  });

  it('a notice left owed while shutdown waited on a session close is delivered after the shutdown rolls back', async () => {
    const memory = new MockMemory();
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB(), 'remind', memory);
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const other = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const { taskId, releaseChild } = await delegateFromParentTurn(parent, parentAgent, childAgent);
    // The delivery parks while resolving the parent's memory.
    let releaseMemory!: () => void;
    const memoryGate = new Promise<void>(r => (releaseMemory = r));
    let memoryEntered!: () => void;
    const entered = new Promise<void>(r => (memoryEntered = r));
    vi.spyOn(parentAgent, 'getMemory').mockImplementationOnce(async () => {
      memoryEntered();
      await memoryGate;
      return memory as never;
    });
    // Another session's close stalls in storage past the shutdown deadline.
    let releaseClose!: () => void;
    const closeGate = new Promise<void>(r => (releaseClose = r));
    let closeEntered!: () => void;
    const closeStalled = new Promise<void>(r => (closeEntered = r));
    const save = InMemoryHarness.prototype.saveSession;
    const saveSpy = vi
      .spyOn(InMemoryHarness.prototype, 'saveSession')
      .mockImplementation(async function (this: InMemoryHarness, record, opts) {
        if (record.id === other.id && other.isClosing) {
          closeEntered();
          await closeGate;
        }
        return save.call(this, record, opts);
      });
    onTestFinished(() => {
      releaseClose();
      saveSpy.mockRestore();
    });
    const closing = other.close().catch(() => {});
    await closeStalled;

    releaseChild();
    await entered;
    const shutdown = harness.shutdown({ drainTimeoutMs: 300 });
    // Shutdown is pending, so the delivery bails out and leaves the notice owed.
    releaseMemory();
    await expect(shutdown).rejects.toMatchObject({
      name: 'HarnessStorageError',
      cause: expect.objectContaining({ message: expect.stringContaining('Session close') }),
    });
    expect(owedNotice(await planRow(parent, taskId))).toMatchObject({ mode: 'remind' });

    // No new trigger, and the close is still stalled: the rolled-back harness
    // delivers the owed notice itself.
    await deliveryFinished(parent, taskId);
    const { messages } = await memory.recall({ threadId: parent.threadId, resourceId: 'u1', hideSignals: false });
    expect(messages.filter(m => JSON.stringify(m.content).includes('settled with outcome'))).toHaveLength(1);
    releaseClose();
    await closing;
    await harness.shutdown();
  });

  it('default config records no notice and never wakes the idle parent', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB(), undefined);
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const { taskId, releaseChild } = await delegateFromParentTurn(parent, parentAgent, childAgent);

    releaseChild();
    await poll(async () => (await planRow(parent, taskId))?.status === 'completed');
    await parent.waitForIdle({ timeoutMs: 2000 });

    expect(owedNotice(await planRow(parent, taskId))).toBeUndefined();
    expect(parentAgent.streamCalls).toHaveLength(1);
    await harness.shutdown();
  });

  it('remind persists one system reminder for the next turn without waking the parent', async () => {
    const memory = new MockMemory();
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB(), 'remind', memory);
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const { taskId, releaseChild } = await delegateFromParentTurn(parent, parentAgent, childAgent);

    releaseChild();
    await poll(async () => (await planRow(parent, taskId))?.status === 'completed');
    await deliveryFinished(parent, taskId);

    const { messages } = await memory.recall({ threadId: parent.threadId, resourceId: 'u1', hideSignals: false });
    const reminders = messages.filter(m => JSON.stringify(m.content).includes('settled with outcome'));
    expect(reminders).toHaveLength(1);
    expect(JSON.stringify(reminders[0]!.content)).toContain(taskId);
    expect(parentAgent.streamCalls).toHaveLength(1);
    await harness.shutdown();
  });

  it('remind persists into the delegating mode memory after the parent switched to a memory-less mode', async () => {
    const memory = new MockMemory();
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB(), 'remind', memory);
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const { taskId, releaseChild } = await delegateFromParentTurn(parent, parentAgent, childAgent);
    await parent.switchMode({ mode: 'quiet-mode' });

    releaseChild();
    await deliveryFinished(parent, taskId);

    const { messages } = await memory.recall({ threadId: parent.threadId, resourceId: 'u1', hideSignals: false });
    const reminders = messages.filter(m => JSON.stringify(m.content).includes('settled with outcome'));
    expect(reminders).toHaveLength(1);
    expect(JSON.stringify(reminders[0]!.content)).toContain(taskId);
    await harness.shutdown();
  });

  it('remind during a running parent turn never starts a turn', async () => {
    const memory = new MockMemory();
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB(), 'remind', memory);
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const { taskId, releaseChild, endParentTurn } = await delegateFromParentTurn(parent, parentAgent, childAgent, {
      keepParentTurn: true,
    });

    releaseChild();
    await poll(async () => owedNotice(await planRow(parent, taskId)) === undefined);
    await poll(async () => (await planRow(parent, taskId))?.status === 'completed');
    await endParentTurn();
    await deliveryFinished(parent, taskId);

    const { messages } = await memory.recall({ threadId: parent.threadId, resourceId: 'u1', hideSignals: false });
    expect(messages.filter(m => JSON.stringify(m.content).includes('settled with outcome'))).toHaveLength(1);
    expect(parentAgent.streamCalls).toHaveLength(1);
    await harness.shutdown();
  });

  it('wake starts exactly one follow-up turn after the running parent turn ends, and a restart does not repeat it', async () => {
    const db = new InMemoryDB();
    const a = buildHarness(db, 'wake');
    const parentA = await a.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const { taskId, releaseChild, endParentTurn } = await delegateFromParentTurn(parentA, a.parentAgent, a.childAgent, {
      keepParentTurn: true,
    });

    releaseChild();
    await poll(async () => (await planRow(parentA, taskId))?.status === 'completed');
    expect(reminderCalls(a.parentAgent)).toHaveLength(0);
    await endParentTurn();
    await poll(() => reminderCalls(a.parentAgent).length === 1);
    const wake = reminderCalls(a.parentAgent)[0]!.messages as { contents?: unknown };
    expect(String(wake.contents)).toContain(taskId);
    expect(String(wake.contents)).toContain('completed');
    await deliveryFinished(parentA, taskId);

    // Restart reconcile scans the same settled task and must not deliver again.
    crashParent(db, parentA);
    await a.harness.shutdown().catch(() => {});
    const b = buildHarness(db, 'wake');
    const parentB = await b.harness.session({ sessionId: parentA.id, resourceId: 'u1' });
    await (parentB as any)._reconcileDelegationsOnHydrate(0);
    await deliveryFinished(parentB, taskId);
    expect(reminderCalls(b.parentAgent)).toHaveLength(0);
    expect(reminderCalls(a.parentAgent)).toHaveLength(1);
    await b.harness.shutdown();
  });

  // PF-7626: a consumer that renders the woken turn (for example as a reply
  // bubble with its own id) must tell it apart from a caller-started turn from
  // the public event alone, without reading the private signal-id format.
  it('a settlement wake turn announces itself on agent_start; the caller-started turn does not', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB(), 'wake');
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const sessionEvents: Array<Record<string, unknown>> = [];
    const hostEvents: Array<Record<string, unknown>> = [];
    const offSession = parent.subscribe(event => {
      sessionEvents.push({ ...event });
    });
    const offHost = harness.subscribe(event => {
      hostEvents.push({ ...event });
    });
    const { taskId, releaseChild } = await delegateFromParentTurn(parent, parentAgent, childAgent);
    // The woken run streams a reply. The chunks carry no runId: the mock cannot
    // know the runtime-allocated one, so the Session attributes them to the run
    // its agent_start stamped.
    parentAgent.enqueueRun({
      finishReason: 'stop',
      chunks: [
        { type: 'text-start', payload: { id: 'wake-msg' } },
        { type: 'text-delta', payload: { id: 'wake-msg', text: 'woken reply' } },
        { type: 'text-end', payload: { id: 'wake-msg' } },
      ],
    });

    releaseChild();
    await poll(() => reminderCalls(parentAgent).length === 1);
    await deliveryFinished(parent, taskId);
    offSession();
    offHost();

    const starts = sessionEvents.filter(e => e.type === 'agent_start');
    expect(starts).toHaveLength(2);
    const [userTurn, wakeTurn] = starts;
    expect(userTurn).not.toHaveProperty('origin');
    expect(userTurn).not.toHaveProperty('wakeNoticeId');
    expect(wakeTurn).toMatchObject({ origin: 'wake', wakeNoticeId: expect.any(String) });
    expect(wakeTurn!.runId).not.toBe(userTurn!.runId);
    // The notice id names the persisted reminder the woken run delivers.
    const delivered = reminderCalls(parentAgent)[0]!.messages as { id?: unknown };
    expect(wakeTurn!.wakeNoticeId).toBe(delivered.id);

    // A consumer registers the reply on agent_start, so it must arrive before
    // the woken run's first streamed token.
    const wakeStartAt = sessionEvents.indexOf(wakeTurn!);
    const firstWakeDeltaAt = sessionEvents.findIndex(e => e.type === 'text_delta' && e.delta === 'woken reply');
    expect(firstWakeDeltaAt).toBeGreaterThan(-1);
    expect(sessionEvents[firstWakeDeltaAt]).toMatchObject({ runId: wakeTurn!.runId });
    expect(wakeStartAt).toBeLessThan(firstWakeDeltaAt);

    // The host-level subscription sees the same wake identity.
    const hostStarts = hostEvents.filter(e => e.type === 'agent_start' && e.sessionId === parent.id);
    expect(hostStarts).toHaveLength(2);
    expect(hostStarts[0]).not.toHaveProperty('origin');
    expect(hostStarts[1]).toMatchObject({
      runId: wakeTurn!.runId,
      origin: 'wake',
      wakeNoticeId: wakeTurn!.wakeNoticeId,
    });
    await harness.shutdown();
  });

  it('a restart between the settlement commit and the notice delivers the owed wake exactly once', async () => {
    const db = new InMemoryDB();
    const a = buildHarness(db, 'wake');
    const parentA = await a.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    // The process dies after the fenced settlement write, before the reminder
    // row lands: every parent-thread write fails on instance A.
    const attemptedReminderIds: string[] = [];
    vi.spyOn(a.parentAgent, 'getMemory').mockResolvedValue({
      saveMessages: (args: { messages: Array<{ id?: string }> }) => {
        for (const m of args.messages) if (m.id !== undefined) attemptedReminderIds.push(m.id);
        return Promise.reject(new Error('storage down'));
      },
    } as never);
    const { taskId, releaseChild } = await delegateFromParentTurn(parentA, a.parentAgent, a.childAgent);

    releaseChild();
    await poll(async () => (await planRow(parentA, taskId))?.status === 'completed');
    await poll(() => (parentA as any)._delegationNoticesInFlight.size === 0);
    expect(owedNotice(await planRow(parentA, taskId))).toMatchObject({ mode: 'wake' });
    expect(reminderCalls(a.parentAgent)).toHaveLength(0);
    crashParent(db, parentA);
    await a.harness.shutdown().catch(() => {});

    expect(attemptedReminderIds).toHaveLength(1);

    const memoryB = new MockMemory();
    const b = buildHarness(db, 'wake', memoryB);
    // PF-7626: the redelivered wake carries the same notice id as before the restart.
    const startsB: Array<Record<string, unknown>> = [];
    const offB = b.harness.subscribe(event => {
      if (event.type === 'agent_start') startsB.push({ ...event });
    });
    const parentB = await b.harness.session({ sessionId: parentA.id, resourceId: 'u1' });
    await poll(() => reminderCalls(b.parentAgent).length === 1);
    await deliveryFinished(parentB, taskId);
    offB();
    const { messages: rowsB } = await memoryB.recall({
      threadId: parentB.threadId,
      resourceId: 'u1',
      hideSignals: false,
    });
    const persisted = rowsB.filter(m => JSON.stringify(m.content).includes('settled with outcome'));
    expect(persisted).toHaveLength(1);
    expect(persisted[0]!.id).toBe(attemptedReminderIds[0]);
    const wakeStartsB = startsB.filter(e => e.sessionId === parentB.id && e.origin === 'wake');
    expect(wakeStartsB).toHaveLength(1);
    expect(wakeStartsB[0]).toMatchObject({ origin: 'wake', wakeNoticeId: attemptedReminderIds[0] });

    crashParent(db, parentB);
    await b.harness.shutdown().catch(() => {});
    const c = buildHarness(db, 'wake');
    const parentC = await c.harness.session({ sessionId: parentA.id, resourceId: 'u1' });
    await (parentC as any)._reconcileDelegationsOnHydrate(0);
    await deliveryFinished(parentC, taskId);
    expect(reminderCalls(c.parentAgent)).toHaveLength(0);
    expect(reminderCalls(b.parentAgent)).toHaveLength(1);
    await c.harness.shutdown();
  });

  it('a live delivery racing a restart reconcile on shared storage wakes the parent exactly once', async () => {
    const db = new InMemoryDB();
    const gated = gatedMemory();
    const a = buildHarness(db, 'wake', gated.memory);
    const parentA = await a.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const { taskId, releaseChild } = await delegateFromParentTurn(parentA, a.parentAgent, a.childAgent);

    // A's live delivery parks inside its reminder write.
    releaseChild();
    await gated.entered;
    // Instance B takes the parent over and reconciles the same owed notice.
    crashParent(db, parentA);
    const b = buildHarness(db, 'wake');
    const parentB = await b.harness.session({ sessionId: parentA.id, resourceId: 'u1' });
    await poll(() => reminderCalls(b.parentAgent).length === 1);
    await deliveryFinished(parentB, taskId);

    // A resumes with its stale view; the fenced claim must stop its wake.
    gated.release();
    await poll(() => (parentA as any)._delegationNoticesInFlight.size === 0);
    await parentA.waitForIdle({ timeoutMs: 2000 }).catch(() => {});
    expect(reminderCalls(a.parentAgent)).toHaveLength(0);
    expect(reminderCalls(b.parentAgent)).toHaveLength(1);
    await b.harness.shutdown();
    await a.harness.shutdown().catch(() => {});
  });

  it('a memory-less notice still wakes after a parent turn that ran after the save (PF-7679)', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB(), 'wake');
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const { taskId, releaseChild } = await delegateFromParentTurn(parent, parentAgent, childAgent);
    // A user turn runs to completion between the reminder save and the wake claim.
    const serialize = (parent as any)._serializePlanTaskWrite.bind(parent);
    let userTurn: Promise<unknown> | undefined;
    vi.spyOn(parent as any, '_serializePlanTaskWrite').mockImplementation(async (run: unknown) => {
      if (userTurn === undefined && (parent as any)._delegationNoticesInFlight.size > 0) {
        userTurn = parent.message({ content: 'a new user turn' });
        await userTurn;
      }
      return serialize(run);
    });

    releaseChild();
    await deliveryFinished(parent, taskId);

    expect(userTurn).toBeDefined();
    // No row exists, so the user turn could not have read the notice.
    expect(reminderCalls(parentAgent)).toHaveLength(1);
    expect(parentAgent.streamCalls).toHaveLength(3);
    await harness.shutdown();
  });

  it('F8.4: a notice claim survives the Stop downgrade; only its own claimId clears or acknowledges it', async () => {
    const memory = new MockMemory();
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB(), 'wake', memory);
    try {
      const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const { taskId, childId, releaseChild } = await delegateFromParentTurn(parent, parentAgent, childAgent);
      // Capture the owed notice instead of delivering it.
      let notice: { digest: string } | undefined;
      vi.spyOn(parent as any, '_startDelegationSettlementNotice').mockImplementation((n: unknown) => {
        notice = n as { digest: string };
      });
      releaseChild();
      await poll(() => notice !== undefined);
      const port = (parent as any)._planTaskPort();
      const key = { taskId, subagentSessionId: childId, digest: notice!.digest };
      const claimA = { claimId: 'claim-a', ownerId: 'owner-a', claimedAt: 1_000 };
      const never = () => false;

      expect(await planTaskClaimDelegationSettlementNotice(port, { ...key, claim: claimA, takeover: never })).toBe(
        'claimed',
      );
      expect(owedNotice(await planRow(parent, taskId))).toEqual({ mode: 'wake', digest: key.digest, claim: claimA });
      expect(readPendingDelegationSettlementNotice((await planRow(parent, taskId))!)?.claim).toEqual(claimA);
      // A second claimer is refused while the claim holds; another digest is refused.
      const claimB = { claimId: 'claim-b', ownerId: 'owner-b', claimedAt: 2_000 };
      expect(await planTaskClaimDelegationSettlementNotice(port, { ...key, claim: claimB, takeover: never })).toBe(
        'claim_lost',
      );
      expect(
        await planTaskClaimDelegationSettlementNotice(port, {
          ...key,
          digest: 'f'.repeat(64),
          claim: claimB,
          takeover: () => true,
        }),
      ).toBe('claim_lost');

      // The Stop downgrade sets remind and keeps the claim.
      expect(await planTaskDowngradeDelegationWakeNotices(port)).toBe(1);
      expect(owedNotice(await planRow(parent, taskId))).toEqual({ mode: 'remind', digest: key.digest, claim: claimA });
      expect(readPendingDelegationSettlementNotice((await planRow(parent, taskId))!)).toMatchObject({
        mode: 'remind',
        claim: claimA,
      });

      // Another claimId neither acknowledges nor clears it.
      expect(await planTaskAcknowledgeDelegationSettlementNotice(port, { ...key, claimId: 'claim-b' })).toBe(false);
      expect(await planTaskAcknowledgeDelegationSettlementNotice(port, key)).toBe(false);
      expect(
        await planTaskClearDelegationSettlementNoticeClaim(port, { ...key, claimId: 'claim-b', mode: 'wake' }),
      ).toBe(false);
      // Clearing never upgrades the Stop's remind.
      expect(
        await planTaskClearDelegationSettlementNoticeClaim(port, { ...key, claimId: 'claim-a', mode: 'wake' }),
      ).toBe(true);
      expect(owedNotice(await planRow(parent, taskId))).toEqual({ mode: 'remind', digest: key.digest });

      // A takeover-eligible claim is overwritten, then acknowledged by its own claimId.
      expect(await planTaskClaimDelegationSettlementNotice(port, { ...key, claim: claimB, takeover: never })).toBe(
        'claimed',
      );
      const claimC = { claimId: 'claim-c', ownerId: 'owner-c', claimedAt: 3_000 };
      expect(
        await planTaskClaimDelegationSettlementNotice(port, {
          ...key,
          claim: claimC,
          takeover: claim => claim.claimId === 'claim-b',
        }),
      ).toBe('claimed');
      expect(await planTaskAcknowledgeDelegationSettlementNotice(port, { ...key, claimId: 'claim-b' })).toBe(false);
      expect(await planTaskAcknowledgeDelegationSettlementNotice(port, { ...key, claimId: 'claim-c' })).toBe(true);
      expect(owedNotice(await planRow(parent, taskId))).toBeUndefined();
    } finally {
      await harness.shutdown();
    }
  });

  it('a Stop between a failed delivery and its retry still suppresses the wake', async () => {
    const memory = new MockMemory();
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB(), 'wake', memory);
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const save = memory.saveMessages.bind(memory);
    let failReminderWrite = true;
    vi.spyOn(memory, 'saveMessages').mockImplementation(async (args: any) => {
      if (
        failReminderWrite &&
        args.messages.some((m: { id?: string }) => m.id?.startsWith('harness-delegation-notice-'))
      ) {
        failReminderWrite = false;
        throw new Error('transient storage failure');
      }
      return save(args);
    });
    const { taskId, releaseChild } = await delegateFromParentTurn(parent, parentAgent, childAgent);

    releaseChild();
    await poll(() => !failReminderWrite && (parent as any)._delegationNoticesInFlight.size === 0);
    expect(owedNotice(await planRow(parent, taskId))).toMatchObject({ mode: 'wake' });
    await parent.abortActiveWork({ reason: 'user_requested' });
    // The retry runs under a fresh reconcile pass with a post-Stop epoch.
    await (parent as any)._reconcileDelegationsOnHydrate(0);
    await deliveryFinished(parent, taskId);

    expect(reminderCalls(parentAgent)).toHaveLength(0);
    expect(parentAgent.streamCalls).toHaveLength(1);
    await harness.shutdown();
  });

  it('a Stop before a restart durably suppresses the owed wake', async () => {
    const db = new InMemoryDB();
    const a = buildHarness(db, 'wake');
    const parentA = await a.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const attemptedReminderIds: string[] = [];
    vi.spyOn(a.parentAgent, 'getMemory').mockResolvedValue({
      saveMessages: (args: { messages: Array<{ id?: string }> }) => {
        for (const m of args.messages) if (m.id !== undefined) attemptedReminderIds.push(m.id);
        return Promise.reject(new Error('storage down'));
      },
    } as never);
    const { taskId, releaseChild } = await delegateFromParentTurn(parentA, a.parentAgent, a.childAgent);
    releaseChild();
    await poll(async () => (await planRow(parentA, taskId))?.status === 'completed');
    await poll(() => (parentA as any)._delegationNoticesInFlight.size === 0);

    // A failed suppression write fails the Stop instead of reporting success.
    const mutate = parentA._internalStorage.mutatePlanTasksForSession.bind(parentA._internalStorage);
    const failOnce = vi
      .spyOn(parentA._internalStorage, 'mutatePlanTasksForSession')
      .mockRejectedValueOnce(new Error('plan write failed'));
    await expect(parentA.abortActiveWork({ reason: 'user_requested' })).rejects.toThrow('plan write failed');
    expect(owedNotice(await planRow(parentA, taskId))).toMatchObject({ mode: 'wake' });
    failOnce.mockImplementation(mutate);
    await parentA.abortActiveWork({ reason: 'user_requested' });
    expect(owedNotice(await planRow(parentA, taskId))).toMatchObject({ mode: 'remind' });
    crashParent(db, parentA);
    await a.harness.shutdown().catch(() => {});

    const b = buildHarness(db, 'wake');
    const parentB = await b.harness.session({ sessionId: parentA.id, resourceId: 'u1' });
    await (parentB as any)._reconcileDelegationsOnHydrate(0);
    await deliveryFinished(parentB, taskId);
    expect(reminderCalls(b.parentAgent)).toHaveLength(0);
    expect(b.parentAgent.streamCalls).toHaveLength(0);
    await b.harness.shutdown();
  });

  it('a Stop after the reminder was persisted suppresses the pending wake', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB(), 'wake');
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const { taskId, releaseChild, endParentTurn } = await delegateFromParentTurn(parent, parentAgent, childAgent, {
      keepParentTurn: true,
    });

    // The child settles mid-turn; the wake waits for the parent turn to end.
    releaseChild();
    await poll(async () => (await planRow(parent, taskId))?.status === 'completed');
    await parent.abortActiveWork({ reason: 'user_requested' });
    await endParentTurn().catch(() => {});
    await deliveryFinished(parent, taskId);

    expect(reminderCalls(parentAgent)).toHaveLength(0);
    expect(parentAgent.streamCalls).toHaveLength(1);
    await harness.shutdown();
  });

  it('a parent Stop that cancels the child sends no wake and owes no notice', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB(), 'wake');
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const { taskId, releaseChild } = await delegateFromParentTurn(parent, parentAgent, childAgent);

    await parent.abortActiveWork({ reason: 'user_requested' });
    releaseChild();
    await poll(async () => (await planRow(parent, taskId))?.status === 'failed');
    await parent.waitForIdle({ timeoutMs: 2000 });

    expect(owedNotice(await planRow(parent, taskId))).toBeUndefined();
    expect(reminderCalls(parentAgent)).toHaveLength(0);
    expect(parentAgent.streamCalls).toHaveLength(1);
    await harness.shutdown();
  });
});

/**
 * PF-7632 — hosts that gate user-turn admission on `isTurnActive()` now admit a
 * user turn while a detached child still runs. These rows pin how such a turn
 * interacts with the child's settlement, a settlement wake, and Stop.
 */
describe('user turns admitted while a delegated child runs (isTurnActive)', () => {
  /** Count overlapping parent turns from the public event stream. */
  function trackTurns(parent: Session) {
    const order: string[] = [];
    let active = 0;
    let maxActive = 0;
    parent.subscribe(event => {
      if (event.type === 'agent_start') {
        active += 1;
        maxActive = Math.max(maxActive, active);
        order.push('start');
      } else if (event.type === 'agent_end') {
        active -= 1;
        order.push('end');
      }
    });
    return { order, maxActive: () => maxActive };
  }

  /** Start a user turn whose model run is held; `release` lets it finish. */
  function startHeldUserTurn(parent: Session, parentAgent: MockAgent, content: string) {
    let release!: () => void;
    parentAgent.enqueueRun({ holdUntil: new Promise<void>(r => (release = r)), finishReason: 'stop', text: content });
    const turn = parent.message({ content });
    void turn.catch(() => {});
    return {
      turn,
      release,
      end: async () => {
        release();
        await turn;
      },
    };
  }

  async function heldUserTurn(parent: Session, parentAgent: MockAgent, content: string) {
    const before = parentAgent.streamCalls.length;
    const held = startHeldUserTurn(parent, parentAgent, content);
    await poll(() => parentAgent.streamCalls.length === before + 1);
    return held;
  }

  it('a user turn admitted as the child settles loses no settlement and never overlaps another turn', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB(), undefined);
    const releases: Array<() => void> = [];
    try {
      const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const { taskId, releaseChild } = await delegateFromParentTurn(parent, parentAgent, childAgent);
      releases.push(releaseChild);
      const turns = trackTurns(parent);
      expect(parent.isTurnActive()).toBe(false);
      expect(parent.isBusy()).toBe(true);

      // Release the child, then admit a user turn without waiting for the settlement.
      releaseChild();
      const user = await heldUserTurn(parent, parentAgent, 'how are the tasks going?');
      releases.push(user.release);
      expect(parent.isTurnActive()).toBe(true);
      await poll(async () => (await planRow(parent, taskId))?.status === 'completed');
      await user.end();
      await parent.waitForIdle({ timeoutMs: 2000 });

      expect(parent.isTurnActive()).toBe(false);
      expect(parent.isBusy()).toBe(false);
      expect(parent._internalSubagentExecutionsInFlight).toBe(0);
      expect((await planRow(parent, taskId))?.status).toBe('completed');
      expect(turns.order).toEqual(['start', 'end']);
      expect(turns.maxActive()).toBe(1);
      expect(parentAgent.streamCalls).toHaveLength(2);
    } finally {
      for (const release of releases) release();
      await harness.shutdown();
    }
  });

  it('a settlement wake owed during an admitted user turn starts exactly once, after that turn ends', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB(), 'wake');
    const releases: Array<() => void> = [];
    try {
      const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      // The user turn here did not delegate: it was admitted on isTurnActive()
      // after the delegating turn had already ended.
      const { taskId, releaseChild } = await delegateFromParentTurn(parent, parentAgent, childAgent);
      releases.push(releaseChild);
      const turns = trackTurns(parent);
      expect(parent.isTurnActive()).toBe(false);

      const user = await heldUserTurn(parent, parentAgent, 'anything new?');
      releases.push(user.release);
      releaseChild();
      await poll(async () => (await planRow(parent, taskId))?.status === 'completed');
      expect(reminderCalls(parentAgent)).toHaveLength(0);
      expect(turns.order).toEqual(['start']);

      await user.end();
      await deliveryFinished(parent, taskId);

      expect(reminderCalls(parentAgent)).toHaveLength(1);
      expect(String((reminderCalls(parentAgent)[0]!.messages as { contents?: unknown }).contents)).toContain(taskId);
      expect(turns.order).toEqual(['start', 'end', 'start', 'end']);
      expect(turns.maxActive()).toBe(1);
      expect(parent.isTurnActive()).toBe(false);
      expect(parent.isBusy()).toBe(false);
    } finally {
      for (const release of releases) release();
      await harness.shutdown();
    }
  });

  // PF-7679 (Option A): `MockAgent` runs no input processors, so its user turn
  // is never recorded as loading the reminder row; the wake follows that turn.
  // The real-Agent rows below prove the case where the user run loads the row.
  it('a user turn admitted just before the claimed wake begins keeps the slot; the wake runs after it ends', async () => {
    const memory = new MockMemory();
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB(), 'wake', memory);
    const releases: Array<() => void> = [];
    try {
      const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const { taskId, releaseChild } = await delegateFromParentTurn(parent, parentAgent, childAgent);
      releases.push(releaseChild);
      const turns = trackTurns(parent);
      // The notice is saved and claimed; the user's message lands before the
      // wake reaches its own turn admission. Seam: the private wake injector.
      let user: ReturnType<typeof startHeldUserTurn> | undefined;
      let turnActiveBeforeAdmission: boolean | undefined;
      let turnActiveAtAdmission: boolean | undefined;
      let firstAttemptSettled = false;
      const inject = (parent as any)._injectSystemReminder.bind(parent);
      vi.spyOn(parent as any, '_injectSystemReminder').mockImplementation((...args: unknown[]) => {
        const first = user === undefined;
        if (first) {
          turnActiveBeforeAdmission = parent.isTurnActive();
          user = startHeldUserTurn(parent, parentAgent, 'what happened?');
          releases.push(user.release);
          turnActiveAtAdmission = parent.isTurnActive();
        }
        const attempt = inject(...args) as Promise<unknown>;
        if (first)
          void attempt.then(
            () => (firstAttemptSettled = true),
            () => (firstAttemptSettled = true),
          );
        return attempt;
      });

      releaseChild();
      await poll(() => user !== undefined);
      // PF-7679: the refused wake waits, unclaimed, for the user's turn to end.
      await poll(() => firstAttemptSettled);
      expect(owedNotice(await planRow(parent, taskId))).toBeDefined();
      await user!.end();
      await deliveryFinished(parent, taskId);

      expect(turnActiveBeforeAdmission).toBe(false);
      expect(turnActiveAtAdmission).toBe(true);
      expect(reminderCalls(parentAgent)).toHaveLength(1);
      expect(turns.order).toEqual(['start', 'end', 'start', 'end']);
      expect(turns.maxActive()).toBe(1);
      // The row is persisted once.
      const { messages } = await memory.recall({ threadId: parent.threadId, resourceId: 'u1', hideSignals: false });
      expect(messages.filter(m => JSON.stringify(m.content).includes('settled with outcome'))).toHaveLength(1);
      expect(owedNotice(await planRow(parent, taskId))).toBeUndefined();
    } finally {
      for (const release of releases) release();
      await harness.shutdown();
    }
  });

  // PF-7626: the wake passes its idle check, then awaits turn context. A user
  // turn that starts its run inside that await keeps the slot, and no
  // agent_start is labelled a wake. PF-7679: the claimed wake is not sent into
  // that turn: it clears its claim and waits. `MockAgent` runs no input
  // processors, so the user run is never recorded as loading the row, and the
  // wake follows in its own, labelled run.
  it('a user turn that starts while the claimed wake builds its context is never labelled a wake', async () => {
    const memory = new MockMemory();
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB(), 'wake', memory);
    const releases: Array<() => void> = [];
    try {
      const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const { taskId, releaseChild } = await delegateFromParentTurn(parent, parentAgent, childAgent);
      releases.push(releaseChild);
      const starts: Array<Record<string, unknown>> = [];
      parent.subscribe(event => {
        if (event.type === 'agent_start') starts.push({ ...event });
      });
      // Seam: hold the wake's context build (the await between its idle check
      // and its dispatch) until the user's run has started.
      let wakeAdmitted = false;
      let held = false;
      let firstAttemptSettled = false;
      let user: ReturnType<typeof startHeldUserTurn> | undefined;
      const inject = (parent as any)._injectSystemReminder.bind(parent);
      vi.spyOn(parent as any, '_injectSystemReminder').mockImplementation((...args: unknown[]) => {
        const first = !wakeAdmitted;
        wakeAdmitted = true;
        const attempt = inject(...args) as Promise<unknown>;
        if (first)
          void attempt.then(
            () => (firstAttemptSettled = true),
            () => (firstAttemptSettled = true),
          );
        return attempt;
      });
      const build = (parent as any)._buildTurnContextAndInstructions.bind(parent);
      vi.spyOn(parent as any, '_buildTurnContextAndInstructions').mockImplementation(async (...args: unknown[]) => {
        // The user's own message() reaches this seam synchronously, so mark the
        // hold taken before starting it.
        if (wakeAdmitted && !held) {
          held = true;
          const before = parentAgent.streamCalls.length;
          user = startHeldUserTurn(parent, parentAgent, 'what happened?');
          releases.push(user.release);
          await poll(() => parentAgent.streamCalls.length === before + 1);
        }
        return build(...args);
      });

      releaseChild();
      await poll(() => user !== undefined);
      await poll(() => firstAttemptSettled);
      await user!.end();
      await deliveryFinished(parent, taskId);

      // The interleave happened: the user's run started during the wake's context build.
      expect(held).toBe(true);
      // The user's run carries no wake identity; only the later wake run does.
      expect(starts).toHaveLength(2);
      expect(starts[0]).not.toHaveProperty('origin');
      expect(starts[0]).not.toHaveProperty('wakeNoticeId');
      expect(starts[1]).toMatchObject({ origin: 'wake' });
      expect(reminderCalls(parentAgent)).toHaveLength(1);
      // The persisted row is written once.
      const { messages } = await memory.recall({ threadId: parent.threadId, resourceId: 'u1', hideSignals: false });
      expect(messages.filter(m => JSON.stringify(m.content).includes('settled with outcome'))).toHaveLength(1);
      expect(owedNotice(await planRow(parent, taskId))).toBeUndefined();
    } finally {
      for (const release of releases) release();
      await harness.shutdown();
    }
  });

  it('aborting an admitted user turn leaves the child running; Stop (abortActiveWork) cancels it', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB(), 'wake');
    const releases: Array<() => void> = [];
    try {
      const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const { taskId, childId, releaseChild } = await delegateFromParentTurn(parent, parentAgent, childAgent);
      releases.push(releaseChild);
      const childRunning = () => harness._internalGetLiveSession(childId)?.isRunning() === true;

      const first = await heldUserTurn(parent, parentAgent, 'never mind');
      releases.push(first.release);
      parent.abort();
      await first.turn.catch(() => {});
      await poll(() => !parent.isRunning());
      expect(parent.isTurnActive()).toBe(false);
      expect(parent.isBusy()).toBe(true);
      expect(childRunning()).toBe(true);
      expect((await planRow(parent, taskId))?.status).toBe('in_progress');

      const second = await heldUserTurn(parent, parentAgent, 'stop everything');
      releases.push(second.release);
      const stopped = await parent.abortActiveWork({ reason: 'user_requested' });
      await second.turn.catch(() => {});
      releaseChild();
      await poll(async () => (await planRow(parent, taskId))?.status === 'failed');
      await parent.waitForIdle({ timeoutMs: 2000 });

      expect(stopped.cancelledSubagentSessionIds).toEqual([childId]);
      expect(owedNotice(await planRow(parent, taskId))).toBeUndefined();
      expect(reminderCalls(parentAgent)).toHaveLength(0);
      expect(parent.isTurnActive()).toBe(false);
      expect(parent.isBusy()).toBe(false);
    } finally {
      for (const release of releases) release();
      await harness.shutdown();
    }
  });

  it('reads as an active turn until Stop finishes cancelling children, so a message admitted after Stop survives', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB(), undefined);
    const releases: Array<() => void> = [];
    try {
      const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const { childId, releaseChild } = await delegateFromParentTurn(parent, parentAgent, childAgent);
      releases.push(releaseChild);
      const user = await heldUserTurn(parent, parentAgent, 'working on it?');
      releases.push(user.release);
      // Hold Stop between the turn abort and the child cancellation. Seam: the
      // private delegated-child discovery Stop awaits before cancelling.
      let releaseDiscovery!: () => void;
      const discoveryGate = new Promise<void>(r => (releaseDiscovery = r));
      releases.push(releaseDiscovery);
      const listDelegated = (parent as any)._listDelegatedSubagentSessionIds.bind(parent);
      vi.spyOn(parent as any, '_listDelegatedSubagentSessionIds').mockImplementation(async () => {
        await discoveryGate;
        return listDelegated();
      });

      const stopping = parent.abortActiveWork({ reason: 'user_requested' });
      await user.turn.catch(() => {});
      await poll(() => !parent.isRunning());
      expect(parent.isTurnActive()).toBe(true);

      releaseDiscovery();
      const stopped = await stopping;
      releaseChild();
      expect(stopped.cancelledSubagentSessionIds).toEqual([childId]);
      expect(parent.isTurnActive()).toBe(false);

      parentAgent.enqueueRun({ finishReason: 'stop', text: 'next answer' });
      const next = (await parent.message({ content: 'start over' })) as { text?: string; finishReason?: string };
      expect(next.finishReason).toBe('stop');
      expect(next.text).toBe('next answer');
      await parent.waitForIdle({ timeoutMs: 2000 });
    } finally {
      for (const release of releases) release();
      await harness.shutdown();
    }
  });
});

// PF-7626: the race path with REAL parent Agents over one mock model, so thread
// history and queued signals reach the provider prompt through the real
// runtime. A user turn starts its run while the claimed wake builds its turn
// context. The wake then sends its content exactly as fork main does and only
// drops its label: no agent_start may carry origin or wakeNoticeId. The one
// difference from fork main is a Stop during the build await, which the Stop
// contract ("not stopped or cancelled since the settlement") keeps silent.
describe('a caller turn that takes the slot from the claimed wake (real Agent model input)', () => {
  const usage = { inputTokens: 1, outputTokens: 1, totalTokens: 2 };
  const NOTICE = 'settled with outcome';

  /**
   * `default` runs `parent-agent` (memory optional); `quiet-mode` runs a
   * memory-less agent. `grant` registers the message and wake finalizers.
   */
  function realParent(
    withMemory: boolean,
    shared?: {
      db: InMemoryDB;
      memory?: MockMemory;
      dynamicMemory?: boolean;
      memoryFactory?: () => MockMemory;
      /** The reply text of provider call `call` (default `reply <call>`). */
      reply?: (call: number) => string;
    },
    grant = false,
  ) {
    const prompts: string[] = [];
    const holds = new Map<number, Promise<void>>();
    const model = new MockLanguageModelV2({
      // The structured sync `message()` path (`agent.generate`) calls this.
      doGenerate: async options => {
        const call = prompts.length;
        prompts.push(JSON.stringify((options as { prompt: unknown }).prompt));
        await holds.get(call);
        const text = shared?.reply?.(call) ?? `reply ${call}`;
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          finishReason: 'stop' as const,
          usage,
          warnings: [],
          content: [{ type: 'text' as const, text }],
        };
      },
      doStream: async options => {
        const call = prompts.length;
        prompts.push(JSON.stringify((options as { prompt: unknown }).prompt));
        await holds.get(call);
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: convertArrayToReadableStream([
            { type: 'stream-start', warnings: [] },
            { type: 'response-metadata', id: `id-${call}`, modelId: 'mock-model-id', timestamp: new Date(0) },
            { type: 'text-start', id: 't' },
            { type: 'text-delta', id: 't', delta: shared?.reply?.(call) ?? `reply ${call}` },
            { type: 'text-end', id: 't' },
            { type: 'finish', finishReason: 'stop', usage },
          ]),
        };
      },
    });
    const memory = withMemory ? (shared?.memory ?? new MockMemory()) : undefined;
    const parentAgent = new Agent({
      id: 'parent-agent',
      name: 'parent-agent',
      instructions: 'coordinate the work',
      model,
      ...(shared?.memoryFactory
        ? { memory: shared.memoryFactory }
        : memory
          ? { memory: shared?.dynamicMemory ? () => memory : memory }
          : {}),
    });
    const quietAgent = new Agent({ id: 'quiet-agent', name: 'quiet-agent', instructions: 'answer briefly', model });
    const storage = new InMemoryHarness({
      db: shared?.db ?? new InMemoryDB(),
      ...(grant ? { terminalHandoff: { enabled: true }, sessionRecordProjection: { enabled: true } } : {}),
    });
    const childAgent = new MockAgent({ id: 'child-agent', defaultOutput: { terminalToolResult: completedReport } });
    const harness = new Harness({
      agents: { 'parent-agent': parentAgent, 'quiet-agent': quietAgent, 'child-agent': childAgent } as any,
      modes: [
        { id: 'default', agentId: 'parent-agent' },
        { id: 'worker-mode', agentId: 'child-agent' },
        { id: 'quiet-mode', agentId: 'quiet-agent', harnessBuiltins: 'exclude' },
      ],
      defaultModeId: 'default',
      sessions: grant
        ? { storage, terminalHandoff: { finalizer: messageFinalizer, wakeFinalizer } as never }
        : { storage },
      subagents: {
        maxDepth: 2,
        onDelegatedSettlement: 'wake',
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
    const releases: Array<() => void> = [];
    /** Hold the provider call with this index until the returned release runs. */
    const holdCall = (call: number) => {
      let release!: () => void;
      holds.set(call, new Promise<void>(r => (release = r)));
      releases.push(() => release());
      return () => release();
    };
    /** Release every held provider call, so a failed assertion cannot park shutdown. */
    const releaseAll = () => {
      for (const release of releases) release();
    };
    return { harness, storage, agents: [parentAgent, quietAgent], childAgent, prompts, holdCall, releaseAll };
  }

  function noticeCount(prompt: string): number {
    return prompt.split(NOTICE).length - 1;
  }

  type Row = {
    /** `parent-agent` (the delegating mode) has memory. */
    memory: boolean;
    /** The parent switches to the memory-less `quiet-mode` before the settlement. */
    switchToQuietMode: boolean;
    /** The caller turn that takes the slot runs `quiet-mode` through a per-turn override. */
    callerQuietMode: boolean;
    /** A Stop lands with the caller turn during the wake's context build. */
    stop: boolean;
    /**
     * Where the notice text must land (PF-7679: in exactly one provider prompt,
     * or none after a Stop). `r1-loads-row`: R1 dispatched after the reminder
     * row was saved and loaded it from history, so the wake is not sent.
     * `wake-after-r1`: the claimed wake re-owes the notice, waits for R1 to end,
     * and delivers it in its own run.
     */
    expected: 'r1-loads-row' | 'wake-after-r1' | 'nowhere';
  };

  it.each([
    [
      'active mode memory: R1 loads the row, no wake',
      { memory: true, switchToQuietMode: false, callerQuietMode: false, stop: false, expected: 'r1-loads-row' },
    ],
    [
      'no memory: the wake delivers after R1',
      { memory: false, switchToQuietMode: false, callerQuietMode: false, stop: false, expected: 'wake-after-r1' },
    ],
    [
      'only the delegating mode has memory: the wake delivers after R1',
      { memory: true, switchToQuietMode: true, callerQuietMode: false, stop: false, expected: 'wake-after-r1' },
    ],
    [
      'the caller turn runs a memory-less agent: the wake delivers after R1',
      { memory: true, switchToQuietMode: false, callerQuietMode: true, stop: false, expected: 'wake-after-r1' },
    ],
    // Differs from fork main by design: main delivered after the Stop.
    [
      'a Stop with the caller turn: the notice reaches no model input',
      { memory: false, switchToQuietMode: false, callerQuietMode: false, stop: true, expected: 'nowhere' },
    ],
  ] as Array<[string, Row]>)('%s', async (_label, row) => {
    const ctx = realParent(row.memory);
    try {
      const parent = await ctx.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const starts: Array<Record<string, unknown>> = [];
      const ended: string[] = [];
      parent.subscribe(event => {
        if (event.type === 'agent_start') starts.push({ ...event });
        if (event.type === 'agent_end') ended.push((event as { runId: string }).runId);
      });

      // R0 delegates the task and ends; the child is held.
      let releaseChild!: () => void;
      ctx.childAgent.enqueueRun({
        holdUntil: new Promise<void>(r => (releaseChild = r)),
        finishReason: 'tool-calls',
        terminalToolResult: completedReport,
      });
      const releaseR0 = ctx.holdCall(0);
      const r0 = parent.message({ content: 'go' });
      void r0.catch(() => {});
      await poll(() => ctx.prompts.length === 1);
      const tools = createPlanTaskTools(parent);
      const task = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content: 'background work' } as any, toolCtx)) as any;
      const taskId = task.taskId as string;
      await tools[TASK_DELEGATE_TOOL_ID]!.execute!({ taskId, agentType: 'worker' } as any, toolCtx);
      await poll(() => ctx.childAgent.streamCalls.length === 1);
      releaseR0();
      await r0;
      if (row.switchToQuietMode) await parent.switchMode({ mode: 'quiet-mode' });

      // Seam: when the claimed wake builds its turn context, start the user's
      // turn, wait until its run R1 is on its (held) provider call, and
      // optionally Stop. The Stop's invalidation is synchronous.
      const releaseR1 = ctx.holdCall(1);
      let wakeAdmitted = false;
      let held = false;
      let stopIssuedDuringBuild = false;
      let user: Promise<unknown> | undefined;
      let stopping: Promise<unknown> | undefined;
      // The first wake attempt, which the caller turn interleaves with.
      let firstWakeSettled = false;
      const inject = (parent as any)._injectSystemReminder.bind(parent);
      vi.spyOn(parent as any, '_injectSystemReminder').mockImplementation((...args: unknown[]) => {
        const first = !wakeAdmitted;
        wakeAdmitted = true;
        const attempt = inject(...args) as Promise<unknown>;
        if (first) {
          void attempt.then(
            () => (firstWakeSettled = true),
            () => (firstWakeSettled = true),
          );
        }
        return attempt;
      });
      const build = (parent as any)._buildTurnContextAndInstructions.bind(parent);
      vi.spyOn(parent as any, '_buildTurnContextAndInstructions').mockImplementation(async (...args: unknown[]) => {
        // The user's own message() reaches this seam, so mark the hold taken first.
        if (wakeAdmitted && !held) {
          held = true;
          user = parent.message({
            content: 'what happened?',
            ...(row.callerQuietMode ? { mode: 'quiet-mode' } : {}),
          });
          void user.catch(() => {});
          await poll(() => ctx.prompts.length === 2);
          if (row.stop) {
            stopping = parent.abortActiveWork({ reason: 'user_requested' });
            void stopping.catch(() => {});
            stopIssuedDuringBuild = true;
          }
        }
        return build(...args);
      });
      // Record the wake's dispatch and whether R1 was still on its held provider call.
      let r1Released = false;
      const reminderDispatches: Array<{ callerRunActive: boolean }> = [];
      const reminderRunIds: string[] = [];
      for (const agent of ctx.agents) {
        const sendSignal = agent.sendSignal.bind(agent);
        vi.spyOn(agent, 'sendSignal').mockImplementation(((signal: { type?: string }, opts: unknown) => {
          if (signal?.type !== 'system-reminder') return sendSignal(signal as never, opts as never);
          reminderDispatches.push({ callerRunActive: ctx.prompts.length === 2 && !r1Released });
          const result = sendSignal(signal as never, opts as never);
          reminderRunIds.push((result as { runId: string }).runId);
          return result;
        }) as never);
      }

      releaseChild();
      await poll(() => user !== undefined);
      // The first wake attempt has ended (it never dispatches while R1 runs).
      await poll(() => firstWakeSettled);
      r1Released = true;
      releaseR1();
      await user!.catch(() => {});
      await stopping?.catch(() => {});
      await deliveryFinished(parent, taskId);
      // A wake signal queued behind a caller run on another agent starts its own
      // run once R1 clears the thread; wait for every dispatched run to end.
      await poll(() => reminderRunIds.every(id => ended.includes(id)));

      // The interleave happened: R1's provider call began during the wake's context build.
      expect(held).toBe(true);
      expect(stopIssuedDuringBuild).toBe(row.stop);
      expect(ctx.prompts[1]).toContain('what happened?');
      // R1 ran the agent of the mode the caller turn used: only the quiet agent's
      // prompt carries its instructions.
      if (row.callerQuietMode || row.switchToQuietMode) expect(ctx.prompts[1]).toContain('answer briefly');
      else expect(ctx.prompts[1]).not.toContain('answer briefly');
      const withNotice = ctx.prompts.filter(p => p.includes(NOTICE));
      if (row.expected === 'nowhere') {
        expect(withNotice).toHaveLength(0);
        expect(reminderDispatches).toEqual([]);
      }
      if (row.expected === 'r1-loads-row') {
        // R1 dispatched after the save, so its history carries the row once.
        expect(withNotice).toHaveLength(1);
        expect(noticeCount(ctx.prompts[1]!)).toBe(1);
        expect(reminderDispatches).toEqual([]);
      }
      if (row.expected === 'wake-after-r1') {
        // R1 never sees it; the wake, sent after R1 ended, carries it once.
        expect(withNotice).toHaveLength(1);
        expect(noticeCount(withNotice[0]!)).toBe(1);
        expect(noticeCount(ctx.prompts[1]!)).toBe(0);
        expect(reminderDispatches).toEqual([{ callerRunActive: false }]);
        expect(ctx.prompts.indexOf(withNotice[0]!)).toBeGreaterThan(1);
      }
      // No caller start carries the wake label; a wake run sent after R1 does.
      const labelled = starts.filter(s => 'origin' in s || 'wakeNoticeId' in s);
      expect(labelled.filter(s => !reminderRunIds.includes(s.runId as string))).toEqual([]);
      expect(labelled).toHaveLength(row.expected === 'wake-after-r1' ? 1 : 0);
      expect(owedNotice(await planRow(parent, taskId))).toBeUndefined();
    } finally {
      ctx.releaseAll();
      await ctx.harness.shutdown();
    }
  });

  /**
   * PF-7679 exactly-once table. R0 delegates; the child settles at a chosen
   * point; every row asserts the notice text reaches exactly one provider prompt
   * (summed across restarts), no caller `agent_start` carries a wake label, and
   * nothing is owed at the end. Rows run with and without parent memory.
   */
  async function delegateFromR0(ctx: ReturnType<typeof realParent>, parent: Session) {
    let releaseChild!: () => void;
    ctx.childAgent.enqueueRun({
      holdUntil: new Promise<void>(r => (releaseChild = r)),
      finishReason: 'tool-calls',
      terminalToolResult: completedReport,
    });
    const releaseR0 = ctx.holdCall(0);
    const r0 = parent.message({ content: 'go' });
    void r0.catch(() => {});
    await poll(() => ctx.prompts.length === 1);
    const tools = createPlanTaskTools(parent);
    const task = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content: 'background work' } as any, toolCtx)) as any;
    const taskId = task.taskId as string;
    await tools[TASK_DELEGATE_TOOL_ID]!.execute!({ taskId, agentType: 'worker' } as any, toolCtx);
    await poll(() => ctx.childAgent.streamCalls.length === 1);
    const endR0 = async () => {
      releaseR0();
      await r0;
    };
    return { taskId, releaseChild, endR0 };
  }

  function watchStarts(parent: Session) {
    const starts: Array<Record<string, unknown>> = [];
    parent.subscribe(event => {
      if (event.type === 'agent_start') starts.push({ ...event });
    });
    return starts;
  }

  /** The delivery is parked in its unclaimed wait for quiescence. */
  async function waitingForQuiescence(parent: Session, taskId: string) {
    await poll(async () => (parent as any)._delegationNoticesInFlight.size === 1);
    await poll(async () => owedNotice(await planRow(parent, taskId)) !== undefined);
  }

  const total = (...prompts: string[][]) => prompts.flat().reduce((sum, p) => sum + noticeCount(p), 0);
  const MEMORY = [
    ['with memory', true],
    ['no memory', false],
  ] as const;

  it.each(MEMORY)('row 2: the child settles while R0 runs; one delivery after R0 (%s)', async (_l, memory) => {
    const ctx = realParent(memory);
    try {
      const parent = await ctx.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const starts = watchStarts(parent);
      const { taskId, releaseChild, endR0 } = await delegateFromR0(ctx, parent);
      releaseChild();
      await waitingForQuiescence(parent, taskId);
      // The interleave: R0 is still on its held provider call at the save.
      expect(ctx.prompts).toHaveLength(1);
      await endR0();
      await deliveryFinished(parent, taskId);
      expect(total(ctx.prompts)).toBe(1);
      expect(starts.filter(s => s.origin === 'wake')).toHaveLength(1);
      expect(owedNotice(await planRow(parent, taskId))).toBeUndefined();
    } finally {
      ctx.releaseAll();
      await ctx.harness.shutdown();
    }
  });

  // The provider prompt, not storage, proves where the notice landed: (a) the
  // claimed wake's own run, (b) a user turn admitted at the wake's entry, which
  // loads the saved row from history while the wake is dropped.
  it.each([
    ['claimed wake: the wake run prompt carries the notice', 'wake'],
    ['user turn admitted just before the wake: its prompt carries the notice', 'user-turn'],
  ] as const)('%s', async (_label, path) => {
    const ctx = realParent(true);
    try {
      const parent = await ctx.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const starts = watchStarts(parent);
      const { taskId, releaseChild, endR0 } = await delegateFromR0(ctx, parent);
      await endR0();
      let user: Promise<unknown> | undefined;
      if (path === 'user-turn') {
        const inject = (parent as any)._injectSystemReminder.bind(parent);
        vi.spyOn(parent as any, '_injectSystemReminder').mockImplementation((...args: unknown[]) => {
          if (user === undefined) {
            user = parent.message({ content: 'what happened?' });
            void user.catch(() => {});
          }
          return inject(...args);
        });
      }
      releaseChild();
      await deliveryFinished(parent, taskId);
      if (user !== undefined) await user;
      await parent.waitForIdle({ timeoutMs: 2000 });

      expect(ctx.prompts).toHaveLength(2);
      expect(noticeCount(ctx.prompts[1]!)).toBe(1);
      expect(total(ctx.prompts)).toBe(1);
      if (path === 'user-turn') {
        expect(user).toBeDefined();
        expect(ctx.prompts[1]).toContain('what happened?');
        expect(starts.filter(s => s.origin === 'wake')).toHaveLength(0);
      } else {
        expect(starts.filter(s => s.origin === 'wake')).toHaveLength(1);
      }
      expect(owedNotice(await planRow(parent, taskId))).toBeUndefined();
    } finally {
      ctx.releaseAll();
      await ctx.harness.shutdown();
    }
  });

  // Another runtime wins the thread lease after the local idle check: it holds
  // run R, so the runtime answers a grant-less claimed wake as its `ifActive`
  // says: folded into R (`deliver`) or refused (`discard`).
  it('a grant-less claimed wake that loses the thread to another runtime is not labelled and is retried', async () => {
    const ctx = realParent(false);
    try {
      const parent = await ctx.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const starts = watchStarts(parent);
      const { taskId, releaseChild, endR0 } = await delegateFromR0(ctx, parent);
      await endR0();
      const [parentAgent] = ctx.agents;
      const sendSignal = parentAgent!.sendSignal.bind(parentAgent);
      const lost: Array<{ ifActive: unknown }> = [];
      vi.spyOn(parentAgent!, 'sendSignal').mockImplementation(((signal: any, opts: any) => {
        if (signal?.type !== 'system-reminder' || lost.length > 0) return sendSignal(signal, opts);
        lost.push({ ifActive: opts?.ifActive?.behavior });
        const action = opts?.ifActive?.behavior === 'discard' ? 'discard' : 'deliver';
        return {
          runId: 'lost-wake-run',
          signal: { ...signal, id: signal.id ?? 'lost-signal' },
          accepted: Promise.resolve({ action, runId: action === 'deliver' ? 'foreign-run-r' : 'lost-wake-run' }),
        };
      }) as never);
      releaseChild();
      await poll(() => lost.length === 1);
      // The lost dispatch never announces a wake run.
      await new Promise(r => setTimeout(r, 50));
      expect(starts.filter(s => s.runId === 'lost-wake-run')).toEqual([]);
      await deliveryFinished(parent, taskId);
      await poll(() => total(ctx.prompts) > 0);

      expect(lost).toEqual([{ ifActive: 'discard' }]);
      expect(starts.filter(s => s.runId === 'lost-wake-run')).toEqual([]);
      // The refused attempt re-owed the notice; the retry woke and delivered it once.
      expect(total(ctx.prompts)).toBe(1);
      expect(starts.filter(s => s.origin === 'wake')).toHaveLength(1);
      expect(owedNotice(await planRow(parent, taskId))).toBeUndefined();
    } finally {
      ctx.releaseAll();
      await ctx.harness.shutdown({ drainTimeoutMs: 2000 }).catch(() => {});
    }
  }, 15_000);

  it('a claimed wake refused by a run this process cannot see retries with backoff, then delivers once', async () => {
    const ctx = realParent(false);
    try {
      const parent = await ctx.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const { taskId, releaseChild, endR0 } = await delegateFromR0(ctx, parent);
      await endR0();
      const [parentAgent] = ctx.agents;
      const sendSignal = parentAgent!.sendSignal.bind(parentAgent);
      // A foreign lease holder refuses every wake for this window.
      const windowMs = 600;
      let refusingUntil: number | undefined;
      let refused = 0;
      vi.spyOn(parentAgent!, 'sendSignal').mockImplementation(((signal: any, opts: any) => {
        if (signal?.type !== 'system-reminder') return sendSignal(signal, opts);
        refusingUntil ??= Date.now() + windowMs;
        if (Date.now() >= refusingUntil) return sendSignal(signal, opts);
        refused += 1;
        return {
          runId: `refused-wake-run-${refused}`,
          signal: { ...signal, id: signal.id ?? 'refused-signal' },
          accepted: Promise.resolve({ action: 'discard', runId: `refused-wake-run-${refused}` }),
        };
      }) as never);
      releaseChild();
      await poll(() => refused > 0);
      await deliveryFinished(parent, taskId);
      await poll(() => total(ctx.prompts) > 0);

      expect(refused).toBeLessThanOrEqual(windowMs / 250 + 2);
      expect(total(ctx.prompts)).toBe(1);
      expect(owedNotice(await planRow(parent, taskId))).toBeUndefined();
    } finally {
      ctx.releaseAll();
      await ctx.harness.shutdown({ drainTimeoutMs: 2000 }).catch(() => {});
    }
  }, 15_000);

  it('an abort while the wake claim write stalls releases the turn; the notice is still delivered once', async () => {
    const ctx = realParent(false);
    let releaseAck!: () => void;
    const ack = new Promise<void>(r => (releaseAck = r));
    try {
      const parent = await ctx.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      let claimEntered = false;
      const inject = (parent as any)._injectSystemReminder.bind(parent);
      vi.spyOn(parent as any, '_injectSystemReminder').mockImplementation(
        (content: unknown, opts: unknown, internal: any) => {
          const claimNotice = internal?.claimNotice as (() => Promise<boolean>) | undefined;
          if (claimNotice === undefined || claimEntered) return inject(content, opts, internal);
          return inject(content, opts, {
            ...internal,
            claimNotice: async () => {
              claimEntered = true;
              const won = await claimNotice();
              await ack;
              return won;
            },
          });
        },
      );
      const { taskId, releaseChild, endR0 } = await delegateFromR0(ctx, parent);
      await endR0();
      releaseChild();
      await poll(() => claimEntered);
      parent.abort();
      // The turn ends while the claim write is still unacknowledged.
      await poll(() => !parent.isRunning(), 1000);
      releaseAck();
      await poll(() => total(ctx.prompts) > 0, 5000);
      await deliveryFinished(parent, taskId);

      expect(total(ctx.prompts)).toBe(1);
      expect(owedNotice(await planRow(parent, taskId))).toBeUndefined();
    } finally {
      releaseAck();
      ctx.releaseAll();
      await ctx.harness.shutdown({ drainTimeoutMs: 2000 }).catch(() => {});
    }
  }, 15_000);

  it('F9.1: a claimed notice whose clear-claim write fails is marked dead; a retry takes it over and delivers once', async () => {
    const ctx = realParent(false);
    try {
      const parent = await ctx.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const { taskId, releaseChild, endR0 } = await delegateFromR0(ctx, parent);
      await endR0();
      const [parentAgent] = ctx.agents;
      const sendSignal = parentAgent!.sendSignal.bind(parentAgent);
      let refused = 0;
      vi.spyOn(parentAgent!, 'sendSignal').mockImplementation(((signal: any, opts: any) => {
        if (signal?.type !== 'system-reminder' || refused > 0) return sendSignal(signal, opts);
        refused += 1;
        return {
          runId: 'refused-wake-run',
          signal: { ...signal, id: signal.id ?? 'refused-signal' },
          accepted: Promise.resolve({ action: 'discard', runId: 'refused-wake-run' }),
        };
      }) as never);
      // The first plan-task write after the refusal is the restore: it fails once.
      const mutate = ctx.storage.mutatePlanTasksForSession.bind(ctx.storage);
      let failed = 0;
      vi.spyOn(ctx.storage, 'mutatePlanTasksForSession').mockImplementation(async (input: any) => {
        if (refused === 1 && failed === 0) {
          failed += 1;
          throw new Error('transient storage failure');
        }
        return mutate(input);
      });
      releaseChild();
      await poll(() => failed === 1);
      await deliveryFinished(parent, taskId);
      await new Promise(r => setTimeout(r, 300));

      expect(total(ctx.prompts)).toBe(1);
      expect(owedNotice(await planRow(parent, taskId))).toBeUndefined();
    } finally {
      ctx.releaseAll();
      await ctx.harness.shutdown({ drainTimeoutMs: 2000 }).catch(() => {});
    }
  }, 15_000);

  it('a caller run that loads the row between a failed attempt and its retry is the delivery; no wake follows', async () => {
    const ctx = realParent(true);
    try {
      const parent = await ctx.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const { taskId, releaseChild, endR0 } = await delegateFromR0(ctx, parent);
      await endR0();
      const starts = watchStarts(parent);
      const [parentAgent] = ctx.agents;
      const sendSignal = parentAgent!.sendSignal.bind(parentAgent);
      let thrown = false;
      vi.spyOn(parentAgent!, 'sendSignal').mockImplementation(((signal: any, opts: any) => {
        if (signal?.type !== 'system-reminder' || thrown) return sendSignal(signal, opts);
        thrown = true;
        throw new Error('untyped dispatch failure');
      }) as never);
      const outcomes = vi.spyOn(parent as any, '_settleDelegationNoticeOutcome');
      releaseChild();
      await poll(() => outcomes.mock.calls.length === 1);
      // Inside the 500 ms reconcile backoff: a caller turn loads the saved row.
      await parent.message({ content: 'what happened?' });
      await deliveryFinished(parent, taskId);
      await new Promise(r => setTimeout(r, 1_500));

      expect(total(ctx.prompts)).toBe(1);
      expect(noticeCount(ctx.prompts[1]!)).toBe(1);
      expect(starts.filter(s => s.origin === 'wake')).toHaveLength(0);
      expect(outcomes.mock.calls.map(call => call[0])).toEqual(['left_owed', 'history']);
      expect(owedNotice(await planRow(parent, taskId))).toBeUndefined();
      expect(hasHistoryLoadWatches()).toBe(false);
    } finally {
      ctx.releaseAll();
      await ctx.harness.shutdown({ drainTimeoutMs: 2000 }).catch(() => {});
    }
  }, 15_000);

  it.each(MEMORY)(
    'row 3: a caller turn begun before the save dispatches after it; one delivery (%s)',
    async (_l, memory) => {
      const ctx = realParent(memory);
      try {
        const parent = await ctx.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
        const starts = watchStarts(parent);
        const { taskId, releaseChild, endR0 } = await delegateFromR0(ctx, parent);
        await endR0();
        // The caller turn begins, and its context build is held across the settlement.
        let releaseBuild!: () => void;
        const buildGate = new Promise<void>(r => (releaseBuild = r));
        let buildEntered = false;
        const build = (parent as any)._buildTurnContextAndInstructions.bind(parent);
        vi.spyOn(parent as any, '_buildTurnContextAndInstructions').mockImplementationOnce(
          async (...args: unknown[]) => {
            buildEntered = true;
            await buildGate;
            return build(...args);
          },
        );
        const user = parent.message({ content: 'what happened?' });
        void user.catch(() => {});
        await poll(() => buildEntered);
        releaseChild();
        // With memory the row is saved; without, the delivery has nothing to save.
        await waitingForQuiescence(parent, taskId);
        releaseBuild();
        await user;
        await deliveryFinished(parent, taskId);
        expect(ctx.prompts[1]).toContain('what happened?');
        // With memory R1 loaded the row; without memory the wake follows R1.
        expect(noticeCount(ctx.prompts[1]!)).toBe(memory ? 1 : 0);
        expect(total(ctx.prompts)).toBe(1);
        expect(starts.filter(s => s.origin === 'wake')).toHaveLength(memory ? 0 : 1);
        expect(owedNotice(await planRow(parent, taskId))).toBeUndefined();
      } finally {
        ctx.releaseAll();
        await ctx.harness.shutdown();
      }
    },
  );

  it.each(MEMORY)('row 7: a queued turn waits at the settlement; one delivery (%s)', async (_l, memory) => {
    const ctx = realParent(memory);
    try {
      const parent = await ctx.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const starts = watchStarts(parent);
      const { taskId, releaseChild, endR0 } = await delegateFromR0(ctx, parent);
      const queued = parent.queue({ content: 'queued follow-up' });
      void Promise.resolve(queued).catch(() => {});
      await poll(() => ((parent as any)._record.pendingQueue ?? []).length === 1);
      releaseChild();
      await waitingForQuiescence(parent, taskId);
      await endR0();
      await deliveryFinished(parent, taskId);
      const queuedPrompt = ctx.prompts.find(p => p.includes('queued follow-up'));
      expect(queuedPrompt).toBeDefined();
      // The queued run dispatched after the save: with memory it loaded the row.
      expect(noticeCount(queuedPrompt!)).toBe(memory ? 1 : 0);
      expect(total(ctx.prompts)).toBe(1);
      expect(starts.filter(s => s.origin === 'wake')).toHaveLength(memory ? 0 : 1);
      expect(owedNotice(await planRow(parent, taskId))).toBeUndefined();
    } finally {
      ctx.releaseAll();
      await ctx.harness.shutdown();
    }
  });

  it.each(MEMORY)(
    'row 8: a close during the wait leaves the notice owed; reopen delivers once (%s)',
    async (_l, memory) => {
      const ctx = realParent(memory);
      try {
        const parent = await ctx.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
        const { taskId, releaseChild, endR0 } = await delegateFromR0(ctx, parent);
        releaseChild();
        await waitingForQuiescence(parent, taskId);
        const closing = parent.close();
        await endR0();
        await closing;
        await poll(() => (parent as any)._delegationNoticesInFlight.size === 0);
        // Unclaimed: still owed, and no model input carried it.
        expect(owedNotice(await planRow(parent, taskId))).toMatchObject({ mode: 'wake' });
        expect(total(ctx.prompts)).toBe(0);
        const reopened = await ctx.harness.session({ sessionId: parent.id, resourceId: 'u1' });
        await deliveryFinished(reopened, taskId);
        expect(total(ctx.prompts)).toBe(1);
        expect(owedNotice(await planRow(reopened, taskId))).toBeUndefined();
      } finally {
        ctx.releaseAll();
        await ctx.harness.shutdown();
      }
    },
  );

  it.each([
    ['fixed memory', false],
    ['function memory', true],
  ] as Array<[string, boolean]>)(
    'a queued turn that loads the saved row consumes the notice; no wake follows (%s)',
    async (_l, dynamicMemory) => {
      const ctx = realParent(true, { db: new InMemoryDB(), dynamicMemory });
      try {
        const parent = await ctx.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
        const starts = watchStarts(parent);
        const { taskId, releaseChild, endR0 } = await delegateFromR0(ctx, parent);
        const queued = parent.queue({ content: 'queued follow-up' });
        void Promise.resolve(queued).catch(() => {});
        await poll(() => ((parent as any)._record.pendingQueue ?? []).length === 1);
        releaseChild();
        await waitingForQuiescence(parent, taskId);
        // R1 (the queued turn) dispatches after the save and loads the row.
        await endR0();
        await deliveryFinished(parent, taskId);
        await new Promise(r => setTimeout(r, 200));

        expect(ctx.prompts).toHaveLength(2);
        expect(noticeCount(ctx.prompts[1]!)).toBe(1);
        expect(total(ctx.prompts)).toBe(1);
        expect(starts.filter(s => s.origin === 'wake')).toEqual([]);
      } finally {
        ctx.releaseAll();
        await ctx.harness.shutdown();
      }
    },
  );

  // Design D/E (Option A, P2-4): a run counts as delivered when it loaded the
  // row itself, whatever memory instance it resolved.
  it('a run whose memory factory returns a new instance per call loads the row; no wake follows', async () => {
    const store = new InMemoryStore();
    const ctx = realParent(true, { db: new InMemoryDB(), memoryFactory: () => new MockMemory({ storage: store }) });
    try {
      const parent = await ctx.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const starts = watchStarts(parent);
      const { taskId, releaseChild, endR0 } = await delegateFromR0(ctx, parent);
      const queued = parent.queue({ content: 'queued follow-up' });
      void Promise.resolve(queued).catch(() => {});
      await poll(() => ((parent as any)._record.pendingQueue ?? []).length === 1);
      releaseChild();
      await waitingForQuiescence(parent, taskId);
      await endR0();
      await deliveryFinished(parent, taskId);
      await new Promise(r => setTimeout(r, 200));

      expect(ctx.prompts).toHaveLength(2);
      expect(noticeCount(ctx.prompts[1]!)).toBe(1);
      expect(total(ctx.prompts)).toBe(1);
      expect(starts.filter(s => s.origin === 'wake')).toEqual([]);
    } finally {
      ctx.releaseAll();
      await ctx.harness.shutdown();
    }
  });

  // Option A covers any in-process run on the parent thread, not only Harness
  // turns. The direct stream waits on the thread reservation R0 holds, so it
  // is started before R0 ends and awaited after.
  it('a direct in-process agent.stream on the parent thread loads the row; no wake follows', async () => {
    const ctx = realParent(true, { db: new InMemoryDB() });
    try {
      const parent = await ctx.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const starts = watchStarts(parent);
      const outcomes = vi.spyOn(parent as any, '_settleDelegationNoticeOutcome');
      const { taskId, releaseChild, endR0 } = await delegateFromR0(ctx, parent);
      releaseChild();
      await waitingForQuiescence(parent, taskId);
      const [parentAgent] = ctx.agents;
      const direct = parentAgent!
        .stream('a direct question', { memory: { thread: parent.threadId, resource: 'u1' } })
        .then(output => output.consumeStream());
      void direct.catch(() => {});
      // The direct stream is parked on R0's thread reservation, not on a provider call.
      await new Promise(r => setTimeout(r, 100));
      expect(ctx.prompts).toHaveLength(1);
      await endR0();
      await direct;
      await deliveryFinished(parent, taskId);
      await new Promise(r => setTimeout(r, 200));

      expect(ctx.prompts).toHaveLength(2);
      expect(noticeCount(ctx.prompts[1]!)).toBe(1);
      expect(total(ctx.prompts)).toBe(1);
      expect(starts.filter(s => s.origin === 'wake')).toEqual([]);
      expect(outcomes.mock.calls.map(call => call[0])).toEqual(['history']);
    } finally {
      ctx.releaseAll();
      await ctx.harness.shutdown();
    }
  });

  // E.4 (P2-3): the structured `message({ sync: true, output })` path runs
  // `agent.generate`; its run loads the row like any other. The delivery is
  // parked before its quiescence check so the structured turn runs first.
  it('a structured sync message on the parent loads the row; no wake follows', async () => {
    const ctx = realParent(true, {
      db: new InMemoryDB(),
      reply: call => (call === 1 ? '{"answer":"42"}' : `reply ${call}`),
    });
    try {
      const parent = await ctx.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const starts = watchStarts(parent);
      const outcomes = vi.spyOn(parent as any, '_settleDelegationNoticeOutcome');
      let release!: () => void;
      const gate = new Promise<void>(r => (release = r));
      let parked = false;
      const quiescence = (parent as any)._awaitDelegationNoticeQuiescence.bind(parent);
      vi.spyOn(parent as any, '_awaitDelegationNoticeQuiescence').mockImplementation(async (...args: unknown[]) => {
        if (!parked) {
          parked = true;
          await gate;
        }
        return quiescence(...args);
      });
      const { taskId, releaseChild, endR0 } = await delegateFromR0(ctx, parent);
      releaseChild();
      await poll(() => parked);
      await endR0();
      const out = await parent.message({ content: 'compute', output: z.object({ answer: z.string() }), sync: true });
      release();
      await deliveryFinished(parent, taskId);
      await new Promise(r => setTimeout(r, 200));

      expect(out).toEqual({ answer: '42' });
      expect(ctx.prompts).toHaveLength(2);
      expect(noticeCount(ctx.prompts[1]!)).toBe(1);
      expect(total(ctx.prompts)).toBe(1);
      expect(starts.filter(s => s.origin === 'wake')).toEqual([]);
      expect(outcomes.mock.calls.map(call => call[0])).toEqual(['history']);
    } finally {
      ctx.releaseAll();
      await ctx.harness.shutdown();
    }
  });

  it('a history that excludes the row (lastMessages: 0) is not a delivery; the wake follows once', async () => {
    const ctx = realParent(true, { db: new InMemoryDB(), memory: new MockMemory({ options: { lastMessages: 0 } }) });
    try {
      const parent = await ctx.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const starts = watchStarts(parent);
      const { taskId, releaseChild, endR0 } = await delegateFromR0(ctx, parent);
      const queued = parent.queue({ content: 'queued follow-up' });
      void Promise.resolve(queued).catch(() => {});
      await poll(() => ((parent as any)._record.pendingQueue ?? []).length === 1);
      releaseChild();
      await waitingForQuiescence(parent, taskId);
      await endR0();
      await deliveryFinished(parent, taskId);

      expect(ctx.prompts).toHaveLength(3);
      expect(noticeCount(ctx.prompts[1]!)).toBe(0);
      expect(total(ctx.prompts)).toBe(1);
      expect(starts.filter(s => s.origin === 'wake')).toHaveLength(1);
    } finally {
      ctx.releaseAll();
      await ctx.harness.shutdown();
    }
  });

  it('a run that loads history while the reminder row is being saved does not see it; the wake follows once', async () => {
    const gated = gatedMemory();
    const ctx = realParent(true, { db: new InMemoryDB(), memory: gated.memory });
    try {
      const parent = await ctx.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const starts = watchStarts(parent);
      const { taskId, releaseChild, endR0 } = await delegateFromR0(ctx, parent);
      const queued = parent.queue({ content: 'queued follow-up' });
      void Promise.resolve(queued).catch(() => {});
      await poll(() => ((parent as any)._record.pendingQueue ?? []).length === 1);
      // R1 loads history and reaches its provider call while the save is held.
      const releaseR1 = ctx.holdCall(1);
      releaseChild();
      await gated.entered;
      await endR0();
      await poll(() => ctx.prompts.length === 2);
      gated.release();
      await poll(() => gated.written());
      releaseR1();
      await deliveryFinished(parent, taskId);

      expect(noticeCount(ctx.prompts[1]!)).toBe(0);
      expect(ctx.prompts).toHaveLength(3);
      expect(total(ctx.prompts)).toBe(1);
      expect(starts.filter(s => s.origin === 'wake')).toHaveLength(1);
    } finally {
      gated.release();
      ctx.releaseAll();
      await ctx.harness.shutdown();
    }
  });

  it('row 8d: a close while a fresh run that loaded the row is still running does not re-wake after reopen', async () => {
    const ctx = realParent(true);
    try {
      const parent = await ctx.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const { taskId, releaseChild, endR0 } = await delegateFromR0(ctx, parent);
      const queued = parent.queue({ content: 'queued follow-up' });
      void Promise.resolve(queued).catch(() => {});
      await poll(() => ((parent as any)._record.pendingQueue ?? []).length === 1);
      releaseChild();
      await waitingForQuiescence(parent, taskId);
      // R1 (the queued turn) dispatches after the save and loads the row; hold it.
      const releaseR1 = ctx.holdCall(1);
      await endR0();
      await poll(() => ctx.prompts.length === 2);
      expect(noticeCount(ctx.prompts[1]!)).toBe(1);
      const closing = parent.close();
      releaseR1();
      await closing;
      await poll(() => (parent as any)._delegationNoticesInFlight.size === 0);
      const reopened = await ctx.harness.session({ sessionId: parent.id, resourceId: 'u1' });
      await poll(async () => owedNotice(await planRow(reopened, taskId)) === undefined);
      await poll(() => (reopened as any)._delegationNoticesInFlight.size === 0);
      await reopened.waitForIdle({ timeoutMs: 2000 });
      expect(total(ctx.prompts)).toBe(1);
    } finally {
      ctx.releaseAll();
      await ctx.harness.shutdown();
    }
  });

  it.each(MEMORY)(
    'row 8c: a crash during the wait leaves the notice owed; the next owner delivers once (%s)',
    async (_l, memory) => {
      const db = new InMemoryDB();
      const sharedMemory = memory ? new MockMemory() : undefined;
      const a = realParent(memory, { db, memory: sharedMemory });
      const b = realParent(memory, { db, memory: sharedMemory });
      try {
        const parentA = await a.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
        const { taskId, releaseChild } = await delegateFromR0(a, parentA);
        releaseChild();
        await waitingForQuiescence(parentA, taskId);
        // R0 never ends on A: the process dies with the delivery still waiting.
        crashParent(db, parentA);
        expect(owedNotice(await planRow(parentA, taskId))).toMatchObject({ mode: 'wake' });
        const parentB = await b.harness.session({ sessionId: parentA.id, resourceId: 'u1' });
        await deliveryFinished(parentB, taskId);
        expect(total(a.prompts, b.prompts)).toBe(1);
        expect(owedNotice(await planRow(parentB, taskId))).toBeUndefined();
      } finally {
        a.releaseAll();
        b.releaseAll();
        await b.harness.shutdown();
        await a.harness.shutdown().catch(() => {});
      }
    },
  );

  it.each(MEMORY)(
    'row 9: a dispatch that throws after the claim re-owes the notice; the retry delivers once (%s)',
    async (_l, memory) => {
      const ctx = realParent(memory);
      try {
        const parent = await ctx.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
        const { taskId, releaseChild, endR0 } = await delegateFromR0(ctx, parent);
        await endR0();
        const [parentAgent] = ctx.agents;
        const sendSignal = parentAgent!.sendSignal.bind(parentAgent);
        let thrown = 0;
        vi.spyOn(parentAgent!, 'sendSignal').mockImplementation(((signal: { type?: string }, opts: unknown) => {
          if (signal?.type === 'system-reminder' && thrown === 0) {
            thrown += 1;
            throw new Error('runtime refused the dispatch');
          }
          return sendSignal(signal as never, opts as never);
        }) as never);
        releaseChild();
        await poll(() => thrown === 1);
        await deliveryFinished(parent, taskId);
        await poll(() => total(ctx.prompts) > 0);
        expect(total(ctx.prompts)).toBe(1);
        expect(owedNotice(await planRow(parent, taskId))).toBeUndefined();
      } finally {
        ctx.releaseAll();
        await ctx.harness.shutdown();
      }
    },
  );

  /**
   * PF-7679 × PF-7631: a wake grant given up after its admission (a lost slot,
   * a `discard` acceptance, a dispatch that throws, a Stop, a mode switch), or
   * orphaned by a crash between the admission and the claim, leaves a durable
   * reservation under the per-notice grant key. A later delivery of the same
   * notice, in this process or by the next owner, is refused that grant: it
   * must still deliver, without the grant, in a bounded number of attempts.
   * Every row runs with `wakeFinalizer` registered and parent memory (the grant
   * needs the dispatching agent's own memory to hold the row).
   */
  describe('a wake grant given up after its admission (wakeFinalizer registered)', () => {
    type GiveUp = 'slot-lost' | 'discard' | 'dispatch-throws' | 'stop' | 'mode-switch' | 'crash-before-claim';
    type Retry = 'same process' | 'next owner';

    function wakeAdmissions(db: InMemoryDB) {
      return [...db.harnessTerminalAdmissions.values()].filter(a => a.admissionId.startsWith('harness-wake:'));
    }
    function wakeIntents(db: InMemoryDB) {
      return [...db.harnessTerminalIntents.values()].filter(i => i.finalizerId === wakeFinalizer.id);
    }

    /** Hold the acknowledgement of the wake admission's dispatch-claim write (the write itself commits). */
    function holdWakeClaimAck(ctx: ReturnType<typeof realParent>) {
      const cas = ctx.storage.compareAndSwapSignalDispatch.bind(ctx.storage);
      let stamped!: () => void;
      const written = new Promise<void>(r => (stamped = r));
      let release!: () => void;
      const ack = new Promise<void>(r => (release = r));
      vi.spyOn(ctx.storage, 'compareAndSwapSignalDispatch').mockImplementation(async (input: any) => {
        const receipt = await cas(input);
        if (String(input.admissionId).startsWith('harness-wake:')) {
          stamped();
          await ack;
        }
        return receipt;
      });
      return { written, release };
    }

    it('an owner whose wake claim lapsed before its last write was acknowledged never dispatches the wake', async () => {
      const db = new InMemoryDB();
      const memory = new MockMemory();
      const a = realParent(true, { db, memory }, true);
      const b = realParent(true, { db, memory }, true);
      // The last write before the dispatch is the settlement notice claim: it
      // commits, but its acknowledgement reaches A only after the lapse.
      let written!: () => void;
      const claimWritten = new Promise<void>(r => (written = r));
      let releaseAck!: () => void;
      const ack = new Promise<void>(r => (releaseAck = r));
      const held = { written: claimWritten, release: () => releaseAck() };
      try {
        const parentA = await a.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
        const startsA = watchStarts(parentA);
        const inject = (parentA as any)._injectSystemReminder.bind(parentA);
        vi.spyOn(parentA as any, '_injectSystemReminder').mockImplementation(
          (content: unknown, opts: unknown, internal: any) => {
            const claimNotice = internal?.claimNotice as (() => Promise<boolean>) | undefined;
            if (claimNotice === undefined) return inject(content, opts, internal);
            return inject(content, opts, {
              ...internal,
              claimNotice: async () => {
                const won = await claimNotice();
                written();
                await ack;
                return won;
              },
            });
          },
        );
        const { releaseChild, endR0 } = await delegateFromR0(a, parentA);
        await endR0();
        releaseChild();
        await held.written;
        // The grant was admitted before the claim.
        expect(wakeAdmissions(db)).toHaveLength(1);
        // The claim and A's session lease lapse; B recovers the wake as aborted.
        vi.useFakeTimers({ toFake: ['Date'] });
        vi.setSystemTime(Date.now() + 31_000);
        await b.harness.session({ sessionId: parentA.id, resourceId: 'u1' });
        expect(wakeIntents(db).map(i => i.terminalResult.status)).toEqual(['aborted']);
        held.release();
        await poll(() => (parentA as any)._delegationNoticesInFlight.size === 0);
        await poll(() => !parentA.isRunning());

        expect(a.prompts).toHaveLength(1);
        expect(total(a.prompts)).toBe(0);
        expect(startsA.filter(s => s.origin === 'wake')).toEqual([]);
      } finally {
        vi.useRealTimers();
        held.release();
        a.releaseAll();
        b.releaseAll();
        await b.harness.shutdown().catch(() => {});
        await a.harness.shutdown().catch(() => {});
      }
    });

    it('a wake whose own claim lapsed before the dispatch re-owes the notice and delivers it once', async () => {
      const db = new InMemoryDB();
      const a = realParent(true, { db }, true);
      try {
        const parent = await a.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
        const starts = watchStarts(parent);
        // A slow claim write: the check right after the claim sees the wake's
        // dispatch claim lapsed. No other owner exists.
        let lapseNext = false;
        let lapsed = 0;
        const inject = (parent as any)._injectSystemReminder.bind(parent);
        vi.spyOn(parent as any, '_injectSystemReminder').mockImplementation(
          (content: unknown, opts: unknown, internal: any) => {
            const claimNotice = internal?.claimNotice as (() => Promise<boolean>) | undefined;
            if (claimNotice === undefined || lapsed > 0) return inject(content, opts, internal);
            return inject(content, opts, {
              ...internal,
              claimNotice: async () => {
                const won = await claimNotice();
                lapseNext = won && internal?.wakeSettlement !== undefined;
                return won;
              },
            });
          },
        );
        const stillHeld = (parent as any)._dispatchClaimStillHeld.bind(parent);
        vi.spyOn(parent as any, '_dispatchClaimStillHeld').mockImplementation((expiresAt: unknown) => {
          if (!lapseNext) return stillHeld(expiresAt);
          lapseNext = false;
          lapsed += 1;
          return false;
        });
        const { taskId, releaseChild, endR0 } = await delegateFromR0(a, parent);
        await endR0();
        releaseChild();
        await poll(() => lapsed === 1);
        await deliveryFinished(parent, taskId);
        await parent.waitForIdle({ timeoutMs: 2000 });

        expect(total(a.prompts)).toBe(1);
        expect(starts.filter(s => s.origin === 'wake')).toHaveLength(1);
        expect(owedNotice(await planRow(parent, taskId))).toBeUndefined();
      } finally {
        a.releaseAll();
        await a.harness.shutdown().catch(() => {});
      }
    });

    it('a Stop during a wake admission leaves no per-run grant state behind', async () => {
      const db = new InMemoryDB();
      const a = realParent(true, { db }, true);
      const held = holdWakeClaimAck(a);
      try {
        const parent = await a.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
        const { taskId, releaseChild, endR0 } = await delegateFromR0(a, parent);
        await endR0();
        releaseChild();
        await held.written;
        await parent.abortActiveWork({ reason: 'user_requested' });
        held.release();
        await poll(() => wakeAdmissions(db).length === 1 && wakeAdmissions(db)[0]!.status === 'cancelled', 3000);
        await poll(() => (parent as any)._wakeTerminalAdmissionsInFlight.size === 0, 3000);
        await deliveryFinished(parent, taskId);

        expect((parent as any)._terminalStagingAdmissions.size).toBe(0);
        expect((parent as any)._nativeTerminalRunIds.size).toBe(0);
        // A later turn runs normally and is not treated as the cancelled grant's run.
        await parent.message({ content: 'next' });
        expect((parent as any)._nativeTerminalRunIds.size).toBe(0);
        expect(a.prompts.at(-1)).toContain('next');
      } finally {
        held.release();
        a.releaseAll();
        await a.harness.shutdown().catch(() => {});
      }
    });

    // Design B, await point I3 (grant admission), with a registered wake
    // finalizer: [interleaving, notices delivered, first outcome]. A Stop leaves
    // no wake and the row reaches the next turn once; an abort, a close (the
    // reopened session) and a lost lease (the next holder) deliver once.
    it.each([
      ['stop', 0, 'left_owed'],
      ['abort', 1, 'left_owed'],
      ['close', 1, 'left_owed'],
      ['lease', 1, 'left_owed'],
    ] as const)('await point I3 grant admission: %s', async (interleaving, notices, firstOutcome) => {
      const db = new InMemoryDB();
      const memory = new MockMemory();
      const a = realParent(true, { db, memory }, true);
      const b = realParent(true, { db, memory }, true);
      let release = () => {};
      try {
        const parent = await a.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
        const outcomes = vi.spyOn(parent as any, '_settleDelegationNoticeOutcome');
        const { taskId, releaseChild, endR0 } = await delegateFromR0(a, parent);
        await endR0();
        const gate = new Promise<void>(r => (release = r));
        let reach!: () => void;
        const reached = new Promise<void>(r => (reach = r));
        let armed = true;
        const admit = a.storage.admitTerminalHandoff.bind(a.storage);
        vi.spyOn(a.storage, 'admitTerminalHandoff').mockImplementation(async (input: any) => {
          if (armed && JSON.stringify(input).includes('"harness-wake:')) {
            armed = false;
            reach();
            await gate;
          }
          return admit(input);
        });
        releaseChild();
        await reached;

        let holder: Session = parent;
        if (interleaving === 'stop') {
          const stop = parent.abortActiveWork({ reason: 'user_requested' });
          void stop.catch(() => {});
          await new Promise(r => setTimeout(r, 20));
          release();
          await stop.catch(() => {});
        } else if (interleaving === 'abort') {
          parent.abort();
          await new Promise(r => setTimeout(r, 20));
          release();
        } else if (interleaving === 'close') {
          const closing = parent.close();
          void closing.catch(() => {});
          await new Promise(r => setTimeout(r, 20));
          release();
          await closing.catch(() => {});
          holder = await a.harness.session({ sessionId: parent.id, resourceId: 'u1' });
        } else {
          crashParent(db, parent);
          const record = (parent as any)._record;
          (parent as any)._record = { ...record, leaseExpiresAt: Date.now() - 1 };
          release();
          await poll(() => (parent as any)._delegationNoticesInFlight.size === 0, 3000);
          holder = await b.harness.session({ sessionId: parent.id, resourceId: 'u1' });
        }
        if (holder !== parent) await (holder as any)._reconcileDelegationsOnHydrate(0);
        await deliveryFinished(holder, taskId);
        await new Promise(r => setTimeout(r, 100));

        expect(total(a.prompts, b.prompts)).toBe(notices);
        expect(outcomes.mock.calls[0]?.[0]).toBe(firstOutcome);
        expect(owedNotice(await planRow(holder, taskId))).toBeUndefined();
        if (interleaving === 'stop') {
          await holder.message({ content: 'next' });
          expect(total(a.prompts, b.prompts)).toBe(1);
        }
      } finally {
        release();
        a.releaseAll();
        b.releaseAll();
        await b.harness.shutdown().catch(() => {});
        await a.harness.shutdown().catch(() => {});
      }
    });

    it.each([
      ['slot-lost', 'same process'],
      ['slot-lost', 'next owner'],
      ['discard', 'same process'],
      ['discard', 'next owner'],
      ['dispatch-throws', 'same process'],
      ['dispatch-throws', 'next owner'],
      ['stop', 'same process'],
      ['stop', 'next owner'],
      ['mode-switch', 'same process'],
      ['mode-switch', 'next owner'],
      ['crash-before-claim', 'next owner'],
    ] as Array<[GiveUp, Retry]>)('%s, retried by the %s: one delivery, bounded attempts', async (giveUp, retry) => {
      const db = new InMemoryDB();
      const sharedMemory = new MockMemory();
      const a = realParent(true, { db, memory: sharedMemory }, true);
      const b = realParent(true, { db, memory: sharedMemory }, true);
      let releaseHangs!: () => void;
      const hang = new Promise<void>(r => (releaseHangs = r));
      let parentB: Session | undefined;
      try {
        const parentA = await a.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
        const startsA = watchStarts(parentA);
        const proto = Object.getPrototypeOf(parentA);
        let attempts = 0;
        const inject = proto._injectSystemReminder as (...args: unknown[]) => Promise<unknown>;
        const countSpy = vi.spyOn(proto, '_injectSystemReminder').mockImplementation(function (
          this: Session,
          ...args: unknown[]
        ) {
          attempts += 1;
          return inject.apply(this, args);
        });
        onTestFinished(() => countSpy.mockRestore());

        const { taskId, releaseChild, endR0 } = await delegateFromR0(a, parentA);
        await endR0();

        // The give-up seams, all on owner A's first wake attempt.
        let admitted = false;
        let gaveUp = false;
        let caller: Promise<unknown> | undefined;
        const admit = a.storage.admitTerminalHandoff.bind(a.storage);
        vi.spyOn(a.storage, 'admitTerminalHandoff').mockImplementation(async (input: any) => {
          if (!JSON.stringify(input).includes('"harness-wake:') || admitted) return admit(input);
          admitted = true;
          if (giveUp === 'slot-lost') {
            // A caller turn on the memory-less `quiet-mode` takes the slot during
            // the admission; it never reads the row, so the notice is re-owed.
            a.holdCall(1);
            caller = parentA.message({ content: 'what happened?', mode: 'quiet-mode' } as never);
            void caller.catch(() => {});
            await poll(() => a.prompts.length === 2);
          }
          if (giveUp === 'stop') void parentA.abortActiveWork({ reason: 'user_requested' });
          if (giveUp === 'mode-switch') await parentA.switchMode({ mode: 'quiet-mode' });
          const receipt = await admit(input);
          if (giveUp === 'crash-before-claim') {
            crashParent(db, parentA);
            gaveUp = true;
            await hang;
          }
          return receipt;
        });
        const [parentAgent] = a.agents;
        const sendSignal = parentAgent!.sendSignal.bind(parentAgent);
        vi.spyOn(parentAgent!, 'sendSignal').mockImplementation(((signal: any, opts: any) => {
          const reserved = typeof opts?.runId === 'string' && opts.runId.startsWith('harness-wake-');
          if (signal?.type === 'system-reminder' && reserved && !gaveUp) {
            if (giveUp === 'discard') {
              gaveUp = true;
              return { signal, runId: opts.runId, accepted: Promise.resolve({ action: 'discard' }) };
            }
            if (giveUp === 'dispatch-throws') {
              gaveUp = true;
              throw new Error('runtime refused the dispatch');
            }
          }
          return sendSignal(signal, opts);
        }) as never);
        if (giveUp === 'slot-lost' || giveUp === 'stop' || giveUp === 'mode-switch') {
          const mark = () => (gaveUp = true);
          void poll(() => admitted, 5000).then(mark, () => {});
        }
        // Next owner: owner A crashes after its give-up, before any retry runs
        // (its next wait for quiescence never returns).
        if (retry === 'next owner') {
          const quiescence = (parentA as any)._awaitDelegationNoticeQuiescence.bind(parentA);
          vi.spyOn(parentA as any, '_awaitDelegationNoticeQuiescence').mockImplementation(
            async (...args: unknown[]) => {
              if (gaveUp) await hang;
              return quiescence(...args);
            },
          );
        }

        releaseChild();
        await poll(() => admitted && gaveUp, 3000);

        let owner: Session = parentA;
        if (retry === 'next owner') {
          if (giveUp === 'slot-lost' || giveUp === 'discard' || giveUp === 'dispatch-throws') {
            // The given-up grant is cancelled before the crash.
            await poll(() => wakeAdmissions(db).some(w => w.status === 'cancelled'), 3000);
          }
          crashParent(db, parentA);
          parentB = await b.harness.session({ sessionId: parentA.id, resourceId: 'u1' });
          owner = parentB;
        } else if (caller !== undefined) {
          a.releaseAll();
          await caller.catch(() => {});
        }
        await deliveryFinished(owner, taskId);

        if (giveUp === 'stop') {
          // The Stop contract: no wake; the saved row reaches the next turn once.
          expect(total(a.prompts, b.prompts)).toBe(0);
          await owner.message({ content: 'next' });
        }
        expect(total(a.prompts, b.prompts)).toBe(1);
        expect(owedNotice(await planRow(owner, taskId))).toBeUndefined();
        expect(attempts).toBeLessThanOrEqual(3);
        // A delivery after a given-up grant runs without one, so it stages no
        // wake intent. Only the admission A orphaned by its crash is settled by
        // dispatch recovery, as an aborted wake (PF-7631).
        expect(wakeIntents(db).map(i => i.terminalResult.status)).toEqual(
          giveUp === 'crash-before-claim' ? ['aborted'] : [],
        );
        if (giveUp !== 'stop' && retry === 'same process') {
          expect(startsA.filter(s => s.origin === 'wake')).toHaveLength(1);
        }
        if (giveUp !== 'stop' && giveUp !== 'crash-before-claim') {
          await poll(() => wakeAdmissions(db).length === 1 && wakeAdmissions(db)[0]!.status === 'cancelled', 3000);
        }
      } finally {
        releaseHangs();
        a.releaseAll();
        b.releaseAll();
        await b.harness.shutdown();
        await a.harness.shutdown().catch(() => {});
      }
    });
  });
});

// PF-7679 claim lease and fence matrix (design B/C/D, F5, F8.1, F9.1). One parent
// on `MockAgent` with memory; `MockAgent` runs no input processors, so only a
// wake delivers the notice to the model here (`reminderCalls`). Each row parks
// the delivery at one await point, applies one interleaving there, and records
// the wake count across every owner and this delivery's first typed outcome.
describe('settlement notice claim lease and fence matrix (PF-7679)', () => {
  type AwaitPoint =
    | 'settlement-write'
    | 'reconcile-list'
    | 'owed-reread'
    | 'memory-resolve'
    | 'row-save'
    | 'quiescence'
    | 'subscription'
    | 'context-build'
    | 'claim-write'
    | 'claimed'
    | 'acceptance'
    | 'ack-write'
    | 'clear-write'
    | 'backoff';
  type Interleaving = 'stop' | 'abort' | 'close' | 'lease' | 'crash';
  type Observed = { wakes: number; firstOutcome: string | undefined };

  const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

  /** Expire this owner's lease in storage and in its own record (another owner may now take it). */
  function loseLease(db: InMemoryDB, parent: Session): void {
    crashParent(db, parent);
    const record = (parent as any)._record;
    (parent as any)._record = { ...record, leaseExpiresAt: Date.now() - 1 };
  }

  /** Discard the first settlement wake dispatch, as a run that took the slot does. */
  function discardFirstWake(agent: MockAgent): void {
    const sendSignal = agent.sendSignal.bind(agent);
    let discarded = false;
    vi.spyOn(agent, 'sendSignal').mockImplementation(((signal: any, opts: any) => {
      if (signal?.type !== 'system-reminder' || discarded) return sendSignal(signal, opts);
      discarded = true;
      return {
        runId: 'discarded-wake-run',
        signal: { ...signal, id: signal.id ?? 'discarded-signal' },
        accepted: Promise.resolve({ action: 'discard', runId: 'discarded-wake-run' }),
      };
    }) as never);
  }

  /** Park the delivery at `point` (its first pass only). */
  function park(parent: Session, agent: MockAgent, memory: MockMemory, point: AwaitPoint) {
    let release!: () => void;
    const gate = new Promise<void>(r => (release = r));
    let reach!: () => void;
    const reached = new Promise<void>(r => (reach = r));
    let armed = true;
    const take = () => {
      if (!armed) return false;
      armed = false;
      reach();
      return true;
    };
    const before = (name: string) => {
      const original = (parent as any)[name].bind(parent);
      vi.spyOn(parent as any, name).mockImplementation(async (...args: unknown[]) => {
        if (take()) await gate;
        return original(...args);
      });
    };
    switch (point) {
      case 'owed-reread':
        before('_delegationNoticeIsOwed');
        break;
      case 'memory-resolve':
        before('_resolveDelegationNoticeMemory');
        break;
      case 'subscription':
        before('_ensureThreadSubscription');
        break;
      case 'context-build':
        before('_buildTurnContextAndInstructions');
        break;
      case 'row-save': {
        const save = memory.saveMessages.bind(memory);
        vi.spyOn(memory, 'saveMessages').mockImplementation(async (args: any) => {
          if (args.messages.some((m: { id?: string }) => m.id?.startsWith('harness-delegation-notice-')) && take()) {
            await gate;
          }
          return save(args);
        });
        break;
      }
      case 'quiescence':
        // The parent turn is held by the caller; its end is the release.
        break;
      case 'settlement-write': {
        // R1: the child settlement's plan-task write that makes the notice owed.
        const storage = parent._internalStorage;
        const mutate = storage.mutatePlanTasksForSession.bind(storage);
        vi.spyOn(storage, 'mutatePlanTasksForSession').mockImplementation(async (input: any) => {
          const ops = JSON.stringify(input.ops);
          if (ops.includes('"notice":{') && !ops.includes('"claim":{') && take()) await gate;
          return mutate(input);
        });
        break;
      }
      case 'reconcile-list': {
        // R2: the reconcile pass's plan-task listing (armed by `run`).
        const storage = parent._internalStorage;
        const list = storage.listPlanTasks.bind(storage);
        vi.spyOn(storage, 'listPlanTasks').mockImplementation(async (input: any) => {
          if (take()) await gate;
          return list(input);
        });
        break;
      }
      case 'claim-write': {
        const storage = parent._internalStorage;
        const mutate = storage.mutatePlanTasksForSession.bind(storage);
        vi.spyOn(storage, 'mutatePlanTasksForSession').mockImplementation(async (input: any) => {
          if (JSON.stringify(input.ops).includes('"claim":{') && take()) await gate;
          return mutate(input);
        });
        break;
      }
      case 'claimed': {
        const inject = (parent as any)._injectSystemReminder.bind(parent);
        vi.spyOn(parent as any, '_injectSystemReminder').mockImplementation(
          (content: unknown, opts: unknown, internal: any) => {
            const claimNotice = internal?.claimNotice as (() => Promise<boolean>) | undefined;
            if (claimNotice === undefined || !armed) return inject(content, opts, internal);
            return inject(content, opts, {
              ...internal,
              claimNotice: async () => {
                const won = await claimNotice();
                if (take()) await gate;
                return won;
              },
            });
          },
        );
        break;
      }
      case 'acceptance': {
        const sendSignal = agent.sendSignal.bind(agent);
        vi.spyOn(agent, 'sendSignal').mockImplementation(((signal: any, opts: any) => {
          const dispatched = sendSignal(signal, opts) as any;
          if (signal?.type !== 'system-reminder' || !take()) return dispatched;
          return { ...dispatched, accepted: gate.then(() => dispatched.accepted) };
        }) as never);
        break;
      }
      case 'ack-write':
      case 'clear-write':
      case 'backoff': {
        if (point !== 'ack-write') discardFirstWake(agent);
        const site = point === 'ack-write' ? 'dispatch' : 'restore';
        const withFence = (parent as any)._withNoticeFence.bind(parent);
        vi.spyOn(parent as any, '_withNoticeFence').mockImplementation(
          async (delivery: unknown, at: unknown, body: unknown) => {
            if (at !== site || !armed) return withFence(delivery, at, body);
            if (point !== 'backoff') {
              take();
              await gate;
              return withFence(delivery, at, body);
            }
            const result = await withFence(delivery, at, body);
            take();
            await gate;
            return result;
          },
        );
        break;
      }
    }
    return { reached, release };
  }

  async function run(point: AwaitPoint, interleaving: Interleaving): Promise<Observed> {
    const db = new InMemoryDB();
    const memory = new MockMemory();
    const a = buildHarness(db, 'wake', memory);
    let b: ReturnType<typeof buildHarness> | undefined;
    let release = () => {};
    let observed!: Observed;
    try {
      const parent = await a.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const { taskId, releaseChild, endParentTurn } = await delegateFromParentTurn(
        parent,
        a.parentAgent,
        a.childAgent,
        {
          keepParentTurn: point === 'quiescence',
        },
      );
      const outcomes = vi.spyOn(parent as any, '_settleDelegationNoticeOutcome');
      // R2: the settlement's own delivery start is held back, so the reconcile
      // pass parked at its listing is the one that delivers.
      let heldBack = false;
      if (point === 'reconcile-list') {
        const start = (parent as any)._startDelegationSettlementNotice.bind(parent);
        vi.spyOn(parent as any, '_startDelegationSettlementNotice').mockImplementation((...args: unknown[]) => {
          if (heldBack) return start(...args);
          heldBack = true;
        });
      }
      const parked = point === 'reconcile-list' ? undefined : park(parent, a.parentAgent, memory, point);
      releaseChild();
      let parkedAt = parked;
      if (point === 'reconcile-list') {
        await poll(() => heldBack);
        parkedAt = park(parent, a.parentAgent, memory, point);
        void (parent as any)._reconcileDelegationsOnHydrate(0);
      }
      release = point === 'quiescence' ? () => void endParentTurn().catch(() => {}) : parkedAt!.release;
      if (point === 'quiescence') {
        await poll(async () => (parent as any)._delegationNoticesInFlight.size === 1);
        await sleep(50);
      } else {
        await parkedAt!.reached;
      }

      let holder: Session = parent;
      if (interleaving === 'stop') {
        const stop = parent.abortActiveWork({ reason: 'user_requested' });
        void stop.catch(() => {});
        await sleep(20);
        release();
        await stop.catch(() => {});
      } else if (interleaving === 'abort') {
        parent.abort();
        await sleep(20);
        release();
      } else if (interleaving === 'close') {
        const closing = parent.close();
        void closing.catch(() => {});
        await sleep(20);
        release();
        await closing.catch(() => {});
        holder = await a.harness.session({ sessionId: parent.id, resourceId: 'u1' });
      } else {
        loseLease(db, parent);
        if (interleaving === 'lease') {
          release();
          await poll(() => (parent as any)._delegationNoticesInFlight.size === 0, 3000);
        }
        b = buildHarness(db, 'wake', new MockMemory());
        holder = await b.harness.session({ sessionId: parent.id, resourceId: 'u1' });
      }
      // A delivery left owed in this session is retried by its own reconcile timer.
      if (holder !== parent) await (holder as any)._reconcileDelegationsOnHydrate(0);
      await deliveryFinished(holder, taskId);
      await sleep(100);
      observed = {
        wakes: reminderCalls(a.parentAgent).length + (b ? reminderCalls(b.parentAgent).length : 0),
        firstOutcome: outcomes.mock.calls[0]?.[0] as string | undefined,
      };
      expect(owedNotice(await planRow(holder, taskId))).toBeUndefined();
    } finally {
      release();
      await b?.harness.shutdown({ drainTimeoutMs: 2000 }).catch(() => {});
      await a.harness.shutdown({ drainTimeoutMs: 2000 }).catch(() => {});
    }
    // Spec E.4: no history-load watch outlives the delivery or its session.
    expect(hasHistoryLoadWatches()).toBe(false);
    return observed;
  }

  // Design B, one table per await point: [interleaving, wakes, first outcome].
  // Stop is NOWAKE (except once the wake run started: that run is the
  // delivery); an abort (no epoch bump) is ONCE; a close leaves it owed and the
  // reopened session delivers ONCE; a lost lease leaves it to the next holder
  // (ONCE), or wakes twice when the wake was already dispatched (DUP, F5).
  // Design B R1/R2: NOWAKE / ONCE / OWED / HOLDER. In the R1 lease cell the
  // first owner never starts a delivery; only the new holder delivers.
  it.each([
    ['stop', 0, 'fenced'],
    ['abort', 1, 'wake'],
    ['close', 1, 'left_owed'],
    ['lease', 1, undefined],
  ] as const)('await point R1 settlement write: %s', async (interleaving, wakes, firstOutcome) => {
    expect(await run('settlement-write', interleaving)).toEqual({ wakes, firstOutcome });
  });

  it.each([
    ['stop', 0, 'history'],
    ['abort', 1, 'wake'],
    ['close', 1, 'left_owed'],
    ['lease', 1, 'left_owed'],
  ] as const)('await point R2 reconcile listing: %s', async (interleaving, wakes, firstOutcome) => {
    expect(await run('reconcile-list', interleaving)).toEqual({ wakes, firstOutcome });
  });

  it.each([
    ['stop', 0, 'fenced'],
    ['abort', 1, 'wake'],
    ['close', 1, 'left_owed'],
    ['lease', 1, 'left_owed'],
  ] as const)('await point D3 owed re-read: %s', async (interleaving, wakes, firstOutcome) => {
    expect(await run('owed-reread', interleaving)).toEqual({ wakes, firstOutcome });
  });

  it.each([
    ['stop', 0, 'fenced'],
    ['abort', 1, 'wake'],
    ['close', 1, 'left_owed'],
    ['lease', 1, 'left_owed'],
  ] as const)('await point D5 memory resolve: %s', async (interleaving, wakes, firstOutcome) => {
    expect(await run('memory-resolve', interleaving)).toEqual({ wakes, firstOutcome });
  });

  it.each([
    ['stop', 0, 'fenced'],
    ['abort', 1, 'wake'],
    ['close', 1, 'left_owed'],
    ['lease', 1, 'left_owed'],
  ] as const)('await point D6 row save: %s', async (interleaving, wakes, firstOutcome) => {
    expect(await run('row-save', interleaving)).toEqual({ wakes, firstOutcome });
  });

  it.each([
    ['stop', 0, 'fenced'],
    ['abort', 1, 'wake'],
    ['close', 1, 'left_owed'],
    ['lease', 1, 'left_owed'],
  ] as const)('await point D8 quiescence: %s', async (interleaving, wakes, firstOutcome) => {
    expect(await run('quiescence', interleaving)).toEqual({ wakes, firstOutcome });
  });

  it.each([
    ['stop', 0, 'fenced'],
    ['abort', 1, 'wake'],
    ['close', 1, 'left_owed'],
    ['lease', 1, 'left_owed'],
  ] as const)('await point I1 subscription: %s', async (interleaving, wakes, firstOutcome) => {
    expect(await run('subscription', interleaving)).toEqual({ wakes, firstOutcome });
  });

  it.each([
    ['stop', 0, 'left_owed'],
    ['abort', 1, 'left_owed'],
    ['close', 1, 'left_owed'],
    ['lease', 1, 'left_owed'],
  ] as const)('await point I2 context build: %s', async (interleaving, wakes, firstOutcome) => {
    expect(await run('context-build', interleaving)).toEqual({ wakes, firstOutcome });
  });

  it.each([
    ['stop', 0, 'left_owed'],
    ['abort', 1, 'left_owed'],
    // The wake turn was admitted before the close, which drains it.
    ['close', 1, 'wake'],
    ['lease', 1, 'left_owed'],
  ] as const)('await point D12/I5 claim write: %s', async (interleaving, wakes, firstOutcome) => {
    expect(await run('claim-write', interleaving)).toEqual({ wakes, firstOutcome });
  });

  it.each([
    ['stop', 1, 'lost'],
    ['abort', 1, 'lost'],
    ['close', 1, 'wake'],
    ['lease', 2, 'wake'],
  ] as const)('await point I6 acceptance: %s', async (interleaving, wakes, firstOutcome) => {
    expect(await run('acceptance', interleaving)).toEqual({ wakes, firstOutcome });
  });

  it.each([
    ['stop', 1, 'wake'],
    ['abort', 1, 'wake'],
    // The close releases the lease before the ack lands: the claim is left
    // and the reopened session takes it over (DUP-allowed, as a lost lease).
    // `MockAgent` folds that second wake into its still-registered run, so
    // this cell observes one provider call: it cannot see the disclosed
    // duplicate, only that the notice ends acknowledged.
    ['close', 1, 'left_owed'],
    ['lease', 2, 'wake'],
  ] as const)('await point P3/P4 delivered-ack write: %s', async (interleaving, wakes, firstOutcome) => {
    expect(await run('ack-write', interleaving)).toEqual({ wakes, firstOutcome });
  });

  it.each([
    ['stop', 0, 'fenced'],
    ['abort', 1, 'wake'],
    ['close', 1, 'left_owed'],
    ['lease', 1, 'left_owed'],
  ] as const)('await point D1/P5/P6 clear-claim write: %s', async (interleaving, wakes, firstOutcome) => {
    expect(await run('clear-write', interleaving)).toEqual({ wakes, firstOutcome });
  });

  it.each([
    ['stop', 0, 'fenced'],
    ['abort', 1, 'wake'],
    ['close', 1, 'left_owed'],
    ['lease', 1, 'left_owed'],
  ] as const)('await point D14/D16 backoff after a cleared claim: %s', async (interleaving, wakes, firstOutcome) => {
    expect(await run('backoff', interleaving)).toEqual({ wakes, firstOutcome });
  });

  // F5: [point, interleaving, wakes, first outcome]. A crashed claimer never
  // settles an outcome before the next holder delivers.
  it.each([
    ['before the claim', 'claim-write', 'crash', 1, undefined],
    ['before the claim', 'claim-write', 'lease', 1, 'left_owed'],
    ['before the claim', 'claim-write', 'stop', 0, 'left_owed'],
    ['claim written, before dispatch', 'claimed', 'crash', 1, undefined],
    ['claim written, before dispatch', 'claimed', 'lease', 1, 'left_owed'],
    ['claim written, before dispatch', 'claimed', 'stop', 0, 'left_owed'],
    ['dispatched, before the delivered-ack', 'ack-write', 'crash', 2, undefined],
    ['dispatched, before the delivered-ack', 'ack-write', 'lease', 2, 'wake'],
    ['dispatched, before the delivered-ack', 'ack-write', 'stop', 1, 'wake'],
    ['claim cleared, before the next loop', 'backoff', 'crash', 1, undefined],
    ['claim cleared, before the next loop', 'backoff', 'lease', 1, 'left_owed'],
    ['claim cleared, before the next loop', 'backoff', 'stop', 0, 'fenced'],
  ] as const)('F5 %s × %s (%s)', async (_label, point, interleaving, wakes, firstOutcome) => {
    expect(await run(point, interleaving)).toEqual({ wakes, firstOutcome });
  });

  it('F5 two takers race on one claim: exactly one wins on the version', async () => {
    const memory = new MockMemory();
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB(), 'wake', memory);
    try {
      const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const { taskId, childId, releaseChild } = await delegateFromParentTurn(parent, parentAgent, childAgent);
      let notice: { digest: string } | undefined;
      vi.spyOn(parent as any, '_startDelegationSettlementNotice').mockImplementation((n: unknown) => {
        notice = n as { digest: string };
      });
      releaseChild();
      await poll(() => notice !== undefined);
      const port = (parent as any)._planTaskPort();
      const key = { taskId, subagentSessionId: childId, digest: notice!.digest };
      const dead = { claimId: 'claim-dead', ownerId: 'crashed-owner', claimedAt: 1_000 };
      expect(await planTaskClaimDelegationSettlementNotice(port, { ...key, claim: dead, takeover: () => false })).toBe(
        'claimed',
      );
      const results = await Promise.allSettled(
        ['claim-x', 'claim-y'].map(claimId =>
          planTaskClaimDelegationSettlementNotice(port, {
            ...key,
            claim: { claimId, ownerId: 'taker', claimedAt: 2_000 },
            takeover: claim => claim.ownerId === 'crashed-owner',
          }),
        ),
      );
      const claimed = results.filter(r => r.status === 'fulfilled' && r.value === 'claimed');
      expect(claimed).toHaveLength(1);
      expect(results.filter(r => r.status === 'rejected')).toHaveLength(1);
      expect((owedNotice(await planRow(parent, taskId)) as any).claim.claimId).toMatch(/^claim-[xy]$/);
    } finally {
      await harness.shutdown();
    }
  });

  it('F8.1: a reconcile pass racing a live claim does not take it over; the notice is delivered once', async () => {
    const db = new InMemoryDB();
    const memory = new MockMemory();
    const { harness, parentAgent, childAgent } = buildHarness(db, 'wake', memory);
    let release = () => {};
    try {
      const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const { taskId, releaseChild } = await delegateFromParentTurn(parent, parentAgent, childAgent);
      const parked = park(parent, parentAgent, memory, 'claimed');
      release = parked.release;
      releaseChild();
      await parked.reached;
      expect(owedNotice(await planRow(parent, taskId))).toMatchObject({ claim: { ownerId: harness.ownerId } });
      // The live claim's id is held, so reconcile does not start a delivery for it.
      const inFlight = (parent as any)._delegationNoticesInFlight as Set<string>;
      const keys = [...inFlight];
      inFlight.clear();
      const start = vi.spyOn(parent as any, '_startDelegationSettlementNotice');
      await (parent as any)._reconcileDelegationsOnHydrate(0);
      expect(start).not.toHaveBeenCalled();
      for (const key of keys) inFlight.add(key);
      parked.release();
      await deliveryFinished(parent, taskId);
      expect(reminderCalls(parentAgent)).toHaveLength(1);
    } finally {
      release();
      await harness.shutdown({ drainTimeoutMs: 2000 }).catch(() => {});
    }
  });

  // F9.1: an untyped throw right after the claim write settles (in the claim
  // step itself, or from the dispatch) clears the claim and leaves the notice
  // owed; the same process retries and delivers it once, with no restart.
  it.each([
    ['thrown right after the claim write settles', 'claim'],
    ['thrown by the dispatch after the claim', 'dispatch'],
  ] as const)('F9.1: an untyped error %s is delivered once in the same process', async (_label, where) => {
    const memory = new MockMemory();
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB(), 'wake', memory);
    try {
      const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const { taskId, releaseChild } = await delegateFromParentTurn(parent, parentAgent, childAgent);
      const outcomes = vi.spyOn(parent as any, '_settleDelegationNoticeOutcome');
      let thrown = false;
      if (where === 'claim') {
        const inject = (parent as any)._injectSystemReminder.bind(parent);
        vi.spyOn(parent as any, '_injectSystemReminder').mockImplementation(
          (content: unknown, opts: unknown, internal: any) => {
            const claimNotice = internal?.claimNotice as (() => Promise<boolean>) | undefined;
            if (claimNotice === undefined || thrown) return inject(content, opts, internal);
            return inject(content, opts, {
              ...internal,
              claimNotice: async () => {
                await claimNotice();
                thrown = true;
                throw new Error('untyped failure after the claim');
              },
            });
          },
        );
      } else {
        const sendSignal = parentAgent.sendSignal.bind(parentAgent);
        vi.spyOn(parentAgent, 'sendSignal').mockImplementation(((signal: any, opts: any) => {
          if (signal?.type !== 'system-reminder' || thrown) return sendSignal(signal, opts);
          thrown = true;
          throw new Error('untyped dispatch failure');
        }) as never);
      }
      releaseChild();
      await poll(() => thrown);
      await deliveryFinished(parent, taskId);

      expect(reminderCalls(parentAgent)).toHaveLength(1);
      expect(outcomes.mock.calls.map(call => call[0])).toEqual(['left_owed', 'wake']);
      expect(owedNotice(await planRow(parent, taskId))).toBeUndefined();
    } finally {
      await harness.shutdown();
    }
  });

  /** Throw once from the first plan-task write that `when` selects, before it reaches storage. */
  function failWriteOnce(parent: Session, when: (ops: string) => boolean): () => boolean {
    const storage = parent._internalStorage;
    const mutate = storage.mutatePlanTasksForSession.bind(storage);
    let failed = false;
    vi.spyOn(storage, 'mutatePlanTasksForSession').mockImplementation(async (input: any) => {
      if (!failed && when(JSON.stringify(input.ops))) {
        failed = true;
        throw new Error('transient storage failure');
      }
      return mutate(input);
    });
    return () => failed;
  }

  it('a delivered-ack that fails once is retried under the live claim; the parent wakes once', async () => {
    const memory = new MockMemory();
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB(), 'wake', memory);
    try {
      const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const { taskId, releaseChild } = await delegateFromParentTurn(parent, parentAgent, childAgent);
      const outcomes = vi.spyOn(parent as any, '_settleDelegationNoticeOutcome');
      // Count wake dispatches: `MockAgent` folds a second wake into a run it
      // still holds, so its stream calls alone could miss a duplicate.
      const dispatches = vi.spyOn(parentAgent, 'sendSignal');
      const wakes = () => dispatches.mock.calls.filter(([signal]) => (signal as any)?.type === 'system-reminder');
      // The first plan-task write after the wake is dispatched is its ack.
      const failed = failWriteOnce(parent, () => reminderCalls(parentAgent).length === 1);
      releaseChild();
      await poll(failed);
      await deliveryFinished(parent, taskId);
      // Past the first reconcile retry (500 ms), which a dead claim would get.
      await sleep(1_500);

      expect(wakes()).toHaveLength(1);
      expect(reminderCalls(parentAgent)).toHaveLength(1);
      expect(outcomes.mock.calls.map(call => call[0])).toEqual(['wake']);
      expect(owedNotice(await planRow(parent, taskId))).toBeUndefined();
      expect(hasHistoryLoadWatches()).toBe(false);
    } finally {
      await harness.shutdown();
    }
  });

  it('a claim write that throws leaves the notice owed; the same process retries and wakes once', async () => {
    const memory = new MockMemory();
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB(), 'wake', memory);
    try {
      const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const { taskId, releaseChild } = await delegateFromParentTurn(parent, parentAgent, childAgent);
      const outcomes = vi.spyOn(parent as any, '_settleDelegationNoticeOutcome');
      const failed = failWriteOnce(parent, ops => ops.includes('"claim":{'));
      releaseChild();
      await poll(failed);
      // Nothing else runs reconcile here: only the delivery's own retry can wake.
      await poll(() => reminderCalls(parentAgent).length === 1, 3_000);
      await deliveryFinished(parent, taskId);
      await sleep(1_000);

      expect(reminderCalls(parentAgent)).toHaveLength(1);
      expect(outcomes.mock.calls.map(call => call[0])).toEqual(['wake']);
      expect(owedNotice(await planRow(parent, taskId))).toBeUndefined();
      expect(hasHistoryLoadWatches()).toBe(false);
    } finally {
      await harness.shutdown();
    }
  });

  it('F5 dead claimer: a Stop downgrade before the takeover keeps remind; no wake follows', async () => {
    const memory = new MockMemory();
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB(), 'wake', memory);
    try {
      const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const { taskId, childId, releaseChild } = await delegateFromParentTurn(parent, parentAgent, childAgent);
      let notice: { digest: string } | undefined;
      const start = vi.spyOn(parent as any, '_startDelegationSettlementNotice').mockImplementation((n: unknown) => {
        notice = n as { digest: string };
      });
      releaseChild();
      await poll(() => notice !== undefined);
      // A claim by this owner whose id is not live: its claimer died.
      const dead = { claimId: 'claim-dead', ownerId: harness.ownerId, claimedAt: 1_000 };
      const port = (parent as any)._planTaskPort();
      const key = { taskId, subagentSessionId: childId, digest: notice!.digest };
      expect(await planTaskClaimDelegationSettlementNotice(port, { ...key, claim: dead, takeover: () => false })).toBe(
        'claimed',
      );
      await parent.abortActiveWork({ reason: 'user_requested' });
      expect(owedNotice(await planRow(parent, taskId))).toEqual({ mode: 'remind', digest: key.digest, claim: dead });

      start.mockRestore();
      const outcomes = vi.spyOn(parent as any, '_settleDelegationNoticeOutcome');
      await (parent as any)._reconcileDelegationsOnHydrate(0);
      await deliveryFinished(parent, taskId);

      expect(reminderCalls(parentAgent)).toHaveLength(0);
      expect(outcomes.mock.calls[0]?.[0]).toBe('history');
      expect(owedNotice(await planRow(parent, taskId))).toBeUndefined();
    } finally {
      await harness.shutdown();
    }
  });
});
