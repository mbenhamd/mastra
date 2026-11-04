/**
 * PF-7472 — parent notice when a detached `task_delegate` child settles.
 *
 * `subagents.onDelegatedSettlement` decides whether the parent model hears about
 * a settlement: `'none'` (default) keeps today's poll-only behaviour, `'remind'`
 * persists a system reminder for the next parent turn, and `'wake'` starts a
 * follow-up parent turn. The owed notice is written with the fenced settlement,
 * so delivery survives a restart and happens at most once.
 */

import { describe, expect, it, vi } from 'vitest';

import { MockMemory } from '../../memory/mock';
import { InMemoryHarness } from '../../storage/domains/harness/inmemory';
import { InMemoryDB } from '../../storage/domains/inmemory-db';

import { MockAgent } from './__test-utils__';
import { Harness } from './harness';
import { createPlanTaskTools, TASK_ADD_TOOL_ID, TASK_DELEGATE_TOOL_ID } from './plan-task-tool';
import type { Session } from './session';
import {
  HARNESS_SUBAGENT_OUTCOME_REPORT_KIND,
  HARNESS_SUBAGENT_OUTCOME_REPORT_TOOL_ID,
} from './terminal-subagent-result';

type NoticeMode = 'remind' | 'wake' | 'none' | undefined;

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
  if (memory !== undefined) vi.spyOn(parentAgent, 'getMemory').mockResolvedValue(memory as never);
  const childAgent = new MockAgent({ id: 'child-agent', defaultOutput: { terminalToolResult: completedReport } });
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
function gatedMemory(): { memory: MockMemory; release: () => void; entered: Promise<void> } {
  const memory = new MockMemory();
  let release!: () => void;
  const gate = new Promise<void>(r => (release = r));
  let enter!: () => void;
  const entered = new Promise<void>(r => (enter = r));
  const save = memory.saveMessages.bind(memory);
  vi.spyOn(memory, 'saveMessages').mockImplementation(async (args: any) => {
    if (args.messages.some((m: { id?: string }) => m.id?.startsWith('harness-delegation-notice-'))) {
      enter();
      await gate;
    }
    return save(args);
  });
  return { memory, release, entered };
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

  it('a restart between the settlement commit and the notice delivers the owed wake exactly once', async () => {
    const db = new InMemoryDB();
    const a = buildHarness(db, 'wake');
    const parentA = await a.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    // The process dies after the fenced settlement write, before the reminder
    // row lands: every parent-thread write fails on instance A.
    vi.spyOn(a.parentAgent, 'getMemory').mockResolvedValue({
      saveMessages: () => Promise.reject(new Error('storage down')),
    } as never);
    const { taskId, releaseChild } = await delegateFromParentTurn(parentA, a.parentAgent, a.childAgent);

    releaseChild();
    await poll(async () => (await planRow(parentA, taskId))?.status === 'completed');
    await poll(() => (parentA as any)._delegationNoticesInFlight.size === 0);
    expect(owedNotice(await planRow(parentA, taskId))).toMatchObject({ mode: 'wake' });
    expect(reminderCalls(a.parentAgent)).toHaveLength(0);
    crashParent(db, parentA);
    await a.harness.shutdown().catch(() => {});

    const b = buildHarness(db, 'wake');
    const parentB = await b.harness.session({ sessionId: parentA.id, resourceId: 'u1' });
    await poll(() => reminderCalls(b.parentAgent).length === 1);
    await deliveryFinished(parentB, taskId);

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

  it('skips the wake when another parent turn began after the reminder was saved', async () => {
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
    expect(reminderCalls(parentAgent)).toHaveLength(0);
    expect(parentAgent.streamCalls).toHaveLength(2);
    await harness.shutdown();
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
    vi.spyOn(a.parentAgent, 'getMemory').mockResolvedValue({
      saveMessages: () => Promise.reject(new Error('storage down')),
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

      // Settlement and admission start in the same tick.
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

  it('a user turn admitted just before the claimed wake begins keeps the only turn; the wake is dropped', async () => {
    const memory = new MockMemory();
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB(), 'wake', memory);
    const releases: Array<() => void> = [];
    try {
      const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const { taskId, releaseChild } = await delegateFromParentTurn(parent, parentAgent, childAgent);
      releases.push(releaseChild);
      const turns = trackTurns(parent);
      // The notice is saved and claimed; the user's message lands before the
      // wake reaches its own turn admission.
      let user: ReturnType<typeof startHeldUserTurn> | undefined;
      let turnActiveBeforeAdmission: boolean | undefined;
      let turnActiveAtAdmission: boolean | undefined;
      const inject = (parent as any)._injectSystemReminder.bind(parent);
      vi.spyOn(parent as any, '_injectSystemReminder').mockImplementation((...args: unknown[]) => {
        if (user === undefined) {
          turnActiveBeforeAdmission = parent.isTurnActive();
          user = startHeldUserTurn(parent, parentAgent, 'what happened?');
          releases.push(user.release);
          turnActiveAtAdmission = parent.isTurnActive();
        }
        return inject(...args);
      });

      releaseChild();
      await poll(() => user !== undefined);
      await poll(async () => (parent as any)._delegationNoticesInFlight.size === 0);
      await user!.end();
      await deliveryFinished(parent, taskId);

      expect(turnActiveBeforeAdmission).toBe(false);
      expect(turnActiveAtAdmission).toBe(true);
      expect(reminderCalls(parentAgent)).toHaveLength(0);
      expect(turns.order).toEqual(['start', 'end']);
      expect(turns.maxActive()).toBe(1);
      // The user turn reads the persisted notice from history instead.
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
      // Hold Stop between the turn abort and the child cancellation.
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
