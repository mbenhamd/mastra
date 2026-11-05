/**
 * PF-7631 — a settlement wake turn stages its own terminal intent.
 *
 * With `sessions.terminalHandoff.wakeFinalizer` registered, the turn that
 * `subagents.onDelegatedSettlement: 'wake'` starts is admitted under a
 * Harness-minted grant (`harness-wake:<wakeNoticeId>`, generation 1) and settles
 * through the same admission, staging, and commit path as an admitted message()
 * turn. Its intent is committed under the wake finalizer, so a consumer claims
 * it by finalizer id without being held behind (or holding) message intents.
 */

import { afterEach, describe, expect, it, vi } from 'vitest';

import { Agent } from '../../agent';
import { convertArrayToReadableStream, MockLanguageModelV2 } from '../../agent/__tests__/mock-model';
import { MockMemory } from '../../memory/mock';
import type { HarnessTerminalFinalizer, HarnessTerminalIntent } from '../../storage/domains/harness';
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

const LEASE_AND_DISPATCH_CLAIM_LAPSED_MS = 31_000;

const messageFinalizer: HarnessTerminalFinalizer = {
  id: 'doxa.chat',
  version: '2026-09-28',
  finalize: async ({ result }) => ({
    projectionKind: 'chat.summary',
    projectionId: result.runId,
    payload: { status: result.status, code: result.error?.code ?? null },
  }),
};

const wakeFinalizer: HarnessTerminalFinalizer = {
  id: 'doxa.chat-wake',
  version: '2026-11-04',
  finalize: async ({ result, fullOutput }) => ({
    projectionKind: 'chat.wake',
    projectionId: result.runId,
    payload: { status: result.status, text: fullOutput?.text ?? null },
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

const toolCtx = {
  abortSignal: new AbortController().signal,
  agent: { toolCallId: 'tc-deleg', runId: 'mock-run' },
  requestContext: { get: () => undefined },
} as any;

function buildHarness(
  db: InMemoryDB,
  terminalHandoff: Record<string, unknown> = { finalizer: messageFinalizer, wakeFinalizer },
) {
  const parentAgent = new MockAgent({ id: 'parent-agent', defaultOutput: { text: 'woken reply' } });
  // The wake grant is admitted only when the dispatching agent's own fixed
  // memory saved the reminder row, so the parent gets a fixed memory instance.
  parentAgent.__setMemory(new MockMemory());
  const childAgent = new MockAgent({ id: 'child-agent', defaultOutput: { terminalToolResult: completedReport } });
  const storage = new InMemoryHarness({
    db,
    terminalHandoff: { enabled: true },
    sessionRecordProjection: { enabled: true },
  });
  const harness = new Harness({
    agents: { 'parent-agent': parentAgent, 'child-agent': childAgent } as any,
    modes: [
      { id: 'default', agentId: 'parent-agent' },
      { id: 'worker-mode', agentId: 'child-agent' },
    ],
    defaultModeId: 'default',
    sessions: { storage, terminalHandoff: terminalHandoff as never },
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
  return { harness, storage, parentAgent, childAgent };
}

async function poll(pred: () => boolean | Promise<boolean>, timeoutMs = 3000): Promise<void> {
  const start = Date.now();
  for (;;) {
    if (await pred()) return;
    if (Date.now() - start > timeoutMs) throw new Error('poll timeout');
    await new Promise(r => setTimeout(r, 5));
  }
}

async function planRow(parent: Session, taskId: string) {
  const page = await parent._internalStorage.listPlanTasks({ sessionId: parent.id, limit: 100 });
  return page.tasks.find(t => t.taskId === taskId);
}

function reminderCalls(agent: MockAgent) {
  return agent.streamCalls.filter(call => (call.messages as { tagName?: string })?.tagName === 'system-reminder');
}

function intents(db: InMemoryDB, finalizerId: string): HarnessTerminalIntent[] {
  return [...db.harnessTerminalIntents.values()].filter(intent => intent.finalizerId === finalizerId);
}

function crashParent(db: InMemoryDB, parent: Session): void {
  for (const [key, r] of db.harnessSessions) {
    if (r.ownerId === (parent as any)._internalOwnerId) {
      db.harnessSessions.set(key, { ...r, ownerId: 'crashed', leaseExpiresAt: Date.now() - 1 });
    }
  }
}

/**
 * Run one admitted message() turn that delegates a held child task, end the
 * turn, then release the child so its settlement wakes the idle parent.
 */
async function delegateFromAdmittedTurn(parent: Session, parentAgent: MockAgent, childAgent: MockAgent) {
  let releaseChild!: () => void;
  childAgent.enqueueRun({
    holdUntil: new Promise<void>(r => (releaseChild = r)),
    finishReason: 'tool-calls',
    terminalToolResult: completedReport,
  });
  let releaseParent!: () => void;
  parentAgent.enqueueRun({
    holdUntil: new Promise<void>(r => (releaseParent = r)),
    finishReason: 'stop',
    text: 'user reply',
  });
  const turn = parent.message({
    content: 'go',
    admissionId: 'user-turn',
    executionAuthorityGrant: { key: 'usage-user-turn', generation: 1 },
    terminalAdmissionSeed: { v: 1 },
  } as never);
  await poll(() => parentAgent.streamCalls.length === 1);
  const tools = createPlanTaskTools(parent);
  const task = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content: 'background work' } as any, toolCtx)) as any;
  const delegated = (await tools[TASK_DELEGATE_TOOL_ID]!.execute!(
    { taskId: task.taskId, agentType: 'worker' } as any,
    toolCtx,
  )) as any;
  await poll(() => childAgent.streamCalls.length === 1);
  releaseParent();
  await turn;
  return { taskId: task.taskId as string, childId: delegated.subagentSessionId as string, releaseChild };
}

describe('settlement wake terminal intent (sessions.terminalHandoff.wakeFinalizer)', () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it('commits the wake intent under the reserved grant and wake finalizer; the message() intent is unchanged', async () => {
    const db = new InMemoryDB();
    const { harness, storage, parentAgent, childAgent } = buildHarness(db);
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const starts: Array<Record<string, unknown>> = [];
    const off = parent.subscribe(event => {
      if (event.type === 'agent_start') starts.push({ ...event });
    });
    const { taskId, childId, releaseChild } = await delegateFromAdmittedTurn(parent, parentAgent, childAgent);

    releaseChild();
    await poll(() => reminderCalls(parentAgent).length === 1);
    await poll(() => intents(db, wakeFinalizer.id).length === 1);
    await parent.waitForIdle({ timeoutMs: 3000 });
    off();

    const wakeStart = starts.find(start => start.origin === 'wake')!;
    const wakeNoticeId = wakeStart.wakeNoticeId as string;
    const [wake] = intents(db, wakeFinalizer.id);
    expect(wake).toMatchObject({
      admissionId: `harness-wake:${wakeNoticeId}`,
      executionGrant: { key: `harness-wake:${wakeNoticeId}`, generation: 1 },
      signalId: wakeNoticeId,
      runId: wakeStart.runId,
      finalizerVersion: wakeFinalizer.version,
      terminalResult: { status: 'completed' },
    });
    expect(wake!.projection.payload).toEqual({
      origin: 'wake',
      wakeNoticeId,
      runId: wakeStart.runId,
      taskId,
      subagentSessionId: childId,
      outcome: 'completed',
      reply: { status: 'completed', text: 'woken reply' },
    });

    // The caller's message() turn commits exactly as before, under its own grant.
    const messages = intents(db, messageFinalizer.id);
    expect(messages).toHaveLength(1);
    expect(messages[0]).toMatchObject({
      admissionId: 'user-turn',
      executionGrant: { key: 'usage-user-turn', generation: 1 },
      projection: { projectionKind: 'chat.summary', payload: { status: 'completed', code: null } },
    });

    // Each kind is claimable on its own while the other kind is held claimed.
    const wakeClaim = await storage.claimTerminalIntents({
      harnessName: 'default',
      consumerId: 'wake-consumer',
      limit: 10,
      finalizerIds: [wakeFinalizer.id],
    });
    expect(wakeClaim.intents.map(intent => intent.id)).toEqual([wake!.id]);
    const messageClaim = await storage.claimTerminalIntents({
      harnessName: 'default',
      consumerId: 'usage-consumer',
      limit: 10,
      finalizerIds: [messageFinalizer.id],
    });
    expect(messageClaim.intents.map(intent => intent.id)).toEqual([messages[0]!.id]);
    expect((await planRow(parent, taskId))?.status).toBe('completed');
    await harness.shutdown();
  });

  it('a crash between the wake run end and its intent write is settled once on restart without re-running', async () => {
    const db = new InMemoryDB();
    const a = buildHarness(db);
    // The wake run ends and stages its output, then the process dies before the
    // intent write: every wake commit fails on instance A.
    const commit = a.storage.commitTerminalHandoff.bind(a.storage);
    let refusedWakeCommits = 0;
    vi.spyOn(a.storage, 'commitTerminalHandoff').mockImplementation(async input => {
      if (JSON.stringify(input).includes('"harness-wake:')) {
        refusedWakeCommits += 1;
        throw new Error('storage down');
      }
      return commit(input);
    });
    const parentA = await a.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const { taskId, releaseChild } = await delegateFromAdmittedTurn(parentA, a.parentAgent, a.childAgent);

    releaseChild();
    await poll(() => reminderCalls(a.parentAgent).length === 1);
    await poll(() => refusedWakeCommits > 0);
    await parentA.waitForIdle({ timeoutMs: 3000 }).catch(() => {});
    expect(intents(db, wakeFinalizer.id)).toHaveLength(0);
    crashParent(db, parentA);
    await a.harness.shutdown().catch(() => {});

    // The owner's lease and the wake's dispatch claim lapse before adoption.
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(Date.now() + LEASE_AND_DISPATCH_CLAIM_LAPSED_MS);
    const b = buildHarness(db);
    const parentB = await b.harness.session({ sessionId: parentA.id, resourceId: 'u1' });
    await poll(() => intents(db, wakeFinalizer.id).length === 1);
    await parentB.waitForIdle({ timeoutMs: 3000 });

    const [wake] = intents(db, wakeFinalizer.id);
    expect(wake!.terminalResult.status).toBe('completed');
    expect(wake!.projection.payload).toMatchObject({
      origin: 'wake',
      taskId,
      outcome: 'completed',
      reply: { status: 'completed', text: 'woken reply' },
    });
    // The adopter recovered the staged output; it never re-ran the wake.
    expect(b.parentAgent.streamCalls).toHaveLength(0);

    // A further adoption finds the settled wake and writes no duplicate.
    crashParent(db, parentB);
    await b.harness.shutdown().catch(() => {});
    vi.setSystemTime(Date.now() + LEASE_AND_DISPATCH_CLAIM_LAPSED_MS);
    const c = buildHarness(db);
    const parentC = await c.harness.session({ sessionId: parentA.id, resourceId: 'u1' });
    await (parentC as any)._reconcileDelegationsOnHydrate(0);
    await parentC.waitForIdle({ timeoutMs: 3000 });
    expect(intents(db, wakeFinalizer.id)).toHaveLength(1);
    expect(c.parentAgent.streamCalls).toHaveLength(0);
    expect(reminderCalls(a.parentAgent)).toHaveLength(1);
    await c.harness.shutdown();
  });

  it('rejects caller-supplied identities in the reserved wake namespace', async () => {
    const { harness, parentAgent } = buildHarness(new InMemoryDB());
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    await expect(
      session.message({
        content: 'hi',
        admissionId: 'harness-wake:forged',
        executionAuthorityGrant: { key: 'usage-1', generation: 1 },
        terminalAdmissionSeed: { v: 1 },
      } as never),
    ).rejects.toThrow(/reserved wake namespace/);
    await expect(
      session.message({
        content: 'hi',
        admissionId: 'turn-1',
        executionAuthorityGrant: { key: 'harness-wake:forged', generation: 1 },
        terminalAdmissionSeed: { v: 1 },
      } as never),
    ).rejects.toThrow(/reserved wake namespace/);
    expect(parentAgent.streamCalls).toHaveLength(0);
    await harness.shutdown();
  });

  it('rejects a wake finalizer that reuses the message finalizer id', () => {
    expect(() =>
      buildHarness(new InMemoryDB(), {
        finalizer: messageFinalizer,
        wakeFinalizer: { ...wakeFinalizer, id: messageFinalizer.id },
      }),
    ).toThrow(/wakeFinalizer/);
  });
});

// The wake grant binds the wake to a reserved run. It is kept only in the one
// proven case: the wake still holds the idle slot at dispatch, and the
// dispatching agent's own fixed memory saved the row with no mode change since.
// Every other case, including every caller run that took the slot, is fork
// main's path: delivery and label as main, and no intent. A grant given up
// after its admission is cancelled: no terminal result, no intent.
//
// Dimensions under a registered wake finalizer: the delegating mode (`default`,
// `parent-agent`) vs the active mode at settlement (same, a per-mode agent
// `other-agent`, or `shared-mode` on the same `parent-agent`); memory on each
// side; a mode switch during the wake's context build or its grant admission;
// a caller run that takes the slot, and its own agent (none, the session's
// mode, or a per-turn `message({ mode })` override). Collapsed combinations:
// - both sides memory-less, any agents: no row is saved, so the path is rows
//   3/4 (memory-less parent) whatever the agents;
// - both sides with memory on different agents: resolution takes the active
//   mode's memory first, so the delegating side is never read: rows 7/8;
// - a shared agent with no memory: no row, rows 3/4;
// - a mode switch together with a caller run, or a caller in any other cell:
//   a caller that takes the slot always gives the grant up (rows 2, 8, 10,
//   14-16), and the switch alone already takes main's path (rows 11-13).
describe('PF-7631 wake grant allowlist (real Agent model input)', () => {
  const usage = { inputTokens: 1, outputTokens: 1, totalTokens: 2 };
  const NOTICE = 'settled with outcome';

  type ModeId = 'default' | 'other-mode' | 'shared-mode';
  type Row = {
    parentMemory: boolean;
    otherMemory: boolean;
    /** Active mode at settlement (switched to after R0, before the settlement). */
    active: ModeId;
    callerRun: boolean;
    /** The caller turn starts during the wake's context build (default) or grant admission. */
    callerDuring?: 'build' | 'admission';
    /** A per-turn `message({ mode })` override for the caller turn. */
    callerMode?: ModeId;
    /** A switch to `switchTo` during the wake's context build or grant admission. */
    switchDuring: 'none' | 'build' | 'admission';
    switchTo?: ModeId;
    grant: boolean;
    /**
     * The run that delivers the notice. PF-7679: a caller run that takes the
     * slot never receives the wake signal. `caller-loads-row`: the caller run
     * dispatched after the save on the agent whose memory holds the row, so it
     * read the notice from history and no wake is sent. `wake-after-caller`:
     * the claimed wake re-owes the notice and runs after the caller run ends.
     */
    delivery: 'wake-run' | 'caller-loads-row' | 'wake-after-caller';
    label: 'wake' | 'none';
  };

  function realParent(row: Row) {
    const prompts: string[] = [];
    const holds = new Map<number, Promise<void>>();
    const model = new MockLanguageModelV2({
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
            { type: 'text-delta', id: 't', delta: `reply ${call}` },
            { type: 'text-end', id: 't' },
            { type: 'finish', finishReason: 'stop', usage },
          ]),
        };
      },
    });
    const parentMemory = row.parentMemory ? new MockMemory() : undefined;
    const otherMemory = row.otherMemory ? new MockMemory() : undefined;
    const parentAgent = new Agent({
      id: 'parent-agent',
      name: 'parent-agent',
      instructions: 'coordinate the work',
      model,
      ...(parentMemory ? { memory: parentMemory } : {}),
    });
    const otherAgent = new Agent({
      id: 'other-agent',
      name: 'other-agent',
      instructions: 'answer briefly',
      model,
      ...(otherMemory ? { memory: otherMemory } : {}),
    });
    const childAgent = new MockAgent({ id: 'child-agent', defaultOutput: { terminalToolResult: completedReport } });
    const db = new InMemoryDB();
    const storage = new InMemoryHarness({
      db,
      terminalHandoff: { enabled: true },
      sessionRecordProjection: { enabled: true },
    });
    const harness = new Harness({
      agents: { 'parent-agent': parentAgent, 'other-agent': otherAgent, 'child-agent': childAgent } as any,
      modes: [
        { id: 'default', agentId: 'parent-agent' },
        { id: 'worker-mode', agentId: 'child-agent' },
        { id: 'other-mode', agentId: 'other-agent', harnessBuiltins: 'exclude' },
        { id: 'shared-mode', agentId: 'parent-agent' },
      ],
      defaultModeId: 'default',
      sessions: { storage, terminalHandoff: { finalizer: messageFinalizer, wakeFinalizer } as never },
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
    const holdCall = (call: number) => {
      let release!: () => void;
      holds.set(call, new Promise<void>(r => (release = r)));
      releases.push(() => release());
      return () => release();
    };
    const releaseAll = () => {
      for (const release of releases) release();
    };
    return { harness, db, storage, agents: [parentAgent, otherAgent], childAgent, prompts, holdCall, releaseAll };
  }

  function wakeEvidence(db: InMemoryDB) {
    return [...db.harnessMessageResultEvidence.values()].filter(
      row => typeof row.admissionId === 'string' && row.admissionId.startsWith('harness-wake:'),
    );
  }

  const base = { parentMemory: true, otherMemory: false, callerRun: false, switchDuring: 'none' } as const;
  it.each([
    // Same mode as the delegating mode.
    [
      '1 same mode, memory, no caller: GRANT, reserved run labelled',
      { ...base, active: 'default', grant: true, delivery: 'wake-run', label: 'wake' },
    ],
    [
      '2 same mode, memory, caller run: the caller run reads the row, no wake',
      { ...base, active: 'default', callerRun: true, grant: false, delivery: 'caller-loads-row', label: 'none' },
    ],
    [
      '3 same mode, memory-less, no caller: main, labelled as main',
      { ...base, parentMemory: false, active: 'default', grant: false, delivery: 'wake-run', label: 'wake' },
    ],
    [
      '4 same mode, memory-less, caller run: the wake runs after it, labelled',
      {
        ...base,
        parentMemory: false,
        active: 'default',
        callerRun: true,
        grant: false,
        delivery: 'wake-after-caller',
        label: 'wake',
      },
    ],
    // Active mode on another agent at settlement.
    [
      '5 W4c: row in the delegating memory, active memory-less, caller run: the wake runs after it',
      { ...base, active: 'other-mode', callerRun: true, grant: false, delivery: 'wake-after-caller', label: 'wake' },
    ],
    [
      '6 row in the delegating memory, active memory-less, no caller: main, labelled',
      { ...base, active: 'other-mode', grant: false, delivery: 'wake-run', label: 'wake' },
    ],
    [
      '7 delegating memory-less, active memory, no caller: GRANT (active agent saved the row)',
      {
        ...base,
        parentMemory: false,
        otherMemory: true,
        active: 'other-mode',
        grant: true,
        delivery: 'wake-run',
        label: 'wake',
      },
    ],
    [
      '8 delegating memory-less, active memory, caller run: the caller run reads the row, no wake',
      {
        ...base,
        parentMemory: false,
        otherMemory: true,
        active: 'other-mode',
        callerRun: true,
        grant: false,
        delivery: 'caller-loads-row',
        label: 'none',
      },
    ],
    // Active mode on the same agent.
    [
      '9 shared agent, memory, no caller: GRANT',
      { ...base, active: 'shared-mode', grant: true, delivery: 'wake-run', label: 'wake' },
    ],
    [
      '10 shared agent, memory, caller run: the caller run reads the row, no wake',
      { ...base, active: 'shared-mode', callerRun: true, grant: false, delivery: 'caller-loads-row', label: 'none' },
    ],
    // A mode switch during the wake's awaits.
    [
      '11 switch to a memory-less agent during the context build: main, labelled',
      {
        ...base,
        active: 'default',
        switchDuring: 'build',
        switchTo: 'other-mode',
        grant: false,
        delivery: 'wake-run',
        label: 'wake',
      },
    ],
    [
      '12 switch during the grant admission: cancelled, main, labelled',
      {
        ...base,
        active: 'default',
        switchDuring: 'admission',
        switchTo: 'other-mode',
        grant: false,
        delivery: 'wake-run',
        label: 'wake',
      },
    ],
    [
      '13 switch to a mode on the same agent during the build: main, labelled',
      {
        ...base,
        active: 'default',
        switchDuring: 'build',
        switchTo: 'shared-mode',
        grant: false,
        delivery: 'wake-run',
        label: 'wake',
      },
    ],
    // The caller run's own agent.
    [
      '14 per-turn override caller on a memory-less agent: GRANT, the wake runs after it',
      {
        ...base,
        active: 'default',
        callerRun: true,
        callerMode: 'other-mode',
        grant: true,
        delivery: 'wake-after-caller',
        label: 'wake',
      },
    ],
    [
      '15 per-turn override caller on the same agent: the caller run reads the row, no wake',
      {
        ...base,
        active: 'default',
        callerRun: true,
        callerMode: 'shared-mode',
        grant: false,
        delivery: 'caller-loads-row',
        label: 'none',
      },
    ],
    [
      '16 same-mode caller during the grant admission: cancelled, the caller run reads the row',
      {
        ...base,
        active: 'default',
        callerRun: true,
        callerDuring: 'admission',
        grant: false,
        delivery: 'caller-loads-row',
        label: 'none',
      },
    ],
  ] as Array<[string, Row]>)('%s', async (_label, row) => {
    const ctx = realParent(row);
    try {
      const parent = await ctx.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const starts: Array<Record<string, unknown>> = [];
      const ended: string[] = [];
      parent.subscribe(event => {
        if (event.type === 'agent_start') starts.push({ ...event });
        if (event.type === 'agent_end') ended.push((event as { runId: string }).runId);
      });

      // R0 (delegating mode `default`) delegates the task and ends; the child is held.
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
      await tools[TASK_DELEGATE_TOOL_ID]!.execute!({ taskId: task.taskId, agentType: 'worker' } as any, toolCtx);
      await poll(() => ctx.childAgent.streamCalls.length === 1);
      releaseR0();
      await r0;
      if (row.active !== 'default') await parent.switchMode({ mode: row.active });

      // Seams: during the wake's context build, start the caller turn (its run
      // R1 held on its provider call) or switch the mode; during the wake's
      // grant admission, switch the mode.
      const releaseR1 = row.callerRun ? ctx.holdCall(1) : () => {};
      let wakeAdmitted = false;
      let held = false;
      let switched = false;
      let user: Promise<unknown> | undefined;
      const startCaller = async () => {
        held = true;
        user = parent.message({ content: 'what happened?', ...(row.callerMode ? { mode: row.callerMode } : {}) });
        void user.catch(() => {});
        await poll(() => ctx.prompts.length === 2);
      };
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
        if (wakeAdmitted && !held && !switched) {
          if (row.callerRun && row.callerDuring !== 'admission') await startCaller();
          if (row.switchDuring === 'build') {
            await parent.switchMode({ mode: row.switchTo! });
            switched = true;
          }
        }
        return build(...args);
      });
      const admit = ctx.storage.admitTerminalHandoff.bind(ctx.storage);
      vi.spyOn(ctx.storage, 'admitTerminalHandoff').mockImplementation(async (input: any) => {
        if (JSON.stringify(input).includes('"harness-wake:')) {
          if (row.switchDuring === 'admission' && !switched) {
            await parent.switchMode({ mode: row.switchTo! });
            switched = true;
          }
          if (row.callerRun && row.callerDuring === 'admission' && !held) await startCaller();
        }
        return admit(input);
      });
      let r1Released = !row.callerRun;
      const dispatches: Array<{ callerRunActive: boolean; reservedRun: boolean }> = [];
      const dispatchedRunIds: string[] = [];
      for (const agent of ctx.agents) {
        const sendSignal = agent.sendSignal.bind(agent);
        vi.spyOn(agent, 'sendSignal').mockImplementation(((signal: { type?: string }, opts: any) => {
          if (signal?.type !== 'system-reminder') return sendSignal(signal as never, opts);
          dispatches.push({
            callerRunActive: row.callerRun && ctx.prompts.length === 2 && !r1Released,
            reservedRun: typeof opts?.runId === 'string' && opts.runId.startsWith('harness-wake-'),
          });
          const result = sendSignal(signal as never, opts);
          dispatchedRunIds.push((result as { runId: string }).runId);
          return result;
        }) as never);
      }

      releaseChild();
      await poll(() => wakeAdmitted);
      if (row.callerRun) await poll(() => user !== undefined);
      // The first wake attempt has dispatched or been refused (PF-7679: it never
      // dispatches while a caller run holds the slot).
      await poll(() => firstWakeSettled);
      r1Released = true;
      releaseR1();
      await user?.catch(() => {});
      await poll(() => (parent as any)._delegationNoticesInFlight.size === 0);
      // Delivery is asserted before the runs are required to have ended.
      const runsEnded = await poll(() => dispatchedRunIds.every(id => ended.includes(id))).then(
        () => true,
        () => false,
      );
      if (row.switchDuring !== 'none') expect(switched).toBe(true);
      if (row.callerRun) expect(held).toBe(true);

      // Delivery first: the notice reaches the run main delivers it to.
      // PF-7679: exactly one provider prompt carries the notice, once.
      const withNotice = ctx.prompts.filter(p => p.includes(NOTICE));
      expect(withNotice).toHaveLength(1);
      expect(withNotice[0]!.split(NOTICE)).toHaveLength(2);
      if (row.delivery === 'caller-loads-row') {
        // The caller run's first model input read the saved row from history.
        expect(ctx.prompts[1]).toContain('what happened?');
        expect(ctx.prompts[1]).toContain(NOTICE);
      } else if (row.delivery === 'wake-after-caller') {
        // The caller run never sees it; the wake, sent after it ended, does.
        expect(ctx.prompts[1]).toContain('what happened?');
        expect(ctx.prompts[1]).not.toContain(NOTICE);
        expect(ctx.prompts.indexOf(withNotice[0]!)).toBeGreaterThan(1);
      } else {
        // The wake's own run (the next provider call after R0) carries it.
        expect(ctx.prompts[1]).toContain(NOTICE);
        expect(ctx.prompts[1]).not.toContain('what happened?');
      }

      expect(runsEnded).toBe(true);
      await parent.waitForIdle({ timeoutMs: 3000 });
      expect((await planRow(parent, task.taskId))?.status).toBe('completed');

      // Grant: kept only in the allowlisted cells; a caller run gets main's call.
      const labelled = starts.filter(s => s.origin === 'wake');
      expect(dispatches.some(d => d.reservedRun)).toBe(row.grant);
      if (row.delivery === 'caller-loads-row') expect(dispatches).toEqual([]);
      if (row.delivery === 'wake-after-caller') {
        expect(dispatches).toEqual([{ callerRunActive: false, reservedRun: row.grant }]);
      }
      if (row.grant) await poll(() => intents(ctx.db, wakeFinalizer.id).length === 1);
      expect(intents(ctx.db, wakeFinalizer.id)).toHaveLength(row.grant ? 1 : 0);
      if (row.grant) {
        expect(intents(ctx.db, wakeFinalizer.id)[0]).toMatchObject({
          signalId: labelled[0]?.wakeNoticeId,
          runId: labelled[0]?.runId,
        });
      }

      // Label: as main outside the allowlist; the reserved run's own inside it.
      if (row.label === 'none') {
        expect(labelled).toEqual([]);
      } else {
        expect(labelled).toHaveLength(1);
        if (row.grant) expect(labelled[0]!.runId).toMatch(/^harness-wake-/);
        else expect(labelled[0]!.runId).not.toMatch(/^harness-wake-/);
      }

      // After close: a kept grant committed its one intent; a grant given up
      // after its admission is cancelled (no terminal result, no intent, its
      // reservation settled failed `harness.terminal_cancelled`); any other
      // wake left no wake state at all, as on fork main.
      await parent.close();
      const admissions = [...ctx.db.harnessTerminalAdmissions.values()].filter(a =>
        a.admissionId.startsWith('harness-wake:'),
      );
      expect(intents(ctx.db, wakeFinalizer.id)).toHaveLength(row.grant ? 1 : 0);
      for (const evidence of wakeEvidence(ctx.db)) expect(evidence.status).not.toBe('pending');
      const givenUp = row.switchDuring === 'admission' || (row.callerRun && row.callerDuring === 'admission');
      if (row.grant) {
        expect(admissions.map(a => a.status)).toEqual(['committed']);
      } else if (givenUp) {
        expect(admissions.map(a => a.status)).toEqual(['cancelled']);
        expect(wakeEvidence(ctx.db)).toHaveLength(1);
        expect(wakeEvidence(ctx.db)[0]).toMatchObject({
          status: 'failed',
          error: { code: 'harness.terminal_cancelled' },
        });
      } else {
        expect(admissions).toEqual([]);
        expect(wakeEvidence(ctx.db)).toEqual([]);
      }
    } finally {
      ctx.releaseAll();
      await ctx.harness.shutdown();
    }
  });
});
