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
