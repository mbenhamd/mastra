/**
 * PF-7631 — settlement wake turn robustness.
 *
 * - Every public admission boundary refuses a caller identity in the reserved
 *   `harness-wake:` namespace before it reads or writes admission evidence.
 * - A wake turn's failed terminal commit is surfaced and re-driven with bounded
 *   backoff; when storage stays down the close is refused (typed) and the
 *   admission stays durable for the next owner.
 * - A Stop or eviction while the wake is being admitted or dispatched never
 *   leaks the admission: it ends terminal, nothing stays in flight, and the
 *   close succeeds.
 * - A wake notice is admitted at most once, and a revoked wake grant refuses
 *   dispatch with an `error` event instead of dropping the wake silently.
 */

import { afterEach, describe, expect, it, vi } from 'vitest';

import type { HarnessTerminalFinalizer, HarnessTerminalIntent } from '../../storage/domains/harness';
import { InMemoryHarness } from '../../storage/domains/harness/inmemory';
import { InMemoryDB } from '../../storage/domains/inmemory-db';

import { MockAgent } from './__test-utils__';
import { HarnessSessionLockedError } from './errors';
import { Harness } from './harness';
import { createPlanTaskTools, TASK_ADD_TOOL_ID, TASK_DELEGATE_TOOL_ID } from './plan-task-tool';
import type { Session } from './session';
import {
  HARNESS_SUBAGENT_OUTCOME_REPORT_KIND,
  HARNESS_SUBAGENT_OUTCOME_REPORT_TOOL_ID,
} from './terminal-subagent-result';

const DISPATCH_CLAIM_LAPSED_MS = 31_000;

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

function buildHarness(db: InMemoryDB) {
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

function reminderCalls(agent: MockAgent) {
  return agent.streamCalls.filter(call => (call.messages as { tagName?: string })?.tagName === 'system-reminder');
}

function wakeIntents(db: InMemoryDB): HarnessTerminalIntent[] {
  return [...db.harnessTerminalIntents.values()].filter(intent => intent.finalizerId === wakeFinalizer.id);
}

function wakeEvidence(db: InMemoryDB) {
  return [...db.harnessMessageResultEvidence.values()].filter(
    row => typeof row.admissionId === 'string' && row.admissionId.startsWith('harness-wake:'),
  );
}

function isWakeInput(input: unknown): boolean {
  return JSON.stringify(input).includes('"harness-wake:');
}

function wakeInFlight(session: Session): Set<string> {
  return (session as any)._wakeTerminalAdmissionsInFlight as Set<string>;
}

function errorEvents(session: Session) {
  const events: Array<Record<string, unknown>> = [];
  const off = session.subscribe(event => {
    if (event.type === 'error') events.push({ ...event });
  });
  return { events, off };
}

/** Hold the first wake-scoped call of a storage method until released. */
function holdFirstWakeCall(target: any, method: string) {
  const real = target[method].bind(target);
  let reached!: () => void;
  const reachedPromise = new Promise<void>(r => (reached = r));
  let release!: () => void;
  const released = new Promise<void>(r => (release = r));
  let held = false;
  vi.spyOn(target, method).mockImplementation(async (...args: unknown[]) => {
    if (!held && isWakeInput(args[0])) {
      held = true;
      reached();
      await released;
    }
    return real(...args);
  });
  return { reached: reachedPromise, release };
}

/** Hold the wake dispatch's `accepted` until released (the run itself starts). */
function holdWakeAccepted(agent: MockAgent) {
  const real = agent.sendSignal.bind(agent);
  let reached!: () => void;
  const reachedPromise = new Promise<void>(r => (reached = r));
  let release!: () => void;
  const released = new Promise<void>(r => (release = r));
  let held = false;
  vi.spyOn(agent, 'sendSignal').mockImplementation(((signal: any, target: any) => {
    const result = real(signal, target);
    if (held || typeof target?.runId !== 'string' || !target.runId.startsWith('harness-wake-')) return result;
    held = true;
    reached();
    const accepted = released.then(() => result.accepted);
    return new Proxy(result, { get: (t, p) => (p === 'accepted' ? accepted : Reflect.get(t, p)) });
  }) as never);
  return { reached: reachedPromise, release };
}

/**
 * Run one admitted message() turn that delegates a held child task, end the
 * turn, then return the child's release so its settlement wakes the idle parent.
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
  await tools[TASK_DELEGATE_TOOL_ID]!.execute!({ taskId: task.taskId, agentType: 'worker' } as any, toolCtx);
  await poll(() => childAgent.streamCalls.length === 1);
  releaseParent();
  await turn;
  return { releaseChild };
}

const WAKE = 'harness-wake:forged';

type Boundary = {
  path: string;
  field: RegExp;
  admit: (session: Session) => Promise<unknown>;
};

const boundaries: Boundary[] = [
  {
    path: 'message() admissionId',
    field: /message\(\)\.admissionId/,
    admit: s =>
      s.message({
        content: 'hi',
        admissionId: WAKE,
        executionAuthorityGrant: { key: 'usage-1', generation: 1 },
        terminalAdmissionSeed: { v: 1 },
      } as never),
  },
  {
    path: 'message() executionAuthorityGrant.key',
    field: /message\(\)\.executionAuthorityGrant\.key/,
    admit: s =>
      s.message({
        content: 'hi',
        admissionId: 'turn-1',
        executionAuthorityGrant: { key: WAKE, generation: 1 },
        terminalAdmissionSeed: { v: 1 },
      } as never),
  },
  {
    path: 'admitMessage() admissionId',
    field: /admitMessage\(\)\.admissionId/,
    admit: s => s.admitMessage({ content: 'hi', admissionId: WAKE } as never),
  },
  {
    path: 'admitMessage() executionAuthorityGrant.key',
    field: /admitMessage\(\)\.executionAuthorityGrant\.key/,
    admit: s =>
      s.admitMessage({
        content: 'hi',
        admissionId: 'turn-1',
        executionAuthorityGrant: { key: WAKE, generation: 1 },
        terminalAdmissionSeed: { v: 1 },
      } as never),
  },
  {
    path: 'signal() admissionId',
    field: /signal\(\)\.admissionId/,
    admit: s => s.signal({ content: 'hi', admissionId: WAKE }).then(handle => handle.result),
  },
  {
    path: 'queue() admissionId',
    field: /queue\(\)\.admissionId/,
    admit: s => s.queue({ content: 'hi', admissionId: WAKE }),
  },
  {
    path: 'admitQueue() admissionId',
    field: /admitQueue\(\)\.admissionId/,
    admit: s => s.admitQueue({ content: 'hi', admissionId: WAKE }),
  },
  {
    path: '_admitWakeupQueue (durable wakeup worker)',
    field: /admitQueue\(\)\.admissionId/,
    admit: s => s._admitWakeupQueue({ content: 'hi', admissionId: WAKE, attachments: [] }),
  },
  {
    path: '_admitChannelQueueTurn (channel ingress, queue delivery)',
    field: /admitQueue\(\)\.admissionId/,
    admit: s => s._admitChannelQueueTurn({ content: 'hi', admissionId: WAKE, requestContext: {} as never }),
  },
  {
    path: '_admitChannelSignalTurn (channel ingress, signal delivery)',
    field: /signal\(\)\.admissionId/,
    admit: s =>
      s._admitChannelSignalTurn({
        content: 'hi',
        admissionId: WAKE,
        requestContext: {} as never,
        expectedAdmissionHash: 'hash',
      }),
  },
];

describe('PF-7631 reserved wake namespace: every admission boundary', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it.each(boundaries)('$path refuses a harness-wake: identity before any admission evidence', async row => {
    const { harness, storage, parentAgent } = buildHarness(new InMemoryDB());
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const reads = vi.spyOn(storage, 'resolveOperationAdmissionEvidence');
    const writes = vi.spyOn(storage, 'writeMessageResultEvidence');
    const queueWrites = vi.spyOn(storage, 'saveSession');

    const refusal = await row.admit(session).then(
      () => undefined,
      (error: unknown) => error as Error,
    );
    expect(refusal?.message).toMatch(row.field);
    expect(refusal?.message).toMatch(/reserved wake namespace/);
    expect(reads).not.toHaveBeenCalled();
    expect(writes).not.toHaveBeenCalled();
    expect(queueWrites).not.toHaveBeenCalled();
    expect(parentAgent.streamCalls).toHaveLength(0);
    await harness.shutdown();
  });
});

describe('PF-7631 wake terminal commit re-drive (bounded)', () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it('a transient commit failure is retried in the turn: one intent, one error event per failed attempt, close succeeds', async () => {
    const db = new InMemoryDB();
    const { harness, storage, parentAgent, childAgent } = buildHarness(db);
    const commit = storage.commitTerminalHandoff.bind(storage);
    let wakeCommits = 0;
    vi.spyOn(storage, 'commitTerminalHandoff').mockImplementation(async input => {
      if (isWakeInput(input)) {
        wakeCommits += 1;
        if (wakeCommits <= 2) throw new Error('storage blip');
      }
      return commit(input);
    });
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const { events, off } = errorEvents(parent);
    const { releaseChild } = await delegateFromAdmittedTurn(parent, parentAgent, childAgent);

    releaseChild();
    await poll(() => wakeIntents(db).length === 1);
    await parent.waitForIdle({ timeoutMs: 3000 });
    off();

    expect(wakeCommits).toBe(3);
    expect(wakeIntents(db)).toHaveLength(1);
    expect(wakeIntents(db)[0]!.terminalResult.status).toBe('completed');
    const wakeErrors = events.filter(event => event.signalId === wakeIntents(db)[0]!.signalId);
    expect(wakeErrors).toHaveLength(2);
    expect(wakeInFlight(parent).size).toBe(0);
    await parent.close();
    expect(wakeIntents(db)).toHaveLength(1);
    expect(reminderCalls(parentAgent)).toHaveLength(1);
    await harness.shutdown();
  });

  it('a commit that never recovers stops at the cap, close is refused typed, and the next owner settles it once', async () => {
    const db = new InMemoryDB();
    const a = buildHarness(db);
    let wakeCommits = 0;
    const commit = a.storage.commitTerminalHandoff.bind(a.storage);
    vi.spyOn(a.storage, 'commitTerminalHandoff').mockImplementation(async input => {
      if (isWakeInput(input)) {
        wakeCommits += 1;
        throw new Error('storage down');
      }
      return commit(input);
    });
    const parent = await a.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const { events, off } = errorEvents(parent);
    const { releaseChild } = await delegateFromAdmittedTurn(parent, a.parentAgent, a.childAgent);

    releaseChild();
    await poll(() => reminderCalls(a.parentAgent).length === 1);
    await poll(() => wakeCommits >= 4 && !parent.isRunning());
    await new Promise(r => setTimeout(r, 1_200));
    expect(wakeCommits).toBe(4);
    expect(events).toHaveLength(4);

    const startedAt = Date.now();
    await expect(parent.close()).rejects.toBeInstanceOf(HarnessSessionLockedError);
    expect(Date.now() - startedAt).toBeLessThan(5_000);
    off();
    expect(events.length).toBeGreaterThan(4);
    expect(wakeIntents(db)).toHaveLength(0);
    expect(wakeEvidence(db).map(row => row.status)).toEqual(['pending']);

    // The next owner settles the durable admission from the staged output.
    for (const [key, r] of db.harnessSessions) {
      if (r.id === parent.id) db.harnessSessions.set(key, { ...r, ownerId: 'crashed', leaseExpiresAt: Date.now() - 1 });
    }
    await a.harness.shutdown().catch(() => {});
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(Date.now() + DISPATCH_CLAIM_LAPSED_MS);
    const b = buildHarness(db);
    await b.harness.closeSession({ sessionId: parent.id });
    expect(wakeIntents(db)).toHaveLength(1);
    expect(wakeIntents(db)[0]!.terminalResult.status).toBe('completed');
    expect(b.parentAgent.streamCalls).toHaveLength(0);
    await b.harness.shutdown();
  });
});

type Await = 'reservation' | 'terminal admission' | 'dispatch claim' | 'dispatch accepted';
type Interrupt = 'Stop' | 'eviction';

const awaits: Await[] = ['reservation', 'terminal admission', 'dispatch claim', 'dispatch accepted'];
const interruptRows: Array<{ interrupt: Interrupt; at: Await }> = (['Stop', 'eviction'] as const).flatMap(interrupt =>
  awaits.map(at => ({ interrupt, at })),
);

describe('PF-7631 wake admission interrupted at each await', () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it.each(interruptRows)('$interrupt during the $at await leaves nothing active or in flight', async row => {
    const db = new InMemoryDB();
    const { harness, storage, parentAgent, childAgent } = buildHarness(db);
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const { releaseChild } = await delegateFromAdmittedTurn(parent, parentAgent, childAgent);
    const gate =
      row.at === 'reservation'
        ? holdFirstWakeCall(storage, 'writeMessageResultEvidence')
        : row.at === 'terminal admission'
          ? holdFirstWakeCall(storage, 'admitTerminalHandoff')
          : row.at === 'dispatch claim'
            ? holdFirstWakeCall(storage, 'compareAndSwapSignalDispatch')
            : holdWakeAccepted(parentAgent);

    releaseChild();
    await gate.reached;
    if (row.interrupt === 'Stop') {
      await parent.abortActiveWork();
    } else {
      await harness._internalEvictLiveSessionLeaseLost(parent);
    }
    // The interrupt wins the race: the owned turn has ended while the call is held.
    await poll(() => !parent.isRunning());
    // The held call then lands after the interrupt.
    gate.release();
    await poll(() => wakeInFlight(parent).size === 0);

    if (row.interrupt === 'Stop') {
      await parent.waitForIdle({ timeoutMs: 3000 }).catch(() => {});
      await parent.close();
    } else {
      // The next resolution of the session settles it once any claim lapses.
      vi.useFakeTimers({ toFake: ['Date'] });
      vi.setSystemTime(Date.now() + DISPATCH_CLAIM_LAPSED_MS);
      await harness.closeSession({ sessionId: parent.id });
    }
    expect(wakeInFlight(parent).size).toBe(0);
    for (const evidence of wakeEvidence(db)) expect(evidence.status).not.toBe('pending');
    expect(wakeIntents(db).length).toBeLessThanOrEqual(1);
    await harness.shutdown();
  });
});

describe('PF-7631 wake grant invariants', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('a redelivered wake notice is refused busy: one admission, one run, one intent', async () => {
    const db = new InMemoryDB();
    const { harness, parentAgent, childAgent } = buildHarness(db);
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const starts: Array<Record<string, unknown>> = [];
    const off = parent.subscribe(event => {
      if (event.type === 'agent_start' && event.origin === 'wake') starts.push({ ...event });
    });
    const { releaseChild } = await delegateFromAdmittedTurn(parent, parentAgent, childAgent);
    releaseChild();
    await poll(() => wakeIntents(db).length === 1);
    await parent.waitForIdle({ timeoutMs: 3000 });
    off();
    const [wake] = wakeIntents(db);
    const payload = wake!.projection.payload as { taskId: string; subagentSessionId: string; outcome: string };

    // The same notice delivered again (a redelivery after its acknowledgement).
    await expect(
      (parent as any)._injectSystemReminder(
        'again',
        {},
        {
          signalId: wake!.signalId,
          wakeNoticeId: wake!.signalId,
          wakeSettlement: {
            taskId: payload.taskId,
            subagentSessionId: payload.subagentSessionId,
            outcome: payload.outcome,
          },
        },
      ),
    ).rejects.toMatchObject({ name: 'HarnessBusyError' });
    await parent.waitForIdle({ timeoutMs: 3000 });
    expect(starts).toHaveLength(1);
    expect(reminderCalls(parentAgent)).toHaveLength(1);
    expect(wakeIntents(db)).toHaveLength(1);
    expect(wakeEvidence(db)).toHaveLength(1);
    await parent.close();
    await harness.shutdown();
  });

  it('a revoked wake grant refuses dispatch with an error event; the reservation is settled and close succeeds', async () => {
    const db = new InMemoryDB();
    const { harness, storage, parentAgent } = buildHarness(db);
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    await storage.revokeTerminalGrant({
      harnessName: 'default',
      executionGrant: { key: 'harness-wake:notice-x', generation: 1 },
      sessionId: parent.id,
      admissionId: 'harness-wake:notice-x',
      reason: { code: 'test.revoked', message: 'revoked by the test' },
    } as never);
    const { events, off } = errorEvents(parent);

    await expect(
      (parent as any)._injectSystemReminder(
        'wake',
        {},
        {
          signalId: 'notice-x',
          wakeNoticeId: 'notice-x',
          wakeSettlement: { taskId: 't', subagentSessionId: 's', outcome: 'completed' },
        },
      ),
    ).rejects.toThrow();
    off();
    expect(parentAgent.streamCalls).toHaveLength(0);
    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({ signalId: 'notice-x' });
    expect(wakeIntents(db)).toHaveLength(0);
    expect(wakeInFlight(parent).size).toBe(0);
    await parent.close();
    for (const evidence of wakeEvidence(db)) expect(evidence.status).not.toBe('pending');
    await harness.shutdown();
  });
});
