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

import { MockMemory } from '../../memory/mock';
import type { HarnessTerminalFinalizer, HarnessTerminalIntent } from '../../storage/domains/harness';
import { HarnessTerminalHandoffError } from '../../storage/domains/harness';
import { InMemoryHarness } from '../../storage/domains/harness/inmemory';
import { InMemoryDB } from '../../storage/domains/inmemory-db';

import { MockAgent } from './__test-utils__';
import { HarnessSessionLockedError, HarnessStorageError } from './errors';
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

type WakeCaller = 'completed' | 'rejected';
type CommitFailure = 'fails twice then recovers' | 'never recovers';

const commitRows: Array<{ caller: WakeCaller; failure: CommitFailure }> = (['completed', 'rejected'] as const).flatMap(
  caller => (['fails twice then recovers', 'never recovers'] as const).map(failure => ({ caller, failure })),
);

/** A wake terminal commit failure; the session surfaces it as `harness.terminal_pending`. */
function commitFailure() {
  return new HarnessStorageError({ operation: 'session_save', cause: new Error('storage down') });
}

/** The public code of a failed close-time settlement of an orphaned wake dispatch. */
const CLOSE_SETTLE_ERROR_CODE = 'harness.storage';

function commitErrorEvents(events: Array<Record<string, unknown>>) {
  return events.filter(event => (event.error as { code?: string } | undefined)?.code === 'harness.terminal_pending');
}

describe('PF-7631 wake terminal commit re-drive (bounded), both callers through one helper', () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it.each(commitRows)('$caller wake run, commit $failure', async ({ caller, failure }) => {
    const db = new InMemoryDB();
    const a = buildHarness(db);
    const commit = a.storage.commitTerminalHandoff.bind(a.storage);
    let wakeCommits = 0;
    let observed: Array<Record<string, unknown>> = [];
    let eventsBeforeFirstCommit: Array<Record<string, unknown>> | undefined;
    vi.spyOn(a.storage, 'commitTerminalHandoff').mockImplementation(async input => {
      if (isWakeInput(input)) {
        eventsBeforeFirstCommit ??= [...observed];
        wakeCommits += 1;
        if (failure === 'never recovers' || wakeCommits <= 2) throw commitFailure();
      }
      return commit(input);
    });
    const parent = await a.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const { events, off } = errorEvents(parent);
    observed = events;
    const wakeAgentEnds: Array<Record<string, unknown>> = [];
    const offAgentEnds = parent.subscribe(event => {
      if (event.type === 'agent_end' && String(event.runId).startsWith('harness-wake-'))
        wakeAgentEnds.push({ ...event });
    });
    const { releaseChild } = await delegateFromAdmittedTurn(parent, a.parentAgent, a.childAgent);
    // The wake run itself: completed, or failed by its own output.
    a.parentAgent.enqueueRun(caller === 'rejected' ? { failWith: new Error('provider rejected the wake run') } : {});
    const expectedStatus = caller === 'completed' ? 'completed' : 'failed';
    // Dispatch recovery commits the staged output of a completed run; a failed
    // run stages none, so the next owner settles it as interrupted.
    const recoveredResult =
      caller === 'completed'
        ? { status: 'completed' }
        : { status: 'aborted', error: { code: 'harness.run_interrupted' } };

    releaseChild();
    await poll(() => reminderCalls(a.parentAgent).length === 1);
    await poll(() => wakeCommits >= 1);

    // A failed wake run's own error is surfaced once, with its run id and
    // error class, before the first commit attempt; a completed run has none.
    const runErrors = (list: Array<Record<string, unknown>>) =>
      list.filter(event => (event.error as { code?: string } | undefined)?.code === 'harness.internal');
    if (caller === 'rejected') {
      expect(runErrors(eventsBeforeFirstCommit!)).toEqual([
        expect.objectContaining({
          runId: expect.stringMatching(/^harness-wake-/),
          error: expect.objectContaining({ code: 'harness.internal' }),
        }),
      ]);
    } else {
      expect(runErrors(eventsBeforeFirstCommit!)).toEqual([]);
    }

    if (failure === 'fails twice then recovers') {
      await poll(() => wakeIntents(db).length === 1);
      await poll(() => !parent.isRunning());
      off();
      expect(wakeCommits).toBe(3);
      expect(commitErrorEvents(events)).toHaveLength(2);
      expect(runErrors(events)).toHaveLength(caller === 'rejected' ? 1 : 0);
      expect(wakeIntents(db)).toHaveLength(1);
      expect(wakeIntents(db)[0]!.terminalResult.status).toBe(expectedStatus);
      expect(wakeEvidence(db).map(row => row.status)).toEqual([expectedStatus]);
      expect(wakeInFlight(parent).size).toBe(0);
      await parent.close();
      expect(wakeIntents(db)).toHaveLength(1);
      expect(reminderCalls(a.parentAgent)).toHaveLength(1);
      await a.harness.shutdown();
      return;
    }

    await poll(() => wakeCommits >= 4 && !parent.isRunning());
    // Past the whole backoff window: the in-turn re-drive stopped at the cap.
    await new Promise(r => setTimeout(r, 1_200));
    expect(wakeCommits).toBe(4);
    // The commit never landed: as for message(), the live turn leaves agent_end
    // to the final settlement instead of reporting a failure it cannot know.
    offAgentEnds();
    expect(wakeAgentEnds).toEqual([]);
    expect(commitErrorEvents(events)).toHaveLength(4);
    expect(runErrors(events)).toHaveLength(caller === 'rejected' ? 1 : 0);

    // Close is refused typed, within its bound, and the admission stays durable.
    const startedAt = Date.now();
    const beforeClose = events.length;
    await expect(parent.close()).rejects.toBeInstanceOf(HarnessSessionLockedError);
    expect(Date.now() - startedAt).toBeLessThan(5_000);
    // The close-time settlement attempt also fails and is surfaced, not swallowed.
    expect(events.slice(beforeClose)).toEqual([
      expect.objectContaining({ type: 'error', error: expect.objectContaining({ code: CLOSE_SETTLE_ERROR_CODE }) }),
    ]);
    off();
    expect(wakeIntents(db)).toHaveLength(0);
    expect(wakeEvidence(db).map(row => row.status)).toEqual(['pending']);

    // The next owner settles it exactly once, without re-running the wake.
    for (const [key, r] of db.harnessSessions) {
      if (r.id === parent.id) db.harnessSessions.set(key, { ...r, ownerId: 'crashed', leaseExpiresAt: Date.now() - 1 });
    }
    await a.harness.shutdown().catch(() => {});
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(Date.now() + DISPATCH_CLAIM_LAPSED_MS);
    const b = buildHarness(db);
    await b.harness.closeSession({ sessionId: parent.id });
    expect(wakeIntents(db)).toHaveLength(1);
    expect(wakeIntents(db)[0]!.terminalResult).toMatchObject(recoveredResult);
    expect(wakeEvidence(db).map(row => row.status)).not.toContain('pending');
    expect(b.parentAgent.streamCalls).toHaveLength(0);
    await b.harness.shutdown();
  });

  /**
   * Park a wake run on a tool approval. Wake terminal commits after the
   * resume fail while `storage.down` is true.
   */
  async function parkResumedWake() {
    const db = new InMemoryDB();
    const a = buildHarness(db);
    const commit = a.storage.commitTerminalHandoff.bind(a.storage);
    const storage = { resumed: false, down: true, wakeCommits: 0, failFirst: Number.POSITIVE_INFINITY };
    vi.spyOn(a.storage, 'commitTerminalHandoff').mockImplementation(async input => {
      if (storage.resumed && isWakeInput(input)) {
        storage.wakeCommits += 1;
        if (storage.down && storage.wakeCommits <= storage.failFirst) throw commitFailure();
      }
      return commit(input);
    });
    const parent = await a.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const { events, off } = errorEvents(parent);
    const { releaseChild } = await delegateFromAdmittedTurn(parent, a.parentAgent, a.childAgent);
    // The wake run asks for a tool approval; the approved resume completes it.
    a.parentAgent.enqueueRun({
      finishReason: 'suspended',
      suspendPayload: { toolCallId: 'wake-approval', toolName: 'shell', args: { cmd: 'ls' } },
    });
    a.parentAgent.enqueueRun({ finishReason: 'stop', text: 'resumed wake reply' });

    releaseChild();
    await poll(() => reminderCalls(a.parentAgent).length === 1);
    await poll(() => parent.getRecord().pendingResume !== undefined && !parent.isRunning());
    expect(wakeEvidence(db).map(row => row.status)).toEqual(['pending']);
    expect(wakeIntents(db)).toHaveLength(0);
    storage.resumed = true;
    return { db, a, parent, events, off, storage };
  }

  /** The first answer's resumed commit fails all four attempts; the answer rejects typed and stays parked. */
  async function failFirstAnswer(parked: Awaited<ReturnType<typeof parkResumedWake>>) {
    const { db, parent, events, storage } = parked;
    await expect(parent.respondToToolApproval({ approved: true })).rejects.toMatchObject({
      code: 'harness.terminal_pending',
    });
    expect(parent.getRecord().pendingResume).toBeDefined();
    expect(storage.wakeCommits).toBe(4);
    expect(commitErrorEvents(events)).toHaveLength(4);
    expect(wakeIntents(db)).toHaveLength(0);
    expect(wakeEvidence(db).map(row => row.status)).toEqual(['pending']);
  }

  const resumedReply = { status: 'completed', text: 'resumed wake reply' };

  it('resumed wake run (tool approval), commit fails twice then recovers', async () => {
    const { db, a, parent, events, off, storage } = await parkResumedWake();
    storage.failFirst = 2;
    await parent.respondToToolApproval({ approved: true });
    off();
    expect(storage.wakeCommits).toBe(3);
    expect(commitErrorEvents(events)).toHaveLength(2);
    expect(wakeIntents(db)).toHaveLength(1);
    expect(wakeIntents(db)[0]!.terminalResult.status).toBe('completed');
    expect(wakeIntents(db)[0]!.projection.payload).toMatchObject({ outcome: 'completed', reply: resumedReply });
    expect(wakeEvidence(db).map(row => row.status)).toEqual(['completed']);
    expect(parent.getRecord().pendingResume).toBeUndefined();
    await parent.close();
    expect(wakeIntents(db)).toHaveLength(1);
    await a.harness.shutdown();
  });

  it('resumed wake run (tool approval), commit never recovers: close refused, the next owner commits once', async () => {
    const parked = await parkResumedWake();
    const { db, a, parent, events, off, storage } = parked;
    await failFirstAnswer(parked);

    // Close is refused typed, within its bound, and the admission stays durable.
    const startedAt = Date.now();
    const beforeClose = events.length;
    await expect(parent.close()).rejects.toBeInstanceOf(HarnessSessionLockedError);
    expect(Date.now() - startedAt).toBeLessThan(5_000);
    // The interaction is still parked (pendingResume), so close refuses on the
    // live resume without a settlement attempt: no new commit, no new event.
    expect(storage.wakeCommits).toBe(4);
    expect(events).toHaveLength(beforeClose);
    off();
    expect(wakeIntents(db)).toHaveLength(0);

    // The next owner settles it exactly once from the staged resumed output.
    for (const [key, r] of db.harnessSessions) {
      if (r.id === parent.id) db.harnessSessions.set(key, { ...r, ownerId: 'crashed', leaseExpiresAt: Date.now() - 1 });
    }
    await a.harness.shutdown().catch(() => {});
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(Date.now() + DISPATCH_CLAIM_LAPSED_MS);
    const b = buildHarness(db);
    const nextCommit = b.storage.commitTerminalHandoff.bind(b.storage);
    let nextOwnerWakeCommits = 0;
    vi.spyOn(b.storage, 'commitTerminalHandoff').mockImplementation(async input => {
      if (isWakeInput(input)) nextOwnerWakeCommits += 1;
      return nextCommit(input);
    });
    await b.harness.closeSession({ sessionId: parent.id });
    expect(nextOwnerWakeCommits).toBe(1);
    expect(wakeIntents(db)).toHaveLength(1);
    expect(wakeIntents(db)[0]!.terminalResult.status).toBe('completed');
    expect(wakeIntents(db)[0]!.projection.payload).toMatchObject({ outcome: 'completed', reply: resumedReply });
    expect(wakeEvidence(db).map(row => row.status)).toEqual(['completed']);
    expect(b.parentAgent.streamCalls).toHaveLength(0);
    await b.harness.shutdown();
  });

  it('resumed wake run: the answer retried after storage recovers settles through the re-drive', async () => {
    const parked = await parkResumedWake();
    const { db, a, parent, off, storage } = parked;
    await failFirstAnswer(parked);
    const streamsBefore = a.parentAgent.streamCalls.length;

    storage.down = false;
    await expect(parent.respondToToolApproval({ approved: true })).resolves.toBeDefined();
    off();
    expect(storage.wakeCommits).toBe(5);
    expect(wakeIntents(db)).toHaveLength(1);
    expect(wakeIntents(db)[0]!.terminalResult.status).toBe('completed');
    expect(wakeIntents(db)[0]!.projection.payload).toMatchObject({ outcome: 'completed', reply: resumedReply });
    expect(wakeEvidence(db).map(row => row.status)).toEqual(['completed']);
    expect(parent.getRecord().pendingResume).toBeUndefined();
    expect(a.parentAgent.streamCalls).toHaveLength(streamsBefore);
    await parent.close();
    expect(wakeIntents(db)).toHaveLength(1);
    await a.harness.shutdown();
  });

  it.each(['storage recovered', 'storage still down'] as const)(
    'resumed wake run: once the interaction is due, the live owner settles it on close (%s)',
    async state => {
      const parked = await parkResumedWake();
      const { db, a, parent, events, off, storage } = parked;
      await failFirstAnswer(parked);
      const streamsBefore = a.parentAgent.streamCalls.length;

      // The owner never crashes: the resume admission goes stale after 30 s.
      // Its heartbeat keeps the lease; only the clock is faked here.
      if (state === 'storage recovered') storage.down = false;
      vi.useFakeTimers({ toFake: ['Date'] });
      vi.setSystemTime(Date.now() + DISPATCH_CLAIM_LAPSED_MS);
      for (const [key, r] of db.harnessSessions) {
        if (r.id === parent.id) db.harnessSessions.set(key, { ...r, leaseExpiresAt: Date.now() + 60_000 });
      }

      if (state === 'storage recovered') {
        await parent.close();
        off();
        expect(storage.wakeCommits).toBe(5);
        expect(wakeIntents(db)).toHaveLength(1);
        expect(wakeIntents(db)[0]!.terminalResult.status).toBe('completed');
        expect(wakeIntents(db)[0]!.projection.payload).toMatchObject({ outcome: 'completed', reply: resumedReply });
        expect(wakeEvidence(db).map(row => row.status)).toEqual(['completed']);
        expect(a.parentAgent.streamCalls).toHaveLength(streamsBefore);
        await a.harness.shutdown();
        return;
      }

      // Still down: close is refused typed, the failed attempt is surfaced,
      // and the admission stays durable.
      const beforeClose = events.length;
      const refusal = await parent.close().then(
        () => undefined,
        (error: unknown) => error,
      );
      // Refused by the parked scan, so the closing marker is cleared and the
      // interaction stays with this live owner.
      expect(refusal).toBeInstanceOf(HarnessSessionLockedError);
      expect(refusal).toMatchObject({ currentOwnerId: 'parked-admission' });
      // One settlement attempt (the session is closing, so no re-drive), surfaced once.
      expect(storage.wakeCommits).toBe(5);
      expect(events.slice(beforeClose)).toEqual([
        expect.objectContaining({
          type: 'error',
          runId: expect.stringMatching(/^harness-wake-/),
          error: expect.objectContaining({ code: 'harness.terminal_pending' }),
        }),
      ]);
      expect(parent.getRecord().pendingResume).toBeDefined();
      expect(wakeIntents(db)).toHaveLength(0);
      expect(wakeEvidence(db).map(row => row.status)).toEqual(['pending']);

      // Once storage recovers, the live owner's expiry timer settles it on its
      // own: one more commit, the resumed reply, no new answer or close.
      storage.down = false;
      await poll(() => wakeIntents(db).length === 1, 5_000);
      off();
      expect(storage.wakeCommits).toBe(6);
      expect(wakeIntents(db)[0]!.projection.payload).toMatchObject({ outcome: 'completed', reply: resumedReply });
      expect(a.parentAgent.streamCalls).toHaveLength(streamsBefore);
      await parent.close();
      await a.harness.shutdown();
    },
  );

  it('resumed wake run: a due close whose commit and wake probe both fail rejects with the raw handoff error', async () => {
    const parked = await parkResumedWake();
    const { db, a, parent, events, off, storage } = parked;
    await failFirstAnswer(parked);

    // Storage is out for the commit and for close's wake-admission probe read
    // after it: close cannot tell the admission is a wake, so the narrowed
    // contract applies and the raw error is surfaced (no typed refusal).
    const load = a.storage.loadTerminalAdmissionByRun.bind(a.storage);
    vi.spyOn(a.storage, 'loadTerminalAdmissionByRun').mockImplementation(async input => {
      if (storage.wakeCommits >= 5) throw commitFailure();
      return load(input);
    });
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(Date.now() + DISPATCH_CLAIM_LAPSED_MS);
    for (const [key, r] of db.harnessSessions) {
      if (r.id === parent.id) db.harnessSessions.set(key, { ...r, leaseExpiresAt: Date.now() + 60_000 });
    }

    const beforeClose = events.length;
    const refusal = await parent.close().then(
      () => undefined,
      (error: unknown) => error,
    );
    off();
    expect(refusal).not.toBeInstanceOf(HarnessSessionLockedError);
    expect(refusal).toBeInstanceOf(HarnessTerminalHandoffError);
    expect(refusal).toMatchObject({ code: 'harness.terminal_pending' });
    expect(storage.wakeCommits).toBe(5);
    expect(events.slice(beforeClose)).toEqual([]);
    expect(wakeIntents(db)).toHaveLength(0);
    expect(wakeEvidence(db).map(row => row.status)).toEqual(['pending']);
    expect(parent.getRecord().pendingResume).toBeDefined();
    await a.harness.shutdown().catch(() => {});
  });

  it('a Stop while a wake commit attempt is in flight ends the re-drive with no error event for the abort', async () => {
    const db = new InMemoryDB();
    const { harness, storage, parentAgent, childAgent } = buildHarness(db);
    const commit = storage.commitTerminalHandoff.bind(storage);
    let reached!: () => void;
    const commitReached = new Promise<void>(r => (reached = r));
    let release!: () => void;
    const commitReleased = new Promise<void>(r => (release = r));
    let wakeCommits = 0;
    vi.spyOn(storage, 'commitTerminalHandoff').mockImplementation(async input => {
      if (isWakeInput(input)) {
        wakeCommits += 1;
        reached();
        await commitReleased;
        throw commitFailure();
      }
      return commit(input);
    });
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const { events, off } = errorEvents(parent);
    const { releaseChild } = await delegateFromAdmittedTurn(parent, parentAgent, childAgent);

    releaseChild();
    await commitReached;
    await parent.abortActiveWork();
    await poll(() => !parent.isRunning());
    release();
    await new Promise(r => setTimeout(r, 1_200));
    off();

    expect(wakeCommits).toBe(1);
    expect(events).toEqual([]);
    expect(wakeInFlight(parent).size).toBe(0);
    await harness.shutdown().catch(() => {});
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
    // Every await but the last is inside the admission, before the dispatch.
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
    // Before the dispatch nothing ran under the grant: the admission is
    // cancelled, with no intent. Once dispatched, the reserved run started
    // under the grant, so recovery settles it interrupted with one aborted intent.
    const admissions = [...db.harnessTerminalAdmissions.values()].filter(a =>
      a.admissionId.startsWith('harness-wake:'),
    );
    const dispatched = row.at === 'dispatch accepted';
    expect(wakeIntents(db)).toHaveLength(dispatched ? 1 : 0);
    expect(admissions.map(a => a.status)).toEqual([dispatched ? 'committed' : 'cancelled']);
    expect(wakeEvidence(db)).toHaveLength(1);
    expect(wakeEvidence(db)[0]).toMatchObject({
      status: 'failed',
      error: { code: dispatched ? 'harness.run_interrupted' : 'harness.terminal_cancelled' },
    });
    await harness.shutdown();
  });
});

type DispatchOutcome = 'sendSignal throws synchronously' | 'accepted discard' | 'accepted never settles';

describe('PF-7631 wake dispatch outcomes', () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it.each<{ outcome: DispatchOutcome }>([
    { outcome: 'sendSignal throws synchronously' },
    { outcome: 'accepted discard' },
    { outcome: 'accepted never settles' },
  ])('$outcome', async ({ outcome }) => {
    const db = new InMemoryDB();
    const { harness, parentAgent, childAgent } = buildHarness(db);
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const { releaseChild } = await delegateFromAdmittedTurn(parent, parentAgent, childAgent);
    const real = parentAgent.sendSignal.bind(parentAgent);
    let wakeDispatches = 0;
    vi.spyOn(parentAgent, 'sendSignal').mockImplementation(((signal: any, target: any) => {
      if (typeof target?.runId !== 'string' || !target.runId.startsWith('harness-wake-')) return real(signal, target);
      wakeDispatches += 1;
      if (outcome === 'sendSignal throws synchronously') throw new Error('refused before any run reservation');
      if (outcome === 'accepted discard') {
        return {
          signal: { ...signal, id: signal.id },
          runId: target.runId,
          accepted: Promise.resolve({ action: 'discard' }),
        };
      }
      const result = real(signal, target);
      const never = new Promise<never>(() => {});
      return new Proxy(result, { get: (t, p) => (p === 'accepted' ? never : Reflect.get(t, p)) });
    }) as never);
    // The wake's native acceptance bound (SIGNAL_NATIVE_ACCEPT_TIMEOUT_MS,
    // 30 s), the first 30 s timer armed after the wake's dispatch, fires at
    // once; every other timer keeps its delay.
    const realSetTimeout = globalThis.setTimeout;
    let acceptanceBoundShortened = false;
    vi.spyOn(globalThis, 'setTimeout').mockImplementation(((fn: () => void, ms?: number, ...args: unknown[]) => {
      const shorten = ms === 30_000 && wakeDispatches === 1 && !acceptanceBoundShortened;
      if (shorten) acceptanceBoundShortened = true;
      return realSetTimeout(fn, shorten ? 0 : ms, ...args);
    }) as never);

    releaseChild();
    await poll(() => wakeDispatches === 1);
    // The wake turn is released, not parked on the acknowledgement.
    await poll(() => !parent.isRunning());
    await poll(() => wakeInFlight(parent).size === 0);
    vi.mocked(globalThis.setTimeout).mockRestore();
    await parent.close();

    // A synchronous throw comes before any run reservation, and a `discard`
    // proves nothing ran: the grant is cancelled, with no terminal result and
    // no intent. A timed-out acknowledgement is ambiguous (the run may have
    // started), so the aborted turn is left to recovery, which settles it
    // interrupted.
    const admissions = [...db.harnessTerminalAdmissions.values()].filter(a =>
      a.admissionId.startsWith('harness-wake:'),
    );
    const ambiguous = outcome === 'accepted never settles';
    expect(wakeIntents(db)).toHaveLength(ambiguous ? 1 : 0);
    expect(admissions.map(a => a.status)).toEqual([ambiguous ? 'committed' : 'cancelled']);
    expect(wakeEvidence(db)).toHaveLength(1);
    expect(wakeEvidence(db)[0]).toMatchObject({
      status: 'failed',
      error: { code: ambiguous ? 'harness.run_interrupted' : 'harness.terminal_cancelled' },
    });
    expect(wakeInFlight(parent).size).toBe(0);
    for (const evidence of wakeEvidence(db)) expect(evidence.status).not.toBe('pending');
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
          wakeReminderRow: {
            agent: parentAgent,
            memory: parentAgent.__fixedMemory()!,
            modeId: 'default',
            modeEpoch: 0,
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
          wakeReminderRow: {
            agent: parentAgent,
            memory: parentAgent.__fixedMemory()!,
            modeId: 'default',
            modeEpoch: 0,
          },
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
