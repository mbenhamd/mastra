/**
 * Harness v1 — settle-only terminal finalizers (PF-5505). A process admits new
 * terminal turns under its primary finalizer only, and settles a pending turn
 * under the registration matching the turn's recorded finalizer id@version: the
 * primary or a `settleOnly` entry, in either version direction. A turn whose
 * id@version is not registered stays pending.
 */

import { afterEach, describe, expect, it, vi } from 'vitest';

import type { HarnessTerminalFinalizer } from '../../storage/domains/harness';
import { InMemoryHarness } from '../../storage/domains/harness/inmemory';
import { InMemoryDB } from '../../storage/domains/inmemory-db';

import { MockAgent } from './__test-utils__/mock-agent';
import { HarnessConfigError } from './errors';
import { Harness } from './harness';

const LEASE_AND_DISPATCH_CLAIM_LAPSED_MS = 31_000;
const FINALIZER_ID = 'doxa.chat-usage.terminal';

type FinalizeCall = { version: string; finalizerVersion: string; status: string; reply: string | null };

function finalizerAt(version: string, calls: FinalizeCall[]): HarnessTerminalFinalizer {
  return {
    id: FINALIZER_ID,
    version,
    finalize: async ({ finalizerVersion, result, fullOutput }) => {
      // The reply is whatever output the run produced; an interrupted orphan
      // never produced one.
      const text = (fullOutput as { text?: unknown } | undefined)?.text;
      const reply = typeof text === 'string' ? text : null;
      calls.push({ version, finalizerVersion, status: result.status, reply });
      return {
        projectionKind: `chat.summary.v${version}`,
        projectionId: result.runId,
        payload: { status: result.status, code: result.error?.code ?? null, reply },
      };
    },
  };
}

type TerminalRegistration = { finalizer: HarnessTerminalFinalizer; settleOnly?: HarnessTerminalFinalizer[] };

function harnessProcess(db: InMemoryDB, agent: MockAgent, terminalHandoff: TerminalRegistration) {
  const storage = new InMemoryHarness({
    db,
    terminalHandoff: { enabled: true },
    sessionRecordProjection: { enabled: true },
  });
  const harness = new Harness({
    agents: { default: agent } as any,
    modes: [{ id: 'default', agentId: 'default' }],
    defaultModeId: 'default',
    sessions: { storage, terminalHandoff },
  });
  return { harness, storage };
}

const grant = { key: 'usage-claim-drain', generation: 1 };
const admissionId = 'drain-turn';

/** Admit a terminal turn under `registration` whose owner then dies mid-run. */
async function orphanTurnAdmittedUnder(db: InMemoryDB, registration: TerminalRegistration) {
  const agent = new MockAgent({ id: 'default' });
  let release!: () => void;
  agent.enqueueRun({ holdUntil: new Promise<void>(resolve => (release = resolve)) });
  const owner = harnessProcess(db, agent, registration);
  const session = await owner.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
  await session.message({
    content: 'hi',
    admissionId,
    executionAuthorityGrant: grant,
    terminalAdmissionSeed: { v: 1 },
    stream: true,
  } as never);
  expect(agent.streamCalls).toHaveLength(1);
  const stop = async () => {
    release();
    await owner.harness.shutdown().catch(() => {});
  };
  return { sessionId: session.id, stop };
}

async function adoptAfterLeaseLapse(
  db: InMemoryDB,
  sessionId: string,
  registration: TerminalRegistration,
  configure?: (process: ReturnType<typeof harnessProcess>) => void,
) {
  vi.useFakeTimers({ toFake: ['Date'] });
  vi.setSystemTime(Date.now() + LEASE_AND_DISPATCH_CLAIM_LAPSED_MS);
  const agent = new MockAgent({ id: 'default', defaultOutput: { text: 'must not run' } });
  const adopter = harnessProcess(db, agent, registration);
  configure?.(adopter);
  await adopter.harness.session({ sessionId, resourceId: 'u1' });
  const admission = await adopter.storage.loadTerminalAdmission({
    harnessName: 'default',
    sessionId,
    admissionId,
    executionGrant: grant,
  });
  return { ...adopter, agent, admission };
}

const countFor = (version: string) => ({
  harnessName: 'default',
  finalizerId: FINALIZER_ID,
  finalizerVersion: version,
});

describe('Harness terminal handoff — settle-only finalizers', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it.each([
    { admitted: '1', primary: '2', direction: 'a previous version (drain after a bump)' },
    { admitted: '2', primary: '1', direction: 'a later version (rollback to a settle-capable release)' },
  ])('settles an orphaned turn admitted under $direction', async ({ admitted, primary }) => {
    const db = new InMemoryDB();
    const calls: FinalizeCall[] = [];
    const owner = await orphanTurnAdmittedUnder(db, { finalizer: finalizerAt(admitted, calls) });
    const adopter = await adoptAfterLeaseLapse(db, owner.sessionId, {
      finalizer: finalizerAt(primary, calls),
      settleOnly: [finalizerAt(admitted, calls)],
    });
    try {
      // The admitted version's registration settles the turn; the primary never
      // finalizes it under another version's identity.
      // A run cut off mid-flight never produced a reply, so it is finalized
      // with the explicit interrupted outcome — never as a completed turn.
      expect(calls).toEqual([{ version: admitted, finalizerVersion: admitted, status: 'aborted', reply: null }]);
      expect(adopter.admission).toMatchObject({
        status: 'committed',
        finalizerVersion: admitted,
        terminalResult: { status: 'aborted', error: { code: 'harness.run_interrupted' } },
      });
      await expect(adopter.storage.countPendingTerminalAdmissions(countFor(admitted))).resolves.toBe(0);
      // The committed intent carries the admitted version's projection and is
      // still owed to a consumer.
      await expect(adopter.storage.countUndeliveredTerminalIntents(countFor(admitted))).resolves.toBe(1);
      const claimed = await adopter.storage.claimTerminalIntents({
        harnessName: 'default',
        consumerId: 'consumer',
        limit: 10,
      });
      expect(claimed.intents).toMatchObject([
        { finalizerVersion: admitted, projection: { projectionKind: `chat.summary.v${admitted}` } },
      ]);
      expect(JSON.parse(claimed.intents[0]!.projection.payloadJson)).toEqual({
        status: 'aborted',
        code: 'harness.run_interrupted',
        reply: null,
      });
      expect(adopter.agent.streamCalls).toHaveLength(0);
    } finally {
      await adopter.harness.shutdown();
      await owner.stop();
    }
  });

  it.each([
    { name: 'before usage save', usageSaveBoundary: 'before', cancelled: false },
    { name: 'after usage save', usageSaveBoundary: 'after', cancelled: false },
    { name: 'after a staged turn is deliberately cancelled', usageSaveBoundary: 'before', cancelled: true },
  ])('recovers the canonical terminal outcome after its owner dies $name', async ({ usageSaveBoundary, cancelled }) => {
    const db = new InMemoryDB();
    const calls: FinalizeCall[] = [];
    const ownerAgent = new MockAgent({ id: 'default' });
    ownerAgent.enqueueRun({
      finishReason: 'stop',
      text: 'final answer',
      usage: { inputTokens: 8, outputTokens: 5, totalTokens: 13 },
    });
    const owner = harnessProcess(db, ownerAgent, { finalizer: finalizerAt('1', calls) });
    const ownerSession = await owner.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    // The run finishes, then its owner stops during the session-usage flush,
    // before it even reaches the finalizer or terminal commit.
    const saveSession = owner.storage.saveSession.bind(owner.storage);
    let releaseUsageFlush!: () => void;
    const usageFlush = new Promise<void>(resolve => (releaseUsageFlush = resolve));
    let startedUsageFlush!: () => void;
    const usageFlushStarted = new Promise<void>(resolve => (startedUsageFlush = resolve));
    owner.storage.saveSession = async (record, options) => {
      if ((record.tokenUsage?.totalTokens ?? 0) === 0) return saveSession(record, options);
      const saved = usageSaveBoundary === 'after' ? await saveSession(record, options) : undefined;
      startedUsageFlush();
      await usageFlush;
      return saved ?? saveSession(record, options);
    };
    const turn = ownerSession.message({
      content: 'hi',
      admissionId,
      executionAuthorityGrant: grant,
      terminalAdmissionSeed: { v: 1 },
    });
    void turn.catch(() => {});
    const scope = { harnessName: 'default', sessionId: ownerSession.id, resourceId: 'u1' };
    await usageFlushStarted;
    await vi.waitFor(async () => {
      const pending = await owner.storage.listPendingMessageAdmissions({
        ...scope,
        threadId: ownerSession.threadId,
        now: Date.now(),
        limit: 10,
      });
      expect(pending.items).toMatchObject([
        { evidence: { status: 'pending', stagedResult: { text: 'final answer' } } },
      ]);
    });
    calls.length = 0;
    const pending = await owner.storage.loadTerminalAdmission({ ...scope, admissionId, executionGrant: grant });
    if (cancelled) {
      await owner.storage.cancelTerminalHandoff({
        ...pending!,
        reason: { code: 'harness.terminal_cancelled', message: 'cancelled deliberately' },
      });
    }

    const adopter = await adoptAfterLeaseLapse(db, ownerSession.id, {
      finalizer: finalizerAt('2', calls),
      settleOnly: [finalizerAt('1', calls)],
    });
    try {
      if (cancelled) {
        expect(adopter.admission).toMatchObject({ status: 'cancelled' });
        expect(calls).toEqual([]);
        await expect(adopter.storage.countUndeliveredTerminalIntents(countFor('1'))).resolves.toBe(0);
        await expect(
          adopter.storage.loadRunSummary({ harnessName: 'default', runId: pending!.runId }),
        ).resolves.toMatchObject({ status: 'interrupted' });
        expect(adopter.agent.streamCalls).toHaveLength(0);
        return;
      }
      // Recovery commits the finished turn as completed with its reply — never
      // as interrupted — through the finalizer of the version it was admitted under.
      expect(calls).toEqual([{ version: '1', finalizerVersion: '1', status: 'completed', reply: 'final answer' }]);
      expect(adopter.admission).toMatchObject({
        status: 'committed',
        finalizerVersion: '1',
        terminalResult: { status: 'completed' },
      });
      const [intent] = (
        await adopter.storage.claimTerminalIntents({ harnessName: 'default', consumerId: 'consumer', limit: 10 })
      ).intents;
      expect(JSON.parse(intent!.projection.payloadJson)).toEqual({
        status: 'completed',
        code: null,
        reply: 'final answer',
      });
      await expect(
        adopter.storage.loadMessageResultEvidence({
          ...scope,
          threadId: ownerSession.threadId,
          signalId: intent!.signalId,
        }),
      ).resolves.toMatchObject({ status: 'completed', result: { text: 'final answer' } });
      await expect(adopter.storage.loadSession(scope)).resolves.toMatchObject({
        tokenUsage: { promptTokens: 8, completionTokens: 5, totalTokens: 13 },
      });
      await expect(
        adopter.storage.loadRunSummary({ harnessName: 'default', runId: intent!.runId }),
      ).resolves.toMatchObject({ status: 'completed' });
      expect(adopter.agent.streamCalls).toHaveLength(0);
    } finally {
      await adopter.harness.shutdown();
      releaseUsageFlush();
      await turn.catch(() => {});
      await owner.harness.shutdown().catch(() => {});
    }
  });

  it.each([
    { name: 'cold recovery', firstStream: false, warmRetry: false },
    { name: 'warm streamed retry', firstStream: true, warmRetry: true },
  ])(
    'retries staging and completion publication through $name without a provisional failed summary',
    async ({ firstStream, warmRetry }) => {
      const db = new InMemoryDB();
      const calls: FinalizeCall[] = [];
      const ownerAgent = new MockAgent({ id: 'default' });
      ownerAgent.enqueueRun({
        finishReason: 'stop',
        text: 'durable answer',
        usage: { inputTokens: 8, outputTokens: 5, totalTokens: 13 },
      });
      const owner = harnessProcess(db, ownerAgent, { finalizer: finalizerAt('1', calls) });
      const session = await owner.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const completionEvents: string[] = [];
      session.subscribe(event => {
        if (event.type === 'agent_end' || event.type === 'run_completed') completionEvents.push(event.type);
      });
      const commit = owner.storage.commitTerminalHandoff.bind(owner.storage);
      owner.storage.commitTerminalHandoff = async () => {
        throw new Error('terminal commit temporarily unavailable');
      };
      const stage = owner.storage.stageTerminalCompletion.bind(owner.storage);
      let stagingFailed = false;
      owner.storage.stageTerminalCompletion = async input => {
        if (!stagingFailed) {
          stagingFailed = true;
          throw new Error('completed output staging temporarily unavailable');
        }
        return stage(input);
      };
      const failures: Error[] = [];
      const message = {
        content: 'hi',
        admissionId,
        executionAuthorityGrant: grant,
        terminalAdmissionSeed: { v: 1 },
        onTerminalCommitError: (error: Error) => failures.push(error),
      };
      if (firstStream) {
        await session.message({ ...message, stream: true });
        await vi.waitFor(() => expect(failures).toHaveLength(1));
      } else {
        await expect(session.message(message)).rejects.toMatchObject({ code: 'harness.terminal_pending' });
        expect(failures).toHaveLength(1);
      }
      // A transient staging failure preserves the already-produced output for
      // retry. Retrying stages it before the usage save and never re-runs it.
      await expect(session.message(message)).rejects.toMatchObject({ code: 'harness.terminal_pending' });
      expect(failures).toHaveLength(2);
      expect(ownerAgent.streamCalls).toHaveLength(1);
      const identity = { harnessName: 'default', sessionId: session.id, admissionId, executionGrant: grant };
      const pending = await owner.storage.loadTerminalAdmission(identity);
      expect(pending).toMatchObject({ status: 'pending' });
      await expect(owner.storage.loadRunSummary({ harnessName: 'default', runId: pending!.runId })).resolves.toBeNull();
      if (warmRetry) {
        owner.storage.commitTerminalHandoff = commit;
        const replies = await Promise.all([session.message(message), session.message(message)]);
        expect(replies).toMatchObject([{ text: 'durable answer' }, { text: 'durable answer' }]);
        await expect(owner.storage.loadTerminalAdmission(identity)).resolves.toMatchObject({ status: 'committed' });
        await expect(
          owner.storage.loadRunSummary({ harnessName: 'default', runId: pending!.runId }),
        ).resolves.toMatchObject({ status: 'completed' });
        expect(failures).toHaveLength(2);
        expect(ownerAgent.streamCalls).toHaveLength(1);
        expect(completionEvents).toEqual(['agent_end', 'run_completed']);
        await owner.harness.shutdown();
        return;
      }
      await owner.harness.shutdown();
      calls.length = 0;
      const registration = { finalizer: finalizerAt('2', calls), settleOnly: [finalizerAt('1', calls)] };
      let failedAdopter: ReturnType<typeof harnessProcess> | undefined;
      let adopter: Awaited<ReturnType<typeof adoptAfterLeaseLapse>> | undefined;
      try {
        await expect(
          adoptAfterLeaseLapse(db, session.id, registration, process => {
            failedAdopter = process;
            process.storage.saveRunSummary = async () => {
              throw new Error('completion publication temporarily unavailable');
            };
          }),
        ).rejects.toThrow();
        await expect(failedAdopter!.storage.loadTerminalAdmission(identity)).resolves.toMatchObject({
          status: 'committed',
        });
        await expect(
          failedAdopter!.storage.loadRunSummary({ harnessName: 'default', runId: pending!.runId }),
        ).resolves.toBeNull();
        await failedAdopter!.harness.shutdown().catch(() => {});

        // The next process discovers the committed output with no summary and
        // publishes completion without executing or finalizing the run again.
        adopter = await adoptAfterLeaseLapse(db, session.id, registration);
        expect(calls).toEqual([{ version: '1', finalizerVersion: '1', status: 'completed', reply: 'durable answer' }]);
        await expect(
          adopter.storage.loadRunSummary({ harnessName: 'default', runId: pending!.runId }),
        ).resolves.toMatchObject({ status: 'completed' });
        await expect(
          adopter.storage.loadSession({ harnessName: 'default', sessionId: session.id }),
        ).resolves.toMatchObject({
          tokenUsage: { promptTokens: 8, completionTokens: 5, totalTokens: 13 },
        });
        const [intent] = (
          await adopter.storage.claimTerminalIntents({ harnessName: 'default', consumerId: 'consumer', limit: 10 })
        ).intents;
        expect(JSON.parse(intent!.projection.payloadJson)).toEqual({
          status: 'completed',
          code: null,
          reply: 'durable answer',
        });
        expect(adopter.agent.streamCalls).toHaveLength(0);
      } finally {
        await adopter?.harness.shutdown();
        await failedAdopter?.harness.shutdown().catch(() => {});
      }
    },
  );

  it('publishes one completion when retries overlap an outstanding original summary save', async () => {
    const db = new InMemoryDB();
    const agent = new MockAgent({ id: 'default' });
    agent.enqueueRun({ finishReason: 'stop', text: 'answer' });
    const owner = harnessProcess(db, agent, { finalizer: finalizerAt('1', []) });
    const session = await owner.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const completionEvents: string[] = [];
    session.subscribe(event => {
      if (event.type === 'agent_end' || event.type === 'run_completed') completionEvents.push(event.type);
    });
    let releaseSave!: () => void;
    const saveGate = new Promise<void>(resolve => (releaseSave = resolve));
    let startedSave!: () => void;
    const saveStarted = new Promise<void>(resolve => (startedSave = resolve));
    const save = owner.storage.saveRunSummary.bind(owner.storage);
    owner.storage.saveRunSummary = async input => {
      startedSave();
      await saveGate;
      return save(input);
    };
    const message = { content: 'hi', admissionId, executionAuthorityGrant: grant, terminalAdmissionSeed: { v: 1 } };
    try {
      await expect(session.message(message)).resolves.toMatchObject({ text: 'answer' });
      await saveStarted;
      let receipts = 0;
      let receivedBoth!: () => void;
      const bothReceived = new Promise<void>(resolve => (receivedBoth = resolve));
      const retry = () =>
        session.message({
          ...message,
          onTerminalCommit: () => {
            if (++receipts === 2) receivedBoth();
          },
        });
      const retries = Promise.all([retry(), retry()]);
      await bothReceived;
      // Let both canonical receipt continuations run while the original
      // summary write is deliberately held, without relying on a time delay.
      await new Promise<void>(resolve => setImmediate(resolve));
      releaseSave();
      await expect(retries).resolves.toMatchObject([{ text: 'answer' }, { text: 'answer' }]);
      expect(completionEvents).toEqual(['agent_end', 'run_completed']);
      expect(agent.streamCalls).toHaveLength(1);
    } finally {
      releaseSave();
      await owner.harness.shutdown();
    }
  });

  it('leaves an orphaned turn pending when its admitted version is not registered', async () => {
    const db = new InMemoryDB();
    const calls: FinalizeCall[] = [];
    const owner = await orphanTurnAdmittedUnder(db, { finalizer: finalizerAt('1', calls) });
    const adopter = await adoptAfterLeaseLapse(db, owner.sessionId, {
      finalizer: finalizerAt('2', calls),
      settleOnly: [finalizerAt('3', calls)],
    });
    try {
      expect(calls).toEqual([]);
      expect(adopter.admission?.status).toBe('pending');
      await expect(adopter.storage.countPendingTerminalAdmissions(countFor('1'))).resolves.toBe(1);
      await expect(adopter.storage.countUndeliveredTerminalIntents(countFor('1'))).resolves.toBe(0);
    } finally {
      await adopter.harness.shutdown();
      await owner.stop();
    }
  });

  it('commits a turn admitted under the previous version with its reply through its settle-only finalizer', async () => {
    const db = new InMemoryDB();
    const calls: FinalizeCall[] = [];
    // A turn admitted under @1 parks for a tool approval before its owner stops.
    const ownerAgent = new MockAgent({ id: 'default' });
    ownerAgent.enqueueRun({
      finishReason: 'suspended',
      suspendPayload: { toolCallId: 'tc-1', toolName: 'shell', args: { cmd: 'ls' } },
    });
    const owner = harnessProcess(db, ownerAgent, { finalizer: finalizerAt('1', calls) });
    const ownerSession = await owner.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const suspended = await ownerSession.message({
      content: 'needs approval',
      admissionId,
      executionAuthorityGrant: grant,
      terminalAdmissionSeed: { v: 1 },
    });
    expect(suspended.finishReason).toBe('suspended');
    await owner.harness.shutdown();

    // The process that resumes it admits under @2 and settles @1.
    const agent = new MockAgent({ id: 'default' });
    agent.enqueueRun({ finishReason: 'stop', text: 'resumed answer' });
    const resumer = harnessProcess(db, agent, {
      finalizer: finalizerAt('2', calls),
      settleOnly: [finalizerAt('1', calls)],
    });
    const resumeSession = await resumer.harness.session({ sessionId: ownerSession.id, resourceId: 'u1' });
    resumer.storage.commitTerminalHandoff = async () => {
      throw new Error('process stopped before the resumed terminal commit');
    };
    const stage = resumer.storage.stageTerminalCompletion.bind(resumer.storage);
    let stagingFailed = false;
    resumer.storage.stageTerminalCompletion = async input => {
      if (!stagingFailed) {
        stagingFailed = true;
        throw new Error('completed output staging temporarily unavailable');
      }
      return stage(input);
    };
    await expect(resumeSession.respondToToolApproval({ approved: true })).rejects.toMatchObject({
      code: 'harness.terminal_pending',
    });
    // Retrying uses the cached output, restages it before finalization, and
    // never executes the provider twice.
    await expect(resumeSession.respondToToolApproval({ approved: true })).rejects.toMatchObject({
      code: 'harness.terminal_pending',
    });
    expect(agent.resumeCalls).toHaveLength(1);
    await resumer.harness.shutdown();
    calls.length = 0;
    const adopter = await adoptAfterLeaseLapse(db, ownerSession.id, {
      finalizer: finalizerAt('2', calls),
      settleOnly: [finalizerAt('1', calls)],
    });
    try {
      const session = await adopter.harness.session({ sessionId: ownerSession.id, resourceId: 'u1' });
      // The previous owner is gone; the reply produced on resume reaches the
      // admitted version's finalizer and its committed intent.
      expect(calls).toEqual([{ version: '1', finalizerVersion: '1', status: 'completed', reply: 'resumed answer' }]);
      const [intent] = (
        await adopter.storage.claimTerminalIntents({ harnessName: 'default', consumerId: 'consumer', limit: 10 })
      ).intents;
      expect(intent).toMatchObject({ finalizerVersion: '1', projection: { projectionKind: 'chat.summary.v1' } });
      expect(JSON.parse(intent!.projection.payloadJson)).toEqual({
        status: 'completed',
        code: null,
        reply: 'resumed answer',
      });
      await expect(
        adopter.storage.loadTerminalAdmission({
          harnessName: 'default',
          sessionId: ownerSession.id,
          admissionId,
          executionGrant: grant,
        }),
      ).resolves.toMatchObject({ status: 'committed', finalizerVersion: '1' });

      expect(adopter.agent.resumeCalls).toHaveLength(0);
      // A new turn on the recovering process is admitted under the primary @2.
      adopter.agent.enqueueRun({ finishReason: 'stop', text: 'next answer' });
      await session.message({
        content: 'next',
        admissionId: 'next-turn',
        executionAuthorityGrant: { key: 'usage-claim-next', generation: 1 },
        terminalAdmissionSeed: { v: 2 },
      });
      await expect(
        adopter.storage.loadTerminalAdmission({
          harnessName: 'default',
          sessionId: ownerSession.id,
          admissionId: 'next-turn',
          executionGrant: { key: 'usage-claim-next', generation: 1 },
        }),
      ).resolves.toMatchObject({ status: 'committed', finalizerVersion: '2' });
    } finally {
      await adopter.harness.shutdown();
    }
  });

  it('cancels, without finalizing, a parked turn of the previous version that is aborted deliberately', async () => {
    const db = new InMemoryDB();
    const calls: FinalizeCall[] = [];
    const ownerAgent = new MockAgent({ id: 'default' });
    ownerAgent.enqueueRun({
      finishReason: 'suspended',
      suspendPayload: { toolCallId: 'tc-1', toolName: 'shell', args: { cmd: 'ls' } },
    });
    const owner = harnessProcess(db, ownerAgent, { finalizer: finalizerAt('1', calls) });
    const ownerSession = await owner.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    await ownerSession.message({
      content: 'needs approval',
      admissionId,
      executionAuthorityGrant: grant,
      terminalAdmissionSeed: { v: 1 },
    });
    await owner.harness.shutdown();

    const resumer = harnessProcess(db, new MockAgent({ id: 'default' }), {
      finalizer: finalizerAt('2', calls),
      settleOnly: [finalizerAt('1', calls)],
    });
    try {
      const session = await resumer.harness.session({ sessionId: ownerSession.id, resourceId: 'u1' });
      await session.abortActiveWork();
      // A deliberate cancellation ends without a reply only as an explicit
      // cancellation: no finalizer runs and no intent is committed.
      await expect(
        resumer.storage.loadTerminalAdmission({
          harnessName: 'default',
          sessionId: ownerSession.id,
          admissionId,
          executionGrant: grant,
        }),
      ).resolves.toMatchObject({ status: 'cancelled', finalizerVersion: '1' });
      expect(calls).toEqual([]);
      await expect(resumer.storage.countPendingTerminalAdmissions(countFor('1'))).resolves.toBe(0);
      await expect(resumer.storage.countUndeliveredTerminalIntents(countFor('1'))).resolves.toBe(0);
    } finally {
      await resumer.harness.shutdown();
    }
  });

  it.each([
    {
      name: 'a settle-only entry equal to the primary',
      settleOnly: (calls: FinalizeCall[]) => [finalizerAt('2', calls)],
      field: 'sessions.terminalHandoff.settleOnly[0]',
    },
    {
      name: 'two settle-only entries with the same id@version',
      settleOnly: (calls: FinalizeCall[]) => [finalizerAt('1', calls), finalizerAt('1', calls)],
      field: 'sessions.terminalHandoff.settleOnly[1]',
    },
    {
      name: 'a settle-only entry without a version',
      settleOnly: (calls: FinalizeCall[]) => [{ ...finalizerAt('1', calls), version: '' }],
      field: 'sessions.terminalHandoff.settleOnly[0]',
    },
  ])('rejects $name at construction', ({ settleOnly, field }) => {
    const calls: FinalizeCall[] = [];
    let thrown: unknown;
    try {
      harnessProcess(new InMemoryDB(), new MockAgent({ id: 'default' }), {
        finalizer: finalizerAt('2', calls),
        settleOnly: settleOnly(calls),
      });
    } catch (error) {
      thrown = error;
    }
    expect(thrown).toBeInstanceOf(HarnessConfigError);
    expect((thrown as Error).message).toContain(field);
  });
});
