/**
 * Harness v1 — session adoption interrupts admitted `message()` dispatches whose
 * owning process died mid-run (PF-4598). The adopter never re-runs provider
 * work: the orphaned run is settled `harness.run_interrupted` and reported as a
 * reconstructed `run_completed{status:'interrupted'}`.
 */

import { afterEach, describe, expect, it, vi } from 'vitest';

import type { AgentSignalResultEvidence, HarnessTerminalFinalizer } from '../../storage/domains/harness';
import { InMemoryHarness } from '../../storage/domains/harness/inmemory';
import { InMemoryDB } from '../../storage/domains/inmemory-db';

import { MockAgent } from './__test-utils__/mock-agent';
import { HarnessSessionLockedError } from './errors';
import type { HarnessEvent } from './events';
import { Harness } from './harness';

const LEASE_AND_DISPATCH_CLAIM_LAPSED_MS = 31_000;

const finalizer: HarnessTerminalFinalizer = {
  id: 'doxa.chat',
  version: '2026-09-28',
  finalize: async ({ result }) => ({
    projectionKind: 'chat.summary',
    projectionId: result.runId,
    payload: { status: result.status, code: result.error?.code ?? null },
  }),
};

const grant = { key: 'usage-claim-orphaned', generation: 1 };

type DispatchShape = {
  name: string;
  terminal: boolean;
  message: Record<string, unknown>;
};

const plainAdmittedMessage: DispatchShape = {
  name: 'plain admitted message (fenced by the session lease)',
  terminal: false,
  message: { content: 'hi', admissionId: 'orphaned-turn' },
};

const terminalHandoffMessage: DispatchShape = {
  name: 'native terminal handoff (expired dispatch claim)',
  terminal: true,
  message: {
    content: 'hi',
    admissionId: 'orphaned-turn',
    executionAuthorityGrant: grant,
    terminalAdmissionSeed: { v: 1 },
  },
};

/** One Harness process over the shared durable store. */
function harnessProcess(
  db: InMemoryDB,
  agent: MockAgent,
  terminal: boolean,
  terminalFinalizer: HarnessTerminalFinalizer = finalizer,
  closeTimeoutMs?: number,
) {
  const storage = new InMemoryHarness({
    db,
    ...(terminal ? { terminalHandoff: { enabled: true }, sessionRecordProjection: { enabled: true } } : {}),
  });
  const harness = new Harness({
    agents: { default: agent } as any,
    modes: [{ id: 'default', agentId: 'default' }],
    defaultModeId: 'default',
    sessions: {
      storage,
      ...(closeTimeoutMs !== undefined ? { closeTimeoutMs } : {}),
      ...(terminal ? { terminalHandoff: { finalizer: terminalFinalizer } } : {}),
    },
  });
  return { harness, storage };
}

/** Start an admitted turn whose provider run never finishes in the owning process. */
async function dispatchHeldTurn(db: InMemoryDB, shape: DispatchShape) {
  const agent = new MockAgent({ id: 'default' });
  let release!: () => void;
  agent.enqueueRun({ holdUntil: new Promise<void>(resolve => (release = resolve)) });
  const owner = harnessProcess(db, agent, shape.terminal);
  const session = await owner.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
  const stream = (await session.message({ ...shape.message, stream: true } as never)) as { runId: string };
  expect(agent.streamCalls).toHaveLength(1);
  const scope = { harnessName: 'default', sessionId: session.id, resourceId: 'u1', threadId: session.threadId };
  const pending = await owner.storage.listPendingMessageAdmissions({ ...scope, now: Date.now(), limit: 10 });
  expect(pending.items).toHaveLength(1);
  const signalId = pending.items[0]!.evidence.signalId;
  return { ...owner, agent, scope, sessionId: session.id, runId: stream.runId, signalId, release };
}

/** A pending admitted message row with no dispatch claim and no live run anywhere. */
function orphanedAdmission(
  scope: { harnessName: string; sessionId: string; resourceId: string; threadId: string },
  tag: string,
): AgentSignalResultEvidence {
  return {
    ...scope,
    status: 'pending',
    signalId: `signal-${tag}`,
    runId: `run-${tag}`,
    operationKind: 'message',
    admissionId: tag,
    admissionHash: `hash-${tag}`,
    createdAt: Date.now(),
    updatedAt: Date.now(),
  };
}

async function stopDeadOwner(owner: { harness: Harness; release: () => void }) {
  owner.release();
  await owner.harness.shutdown().catch(() => {});
}

describe('Session adoption — orphaned message dispatch', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it.each([plainAdmittedMessage, terminalHandoffMessage])(
    'interrupts a crashed dispatch with no live run and never re-runs the provider: $name',
    async shape => {
      const db = new InMemoryDB();
      const deadOwner = await dispatchHeldTurn(db, shape);

      // The owning process dies mid-run: it stops renewing, so its session
      // lease (and any stamped dispatch claim) lapses.
      vi.useFakeTimers({ toFake: ['Date'] });
      vi.setSystemTime(Date.now() + LEASE_AND_DISPATCH_CLAIM_LAPSED_MS);

      const adopterAgent = new MockAgent({ id: 'default', defaultOutput: { text: 'must not run' } });
      const adopter = harnessProcess(db, adopterAgent, shape.terminal);
      const events: HarnessEvent[] = [];
      adopter.harness.subscribe(event => events.push(event));
      try {
        const session = await adopter.harness.session({ sessionId: deadOwner.sessionId, resourceId: 'u1' });

        expect(events).toContainEqual(
          expect.objectContaining({
            type: 'run_completed',
            runId: deadOwner.runId,
            status: 'interrupted',
            reconstructed: true,
          }),
        );
        await expect(
          adopter.storage.loadMessageResultEvidence({ ...deadOwner.scope, signalId: deadOwner.signalId }),
        ).resolves.toMatchObject({ status: 'failed', error: { code: 'harness.run_interrupted' } });
        // A same-admission retry replays the settled failure instead of
        // dispatching the provider again.
        await expect(session.message({ ...shape.message } as never)).rejects.toThrow();
        if (shape.terminal) {
          const admission = await adopter.storage.loadTerminalAdmission({
            harnessName: 'default',
            sessionId: deadOwner.sessionId,
            admissionId: 'orphaned-turn',
            executionGrant: grant,
          });
          expect(admission).toMatchObject({
            status: 'committed',
            terminalResult: { status: 'aborted', error: { code: 'harness.run_interrupted' } },
          });
        }
        expect(adopterAgent.streamCalls).toHaveLength(0);
        expect(adopterAgent.resumeCalls).toHaveLength(0);
      } finally {
        await adopter.harness.shutdown();
        await stopDeadOwner(deadOwner);
      }
    },
  );

  it('refuses to close while a dispatch claim is live, then finishes the crashed close', async () => {
    const db = new InMemoryDB();
    const deadOwner = await dispatchHeldTurn(db, terminalHandoffMessage);
    // The owner persisted its closing marker, then died before the close
    // drained; its session lease lapses before its dispatch claim does.
    const record = (await deadOwner.storage.loadSession({ sessionId: deadOwner.sessionId }))!;
    await deadOwner.storage.saveSession(
      { ...record, closingAt: Date.now(), closeDeadlineAt: Date.now() + 60_000 },
      { ownerId: deadOwner.harness.ownerId, ifVersion: record.version },
    );
    await deadOwner.storage.releaseSessionLease({ sessionId: deadOwner.sessionId, ownerId: deadOwner.harness.ownerId });
    // Another orphaned turn of the same closing session is not enough to list
    // it while one turn is still claimed.
    await deadOwner.storage.writeMessageResultEvidence(orphanedAdmission(deadOwner.scope, 'another-orphan'));

    const adopterAgent = new MockAgent({ id: 'default', defaultOutput: { text: 'must not run' } });
    const adopter = harnessProcess(db, adopterAgent, true);
    const events: HarnessEvent[] = [];
    adopter.harness.subscribe(event => events.push(event));
    const target = { sessionId: deadOwner.sessionId, resourceId: 'u1' };
    try {
      // Neither adoption nor close can advance it until the claim expires.
      await expect(adopter.storage.listRecoverableSessions({ now: Date.now(), limit: 10 })).resolves.toEqual({
        items: [],
      });
      // `harness.session()` rejects a closing session; the existing close API
      // resumes the persisted close, but not over a dispatch it cannot settle.
      await expect(adopter.harness.closeSession(target)).rejects.toBeInstanceOf(HarnessSessionLockedError);
      expect((await adopter.storage.loadSession({ sessionId: deadOwner.sessionId }))?.closedAt).toBeUndefined();

      vi.useFakeTimers({ toFake: ['Date'] });
      vi.setSystemTime(Date.now() + LEASE_AND_DISPATCH_CLAIM_LAPSED_MS);
      await expect(adopter.storage.listRecoverableSessions({ now: Date.now(), limit: 10 })).resolves.toMatchObject({
        items: [{ sessionId: deadOwner.sessionId, closing: true, pendingMessageAdmission: true }],
      });
      await adopter.harness.closeSession(target);

      expect(events).toContainEqual(
        expect.objectContaining({ type: 'run_completed', runId: deadOwner.runId, status: 'interrupted' }),
      );
      await expect(
        adopter.storage.loadMessageResultEvidence({ ...deadOwner.scope, signalId: deadOwner.signalId }),
      ).resolves.toMatchObject({ status: 'failed', error: { code: 'harness.run_interrupted' } });
      expect((await adopter.storage.loadSession({ sessionId: deadOwner.sessionId }))?.closedAt).toBeDefined();
      await expect(adopter.storage.listRecoverableSessions({ now: Date.now(), limit: 10 })).resolves.toEqual({
        items: [],
      });
      expect(adopterAgent.streamCalls).toHaveLength(0);
    } finally {
      await adopter.harness.shutdown();
      await stopDeadOwner(deadOwner);
    }
  });

  it('settles a pending admission whose run already has a summary without re-running the provider', async () => {
    const db = new InMemoryDB();
    const deadOwner = await dispatchHeldTurn(db, plainAdmittedMessage);
    // A summary proves only that a terminal was reported, not that it settled.
    await deadOwner.storage.saveRunSummary({
      summary: {
        harnessName: 'default',
        runId: deadOwner.runId,
        sessionId: deadOwner.sessionId,
        resourceId: 'u1',
        threadId: deadOwner.scope.threadId,
        agentId: 'default',
        modeId: 'default',
        modelId: 'model',
        status: 'failed',
        finishReason: 'error',
        reconstructed: false,
        completedAt: Date.now(),
        usage: { promptTokens: 0, completionTokens: 0, totalTokens: 0 },
        createdAt: Date.now(),
      },
    });

    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(Date.now() + LEASE_AND_DISPATCH_CLAIM_LAPSED_MS);

    const adopterAgent = new MockAgent({ id: 'default', defaultOutput: { text: 'must not run' } });
    const adopter = harnessProcess(db, adopterAgent, false);
    try {
      await adopter.harness.session({ sessionId: deadOwner.sessionId, resourceId: 'u1' });

      await expect(
        adopter.storage.loadMessageResultEvidence({ ...deadOwner.scope, signalId: deadOwner.signalId }),
      ).resolves.toMatchObject({ status: 'failed', error: { code: 'harness.run_interrupted' } });
      expect(adopterAgent.streamCalls).toHaveLength(0);
    } finally {
      await adopter.harness.shutdown();
      await stopDeadOwner(deadOwner);
    }
  });

  it.each([
    { shape: plainAdmittedMessage, method: 'compareAndSwapSignalTerminal' as const },
    { shape: terminalHandoffMessage, method: 'commitTerminalHandoff' as const },
  ])(
    'publishes the interrupted completion on a later adoption when the settlement acknowledgement is lost: $shape.name',
    async ({ shape, method }) => {
      const db = new InMemoryDB();
      const deadOwner = await dispatchHeldTurn(db, shape);
      vi.useFakeTimers({ toFake: ['Date'] });
      vi.setSystemTime(Date.now() + LEASE_AND_DISPATCH_CLAIM_LAPSED_MS);

      const adopterAgent = new MockAgent({ id: 'default', defaultOutput: { text: 'must not run' } });
      const adopter = harnessProcess(db, adopterAgent, shape.terminal);
      // The settlement commits, but its acknowledgement never reaches the adopter.
      const settle = (adopter.storage[method] as (input: unknown) => Promise<unknown>).bind(adopter.storage);
      let acknowledgementLost = false;
      (adopter.storage as unknown as Record<string, unknown>)[method] = async (input: unknown) => {
        const settled = await settle(input);
        if (acknowledgementLost) return settled;
        acknowledgementLost = true;
        throw new Error('settlement acknowledgement lost');
      };
      const events: HarnessEvent[] = [];
      adopter.harness.subscribe(event => events.push(event));
      const target = { sessionId: deadOwner.sessionId, resourceId: 'u1' };
      try {
        await expect(adopter.harness.session(target)).rejects.toThrow();
        await expect(
          adopter.storage.loadMessageResultEvidence({ ...deadOwner.scope, signalId: deadOwner.signalId }),
        ).resolves.toMatchObject({ status: 'failed', error: { code: 'harness.run_interrupted' } });

        await adopter.harness.session(target);

        const completions = events.filter(event => event.type === 'run_completed' && event.runId === deadOwner.runId);
        expect(completions).toEqual([expect.objectContaining({ status: 'interrupted', reconstructed: true })]);
        await expect(adopter.storage.loadRunSummary({ runId: deadOwner.runId })).resolves.toMatchObject({
          status: 'interrupted',
        });
        expect(adopterAgent.streamCalls).toHaveLength(0);

        // A warm close whose settlement of a later orphan fails transiently is
        // refused, and the next resolution retries the recovery.
        const lateOrphan = orphanedAdmission(deadOwner.scope, 'late-orphan');
        await adopter.storage.writeMessageResultEvidence(lateOrphan);
        const settleLate = adopter.storage.compareAndSwapSignalTerminal.bind(adopter.storage);
        let settlementFailed = false;
        adopter.storage.compareAndSwapSignalTerminal = async input => {
          if (settlementFailed) return settleLate(input);
          settlementFailed = true;
          throw new Error('transient settlement failure');
        };
        await expect(adopter.harness.closeSession(target)).rejects.toThrow();
        await adopter.harness.session(target);
        await expect(
          adopter.storage.loadMessageResultEvidence({ ...deadOwner.scope, signalId: lateOrphan.signalId }),
        ).resolves.toMatchObject({ status: 'failed', error: { code: 'harness.run_interrupted' } });
      } finally {
        await adopter.harness.shutdown();
        await stopDeadOwner(deadOwner);
      }
    },
  );

  it('does not interrupt a dispatch that a zombie owner re-claims while the finalizer runs', async () => {
    const db = new InMemoryDB();
    const deadOwner = await dispatchHeldTurn(db, terminalHandoffMessage);
    const observed = (await deadOwner.storage.loadMessageResultEvidence({
      ...deadOwner.scope,
      signalId: deadOwner.signalId,
    })) as AgentSignalResultEvidence;
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(Date.now() + LEASE_AND_DISPATCH_CLAIM_LAPSED_MS);

    let reclaims = 0;
    const zombieReclaimingFinalizer: HarnessTerminalFinalizer = {
      ...finalizer,
      finalize: async input => {
        // The stalled owner resumes and stamps a fresh claim before the
        // adopter's interrupted settlement commits.
        const current = (await deadOwner.storage.loadMessageResultEvidence({
          ...deadOwner.scope,
          signalId: deadOwner.signalId,
        })) as AgentSignalResultEvidence;
        reclaims += 1;
        await deadOwner.storage.compareAndSwapSignalDispatch({
          ...deadOwner.scope,
          signalId: deadOwner.signalId,
          admissionId: observed.admissionId!,
          admissionHash: observed.admissionHash!,
          operationKind: 'message',
          expected: current.dispatch!,
          next: {
            ...current.dispatch!,
            attemptId: `zombie-attempt-${reclaims}`,
            claimExpiresAt: Date.now() + 30_000,
          } as never,
          updatedAt: Date.now(),
        });
        return finalizer.finalize(input);
      },
    };
    const adopterAgent = new MockAgent({ id: 'default', defaultOutput: { text: 'must not run' } });
    const adopter = harnessProcess(db, adopterAgent, true, zombieReclaimingFinalizer);
    const events: HarnessEvent[] = [];
    adopter.harness.subscribe(event => events.push(event));
    try {
      await adopter.harness.session({ sessionId: deadOwner.sessionId, resourceId: 'u1' });

      expect(events).not.toContainEqual(expect.objectContaining({ type: 'run_completed', runId: deadOwner.runId }));
      await expect(
        adopter.storage.loadMessageResultEvidence({ ...deadOwner.scope, signalId: deadOwner.signalId }),
      ).resolves.toMatchObject({ status: 'pending', dispatch: { attemptId: 'zombie-attempt-1' } });
      await expect(
        adopter.storage.loadTerminalAdmission({
          harnessName: 'default',
          sessionId: deadOwner.sessionId,
          admissionId: 'orphaned-turn',
          executionGrant: grant,
        }),
      ).resolves.toMatchObject({ status: 'pending' });
      expect(adopterAgent.streamCalls).toHaveLength(0);

      // A close whose own settlement loses to a fresh claim must not close over
      // the still-pending turn.
      vi.setSystemTime(Date.now() + LEASE_AND_DISPATCH_CLAIM_LAPSED_MS);
      // The adopter's renewal loop keeps its lease across the jump.
      await adopter.storage.acquireSessionLease({
        sessionId: deadOwner.sessionId,
        ownerId: adopter.harness.ownerId,
        ttlMs: 60_000,
      });
      await expect(
        adopter.harness.closeSession({ sessionId: deadOwner.sessionId, resourceId: 'u1' }),
      ).rejects.toBeInstanceOf(HarnessSessionLockedError);
      expect(reclaims).toBe(2);
      expect((await adopter.storage.loadSession({ sessionId: deadOwner.sessionId }))?.closedAt).toBeUndefined();
      await expect(
        adopter.storage.loadMessageResultEvidence({ ...deadOwner.scope, signalId: deadOwner.signalId }),
      ).resolves.toMatchObject({ status: 'pending', dispatch: { attemptId: 'zombie-attempt-2' } });
    } finally {
      await adopter.harness.shutdown();
      await stopDeadOwner(deadOwner);
    }
  });

  it('leaves a dispatch whose claim is still valid untouched', async () => {
    const db = new InMemoryDB();
    const liveOwner = await dispatchHeldTurn(db, terminalHandoffMessage);

    // The session lease is released while the dispatch claim is still valid.
    await liveOwner.storage.releaseSessionLease({ sessionId: liveOwner.sessionId, ownerId: liveOwner.harness.ownerId });

    const adopterAgent = new MockAgent({ id: 'default', defaultOutput: { text: 'must not run' } });
    const adopter = harnessProcess(db, adopterAgent, true);
    const events: HarnessEvent[] = [];
    adopter.harness.subscribe(event => events.push(event));
    try {
      await adopter.harness.session({ sessionId: liveOwner.sessionId, resourceId: 'u1' });

      expect(events).not.toContainEqual(expect.objectContaining({ type: 'run_completed', runId: liveOwner.runId }));
      const admission = await adopter.storage.loadTerminalAdmission({
        harnessName: 'default',
        sessionId: liveOwner.sessionId,
        admissionId: 'orphaned-turn',
        executionGrant: grant,
      });
      expect(admission?.status).toBe('pending');
      expect(adopterAgent.streamCalls).toHaveLength(0);

      // Once the claim expires, the warm session is recovered on its next
      // access. Its first completion write fails transiently…
      vi.useFakeTimers({ toFake: ['Date'] });
      vi.setSystemTime(Date.now() + LEASE_AND_DISPATCH_CLAIM_LAPSED_MS);
      // The adopter's renewal loop keeps its lease across the jump.
      await adopter.storage.acquireSessionLease({
        sessionId: liveOwner.sessionId,
        ownerId: adopter.harness.ownerId,
        ttlMs: 60_000,
      });
      const saveRunSummary = adopter.storage.saveRunSummary.bind(adopter.storage);
      let summaryWriteFailed = false;
      adopter.storage.saveRunSummary = async input => {
        if (summaryWriteFailed) return saveRunSummary(input);
        summaryWriteFailed = true;
        throw new Error('transient run summary write failure');
      };
      const target = { sessionId: liveOwner.sessionId, resourceId: 'u1' };
      await expect(adopter.harness.session(target)).rejects.toThrow();
      await expect(
        adopter.storage.loadMessageResultEvidence({ ...liveOwner.scope, signalId: liveOwner.signalId }),
      ).resolves.toMatchObject({ status: 'failed', error: { code: 'harness.run_interrupted' } });
      // …and the same warm session retries it on the next access.
      await adopter.harness.session(target);
      await expect(adopter.storage.loadRunSummary({ runId: liveOwner.runId })).resolves.toMatchObject({
        status: 'interrupted',
      });
      expect(events).toContainEqual(
        expect.objectContaining({ type: 'run_completed', runId: liveOwner.runId, status: 'interrupted' }),
      );
      expect(adopterAgent.streamCalls).toHaveLength(0);
    } finally {
      await adopter.harness.shutdown();
      await stopDeadOwner(liveOwner);
    }
  });

  it.each([
    { shape: plainAdmittedMessage, stallAfter: 'reservation' as const },
    { shape: terminalHandoffMessage, stallAfter: 'reservation' as const },
    { shape: plainAdmittedMessage, stallAfter: 'claim' as const },
    { shape: terminalHandoffMessage, stallAfter: 'claim' as const },
  ])(
    'never dispatches a turn that was interrupted while its owner stalled after the $stallAfter: $shape.name',
    async ({ shape, stallAfter }) => {
      const db = new InMemoryDB();
      const ownerAgent = new MockAgent({ id: 'default' });
      const owner = harnessProcess(db, ownerAgent, shape.terminal);
      const session = await owner.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      // The owner commits its reservation (or its dispatch claim), then stalls
      // before the write's acknowledgement returns.
      let stalled!: () => void;
      const reservationPersisted = new Promise<void>(resolve => (stalled = resolve));
      let resume!: () => void;
      const resumed = new Promise<void>(resolve => (resume = resolve));
      if (stallAfter === 'reservation') {
        const reserve = owner.storage.writeMessageResultEvidence.bind(owner.storage);
        owner.storage.writeMessageResultEvidence = async record => {
          const written = await reserve(record);
          if (record.status === 'pending') {
            stalled();
            await resumed;
          }
          return written;
        };
      } else {
        const claim = owner.storage.compareAndSwapSignalDispatch.bind(owner.storage);
        owner.storage.compareAndSwapSignalDispatch = async input => {
          const claimed = await claim(input);
          if (input.next.state === 'dispatching') {
            stalled();
            await resumed;
          }
          return claimed;
        };
      }
      const turn = session.message({ ...shape.message, stream: true } as never);
      void turn.catch(() => {});
      await reservationPersisted;
      const scope = { harnessName: 'default', sessionId: session.id, resourceId: 'u1', threadId: session.threadId };
      const [reserved] = (await owner.storage.listPendingMessageAdmissions({ ...scope, now: Date.now(), limit: 10 }))
        .items;

      vi.useFakeTimers({ toFake: ['Date'] });
      vi.setSystemTime(Date.now() + LEASE_AND_DISPATCH_CLAIM_LAPSED_MS);
      const adopterAgent = new MockAgent({ id: 'default', defaultOutput: { text: 'must not run' } });
      const adopter = harnessProcess(db, adopterAgent, shape.terminal);
      try {
        await adopter.harness.session({ sessionId: session.id, resourceId: 'u1' });
        await expect(
          adopter.storage.loadMessageResultEvidence({ ...scope, signalId: reserved!.evidence.signalId }),
        ).resolves.toMatchObject({ status: 'failed', error: { code: 'harness.run_interrupted' } });

        resume();
        await expect(turn).rejects.toThrow();
        expect(ownerAgent.streamCalls).toHaveLength(0);
        if (shape.terminal) {
          // No terminal admission is left pending behind the interrupted turn.
          const admission = await adopter.storage.loadTerminalAdmission({
            harnessName: 'default',
            sessionId: session.id,
            admissionId: 'orphaned-turn',
            executionGrant: grant,
          });
          expect(admission?.status).not.toBe('pending');
        }
        expect(adopterAgent.streamCalls).toHaveLength(0);
      } finally {
        resume();
        await adopter.harness.shutdown();
        await owner.harness.shutdown().catch(() => {});
      }
    },
  );

  it.each(['before the close', 'during the close drain'] as const)(
    'refuses to close over a turn that finished %s but whose result write failed',
    async finishes => {
      const db = new InMemoryDB();
      const agent = new MockAgent({ id: 'default' });
      let finishRun!: () => void;
      if (finishes === 'during the close drain') {
        agent.enqueueRun({ holdUntil: new Promise<void>(resolve => (finishRun = resolve)) });
      }
      const owner = harnessProcess(db, agent, false);
      const session = await owner.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const scope = { harnessName: 'default', sessionId: session.id, resourceId: 'u1', threadId: session.threadId };
      const write = owner.storage.writeMessageResultEvidence.bind(owner.storage);
      owner.storage.writeMessageResultEvidence = async record => {
        if (record.status === 'completed') throw new Error('completed result write failed');
        return write(record);
      };
      const target = { sessionId: session.id, resourceId: 'u1' };
      try {
        const turn = session.message({ content: 'hi', admissionId: 'finished-turn' });
        void turn.catch(() => {});
        let close: Promise<void>;
        if (finishes === 'before the close') {
          await expect(turn).rejects.toThrow();
          close = owner.harness.closeSession(target);
        } else {
          // The run is still in flight when close starts, and finishes while
          // close drains it.
          await vi.waitFor(() => expect(agent.streamCalls).toHaveLength(1));
          close = owner.harness.closeSession(target);
          await vi.waitFor(() => expect(session.isClosing).toBe(true));
          finishRun();
          await expect(turn).rejects.toThrow();
        }

        // The run finished here, but its admission is still pending: closing
        // now would hide it from every later recovery.
        await expect(close).rejects.toBeInstanceOf(HarnessSessionLockedError);
        expect((await owner.storage.loadSession({ sessionId: session.id }))?.closedAt).toBeUndefined();
        const pending = await owner.storage.listPendingMessageAdmissions({ ...scope, now: Date.now(), limit: 10 });
        expect(pending.items).toMatchObject([{ evidence: { status: 'pending' } }]);
      } finally {
        await owner.harness.shutdown().catch(() => {});
      }
    },
  );

  it.each([
    { shape: plainAdmittedMessage, stream: true, resultWriteDelayMs: 0 },
    { shape: terminalHandoffMessage, stream: true, resultWriteDelayMs: 0 },
    { shape: plainAdmittedMessage, stream: false, resultWriteDelayMs: 50 },
  ])(
    'still closes after aborting an in-flight turn at the close deadline: $shape.name (stream: $stream)',
    async ({ shape, stream, resultWriteDelayMs }) => {
      const db = new InMemoryDB();
      const agent = new MockAgent({ id: 'default' });
      agent.enqueueRun({ holdUntil: new Promise<void>(() => {}) });
      const owner = harnessProcess(db, agent, shape.terminal, finalizer, 100);
      if (resultWriteDelayMs > 0) {
        // The aborted turn's result write lands a little after the drain ends.
        const write = owner.storage.writeMessageResultEvidence.bind(owner.storage);
        owner.storage.writeMessageResultEvidence = async record => {
          if (record.status !== 'pending') await new Promise(resolve => setTimeout(resolve, resultWriteDelayMs));
          return write(record);
        };
      }
      const session = await owner.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      try {
        const turn = session.message({ ...shape.message, ...(stream ? { stream: true } : {}) } as never);
        void turn.catch(() => {});
        await vi.waitFor(() => expect(agent.streamCalls).toHaveLength(1));

        // The aborted turn records its own result; close does not refuse it.
        await owner.harness.closeSession({ sessionId: session.id, resourceId: 'u1' });
        expect((await owner.storage.loadSession({ sessionId: session.id }))?.closedAt).toBeDefined();
      } finally {
        await owner.harness.shutdown().catch(() => {});
      }
    },
  );

  it('does not interrupt a same-admission retry while it re-drives its terminal admission', async () => {
    const db = new InMemoryDB();
    const agent = new MockAgent({ id: 'default' });
    const owner = harnessProcess(db, agent, true);
    const session = await owner.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const scope = { harnessName: 'default', sessionId: session.id, resourceId: 'u1', threadId: session.threadId };
    // The first attempt reserves the turn, then its terminal admission fails
    // transiently; the retry re-drives the admission and stalls inside it.
    const admit = owner.storage.admitTerminalHandoff.bind(owner.storage);
    let admissions = 0;
    let admitting!: () => void;
    const retryAdmitting = new Promise<void>(resolve => (admitting = resolve));
    let releaseAdmit!: () => void;
    const admitReleased = new Promise<void>(resolve => (releaseAdmit = resolve));
    let admitted!: () => void;
    const admissionInserted = new Promise<void>(resolve => (admitted = resolve));
    let releaseAck!: () => void;
    const ackReleased = new Promise<void>(resolve => (releaseAck = resolve));
    owner.storage.admitTerminalHandoff = async (input, opts) => {
      admissions += 1;
      if (admissions === 1) throw new Error('transient terminal admission failure');
      admitting();
      await admitReleased;
      const receipt = await admit(input, opts);
      admitted();
      await ackReleased;
      return receipt;
    };
    // Recovery that finds no admission yet lets the retry insert it, then
    // settles before the retry's acknowledgement (and its claim) returns.
    const lookup = owner.storage.loadTerminalAdmissionByRun.bind(owner.storage);
    owner.storage.loadTerminalAdmissionByRun = async input => {
      const found = await lookup(input);
      if (found === null) {
        releaseAdmit();
        await admissionInserted;
      }
      return found;
    };
    const settle = owner.storage.compareAndSwapSignalTerminal.bind(owner.storage);
    owner.storage.compareAndSwapSignalTerminal = async input => {
      const settled = await settle(input);
      releaseAck();
      return settled;
    };
    try {
      await expect(session.message({ ...terminalHandoffMessage.message } as never)).rejects.toThrow();
      const retry = session.message({ ...terminalHandoffMessage.message, stream: true } as never);
      void retry.catch(() => {});
      await retryAdmitting;
      const close = owner.harness.closeSession({ sessionId: session.id, resourceId: 'u1' });
      void close.catch(() => {});
      // Resume the retry only once close's pre-drain recovery has run.
      await vi.waitFor(async () =>
        expect((await owner.storage.loadSession({ sessionId: session.id }))?.closingAt).toBeDefined(),
      );
      releaseAdmit();
      releaseAck();

      // The retry owned its turn, so recovery never interrupted it under the
      // retry: its evidence and its terminal admission stay pending together
      // (it fails closed on the closing session), and close refuses rather
      // than closing over them.
      await expect(close).rejects.toBeInstanceOf(HarnessSessionLockedError);
      await expect(retry).rejects.toThrow();
      const [pending] = (await owner.storage.listPendingMessageAdmissions({ ...scope, now: Date.now(), limit: 10 }))
        .items;
      expect(pending?.evidence.status).toBe('pending');
      await expect(
        owner.storage.loadTerminalAdmission({
          harnessName: 'default',
          sessionId: session.id,
          admissionId: 'orphaned-turn',
          executionGrant: grant,
        }),
      ).resolves.toMatchObject({ status: 'pending' });
      expect((await owner.storage.loadSession({ sessionId: session.id }))?.closedAt).toBeUndefined();

      // Recovery's fallback settlement never lands over a pending terminal
      // admission, atomically in the adapter.
      const extra = orphanedAdmission(scope, 'admitted-extra');
      await owner.storage.writeMessageResultEvidence(extra);
      const record = (await owner.storage.loadSession({ sessionId: session.id }))!;
      await admit(
        {
          ...scope,
          sessionIncarnation: record.sessionIncarnation!,
          admissionId: extra.admissionId!,
          admissionHash: extra.admissionHash!,
          signalId: extra.signalId,
          runId: extra.runId!,
          executionGrant: { key: 'grant-admitted-extra', generation: 1 },
          finalizerId: finalizer.id,
          finalizerVersion: finalizer.version,
          seed: { v: 1 },
        },
        {},
      );
      await expect(
        owner.storage.compareAndSwapSignalTerminal({
          ...scope,
          signalId: extra.signalId,
          admissionId: extra.admissionId!,
          admissionHash: extra.admissionHash!,
          operationKind: 'message',
          expected: { state: 'reserved' },
          leaseOwner: { ownerId: record.ownerId! },
          terminal: {
            status: 'failed',
            signalId: extra.signalId,
            error: { code: 'harness.run_interrupted', message: 'x' },
          },
          updatedAt: Date.now(),
        }),
      ).resolves.toMatchObject({ applied: false });
    } finally {
      releaseAdmit();
      releaseAck();
      await owner.harness.shutdown().catch(() => {});
    }
  });

  it('refuses to close, warm or cold, while a suspended turn is parked for a response', async () => {
    const db = new InMemoryDB();
    const agent = new MockAgent({ id: 'default' });
    agent.enqueueRun({
      finishReason: 'suspended',
      suspendPayload: { toolCallId: 'tc-1', toolName: 'shell', args: { cmd: 'ls' } },
    });
    const owner = harnessProcess(db, agent, true);
    const session = await owner.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const target = { sessionId: session.id, resourceId: 'u1' };
    const adopter = harnessProcess(db, new MockAgent({ id: 'default' }), true);
    try {
      const result = (await session.message({ ...terminalHandoffMessage.message } as never)) as {
        finishReason: string;
      };
      expect(result.finishReason).toBe('suspended');

      // Its admission waits for the user's response: closing now would close
      // over pending evidence and a pending terminal admission.
      await expect(owner.harness.closeSession(target)).rejects.toBeInstanceOf(HarnessSessionLockedError);
      await owner.storage.releaseSessionLease({ sessionId: session.id, ownerId: owner.harness.ownerId });
      await expect(adopter.harness.closeSession(target)).rejects.toBeInstanceOf(HarnessSessionLockedError);

      expect((await owner.storage.loadSession({ sessionId: session.id }))?.closedAt).toBeUndefined();
      await expect(
        owner.storage.loadTerminalAdmission({
          harnessName: 'default',
          sessionId: session.id,
          admissionId: 'orphaned-turn',
          executionGrant: grant,
        }),
      ).resolves.toMatchObject({ status: 'pending' });

      // A crashed close left it closing: neither adoption nor close can
      // advance a parked admission, so discovery does not list it.
      const record = (await owner.storage.loadSession({ sessionId: session.id }))!;
      await owner.storage.saveSession(
        { ...record, closingAt: Date.now(), closeDeadlineAt: Date.now() + 60_000 },
        { ownerId: 'crashed-closer', ifVersion: record.version },
      );
      await expect(owner.storage.listRecoverableSessions({ now: Date.now(), limit: 10 })).resolves.toEqual({
        items: [],
      });
    } finally {
      await adopter.harness.shutdown().catch(() => {});
      await owner.harness.shutdown().catch(() => {});
    }
  });

  it('reopens a session whose turn parks during the close drain, so the turn stays answerable', async () => {
    const db = new InMemoryDB();
    const agent = new MockAgent({ id: 'default' });
    let park!: () => void;
    agent.enqueueRun({
      holdUntil: new Promise<void>(resolve => (park = resolve)),
      finishReason: 'suspended',
      suspendPayload: { toolCallId: 'tc-1', toolName: 'shell', args: { cmd: 'ls' } },
    });
    agent.enqueueRun({ finishReason: 'stop', text: 'answered' });
    const owner = harnessProcess(db, agent, true);
    const session = await owner.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const target = { sessionId: session.id, resourceId: 'u1' };
    try {
      const turn = session.message({ ...terminalHandoffMessage.message } as never);
      await vi.waitFor(() => expect(agent.streamCalls).toHaveLength(1));
      const close = owner.harness.closeSession(target);
      await vi.waitFor(async () =>
        expect((await owner.storage.loadSession({ sessionId: session.id }))?.closingAt).toBeDefined(),
      );
      park();
      await expect(turn).resolves.toMatchObject({ finishReason: 'suspended' });

      // Close refuses over the parked turn and restores the session.
      await expect(close).rejects.toBeInstanceOf(HarnessSessionLockedError);
      expect((await owner.storage.loadSession({ sessionId: session.id }))?.closingAt).toBeUndefined();
      expect(session.lifecycleState).toBe('live');

      await session.respondToToolApproval({ approved: true });
      await owner.harness.closeSession(target);
      expect((await owner.storage.loadSession({ sessionId: session.id }))?.closedAt).toBeDefined();
    } finally {
      await owner.harness.shutdown().catch(() => {});
    }
  });

  it('reopens the parent a subtree close marked when a child turn parked for a response refuses it', async () => {
    const db = new InMemoryDB();
    const agent = new MockAgent({ id: 'default' });
    agent.enqueueRun({
      finishReason: 'suspended',
      suspendPayload: { toolCallId: 'tc-1', toolName: 'shell', args: { cmd: 'ls' } },
    });
    const owner = harnessProcess(db, agent, true);
    const parent = await owner.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const child = await owner.harness.session({
      resourceId: 'u1',
      threadId: { fresh: true },
      parentSessionId: parent.id,
    });
    try {
      await expect(child.message({ ...terminalHandoffMessage.message } as never)).resolves.toMatchObject({
        finishReason: 'suspended',
      });

      await expect(owner.harness.closeSession({ sessionId: parent.id, resourceId: 'u1' })).rejects.toBeInstanceOf(
        HarnessSessionLockedError,
      );
      expect((await owner.storage.loadSession({ sessionId: parent.id }))?.closingAt).toBeUndefined();
      expect(parent.lifecycleState).toBe('live');
      expect(child.lifecycleState).toBe('live');
    } finally {
      await owner.harness.shutdown().catch(() => {});
    }
  });

  it('refuses to close while a result write is still in flight, then closes once it lands', async () => {
    const db = new InMemoryDB();
    const agent = new MockAgent({ id: 'default' });
    const owner = harnessProcess(db, agent, false, finalizer, 100);
    const session = await owner.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const scope = { harnessName: 'default', sessionId: session.id, resourceId: 'u1', threadId: session.threadId };
    const target = { sessionId: session.id, resourceId: 'u1' };
    // The turn's reservation write is held past the post-drain wait.
    const write = owner.storage.writeMessageResultEvidence.bind(owner.storage);
    let writing!: () => void;
    const reservationStarted = new Promise<void>(resolve => (writing = resolve));
    let land!: () => void;
    const landed = new Promise<void>(resolve => (land = resolve));
    let held = false;
    owner.storage.writeMessageResultEvidence = async record => {
      if (record.status === 'pending' && !held) {
        held = true;
        writing();
        await landed;
      }
      return write(record);
    };
    try {
      const turn = session.message({ content: 'hi', admissionId: 'held-reservation' });
      void turn.catch(() => {});
      await reservationStarted;

      await expect(owner.harness.closeSession(target)).rejects.toBeInstanceOf(HarnessSessionLockedError);
      expect((await owner.storage.loadSession({ sessionId: session.id }))?.closedAt).toBeUndefined();

      land();
      await vi.waitFor(async () =>
        expect(
          (await owner.storage.listPendingMessageAdmissions({ ...scope, now: Date.now(), limit: 10 })).items,
        ).toHaveLength(1),
      );
      await owner.harness.closeSession(target);
      expect((await owner.storage.loadSession({ sessionId: session.id }))?.closedAt).toBeDefined();
      const [settled] = (await owner.storage.listPendingMessageAdmissions({ ...scope, now: Date.now(), limit: 10 }))
        .items;
      expect(settled).toBeUndefined();
      expect(agent.streamCalls).toHaveLength(0);
    } finally {
      land();
      await owner.harness.shutdown().catch(() => {});
    }
  }, 20_000);

  it.each([plainAdmittedMessage, terminalHandoffMessage])(
    'refuses to close over a turn whose claim acknowledgement arrives after close started: $name',
    async shape => {
      const db = new InMemoryDB();
      const ownerAgent = new MockAgent({ id: 'default' });
      const owner = harnessProcess(db, ownerAgent, shape.terminal);
      const session = await owner.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const scope = { harnessName: 'default', sessionId: session.id, resourceId: 'u1', threadId: session.threadId };
      let stalled!: () => void;
      const claimCommitted = new Promise<void>(resolve => (stalled = resolve));
      let resume!: () => void;
      const resumed = new Promise<void>(resolve => (resume = resolve));
      const claim = owner.storage.compareAndSwapSignalDispatch.bind(owner.storage);
      owner.storage.compareAndSwapSignalDispatch = async input => {
        const claimed = await claim(input);
        if (input.next.state === 'dispatching') {
          stalled();
          await resumed;
        }
        return claimed;
      };
      try {
        const turn = session.message({ ...shape.message, stream: true } as never);
        void turn.catch(() => {});
        await claimCommitted;
        const close = owner.harness.closeSession({ sessionId: session.id, resourceId: 'u1' });
        void close.catch(() => {});
        await vi.waitFor(() => expect(session.isClosing).toBe(true));
        resume();

        await expect(turn).rejects.toThrow();
        expect(ownerAgent.streamCalls).toHaveLength(0);
        await expect(close).rejects.toBeInstanceOf(HarnessSessionLockedError);
        expect((await owner.storage.loadSession({ sessionId: session.id }))?.closedAt).toBeUndefined();
        const pending = await owner.storage.listPendingMessageAdmissions({ ...scope, now: Date.now(), limit: 10 });
        expect(pending.items).toMatchObject([{ evidence: { status: 'pending' } }]);
      } finally {
        resume();
        await owner.harness.shutdown().catch(() => {});
      }
    },
  );

  it('does not commit an interruption after its lease expired while the finalizer ran', async () => {
    const db = new InMemoryDB();
    const deadOwner = await dispatchHeldTurn(db, terminalHandoffMessage);
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(Date.now() + LEASE_AND_DISPATCH_CLAIM_LAPSED_MS);
    const slowFinalizer: HarnessTerminalFinalizer = {
      ...finalizer,
      finalize: async input => {
        // The finalizer outlives the adopter's lease.
        vi.setSystemTime(Date.now() + 5 * 60_000);
        return finalizer.finalize(input);
      },
    };
    const adopterAgent = new MockAgent({ id: 'default', defaultOutput: { text: 'must not run' } });
    const adopter = harnessProcess(db, adopterAgent, true, slowFinalizer);
    try {
      await adopter.harness.session({ sessionId: deadOwner.sessionId, resourceId: 'u1' });

      await expect(
        adopter.storage.loadMessageResultEvidence({ ...deadOwner.scope, signalId: deadOwner.signalId }),
      ).resolves.toMatchObject({ status: 'pending' });
      await expect(
        adopter.storage.loadTerminalAdmission({
          harnessName: 'default',
          sessionId: deadOwner.sessionId,
          admissionId: 'orphaned-turn',
          executionGrant: grant,
        }),
      ).resolves.toMatchObject({ status: 'pending' });
    } finally {
      await adopter.harness.shutdown();
      await stopDeadOwner(deadOwner);
    }
  });
});
