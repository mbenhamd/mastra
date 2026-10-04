/**
 * Harness v1 — native terminal handoff fencing (PF-4981).
 *
 * - A grant revoked before admission never dispatches.
 * - The dispatch stamp is fenced on the session lease, and the final claim
 *   check is the last step before the provider dispatch.
 * - A dispatched run whose own output rejects (a provider error) commits a
 *   durable `failed` terminal with its delivery intent, so neither a
 *   same-admission retry nor a later adoption re-runs or relabels it.
 */

import { afterEach, describe, expect, it, vi } from 'vitest';

import { Agent } from '../../agent';
import { MockLanguageModelV2 } from '../../agent/__tests__/mock-model';
import { InMemoryStore } from '../../storage';
import type {
  AgentSignalResultEvidence,
  HarnessTerminalCommitReceipt,
  HarnessTerminalFinalizer,
  HarnessTerminalFinalizerInput,
} from '../../storage/domains/harness';
import { HarnessTerminalHandoffCancelledError, harnessTerminalIntentId } from '../../storage/domains/harness';
import { InMemoryHarness } from '../../storage/domains/harness/inmemory';
import { InMemoryDB } from '../../storage/domains/inmemory-db';

import { MockAgent } from './__test-utils__/mock-agent';
import { HarnessSessionLockedError } from './errors';
import type { HarnessEvent } from './events';
import { Harness } from './harness';

const LEASE_AND_DISPATCH_CLAIM_LAPSED_MS = 31_000;
const grant = { key: 'usage-claim-fencing', generation: 1 };

function recordingFinalizer(calls: HarnessTerminalFinalizerInput[] = []): HarnessTerminalFinalizer {
  return {
    id: 'doxa.chat',
    version: '2026-10-03',
    finalize: async input => {
      calls.push(input);
      return {
        projectionKind: 'chat.summary',
        projectionId: input.result.runId,
        payload: { status: input.result.status, code: input.result.error?.code ?? null },
      };
    },
  };
}

/** One Harness process over the shared durable store. */
function harnessProcess(
  db: InMemoryDB,
  agent: Agent<any, any, any>,
  finalizer: HarnessTerminalFinalizer = recordingFinalizer(),
) {
  const storage = new InMemoryHarness({
    db,
    terminalHandoff: { enabled: true },
    sessionRecordProjection: { enabled: true },
  });
  const harness = new Harness({
    agents: { default: agent } as any,
    storage: new InMemoryStore(),
    modes: [{ id: 'default', agentId: 'default' }],
    defaultModeId: 'default',
    sessions: { storage, terminalHandoff: { finalizer } },
  });
  return { harness, storage };
}

function terminalMessage(extra: Record<string, unknown> = {}) {
  return {
    content: 'hi',
    admissionId: 'turn-1',
    executionAuthorityGrant: grant,
    terminalAdmissionSeed: { v: 1 },
    ...extra,
  };
}

async function evidenceFor(storage: InMemoryHarness, sessionId: string, threadId: string) {
  const rows = [...(storage as any).db.harnessMessageResultEvidence.values()] as AgentSignalResultEvidence[];
  const row = rows.find(r => r.sessionId === sessionId && r.threadId === threadId && r.admissionId === 'turn-1');
  if (row === undefined) return undefined;
  return storage.loadMessageResultEvidence({
    harnessName: 'default',
    sessionId,
    resourceId: row.resourceId,
    threadId,
    signalId: row.signalId,
  }) as Promise<AgentSignalResultEvidence | null>;
}

async function admissionFor(storage: InMemoryHarness, sessionId: string) {
  return storage.loadTerminalAdmission({
    harnessName: 'default',
    sessionId,
    admissionId: 'turn-1',
    executionGrant: grant,
  });
}

/** A real Agent whose provider rejects every call (counted). */
function rejectingProviderAgent() {
  const provider = { calls: 0 };
  const model = new MockLanguageModelV2({
    doStream: async () => {
      provider.calls += 1;
      throw new Error('provider rejected: upstream 503 from internal host');
    },
  });
  const agent = new Agent({ id: 'default', name: 'default', instructions: 'reply', model });
  return { agent, provider };
}

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe('pre-admission grant revocation', () => {
  it('refuses message() before any provider call when the grant was revoked before admission', async () => {
    const db = new InMemoryDB();
    const agent = new MockAgent({ id: 'default' });
    const { harness, storage } = harnessProcess(db, agent);
    try {
      const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      await expect(
        storage.revokeTerminalGrant({
          harnessName: 'default',
          sessionId: session.id,
          admissionId: 'turn-1',
          executionGrant: grant,
          reason: { code: 'doxa.turn_released', message: 'released before admission' },
        }),
      ).resolves.toMatchObject({ status: 'revoked' });

      const failures: Error[] = [];
      await expect(
        session.message(terminalMessage({ onTerminalCommitError: (err: Error) => failures.push(err) }) as never),
      ).rejects.toBeInstanceOf(HarnessTerminalHandoffCancelledError);
      expect(agent.streamCalls).toHaveLength(0);
      expect(failures.map(err => err.name)).toEqual(['HarnessTerminalHandoffError:harness.terminal_cancelled']);
      await expect(admissionFor(storage, session.id)).resolves.toBeNull();
      // The reservation is settled with the cancellation instead of staying
      // pending (it would block close and later be published as interrupted).
      await expect(evidenceFor(storage, session.id, session.threadId)).resolves.toMatchObject({
        status: 'failed',
        error: { code: 'harness.terminal_cancelled' },
      });

      // A same-admission retry replays the cancellation and still never dispatches.
      await expect(session.message(terminalMessage() as never)).rejects.toBeInstanceOf(
        HarnessTerminalHandoffCancelledError,
      );
      expect(agent.streamCalls).toHaveLength(0);
      await expect(harness.closeSession({ sessionId: session.id, resourceId: 'u1' })).resolves.toBeUndefined();
    } finally {
      await harness.shutdown();
    }
  });

  it('keeps a revoked turn cancelled through adoption even when the follow-up settlement fails', async () => {
    const db = new InMemoryDB();
    const agent = new MockAgent({ id: 'default' });
    const owner = harnessProcess(db, agent);
    const session = await owner.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      await owner.storage.revokeTerminalGrant({
        harnessName: 'default',
        sessionId: session.id,
        admissionId: 'turn-1',
        executionGrant: grant,
        reason: { code: 'doxa.turn_released', message: 'released before admission' },
      });
      // Any later settlement write fails: the refusal itself must already have
      // settled the reservation.
      vi.spyOn(owner.storage, 'compareAndSwapSignalTerminal').mockRejectedValue(new Error('storage unavailable'));
      await expect(session.message(terminalMessage() as never)).rejects.toBeInstanceOf(
        HarnessTerminalHandoffCancelledError,
      );

      vi.useFakeTimers({ toFake: ['Date'] });
      vi.setSystemTime(Date.now() + LEASE_AND_DISPATCH_CLAIM_LAPSED_MS);
      const adopterAgent = new MockAgent({ id: 'default' });
      const adopter = harnessProcess(db, adopterAgent);
      const events: HarnessEvent[] = [];
      adopter.harness.subscribe(event => events.push(event));
      try {
        await adopter.harness.session({ sessionId: session.id, resourceId: 'u1' });
        expect(events.filter(event => event.type === 'run_completed')).toEqual([]);
        await expect(evidenceFor(adopter.storage, session.id, session.threadId)).resolves.toMatchObject({
          status: 'failed',
          error: { code: 'harness.terminal_cancelled' },
        });
        expect(agent.streamCalls).toHaveLength(0);
        expect(adopterAgent.streamCalls).toHaveLength(0);
      } finally {
        vi.useRealTimers();
        await adopter.harness.shutdown();
      }
    } finally {
      await owner.harness.shutdown();
    }
  });

  it('settles the undispatched reservation of a grant cancelled after admission when a retry finds it', async () => {
    const db = new InMemoryDB();
    const agent = new MockAgent({ id: 'default' });
    const { harness, storage } = harnessProcess(db, agent);
    try {
      const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      // The first attempt admits the grant, then fails before stamping its
      // dispatch: the turn is admitted but provably never dispatched.
      vi.spyOn(storage, 'compareAndSwapSignalDispatch').mockRejectedValueOnce(new Error('stamp lost'));
      await expect(session.message(terminalMessage() as never)).rejects.toThrow();
      const admission = (await admissionFor(storage, session.id))!;
      expect(admission.status).toBe('pending');
      await storage.cancelTerminalHandoff({
        harnessName: 'default',
        sessionId: session.id,
        sessionIncarnation: admission.sessionIncarnation,
        admissionId: admission.admissionId,
        admissionHash: admission.admissionHash,
        executionGrant: grant,
        reason: { code: 'doxa.turn_released', message: 'released' },
      });

      await expect(session.message(terminalMessage() as never)).rejects.toBeInstanceOf(
        HarnessTerminalHandoffCancelledError,
      );
      expect(agent.streamCalls).toHaveLength(0);
      await expect(evidenceFor(storage, session.id, session.threadId)).resolves.toMatchObject({
        status: 'failed',
        error: { code: 'harness.terminal_cancelled' },
      });
    } finally {
      await harness.shutdown();
    }
  });

  it('returns the admission unchanged when the grant was already admitted, and the turn still settles', async () => {
    const db = new InMemoryDB();
    const agent = new MockAgent({ id: 'default' });
    let release!: () => void;
    agent.enqueueRun({ holdUntil: new Promise<void>(resolve => (release = resolve)), text: 'answer' });
    const { harness, storage } = harnessProcess(db, agent);
    try {
      const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const receipts: HarnessTerminalCommitReceipt[] = [];
      const turn = session.message(
        terminalMessage({
          onTerminalCommit: (receipt: HarnessTerminalCommitReceipt) => receipts.push(receipt),
        }) as never,
      );
      await vi.waitFor(() => expect(agent.streamCalls).toHaveLength(1));

      const revocation = await storage.revokeTerminalGrant({
        harnessName: 'default',
        sessionId: session.id,
        admissionId: 'turn-1',
        executionGrant: grant,
        reason: { code: 'doxa.turn_released', message: 'too late' },
      });
      expect(revocation).toMatchObject({ status: 'admitted', admission: { status: 'pending', admissionId: 'turn-1' } });

      release();
      await expect(turn).resolves.toMatchObject({ text: 'answer' });
      expect(receipts).toHaveLength(1);
      await expect(admissionFor(storage, session.id)).resolves.toMatchObject({
        status: 'committed',
        terminalResult: { status: 'completed' },
      });
    } finally {
      await harness.shutdown();
    }
  });
});

describe('lease-fenced message dispatch', () => {
  it.each([
    { name: 'native terminal handoff', terminal: true, hook: 'admitTerminalHandoff' as const },
    { name: 'plain admitted message', terminal: false, hook: 'writeMessageResultEvidence' as const },
  ])(
    "refuses the stale owner's dispatch stamp once another owner adopted the session: $name",
    async ({ terminal, hook }) => {
      const db = new InMemoryDB();
      const agent = new MockAgent({ id: 'default' });
      const { harness, storage } = harnessProcess(db, agent);
      try {
        const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
        // Another process adopts the session right after this owner's last
        // durable step before the stamp. The owner's local view of its lease
        // is unchanged, so only the storage fence can stop the dispatch.
        const adopt = async () => {
          await storage.releaseSessionLease({ sessionId: session.id, ownerId: harness.ownerId });
          await storage.acquireSessionLease({ sessionId: session.id, ownerId: 'adopter-process', ttlMs: 60_000 });
        };
        const real = (storage[hook] as (...args: unknown[]) => Promise<unknown>).bind(storage);
        vi.spyOn(storage, hook).mockImplementation((async (...args: unknown[]) => {
          const result = await real(...args);
          if (hook === 'admitTerminalHandoff' || (args[0] as { status?: string }).status === 'pending') await adopt();
          return result;
        }) as never);

        await expect(
          session.message((terminal ? terminalMessage() : { content: 'hi', admissionId: 'turn-1' }) as never),
        ).rejects.toMatchObject({ name: 'HarnessSessionLockedError', currentOwnerId: 'adopter-process' });
        expect(agent.streamCalls).toHaveLength(0);
        // Nothing was stamped: the adopter's recovery owns the reservation.
        const evidence = await evidenceFor(storage, session.id, session.threadId);
        expect(evidence).toMatchObject({ status: 'pending' });
        expect(evidence?.dispatch).toBeUndefined();
      } finally {
        await harness.shutdown();
      }
    },
  );

  it('does not dispatch when the claim lapses while the caller observes evidence_reserved', async () => {
    const db = new InMemoryDB();
    const agent = new MockAgent({ id: 'default' });
    const { harness, storage } = harnessProcess(db, agent);
    try {
      const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      // The synchronous phase observer stalls past the dispatch claim and the
      // lease (simulated by moving the clock).
      const onPhase = (phase: string) => {
        if (phase === 'evidence_reserved') {
          vi.useFakeTimers({ toFake: ['Date'], now: Date.now() + LEASE_AND_DISPATCH_CLAIM_LAPSED_MS });
        }
      };
      await expect(session.message(terminalMessage({ onPhase }) as never)).rejects.toThrow(
        'durable message dispatch claim lapsed before dispatch',
      );
      expect(agent.streamCalls).toHaveLength(0);
      await expect(evidenceFor(storage, session.id, session.threadId)).resolves.toMatchObject({
        status: 'pending',
        dispatch: { state: 'dispatching' },
      });
      vi.useRealTimers();
    } finally {
      vi.useRealTimers();
      await harness.shutdown();
    }
  });
});

describe('rejected provider runs commit a durable failed terminal', () => {
  it.each([{ stream: false }, { stream: true }])(
    'commits failed evidence and exactly one intent; retries and adoption never re-run it (stream: $stream)',
    async ({ stream }) => {
      const db = new InMemoryDB();
      const { agent, provider } = rejectingProviderAgent();
      const finalizerCalls: HarnessTerminalFinalizerInput[] = [];
      const owner = harnessProcess(db, agent, recordingFinalizer(finalizerCalls));
      const receipts: HarnessTerminalCommitReceipt[] = [];
      const failures: Error[] = [];
      const session = await owner.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const options = terminalMessage({
        ...(stream ? { stream: true } : {}),
        onTerminalCommit: (receipt: HarnessTerminalCommitReceipt) => receipts.push(receipt),
        onTerminalCommitError: (err: Error) => failures.push(err),
      });
      try {
        if (stream) {
          await session.message(options as never);
          await vi.waitFor(() => expect(receipts).toHaveLength(1));
        } else {
          const rejection = await session.message(options as never).then(
            () => undefined,
            (err: unknown) => err,
          );
          // The caller sees the redacted run failure, not an indeterminate one.
          expect(rejection).toMatchObject({ name: 'HarnessExecutionError' });
        }
        const providerCallsAfterRun = provider.calls;
        expect(providerCallsAfterRun).toBeGreaterThan(0);
        expect(failures).toEqual([]);
        expect(receipts).toHaveLength(1);
        expect(receipts[0]).toMatchObject({
          status: 'committed',
          intent: { terminalResult: { status: 'failed', error: { code: 'harness.internal' } } },
        });
        expect(finalizerCalls).toHaveLength(1);
        expect(finalizerCalls[0]).toMatchObject({ result: { status: 'failed' }, fullOutput: undefined });
        await expect(evidenceFor(owner.storage, session.id, session.threadId)).resolves.toMatchObject({
          status: 'failed',
          error: { code: 'harness.internal' },
        });
        const admission = await admissionFor(owner.storage, session.id);
        expect(admission).toMatchObject({ status: 'committed', terminalResult: { status: 'failed' } });
        await expect(owner.storage.getTerminalQueuePressure({ harnessName: 'default' })).resolves.toMatchObject({
          pendingIntents: 1,
        });

        // A same-admission retry replays the durable failure and its receipt;
        // the provider is not called again.
        const retryReceipts: HarnessTerminalCommitReceipt[] = [];
        await expect(
          session.message(
            terminalMessage({
              onTerminalCommit: (receipt: HarnessTerminalCommitReceipt) => retryReceipts.push(receipt),
            }) as never,
          ),
        ).rejects.toThrow();
        expect(provider.calls).toBe(providerCallsAfterRun);
        expect(retryReceipts).toEqual([
          expect.objectContaining({
            status: 'duplicate',
            intent: expect.objectContaining({ id: harnessTerminalIntentId(admission!.id) }),
          }),
        ]);

        // The owner dies; another process adopts the session after its lease
        // and any dispatch claim lapsed. The failure is already durable, so
        // adoption neither interrupts nor re-runs it.
        vi.useFakeTimers({ toFake: ['Date'] });
        vi.setSystemTime(Date.now() + LEASE_AND_DISPATCH_CLAIM_LAPSED_MS);
        const adopterProvider = rejectingProviderAgent();
        const adopter = harnessProcess(db, adopterProvider.agent);
        const events: HarnessEvent[] = [];
        adopter.harness.subscribe(event => events.push(event));
        try {
          await adopter.harness.session({ sessionId: session.id, resourceId: 'u1' });
          expect(events.filter(event => event.type === 'run_completed')).toEqual([]);
          await expect(evidenceFor(adopter.storage, session.id, session.threadId)).resolves.toMatchObject({
            status: 'failed',
            error: { code: 'harness.internal' },
          });
          await expect(admissionFor(adopter.storage, session.id)).resolves.toMatchObject({
            status: 'committed',
            terminalResult: { status: 'failed' },
          });
          await expect(adopter.storage.getTerminalQueuePressure({ harnessName: 'default' })).resolves.toMatchObject({
            pendingIntents: 1,
          });
          expect(adopterProvider.provider.calls).toBe(0);
        } finally {
          vi.useRealTimers();
          await adopter.harness.shutdown();
        }
      } finally {
        await owner.harness.shutdown();
      }
    },
  );

  it('releases the caller when the turn is aborted while the failed terminal commit is stalled', async () => {
    const db = new InMemoryDB();
    const { agent } = rejectingProviderAgent();
    const calls: HarnessTerminalFinalizerInput[] = [];
    const recording = recordingFinalizer(calls);
    let releaseFinalizer!: () => void;
    const finalizerReleased = new Promise<void>(resolve => (releaseFinalizer = resolve));
    let finalizerEntered!: () => void;
    const entered = new Promise<void>(resolve => (finalizerEntered = resolve));
    const finalizer: HarnessTerminalFinalizer = {
      ...recording,
      finalize: async input => {
        finalizerEntered();
        await finalizerReleased;
        return recording.finalize(input);
      },
    };
    const { harness, storage } = harnessProcess(db, agent, finalizer);
    try {
      const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const receipts: HarnessTerminalCommitReceipt[] = [];
      const settled = session
        .message(
          terminalMessage({
            onTerminalCommit: (receipt: HarnessTerminalCommitReceipt) => receipts.push(receipt),
          }) as never,
        )
        .then(
          () => ({ ok: true as const }),
          (err: unknown) => ({ ok: false as const, err }),
        );
      await entered;
      session.abort();
      let raceTimer: ReturnType<typeof setTimeout> | undefined;
      const outcome = await Promise.race([
        settled,
        new Promise<'still-waiting'>(resolve => {
          raceTimer = setTimeout(() => resolve('still-waiting'), 2_000);
        }),
      ]).finally(() => clearTimeout(raceTimer));
      expect(outcome).toMatchObject({
        ok: false,
        err: { name: 'HarnessTerminalHandoffError:harness.terminal_pending' },
      });

      // The detached commit still settles the durable failure.
      releaseFinalizer();
      await vi.waitFor(() => expect(receipts).toHaveLength(1));
      await expect(evidenceFor(storage, session.id, session.threadId)).resolves.toMatchObject({ status: 'failed' });
      await expect(admissionFor(storage, session.id)).resolves.toMatchObject({
        status: 'committed',
        terminalResult: { status: 'failed' },
      });
    } finally {
      releaseFinalizer();
      await harness.shutdown();
    }
  });

  it('keeps a failed commit indeterminate, then lets a same-admission retry commit it without re-running', async () => {
    const db = new InMemoryDB();
    const { agent, provider } = rejectingProviderAgent();
    const calls: HarnessTerminalFinalizerInput[] = [];
    const flaky = recordingFinalizer(calls);
    let failNext = true;
    const finalizer: HarnessTerminalFinalizer = {
      ...flaky,
      finalize: async input => {
        if (failNext) {
          failNext = false;
          throw new Error('projection store unavailable');
        }
        return flaky.finalize(input);
      },
    };
    const { harness, storage } = harnessProcess(db, agent, finalizer);
    try {
      const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      await expect(session.message(terminalMessage() as never)).rejects.toMatchObject({
        name: 'HarnessTerminalHandoffError:harness.terminal_pending',
      });
      const providerCalls = provider.calls;
      await expect(evidenceFor(storage, session.id, session.threadId)).resolves.toMatchObject({ status: 'pending' });
      await expect(admissionFor(storage, session.id)).resolves.toMatchObject({ status: 'pending' });

      const receipts: HarnessTerminalCommitReceipt[] = [];
      await expect(
        session.message(
          terminalMessage({
            onTerminalCommit: (receipt: HarnessTerminalCommitReceipt) => receipts.push(receipt),
          }) as never,
        ),
      ).rejects.toMatchObject({ name: 'HarnessExecutionError' });
      expect(provider.calls).toBe(providerCalls);
      expect(receipts).toEqual([
        expect.objectContaining({ status: 'committed', intent: expect.objectContaining({ status: 'pending' }) }),
      ]);
      await expect(evidenceFor(storage, session.id, session.threadId)).resolves.toMatchObject({ status: 'failed' });
      await expect(admissionFor(storage, session.id)).resolves.toMatchObject({
        status: 'committed',
        terminalResult: { status: 'failed' },
      });
    } finally {
      await harness.shutdown();
    }
  });

  it('never commits an aborted turn as failed when its run fails after the abort released the caller', async () => {
    const db = new InMemoryDB();
    const agent = new MockAgent({ id: 'default' });
    const providerError = new Error('provider failed after abort');
    let releaseOutput!: () => void;
    const outputReleased = new Promise<void>(resolve => (releaseOutput = resolve));
    let outputEntered!: () => void;
    const entered = new Promise<void>(resolve => (outputEntered = resolve));
    // The run's output reports an explicit failure, but only after the turn
    // was aborted and the caller released.
    const buildOutput = (agent as any).buildOutput.bind(agent);
    vi.spyOn(agent as any, 'buildOutput').mockImplementation((...args: unknown[]) => {
      const out = buildOutput(...args);
      let failed = false;
      Object.defineProperty(out, 'status', { get: () => (failed ? 'failed' : 'running') });
      out.getFullOutput = async () => {
        outputEntered();
        await outputReleased;
        failed = true;
        out.error = providerError;
        throw providerError;
      };
      return out;
    });
    const { harness, storage } = harnessProcess(db, agent);
    try {
      const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const turn = session.message(terminalMessage() as never).then(
        () => undefined,
        (err: unknown) => err,
      );
      await entered;
      session.abort();
      await expect(turn).resolves.toMatchObject({ name: 'HarnessTerminalHandoffError:harness.terminal_pending' });
      releaseOutput();
      await vi.waitFor(() => expect((session as any)._completedRuns.size).toBeGreaterThan(0));

      // A same-admission retry reports the failure but leaves the aborted
      // turn's outcome to reconciliation instead of committing it as failed.
      await expect(session.message(terminalMessage() as never)).rejects.toThrow();
      await expect(admissionFor(storage, session.id)).resolves.toMatchObject({ status: 'pending' });
      await expect(storage.getTerminalQueuePressure({ harnessName: 'default' })).resolves.toMatchObject({
        pendingIntents: 0,
      });
    } finally {
      releaseOutput();
      await harness.shutdown();
    }
  });

  it('keeps a run whose output closed without an explicit failure indeterminate', async () => {
    const db = new InMemoryDB();
    const agent = new MockAgent({ id: 'default' });
    // The collector rejects without the run reporting a failure (no `error`
    // chunk): the outcome is unknown, so nothing may be committed as failed.
    const buildOutput = (agent as any).buildOutput.bind(agent);
    vi.spyOn(agent as any, 'buildOutput').mockImplementation((...args: unknown[]) => {
      const out = buildOutput(...args);
      out.getFullOutput = async () => {
        throw new Error("promise 'steps' was not resolved or rejected when stream finished");
      };
      return out;
    });
    const { harness, storage } = harnessProcess(db, agent);
    try {
      const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      await expect(session.message(terminalMessage() as never)).rejects.toMatchObject({
        name: 'HarnessTerminalHandoffError:harness.terminal_pending',
      });
      await expect(evidenceFor(storage, session.id, session.threadId)).resolves.toMatchObject({ status: 'pending' });
      await expect(admissionFor(storage, session.id)).resolves.toMatchObject({ status: 'pending' });
      await expect(storage.getTerminalQueuePressure({ harnessName: 'default' })).resolves.toMatchObject({
        pendingIntents: 0,
      });
    } finally {
      await harness.shutdown();
    }
  });
});
