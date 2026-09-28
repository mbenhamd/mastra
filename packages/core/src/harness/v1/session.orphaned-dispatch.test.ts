/**
 * Harness v1 — session adoption interrupts admitted `message()` dispatches whose
 * owning process died mid-run (PF-4598). The adopter never re-runs provider
 * work: the orphaned run is settled `harness.run_interrupted` and reported as a
 * reconstructed `run_completed{status:'interrupted'}`.
 */

import { afterEach, describe, expect, it, vi } from 'vitest';

import type { HarnessTerminalFinalizer } from '../../storage/domains/harness';
import { InMemoryHarness } from '../../storage/domains/harness/inmemory';
import { InMemoryDB } from '../../storage/domains/inmemory-db';

import { MockAgent } from './__test-utils__/mock-agent';
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
function harnessProcess(db: InMemoryDB, agent: MockAgent, terminal: boolean) {
  const storage = new InMemoryHarness({
    db,
    ...(terminal ? { terminalHandoff: { enabled: true }, sessionRecordProjection: { enabled: true } } : {}),
  });
  const harness = new Harness({
    agents: { default: agent } as any,
    modes: [{ id: 'default', agentId: 'default' }],
    defaultModeId: 'default',
    sessions: { storage, ...(terminal ? { terminalHandoff: { finalizer } } : {}) },
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
  const pending = await owner.storage.listPendingMessageAdmissions({ ...scope, limit: 10 });
  expect(pending.items).toHaveLength(1);
  const signalId = pending.items[0]!.evidence.signalId;
  return { ...owner, agent, scope, sessionId: session.id, runId: stream.runId, signalId, release };
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
    } finally {
      await adopter.harness.shutdown();
      await stopDeadOwner(liveOwner);
    }
  });
});
