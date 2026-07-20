/**
 * §4.2e / §9.2 — Observational Memory config surface (`session.om.*`).
 *
 * Native per-turn OM engine selection is not available yet. The Harness must
 * therefore reject enabled configuration and model switches before persisting
 * unsupported intent, while retaining a recovery path for records written by
 * older builds.
 */

import { describe, expect, it } from 'vitest';

import { setupHarness } from './__test-utils__/setup';

describe('session.om — fail-loud configuration and recovery (§9.2)', () => {
  it('keeps OM disabled when omitted or explicitly disabled', async () => {
    for (const observationalMemory of [undefined, false, { enabled: false } as const]) {
      const { harness } = setupHarness(observationalMemory === undefined ? {} : { observationalMemory });
      const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      try {
        expect(session.getRecord().observationalMemory).toBeUndefined();
        expect(session.om.getObserverModelId()).toBeNull();
        expect(session.om.getReflectorModelId()).toBeNull();
        expect(session.om.getObservationThreshold()).toBe(0);
        expect(session.om.getReflectionThreshold()).toBe(0);
      } finally {
        await harness.shutdown();
      }
    }
  });

  it('rejects every enabled Harness OM form at construction', () => {
    expect(() => setupHarness({ observationalMemory: true })).toThrow(/not supported until @mastra\/memory/);
    expect(() =>
      setupHarness({
        observationalMemory: {
          scope: 'resource',
          observation: { model: 'anthropic/claude-haiku-4-5', messageTokens: 20_000 },
          reflection: { model: 'openai/gpt-4o-mini', observationTokens: 80_000 },
        },
      }),
    ).toThrow(/not supported until @mastra\/memory/);
    expect(() => setupHarness({ observationalMemory: { model: 'google/gemini-2.5-flash' } })).toThrow(
      /not supported until @mastra\/memory/,
    );
  });

  it('rejects unsupported model switches before persistence', async () => {
    const { harness } = setupHarness();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      await expect(session.om.switchObserverModel({ model: 'anthropic/claude-haiku-4-5' })).rejects.toThrow(
        /model switches are not supported/,
      );
      await expect(session.om.switchReflectorModel({ model: 'openai/gpt-4o-mini' })).rejects.toThrow(
        /model switches are not supported/,
      );
      expect(session.getRecord().observationalMemory).toBeUndefined();
      expect(session.om.getObserverModelId()).toBeNull();
      expect(session.om.getReflectorModelId()).toBeNull();
    } finally {
      await harness.shutdown();
    }
  });

  it('validates a switch model before reporting unsupported runtime selection', async () => {
    const { harness } = setupHarness();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      await expect(session.om.switchObserverModel({ model: '' } as any)).rejects.toThrow(/model/);
      await expect(session.om.switchReflectorModel({ model: '' } as any)).rejects.toThrow(/model/);
      expect(session.getRecord().observationalMemory).toBeUndefined();
    } finally {
      await harness.shutdown();
    }
  });

  it('rejects malformed config fields before the unsupported-feature error', () => {
    expect(() => setupHarness({ observationalMemory: { scope: 'bogus' as any } })).toThrow(/scope/);
    expect(() => setupHarness({ observationalMemory: { observation: { messageTokens: 0 } } })).toThrow(/messageTokens/);
    expect(() => setupHarness({ observationalMemory: { observation: { model: '' } } })).toThrow(/model/);
    expect(() => setupHarness({ observationalMemory: null as any })).toThrow(/observationalMemory/);
    expect(() => setupHarness({ observationalMemory: { enabled: 'false' as any } })).toThrow(/enabled/);
    expect(() => setupHarness({ observationalMemory: { observation: 'nope' as any } })).toThrow(/observation/);
    expect(() => setupHarness({ observationalMemory: { processorOptions: { fn: (() => {}) as any } } })).toThrow(
      /processorOptions/,
    );
  });

  it('keeps ordinary message-history turns unchanged when Harness OM is disabled', async () => {
    const { harness, agent } = setupHarness({ observationalMemory: false });
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      await session.message({ content: 'hi' });
      const memory = (agent.streamCalls.at(-1)!.options as any).memory;
      expect(memory).toEqual({ thread: session.threadId, resource: 'u1' });
    } finally {
      await harness.shutdown();
    }
  });

  it('clears a legacy persisted override and restores dispatch after rehydrate', async () => {
    const { harness: firstHarness, storage } = setupHarness();
    const firstSession = await firstHarness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const sessionId = firstSession.id;
    const stored = (await storage.loadSession({ harnessName: 'default', sessionId }))!;

    // Simulate a row written by the older implementation, which persisted a
    // switch even though the native Memory engine could not honor it.
    await storage.saveSession(
      {
        ...stored,
        observationalMemory: {
          scope: 'thread',
          observerModelId: 'anthropic/claude-haiku-4-5',
        },
      },
      { harnessName: 'default', ownerId: firstHarness.ownerId, ifVersion: stored.version },
    );
    await firstHarness.shutdown();

    const { harness: secondHarness, agent } = setupHarness({ sessions: { storage } });
    try {
      const rehydrated = await secondHarness.session({ sessionId, resourceId: 'u1' });
      expect(rehydrated.om.getObserverModelId()).toBe('anthropic/claude-haiku-4-5');
      await expect(rehydrated.message({ content: 'blocked before recovery' })).rejects.toThrow(
        /not supported until @mastra\/memory/,
      );
      expect(agent.streamCalls).toHaveLength(0);

      await rehydrated.om.clearOverride();
      await rehydrated.om.clearOverride();
      expect(rehydrated.getRecord().observationalMemory).toBeUndefined();

      await rehydrated.message({ content: 'works after recovery' });
      expect(agent.streamCalls).toHaveLength(1);
    } finally {
      await secondHarness.shutdown();
    }
  });

  it('parks a hydrated queued item without mutating its receipt until the legacy override is cleared', async () => {
    const { harness: firstHarness, storage } = setupHarness();
    const firstSession = await firstHarness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const sessionId = firstSession.id;
    const stored = (await storage.loadSession({ harnessName: 'default', sessionId }))!;
    const now = Date.now();
    const queuedItem = {
      id: 'legacy-om-queued-item',
      admissionId: 'legacy-om-queue-admission',
      admissionHash: 'legacy-om-queue-hash',
      enqueuedAt: now,
      content: 'run only after OM recovery',
      attachments: [],
    };
    const queuedReceipt = {
      admissionId: queuedItem.admissionId,
      admissionHash: queuedItem.admissionHash,
      queuedItemId: queuedItem.id,
      status: 'queued' as const,
      attempts: 0,
      enqueuedAt: now,
      updatedAt: now,
    };

    await storage.saveSession(
      {
        ...stored,
        observationalMemory: {
          scope: 'thread',
          observerModelId: 'anthropic/claude-haiku-4-5',
        },
        pendingQueue: [queuedItem],
        queueAdmissionReceipts: { [queuedItem.id]: queuedReceipt },
      },
      { harnessName: 'default', ownerId: firstHarness.ownerId, ifVersion: stored.version },
    );
    await firstHarness.shutdown();

    const { harness: secondHarness, agent } = setupHarness({ sessions: { storage } });
    let releaseQueuedTurn!: () => void;
    const queuedTurnGate = new Promise<void>(resolve => {
      releaseQueuedTurn = resolve;
    });
    agent.enqueueRun({ text: 'recovered queued reply', holdUntil: queuedTurnGate });
    try {
      const rehydrated = await secondHarness.session({ sessionId, resourceId: 'u1' });
      // Hydration kicks queue replay asynchronously. Give it a turn to prove the
      // unsupported legacy intent parks before scheduler/receipt admission.
      await new Promise(resolve => setImmediate(resolve));

      expect(agent.streamCalls).toHaveLength(0);
      expect(rehydrated.getRecord().pendingQueue).toEqual([queuedItem]);
      expect(rehydrated.getRecord().queueAdmissionReceipts?.[queuedItem.id]).toEqual(queuedReceipt);
      const parkedStored = (await storage.loadSession({ harnessName: 'default', sessionId }))!;
      expect(parkedStored.pendingQueue).toEqual([queuedItem]);
      expect(parkedStored.queueAdmissionReceipts?.[queuedItem.id]).toEqual(queuedReceipt);

      // The recovery call commits and returns without waiting for the queued LLM
      // turn it wakes. Calling it again while that drain is active is harmless.
      await rehydrated.om.clearOverride();
      await rehydrated.om.clearOverride();
      await expect.poll(() => agent.streamCalls.length).toBe(1);
      expect(rehydrated.getRecord().pendingQueue).toEqual([queuedItem]);

      releaseQueuedTurn();
      await rehydrated.waitForIdle({ timeoutMs: 1_000 });

      expect(agent.streamCalls).toHaveLength(1);
      expect(rehydrated.getRecord().pendingQueue).toEqual([]);
      expect(rehydrated.getRecord().queueAdmissionReceipts?.[queuedItem.id]).toMatchObject({
        status: 'completed',
        attempts: 1,
      });
    } finally {
      releaseQueuedTurn();
      await secondHarness.shutdown();
    }
  });
});
