/**
 * §4.2e / §9.2 — Observational Memory config surface (`session.om.*`).
 *
 * Covers config resolution/validation, per-session record seeding, the config
 * read accessors (model ids + thresholds), and the lease-backed model switches.
 * The redacted snapshot read (`getRecord`/`loadProgress`, §4.8) is a separate
 * follow-up slice and is not exercised here.
 */

import { describe, expect, it } from 'vitest';

import { setupHarness } from './__test-utils__/setup';

describe('session.om — config resolution + accessors (§9.2)', () => {
  it('resolves explicit observation/reflection models + thresholds + scope', async () => {
    const { harness } = setupHarness({
      observationalMemory: {
        scope: 'resource',
        observation: { model: 'anthropic/claude-haiku-4-5', messageTokens: 20_000 },
        reflection: { model: 'openai/gpt-4o-mini', observationTokens: 80_000 },
      },
    });
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      expect(session.om.getObserverModelId()).toBe('anthropic/claude-haiku-4-5');
      expect(session.om.getReflectorModelId()).toBe('openai/gpt-4o-mini');
      expect(session.om.getObservationThreshold()).toBe(20_000);
      expect(session.om.getReflectionThreshold()).toBe(80_000);
      // The per-session snapshot is seeded from the resolved defaults.
      expect(session.getRecord().observationalMemory?.scope).toBe('resource');
    } finally {
      await harness.shutdown();
    }
  });

  it('a shared `model` falls back to both observer and reflector', async () => {
    const { harness } = setupHarness({ observationalMemory: { model: 'google/gemini-2.5-flash' } });
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      expect(session.om.getObserverModelId()).toBe('google/gemini-2.5-flash');
      expect(session.om.getReflectorModelId()).toBe('google/gemini-2.5-flash');
    } finally {
      await harness.shutdown();
    }
  });

  it('enabled with defaults (`true`) → null models, 0 thresholds (adapter default), thread scope', async () => {
    const { harness } = setupHarness({ observationalMemory: true });
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      expect(session.om.getObserverModelId()).toBeNull();
      expect(session.om.getReflectorModelId()).toBeNull();
      expect(session.om.getObservationThreshold()).toBe(0);
      expect(session.om.getReflectionThreshold()).toBe(0);
      expect(session.getRecord().observationalMemory?.scope).toBe('thread');
    } finally {
      await harness.shutdown();
    }
  });

  it('disabled (omitted) → no per-session snapshot, null/0 reads', async () => {
    const { harness } = setupHarness();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      expect(session.getRecord().observationalMemory).toBeUndefined();
      expect(session.om.getObserverModelId()).toBeNull();
      expect(session.om.getObservationThreshold()).toBe(0);
    } finally {
      await harness.shutdown();
    }
  });

  it('switchObserverModel / switchReflectorModel commit and the getters reflect them', async () => {
    const { harness } = setupHarness({ observationalMemory: { model: 'google/gemini-2.5-flash' } });
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      await session.om.switchObserverModel({ model: 'anthropic/claude-haiku-4-5' });
      await session.om.switchReflectorModel({ model: 'openai/gpt-4o-mini' });
      expect(session.om.getObserverModelId()).toBe('anthropic/claude-haiku-4-5');
      expect(session.om.getReflectorModelId()).toBe('openai/gpt-4o-mini');
      // Persisted on the record under the session lease.
      expect(session.getRecord().observationalMemory?.observerModelId).toBe('anthropic/claude-haiku-4-5');
      expect(session.getRecord().observationalMemory?.reflectorModelId).toBe('openai/gpt-4o-mini');
    } finally {
      await harness.shutdown();
    }
  });

  it('switchObserverModel survives rehydrate', async () => {
    const { harness: h1, storage } = setupHarness({ observationalMemory: true });
    const session = await h1.session({ resourceId: 'u1', threadId: { fresh: true } });
    const sessionId = session.id;
    await session.om.switchObserverModel({ model: 'anthropic/claude-haiku-4-5' });
    await h1.shutdown();

    const { harness: h2 } = setupHarness({ observationalMemory: true, sessions: { storage } });
    try {
      const rehydrated = await h2.session({ sessionId, resourceId: 'u1' });
      expect(rehydrated.om.getObserverModelId()).toBe('anthropic/claude-haiku-4-5');
    } finally {
      await h2.shutdown();
    }
  });

  it('rejects an empty model on switch', async () => {
    const { harness } = setupHarness({ observationalMemory: true });
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      await expect(session.om.switchObserverModel({ model: '' } as any)).rejects.toThrow(/model/);
    } finally {
      await harness.shutdown();
    }
  });

  it('rejects malformed harness OM config at construction', () => {
    expect(() => setupHarness({ observationalMemory: { scope: 'bogus' as any } })).toThrow(/scope/);
    expect(() => setupHarness({ observationalMemory: { observation: { messageTokens: 0 } } })).toThrow(/messageTokens/);
    expect(() => setupHarness({ observationalMemory: { observation: { model: '' } } })).toThrow(/model/);
    // Runtime-shape holes (Codex finding 2): null config, non-boolean enabled,
    // non-object step, and non-JSON processorOptions must all reject.
    expect(() => setupHarness({ observationalMemory: null as any })).toThrow(/observationalMemory/);
    expect(() => setupHarness({ observationalMemory: { enabled: 'false' as any } })).toThrow(/enabled/);
    expect(() => setupHarness({ observationalMemory: { observation: 'nope' as any } })).toThrow(/observation/);
    expect(() => setupHarness({ observationalMemory: { processorOptions: { fn: (() => {}) as any } } })).toThrow(
      /processorOptions/,
    );
  });
});

describe('session.om — per-turn enablement is NOT runtime-threaded (blocked on @mastra/memory)', () => {
  it('does NOT thread a memory.options OM bag (runtime OM would suppress history without attaching OM)', async () => {
    // @mastra/memory builds its OM engine from the Memory CONSTRUCTOR config; a
    // runtime memory.options.observationalMemory only suppresses MessageHistory
    // without attaching OM, so the harness must not pass one. The turn memory
    // option stays { thread, resource } whether or not OM config is present.
    for (const om of [undefined, { observation: { messageTokens: 10_000 } } as const]) {
      const { harness, agent } = setupHarness(om ? { observationalMemory: om } : {});
      const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      try {
        await session.message({ content: 'hi' });
        const memory = (agent.streamCalls.at(-1)!.options as any).memory;
        expect(memory.thread).toBe(session.threadId);
        expect(memory.resource).toBe('u1');
        expect(memory.options).toBeUndefined();
      } finally {
        await harness.shutdown();
      }
    }
  });
});
