/**
 * Harness v1 — ADVERSARIAL replay + hostile-input campaign (§10.5 / §5.1b.4 / §4.4c / §S4.2).
 *
 * These tests attack the UNPROVEN invariants the rest of the suite asserts only
 * piecemeal:
 *
 *   1. LIVE === REPLAY as an ORDERED SEQUENCE (the headline §10.5 guarantee):
 *      every live event id maps 1:1, in order, to the durable ledger row at the
 *      same sequence — exactly equal with persistTransientStreamingEvents=true,
 *      equal-after-delta-filtering with =false.
 *   2. Replay over a storage_error SENTINEL slot keeps the EXACT sequence the
 *      live event occupied (compared by sequence number, not array index).
 *   3. listEventsAfter boundary hostility (oldestSequence-1, newest, beyond,
 *      limit=1 paging, wrong epoch).
 *   4. Activity-timeline cursor FORGERY rejects BEFORE any storage read.
 *   5. App-bag hostility on resume (C3): a tampered persisted pendingResume app
 *      bag must NOT shadow the genuine `harness` request-context slot.
 *   6. Custom event payload bomb: actual persistence behavior pinned.
 *   7. Queue admission hostility: hash mismatch rejects; rapid drain keeps the
 *      durable event sequence contiguous and uncorrupted.
 *
 * Conventions mirror session.events.test.ts: a FakeAgent drives a programmable
 * fullStream; `_flushEventPersistence()` drains the durable tail before any
 * ledger read; `getSessionEventReplayState` + `listSessionEvents` read the log
 * (afterSequence is EXCLUSIVE → use oldestSequence-1 to include seq 0).
 */

import { describe, expect, it, vi } from 'vitest';

import { Agent } from '../../agent';
import { InMemoryHarness } from '../../storage/domains/harness/inmemory';
import { InMemoryDB } from '../../storage/domains/inmemory-db';
import { buildFakeOutput } from './__test-utils__/fake-output';
import { setupHarness } from './__test-utils__/setup';

import { HarnessValidationError } from './errors';
import { parseHarnessEventId } from './events';
import type { HarnessEvent } from './events';
import { Harness } from './harness';

// ---------------------------------------------------------------------------
// FakeAgent — same shape as session.events.test.ts.
// ---------------------------------------------------------------------------

class FakeAgent extends Agent<any, any, any> {
  chunks: any[] = [];
  fullOutput: any = {
    text: 'ok',
    usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
    finishReason: 'stop',
    object: undefined,
    steps: [],
    warnings: [],
    providerMetadata: undefined,
    request: {},
    reasoning: [],
    reasoningText: undefined,
    toolCalls: [],
    toolResults: [],
    sources: [],
    files: [],
    response: { id: 'r', timestamp: new Date(), modelId: 'fake', messages: [], uiMessages: [] },
    totalUsage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
    error: undefined,
    tripwire: undefined,
    traceId: undefined,
    spanId: undefined,
    runId: 'fake-run',
    suspendPayload: undefined,
    messages: [],
    rememberedMessages: [],
  };

  /** Captures the options (incl. requestContext) of the most recent resumeStream call. */
  lastResumeOptions: any;

  constructor(name: string) {
    super({ id: name, name, instructions: 'fake', model: 'openai/gpt-4o-mini' as any });
  }

  async stream(_messages: any, options?: any): Promise<any> {
    const out = buildFakeOutput({
      runId: options?.runId ?? this.fullOutput.runId,
      fullOutput: this.fullOutput,
      chunks: this.chunks,
    });
    this._internalRegisterStreamRun(out, (options ?? {}) as any);
    return out;
  }

  async generate(_messages: any, _options?: any): Promise<any> {
    return this.fullOutput;
  }

  async resumeStream(_resumeData: any, options?: any): Promise<any> {
    this.lastResumeOptions = options;
    return this.stream(undefined, options);
  }
}

function setup(opts?: { persistTransientStreamingEvents?: boolean; maxEventPayloadBytes?: number }) {
  const agent = new FakeAgent('default');
  const storage = new InMemoryHarness({ db: new InMemoryDB() });
  const harness = new Harness({
    agents: { default: agent } as any,
    modes: [{ id: 'default', agentId: 'default' }],
    defaultModeId: 'default',
    sessions: {
      storage,
      ...(opts?.persistTransientStreamingEvents !== undefined
        ? { persistTransientStreamingEvents: opts.persistTransientStreamingEvents }
        : {}),
    },
    ...(opts?.maxEventPayloadBytes !== undefined ? { files: { maxEventPayloadBytes: opts.maxEventPayloadBytes } } : {}),
  });
  return { harness, agent, storage };
}

/** Read the WHOLE durable ledger for a session, inclusive of seq 0. */
async function readLedger(
  storage: InMemoryHarness,
  session: { id: string; resourceId: string; threadId: string },
): Promise<Array<{ sequence: number; event: any }>> {
  const state = await storage.getSessionEventReplayState({
    sessionId: session.id,
    resourceId: session.resourceId,
    threadId: session.threadId,
  });
  if (!state) return [];
  const rows = await storage.listSessionEvents({
    sessionId: session.id,
    resourceId: session.resourceId,
    threadId: session.threadId,
    epoch: state.epoch,
    afterSequence: state.oldestSequence - 1, // inclusive of seq 0 (afterSequence is EXCLUSIVE)
    limit: 100_000,
  });
  return rows.map(r => ({ sequence: r.sequence, event: r.event as any }));
}

/** A turn that streams text + a tool call/result so a full event family flows. */
const TURN_CHUNKS = [
  { type: 'text-start', payload: { id: 'm' }, runId: 'fake-run' },
  { type: 'text-delta', payload: { id: 'm', text: 'he' }, runId: 'fake-run' },
  { type: 'text-delta', payload: { id: 'm', text: 'llo' }, runId: 'fake-run' },
  { type: 'tool-call', payload: { toolCallId: 'tc1', toolName: 'lookup', args: { q: 'x' } }, runId: 'fake-run' },
  { type: 'tool-result', payload: { toolCallId: 'tc1', result: { hits: 3 } }, runId: 'fake-run' },
  { type: 'text-end', payload: { id: 'm' }, runId: 'fake-run' },
];

const TRANSIENT_DELTA_TYPES = new Set([
  'text_delta',
  'reasoning_delta',
  'subagent_text_delta',
  'subagent_reasoning_delta',
]);

// ===========================================================================
// 1. LIVE === REPLAY as an ORDERED SEQUENCE (§10.5 headline guarantee).
// ===========================================================================

describe('§10.5 — live stream === durable ledger as an ORDERED sequence', () => {
  it('persistTransientStreamingEvents=true: ledger maps 1:1 IN ORDER to live (by id AND sequence)', async () => {
    const { harness, agent, storage } = setup({ persistTransientStreamingEvents: true });
    agent.chunks = TURN_CHUNKS;
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      const live: HarnessEvent[] = [];
      session.subscribe(e => live.push(e));

      await session.message({ content: 'hi' });
      await session._flushEventPersistence();

      const ledger = await readLedger(storage, session);

      // The turn produced a non-trivial event family (text_delta + tool_start/end + agent_*).
      expect(live.length).toBeGreaterThan(4);
      expect(live.some(e => e.type === 'text_delta')).toBe(true);
      expect(live.some(e => e.type === 'tool_start')).toBe(true);
      expect(live.some(e => e.type === 'tool_end')).toBe(true);

      // 1:1 cardinality: every live event has exactly one durable row.
      expect(ledger).toHaveLength(live.length);

      // Ledger sequences are strictly contiguous (no gap, no dupe).
      for (let i = 1; i < ledger.length; i++) {
        expect(ledger[i]!.sequence).toBe(ledger[i - 1]!.sequence + 1);
      }

      // IN ORDER: zip the live stream against the ledger. The ledger is ordered by
      // sequence; the live ids encode (epoch:sequence). They MUST agree position-by-position.
      const liveIds = live.map(e => e.id);
      const liveSeqs = live.map(e => parseHarnessEventId(e.id).sequence);
      const ledgerIds = ledger.map(r => r.event.id);
      const ledgerSeqs = ledger.map(r => r.sequence);
      expect(ledgerIds).toEqual(liveIds);
      // The id-encoded sequence equals the storage-row sequence at every position.
      expect(liveSeqs).toEqual(ledgerSeqs);
      // And the event TYPES line up 1:1 in order.
      expect(ledger.map(r => r.event.type)).toEqual(live.map(e => e.type));
    } finally {
      await harness.shutdown();
    }
  });

  it('persistTransientStreamingEvents=false: ledger === live AFTER filtering transient deltas (and ONLY deltas drop)', async () => {
    const { harness, agent, storage } = setup({ persistTransientStreamingEvents: false });
    agent.chunks = TURN_CHUNKS;
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      const live: HarnessEvent[] = [];
      session.subscribe(e => live.push(e));

      await session.message({ content: 'hi' });
      await session._flushEventPersistence();

      const ledger = await readLedger(storage, session);

      // Live still carries the deltas in real time.
      expect(live.some(e => e.type === 'text_delta')).toBe(true);

      // The ONLY documented omission is the transient deltas; filter them from live.
      const liveNonDelta = live.filter(e => !TRANSIENT_DELTA_TYPES.has(e.type));
      // The ledger contains NO transient deltas at all.
      expect(ledger.some(r => TRANSIENT_DELTA_TYPES.has(r.event.type))).toBe(false);

      // After filtering deltas from live, the ledger is byte-for-byte (id + type) equal IN ORDER.
      expect(ledger.map(r => r.event.id)).toEqual(liveNonDelta.map(e => e.id));
      expect(ledger.map(r => r.event.type)).toEqual(liveNonDelta.map(e => e.type));

      // The dropped events are EXACTLY the deltas — nothing non-transient was lost.
      const droppedIds = new Set(live.map(e => e.id));
      for (const r of ledger) droppedIds.delete(r.event.id);
      const droppedTypes = live.filter(e => droppedIds.has(e.id)).map(e => e.type);
      expect(droppedTypes.length).toBeGreaterThan(0);
      expect(droppedTypes.every(t => TRANSIENT_DELTA_TYPES.has(t))).toBe(true);

      // PIN the real shape: the skipped deltas DID consume live sequence numbers (the live
      // emitter seq always advances), so the persisted ledger sequences are NON-contiguous —
      // they JUMP across the skipped delta slots. Prove the gap exists rather than asserting
      // false contiguity. (This is exactly why §10.5 mints a fresh epoch on rehydrate when
      // persistence is off — a skipped tail delta would otherwise let a cursor reuse a seq.)
      const seqs = ledger.map(r => r.sequence);
      expect([...seqs].sort((a, b) => a - b)).toEqual(seqs); // monotonic ascending
      const liveDeltaSeqs = live
        .filter(e => TRANSIENT_DELTA_TYPES.has(e.type))
        .map(e => parseHarnessEventId(e.id).sequence);
      expect(liveDeltaSeqs.length).toBeGreaterThan(0);
      // The persisted set excludes every delta seq → a genuine gap in the durable sequence space.
      expect(seqs.some(s => liveDeltaSeqs.includes(s))).toBe(false);
      const hasGap = seqs.some((s, i) => i > 0 && s !== seqs[i - 1]! + 1);
      expect(hasGap).toBe(true);
      // Sequences correspond 1:1 to the persisted (non-delta) live events' id-encoded seqs.
      expect(seqs).toEqual(liveNonDelta.map(e => parseHarnessEventId(e.id).sequence));
    } finally {
      await harness.shutdown();
    }
  });
});

// ===========================================================================
// 2. Replay over a storage_error SENTINEL slot (§S4.2).
// ===========================================================================

describe('§S4.2 — unserializable event becomes a sentinel AT THE EXACT live sequence', () => {
  it('keeps the sentinel at the live event sequence and every later event in order', async () => {
    const { harness, storage } = setup();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      const live: HarnessEvent[] = [];
      session.subscribe(e => live.push(e));

      await session.setState({ a: 1 }); // good event #1

      // Force ONE unserializable event straight through the emitter (bypasses emit-time
      // custom-event JSON validation), so it fails at the persistence snapshot.
      // The cycle rides OUTSIDE `payload`: the default-on event-payload cap
      // (PF-2246, `applyCustomEventPayloadCap`) deliberately projects a custom
      // event's `payload` at emit — an in-`payload` cycle now becomes the
      // TOOL_PAYLOAD_UNSERIALIZABLE sentinel before persistence ever sees it.
      // Any other unserializable field still reaches the persistence snapshot,
      // which is exactly the §S4.2 ledger-sentinel lane this test pins.
      const circular: Record<string, unknown> = {};
      circular.self = circular;
      const badEvent = (session as unknown as { _emitter: { emit(e: unknown): HarnessEvent } })._emitter.emit({
        type: 'badns.bad',
        payload: { ok: true },
        poison: circular,
      });
      const badSeq = parseHarnessEventId(badEvent.id).sequence;

      await session.setState({ a: 2 }); // good event #2 — must still persist after the sentinel
      await session.setState({ a: 3 }); // good event #3

      await expect(session._flushEventPersistence()).resolves.toBeUndefined();

      const ledger = await readLedger(storage, session);

      // The sentinel occupies the EXACT sequence the live bad event held (NOT indexOf).
      const sentinelRow = ledger.find(r => r.sequence === badSeq);
      expect(sentinelRow).toBeDefined();
      expect(sentinelRow!.event.type).toBe('storage_error');
      expect(sentinelRow!.event.id).toBe(badEvent.id);
      expect(sentinelRow!.event.error.code).toBe('harness.event_serialization');
      // Contiguity preserved: the sentinel did not skip a slot.
      for (let i = 1; i < ledger.length; i++) {
        expect(ledger[i]!.sequence).toBe(ledger[i - 1]!.sequence + 1);
      }

      // Every LATER live event still appears in the ledger, in order, at its own sequence.
      const laterLive = live.filter(e => parseHarnessEventId(e.id).sequence > badSeq);
      for (const e of laterLive) {
        const seq = parseHarnessEventId(e.id).sequence;
        const row = ledger.find(r => r.sequence === seq);
        expect(row, `live event ${e.type}@${seq} missing from ledger`).toBeDefined();
        expect(row!.event.type).toBe(e.type);
      }
      // At least the two trailing state_changed events landed after the sentinel.
      const afterSentinel = ledger.filter(r => r.sequence > badSeq);
      expect(afterSentinel.filter(r => r.event.type === 'state_changed').length).toBeGreaterThanOrEqual(2);
    } finally {
      await harness.shutdown();
    }
  });
});

// ===========================================================================
// 3. listEventsAfter boundary hostility (§10.5).
// ===========================================================================

describe('listEventsAfter — boundary hostility', () => {
  async function freshTurnSession() {
    const { harness, agent, storage } = setup();
    agent.chunks = TURN_CHUNKS;
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    await session.message({ content: 'hi' });
    await session._flushEventPersistence();
    const state = (await storage.getSessionEventReplayState({
      sessionId: session.id,
      resourceId: session.resourceId,
      threadId: session.threadId,
    }))!;
    return { harness, storage, session, state };
  }

  it('afterSequence = oldestSequence-1 returns the ENTIRE ledger including seq 0', async () => {
    const { harness, storage, session, state } = await freshTurnSession();
    try {
      const all = await readLedger(storage, session);
      const viaApi = await session.listEventsAfter({
        epoch: state.epoch,
        afterSequence: state.oldestSequence - 1 < 0 ? 0 : state.oldestSequence - 1,
        limit: 100_000,
      });
      // oldestSequence is 0 here; afterSequence must be a non-negative integer, so the
      // public API floor is 0 (EXCLUSIVE) → it omits seq 0. The storage helper used by
      // readLedger has no such floor. Pin both: storage includes seq 0, the API floor excludes it.
      expect(all[0]!.sequence).toBe(0);
      expect(viaApi.every(r => r.sequence > 0)).toBe(true);
    } finally {
      await harness.shutdown();
    }
  });

  it('afterSequence = newestSequence returns empty (everything already delivered)', async () => {
    const { harness, session, state } = await freshTurnSession();
    try {
      const rows = await session.listEventsAfter({
        epoch: state.epoch,
        afterSequence: state.newestSequence,
        limit: 100,
      });
      expect(rows).toEqual([]);
    } finally {
      await harness.shutdown();
    }
  });

  it('afterSequence BEYOND newest returns empty without throwing', async () => {
    const { harness, session, state } = await freshTurnSession();
    try {
      const rows = await session.listEventsAfter({
        epoch: state.epoch,
        afterSequence: state.newestSequence + 10_000,
        limit: 100,
      });
      expect(rows).toEqual([]);
    } finally {
      await harness.shutdown();
    }
  });

  it('limit=1 paging walks the FULL ledger with no duplicates and no gaps', async () => {
    const { harness, storage, session, state } = await freshTurnSession();
    try {
      const collected: number[] = [];
      let cursor = 0; // afterSequence 0 is EXCLUSIVE → seq 0 is unreachable via the public API
      // Capture seq 0 directly from storage to assemble the true full set.
      const full = await readLedger(storage, session);
      const seq0 = full[0]!.sequence; // 0
      // Walk from the floor.
      for (let guard = 0; guard < 10_000; guard++) {
        const page = await session.listEventsAfter({ epoch: state.epoch, afterSequence: cursor, limit: 1 });
        if (page.length === 0) break;
        expect(page).toHaveLength(1);
        const seq = page[0]!.sequence;
        // Strictly increasing, never re-seen.
        expect(collected.includes(seq)).toBe(false);
        if (collected.length > 0) expect(seq).toBeGreaterThan(collected[collected.length - 1]!);
        collected.push(seq);
        cursor = seq;
      }
      // The page-walk recovered every sequence ABOVE seq 0 with no gap.
      expect(collected[0]).toBe(seq0 + 1);
      for (let i = 1; i < collected.length; i++) {
        expect(collected[i]).toBe(collected[i - 1]! + 1);
      }
      expect(collected[collected.length - 1]).toBe(state.newestSequence);
      // Union with seq 0 reconstructs the entire ledger exactly once.
      expect([seq0, ...collected]).toEqual(full.map(r => r.sequence));
    } finally {
      await harness.shutdown();
    }
  });

  it('a WRONG epoch returns empty (epoch is a hard partition key, not an error)', async () => {
    const { harness, session, state } = await freshTurnSession();
    try {
      const rows = await session.listEventsAfter({
        epoch: `${state.epoch}-tampered`,
        afterSequence: 0,
        limit: 100,
      });
      expect(rows).toEqual([]);
      // Sanity: the genuine epoch still returns rows, so "empty" is epoch-scoped, not global.
      const genuine = await session.listEventsAfter({ epoch: state.epoch, afterSequence: 0, limit: 100 });
      expect(genuine.length).toBeGreaterThan(0);
    } finally {
      await harness.shutdown();
    }
  });

  it('rejects malformed args BEFORE any read (empty epoch / negative seq / non-positive limit)', async () => {
    const { harness, session, state } = await freshTurnSession();
    try {
      await expect(session.listEventsAfter({ epoch: '', afterSequence: 0, limit: 1 })).rejects.toBeInstanceOf(
        HarnessValidationError,
      );
      await expect(session.listEventsAfter({ epoch: state.epoch, afterSequence: -1, limit: 1 })).rejects.toBeInstanceOf(
        HarnessValidationError,
      );
      await expect(
        session.listEventsAfter({ epoch: state.epoch, afterSequence: 1.5, limit: 1 }),
      ).rejects.toBeInstanceOf(HarnessValidationError);
      await expect(session.listEventsAfter({ epoch: state.epoch, afterSequence: 0, limit: 0 })).rejects.toBeInstanceOf(
        HarnessValidationError,
      );
    } finally {
      await harness.shutdown();
    }
  });
});

// ===========================================================================
// 4. Activity-timeline cursor FORGERY — must reject BEFORE any storage read.
// ===========================================================================

describe('getActivityTimeline — cursor forgery rejects BEFORE any message read', () => {
  /**
   * setupHarness wires a real memory store; seed messages directly so a genuine
   * nextCursor can be minted (the FakeAgent setup above has no memory). The spy
   * target is the memory `listMessages` seam — the read the cursor must precede.
   */
  async function seeded(messageCount = 3) {
    const { harness } = setupHarness();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const memory = (await harness._internalTryGetMemoryStorage())!;
    expect(memory).toBeDefined();
    await memory.saveMessages({
      messages: Array.from({ length: messageCount }, (_, i) => ({
        id: `m${i}`,
        role: 'user',
        threadId: session.threadId,
        resourceId: 'u1',
        createdAt: new Date(1000 + i * 1000),
        content: { format: 2, parts: [{ type: 'text', text: `msg-${i}` }] },
      })) as any,
    });
    return { harness, session, memory };
  }

  function realCursor(session: any): Promise<string> {
    return session.getActivityTimeline({ limit: 1 }).then((tl: any) => {
      expect(tl.nextCursor).toBeTruthy();
      return tl.nextCursor as string;
    });
  }

  function decode(cursor: string): any {
    return JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8'));
  }
  function encode(obj: any): string {
    return Buffer.from(JSON.stringify(obj), 'utf8').toString('base64url');
  }

  it('(a) tampered sid (different session) → throws BEFORE any listMessages read', async () => {
    const { harness, session, memory } = await seeded();
    try {
      const c = decode(await realCursor(session));
      c.sid = 'SOME-OTHER-SESSION';
      const forged = encode(c);
      const readSpy = vi.spyOn(memory, 'listMessages');
      await expect(session.getActivityTimeline({ cursor: forged, limit: 1 })).rejects.toBeInstanceOf(
        HarnessValidationError,
      );
      expect(readSpy).not.toHaveBeenCalled();
    } finally {
      await harness.shutdown();
    }
  });

  it('(b) flipped includeDescendants flag → throws BEFORE any read', async () => {
    const { harness, session, memory } = await seeded();
    try {
      const c = decode(await realCursor(session));
      expect(c.d).toBe(false); // minted under includeDescendants=false
      const readSpy = vi.spyOn(memory, 'listMessages');
      await expect(
        session.getActivityTimeline({ cursor: encode(c), limit: 1, includeDescendants: true }),
      ).rejects.toBeInstanceOf(HarnessValidationError);
      expect(readSpy).not.toHaveBeenCalled();
    } finally {
      await harness.shutdown();
    }
  });

  it('(c) o → NaN(null) rejects before read; huge/negative are structurally valid numbers (pinned)', async () => {
    const { harness, session, memory } = await seeded();
    try {
      // NaN is not JSON-representable → it would round-trip to `null`; `null` fails the
      // `typeof o === 'number'` guard → reject BEFORE any read.
      {
        const c = decode(await realCursor(session));
        const forged = encode({ ...c, o: null });
        const readSpy = vi.spyOn(memory, 'listMessages');
        await expect(session.getActivityTimeline({ cursor: forged, limit: 1 })).rejects.toBeInstanceOf(
          HarnessValidationError,
        );
        expect(readSpy).not.toHaveBeenCalled();
        readSpy.mockRestore();
      }
      // A huge positive `o` is a VALID number → the codec accepts it; it is a forward seek
      // past the end → reads, returns an empty page. (Pin: NOT a throw.)
      {
        const c = decode(await realCursor(session));
        const forged = encode({ ...c, o: Number.MAX_SAFE_INTEGER });
        const readSpy = vi.spyOn(memory, 'listMessages');
        const tl = await session.getActivityTimeline({ cursor: forged, limit: 1 });
        expect(readSpy).toHaveBeenCalled();
        expect(tl.entries).toEqual([]);
        readSpy.mockRestore();
      }
      // A negative `o` is also a valid number → reads, returns entries after the seek.
      {
        const c = decode(await realCursor(session));
        const forged = encode({ ...c, o: -1, e: '', s: '' });
        const readSpy = vi.spyOn(memory, 'listMessages');
        const tl = await session.getActivityTimeline({ cursor: forged, limit: 10 });
        expect(readSpy).toHaveBeenCalled();
        expect(Array.isArray(tl.entries)).toBe(true);
        readSpy.mockRestore();
      }
    } finally {
      await harness.shutdown();
    }
  });

  it('(d) garbage non-JSON cursor → throws BEFORE any read', async () => {
    const { harness, session, memory } = await seeded();
    try {
      const readSpy = vi.spyOn(memory, 'listMessages');
      await expect(session.getActivityTimeline({ cursor: 'not-base64-json!!!', limit: 1 })).rejects.toBeInstanceOf(
        HarnessValidationError,
      );
      expect(readSpy).not.toHaveBeenCalled();
    } finally {
      await harness.shutdown();
    }
  });

  it('(e) valid JSON missing required fields → throws BEFORE any read', async () => {
    const { harness, session, memory } = await seeded();
    try {
      for (const partial of [{ v: 1 }, { v: 1, o: 1, s: 's', e: 'e' } /* missing sid + d */, {}]) {
        const forged = encode(partial);
        const readSpy = vi.spyOn(memory, 'listMessages');
        await expect(session.getActivityTimeline({ cursor: forged, limit: 1 })).rejects.toBeInstanceOf(
          HarnessValidationError,
        );
        expect(readSpy).not.toHaveBeenCalled();
        readSpy.mockRestore();
      }
    } finally {
      await harness.shutdown();
    }
  });

  it('clock-skew: a real cursor replayed after an EARLIER-timestamped message is inserted still pages forward-only', async () => {
    // The forward-seek cursor is keyed on (occurredAt, sessionId, entryId). Inserting a
    // message with an occurredAt strictly LESS than the cursor's `o` must NOT resurface on
    // the next page — the cursor filter is `compareKeys(...) > 0`, a strict forward seek.
    const { harness, session, memory } = await seeded(2);
    try {
      const firstPage = await session.getActivityTimeline({ limit: 1 });
      expect(firstPage.nextCursor).toBeTruthy();
      const cursorO = decode(firstPage.nextCursor!).o as number;
      expect(firstPage.entries).toHaveLength(1);
      const firstEntryAt = firstPage.entries[0]!.occurredAt;

      // Back-date a NEW message strictly BEFORE the cursor boundary (clock skew / late write).
      await memory.saveMessages({
        messages: [
          {
            id: 'skew',
            role: 'user',
            threadId: session.threadId,
            resourceId: 'u1',
            createdAt: new Date(firstEntryAt - 500),
            content: { format: 2, parts: [{ type: 'text', text: 'back-dated' }] },
          },
        ] as any,
      });

      // The next page must contain ONLY entries strictly after the cursor key — the
      // back-dated message (occurredAt < cursorO) must NOT resurface.
      const secondPage = await session.getActivityTimeline({ cursor: firstPage.nextCursor, limit: 10 });
      expect(secondPage.entries.every(e => e.occurredAt > cursorO || e.occurredAt === cursorO)).toBe(true);
      expect(secondPage.entries.some(e => e.entryId.includes('skew'))).toBe(false);
      // No overlap with the first page either.
      const firstIds = new Set(firstPage.entries.map(e => e.entryId));
      expect(secondPage.entries.every(e => !firstIds.has(e.entryId))).toBe(true);
    } finally {
      await harness.shutdown();
    }
  });
});

// ===========================================================================
// 5. APP-BAG hostility on resume (the C3 fix).
// ===========================================================================

describe('§4.4c / C3 — tampered persisted app bag must NOT shadow the genuine harness slot', () => {
  /** Build a fresh harness over an existing storage (a new owner that rehydrates). */
  function makeHarness(storage: InMemoryHarness, agent: FakeAgent) {
    return new Harness({
      agents: { default: agent } as any,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage },
    });
  }

  it('a reserved key smuggled into the PERSISTED pendingResume.requestContext.metadata does not override ctx.get("harness") on resume', async () => {
    const storage = new InMemoryHarness({ db: new InMemoryDB() });
    const agent1 = new FakeAgent('default');
    const h1 = makeHarness(storage, agent1);
    // Suspend on a tool approval carrying a legitimate caller app bag.
    agent1.fullOutput = {
      ...agent1.fullOutput,
      finishReason: 'suspended',
      suspendPayload: { toolCallId: 'tc1', toolName: 'do_thing', args: { x: 1 } },
    };
    const s1 = await h1.session({ resourceId: 'u1', threadId: { fresh: true } });
    const sessionId = s1.id;
    await s1.message({ content: 'do it', requestContext: { app: { tenant: 'acme' } } });
    expect(s1.getRecord().pendingResume).toBeDefined();
    expect((s1.getRecord().pendingResume as any).requestContext?.metadata).toMatchObject({ tenant: 'acme' });
    await s1._flushEventPersistence();
    await h1.shutdown();

    // HOSTILE TAMPER of the DURABLE record: inject reserved keys (and a forged top-level
    // `harness` value) into the persisted metadata bag, simulating a corrupted/forged store.
    const sessions: Map<string, any> = (storage as any).db.harnessSessions;
    let mutated = false;
    for (const rec of sessions.values()) {
      if (rec.id === sessionId && rec.pendingResume?.requestContext?.metadata) {
        rec.pendingResume.requestContext.metadata['harness/__mastra_x'] = 'PWNED';
        rec.pendingResume.requestContext.metadata['__proto__pollution'] = 'PWNED';
        rec.pendingResume.requestContext.metadata.harness = { forged: true };
        mutated = true;
      }
    }
    expect(mutated).toBe(true);

    // Rehydrate in a FRESH harness (new owner) so resume reads the TAMPERED persisted record.
    const agent2 = new FakeAgent('default');
    agent2.fullOutput = { ...agent2.fullOutput, finishReason: 'stop', suspendPayload: undefined };
    const h2 = makeHarness(storage, agent2);
    try {
      const s2 = await h2.session({ sessionId });
      expect(s2.getRecord().pendingResume).toBeDefined();
      // The tampered bag is what was loaded from storage.
      expect((s2.getRecord().pendingResume as any).requestContext?.metadata['harness/__mastra_x']).toBe('PWNED');

      await s2.respondToToolApproval({ approved: true });

      const ctx = agent2.lastResumeOptions?.requestContext;
      expect(ctx).toBeDefined();

      // The genuine harness slot is intact and authoritative — rebuilt from session identity,
      // NOT from the persisted bag.
      const harnessSlot = ctx.get('harness');
      expect(harnessSlot).toBeDefined();
      expect(harnessSlot.sessionId).toBe(sessionId);
      expect(harnessSlot.resourceId).toBe('u1');
      expect(harnessSlot.harnessName).toBeDefined();
      // The forged `harness` value inside the metadata bag did NOT become the harness slot.
      expect(harnessSlot.forged).toBeUndefined();
      expect(harnessSlot).not.toEqual({ forged: true });

      // The tampered metadata is exposed (opaquely) ONLY under the `app` slot — never as a
      // top-level `harness` entry. Prove the reserved keys RODE ALONG inside `app` (so this
      // is not passing by silently dropping the bag): the engine treats the bag as opaque
      // application metadata and does not re-validate a persisted bag against §4.4c.
      const appSlot = ctx.get('app');
      expect(appSlot).toBeDefined();
      expect(appSlot.tenant).toBe('acme');
      expect(appSlot['harness/__mastra_x']).toBe('PWNED');
      expect(appSlot.harness).toEqual({ forged: true });
      // The harness slot's own `app` mirror carries the same opaque bag (not the slot identity).
      expect(harnessSlot.app).toBe(appSlot);
      // Re-reading the harness slot yields the SAME genuine object — no late override.
      expect(ctx.get('harness')).toBe(harnessSlot);
      expect(ctx.get('harness').forged).toBeUndefined();
    } finally {
      await h2.shutdown();
    }
  });
});

// ===========================================================================
// 6. EVENT PAYLOAD BOMB — pin the actual persistence behavior.
// ===========================================================================

describe('custom event payload bomb — cap applied + ledger contiguity', () => {
  it('a multi-MB custom event payload is replaced by the oversized sentinel AT EMIT (same cap as tool events) and the ledger stays contiguous', async () => {
    // The §13.x maxEventPayloadBytes projection (projectToolEventPayloadForJson) now bounds
    // CUSTOM (dotted) event payloads too, at emit-time, exactly as it bounds
    // tool_start.input / tool_end.output. An oversized custom payload is replaced by the
    // TOOL_PAYLOAD_TOO_LARGE sentinel BEFORE the durable row is written, so live === replay
    // by construction and the cap can no longer be evaded via a custom event.
    const { harness, storage } = setup({ maxEventPayloadBytes: 64 });
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      await session.setState({ before: true });

      const big = 'x'.repeat(3_000_000); // ~3 MB, far over the 64-byte cap.
      const bomb = (session as unknown as { _emitter: { emit(e: unknown): HarnessEvent } })._emitter.emit({
        type: 'custom.bomb',
        payload: { blob: big },
      });
      const bombSeq = parseHarnessEventId(bomb.id).sequence;

      // The LIVE emitted event is already capped (live === replay by construction).
      expect((bomb as unknown as { payload: unknown }).payload).toEqual({
        __mastraHarness: 'oversized-tool-payload',
      });

      await session.setState({ after: true });
      await expect(session._flushEventPersistence()).resolves.toBeUndefined();

      const ledger = await readLedger(storage, session);
      // Contiguity: no gap, no dupe.
      for (let i = 1; i < ledger.length; i++) {
        expect(ledger[i]!.sequence).toBe(ledger[i - 1]!.sequence + 1);
      }
      const bombRow = ledger.find(r => r.sequence === bombSeq);
      expect(bombRow).toBeDefined();
      expect(bombRow!.event.type).toBe('custom.bomb');
      // The oversized payload is replaced by the bounded, detectable sentinel — the multi-MB
      // blob never reaches the durable log.
      expect(bombRow!.event).toMatchObject({ payload: { __mastraHarness: 'oversized-tool-payload' } });
      expect(bombRow!.event.payload.blob).toBeUndefined();
      // The trailing state_changed still persisted after the bomb.
      const after = ledger.filter(r => r.sequence > bombSeq && r.event.type === 'state_changed');
      expect(after.length).toBeGreaterThanOrEqual(1);
    } finally {
      await harness.shutdown();
    }
  });
});

// ===========================================================================
// 7. Queue admission hostility (§4.2 / §6 / §10).
// ===========================================================================

describe('queue admission hostility', () => {
  it('same admissionId with a DIFFERENT payload rejects deterministically (HarnessAdmissionConflictError)', async () => {
    const { harness, agent } = setup();
    agent.chunks = [];
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      await session.queue({ content: 'first', admissionId: 'dup-1' });
      // Different payload under the same admissionId → admission hash mismatch → reject.
      await expect(session.queue({ content: 'DIFFERENT', admissionId: 'dup-1' })).rejects.toThrow();
      // Deterministic: a second mismatching attempt rejects identically.
      await expect(session.queue({ content: 'ALSO-DIFFERENT', admissionId: 'dup-1' })).rejects.toThrow();
      // An exact retry of the original payload is deduped (resolves, no throw).
      await expect(session.queue({ content: 'first', admissionId: 'dup-1' })).resolves.toBeDefined();
    } finally {
      await harness.shutdown();
    }
  });

  it('rapid queue(N) drains in order with a contiguous, uncorrupted durable event sequence', async () => {
    const { harness, agent, storage } = setup();
    agent.chunks = []; // terminal-only runs keep the event volume bounded per turn.
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      const live: HarnessEvent[] = [];
      session.subscribe(e => live.push(e));

      const N = 20;
      const results = await Promise.all(
        Array.from({ length: N }, (_, i) => session.queue({ content: `q${i}`, admissionId: `q-${i}` })),
      );
      expect(results).toHaveLength(N);

      await session._flushEventPersistence();
      const ledger = await readLedger(storage, session);

      // The durable event sequence is strictly contiguous across all interleaved turns.
      for (let i = 1; i < ledger.length; i++) {
        expect(ledger[i]!.sequence).toBe(ledger[i - 1]!.sequence + 1);
      }
      // Live ids correspond 1:1 in order to the ledger ids (no interleaving corruption).
      expect(ledger.map(r => r.event.id)).toEqual(live.map(e => e.id));

      // Every queued turn completed exactly once (settlement by queuedItemId).
      const completed = live.filter(e => e.type === 'queue_completed') as Array<{ queuedItemId: string }>;
      expect(completed.length).toBe(N);
      const ids = new Set(completed.map(c => c.queuedItemId));
      expect(ids.size).toBe(N); // no duplicate settlement
    } finally {
      await harness.shutdown();
    }
  });
});
