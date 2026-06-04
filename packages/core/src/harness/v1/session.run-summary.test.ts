/**
 * Harness v1 — span-summary Slice A (S3/S4/O1/O2/O6): the canonical
 * `run_completed` event + `tool_end.durationMs` + per-run tool rollup.
 *
 * Covers:
 *   - run_completed emitted exactly once on a normal terminal, with the run's
 *     identity, wall-clock timing, finishReason→status, per-run usage, and the
 *     compact tool rollup; tool_end carries durationMs.
 *   - status mapping (complete→completed, error→failed, aborted→interrupted).
 *   - finishReason 'suspended' is NOT terminal → no run_completed.
 *   - dedup: a runId finalizes at most once across duplicate terminal paths.
 *   - reconstructed: a terminal with no open run span omits start/duration/
 *     rollup but still reports usage + identity.
 */

import { describe, expect, it } from 'vitest';

import { Agent } from '../../agent';
import { InMemoryHarness } from '../../storage/domains/harness/inmemory';
import { InMemoryDB } from '../../storage/domains/inmemory-db';
import { buildFakeOutput } from './__test-utils__/fake-output';

import type { HarnessEvent, RunCompletedEvent } from './events';
import { Harness } from './harness';

class FakeAgent extends Agent<any, any, any> {
  chunks: any[] = [];
  fullOutput: any = {
    text: 'ok',
    usage: { inputTokens: 3, outputTokens: 5, totalTokens: 8 },
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
    totalUsage: { inputTokens: 3, outputTokens: 5, totalTokens: 8 },
    error: undefined,
    tripwire: undefined,
    traceId: undefined,
    spanId: undefined,
    runId: 'fake-run',
    suspendPayload: undefined,
    messages: [],
    rememberedMessages: [],
  };

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
    return this.stream(undefined, options);
  }
}

function setup() {
  const agent = new FakeAgent('default');
  const storage = new InMemoryHarness({ db: new InMemoryDB() });
  const harness = new Harness({
    agents: { default: agent } as any,
    modes: [{ id: 'default', agentId: 'default' }],
    defaultModeId: 'default',
    sessions: { storage },
  });
  return { harness, agent, storage };
}

const USAGE = { promptTokens: 1, completionTokens: 2, totalTokens: 3 };

describe('span-summary — run_completed event (Slice A)', () => {
  it('emits run_completed once on a normal terminal, with identity, timing, usage, and tool rollup', async () => {
    const { harness, agent } = setup();
    // Chunks inherit the run's minted runId (as a real agent's chunks do) so the
    // tool events share the run id under which the span was opened.
    agent.chunks = [
      { type: 'tool-call', payload: { toolCallId: 'tc1', toolName: 'lookup', args: { q: 'x' } } },
      { type: 'tool-result', payload: { toolCallId: 'tc1', result: { hits: 3 } } },
    ];
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const events: HarnessEvent[] = [];
    session.subscribe(e => events.push(e));

    await session.message({ content: 'hi' });

    const completed = events.filter(e => e.type === 'run_completed') as RunCompletedEvent[];
    expect(completed).toHaveLength(1);
    const rc = completed[0]!;
    const agentEnd = events.find(e => e.type === 'agent_end') as { runId: string };
    expect(rc.runId).toBe(agentEnd.runId);
    expect(rc.status).toBe('completed');
    expect(rc.reconstructed).toBeUndefined();
    expect(rc.sessionId).toBe(session.id);
    expect(rc.resourceId).toBe('u1');
    expect(rc.threadId).toBe(session.threadId);
    expect(rc.agentId).toBe('default');
    expect(rc.modeId).toBe('default');
    expect(typeof rc.startedAt).toBe('number');
    expect(typeof rc.completedAt).toBe('number');
    expect(rc.durationMs).toBeGreaterThanOrEqual(0);
    expect(rc.usage).toBeDefined();
    // run_completed follows agent_end.
    const agentEndIdx = events.findIndex(e => e.type === 'agent_end');
    const rcIdx = events.findIndex(e => e.type === 'run_completed');
    expect(rcIdx).toBeGreaterThan(agentEndIdx);
    // Tool rollup + per-tool breakdown.
    expect(rc.toolRollup?.count).toBe(1);
    expect(rc.toolRollup?.errors).toBe(0);
    expect(rc.toolRollup?.perTool.lookup?.count).toBe(1);
    // tool_end carries a numeric durationMs.
    const end = events.find(e => e.type === 'tool_end') as { durationMs?: number } | undefined;
    expect(typeof end?.durationMs).toBe('number');
  });

  it('counts an errored tool in the rollup errors', async () => {
    const { harness, agent } = setup();
    agent.chunks = [
      { type: 'tool-call', payload: { toolCallId: 'tc1', toolName: 'lookup', args: { q: 'x' } } },
      { type: 'tool-error', payload: { toolCallId: 'tc1', error: 'boom' } },
    ];
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const events: HarnessEvent[] = [];
    session.subscribe(e => events.push(e));

    await session.message({ content: 'hi' });

    const rc = events.find(e => e.type === 'run_completed') as RunCompletedEvent | undefined;
    expect(rc?.toolRollup?.count).toBe(1);
    expect(rc?.toolRollup?.errors).toBe(1);
    expect(rc?.toolRollup?.perTool.lookup?.errors).toBe(1);
  });

  it('maps finishReason to status and is a no-op on suspended (terminal-only)', async () => {
    const { harness } = setup();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const completed: RunCompletedEvent[] = [];
    session.subscribe(e => {
      if (e.type === 'run_completed') completed.push(e as RunCompletedEvent);
    });

    // suspended is NOT terminal — emits nothing.
    (session as any)._emitTurnEvent({ type: 'agent_end', runId: 'r-susp', finishReason: 'suspended', usage: USAGE });
    expect(completed).toHaveLength(0);

    (session as any)._emitTurnEvent({ type: 'agent_end', runId: 'r-ok', finishReason: 'complete', usage: USAGE });
    (session as any)._emitTurnEvent({ type: 'agent_end', runId: 'r-err', finishReason: 'error', usage: USAGE });
    (session as any)._emitTurnEvent({ type: 'agent_end', runId: 'r-abrt', finishReason: 'aborted', usage: USAGE });

    expect(completed.map(c => [c.runId, c.status])).toEqual([
      ['r-ok', 'completed'],
      ['r-err', 'failed'],
      ['r-abrt', 'interrupted'],
    ]);
  });

  it('finalizes a runId at most once across duplicate terminal paths', async () => {
    const { harness } = setup();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const completed: RunCompletedEvent[] = [];
    session.subscribe(e => {
      if (e.type === 'run_completed') completed.push(e as RunCompletedEvent);
    });

    (session as any)._emitTurnEvent({ type: 'agent_end', runId: 'dup', finishReason: 'complete', usage: USAGE });
    // A second terminal for the same run (e.g. error-then-cleanup) must NOT re-emit.
    (session as any)._emitTurnEvent({ type: 'agent_end', runId: 'dup', finishReason: 'error', usage: USAGE });

    expect(completed).toHaveLength(1);
    expect(completed[0]!.status).toBe('completed');
  });

  it('marks a terminal with no open run span as reconstructed (omits start/duration/rollup, keeps usage + identity)', async () => {
    const { harness } = setup();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const completed: RunCompletedEvent[] = [];
    session.subscribe(e => {
      if (e.type === 'run_completed') completed.push(e as RunCompletedEvent);
    });

    // No prior agent_start opened a span for this runId (≈ a run that began
    // before a process restart and completes after).
    (session as any)._emitTurnEvent({ type: 'agent_end', runId: 'r-recon', finishReason: 'complete', usage: USAGE });

    expect(completed).toHaveLength(1);
    const rc = completed[0]!;
    expect(rc.reconstructed).toBe(true);
    expect(rc.startedAt).toBeUndefined();
    expect(rc.durationMs).toBeUndefined();
    expect(rc.toolRollup).toBeUndefined();
    expect(rc.usage).toEqual(USAGE);
    expect(rc.resourceId).toBe('u1');
    expect(rc.agentId).toBe('default');
  });
});

describe('span-summary — durable run history (Slice B)', () => {
  it('persists a completed run summary readable via loadRunSummary + listRunSummaries', async () => {
    const { harness, agent, storage } = setup();
    agent.chunks = [
      { type: 'tool-call', payload: { toolCallId: 'tc1', toolName: 'lookup', args: { q: 'x' } } },
      { type: 'tool-result', payload: { toolCallId: 'tc1', result: { ok: true } } },
    ];
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const completed: RunCompletedEvent[] = [];
    session.subscribe(e => {
      if (e.type === 'run_completed') completed.push(e as RunCompletedEvent);
    });

    await session.message({ content: 'hi' });
    await (session as any)._internalFlushRunSummaries();

    const runId = completed[0]!.runId;
    const row = await storage.loadRunSummary({ runId });
    expect(row).not.toBeNull();
    expect(row!.runId).toBe(runId);
    expect(row!.sessionId).toBe(session.id);
    expect(row!.status).toBe('completed');
    expect(row!.reconstructed).toBe(false);
    expect(typeof row!.startedAt).toBe('number');
    expect(typeof row!.durationMs).toBe('number');
    expect(row!.toolRollup?.count).toBe(1);
    expect(row!.agentId).toBe('default');

    const listed = await storage.listRunSummaries({ sessionId: session.id });
    expect(listed.summaries.map(s => s.runId)).toContain(runId);
  });

  it('persists a reconstructed run with null start/duration/rollup but real usage + identity', async () => {
    const { harness, storage } = setup();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    (session as any)._emitTurnEvent({ type: 'agent_end', runId: 'r-recon', finishReason: 'error', usage: USAGE });
    await (session as any)._internalFlushRunSummaries();

    const row = await storage.loadRunSummary({ runId: 'r-recon' });
    expect(row).not.toBeNull();
    expect(row!.reconstructed).toBe(true);
    expect(row!.status).toBe('failed');
    expect(row!.startedAt).toBeUndefined();
    expect(row!.durationMs).toBeUndefined();
    expect(row!.toolRollup).toBeUndefined();
    expect(row!.usage).toEqual(USAGE);
    expect(row!.sessionId).toBe(session.id);
  });

  it('is idempotent — a duplicate terminal does not overwrite the first summary', async () => {
    const { harness, storage } = setup();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    // Direct storage idempotency: first write wins.
    const base = {
      harnessName: 'default',
      runId: 'r-idem',
      sessionId: session.id,
      resourceId: 'u1',
      threadId: session.threadId,
      agentId: 'default',
      modeId: 'default',
      modelId: '',
      status: 'completed' as const,
      finishReason: 'complete',
      reconstructed: false,
      completedAt: 100,
      usage: USAGE,
      createdAt: 100,
    };
    const first = await storage.saveRunSummary({ summary: { ...base, durationMs: 5 } });
    const second = await storage.saveRunSummary({ summary: { ...base, durationMs: 999, status: 'failed' } });
    expect(first.durationMs).toBe(5);
    expect(second.durationMs).toBe(5);
    expect(second.status).toBe('completed');
  });

  it('composite keyset paging returns same-completedAt rows without skipping (InMemory)', async () => {
    const { harness, storage } = setup();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const mk = (runId: string, completedAt: number) => ({
      harnessName: 'default',
      runId,
      sessionId: session.id,
      resourceId: 'u1',
      threadId: session.threadId,
      agentId: 'default',
      modeId: 'default',
      modelId: '',
      status: 'completed' as const,
      finishReason: 'complete',
      reconstructed: false,
      completedAt,
      usage: USAGE,
      createdAt: completedAt,
    });
    // Two rows share completedAt=200.
    for (const s of [mk('a', 300), mk('b', 200), mk('c', 200), mk('d', 100)]) {
      await storage.saveRunSummary({ summary: s });
    }
    const seen: string[] = [];
    let cursorC: number | undefined;
    let cursorR: string | undefined;
    for (let i = 0; i < 10; i++) {
      const p = await storage.listRunSummaries({ sessionId: session.id, limit: 1, beforeCompletedAt: cursorC, beforeRunId: cursorR });
      seen.push(...p.summaries.map(s => s.runId));
      if (p.nextBeforeCompletedAt === undefined) break;
      cursorC = p.nextBeforeCompletedAt;
      cursorR = p.nextBeforeRunId;
    }
    // (completedAt DESC, runId DESC): a(300), c(200), b(200), d(100).
    expect(seen).toEqual(['a', 'c', 'b', 'd']);
  });
});
