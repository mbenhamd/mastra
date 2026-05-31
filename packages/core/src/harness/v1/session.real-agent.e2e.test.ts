/**
 * Harness v1 — REAL-agent E2E (NOT MockAgent).
 *
 * Every other harness v1 test hands the harness a duck-typed `MockAgent` /
 * `FakeAgent` that overrides `stream` / `generate` / `resumeStream` WITHOUT
 * calling super, hand-staging a fabricated `MastraModelOutput`. That validates
 * the harness call shape, but it never exercises the real
 *   ai-sdk → provider → transform → loop → fullStream
 * path. The harness chunk→event mapping (`_emitForChunk`, session.ts:4225) reads
 * `chunk.payload.text` / `.toolCallId` / `.args` / `.result` off the REAL
 * `payload`-nested chunks that `aisdk/v5/transform.ts` produces — none of which
 * the MockAgent path can produce.
 *
 * This file binds a REAL `Agent` (driven by a deterministic
 * `MockLanguageModelV2`, the same provider-level mock the agent/loop tests use)
 * into a real `Harness` over `InMemoryStore`, and drives
 * `session.message` / `session.signal` / `session.queue` / `session.respondTo*`
 * through the genuine loop. It asserts:
 *
 *   S1  real streamed text-delta chunks surface as harness `text_delta` events
 *   S2  a real tool-call surfaces `tool_start`/`tool_end` with JSON-safe
 *       projected payloads, and the tool sees the harness RequestContext
 *   S3  suspend via an approval-requesting tool → `respondToToolApproval` →
 *       resume drains the continuation LIVE (§10.4: a live `tool_end` +
 *       post-approval `text_delta` precede the terminal `agent_end:complete`)
 *   S4  `signal_completed` / `queue_completed` settlement evidence is written
 *   S5  provider/model error (before run start AND mid-stream) surfaces as the
 *       right harness error / `agent_end:error` (redacted public error), and
 *       the in-process `message()` rejection is REDACTED too (§13.3f.1): its
 *       `.message` is generic and the raw provider detail is kept on `.cause`
 *   S6  abort terminalizes the real run: a MID-RUN abort (fired from inside a
 *       running tool) drives the real `turnAbortController` path to an
 *       `agent_end:aborted` terminal; an ALREADY-aborted caller signal instead
 *       rejects `message()` pre-dispatch with `agent_aborted` and emits NO
 *       harness events (no run is ever dispatched).
 *
 * No `MockAgent` / `FakeAgent` is used in this file — the agent is the real
 * `Agent` class; only the LANGUAGE MODEL is mocked (deterministic, no network).
 */

import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod';

import { convertArrayToReadableStream, MockLanguageModelV2 } from '../../agent/__tests__/mock-model';
import { Agent } from '../../agent';
import { InMemoryStore } from '../../storage';
import { createTool } from '../../tools';

import type { HarnessEvent } from './events';
import { Harness } from './harness';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const testUsage = { inputTokens: 10, outputTokens: 20, totalTokens: 30 };

/** A raw provider stream that emits text in N deltas then finishes with `stop`. */
function textStream(deltas: string[]) {
  return convertArrayToReadableStream([
    { type: 'stream-start', warnings: [] },
    { type: 'response-metadata', id: 'id-text', modelId: 'mock-model-id', timestamp: new Date(0) },
    { type: 'text-start', id: 'text-1' },
    ...deltas.map(delta => ({ type: 'text-delta', id: 'text-1', delta })),
    { type: 'text-end', id: 'text-1' },
    { type: 'finish', finishReason: 'stop', usage: testUsage },
  ]);
}

/** A raw provider stream that calls `toolName` with `inputJson`, finishing `tool-calls`. */
function toolCallStream(toolCallId: string, toolName: string, inputJson: string) {
  return convertArrayToReadableStream([
    { type: 'stream-start', warnings: [] },
    { type: 'response-metadata', id: 'id-tool', modelId: 'mock-model-id', timestamp: new Date(0) },
    { type: 'tool-call', toolCallId, toolName, input: inputJson, providerExecuted: false },
    { type: 'finish', finishReason: 'tool-calls', usage: testUsage },
  ]);
}

function newHarness(agent: Agent<any, any, any>) {
  return new Harness({
    agents: { default: agent } as any,
    storage: new InMemoryStore(),
    modes: [{ id: 'default', agentId: 'default' }],
    defaultModeId: 'default',
  });
}

/** Wait until `predicate()` is true, polling the microtask/timer queue. */
async function waitFor(predicate: () => boolean, label: string, timeoutMs = 2000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${label}`);
    await new Promise(resolve => setImmediate(resolve));
  }
}

// ===========================================================================
// S1 — real streamed text-delta → harness text_delta events
// ===========================================================================

describe('Harness v1 real-agent E2E — S1 streamed text', () => {
  it('maps N real provider text-delta chunks to N harness text_delta events (concatenated == model text)', async () => {
    const deltas = ['Hello', ', ', 'world!'];
    const model = new MockLanguageModelV2({
      doStream: async () => ({
        rawCall: { rawPrompt: null, rawSettings: {} },
        warnings: [],
        stream: textStream(deltas),
      }),
    });
    const agent = new Agent({ id: 'default', name: 'default', instructions: 'reply', model });
    const harness = newHarness(agent);
    try {
      const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));

      const result = (await session.message({ content: 'hi' })) as any;

      // The real loop concatenated the deltas into the final text.
      expect(result.text).toBe(deltas.join(''));
      // The real loop carries genuine usage (NOT the MockAgent's fabricated 1/1/2).
      expect(result.usage.totalTokens).toBe(testUsage.totalTokens);

      const textDeltas = events.filter(e => e.type === 'text_delta') as Array<{ delta: string; runId: string }>;
      expect(textDeltas).toHaveLength(deltas.length);
      expect(textDeltas.map(e => e.delta)).toEqual(deltas);
      expect(textDeltas.map(e => e.delta).join('')).toBe(result.text);

      // agent_start precedes first text_delta; agent_end:complete follows last.
      const types = events.map(e => e.type);
      const startIdx = types.indexOf('agent_start');
      const firstDeltaIdx = types.indexOf('text_delta');
      const endIdx = types.lastIndexOf('agent_end');
      expect(startIdx).toBeGreaterThanOrEqual(0);
      expect(startIdx).toBeLessThan(firstDeltaIdx);
      expect(endIdx).toBeGreaterThan(types.lastIndexOf('text_delta'));
      const agentEnd = events.find(e => e.type === 'agent_end') as { finishReason: string; runId: string };
      expect(agentEnd.finishReason).toBe('complete');
      // runId is consistent across the turn's events.
      expect(textDeltas.every(e => e.runId === agentEnd.runId)).toBe(true);
    } finally {
      await harness.shutdown();
    }
  });
});

// ===========================================================================
// S2 — real tool-call/tool-result → tool_start/tool_end + JSON-safe payloads
//      + the tool sees the harness RequestContext
// ===========================================================================

describe('Harness v1 real-agent E2E — S2 tool round-trip', () => {
  it('emits tool_start/tool_end with JSON-safe projected payloads and passes RequestContext to the tool', async () => {
    const seenHarnessCtx: unknown[] = [];
    const findUser = createTool({
      id: 'findUser',
      description: 'look up a user',
      inputSchema: z.object({ name: z.string() }),
      // Return a non-JSON-native value (Date) to prove projection happens at emit.
      execute: async (input, context) => {
        // The harness injects its context under the 'harness' key (session.tool-context.test.ts).
        seenHarnessCtx.push(context?.requestContext?.get('harness'));
        return { name: (input as { name: string }).name, lastSeen: new Date(0) };
      },
    });

    let callCount = 0;
    const model = new MockLanguageModelV2({
      doStream: async () => {
        callCount++;
        if (callCount === 1) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream('call-1', 'findUser', '{"name":"Dero Israel"}'),
          };
        }
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: textStream(['Found ', 'Dero Israel']),
        };
      },
    });

    const agent = new Agent({
      id: 'default',
      name: 'default',
      instructions: 'use findUser',
      model,
      tools: { findUser },
    });
    const harness = newHarness(agent);
    try {
      const session = await harness.session({ resourceId: 'u-tool', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));

      const result = (await session.message({ content: 'find Dero Israel' })) as any;

      const toolStart = events.find(e => e.type === 'tool_start') as
        | { toolCallId: string; toolName: string; input: unknown; runId: string }
        | undefined;
      const toolEnd = events.find(e => e.type === 'tool_end') as
        | { toolCallId: string; toolName: string; output: any; isError: boolean; runId: string }
        | undefined;

      expect(toolStart).toBeDefined();
      expect(toolStart!.toolName).toBe('findUser');
      expect(toolStart!.toolCallId).toBe('call-1');
      // input is the parsed args, JSON-safe.
      expect(toolStart!.input).toEqual({ name: 'Dero Israel' });

      expect(toolEnd).toBeDefined();
      expect(toolEnd!.toolCallId).toBe('call-1');
      expect(toolEnd!.isError).toBe(false);
      expect(toolEnd!.output.name).toBe('Dero Israel');
      // The Date was projected to an ISO string at emit (projectToolEventPayloadForJson) —
      // a raw Date instance must NOT cross the harness event boundary.
      expect(toolEnd!.output.lastSeen).toBe(new Date(0).toISOString());
      expect(toolEnd!.output.lastSeen).not.toBeInstanceOf(Date);

      // Ordering: tool_start → tool_end → text_delta.
      const types = events.map(e => e.type);
      expect(types.indexOf('tool_start')).toBeLessThan(types.indexOf('tool_end'));
      expect(types.indexOf('tool_end')).toBeLessThan(types.lastIndexOf('text_delta'));

      // The tool actually ran inside the real loop and saw the harness context.
      expect(seenHarnessCtx).toHaveLength(1);
      expect(seenHarnessCtx[0]).toBeDefined();
      expect((seenHarnessCtx[0] as { sessionId?: string }).sessionId).toBe(session.id);

      // The real FullOutput carries real toolCalls/toolResults (proving the loop ran,
      // not a fabricated empty-array output).
      expect(Array.isArray(result.toolCalls)).toBe(true);
      expect(result.toolCalls.length).toBeGreaterThan(0);
      expect(result.toolResults.length).toBeGreaterThan(0);
    } finally {
      await harness.shutdown();
    }
  });
});

// ===========================================================================
// S3 — real approval suspend → respondToToolApproval → resume terminalizes
// ===========================================================================

describe('Harness v1 real-agent E2E — S3 approval suspend/resume', () => {
  it('a requireApproval tool suspends the real run; respondToToolApproval resumes it to complete', async () => {
    const findUser = createTool({
      id: 'findUser',
      description: 'look up a user',
      inputSchema: z.object({ name: z.string() }),
      requireApproval: true,
      execute: async input => {
        return { name: (input as { name: string }).name, email: 'dero@mail.com' };
      },
    });

    // Step 1: tool-call (suspends on approval). Step 2: post-approval text.
    let callCount = 0;
    const model = new MockLanguageModelV2({
      doStream: async () => {
        callCount++;
        if (callCount === 1) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream('call-approve', 'findUser', '{"name":"Dero Israel"}'),
          };
        }
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: textStream(['User is ', 'Dero Israel']),
        };
      },
    });

    const agent = new Agent({
      id: 'default',
      name: 'default',
      instructions: 'use findUser',
      model,
      tools: { findUser },
      // autoResume OFF (default): the suspend must surface to the harness so
      // respondToToolApproval drives the resume.
    });
    const harness = newHarness(agent);
    try {
      const session = await harness.session({ resourceId: 'u-approve', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));

      const suspended = (await session.message({ content: 'find Dero Israel' })) as any;
      expect(suspended.finishReason).toBe('suspended');

      // Pre-resume turn events (drained from the live subscription): the
      // approval-required tool surfaced a tool_start, an explicit
      // tool_approval_required, and a terminal agent_end:suspended.
      const preResumeTypes = events.map(e => e.type);
      expect(preResumeTypes).toContain('tool_start');
      expect(preResumeTypes).toContain('tool_approval_required');
      expect(
        events.some(e => e.type === 'agent_end' && (e as any).finishReason === 'suspended'),
      ).toBe(true);
      const toolStart = events.find(e => e.type === 'tool_start') as { toolName: string; input: unknown };
      expect(toolStart.toolName).toBe('findUser');
      expect(toolStart.input).toEqual({ name: 'Dero Israel' });

      // A real tool suspended mid-loop → harness captured a pending resume.
      const pending = session.getRecord().pendingResume;
      expect(pending).toBeDefined();
      expect(pending!.kind).toBe('tool-approval');
      expect(pending!.toolName).toBe('findUser');
      expect(pending!.runId).toBe(suspended.runId);

      const eventsAtSuspend = events.length;

      // Approve → real resumeStream runs the approved tool + post-approval turn.
      await session.respondToToolApproval({ approved: true });

      await waitFor(
        () => events.some(e => e.type === 'agent_end' && (e as any).finishReason === 'complete'),
        'agent_end:complete after resume',
      );
      // The resume terminalized and cleared the pending registration.
      expect(session.getRecord().pendingResume).toBeUndefined();

      // §10.4 (harnessv1/sections/10-events/04-ordering-guarantees.md): a
      // suspension event is "followed by either a tool_end (after resume) or an
      // agent_end (after abort)" on the live subscriber stream. The resume run
      // reuses the suspended run's `runId`, which the long-lived thread
      // subscription has already recorded in its `seenRunIds`, so that
      // subscription dedups the re-registered resume run. `_resume`
      // (session.ts) therefore drains the resume run's OWN `fullStream` through
      // `_emitForChunk` (`_drainResumeStream`) so the approved tool's `tool_end`
      // and the post-approval `text_delta` surface LIVE before the terminal
      // `agent_end:complete`. (A UI streams the continuation after approval; it
      // does not have to read the tool result from display-state / FullOutput.)
      const postResumeEvents = events.slice(eventsAtSuspend);
      const postResumeTypes = postResumeEvents.map(e => e.type);

      // The approved tool's result surfaced as a live `tool_end` after resume.
      const postResumeToolEnd = postResumeEvents.find(e => e.type === 'tool_end') as
        | { toolName: string; toolCallId: string; output: any; isError: boolean }
        | undefined;
      expect(postResumeToolEnd).toBeDefined();
      expect(postResumeToolEnd!.toolName).toBe('findUser');
      expect(postResumeToolEnd!.toolCallId).toBe('call-approve');
      expect(postResumeToolEnd!.isError).toBe(false);
      expect(postResumeToolEnd!.output).toEqual({ name: 'Dero Israel', email: 'dero@mail.com' });

      // The post-approval model text surfaced as live `text_delta`s.
      const postResumeText = (
        postResumeEvents.filter(e => e.type === 'text_delta') as Array<{ delta: string }>
      )
        .map(e => e.delta)
        .join('');
      expect(postResumeText).toBe('User is Dero Israel');

      // §10.4 ordering: the live `tool_end` precedes the terminal
      // `agent_end:complete` on the subscriber stream.
      const resumeToolEndIdx = postResumeTypes.indexOf('tool_end');
      const resumeAgentEndIdx = postResumeTypes.lastIndexOf('agent_end');
      expect(resumeToolEndIdx).toBeGreaterThanOrEqual(0);
      expect(resumeToolEndIdx).toBeLessThan(resumeAgentEndIdx);

      // After the resume terminalizes, the display state's pending slot clears
      // (the captured approval registration is consumed).
      expect(session.getDisplayState().pending == null).toBe(true);
    } finally {
      await harness.shutdown();
    }
  });
});

// ===========================================================================
// S4 — signal_completed / queue_completed settlement evidence
// ===========================================================================

describe('Harness v1 real-agent E2E — S4 settlement evidence', () => {
  it('signal() settles signal_completed and lookupMessageResult reports completed', async () => {
    const model = new MockLanguageModelV2({
      doStream: async () => ({
        rawCall: { rawPrompt: null, rawSettings: {} },
        warnings: [],
        stream: textStream(['done']),
      }),
    });
    const agent = new Agent({ id: 'default', name: 'default', instructions: 'reply', model });
    const harness = newHarness(agent);
    try {
      const session = await harness.session({ resourceId: 'u-signal', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));

      const handle = await session.signal({ content: 'go' });
      const result = await handle.result;
      expect(result.finishReason).toBe('stop');

      await waitFor(() => events.some(e => e.type === 'signal_completed'), 'signal_completed');
      const completed = events.find(e => e.type === 'signal_completed') as { runId: string } | undefined;
      expect(completed).toBeDefined();
      expect(typeof completed!.runId).toBe('string');
      expect(completed!.runId.length).toBeGreaterThan(0);

      const lookup = await session.lookupMessageResult(handle.id);
      expect(lookup && 'status' in lookup ? lookup.status : null).toBe('completed');
    } finally {
      await harness.shutdown();
    }
  });

  it('queue() settles queue_completed carrying queuedItemId + signalId', async () => {
    const model = new MockLanguageModelV2({
      doStream: async () => ({
        rawCall: { rawPrompt: null, rawSettings: {} },
        warnings: [],
        stream: textStream(['queued reply']),
      }),
    });
    const agent = new Agent({ id: 'default', name: 'default', instructions: 'reply', model });
    const harness = newHarness(agent);
    try {
      const session = await harness.session({ resourceId: 'u-queue', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));

      const result = (await session.queue({ content: 'do work' })) as any;
      expect(result.text).toBe('queued reply');

      await waitFor(() => events.some(e => e.type === 'queue_completed'), 'queue_completed');
      const completed = events.find(e => e.type === 'queue_completed') as
        | { queuedItemId: string; signalId: string; runId: string }
        | undefined;
      expect(completed).toBeDefined();
      expect(completed!.queuedItemId.length).toBeGreaterThan(0);
      expect(completed!.signalId.length).toBeGreaterThan(0);
      expect(completed!.runId.length).toBeGreaterThan(0);
      expect(session.getRecord().pendingQueue).toEqual([]);
    } finally {
      await harness.shutdown();
    }
  });
});

// ===========================================================================
// S5 — provider/model error (before run start AND mid-stream)
// ===========================================================================

describe('Harness v1 real-agent E2E — S5 provider error', () => {
  it('(a) error BEFORE the stream: agent_end:error fires (redacted boundary) AND message() rejects with a REDACTED error (raw detail only on .cause)', async () => {
    const RAW = 'connect ECONNREFUSED 10.0.0.5:5432 — secret provider detail';
    const model = new MockLanguageModelV2({
      doStream: async () => {
        throw new Error(RAW);
      },
    });
    const agent = new Agent({ id: 'default', name: 'default', instructions: 'reply', model });
    const harness = newHarness(agent);
    try {
      const session = await harness.session({ resourceId: 'u-err-pre', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));

      const thrown = await session.message({ content: 'hi' }).then(
        () => undefined,
        (e: unknown) => e,
      );

      // The default-path message() rejects (it does not swallow a pre-stream
      // provider failure).
      expect(thrown).toBeInstanceOf(Error);

      // The turn-event boundary IS the redacted boundary: agent_start then a
      // terminal agent_end with finishReason 'error' (no spurious complete).
      await waitFor(
        () => events.some(e => e.type === 'agent_end' && (e as any).finishReason === 'error'),
        'agent_end:error (pre-stream)',
        4000,
      );
      const ended = events.find(e => e.type === 'agent_end') as { finishReason: string };
      expect(ended.finishReason).toBe('error');
      expect(events.some(e => e.type === 'agent_end' && (e as any).finishReason === 'complete')).toBe(false);

      // §13.3f.1 (harnessv1/.../06-error-envelope.md, 07-... ): `message()` is a
      // public §4.2b boundary, and SDK promise rejections are explicitly among
      // the surfaces that must expose only a redacted projection — raw
      // driver/SQL/path/`err.message` text MUST NOT cross it. The harness wraps a
      // raw provider failure in a `HarnessExecutionError` (the same
      // both-satisfying shape as `HarnessStorageError`): its public `.message` is
      // the generic, already-redacted `harness.internal` text, and the raw
      // original is kept LOCAL-ONLY on `.cause` so the trusted Doxa caller still
      // has full fidelity for logging / retry classification. A naive caller that
      // surfaces `.message` (e.g. spawn-subagent-tool projecting a child failure)
      // therefore cannot leak provider detail.
      expect((thrown as Error).name).toBe('HarnessExecutionError');
      expect((thrown as Error).message).toBe('An internal harness error occurred');
      expect((thrown as Error).message).not.toContain('ECONNREFUSED');
      // The raw provider detail is preserved local-only on `.cause`.
      const cause = (thrown as { cause?: unknown }).cause as Error | undefined;
      expect(cause).toBeInstanceOf(Error);
      expect(cause!.message).toBe(RAW);
      expect(cause!.message).toContain('ECONNREFUSED');
    } finally {
      await harness.shutdown();
    }
  });

  it('(b) error DURING the stream emits partial text_delta then agent_end:error', async () => {
    const model = new MockLanguageModelV2({
      doStream: async () => ({
        rawCall: { rawPrompt: null, rawSettings: {} },
        warnings: [],
        stream: convertArrayToReadableStream([
          { type: 'stream-start', warnings: [] },
          { type: 'response-metadata', id: 'id-mid', modelId: 'mock-model-id', timestamp: new Date(0) },
          { type: 'text-start', id: 'text-1' },
          { type: 'text-delta', id: 'text-1', delta: 'partial' },
          { type: 'error', error: new Error('mid-stream provider blowup — host db.internal:5432') },
        ]),
      }),
    });
    const agent = new Agent({ id: 'default', name: 'default', instructions: 'reply', model });
    const harness = newHarness(agent);
    try {
      const session = await harness.session({ resourceId: 'u-err-mid', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));

      await session.message({ content: 'hi' }).catch(() => {});

      await waitFor(
        () => events.some(e => e.type === 'agent_end'),
        'agent_end (mid-stream error)',
        4000,
      );
      const textDeltas = events.filter(e => e.type === 'text_delta') as Array<{ delta: string }>;
      // The partial delta emitted before the error chunk must have surfaced.
      expect(textDeltas.map(e => e.delta).join('')).toContain('partial');
      const ended = events.find(e => e.type === 'agent_end') as { finishReason: string };
      // A mid-stream error terminalizes as error (not complete).
      expect(['error', 'aborted']).toContain(ended.finishReason);
      // No spurious 'complete' terminal for an errored run.
      expect(events.filter(e => e.type === 'agent_end' && (e as any).finishReason === 'complete')).toHaveLength(0);
    } finally {
      await harness.shutdown();
    }
  });
});

// ===========================================================================
// S6 — abort terminalizes the real run
//
// Two distinct real behaviors, both verified at runtime against the real Agent
// over InMemoryStore (instrumented probe):
//
//   (a) MID-RUN abort — abort fired from INSIDE a running tool, AFTER the run is
//       dispatched. This is the path that exercises the real
//       `turnAbortController`: the live turn aborts and the harness emits a
//       terminal `agent_end:aborted` (observed sequence:
//       agent_start → tool_start → agent_end:aborted → tool_end). `message()`
//       still rejects with the `agent_aborted` error (the active-turn waiter
//       loses the race), but the genuine terminal event fires. This is the
//       coverage the file header claims, and it catches a regression if the
//       loop ever stops terminalizing an aborted run.
//
//   (b) ALREADY-aborted caller signal — `_beginTurn` (session.ts:1397) aborts
//       the turn controller synchronously before the run is ever dispatched, so
//       `message()` rejects pre-dispatch with `agent_aborted` and emits ZERO
//       harness events (no agent_start, no agent_end). This characterizes the
//       real "nothing was launched" contract; it must NOT pretend a terminal
//       `agent_end:aborted` was reached, because none is.
// ===========================================================================

describe('Harness v1 real-agent E2E — S6 abort', () => {
  it('(a) a MID-RUN abort (from inside a running tool) terminalizes the real run as agent_end:aborted', async () => {
    const ac = new AbortController();
    // The tool aborts the run while it is executing, then yields so the loop
    // observes the abort mid-flight (real turnAbortController path).
    const abortingTool = createTool({
      id: 'abortNow',
      description: 'aborts the in-flight run',
      inputSchema: z.object({}),
      execute: async () => {
        ac.abort();
        await new Promise(resolve => setTimeout(resolve, 20));
        return { aborted: true };
      },
    });

    // Step 1: a tool-call that triggers the abort. Step 2 would be post-tool
    // text — it must never be reached because the run aborts inside the tool.
    let callCount = 0;
    const model = new MockLanguageModelV2({
      doStream: async () => {
        callCount++;
        if (callCount === 1) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream('call-abort', 'abortNow', '{}'),
          };
        }
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: textStream(['should not be reached']),
        };
      },
    });

    const agent = new Agent({
      id: 'default',
      name: 'default',
      instructions: 'use abortNow',
      model,
      tools: { abortNow: abortingTool },
    });
    const harness = newHarness(agent);
    try {
      const session = await harness.session({ resourceId: 'u-abort-mid', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));

      // The default-path message() rejects when the live turn is aborted: the
      // active-turn waiter rejects with the `agent_aborted` error.
      const thrown = await session.message({ content: 'go', abortSignal: ac.signal }).then(
        () => undefined,
        (e: unknown) => e,
      );
      expect(thrown).toBeInstanceOf(Error);
      expect((thrown as Error).message).toContain('agent_aborted');

      // The real turnAbortController path emitted a terminal agent_end:aborted.
      // No `.catch(()=>{})` swallow and no `if (ended)` guard — a missing or
      // wrong-reason terminal FAILS the test (this is the regression the
      // scenario is meant to catch).
      await waitFor(
        () => events.some(e => e.type === 'agent_end'),
        'agent_end (mid-run abort)',
      );
      const ended = events.find(e => e.type === 'agent_end') as { finishReason: string };
      expect(ended).toBeDefined();
      expect(ended.finishReason).toBe('aborted');

      // The run was actually dispatched: agent_start preceded the abort terminal.
      const types = events.map(e => e.type);
      expect(types.indexOf('agent_start')).toBeGreaterThanOrEqual(0);
      expect(types.indexOf('agent_start')).toBeLessThan(types.indexOf('agent_end'));
      // The aborting tool actually started inside the loop before the terminal.
      expect(types).toContain('tool_start');
      // No spurious complete terminal for an aborted turn.
      expect(events.filter(e => e.type === 'agent_end' && (e as any).finishReason === 'complete')).toHaveLength(0);
    } finally {
      await harness.shutdown();
    }
  });

  it('(b) an ALREADY-aborted caller signal rejects message() pre-dispatch with agent_aborted and emits NO harness events', async () => {
    const model = new MockLanguageModelV2({
      doStream: async () => ({
        rawCall: { rawPrompt: null, rawSettings: {} },
        warnings: [],
        stream: textStream(['should not finish']),
      }),
    });
    const agent = new Agent({ id: 'default', name: 'default', instructions: 'reply', model });
    const harness = newHarness(agent);
    try {
      const session = await harness.session({ resourceId: 'u-abort', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));

      const ac = new AbortController();
      ac.abort();

      // `_beginTurn` aborts the turn controller synchronously before dispatch,
      // so message() rejects with the `agent_aborted` error — verified at
      // runtime, NOT swallowed.
      const thrown = await session.message({ content: 'go', abortSignal: ac.signal }).then(
        () => undefined,
        (e: unknown) => e,
      );
      expect(thrown).toBeInstanceOf(Error);
      expect((thrown as Error).message).toContain('agent_aborted');

      // Let any scheduled setTimeout(0) abort-fanout flush, then assert NO run
      // was ever dispatched: zero events, in particular no agent_start and no
      // agent_end. (This is the real contract; the prior S6 masked a guaranteed
      // 4s waitFor timeout and asserted nothing.)
      await new Promise(resolve => setTimeout(resolve, 10));
      expect(events).toHaveLength(0);
      expect(events.some(e => e.type === 'agent_start')).toBe(false);
      expect(events.some(e => e.type === 'agent_end')).toBe(false);
    } finally {
      await harness.shutdown();
    }
  });
});
