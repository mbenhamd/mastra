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
import { MockMemory } from '../../memory/mock';
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

// ===========================================================================
// S7 — REAL parent agent spawns a REAL subagent through the genuine
//      spawn_subagent → child.message() → ai-sdk loop path, and the CHILD's
//      streamed events surface on the PARENT's subscriber as `subagent_*`.
//
// Every prior subagent test (session.spawn-subagent.test.ts,
// session.subagent-events.test.ts) drives this surface with a `FakeAgent`
// (a duck-typed Agent that overrides `stream` to hand back a fabricated
// `MastraModelOutput`) or by calling `_emitSubagentEvent` directly. NONE of
// them exercise the real chain:
//
//   parent model emits a spawn_subagent tool-call
//     → the built-in spawn tool runs `child.message({ content: task })`
//       → the REAL child Agent runs its OWN deterministic-model loop
//         → the child's real `text-delta` / `tool-call` / `tool-result`
//           chunks flow through the child session's `_emitForChunk`
//           → the child session publishes real `agent_start` / `text_delta`
//             / `tool_start` / `tool_end` harness events
//             → the spawn tool's `child.subscribe(...)` bridge re-emits them
//               as `subagent_*` on the PARENT via `_emitSubagentEvent`.
//
// Both agents are the real `Agent` class; only their LANGUAGE MODELS are the
// deterministic `MockLanguageModelV2` provider mock. No `FakeAgent` /
// `MockAgent` here.
// ===========================================================================

/**
 * A raw provider stream that emits a tool-result-bearing turn: a `tool-call`
 * to `toolName`, then finishes `tool-calls`. (Same shape as `toolCallStream`,
 * kept distinct for readability in the child's two-call sequence.)
 */
function childToolCallStream(toolCallId: string, toolName: string, inputJson: string) {
  return toolCallStream(toolCallId, toolName, inputJson);
}

describe('Harness v1 real-agent E2E — S7 real subagent streaming', () => {
  it('a REAL parent spawning a REAL subagent surfaces the child’s streamed text + real tool round-trip as subagent_* events with correct attribution', async () => {
    // --- REAL child tool (runs inside the child agent's real loop) ----------
    // Returns a non-JSON-native value (Date) to prove the subagent_tool_end
    // output is projected JSON-safe at the parent event boundary.
    const childTool = createTool({
      id: 'lookupFact',
      description: 'look up a fact for the subagent',
      inputSchema: z.object({ topic: z.string() }),
      execute: async input => {
        return { topic: (input as { topic: string }).topic, value: 42, fetchedAt: new Date(0) };
      },
    });

    // --- REAL child agent: model emits a real tool-call, then streamed text --
    const childDeltas = ['The ', 'answer ', 'is ', '42.'];
    let childCall = 0;
    const childModel = new MockLanguageModelV2({
      doStream: async () => {
        childCall++;
        if (childCall === 1) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: childToolCallStream('child-tc-1', 'lookupFact', '{"topic":"the answer"}'),
          };
        }
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: textStream(childDeltas),
        };
      },
    });
    const childAgent = new Agent({
      id: 'child-agent',
      name: 'child-agent',
      instructions: 'use lookupFact then summarize',
      model: childModel,
      tools: { lookupFact: childTool },
    });

    // --- REAL parent agent: model emits a spawn_subagent tool-call, then text -
    let parentCall = 0;
    const parentModel = new MockLanguageModelV2({
      doStream: async () => {
        parentCall++;
        if (parentCall === 1) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream(
              'parent-spawn-tc',
              'spawn_subagent',
              JSON.stringify({ agentType: 'explore', task: 'Find the answer to the question.' }),
            ),
          };
        }
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: textStream(['Delegated ', 'and done.']),
        };
      },
    });
    const parentAgent = new Agent({
      id: 'parent-agent',
      name: 'parent-agent',
      instructions: 'delegate to a subagent',
      model: parentModel,
    });

    // --- REAL harness wiring both real agents + a real subagent type --------
    const harness = new Harness({
      agents: { 'parent-agent': parentAgent, 'child-agent': childAgent } as any,
      storage: new InMemoryStore(),
      modes: [
        { id: 'default', agentId: 'parent-agent' },
        { id: 'explore-mode', agentId: 'child-agent' },
      ],
      defaultModeId: 'default',
      subagents: {
        maxDepth: 2,
        types: {
          explore: {
            agentId: 'child-agent',
            modeId: 'explore-mode',
            description: 'Read-only fact lookup',
            defaultModelId: 'openai/gpt-4o-mini',
            workspace: 'inherit',
          },
        },
      },
    });

    try {
      const session = await harness.session({ resourceId: 'u-subagent', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));

      const result = (await session.message({ content: 'answer my question via a subagent' })) as any;

      // The parent's real loop ran the spawn tool and continued to final text.
      expect(result.text).toBe('Delegated and done.');
      expect(Array.isArray(result.toolCalls)).toBe(true);
      expect(result.toolCalls.some((c: any) => (c.toolName ?? c.payload?.toolName) === 'spawn_subagent')).toBe(true);

      // --- subagent_start -----------------------------------------------------
      const subStart = events.find(e => e.type === 'subagent_start') as
        | {
            agentType: string;
            task: string;
            parentId: string;
            depth: number;
            subagentSessionId: string;
            toolCallId: string;
          }
        | undefined;
      expect(subStart).toBeDefined();
      expect(subStart!.agentType).toBe('explore');
      expect(subStart!.task).toBe('Find the answer to the question.');
      expect(subStart!.parentId).toBe(session.id);
      expect(subStart!.depth).toBe(1);
      expect(typeof subStart!.subagentSessionId).toBe('string');
      expect(subStart!.subagentSessionId.length).toBeGreaterThan(0);
      expect(subStart!.subagentSessionId).not.toBe(session.id);
      // The bridge stamps the PARENT's spawn tool-call id on every child event.
      expect(subStart!.toolCallId).toBe('parent-spawn-tc');

      const childSessionId = subStart!.subagentSessionId;

      // --- subagent_text_delta (the child's REAL streamed deltas) ------------
      const subTextDeltas = events.filter(e => e.type === 'subagent_text_delta') as Array<{
        delta: string;
        depth: number;
        parentId: string;
        subagentSessionId: string;
      }>;
      expect(subTextDeltas.length).toBe(childDeltas.length);
      expect(subTextDeltas.map(e => e.delta)).toEqual(childDeltas);
      expect(subTextDeltas.map(e => e.delta).join('')).toBe(childDeltas.join(''));
      // Attribution: all child deltas carry the parent id, child session id, depth 1.
      expect(subTextDeltas.every(e => e.parentId === session.id)).toBe(true);
      expect(subTextDeltas.every(e => e.subagentSessionId === childSessionId)).toBe(true);
      expect(subTextDeltas.every(e => e.depth === 1)).toBe(true);

      // --- subagent_tool_start / subagent_tool_end (child's REAL tool) -------
      const subToolStart = events.find(e => e.type === 'subagent_tool_start') as
        | { toolName: string; innerToolCallId: string; parentId: string; depth: number }
        | undefined;
      const subToolEnd = events.find(e => e.type === 'subagent_tool_end') as
        | { toolName: string; innerToolCallId: string; output: any; isError: boolean; parentId: string }
        | undefined;
      expect(subToolStart).toBeDefined();
      expect(subToolStart!.toolName).toBe('lookupFact');
      expect(subToolStart!.innerToolCallId).toBe('child-tc-1');
      expect(subToolStart!.parentId).toBe(session.id);
      expect(subToolStart!.depth).toBe(1);

      expect(subToolEnd).toBeDefined();
      expect(subToolEnd!.toolName).toBe('lookupFact');
      expect(subToolEnd!.innerToolCallId).toBe('child-tc-1');
      expect(subToolEnd!.isError).toBe(false);
      expect(subToolEnd!.output.topic).toBe('the answer');
      expect(subToolEnd!.output.value).toBe(42);
      // JSON-safe projection: the child tool's Date crossed as an ISO string.
      expect(subToolEnd!.output.fetchedAt).toBe(new Date(0).toISOString());
      expect(subToolEnd!.output.fetchedAt).not.toBeInstanceOf(Date);

      // --- subagent_end -------------------------------------------------------
      const subEnd = events.find(e => e.type === 'subagent_end') as
        | { output: any; isError: boolean; durationMs: number; parentId: string; depth: number }
        | undefined;
      expect(subEnd).toBeDefined();
      expect(subEnd!.isError).toBe(false);
      expect(typeof subEnd!.durationMs).toBe('number');
      expect(subEnd!.durationMs).toBeGreaterThanOrEqual(0);
      expect(subEnd!.parentId).toBe(session.id);
      expect(subEnd!.depth).toBe(1);
      // The child's real FullOutput surfaced as the subagent's output; its text
      // is the concatenation of the child's real streamed deltas.
      expect((subEnd!.output as { text?: string }).text).toBe(childDeltas.join(''));

      // --- Ordering: start → (child events) → end ----------------------------
      const subTypes = events.filter(e => e.type.startsWith('subagent_')).map(e => e.type);
      const startIdx = subTypes.indexOf('subagent_start');
      const endIdx = subTypes.lastIndexOf('subagent_end');
      const toolStartIdx = subTypes.indexOf('subagent_tool_start');
      const toolEndIdx = subTypes.indexOf('subagent_tool_end');
      const firstTextIdx = subTypes.indexOf('subagent_text_delta');
      expect(startIdx).toBe(0);
      expect(endIdx).toBe(subTypes.length - 1);
      // The child ran its tool BEFORE it streamed its post-tool summary text.
      expect(toolStartIdx).toBeGreaterThan(startIdx);
      expect(toolStartIdx).toBeLessThan(toolEndIdx);
      expect(toolEndIdx).toBeLessThan(firstTextIdx);
      expect(firstTextIdx).toBeLessThan(endIdx);
      // Exactly one start and one end for the single spawned child.
      expect(subTypes.filter(t => t === 'subagent_start')).toHaveLength(1);
      expect(subTypes.filter(t => t === 'subagent_end')).toHaveLength(1);

      // The child agent's model was actually driven twice (tool round-trip +
      // summary), proving the REAL child loop ran, not a fabricated output.
      expect(childCall).toBe(2);
    } finally {
      await harness.shutdown();
    }
  });
});

// ===========================================================================
// S8 — Multi-turn conversation builds REAL thread context.
//
// Every prior real-agent scenario opens a fresh thread and sends exactly ONE
// message(). Nothing asserts that turn N+1's real ai-sdk prompt carries turn
// N's exchange. `MockLanguageModelV2.doStream` receives the provider call
// `_options` (incl. `.prompt`, the message array the loop assembled from real
// thread history), so prompt growth across turns is directly assertable.
//
// This drives THREE sequential message() turns on ONE session and proves the
// real loop threads prior user+assistant content into later prompts — not
// isolated, stateless model calls.
//
// NOTE: conversation history threading is a `Memory` feature, not a bare-agent
// one. A real Agent with NO `memory` configured logs "No memory is configured
// but resourceId and threadId were passed" and assembles each turn's prompt
// from ONLY the system instruction + the new user message (verified at runtime:
// turn 3's prompt contained neither prior user content nor prior replies). So
// this scenario wires a real `MockMemory` (the same store the agent memory
// tests use) onto the agent — the harness passes `memory: { thread, resource }`
// into every turn's exec options (session.ts), so real history accretes.
// ===========================================================================

describe('Harness v1 real-agent E2E — S8 multi-turn context threading', () => {
  it('three sequential message() turns share a thread and later prompts carry earlier turns', async () => {
    // Capture every provider prompt the real loop assembled, in call order.
    const prompts: unknown[][] = [];
    let turn = 0;
    const replies = ['Noted, Dero.', 'I will remember it.', 'Your name is Dero.'];
    const model = new MockLanguageModelV2({
      doStream: async _options => {
        // `_options.prompt` is the message array the agent loop built from real
        // thread history + the new user content (title-generation.test.ts reads
        // `options.prompt` the same way).
        prompts.push((_options as { prompt: unknown[] }).prompt);
        const reply = replies[turn] ?? 'ok';
        turn++;
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: textStream([reply]),
        };
      },
    });
    const agent = new Agent({
      id: 'default',
      name: 'default',
      instructions: 'remember details',
      model,
      memory: new MockMemory(),
    });
    const harness = newHarness(agent);
    try {
      const session = await harness.session({ resourceId: 'u-multi', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));

      const r1 = (await session.message({ content: 'my name is Dero' })) as any;
      const r2 = (await session.message({ content: 'please remember it' })) as any;
      const r3 = (await session.message({ content: 'what is my name?' })) as any;

      // (a) each turn resolved with its expected real reply.
      expect(r1.text).toBe(replies[0]);
      expect(r2.text).toBe(replies[1]);
      expect(r3.text).toBe(replies[2]);

      // (b) three real provider calls were made (one per message turn).
      expect(prompts).toHaveLength(3);

      // (c) prompt for turn 3 carries the prior turns' user content — proving
      // real thread-history threading, not isolated stateless calls.
      const turn3Json = JSON.stringify(prompts[2]);
      expect(turn3Json).toContain('my name is Dero');
      expect(turn3Json).toContain('please remember it');
      // turn 3's prompt also carries an earlier assistant reply.
      expect(turn3Json).toContain(replies[0]);

      // (d) the assembled prompt strictly grows turn over turn (history accretes).
      expect(prompts[1]!.length).toBeGreaterThan(prompts[0]!.length);
      expect(prompts[2]!.length).toBeGreaterThan(prompts[1]!.length);
      // turn 1's prompt has no later turns' content.
      expect(JSON.stringify(prompts[0])).not.toContain('what is my name?');

      // (e) all three turns ran on the SAME thread with DISTINCT runIds.
      const ends = events.filter(e => e.type === 'agent_end') as Array<{ runId: string; finishReason: string }>;
      expect(ends).toHaveLength(3);
      expect(ends.every(e => e.finishReason === 'complete')).toBe(true);
      const runIds = ends.map(e => e.runId);
      expect(new Set(runIds).size).toBe(3);
    } finally {
      await harness.shutdown();
    }
  });
});

// ===========================================================================
// S9 — A queue of DISTINCT items drains FIFO through the REAL loop.
//
// session.queue.test.ts drives FIFO ordering with a MockAgent staging
// fabricated outputs; the real-agent S4 queue case is a single item. This
// enqueues THREE distinct items back-to-back on an idle session and asserts
// they drain in A→B→C order through the genuine loop with distinct results.
// ===========================================================================

describe('Harness v1 real-agent E2E — S9 queue FIFO drain (real loop)', () => {
  it('three distinct queued items drain FIFO with distinct results and empty the pendingQueue', async () => {
    // Stateful model: distinct reply keyed on the user content the loop sent.
    const model = new MockLanguageModelV2({
      doStream: async _options => {
        const promptJson = JSON.stringify((_options as { prompt: unknown[] }).prompt);
        const which = promptJson.includes('task-C')
          ? 'reply-C'
          : promptJson.includes('task-B')
            ? 'reply-B'
            : 'reply-A';
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: textStream([which]),
        };
      },
    });
    const agent = new Agent({ id: 'default', name: 'default', instructions: 'reply', model });
    const harness = newHarness(agent);
    try {
      const session = await harness.session({ resourceId: 'u-queue-fifo', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));

      // Fire all three back-to-back without awaiting; the loop drains FIFO.
      const pA = session.queue({ content: 'task-A' });
      const pB = session.queue({ content: 'task-B' });
      const pC = session.queue({ content: 'task-C' });
      const [rA, rB, rC] = (await Promise.all([pA, pB, pC])) as any[];

      // (a) distinct results, each its own queued item's reply.
      expect(rA.text).toBe('reply-A');
      expect(rB.text).toBe('reply-B');
      expect(rC.text).toBe('reply-C');

      // (b) exactly three queue_completed events, in A,B,C drain order.
      await waitFor(
        () => events.filter(e => e.type === 'queue_completed').length === 3,
        'three queue_completed events',
      );
      const completed = events.filter(e => e.type === 'queue_completed') as Array<{
        queuedItemId: string;
        signalId: string;
        runId: string;
      }>;
      expect(completed).toHaveLength(3);
      // (c) distinct queuedItemIds / runIds across the three drained turns.
      expect(new Set(completed.map(c => c.queuedItemId)).size).toBe(3);
      expect(new Set(completed.map(c => c.runId)).size).toBe(3);
      expect(completed.every(c => c.signalId.length > 0)).toBe(true);

      // (d) ordering: each turn's agent_start/agent_end nest in FIFO order.
      // The three runs ran sequentially, so their agent_end runIds appear in
      // the SAME order as the queued items drained.
      const ends = events.filter(e => e.type === 'agent_end') as Array<{ runId: string }>;
      expect(ends.map(e => e.runId)).toEqual(completed.map(c => c.runId));

      // (e) the pending queue fully drained.
      expect(session.getRecord().pendingQueue).toEqual([]);
    } finally {
      await harness.shutdown();
    }
  });
});

// ===========================================================================
// S10 — A signal INTERLEAVES a LIVE real run (§21 shared terminal).
//
// session.signal.test.ts holds the run open with a MockAgent fabricated
// output; willInterleave / the shared-terminal settlement are only exercised
// against a mock. Here a REAL run is genuinely in-flight (a tool blocks on an
// external barrier), and a signal() fired during that window must report
// willInterleave:true with the active run's id, then settle off the SHARED
// run terminal.
//
// CHARACTERIZATION: per-segment DISTINCT-answer attribution for the
// interleaved content is a documented runtime refinement (session.ts shared-
// terminal comment). This asserts only the GUARANTEED contract — willInterleave
// + shared runId + override-rejection + shared-terminal settlement — NOT a
// distinct per-segment answer for the interleaved signal.
// ===========================================================================

describe('Harness v1 real-agent E2E — S10 signal interleaves a live run', () => {
  it('a signal fired during a live real run reports willInterleave + shared runId, rejects overrides, and settles off the shared terminal', async () => {
    let releaseBarrier!: () => void;
    const barrier = new Promise<void>(resolve => {
      releaseBarrier = resolve;
    });
    let toolStarted = false;
    // A real tool that blocks the run open on an external barrier, so a real
    // run is genuinely in-flight while we fire the interleaving signal.
    const holdTool = createTool({
      id: 'holdOpen',
      description: 'holds the run open until released',
      inputSchema: z.object({}),
      execute: async () => {
        toolStarted = true;
        await barrier;
        return { held: true };
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
            stream: toolCallStream('call-hold', 'holdOpen', '{}'),
          };
        }
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: textStream(['parent done']),
        };
      },
    });
    const agent = new Agent({
      id: 'default',
      name: 'default',
      instructions: 'use holdOpen',
      model,
      tools: { holdOpen: holdTool },
    });
    const harness = newHarness(agent);
    try {
      const session = await harness.session({ resourceId: 'u-interleave', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));

      // Fire the parent run WITHOUT awaiting — it blocks inside holdOpen.
      const parentPromise = session.message({ content: 'go' }) as Promise<any>;
      void parentPromise.catch(() => {});

      // Wait until the run is genuinely in-flight (tool executing).
      await waitFor(() => toolStarted, 'holdOpen tool started');
      const parentRunId = (events.find(e => e.type === 'agent_start') as { runId: string }).runId;

      // (a) a signal fired NOW interleaves the live run, sharing its runId.
      const handle = await session.signal({ content: 'steer' });
      expect(handle.willInterleave).toBe(true);
      expect(handle.runId).toBe(parentRunId);

      // (b) a per-turn override on an active-delivery signal rejects against the
      // REAL in-flight run (HarnessOverrideConflictError, session.ts:5858-5862).
      await expect(session.signal({ content: 'x', mode: 'default' })).rejects.toMatchObject({
        name: 'HarnessOverrideConflictError',
      });
      await expect(
        session.signal({ content: 'x', additionalTools: { holdOpen: holdTool } as any }),
      ).rejects.toMatchObject({ name: 'HarnessOverrideConflictError' });

      // Release the barrier — the real run finishes.
      releaseBarrier();
      const parentResult = (await parentPromise) as any;
      expect(parentResult.finishReason).toBe('stop');

      // (c) the interleaved signal settled off the SHARED run terminal: its
      // result resolves to the same run's AgentResult.
      const signalResult = (await handle.result) as any;
      expect(signalResult.finishReason).toBe('stop');

      // (d) a signal_completed for the interleaved signal carries the shared runId.
      await waitFor(
        () => events.some(e => e.type === 'signal_completed' && (e as any).runId === parentRunId),
        'signal_completed on shared runId',
      );

      // (e) lookup reports the interleaved signal completed.
      const lookup = await session.lookupMessageResult(handle.id);
      expect(lookup && 'status' in lookup ? lookup.status : null).toBe('completed');
    } finally {
      // Ensure the barrier is released so shutdown never hangs.
      releaseBarrier();
      await harness.shutdown();
    }
  });
});

// ===========================================================================
// S11 — Multi-tool turn: TWO tool round-trips in ONE real run.
//
// S2 is exactly one tool call. The real loop's ability to run two distinct
// tool round-trips across loop iterations in a single turn (toolA → toolB →
// text) is unverified at the harness event boundary. This drives it.
// ===========================================================================

describe('Harness v1 real-agent E2E — S11 multi-tool single turn', () => {
  it('two tool round-trips in one real run surface two ordered tool_start/tool_end pairs', async () => {
    const seenHarnessCtx: unknown[] = [];
    const toolA = createTool({
      id: 'stepA',
      description: 'first step',
      inputSchema: z.object({ x: z.number() }),
      execute: async (input, context) => {
        seenHarnessCtx.push(context?.requestContext?.get('harness'));
        return { a: (input as { x: number }).x + 1 };
      },
    });
    const toolB = createTool({
      id: 'stepB',
      description: 'second step',
      inputSchema: z.object({ y: z.number() }),
      execute: async (input, context) => {
        seenHarnessCtx.push(context?.requestContext?.get('harness'));
        return { b: (input as { y: number }).y * 2 };
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
            stream: toolCallStream('tc-a', 'stepA', '{"x":1}'),
          };
        }
        if (callCount === 2) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream('tc-b', 'stepB', '{"y":3}'),
          };
        }
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: textStream(['both ', 'done']),
        };
      },
    });
    const agent = new Agent({
      id: 'default',
      name: 'default',
      instructions: 'use stepA then stepB',
      model,
      tools: { stepA: toolA, stepB: toolB },
    });
    const harness = newHarness(agent);
    try {
      const session = await harness.session({ resourceId: 'u-multitool', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));

      const result = (await session.message({ content: 'run both steps' })) as any;
      expect(result.text).toBe('both done');

      // (a) exactly two tool_start and two tool_end events, in [A,B] order.
      const toolStarts = events.filter(e => e.type === 'tool_start') as Array<{
        toolName: string;
        toolCallId: string;
      }>;
      const toolEnds = events.filter(e => e.type === 'tool_end') as Array<{
        toolName: string;
        toolCallId: string;
        isError: boolean;
        output: any;
      }>;
      expect(toolStarts.map(e => e.toolName)).toEqual(['stepA', 'stepB']);
      expect(toolEnds.map(e => e.toolName)).toEqual(['stepA', 'stepB']);
      expect(toolEnds.every(e => e.isError === false)).toBe(true);
      expect(toolEnds[0]!.output).toEqual({ a: 2 });
      expect(toolEnds[1]!.output).toEqual({ b: 6 });

      // (b) ordering: stepA_end precedes stepB_start precedes final text.
      const types = events.map(e => e.type);
      const aEndIdx = events.findIndex(e => e.type === 'tool_end' && (e as any).toolName === 'stepA');
      const bStartIdx = events.findIndex(e => e.type === 'tool_start' && (e as any).toolName === 'stepB');
      expect(aEndIdx).toBeGreaterThanOrEqual(0);
      expect(aEndIdx).toBeLessThan(bStartIdx);
      expect(bStartIdx).toBeLessThan(types.lastIndexOf('text_delta'));

      // (c) the real FullOutput carries >=2 tool calls/results.
      expect(result.toolCalls.length).toBeGreaterThanOrEqual(2);
      expect(result.toolResults.length).toBeGreaterThanOrEqual(2);

      // (d) BOTH tools saw the harness RequestContext for this session.
      expect(seenHarnessCtx).toHaveLength(2);
      expect((seenHarnessCtx[0] as { sessionId?: string }).sessionId).toBe(session.id);
      expect((seenHarnessCtx[1] as { sessionId?: string }).sessionId).toBe(session.id);
    } finally {
      await harness.shutdown();
    }
  });
});

// ===========================================================================
// S12 — Approval → resume → a SECOND (non-approval) tool → complete.
//
// S3's resume turn is text-only. The path where the resumed run calls ANOTHER
// tool before completing (§10.4 live-drain past a fresh tool) is uncovered.
// ===========================================================================

describe('Harness v1 real-agent E2E — S12 resume runs a fresh tool', () => {
  it('after approving the first tool, the resumed run runs a SECOND tool then completes', async () => {
    const approveTool = createTool({
      id: 'findUser',
      description: 'look up a user (needs approval)',
      inputSchema: z.object({ name: z.string() }),
      requireApproval: true,
      execute: async input => ({ name: (input as { name: string }).name, id: 7 }),
    });
    const followTool = createTool({
      id: 'recordResult',
      description: 'record the looked-up user (no approval)',
      inputSchema: z.object({ id: z.number() }),
      execute: async input => ({ recorded: (input as { id: number }).id }),
    });

    let callCount = 0;
    const model = new MockLanguageModelV2({
      doStream: async () => {
        callCount++;
        if (callCount === 1) {
          // suspends on approval
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream('tc-approve', 'findUser', '{"name":"Dero"}'),
          };
        }
        if (callCount === 2) {
          // post-approval continuation calls a fresh, non-approval tool
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream('tc-follow', 'recordResult', '{"id":7}'),
          };
        }
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: textStream(['recorded']),
        };
      },
    });
    const agent = new Agent({
      id: 'default',
      name: 'default',
      instructions: 'find then record',
      model,
      tools: { findUser: approveTool, recordResult: followTool },
    });
    const harness = newHarness(agent);
    try {
      const session = await harness.session({ resourceId: 'u-resume-tool', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));

      const suspended = (await session.message({ content: 'find and record Dero' })) as any;
      expect(suspended.finishReason).toBe('suspended');
      expect(events.some(e => e.type === 'tool_approval_required')).toBe(true);
      expect(session.getRecord().pendingResume).toBeDefined();

      const eventsAtSuspend = events.length;

      await session.respondToToolApproval({ approved: true });
      await waitFor(
        () => events.some(e => e.type === 'agent_end' && (e as any).finishReason === 'complete'),
        'agent_end:complete after resume + second tool',
      );

      const postResume = events.slice(eventsAtSuspend);
      const postTypes = postResume.map(e => e.type);

      // (a) the approved tool (findUser) surfaced its tool_end after resume.
      const approvedEnd = postResume.find(e => e.type === 'tool_end' && (e as any).toolName === 'findUser') as
        | { output: any; isError: boolean }
        | undefined;
      expect(approvedEnd).toBeDefined();
      expect(approvedEnd!.isError).toBe(false);
      expect(approvedEnd!.output).toEqual({ name: 'Dero', id: 7 });

      // (b) the SECOND, fresh tool ran in the continuation: a recordResult
      // tool_start + tool_end appear AFTER the approved tool's tool_end.
      const followStartIdx = postResume.findIndex(e => e.type === 'tool_start' && (e as any).toolName === 'recordResult');
      const followEndIdx = postResume.findIndex(e => e.type === 'tool_end' && (e as any).toolName === 'recordResult');
      const approvedEndIdx = postResume.findIndex(e => e.type === 'tool_end' && (e as any).toolName === 'findUser');
      expect(followStartIdx).toBeGreaterThanOrEqual(0);
      expect(followEndIdx).toBeGreaterThan(followStartIdx);
      expect(approvedEndIdx).toBeLessThan(followStartIdx);
      const followEnd = postResume[followEndIdx] as any;
      expect(followEnd.output).toEqual({ recorded: 7 });

      // (c) the terminal text + complete follow the second tool.
      const postText = (postResume.filter(e => e.type === 'text_delta') as Array<{ delta: string }>)
        .map(e => e.delta)
        .join('');
      expect(postText).toBe('recorded');
      expect(followEndIdx).toBeLessThan(postTypes.lastIndexOf('text_delta'));
      expect(postTypes.lastIndexOf('text_delta')).toBeLessThan(postTypes.lastIndexOf('agent_end'));

      // (d) the pending resume registration cleared.
      expect(session.getRecord().pendingResume).toBeUndefined();
    } finally {
      await harness.shutdown();
    }
  });
});

// ===========================================================================
// S13 — Subagent runs a tool, returns, THEN the PARENT runs its OWN tool.
//
// S7's parent has no tools — it only spawns then emits text. A parent that
// consumes the subagent result and then runs its OWN tool (proving the parent
// resumed its own loop post-delegation) is uncovered.
// ===========================================================================

describe('Harness v1 real-agent E2E — S13 parent runs a tool after the subagent', () => {
  it('a parent runs its OWN tool after the subagent returns, surfacing a parent-level tool_end (not subagent_*)', async () => {
    // Child: one tool + text (same shape as S7's child).
    const childTool = createTool({
      id: 'lookupFact',
      description: 'look up a fact',
      inputSchema: z.object({ topic: z.string() }),
      execute: async input => ({ topic: (input as { topic: string }).topic, value: 42 }),
    });
    const childDeltas = ['answer ', '42'];
    let childCall = 0;
    const childModel = new MockLanguageModelV2({
      doStream: async () => {
        childCall++;
        if (childCall === 1) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream('child-tc', 'lookupFact', '{"topic":"the answer"}'),
          };
        }
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: textStream(childDeltas),
        };
      },
    });
    const childAgent = new Agent({
      id: 'child-agent',
      name: 'child-agent',
      instructions: 'use lookupFact then summarize',
      model: childModel,
      tools: { lookupFact: childTool },
    });

    // Parent: a REAL tool that records the subagent's returned text.
    let recordedInput: unknown;
    const parentTool = createTool({
      id: 'recordResult',
      description: 'record the subagent summary',
      inputSchema: z.object({ summary: z.string() }),
      execute: async input => {
        recordedInput = input;
        return { stored: true };
      },
    });

    // Parent model: call-1 spawn_subagent, call-2 a tool-call to recordResult,
    // call-3 final text.
    let parentCall = 0;
    const parentModel = new MockLanguageModelV2({
      doStream: async () => {
        parentCall++;
        if (parentCall === 1) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream(
              'parent-spawn-tc',
              'spawn_subagent',
              JSON.stringify({ agentType: 'explore', task: 'Find the answer.' }),
            ),
          };
        }
        if (parentCall === 2) {
          return {
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
            stream: toolCallStream('parent-record-tc', 'recordResult', '{"summary":"answer 42"}'),
          };
        }
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: textStream(['parent done']),
        };
      },
    });
    const parentAgent = new Agent({
      id: 'parent-agent',
      name: 'parent-agent',
      instructions: 'delegate then record',
      model: parentModel,
      tools: { recordResult: parentTool },
    });

    const harness = new Harness({
      agents: { 'parent-agent': parentAgent, 'child-agent': childAgent } as any,
      storage: new InMemoryStore(),
      modes: [
        { id: 'default', agentId: 'parent-agent' },
        { id: 'explore-mode', agentId: 'child-agent' },
      ],
      defaultModeId: 'default',
      subagents: {
        maxDepth: 2,
        types: {
          explore: {
            agentId: 'child-agent',
            modeId: 'explore-mode',
            description: 'Read-only fact lookup',
            defaultModelId: 'openai/gpt-4o-mini',
            workspace: 'inherit',
          },
        },
      },
    });

    try {
      const session = await harness.session({ resourceId: 'u-sub-then-tool', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(e => events.push(e));

      const result = (await session.message({ content: 'delegate then record' })) as any;
      expect(result.text).toBe('parent done');

      // (a) the full subagent_* sequence ran (start..end).
      const subStart = events.find(e => e.type === 'subagent_start');
      const subEnd = events.find(e => e.type === 'subagent_end');
      expect(subStart).toBeDefined();
      expect(subEnd).toBeDefined();

      // (b) AFTER subagent_end, a PARENT-level tool_start/tool_end (NOT
      // subagent_*) for recordResult — proving the parent resumed its OWN loop
      // with a fresh tool post-delegation.
      const subEndIdx = events.findIndex(e => e.type === 'subagent_end');
      const parentToolStart = events.find(
        (e, i) => i > subEndIdx && e.type === 'tool_start' && (e as any).toolName === 'recordResult',
      ) as { toolName: string; toolCallId: string } | undefined;
      const parentToolEnd = events.find(
        (e, i) => i > subEndIdx && e.type === 'tool_end' && (e as any).toolName === 'recordResult',
      ) as { toolName: string; output: any; isError: boolean } | undefined;
      expect(parentToolStart).toBeDefined();
      expect(parentToolStart!.toolCallId).toBe('parent-record-tc');
      expect(parentToolEnd).toBeDefined();
      expect(parentToolEnd!.isError).toBe(false);
      expect(parentToolEnd!.output).toEqual({ stored: true });

      // The parent tool is a plain tool_end, not a subagent_tool_end.
      expect(parentToolEnd!.toolName).toBe('recordResult');

      // (c) the parent tool's input reflects the (model-relayed) subagent summary.
      expect(recordedInput).toEqual({ summary: 'answer 42' });

      // (d) the child's real loop ran (tool round-trip + summary).
      expect(childCall).toBe(2);

      // (e) terminal complete after the parent tool.
      const ends = events.filter(e => e.type === 'agent_end') as Array<{ finishReason: string }>;
      expect(ends.some(e => e.finishReason === 'complete')).toBe(true);
    } finally {
      await harness.shutdown();
    }
  });
});
