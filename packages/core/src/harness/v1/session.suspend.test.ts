/**
 * Harness v1 — Session suspend/resume flow.
 *
 * Covers:
 *   - message() captures `pendingResume` when finishReason === 'suspended'
 *   - The four typed `respond*` methods classify by chunk + tool name
 *   - Wrong-kind / no-pending / already-resumed rejections
 *   - plan-approval mode flip applied atomically with the pending clear
 *   - Re-suspension chain (resume → suspend again → resume) round-trips
 */

import { describe, expect, it } from 'vitest';

import { Agent } from '../../agent';
import { InMemoryHarness } from '../../storage/domains/harness/inmemory';
import type { PendingResume } from '../../storage/domains/harness/types';
import { InMemoryDB } from '../../storage/domains/inmemory-db';
import { InMemoryStore } from '../../storage/mock';
import type { MastraModelOutput } from '../../stream/base/output';
import { buildFakeOutput } from './__test-utils__/fake-output';

import {
  HarnessBusyError,
  HarnessInboxResponseConflictError,
  HarnessPendingInteractionExpiredError,
  HarnessSessionCorruptError,
  HarnessSessionDeletedError,
  HarnessValidationError,
} from './errors';
import { Harness } from './harness';

// ---------------------------------------------------------------------------
// Fake agent with stub stream/generate/resumeStream + a queue of "next runs"
// so a single test can stage suspend → resume → suspend chains.
// ---------------------------------------------------------------------------

interface RunSpec {
  finishReason: string;
  suspendPayload?:
    | {
        toolCallId: string;
        toolName: string;
        args?: unknown;
        suspendPayload?: unknown;
        resumeSchema?: string;
      }
    | undefined;
  text?: string;
  runId?: string;
  holdUntil?: Promise<void>;
  totalUsage?: { inputTokens: number; outputTokens: number; totalTokens: number };
  chunks?: Array<Record<string, unknown>>;
  terminalToolResult?: unknown;
}

interface ResumeCall {
  resumeData: unknown;
  options: { runId?: string; toolCallId?: string; abortSignal?: AbortSignal };
}

class FakeAgent extends Agent<any, any, any> {
  /** Each entry is consumed in order by stream() / resumeStream() / generate(). */
  runs: RunSpec[] = [];
  streamCalls: { messages: unknown; options: any }[] = [];
  resumeCalls: ResumeCall[] = [];

  constructor() {
    super({
      id: 'fake',
      name: 'fake',
      instructions: 'fake',
      model: 'openai/gpt-4o-mini' as any,
    });
  }

  /** Push a run that will be returned by the next stream/resumeStream/generate. */
  enqueueRun(spec: RunSpec): void {
    this.runs.push({ runId: 'fake-run', text: 'ok', ...spec });
  }

  private buildOutput(spec: RunSpec, runIdOverride?: string): MastraModelOutput {
    const runId = runIdOverride ?? spec.runId;
    const totalUsage = spec.totalUsage ?? { inputTokens: 0, outputTokens: 0, totalTokens: 0 };
    return buildFakeOutput({
      runId,
      fullOutput: {
        text: spec.text ?? '',
        usage: totalUsage,
        finishReason: spec.finishReason,
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
        totalUsage,
        error: undefined,
        tripwire: undefined,
        traceId: undefined,
        spanId: undefined,
        suspendPayload: spec.suspendPayload,
        messages: [],
        rememberedMessages: [],
        terminalToolResult: spec.terminalToolResult,
      },
      chunks: spec.chunks?.map(chunk => ({ ...chunk, runId })),
    });
  }

  async stream(_messages: any, options?: any): Promise<any> {
    this.streamCalls.push({ messages: _messages, options });
    const spec = this.runs.shift();
    if (!spec) throw new Error('FakeAgent: no run enqueued for stream()');
    await spec.holdUntil;
    const out = this.buildOutput(spec, options?.runId);
    this._internalRegisterStreamRun(out, (options ?? {}) as any);
    return out;
  }

  async generate(_messages: any, _options?: any): Promise<any> {
    const spec = this.runs.shift();
    if (!spec) throw new Error('FakeAgent: no run enqueued for generate()');
    await spec.holdUntil;
    const out = this.buildOutput(spec);
    return await out.getFullOutput();
  }

  async resumeStream(resumeData: any, options?: any): Promise<any> {
    this.resumeCalls.push({ resumeData, options });
    const spec = this.runs.shift();
    if (!spec) throw new Error('FakeAgent: no run enqueued for resumeStream()');
    await spec.holdUntil;
    const out = this.buildOutput(spec, options?.runId);
    this._internalRegisterStreamRun(out, (options ?? {}) as any);
    return out;
  }
}

function setup(modes?: any, sessionOptions: Record<string, unknown> = {}) {
  const agent = new FakeAgent();
  const storage = new InMemoryHarness({ db: new InMemoryDB() });
  const resolvedModes = modes ?? [{ id: 'default', agentId: 'default' }];
  const harness = new Harness({
    agents: { default: agent } as any,
    modes: resolvedModes,
    defaultModeId: resolvedModes[0].id,
    sessions: { storage, ...sessionOptions },
  });
  return { harness, agent, storage };
}

function pendingResponseIdentity(pending: PendingResume) {
  return {
    itemId: pending.itemId!,
    runId: pending.runId,
    toolCallId: pending.toolCallId,
    pendingRequestedAt: pending.requestedAt,
  };
}

/** The terminal events `_expirePendingInteraction` is responsible for. */
const EXPIRY_TERMINAL_EVENT_TYPES = new Set(['tool_end', 'resume_failed', 'agent_end', 'run_completed']);

async function waitFor(predicate: () => boolean, label: string, timeoutMs = 500): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) {
      throw new Error(`timed out waiting for ${label}`);
    }
    await new Promise(resolve => setImmediate(resolve));
  }
}

// ---------------------------------------------------------------------------
// Capture
// ---------------------------------------------------------------------------

describe('Session — suspend capture on message()', () => {
  it('writes pendingResume with kind "tool-approval" for a plain approval suspend', async () => {
    const { harness, agent } = setup();
    agent.enqueueRun({
      finishReason: 'suspended',
      runId: 'run-A',
      suspendPayload: { toolCallId: 'tc-1', toolName: 'shell', args: { cmd: 'rm -rf /' } },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });

    const result = await session.message({ content: 'do it' });

    expect(result.finishReason).toBe('suspended');
    const pending = session.getRecord().pendingResume;
    expect(pending).toBeDefined();
    expect(pending!.kind).toBe('tool-approval');
    // Signal-routed message() stamps the runtime-allocated runId on the
    // resulting MastraModelOutput; pending capture mirrors it. The test's
    // enqueued `runId: 'run-A'` is overridden in the same way real
    // Agent.stream() honours options.runId.
    expect(pending!.runId).toBe(result.runId);
    expect(pending!.toolCallId).toBe('tc-1');
    expect(pending!.toolName).toBe('shell');
    expect(pending!.payload).toEqual({ input: { cmd: 'rm -rf /' } });
    expect(session.getDisplayState().pending?.kind).toBe('tool-approval');
  });

  it('persists bounded failed-tool receipts across suspension rehydration for terminal report verification', async () => {
    const { harness, agent, storage } = setup();
    agent.enqueueRun({
      finishReason: 'suspended',
      suspendPayload: { toolCallId: 'approval-1', toolName: 'write_file', args: { path: 'paper.tex' } },
      chunks: [
        {
          type: 'tool-error',
          payload: {
            toolCallId: 'compile-1',
            toolName: 'compile_latex',
            error: new Error('compiler service unavailable'),
          },
        },
      ],
    });
    const source = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await source.message({ content: 'diagnose, then ask before editing' });
    expect(source.getRecord().pendingResume).toMatchObject({
      toolReceipts: [
        {
          toolCallId: 'compile-1',
          toolName: 'compile_latex',
          status: 'error',
        },
      ],
    });
    const sessionId = source.id;
    await harness.shutdown();

    const recoveryAgent = new FakeAgent();
    recoveryAgent.enqueueRun({
      finishReason: 'tool-calls',
      terminalToolResult: {
        status: 'success',
        items: [
          {
            toolName: 'report_subagent_outcome',
            toolCallId: 'report-1',
            status: 'success',
            value: {
              kind: 'subagent-outcome-report',
              outcome: 'blocked',
              summary: 'No source repair was attempted because the compiler service was unavailable.',
              evidence: [
                {
                  kind: 'tool-result',
                  description: 'Compiler service call failed',
                  toolName: 'compile_latex',
                  toolCallId: 'compile-1',
                  status: 'error',
                },
              ],
              issue: {
                code: 'compiler.service_unavailable',
                message: 'Compiler service unavailable',
                retryable: true,
              },
            },
          },
        ],
      },
    });
    const harness2 = new Harness({
      agents: { default: recoveryAgent } as any,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage },
    });
    const restored = await harness2.session({ sessionId, resourceId: 'u' });
    const result = (await restored.respondToToolApproval({ approved: false })) as Record<string, unknown>;
    expect(result).toMatchObject({
      harnessToolReceipts: [
        {
          toolCallId: 'compile-1',
          toolName: 'compile_latex',
          status: 'error',
        },
      ],
    });
    await harness2.shutdown();
  });

  it('signal(): a suspended owned turn is parked, not settled — no signal_completed, evidence stays pending (§4.2f/§10.2)', async () => {
    const { harness, agent } = setup();
    agent.enqueueRun({
      finishReason: 'suspended',
      runId: 'run-S',
      suspendPayload: { toolCallId: 'tc-s', toolName: 'shell', args: { cmd: 'ls' } },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });

    const events: { type: string }[] = [];
    session.subscribe(e => events.push(e));

    const handle = await session.signal({ content: 'do it' });
    const result = await handle.result;
    expect(result.finishReason).toBe('suspended');

    // §10.2 — a parked turn is not answered, so the operation does not settle.
    expect(events.some(e => e.type === 'signal_completed')).toBe(false);
    expect(events.some(e => e.type === 'signal_failed')).toBe(false);

    // §4.2f — durable per-signalId evidence stays `pending` until resume
    // terminalizes (the accept-time pending write is the durable barrier).
    await waitFor(() => session.getRecord().pendingResume !== undefined, 'pendingResume captured for suspended signal');
    const lookup = await session.lookupMessageResult(handle.id);
    expect(lookup && 'status' in lookup ? lookup.status : null).toBe('pending');
  });

  it('signal(): terminal resume of a suspended owned turn settles signal_completed + durable evidence (§4.2f/§10.2)', async () => {
    const { harness, agent } = setup();
    // 1) initial owned turn suspends
    agent.enqueueRun({
      finishReason: 'suspended',
      runId: 'run-RS',
      suspendPayload: { toolCallId: 'tc-rs', toolName: 'shell', args: { cmd: 'ls' } },
    });
    // 2) resume terminalizes (no further suspend)
    agent.enqueueRun({ finishReason: 'stop', runId: 'run-RS', text: 'done' });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });

    const handle = await session.signal({ content: 'do it' });
    const suspended = await handle.result;
    expect(suspended.finishReason).toBe('suspended');

    // Evidence is parked while suspended (companion to the test above).
    await waitFor(() => session.getRecord().pendingResume !== undefined, 'pendingResume captured for suspended signal');
    expect((session.getRecord().pendingResume as { originSignalId?: string }).originSignalId).toBe(handle.id);

    // Now approve the tool → resume runs to a terminal (non-suspended) finish.
    const events: { type: string }[] = [];
    session.subscribe(e => events.push(e));
    await session.respondToToolApproval({ approved: true });

    // §4.2f — the terminal resume IS this signal's answer: it settles the
    // durable per-signalId evidence and projects signal_completed (not just
    // agent_end), so a suspended owned signal does not stay pending forever.
    await waitFor(() => events.some(e => e.type === 'signal_completed'), 'signal_completed after terminal resume');
    const lookup = await session.lookupMessageResult(handle.id);
    expect(lookup && 'status' in lookup ? lookup.status : null).toBe('completed');
    expect(session.getRecord().pendingResume).toBeUndefined();
  });

  it('signal(): pre-output resume failure stays retryable and leaves signal evidence pending (§4.2f)', async () => {
    const { harness, agent } = setup();
    agent.enqueueRun({
      finishReason: 'suspended',
      runId: 'run-RF',
      suspendPayload: { toolCallId: 'tc-rf', toolName: 'shell', args: { cmd: 'ls' } },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });

    const handle = await session.signal({ content: 'do it' });
    await handle.result;
    await waitFor(() => session.getRecord().pendingResume !== undefined, 'pendingResume captured for suspended signal');

    // No further run enqueued → resumeStream throws → terminal failure.
    const events: { type: string; retryable?: boolean }[] = [];
    session.subscribe(e => events.push(e));
    await expect(session.respondToToolApproval({ approved: true })).rejects.toBeTruthy();

    await waitFor(() => events.some(e => e.type === 'resume_failed'), 'retryable resume_failed after failed resume');
    expect(events).toContainEqual(expect.objectContaining({ type: 'resume_failed', retryable: true }));
    expect(events.some(event => event.type === 'signal_failed')).toBe(false);
    const lookup = await session.lookupMessageResult(handle.id);
    expect(lookup && 'status' in lookup ? lookup.status : null).toBe('pending');
    expect(session.getRecord().pendingResume?.resumedAt).toBeUndefined();
  });

  it('signal(): cold recovery settles an admitted resume abandoned across restart as failed', async () => {
    const { harness, agent, storage } = setup();
    agent.enqueueRun({
      finishReason: 'suspended',
      runId: 'run-signal-crash',
      suspendPayload: { toolCallId: 'tc-signal-crash', toolName: 'shell', args: { cmd: 'publish' } },
    });
    const source = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    const handle = await source.signal({ content: 'do it' });
    await handle.result;
    await waitFor(() => source.getRecord().pendingResume !== undefined, 'signal pending before crash fixture');
    const sessionId = source.id;
    await harness.shutdown();

    const fixtureOwner = 'signal-resume-crash-fixture';
    const lease = await storage.acquireSessionLease({ sessionId, ownerId: fixtureOwner, ttlMs: 30_000 });
    const stored = await storage.loadSession({ sessionId });
    if (!stored?.pendingResume) throw new Error('expected pending signal fixture');
    const resumedAt = Date.now() - 60_000;
    await storage.saveSession(
      {
        ...stored,
        ownerId: fixtureOwner,
        leaseExpiresAt: lease.expiresAt,
        pendingResume: {
          ...stored.pendingResume,
          resumedAt,
          resumeRecoveryAt: resumedAt + 30_000,
        },
      },
      { ownerId: fixtureOwner, ifVersion: lease.version },
    );
    await storage.releaseSessionLease({ sessionId, ownerId: fixtureOwner });

    const harness2 = new Harness({
      agents: { default: new FakeAgent() } as any,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage },
    });
    let recovered = await storage.loadSession({ sessionId });
    const deadline = Date.now() + 2_000;
    while (recovered?.pendingResume !== undefined && Date.now() < deadline) {
      await new Promise(resolve => setTimeout(resolve, 10));
      recovered = await storage.loadSession({ sessionId });
    }
    expect(recovered?.pendingResume).toBeUndefined();

    const restored = await harness2.session({ sessionId, resourceId: 'u' });
    await expect(restored.lookupMessageResult(handle.id)).resolves.toMatchObject({
      status: 'failed',
      error: { code: 'harness.resume_recovery_stale' },
    });
    await harness2.shutdown();
  });

  it('classifies as "tool-suspension" when the chunk carries a suspendPayload field', async () => {
    const { harness, agent } = setup();
    agent.enqueueRun({
      finishReason: 'suspended',
      runId: 'run-B',
      suspendPayload: {
        toolCallId: 'tc-2',
        toolName: 'long_running',
        args: { x: 1 },
        suspendPayload: { progress: 42 },
      },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });

    await session.message({ content: 'go' });

    const pending = session.getRecord().pendingResume!;
    expect(pending.kind).toBe('tool-suspension');
    expect(pending.payload).toEqual({ input: { x: 1 }, suspendData: { progress: 42 } });
    expect(session.getDisplayState().pending?.kind).toBe('tool-suspension');
  });

  it('classifies as "question" when the tool name is ask_user', async () => {
    const { harness, agent } = setup();
    agent.enqueueRun({
      finishReason: 'suspended',
      runId: 'run-C',
      suspendPayload: {
        toolCallId: 'tc-3',
        toolName: 'ask_user',
        args: {
          question: 'pick a color',
          options: [{ label: 'red' }, { label: 'blue' }],
          selectionMode: 'single_select',
        },
      },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });

    await session.message({ content: 'choose' });

    const pending = session.getRecord().pendingResume!;
    expect(pending.kind).toBe('question');
    expect(pending.itemId).toBe('question:tc-3');
    expect(pending.payload).toEqual({
      question: 'pick a color',
      options: [{ label: 'red' }, { label: 'blue' }],
      selectionMode: 'single_select',
    });
    expect(session.getDisplayState().pending?.kind).toBe('question');
  });

  it('classifies as "plan-approval" when the tool name is submit_plan, freezing transitionsTo', async () => {
    const { harness, agent } = setup([
      { id: 'planner', agentId: 'default', transitionsTo: 'builder' },
      { id: 'builder', agentId: 'default' },
    ]);
    agent.enqueueRun({
      finishReason: 'suspended',
      runId: 'run-D',
      suspendPayload: {
        toolCallId: 'tc-4',
        toolName: 'submit_plan',
        args: { title: 'Refactor X', plan: 'do A then B' },
      },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await session.switchMode({ mode: 'planner' });

    await session.message({ content: 'plan it' });

    const pending = session.getRecord().pendingResume!;
    expect(pending.kind).toBe('plan-approval');
    expect(pending.itemId).toBe('plan-approval:tc-4');
    expect(pending.payload).toEqual({ title: 'Refactor X', plan: 'do A then B' });
    expect(pending.transitionModeId).toBe('builder');
    expect(session.getDisplayState().pending?.kind).toBe('plan-approval');
  });

  it('does not write pendingResume when finishReason is not "suspended"', async () => {
    const { harness, agent } = setup();
    agent.enqueueRun({ finishReason: 'stop', text: 'done' });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });

    await session.message({ content: 'hi' });

    expect(session.getRecord().pendingResume).toBeUndefined();
  });

  it('persists a message mode override on pendingResume and resumes through that mode', async () => {
    const agentA = new FakeAgent();
    const agentB = new FakeAgent();
    const storage = new InMemoryHarness({ db: new InMemoryDB() });
    const harness = new Harness({
      agents: { agentA, agentB } as any,
      modes: [
        { id: 'modeA', agentId: 'agentA' },
        { id: 'modeB', agentId: 'agentB' },
      ],
      defaultModeId: 'modeA',
      sessions: { storage },
    });
    agentB.enqueueRun({
      finishReason: 'suspended',
      runId: 'run-mode-b',
      suspendPayload: { toolCallId: 'tc-mode-b', toolName: 'shell', args: { cmd: 'ls' } },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });

    await session.message({ content: 'use mode b', mode: 'modeB' });

    expect(session.getRecord().pendingResume).toMatchObject({
      modeId: 'modeB',
    });
    expect(agentB.streamCalls).toHaveLength(1);
    expect(agentA.streamCalls).toHaveLength(0);

    agentB.enqueueRun({ finishReason: 'stop', runId: 'run-mode-b', text: 'resumed on mode b' });
    const result = await session.respondToToolApproval({ approved: true });

    expect(result.text).toBe('resumed on mode b');
    expect(agentB.resumeCalls).toHaveLength(1);
    expect(agentA.resumeCalls).toHaveLength(0);
  });

  it('keeps a question registered by the tool before suspend capture', async () => {
    const { harness } = setup();
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });

    await (session as any)._registerQuestion({
      questionId: 'tc-registered',
      question: 'registered prompt',
      options: [{ label: 'yes' }],
      selectionMode: 'single_select',
      runId: 'run-registered',
      toolCallId: 'tc-registered',
    });
    await (session as any)._maybeCaptureSuspend({
      runId: 'run-registered',
      finishReason: 'suspended',
      suspendPayload: {
        toolCallId: 'tc-registered',
        toolName: 'ask_user',
        args: { question: 'stale prompt' },
      },
    });

    const pending = session.getRecord().pendingResume!;
    expect(pending.kind).toBe('question');
    expect(pending.itemId).toBe('tc-registered');
    expect(pending.modeId).toBe('default');
    expect(pending.payload).toEqual({
      question: 'registered prompt',
      options: [{ label: 'yes' }],
      selectionMode: 'single_select',
    });
  });

  it('§4.2e: a pre-registered question pending carries the active turn yolo (so resume keeps auto-granting)', async () => {
    // Regression for the yolo-lost-on-pre-registered-pending bug: registerQuestion
    // writes the pending BEFORE suspend capture, and capture early-returns on the
    // matching run/toolCallId, so yolo must be stamped on the registered pending.
    const { harness } = setup();
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });

    // Arm the turn yolo as the queued-drain / resume paths do.
    (session as any)._currentTurnYolo = true;
    await (session as any)._registerQuestion({
      questionId: 'tc-yolo-q',
      question: 'proceed?',
      runId: 'run-yolo-q',
      toolCallId: 'tc-yolo-q',
    });
    await (session as any)._maybeCaptureSuspend({
      runId: 'run-yolo-q',
      finishReason: 'suspended',
      suspendPayload: { toolCallId: 'tc-yolo-q', toolName: 'ask_user', args: { question: 'proceed?' } },
    });
    expect(session.getRecord().pendingResume?.yolo).toBe(true);
  });

  it('§4.2e: a non-yolo turn does NOT stamp yolo on a pre-registered question pending', async () => {
    const { harness } = setup();
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });

    // _currentTurnYolo defaults false; do not arm it.
    await (session as any)._registerQuestion({
      questionId: 'tc-plain-q',
      question: 'proceed?',
      runId: 'run-plain-q',
      toolCallId: 'tc-plain-q',
    });
    await (session as any)._maybeCaptureSuspend({
      runId: 'run-plain-q',
      finishReason: 'suspended',
      suspendPayload: { toolCallId: 'tc-plain-q', toolName: 'ask_user', args: { question: 'proceed?' } },
    });
    expect(session.getRecord().pendingResume?.yolo).toBeUndefined();
  });

  it('keeps a registered question when suspend capture reports a generic tool suspension', async () => {
    const { harness } = setup();
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });

    await (session as any)._registerQuestion({
      questionId: 'sandbox-registered',
      question: 'Allow access?',
      options: [{ label: 'yes' }],
      selectionMode: 'single_select',
      runId: 'run-sandbox',
      toolCallId: 'tc-sandbox',
    });
    await (session as any)._maybeCaptureSuspend({
      runId: 'run-sandbox',
      finishReason: 'suspended',
      suspendPayload: {
        toolCallId: 'tc-sandbox',
        toolName: 'request_access',
        suspendPayload: {},
      },
    });

    const pending = session.getRecord().pendingResume!;
    expect(pending.kind).toBe('question');
    expect(pending.itemId).toBe('sandbox-registered');
    expect(pending.toolCallId).toBe('tc-sandbox');
    expect(pending.payload).toEqual({
      question: 'Allow access?',
      options: [{ label: 'yes' }],
      selectionMode: 'single_select',
    });
  });

  it('keeps a plan approval registered by the tool before suspend capture', async () => {
    const { harness } = setup([
      { id: 'planner', agentId: 'default', transitionsTo: 'builder' },
      { id: 'builder', agentId: 'default' },
    ]);
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await session.switchMode({ mode: 'planner' });

    await (session as any)._registerPlanApproval({
      planId: 'tc-plan',
      title: 'registered title',
      plan: 'registered plan',
      runId: 'run-plan',
      toolCallId: 'tc-plan',
      modeId: 'planner',
    });
    await (session as any)._maybeCaptureSuspend({
      runId: 'run-plan',
      finishReason: 'suspended',
      suspendPayload: {
        toolCallId: 'tc-plan',
        toolName: 'submit_plan',
        args: { title: 'stale title', plan: 'stale plan' },
      },
    });

    const pending = session.getRecord().pendingResume!;
    expect(pending.kind).toBe('plan-approval');
    expect(pending.itemId).toBe('tc-plan');
    expect(pending.modeId).toBe('planner');
    expect(pending.payload).toEqual({ title: 'registered title', plan: 'registered plan' });
    expect(pending.transitionModeId).toBe('builder');
  });
});

// ---------------------------------------------------------------------------
// Resume — happy path
// ---------------------------------------------------------------------------

describe('Session — respondToToolApproval / Suspension / Question / PlanApproval', () => {
  it('captures a durable 10-minute interaction deadline', async () => {
    const { harness, agent } = setup();
    agent.enqueueRun({
      finishReason: 'suspended',
      suspendPayload: { toolCallId: 'tc-deadline', toolName: 'shell', args: { cmd: 'ls' } },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await session.message({ content: 'go' });

    const pending = session.getRecord().pendingResume!;
    expect(pending.expiresAt).toBe(pending.requestedAt + 10 * 60 * 1_000);
    expect(session.getDisplayState().pending?.expiresAt).toBe(pending.expiresAt);
  });

  it('fails closed with a typed corruption error when durable pending state has no valid expiresAt', async () => {
    const { harness, agent } = setup();
    agent.enqueueRun({
      finishReason: 'suspended',
      suspendPayload: { toolCallId: 'tc-corrupt-deadline', toolName: 'shell', args: { cmd: 'ls' } },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await session.message({ content: 'go' });
    const pending = session.getRecord().pendingResume!;
    const validRecord = (session as any)._record;
    const corruptPending = { ...validRecord.pendingResume };
    delete corruptPending.expiresAt;
    (session as any)._record = { ...validRecord, pendingResume: corruptPending };

    const response = session.respondToToolApproval({
      ...pendingResponseIdentity(pending),
      responseId: 'corrupt-deadline-response',
      approved: true,
    });
    await expect(response).rejects.toMatchObject({
      name: HarnessSessionCorruptError.name,
      code: 'harness.session_corrupt',
      reason: 'pending_state_corrupt',
    });
    expect(agent.resumeCalls).toHaveLength(0);
    (session as any)._record = validRecord;
    await harness.shutdown();
  });

  it('atomically persists allow-always with response admission and deduplicates retries', async () => {
    const { harness, agent } = setup();
    agent.enqueueRun({
      finishReason: 'suspended',
      suspendPayload: { toolCallId: 'tc-always', toolName: 'shell', args: { cmd: 'ls' } },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await session.message({ content: 'go' });
    const pending = session.getRecord().pendingResume!;
    const events: string[] = [];
    session.subscribe(event => {
      if (event.type === 'permission_granted') events.push(event.toolName ?? event.category ?? '');
    });
    agent.enqueueRun({ finishReason: 'stop', text: 'done' });

    const response = {
      ...pendingResponseIdentity(pending),
      responseId: 'allow-always-response',
      approved: true,
      approvalScope: 'always' as const,
    };
    const first = await session.respondToToolApproval(response);
    const duplicate = await session.respondToToolApproval(response);

    expect(first).toMatchObject({ status: 'applied', duplicate: false });
    expect(duplicate).toMatchObject({ status: 'applied', duplicate: true });
    expect(session.permissions.getGrants().tools).toEqual(['shell']);
    expect(session.getRecord().inboxResponseReceipts?.['allow-always-response']).toMatchObject({
      approvalScope: 'always',
      status: 'applied',
    });
    expect(events).toEqual(['shell']);
    expect(agent.resumeCalls).toHaveLength(1);
  });

  it('lets Stop cancel execution after admission while retaining the committed allow-always policy grant', async () => {
    const { harness, agent } = setup();
    agent.enqueueRun({
      finishReason: 'suspended',
      suspendPayload: { toolCallId: 'tc-stop-race', toolName: 'shell', args: { cmd: 'rm draft.tmp' } },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await session.message({ content: 'remove the temporary draft' });
    const pending = session.getRecord().pendingResume!;
    const events: string[] = [];
    session.subscribe(event => {
      if (event.type === 'permission_granted') events.push(event.toolName ?? event.category ?? '');
    });

    const originalFlush = (session as any)._flushUpdate.bind(session);
    let releaseAdmission!: () => void;
    let admissionCommitted!: () => void;
    let gateAdmission = true;
    const admissionCommittedPromise = new Promise<void>(resolve => (admissionCommitted = resolve));
    const admissionGate = new Promise<void>(resolve => (releaseAdmission = resolve));
    (session as any)._flushUpdate = async (updater: any) => {
      const wasResumed = session.getRecord().pendingResume?.resumedAt !== undefined;
      const result = await originalFlush(updater);
      if (gateAdmission && !wasResumed && session.getRecord().pendingResume?.resumedAt !== undefined) {
        gateAdmission = false;
        admissionCommitted();
        await admissionGate;
      }
      return result;
    };

    const response = session.respondToToolApproval({
      ...pendingResponseIdentity(pending),
      responseId: 'stop-race-response',
      approved: true,
      approvalScope: 'always',
    });
    await admissionCommittedPromise;
    await session.abortActiveWork({ reason: 'user_requested', settleTimeoutMs: 1_000 });
    releaseAdmission();

    await expect(response).rejects.toBeInstanceOf(HarnessBusyError);
    expect(agent.resumeCalls).toHaveLength(0);
    expect(session.permissions.getGrants().tools).toEqual(['shell']);
    expect(events).toEqual(['shell']);
    expect(session.getRecord().pendingResume).toBeUndefined();
    expect(session.getRecord().inboxResponseReceipts?.['stop-race-response']).toMatchObject({
      status: 'failed',
      retryable: false,
    });
    await harness.shutdown();
  });

  it('rejects allow-always on a denial before mutating durable grants', async () => {
    const { harness, agent } = setup();
    agent.enqueueRun({
      finishReason: 'suspended',
      suspendPayload: { toolCallId: 'tc-deny-always', toolName: 'shell', args: { cmd: 'rm' } },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await session.message({ content: 'go' });

    await expect(session.respondToToolApproval({ approved: false, approvalScope: 'always' })).rejects.toBeInstanceOf(
      HarnessValidationError,
    );
    expect(session.permissions.getGrants().tools).toEqual([]);
    expect(session.getRecord().pendingResume).toBeDefined();
    expect(agent.resumeCalls).toHaveLength(0);
  });

  it('expires an unanswered interaction, aborts only that run, and rejects a late response durably', async () => {
    const { harness, agent } = setup(undefined, { pendingInteractionTtlMs: 20 });
    agent.enqueueRun({
      finishReason: 'suspended',
      suspendPayload: { toolCallId: 'tc-expire', toolName: 'shell', args: { cmd: 'ls' } },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await session.message({ content: 'go' });
    const pending = session.getRecord().pendingResume!;

    await waitFor(() => session.getRecord().pendingResume === undefined, 'pending interaction expiry', 1_000);
    expect(Object.values(session.getRecord().expiredPendingInteractions ?? {})).toContainEqual(
      expect.objectContaining({
        itemId: pending.itemId,
        kind: 'tool-approval',
        expiresAt: pending.expiresAt,
        error: expect.objectContaining({ code: 'harness.pending_interaction_expired' }),
      }),
    );
    await expect(
      session.respondToToolApproval({
        ...pendingResponseIdentity(pending),
        responseId: 'late-response',
        approved: true,
      }),
    ).rejects.toBeInstanceOf(HarnessPendingInteractionExpiredError);
    expect(agent.resumeCalls).toHaveLength(0);
    expect(session.isClosed).toBe(false);

    // Expiry terminalizes the abandoned run, not the project/thread/session.
    agent.enqueueRun({ finishReason: 'stop', text: 'recovered' });
    await expect(session.message({ content: 'new turn' })).resolves.toMatchObject({ text: 'recovered' });
  });

  it('applies a per-kind TTL override and leaves every other kind on the default', async () => {
    const { harness, agent } = setup(undefined, {
      pendingInteractionTtlMs: 60_000,
      pendingInteractionTtlMsByKind: { 'tool-approval': 900_000 },
    });

    agent.enqueueRun({
      finishReason: 'suspended',
      suspendPayload: { toolCallId: 'tc-kind-approval', toolName: 'shell', args: { cmd: 'rm draft.tmp' } },
    });
    const approvalSession = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await approvalSession.message({ content: 'remove the draft' });
    const approval = approvalSession.getRecord().pendingResume!;
    expect(approval.kind).toBe('tool-approval');
    expect(approval.expiresAt).toBe(approval.requestedAt + 900_000);

    // A nested suspendPayload classifies as 'tool-suspension', which carries no
    // override and must therefore keep the single default deadline.
    agent.enqueueRun({
      finishReason: 'suspended',
      suspendPayload: {
        toolCallId: 'tc-kind-suspension',
        toolName: 'shell',
        args: { cmd: 'ls' },
        suspendPayload: { prompt: 'which directory?' },
      },
    });
    const suspensionSession = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await suspensionSession.message({ content: 'list something' });
    const suspension = suspensionSession.getRecord().pendingResume!;
    expect(suspension.kind).toBe('tool-suspension');
    expect(suspension.expiresAt).toBe(suspension.requestedAt + 60_000);

    await harness.shutdown();
  });

  it('rejects an unknown kind or an out-of-range per-kind pending-interaction TTL at construction', () => {
    const build = (byKind: Record<string, number>) =>
      new Harness({
        agents: { default: new FakeAgent() } as any,
        modes: [{ id: 'default', agentId: 'default' }],
        defaultModeId: 'default',
        sessions: {
          storage: new InMemoryHarness({ db: new InMemoryDB() }),
          pendingInteractionTtlMsByKind: byKind,
        } as any,
      });

    expect(() => build({ 'tool-aproval': 1_000 })).toThrow(/pendingInteractionTtlMsByKind\.tool-aproval/);
    expect(() => build({ 'sandbox-access': 0 })).toThrow(/pendingInteractionTtlMsByKind\.sandbox-access/);
    expect(() => build({ question: 1.5 })).toThrow(/pendingInteractionTtlMsByKind\.question/);
  });

  // The post-expiry terminal sequence is the contract the Doxa projection
  // depends on to close out a parked run. `_expirePendingInteraction` emits
  //   _emitAbortedToolEnds({force:true}) → resume_failed → agent_end(error)
  // and `run_completed` rides the terminal agent_end, but the first and third
  // are conditional: a PARKED interaction has no live turn, so its active-tool
  // map was already drained by `_endTurn` (no tool_end) and expiry owns the
  // run terminal; a STILL-LIVE pre-registered owner keeps its own terminal but
  // does have an active tool. The two tests below pin both orders so an
  // upstream merge cannot reorder or drop any of them.
  it('terminalizes an expired parked run as resume_failed → agent_end(error) → run_completed(failed)', async () => {
    const { harness, agent } = setup(undefined, { pendingInteractionTtlMs: 20 });
    agent.enqueueRun({
      finishReason: 'suspended',
      suspendPayload: { toolCallId: 'tc-order-expire', toolName: 'shell', args: { cmd: 'rm -rf build' } },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    const observed: any[] = [];

    await session.message({ content: 'clean the build directory' });
    const pending = session.getRecord().pendingResume!;
    // Subscribe only once the turn has parked, so the park's own agent_end is
    // not confused with the terminal expiry emits.
    session.subscribe(event => {
      if (EXPIRY_TERMINAL_EVENT_TYPES.has(event.type)) observed.push(event);
    });

    await waitFor(() => session.getRecord().pendingResume === undefined, 'pending interaction expiry', 1_000);
    await waitFor(() => observed.some(event => event.type === 'run_completed'), 'run_completed', 1_000);

    expect(observed.map(event => event.type)).toEqual(['resume_failed', 'agent_end', 'run_completed']);
    const [resumeFailed, agentEnd, runCompleted] = observed;
    expect(resumeFailed).toMatchObject({
      retryable: false,
      error: { code: 'harness.pending_interaction_expired' },
    });
    expect(agentEnd).toMatchObject({ finishReason: 'error' });
    expect(runCompleted).toMatchObject({ status: 'failed', finishReason: 'error' });
    // Every terminal event closes the SAME parked run.
    expect(new Set(observed.map(event => event.runId))).toEqual(new Set([pending.runId]));

    await harness.shutdown();
  });

  it('emits tool_end(aborted) before resume_failed and leaves the terminal to a still-live owner', async () => {
    const { harness, agent } = setup(undefined, { pendingInteractionTtlMs: 200 });
    agent.enqueueRun({
      finishReason: 'suspended',
      suspendPayload: { toolCallId: 'tc-live-expire', toolName: 'shell', args: { cmd: 'rm -rf build' } },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await session.message({ content: 'clean the build directory' });
    const pending = session.getRecord().pendingResume!;

    // Model the still-running pre-registered ask/plan/sandbox owner: its tool
    // is live and it still holds the turn's abort controller.
    const controller = new AbortController();
    (session as any)._currentRunId = pending.runId;
    (session as any)._currentTurnAbortController = controller;
    (session as any)._activeTools.set(pending.toolCallId, {
      toolCallId: pending.toolCallId,
      toolName: pending.toolName,
      args: {},
      startedAt: Date.now(),
    });

    const observed: any[] = [];
    session.subscribe(event => {
      if (EXPIRY_TERMINAL_EVENT_TYPES.has(event.type)) observed.push(event);
    });

    await waitFor(() => session.getRecord().pendingResume === undefined, 'pending interaction expiry', 2_000);

    expect(observed.map(event => event.type)).toEqual(['tool_end', 'resume_failed']);
    expect(observed[0]).toMatchObject({
      runId: pending.runId,
      toolCallId: pending.toolCallId,
      isError: true,
      output: { aborted: true },
    });
    expect(observed[1]).toMatchObject({
      runId: pending.runId,
      retryable: false,
      error: { code: 'harness.pending_interaction_expired' },
    });
    // The live owner is aborted and keeps ownership of agent_end/run_completed.
    expect(controller.signal.aborted).toBe(true);

    // Unwind the modelled owner through the real teardown so the session reads
    // idle for shutdown (expiry already drained the active-tool map).
    (session as any)._endTurn(controller);
    await harness.shutdown();
  });

  it('keeps the original deadline across shutdown and hydration', async () => {
    const { harness, agent, storage } = setup(undefined, { pendingInteractionTtlMs: 20 });
    agent.enqueueRun({
      finishReason: 'suspended',
      suspendPayload: { toolCallId: 'tc-restart-expire', toolName: 'shell', args: { cmd: 'ls' } },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await session.message({ content: 'go' });
    const sessionId = session.id;
    const pending = session.getRecord().pendingResume!;
    await harness.shutdown();
    await new Promise(resolve => setTimeout(resolve, Math.max(0, pending.expiresAt! - Date.now() + 5)));

    const harness2 = new Harness({
      agents: { default: agent } as any,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      // A much longer new config must not move the already-persisted deadline.
      sessions: { storage, pendingInteractionTtlMs: 60_000 },
    });
    const restored = await harness2.session({ sessionId, resourceId: 'u' });

    expect(restored.getRecord().pendingResume).toBeUndefined();
    expect(Object.values(restored.getRecord().expiredPendingInteractions ?? {})[0]?.expiresAt).toBe(pending.expiresAt);
    await expect(
      restored.respondToToolApproval({
        ...pendingResponseIdentity(pending),
        responseId: 'late-after-restart',
        approved: true,
      }),
    ).rejects.toBeInstanceOf(HarnessPendingInteractionExpiredError);
    expect(agent.resumeCalls).toHaveLength(0);
  });

  it('cold-sweeps an overdue interaction after restart without requiring session hydration', async () => {
    const { harness, agent, storage } = setup(undefined, { pendingInteractionTtlMs: 40 });
    agent.enqueueRun({
      finishReason: 'suspended',
      suspendPayload: { toolCallId: 'tc-cold-expire', toolName: 'shell', args: { cmd: 'ls' } },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await session.message({ content: 'go' });
    const sessionId = session.id;
    const pending = session.getRecord().pendingResume!;
    await harness.shutdown();

    const harness2 = new Harness({
      agents: { default: new FakeAgent() } as any,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage, pendingInteractionTtlMs: 60_000 },
    });

    let stored = await storage.loadSession({ sessionId });
    const deadline = Date.now() + 2_000;
    while (stored?.pendingResume !== undefined && Date.now() < deadline) {
      await new Promise(resolve => setTimeout(resolve, 10));
      stored = await storage.loadSession({ sessionId });
    }

    expect(stored?.pendingResume).toBeUndefined();
    expect(harness2._internalLiveSessionCount()).toBe(0);
    expect(Object.values(stored?.expiredPendingInteractions ?? {})).toContainEqual(
      expect.objectContaining({
        itemId: pending.itemId,
        runId: pending.runId,
        toolCallId: pending.toolCallId,
        requestedAt: pending.requestedAt,
        expiresAt: pending.expiresAt,
      }),
    );

    const restored = await harness2.session({ sessionId, resourceId: 'u' });
    await expect(
      restored.respondToToolApproval({
        ...pendingResponseIdentity(pending),
        responseId: 'late-after-cold-sweep',
        approved: true,
      }),
    ).rejects.toBeInstanceOf(HarnessPendingInteractionExpiredError);
    await harness2.shutdown();
  });

  it('cold-terminalizes an admitted resume abandoned by a process crash without redispatching it', async () => {
    const { harness, agent, storage } = setup(undefined, { pendingInteractionTtlMs: 60_000 });
    agent.enqueueRun({
      finishReason: 'suspended',
      suspendPayload: { toolCallId: 'tc-admitted-crash', toolName: 'shell', args: { cmd: 'publish' } },
    });
    const source = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await source.message({ content: 'go' });
    const sessionId = source.id;
    const pending = source.getRecord().pendingResume!;
    const responseId = 'response-admitted-before-crash';
    const response = { approved: true };
    const responseHash = (source as any)._computeInboxResponseHash({
      kind: pending.kind,
      itemId: pending.itemId,
      runId: pending.runId,
      pendingRequestedAt: pending.requestedAt,
      response,
      approvalScope: 'once',
    });
    await harness.shutdown();

    const fixtureOwner = 'admitted-resume-crash-fixture';
    const lease = await storage.acquireSessionLease({ sessionId, ownerId: fixtureOwner, ttlMs: 30_000 });
    const stored = await storage.loadSession({ sessionId });
    if (!stored?.pendingResume?.itemId) throw new Error('expected pending interaction fixture');
    const resumedAt = Date.now() - 60_000;
    await storage.saveSession(
      {
        ...stored,
        ownerId: fixtureOwner,
        leaseExpiresAt: lease.expiresAt,
        pendingResume: {
          ...stored.pendingResume,
          resumedAt,
          resumeRecoveryAt: resumedAt + 30_000,
        },
        inboxResponseReceipts: {
          ...(stored.inboxResponseReceipts ?? {}),
          [responseId]: {
            responseId,
            responseHash,
            resumeAttemptId: responseId,
            itemId: stored.pendingResume.itemId,
            kind: stored.pendingResume.kind,
            runId: stored.pendingResume.runId,
            toolCallId: stored.pendingResume.toolCallId,
            pendingRequestedAt: stored.pendingResume.requestedAt,
            response,
            approvalScope: 'once',
            status: 'accepted',
            acceptedAt: resumedAt,
            updatedAt: resumedAt,
          },
        },
      },
      { ownerId: fixtureOwner, ifVersion: lease.version },
    );
    await storage.releaseSessionLease({ sessionId, ownerId: fixtureOwner });

    const recoveryAgent = new FakeAgent();
    const harness2 = new Harness({
      agents: { default: recoveryAgent } as any,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage, pendingInteractionTtlMs: 60_000 },
    });

    let recovered = await storage.loadSession({ sessionId });
    const deadline = Date.now() + 2_000;
    while (recovered?.pendingResume !== undefined && Date.now() < deadline) {
      await new Promise(resolve => setTimeout(resolve, 10));
      recovered = await storage.loadSession({ sessionId });
    }

    expect(recovered?.pendingResume).toBeUndefined();
    expect(recovered?.inboxResponseReceipts?.[responseId]).toMatchObject({
      status: 'failed',
      retryable: false,
      error: { code: 'harness.resume_recovery_stale' },
    });
    expect(recovered?.expiredPendingInteractions).toBeUndefined();
    expect(recoveryAgent.resumeCalls).toHaveLength(0);
    expect(harness2._internalLiveSessionCount()).toBe(0);

    const restored = await harness2.session({ sessionId, resourceId: 'u' });
    await expect(
      restored.respondToToolApproval({
        ...pendingResponseIdentity(pending),
        responseId,
        approved: true,
        approvalScope: 'once',
      }),
    ).rejects.toThrow('resume was admitted but no live owner');
    expect(recoveryAgent.resumeCalls).toHaveLength(0);
    await harness2.shutdown();
  });

  it('cold expiry bypasses a maxLive slot occupied by an unrelated pinned session', async () => {
    const { harness, agent, storage } = setup(undefined, { pendingInteractionTtlMs: 60_000 });
    agent.enqueueRun({
      finishReason: 'suspended',
      suspendPayload: { toolCallId: 'tc-pinned', toolName: 'shell', args: { cmd: 'one' } },
    });
    const pinnedSource = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await pinnedSource.message({ content: 'one' });
    agent.enqueueRun({
      finishReason: 'suspended',
      suspendPayload: { toolCallId: 'tc-due-cold', toolName: 'shell', args: { cmd: 'two' } },
    });
    const dueSource = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await dueSource.message({ content: 'two' });
    const dueSessionId = dueSource.id;
    await harness.shutdown();

    const fixtureOwner = 'pending-expiry-fixture';
    const lease = await storage.acquireSessionLease({
      sessionId: dueSessionId,
      ownerId: fixtureOwner,
      ttlMs: 30_000,
    });
    const dueStored = await storage.loadSession({ sessionId: dueSessionId });
    if (!dueStored?.pendingResume) throw new Error('expected due pending interaction fixture');
    const dueExpiresAt = Date.now() - 1;
    await storage.saveSession(
      {
        ...dueStored,
        ownerId: fixtureOwner,
        leaseExpiresAt: lease.expiresAt,
        pendingResume: {
          ...dueStored.pendingResume,
          requestedAt: Math.min(dueStored.pendingResume.requestedAt, dueExpiresAt),
          expiresAt: dueExpiresAt,
        },
      },
      { ownerId: fixtureOwner, ifVersion: lease.version },
    );
    await storage.releaseSessionLease({ sessionId: dueSessionId, ownerId: fixtureOwner });

    const harness2 = new Harness({
      agents: { default: new FakeAgent() } as any,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage, maxLive: 1, pendingInteractionTtlMs: 60_000 },
    });
    // Hold the constructor's immediate sweep until the unrelated session has
    // occupied the only live slot, then run one deterministic pass ourselves.
    (harness2 as any)._stopPendingInteractionExpirySweepLoop();
    (harness2 as any)._pendingInteractionExpirySweepRunning = true;
    const pinned = await harness2.session({ sessionId: pinnedSource.id, resourceId: 'u' });
    expect(pinned.getRecord().pendingResume).toBeDefined();
    expect(harness2._internalLiveSessionCount()).toBe(1);
    (harness2 as any)._pendingInteractionExpirySweepRunning = false;

    await harness2._tickPendingInteractionExpirySweep();

    const expired = await storage.loadSession({ sessionId: dueSessionId });
    expect(expired?.pendingResume).toBeUndefined();
    expect(harness2._internalLiveSessionCount()).toBe(1);
    expect(pinned.getRecord().pendingResume).toBeDefined();
    await harness2.shutdown();
  });

  it('fences a stale due-scan generation after the pending interaction is replaced', async () => {
    const { harness, agent, storage } = setup(undefined, { pendingInteractionTtlMs: 60_000 });
    agent.enqueueRun({
      finishReason: 'suspended',
      suspendPayload: { toolCallId: 'tc-stale-generation', toolName: 'shell', args: { cmd: 'old' } },
    });
    const source = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await source.message({ content: 'old' });
    const sessionId = source.id;
    await harness.shutdown();

    const fixtureOwner = 'pending-expiry-aba-fixture';
    const lease = await storage.acquireSessionLease({ sessionId, ownerId: fixtureOwner, ttlMs: 30_000 });
    const stored = await storage.loadSession({ sessionId });
    if (!stored?.pendingResume) throw new Error('expected pending interaction fixture');
    const staleExpiresAt = Date.now() - 1;
    await storage.saveSession(
      {
        ...stored,
        ownerId: fixtureOwner,
        leaseExpiresAt: lease.expiresAt,
        pendingResume: {
          ...stored.pendingResume,
          requestedAt: Math.min(stored.pendingResume.requestedAt, staleExpiresAt),
          expiresAt: staleExpiresAt,
        },
      },
      { ownerId: fixtureOwner, ifVersion: lease.version },
    );
    const staleGeneration = (await storage.listDuePendingInteractions({ now: Date.now(), limit: 1 })).items[0];
    if (!staleGeneration) throw new Error('expected stale due-scan generation');

    const dueStored = await storage.loadSession({ sessionId });
    if (!dueStored?.pendingResume) throw new Error('expected due pending interaction fixture');
    const replacementRequestedAt = dueStored.pendingResume.requestedAt + 1;
    await storage.saveSession(
      {
        ...dueStored,
        ownerId: fixtureOwner,
        leaseExpiresAt: lease.expiresAt,
        pendingResume: {
          ...dueStored.pendingResume,
          itemId: 'replacement-item',
          runId: 'replacement-run',
          toolCallId: 'replacement-tool-call',
          requestedAt: replacementRequestedAt,
          expiresAt: Date.now() + 60_000,
          resumedAt: undefined,
          expiryStartedAt: undefined,
        },
      },
      { ownerId: fixtureOwner, ifVersion: dueStored.version },
    );
    await storage.releaseSessionLease({ sessionId, ownerId: fixtureOwner });

    const harness2 = new Harness({
      agents: { default: new FakeAgent() } as any,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage, pendingInteractionTtlMs: 60_000 },
    });
    (harness2 as any)._stopPendingInteractionExpirySweepLoop();
    (harness2 as any)._pendingInteractionExpirySweepRunning = true;
    await (harness2 as any)._runPendingInteractionExpiryGeneration(storage, staleGeneration);
    (harness2 as any)._pendingInteractionExpirySweepRunning = false;

    const after = await storage.loadSession({ sessionId });
    expect(after?.pendingResume).toMatchObject({
      itemId: 'replacement-item',
      runId: 'replacement-run',
      toolCallId: 'replacement-tool-call',
      requestedAt: replacementRequestedAt,
    });
    expect(after?.expiredPendingInteractions).toBeUndefined();
    await harness2.shutdown();
  });

  it('retries a cold due row after a competing lease is released', async () => {
    const { harness, agent, storage } = setup(undefined, { pendingInteractionTtlMs: 60_000 });
    agent.enqueueRun({
      finishReason: 'suspended',
      suspendPayload: { toolCallId: 'tc-lease-retry', toolName: 'shell', args: { cmd: 'ls' } },
    });
    const source = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await source.message({ content: 'go' });
    const sessionId = source.id;
    await harness.shutdown();

    const competingOwner = 'pending-expiry-competing-owner';
    const lease = await storage.acquireSessionLease({ sessionId, ownerId: competingOwner, ttlMs: 30_000 });
    const stored = await storage.loadSession({ sessionId });
    if (!stored?.pendingResume) throw new Error('expected pending interaction fixture');
    const retryExpiresAt = Date.now() - 1;
    await storage.saveSession(
      {
        ...stored,
        ownerId: competingOwner,
        leaseExpiresAt: lease.expiresAt,
        pendingResume: {
          ...stored.pendingResume,
          requestedAt: Math.min(stored.pendingResume.requestedAt, retryExpiresAt),
          expiresAt: retryExpiresAt,
        },
      },
      { ownerId: competingOwner, ifVersion: lease.version },
    );

    const harness2 = new Harness({
      agents: { default: new FakeAgent() } as any,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage, pendingInteractionTtlMs: 60_000 },
    });
    (harness2 as any)._stopPendingInteractionExpirySweepLoop();
    (harness2 as any)._pendingInteractionExpirySweepRunning = true;
    await Promise.resolve();
    (harness2 as any)._pendingInteractionExpirySweepRunning = false;

    await harness2._tickPendingInteractionExpirySweep();
    expect((await storage.loadSession({ sessionId }))?.pendingResume).toBeDefined();

    await storage.releaseSessionLease({ sessionId, ownerId: competingOwner });
    await harness2._tickPendingInteractionExpirySweep();
    expect((await storage.loadSession({ sessionId }))?.pendingResume).toBeUndefined();
    await harness2.shutdown();
  });

  it('shutdown waits for an in-flight cold sweep to leave storage ownership cleanly', async () => {
    const { harness, storage } = setup();
    (harness as any)._stopPendingInteractionExpirySweepLoop();
    (harness as any)._pendingInteractionExpirySweepRunning = true;
    await Promise.resolve();
    (harness as any)._pendingInteractionExpirySweepRunning = false;

    let releaseQuery!: () => void;
    const queryGate = new Promise<void>(resolve => {
      releaseQuery = resolve;
    });
    let markQueryEntered!: () => void;
    const queryEntered = new Promise<void>(resolve => {
      markQueryEntered = resolve;
    });
    const originalListDue = storage.listDuePendingInteractions.bind(storage);
    (storage as any).listDuePendingInteractions = async (options: Parameters<typeof originalListDue>[0]) => {
      markQueryEntered();
      await queryGate;
      return originalListDue(options);
    };

    const sweep = harness._tickPendingInteractionExpirySweep();
    await queryEntered;
    let shutdownSettled = false;
    const shutdown = harness.shutdown().then(() => {
      shutdownSettled = true;
    });
    await Promise.resolve();
    expect(shutdownSettled).toBe(false);

    releaseQuery();
    await Promise.all([sweep, shutdown]);
    expect(shutdownSettled).toBe(true);
    expect((harness as any)._pendingInteractionExpirySweepInFlight).toBeUndefined();
  });

  it('respondToToolApproval calls agent.resumeStream and clears pendingResume', async () => {
    const { harness, agent } = setup();
    agent.enqueueRun({
      finishReason: 'suspended',
      runId: 'run-A',
      suspendPayload: { toolCallId: 'tc-1', toolName: 'shell', args: { cmd: 'ls' } },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    const first = await session.message({ content: 'go' });

    agent.enqueueRun({ finishReason: 'stop', runId: first.runId, text: 'done' });

    const result = await session.respondToToolApproval({ approved: true });

    expect(result.text).toBe('done');
    expect(result.finishReason).toBe('stop');
    expect(agent.resumeCalls).toHaveLength(1);
    expect(agent.resumeCalls[0]!.resumeData).toEqual({ approved: true });
    expect(agent.resumeCalls[0]!.options).toMatchObject({ runId: first.runId, toolCallId: 'tc-1' });
    expect(agent.resumeCalls[0]!.options.abortSignal).toBeInstanceOf(AbortSignal);
    expect(session.getRecord().pendingResume).toBeUndefined();
    expect(session.getDisplayState().pending).toBeNull();
  });

  it('respondToToolApproval records denial receipts and forwards reason', async () => {
    const { harness, agent } = setup();
    agent.enqueueRun({
      finishReason: 'suspended',
      runId: 'run-deny',
      suspendPayload: { toolCallId: 'tc-deny', toolName: 'shell', args: { cmd: 'rm' } },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await session.message({ content: 'go' });
    const pending = session.getRecord().pendingResume!;

    agent.enqueueRun({ finishReason: 'stop', runId: 'run-deny', text: 'denied' });
    const receipt = await session.respondToToolApproval({
      approved: false,
      reason: 'needs review',
      ...pendingResponseIdentity(pending),
      responseId: 'deny-response-1',
    });

    expect(receipt).toEqual({
      itemId: 'tool-approval:tc-deny',
      kind: 'tool-approval',
      status: 'applied',
      responseId: 'deny-response-1',
      duplicate: false,
    });
    expect(agent.resumeCalls[0]!.resumeData).toEqual({ approved: false, reason: 'needs review' });
    expect(session.getRecord().inboxResponseReceipts?.['deny-response-1']).toMatchObject({
      itemId: 'tool-approval:tc-deny',
      kind: 'tool-approval',
      resumeAttemptId: 'deny-response-1',
      status: 'applied',
      response: { approved: false, reason: 'needs review' },
      result: expect.objectContaining({ text: 'denied' }),
    });
  });

  it('respondToToolSuspension forwards opaque resumeData', async () => {
    const { harness, agent } = setup();
    agent.enqueueRun({
      finishReason: 'suspended',
      runId: 'run-S',
      suspendPayload: {
        toolCallId: 'tc-S',
        toolName: 'long',
        args: {},
        suspendPayload: { step: 'A' },
      },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await session.message({ content: 'go' });

    agent.enqueueRun({ finishReason: 'stop', runId: 'run-S' });
    await session.respondToToolSuspension({ resumeData: { result: 'ok' } });

    expect(agent.resumeCalls[0]!.resumeData).toEqual({ result: 'ok' });
    expect(session.getRecord().pendingResume).toBeUndefined();
  });

  it('respondToToolSuspension without responseId preserves legacy opaque resumeData behavior', async () => {
    const { harness, agent } = setup();
    agent.enqueueRun({
      finishReason: 'suspended',
      runId: 'run-S',
      suspendPayload: {
        toolCallId: 'tc-S',
        toolName: 'long',
        args: {},
        suspendPayload: { step: 'A' },
      },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await session.message({ content: 'go' });

    const resumeData = { completedAt: new Date('2026-05-15T00:00:00.000Z') };
    agent.enqueueRun({ finishReason: 'stop', runId: 'run-S', text: 'done' });
    const result = await session.respondToToolSuspension({ resumeData });

    expect(result.text).toBe('done');
    expect(agent.resumeCalls[0]!.resumeData).toBe(resumeData);
    expect(session.getRecord().inboxResponseReceipts).toBeUndefined();
  });

  it('respondToQuestion forwards { answer }', async () => {
    const { harness, agent } = setup();
    agent.enqueueRun({
      finishReason: 'suspended',
      runId: 'run-Q',
      suspendPayload: {
        toolCallId: 'tc-Q',
        toolName: 'ask_user',
        args: { question: 'pick' },
      },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await session.message({ content: 'go' });

    agent.enqueueRun({ finishReason: 'stop', runId: 'run-Q' });
    await session.respondToQuestion({ answer: 'red' });

    expect(agent.resumeCalls[0]!.resumeData).toEqual({ answer: 'red' });
  });

  it('fails closed when a recovered question resume observes mode-to-agent binding drift', async () => {
    const storage = new InMemoryStore();
    const harnessStore = await storage.getStore('harness');
    if (!harnessStore) throw new Error('expected harness storage');
    const sessionId = 'sess-question-agent-drift';
    const requestedAt = Date.now();
    await harnessStore.saveSession(
      {
        harnessName: 'default',
        id: sessionId,
        resourceId: 'u',
        threadId: 't-question-agent-drift',
        origin: 'top-level',
        ownsThread: false,
        modeId: 'default',
        modelId: 'default',
        subagentModelOverrides: {},
        permissionRules: { categories: {}, tools: {} },
        sessionGrants: { categories: [], tools: [] },
        tokenUsage: { promptTokens: 0, completionTokens: 0, totalTokens: 0 },
        pendingQueue: [],
        pendingResume: {
          kind: 'question',
          itemId: 'question:tc-Q',
          runId: 'run-Q',
          toolCallId: 'tc-Q',
          toolName: 'ask_user',
          source: 'parent',
          requestedAt,
          expiresAt: requestedAt + 10 * 60 * 1_000,
          modeId: 'default',
          runtimeDependencies: { modeId: 'default', agentId: 'old-agent', modelId: 'default' },
          payload: { question: 'pick' },
        },
        state: undefined,
        createdAt: requestedAt,
        lastActivityAt: requestedAt,
        version: 0,
      },
      { harnessName: 'default', ifVersion: 0 },
    );
    const agent = new FakeAgent();
    const harness = new Harness({
      agents: { default: agent } as any,
      storage,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
    });

    const session = await harness.session({ sessionId });
    const responseIdentity = pendingResponseIdentity(session.getRecord().pendingResume!);
    await expect(
      session.respondToQuestion({ ...responseIdentity, responseId: 'answer-1', answer: 'red' }),
    ).rejects.toMatchObject({ code: 'harness.runtime_drift' });

    expect(agent.resumeCalls).toHaveLength(0);
    expect(session.getRecord().inboxResponseReceipts?.['answer-1']).toMatchObject({
      status: 'failed',
      error: { code: 'harness.runtime_drift' },
    });
    expect(session.getRecord().pendingResume).toMatchObject({ runId: 'run-Q' });
    expect(session.getRecord().pendingResume?.resumedAt).toBeUndefined();
  });

  it('fails closed when a recovered question resume observes runtime compatibility generation drift', async () => {
    const storage = new InMemoryStore();
    const harnessStore = await storage.getStore('harness');
    if (!harnessStore) throw new Error('expected harness storage');
    const sessionId = 'sess-question-generation-drift';
    const requestedAt = Date.now();
    await harnessStore.saveSession(
      {
        harnessName: 'default',
        id: sessionId,
        resourceId: 'u',
        threadId: 't-question-generation-drift',
        origin: 'top-level',
        ownsThread: false,
        modeId: 'default',
        modelId: 'default',
        subagentModelOverrides: {},
        permissionRules: { categories: {}, tools: {} },
        sessionGrants: { categories: [], tools: [] },
        tokenUsage: { promptTokens: 0, completionTokens: 0, totalTokens: 0 },
        pendingQueue: [],
        pendingResume: {
          kind: 'question',
          itemId: 'question:tc-Q-generation',
          runId: 'run-Q-generation',
          toolCallId: 'tc-Q-generation',
          toolName: 'ask_user',
          source: 'parent',
          requestedAt,
          expiresAt: requestedAt + 10 * 60 * 1_000,
          modeId: 'default',
          runtimeDependencies: {
            modeId: 'default',
            agentId: 'default',
            modelId: 'default',
            runtimeCompatibilityGeneration: 'generation-a',
          },
          payload: { question: 'pick' },
        },
        state: undefined,
        createdAt: requestedAt,
        lastActivityAt: requestedAt,
        version: 0,
      },
      { harnessName: 'default', ifVersion: 0 },
    );
    const agent = new FakeAgent();
    const harness = new Harness({
      agents: { default: agent } as any,
      storage,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      runtimeCompatibilityGeneration: 'generation-b',
    });

    const session = await harness.session({ sessionId });
    const responseIdentity = pendingResponseIdentity(session.getRecord().pendingResume!);
    await expect(
      session.respondToQuestion({ ...responseIdentity, responseId: 'answer-generation', answer: 'red' }),
    ).rejects.toMatchObject({ code: 'harness.runtime_drift' });

    expect(agent.resumeCalls).toHaveLength(0);
    expect(session.getRecord().inboxResponseReceipts?.['answer-generation']).toMatchObject({
      status: 'failed',
      error: {
        code: 'harness.runtime_drift',
        message: expect.stringContaining('recorded generation "generation-a"'),
      },
    });
    expect(session.getRecord().pendingResume).toMatchObject({ runId: 'run-Q-generation' });
    expect(session.getRecord().pendingResume?.resumedAt).toBeUndefined();
  });

  it('rejects an active question resume when the live session is marked deleted', async () => {
    const { harness, agent } = setup();
    agent.enqueueRun({
      finishReason: 'suspended',
      runId: 'run-Q',
      suspendPayload: {
        toolCallId: 'tc-Q',
        toolName: 'ask_user',
        args: { question: 'pick' },
      },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await session.message({ content: 'go' });
    const responseIdentity = pendingResponseIdentity(session.getRecord().pendingResume!);

    let releaseFullOutput!: () => void;
    const fullOutput = new Promise<unknown>(resolve => {
      releaseFullOutput = () => resolve({});
    });
    agent.resumeStream = async (resumeData: any, options?: any) => {
      agent.resumeCalls.push({ resumeData, options });
      return { getFullOutput: () => fullOutput };
    };
    const response = session.respondToQuestion({ ...responseIdentity, responseId: 'answer-1', answer: 'red' });
    await waitFor(() => session.isRunning(), 'active resume turn start');
    const idle = session.waitForIdle();

    (session as any)._markDeleted();

    expect(session.isRunning()).toBe(false);
    try {
      await expect(response).rejects.toBeInstanceOf(HarnessSessionDeletedError);
      await expect(idle).rejects.toBeInstanceOf(HarnessSessionDeletedError);
      await expect(session.setState({ afterDelete: true })).rejects.toBeInstanceOf(HarnessSessionDeletedError);
    } finally {
      releaseFullOutput();
    }
  });

  it('respondToQuestion returns duplicate receipt without resuming twice', async () => {
    const { harness, agent } = setup();
    agent.enqueueRun({
      finishReason: 'suspended',
      runId: 'run-Q',
      suspendPayload: {
        toolCallId: 'tc-Q',
        toolName: 'ask_user',
        args: { question: 'pick' },
      },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await session.message({ content: 'go' });
    const responseIdentity = pendingResponseIdentity(session.getRecord().pendingResume!);

    agent.enqueueRun({ finishReason: 'stop', runId: 'run-Q' });
    const first = await session.respondToQuestion({ ...responseIdentity, responseId: 'answer-1', answer: 'red' });
    const duplicate = await session.respondToQuestion({
      ...responseIdentity,
      responseId: 'answer-1',
      answer: 'red',
    });

    expect(first).toMatchObject({ status: 'applied', duplicate: false, responseId: 'answer-1' });
    expect(duplicate).toMatchObject({ status: 'applied', duplicate: true, responseId: 'answer-1' });
    expect(agent.resumeCalls).toHaveLength(1);
  });

  it('respondToQuestion serializes concurrent duplicate responseIds before resuming', async () => {
    const { harness, agent } = setup();
    agent.enqueueRun({
      finishReason: 'suspended',
      runId: 'run-Q',
      suspendPayload: {
        toolCallId: 'tc-Q',
        toolName: 'ask_user',
        args: { question: 'pick' },
      },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await session.message({ content: 'go' });
    const responseIdentity = pendingResponseIdentity(session.getRecord().pendingResume!);

    let release!: () => void;
    const holdUntil = new Promise<void>(resolve => {
      release = resolve;
    });
    agent.enqueueRun({ finishReason: 'stop', runId: 'run-Q', holdUntil });

    const first = session.respondToQuestion({ ...responseIdentity, responseId: 'answer-1', answer: 'red' });
    const second = session.respondToQuestion({ ...responseIdentity, responseId: 'answer-1', answer: 'red' });

    await new Promise(resolve => setImmediate(resolve));
    expect(agent.resumeCalls).toHaveLength(1);

    release();
    await expect(first).resolves.toMatchObject({ status: 'applied', duplicate: false, responseId: 'answer-1' });
    await expect(second).resolves.toMatchObject({ status: 'accepted', duplicate: true, responseId: 'answer-1' });
    expect(agent.resumeCalls).toHaveLength(1);
  });

  it('respondToQuestion rejects concurrent distinct responseIds after one response wins admission', async () => {
    const { harness, agent } = setup();
    agent.enqueueRun({
      finishReason: 'suspended',
      runId: 'run-Q',
      suspendPayload: {
        toolCallId: 'tc-Q',
        toolName: 'ask_user',
        args: { question: 'pick' },
      },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await session.message({ content: 'go' });
    const responseIdentity = pendingResponseIdentity(session.getRecord().pendingResume!);

    let release!: () => void;
    const holdUntil = new Promise<void>(resolve => {
      release = resolve;
    });
    agent.enqueueRun({ finishReason: 'stop', runId: 'run-Q', holdUntil });

    const first = session.respondToQuestion({ ...responseIdentity, responseId: 'answer-1', answer: 'red' });
    const second = session
      .respondToQuestion({ ...responseIdentity, responseId: 'answer-2', answer: 'red' })
      .catch(err => err);

    await new Promise(resolve => setImmediate(resolve));
    await expect(second).resolves.toMatchObject({
      message: expect.stringContaining('pending resume already responded; awaiting agent confirmation'),
    });
    expect(agent.resumeCalls).toHaveLength(1);

    release();
    await expect(first).resolves.toMatchObject({ status: 'applied', duplicate: false, responseId: 'answer-1' });
    expect(session.getRecord().inboxResponseReceipts?.['answer-2']).toBeUndefined();
  });

  it('respondToQuestion returns accepted duplicate receipt while resume is in flight', async () => {
    const { harness, agent } = setup();
    agent.enqueueRun({
      finishReason: 'suspended',
      runId: 'run-Q',
      suspendPayload: {
        toolCallId: 'tc-Q',
        toolName: 'ask_user',
        args: { question: 'pick' },
      },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await session.message({ content: 'go' });
    const responseIdentity = pendingResponseIdentity(session.getRecord().pendingResume!);

    let release!: () => void;
    const holdUntil = new Promise<void>(resolve => {
      release = resolve;
    });
    agent.enqueueRun({ finishReason: 'stop', runId: 'run-Q', holdUntil });

    const first = session.respondToQuestion({ ...responseIdentity, responseId: 'answer-1', answer: 'red' });
    await new Promise(resolve => setImmediate(resolve));

    const duplicate = await session.respondToQuestion({
      ...responseIdentity,
      responseId: 'answer-1',
      answer: 'red',
    });

    expect(duplicate).toMatchObject({ status: 'accepted', duplicate: true, responseId: 'answer-1' });
    expect(agent.resumeCalls).toHaveLength(1);

    release();
    await expect(first).resolves.toMatchObject({ status: 'applied', duplicate: false, responseId: 'answer-1' });
  });

  it('respondToQuestion rejects same responseId with different answer', async () => {
    const { harness, agent } = setup();
    agent.enqueueRun({
      finishReason: 'suspended',
      runId: 'run-Q',
      suspendPayload: {
        toolCallId: 'tc-Q',
        toolName: 'ask_user',
        args: { question: 'pick' },
      },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await session.message({ content: 'go' });
    const responseIdentity = pendingResponseIdentity(session.getRecord().pendingResume!);

    agent.enqueueRun({ finishReason: 'stop', runId: 'run-Q' });
    await session.respondToQuestion({ ...responseIdentity, responseId: 'answer-1', answer: 'red' });

    await expect(
      session.respondToQuestion({ ...responseIdentity, responseId: 'answer-1', answer: 'blue' }),
    ).rejects.toBeInstanceOf(HarnessInboxResponseConflictError);
    expect(agent.resumeCalls).toHaveLength(1);
  });

  it('respondToQuestion returns applied duplicate after the resumed run suspends again', async () => {
    const { harness, agent } = setup();
    agent.enqueueRun({
      finishReason: 'suspended',
      runId: 'run-Q',
      suspendPayload: {
        toolCallId: 'tc-Q',
        toolName: 'ask_user',
        args: { question: 'pick' },
      },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await session.message({ content: 'go' });
    const responseIdentity = pendingResponseIdentity(session.getRecord().pendingResume!);

    agent.enqueueRun({
      finishReason: 'suspended',
      runId: 'run-Q',
      suspendPayload: {
        toolCallId: 'tc-Q2',
        toolName: 'ask_user',
        args: { question: 'pick again' },
      },
    });
    const first = await session.respondToQuestion({ ...responseIdentity, responseId: 'answer-1', answer: 'red' });
    const duplicate = await session.respondToQuestion({
      ...responseIdentity,
      responseId: 'answer-1',
      answer: 'red',
    });

    expect(first).toMatchObject({ status: 'applied', duplicate: false, responseId: 'answer-1' });
    expect(duplicate).toMatchObject({ status: 'applied', duplicate: true, responseId: 'answer-1' });
    expect(session.getRecord().pendingResume).toMatchObject({ itemId: 'question:tc-Q2' });
    expect(agent.resumeCalls).toHaveLength(1);
  });

  it('makes a failed receipt retryable and reuses the same deterministic response id', async () => {
    const { harness, agent } = setup();
    agent.enqueueRun({
      finishReason: 'suspended',
      runId: 'run-Q',
      suspendPayload: {
        toolCallId: 'tc-Q',
        toolName: 'ask_user',
        args: { question: 'pick' },
      },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await session.message({ content: 'go' });
    const responseIdentity = pendingResponseIdentity(session.getRecord().pendingResume!);

    // §13.3f.1: respondTo* is a public §4.2b boundary, so a raw resumeStream
    // failure is REDACTED on the in-process rejection too — generic
    // `harness.internal` message, with the raw "FakeAgent: …" text preserved
    // local-only on `.cause`.
    const firstThrown = await session
      .respondToQuestion({ ...responseIdentity, responseId: 'answer-1', answer: 'red' })
      .then(
        () => undefined,
        (e: unknown) => e,
      );
    expect((firstThrown as Error).name).toBe('HarnessExecutionError');
    expect((firstThrown as Error).message).toBe('An internal harness error occurred');
    expect(((firstThrown as { cause?: Error }).cause as Error).message).toBe(
      'FakeAgent: no run enqueued for resumeStream()',
    );

    // §13.3f.1: the durable failure receipt is a public projection surface — a
    // raw (non-namespaced) agent Error redacts to `harness.internal` with a
    // generic message; the raw text must not be persisted onto the receipt.
    expect(session.getRecord().inboxResponseReceipts?.['answer-1']).toMatchObject({
      itemId: 'question:tc-Q',
      status: 'failed',
      retryable: true,
      error: { code: 'harness.internal', message: 'An internal harness error occurred' },
    });
    expect(session.getRecord().pendingResume?.resumedAt).toBeUndefined();

    agent.enqueueRun({ finishReason: 'stop', runId: 'run-Q', text: 'recovered' });
    await expect(
      session.respondToQuestion({ ...responseIdentity, responseId: 'answer-1', answer: 'red' }),
    ).resolves.toMatchObject({ status: 'applied', duplicate: false });
    expect(agent.resumeCalls).toHaveLength(2);
  });

  it('reverts the at-least-once marker to a retryable waiting state when an in-process resume throws (F2)', async () => {
    const { harness, agent } = setup();
    agent.enqueueRun({
      finishReason: 'suspended',
      runId: 'run-RT',
      suspendPayload: { toolCallId: 'tc-RT', toolName: 'shell', args: { cmd: 'ls' } },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await session.message({ content: 'go' });
    expect(session.getRecord().pendingResume).toBeDefined();

    // First resume: no run enqueued → resumeStream throws (in-process failure).
    // The marker (`resumedAt`) was stamped under the lease before the agent
    // call; the in-process catch must revert it so the suspension is
    // immediately retryable instead of stuck "resuming".
    await expect(session.respondToToolApproval({ approved: true })).rejects.toBeTruthy();
    expect(agent.resumeCalls).toHaveLength(1);

    // No `resumedAt` orphan; suspension is back to `waiting`, not `resuming`.
    const pending = session.getRecord().pendingResume;
    expect(pending).toBeDefined();
    expect(pending!.resumedAt).toBeUndefined();
    // §5.1b currentRun projection: a suspended run with the marker cleared
    // reads `waiting`, not `resuming`.
    expect(session.getDisplayState().currentRun?.status).toBe('waiting');

    // An immediate retry is allowed (does NOT hit "awaiting agent confirmation")
    // and re-invokes the agent. This time the run succeeds and clears pending.
    agent.enqueueRun({ finishReason: 'stop', runId: 'run-RT', text: 'done' });
    const result = await session.respondToToolApproval({ approved: true });
    expect(result.finishReason).toBe('stop');
    expect(agent.resumeCalls).toHaveLength(2);
    expect(session.getRecord().pendingResume).toBeUndefined();
  });

  it('a failed in-process resume does not permanently park the queue (F2 liveness)', async () => {
    const { harness, agent } = setup();
    // Queue two turns: the first suspends; the second waits behind the
    // suspension. A failed resume of the first must not park the drain forever.
    agent.enqueueRun({
      finishReason: 'suspended',
      runId: 'run-Q1',
      suspendPayload: { toolCallId: 'tc-Q1', toolName: 'shell', args: { cmd: 'ls' } },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });

    const first = session.queue({ content: 'first' });
    void first.catch(() => {});
    // Yield so the drain starts the first item and it parks on the suspension.
    await waitFor(() => session.getRecord().pendingResume !== undefined, 'first queued turn suspended');
    const second = session.queue({ content: 'second' });
    void second.catch(() => {});

    // Resume fails in-process (no run enqueued for resumeStream).
    await expect(session.respondToToolApproval({ approved: true })).rejects.toBeTruthy();

    // Marker reverted → suspension retryable, queue not permanently parked.
    expect(session.getRecord().pendingResume?.resumedAt).toBeUndefined();

    // Retry succeeds: the first queued turn completes, then the drain advances
    // to the second item (proves the queue self-healed, not stuck behind a
    // stale `resumedAt`).
    agent.enqueueRun({ finishReason: 'stop', runId: 'run-Q1', text: 'first done' });
    agent.enqueueRun({ finishReason: 'stop', runId: 'run-Q2', text: 'second done' });
    await session.respondToToolApproval({ approved: true });
    await first;

    const secondResult = await second;
    expect(secondResult.finishReason).toBe('stop');
    expect(session.getRecord().pendingResume).toBeUndefined();
    expect(session.getQueueDepth()).toBe(0);
  });

  it('a failed resume followed by close shuts down cleanly with no stuck in-flight turn (F4)', async () => {
    // A parked suspension is durable state, not process-local work. Closing
    // must not wait for its HITL deadline merely because no user can answer it
    // while the session is closing.
    const agent = new FakeAgent();
    const storage = new InMemoryHarness({ db: new InMemoryDB() });
    const harness = new Harness({
      agents: { default: agent } as any,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage },
    });
    agent.enqueueRun({
      finishReason: 'suspended',
      runId: 'run-C',
      suspendPayload: { toolCallId: 'tc-C', toolName: 'shell', args: { cmd: 'ls' } },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await session.message({ content: 'go' });
    expect(session.getRecord().pendingResume).toBeDefined();

    // Resume fails in-process (no run enqueued for resumeStream). The marker is
    // reverted in-process (the session is still live here), so the original
    // resume error surfaces and the turn ends without leaving an in-flight run.
    await expect(session.respondToToolApproval({ approved: true })).rejects.toBeTruthy();
    expect(session.getRecord().pendingResume?.resumedAt).toBeUndefined();
    expect(session.isRunning()).toBe(false);

    // Closing completes promptly without waiting the default 30-second close
    // deadline (the revert never masked the resume error nor wedged shutdown).
    const closeStartedAt = Date.now();
    await expect(session.close()).resolves.toBeUndefined();
    expect(Date.now() - closeStartedAt).toBeLessThan(1_000);
    expect(session.isClosed).toBe(true);
    expect(session.isRunning()).toBe(false);
  });

  it('clears a stale non-queued pending response without replaying resumeStream', async () => {
    const { harness, agent } = setup();
    agent.enqueueRun({
      finishReason: 'suspended',
      runId: 'run-Q',
      suspendPayload: {
        toolCallId: 'tc-Q',
        toolName: 'ask_user',
        args: { question: 'pick' },
      },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await session.message({ content: 'go' });
    const pending = session.getRecord().pendingResume!;
    const staleAt = Date.now() - 60_000;
    const response = { answer: 'red' };
    const responseHash = (session as any)._computeInboxResponseHash({
      kind: 'question',
      itemId: pending.itemId,
      runId: pending.runId,
      pendingRequestedAt: pending.requestedAt,
      response,
    });
    await (session as any)._flushUpdate((prev: any) => ({
      ...prev,
      pendingResume: {
        ...prev.pendingResume,
        resumedAt: staleAt,
        resumeRecoveryAt: staleAt + 30_000,
      },
      inboxResponseReceipts: {
        stale: {
          responseId: 'stale',
          responseHash,
          resumeAttemptId: 'stale',
          itemId: pending.itemId,
          kind: 'question',
          runId: pending.runId,
          toolCallId: pending.toolCallId,
          pendingRequestedAt: pending.requestedAt,
          response,
          status: 'accepted',
          acceptedAt: staleAt,
          updatedAt: staleAt,
        },
      },
    }));

    await expect(
      session.respondToQuestion({ ...pendingResponseIdentity(pending), responseId: 'stale', answer: 'red' }),
    ).rejects.toThrow('resume was admitted but no live owner');

    expect(session.getRecord().pendingResume).toBeUndefined();
    expect(session.getRecord().inboxResponseReceipts?.stale).toMatchObject({ status: 'failed' });

    agent.enqueueRun({ finishReason: 'stop', text: 'fresh answer' });
    await expect(
      session.respondToQuestion({ ...pendingResponseIdentity(pending), responseId: 'fresh', answer: 'red' }),
    ).rejects.toThrow('no pending resume on this session');

    expect(agent.resumeCalls).toHaveLength(0);
  });

  it('clears a stale non-queued pending response on a fresh responseId retry', async () => {
    const { harness, agent } = setup();
    agent.enqueueRun({
      finishReason: 'suspended',
      runId: 'run-Q',
      suspendPayload: {
        toolCallId: 'tc-Q',
        toolName: 'ask_user',
        args: { question: 'pick' },
      },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await session.message({ content: 'go' });
    const pending = session.getRecord().pendingResume!;
    const staleAt = Date.now() - 60_000;
    await (session as any)._flushUpdate((prev: any) => ({
      ...prev,
      pendingResume: {
        ...prev.pendingResume,
        resumedAt: staleAt,
        resumeRecoveryAt: staleAt + 30_000,
      },
    }));

    await expect(
      session.respondToQuestion({ ...pendingResponseIdentity(pending), responseId: 'fresh', answer: 'red' }),
    ).rejects.toThrow('resume was admitted but no live owner');

    expect(session.getRecord().pendingResume).toBeUndefined();
    expect(session.getRecord().inboxResponseReceipts?.fresh).toBeUndefined();
    expect(agent.resumeCalls).toHaveLength(0);
  });

  it('respondToPlanApproval flips active mode atomically when approved + transitionsTo set', async () => {
    const { harness, agent } = setup([
      { id: 'planner', agentId: 'default', transitionsTo: 'builder' },
      { id: 'builder', agentId: 'default' },
    ]);
    agent.enqueueRun({
      finishReason: 'suspended',
      runId: 'run-P',
      suspendPayload: { toolCallId: 'tc-P', toolName: 'submit_plan', args: { title: 't', plan: 'p' } },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await session.switchMode({ mode: 'planner' });
    await session.message({ content: 'plan' });

    expect(session.getCurrentMode().id).toBe('planner');

    agent.enqueueRun({ finishReason: 'stop', runId: 'run-P' });
    await session.respondToPlanApproval({ approved: true });

    expect(session.getCurrentMode().id).toBe('builder');
    expect(session.getRecord().pendingResume).toBeUndefined();
  });

  it('respondToPlanApproval does NOT flip mode when rejected', async () => {
    const { harness, agent } = setup([
      { id: 'planner', agentId: 'default', transitionsTo: 'builder' },
      { id: 'builder', agentId: 'default' },
    ]);
    agent.enqueueRun({
      finishReason: 'suspended',
      runId: 'run-P',
      suspendPayload: { toolCallId: 'tc-P', toolName: 'submit_plan', args: { title: 't', plan: 'p' } },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await session.switchMode({ mode: 'planner' });
    await session.message({ content: 'plan' });

    agent.enqueueRun({ finishReason: 'stop', runId: 'run-P' });
    await session.respondToPlanApproval({ approved: false });

    expect(session.getCurrentMode().id).toBe('planner');
  });

  it('captures a follow-on suspend produced by the resumed run', async () => {
    const { harness, agent } = setup();
    agent.enqueueRun({
      finishReason: 'suspended',
      runId: 'run-1',
      suspendPayload: { toolCallId: 'tc-1', toolName: 'shell', args: {} },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await session.message({ content: 'go' });

    // Resume returns ANOTHER suspend.
    agent.enqueueRun({
      finishReason: 'suspended',
      runId: 'run-1',
      suspendPayload: { toolCallId: 'tc-2', toolName: 'shell', args: {} },
    });
    const result = await session.respondToToolApproval({ approved: true });

    expect(result.finishReason).toBe('suspended');
    const pending = session.getRecord().pendingResume!;
    expect(pending.toolCallId).toBe('tc-2');
    expect(pending.kind).toBe('tool-approval');

    // Second respond resolves cleanly.
    agent.enqueueRun({ finishReason: 'stop', runId: 'run-1' });
    await session.respondToToolApproval({ approved: true });
    expect(session.getRecord().pendingResume).toBeUndefined();
    expect(agent.resumeCalls).toHaveLength(2);
  });

  it('accounts cumulative usage once across re-suspend and process restart', async () => {
    const { harness, agent, storage } = setup();
    agent.enqueueRun({
      finishReason: 'suspended',
      runId: 'run-usage',
      totalUsage: { inputTokens: 100, outputTokens: 20, totalTokens: 120 },
      suspendPayload: { toolCallId: 'tc-usage-1', toolName: 'shell', args: {} },
    });
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await session.message({ content: 'go' });

    expect(session.getTokenUsage()).toEqual({ promptTokens: 100, completionTokens: 20, totalTokens: 120 });
    expect(session.getRecord().pendingResume?.accountedTokenUsage).toEqual({
      promptTokens: 100,
      completionTokens: 20,
      totalTokens: 120,
    });

    agent.enqueueRun({
      finishReason: 'suspended',
      runId: 'run-usage',
      totalUsage: { inputTokens: 150, outputTokens: 35, totalTokens: 185 },
      suspendPayload: { toolCallId: 'tc-usage-2', toolName: 'shell', args: {} },
    });
    await session.respondToToolApproval({ approved: true });

    expect(session.getTokenUsage()).toEqual({ promptTokens: 150, completionTokens: 35, totalTokens: 185 });
    expect(session.getRecord().pendingResume?.accountedTokenUsage).toEqual({
      promptTokens: 150,
      completionTokens: 35,
      totalTokens: 185,
    });

    const sessionId = session.id;
    await harness.shutdown();
    const restartedAgent = new FakeAgent();
    const restartedHarness = new Harness({
      agents: { default: restartedAgent } as any,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage },
    });
    const restarted = await restartedHarness.session({ sessionId, resourceId: 'u' });
    restartedAgent.enqueueRun({
      finishReason: 'stop',
      runId: 'run-usage',
      totalUsage: { inputTokens: 180, outputTokens: 50, totalTokens: 230 },
    });

    await restarted.respondToToolApproval({ approved: true });

    expect(restarted.getTokenUsage()).toEqual({ promptTokens: 180, completionTokens: 50, totalTokens: 230 });
    expect(restarted.getRecord().pendingResume).toBeUndefined();
    await restartedHarness.shutdown();
  });
});

// ---------------------------------------------------------------------------
// Resume — rejection cases
// ---------------------------------------------------------------------------

// One descriptor per responder so the four rejection paths can be
// exercised uniformly by every public entry point.
type Kind = 'tool-approval' | 'tool-suspension' | 'question' | 'plan-approval';

interface Responder {
  /** Kind that, when pending, this responder is the *correct* one to call. */
  kind: Kind;
  /** Suspend chunk that produces a pendingResume of this kind. */
  enqueueSuspend: (agent: FakeAgent) => void;
  /** Call the public method on Session with kind-shaped resumeData. */
  call: (s: any) => Promise<unknown>;
  label: string;
}

const responders: Responder[] = [
  {
    kind: 'tool-approval',
    label: 'respondToToolApproval',
    enqueueSuspend: agent =>
      agent.enqueueRun({
        finishReason: 'suspended',
        runId: 'run-A',
        suspendPayload: { toolCallId: 'tc', toolName: 'shell', args: {} },
      }),
    call: s => s.respondToToolApproval({ approved: true }),
  },
  {
    kind: 'tool-suspension',
    label: 'respondToToolSuspension',
    enqueueSuspend: agent =>
      agent.enqueueRun({
        finishReason: 'suspended',
        runId: 'run-S',
        suspendPayload: {
          toolCallId: 'tc',
          toolName: 'long',
          args: {},
          suspendPayload: { step: 'A' },
        },
      }),
    call: s => s.respondToToolSuspension({ resumeData: { ok: true } }),
  },
  {
    kind: 'question',
    label: 'respondToQuestion',
    enqueueSuspend: agent =>
      agent.enqueueRun({
        finishReason: 'suspended',
        runId: 'run-Q',
        suspendPayload: { toolCallId: 'tc', toolName: 'ask_user', args: { question: 'pick' } },
      }),
    call: s => s.respondToQuestion({ answer: 'red' }),
  },
  {
    kind: 'plan-approval',
    label: 'respondToPlanApproval',
    enqueueSuspend: agent =>
      agent.enqueueRun({
        finishReason: 'suspended',
        runId: 'run-P',
        suspendPayload: { toolCallId: 'tc', toolName: 'submit_plan', args: { title: 't', plan: 'p' } },
      }),
    call: s => s.respondToPlanApproval({ approved: true }),
  },
];

describe('Session — respond* rejection paths (all responders)', () => {
  it.each(responders)('$label rejects when nothing is pending', async ({ call }) => {
    const { harness } = setup();
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });

    await expect(call(session)).rejects.toBeInstanceOf(HarnessValidationError);
  });

  // Build every (pending-kind, responder) pair where pending != responder.kind
  // so wrong-kind is exercised on all four responders.
  const wrongKindMatrix = responders.flatMap(responder =>
    responders.filter(p => p.kind !== responder.kind).map(pending => ({ responder, pending })),
  );

  it.each(wrongKindMatrix)(
    '$responder.label rejects when pending kind is "$pending.kind"',
    async ({ responder, pending }) => {
      const { harness, agent } = setup();
      pending.enqueueSuspend(agent);
      const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
      await session.message({ content: 'go' });

      await expect(responder.call(session)).rejects.toBeInstanceOf(HarnessValidationError);
      // Pending record is left untouched so the correct responder can still resolve it.
      expect(session.getRecord().pendingResume!.kind).toBe(pending.kind);
      expect(agent.resumeCalls).toHaveLength(0);
    },
  );

  it.each(responders)(
    '$label rejects when pendingResume.resumedAt is already set (in-flight)',
    async ({ enqueueSuspend, call }) => {
      const { harness, agent, storage } = setup();
      enqueueSuspend(agent);
      const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
      await session.message({ content: 'go' });

      // Simulate a crash mid-resume by poking resumedAt directly into storage.
      const rec = (await storage.loadSession({ sessionId: session.id }))!;
      const resumedAt = Date.now();
      await storage.saveSession(
        {
          ...rec,
          pendingResume: {
            ...rec.pendingResume!,
            resumedAt,
            resumeRecoveryAt: resumedAt + 30_000,
          },
        },
        { ownerId: session._internalOwnerId, ifVersion: rec.version },
      );
      // Re-hydrate so the next call sees the persisted marker.
      await harness.shutdown();
      const harness2 = new Harness({
        agents: { default: agent } as any,
        modes: [{ id: 'default', agentId: 'default' }],
        defaultModeId: 'default',
        sessions: { storage },
      });
      const session2 = await harness2.session({ sessionId: session.id, resourceId: 'u' });

      await expect(call(session2)).rejects.toBeInstanceOf(HarnessValidationError);
      expect(agent.resumeCalls).toHaveLength(0);
    },
  );

  it.each(responders)('$label rejects on a closed session', async ({ enqueueSuspend, call }) => {
    const { harness, agent } = setup();
    enqueueSuspend(agent);
    const session = await harness.session({ resourceId: 'u', threadId: { fresh: true } });
    await session.message({ content: 'go' });
    const closeStartedAt = Date.now();
    await session.close();
    expect(Date.now() - closeStartedAt).toBeLessThan(1_000);

    await expect(call(session)).rejects.toThrow(/closed/);
  });
});
