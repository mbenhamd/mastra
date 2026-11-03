/**
 * PF-7473 — the model-facing `subagent` control tool (status / wait / send /
 * cancel) over a parent session's own children.
 *
 * Children are started the way a model starts them (`task_delegate`), and the
 * control tool is driven through its public `execute` seam. The cross-process
 * case uses two `Harness` instances over ONE `InMemoryDB`: the second instance
 * holds the child's lease, so the parent's process can only record a durable
 * cancel intent on the child row.
 */

import { describe, expect, it, vi } from 'vitest';

import { Agent } from '../../agent';
import { convertArrayToReadableStream, MockLanguageModelV2 } from '../../agent/__tests__/mock-model';
import { InMemoryHarness } from '../../storage/domains/harness/inmemory';
import { InMemoryDB } from '../../storage/domains/inmemory-db';

import { MockAgent } from './__test-utils__';
import type { HarnessEvent } from './events';
import { Harness } from './harness';
import { createPlanTaskTools, TASK_ADD_TOOL_ID, TASK_DELEGATE_TOOL_ID } from './plan-task-tool';
import type { Session } from './session';
import { createSubagentControlTool, SUBAGENT_CONTROL_TOOL_ID } from './subagent-control-tool';
import {
  HARNESS_SUBAGENT_OUTCOME_REPORT_KIND,
  HARNESS_SUBAGENT_OUTCOME_REPORT_TOOL_ID,
} from './terminal-subagent-result';

function completedOutcome(summary: string) {
  return {
    status: 'success' as const,
    items: [
      {
        toolName: HARNESS_SUBAGENT_OUTCOME_REPORT_TOOL_ID,
        toolCallId: 'report-call-1',
        status: 'success' as const,
        value: {
          kind: HARNESS_SUBAGENT_OUTCOME_REPORT_KIND,
          outcome: 'completed',
          summary,
          evidence: [{ kind: 'analysis', description: 'Verified by the control-tool fixture' }],
        },
      },
    ],
  };
}

function subagentsConfig() {
  return {
    maxDepth: 2,
    types: {
      worker: {
        agentId: 'child-agent',
        modeId: 'worker-mode',
        description: 'Background worker subagent',
        defaultModelId: 'openai/gpt-4o-mini',
        workspace: 'inherit' as const,
      },
    },
  };
}

function buildHarness(db: InMemoryDB) {
  const parentAgent = new MockAgent({ id: 'parent-agent' });
  const childAgent = new MockAgent({
    id: 'child-agent',
    defaultOutput: { terminalToolResult: completedOutcome('Worker finished the survey') },
  });
  const harness = new Harness({
    agents: { 'parent-agent': parentAgent, 'child-agent': childAgent } as any,
    modes: [
      { id: 'default', agentId: 'parent-agent' },
      { id: 'worker-mode', agentId: 'child-agent' },
    ],
    defaultModeId: 'default',
    sessions: { storage: new InMemoryHarness({ db }) },
    subagents: subagentsConfig(),
  });
  return { harness, parentAgent, childAgent };
}

const toolCtx = {
  abortSignal: new AbortController().signal,
  agent: { toolCallId: 'tc-control', runId: 'mock-run' },
  requestContext: { get: () => undefined },
} as any;

async function poll(pred: () => boolean | Promise<boolean>, timeoutMs = 2000): Promise<void> {
  const start = Date.now();
  for (;;) {
    if (await pred()) return;
    if (Date.now() - start > timeoutMs) throw new Error('poll timeout');
    await new Promise(r => setTimeout(r, 5));
  }
}

async function openParentTurn(parent: Session, agent: MockAgent): Promise<() => void> {
  let release!: () => void;
  agent.enqueueRun({ holdUntil: new Promise<void>(r => (release = r)), finishReason: 'stop', text: 'done' });
  const turn = parent.message({ content: 'go' });
  await poll(() => agent.streamCalls.length > 0);
  return () => {
    release();
    void turn.catch(() => {});
  };
}

/** Delegate one plan task; the child's run is held until the returned release fires. */
async function delegateHeldChild(parent: Session, childAgent: MockAgent, content: string) {
  let releaseChild!: () => void;
  childAgent.enqueueRun({
    holdUntil: new Promise<void>(r => (releaseChild = r)),
    finishReason: 'stop',
    terminalToolResult: completedOutcome(`${content}: done`),
  });
  const tools = createPlanTaskTools(parent);
  const task = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content } as any, toolCtx)) as any;
  const delegated = (await tools[TASK_DELEGATE_TOOL_ID]!.execute!(
    { taskId: task.taskId, agentType: 'worker' } as any,
    toolCtx,
  )) as any;
  await poll(() => childAgent.streamCalls.length > 0);
  return { taskId: task.taskId as string, childId: delegated.subagentSessionId as string, releaseChild };
}

function control(parent: Session) {
  const tool = createSubagentControlTool(parent);
  if (!tool) throw new Error('subagent control tool not registered');
  return (input: Record<string, unknown>) => tool.execute!(input as any, toolCtx) as Promise<any>;
}

describe('subagent control tool', () => {
  it('status and wait report a delegated child running, then its settled result', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const closeTurn = await openParentTurn(parent, parentAgent);
    const { taskId, childId, releaseChild } = await delegateHeldChild(parent, childAgent, 'survey the corpus');
    const run = control(parent);

    const listed = await run({ action: 'status' });
    expect(listed.subagents).toEqual([
      expect.objectContaining({
        subagentSessionId: childId,
        source: 'delegated',
        status: 'running',
        settled: false,
        taskId,
        agentType: 'worker',
      }),
    ]);

    const waiting = run({ action: 'wait', subagentSessionId: childId, timeoutMs: 5_000 });
    releaseChild();
    const waited = await waiting;
    expect(waited.timedOut).toBe(false);
    expect(waited.runningSubagents).toEqual([]);
    expect(waited.settledSubagents).toEqual([
      expect.objectContaining({
        subagentSessionId: childId,
        status: 'completed',
        settled: true,
        result: expect.objectContaining({ text: 'survey the corpus: done' }),
      }),
    ]);

    closeTurn();
    await harness.shutdown();
  });

  it('wait returns the still-running child when the timeout passes', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const closeTurn = await openParentTurn(parent, parentAgent);
    const { childId, releaseChild } = await delegateHeldChild(parent, childAgent, 'long job');

    const started = Date.now();
    const waited = await control(parent)({ action: 'wait', timeoutMs: 150 });
    expect(Date.now() - started).toBeGreaterThanOrEqual(140);
    expect(waited.timedOut).toBe(true);
    expect(waited.settledSubagents).toEqual([]);
    expect(waited.runningSubagents).toEqual([
      expect.objectContaining({ subagentSessionId: childId, status: 'running' }),
    ]);

    releaseChild();
    closeTurn();
    await harness.shutdown();
  });

  it('send interleaves a system reminder into the running child run', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const closeTurn = await openParentTurn(parent, parentAgent);
    const { childId, releaseChild } = await delegateHeldChild(parent, childAgent, 'draft the summary');
    await poll(() => harness._internalGetLiveSession(childId)?.isRunning() === true);
    const sendSignal = vi.spyOn(childAgent, 'sendSignal');

    const sent = await control(parent)({
      action: 'send',
      subagentSessionId: childId,
      message: 'Limit the summary to trials after 2020.',
    });

    expect(sent).toMatchObject({ action: 'send', subagentSessionId: childId, delivered: true });
    expect(sendSignal).toHaveBeenCalledWith(
      expect.objectContaining({
        type: 'system-reminder',
        contents: 'Limit the summary to trials after 2020.',
        attributes: { source: 'parent_session', parentSessionId: parent.id },
      }),
      expect.anything(),
    );

    releaseChild();
    closeTurn();
    await harness.shutdown();
  });

  it('cancel acknowledges a live child, then the delegation settles failed; a second cancel is a no-op', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const closeTurn = await openParentTurn(parent, parentAgent);
    const { childId, releaseChild } = await delegateHeldChild(parent, childAgent, 'cancel me');
    const run = control(parent);

    const cancelled = await run({ action: 'cancel', subagentSessionId: childId });
    expect(cancelled).toMatchObject({ action: 'cancel', subagentSessionId: childId, cancel: 'acknowledged' });

    releaseChild();
    const waited = await run({ action: 'wait', subagentSessionId: childId, timeoutMs: 5_000 });
    expect(waited.settledSubagents).toEqual([
      expect.objectContaining({ subagentSessionId: childId, status: 'failed', cancelRequested: true }),
    ]);

    const again = await run({ action: 'cancel', subagentSessionId: childId });
    expect(again).toMatchObject({ cancel: 'already_settled', settled: true, status: 'failed' });

    closeTurn();
    await harness.shutdown();
  });

  it('cancel after a child settled is a no-op that records no cancel request', async () => {
    const { harness, parentAgent, childAgent } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const closeTurn = await openParentTurn(parent, parentAgent);
    const { childId, releaseChild } = await delegateHeldChild(parent, childAgent, 'quick job');
    releaseChild();
    const run = control(parent);
    await run({ action: 'wait', subagentSessionId: childId, timeoutMs: 5_000 });

    const cancelled = await run({ action: 'cancel', subagentSessionId: childId });

    expect(cancelled).toMatchObject({ cancel: 'already_settled', settled: true, status: 'completed' });
    const stored = await parent._internalStorage.loadSession({ sessionId: childId });
    expect(stored?.cancelRequest).toBeUndefined();

    closeTurn();
    await harness.shutdown();
  });

  it('rejects a session that is not its own child and leaves it untouched', async () => {
    const { harness } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const other = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const foreignChild = await harness.session({
      resourceId: 'u1',
      threadId: { fresh: true },
      parentSessionId: other.id,
      origin: 'subagent-tool',
      subagentDepth: 1,
    });
    const run = control(parent);

    for (const input of [
      { action: 'status', subagentSessionId: foreignChild.id },
      { action: 'wait', subagentSessionId: foreignChild.id, timeoutMs: 0 },
      { action: 'send', subagentSessionId: foreignChild.id, message: 'stop' },
      { action: 'cancel', subagentSessionId: foreignChild.id },
      { action: 'cancel', subagentSessionId: 'no-such-session' },
    ]) {
      expect(await run(input)).toMatchObject({ isError: true, errorName: 'HarnessValidationError' });
    }
    expect(foreignChild.getRecord().cancelRequest).toBeUndefined();

    await harness.shutdown();
  });

  it('cancel of a child leased by another process records a durable intent that the child honours', async () => {
    const db = new InMemoryDB();
    const a = buildHarness(db);
    const b = buildHarness(db);
    const parent = await a.harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    // Process B holds the child's lease.
    const childInB = await b.harness.session({
      resourceId: 'u1',
      threadId: { fresh: true },
      parentSessionId: parent.id,
      origin: 'subagent-tool',
      modeId: 'worker-mode',
      subagentDepth: 1,
      subagentTypeId: 'worker',
    });
    const leaseOwner = (await parent._internalStorage.loadSession({ sessionId: childInB.id }))?.ownerId;

    const cancelled = await control(parent)({ action: 'cancel', subagentSessionId: childInB.id });

    expect(cancelled).toMatchObject({ cancel: 'intent_recorded', settled: false });
    const stored = await parent._internalStorage.loadSession({ sessionId: childInB.id });
    expect(stored?.cancelRequest).toMatchObject({ reason: 'subagent_cancelled', requestedBy: parent.id });
    expect(stored?.ownerId).toBe(leaseOwner);
    expect(await control(parent)({ action: 'cancel', subagentSessionId: childInB.id })).toMatchObject({
      cancel: 'already_requested',
    });

    // Once B lets go, whichever process hydrates the child refuses new work.
    await b.harness.shutdown();
    const c = buildHarness(db);
    const childInC = await c.harness.session({ sessionId: childInB.id, resourceId: 'u1' });
    await expect(childInC.message({ content: 'keep going' })).rejects.toMatchObject({
      name: 'HarnessSessionCancelledError',
    });

    await c.harness.shutdown();
    await a.harness.shutdown();
  });

  it('is absent without subagent types and for a session already at maxDepth', async () => {
    const plain = new Harness({
      agents: { default: new MockAgent({ id: 'default' }) } as any,
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage: new InMemoryHarness({ db: new InMemoryDB() }) },
    });
    const root = await plain.session({ resourceId: 'u1', threadId: { fresh: true } });
    expect(createSubagentControlTool(root)).toBeUndefined();
    await plain.shutdown();

    const { harness } = buildHarness(new InMemoryDB());
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const deepest = await harness.session({
      resourceId: 'u1',
      threadId: { fresh: true },
      parentSessionId: parent.id,
      origin: 'subagent-tool',
      subagentDepth: 2,
    });
    expect(createSubagentControlTool(parent)).toBeDefined();
    expect(createSubagentControlTool(deepest)).toBeUndefined();
    await harness.shutdown();
  });
});

describe('subagent control tool — permission gate (real agent loop)', () => {
  it('a host deny removes the tool before the model sees it, so it never runs', async () => {
    const testUsage = { inputTokens: 1, outputTokens: 1, totalTokens: 2 };
    let call = 0;
    const model = new MockLanguageModelV2({
      doStream: async () => {
        call++;
        const parts =
          call === 1
            ? [
                {
                  type: 'tool-call',
                  toolCallId: 'call-1',
                  toolName: SUBAGENT_CONTROL_TOOL_ID,
                  input: '{"action":"status"}',
                },
                { type: 'finish', finishReason: 'tool-calls', usage: testUsage },
              ]
            : [
                { type: 'text-start', id: 't1' },
                { type: 'text-delta', id: 't1', delta: 'done' },
                { type: 'text-end', id: 't1' },
                { type: 'finish', finishReason: 'stop', usage: testUsage },
              ];
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: convertArrayToReadableStream([{ type: 'stream-start', warnings: [] }, ...parts] as any),
        };
      },
    });
    const parentAgent = new Agent({ id: 'parent-agent', name: 'parent-agent', instructions: 'control', model });
    const harness = new Harness({
      agents: { 'parent-agent': parentAgent, 'child-agent': new MockAgent({ id: 'child-agent' }) } as any,
      modes: [
        {
          id: 'default',
          agentId: 'parent-agent',
          permissions: { categories: {}, tools: { [SUBAGENT_CONTROL_TOOL_ID]: 'deny' } },
        },
        { id: 'worker-mode', agentId: 'child-agent' },
      ],
      defaultModeId: 'default',
      sessions: { storage: new InMemoryHarness({ db: new InMemoryDB() }) },
      subagents: subagentsConfig(),
    });
    try {
      const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const events: HarnessEvent[] = [];
      session.subscribe(event => events.push(event));

      const result = (await session.message({ content: 'check my subagents' })) as any;

      expect(result.text).toContain('done');
      expect(
        events.some(
          event =>
            event.type === 'tool_denied' &&
            (event as any).stage === 'pre-exposure' &&
            (event as any).toolName === SUBAGENT_CONTROL_TOOL_ID,
        ),
      ).toBe(true);
      // The loop may still report the model's call, but the control tool never
      // ran: no tool result carries its status payload.
      const toolOutputs = events.filter(event => event.type === 'tool_end').map(event => (event as any).output);
      expect(toolOutputs.some(output => (output as any)?.action === 'status')).toBe(false);
    } finally {
      await harness.shutdown();
    }
  });
});
