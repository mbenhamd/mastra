/**
 * Harness v1 — spawn_subagent built-in tool (§9).
 *
 * The tool is auto-registered when `HarnessConfig.subagents.types` is
 * non-empty. Invoking it should:
 *
 *   1. validate `agentType` against the registry,
 *   2. enforce the depth cap (`HarnessSubagentDepthExceededError`),
 *   3. create a fresh child session with `origin: 'subagent-tool'` and
 *      `parentSessionId` wired to the caller,
 *   4. bridge the child's per-turn events into the parent's subscriber
 *      stream as `subagent_*` shapes,
 *   5. track the child in `_activeSubagents` while running, and
 *   6. close the child + drop the entry once the tool returns.
 */

import { describe, expect, it } from 'vitest';

import { Agent } from '../../agent';
import { InMemoryHarness } from '../../storage/domains/harness/inmemory';
import { InMemoryDB } from '../../storage/domains/inmemory-db';
import { buildFakeOutput } from './__test-utils__/fake-output';

import type { HarnessEvent } from './events';
import { Harness } from './harness';
import { createSpawnSubagentTool, SPAWN_SUBAGENT_TOOL_ID } from './spawn-subagent-tool';

class FakeAgent extends Agent<any, any, any> {
  chunks: any[] = [];
  fullOutput: any = {
    text: 'child-result',
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

function setup(opts?: { maxDepth?: number; maxConcurrent?: number; chunks?: any[] }) {
  const parentAgent = new FakeAgent('parent-agent');
  const childAgent = new FakeAgent('child-agent');
  if (opts?.chunks) childAgent.chunks = opts.chunks;
  const storage = new InMemoryHarness({ db: new InMemoryDB() });
  const harness = new Harness({
    agents: { 'parent-agent': parentAgent, 'child-agent': childAgent } as any,
    modes: [
      { id: 'default', agentId: 'parent-agent' },
      { id: 'explore-mode', agentId: 'child-agent' },
    ],
    defaultModeId: 'default',
    sessions: { storage },
    subagents: {
      maxDepth: opts?.maxDepth ?? 2,
      ...(opts?.maxConcurrent !== undefined ? { maxConcurrent: opts.maxConcurrent } : {}),
      types: {
        explore: {
          agentId: 'child-agent',
          modeId: 'explore-mode',
          description: 'Read-only codebase exploration',
          defaultModelId: 'openai/gpt-4o-mini',
          workspace: 'inherit',
        },
      },
    },
  });
  return { harness, parentAgent, childAgent, storage };
}

// Minimal mock execution context for direct tool.execute() calls.
function execCtx(toolCallId = 'tc-1') {
  return {
    abortSignal: new AbortController().signal,
    agent: { toolCallId, runId: 'run-1' },
    runId: 'run-1',
    tracingContext: {} as any,
    requestContext: { get: () => undefined } as any,
    mastra: undefined,
  } as any;
}

describe('spawn_subagent tool — registration', () => {
  it('is undefined when no subagent types are configured', async () => {
    const storage = new InMemoryHarness({ db: new InMemoryDB() });
    const agent = new FakeAgent('parent-agent');
    const harness = new Harness({
      agents: { 'parent-agent': agent } as any,
      modes: [{ id: 'default', agentId: 'parent-agent' }],
      defaultModeId: 'default',
      sessions: { storage },
    });
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const tool = createSpawnSubagentTool(session);
    expect(tool).toBeUndefined();
  });

  it('is registered with the canonical id when subagent types exist', async () => {
    const { harness } = setup();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const tool = createSpawnSubagentTool(session);
    expect(tool).toBeDefined();
    expect(tool!.id).toBe(SPAWN_SUBAGENT_TOOL_ID);
  });
});

describe('spawn_subagent tool — execution', () => {
  it('rejects unknown agentType via input-schema validation', async () => {
    const { harness } = setup();
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const tool = createSpawnSubagentTool(parent)!;

    const result = (await tool.execute!({ agentType: 'bogus', task: 'nope' } as any, execCtx())) as any;

    // The zod enum on `agentType` rejects unknown values before execute runs.
    // The tool framework returns a ValidationError object with `error: true`.
    expect(result.error).toBe(true);
    expect(typeof result.message).toBe('string');
    expect(result.message).toMatch(/agentType/);
  });

  it('enforces the subagent depth cap', async () => {
    const { harness } = setup({ maxDepth: 1 });
    // Manually mint a session sitting at depth=1 to simulate an already-
    // nested subagent. The next spawn would push to depth=2 > max=1.
    const parent = await harness.session({
      resourceId: 'u1',
      threadId: { fresh: true },
      subagentDepth: 1,
    });
    const tool = createSpawnSubagentTool(parent)!;

    const result = (await tool.execute!({ agentType: 'explore', task: 'go deeper' }, execCtx())) as any;

    expect(result.isError).toBe(true);
    expect(result.errorName).toBe('HarnessSubagentDepthExceededError');
    expect(result.attemptedDepth).toBe(2);
    expect(result.maxDepth).toBe(1);
    expect(parent.subagentDepth).toBe(1);
  });

  it('creates a fresh child session and returns subagentSessionId + result', async () => {
    const { harness, childAgent } = setup();
    childAgent.fullOutput = { ...childAgent.fullOutput, text: 'child says hi' };

    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const tool = createSpawnSubagentTool(parent)!;

    const result = (await tool.execute!({ agentType: 'explore', task: 'find usages of X' }, execCtx())) as any;

    expect(typeof result.subagentSessionId).toBe('string');
    expect(result.subagentSessionId).not.toBe(parent.id);
    expect(result.subagentSessionId.length).toBeGreaterThan(0);
  });

  it('emits subagent_start + subagent_end on the parent session', async () => {
    const { harness } = setup();
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const tool = createSpawnSubagentTool(parent)!;

    const events: HarnessEvent[] = [];
    parent.subscribe(e => {
      events.push(e);
    });

    await tool.execute!({ agentType: 'explore', task: 'find usages of X' }, execCtx('tc-spawn-1'));

    const start = events.find(e => e.type === 'subagent_start');
    const end = events.find(e => e.type === 'subagent_end');
    expect(start).toBeDefined();
    expect(end).toBeDefined();
    expect((start as any).toolCallId).toBe('tc-spawn-1');
    expect((start as any).agentType).toBe('explore');
    expect((start as any).task).toBe('find usages of X');
    expect((start as any).depth).toBe(1);
    expect((end as any).toolCallId).toBe('tc-spawn-1');
    expect((end as any).isError).toBe(false);
    expect(typeof (end as any).durationMs).toBe('number');
  });

  it('clears _activeSubagents after the child completes', async () => {
    const { harness } = setup();
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const tool = createSpawnSubagentTool(parent)!;

    await tool.execute!({ agentType: 'explore', task: 'go' }, execCtx('tc-active-1'));

    // After completion, the active map should be empty.
    const display = parent.getDisplayState();
    expect(display.activeSubagents).toEqual({});
  });

  it('child session is auto-closed after the tool returns', async () => {
    const { harness, storage } = setup();
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const tool = createSpawnSubagentTool(parent)!;

    const result = (await tool.execute!({ agentType: 'explore', task: 'go' }, execCtx('tc-close-1'))) as any;

    const childId = result.subagentSessionId as string;
    const childRecord = await storage.loadSession({ sessionId: childId });
    expect(childRecord?.closedAt).toBeDefined();
  });
});

describe('spawn_subagent — mode inheritance (§9)', () => {
  it('a subagent type with NO modeId inherits the PARENT current mode, not the harness default', async () => {
    const parentAgent = new FakeAgent('parent-agent');
    const childAgent = new FakeAgent('child-agent');
    const storage = new InMemoryHarness({ db: new InMemoryDB() });
    const harness = new Harness({
      agents: { 'parent-agent': parentAgent, 'child-agent': childAgent } as any,
      modes: [
        { id: 'default', agentId: 'parent-agent' },
        { id: 'parentMode', agentId: 'parent-agent' },
        { id: 'childMode', agentId: 'child-agent' },
      ],
      defaultModeId: 'default',
      sessions: { storage },
      subagents: {
        maxDepth: 2,
        types: {
          // No modeId → inherits the parent's CURRENT mode (§9).
          inheriter: { agentId: 'child-agent', description: 'inherits parent mode' },
          // Control: an explicit modeId is honored as-is.
          pinned: { agentId: 'child-agent', modeId: 'childMode', description: 'pinned mode' },
        },
      },
    });
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      await parent.switchMode({ mode: 'parentMode' });
      const tool = createSpawnSubagentTool(parent)!;

      const inherited = (await tool.execute!({ agentType: 'inheriter', task: 't' } as any, execCtx('tc-a'))) as any;
      const inheritedRec = await storage.loadSession({ sessionId: inherited.subagentSessionId });
      // Inherits the parent's CURRENT mode ('parentMode'), NOT the harness default ('default').
      expect(inheritedRec!.modeId).toBe('parentMode');

      const pinned = (await tool.execute!({ agentType: 'pinned', task: 't' } as any, execCtx('tc-b'))) as any;
      const pinnedRec = await storage.loadSession({ sessionId: pinned.subagentSessionId });
      expect(pinnedRec!.modeId).toBe('childMode'); // explicit modeId still honored
    } finally {
      await harness.shutdown();
    }
  });
});

describe('subagent type validation (§9)', () => {
  it('rejects a subagent type whose modeId is backed by a DIFFERENT agent than def.agentId', () => {
    expect(
      () =>
        new Harness({
          agents: { a: new FakeAgent('a'), b: new FakeAgent('b') } as any,
          modes: [
            { id: 'default', agentId: 'a' },
            { id: 'b-mode', agentId: 'b' },
          ],
          defaultModeId: 'default',
          sessions: { storage: new InMemoryHarness({ db: new InMemoryDB() }) },
          subagents: {
            maxDepth: 2,
            // agentId 'a' but modeId 'b-mode' runs agent 'b' → must be rejected.
            types: { mismatch: { agentId: 'a', modeId: 'b-mode', description: 'bad' } },
          },
        }),
    ).toThrow(/conflicts with mode/);
  });
});

describe('spawn_subagent tool — concurrency backpressure (§SA3)', () => {
  it('rejects a spawn once maxConcurrent in-flight is reached, then allows after one frees', async () => {
    const { harness } = setup({ maxConcurrent: 1 });
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      const tool = createSpawnSubagentTool(parent)!;
      // One spawn already in flight (the create→run window the counter reserves).
      (parent as any)._subagentSpawnInFlight = 1;
      const rejected = (await tool.execute!({ agentType: 'explore', task: 'x' } as any, execCtx('tc-2'))) as any;
      expect(rejected.isError).toBe(true);
      expect(rejected.errorName).toBe('HarnessSubagentConcurrencyLimitError');
      expect(rejected.maxConcurrent).toBe(1);
      expect(rejected.subagentSessionId).toBe('');

      // Free the slot → a spawn proceeds (child runs to completion) and releases.
      (parent as any)._subagentSpawnInFlight = 0;
      const ok = (await tool.execute!({ agentType: 'explore', task: 'y' } as any, execCtx('tc-3'))) as any;
      expect(ok.isError).toBeFalsy();
      expect(typeof ok.subagentSessionId).toBe('string');
      expect((parent as any)._subagentSpawnInFlight).toBe(0); // reservation released in finally
    } finally {
      await harness.shutdown();
    }
  });

  it('does not gate spawns when maxConcurrent is unset (no per-parent limit)', async () => {
    const { harness } = setup();
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      const tool = createSpawnSubagentTool(parent)!;
      (parent as any)._subagentSpawnInFlight = 99; // would exceed any limit
      const ok = (await tool.execute!({ agentType: 'explore', task: 'z' } as any, execCtx('tc-4'))) as any;
      expect(ok.isError).toBeFalsy();
    } finally {
      await harness.shutdown();
    }
  });
});

describe('spawn_subagent tool — toolAllowlist (§M4)', () => {
  it('wires a definition toolAllowlist onto the child session', async () => {
    const storage = new InMemoryHarness({ db: new InMemoryDB() });
    const harness = new Harness({
      agents: { 'parent-agent': new FakeAgent('parent-agent'), 'child-agent': new FakeAgent('child-agent') } as any,
      modes: [
        { id: 'default', agentId: 'parent-agent' },
        { id: 'scoped-mode', agentId: 'child-agent' },
      ],
      defaultModeId: 'default',
      sessions: { storage },
      subagents: {
        maxDepth: 2,
        types: {
          scoped: {
            agentId: 'child-agent',
            modeId: 'scoped-mode',
            description: 'a scoped subagent',
            toolAllowlist: ['readDoc', 'search'],
          },
        },
      },
    });
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      // Acquire the child the spawn tool will create + observe its allowlist by
      // re-resolving the type the way spawn does (the spawn wiring sets the field
      // from def.toolAllowlist).
      const def = (harness as any)._getSubagentType('scoped');
      expect(def.toolAllowlist).toEqual(['readDoc', 'search']);
      const child = await harness.session({
        resourceId: 'u1',
        threadId: { fresh: true },
        parentSessionId: parent.id,
        origin: 'subagent-tool',
        modeId: 'scoped-mode',
        subagentDepth: 1,
      });
      // Mirror the spawn wiring + assert the resolver hard-denies a non-listed tool.
      (child as any)._subagentToolAllowlist = def.toolAllowlist;
      expect((child as any)._toolPermissionGateEngaged()).toBe(true);
      expect(
        (child as any)._resolveToolPolicy(
          'writeDoc',
          { categories: {}, tools: {} },
          { categories: [], tools: [] },
          'allow',
          new Set(def.toolAllowlist),
        ),
      ).toBe('deny');
      expect(
        (child as any)._resolveToolPolicy(
          'readDoc',
          { categories: {}, tools: {} },
          { categories: [], tools: [] },
          'allow',
          new Set(def.toolAllowlist),
        ),
      ).toBe('allow');
    } finally {
      await harness.shutdown();
    }
  });

  it('rejects a malformed toolAllowlist at construction', () => {
    const base = {
      agents: { a: new FakeAgent('a') } as any,
      modes: [{ id: 'm', agentId: 'a' }],
      defaultModeId: 'm',
      sessions: { storage: new InMemoryHarness({ db: new InMemoryDB() }) },
    };
    for (const bad of [{ toolAllowlist: 'x' as any }, { toolAllowlist: [''] }, { toolAllowlist: ['dup', 'dup'] }]) {
      expect(
        () =>
          new Harness({
            ...base,
            subagents: { types: { t: { agentId: 'a', description: 'd', ...bad } } },
          } as any),
      ).toThrow(/toolAllowlist/);
    }
  });
});

describe('subagent live progress projection (§SA2)', () => {
  it('folds bridged child events into the parent activeSubagents display snapshot', async () => {
    const { harness } = setup();
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      const key = 'tc-sa2';
      (parent as any)._activeSubagents.set(key, {
        subagentSessionId: 'child-1',
        agentType: 'explore',
        task: 'look around',
        parentToolCallId: key,
        startedAt: 1,
      });
      const upd = (e: any) => (parent as any)._internalUpdateSubagentProgress(key, e);

      upd({ type: 'agent_start', runId: 'r' });
      upd({ type: 'tool_start', runId: 'r', toolCallId: 'it1', toolName: 'readDoc', input: {} });
      let snap = parent.getDisplayState().activeSubagents[key]!;
      expect(snap.status).toBe('running');
      expect(snap.currentToolName).toBe('readDoc');
      expect(snap.toolCalls).toBe(1);

      upd({ type: 'tool_end', runId: 'r', toolCallId: 'it1', toolName: 'readDoc', output: {}, isError: false });
      snap = parent.getDisplayState().activeSubagents[key]!;
      expect(snap.currentToolName).toBeUndefined();
      expect(snap.toolCalls).toBe(1);

      upd({
        type: 'agent_end',
        runId: 'r',
        finishReason: 'complete',
        usage: { promptTokens: 2, completionTokens: 3, totalTokens: 5 },
      });
      snap = parent.getDisplayState().activeSubagents[key]!;
      expect(snap.status).toBe('completed');
      expect(snap.usage).toEqual({ promptTokens: 2, completionTokens: 3, totalTokens: 5 });
    } finally {
      await harness.shutdown();
    }
  });

  it('maps an errored/aborted child terminal to failed status', async () => {
    const { harness } = setup();
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      const key = 'tc-sa2b';
      (parent as any)._activeSubagents.set(key, {
        subagentSessionId: 'child-2',
        agentType: 'explore',
        task: 't',
        parentToolCallId: key,
        startedAt: 1,
      });
      (parent as any)._internalUpdateSubagentProgress(key, {
        type: 'agent_end',
        runId: 'r',
        finishReason: 'error',
        usage: { promptTokens: 0, completionTokens: 0, totalTokens: 0 },
      });
      expect(parent.getDisplayState().activeSubagents[key]!.status).toBe('failed');
    } finally {
      await harness.shutdown();
    }
  });

  it('is a no-op once the subagent entry was cleared (child closed)', async () => {
    const { harness } = setup();
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      // No entry for this key — must not throw.
      expect(() =>
        (parent as any)._internalUpdateSubagentProgress('gone', { type: 'agent_start', runId: 'r' }),
      ).not.toThrow();
      expect(parent.getDisplayState().activeSubagents.gone).toBeUndefined();
    } finally {
      await harness.shutdown();
    }
  });
});

describe('spawn_subagent — inline suspension is an error, not a completion (§S3.3)', () => {
  it('reports a HITL-suspended inline subagent as isError + HarnessSubagentSuspendedError', async () => {
    const { harness, childAgent } = setup();
    // Drive the child to a HITL suspension: a default message() RESOLVES with
    // finishReason 'suspended' (it does not reject).
    childAgent.fullOutput = {
      ...childAgent.fullOutput,
      finishReason: 'suspended',
      suspendPayload: { toolCallId: 'c-tc', toolName: 'need_input', args: {} },
    };
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      const events: any[] = [];
      parent.subscribe(e => events.push(e));
      const tool = createSpawnSubagentTool(parent)!;
      const out = (await tool.execute!({ agentType: 'explore', task: 'explore X' } as any, execCtx('tc-susp'))) as any;
      // The inline subagent cannot be resumed → error result, NOT a success.
      expect(out.isError).toBe(true);
      expect((out.result as { errorName?: string })?.errorName).toBe('HarnessSubagentSuspendedError');
      const end = events.find(e => e.type === 'subagent_end') as { isError?: boolean } | undefined;
      expect(end?.isError).toBe(true);
    } finally {
      await harness.shutdown();
    }
  });
});
