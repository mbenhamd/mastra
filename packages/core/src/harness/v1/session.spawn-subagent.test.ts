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
import { RequestContext } from '../../request-context';
import { InMemoryHarness } from '../../storage/domains/harness/inmemory';
import { InMemoryDB } from '../../storage/domains/inmemory-db';
import { buildFakeOutput } from './__test-utils__/fake-output';

import type { HarnessEvent } from './events';
import { Harness } from './harness';
import { createSpawnSubagentTool, SPAWN_SUBAGENT_TOOL_ID } from './spawn-subagent-tool';
import {
  HARNESS_SUBAGENT_OUTCOME_REPORT_KIND,
  HARNESS_SUBAGENT_OUTCOME_REPORT_TOOL_ID,
  MAX_HARNESS_SUBAGENT_EVENT_TEXT_BYTES,
  MAX_HARNESS_SUBAGENT_RESULT_TEXT_BYTES,
} from './terminal-subagent-result';

function outcomeTerminalResult(summary = 'child-result') {
  return {
    status: 'success' as const,
    items: [
      {
        toolName: HARNESS_SUBAGENT_OUTCOME_REPORT_TOOL_ID,
        toolCallId: 'report-subagent-outcome-1',
        status: 'success' as const,
        value: {
          kind: HARNESS_SUBAGENT_OUTCOME_REPORT_KIND,
          outcome: 'completed' as const,
          summary,
          evidence: [{ kind: 'analysis' as const, description: 'Verified by the inline subagent fixture.' }],
        },
      },
    ],
  };
}

class FakeAgent extends Agent<any, any, any> {
  chunks: any[] = [];
  streamCalls = 0;
  streamOptions: any[] = [];
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
    terminalToolResult: outcomeTerminalResult(),
  };

  constructor(name: string) {
    super({ id: name, name, instructions: 'fake', model: 'openai/gpt-4o-mini' as any });
  }

  async stream(_messages: any, options?: any): Promise<any> {
    this.streamCalls += 1;
    this.streamOptions.push(options);
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

function setup(opts?: {
  maxDepth?: number;
  maxConcurrent?: number;
  closeTimeoutMs?: number;
  chunks?: any[];
  allowInline?: boolean;
  inheritRequestContextAppKeys?: string[];
}) {
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
    sessions: { storage, ...(opts?.closeTimeoutMs !== undefined ? { closeTimeoutMs: opts.closeTimeoutMs } : {}) },
    subagents: {
      maxDepth: opts?.maxDepth ?? 2,
      ...(opts?.maxConcurrent !== undefined ? { maxConcurrent: opts.maxConcurrent } : {}),
      ...(opts?.inheritRequestContextAppKeys !== undefined
        ? { inheritRequestContextAppKeys: opts.inheritRequestContextAppKeys }
        : {}),
      types: {
        explore: {
          agentId: 'child-agent',
          modeId: 'explore-mode',
          description: 'Read-only codebase exploration',
          defaultModelId: 'openai/gpt-4o-mini',
          workspace: 'inherit',
          ...(opts?.allowInline !== undefined ? { allowInline: opts.allowInline } : {}),
        },
      },
    },
  });
  return { harness, parentAgent, childAgent, storage };
}

// Minimal mock execution context for direct tool.execute() calls.
function execCtx(toolCallId = 'tc-1', requestContext = new RequestContext()) {
  return {
    abortSignal: new AbortController().signal,
    agent: { toolCallId, runId: 'run-1' },
    runId: 'run-1',
    tracingContext: {} as any,
    requestContext,
    mastra: undefined,
  } as any;
}

describe('spawn_subagent tool — registration', () => {
  it.each([
    ['a non-array value', 'turnCorrelationId', /array/],
    ['an empty key', [''], /non-empty/],
    ['a duplicate key', ['turnCorrelationId', 'turnCorrelationId'], /duplicate/],
    ['the app container', ['app'], /infrastructure-owned/],
    ['an infrastructure key', ['harness'], /infrastructure-owned/],
  ])('rejects %s in inheritRequestContextAppKeys', (_label, value, expected) => {
    expect(() => setup({ inheritRequestContextAppKeys: value as never })).toThrow(expected);
  });

  it('rejects a non-boolean allowInline boundary', () => {
    expect(() => setup({ allowInline: 'sometimes' as never })).toThrow(/allowInline/);
  });

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
    expect(tool!.terminalResult).toBeDefined();
  });

  it('omits durable-only types from the inline schema while retaining them for delegation', async () => {
    const { harness } = setup({ allowInline: false });
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    expect(createSpawnSubagentTool(session)).toBeUndefined();
    expect(harness._listSubagentTypeIds({ invocation: 'inline' })).toEqual([]);
    expect(harness._listSubagentTypeIds({ invocation: 'delegated' })).toEqual(['explore']);
  });
});

describe('spawn_subagent tool — execution', () => {
  it('inherits no caller app metadata by default', async () => {
    const { harness, childAgent } = setup();
    try {
      const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const tool = createSpawnSubagentTool(parent)!;
      const parentRequestContext = new RequestContext<unknown>([
        ['app', { turnCorrelationId: 'must-not-cross-without-approval' }],
      ]);

      const result = (await tool.execute!(
        { agentType: 'explore', task: 'keep caller context isolated' },
        execCtx('tc-no-lineage', parentRequestContext),
      )) as any;

      expect(result.isError).not.toBe(true);
      const childRequestContext = childAgent.streamOptions[0]?.requestContext as RequestContext | undefined;
      expect(childRequestContext?.get('turnCorrelationId')).toBeUndefined();
      expect(childRequestContext?.get('app')).toBeUndefined();
      expect((childRequestContext?.get('harness') as { app?: unknown } | undefined)?.app).toBeUndefined();
    } finally {
      await harness.shutdown();
    }
  });

  it('copies only an explicitly approved observability key into the child turn context', async () => {
    const lineageKey = 'turnCorrelationId';
    const lineageValue = 'opaque-turn-5049';
    const functionalRequestId = 'functional-request-must-not-cross';
    const secretCanary = 'PRIVATE_APP_METADATA_CANARY';
    const { harness, childAgent } = setup({ inheritRequestContextAppKeys: [lineageKey] });
    try {
      const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
      const tool = createSpawnSubagentTool(parent)!;
      const parentRequestContext = new RequestContext<unknown>([
        [
          'app',
          {
            [lineageKey]: lineageValue,
            'papersflow.requestId': functionalRequestId,
            'papersflow.userId': 'user-must-not-cross',
            instructions: secretCanary,
          },
        ],
      ]);

      const result = (await tool.execute!(
        { agentType: 'explore', task: 'inspect lineage safely' },
        execCtx('tc-lineage', parentRequestContext),
      )) as any;

      expect(result.isError).not.toBe(true);
      const childRequestContext = childAgent.streamOptions[0]?.requestContext as RequestContext | undefined;
      expect(childRequestContext).toBeInstanceOf(RequestContext);
      expect(childRequestContext?.get(lineageKey)).toBe(lineageValue);
      expect(childRequestContext?.get('app')).toEqual({ [lineageKey]: lineageValue });
      expect((childRequestContext?.get('harness') as { app?: unknown } | undefined)?.app).toEqual({
        [lineageKey]: lineageValue,
      });
      expect(childRequestContext?.get('requestId')).toBeUndefined();
      expect(childRequestContext?.get('userId')).toBeUndefined();
      expect((childRequestContext?.get('app') as Record<string, unknown>)['papersflow.requestId']).toBeUndefined();
      expect(childRequestContext?.get('papersflow.userId')).toBeUndefined();
      expect(JSON.stringify(childRequestContext?.toJSON())).not.toContain(secretCanary);
      expect(JSON.stringify(childRequestContext?.toJSON())).not.toContain(functionalRequestId);
      expect(JSON.stringify(childRequestContext?.toJSON())).not.toContain('user-must-not-cross');
    } finally {
      await harness.shutdown();
    }
  });

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
    childAgent.fullOutput = {
      ...childAgent.fullOutput,
      text: 'child says hi',
      terminalToolResult: outcomeTerminalResult('child says hi'),
    };

    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const tool = createSpawnSubagentTool(parent)!;

    const result = (await tool.execute!({ agentType: 'explore', task: 'find usages of X' }, execCtx())) as any;

    expect(typeof result.subagentSessionId).toBe('string');
    expect(result.subagentSessionId).not.toBe(parent.id);
    expect(result.subagentSessionId.length).toBeGreaterThan(0);
    const terminalContext = {
      toolName: SPAWN_SUBAGENT_TOOL_ID,
      toolCallId: 'tc-1',
      args: { delivery: 'final' },
      batchSize: 1,
      batchIndex: 0,
      runId: 'run-1',
      abortSignal: new AbortController().signal,
    };
    expect(await tool.terminalResult!.isSuccess(result, terminalContext)).toBe(true);
    expect(await tool.terminalResult!.project!(result, terminalContext)).toEqual({
      kind: 'subagent-direct-answer',
      subagentSessionId: result.subagentSessionId,
      text: 'child says hi',
    });
    expect(
      await tool.terminalResult!.isSuccess(result, {
        ...terminalContext,
        args: {},
      }),
    ).toBe(false);
  });

  it('treats every resolved non-stop child finish reason as incomplete, never as direct success', async () => {
    const { harness, childAgent } = setup();
    childAgent.fullOutput = {
      ...childAgent.fullOutput,
      finishReason: 'length',
      text: 'partial answer cut off by the provider',
      terminalToolResult: undefined,
    };
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      const events: HarnessEvent[] = [];
      parent.subscribe(event => events.push(event));
      const tool = createSpawnSubagentTool(parent)!;
      const output = (await tool.execute!(
        { agentType: 'explore', task: 'produce a complete answer', delivery: 'final' },
        execCtx('tc-incomplete-child'),
      )) as any;
      const terminalContext = {
        toolName: SPAWN_SUBAGENT_TOOL_ID,
        toolCallId: 'tc-incomplete-child',
        args: { delivery: 'final' },
        batchSize: 1,
        batchIndex: 0,
        runId: 'run-1',
        abortSignal: new AbortController().signal,
      };

      expect(output).toMatchObject({
        isError: true,
        result: {
          status: 'error',
          finishReason: 'length',
          error: {
            code: 'harness.subagent_incomplete',
          },
        },
      });
      expect(await tool.terminalResult!.isSuccess(output, terminalContext)).toBe(false);
      expect(events.find(event => event.type === 'subagent_end')).toMatchObject({
        type: 'subagent_end',
        isError: true,
        output: {
          status: 'error',
          finishReason: 'length',
        },
      });
    } finally {
      await harness.shutdown();
    }
  });

  it('rejects a bare provider stop without the framework-owned semantic outcome report', async () => {
    const { harness, childAgent } = setup();
    childAgent.fullOutput = {
      ...childAgent.fullOutput,
      finishReason: 'stop',
      text: 'I edited the source and compilation succeeded.',
      terminalToolResult: undefined,
    };
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      const tool = createSpawnSubagentTool(parent)!;
      const output = (await tool.execute!(
        { agentType: 'explore', task: 'repair and compile the project', delivery: 'final' },
        execCtx('tc-missing-outcome'),
      )) as any;

      expect(output).toMatchObject({
        isError: true,
        result: {
          status: 'error',
          finishReason: 'stop',
          error: { code: 'harness.subagent_outcome_missing' },
        },
      });
    } finally {
      await harness.shutdown();
    }
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

  it('returns and emits only a bounded child summary, never raw FullOutput internals', async () => {
    const { harness, childAgent } = setup();
    const secret = 'PF_RAW_CHILD_COMPILE_LOG_SECRET_7f0f6f';
    const multiMegabyteCompileLog = `${'compiler output '.repeat(160_000)}${secret}`;
    childAgent.fullOutput = {
      ...childAgent.fullOutput,
      text: 'The child completed the repair.',
      terminalToolResult: outcomeTerminalResult('The child completed the repair.'),
      steps: [{ providerMetadata: { privateCompileLog: multiMegabyteCompileLog } }],
      toolCalls: [{ toolName: 'compileLatex', args: { source: multiMegabyteCompileLog } }],
      toolResults: [{ toolName: 'compileLatex', result: { log: multiMegabyteCompileLog } }],
      providerMetadata: { privateCompileLog: multiMegabyteCompileLog },
      messages: [{ role: 'tool', content: multiMegabyteCompileLog }],
      totalUsage: {
        inputTokens: 12_345,
        outputTokens: 678,
        totalTokens: 13_023,
        cachedInputTokens: 10_000,
        raw: { privateCompileLog: multiMegabyteCompileLog },
      },
    };
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      const events: HarnessEvent[] = [];
      parent.subscribe(event => events.push(event));
      const tool = createSpawnSubagentTool(parent)!;

      const output = (await tool.execute!(
        { agentType: 'explore', task: 'diagnose and repair the LaTeX project' },
        execCtx('tc-bounded-child'),
      )) as any;
      const end = events.find(event => event.type === 'subagent_end') as any;
      const serializedToolOutput = JSON.stringify(output);
      const serializedEndOutput = JSON.stringify(end?.output);

      expect(output.result).toEqual({
        status: 'success',
        outcome: 'completed',
        text: 'The child completed the repair.',
        textTruncated: false,
        finishReason: 'stop',
        stepCount: 1,
        toolCallCount: 1,
        toolResultCount: 1,
        usage: {
          inputTokens: 12_345,
          outputTokens: 678,
          totalTokens: 13_023,
          cachedInputTokens: 10_000,
        },
        evidence: [{ kind: 'analysis', description: 'Verified by the inline subagent fixture.' }],
      });
      expect(end?.output).toEqual(output.result);
      expect(serializedToolOutput).not.toContain(secret);
      expect(serializedEndOutput).not.toContain(secret);
      expect(serializedToolOutput).not.toContain('privateCompileLog');
      expect(serializedToolOutput).not.toContain('providerMetadata');
      expect(Buffer.byteLength(serializedToolOutput, 'utf8')).toBeLessThan(64 * 1024);
      expect(Buffer.byteLength(serializedEndOutput, 'utf8')).toBeLessThan(64 * 1024);
    } finally {
      await harness.shutdown();
    }
  });

  it('UTF-8 truncates oversized child text and refuses partial direct delivery', async () => {
    const { harness, childAgent } = setup();
    childAgent.fullOutput = {
      ...childAgent.fullOutput,
      text: '\ud83e\uddea'.repeat(20_000),
      terminalToolResult: outcomeTerminalResult('\ud83e\uddea'.repeat(20_000)),
    };
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      const events: HarnessEvent[] = [];
      parent.subscribe(event => events.push(event));
      const tool = createSpawnSubagentTool(parent)!;
      const output = (await tool.execute!(
        { agentType: 'explore', task: 'return an intentionally huge report', delivery: 'final' },
        execCtx('tc-truncated-child'),
      )) as any;
      const terminalContext = {
        toolName: SPAWN_SUBAGENT_TOOL_ID,
        toolCallId: 'tc-truncated-child',
        args: { delivery: 'final' },
        batchSize: 1,
        batchIndex: 0,
        runId: 'run-1',
        abortSignal: new AbortController().signal,
      };

      expect(output.result.status).toBe('error');
      expect(output.result.error.code).toBe('harness.subagent_outcome_missing');
      expect(output.result.textTruncated).toBe(true);
      expect(output.result.text).toContain('[truncated by Harness]');
      expect(output.result.text).not.toContain('\ufffd');
      expect(new TextEncoder().encode(output.result.text).byteLength).toBeLessThanOrEqual(
        MAX_HARNESS_SUBAGENT_RESULT_TEXT_BYTES,
      );
      expect(Buffer.byteLength(JSON.stringify(output), 'utf8')).toBeLessThan(64 * 1024);
      expect(await tool.terminalResult!.isSuccess(output, terminalContext)).toBe(false);
      const end = events.find(event => event.type === 'subagent_end');
      expect(end?.type).toBe('subagent_end');
      if (end?.type === 'subagent_end') {
        expect(end.output.textTruncated).toBe(true);
        expect(new TextEncoder().encode(end.output.text).byteLength).toBeLessThanOrEqual(
          MAX_HARNESS_SUBAGENT_EVENT_TEXT_BYTES,
        );
        expect(Buffer.byteLength(JSON.stringify(end), 'utf8')).toBeLessThan(8 * 1024);
      }
    } finally {
      await harness.shutdown();
    }
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

  it('does not await an ancestor close that is draining the inline tool itself', async () => {
    const closeTimeoutMs = 1_000;
    const { harness, storage } = setup({ closeTimeoutMs });
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const originalSession = harness.session.bind(harness);
    let child!: Awaited<ReturnType<typeof harness.session>>;
    let childMessageStarted!: () => void;
    const childMessageSeen = new Promise<void>(resolve => {
      childMessageStarted = resolve;
    });
    let releaseChildMessage!: () => void;
    const childMessageGate = new Promise<void>(resolve => {
      releaseChildMessage = resolve;
    });

    // Hold the inline child's terminal return across the ancestor close walk.
    // This models a provider/tool turn finishing just after the user closes the
    // chat. The parent reservation keeps the root drain busy until execute()
    // returns, which is the exact cycle this regression protects.
    (harness as any).session = async (opts: any) => {
      const session = await originalSession(opts);
      if (opts?.parentSessionId === parent.id) {
        child = session;
        (session as any).message = async () => {
          childMessageStarted();
          await childMessageGate;
          return {
            text: 'child finished while the subtree was closing',
            finishReason: 'stop',
            terminalToolResult: outcomeTerminalResult('child finished while the subtree was closing'),
            usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
          };
        };
      }
      return session;
    };

    try {
      const tool = createSpawnSubagentTool(parent)!;
      const execution = tool.execute!({ agentType: 'explore', task: 'finish during close' }, execCtx('tc-close-cycle'));
      await childMessageSeen;

      expect(
        await storage.listSessions({
          harnessName: 'default',
          resourceId: parent.resourceId,
          includeClosed: false,
          parentSessionId: parent.id,
        }),
      ).toEqual([expect.objectContaining({ id: child.id, parentSessionId: parent.id })]);

      const closeStartedAt = Date.now();
      const closing = parent.close();
      let closeError: unknown;
      void closing.catch(error => {
        closeError = error;
      });
      while (!parent.isClosing || !child.isClosing) {
        if (Date.now() - closeStartedAt > closeTimeoutMs / 2) {
          throw new Error(
            `ancestor close did not claim the inline child (parent=${parent.lifecycleState}, child=${child.lifecycleState}, error=${String(closeError)})`,
          );
        }
        await new Promise(resolve => setTimeout(resolve, 1));
      }
      releaseChildMessage();

      const output = (await execution) as any;
      await closing;
      const elapsedMs = Date.now() - closeStartedAt;

      expect(output).toMatchObject({
        subagentSessionId: child.id,
        result: { status: 'success', finishReason: 'stop' },
      });
      // The old path awaited the ancestor's own close promise and therefore
      // consumed essentially the full 1s deadline before the tool could return.
      expect(elapsedMs).toBeLessThan(closeTimeoutMs / 2);
      expect(parent._internalSubagentExecutionsInFlight).toBe(0);
      expect((await storage.loadSession({ sessionId: child.id }))?.closedAt).toBeDefined();
    } finally {
      await harness.shutdown();
    }
  });

  it('does not start a stale inline child after conversation revision wins during allocation', async () => {
    const { harness, childAgent, storage } = setup();
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    const originalSession = harness.session.bind(harness);
    let allocationStarted!: () => void;
    const allocationSeen = new Promise<void>(resolve => {
      allocationStarted = resolve;
    });
    let releaseAllocation!: () => void;
    const allocationGate = new Promise<void>(resolve => {
      releaseAllocation = resolve;
    });

    (harness as any).session = async (opts: any) => {
      if (opts?.parentSessionId === parent.id) {
        allocationStarted();
        await allocationGate;
      }
      return originalSession(opts);
    };

    try {
      const tool = createSpawnSubagentTool(parent)!;
      const execution = tool.execute!({ agentType: 'explore', task: 'stale branch work' }, execCtx('tc-revision-race'));
      await allocationSeen;

      await parent.resetPlanTasks({ reason: 'message_edited' });
      releaseAllocation();
      const output = (await execution) as any;

      expect(output).toMatchObject({
        isError: true,
        errorName: 'HarnessBusyError',
        reason: 'harness.busy',
      });
      expect(output.subagentSessionId).not.toBe('');
      expect(childAgent.streamCalls).toBe(0);
      expect(parent.getDisplayState().activeSubagents).toEqual({});
      expect(parent._internalSubagentExecutionsInFlight).toBe(0);
      expect((await storage.loadSession({ sessionId: output.subagentSessionId }))?.closedAt).toBeDefined();
    } finally {
      releaseAllocation();
      await harness.shutdown();
    }
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
      expect(parent._internalTryReserveSubagentExecution().reserved).toBe(true);
      const rejected = (await tool.execute!({ agentType: 'explore', task: 'x' } as any, execCtx('tc-2'))) as any;
      expect(rejected.isError).toBe(true);
      expect(rejected.errorName).toBe('HarnessSubagentConcurrencyLimitError');
      expect(rejected.maxConcurrent).toBe(1);
      expect(rejected.subagentSessionId).toBe('');

      // Free the slot → a spawn proceeds (child runs to completion) and releases.
      parent._internalReleaseSubagentExecution();
      const ok = (await tool.execute!({ agentType: 'explore', task: 'y' } as any, execCtx('tc-3'))) as any;
      expect(ok.isError).toBeFalsy();
      expect(typeof ok.subagentSessionId).toBe('string');
      expect(parent._internalSubagentExecutionsInFlight).toBe(0); // reservation released in finally
    } finally {
      await harness.shutdown();
    }
  });

  it('does not gate spawns when maxConcurrent is unset (no per-parent limit)', async () => {
    const { harness } = setup();
    const parent = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    try {
      const tool = createSpawnSubagentTool(parent)!;
      for (let i = 0; i < 99; i++) expect(parent._internalTryReserveSubagentExecution().reserved).toBe(true);
      const ok = (await tool.execute!({ agentType: 'explore', task: 'z' } as any, execCtx('tc-4'))) as any;
      expect(ok.isError).toBeFalsy();
      expect(parent._internalSubagentExecutionsInFlight).toBe(99);
      for (let i = 0; i < 99; i++) parent._internalReleaseSubagentExecution();
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
  it('reports a HITL-suspended inline subagent as a bounded public error summary', async () => {
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
      expect(out.result).toMatchObject({
        status: 'error',
        finishReason: 'suspended',
        error: {
          code: 'harness.subagent_suspended',
          messageTruncated: false,
        },
      });
      const end = events.find(e => e.type === 'subagent_end') as { isError?: boolean } | undefined;
      expect(end?.isError).toBe(true);
    } finally {
      await harness.shutdown();
    }
  });
});
