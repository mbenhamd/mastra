import { Agent } from '@mastra/core/agent';
import { AgentController } from '@mastra/core/agent-controller';
import type { AgentControllerEvent } from '@mastra/core/agent-controller';
import { Mastra } from '@mastra/core/mastra';
import { MastraLanguageModelV2Mock } from '@mastra/core/test-utils/llm-mock';
import { createTool } from '@mastra/core/tools';
import { Workspace } from '@mastra/core/workspace';
import { LibSQLStore } from '@mastra/libsql';
import { describe, it, expect, vi } from 'vitest';
import z from 'zod';

import { renderJsonResult } from './format.js';
import { runMC } from './run-mc.js';
import type { ResolutionPolicy } from './types.js';

vi.setConfig({ testTimeout: 30_000 });

function textStream(text: string, finishReason: 'stop' | 'tool-calls' = 'stop') {
  return new ReadableStream({
    start(controller) {
      controller.enqueue({ type: 'stream-start', warnings: [] });
      controller.enqueue({ type: 'response-metadata', id: 'id-1', modelId: 'mock', timestamp: new Date(0) });
      controller.enqueue({ type: 'text-start', id: 'text-1' });
      controller.enqueue({ type: 'text-delta', id: 'text-1', delta: text });
      controller.enqueue({ type: 'text-end', id: 'text-1' });
      controller.enqueue({
        type: 'finish',
        finishReason,
        usage: { inputTokens: 10, outputTokens: 20, totalTokens: 30 },
      });
      controller.close();
    },
  });
}

function toolCallStream() {
  return new ReadableStream({
    start(controller) {
      controller.enqueue({ type: 'stream-start', warnings: [] });
      controller.enqueue({ type: 'response-metadata', id: 'id-0', modelId: 'mock', timestamp: new Date(0) });
      controller.enqueue({
        type: 'tool-call',
        toolCallId: 'call-1',
        toolName: 'readFile',
        input: '{"path":"test.txt"}',
        providerExecuted: false,
      });
      controller.enqueue({
        type: 'finish',
        finishReason: 'tool-calls',
        usage: { inputTokens: 10, outputTokens: 20, totalTokens: 30 },
      });
      controller.close();
    },
  });
}

interface HarnessOptions {
  doStream: () => Promise<{ stream: ReadableStream }>;
  withReadFileTool?: boolean;
  readFileNeedsApproval?: boolean;
}

async function makeHarness(opts: HarnessOptions) {
  const storage = new LibSQLStore({ id: 'test-store', url: 'file::memory:?cache=shared' });

  const tools: Record<string, ReturnType<typeof createTool>> = {};
  if (opts.withReadFileTool) {
    tools.readFile = createTool({
      id: 'readFile',
      description: 'Read a file',
      inputSchema: z.object({ path: z.string() }),
      ...(opts.readFileNeedsApproval ? { requireApproval: true } : {}),
      execute: async () => ({ content: 'file contents' }),
    });
  }

  const agent = new Agent({
    id: 'test-agent',
    name: 'Test Agent',
    instructions: 'You answer questions.',
    model: new MastraLanguageModelV2Mock({ doStream: opts.doStream }) as any,
    tools,
  });

  const mastra = new Mastra({ agents: { 'test-agent': agent }, logger: false, storage });
  const registeredAgent = mastra.getAgent('test-agent');

  const controller = new AgentController({
    id: 'test-controller',
    storage,
    workspace: new Workspace({ name: 'test-workspace', skills: ['/tmp/test-skills'] }),
    modes: [
      {
        id: 'default',
        name: 'Default',
        description: 'default',
        defaultModelId: 'test',
        metadata: { default: true },
        instructions: 'You answer questions.',
      },
    ],
    initialState: { yolo: false },
  });
  (controller as any).getAgentForMode = () => registeredAgent;

  await controller.init();
  const session = await controller.createSession({ id: `s-${Math.random()}`, ownerId: 'test-owner' });
  await session.thread.create();

  return { controller, session };
}

type GoalEvaluationEvent = Extract<AgentControllerEvent, { type: 'goal_evaluation' }>;

function goalEvaluationEvent(overrides: Partial<GoalEvaluationEvent['payload']> = {}): GoalEvaluationEvent {
  return {
    type: 'goal_evaluation',
    payload: {
      objective: 'finish the task',
      iteration: 1,
      maxRuns: 5,
      passed: true,
      status: 'done',
      results: [],
      reason: 'goal complete',
      duration: 0,
      timedOut: false,
      maxRunsReached: false,
      suppressFeedback: false,
      ...overrides,
    },
  };
}

interface FakeGoalHarnessOptions {
  signal?: AbortSignal;
  timeoutMs?: number;
  maxTurns?: number;
  abortEvent?: AgentControllerEvent;
  policy?: ResolutionPolicy;
  model?: string;
  availableModels?: Promise<Array<{ id: string; hasApiKey: boolean; apiKeyEnvVar?: string }>>;
  modelSwitch?: Promise<void>;
  thinkingLevel?: 'high';
  thread?: { id?: string; continueLatest?: boolean };
  threadList?: Promise<Array<{ id: string; updatedAt: Date }>>;
}

function makeFakeGoalHarness(options: FakeGoalHarnessOptions = {}) {
  let listener: ((event: AgentControllerEvent) => void) | undefined;
  let resolveStarted!: () => void;
  const started = new Promise<void>(resolve => {
    resolveStarted = resolve;
  });
  const objectiveRecord = {
    id: 'goal-1',
    objective: 'finish the task',
    status: 'active' as const,
    runsUsed: 0,
    maxRuns: 5,
    judgeModelId: 'mock-judge',
    startedAt: Date.now(),
    updatedAt: Date.now(),
  };
  const fakeAgent = {
    getObjective: vi.fn().mockResolvedValue(objectiveRecord),
  };
  const unsubscribe = vi.fn();
  const controller = {
    getCurrentAgent: vi.fn(() => fakeAgent),
    setResourceId: vi.fn(),
    listAvailableModels: vi.fn(
      () => options.availableModels ?? Promise.resolve([{ id: options.model ?? 'mock-model', hasApiKey: true }]),
    ),
  };
  const session = {
    subscribe: vi.fn((handler: (event: AgentControllerEvent) => void) => {
      listener = handler;
      return unsubscribe;
    }),
    sendSignal: vi.fn(() => {
      resolveStarted();
      return { accepted: Promise.resolve() };
    }),
    sendMessage: vi.fn(),
    abort: vi.fn(() => {
      if (options.abortEvent) listener?.(options.abortEvent);
    }),
    respondToToolApproval: vi.fn(),
    respondToToolSuspension: vi.fn().mockResolvedValue(undefined),
    model: {
      switch: vi.fn(() => options.modelSwitch ?? Promise.resolve()),
    },
    state: {
      set: vi.fn().mockResolvedValue(undefined),
    },
    thread: {
      getId: vi.fn(() => 'thread-1'),
      getById: vi.fn().mockResolvedValue({ id: 'thread-1' }),
      create: vi.fn(),
      list: vi.fn(() => options.threadList ?? Promise.resolve([])),
      switch: vi.fn().mockResolvedValue(undefined),
    },
  };
  const goalManager = {
    setGoal: vi.fn().mockResolvedValue(objectiveRecord),
    saveToThread: vi.fn().mockResolvedValue(undefined),
  };
  const run = runMC({
    controller: controller as any,
    session: session as any,
    goal: {
      objective: objectiveRecord.objective,
      judgeModelId: objectiveRecord.judgeModelId,
      maxRuns: objectiveRecord.maxRuns,
      goalManager: goalManager as any,
    },
    ...(options.signal ? { signal: options.signal } : {}),
    ...(options.timeoutMs !== undefined ? { timeoutMs: options.timeoutMs } : {}),
    ...(options.maxTurns !== undefined ? { maxTurns: options.maxTurns } : {}),
    ...(options.policy ? { policy: options.policy } : {}),
    ...(options.model ? { model: options.model } : {}),
    ...(options.thinkingLevel ? { thinkingLevel: options.thinkingLevel } : {}),
    ...(options.thread ? { thread: options.thread } : {}),
  });

  return {
    run,
    started,
    controller,
    session,
    unsubscribe,
    emit: (event: AgentControllerEvent) => listener?.(event),
  };
}

async function expectResultPending(result: Promise<unknown>) {
  let settled = false;
  void result.then(() => {
    settled = true;
  });
  await Promise.resolve();
  expect(settled).toBe(false);
}

describe('runMC', () => {
  it('resolves the final result when awaited without iterating', async () => {
    const { controller, session } = await makeHarness({
      doStream: async () => ({ stream: textStream('The answer is 4.') }),
    });

    const run = runMC({ controller, session, prompt: 'What is 2+2?' });
    const result = await run.result;

    expect(result.status).toBe('completed');
    expect(result.exitCode).toBe(0);
    expect(result.text).toBe('The answer is 4.');
    expect(result.usage).toEqual({ inputTokens: 10, outputTokens: 20, totalTokens: 30 });
    expect(result.threadId).toBeTruthy();
  });

  it('yields controller events while iterating, then resolves', async () => {
    const { controller, session } = await makeHarness({ doStream: async () => ({ stream: textStream('Hi there') }) });

    const run = runMC({ controller, session, prompt: 'Greet me' });
    const types: string[] = [];
    for await (const event of run) {
      types.push(event.type);
    }
    const result = await run.result;

    expect(types).toContain('agent_start');
    expect(types).toContain('agent_end');
    expect(result.status).toBe('completed');
    expect(result.text).toBe('Hi there');
  });

  it('applies a tool approval policy and records tool calls', async () => {
    let call = 0;
    const { controller, session } = await makeHarness({
      withReadFileTool: true,
      readFileNeedsApproval: true,
      doStream: async () => {
        call++;
        return { stream: call === 1 ? toolCallStream() : textStream('Done reading') };
      },
    });

    const approvals: string[] = [];
    const policy: ResolutionPolicy = {
      onToolApproval: event => {
        approvals.push(event.toolName);
        return 'approve';
      },
      onSuspension: () => ({ resumeData: 'Yes' }),
    };

    const run = runMC({ controller, session, prompt: 'Read test.txt', policy });
    const result = await run.result;

    expect(result.status).toBe('completed');
    expect(approvals).toContain('readFile');
    expect(result.toolCalls.map(c => c.name)).toContain('readFile');
  });

  it('returns status "aborted" with exit code 1 when aborted', async () => {
    const { controller, session } = await makeHarness({
      doStream: async () => {
        await new Promise(r => setTimeout(r, 500));
        return { stream: textStream('too late') };
      },
    });

    const run = runMC({ controller, session, prompt: 'Slow task' });
    run.abort();
    const result = await run.result;

    expect(result.status).toBe('aborted');
    expect(result.exitCode).toBe(1);
  });

  it('returns status "aborted" when an external signal is already aborted', async () => {
    const { controller, session } = await makeHarness({
      doStream: async () => {
        await new Promise(r => setTimeout(r, 500));
        return { stream: textStream('too late') };
      },
    });

    const result = await runMC({ controller, session, prompt: 'Slow', signal: AbortSignal.abort() }).result;
    expect(result.status).toBe('aborted');
  });

  it('returns status "timeout" with exit code 2 when the timeout elapses', async () => {
    const { controller, session } = await makeHarness({
      doStream: async () => {
        await new Promise(r => setTimeout(r, 1000));
        return { stream: textStream('too late') };
      },
    });

    const run = runMC({ controller, session, prompt: 'Slow task', timeoutMs: 50 });
    const result = await run.result;

    expect(result.status).toBe('timeout');
    expect(result.exitCode).toBe(2);
  });

  it('returns a structured error result for an unknown model (no throw)', async () => {
    const { controller, session } = await makeHarness({ doStream: async () => ({ stream: textStream('unused') }) });

    const result = await runMC({ controller, session, prompt: 'x', model: 'does-not-exist/model' }).result;

    expect(result.status).toBe('error');
    expect(result.exitCode).toBe(1);
    expect(result.error?.message).toMatch(/Unknown model/);
  });

  it('returns a structured error result when thread resolution fails', async () => {
    const { controller, session } = await makeHarness({ doStream: async () => ({ stream: textStream('unused') }) });

    const result = await runMC({
      controller,
      session,
      prompt: 'x',
      thread: { id: 'no-such-thread-or-title' },
    }).result;

    expect(result.status).toBe('error');
    expect(result.error?.message).toMatch(/No thread found/);
  });

  it('returns status "max_turns" with exit code 1 when the turn cap is hit mid-task', async () => {
    // First turn is a tool call, so the agent still has work to do when the
    // single-turn cap forces an abort. Later turns would produce text, but the
    // cap should stop the run before then.
    let call = 0;
    const { controller, session } = await makeHarness({
      withReadFileTool: true,
      doStream: async () => {
        call++;
        return { stream: call === 1 ? toolCallStream() : textStream('summary') };
      },
    });

    const run = runMC({ controller, session, prompt: 'Read then summarize', maxTurns: 1 });
    const result = await run.result;

    expect(result.status).toBe('max_turns');
    expect(result.exitCode).toBe(1);
  });

  it('completes normally when the run finishes within the turn cap', async () => {
    const { controller, session } = await makeHarness({
      doStream: async () => ({ stream: textStream('All done') }),
    });

    // Generous cap the single-turn run never reaches.
    const result = await runMC({ controller, session, prompt: 'One shot', maxTurns: 5 }).result;

    expect(result.status).toBe('completed');
    expect(result.exitCode).toBe(0);
    expect(result.text).toBe('All done');
  });

  describe('goal terminal settlement', () => {
    it('settles an explicit abort exactly once without waiting for agent_end', async () => {
      const harness = makeFakeGoalHarness({ abortEvent: goalEvaluationEvent() });
      await harness.started;

      harness.run.abort();
      const result = await harness.run.result;

      expect(result.status).toBe('aborted');
      expect(result.exitCode).toBe(1);
      expect(harness.session.abort).toHaveBeenCalledTimes(1);
      harness.emit(goalEvaluationEvent());
      expect(await harness.run.result).toBe(result);
      expect(harness.unsubscribe).toHaveBeenCalledTimes(1);
    });

    it('settles an already-aborted external signal before starting goal work', async () => {
      const harness = makeFakeGoalHarness({ signal: AbortSignal.abort() });

      const result = await harness.run.result;

      expect(result.status).toBe('aborted');
      expect(harness.session.abort).toHaveBeenCalledTimes(1);
      expect(harness.session.sendSignal).not.toHaveBeenCalled();
      expect(harness.session.subscribe).not.toHaveBeenCalled();
      expect(harness.unsubscribe).not.toHaveBeenCalled();
    });

    it('settles an in-flight external abort exactly once', async () => {
      const abortController = new AbortController();
      const harness = makeFakeGoalHarness({
        signal: abortController.signal,
        abortEvent: goalEvaluationEvent(),
      });
      await harness.started;

      abortController.abort();
      const result = await harness.run.result;

      expect(result.status).toBe('aborted');
      expect(harness.session.abort).toHaveBeenCalledTimes(1);
      harness.emit(goalEvaluationEvent({ status: 'paused' }));
      expect(await harness.run.result).toBe(result);
      expect(harness.unsubscribe).toHaveBeenCalledTimes(1);
    });

    it('does not mutate setup after abort settles during a deferred model lookup', async () => {
      const abortController = new AbortController();
      let resolveModels!: (models: Array<{ id: string; hasApiKey: boolean }>) => void;
      const availableModels = new Promise<Array<{ id: string; hasApiKey: boolean }>>(resolve => {
        resolveModels = resolve;
      });
      const harness = makeFakeGoalHarness({
        signal: abortController.signal,
        model: 'mock-model',
        availableModels,
      });

      expect(harness.controller.listAvailableModels).toHaveBeenCalledTimes(1);
      abortController.abort();
      const result = await harness.run.result;
      resolveModels([{ id: 'mock-model', hasApiKey: true }]);
      await Promise.resolve();
      await Promise.resolve();

      expect(result.status).toBe('aborted');
      expect(harness.session.model.switch).not.toHaveBeenCalled();
      expect(harness.session.subscribe).not.toHaveBeenCalled();
      expect(harness.session.sendSignal).not.toHaveBeenCalled();
    });

    it('does not continue setup after abort settles during a model switch', async () => {
      const abortController = new AbortController();
      let resolveSwitch!: () => void;
      const modelSwitch = new Promise<void>(resolve => {
        resolveSwitch = resolve;
      });
      const harness = makeFakeGoalHarness({
        signal: abortController.signal,
        model: 'mock-model',
        modelSwitch,
        thinkingLevel: 'high',
      });
      await vi.waitFor(() => expect(harness.session.model.switch).toHaveBeenCalledTimes(1));

      abortController.abort();
      const result = await harness.run.result;
      resolveSwitch();
      await Promise.resolve();
      await Promise.resolve();

      expect(result.status).toBe('aborted');
      expect(harness.session.state.set).not.toHaveBeenCalled();
      expect(harness.session.subscribe).not.toHaveBeenCalled();
      expect(harness.session.sendSignal).not.toHaveBeenCalled();
    });

    it('does not switch threads after abort settles during a deferred thread lookup', async () => {
      const abortController = new AbortController();
      let resolveThreads!: (threads: Array<{ id: string; updatedAt: Date }>) => void;
      const threadList = new Promise<Array<{ id: string; updatedAt: Date }>>(resolve => {
        resolveThreads = resolve;
      });
      const harness = makeFakeGoalHarness({
        signal: abortController.signal,
        thread: { continueLatest: true },
        threadList,
      });

      expect(harness.session.thread.list).toHaveBeenCalledTimes(1);
      abortController.abort();
      const result = await harness.run.result;
      resolveThreads([{ id: 'newest-thread', updatedAt: new Date() }]);
      await Promise.resolve();
      await Promise.resolve();

      expect(result.status).toBe('aborted');
      expect(harness.session.thread.switch).not.toHaveBeenCalled();
      expect(harness.session.sendSignal).not.toHaveBeenCalled();
    });

    it('keeps maxTurns precedence when abort races a terminal goal evaluation', async () => {
      const harness = makeFakeGoalHarness({
        maxTurns: 1,
        abortEvent: goalEvaluationEvent(),
      });
      await harness.started;

      harness.emit({
        type: 'message_end',
        message: {
          id: 'assistant-1',
          role: 'assistant',
          content: [],
          createdAt: new Date(),
          stopReason: 'tool_use',
        },
      });
      const result = await harness.run.result;

      expect(result.status).toBe('max_turns');
      expect(result.exitCode).toBe(1);
      expect(harness.session.abort).toHaveBeenCalledTimes(1);
      expect(harness.unsubscribe).toHaveBeenCalledTimes(1);
    });

    it('settles agent_end error even when no error event was emitted first', async () => {
      const harness = makeFakeGoalHarness();
      await harness.started;

      harness.emit({ type: 'agent_end', reason: 'error' });
      const result = await harness.run.result;

      expect(result.status).toBe('error');
      expect(result.finishReason).toBe('error');
      expect(result.error).toEqual({
        name: 'Error',
        message: 'Agent run ended with an error before reporting details.',
      });
      // Human mode prints result.error.message, while JSON mode serializes the
      // same structured diagnostic.
      expect(`Error: ${result.error!.message}\n`).toBe(
        'Error: Agent run ended with an error before reporting details.\n',
      );
      expect(JSON.parse(renderJsonResult(result))).toMatchObject({
        status: 'error',
        error: {
          name: 'Error',
          message: 'Agent run ended with an error before reporting details.',
        },
      });
      harness.emit(goalEvaluationEvent());
      expect(await harness.run.result).toBe(result);
      expect(harness.unsubscribe).toHaveBeenCalledTimes(1);
    });

    it('maps a waitingForUser goal evaluation to a paused headless result', async () => {
      const harness = makeFakeGoalHarness();
      await harness.started;
      const event = goalEvaluationEvent({
        passed: false,
        status: 'active',
        waitingForUser: true,
        reason: 'Need the user to choose a direction.',
      });

      harness.emit(event);
      await expectResultPending(harness.run.result);
      harness.emit({ type: 'agent_end', reason: 'complete' });
      const result = await harness.run.result;

      expect(result).toMatchObject({
        status: 'paused',
        reason: 'Need the user to choose a direction.',
        goalEvent: event,
      });
      expect(harness.unsubscribe).toHaveBeenCalledTimes(1);
    });

    it('defensively maps maxRunsReached with an active Core status to paused', async () => {
      const harness = makeFakeGoalHarness();
      await harness.started;
      const event = goalEvaluationEvent({
        passed: false,
        status: 'active',
        maxRunsReached: true,
        reason: 'Evaluation budget exhausted.',
      });

      harness.emit(event);
      await expectResultPending(harness.run.result);
      harness.emit({ type: 'agent_end', reason: 'complete' });
      const result = await harness.run.result;

      expect(result).toMatchObject({
        status: 'paused',
        reason: 'Evaluation budget exhausted.',
        goalEvent: event,
      });
      expect(harness.unsubscribe).toHaveBeenCalledTimes(1);
    });

    it('keeps timeout precedence when abort races a terminal goal evaluation', async () => {
      const harness = makeFakeGoalHarness({
        timeoutMs: 25,
        abortEvent: goalEvaluationEvent(),
      });
      await harness.started;

      const result = await harness.run.result;

      expect(result.status).toBe('timeout');
      expect(result.exitCode).toBe(2);
      expect(harness.session.abort).toHaveBeenCalledTimes(1);
      expect(harness.unsubscribe).toHaveBeenCalledTimes(1);
    });

    it('waits through pending evaluation and complete agent_end for a terminal goal evaluation', async () => {
      const harness = makeFakeGoalHarness();
      await harness.started;

      harness.emit({ type: 'agent_start' });
      harness.emit(goalEvaluationEvent({ pending: true }));
      harness.emit({ type: 'agent_end', reason: 'complete' });
      await expectResultPending(harness.run.result);

      harness.emit(goalEvaluationEvent({ reason: 'Completed after agent_end.' }));
      const result = await harness.run.result;

      expect(result).toMatchObject({
        status: 'done',
        finishReason: 'complete',
        reason: 'Completed after agent_end.',
      });
    });

    it('waits for the resumed agent_end when terminal goal evaluation arrives first', async () => {
      const policy: ResolutionPolicy = {
        onToolApproval: () => 'approve',
        onSuspension: () => ({ resumeData: { answer: 'continue' } }),
      };
      const harness = makeFakeGoalHarness({ policy });
      await harness.started;

      harness.emit({
        type: 'tool_suspended',
        toolCallId: 'call-1',
        toolName: 'ask_user',
        args: {},
        suspendPayload: { question: 'Continue?' },
      });
      harness.emit({ type: 'agent_end', reason: 'suspended' });
      await expectResultPending(harness.run.result);
      expect(harness.session.respondToToolSuspension).toHaveBeenCalledWith({
        toolCallId: 'call-1',
        resumeData: { answer: 'continue' },
      });

      const terminalEvaluation = goalEvaluationEvent();
      harness.emit({ type: 'agent_start' });
      harness.emit({ type: 'agent_end', reason: 'complete' });
      await expectResultPending(harness.run.result);

      // Core opens a fresh controller run when the goal chunk follows a model
      // finish. The prior complete boundary must not authorize this evaluation.
      harness.emit({ type: 'agent_start' });
      harness.emit(terminalEvaluation);
      await expectResultPending(harness.run.result);

      harness.emit({ type: 'agent_end', reason: 'complete' });
      const result = await harness.run.result;

      expect(result).toMatchObject({
        status: 'done',
        finishReason: 'complete',
        goalEvent: terminalEvaluation,
      });
      harness.emit(goalEvaluationEvent({ status: 'paused' }));
      harness.emit({ type: 'agent_end', reason: 'complete' });
      expect(await harness.run.result).toBe(result);
      expect(harness.unsubscribe).toHaveBeenCalledTimes(1);
    });
  });

  it('does not call process.exit', async () => {
    const exitSpy = vi.spyOn(process, 'exit').mockImplementation((() => undefined) as never);
    try {
      const { controller, session } = await makeHarness({ doStream: async () => ({ stream: textStream('ok') }) });

      await runMC({ controller, session, prompt: 'hi' }).result;

      expect(exitSpy).not.toHaveBeenCalled();
    } finally {
      exitSpy.mockRestore();
    }
  });
});
